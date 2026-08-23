use arrow_ipc::writer::FileWriter;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::File;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Deserialize)]
struct S3Config {
    bucket: String,
    region: Option<String>,
    endpoint_url: Option<String>,
    path_style: Option<bool>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
}

#[derive(Deserialize)]
struct ParquetReadRequest {
    #[serde(flatten)]
    s3: S3Config,
    object_key: String,
    file_size: u64,
    footer_size_hint: Option<usize>,
    projection: Option<Vec<String>>,
    row_groups: Option<Vec<usize>>,
    row_ranges: Option<Vec<RowRange>>,
    batch_size: Option<usize>,
    output_ipc_path: String,
}

#[derive(Clone, Deserialize, Serialize)]
struct RowRange {
    start: usize,
    end_exclusive: usize,
}

#[derive(Serialize)]
struct ParquetReadResult {
    schema_version: &'static str,
    rows: usize,
    batches: usize,
    output_bytes: u64,
    projected_columns: Vec<String>,
    selected_row_groups: Vec<usize>,
    selected_row_ranges: Vec<RowRange>,
    requested_rows: usize,
    range_backend: &'static str,
    range_count: u64,
    requested_range_bytes: u64,
    received_range_bytes: u64,
}

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    range_count: Arc<AtomicU64>,
    requested_range_bytes: Arc<AtomicU64>,
    received_range_bytes: Arc<AtomicU64>,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> (Self, RangeCounters) {
        let counters = RangeCounters {
            range_count: Arc::new(AtomicU64::new(0)),
            requested_range_bytes: Arc::new(AtomicU64::new(0)),
            received_range_bytes: Arc::new(AtomicU64::new(0)),
        };
        (
            Self {
                inner,
                range_count: Arc::clone(&counters.range_count),
                requested_range_bytes: Arc::clone(&counters.requested_range_bytes),
                received_range_bytes: Arc::clone(&counters.received_range_bytes),
            },
            counters,
        )
    }
}

#[derive(Clone, Debug)]
struct RangeCounters {
    range_count: Arc<AtomicU64>,
    requested_range_bytes: Arc<AtomicU64>,
    received_range_bytes: Arc<AtomicU64>,
}

impl RangeCounters {
    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.range_count.load(Ordering::Relaxed),
            self.requested_range_bytes.load(Ordering::Relaxed),
            self.received_range_bytes.load(Ordering::Relaxed),
        )
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "counting({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: Range<u64>,
    ) -> object_store::Result<bytes::Bytes> {
        let requested = range.end.saturating_sub(range.start);
        self.range_count.fetch_add(1, Ordering::Relaxed);
        self.requested_range_bytes
            .fetch_add(requested, Ordering::Relaxed);
        let bytes = self.inner.get_range(location, range).await?;
        self.received_range_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[derive(Deserialize)]
struct RangeRequest {
    #[serde(flatten)]
    s3: S3Config,
    object_key: String,
    start: u64,
    end_exclusive: u64,
}

pub fn s3_get_range_impl(request_json: &str) -> Result<Vec<u8>, String> {
    let request: RangeRequest = serde_json::from_str(request_json)
        .map_err(|error| format!("invalid range request: {error}"))?;
    if request.end_exclusive <= request.start {
        return Err("range must satisfy start < end_exclusive".to_string());
    }
    let runtime = tokio::runtime::Runtime::new().map_err(safe_error)?;
    runtime.block_on(async move {
        let store = build_store(request.s3)?;
        let path = ObjectPath::parse(request.object_key).map_err(safe_error)?;
        let bytes = store
            .get_range(&path, request.start..request.end_exclusive)
            .await
            .map_err(safe_error)?;
        Ok(bytes.to_vec())
    })
}

pub fn read_s3_parquet_to_ipc_impl(request_json: &str) -> Result<String, String> {
    let request: ParquetReadRequest = serde_json::from_str(request_json)
        .map_err(|error| format!("invalid parquet read request: {error}"))?;
    if request.file_size == 0 {
        return Err("file_size must be positive".to_string());
    }
    let runtime = tokio::runtime::Runtime::new().map_err(safe_error)?;
    runtime.block_on(read_parquet(request))
}

async fn read_parquet(request: ParquetReadRequest) -> Result<String, String> {
    let (counting_store, range_counters) = CountingObjectStore::new(build_store(request.s3)?);
    let store: Arc<dyn ObjectStore> = Arc::new(counting_store);
    let path = ObjectPath::parse(request.object_key).map_err(safe_error)?;
    let mut reader = ParquetObjectReader::new(store, path).with_file_size(request.file_size);
    if let Some(hint) = request.footer_size_hint {
        reader = reader.with_footer_size_hint(hint);
    }
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(safe_error)?;
    if let Some(size) = request.batch_size {
        if size == 0 {
            return Err("batch_size must be positive".to_string());
        }
        builder = builder.with_batch_size(size);
    }
    let selected_row_groups = request.row_groups.unwrap_or_default();
    let row_group_count = builder.metadata().num_row_groups();
    if selected_row_groups
        .iter()
        .any(|value| *value >= row_group_count)
    {
        return Err("row group selection is out of range".to_string());
    }
    let selected_total_rows = if selected_row_groups.is_empty() {
        builder.metadata().file_metadata().num_rows() as usize
    } else {
        selected_row_groups
            .iter()
            .map(|index| builder.metadata().row_group(*index).num_rows() as usize)
            .sum()
    };
    if !selected_row_groups.is_empty() {
        builder = builder.with_row_groups(selected_row_groups.clone());
    }
    let selected_row_ranges = request.row_ranges.unwrap_or_default();
    let requested_rows = if selected_row_ranges.is_empty() {
        selected_total_rows
    } else {
        let ranges = validate_row_ranges(&selected_row_ranges, selected_total_rows)?;
        let rows = ranges.iter().map(|range| range.end - range.start).sum();
        builder = builder.with_row_selection(RowSelection::from_consecutive_ranges(
            ranges.into_iter(),
            selected_total_rows,
        ));
        rows
    };
    let projected_columns = request.projection.unwrap_or_else(|| {
        builder
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    });
    if projected_columns.is_empty() {
        return Err("projection must contain at least one column".to_string());
    }
    let mut indexes = Vec::with_capacity(projected_columns.len());
    for name in &projected_columns {
        let index = builder
            .schema()
            .index_of(name)
            .map_err(|_| format!("projection column is missing: {name}"))?;
        indexes.push(index);
    }
    let projected_schema = builder.schema().project(&indexes).map_err(safe_error)?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), indexes);
    let mut stream = builder.with_projection(mask).build().map_err(safe_error)?;
    let output = File::create(&request.output_ipc_path).map_err(safe_error)?;
    let mut writer: Option<FileWriter<File>> = None;
    let mut rows = 0usize;
    let mut batches = 0usize;
    while let Some(batch) = stream.try_next().await.map_err(safe_error)? {
        if writer.is_none() {
            writer = Some(
                FileWriter::try_new(
                    output.try_clone().map_err(safe_error)?,
                    batch.schema().as_ref(),
                )
                .map_err(safe_error)?,
            );
        }
        writer.as_mut().unwrap().write(&batch).map_err(safe_error)?;
        rows += batch.num_rows();
        batches += 1;
    }
    if let Some(mut writer) = writer {
        writer.finish().map_err(safe_error)?;
    } else {
        let mut empty_writer =
            FileWriter::try_new(output, &projected_schema).map_err(safe_error)?;
        empty_writer.finish().map_err(safe_error)?;
    }
    let output_bytes = std::fs::metadata(&request.output_ipc_path)
        .map_err(safe_error)?
        .len();
    let (range_count, requested_range_bytes, received_range_bytes) = range_counters.snapshot();
    serde_json::to_string(&ParquetReadResult {
        schema_version: "smoking-data.s3-parquet-read.v1",
        rows,
        batches,
        output_bytes,
        projected_columns,
        selected_row_groups,
        selected_row_ranges,
        requested_rows,
        range_backend: "rust-object_store",
        range_count,
        requested_range_bytes,
        received_range_bytes,
    })
    .map_err(safe_error)
}

fn validate_row_ranges(
    values: &[RowRange],
    total_rows: usize,
) -> Result<Vec<std::ops::Range<usize>>, String> {
    let mut previous_end = 0usize;
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        if value.start >= value.end_exclusive || value.end_exclusive > total_rows {
            return Err("row range is empty or outside the selected row groups".to_string());
        }
        if !result.is_empty() && value.start < previous_end {
            return Err("row ranges must be sorted and non-overlapping".to_string());
        }
        previous_end = value.end_exclusive;
        result.push(value.start..value.end_exclusive);
    }
    Ok(result)
}

fn build_store(config: S3Config) -> Result<Arc<dyn ObjectStore>, String> {
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(config.bucket);
    if let Some(region) = config.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = config.endpoint_url {
        let allow_http = endpoint.starts_with("http://");
        builder = builder.with_endpoint(endpoint).with_allow_http(allow_http);
    }
    builder = builder.with_virtual_hosted_style_request(!config.path_style.unwrap_or(false));
    match (config.access_key_id, config.secret_access_key) {
        (Some(access), Some(secret)) => {
            builder = builder
                .with_access_key_id(access)
                .with_secret_access_key(secret);
        }
        (None, None) => {}
        _ => return Err("S3 access key and secret key must be provided together".to_string()),
    }
    if let Some(token) = config.session_token {
        builder = builder.with_token(token);
    }
    builder
        .build()
        .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
        .map_err(safe_error)
}

fn safe_error(error: impl std::fmt::Display) -> String {
    // Do not return request URLs or provider errors that may contain signed query parameters.
    let message = error.to_string();
    if message.contains("X-Amz-") || message.contains("Signature=") {
        "remote object-store request failed (details redacted)".to_string()
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_row_ranges, RowRange};

    #[test]
    fn validates_bounded_sorted_row_ranges() {
        let ranges = validate_row_ranges(
            &[
                RowRange {
                    start: 1,
                    end_exclusive: 3,
                },
                RowRange {
                    start: 5,
                    end_exclusive: 6,
                },
            ],
            8,
        )
        .unwrap();
        assert_eq!(ranges, vec![1..3, 5..6]);
    }

    #[test]
    fn rejects_overlapping_or_out_of_bounds_row_ranges() {
        assert!(validate_row_ranges(
            &[
                RowRange {
                    start: 1,
                    end_exclusive: 4,
                },
                RowRange {
                    start: 3,
                    end_exclusive: 5,
                },
            ],
            8,
        )
        .is_err());
        assert!(validate_row_ranges(
            &[RowRange {
                start: 7,
                end_exclusive: 9,
            }],
            8,
        )
        .is_err());
    }
}
