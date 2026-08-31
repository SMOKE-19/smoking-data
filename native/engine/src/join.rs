#[cfg(feature = "polars-join-experiment")]
use crate::polars_bridge::{apache_to_polars, polars_to_apache};
use crate::post_operations::{execute_post_operations, requires_complete_input, PostOperation};
use arrow_array::{
    new_null_array, Array, ArrayRef, BooleanArray, LargeStringArray, RecordBatch, StringArray,
    UInt32Array,
};
use arrow_cast::display::array_value_to_string;
use arrow_ipc::{reader::FileReader as IpcFileReader, writer::FileWriter as IpcFileWriter};
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::{concat::concat_batches, take::take, zip::zip};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
#[cfg(feature = "polars-join-experiment")]
use polars_core::prelude::{NamedFrom, PlSmallStr, Series};
#[cfg(feature = "polars-join-experiment")]
use polars_ops::frame::join::{
    DataFrameJoinOps, JoinArgs, JoinCoalesce, JoinType, MaintainOrderJoin,
};
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Deserialize)]
struct JoinTaskConfig {
    left_files: Vec<String>,
    #[serde(default)]
    left_row_groups: HashMap<String, Vec<usize>>,
    #[serde(default)]
    left_columns: ColumnPolicy,
    right_sources: Vec<RightSource>,
    partition_value: String,
    left_partition_key: String,
    #[serde(default)]
    right_partition_key: String,
    output_partition_column: String,
    output_path: String,
    #[serde(default)]
    key_rows: Vec<HashMap<String, Value>>,
    #[serde(default = "default_true")]
    left_key_filter_required: bool,
    output_row_group_rows: Option<usize>,
    input_batch_rows: Option<usize>,
    bounded_join: Option<bool>,
    compression: Option<String>,
    #[serde(default)]
    ordered_operations: Vec<OrderedOperation>,
    #[serde(default)]
    post_operations: Vec<PostOperation>,
    #[serde(default)]
    join_backend: JoinBackend,
    #[serde(default)]
    right_staging_mode: RightStagingMode,
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum JoinBackend {
    #[default]
    ArrowNative,
    Polars,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RightStagingMode {
    Off,
    #[default]
    Auto,
    Force,
}

#[derive(Debug, Default)]
struct RightStagingProfile {
    staged_sources: usize,
    skipped_sources: usize,
    input_rows: usize,
    output_rows: usize,
    peak_input_batch_rows: usize,
    peak_staged_bytes: u64,
    write_sec: f64,
    read_sec: f64,
    filter_sec: f64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderedOperation {
    operation_id: String,
    kind: String,
    execution_target: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ColumnPolicy {
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    regex: Vec<String>,
}

#[derive(Debug, Default)]
struct ReadProjectionProfile {
    sources: usize,
    total_columns: usize,
    projected_columns: usize,
}

impl ReadProjectionProfile {
    fn accumulate(&mut self, other: &Self) {
        self.sources += other.sources;
        self.total_columns += other.total_columns;
        self.projected_columns += other.projected_columns;
    }
}

#[derive(Debug, Default)]
struct ParquetBatchReadProfile {
    reader_setup_sec: f64,
    decode_sec: f64,
    schema_align_sec: f64,
    batches: usize,
    rows: usize,
    compressed_bytes: usize,
}

impl ParquetBatchReadProfile {
    fn measured_sec(&self) -> f64 {
        self.reader_setup_sec + self.decode_sec + self.schema_align_sec
    }
}

#[derive(Clone, Debug, Deserialize)]
struct RightSource {
    name: String,
    files: Vec<String>,
    #[serde(default)]
    row_groups: HashMap<String, Vec<usize>>,
    #[serde(default)]
    columns: ColumnPolicy,
    left_on: Vec<String>,
    right_on: Vec<String>,
    how: String,
    suffix: String,
    #[serde(default)]
    keep_right_partition_column: bool,
    staging_estimated_match_rows: Option<usize>,
}

#[derive(Debug)]
struct JoinResult {
    batch: RecordBatch,
    matched_rows: usize,
    bridge_profile: BridgeProfile,
    kernel_profile: JoinKernelProfile,
}

pub(crate) fn load_lookup_projection(
    files: &[String],
    lookup_keys: &[String],
    value_columns: &[(String, String)],
    alias: &str,
) -> Result<RecordBatch, String> {
    if files.is_empty() || lookup_keys.is_empty() {
        return Err(format!("Lookup {alias} requires files and composite keys"));
    }
    let required = lookup_keys
        .iter()
        .cloned()
        .chain(value_columns.iter().map(|(source, _)| source.clone()))
        .collect::<Vec<_>>();
    let (batch, _, _) =
        read_parquet_union_projected(files, None, &ColumnPolicy::default(), &required, alias)?;
    let mut fields = Vec::with_capacity(required.len());
    let mut arrays = Vec::with_capacity(required.len());
    for key in lookup_keys {
        let array = batch
            .column_by_name(key)
            .ok_or_else(|| format!("Lookup {alias} key column not found: {key}"))?;
        fields.push(Arc::new(Field::new(
            key,
            array.data_type().clone(),
            array.null_count() > 0,
        )));
        arrays.push(Arc::clone(array));
    }
    for (source, output) in value_columns {
        let array = batch
            .column_by_name(source)
            .ok_or_else(|| format!("Lookup {alias} value column not found: {source}"))?;
        fields.push(Arc::new(Field::new(
            output,
            array.data_type().clone(),
            true,
        )));
        arrays.push(Arc::clone(array));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}

pub(crate) fn enrich_many_to_one_lookup(
    left: RecordBatch,
    right: &RecordBatch,
    source_keys: &[String],
    lookup_keys: &[String],
    alias: &str,
) -> Result<RecordBatch, String> {
    let source = RightSource {
        name: alias.to_string(),
        files: Vec::new(),
        row_groups: HashMap::new(),
        columns: ColumnPolicy::default(),
        left_on: source_keys.to_vec(),
        right_on: lookup_keys.to_vec(),
        how: "left".to_string(),
        suffix: format!(".{alias}"),
        keep_right_partition_column: false,
        staging_estimated_match_rows: None,
    };
    join_batches(left, right.clone(), &source, None, JoinBackend::ArrowNative)
        .map(|result| result.batch)
}

#[derive(Debug, Default)]
struct JoinKernelProfile {
    right_key_map_build_sec: f64,
    left_key_encode_sec: f64,
    hash_probe_sec: f64,
    result_materialize_sec: f64,
    join_index_array_build_sec: f64,
    left_take_sec: f64,
    join_key_zip_sec: f64,
    right_take_sec: f64,
    record_batch_build_sec: f64,
    left_identity_reuse_batches: usize,
    right_identity_reuse_batches: usize,
}

impl JoinKernelProfile {
    fn accumulate(&mut self, other: &Self) {
        self.right_key_map_build_sec += other.right_key_map_build_sec;
        self.left_key_encode_sec += other.left_key_encode_sec;
        self.hash_probe_sec += other.hash_probe_sec;
        self.result_materialize_sec += other.result_materialize_sec;
        self.join_index_array_build_sec += other.join_index_array_build_sec;
        self.left_take_sec += other.left_take_sec;
        self.join_key_zip_sec += other.join_key_zip_sec;
        self.right_take_sec += other.right_take_sec;
        self.record_batch_build_sec += other.record_batch_build_sec;
        self.left_identity_reuse_batches += other.left_identity_reuse_batches;
        self.right_identity_reuse_batches += other.right_identity_reuse_batches;
    }

    fn measured_sec(&self) -> f64 {
        self.right_key_map_build_sec
            + self.left_key_encode_sec
            + self.hash_probe_sec
            + self.result_materialize_sec
    }

    fn measured_materialize_sec(&self) -> f64 {
        self.join_index_array_build_sec
            + self.left_take_sec
            + self.join_key_zip_sec
            + self.right_take_sec
            + self.record_batch_build_sec
    }
}

#[derive(Debug, Default)]
pub(crate) struct BridgeProfile {
    pub apache_input_bytes: usize,
    pub polars_input_estimated_bytes: usize,
    pub polars_output_estimated_bytes: usize,
    pub apache_output_bytes: usize,
    pub import_sec: f64,
    pub join_sec: f64,
    pub export_sec: f64,
}

impl BridgeProfile {
    pub fn accumulate(&mut self, other: &BridgeProfile) {
        self.apache_input_bytes += other.apache_input_bytes;
        self.polars_input_estimated_bytes += other.polars_input_estimated_bytes;
        self.polars_output_estimated_bytes += other.polars_output_estimated_bytes;
        self.apache_output_bytes += other.apache_output_bytes;
        self.import_sec += other.import_sec;
        self.join_sec += other.join_sec;
        self.export_sec += other.export_sec;
    }
}

type EncodedJoinKey = Box<[u8]>;
type RightKeyMap = HashMap<EncodedJoinKey, Vec<u32>>;
type DisplayRightKeyMap = HashMap<String, Vec<u32>>;

const BINARY_ROW_MIN_BATCH_ROWS: usize = 8_192;

enum PreparedRightKeyMap {
    Pending,
    Binary {
        map: RightKeyMap,
        converter: RowConverter,
        scratch_rows: Rows,
    },
    Display {
        map: DisplayRightKeyMap,
    },
}

impl PreparedRightKeyMap {
    fn unique_keys(&self) -> usize {
        match self {
            Self::Pending => 0,
            Self::Binary { map, .. } => map.len(),
            Self::Display { map } => map.len(),
        }
    }

    fn indexed_rows(&self) -> usize {
        match self {
            Self::Pending => 0,
            Self::Binary { map, .. } => map.values().map(Vec::len).sum(),
            Self::Display { map } => map.values().map(Vec::len).sum(),
        }
    }
}

#[derive(Debug, Default)]
struct JoinIndices {
    left: Vec<Option<u32>>,
    right: Vec<Option<u32>>,
    matched_rows: usize,
}

impl JoinIndices {
    fn with_capacity(rows: usize) -> Self {
        Self {
            left: Vec::with_capacity(rows),
            right: Vec::with_capacity(rows),
            matched_rows: 0,
        }
    }
}

fn indices_are_identity(indices: &[Option<u32>], source_rows: usize) -> bool {
    indices.len() == source_rows
        && indices
            .iter()
            .enumerate()
            .all(|(index, value)| *value == Some(index as u32))
}

pub fn execute_join_task_json(task_json: &str) -> Result<HashMap<String, f64>, String> {
    let config: JoinTaskConfig =
        serde_json::from_str(task_json).map_err(|error| format!("invalid join task: {error}"))?;
    validate_task(&config)?;

    if supports_bounded_join(&config) {
        execute_join_task_bounded(&config)
    } else {
        execute_join_task_eager(&config)
    }
}

fn execute_join_task_eager(config: &JoinTaskConfig) -> Result<HashMap<String, f64>, String> {
    let (mut current, mut row_groups_touched) =
        read_parquet_union(&config.left_files, Some(&config.left_row_groups))?;
    current = select_columns(
        &current,
        &config.left_columns,
        std::slice::from_ref(&config.left_partition_key),
        "left",
    )?;
    current = filter_column_value(
        &current,
        &config.left_partition_key,
        &config.partition_value,
    )?;
    if !config.key_rows.is_empty()
        && !config.right_sources[0].left_on.is_empty()
        && config.left_key_filter_required
    {
        current = filter_key_rows(&current, &config.right_sources[0].left_on, &config.key_rows)?.0;
    }
    let left_rows = current.num_rows();
    let mut matched_rows = 0usize;
    let mut right_rows = 0usize;
    let mut source_files = config.left_files.len();
    let mut bridge_profile = BridgeProfile::default();

    for source in &config.right_sources {
        source_files += source.files.len();
        let (mut right, touched) = read_parquet_union(&source.files, Some(&source.row_groups))?;
        row_groups_touched += touched;
        let mut required = source.right_on.clone();
        if !config.right_partition_key.is_empty() {
            required.push(config.right_partition_key.clone());
        }
        right = select_columns(&right, &source.columns, &required, &source.name)?;
        if !config.right_partition_key.is_empty() {
            right =
                filter_column_value(&right, &config.right_partition_key, &config.partition_value)?;
        }
        right_rows += right.num_rows();
        let partition_helper = (!source.keep_right_partition_column
            && !config.right_partition_key.is_empty())
        .then_some(config.right_partition_key.as_str());
        let result = join_batches(
            current,
            right,
            source,
            partition_helper,
            config.join_backend,
        )?;
        current = result.batch;
        matched_rows += result.matched_rows;
        bridge_profile.accumulate(&result.bridge_profile);
    }

    if !config.post_operations.is_empty() {
        current = execute_post_operations(current, &config.post_operations)?;
    }
    current = set_partition_column(
        &current,
        &config.output_partition_column,
        &config.partition_value,
    )?;
    write_parquet_atomic(
        &current,
        Path::new(&config.output_path),
        config.output_row_group_rows,
        config.compression.as_deref(),
    )?;
    Ok(task_counters(
        config,
        left_rows,
        right_rows,
        current.num_rows(),
        matched_rows,
        source_files,
        row_groups_touched,
        0,
        0,
        0,
        bridge_profile,
    ))
}

#[allow(clippy::too_many_arguments)]
fn task_counters(
    config: &JoinTaskConfig,
    left_rows: usize,
    right_rows: usize,
    output_rows: usize,
    matched_rows: usize,
    source_files: usize,
    row_groups_touched: usize,
    bounded_input_batches: usize,
    peak_input_batch_rows: usize,
    peak_output_batch_rows: usize,
    bridge_profile: BridgeProfile,
) -> HashMap<String, f64> {
    HashMap::from([
        ("left_rows".to_string(), left_rows as f64),
        ("right_rows".to_string(), right_rows as f64),
        ("output_rows".to_string(), output_rows as f64),
        ("matched_rows".to_string(), matched_rows as f64),
        ("source_files_touched".to_string(), source_files as f64),
        ("row_groups_touched".to_string(), row_groups_touched as f64),
        (
            "ordered_operation_count".to_string(),
            config.ordered_operations.len() as f64,
        ),
        (
            "bounded_join_enabled".to_string(),
            f64::from(bounded_input_batches > 0),
        ),
        (
            "bounded_input_batches".to_string(),
            bounded_input_batches as f64,
        ),
        (
            "configured_input_batch_rows".to_string(),
            config.input_batch_rows.unwrap_or(65_536) as f64,
        ),
        (
            "peak_input_batch_rows".to_string(),
            peak_input_batch_rows as f64,
        ),
        (
            "peak_output_batch_rows".to_string(),
            peak_output_batch_rows as f64,
        ),
        ("right_key_index_builds".to_string(), 0.0),
        ("right_key_index_unique_keys".to_string(), 0.0),
        ("right_key_index_rows".to_string(), 0.0),
        ("right_key_index_reuses".to_string(), 0.0),
        ("binary_row_key_sources".to_string(), 0.0),
        ("display_key_sources".to_string(), 0.0),
        ("right_read_sec".to_string(), 0.0),
        ("right_staging_sources".to_string(), 0.0),
        ("right_staging_skipped_sources".to_string(), 0.0),
        ("right_staging_input_rows".to_string(), 0.0),
        ("right_staging_output_rows".to_string(), 0.0),
        ("right_staging_peak_input_batch_rows".to_string(), 0.0),
        ("right_staging_peak_bytes".to_string(), 0.0),
        ("right_staging_write_sec".to_string(), 0.0),
        ("right_staging_read_sec".to_string(), 0.0),
        ("right_staging_filter_sec".to_string(), 0.0),
        ("left_schema_sec".to_string(), 0.0),
        ("left_read_sec".to_string(), 0.0),
        ("left_reader_setup_sec".to_string(), 0.0),
        ("left_parquet_decode_sec".to_string(), 0.0),
        ("left_schema_align_sec".to_string(), 0.0),
        ("left_read_unattributed_sec".to_string(), 0.0),
        ("left_preprocess_sec".to_string(), 0.0),
        ("left_select_sec".to_string(), 0.0),
        ("left_partition_filter_sec".to_string(), 0.0),
        ("left_key_filter_sec".to_string(), 0.0),
        ("left_key_filter_identity_batches".to_string(), 0.0),
        ("left_key_filter_skipped_batches".to_string(), 0.0),
        ("left_preprocess_unattributed_sec".to_string(), 0.0),
        ("join_compute_sec".to_string(), 0.0),
        ("right_key_map_build_sec".to_string(), 0.0),
        ("left_key_encode_sec".to_string(), 0.0),
        ("hash_probe_sec".to_string(), 0.0),
        ("result_materialize_sec".to_string(), 0.0),
        ("join_kernel_unattributed_sec".to_string(), 0.0),
        ("join_index_array_build_sec".to_string(), 0.0),
        ("left_take_sec".to_string(), 0.0),
        ("join_key_zip_sec".to_string(), 0.0),
        ("right_take_sec".to_string(), 0.0),
        ("record_batch_build_sec".to_string(), 0.0),
        ("result_materialize_unattributed_sec".to_string(), 0.0),
        ("left_identity_reuse_batches".to_string(), 0.0),
        ("right_identity_reuse_batches".to_string(), 0.0),
        ("post_operation_sec".to_string(), 0.0),
        ("parquet_write_sec".to_string(), 0.0),
        ("writer_close_sec".to_string(), 0.0),
        ("output_commit_sec".to_string(), 0.0),
        ("writer_dictionary_disabled_columns".to_string(), 0.0),
        ("parquet_projection_sources".to_string(), 0.0),
        ("parquet_total_columns".to_string(), 0.0),
        ("parquet_projected_columns".to_string(), 0.0),
        (
            "join_backend_polars".to_string(),
            f64::from(config.join_backend == JoinBackend::Polars),
        ),
        (
            "polars_bridge_apache_input_bytes".to_string(),
            bridge_profile.apache_input_bytes as f64,
        ),
        (
            "polars_bridge_frame_input_bytes".to_string(),
            bridge_profile.polars_input_estimated_bytes as f64,
        ),
        (
            "polars_join_output_bytes".to_string(),
            bridge_profile.polars_output_estimated_bytes as f64,
        ),
        (
            "polars_bridge_apache_output_bytes".to_string(),
            bridge_profile.apache_output_bytes as f64,
        ),
        (
            "polars_bridge_import_sec".to_string(),
            bridge_profile.import_sec,
        ),
        ("polars_join_sec".to_string(), bridge_profile.join_sec),
        (
            "polars_bridge_export_sec".to_string(),
            bridge_profile.export_sec,
        ),
    ])
}

fn supports_bounded_join(config: &JoinTaskConfig) -> bool {
    config.bounded_join.unwrap_or(true)
        && config.join_backend == JoinBackend::ArrowNative
        && config
            .right_sources
            .iter()
            .all(|source| matches!(source.how.as_str(), "inner" | "left"))
        && !requires_complete_input(&config.post_operations)
        && config
            .post_operations
            .iter()
            .all(|operation| operation.kind != "unpivot")
}

fn mapped_right_key_rows(
    source: &RightSource,
    key_rows: &[HashMap<String, Value>],
) -> Option<Vec<HashMap<String, Value>>> {
    if key_rows.is_empty()
        || source.left_on.is_empty()
        || source.left_on.len() != source.right_on.len()
        || !key_rows
            .iter()
            .all(|row| source.left_on.iter().all(|name| row.contains_key(name)))
    {
        return None;
    }
    Some(
        key_rows
            .iter()
            .map(|row| {
                source
                    .left_on
                    .iter()
                    .zip(&source.right_on)
                    .map(|(left, right)| (right.clone(), row[left].clone()))
                    .collect()
            })
            .collect(),
    )
}

fn selected_parquet_rows(
    paths: &[String],
    selected_row_groups: Option<&HashMap<String, Vec<usize>>>,
) -> Result<usize, String> {
    let mut rows = 0usize;
    for path in paths {
        let file = File::open(path).map_err(|error| format!("open {path}: {error}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| format!("read parquet metadata {path}: {error}"))?;
        let row_groups = selected_row_groups
            .and_then(|items| items.get(path))
            .cloned()
            .unwrap_or_else(|| (0..builder.metadata().num_row_groups()).collect());
        for index in row_groups {
            let row_group = builder.metadata().row_groups().get(index).ok_or_else(|| {
                format!(
                    "row group {index} exceeds row group count {} for {path}",
                    builder.metadata().num_row_groups()
                )
            })?;
            rows = rows.saturating_add(row_group.num_rows().max(0) as usize);
        }
    }
    Ok(rows)
}

fn should_stage_right_source(
    config: &JoinTaskConfig,
    source: &RightSource,
    input_batch_rows: usize,
) -> Result<Option<Vec<HashMap<String, Value>>>, String> {
    if config.right_staging_mode == RightStagingMode::Off {
        return Ok(None);
    }
    let Some(mapped_keys) = mapped_right_key_rows(source, &config.key_rows) else {
        return Ok(None);
    };
    if config.right_staging_mode == RightStagingMode::Force {
        return Ok(Some(mapped_keys));
    }
    let candidate_rows = selected_parquet_rows(&source.files, Some(&source.row_groups))?;
    let Some(estimated_match_rows) = source.staging_estimated_match_rows else {
        return Ok(None);
    };
    Ok((candidate_rows > input_batch_rows
        && estimated_match_rows.saturating_mul(2) <= candidate_rows)
        .then_some(mapped_keys))
}

fn stage_filtered_right_to_ipc(
    config: &JoinTaskConfig,
    source: &RightSource,
    source_index: usize,
    input_batch_rows: usize,
    required: &[String],
    mapped_keys: &[HashMap<String, Value>],
) -> Result<
    (
        RecordBatch,
        usize,
        ReadProjectionProfile,
        RightStagingProfile,
    ),
    String,
> {
    let (full_schema, row_groups_touched) =
        parquet_union_schema(&source.files, Some(&source.row_groups))?;
    let selected = selected_column_names(
        full_schema.as_ref(),
        &source.columns,
        required,
        &source.name,
    )?;
    let schema = projected_schema(full_schema.as_ref(), &selected)?;
    let projection_names = selected.iter().cloned().collect::<HashSet<_>>();
    let output_path = Path::new(&config.output_path);
    let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| {
        format!(
            "create right staging directory {}: {error}",
            parent.display()
        )
    })?;
    let output_name = output_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("join-output");
    let staging_path = parent.join(format!(
        ".{output_name}.right-{source_index}-{}.arrow",
        std::process::id()
    ));
    let _ = fs::remove_file(&staging_path);
    let result = (|| {
        let file = File::create(&staging_path).map_err(|error| {
            format!(
                "create right Arrow staging {}: {error}",
                staging_path.display()
            )
        })?;
        let mut writer = IpcFileWriter::try_new(file, &schema).map_err(|error| {
            format!(
                "initialize right Arrow staging {}: {error}",
                staging_path.display()
            )
        })?;
        let mut profile = RightStagingProfile {
            staged_sources: 1,
            ..RightStagingProfile::default()
        };
        let write_started = Instant::now();
        for_each_parquet_batch(
            &source.files,
            Some(&source.row_groups),
            input_batch_rows,
            &schema,
            Some(&projection_names),
            |batch| {
                profile.input_rows = profile.input_rows.saturating_add(batch.num_rows());
                profile.peak_input_batch_rows = profile.peak_input_batch_rows.max(batch.num_rows());
                let filter_started = Instant::now();
                let mut filtered = batch;
                if !config.right_partition_key.is_empty() {
                    filtered = filter_column_value(
                        &filtered,
                        &config.right_partition_key,
                        &config.partition_value,
                    )?;
                }
                filtered = filter_key_rows(&filtered, &source.right_on, mapped_keys)?.0;
                profile.filter_sec += filter_started.elapsed().as_secs_f64();
                profile.output_rows = profile.output_rows.saturating_add(filtered.num_rows());
                if filtered.num_rows() > 0 {
                    writer.write(&filtered).map_err(|error| {
                        format!(
                            "write right Arrow staging {}: {error}",
                            staging_path.display()
                        )
                    })?;
                }
                Ok(())
            },
        )?;
        writer.finish().map_err(|error| {
            format!(
                "finish right Arrow staging {}: {error}",
                staging_path.display()
            )
        })?;
        drop(writer);
        profile.write_sec = write_started.elapsed().as_secs_f64();
        profile.peak_staged_bytes = fs::metadata(&staging_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);

        let read_started = Instant::now();
        let file = File::open(&staging_path).map_err(|error| {
            format!(
                "open right Arrow staging {}: {error}",
                staging_path.display()
            )
        })?;
        let reader = IpcFileReader::try_new(file, None).map_err(|error| {
            format!(
                "read right Arrow staging {}: {error}",
                staging_path.display()
            )
        })?;
        let batches = reader
            .map(|batch| batch.map_err(|error| error.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = if batches.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            concat_batches(&schema, &batches).map_err(|error| error.to_string())?
        };
        profile.read_sec = read_started.elapsed().as_secs_f64();
        Ok((
            batch,
            row_groups_touched,
            ReadProjectionProfile {
                sources: source.files.len(),
                total_columns: full_schema
                    .fields()
                    .len()
                    .saturating_mul(source.files.len()),
                projected_columns: selected.len().saturating_mul(source.files.len()),
            },
            profile,
        ))
    })();
    let _ = fs::remove_file(&staging_path);
    result
}

fn execute_join_task_bounded(config: &JoinTaskConfig) -> Result<HashMap<String, f64>, String> {
    let input_batch_rows = config.input_batch_rows.unwrap_or(65_536).max(1);
    let mut right_batches = Vec::with_capacity(config.right_sources.len());
    let mut right_key_maps = Vec::with_capacity(config.right_sources.len());
    let mut right_rows = 0usize;
    let mut source_files = config.left_files.len();
    let mut row_groups_touched = 0usize;
    let mut projection_profile = ReadProjectionProfile::default();
    let mut right_staging_profile = RightStagingProfile::default();
    let mut right_read_sec = 0.0;
    for (source_index, source) in config.right_sources.iter().enumerate() {
        let right_read_started = Instant::now();
        source_files += source.files.len();
        let mut required = source.right_on.clone();
        if !config.right_partition_key.is_empty() {
            required.push(config.right_partition_key.clone());
        }
        let staged_keys = should_stage_right_source(config, source, input_batch_rows)?;
        let staging_enabled = staged_keys.is_some();
        let (mut right, touched, source_projection) = if let Some(mapped_keys) = staged_keys {
            let (right, touched, projection, profile) = stage_filtered_right_to_ipc(
                config,
                source,
                source_index,
                input_batch_rows,
                &required,
                &mapped_keys,
            )?;
            right_staging_profile.staged_sources += profile.staged_sources;
            right_staging_profile.input_rows += profile.input_rows;
            right_staging_profile.output_rows += profile.output_rows;
            right_staging_profile.peak_input_batch_rows = right_staging_profile
                .peak_input_batch_rows
                .max(profile.peak_input_batch_rows);
            right_staging_profile.peak_staged_bytes = right_staging_profile
                .peak_staged_bytes
                .max(profile.peak_staged_bytes);
            right_staging_profile.write_sec += profile.write_sec;
            right_staging_profile.read_sec += profile.read_sec;
            right_staging_profile.filter_sec += profile.filter_sec;
            (right, touched, projection)
        } else {
            right_staging_profile.skipped_sources += 1;
            read_parquet_union_projected(
                &source.files,
                Some(&source.row_groups),
                &source.columns,
                &required,
                &source.name,
            )?
        };
        row_groups_touched += touched;
        projection_profile.accumulate(&source_projection);
        if !staging_enabled && !config.right_partition_key.is_empty() {
            right =
                filter_column_value(&right, &config.right_partition_key, &config.partition_value)?;
        }
        right_rows += right.num_rows();
        right_key_maps.push(PreparedRightKeyMap::Pending);
        right_batches.push(right);
        right_read_sec += right_read_started.elapsed().as_secs_f64();
    }

    let left_schema_started = Instant::now();
    let (full_left_schema, left_row_groups_touched) =
        parquet_union_schema(&config.left_files, Some(&config.left_row_groups))?;
    row_groups_touched += left_row_groups_touched;
    let mut required_left = vec![config.left_partition_key.clone()];
    for (source_index, source) in config.right_sources.iter().enumerate() {
        for name in &source.left_on {
            let originates_from_left = full_left_schema.field_with_name(name).is_ok();
            if (source_index == 0 || originates_from_left) && !required_left.contains(name) {
                required_left.push(name.clone());
            }
        }
    }
    let selected_left_names = selected_column_names(
        full_left_schema.as_ref(),
        &config.left_columns,
        &required_left,
        "left",
    )?;
    let left_schema = projected_schema(full_left_schema.as_ref(), &selected_left_names)?;
    let left_projection_names = selected_left_names.iter().cloned().collect::<HashSet<_>>();
    projection_profile.accumulate(&ReadProjectionProfile {
        sources: config.left_files.len(),
        total_columns: full_left_schema
            .fields()
            .len()
            .saturating_mul(config.left_files.len()),
        projected_columns: selected_left_names
            .len()
            .saturating_mul(config.left_files.len()),
    });
    let left_schema_sec = left_schema_started.elapsed().as_secs_f64();
    let mut writer = AtomicParquetBatchWriter::new(
        Path::new(&config.output_path),
        config.output_row_group_rows,
        config.compression.as_deref(),
    )?;
    let mut left_rows = 0usize;
    let mut output_rows = 0usize;
    let mut matched_rows = 0usize;
    let mut bounded_input_batches = 0usize;
    let mut peak_input_batch_rows = 0usize;
    let mut peak_output_batch_rows = 0usize;
    let mut bridge_profile = BridgeProfile::default();
    let mut callback_sec = 0.0;
    let mut left_preprocess_sec = 0.0;
    let mut left_select_sec = 0.0;
    let mut left_partition_filter_sec = 0.0;
    let mut left_key_filter_sec = 0.0;
    let mut left_key_filter_identity_batches = 0usize;
    let mut left_key_filter_skipped_batches = 0usize;
    let mut join_compute_sec = 0.0;
    let mut join_kernel_profile = JoinKernelProfile::default();
    let mut post_operation_sec = 0.0;
    let mut parquet_write_sec = 0.0;

    let left_stream_started = Instant::now();
    let left_read_profile = for_each_parquet_batch(
        &config.left_files,
        Some(&config.left_row_groups),
        input_batch_rows,
        &left_schema,
        Some(&left_projection_names),
        |batch| {
            let callback_started = Instant::now();
            bounded_input_batches += 1;
            peak_input_batch_rows = peak_input_batch_rows.max(batch.num_rows());
            let preprocess_started = Instant::now();
            let select_started = Instant::now();
            let mut current = select_columns(
                &batch,
                &config.left_columns,
                std::slice::from_ref(&config.left_partition_key),
                "left",
            )?;
            left_select_sec += select_started.elapsed().as_secs_f64();
            let partition_filter_started = Instant::now();
            current = filter_column_value(
                &current,
                &config.left_partition_key,
                &config.partition_value,
            )?;
            left_partition_filter_sec += partition_filter_started.elapsed().as_secs_f64();
            if !config.key_rows.is_empty()
                && !config.right_sources[0].left_on.is_empty()
                && config.left_key_filter_required
            {
                let key_filter_started = Instant::now();
                let (filtered, identity) =
                    filter_key_rows(&current, &config.right_sources[0].left_on, &config.key_rows)?;
                current = filtered;
                left_key_filter_identity_batches += usize::from(identity);
                left_key_filter_sec += key_filter_started.elapsed().as_secs_f64();
            } else if !config.key_rows.is_empty() && !config.right_sources[0].left_on.is_empty() {
                left_key_filter_skipped_batches += 1;
            }
            left_rows += current.num_rows();
            left_preprocess_sec += preprocess_started.elapsed().as_secs_f64();
            let join_started = Instant::now();
            for ((source, right), right_key_map) in config
                .right_sources
                .iter()
                .zip(&right_batches)
                .zip(&mut right_key_maps)
            {
                let partition_helper = (!source.keep_right_partition_column
                    && !config.right_partition_key.is_empty())
                .then_some(config.right_partition_key.as_str());
                let result = join_batches_with_right_key_map(
                    current,
                    right.clone(),
                    source,
                    partition_helper,
                    JoinBackend::ArrowNative,
                    Some(right_key_map),
                )?;
                current = result.batch;
                matched_rows += result.matched_rows;
                bridge_profile.accumulate(&result.bridge_profile);
                join_kernel_profile.accumulate(&result.kernel_profile);
            }
            join_compute_sec += join_started.elapsed().as_secs_f64();
            let post_started = Instant::now();
            if !config.post_operations.is_empty() {
                current = execute_post_operations(current, &config.post_operations)?;
            }
            current = set_partition_column(
                &current,
                &config.output_partition_column,
                &config.partition_value,
            )?;
            post_operation_sec += post_started.elapsed().as_secs_f64();
            peak_output_batch_rows = peak_output_batch_rows.max(current.num_rows());
            output_rows += current.num_rows();
            let write_started = Instant::now();
            writer.write(&current)?;
            parquet_write_sec += write_started.elapsed().as_secs_f64();
            callback_sec += callback_started.elapsed().as_secs_f64();
            Ok(())
        },
    )?;
    let left_stream_sec = left_stream_started.elapsed().as_secs_f64();
    let left_read_sec = (left_stream_sec - callback_sec).max(0.0);
    let finish_profile = writer.finish()?;

    let mut counters = task_counters(
        config,
        left_rows,
        right_rows,
        output_rows,
        matched_rows,
        source_files,
        row_groups_touched,
        bounded_input_batches,
        peak_input_batch_rows,
        peak_output_batch_rows,
        bridge_profile,
    );
    counters.insert(
        "right_key_index_builds".to_string(),
        right_key_maps.len() as f64,
    );
    counters.insert(
        "right_key_index_unique_keys".to_string(),
        right_key_maps
            .iter()
            .map(PreparedRightKeyMap::unique_keys)
            .sum::<usize>() as f64,
    );
    counters.insert(
        "right_key_index_rows".to_string(),
        right_key_maps
            .iter()
            .map(PreparedRightKeyMap::indexed_rows)
            .sum::<usize>() as f64,
    );
    counters.insert(
        "binary_row_key_sources".to_string(),
        right_key_maps
            .iter()
            .filter(|prepared| matches!(prepared, PreparedRightKeyMap::Binary { .. }))
            .count() as f64,
    );
    counters.insert(
        "display_key_sources".to_string(),
        right_key_maps
            .iter()
            .filter(|prepared| matches!(prepared, PreparedRightKeyMap::Display { .. }))
            .count() as f64,
    );
    counters.insert(
        "right_key_index_reuses".to_string(),
        bounded_input_batches.saturating_mul(right_key_maps.len()) as f64,
    );
    counters.insert(
        "parquet_projection_sources".to_string(),
        projection_profile.sources as f64,
    );
    counters.insert(
        "parquet_total_columns".to_string(),
        projection_profile.total_columns as f64,
    );
    counters.insert(
        "parquet_projected_columns".to_string(),
        projection_profile.projected_columns as f64,
    );
    counters.insert("right_read_sec".to_string(), right_read_sec);
    counters.insert(
        "right_staging_sources".to_string(),
        right_staging_profile.staged_sources as f64,
    );
    counters.insert(
        "right_staging_skipped_sources".to_string(),
        right_staging_profile.skipped_sources as f64,
    );
    counters.insert(
        "right_staging_input_rows".to_string(),
        right_staging_profile.input_rows as f64,
    );
    counters.insert(
        "right_staging_output_rows".to_string(),
        right_staging_profile.output_rows as f64,
    );
    counters.insert(
        "right_staging_peak_input_batch_rows".to_string(),
        right_staging_profile.peak_input_batch_rows as f64,
    );
    counters.insert(
        "right_staging_peak_bytes".to_string(),
        right_staging_profile.peak_staged_bytes as f64,
    );
    counters.insert(
        "right_staging_write_sec".to_string(),
        right_staging_profile.write_sec,
    );
    counters.insert(
        "right_staging_read_sec".to_string(),
        right_staging_profile.read_sec,
    );
    counters.insert(
        "right_staging_filter_sec".to_string(),
        right_staging_profile.filter_sec,
    );
    counters.insert("left_schema_sec".to_string(), left_schema_sec);
    counters.insert("left_read_sec".to_string(), left_read_sec);
    counters.insert(
        "left_reader_setup_sec".to_string(),
        left_read_profile.reader_setup_sec,
    );
    counters.insert(
        "left_parquet_decode_sec".to_string(),
        left_read_profile.decode_sec,
    );
    counters.insert(
        "left_schema_align_sec".to_string(),
        left_read_profile.schema_align_sec,
    );
    counters.insert(
        "left_parquet_decode_batches".to_string(),
        left_read_profile.batches as f64,
    );
    counters.insert(
        "left_parquet_decode_rows".to_string(),
        left_read_profile.rows as f64,
    );
    counters.insert(
        "left_parquet_compressed_bytes".to_string(),
        left_read_profile.compressed_bytes as f64,
    );
    counters.insert(
        "left_parquet_decode_rows_per_sec".to_string(),
        if left_read_profile.decode_sec > 0.0 {
            left_read_profile.rows as f64 / left_read_profile.decode_sec
        } else {
            0.0
        },
    );
    counters.insert(
        "left_parquet_decode_mib_per_sec".to_string(),
        if left_read_profile.decode_sec > 0.0 {
            left_read_profile.compressed_bytes as f64
                / (1024.0 * 1024.0)
                / left_read_profile.decode_sec
        } else {
            0.0
        },
    );
    counters.insert(
        "left_read_unattributed_sec".to_string(),
        (left_read_sec - left_read_profile.measured_sec()).max(0.0),
    );
    counters.insert("left_preprocess_sec".to_string(), left_preprocess_sec);
    counters.insert("left_select_sec".to_string(), left_select_sec);
    counters.insert(
        "left_partition_filter_sec".to_string(),
        left_partition_filter_sec,
    );
    counters.insert("left_key_filter_sec".to_string(), left_key_filter_sec);
    counters.insert(
        "left_key_filter_identity_batches".to_string(),
        left_key_filter_identity_batches as f64,
    );
    counters.insert(
        "left_key_filter_skipped_batches".to_string(),
        left_key_filter_skipped_batches as f64,
    );
    counters.insert(
        "left_preprocess_unattributed_sec".to_string(),
        (left_preprocess_sec - left_select_sec - left_partition_filter_sec - left_key_filter_sec)
            .max(0.0),
    );
    counters.insert("join_compute_sec".to_string(), join_compute_sec);
    counters.insert(
        "right_key_map_build_sec".to_string(),
        join_kernel_profile.right_key_map_build_sec,
    );
    counters.insert(
        "left_key_encode_sec".to_string(),
        join_kernel_profile.left_key_encode_sec,
    );
    counters.insert(
        "hash_probe_sec".to_string(),
        join_kernel_profile.hash_probe_sec,
    );
    counters.insert(
        "result_materialize_sec".to_string(),
        join_kernel_profile.result_materialize_sec,
    );
    counters.insert(
        "join_kernel_unattributed_sec".to_string(),
        (join_compute_sec - join_kernel_profile.measured_sec()).max(0.0),
    );
    counters.insert(
        "join_index_array_build_sec".to_string(),
        join_kernel_profile.join_index_array_build_sec,
    );
    counters.insert(
        "left_take_sec".to_string(),
        join_kernel_profile.left_take_sec,
    );
    counters.insert(
        "join_key_zip_sec".to_string(),
        join_kernel_profile.join_key_zip_sec,
    );
    counters.insert(
        "right_take_sec".to_string(),
        join_kernel_profile.right_take_sec,
    );
    counters.insert(
        "record_batch_build_sec".to_string(),
        join_kernel_profile.record_batch_build_sec,
    );
    counters.insert(
        "result_materialize_unattributed_sec".to_string(),
        (join_kernel_profile.result_materialize_sec
            - join_kernel_profile.measured_materialize_sec())
        .max(0.0),
    );
    counters.insert(
        "left_identity_reuse_batches".to_string(),
        join_kernel_profile.left_identity_reuse_batches as f64,
    );
    counters.insert(
        "right_identity_reuse_batches".to_string(),
        join_kernel_profile.right_identity_reuse_batches as f64,
    );
    counters.insert("post_operation_sec".to_string(), post_operation_sec);
    counters.insert("parquet_write_sec".to_string(), parquet_write_sec);
    counters.insert("writer_close_sec".to_string(), finish_profile.close_sec);
    counters.insert("output_commit_sec".to_string(), finish_profile.commit_sec);
    counters.insert(
        "writer_dictionary_disabled_columns".to_string(),
        finish_profile.dictionary_disabled_columns as f64,
    );
    Ok(counters)
}

fn parquet_union_schema(
    paths: &[String],
    selected_row_groups: Option<&HashMap<String, Vec<usize>>>,
) -> Result<(Arc<Schema>, usize), String> {
    let mut fields: Vec<Arc<Field>> = Vec::new();
    let mut field_index: HashMap<String, usize> = HashMap::new();
    let mut row_groups_touched = 0usize;
    for path in paths {
        let file = File::open(path).map_err(|error| format!("open {path}: {error}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| format!("read parquet metadata {path}: {error}"))?;
        row_groups_touched += validate_selected_row_groups(path, &builder, selected_row_groups)?;
        for field in builder.schema().fields() {
            if let Some(index) = field_index.get(field.name()) {
                if fields[*index].data_type() != field.data_type() {
                    return Err(format!(
                        "incompatible schema drift for column {}: {:?} vs {:?}",
                        field.name(),
                        fields[*index].data_type(),
                        field.data_type()
                    ));
                }
            } else {
                field_index.insert(field.name().clone(), fields.len());
                fields.push(field.clone());
            }
        }
    }
    if fields.is_empty() {
        return Err("no parquet schemas discovered".to_string());
    }
    Ok((Arc::new(Schema::new(fields)), row_groups_touched))
}

fn for_each_parquet_batch<F>(
    paths: &[String],
    selected_row_groups: Option<&HashMap<String, Vec<usize>>>,
    batch_size: usize,
    schema: &Arc<Schema>,
    projection_names: Option<&HashSet<String>>,
    mut consume: F,
) -> Result<ParquetBatchReadProfile, String>
where
    F: FnMut(RecordBatch) -> Result<(), String>,
{
    let mut discovered = false;
    let mut profile = ParquetBatchReadProfile::default();
    for path in paths {
        let reader_setup_started = Instant::now();
        let file = File::open(path).map_err(|error| format!("open {path}: {error}"))?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| format!("read parquet metadata {path}: {error}"))?;
        let row_group_indices = selected_row_groups
            .and_then(|items| items.get(path))
            .cloned()
            .unwrap_or_else(|| (0..builder.metadata().num_row_groups()).collect());
        for index in &row_group_indices {
            let row_group = builder.metadata().row_groups().get(*index).ok_or_else(|| {
                format!(
                    "row group {index} exceeds row group count {} for {path}",
                    builder.metadata().num_row_groups()
                )
            })?;
            profile.compressed_bytes += row_group.compressed_size().max(0) as usize;
        }
        if let Some(row_groups) = selected_row_groups.and_then(|items| items.get(path)) {
            builder = builder.with_row_groups(row_groups.clone());
        }
        if let Some(projection_names) = projection_names {
            let root_indices = builder
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(index, field)| {
                    projection_names.contains(field.name()).then_some(index)
                })
                .collect::<Vec<_>>();
            let mask = ProjectionMask::roots(
                builder.metadata().file_metadata().schema_descr(),
                root_indices,
            );
            builder = builder.with_projection(mask);
        }
        let mut reader = builder
            .with_batch_size(batch_size)
            .build()
            .map_err(|error| format!("build parquet reader {path}: {error}"))?;
        profile.reader_setup_sec += reader_setup_started.elapsed().as_secs_f64();
        loop {
            let decode_started = Instant::now();
            let Some(batch) = reader.next() else {
                profile.decode_sec += decode_started.elapsed().as_secs_f64();
                break;
            };
            profile.decode_sec += decode_started.elapsed().as_secs_f64();
            discovered = true;
            let batch = batch.map_err(|error| format!("read parquet batch {path}: {error}"))?;
            profile.batches += 1;
            profile.rows += batch.num_rows();
            let align_started = Instant::now();
            let batch = align_batch(&batch, schema)?;
            profile.schema_align_sec += align_started.elapsed().as_secs_f64();
            consume(batch)?;
        }
    }
    if !discovered {
        return Err("no parquet rows or record batches discovered".to_string());
    }
    Ok(profile)
}

fn validate_selected_row_groups(
    path: &str,
    builder: &ParquetRecordBatchReaderBuilder<File>,
    selected_row_groups: Option<&HashMap<String, Vec<usize>>>,
) -> Result<usize, String> {
    if let Some(row_groups) = selected_row_groups.and_then(|items| items.get(path)) {
        if row_groups.is_empty() {
            return Err(format!("selected row group list is empty for {path}"));
        }
        let available = builder.metadata().num_row_groups();
        if let Some(invalid) = row_groups.iter().find(|value| **value >= available) {
            return Err(format!(
                "row group {invalid} exceeds row group count {available} for {path}"
            ));
        }
        Ok(row_groups.len())
    } else {
        Ok(builder.metadata().num_row_groups())
    }
}

fn validate_task(config: &JoinTaskConfig) -> Result<(), String> {
    if config.left_files.is_empty() {
        return Err("join task requires left_files".to_string());
    }
    if config.right_sources.is_empty() {
        return Err("join task requires right_sources".to_string());
    }
    if config.left_partition_key.trim().is_empty()
        || config.output_partition_column.trim().is_empty()
    {
        return Err("join task requires partition columns".to_string());
    }
    for source in &config.right_sources {
        if source.files.is_empty() {
            return Err(format!("right source {} has no files", source.name));
        }
        if !matches!(
            source.how.as_str(),
            "inner" | "left" | "right" | "full" | "cross"
        ) {
            return Err(format!("unsupported join type: {}", source.how));
        }
        if source.how == "cross" {
            if !source.left_on.is_empty() || !source.right_on.is_empty() {
                return Err("cross join must not define keys".to_string());
            }
        } else if source.left_on.is_empty() || source.left_on.len() != source.right_on.len() {
            return Err(format!("invalid join keys for source {}", source.name));
        }
    }
    if !config.ordered_operations.is_empty() {
        let mut ids = HashSet::new();
        for operation in &config.ordered_operations {
            if operation.operation_id.trim().is_empty()
                || operation
                    .execution_target
                    .as_deref()
                    .is_none_or(str::is_empty)
                || !ids.insert(&operation.operation_id)
            {
                return Err("ordered join operations require unique non-empty IDs".to_string());
            }
        }
        let kinds = config
            .ordered_operations
            .iter()
            .map(|operation| operation.kind.as_str())
            .collect::<Vec<_>>();
        if kinds.last() != Some(&"write_dataset") {
            return Err("ordered join operations must terminate with write_dataset".to_string());
        }
        let body = &kinds[..kinds.len() - 1];
        if body.len() < config.right_sources.len()
            || body[..config.right_sources.len()]
                .iter()
                .any(|kind| *kind != "join")
        {
            return Err(
                "ordered join operations must start with one join per right source".to_string(),
            );
        }
        let allowed_suffix = HashSet::from([
            "include_columns",
            "exclude_columns",
            "rename_columns",
            "unpivot",
            "data_assertion",
        ]);
        if body[config.right_sources.len()..]
            .iter()
            .any(|kind| !allowed_suffix.contains(kind))
        {
            return Err(
                "ordered join operations contain unsupported post-join suffix kinds".to_string(),
            );
        }
    }
    Ok(())
}

fn read_parquet_union(
    paths: &[String],
    selected_row_groups: Option<&HashMap<String, Vec<usize>>>,
) -> Result<(RecordBatch, usize), String> {
    let mut batches = Vec::new();
    let mut fields: Vec<Arc<Field>> = Vec::new();
    let mut field_index: HashMap<String, usize> = HashMap::new();
    let mut row_groups_touched = 0usize;
    for path in paths {
        let file = File::open(path).map_err(|error| format!("open {path}: {error}"))?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| format!("read parquet metadata {path}: {error}"))?;
        if let Some(row_groups) = selected_row_groups.and_then(|items| items.get(path)) {
            if row_groups.is_empty() {
                return Err(format!("selected row group list is empty for {path}"));
            }
            let available = builder.metadata().num_row_groups();
            if let Some(invalid) = row_groups.iter().find(|value| **value >= available) {
                return Err(format!(
                    "row group {invalid} exceeds row group count {available} for {path}"
                ));
            }
            row_groups_touched += row_groups.len();
            builder = builder.with_row_groups(row_groups.clone());
        } else {
            row_groups_touched += builder.metadata().num_row_groups();
        }
        let mut reader = builder
            .with_batch_size(65_536)
            .build()
            .map_err(|error| format!("build parquet reader {path}: {error}"))?;
        for batch in &mut reader {
            let batch = batch.map_err(|error| format!("read parquet batch {path}: {error}"))?;
            for field in batch.schema().fields() {
                if let Some(index) = field_index.get(field.name()) {
                    if fields[*index].data_type() != field.data_type() {
                        return Err(format!(
                            "incompatible schema drift for column {}: {:?} vs {:?}",
                            field.name(),
                            fields[*index].data_type(),
                            field.data_type()
                        ));
                    }
                } else {
                    field_index.insert(field.name().clone(), fields.len());
                    fields.push(field.clone());
                }
            }
            batches.push(batch);
        }
    }
    if batches.is_empty() {
        return Err("no parquet rows or record batches discovered".to_string());
    }
    let schema = Arc::new(Schema::new(fields));
    let aligned = batches
        .into_iter()
        .map(|batch| align_batch(&batch, &schema))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = concat_batches(&schema, &aligned).map_err(|error| error.to_string())?;
    Ok((batch, row_groups_touched))
}

fn read_parquet_union_projected(
    paths: &[String],
    selected_row_groups: Option<&HashMap<String, Vec<usize>>>,
    policy: &ColumnPolicy,
    required: &[String],
    source_name: &str,
) -> Result<(RecordBatch, usize, ReadProjectionProfile), String> {
    let (full_schema, row_groups_touched) = parquet_union_schema(paths, selected_row_groups)?;
    let selected = selected_column_names(full_schema.as_ref(), policy, required, source_name)?;
    let schema = projected_schema(full_schema.as_ref(), &selected)?;
    let projection_names = selected.iter().cloned().collect::<HashSet<_>>();
    let mut batches = Vec::new();
    for_each_parquet_batch(
        paths,
        selected_row_groups,
        65_536,
        &schema,
        Some(&projection_names),
        |batch| {
            batches.push(batch);
            Ok(())
        },
    )?;
    let batch = concat_batches(&schema, &batches).map_err(|error| error.to_string())?;
    Ok((
        batch,
        row_groups_touched,
        ReadProjectionProfile {
            sources: paths.len(),
            total_columns: full_schema.fields().len().saturating_mul(paths.len()),
            projected_columns: selected.len().saturating_mul(paths.len()),
        },
    ))
}

fn align_batch(batch: &RecordBatch, schema: &Arc<Schema>) -> Result<RecordBatch, String> {
    if batch.schema().fields() == schema.fields() {
        return Ok(batch.clone());
    }
    let arrays = schema
        .fields()
        .iter()
        .map(|field| {
            batch
                .column_by_name(field.name())
                .cloned()
                .unwrap_or_else(|| new_null_array(field.data_type(), batch.num_rows()))
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema.clone(), arrays).map_err(|error| error.to_string())
}

fn select_columns(
    batch: &RecordBatch,
    policy: &ColumnPolicy,
    required: &[String],
    source_name: &str,
) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let selected = selected_column_names(schema.as_ref(), policy, required, source_name)?;
    let fields = selected
        .iter()
        .map(|name| {
            schema
                .field_with_name(name)
                .cloned()
                .map(Arc::new)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let arrays = selected
        .iter()
        .map(|name| batch.column_by_name(name).cloned().unwrap())
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}

fn selected_column_names(
    schema: &Schema,
    policy: &ColumnPolicy,
    required: &[String],
    source_name: &str,
) -> Result<Vec<String>, String> {
    let names = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    for name in required {
        if !names.contains(name) {
            return Err(format!(
                "source {source_name} is missing required column {name}"
            ));
        }
        if policy.exclude.contains(name) {
            return Err(format!(
                "source {source_name} cannot exclude required column {name}"
            ));
        }
    }
    let patterns = policy
        .regex
        .iter()
        .map(|pattern| Regex::new(pattern).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut selected = if policy.include.is_empty() && patterns.is_empty() {
        names.clone()
    } else {
        let mut selected = policy.include.clone();
        for name in &names {
            if patterns.iter().any(|pattern| pattern.is_match(name)) && !selected.contains(name) {
                selected.push(name.clone());
            }
        }
        selected
    };
    for name in &selected {
        if !names.contains(name) {
            return Err(format!(
                "source {source_name} include column not found: {name}"
            ));
        }
    }
    selected.retain(|name| !policy.exclude.contains(name));
    for name in required {
        if !selected.contains(name) {
            selected.push(name.clone());
        }
    }
    Ok(selected)
}

fn projected_schema(schema: &Schema, names: &[String]) -> Result<Arc<Schema>, String> {
    let fields = names
        .iter()
        .map(|name| {
            schema
                .field_with_name(name)
                .cloned()
                .map(Arc::new)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn filter_column_value(
    batch: &RecordBatch,
    column: &str,
    value: &str,
) -> Result<RecordBatch, String> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| format!("partition column not found: {column}"))?;
    let mut indices = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        if array_string_value_equals(array.as_ref(), index, value)? {
            indices.push(index as u32);
        }
    }
    if indices.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    take_batch(batch, &UInt32Array::from(indices))
}

fn array_string_value_equals(array: &dyn Array, index: usize, value: &str) -> Result<bool, String> {
    if array.is_null(index) {
        return Ok(false);
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(strings.value(index) == value);
    }
    if let Some(strings) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(strings.value(index) == value);
    }
    array_value_to_string(array, index)
        .map(|rendered| rendered == value)
        .map_err(|error| error.to_string())
}

fn filter_key_rows(
    batch: &RecordBatch,
    columns: &[String],
    key_rows: &[HashMap<String, Value>],
) -> Result<(RecordBatch, bool), String> {
    let allowed = key_rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| json_key_value(row.get(column)))
                .collect::<Vec<_>>()
                .join("\u{1f}")
        })
        .collect::<HashSet<_>>();
    let arrays = columns
        .iter()
        .map(|column| {
            batch
                .column_by_name(column)
                .cloned()
                .ok_or_else(|| format!("join key column not found: {column}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let indices = (0..batch.num_rows())
        .filter_map(|index| {
            let key = row_key(&arrays, index, true).ok()?;
            allowed.contains(&key).then_some(index as u32)
        })
        .collect::<Vec<_>>();
    if indices.len() == batch.num_rows() {
        return Ok((batch.clone(), true));
    }
    Ok((take_batch(batch, &UInt32Array::from(indices))?, false))
}

fn json_key_value(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "#NULL".to_string(),
        Some(Value::String(value)) => format!("{}:{value}", value.len()),
        Some(value) => {
            let rendered = value.to_string();
            format!("{}:{rendered}", rendered.len())
        }
    }
}

fn row_key(arrays: &[ArrayRef], index: usize, allow_null: bool) -> Result<String, String> {
    let mut values = Vec::with_capacity(arrays.len());
    for array in arrays {
        if array.is_null(index) {
            if !allow_null {
                return Err("null join key".to_string());
            }
            values.push("#NULL".to_string());
        } else {
            let value =
                array_value_to_string(array.as_ref(), index).map_err(|error| error.to_string())?;
            values.push(format!("{}:{value}", value.len()));
        }
    }
    Ok(values.join("\u{1f}"))
}

fn join_batches(
    left: RecordBatch,
    right: RecordBatch,
    source: &RightSource,
    partition_helper_to_drop: Option<&str>,
    backend: JoinBackend,
) -> Result<JoinResult, String> {
    join_batches_with_right_key_map(left, right, source, partition_helper_to_drop, backend, None)
}

fn join_batches_with_right_key_map(
    left: RecordBatch,
    right: RecordBatch,
    source: &RightSource,
    partition_helper_to_drop: Option<&str>,
    backend: JoinBackend,
    right_key_map: Option<&mut PreparedRightKeyMap>,
) -> Result<JoinResult, String> {
    if backend == JoinBackend::Polars {
        #[cfg(feature = "polars-join-experiment")]
        {
            return join_batches_polars(left, right, source, partition_helper_to_drop);
        }
        #[cfg(not(feature = "polars-join-experiment"))]
        {
            return Err(
                "Polars join backend is not compiled; enable polars-join-experiment".to_string(),
            );
        }
    }
    let mut kernel_profile = JoinKernelProfile::default();
    let indices = if source.how == "cross" {
        let output_rows = left.num_rows().saturating_mul(right.num_rows());
        let mut indices = JoinIndices {
            left: Vec::with_capacity(output_rows),
            right: Vec::with_capacity(output_rows),
            matched_rows: output_rows,
        };
        for left_index in 0..left.num_rows() {
            for right_index in 0..right.num_rows() {
                indices.left.push(Some(left_index as u32));
                indices.right.push(Some(right_index as u32));
            }
        }
        indices
    } else if let Some(right_key_map) = right_key_map {
        let (indices, profile) =
            build_join_indices_from_right_key_map(&left, &right, source, right_key_map)?;
        kernel_profile = profile;
        indices
    } else {
        build_join_indices(&left, &right, source)?
    };
    let matched_rows = indices.matched_rows;
    let materialize_started = Instant::now();
    let index_array_started = Instant::now();
    let left_identity = indices_are_identity(&indices.left, left.num_rows());
    let right_identity = indices_are_identity(&indices.right, right.num_rows());
    let left_present = matches!(source.how.as_str(), "right" | "full").then(|| {
        BooleanArray::from(
            indices
                .left
                .iter()
                .map(|index| index.is_some())
                .collect::<Vec<_>>(),
        )
    });
    let left_indices = (!left_identity).then(|| UInt32Array::from(indices.left));
    let right_indices = (!right_identity).then(|| UInt32Array::from(indices.right));
    kernel_profile.join_index_array_build_sec += index_array_started.elapsed().as_secs_f64();
    kernel_profile.left_identity_reuse_batches += usize::from(left_identity);
    kernel_profile.right_identity_reuse_batches += usize::from(right_identity);
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    let mut names = HashSet::new();
    for field in left.schema().fields() {
        let left_source = left.column_by_name(field.name()).unwrap();
        let left_array = if left_identity {
            Arc::clone(left_source)
        } else {
            let take_started = Instant::now();
            let array = take(left_source.as_ref(), left_indices.as_ref().unwrap(), None)
                .map_err(|error| error.to_string())?;
            kernel_profile.left_take_sec += take_started.elapsed().as_secs_f64();
            array
        };
        let array =
            if let Some(position) = source.left_on.iter().position(|name| name == field.name()) {
                if let Some(left_present) = left_present.as_ref() {
                    let zip_started = Instant::now();
                    let right_name = &source.right_on[position];
                    let right_source = right
                        .column_by_name(right_name)
                        .ok_or_else(|| format!("right join key not found: {right_name}"))?;
                    if right_source.data_type() != left_array.data_type() {
                        return Err(format!(
                            "join key dtype mismatch: {} {:?} vs {} {:?}",
                            field.name(),
                            left_array.data_type(),
                            right_name,
                            right_source.data_type()
                        ));
                    }
                    let right_array = if right_identity {
                        Arc::clone(right_source)
                    } else {
                        take(right_source.as_ref(), right_indices.as_ref().unwrap(), None)
                            .map_err(|error| error.to_string())?
                    };
                    let array = zip(left_present, &left_array, &right_array)
                        .map_err(|error| error.to_string())?;
                    kernel_profile.join_key_zip_sec += zip_started.elapsed().as_secs_f64();
                    array
                } else {
                    left_array
                }
            } else {
                left_array
            };
        fields.push(Arc::new(Field::new(
            field.name(),
            field.data_type().clone(),
            true,
        )));
        arrays.push(array);
        names.insert(field.name().clone());
    }
    for field in right.schema().fields() {
        if source.right_on.contains(field.name()) {
            continue;
        }
        if partition_helper_to_drop == Some(field.name().as_str()) {
            continue;
        }
        let output_name = if names.contains(field.name()) {
            format!("{}{}", field.name(), source.suffix)
        } else {
            field.name().clone()
        };
        if !names.insert(output_name.clone()) {
            return Err(format!("join output column collision: {output_name}"));
        }
        let right_source = right.column_by_name(field.name()).unwrap();
        let array = if right_identity {
            Arc::clone(right_source)
        } else {
            let take_started = Instant::now();
            let array = take(right_source.as_ref(), right_indices.as_ref().unwrap(), None)
                .map_err(|error| error.to_string())?;
            kernel_profile.right_take_sec += take_started.elapsed().as_secs_f64();
            array
        };
        fields.push(Arc::new(Field::new(
            output_name,
            field.data_type().clone(),
            true,
        )));
        arrays.push(array);
    }
    let record_batch_started = Instant::now();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| error.to_string())?;
    kernel_profile.record_batch_build_sec += record_batch_started.elapsed().as_secs_f64();
    kernel_profile.result_materialize_sec += materialize_started.elapsed().as_secs_f64();
    Ok(JoinResult {
        batch,
        matched_rows,
        bridge_profile: BridgeProfile::default(),
        kernel_profile,
    })
}

#[cfg(feature = "polars-join-experiment")]
fn join_batches_polars(
    left: RecordBatch,
    right: RecordBatch,
    source: &RightSource,
    partition_helper_to_drop: Option<&str>,
) -> Result<JoinResult, String> {
    let (mut left, left_profile) = apache_to_polars(left)?;
    let (mut right, right_profile) = apache_to_polars(right)?;
    let mut profile = BridgeProfile::default();
    profile.accumulate(&left_profile);
    profile.accumulate(&right_profile);

    if let Some(helper) = partition_helper_to_drop {
        if !source.right_on.iter().any(|name| name == helper)
            && right
                .get_column_names()
                .iter()
                .any(|name| name.as_str() == helper)
        {
            right
                .drop_in_place(helper)
                .map_err(|error| error.to_string())?;
        }
    }

    let left_marker = unique_marker_name(&left, &right, "__smoking_data_left_present");
    let right_marker = unique_marker_name(&left, &right, "__smoking_data_right_present");
    left.with_column(
        Series::new(
            PlSmallStr::from_str(&left_marker),
            vec![1_i32; left.height()].as_slice(),
        )
        .into(),
    )
    .map_err(|error| error.to_string())?;
    right
        .with_column(
            Series::new(
                PlSmallStr::from_str(&right_marker),
                vec![1_i32; right.height()].as_slice(),
            )
            .into(),
        )
        .map_err(|error| error.to_string())?;

    let how = match source.how.as_str() {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "right" => JoinType::Right,
        "full" => JoinType::Full,
        "cross" => JoinType::Cross,
        other => return Err(format!("unsupported Polars join type: {other}")),
    };
    let started = Instant::now();
    let mut joined = left
        .join(
            &right,
            source.left_on.iter().map(String::as_str),
            source.right_on.iter().map(String::as_str),
            JoinArgs {
                how,
                suffix: Some(PlSmallStr::from_str(&source.suffix)),
                nulls_equal: false,
                coalesce: JoinCoalesce::CoalesceColumns,
                maintain_order: MaintainOrderJoin::None,
                ..JoinArgs::default()
            },
            None,
        )
        .map_err(|error| error.to_string())?;
    profile.join_sec += started.elapsed().as_secs_f64();
    profile.polars_output_estimated_bytes += joined.estimated_size();

    let matched_rows = match source.how.as_str() {
        "inner" | "cross" => joined.height(),
        "left" => {
            joined.height()
                - joined
                    .column(&right_marker)
                    .map_err(|error| error.to_string())?
                    .null_count()
        }
        "right" => {
            joined.height()
                - joined
                    .column(&left_marker)
                    .map_err(|error| error.to_string())?
                    .null_count()
        }
        "full" => {
            let left_valid = joined
                .column(&left_marker)
                .map_err(|error| error.to_string())?
                .is_not_null();
            let right_valid = joined
                .column(&right_marker)
                .map_err(|error| error.to_string())?
                .is_not_null();
            (&left_valid & &right_valid).sum().unwrap_or(0) as usize
        }
        _ => unreachable!(),
    };
    joined
        .drop_in_place(&left_marker)
        .map_err(|error| error.to_string())?;
    joined
        .drop_in_place(&right_marker)
        .map_err(|error| error.to_string())?;
    let (batch, export_profile) = polars_to_apache(joined)?;
    profile.accumulate(&export_profile);
    Ok(JoinResult {
        batch,
        matched_rows,
        bridge_profile: profile,
        kernel_profile: JoinKernelProfile::default(),
    })
}

#[cfg(feature = "polars-join-experiment")]
fn unique_marker_name(
    left: &polars_core::prelude::DataFrame,
    right: &polars_core::prelude::DataFrame,
    prefix: &str,
) -> String {
    let mut name = prefix.to_string();
    while left
        .get_column_names()
        .iter()
        .any(|item| item.as_str() == name)
        || right
            .get_column_names()
            .iter()
            .any(|item| item.as_str() == name)
    {
        name.push('_');
    }
    name
}

fn validate_join_key_dtypes(left: &[ArrayRef], right: &[ArrayRef]) -> Result<(), String> {
    if left.len() != right.len() {
        return Err(format!(
            "join key width mismatch: {} vs {}",
            left.len(),
            right.len()
        ));
    }
    for (left, right) in left.iter().zip(right) {
        if left.data_type() != right.data_type() {
            return Err(format!(
                "join key dtype mismatch: {:?} vs {:?}",
                left.data_type(),
                right.data_type()
            ));
        }
    }
    Ok(())
}

fn join_row_converter(arrays: &[ArrayRef]) -> Result<RowConverter, String> {
    if arrays.is_empty() {
        return Err("join key columns must not be empty".to_string());
    }
    let fields = arrays
        .iter()
        .map(|array| SortField::new(array.data_type().clone()))
        .collect();
    RowConverter::new(fields).map_err(|error| format!("prepare binary join key encoder: {error}"))
}

fn encode_join_rows(arrays: &[ArrayRef]) -> Result<Rows, String> {
    let converter = join_row_converter(arrays)?;
    converter
        .convert_columns(arrays)
        .map_err(|error| format!("encode binary join keys: {error}"))
}

fn key_has_null(arrays: &[ArrayRef], index: usize) -> bool {
    arrays.iter().any(|array| array.is_null(index))
}

fn build_join_indices(
    left: &RecordBatch,
    right: &RecordBatch,
    source: &RightSource,
) -> Result<JoinIndices, String> {
    let left_arrays = join_arrays(left, &source.left_on)?;
    let right_arrays = join_arrays(right, &source.right_on)?;
    validate_join_key_dtypes(&left_arrays, &right_arrays)?;
    let right_rows = encode_join_rows(&right_arrays)?;
    let left_rows = encode_join_rows(&left_arrays)?;
    let mut right_map: RightKeyMap = HashMap::new();
    let right_has_nulls = right_arrays.iter().any(|array| array.null_count() > 0);
    for index in 0..right.num_rows() {
        if !(right_has_nulls && key_has_null(&right_arrays, index)) {
            right_map
                .entry(right_rows.row(index).as_ref().into())
                .or_default()
                .push(index as u32);
        }
    }
    let mut indices = JoinIndices::with_capacity(left.num_rows());
    let mut matched_right = vec![false; right.num_rows()];
    let left_has_nulls = left_arrays.iter().any(|array| array.null_count() > 0);
    for left_index in 0..left.num_rows() {
        let matches = (!(left_has_nulls && key_has_null(&left_arrays, left_index)))
            .then(|| right_map.get(left_rows.row(left_index).as_ref()))
            .flatten();
        if let Some(right_indices) = matches {
            for right_index in right_indices {
                indices_push_match(&mut indices, left_index as u32, *right_index);
                matched_right[*right_index as usize] = true;
            }
        } else if matches!(source.how.as_str(), "left" | "full") {
            indices.left.push(Some(left_index as u32));
            indices.right.push(None);
        }
    }
    if matches!(source.how.as_str(), "right" | "full") {
        for (right_index, matched) in matched_right.into_iter().enumerate() {
            if !matched {
                indices.left.push(None);
                indices.right.push(Some(right_index as u32));
            }
        }
    }
    Ok(indices)
}

fn prepare_binary_right_key_map(
    right: &RecordBatch,
    source: &RightSource,
) -> Result<PreparedRightKeyMap, String> {
    let right_arrays = join_arrays(right, &source.right_on)?;
    let converter = join_row_converter(&right_arrays)?;
    let right_rows = converter
        .convert_columns(&right_arrays)
        .map_err(|error| format!("encode binary join keys: {error}"))?;
    let mut right_map: RightKeyMap = HashMap::new();
    let right_has_nulls = right_arrays.iter().any(|array| array.null_count() > 0);
    for index in 0..right.num_rows() {
        if !(right_has_nulls && key_has_null(&right_arrays, index)) {
            right_map
                .entry(right_rows.row(index).as_ref().into())
                .or_default()
                .push(index as u32);
        }
    }
    let scratch_rows = converter.empty_rows(0, 0);
    Ok(PreparedRightKeyMap::Binary {
        map: right_map,
        converter,
        scratch_rows,
    })
}

fn prepare_display_right_key_map(
    right: &RecordBatch,
    source: &RightSource,
) -> Result<PreparedRightKeyMap, String> {
    let right_arrays = join_arrays(right, &source.right_on)?;
    let mut map = DisplayRightKeyMap::new();
    let right_has_nulls = right_arrays.iter().any(|array| array.null_count() > 0);
    for index in 0..right.num_rows() {
        if !(right_has_nulls && key_has_null(&right_arrays, index)) {
            let key = row_key(&right_arrays, index, false)?;
            map.entry(key).or_default().push(index as u32);
        }
    }
    Ok(PreparedRightKeyMap::Display { map })
}

fn indices_push_match(indices: &mut JoinIndices, left_index: u32, right_index: u32) {
    indices.left.push(Some(left_index));
    indices.right.push(Some(right_index));
    indices.matched_rows += 1;
}

fn build_join_indices_from_right_key_map(
    left: &RecordBatch,
    right: &RecordBatch,
    source: &RightSource,
    prepared: &mut PreparedRightKeyMap,
) -> Result<(JoinIndices, JoinKernelProfile), String> {
    let mut profile = JoinKernelProfile::default();
    if !matches!(source.how.as_str(), "inner" | "left") {
        return Err(format!(
            "prepared right key map does not support join type {}",
            source.how
        ));
    }
    let left_arrays = join_arrays(left, &source.left_on)?;
    let right_arrays = join_arrays(right, &source.right_on)?;
    validate_join_key_dtypes(&left_arrays, &right_arrays)?;
    if matches!(prepared, PreparedRightKeyMap::Pending) {
        let right_build_started = Instant::now();
        *prepared = if left.num_rows() >= BINARY_ROW_MIN_BATCH_ROWS {
            prepare_binary_right_key_map(right, source)?
        } else {
            prepare_display_right_key_map(right, source)?
        };
        profile.right_key_map_build_sec += right_build_started.elapsed().as_secs_f64();
    }
    let mut indices = JoinIndices::with_capacity(left.num_rows());
    let left_has_nulls = left_arrays.iter().any(|array| array.null_count() > 0);
    match prepared {
        PreparedRightKeyMap::Pending => unreachable!(),
        PreparedRightKeyMap::Binary {
            map,
            converter,
            scratch_rows,
        } => {
            let encode_started = Instant::now();
            scratch_rows.clear();
            converter
                .append(scratch_rows, &left_arrays)
                .map_err(|error| format!("encode binary join keys: {error}"))?;
            profile.left_key_encode_sec += encode_started.elapsed().as_secs_f64();
            let probe_started = Instant::now();
            for left_index in 0..left.num_rows() {
                let matches = (!(left_has_nulls && key_has_null(&left_arrays, left_index)))
                    .then(|| map.get(scratch_rows.row(left_index).as_ref()))
                    .flatten();
                append_left_matches(&mut indices, left_index, matches, source.how == "left");
            }
            profile.hash_probe_sec += probe_started.elapsed().as_secs_f64();
        }
        PreparedRightKeyMap::Display { map } => {
            let encode_started = Instant::now();
            let left_keys = (0..left.num_rows())
                .map(|left_index| {
                    if left_has_nulls && key_has_null(&left_arrays, left_index) {
                        None
                    } else {
                        row_key(&left_arrays, left_index, false).ok()
                    }
                })
                .collect::<Vec<_>>();
            profile.left_key_encode_sec += encode_started.elapsed().as_secs_f64();
            let probe_started = Instant::now();
            for (left_index, key) in left_keys.iter().enumerate() {
                let matches = key.as_ref().and_then(|key| map.get(key));
                append_left_matches(&mut indices, left_index, matches, source.how == "left");
            }
            profile.hash_probe_sec += probe_started.elapsed().as_secs_f64();
        }
    }
    Ok((indices, profile))
}

fn append_left_matches(
    indices: &mut JoinIndices,
    left_index: usize,
    matches: Option<&Vec<u32>>,
    emit_unmatched: bool,
) {
    if let Some(right_indices) = matches {
        for right_index in right_indices {
            indices_push_match(indices, left_index as u32, *right_index);
        }
    } else if emit_unmatched {
        indices.left.push(Some(left_index as u32));
        indices.right.push(None);
    }
}

fn join_arrays(batch: &RecordBatch, names: &[String]) -> Result<Vec<ArrayRef>, String> {
    names
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .cloned()
                .ok_or_else(|| format!("join key column not found: {name}"))
        })
        .collect()
}

fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch, String> {
    let arrays = batch
        .columns()
        .iter()
        .map(|array| take(array.as_ref(), indices, None).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), arrays).map_err(|error| error.to_string())
}

fn set_partition_column(
    batch: &RecordBatch,
    column: &str,
    value: &str,
) -> Result<RecordBatch, String> {
    let partition: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![
        value.to_string();
        batch.num_rows()
    ]));
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    let mut arrays = batch.columns().to_vec();
    if let Some(index) = fields.iter().position(|field| field.name() == column) {
        fields[index] = Arc::new(Field::new(column, partition.data_type().clone(), false));
        arrays[index] = partition;
    } else {
        fields.push(Arc::new(Field::new(
            column,
            partition.data_type().clone(),
            false,
        )));
        arrays.push(partition);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}

struct AtomicParquetBatchWriter {
    output_path: PathBuf,
    temp_path: PathBuf,
    output_row_group_rows: Option<usize>,
    compression: String,
    schema: Option<Arc<Schema>>,
    writer: Option<ArrowWriter<File>>,
    dictionary_disabled_columns: usize,
}

#[derive(Debug, Default)]
struct WriterFinishProfile {
    close_sec: f64,
    commit_sec: f64,
    dictionary_disabled_columns: usize,
}

impl AtomicParquetBatchWriter {
    fn new(
        output_path: &Path,
        output_row_group_rows: Option<usize>,
        compression: Option<&str>,
    ) -> Result<Self, String> {
        let parent = output_path
            .parent()
            .ok_or_else(|| "join output path has no parent".to_string())?;
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        Ok(Self {
            output_path: output_path.to_path_buf(),
            temp_path: temp_path_for(output_path),
            output_row_group_rows,
            compression: compression.unwrap_or("zstd").to_string(),
            schema: None,
            writer: None,
            dictionary_disabled_columns: 0,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        if self.writer.is_none() {
            let file = File::create(&self.temp_path).map_err(|error| error.to_string())?;
            self.schema = Some(batch.schema());
            let dictionary_disabled_columns = high_cardinality_string_columns(batch);
            let properties = parquet_writer_properties(
                self.output_row_group_rows,
                Some(&self.compression),
                Some(batch.schema().as_ref()),
                &dictionary_disabled_columns,
            )?;
            self.dictionary_disabled_columns = dictionary_disabled_columns.len();
            self.writer = Some(
                ArrowWriter::try_new(file, batch.schema(), properties)
                    .map_err(|error| error.to_string())?,
            );
        }
        let writer = self.writer.as_mut().expect("writer initialized above");
        if self.schema.as_ref() != Some(&batch.schema()) {
            return Err("bounded join produced inconsistent output schemas".to_string());
        }
        writer.write(batch).map_err(|error| error.to_string())
    }

    fn finish(mut self) -> Result<WriterFinishProfile, String> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| "bounded join produced no output batches".to_string())?;
        let close_started = Instant::now();
        writer.close().map_err(|error| error.to_string())?;
        let close_sec = close_started.elapsed().as_secs_f64();
        let commit_started = Instant::now();
        replace_output_file(&self.temp_path, &self.output_path)?;
        Ok(WriterFinishProfile {
            close_sec,
            commit_sec: commit_started.elapsed().as_secs_f64(),
            dictionary_disabled_columns: self.dictionary_disabled_columns,
        })
    }
}

fn parquet_writer_properties(
    output_row_group_rows: Option<usize>,
    compression: Option<&str>,
    schema: Option<&Schema>,
    dictionary_disabled_columns: &HashSet<String>,
) -> Result<Option<WriterProperties>, String> {
    let compression = match compression.unwrap_or("zstd").to_ascii_lowercase().as_str() {
        "snappy" => Compression::SNAPPY,
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        other => return Err(format!("unsupported parquet compression: {other}")),
    };
    let mut properties = WriterProperties::builder()
        .set_compression(compression)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_offset_index_disabled(false);
    if let Some(rows) = output_row_group_rows {
        properties = properties.set_max_row_group_size(rows.max(1));
    }
    if let Some(schema) = schema {
        for field in schema.fields() {
            if dictionary_disabled_columns.contains(field.name())
                || matches!(
                    field.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                )
            {
                properties = properties.set_column_dictionary_enabled(
                    ColumnPath::new(vec![field.name().clone()]),
                    false,
                );
            }
        }
    }
    Ok(Some(properties.build()))
}

fn high_cardinality_string_columns(batch: &RecordBatch) -> HashSet<String> {
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            is_long_high_cardinality_string(batch.column(index).as_ref())
                .then(|| field.name().clone())
        })
        .collect()
}

fn is_long_high_cardinality_string(array: &dyn Array) -> bool {
    const SAMPLE_ROWS: usize = 1_024;
    const MIN_AVG_BYTES: usize = 32;
    let sample_rows = array.len().min(SAMPLE_ROWS);
    if sample_rows < 32 {
        return false;
    }
    let mut unique = HashSet::with_capacity(sample_rows);
    let mut total_bytes = 0usize;
    let mut non_null_rows = 0usize;
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        for index in 0..sample_rows {
            if !values.is_null(index) {
                let value = values.value(index);
                total_bytes += value.len();
                unique.insert(value);
                non_null_rows += 1;
            }
        }
    } else if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        for index in 0..sample_rows {
            if !values.is_null(index) {
                let value = values.value(index);
                total_bytes += value.len();
                unique.insert(value);
                non_null_rows += 1;
            }
        }
    } else {
        return false;
    }
    non_null_rows > 0
        && total_bytes / non_null_rows >= MIN_AVG_BYTES
        && unique.len() * 10 >= non_null_rows * 8
}

fn replace_output_file(temp_path: &Path, output_path: &Path) -> Result<(), String> {
    if output_path.exists() {
        fs::remove_file(output_path).map_err(|error| error.to_string())?;
    }
    fs::rename(temp_path, output_path).map_err(|error| error.to_string())
}

fn write_parquet_atomic(
    batch: &RecordBatch,
    output_path: &Path,
    output_row_group_rows: Option<usize>,
    compression: Option<&str>,
) -> Result<(), String> {
    let parent = output_path
        .parent()
        .ok_or_else(|| "join output path has no parent".to_string())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let temp_path = temp_path_for(output_path);
    let file = File::create(&temp_path).map_err(|error| error.to_string())?;
    let properties = parquet_writer_properties(
        output_row_group_rows,
        compression,
        Some(batch.schema().as_ref()),
        &high_cardinality_string_columns(batch),
    )?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), properties)
        .map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    replace_output_file(&temp_path, output_path)
}

fn temp_path_for(output_path: &Path) -> PathBuf {
    let name = output_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("part.parquet");
    output_path.with_file_name(format!(".{name}.tmp"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, LargeStringArray, StringArray};

    #[test]
    fn long_high_cardinality_strings_disable_dictionary_encoding() {
        let values = (0..64)
            .map(|index| format!("{index:04}-{}", "x".repeat(48)))
            .collect::<Vec<_>>();
        let array = LargeStringArray::from_iter_values(values.iter());

        assert!(is_long_high_cardinality_string(&array));
    }

    #[test]
    fn repetitive_long_strings_keep_dictionary_encoding() {
        let values = (0..64)
            .map(|_| format!("constant-{}", "x".repeat(48)))
            .collect::<Vec<_>>();
        let array = LargeStringArray::from_iter_values(values.iter());

        assert!(!is_long_high_cardinality_string(&array));
    }

    fn left_join_source() -> RightSource {
        RightSource {
            name: "right".to_string(),
            files: Vec::new(),
            row_groups: HashMap::new(),
            columns: ColumnPolicy::default(),
            left_on: vec!["key_text".to_string(), "key_number".to_string()],
            right_on: vec!["key_text_r".to_string(), "key_number_r".to_string()],
            how: "left".to_string(),
            suffix: "_right".to_string(),
            keep_right_partition_column: false,
            staging_estimated_match_rows: None,
        }
    }

    #[test]
    fn binary_join_keys_preserve_composite_boundaries() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["a", "a\u{1f}b"])),
            Arc::new(StringArray::from(vec!["b\u{1f}c", "c"])),
        ];

        let rows = encode_join_rows(&arrays).unwrap();

        assert_ne!(rows.row(0).as_ref(), rows.row(1).as_ref());
    }

    #[test]
    fn identity_indices_require_complete_ordered_coverage() {
        assert!(indices_are_identity(&[Some(0), Some(1), Some(2)], 3));
        assert!(!indices_are_identity(&[Some(0), None, Some(2)], 3));
        assert!(!indices_are_identity(&[Some(0), Some(2), Some(1)], 3));
        assert!(!indices_are_identity(&[Some(0), Some(1)], 3));
    }

    #[test]
    fn key_row_filter_reuses_batch_when_every_row_is_selected() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "join_key",
                Arc::new(StringArray::from(vec!["A", "B"])) as ArrayRef,
            ),
            ("value", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ])
        .unwrap();
        let key_rows = vec![
            HashMap::from([("join_key".to_string(), Value::String("A".to_string()))]),
            HashMap::from([("join_key".to_string(), Value::String("B".to_string()))]),
        ];

        let (filtered, identity) =
            filter_key_rows(&batch, &["join_key".to_string()], &key_rows).unwrap();

        assert!(identity);
        assert!(Arc::ptr_eq(batch.column(0), filtered.column(0)));
        assert!(Arc::ptr_eq(batch.column(1), filtered.column(1)));
    }

    #[test]
    fn binary_left_join_keeps_null_keys_unmatched() {
        let left = RecordBatch::try_from_iter(vec![
            (
                "key_text",
                Arc::new(StringArray::from(vec![Some("A"), None, Some("B")])) as ArrayRef,
            ),
            (
                "key_number",
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(2)])) as ArrayRef,
            ),
        ])
        .unwrap();
        let right = RecordBatch::try_from_iter(vec![
            (
                "key_text_r",
                Arc::new(StringArray::from(vec!["A", "B"])) as ArrayRef,
            ),
            (
                "key_number_r",
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ),
        ])
        .unwrap();
        let source = left_join_source();
        let mut right_map = prepare_binary_right_key_map(&right, &source).unwrap();

        let (indices, profile) =
            build_join_indices_from_right_key_map(&left, &right, &source, &mut right_map).unwrap();

        assert_eq!(indices.left, vec![Some(0), Some(1), Some(2)]);
        assert_eq!(indices.right, vec![Some(0), None, Some(1)]);
        assert_eq!(indices.matched_rows, 2);
        assert_eq!(profile.right_key_map_build_sec, 0.0);
        assert!(profile.left_key_encode_sec > 0.0);
        assert!(profile.hash_probe_sec > 0.0);
    }

    #[test]
    fn calculated_fact_lookup_enriches_composite_keys_and_keeps_unmatched_null() {
        let left = RecordBatch::try_from_iter(vec![
            (
                "group",
                Arc::new(StringArray::from(vec![Some("A"), Some("A"), None])) as ArrayRef,
            ),
            (
                "x",
                Arc::new(arrow_array::Int32Array::from(vec![1, 2, 1])) as ArrayRef,
            ),
        ])
        .unwrap();
        let right = RecordBatch::try_from_iter(vec![
            (
                "lot_group",
                Arc::new(StringArray::from(vec!["A", "A"])) as ArrayRef,
            ),
            (
                "lookup_x",
                Arc::new(arrow_array::Int32Array::from(vec![1, 2])) as ArrayRef,
            ),
            (
                "lot_lookup.rate",
                Arc::new(arrow_array::Float64Array::from(vec![1.5, 2.5])) as ArrayRef,
            ),
        ])
        .unwrap();

        let output = enrich_many_to_one_lookup(
            left,
            &right,
            &["group".into(), "x".into()],
            &["lot_group".into(), "lookup_x".into()],
            "lot_lookup",
        )
        .unwrap();

        let rate = output
            .column_by_name("lot_lookup.rate")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert_eq!(rate.value(0), 1.5);
        assert_eq!(rate.value(1), 2.5);
        assert!(rate.is_null(2));
        assert_eq!(output.num_rows(), 3);
    }

    #[test]
    fn partition_filter_reuses_arrays_when_every_row_matches() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "partition",
                Arc::new(StringArray::from(vec!["KOR", "KOR"])) as ArrayRef,
            ),
            ("value", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ])
        .unwrap();

        let filtered = filter_column_value(&batch, "partition", "KOR").unwrap();

        assert_eq!(filtered.num_rows(), 2);
        assert!(Arc::ptr_eq(batch.column(0), filtered.column(0)));
        assert!(Arc::ptr_eq(batch.column(1), filtered.column(1)));
    }

    #[test]
    fn partition_filter_still_selects_rows_for_mixed_input() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "partition",
                Arc::new(StringArray::from(vec!["KOR", "USA", "KOR"])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
        ])
        .unwrap();

        let filtered = filter_column_value(&batch, "partition", "KOR").unwrap();
        let values = filtered
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(values.values(), &[1, 3]);
    }
}
