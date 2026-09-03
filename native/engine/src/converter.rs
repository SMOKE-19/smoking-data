use arrow_array::builder::{BooleanBuilder, ListBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Float32Type, Float64Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, RecordBatch, StringArray,
};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use arrow_select::take::take;
use bytes::Bytes;
#[cfg(unix)]
use libc::{off_t, posix_fadvise, POSIX_FADV_DONTNEED};
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::Result as ParquetResult;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{ChunkReader, Length};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::coord::read_coord_groups;
use crate::expression_executor::execute_expression_ir_retaining;
use crate::expression_ir::{validate_expression_ir, ExpressionIrDocument};
use crate::join::{enrich_many_to_one_lookup, load_lookup_projection};
use crate::long_fact::{to_persisted_long_fact_v1, FactColumnMetadata};
use crate::parser::{parse_json_f64_array, parse_json_i32_array, parse_json_string_array};
use crate::pivot::{pivot_record_batch, PivotConfig};
use crate::post_operations::{execute_post_operations, requires_complete_input, PostOperation};
use crate::reference::{build_dense_index, load_reference_map, DenseReferenceMap};

const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const ACTIVE_ORDER_COLUMN: &str = "__active_order";

#[derive(Clone)]
struct CountingChunkReader {
    file: Arc<File>,
    length: u64,
    bytes_read: Arc<AtomicU64>,
}

struct CountingRead {
    reader: BufReader<File>,
    bytes_read: Arc<AtomicU64>,
}

impl Read for CountingRead {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.reader.read(buffer)?;
        self.bytes_read
            .fetch_add(read as u64, AtomicOrdering::Relaxed);
        Ok(read)
    }
}

impl Length for CountingChunkReader {
    fn len(&self) -> u64 {
        self.length
    }
}

impl ChunkReader for CountingChunkReader {
    type T = CountingRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        Ok(CountingRead {
            reader: BufReader::new(file),
            bytes_read: Arc::clone(&self.bytes_read),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        let mut buffer = vec![0_u8; length];
        file.read_exact(&mut buffer)?;
        self.bytes_read
            .fetch_add(length as u64, AtomicOrdering::Relaxed);
        Ok(buffer.into())
    }
}

#[derive(Debug, Deserialize)]
struct RestoreConfig {
    #[serde(default = "default_restore_enabled")]
    enabled: bool,
    #[serde(default)]
    key_column: String,
    #[serde(default)]
    order_column: String,
    #[serde(default)]
    value_columns: Vec<String>,
    value_column: Option<String>,
    #[serde(default)]
    source_coord_columns: Vec<String>,
    #[serde(default)]
    lookup_coord_columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct WriterConfig {
    output_file_name: Option<String>,
    #[serde(default)]
    single_partition_guaranteed: bool,
    writer_input_contract: Option<WriterInputContract>,
    file_name_rule: Option<String>,
    source_stem: Option<String>,
    coord_chunk_id: Option<usize>,
    #[serde(default)]
    partition_columns: Vec<String>,
    file_name_prefix: Option<String>,
    #[serde(default)]
    projection_columns: Vec<ProjectionColumn>,
    #[serde(default)]
    output_columns: Vec<String>,
    #[serde(default)]
    output_projection_columns: Vec<ProjectionColumn>,
    reference_replace: Option<ReferenceReplaceConfigs>,
    expression_ir: Option<ExpressionIrDocument>,
    #[serde(default)]
    lookup_enrich: Vec<LookupEnrichConfig>,
    long_fact: Option<LongFactWriterConfig>,
    pivot: Option<PivotConfig>,
    output_row_group_rows: Option<usize>,
    #[serde(default)]
    pre_pivot_operations: Vec<PostOperation>,
    #[serde(default)]
    post_operations: Vec<PostOperation>,
    #[serde(default)]
    ordered_operations: Vec<OrderedOperation>,
    compression: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LookupEnrichConfig {
    alias: String,
    files: Vec<String>,
    source_keys: Vec<String>,
    lookup_keys: Vec<String>,
    value_columns: Vec<LookupValueColumnConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LookupValueColumnConfig {
    source: String,
    output: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LongFactWriterConfig {
    contract: String,
    identity_columns: Vec<String>,
    calculated_columns: Vec<LongFactColumnConfig>,
    generation_seq: i64,
    calculated_at_us: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LongFactColumnConfig {
    name: String,
    #[serde(default)]
    output_name: Option<String>,
    expression_hash: String,
    binding_hash: String,
    source_fingerprint: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderedOperation {
    operation_id: String,
    kind: String,
    execution_target: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ProjectionColumn {
    name: String,
    source: Option<String>,
    #[serde(default)]
    allow_missing: bool,
    dtype: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct WriterInputContract {
    contract_version: String,
    writer_mode: String,
    partition_value: Option<String>,
    #[serde(default)]
    single_partition_guaranteed: bool,
    #[serde(default)]
    expected_source_files: usize,
    #[serde(default)]
    expected_row_groups: usize,
    expected_input_rows: Option<usize>,
    expected_output_rows: Option<usize>,
    expected_payload_bytes: Option<usize>,
    #[serde(default)]
    output_columns: Vec<String>,
    #[serde(default)]
    partition_columns: Vec<String>,
    #[serde(default)]
    extras: HashMap<String, JsonValue>,
}

#[derive(Clone, Debug, Deserialize)]
struct ReferenceReplaceConfig {
    reference_parquet: String,
    source_column: String,
    reference_input_column: String,
    reference_output_column: String,
    output_column: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum ReferenceReplaceConfigs {
    One(ReferenceReplaceConfig),
    Many(Vec<ReferenceReplaceConfig>),
}

impl ReferenceReplaceConfigs {
    fn items(&self) -> Vec<&ReferenceReplaceConfig> {
        match self {
            Self::One(config) => vec![config],
            Self::Many(configs) => configs.iter().collect(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ValueColumnKind {
    Float32,
    Float64,
    Integer,
    Text,
}

enum ValueColumnBuilder {
    Float32(ListBuilder<PrimitiveBuilder<Float32Type>>),
    Float64(ListBuilder<PrimitiveBuilder<Float64Type>>),
    Integer(ListBuilder<PrimitiveBuilder<Int32Type>>),
    Text(ListBuilder<StringBuilder>),
}

// Native Arrow lists are the primary path. String-like JSON columns remain a
// compatibility fallback for datasets written by the legacy fastparquet flow.
enum SparseListInput {
    Native(ArrayRef),
    Json(Vec<String>),
}

impl SparseListInput {
    fn is_native(&self) -> bool {
        matches!(self, Self::Native(_))
    }
}

struct ManagedParquetWriter {
    writer: ArrowWriter<File>,
    final_path: PathBuf,
    temp_path: PathBuf,
}

struct TempOutputGuard {
    path: PathBuf,
    committed: bool,
}

impl Drop for TempOutputGuard {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

const FILE_OP_RETRY_ATTEMPTS: usize = 8;

#[derive(Default)]
struct DetailedProfile {
    batches_processed: usize,
    input_rows: usize,
    max_batch_rows: usize,
    max_dense_len: usize,
    max_restored_batch_array_bytes: usize,
    sum_restored_batch_array_bytes: usize,
    max_projected_input_array_bytes: usize,
    sum_projected_input_array_bytes: usize,
    max_pre_expression_array_bytes: usize,
    sum_pre_expression_array_bytes: usize,
    source_extract_sec: f64,
    dense_restore_sec: f64,
    record_batch_build_sec: f64,
    projection_sec: f64,
    active_order_sec: f64,
    reference_replace_sec: f64,
    expression_project_sec: f64,
    post_operation_sec: f64,
    concat_batches_sec: f64,
    active_order_sort_sec: f64,
    pivot_sec: f64,
    writer_write_sec: f64,
    cache_hint_sec: f64,
    cache_hint_calls: usize,
    native_list_fast_path_batch_columns: usize,
    json_list_fallback_batch_columns: usize,
}

type DenseIndex = HashMap<(i32, i32), usize>;
type DenseIndexCache = HashMap<String, DenseIndex>;

fn default_restore_enabled() -> bool {
    true
}

fn validate_writer_input_contract(writer_config: &WriterConfig) -> pyo3::PyResult<()> {
    let Some(contract) = &writer_config.writer_input_contract else {
        return Ok(());
    };
    if contract.contract_version.trim().is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "writer_input_contract.contract_version is required",
        ));
    }
    if contract.writer_mode.trim().is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "writer_input_contract.writer_mode is required",
        ));
    }
    if contract.single_partition_guaranteed != writer_config.single_partition_guaranteed {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "writer_input_contract.single_partition_guaranteed mismatch",
        ));
    }
    if !writer_config.partition_columns.is_empty()
        && contract.partition_columns != writer_config.partition_columns
    {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "writer_input_contract.partition_columns mismatch",
        ));
    }
    if !contract.output_columns.is_empty()
        && contract.output_columns != writer_config.output_columns
    {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "writer_input_contract.output_columns mismatch",
        ));
    }
    Ok(())
}

fn normalize_dtype(dtype: &str) -> &str {
    match dtype {
        "TEXT" | "Utf8" | "String" | "string" => "TEXT",
        "DATE" | "Date" | "date32[day]" => "DATE",
        "TIME" | "Time" | "time64[us]" => "TIME",
        "TIMESTAMP" | "Datetime" | "timestamp[us]" => "TIMESTAMP",
        "DURATION" | "Duration" | "duration[us]" => "DURATION",
        "BOOLEAN" | "BOOL" | "Boolean" | "bool" => "BOOLEAN",
        "TINYINT" | "INT8" | "Int8" | "int8" => "TINYINT",
        "SMALLINT" | "INT16" | "Int16" | "int16" => "SMALLINT",
        "INTEGER" | "INT32" | "Int32" | "int32" => "INTEGER",
        "BIGINT" | "INT64" | "Int64" | "int64" => "BIGINT",
        "FLOAT" | "Float32" | "float" => "FLOAT",
        "DOUBLE" | "Float64" | "double" => "DOUBLE",
        "INTEGER[]" | "List(Int8)" | "List(Int16)" | "List(Int32)" | "List(Int64)" => "INTEGER[]",
        "list<int8>" => "LIST_INT8",
        "list<int16>" => "LIST_INT16",
        "list<int32>" => "LIST_INT32",
        "list<int64>" => "LIST_INT64",
        "FLOAT[]" | "List(Float32)" | "list<float>" => "FLOAT[]",
        "DOUBLE[]" | "List(Float64)" | "list<double>" => "DOUBLE[]",
        "TEXT[]" | "List(Utf8)" | "List(String)" | "list<string>" => "TEXT[]",
        _ => dtype,
    }
}

fn parse_config(config_json: &str) -> pyo3::PyResult<RestoreConfig> {
    let mut config: RestoreConfig = serde_json::from_str(config_json).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid restore config: {err}"))
    })?;
    if !config.enabled {
        return Ok(config);
    }
    if config.key_column.trim().is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "key_column is required when list restore is enabled.",
        ));
    }
    if config.order_column.trim().is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "order_column is required when list restore is enabled.",
        ));
    }
    if config.source_coord_columns.len() != 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "source_coord_columns must contain exactly 2 items, got {}",
            config.source_coord_columns.len()
        )));
    }
    if config.lookup_coord_columns.len() != 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "lookup_coord_columns must contain exactly 2 items, got {}",
            config.lookup_coord_columns.len()
        )));
    }
    if config.value_columns.is_empty() {
        if let Some(value_column) = config.value_column.clone() {
            config.value_columns.push(value_column);
        }
    }
    if config.value_columns.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "value_column 또는 value_columns 를 반드시 지정해야 합니다.",
        ));
    }
    Ok(config)
}

#[cfg(unix)]
fn drop_file_cache_hint(file: &File) {
    let fd = file.as_raw_fd();
    unsafe {
        let _ = posix_fadvise(fd, 0 as off_t, 0 as off_t, POSIX_FADV_DONTNEED);
    }
}

#[cfg(not(unix))]
fn drop_file_cache_hint(_file: &File) {}

fn output_dtype_for_column(
    schema: &HashMap<String, String>,
    column_name: &str,
    input_dtype: &DataType,
) -> pyo3::PyResult<DataType> {
    let Some(raw_dtype) = schema.get(column_name) else {
        return Ok(input_dtype.clone());
    };
    if let Some((precision, scale)) = parse_decimal_type(raw_dtype) {
        return Ok(DataType::Decimal128(precision, scale));
    }
    let normalized = normalize_dtype(raw_dtype);
    let output = match normalized {
        "TEXT" => DataType::Utf8,
        "DATE" => DataType::Date32,
        "TIME" => DataType::Time64(TimeUnit::Microsecond),
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "DURATION" => DataType::Duration(TimeUnit::Microsecond),
        "BOOLEAN" => DataType::Boolean,
        "TINYINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "FLOAT[]" => DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
        "DOUBLE[]" => DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        "INTEGER[]" => DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        "LIST_INT8" => DataType::List(Arc::new(Field::new("item", DataType::Int8, true))),
        "LIST_INT16" => DataType::List(Arc::new(Field::new("item", DataType::Int16, true))),
        "LIST_INT32" => DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        "LIST_INT64" => DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        "TEXT[]" => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        _ => input_dtype.clone(),
    };
    Ok(output)
}

fn parse_decimal_type(dtype: &str) -> Option<(u8, i8)> {
    let normalized = dtype.trim().to_ascii_uppercase().replace(' ', "");
    let body = normalized.strip_prefix("DECIMAL(")?.strip_suffix(')')?;
    let (precision, scale) = body.split_once(',')?;
    let precision = precision.parse::<u8>().ok()?;
    let scale = scale.parse::<i8>().ok()?;
    (precision > 0 && scale >= 0 && scale as u8 <= precision).then_some((precision, scale))
}

fn value_column_kind(
    schema: &HashMap<String, String>,
    column_name: &str,
) -> pyo3::PyResult<ValueColumnKind> {
    let Some(raw_dtype) = schema.get(column_name) else {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "missing schema for restore value column {column_name}"
        )));
    };
    match normalize_dtype(raw_dtype) {
        "FLOAT[]" => Ok(ValueColumnKind::Float32),
        "DOUBLE[]" => Ok(ValueColumnKind::Float64),
        "INTEGER[]" => Ok(ValueColumnKind::Integer),
        "TEXT[]" => Ok(ValueColumnKind::Text),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "restore value column {column_name} must be INTEGER[]/DOUBLE[]/TEXT[], got {other}"
        ))),
    }
}

fn build_value_column_builders(
    schema: &HashMap<String, String>,
    value_columns: &[String],
) -> pyo3::PyResult<Vec<(ValueColumnKind, ValueColumnBuilder)>> {
    value_columns
        .iter()
        .map(|column_name| {
            let kind = value_column_kind(schema, column_name)?;
            let builder = match kind {
                ValueColumnKind::Float32 => ValueColumnBuilder::Float32(ListBuilder::new(
                    PrimitiveBuilder::<Float32Type>::new(),
                )),
                ValueColumnKind::Float64 => ValueColumnBuilder::Float64(ListBuilder::new(
                    PrimitiveBuilder::<Float64Type>::new(),
                )),
                ValueColumnKind::Integer => ValueColumnBuilder::Integer(ListBuilder::new(
                    PrimitiveBuilder::<Int32Type>::new(),
                )),
                ValueColumnKind::Text => {
                    ValueColumnBuilder::Text(ListBuilder::new(StringBuilder::new()))
                }
            };
            Ok((kind, builder))
        })
        .collect()
}

fn restore_dense_values<T: Clone>(
    value_sparse: &[Option<T>],
    coord_a_sparse: &[i32],
    coord_b_sparse: &[i32],
    dense_index: &HashMap<(i32, i32), usize>,
    dense_len: usize,
) -> Vec<Option<T>> {
    let sparse_len = value_sparse
        .len()
        .min(coord_a_sparse.len())
        .min(coord_b_sparse.len());
    let mut dense = vec![None; dense_len];
    for idx in 0..sparse_len {
        let key = (coord_a_sparse[idx], coord_b_sparse[idx]);
        if let Some(&dense_pos) = dense_index.get(&key) {
            dense[dense_pos] = value_sparse[idx].clone();
        }
    }
    dense
}

fn validate_sparse_row_lengths(
    column_name: &str,
    value_len: usize,
    coord_a_len: usize,
    coord_b_len: usize,
    input_path: &str,
    row_index: usize,
) -> pyo3::PyResult<()> {
    if value_len == coord_a_len && value_len == coord_b_len {
        return Ok(());
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "list_restore sparse length mismatch for {column_name} in {input_path} at row {row_index}: value={value_len}, coord_a={coord_a_len}, coord_b={coord_b_len}"
    )))
}

fn append_float_dense(
    builder: &mut ListBuilder<PrimitiveBuilder<Float64Type>>,
    dense_value: Vec<Option<f64>>,
) {
    for item in dense_value {
        match item {
            Some(value) => builder.values().append_value(value),
            None => builder.values().append_null(),
        }
    }
    builder.append(true);
}

fn append_float32_dense(
    builder: &mut ListBuilder<PrimitiveBuilder<Float32Type>>,
    dense_value: Vec<Option<f32>>,
) {
    for item in dense_value {
        match item {
            Some(value) => builder.values().append_value(value),
            None => builder.values().append_null(),
        }
    }
    builder.append(true);
}

fn append_int_dense(
    builder: &mut ListBuilder<PrimitiveBuilder<Int32Type>>,
    dense_value: Vec<Option<i32>>,
) {
    for item in dense_value {
        match item {
            Some(value) => builder.values().append_value(value),
            None => builder.values().append_null(),
        }
    }
    builder.append(true);
}

fn append_text_dense(builder: &mut ListBuilder<StringBuilder>, dense_value: Vec<Option<String>>) {
    for item in dense_value {
        match item {
            Some(value) => builder.values().append_value(value),
            None => builder.values().append_null(),
        }
    }
    builder.append(true);
}

fn finish_value_column_builder(builder: ValueColumnBuilder) -> ArrayRef {
    match builder {
        ValueColumnBuilder::Float32(mut inner) => Arc::new(inner.finish()) as ArrayRef,
        ValueColumnBuilder::Float64(mut inner) => Arc::new(inner.finish()) as ArrayRef,
        ValueColumnBuilder::Integer(mut inner) => Arc::new(inner.finish()) as ArrayRef,
        ValueColumnBuilder::Text(mut inner) => Arc::new(inner.finish()) as ArrayRef,
    }
}

fn sparse_list_input(
    batch: &RecordBatch,
    name: &str,
    input_path: &str,
    target_item_type: DataType,
) -> pyo3::PyResult<SparseListInput> {
    let index = batch.schema().index_of(name).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "missing {name} column in {input_path}: {err}"
        ))
    })?;
    let column = batch.column(index);
    let target_type = match column.data_type() {
        DataType::List(_) => {
            DataType::List(Arc::new(Field::new("item", target_item_type.clone(), true)))
        }
        DataType::LargeList(_) => {
            DataType::LargeList(Arc::new(Field::new("item", target_item_type, true)))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            return batch_string_values(batch, name, input_path).map(SparseListInput::Json);
        }
        other => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "restore source column {name} must be Arrow List/LargeList or string-like in {input_path}: {other:?}"
            )));
        }
    };
    cast(column.as_ref(), &target_type)
        .map(SparseListInput::Native)
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to normalize native list column {name} to {target_type:?} in {input_path}: {err}"
            ))
        })
}

fn native_list_row(input: &ArrayRef, row_index: usize) -> pyo3::PyResult<Option<ArrayRef>> {
    if let Some(values) = input.as_any().downcast_ref::<ListArray>() {
        return Ok((!values.is_null(row_index)).then(|| values.value(row_index)));
    }
    if let Some(values) = input.as_any().downcast_ref::<LargeListArray>() {
        return Ok((!values.is_null(row_index)).then(|| values.value(row_index)));
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "normalized list input has unsupported type: {:?}",
        input.data_type()
    )))
}

fn sparse_i32_row(input: &SparseListInput, row_index: usize) -> pyo3::PyResult<Vec<Option<i32>>> {
    match input {
        SparseListInput::Json(rows) => parse_json_i32_array(&rows[row_index])
            .map(|values| values.into_iter().map(Some).collect()),
        SparseListInput::Native(array) => {
            let Some(row) = native_list_row(array, row_index)? else {
                return Ok(Vec::new());
            };
            let values = row.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "normalized integer list row has unsupported type: {:?}",
                    row.data_type()
                ))
            })?;
            Ok((0..values.len())
                .map(|index| (!values.is_null(index)).then(|| values.value(index)))
                .collect())
        }
    }
}

fn sparse_f32_row(input: &SparseListInput, row_index: usize) -> pyo3::PyResult<Vec<Option<f32>>> {
    match input {
        SparseListInput::Json(rows) => parse_json_f64_array(&rows[row_index])
            .map(|values| values.into_iter().map(|value| Some(value as f32)).collect()),
        SparseListInput::Native(array) => {
            let Some(row) = native_list_row(array, row_index)? else {
                return Ok(Vec::new());
            };
            let values = row.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "normalized float32 list row has unsupported type: {:?}",
                    row.data_type()
                ))
            })?;
            Ok((0..values.len())
                .map(|index| (!values.is_null(index)).then(|| values.value(index)))
                .collect())
        }
    }
}

fn sparse_f64_row(input: &SparseListInput, row_index: usize) -> pyo3::PyResult<Vec<Option<f64>>> {
    match input {
        SparseListInput::Json(rows) => parse_json_f64_array(&rows[row_index])
            .map(|values| values.into_iter().map(Some).collect()),
        SparseListInput::Native(array) => {
            let Some(row) = native_list_row(array, row_index)? else {
                return Ok(Vec::new());
            };
            let values = row.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "normalized float64 list row has unsupported type: {:?}",
                    row.data_type()
                ))
            })?;
            Ok((0..values.len())
                .map(|index| (!values.is_null(index)).then(|| values.value(index)))
                .collect())
        }
    }
}

fn sparse_text_row(
    input: &SparseListInput,
    row_index: usize,
) -> pyo3::PyResult<Vec<Option<String>>> {
    match input {
        SparseListInput::Json(rows) => parse_json_string_array(&rows[row_index])
            .map(|values| values.into_iter().map(Some).collect()),
        SparseListInput::Native(array) => {
            let Some(row) = native_list_row(array, row_index)? else {
                return Ok(Vec::new());
            };
            let values = row.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "normalized text list row has unsupported type: {:?}",
                    row.data_type()
                ))
            })?;
            Ok((0..values.len())
                .map(|index| (!values.is_null(index)).then(|| values.value(index).to_string()))
                .collect())
        }
    }
}

fn batch_string_values(
    batch: &RecordBatch,
    name: &str,
    input_path: &str,
) -> pyo3::PyResult<Vec<String>> {
    let index = batch.schema().index_of(name).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "missing {name} column in {input_path}: {err}"
        ))
    })?;
    if let Some(values) = batch.column(index).as_any().downcast_ref::<StringArray>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    String::new()
                } else {
                    values.value(row_index).to_string()
                }
            })
            .collect());
    }
    if let Some(values) = batch
        .column(index)
        .as_any()
        .downcast_ref::<LargeStringArray>()
    {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    String::new()
                } else {
                    values.value(row_index).to_string()
                }
            })
            .collect());
    }
    if let Some(values) = batch.column(index).as_any().downcast_ref::<BinaryArray>() {
        return binary_array_to_strings(values, name, input_path);
    }
    if let Some(values) = batch
        .column(index)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
    {
        return large_binary_array_to_strings(values, name, input_path);
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "restore source column {name} must be string-like in {input_path}: {:?}",
        batch.column(index).data_type()
    )))
}

fn binary_array_to_strings(
    values: &BinaryArray,
    name: &str,
    input_path: &str,
) -> pyo3::PyResult<Vec<String>> {
    (0..values.len())
        .map(|row_index| {
            if values.is_null(row_index) {
                return Ok(String::new());
            }
            std::str::from_utf8(values.value(row_index))
                .map(|value| value.to_string())
                .map_err(|err| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "restore source column {name} contains invalid utf-8 bytes in {input_path} at row {row_index}: {err}"
                    ))
                })
        })
        .collect()
}

fn large_binary_array_to_strings(
    values: &LargeBinaryArray,
    name: &str,
    input_path: &str,
) -> pyo3::PyResult<Vec<String>> {
    (0..values.len())
        .map(|row_index| {
            if values.is_null(row_index) {
                return Ok(String::new());
            }
            std::str::from_utf8(values.value(row_index))
                .map(|value| value.to_string())
                .map_err(|err| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "restore source column {name} contains invalid utf-8 bytes in {input_path} at row {row_index}: {err}"
                    ))
                })
        })
        .collect()
}

fn batch_value_as_path_component(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
    column_name: &str,
) -> pyo3::PyResult<String> {
    let column = batch.column(column_index);
    if column.is_null(row_index) {
        return Ok("__null__".to_string());
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(sanitize_path_component(values.value(row_index)));
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(sanitize_path_component(values.value(row_index)));
    }
    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok(values.value(row_index).to_string());
    }
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok(values.value(row_index).to_string());
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "partition column {column_name} must be string/int32/int64, got {:?}",
        column.data_type()
    )))
}

fn sanitize_path_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|item| match item {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => item,
        })
        .collect();
    if sanitized.is_empty() {
        "__empty__".to_string()
    } else {
        sanitized
    }
}

fn build_output_schema(
    input_schema: &Schema,
    schema: &HashMap<String, String>,
) -> pyo3::PyResult<Arc<Schema>> {
    let mut fields = Vec::with_capacity(input_schema.fields().len());
    for field in input_schema.fields() {
        fields.push(Arc::new(field.as_ref().clone().with_data_type(
            output_dtype_for_column(schema, field.name(), field.data_type())?,
        )));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    )))
}

fn cast_array_for_output(
    array: ArrayRef,
    field: &Field,
    context: &str,
) -> pyo3::PyResult<ArrayRef> {
    if array.data_type() == field.data_type() {
        return Ok(array);
    }
    cast(array.as_ref(), field.data_type()).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to cast {context} to {:?}: {err}",
            field.data_type()
        ))
    })
}

fn apply_projection(
    batch: &RecordBatch,
    projection_columns: &[ProjectionColumn],
) -> pyo3::PyResult<RecordBatch> {
    if projection_columns.is_empty() {
        return Ok(batch.clone());
    }
    let mut fields = Vec::with_capacity(projection_columns.len());
    let mut columns = Vec::with_capacity(projection_columns.len());
    for projection in projection_columns {
        let source_name = projection.source.as_deref().unwrap_or(&projection.name);
        let index = match batch.schema().index_of(source_name) {
            Ok(index) => index,
            Err(_err) if projection.allow_missing => {
                let dtype = projection.dtype.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "missing projection source column {source_name} requires dtype"
                    ))
                })?;
                let schema = HashMap::from([(projection.name.clone(), dtype.clone())]);
                let target = output_dtype_for_column(&schema, &projection.name, &DataType::Null)?;
                fields.push(Arc::new(Field::new(&projection.name, target.clone(), true)));
                columns.push(arrow_array::new_null_array(&target, batch.num_rows()));
                continue;
            }
            Err(err) => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "missing projection source column {source_name}: {err}"
                )));
            }
        };
        let batch_schema = batch.schema();
        let source_field = batch_schema.field(index);
        fields.push(Arc::new(Field::new(
            &projection.name,
            source_field.data_type().clone(),
            source_field.is_nullable(),
        )));
        columns.push(batch.column(index).clone());
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build projected record batch: {err}"
        ))
    })
}

fn scalar_column_as_strings(
    batch: &RecordBatch,
    column_name: &str,
    context_path: &str,
) -> pyo3::PyResult<Vec<Option<String>>> {
    let index = batch.schema().index_of(column_name).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "missing {column_name} column in {context_path}: {err}"
        ))
    })?;
    let column = batch.column(index);
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    None
                } else {
                    Some(values.value(row_index).to_string())
                }
            })
            .collect());
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    None
                } else {
                    Some(values.value(row_index).to_string())
                }
            })
            .collect());
    }
    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    None
                } else {
                    Some(values.value(row_index).to_string())
                }
            })
            .collect());
    }
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    None
                } else {
                    Some(values.value(row_index).to_string())
                }
            })
            .collect());
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "reference replace column {column_name} must be string/int32/int64-like in {context_path}: {:?}",
        column.data_type()
    )))
}

fn load_reference_replace_map(
    config: &ReferenceReplaceConfig,
) -> pyo3::PyResult<HashMap<String, String>> {
    let file = File::open(&config.reference_parquet).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to open reference_replace parquet {}: {err}",
            config.reference_parquet
        ))
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to initialize reference_replace parquet reader {}: {err}",
                config.reference_parquet
            ))
        })?
        .build()
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to build reference_replace parquet reader {}: {err}",
                config.reference_parquet
            ))
        })?;
    let mut mapping = HashMap::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to read reference_replace batch {}: {err}",
                config.reference_parquet
            ))
        })?;
        let keys = scalar_column_as_strings(
            &batch,
            &config.reference_input_column,
            &config.reference_parquet,
        )?;
        let values = scalar_column_as_strings(
            &batch,
            &config.reference_output_column,
            &config.reference_parquet,
        )?;
        for (key, value) in keys.into_iter().zip(values) {
            if let (Some(key), Some(value)) = (key, value) {
                if mapping.insert(key.clone(), value).is_some() {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "duplicate reference_replace lookup key {key:?} in {}",
                        config.reference_parquet
                    )));
                }
            }
        }
    }
    Ok(mapping)
}

fn apply_reference_replace(
    batch: &RecordBatch,
    config: &ReferenceReplaceConfig,
    mapping: &HashMap<String, String>,
    context_path: &str,
) -> pyo3::PyResult<RecordBatch> {
    let target_column = config
        .output_column
        .as_ref()
        .unwrap_or(&config.source_column);
    let _source_index = batch
        .schema()
        .index_of(&config.source_column)
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "missing reference_replace source column {} in {context_path}: {err}",
                config.source_column
            ))
        })?;
    let source_values = scalar_column_as_strings(batch, &config.source_column, context_path)?;
    let mut replacement_builder = StringBuilder::new();
    for value in source_values {
        match value {
            Some(original) => {
                if let Some(replaced) = mapping.get(&original) {
                    replacement_builder.append_value(replaced);
                } else {
                    replacement_builder.append_value(original);
                }
            }
            None => replacement_builder.append_null(),
        }
    }
    let target_index = batch.schema().index_of(target_column).ok();
    let mut fields = Vec::with_capacity(batch.num_columns() + usize::from(target_index.is_none()));
    let mut columns = Vec::with_capacity(batch.num_columns() + usize::from(target_index.is_none()));
    let batch_schema = batch.schema();
    for index in 0..batch.num_columns() {
        let field = batch_schema.field(index);
        if target_index == Some(index) {
            fields.push(Arc::new(Field::new(target_column, DataType::Utf8, true)));
            columns.push(Arc::new(replacement_builder.finish()) as ArrayRef);
        } else {
            fields.push(Arc::new(field.clone()));
            columns.push(batch.column(index).clone());
        }
    }
    if target_index.is_none() {
        fields.push(Arc::new(Field::new(target_column, DataType::Utf8, true)));
        columns.push(Arc::new(replacement_builder.finish()) as ArrayRef);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build reference_replace batch for {context_path}: {err}"
        ))
    })
}

fn build_dense_index_cache(refs: &DenseReferenceMap) -> pyo3::PyResult<DenseIndexCache> {
    let mut cache = HashMap::with_capacity(refs.len());
    for (key, (coord_a, coord_b)) in refs {
        cache.insert(key.clone(), build_dense_index(coord_a, coord_b)?);
    }
    Ok(cache)
}

fn row_selection_from_offsets(
    row_offsets: &[usize],
    total_rows: usize,
) -> pyo3::PyResult<RowSelection> {
    if row_offsets.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "coord group must contain at least one row offset",
        ));
    }
    let mut selectors = Vec::new();
    let mut cursor = 0usize;
    for &offset in row_offsets {
        if offset >= total_rows {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "row_offset_in_group {offset} exceeds row group row count {total_rows}"
            )));
        }
        if offset > cursor {
            selectors.push(RowSelector::skip(offset - cursor));
        }
        selectors.push(RowSelector::select(1));
        cursor = offset + 1;
    }
    if cursor < total_rows {
        selectors.push(RowSelector::skip(total_rows - cursor));
    }
    Ok(RowSelection::from(selectors))
}

fn partition_key_for_row(
    batch: &RecordBatch,
    partition_columns: &[String],
    row_index: usize,
) -> pyo3::PyResult<Vec<(String, String)>> {
    partition_columns
        .iter()
        .map(|column_name| {
            let index = batch.schema().index_of(column_name).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "missing partition column {column_name}: {err}"
                ))
            })?;
            let value = batch_value_as_path_component(batch, index, row_index, column_name)?;
            Ok((column_name.clone(), value))
        })
        .collect()
}

fn output_path_for_partition(
    output_dir: &str,
    writer_config: &WriterConfig,
    partition_key: &[(String, String)],
    row_group_id: usize,
) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(output_dir);
    for (column, value) in partition_key {
        if writer_config.long_fact.is_some() {
            path.push(format!(
                "partition-{}-{}",
                sanitize_path_component(column),
                value
            ));
        } else {
            path.push(format!("{}={}", sanitize_path_component(column), value));
        }
    }
    let file_name = render_output_file_name(writer_config, Some(row_group_id));
    path.push(file_name);
    path
}

fn render_output_file_name(writer_config: &WriterConfig, row_group_id: Option<usize>) -> String {
    if let Some(rule) = &writer_config.file_name_rule {
        let source_stem = writer_config.source_stem.as_deref().unwrap_or("source");
        let chunk_id = writer_config.coord_chunk_id.unwrap_or(0);
        let row_group = row_group_id.unwrap_or(0);
        return rule
            .replace("<source_stem>", source_stem)
            .replace("{source_stem}", source_stem)
            .replace("<chunk_id>", &format!("{chunk_id:06}"))
            .replace("{chunk_id}", &format!("{chunk_id:06}"))
            .replace("<row_group_id>", &format!("{row_group:06}"))
            .replace("{row_group_id}", &format!("{row_group:06}"));
    }
    writer_config.output_file_name.clone().unwrap_or_else(|| {
        let prefix = writer_config
            .file_name_prefix
            .clone()
            .unwrap_or_else(|| "part".to_string());
        format!("{prefix}-00000.parquet")
    })
}

fn write_partitioned_batch(
    writers: &mut HashMap<String, ManagedParquetWriter>,
    output_dir: &str,
    writer_config: &WriterConfig,
    output_schema: Arc<Schema>,
    batch: &RecordBatch,
    row_group_id: usize,
) -> pyo3::PyResult<usize> {
    if writer_config.partition_columns.is_empty() {
        let output_path = output_path_for_partition(output_dir, writer_config, &[], row_group_id);
        write_batch_to_path(
            writers,
            output_path,
            output_schema,
            batch,
            writer_config.output_row_group_rows,
            writer_config.compression.as_deref(),
        )?;
        return Ok(1);
    }

    if writer_config.single_partition_guaranteed {
        let first_key = partition_key_for_row(batch, &writer_config.partition_columns, 0)?;
        let output_path =
            output_path_for_partition(output_dir, writer_config, &first_key, row_group_id);
        write_batch_to_path(
            writers,
            output_path,
            output_schema,
            batch,
            writer_config.output_row_group_rows,
            writer_config.compression.as_deref(),
        )?;
        return Ok(1);
    }

    let first_key = partition_key_for_row(batch, &writer_config.partition_columns, 0)?;
    let mut single_partition = true;
    for row_index in 1..batch.num_rows() {
        let row_key = partition_key_for_row(batch, &writer_config.partition_columns, row_index)?;
        if row_key != first_key {
            single_partition = false;
            break;
        }
    }
    if single_partition {
        let output_path =
            output_path_for_partition(output_dir, writer_config, &first_key, row_group_id);
        write_batch_to_path(
            writers,
            output_path,
            output_schema,
            batch,
            writer_config.output_row_group_rows,
            writer_config.compression.as_deref(),
        )?;
        return Ok(1);
    }

    let mut keys_by_row: Vec<Vec<(String, String)>> = Vec::with_capacity(batch.num_rows());
    let mut unique_keys: Vec<Vec<(String, String)>> = Vec::new();
    for row_index in 0..batch.num_rows() {
        let key = partition_key_for_row(batch, &writer_config.partition_columns, row_index)?;
        if !unique_keys.iter().any(|item| item == &key) {
            unique_keys.push(key.clone());
        }
        keys_by_row.push(key);
    }

    let mut files_touched = 0usize;
    for key in unique_keys {
        let mut builder = BooleanBuilder::with_capacity(batch.num_rows());
        for row_key in &keys_by_row {
            builder.append_value(row_key == &key);
        }
        let mask = builder.finish();
        let filtered = filter_record_batch(batch, &mask).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to filter partition batch: {err}"
            ))
        })?;
        if filtered.num_rows() == 0 {
            continue;
        }
        let output_path = output_path_for_partition(output_dir, writer_config, &key, row_group_id);
        write_batch_to_path(
            writers,
            output_path,
            output_schema.clone(),
            &filtered,
            writer_config.output_row_group_rows,
            writer_config.compression.as_deref(),
        )?;
        files_touched += 1;
    }
    Ok(files_touched)
}

fn write_batch_to_path(
    writers: &mut HashMap<String, ManagedParquetWriter>,
    output_path: PathBuf,
    output_schema: Arc<Schema>,
    batch: &RecordBatch,
    output_row_group_rows: Option<usize>,
    compression: Option<&str>,
) -> pyo3::PyResult<()> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to create output directory {}: {err}",
                parent.display()
            ))
        })?;
    }
    let key = output_path.to_string_lossy().to_string();
    if !writers.contains_key(&key) {
        let temp_path = temp_output_path(&output_path);
        let output_file = File::create(&temp_path).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to create temp output parquet {}: {err}",
                temp_path.display()
            ))
        })?;
        let mut properties = WriterProperties::builder()
            .set_compression(writer_compression(compression)?)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_offset_index_disabled(false);
        if let Some(rows) = output_row_group_rows {
            properties = properties.set_max_row_group_size(rows.max(1));
        }
        let properties = Some(properties.build());
        let writer =
            ArrowWriter::try_new(output_file, output_schema, properties).map_err(|err| {
                let _ = std::fs::remove_file(&temp_path);
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to initialize parquet writer {}: {err}",
                    temp_path.display()
                ))
            })?;
        writers.insert(
            key.clone(),
            ManagedParquetWriter {
                writer,
                final_path: output_path.clone(),
                temp_path,
            },
        );
    }
    writers
        .get_mut(&key)
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("missing partition writer"))?
        .writer
        .write(batch)
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to write restored parquet batch {}: {err}",
                output_path.display()
            ))
        })
}

fn temp_output_path(output_path: &std::path::Path) -> PathBuf {
    let file_name = output_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("part.parquet");
    output_path.with_file_name(format!(".{file_name}.tmp"))
}

fn retry_file_op<F>(mut op: F) -> std::io::Result<()>
where
    F: FnMut() -> std::io::Result<()>,
{
    let mut last_error: Option<std::io::Error> = None;
    for attempt in 0..FILE_OP_RETRY_ATTEMPTS {
        match op() {
            Ok(()) => return Ok(()),
            Err(err) => {
                last_error = Some(err);
                if attempt + 1 < FILE_OP_RETRY_ATTEMPTS {
                    std::thread::sleep(Duration::from_millis(25 * (attempt as u64 + 1)));
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| std::io::Error::other("file operation failed")))
}

fn remove_file_with_retry(path: &Path) -> std::io::Result<()> {
    retry_file_op(|| match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    })
}

fn rename_with_retry(from: &Path, to: &Path) -> std::io::Result<()> {
    retry_file_op(|| std::fs::rename(from, to))
}

#[allow(clippy::too_many_arguments)]
fn restore_batch_columns(
    batch: &RecordBatch,
    input_path: &str,
    config: &RestoreConfig,
    schema: &HashMap<String, String>,
    refs: &DenseReferenceMap,
    dense_index_cache: &DenseIndexCache,
    output_schema: Arc<Schema>,
    detailed_profile: Option<&mut DetailedProfile>,
) -> pyo3::PyResult<RecordBatch> {
    let extract_started = Instant::now();
    let key_values = batch_string_values(batch, &config.key_column, input_path)?;
    let mut value_builders = build_value_column_builders(schema, &config.value_columns)?;
    let value_sparse_inputs: Vec<SparseListInput> = config
        .value_columns
        .iter()
        .zip(value_builders.iter())
        .map(|(column_name, (kind, _))| {
            let item_type = match kind {
                ValueColumnKind::Float32 => DataType::Float32,
                ValueColumnKind::Float64 => DataType::Float64,
                ValueColumnKind::Integer => DataType::Int32,
                ValueColumnKind::Text => DataType::Utf8,
            };
            sparse_list_input(batch, column_name, input_path, item_type)
        })
        .collect::<pyo3::PyResult<Vec<_>>>()?;
    let coord_a_sparse_input = sparse_list_input(
        batch,
        &config.source_coord_columns[0],
        input_path,
        DataType::Int32,
    )?;
    let coord_b_sparse_input = sparse_list_input(
        batch,
        &config.source_coord_columns[1],
        input_path,
        DataType::Int32,
    )?;
    let native_list_fast_path_batch_columns = value_sparse_inputs
        .iter()
        .chain([&coord_a_sparse_input, &coord_b_sparse_input])
        .filter(|input| input.is_native())
        .count();
    let json_list_fallback_batch_columns =
        value_sparse_inputs.len() + 2 - native_list_fast_path_batch_columns;
    let extract_sec = extract_started.elapsed().as_secs_f64();

    let mut coord_a_builder = ListBuilder::new(PrimitiveBuilder::<Int32Type>::new());
    let mut coord_b_builder = ListBuilder::new(PrimitiveBuilder::<Int32Type>::new());

    let dense_restore_started = Instant::now();
    for row_index in 0..batch.num_rows() {
        let group_key = &key_values[row_index];
        let (dense_coord_a, dense_coord_b) = refs.get(group_key).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("unknown lookup key '{group_key}'"))
        })?;
        let dense_index = dense_index_cache.get(group_key).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "missing dense index for lookup key '{group_key}'"
            ))
        })?;

        let coord_a_sparse = sparse_i32_row(&coord_a_sparse_input, row_index)?;
        let coord_b_sparse = sparse_i32_row(&coord_b_sparse_input, row_index)?;
        let coord_a_values: Vec<i32> = coord_a_sparse.iter().flatten().copied().collect();
        let coord_b_values: Vec<i32> = coord_b_sparse.iter().flatten().copied().collect();
        if coord_a_values.len() != coord_a_sparse.len()
            || coord_b_values.len() != coord_b_sparse.len()
        {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "list_restore coordinate columns cannot contain null elements in {input_path} at row {row_index}"
            )));
        }

        for (((kind, builder), value_sparse_input), column_name) in value_builders
            .iter_mut()
            .zip(value_sparse_inputs.iter())
            .zip(config.value_columns.iter())
        {
            match kind {
                ValueColumnKind::Float32 => {
                    let value_sparse = sparse_f32_row(value_sparse_input, row_index)?;
                    validate_sparse_row_lengths(
                        column_name,
                        value_sparse.len(),
                        coord_a_values.len(),
                        coord_b_values.len(),
                        input_path,
                        row_index,
                    )?;
                    let dense_value = restore_dense_values(
                        &value_sparse,
                        &coord_a_values,
                        &coord_b_values,
                        dense_index,
                        dense_coord_a.len(),
                    );
                    if let ValueColumnBuilder::Float32(inner) = builder {
                        append_float32_dense(inner, dense_value);
                    }
                }
                ValueColumnKind::Float64 => {
                    let value_sparse = sparse_f64_row(value_sparse_input, row_index)?;
                    validate_sparse_row_lengths(
                        column_name,
                        value_sparse.len(),
                        coord_a_values.len(),
                        coord_b_values.len(),
                        input_path,
                        row_index,
                    )?;
                    let dense_value = restore_dense_values(
                        &value_sparse,
                        &coord_a_values,
                        &coord_b_values,
                        dense_index,
                        dense_coord_a.len(),
                    );
                    if let ValueColumnBuilder::Float64(inner) = builder {
                        append_float_dense(inner, dense_value);
                    }
                }
                ValueColumnKind::Integer => {
                    let value_sparse = sparse_i32_row(value_sparse_input, row_index)?;
                    validate_sparse_row_lengths(
                        column_name,
                        value_sparse.len(),
                        coord_a_values.len(),
                        coord_b_values.len(),
                        input_path,
                        row_index,
                    )?;
                    let dense_value = restore_dense_values(
                        &value_sparse,
                        &coord_a_values,
                        &coord_b_values,
                        dense_index,
                        dense_coord_a.len(),
                    );
                    if let ValueColumnBuilder::Integer(inner) = builder {
                        append_int_dense(inner, dense_value);
                    }
                }
                ValueColumnKind::Text => {
                    let value_sparse = sparse_text_row(value_sparse_input, row_index)?;
                    validate_sparse_row_lengths(
                        column_name,
                        value_sparse.len(),
                        coord_a_values.len(),
                        coord_b_values.len(),
                        input_path,
                        row_index,
                    )?;
                    let dense_value = restore_dense_values(
                        &value_sparse,
                        &coord_a_values,
                        &coord_b_values,
                        dense_index,
                        dense_coord_a.len(),
                    );
                    if let ValueColumnBuilder::Text(inner) = builder {
                        append_text_dense(inner, dense_value);
                    }
                }
            }
        }

        for item in dense_coord_a {
            coord_a_builder.values().append_value(*item);
        }
        coord_a_builder.append(true);

        for item in dense_coord_b {
            coord_b_builder.values().append_value(*item);
        }
        coord_b_builder.append(true);
    }
    let dense_restore_sec = dense_restore_started.elapsed().as_secs_f64();

    let finished_value_columns: HashMap<String, ArrayRef> = config
        .value_columns
        .iter()
        .cloned()
        .zip(
            value_builders
                .into_iter()
                .map(|(_, builder)| finish_value_column_builder(builder)),
        )
        .collect();

    let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let batch_schema = batch.schema();
    for index in 0..batch.num_columns() {
        let field = batch_schema.field(index);
        let output_field = output_schema.field(index);
        let column_name = field.name();
        if let Some(restored_array) = finished_value_columns.get(column_name.as_str()) {
            output_columns.push(cast_array_for_output(
                restored_array.clone(),
                output_field,
                column_name,
            )?);
        } else if column_name == &config.source_coord_columns[0] {
            output_columns.push(cast_array_for_output(
                Arc::new(coord_a_builder.finish()) as ArrayRef,
                output_field,
                column_name,
            )?);
        } else if column_name == &config.source_coord_columns[1] {
            output_columns.push(cast_array_for_output(
                Arc::new(coord_b_builder.finish()) as ArrayRef,
                output_field,
                column_name,
            )?);
        } else {
            output_columns.push(cast_array_for_output(
                batch.column(index).clone(),
                output_field,
                column_name,
            )?);
        }
    }

    let build_started = Instant::now();
    let restored_batch = RecordBatch::try_new(output_schema, output_columns).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build restored record batch for {input_path}: {err}"
        ))
    })?;
    let build_sec = build_started.elapsed().as_secs_f64();

    if let Some(profile) = detailed_profile {
        profile.batches_processed += 1;
        profile.input_rows += batch.num_rows();
        profile.max_batch_rows = profile.max_batch_rows.max(batch.num_rows());
        profile.max_dense_len = profile.max_dense_len.max(
            refs.values()
                .map(|(coord_a, _)| coord_a.len())
                .max()
                .unwrap_or(0),
        );
        profile.source_extract_sec += extract_sec;
        profile.dense_restore_sec += dense_restore_sec;
        profile.record_batch_build_sec += build_sec;
        profile.native_list_fast_path_batch_columns += native_list_fast_path_batch_columns;
        profile.json_list_fallback_batch_columns += json_list_fallback_batch_columns;
    }

    Ok(restored_batch)
}

fn cast_projected_batch_for_output(
    batch: &RecordBatch,
    schema: &HashMap<String, String>,
    detailed_profile: Option<&mut DetailedProfile>,
) -> pyo3::PyResult<RecordBatch> {
    let build_started = Instant::now();
    let output_schema = build_output_schema(batch.schema().as_ref(), schema)?;
    let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let batch_schema = batch.schema();
    for index in 0..batch.num_columns() {
        let field = batch_schema.field(index);
        let output_field = output_schema.field(index);
        output_columns.push(cast_array_for_output(
            batch.column(index).clone(),
            output_field,
            field.name(),
        )?);
    }
    let restored_batch = RecordBatch::try_new(output_schema, output_columns).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build casted record batch: {err}"
        ))
    })?;
    if let Some(profile) = detailed_profile {
        profile.batches_processed += 1;
        profile.input_rows += batch.num_rows();
        profile.max_batch_rows = profile.max_batch_rows.max(batch.num_rows());
        profile.record_batch_build_sec += build_started.elapsed().as_secs_f64();
    }
    Ok(restored_batch)
}

#[allow(clippy::too_many_arguments)]
fn restore_parquet_to_parquet_internal(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
    print_timing: bool,
    detailed: bool,
) -> pyo3::PyResult<HashMap<String, f64>> {
    if print_timing {
        println!(
            "[smoking_data_engine_rs] version={PKG_VERSION} input_parquet_path={input_parquet_path} output_parquet_path={output_parquet_path}"
        );
    }

    let total_started = Instant::now();
    let config = parse_config(&config_json)?;

    let reference_started = Instant::now();
    let refs = load_reference_map(
        &lookup_path,
        &config.key_column,
        &config.order_column,
        &config.lookup_coord_columns,
    )?;
    let dense_index_cache = build_dense_index_cache(&refs)?;
    let reference_load_sec = reference_started.elapsed().as_secs_f64();
    if print_timing {
        println!("[smoking_data_engine_rs] reference_load_sec={reference_load_sec:.6}");
    }

    let input_file = File::open(&input_parquet_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to open input parquet {input_parquet_path}: {err}"
        ))
    })?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(input_file).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to initialize parquet reader {input_parquet_path}: {err}"
        ))
    })?;
    if let Some(size) = batch_size {
        builder = builder.with_batch_size(size);
    }
    let output_schema = build_output_schema(builder.schema().as_ref(), &schema)?;
    let reader = builder.build().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build parquet reader {input_parquet_path}: {err}"
        ))
    })?;

    let final_output_path = PathBuf::from(&output_parquet_path);
    let temporary_output_path = temp_output_path(&final_output_path);
    remove_file_with_retry(&temporary_output_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to clean stale temp output parquet {}: {err}",
            temporary_output_path.display()
        ))
    })?;
    let mut output_guard = TempOutputGuard {
        path: temporary_output_path.clone(),
        committed: false,
    };
    let output_file = File::create(&temporary_output_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to create temp output parquet {}: {err}",
            temporary_output_path.display()
        ))
    })?;
    let mut writer =
        ArrowWriter::try_new(output_file, output_schema.clone(), None).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to initialize parquet writer {output_parquet_path}: {err}"
            ))
        })?;

    let restore_started = Instant::now();
    let mut rows_written = 0usize;
    let mut detailed_profile = DetailedProfile::default();
    for batch_result in reader {
        let batch = batch_result.map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to read parquet batch {input_parquet_path}: {err}"
            ))
        })?;
        let restored = restore_batch_columns(
            &batch,
            &input_parquet_path,
            &config,
            &schema,
            &refs,
            &dense_index_cache,
            output_schema.clone(),
            if detailed {
                Some(&mut detailed_profile)
            } else {
                None
            },
        )?;
        rows_written += restored.num_rows();
        if detailed {
            let restored_batch_array_bytes = restored.get_array_memory_size();
            detailed_profile.max_restored_batch_array_bytes = detailed_profile
                .max_restored_batch_array_bytes
                .max(restored_batch_array_bytes);
            detailed_profile.sum_restored_batch_array_bytes += restored_batch_array_bytes;
        }
        let writer_write_started = Instant::now();
        writer.write(&restored).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to write restored parquet batch {output_parquet_path}: {err}"
            ))
        })?;
        if detailed {
            detailed_profile.writer_write_sec += writer_write_started.elapsed().as_secs_f64();
        }
    }
    let restore_sec = restore_started.elapsed().as_secs_f64();
    if print_timing {
        println!("[smoking_data_engine_rs] restore_sec={restore_sec:.6}");
    }

    let write_started = Instant::now();
    writer.close().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to finalize temp output parquet {}: {err}",
            temporary_output_path.display()
        ))
    })?;
    if final_output_path.exists() {
        remove_file_with_retry(&final_output_path).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to replace existing output parquet {}: {err}",
                final_output_path.display()
            ))
        })?;
    }
    rename_with_retry(&temporary_output_path, &final_output_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to publish output parquet {} -> {}: {err}",
            temporary_output_path.display(),
            final_output_path.display()
        ))
    })?;
    output_guard.committed = true;
    let parquet_write_sec = write_started.elapsed().as_secs_f64();
    if print_timing {
        println!("[smoking_data_engine_rs] parquet_write_sec={parquet_write_sec:.6}");
    }

    let total_sec = total_started.elapsed().as_secs_f64();
    if print_timing {
        println!("[smoking_data_engine_rs] total_sec={total_sec:.6}");
    }

    if drop_cache_hint {
        let cache_hint_started = Instant::now();
        if let Ok(file) = File::open(&input_parquet_path) {
            drop_file_cache_hint(&file);
            if detailed {
                detailed_profile.cache_hint_calls += 1;
            }
        }
        if let Ok(file) = File::open(&output_parquet_path) {
            drop_file_cache_hint(&file);
            if detailed {
                detailed_profile.cache_hint_calls += 1;
            }
        }
        if detailed {
            detailed_profile.cache_hint_sec += cache_hint_started.elapsed().as_secs_f64();
        }
    }

    let mut stats = HashMap::new();
    stats.insert("rows_written".to_string(), rows_written as f64);
    stats.insert("selected_row_count".to_string(), rows_written as f64);
    stats.insert("reference_load_sec".to_string(), reference_load_sec);
    stats.insert("restore_sec".to_string(), restore_sec);
    stats.insert("parquet_write_sec".to_string(), parquet_write_sec);
    stats.insert("restore_elapsed_sec".to_string(), restore_sec);
    stats.insert("write_elapsed_sec".to_string(), parquet_write_sec);
    stats.insert("total_sec".to_string(), total_sec);
    if detailed {
        stats.insert(
            "batches_processed".to_string(),
            detailed_profile.batches_processed as f64,
        );
        stats.insert("input_rows".to_string(), detailed_profile.input_rows as f64);
        stats.insert(
            "max_batch_rows".to_string(),
            detailed_profile.max_batch_rows as f64,
        );
        stats.insert(
            "max_dense_len".to_string(),
            detailed_profile.max_dense_len as f64,
        );
        stats.insert(
            "max_restored_batch_array_bytes".to_string(),
            detailed_profile.max_restored_batch_array_bytes as f64,
        );
        stats.insert(
            "avg_restored_batch_array_bytes".to_string(),
            if detailed_profile.batches_processed == 0 {
                0.0
            } else {
                detailed_profile.sum_restored_batch_array_bytes as f64
                    / detailed_profile.batches_processed as f64
            },
        );
        stats.insert(
            "value_column_count".to_string(),
            config.value_columns.len() as f64,
        );
        stats.insert(
            "native_list_fast_path_batch_columns".to_string(),
            detailed_profile.native_list_fast_path_batch_columns as f64,
        );
        stats.insert(
            "json_list_fallback_batch_columns".to_string(),
            detailed_profile.json_list_fallback_batch_columns as f64,
        );
        stats.insert(
            "source_extract_sec".to_string(),
            detailed_profile.source_extract_sec,
        );
        stats.insert(
            "dense_restore_sec".to_string(),
            detailed_profile.dense_restore_sec,
        );
        stats.insert(
            "record_batch_build_sec".to_string(),
            detailed_profile.record_batch_build_sec,
        );
        stats.insert(
            "writer_write_sec".to_string(),
            detailed_profile.writer_write_sec,
        );
        stats.insert(
            "cache_hint_sec".to_string(),
            detailed_profile.cache_hint_sec,
        );
        stats.insert(
            "cache_hint_calls".to_string(),
            detailed_profile.cache_hint_calls as f64,
        );
        if let Ok(metadata) = std::fs::metadata(&output_parquet_path) {
            stats.insert("output_file_size_bytes".to_string(), metadata.len() as f64);
        }
    }
    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
pub fn restore_parquet_to_parquet_impl(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
    print_timing: bool,
) -> pyo3::PyResult<HashMap<String, f64>> {
    restore_parquet_to_parquet_internal(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        batch_size,
        drop_cache_hint,
        print_timing,
        false,
    )
}

pub fn restore_parquet_to_parquet_profiled_impl(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
) -> pyo3::PyResult<HashMap<String, f64>> {
    restore_parquet_to_parquet_internal(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        batch_size,
        drop_cache_hint,
        false,
        true,
    )
}

fn expression_node_has_window(node: &crate::expression_ir::ExpressionNode) -> bool {
    use crate::expression_ir::ExpressionNode;
    match node {
        ExpressionNode::Window { .. } => true,
        ExpressionNode::Unary { operand, .. } => expression_node_has_window(operand),
        ExpressionNode::Binary { left, right, .. } => {
            expression_node_has_window(left) || expression_node_has_window(right)
        }
        ExpressionNode::Call { arguments, .. } => arguments.iter().any(expression_node_has_window),
        ExpressionNode::Case {
            branches,
            otherwise,
        } => {
            branches.iter().any(|branch| {
                expression_node_has_window(&branch.when) || expression_node_has_window(&branch.then)
            }) || expression_node_has_window(otherwise)
        }
        ExpressionNode::Cast { expression, .. } | ExpressionNode::Alias { expression, .. } => {
            expression_node_has_window(expression)
        }
        ExpressionNode::Column { .. } | ExpressionNode::Literal { .. } => false,
    }
}

fn document_has_window(document: &ExpressionIrDocument) -> bool {
    document.layers.iter().any(|layer| {
        layer
            .expressions
            .iter()
            .any(|expression| expression_node_has_window(&expression.expr))
    })
}

fn append_active_order(batch: RecordBatch, orders: &[i64]) -> pyo3::PyResult<RecordBatch> {
    if batch.num_rows() != orders.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "active-order length mismatch: rows={}, orders={}",
            batch.num_rows(),
            orders.len()
        )));
    }
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    let mut columns = batch.columns().to_vec();
    fields.push(Arc::new(Field::new(
        ACTIVE_ORDER_COLUMN,
        DataType::Int64,
        false,
    )));
    columns.push(Arc::new(Int64Array::from(orders.to_vec())) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to append active-order column: {error}"
        ))
    })
}

fn sort_by_active_order(batch: &RecordBatch) -> pyo3::PyResult<RecordBatch> {
    let values = batch
        .column_by_name(ACTIVE_ORDER_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("missing Int64 active-order column")
        })?;
    let mut indexes: Vec<u32> = (0..batch.num_rows() as u32).collect();
    indexes.sort_unstable_by_key(|index| values.value(*index as usize));
    let indexes = arrow_array::UInt32Array::from(indexes);
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indexes, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to restore active row order: {error}"
            ))
        })?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build active-ordered batch: {error}"
        ))
    })
}

fn execute_and_project_writer_batch(
    batch: RecordBatch,
    writer_config: &WriterConfig,
    context: &str,
    _fingerprint_offset: usize,
) -> pyo3::PyResult<RecordBatch> {
    let final_live_columns = writer_final_live_columns(writer_config);
    let batch = match &writer_config.expression_ir {
        Some(document) => execute_expression_ir_retaining(batch, document, &final_live_columns)
            .map_err(|error| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to execute expression IR for {context}: {error}"
                ))
            })?,
        None => batch,
    };
    let batch =
        execute_post_operations(batch, &writer_config.pre_pivot_operations).map_err(|error| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to execute pre-pivot operations for {context}: {error}"
            ))
        })?;
    if let Some(config) = &writer_config.long_fact {
        let metadata = config
            .calculated_columns
            .iter()
            .map(|item| {
                Ok(FactColumnMetadata {
                    expression_hash: item.expression_hash.clone(),
                    binding_hash: item.binding_hash.clone(),
                    source_fingerprint: item.source_fingerprint.clone(),
                })
            })
            .collect::<pyo3::PyResult<Vec<_>>>()?;
        let persisted = to_persisted_long_fact_v1(
            &batch,
            &config.identity_columns,
            &config
                .calculated_columns
                .iter()
                .map(|item| item.name.clone())
                .collect::<Vec<_>>(),
            &metadata,
            config.generation_seq,
            config.calculated_at_us,
        )
        .map_err(|error| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to emit persisted Long Fact for {context}: {error}"
            ))
        })?;
        let published_names = config
            .calculated_columns
            .iter()
            .map(|item| item.output_name.as_deref().unwrap_or(item.name.as_str()))
            .flat_map(|name| std::iter::repeat_n(name, batch.num_rows()))
            .collect::<Vec<_>>();
        let column_index = persisted
            .schema()
            .index_of("_sd_column_name")
            .map_err(|error| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "long_fact.schema_mismatch: missing _sd_column_name: {error}"
                ))
            })?;
        let mut columns = persisted.columns().to_vec();
        columns[column_index] = Arc::new(StringArray::from(published_names));
        return RecordBatch::try_new(persisted.schema(), columns).map_err(|error| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to apply Long Fact published names for {context}: {error}"
            ))
        });
    }
    if !writer_config.output_projection_columns.is_empty() {
        return apply_projection(&batch, &writer_config.output_projection_columns);
    }
    let output_columns = if writer_config.output_columns.is_empty() {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| name != ACTIVE_ORDER_COLUMN)
            .collect::<Vec<_>>()
    } else {
        writer_config.output_columns.clone()
    };
    let output_projection: Vec<ProjectionColumn> = output_columns
        .into_iter()
        .map(|name| ProjectionColumn {
            source: Some(name.clone()),
            name,
            allow_missing: false,
            dtype: None,
        })
        .collect();
    apply_projection(&batch, &output_projection)
}

fn writer_final_live_columns(writer_config: &WriterConfig) -> HashSet<String> {
    if let Some(config) = &writer_config.long_fact {
        return config
            .identity_columns
            .iter()
            .cloned()
            .chain(
                config
                    .calculated_columns
                    .iter()
                    .map(|item| item.name.clone()),
            )
            .collect();
    }
    if !writer_config.output_projection_columns.is_empty() {
        return writer_config
            .output_projection_columns
            .iter()
            .filter_map(|item| item.source.clone())
            .collect();
    }
    HashSet::new()
}

#[allow(clippy::too_many_arguments)]
pub fn execute_curated_task_impl(
    coord_path: String,
    output_dir: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    writer_config_json: Option<String>,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
    print_timing: bool,
) -> pyo3::PyResult<HashMap<String, f64>> {
    if print_timing {
        println!(
            "[smoking_data_engine_rs] version={PKG_VERSION} coord_path={coord_path} output_dir={output_dir}"
        );
    }

    let total_started = Instant::now();
    let config = parse_config(&config_json)?;
    let writer_config = match writer_config_json {
        Some(raw) if !raw.trim().is_empty() => {
            serde_json::from_str::<WriterConfig>(&raw).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid writer config: {err}"))
            })?
        }
        _ => WriterConfig {
            output_file_name: None,
            single_partition_guaranteed: false,
            writer_input_contract: None,
            file_name_rule: None,
            source_stem: None,
            coord_chunk_id: None,
            partition_columns: Vec::new(),
            file_name_prefix: None,
            projection_columns: Vec::new(),
            output_columns: Vec::new(),
            output_projection_columns: Vec::new(),
            reference_replace: None,
            expression_ir: None,
            lookup_enrich: Vec::new(),
            long_fact: None,
            pivot: None,
            output_row_group_rows: None,
            pre_pivot_operations: Vec::new(),
            post_operations: Vec::new(),
            ordered_operations: Vec::new(),
            compression: None,
        },
    };
    validate_writer_input_contract(&writer_config)?;
    validate_ordered_operations(&writer_config)?;

    let reference_started = Instant::now();
    let restore_refs = if config.enabled {
        Some(load_reference_map(
            &lookup_path,
            &config.key_column,
            &config.order_column,
            &config.lookup_coord_columns,
        )?)
    } else {
        None
    };
    let dense_index_cache = match &restore_refs {
        Some(refs) => Some(build_dense_index_cache(refs)?),
        None => None,
    };
    let reference_replace_maps = writer_config
        .reference_replace
        .as_ref()
        .map(|configs| {
            configs
                .items()
                .into_iter()
                .map(|config| Ok((config.clone(), load_reference_replace_map(config)?)))
                .collect::<pyo3::PyResult<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    let lookup_batches = writer_config
        .lookup_enrich
        .iter()
        .map(|config| {
            let values = config
                .value_columns
                .iter()
                .map(|item| (item.source.clone(), item.output.clone()))
                .collect::<Vec<_>>();
            load_lookup_projection(&config.files, &config.lookup_keys, &values, &config.alias)
                .map(|batch| (config, batch))
                .map_err(pyo3::exceptions::PyValueError::new_err)
        })
        .collect::<pyo3::PyResult<Vec<_>>>()?;
    let reference_load_sec = reference_started.elapsed().as_secs_f64();
    if let Some(document) = &writer_config.expression_ir {
        validate_expression_ir(document).map_err(|reason| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "invalid writer expression IR: {reason}"
            ))
        })?;
    }
    if let Some(config) = &writer_config.long_fact {
        if config.contract != "long_fact_v1" {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "long_fact.invalid_contract: writer contract must be long_fact_v1",
            ));
        }
        if config.identity_columns.is_empty() || config.calculated_columns.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "long_fact.invalid_contract: identity and calculated columns are required",
            ));
        }
        if document_has_window(writer_config.expression_ir.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "long_fact.invalid_contract: expression_ir is required",
            )
        })?) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "long_fact.window_unsupported: task-local fingerprint order cannot cover windows",
            ));
        }
    }
    let window_execution = writer_config
        .expression_ir
        .as_ref()
        .is_some_and(document_has_window);
    let pivot_execution = writer_config
        .pivot
        .as_ref()
        .is_some_and(|config| config.enabled);
    let complete_post_execution = requires_complete_input(&writer_config.post_operations);

    let coord_started = Instant::now();
    let coord_groups = read_coord_groups(&coord_path)?;
    let coord_read_sec = coord_started.elapsed().as_secs_f64();
    if coord_groups.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "coord file has no row groups: {coord_path}"
        )));
    }

    std::fs::create_dir_all(&output_dir).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to create output dir {output_dir}: {err}"
        ))
    })?;
    let mut writers: HashMap<String, ManagedParquetWriter> = HashMap::new();
    let mut output_schema: Option<Arc<Schema>> = None;
    let mut rows_written = 0usize;
    let mut source_files_seen: HashSet<String> = HashSet::new();
    let mut row_group_count = 0usize;
    let mut output_file_write_touches = 0usize;
    let mut detailed_profile = DetailedProfile::default();
    let mut long_fact_fingerprint_cursor = 0usize;
    let source_bytes_read = Arc::new(AtomicU64::new(0));
    let mut window_batches: Vec<RecordBatch> = Vec::new();
    let mut pivot_duplicate_first_cells = 0usize;
    let mut pivot_column_values = 0usize;
    let restore_started = Instant::now();

    for group in &coord_groups {
        source_files_seen.insert(group.source_file.clone());
        row_group_count += 1;
        let input_file = File::open(&group.source_file).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to open input parquet {}: {err}",
                group.source_file
            ))
        })?;
        let input_length = input_file
            .metadata()
            .map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to stat input parquet {}: {err}",
                    group.source_file
                ))
            })?
            .len();
        let input_reader = CountingChunkReader {
            file: Arc::new(input_file),
            length: input_length,
            bytes_read: Arc::clone(&source_bytes_read),
        };
        let reader_options = ArrowReaderOptions::new().with_page_index(true);
        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new_with_options(input_reader, reader_options)
                .map_err(|err| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "failed to initialize parquet reader {}: {err}",
                        group.source_file
                    ))
                })?;
        if group.row_group_id >= builder.metadata().num_row_groups() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "row_group_id {} exceeds row group count {} for {}",
                group.row_group_id,
                builder.metadata().num_row_groups(),
                group.source_file
            )));
        }
        let row_group_rows = builder
            .metadata()
            .row_group(group.row_group_id)
            .num_rows()
            .try_into()
            .map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid row count for row group {} in {}",
                    group.row_group_id, group.source_file
                ))
            })?;
        let selection = row_selection_from_offsets(&group.row_offsets, row_group_rows)?;
        if !writer_config.projection_columns.is_empty() {
            let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
            let source_columns = writer_config
                .projection_columns
                .iter()
                .map(|column| column.source.as_deref().unwrap_or(column.name.as_str()));
            builder =
                builder.with_projection(ProjectionMask::columns(&schema_descr, source_columns));
        }
        if let Some(size) = batch_size {
            builder = builder.with_batch_size(size);
        }
        let reader = builder
            .with_row_groups(vec![group.row_group_id])
            .with_row_selection(selection)
            .build()
            .map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to build parquet reader {}: {err}",
                    group.source_file
                ))
            })?;
        let mut selected_row_cursor = 0usize;
        for batch_result in reader {
            let raw_batch = batch_result.map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to read selected parquet batch {}: {err}",
                    group.source_file
                ))
            })?;
            let projection_started = Instant::now();
            let batch = apply_projection(&raw_batch, &writer_config.projection_columns)?;
            detailed_profile.projection_sec += projection_started.elapsed().as_secs_f64();
            let projected_input_array_bytes = batch.get_array_memory_size();
            detailed_profile.max_projected_input_array_bytes = detailed_profile
                .max_projected_input_array_bytes
                .max(projected_input_array_bytes);
            detailed_profile.sum_projected_input_array_bytes += projected_input_array_bytes;
            let batch_orders = group
                .active_orders
                .get(selected_row_cursor..selected_row_cursor + batch.num_rows())
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "selected coordinate/order mismatch for {} row_group={}",
                        group.source_file, group.row_group_id
                    ))
                })?;
            selected_row_cursor += batch.num_rows();
            let active_order_started = Instant::now();
            let batch = append_active_order(batch, batch_orders)?;
            detailed_profile.active_order_sec += active_order_started.elapsed().as_secs_f64();
            let restored = if config.enabled {
                let refs = restore_refs.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("missing restore references")
                })?;
                let dense_index_cache = dense_index_cache.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("missing dense index cache")
                })?;
                restore_batch_columns(
                    &batch,
                    &group.source_file,
                    &config,
                    &schema,
                    refs,
                    dense_index_cache,
                    build_output_schema(batch.schema().as_ref(), &schema)?,
                    Some(&mut detailed_profile),
                )?
            } else {
                cast_projected_batch_for_output(&batch, &schema, Some(&mut detailed_profile))?
            };
            let mut restored = restored;
            for (config, mapping) in &reference_replace_maps {
                let reference_replace_started = Instant::now();
                restored = apply_reference_replace(&restored, config, mapping, &group.source_file)?;
                detailed_profile.reference_replace_sec +=
                    reference_replace_started.elapsed().as_secs_f64();
            }
            for (lookup, right) in &lookup_batches {
                restored = enrich_many_to_one_lookup(
                    restored,
                    right,
                    &lookup.source_keys,
                    &lookup.lookup_keys,
                    &lookup.alias,
                )
                .map_err(pyo3::exceptions::PyValueError::new_err)?;
            }
            let pre_expression_array_bytes = restored.get_array_memory_size();
            detailed_profile.max_pre_expression_array_bytes = detailed_profile
                .max_pre_expression_array_bytes
                .max(pre_expression_array_bytes);
            detailed_profile.sum_pre_expression_array_bytes += pre_expression_array_bytes;
            if window_execution || pivot_execution || complete_post_execution {
                window_batches.push(restored);
                continue;
            }
            let expression_project_started = Instant::now();
            let input_rows = restored.num_rows();
            let restored = execute_and_project_writer_batch(
                restored,
                &writer_config,
                &group.source_file,
                long_fact_fingerprint_cursor,
            )?;
            long_fact_fingerprint_cursor += input_rows;
            detailed_profile.expression_project_sec +=
                expression_project_started.elapsed().as_secs_f64();
            let post_operation_started = Instant::now();
            let restored = execute_post_operations(restored, &writer_config.post_operations)
                .map_err(pyo3::exceptions::PyValueError::new_err)?;
            detailed_profile.post_operation_sec += post_operation_started.elapsed().as_secs_f64();
            let group_output_schema = match &output_schema {
                Some(existing) => existing.clone(),
                None => {
                    let built = build_output_schema(restored.schema().as_ref(), &schema)?;
                    output_schema = Some(built.clone());
                    built
                }
            };
            rows_written += restored.num_rows();
            let restored_batch_array_bytes = restored.get_array_memory_size();
            detailed_profile.max_restored_batch_array_bytes = detailed_profile
                .max_restored_batch_array_bytes
                .max(restored_batch_array_bytes);
            detailed_profile.sum_restored_batch_array_bytes += restored_batch_array_bytes;
            let writer_write_started = Instant::now();
            output_file_write_touches += write_partitioned_batch(
                &mut writers,
                &output_dir,
                &writer_config,
                group_output_schema.clone(),
                &restored,
                group.row_group_id,
            )?;
            detailed_profile.writer_write_sec += writer_write_started.elapsed().as_secs_f64();
        }
        if drop_cache_hint {
            if let Ok(file) = File::open(&group.source_file) {
                drop_file_cache_hint(&file);
                detailed_profile.cache_hint_calls += 1;
            }
        }
    }
    if (window_execution || pivot_execution || complete_post_execution)
        && !window_batches.is_empty()
    {
        let batch_schema = window_batches[0].schema();
        let concat_started = Instant::now();
        let combined = concat_batches(&batch_schema, &window_batches).map_err(|error| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to combine task batches for window execution: {error}"
            ))
        })?;
        detailed_profile.concat_batches_sec += concat_started.elapsed().as_secs_f64();
        let sort_started = Instant::now();
        let combined = sort_by_active_order(&combined)?;
        detailed_profile.active_order_sort_sec += sort_started.elapsed().as_secs_f64();
        let expression_project_started = Instant::now();
        let restored =
            execute_and_project_writer_batch(combined, &writer_config, "combined task", 0)?;
        detailed_profile.expression_project_sec +=
            expression_project_started.elapsed().as_secs_f64();
        let restored = match &writer_config.pivot {
            Some(config) if config.enabled => {
                let pivot_started = Instant::now();
                let result = pivot_record_batch(&restored, config).map_err(|error| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "failed to execute pivot for combined task: {error}"
                    ))
                })?;
                detailed_profile.pivot_sec += pivot_started.elapsed().as_secs_f64();
                pivot_duplicate_first_cells = result.duplicate_first_cells;
                pivot_column_values = result.enumerated_column_values;
                result.batch
            }
            _ => restored,
        };
        let post_operation_started = Instant::now();
        let restored = execute_post_operations(restored, &writer_config.post_operations)
            .map_err(pyo3::exceptions::PyValueError::new_err)?;
        detailed_profile.post_operation_sec += post_operation_started.elapsed().as_secs_f64();
        let group_output_schema = build_output_schema(restored.schema().as_ref(), &schema)?;
        rows_written += restored.num_rows();
        let restored_batch_array_bytes = restored.get_array_memory_size();
        detailed_profile.max_restored_batch_array_bytes = detailed_profile
            .max_restored_batch_array_bytes
            .max(restored_batch_array_bytes);
        detailed_profile.sum_restored_batch_array_bytes += restored_batch_array_bytes;
        let writer_write_started = Instant::now();
        output_file_write_touches += write_partitioned_batch(
            &mut writers,
            &output_dir,
            &writer_config,
            group_output_schema,
            &restored,
            0,
        )?;
        detailed_profile.writer_write_sec += writer_write_started.elapsed().as_secs_f64();
    }
    let restore_sec = restore_started.elapsed().as_secs_f64();

    let write_started = Instant::now();
    if writers.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "curated task produced no output writers",
        ));
    }
    let mut output_paths: Vec<String> = Vec::new();
    for (_path, managed) in writers {
        let ManagedParquetWriter {
            writer,
            final_path,
            temp_path,
        } = managed;
        writer.close().map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to finalize output parquet {}: {err}",
                temp_path.display()
            ))
        })?;
        if final_path.exists() {
            remove_file_with_retry(&final_path).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to replace existing output parquet {}: {err}",
                    final_path.display()
                ))
            })?;
        }
        rename_with_retry(&temp_path, &final_path).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to publish output parquet {} -> {}: {err}",
                temp_path.display(),
                final_path.display()
            ))
        })?;
        output_paths.push(final_path.to_string_lossy().to_string());
    }
    let parquet_write_sec = write_started.elapsed().as_secs_f64();
    if drop_cache_hint {
        for output_path in &output_paths {
            if let Ok(file) = File::open(output_path) {
                drop_file_cache_hint(&file);
                detailed_profile.cache_hint_calls += 1;
            }
        }
    }

    let total_sec = total_started.elapsed().as_secs_f64();
    let mut stats = HashMap::new();
    stats.insert("rows_written".to_string(), rows_written as f64);
    stats.insert("selected_row_count".to_string(), rows_written as f64);
    stats.insert(
        "source_file_count".to_string(),
        source_files_seen.len() as f64,
    );
    stats.insert("row_group_count".to_string(), row_group_count as f64);
    stats.insert(
        "source_bytes_read".to_string(),
        source_bytes_read.load(AtomicOrdering::Relaxed) as f64,
    );
    stats.insert("output_file_count".to_string(), output_paths.len() as f64);
    let output_partition_count = if writer_config.partition_columns.is_empty() {
        0usize
    } else {
        output_paths
            .iter()
            .filter_map(|path| {
                std::path::Path::new(path)
                    .parent()
                    .map(|parent| parent.to_path_buf())
            })
            .collect::<HashSet<_>>()
            .len()
    };
    stats.insert(
        "output_partition_count".to_string(),
        output_partition_count as f64,
    );
    stats.insert(
        "output_file_write_touches".to_string(),
        output_file_write_touches as f64,
    );
    stats.insert("coord_groups".to_string(), coord_groups.len() as f64);
    stats.insert("coord_read_sec".to_string(), coord_read_sec);
    stats.insert("reference_load_sec".to_string(), reference_load_sec);
    stats.insert("restore_sec".to_string(), restore_sec);
    stats.insert("parquet_write_sec".to_string(), parquet_write_sec);
    stats.insert("restore_elapsed_sec".to_string(), restore_sec);
    stats.insert("write_elapsed_sec".to_string(), parquet_write_sec);
    stats.insert("total_sec".to_string(), total_sec);
    stats.insert(
        "pivot_enabled".to_string(),
        if pivot_execution { 1.0 } else { 0.0 },
    );
    stats.insert(
        "pivot_duplicate_first_cells".to_string(),
        pivot_duplicate_first_cells as f64,
    );
    stats.insert(
        "pivot_column_values".to_string(),
        pivot_column_values as f64,
    );
    stats.insert(
        "ordered_operation_count".to_string(),
        writer_config.ordered_operations.len() as f64,
    );
    stats.insert(
        "post_operation_count".to_string(),
        writer_config.post_operations.len() as f64,
    );
    stats.insert(
        "batches_processed".to_string(),
        detailed_profile.batches_processed as f64,
    );
    stats.insert("input_rows".to_string(), detailed_profile.input_rows as f64);
    stats.insert(
        "max_batch_rows".to_string(),
        detailed_profile.max_batch_rows as f64,
    );
    stats.insert(
        "max_dense_len".to_string(),
        detailed_profile.max_dense_len as f64,
    );
    stats.insert(
        "max_restored_batch_array_bytes".to_string(),
        detailed_profile.max_restored_batch_array_bytes as f64,
    );
    stats.insert(
        "max_projected_input_array_bytes".to_string(),
        detailed_profile.max_projected_input_array_bytes as f64,
    );
    stats.insert(
        "projected_input_array_bytes".to_string(),
        detailed_profile.sum_projected_input_array_bytes as f64,
    );
    stats.insert(
        "avg_projected_input_array_bytes".to_string(),
        if detailed_profile.batches_processed == 0 {
            0.0
        } else {
            detailed_profile.sum_projected_input_array_bytes as f64
                / detailed_profile.batches_processed as f64
        },
    );
    stats.insert(
        "max_pre_expression_array_bytes".to_string(),
        detailed_profile.max_pre_expression_array_bytes as f64,
    );
    stats.insert(
        "avg_pre_expression_array_bytes".to_string(),
        if detailed_profile.batches_processed == 0 {
            0.0
        } else {
            detailed_profile.sum_pre_expression_array_bytes as f64
                / detailed_profile.batches_processed as f64
        },
    );
    stats.insert(
        "avg_restored_batch_array_bytes".to_string(),
        if detailed_profile.batches_processed == 0 {
            0.0
        } else {
            detailed_profile.sum_restored_batch_array_bytes as f64
                / detailed_profile.batches_processed as f64
        },
    );
    stats.insert(
        "value_column_count".to_string(),
        config.value_columns.len() as f64,
    );
    stats.insert(
        "native_list_fast_path_batch_columns".to_string(),
        detailed_profile.native_list_fast_path_batch_columns as f64,
    );
    stats.insert(
        "json_list_fallback_batch_columns".to_string(),
        detailed_profile.json_list_fallback_batch_columns as f64,
    );
    stats.insert(
        "source_extract_sec".to_string(),
        detailed_profile.source_extract_sec,
    );
    stats.insert(
        "dense_restore_sec".to_string(),
        detailed_profile.dense_restore_sec,
    );
    stats.insert(
        "record_batch_build_sec".to_string(),
        detailed_profile.record_batch_build_sec,
    );
    stats.insert(
        "projection_sec".to_string(),
        detailed_profile.projection_sec,
    );
    stats.insert(
        "active_order_sec".to_string(),
        detailed_profile.active_order_sec,
    );
    stats.insert(
        "reference_replace_sec".to_string(),
        detailed_profile.reference_replace_sec,
    );
    stats.insert(
        "expression_project_sec".to_string(),
        detailed_profile.expression_project_sec,
    );
    stats.insert(
        "post_operation_sec".to_string(),
        detailed_profile.post_operation_sec,
    );
    stats.insert(
        "concat_batches_sec".to_string(),
        detailed_profile.concat_batches_sec,
    );
    stats.insert(
        "active_order_sort_sec".to_string(),
        detailed_profile.active_order_sort_sec,
    );
    stats.insert("pivot_sec".to_string(), detailed_profile.pivot_sec);
    stats.insert(
        "writer_write_sec".to_string(),
        detailed_profile.writer_write_sec,
    );
    stats.insert(
        "cache_hint_calls".to_string(),
        detailed_profile.cache_hint_calls as f64,
    );
    let output_total_size_bytes: u64 = output_paths
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok().map(|metadata| metadata.len()))
        .sum();
    stats.insert(
        "output_file_size_bytes".to_string(),
        output_total_size_bytes as f64,
    );
    stats.insert(
        "output_total_size_bytes".to_string(),
        output_total_size_bytes as f64,
    );
    if print_timing {
        println!("[smoking_data_engine_rs] coord_read_sec={coord_read_sec:.6}");
        println!("[smoking_data_engine_rs] restore_sec={restore_sec:.6}");
        println!("[smoking_data_engine_rs] parquet_write_sec={parquet_write_sec:.6}");
        println!("[smoking_data_engine_rs] total_sec={total_sec:.6}");
    }
    Ok(stats)
}

fn validate_ordered_operations(writer_config: &WriterConfig) -> pyo3::PyResult<()> {
    if writer_config.ordered_operations.is_empty() {
        return Ok(());
    }
    let mut ids = HashSet::new();
    for operation in &writer_config.ordered_operations {
        if operation.operation_id.trim().is_empty() || operation.kind.trim().is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "ordered operation requires non-empty operation_id and kind",
            ));
        }
        if operation
            .execution_target
            .as_deref()
            .is_none_or(str::is_empty)
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "ordered operation requires execution_target",
            ));
        }
        if !ids.insert(operation.operation_id.as_str()) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "duplicate ordered operation id: {}",
                operation.operation_id
            )));
        }
    }
    if writer_config
        .ordered_operations
        .last()
        .is_none_or(|operation| operation.kind != "write_dataset")
    {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "ordered operation sequence must end with write_dataset",
        ));
    }
    let positions = writer_config
        .ordered_operations
        .iter()
        .enumerate()
        .map(|(index, operation)| (operation.operation_id.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut previous = None;
    for operation in &writer_config.post_operations {
        let position = positions
            .get(operation.operation_id.as_str())
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "post operation is missing from ordered operations: {}",
                    operation.operation_id
                ))
            })?;
        if previous.is_some_and(|previous| previous >= *position) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "post operation order differs from ordered operation contract",
            ));
        }
        previous = Some(*position);
    }
    Ok(())
}

fn writer_compression(value: Option<&str>) -> pyo3::PyResult<Compression> {
    match value.unwrap_or("zstd").to_ascii_lowercase().as_str() {
        "snappy" => Ok(Compression::SNAPPY),
        "zstd" => Ok(Compression::ZSTD(ZstdLevel::default())),
        "uncompressed" | "none" => Ok(Compression::UNCOMPRESSED),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unsupported parquet compression: {other}"
        ))),
    }
}

#[cfg(test)]
mod type_contract_tests {
    use super::*;

    #[test]
    fn parquet_writer_defaults_to_zstd_and_accepts_explicit_zstd() {
        let expected = Compression::ZSTD(ZstdLevel::default());
        assert_eq!(
            writer_compression(None).expect("default compression"),
            expected
        );
        assert_eq!(
            writer_compression(Some("zstd")).expect("zstd compression"),
            expected
        );
    }

    #[test]
    fn preserves_explicit_integer_widths_and_boolean() {
        let schema = HashMap::from([
            ("small".to_string(), "INT16".to_string()),
            ("regular".to_string(), "INT32".to_string()),
            ("large".to_string(), "INT64".to_string()),
            ("flag".to_string(), "BOOL".to_string()),
            ("decimal".to_string(), "DECIMAL(12, 2)".to_string()),
        ]);

        assert_eq!(
            output_dtype_for_column(&schema, "small", &DataType::Utf8).expect("INT16"),
            DataType::Int16
        );
        assert_eq!(
            output_dtype_for_column(&schema, "regular", &DataType::Utf8).expect("INT32"),
            DataType::Int32
        );
        assert_eq!(
            output_dtype_for_column(&schema, "large", &DataType::Utf8).expect("INT64"),
            DataType::Int64
        );
        assert_eq!(
            output_dtype_for_column(&schema, "flag", &DataType::Utf8).expect("BOOL"),
            DataType::Boolean
        );
        assert_eq!(
            output_dtype_for_column(&schema, "decimal", &DataType::Utf8).expect("DECIMAL"),
            DataType::Decimal128(12, 2)
        );
    }

    #[test]
    fn accepts_canonical_expression_dtypes_for_missing_wide_columns() {
        let cases = [
            ("int64", DataType::Int64),
            ("double", DataType::Float64),
            ("bool", DataType::Boolean),
            (
                "list<int64>",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            ),
        ];
        for (raw, expected) in cases {
            let schema = HashMap::from([("calculated".to_string(), raw.to_string())]);
            assert_eq!(
                output_dtype_for_column(&schema, "calculated", &DataType::Null).unwrap(),
                expected,
                "{raw}"
            );
        }
    }

    #[test]
    fn output_schema_preserves_contract_and_field_metadata() {
        let input = Schema::new_with_metadata(
            vec![
                Field::new("value", DataType::Int32, true).with_metadata(HashMap::from([(
                    "logical_name".to_string(),
                    "business.value".to_string(),
                )])),
            ],
            HashMap::from([(
                "smoking_data.contract".to_string(),
                "long_fact_v1".to_string(),
            )]),
        );

        let output = build_output_schema(&input, &HashMap::new()).unwrap();

        assert_eq!(output.metadata(), input.metadata());
        assert_eq!(output.field(0).metadata(), input.field(0).metadata());
    }
}
