use arrow_array::{
    builder::StringDictionaryBuilder, types::Int32Type, Array, Date32Array, Date64Array,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Deserialize)]
struct PlannerConfig {
    row_count: Option<usize>,
    #[serde(default)]
    row_keys: Vec<String>,
    mode: Option<String>,
    max_source_files_per_chunk: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct FilterConfig {
    #[serde(default)]
    source_filters: Vec<ThinFilter>,
    dedupe: Option<DedupeConfig>,
}

#[derive(Clone, Debug, Deserialize)]
struct ThinFilter {
    #[serde(default)]
    column: String,
    op: String,
    value: Option<String>,
    #[serde(default)]
    values: Vec<String>,
    #[serde(default)]
    filters: Vec<ThinFilter>,
}

#[derive(Clone, Debug, Deserialize)]
struct DedupeConfig {
    enabled: bool,
    #[serde(default)]
    group_keys: Vec<String>,
    #[serde(default)]
    sort: Vec<SortRule>,
}

#[derive(Clone, Debug, Deserialize)]
struct SortRule {
    column: String,
    direction: String,
}

#[derive(Clone, Debug)]
struct CandidateRow {
    source_file: Arc<str>,
    row_group_id: usize,
    row_index: i64,
    row_offset_in_group: usize,
    values: HashMap<String, ScalarValue>,
    ordinal: usize,
}

#[derive(Clone, Debug)]
enum ScalarValue {
    Null,
    String(String),
    Int(i64),
    Float(f64),
    Timestamp(i64),
}

struct CoordBuffers {
    source_file: Vec<Arc<str>>,
    row_group_id: Vec<i32>,
    row_index: Vec<i64>,
    row_offset_in_group: Vec<i32>,
    planner_chunk_id: Vec<i32>,
}

impl CoordBuffers {
    fn new() -> Self {
        Self {
            source_file: Vec::new(),
            row_group_id: Vec::new(),
            row_index: Vec::new(),
            row_offset_in_group: Vec::new(),
            planner_chunk_id: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.source_file.len()
    }

    fn is_empty(&self) -> bool {
        self.source_file.is_empty()
    }

    fn push(
        &mut self,
        source_file: &Arc<str>,
        row_group_id: usize,
        row_index: i64,
        row_offset_in_group: usize,
        planner_chunk_id: usize,
    ) -> pyo3::PyResult<()> {
        self.source_file.push(Arc::clone(source_file));
        self.row_group_id
            .push(i32::try_from(row_group_id).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "row_group_id conversion failed: {err}"
                ))
            })?);
        self.row_index.push(row_index);
        self.row_offset_in_group
            .push(i32::try_from(row_offset_in_group).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "row_offset_in_group conversion failed: {err}"
                ))
            })?);
        self.planner_chunk_id
            .push(i32::try_from(planner_chunk_id).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "planner_chunk_id conversion failed: {err}"
                ))
            })?);
        Ok(())
    }
}

fn coord_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "source_file",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("row_group_id", DataType::Int32, false),
        Field::new("row_index", DataType::Int64, false),
        Field::new("row_offset_in_group", DataType::Int32, false),
        Field::new("planner_chunk_id", DataType::Int32, false),
    ]))
}

fn write_coord_chunk(
    coord_output_dir: &str,
    chunk_id: usize,
    buffers: CoordBuffers,
) -> pyo3::PyResult<u64> {
    std::fs::create_dir_all(coord_output_dir).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to create coord output dir {coord_output_dir}: {err}"
        ))
    })?;
    let path = std::path::Path::new(coord_output_dir).join(format!("chunk-{chunk_id:06}.arrow"));
    let schema = coord_schema();
    let mut source_file = StringDictionaryBuilder::<Int32Type>::new();
    for value in buffers.source_file {
        source_file.append(value.as_ref()).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to dictionary-encode coord source_file: {err}"
            ))
        })?;
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(source_file.finish()),
            Arc::new(Int32Array::from(buffers.row_group_id)),
            Arc::new(Int64Array::from(buffers.row_index)),
            Arc::new(Int32Array::from(buffers.row_offset_in_group)),
            Arc::new(Int32Array::from(buffers.planner_chunk_id)),
        ],
    )
    .map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("failed to build coord batch: {err}"))
    })?;
    let file = File::create(&path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to create coord file {}: {err}",
            path.display()
        ))
    })?;
    let mut writer = FileWriter::try_new(file, &schema).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to initialize coord IPC writer {}: {err}",
            path.display()
        ))
    })?;
    writer.write(&batch).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to write coord batch {}: {err}",
            path.display()
        ))
    })?;
    writer.finish().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to finish coord file {}: {err}",
            path.display()
        ))
    })?;
    Ok(std::fs::metadata(&path)
        .map(|metadata| metadata.len())
        .unwrap_or(0))
}

fn required_filter_columns(filter_config: &FilterConfig) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();
    for filter in &filter_config.source_filters {
        collect_filter_columns(filter, &mut seen, &mut columns);
    }
    if let Some(dedupe) = &filter_config.dedupe {
        if dedupe.enabled {
            for column in &dedupe.group_keys {
                if seen.insert(column.clone()) {
                    columns.push(column.clone());
                }
            }
            for rule in &dedupe.sort {
                if seen.insert(rule.column.clone()) {
                    columns.push(rule.column.clone());
                }
            }
        }
    }
    columns
}

fn required_planner_columns(
    filter_config: &FilterConfig,
    planner_config: &PlannerConfig,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();
    for column in required_filter_columns(filter_config) {
        if seen.insert(column.clone()) {
            columns.push(column);
        }
    }
    for column in &planner_config.row_keys {
        if seen.insert(column.clone()) {
            columns.push(column.clone());
        }
    }
    columns
}

fn collect_filter_columns(
    filter: &ThinFilter,
    seen: &mut HashSet<String>,
    columns: &mut Vec<String>,
) {
    if !filter.column.is_empty() && seen.insert(filter.column.clone()) {
        columns.push(filter.column.clone());
    }
    for child in &filter.filters {
        collect_filter_columns(child, seen, columns);
    }
}

fn scalar_value(
    batch: &RecordBatch,
    column_name: &str,
    row_index: usize,
) -> pyo3::PyResult<ScalarValue> {
    let index = batch.schema().index_of(column_name).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "missing planner column {column_name}: {err}"
        ))
    })?;
    let column = batch.column(index);
    if column.is_null(row_index) {
        return Ok(ScalarValue::Null);
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(ScalarValue::String(values.value(row_index).to_string()));
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(ScalarValue::String(values.value(row_index).to_string()));
    }
    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok(ScalarValue::Int(values.value(row_index) as i64));
    }
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok(ScalarValue::Int(values.value(row_index)));
    }
    if let Some(values) = column.as_any().downcast_ref::<Date32Array>() {
        return Ok(ScalarValue::Int(values.value(row_index) as i64));
    }
    if let Some(values) = column.as_any().downcast_ref::<Date64Array>() {
        return Ok(ScalarValue::Timestamp(values.value(row_index)));
    }
    if let Some(values) = column.as_any().downcast_ref::<TimestampSecondArray>() {
        return Ok(ScalarValue::Timestamp(values.value(row_index)));
    }
    if let Some(values) = column.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(ScalarValue::Timestamp(values.value(row_index)));
    }
    if let Some(values) = column.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(ScalarValue::Timestamp(values.value(row_index)));
    }
    if let Some(values) = column.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(ScalarValue::Timestamp(values.value(row_index)));
    }
    if let Some(values) = column.as_any().downcast_ref::<Float32Array>() {
        return Ok(ScalarValue::Float(values.value(row_index) as f64));
    }
    if let Some(values) = column.as_any().downcast_ref::<Float64Array>() {
        return Ok(ScalarValue::Float(values.value(row_index)));
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "unsupported planner column type for {column_name}: {:?}",
        column.data_type()
    )))
}

fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::String(value) => Some(value.clone()),
        ScalarValue::Int(value) => Some(value.to_string()),
        ScalarValue::Float(value) => Some(value.to_string()),
        ScalarValue::Timestamp(value) => Some(value.to_string()),
    }
}

fn scalar_cmp(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    match (left, right) {
        (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
        (ScalarValue::Null, _) => Ordering::Less,
        (_, ScalarValue::Null) => Ordering::Greater,
        (ScalarValue::Int(a), ScalarValue::Int(b)) => a.cmp(b),
        (ScalarValue::Timestamp(a), ScalarValue::Timestamp(b)) => a.cmp(b),
        (ScalarValue::Int(a), ScalarValue::Timestamp(b)) => a.cmp(b),
        (ScalarValue::Timestamp(a), ScalarValue::Int(b)) => a.cmp(b),
        (ScalarValue::Float(a), ScalarValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (ScalarValue::Int(a), ScalarValue::Float(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (ScalarValue::Float(a), ScalarValue::Int(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        _ => scalar_to_string(left).cmp(&scalar_to_string(right)),
    }
}

fn filter_matches(row: &CandidateRow, filters: &[ThinFilter]) -> bool {
    filters
        .iter()
        .all(|filter| thin_filter_matches(row, filter))
}

fn thin_filter_matches(row: &CandidateRow, filter: &ThinFilter) -> bool {
    match filter.op.to_ascii_lowercase().as_str() {
        "and" => filter
            .filters
            .iter()
            .all(|child| thin_filter_matches(row, child)),
        "or" => filter
            .filters
            .iter()
            .any(|child| thin_filter_matches(row, child)),
        "is_not_null" => {
            let value = row.values.get(&filter.column).unwrap_or(&ScalarValue::Null);
            !matches!(value, ScalarValue::Null)
        }
        "is_null" => {
            let value = row.values.get(&filter.column).unwrap_or(&ScalarValue::Null);
            matches!(value, ScalarValue::Null)
        }
        "ne" | "!=" | "<>" => {
            let value = row.values.get(&filter.column).unwrap_or(&ScalarValue::Null);
            scalar_to_string(value) != filter.value
        }
        "eq" | "==" | "=" => {
            let value = row.values.get(&filter.column).unwrap_or(&ScalarValue::Null);
            scalar_to_string(value) == filter.value
        }
        "in" => {
            let value = row.values.get(&filter.column).unwrap_or(&ScalarValue::Null);
            let Some(value) = scalar_to_string(value) else {
                return false;
            };
            filter.values.iter().any(|candidate| candidate == &value)
        }
        "like" => {
            let value = row.values.get(&filter.column).unwrap_or(&ScalarValue::Null);
            let Some(value) = scalar_to_string(value) else {
                return false;
            };
            let Some(pattern) = &filter.value else {
                return false;
            };
            like_matches(&value, pattern)
        }
        _ => false,
    }
}

fn like_matches(value: &str, pattern: &str) -> bool {
    fn inner(value: &[char], pattern: &[char]) -> bool {
        if pattern.is_empty() {
            return value.is_empty();
        }
        match pattern[0] {
            '%' => {
                inner(value, &pattern[1..]) || (!value.is_empty() && inner(&value[1..], pattern))
            }
            '_' => !value.is_empty() && inner(&value[1..], &pattern[1..]),
            expected => {
                !value.is_empty() && value[0] == expected && inner(&value[1..], &pattern[1..])
            }
        }
    }
    inner(
        &value.chars().collect::<Vec<_>>(),
        &pattern.chars().collect::<Vec<_>>(),
    )
}

fn apply_dedupe(rows: Vec<CandidateRow>, dedupe: Option<&DedupeConfig>) -> Vec<CandidateRow> {
    let Some(dedupe) = dedupe else {
        return rows;
    };
    if !dedupe.enabled || dedupe.group_keys.is_empty() || dedupe.sort.is_empty() {
        return rows;
    }
    let mut ordered = rows;
    ordered.sort_by(|left, right| {
        for rule in &dedupe.sort {
            let ordering = scalar_cmp(
                left.values.get(&rule.column).unwrap_or(&ScalarValue::Null),
                right.values.get(&rule.column).unwrap_or(&ScalarValue::Null),
            );
            if ordering != Ordering::Equal {
                return if rule.direction.eq_ignore_ascii_case("desc") {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }
        left.ordinal.cmp(&right.ordinal)
    });

    let mut seen = HashSet::new();
    let mut kept = Vec::new();
    for row in ordered {
        let key = dedupe
            .group_keys
            .iter()
            .map(|column| {
                scalar_to_string(row.values.get(column).unwrap_or(&ScalarValue::Null))
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join("\u{1f}");
        if seen.insert(key) {
            kept.push(row);
        }
    }
    kept.sort_by_key(|row| row.ordinal);
    kept
}

fn row_key_signature(row: &CandidateRow, row_keys: &[String]) -> Vec<String> {
    row_keys
        .iter()
        .map(|column| {
            scalar_to_string(row.values.get(column).unwrap_or(&ScalarValue::Null))
                .unwrap_or_default()
        })
        .collect()
}

fn natural_sort_key(value: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut current_is_digit: Option<bool> = None;
    for ch in value.chars() {
        let is_digit = ch.is_ascii_digit();
        if current_is_digit == Some(is_digit) {
            current.push(ch);
            continue;
        }
        if !current.is_empty() {
            if current_is_digit == Some(true) {
                parts.push(format!("{:020}", current.parse::<u64>().unwrap_or(0)));
            } else {
                parts.push(current.clone());
            }
            current.clear();
        }
        current_is_digit = Some(is_digit);
        current.push(ch);
    }
    if !current.is_empty() {
        if current_is_digit == Some(true) {
            parts.push(format!("{:020}", current.parse::<u64>().unwrap_or(0)));
        } else {
            parts.push(current);
        }
    }
    parts
}

fn source_file_locality_signature(files: &HashSet<Arc<str>>) -> String {
    let mut ordered = files.iter().cloned().collect::<Vec<_>>();
    ordered.sort_by_key(|value| natural_sort_key(value));
    ordered.join("\u{1f}")
}

fn write_selected_rows_by_row_order(
    coord_output_dir: &str,
    rows: Vec<CandidateRow>,
    row_count: usize,
) -> pyo3::PyResult<(usize, u64, usize, usize, f64)> {
    let mut coord_chunk_count = 0usize;
    let mut coord_total_size_bytes = 0u64;
    let mut buffers = CoordBuffers::new();
    for row in rows {
        if buffers.len() >= row_count {
            coord_total_size_bytes +=
                write_coord_chunk(coord_output_dir, coord_chunk_count, buffers)?;
            coord_chunk_count += 1;
            buffers = CoordBuffers::new();
        }
        buffers.push(
            &row.source_file,
            row.row_group_id,
            row.row_index,
            row.row_offset_in_group,
            coord_chunk_count,
        )?;
    }
    if !buffers.is_empty() {
        coord_total_size_bytes += write_coord_chunk(coord_output_dir, coord_chunk_count, buffers)?;
        coord_chunk_count += 1;
    }
    Ok((coord_chunk_count, coord_total_size_bytes, 0, 0, 0.0))
}

fn write_unfiltered_rows_by_source_file_locality(
    coord_output_dir: &str,
    input_parquet_paths: &[String],
    row_count: usize,
    max_source_files_per_chunk: Option<usize>,
) -> pyo3::PyResult<(usize, u64, usize, usize, f64, usize, usize)> {
    let mut coord_chunk_count = 0usize;
    let mut coord_total_size_bytes = 0u64;
    let mut input_row_group_count = 0usize;
    let mut selected_row_count = 0usize;
    let mut current_files: HashSet<Arc<str>> = HashSet::new();
    let mut chunk_file_counts = Vec::new();
    let mut buffers = CoordBuffers::new();

    for source_file in input_parquet_paths {
        let source_file_ref: Arc<str> = Arc::from(source_file.as_str());
        let adds_file = !current_files.contains(&source_file_ref);
        let would_exceed_files = max_source_files_per_chunk
            .map(|limit| adds_file && !buffers.is_empty() && current_files.len() >= limit)
            .unwrap_or(false);
        if would_exceed_files {
            chunk_file_counts.push(current_files.len());
            coord_total_size_bytes +=
                write_coord_chunk(coord_output_dir, coord_chunk_count, buffers)?;
            coord_chunk_count += 1;
            buffers = CoordBuffers::new();
            current_files = HashSet::new();
        }
        current_files.insert(Arc::clone(&source_file_ref));

        let file = File::open(source_file).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to open input parquet {source_file}: {err}"
            ))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to initialize parquet reader {source_file}: {err}"
            ))
        })?;
        let row_group_count = builder.metadata().num_row_groups();
        input_row_group_count += row_group_count;
        let mut file_row_base = 0i64;
        for row_group_id in 0..row_group_count {
            let row_group_rows: usize = builder
                .metadata()
                .row_group(row_group_id)
                .num_rows()
                .try_into()
                .map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "invalid row count for row group {row_group_id} in {source_file}"
                    ))
                })?;
            for offset in 0..row_group_rows {
                if buffers.len() >= row_count {
                    chunk_file_counts.push(current_files.len());
                    coord_total_size_bytes +=
                        write_coord_chunk(coord_output_dir, coord_chunk_count, buffers)?;
                    coord_chunk_count += 1;
                    buffers = CoordBuffers::new();
                    current_files = HashSet::from([Arc::clone(&source_file_ref)]);
                }
                buffers.push(
                    &source_file_ref,
                    row_group_id,
                    file_row_base + offset as i64,
                    offset,
                    coord_chunk_count,
                )?;
                selected_row_count += 1;
            }
            file_row_base += row_group_rows as i64;
        }
    }
    if !buffers.is_empty() {
        chunk_file_counts.push(current_files.len());
        coord_total_size_bytes += write_coord_chunk(coord_output_dir, coord_chunk_count, buffers)?;
        coord_chunk_count += 1;
    }
    let min_files = chunk_file_counts.iter().copied().min().unwrap_or(0);
    let max_files = chunk_file_counts.iter().copied().max().unwrap_or(0);
    let avg_files = if chunk_file_counts.is_empty() {
        0.0
    } else {
        chunk_file_counts.iter().sum::<usize>() as f64 / chunk_file_counts.len() as f64
    };
    Ok((
        coord_chunk_count,
        coord_total_size_bytes,
        min_files,
        max_files,
        avg_files,
        input_row_group_count,
        selected_row_count,
    ))
}

fn write_selected_rows_by_source_file_locality(
    coord_output_dir: &str,
    rows: Vec<CandidateRow>,
    row_keys: &[String],
    row_count: usize,
    max_source_files_per_chunk: Option<usize>,
) -> pyo3::PyResult<(usize, u64, usize, usize, f64)> {
    let mut groups: HashMap<Vec<String>, (Vec<CandidateRow>, HashSet<Arc<str>>, usize)> =
        HashMap::new();
    for row in rows {
        let signature = row_key_signature(&row, row_keys);
        let ordinal = row.ordinal;
        let source_file = row.source_file.clone();
        let entry = groups
            .entry(signature)
            .or_insert_with(|| (Vec::new(), HashSet::new(), ordinal));
        entry.0.push(row);
        entry.1.insert(source_file);
        entry.2 = entry.2.min(ordinal);
    }

    let mut ordered = groups.into_iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        let left_files = source_file_locality_signature(&(left.1).1);
        let right_files = source_file_locality_signature(&(right.1).1);
        left_files
            .cmp(&right_files)
            .then((left.1).1.len().cmp(&(right.1).1.len()))
            .then(left.0.cmp(&right.0))
            .then((left.1).2.cmp(&(right.1).2))
    });

    let mut coord_chunk_count = 0usize;
    let mut coord_total_size_bytes = 0u64;
    let mut current_row_key_count = 0usize;
    let mut current_files: HashSet<Arc<str>> = HashSet::new();
    let mut buffers = CoordBuffers::new();
    let mut chunk_file_counts = Vec::new();

    for (_row_key, (mut group_rows, group_files, _ordinal)) in ordered {
        let next_files = current_files
            .union(&group_files)
            .cloned()
            .collect::<HashSet<_>>();
        let would_exceed_rows = current_row_key_count >= row_count;
        let would_exceed_files = max_source_files_per_chunk
            .map(|limit| current_row_key_count > 0 && next_files.len() > limit)
            .unwrap_or(false);
        if !buffers.is_empty() && (would_exceed_rows || would_exceed_files) {
            chunk_file_counts.push(current_files.len());
            coord_total_size_bytes +=
                write_coord_chunk(coord_output_dir, coord_chunk_count, buffers)?;
            coord_chunk_count += 1;
            buffers = CoordBuffers::new();
            current_row_key_count = 0;
            current_files = HashSet::new();
        }

        group_rows.sort_by_key(|row| row.ordinal);
        for row in group_rows {
            buffers.push(
                &row.source_file,
                row.row_group_id,
                row.row_index,
                row.row_offset_in_group,
                coord_chunk_count,
            )?;
        }
        current_row_key_count += 1;
        current_files.extend(group_files);
    }

    if !buffers.is_empty() {
        chunk_file_counts.push(current_files.len());
        coord_total_size_bytes += write_coord_chunk(coord_output_dir, coord_chunk_count, buffers)?;
        coord_chunk_count += 1;
    }

    let min_files = chunk_file_counts.iter().copied().min().unwrap_or(0);
    let max_files = chunk_file_counts.iter().copied().max().unwrap_or(0);
    let avg_files = if chunk_file_counts.is_empty() {
        0.0
    } else {
        chunk_file_counts.iter().sum::<usize>() as f64 / chunk_file_counts.len() as f64
    };
    Ok((
        coord_chunk_count,
        coord_total_size_bytes,
        min_files,
        max_files,
        avg_files,
    ))
}

fn collect_candidate_rows(
    source_file: &str,
    filter_config: &FilterConfig,
    required_columns: &[String],
) -> pyo3::PyResult<(Vec<CandidateRow>, usize)> {
    let source_file_ref: Arc<str> = Arc::from(source_file);
    let file = File::open(source_file).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to open input parquet {source_file}: {err}"
        ))
    })?;
    let base_builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to initialize parquet reader {source_file}: {err}"
        ))
    })?;
    let row_group_count = base_builder.metadata().num_row_groups();
    if required_columns.is_empty() {
        let mut rows = Vec::new();
        let mut file_row_base = 0i64;
        let mut ordinal = 0usize;
        for row_group_id in 0..row_group_count {
            let row_group_rows: usize = base_builder
                .metadata()
                .row_group(row_group_id)
                .num_rows()
                .try_into()
                .map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "invalid row count for row group {row_group_id} in {source_file}"
                    ))
                })?;
            for offset in 0..row_group_rows {
                rows.push(CandidateRow {
                    source_file: Arc::clone(&source_file_ref),
                    row_group_id,
                    row_index: file_row_base + offset as i64,
                    row_offset_in_group: offset,
                    values: HashMap::new(),
                    ordinal,
                });
                ordinal += 1;
            }
            file_row_base += row_group_rows as i64;
        }
        return Ok((rows, row_group_count));
    }

    let schema_descr = base_builder.metadata().file_metadata().schema_descr_ptr();
    let projection =
        ProjectionMask::columns(&schema_descr, required_columns.iter().map(String::as_str));
    let mut rows = Vec::new();
    let mut file_row_base = 0i64;
    let mut ordinal = 0usize;
    for row_group_id in 0..row_group_count {
        let row_group_rows: usize = base_builder
            .metadata()
            .row_group(row_group_id)
            .num_rows()
            .try_into()
            .map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid row count for row group {row_group_id} in {source_file}"
                ))
            })?;
        let file = File::open(source_file).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to reopen input parquet {source_file}: {err}"
            ))
        })?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to initialize projected parquet reader {source_file}: {err}"
                ))
            })?
            .with_projection(projection.clone())
            .with_row_groups(vec![row_group_id])
            .build()
            .map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to build projected parquet reader {source_file}: {err}"
                ))
            })?;
        let mut offset_base = 0usize;
        for batch_result in reader {
            let batch = batch_result.map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to read projected parquet batch {source_file}: {err}"
                ))
            })?;
            for batch_row in 0..batch.num_rows() {
                let offset = offset_base + batch_row;
                let mut values = HashMap::new();
                for column in required_columns {
                    values.insert(column.clone(), scalar_value(&batch, column, batch_row)?);
                }
                let row = CandidateRow {
                    source_file: Arc::clone(&source_file_ref),
                    row_group_id,
                    row_index: file_row_base + offset as i64,
                    row_offset_in_group: offset,
                    values,
                    ordinal,
                };
                if filter_matches(&row, &filter_config.source_filters) {
                    rows.push(row);
                }
                ordinal += 1;
            }
            offset_base += batch.num_rows();
        }
        file_row_base += row_group_rows as i64;
    }
    Ok((rows, row_group_count))
}

pub fn plan_coordinates_impl(
    input_parquet_paths: Vec<String>,
    coord_output_dir: String,
    filter_config_json: Option<String>,
    planner_config_json: Option<String>,
) -> pyo3::PyResult<HashMap<String, f64>> {
    let started = Instant::now();
    let filter_config = match filter_config_json {
        Some(raw) if !raw.trim().is_empty() => {
            serde_json::from_str::<FilterConfig>(&raw).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid filter config: {err}"))
            })?
        }
        _ => FilterConfig::default(),
    };
    let planner_config = match planner_config_json {
        Some(raw) if !raw.trim().is_empty() => serde_json::from_str::<PlannerConfig>(&raw)
            .map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid planner config: {err}"))
            })?,
        _ => PlannerConfig {
            row_count: None,
            row_keys: Vec::new(),
            mode: None,
            max_source_files_per_chunk: None,
        },
    };
    let row_count = planner_config.row_count.unwrap_or(10_000).max(1);

    let planner_mode = planner_config
        .mode
        .clone()
        .unwrap_or_else(|| "row_order".to_string())
        .to_ascii_lowercase();
    let unfiltered_streaming = planner_mode == "source_file_locality"
        && !planner_config.row_keys.is_empty()
        && filter_config.source_filters.is_empty()
        && filter_config.dedupe.is_none();
    let (
        coord_chunk_count,
        coord_total_size_bytes,
        chunk_source_file_count_min,
        chunk_source_file_count_max,
        chunk_source_file_count_avg,
        input_row_group_count,
        selected_row_count,
    ) = if unfiltered_streaming {
        write_unfiltered_rows_by_source_file_locality(
            &coord_output_dir,
            &input_parquet_paths,
            row_count,
            planner_config.max_source_files_per_chunk,
        )?
    } else {
        let required_columns = required_planner_columns(&filter_config, &planner_config);
        let mut selected_rows = Vec::new();
        let mut input_row_group_count = 0usize;
        for source_file in &input_parquet_paths {
            let (candidate_rows, row_group_count) =
                collect_candidate_rows(source_file, &filter_config, &required_columns)?;
            input_row_group_count += row_group_count;
            selected_rows.extend(apply_dedupe(candidate_rows, filter_config.dedupe.as_ref()));
        }
        let selected_row_count = selected_rows.len();
        let planned =
            if planner_mode == "source_file_locality" && !planner_config.row_keys.is_empty() {
                write_selected_rows_by_source_file_locality(
                    &coord_output_dir,
                    selected_rows,
                    &planner_config.row_keys,
                    row_count,
                    planner_config.max_source_files_per_chunk,
                )?
            } else {
                write_selected_rows_by_row_order(&coord_output_dir, selected_rows, row_count)?
            };
        (
            planned.0,
            planned.1,
            planned.2,
            planned.3,
            planned.4,
            input_row_group_count,
            selected_row_count,
        )
    };

    let mut stats = HashMap::new();
    stats.insert(
        "input_file_count".to_string(),
        input_parquet_paths.len() as f64,
    );
    stats.insert(
        "input_row_group_count".to_string(),
        input_row_group_count as f64,
    );
    stats.insert(
        "scanned_row_group_count".to_string(),
        input_row_group_count as f64,
    );
    stats.insert("skipped_row_group_count".to_string(), 0.0);
    stats.insert("selected_row_count".to_string(), selected_row_count as f64);
    stats.insert("coord_chunk_count".to_string(), coord_chunk_count as f64);
    stats.insert(
        "chunk_source_file_count_min".to_string(),
        chunk_source_file_count_min as f64,
    );
    stats.insert(
        "chunk_source_file_count_max".to_string(),
        chunk_source_file_count_max as f64,
    );
    stats.insert(
        "chunk_source_file_count_avg".to_string(),
        chunk_source_file_count_avg,
    );
    stats.insert(
        "coord_total_size_bytes".to_string(),
        coord_total_size_bytes as f64,
    );
    stats.insert(
        "planner_mode_source_file_locality".to_string(),
        if planner_mode == "source_file_locality" && !planner_config.row_keys.is_empty() {
            1.0
        } else {
            0.0
        },
    );
    stats.insert(
        "planner_mode_bounded_unfiltered_streaming".to_string(),
        if unfiltered_streaming { 1.0 } else { 0.0 },
    );
    stats.insert(
        "planner_elapsed_sec".to_string(),
        started.elapsed().as_secs_f64(),
    );
    Ok(stats)
}
