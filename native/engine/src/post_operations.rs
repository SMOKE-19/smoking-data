use arrow_array::{Array, ArrayRef, Float64Array, RecordBatch, StringArray, UInt32Array};
use arrow_cast::{cast, display::array_value_to_string};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::{interleave::interleave, take::take};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostOperation {
    pub operation_id: String,
    pub kind: String,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameConfig {
    #[serde(default)]
    resolved_mapping: HashMap<String, String>,
    #[serde(default)]
    mapping: HashMap<String, String>,
    #[serde(default)]
    regex: Vec<serde_json::Value>,
    case_sensitive: Option<bool>,
    unmatched: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnpivotConfig {
    id_columns: Vec<String>,
    value_columns: Vec<String>,
    name_column: String,
    value_column: String,
    #[serde(default = "default_unpivot_coercion")]
    coercion: String,
    #[serde(default = "default_true")]
    preserve_nulls: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertionConfig {
    rules: Vec<AssertionRule>,
    #[serde(default = "default_sample_limit")]
    sample_limit: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertionRule {
    id: String,
    kind: String,
    #[serde(default)]
    columns: Vec<String>,
    column: Option<String>,
    dtype: Option<String>,
    values: Option<Vec<serde_json::Value>>,
    min: Option<serde_json::Value>,
    max: Option<serde_json::Value>,
    #[serde(default = "default_true")]
    inclusive: bool,
    min_rows: Option<usize>,
    max_rows: Option<usize>,
}

#[derive(Default)]
struct AssertionOutcome {
    violation_count: usize,
    sample_row_indexes: Vec<usize>,
}

const UNIQUE_SPILL_BUCKETS: usize = 128;

fn default_true() -> bool {
    true
}

fn default_unpivot_coercion() -> String {
    "strict".to_string()
}

fn default_sample_limit() -> usize {
    20
}

pub fn requires_complete_input(operations: &[PostOperation]) -> bool {
    operations
        .iter()
        .any(|operation| operation.kind == "data_assertion")
}

pub fn execute_post_operations(
    mut batch: RecordBatch,
    operations: &[PostOperation],
) -> Result<RecordBatch, String> {
    for operation in operations {
        batch = match operation.kind.as_str() {
            "include_columns" => include_columns(batch, operation)?,
            "exclude_columns" => exclude_columns(batch, operation)?,
            "rename_columns" => rename_columns(batch, operation)?,
            "unpivot" => unpivot(batch, operation)?,
            "data_assertion" => {
                validate_assertions(&batch, operation)?;
                batch
            }
            other => {
                return Err(format!(
                    "unsupported post operation: id={:?}, kind={other:?}",
                    operation.operation_id
                ))
            }
        };
    }
    Ok(batch)
}

fn configured_columns(operation: &PostOperation) -> Result<Vec<String>, String> {
    operation
        .config
        .get("columns")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{} requires config.columns", operation.operation_id))?
        .iter()
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                format!(
                    "{} config.columns must contain strings",
                    operation.operation_id
                )
            })
        })
        .collect()
}

fn include_columns(batch: RecordBatch, operation: &PostOperation) -> Result<RecordBatch, String> {
    let names = configured_columns(operation)?;
    let mut fields = Vec::with_capacity(names.len());
    let mut arrays = Vec::with_capacity(names.len());
    for name in names {
        let index = batch
            .schema()
            .index_of(&name)
            .map_err(|_| format!("include column not found: {name}"))?;
        fields.push(Arc::new(batch.schema().field(index).clone()));
        arrays.push(batch.column(index).clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}

fn exclude_columns(batch: RecordBatch, operation: &PostOperation) -> Result<RecordBatch, String> {
    let excluded = configured_columns(operation)?
        .into_iter()
        .collect::<HashSet<_>>();
    let schema = batch.schema();
    let selected = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !excluded.contains(field.name()))
        .collect::<Vec<_>>();
    let fields = selected
        .iter()
        .map(|(_, field)| (*field).clone())
        .collect::<Vec<_>>();
    let arrays = selected
        .iter()
        .map(|(index, _)| batch.column(*index).clone())
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}

fn rename_columns(batch: RecordBatch, operation: &PostOperation) -> Result<RecordBatch, String> {
    let config: RenameConfig =
        serde_json::from_value(operation.config.clone()).map_err(|error| {
            format!(
                "invalid rename config for {}: {error}",
                operation.operation_id
            )
        })?;
    if !config.regex.is_empty() {
        return Err(format!(
            "rename regex must be resolved by the semantic compiler: operation={}",
            operation.operation_id
        ));
    }
    let _ = (config.case_sensitive, config.unmatched);
    let mapping = if config.resolved_mapping.is_empty() {
        config.mapping
    } else {
        config.resolved_mapping
    };
    let mut names = HashSet::new();
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let name = mapping.get(field.name()).unwrap_or(field.name());
            if !names.insert(name.clone()) {
                return Err(format!(
                    "rename target collision: operation={}, column={name:?}",
                    operation.operation_id
                ));
            }
            Ok(Arc::new(Field::new(
                name,
                field.data_type().clone(),
                field.is_nullable(),
            )))
        })
        .collect::<Result<Vec<_>, String>>()?;
    // RecordBatch arrays are reused; only schema fields are replaced.
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
        .map_err(|error| error.to_string())
}

fn unpivot(batch: RecordBatch, operation: &PostOperation) -> Result<RecordBatch, String> {
    let config: UnpivotConfig =
        serde_json::from_value(operation.config.clone()).map_err(|error| {
            format!(
                "invalid unpivot config for {}: {error}",
                operation.operation_id
            )
        })?;
    if config.value_columns.is_empty() {
        return Err("unpivot value_columns must not be empty".to_string());
    }
    let value_type = unpivot_value_type(&batch, &config)?;
    let value_arrays = config
        .value_columns
        .iter()
        .map(|name| {
            let value = batch
                .column_by_name(name)
                .ok_or_else(|| format!("unpivot value column not found: {name}"))?;
            cast(value.as_ref(), &value_type).map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut row_indexes = Vec::with_capacity(batch.num_rows() * value_arrays.len());
    let mut value_indexes = Vec::with_capacity(batch.num_rows() * value_arrays.len());
    let mut names = Vec::with_capacity(batch.num_rows() * value_arrays.len());
    for row in 0..batch.num_rows() {
        for (value_index, name) in config.value_columns.iter().enumerate() {
            if !config.preserve_nulls && value_arrays[value_index].is_null(row) {
                continue;
            }
            row_indexes.push(u32::try_from(row).map_err(|error| error.to_string())?);
            value_indexes.push((value_index, row));
            names.push(Some(name.as_str()));
        }
    }
    let take_indexes = UInt32Array::from(row_indexes);
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for name in &config.id_columns {
        let source = batch
            .column_by_name(name)
            .ok_or_else(|| format!("unpivot id column not found: {name}"))?;
        fields.push(Arc::new(Field::new(
            name,
            source.data_type().clone(),
            source.null_count() > 0,
        )));
        arrays.push(take(source.as_ref(), &take_indexes, None).map_err(|error| error.to_string())?);
    }
    fields.push(Arc::new(Field::new(
        &config.name_column,
        DataType::Utf8,
        false,
    )));
    arrays.push(Arc::new(StringArray::from(names)) as ArrayRef);
    let refs = value_arrays
        .iter()
        .map(|array| array.as_ref())
        .collect::<Vec<_>>();
    let values = interleave(&refs, &value_indexes).map_err(|error| error.to_string())?;
    fields.push(Arc::new(Field::new(
        &config.value_column,
        value_type,
        values.null_count() > 0,
    )));
    arrays.push(values);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}

fn unpivot_value_type(batch: &RecordBatch, config: &UnpivotConfig) -> Result<DataType, String> {
    let types = config
        .value_columns
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .map(|array| array.data_type().clone())
                .ok_or_else(|| format!("unpivot value column not found: {name}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if types.iter().all(|value| value == &types[0]) {
        return Ok(types[0].clone());
    }
    if types.iter().all(is_numeric) {
        return Ok(DataType::Float64);
    }
    if config.coercion == "string" {
        return Ok(DataType::Utf8);
    }
    Err(format!(
        "unpivot mixed value dtypes require coercion=string or numeric-compatible inputs: {types:?}"
    ))
}

fn is_numeric(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

fn validate_assertions(batch: &RecordBatch, operation: &PostOperation) -> Result<(), String> {
    let config: AssertionConfig =
        serde_json::from_value(operation.config.clone()).map_err(|error| {
            format!(
                "invalid assertion config for {}: {error}",
                operation.operation_id
            )
        })?;
    for rule in &config.rules {
        let violations = assertion_violations(batch, rule)?;
        if !violations.is_empty() {
            let samples = violations
                .iter()
                .take(config.sample_limit)
                .copied()
                .collect::<Vec<_>>();
            let detail = serde_json::json!({
                "operation_id": operation.operation_id,
                "rule_id": rule.id,
                "kind": rule.kind,
                "violation_count": violations.len(),
                "sample_row_indexes": samples,
            });
            return Err(format!("DATA_ASSERTION_FAILED {detail}"));
        }
    }
    Ok(())
}

fn assertion_violations(batch: &RecordBatch, rule: &AssertionRule) -> Result<Vec<usize>, String> {
    let columns = if rule.columns.is_empty() {
        rule.column.iter().cloned().collect::<Vec<_>>()
    } else {
        rule.columns.clone()
    };
    match rule.kind.as_str() {
        "required_columns" => {
            let missing = columns
                .iter()
                .filter(|name| batch.column_by_name(name).is_none())
                .cloned()
                .collect::<Vec<_>>();
            if missing.is_empty() {
                Ok(Vec::new())
            } else {
                Err(format!("required columns missing: {missing:?}"))
            }
        }
        "dtype" => {
            let expected = rule
                .dtype
                .as_deref()
                .ok_or("dtype assertion requires dtype")?;
            let actual = columns
                .iter()
                .map(|name| {
                    batch
                        .column_by_name(name)
                        .map(|array| format!("{}", array.data_type()))
                        .ok_or_else(|| format!("assertion column not found: {name}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if actual
                .iter()
                .all(|value| value.eq_ignore_ascii_case(expected))
            {
                Ok(Vec::new())
            } else {
                Err(format!(
                    "dtype assertion expected={expected:?}, actual={actual:?}"
                ))
            }
        }
        "not_null" => {
            let arrays = assertion_arrays(batch, &columns)?;
            Ok((0..batch.num_rows())
                .filter(|row| arrays.iter().any(|array| array.is_null(*row)))
                .collect())
        }
        "unique" => {
            let arrays = assertion_arrays(batch, &columns)?;
            let mut seen = HashSet::new();
            let mut violations = Vec::new();
            for row in 0..batch.num_rows() {
                let key = arrays
                    .iter()
                    .map(|array| {
                        array_value_to_string(array.as_ref(), row)
                            .unwrap_or_else(|_| "<error>".into())
                    })
                    .collect::<Vec<_>>()
                    .join("\u{1f}");
                if !seen.insert(key) {
                    violations.push(row);
                }
            }
            Ok(violations)
        }
        "accepted_values" => {
            let array = one_assertion_array(batch, &columns)?;
            let accepted = rule
                .values
                .as_ref()
                .ok_or("accepted_values assertion requires values")?
                .iter()
                .map(json_scalar)
                .collect::<HashSet<_>>();
            Ok((0..batch.num_rows())
                .filter(|row| {
                    !array.is_null(*row)
                        && !accepted.contains(
                            &array_value_to_string(array.as_ref(), *row).unwrap_or_default(),
                        )
                })
                .collect())
        }
        "range" => {
            let array = one_assertion_array(batch, &columns)?;
            if rule
                .min
                .as_ref()
                .or(rule.max.as_ref())
                .is_some_and(serde_json::Value::is_number)
            {
                let values =
                    cast(array.as_ref(), &DataType::Float64).map_err(|error| error.to_string())?;
                let values = values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or("range cast did not produce Float64")?;
                let minimum = rule.min.as_ref().and_then(serde_json::Value::as_f64);
                let maximum = rule.max.as_ref().and_then(serde_json::Value::as_f64);
                Ok((0..batch.num_rows())
                    .filter(|row| {
                        if values.is_null(*row) {
                            return false;
                        }
                        outside_numeric_range(values.value(*row), minimum, maximum, rule.inclusive)
                    })
                    .collect())
            } else {
                let minimum = rule.min.as_ref().map(json_scalar);
                let maximum = rule.max.as_ref().map(json_scalar);
                Ok((0..batch.num_rows())
                    .filter(|row| {
                        if array.is_null(*row) {
                            return false;
                        }
                        let value = array_value_to_string(array.as_ref(), *row).unwrap_or_default();
                        outside_lexical_range(
                            &value,
                            minimum.as_deref(),
                            maximum.as_deref(),
                            rule.inclusive,
                        )
                    })
                    .collect())
            }
        }
        "row_count" => {
            let invalid = rule
                .min_rows
                .is_some_and(|minimum| batch.num_rows() < minimum)
                || rule
                    .max_rows
                    .is_some_and(|maximum| batch.num_rows() > maximum);
            Ok(if invalid {
                (0..batch.num_rows().max(1)).collect()
            } else {
                Vec::new()
            })
        }
        other => Err(format!("unsupported assertion kind: {other}")),
    }
}

fn assertion_arrays<'a>(
    batch: &'a RecordBatch,
    columns: &[String],
) -> Result<Vec<&'a ArrayRef>, String> {
    columns
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .ok_or_else(|| format!("assertion column not found: {name}"))
        })
        .collect()
}

fn one_assertion_array<'a>(
    batch: &'a RecordBatch,
    columns: &[String],
) -> Result<&'a ArrayRef, String> {
    if columns.len() != 1 {
        return Err("assertion requires exactly one column".to_string());
    }
    batch
        .column_by_name(&columns[0])
        .ok_or_else(|| format!("assertion column not found: {}", columns[0]))
}

fn json_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn outside_numeric_range(
    value: f64,
    minimum: Option<f64>,
    maximum: Option<f64>,
    inclusive: bool,
) -> bool {
    minimum.is_some_and(|bound| {
        if inclusive {
            value < bound
        } else {
            value <= bound
        }
    }) || maximum.is_some_and(|bound| {
        if inclusive {
            value > bound
        } else {
            value >= bound
        }
    })
}

fn outside_lexical_range(
    value: &str,
    minimum: Option<&str>,
    maximum: Option<&str>,
    inclusive: bool,
) -> bool {
    minimum.is_some_and(|bound| {
        if inclusive {
            value < bound
        } else {
            value <= bound
        }
    }) || maximum.is_some_and(|bound| {
        if inclusive {
            value > bound
        } else {
            value >= bound
        }
    })
}

pub fn validate_dataset(
    parquet_paths: &[String],
    config_json: &str,
    spill_dir: &str,
) -> Result<String, String> {
    let config: AssertionConfig = serde_json::from_str(config_json)
        .map_err(|error| format!("invalid dataset assertion config: {error}"))?;
    if parquet_paths.is_empty() {
        return Err("dataset assertion requires at least one parquet file".to_string());
    }
    let spill_root = PathBuf::from(spill_dir);
    if spill_root.exists() {
        fs::remove_dir_all(&spill_root).map_err(|error| error.to_string())?;
    }
    fs::create_dir_all(&spill_root).map_err(|error| error.to_string())?;
    let mut outcomes = config
        .rules
        .iter()
        .map(|rule| (rule.id.clone(), AssertionOutcome::default()))
        .collect::<HashMap<_, _>>();
    let unique_rules = config
        .rules
        .iter()
        .filter(|rule| rule.kind == "unique")
        .collect::<Vec<_>>();
    let mut spill_writers: HashMap<(String, usize), BufWriter<File>> = HashMap::new();
    let mut global_row_index = 0usize;

    let validation = (|| -> Result<(), String> {
        for parquet_path in parquet_paths {
            let file = File::open(parquet_path).map_err(|error| {
                format!("failed to open assertion parquet {parquet_path}: {error}")
            })?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
                format!("failed to inspect assertion parquet {parquet_path}: {error}")
            })?;
            let schema_batch = RecordBatch::new_empty(builder.schema().clone());
            for rule in &config.rules {
                if matches!(rule.kind.as_str(), "required_columns" | "dtype") {
                    if let Err(error) = assertion_violations(&schema_batch, rule) {
                        return Err(structured_assertion_error(
                            "dataset_assertion",
                            rule,
                            1,
                            &[global_row_index],
                            Some(&error),
                        ));
                    }
                }
            }
            let reader = builder.with_batch_size(65_536).build().map_err(|error| {
                format!("failed to read assertion parquet {parquet_path}: {error}")
            })?;
            for batch_result in reader {
                let batch = batch_result.map_err(|error| {
                    format!("failed to read assertion batch {parquet_path}: {error}")
                })?;
                for rule in &config.rules {
                    match rule.kind.as_str() {
                        "unique" | "row_count" => {}
                        "required_columns" | "dtype" => {}
                        _ => {
                            let violations = assertion_violations(&batch, rule)?;
                            let outcome = outcomes.get_mut(&rule.id).expect("assertion outcome");
                            outcome.violation_count += violations.len();
                            for row in violations {
                                if outcome.sample_row_indexes.len() < config.sample_limit {
                                    outcome.sample_row_indexes.push(global_row_index + row);
                                }
                            }
                        }
                    }
                }
                for rule in &unique_rules {
                    let columns = assertion_rule_columns(rule);
                    let arrays = assertion_arrays(&batch, &columns)?;
                    for row in 0..batch.num_rows() {
                        let key = encode_unique_key(&arrays, row)?;
                        let mut hasher = DefaultHasher::new();
                        key.hash(&mut hasher);
                        let bucket = (hasher.finish() as usize) % UNIQUE_SPILL_BUCKETS;
                        let writer =
                            spill_writer(&spill_root, &rule.id, bucket, &mut spill_writers)?;
                        writer
                            .write_all(&(global_row_index + row).to_le_bytes())
                            .and_then(|_| writer.write_all(&(key.len() as u32).to_le_bytes()))
                            .and_then(|_| writer.write_all(&key))
                            .map_err(|error| error.to_string())?;
                    }
                }
                global_row_index += batch.num_rows();
            }
        }
        drop(spill_writers);

        for rule in &unique_rules {
            let outcome = outcomes.get_mut(&rule.id).expect("assertion outcome");
            for bucket in 0..UNIQUE_SPILL_BUCKETS {
                let path = unique_bucket_path(&spill_root, &rule.id, bucket);
                if !path.is_file() {
                    continue;
                }
                let mut reader =
                    BufReader::new(File::open(&path).map_err(|error| error.to_string())?);
                let mut seen = HashSet::new();
                while let Some((row_index, key)) = read_spilled_key(&mut reader)? {
                    if !seen.insert(key) {
                        outcome.violation_count += 1;
                        if outcome.sample_row_indexes.len() < config.sample_limit {
                            outcome.sample_row_indexes.push(row_index);
                        }
                    }
                }
            }
        }
        for rule in &config.rules {
            if rule.kind == "row_count" {
                let invalid = rule
                    .min_rows
                    .is_some_and(|minimum| global_row_index < minimum)
                    || rule
                        .max_rows
                        .is_some_and(|maximum| global_row_index > maximum);
                if invalid {
                    let outcome = outcomes.get_mut(&rule.id).expect("assertion outcome");
                    outcome.violation_count = 1;
                    outcome.sample_row_indexes.push(0);
                }
            }
        }
        for rule in &config.rules {
            let outcome = outcomes.get(&rule.id).expect("assertion outcome");
            if outcome.violation_count > 0 {
                return Err(structured_assertion_error(
                    "dataset_assertion",
                    rule,
                    outcome.violation_count,
                    &outcome.sample_row_indexes,
                    None,
                ));
            }
        }
        Ok(())
    })();
    let _ = fs::remove_dir_all(&spill_root);
    validation?;
    Ok(serde_json::json!({
        "ok": true,
        "rows_validated": global_row_index,
        "files_validated": parquet_paths.len(),
        "rules_validated": config.rules.len(),
        "unique_spill_buckets": UNIQUE_SPILL_BUCKETS,
    })
    .to_string())
}

fn assertion_rule_columns(rule: &AssertionRule) -> Vec<String> {
    if rule.columns.is_empty() {
        rule.column.iter().cloned().collect()
    } else {
        rule.columns.clone()
    }
}

fn encode_unique_key(arrays: &[&ArrayRef], row: usize) -> Result<Vec<u8>, String> {
    let mut key = Vec::new();
    for array in arrays {
        if array.is_null(row) {
            key.extend_from_slice(&u32::MAX.to_le_bytes());
            continue;
        }
        let value =
            array_value_to_string(array.as_ref(), row).map_err(|error| error.to_string())?;
        key.extend_from_slice(&(value.len() as u32).to_le_bytes());
        key.extend_from_slice(value.as_bytes());
    }
    Ok(key)
}

fn spill_writer<'a>(
    root: &Path,
    rule_id: &str,
    bucket: usize,
    writers: &'a mut HashMap<(String, usize), BufWriter<File>>,
) -> Result<&'a mut BufWriter<File>, String> {
    let key = (rule_id.to_string(), bucket);
    if !writers.contains_key(&key) {
        let path = unique_bucket_path(root, rule_id, bucket);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        writers.insert(key.clone(), BufWriter::new(file));
    }
    Ok(writers.get_mut(&key).expect("spill writer"))
}

fn unique_bucket_path(root: &Path, rule_id: &str, bucket: usize) -> PathBuf {
    let safe_rule = rule_id
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric() || value == '-' || value == '_' {
                value
            } else {
                '_'
            }
        })
        .collect::<String>();
    root.join(format!("{safe_rule}-{bucket:03}.bin"))
}

fn read_spilled_key(reader: &mut BufReader<File>) -> Result<Option<(usize, Vec<u8>)>, String> {
    let mut row_bytes = [0u8; std::mem::size_of::<usize>()];
    match reader.read_exact(&mut row_bytes) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.to_string()),
    }
    let mut length_bytes = [0u8; 4];
    reader
        .read_exact(&mut length_bytes)
        .map_err(|error| error.to_string())?;
    let length = u32::from_le_bytes(length_bytes) as usize;
    let mut key = vec![0u8; length];
    reader
        .read_exact(&mut key)
        .map_err(|error| error.to_string())?;
    Ok(Some((usize::from_le_bytes(row_bytes), key)))
}

fn structured_assertion_error(
    operation_id: &str,
    rule: &AssertionRule,
    violation_count: usize,
    sample_row_indexes: &[usize],
    message: Option<&str>,
) -> String {
    let detail = serde_json::json!({
        "operation_id": operation_id,
        "rule_id": rule.id,
        "kind": rule.kind,
        "violation_count": violation_count,
        "sample_row_indexes": sample_row_indexes,
        "message": message,
    });
    format!("DATA_ASSERTION_FAILED {detail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            ("a", Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![30, 40])) as ArrayRef),
            (
                "label",
                Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
            ),
        ])
        .expect("batch")
    }

    #[test]
    fn rename_reuses_arrays_and_unpivot_is_row_major() {
        let input = batch();
        let original_label = input.column(3).clone();
        let renamed = execute_post_operations(
            input,
            &[PostOperation {
                operation_id: "rename".into(),
                kind: "rename_columns".into(),
                config: serde_json::json!({"resolved_mapping":{"label":"name"}}),
            }],
        )
        .expect("rename");
        assert!(Arc::ptr_eq(renamed.column(3), &original_label));
        let unpivoted = execute_post_operations(
            renamed,
            &[PostOperation {
                operation_id: "long".into(),
                kind: "unpivot".into(),
                config: serde_json::json!({
                    "id_columns":["id"], "value_columns":["a","b"],
                    "name_column":"metric", "value_column":"value"
                }),
            }],
        )
        .expect("unpivot");
        assert_eq!(unpivoted.num_rows(), 4);
        let names = unpivoted
            .column_by_name("metric")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (0..4).map(|index| names.value(index)).collect::<Vec<_>>(),
            vec!["a", "b", "a", "b"]
        );
    }

    #[test]
    fn assertion_reports_rule_and_violations() {
        let error = execute_post_operations(
            batch(),
            &[PostOperation {
                operation_id: "validate".into(),
                kind: "data_assertion".into(),
                config: serde_json::json!({"rules":[{"id":"unique_label","kind":"unique","columns":["label"]}]}),
            }],
        );
        assert!(error.is_ok());
    }
}
