use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use arrow_cast::{cast, display::array_value_to_string};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize)]
pub struct PivotConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    row_keys: Vec<String>,
    #[serde(default)]
    column_keys: Vec<String>,
    #[serde(default)]
    value_keys: Vec<PivotValueKey>,
    #[serde(default)]
    value_keys_without_column: Vec<PivotValueKey>,
    #[serde(default = "default_null_column_key_policy")]
    null_column_key_policy: String,
    #[serde(default = "default_column_key_separator")]
    column_key_separator: String,
    #[serde(default = "default_first_duplicate_policy")]
    first_duplicate_policy: String,
}

#[derive(Clone, Debug, Deserialize)]
struct PivotValueKey {
    name: Option<String>,
    source_column: String,
    aggregation: String,
    output_dtype: Option<String>,
    column_name_rule: Option<String>,
}

#[derive(Debug)]
pub struct PivotResult {
    pub batch: RecordBatch,
    pub duplicate_first_cells: usize,
    pub enumerated_column_values: usize,
}

fn default_null_column_key_policy() -> String {
    "error".to_string()
}

fn default_column_key_separator() -> String {
    "__".to_string()
}

fn default_first_duplicate_policy() -> String {
    "warn".to_string()
}

pub fn pivot_record_batch(
    batch: &RecordBatch,
    config: &PivotConfig,
) -> Result<PivotResult, String> {
    validate_config(batch, config)?;
    if !config.enabled {
        return Ok(PivotResult {
            batch: batch.clone(),
            duplicate_first_cells: 0,
            enumerated_column_values: 0,
        });
    }

    let row_arrays = columns(batch, &config.row_keys)?;
    let column_arrays = columns(batch, &config.column_keys)?;
    let mut row_group_index: HashMap<String, usize> = HashMap::new();
    let mut row_first_indices: Vec<usize> = Vec::new();
    let mut column_value_index: HashMap<String, usize> = HashMap::new();
    let mut column_values: Vec<String> = Vec::new();
    let mut cells: HashMap<(usize, usize, usize), Vec<usize>> = HashMap::new();
    let mut no_column_cells: HashMap<(usize, usize), Vec<usize>> = HashMap::new();

    for row_index in 0..batch.num_rows() {
        let row_key = composite_key(&row_arrays, row_index, false, config)?;
        let next_group = row_group_index.len();
        let group_index = *row_group_index.entry(row_key).or_insert_with(|| {
            row_first_indices.push(row_index);
            next_group
        });

        if !config.value_keys.is_empty() {
            let column_value = composite_key(&column_arrays, row_index, true, config)?;
            let next_column = column_value_index.len();
            let column_index = *column_value_index
                .entry(column_value.clone())
                .or_insert_with(|| {
                    column_values.push(column_value);
                    next_column
                });
            for value_index in 0..config.value_keys.len() {
                cells
                    .entry((group_index, column_index, value_index))
                    .or_default()
                    .push(row_index);
            }
        }
        for value_index in 0..config.value_keys_without_column.len() {
            no_column_cells
                .entry((group_index, value_index))
                .or_default()
                .push(row_index);
        }
    }

    let row_take = UInt32Array::from(
        row_first_indices
            .iter()
            .map(|index| u32::try_from(*index).map_err(|error| error.to_string()))
            .collect::<Result<Vec<_>, _>>()?,
    );
    let mut output_fields: Vec<Arc<Field>> = Vec::new();
    let mut output_arrays: Vec<ArrayRef> = Vec::new();
    let mut output_names: HashSet<String> = HashSet::new();
    for row_key in &config.row_keys {
        let source = batch
            .column_by_name(row_key)
            .ok_or_else(|| format!("pivot row key column not found: {row_key}"))?;
        output_fields.push(Arc::new(Field::new(
            row_key,
            source.data_type().clone(),
            source.null_count() > 0,
        )));
        output_arrays
            .push(take(source.as_ref(), &row_take, None).map_err(|error| error.to_string())?);
        output_names.insert(row_key.clone());
    }

    let mut duplicate_first_cells = 0usize;
    for (value_index, value_spec) in config.value_keys.iter().enumerate() {
        let source = value_source(batch, value_spec)?;
        for (column_index, column_value) in column_values.iter().enumerate() {
            let output_name = render_column_name(value_spec, Some(column_value))?;
            ensure_unique_name(&mut output_names, &output_name)?;
            let groups = (0..row_first_indices.len())
                .map(|group_index| {
                    cells
                        .get(&(group_index, column_index, value_index))
                        .map(Vec::as_slice)
                        .unwrap_or(&[])
                })
                .collect::<Vec<_>>();
            let (array, duplicates) = aggregate_column(source, &groups, value_spec)?;
            duplicate_first_cells += duplicates;
            output_fields.push(Arc::new(Field::new(
                &output_name,
                array.data_type().clone(),
                true,
            )));
            output_arrays.push(array);
        }
    }

    for (value_index, value_spec) in config.value_keys_without_column.iter().enumerate() {
        let source = value_source(batch, value_spec)?;
        let output_name = render_column_name(value_spec, None)?;
        ensure_unique_name(&mut output_names, &output_name)?;
        let groups = (0..row_first_indices.len())
            .map(|group_index| {
                no_column_cells
                    .get(&(group_index, value_index))
                    .map(Vec::as_slice)
                    .unwrap_or(&[])
            })
            .collect::<Vec<_>>();
        let (array, duplicates) = aggregate_column(source, &groups, value_spec)?;
        duplicate_first_cells += duplicates;
        output_fields.push(Arc::new(Field::new(
            &output_name,
            array.data_type().clone(),
            true,
        )));
        output_arrays.push(array);
    }

    if duplicate_first_cells > 0 && config.first_duplicate_policy == "error" {
        return Err(format!(
            "pivot first aggregation found {duplicate_first_cells} duplicate cell(s)"
        ));
    }
    let output = RecordBatch::try_new(Arc::new(Schema::new(output_fields)), output_arrays)
        .map_err(|error| error.to_string())?;
    Ok(PivotResult {
        batch: output,
        duplicate_first_cells,
        enumerated_column_values: column_values.len(),
    })
}

fn validate_config(batch: &RecordBatch, config: &PivotConfig) -> Result<(), String> {
    if !config.enabled {
        return Ok(());
    }
    if config.row_keys.is_empty() {
        return Err("pivot.row_keys must not be empty".to_string());
    }
    if config.value_keys.is_empty() && config.value_keys_without_column.is_empty() {
        return Err("pivot requires value_keys or value_keys_without_column".to_string());
    }
    if !config.value_keys.is_empty() && config.column_keys.is_empty() {
        return Err(
            "pivot.column_keys must not be empty when value_keys are configured".to_string(),
        );
    }
    if !matches!(config.null_column_key_policy.as_str(), "error" | "label") {
        return Err("pivot.null_column_key_policy must be error or label".to_string());
    }
    if !matches!(
        config.first_duplicate_policy.as_str(),
        "warn" | "error" | "allow"
    ) {
        return Err("pivot.first_duplicate_policy must be warn, error or allow".to_string());
    }
    for name in config.row_keys.iter().chain(config.column_keys.iter()) {
        if batch.column_by_name(name).is_none() {
            return Err(format!("pivot key column not found: {name}"));
        }
    }
    for value in config
        .value_keys
        .iter()
        .chain(config.value_keys_without_column.iter())
    {
        if value.source_column.trim().is_empty()
            || batch.column_by_name(&value.source_column).is_none()
        {
            return Err(format!(
                "pivot value source column not found: {}",
                value.source_column
            ));
        }
        if !matches!(
            value.aggregation.to_lowercase().as_str(),
            "first" | "count" | "sum" | "avg" | "mean" | "min" | "max" | "unique_concatenate"
        ) {
            return Err(format!(
                "unsupported pivot aggregation: {}",
                value.aggregation
            ));
        }
    }
    Ok(())
}

fn columns(batch: &RecordBatch, names: &[String]) -> Result<Vec<ArrayRef>, String> {
    names
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .cloned()
                .ok_or_else(|| format!("pivot column not found: {name}"))
        })
        .collect()
}

fn value_source<'a>(batch: &'a RecordBatch, value: &PivotValueKey) -> Result<&'a ArrayRef, String> {
    batch.column_by_name(&value.source_column).ok_or_else(|| {
        format!(
            "pivot value source column not found: {}",
            value.source_column
        )
    })
}

fn composite_key(
    arrays: &[ArrayRef],
    row_index: usize,
    render: bool,
    config: &PivotConfig,
) -> Result<String, String> {
    let mut values = Vec::with_capacity(arrays.len());
    for array in arrays {
        if array.is_null(row_index) {
            if render && config.null_column_key_policy == "error" {
                return Err("pivot column key contains null".to_string());
            }
            values.push("__null__".to_string());
        } else {
            values.push(
                array_value_to_string(array.as_ref(), row_index)
                    .map_err(|error| error.to_string())?,
            );
        }
    }
    if render {
        Ok(values.join(&config.column_key_separator))
    } else {
        Ok(values
            .into_iter()
            .map(|value| format!("{}:{value}", value.len()))
            .collect::<Vec<_>>()
            .join("|"))
    }
}

fn render_column_name(value: &PivotValueKey, column_value: Option<&str>) -> Result<String, String> {
    let value_name = value
        .name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or(&value.source_column);
    let default_rule = if column_value.is_some() {
        "<column_key_value>__<value_key_name>"
    } else {
        "<value_key_name>__<agg>"
    };
    let rule = value.column_name_rule.as_deref().unwrap_or(default_rule);
    if column_value.is_none()
        && (rule.contains("<column_key_value>") || rule.contains("{column_key_value}"))
    {
        return Err("pivot value without column cannot reference column_key_value".to_string());
    }
    let rendered = rule
        .replace("<column_key_value>", column_value.unwrap_or(""))
        .replace("{column_key_value}", column_value.unwrap_or(""))
        .replace("<value_key_name>", value_name)
        .replace("{value_key_name}", value_name)
        .replace("<agg>", &value.aggregation)
        .replace("{agg}", &value.aggregation);
    if rendered.trim().is_empty() {
        return Err("pivot column name rule produced an empty name".to_string());
    }
    Ok(rendered)
}

fn ensure_unique_name(names: &mut HashSet<String>, name: &str) -> Result<(), String> {
    if !names.insert(name.to_string()) {
        return Err(format!("pivot output column name collision: {name}"));
    }
    Ok(())
}

fn aggregate_column(
    source: &ArrayRef,
    groups: &[&[usize]],
    value: &PivotValueKey,
) -> Result<(ArrayRef, usize), String> {
    let aggregation = value.aggregation.to_lowercase();
    let mut duplicate_first_cells = 0usize;
    let output: ArrayRef = match aggregation.as_str() {
        "first" => {
            let indices = groups
                .iter()
                .map(|indices| {
                    if indices.len() > 1 {
                        duplicate_first_cells += 1;
                    }
                    indices.first().map(|index| *index as u32)
                })
                .collect::<Vec<_>>();
            take(source.as_ref(), &UInt32Array::from(indices), None)
                .map_err(|error| error.to_string())?
        }
        "count" => Arc::new(Int64Array::from(
            groups
                .iter()
                .map(|indices| {
                    indices
                        .iter()
                        .filter(|index| !source.is_null(**index))
                        .count() as i64
                })
                .collect::<Vec<_>>(),
        )),
        "sum" | "avg" | "mean" => {
            let values = groups
                .iter()
                .map(|indices| numeric_aggregate(source, indices, aggregation.as_str()))
                .collect::<Result<Vec<_>, _>>()?;
            Arc::new(Float64Array::from(values))
        }
        "min" | "max" => {
            let indices = groups
                .iter()
                .map(|indices| extremum_index(source, indices, aggregation.as_str()))
                .collect::<Result<Vec<_>, _>>()?;
            take(source.as_ref(), &UInt32Array::from(indices), None)
                .map_err(|error| error.to_string())?
        }
        "unique_concatenate" => Arc::new(StringArray::from(
            groups
                .iter()
                .map(|indices| unique_concatenate(source, indices))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        _ => return Err(format!("unsupported pivot aggregation: {aggregation}")),
    };
    // output_dtype is an optional override.  When it is omitted, retain the
    // aggregation's native Arrow type, which for first/min/max inherits the
    // source column type.  This keeps schema declarations optional without
    // changing the natural result type of count/sum/avg/concatenation.
    let output = match value.output_dtype.as_deref() {
        Some(dtype) if !dtype.trim().is_empty() => {
            cast(&output, &parse_output_dtype(dtype)?).map_err(|error| error.to_string())?
        }
        _ => output,
    };
    Ok((output, duplicate_first_cells))
}

fn numeric_aggregate(
    source: &ArrayRef,
    indices: &[usize],
    aggregation: &str,
) -> Result<Option<f64>, String> {
    let values = indices
        .iter()
        .filter(|index| !source.is_null(**index))
        .map(|index| {
            array_value_to_string(source.as_ref(), *index)
                .map_err(|error| error.to_string())?
                .parse::<f64>()
                .map_err(|_| format!("pivot {aggregation} requires numeric input"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Ok(None);
    }
    let sum = values.iter().sum::<f64>();
    Ok(Some(if matches!(aggregation, "avg" | "mean") {
        sum / values.len() as f64
    } else {
        sum
    }))
}

fn extremum_index(
    source: &ArrayRef,
    indices: &[usize],
    aggregation: &str,
) -> Result<Option<u32>, String> {
    let mut best: Option<(usize, String)> = None;
    for index in indices
        .iter()
        .copied()
        .filter(|index| !source.is_null(*index))
    {
        let value =
            array_value_to_string(source.as_ref(), index).map_err(|error| error.to_string())?;
        let replace = match &best {
            None => true,
            Some((_, current)) => {
                let ordering = compare_values(&value, current);
                (aggregation == "min" && ordering == Ordering::Less)
                    || (aggregation == "max" && ordering == Ordering::Greater)
            }
        };
        if replace {
            best = Some((index, value));
        }
    }
    Ok(best.map(|(index, _)| index as u32))
}

fn compare_values(left: &str, right: &str) -> Ordering {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(left), Ok(right)) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
        _ => left.cmp(right),
    }
}

fn unique_concatenate(source: &ArrayRef, indices: &[usize]) -> Result<Option<String>, String> {
    let mut seen = HashSet::new();
    let mut values = Vec::new();
    for index in indices
        .iter()
        .copied()
        .filter(|index| !source.is_null(*index))
    {
        let value =
            array_value_to_string(source.as_ref(), index).map_err(|error| error.to_string())?;
        if seen.insert(value.clone()) {
            values.push(value);
        }
    }
    Ok((!values.is_empty()).then(|| values.join(",")))
}

fn parse_output_dtype(value: &str) -> Result<DataType, String> {
    match value.trim().to_uppercase().as_str() {
        "TEXT" | "STRING" => Ok(DataType::Utf8),
        "INT32" | "INTEGER" => Ok(DataType::Int32),
        "INT64" | "BIGINT" => Ok(DataType::Int64),
        "FLOAT" | "FLOAT32" => Ok(DataType::Float32),
        "DOUBLE" | "FLOAT64" => Ok(DataType::Float64),
        "BOOL" | "BOOLEAN" => Ok(DataType::Boolean),
        other => Err(format!("unsupported pivot output_dtype: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};

    fn input_batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "row",
                Arc::new(StringArray::from(vec!["A", "A", "B"])) as ArrayRef,
            ),
            (
                "column",
                Arc::new(StringArray::from(vec!["X", "Y", "X"])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn pivots_first_with_part_local_columns() {
        let config: PivotConfig = serde_json::from_str(
            r#"{
                "enabled":true,
                "row_keys":["row"],
                "column_keys":["column"],
                "value_keys":[{
                    "source_column":"value",
                    "aggregation":"first",
                    "column_name_rule":"<column_key_value>__<value_key_name>"
                }]
            }"#,
        )
        .unwrap();
        let result = pivot_record_batch(&input_batch(), &config).unwrap();
        assert_eq!(result.batch.schema().fields().len(), 3);
        assert_eq!(result.batch.num_rows(), 2);
        assert_eq!(result.enumerated_column_values, 2);
        assert_eq!(result.batch.schema().field(1).name(), "X__value");
        assert_eq!(result.batch.schema().field(2).name(), "Y__value");
    }

    #[test]
    fn preserves_dotted_output_column_as_one_arrow_field_name() {
        let config: PivotConfig = serde_json::from_str(
            r#"{
                "enabled":true,
                "row_keys":["row"],
                "value_keys_without_column":[{
                    "name":"value",
                    "source_column":"value",
                    "aggregation":"max",
                    "column_name_rule":"<value_key_name>.<agg>"
                }]
            }"#,
        )
        .unwrap();

        let result = pivot_record_batch(&input_batch(), &config).unwrap();

        assert_eq!(result.batch.schema().field(1).name(), "value.max");
        assert!(result.batch.column_by_name("value.max").is_some());
    }

    #[test]
    fn rejects_duplicate_first_cells_when_configured() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "row",
                Arc::new(StringArray::from(vec!["A", "A"])) as ArrayRef,
            ),
            (
                "column",
                Arc::new(StringArray::from(vec!["X", "X"])) as ArrayRef,
            ),
            ("value", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ])
        .unwrap();
        let config: PivotConfig = serde_json::from_str(
            r#"{
                "enabled":true,
                "row_keys":["row"],
                "column_keys":["column"],
                "first_duplicate_policy":"error",
                "value_keys":[{"source_column":"value","aggregation":"first"}]
            }"#,
        )
        .unwrap();
        assert!(pivot_record_batch(&batch, &config)
            .unwrap_err()
            .contains("duplicate cell"));
    }
}
