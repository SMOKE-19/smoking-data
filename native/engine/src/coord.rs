use arrow_array::{
    types::Int32Type, Array, DictionaryArray, Int32Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray,
};
use arrow_ipc::reader::FileReader;
use std::collections::BTreeMap;
use std::fs::File;

#[derive(Clone, Debug)]
pub struct CoordGroup {
    pub source_file: String,
    pub row_group_id: usize,
    pub row_offsets: Vec<usize>,
    pub active_orders: Vec<i64>,
}

fn column_index(batch: &RecordBatch, name: &str, coord_path: &str) -> pyo3::PyResult<usize> {
    batch.schema().index_of(name).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "missing coord column {name} in {coord_path}: {err}"
        ))
    })
}

fn string_value(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
    coord_path: &str,
) -> pyo3::PyResult<String> {
    if let Some(values) = batch
        .column(column_index)
        .as_any()
        .downcast_ref::<StringArray>()
    {
        if values.is_null(row_index) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "coord source_file cannot be null in {coord_path} at row {row_index}"
            )));
        }
        return Ok(values.value(row_index).to_string());
    }
    if let Some(values) = batch
        .column(column_index)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
    {
        if values.is_null(row_index) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "coord source_file cannot be null in {coord_path} at row {row_index}"
            )));
        }
        let dictionary = values
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "coord source_file dictionary values must be utf8 in {coord_path}"
                ))
            })?;
        let key = values.keys().value(row_index) as usize;
        return Ok(dictionary.value(key).to_string());
    }
    if let Some(values) = batch
        .column(column_index)
        .as_any()
        .downcast_ref::<LargeStringArray>()
    {
        if values.is_null(row_index) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "coord source_file cannot be null in {coord_path} at row {row_index}"
            )));
        }
        return Ok(values.value(row_index).to_string());
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "coord source_file must be string-like in {coord_path}"
    )))
}

fn integer_value(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
    coord_path: &str,
    name: &str,
) -> pyo3::PyResult<i64> {
    if let Some(values) = batch
        .column(column_index)
        .as_any()
        .downcast_ref::<Int32Array>()
    {
        if values.is_null(row_index) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "coord {name} cannot be null in {coord_path} at row {row_index}"
            )));
        }
        return Ok(values.value(row_index) as i64);
    }
    if let Some(values) = batch
        .column(column_index)
        .as_any()
        .downcast_ref::<Int64Array>()
    {
        if values.is_null(row_index) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "coord {name} cannot be null in {coord_path} at row {row_index}"
            )));
        }
        return Ok(values.value(row_index));
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "coord {name} must be int32/int64 in {coord_path}"
    )))
}

pub fn read_coord_groups(coord_path: &str) -> pyo3::PyResult<Vec<CoordGroup>> {
    let file = File::open(coord_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to open coord file {coord_path}: {err}"
        ))
    })?;
    let reader = FileReader::try_new(file, None).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to initialize coord IPC reader {coord_path}: {err}"
        ))
    })?;

    let mut grouped: BTreeMap<(String, usize), Vec<(usize, i64)>> = BTreeMap::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to read coord batch {coord_path}: {err}"
            ))
        })?;
        let source_file_idx = column_index(&batch, "source_file", coord_path)?;
        let row_group_idx = column_index(&batch, "row_group_id", coord_path)?;
        let row_offset_idx = column_index(&batch, "row_offset_in_group", coord_path)?;
        let active_order_idx = batch.schema().index_of("active_order").ok();
        for row_index in 0..batch.num_rows() {
            let source_file = string_value(&batch, source_file_idx, row_index, coord_path)?;
            let row_group_id =
                integer_value(&batch, row_group_idx, row_index, coord_path, "row_group_id")?;
            let row_offset = integer_value(
                &batch,
                row_offset_idx,
                row_index,
                coord_path,
                "row_offset_in_group",
            )?;
            let active_order = match active_order_idx {
                Some(index) => integer_value(&batch, index, row_index, coord_path, "active_order")?,
                None => row_index as i64,
            };
            if row_group_id < 0 || row_offset < 0 {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "coord row_group_id and row_offset_in_group must be non-negative in {coord_path}"
                )));
            }
            grouped
                .entry((source_file, row_group_id as usize))
                .or_default()
                .push((row_offset as usize, active_order));
        }
    }

    Ok(grouped
        .into_iter()
        .map(|((source_file, row_group_id), mut coordinates)| {
            coordinates.sort_unstable_by_key(|item| item.0);
            coordinates.dedup_by_key(|item| item.0);
            CoordGroup {
                source_file,
                row_group_id,
                row_offsets: coordinates.iter().map(|item| item.0).collect(),
                active_orders: coordinates.iter().map(|item| item.1).collect(),
            }
        })
        .collect())
}
