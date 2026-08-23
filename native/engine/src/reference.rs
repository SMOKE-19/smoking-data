use arrow_array::{Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{HashMap, HashSet};
use std::fs::File;

pub type DenseReferenceMap = HashMap<String, (Vec<i32>, Vec<i32>)>;

fn batch_column_index(batch: &RecordBatch, name: &str, lookup_path: &str) -> pyo3::PyResult<usize> {
    batch.schema().index_of(name).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "missing {name} column in {lookup_path}: {err}"
        ))
    })
}

fn batch_string_values(
    batch: &RecordBatch,
    index: usize,
    name: &str,
    lookup_path: &str,
) -> pyo3::PyResult<Vec<String>> {
    if let Some(values) = batch.column(index).as_any().downcast_ref::<StringArray>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| values.value(row_index).to_string())
            .collect());
    }
    if let Some(values) = batch
        .column(index)
        .as_any()
        .downcast_ref::<LargeStringArray>()
    {
        return Ok((0..batch.num_rows())
            .map(|row_index| values.value(row_index).to_string())
            .collect());
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "invalid {name} column type in {lookup_path}: {:?}",
        batch.column(index).data_type()
    )))
}

fn batch_i32_values(
    batch: &RecordBatch,
    index: usize,
    name: &str,
    lookup_path: &str,
) -> pyo3::PyResult<Vec<i32>> {
    if let Some(values) = batch.column(index).as_any().downcast_ref::<Int32Array>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| values.value(row_index))
            .collect());
    }
    if let Some(values) = batch.column(index).as_any().downcast_ref::<Int64Array>() {
        return (0..batch.num_rows())
            .map(|row_index| {
                i32::try_from(values.value(row_index)).map_err(|err| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "invalid {name} value in {lookup_path}: {err}"
                    ))
                })
            })
            .collect();
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "invalid {name} column type in {lookup_path}: {:?}",
        batch.column(index).data_type()
    )))
}

pub fn load_reference_map(
    lookup_path: &str,
    key_column: &str,
    order_column: &str,
    coord_columns: &[String],
) -> pyo3::PyResult<DenseReferenceMap> {
    if coord_columns.len() != 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "lookup.coord_columns must contain exactly 2 items, got {}",
            coord_columns.len()
        )));
    }

    let file = File::open(lookup_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to open lookup parquet {lookup_path}: {err}"
        ))
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to initialize arrow parquet reader {lookup_path}: {err}"
            ))
        })?
        .build()
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to build arrow parquet reader {lookup_path}: {err}"
            ))
        })?;

    let mut grouped: HashMap<String, Vec<(i32, i32, i32)>> = HashMap::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to read record batch from {lookup_path}: {err}"
            ))
        })?;

        let group_key_index = batch_column_index(&batch, key_column, lookup_path)?;
        let order_idx_index = batch_column_index(&batch, order_column, lookup_path)?;
        let coord_a_index = batch_column_index(&batch, &coord_columns[0], lookup_path)?;
        let coord_b_index = batch_column_index(&batch, &coord_columns[1], lookup_path)?;

        let group_keys = batch_string_values(&batch, group_key_index, key_column, lookup_path)?;
        let order_idx = batch_i32_values(&batch, order_idx_index, order_column, lookup_path)?;
        let coord_a = batch_i32_values(&batch, coord_a_index, &coord_columns[0], lookup_path)?;
        let coord_b = batch_i32_values(&batch, coord_b_index, &coord_columns[1], lookup_path)?;

        for row_index in 0..batch.num_rows() {
            let group_key = group_keys[row_index].clone();
            let order_idx = order_idx[row_index];
            let coord_a = coord_a[row_index];
            let coord_b = coord_b[row_index];
            grouped
                .entry(group_key)
                .or_default()
                .push((order_idx, coord_a, coord_b));
        }
    }

    let mut refs: DenseReferenceMap = HashMap::with_capacity(grouped.len());
    for (group_key, mut items) in grouped {
        items.sort_by_key(|(order_idx, _, _)| *order_idx);
        let mut seen_order = HashSet::with_capacity(items.len());
        let mut dense_coord_a = Vec::with_capacity(items.len());
        let mut dense_coord_b = Vec::with_capacity(items.len());

        for (order_idx, coord_a, coord_b) in items {
            if !seen_order.insert(order_idx) {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "duplicate order_idx={order_idx} found for group_key={group_key}"
                )));
            }
            dense_coord_a.push(coord_a);
            dense_coord_b.push(coord_b);
        }
        refs.insert(group_key, (dense_coord_a, dense_coord_b));
    }
    Ok(refs)
}

pub fn build_dense_index(
    dense_coord_a: &[i32],
    dense_coord_b: &[i32],
) -> pyo3::PyResult<HashMap<(i32, i32), usize>> {
    if dense_coord_a.len() != dense_coord_b.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "dense coordinate length mismatch: {} != {}",
            dense_coord_a.len(),
            dense_coord_b.len()
        )));
    }
    let mut index = HashMap::with_capacity(dense_coord_a.len());
    for (position, (&coord_a, &coord_b)) in
        dense_coord_a.iter().zip(dense_coord_b.iter()).enumerate()
    {
        index.insert((coord_a, coord_b), position);
    }
    Ok(index)
}
