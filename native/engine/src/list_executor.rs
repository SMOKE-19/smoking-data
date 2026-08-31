use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, UInt32Array};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, Field, Schema};
use arrow_select::take::take;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ListExpansionShape {
    offsets: Vec<i32>,
    validity: Vec<bool>,
    pub element_rows: usize,
}

pub fn expand_list_rows(
    batch: &RecordBatch,
    columns: &[(String, String)],
) -> Result<(RecordBatch, ListExpansionShape), ArrowError> {
    if columns.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "expand_list_rows requires at least one List column".into(),
        ));
    }
    let mut aliases = HashSet::new();
    let mut sources = HashSet::new();
    let mut lists: HashMap<&str, &ListArray> = HashMap::new();
    for (source, alias) in columns {
        if !sources.insert(source.as_str()) || !aliases.insert(alias.as_str()) {
            return Err(ArrowError::InvalidArgumentError(
                "list.duplicate_binding: expand source and alias names must be unique".into(),
            ));
        }
        let array = batch.column_by_name(source).ok_or_else(|| {
            ArrowError::SchemaError(format!(
                "expand_list_rows source column is missing: {source}"
            ))
        })?;
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "list.unsupported_nested_type: expand source must be List, got {} for {source}",
                array.data_type()
            ))
        })?;
        if matches!(
            list.values().data_type(),
            arrow_schema::DataType::List(_) | arrow_schema::DataType::LargeList(_)
        ) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "list.unsupported_nested_type: nested List source {source}"
            )));
        }
        lists.insert(source, list);
    }

    let template = lists[columns[0].0.as_str()];
    for (source, _) in columns.iter().skip(1) {
        ensure_same_shape(template, lists[source.as_str()])?;
    }
    let parent_indices = parent_indices(template)?;
    let mut fields: Vec<Arc<Field>> = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    let aliases_by_source: HashMap<&str, &str> = columns
        .iter()
        .map(|(source, alias)| (source.as_str(), alias.as_str()))
        .collect();
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        if let Some(alias) = aliases_by_source.get(field.name().as_str()) {
            let values = lists[field.name().as_str()].values().clone();
            fields.push(Arc::new(Field::new(
                (*alias).to_string(),
                values.data_type().clone(),
                true,
            )));
            arrays.push(values);
        } else {
            let expanded = take(array.as_ref(), &parent_indices, None)?;
            fields.push(Arc::new(Field::new(
                field.name(),
                expanded.data_type().clone(),
                field.is_nullable(),
            )));
            arrays.push(expanded);
        }
    }
    let shape = ListExpansionShape {
        offsets: template.value_offsets().to_vec(),
        validity: (0..template.len())
            .map(|index| template.is_valid(index))
            .collect(),
        element_rows: template.values().len(),
    };
    Ok((
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?,
        shape,
    ))
}

pub fn compact_list_rows(
    values: ArrayRef,
    shape: &ListExpansionShape,
    output_name: &str,
) -> Result<ArrayRef, ArrowError> {
    if values.len() != shape.element_rows {
        return Err(ArrowError::InvalidArgumentError(format!(
            "list.cardinality_change_unsupported: expected={} elements, actual={} for {output_name}",
            shape.element_rows,
            values.len()
        )));
    }
    let offsets = OffsetBuffer::new(ScalarBuffer::from(shape.offsets.clone()));
    let nulls = if shape.validity.iter().all(|valid| *valid) {
        None
    } else {
        Some(NullBuffer::from(shape.validity.clone()))
    };
    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        offsets,
        values,
        nulls,
    )?))
}

fn parent_indices(list: &ListArray) -> Result<UInt32Array, ArrowError> {
    let mut result = Vec::with_capacity(list.values().len());
    for (parent, offsets) in list.value_offsets().windows(2).enumerate() {
        let count = usize::try_from(offsets[1] - offsets[0])
            .map_err(|_| ArrowError::InvalidArgumentError("negative List offset length".into()))?;
        let parent = u32::try_from(parent).map_err(|_| {
            ArrowError::InvalidArgumentError("List parent index exceeds UInt32".into())
        })?;
        result.extend(std::iter::repeat_n(parent, count));
    }
    Ok(UInt32Array::from(result))
}

fn ensure_same_shape(left: &ListArray, right: &ListArray) -> Result<(), ArrowError> {
    if left.len() != right.len() || left.value_offsets() != right.value_offsets() {
        return Err(ArrowError::InvalidArgumentError(
            "list.shape_mismatch: zipped Lists require identical per-row element counts".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int64Array, StringArray};

    fn lists(rows: Vec<Option<Vec<Option<i64>>>>) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(rows))
    }

    #[test]
    fn expands_and_compacts_null_empty_and_null_element_shapes() {
        let source = lists(vec![
            None,
            Some(vec![]),
            Some(vec![None]),
            Some(vec![Some(2), Some(3)]),
        ]);
        let batch = RecordBatch::try_from_iter(vec![
            (
                "row_key",
                Arc::new(StringArray::from(vec!["n", "e", "x", "v"])) as ArrayRef,
            ),
            ("values", source.clone()),
        ])
        .unwrap();

        let (expanded, shape) =
            expand_list_rows(&batch, &[("values".to_string(), "value".to_string())])
                .expect("expand List rows");

        assert_eq!(shape.offsets.len() - 1, 4);
        assert_eq!(shape.element_rows, 3);
        assert_eq!(
            expanded
                .column_by_name("row_key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec!["x", "v", "v"])
        );
        let calculated: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(20), Some(30)]));
        let compacted = compact_list_rows(calculated, &shape, "adjusted").expect("compact List");
        let expected = lists(vec![
            None,
            Some(vec![]),
            Some(vec![None]),
            Some(vec![Some(20), Some(30)]),
        ]);
        assert_eq!(compacted.to_data(), expected.to_data());
    }

    #[test]
    fn strict_zip_rejects_different_list_lengths() {
        let batch = RecordBatch::try_from_iter(vec![
            ("left", lists(vec![Some(vec![Some(1), Some(2)])])),
            ("right", lists(vec![Some(vec![Some(1)])])),
        ])
        .unwrap();

        let error = expand_list_rows(
            &batch,
            &[
                ("left".to_string(), "left_value".to_string()),
                ("right".to_string(), "right_value".to_string()),
            ],
        )
        .unwrap_err();

        assert!(error.to_string().contains("list.shape_mismatch"));
    }

    #[test]
    fn compaction_rejects_element_cardinality_changes() {
        let batch =
            RecordBatch::try_from_iter(vec![("values", lists(vec![Some(vec![Some(1), Some(2)])]))])
                .unwrap();
        let (_, shape) =
            expand_list_rows(&batch, &[("values".to_string(), "value".to_string())]).unwrap();

        let error = compact_list_rows(
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            &shape,
            "adjusted",
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("list.cardinality_change_unsupported"));
    }
}
