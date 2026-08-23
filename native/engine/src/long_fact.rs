use arrow_array::{
    new_null_array, Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_cast::cast;
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use arrow_select::concat::concat;
use std::sync::Arc;

pub struct FactColumnMetadata {
    pub expression_hash: String,
    pub binding_hash: String,
    pub source_fingerprint: String,
}

const LANE_NAMES: &[&str] = &[
    "_sd_value_boolean",
    "_sd_value_int64",
    "_sd_value_float64",
    "_sd_value_decimal",
    "_sd_value_string",
    "_sd_value_datetime",
    "_sd_value_duration",
    "_sd_value_list_boolean",
    "_sd_value_list_int64",
    "_sd_value_list_float64",
    "_sd_value_list_decimal",
    "_sd_value_list_string",
    "_sd_value_list_datetime",
    "_sd_value_list_duration",
];

pub fn to_long_fact_v1(
    batch: &RecordBatch,
    identity_columns: &[String],
    calculated_columns: &[String],
) -> Result<RecordBatch, ArrowError> {
    if identity_columns.is_empty() || calculated_columns.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "long_fact_v1 requires identity and calculated columns".into(),
        ));
    }
    let mut fields: Vec<Arc<Field>> = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for name in identity_columns {
        let index = batch.schema().index_of(name)?;
        let source = batch.column(index);
        let chunks: Vec<&dyn Array> = (0..calculated_columns.len())
            .map(|_| source.as_ref())
            .collect();
        fields.push(batch.schema().field(index).clone().into());
        arrays.push(concat(&chunks)?);
    }

    let mut normalized = Vec::with_capacity(calculated_columns.len());
    for name in calculated_columns {
        let source = batch.column_by_name(name).ok_or_else(|| {
            ArrowError::SchemaError(format!("long_fact calculated column is missing: {name}"))
        })?;
        normalized.push(normalize_value(source.clone())?);
    }
    let row_count = batch.num_rows();
    fields.push(Arc::new(Field::new(
        "_sd_column_name",
        DataType::Utf8,
        false,
    )));
    let column_names: Vec<&str> = calculated_columns
        .iter()
        .flat_map(|name| std::iter::repeat_n(name.as_str(), row_count))
        .collect();
    arrays.push(Arc::new(StringArray::from(column_names)));
    fields.push(Arc::new(Field::new(
        "_sd_value_type",
        DataType::Utf8,
        false,
    )));
    let value_types: Vec<&str> = normalized
        .iter()
        .flat_map(|value| std::iter::repeat_n(value.tag, row_count))
        .collect();
    arrays.push(Arc::new(StringArray::from(value_types)));

    for (lane_index, (lane_name, lane_type)) in lane_contracts().iter().enumerate() {
        let lane_chunks: Vec<ArrayRef> = normalized
            .iter()
            .map(|value| {
                if value.lane_index == lane_index {
                    value.array.clone()
                } else {
                    new_null_array(lane_type, row_count)
                }
            })
            .collect();
        let references: Vec<&dyn Array> = lane_chunks.iter().map(|item| item.as_ref()).collect();
        fields.push(Arc::new(Field::new(*lane_name, lane_type.clone(), true)));
        arrays.push(concat(&references)?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
}

pub fn to_persisted_long_fact_v1(
    batch: &RecordBatch,
    identity_columns: &[String],
    calculated_columns: &[String],
    column_metadata: &[FactColumnMetadata],
    generation_seq: i64,
    calculated_at_us: i64,
) -> Result<RecordBatch, ArrowError> {
    if calculated_columns.len() != column_metadata.len() {
        return Err(ArrowError::InvalidArgumentError(
            "long_fact.metadata_mismatch: calculated column metadata length differs".into(),
        ));
    }
    let base = to_long_fact_v1(batch, identity_columns, calculated_columns)?;
    let row_count = batch.num_rows();
    let fact_rows = row_count * calculated_columns.len();
    let mut fields = base.schema().fields().to_vec();
    let mut arrays = base.columns().to_vec();
    fields.extend([
        Arc::new(Field::new("_sd_generation_seq", DataType::Int64, false)),
        Arc::new(Field::new("_sd_expression_hash", DataType::Utf8, false)),
        Arc::new(Field::new("_sd_binding_hash", DataType::Utf8, false)),
        Arc::new(Field::new("_sd_source_fingerprint", DataType::Utf8, false)),
        Arc::new(Field::new(
            "_sd_calculated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )),
        Arc::new(Field::new("_sd_is_deleted", DataType::Boolean, false)),
    ]);
    arrays.push(Arc::new(Int64Array::from_value(generation_seq, fact_rows)));
    arrays.push(Arc::new(StringArray::from(
        column_metadata
            .iter()
            .flat_map(|item| std::iter::repeat_n(item.expression_hash.as_str(), row_count))
            .collect::<Vec<_>>(),
    )));
    arrays.push(Arc::new(StringArray::from(
        column_metadata
            .iter()
            .flat_map(|item| std::iter::repeat_n(item.binding_hash.as_str(), row_count))
            .collect::<Vec<_>>(),
    )));
    arrays.push(Arc::new(StringArray::from(
        column_metadata
            .iter()
            .flat_map(|item| std::iter::repeat_n(item.source_fingerprint.as_str(), row_count))
            .collect::<Vec<_>>(),
    )));
    arrays.push(Arc::new(
        TimestampMicrosecondArray::from_value(calculated_at_us, fact_rows).with_timezone("UTC"),
    ));
    arrays.push(Arc::new(BooleanArray::from(vec![false; fact_rows])));
    let schema = Schema::new_with_metadata(
        fields,
        [(
            "smoking_data.contract".to_string(),
            "long_fact_v1".to_string(),
        )]
        .into_iter()
        .collect(),
    );
    RecordBatch::try_new(Arc::new(schema), arrays)
}

struct NormalizedValue {
    tag: &'static str,
    lane_index: usize,
    array: ArrayRef,
}

fn normalize_value(array: ArrayRef) -> Result<NormalizedValue, ArrowError> {
    let (tag, lane_index, target) = match array.data_type() {
        DataType::Boolean => ("boolean", 0, DataType::Boolean),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => ("int64", 1, DataType::Int64),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            ("float64", 2, DataType::Float64)
        }
        DataType::Decimal128(_, _) => ("decimal", 3, DataType::Decimal128(38, 10)),
        DataType::Utf8 | DataType::LargeUtf8 => ("string", 4, DataType::Utf8),
        DataType::Timestamp(_, _) => (
            "datetime",
            5,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        DataType::Duration(_) => ("duration", 6, DataType::Duration(TimeUnit::Microsecond)),
        DataType::List(field) => list_contract(field.data_type())?,
        DataType::LargeList(_) => {
            return Err(ArrowError::NotYetImplemented(
                "long_fact.unsupported_dtype: LargeList must be normalized before long_fact_v1"
                    .into(),
            ))
        }
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "long_fact.unsupported_dtype: {other}"
            )))
        }
    };
    let normalized = if array.data_type() == &target {
        array
    } else {
        cast(array.as_ref(), &target)?
    };
    Ok(NormalizedValue {
        tag,
        lane_index,
        array: normalized,
    })
}

fn list_contract(child: &DataType) -> Result<(&'static str, usize, DataType), ArrowError> {
    let (tag, lane_index, target_child) = match child {
        DataType::Boolean => ("list_boolean", 7, DataType::Boolean),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => ("list_int64", 8, DataType::Int64),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            ("list_float64", 9, DataType::Float64)
        }
        DataType::Decimal128(_, _) => ("list_decimal", 10, DataType::Decimal128(38, 10)),
        DataType::Utf8 | DataType::LargeUtf8 => ("list_string", 11, DataType::Utf8),
        DataType::Timestamp(_, _) => (
            "list_datetime",
            12,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        DataType::Duration(_) => (
            "list_duration",
            13,
            DataType::Duration(TimeUnit::Microsecond),
        ),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "long_fact.unsupported_dtype: List<{other}>"
            )))
        }
    };
    Ok((
        tag,
        lane_index,
        DataType::List(Arc::new(Field::new("item", target_child, true))),
    ))
}

fn lane_contracts() -> Vec<(&'static str, DataType)> {
    let scalar = vec![
        DataType::Boolean,
        DataType::Int64,
        DataType::Float64,
        DataType::Decimal128(38, 10),
        DataType::Utf8,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Duration(TimeUnit::Microsecond),
    ];
    let list: Vec<DataType> = scalar
        .iter()
        .cloned()
        .map(|child| DataType::List(Arc::new(Field::new("item", child, true))))
        .collect();
    LANE_NAMES
        .iter()
        .copied()
        .zip(scalar.into_iter().chain(list))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Float64Type;
    use arrow_array::{Int32Array, ListArray, StringArray};

    #[test]
    fn emits_one_typed_lane_per_scalar_and_list_fact() {
        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(1.5), None]),
            Some(vec![]),
        ]));
        let batch = RecordBatch::try_from_iter(vec![
            (
                "row_key",
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
            (
                "scalar.value",
                Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
            ),
            ("list.value", lists),
        ])
        .unwrap();

        let output = to_long_fact_v1(
            &batch,
            &["row_key".to_string()],
            &["scalar.value".to_string(), "list.value".to_string()],
        )
        .expect("long fact conversion");

        assert_eq!(output.num_rows(), 4);
        assert_eq!(
            output
                .column_by_name("_sd_column_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec![
                "scalar.value",
                "scalar.value",
                "list.value",
                "list.value",
            ])
        );
        assert_eq!(
            output
                .column_by_name("_sd_value_type")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec!["int64", "int64", "list_float64", "list_float64",])
        );
        let int_lane = output.column_by_name("_sd_value_int64").unwrap();
        let list_lane = output.column_by_name("_sd_value_list_float64").unwrap();
        assert_eq!(int_lane.null_count(), 2);
        assert_eq!(list_lane.null_count(), 2);
    }

    #[test]
    fn rejects_nested_list_results() {
        let nested = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(1.0)]),
        ])) as ArrayRef;
        let outer = Arc::new(ListArray::new(
            Arc::new(Field::new("item", nested.data_type().clone(), true)),
            arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0_i32, 1])),
            nested,
            None,
        )) as ArrayRef;
        let batch = RecordBatch::try_from_iter(vec![
            (
                "row_key",
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            ),
            ("nested", outer),
        ])
        .unwrap();

        let error =
            to_long_fact_v1(&batch, &["row_key".to_string()], &["nested".to_string()]).unwrap_err();

        assert!(error.to_string().contains("long_fact.unsupported_dtype"));
    }

    #[test]
    fn preserves_null_empty_and_null_element_list_values() {
        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            None,
            Some(vec![]),
            Some(vec![None]),
        ]));
        let batch = RecordBatch::try_from_iter(vec![
            (
                "row_key",
                Arc::new(StringArray::from(vec!["n", "e", "x"])) as ArrayRef,
            ),
            ("result", lists.clone()),
        ])
        .unwrap();

        let output =
            to_long_fact_v1(&batch, &["row_key".to_string()], &["result".to_string()]).unwrap();

        assert_eq!(
            output
                .column_by_name("_sd_value_list_float64")
                .unwrap()
                .to_data(),
            lists.to_data()
        );
    }

    #[test]
    fn appends_generation_and_per_expression_fingerprint_metadata() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "row_key",
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
            ("first", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
            ("second", Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef),
        ])
        .unwrap();
        let metadata = vec![
            FactColumnMetadata {
                expression_hash: "expression-1".into(),
                binding_hash: "binding-1".into(),
                source_fingerprint: "source-a".into(),
            },
            FactColumnMetadata {
                expression_hash: "expression-2".into(),
                binding_hash: "binding-2".into(),
                source_fingerprint: "source-b".into(),
            },
        ];

        let output = to_persisted_long_fact_v1(
            &batch,
            &["row_key".to_string()],
            &["first".to_string(), "second".to_string()],
            &metadata,
            7,
            1_786_736_400_000_000,
        )
        .unwrap();

        assert_eq!(output.num_rows(), 4);
        assert_eq!(
            output
                .column_by_name("_sd_source_fingerprint")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec!["source-a", "source-a", "source-b", "source-b"])
        );
        assert_eq!(
            output.schema().metadata().get("smoking_data.contract"),
            Some(&"long_fact_v1".to_string())
        );
        assert_eq!(
            output
                .column_by_name("_sd_generation_seq")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
    }
}
