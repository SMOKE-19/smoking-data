use arrow_arith::{
    boolean::{and_kleene, not, or_kleene},
    numeric::{add, div, mul, rem, sub},
};
use arrow_array::{
    builder::{ListBuilder, StringBuilder},
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, DurationMicrosecondArray,
    Float64Array, Int64Array, LargeStringArray, ListArray, NullArray, RecordBatch, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow_cast::cast;
use arrow_cast::display::array_value_to_string;
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_ord::sort::{lexsort_to_indices, SortColumn};
use arrow_schema::{ArrowError, DataType, Field, Schema, SortOptions, TimeUnit};
use arrow_select::take::take;
use arrow_select::zip::zip;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Weekday};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::expression_ir::{ExpressionIrDocument, ExpressionNode, IrExpression, WindowOrder};
use crate::list_executor::{compact_list_rows, expand_list_rows};

pub fn execute_expression_ir(
    mut batch: RecordBatch,
    document: &ExpressionIrDocument,
) -> Result<RecordBatch, ArrowError> {
    for layer in &document.layers {
        let input = batch.clone();
        let mut fields: Vec<Arc<Field>> = input.schema().fields().iter().cloned().collect();
        let mut columns = input.columns().to_vec();
        for expression in &layer.expressions {
            let value = evaluate_expression(expression, &input)?;
            if let Some(index) = fields
                .iter()
                .position(|field| field.name() == &expression.name)
            {
                fields[index] = Arc::new(Field::new(
                    expression.name.clone(),
                    value.data_type().clone(),
                    expression.nullable,
                ));
                columns[index] = value;
            } else {
                fields.push(Arc::new(Field::new(
                    expression.name.clone(),
                    value.data_type().clone(),
                    expression.nullable,
                )));
                columns.push(value);
            }
        }
        batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    }
    Ok(batch)
}

fn evaluate_expression(
    expression: &IrExpression,
    batch: &RecordBatch,
) -> Result<ArrayRef, ArrowError> {
    let mut referenced_columns = Vec::new();
    collect_node_columns(&expression.expr, &mut referenced_columns);
    let list_dependencies: Vec<(String, String)> = referenced_columns
        .into_iter()
        .filter_map(|name| {
            batch.column_by_name(&name).and_then(|array| {
                matches!(array.data_type(), DataType::List(_)).then(|| (name.clone(), name.clone()))
            })
        })
        .collect();
    if list_dependencies.is_empty() || node_supports_list_native(&expression.expr) {
        return evaluate_node(&expression.expr, batch);
    }
    if node_contains_window(&expression.expr) {
        return Err(ArrowError::NotYetImplemented(format!(
            "list.expression_unsupported: window expression requires a complete-group contract: {}",
            expression.name
        )));
    }
    let (expanded, shape) = expand_list_rows(batch, &list_dependencies)?;
    let values = evaluate_node(&expression.expr, &expanded)?;
    compact_list_rows(values, &shape, &expression.name)
}

fn collect_node_columns(node: &ExpressionNode, columns: &mut Vec<String>) {
    match node {
        ExpressionNode::Column { name } => {
            if !columns.contains(name) {
                columns.push(name.clone());
            }
        }
        ExpressionNode::Literal { .. } => {}
        ExpressionNode::Alias { expression, .. }
        | ExpressionNode::Unary {
            operand: expression,
            ..
        }
        | ExpressionNode::Cast { expression, .. } => collect_node_columns(expression, columns),
        ExpressionNode::Binary { left, right, .. } => {
            collect_node_columns(left, columns);
            collect_node_columns(right, columns);
        }
        ExpressionNode::Call { arguments, .. } => {
            for argument in arguments {
                collect_node_columns(argument, columns);
            }
        }
        ExpressionNode::Case {
            branches,
            otherwise,
        } => {
            for branch in branches {
                collect_node_columns(&branch.when, columns);
                collect_node_columns(&branch.then, columns);
            }
            collect_node_columns(otherwise, columns);
        }
        ExpressionNode::Window {
            expression,
            partition_by,
            order_by,
            ..
        } => {
            collect_node_columns(expression, columns);
            for partition in partition_by {
                collect_node_columns(partition, columns);
            }
            for order in order_by {
                collect_node_columns(&order.expression, columns);
            }
        }
    }
}

fn node_supports_list_native(node: &ExpressionNode) -> bool {
    match node {
        ExpressionNode::Column { .. } | ExpressionNode::Literal { .. } => true,
        ExpressionNode::Alias { expression, .. } => node_supports_list_native(expression),
        ExpressionNode::Binary {
            operator,
            left,
            right,
        } => {
            matches!(
                operator.as_str(),
                "+" | "-" | "*" | "/" | "%" | "=" | "!=" | "<" | "<=" | ">" | ">=" | "and" | "or"
            ) && node_supports_list_native(left)
                && node_supports_list_native(right)
        }
        ExpressionNode::Case {
            branches,
            otherwise,
        } => {
            branches.iter().all(|branch| {
                node_supports_list_native(&branch.when) && node_supports_list_native(&branch.then)
            }) && node_supports_list_native(otherwise)
        }
        ExpressionNode::Unary { .. }
        | ExpressionNode::Call { .. }
        | ExpressionNode::Cast { .. }
        | ExpressionNode::Window { .. } => false,
    }
}

fn node_contains_window(node: &ExpressionNode) -> bool {
    match node {
        ExpressionNode::Window { .. } => true,
        ExpressionNode::Alias { expression, .. }
        | ExpressionNode::Unary {
            operand: expression,
            ..
        }
        | ExpressionNode::Cast { expression, .. } => node_contains_window(expression),
        ExpressionNode::Binary { left, right, .. } => {
            node_contains_window(left) || node_contains_window(right)
        }
        ExpressionNode::Call { arguments, .. } => arguments.iter().any(node_contains_window),
        ExpressionNode::Case {
            branches,
            otherwise,
        } => {
            branches.iter().any(|branch| {
                node_contains_window(&branch.when) || node_contains_window(&branch.then)
            }) || node_contains_window(otherwise)
        }
        ExpressionNode::Column { .. } | ExpressionNode::Literal { .. } => false,
    }
}

fn evaluate_node(node: &ExpressionNode, batch: &RecordBatch) -> Result<ArrayRef, ArrowError> {
    match node {
        ExpressionNode::Column { name } => batch
            .column_by_name(name)
            .cloned()
            .ok_or_else(|| ArrowError::SchemaError(format!("missing expression column: {name}"))),
        ExpressionNode::Literal { dtype, value } => literal_array(dtype, value, batch.num_rows()),
        ExpressionNode::Alias { expression, .. } => evaluate_node(expression, batch),
        ExpressionNode::Unary { operator, operand } => {
            let value = evaluate_node(operand, batch)?;
            match operator.as_str() {
                "positive" => Ok(value),
                "negate" => {
                    let zero: ArrayRef = match value.data_type() {
                        arrow_schema::DataType::Int64 => {
                            Arc::new(Int64Array::from_value(0, batch.num_rows()))
                        }
                        arrow_schema::DataType::Float64 => {
                            Arc::new(Float64Array::from_value(0.0, batch.num_rows()))
                        }
                        other => {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "negate does not support {other}"
                            )))
                        }
                    };
                    sub(&zero.as_ref(), &value.as_ref())
                }
                "not" => {
                    let boolean =
                        value
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                ArrowError::InvalidArgumentError(format!(
                                    "not requires Boolean, got {}",
                                    value.data_type()
                                ))
                            })?;
                    Ok(Arc::new(not(boolean)?))
                }
                other => Err(ArrowError::NotYetImplemented(format!(
                    "unsupported unary operator: {other}"
                ))),
            }
        }
        ExpressionNode::Binary {
            operator,
            left,
            right,
        } => {
            let left = evaluate_node(left, batch)?;
            let right = evaluate_node(right, batch)?;
            evaluate_binary(operator, left, right)
        }
        ExpressionNode::Call {
            function,
            arguments,
        } => evaluate_call(function, arguments, batch),
        ExpressionNode::Case {
            branches,
            otherwise,
        } => {
            let mut result = evaluate_node(otherwise, batch)?;
            for branch in branches.iter().rev() {
                let condition = evaluate_node(&branch.when, batch)?;
                let truthy = evaluate_node(&branch.then, batch)?;
                result = evaluate_case_branch(condition, truthy, result)?;
            }
            Ok(result)
        }
        ExpressionNode::Cast {
            expression,
            target_dtype,
        } => {
            let value = evaluate_node(expression, batch)?;
            cast(value.as_ref(), &ir_data_type(target_dtype)?)
        }
        ExpressionNode::Window {
            expression,
            partition_by,
            order_by,
            frame,
        } => evaluate_window(expression, partition_by, order_by, frame.as_ref(), batch),
    }
}

fn ir_data_type(dtype: &str) -> Result<DataType, ArrowError> {
    match dtype {
        "bool" => Ok(DataType::Boolean),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "float32" => Ok(DataType::Float32),
        "float64" => Ok(DataType::Float64),
        "string" => Ok(DataType::Utf8),
        "date32" => Ok(DataType::Date32),
        "time64_us" => Ok(DataType::Time64(TimeUnit::Microsecond)),
        "timestamp_us" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "duration_us" => Ok(DataType::Duration(TimeUnit::Microsecond)),
        "decimal128_38_10" => Ok(DataType::Decimal128(38, 10)),
        other => Err(ArrowError::NotYetImplemented(format!(
            "unsupported cast target: {other}"
        ))),
    }
}

fn evaluate_binary(
    operator: &str,
    left: ArrayRef,
    right: ArrayRef,
) -> Result<ArrayRef, ArrowError> {
    if matches!(left.data_type(), DataType::List(_))
        || matches!(right.data_type(), DataType::List(_))
    {
        return evaluate_list_binary(operator, left, right);
    }
    match operator {
        "+" | "-" | "*" | "/" | "%" => {
            let (left, right) = if operator == "/" {
                (
                    cast(left.as_ref(), &DataType::Float64)?,
                    cast(right.as_ref(), &DataType::Float64)?,
                )
            } else {
                promote_numeric_pair(left, right)?
            };
            match operator {
                "+" => add(&left.as_ref(), &right.as_ref()),
                "-" => sub(&left.as_ref(), &right.as_ref()),
                "*" => mul(&left.as_ref(), &right.as_ref()),
                "/" => div(&left.as_ref(), &right.as_ref()),
                "%" => rem(&left.as_ref(), &right.as_ref()),
                _ => unreachable!("validated arithmetic operator"),
            }
        }
        "=" | "!=" | "<" | "<=" | ">" | ">=" => {
            let (left, right) =
                if is_numeric_type(left.data_type()) && is_numeric_type(right.data_type()) {
                    promote_numeric_pair(left, right)?
                } else {
                    (left, right)
                };
            let result = match operator {
                "=" => eq(&left.as_ref(), &right.as_ref())?,
                "!=" => neq(&left.as_ref(), &right.as_ref())?,
                "<" => lt(&left.as_ref(), &right.as_ref())?,
                "<=" => lt_eq(&left.as_ref(), &right.as_ref())?,
                ">" => gt(&left.as_ref(), &right.as_ref())?,
                ">=" => gt_eq(&left.as_ref(), &right.as_ref())?,
                _ => unreachable!("validated comparison operator"),
            };
            Ok(Arc::new(result))
        }
        "and" | "or" => {
            let left = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("and/or requires Boolean".into())
                })?;
            let right = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("and/or requires Boolean".into())
                })?;
            let result = if operator == "and" {
                and_kleene(left, right)?
            } else {
                or_kleene(left, right)?
            };
            Ok(Arc::new(result))
        }
        "contains" => string_binary(left, right, |left, right| left.contains(right), false),
        "concat" => string_binary(left, right, |left, right| format!("{left}{right}"), true),
        other => Err(ArrowError::NotYetImplemented(format!(
            "unsupported binary operator: {other}"
        ))),
    }
}

fn evaluate_list_binary(
    operator: &str,
    left: ArrayRef,
    right: ArrayRef,
) -> Result<ArrayRef, ArrowError> {
    let operands = [&left, &right];
    let template = list_template(&operands)?;
    let left_values = list_operand_values(&left, template, "left")?;
    let right_values = list_operand_values(&right, template, "right")?;
    let values = evaluate_binary(operator, left_values, right_values)?;
    rebuild_list(template, values)
}

fn evaluate_case_branch(
    condition: ArrayRef,
    truthy: ArrayRef,
    falsy: ArrayRef,
) -> Result<ArrayRef, ArrowError> {
    if !matches!(condition.data_type(), DataType::List(_))
        && !matches!(truthy.data_type(), DataType::List(_))
        && !matches!(falsy.data_type(), DataType::List(_))
    {
        let (truthy, falsy) = coerce_case_pair(truthy, falsy)?;
        return zip(
            boolean_array(&condition, "case condition")?,
            &truthy.as_ref(),
            &falsy.as_ref(),
        );
    }

    let operands = [&condition, &truthy, &falsy];
    let template = list_template(&operands)?;
    let condition_values = list_operand_values(&condition, template, "case condition")?;
    let truthy_values = list_operand_values(&truthy, template, "case truthy")?;
    let falsy_values = list_operand_values(&falsy, template, "case falsy")?;
    let (truthy_values, falsy_values) = coerce_case_pair(truthy_values, falsy_values)?;
    let values = zip(
        boolean_array(&condition_values, "case condition")?,
        &truthy_values.as_ref(),
        &falsy_values.as_ref(),
    )?;
    rebuild_list(template, values)
}

fn coerce_case_pair(truthy: ArrayRef, falsy: ArrayRef) -> Result<(ArrayRef, ArrayRef), ArrowError> {
    if truthy.data_type() == falsy.data_type() {
        return Ok((truthy, falsy));
    }
    if matches!(truthy.data_type(), DataType::Null) {
        return Ok((cast(truthy.as_ref(), falsy.data_type())?, falsy));
    }
    if matches!(falsy.data_type(), DataType::Null) {
        return Ok((truthy.clone(), cast(falsy.as_ref(), truthy.data_type())?));
    }
    if is_numeric_type(truthy.data_type()) && is_numeric_type(falsy.data_type()) {
        return promote_numeric_pair(truthy, falsy);
    }
    Err(ArrowError::InvalidArgumentError(format!(
        "case branches require a common data type, got {} and {}",
        truthy.data_type(),
        falsy.data_type()
    )))
}

fn list_template<'a>(operands: &'a [&'a ArrayRef]) -> Result<&'a ListArray, ArrowError> {
    let template = operands
        .iter()
        .find_map(|array| array.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError("list-native expression has no List operand".into())
        })?;
    for array in operands {
        if let Some(other) = array.as_any().downcast_ref::<ListArray>() {
            ensure_same_list_shape(template, other)?;
        }
    }
    Ok(template)
}

fn ensure_same_list_shape(left: &ListArray, right: &ListArray) -> Result<(), ArrowError> {
    if left.len() != right.len()
        || left.value_offsets() != right.value_offsets()
        || (0..left.len()).any(|index| left.is_null(index) != right.is_null(index))
    {
        return Err(ArrowError::InvalidArgumentError(
            "list.shape_mismatch: List operands require identical row offsets and null shape"
                .into(),
        ));
    }
    Ok(())
}

fn list_operand_values(
    operand: &ArrayRef,
    template: &ListArray,
    role: &str,
) -> Result<ArrayRef, ArrowError> {
    if let Some(list) = operand.as_any().downcast_ref::<ListArray>() {
        ensure_same_list_shape(template, list)?;
        return Ok(list.values().clone());
    }
    if operand.len() != template.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "list-native {role} scalar length mismatch: expected={}, actual={}",
            template.len(),
            operand.len()
        )));
    }
    let mut parent_indices = Vec::with_capacity(template.values().len());
    for (parent_index, offsets) in template.value_offsets().windows(2).enumerate() {
        let element_count = usize::try_from(offsets[1] - offsets[0])
            .map_err(|_| ArrowError::InvalidArgumentError("negative List offset length".into()))?;
        let parent_index = u32::try_from(parent_index).map_err(|_| {
            ArrowError::InvalidArgumentError("List parent index exceeds UInt32".into())
        })?;
        parent_indices.extend(std::iter::repeat_n(parent_index, element_count));
    }
    take(operand.as_ref(), &UInt32Array::from(parent_indices), None)
}

fn rebuild_list(template: &ListArray, values: ArrayRef) -> Result<ArrayRef, ArrowError> {
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    Ok(Arc::new(ListArray::try_new(
        field,
        template.offsets().clone(),
        values,
        template.nulls().cloned(),
    )?))
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
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

fn promote_numeric_pair(
    left: ArrayRef,
    right: ArrayRef,
) -> Result<(ArrayRef, ArrayRef), ArrowError> {
    if !is_numeric_type(left.data_type()) || !is_numeric_type(right.data_type()) {
        return Err(ArrowError::InvalidArgumentError(format!(
            "numeric operation requires numeric operands, got {} and {}",
            left.data_type(),
            right.data_type()
        )));
    }
    let target = if matches!(left.data_type(), DataType::Float32 | DataType::Float64)
        || matches!(right.data_type(), DataType::Float32 | DataType::Float64)
    {
        DataType::Float64
    } else {
        DataType::Int64
    };
    Ok((
        cast(left.as_ref(), &target)?,
        cast(right.as_ref(), &target)?,
    ))
}

fn evaluate_call(
    function: &str,
    arguments: &[ExpressionNode],
    batch: &RecordBatch,
) -> Result<ArrayRef, ArrowError> {
    let values = arguments
        .iter()
        .map(|argument| evaluate_node(argument, batch))
        .collect::<Result<Vec<_>, _>>()?;
    if function == "concatenate" {
        return concatenate_many(&values, batch.num_rows());
    }
    match (function, values.as_slice()) {
        ("isnull", [value]) => Ok(Arc::new(BooleanArray::from_iter(
            (0..value.len()).map(|index| Some(value.is_null(index))),
        ))),
        ("if", [condition, truthy, falsy]) => {
            let condition = boolean_array(condition, "if condition")?;
            zip(condition, &truthy.as_ref(), &falsy.as_ref())
        }
        ("sn" | "coalesce", [left, right]) => {
            let valid =
                BooleanArray::from_iter((0..left.len()).map(|index| Some(left.is_valid(index))));
            zip(&valid, &left.as_ref(), &right.as_ref())
        }
        ("len", [value]) => {
            let values = string_values(value, "len")?;
            Ok(Arc::new(Int64Array::from_iter(values.map(|value| {
                value.map(|item| item.chars().count() as i64)
            }))))
        }
        ("right", [value, count]) => {
            let values = string_values(value, "right")?;
            let counts = int64_values(count, "right count")?;
            Ok(Arc::new(StringArray::from_iter(values.zip(counts).map(
                |(value, count)| match (value, count) {
                    (Some(value), Some(count)) if count >= 0 => {
                        let chars: Vec<char> = value.chars().collect();
                        let start = chars.len().saturating_sub(count as usize);
                        Some(chars[start..].iter().collect::<String>())
                    }
                    _ => None,
                },
            ))))
        }
        ("mid", [value, start, count]) => {
            let values = string_values(value, "mid")?;
            let starts = int64_values(start, "mid start")?;
            let counts = int64_values(count, "mid count")?;
            Ok(Arc::new(StringArray::from_iter(
                values
                    .zip(starts)
                    .zip(counts)
                    .map(|((value, start), count)| match (value, start, count) {
                        (Some(value), Some(start), Some(count)) if start >= 1 && count >= 0 => {
                            Some(
                                value
                                    .chars()
                                    .skip((start - 1) as usize)
                                    .take(count as usize)
                                    .collect::<String>(),
                            )
                        }
                        _ => None,
                    }),
            )))
        }
        ("substitute", [value, old, new]) => {
            let values = string_values(value, "substitute")?;
            let old = string_values(old, "substitute old")?;
            let new = string_values(new, "substitute new")?;
            Ok(Arc::new(StringArray::from_iter(
                values
                    .zip(old)
                    .zip(new)
                    .map(|((value, old), new)| Some(value?.replace(old?, new?))),
            )))
        }
        ("find" | "charindex", [needle, haystack]) => find_strings(needle, haystack, None),
        ("find" | "charindex", [needle, haystack, start]) => {
            find_strings(needle, haystack, Some(start))
        }
        ("split", [value, separator]) => split_strings(value, separator),
        ("split", [value, separator, index]) => split_string_at(value, separator, index),
        ("rxextract", [value, pattern]) => regex_extract(value, pattern, None),
        ("rxextract", [value, pattern, group]) => regex_extract(value, pattern, Some(group)),
        ("rxreplace", [value, pattern, replacement]) => regex_replace(value, pattern, replacement),
        (
            "abs" | "acos" | "asin" | "atan" | "ceiling" | "cos" | "exp" | "floor" | "ln" | "log"
            | "log10" | "sin" | "sqrt" | "tan",
            [value],
        ) => numeric_unary(function, value),
        ("atan2" | "power" | "mod", [left, right]) => numeric_binary_call(function, left, right),
        ("round", [value]) => round_numeric(value, None),
        ("round", [value, digits]) => round_numeric(value, Some(digits)),
        ("pi", []) => Ok(Arc::new(Float64Array::from_value(
            std::f64::consts::PI,
            batch.num_rows(),
        ))),
        ("lower" | "upper" | "trim", [value]) => string_unary(function, value),
        ("left", [value, count]) => left_strings(value, count),
        ("substring", [value, start]) => substring_strings(value, start, None),
        ("substring", [value, start, count]) => substring_strings(value, start, Some(count)),
        ("parsereal", [value]) => parse_real(value),
        ("base64encode" | "base64decode", [value]) => base64_strings(function, value),
        (
            "day" | "dayofmonth" | "dayofweek" | "dayofyear" | "hour" | "millisecond" | "minute"
            | "month" | "quarter" | "second" | "week" | "year",
            [value],
        ) => date_part(function, value),
        ("rand", [seed]) => seeded_rand(seed),
        ("randbetween", [low, high, seed]) => seeded_rand_between(low, high, seed),
        ("percent", [numerator, denominator]) => percent_ratio(numerator, denominator),
        ("timespan", [days, hours, minutes, seconds, milliseconds]) => {
            time_span(days, hours, minutes, seconds, milliseconds)
        }
        ("fiscalmonth" | "fiscalquarter" | "fiscalyear", [value]) => {
            fiscal_part(function, value, None)
        }
        ("fiscalmonth" | "fiscalquarter" | "fiscalyear", [value, offset]) => {
            fiscal_part(function, value, Some(offset))
        }
        ("parsedate", [value]) => parse_date(value, "%Y-%m-%d"),
        ("parsedate", [value, format]) => {
            parse_date(value, scalar_string(format, "ParseDate format")?)
        }
        ("parsedatetime", [value]) => parse_datetime(value, "%Y-%m-%d %H:%M:%S"),
        ("parsedatetime", [value, format]) => {
            parse_datetime(value, scalar_string(format, "ParseDateTime format")?)
        }
        ("parsetime", [value]) => parse_time(value, "%H:%M:%S"),
        ("parsetime", [value, format]) => {
            parse_time(value, scalar_string(format, "ParseTime format")?)
        }
        ("datepart", [unit, value]) => date_part(scalar_string(unit, "DatePart unit")?, value),
        ("dateadd", [unit, amount, value]) => {
            date_add(scalar_string(unit, "DateAdd unit")?, amount, value)
        }
        ("datediff", [unit, start, end]) => {
            date_diff(scalar_string(unit, "DateDiff unit")?, start, end)
        }
        ("isoweek", [value]) => date_part("isoweek", value),
        ("isoyear", [value]) => date_part("isoyear", value),
        ("yearandweek", [value]) => year_and_week(value),
        ("toepochseconds", [value]) => timestamp_epoch(value, 1_000_000),
        ("toepochmilliseconds", [value]) => timestamp_epoch(value, 1_000),
        ("fromepochseconds", [value]) => epoch_timestamp(value, 1_000_000),
        ("fromepochmilliseconds", [value]) => epoch_timestamp(value, 1_000),
        ("days", [value]) => number_to_duration(value, 86_400_000_000),
        ("hours", [value]) => number_to_duration(value, 3_600_000_000),
        ("minutes", [value]) => number_to_duration(value, 60_000_000),
        ("seconds", [value]) => number_to_duration(value, 1_000_000),
        ("milliseconds", [value]) => number_to_duration(value, 1_000),
        ("totaldays", [value]) => duration_total(value, 86_400_000_000),
        ("totalhours", [value]) => duration_total(value, 3_600_000_000),
        ("totalminutes", [value]) => duration_total(value, 60_000_000),
        ("totalseconds", [value]) => duration_total(value, 1_000_000),
        ("totalmilliseconds", [value]) => duration_total(value, 1_000),
        _ => Err(ArrowError::NotYetImplemented(format!(
            "unsupported scalar function or arity: {function}/{}",
            values.len()
        ))),
    }
}

fn evaluate_window(
    expression: &ExpressionNode,
    partition_by: &[ExpressionNode],
    order_by: &[WindowOrder],
    frame: Option<&serde_json::Value>,
    batch: &RecordBatch,
) -> Result<ArrayRef, ArrowError> {
    let ExpressionNode::Call {
        function,
        arguments,
    } = expression
    else {
        return Err(ArrowError::NotYetImplemented(
            "window expression must contain a canonical call node".to_string(),
        ));
    };
    let values = arguments
        .iter()
        .map(|argument| evaluate_node(argument, batch))
        .collect::<Result<Vec<_>, _>>()?;
    let partitions = partition_by
        .iter()
        .map(|partition| evaluate_node(partition, batch))
        .collect::<Result<Vec<_>, _>>()?;
    let groups = window_groups(&partitions, batch.num_rows())?;
    let ordered_groups = if matches!(function.as_str(), "lag" | "lead") && !order_by.is_empty() {
        order_window_groups(&groups, order_by, batch)?
    } else {
        groups.clone()
    };

    match function.as_str() {
        "rownumber" => window_row_number(order_by, &groups, batch),
        "count" | "uniquecount" => {
            let value = values.first().ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!("{function} requires one argument"))
            })?;
            let mut output = vec![None; batch.num_rows()];
            for indices in groups.values() {
                let count = if function == "count" {
                    indices
                        .iter()
                        .filter(|&&index| value.is_valid(index))
                        .count() as i64
                } else {
                    let mut unique = HashSet::new();
                    for &index in indices {
                        if value.is_valid(index) {
                            unique.insert(array_value_to_string(value.as_ref(), index)?);
                        }
                    }
                    unique.len() as i64
                };
                for &index in indices {
                    output[index] = Some(count);
                }
            }
            Ok(Arc::new(Int64Array::from(output)))
        }
        "valueformax" | "valueformin" | "lastvalueformax" | "lastvalueformin" => {
            window_value_for(function, &values, &groups)
        }
        "denserank" | "rankreal" => window_rank(function, &values, &groups),
        "firstvalidafter" | "lastvalidbefore" => window_fill(function, &values, &groups),
        "first" | "last" => window_first_last(function, &values, &groups),
        "lag" | "lead" => window_lag_lead(function, &values, &ordered_groups),
        "rollingavg" | "rollingmean" | "rollingsum" | "rollingmin" | "rollingmax" => {
            window_rolling_numeric(function, &values, &ordered_groups, frame)
        }
        "mostcommon" => window_most_common(&values, &groups),
        "uniqueconcatenate" => window_unique_concatenate(&values, &groups),
        _ => window_numeric_stat(function, &values, &groups, batch.num_rows()),
    }
}

fn window_rolling_numeric(
    function: &str,
    arguments: &[ArrayRef],
    groups: &HashMap<String, Vec<usize>>,
    frame: Option<&serde_json::Value>,
) -> Result<ArrayRef, ArrowError> {
    let value = arguments.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("{function} requires one value"))
    })?;
    let frame = frame
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError("rolling window frame is required".to_string())
        })?;
    let preceding = frame
        .get("preceding")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError("rolling frame preceding is required".to_string())
        })? as usize;
    let following = frame
        .get("following")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as usize;
    let minimum = frame
        .get("minimum_periods")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1) as usize;
    let numeric = cast(value.as_ref(), &DataType::Float64)?;
    let numeric = numeric
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError("rolling value must be numeric".to_string())
        })?;
    let mut output = vec![None; value.len()];
    for indices in groups.values() {
        for position in 0..indices.len() {
            let start = position.saturating_sub(preceding);
            let end = (position + following + 1).min(indices.len());
            let values: Vec<f64> = indices[start..end]
                .iter()
                .filter_map(|index| numeric.is_valid(*index).then(|| numeric.value(*index)))
                .collect();
            if values.len() < minimum {
                continue;
            }
            let result = match function {
                "rollingavg" | "rollingmean" => values.iter().sum::<f64>() / values.len() as f64,
                "rollingsum" => values.iter().sum(),
                "rollingmin" => values.iter().copied().fold(f64::INFINITY, f64::min),
                "rollingmax" => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                _ => unreachable!(),
            };
            output[indices[position]] = Some(result);
        }
    }
    Ok(Arc::new(Float64Array::from(output)))
}

fn window_groups(
    partitions: &[ArrayRef],
    row_count: usize,
) -> Result<HashMap<String, Vec<usize>>, ArrowError> {
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for row_index in 0..row_count {
        let key = if partitions.is_empty() {
            "__global__".to_string()
        } else {
            partitions
                .iter()
                .map(|array| {
                    if array.is_null(row_index) {
                        Ok("<NULL>".to_string())
                    } else {
                        array_value_to_string(array.as_ref(), row_index)
                    }
                })
                .collect::<Result<Vec<_>, ArrowError>>()?
                .join("\u{1f}")
        };
        groups.entry(key).or_default().push(row_index);
    }
    Ok(groups)
}

fn order_window_groups(
    groups: &HashMap<String, Vec<usize>>,
    order_by: &[WindowOrder],
    batch: &RecordBatch,
) -> Result<HashMap<String, Vec<usize>>, ArrowError> {
    let order_arrays = order_by
        .iter()
        .map(|order| evaluate_node(&order.expression, batch))
        .collect::<Result<Vec<_>, _>>()?;
    let source_indexes =
        Arc::new(UInt32Array::from_iter_values(0..batch.num_rows() as u32)) as ArrayRef;
    let mut ordered = HashMap::with_capacity(groups.len());
    for (key, indices) in groups {
        let take_indexes = UInt32Array::from(
            indices
                .iter()
                .map(|index| u32::try_from(*index).map_err(|error| error.to_string()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ArrowError::InvalidArgumentError)?,
        );
        let mut columns = order_arrays
            .iter()
            .zip(order_by)
            .map(|(array, order)| {
                Ok(SortColumn {
                    values: take(array.as_ref(), &take_indexes, None)?,
                    options: Some(SortOptions {
                        descending: order.direction == "desc",
                        nulls_first: order.nulls == "first",
                    }),
                })
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;
        columns.push(SortColumn {
            values: take(source_indexes.as_ref(), &take_indexes, None)?,
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        });
        let local = lexsort_to_indices(&columns, None)?;
        ordered.insert(
            key.clone(),
            local
                .values()
                .iter()
                .map(|index| indices[*index as usize])
                .collect(),
        );
    }
    Ok(ordered)
}

fn window_numeric_stat(
    function: &str,
    arguments: &[ArrayRef],
    groups: &HashMap<String, Vec<usize>>,
    row_count: usize,
) -> Result<ArrayRef, ArrowError> {
    let first = arguments.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("{function} requires at least one argument"))
    })?;
    let first = cast(first.as_ref(), &DataType::Float64)?;
    let first = first
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 cast");
    let second = if arguments.len() > 1 && matches!(function, "covariance" | "weightedaverage") {
        let casted = cast(arguments[1].as_ref(), &DataType::Float64)?;
        Some(casted)
    } else {
        None
    };
    let mut output = vec![None; row_count];
    for indices in groups.values() {
        let values: Vec<f64> = indices
            .iter()
            .filter(|&&index| first.is_valid(index))
            .map(|&index| first.value(index))
            .collect();
        let statistic = match function {
            "sum" => (!values.is_empty()).then(|| values.iter().sum()),
            "product" => (!values.is_empty()).then(|| values.iter().product()),
            "avg" | "average" => mean(&values),
            "min" => values.iter().copied().reduce(f64::min),
            "max" => values.iter().copied().reduce(f64::max),
            "median" => quantile(&values, 0.5),
            "percentile" => quantile(
                &values,
                scalar_number(arguments.get(1), "Percentile")? / 100.0,
            ),
            "p10" => quantile(&values, 0.10),
            "p90" => quantile(&values, 0.90),
            "q1" => quantile(&values, 0.25),
            "q3" => quantile(&values, 0.75),
            "iqr" => match (quantile(&values, 0.25), quantile(&values, 0.75)) {
                (Some(q1), Some(q3)) => Some(q3 - q1),
                _ => None,
            },
            "var" | "variance" => sample_variance(&values),
            "stddev" => sample_variance(&values).map(f64::sqrt),
            "range" => match (
                values.iter().copied().reduce(f64::min),
                values.iter().copied().reduce(f64::max),
            ) {
                (Some(min), Some(max)) => Some(max - min),
                _ => None,
            },
            "geometricmean" => (!values.is_empty() && values.iter().all(|value| *value > 0.0))
                .then(|| {
                    (values.iter().map(|value| value.ln()).sum::<f64>() / values.len() as f64).exp()
                }),
            "stderr" => sample_variance(&values)
                .map(|variance| variance.sqrt() / (values.len() as f64).sqrt()),
            "l95" | "u95" => match (mean(&values), sample_variance(&values)) {
                (Some(average), Some(variance)) => {
                    let margin = 1.96 * variance.sqrt() / (values.len() as f64).sqrt();
                    Some(if function == "l95" {
                        average - margin
                    } else {
                        average + margin
                    })
                }
                _ => None,
            },
            "lif" | "uif" | "lof" | "uof" => fence_stat(function, &values),
            "meandeviation" => mean(&values).map(|avg| {
                values.iter().map(|value| (value - avg).abs()).sum::<f64>() / values.len() as f64
            }),
            "medianabsolutedeviation" => quantile(&values, 0.5).and_then(|median| {
                quantile(
                    &values
                        .iter()
                        .map(|value| (value - median).abs())
                        .collect::<Vec<_>>(),
                    0.5,
                )
            }),
            "trimmedmean" => trimmed_mean(&values, scalar_number(arguments.get(1), "TrimmedMean")?),
            "nthlargest" => nth_value(&values, scalar_usize(arguments.get(1), "NthLargest")?, true),
            "nthsmallest" => nth_value(
                &values,
                scalar_usize(arguments.get(1), "NthSmallest")?,
                false,
            ),
            "covariance" => covariance(
                first,
                second
                    .as_ref()
                    .expect("second")
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("Float64"),
                indices,
            ),
            "weightedaverage" => weighted_average(
                first,
                second
                    .as_ref()
                    .expect("second")
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("Float64"),
                indices,
            ),
            "lav" | "uav" | "outliers" | "pctoutliers" => outlier_stat(function, &values),
            other => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "unsupported window function: {other}"
                )))
            }
        };
        for &index in indices {
            output[index] = statistic;
        }
    }
    Ok(Arc::new(Float64Array::from(output)))
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn quantile(values: &[f64], probability: f64) -> Option<f64> {
    if values.is_empty() || !(0.0..=1.0).contains(&probability) {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let position = probability * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    Some(sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower as f64))
}

fn sample_variance(values: &[f64]) -> Option<f64> {
    let average = mean(values)?;
    (values.len() > 1).then(|| {
        values
            .iter()
            .map(|value| (value - average).powi(2))
            .sum::<f64>()
            / (values.len() - 1) as f64
    })
}

fn covariance(left: &Float64Array, right: &Float64Array, indices: &[usize]) -> Option<f64> {
    let pairs: Vec<(f64, f64)> = indices
        .iter()
        .filter(|&&index| left.is_valid(index) && right.is_valid(index))
        .map(|&index| (left.value(index), right.value(index)))
        .collect();
    if pairs.len() < 2 {
        return None;
    }
    let left_mean = pairs.iter().map(|item| item.0).sum::<f64>() / pairs.len() as f64;
    let right_mean = pairs.iter().map(|item| item.1).sum::<f64>() / pairs.len() as f64;
    Some(
        pairs
            .iter()
            .map(|item| (item.0 - left_mean) * (item.1 - right_mean))
            .sum::<f64>()
            / (pairs.len() - 1) as f64,
    )
}

fn weighted_average(
    values: &Float64Array,
    weights: &Float64Array,
    indices: &[usize],
) -> Option<f64> {
    let mut numerator = 0.0;
    let mut denominator = 0.0;
    for &index in indices {
        if values.is_valid(index) && weights.is_valid(index) {
            numerator += values.value(index) * weights.value(index);
            denominator += weights.value(index);
        }
    }
    (denominator != 0.0).then_some(numerator / denominator)
}

fn trimmed_mean(values: &[f64], percent: f64) -> Option<f64> {
    if values.is_empty() || !(0.0..100.0).contains(&percent) {
        return None;
    }
    let tail = percent / 200.0;
    let lower = quantile(values, tail)?;
    let upper = quantile(values, 1.0 - tail)?;
    mean(
        &values
            .iter()
            .copied()
            .filter(|value| *value >= lower && *value <= upper)
            .collect::<Vec<_>>(),
    )
}

fn nth_value(values: &[f64], n: usize, descending: bool) -> Option<f64> {
    if n == 0 || n > values.len() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    if descending {
        sorted.reverse();
    }
    Some(sorted[n - 1])
}

fn outlier_stat(function: &str, values: &[f64]) -> Option<f64> {
    let q1 = quantile(values, 0.25)?;
    let q3 = quantile(values, 0.75)?;
    let lower = q1 - 1.5 * (q3 - q1);
    let upper = q3 + 1.5 * (q3 - q1);
    let inliers: Vec<f64> = values
        .iter()
        .copied()
        .filter(|value| *value >= lower && *value <= upper)
        .collect();
    let outliers = values
        .iter()
        .filter(|value| **value < lower || **value > upper)
        .count();
    match function {
        "lav" => inliers.iter().copied().reduce(f64::min),
        "uav" => inliers.iter().copied().reduce(f64::max),
        "outliers" => Some(outliers as f64),
        "pctoutliers" => (!values.is_empty()).then(|| outliers as f64 / values.len() as f64),
        _ => None,
    }
}

fn fence_stat(function: &str, values: &[f64]) -> Option<f64> {
    let q1 = quantile(values, 0.25)?;
    let q3 = quantile(values, 0.75)?;
    let multiplier = if matches!(function, "lof" | "uof") {
        3.0
    } else {
        1.5
    };
    Some(if matches!(function, "lif" | "lof") {
        q1 - multiplier * (q3 - q1)
    } else {
        q3 + multiplier * (q3 - q1)
    })
}

fn window_most_common(
    arguments: &[ArrayRef],
    groups: &HashMap<String, Vec<usize>>,
) -> Result<ArrayRef, ArrowError> {
    let value = arguments.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError("MostCommon requires one value".to_string())
    })?;
    let mut selected = vec![None; value.len()];
    for indices in groups.values() {
        let mut counts: HashMap<String, (usize, usize)> = HashMap::new();
        for &index in indices {
            if value.is_valid(index) {
                let key = array_value_to_string(value.as_ref(), index)?;
                let entry = counts.entry(key).or_insert((0, index));
                entry.0 += 1;
            }
        }
        let candidate = counts
            .values()
            .max_by_key(|(count, first_index)| (*count, std::cmp::Reverse(*first_index)))
            .map(|item| item.1 as u32);
        for &index in indices {
            selected[index] = candidate;
        }
    }
    take(value.as_ref(), &UInt32Array::from(selected), None)
}

fn window_unique_concatenate(
    arguments: &[ArrayRef],
    groups: &HashMap<String, Vec<usize>>,
) -> Result<ArrayRef, ArrowError> {
    let value = arguments.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError("UniqueConcatenate requires one value".to_string())
    })?;
    let mut output = vec![None; value.len()];
    for indices in groups.values() {
        let mut seen = HashSet::new();
        let mut items = Vec::new();
        for &index in indices {
            if value.is_valid(index) {
                let item = array_value_to_string(value.as_ref(), index)?;
                if seen.insert(item.clone()) {
                    items.push(item);
                }
            }
        }
        let joined = (!items.is_empty()).then(|| items.join(","));
        for &index in indices {
            output[index] = joined.clone();
        }
    }
    Ok(Arc::new(StringArray::from(output)))
}

fn scalar_number(argument: Option<&ArrayRef>, context: &str) -> Result<f64, ArrowError> {
    let value = argument.ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("{context} requires a numeric literal"))
    })?;
    let casted = cast(value.as_ref(), &DataType::Float64)?;
    let values = casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    if values.is_null(0) {
        Err(ArrowError::InvalidArgumentError(format!(
            "{context} literal is null"
        )))
    } else {
        Ok(values.value(0))
    }
}

fn scalar_usize(argument: Option<&ArrayRef>, context: &str) -> Result<usize, ArrowError> {
    let value = scalar_number(argument, context)?;
    if value < 1.0 || value.fract() != 0.0 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "{context} requires a positive integer"
        )));
    }
    Ok(value as usize)
}

fn window_value_for(
    function: &str,
    arguments: &[ArrayRef],
    groups: &HashMap<String, Vec<usize>>,
) -> Result<ArrayRef, ArrowError> {
    if arguments.len() != 2 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "{function} requires value and order arguments"
        )));
    }
    let order = cast(arguments[1].as_ref(), &DataType::Float64)?;
    let order = order
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    let mut selected = vec![None; arguments[0].len()];
    for indices in groups.values() {
        let mut candidate: Option<usize> = None;
        for &index in indices {
            if !order.is_valid(index) {
                continue;
            }
            candidate = match candidate {
                None => Some(index),
                Some(current)
                    if matches!(function, "valueformax" | "lastvalueformax")
                        && order.value(index) >= order.value(current) =>
                {
                    Some(index)
                }
                Some(current)
                    if matches!(function, "valueformin" | "lastvalueformin")
                        && order.value(index) < order.value(current) =>
                {
                    Some(index)
                }
                other => other,
            };
        }
        for &index in indices {
            selected[index] = candidate.map(|item| item as u32);
        }
    }
    take(arguments[0].as_ref(), &UInt32Array::from(selected), None)
}

fn window_rank(
    function: &str,
    arguments: &[ArrayRef],
    groups: &HashMap<String, Vec<usize>>,
) -> Result<ArrayRef, ArrowError> {
    let values = cast(
        arguments
            .first()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!("{function} requires a value"))
            })?
            .as_ref(),
        &DataType::Float64,
    )?;
    let values = values
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    let descending = arguments
        .get(1)
        .map(|value| {
            scalar_string(value, "rank direction").map(|item| {
                item.eq_ignore_ascii_case("desc") || item.eq_ignore_ascii_case("descending")
            })
        })
        .transpose()?
        .unwrap_or(false);
    if function == "rankreal" {
        if let Some(ties) = arguments.get(2) {
            let ties = scalar_string(ties, "RankReal ties method")?;
            if !ties.eq_ignore_ascii_case("ties.method=average") {
                return Err(ArrowError::NotYetImplemented(format!(
                    "RankReal only supports ties.method=average, got {ties:?}"
                )));
            }
        }
    }
    let mut output = vec![None; values.len()];
    for indices in groups.values() {
        let mut ordered: Vec<(usize, f64)> = indices
            .iter()
            .filter(|&&index| values.is_valid(index))
            .map(|&index| (index, values.value(index)))
            .collect();
        ordered.sort_by(|left, right| {
            if descending {
                right.1.total_cmp(&left.1)
            } else {
                left.1.total_cmp(&right.1)
            }
        });
        if function == "denserank" {
            let mut rank = 0.0;
            let mut previous: Option<f64> = None;
            for (index, value) in ordered {
                if previous.is_none_or(|item| item.total_cmp(&value).is_ne()) {
                    rank += 1.0;
                    previous = Some(value);
                }
                output[index] = Some(rank);
            }
        } else {
            let mut position = 0;
            while position < ordered.len() {
                let mut end = position + 1;
                while end < ordered.len() && ordered[end].1.total_cmp(&ordered[position].1).is_eq()
                {
                    end += 1;
                }
                let average_rank = ((position + 1 + end) as f64) / 2.0;
                for item in &ordered[position..end] {
                    output[item.0] = Some(average_rank);
                }
                position = end;
            }
        }
    }
    Ok(Arc::new(Float64Array::from(output)))
}

fn window_row_number(
    order_by: &[WindowOrder],
    groups: &HashMap<String, Vec<usize>>,
    batch: &RecordBatch,
) -> Result<ArrayRef, ArrowError> {
    if order_by.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "RowNumber requires at least one order_by item".to_string(),
        ));
    }
    let order_arrays = order_by
        .iter()
        .map(|order| evaluate_node(&order.expression, batch))
        .collect::<Result<Vec<_>, _>>()?;
    let source_indexes = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
    let source_indexes: ArrayRef = Arc::new(source_indexes);
    let mut output = vec![None; batch.num_rows()];
    for indices in groups.values() {
        let take_indexes = UInt32Array::from(
            indices
                .iter()
                .map(|index| u32::try_from(*index).map_err(|error| error.to_string()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ArrowError::InvalidArgumentError)?,
        );
        let mut columns = order_arrays
            .iter()
            .zip(order_by)
            .map(|(array, order)| {
                Ok(SortColumn {
                    values: take(array.as_ref(), &take_indexes, None)?,
                    options: Some(SortOptions {
                        descending: order.direction == "desc",
                        nulls_first: order.nulls == "first",
                    }),
                })
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;
        // The source row index is a deterministic final tie-break. Physical lowering
        // replaces this with source path + row index for cross-file selection.
        columns.push(SortColumn {
            values: take(source_indexes.as_ref(), &take_indexes, None)?,
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        });
        let ordered = lexsort_to_indices(&columns, None)?;
        for (position, local_index) in ordered.values().iter().enumerate() {
            output[indices[*local_index as usize]] = Some((position + 1) as i64);
        }
    }
    Ok(Arc::new(Int64Array::from(output)))
}

fn window_fill(
    function: &str,
    arguments: &[ArrayRef],
    groups: &HashMap<String, Vec<usize>>,
) -> Result<ArrayRef, ArrowError> {
    let value = arguments.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("{function} requires one value"))
    })?;
    let mut selected = vec![None; value.len()];
    for indices in groups.values() {
        let iterator: Box<dyn Iterator<Item = &usize>> = if function == "firstvalidafter" {
            Box::new(indices.iter().rev())
        } else {
            Box::new(indices.iter())
        };
        let mut valid: Option<u32> = None;
        for &index in iterator {
            if value.is_valid(index) {
                valid = Some(index as u32);
            }
            selected[index] = valid;
        }
    }
    take(value.as_ref(), &UInt32Array::from(selected), None)
}

fn window_first_last(
    function: &str,
    arguments: &[ArrayRef],
    groups: &HashMap<String, Vec<usize>>,
) -> Result<ArrayRef, ArrowError> {
    let value = arguments.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("{function} requires one value"))
    })?;
    let mut selected = vec![None; value.len()];
    for indices in groups.values() {
        let candidate = if function == "first" {
            indices.first()
        } else {
            indices.last()
        }
        .map(|index| *index as u32);
        for &index in indices {
            selected[index] = candidate;
        }
    }
    take(value.as_ref(), &UInt32Array::from(selected), None)
}

fn window_lag_lead(
    function: &str,
    arguments: &[ArrayRef],
    groups: &HashMap<String, Vec<usize>>,
) -> Result<ArrayRef, ArrowError> {
    let value = arguments.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("{function} requires one value"))
    })?;
    let offset = match arguments.get(1) {
        Some(argument) => scalar_usize(Some(argument), function)?,
        None => 1,
    };
    let mut selected = vec![None; value.len()];
    for indices in groups.values() {
        for (position, &index) in indices.iter().enumerate() {
            let source_position = if function == "lag" {
                position.checked_sub(offset)
            } else {
                position
                    .checked_add(offset)
                    .filter(|item| *item < indices.len())
            };
            selected[index] = source_position.map(|item| indices[item] as u32);
        }
    }
    take(value.as_ref(), &UInt32Array::from(selected), None)
}

fn numeric_unary(function: &str, value: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let casted = cast(value.as_ref(), &DataType::Float64)?;
    let values = casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    Ok(Arc::new(Float64Array::from_iter(values.iter().map(
        |item| {
            item.map(|value| match function {
                "abs" => value.abs(),
                "acos" => value.acos(),
                "asin" => value.asin(),
                "atan" => value.atan(),
                "ceiling" => value.ceil(),
                "cos" => value.cos(),
                "exp" => value.exp(),
                "floor" => value.floor(),
                "ln" | "log" => value.ln(),
                "log10" => value.log10(),
                "sin" => value.sin(),
                "sqrt" => value.sqrt(),
                "tan" => value.tan(),
                _ => unreachable!("validated numeric unary"),
            })
        },
    ))))
}

fn numeric_binary_call(
    function: &str,
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<ArrayRef, ArrowError> {
    let left = cast(left.as_ref(), &DataType::Float64)?;
    let right = cast(right.as_ref(), &DataType::Float64)?;
    let left = left
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    let right = right
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    Ok(Arc::new(Float64Array::from_iter(
        left.iter().zip(right.iter()).map(|(left, right)| {
            Some(match function {
                "atan2" => left?.atan2(right?),
                "power" => left?.powf(right?),
                "mod" => left? % right?,
                _ => unreachable!("validated numeric binary"),
            })
        }),
    )))
}

fn round_numeric(value: &ArrayRef, digits: Option<&ArrayRef>) -> Result<ArrayRef, ArrowError> {
    let values = cast(value.as_ref(), &DataType::Float64)?;
    let values = values
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    let digits = digits
        .map(|array| int64_values(array, "Round digits"))
        .transpose()?
        .unwrap_or_else(|| Box::new(std::iter::repeat_n(Some(0), value.len())));
    Ok(Arc::new(Float64Array::from_iter(
        values.iter().zip(digits).map(|(value, digits)| {
            let factor = 10_f64.powi(i32::try_from(digits?).ok()?);
            Some((value? * factor).round() / factor)
        }),
    )))
}

fn string_unary(function: &str, value: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(StringArray::from_iter(
        string_values(value, function)?.map(|item| {
            item.map(|value| match function {
                "lower" => value.to_lowercase(),
                "upper" => value.to_uppercase(),
                "trim" => value.trim().to_string(),
                _ => unreachable!("validated string unary"),
            })
        }),
    )))
}

fn concatenate_many(values: &[ArrayRef], row_count: usize) -> Result<ArrayRef, ArrowError> {
    if values.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Concatenate requires at least one argument".to_string(),
        ));
    }
    let iterators = values
        .iter()
        .map(|value| string_values(value, "Concatenate"))
        .collect::<Result<Vec<_>, _>>()?;
    let columns = iterators
        .into_iter()
        .map(|iterator| iterator.collect::<Vec<_>>())
        .collect::<Vec<_>>();
    Ok(Arc::new(StringArray::from_iter((0..row_count).map(
        |row_index| {
            let mut output = String::new();
            for column in &columns {
                output.push_str(column[row_index]?);
            }
            Some(output)
        },
    ))))
}

fn left_strings(value: &ArrayRef, count: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let values = string_values(value, "Left")?;
    let counts = int64_values(count, "Left count")?;
    Ok(Arc::new(StringArray::from_iter(values.zip(counts).map(
        |(value, count)| match (value, count) {
            (Some(value), Some(count)) if count >= 0 => {
                Some(value.chars().take(count as usize).collect::<String>())
            }
            _ => None,
        },
    ))))
}

fn substring_strings(
    value: &ArrayRef,
    start: &ArrayRef,
    count: Option<&ArrayRef>,
) -> Result<ArrayRef, ArrowError> {
    let values = string_values(value, "Substring")?;
    let starts = int64_values(start, "Substring start")?;
    let counts: Box<dyn Iterator<Item = Option<i64>>> = match count {
        Some(value) => int64_values(value, "Substring count")?,
        None => Box::new(std::iter::repeat_n(Some(i64::MAX), value.len())),
    };
    Ok(Arc::new(StringArray::from_iter(
        values
            .zip(starts)
            .zip(counts)
            .map(|((value, start), count)| match (value, start, count) {
                (Some(value), Some(start), Some(count)) if start >= 1 && count >= 0 => Some(
                    value
                        .chars()
                        .skip((start - 1) as usize)
                        .take(count as usize)
                        .collect::<String>(),
                ),
                _ => None,
            }),
    )))
}

fn parse_real(value: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let parsed = string_values(value, "ParseReal")?
        .map(|item| {
            item.map(|text| {
                text.parse::<f64>().map_err(|error| {
                    ArrowError::ParseError(format!("ParseReal failed for {text:?}: {error}"))
                })
            })
            .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Float64Array::from(parsed)))
}

fn base64_strings(function: &str, value: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let output = string_values(value, function)?
        .map(|item| -> Result<Option<String>, ArrowError> {
            let Some(text) = item else {
                return Ok(None);
            };
            if function == "base64encode" {
                Ok(Some(BASE64_STANDARD.encode(text.as_bytes())))
            } else {
                let decoded = BASE64_STANDARD.decode(text).map_err(|error| {
                    ArrowError::ParseError(format!("Base64Decode failed: {error}"))
                })?;
                String::from_utf8(decoded).map(Some).map_err(|error| {
                    ArrowError::ParseError(format!("Base64Decode is not UTF-8: {error}"))
                })
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(StringArray::from(output)))
}

fn deterministic_random(seed: i64, row_index: usize) -> f64 {
    let mut value = (seed as u64) ^ (row_index as u64).wrapping_mul(0x9E3779B97F4A7C15);
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    let bits = value.wrapping_mul(0x2545F4914F6CDD1D) >> 11;
    bits as f64 / ((1_u64 << 53) as f64)
}

fn seeded_rand(seed: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let seeds = int64_values(seed, "Rand seed")?;
    Ok(Arc::new(Float64Array::from_iter(seeds.enumerate().map(
        |(index, seed)| seed.map(|value| deterministic_random(value, index)),
    ))))
}

fn seeded_rand_between(
    low: &ArrayRef,
    high: &ArrayRef,
    seed: &ArrayRef,
) -> Result<ArrayRef, ArrowError> {
    let lows = int64_values(low, "RandBetween low")?;
    let highs = int64_values(high, "RandBetween high")?;
    let seeds = int64_values(seed, "RandBetween seed")?;
    Ok(Arc::new(Int64Array::from_iter(
        lows.zip(highs)
            .zip(seeds)
            .enumerate()
            .map(|(index, ((low, high), seed))| {
                let (low, high, seed) = (low?, high?, seed?);
                if high < low {
                    return None;
                }
                let width = high.checked_sub(low)?.checked_add(1)?;
                Some(low + (deterministic_random(seed, index) * width as f64).floor() as i64)
            }),
    )))
}

fn percent_ratio(numerator: &ArrayRef, denominator: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let numerator = cast(numerator.as_ref(), &DataType::Float64)?;
    let denominator = cast(denominator.as_ref(), &DataType::Float64)?;
    let numerator = numerator
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    let denominator = denominator
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    Ok(Arc::new(Float64Array::from_iter(
        numerator
            .iter()
            .zip(denominator.iter())
            .map(|(left, right)| {
                let left = left?;
                let right = right?;
                (right != 0.0).then_some(left / right)
            }),
    )))
}

fn time_span(
    days: &ArrayRef,
    hours: &ArrayRef,
    minutes: &ArrayRef,
    seconds: &ArrayRef,
    milliseconds: &ArrayRef,
) -> Result<ArrayRef, ArrowError> {
    let days = int64_values(days, "TimeSpan days")?;
    let hours = int64_values(hours, "TimeSpan hours")?;
    let minutes = int64_values(minutes, "TimeSpan minutes")?;
    let seconds = int64_values(seconds, "TimeSpan seconds")?;
    let milliseconds = int64_values(milliseconds, "TimeSpan milliseconds")?;
    Ok(Arc::new(DurationMicrosecondArray::from_iter(
        days.zip(hours)
            .zip(minutes)
            .zip(seconds)
            .zip(milliseconds)
            .map(|((((days, hours), minutes), seconds), milliseconds)| {
                days?
                    .checked_mul(86_400_000_000)?
                    .checked_add(hours?.checked_mul(3_600_000_000)?)?
                    .checked_add(minutes?.checked_mul(60_000_000)?)?
                    .checked_add(seconds?.checked_mul(1_000_000)?)?
                    .checked_add(milliseconds?.checked_mul(1_000)?)
            }),
    )))
}

fn fiscal_part(
    function: &str,
    value: &ArrayRef,
    offset: Option<&ArrayRef>,
) -> Result<ArrayRef, ArrowError> {
    let dates = temporal_datetimes(value, function)?;
    let offsets: Box<dyn Iterator<Item = Option<i64>>> = match offset {
        Some(value) => int64_values(value, "fiscal month offset")?,
        None => Box::new(std::iter::repeat_n(Some(0), value.len())),
    };
    Ok(Arc::new(Int64Array::from_iter(dates.zip(offsets).map(
        |(date, offset)| {
            let date = date?;
            let offset = u32::try_from(offset?).ok()?;
            let shifted = date.checked_sub_months(chrono::Months::new(offset))?;
            Some(match function {
                "fiscalmonth" => shifted.month() as i64,
                "fiscalquarter" => (shifted.month0() / 3 + 1) as i64,
                "fiscalyear" => shifted.year() as i64,
                _ => unreachable!("validated fiscal function"),
            })
        },
    ))))
}

fn scalar_string<'a>(array: &'a ArrayRef, context: &str) -> Result<&'a str, ArrowError> {
    let mut values = string_values(array, context)?;
    values.next().flatten().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("{context} must be a non-null string literal"))
    })
}

fn parse_date(value: &ArrayRef, format: &str) -> Result<ArrayRef, ArrowError> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    let parsed = string_values(value, "ParseDate")?
        .map(|item| {
            item.map(|text| {
                NaiveDate::parse_from_str(text, format)
                    .map(|date| (date - epoch).num_days() as i32)
                    .map_err(|error| {
                        ArrowError::ParseError(format!("ParseDate failed for {text:?}: {error}"))
                    })
            })
            .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Date32Array::from(parsed)))
}

fn parse_datetime(value: &ArrayRef, format: &str) -> Result<ArrayRef, ArrowError> {
    let parsed = string_values(value, "ParseDateTime")?
        .map(|item| {
            item.map(|text| {
                NaiveDateTime::parse_from_str(text, format)
                    .map(|date| date.and_utc().timestamp_micros())
                    .map_err(|error| {
                        ArrowError::ParseError(format!(
                            "ParseDateTime failed for {text:?}: {error}"
                        ))
                    })
            })
            .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(TimestampMicrosecondArray::from(parsed)))
}

fn parse_time(value: &ArrayRef, format: &str) -> Result<ArrayRef, ArrowError> {
    let parsed = string_values(value, "ParseTime")?
        .map(|item| {
            item.map(|text| {
                NaiveTime::parse_from_str(text, format)
                    .map(|time| {
                        time.num_seconds_from_midnight() as i64 * 1_000_000
                            + time.nanosecond() as i64 / 1_000
                    })
                    .map_err(|error| {
                        ArrowError::ParseError(format!("ParseTime failed for {text:?}: {error}"))
                    })
            })
            .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Time64MicrosecondArray::from(parsed)))
}

fn date_part(unit: &str, value: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let dates = temporal_datetimes(value, "DatePart")?;
    let unit = unit.to_ascii_lowercase();
    let extract: fn(NaiveDateTime) -> i64 = match unit.as_str() {
        "year" => |dt| dt.year() as i64,
        "quarter" => |dt| (dt.month0() / 3 + 1) as i64,
        "month" => |dt| dt.month() as i64,
        "day" | "dayofmonth" => |dt| dt.day() as i64,
        "dayofyear" => |dt| dt.ordinal() as i64,
        "weekday" | "dayofweek" => |dt| weekday_number(dt.weekday()),
        "week" | "isoweek" => |dt| dt.iso_week().week() as i64,
        "isoyear" => |dt| dt.iso_week().year() as i64,
        "hour" => |dt| dt.hour() as i64,
        "minute" => |dt| dt.minute() as i64,
        "second" => |dt| dt.second() as i64,
        "millisecond" => |dt| dt.and_utc().timestamp_subsec_millis() as i64,
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "unsupported DatePart unit: {other}"
            )))
        }
    };
    Ok(Arc::new(Int64Array::from_iter(
        dates.map(|item| item.map(extract)),
    )))
}

fn weekday_number(day: Weekday) -> i64 {
    day.num_days_from_monday() as i64 + 1
}

fn date_add(unit: &str, amount: &ArrayRef, value: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let multiplier = fixed_unit_micros(unit)?;
    let amounts = int64_values(amount, "DateAdd amount")?;
    match value.data_type() {
        DataType::Date32 => {
            if multiplier % 86_400_000_000 != 0 {
                return Err(ArrowError::InvalidArgumentError(
                    "DateAdd on Date32 only supports day".into(),
                ));
            }
            let values = value
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Date32");
            Ok(Arc::new(Date32Array::from_iter(
                values
                    .iter()
                    .zip(amounts)
                    .map(|(date, n)| Some(date? + (n? * multiplier / 86_400_000_000) as i32)),
            )))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let values = value
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp us");
            Ok(Arc::new(TimestampMicrosecondArray::from_iter(
                values
                    .iter()
                    .zip(amounts)
                    .map(|(date, n)| date?.checked_add(n?.checked_mul(multiplier)?)),
            )))
        }
        other => Err(ArrowError::InvalidArgumentError(format!(
            "DateAdd requires Date32 or timezone-naive Timestamp(us), got {other}"
        ))),
    }
}

fn date_diff(unit: &str, start: &ArrayRef, end: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let divisor = fixed_unit_micros(unit)?;
    let starts = temporal_micros(start, "DateDiff start")?;
    let ends = temporal_micros(end, "DateDiff end")?;
    Ok(Arc::new(Int64Array::from_iter(
        starts
            .zip(ends)
            .map(|(start, end)| Some((end? - start?) / divisor)),
    )))
}

fn fixed_unit_micros(unit: &str) -> Result<i64, ArrowError> {
    match unit.to_ascii_lowercase().as_str() {
        "day" | "days" => Ok(86_400_000_000),
        "hour" | "hours" => Ok(3_600_000_000),
        "minute" | "minutes" => Ok(60_000_000),
        "second" | "seconds" => Ok(1_000_000),
        "millisecond" | "milliseconds" => Ok(1_000),
        other => Err(ArrowError::NotYetImplemented(format!("calendar or unsupported date unit: {other}; supported units are day/hour/minute/second/millisecond"))),
    }
}

fn year_and_week(value: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(StringArray::from_iter(
        temporal_datetimes(value, "YearAndWeek")?.map(|item| {
            item.map(|dt| format!("{:04}-{:02}", dt.iso_week().year(), dt.iso_week().week()))
        }),
    )))
}

fn timestamp_epoch(value: &ArrayRef, divisor: i64) -> Result<ArrayRef, ArrowError> {
    let values = value
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "ToEpoch requires timezone-naive Timestamp(us), got {}",
                value.data_type()
            ))
        })?;
    Ok(Arc::new(Int64Array::from_iter(
        values.iter().map(|item| item.map(|v| v / divisor)),
    )))
}

fn epoch_timestamp(value: &ArrayRef, multiplier: i64) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(TimestampMicrosecondArray::from_iter(
        int64_values(value, "FromEpoch")?.map(|item| item.and_then(|v| v.checked_mul(multiplier))),
    )))
}

fn number_to_duration(value: &ArrayRef, multiplier: i64) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(DurationMicrosecondArray::from_iter(
        int64_values(value, "duration constructor")?
            .map(|item| item.and_then(|v| v.checked_mul(multiplier))),
    )))
}

fn duration_total(value: &ArrayRef, divisor: i64) -> Result<ArrayRef, ArrowError> {
    let values = value
        .as_any()
        .downcast_ref::<DurationMicrosecondArray>()
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Total* requires Duration(us), got {}",
                value.data_type()
            ))
        })?;
    Ok(Arc::new(Float64Array::from_iter(
        values
            .iter()
            .map(|item| item.map(|v| v as f64 / divisor as f64)),
    )))
}

fn temporal_micros<'a>(
    array: &'a ArrayRef,
    context: &str,
) -> Result<Box<dyn Iterator<Item = Option<i64>> + 'a>, ArrowError> {
    match array.data_type() {
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Date32");
            Ok(Box::new(
                values
                    .iter()
                    .map(|v| v.map(|days| days as i64 * 86_400_000_000)),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp us");
            Ok(Box::new(values.iter()))
        }
        other => Err(ArrowError::InvalidArgumentError(format!(
            "{context} requires Date32 or timezone-naive Timestamp(us), got {other}"
        ))),
    }
}

fn temporal_datetimes<'a>(
    array: &'a ArrayRef,
    context: &str,
) -> Result<Box<dyn Iterator<Item = Option<NaiveDateTime>> + 'a>, ArrowError> {
    let micros = temporal_micros(array, context)?;
    Ok(Box::new(micros.map(|value| {
        value.and_then(|v| chrono::DateTime::from_timestamp_micros(v).map(|dt| dt.naive_utc()))
    })))
}

fn find_strings(
    needle: &ArrayRef,
    haystack: &ArrayRef,
    start: Option<&ArrayRef>,
) -> Result<ArrayRef, ArrowError> {
    let needles = string_values(needle, "find needle")?;
    let haystacks = string_values(haystack, "find haystack")?;
    let starts: Box<dyn Iterator<Item = Option<i64>>> = match start {
        Some(value) => int64_values(value, "find start")?,
        None => Box::new(std::iter::repeat_n(Some(1), needle.len())),
    };
    Ok(Arc::new(Int64Array::from_iter(
        needles
            .zip(haystacks)
            .zip(starts)
            .map(
                |((needle, haystack), start)| match (needle, haystack, start) {
                    (Some(needle), Some(haystack), Some(start)) if start >= 1 => {
                        let start_index = (start - 1) as usize;
                        let suffix: String = haystack.chars().skip(start_index).collect();
                        let found = suffix
                            .find(needle)
                            .map(|byte_index| suffix[..byte_index].chars().count() as i64 + start);
                        Some(found.unwrap_or(0))
                    }
                    _ => None,
                },
            ),
    )))
}

fn split_strings(value: &ArrayRef, separator: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let values = string_values(value, "split value")?;
    let separators = string_values(separator, "split separator")?;
    let mut builder = ListBuilder::new(StringBuilder::new());
    for (value, separator) in values.zip(separators) {
        match (value, separator) {
            (Some(value), Some(separator)) => {
                for item in value.split(separator) {
                    builder.values().append_value(item);
                }
                builder.append(true);
            }
            _ => builder.append(false),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn split_string_at(
    value: &ArrayRef,
    separator: &ArrayRef,
    index: &ArrayRef,
) -> Result<ArrayRef, ArrowError> {
    let values = string_values(value, "split value")?;
    let separators = string_values(separator, "split separator")?;
    let indexes = int64_values(index, "split index")?;
    Ok(Arc::new(StringArray::from_iter(
        values
            .zip(separators)
            .zip(indexes)
            .map(
                |((value, separator), index)| match (value, separator, index) {
                    (Some(value), Some(separator), Some(index)) if index >= 1 => value
                        .split(separator)
                        .nth((index - 1) as usize)
                        .map(str::to_string),
                    _ => None,
                },
            ),
    )))
}

fn regex_extract(
    value: &ArrayRef,
    pattern: &ArrayRef,
    group: Option<&ArrayRef>,
) -> Result<ArrayRef, ArrowError> {
    let values = string_values(value, "rxextract value")?;
    let patterns = string_values(pattern, "rxextract pattern")?;
    let groups: Box<dyn Iterator<Item = Option<i64>>> = match group {
        Some(value) => int64_values(value, "rxextract group")?,
        None => Box::new(std::iter::repeat_n(Some(1), value.len())),
    };
    let output = values
        .zip(patterns)
        .zip(groups)
        .map(
            |((value, pattern), group)| -> Result<Option<String>, ArrowError> {
                let (Some(value), Some(pattern), Some(group)) = (value, pattern, group) else {
                    return Ok(None);
                };
                if group < 0 {
                    return Ok(None);
                }
                let regex = Regex::new(pattern)
                    .map_err(|error| ArrowError::ParseError(format!("invalid regex: {error}")))?;
                Ok(regex
                    .captures(value)
                    .and_then(|captures| captures.get(group as usize))
                    .map(|matched| matched.as_str().to_string()))
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(StringArray::from(output)))
}

fn regex_replace(
    value: &ArrayRef,
    pattern: &ArrayRef,
    replacement: &ArrayRef,
) -> Result<ArrayRef, ArrowError> {
    let values = string_values(value, "rxreplace value")?;
    let patterns = string_values(pattern, "rxreplace pattern")?;
    let replacements = string_values(replacement, "rxreplace replacement")?;
    let output = values
        .zip(patterns)
        .zip(replacements)
        .map(
            |((value, pattern), replacement)| -> Result<Option<String>, ArrowError> {
                let (Some(value), Some(pattern), Some(replacement)) = (value, pattern, replacement)
                else {
                    return Ok(None);
                };
                let regex = Regex::new(pattern)
                    .map_err(|error| ArrowError::ParseError(format!("invalid regex: {error}")))?;
                Ok(Some(regex.replace_all(value, replacement).into_owned()))
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(StringArray::from(output)))
}

fn boolean_array<'a>(array: &'a ArrayRef, context: &str) -> Result<&'a BooleanArray, ArrowError> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "{context} requires Boolean, got {}",
                array.data_type()
            ))
        })
}

fn int64_values<'a>(
    array: &'a ArrayRef,
    context: &str,
) -> Result<Box<dyn Iterator<Item = Option<i64>> + 'a>, ArrowError> {
    let values = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "{context} requires Int64, got {}",
            array.data_type()
        ))
    })?;
    Ok(Box::new(values.iter()))
}

fn string_values<'a>(
    array: &'a ArrayRef,
    context: &str,
) -> Result<Box<dyn Iterator<Item = Option<&'a str>> + 'a>, ArrowError> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Box::new(values.iter()));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Box::new(values.iter()));
    }
    Err(ArrowError::InvalidArgumentError(format!(
        "{context} requires String, got {}",
        array.data_type()
    )))
}

fn string_binary<T>(
    left: ArrayRef,
    right: ArrayRef,
    operation: impl Fn(&str, &str) -> T,
    string_output: bool,
) -> Result<ArrayRef, ArrowError>
where
    T: ToString,
{
    let left = string_values(&left, "string binary left")?;
    let right = string_values(&right, "string binary right")?;
    if string_output {
        Ok(Arc::new(StringArray::from_iter(left.zip(right).map(
            |(left, right)| Some(operation(left?, right?).to_string()),
        ))))
    } else {
        Ok(Arc::new(BooleanArray::from_iter(left.zip(right).map(
            |(left, right)| {
                let value = operation(left?, right?).to_string();
                Some(value == "true")
            },
        ))))
    }
}

fn literal_array(
    dtype: &str,
    value: &serde_json::Value,
    length: usize,
) -> Result<ArrayRef, ArrowError> {
    match dtype {
        "null" => Ok(Arc::new(NullArray::new(length))),
        "bool" => value
            .as_bool()
            .map(|item| Arc::new(BooleanArray::from(vec![Some(item); length])) as ArrayRef)
            .ok_or_else(|| ArrowError::ParseError(format!("invalid bool literal: {value}"))),
        "int8" | "int16" | "int32" | "int64" => {
            let item = value.as_i64().ok_or_else(|| {
                ArrowError::ParseError(format!("invalid {dtype} literal: {value}"))
            })?;
            cast(&Int64Array::from_value(item, length), &ir_data_type(dtype)?)
        }
        "float32" | "float64" => {
            let item = value.as_f64().ok_or_else(|| {
                ArrowError::ParseError(format!("invalid {dtype} literal: {value}"))
            })?;
            cast(
                &Float64Array::from_value(item, length),
                &ir_data_type(dtype)?,
            )
        }
        "string" => value
            .as_str()
            .map(|item| Arc::new(StringArray::from(vec![Some(item); length])) as ArrayRef)
            .ok_or_else(|| ArrowError::ParseError(format!("invalid string literal: {value}"))),
        "decimal128_38_10" => {
            let unscaled = parse_decimal_literal(value, 10)?;
            Ok(Arc::new(
                Decimal128Array::from_value(unscaled, length).with_precision_and_scale(38, 10)?,
            ))
        }
        "date32" => {
            let text = value.as_str().ok_or_else(|| {
                ArrowError::ParseError(format!("invalid date32 literal: {value}"))
            })?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
            let parsed = NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|error| {
                ArrowError::ParseError(format!("invalid date32 literal {text:?}: {error}"))
            })?;
            Ok(Arc::new(Date32Array::from_value(
                (parsed - epoch).num_days() as i32,
                length,
            )))
        }
        "timestamp_us" => {
            let text = value.as_str().ok_or_else(|| {
                ArrowError::ParseError(format!("invalid timestamp_us literal: {value}"))
            })?;
            let parsed = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f"))
                .map_err(|error| {
                    ArrowError::ParseError(format!(
                        "invalid timestamp_us literal {text:?}: {error}"
                    ))
                })?;
            Ok(Arc::new(TimestampMicrosecondArray::from_value(
                parsed.and_utc().timestamp_micros(),
                length,
            )))
        }
        "time64_us" => {
            let text = value.as_str().ok_or_else(|| {
                ArrowError::ParseError(format!("invalid time64_us literal: {value}"))
            })?;
            let parsed = NaiveTime::parse_from_str(text, "%H:%M:%S%.f").map_err(|error| {
                ArrowError::ParseError(format!("invalid time64_us literal {text:?}: {error}"))
            })?;
            let micros = i64::from(parsed.num_seconds_from_midnight()) * 1_000_000
                + i64::from(parsed.nanosecond() / 1_000);
            Ok(Arc::new(Time64MicrosecondArray::from_value(micros, length)))
        }
        "duration_us" => value
            .as_i64()
            .map(|item| Arc::new(DurationMicrosecondArray::from_value(item, length)) as ArrayRef)
            .ok_or_else(|| ArrowError::ParseError(format!("invalid duration_us literal: {value}"))),
        other => Err(ArrowError::NotYetImplemented(format!(
            "unsupported literal dtype: {other}"
        ))),
    }
}

fn parse_decimal_literal(value: &serde_json::Value, scale: usize) -> Result<i128, ArrowError> {
    let text = value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string());
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, text.as_str()),
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.bytes().all(|item| item.is_ascii_digit())
        || !fraction.bytes().all(|item| item.is_ascii_digit())
        || fraction.len() > scale
    {
        return Err(ArrowError::ParseError(format!(
            "invalid decimal128 literal for scale {scale}: {text:?}"
        )));
    }
    let whole = whole.parse::<i128>().map_err(|error| {
        ArrowError::ParseError(format!("invalid decimal128 literal {text:?}: {error}"))
    })?;
    let padded_fraction = format!("{fraction:0<scale$}");
    let fraction = if padded_fraction.is_empty() {
        0
    } else {
        padded_fraction.parse::<i128>().map_err(|error| {
            ArrowError::ParseError(format!("invalid decimal128 literal {text:?}: {error}"))
        })?
    };
    let factor = 10_i128.pow(scale as u32);
    let unscaled = whole
        .checked_mul(factor)
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| ArrowError::ParseError(format!("decimal128 literal overflow: {text:?}")))?;
    Ok(if negative { -unscaled } else { unscaled })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::{Float64Type, Int64Type};

    fn int64_lists(rows: Vec<Option<Vec<Option<i64>>>>) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(rows))
    }

    fn float64_lists(rows: Vec<Option<Vec<Option<f64>>>>) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(rows))
    }

    #[test]
    fn list_native_case_preserves_shape_and_broadcasts_scalars() {
        let source = int64_lists(vec![
            Some(vec![Some(4), Some(12), None]),
            None,
            Some(vec![]),
            Some(vec![Some(20)]),
        ]);
        let threshold: ArrayRef = Arc::new(Int64Array::from_value(10, 4));
        let condition = evaluate_binary(">", source.clone(), threshold).expect("List comparison");
        let multiplier: ArrayRef = Arc::new(Int64Array::from_value(2, 4));
        let truthy = evaluate_binary("*", source, multiplier).expect("List multiplication");
        let falsy: ArrayRef = Arc::new(Int64Array::from_value(0, 4));

        let output = evaluate_case_branch(condition, truthy, falsy).expect("List CASE");
        let expected = int64_lists(vec![
            Some(vec![Some(0), Some(24), Some(0)]),
            None,
            Some(vec![]),
            Some(vec![Some(40)]),
        ]);

        assert_eq!(output.to_data(), expected.to_data());
    }

    #[test]
    fn list_native_case_uses_four_columns_and_promotes_numeric_else() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "a",
                float64_lists(vec![
                    Some(vec![Some(1.0), Some(5.0)]),
                    Some(vec![Some(10.0)]),
                ]),
            ),
            (
                "b",
                float64_lists(vec![
                    Some(vec![Some(0.0), Some(6.0)]),
                    Some(vec![Some(5.0)]),
                ]),
            ),
            (
                "c",
                float64_lists(vec![
                    Some(vec![Some(2.0), Some(8.0)]),
                    Some(vec![Some(4.0)]),
                ]),
            ),
            (
                "d",
                float64_lists(vec![
                    Some(vec![Some(3.0), Some(7.0)]),
                    Some(vec![Some(8.0)]),
                ]),
            ),
            (
                "scale",
                Arc::new(Float64Array::from(vec![2.0, 2.0])) as ArrayRef,
            ),
        ])
        .unwrap();
        let document: ExpressionIrDocument = serde_json::from_value(serde_json::json!({
            "version": "spotfire-expression-ir.v1",
            "layers": [{
                "expressions": [{
                    "name": "adjusted",
                    "dtype": "float64",
                    "nullable": true,
                    "source": "CASE using a, b, c, d, scale",
                    "dependencies": ["a", "b", "c", "d", "scale"],
                    "expr": {
                        "kind": "alias",
                        "name": "adjusted",
                        "expression": {
                            "kind": "case",
                            "branches": [{
                                "when": {
                                    "kind": "binary",
                                    "operator": "and",
                                    "left": {
                                        "kind": "binary",
                                        "operator": ">",
                                        "left": {"kind": "column", "name": "a"},
                                        "right": {"kind": "column", "name": "b"}
                                    },
                                    "right": {
                                        "kind": "binary",
                                        "operator": "<",
                                        "left": {"kind": "column", "name": "c"},
                                        "right": {"kind": "column", "name": "d"}
                                    }
                                },
                                "then": {
                                    "kind": "binary",
                                    "operator": "*",
                                    "left": {
                                        "kind": "binary",
                                        "operator": "+",
                                        "left": {"kind": "column", "name": "a"},
                                        "right": {"kind": "column", "name": "c"}
                                    },
                                    "right": {"kind": "column", "name": "scale"}
                                }
                            }],
                            "otherwise": {"kind": "literal", "dtype": "int64", "value": 0}
                        }
                    }
                }]
            }]
        }))
        .unwrap();

        let output = execute_expression_ir(batch, &document).expect("four-column List CASE");
        let expected = float64_lists(vec![
            Some(vec![Some(6.0), Some(0.0)]),
            Some(vec![Some(28.0)]),
        ]);

        assert_eq!(
            output.column_by_name("adjusted").unwrap().to_data(),
            expected.to_data()
        );
    }

    #[test]
    fn list_expand_compact_case_uses_four_columns_and_promotes_numeric_else() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "a",
                float64_lists(vec![
                    Some(vec![Some(1.0), Some(5.0)]),
                    Some(vec![Some(10.0)]),
                ]),
            ),
            (
                "b",
                float64_lists(vec![
                    Some(vec![Some(0.0), Some(6.0)]),
                    Some(vec![Some(5.0)]),
                ]),
            ),
            (
                "c",
                float64_lists(vec![
                    Some(vec![Some(2.0), Some(8.0)]),
                    Some(vec![Some(4.0)]),
                ]),
            ),
            (
                "d",
                float64_lists(vec![
                    Some(vec![Some(3.0), Some(7.0)]),
                    Some(vec![Some(8.0)]),
                ]),
            ),
        ])
        .unwrap();
        let document: ExpressionIrDocument = serde_json::from_value(serde_json::json!({
            "version": "spotfire-expression-ir.v1",
            "layers": [{
                "expressions": [{
                    "name": "distance",
                    "dtype": "float64",
                    "nullable": true,
                    "source": "CASE using a, b, c, d with Abs fallback execution",
                    "dependencies": ["a", "b", "c", "d"],
                    "expr": {
                        "kind": "alias",
                        "name": "distance",
                        "expression": {
                            "kind": "case",
                            "branches": [{
                                "when": {
                                    "kind": "binary",
                                    "operator": "and",
                                    "left": {
                                        "kind": "binary",
                                        "operator": ">",
                                        "left": {"kind": "column", "name": "a"},
                                        "right": {"kind": "column", "name": "b"}
                                    },
                                    "right": {
                                        "kind": "binary",
                                        "operator": "<",
                                        "left": {"kind": "column", "name": "c"},
                                        "right": {"kind": "column", "name": "d"}
                                    }
                                },
                                "then": {
                                    "kind": "call",
                                    "function": "abs",
                                    "arguments": [{
                                        "kind": "binary",
                                        "operator": "-",
                                        "left": {"kind": "column", "name": "a"},
                                        "right": {"kind": "column", "name": "c"}
                                    }]
                                }
                            }],
                            "otherwise": {"kind": "literal", "dtype": "int64", "value": 0}
                        }
                    }
                }]
            }]
        }))
        .unwrap();

        let output = execute_expression_ir(batch, &document).expect("four-column expanded CASE");
        let expected = float64_lists(vec![
            Some(vec![Some(1.0), Some(0.0)]),
            Some(vec![Some(6.0)]),
        ]);

        assert_eq!(
            output.column_by_name("distance").unwrap().to_data(),
            expected.to_data()
        );
    }

    #[test]
    fn list_native_case_rejects_different_offsets() {
        let condition_source = int64_lists(vec![Some(vec![Some(1), Some(2)])]);
        let threshold: ArrayRef = Arc::new(Int64Array::from_value(0, 1));
        let condition = evaluate_binary(">", condition_source, threshold).expect("List comparison");
        let truthy = int64_lists(vec![Some(vec![Some(10)])]);
        let falsy: ArrayRef = Arc::new(Int64Array::from_value(0, 1));

        let error = evaluate_case_branch(condition, truthy, falsy).unwrap_err();

        assert!(error.to_string().contains("list.shape_mismatch"));
    }

    #[test]
    fn list_non_native_call_uses_expand_calculate_compact() {
        let source = int64_lists(vec![
            None,
            Some(vec![]),
            Some(vec![Some(-2), None, Some(3)]),
        ]);
        let batch = RecordBatch::try_from_iter(vec![("values", source)]).unwrap();
        let document: ExpressionIrDocument = serde_json::from_value(serde_json::json!({
            "version": "spotfire-expression-ir.v1",
            "layers": [{
                "expressions": [{
                    "name": "absolute_values",
                    "dtype": "float64",
                    "nullable": true,
                    "source": "Abs([values])",
                    "dependencies": ["values"],
                    "expr": {
                        "kind": "alias",
                        "name": "absolute_values",
                        "expression": {
                            "kind": "call",
                            "function": "abs",
                            "arguments": [{"kind": "column", "name": "values"}]
                        }
                    }
                }]
            }]
        }))
        .unwrap();

        let output = execute_expression_ir(batch, &document).expect("List fallback expression");
        let expected = float64_lists(vec![
            None,
            Some(vec![]),
            Some(vec![Some(2.0), None, Some(3.0)]),
        ]);

        assert_eq!(
            output.column_by_name("absolute_values").unwrap().to_data(),
            expected.to_data()
        );
    }

    #[test]
    fn preserves_input_array_buffers_when_adding_expression_columns() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("value", source.clone())]).unwrap();
        let document: ExpressionIrDocument = serde_json::from_value(serde_json::json!({
            "version": "spotfire-expression-ir.v1",
            "layers": [{
                "expressions": [{
                    "name": "value_x2",
                    "dtype": "int64",
                    "nullable": false,
                    "source": "[value] * 2",
                    "expr": {
                        "kind": "binary",
                        "operator": "*",
                        "left": {"kind": "column", "name": "value"},
                        "right": {"kind": "literal", "dtype": "int64", "value": 2}
                    }
                }]
            }]
        }))
        .unwrap();

        let output = execute_expression_ir(batch, &document).expect("execute IR");

        assert!(Arc::ptr_eq(
            &source,
            output.column_by_name("value").unwrap()
        ));
        assert_eq!(output.num_columns(), 2);
    }

    #[test]
    fn executes_all_typed_literal_contracts() {
        let cases = [
            ("bool", serde_json::json!(true), DataType::Boolean),
            ("int8", serde_json::json!(1), DataType::Int8),
            ("int16", serde_json::json!(2), DataType::Int16),
            ("int32", serde_json::json!(3), DataType::Int32),
            ("int64", serde_json::json!(4), DataType::Int64),
            ("float32", serde_json::json!(1.5), DataType::Float32),
            ("float64", serde_json::json!(2.5), DataType::Float64),
            ("string", serde_json::json!("text"), DataType::Utf8),
            (
                "decimal128_38_10",
                serde_json::json!("12.34"),
                DataType::Decimal128(38, 10),
            ),
            ("date32", serde_json::json!("2024-01-02"), DataType::Date32),
            (
                "timestamp_us",
                serde_json::json!("2024-01-02T03:04:05.006"),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "time64_us",
                serde_json::json!("03:04:05.006"),
                DataType::Time64(TimeUnit::Microsecond),
            ),
            (
                "duration_us",
                serde_json::json!(1234),
                DataType::Duration(TimeUnit::Microsecond),
            ),
        ];
        for (dtype, value, expected) in cases {
            let array = literal_array(dtype, &value, 2)
                .unwrap_or_else(|error| panic!("{dtype} literal failed: {error}"));
            assert_eq!(array.data_type(), &expected, "{dtype}");
            assert_eq!(array.len(), 2, "{dtype}");
        }
        assert_eq!(
            parse_decimal_literal(&serde_json::json!("-12.34"), 10).unwrap(),
            -123_400_000_000
        );
        assert!(parse_decimal_literal(&serde_json::json!("1.12345678901"), 10).is_err());
    }
    use crate::expression_ir::parse_expression_ir;

    #[test]
    fn executes_layered_int64_arithmetic() {
        let batch = RecordBatch::try_from_iter(vec![(
            "amount",
            Arc::new(Int64Array::from(vec![Some(2), None, Some(7)])) as ArrayRef,
        )])
        .expect("input batch");
        let document = parse_expression_ir(
            r#"{
              "version":"spotfire-expression-ir.v1",
              "layers":[
                {"expressions":[{
                  "name":"amount_x2","source":"[amount] * 2","dependencies":[],
                  "dtype":"unknown","nullable":true,
                  "expr":{"kind":"alias","name":"amount_x2","expression":{
                    "kind":"binary","operator":"*",
                    "left":{"kind":"column","name":"amount"},
                    "right":{"kind":"literal","dtype":"int64","value":2}
                  }}
                }]},
                {"expressions":[{
                  "name":"is_high","source":"[amount_x2] >= 10","dependencies":["amount_x2"],
                  "dtype":"unknown","nullable":true,
                  "expr":{"kind":"alias","name":"is_high","expression":{
                    "kind":"binary","operator":">=",
                    "left":{"kind":"column","name":"amount_x2"},
                    "right":{"kind":"literal","dtype":"int64","value":10}
                  }}
                }]}
              ]
            }"#,
        )
        .expect("IR fixture");

        let output = execute_expression_ir(batch, &document).expect("execute IR");
        let doubled = output
            .column_by_name("amount_x2")
            .expect("amount_x2")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        let high = output
            .column_by_name("is_high")
            .expect("is_high")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("bool output");
        assert_eq!(doubled, &Int64Array::from(vec![Some(4), None, Some(14)]));
        assert_eq!(
            high,
            &BooleanArray::from(vec![Some(false), None, Some(true)])
        );

        let mixed = evaluate_binary(
            "*",
            Arc::new(Float64Array::from(vec![Some(1.5), None])),
            Arc::new(Int64Array::from_value(2, 2)),
        )
        .expect("mixed numeric promotion");
        assert_eq!(
            mixed.as_any().downcast_ref::<Float64Array>().unwrap(),
            &Float64Array::from(vec![Some(3.0), None])
        );
    }

    #[test]
    fn executes_null_and_string_functions() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "name",
                Arc::new(StringArray::from(vec![Some("KOREA"), None, Some("US")])) as ArrayRef,
            ),
            (
                "fallback",
                Arc::new(StringArray::from(vec![Some("X"), Some("EMPTY"), Some("Y")])) as ArrayRef,
            ),
        ])
        .expect("input batch");
        let document = parse_expression_ir(
            r#"{
              "version":"spotfire-expression-ir.v1",
              "layers":[{"expressions":[
                {"name":"display","source":"SN([name], [fallback])","dependencies":[],"dtype":"unknown","nullable":true,
                 "expr":{"kind":"alias","name":"display","expression":{"kind":"call","function":"sn","arguments":[{"kind":"column","name":"name"},{"kind":"column","name":"fallback"}]}}},
                {"name":"name_len","source":"Len([name])","dependencies":[],"dtype":"unknown","nullable":true,
                 "expr":{"kind":"alias","name":"name_len","expression":{"kind":"call","function":"len","arguments":[{"kind":"column","name":"name"}]}}}
              ]}]
            }"#,
        )
        .expect("IR fixture");

        let output = execute_expression_ir(batch, &document).expect("execute IR");
        let display = output
            .column_by_name("display")
            .expect("display")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string output");
        let lengths = output
            .column_by_name("name_len")
            .expect("name_len")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("length output");
        assert_eq!(
            display,
            &StringArray::from(vec![Some("KOREA"), Some("EMPTY"), Some("US")])
        );
        assert_eq!(lengths, &Int64Array::from(vec![Some(5), None, Some(2)]));
    }

    #[test]
    fn executes_case_cast_concat_and_contains() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "amount",
                Arc::new(Int64Array::from(vec![Some(2), Some(7)])) as ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec![Some("KOR"), Some("USA")])) as ArrayRef,
            ),
        ])
        .expect("input batch");
        let document = parse_expression_ir(
            r#"{
              "version":"spotfire-expression-ir.v1",
              "layers":[{"expressions":[
                {"name":"bucket","source":"If([amount] >= 5, 10, 0)","dependencies":[],"dtype":"unknown","nullable":true,
                 "expr":{"kind":"alias","name":"bucket","expression":{"kind":"cast","target_dtype":"float64","expression":{"kind":"case","branches":[{"when":{"kind":"binary","operator":">=","left":{"kind":"column","name":"amount"},"right":{"kind":"literal","dtype":"int64","value":5}},"then":{"kind":"literal","dtype":"int64","value":10}}],"otherwise":{"kind":"literal","dtype":"int64","value":0}}}}},
                {"name":"is_kor","source":"[name] ~= \"KOR\"","dependencies":[],"dtype":"unknown","nullable":true,
                 "expr":{"kind":"alias","name":"is_kor","expression":{"kind":"binary","operator":"contains","left":{"kind":"column","name":"name"},"right":{"kind":"literal","dtype":"string","value":"KOR"}}}}
              ]}]
            }"#,
        )
        .expect("IR fixture");

        let output = execute_expression_ir(batch, &document).expect("execute IR");
        let bucket = output
            .column_by_name("bucket")
            .expect("bucket")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float output");
        let is_kor = output
            .column_by_name("is_kor")
            .expect("is_kor")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("bool output");
        assert_eq!(bucket, &Float64Array::from(vec![Some(0.0), Some(10.0)]));
        assert_eq!(is_kor, &BooleanArray::from(vec![Some(true), Some(false)]));
    }

    #[test]
    fn executes_find_split_and_regex_functions() {
        let batch = RecordBatch::try_from_iter(vec![(
            "code",
            Arc::new(StringArray::from(vec![Some("ABC-123"), Some("USA-9")])) as ArrayRef,
        )])
        .expect("input batch");
        let document = parse_expression_ir(
            r#"{
              "version":"spotfire-expression-ir.v1",
              "layers":[{"expressions":[
                {"name":"prefix","source":"Split([code], \"-\", 1)","dependencies":[],"dtype":"unknown","nullable":true,"expr":{"kind":"alias","name":"prefix","expression":{"kind":"call","function":"split","arguments":[{"kind":"column","name":"code"},{"kind":"literal","dtype":"string","value":"-"},{"kind":"literal","dtype":"int64","value":1}]}}},
                {"name":"dash_at","source":"Find(\"-\", [code])","dependencies":[],"dtype":"unknown","nullable":true,"expr":{"kind":"alias","name":"dash_at","expression":{"kind":"call","function":"find","arguments":[{"kind":"literal","dtype":"string","value":"-"},{"kind":"column","name":"code"}]}}},
                {"name":"letters","source":"RxExtract([code], \"([A-Z]+)-\", 1)","dependencies":[],"dtype":"unknown","nullable":true,"expr":{"kind":"alias","name":"letters","expression":{"kind":"call","function":"rxextract","arguments":[{"kind":"column","name":"code"},{"kind":"literal","dtype":"string","value":"([A-Z]+)-"},{"kind":"literal","dtype":"int64","value":1}]}}}
              ]}]
            }"#,
        )
        .expect("IR fixture");

        let output = execute_expression_ir(batch, &document).expect("execute IR");
        let prefix = output
            .column_by_name("prefix")
            .expect("prefix")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string output");
        let dash_at = output
            .column_by_name("dash_at")
            .expect("dash_at")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int output");
        let letters = output
            .column_by_name("letters")
            .expect("letters")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string output");
        assert_eq!(prefix, &StringArray::from(vec![Some("ABC"), Some("USA")]));
        assert_eq!(dash_at, &Int64Array::from(vec![Some(4), Some(4)]));
        assert_eq!(letters, &StringArray::from(vec![Some("ABC"), Some("USA")]));
    }

    #[test]
    fn executes_datetime_epoch_and_duration_contract() {
        let raw_dates: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2024-12-30"),
            Some("2025-01-02"),
            None,
        ]));
        let dates = parse_date(&raw_dates, "%Y-%m-%d").expect("parse date");
        let iso_year = date_part("isoyear", &dates).expect("ISO year");
        let iso_week = date_part("isoweek", &dates).expect("ISO week");
        let year_week = year_and_week(&dates).expect("year and week");
        assert_eq!(
            iso_year
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("year"),
            &Int64Array::from(vec![Some(2025), Some(2025), None])
        );
        assert_eq!(
            iso_week
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("week"),
            &Int64Array::from(vec![Some(1), Some(1), None])
        );
        assert_eq!(
            year_week
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("year-week"),
            &StringArray::from(vec![Some("2025-01"), Some("2025-01"), None])
        );

        let raw_datetimes: ArrayRef = Arc::new(StringArray::from(vec![
            Some("1970-01-01 00:00:01.500"),
            None,
        ]));
        let datetimes =
            parse_datetime(&raw_datetimes, "%Y-%m-%d %H:%M:%S%.3f").expect("parse datetime");
        let epoch_ms = timestamp_epoch(&datetimes, 1_000).expect("epoch millis");
        assert_eq!(
            epoch_ms
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("millis"),
            &Int64Array::from(vec![Some(1500), None])
        );
        let round_trip = epoch_timestamp(&epoch_ms, 1_000).expect("from epoch millis");
        assert_eq!(round_trip.as_ref(), datetimes.as_ref());

        let raw_times: ArrayRef = Arc::new(StringArray::from(vec![Some("01:02:03.004"), None]));
        let times = parse_time(&raw_times, "%H:%M:%S%.3f").expect("parse time");
        assert_eq!(
            times
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .expect("time"),
            &Time64MicrosecondArray::from(vec![Some(3_723_004_000), None])
        );

        let counts: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), None]));
        let duration = number_to_duration(&counts, 3_600_000_000).expect("hours");
        let total_hours = duration_total(&duration, 3_600_000_000).expect("total hours");
        assert_eq!(
            total_hours
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("hours"),
            &Float64Array::from(vec![Some(2.0), None])
        );
        assert!(duration_total(&datetimes, 1_000_000).is_err());

        let one_day: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(1), None]));
        let next = date_add("day", &one_day, &dates).expect("date add");
        let difference = date_diff("day", &dates, &next).expect("date diff");
        assert_eq!(
            difference
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("difference"),
            &Int64Array::from(vec![Some(1), Some(1), None])
        );
        assert!(date_add("month", &one_day, &dates).is_err());
    }

    #[test]
    fn executes_partitioned_window_statistics_and_ranks() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "group",
                Arc::new(StringArray::from(vec![
                    Some("A"),
                    Some("A"),
                    Some("A"),
                    Some("B"),
                ])) as ArrayRef,
            ),
            (
                "amount",
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(2.0),
                    Some(100.0),
                    Some(5.0),
                ])) as ArrayRef,
            ),
            (
                "weight",
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(2.0),
                    Some(1.0),
                    Some(2.0),
                ])) as ArrayRef,
            ),
            (
                "label",
                Arc::new(StringArray::from(vec![
                    Some("one"),
                    Some("two"),
                    Some("max"),
                    Some("five"),
                ])) as ArrayRef,
            ),
        ])
        .expect("window input");
        let document = parse_expression_ir(r#"{
          "version":"spotfire-expression-ir.v1","layers":[{"expressions":[
            {"name":"avg_amount","source":"Avg([amount]) OVER ([group])","dependencies":[],"dtype":"unknown","nullable":true,"expr":{"kind":"alias","name":"avg_amount","expression":{"kind":"window","expression":{"kind":"call","function":"avg","arguments":[{"kind":"column","name":"amount"}]},"partition_by":[{"kind":"column","name":"group"}],"order_by":[],"frame":null}}},
            {"name":"weighted","source":"WeightedAverage([amount],[weight]) OVER ([group])","dependencies":[],"dtype":"unknown","nullable":true,"expr":{"kind":"alias","name":"weighted","expression":{"kind":"window","expression":{"kind":"call","function":"weightedaverage","arguments":[{"kind":"column","name":"amount"},{"kind":"column","name":"weight"}]},"partition_by":[{"kind":"column","name":"group"}],"order_by":[],"frame":null}}},
            {"name":"max_label","source":"ValueForMax([label],[amount]) OVER ([group])","dependencies":[],"dtype":"unknown","nullable":true,"expr":{"kind":"alias","name":"max_label","expression":{"kind":"window","expression":{"kind":"call","function":"valueformax","arguments":[{"kind":"column","name":"label"},{"kind":"column","name":"amount"}]},"partition_by":[{"kind":"column","name":"group"}],"order_by":[],"frame":null}}},
            {"name":"dense_rank","source":"DenseRank([amount],\"desc\",[group])","dependencies":[],"dtype":"unknown","nullable":true,"expr":{"kind":"alias","name":"dense_rank","expression":{"kind":"window","expression":{"kind":"call","function":"denserank","arguments":[{"kind":"column","name":"amount"},{"kind":"literal","dtype":"string","value":"desc"}]},"partition_by":[{"kind":"column","name":"group"}],"order_by":[],"frame":null}}}
          ]}]}
        "#).expect("window IR");

        let output = execute_expression_ir(batch, &document).expect("execute windows");
        assert_eq!(
            output
                .column_by_name("avg_amount")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &Float64Array::from(vec![
                Some(103.0 / 3.0),
                Some(103.0 / 3.0),
                Some(103.0 / 3.0),
                Some(5.0)
            ])
        );
        assert_eq!(
            output
                .column_by_name("weighted")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &Float64Array::from(vec![
                Some(105.0 / 4.0),
                Some(105.0 / 4.0),
                Some(105.0 / 4.0),
                Some(5.0)
            ])
        );
        assert_eq!(
            output
                .column_by_name("max_label")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec![Some("max"), Some("max"), Some("max"), Some("five")])
        );
        assert_eq!(
            output
                .column_by_name("dense_rank")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &Float64Array::from(vec![Some(3.0), Some(2.0), Some(1.0), Some(1.0)])
        );
    }

    #[test]
    fn executes_deterministic_row_number_with_null_ordering() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "group",
                Arc::new(StringArray::from(vec!["a", "a", "a", "b"])) as ArrayRef,
            ),
            (
                "updated",
                Arc::new(Int64Array::from(vec![Some(2), None, Some(2), Some(5)])) as ArrayRef,
            ),
        ])
        .expect("row number input");
        let document: ExpressionIrDocument = serde_json::from_str(
            r#"{
              "version":"spotfire-expression-ir.v1",
              "layers":[{"expressions":[{
                "name":"rn","source":"RowNumber","dependencies":[],"dtype":"int64","nullable":false,
                "expr":{"kind":"alias","name":"rn","expression":{
                  "kind":"window",
                  "expression":{"kind":"call","function":"rownumber","arguments":[]},
                  "partition_by":[{"kind":"column","name":"group"}],
                  "order_by":[{"expression":{"kind":"column","name":"updated"},"direction":"desc","nulls":"last"}],
                  "frame":null
                }}
              }]}]
            }"#,
        )
        .expect("row number IR");

        let output = execute_expression_ir(batch, &document).expect("row number execution");
        let row_numbers = output
            .column_by_name("rn")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            (0..row_numbers.len())
                .map(|index| row_numbers.value(index))
                .collect::<Vec<_>>(),
            vec![1, 3, 2, 1]
        );
    }

    #[test]
    fn executes_promoted_passthrough_scalar_and_seeded_random_functions() {
        let numbers: ArrayRef = Arc::new(Float64Array::from(vec![Some(-2.0), Some(4.0), None]));
        let absolute = numeric_unary("abs", &numbers).expect("abs");
        assert_eq!(
            absolute.as_any().downcast_ref::<Float64Array>().unwrap(),
            &Float64Array::from(vec![Some(2.0), Some(4.0), None])
        );
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some(" AbC "), None]));
        let trimmed = string_unary("trim", &text).expect("trim");
        assert_eq!(
            trimmed.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![Some("AbC"), None])
        );
        let encoded = base64_strings("base64encode", &text).expect("base64 encode");
        let decoded = base64_strings("base64decode", &encoded).expect("base64 decode");
        assert_eq!(decoded.as_ref(), text.as_ref());

        let seed: ArrayRef = Arc::new(Int64Array::from_value(7, 3));
        let first = seeded_rand(&seed).expect("seeded rand");
        let second = seeded_rand(&seed).expect("seeded rand repeat");
        assert_eq!(first.as_ref(), second.as_ref());
        let low: ArrayRef = Arc::new(Int64Array::from_value(1, 3));
        let high: ArrayRef = Arc::new(Int64Array::from_value(3, 3));
        let buckets = seeded_rand_between(&low, &high, &seed).expect("rand between");
        assert!(buckets
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .all(|value| (1..=3).contains(&value)));
    }

    #[test]
    fn executes_all_canonical_scalar_function_branches() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "number",
                Arc::new(Float64Array::from(vec![Some(0.5), Some(2.0), None])) as ArrayRef,
            ),
            (
                "integer",
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![Some(" AbC-12 "), Some("X-9"), None])) as ArrayRef,
            ),
            (
                "date_text",
                Arc::new(StringArray::from(vec![
                    Some("2024-02-03"),
                    Some("2025-03-04"),
                    None,
                ])) as ArrayRef,
            ),
            (
                "datetime_text",
                Arc::new(StringArray::from(vec![
                    Some("2024-02-03 01:02:03"),
                    Some("2025-03-04 04:05:06"),
                    None,
                ])) as ArrayRef,
            ),
            (
                "time_text",
                Arc::new(StringArray::from(vec![
                    Some("01:02:03"),
                    Some("04:05:06"),
                    None,
                ])) as ArrayRef,
            ),
        ])
        .expect("scalar coverage input");
        let column = |name: &str| ExpressionNode::Column {
            name: name.to_string(),
        };
        let literal_string = |value: &str| ExpressionNode::Literal {
            dtype: "string".to_string(),
            value: serde_json::Value::String(value.to_string()),
        };
        let literal_int = |value: i64| ExpressionNode::Literal {
            dtype: "int64".to_string(),
            value: serde_json::Value::from(value),
        };
        let run = |function: &str, arguments: Vec<ExpressionNode>| {
            evaluate_call(function, &arguments, &batch)
                .unwrap_or_else(|error| panic!("{function} execution fixture failed: {error}"))
        };

        for function in [
            "abs", "acos", "asin", "atan", "ceiling", "cos", "exp", "floor", "ln", "log", "log10",
            "sin", "sqrt", "tan",
        ] {
            run(function, vec![column("number")]);
        }
        for function in ["atan2", "power", "mod"] {
            run(function, vec![column("number"), column("integer")]);
        }
        run("round", vec![column("number"), literal_int(1)]);
        run("pi", vec![]);
        for function in ["lower", "upper", "trim"] {
            run(function, vec![column("text")]);
        }
        run("left", vec![column("text"), literal_int(2)]);
        run(
            "substring",
            vec![column("text"), literal_int(2), literal_int(3)],
        );
        run("len", vec![column("text")]);
        run("right", vec![column("text"), literal_int(2)]);
        run("mid", vec![column("text"), literal_int(2), literal_int(3)]);
        run(
            "substitute",
            vec![column("text"), literal_string("-"), literal_string("_")],
        );
        for function in ["find", "charindex"] {
            run(function, vec![literal_string("-"), column("text")]);
        }
        run("split", vec![column("text"), literal_string("-")]);
        run(
            "rxextract",
            vec![
                column("text"),
                literal_string("([A-Za-z]+)"),
                literal_int(1),
            ],
        );
        run(
            "rxreplace",
            vec![column("text"), literal_string("-"), literal_string("_")],
        );
        run("parsereal", vec![literal_string("1.5")]);
        let encoded = run("base64encode", vec![column("text")]);
        let encoded_batch = RecordBatch::try_from_iter(vec![("encoded", encoded)]).unwrap();
        evaluate_call(
            "base64decode",
            &[ExpressionNode::Column {
                name: "encoded".to_string(),
            }],
            &encoded_batch,
        )
        .expect("Base64Decode execution fixture");
        run(
            "concatenate",
            vec![column("text"), literal_string("!"), column("text")],
        );

        let parsed_date = run("parsedate", vec![column("date_text")]);
        let parsed_datetime = run("parsedatetime", vec![column("datetime_text")]);
        run("parsetime", vec![column("time_text")]);
        let temporal_batch = RecordBatch::try_from_iter(vec![
            ("date", parsed_date.clone()),
            ("datetime", parsed_datetime.clone()),
            ("integer", batch.column_by_name("integer").unwrap().clone()),
        ])
        .unwrap();
        let temporal_column = |name: &str| ExpressionNode::Column {
            name: name.to_string(),
        };
        let temporal_run = |function: &str, arguments: Vec<ExpressionNode>| {
            evaluate_call(function, &arguments, &temporal_batch)
                .unwrap_or_else(|error| panic!("{function} execution fixture failed: {error}"))
        };
        for function in [
            "day",
            "dayofmonth",
            "dayofweek",
            "dayofyear",
            "month",
            "quarter",
            "week",
            "year",
            "isoweek",
            "isoyear",
            "yearandweek",
            "fiscalmonth",
            "fiscalquarter",
            "fiscalyear",
        ] {
            temporal_run(function, vec![temporal_column("date")]);
        }
        for function in ["hour", "minute", "second", "millisecond"] {
            temporal_run(function, vec![temporal_column("datetime")]);
        }
        temporal_run(
            "datepart",
            vec![literal_string("day"), temporal_column("date")],
        );
        temporal_run(
            "dateadd",
            vec![
                literal_string("day"),
                temporal_column("integer"),
                temporal_column("date"),
            ],
        );
        temporal_run(
            "datediff",
            vec![
                literal_string("day"),
                temporal_column("date"),
                temporal_column("date"),
            ],
        );
        temporal_run("toepochseconds", vec![temporal_column("datetime")]);
        temporal_run("toepochmilliseconds", vec![temporal_column("datetime")]);
        temporal_run("fromepochseconds", vec![temporal_column("integer")]);
        temporal_run("fromepochmilliseconds", vec![temporal_column("integer")]);

        for (duration_function, total_function) in [
            ("days", "totaldays"),
            ("hours", "totalhours"),
            ("minutes", "totalminutes"),
            ("seconds", "totalseconds"),
            ("milliseconds", "totalmilliseconds"),
        ] {
            let duration = run(duration_function, vec![column("integer")]);
            let duration_batch = RecordBatch::try_from_iter(vec![("duration", duration)]).unwrap();
            evaluate_call(
                total_function,
                &[ExpressionNode::Column {
                    name: "duration".to_string(),
                }],
                &duration_batch,
            )
            .unwrap_or_else(|error| panic!("{total_function} execution fixture failed: {error}"));
        }
        run(
            "timespan",
            vec![
                column("integer"),
                column("integer"),
                column("integer"),
                column("integer"),
                column("integer"),
            ],
        );
        run("percent", vec![column("number"), column("integer")]);
        run("rand", vec![column("integer")]);
        run(
            "randbetween",
            vec![literal_int(1), literal_int(10), column("integer")],
        );
        run("isnull", vec![column("text")]);
        run("sn", vec![column("text"), literal_string("fallback")]);
        run("coalesce", vec![column("text"), literal_string("fallback")]);
        run(
            "if",
            vec![
                ExpressionNode::Binary {
                    operator: ">".to_string(),
                    left: Box::new(column("integer")),
                    right: Box::new(literal_int(0)),
                },
                literal_int(1),
                literal_int(0),
            ],
        );
    }

    #[test]
    fn executes_all_canonical_window_function_branches() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "group",
                Arc::new(StringArray::from(vec![Some("A"), Some("A"), Some("A")])) as ArrayRef,
            ),
            (
                "number",
                Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), Some(4.0)])) as ArrayRef,
            ),
            (
                "other",
                Arc::new(Float64Array::from(vec![Some(2.0), Some(3.0), Some(5.0)])) as ArrayRef,
            ),
            (
                "label",
                Arc::new(StringArray::from(vec![Some("x"), Some("y"), Some("z")])) as ArrayRef,
            ),
        ])
        .unwrap();
        let column = |name: &str| ExpressionNode::Column {
            name: name.to_string(),
        };
        let literal_string = |value: &str| ExpressionNode::Literal {
            dtype: "string".to_string(),
            value: serde_json::Value::String(value.to_string()),
        };
        let literal_float = |value: f64| ExpressionNode::Literal {
            dtype: "float64".to_string(),
            value: serde_json::Value::from(value),
        };
        let literal_int = |value: i64| ExpressionNode::Literal {
            dtype: "int64".to_string(),
            value: serde_json::Value::from(value),
        };
        let run = |function: &str, arguments: Vec<ExpressionNode>| {
            evaluate_window(
                &ExpressionNode::Call {
                    function: function.to_string(),
                    arguments,
                },
                &[column("group")],
                &[],
                None,
                &batch,
            )
            .unwrap_or_else(|error| panic!("{function} execution fixture failed: {error}"));
        };

        for function in [
            "sum",
            "avg",
            "min",
            "max",
            "median",
            "count",
            "uniquecount",
            "p10",
            "p90",
            "q1",
            "q3",
            "iqr",
            "var",
            "product",
            "range",
            "stddev",
            "stderr",
            "geometricmean",
            "l95",
            "u95",
            "lif",
            "uif",
            "lof",
            "uof",
            "meandeviation",
            "medianabsolutedeviation",
            "lav",
            "uav",
            "outliers",
            "pctoutliers",
            "first",
            "last",
            "mostcommon",
            "uniqueconcatenate",
            "firstvalidafter",
            "lastvalidbefore",
        ] {
            run(function, vec![column("number")]);
        }
        run("percentile", vec![column("number"), literal_float(0.5)]);
        run("trimmedmean", vec![column("number"), literal_float(0.1)]);
        run("nthlargest", vec![column("number"), literal_int(2)]);
        run("nthsmallest", vec![column("number"), literal_int(2)]);
        run("covariance", vec![column("number"), column("other")]);
        run("weightedaverage", vec![column("number"), column("other")]);
        for function in [
            "valueformax",
            "valueformin",
            "lastvalueformax",
            "lastvalueformin",
        ] {
            run(function, vec![column("label"), column("number")]);
        }
        for function in ["denserank", "rankreal"] {
            run(function, vec![column("number"), literal_string("asc")]);
        }
        run("lag", vec![column("label"), literal_int(1)]);
        run("lead", vec![column("label"), literal_int(1)]);
    }
}
