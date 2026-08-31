use pyo3::exceptions::PyValueError;
use serde::{Deserialize, Serialize};

pub const SUPPORTED_IR_VERSION: &str = "spotfire-expression-ir.v1";
pub const SUPPORTED_SCALAR_FUNCTIONS: &[&str] = &[
    "if",
    "isnull",
    "sn",
    "coalesce",
    "len",
    "right",
    "mid",
    "substitute",
    "find",
    "charindex",
    "split",
    "rxextract",
    "rxreplace",
    "parsedate",
    "parsedatetime",
    "parsetime",
    "datepart",
    "dateadd",
    "datediff",
    "isoweek",
    "isoyear",
    "yearandweek",
    "toepochseconds",
    "toepochmilliseconds",
    "fromepochseconds",
    "fromepochmilliseconds",
    "days",
    "hours",
    "minutes",
    "seconds",
    "milliseconds",
    "totaldays",
    "totalhours",
    "totalminutes",
    "totalseconds",
    "totalmilliseconds",
    "sum",
    "avg",
    "average",
    "min",
    "max",
    "median",
    "count",
    "uniquecount",
    "percentile",
    "p10",
    "p90",
    "q1",
    "q3",
    "iqr",
    "var",
    "variance",
    "covariance",
    "weightedaverage",
    "denserank",
    "rownumber",
    "rankreal",
    "valueformax",
    "valueformin",
    "nthlargest",
    "nthsmallest",
    "meandeviation",
    "medianabsolutedeviation",
    "trimmedmean",
    "lav",
    "uav",
    "outliers",
    "pctoutliers",
    "firstvalidafter",
    "lastvalidbefore",
    "abs",
    "acos",
    "asin",
    "atan",
    "atan2",
    "ceiling",
    "cos",
    "exp",
    "floor",
    "left",
    "ln",
    "log",
    "log10",
    "lower",
    "mod",
    "pi",
    "power",
    "round",
    "sin",
    "sqrt",
    "substring",
    "tan",
    "trim",
    "upper",
    "parsereal",
    "base64encode",
    "base64decode",
    "day",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "hour",
    "millisecond",
    "minute",
    "month",
    "quarter",
    "second",
    "week",
    "year",
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
    "mostcommon",
    "uniqueconcatenate",
    "lastvalueformax",
    "lastvalueformin",
    "rand",
    "randbetween",
    "percent",
    "timespan",
    "first",
    "last",
    "lag",
    "lead",
    "rollingavg",
    "rollingmean",
    "rollingsum",
    "rollingmin",
    "rollingmax",
    "fiscalmonth",
    "fiscalquarter",
    "fiscalyear",
    "concatenate",
];

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpressionIrDocument {
    pub version: String,
    pub layers: Vec<ExpressionLayer>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpressionLayer {
    pub expressions: Vec<IrExpression>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IrExpression {
    pub name: String,
    pub source: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub dtype: String,
    pub nullable: bool,
    pub expr: ExpressionNode,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExpressionNode {
    Column {
        name: String,
    },
    Literal {
        dtype: String,
        value: serde_json::Value,
    },
    Unary {
        operator: String,
        operand: Box<ExpressionNode>,
    },
    Binary {
        operator: String,
        left: Box<ExpressionNode>,
        right: Box<ExpressionNode>,
    },
    Call {
        function: String,
        arguments: Vec<ExpressionNode>,
    },
    Case {
        branches: Vec<CaseBranch>,
        otherwise: Box<ExpressionNode>,
    },
    Cast {
        expression: Box<ExpressionNode>,
        target_dtype: String,
    },
    Window {
        expression: Box<ExpressionNode>,
        partition_by: Vec<ExpressionNode>,
        order_by: Vec<WindowOrder>,
        frame: Option<serde_json::Value>,
    },
    Alias {
        name: String,
        expression: Box<ExpressionNode>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WindowOrder {
    pub expression: ExpressionNode,
    pub direction: String,
    pub nulls: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CaseBranch {
    pub when: ExpressionNode,
    pub then: ExpressionNode,
}

pub fn parse_expression_ir(ir_json: &str) -> Result<ExpressionIrDocument, String> {
    let document: ExpressionIrDocument =
        serde_json::from_str(ir_json).map_err(|error| format!("invalid expression IR: {error}"))?;
    validate_expression_ir(&document)?;
    Ok(document)
}

pub fn validate_expression_ir(document: &ExpressionIrDocument) -> Result<(), String> {
    if document.version != SUPPORTED_IR_VERSION {
        return Err(format!(
            "unsupported expression IR version: expected={SUPPORTED_IR_VERSION}, actual={}",
            document.version
        ));
    }
    for (layer_index, layer) in document.layers.iter().enumerate() {
        for expression in &layer.expressions {
            if expression.name.trim().is_empty() {
                return Err(format!("expression name is empty in layer {layer_index}"));
            }
            validate_node(&expression.expr).map_err(|reason| {
                format!(
                    "expression validation failed: expression={:?}, source={:?}, reason={reason}",
                    expression.name, expression.source
                )
            })?;
        }
    }
    Ok(())
}

fn validate_node(node: &ExpressionNode) -> Result<(), String> {
    match node {
        ExpressionNode::Column { name } if name.trim().is_empty() => {
            Err("column name is empty".to_string())
        }
        ExpressionNode::Unary { operator, operand } => {
            if !matches!(operator.as_str(), "positive" | "negate" | "not") {
                return Err(format!("unsupported unary node: {operator}"));
            }
            validate_node(operand)
        }
        ExpressionNode::Binary {
            operator,
            left,
            right,
        } => {
            if !matches!(
                operator.as_str(),
                "+" | "-"
                    | "*"
                    | "/"
                    | "%"
                    | "="
                    | "!="
                    | "<"
                    | "<="
                    | ">"
                    | ">="
                    | "and"
                    | "or"
                    | "contains"
                    | "concat"
            ) {
                return Err(format!("unsupported binary node: {operator}"));
            }
            validate_node(left)?;
            validate_node(right)
        }
        ExpressionNode::Call {
            function,
            arguments,
        } => {
            if function.trim().is_empty() {
                return Err("function name is empty".to_string());
            }
            for argument in arguments {
                validate_node(argument)?;
            }
            if function == "rand" && arguments.len() != 1 {
                return Err("Rand requires an explicit seed: Rand(seed)".to_string());
            }
            if function == "randbetween" && arguments.len() != 3 {
                return Err(
                    "RandBetween requires an explicit seed: RandBetween(low, high, seed)"
                        .to_string(),
                );
            }
            if !SUPPORTED_SCALAR_FUNCTIONS.contains(&function.as_str()) {
                return Err(format!("unsupported scalar function: {function}"));
            }
            Ok(())
        }
        ExpressionNode::Case {
            branches,
            otherwise,
        } => {
            if branches.is_empty() {
                return Err("case node requires at least one branch".to_string());
            }
            for branch in branches {
                validate_node(&branch.when)?;
                validate_node(&branch.then)?;
            }
            validate_node(otherwise)
        }
        ExpressionNode::Cast {
            expression,
            target_dtype,
        } => {
            if !matches!(
                target_dtype.as_str(),
                "bool"
                    | "int8"
                    | "int16"
                    | "int32"
                    | "int64"
                    | "float32"
                    | "float64"
                    | "string"
                    | "date32"
                    | "time64_us"
                    | "timestamp_us"
                    | "duration_us"
                    | "decimal128_38_10"
            ) {
                return Err(format!("unsupported cast target: {target_dtype}"));
            }
            validate_node(expression)
        }
        ExpressionNode::Window {
            expression,
            partition_by,
            order_by,
            frame,
        } => {
            if let Some(frame) = frame {
                let object = frame
                    .as_object()
                    .ok_or_else(|| "window frame must be an object".to_string())?;
                if object.get("kind").and_then(|value| value.as_str()) != Some("rows") {
                    return Err("window frame kind must be rows".to_string());
                }
                let preceding = object
                    .get("preceding")
                    .and_then(|value| value.as_u64())
                    .ok_or_else(|| {
                        "window frame preceding must be a non-negative integer".to_string()
                    })?;
                let following = object
                    .get("following")
                    .and_then(|value| value.as_u64())
                    .ok_or_else(|| {
                        "window frame following must be a non-negative integer".to_string()
                    })?;
                let minimum = object
                    .get("minimum_periods")
                    .and_then(|value| value.as_u64())
                    .ok_or_else(|| {
                        "window frame minimum_periods must be a positive integer".to_string()
                    })?;
                if minimum == 0 || minimum > preceding + following + 1 {
                    return Err("window frame minimum_periods is outside the frame".to_string());
                }
                if order_by.is_empty() {
                    return Err("window frame requires at least one order_by item".to_string());
                }
                if !matches!(
                    expression.as_ref(),
                    ExpressionNode::Call { function, .. }
                        if matches!(
                            function.as_str(),
                            "rollingavg"
                                | "rollingmean"
                                | "rollingsum"
                                | "rollingmin"
                                | "rollingmax"
                        )
                ) {
                    return Err("window frames require a rolling aggregate function".to_string());
                }
            }
            if !matches!(expression.as_ref(), ExpressionNode::Call { .. }) {
                return Err("window expression must contain a canonical call node".to_string());
            }
            validate_node(expression)?;
            for partition in partition_by {
                validate_node(partition)?;
            }
            for order in order_by {
                validate_node(&order.expression)?;
                if !matches!(order.direction.as_str(), "asc" | "desc") {
                    return Err(format!(
                        "invalid window order direction: {}",
                        order.direction
                    ));
                }
                if !matches!(order.nulls.as_str(), "first" | "last") {
                    return Err(format!("invalid window null ordering: {}", order.nulls));
                }
            }
            if matches!(expression.as_ref(), ExpressionNode::Call { function, .. } if function == "rownumber")
                && order_by.is_empty()
            {
                return Err("RowNumber requires at least one order_by item".to_string());
            }
            Ok(())
        }
        ExpressionNode::Alias { name, expression } => {
            if name.trim().is_empty() {
                return Err("alias name is empty".to_string());
            }
            validate_node(expression)
        }
        _ => Ok(()),
    }
}

pub fn validate_expression_ir_json(ir_json: String) -> pyo3::PyResult<String> {
    let document = parse_expression_ir(&ir_json).map_err(PyValueError::new_err)?;
    serde_json::to_string(&document).map_err(|error| {
        PyValueError::new_err(format!("failed to serialize expression IR: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_ir() -> &'static str {
        r#"{
          "version":"spotfire-expression-ir.v1",
          "layers":[{"expressions":[{
            "name":"amount_x2",
            "source":"[amount] * 2",
            "dependencies":[],
            "dtype":"unknown",
            "nullable":true,
            "expr":{"kind":"alias","name":"amount_x2","expression":{
              "kind":"binary","operator":"*",
              "left":{"kind":"column","name":"amount"},
              "right":{"kind":"literal","dtype":"int64","value":2}
            }}
          }]}]
        }"#
    }

    #[test]
    fn parses_and_validates_v1_document() {
        let document = parse_expression_ir(valid_ir()).expect("valid IR");
        assert_eq!(document.version, SUPPORTED_IR_VERSION);
        assert_eq!(document.layers[0].expressions[0].name, "amount_x2");
    }

    #[test]
    fn rejects_unknown_version() {
        let invalid = valid_ir().replace(SUPPORTED_IR_VERSION, "spotfire-expression-ir.v2");
        let error = parse_expression_ir(&invalid).expect_err("unsupported version");
        assert!(error.contains("unsupported expression IR version"));
    }

    #[test]
    fn rejects_noncanonical_window_before_task_execution() {
        let mut invalid: serde_json::Value =
            serde_json::from_str(valid_ir()).expect("valid JSON fixture");
        invalid["layers"][0]["expressions"][0]["expr"]["expression"] = serde_json::json!({
            "kind": "window",
            "expression": {"kind": "column", "name": "amount"},
            "partition_by": [],
            "order_by": [],
            "frame": null
        });
        let error = parse_expression_ir(&invalid.to_string()).expect_err("unsupported window");
        assert!(error.contains("window expression must contain a canonical call node"));
    }
}
