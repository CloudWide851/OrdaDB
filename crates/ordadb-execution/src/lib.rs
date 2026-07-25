//! Physical relational operators and typed scalar evaluation for OrdaDB.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;

use ordadb_index::{BPlusTree, IndexKey};
use ordadb_optimizer::{AccessPath, PlanKind, PlanNode};
use ordadb_sql::{
    AggregateFunction, BinaryOperator, BoundExpr, BoundExprKind, BoundOrder, UnaryOperator,
};
use ordadb_types::{DbError, IndexId, Result, Row, ScalarType, TableId, Value};
use rust_decimal::Decimal;

pub struct ExecutionContext<'a> {
    pub tables: &'a BTreeMap<TableId, Vec<Row>>,
    pub indexes: &'a BTreeMap<IndexId, BPlusTree>,
    pub params: &'a [Value],
}

pub fn execute(plan: &PlanNode, context: &ExecutionContext<'_>) -> Result<Vec<Row>> {
    match &plan.kind {
        PlanKind::Scan {
            table_id, access, ..
        } => scan(*table_id, access, context),
        PlanKind::Filter { predicate, input } => execute(input, context)?
            .into_iter()
            .filter_map(
                |row| match predicate_matches(predicate, &row, context.params) {
                    Ok(true) => Some(Ok(row)),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                },
            )
            .collect(),
        PlanKind::Projection { expressions, input } => execute(input, context)?
            .iter()
            .map(|row| {
                expressions
                    .iter()
                    .map(|projection| evaluate(&projection.expr, &row.values, context.params))
                    .collect::<Result<Vec<_>>>()
                    .map(Row::new)
            })
            .collect(),
        PlanKind::Sort { order_by, input } => {
            let mut rows = execute(input, context)?;
            let mut error = None;
            rows.sort_by(|left, right| {
                compare_rows(left, right, order_by).unwrap_or_else(|sort_error| {
                    error = Some(sort_error);
                    Ordering::Equal
                })
            });
            if let Some(error) = error {
                return Err(error);
            }
            Ok(rows)
        }
        PlanKind::Limit { limit, input } => {
            let mut rows = execute(input, context)?;
            rows.truncate(evaluate_limit(limit, context.params)?);
            Ok(rows)
        }
    }
}

fn scan(
    table_id: TableId,
    access: &AccessPath,
    context: &ExecutionContext<'_>,
) -> Result<Vec<Row>> {
    let rows = context.tables.get(&table_id).map_or(&[][..], Vec::as_slice);
    match access {
        AccessPath::Empty => Ok(Vec::new()),
        AccessPath::Sequential => Ok(rows.to_vec()),
        AccessPath::Index {
            index_id,
            operator,
            value,
            ..
        } => {
            let value = evaluate(value, &[], context.params)?;
            if value.is_null() {
                return Ok(Vec::new());
            }
            let key = IndexKey::from_values(&[value])?;
            let tree = context
                .indexes
                .get(index_id)
                .ok_or_else(|| DbError::internal("planned index is unavailable"))?;
            let entries = match operator {
                BinaryOperator::Eq => tree.get(&key),
                BinaryOperator::Lt => tree.range(Bound::Unbounded, Bound::Excluded(&key)),
                BinaryOperator::LtEq => tree.range(Bound::Unbounded, Bound::Included(&key)),
                BinaryOperator::Gt => tree.range(Bound::Excluded(&key), Bound::Unbounded),
                BinaryOperator::GtEq => tree.range(Bound::Included(&key), Bound::Unbounded),
                _ => {
                    return Err(DbError::internal(
                        "optimizer selected an unsupported index operator",
                    ));
                }
            };
            entries
                .into_iter()
                .map(|entry| {
                    usize::try_from(entry.row_id.get())
                        .ok()
                        .and_then(|row_id| rows.get(row_id))
                        .cloned()
                        .ok_or_else(|| DbError::internal("index row reference is out of bounds"))
                })
                .collect()
        }
    }
}

pub fn evaluate(expr: &BoundExpr, row: &[Value], params: &[Value]) -> Result<Value> {
    let value = match &expr.kind {
        BoundExprKind::Column { index } => row.get(*index).cloned().ok_or_else(|| {
            DbError::internal(format!("bound column index {index} is out of range"))
        })?,
        BoundExprKind::Literal(value) => value.clone(),
        BoundExprKind::Parameter { index } => params.get(index - 1).cloned().ok_or_else(|| {
            DbError::new("42P02", format!("no value supplied for parameter ${index}"))
        })?,
        BoundExprKind::Unary { op, expr } => evaluate_unary(*op, evaluate(expr, row, params)?)?,
        BoundExprKind::Binary { left, op, right } => evaluate_binary(
            evaluate(left, row, params)?,
            *op,
            evaluate(right, row, params)?,
        )?,
        BoundExprKind::Aggregate { .. } => {
            return Err(DbError::internal(
                "aggregate expression requires a grouped execution context",
            ));
        }
    };
    coerce_value(value, &expr.data_type)
}

pub fn evaluate_group(
    expr: &BoundExpr,
    rows: &[Row],
    representative: &[Value],
    params: &[Value],
) -> Result<Value> {
    let value = match &expr.kind {
        BoundExprKind::Aggregate { function, argument } => {
            evaluate_aggregate(*function, argument.as_deref(), rows, params)?
        }
        BoundExprKind::Column { .. }
        | BoundExprKind::Literal(_)
        | BoundExprKind::Parameter { .. } => evaluate(expr, representative, params)?,
        BoundExprKind::Unary { op, expr } => {
            evaluate_unary(*op, evaluate_group(expr, rows, representative, params)?)?
        }
        BoundExprKind::Binary { left, op, right } => evaluate_binary(
            evaluate_group(left, rows, representative, params)?,
            *op,
            evaluate_group(right, rows, representative, params)?,
        )?,
    };
    coerce_value(value, &expr.data_type)
}

fn evaluate_aggregate(
    function: AggregateFunction,
    argument: Option<&BoundExpr>,
    rows: &[Row],
    params: &[Value],
) -> Result<Value> {
    if function == AggregateFunction::Count {
        let count = if let Some(argument) = argument {
            rows.iter()
                .map(|row| evaluate(argument, &row.values, params))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .filter(|value| !value.is_null())
                .count()
        } else {
            rows.len()
        };
        return i64::try_from(count)
            .map(Value::Int64)
            .map_err(|_| DbError::new("22003", "COUNT result is out of range"));
    }
    let argument = argument.ok_or_else(|| DbError::internal("aggregate argument is missing"))?;
    let values = rows
        .iter()
        .map(|row| evaluate(argument, &row.values, params))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|value| !value.is_null())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Ok(Value::Null);
    }
    match function {
        AggregateFunction::Count => unreachable!("handled above"),
        AggregateFunction::Sum => sum_values(&values),
        AggregateFunction::Avg => {
            let sum = values.iter().try_fold(0.0, |sum, value| {
                numeric_f64(value).map(|value| sum + value)
            })?;
            Ok(Value::Float64(sum / values.len() as f64))
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let mut selected = values[0].clone();
            for value in values.iter().skip(1) {
                let ordering = compare_values(value, &selected)?;
                let replace = if function == AggregateFunction::Min {
                    ordering == Ordering::Less
                } else {
                    ordering == Ordering::Greater
                };
                if replace {
                    selected = value.clone();
                }
            }
            Ok(selected)
        }
    }
}

fn sum_values(values: &[Value]) -> Result<Value> {
    match &values[0] {
        Value::Int16(_) | Value::Int32(_) | Value::Int64(_) => values
            .iter()
            .try_fold(0_i64, |sum, value| {
                let value = match value {
                    Value::Int16(value) => i64::from(*value),
                    Value::Int32(value) => i64::from(*value),
                    Value::Int64(value) => *value,
                    _ => return Err(DbError::new("42804", "SUM values have mixed types")),
                };
                sum.checked_add(value)
                    .ok_or_else(|| DbError::new("22003", "SUM result is out of range"))
            })
            .map(Value::Int64),
        Value::Float32(_) | Value::Float64(_) => values
            .iter()
            .try_fold(0.0, |sum, value| {
                numeric_f64(value).map(|value| sum + value)
            })
            .map(Value::Float64),
        Value::Decimal(_) => values
            .iter()
            .try_fold(Decimal::ZERO, |sum, value| match value {
                Value::Decimal(value) => sum
                    .checked_add(*value)
                    .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
                _ => Err(DbError::new("42804", "SUM values have mixed types")),
            })
            .map(Value::Decimal),
        _ => Err(DbError::new("42804", "SUM requires numeric values")),
    }
}

fn numeric_f64(value: &Value) -> Result<f64> {
    match value {
        Value::Int16(value) => Ok(f64::from(*value)),
        Value::Int32(value) => Ok(f64::from(*value)),
        Value::Int64(value) => Ok(*value as f64),
        Value::Float32(value) => Ok(f64::from(*value)),
        Value::Float64(value) => Ok(*value),
        Value::Decimal(value) => value
            .to_string()
            .parse()
            .map_err(|_| DbError::new("22003", "decimal cannot be represented as FLOAT8")),
        _ => Err(DbError::new("42804", "numeric value required")),
    }
}

fn evaluate_unary(operator: UnaryOperator, value: Value) -> Result<Value> {
    match (operator, value) {
        (_, Value::Null) => Ok(Value::Null),
        (UnaryOperator::Not, Value::Boolean(value)) => Ok(Value::Boolean(!value)),
        (UnaryOperator::Negate, Value::Int16(value)) => value
            .checked_neg()
            .map(Value::Int16)
            .ok_or_else(|| DbError::new("22003", "numeric value out of range")),
        (UnaryOperator::Negate, Value::Int32(value)) => value
            .checked_neg()
            .map(Value::Int32)
            .ok_or_else(|| DbError::new("22003", "numeric value out of range")),
        (UnaryOperator::Negate, Value::Int64(value)) => value
            .checked_neg()
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "numeric value out of range")),
        (UnaryOperator::Negate, Value::Float32(value)) => Ok(Value::Float32(-value)),
        (UnaryOperator::Negate, Value::Float64(value)) => Ok(Value::Float64(-value)),
        (UnaryOperator::Negate, Value::Decimal(value)) => Ok(Value::Decimal(-value)),
        _ => Err(DbError::new(
            "42804",
            "unary operator received an incompatible value",
        )),
    }
}

fn evaluate_binary(left: Value, operator: BinaryOperator, right: Value) -> Result<Value> {
    if matches!(operator, BinaryOperator::And | BinaryOperator::Or) {
        return evaluate_boolean_binary(left, operator, right);
    }
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    match operator {
        BinaryOperator::Eq => return Ok(Value::Boolean(left == right)),
        BinaryOperator::NotEq => return Ok(Value::Boolean(left != right)),
        _ => {}
    }
    let ordering = compare_values(&left, &right)?;
    Ok(Value::Boolean(match operator {
        BinaryOperator::Lt => ordering == Ordering::Less,
        BinaryOperator::LtEq => ordering != Ordering::Greater,
        BinaryOperator::Gt => ordering == Ordering::Greater,
        BinaryOperator::GtEq => ordering != Ordering::Less,
        _ => unreachable!("handled above"),
    }))
}

fn evaluate_boolean_binary(left: Value, operator: BinaryOperator, right: Value) -> Result<Value> {
    let left = boolean_or_null(left)?;
    let right = boolean_or_null(right)?;
    let value = match operator {
        BinaryOperator::And => match (left, right) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        BinaryOperator::Or => match (left, right) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        _ => unreachable!("only boolean operators are accepted"),
    };
    Ok(value.map_or(Value::Null, Value::Boolean))
}

fn boolean_or_null(value: Value) -> Result<Option<bool>> {
    match value {
        Value::Null => Ok(None),
        Value::Boolean(value) => Ok(Some(value)),
        _ => Err(DbError::new("42804", "boolean value required")),
    }
}

pub fn predicate_matches(expr: &BoundExpr, row: &Row, params: &[Value]) -> Result<bool> {
    match evaluate(expr, &row.values, params)? {
        Value::Boolean(value) => Ok(value),
        Value::Null => Ok(false),
        _ => Err(DbError::new("42804", "predicate must evaluate to boolean")),
    }
}

fn evaluate_limit(expr: &BoundExpr, params: &[Value]) -> Result<usize> {
    match evaluate(expr, &[], params)? {
        Value::Int64(value) if value >= 0 => {
            usize::try_from(value).map_err(|_| DbError::new("22003", "LIMIT value is out of range"))
        }
        Value::Null => Err(DbError::new("22004", "LIMIT cannot be null")),
        _ => Err(DbError::new(
            "2201W",
            "LIMIT must be a non-negative integer",
        )),
    }
}

pub fn coerce_value(value: Value, target: &ScalarType) -> Result<Value> {
    if value.is_null() {
        return Ok(value);
    }
    match (value, target) {
        (Value::Boolean(value), ScalarType::Boolean) => Ok(Value::Boolean(value)),
        (Value::Int16(value), ScalarType::Int16) => Ok(Value::Int16(value)),
        (Value::Int16(value), ScalarType::Int32) => Ok(Value::Int32(i32::from(value))),
        (Value::Int16(value), ScalarType::Int64) => Ok(Value::Int64(i64::from(value))),
        (Value::Int16(value), ScalarType::Float32) => Ok(Value::Float32(f32::from(value))),
        (Value::Int16(value), ScalarType::Float64) => Ok(Value::Float64(f64::from(value))),
        (Value::Int16(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Int32(value), ScalarType::Int32) => Ok(Value::Int32(value)),
        (Value::Int32(value), ScalarType::Int64) => Ok(Value::Int64(i64::from(value))),
        (Value::Int32(value), ScalarType::Float64) => Ok(Value::Float64(f64::from(value))),
        (Value::Int32(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Int64(value), ScalarType::Int64) => Ok(Value::Int64(value)),
        (Value::Int64(value), ScalarType::Float64) => Ok(Value::Float64(value as f64)),
        (Value::Int64(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Float32(value), ScalarType::Float32) => Ok(Value::Float32(value)),
        (Value::Float64(value), ScalarType::Float64) => Ok(Value::Float64(value)),
        (Value::Decimal(value), ScalarType::Decimal { .. }) => Ok(Value::Decimal(value)),
        (
            Value::Text(value),
            ScalarType::Text | ScalarType::Char { .. } | ScalarType::Varchar { .. },
        ) => Ok(Value::Text(value)),
        (Value::Binary(value), ScalarType::Binary) => Ok(Value::Binary(value)),
        (Value::Date(value), ScalarType::Date) => Ok(Value::Date(value)),
        (Value::Time(value), ScalarType::Time) => Ok(Value::Time(value)),
        (
            Value::Timestamp(value),
            ScalarType::Timestamp {
                with_timezone: false,
            },
        ) => Ok(Value::Timestamp(value)),
        (Value::Json(value), ScalarType::Json) => Ok(Value::Json(value)),
        (Value::Jsonb(value), ScalarType::Jsonb) => Ok(Value::Jsonb(value)),
        (Value::Uuid(value), ScalarType::Uuid) => Ok(Value::Uuid(value)),
        (Value::Vector(value), ScalarType::Vector { dimensions })
            if dimensions.is_none_or(|dimensions| dimensions == value.len()) =>
        {
            Ok(Value::Vector(value))
        }
        (value, target) => Err(DbError::new(
            "42804",
            format!("value {value:?} cannot be assigned to {target:?}"),
        )),
    }
}

fn compare_rows(left: &Row, right: &Row, order_by: &[BoundOrder]) -> Result<Ordering> {
    for order in order_by {
        let left_value = &left.values[order.column_index];
        let right_value = &right.values[order.column_index];
        let ordering = match (left_value.is_null(), right_value.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if order.nulls_first.unwrap_or(!order.ascending) {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if order.nulls_first.unwrap_or(!order.ascending) {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = compare_values(left_value, right_value)?;
                if order.ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            }
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

pub fn compare_values(left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        (Value::Boolean(left), Value::Boolean(right)) => Ok(left.cmp(right)),
        (Value::Int16(left), Value::Int16(right)) => Ok(left.cmp(right)),
        (Value::Int32(left), Value::Int32(right)) => Ok(left.cmp(right)),
        (Value::Int64(left), Value::Int64(right)) => Ok(left.cmp(right)),
        (Value::Float32(left), Value::Float32(right)) => left
            .partial_cmp(right)
            .ok_or_else(|| DbError::new("22000", "NaN values are not orderable")),
        (Value::Float64(left), Value::Float64(right)) => left
            .partial_cmp(right)
            .ok_or_else(|| DbError::new("22000", "NaN values are not orderable")),
        (Value::Decimal(left), Value::Decimal(right)) => Ok(left.cmp(right)),
        (Value::Text(left), Value::Text(right)) => Ok(left.cmp(right)),
        (Value::Binary(left), Value::Binary(right)) => Ok(left.cmp(right)),
        (Value::Date(left), Value::Date(right)) => Ok(left.cmp(right)),
        (Value::Time(left), Value::Time(right)) => Ok(left.cmp(right)),
        (Value::Timestamp(left), Value::Timestamp(right)) => Ok(left.cmp(right)),
        (Value::Uuid(left), Value::Uuid(right)) => Ok(left.cmp(right)),
        _ => Err(DbError::new(
            "42883",
            "values do not have a compatible ordering operator",
        )),
    }
}
