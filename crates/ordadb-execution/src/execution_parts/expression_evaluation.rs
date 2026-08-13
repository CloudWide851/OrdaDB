
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
        (UnaryOperator::Negate, Value::Interval(value)) => {
            Ok(Value::Interval(ordadb_types::PgInterval::new(
                value
                    .months
                    .checked_neg()
                    .ok_or_else(|| DbError::new("22015", "interval field is out of range"))?,
                value
                    .days
                    .checked_neg()
                    .ok_or_else(|| DbError::new("22015", "interval field is out of range"))?,
                value
                    .microseconds
                    .checked_neg()
                    .ok_or_else(|| DbError::new("22015", "interval field is out of range"))?,
            )))
        }
        _ => Err(DbError::new(
            "42804",
            "unary operator received an incompatible value",
        )),
    }
}

fn evaluate_binary(left: Value, operator: BinaryOperator, right: Value) -> Result<Value> {
    let operand_type = left
        .scalar_type()
        .or_else(|| right.scalar_type())
        .unwrap_or(ScalarType::Text);
    evaluate_binary_as(left, operator, right, &operand_type)
}

fn evaluate_binary_as(
    left: Value,
    operator: BinaryOperator,
    right: Value,
    operand_type: &ScalarType,
) -> Result<Value> {
    if matches!(operator, BinaryOperator::And | BinaryOperator::Or) {
        return evaluate_boolean_binary(left, operator, right);
    }
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    if matches!(
        operator,
        BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
    ) {
        return evaluate_arithmetic_binary(left, operator, right);
    }
    let equals = match (&left, &right) {
        (Value::Interval(left), Value::Interval(right)) => {
            interval_comparison_key(*left) == interval_comparison_key(*right)
        }
        _ => left == right,
    };
    match operator {
        BinaryOperator::Eq => return Ok(Value::Boolean(equals)),
        BinaryOperator::NotEq => return Ok(Value::Boolean(!equals)),
        _ => {}
    }
    let ordering = compare_values_as(&left, &right, operand_type)?;
    Ok(Value::Boolean(match operator {
        BinaryOperator::Lt => ordering == Ordering::Less,
        BinaryOperator::LtEq => ordering != Ordering::Greater,
        BinaryOperator::Gt => ordering == Ordering::Greater,
        BinaryOperator::GtEq => ordering != Ordering::Less,
        _ => unreachable!("handled above"),
    }))
}

fn evaluate_arithmetic_binary(
    left: Value,
    operator: BinaryOperator,
    right: Value,
) -> Result<Value> {
    macro_rules! checked_integer {
        ($left:expr, $right:expr, $variant:ident) => {{
            let value = match operator {
                BinaryOperator::Add => $left.checked_add($right),
                BinaryOperator::Subtract => $left.checked_sub($right),
                BinaryOperator::Multiply => $left.checked_mul($right),
                BinaryOperator::Divide if $right == 0 => return Err(division_by_zero()),
                BinaryOperator::Divide => $left.checked_div($right),
                BinaryOperator::Modulo if $right == 0 => return Err(division_by_zero()),
                BinaryOperator::Modulo => $left.checked_rem($right),
                _ => unreachable!("arithmetic operator checked by caller"),
            }
            .ok_or_else(numeric_out_of_range)?;
            Ok(Value::$variant(value))
        }};
    }

    match (left, right) {
        (Value::Int16(left), Value::Int16(right)) => checked_integer!(left, right, Int16),
        (Value::Int32(left), Value::Int32(right)) => checked_integer!(left, right, Int32),
        (Value::Int64(left), Value::Int64(right)) => checked_integer!(left, right, Int64),
        (Value::Float32(left), Value::Float32(right)) => {
            evaluate_float32_arithmetic(left, operator, right)
        }
        (Value::Float64(left), Value::Float64(right)) => {
            evaluate_float64_arithmetic(left, operator, right)
        }
        (Value::Decimal(left), Value::Decimal(right)) => {
            let value = match operator {
                BinaryOperator::Add => left.checked_add(right),
                BinaryOperator::Subtract => left.checked_sub(right),
                BinaryOperator::Multiply => left.checked_mul(right),
                BinaryOperator::Divide if right.is_zero() => return Err(division_by_zero()),
                BinaryOperator::Divide => left.checked_div(right),
                BinaryOperator::Modulo if right.is_zero() => return Err(division_by_zero()),
                BinaryOperator::Modulo => left.checked_rem(right),
                _ => unreachable!("arithmetic operator checked by caller"),
            }
            .ok_or_else(numeric_out_of_range)?;
            Ok(Value::Decimal(value))
        }
        _ => Err(DbError::new(
            "42883",
            "arithmetic operands do not have a common numeric type",
        )),
    }
}

fn evaluate_float32_arithmetic(left: f32, operator: BinaryOperator, right: f32) -> Result<Value> {
    if matches!(operator, BinaryOperator::Divide | BinaryOperator::Modulo) && right == 0.0 {
        return Err(division_by_zero());
    }
    let value = match operator {
        BinaryOperator::Add => left + right,
        BinaryOperator::Subtract => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => left / right,
        BinaryOperator::Modulo => left % right,
        _ => unreachable!("arithmetic operator checked by caller"),
    };
    if value.is_infinite() && left.is_finite() && right.is_finite() {
        return Err(numeric_out_of_range());
    }
    Ok(Value::Float32(value))
}

fn evaluate_float64_arithmetic(left: f64, operator: BinaryOperator, right: f64) -> Result<Value> {
    if matches!(operator, BinaryOperator::Divide | BinaryOperator::Modulo) && right == 0.0 {
        return Err(division_by_zero());
    }
    let value = match operator {
        BinaryOperator::Add => left + right,
        BinaryOperator::Subtract => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => left / right,
        BinaryOperator::Modulo => left % right,
        _ => unreachable!("arithmetic operator checked by caller"),
    };
    if value.is_infinite() && left.is_finite() && right.is_finite() {
        return Err(numeric_out_of_range());
    }
    Ok(Value::Float64(value))
}

fn division_by_zero() -> DbError {
    DbError::new("22012", "division by zero")
}

fn numeric_out_of_range() -> DbError {
    DbError::new("22003", "numeric value is out of range")
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

fn evaluate_limit_program(program: &ExpressionProgram, params: &[Value]) -> Result<usize> {
    match program.evaluate(&[], params)? {
        Value::Int64(value) if value >= 0 => {
            usize::try_from(value).map_err(|_| DbError::new("22003", "LIMIT value is out of range"))
        }
        Value::Null => Ok(usize::MAX),
        _ => Err(DbError::new(
            "2201W",
            "LIMIT must be a non-negative integer",
        )),
    }
}

fn evaluate_offset_program(program: &ExpressionProgram, params: &[Value]) -> Result<usize> {
    match program.evaluate(&[], params)? {
        Value::Int64(value) if value >= 0 => usize::try_from(value)
            .map_err(|_| DbError::new("22003", "OFFSET value is out of range")),
        Value::Null => Ok(0),
        _ => Err(DbError::new(
            "2201X",
            "OFFSET must be a non-negative integer",
        )),
    }
}

fn evaluate_scalar_function(function: ScalarFunction, arguments: Vec<Value>) -> Result<Value> {
    match function {
        ScalarFunction::Version
        | ScalarFunction::CurrentDatabase
        | ScalarFunction::CurrentUser
        | ScalarFunction::SessionUser
        | ScalarFunction::CurrentSetting => Err(DbError::internal(
            "session scalar function reached execution without binding metadata",
        )),
        ScalarFunction::Coalesce => Ok(arguments
            .into_iter()
            .find(|value| !value.is_null())
            .unwrap_or(Value::Null)),
        ScalarFunction::NullIf => {
            let mut arguments = arguments.into_iter();
            let left = arguments
                .next()
                .ok_or_else(|| DbError::internal("NULLIF lost its first argument"))?;
            let right = arguments
                .next()
                .ok_or_else(|| DbError::internal("NULLIF lost its second argument"))?;
            if left.is_null() || right.is_null() {
                return Ok(left);
            }
            match evaluate_binary(left.clone(), BinaryOperator::Eq, right)? {
                Value::Boolean(true) => Ok(Value::Null),
                Value::Boolean(false) | Value::Null => Ok(left),
                _ => Err(DbError::internal("NULLIF equality did not return boolean")),
            }
        }
        ScalarFunction::Concat => {
            let mut output = String::new();
            for argument in arguments {
                if !argument.is_null() {
                    output.push_str(&value_to_cast_text(&argument)?);
                }
            }
            Ok(Value::Text(output))
        }
        ScalarFunction::Greatest | ScalarFunction::Least => {
            let select_greatest = function == ScalarFunction::Greatest;
            let mut best = None;
            for argument in arguments {
                if argument.is_null() {
                    continue;
                }
                best = Some(match best {
                    None => argument,
                    Some(current) => {
                        let ordering = compare_values(&argument, &current)?;
                        if (select_greatest && ordering == Ordering::Greater)
                            || (!select_greatest && ordering == Ordering::Less)
                        {
                            argument
                        } else {
                            current
                        }
                    }
                });
            }
            Ok(best.unwrap_or(Value::Null))
        }
        _ if arguments.iter().any(Value::is_null) => Ok(Value::Null),
        ScalarFunction::Lower | ScalarFunction::Upper => {
            let Value::Text(value) = &arguments[0] else {
                return Err(DbError::new(
                    "42883",
                    "text function requires a textual argument",
                ));
            };
            Ok(Value::Text(if function == ScalarFunction::Lower {
                value.to_lowercase()
            } else {
                value.to_uppercase()
            }))
        }
        ScalarFunction::CharacterLength => match &arguments[0] {
            Value::Text(value) => i32::try_from(value.chars().count())
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "character length exceeds INTEGER range")),
            Value::Binary(value) => i32::try_from(value.len())
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "bytea length exceeds INTEGER range")),
            _ => Err(DbError::new("42883", "length requires text or bytea")),
        },
        ScalarFunction::OctetLength => match &arguments[0] {
            Value::Text(value) => i32::try_from(value.len())
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "octet length exceeds INTEGER range")),
            Value::Binary(value) => i32::try_from(value.len())
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "bytea length exceeds INTEGER range")),
            _ => Err(DbError::new("42883", "octet_length requires text or bytea")),
        },
        ScalarFunction::Abs => match arguments[0] {
            Value::Int16(value) => value
                .checked_abs()
                .map(Value::Int16)
                .ok_or_else(|| DbError::new("22003", "smallint out of range")),
            Value::Int32(value) => value
                .checked_abs()
                .map(Value::Int32)
                .ok_or_else(|| DbError::new("22003", "integer out of range")),
            Value::Int64(value) => value
                .checked_abs()
                .map(Value::Int64)
                .ok_or_else(|| DbError::new("22003", "bigint out of range")),
            Value::Float32(value) => Ok(Value::Float32(value.abs())),
            Value::Float64(value) => Ok(Value::Float64(value.abs())),
            Value::Decimal(value) => Ok(Value::Decimal(value.abs())),
            _ => Err(DbError::new("42883", "ABS requires a numeric argument")),
        },
        ScalarFunction::Substring => evaluate_substring(&arguments),
        ScalarFunction::Btrim | ScalarFunction::Ltrim | ScalarFunction::Rtrim => {
            let Value::Text(value) = &arguments[0] else {
                return Err(DbError::new("42883", "trim requires textual arguments"));
            };
            let characters = arguments
                .get(1)
                .map(|argument| match argument {
                    Value::Text(characters) => Ok(characters.as_str()),
                    _ => Err(DbError::new("42883", "trim characters must be text")),
                })
                .transpose()?;
            let trimmed = match (function, characters) {
                (ScalarFunction::Btrim, Some(characters)) => {
                    value.trim_matches(|character| characters.contains(character))
                }
                (ScalarFunction::Ltrim, Some(characters)) => {
                    value.trim_start_matches(|character| characters.contains(character))
                }
                (ScalarFunction::Rtrim, Some(characters)) => {
                    value.trim_end_matches(|character| characters.contains(character))
                }
                (ScalarFunction::Btrim, None) => value.trim(),
                (ScalarFunction::Ltrim, None) => value.trim_start(),
                (ScalarFunction::Rtrim, None) => value.trim_end(),
                _ => return Err(DbError::internal("unexpected trim function")),
            };
            Ok(Value::Text(trimmed.to_owned()))
        }
        ScalarFunction::Replace => {
            let (Value::Text(value), Value::Text(from), Value::Text(to)) =
                (&arguments[0], &arguments[1], &arguments[2])
            else {
                return Err(DbError::new(
                    "42883",
                    "replace requires three textual arguments",
                ));
            };
            Ok(Value::Text(value.replace(from, to)))
        }
        ScalarFunction::Strpos => {
            let (Value::Text(value), Value::Text(needle)) = (&arguments[0], &arguments[1]) else {
                return Err(DbError::new(
                    "42883",
                    "strpos requires two textual arguments",
                ));
            };
            let position = value.find(needle).map_or(0_usize, |byte_offset| {
                value[..byte_offset].chars().count().saturating_add(1)
            });
            i32::try_from(position)
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "text position exceeds INTEGER range"))
        }
        ScalarFunction::JsonbTypeof => {
            let Value::Jsonb(value) = &arguments[0] else {
                return Err(DbError::new("42883", "jsonb_typeof requires jsonb"));
            };
            let name = match value {
                serde_json::Value::Null => "null",
                serde_json::Value::Bool(_) => "boolean",
                serde_json::Value::Number(_) => "number",
                serde_json::Value::String(_) => "string",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Object(_) => "object",
            };
            Ok(Value::Text(name.to_owned()))
        }
        ScalarFunction::ArrayLength => {
            let (Value::Array(array), Value::Int32(dimension)) = (&arguments[0], &arguments[1])
            else {
                return Err(DbError::new(
                    "42883",
                    "array_length requires array, integer",
                ));
            };
            let Some(index) = dimension
                .checked_sub(1)
                .and_then(|value| usize::try_from(value).ok())
            else {
                return Ok(Value::Null);
            };
            array
                .dimensions()
                .get(index)
                .map_or(Ok(Value::Null), |dimension| {
                    i32::try_from(dimension.length)
                        .map(Value::Int32)
                        .map_err(|_| DbError::new("22003", "array length exceeds INTEGER range"))
                })
        }
        ScalarFunction::Cardinality => {
            let Value::Array(array) = &arguments[0] else {
                return Err(DbError::new("42883", "cardinality requires an array"));
            };
            i32::try_from(array.values().len())
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "array cardinality exceeds INTEGER range"))
        }
    }
}

fn evaluate_substring(arguments: &[Value]) -> Result<Value> {
    let (Value::Text(value), Value::Int32(start)) = (&arguments[0], &arguments[1]) else {
        return Err(DbError::new("42883", "substring requires text, integer"));
    };
    let requested_length = arguments
        .get(2)
        .map(|value| match value {
            Value::Int32(length) if *length >= 0 => Ok(i64::from(*length)),
            Value::Int32(_) => Err(DbError::new(
                "22011",
                "negative substring length not allowed",
            )),
            _ => Err(DbError::new("42883", "substring length must be integer")),
        })
        .transpose()?;
    let start = i64::from(*start) - 1;
    let end = requested_length.and_then(|length| start.checked_add(length));
    let begin = usize::try_from(start.max(0)).unwrap_or(usize::MAX);
    let take = end.map_or(usize::MAX, |end| {
        usize::try_from(end.max(0).saturating_sub(start.max(0))).unwrap_or(usize::MAX)
    });
    Ok(Value::Text(value.chars().skip(begin).take(take).collect()))
}

pub fn cast_value(value: Value, target: &ScalarType) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    if matches!(target, ScalarType::Enum { .. })
        || matches!(target, ScalarType::Array { element } if matches!(element.as_ref(), ScalarType::Enum { .. }))
    {
        return coerce_value(value, target);
    }
    if let Ok(value) = coerce_value(value.clone(), target) {
        return Ok(value);
    }
    if matches!(
        target,
        ScalarType::Text
            | ScalarType::Char { .. }
            | ScalarType::Varchar { .. }
            | ScalarType::Name
            | ScalarType::InternalChar
    ) {
        return cast_text_result(value_to_cast_text(&value)?, target);
    }
    if value.scalar_type().as_ref().is_some_and(is_numeric_type) && is_numeric_type(target) {
        return cast_numeric(value, target);
    }
    match (value, target) {
        (Value::Text(value), ScalarType::Boolean) => parse_boolean_text(&value).map(Value::Boolean),
        (Value::Text(value), target) if is_numeric_type(target) => {
            cast_numeric_text(&value, target)
        }
        (Value::Text(value), ScalarType::Binary) => parse_bytea_text(&value).map(Value::Binary),
        (Value::Text(value), ScalarType::Date) => NaiveDate::parse_from_str(&value, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|_| invalid_text_cast(&value, target)),
        (Value::Text(value), ScalarType::Time) => NaiveTime::parse_from_str(&value, "%H:%M:%S%.f")
            .map(Value::Time)
            .map_err(|_| invalid_text_cast(&value, target)),
        (Value::Text(value), ScalarType::Timestamp { .. }) => {
            NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%.f"))
                .map(Value::Timestamp)
                .map_err(|_| invalid_text_cast(&value, target))
        }
        (Value::Text(value), ScalarType::Interval) => PgInterval::from_str(&value)
            .map(Value::Interval)
            .map_err(|error| DbError::new(error.sql_state, error.message)),
        (Value::Text(value), ScalarType::Json) => serde_json::from_str(&value)
            .map(Value::Json)
            .map_err(|_| invalid_text_cast(&value, target)),
        (Value::Text(value), ScalarType::Jsonb) => serde_json::from_str(&value)
            .map(Value::Jsonb)
            .map_err(|_| invalid_text_cast(&value, target)),
        (Value::Text(value), ScalarType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(Value::Uuid)
            .map_err(|_| invalid_text_cast(&value, target)),
        (Value::Date(value), ScalarType::Timestamp { .. }) => value
            .and_hms_opt(0, 0, 0)
            .map(Value::Timestamp)
            .ok_or_else(|| DbError::new("22008", "date is outside timestamp range")),
        (Value::Timestamp(value), ScalarType::Date) => Ok(Value::Date(value.date())),
        (Value::Timestamp(value), ScalarType::Time) => Ok(Value::Time(value.time())),
        (Value::Timestamp(value), ScalarType::Timestamp { .. }) => Ok(Value::Timestamp(value)),
        (Value::Json(value), ScalarType::Jsonb) => Ok(Value::Jsonb(value)),
        (Value::Jsonb(value), ScalarType::Json) => Ok(Value::Json(value)),
        (Value::Array(value), ScalarType::Array { element }) => {
            let dimensions = value.dimensions().to_vec();
            let values = value
                .values()
                .iter()
                .cloned()
                .map(|value| cast_value(value, element))
                .collect::<Result<Vec<_>>>()?;
            PgArray::new(element.as_ref().clone(), dimensions, values).map(Value::Array)
        }
        (value, target) => Err(DbError::new(
            "42846",
            format!("cannot cast value {value:?} to {target:?}"),
        )),
    }
}

fn is_numeric_type(data_type: &ScalarType) -> bool {
    matches!(
        data_type,
        ScalarType::Int16
            | ScalarType::Int32
            | ScalarType::Int64
            | ScalarType::Oid
            | ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Decimal { .. }
    )
}

fn cast_numeric(value: Value, target: &ScalarType) -> Result<Value> {
    match target {
        ScalarType::Int16 => numeric_to_i128(&value)
            .and_then(|value| i16::try_from(value).map_err(|_| cast_numeric_out_of_range(target)))
            .map(Value::Int16),
        ScalarType::Int32 => numeric_to_i128(&value)
            .and_then(|value| i32::try_from(value).map_err(|_| cast_numeric_out_of_range(target)))
            .map(Value::Int32),
        ScalarType::Int64 => numeric_to_i128(&value)
            .and_then(|value| i64::try_from(value).map_err(|_| cast_numeric_out_of_range(target)))
            .map(Value::Int64),
        ScalarType::Oid => numeric_to_i128(&value)
            .and_then(|value| u32::try_from(value).map_err(|_| cast_numeric_out_of_range(target)))
            .map(|value| Value::Int64(i64::from(value))),
        ScalarType::Float32 => {
            let value = numeric_to_f64(&value)?;
            let narrowed = value as f32;
            if narrowed.is_finite() || value == 0.0 {
                Ok(Value::Float32(narrowed))
            } else {
                Err(cast_numeric_out_of_range(target))
            }
        }
        ScalarType::Float64 => numeric_to_f64(&value).map(Value::Float64),
        ScalarType::Decimal { .. } => numeric_to_decimal(&value).map(Value::Decimal),
        _ => Err(DbError::internal(
            "numeric cast received a non-numeric target",
        )),
    }
}

fn numeric_to_i128(value: &Value) -> Result<i128> {
    match value {
        Value::Int16(value) => Ok(i128::from(*value)),
        Value::Int32(value) => Ok(i128::from(*value)),
        Value::Int64(value) => Ok(i128::from(*value)),
        Value::Float32(value) if value.is_finite() => (*value as f64)
            .round()
            .to_i128()
            .ok_or_else(|| cast_numeric_out_of_range(&ScalarType::Int64)),
        Value::Float64(value) if value.is_finite() => value
            .round()
            .to_i128()
            .ok_or_else(|| cast_numeric_out_of_range(&ScalarType::Int64)),
        Value::Decimal(value) => value
            .round()
            .to_i128()
            .ok_or_else(|| cast_numeric_out_of_range(&ScalarType::Int64)),
        _ => Err(cast_numeric_out_of_range(&ScalarType::Int64)),
    }
}

fn numeric_to_f64(value: &Value) -> Result<f64> {
    let value = match value {
        Value::Int16(value) => f64::from(*value),
        Value::Int32(value) => f64::from(*value),
        Value::Int64(value) => *value as f64,
        Value::Float32(value) => f64::from(*value),
        Value::Float64(value) => *value,
        Value::Decimal(value) => value
            .to_f64()
            .ok_or_else(|| cast_numeric_out_of_range(&ScalarType::Float64))?,
        _ => {
            return Err(DbError::internal(
                "numeric conversion received a non-numeric value",
            ));
        }
    };
    if value.is_finite() {
        Ok(value)
    } else {
        Err(cast_numeric_out_of_range(&ScalarType::Float64))
    }
}
