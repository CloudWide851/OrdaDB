
fn numeric_to_decimal(value: &Value) -> Result<Decimal> {
    match value {
        Value::Int16(value) => Ok(Decimal::from(*value)),
        Value::Int32(value) => Ok(Decimal::from(*value)),
        Value::Int64(value) => Ok(Decimal::from(*value)),
        Value::Float32(value) => Decimal::from_f32(*value).ok_or_else(|| {
            cast_numeric_out_of_range(&ScalarType::Decimal {
                precision: None,
                scale: None,
            })
        }),
        Value::Float64(value) => Decimal::from_f64(*value).ok_or_else(|| {
            cast_numeric_out_of_range(&ScalarType::Decimal {
                precision: None,
                scale: None,
            })
        }),
        Value::Decimal(value) => Ok(*value),
        _ => Err(DbError::internal(
            "decimal conversion received a non-numeric value",
        )),
    }
}

fn cast_numeric_text(value: &str, target: &ScalarType) -> Result<Value> {
    let invalid = || invalid_text_cast(value, target);
    match target {
        ScalarType::Int16 => value
            .parse::<i16>()
            .map(Value::Int16)
            .map_err(|_| invalid()),
        ScalarType::Int32 => value
            .parse::<i32>()
            .map(Value::Int32)
            .map_err(|_| invalid()),
        ScalarType::Int64 => value
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|_| invalid()),
        ScalarType::Oid => value
            .parse::<u32>()
            .map(|value| Value::Int64(i64::from(value)))
            .map_err(|_| invalid()),
        ScalarType::Float32 => value
            .parse::<f32>()
            .map_err(|_| invalid())
            .and_then(|value| {
                value
                    .is_finite()
                    .then_some(Value::Float32(value))
                    .ok_or_else(invalid)
            }),
        ScalarType::Float64 => value
            .parse::<f64>()
            .map_err(|_| invalid())
            .and_then(|value| {
                value
                    .is_finite()
                    .then_some(Value::Float64(value))
                    .ok_or_else(invalid)
            }),
        ScalarType::Decimal { .. } => Decimal::from_str(value)
            .map(Value::Decimal)
            .map_err(|_| invalid()),
        _ => Err(DbError::internal(
            "text numeric cast received a non-numeric target",
        )),
    }
}

fn parse_boolean_text(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Ok(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Ok(false),
        _ => Err(DbError::new(
            "22P02",
            format!("invalid input syntax for type boolean: {value:?}"),
        )),
    }
}

fn cast_text_result(value: String, target: &ScalarType) -> Result<Value> {
    let limit = match target {
        ScalarType::Char { length } | ScalarType::Varchar { length } => *length,
        ScalarType::Text => None,
        ScalarType::Name => {
            if value.len() > MAX_POSTGRES_NAME_BYTES {
                return Err(DbError::new("22001", "PostgreSQL name exceeds 63 bytes"));
            }
            None
        }
        ScalarType::InternalChar => {
            if value.len() != 1 {
                return Err(DbError::new(
                    "22001",
                    "PostgreSQL internal char must contain exactly one byte",
                ));
            }
            None
        }
        _ => return Err(DbError::internal("text cast received a non-text target")),
    };
    let value = limit.map_or(value.clone(), |length| {
        value
            .chars()
            .take(usize::try_from(length).unwrap_or(usize::MAX))
            .collect()
    });
    Ok(Value::Text(value))
}

fn value_to_cast_text(value: &Value) -> Result<String> {
    match value {
        Value::Null => Ok(String::new()),
        Value::Boolean(value) => Ok(if *value { "true" } else { "false" }.to_owned()),
        Value::Int16(value) => Ok(value.to_string()),
        Value::Int32(value) => Ok(value.to_string()),
        Value::Int64(value) => Ok(value.to_string()),
        Value::Float32(value) => Ok(value.to_string()),
        Value::Float64(value) => Ok(value.to_string()),
        Value::Decimal(value) => Ok(value.to_string()),
        Value::Text(value) => Ok(value.clone()),
        Value::Binary(value) => Ok(format_bytea(value)),
        Value::Date(value) => Ok(value.format("%Y-%m-%d").to_string()),
        Value::Time(value) => Ok(value.format("%H:%M:%S%.f").to_string()),
        Value::Timestamp(value) => Ok(value.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
        Value::Interval(value) => Ok(value.to_string()),
        Value::Array(value) => array_to_text(value),
        Value::Json(value) | Value::Jsonb(value) => serde_json::to_string(value)
            .map_err(|error| DbError::internal(format!("JSON serialization failed: {error}"))),
        Value::Uuid(value) => Ok(value.to_string()),
        Value::Vector(values) => Ok(format!(
            "[{}]",
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )),
    }
}

fn parse_bytea_text(value: &str) -> Result<Vec<u8>> {
    let Some(hex) = value.strip_prefix("\\x") else {
        return Ok(value.as_bytes().to_vec());
    };
    if hex.len() % 2 != 0 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DbError::new("22P02", "invalid hexadecimal bytea input"));
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)
                .map_err(|_| DbError::new("22P02", "invalid hexadecimal bytea input"))?;
            u8::from_str_radix(text, 16)
                .map_err(|_| DbError::new("22P02", "invalid hexadecimal bytea input"))
        })
        .collect()
}

fn format_bytea(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len().saturating_mul(2).saturating_add(2));
    output.push_str("\\x");
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn array_to_text(array: &PgArray) -> Result<String> {
    if array.dimensions().is_empty() {
        return Ok("{}".to_owned());
    }
    let mut output = String::new();
    if array
        .dimensions()
        .iter()
        .any(|dimension| dimension.lower_bound != 1)
    {
        for dimension in array.dimensions() {
            let upper = i64::from(dimension.lower_bound)
                .checked_add(i64::from(dimension.length))
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| DbError::new("2202E", "array bounds overflow"))?;
            output.push_str(&format!("[{}:{upper}]", dimension.lower_bound));
        }
        output.push('=');
    }
    let mut cursor = 0_usize;
    write_array_dimension(array, 0, &mut cursor, &mut output)?;
    Ok(output)
}

fn write_array_dimension(
    array: &PgArray,
    dimension: usize,
    cursor: &mut usize,
    output: &mut String,
) -> Result<()> {
    let current = array
        .dimensions()
        .get(dimension)
        .ok_or_else(|| DbError::internal("array text writer lost its dimension"))?;
    output.push('{');
    for index in 0..current.length {
        if index > 0 {
            output.push(',');
        }
        if dimension + 1 < array.dimensions().len() {
            write_array_dimension(array, dimension + 1, cursor, output)?;
        } else {
            let value = array
                .values()
                .get(*cursor)
                .ok_or_else(|| DbError::internal("array text writer exceeded its values"))?;
            *cursor += 1;
            if value.is_null() {
                output.push_str("NULL");
            } else {
                let text = value_to_cast_text(value)?;
                output.push('"');
                for character in text.chars() {
                    if matches!(character, '"' | '\\') {
                        output.push('\\');
                    }
                    output.push(character);
                }
                output.push('"');
            }
        }
    }
    output.push('}');
    Ok(())
}

fn invalid_text_cast(value: &str, target: &ScalarType) -> DbError {
    DbError::new(
        "22P02",
        format!("invalid input syntax for type {target:?}: {value:?}"),
    )
}

fn cast_numeric_out_of_range(target: &ScalarType) -> DbError {
    DbError::new(
        "22003",
        format!("numeric value is out of range for {target:?}"),
    )
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
        (Value::Int16(value), ScalarType::Oid) => u32::try_from(value)
            .map(|value| Value::Int64(i64::from(value)))
            .map_err(|_| cast_numeric_out_of_range(target)),
        (Value::Int16(value), ScalarType::Float32) => Ok(Value::Float32(f32::from(value))),
        (Value::Int16(value), ScalarType::Float64) => Ok(Value::Float64(f64::from(value))),
        (Value::Int16(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Int32(value), ScalarType::Int32) => Ok(Value::Int32(value)),
        (Value::Int32(value), ScalarType::Int64) => Ok(Value::Int64(i64::from(value))),
        (Value::Int32(value), ScalarType::Oid) => u32::try_from(value)
            .map(|value| Value::Int64(i64::from(value)))
            .map_err(|_| cast_numeric_out_of_range(target)),
        (Value::Int32(value), ScalarType::Float64) => Ok(Value::Float64(f64::from(value))),
        (Value::Int32(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Int64(value), ScalarType::Int64) => Ok(Value::Int64(value)),
        (Value::Int64(value), ScalarType::Oid) => u32::try_from(value)
            .map(|value| Value::Int64(i64::from(value)))
            .map_err(|_| cast_numeric_out_of_range(target)),
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
        (Value::Text(value), ScalarType::Name) if value.len() <= MAX_POSTGRES_NAME_BYTES => {
            Ok(Value::Text(value))
        }
        (Value::Text(_), ScalarType::Name) => {
            Err(DbError::new("22001", "PostgreSQL name exceeds 63 bytes"))
        }
        (Value::Text(value), ScalarType::InternalChar) if value.len() == 1 => {
            Ok(Value::Text(value))
        }
        (Value::Text(_), ScalarType::InternalChar) => Err(DbError::new(
            "22001",
            "PostgreSQL internal char must contain exactly one byte",
        )),
        (Value::Text(value), ScalarType::Enum { labels, .. }) => {
            if labels.iter().any(|label| label == &value) {
                Ok(Value::Text(value))
            } else {
                Err(DbError::new(
                    "22P02",
                    format!("invalid input value for enum: {value}"),
                ))
            }
        }
        (Value::Binary(value), ScalarType::Binary) => Ok(Value::Binary(value)),
        (Value::Date(value), ScalarType::Date) => Ok(Value::Date(value)),
        (Value::Time(value), ScalarType::Time) => Ok(Value::Time(value)),
        (
            Value::Timestamp(value),
            ScalarType::Timestamp {
                with_timezone: false,
            },
        ) => Ok(Value::Timestamp(value)),
        (
            Value::Timestamp(value),
            ScalarType::Timestamp {
                with_timezone: true,
            },
        ) => Ok(Value::Timestamp(value)),
        (Value::Interval(value), ScalarType::Interval) => Ok(Value::Interval(value)),
        (Value::Array(value), ScalarType::Array { element }) => {
            let dimensions = value.dimensions().to_vec();
            let values = value
                .values()
                .iter()
                .cloned()
                .map(|value| coerce_value(value, element))
                .collect::<Result<Vec<_>>>()?;
            ordadb_types::PgArray::new(element.as_ref().clone(), dimensions, values)
                .map(Value::Array)
        }
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
                let ordering = compare_values_as(left_value, right_value, &order.data_type)?;
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
        (Value::Interval(left), Value::Interval(right)) => {
            Ok(interval_comparison_key(*left).cmp(&interval_comparison_key(*right)))
        }
        (Value::Array(left), Value::Array(right)) => compare_arrays(left, right),
        (Value::Uuid(left), Value::Uuid(right)) => Ok(left.cmp(right)),
        _ => Err(DbError::new(
            "42883",
            "values do not have a compatible ordering operator",
        )),
    }
}

pub fn compare_values_as(left: &Value, right: &Value, data_type: &ScalarType) -> Result<Ordering> {
    match data_type {
        ScalarType::Enum { labels, .. } => {
            let (Value::Text(left), Value::Text(right)) = (left, right) else {
                return Err(DbError::new(
                    "42804",
                    "enum comparison requires enum text values",
                ));
            };
            let left = labels
                .iter()
                .position(|label| label == left)
                .ok_or_else(|| {
                    DbError::new("22P02", format!("invalid input value for enum: {left}"))
                })?;
            let right = labels
                .iter()
                .position(|label| label == right)
                .ok_or_else(|| {
                    DbError::new("22P02", format!("invalid input value for enum: {right}"))
                })?;
            Ok(left.cmp(&right))
        }
        ScalarType::Array { element } => compare_arrays_as(left, right, element),
        _ => compare_values(left, right),
    }
}

fn compare_arrays_as(left: &Value, right: &Value, element: &ScalarType) -> Result<Ordering> {
    let (Value::Array(left), Value::Array(right)) = (left, right) else {
        return Err(DbError::new(
            "42804",
            "array comparison requires array values",
        ));
    };
    if left.element_type() != element || right.element_type() != element {
        return Err(DbError::new(
            "42883",
            "arrays do not have compatible element types",
        ));
    }
    for (left, right) in left.values().iter().zip(right.values()) {
        let ordering = match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => compare_values_as(left, right, element)?,
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    let length_ordering = left.values().len().cmp(&right.values().len());
    if length_ordering != Ordering::Equal {
        return Ok(length_ordering);
    }
    Ok(left
        .dimensions()
        .iter()
        .map(|dimension| (dimension.length, dimension.lower_bound))
        .cmp(
            right
                .dimensions()
                .iter()
                .map(|dimension| (dimension.length, dimension.lower_bound)),
        ))
}

fn interval_comparison_key(value: ordadb_types::PgInterval) -> i128 {
    const MICROS_PER_DAY: i128 = 86_400_000_000;
    (i128::from(value.months) * 30 + i128::from(value.days)) * MICROS_PER_DAY
        + i128::from(value.microseconds)
}

fn compare_arrays(left: &ordadb_types::PgArray, right: &ordadb_types::PgArray) -> Result<Ordering> {
    if left.element_type() != right.element_type() {
        return Err(DbError::new(
            "42883",
            "arrays do not have compatible element types",
        ));
    }
    for (left, right) in left.values().iter().zip(right.values()) {
        let ordering = match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => compare_values(left, right)?,
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    let length_ordering = left.values().len().cmp(&right.values().len());
    if length_ordering != Ordering::Equal {
        return Ok(length_ordering);
    }
    Ok(left
        .dimensions()
        .iter()
        .map(|dimension| (dimension.length, dimension.lower_bound))
        .cmp(
            right
                .dimensions()
                .iter()
                .map(|dimension| (dimension.length, dimension.lower_bound)),
        ))
}
