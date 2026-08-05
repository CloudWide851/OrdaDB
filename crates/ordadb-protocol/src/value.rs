use std::io::Write;
use std::str::FromStr;

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use ordadb_types::{
    ArrayDimension, DbError, MAX_ARRAY_DIMENSIONS, MAX_ARRAY_ELEMENTS, PgArray, PgInterval, Result,
    Row, ScalarType, Schema, TypeId, Value,
};

use crate::codec::{protocol, push_cstring, write_message};

pub const OID_BOOL: u32 = 16;
pub const OID_BYTEA: u32 = 17;
pub const OID_INT8: u32 = 20;
pub const OID_INT2: u32 = 21;
pub const OID_INT4: u32 = 23;
pub const OID_TEXT: u32 = 25;
pub const OID_FLOAT4: u32 = 700;
pub const OID_FLOAT8: u32 = 701;
pub const OID_BPCHAR: u32 = 1042;
pub const OID_VARCHAR: u32 = 1043;
pub const OID_DATE: u32 = 1082;
pub const OID_TIME: u32 = 1083;
pub const OID_TIMESTAMP: u32 = 1114;
pub const OID_TIMESTAMPTZ: u32 = 1184;
pub const OID_INTERVAL: u32 = 1186;
pub const OID_NUMERIC: u32 = 1700;
pub const OID_JSON: u32 = 114;
pub const OID_UUID: u32 = 2950;
pub const OID_JSONB: u32 = 3802;
pub const OID_BOOL_ARRAY: u32 = 1000;
pub const OID_BYTEA_ARRAY: u32 = 1001;
pub const OID_INT2_ARRAY: u32 = 1005;
pub const OID_INT4_ARRAY: u32 = 1007;
pub const OID_TEXT_ARRAY: u32 = 1009;
pub const OID_BPCHAR_ARRAY: u32 = 1014;
pub const OID_VARCHAR_ARRAY: u32 = 1015;
pub const OID_INT8_ARRAY: u32 = 1016;
pub const OID_FLOAT4_ARRAY: u32 = 1021;
pub const OID_FLOAT8_ARRAY: u32 = 1022;
pub const OID_JSON_ARRAY: u32 = 199;
pub const OID_TIMESTAMP_ARRAY: u32 = 1115;
pub const OID_DATE_ARRAY: u32 = 1182;
pub const OID_TIME_ARRAY: u32 = 1183;
pub const OID_TIMESTAMPTZ_ARRAY: u32 = 1185;
pub const OID_INTERVAL_ARRAY: u32 = 1187;
pub const OID_NUMERIC_ARRAY: u32 = 1231;
pub const OID_UUID_ARRAY: u32 = 2951;
pub const OID_JSONB_ARRAY: u32 = 3807;
pub const OID_USER_DEFINED_ENUM_BASE: u32 = 16_384;

const POSTGRES_EPOCH_DATE: (i32, u32, u32) = (2000, 1, 1);
const NUMERIC_POSITIVE: u16 = 0x0000;
const NUMERIC_NEGATIVE: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;

pub fn resolve_formats(formats: &[i16], count: usize) -> Result<Vec<i16>> {
    let resolved = match formats {
        [] => vec![0; count],
        [format] => vec![*format; count],
        values if values.len() == count => values.to_vec(),
        _ => {
            return Err(protocol(format!(
                "format count {} does not match value count {count}",
                formats.len()
            )));
        }
    };
    if resolved.iter().any(|format| !matches!(format, 0 | 1)) {
        return Err(protocol("format code must be zero (text) or one (binary)"));
    }
    Ok(resolved)
}

pub fn decode_parameters_as(
    oids: &[u32],
    data_types: &[ScalarType],
    formats: &[i16],
    parameters: &[Option<Vec<u8>>],
) -> Result<Vec<Value>> {
    if data_types.len() != parameters.len() {
        return Err(protocol(format!(
            "inferred parameter count {} does not match bound count {}",
            data_types.len(),
            parameters.len()
        )));
    }
    if !oids.is_empty() && oids.len() != parameters.len() {
        return Err(protocol(format!(
            "declared parameter count {} does not match bound count {}",
            oids.len(),
            parameters.len()
        )));
    }
    let formats = resolve_formats(formats, parameters.len())?;
    parameters
        .iter()
        .zip(data_types)
        .enumerate()
        .map(|(index, (parameter, data_type))| {
            let oid = oids.get(index).copied().unwrap_or(0);
            match parameter {
                None => Ok(Value::Null),
                Some(bytes) => decode_parameter_as(oid, formats[index], bytes, data_type),
            }
        })
        .collect()
}

pub fn write_row_description<W: Write>(
    writer: &mut W,
    schema: &Schema,
    result_formats: &[i16],
) -> Result<()> {
    let formats = resolve_formats(result_formats, schema.fields.len())?;
    let field_count = u16::try_from(schema.fields.len())
        .map_err(|_| protocol("result field count exceeds u16"))?;
    let mut payload = Vec::new();
    payload.extend_from_slice(&field_count.to_be_bytes());
    for (field, format) in schema.fields.iter().zip(formats) {
        push_cstring(&mut payload, &field.name)?;
        payload.extend_from_slice(&0_u32.to_be_bytes());
        payload.extend_from_slice(&0_u16.to_be_bytes());
        payload.extend_from_slice(&type_oid(&field.data_type).to_be_bytes());
        payload.extend_from_slice(&type_size(&field.data_type).to_be_bytes());
        payload.extend_from_slice(&type_modifier(&field.data_type).to_be_bytes());
        payload.extend_from_slice(&format.to_be_bytes());
    }
    write_message(writer, b'T', &payload)
}

pub fn write_data_row<W: Write>(
    writer: &mut W,
    schema: &Schema,
    row: &Row,
    result_formats: &[i16],
) -> Result<()> {
    if schema.fields.len() != row.values.len() {
        return Err(DbError::new(
            "XX000",
            "query row width does not match its schema",
        ));
    }
    let formats = resolve_formats(result_formats, row.values.len())?;
    let field_count =
        u16::try_from(row.values.len()).map_err(|_| protocol("result row width exceeds u16"))?;
    let mut payload = Vec::new();
    payload.extend_from_slice(&field_count.to_be_bytes());
    for ((value, field), format) in row.values.iter().zip(&schema.fields).zip(formats) {
        if matches!(value, Value::Null) {
            payload.extend_from_slice(&(-1_i32).to_be_bytes());
            continue;
        }
        validate_enum_value(value, &field.data_type)?;
        let bytes = if format == 0 {
            encode_text(value)?
        } else {
            encode_binary(value, &field.data_type)?
        };
        let length =
            i32::try_from(bytes.len()).map_err(|_| protocol("result value exceeds i32"))?;
        payload.extend_from_slice(&length.to_be_bytes());
        payload.extend_from_slice(&bytes);
    }
    write_message(writer, b'D', &payload)
}

pub fn type_oid(data_type: &ScalarType) -> u32 {
    match data_type {
        ScalarType::Boolean => OID_BOOL,
        ScalarType::Int16 => OID_INT2,
        ScalarType::Int32 => OID_INT4,
        ScalarType::Int64 => OID_INT8,
        ScalarType::Float32 => OID_FLOAT4,
        ScalarType::Float64 => OID_FLOAT8,
        ScalarType::Decimal { .. } => OID_NUMERIC,
        ScalarType::Char { .. } => OID_BPCHAR,
        ScalarType::Varchar { .. } => OID_VARCHAR,
        ScalarType::Text | ScalarType::Vector { .. } => OID_TEXT,
        ScalarType::Enum { type_id, .. } => enum_type_oid(*type_id),
        ScalarType::Binary => OID_BYTEA,
        ScalarType::Date => OID_DATE,
        ScalarType::Time => OID_TIME,
        ScalarType::Timestamp {
            with_timezone: false,
        } => OID_TIMESTAMP,
        ScalarType::Timestamp {
            with_timezone: true,
        } => OID_TIMESTAMPTZ,
        ScalarType::Interval => OID_INTERVAL,
        ScalarType::Array { element } => array_oid(element).unwrap_or(0),
        ScalarType::Json => OID_JSON,
        ScalarType::Jsonb => OID_JSONB,
        ScalarType::Uuid => OID_UUID,
    }
}

#[must_use]
pub fn enum_type_oid(type_id: TypeId) -> u32 {
    user_defined_type_oid(type_id)
}

#[must_use]
pub fn enum_array_oid(type_id: TypeId) -> u32 {
    user_defined_array_oid(type_id)
}

/// Return the stable PostgreSQL-compatible pseudo OID for a Catalog-owned
/// enum or domain type.
#[must_use]
pub fn user_defined_type_oid(type_id: TypeId) -> u32 {
    user_defined_type_oid_with_offset(type_id, 0).unwrap_or(0)
}

/// Return the stable pseudo OID for the automatically projected array type of
/// a Catalog-owned enum or domain.
#[must_use]
pub fn user_defined_array_oid(type_id: TypeId) -> u32 {
    user_defined_type_oid_with_offset(type_id, 1).unwrap_or(0)
}

fn user_defined_type_oid_with_offset(type_id: TypeId, array_offset: u32) -> Option<u32> {
    let ordinal = type_id.get().checked_sub(1)?;
    let ordinal = u32::try_from(ordinal).ok()?;
    OID_USER_DEFINED_ENUM_BASE
        .checked_add(ordinal.checked_mul(2)?)?
        .checked_add(array_offset)
}

fn enum_type_id_from_oid(oid: u32, array: bool) -> Option<TypeId> {
    let offset = oid.checked_sub(OID_USER_DEFINED_ENUM_BASE)?;
    if offset % 2 != u32::from(array) {
        return None;
    }
    Some(TypeId::new(u64::from(offset / 2) + 1))
}

fn type_size(data_type: &ScalarType) -> i16 {
    match data_type {
        ScalarType::Boolean => 1,
        ScalarType::Int16 => 2,
        ScalarType::Int32 | ScalarType::Float32 | ScalarType::Date => 4,
        ScalarType::Int64
        | ScalarType::Float64
        | ScalarType::Time
        | ScalarType::Timestamp { .. } => 8,
        ScalarType::Interval => 16,
        ScalarType::Uuid => 16,
        _ => -1,
    }
}

fn type_modifier(data_type: &ScalarType) -> i32 {
    match data_type {
        ScalarType::Char {
            length: Some(length),
        }
        | ScalarType::Varchar {
            length: Some(length),
        } => i32::try_from(*length)
            .ok()
            .and_then(|length| length.checked_add(4))
            .unwrap_or(-1),
        ScalarType::Decimal {
            precision: Some(precision),
            scale: Some(scale),
        } => (i32::from(*precision) << 16) | i32::from(*scale) | 4,
        _ => -1,
    }
}

fn decode_parameter_as(
    oid: u32,
    format: i16,
    bytes: &[u8],
    data_type: &ScalarType,
) -> Result<Value> {
    let expected_oid = type_oid(data_type);
    if oid != 0 && oid != expected_oid {
        return if format == 0 {
            decode_text(oid, bytes)
        } else {
            decode_binary(oid, bytes)
        };
    }
    match data_type {
        ScalarType::Enum { labels, .. } => decode_enum(bytes, labels),
        ScalarType::Array { element } if matches!(element.as_ref(), ScalarType::Enum { .. }) => {
            let element_oid = type_oid(element);
            let value = if format == 0 {
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| DbError::new("22021", "text parameter is not valid UTF-8"))?;
                decode_array_text(text, element.as_ref().clone(), element_oid)
            } else {
                decode_array_binary(bytes, element.as_ref().clone(), element_oid)
            }?;
            validate_enum_value(&value, data_type)?;
            Ok(value)
        }
        _ if format == 0 => decode_text(expected_oid, bytes),
        _ => decode_binary(expected_oid, bytes),
    }
}

fn decode_enum(bytes: &[u8], labels: &[String]) -> Result<Value> {
    let value = std::str::from_utf8(bytes)
        .map_err(|_| DbError::new("22021", "enum parameter is not valid UTF-8"))?;
    if !labels.iter().any(|label| label == value) {
        return Err(DbError::new(
            "22P02",
            format!("invalid input value for enum: {value}"),
        ));
    }
    Ok(Value::Text(value.to_owned()))
}

fn validate_enum_value(value: &Value, data_type: &ScalarType) -> Result<()> {
    match (value, data_type) {
        (Value::Null, _) => Ok(()),
        (Value::Text(label), ScalarType::Enum { labels, .. }) => {
            if labels.iter().any(|candidate| candidate == label) {
                Ok(())
            } else {
                Err(DbError::new(
                    "22P02",
                    format!("invalid input value for enum: {label}"),
                ))
            }
        }
        (Value::Array(array), ScalarType::Array { element })
            if matches!(element.as_ref(), ScalarType::Enum { .. }) =>
        {
            for value in array.values() {
                validate_enum_value(value, element)?;
            }
            Ok(())
        }
        (_, ScalarType::Enum { .. }) => {
            Err(DbError::new("42804", "enum value must use its text label"))
        }
        (_, ScalarType::Array { element })
            if matches!(element.as_ref(), ScalarType::Enum { .. }) =>
        {
            Err(DbError::new("42804", "enum array value is required"))
        }
        _ => Ok(()),
    }
}

fn decode_text(oid: u32, bytes: &[u8]) -> Result<Value> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| DbError::new("22021", "text parameter is not valid UTF-8"))?;
    let invalid = || {
        DbError::new(
            "22P02",
            format!("parameter is not valid for PostgreSQL type OID {oid}"),
        )
    };
    match oid {
        0 | OID_TEXT | OID_BPCHAR | OID_VARCHAR => Ok(Value::Text(text.to_owned())),
        oid if enum_type_id_from_oid(oid, false).is_some() => Ok(Value::Text(text.to_owned())),
        OID_BOOL => match text.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" => Ok(Value::Boolean(true)),
            "f" | "false" | "0" => Ok(Value::Boolean(false)),
            _ => Err(invalid()),
        },
        OID_INT2 => text.parse().map(Value::Int16).map_err(|_| invalid()),
        OID_INT4 => text.parse().map(Value::Int32).map_err(|_| invalid()),
        OID_INT8 => text.parse().map(Value::Int64).map_err(|_| invalid()),
        OID_FLOAT4 => text.parse().map(Value::Float32).map_err(|_| invalid()),
        OID_FLOAT8 => text.parse().map(Value::Float64).map_err(|_| invalid()),
        OID_NUMERIC => Decimal::from_str(text)
            .map(Value::Decimal)
            .map_err(|_| invalid()),
        OID_BYTEA => decode_bytea_text(text).map(Value::Binary),
        OID_DATE => NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|_| invalid()),
        OID_TIME => NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
            .map(Value::Time)
            .map_err(|_| invalid()),
        OID_TIMESTAMP | OID_TIMESTAMPTZ => parse_timestamp(text)
            .map(Value::Timestamp)
            .ok_or_else(invalid),
        OID_INTERVAL => text.parse::<PgInterval>().map(Value::Interval),
        OID_JSON => serde_json::from_str(text)
            .map(Value::Json)
            .map_err(|_| invalid()),
        OID_JSONB => serde_json::from_str(text)
            .map(Value::Jsonb)
            .map_err(|_| invalid()),
        OID_UUID => Uuid::parse_str(text)
            .map(Value::Uuid)
            .map_err(|_| invalid()),
        oid if array_element_type(oid).is_some() => {
            let (element_type, element_oid) = array_element_type(oid).ok_or_else(invalid)?;
            decode_array_text(text, element_type, element_oid)
        }
        _ => Err(DbError::new(
            "0A000",
            format!("text parameter type OID {oid} is unsupported"),
        )),
    }
}

fn decode_binary(oid: u32, bytes: &[u8]) -> Result<Value> {
    let length = |expected: usize| {
        (bytes.len() == expected).then_some(()).ok_or_else(|| {
            protocol(format!(
                "binary parameter OID {oid} expected {expected} bytes, received {}",
                bytes.len()
            ))
        })
    };
    match oid {
        OID_BOOL => {
            length(1)?;
            match bytes[0] {
                0 => Ok(Value::Boolean(false)),
                1 => Ok(Value::Boolean(true)),
                _ => Err(protocol("binary boolean must be zero or one")),
            }
        }
        OID_INT2 => {
            length(2)?;
            Ok(Value::Int16(i16::from_be_bytes(
                bytes.try_into().expect("checked length"),
            )))
        }
        OID_INT4 => {
            length(4)?;
            Ok(Value::Int32(i32::from_be_bytes(
                bytes.try_into().expect("checked length"),
            )))
        }
        OID_INT8 => {
            length(8)?;
            Ok(Value::Int64(i64::from_be_bytes(
                bytes.try_into().expect("checked length"),
            )))
        }
        OID_FLOAT4 => {
            length(4)?;
            Ok(Value::Float32(f32::from_bits(u32::from_be_bytes(
                bytes.try_into().expect("checked length"),
            ))))
        }
        OID_FLOAT8 => {
            length(8)?;
            Ok(Value::Float64(f64::from_bits(u64::from_be_bytes(
                bytes.try_into().expect("checked length"),
            ))))
        }
        OID_NUMERIC => decode_numeric_binary(bytes).map(Value::Decimal),
        OID_TEXT | OID_BPCHAR | OID_VARCHAR => std::str::from_utf8(bytes)
            .map(|text| Value::Text(text.to_owned()))
            .map_err(|_| DbError::new("22021", "binary text is not valid UTF-8")),
        oid if enum_type_id_from_oid(oid, false).is_some() => std::str::from_utf8(bytes)
            .map(|text| Value::Text(text.to_owned()))
            .map_err(|_| DbError::new("22021", "binary enum is not valid UTF-8")),
        OID_BYTEA => Ok(Value::Binary(bytes.to_vec())),
        OID_DATE => {
            length(4)?;
            let days = i32::from_be_bytes(bytes.try_into().expect("checked length"));
            let epoch = postgres_epoch_date()?;
            epoch
                .checked_add_signed(Duration::days(i64::from(days)))
                .map(Value::Date)
                .ok_or_else(|| DbError::new("22008", "binary date is out of range"))
        }
        OID_TIME => {
            length(8)?;
            let micros = i64::from_be_bytes(bytes.try_into().expect("checked length"));
            if !(0..86_400_000_000).contains(&micros) {
                return Err(DbError::new("22008", "binary time is out of range"));
            }
            NaiveTime::from_num_seconds_from_midnight_opt(
                u32::try_from(micros / 1_000_000).expect("bounded seconds"),
                u32::try_from((micros % 1_000_000) * 1_000).expect("bounded nanos"),
            )
            .map(Value::Time)
            .ok_or_else(|| DbError::new("22008", "binary time is out of range"))
        }
        OID_TIMESTAMP | OID_TIMESTAMPTZ => {
            length(8)?;
            let micros = i64::from_be_bytes(bytes.try_into().expect("checked length"));
            postgres_epoch_timestamp()?
                .checked_add_signed(Duration::microseconds(micros))
                .map(Value::Timestamp)
                .ok_or_else(|| DbError::new("22008", "binary timestamp is out of range"))
        }
        OID_INTERVAL => {
            length(16)?;
            Ok(Value::Interval(PgInterval::new(
                i32::from_be_bytes(bytes[12..16].try_into().expect("checked length")),
                i32::from_be_bytes(bytes[8..12].try_into().expect("checked length")),
                i64::from_be_bytes(bytes[0..8].try_into().expect("checked length")),
            )))
        }
        OID_JSON => serde_json::from_slice(bytes)
            .map(Value::Json)
            .map_err(|_| DbError::new("22P02", "binary JSON is invalid")),
        OID_JSONB => {
            if bytes.first() != Some(&1) {
                return Err(protocol("binary JSONB version must be one"));
            }
            serde_json::from_slice(&bytes[1..])
                .map(Value::Jsonb)
                .map_err(|_| DbError::new("22P02", "binary JSONB is invalid"))
        }
        OID_UUID => {
            length(16)?;
            Ok(Value::Uuid(
                Uuid::from_slice(bytes).expect("checked UUID length"),
            ))
        }
        oid if array_element_type(oid).is_some() => {
            let (element_type, element_oid) = array_element_type(oid)
                .ok_or_else(|| DbError::internal("array OID mapping disappeared"))?;
            decode_array_binary(bytes, element_type, element_oid)
        }
        0 => Err(DbError::new(
            "0A000",
            format!("binary parameter type OID {oid} is unsupported"),
        )),
        _ => Err(DbError::new(
            "0A000",
            format!("binary parameter type OID {oid} is unsupported"),
        )),
    }
}

pub fn encode_text(value: &Value) -> Result<Vec<u8>> {
    let text = match value {
        Value::Null => return Err(DbError::new("XX000", "NULL has no text payload")),
        Value::Boolean(value) => {
            if *value {
                "t".to_owned()
            } else {
                "f".to_owned()
            }
        }
        Value::Int16(value) => value.to_string(),
        Value::Int32(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float32(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Decimal(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Binary(value) => format!("\\x{}", encode_hex(value)),
        Value::Date(value) => value.format("%Y-%m-%d").to_string(),
        Value::Time(value) => value.format("%H:%M:%S%.f").to_string(),
        Value::Timestamp(value) => value.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
        Value::Interval(value) => value.to_string(),
        Value::Array(value) => encode_array_text(value)?,
        Value::Json(value) | Value::Jsonb(value) => value.to_string(),
        Value::Uuid(value) => value.to_string(),
        Value::Vector(values) => format!(
            "[{}]",
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
    };
    Ok(text.into_bytes())
}

fn encode_binary(value: &Value, data_type: &ScalarType) -> Result<Vec<u8>> {
    validate_enum_value(value, data_type)?;
    match value {
        Value::Boolean(value) => Ok(vec![u8::from(*value)]),
        Value::Int16(value) => Ok(value.to_be_bytes().to_vec()),
        Value::Int32(value) => Ok(value.to_be_bytes().to_vec()),
        Value::Int64(value) => Ok(value.to_be_bytes().to_vec()),
        Value::Float32(value) => Ok(value.to_bits().to_be_bytes().to_vec()),
        Value::Float64(value) => Ok(value.to_bits().to_be_bytes().to_vec()),
        Value::Decimal(value) => encode_numeric_binary(*value),
        Value::Text(value) => Ok(value.as_bytes().to_vec()),
        Value::Binary(value) => Ok(value.clone()),
        Value::Date(value) => {
            let days = value
                .signed_duration_since(postgres_epoch_date()?)
                .num_days();
            let days =
                i32::try_from(days).map_err(|_| DbError::new("22008", "date is out of range"))?;
            Ok(days.to_be_bytes().to_vec())
        }
        Value::Time(value) => {
            let micros = i64::from(value.num_seconds_from_midnight()) * 1_000_000
                + i64::from(value.nanosecond() / 1_000);
            Ok(micros.to_be_bytes().to_vec())
        }
        Value::Timestamp(value) => {
            let micros = value
                .signed_duration_since(postgres_epoch_timestamp()?)
                .num_microseconds()
                .ok_or_else(|| DbError::new("22008", "timestamp is out of range"))?;
            Ok(micros.to_be_bytes().to_vec())
        }
        Value::Interval(value) => {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&value.microseconds.to_be_bytes());
            bytes.extend_from_slice(&value.days.to_be_bytes());
            bytes.extend_from_slice(&value.months.to_be_bytes());
            Ok(bytes)
        }
        Value::Array(value) => encode_array_binary(value, data_type),
        Value::Json(value) => serde_json::to_vec(value).map_err(|error| {
            DbError::new("XX000", "failed to encode JSON").with_detail(error.to_string())
        }),
        Value::Jsonb(value) => {
            let mut encoded = vec![1];
            encoded.extend(serde_json::to_vec(value).map_err(|error| {
                DbError::new("XX000", "failed to encode JSONB").with_detail(error.to_string())
            })?);
            Ok(encoded)
        }
        Value::Uuid(value) => Ok(value.as_bytes().to_vec()),
        Value::Vector(_) => Err(DbError::new(
            "0A000",
            format!("binary results are unsupported for {data_type:?}"),
        )),
        Value::Null => Err(DbError::new("XX000", "NULL has no binary payload")),
    }
}

fn encode_numeric_binary(value: Decimal) -> Result<Vec<u8>> {
    let scale = value.scale();
    let mut text = value.abs().to_string();
    if scale > 0 && !text.contains('.') {
        text.push('.');
        text.push_str(&"0".repeat(usize::try_from(scale).unwrap_or(0)));
    }
    let (integer, fraction) = text.split_once('.').unwrap_or((&text, ""));
    let integer_padding = (4 - integer.len() % 4) % 4;
    let mut padded_integer = String::with_capacity(integer_padding + integer.len());
    padded_integer.push_str(&"0".repeat(integer_padding));
    padded_integer.push_str(integer);
    let fraction_padding = (4 - fraction.len() % 4) % 4;
    let mut padded_fraction = String::with_capacity(fraction.len() + fraction_padding);
    padded_fraction.push_str(fraction);
    padded_fraction.push_str(&"0".repeat(fraction_padding));
    let integer_groups = padded_integer.len() / 4;
    let mut digits = padded_integer
        .as_bytes()
        .chunks_exact(4)
        .chain(padded_fraction.as_bytes().chunks_exact(4))
        .map(|chunk| {
            std::str::from_utf8(chunk)
                .map_err(|_| DbError::internal("numeric encoder generated invalid UTF-8"))?
                .parse::<u16>()
                .map_err(|_| DbError::internal("numeric encoder generated an invalid digit"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut weight = i16::try_from(integer_groups)
        .map_err(|_| DbError::new("22003", "numeric weight exceeds i16"))?
        .checked_sub(1)
        .ok_or_else(|| DbError::internal("numeric integer group count underflowed"))?;
    while digits.first() == Some(&0) {
        digits.remove(0);
        weight = weight
            .checked_sub(1)
            .ok_or_else(|| DbError::new("22003", "numeric weight is out of range"))?;
    }
    while digits.last() == Some(&0) {
        digits.pop();
    }
    if digits.is_empty() {
        weight = 0;
    }
    let ndigits = i16::try_from(digits.len())
        .map_err(|_| DbError::new("22003", "numeric digit count exceeds i16"))?;
    let dscale =
        u16::try_from(scale).map_err(|_| DbError::new("22003", "numeric scale exceeds u16"))?;
    let sign = if value.is_sign_negative() && !value.is_zero() {
        NUMERIC_NEGATIVE
    } else {
        NUMERIC_POSITIVE
    };
    let mut output = Vec::with_capacity(8 + digits.len() * 2);
    output.extend_from_slice(&ndigits.to_be_bytes());
    output.extend_from_slice(&weight.to_be_bytes());
    output.extend_from_slice(&sign.to_be_bytes());
    output.extend_from_slice(&dscale.to_be_bytes());
    for digit in digits {
        output.extend_from_slice(&digit.to_be_bytes());
    }
    Ok(output)
}

fn decode_numeric_binary(bytes: &[u8]) -> Result<Decimal> {
    if bytes.len() < 8 || bytes.len() % 2 != 0 {
        return Err(protocol("binary numeric payload has an invalid length"));
    }
    let mut cursor = NetworkCursor::new(bytes);
    let ndigits = cursor.read_i16()?;
    let weight = cursor.read_i16()?;
    let sign = cursor.read_u16()?;
    let dscale = cursor.read_u16()?;
    if ndigits < 0 {
        return Err(protocol("binary numeric digit count is negative"));
    }
    if sign == NUMERIC_NAN {
        return Err(DbError::new(
            "0A000",
            "numeric NaN is not supported by the current decimal representation",
        ));
    }
    if !matches!(sign, NUMERIC_POSITIVE | NUMERIC_NEGATIVE) {
        return Err(protocol("binary numeric sign is invalid"));
    }
    if dscale > 28 {
        return Err(DbError::new(
            "22003",
            "numeric scale exceeds the current maximum of 28 digits",
        ));
    }
    let ndigits = usize::try_from(ndigits)
        .map_err(|_| protocol("binary numeric digit count is not addressable"))?;
    if cursor.remaining() != ndigits.saturating_mul(2) {
        return Err(protocol(
            "binary numeric digit count does not match its payload",
        ));
    }
    let mut digits = Vec::with_capacity(ndigits);
    for _ in 0..ndigits {
        let digit = cursor.read_u16()?;
        if digit >= 10_000 {
            return Err(protocol("binary numeric base-10000 digit is out of range"));
        }
        digits.push(digit);
    }
    cursor.finish()?;

    let integer_groups = if weight >= 0 {
        usize::try_from(i32::from(weight) + 1)
            .map_err(|_| DbError::new("22003", "numeric weight is out of range"))?
    } else {
        0
    };
    let fraction_groups = usize::from(dscale).div_ceil(4);
    let mut text = String::new();
    if sign == NUMERIC_NEGATIVE && digits.iter().any(|digit| *digit != 0) {
        text.push('-');
    }
    if integer_groups == 0 {
        text.push('0');
    } else {
        for position in 0..integer_groups {
            let exponent = i32::try_from(integer_groups - position - 1)
                .map_err(|_| DbError::new("22003", "numeric exponent is out of range"))?;
            let digit_index = i32::from(weight) - exponent;
            let digit = if digit_index >= 0 {
                usize::try_from(digit_index)
                    .ok()
                    .and_then(|index| digits.get(index))
                    .copied()
                    .unwrap_or(0)
            } else {
                0
            };
            if position == 0 {
                text.push_str(&digit.to_string());
            } else {
                text.push_str(&format!("{digit:04}"));
            }
        }
    }
    if dscale > 0 {
        text.push('.');
        let mut fraction = String::with_capacity(fraction_groups * 4);
        for group in 0..fraction_groups {
            let exponent = -(i32::try_from(group)
                .map_err(|_| DbError::new("22003", "numeric exponent is out of range"))?
                + 1);
            let digit_index = i32::from(weight) - exponent;
            let digit = if digit_index >= 0 {
                usize::try_from(digit_index)
                    .ok()
                    .and_then(|index| digits.get(index))
                    .copied()
                    .unwrap_or(0)
            } else {
                0
            };
            fraction.push_str(&format!("{digit:04}"));
        }
        fraction.truncate(usize::from(dscale));
        text.push_str(&fraction);
    }
    Decimal::from_str(&text).map_err(|_| DbError::new("22003", "binary numeric is out of range"))
}

fn array_oid(element: &ScalarType) -> Option<u32> {
    match element {
        ScalarType::Boolean => Some(OID_BOOL_ARRAY),
        ScalarType::Int16 => Some(OID_INT2_ARRAY),
        ScalarType::Int32 => Some(OID_INT4_ARRAY),
        ScalarType::Int64 => Some(OID_INT8_ARRAY),
        ScalarType::Float32 => Some(OID_FLOAT4_ARRAY),
        ScalarType::Float64 => Some(OID_FLOAT8_ARRAY),
        ScalarType::Decimal { .. } => Some(OID_NUMERIC_ARRAY),
        ScalarType::Char { .. } => Some(OID_BPCHAR_ARRAY),
        ScalarType::Varchar { .. } => Some(OID_VARCHAR_ARRAY),
        ScalarType::Text => Some(OID_TEXT_ARRAY),
        ScalarType::Enum { type_id, .. } => Some(enum_array_oid(*type_id)),
        ScalarType::Binary => Some(OID_BYTEA_ARRAY),
        ScalarType::Date => Some(OID_DATE_ARRAY),
        ScalarType::Time => Some(OID_TIME_ARRAY),
        ScalarType::Timestamp {
            with_timezone: false,
        } => Some(OID_TIMESTAMP_ARRAY),
        ScalarType::Timestamp {
            with_timezone: true,
        } => Some(OID_TIMESTAMPTZ_ARRAY),
        ScalarType::Interval => Some(OID_INTERVAL_ARRAY),
        ScalarType::Json => Some(OID_JSON_ARRAY),
        ScalarType::Jsonb => Some(OID_JSONB_ARRAY),
        ScalarType::Uuid => Some(OID_UUID_ARRAY),
        ScalarType::Array { .. } | ScalarType::Vector { .. } => None,
    }
}

fn array_element_type(oid: u32) -> Option<(ScalarType, u32)> {
    let value = match oid {
        OID_BOOL_ARRAY => (ScalarType::Boolean, OID_BOOL),
        OID_BYTEA_ARRAY => (ScalarType::Binary, OID_BYTEA),
        OID_INT2_ARRAY => (ScalarType::Int16, OID_INT2),
        OID_INT4_ARRAY => (ScalarType::Int32, OID_INT4),
        OID_TEXT_ARRAY => (ScalarType::Text, OID_TEXT),
        OID_BPCHAR_ARRAY => (ScalarType::Char { length: None }, OID_BPCHAR),
        OID_VARCHAR_ARRAY => (ScalarType::Varchar { length: None }, OID_VARCHAR),
        OID_INT8_ARRAY => (ScalarType::Int64, OID_INT8),
        OID_FLOAT4_ARRAY => (ScalarType::Float32, OID_FLOAT4),
        OID_FLOAT8_ARRAY => (ScalarType::Float64, OID_FLOAT8),
        OID_JSON_ARRAY => (ScalarType::Json, OID_JSON),
        OID_TIMESTAMP_ARRAY => (
            ScalarType::Timestamp {
                with_timezone: false,
            },
            OID_TIMESTAMP,
        ),
        OID_DATE_ARRAY => (ScalarType::Date, OID_DATE),
        OID_TIME_ARRAY => (ScalarType::Time, OID_TIME),
        OID_TIMESTAMPTZ_ARRAY => (
            ScalarType::Timestamp {
                with_timezone: true,
            },
            OID_TIMESTAMPTZ,
        ),
        OID_INTERVAL_ARRAY => (ScalarType::Interval, OID_INTERVAL),
        OID_NUMERIC_ARRAY => (
            ScalarType::Decimal {
                precision: None,
                scale: None,
            },
            OID_NUMERIC,
        ),
        OID_UUID_ARRAY => (ScalarType::Uuid, OID_UUID),
        OID_JSONB_ARRAY => (ScalarType::Jsonb, OID_JSONB),
        _ => return None,
    };
    Some(value)
}

fn decode_array_text(text: &str, element_type: ScalarType, element_oid: u32) -> Result<Value> {
    let mut parser = ArrayTextParser::new(text);
    let declared = parser.parse_dimensions()?;
    let (shape, elements) = parser.parse_level(0)?;
    parser.finish()?;
    let dimensions = if let Some(declared) = declared {
        if declared
            .iter()
            .map(|dimension| dimension.length)
            .collect::<Vec<_>>()
            != shape
                .iter()
                .map(|length| u32::try_from(*length).unwrap_or(u32::MAX))
                .collect::<Vec<_>>()
        {
            return Err(DbError::new(
                "22P02",
                "array dimensions do not match the array literal contents",
            ));
        }
        declared
    } else if shape == [0] {
        Vec::new()
    } else {
        shape
            .into_iter()
            .map(|length| {
                u32::try_from(length)
                    .map(|length| ArrayDimension::new(length, 1))
                    .map_err(|_| DbError::new("54000", "array dimension exceeds u32::MAX"))
            })
            .collect::<Result<Vec<_>>>()?
    };
    let values = elements
        .into_iter()
        .map(|element| match element {
            None => Ok(Value::Null),
            Some(element) => match &element_type {
                ScalarType::Enum { labels, .. } => decode_enum(element.as_bytes(), labels),
                _ => decode_text(element_oid, element.as_bytes()),
            },
        })
        .collect::<Result<Vec<_>>>()?;
    PgArray::new(element_type, dimensions, values).map(Value::Array)
}

fn encode_array_text(array: &PgArray) -> Result<String> {
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
                .ok_or_else(|| DbError::new("2202E", "array upper bound overflows"))?;
            output.push_str(&format!("[{}:{upper}]", dimension.lower_bound));
        }
        output.push('=');
    }
    if array.dimensions().is_empty() {
        output.push_str("{}");
        return Ok(output);
    }
    let mut offset = 0;
    encode_array_level(&mut output, array.dimensions(), array.values(), &mut offset)?;
    if offset != array.values().len() {
        return Err(DbError::internal(
            "array encoder did not consume every value",
        ));
    }
    Ok(output)
}

fn encode_array_level(
    output: &mut String,
    dimensions: &[ArrayDimension],
    values: &[Value],
    offset: &mut usize,
) -> Result<()> {
    let Some((dimension, remaining)) = dimensions.split_first() else {
        return Err(DbError::internal(
            "array encoder reached an empty dimension",
        ));
    };
    output.push('{');
    for index in 0..dimension.length {
        if index != 0 {
            output.push(',');
        }
        if remaining.is_empty() {
            let value = values
                .get(*offset)
                .ok_or_else(|| DbError::internal("array encoder ran out of values"))?;
            *offset += 1;
            output.push_str(&encode_array_element_text(value)?);
        } else {
            encode_array_level(output, remaining, values, offset)?;
        }
    }
    output.push('}');
    Ok(())
}

fn encode_array_element_text(value: &Value) -> Result<String> {
    if value.is_null() {
        return Ok("NULL".to_owned());
    }
    let bytes = encode_text(value)?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| DbError::new("22021", "array element text is not valid UTF-8"))?;
    let needs_quotes = text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text
            .chars()
            .any(|character| character.is_whitespace() || ",{}\"\\".contains(character));
    if !needs_quotes {
        return Ok(text.to_owned());
    }
    Ok(format!(
        "\"{}\"",
        text.replace('\\', "\\\\").replace('\"', "\\\"")
    ))
}

fn encode_array_binary(array: &PgArray, data_type: &ScalarType) -> Result<Vec<u8>> {
    let ScalarType::Array { element } = data_type else {
        return Err(DbError::new(
            "42804",
            "array value requires an array result type",
        ));
    };
    if element.as_ref() != array.element_type() {
        return Err(DbError::new(
            "42804",
            "array value element type does not match its result type",
        ));
    }
    let element_oid = type_oid(element);
    if element_oid == 0 {
        return Err(DbError::new(
            "0A000",
            "binary arrays are unsupported for this element type",
        ));
    }
    if array_oid(element).is_none() {
        return Err(DbError::new(
            "0A000",
            "binary arrays are unsupported for this element type",
        ));
    }
    let dimensions = i32::try_from(array.dimensions().len())
        .map_err(|_| DbError::new("54000", "array dimension count exceeds i32::MAX"))?;
    let mut output = Vec::new();
    output.extend_from_slice(&dimensions.to_be_bytes());
    output.extend_from_slice(&i32::from(array.values().iter().any(Value::is_null)).to_be_bytes());
    output.extend_from_slice(&element_oid.to_be_bytes());
    for dimension in array.dimensions() {
        output.extend_from_slice(
            &i32::try_from(dimension.length)
                .map_err(|_| DbError::new("54000", "array dimension exceeds i32::MAX"))?
                .to_be_bytes(),
        );
        output.extend_from_slice(&dimension.lower_bound.to_be_bytes());
    }
    for value in array.values() {
        if value.is_null() {
            output.extend_from_slice(&(-1_i32).to_be_bytes());
            continue;
        }
        let encoded = encode_array_element_binary(value, element)?;
        output.extend_from_slice(
            &i32::try_from(encoded.len())
                .map_err(|_| DbError::new("54000", "array element exceeds i32::MAX bytes"))?
                .to_be_bytes(),
        );
        output.extend_from_slice(&encoded);
    }
    Ok(output)
}

fn encode_array_element_binary(value: &Value, element: &ScalarType) -> Result<Vec<u8>> {
    match (value, element) {
        (Value::Int16(value), ScalarType::Int32) => Ok(i32::from(*value).to_be_bytes().to_vec()),
        (Value::Int16(value), ScalarType::Int64) => Ok(i64::from(*value).to_be_bytes().to_vec()),
        (Value::Int32(value), ScalarType::Int64) => Ok(i64::from(*value).to_be_bytes().to_vec()),
        (Value::Int16(value), ScalarType::Float32) => {
            Ok(f32::from(*value).to_bits().to_be_bytes().to_vec())
        }
        (Value::Int16(value), ScalarType::Float64) => {
            Ok(f64::from(*value).to_bits().to_be_bytes().to_vec())
        }
        (Value::Int32(value), ScalarType::Float64) => {
            Ok(f64::from(*value).to_bits().to_be_bytes().to_vec())
        }
        (Value::Int64(value), ScalarType::Float64) => {
            Ok((*value as f64).to_bits().to_be_bytes().to_vec())
        }
        _ => encode_binary(value, element),
    }
}

fn decode_array_binary(bytes: &[u8], element_type: ScalarType, element_oid: u32) -> Result<Value> {
    let mut cursor = NetworkCursor::new(bytes);
    let dimension_count = cursor.read_i32()?;
    if dimension_count < 0
        || usize::try_from(dimension_count)
            .ok()
            .is_none_or(|count| count > MAX_ARRAY_DIMENSIONS)
    {
        return Err(protocol("binary array dimension count is out of range"));
    }
    let has_null = cursor.read_i32()?;
    if !matches!(has_null, 0 | 1) {
        return Err(protocol("binary array NULL flag must be zero or one"));
    }
    if cursor.read_u32()? != element_oid {
        return Err(protocol(
            "binary array element OID does not match its array OID",
        ));
    }
    let mut dimensions = Vec::with_capacity(usize::try_from(dimension_count).unwrap_or(0));
    let mut element_count = if dimension_count == 0 {
        0_usize
    } else {
        1_usize
    };
    for _ in 0..dimension_count {
        let length = cursor.read_i32()?;
        if length < 0 {
            return Err(protocol(
                "binary array dimension length must be non-negative",
            ));
        }
        let length = usize::try_from(length)
            .map_err(|_| protocol("binary array dimension length is not addressable"))?;
        element_count = element_count
            .checked_mul(length)
            .ok_or_else(|| DbError::new("54000", "binary array element count overflows"))?;
        if element_count > MAX_ARRAY_ELEMENTS {
            return Err(DbError::new(
                "54000",
                "binary array exceeds the maximum element count",
            ));
        }
        dimensions.push(ArrayDimension::new(
            u32::try_from(length)
                .map_err(|_| DbError::new("54000", "array dimension exceeds u32::MAX"))?,
            cursor.read_i32()?,
        ));
    }
    let mut values = Vec::with_capacity(element_count);
    for _ in 0..element_count {
        let length = cursor.read_i32()?;
        if length == -1 {
            values.push(Value::Null);
            continue;
        }
        if length < 0 {
            return Err(protocol("binary array element length is invalid"));
        }
        let payload = cursor.take(
            usize::try_from(length)
                .map_err(|_| protocol("binary array element length is not addressable"))?,
        )?;
        values.push(match &element_type {
            ScalarType::Enum { labels, .. } => decode_enum(payload, labels)?,
            _ => decode_binary(element_oid, payload)?,
        });
    }
    cursor.finish()?;
    if has_null == 0 && values.iter().any(Value::is_null) {
        return Err(protocol(
            "binary array contains NULL despite its header flag",
        ));
    }
    PgArray::new(element_type, dimensions, values).map(Value::Array)
}

struct NetworkCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> NetworkCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("checked length"),
        ))
    }

    fn read_i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().expect("checked length"),
        ))
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("checked length"),
        ))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("checked length"),
        ))
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| protocol("binary array offset overflows"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| protocol("binary array payload is truncated"))?;
        self.offset = end;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(protocol("binary array payload has trailing bytes"));
        }
        Ok(())
    }
}

struct ArrayTextParser<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ArrayTextParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            bytes: input.as_bytes(),
            offset: 0,
        }
    }

    fn parse_dimensions(&mut self) -> Result<Option<Vec<ArrayDimension>>> {
        self.skip_whitespace();
        if self.peek() != Some(b'[') {
            return Ok(None);
        }
        let mut dimensions = Vec::new();
        while self.peek() == Some(b'[') {
            self.offset += 1;
            let lower = self.parse_bound_integer()?;
            self.expect(b':')?;
            let upper = self.parse_bound_integer()?;
            self.expect(b']')?;
            let length = i64::from(upper)
                .checked_sub(i64::from(lower))
                .and_then(|value| value.checked_add(1))
                .filter(|value| *value >= 0)
                .ok_or_else(|| DbError::new("22P02", "array bounds are invalid"))?;
            dimensions.push(ArrayDimension::new(
                u32::try_from(length)
                    .map_err(|_| DbError::new("54000", "array dimension exceeds u32::MAX"))?,
                lower,
            ));
            if dimensions.len() > MAX_ARRAY_DIMENSIONS {
                return Err(DbError::new(
                    "54000",
                    "array exceeds the maximum dimension count",
                ));
            }
        }
        self.expect(b'=')?;
        Ok(Some(dimensions))
    }

    fn parse_level(&mut self, depth: usize) -> Result<(Vec<usize>, Vec<Option<String>>)> {
        if depth >= MAX_ARRAY_DIMENSIONS {
            return Err(DbError::new(
                "54000",
                "array exceeds the maximum dimension count",
            ));
        }
        self.skip_whitespace();
        self.expect(b'{')?;
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.offset += 1;
            return Ok((vec![0], Vec::new()));
        }
        let nested = self.peek() == Some(b'{');
        let mut count = 0_usize;
        let mut child_shape: Option<Vec<usize>> = None;
        let mut values = Vec::new();
        loop {
            self.skip_whitespace();
            if nested {
                if self.peek() != Some(b'{') {
                    return Err(DbError::new(
                        "22P02",
                        "multidimensional array has mixed scalar and nested elements",
                    ));
                }
                let (shape, mut child_values) = self.parse_level(depth + 1)?;
                if child_shape
                    .as_ref()
                    .is_some_and(|expected| expected != &shape)
                {
                    return Err(DbError::new(
                        "22P02",
                        "multidimensional arrays must have matching dimensions",
                    ));
                }
                child_shape.get_or_insert(shape);
                values.append(&mut child_values);
            } else {
                if self.peek() == Some(b'{') {
                    return Err(DbError::new(
                        "22P02",
                        "multidimensional array has mixed scalar and nested elements",
                    ));
                }
                values.push(self.parse_element()?);
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| DbError::new("54000", "array element count overflows"))?;
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.offset += 1,
                Some(b'}') => {
                    self.offset += 1;
                    break;
                }
                _ => return Err(DbError::new("22P02", "array literal is malformed")),
            }
        }
        let mut shape = vec![count];
        if let Some(child_shape) = child_shape {
            shape.extend(child_shape);
        }
        Ok((shape, values))
    }

    fn parse_element(&mut self) -> Result<Option<String>> {
        if self.peek() == Some(b'\"') {
            return self.parse_quoted().map(Some);
        }
        let mut bytes = Vec::new();
        while let Some(byte) = self.peek() {
            if matches!(byte, b',' | b'}') {
                break;
            }
            self.offset += 1;
            if byte == b'\\' {
                let escaped = self
                    .peek()
                    .ok_or_else(|| DbError::new("22P02", "array element ends with an escape"))?;
                self.offset += 1;
                bytes.push(escaped);
            } else {
                bytes.push(byte);
            }
        }
        let value = std::str::from_utf8(&bytes)
            .map_err(|_| DbError::new("22021", "array element is not valid UTF-8"))?
            .trim()
            .to_owned();
        if value.is_empty() {
            return Err(DbError::new("22P02", "unquoted array element is empty"));
        }
        Ok((!value.eq_ignore_ascii_case("NULL")).then_some(value))
    }

    fn parse_quoted(&mut self) -> Result<String> {
        self.expect(b'\"')?;
        let mut bytes = Vec::new();
        loop {
            let byte = self
                .peek()
                .ok_or_else(|| DbError::new("22P02", "quoted array element is unterminated"))?;
            self.offset += 1;
            match byte {
                b'\"' => break,
                b'\\' => {
                    let escaped = self.peek().ok_or_else(|| {
                        DbError::new("22P02", "quoted array element ends with an escape")
                    })?;
                    self.offset += 1;
                    bytes.push(escaped);
                }
                _ => bytes.push(byte),
            }
        }
        String::from_utf8(bytes)
            .map_err(|_| DbError::new("22021", "quoted array element is not valid UTF-8"))
    }

    fn parse_bound_integer(&mut self) -> Result<i32> {
        let start = self.offset;
        if matches!(self.peek(), Some(b'-' | b'+')) {
            self.offset += 1;
        }
        while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            self.offset += 1;
        }
        let value = std::str::from_utf8(&self.bytes[start..self.offset])
            .map_err(|_| DbError::new("22P02", "array bound is not valid UTF-8"))?;
        value
            .parse()
            .map_err(|_| DbError::new("22P02", "array bound is invalid"))
    }

    fn expect(&mut self, expected: u8) -> Result<()> {
        self.skip_whitespace();
        if self.peek() != Some(expected) {
            return Err(DbError::new(
                "22P02",
                format!("array literal expected `{}`", char::from(expected)),
            ));
        }
        self.offset += 1;
        Ok(())
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.offset += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.offset).copied()
    }

    fn finish(mut self) -> Result<()> {
        self.skip_whitespace();
        if self.offset != self.bytes.len() {
            return Err(DbError::new("22P02", "array literal has trailing input"));
        }
        Ok(())
    }
}

fn parse_timestamp(text: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
        .or_else(|| {
            DateTime::parse_from_rfc3339(text)
                .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%:z"))
                .ok()
                .map(|value| value.with_timezone(&Utc).naive_utc())
        })
}

fn decode_bytea_text(text: &str) -> Result<Vec<u8>> {
    let Some(hex) = text.strip_prefix("\\x") else {
        return Ok(text.as_bytes().to_vec());
    };
    if hex.len() % 2 != 0 {
        return Err(DbError::new(
            "22P02",
            "hex bytea has an odd number of digits",
        ));
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(DbError::new("22P02", "bytea contains a non-hex digit")),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn postgres_epoch_date() -> Result<NaiveDate> {
    NaiveDate::from_ymd_opt(
        POSTGRES_EPOCH_DATE.0,
        POSTGRES_EPOCH_DATE.1,
        POSTGRES_EPOCH_DATE.2,
    )
    .ok_or_else(|| DbError::new("XX000", "PostgreSQL epoch date is invalid"))
}

fn postgres_epoch_timestamp() -> Result<NaiveDateTime> {
    postgres_epoch_date()?
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| DbError::new("XX000", "PostgreSQL epoch timestamp is invalid"))
}

#[cfg(test)]
mod tests {
    use ordadb_types::Field;

    use super::*;

    #[test]
    fn text_and_binary_round_trip_supported_scalar_parameters() {
        assert_eq!(decode_text(OID_INT8, b"42").expect("int"), Value::Int64(42));
        assert_eq!(
            decode_binary(OID_INT4, &42_i32.to_be_bytes()).expect("int"),
            Value::Int32(42)
        );
        let date = NaiveDate::from_ymd_opt(2026, 7, 25).expect("date");
        let binary = encode_binary(&Value::Date(date), &ScalarType::Date).expect("encode");
        assert_eq!(
            decode_binary(OID_DATE, &binary).expect("decode"),
            Value::Date(date)
        );
    }

    #[test]
    fn interval_timestamptz_and_array_formats_round_trip() {
        for numeric in [
            Decimal::new(0, 0),
            Decimal::new(1_234_567, 2),
            Decimal::new(-12, 4),
            Decimal::new(120_000, 4),
        ] {
            let binary = encode_binary(
                &Value::Decimal(numeric),
                &ScalarType::Decimal {
                    precision: None,
                    scale: None,
                },
            )
            .expect("numeric binary");
            assert_eq!(
                decode_binary(OID_NUMERIC, &binary).expect("numeric binary"),
                Value::Decimal(numeric)
            );
        }

        let interval = PgInterval::new(14, 3, 14_706_750_000);
        assert_eq!(
            decode_text(OID_INTERVAL, b"1 year 2 mons 3 days 04:05:06.75").expect("interval text"),
            Value::Interval(interval)
        );
        let binary = encode_binary(&Value::Interval(interval), &ScalarType::Interval)
            .expect("interval binary");
        assert_eq!(
            decode_binary(OID_INTERVAL, &binary).expect("interval binary"),
            Value::Interval(interval)
        );

        let timestamp =
            decode_text(OID_TIMESTAMPTZ, b"2026-08-03 12:30:00+08:00").expect("timestamptz");
        assert_eq!(
            timestamp,
            Value::Timestamp(
                NaiveDateTime::parse_from_str("2026-08-03 04:30:00", "%Y-%m-%d %H:%M:%S")
                    .expect("timestamp")
            )
        );
        let binary = encode_binary(
            &timestamp,
            &ScalarType::Timestamp {
                with_timezone: true,
            },
        )
        .expect("timestamptz binary");
        assert_eq!(
            decode_binary(OID_TIMESTAMPTZ, &binary).expect("timestamptz binary"),
            timestamp
        );

        let array = decode_text(OID_TEXT_ARRAY, br#"[-1:0][2:3]={{"a,b",NULL},{x,"NULL"}}"#)
            .expect("array text");
        let Value::Array(array) = array else {
            panic!("array expected");
        };
        assert_eq!(
            array.dimensions(),
            &[ArrayDimension::new(2, -1), ArrayDimension::new(2, 2)]
        );
        assert_eq!(
            array.values(),
            &[
                Value::Text("a,b".into()),
                Value::Null,
                Value::Text("x".into()),
                Value::Text("NULL".into())
            ]
        );
        assert_eq!(
            String::from_utf8(encode_text(&Value::Array(array)).expect("array encode"))
                .expect("utf8"),
            r#"[-1:0][2:3]={{"a,b",NULL},{x,"NULL"}}"#
        );

        let array = PgArray::one_dimensional(
            ScalarType::Int32,
            vec![Value::Int32(1), Value::Null, Value::Int32(3)],
        )
        .expect("array");
        let data_type = ScalarType::Array {
            element: Box::new(ScalarType::Int32),
        };
        assert_eq!(type_oid(&data_type), OID_INT4_ARRAY);
        let binary = encode_binary(&Value::Array(array.clone()), &data_type).expect("array binary");
        assert_eq!(
            decode_binary(OID_INT4_ARRAY, &binary).expect("array binary"),
            Value::Array(array)
        );
    }

    #[test]
    fn enum_oids_and_text_binary_values_are_stable_and_validated() {
        let enum_type = ScalarType::Enum {
            type_id: TypeId::new(7),
            labels: vec!["queued".into(), "running".into(), "done".into()],
        };
        let scalar_oid = OID_USER_DEFINED_ENUM_BASE + 12;
        let array_oid = scalar_oid + 1;
        assert_eq!(enum_type_oid(TypeId::new(7)), scalar_oid);
        assert_eq!(enum_array_oid(TypeId::new(7)), array_oid);
        assert_eq!(type_oid(&enum_type), scalar_oid);

        for format in [0, 1] {
            assert_eq!(
                decode_parameters_as(
                    &[scalar_oid],
                    std::slice::from_ref(&enum_type),
                    &[format],
                    &[Some(b"running".to_vec())],
                )
                .expect("enum parameter"),
                [Value::Text("running".into())]
            );
        }
        assert_eq!(
            encode_binary(&Value::Text("done".into()), &enum_type).expect("enum binary"),
            b"done"
        );

        let array_type = ScalarType::Array {
            element: Box::new(enum_type.clone()),
        };
        assert_eq!(type_oid(&array_type), array_oid);
        let array = PgArray::one_dimensional(
            enum_type.clone(),
            vec![
                Value::Text("queued".into()),
                Value::Null,
                Value::Text("done".into()),
            ],
        )
        .expect("enum array");
        let expected = Value::Array(array.clone());
        assert_eq!(
            decode_parameters_as(
                &[array_oid],
                std::slice::from_ref(&array_type),
                &[0],
                &[Some(b"{queued,NULL,done}".to_vec())],
            )
            .expect("enum array text"),
            std::slice::from_ref(&expected)
        );
        let binary = encode_binary(&expected, &array_type).expect("enum array binary");
        assert_eq!(
            decode_parameters_as(
                &[array_oid],
                std::slice::from_ref(&array_type),
                &[1],
                &[Some(binary)],
            )
            .expect("enum array binary"),
            [expected]
        );

        let invalid = decode_parameters_as(
            &[scalar_oid],
            std::slice::from_ref(&enum_type),
            &[0],
            &[Some(b"blocked".to_vec())],
        )
        .expect_err("invalid enum label");
        assert_eq!(invalid.sql_state, "22P02");
        let invalid_array = decode_parameters_as(
            &[array_oid],
            std::slice::from_ref(&array_type),
            &[0],
            &[Some(b"{queued,blocked}".to_vec())],
        )
        .expect_err("invalid enum array label");
        assert_eq!(invalid_array.sql_state, "22P02");
    }

    #[test]
    fn row_description_and_data_row_honor_result_formats() {
        let schema = Schema::new(vec![
            Field::new("id", ScalarType::Int64, false),
            Field::new("title", ScalarType::Text, false),
        ]);
        let mut bytes = Vec::new();
        write_row_description(&mut bytes, &schema, &[1, 0]).expect("description");
        write_data_row(
            &mut bytes,
            &schema,
            &Row::new(vec![Value::Int64(42), Value::Text("hello".into())]),
            &[1, 0],
        )
        .expect("row");
        assert!(bytes.starts_with(b"T"));
        assert!(bytes.windows(5).any(|window| window == b"hello"));
    }

    #[test]
    fn malformed_binary_numeric_is_rejected() {
        let error = decode_binary(OID_NUMERIC, &[0, 0]).expect_err("malformed numeric");
        assert_eq!(error.sql_state, "08P01");
    }
}
