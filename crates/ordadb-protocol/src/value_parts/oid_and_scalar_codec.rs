use std::io::Write;
use std::str::FromStr;

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use ordadb_types::{
    ArrayDimension, DbError, MAX_ARRAY_DIMENSIONS, MAX_ARRAY_ELEMENTS, MAX_POSTGRES_NAME_BYTES,
    PgArray, PgInterval, Result, Row, ScalarType, Schema, TypeId, Value,
};

use crate::codec::{protocol, push_cstring, write_message};

pub const OID_BOOL: u32 = 16;
pub const OID_BYTEA: u32 = 17;
pub const OID_INTERNAL_CHAR: u32 = 18;
pub const OID_NAME: u32 = 19;
pub const OID_INT8: u32 = 20;
pub const OID_INT2: u32 = 21;
pub const OID_INT4: u32 = 23;
pub const OID_TEXT: u32 = 25;
pub const OID_OID: u32 = 26;
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
pub const OID_INTERNAL_CHAR_ARRAY: u32 = 1002;
pub const OID_NAME_ARRAY: u32 = 1003;
pub const OID_INT2_ARRAY: u32 = 1005;
pub const OID_INT4_ARRAY: u32 = 1007;
pub const OID_TEXT_ARRAY: u32 = 1009;
pub const OID_BPCHAR_ARRAY: u32 = 1014;
pub const OID_VARCHAR_ARRAY: u32 = 1015;
pub const OID_INT8_ARRAY: u32 = 1016;
pub const OID_FLOAT4_ARRAY: u32 = 1021;
pub const OID_FLOAT8_ARRAY: u32 = 1022;
pub const OID_OID_ARRAY: u32 = 1028;
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
        validate_wire_value(value, &field.data_type)?;
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
        ScalarType::Oid => OID_OID,
        ScalarType::Name => OID_NAME,
        ScalarType::InternalChar => OID_INTERNAL_CHAR,
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
        ScalarType::InternalChar => 1,
        ScalarType::Name => 64,
        ScalarType::Int16 => 2,
        ScalarType::Int32 | ScalarType::Oid | ScalarType::Float32 | ScalarType::Date => 4,
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
        OID_NAME if text.len() <= MAX_POSTGRES_NAME_BYTES => Ok(Value::Text(text.to_owned())),
        OID_NAME => Err(DbError::new(
            "22001",
            "PostgreSQL name parameter exceeds 63 bytes",
        )),
        OID_INTERNAL_CHAR if text.len() == 1 => Ok(Value::Text(text.to_owned())),
        OID_INTERNAL_CHAR => Err(invalid()),
        oid if enum_type_id_from_oid(oid, false).is_some() => Ok(Value::Text(text.to_owned())),
        OID_BOOL => match text.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" => Ok(Value::Boolean(true)),
            "f" | "false" | "0" => Ok(Value::Boolean(false)),
            _ => Err(invalid()),
        },
        OID_INT2 => text.parse().map(Value::Int16).map_err(|_| invalid()),
        OID_INT4 => text.parse().map(Value::Int32).map_err(|_| invalid()),
        OID_INT8 => text.parse().map(Value::Int64).map_err(|_| invalid()),
        OID_OID => {
            let value = text.parse::<u64>().map_err(|_| invalid())?;
            u32::try_from(value)
                .map(|value| Value::Int64(i64::from(value)))
                .map_err(|_| DbError::new("22003", "OID parameter is out of range"))
        }
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
        OID_OID => {
            length(4)?;
            Ok(Value::Int64(i64::from(u32::from_be_bytes(
                bytes.try_into().expect("checked length"),
            ))))
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
        OID_NAME => {
            if bytes.len() > MAX_POSTGRES_NAME_BYTES {
                return Err(DbError::new(
                    "22001",
                    "binary PostgreSQL name exceeds 63 bytes",
                ));
            }
            std::str::from_utf8(bytes)
                .map(|text| Value::Text(text.to_owned()))
                .map_err(|_| DbError::new("22021", "binary name is not valid UTF-8"))
        }
        OID_INTERNAL_CHAR => {
            length(1)?;
            std::str::from_utf8(bytes)
                .map(|text| Value::Text(text.to_owned()))
                .map_err(|_| DbError::new("22021", "binary internal char is not valid UTF-8"))
        }
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
    validate_wire_value(value, data_type)?;
    if matches!(data_type, ScalarType::Oid) {
        let Value::Int64(value) = value else {
            return Err(DbError::new(
                "42804",
                "OID result must use an integer value",
            ));
        };
        return u32::try_from(*value)
            .map(|value| value.to_be_bytes().to_vec())
            .map_err(|_| DbError::new("22003", "OID result is out of range"));
    }
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

fn validate_wire_value(value: &Value, data_type: &ScalarType) -> Result<()> {
    validate_enum_value(value, data_type)?;
    match (data_type, value) {
        (ScalarType::Oid, Value::Int64(value)) if u32::try_from(*value).is_ok() => Ok(()),
        (ScalarType::Oid, Value::Int64(_)) => {
            Err(DbError::new("22003", "OID result is out of range"))
        }
        (ScalarType::Oid, _) => Err(DbError::new(
            "42804",
            "OID result must use an integer value",
        )),
        (ScalarType::Name, Value::Text(value)) if value.len() <= MAX_POSTGRES_NAME_BYTES => Ok(()),
        (ScalarType::Name, Value::Text(_)) => {
            Err(DbError::new("22001", "PostgreSQL name exceeds 63 bytes"))
        }
        (ScalarType::Name, _) => Err(DbError::new(
            "42804",
            "PostgreSQL name result must use a text value",
        )),
        (ScalarType::InternalChar, Value::Text(value)) if value.len() == 1 => Ok(()),
        (ScalarType::InternalChar, Value::Text(_)) => Err(DbError::new(
            "22001",
            "PostgreSQL internal char must contain exactly one byte",
        )),
        (ScalarType::InternalChar, _) => Err(DbError::new(
            "42804",
            "PostgreSQL internal char result must use a text value",
        )),
        _ => Ok(()),
    }
}
