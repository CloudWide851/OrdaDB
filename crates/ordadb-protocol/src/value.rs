use std::io::Write;
use std::str::FromStr;

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use rust_decimal::Decimal;
use uuid::Uuid;

use ordadb_types::{DbError, Result, Row, ScalarType, Schema, Value};

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
pub const OID_NUMERIC: u32 = 1700;
pub const OID_JSON: u32 = 114;
pub const OID_UUID: u32 = 2950;
pub const OID_JSONB: u32 = 3802;

const POSTGRES_EPOCH_DATE: (i32, u32, u32) = (2000, 1, 1);

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

pub fn decode_parameters(
    oids: &[u32],
    formats: &[i16],
    parameters: &[Option<Vec<u8>>],
) -> Result<Vec<Value>> {
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
        .enumerate()
        .map(|(index, parameter)| {
            let oid = oids.get(index).copied().unwrap_or(0);
            match parameter {
                None => Ok(Value::Null),
                Some(bytes) if formats[index] == 0 => decode_text(oid, bytes),
                Some(bytes) => decode_binary(oid, bytes),
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
        ScalarType::Binary => OID_BYTEA,
        ScalarType::Date => OID_DATE,
        ScalarType::Time => OID_TIME,
        ScalarType::Timestamp {
            with_timezone: false,
        } => OID_TIMESTAMP,
        ScalarType::Timestamp {
            with_timezone: true,
        } => OID_TIMESTAMPTZ,
        ScalarType::Json => OID_JSON,
        ScalarType::Jsonb => OID_JSONB,
        ScalarType::Uuid => OID_UUID,
    }
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
        OID_JSON => serde_json::from_str(text)
            .map(Value::Json)
            .map_err(|_| invalid()),
        OID_JSONB => serde_json::from_str(text)
            .map(Value::Jsonb)
            .map_err(|_| invalid()),
        OID_UUID => Uuid::parse_str(text)
            .map(Value::Uuid)
            .map_err(|_| invalid()),
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
        OID_TEXT | OID_BPCHAR | OID_VARCHAR => std::str::from_utf8(bytes)
            .map(|text| Value::Text(text.to_owned()))
            .map_err(|_| DbError::new("22021", "binary text is not valid UTF-8")),
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
        OID_TIMESTAMP => {
            length(8)?;
            let micros = i64::from_be_bytes(bytes.try_into().expect("checked length"));
            postgres_epoch_timestamp()?
                .checked_add_signed(Duration::microseconds(micros))
                .map(Value::Timestamp)
                .ok_or_else(|| DbError::new("22008", "binary timestamp is out of range"))
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
        0 | OID_NUMERIC | OID_TIMESTAMPTZ => Err(DbError::new(
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
    match value {
        Value::Boolean(value) => Ok(vec![u8::from(*value)]),
        Value::Int16(value) => Ok(value.to_be_bytes().to_vec()),
        Value::Int32(value) => Ok(value.to_be_bytes().to_vec()),
        Value::Int64(value) => Ok(value.to_be_bytes().to_vec()),
        Value::Float32(value) => Ok(value.to_bits().to_be_bytes().to_vec()),
        Value::Float64(value) => Ok(value.to_bits().to_be_bytes().to_vec()),
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
            if matches!(
                data_type,
                ScalarType::Timestamp {
                    with_timezone: true
                }
            ) {
                return Err(DbError::new(
                    "0A000",
                    "binary timestamptz results are unsupported",
                ));
            }
            let micros = value
                .signed_duration_since(postgres_epoch_timestamp()?)
                .num_microseconds()
                .ok_or_else(|| DbError::new("22008", "timestamp is out of range"))?;
            Ok(micros.to_be_bytes().to_vec())
        }
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
        Value::Decimal(_) | Value::Vector(_) => Err(DbError::new(
            "0A000",
            format!("binary results are unsupported for {data_type:?}"),
        )),
        Value::Null => Err(DbError::new("XX000", "NULL has no binary payload")),
    }
}

fn parse_timestamp(text: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
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
    fn unsupported_binary_numeric_is_explicit() {
        let error = decode_binary(OID_NUMERIC, &[0, 0]).expect_err("unsupported");
        assert_eq!(error.sql_state, "0A000");
    }
}
