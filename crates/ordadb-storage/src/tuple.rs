use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};
use ordadb_types::{Result, Row, Value};
use rust_decimal::Decimal;

use crate::corruption;

const TAG_NULL: u8 = 0;
const TAG_BOOLEAN: u8 = 1;
const TAG_INT16: u8 = 2;
const TAG_INT32: u8 = 3;
const TAG_INT64: u8 = 4;
const TAG_FLOAT32: u8 = 5;
const TAG_FLOAT64: u8 = 6;
const TAG_DECIMAL: u8 = 7;
const TAG_TEXT: u8 = 8;
const TAG_BINARY: u8 = 9;
const TAG_DATE: u8 = 10;
const TAG_TIME: u8 = 11;
const TAG_TIMESTAMP: u8 = 12;
const TAG_JSON: u8 = 13;
const TAG_JSONB: u8 = 14;
const TAG_UUID: u8 = 15;
const TAG_VECTOR: u8 = 16;

pub fn encode_row(row: &Row) -> Result<Vec<u8>> {
    let value_count = u16::try_from(row.values.len())
        .map_err(|_| corruption("tuple contains more than 65535 values"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&value_count.to_le_bytes());
    for value in &row.values {
        encode_value(value, &mut bytes)?;
    }
    Ok(bytes)
}

pub fn decode_row(bytes: &[u8]) -> Result<Row> {
    let mut cursor = Cursor::new(bytes);
    let value_count = cursor.read_u16()?;
    let mut values = Vec::with_capacity(usize::from(value_count));
    for _ in 0..value_count {
        values.push(decode_value(&mut cursor)?);
    }
    if !cursor.is_finished() {
        return Err(corruption("tuple contains trailing bytes"));
    }
    Ok(Row::new(values))
}

fn encode_value(value: &Value, bytes: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => bytes.push(TAG_NULL),
        Value::Boolean(value) => {
            bytes.push(TAG_BOOLEAN);
            bytes.push(u8::from(*value));
        }
        Value::Int16(value) => {
            bytes.push(TAG_INT16);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Value::Int32(value) => {
            bytes.push(TAG_INT32);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Value::Int64(value) => {
            bytes.push(TAG_INT64);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Value::Float32(value) => {
            bytes.push(TAG_FLOAT32);
            bytes.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::Float64(value) => {
            bytes.push(TAG_FLOAT64);
            bytes.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::Decimal(value) => {
            bytes.push(TAG_DECIMAL);
            bytes.extend_from_slice(&value.serialize());
        }
        Value::Text(value) => {
            bytes.push(TAG_TEXT);
            write_length_prefixed(bytes, value.as_bytes())?;
        }
        Value::Binary(value) => {
            bytes.push(TAG_BINARY);
            write_length_prefixed(bytes, value)?;
        }
        Value::Date(value) => {
            bytes.push(TAG_DATE);
            bytes.extend_from_slice(&value.num_days_from_ce().to_le_bytes());
        }
        Value::Time(value) => {
            bytes.push(TAG_TIME);
            bytes.extend_from_slice(&value.num_seconds_from_midnight().to_le_bytes());
            bytes.extend_from_slice(&value.nanosecond().to_le_bytes());
        }
        Value::Timestamp(value) => {
            bytes.push(TAG_TIMESTAMP);
            let utc = value.and_utc();
            bytes.extend_from_slice(&utc.timestamp().to_le_bytes());
            bytes.extend_from_slice(&utc.timestamp_subsec_nanos().to_le_bytes());
        }
        Value::Json(value) => {
            bytes.push(TAG_JSON);
            let encoded = serde_json::to_vec(value)
                .map_err(|error| corruption(format!("JSON encoding failed: {error}")))?;
            write_length_prefixed(bytes, &encoded)?;
        }
        Value::Jsonb(value) => {
            bytes.push(TAG_JSONB);
            let encoded = serde_json::to_vec(value)
                .map_err(|error| corruption(format!("JSONB encoding failed: {error}")))?;
            write_length_prefixed(bytes, &encoded)?;
        }
        Value::Uuid(value) => {
            bytes.push(TAG_UUID);
            bytes.extend_from_slice(value.as_bytes());
        }
        Value::Vector(values) => {
            bytes.push(TAG_VECTOR);
            let count = u32::try_from(values.len())
                .map_err(|_| corruption("VECTOR contains more than u32::MAX elements"))?;
            bytes.extend_from_slice(&count.to_le_bytes());
            for value in values {
                bytes.extend_from_slice(&value.to_bits().to_le_bytes());
            }
        }
    }
    Ok(())
}

fn decode_value(cursor: &mut Cursor<'_>) -> Result<Value> {
    let tag = cursor.read_u8()?;
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_BOOLEAN => match cursor.read_u8()? {
            0 => Ok(Value::Boolean(false)),
            1 => Ok(Value::Boolean(true)),
            value => Err(corruption(format!(
                "tuple contains invalid boolean byte {value}"
            ))),
        },
        TAG_INT16 => Ok(Value::Int16(i16::from_le_bytes(cursor.read_array()?))),
        TAG_INT32 => Ok(Value::Int32(i32::from_le_bytes(cursor.read_array()?))),
        TAG_INT64 => Ok(Value::Int64(i64::from_le_bytes(cursor.read_array()?))),
        TAG_FLOAT32 => Ok(Value::Float32(f32::from_bits(u32::from_le_bytes(
            cursor.read_array()?,
        )))),
        TAG_FLOAT64 => Ok(Value::Float64(f64::from_bits(u64::from_le_bytes(
            cursor.read_array()?,
        )))),
        TAG_DECIMAL => Ok(Value::Decimal(Decimal::deserialize(cursor.read_array()?))),
        TAG_TEXT => {
            let payload = cursor.read_length_prefixed()?;
            let value = std::str::from_utf8(payload)
                .map_err(|error| corruption(format!("tuple text is not UTF-8: {error}")))?;
            Ok(Value::Text(value.to_owned()))
        }
        TAG_BINARY => Ok(Value::Binary(cursor.read_length_prefixed()?.to_vec())),
        TAG_DATE => {
            let days = i32::from_le_bytes(cursor.read_array()?);
            let value = NaiveDate::from_num_days_from_ce_opt(days)
                .ok_or_else(|| corruption(format!("tuple date day {days} is out of range")))?;
            Ok(Value::Date(value))
        }
        TAG_TIME => {
            let seconds = u32::from_le_bytes(cursor.read_array()?);
            let nanos = u32::from_le_bytes(cursor.read_array()?);
            let value =
                NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos).ok_or_else(|| {
                    corruption(format!(
                        "tuple time seconds={seconds}, nanos={nanos} is invalid"
                    ))
                })?;
            Ok(Value::Time(value))
        }
        TAG_TIMESTAMP => {
            let seconds = i64::from_le_bytes(cursor.read_array()?);
            let nanos = u32::from_le_bytes(cursor.read_array()?);
            let value = chrono::DateTime::from_timestamp(seconds, nanos)
                .ok_or_else(|| {
                    corruption(format!(
                        "tuple timestamp seconds={seconds}, nanos={nanos} is invalid"
                    ))
                })?
                .naive_utc();
            Ok(Value::Timestamp(value))
        }
        TAG_JSON | TAG_JSONB => {
            let payload = cursor.read_length_prefixed()?;
            let value = serde_json::from_slice(payload)
                .map_err(|error| corruption(format!("tuple JSON is malformed: {error}")))?;
            if tag == TAG_JSON {
                Ok(Value::Json(value))
            } else {
                Ok(Value::Jsonb(value))
            }
        }
        TAG_UUID => Ok(Value::Uuid(uuid_from_bytes(cursor.read_array()?))),
        TAG_VECTOR => {
            let count = usize::try_from(u32::from_le_bytes(cursor.read_array()?))
                .map_err(|_| corruption("VECTOR element count is not addressable"))?;
            let byte_count = count
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(|| corruption("VECTOR byte length overflow"))?;
            cursor.ensure_remaining(byte_count)?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(f32::from_bits(u32::from_le_bytes(cursor.read_array()?)));
            }
            Ok(Value::Vector(values))
        }
        value => Err(corruption(format!(
            "tuple contains unknown value tag {value}"
        ))),
    }
}

fn uuid_from_bytes(bytes: [u8; 16]) -> uuid::Uuid {
    uuid::Uuid::from_bytes(bytes)
}

fn write_length_prefixed(target: &mut Vec<u8>, payload: &[u8]) -> Result<()> {
    let length = u32::try_from(payload.len())
        .map_err(|_| corruption("tuple variable-width value exceeds u32::MAX bytes"))?;
    target.extend_from_slice(&length.to_le_bytes());
    target.extend_from_slice(payload);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.ensure_remaining(N)?;
        let end = self.offset + N;
        let mut bytes = [0_u8; N];
        bytes.copy_from_slice(&self.bytes[self.offset..end]);
        self.offset = end;
        Ok(bytes)
    }

    fn read_length_prefixed(&mut self) -> Result<&'a [u8]> {
        let length = usize::try_from(u32::from_le_bytes(self.read_array()?))
            .map_err(|_| corruption("tuple length is not addressable"))?;
        self.ensure_remaining(length)?;
        let end = self.offset + length;
        let payload = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(payload)
    }

    fn ensure_remaining(&self, needed: usize) -> Result<()> {
        let remaining = self.bytes.len().saturating_sub(self.offset);
        if needed > remaining {
            return Err(corruption(format!(
                "tuple is truncated: needs {needed} bytes, only {remaining} remain"
            )));
        }
        Ok(())
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};
    use ordadb_types::Value;
    use rust_decimal::Decimal;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn every_value_variant_round_trips_exactly() {
        let date = NaiveDate::from_ymd_opt(2026, 7, 25).expect("date");
        let time = NaiveTime::from_hms_nano_opt(12, 34, 56, 123_456_789).expect("time");
        let row = Row::new(vec![
            Value::Null,
            Value::Boolean(true),
            Value::Int16(-16),
            Value::Int32(-32),
            Value::Int64(-64),
            Value::Float32(1.25),
            Value::Float64(-2.5),
            Value::Decimal(Decimal::new(12345, 3)),
            Value::Text("OrdaDB 数据".into()),
            Value::Binary(vec![0, 1, 255]),
            Value::Date(date),
            Value::Time(time),
            Value::Timestamp(date.and_time(time)),
            Value::Json(json!({"order": [1, 2]})),
            Value::Jsonb(json!({"stable": true})),
            Value::Uuid(Uuid::from_u128(0x12345678_1234_5678_9abc_def012345678)),
            Value::Vector(vec![1.0, -0.0, 3.5]),
        ]);

        let encoded = encode_row(&row).expect("encode");
        assert_eq!(decode_row(&encoded).expect("decode"), row);
    }

    #[test]
    fn malformed_truncated_and_trailing_payloads_are_rejected() {
        assert_eq!(
            decode_row(&[1, 0, TAG_BOOLEAN, 2])
                .expect_err("bool")
                .sql_state,
            "XX001"
        );
        assert_eq!(
            decode_row(&[1, 0, TAG_INT64, 1])
                .expect_err("truncated")
                .sql_state,
            "XX001"
        );

        let mut encoded = encode_row(&Row::new(vec![Value::Null])).expect("encode");
        encoded.push(99);
        assert_eq!(
            decode_row(&encoded).expect_err("trailing").sql_state,
            "XX001"
        );
        assert_eq!(
            decode_row(&[1, 0, 255]).expect_err("unknown tag").sql_state,
            "XX001"
        );
    }

    #[test]
    fn over_wide_tuple_is_rejected() {
        let row = Row::new(vec![Value::Null; usize::from(u16::MAX) + 1]);
        assert_eq!(
            encode_row(&row).expect_err("too many values").sql_state,
            "XX001"
        );
    }
}
