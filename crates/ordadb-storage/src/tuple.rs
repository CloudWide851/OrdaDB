use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};
use ordadb_types::{DbError, PgArray, Result, Row, Value};
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
const TAG_INTERVAL: u8 = 17;
const TAG_ARRAY: u8 = 18;

pub const TUPLE_FORMAT_V1: u16 = 1;
pub const TUPLE_FORMAT_V2: u16 = 2;
pub const TUPLE_HEADER_V2_BYTES: u16 = 32;
pub const FROZEN_TRANSACTION_ID: u64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleHeaderV2 {
    pub flags: u16,
    pub column_count: u16,
    pub xmin: u64,
    pub xmax: u64,
    pub command_id: u32,
    pub previous_version: u32,
}

impl TupleHeaderV2 {
    pub fn frozen(row: &Row) -> Result<Self> {
        Ok(Self {
            flags: 0,
            column_count: value_count(row)?,
            xmin: FROZEN_TRANSACTION_ID,
            xmax: 0,
            command_id: 0,
            previous_version: 0,
        })
    }

    fn validate(self) -> Result<Self> {
        if self.flags != 0 {
            return Err(corruption(format!(
                "tuple v2 uses unsupported flags 0x{:04x}",
                self.flags
            )));
        }
        if self.xmin == 0 {
            return Err(corruption("tuple v2 xmin must be non-zero"));
        }
        Ok(self)
    }
}

pub fn encode_row(row: &Row) -> Result<Vec<u8>> {
    let value_count = value_count(row)?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&value_count.to_le_bytes());
    encode_values(row, &mut bytes)?;
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

pub fn encode_row_v2(row: &Row, header: TupleHeaderV2) -> Result<Vec<u8>> {
    let header = header.validate()?;
    let actual_count = value_count(row)?;
    if header.column_count != actual_count {
        return Err(corruption(format!(
            "tuple v2 header declares {} columns for a row with {actual_count}",
            header.column_count
        )));
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&TUPLE_FORMAT_V2.to_le_bytes());
    bytes.extend_from_slice(&TUPLE_HEADER_V2_BYTES.to_le_bytes());
    bytes.extend_from_slice(&header.flags.to_le_bytes());
    bytes.extend_from_slice(&header.column_count.to_le_bytes());
    bytes.extend_from_slice(&header.xmin.to_le_bytes());
    bytes.extend_from_slice(&header.xmax.to_le_bytes());
    bytes.extend_from_slice(&header.command_id.to_le_bytes());
    bytes.extend_from_slice(&header.previous_version.to_le_bytes());
    encode_values(row, &mut bytes)?;
    Ok(bytes)
}

pub fn decode_row_v2(bytes: &[u8]) -> Result<(TupleHeaderV2, Row)> {
    let mut cursor = Cursor::new(bytes);
    let format_version = cursor.read_u16()?;
    if format_version != TUPLE_FORMAT_V2 {
        return Err(unsupported_tuple_version(format_version));
    }
    let header_bytes = cursor.read_u16()?;
    if header_bytes != TUPLE_HEADER_V2_BYTES {
        return Err(corruption(format!(
            "tuple v2 header is {header_bytes} bytes; expected {TUPLE_HEADER_V2_BYTES}"
        )));
    }
    let header = TupleHeaderV2 {
        flags: cursor.read_u16()?,
        column_count: cursor.read_u16()?,
        xmin: cursor.read_u64()?,
        xmax: cursor.read_u64()?,
        command_id: cursor.read_u32()?,
        previous_version: cursor.read_u32()?,
    }
    .validate()?;
    let mut values = Vec::with_capacity(usize::from(header.column_count));
    for _ in 0..header.column_count {
        values.push(decode_value(&mut cursor)?);
    }
    if !cursor.is_finished() {
        return Err(corruption("tuple v2 contains trailing bytes"));
    }
    Ok((header, Row::new(values)))
}

fn value_count(row: &Row) -> Result<u16> {
    u16::try_from(row.values.len()).map_err(|_| corruption("tuple contains more than 65535 values"))
}

fn encode_values(row: &Row, bytes: &mut Vec<u8>) -> Result<()> {
    for value in &row.values {
        encode_value(value, bytes)?;
    }
    Ok(())
}

fn unsupported_tuple_version(version: u16) -> DbError {
    DbError::new(
        "0A000",
        format!("tuple format version {version} is not supported"),
    )
    .with_detail(format!(
        "this OrdaDB build supports tuple format versions {TUPLE_FORMAT_V1} and {TUPLE_FORMAT_V2}"
    ))
    .with_hint("restore from a compatible logical backup or run an explicit migration")
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
        Value::Interval(value) => {
            bytes.push(TAG_INTERVAL);
            bytes.extend_from_slice(&value.months.to_le_bytes());
            bytes.extend_from_slice(&value.days.to_le_bytes());
            bytes.extend_from_slice(&value.microseconds.to_le_bytes());
        }
        Value::Array(value) => {
            bytes.push(TAG_ARRAY);
            let encoded = serde_json::to_vec(value)
                .map_err(|error| corruption(format!("array encoding failed: {error}")))?;
            write_length_prefixed(bytes, &encoded)?;
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
        TAG_INTERVAL => Ok(Value::Interval(ordadb_types::PgInterval::new(
            i32::from_le_bytes(cursor.read_array()?),
            i32::from_le_bytes(cursor.read_array()?),
            i64::from_le_bytes(cursor.read_array()?),
        ))),
        TAG_ARRAY => {
            let payload = cursor.read_length_prefixed()?;
            let decoded: PgArray = serde_json::from_slice(payload)
                .map_err(|error| corruption(format!("tuple array is malformed: {error}")))?;
            let validated = PgArray::new(
                decoded.element_type().clone(),
                decoded.dimensions().to_vec(),
                decoded.values().to_vec(),
            )
            .map_err(|error| corruption(format!("tuple array is invalid: {}", error.message)))?;
            Ok(Value::Array(validated))
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

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_array()?))
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
    use ordadb_types::{ArrayDimension, PgArray, PgInterval, ScalarType, Value};
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
            Value::Interval(PgInterval::new(14, 3, 4_500_000)),
            Value::Array(
                PgArray::new(
                    ScalarType::Text,
                    vec![ArrayDimension::new(2, -1)],
                    vec![Value::Text("a".into()), Value::Null],
                )
                .expect("array"),
            ),
            Value::Json(json!({"order": [1, 2]})),
            Value::Jsonb(json!({"stable": true})),
            Value::Uuid(Uuid::from_u128(0x12345678_1234_5678_9abc_def012345678)),
            Value::Vector(vec![1.0, -0.0, 3.5]),
        ]);

        let encoded = encode_row(&row).expect("encode");
        assert_eq!(decode_row(&encoded).expect("decode"), row);
        let header = TupleHeaderV2::frozen(&row).expect("header");
        let encoded = encode_row_v2(&row, header).expect("encode v2");
        assert_eq!(decode_row_v2(&encoded).expect("decode v2"), (header, row));
    }

    #[test]
    fn tuple_v2_header_has_a_stable_little_endian_golden_encoding() {
        let row = Row::new(vec![Value::Int64(42)]);
        let encoded = encode_row_v2(
            &row,
            TupleHeaderV2 {
                flags: 0,
                column_count: 1,
                xmin: FROZEN_TRANSACTION_ID,
                xmax: 0,
                command_id: 7,
                previous_version: 0x0102_0304,
            },
        )
        .expect("encode");
        assert_eq!(
            &encoded[..TUPLE_HEADER_V2_BYTES as usize],
            &[
                2, 0, 32, 0, 0, 0, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0,
                0, 4, 3, 2, 1,
            ]
        );
        assert_eq!(
            &encoded[TUPLE_HEADER_V2_BYTES as usize..],
            &[TAG_INT64, 42, 0, 0, 0, 0, 0, 0, 0]
        );
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
        assert_eq!(
            decode_row_v2(&[9, 0]).expect_err("version").sql_state,
            "0A000"
        );
        let row = Row::new(vec![Value::Null]);
        let mut encoded =
            encode_row_v2(&row, TupleHeaderV2::frozen(&row).expect("header")).expect("encode");
        encoded[28] = 1;
        let (header, decoded) = decode_row_v2(&encoded).expect("version predecessor");
        assert_eq!(header.previous_version, 1);
        assert_eq!(decoded, row);
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
