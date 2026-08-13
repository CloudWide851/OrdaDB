use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use ordadb_sql::BinaryOperator;
use ordadb_types::{DbError, PgArray, PgInterval, Result, Row, ScalarType, Value};
use rust_decimal::Decimal;
use std::sync::Arc;
use uuid::Uuid;

use crate::{MemoryGrant, Reservation, estimated_row_bytes, estimated_value_bytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnVectorKind {
    Null,
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal,
    Text,
    Binary,
    Date,
    Time,
    Timestamp,
    Interval,
    Array,
    Json,
    Jsonb,
    Uuid,
    Vector,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowColumnView {
    rows: Arc<Vec<Row>>,
    start: usize,
    end: usize,
    column: usize,
}

impl RowColumnView {
    fn value(&self, index: usize) -> Result<&Value> {
        if index >= self.len() {
            return Err(DbError::internal(
                "row-backed column vector index is out of bounds",
            ));
        }
        self.rows
            .get(self.start + index)
            .and_then(|row| row.values.get(self.column))
            .ok_or_else(|| DbError::internal("row-backed column value is unavailable"))
    }

    fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }
}

/// A typed, nullable vector owned by one execution chunk.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnVector {
    RowBacked {
        kind: ColumnVectorKind,
        view: RowColumnView,
    },
    Null(usize),
    Boolean(Vec<Option<bool>>),
    Int16(Vec<Option<i16>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Decimal(Vec<Option<Decimal>>),
    Text(Vec<Option<String>>),
    Binary(Vec<Option<Vec<u8>>>),
    Date(Vec<Option<NaiveDate>>),
    Time(Vec<Option<NaiveTime>>),
    Timestamp(Vec<Option<NaiveDateTime>>),
    Interval(Vec<Option<PgInterval>>),
    Array(Vec<Option<PgArray>>),
    Json(Vec<Option<serde_json::Value>>),
    Jsonb(Vec<Option<serde_json::Value>>),
    Uuid(Vec<Option<Uuid>>),
    Vector(Vec<Option<Vec<f32>>>),
}
