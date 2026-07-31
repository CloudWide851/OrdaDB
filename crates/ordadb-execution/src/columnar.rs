use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use ordadb_sql::BinaryOperator;
use ordadb_types::{DbError, Result, Row, ScalarType, Value};
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
    Json(Vec<Option<serde_json::Value>>),
    Jsonb(Vec<Option<serde_json::Value>>),
    Uuid(Vec<Option<Uuid>>),
    Vector(Vec<Option<Vec<f32>>>),
}

impl ColumnVector {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::RowBacked { view, .. } => view.len(),
            Self::Null(len) => *len,
            Self::Boolean(values) => values.len(),
            Self::Int16(values) => values.len(),
            Self::Int32(values) => values.len(),
            Self::Int64(values) => values.len(),
            Self::Float32(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Decimal(values) => values.len(),
            Self::Text(values) => values.len(),
            Self::Binary(values) => values.len(),
            Self::Date(values) => values.len(),
            Self::Time(values) => values.len(),
            Self::Timestamp(values) => values.len(),
            Self::Json(values) => values.len(),
            Self::Jsonb(values) => values.len(),
            Self::Uuid(values) => values.len(),
            Self::Vector(values) => values.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn value(&self, index: usize) -> Result<Value> {
        let missing = || DbError::internal("column vector index is out of bounds");
        Ok(match self {
            Self::RowBacked { view, .. } => view.value(index)?.clone(),
            Self::Null(len) => {
                if index >= *len {
                    return Err(missing());
                }
                Value::Null
            }
            Self::Boolean(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Boolean),
            Self::Int16(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Int16),
            Self::Int32(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Int32),
            Self::Int64(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Int64),
            Self::Float32(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Float32),
            Self::Float64(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Float64),
            Self::Decimal(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Decimal),
            Self::Text(values) => values
                .get(index)
                .ok_or_else(missing)?
                .as_ref()
                .map_or(Value::Null, |value| Value::Text(value.clone())),
            Self::Binary(values) => values
                .get(index)
                .ok_or_else(missing)?
                .as_ref()
                .map_or(Value::Null, |value| Value::Binary(value.clone())),
            Self::Date(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Date),
            Self::Time(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Time),
            Self::Timestamp(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Timestamp),
            Self::Json(values) => values
                .get(index)
                .ok_or_else(missing)?
                .as_ref()
                .map_or(Value::Null, |value| Value::Json(value.clone())),
            Self::Jsonb(values) => values
                .get(index)
                .ok_or_else(missing)?
                .as_ref()
                .map_or(Value::Null, |value| Value::Jsonb(value.clone())),
            Self::Uuid(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Uuid),
            Self::Vector(values) => values
                .get(index)
                .ok_or_else(missing)?
                .as_ref()
                .map_or(Value::Null, |value| Value::Vector(value.clone())),
        })
    }

    fn take_value(&mut self, index: usize) -> Result<Value> {
        let missing = || DbError::internal("column vector index is out of bounds");
        Ok(match self {
            Self::RowBacked { view, .. } => view.value(index)?.clone(),
            Self::Null(len) => {
                if index >= *len {
                    return Err(missing());
                }
                Value::Null
            }
            Self::Boolean(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Boolean),
            Self::Int16(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Int16),
            Self::Int32(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Int32),
            Self::Int64(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Int64),
            Self::Float32(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Float32),
            Self::Float64(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Float64),
            Self::Decimal(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Decimal),
            Self::Text(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Text),
            Self::Binary(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Binary),
            Self::Date(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Date),
            Self::Time(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Time),
            Self::Timestamp(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Timestamp),
            Self::Json(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Json),
            Self::Jsonb(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Jsonb),
            Self::Uuid(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Uuid),
            Self::Vector(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Vector),
        })
    }

    pub(crate) fn compare_literal(
        &self,
        index: usize,
        literal: &Value,
        operator: BinaryOperator,
    ) -> Option<Result<Value>> {
        macro_rules! compare {
            ($values:expr, $literal:expr) => {{
                let value = match $values.get(index) {
                    Some(Some(value)) => value,
                    Some(None) => return Some(Ok(Value::Null)),
                    None => {
                        return Some(Err(DbError::internal(
                            "column vector index is out of bounds",
                        )));
                    }
                };
                Some(compare_scalar(value, $literal, operator))
            }};
        }
        match (self, literal) {
            (Self::RowBacked { kind, view }, literal)
                if row_backed_literal_supported(*kind, literal) =>
            {
                compare_value_literal(view.value(index), literal, operator)
            }
            (Self::Boolean(values), Value::Boolean(literal)) => compare!(values, literal),
            (Self::Int16(values), Value::Int16(literal)) => compare!(values, literal),
            (Self::Int32(values), Value::Int32(literal)) => compare!(values, literal),
            (Self::Int64(values), Value::Int64(literal)) => compare!(values, literal),
            (Self::Decimal(values), Value::Decimal(literal)) => compare!(values, literal),
            (Self::Text(values), Value::Text(literal)) => compare!(values, literal),
            (Self::Date(values), Value::Date(literal)) => compare!(values, literal),
            (Self::Time(values), Value::Time(literal)) => compare!(values, literal),
            (Self::Timestamp(values), Value::Timestamp(literal)) => compare!(values, literal),
            (Self::Uuid(values), Value::Uuid(literal)) => compare!(values, literal),
            (Self::Null(len), _) if index < *len => Some(Ok(Value::Null)),
            _ => None,
        }
    }

    fn retain_literal_comparison(
        &self,
        indexes: &mut Vec<u32>,
        literal: &Value,
        operator: BinaryOperator,
    ) -> Option<Result<()>> {
        if !matches!(
            operator,
            BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
        ) {
            return None;
        }
        macro_rules! retain {
            ($values:expr, $literal:expr) => {{
                if indexes.iter().any(|index| *index as usize >= $values.len()) {
                    return Some(Err(DbError::internal(
                        "selection vector index is outside the column vector",
                    )));
                }
                indexes.retain(|index| {
                    $values[*index as usize]
                        .as_ref()
                        .is_some_and(|value| scalar_predicate(value, $literal, operator))
                });
                Some(Ok(()))
            }};
        }
        match (self, literal) {
            (Self::RowBacked { kind, view }, literal)
                if row_backed_literal_supported(*kind, literal) =>
            {
                for index in indexes.iter().copied() {
                    if let Err(error) = view.value(index as usize) {
                        return Some(Err(error));
                    }
                }
                indexes.retain(|index| {
                    view.value(*index as usize)
                        .ok()
                        .is_some_and(|value| value_predicate(value, literal, operator))
                });
                Some(Ok(()))
            }
            (Self::Boolean(values), Value::Boolean(literal)) => retain!(values, literal),
            (Self::Int16(values), Value::Int16(literal)) => retain!(values, literal),
            (Self::Int32(values), Value::Int32(literal)) => retain!(values, literal),
            (Self::Int64(values), Value::Int64(literal)) => retain!(values, literal),
            (Self::Decimal(values), Value::Decimal(literal)) => retain!(values, literal),
            (Self::Text(values), Value::Text(literal)) => retain!(values, literal),
            (Self::Date(values), Value::Date(literal)) => retain!(values, literal),
            (Self::Time(values), Value::Time(literal)) => retain!(values, literal),
            (Self::Timestamp(values), Value::Timestamp(literal)) => retain!(values, literal),
            (Self::Uuid(values), Value::Uuid(literal)) => retain!(values, literal),
            (Self::Null(len), _) => {
                if indexes.iter().any(|index| *index as usize >= *len) {
                    return Some(Err(DbError::internal(
                        "selection vector index is outside the null column",
                    )));
                }
                indexes.clear();
                Some(Ok(()))
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::RowBacked { .. } => 0,
                Self::Null(_) => 0,
                Self::Boolean(values) => values.capacity() * std::mem::size_of::<Option<bool>>(),
                Self::Int16(values) => values.capacity() * std::mem::size_of::<Option<i16>>(),
                Self::Int32(values) => values.capacity() * std::mem::size_of::<Option<i32>>(),
                Self::Int64(values) => values.capacity() * std::mem::size_of::<Option<i64>>(),
                Self::Float32(values) => values.capacity() * std::mem::size_of::<Option<f32>>(),
                Self::Float64(values) => values.capacity() * std::mem::size_of::<Option<f64>>(),
                Self::Decimal(values) => values.capacity() * std::mem::size_of::<Option<Decimal>>(),
                Self::Text(values) => {
                    values.capacity() * std::mem::size_of::<Option<String>>()
                        + values.iter().flatten().map(String::capacity).sum::<usize>()
                }
                Self::Binary(values) => {
                    values.capacity() * std::mem::size_of::<Option<Vec<u8>>>()
                        + values.iter().flatten().map(Vec::capacity).sum::<usize>()
                }
                Self::Date(values) => values.capacity() * std::mem::size_of::<Option<NaiveDate>>(),
                Self::Time(values) => values.capacity() * std::mem::size_of::<Option<NaiveTime>>(),
                Self::Timestamp(values) => {
                    values.capacity() * std::mem::size_of::<Option<NaiveDateTime>>()
                }
                Self::Json(values) | Self::Jsonb(values) => {
                    values.capacity() * std::mem::size_of::<Option<serde_json::Value>>()
                }
                Self::Uuid(values) => values.capacity() * std::mem::size_of::<Option<Uuid>>(),
                Self::Vector(values) => {
                    values.capacity() * std::mem::size_of::<Option<Vec<f32>>>()
                        + values
                            .iter()
                            .flatten()
                            .map(|value| value.capacity() * std::mem::size_of::<f32>())
                            .sum::<usize>()
                }
            }
    }

    fn estimated_value_bytes(&self, index: usize) -> Result<usize> {
        let base = std::mem::size_of::<Value>();
        Ok(match self {
            Self::RowBacked { view, .. } => estimated_value_bytes(view.value(index)?),
            Self::Null(len) => {
                if index >= *len {
                    return Err(DbError::internal("column vector index is out of bounds"));
                }
                base
            }
            Self::Text(values) => {
                base + values
                    .get(index)
                    .ok_or_else(|| DbError::internal("column vector index is out of bounds"))?
                    .as_ref()
                    .map_or(0, String::len)
            }
            Self::Binary(values) => {
                base + values
                    .get(index)
                    .ok_or_else(|| DbError::internal("column vector index is out of bounds"))?
                    .as_ref()
                    .map_or(0, Vec::len)
            }
            Self::Json(values) | Self::Jsonb(values) => {
                base + values
                    .get(index)
                    .ok_or_else(|| DbError::internal("column vector index is out of bounds"))?
                    .as_ref()
                    .map(serde_json::Value::to_string)
                    .map_or(0, |value| value.len())
            }
            Self::Vector(values) => {
                base + values
                    .get(index)
                    .ok_or_else(|| DbError::internal("column vector index is out of bounds"))?
                    .as_ref()
                    .map_or(0, |value| {
                        value.len().saturating_mul(std::mem::size_of::<f32>())
                    })
            }
            _ => {
                if index >= self.len() {
                    return Err(DbError::internal("column vector index is out of bounds"));
                }
                base
            }
        })
    }

    fn fixed_value_bytes(&self) -> Option<usize> {
        match self {
            Self::RowBacked { .. }
            | Self::Text(_)
            | Self::Binary(_)
            | Self::Json(_)
            | Self::Jsonb(_)
            | Self::Vector(_) => None,
            _ => Some(std::mem::size_of::<Value>()),
        }
    }

    fn with_kind(kind: ColumnVectorKind, capacity: usize) -> Self {
        match kind {
            ColumnVectorKind::Null => Self::Null(0),
            ColumnVectorKind::Boolean => Self::Boolean(Vec::with_capacity(capacity)),
            ColumnVectorKind::Int16 => Self::Int16(Vec::with_capacity(capacity)),
            ColumnVectorKind::Int32 => Self::Int32(Vec::with_capacity(capacity)),
            ColumnVectorKind::Int64 => Self::Int64(Vec::with_capacity(capacity)),
            ColumnVectorKind::Float32 => Self::Float32(Vec::with_capacity(capacity)),
            ColumnVectorKind::Float64 => Self::Float64(Vec::with_capacity(capacity)),
            ColumnVectorKind::Decimal => Self::Decimal(Vec::with_capacity(capacity)),
            ColumnVectorKind::Text => Self::Text(Vec::with_capacity(capacity)),
            ColumnVectorKind::Binary => Self::Binary(Vec::with_capacity(capacity)),
            ColumnVectorKind::Date => Self::Date(Vec::with_capacity(capacity)),
            ColumnVectorKind::Time => Self::Time(Vec::with_capacity(capacity)),
            ColumnVectorKind::Timestamp => Self::Timestamp(Vec::with_capacity(capacity)),
            ColumnVectorKind::Json => Self::Json(Vec::with_capacity(capacity)),
            ColumnVectorKind::Jsonb => Self::Jsonb(Vec::with_capacity(capacity)),
            ColumnVectorKind::Uuid => Self::Uuid(Vec::with_capacity(capacity)),
            ColumnVectorKind::Vector => Self::Vector(Vec::with_capacity(capacity)),
        }
    }

    fn kind(&self) -> ColumnVectorKind {
        match self {
            Self::RowBacked { kind, .. } => *kind,
            Self::Null(_) => ColumnVectorKind::Null,
            Self::Boolean(_) => ColumnVectorKind::Boolean,
            Self::Int16(_) => ColumnVectorKind::Int16,
            Self::Int32(_) => ColumnVectorKind::Int32,
            Self::Int64(_) => ColumnVectorKind::Int64,
            Self::Float32(_) => ColumnVectorKind::Float32,
            Self::Float64(_) => ColumnVectorKind::Float64,
            Self::Decimal(_) => ColumnVectorKind::Decimal,
            Self::Text(_) => ColumnVectorKind::Text,
            Self::Binary(_) => ColumnVectorKind::Binary,
            Self::Date(_) => ColumnVectorKind::Date,
            Self::Time(_) => ColumnVectorKind::Time,
            Self::Timestamp(_) => ColumnVectorKind::Timestamp,
            Self::Json(_) => ColumnVectorKind::Json,
            Self::Jsonb(_) => ColumnVectorKind::Jsonb,
            Self::Uuid(_) => ColumnVectorKind::Uuid,
            Self::Vector(_) => ColumnVectorKind::Vector,
        }
    }

    pub(crate) fn matches_type(&self, target: &ScalarType) -> bool {
        matches!(
            (self.kind(), target),
            (ColumnVectorKind::Null, _)
                | (ColumnVectorKind::Boolean, ScalarType::Boolean)
                | (ColumnVectorKind::Int16, ScalarType::Int16)
                | (ColumnVectorKind::Int32, ScalarType::Int32)
                | (ColumnVectorKind::Int64, ScalarType::Int64)
                | (ColumnVectorKind::Float32, ScalarType::Float32)
                | (ColumnVectorKind::Float64, ScalarType::Float64)
                | (ColumnVectorKind::Decimal, ScalarType::Decimal { .. })
                | (
                    ColumnVectorKind::Text,
                    ScalarType::Text | ScalarType::Char { .. } | ScalarType::Varchar { .. }
                )
                | (ColumnVectorKind::Binary, ScalarType::Binary)
                | (ColumnVectorKind::Date, ScalarType::Date)
                | (ColumnVectorKind::Time, ScalarType::Time)
                | (ColumnVectorKind::Timestamp, ScalarType::Timestamp { .. })
                | (ColumnVectorKind::Json, ScalarType::Json)
                | (ColumnVectorKind::Jsonb, ScalarType::Jsonb)
                | (ColumnVectorKind::Uuid, ScalarType::Uuid)
                | (ColumnVectorKind::Vector, ScalarType::Vector { .. })
        )
    }

    fn clear(&mut self) {
        match self {
            Self::RowBacked { view, .. } => view.start = view.end,
            Self::Null(len) => *len = 0,
            Self::Boolean(values) => values.clear(),
            Self::Int16(values) => values.clear(),
            Self::Int32(values) => values.clear(),
            Self::Int64(values) => values.clear(),
            Self::Float32(values) => values.clear(),
            Self::Float64(values) => values.clear(),
            Self::Decimal(values) => values.clear(),
            Self::Text(values) => values.clear(),
            Self::Binary(values) => values.clear(),
            Self::Date(values) => values.clear(),
            Self::Time(values) => values.clear(),
            Self::Timestamp(values) => values.clear(),
            Self::Json(values) => values.clear(),
            Self::Jsonb(values) => values.clear(),
            Self::Uuid(values) => values.clear(),
            Self::Vector(values) => values.clear(),
        }
    }

    fn push(&mut self, value: &Value) -> Result<()> {
        match (self, value) {
            (Self::RowBacked { .. }, _) => {
                return Err(DbError::internal(
                    "cannot append to a row-backed column vector",
                ));
            }
            (Self::Null(len), Value::Null) => *len = len.saturating_add(1),
            (Self::Boolean(values), Value::Boolean(value)) => values.push(Some(*value)),
            (Self::Int16(values), Value::Int16(value)) => values.push(Some(*value)),
            (Self::Int32(values), Value::Int32(value)) => values.push(Some(*value)),
            (Self::Int64(values), Value::Int64(value)) => values.push(Some(*value)),
            (Self::Float32(values), Value::Float32(value)) => values.push(Some(*value)),
            (Self::Float64(values), Value::Float64(value)) => values.push(Some(*value)),
            (Self::Decimal(values), Value::Decimal(value)) => values.push(Some(*value)),
            (Self::Text(values), Value::Text(value)) => values.push(Some(value.clone())),
            (Self::Binary(values), Value::Binary(value)) => values.push(Some(value.clone())),
            (Self::Date(values), Value::Date(value)) => values.push(Some(*value)),
            (Self::Time(values), Value::Time(value)) => values.push(Some(*value)),
            (Self::Timestamp(values), Value::Timestamp(value)) => values.push(Some(*value)),
            (Self::Json(values), Value::Json(value)) => values.push(Some(value.clone())),
            (Self::Jsonb(values), Value::Jsonb(value)) => values.push(Some(value.clone())),
            (Self::Uuid(values), Value::Uuid(value)) => values.push(Some(*value)),
            (Self::Vector(values), Value::Vector(value)) => values.push(Some(value.clone())),
            (Self::Boolean(values), Value::Null) => values.push(None),
            (Self::Int16(values), Value::Null) => values.push(None),
            (Self::Int32(values), Value::Null) => values.push(None),
            (Self::Int64(values), Value::Null) => values.push(None),
            (Self::Float32(values), Value::Null) => values.push(None),
            (Self::Float64(values), Value::Null) => values.push(None),
            (Self::Decimal(values), Value::Null) => values.push(None),
            (Self::Text(values), Value::Null) => values.push(None),
            (Self::Binary(values), Value::Null) => values.push(None),
            (Self::Date(values), Value::Null) => values.push(None),
            (Self::Time(values), Value::Null) => values.push(None),
            (Self::Timestamp(values), Value::Null) => values.push(None),
            (Self::Json(values), Value::Null) => values.push(None),
            (Self::Jsonb(values), Value::Null) => values.push(None),
            (Self::Uuid(values), Value::Null) => values.push(None),
            (Self::Vector(values), Value::Null) => values.push(None),
            _ => {
                return Err(DbError::new(
                    "42804",
                    "column chunk contains incompatible value types",
                ));
            }
        }
        Ok(())
    }
}

/// Logical row indexes selected from a physical column batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectionVector {
    indexes: Vec<u32>,
}

impl SelectionVector {
    pub fn all(row_count: usize) -> Result<Self> {
        let indexes = (0..row_count)
            .map(|index| {
                u32::try_from(index).map_err(|_| {
                    DbError::new("54000", "data chunk exceeds selection-vector capacity")
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { indexes })
    }

    fn reset_all(&mut self, row_count: usize) -> Result<()> {
        let row_count = u32::try_from(row_count)
            .map_err(|_| DbError::new("54000", "data chunk exceeds selection-vector capacity"))?;
        self.indexes.clear();
        self.indexes.extend(0..row_count);
        Ok(())
    }

    pub fn from_indexes(indexes: Vec<u32>, physical_rows: usize) -> Result<Self> {
        if indexes
            .iter()
            .any(|index| usize::try_from(*index).map_or(true, |index| index >= physical_rows))
        {
            return Err(DbError::internal(
                "selection vector index is out of physical chunk bounds",
            ));
        }
        Ok(Self { indexes })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub fn retain(&mut self, mut predicate: impl FnMut(usize) -> Result<bool>) -> Result<()> {
        let mut retained = Vec::with_capacity(self.indexes.len());
        for index in self.indexes.iter().copied() {
            if predicate(index as usize)? {
                retained.push(index);
            }
        }
        self.indexes = retained;
        Ok(())
    }

    pub fn truncate(&mut self, len: usize) {
        self.indexes.truncate(len);
    }

    fn physical_index(&self, logical_index: usize) -> Result<usize> {
        self.indexes
            .get(logical_index)
            .copied()
            .map(|index| index as usize)
            .ok_or_else(|| DbError::internal("logical chunk row is out of bounds"))
    }
}

/// A columnar execution batch. Public row APIs materialize only at boundaries.
#[derive(Debug, Clone, PartialEq)]
pub struct DataChunk {
    columns: Vec<ColumnVector>,
    selection: SelectionVector,
    physical_rows: usize,
}

impl DataChunk {
    pub fn from_rows(rows: &[Row]) -> Result<Self> {
        let kinds = infer_kinds(rows)?;
        let mut columns = kinds
            .iter()
            .map(|kind| ColumnVector::with_kind(*kind, rows.len()))
            .collect::<Vec<_>>();
        append_rows(&mut columns, rows)?;
        Ok(Self {
            columns,
            selection: SelectionVector::all(rows.len())?,
            physical_rows: rows.len(),
        })
    }

    pub(crate) fn from_row_snapshot(rows: Arc<Vec<Row>>, start: usize, end: usize) -> Result<Self> {
        if start > end || end > rows.len() {
            return Err(DbError::internal("row-backed data chunk range is invalid"));
        }
        let kinds = infer_kinds(&rows[start..end])?;
        let columns = kinds
            .into_iter()
            .enumerate()
            .map(|(column, kind)| ColumnVector::RowBacked {
                kind,
                view: RowColumnView {
                    rows: Arc::clone(&rows),
                    start,
                    end,
                    column,
                },
            })
            .collect::<Vec<_>>();
        let physical_rows = end - start;
        Ok(Self {
            columns,
            selection: SelectionVector::all(physical_rows)?,
            physical_rows,
        })
    }

    pub fn from_columns(columns: Vec<ColumnVector>) -> Result<Self> {
        let physical_rows = columns.first().map_or(0, ColumnVector::len);
        if columns.iter().any(|column| column.len() != physical_rows) {
            return Err(DbError::internal(
                "data chunk columns have different physical lengths",
            ));
        }
        Ok(Self {
            columns,
            selection: SelectionVector::all(physical_rows)?,
            physical_rows,
        })
    }

    #[must_use]
    pub fn columns(&self) -> &[ColumnVector] {
        &self.columns
    }

    #[must_use]
    pub fn selection(&self) -> &SelectionVector {
        &self.selection
    }

    pub fn selection_mut(&mut self) -> &mut SelectionVector {
        &mut self.selection
    }

    #[must_use]
    pub fn physical_rows(&self) -> usize {
        self.physical_rows
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.selection.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.selection.is_empty()
    }

    pub fn row(&self, logical_index: usize) -> Result<Row> {
        let physical_index = self.selection.physical_index(logical_index)?;
        self.physical_row(physical_index)
    }

    pub fn physical_row(&self, physical_index: usize) -> Result<Row> {
        if physical_index >= self.physical_rows {
            return Err(DbError::internal("physical chunk row is out of bounds"));
        }
        self.columns
            .iter()
            .map(|column| column.value(physical_index))
            .collect::<Result<Vec<_>>>()
            .map(Row::new)
    }

    pub fn into_rows(mut self) -> Result<Vec<Row>> {
        self.take_rows()
    }

    pub(crate) fn take_rows(&mut self) -> Result<Vec<Row>> {
        let indexes = std::mem::take(&mut self.selection.indexes);
        let mut rows = Vec::with_capacity(indexes.len());
        if self.columns.is_empty() {
            rows.resize_with(indexes.len(), || Row::new(Vec::new()));
            self.physical_rows = 0;
            return Ok(rows);
        }
        if let Some((snapshot, start, end)) = identity_row_snapshot(&self.columns) {
            for physical_index in indexes {
                let physical_index = physical_index as usize;
                if physical_index >= end.saturating_sub(start) {
                    return Err(DbError::internal(
                        "row-backed selection index is out of bounds",
                    ));
                }
                rows.push(
                    snapshot
                        .get(start + physical_index)
                        .cloned()
                        .ok_or_else(|| {
                            DbError::internal("row-backed snapshot row is unavailable")
                        })?,
                );
            }
            self.physical_rows = 0;
            return Ok(rows);
        }
        if self.columns.len() == 1 {
            take_single_column_rows(&mut self.columns[0], indexes, &mut rows)?;
            self.physical_rows = 0;
            return Ok(rows);
        }
        for physical_index in indexes {
            let values = self
                .columns
                .iter_mut()
                .map(|column| column.take_value(physical_index as usize))
                .collect::<Result<Vec<_>>>()?;
            rows.push(Row::new(values));
        }
        self.physical_rows = 0;
        Ok(rows)
    }

    pub(crate) fn value(&self, column: usize, physical_row: usize) -> Result<Value> {
        self.columns
            .get(column)
            .ok_or_else(|| DbError::internal("column index is outside the data chunk"))?
            .value(physical_row)
    }

    pub(crate) fn compare_literal(
        &self,
        column: usize,
        physical_row: usize,
        literal: &Value,
        operator: BinaryOperator,
    ) -> Option<Result<Value>> {
        self.columns
            .get(column)?
            .compare_literal(physical_row, literal, operator)
    }

    pub(crate) fn retain_selected(
        &mut self,
        mut predicate: impl FnMut(&Self, usize) -> Result<bool>,
    ) -> Result<()> {
        let indexes = std::mem::take(&mut self.selection.indexes);
        let mut retained = Vec::with_capacity(indexes.len());
        for index in indexes {
            if predicate(self, index as usize)? {
                retained.push(index);
            }
        }
        self.selection.indexes = retained;
        Ok(())
    }

    pub(crate) fn retain_literal_comparison(
        &mut self,
        column: usize,
        literal: &Value,
        operator: BinaryOperator,
    ) -> Option<Result<()>> {
        let column = self.columns.get(column)?;
        column.retain_literal_comparison(&mut self.selection.indexes, literal, operator)
    }

    pub(crate) fn project_columns_in_place(
        &mut self,
        projections: &[(usize, ScalarType)],
    ) -> Result<bool> {
        if !self.can_project_columns(projections)? {
            return Ok(false);
        }
        if projections.len() == self.columns.len()
            && projections
                .iter()
                .enumerate()
                .all(|(position, (index, _))| position == *index)
        {
            return Ok(true);
        }
        let mut source = std::mem::take(&mut self.columns)
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        let mut columns = Vec::with_capacity(projections.len());
        for (index, _) in projections {
            columns.push(
                source
                    .get_mut(*index)
                    .and_then(Option::take)
                    .ok_or_else(|| DbError::internal("projection column disappeared"))?,
            );
        }
        self.columns = columns;
        Ok(true)
    }

    pub(crate) fn can_project_columns(&self, projections: &[(usize, ScalarType)]) -> Result<bool> {
        if projections
            .iter()
            .enumerate()
            .any(|(position, (index, _))| {
                projections[..position]
                    .iter()
                    .any(|(earlier, _)| earlier == index)
            })
        {
            return Ok(false);
        }
        for (index, target) in projections {
            let column = self
                .columns
                .get(*index)
                .ok_or_else(|| DbError::internal("projection column is outside the data chunk"))?;
            if !column.matches_type(target) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .columns
                .iter()
                .map(ColumnVector::estimated_bytes)
                .sum::<usize>()
            + self.selection.indexes.capacity() * std::mem::size_of::<u32>()
    }

    pub(crate) fn estimated_selected_row_bytes(&self) -> Result<usize> {
        let mut fixed_row_bytes = std::mem::size_of::<Row>();
        for column in &self.columns {
            if let Some(bytes) = column.fixed_value_bytes() {
                fixed_row_bytes = fixed_row_bytes.checked_add(bytes).ok_or_else(|| {
                    DbError::new("53200", "query memory limit exceeded")
                        .with_detail("selected row estimate overflow")
                })?;
            }
        }
        let mut total = fixed_row_bytes
            .checked_mul(self.selection.indexes.len())
            .ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("selected chunk estimate overflow")
            })?;
        for column in &self.columns {
            if column.fixed_value_bytes().is_some() {
                continue;
            }
            for physical_index in &self.selection.indexes {
                total = total
                    .checked_add(column.estimated_value_bytes(*physical_index as usize)?)
                    .ok_or_else(|| {
                        DbError::new("53200", "query memory limit exceeded")
                            .with_detail("selected chunk estimate overflow")
                    })?;
            }
        }
        Ok(total)
    }

    fn reset_from_rows(&mut self, rows: &[Row]) -> Result<bool> {
        let kinds = rows.first().map_or_else(
            || Ok(Vec::new()),
            |row| {
                row.values
                    .iter()
                    .map(value_kind)
                    .collect::<Result<Vec<_>>>()
            },
        )?;
        if kinds.len() != self.columns.len()
            || kinds
                .iter()
                .zip(&self.columns)
                .any(|(kind, column)| *kind != column.kind())
        {
            return Ok(false);
        }
        for column in &mut self.columns {
            column.clear();
        }
        append_rows(&mut self.columns, rows)?;
        self.selection.reset_all(rows.len())?;
        self.physical_rows = rows.len();
        Ok(true)
    }
}

fn identity_row_snapshot(columns: &[ColumnVector]) -> Option<(Arc<Vec<Row>>, usize, usize)> {
    let ColumnVector::RowBacked { view: first, .. } = columns.first()? else {
        return None;
    };
    if first.column != 0
        || first
            .rows
            .get(first.start)
            .is_some_and(|row| row.values.len() != columns.len())
        || columns
            .iter()
            .enumerate()
            .any(|(column, vector)| match vector {
                ColumnVector::RowBacked { view, .. } => {
                    view.column != column
                        || view.start != first.start
                        || view.end != first.end
                        || !Arc::ptr_eq(&view.rows, &first.rows)
                }
                _ => true,
            })
    {
        return None;
    }
    Some((Arc::clone(&first.rows), first.start, first.end))
}

fn take_single_column_rows(
    column: &mut ColumnVector,
    indexes: Vec<u32>,
    rows: &mut Vec<Row>,
) -> Result<()> {
    let missing = || DbError::internal("column vector index is out of bounds");
    macro_rules! take_values {
        ($values:expr, $constructor:path) => {
            for physical_index in indexes {
                let value = $values
                    .get_mut(physical_index as usize)
                    .ok_or_else(missing)?
                    .take()
                    .map_or(Value::Null, $constructor);
                rows.push(Row::new(vec![value]));
            }
        };
    }
    match column {
        ColumnVector::RowBacked { view, .. } => {
            for physical_index in indexes {
                rows.push(Row::new(vec![view.value(physical_index as usize)?.clone()]));
            }
        }
        ColumnVector::Null(len) => {
            for physical_index in indexes {
                if physical_index as usize >= *len {
                    return Err(missing());
                }
                rows.push(Row::new(vec![Value::Null]));
            }
        }
        ColumnVector::Boolean(values) => take_values!(values, Value::Boolean),
        ColumnVector::Int16(values) => take_values!(values, Value::Int16),
        ColumnVector::Int32(values) => take_values!(values, Value::Int32),
        ColumnVector::Int64(values) => take_values!(values, Value::Int64),
        ColumnVector::Float32(values) => take_values!(values, Value::Float32),
        ColumnVector::Float64(values) => take_values!(values, Value::Float64),
        ColumnVector::Decimal(values) => take_values!(values, Value::Decimal),
        ColumnVector::Text(values) => take_values!(values, Value::Text),
        ColumnVector::Binary(values) => take_values!(values, Value::Binary),
        ColumnVector::Date(values) => take_values!(values, Value::Date),
        ColumnVector::Time(values) => take_values!(values, Value::Time),
        ColumnVector::Timestamp(values) => take_values!(values, Value::Timestamp),
        ColumnVector::Json(values) => take_values!(values, Value::Json),
        ColumnVector::Jsonb(values) => take_values!(values, Value::Jsonb),
        ColumnVector::Uuid(values) => take_values!(values, Value::Uuid),
        ColumnVector::Vector(values) => take_values!(values, Value::Vector),
    }
    Ok(())
}

fn compare_scalar<T: PartialOrd + PartialEq>(
    value: &T,
    literal: &T,
    operator: BinaryOperator,
) -> Result<Value> {
    let compared = match scalar_predicate_checked(value, literal, operator) {
        Some(compared) => compared,
        None => {
            return Err(DbError::internal(
                "unsupported columnar comparison operator",
            ));
        }
    };
    Ok(Value::Boolean(compared))
}

fn scalar_predicate<T: PartialOrd + PartialEq>(
    value: &T,
    literal: &T,
    operator: BinaryOperator,
) -> bool {
    scalar_predicate_checked(value, literal, operator)
        .expect("columnar predicate operator was validated")
}

fn scalar_predicate_checked<T: PartialOrd + PartialEq>(
    value: &T,
    literal: &T,
    operator: BinaryOperator,
) -> Option<bool> {
    Some(match operator {
        BinaryOperator::Eq => value == literal,
        BinaryOperator::NotEq => value != literal,
        BinaryOperator::Lt => value < literal,
        BinaryOperator::LtEq => value <= literal,
        BinaryOperator::Gt => value > literal,
        BinaryOperator::GtEq => value >= literal,
        _ => {
            return None;
        }
    })
}

fn row_backed_literal_supported(kind: ColumnVectorKind, literal: &Value) -> bool {
    matches!(
        (kind, literal),
        (ColumnVectorKind::Boolean, Value::Boolean(_))
            | (ColumnVectorKind::Int16, Value::Int16(_))
            | (ColumnVectorKind::Int32, Value::Int32(_))
            | (ColumnVectorKind::Int64, Value::Int64(_))
            | (ColumnVectorKind::Decimal, Value::Decimal(_))
            | (ColumnVectorKind::Text, Value::Text(_))
            | (ColumnVectorKind::Date, Value::Date(_))
            | (ColumnVectorKind::Time, Value::Time(_))
            | (ColumnVectorKind::Timestamp, Value::Timestamp(_))
            | (ColumnVectorKind::Uuid, Value::Uuid(_))
            | (ColumnVectorKind::Null, _)
    )
}

fn value_predicate(value: &Value, literal: &Value, operator: BinaryOperator) -> bool {
    if value.is_null() || literal.is_null() {
        return false;
    }
    if matches!(operator, BinaryOperator::Eq | BinaryOperator::NotEq) {
        return if operator == BinaryOperator::Eq {
            value == literal
        } else {
            value != literal
        };
    }
    match (value, literal) {
        (Value::Boolean(value), Value::Boolean(literal)) => {
            scalar_predicate(value, literal, operator)
        }
        (Value::Int16(value), Value::Int16(literal)) => scalar_predicate(value, literal, operator),
        (Value::Int32(value), Value::Int32(literal)) => scalar_predicate(value, literal, operator),
        (Value::Int64(value), Value::Int64(literal)) => scalar_predicate(value, literal, operator),
        (Value::Decimal(value), Value::Decimal(literal)) => {
            scalar_predicate(value, literal, operator)
        }
        (Value::Text(value), Value::Text(literal)) => scalar_predicate(value, literal, operator),
        (Value::Date(value), Value::Date(literal)) => scalar_predicate(value, literal, operator),
        (Value::Time(value), Value::Time(literal)) => scalar_predicate(value, literal, operator),
        (Value::Timestamp(value), Value::Timestamp(literal)) => {
            scalar_predicate(value, literal, operator)
        }
        (Value::Uuid(value), Value::Uuid(literal)) => scalar_predicate(value, literal, operator),
        _ => false,
    }
}

fn compare_value_literal(
    value: Result<&Value>,
    literal: &Value,
    operator: BinaryOperator,
) -> Option<Result<Value>> {
    let value = match value {
        Ok(value) => value,
        Err(error) => return Some(Err(error)),
    };
    if value.is_null() || literal.is_null() {
        return Some(Ok(Value::Null));
    }
    if matches!(operator, BinaryOperator::Eq | BinaryOperator::NotEq) {
        return Some(Ok(Value::Boolean(if operator == BinaryOperator::Eq {
            value == literal
        } else {
            value != literal
        })));
    }
    match (value, literal) {
        (Value::Boolean(value), Value::Boolean(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Int16(value), Value::Int16(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Int32(value), Value::Int32(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Int64(value), Value::Int64(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Decimal(value), Value::Decimal(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Text(value), Value::Text(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Date(value), Value::Date(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Time(value), Value::Time(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Timestamp(value), Value::Timestamp(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Uuid(value), Value::Uuid(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        _ => None,
    }
}

/// Reuses compatible column buffers while retaining a strict bounded cache.
#[derive(Debug)]
pub struct ChunkPool {
    chunks: Vec<(DataChunk, Reservation)>,
    max_retained: usize,
    max_rows: usize,
}

impl ChunkPool {
    #[must_use]
    pub fn new(max_rows: usize, max_retained: usize) -> Self {
        Self {
            chunks: Vec::new(),
            max_retained,
            max_rows,
        }
    }

    pub fn materialize(
        &mut self,
        rows: &[Row],
        grant: &MemoryGrant,
    ) -> Result<(DataChunk, Reservation)> {
        if rows.len() > self.max_rows {
            return Err(DbError::new(
                "54000",
                "data chunk exceeds the configured row limit",
            ));
        }
        while let Some((mut chunk, mut reservation)) = self.chunks.pop() {
            if chunk.reset_from_rows(rows)? {
                reservation.resize(chunk.estimated_bytes())?;
                return Ok((chunk, reservation));
            }
            drop(reservation);
            drop(chunk);
        }
        let estimated = rows
            .iter()
            .map(estimated_row_bytes)
            .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
            .ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("chunk pool row estimate overflow")
            })?;
        let mut reservation = grant.try_reserve(estimated)?;
        let chunk = DataChunk::from_rows(rows)?;
        reservation.resize(chunk.estimated_bytes())?;
        Ok((chunk, reservation))
    }

    pub fn recycle(&mut self, mut chunk: DataChunk, reservation: Reservation) {
        if self.chunks.len() >= self.max_retained || chunk.physical_rows > self.max_rows {
            return;
        }
        for column in &mut chunk.columns {
            column.clear();
        }
        chunk.selection.indexes.clear();
        chunk.physical_rows = 0;
        self.chunks.push((chunk, reservation));
    }

    #[must_use]
    pub fn retained(&self) -> usize {
        self.chunks.len()
    }
}

fn infer_kinds(rows: &[Row]) -> Result<Vec<ColumnVectorKind>> {
    let width = rows.first().map_or(0, |row| row.values.len());
    if rows.iter().any(|row| row.values.len() != width) {
        return Err(DbError::internal("data chunk rows have different widths"));
    }
    (0..width)
        .map(|column_index| {
            rows.iter()
                .map(|row| &row.values[column_index])
                .find(|value| !value.is_null())
                .map_or(Ok(ColumnVectorKind::Null), value_kind)
        })
        .collect()
}

fn value_kind(value: &Value) -> Result<ColumnVectorKind> {
    Ok(match value {
        Value::Null => ColumnVectorKind::Null,
        Value::Boolean(_) => ColumnVectorKind::Boolean,
        Value::Int16(_) => ColumnVectorKind::Int16,
        Value::Int32(_) => ColumnVectorKind::Int32,
        Value::Int64(_) => ColumnVectorKind::Int64,
        Value::Float32(_) => ColumnVectorKind::Float32,
        Value::Float64(_) => ColumnVectorKind::Float64,
        Value::Decimal(_) => ColumnVectorKind::Decimal,
        Value::Text(_) => ColumnVectorKind::Text,
        Value::Binary(_) => ColumnVectorKind::Binary,
        Value::Date(_) => ColumnVectorKind::Date,
        Value::Time(_) => ColumnVectorKind::Time,
        Value::Timestamp(_) => ColumnVectorKind::Timestamp,
        Value::Json(_) => ColumnVectorKind::Json,
        Value::Jsonb(_) => ColumnVectorKind::Jsonb,
        Value::Uuid(_) => ColumnVectorKind::Uuid,
        Value::Vector(_) => ColumnVectorKind::Vector,
    })
}

fn append_rows(columns: &mut [ColumnVector], rows: &[Row]) -> Result<()> {
    for row in rows {
        for (column, value) in columns.iter_mut().zip(&row.values) {
            column.push(value)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_vectors_round_trip_nulls_and_variable_width_values() {
        let rows = vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Text("alpha".into()),
                Value::Null,
            ]),
            Row::new(vec![Value::Int64(2), Value::Null, Value::Boolean(true)]),
        ];
        let chunk = DataChunk::from_rows(&rows).expect("chunk");
        assert_eq!(chunk.len(), 2);
        assert_eq!(chunk.into_rows().expect("rows"), rows);
    }

    #[test]
    fn selection_vector_filters_and_truncates_without_moving_columns() {
        let rows = (0..6)
            .map(|value| Row::new(vec![Value::Int64(value)]))
            .collect::<Vec<_>>();
        let mut chunk = DataChunk::from_rows(&rows).expect("chunk");
        chunk
            .selection_mut()
            .retain(|physical| Ok(physical % 2 == 0))
            .expect("filter");
        chunk.selection_mut().truncate(2);
        assert_eq!(
            chunk.into_rows().expect("rows"),
            vec![
                Row::new(vec![Value::Int64(0)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
    }

    #[test]
    fn literal_comparison_filters_a_typed_column_without_materializing_values() {
        let mut chunk = DataChunk::from_rows(&[
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Null]),
            Row::new(vec![Value::Int64(3)]),
            Row::new(vec![Value::Int64(4)]),
        ])
        .expect("chunk");
        chunk
            .retain_literal_comparison(0, &Value::Int64(3), BinaryOperator::GtEq)
            .expect("typed fast path")
            .expect("filter");
        assert_eq!(chunk.selection().indexes, [2, 3]);
    }

    #[test]
    fn row_backed_comparison_defers_mismatched_physical_literal_types() {
        let snapshot = Arc::new(vec![Row::new(vec![Value::Int64(7)])]);
        let chunk = DataChunk::from_row_snapshot(snapshot, 0, 1).expect("row-backed data chunk");

        assert!(
            chunk
                .compare_literal(0, 0, &Value::Int32(7), BinaryOperator::Eq)
                .is_none()
        );
    }

    #[test]
    fn selected_row_estimate_combines_fixed_and_variable_width_columns() {
        let mut chunk = DataChunk::from_rows(&[
            Row::new(vec![Value::Int64(1), Value::Text("alpha".into())]),
            Row::new(vec![Value::Int64(2), Value::Text("beta".into())]),
        ])
        .expect("chunk");
        chunk.selection_mut().truncate(1);
        assert_eq!(
            chunk.estimated_selected_row_bytes().expect("estimate"),
            std::mem::size_of::<Row>() + std::mem::size_of::<Value>() * 2 + "alpha".len()
        );
    }

    #[test]
    fn pool_reuses_only_compatible_bounded_chunks() {
        let rows = vec![Row::new(vec![Value::Int64(1)])];
        let grant = MemoryGrant::new(1_024, 4_096).expect("grant");
        let mut pool = ChunkPool::new(2, 1);
        let (chunk, reservation) = pool.materialize(&rows, &grant).expect("first");
        pool.recycle(chunk, reservation);
        assert_eq!(pool.retained(), 1);
        assert!(grant.current_bytes() > 0);
        let (chunk, reservation) = pool.materialize(&rows, &grant).expect("reused");
        assert_eq!(chunk.into_rows().expect("rows"), rows);
        drop(reservation);
        assert_eq!(grant.current_bytes(), 0);
    }

    #[test]
    fn identity_row_snapshot_materializes_selected_rows_without_column_rebuild() {
        let snapshot = Arc::new(vec![
            Row::new(vec![Value::Int64(1), Value::Text("one".into())]),
            Row::new(vec![Value::Int64(2), Value::Text("two".into())]),
            Row::new(vec![Value::Int64(3), Value::Text("three".into())]),
        ]);
        let mut chunk = DataChunk::from_row_snapshot(Arc::clone(&snapshot), 0, snapshot.len())
            .expect("row-backed chunk");
        let columns = chunk.columns.as_ptr();
        assert!(
            chunk
                .project_columns_in_place(&[(0, ScalarType::Int64), (1, ScalarType::Text)])
                .expect("identity projection")
        );
        assert_eq!(chunk.columns.as_ptr(), columns);
        chunk.selection.indexes = vec![2, 0];

        assert_eq!(
            chunk.into_rows().expect("selected rows"),
            vec![snapshot[2].clone(), snapshot[0].clone()]
        );
    }

    #[test]
    fn projected_row_snapshot_materializes_only_the_requested_columns() {
        let snapshot = Arc::new(vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("one".into()),
        ])]);
        let mut chunk = DataChunk::from_row_snapshot(Arc::clone(&snapshot), 0, snapshot.len())
            .expect("row-backed chunk");
        assert!(
            chunk
                .project_columns_in_place(&[(1, ScalarType::Text)])
                .expect("projection")
        );

        assert_eq!(
            chunk.into_rows().expect("projected rows"),
            vec![Row::new(vec![Value::Text("one".into())])]
        );
    }

    #[test]
    fn pool_releases_incompatible_capacity_before_materializing_a_replacement() {
        let rows = (0..32)
            .map(|value| Row::new(vec![Value::Int64(value), Value::Int64(value)]))
            .collect::<Vec<_>>();
        let grant = MemoryGrant::new(256, 4_096).expect("grant");
        let mut pool = ChunkPool::new(32, 1);
        let (mut projected, mut reservation) =
            pool.materialize(&rows, &grant).expect("input chunk");
        assert!(
            projected
                .project_columns_in_place(&[(0, ScalarType::Int64)])
                .expect("projection")
        );
        reservation
            .resize(projected.estimated_bytes())
            .expect("projected reservation");
        pool.recycle(projected, reservation);

        let (replacement, reservation) = pool
            .materialize(&rows, &grant)
            .expect("replacement input chunk");
        assert_eq!(replacement.columns().len(), 2);
        drop((replacement, reservation));
        assert_eq!(grant.current_bytes(), 0);
    }

    #[test]
    fn mixed_physical_types_are_rejected() {
        let error = DataChunk::from_rows(&[
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Text("one".into())]),
        ])
        .expect_err("mixed");
        assert_eq!(error.sql_state, "42804");
    }
}
