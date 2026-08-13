
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
            Self::Interval(values) => values.len(),
            Self::Array(values) => values.len(),
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
            Self::Interval(values) => values
                .get(index)
                .ok_or_else(missing)?
                .map_or(Value::Null, Value::Interval),
            Self::Array(values) => values
                .get(index)
                .ok_or_else(missing)?
                .as_ref()
                .map_or(Value::Null, |value| Value::Array(value.clone())),
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
            Self::Interval(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Interval),
            Self::Array(values) => values
                .get_mut(index)
                .ok_or_else(missing)?
                .take()
                .map_or(Value::Null, Value::Array),
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
            (Self::Interval(values), Value::Interval(literal)) => compare!(values, literal),
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
            (Self::Interval(values), Value::Interval(literal)) => retain!(values, literal),
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
                Self::Interval(values) => {
                    values.capacity() * std::mem::size_of::<Option<PgInterval>>()
                }
                Self::Array(values) => {
                    values.capacity() * std::mem::size_of::<Option<PgArray>>()
                        + values
                            .iter()
                            .flatten()
                            .map(estimated_array_bytes)
                            .sum::<usize>()
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
            Self::Array(values) => {
                base + values
                    .get(index)
                    .ok_or_else(|| DbError::internal("column vector index is out of bounds"))?
                    .as_ref()
                    .map_or(0, estimated_array_bytes)
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
            | Self::Array(_)
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
            ColumnVectorKind::Interval => Self::Interval(Vec::with_capacity(capacity)),
            ColumnVectorKind::Array => Self::Array(Vec::with_capacity(capacity)),
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
            Self::Interval(_) => ColumnVectorKind::Interval,
            Self::Array(_) => ColumnVectorKind::Array,
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
                    ScalarType::Text
                        | ScalarType::Char { .. }
                        | ScalarType::Varchar { .. }
                        | ScalarType::Enum { .. }
                )
                | (ColumnVectorKind::Binary, ScalarType::Binary)
                | (ColumnVectorKind::Date, ScalarType::Date)
                | (ColumnVectorKind::Time, ScalarType::Time)
                | (ColumnVectorKind::Timestamp, ScalarType::Timestamp { .. })
                | (ColumnVectorKind::Interval, ScalarType::Interval)
                | (ColumnVectorKind::Array, ScalarType::Array { .. })
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
            Self::Interval(values) => values.clear(),
            Self::Array(values) => values.clear(),
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
            (Self::Interval(values), Value::Interval(value)) => values.push(Some(*value)),
            (Self::Array(values), Value::Array(value)) => values.push(Some(value.clone())),
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
            (Self::Interval(values), Value::Null) => values.push(None),
            (Self::Array(values), Value::Null) => values.push(None),
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
