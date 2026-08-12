use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use ordadb_types::{DbError, PgArray, PgInterval, Result, ScalarType, TypeId, Value};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const DEFAULT_ORDER: usize = 32;
pub const MAX_INDEX_DEPTH: usize = 128;
pub const MAX_INDEX_NODES: usize = 1_000_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexScalar {
    Null,
    Boolean(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(u32),
    Float64(u64),
    Decimal(Decimal),
    Text(String),
    Enum {
        type_id: TypeId,
        ordinal: u32,
        label: String,
    },
    Binary(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    Interval(PgInterval),
    Array {
        value: PgArray,
        elements: Vec<IndexScalar>,
    },
    Uuid(Uuid),
}

impl IndexScalar {
    pub fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Boolean(value) => Ok(Self::Boolean(*value)),
            Value::Int16(value) => Ok(Self::Int16(*value)),
            Value::Int32(value) => Ok(Self::Int32(*value)),
            Value::Int64(value) => Ok(Self::Int64(*value)),
            Value::Float32(value) => Ok(Self::Float32(value.to_bits())),
            Value::Float64(value) => Ok(Self::Float64(value.to_bits())),
            Value::Decimal(value) => Ok(Self::Decimal(*value)),
            Value::Text(value) => Ok(Self::Text(value.clone())),
            Value::Binary(value) => Ok(Self::Binary(value.clone())),
            Value::Date(value) => Ok(Self::Date(*value)),
            Value::Time(value) => Ok(Self::Time(*value)),
            Value::Timestamp(value) => Ok(Self::Timestamp(*value)),
            Value::Interval(value) => Ok(Self::Interval(*value)),
            Value::Uuid(value) => Ok(Self::Uuid(*value)),
            Value::Array(array) => Self::from_array(array, array.element_type()),
            Value::Json(_) | Value::Jsonb(_) | Value::Vector(_) => {
                Err(DbError::new("42804", "value type has no B+Tree ordering"))
            }
        }
    }

    pub fn from_typed_value(value: &Value, data_type: &ScalarType) -> Result<Self> {
        if value.is_null() {
            return Ok(Self::Null);
        }
        if let ScalarType::Enum { type_id, labels } = data_type {
            let Value::Text(label) = value else {
                return Err(DbError::new(
                    "42804",
                    "enum index key requires an enum text value",
                ));
            };
            let ordinal = labels
                .iter()
                .position(|candidate| candidate == label)
                .ok_or_else(|| {
                    DbError::new("22P02", format!("invalid input value for enum: {label}"))
                })?;
            return Ok(Self::Enum {
                type_id: *type_id,
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| DbError::new("54000", "enum label ordinal exceeds u32::MAX"))?,
                label: label.clone(),
            });
        }
        if let ScalarType::Array { element } = data_type {
            let Value::Array(array) = value else {
                return Err(DbError::new(
                    "42804",
                    "array index key requires an array value",
                ));
            };
            if array.element_type() != element.as_ref() {
                return Err(DbError::new(
                    "42804",
                    "array index key element type does not match its column",
                ));
            }
            return Self::from_array(array, element);
        }
        Self::from_value(value)
    }

    fn from_array(array: &PgArray, element_type: &ScalarType) -> Result<Self> {
        let elements = array
            .values()
            .iter()
            .map(|value| Self::from_typed_value(value, element_type))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::Array {
            value: array.clone(),
            elements,
        })
    }

    #[must_use]
    pub fn to_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Boolean(value) => Value::Boolean(*value),
            Self::Int16(value) => Value::Int16(*value),
            Self::Int32(value) => Value::Int32(*value),
            Self::Int64(value) => Value::Int64(*value),
            Self::Float32(value) => Value::Float32(f32::from_bits(*value)),
            Self::Float64(value) => Value::Float64(f64::from_bits(*value)),
            Self::Decimal(value) => Value::Decimal(*value),
            Self::Text(value) => Value::Text(value.clone()),
            Self::Enum { label, .. } => Value::Text(label.clone()),
            Self::Binary(value) => Value::Binary(value.clone()),
            Self::Date(value) => Value::Date(*value),
            Self::Time(value) => Value::Time(*value),
            Self::Timestamp(value) => Value::Timestamp(*value),
            Self::Interval(value) => Value::Interval(*value),
            Self::Array { value, .. } => Value::Array(value.clone()),
            Self::Uuid(value) => Value::Uuid(*value),
        }
    }

    const fn rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::Int16(_) => 2,
            Self::Int32(_) => 3,
            Self::Int64(_) => 4,
            Self::Float32(_) => 5,
            Self::Float64(_) => 6,
            Self::Decimal(_) => 7,
            Self::Text(_) => 8,
            Self::Enum { .. } => 9,
            Self::Binary(_) => 10,
            Self::Date(_) => 11,
            Self::Time(_) => 12,
            Self::Timestamp(_) => 13,
            Self::Interval(_) => 14,
            Self::Array { .. } => 15,
            Self::Uuid(_) => 16,
        }
    }
}

impl PartialEq for IndexScalar {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for IndexScalar {}

impl PartialOrd for IndexScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexScalar {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.rank() != other.rank() {
            return self.rank().cmp(&other.rank());
        }
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
            (Self::Int16(left), Self::Int16(right)) => left.cmp(right),
            (Self::Int32(left), Self::Int32(right)) => left.cmp(right),
            (Self::Int64(left), Self::Int64(right)) => left.cmp(right),
            (Self::Float32(left), Self::Float32(right)) => {
                f32::from_bits(*left).total_cmp(&f32::from_bits(*right))
            }
            (Self::Float64(left), Self::Float64(right)) => {
                f64::from_bits(*left).total_cmp(&f64::from_bits(*right))
            }
            (Self::Decimal(left), Self::Decimal(right)) => left.cmp(right),
            (Self::Text(left), Self::Text(right)) => left.cmp(right),
            (
                Self::Enum {
                    type_id: left_type,
                    ordinal: left_ordinal,
                    label: left_label,
                },
                Self::Enum {
                    type_id: right_type,
                    ordinal: right_ordinal,
                    label: right_label,
                },
            ) => left_type
                .cmp(right_type)
                .then_with(|| left_ordinal.cmp(right_ordinal))
                .then_with(|| left_label.cmp(right_label)),
            (Self::Binary(left), Self::Binary(right)) => left.cmp(right),
            (Self::Date(left), Self::Date(right)) => left.cmp(right),
            (Self::Time(left), Self::Time(right)) => left.cmp(right),
            (Self::Timestamp(left), Self::Timestamp(right)) => left.cmp(right),
            (Self::Interval(left), Self::Interval(right)) => {
                interval_comparison_key(*left).cmp(&interval_comparison_key(*right))
            }
            (
                Self::Array {
                    value: left,
                    elements: left_elements,
                },
                Self::Array {
                    value: right,
                    elements: right_elements,
                },
            ) => compare_array_elements(left_elements, right_elements).then_with(|| {
                left.dimensions()
                    .iter()
                    .map(|dimension| (dimension.length, dimension.lower_bound))
                    .cmp(
                        right
                            .dimensions()
                            .iter()
                            .map(|dimension| (dimension.length, dimension.lower_bound)),
                    )
            }),
            (Self::Uuid(left), Self::Uuid(right)) => left.cmp(right),
            _ => Ordering::Equal,
        }
    }
}

fn compare_array_elements(left: &[IndexScalar], right: &[IndexScalar]) -> Ordering {
    for (left, right) in left.iter().zip(right) {
        let ordering = match (left, right) {
            (IndexScalar::Null, IndexScalar::Null) => Ordering::Equal,
            (IndexScalar::Null, _) => Ordering::Greater,
            (_, IndexScalar::Null) => Ordering::Less,
            _ => left.cmp(right),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn interval_comparison_key(value: PgInterval) -> i128 {
    const MICROS_PER_DAY: i128 = 86_400_000_000;
    (i128::from(value.months) * 30 + i128::from(value.days)) * MICROS_PER_DAY
        + i128::from(value.microseconds)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IndexKey(Vec<IndexScalar>);

impl IndexKey {
    pub fn from_values(values: &[Value]) -> Result<Self> {
        values
            .iter()
            .map(IndexScalar::from_value)
            .collect::<Result<Vec<_>>>()
            .map(Self)
    }

    pub fn from_typed_values(values: &[Value], data_types: &[ScalarType]) -> Result<Self> {
        if values.len() != data_types.len() {
            return Err(DbError::new(
                "22023",
                "index key value count does not match its type count",
            ));
        }
        values
            .iter()
            .zip(data_types)
            .map(|(value, data_type)| IndexScalar::from_typed_value(value, data_type))
            .collect::<Result<Vec<_>>>()
            .map(Self)
    }

    #[must_use]
    pub fn values(&self) -> Vec<Value> {
        self.0.iter().map(IndexScalar::to_value).collect()
    }

    #[must_use]
    pub fn contains_null(&self) -> bool {
        self.0
            .iter()
            .any(|value| matches!(value, IndexScalar::Null))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(u64);

impl RowId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    pub key: IndexKey,
    pub row_id: RowId,
    pub included: Vec<Value>,
}

impl IndexEntry {
    pub fn new(key_values: &[Value], row_id: RowId, included: Vec<Value>) -> Result<Self> {
        let key = IndexKey::from_values(key_values)?;
        if key.is_empty() {
            return Err(DbError::new("22023", "index key cannot be empty"));
        }
        Ok(Self {
            key,
            row_id,
            included,
        })
    }

    pub fn new_typed(
        key_values: &[Value],
        key_types: &[ScalarType],
        row_id: RowId,
        included: Vec<Value>,
    ) -> Result<Self> {
        let key = IndexKey::from_typed_values(key_values, key_types)?;
        Self::from_key(key, row_id, included)
    }

    pub fn from_key(key: IndexKey, row_id: RowId, included: Vec<Value>) -> Result<Self> {
        if key.is_empty() {
            return Err(DbError::new("22023", "index key cannot be empty"));
        }
        Ok(Self {
            key,
            row_id,
            included,
        })
    }
}

#[derive(Debug, Clone)]
enum Node {
    Leaf {
        entries: Vec<IndexEntry>,
        next: Option<usize>,
    },
    Internal {
        keys: Vec<IndexKey>,
        children: Vec<usize>,
    },
}

#[derive(Debug, Clone)]
pub struct BPlusTree {
    nodes: Vec<Node>,
    root: usize,
    order: usize,
    unique: bool,
    key_width: Option<usize>,
    entry_count: usize,
}

pub struct BPlusTreeIter<'tree> {
    tree: &'tree BPlusTree,
    leaf: Option<usize>,
    offset: usize,
    upper: Bound<IndexKey>,
}

pub struct BPlusTreeOwnedIter {
    tree: Arc<BPlusTree>,
    leaf: Option<usize>,
    offset: usize,
    upper: Bound<IndexKey>,
}

impl<'tree> Iterator for BPlusTreeIter<'tree> {
    type Item = &'tree IndexEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf_id = self.leaf?;
            let Node::Leaf { entries, next } = self.tree.nodes.get(leaf_id)? else {
                self.leaf = None;
                return None;
            };
            let Some(entry) = entries.get(self.offset) else {
                self.leaf = *next;
                self.offset = 0;
                continue;
            };
            if exceeds_upper_bound(&entry.key, &self.upper) {
                self.leaf = None;
                return None;
            }
            self.offset += 1;
            return Some(entry);
        }
    }
}

impl Iterator for BPlusTreeOwnedIter {
    type Item = IndexEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf_id = self.leaf?;
            let Node::Leaf { entries, next } = self.tree.nodes.get(leaf_id)? else {
                self.leaf = None;
                return None;
            };
            let Some(entry) = entries.get(self.offset) else {
                self.leaf = *next;
                self.offset = 0;
                continue;
            };
            if exceeds_upper_bound(&entry.key, &self.upper) {
                self.leaf = None;
                return None;
            }
            self.offset += 1;
            return Some(entry.clone());
        }
    }
}
