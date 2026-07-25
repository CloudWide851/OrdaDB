use std::fmt;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;
use uuid::Uuid;

macro_rules! object_id {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name(u64);

        impl $name {
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }
    };
}

object_id!(DatabaseId);
object_id!(SchemaId);
object_id!(TableId);
object_id!(ColumnId);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Identifier {
    value: String,
    quoted: bool,
}

impl Identifier {
    #[must_use]
    pub fn new(value: impl Into<String>, quoted: bool) -> Self {
        let value = value.into();
        Self {
            value: if quoted { value } else { value.to_lowercase() },
            quoted,
        }
    }

    #[must_use]
    pub fn unquoted(value: impl Into<String>) -> Self {
        Self::new(value, false)
    }

    #[must_use]
    pub fn quoted(value: impl Into<String>) -> Self {
        Self::new(value, true)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub const fn is_quoted(&self) -> bool {
        self.quoted
    }
}

impl Serialize for Identifier {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let kind = if self.quoted { 'q' } else { 'u' };
        serializer.serialize_str(&format!("{kind}:{}", self.value))
    }
}

impl<'de> Deserialize<'de> for Identifier {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        if let Some(value) = encoded.strip_prefix("u:") {
            Ok(Self::unquoted(value))
        } else if let Some(value) = encoded.strip_prefix("q:") {
            Ok(Self::quoted(value))
        } else {
            Err(de::Error::custom(
                "identifier must start with the v1 `u:` or `q:` marker",
            ))
        }
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.quoted {
            write!(formatter, "\"{}\"", self.value.replace('"', "\"\""))
        } else {
            formatter.write_str(&self.value)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ScalarType {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal {
        precision: Option<u8>,
        scale: Option<u8>,
    },
    Char {
        length: Option<u32>,
    },
    Varchar {
        length: Option<u32>,
    },
    Text,
    Binary,
    Date,
    Time,
    Timestamp {
        with_timezone: bool,
    },
    Json,
    Jsonb,
    Uuid,
    Vector {
        dimensions: Option<usize>,
    },
}

impl ScalarType {
    #[must_use]
    pub fn accepts(&self, value: &Value) -> bool {
        match value {
            Value::Null => true,
            _ if value.scalar_type().as_ref() == Some(self) => true,
            Value::Int16(_) => matches!(
                self,
                Self::Int32 | Self::Int64 | Self::Float32 | Self::Float64 | Self::Decimal { .. }
            ),
            Value::Int32(_) => {
                matches!(self, Self::Int64 | Self::Float64 | Self::Decimal { .. })
            }
            Value::Int64(_) => matches!(self, Self::Float64 | Self::Decimal { .. }),
            Value::Text(_) => matches!(self, Self::Char { .. } | Self::Varchar { .. }),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Value {
    Null,
    Boolean(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Decimal(Decimal),
    Text(String),
    Binary(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    Json(serde_json::Value),
    Jsonb(serde_json::Value),
    Uuid(Uuid),
    Vector(Vec<f32>),
}

impl Value {
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[must_use]
    pub fn scalar_type(&self) -> Option<ScalarType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(ScalarType::Boolean),
            Self::Int16(_) => Some(ScalarType::Int16),
            Self::Int32(_) => Some(ScalarType::Int32),
            Self::Int64(_) => Some(ScalarType::Int64),
            Self::Float32(_) => Some(ScalarType::Float32),
            Self::Float64(_) => Some(ScalarType::Float64),
            Self::Decimal(_) => Some(ScalarType::Decimal {
                precision: None,
                scale: None,
            }),
            Self::Text(_) => Some(ScalarType::Text),
            Self::Binary(_) => Some(ScalarType::Binary),
            Self::Date(_) => Some(ScalarType::Date),
            Self::Time(_) => Some(ScalarType::Time),
            Self::Timestamp(_) => Some(ScalarType::Timestamp {
                with_timezone: false,
            }),
            Self::Json(_) => Some(ScalarType::Json),
            Self::Jsonb(_) => Some(ScalarType::Jsonb),
            Self::Uuid(_) => Some(ScalarType::Uuid),
            Self::Vector(values) => Some(ScalarType::Vector {
                dimensions: Some(values.len()),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub data_type: ScalarType,
    pub nullable: bool,
}

impl Field {
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: ScalarType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    #[must_use]
    pub const fn empty() -> Self {
        Self { fields: Vec::new() }
    }

    #[must_use]
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    #[must_use]
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Batch {
    pub schema: Schema,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryProgress {
    pub rows_processed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbNotice {
    pub sql_state: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandComplete {
    pub tag: String,
    pub rows_affected: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "camelCase")]
pub enum QueryEvent {
    Schema(Schema),
    Batch(Batch),
    Progress(QueryProgress),
    Notice(DbNotice),
    Complete(CommandComplete),
}

#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
#[error("{sql_state}: {message}")]
pub struct DbError {
    pub sql_state: String,
    pub message: String,
    pub detail: Option<Box<str>>,
    pub hint: Option<Box<str>>,
    pub position: Option<usize>,
    pub query_id: Box<str>,
}

impl DbError {
    #[must_use]
    pub fn new(sql_state: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            sql_state: sql_state.into(),
            message: message.into(),
            detail: None,
            hint: None,
            position: None,
            query_id: Uuid::new_v4().to_string().into_boxed_str(),
        }
    }

    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into().into_boxed_str());
        self
    }

    #[must_use]
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into().into_boxed_str());
        self
    }

    #[must_use]
    pub const fn with_position(mut self, position: usize) -> Self {
        self.position = Some(position);
        self
    }

    #[must_use]
    pub fn unsupported(feature: impl Into<String>) -> Self {
        let feature = feature.into();
        Self::new("0A000", format!("{feature} is not supported"))
            .with_hint("Use the SQL subset documented for this OrdaDB milestone.")
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new("XX000", message)
    }
}

pub type Result<T> = std::result::Result<T, DbError>;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    use super::{Identifier, ScalarType, Value};

    #[test]
    fn normalizes_unquoted_identifiers_but_preserves_quoted_names() {
        assert_eq!(Identifier::unquoted("MixedCase").as_str(), "mixedcase");
        assert_eq!(Identifier::quoted("MixedCase").as_str(), "MixedCase");
    }

    #[test]
    fn identifier_json_is_reversible_and_valid_as_a_map_key() {
        let identifiers = BTreeMap::from([
            (Identifier::unquoted("MixedCase"), 1),
            (Identifier::quoted("MixedCase"), 2),
        ]);
        let encoded = serde_json::to_string(&identifiers).expect("serialize identifiers");
        assert!(encoded.contains("\"u:mixedcase\""));
        assert!(encoded.contains("\"q:MixedCase\""));
        let decoded: BTreeMap<Identifier, i32> =
            serde_json::from_str(&encoded).expect("deserialize identifiers");
        assert_eq!(decoded, identifiers);
    }

    #[test]
    fn supports_null_and_safe_assignment_widening() {
        assert!(ScalarType::Uuid.accepts(&Value::Null));
        assert!(ScalarType::Int64.accepts(&Value::Int32(42)));
        assert!(!ScalarType::Int16.accepts(&Value::Int64(42)));
    }

    #[test]
    fn round_trips_typed_values_through_json() {
        let values = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Int16(-16),
            Value::Int32(32),
            Value::Int64(64),
            Value::Float32(3.25),
            Value::Float64(6.5),
            Value::Decimal(Decimal::new(12345, 2)),
            Value::Text("OrdaDB".into()),
            Value::Binary(vec![0, 1, 2, 255]),
            Value::Date(NaiveDate::from_ymd_opt(2026, 7, 25).expect("date")),
            Value::Time(NaiveTime::from_hms_opt(12, 34, 56).expect("time")),
            Value::Timestamp(
                NaiveDateTime::parse_from_str("2026-07-25 12:34:56", "%Y-%m-%d %H:%M:%S")
                    .expect("timestamp"),
            ),
            Value::Json(serde_json::json!({"kind": "json"})),
            Value::Jsonb(serde_json::json!({"kind": "jsonb"})),
            Value::Uuid(Uuid::parse_str("8b0e3f94-3bbf-4f7c-a931-48cd9ba86c1d").expect("uuid")),
            Value::Vector(vec![0.25, 0.5, 0.75]),
        ];

        let encoded = serde_json::to_string(&values).expect("serialize values");
        let decoded: Vec<Value> = serde_json::from_str(&encoded).expect("deserialize values");
        assert_eq!(decoded, values);
    }
}
