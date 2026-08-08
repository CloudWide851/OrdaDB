use std::fmt;
use std::str::FromStr;

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
object_id!(IndexId);
object_id!(ConstraintId);
object_id!(SequenceId);
object_id!(ViewId);
object_id!(RoutineId);
object_id!(TriggerId);
object_id!(TypeId);

pub const MAX_ARRAY_DIMENSIONS: usize = 6;
pub const MAX_ARRAY_ELEMENTS: usize = 1_000_000;

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

pub const MAX_POSTGRES_NAME_BYTES: usize = 63;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ScalarType {
    Boolean,
    Int16,
    Int32,
    Int64,
    Oid,
    Name,
    InternalChar,
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
    Enum {
        type_id: TypeId,
        labels: Vec<String>,
    },
    Binary,
    Date,
    Time,
    Timestamp {
        with_timezone: bool,
    },
    Interval,
    Array {
        element: Box<ScalarType>,
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
            Value::Int64(value) => {
                matches!(self, Self::Float64 | Self::Decimal { .. })
                    || matches!(self, Self::Oid) && u32::try_from(*value).is_ok()
            }
            Value::Text(value) => match self {
                Self::Char { .. } | Self::Varchar { .. } => true,
                Self::Name => value.len() <= MAX_POSTGRES_NAME_BYTES,
                Self::InternalChar => value.len() == 1,
                Self::Enum { labels, .. } => labels.iter().any(|label| label == value),
                _ => false,
            },
            Value::Array(value) => {
                matches!(self, Self::Array { element }
                    if value.values().iter().all(|value| element.accepts(value)))
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PgInterval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

impl PgInterval {
    #[must_use]
    pub const fn new(months: i32, days: i32, microseconds: i64) -> Self {
        Self {
            months,
            days,
            microseconds,
        }
    }
}

impl fmt::Display for PgInterval {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let negative = self.microseconds.is_negative();
        let micros = i128::from(self.microseconds).abs();
        let hours = micros / 3_600_000_000;
        let minutes = micros / 60_000_000 % 60;
        let seconds = micros / 1_000_000 % 60;
        let fraction = micros % 1_000_000;
        write!(
            formatter,
            "{} mons {} days {}{hours:02}:{minutes:02}:{seconds:02}.{fraction:06}",
            self.months,
            self.days,
            if negative { "-" } else { "" }
        )
    }
}

impl FromStr for PgInterval {
    type Err = DbError;

    fn from_str(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.is_empty() {
            return Err(DbError::new("22007", "interval input is empty"));
        }
        let tokens = input.split_whitespace().collect::<Vec<_>>();
        let mut months = 0_i64;
        let mut days = 0_i64;
        let mut microseconds = 0_i128;
        let mut index = 0;
        while index < tokens.len() {
            let token = tokens[index];
            if token.contains(':') {
                microseconds = microseconds
                    .checked_add(parse_interval_clock(token)?)
                    .ok_or_else(|| DbError::new("22015", "interval time field overflows"))?;
                index += 1;
                continue;
            }
            let unit = tokens.get(index + 1).ok_or_else(|| {
                DbError::new(
                    "22007",
                    format!("interval value `{token}` is missing its unit"),
                )
            })?;
            match unit.to_ascii_lowercase().as_str() {
                "year" | "years" => {
                    let value = parse_interval_integer(token)?;
                    months = months
                        .checked_add(value.checked_mul(12).ok_or_else(|| {
                            DbError::new("22015", "interval year field overflows")
                        })?)
                        .ok_or_else(|| DbError::new("22015", "interval month field overflows"))?;
                }
                "mon" | "mons" | "month" | "months" => {
                    months = months
                        .checked_add(parse_interval_integer(token)?)
                        .ok_or_else(|| DbError::new("22015", "interval month field overflows"))?;
                }
                "day" | "days" => {
                    days = days
                        .checked_add(parse_interval_integer(token)?)
                        .ok_or_else(|| DbError::new("22015", "interval day field overflows"))?;
                }
                "hour" | "hours" => {
                    microseconds = add_interval_unit(
                        microseconds,
                        parse_interval_integer(token)?,
                        3_600_000_000,
                    )?;
                }
                "minute" | "minutes" | "min" | "mins" => {
                    microseconds = add_interval_unit(
                        microseconds,
                        parse_interval_integer(token)?,
                        60_000_000,
                    )?;
                }
                "second" | "seconds" | "sec" | "secs" => {
                    microseconds = microseconds
                        .checked_add(parse_interval_seconds(token)?)
                        .ok_or_else(|| DbError::new("22015", "interval second field overflows"))?;
                }
                "millisecond" | "milliseconds" | "msec" | "msecs" => {
                    microseconds =
                        add_interval_unit(microseconds, parse_interval_integer(token)?, 1_000)?;
                }
                "microsecond" | "microseconds" | "usec" | "usecs" => {
                    microseconds =
                        add_interval_unit(microseconds, parse_interval_integer(token)?, 1)?;
                }
                _ => {
                    return Err(DbError::new(
                        "22007",
                        format!("interval unit `{unit}` is not recognized"),
                    ));
                }
            }
            index += 2;
        }
        Ok(Self {
            months: i32::try_from(months)
                .map_err(|_| DbError::new("22015", "interval month field is out of range"))?,
            days: i32::try_from(days)
                .map_err(|_| DbError::new("22015", "interval day field is out of range"))?,
            microseconds: i64::try_from(microseconds)
                .map_err(|_| DbError::new("22015", "interval time field is out of range"))?,
        })
    }
}

fn parse_interval_integer(value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .map_err(|_| DbError::new("22007", format!("invalid interval field `{value}`")))
}

fn add_interval_unit(current: i128, value: i64, multiplier: i128) -> Result<i128> {
    current
        .checked_add(
            i128::from(value)
                .checked_mul(multiplier)
                .ok_or_else(|| DbError::new("22015", "interval time field overflows"))?,
        )
        .ok_or_else(|| DbError::new("22015", "interval time field overflows"))
}

fn parse_interval_clock(value: &str) -> Result<i128> {
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix(['-', '+']).unwrap_or(value);
    let fields = unsigned.split(':').collect::<Vec<_>>();
    if fields.len() != 3 {
        return Err(DbError::new(
            "22007",
            format!("invalid interval time `{value}`"),
        ));
    }
    let hours = fields[0]
        .parse::<i64>()
        .map_err(|_| DbError::new("22007", format!("invalid interval hour `{}`", fields[0])))?;
    let minutes = fields[1]
        .parse::<u8>()
        .map_err(|_| DbError::new("22007", format!("invalid interval minute `{}`", fields[1])))?;
    if minutes >= 60 {
        return Err(DbError::new("22007", "interval minute must be below 60"));
    }
    let seconds = parse_interval_seconds(fields[2])?;
    if seconds.abs() >= 60_000_000 {
        return Err(DbError::new("22007", "interval second must be below 60"));
    }
    let total = i128::from(hours)
        .checked_mul(3_600_000_000)
        .and_then(|value| value.checked_add(i128::from(minutes) * 60_000_000))
        .and_then(|value| value.checked_add(seconds))
        .ok_or_else(|| DbError::new("22015", "interval time field overflows"))?;
    Ok(if negative { -total } else { total })
}

fn parse_interval_seconds(value: &str) -> Result<i128> {
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix(['-', '+']).unwrap_or(value);
    let mut parts = unsigned.split('.');
    let whole = parts
        .next()
        .ok_or_else(|| DbError::new("22007", "interval second is empty"))?
        .parse::<i64>()
        .map_err(|_| DbError::new("22007", format!("invalid interval second `{value}`")))?;
    let fraction = parts.next().unwrap_or("");
    if parts.next().is_some()
        || fraction.len() > 6
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(DbError::new(
            "22007",
            format!("invalid interval second `{value}`"),
        ));
    }
    let mut micros = if fraction.is_empty() {
        0_i128
    } else {
        fraction
            .parse::<i128>()
            .map_err(|_| DbError::new("22007", format!("invalid interval second `{value}`")))?
            * 10_i128.pow(u32::try_from(6 - fraction.len()).expect("bounded fraction"))
    };
    micros += i128::from(whole) * 1_000_000;
    Ok(if negative { -micros } else { micros })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArrayDimension {
    pub length: u32,
    pub lower_bound: i32,
}

impl ArrayDimension {
    #[must_use]
    pub const fn new(length: u32, lower_bound: i32) -> Self {
        Self {
            length,
            lower_bound,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PgArray {
    element_type: ScalarType,
    dimensions: Vec<ArrayDimension>,
    values: Vec<Value>,
}

impl PgArray {
    pub fn new(
        element_type: ScalarType,
        dimensions: Vec<ArrayDimension>,
        values: Vec<Value>,
    ) -> Result<Self> {
        if matches!(element_type, ScalarType::Array { .. }) {
            return Err(DbError::new(
                "22023",
                "PostgreSQL arrays use dimensions instead of nested array element types",
            ));
        }
        if dimensions.len() > MAX_ARRAY_DIMENSIONS {
            return Err(DbError::new(
                "54000",
                format!(
                    "array has {} dimensions; maximum is {MAX_ARRAY_DIMENSIONS}",
                    dimensions.len()
                ),
            ));
        }
        let expected = if dimensions.is_empty() {
            0
        } else {
            dimensions.iter().try_fold(1_usize, |count, dimension| {
                count
                    .checked_mul(usize::try_from(dimension.length).map_err(|_| {
                        DbError::new("54000", "array dimension length is not addressable")
                    })?)
                    .ok_or_else(|| DbError::new("54000", "array element count overflows"))
            })?
        };
        if expected > MAX_ARRAY_ELEMENTS {
            return Err(DbError::new(
                "54000",
                format!("array contains {expected} elements; maximum is {MAX_ARRAY_ELEMENTS}"),
            ));
        }
        if expected != values.len() {
            return Err(DbError::new(
                "2202E",
                format!(
                    "array dimensions declare {expected} elements but {} values were supplied",
                    values.len()
                ),
            ));
        }
        if values.iter().any(|value| matches!(value, Value::Array(_))) {
            return Err(DbError::new(
                "2202E",
                "array values must be flattened across their declared dimensions",
            ));
        }
        if let Some(value) = values.iter().find(|value| !element_type.accepts(value)) {
            return Err(DbError::new(
                "42804",
                format!("array value {value:?} is not assignable to {element_type:?}"),
            ));
        }
        Ok(Self {
            element_type,
            dimensions,
            values,
        })
    }

    pub fn one_dimensional(element_type: ScalarType, values: Vec<Value>) -> Result<Self> {
        let length = u32::try_from(values.len())
            .map_err(|_| DbError::new("54000", "array contains more than u32::MAX elements"))?;
        Self::new(
            element_type,
            if values.is_empty() {
                Vec::new()
            } else {
                vec![ArrayDimension::new(length, 1)]
            },
            values,
        )
    }

    #[must_use]
    pub const fn element_type(&self) -> &ScalarType {
        &self.element_type
    }

    #[must_use]
    pub fn dimensions(&self) -> &[ArrayDimension] {
        &self.dimensions
    }

    #[must_use]
    pub fn values(&self) -> &[Value] {
        &self.values
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
    Interval(PgInterval),
    Array(PgArray),
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
            Self::Interval(_) => Some(ScalarType::Interval),
            Self::Array(value) => Some(ScalarType::Array {
                element: Box::new(value.element_type().clone()),
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
pub struct DbObjectIdentity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<Box<str>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_name: Option<Box<str>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_name: Option<Box<str>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_type_name: Option<Box<str>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraint_name: Option<Box<str>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbNotice {
    pub sql_state: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<Box<str>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<Box<str>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_identity: Option<Box<DbObjectIdentity>>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_identity: Option<Box<DbObjectIdentity>>,
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
            object_identity: None,
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
    pub fn with_schema_name(mut self, name: impl Into<String>) -> Self {
        self.object_identity_mut().schema_name = Some(name.into().into_boxed_str());
        self
    }

    #[must_use]
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.object_identity_mut().table_name = Some(name.into().into_boxed_str());
        self
    }

    #[must_use]
    pub fn with_column_name(mut self, name: impl Into<String>) -> Self {
        self.object_identity_mut().column_name = Some(name.into().into_boxed_str());
        self
    }

    #[must_use]
    pub fn with_data_type_name(mut self, name: impl Into<String>) -> Self {
        self.object_identity_mut().data_type_name = Some(name.into().into_boxed_str());
        self
    }

    #[must_use]
    pub fn with_constraint_name(mut self, name: impl Into<String>) -> Self {
        self.object_identity_mut().constraint_name = Some(name.into().into_boxed_str());
        self
    }

    fn object_identity_mut(&mut self) -> &mut DbObjectIdentity {
        self.object_identity
            .get_or_insert_with(|| {
                Box::new(DbObjectIdentity {
                    schema_name: None,
                    table_name: None,
                    column_name: None,
                    data_type_name: None,
                    constraint_name: None,
                })
            })
            .as_mut()
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

    use super::{
        ArrayDimension, DbError, Identifier, MAX_POSTGRES_NAME_BYTES, PgArray, PgInterval,
        ScalarType, Value,
    };

    #[test]
    fn database_errors_keep_string_sqlstates_and_optional_object_identity() {
        let error = DbError::new("23505", "duplicate key")
            .with_schema_name("public")
            .with_table_name("items")
            .with_constraint_name("items_pkey");
        assert_eq!(error.sql_state, "23505");
        let encoded = serde_json::to_string(&error).expect("serialize error");
        assert!(encoded.contains("\"sql_state\":\"23505\""));
        let decoded: DbError = serde_json::from_str(&encoded).expect("deserialize error");
        assert_eq!(decoded, error);

        let legacy = serde_json::json!({
            "sql_state": "42P01",
            "message": "missing table",
            "detail": null,
            "hint": null,
            "position": null,
            "query_id": "legacy-query"
        });
        let decoded: DbError = serde_json::from_value(legacy).expect("legacy error");
        assert_eq!(decoded.sql_state, "42P01");
        assert!(decoded.object_identity.is_none());
    }

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
    fn postgres_catalog_scalar_types_enforce_physical_bounds() {
        assert!(ScalarType::Oid.accepts(&Value::Int64(0)));
        assert!(ScalarType::Oid.accepts(&Value::Int64(i64::from(u32::MAX))));
        assert!(!ScalarType::Oid.accepts(&Value::Int64(-1)));
        assert!(!ScalarType::Oid.accepts(&Value::Int64(i64::from(u32::MAX) + 1)));

        assert!(ScalarType::Name.accepts(&Value::Text("n".repeat(MAX_POSTGRES_NAME_BYTES))));
        assert!(!ScalarType::Name.accepts(&Value::Text("n".repeat(MAX_POSTGRES_NAME_BYTES + 1))));
        assert!(ScalarType::InternalChar.accepts(&Value::Text("r".into())));
        assert!(!ScalarType::InternalChar.accepts(&Value::Text("".into())));
        assert!(!ScalarType::InternalChar.accepts(&Value::Text("é".into())));
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
            Value::Interval(PgInterval::new(14, 3, 4_500_000)),
            Value::Array(
                PgArray::new(
                    ScalarType::Int32,
                    vec![ArrayDimension::new(2, 1)],
                    vec![Value::Int32(1), Value::Null],
                )
                .expect("array"),
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

    #[test]
    fn arrays_validate_dimensions_types_and_bounds() {
        let array = PgArray::one_dimensional(
            ScalarType::Int64,
            vec![Value::Int32(1), Value::Int64(2), Value::Null],
        )
        .expect("array");
        assert_eq!(array.dimensions(), &[ArrayDimension::new(3, 1)]);
        assert_eq!(
            Value::Array(array).scalar_type(),
            Some(ScalarType::Array {
                element: Box::new(ScalarType::Int64),
            })
        );

        let mismatch = PgArray::new(
            ScalarType::Int32,
            vec![ArrayDimension::new(2, 1)],
            vec![Value::Int32(1)],
        )
        .expect_err("dimension mismatch");
        assert_eq!(mismatch.sql_state, "2202E");
    }

    #[test]
    fn intervals_parse_and_render_postgresql_fields() {
        let interval = "1 year 2 mons 3 days 04:05:06.75"
            .parse::<PgInterval>()
            .expect("interval");
        assert_eq!(interval, PgInterval::new(14, 3, 14_706_750_000));
        assert_eq!(interval.to_string(), "14 mons 3 days 04:05:06.750000");
        assert_eq!(
            "-01:02:03.000004".parse::<PgInterval>().expect("negative"),
            PgInterval::new(0, 0, -3_723_000_004)
        );
        assert_eq!(
            "1 day 2"
                .parse::<PgInterval>()
                .expect_err("missing unit")
                .sql_state,
            "22007"
        );
    }
}
