use std::collections::{BTreeMap, BTreeSet};

use ordadb_types::{
    ColumnId, ConstraintId, DatabaseId, DbError, Identifier, IndexId, Result, RoutineId,
    ScalarType, Schema, SchemaId, SequenceId, TableId, TriggerId, TypeId, Value, ViewId,
};
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

mod system;

pub use system::*;

const MAX_DEPENDENCY_OBJECTS: usize = 16_384;
const MAX_CATALOG_OWNER_BYTES: usize = 63;

fn system_catalog_read_only() -> DbError {
    DbError::new("42501", "system catalogs are read-only")
        .with_detail("pg_catalog and information_schema cannot be modified")
}

fn ensure_writable_schema_name(name: &Identifier) -> Result<()> {
    if system::is_system_schema_name(name) {
        return Err(system_catalog_read_only());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogExpression {
    pub sql: String,
}

impl CatalogExpression {
    #[must_use]
    pub fn new(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropBehavior {
    #[default]
    Restrict,
    Cascade,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferentialAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewColumn {
    pub name: Identifier,
    pub data_type: ScalarType,
    pub declared_type: Option<TypeId>,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    #[serde(default)]
    pub default: Option<CatalogExpression>,
}

impl NewColumn {
    #[must_use]
    pub fn new(name: Identifier, data_type: ScalarType) -> Self {
        Self {
            name,
            data_type,
            declared_type: None,
            nullable: true,
            primary_key: false,
            unique: false,
            default: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub id: ColumnId,
    pub name: Identifier,
    pub data_type: ScalarType,
    #[serde(default)]
    pub declared_type: Option<TypeId>,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    #[serde(default)]
    pub default: Option<CatalogExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UserDefinedTypeKind {
    Enum {
        labels: Vec<String>,
    },
    Domain {
        base_type: ScalarType,
        #[serde(default)]
        base_declared_type: Option<TypeId>,
        not_null: bool,
        #[serde(default)]
        default: Option<CatalogExpression>,
        #[serde(default)]
        checks: Vec<DomainConstraint>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeDefinition {
    pub id: TypeId,
    pub schema_id: SchemaId,
    pub name: Identifier,
    pub definition: UserDefinedTypeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainConstraint {
    #[serde(default)]
    pub id: Option<ConstraintId>,
    pub name: Option<Identifier>,
    pub expression: CatalogExpression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainBaseType {
    pub data_type: ScalarType,
    pub declared_type: Option<TypeId>,
}

impl DomainBaseType {
    #[must_use]
    pub const fn new(data_type: ScalarType, declared_type: Option<TypeId>) -> Self {
        Self {
            data_type,
            declared_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnumValuePosition {
    Before(String),
    After(String),
}

impl TypeDefinition {
    #[must_use]
    pub fn storage_type(&self) -> &ScalarType {
        match &self.definition {
            UserDefinedTypeKind::Enum { .. } => &ScalarType::Text,
            UserDefinedTypeKind::Domain { base_type, .. } => base_type,
        }
    }

    #[must_use]
    pub fn logical_type(&self) -> ScalarType {
        match &self.definition {
            UserDefinedTypeKind::Enum { labels } => ScalarType::Enum {
                type_id: self.id,
                labels: labels.clone(),
            },
            UserDefinedTypeKind::Domain { base_type, .. } => base_type.clone(),
        }
    }
}

fn validate_enum_label(label: &str) -> Result<()> {
    if label.is_empty() {
        return Err(DbError::new("42601", "enum labels must not be empty"));
    }
    if label.len() > 63 {
        return Err(DbError::new(
            "42622",
            "enum labels must not exceed 63 bytes",
        ));
    }
    Ok(())
}

fn enum_neighbor_missing(label: &str) -> DbError {
    DbError::new("22023", format!("{label:?} is not an existing enum label"))
}

fn refresh_declared_scalar_type(data_type: &mut ScalarType, logical_type: &ScalarType) {
    let declared_as_array = matches!(data_type, ScalarType::Array { .. });
    let logical_is_array = matches!(logical_type, ScalarType::Array { .. });
    *data_type = if declared_as_array && !logical_is_array {
        ScalarType::Array {
            element: Box::new(logical_type.clone()),
        }
    } else {
        logical_type.clone()
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub id: IndexId,
    pub table_id: TableId,
    pub name: Identifier,
    pub key_columns: Vec<ColumnId>,
    pub include_columns: Vec<ColumnId>,
    pub unique: bool,
    pub primary: bool,
    #[serde(default)]
    pub method: IndexMethod,
    #[serde(default)]
    pub options: IndexOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewIndex {
    pub name: Identifier,
    pub key_columns: Vec<Identifier>,
    pub include_columns: Vec<Identifier>,
    pub unique: bool,
    pub method: IndexMethod,
    pub options: IndexOptions,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexMethod {
    #[default]
    BTree,
    FullText,
    Hnsw,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FullTextAnalyzer {
    #[default]
    Standard,
    Whitespace,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorDistanceMetric {
    #[default]
    Cosine,
    L2,
    Dot,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IndexOptions {
    #[default]
    BTree,
    FullText {
        analyzer: FullTextAnalyzer,
    },
    Hnsw {
        metric: VectorDistanceMetric,
        dimensions: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    },
}

impl IndexOptions {
    #[must_use]
    pub const fn method(&self) -> IndexMethod {
        match self {
            Self::BTree => IndexMethod::BTree,
            Self::FullText { .. } => IndexMethod::FullText,
            Self::Hnsw { .. } => IndexMethod::Hnsw,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NewConstraintKind {
    PrimaryKey {
        columns: Vec<Identifier>,
    },
    Unique {
        columns: Vec<Identifier>,
    },
    Check {
        expression: CatalogExpression,
    },
    ForeignKey {
        columns: Vec<Identifier>,
        referenced_table: TableId,
        referenced_columns: Vec<ColumnId>,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewConstraint {
    pub name: Identifier,
    pub kind: NewConstraintKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConstraintKind {
    PrimaryKey {
        columns: Vec<ColumnId>,
    },
    Unique {
        columns: Vec<ColumnId>,
    },
    Check {
        expression: CatalogExpression,
    },
    ForeignKey {
        columns: Vec<ColumnId>,
        referenced_table: TableId,
        referenced_columns: Vec<ColumnId>,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintDefinition {
    pub id: ConstraintId,
    pub table_id: TableId,
    pub name: Identifier,
    pub kind: ConstraintKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceDefinition {
    pub id: SequenceId,
    pub schema_id: SchemaId,
    pub name: Identifier,
    pub data_type: ScalarType,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub start_value: i64,
    pub last_value: i64,
    pub is_called: bool,
    pub cycle: bool,
    pub owner: Option<(TableId, ColumnId)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SequenceAlteration {
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub restart: Option<i64>,
    pub cycle: Option<bool>,
    pub owner: Option<Option<(TableId, ColumnId)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSequence {
    pub name: Identifier,
    pub data_type: ScalarType,
    pub increment: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub start_value: Option<i64>,
    pub cycle: bool,
    pub owner: Option<(TableId, ColumnId)>,
}

impl NewSequence {
    #[must_use]
    pub fn new(name: Identifier) -> Self {
        Self {
            name,
            data_type: ScalarType::Int64,
            increment: 1,
            min_value: None,
            max_value: None,
            start_value: None,
            cycle: false,
            owner: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewKind {
    Regular,
    Materialized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewDefinition {
    pub id: ViewId,
    pub schema_id: SchemaId,
    pub name: Identifier,
    pub kind: ViewKind,
    pub query: String,
    pub output: Schema,
    pub materialized_table_id: Option<TableId>,
    pub populated: bool,
    #[serde(default)]
    triggers: BTreeMap<Identifier, TriggerDefinition>,
}

impl ViewDefinition {
    pub fn triggers(&self) -> impl Iterator<Item = &TriggerDefinition> {
        self.triggers.values()
    }

    #[must_use]
    pub fn trigger(&self, name: &Identifier) -> Option<&TriggerDefinition> {
        self.triggers.get(name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewView {
    pub name: Identifier,
    pub kind: ViewKind,
    pub query: String,
    pub output: Schema,
    pub materialized_table_id: Option<TableId>,
    pub populated: bool,
    pub references: Vec<CatalogObjectRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutineKind {
    Function,
    Procedure,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutineArgumentMode {
    #[default]
    In,
    Out,
    InOut,
    Variadic,
}

impl RoutineArgumentMode {
    #[must_use]
    pub const fn accepts_input(self) -> bool {
        matches!(self, Self::In | Self::InOut | Self::Variadic)
    }

    #[must_use]
    pub const fn produces_output(self) -> bool {
        matches!(self, Self::Out | Self::InOut)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineArgument {
    pub name: Option<Identifier>,
    pub data_type: ScalarType,
    #[serde(default)]
    pub declared_type: Option<TypeId>,
    #[serde(default)]
    pub mode: RoutineArgumentMode,
}

fn routine_arguments_have_same_type(left: &RoutineArgument, right: &RoutineArgument) -> bool {
    match (left.declared_type, right.declared_type) {
        (Some(left), Some(right)) => left == right,
        (None, None) => left.data_type == right.data_type,
        _ => false,
    }
}

fn routine_input_signature_matches(left: &[RoutineArgument], right: &[RoutineArgument]) -> bool {
    let mut left = left.iter().filter(|argument| argument.mode.accepts_input());
    let mut right = right
        .iter()
        .filter(|argument| argument.mode.accepts_input());
    loop {
        match (left.next(), right.next()) {
            (Some(left), Some(right)) if routine_arguments_have_same_type(left, right) => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

fn validate_routine_arguments(
    kind: RoutineKind,
    arguments: &[RoutineArgument],
    return_type: Option<&ScalarType>,
    returns_set: bool,
) -> Result<()> {
    const MAX_ROUTINE_ARGUMENTS: usize = 100;
    if arguments.len() > MAX_ROUTINE_ARGUMENTS {
        return Err(DbError::new(
            "54000",
            format!("routine argument count exceeds the maximum of {MAX_ROUTINE_ARGUMENTS}"),
        ));
    }

    let mut names = BTreeSet::new();
    for argument in arguments {
        if let Some(name) = argument.name.as_ref()
            && !names.insert(name.clone())
        {
            return Err(DbError::new(
                "42P13",
                format!("routine parameter name {name} is used more than once"),
            ));
        }
    }

    let variadic = arguments
        .iter()
        .enumerate()
        .filter(|(_, argument)| argument.mode == RoutineArgumentMode::Variadic)
        .collect::<Vec<_>>();
    if variadic.len() > 1 {
        return Err(DbError::new(
            "42P13",
            "a routine may declare at most one VARIADIC parameter",
        ));
    }
    if let Some((index, argument)) = variadic.first().copied() {
        if !matches!(argument.data_type, ScalarType::Array { .. }) {
            return Err(DbError::new(
                "42P13",
                "VARIADIC parameter must have an array type",
            ));
        }
        if arguments[index + 1..]
            .iter()
            .any(|argument| argument.mode.accepts_input())
        {
            return Err(DbError::new(
                "42P13",
                "VARIADIC parameter must be the last input parameter",
            ));
        }
    }

    let has_output_arguments = arguments
        .iter()
        .any(|argument| argument.mode.produces_output());
    match kind {
        RoutineKind::Function if has_output_arguments && return_type.is_some() => {
            Err(DbError::new(
                "42P13",
                "function OUT parameters cannot be combined with an explicit return type",
            ))
        }
        RoutineKind::Function if has_output_arguments && returns_set => Err(DbError::new(
            "0A000",
            "set-returning functions with OUT parameters are not supported yet",
        )),
        RoutineKind::Procedure if return_type.is_some() || returns_set => Err(DbError::new(
            "42P13",
            "procedures cannot declare a function return type",
        )),
        _ => Ok(()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineDefinition {
    pub id: RoutineId,
    pub schema_id: SchemaId,
    pub name: Identifier,
    pub kind: RoutineKind,
    pub arguments: Vec<RoutineArgument>,
    pub return_type: Option<ScalarType>,
    #[serde(default)]
    pub return_declared_type: Option<TypeId>,
    pub returns_set: bool,
    pub language: String,
    pub body: String,
}

impl RoutineDefinition {
    pub fn input_arguments(&self) -> impl Iterator<Item = &RoutineArgument> {
        self.arguments
            .iter()
            .filter(|argument| argument.mode.accepts_input())
    }

    pub fn output_arguments(&self) -> impl Iterator<Item = &RoutineArgument> {
        self.arguments
            .iter()
            .filter(|argument| argument.mode.produces_output())
    }

    #[must_use]
    pub fn input_arity(&self) -> usize {
        self.input_arguments().count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRoutine {
    pub name: Identifier,
    pub kind: RoutineKind,
    pub arguments: Vec<RoutineArgument>,
    pub return_type: Option<ScalarType>,
    pub return_declared_type: Option<TypeId>,
    pub returns_set: bool,
    pub language: String,
    pub body: String,
    pub replace: bool,
    pub references: Vec<CatalogObjectRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
    BeforeStatement,
    AfterStatement,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerLevel {
    #[default]
    Row,
    Statement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum TriggerTarget {
    Table(TableId),
    View(ViewId),
}

impl TriggerTarget {
    #[must_use]
    pub const fn object_ref(self) -> CatalogObjectRef {
        match self {
            Self::Table(table_id) => CatalogObjectRef::Table(table_id),
            Self::View(view_id) => CatalogObjectRef::View(view_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "TriggerDefinitionOwned")]
pub struct TriggerDefinition {
    pub id: TriggerId,
    pub target: TriggerTarget,
    pub name: Identifier,
    pub timing: TriggerTiming,
    #[serde(default)]
    pub level: TriggerLevel,
    pub events: BTreeSet<TriggerEvent>,
    pub routine_id: RoutineId,
    pub enabled: bool,
}

#[derive(Deserialize)]
struct TriggerDefinitionOwned {
    id: TriggerId,
    #[serde(default)]
    target: Option<TriggerTarget>,
    #[serde(default)]
    table_id: Option<TableId>,
    name: Identifier,
    timing: TriggerTiming,
    #[serde(default)]
    level: TriggerLevel,
    events: BTreeSet<TriggerEvent>,
    routine_id: RoutineId,
    enabled: bool,
}

impl TryFrom<TriggerDefinitionOwned> for TriggerDefinition {
    type Error = DbError;

    fn try_from(encoded: TriggerDefinitionOwned) -> Result<Self> {
        let target = match (encoded.target, encoded.table_id) {
            (Some(target), None) => target,
            (None, Some(table_id)) => TriggerTarget::Table(table_id),
            (Some(_), Some(_)) => {
                return Err(DbError::new(
                    "XX001",
                    "trigger catalog entry contains conflicting targets",
                ));
            }
            (None, None) => {
                return Err(DbError::new(
                    "XX001",
                    "trigger catalog entry is missing its target",
                ));
            }
        };
        Ok(Self {
            id: encoded.id,
            target,
            name: encoded.name,
            timing: encoded.timing,
            level: encoded.level,
            events: encoded.events,
            routine_id: encoded.routine_id,
            enabled: encoded.enabled,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum CatalogObjectRef {
    Schema(SchemaId),
    Table(TableId),
    Column(TableId, ColumnId),
    Index(IndexId),
    Constraint(ConstraintId),
    Sequence(SequenceId),
    View(ViewId),
    Routine(RoutineId),
    Trigger(TriggerId),
    Type(TypeId),
}

/// PostgreSQL reserves OID zero as invalid and OIDs below this boundary for
/// built-in objects. OrdaDB user catalog objects are allocated monotonically
/// from PostgreSQL's `FirstNormalObjectId`.
pub const POSTGRES_OID_FIRST_USER: u32 = 16_384;
pub const POSTGRES_OID_LAST_BUILTIN: u32 = POSTGRES_OID_FIRST_USER - 1;

const MAX_POSTGRES_OID_MAPPINGS: usize = 1_048_576;
const POSTGRES_OID_EXHAUSTED: u64 = u32::MAX as u64 + 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PostgresOid(u32);

impl PostgresOid {
    pub fn new(value: u32) -> Result<Self> {
        if value == 0 {
            return Err(DbError::new("22023", "PostgreSQL OID zero is invalid"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    #[must_use]
    pub const fn is_builtin(self) -> bool {
        self.0 <= POSTGRES_OID_LAST_BUILTIN
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum PostgresOidObject {
    Database(DatabaseId),
    Schema(SchemaId),
    Table(TableId),
    View(ViewId),
    Column(TableId, ColumnId),
    Index(IndexId),
    Constraint(ConstraintId),
    Sequence(SequenceId),
    Routine(RoutineId),
    Trigger(TriggerId),
    Type(TypeId),
}

impl From<CatalogObjectRef> for PostgresOidObject {
    fn from(object: CatalogObjectRef) -> Self {
        match object {
            CatalogObjectRef::Schema(id) => Self::Schema(id),
            CatalogObjectRef::Table(id) => Self::Table(id),
            CatalogObjectRef::Column(table_id, column_id) => Self::Column(table_id, column_id),
            CatalogObjectRef::Index(id) => Self::Index(id),
            CatalogObjectRef::Constraint(id) => Self::Constraint(id),
            CatalogObjectRef::Sequence(id) => Self::Sequence(id),
            CatalogObjectRef::View(id) => Self::View(id),
            CatalogObjectRef::Routine(id) => Self::Routine(id),
            CatalogObjectRef::Trigger(id) => Self::Trigger(id),
            CatalogObjectRef::Type(id) => Self::Type(id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct PostgresOidMapping {
    object: PostgresOidObject,
    oid: PostgresOid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresOidRegistry {
    first_user_oid: u32,
    next_oid: u64,
    mappings: BTreeMap<PostgresOidObject, PostgresOid>,
}

#[derive(Serialize)]
struct PostgresOidRegistryRef {
    first_user_oid: u32,
    next_oid: u64,
    mappings: Vec<PostgresOidMapping>,
}

#[derive(Deserialize)]
struct PostgresOidRegistryOwned {
    first_user_oid: u32,
    next_oid: u64,
    #[serde(default, deserialize_with = "deserialize_postgres_oid_mappings")]
    mappings: Vec<PostgresOidMapping>,
}

fn deserialize_postgres_oid_mappings<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<PostgresOidMapping>, D::Error>
where
    D: Deserializer<'de>,
{
    struct MappingVisitor;

    impl<'de> Visitor<'de> for MappingVisitor {
        type Value = Vec<PostgresOidMapping>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded PostgreSQL OID mapping array")
        }

        fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let capacity = sequence
                .size_hint()
                .unwrap_or_default()
                .min(MAX_POSTGRES_OID_MAPPINGS);
            let mut mappings = Vec::with_capacity(capacity);
            while let Some(mapping) = sequence.next_element::<PostgresOidMapping>()? {
                if mappings.len() >= MAX_POSTGRES_OID_MAPPINGS {
                    return Err(de::Error::custom(
                        "XX001: PostgreSQL OID registry exceeds its mapping limit",
                    ));
                }
                mappings.push(mapping);
            }
            Ok(mappings)
        }
    }

    deserializer.deserialize_seq(MappingVisitor)
}

impl Serialize for PostgresOidRegistry {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        PostgresOidRegistryRef {
            first_user_oid: self.first_user_oid,
            next_oid: self.next_oid,
            mappings: self
                .mappings
                .iter()
                .map(|(object, oid)| PostgresOidMapping {
                    object: *object,
                    oid: *oid,
                })
                .collect(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PostgresOidRegistry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = PostgresOidRegistryOwned::deserialize(deserializer)?;
        if encoded.mappings.len() > MAX_POSTGRES_OID_MAPPINGS {
            return Err(de::Error::custom(
                "XX001: PostgreSQL OID registry exceeds its mapping limit",
            ));
        }
        let mut mappings = BTreeMap::new();
        let mut oids = BTreeSet::new();
        for mapping in encoded.mappings {
            if mappings.insert(mapping.object, mapping.oid).is_some() {
                return Err(de::Error::custom(
                    "XX001: PostgreSQL OID registry contains a duplicate object",
                ));
            }
            if !oids.insert(mapping.oid) {
                return Err(de::Error::custom(
                    "XX001: PostgreSQL OID registry contains a duplicate OID",
                ));
            }
        }
        let registry = Self {
            first_user_oid: encoded.first_user_oid,
            next_oid: encoded.next_oid,
            mappings,
        };
        registry.validate_metadata().map_err(de::Error::custom)?;
        Ok(registry)
    }
}
