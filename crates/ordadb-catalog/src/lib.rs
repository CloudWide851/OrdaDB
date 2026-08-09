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

impl PostgresOidRegistry {
    fn bootstrap(database_id: DatabaseId, public_schema_id: SchemaId) -> Self {
        Self {
            first_user_oid: POSTGRES_OID_FIRST_USER,
            next_oid: u64::from(POSTGRES_OID_FIRST_USER) + 2,
            mappings: BTreeMap::from([
                (
                    PostgresOidObject::Database(database_id),
                    PostgresOid(POSTGRES_OID_FIRST_USER),
                ),
                (
                    PostgresOidObject::Schema(public_schema_id),
                    PostgresOid(POSTGRES_OID_FIRST_USER + 1),
                ),
            ]),
        }
    }

    fn reconstruct(objects: &BTreeSet<PostgresOidObject>) -> Result<Self> {
        let mut registry = Self {
            first_user_oid: POSTGRES_OID_FIRST_USER,
            next_oid: u64::from(POSTGRES_OID_FIRST_USER),
            mappings: BTreeMap::new(),
        };
        for object in objects {
            registry.allocate(*object)?;
        }
        Ok(registry)
    }

    fn allocate(&mut self, object: PostgresOidObject) -> Result<PostgresOid> {
        if self.mappings.contains_key(&object) {
            return Err(DbError::new(
                "XX001",
                "PostgreSQL OID registry already contains the catalog object",
            ));
        }
        if self.mappings.len() >= MAX_POSTGRES_OID_MAPPINGS {
            return Err(DbError::new(
                "54000",
                "PostgreSQL OID registry exceeds its mapping limit",
            ));
        }
        let value = u32::try_from(self.next_oid)
            .map_err(|_| DbError::new("54000", "PostgreSQL OID allocation space is exhausted"))?;
        let oid = PostgresOid(value);
        self.next_oid = self
            .next_oid
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "PostgreSQL OID allocation space is exhausted"))?;
        self.mappings.insert(object, oid);
        Ok(oid)
    }

    fn remove(&mut self, object: PostgresOidObject) {
        self.mappings.remove(&object);
    }

    fn validate_metadata(&self) -> Result<()> {
        if self.first_user_oid != POSTGRES_OID_FIRST_USER {
            return Err(DbError::new(
                "XX001",
                "PostgreSQL OID registry has an incompatible built-in boundary",
            ));
        }
        if !(u64::from(POSTGRES_OID_FIRST_USER)..=POSTGRES_OID_EXHAUSTED).contains(&self.next_oid) {
            return Err(DbError::new(
                "XX001",
                "PostgreSQL OID registry has an invalid allocation cursor",
            ));
        }
        let mut seen = BTreeSet::new();
        for oid in self.mappings.values().copied() {
            if oid.get() < POSTGRES_OID_FIRST_USER {
                return Err(DbError::new(
                    "XX001",
                    "PostgreSQL OID registry maps a user object into the built-in range",
                ));
            }
            if u64::from(oid.get()) >= self.next_oid {
                return Err(DbError::new(
                    "XX001",
                    "PostgreSQL OID registry allocation cursor precedes a live mapping",
                ));
            }
            if !seen.insert(oid) {
                return Err(DbError::new(
                    "XX001",
                    "PostgreSQL OID registry contains a duplicate OID",
                ));
            }
        }
        Ok(())
    }

    fn validate(&self, expected: &BTreeSet<PostgresOidObject>) -> Result<()> {
        self.validate_metadata()?;
        let actual = self.mappings.keys().copied().collect::<BTreeSet<_>>();
        if actual != *expected {
            let missing = expected.difference(&actual).next();
            let stale = actual.difference(expected).next();
            return Err(DbError::new(
                "XX001",
                "PostgreSQL OID registry references do not match the live catalog",
            )
            .with_detail(format!("missing: {missing:?}; stale: {stale:?}")));
        }
        Ok(())
    }

    #[must_use]
    pub const fn first_user_oid(&self) -> u32 {
        self.first_user_oid
    }

    pub fn mappings(&self) -> impl Iterator<Item = (PostgresOidObject, PostgresOid)> + '_ {
        self.mappings.iter().map(|(object, oid)| (*object, *oid))
    }

    #[must_use]
    pub fn oid(&self, object: PostgresOidObject) -> Option<PostgresOid> {
        self.mappings.get(&object).copied()
    }

    #[must_use]
    pub fn object(&self, oid: PostgresOid) -> Option<PostgresOidObject> {
        self.mappings
            .iter()
            .find_map(|(object, candidate)| (*candidate == oid).then_some(*object))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogOwner(String);

impl CatalogOwner {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_CATALOG_OWNER_BYTES
            || value.as_bytes().contains(&0)
        {
            return Err(DbError::new(
                "22023",
                "catalog owner must contain between 1 and 63 bytes without NUL",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for CatalogOwner {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CatalogOwner {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CatalogOwnership {
    owners: BTreeMap<CatalogObjectRef, CatalogOwner>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CatalogOwnershipEntry {
    object: CatalogObjectRef,
    owner: CatalogOwner,
}

impl Serialize for CatalogOwnership {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.owners.len()))?;
        for (object, owner) in &self.owners {
            sequence.serialize_element(&CatalogOwnershipEntry {
                object: *object,
                owner: owner.clone(),
            })?;
        }
        sequence.end()
    }
}

struct CatalogOwnershipVisitor;

impl<'de> Visitor<'de> for CatalogOwnershipVisitor {
    type Value = CatalogOwnership;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded catalog ownership entry array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut owners = BTreeMap::new();
        while let Some(entry) = sequence.next_element::<CatalogOwnershipEntry>()? {
            if owners.len() >= MAX_DEPENDENCY_OBJECTS {
                return Err(de::Error::custom(
                    "catalog ownership exceeds its object limit",
                ));
            }
            if owners.insert(entry.object, entry.owner).is_some() {
                return Err(de::Error::custom(
                    "catalog ownership contains a duplicate object",
                ));
            }
        }
        Ok(CatalogOwnership { owners })
    }
}

impl<'de> Deserialize<'de> for CatalogOwnership {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(CatalogOwnershipVisitor)
    }
}

impl CatalogOwnership {
    fn owner_of(&self, object: CatalogObjectRef) -> Option<&CatalogOwner> {
        self.owners.get(&object)
    }

    fn assign(&mut self, object: CatalogObjectRef, owner: &CatalogOwner) {
        self.owners.insert(object, owner.clone());
    }

    fn remove(&mut self, object: CatalogObjectRef) {
        self.owners.remove(&object);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DependencyGraph {
    outgoing: BTreeMap<CatalogObjectRef, BTreeSet<CatalogObjectRef>>,
    incoming: BTreeMap<CatalogObjectRef, BTreeSet<CatalogObjectRef>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct DependencyEdge {
    dependent: CatalogObjectRef,
    referenced: CatalogObjectRef,
}

#[derive(Serialize)]
struct DependencyGraphRef {
    edges: Vec<DependencyEdge>,
}

#[derive(Deserialize)]
struct DependencyGraphOwned {
    #[serde(default)]
    edges: Vec<DependencyEdge>,
}

impl Serialize for DependencyGraph {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let edges = self
            .outgoing
            .iter()
            .flat_map(|(dependent, references)| {
                references.iter().map(|referenced| DependencyEdge {
                    dependent: *dependent,
                    referenced: *referenced,
                })
            })
            .collect();
        DependencyGraphRef { edges }.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DependencyGraph {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = DependencyGraphOwned::deserialize(deserializer)?;
        if encoded.edges.len() > MAX_DEPENDENCY_OBJECTS {
            return Err(de::Error::custom(
                "catalog dependency graph exceeds its edge limit",
            ));
        }
        let mut graph = Self::default();
        for edge in encoded.edges {
            graph
                .add(edge.dependent, edge.referenced)
                .map_err(de::Error::custom)?;
        }
        Ok(graph)
    }
}

impl DependencyGraph {
    pub fn add(&mut self, dependent: CatalogObjectRef, referenced: CatalogObjectRef) -> Result<()> {
        if dependent == referenced || self.reaches(referenced, dependent)? {
            return Err(DbError::new("2BP01", "catalog dependency cycle detected")
                .with_detail(format!("{dependent:?} cannot depend on {referenced:?}")));
        }
        self.outgoing
            .entry(dependent)
            .or_default()
            .insert(referenced);
        self.incoming
            .entry(referenced)
            .or_default()
            .insert(dependent);
        Ok(())
    }

    pub fn references(
        &self,
        object: CatalogObjectRef,
    ) -> impl Iterator<Item = CatalogObjectRef> + '_ {
        self.outgoing
            .get(&object)
            .into_iter()
            .flat_map(|values| values.iter().copied())
    }

    pub fn dependents(
        &self,
        object: CatalogObjectRef,
    ) -> impl Iterator<Item = CatalogObjectRef> + '_ {
        self.incoming
            .get(&object)
            .into_iter()
            .flat_map(|values| values.iter().copied())
    }

    pub fn drop_order(
        &self,
        root: CatalogObjectRef,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        let direct = self.incoming.get(&root).cloned().unwrap_or_default();
        if behavior == DropBehavior::Restrict && !direct.is_empty() {
            return Err(DbError::new(
                "2BP01",
                "cannot drop object because other objects depend on it",
            )
            .with_detail(format!("dependents: {direct:?}"))
            .with_hint("Use DROP ... CASCADE to remove dependent objects."));
        }

        let mut stack = vec![(root, false)];
        let mut seen = BTreeSet::new();
        let mut order = Vec::new();
        while let Some((object, expanded)) = stack.pop() {
            if expanded {
                order.push(object);
                continue;
            }
            if !seen.insert(object) {
                continue;
            }
            if seen.len() > MAX_DEPENDENCY_OBJECTS {
                return Err(DbError::new(
                    "54001",
                    "catalog dependency traversal exceeded its object limit",
                ));
            }
            stack.push((object, true));
            if behavior == DropBehavior::Cascade
                && let Some(dependents) = self.incoming.get(&object)
            {
                for dependent in dependents.iter().rev() {
                    stack.push((*dependent, false));
                }
            }
        }
        Ok(order)
    }

    pub fn remove(&mut self, object: CatalogObjectRef) {
        self.remove_references(object);
        if let Some(dependents) = self.incoming.remove(&object) {
            for dependent in dependents {
                remove_set_value(&mut self.outgoing, dependent, object);
            }
        }
    }

    pub fn remove_references(&mut self, dependent: CatalogObjectRef) {
        if let Some(references) = self.outgoing.remove(&dependent) {
            for referenced in references {
                remove_set_value(&mut self.incoming, referenced, dependent);
            }
        }
    }

    fn reaches(&self, start: CatalogObjectRef, target: CatalogObjectRef) -> Result<bool> {
        let mut stack = vec![start];
        let mut seen = BTreeSet::new();
        while let Some(object) = stack.pop() {
            if object == target {
                return Ok(true);
            }
            if !seen.insert(object) {
                continue;
            }
            if seen.len() > MAX_DEPENDENCY_OBJECTS {
                return Err(DbError::new(
                    "54001",
                    "catalog dependency traversal exceeded its object limit",
                ));
            }
            if let Some(references) = self.outgoing.get(&object) {
                stack.extend(references.iter().copied());
            }
        }
        Ok(false)
    }
}

fn remove_set_value(
    map: &mut BTreeMap<CatalogObjectRef, BTreeSet<CatalogObjectRef>>,
    key: CatalogObjectRef,
    value: CatalogObjectRef,
) {
    if let Some(values) = map.get_mut(&key) {
        values.remove(&value);
        if values.is_empty() {
            map.remove(&key);
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub null_count: u64,
    pub distinct_count: u64,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableStatistics {
    pub row_count: u64,
    pub columns: BTreeMap<ColumnId, ColumnStatistics>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDefinition {
    pub id: TableId,
    pub schema_id: SchemaId,
    pub name: Identifier,
    columns: Vec<ColumnDefinition>,
    #[serde(default)]
    indexes: BTreeMap<Identifier, IndexDefinition>,
    #[serde(default)]
    constraints: BTreeMap<Identifier, ConstraintDefinition>,
    #[serde(default)]
    triggers: BTreeMap<Identifier, TriggerDefinition>,
    #[serde(default)]
    statistics: TableStatistics,
}

impl TableDefinition {
    #[must_use]
    pub fn expression_scope(column_name: Identifier, data_type: ScalarType) -> Self {
        Self {
            id: TableId::new(1),
            schema_id: SchemaId::new(1),
            name: Identifier::unquoted("__expression_scope"),
            columns: vec![ColumnDefinition {
                id: ColumnId::new(1),
                name: column_name,
                data_type,
                declared_type: None,
                nullable: true,
                primary_key: false,
                unique: false,
                default: None,
            }],
            indexes: BTreeMap::new(),
            constraints: BTreeMap::new(),
            triggers: BTreeMap::new(),
            statistics: TableStatistics::default(),
        }
    }

    pub fn expression_scope_for_schema(name: Identifier, schema: &Schema) -> Result<Self> {
        let columns = schema
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let id = u64::try_from(index)
                    .ok()
                    .and_then(|index| index.checked_add(1))
                    .map(ColumnId::new)
                    .ok_or_else(|| {
                        DbError::new("54000", "relation expression scope is too wide")
                    })?;
                Ok(ColumnDefinition {
                    id,
                    name: Identifier::unquoted(field.name.clone()),
                    data_type: field.data_type.clone(),
                    declared_type: None,
                    nullable: field.nullable,
                    primary_key: false,
                    unique: false,
                    default: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            id: TableId::new(1),
            schema_id: SchemaId::new(1),
            name,
            columns,
            indexes: BTreeMap::new(),
            constraints: BTreeMap::new(),
            triggers: BTreeMap::new(),
            statistics: TableStatistics::default(),
        })
    }

    #[must_use]
    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }

    #[must_use]
    pub fn column(&self, name: &Identifier) -> Option<&ColumnDefinition> {
        self.columns.iter().find(|column| &column.name == name)
    }

    #[must_use]
    pub fn column_index(&self, name: &Identifier) -> Option<usize> {
        self.columns.iter().position(|column| &column.name == name)
    }

    #[must_use]
    pub fn column_index_by_id(&self, id: ColumnId) -> Option<usize> {
        self.columns.iter().position(|column| column.id == id)
    }

    pub fn indexes(&self) -> impl Iterator<Item = &IndexDefinition> {
        self.indexes.values()
    }

    #[must_use]
    pub fn index(&self, name: &Identifier) -> Option<&IndexDefinition> {
        self.indexes.get(name)
    }

    pub fn constraints(&self) -> impl Iterator<Item = &ConstraintDefinition> {
        self.constraints.values()
    }

    #[must_use]
    pub fn constraint(&self, name: &Identifier) -> Option<&ConstraintDefinition> {
        self.constraints.get(name)
    }

    pub fn triggers(&self) -> impl Iterator<Item = &TriggerDefinition> {
        self.triggers.values()
    }

    #[must_use]
    pub fn trigger(&self, name: &Identifier) -> Option<&TriggerDefinition> {
        self.triggers.get(name)
    }

    #[must_use]
    pub const fn statistics(&self) -> &TableStatistics {
        &self.statistics
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub id: SchemaId,
    pub database_id: DatabaseId,
    pub name: Identifier,
    tables: BTreeMap<Identifier, TableDefinition>,
    #[serde(default)]
    sequences: BTreeMap<Identifier, SequenceDefinition>,
    #[serde(default)]
    views: BTreeMap<Identifier, ViewDefinition>,
    #[serde(default)]
    routines: BTreeMap<Identifier, Vec<RoutineDefinition>>,
    #[serde(default)]
    types: BTreeMap<Identifier, TypeDefinition>,
}

impl SchemaDefinition {
    pub fn tables(&self) -> impl Iterator<Item = &TableDefinition> {
        self.tables.values()
    }

    #[must_use]
    pub fn table(&self, name: &Identifier) -> Option<&TableDefinition> {
        self.tables.get(name).or_else(|| {
            system::is_system_schema_id(self.id)
                .then(|| {
                    self.tables
                        .values()
                        .find(|table| table.name.as_str() == name.as_str())
                })
                .flatten()
        })
    }

    pub fn sequences(&self) -> impl Iterator<Item = &SequenceDefinition> {
        self.sequences.values()
    }

    #[must_use]
    pub fn sequence(&self, name: &Identifier) -> Option<&SequenceDefinition> {
        self.sequences.get(name)
    }

    pub fn views(&self) -> impl Iterator<Item = &ViewDefinition> {
        self.views.values()
    }

    #[must_use]
    pub fn view(&self, name: &Identifier) -> Option<&ViewDefinition> {
        self.views.get(name)
    }

    pub fn routines(&self) -> impl Iterator<Item = &RoutineDefinition> {
        self.routines.values().flatten()
    }

    #[must_use]
    pub fn routines_named(&self, name: &Identifier) -> &[RoutineDefinition] {
        self.routines
            .get(name)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub fn types(&self) -> impl Iterator<Item = &TypeDefinition> {
        self.types.values()
    }

    #[must_use]
    pub fn user_defined_type(&self, name: &Identifier) -> Option<&TypeDefinition> {
        self.types.get(name)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatabaseDefinition {
    pub id: DatabaseId,
    pub name: Identifier,
    schemas: BTreeMap<Identifier, SchemaDefinition>,
}

impl DatabaseDefinition {
    pub fn schemas(&self) -> impl Iterator<Item = &SchemaDefinition> {
        self.schemas.values()
    }

    #[must_use]
    pub fn schema(&self, name: &Identifier) -> Option<&SchemaDefinition> {
        self.schemas.get(name)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Catalog {
    database: DatabaseDefinition,
    next_schema_id: u64,
    next_table_id: u64,
    next_column_id: u64,
    #[serde(default = "initial_index_id")]
    next_index_id: u64,
    #[serde(default = "initial_object_id")]
    next_constraint_id: u64,
    #[serde(default = "initial_object_id")]
    next_sequence_id: u64,
    #[serde(default = "initial_object_id")]
    next_view_id: u64,
    #[serde(default = "initial_object_id")]
    next_routine_id: u64,
    #[serde(default = "initial_object_id")]
    next_trigger_id: u64,
    #[serde(default = "initial_object_id")]
    next_type_id: u64,
    #[serde(default)]
    dependencies: DependencyGraph,
    #[serde(default)]
    ownership: CatalogOwnership,
    postgres_oid_registry: PostgresOidRegistry,
}

#[derive(Deserialize)]
struct CatalogOwned {
    database: DatabaseDefinition,
    next_schema_id: u64,
    next_table_id: u64,
    next_column_id: u64,
    #[serde(default = "initial_index_id")]
    next_index_id: u64,
    #[serde(default = "initial_object_id")]
    next_constraint_id: u64,
    #[serde(default = "initial_object_id")]
    next_sequence_id: u64,
    #[serde(default = "initial_object_id")]
    next_view_id: u64,
    #[serde(default = "initial_object_id")]
    next_routine_id: u64,
    #[serde(default = "initial_object_id")]
    next_trigger_id: u64,
    #[serde(default = "initial_object_id")]
    next_type_id: u64,
    #[serde(default)]
    dependencies: DependencyGraph,
    #[serde(default)]
    ownership: CatalogOwnership,
    #[serde(default)]
    postgres_oid_registry: Option<PostgresOidRegistry>,
}

impl<'de> Deserialize<'de> for Catalog {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = CatalogOwned::deserialize(deserializer)?;
        let mut catalog = Self {
            database: encoded.database,
            next_schema_id: encoded.next_schema_id,
            next_table_id: encoded.next_table_id,
            next_column_id: encoded.next_column_id,
            next_index_id: encoded.next_index_id,
            next_constraint_id: encoded.next_constraint_id,
            next_sequence_id: encoded.next_sequence_id,
            next_view_id: encoded.next_view_id,
            next_routine_id: encoded.next_routine_id,
            next_trigger_id: encoded.next_trigger_id,
            next_type_id: encoded.next_type_id,
            dependencies: encoded.dependencies,
            ownership: encoded.ownership,
            postgres_oid_registry: PostgresOidRegistry {
                first_user_oid: POSTGRES_OID_FIRST_USER,
                next_oid: u64::from(POSTGRES_OID_FIRST_USER),
                mappings: BTreeMap::new(),
            },
        };
        catalog.postgres_oid_registry = match encoded.postgres_oid_registry {
            Some(registry) => registry,
            None => PostgresOidRegistry::reconstruct(&catalog.postgres_oid_objects())
                .map_err(de::Error::custom)?,
        };
        catalog
            .validate_postgres_oid_registry()
            .map_err(de::Error::custom)?;
        Ok(catalog)
    }
}

const fn initial_index_id() -> u64 {
    1
}

const fn initial_object_id() -> u64 {
    1
}

impl Default for Catalog {
    fn default() -> Self {
        Self::bootstrap("ordadb")
    }
}

impl Catalog {
    #[must_use]
    pub fn bootstrap(database_name: impl Into<String>) -> Self {
        let database_id = DatabaseId::new(1);
        let public_schema = SchemaDefinition {
            id: SchemaId::new(1),
            database_id,
            name: Identifier::unquoted("public"),
            tables: BTreeMap::new(),
            sequences: BTreeMap::new(),
            views: BTreeMap::new(),
            routines: BTreeMap::new(),
            types: BTreeMap::new(),
        };
        let mut schemas = BTreeMap::new();
        schemas.insert(public_schema.name.clone(), public_schema);

        Self {
            database: DatabaseDefinition {
                id: database_id,
                name: Identifier::unquoted(database_name),
                schemas,
            },
            next_schema_id: 2,
            next_table_id: 1,
            next_column_id: 1,
            next_index_id: initial_index_id(),
            next_constraint_id: initial_object_id(),
            next_sequence_id: initial_object_id(),
            next_view_id: initial_object_id(),
            next_routine_id: initial_object_id(),
            next_trigger_id: initial_object_id(),
            next_type_id: initial_object_id(),
            dependencies: DependencyGraph::default(),
            ownership: CatalogOwnership::default(),
            postgres_oid_registry: PostgresOidRegistry::bootstrap(database_id, SchemaId::new(1)),
        }
    }

    #[must_use]
    pub const fn database(&self) -> &DatabaseDefinition {
        &self.database
    }

    #[must_use]
    pub const fn postgres_oid_registry(&self) -> &PostgresOidRegistry {
        &self.postgres_oid_registry
    }

    pub fn postgres_oid(&self, object: PostgresOidObject) -> Result<PostgresOid> {
        if let Some(oid) = self.postgres_oid_registry.oid(object) {
            return Ok(oid);
        }
        if self.postgres_oid_objects().contains(&object) {
            return Err(DbError::new(
                "XX001",
                "PostgreSQL OID registry is missing a live catalog object",
            ));
        }
        Err(DbError::new(
            "22023",
            "PostgreSQL OID was requested for an object outside the live catalog",
        ))
    }

    #[must_use]
    pub fn postgres_oid_object(&self, oid: PostgresOid) -> Option<PostgresOidObject> {
        self.postgres_oid_registry.object(oid)
    }

    pub fn validate_postgres_oid_registry(&self) -> Result<()> {
        self.postgres_oid_registry
            .validate(&self.postgres_oid_objects())
    }

    fn postgres_oid_objects(&self) -> BTreeSet<PostgresOidObject> {
        let mut objects = BTreeSet::from([PostgresOidObject::Database(self.database.id)]);
        for schema in self.database.schemas() {
            objects.insert(PostgresOidObject::Schema(schema.id));
            for table in schema.tables() {
                objects.insert(PostgresOidObject::Table(table.id));
                objects.extend(
                    table
                        .columns()
                        .iter()
                        .map(|column| PostgresOidObject::Column(table.id, column.id)),
                );
                objects.extend(
                    table
                        .indexes()
                        .map(|index| PostgresOidObject::Index(index.id)),
                );
                objects.extend(
                    table
                        .constraints()
                        .map(|constraint| PostgresOidObject::Constraint(constraint.id)),
                );
                objects.extend(
                    table
                        .triggers()
                        .map(|trigger| PostgresOidObject::Trigger(trigger.id)),
                );
            }
            objects.extend(
                schema
                    .sequences()
                    .map(|sequence| PostgresOidObject::Sequence(sequence.id)),
            );
            objects.extend(schema.views().map(|view| PostgresOidObject::View(view.id)));
            objects.extend(
                schema
                    .views()
                    .flat_map(ViewDefinition::triggers)
                    .map(|trigger| PostgresOidObject::Trigger(trigger.id)),
            );
            objects.extend(
                schema
                    .routines()
                    .map(|routine| PostgresOidObject::Routine(routine.id)),
            );
            for definition in schema.types() {
                objects.insert(PostgresOidObject::Type(definition.id));
                if let UserDefinedTypeKind::Domain { checks, .. } = &definition.definition {
                    objects.extend(
                        checks
                            .iter()
                            .filter_map(|constraint| constraint.id)
                            .map(PostgresOidObject::Constraint),
                    );
                }
            }
        }
        objects
    }

    fn postgres_oid_candidate(
        &self,
        objects: impl IntoIterator<Item = PostgresOidObject>,
    ) -> Result<PostgresOidRegistry> {
        let mut registry = self.postgres_oid_registry.clone();
        for object in objects {
            registry.allocate(object)?;
        }
        Ok(registry)
    }

    fn publish_postgres_oid_candidate(&mut self, registry: PostgresOidRegistry) -> Result<()> {
        registry.validate(&self.postgres_oid_objects())?;
        self.postgres_oid_registry = registry;
        Ok(())
    }

    #[must_use]
    pub fn schema(&self, name: &Identifier) -> Option<&SchemaDefinition> {
        system::system_schema(name).or_else(|| self.database.schema(name))
    }

    #[must_use]
    pub fn schema_by_id(&self, schema_id: SchemaId) -> Option<&SchemaDefinition> {
        system::system_schema_by_id(schema_id).or_else(|| {
            self.database
                .schemas()
                .find(|schema| schema.id == schema_id)
        })
    }

    #[must_use]
    pub fn is_system_schema(name: &Identifier) -> bool {
        system::is_system_schema_name(name)
    }

    #[must_use]
    pub fn is_system_table(table_id: TableId) -> bool {
        system_relation(table_id).is_some()
    }

    fn ensure_writable_schema_id(&self, schema_id: SchemaId) -> Result<()> {
        let is_system = system::is_system_schema_id(schema_id)
            || self
                .database
                .schemas()
                .any(|schema| schema.id == schema_id && Self::is_system_schema(&schema.name));
        if is_system {
            return Err(system_catalog_read_only());
        }
        Ok(())
    }

    fn ensure_writable_table_id(&self, table_id: TableId) -> Result<()> {
        let is_system = Self::is_system_table(table_id)
            || self
                .database
                .schemas()
                .filter(|schema| Self::is_system_schema(&schema.name))
                .flat_map(SchemaDefinition::tables)
                .any(|table| table.id == table_id);
        if is_system {
            return Err(system_catalog_read_only());
        }
        Ok(())
    }

    #[must_use]
    pub const fn dependencies(&self) -> &DependencyGraph {
        &self.dependencies
    }

    #[must_use]
    pub fn owner_of(&self, object: CatalogObjectRef) -> Option<&CatalogOwner> {
        self.ownership.owner_of(object)
    }

    pub fn assign_new_object_owners(
        &mut self,
        previous: &Self,
        owner: &CatalogOwner,
    ) -> Result<()> {
        let previous_objects = previous.object_refs();
        let current_objects = self.object_refs();
        for object in current_objects.difference(&previous_objects) {
            self.ownership.assign(*object, owner);
        }
        if self.ownership.owners.len() > MAX_DEPENDENCY_OBJECTS {
            return Err(DbError::new(
                "54001",
                "catalog ownership exceeds its object limit",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn object_refs(&self) -> BTreeSet<CatalogObjectRef> {
        let mut objects = BTreeSet::new();
        for schema in self.database.schemas() {
            objects.insert(CatalogObjectRef::Schema(schema.id));
            for table in schema.tables() {
                objects.insert(CatalogObjectRef::Table(table.id));
                objects.extend(
                    table
                        .columns()
                        .iter()
                        .map(|column| CatalogObjectRef::Column(table.id, column.id)),
                );
                objects.extend(
                    table
                        .indexes()
                        .map(|index| CatalogObjectRef::Index(index.id)),
                );
                objects.extend(
                    table
                        .constraints()
                        .map(|constraint| CatalogObjectRef::Constraint(constraint.id)),
                );
                objects.extend(
                    table
                        .triggers()
                        .map(|trigger| CatalogObjectRef::Trigger(trigger.id)),
                );
            }
            objects.extend(
                schema
                    .sequences()
                    .map(|sequence| CatalogObjectRef::Sequence(sequence.id)),
            );
            objects.extend(schema.views().map(|view| CatalogObjectRef::View(view.id)));
            objects.extend(
                schema
                    .views()
                    .flat_map(ViewDefinition::triggers)
                    .map(|trigger| CatalogObjectRef::Trigger(trigger.id)),
            );
            objects.extend(
                schema
                    .routines()
                    .map(|routine| CatalogObjectRef::Routine(routine.id)),
            );
            objects.extend(
                schema
                    .types()
                    .map(|definition| CatalogObjectRef::Type(definition.id)),
            );
        }
        objects
    }

    #[must_use]
    pub fn sequence(
        &self,
        schema_name: &Identifier,
        sequence_name: &Identifier,
    ) -> Option<&SequenceDefinition> {
        self.schema(schema_name)?.sequence(sequence_name)
    }

    #[must_use]
    pub fn view(
        &self,
        schema_name: &Identifier,
        view_name: &Identifier,
    ) -> Option<&ViewDefinition> {
        self.schema(schema_name)?.view(view_name)
    }

    #[must_use]
    pub fn routines_named(
        &self,
        schema_name: &Identifier,
        routine_name: &Identifier,
    ) -> &[RoutineDefinition] {
        self.schema(schema_name)
            .map(|schema| schema.routines_named(routine_name))
            .unwrap_or_default()
    }

    #[must_use]
    pub fn routine_by_signature(
        &self,
        schema_name: &Identifier,
        routine_name: &Identifier,
        kind: RoutineKind,
        arguments: &[RoutineArgument],
    ) -> Option<&RoutineDefinition> {
        self.routines_named(schema_name, routine_name)
            .iter()
            .find(|routine| {
                routine.kind == kind
                    && routine_input_signature_matches(&routine.arguments, arguments)
            })
    }

    #[must_use]
    pub fn user_defined_type(
        &self,
        schema_name: &Identifier,
        type_name: &Identifier,
    ) -> Option<&TypeDefinition> {
        self.schema(schema_name)?.user_defined_type(type_name)
    }

    #[must_use]
    pub fn type_by_id(&self, type_id: TypeId) -> Option<&TypeDefinition> {
        self.database
            .schemas()
            .flat_map(SchemaDefinition::types)
            .find(|definition| definition.id == type_id)
    }

    pub fn create_enum_type(
        &mut self,
        schema_name: &Identifier,
        name: Identifier,
        labels: Vec<String>,
    ) -> Result<TypeId> {
        ensure_writable_schema_name(schema_name)?;
        if labels.is_empty() {
            return Err(DbError::new(
                "42601",
                "an enum type must declare at least one label",
            ));
        }
        let mut seen = BTreeSet::new();
        for label in &labels {
            validate_enum_label(label)?;
            if !seen.insert(label) {
                return Err(DbError::new(
                    "42710",
                    format!("enum label {label:?} is specified more than once"),
                ));
            }
        }
        self.create_user_defined_type(schema_name, name, UserDefinedTypeKind::Enum { labels })
    }

    pub fn alter_enum_add_value(
        &mut self,
        type_id: TypeId,
        label: String,
        position: Option<EnumValuePosition>,
        if_not_exists: bool,
    ) -> Result<bool> {
        validate_enum_label(&label)?;
        let logical_type = {
            let definition = self.type_by_id_mut(type_id)?;
            let UserDefinedTypeKind::Enum { labels } = &mut definition.definition else {
                return Err(DbError::new(
                    "42809",
                    "ALTER TYPE ADD VALUE requires an enum type",
                ));
            };
            if labels.iter().any(|existing| existing == &label) {
                if if_not_exists {
                    return Ok(false);
                }
                return Err(DbError::new(
                    "42710",
                    format!("enum label {label:?} already exists"),
                ));
            }
            let index = match position {
                None => labels.len(),
                Some(EnumValuePosition::Before(neighbor)) => labels
                    .iter()
                    .position(|existing| existing == &neighbor)
                    .ok_or_else(|| enum_neighbor_missing(&neighbor))?,
                Some(EnumValuePosition::After(neighbor)) => labels
                    .iter()
                    .position(|existing| existing == &neighbor)
                    .ok_or_else(|| enum_neighbor_missing(&neighbor))?
                    .saturating_add(1),
            };
            labels.insert(index, label);
            definition.logical_type()
        };
        self.refresh_declared_type_cache(type_id, &logical_type);
        Ok(true)
    }

    pub fn alter_enum_rename_value(
        &mut self,
        type_id: TypeId,
        old_label: &str,
        new_label: String,
    ) -> Result<()> {
        validate_enum_label(&new_label)?;
        let logical_type = {
            let definition = self.type_by_id_mut(type_id)?;
            let UserDefinedTypeKind::Enum { labels } = &mut definition.definition else {
                return Err(DbError::new(
                    "42809",
                    "ALTER TYPE RENAME VALUE requires an enum type",
                ));
            };
            if labels.iter().any(|existing| existing == &new_label) {
                return Err(DbError::new(
                    "42710",
                    format!("enum label {new_label:?} already exists"),
                ));
            }
            let label = labels
                .iter_mut()
                .find(|existing| existing.as_str() == old_label)
                .ok_or_else(|| enum_neighbor_missing(old_label))?;
            *label = new_label;
            definition.logical_type()
        };
        self.refresh_declared_type_cache(type_id, &logical_type);
        Ok(())
    }

    pub fn create_domain(
        &mut self,
        schema_name: &Identifier,
        name: Identifier,
        base_type: ScalarType,
        not_null: bool,
        default: Option<CatalogExpression>,
        checks: Vec<DomainConstraint>,
    ) -> Result<TypeId> {
        self.create_domain_with_declared_type(
            schema_name,
            name,
            DomainBaseType::new(base_type, None),
            not_null,
            default,
            checks,
        )
    }

    pub fn create_domain_with_declared_type(
        &mut self,
        schema_name: &Identifier,
        name: Identifier,
        base: DomainBaseType,
        not_null: bool,
        default: Option<CatalogExpression>,
        mut checks: Vec<DomainConstraint>,
    ) -> Result<TypeId> {
        ensure_writable_schema_name(schema_name)?;
        let DomainBaseType {
            data_type: base_type,
            declared_type: base_declared_type,
        } = base;
        if let Some(type_id) = base_declared_type {
            let definition = self
                .type_by_id(type_id)
                .ok_or_else(|| DbError::new("42704", "domain base type does not exist"))?;
            if matches!(definition.definition, UserDefinedTypeKind::Domain { .. }) {
                return Err(DbError::new(
                    "0A000",
                    "domains whose base type is another domain are not supported yet",
                ));
            }
        }
        if checks.len() > MAX_DEPENDENCY_OBJECTS {
            return Err(DbError::new(
                "54000",
                "domain constraint count exceeds the catalog limit",
            ));
        }
        let mut names = BTreeSet::new();
        let mut next_constraint_id = self.next_constraint_id;
        for constraint in &mut checks {
            if let Some(name) = &constraint.name
                && !names.insert(name.clone())
            {
                return Err(DbError::new(
                    "42710",
                    format!("constraint {name} is specified more than once"),
                ));
            }
            constraint.id = Some(ConstraintId::new(next_constraint_id));
            next_constraint_id = next_constraint_id
                .checked_add(1)
                .ok_or_else(|| DbError::new("54000", "catalog constraint ID space is exhausted"))?;
        }
        let expected_id = TypeId::new(self.next_type_id);
        let mut dependencies = self.dependencies.clone();
        if let Some(type_id) = base_declared_type {
            dependencies.add(
                CatalogObjectRef::Type(expected_id),
                CatalogObjectRef::Type(type_id),
            )?;
        }
        let type_id = self.create_user_defined_type(
            schema_name,
            name,
            UserDefinedTypeKind::Domain {
                base_type,
                base_declared_type,
                not_null,
                default,
                checks,
            },
        )?;
        if type_id != expected_id {
            return Err(DbError::internal(
                "catalog allocated an unexpected domain type ID",
            ));
        }
        self.dependencies = dependencies;
        self.next_constraint_id = next_constraint_id;
        Ok(type_id)
    }

    pub fn alter_domain_default(
        &mut self,
        type_id: TypeId,
        default: Option<CatalogExpression>,
    ) -> Result<()> {
        let definition = self.type_by_id_mut(type_id)?;
        let UserDefinedTypeKind::Domain {
            default: current, ..
        } = &mut definition.definition
        else {
            return Err(DbError::new("42809", "ALTER DOMAIN requires a domain type"));
        };
        *current = default;
        Ok(())
    }

    pub fn alter_domain_not_null(&mut self, type_id: TypeId, not_null: bool) -> Result<()> {
        let definition = self.type_by_id_mut(type_id)?;
        let UserDefinedTypeKind::Domain {
            not_null: current, ..
        } = &mut definition.definition
        else {
            return Err(DbError::new("42809", "ALTER DOMAIN requires a domain type"));
        };
        *current = not_null;
        Ok(())
    }

    pub fn add_domain_constraint(
        &mut self,
        type_id: TypeId,
        mut constraint: DomainConstraint,
    ) -> Result<()> {
        let definition = self
            .type_by_id(type_id)
            .ok_or_else(|| DbError::new("42704", "type does not exist"))?;
        let UserDefinedTypeKind::Domain { checks, .. } = &definition.definition else {
            return Err(DbError::new("42809", "ALTER DOMAIN requires a domain type"));
        };
        if checks.len() >= MAX_DEPENDENCY_OBJECTS {
            return Err(DbError::new(
                "54000",
                "domain constraint count exceeds the catalog limit",
            ));
        }
        if let Some(name) = &constraint.name
            && checks
                .iter()
                .any(|existing| existing.name.as_ref() == Some(name))
        {
            return Err(DbError::new(
                "42710",
                format!("constraint {name} already exists"),
            ));
        }
        let constraint_id = ConstraintId::new(self.next_constraint_id);
        let oid_registry =
            self.postgres_oid_candidate([PostgresOidObject::Constraint(constraint_id)])?;
        let next_constraint_id = self
            .next_constraint_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog constraint ID space is exhausted"))?;
        constraint.id = Some(constraint_id);
        let definition = self.type_by_id_mut(type_id)?;
        let UserDefinedTypeKind::Domain { checks, .. } = &mut definition.definition else {
            return Err(DbError::internal(
                "validated domain changed kind before constraint publication",
            ));
        };
        checks.push(constraint);
        self.next_constraint_id = next_constraint_id;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(())
    }

    pub fn drop_domain_constraint(
        &mut self,
        type_id: TypeId,
        name: &Identifier,
        if_exists: bool,
    ) -> Result<bool> {
        let definition = self.type_by_id_mut(type_id)?;
        let UserDefinedTypeKind::Domain { checks, .. } = &mut definition.definition else {
            return Err(DbError::new("42809", "ALTER DOMAIN requires a domain type"));
        };
        let Some(index) = checks
            .iter()
            .position(|constraint| constraint.name.as_ref() == Some(name))
        else {
            if if_exists {
                return Ok(false);
            }
            return Err(DbError::new(
                "42704",
                format!("constraint {name} does not exist"),
            ));
        };
        let constraint_id = checks[index].id;
        checks.remove(index);
        if let Some(constraint_id) = constraint_id {
            self.postgres_oid_registry
                .remove(PostgresOidObject::Constraint(constraint_id));
        }
        self.validate_postgres_oid_registry()?;
        Ok(true)
    }

    pub fn drop_type(
        &mut self,
        type_id: TypeId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        if self.type_by_id(type_id).is_none() {
            return Err(DbError::new("42704", "type does not exist"));
        }
        self.drop_catalog_object(CatalogObjectRef::Type(type_id), behavior)
    }

    fn create_user_defined_type(
        &mut self,
        schema_name: &Identifier,
        name: Identifier,
        definition: UserDefinedTypeKind,
    ) -> Result<TypeId> {
        ensure_writable_schema_name(schema_name)?;
        let schema_id = {
            let schema = self.database.schemas.get(schema_name).ok_or_else(|| {
                DbError::new("3F000", format!("schema {schema_name} does not exist"))
            })?;
            if schema.types.contains_key(&name) {
                return Err(DbError::new("42710", format!("type {name} already exists")));
            }
            schema.id
        };
        let id = TypeId::new(self.next_type_id);
        let mut oid_objects = vec![PostgresOidObject::Type(id)];
        if let UserDefinedTypeKind::Domain { checks, .. } = &definition {
            oid_objects.extend(
                checks
                    .iter()
                    .filter_map(|constraint| constraint.id)
                    .map(PostgresOidObject::Constraint),
            );
        }
        let oid_registry = self.postgres_oid_candidate(oid_objects)?;
        let next_type_id = self
            .next_type_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog type ID space is exhausted"))?;
        self.schema_by_id_mut(schema_id)?.types.insert(
            name.clone(),
            TypeDefinition {
                id,
                schema_id,
                name,
                definition,
            },
        );
        self.next_type_id = next_type_id;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(id)
    }

    fn type_by_id_mut(&mut self, type_id: TypeId) -> Result<&mut TypeDefinition> {
        self.database
            .schemas
            .values_mut()
            .flat_map(|schema| schema.types.values_mut())
            .find(|definition| definition.id == type_id)
            .ok_or_else(|| DbError::new("42704", "type does not exist"))
    }

    fn refresh_declared_type_cache(&mut self, type_id: TypeId, logical_type: &ScalarType) {
        let mut declared_types = vec![(type_id, logical_type.clone())];
        for schema in self.database.schemas.values_mut() {
            for definition in schema.types.values_mut() {
                let UserDefinedTypeKind::Domain {
                    base_type,
                    base_declared_type: Some(base_type_id),
                    ..
                } = &mut definition.definition
                else {
                    continue;
                };
                if *base_type_id == type_id {
                    refresh_declared_scalar_type(base_type, logical_type);
                    declared_types.push((definition.id, definition.logical_type()));
                }
            }
        }
        for schema in self.database.schemas.values_mut() {
            for table in schema.tables.values_mut() {
                for column in &mut table.columns {
                    if let Some((_, declared_type)) =
                        declared_types.iter().find(|(declared_type_id, _)| {
                            column.declared_type == Some(*declared_type_id)
                        })
                    {
                        refresh_declared_scalar_type(&mut column.data_type, declared_type);
                    }
                }
            }
            for routine in schema.routines.values_mut().flatten() {
                for argument in &mut routine.arguments {
                    if let Some((_, declared_type)) =
                        declared_types.iter().find(|(declared_type_id, _)| {
                            argument.declared_type == Some(*declared_type_id)
                        })
                    {
                        refresh_declared_scalar_type(&mut argument.data_type, declared_type);
                    }
                }
                if let Some((_, declared_type)) =
                    declared_types.iter().find(|(declared_type_id, _)| {
                        routine.return_declared_type == Some(*declared_type_id)
                    })
                    && let Some(return_type) = &mut routine.return_type
                {
                    refresh_declared_scalar_type(return_type, declared_type);
                }
            }
        }
    }

    #[must_use]
    pub fn index(
        &self,
        schema_name: &Identifier,
        index_name: &Identifier,
    ) -> Option<&IndexDefinition> {
        self.schema(schema_name)?
            .tables()
            .find_map(|table| table.index(index_name))
    }

    pub fn create_schema(&mut self, name: Identifier) -> Result<SchemaId> {
        ensure_writable_schema_name(&name)?;
        if self.database.schemas.contains_key(&name) {
            return Err(DbError::new(
                "42P06",
                format!("schema {name} already exists"),
            ));
        }

        let id = SchemaId::new(self.next_schema_id);
        let oid_registry = self.postgres_oid_candidate([PostgresOidObject::Schema(id)])?;
        let next_schema_id = self
            .next_schema_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog schema ID space is exhausted"))?;
        self.database.schemas.insert(
            name.clone(),
            SchemaDefinition {
                id,
                database_id: self.database.id,
                name,
                tables: BTreeMap::new(),
                sequences: BTreeMap::new(),
                views: BTreeMap::new(),
                routines: BTreeMap::new(),
                types: BTreeMap::new(),
            },
        );
        self.next_schema_id = next_schema_id;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(id)
    }

    pub fn rename_schema(&mut self, schema_id: SchemaId, new_name: Identifier) -> Result<()> {
        self.ensure_writable_schema_id(schema_id)?;
        ensure_writable_schema_name(&new_name)?;
        if self.database.schemas.contains_key(&new_name) {
            return Err(DbError::new(
                "42P06",
                format!("schema {new_name} already exists"),
            ));
        }
        let old_name = self
            .schema_by_id(schema_id)
            .map(|schema| schema.name.clone())
            .ok_or_else(|| DbError::new("3F000", "schema does not exist"))?;
        let mut schema = self
            .database
            .schemas
            .remove(&old_name)
            .ok_or_else(|| DbError::internal("schema namespace changed during rename"))?;
        schema.name = new_name.clone();
        self.database.schemas.insert(new_name, schema);
        Ok(())
    }

    pub fn drop_schema(
        &mut self,
        schema_id: SchemaId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        self.ensure_writable_schema_id(schema_id)?;
        let schema = self
            .schema_by_id(schema_id)
            .ok_or_else(|| DbError::new("3F000", "schema does not exist"))?;
        let is_empty = schema.tables.is_empty()
            && schema.sequences.is_empty()
            && schema.views.is_empty()
            && schema.routines.is_empty()
            && schema.types.is_empty();
        if behavior == DropBehavior::Restrict && !is_empty {
            return Err(
                DbError::new("2BP01", "cannot drop schema because it contains objects")
                    .with_hint("Use DROP SCHEMA ... CASCADE to remove contained objects."),
            );
        }

        let mut roots = schema
            .tables()
            .map(|table| CatalogObjectRef::Table(table.id))
            .chain(
                schema
                    .sequences()
                    .map(|sequence| CatalogObjectRef::Sequence(sequence.id)),
            )
            .chain(schema.views().map(|view| CatalogObjectRef::View(view.id)))
            .chain(
                schema
                    .routines()
                    .map(|routine| CatalogObjectRef::Routine(routine.id)),
            )
            .chain(
                schema
                    .types()
                    .map(|definition| CatalogObjectRef::Type(definition.id)),
            )
            .collect::<Vec<_>>();
        roots.sort();
        let mut removed = Vec::new();
        for root in roots {
            for object in self.drop_catalog_object(root, DropBehavior::Cascade)? {
                if !removed.contains(&object) {
                    removed.push(object);
                }
            }
        }
        self.remove_catalog_object(CatalogObjectRef::Schema(schema_id))?;
        self.validate_postgres_oid_registry()?;
        removed.push(CatalogObjectRef::Schema(schema_id));
        Ok(removed)
    }

    pub fn create_table(
        &mut self,
        schema_name: &Identifier,
        table_name: Identifier,
        columns: Vec<NewColumn>,
    ) -> Result<TableId> {
        ensure_writable_schema_name(schema_name)?;
        if columns.is_empty() {
            return Err(DbError::new(
                "42601",
                "a table must contain at least one column",
            ));
        }
        if let Some(type_id) = columns
            .iter()
            .filter_map(|column| column.declared_type)
            .find(|type_id| self.type_by_id(*type_id).is_none())
        {
            return Err(DbError::new(
                "42704",
                format!("declared type {} does not exist", type_id.get()),
            ));
        }

        let schema =
            self.database.schemas.get_mut(schema_name).ok_or_else(|| {
                DbError::new("3F000", format!("schema {schema_name} does not exist"))
            })?;

        if schema.tables.contains_key(&table_name) {
            return Err(DbError::new(
                "42P07",
                format!("table {schema_name}.{table_name} already exists"),
            ));
        }

        let mut seen_columns = BTreeMap::<Identifier, ()>::new();
        let mut definitions = Vec::with_capacity(columns.len());
        for column in columns {
            if seen_columns.insert(column.name.clone(), ()).is_some() {
                return Err(DbError::new(
                    "42701",
                    format!("column {} specified more than once", column.name),
                ));
            }

            let id = ColumnId::new(self.next_column_id);
            self.next_column_id += 1;
            definitions.push(ColumnDefinition {
                id,
                name: column.name,
                data_type: column.data_type,
                declared_type: column.declared_type,
                nullable: column.nullable && !column.primary_key,
                primary_key: column.primary_key,
                unique: column.unique || column.primary_key,
                default: column.default,
            });
        }

        let table_id = TableId::new(self.next_table_id);
        self.next_table_id += 1;
        let mut indexes = BTreeMap::new();
        for column in &definitions {
            if column.unique {
                let suffix = if column.primary_key { "pkey" } else { "key" };
                let name = Identifier::unquoted(format!(
                    "{}_{}_{}",
                    table_name.as_str(),
                    column.name.as_str(),
                    suffix
                ));
                let id = IndexId::new(self.next_index_id);
                self.next_index_id += 1;
                indexes.insert(
                    name.clone(),
                    IndexDefinition {
                        id,
                        table_id,
                        name,
                        key_columns: vec![column.id],
                        include_columns: Vec::new(),
                        unique: true,
                        primary: column.primary_key,
                        method: IndexMethod::BTree,
                        options: IndexOptions::BTree,
                    },
                );
            }
        }
        let mut oid_registry = self.postgres_oid_registry.clone();
        oid_registry.allocate(PostgresOidObject::Table(table_id))?;
        for column in &definitions {
            oid_registry.allocate(PostgresOidObject::Column(table_id, column.id))?;
        }
        for index in indexes.values() {
            oid_registry.allocate(PostgresOidObject::Index(index.id))?;
        }
        let declared_types = definitions
            .iter()
            .filter_map(|column| column.declared_type.map(|type_id| (column.id, type_id)))
            .collect::<Vec<_>>();
        schema.tables.insert(
            table_name.clone(),
            TableDefinition {
                id: table_id,
                schema_id: schema.id,
                name: table_name,
                columns: definitions,
                indexes,
                constraints: BTreeMap::new(),
                triggers: BTreeMap::new(),
                statistics: TableStatistics::default(),
            },
        );
        for (column_id, type_id) in declared_types {
            self.dependencies.add(
                CatalogObjectRef::Column(table_id, column_id),
                CatalogObjectRef::Type(type_id),
            )?;
        }
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(table_id)
    }

    #[must_use]
    pub fn table(
        &self,
        schema_name: &Identifier,
        table_name: &Identifier,
    ) -> Option<&TableDefinition> {
        system_relation_by_name(schema_name, table_name)
            .and_then(|relation| system::system_table(relation.table_id))
            .or_else(|| self.database.schema(schema_name)?.table(table_name))
    }

    #[must_use]
    pub fn table_by_id(&self, table_id: TableId) -> Option<&TableDefinition> {
        system::system_table(table_id).or_else(|| {
            self.database
                .schemas()
                .flat_map(SchemaDefinition::tables)
                .find(|table| table.id == table_id)
        })
    }

    pub fn rename_table(&mut self, table_id: TableId, new_name: Identifier) -> Result<()> {
        self.ensure_writable_table_id(table_id)?;
        let (schema_id, old_name) = self
            .table_by_id(table_id)
            .map(|table| (table.schema_id, table.name.clone()))
            .ok_or_else(|| DbError::new("42P01", "table does not exist"))?;
        let schema = self.schema_by_id(schema_id).ok_or_else(|| {
            DbError::internal("table owner schema disappeared during table rename")
        })?;
        if schema.relation_name_exists(&new_name) {
            return Err(DbError::new(
                "42P07",
                format!("relation {new_name} already exists"),
            ));
        }
        let schema = self.schema_by_id_mut(schema_id)?;
        let mut table = schema
            .tables
            .remove(&old_name)
            .ok_or_else(|| DbError::internal("table namespace changed during rename"))?;
        table.name = new_name.clone();
        schema.tables.insert(new_name, table);
        Ok(())
    }

    pub fn drop_table(
        &mut self,
        table_id: TableId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        self.ensure_writable_table_id(table_id)?;
        if self.table_by_id(table_id).is_none() {
            return Err(DbError::new("42P01", "table does not exist"));
        }
        let root = CatalogObjectRef::Table(table_id);
        if behavior == DropBehavior::Restrict {
            let external = self
                .dependencies
                .dependents(root)
                .filter(|object| !self.object_is_owned_by_table(*object, table_id))
                .collect::<Vec<_>>();
            if !external.is_empty() {
                return Err(DbError::new(
                    "2BP01",
                    "cannot drop table because other objects depend on it",
                )
                .with_detail(format!("dependents: {external:?}"))
                .with_hint("Use DROP TABLE ... CASCADE to remove dependent objects."));
            }
        }
        self.drop_catalog_object(root, DropBehavior::Cascade)
    }

    pub fn rename_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        new_name: Identifier,
    ) -> Result<()> {
        self.ensure_writable_table_id(table_id)?;
        let table = self.table_by_id_mut(table_id)?;
        if table.column(&new_name).is_some() {
            return Err(DbError::new(
                "42701",
                format!("column {new_name} already exists"),
            ));
        }
        let index = table
            .column_index_by_id(column_id)
            .ok_or_else(|| DbError::new("42703", "column does not exist"))?;
        table.columns[index].name = new_name;
        Ok(())
    }

    pub fn add_column(&mut self, table_id: TableId, column: NewColumn) -> Result<ColumnId> {
        self.ensure_writable_table_id(table_id)?;
        if let Some(type_id) = column.declared_type
            && self.type_by_id(type_id).is_none()
        {
            return Err(DbError::new(
                "42704",
                format!("declared type {} does not exist", type_id.get()),
            ));
        }
        if self
            .table_by_id(table_id)
            .ok_or_else(|| DbError::new("42P01", "table does not exist"))?
            .column(&column.name)
            .is_some()
        {
            return Err(DbError::new(
                "42701",
                format!("column {} already exists", column.name),
            ));
        }
        let column_id = ColumnId::new(self.next_column_id);
        let oid_registry =
            self.postgres_oid_candidate([PostgresOidObject::Column(table_id, column_id)])?;
        let next_column_id = self
            .next_column_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog column ID space is exhausted"))?;
        let declared_type = column.declared_type;
        let mut dependencies = self.dependencies.clone();
        if let Some(type_id) = declared_type {
            dependencies.add(
                CatalogObjectRef::Column(table_id, column_id),
                CatalogObjectRef::Type(type_id),
            )?;
        }
        self.table_by_id_mut(table_id)?
            .columns
            .push(ColumnDefinition {
                id: column_id,
                name: column.name,
                data_type: column.data_type,
                declared_type: column.declared_type,
                nullable: column.nullable && !column.primary_key,
                primary_key: column.primary_key,
                unique: column.unique || column.primary_key,
                default: column.default,
            });
        self.next_column_id = next_column_id;
        self.dependencies = dependencies;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(column_id)
    }

    pub fn alter_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        data_type: Option<ScalarType>,
        nullable: Option<bool>,
        default: Option<Option<CatalogExpression>>,
        declared_type: Option<Option<TypeId>>,
    ) -> Result<()> {
        self.ensure_writable_table_id(table_id)?;
        let mut dependencies = self.dependencies.clone();
        if let Some(declared_type) = declared_type {
            let object = CatalogObjectRef::Column(table_id, column_id);
            dependencies.remove_references(object);
            if let Some(type_id) = declared_type {
                if self.type_by_id(type_id).is_none() {
                    return Err(DbError::new("42704", "declared column type does not exist"));
                }
                dependencies.add(object, CatalogObjectRef::Type(type_id))?;
            }
        }
        let table = self.table_by_id_mut(table_id)?;
        let index = table
            .column_index_by_id(column_id)
            .ok_or_else(|| DbError::new("42703", "column does not exist"))?;
        let column = &mut table.columns[index];
        if let Some(data_type) = data_type {
            column.data_type = data_type;
        }
        if let Some(nullable) = nullable {
            if nullable && column.primary_key {
                return Err(DbError::new(
                    "42P16",
                    "primary-key columns cannot be nullable",
                ));
            }
            column.nullable = nullable;
        }
        if let Some(default) = default {
            column.default = default;
        }
        if let Some(declared_type) = declared_type {
            column.declared_type = declared_type;
        }
        self.dependencies = dependencies;
        Ok(())
    }

    pub fn drop_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        self.ensure_writable_table_id(table_id)?;
        let table = self
            .table_by_id(table_id)
            .ok_or_else(|| DbError::new("42P01", "table does not exist"))?;
        if table.column_index_by_id(column_id).is_none() {
            return Err(DbError::new("42703", "column does not exist"));
        }
        if table.columns.len() == 1 {
            return Err(DbError::new(
                "42601",
                "cannot drop the only column of a table",
            ));
        }
        let root = CatalogObjectRef::Column(table_id, column_id);
        self.drop_catalog_object(root, behavior)
    }

    #[must_use]
    pub fn index_by_id(&self, index_id: IndexId) -> Option<&IndexDefinition> {
        self.database
            .schemas()
            .flat_map(SchemaDefinition::tables)
            .flat_map(TableDefinition::indexes)
            .find(|index| index.id == index_id)
    }

    pub fn create_index(&mut self, table_id: TableId, new_index: NewIndex) -> Result<IndexId> {
        self.ensure_writable_table_id(table_id)?;
        if new_index.key_columns.is_empty() {
            return Err(DbError::new(
                "42601",
                "an index must contain at least one key column",
            ));
        }
        if new_index.method != new_index.options.method() {
            return Err(DbError::new(
                "22023",
                "index method and options do not describe the same index kind",
            ));
        }
        if self
            .database
            .schemas()
            .flat_map(SchemaDefinition::tables)
            .any(|table| table.index(&new_index.name).is_some())
        {
            return Err(DbError::new(
                "42P07",
                format!("relation {} already exists", new_index.name),
            ));
        }

        let table = self
            .table_by_id(table_id)
            .ok_or_else(|| DbError::new("42P01", "index owner table does not exist"))?;
        let mut seen = BTreeMap::<ColumnId, ()>::new();
        let key_columns = new_index
            .key_columns
            .iter()
            .map(|name| {
                let column = table.column(name).ok_or_else(|| {
                    DbError::new("42703", format!("column {name} does not exist"))
                })?;
                if seen.insert(column.id, ()).is_some() {
                    return Err(DbError::new(
                        "42701",
                        format!("column {name} specified more than once"),
                    ));
                }
                Ok(column.id)
            })
            .collect::<Result<Vec<_>>>()?;
        let include_columns = new_index
            .include_columns
            .iter()
            .map(|name| {
                let column = table.column(name).ok_or_else(|| {
                    DbError::new("42703", format!("column {name} does not exist"))
                })?;
                if seen.insert(column.id, ()).is_some() {
                    return Err(DbError::new(
                        "42701",
                        format!("column {name} specified more than once"),
                    ));
                }
                Ok(column.id)
            })
            .collect::<Result<Vec<_>>>()?;

        match (&new_index.method, &new_index.options) {
            (IndexMethod::BTree, IndexOptions::BTree) => {
                for name in &new_index.key_columns {
                    let column = table
                        .column(name)
                        .ok_or_else(|| DbError::internal("validated B+Tree column disappeared"))?;
                    if !indexable_type(&column.data_type) {
                        return Err(DbError::new(
                            "42804",
                            format!("column {name} has no B+Tree ordering"),
                        ));
                    }
                }
            }
            (IndexMethod::FullText, IndexOptions::FullText { .. }) => {
                if new_index.unique || !new_index.include_columns.is_empty() {
                    return Err(DbError::new(
                        "0A000",
                        "full-text indexes do not support UNIQUE or INCLUDE",
                    ));
                }
                for name in &new_index.key_columns {
                    let column = table.column(name).ok_or_else(|| {
                        DbError::internal("validated full-text column disappeared")
                    })?;
                    if !text_search_type(&column.data_type) {
                        return Err(DbError::new(
                            "42804",
                            format!("full-text index column {name} must be character or text"),
                        ));
                    }
                }
            }
            (
                IndexMethod::Hnsw,
                IndexOptions::Hnsw {
                    dimensions,
                    m,
                    ef_construction,
                    ef_search,
                    ..
                },
            ) => {
                if new_index.unique
                    || !new_index.include_columns.is_empty()
                    || new_index.key_columns.len() != 1
                {
                    return Err(DbError::new(
                        "0A000",
                        "HNSW indexes require one VECTOR column and do not support UNIQUE or INCLUDE",
                    ));
                }
                if !(2..=64).contains(m)
                    || *ef_construction < *m
                    || *ef_construction > 4_096
                    || !(1..=4_096).contains(ef_search)
                {
                    return Err(DbError::new(
                        "22023",
                        "HNSW options require m 2..64, ef_construction m..4096, and ef_search 1..4096",
                    ));
                }
                let name = new_index
                    .key_columns
                    .first()
                    .ok_or_else(|| DbError::internal("validated HNSW key disappeared"))?;
                let column = table
                    .column(name)
                    .ok_or_else(|| DbError::internal("validated HNSW column disappeared"))?;
                match column.data_type {
                    ScalarType::Vector {
                        dimensions: Some(column_dimensions),
                    } if column_dimensions == *dimensions && *dimensions > 0 => {}
                    ScalarType::Vector { dimensions: None } => {
                        return Err(DbError::new(
                            "42804",
                            format!("HNSW index column {name} requires a fixed VECTOR dimension"),
                        ));
                    }
                    ScalarType::Vector {
                        dimensions: Some(column_dimensions),
                    } => {
                        return Err(DbError::new(
                            "22023",
                            format!(
                                "HNSW dimensions {dimensions} do not match column dimension {column_dimensions}"
                            ),
                        ));
                    }
                    _ => {
                        return Err(DbError::new(
                            "42804",
                            format!("HNSW index column {name} must be VECTOR"),
                        ));
                    }
                }
            }
            _ => {
                return Err(DbError::new(
                    "22023",
                    "index method and options do not describe the same index kind",
                ));
            }
        }

        let id = IndexId::new(self.next_index_id);
        let oid_registry = self.postgres_oid_candidate([PostgresOidObject::Index(id)])?;
        let next_index_id = self
            .next_index_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog index ID space is exhausted"))?;
        let definition = IndexDefinition {
            id,
            table_id,
            name: new_index.name.clone(),
            key_columns,
            include_columns,
            unique: new_index.unique,
            primary: false,
            method: new_index.method,
            options: new_index.options,
        };
        self.table_by_id_mut(table_id)?
            .indexes
            .insert(new_index.name, definition);
        self.next_index_id = next_index_id;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(id)
    }

    pub fn rename_index(&mut self, index_id: IndexId, new_name: Identifier) -> Result<()> {
        let (table_id, old_name, schema_id) = self
            .index_by_id(index_id)
            .and_then(|index| {
                self.table_by_id(index.table_id)
                    .map(|table| (index.table_id, index.name.clone(), table.schema_id))
            })
            .ok_or_else(|| DbError::new("42704", "index does not exist"))?;
        if self
            .schema_by_id(schema_id)
            .is_some_and(|schema| schema.relation_name_exists(&new_name))
        {
            return Err(DbError::new(
                "42P07",
                format!("relation {new_name} already exists"),
            ));
        }
        let table = self.table_by_id_mut(table_id)?;
        let mut index = table
            .indexes
            .remove(&old_name)
            .ok_or_else(|| DbError::internal("index namespace changed during rename"))?;
        index.name = new_name.clone();
        table.indexes.insert(new_name, index);
        Ok(())
    }

    pub fn drop_index(
        &mut self,
        index_id: IndexId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        if self.index_by_id(index_id).is_none() {
            return Err(DbError::new("42704", "index does not exist"));
        }
        self.drop_catalog_object(CatalogObjectRef::Index(index_id), behavior)
    }

    pub fn create_constraint(
        &mut self,
        table_id: TableId,
        new_constraint: NewConstraint,
    ) -> Result<ConstraintId> {
        self.ensure_writable_table_id(table_id)?;
        let table = self
            .table_by_id(table_id)
            .ok_or_else(|| DbError::new("42P01", "constraint owner table does not exist"))?;
        if table.constraint(&new_constraint.name).is_some() {
            return Err(DbError::new(
                "42710",
                format!("constraint {} already exists", new_constraint.name),
            ));
        }

        let kind = match &new_constraint.kind {
            NewConstraintKind::PrimaryKey { columns } => {
                if table
                    .constraints()
                    .any(|constraint| matches!(constraint.kind, ConstraintKind::PrimaryKey { .. }))
                    || table.columns().iter().any(|column| column.primary_key)
                {
                    return Err(DbError::new(
                        "42P16",
                        "multiple primary keys for a table are not allowed",
                    ));
                }
                ConstraintKind::PrimaryKey {
                    columns: resolve_constraint_columns(table, columns)?,
                }
            }
            NewConstraintKind::Unique { columns } => ConstraintKind::Unique {
                columns: resolve_constraint_columns(table, columns)?,
            },
            NewConstraintKind::Check { expression } => ConstraintKind::Check {
                expression: expression.clone(),
            },
            NewConstraintKind::ForeignKey {
                columns,
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
            } => {
                let columns = resolve_constraint_columns(table, columns)?;
                let referenced = self.table_by_id(*referenced_table).ok_or_else(|| {
                    DbError::new("42P01", "foreign-key referenced table does not exist")
                })?;
                if columns.len() != referenced_columns.len() || columns.is_empty() {
                    return Err(DbError::new(
                        "42830",
                        "foreign key must reference the same non-zero number of columns",
                    ));
                }
                for column_id in referenced_columns {
                    if referenced.column_index_by_id(*column_id).is_none() {
                        return Err(DbError::new(
                            "42703",
                            "foreign key references a missing column",
                        ));
                    }
                }
                let referenced_is_unique = referenced.indexes().any(|index| {
                    index.unique && index.key_columns.as_slice() == referenced_columns.as_slice()
                });
                if !referenced_is_unique {
                    return Err(DbError::new(
                        "42830",
                        "there is no unique constraint matching the referenced columns",
                    ));
                }
                ConstraintKind::ForeignKey {
                    columns,
                    referenced_table: *referenced_table,
                    referenced_columns: referenced_columns.clone(),
                    on_delete: *on_delete,
                    on_update: *on_update,
                }
            }
        };

        let id = ConstraintId::new(self.next_constraint_id);
        let object = CatalogObjectRef::Constraint(id);
        let mut dependencies = self.dependencies.clone();
        dependencies.add(object, CatalogObjectRef::Table(table_id))?;
        for column_id in constraint_columns(&kind) {
            dependencies.add(object, CatalogObjectRef::Column(table_id, column_id))?;
        }
        if let ConstraintKind::ForeignKey {
            referenced_table,
            referenced_columns,
            ..
        } = &kind
        {
            dependencies.add(object, CatalogObjectRef::Table(*referenced_table))?;
            for column_id in referenced_columns {
                dependencies.add(
                    object,
                    CatalogObjectRef::Column(*referenced_table, *column_id),
                )?;
            }
        }

        let creates_index = matches!(
            kind,
            ConstraintKind::PrimaryKey { .. } | ConstraintKind::Unique { .. }
        );
        if creates_index
            && self
                .database
                .schemas()
                .flat_map(SchemaDefinition::tables)
                .any(|candidate| candidate.index(&new_constraint.name).is_some())
        {
            return Err(DbError::new(
                "42P07",
                format!("relation {} already exists", new_constraint.name),
            ));
        }

        let index_id = creates_index.then(|| IndexId::new(self.next_index_id));
        let mut oid_objects = vec![PostgresOidObject::Constraint(id)];
        oid_objects.extend(index_id.map(PostgresOidObject::Index));
        let oid_registry = self.postgres_oid_candidate(oid_objects)?;
        let next_constraint_id = self
            .next_constraint_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog constraint ID space is exhausted"))?;
        let next_index_id = if creates_index {
            Some(
                self.next_index_id
                    .checked_add(1)
                    .ok_or_else(|| DbError::new("54000", "catalog index ID space is exhausted"))?,
            )
        } else {
            None
        };
        if creates_index {
            let index_id = index_id.ok_or_else(|| {
                DbError::internal("constraint index allocation lost its planned identity")
            })?;
            let key_columns = constraint_columns(&kind).collect::<Vec<_>>();
            self.table_by_id_mut(table_id)?.indexes.insert(
                new_constraint.name.clone(),
                IndexDefinition {
                    id: index_id,
                    table_id,
                    name: new_constraint.name.clone(),
                    key_columns: key_columns.clone(),
                    include_columns: Vec::new(),
                    unique: true,
                    primary: matches!(kind, ConstraintKind::PrimaryKey { .. }),
                    method: IndexMethod::BTree,
                    options: IndexOptions::BTree,
                },
            );
            dependencies.add(object, CatalogObjectRef::Index(index_id))?;
            if matches!(kind, ConstraintKind::PrimaryKey { .. }) {
                let table = self.table_by_id_mut(table_id)?;
                for column_id in key_columns {
                    let index = table.column_index_by_id(column_id).ok_or_else(|| {
                        DbError::internal("primary-key column disappeared during creation")
                    })?;
                    table.columns[index].nullable = false;
                    table.columns[index].primary_key = true;
                    if table.columns.len() == 1 {
                        table.columns[index].unique = true;
                    }
                }
            }
        }
        self.table_by_id_mut(table_id)?.constraints.insert(
            new_constraint.name.clone(),
            ConstraintDefinition {
                id,
                table_id,
                name: new_constraint.name,
                kind,
            },
        );
        self.next_constraint_id = next_constraint_id;
        if let Some(next_index_id) = next_index_id {
            self.next_index_id = next_index_id;
        }
        self.dependencies = dependencies;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(id)
    }

    pub fn create_sequence(
        &mut self,
        schema_name: &Identifier,
        sequence: NewSequence,
    ) -> Result<SequenceId> {
        ensure_writable_schema_name(schema_name)?;
        if let Some((table_id, _)) = sequence.owner {
            self.ensure_writable_table_id(table_id)?;
        }
        let schema_id = {
            let schema = self.schema(schema_name).ok_or_else(|| {
                DbError::new("3F000", format!("schema {schema_name} does not exist"))
            })?;
            if schema.relation_name_exists(&sequence.name) {
                return Err(DbError::new(
                    "42P07",
                    format!("relation {} already exists", sequence.name),
                ));
            }
            schema.id
        };
        let (type_min, type_max) = sequence_type_bounds(&sequence.data_type)?;
        if sequence.increment == 0 {
            return Err(DbError::new("22023", "sequence increment must not be zero"));
        }
        let min_value =
            sequence
                .min_value
                .unwrap_or(if sequence.increment > 0 { 1 } else { type_min });
        let max_value =
            sequence
                .max_value
                .unwrap_or(if sequence.increment > 0 { type_max } else { -1 });
        if min_value >= max_value {
            return Err(DbError::new(
                "22023",
                "sequence minimum must be less than its maximum",
            ));
        }
        let start_value = sequence.start_value.unwrap_or(if sequence.increment > 0 {
            min_value
        } else {
            max_value
        });
        if !(min_value..=max_value).contains(&start_value) {
            return Err(DbError::new(
                "22023",
                "sequence start value is outside its bounds",
            ));
        }
        if let Some((table_id, column_id)) = sequence.owner {
            let table = self
                .table_by_id(table_id)
                .ok_or_else(|| DbError::new("42P01", "sequence owner table does not exist"))?;
            if table.column_index_by_id(column_id).is_none() {
                return Err(DbError::new(
                    "42703",
                    "sequence owner column does not exist",
                ));
            }
        }

        let id = SequenceId::new(self.next_sequence_id);
        let oid_registry = self.postgres_oid_candidate([PostgresOidObject::Sequence(id)])?;
        let next_sequence_id = self
            .next_sequence_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog sequence ID space is exhausted"))?;
        let object = CatalogObjectRef::Sequence(id);
        let mut dependencies = self.dependencies.clone();
        if let Some((table_id, column_id)) = sequence.owner {
            dependencies.add(object, CatalogObjectRef::Table(table_id))?;
            dependencies.add(object, CatalogObjectRef::Column(table_id, column_id))?;
        }
        self.schema_by_id_mut(schema_id)?.sequences.insert(
            sequence.name.clone(),
            SequenceDefinition {
                id,
                schema_id,
                name: sequence.name,
                data_type: sequence.data_type,
                increment: sequence.increment,
                min_value,
                max_value,
                start_value,
                last_value: start_value,
                is_called: false,
                cycle: sequence.cycle,
                owner: sequence.owner,
            },
        );
        self.next_sequence_id = next_sequence_id;
        self.dependencies = dependencies;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(id)
    }

    pub fn create_view(&mut self, schema_name: &Identifier, view: NewView) -> Result<ViewId> {
        ensure_writable_schema_name(schema_name)?;
        if let Some(table_id) = view.materialized_table_id {
            self.ensure_writable_table_id(table_id)?;
        }
        let NewView {
            name,
            kind,
            query,
            output,
            materialized_table_id,
            populated,
            references,
        } = view;
        let schema_id = {
            let schema = self.schema(schema_name).ok_or_else(|| {
                DbError::new("3F000", format!("schema {schema_name} does not exist"))
            })?;
            if schema.relation_name_exists(&name) {
                return Err(DbError::new(
                    "42P07",
                    format!("relation {name} already exists"),
                ));
            }
            schema.id
        };
        if (kind == ViewKind::Materialized) != materialized_table_id.is_some() {
            return Err(DbError::new(
                "22023",
                "materialized views require exactly one backing table",
            ));
        }
        let id = ViewId::new(self.next_view_id);
        let oid_registry = self.postgres_oid_candidate([PostgresOidObject::View(id)])?;
        let next_view_id = self
            .next_view_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog view ID space is exhausted"))?;
        let object = CatalogObjectRef::View(id);
        let mut dependencies = self.dependencies.clone();
        for referenced in references {
            dependencies.add(object, referenced)?;
        }
        if let Some(table_id) = materialized_table_id {
            dependencies.add(object, CatalogObjectRef::Table(table_id))?;
        }
        self.schema_by_id_mut(schema_id)?.views.insert(
            name.clone(),
            ViewDefinition {
                id,
                schema_id,
                name,
                kind,
                query,
                output,
                materialized_table_id,
                populated,
                triggers: BTreeMap::new(),
            },
        );
        self.next_view_id = next_view_id;
        self.dependencies = dependencies;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(id)
    }

    pub fn create_or_replace_routine(
        &mut self,
        schema_name: &Identifier,
        routine: NewRoutine,
    ) -> Result<RoutineId> {
        ensure_writable_schema_name(schema_name)?;
        let NewRoutine {
            name,
            kind,
            arguments,
            return_type,
            return_declared_type,
            returns_set,
            language,
            body,
            replace,
            references,
        } = routine;
        validate_routine_arguments(kind, &arguments, return_type.as_ref(), returns_set)?;
        let schema_id = self
            .schema(schema_name)
            .ok_or_else(|| DbError::new("3F000", format!("schema {schema_name} does not exist")))?
            .id;
        let existing_id = self
            .routine_by_signature(schema_name, &name, kind, &arguments)
            .map(|routine| routine.id);
        if existing_id.is_some() && !replace {
            return Err(DbError::new(
                "42723",
                format!("routine {name} with this signature already exists"),
            ));
        }

        let (id, old_object) = existing_id
            .map(|routine_id| (routine_id, Some(CatalogObjectRef::Routine(routine_id))))
            .unwrap_or_else(|| (RoutineId::new(self.next_routine_id), None));
        let oid_registry = if existing_id.is_none() {
            Some(self.postgres_oid_candidate([PostgresOidObject::Routine(id)])?)
        } else {
            None
        };
        let next_routine_id =
            if existing_id.is_none() {
                Some(self.next_routine_id.checked_add(1).ok_or_else(|| {
                    DbError::new("54000", "catalog routine ID space is exhausted")
                })?)
            } else {
                None
            };
        let object = CatalogObjectRef::Routine(id);
        let mut dependencies = self.dependencies.clone();
        if let Some(old_object) = old_object {
            dependencies.remove(old_object);
        }
        for referenced in references {
            dependencies.add(object, referenced)?;
        }
        let routines = self
            .schema_by_id_mut(schema_id)?
            .routines
            .entry(name.clone())
            .or_default();
        routines.retain(|routine| {
            !(routine.kind == kind
                && routine_input_signature_matches(&routine.arguments, &arguments))
        });
        routines.push(RoutineDefinition {
            id,
            schema_id,
            name,
            kind,
            arguments,
            return_type,
            return_declared_type,
            returns_set,
            language,
            body,
        });
        routines.sort_by_key(|routine| routine.id);
        if let Some(next_routine_id) = next_routine_id {
            self.next_routine_id = next_routine_id;
        }
        self.dependencies = dependencies;
        if let Some(oid_registry) = oid_registry {
            self.publish_postgres_oid_candidate(oid_registry)?;
        } else {
            self.validate_postgres_oid_registry()?;
        }
        Ok(id)
    }

    pub fn create_trigger(
        &mut self,
        table_id: TableId,
        name: Identifier,
        timing: TriggerTiming,
        events: BTreeSet<TriggerEvent>,
        routine_id: RoutineId,
    ) -> Result<TriggerId> {
        self.create_trigger_on_target_with_level(
            TriggerTarget::Table(table_id),
            name,
            timing,
            TriggerLevel::Row,
            events,
            routine_id,
        )
    }

    pub fn create_trigger_with_level(
        &mut self,
        table_id: TableId,
        name: Identifier,
        timing: TriggerTiming,
        level: TriggerLevel,
        events: BTreeSet<TriggerEvent>,
        routine_id: RoutineId,
    ) -> Result<TriggerId> {
        self.create_trigger_on_target_with_level(
            TriggerTarget::Table(table_id),
            name,
            timing,
            level,
            events,
            routine_id,
        )
    }

    pub fn create_trigger_on_target_with_level(
        &mut self,
        target: TriggerTarget,
        name: Identifier,
        timing: TriggerTiming,
        level: TriggerLevel,
        events: BTreeSet<TriggerEvent>,
        routine_id: RoutineId,
    ) -> Result<TriggerId> {
        let activation_is_valid = match target {
            TriggerTarget::Table(table_id) => {
                self.ensure_writable_table_id(table_id)?;
                if self.table_by_id(table_id).is_none() {
                    return Err(DbError::new("42P01", "trigger owner table does not exist"));
                }
                matches!(
                    (timing, level),
                    (
                        TriggerTiming::Before | TriggerTiming::After,
                        TriggerLevel::Row
                    ) | (
                        TriggerTiming::BeforeStatement | TriggerTiming::AfterStatement,
                        TriggerLevel::Statement
                    )
                )
            }
            TriggerTarget::View(view_id) => {
                let view = self
                    .view_by_id(view_id)
                    .ok_or_else(|| DbError::new("42P01", "trigger owner view does not exist"))?;
                if view.kind != ViewKind::Regular {
                    return Err(DbError::new(
                        "42809",
                        "triggers cannot target materialized views",
                    ));
                }
                timing == TriggerTiming::InsteadOf && level == TriggerLevel::Row
            }
        };
        if !activation_is_valid {
            return Err(DbError::new(
                "0A000",
                "trigger timing and level are not supported for this relation kind",
            ));
        }
        if events.is_empty() {
            return Err(DbError::new(
                "42601",
                "a trigger must contain at least one event",
            ));
        }
        let duplicate = match target {
            TriggerTarget::Table(table_id) => self
                .table_by_id(table_id)
                .is_some_and(|table| table.trigger(&name).is_some()),
            TriggerTarget::View(view_id) => self
                .view_by_id(view_id)
                .is_some_and(|view| view.trigger(&name).is_some()),
        };
        if duplicate {
            return Err(DbError::new(
                "42710",
                format!("trigger {name} already exists"),
            ));
        }
        if self.routine_by_id(routine_id).is_none() {
            return Err(DbError::new("42883", "trigger routine does not exist"));
        }
        let id = TriggerId::new(self.next_trigger_id);
        let oid_registry = self.postgres_oid_candidate([PostgresOidObject::Trigger(id)])?;
        let next_trigger_id = self
            .next_trigger_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog trigger ID space is exhausted"))?;
        let object = CatalogObjectRef::Trigger(id);
        let mut dependencies = self.dependencies.clone();
        dependencies.add(object, target.object_ref())?;
        dependencies.add(object, CatalogObjectRef::Routine(routine_id))?;
        let definition = TriggerDefinition {
            id,
            target,
            name: name.clone(),
            timing,
            level,
            events,
            routine_id,
            enabled: true,
        };
        match target {
            TriggerTarget::Table(table_id) => {
                self.table_by_id_mut(table_id)?
                    .triggers
                    .insert(name, definition);
            }
            TriggerTarget::View(view_id) => {
                self.view_by_id_mut(view_id)?
                    .triggers
                    .insert(name, definition);
            }
        }
        self.next_trigger_id = next_trigger_id;
        self.dependencies = dependencies;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(id)
    }

    pub fn rename_sequence(&mut self, sequence_id: SequenceId, new_name: Identifier) -> Result<()> {
        let (schema_id, old_name) = self
            .sequence_by_id(sequence_id)
            .map(|sequence| (sequence.schema_id, sequence.name.clone()))
            .ok_or_else(|| DbError::new("42P01", "sequence does not exist"))?;
        if self
            .schema_by_id(schema_id)
            .is_some_and(|schema| schema.relation_name_exists(&new_name))
        {
            return Err(DbError::new(
                "42P07",
                format!("relation {new_name} already exists"),
            ));
        }
        let schema = self.schema_by_id_mut(schema_id)?;
        let mut sequence = schema
            .sequences
            .remove(&old_name)
            .ok_or_else(|| DbError::internal("sequence namespace changed during rename"))?;
        sequence.name = new_name.clone();
        schema.sequences.insert(new_name, sequence);
        Ok(())
    }

    pub fn alter_sequence(
        &mut self,
        sequence_id: SequenceId,
        alteration: SequenceAlteration,
    ) -> Result<()> {
        if let Some(Some((table_id, _))) = alteration.owner {
            self.ensure_writable_table_id(table_id)?;
        }
        let SequenceAlteration {
            increment,
            min_value,
            max_value,
            restart,
            cycle,
            owner,
        } = alteration;
        let current = self
            .sequence_by_id(sequence_id)
            .cloned()
            .ok_or_else(|| DbError::new("42P01", "sequence does not exist"))?;
        let next_increment = increment.unwrap_or(current.increment);
        if next_increment == 0 {
            return Err(DbError::new("22023", "sequence increment must not be zero"));
        }
        let next_min = min_value.unwrap_or(current.min_value);
        let next_max = max_value.unwrap_or(current.max_value);
        if next_min >= next_max {
            return Err(DbError::new(
                "22023",
                "sequence minimum must be less than its maximum",
            ));
        }
        let next_value = restart.unwrap_or(current.last_value);
        if !(next_min..=next_max).contains(&next_value) {
            return Err(DbError::new(
                "2200H",
                "sequence restart value is outside sequence bounds",
            ));
        }
        let next_owner = owner.unwrap_or(current.owner);
        if let Some((table_id, column_id)) = next_owner {
            let table = self
                .table_by_id(table_id)
                .ok_or_else(|| DbError::new("42P01", "sequence owner table does not exist"))?;
            if table.column_index_by_id(column_id).is_none() {
                return Err(DbError::new(
                    "42703",
                    "sequence owner column does not exist",
                ));
            }
        }
        let object = CatalogObjectRef::Sequence(sequence_id);
        let mut dependencies = self.dependencies.clone();
        dependencies.remove(object);
        if let Some((table_id, column_id)) = next_owner {
            dependencies.add(object, CatalogObjectRef::Table(table_id))?;
            dependencies.add(object, CatalogObjectRef::Column(table_id, column_id))?;
        }
        let sequence = self.sequence_by_id_mut(sequence_id)?;
        sequence.increment = next_increment;
        sequence.min_value = next_min;
        sequence.max_value = next_max;
        sequence.last_value = next_value;
        if restart.is_some() {
            sequence.is_called = false;
        }
        sequence.cycle = cycle.unwrap_or(current.cycle);
        sequence.owner = next_owner;
        self.dependencies = dependencies;
        Ok(())
    }

    pub fn drop_sequence(
        &mut self,
        sequence_id: SequenceId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        if self.sequence_by_id(sequence_id).is_none() {
            return Err(DbError::new("42P01", "sequence does not exist"));
        }
        self.drop_catalog_object(CatalogObjectRef::Sequence(sequence_id), behavior)
    }

    pub fn replace_view(
        &mut self,
        view_id: ViewId,
        query: String,
        output: Schema,
        populated: bool,
        references: impl IntoIterator<Item = CatalogObjectRef>,
    ) -> Result<()> {
        let current = self
            .view_by_id(view_id)
            .cloned()
            .ok_or_else(|| DbError::new("42P01", "view does not exist"))?;
        if current.output.fields.len() != output.fields.len()
            || current
                .output
                .fields
                .iter()
                .zip(&output.fields)
                .any(|(left, right)| left.data_type != right.data_type)
        {
            return Err(DbError::new(
                "42P16",
                "cannot change the data type or count of view columns",
            ));
        }
        let object = CatalogObjectRef::View(view_id);
        let mut dependencies = self.dependencies.clone();
        dependencies.remove_references(object);
        for referenced in references {
            dependencies.add(object, referenced)?;
        }
        if let Some(table_id) = current.materialized_table_id {
            dependencies.add(object, CatalogObjectRef::Table(table_id))?;
        }
        let view = self.view_by_id_mut(view_id)?;
        view.query = query;
        view.output = output;
        view.populated = populated;
        self.dependencies = dependencies;
        Ok(())
    }

    pub fn rename_view(&mut self, view_id: ViewId, new_name: Identifier) -> Result<()> {
        let (schema_id, old_name) = self
            .view_by_id(view_id)
            .map(|view| (view.schema_id, view.name.clone()))
            .ok_or_else(|| DbError::new("42P01", "view does not exist"))?;
        if self
            .schema_by_id(schema_id)
            .is_some_and(|schema| schema.relation_name_exists(&new_name))
        {
            return Err(DbError::new(
                "42P07",
                format!("relation {new_name} already exists"),
            ));
        }
        let schema = self.schema_by_id_mut(schema_id)?;
        let mut view = schema
            .views
            .remove(&old_name)
            .ok_or_else(|| DbError::internal("view namespace changed during rename"))?;
        view.name = new_name.clone();
        schema.views.insert(new_name, view);
        Ok(())
    }

    pub fn set_materialized_view_populated(
        &mut self,
        view_id: ViewId,
        populated: bool,
    ) -> Result<()> {
        let view = self.view_by_id_mut(view_id)?;
        if view.kind != ViewKind::Materialized {
            return Err(DbError::new(
                "42809",
                "only materialized views can change populated state",
            ));
        }
        view.populated = populated;
        Ok(())
    }

    pub fn drop_view(
        &mut self,
        view_id: ViewId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        if self.view_by_id(view_id).is_none() {
            return Err(DbError::new("42P01", "view does not exist"));
        }
        let root = CatalogObjectRef::View(view_id);
        if behavior == DropBehavior::Restrict {
            let external = self
                .dependencies
                .dependents(root)
                .filter(|object| !self.object_is_owned_by_view(*object, view_id))
                .collect::<Vec<_>>();
            if !external.is_empty() {
                return Err(DbError::new(
                    "2BP01",
                    "cannot drop view because other objects depend on it",
                )
                .with_detail(format!("dependents: {external:?}"))
                .with_hint("Use DROP VIEW ... CASCADE to remove dependent objects."));
            }
        }
        self.drop_catalog_object(root, DropBehavior::Cascade)
    }

    pub fn drop_routine(
        &mut self,
        routine_id: RoutineId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        if self.routine_by_id(routine_id).is_none() {
            return Err(DbError::new("42883", "routine does not exist"));
        }
        self.drop_catalog_object(CatalogObjectRef::Routine(routine_id), behavior)
    }

    pub fn set_trigger_enabled(&mut self, trigger_id: TriggerId, enabled: bool) -> Result<()> {
        let trigger = self.trigger_by_id_mut(trigger_id)?;
        trigger.enabled = enabled;
        Ok(())
    }

    pub fn drop_trigger(
        &mut self,
        trigger_id: TriggerId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        if self.trigger_by_id(trigger_id).is_none() {
            return Err(DbError::new("42704", "trigger does not exist"));
        }
        self.drop_catalog_object(CatalogObjectRef::Trigger(trigger_id), behavior)
    }

    pub fn drop_constraint(
        &mut self,
        constraint_id: ConstraintId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        if self.constraint_by_id(constraint_id).is_none() {
            return Err(DbError::new("42704", "constraint does not exist"));
        }
        self.drop_catalog_object(CatalogObjectRef::Constraint(constraint_id), behavior)
    }

    #[must_use]
    pub fn constraint_by_id(&self, constraint_id: ConstraintId) -> Option<&ConstraintDefinition> {
        self.database
            .schemas()
            .flat_map(SchemaDefinition::tables)
            .flat_map(TableDefinition::constraints)
            .find(|constraint| constraint.id == constraint_id)
    }

    #[must_use]
    pub fn sequence_by_id(&self, sequence_id: SequenceId) -> Option<&SequenceDefinition> {
        self.database
            .schemas()
            .flat_map(SchemaDefinition::sequences)
            .find(|sequence| sequence.id == sequence_id)
    }

    pub fn sequence_by_id_mut(
        &mut self,
        sequence_id: SequenceId,
    ) -> Result<&mut SequenceDefinition> {
        self.database
            .schemas
            .values_mut()
            .flat_map(|schema| schema.sequences.values_mut())
            .find(|sequence| sequence.id == sequence_id)
            .ok_or_else(|| DbError::new("42P01", "sequence does not exist"))
    }

    #[must_use]
    pub fn view_by_id(&self, view_id: ViewId) -> Option<&ViewDefinition> {
        self.database
            .schemas()
            .flat_map(SchemaDefinition::views)
            .find(|view| view.id == view_id)
    }

    #[must_use]
    pub fn routine_by_id(&self, routine_id: RoutineId) -> Option<&RoutineDefinition> {
        self.database
            .schemas()
            .flat_map(SchemaDefinition::routines)
            .find(|routine| routine.id == routine_id)
    }

    #[must_use]
    pub fn trigger_by_id(&self, trigger_id: TriggerId) -> Option<&TriggerDefinition> {
        self.database
            .schemas()
            .flat_map(|schema| {
                schema
                    .tables()
                    .flat_map(TableDefinition::triggers)
                    .chain(schema.views().flat_map(ViewDefinition::triggers))
            })
            .find(|trigger| trigger.id == trigger_id)
    }

    pub fn next_sequence_value(&mut self, sequence_id: SequenceId) -> Result<i64> {
        let sequence = self.sequence_by_id_mut(sequence_id)?;
        if !sequence.is_called {
            sequence.is_called = true;
            return Ok(sequence.last_value);
        }
        let next = sequence
            .last_value
            .checked_add(sequence.increment)
            .ok_or_else(|| DbError::new("2200H", "sequence generator limit exceeded"))?;
        if next < sequence.min_value || next > sequence.max_value {
            if !sequence.cycle {
                return Err(DbError::new("2200H", "sequence generator limit exceeded"));
            }
            sequence.last_value = if sequence.increment > 0 {
                sequence.min_value
            } else {
                sequence.max_value
            };
        } else {
            sequence.last_value = next;
        }
        Ok(sequence.last_value)
    }

    pub fn set_sequence_value(
        &mut self,
        sequence_id: SequenceId,
        value: i64,
        is_called: bool,
    ) -> Result<()> {
        let sequence = self.sequence_by_id_mut(sequence_id)?;
        if !(sequence.min_value..=sequence.max_value).contains(&value) {
            return Err(DbError::new(
                "2200H",
                "setval value is outside sequence bounds",
            ));
        }
        sequence.last_value = value;
        sequence.is_called = is_called;
        Ok(())
    }

    pub fn set_table_statistics(
        &mut self,
        table_id: TableId,
        statistics: TableStatistics,
    ) -> Result<()> {
        self.ensure_writable_table_id(table_id)?;
        let table = self.table_by_id_mut(table_id)?;
        if statistics
            .columns
            .keys()
            .any(|column_id| table.column_index_by_id(*column_id).is_none())
        {
            return Err(DbError::internal(
                "statistics reference a column outside their owner table",
            ));
        }
        table.statistics = statistics;
        Ok(())
    }

    pub fn table_by_id_mut(&mut self, table_id: TableId) -> Result<&mut TableDefinition> {
        self.ensure_writable_table_id(table_id)?;
        self.database
            .schemas
            .values_mut()
            .flat_map(|schema| schema.tables.values_mut())
            .find(|table| table.id == table_id)
            .ok_or_else(|| DbError::new("42P01", "table does not exist"))
    }

    fn schema_by_id_mut(&mut self, schema_id: SchemaId) -> Result<&mut SchemaDefinition> {
        self.ensure_writable_schema_id(schema_id)?;
        self.database
            .schemas
            .values_mut()
            .find(|schema| schema.id == schema_id)
            .ok_or_else(|| DbError::new("3F000", "schema does not exist"))
    }

    fn view_by_id_mut(&mut self, view_id: ViewId) -> Result<&mut ViewDefinition> {
        self.database
            .schemas
            .values_mut()
            .flat_map(|schema| schema.views.values_mut())
            .find(|view| view.id == view_id)
            .ok_or_else(|| DbError::new("42P01", "view does not exist"))
    }

    fn trigger_by_id_mut(&mut self, trigger_id: TriggerId) -> Result<&mut TriggerDefinition> {
        for schema in self.database.schemas.values_mut() {
            if let Some(trigger) = schema
                .tables
                .values_mut()
                .flat_map(|table| table.triggers.values_mut())
                .find(|trigger| trigger.id == trigger_id)
            {
                return Ok(trigger);
            }
            if let Some(trigger) = schema
                .views
                .values_mut()
                .flat_map(|view| view.triggers.values_mut())
                .find(|trigger| trigger.id == trigger_id)
            {
                return Ok(trigger);
            }
        }
        Err(DbError::new("42704", "trigger does not exist"))
    }

    fn object_is_owned_by_table(&self, object: CatalogObjectRef, table_id: TableId) -> bool {
        match object {
            CatalogObjectRef::Column(owner, _) => owner == table_id,
            CatalogObjectRef::Index(index_id) => self
                .index_by_id(index_id)
                .is_some_and(|index| index.table_id == table_id),
            CatalogObjectRef::Constraint(constraint_id) => self
                .constraint_by_id(constraint_id)
                .is_some_and(|constraint| constraint.table_id == table_id),
            CatalogObjectRef::Trigger(trigger_id) => self
                .trigger_by_id(trigger_id)
                .is_some_and(|trigger| trigger.target == TriggerTarget::Table(table_id)),
            _ => false,
        }
    }

    fn object_is_owned_by_view(&self, object: CatalogObjectRef, view_id: ViewId) -> bool {
        matches!(object, CatalogObjectRef::Trigger(trigger_id) if self
            .trigger_by_id(trigger_id)
            .is_some_and(|trigger| trigger.target == TriggerTarget::View(view_id)))
    }

    fn drop_catalog_object(
        &mut self,
        root: CatalogObjectRef,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        match root {
            CatalogObjectRef::Schema(schema_id) => self.ensure_writable_schema_id(schema_id)?,
            CatalogObjectRef::Table(table_id) | CatalogObjectRef::Column(table_id, _) => {
                self.ensure_writable_table_id(table_id)?;
            }
            _ => {}
        }
        let order = self.dependencies.drop_order(root, behavior)?;
        for object in &order {
            self.remove_catalog_object(*object)?;
        }
        self.validate_postgres_oid_registry()?;
        Ok(order)
    }

    fn remove_catalog_object(&mut self, object: CatalogObjectRef) -> Result<()> {
        match object {
            CatalogObjectRef::Schema(schema_id) => {
                self.database
                    .schemas
                    .retain(|_, schema| schema.id != schema_id);
            }
            CatalogObjectRef::Table(table_id) => {
                let owned = self
                    .table_by_id(table_id)
                    .map(|table| {
                        table
                            .columns()
                            .iter()
                            .map(|column| CatalogObjectRef::Column(table_id, column.id))
                            .chain(
                                table
                                    .indexes()
                                    .map(|index| CatalogObjectRef::Index(index.id)),
                            )
                            .chain(
                                table
                                    .constraints()
                                    .map(|constraint| CatalogObjectRef::Constraint(constraint.id)),
                            )
                            .chain(
                                table
                                    .triggers()
                                    .map(|trigger| CatalogObjectRef::Trigger(trigger.id)),
                            )
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                for schema in self.database.schemas.values_mut() {
                    schema.tables.retain(|_, table| table.id != table_id);
                }
                for owned in owned {
                    self.dependencies.remove(owned);
                    self.ownership.remove(owned);
                    self.postgres_oid_registry.remove(owned.into());
                }
            }
            CatalogObjectRef::Column(table_id, column_id) => {
                let (removed_indexes, removed_constraints) = {
                    let table = self.table_by_id(table_id).ok_or_else(|| {
                        DbError::new("42P01", "column owner table does not exist")
                    })?;
                    (
                        table
                            .indexes()
                            .filter(|index| {
                                index.key_columns.contains(&column_id)
                                    || index.include_columns.contains(&column_id)
                            })
                            .map(|index| index.id)
                            .collect::<Vec<_>>(),
                        table
                            .constraints()
                            .filter(|constraint| {
                                constraint_columns(&constraint.kind)
                                    .any(|candidate| candidate == column_id)
                            })
                            .map(|constraint| constraint.id)
                            .collect::<Vec<_>>(),
                    )
                };
                let table = self.table_by_id_mut(table_id)?;
                table.columns.retain(|column| column.id != column_id);
                table.indexes.retain(|_, index| {
                    !index.key_columns.contains(&column_id)
                        && !index.include_columns.contains(&column_id)
                });
                table.constraints.retain(|_, constraint| {
                    !constraint_columns(&constraint.kind).any(|candidate| candidate == column_id)
                });
                for index_id in removed_indexes {
                    self.postgres_oid_registry
                        .remove(PostgresOidObject::Index(index_id));
                }
                for constraint_id in removed_constraints {
                    self.postgres_oid_registry
                        .remove(PostgresOidObject::Constraint(constraint_id));
                }
            }
            CatalogObjectRef::Index(index_id) => {
                for schema in self.database.schemas.values_mut() {
                    for table in schema.tables.values_mut() {
                        table.indexes.retain(|_, index| index.id != index_id);
                    }
                }
            }
            CatalogObjectRef::Constraint(constraint_id) => {
                for schema in self.database.schemas.values_mut() {
                    for table in schema.tables.values_mut() {
                        let owned_indexes = table
                            .constraints
                            .values()
                            .filter(|constraint| constraint.id == constraint_id)
                            .filter_map(|constraint| {
                                table
                                    .indexes
                                    .get(&constraint.name)
                                    .map(|index| (constraint.name.clone(), index.id))
                            })
                            .collect::<Vec<_>>();
                        table
                            .constraints
                            .retain(|_, constraint| constraint.id != constraint_id);
                        for (name, index_id) in owned_indexes {
                            table.indexes.remove(&name);
                            self.ownership.remove(CatalogObjectRef::Index(index_id));
                            self.postgres_oid_registry
                                .remove(PostgresOidObject::Index(index_id));
                        }
                    }
                }
            }
            CatalogObjectRef::Sequence(sequence_id) => {
                for schema in self.database.schemas.values_mut() {
                    schema
                        .sequences
                        .retain(|_, sequence| sequence.id != sequence_id);
                }
            }
            CatalogObjectRef::View(view_id) => {
                for schema in self.database.schemas.values_mut() {
                    schema.views.retain(|_, view| view.id != view_id);
                }
            }
            CatalogObjectRef::Routine(routine_id) => {
                for schema in self.database.schemas.values_mut() {
                    for routines in schema.routines.values_mut() {
                        routines.retain(|routine| routine.id != routine_id);
                    }
                    schema.routines.retain(|_, routines| !routines.is_empty());
                }
            }
            CatalogObjectRef::Trigger(trigger_id) => {
                for schema in self.database.schemas.values_mut() {
                    for table in schema.tables.values_mut() {
                        table.triggers.retain(|_, trigger| trigger.id != trigger_id);
                    }
                    for view in schema.views.values_mut() {
                        view.triggers.retain(|_, trigger| trigger.id != trigger_id);
                    }
                }
            }
            CatalogObjectRef::Type(type_id) => {
                let constraints = self
                    .type_by_id(type_id)
                    .and_then(|definition| match &definition.definition {
                        UserDefinedTypeKind::Domain { checks, .. } => Some(
                            checks
                                .iter()
                                .filter_map(|constraint| constraint.id)
                                .collect::<Vec<_>>(),
                        ),
                        UserDefinedTypeKind::Enum { .. } => None,
                    })
                    .unwrap_or_default();
                for schema in self.database.schemas.values_mut() {
                    schema
                        .types
                        .retain(|_, definition| definition.id != type_id);
                }
                for constraint_id in constraints {
                    self.postgres_oid_registry
                        .remove(PostgresOidObject::Constraint(constraint_id));
                }
            }
        }
        self.dependencies.remove(object);
        self.ownership.remove(object);
        self.postgres_oid_registry.remove(object.into());
        Ok(())
    }
}

impl SchemaDefinition {
    #[must_use]
    pub fn relation_name_exists(&self, name: &Identifier) -> bool {
        self.tables.contains_key(name)
            || self.sequences.contains_key(name)
            || self.views.contains_key(name)
            || self
                .tables
                .values()
                .any(|table| table.index(name).is_some())
    }
}

fn resolve_constraint_columns(
    table: &TableDefinition,
    names: &[Identifier],
) -> Result<Vec<ColumnId>> {
    if names.is_empty() {
        return Err(DbError::new(
            "42601",
            "a table constraint must contain at least one column",
        ));
    }
    let mut seen = BTreeSet::new();
    names
        .iter()
        .map(|name| {
            let column = table
                .column(name)
                .ok_or_else(|| DbError::new("42703", format!("column {name} does not exist")))?;
            if !seen.insert(column.id) {
                return Err(DbError::new(
                    "42701",
                    format!("column {name} specified more than once"),
                ));
            }
            Ok(column.id)
        })
        .collect()
}

fn constraint_columns(kind: &ConstraintKind) -> impl Iterator<Item = ColumnId> + '_ {
    match kind {
        ConstraintKind::PrimaryKey { columns }
        | ConstraintKind::Unique { columns }
        | ConstraintKind::ForeignKey { columns, .. } => columns.iter().copied(),
        ConstraintKind::Check { .. } => [].iter().copied(),
    }
}

fn sequence_type_bounds(data_type: &ScalarType) -> Result<(i64, i64)> {
    match data_type {
        ScalarType::Int16 => Ok((i64::from(i16::MIN), i64::from(i16::MAX))),
        ScalarType::Int32 => Ok((i64::from(i32::MIN), i64::from(i32::MAX))),
        ScalarType::Int64 => Ok((i64::MIN, i64::MAX)),
        _ => Err(DbError::new(
            "42804",
            "sequence type must be SMALLINT, INTEGER, or BIGINT",
        )),
    }
}

#[must_use]
pub const fn indexable_type(data_type: &ScalarType) -> bool {
    !matches!(
        data_type,
        ScalarType::Json | ScalarType::Jsonb | ScalarType::Vector { .. }
    )
}

#[must_use]
pub const fn text_search_type(data_type: &ScalarType) -> bool {
    matches!(
        data_type,
        ScalarType::Char { .. } | ScalarType::Varchar { .. } | ScalarType::Text
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use ordadb_types::{
        ColumnId, ConstraintId, Identifier, IndexId, RoutineId, ScalarType, Schema, SchemaId,
        SequenceId, TableId, TriggerId, TypeId, ViewId,
    };

    use super::{
        Catalog, CatalogExpression, CatalogObjectRef, CatalogOwner, ConstraintKind,
        DependencyGraph, DomainBaseType, DomainConstraint, DropBehavior, EnumValuePosition,
        FullTextAnalyzer, IndexDefinition, IndexMethod, IndexOptions, NewColumn, NewConstraint,
        NewConstraintKind, NewIndex, NewRoutine, NewSequence, NewView, PG_AM_TABLE_ID,
        PG_CATALOG_SCHEMA_ID, PG_COLLATION_TABLE_ID, PG_DESCRIPTION_TABLE_ID,
        PG_NAMESPACE_TABLE_ID, POSTGRES_OID_EXHAUSTED, POSTGRES_OID_FIRST_USER,
        POSTGRES_OID_LAST_BUILTIN, PostgresOid, PostgresOidObject, ReferentialAction,
        RoutineArgument, RoutineArgumentMode, RoutineKind, SchemaDefinition, TableDefinition,
        TableStatistics, TriggerDefinition, TriggerEvent, TriggerLevel, TriggerTarget,
        TriggerTiming, UserDefinedTypeKind, VectorDistanceMetric, ViewKind, system_relation,
        system_relations,
    };

    struct OidFixture {
        catalog: Catalog,
        schema_id: SchemaId,
        table_id: TableId,
        column_id: ColumnId,
        index_id: IndexId,
        constraint_id: ConstraintId,
        sequence_id: SequenceId,
        view_id: ViewId,
        materialized_view_id: ViewId,
        routine_id: RoutineId,
        trigger_id: TriggerId,
        type_id: TypeId,
    }

    fn oid_fixture() -> OidFixture {
        let mut catalog = Catalog::default();
        let schema_id = catalog
            .create_schema(Identifier::unquoted("app"))
            .expect("schema");
        let type_id = catalog
            .create_enum_type(
                &Identifier::unquoted("app"),
                Identifier::unquoted("status"),
                vec!["ready".into(), "done".into()],
            )
            .expect("type");
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("app"),
                Identifier::unquoted("items"),
                vec![
                    NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                    NewColumn::new(Identifier::unquoted("label"), ScalarType::Text),
                ],
            )
            .expect("table");
        let column_id = catalog.table_by_id(table_id).expect("table").columns()[1].id;
        let index_id = catalog
            .create_index(
                table_id,
                NewIndex {
                    name: Identifier::unquoted("items_label_idx"),
                    key_columns: vec![Identifier::unquoted("label")],
                    include_columns: Vec::new(),
                    unique: false,
                    method: IndexMethod::BTree,
                    options: IndexOptions::BTree,
                },
            )
            .expect("index");
        let constraint_id = catalog
            .create_constraint(
                table_id,
                NewConstraint {
                    name: Identifier::unquoted("items_id_positive"),
                    kind: NewConstraintKind::Check {
                        expression: CatalogExpression::new("id > 0"),
                    },
                },
            )
            .expect("constraint");
        let sequence_id = catalog
            .create_sequence(
                &Identifier::unquoted("app"),
                NewSequence::new(Identifier::unquoted("items_id_seq")),
            )
            .expect("sequence");
        let view_id = catalog
            .create_view(
                &Identifier::unquoted("app"),
                NewView {
                    name: Identifier::unquoted("item_view"),
                    kind: ViewKind::Regular,
                    query: "SELECT id, label FROM items".into(),
                    output: Schema::empty(),
                    materialized_table_id: None,
                    populated: true,
                    references: vec![CatalogObjectRef::Table(table_id)],
                },
            )
            .expect("view");
        let backing_table_id = catalog
            .create_table(
                &Identifier::unquoted("app"),
                Identifier::unquoted("item_rollup_storage"),
                vec![NewColumn::new(
                    Identifier::unquoted("count"),
                    ScalarType::Int64,
                )],
            )
            .expect("materialized backing table");
        let materialized_view_id = catalog
            .create_view(
                &Identifier::unquoted("app"),
                NewView {
                    name: Identifier::unquoted("item_rollup"),
                    kind: ViewKind::Materialized,
                    query: "SELECT count(*) FROM items".into(),
                    output: Schema::empty(),
                    materialized_table_id: Some(backing_table_id),
                    populated: true,
                    references: vec![CatalogObjectRef::Table(table_id)],
                },
            )
            .expect("materialized view");
        let routine_id = catalog
            .create_or_replace_routine(
                &Identifier::unquoted("app"),
                NewRoutine {
                    name: Identifier::unquoted("touch_item"),
                    kind: RoutineKind::Function,
                    arguments: Vec::new(),
                    return_type: None,
                    return_declared_type: None,
                    returns_set: false,
                    language: "plpgsql".into(),
                    body: "BEGIN RETURN; END".into(),
                    replace: false,
                    references: vec![CatalogObjectRef::View(view_id)],
                },
            )
            .expect("routine");
        let trigger_id = catalog
            .create_trigger(
                table_id,
                Identifier::unquoted("items_touch"),
                TriggerTiming::Before,
                BTreeSet::from([TriggerEvent::Insert]),
                routine_id,
            )
            .expect("trigger");
        OidFixture {
            catalog,
            schema_id,
            table_id,
            column_id,
            index_id,
            constraint_id,
            sequence_id,
            view_id,
            materialized_view_id,
            routine_id,
            trigger_id,
            type_id,
        }
    }

    #[test]
    fn bootstraps_public_schema_with_deterministic_ids() {
        let catalog = Catalog::default();
        assert_eq!(catalog.database().id.get(), 1);
        assert_eq!(
            catalog
                .schema(&Identifier::unquoted("PUBLIC"))
                .expect("public schema")
                .id,
            SchemaId::new(1)
        );
        assert_eq!(POSTGRES_OID_LAST_BUILTIN, 16_383);
        assert_eq!(
            catalog
                .postgres_oid(PostgresOidObject::Database(catalog.database().id))
                .expect("database OID")
                .get(),
            POSTGRES_OID_FIRST_USER
        );
        assert_eq!(
            catalog
                .postgres_oid(PostgresOidObject::Schema(SchemaId::new(1)))
                .expect("public schema OID")
                .get(),
            POSTGRES_OID_FIRST_USER + 1
        );
        assert!(
            PostgresOid::new(POSTGRES_OID_LAST_BUILTIN)
                .expect("built-in OID")
                .is_builtin()
        );
        assert_eq!(
            PostgresOid::new(0).expect_err("zero is invalid").sql_state,
            "22023"
        );
    }

    #[test]
    fn creates_and_resolves_normalized_schema_and_table_names() {
        let mut catalog = Catalog::default();
        assert_eq!(
            catalog
                .create_schema(Identifier::unquoted("Analytics"))
                .expect("create schema"),
            SchemaId::new(2)
        );
        assert_eq!(
            catalog
                .create_table(
                    &Identifier::unquoted("ANALYTICS"),
                    Identifier::unquoted("Events"),
                    vec![NewColumn::new(
                        Identifier::unquoted("ID"),
                        ScalarType::Int64,
                    )],
                )
                .expect("create table"),
            TableId::new(1)
        );
        assert!(
            catalog
                .table(
                    &Identifier::unquoted("analytics"),
                    &Identifier::unquoted("events")
                )
                .is_some()
        );
    }

    #[test]
    fn rejects_duplicate_objects_and_columns() {
        let mut catalog = Catalog::default();
        let duplicate_schema = catalog
            .create_schema(Identifier::unquoted("PUBLIC"))
            .expect_err("duplicate schema");
        assert_eq!(duplicate_schema.sql_state, "42P06");

        let duplicate_column = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("items"),
                vec![
                    NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                    NewColumn::new(Identifier::unquoted("ID"), ScalarType::Int64),
                ],
            )
            .expect_err("duplicate column");
        assert_eq!(duplicate_column.sql_state, "42701");
    }

    #[test]
    fn primary_keys_are_not_nullable_and_are_unique() {
        let mut catalog = Catalog::default();
        let mut id = NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64);
        id.primary_key = true;
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("documents"),
                vec![id],
            )
            .expect("create table");

        let column = &catalog
            .table_by_id(table_id)
            .expect("table by id")
            .columns()[0];
        assert!(!column.nullable);
        assert!(column.unique);
        let index = catalog
            .table_by_id(table_id)
            .expect("table")
            .indexes()
            .next()
            .expect("primary index");
        assert!(index.primary);
        assert!(index.unique);
    }

    #[test]
    fn creates_composite_covering_indexes_and_rejects_overlap() {
        let mut catalog = Catalog::default();
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("events"),
                vec![
                    NewColumn::new(Identifier::unquoted("tenant"), ScalarType::Int64),
                    NewColumn::new(
                        Identifier::unquoted("created_at"),
                        ScalarType::Timestamp {
                            with_timezone: false,
                        },
                    ),
                    NewColumn::new(Identifier::unquoted("payload"), ScalarType::Jsonb),
                ],
            )
            .expect("table");
        let index_id = catalog
            .create_index(
                table_id,
                NewIndex {
                    name: Identifier::unquoted("events_tenant_created"),
                    key_columns: vec![
                        Identifier::unquoted("tenant"),
                        Identifier::unquoted("created_at"),
                    ],
                    include_columns: vec![Identifier::unquoted("payload")],
                    unique: false,
                    method: IndexMethod::BTree,
                    options: IndexOptions::BTree,
                },
            )
            .expect("index");
        let index = catalog.index_by_id(index_id).expect("index by id");
        assert_eq!(index.key_columns.len(), 2);
        assert_eq!(index.include_columns.len(), 1);

        let overlap = catalog
            .create_index(
                table_id,
                NewIndex {
                    name: Identifier::unquoted("bad"),
                    key_columns: vec![Identifier::unquoted("tenant")],
                    include_columns: vec![Identifier::unquoted("tenant")],
                    unique: false,
                    method: IndexMethod::BTree,
                    options: IndexOptions::BTree,
                },
            )
            .expect_err("overlap");
        assert_eq!(overlap.sql_state, "42701");
    }

    #[test]
    fn creates_search_indexes_and_preserves_btree_serde_defaults() {
        let mut catalog = Catalog::default();
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("documents"),
                vec![
                    NewColumn::new(Identifier::unquoted("title"), ScalarType::Text),
                    NewColumn::new(
                        Identifier::unquoted("embedding"),
                        ScalarType::Vector {
                            dimensions: Some(3),
                        },
                    ),
                ],
            )
            .expect("table");
        let full_text_id = catalog
            .create_index(
                table_id,
                NewIndex {
                    name: Identifier::unquoted("documents_fts"),
                    key_columns: vec![Identifier::unquoted("title")],
                    include_columns: Vec::new(),
                    unique: false,
                    method: IndexMethod::FullText,
                    options: IndexOptions::FullText {
                        analyzer: FullTextAnalyzer::Whitespace,
                    },
                },
            )
            .expect("full-text index");
        let hnsw_id = catalog
            .create_index(
                table_id,
                NewIndex {
                    name: Identifier::unquoted("documents_embedding_hnsw"),
                    key_columns: vec![Identifier::unquoted("embedding")],
                    include_columns: Vec::new(),
                    unique: false,
                    method: IndexMethod::Hnsw,
                    options: IndexOptions::Hnsw {
                        metric: VectorDistanceMetric::Cosine,
                        dimensions: 3,
                        m: 16,
                        ef_construction: 64,
                        ef_search: 40,
                    },
                },
            )
            .expect("HNSW index");
        assert_eq!(
            catalog.index_by_id(full_text_id).expect("full-text").method,
            IndexMethod::FullText
        );
        assert_eq!(
            catalog.index_by_id(hnsw_id).expect("HNSW").method,
            IndexMethod::Hnsw
        );

        let definition = IndexDefinition {
            id: ordadb_types::IndexId::new(999),
            table_id,
            name: Identifier::unquoted("legacy"),
            key_columns: vec![
                catalog
                    .table_by_id(table_id)
                    .expect("table")
                    .columns()
                    .first()
                    .expect("column")
                    .id,
            ],
            include_columns: Vec::new(),
            unique: false,
            primary: false,
            method: IndexMethod::BTree,
            options: IndexOptions::BTree,
        };
        let mut encoded = serde_json::to_value(definition).expect("serialize legacy definition");
        let object = encoded.as_object_mut().expect("definition object");
        object.remove("method");
        object.remove("options");
        let decoded: IndexDefinition =
            serde_json::from_value(encoded).expect("decode old B+Tree definition");
        assert_eq!(decoded.method, IndexMethod::BTree);
        assert_eq!(decoded.options, IndexOptions::BTree);
    }

    #[test]
    fn dependency_graph_rejects_cycles_and_orders_cascade_iteratively() {
        let table = CatalogObjectRef::Table(TableId::new(1));
        let first_view = CatalogObjectRef::View(ordadb_types::ViewId::new(1));
        let second_view = CatalogObjectRef::View(ordadb_types::ViewId::new(2));
        let mut graph = DependencyGraph::default();
        graph.add(first_view, table).expect("view depends on table");
        graph
            .add(second_view, first_view)
            .expect("nested view dependency");

        let restrict = graph
            .drop_order(table, DropBehavior::Restrict)
            .expect_err("restrict must fail");
        assert_eq!(restrict.sql_state, "2BP01");
        assert_eq!(
            graph
                .drop_order(table, DropBehavior::Cascade)
                .expect("cascade order"),
            vec![second_view, first_view, table]
        );

        let cycle = graph.add(table, second_view).expect_err("dependency cycle");
        assert_eq!(cycle.sql_state, "2BP01");
    }

    #[test]
    fn persists_constraints_sequences_views_routines_and_triggers() {
        let mut catalog = Catalog::default();
        let mut parent_id = NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64);
        parent_id.primary_key = true;
        let parent = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("parents"),
                vec![parent_id],
            )
            .expect("parent");
        let child = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("children"),
                vec![NewColumn::new(
                    Identifier::unquoted("parent_id"),
                    ScalarType::Int64,
                )],
            )
            .expect("child");
        let referenced_column = catalog.table_by_id(parent).expect("parent table").columns()[0].id;
        catalog
            .create_constraint(
                child,
                NewConstraint {
                    name: Identifier::unquoted("children_parent_fk"),
                    kind: NewConstraintKind::ForeignKey {
                        columns: vec![Identifier::unquoted("parent_id")],
                        referenced_table: parent,
                        referenced_columns: vec![referenced_column],
                        on_delete: ReferentialAction::Cascade,
                        on_update: ReferentialAction::Restrict,
                    },
                },
            )
            .expect("foreign key");
        assert!(matches!(
            catalog
                .table_by_id(child)
                .expect("child table")
                .constraints()
                .next()
                .expect("constraint")
                .kind,
            ConstraintKind::ForeignKey { .. }
        ));

        let sequence = catalog
            .create_sequence(
                &Identifier::unquoted("public"),
                NewSequence::new(Identifier::unquoted("children_id_seq")),
            )
            .expect("sequence");
        assert_eq!(catalog.next_sequence_value(sequence).expect("first"), 1);
        assert_eq!(catalog.next_sequence_value(sequence).expect("second"), 2);

        let view = catalog
            .create_view(
                &Identifier::unquoted("public"),
                NewView {
                    name: Identifier::unquoted("child_view"),
                    kind: ViewKind::Regular,
                    query: "SELECT parent_id FROM children".into(),
                    output: Schema::empty(),
                    materialized_table_id: None,
                    populated: true,
                    references: vec![CatalogObjectRef::Table(child)],
                },
            )
            .expect("view");
        let routine = catalog
            .create_or_replace_routine(
                &Identifier::unquoted("public"),
                NewRoutine {
                    name: Identifier::unquoted("touch_child"),
                    kind: RoutineKind::Function,
                    arguments: vec![RoutineArgument {
                        name: Some(Identifier::unquoted("value")),
                        data_type: ScalarType::Int64,
                        declared_type: None,
                        mode: Default::default(),
                    }],
                    return_type: Some(ScalarType::Int64),
                    return_declared_type: None,
                    returns_set: false,
                    language: "plpgsql".into(),
                    body: "BEGIN RETURN value; END".into(),
                    replace: false,
                    references: vec![CatalogObjectRef::View(view)],
                },
            )
            .expect("routine");
        let trigger = catalog
            .create_trigger(
                child,
                Identifier::unquoted("children_touch"),
                TriggerTiming::Before,
                BTreeSet::from([TriggerEvent::Insert]),
                routine,
            )
            .expect("trigger");
        assert_eq!(
            catalog.trigger_by_id(trigger).expect("trigger").routine_id,
            routine
        );

        let encoded = serde_json::to_vec(&catalog).expect("serialize catalog");
        let decoded: Catalog = serde_json::from_slice(&encoded).expect("deserialize catalog");
        assert_eq!(decoded, catalog);
        assert!(decoded.sequence_by_id(sequence).is_some());
        assert!(decoded.view_by_id(view).is_some());
        assert!(decoded.routine_by_id(routine).is_some());
        assert!(decoded.trigger_by_id(trigger).is_some());
    }

    #[test]
    fn renames_alters_and_drops_catalog_objects_without_stale_names() {
        let mut catalog = Catalog::default();
        let schema_id = catalog
            .create_schema(Identifier::unquoted("app"))
            .expect("schema");
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("app"),
                Identifier::unquoted("items"),
                vec![
                    NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                    NewColumn::new(Identifier::unquoted("label"), ScalarType::Text),
                ],
            )
            .expect("table");
        let column_id = catalog.table_by_id(table_id).expect("table").columns()[1].id;
        catalog
            .rename_schema(schema_id, Identifier::unquoted("core"))
            .expect("rename schema");
        catalog
            .rename_table(table_id, Identifier::unquoted("entries"))
            .expect("rename table");
        catalog
            .rename_column(table_id, column_id, Identifier::unquoted("title"))
            .expect("rename column");
        catalog
            .alter_column(
                table_id,
                column_id,
                None,
                Some(false),
                Some(Some(super::CatalogExpression::new("'untitled'"))),
                None,
            )
            .expect("alter column");

        let table = catalog
            .table(
                &Identifier::unquoted("core"),
                &Identifier::unquoted("entries"),
            )
            .expect("renamed table");
        let column = table
            .column(&Identifier::unquoted("title"))
            .expect("renamed column");
        assert!(!column.nullable);
        assert_eq!(
            column.default.as_ref().map(|value| value.sql.as_str()),
            Some("'untitled'")
        );

        let removed = catalog
            .drop_table(table_id, DropBehavior::Restrict)
            .expect("drop table");
        assert!(removed.contains(&CatalogObjectRef::Table(table_id)));
        assert!(
            catalog
                .table(
                    &Identifier::unquoted("core"),
                    &Identifier::unquoted("entries")
                )
                .is_none()
        );
        catalog
            .drop_schema(schema_id, DropBehavior::Restrict)
            .expect("drop empty schema");
        assert!(catalog.schema(&Identifier::unquoted("core")).is_none());
    }

    #[test]
    fn ownership_round_trips_and_cascade_removes_owned_children() {
        let previous = Catalog::default();
        let mut catalog = previous.clone();
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("owned_items"),
                vec![
                    NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                    NewColumn::new(Identifier::unquoted("label"), ScalarType::Text),
                ],
            )
            .expect("table");
        let owner = CatalogOwner::new("alice").expect("owner");
        catalog
            .assign_new_object_owners(&previous, &owner)
            .expect("assign ownership");
        let created = catalog
            .object_refs()
            .difference(&previous.object_refs())
            .copied()
            .collect::<Vec<_>>();
        assert!(!created.is_empty());
        assert!(created.iter().all(|object| {
            catalog.owner_of(*object).map(CatalogOwner::as_str) == Some("alice")
        }));
        assert!(
            catalog
                .owner_of(CatalogObjectRef::Schema(SchemaId::new(1)))
                .is_none()
        );

        let encoded = serde_json::to_vec(&catalog).expect("serialize ownership");
        let mut reopened: Catalog =
            serde_json::from_slice(&encoded).expect("deserialize ownership");
        assert!(created.iter().all(|object| {
            reopened.owner_of(*object).map(CatalogOwner::as_str) == Some("alice")
        }));

        reopened
            .drop_table(table_id, DropBehavior::Cascade)
            .expect("drop owned table");
        assert!(
            created
                .iter()
                .all(|object| reopened.owner_of(*object).is_none())
        );
    }

    #[test]
    fn restrict_and_cascade_follow_external_dependencies() {
        let mut catalog = Catalog::default();
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("source"),
                vec![NewColumn::new(
                    Identifier::unquoted("id"),
                    ScalarType::Int64,
                )],
            )
            .expect("table");
        let view_id = catalog
            .create_view(
                &Identifier::unquoted("public"),
                NewView {
                    name: Identifier::unquoted("source_view"),
                    kind: ViewKind::Regular,
                    query: "SELECT id FROM source".into(),
                    output: Schema::empty(),
                    materialized_table_id: None,
                    populated: true,
                    references: vec![CatalogObjectRef::Table(table_id)],
                },
            )
            .expect("view");

        let error = catalog
            .drop_table(table_id, DropBehavior::Restrict)
            .expect_err("restrict dependent view");
        assert_eq!(error.sql_state, "2BP01");
        let removed = catalog
            .drop_table(table_id, DropBehavior::Cascade)
            .expect("cascade table");
        assert!(removed.contains(&CatalogObjectRef::View(view_id)));
        assert!(catalog.view_by_id(view_id).is_none());
        assert!(catalog.table_by_id(table_id).is_none());
    }

    #[test]
    fn user_defined_types_round_trip_and_track_column_dependencies() {
        let mut catalog = Catalog::default();
        for (name, labels, sql_state) in [
            ("empty_enum", Vec::new(), "42601"),
            ("empty_label", vec![String::new()], "42601"),
            (
                "duplicate_label",
                vec!["same".to_owned(), "same".to_owned()],
                "42710",
            ),
            ("long_label", vec!["界".repeat(22)], "42622"),
        ] {
            let error = catalog
                .create_enum_type(
                    &Identifier::unquoted("public"),
                    Identifier::unquoted(name),
                    labels,
                )
                .expect_err("invalid enum labels");
            assert_eq!(error.sql_state, sql_state);
        }
        let enum_id = catalog
            .create_enum_type(
                &Identifier::unquoted("public"),
                Identifier::unquoted("mood"),
                vec!["sad".into(), "ok".into(), "happy".into()],
            )
            .expect("enum");
        let domain_id = catalog
            .create_domain(
                &Identifier::unquoted("public"),
                Identifier::unquoted("positive_int"),
                ScalarType::Int32,
                true,
                Some(CatalogExpression::new("1")),
                vec![DomainConstraint {
                    id: None,
                    name: Some(Identifier::unquoted("positive")),
                    expression: CatalogExpression::new("VALUE > 0"),
                }],
            )
            .expect("domain");
        let mut mood = NewColumn::new(Identifier::unquoted("mood"), ScalarType::Text);
        mood.declared_type = Some(enum_id);
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("feelings"),
                vec![mood],
            )
            .expect("table");
        let enum_data_type = catalog
            .type_by_id(enum_id)
            .expect("enum type")
            .logical_type();
        let enum_domain_id = catalog
            .create_domain_with_declared_type(
                &Identifier::unquoted("public"),
                Identifier::unquoted("cheerful_mood"),
                DomainBaseType::new(enum_data_type.clone(), Some(enum_id)),
                false,
                Some(CatalogExpression::new("'ok'::mood")),
                Vec::new(),
            )
            .expect("enum domain");
        assert_eq!(
            catalog
                .dependencies()
                .references(CatalogObjectRef::Type(enum_domain_id))
                .collect::<Vec<_>>(),
            vec![CatalogObjectRef::Type(enum_id)]
        );
        let mut cheerful = NewColumn::new(Identifier::unquoted("cheerful"), enum_data_type.clone());
        cheerful.declared_type = Some(enum_domain_id);
        let enum_domain_table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("cheerful_feelings"),
                vec![cheerful],
            )
            .expect("enum domain table");
        let routine_id = catalog
            .create_or_replace_routine(
                &Identifier::unquoted("public"),
                NewRoutine {
                    name: Identifier::unquoted("echo_mood"),
                    kind: RoutineKind::Function,
                    arguments: vec![RoutineArgument {
                        name: Some(Identifier::unquoted("value")),
                        data_type: enum_data_type,
                        declared_type: Some(enum_id),
                        mode: Default::default(),
                    }],
                    return_type: Some(ScalarType::Int32),
                    return_declared_type: Some(domain_id),
                    returns_set: false,
                    language: "plpgsql".into(),
                    body: "BEGIN RETURN 1; END".into(),
                    replace: false,
                    references: vec![
                        CatalogObjectRef::Type(enum_id),
                        CatalogObjectRef::Type(domain_id),
                    ],
                },
            )
            .expect("routine");

        assert!(
            catalog
                .alter_enum_add_value(
                    enum_id,
                    "calm".into(),
                    Some(EnumValuePosition::Before("happy".into())),
                    false,
                )
                .expect("add enum label")
        );
        assert!(
            !catalog
                .alter_enum_add_value(enum_id, "calm".into(), None, true)
                .expect("duplicate enum label is a no-op")
        );
        catalog
            .alter_enum_rename_value(enum_id, "ok", "fine".into())
            .expect("rename enum label");
        let expected_enum = ScalarType::Enum {
            type_id: enum_id,
            labels: vec!["sad".into(), "fine".into(), "calm".into(), "happy".into()],
        };
        assert_eq!(
            catalog.table_by_id(table_id).expect("table").columns()[0].data_type,
            expected_enum
        );
        assert_eq!(
            catalog
                .routine_by_id(routine_id)
                .expect("routine")
                .arguments[0]
                .data_type,
            expected_enum
        );
        assert!(matches!(
            &catalog
                .type_by_id(enum_domain_id)
                .expect("enum domain")
                .definition,
            UserDefinedTypeKind::Domain {
                base_type,
                base_declared_type: Some(base_type_id),
                ..
            } if base_type == &expected_enum && *base_type_id == enum_id
        ));
        assert_eq!(
            catalog
                .table_by_id(enum_domain_table_id)
                .expect("enum domain table")
                .columns()[0]
                .data_type,
            expected_enum
        );
        catalog
            .alter_domain_default(domain_id, Some(CatalogExpression::new("2")))
            .expect("alter domain default");
        catalog
            .alter_domain_not_null(domain_id, false)
            .expect("drop domain not null");
        catalog
            .add_domain_constraint(
                domain_id,
                DomainConstraint {
                    id: None,
                    name: Some(Identifier::unquoted("below_limit")),
                    expression: CatalogExpression::new("VALUE < 100"),
                },
            )
            .expect("add domain constraint");
        assert!(
            catalog
                .drop_domain_constraint(domain_id, &Identifier::unquoted("positive"), false,)
                .expect("drop domain constraint")
        );

        let error = catalog
            .drop_type(enum_id, DropBehavior::Restrict)
            .expect_err("column dependency");
        assert_eq!(error.sql_state, "2BP01");
        let error = catalog
            .drop_type(domain_id, DropBehavior::Restrict)
            .expect_err("routine return dependency");
        assert_eq!(error.sql_state, "2BP01");
        assert!(matches!(
            &catalog.type_by_id(domain_id).expect("domain").definition,
            UserDefinedTypeKind::Domain {
                not_null: false,
                default: Some(default),
                checks,
                ..
            } if default.sql == "2"
                && checks.len() == 1
                && checks[0].name.as_ref().is_some_and(|name| name.as_str() == "below_limit")
        ));

        let encoded = serde_json::to_vec(&catalog).expect("serialize");
        let decoded: Catalog = serde_json::from_slice(&encoded).expect("deserialize");
        assert_eq!(decoded, catalog);
        assert_eq!(
            decoded.table_by_id(table_id).expect("table").columns()[0].declared_type,
            Some(enum_id)
        );
        let routine = decoded.routine_by_id(routine_id).expect("routine");
        assert_eq!(routine.arguments[0].declared_type, Some(enum_id));
        assert_eq!(routine.return_declared_type, Some(domain_id));
    }

    #[test]
    fn assigns_unique_postgres_oids_to_every_catalog_object_kind() {
        let fixture = oid_fixture();
        let catalog = &fixture.catalog;
        let objects = [
            PostgresOidObject::Database(catalog.database().id),
            PostgresOidObject::Schema(fixture.schema_id),
            PostgresOidObject::Table(fixture.table_id),
            PostgresOidObject::Column(fixture.table_id, fixture.column_id),
            PostgresOidObject::Index(fixture.index_id),
            PostgresOidObject::Constraint(fixture.constraint_id),
            PostgresOidObject::Sequence(fixture.sequence_id),
            PostgresOidObject::View(fixture.view_id),
            PostgresOidObject::View(fixture.materialized_view_id),
            PostgresOidObject::Routine(fixture.routine_id),
            PostgresOidObject::Trigger(fixture.trigger_id),
            PostgresOidObject::Type(fixture.type_id),
        ];
        let mut oids = BTreeSet::new();
        for object in objects {
            let oid = catalog.postgres_oid(object).expect("object OID");
            assert!(oid.get() >= POSTGRES_OID_FIRST_USER);
            assert!(oids.insert(oid));
            assert_eq!(catalog.postgres_oid_object(oid), Some(object));
        }
        catalog
            .validate_postgres_oid_registry()
            .expect("valid registry");
    }

    #[test]
    fn postgres_oids_survive_renames_replacements_and_alterations() {
        let mut fixture = oid_fixture();
        let before = fixture
            .catalog
            .postgres_oid_registry()
            .mappings()
            .collect::<BTreeMap<_, _>>();
        fixture
            .catalog
            .rename_schema(fixture.schema_id, Identifier::unquoted("renamed"))
            .expect("rename schema");
        fixture
            .catalog
            .rename_table(fixture.table_id, Identifier::unquoted("renamed_items"))
            .expect("rename table");
        fixture
            .catalog
            .rename_column(
                fixture.table_id,
                fixture.column_id,
                Identifier::unquoted("renamed_label"),
            )
            .expect("rename column");
        fixture
            .catalog
            .rename_index(fixture.index_id, Identifier::unquoted("renamed_items_idx"))
            .expect("rename index");
        fixture
            .catalog
            .rename_sequence(
                fixture.sequence_id,
                Identifier::unquoted("renamed_items_seq"),
            )
            .expect("rename sequence");
        fixture
            .catalog
            .rename_view(fixture.view_id, Identifier::unquoted("renamed_item_view"))
            .expect("rename view");
        fixture
            .catalog
            .replace_view(
                fixture.view_id,
                "SELECT id FROM renamed_items".into(),
                Schema::empty(),
                true,
                [CatalogObjectRef::Table(fixture.table_id)],
            )
            .expect("replace view");
        let replaced_routine = fixture
            .catalog
            .create_or_replace_routine(
                &Identifier::unquoted("renamed"),
                NewRoutine {
                    name: Identifier::unquoted("touch_item"),
                    kind: RoutineKind::Function,
                    arguments: Vec::new(),
                    return_type: None,
                    return_declared_type: None,
                    returns_set: false,
                    language: "plpgsql".into(),
                    body: "BEGIN NULL; RETURN; END".into(),
                    replace: true,
                    references: vec![CatalogObjectRef::View(fixture.view_id)],
                },
            )
            .expect("replace routine");
        assert_eq!(replaced_routine, fixture.routine_id);
        fixture
            .catalog
            .set_trigger_enabled(fixture.trigger_id, false)
            .expect("alter trigger");
        assert!(
            fixture
                .catalog
                .alter_enum_add_value(fixture.type_id, "archived".into(), None, false)
                .expect("alter type")
        );
        assert_eq!(
            fixture
                .catalog
                .postgres_oid_registry()
                .mappings()
                .collect::<BTreeMap<_, _>>(),
            before
        );
    }

    #[test]
    fn dropped_postgres_oids_are_removed_and_never_reused_after_reopen() {
        let mut catalog = Catalog::default();
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("old_items"),
                vec![NewColumn::new(
                    Identifier::unquoted("id"),
                    ScalarType::Int64,
                )],
            )
            .expect("old table");
        let old_object = PostgresOidObject::Table(table_id);
        let old_oid = catalog.postgres_oid(old_object).expect("old OID");
        catalog
            .drop_table(table_id, DropBehavior::Restrict)
            .expect("drop old table");
        assert!(catalog.postgres_oid_registry().oid(old_object).is_none());
        assert_eq!(
            catalog
                .postgres_oid(old_object)
                .expect_err("dropped object has no live OID")
                .sql_state,
            "22023"
        );

        let encoded = serde_json::to_vec(&catalog).expect("serialize dropped registry");
        let mut reopened: Catalog = serde_json::from_slice(&encoded).expect("reopen catalog");
        let new_table_id = reopened
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("new_items"),
                vec![NewColumn::new(
                    Identifier::unquoted("id"),
                    ScalarType::Int64,
                )],
            )
            .expect("new table");
        let new_oid = reopened
            .postgres_oid(PostgresOidObject::Table(new_table_id))
            .expect("new OID");
        assert!(new_oid.get() > old_oid.get());
    }

    #[test]
    fn legacy_catalog_reconstructs_deterministic_stable_postgres_oids() {
        let fixture = oid_fixture();
        let mut legacy = serde_json::to_value(&fixture.catalog).expect("serialize legacy source");
        legacy
            .as_object_mut()
            .expect("catalog object")
            .remove("postgres_oid_registry");
        let first: Catalog = serde_json::from_value(legacy.clone()).expect("first legacy reopen");
        let second: Catalog = serde_json::from_value(legacy).expect("second legacy reopen");
        assert_eq!(
            first
                .postgres_oid_registry()
                .mappings()
                .collect::<BTreeMap<_, _>>(),
            second
                .postgres_oid_registry()
                .mappings()
                .collect::<BTreeMap<_, _>>()
        );

        let first_encoding = serde_json::to_vec(&first).expect("serialize reconstructed registry");
        let reopened: Catalog =
            serde_json::from_slice(&first_encoding).expect("reopen reconstructed registry");
        let second_encoding = serde_json::to_vec(&reopened).expect("reserialize registry");
        assert_eq!(first_encoding, second_encoding);
        assert_eq!(first, reopened);
    }

    #[test]
    fn rejects_duplicate_and_corrupt_postgres_oid_mappings() {
        let fixture = oid_fixture();
        let mut duplicate = serde_json::to_value(&fixture.catalog).expect("serialize registry");
        let mappings = duplicate
            .get_mut("postgres_oid_registry")
            .and_then(|registry| registry.get_mut("mappings"))
            .and_then(serde_json::Value::as_array_mut)
            .expect("registry mappings");
        let duplicate_oid = mappings[0].get("oid").cloned().expect("first OID");
        mappings[1]
            .as_object_mut()
            .expect("mapping")
            .insert("oid".into(), duplicate_oid);
        let error = serde_json::from_value::<Catalog>(duplicate).expect_err("duplicate OID");
        assert!(error.to_string().contains("XX001"));

        let mut corrupt = serde_json::to_value(&fixture.catalog).expect("serialize registry");
        corrupt
            .get_mut("postgres_oid_registry")
            .and_then(|registry| registry.get_mut("mappings"))
            .and_then(serde_json::Value::as_array_mut)
            .expect("registry mappings")
            .pop();
        let error = serde_json::from_value::<Catalog>(corrupt).expect_err("missing mapping");
        assert!(error.to_string().contains("XX001"));
    }

    #[test]
    fn postgres_oid_exhaustion_is_atomic_and_explicit() {
        let mut catalog = Catalog::default();
        catalog.postgres_oid_registry.next_oid = POSTGRES_OID_EXHAUSTED;
        catalog
            .validate_postgres_oid_registry()
            .expect("exhausted cursor is durable");
        let before = catalog.clone();
        let error = catalog
            .create_schema(Identifier::unquoted("exhausted"))
            .expect_err("OID exhaustion");
        assert_eq!(error.sql_state, "54000");
        assert_eq!(catalog, before);
    }

    #[test]
    fn cloned_catalog_candidates_do_not_publish_rolled_back_oids() {
        let committed = Catalog::default();
        let mut rolled_back = committed.clone();
        let rolled_back_schema = rolled_back
            .create_schema(Identifier::unquoted("candidate"))
            .expect("candidate schema");
        let rolled_back_oid = rolled_back
            .postgres_oid(PostgresOidObject::Schema(rolled_back_schema))
            .expect("candidate OID");
        assert_eq!(
            committed
                .postgres_oid(PostgresOidObject::Schema(rolled_back_schema))
                .expect_err("unpublished candidate")
                .sql_state,
            "22023"
        );
        drop(rolled_back);

        let mut retried = committed.clone();
        let retried_schema = retried
            .create_schema(Identifier::unquoted("candidate"))
            .expect("retried schema");
        assert_eq!(retried_schema, rolled_back_schema);
        assert_eq!(
            retried
                .postgres_oid(PostgresOidObject::Schema(retried_schema))
                .expect("retried OID"),
            rolled_back_oid
        );
    }

    #[test]
    fn system_relation_descriptors_are_unique_stable_and_lookupable() {
        let catalog = Catalog::default();
        let relations = system_relations();
        assert_eq!(relations.len(), 27);

        let mut table_ids = BTreeSet::new();
        let mut relation_oids = BTreeSet::new();
        let mut qualified_names = BTreeSet::new();
        let mut column_ids = BTreeSet::new();
        for relation in relations {
            assert!(table_ids.insert(relation.table_id));
            assert!(relation_oids.insert(relation.oid));
            assert!(relation.oid.is_builtin());
            assert!(qualified_names.insert((relation.schema, relation.name)));
            assert!(Catalog::is_system_schema(&Identifier::unquoted(
                relation.schema
            )));
            assert!(Catalog::is_system_table(relation.table_id));
            assert_eq!(system_relation(relation.table_id), Some(relation));

            let table = catalog
                .table(
                    &Identifier::unquoted(relation.schema),
                    &Identifier::unquoted(relation.name),
                )
                .expect("system relation by name");
            assert_eq!(catalog.table_by_id(relation.table_id), Some(table));
            assert_eq!(table.schema_id, relation.schema_id);
            assert_eq!(table.columns().len(), relation.columns.len());
            for (column, descriptor) in table.columns().iter().zip(relation.columns) {
                assert!(column_ids.insert(column.id));
                assert_eq!(column.id, descriptor.id);
                assert_eq!(column.name.as_str(), descriptor.name);
                assert_eq!(column.data_type, descriptor.data_type);
                assert_eq!(column.nullable, descriptor.nullable);
            }
        }

        let namespace = system_relation(PG_NAMESPACE_TABLE_ID).expect("pg_namespace");
        assert_eq!(namespace.schema, "pg_catalog");
        assert_eq!(namespace.name, "pg_namespace");
        assert_eq!(namespace.oid.get(), 2_615);
        assert_eq!(
            system_relation(PG_AM_TABLE_ID).expect("pg_am").oid.get(),
            2_601
        );
        assert_eq!(
            system_relation(PG_COLLATION_TABLE_ID)
                .expect("pg_collation")
                .oid
                .get(),
            3_456
        );
        assert_eq!(
            system_relation(PG_DESCRIPTION_TABLE_ID)
                .expect("pg_description")
                .oid
                .get(),
            2_609
        );
        assert_eq!(
            namespace
                .columns
                .iter()
                .map(|column| (column.name, &column.data_type, column.nullable))
                .collect::<Vec<_>>(),
            vec![
                ("oid", &ScalarType::Oid, false),
                ("nspname", &ScalarType::Name, false),
                ("nspowner", &ScalarType::Oid, false),
            ]
        );
        assert_eq!(
            catalog
                .schema(&Identifier::quoted("pg_catalog"))
                .expect("quoted system schema")
                .id,
            PG_CATALOG_SCHEMA_ID
        );
        assert!(
            catalog
                .schema(&Identifier::quoted("pg_catalog"))
                .expect("quoted system schema")
                .table(&Identifier::quoted("pg_namespace"))
                .is_some()
        );
    }

    #[test]
    fn system_relations_are_not_serialized_or_registered_as_user_objects() {
        let catalog = Catalog::default();
        let encoded = serde_json::to_string(&catalog).expect("serialize catalog");
        assert!(!encoded.contains("pg_catalog"));
        assert!(!encoded.contains("information_schema"));
        for relation in system_relations() {
            assert!(
                !catalog
                    .object_refs()
                    .contains(&CatalogObjectRef::Table(relation.table_id))
            );
            assert!(
                catalog
                    .postgres_oid_registry()
                    .oid(PostgresOidObject::Table(relation.table_id))
                    .is_none()
            );
        }

        let reopened: Catalog = serde_json::from_str(&encoded).expect("reopen catalog");
        assert_eq!(reopened, catalog);
        assert!(reopened.table_by_id(PG_NAMESPACE_TABLE_ID).is_some());
    }

    #[test]
    fn system_catalog_mutations_fail_atomically_with_insufficient_privilege() {
        fn assert_read_only<T: std::fmt::Debug>(result: ordadb_types::Result<T>) {
            let error = result.expect_err("system catalog mutation must fail");
            assert_eq!(error.sql_state, "42501");
        }

        let mut catalog = Catalog::default();
        let before = catalog.clone();
        let schema = Identifier::unquoted("pg_catalog");
        let table_id = PG_NAMESPACE_TABLE_ID;
        let column_id = system_relation(table_id).expect("pg_namespace").columns[0].id;

        assert_read_only(catalog.create_schema(schema.clone()));
        assert_read_only(catalog.create_schema(Identifier::quoted("pg_catalog")));
        assert_read_only(catalog.rename_schema(
            PG_CATALOG_SCHEMA_ID,
            Identifier::unquoted("renamed_catalog"),
        ));
        assert_read_only(catalog.drop_schema(PG_CATALOG_SCHEMA_ID, DropBehavior::Cascade));
        assert_read_only(catalog.create_table(
            &schema,
            Identifier::unquoted("blocked"),
            vec![NewColumn::new(
                Identifier::unquoted("id"),
                ScalarType::Int64,
            )],
        ));
        assert_read_only(catalog.rename_table(table_id, Identifier::unquoted("blocked")));
        assert_read_only(catalog.drop_table(table_id, DropBehavior::Cascade));
        assert_read_only(catalog.rename_column(
            table_id,
            column_id,
            Identifier::unquoted("blocked"),
        ));
        assert_read_only(catalog.add_column(
            table_id,
            NewColumn::new(Identifier::unquoted("blocked"), ScalarType::Text),
        ));
        assert_read_only(catalog.alter_column(
            table_id,
            column_id,
            Some(ScalarType::Text),
            None,
            None,
            None,
        ));
        assert_read_only(catalog.drop_column(table_id, column_id, DropBehavior::Cascade));
        assert_read_only(catalog.create_index(
            table_id,
            NewIndex {
                name: Identifier::unquoted("blocked_idx"),
                key_columns: vec![Identifier::unquoted("oid")],
                include_columns: Vec::new(),
                unique: false,
                method: IndexMethod::BTree,
                options: IndexOptions::BTree,
            },
        ));
        assert_read_only(catalog.create_constraint(
            table_id,
            NewConstraint {
                name: Identifier::unquoted("blocked_check"),
                kind: NewConstraintKind::Check {
                    expression: CatalogExpression::new("true"),
                },
            },
        ));
        assert_read_only(catalog.create_sequence(
            &schema,
            NewSequence::new(Identifier::unquoted("blocked_seq")),
        ));
        assert_read_only(catalog.create_view(
            &schema,
            NewView {
                name: Identifier::unquoted("blocked_view"),
                kind: ViewKind::Regular,
                query: "SELECT 1".into(),
                output: Schema::empty(),
                materialized_table_id: None,
                populated: true,
                references: Vec::new(),
            },
        ));
        assert_read_only(catalog.create_or_replace_routine(
            &schema,
            NewRoutine {
                name: Identifier::unquoted("blocked_routine"),
                kind: RoutineKind::Function,
                arguments: Vec::new(),
                return_type: None,
                return_declared_type: None,
                returns_set: false,
                language: "plpgsql".into(),
                body: "BEGIN RETURN; END".into(),
                replace: false,
                references: Vec::new(),
            },
        ));
        assert_read_only(catalog.create_trigger(
            table_id,
            Identifier::unquoted("blocked_trigger"),
            TriggerTiming::Before,
            BTreeSet::new(),
            RoutineId::new(999),
        ));
        assert_read_only(catalog.set_table_statistics(table_id, TableStatistics::default()));
        assert_read_only(catalog.table_by_id_mut(table_id));

        assert_eq!(catalog, before);
    }

    #[test]
    fn legacy_serialized_system_names_remain_read_only_by_id() {
        let mut catalog = Catalog::default();
        let schema_id = SchemaId::new(99);
        let table_id = TableId::new(99);
        let mut table =
            TableDefinition::expression_scope(Identifier::unquoted("oid"), ScalarType::Int64);
        table.id = table_id;
        table.schema_id = schema_id;
        table.name = Identifier::unquoted("legacy_relation");
        let schema_name = Identifier::unquoted("pg_catalog");
        catalog.database.schemas.insert(
            schema_name.clone(),
            SchemaDefinition {
                id: schema_id,
                database_id: catalog.database.id,
                name: schema_name,
                tables: BTreeMap::from([(table.name.clone(), table)]),
                sequences: BTreeMap::new(),
                views: BTreeMap::new(),
                routines: BTreeMap::new(),
                types: BTreeMap::new(),
            },
        );
        let before = catalog.clone();

        let schema_error = catalog
            .rename_schema(schema_id, Identifier::unquoted("renamed"))
            .expect_err("legacy system schema rename");
        assert_eq!(schema_error.sql_state, "42501");
        let table_error = catalog
            .rename_table(table_id, Identifier::unquoted("renamed"))
            .expect_err("legacy system table rename");
        assert_eq!(table_error.sql_state, "42501");
        assert_eq!(catalog, before);
    }

    #[test]
    fn routine_modes_use_input_signatures_and_legacy_defaults() {
        let legacy: RoutineArgument = serde_json::from_value(serde_json::json!({
            "name": null,
            "data_type": "int64",
            "declared_type": null
        }))
        .expect("legacy routine argument");
        assert_eq!(legacy.mode, RoutineArgumentMode::In);

        let mut catalog = Catalog::default();
        let routine = |output_type| NewRoutine {
            name: Identifier::unquoted("mode_probe"),
            kind: RoutineKind::Procedure,
            arguments: vec![
                RoutineArgument {
                    name: Some(Identifier::unquoted("input_value")),
                    data_type: ScalarType::Int64,
                    declared_type: None,
                    mode: RoutineArgumentMode::In,
                },
                RoutineArgument {
                    name: Some(Identifier::unquoted("output_value")),
                    data_type: output_type,
                    declared_type: None,
                    mode: RoutineArgumentMode::Out,
                },
            ],
            return_type: None,
            return_declared_type: None,
            returns_set: false,
            language: "plpgsql".into(),
            body: "BEGIN RETURN; END".into(),
            replace: false,
            references: Vec::new(),
        };
        catalog
            .create_or_replace_routine(&Identifier::unquoted("public"), routine(ScalarType::Text))
            .expect("first input signature");
        let duplicate = catalog
            .create_or_replace_routine(&Identifier::unquoted("public"), routine(ScalarType::Int32))
            .expect_err("OUT type does not change the input signature");
        assert_eq!(duplicate.sql_state, "42723");
    }

    #[test]
    fn trigger_level_defaults_to_row_and_validates_activation() {
        let legacy: TriggerDefinition = serde_json::from_value(serde_json::json!({
            "id": 1,
            "table_id": 1,
            "name": "u:legacy_trigger",
            "timing": "before",
            "events": ["insert"],
            "routine_id": 1,
            "enabled": true
        }))
        .expect("legacy trigger definition");
        assert_eq!(legacy.level, TriggerLevel::Row);
        assert_eq!(legacy.target, TriggerTarget::Table(TableId::new(1)));

        let mut catalog = Catalog::default();
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("trigger_level_probe"),
                vec![NewColumn::new(
                    Identifier::unquoted("id"),
                    ScalarType::Int64,
                )],
            )
            .expect("table");
        let routine_id = catalog
            .create_or_replace_routine(
                &Identifier::unquoted("public"),
                NewRoutine {
                    name: Identifier::unquoted("trigger_level_fn"),
                    kind: RoutineKind::Function,
                    arguments: Vec::new(),
                    return_type: None,
                    return_declared_type: None,
                    returns_set: false,
                    language: "plpgsql".into(),
                    body: "BEGIN RETURN; END".into(),
                    replace: false,
                    references: Vec::new(),
                },
            )
            .expect("trigger routine");
        let trigger_id = catalog
            .create_trigger_with_level(
                table_id,
                Identifier::unquoted("statement_trigger"),
                TriggerTiming::AfterStatement,
                TriggerLevel::Statement,
                BTreeSet::from([TriggerEvent::Insert]),
                routine_id,
            )
            .expect("statement trigger");
        assert_eq!(
            catalog.trigger_by_id(trigger_id).expect("trigger").level,
            TriggerLevel::Statement
        );
        let invalid = catalog
            .create_trigger_with_level(
                table_id,
                Identifier::unquoted("invalid_trigger"),
                TriggerTiming::After,
                TriggerLevel::Statement,
                BTreeSet::from([TriggerEvent::Insert]),
                routine_id,
            )
            .expect_err("row timing cannot be statement level");
        assert_eq!(invalid.sql_state, "0A000");
    }

    #[test]
    fn regular_view_instead_of_triggers_round_trip_drop_with_owner_and_fail_closed() {
        let mut catalog = Catalog::default();
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("view_trigger_rows"),
                vec![NewColumn::new(
                    Identifier::unquoted("id"),
                    ScalarType::Int64,
                )],
            )
            .expect("table");
        let view_id = catalog
            .create_view(
                &Identifier::unquoted("public"),
                NewView {
                    name: Identifier::unquoted("view_trigger_target"),
                    kind: ViewKind::Regular,
                    query: "SELECT id FROM view_trigger_rows".into(),
                    output: Schema::new(vec![ordadb_types::Field::new(
                        "id",
                        ScalarType::Int64,
                        false,
                    )]),
                    materialized_table_id: None,
                    populated: true,
                    references: vec![CatalogObjectRef::Table(table_id)],
                },
            )
            .expect("view");
        let routine_id = catalog
            .create_or_replace_routine(
                &Identifier::unquoted("public"),
                NewRoutine {
                    name: Identifier::unquoted("view_trigger_fn"),
                    kind: RoutineKind::Function,
                    arguments: Vec::new(),
                    return_type: None,
                    return_declared_type: None,
                    returns_set: false,
                    language: "plpgsql".into(),
                    body: "BEGIN RETURN NEW; END".into(),
                    replace: false,
                    references: Vec::new(),
                },
            )
            .expect("routine");
        let trigger_id = catalog
            .create_trigger_on_target_with_level(
                TriggerTarget::View(view_id),
                Identifier::unquoted("view_trigger"),
                TriggerTiming::InsteadOf,
                TriggerLevel::Row,
                BTreeSet::from([TriggerEvent::Insert]),
                routine_id,
            )
            .expect("view trigger");
        assert_eq!(
            catalog.trigger_by_id(trigger_id).expect("trigger").target,
            TriggerTarget::View(view_id)
        );

        let encoded = serde_json::to_value(&catalog).expect("serialize catalog");
        let reopened: Catalog = serde_json::from_value(encoded.clone()).expect("reopen catalog");
        assert_eq!(reopened, catalog);

        let mut downgraded = encoded;
        downgraded["database"]["schemas"]["u:public"]["views"]["u:view_trigger_target"]
            .as_object_mut()
            .expect("view object")
            .remove("triggers");
        let error = serde_json::from_value::<Catalog>(downgraded)
            .expect_err("old projection must not silently discard a view trigger");
        assert!(error.to_string().contains("OID registry"));

        let removed = catalog
            .drop_view(view_id, DropBehavior::Restrict)
            .expect("owned view trigger drops with view");
        assert!(removed.contains(&CatalogObjectRef::Trigger(trigger_id)));
        assert!(catalog.trigger_by_id(trigger_id).is_none());
        assert!(catalog.view_by_id(view_id).is_none());
    }
}
