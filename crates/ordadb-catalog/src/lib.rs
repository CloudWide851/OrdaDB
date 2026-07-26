use std::collections::{BTreeMap, BTreeSet};

use ordadb_types::{
    ColumnId, ConstraintId, DatabaseId, DbError, Identifier, IndexId, Result, RoutineId,
    ScalarType, Schema, SchemaId, SequenceId, TableId, TriggerId, Value, ViewId,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

const MAX_DEPENDENCY_OBJECTS: usize = 16_384;

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
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    #[serde(default)]
    pub default: Option<CatalogExpression>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineArgument {
    pub name: Option<Identifier>,
    pub data_type: ScalarType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineDefinition {
    pub id: RoutineId,
    pub schema_id: SchemaId,
    pub name: Identifier,
    pub kind: RoutineKind,
    pub arguments: Vec<RoutineArgument>,
    pub return_type: Option<ScalarType>,
    pub returns_set: bool,
    pub language: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRoutine {
    pub name: Identifier,
    pub kind: RoutineKind,
    pub arguments: Vec<RoutineArgument>,
    pub return_type: Option<ScalarType>,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerDefinition {
    pub id: TriggerId,
    pub table_id: TableId,
    pub name: Identifier,
    pub timing: TriggerTiming,
    pub events: BTreeSet<TriggerEvent>,
    pub routine_id: RoutineId,
    pub enabled: bool,
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
}

impl SchemaDefinition {
    pub fn tables(&self) -> impl Iterator<Item = &TableDefinition> {
        self.tables.values()
    }

    #[must_use]
    pub fn table(&self, name: &Identifier) -> Option<&TableDefinition> {
        self.tables.get(name)
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    #[serde(default)]
    dependencies: DependencyGraph,
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
            dependencies: DependencyGraph::default(),
        }
    }

    #[must_use]
    pub const fn database(&self) -> &DatabaseDefinition {
        &self.database
    }

    #[must_use]
    pub fn schema(&self, name: &Identifier) -> Option<&SchemaDefinition> {
        self.database.schema(name)
    }

    #[must_use]
    pub fn schema_by_id(&self, schema_id: SchemaId) -> Option<&SchemaDefinition> {
        self.database
            .schemas()
            .find(|schema| schema.id == schema_id)
    }

    #[must_use]
    pub const fn dependencies(&self) -> &DependencyGraph {
        &self.dependencies
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
        if self.database.schemas.contains_key(&name) {
            return Err(DbError::new(
                "42P06",
                format!("schema {name} already exists"),
            ));
        }

        let id = SchemaId::new(self.next_schema_id);
        self.next_schema_id += 1;
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
            },
        );
        Ok(id)
    }

    pub fn rename_schema(&mut self, schema_id: SchemaId, new_name: Identifier) -> Result<()> {
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
        let schema = self
            .schema_by_id(schema_id)
            .ok_or_else(|| DbError::new("3F000", "schema does not exist"))?;
        let is_empty = schema.tables.is_empty()
            && schema.sequences.is_empty()
            && schema.views.is_empty()
            && schema.routines.is_empty();
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
        removed.push(CatalogObjectRef::Schema(schema_id));
        Ok(removed)
    }

    pub fn create_table(
        &mut self,
        schema_name: &Identifier,
        table_name: Identifier,
        columns: Vec<NewColumn>,
    ) -> Result<TableId> {
        if columns.is_empty() {
            return Err(DbError::new(
                "42601",
                "a table must contain at least one column",
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
        Ok(table_id)
    }

    #[must_use]
    pub fn table(
        &self,
        schema_name: &Identifier,
        table_name: &Identifier,
    ) -> Option<&TableDefinition> {
        self.schema(schema_name)?.table(table_name)
    }

    #[must_use]
    pub fn table_by_id(&self, table_id: TableId) -> Option<&TableDefinition> {
        self.database
            .schemas()
            .flat_map(SchemaDefinition::tables)
            .find(|table| table.id == table_id)
    }

    pub fn rename_table(&mut self, table_id: TableId, new_name: Identifier) -> Result<()> {
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
        self.next_column_id = self.next_column_id.saturating_add(1);
        self.table_by_id_mut(table_id)?
            .columns
            .push(ColumnDefinition {
                id: column_id,
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable && !column.primary_key,
                primary_key: column.primary_key,
                unique: column.unique || column.primary_key,
                default: column.default,
            });
        Ok(column_id)
    }

    pub fn alter_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        data_type: Option<ScalarType>,
        nullable: Option<bool>,
        default: Option<Option<CatalogExpression>>,
    ) -> Result<()> {
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
        Ok(())
    }

    pub fn drop_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
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
        self.next_index_id += 1;
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

        self.next_constraint_id = self.next_constraint_id.saturating_add(1);
        if creates_index {
            let index_id = IndexId::new(self.next_index_id);
            self.next_index_id = self.next_index_id.saturating_add(1);
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
        self.dependencies = dependencies;
        Ok(id)
    }

    pub fn create_sequence(
        &mut self,
        schema_name: &Identifier,
        sequence: NewSequence,
    ) -> Result<SequenceId> {
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
        let object = CatalogObjectRef::Sequence(id);
        let mut dependencies = self.dependencies.clone();
        if let Some((table_id, column_id)) = sequence.owner {
            dependencies.add(object, CatalogObjectRef::Table(table_id))?;
            dependencies.add(object, CatalogObjectRef::Column(table_id, column_id))?;
        }
        self.next_sequence_id = self.next_sequence_id.saturating_add(1);
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
        self.dependencies = dependencies;
        Ok(id)
    }

    pub fn create_view(&mut self, schema_name: &Identifier, view: NewView) -> Result<ViewId> {
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
        let object = CatalogObjectRef::View(id);
        let mut dependencies = self.dependencies.clone();
        for referenced in references {
            dependencies.add(object, referenced)?;
        }
        if let Some(table_id) = materialized_table_id {
            dependencies.add(object, CatalogObjectRef::Table(table_id))?;
        }
        self.next_view_id = self.next_view_id.saturating_add(1);
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
            },
        );
        self.dependencies = dependencies;
        Ok(id)
    }

    pub fn create_or_replace_routine(
        &mut self,
        schema_name: &Identifier,
        routine: NewRoutine,
    ) -> Result<RoutineId> {
        let NewRoutine {
            name,
            kind,
            arguments,
            return_type,
            returns_set,
            language,
            body,
            replace,
            references,
        } = routine;
        let signature = arguments
            .iter()
            .map(|argument| argument.data_type.clone())
            .collect::<Vec<_>>();
        let (schema_id, existing_id) = {
            let schema = self.schema(schema_name).ok_or_else(|| {
                DbError::new("3F000", format!("schema {schema_name} does not exist"))
            })?;
            let existing_id = schema
                .routines_named(&name)
                .iter()
                .find(|routine| {
                    routine.kind == kind
                        && routine
                            .arguments
                            .iter()
                            .map(|argument| argument.data_type.clone())
                            .eq(signature.iter().cloned())
                })
                .map(|routine| routine.id);
            (schema.id, existing_id)
        };
        if existing_id.is_some() && !replace {
            return Err(DbError::new(
                "42723",
                format!("routine {name} with this signature already exists"),
            ));
        }

        let (id, old_object) = existing_id
            .map(|routine_id| (routine_id, Some(CatalogObjectRef::Routine(routine_id))))
            .unwrap_or_else(|| (RoutineId::new(self.next_routine_id), None));
        let object = CatalogObjectRef::Routine(id);
        let mut dependencies = self.dependencies.clone();
        if let Some(old_object) = old_object {
            dependencies.remove(old_object);
        }
        for referenced in references {
            dependencies.add(object, referenced)?;
        }
        if existing_id.is_none() {
            self.next_routine_id = self.next_routine_id.saturating_add(1);
        }
        let routines = self
            .schema_by_id_mut(schema_id)?
            .routines
            .entry(name.clone())
            .or_default();
        routines.retain(|routine| {
            !(routine.kind == kind
                && routine
                    .arguments
                    .iter()
                    .map(|argument| &argument.data_type)
                    .eq(signature.iter()))
        });
        routines.push(RoutineDefinition {
            id,
            schema_id,
            name,
            kind,
            arguments,
            return_type,
            returns_set,
            language,
            body,
        });
        routines.sort_by_key(|routine| routine.id);
        self.dependencies = dependencies;
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
        if events.is_empty() {
            return Err(DbError::new(
                "42601",
                "a trigger must contain at least one event",
            ));
        }
        let table = self
            .table_by_id(table_id)
            .ok_or_else(|| DbError::new("42P01", "trigger owner table does not exist"))?;
        if table.trigger(&name).is_some() {
            return Err(DbError::new(
                "42710",
                format!("trigger {name} already exists"),
            ));
        }
        if self.routine_by_id(routine_id).is_none() {
            return Err(DbError::new("42883", "trigger routine does not exist"));
        }
        let id = TriggerId::new(self.next_trigger_id);
        let object = CatalogObjectRef::Trigger(id);
        let mut dependencies = self.dependencies.clone();
        dependencies.add(object, CatalogObjectRef::Table(table_id))?;
        dependencies.add(object, CatalogObjectRef::Routine(routine_id))?;
        self.next_trigger_id = self.next_trigger_id.saturating_add(1);
        self.table_by_id_mut(table_id)?.triggers.insert(
            name.clone(),
            TriggerDefinition {
                id,
                table_id,
                name,
                timing,
                events,
                routine_id,
                enabled: true,
            },
        );
        self.dependencies = dependencies;
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
        self.drop_catalog_object(CatalogObjectRef::View(view_id), behavior)
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
            .flat_map(SchemaDefinition::tables)
            .flat_map(TableDefinition::triggers)
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
        self.database
            .schemas
            .values_mut()
            .flat_map(|schema| schema.tables.values_mut())
            .find(|table| table.id == table_id)
            .ok_or_else(|| DbError::new("42P01", "table does not exist"))
    }

    fn schema_by_id_mut(&mut self, schema_id: SchemaId) -> Result<&mut SchemaDefinition> {
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
        self.database
            .schemas
            .values_mut()
            .flat_map(|schema| schema.tables.values_mut())
            .flat_map(|table| table.triggers.values_mut())
            .find(|trigger| trigger.id == trigger_id)
            .ok_or_else(|| DbError::new("42704", "trigger does not exist"))
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
                .is_some_and(|trigger| trigger.table_id == table_id),
            _ => false,
        }
    }

    fn drop_catalog_object(
        &mut self,
        root: CatalogObjectRef,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        let order = self.dependencies.drop_order(root, behavior)?;
        for object in &order {
            self.remove_catalog_object(*object)?;
        }
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
                }
            }
            CatalogObjectRef::Column(table_id, column_id) => {
                let table = self.table_by_id_mut(table_id)?;
                table.columns.retain(|column| column.id != column_id);
                table.indexes.retain(|_, index| {
                    !index.key_columns.contains(&column_id)
                        && !index.include_columns.contains(&column_id)
                });
                table.constraints.retain(|_, constraint| {
                    !constraint_columns(&constraint.kind).any(|candidate| candidate == column_id)
                });
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
                        let index_names = table
                            .constraints
                            .values()
                            .filter(|constraint| constraint.id == constraint_id)
                            .map(|constraint| constraint.name.clone())
                            .collect::<Vec<_>>();
                        table
                            .constraints
                            .retain(|_, constraint| constraint.id != constraint_id);
                        for name in index_names {
                            table.indexes.remove(&name);
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
                }
            }
        }
        self.dependencies.remove(object);
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
    use std::collections::BTreeSet;

    use ordadb_types::{Identifier, ScalarType, Schema, SchemaId, TableId};

    use super::{
        Catalog, CatalogObjectRef, ConstraintKind, DependencyGraph, DropBehavior, FullTextAnalyzer,
        IndexDefinition, IndexMethod, IndexOptions, NewColumn, NewConstraint, NewConstraintKind,
        NewIndex, NewRoutine, NewSequence, NewView, ReferentialAction, RoutineArgument,
        RoutineKind, TriggerEvent, TriggerTiming, VectorDistanceMetric, ViewKind,
    };

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
                    }],
                    return_type: Some(ScalarType::Int64),
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
}
