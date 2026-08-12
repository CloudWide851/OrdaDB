
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
