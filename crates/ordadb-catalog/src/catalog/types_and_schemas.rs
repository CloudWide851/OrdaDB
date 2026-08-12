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
}
