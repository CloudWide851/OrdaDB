impl Catalog {
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
}
