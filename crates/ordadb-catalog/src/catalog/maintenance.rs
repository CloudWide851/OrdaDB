impl Catalog {
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
    include!("tests_ownership.rs");
    include!("tests_objects.rs");
    include!("tests_oids.rs");
}
