
fn execute_drop_objects(
    state: &mut DatabaseState,
    kind: DdlObjectKind,
    objects: Vec<CatalogObjectRef>,
    behavior: DropBehavior,
) -> Result<(Vec<QueryEvent>, bool)> {
    if objects.is_empty() {
        return Ok((
            command_events(Schema::empty(), drop_command_tag(kind), 0, None),
            false,
        ));
    }
    let catalog_before = Arc::clone(&state.catalog);
    let mut removed = Vec::new();
    for object in objects {
        let dropped = drop_catalog_root(Arc::make_mut(&mut state.catalog), object, behavior)?;
        for object in dropped {
            if !removed.contains(&object) {
                removed.push(object);
            }
        }
    }

    let backing_tables = removed
        .iter()
        .filter_map(|object| match object {
            CatalogObjectRef::View(view_id) => catalog_before
                .view_by_id(*view_id)
                .and_then(|view| view.materialized_table_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    for table_id in backing_tables {
        if state.catalog.table_by_id(table_id).is_some() {
            for object in
                Arc::make_mut(&mut state.catalog).drop_table(table_id, DropBehavior::Cascade)?
            {
                if !removed.contains(&object) {
                    removed.push(object);
                }
            }
        }
    }
    cleanup_removed_columns(state, &catalog_before, &removed)?;
    cleanup_removed_state(state, &removed);
    reconcile_search_catalog(state)?;
    Ok((
        command_events(Schema::empty(), drop_command_tag(kind), 0, None),
        true,
    ))
}

fn drop_catalog_root(
    catalog: &mut Catalog,
    object: CatalogObjectRef,
    behavior: DropBehavior,
) -> Result<Vec<CatalogObjectRef>> {
    match object {
        CatalogObjectRef::Schema(id) if catalog.schema_by_id(id).is_some() => {
            catalog.drop_schema(id, behavior)
        }
        CatalogObjectRef::Table(id) if catalog.table_by_id(id).is_some() => {
            catalog.drop_table(id, behavior)
        }
        CatalogObjectRef::Index(id) if catalog.index_by_id(id).is_some() => {
            catalog.drop_index(id, behavior)
        }
        CatalogObjectRef::Sequence(id) if catalog.sequence_by_id(id).is_some() => {
            catalog.drop_sequence(id, behavior)
        }
        CatalogObjectRef::View(id) if catalog.view_by_id(id).is_some() => {
            catalog.drop_view(id, behavior)
        }
        CatalogObjectRef::Constraint(id) if catalog.constraint_by_id(id).is_some() => {
            catalog.drop_constraint(id, behavior)
        }
        CatalogObjectRef::Routine(id) if catalog.routine_by_id(id).is_some() => {
            catalog.drop_routine(id, behavior)
        }
        CatalogObjectRef::Trigger(id) if catalog.trigger_by_id(id).is_some() => {
            catalog.drop_trigger(id, behavior)
        }
        CatalogObjectRef::Type(id) if catalog.type_by_id(id).is_some() => {
            catalog.drop_type(id, behavior)
        }
        CatalogObjectRef::Column(_, _) => Err(internal_error(
            "column drops must be routed through ALTER TABLE",
        )),
        _ => Ok(Vec::new()),
    }
}

fn cleanup_removed_state(state: &mut DatabaseState, removed: &[CatalogObjectRef]) {
    for object in removed {
        match object {
            CatalogObjectRef::Table(table_id) => {
                state.rows.remove(table_id);
            }
            CatalogObjectRef::Index(index_id) => {
                state.indexes.remove(index_id);
            }
            _ => {}
        }
    }
    state
        .rows
        .retain(|table_id, _| state.catalog.table_by_id(*table_id).is_some());
    state
        .indexes
        .retain(|index_id, _| state.catalog.index_by_id(*index_id).is_some());
}

fn cleanup_removed_columns(
    state: &mut DatabaseState,
    catalog_before: &Catalog,
    removed: &[CatalogObjectRef],
) -> Result<()> {
    let mut positions_by_table = BTreeMap::<TableId, Vec<usize>>::new();
    for object in removed {
        let CatalogObjectRef::Column(table_id, column_id) = object else {
            continue;
        };
        if state.catalog.table_by_id(*table_id).is_none() {
            continue;
        }
        let position = catalog_before
            .table_by_id(*table_id)
            .and_then(|table| table.column_index_by_id(*column_id))
            .ok_or_else(|| internal_error("dropped column is absent from the prior catalog"))?;
        positions_by_table
            .entry(*table_id)
            .or_default()
            .push(position);
    }
    for (table_id, positions) in &mut positions_by_table {
        positions.sort_unstable_by(|left, right| right.cmp(left));
        positions.dedup();
        for row in Arc::make_mut(
            state
                .rows
                .entry(*table_id)
                .or_insert_with(|| Arc::new(Vec::new())),
        ) {
            for position in positions.iter().copied() {
                if position >= row.values.len() {
                    return Err(internal_error(
                        "dropped column position exceeds the stored row width",
                    ));
                }
                row.values.remove(position);
            }
        }
    }
    Ok(())
}

fn drop_command_tag(kind: DdlObjectKind) -> &'static str {
    match kind {
        DdlObjectKind::Schema => "DROP SCHEMA",
        DdlObjectKind::Table => "DROP TABLE",
        DdlObjectKind::Index => "DROP INDEX",
        DdlObjectKind::Sequence => "DROP SEQUENCE",
        DdlObjectKind::View => "DROP VIEW",
        DdlObjectKind::MaterializedView => "DROP MATERIALIZED VIEW",
        DdlObjectKind::Type => "DROP TYPE",
    }
}

fn rewrite_enum_values(
    state: &mut DatabaseState,
    type_id: TypeId,
    renamed_label: Option<(&str, &str)>,
) -> Result<()> {
    let affected = state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .filter_map(|table| {
            let columns = table
                .columns()
                .iter()
                .enumerate()
                .filter(|(_, column)| {
                    column.declared_type.is_some_and(|declared_type| {
                        declared_type == type_id
                            || state
                                .catalog
                                .type_by_id(declared_type)
                                .is_some_and(|definition| {
                                    matches!(
                                        definition.definition,
                                        UserDefinedTypeKind::Domain {
                                            base_declared_type: Some(base_type_id),
                                            ..
                                        } if base_type_id == type_id
                                    )
                                })
                    })
                })
                .map(|(index, column)| (index, column.data_type.clone()))
                .collect::<Vec<_>>();
            (!columns.is_empty()).then_some((table.id, columns))
        })
        .collect::<Vec<_>>();
    for (table_id, columns) in affected {
        for row in Arc::make_mut(
            state
                .rows
                .entry(table_id)
                .or_insert_with(|| Arc::new(Vec::new())),
        ) {
            for (index, data_type) in &columns {
                let value = row.values.get_mut(*index).ok_or_else(|| {
                    internal_error("enum column position exceeds the stored row width")
                })?;
                rewrite_enum_column_value(value, data_type, renamed_label)?;
            }
        }
        rebuild_table_derived(state, table_id)?;
    }
    validate_database_rows(state)
}

fn rewrite_enum_column_value(
    value: &mut Value,
    data_type: &ScalarType,
    renamed_label: Option<(&str, &str)>,
) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        Value::Text(label) => {
            if let Some((old_label, new_label)) = renamed_label
                && label == old_label
            {
                *label = new_label.to_owned();
            }
            Ok(())
        }
        Value::Array(array) => {
            let ScalarType::Array { element } = data_type else {
                return Err(internal_error(
                    "enum array value is paired with a non-array declared type",
                ));
            };
            let mut values = array.values().to_vec();
            for value in &mut values {
                if let (Value::Text(label), Some((old_label, new_label))) = (value, renamed_label)
                    && label == old_label
                {
                    *label = new_label.to_owned();
                }
            }
            *array = PgArray::new((**element).clone(), array.dimensions().to_vec(), values)?;
            Ok(())
        }
        _ => Err(DbError::new(
            "42804",
            "stored value is not assignable to the altered enum type",
        )),
    }
}

fn execute_alter_table(
    state: &mut DatabaseState,
    table_id: TableId,
    operations: Vec<BoundAlterTableOperation>,
) -> Result<(Vec<QueryEvent>, bool)> {
    for operation in operations {
        match operation {
            BoundAlterTableOperation::RenameTable { new_name } => {
                Arc::make_mut(&mut state.catalog).rename_table(table_id, new_name)?;
            }
            BoundAlterTableOperation::RenameColumn {
                column_id,
                new_name,
            } => {
                Arc::make_mut(&mut state.catalog).rename_column(table_id, column_id, new_name)?;
            }
            BoundAlterTableOperation::AddColumn {
                column,
                if_not_exists,
            } => {
                if table_definition(state, table_id)?
                    .column(&column.name)
                    .is_some()
                    && if_not_exists
                {
                    continue;
                }
                let value = new_column_default_value(&state.catalog, &column)?;
                Arc::make_mut(&mut state.catalog).add_column(table_id, column)?;
                for row in Arc::make_mut(
                    state
                        .rows
                        .entry(table_id)
                        .or_insert_with(|| Arc::new(Vec::new())),
                ) {
                    row.values.push(value.clone());
                }
            }
            BoundAlterTableOperation::DropColumns {
                column_ids,
                if_exists: _,
                behavior,
            } => {
                let table = table_definition(state, table_id)?.clone();
                let mut positions = column_ids
                    .iter()
                    .filter_map(|column_id| table.column_index_by_id(*column_id))
                    .collect::<Vec<_>>();
                for column_id in column_ids {
                    if state
                        .catalog
                        .table_by_id(table_id)
                        .is_some_and(|table| table.column_index_by_id(column_id).is_some())
                    {
                        Arc::make_mut(&mut state.catalog)
                            .drop_column(table_id, column_id, behavior)?;
                    }
                }
                positions.sort_unstable_by(|left, right| right.cmp(left));
                for row in Arc::make_mut(
                    state
                        .rows
                        .entry(table_id)
                        .or_insert_with(|| Arc::new(Vec::new())),
                ) {
                    for position in &positions {
                        row.values.remove(*position);
                    }
                }
            }
            BoundAlterTableOperation::SetNotNull { column_id } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    Some(false),
                    None,
                    None,
                )?;
            }
            BoundAlterTableOperation::DropNotNull { column_id } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    Some(true),
                    None,
                    None,
                )?;
            }
            BoundAlterTableOperation::SetDefault { column_id, default } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    None,
                    Some(Some(default)),
                    None,
                )?;
            }
            BoundAlterTableOperation::DropDefault { column_id } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    None,
                    Some(None),
                    None,
                )?;
            }
            BoundAlterTableOperation::SetDataType {
                column_id,
                data_type,
                declared_type,
            } => {
                let position = table_definition(state, table_id)?
                    .column_index_by_id(column_id)
                    .ok_or_else(|| DbError::new("42703", "column does not exist"))?;
                for row in Arc::make_mut(
                    state
                        .rows
                        .entry(table_id)
                        .or_insert_with(|| Arc::new(Vec::new())),
                ) {
                    row.values[position] =
                        coerce_execution_value(row.values[position].clone(), &data_type)?;
                }
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    Some(data_type),
                    None,
                    None,
                    Some(declared_type),
                )?;
            }
            BoundAlterTableOperation::AddConstraint { constraint } => {
                Arc::make_mut(&mut state.catalog).create_constraint(table_id, constraint)?;
            }
            BoundAlterTableOperation::DropConstraint {
                constraint_id,
                if_exists: _,
                behavior,
            } => {
                if let Some(constraint_id) = constraint_id {
                    let removed = Arc::make_mut(&mut state.catalog)
                        .drop_constraint(constraint_id, behavior)?;
                    cleanup_removed_state(state, &removed);
                }
            }
            BoundAlterTableOperation::SetTriggerEnabled {
                trigger_id,
                name,
                enabled,
            } => {
                let trigger_id = trigger_id.ok_or_else(|| {
                    DbError::new("42704", format!("trigger {name} does not exist"))
                })?;
                Arc::make_mut(&mut state.catalog).set_trigger_enabled(trigger_id, enabled)?;
            }
        }
    }
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    Ok((
        command_events(Schema::empty(), "ALTER TABLE", 0, None),
        true,
    ))
}

fn catalog_default_value(
    catalog: &Catalog,
    expression: Option<&CatalogExpression>,
    data_type: &ScalarType,
) -> Result<Value> {
    let Some(expression) = expression else {
        return Ok(Value::Null);
    };
    let bound = bind_catalog_expression_with_catalog(expression, None, Some(data_type), catalog)?;
    evaluate_scalar(&bound, &[], &[])
}

fn column_default_value(catalog: &Catalog, column: &ColumnDefinition) -> Result<Value> {
    declared_column_default_value(
        catalog,
        column.default.as_ref(),
        column.declared_type,
        &column.data_type,
    )
}

fn new_column_default_value(catalog: &Catalog, column: &NewColumn) -> Result<Value> {
    declared_column_default_value(
        catalog,
        column.default.as_ref(),
        column.declared_type,
        &column.data_type,
    )
}

fn declared_column_default_value(
    catalog: &Catalog,
    explicit_default: Option<&CatalogExpression>,
    declared_type: Option<TypeId>,
    data_type: &ScalarType,
) -> Result<Value> {
    let domain_default =
        if explicit_default.is_none() && !matches!(data_type, ScalarType::Array { .. }) {
            declared_type
                .and_then(|type_id| catalog.type_by_id(type_id))
                .and_then(|definition| match &definition.definition {
                    UserDefinedTypeKind::Domain { default, .. } => default.as_ref(),
                    UserDefinedTypeKind::Enum { .. } => None,
                })
        } else {
            None
        };
    catalog_default_value(catalog, explicit_default.or(domain_default), data_type)
}

fn execute_create_view(
    state: &mut DatabaseState,
    view: CreateViewExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let tag = match view.kind {
        ViewKind::Regular => "CREATE VIEW",
        ViewKind::Materialized => "CREATE MATERIALIZED VIEW",
    };
    if view.existing.is_some() && view.if_not_exists && !view.replace {
        return Ok((command_events(Schema::empty(), tag, 0, None), false));
    }
    let materialized_rows = if view.kind == ViewKind::Materialized && view.with_data {
        Some(materialize_statement_rows(
            state,
            view.query.clone(),
            params,
        )?)
    } else {
        None
    };

    if let Some(view_id) = view.existing {
        let current = state
            .catalog
            .view_by_id(view_id)
            .cloned()
            .ok_or_else(|| DbError::new("42P01", "view does not exist"))?;
        if current.kind != view.kind {
            return Err(DbError::new(
                "42809",
                "cannot replace a view with a different relation kind",
            ));
        }
        Arc::make_mut(&mut state.catalog).replace_view(
            view_id,
            view.query_sql,
            view.output,
            view.kind == ViewKind::Regular || view.with_data,
            view.references,
        )?;
        if let Some(table_id) = current.materialized_table_id {
            state
                .rows
                .insert(table_id, Arc::new(materialized_rows.unwrap_or_default()));
            rebuild_table_derived(state, table_id)?;
        }
        return Ok((command_events(Schema::empty(), tag, 0, None), true));
    }

    let materialized_table_id = if view.kind == ViewKind::Materialized {
        let backing_name = Identifier::unquoted(format!("__ordadb_mv_{}", view.name.as_str()));
        let columns = view
            .output
            .fields
            .iter()
            .map(|field| NewColumn {
                name: Identifier::unquoted(field.name.clone()),
                data_type: field.data_type.clone(),
                declared_type: None,
                nullable: field.nullable,
                primary_key: false,
                unique: false,
                default: None,
            })
            .collect();
        let table_id =
            Arc::make_mut(&mut state.catalog).create_table(&view.schema, backing_name, columns)?;
        state
            .rows
            .insert(table_id, Arc::new(materialized_rows.unwrap_or_default()));
        rebuild_table_derived(state, table_id)?;
        Some(table_id)
    } else {
        None
    };
    Arc::make_mut(&mut state.catalog).create_view(
        &view.schema,
        NewView {
            name: view.name,
            kind: view.kind,
            query: view.query_sql,
            output: view.output,
            materialized_table_id,
            populated: view.kind == ViewKind::Regular || view.with_data,
            references: view.references,
        },
    )?;
    Ok((command_events(Schema::empty(), tag, 0, None), true))
}

fn materialize_statement_rows(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
) -> Result<Vec<Row>> {
    let (events, dirty) = execute_bound(state, statement, params)?;
    if dirty {
        return Err(internal_error(
            "a materialized query attempted to mutate database state",
        ));
    }
    Ok(events
        .into_iter()
        .filter_map(|event| match event {
            QueryEvent::Batch(batch) => Some(batch.rows),
            _ => None,
        })
        .flatten()
        .collect())
}

struct EnginePlpgsqlHost<'a> {
    state: &'a mut DatabaseState,
    trigger: Option<&'a mut TriggerRowContext>,
    exception_states: Vec<DatabaseState>,
    exception_triggers: Vec<Option<TriggerRowSavepoint>>,
    exception_charges: Vec<usize>,
    exception_memory: VmMemoryReservation,
    sql_dirty: bool,
}

fn estimated_btree_clone_bytes<K, V>(len: usize) -> usize {
    len.saturating_mul(
        std::mem::size_of::<(K, V)>().saturating_add(4 * std::mem::size_of::<usize>()),
    )
}

fn estimated_database_state_snapshot_bytes(state: &DatabaseState) -> Result<usize> {
    let routine_frames = state
        .routine_frames
        .arena
        .capacity()
        .saturating_mul(std::mem::size_of::<Option<RoutineFrame>>())
        .saturating_add(
            state
                .routine_frames
                .free
                .capacity()
                .saturating_mul(std::mem::size_of::<usize>()),
        )
        .saturating_add(
            state
                .routine_frames
                .active
                .capacity()
                .saturating_mul(std::mem::size_of::<RoutineFrameId>()),
        );
    let notices = state
        .pending_notices
        .capacity()
        .saturating_mul(std::mem::size_of::<DbNotice>())
        .saturating_add(
            state
                .pending_notices
                .iter()
                .map(|notice| {
                    notice
                        .sql_state
                        .capacity()
                        .saturating_add(notice.message.capacity())
                        .saturating_add(notice.detail.as_ref().map_or(0, |value| value.len()))
                        .saturating_add(notice.hint.as_ref().map_or(0, |value| value.len()))
                })
                .sum::<usize>(),
        );
    let listener_actions = state
        .pending_notifications
        .listener_actions
        .capacity()
        .saturating_mul(std::mem::size_of::<NotificationListenerAction>())
        .saturating_add(
            state
                .pending_notifications
                .listener_actions
                .iter()
                .map(|action| match action {
                    NotificationListenerAction::Listen(channel)
                    | NotificationListenerAction::Unlisten(channel) => channel.as_str().len(),
                    NotificationListenerAction::UnlistenAll => 0,
                })
                .sum::<usize>(),
        );
    let notifications = state
        .pending_notifications
        .notifications
        .capacity()
        .saturating_mul(std::mem::size_of::<(Identifier, String)>())
        .saturating_add(
            state
                .pending_notifications
                .notifications
                .iter()
                .map(|(channel, payload)| channel.as_str().len().saturating_add(payload.capacity()))
                .sum::<usize>(),
        );
    let coalesced = state
        .pending_notifications
        .coalesced
        .iter()
        .map(|(channel, payload)| {
            std::mem::size_of::<(Identifier, String)>()
                .saturating_add(channel.as_str().len())
                .saturating_add(payload.capacity())
                .saturating_add(4 * std::mem::size_of::<usize>())
        })
        .sum::<usize>();
    let total = std::mem::size_of::<DatabaseState>()
        .saturating_add(estimated_btree_clone_bytes::<TableId, Arc<Vec<Row>>>(
            state.rows.len(),
        ))
        .saturating_add(
            estimated_btree_clone_bytes::<TableId, Arc<Vec<VersionedRow>>>(state.versions.len()),
        )
        .saturating_add(estimated_btree_clone_bytes::<TableId, Arc<Vec<u32>>>(
            state.visible_versions.len(),
        ))
        .saturating_add(estimated_btree_clone_bytes::<IndexId, Arc<BPlusTree>>(
            state.indexes.len(),
        ))
        .saturating_add(estimated_btree_clone_bytes::<SequenceId, i64>(
            state.sequence_currvals.len(),
        ))
        .saturating_add(routine_frames)
        .saturating_add(notices)
        .saturating_add(listener_actions)
        .saturating_add(notifications)
        .saturating_add(coalesced);
    if total == usize::MAX {
        return Err(DbError::new(
            "53200",
            "PL/pgSQL exception savepoint memory accounting overflowed",
        ));
    }
    Ok(total)
}

fn estimated_trigger_savepoint_bytes(trigger: Option<&TriggerRowContext>) -> usize {
    trigger.map_or(0, |trigger| {
        std::mem::size_of::<TriggerRowSavepoint>()
            .saturating_add(trigger.old.as_ref().map_or(0, estimated_row_bytes))
            .saturating_add(trigger.new.as_ref().map_or(0, estimated_row_bytes))
    })
}
