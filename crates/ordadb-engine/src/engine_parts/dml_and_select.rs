
fn ensure_statement_not_cancelled(state: &DatabaseState) -> Result<()> {
    if state
        .cancellation
        .as_ref()
        .is_some_and(|cancellation| cancellation.load(Ordering::Acquire))
    {
        Err(DbError::new("57014", "query was cancelled"))
    } else {
        Ok(())
    }
}

fn execute_insert(
    state: &mut DatabaseState,
    table_id: TableId,
    column_indexes: Vec<usize>,
    expressions: Vec<Vec<BoundExpr>>,
    on_conflict: Option<BoundOnConflict>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let table = table_definition(state, table_id)?.clone();
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::BeforeStatement,
        TriggerEvent::Insert,
    )?;
    let conflict_update = matches!(
        on_conflict.as_ref().map(|conflict| &conflict.action),
        Some(BoundConflictAction::DoUpdate { .. })
    );
    if conflict_update {
        fire_statement_triggers(
            state,
            table_id,
            TriggerTiming::BeforeStatement,
            TriggerEvent::Update,
        )?;
    }
    let mut affected = 0u64;
    let mut returned_rows = Vec::new();
    let mut command_affected_rows = BTreeSet::new();
    let conflict_memory = MemoryGrant::new(DEFAULT_SOFT_MEMORY_BYTES, DEFAULT_HARD_MEMORY_BYTES)?;
    let mut conflict_reservation = conflict_memory.try_reserve(0)?;
    for expressions in expressions {
        let mut values = table
            .columns()
            .iter()
            .map(|column| column_default_value(&state.catalog, column))
            .collect::<Result<Vec<_>>>()?;
        for (expression, column_index) in expressions.into_iter().zip(&column_indexes) {
            values[*column_index] = evaluate_scalar(&expression, &[], params)?;
        }
        let proposed = Row::new(values);
        let inserted_row = match fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::Before,
            TriggerEvent::Insert,
            None,
            Some(&proposed),
        )? {
            RowTriggerOutcome::Proceed(Some(row)) => row,
            RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
        };
        validate_rows(&state.catalog, &table, std::slice::from_ref(&inserted_row))?;
        if let Some(on_conflict) = &on_conflict
            && let Some(position) = conflicting_row_position(
                state,
                &table,
                &inserted_row,
                on_conflict.target_columns.as_deref(),
            )?
        {
            match &on_conflict.action {
                BoundConflictAction::DoNothing => continue,
                BoundConflictAction::DoUpdate {
                    assignments,
                    filter,
                } => {
                    if command_affected_rows.contains(&position) {
                        return Err(DbError::new(
                            "21000",
                            "ON CONFLICT DO UPDATE command cannot affect row a second time",
                        )
                        .with_hint(
                            "Ensure that no rows proposed for insertion within the same command have duplicate constrained values.",
                        ));
                    }
                    conflict_reservation.grow(std::mem::size_of::<usize>() * 4)?;
                    command_affected_rows.insert(position);
                    let replacement = execute_conflict_update(
                        state,
                        table_id,
                        position,
                        &inserted_row,
                        assignments,
                        filter.as_ref(),
                        params,
                    )?;
                    if let Some(replacement) = replacement {
                        if let Some(returning) = &returning {
                            returned_rows.push(evaluate_returning(
                                returning,
                                &replacement,
                                params,
                            )?);
                        }
                        affected = affected.saturating_add(1);
                    }
                    continue;
                }
            }
        }
        let inserted_position = state.rows.get(&table_id).map_or(0, |rows| rows.len());
        Arc::make_mut(
            state
                .rows
                .entry(table_id)
                .or_insert_with(|| Arc::new(Vec::new())),
        )
        .push(inserted_row.clone());
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        let _ = fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::After,
            TriggerEvent::Insert,
            None,
            Some(&inserted_row),
        )?;
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        if let Some(returning) = &returning {
            returned_rows.push(evaluate_returning(returning, &inserted_row, params)?);
        }
        if conflict_update {
            conflict_reservation.grow(std::mem::size_of::<usize>() * 4)?;
            command_affected_rows.insert(inserted_position);
        }
        affected = affected.saturating_add(1);
    }
    if conflict_update {
        fire_statement_triggers(
            state,
            table_id,
            TriggerTiming::AfterStatement,
            TriggerEvent::Update,
        )?;
    }
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::AfterStatement,
        TriggerEvent::Insert,
    )?;
    validate_database_rows(state)?;
    Ok((
        dml_command_events(
            returning.as_ref(),
            format!("INSERT 0 {affected}"),
            affected,
            returned_rows,
        ),
        true,
    ))
}

fn execute_view_insert(
    state: &mut DatabaseState,
    view_id: ViewId,
    _source: BoundStatement,
    column_indexes: Vec<usize>,
    expressions: Vec<Vec<BoundExpr>>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let view = state
        .catalog
        .view_by_id(view_id)
        .cloned()
        .ok_or_else(|| internal_error("view INSERT target disappeared"))?;
    if view.kind != ViewKind::Regular {
        return Err(DbError::new("42809", "cannot modify a materialized view"));
    }
    let mut affected = 0_u64;
    let mut returned_rows = Vec::new();
    for expressions in expressions {
        ensure_statement_not_cancelled(state)?;
        let mut values = vec![Value::Null; view.output.fields.len()];
        for (expression, column_index) in expressions.into_iter().zip(&column_indexes) {
            let target = values
                .get_mut(*column_index)
                .ok_or_else(|| internal_error("view INSERT column is out of bounds"))?;
            *target = evaluate_scalar(&expression, &[], params)?;
        }
        let proposed = Row::new(values);
        let returned = match fire_view_row_triggers_with_rows(
            state,
            view_id,
            TriggerEvent::Insert,
            None,
            Some(&proposed),
        )? {
            RowTriggerOutcome::Proceed(Some(row)) => row,
            RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
        };
        if let Some(returning) = &returning {
            returned_rows.push(evaluate_returning(returning, &returned, params)?);
        }
        affected = affected.saturating_add(1);
    }
    validate_database_rows(state)?;
    Ok((
        dml_command_events(
            returning.as_ref(),
            format!("INSERT 0 {affected}"),
            affected,
            returned_rows,
        ),
        true,
    ))
}

fn execute_view_source_rows(
    state: &mut DatabaseState,
    source: BoundStatement,
    expected: &Schema,
    params: &[Value],
) -> Result<(Vec<Row>, Vec<DbNotice>)> {
    let (events, dirty) = execute_bound(state, source, params)?;
    if dirty {
        return Err(internal_error(
            "a stored view query attempted to mutate state",
        ));
    }
    let mut rows = Vec::new();
    let mut notices = Vec::new();
    for event in events {
        match event {
            QueryEvent::Schema(schema) if schema != *expected => {
                return Err(DbError::new(
                    "42P16",
                    "stored view query output no longer matches its catalog definition",
                ));
            }
            QueryEvent::Schema(_) | QueryEvent::Progress(_) | QueryEvent::Complete(_) => {}
            QueryEvent::Batch(batch) => rows.extend(batch.rows),
            QueryEvent::Notice(notice) => notices.push(notice),
        }
    }
    Ok((rows, notices))
}

fn conflicting_row_position(
    state: &DatabaseState,
    table: &TableDefinition,
    candidate: &Row,
    target_columns: Option<&[usize]>,
) -> Result<Option<usize>> {
    let target_column_ids = target_columns
        .map(|target| {
            target
                .iter()
                .map(|position| {
                    table
                        .columns()
                        .get(*position)
                        .map(|column| column.id)
                        .ok_or_else(|| internal_error("conflict target column is out of bounds"))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    for definition in table.indexes().filter(|index| {
        index.unique
            && index.method == IndexMethod::BTree
            && target_column_ids.as_deref().is_none_or(|target| {
                target.len() == index.key_columns.len()
                    && target
                        .iter()
                        .all(|column_id| index.key_columns.contains(column_id))
            })
    }) {
        let positions = definition
            .key_columns
            .iter()
            .map(|column_id| {
                table
                    .column_index_by_id(*column_id)
                    .ok_or_else(|| internal_error("conflict index column is absent from its table"))
            })
            .collect::<Result<Vec<_>>>()?;
        let values = positions
            .iter()
            .map(|position| candidate.values[*position].clone())
            .collect::<Vec<_>>();
        if values.iter().any(Value::is_null) {
            continue;
        }
        let key_types = positions
            .iter()
            .map(|position| table.columns()[*position].data_type.clone())
            .collect::<Vec<_>>();
        let key = IndexKey::from_typed_values(&values, &key_types)?;
        let tree = state
            .indexes
            .get(&definition.id)
            .ok_or_else(|| internal_error("conflict arbiter index is absent from live state"))?;
        if let Some(entry) = tree.get_iter(&key).next() {
            let position = usize::try_from(entry.row_id.get())
                .map_err(|_| internal_error("conflict row ID does not fit this platform"))?;
            if state
                .rows
                .get(&table.id)
                .and_then(|rows| rows.get(position))
                .is_none()
            {
                return Err(internal_error("conflict index row ID is outside its table"));
            }
            return Ok(Some(position));
        }
    }
    Ok(None)
}

fn execute_conflict_update(
    state: &mut DatabaseState,
    table_id: TableId,
    conflict_position: usize,
    excluded: &Row,
    assignments: &[(usize, BoundExpr)],
    filter: Option<&BoundExpr>,
    params: &[Value],
) -> Result<Option<Row>> {
    let old_row = state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.get(conflict_position))
        .cloned()
        .ok_or_else(|| internal_error("conflict row disappeared before update"))?;
    let mut conflict_values = old_row.values.clone();
    conflict_values.extend(excluded.values.iter().cloned());
    let conflict_row = Row::new(conflict_values);
    if !filter
        .map(|filter| execution_predicate_matches(filter, &conflict_row, params))
        .transpose()?
        .unwrap_or(true)
    {
        return Ok(None);
    }

    let mut replacements = Vec::with_capacity(assignments.len());
    for (column_index, expression) in assignments {
        replacements.push((
            *column_index,
            evaluate_scalar(expression, &conflict_row.values, params)?,
        ));
    }
    let mut proposed = old_row.clone();
    for (column_index, value) in replacements {
        proposed.values[column_index] = value;
    }
    let replacement = match fire_row_triggers_with_rows(
        state,
        table_id,
        TriggerTiming::Before,
        TriggerEvent::Update,
        Some(&old_row),
        Some(&proposed),
    )? {
        RowTriggerOutcome::Proceed(Some(row)) => row,
        RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => return Ok(None),
    };
    let position = state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.iter().position(|row| row == &old_row))
        .ok_or_else(|| {
            DbError::new(
                "55000",
                "BEFORE trigger changed the row targeted by ON CONFLICT DO UPDATE",
            )
            .with_hint("Return a replacement NEW row instead of updating the same row recursively.")
        })?;
    Arc::make_mut(
        state
            .rows
            .get_mut(&table_id)
            .ok_or_else(|| internal_error("conflict target rows disappeared"))?,
    )[position] = replacement.clone();
    if replacement != old_row {
        apply_referential_actions(
            state,
            vec![ReferentialChange::Update {
                table_id,
                old: old_row.clone(),
                new: replacement.clone(),
            }],
        )?;
    }
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    let _ = fire_row_triggers_with_rows(
        state,
        table_id,
        TriggerTiming::After,
        TriggerEvent::Update,
        Some(&old_row),
        Some(&replacement),
    )?;
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    Ok(Some(replacement))
}

fn execute_select(
    state: &DatabaseState,
    execution: SelectExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let (schema, mut cursor) =
        prepare_select_cursor(state, execution, params, None, &ExecutionOptions::default())?;
    let mut events = vec![QueryEvent::Schema(schema.clone())];
    let mut count = 0_u64;
    let mut emitted_batch = false;
    while let Some(batch) = cursor.next_batch()? {
        count = count.saturating_add(batch.rows.len() as u64);
        emitted_batch = true;
        events.push(QueryEvent::Batch(batch));
    }
    if !emitted_batch {
        events.push(QueryEvent::Batch(Batch {
            schema,
            rows: Vec::new(),
        }));
    }
    events.push(QueryEvent::Progress(QueryProgress {
        rows_processed: count,
    }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: format!("SELECT {count}"),
        rows_affected: count,
    }));
    Ok((events, false))
}

fn execute_with_clause(
    state: &DatabaseState,
    ctes: Vec<BoundCte>,
    body: BoundStatement,
    catalog: Catalog,
    schema: Schema,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let memory = MemoryGrant::new(DEFAULT_SOFT_MEMORY_BYTES, DEFAULT_HARD_MEMORY_BYTES)?;
    let mut reservation = memory.try_reserve(0)?;
    let mut local = state.clone();
    local.catalog = Arc::new(catalog);
    for cte in ctes {
        ensure_statement_not_cancelled(state)?;
        let cte_schema = cte_table_schema(&local, cte.table_id)?;
        let (events, changed) = execute_bound(&mut local, *cte.seed, params)?;
        if changed {
            return Err(internal_error(
                "CTE query unexpectedly changed database state",
            ));
        }
        let mut rows = coerce_set_rows(
            collect_set_operand_rows(events),
            &cte_schema,
            &mut reservation,
        )?;
        ensure_recursive_cte_row_limit(rows.len())?;
        if let Some(recursive) = cte.recursive {
            let mut seen = (!cte.union_all).then(HashSet::new);
            if let Some(seen) = &mut seen {
                for row in &rows {
                    seen.insert(accounted_set_row_key(row, &mut reservation)?);
                }
            }
            let mut working = rows.clone();
            for row in &working {
                reservation.grow(estimated_row_bytes(row))?;
            }
            let mut iteration = 0_usize;
            while !working.is_empty() {
                ensure_statement_not_cancelled(state)?;
                iteration = iteration.saturating_add(1);
                if iteration > MAX_RECURSIVE_CTE_ITERATIONS {
                    return Err(DbError::new(
                        "54001",
                        format!("recursive CTE exceeded {MAX_RECURSIVE_CTE_ITERATIONS} iterations"),
                    )
                    .with_hint("Add a terminating predicate to the recursive term."));
                }
                local.rows.insert(cte.table_id, Arc::new(working));
                let (events, changed) = execute_bound(&mut local, (*recursive).clone(), params)?;
                if changed {
                    return Err(internal_error(
                        "recursive CTE term unexpectedly changed database state",
                    ));
                }
                let candidates = coerce_set_rows(
                    collect_set_operand_rows(events),
                    &cte_schema,
                    &mut reservation,
                )?;
                let mut next = Vec::new();
                for row in candidates {
                    ensure_statement_not_cancelled(state)?;
                    if let Some(seen) = &mut seen {
                        let key = accounted_set_row_key(&row, &mut reservation)?;
                        if !seen.insert(key) {
                            continue;
                        }
                    }
                    ensure_recursive_cte_row_limit(
                        rows.len().saturating_add(next.len()).saturating_add(1),
                    )?;
                    next.push(row);
                }
                for row in &next {
                    reservation.grow(estimated_row_bytes(row))?;
                }
                working = next.clone();
                rows.extend(next);
            }
        }
        local.rows.insert(cte.table_id, Arc::new(rows));
    }
    if bound_statement_schema(&body) != schema {
        return Err(internal_error(
            "WITH body schema changed after binding its CTEs",
        ));
    }
    let (events, changed) = execute_bound(&mut local, body, params)?;
    if changed {
        return Err(internal_error(
            "WITH body unexpectedly changed database state",
        ));
    }
    Ok((events, false))
}

fn ensure_recursive_cte_row_limit(row_count: usize) -> Result<()> {
    if row_count > MAX_RECURSIVE_CTE_ROWS {
        return Err(DbError::new(
            "54000",
            format!("recursive CTE exceeded {MAX_RECURSIVE_CTE_ROWS} rows"),
        ));
    }
    Ok(())
}

fn cte_table_schema(state: &DatabaseState, table_id: TableId) -> Result<Schema> {
    Ok(Schema::new(
        table_definition(state, table_id)?
            .columns()
            .iter()
            .map(|column| {
                Field::new(
                    column.name.as_str(),
                    column.data_type.clone(),
                    column.nullable,
                )
            })
            .collect(),
    ))
}

fn execute_set_operation(
    state: &mut DatabaseState,
    execution: SetExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let SetExecution {
        left,
        operator,
        all,
        right,
        schema,
        order_by,
        offset,
        limit,
    } = execution;
    let memory = MemoryGrant::new(DEFAULT_SOFT_MEMORY_BYTES, DEFAULT_HARD_MEMORY_BYTES)?;
    let mut reservation = memory.try_reserve(0)?;
    let (left_events, left_changed) = execute_bound(state, *left, params)?;
    let (right_events, right_changed) = execute_bound(state, *right, params)?;
    if left_changed || right_changed {
        return Err(internal_error(
            "set-operation operand unexpectedly changed database state",
        ));
    }
    let left = coerce_set_rows(
        collect_set_operand_rows(left_events),
        &schema,
        &mut reservation,
    )?;
    let right = coerce_set_rows(
        collect_set_operand_rows(right_events),
        &schema,
        &mut reservation,
    )?;
    let mut rows = combine_set_rows(state, left, right, operator, all, &memory)?;
    if !order_by.is_empty() {
        sort_set_rows(&mut rows, &order_by)?;
    }
    let offset = evaluate_set_offset(offset.as_ref(), params)?;
    let limit = evaluate_set_limit(limit.as_ref(), params)?;
    let rows = rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    Ok((select_rows_events(schema, rows), false))
}

fn collect_set_operand_rows(events: Vec<QueryEvent>) -> Vec<Row> {
    let mut rows = Vec::new();
    for event in events {
        if let QueryEvent::Batch(mut batch) = event {
            rows.append(&mut batch.rows);
        }
    }
    rows
}

fn coerce_set_rows(
    rows: Vec<Row>,
    schema: &Schema,
    reservation: &mut Reservation,
) -> Result<Vec<Row>> {
    rows.into_iter()
        .map(|row| {
            if row.values.len() != schema.fields.len() {
                return Err(internal_error(
                    "set-operation row width does not match its bound schema",
                ));
            }
            let row = Row::new(
                row.values
                    .into_iter()
                    .zip(&schema.fields)
                    .map(|(value, field)| coerce_execution_value(value, &field.data_type))
                    .collect::<Result<Vec<_>>>()?,
            );
            reservation.grow(estimated_row_bytes(&row))?;
            Ok(row)
        })
        .collect()
}

fn combine_set_rows(
    state: &DatabaseState,
    left: Vec<Row>,
    right: Vec<Row>,
    operator: QuerySetOperator,
    all: bool,
    memory: &MemoryGrant,
) -> Result<Vec<Row>> {
    if operator == QuerySetOperator::Union && all {
        return Ok(left.into_iter().chain(right).collect());
    }
    let mut key_reservation = memory.try_reserve(0)?;
    let mut output = Vec::new();
    match operator {
        QuerySetOperator::Union => {
            let mut seen = HashSet::new();
            for row in left.into_iter().chain(right) {
                ensure_statement_not_cancelled(state)?;
                let key = accounted_set_row_key(&row, &mut key_reservation)?;
                if seen.insert(key) {
                    output.push(row);
                }
            }
        }
        QuerySetOperator::Intersect => {
            let mut right_counts = set_row_counts(state, right, &mut key_reservation)?;
            let mut emitted = (!all).then(HashSet::new);
            for row in left {
                ensure_statement_not_cancelled(state)?;
                let key = accounted_set_row_key(&row, &mut key_reservation)?;
                if emitted
                    .as_ref()
                    .is_some_and(|emitted| emitted.contains(&key))
                {
                    continue;
                }
                let Some(count) = right_counts.get_mut(&key) else {
                    continue;
                };
                if *count == 0 {
                    continue;
                }
                if all {
                    *count -= 1;
                } else if let Some(emitted) = &mut emitted {
                    emitted.insert(key);
                }
                output.push(row);
            }
        }
        QuerySetOperator::Except => {
            let mut right_counts = set_row_counts(state, right, &mut key_reservation)?;
            let mut emitted = (!all).then(HashSet::new);
            for row in left {
                ensure_statement_not_cancelled(state)?;
                let key = accounted_set_row_key(&row, &mut key_reservation)?;
                if emitted
                    .as_ref()
                    .is_some_and(|emitted| emitted.contains(&key))
                {
                    continue;
                }
                if let Some(count) = right_counts.get_mut(&key)
                    && *count > 0
                {
                    if all {
                        *count -= 1;
                    }
                    continue;
                }
                if let Some(emitted) = &mut emitted {
                    emitted.insert(key);
                }
                output.push(row);
            }
        }
    }
    Ok(output)
}

fn set_row_counts(
    state: &DatabaseState,
    rows: Vec<Row>,
    reservation: &mut Reservation,
) -> Result<HashMap<SetRowKey, usize>> {
    let mut counts = HashMap::<SetRowKey, usize>::new();
    for row in rows {
        ensure_statement_not_cancelled(state)?;
        let key = accounted_set_row_key(&row, reservation)?;
        let count = counts.entry(key).or_default();
        *count = count
            .checked_add(1)
            .ok_or_else(|| DbError::new("22003", "set-operation duplicate count overflowed"))?;
    }
    Ok(counts)
}

fn accounted_set_row_key(row: &Row, reservation: &mut Reservation) -> Result<SetRowKey> {
    let key = set_row_key(row)?;
    reservation.grow(estimated_set_key_bytes(&key).saturating_add(64))?;
    Ok(key)
}

fn set_row_key(row: &Row) -> Result<SetRowKey> {
    row.values
        .iter()
        .map(|value| match value {
            Value::Null => Ok(SetValueKey::Null),
            Value::Boolean(value) => Ok(SetValueKey::Boolean(*value)),
            Value::Int16(value) => Ok(SetValueKey::Int16(*value)),
            Value::Int32(value) => Ok(SetValueKey::Int32(*value)),
            Value::Int64(value) => Ok(SetValueKey::Int64(*value)),
            Value::Float32(value) => Ok(SetValueKey::Float32(canonical_float32(*value))),
            Value::Float64(value) => Ok(SetValueKey::Float64(canonical_float64(*value))),
            Value::Decimal(value) => Ok(SetValueKey::Decimal(value.normalize().to_string())),
            Value::Text(value) => Ok(SetValueKey::Text(value.clone())),
            Value::Binary(value) => Ok(SetValueKey::Binary(value.clone())),
            Value::Date(value) => Ok(SetValueKey::Date(value.to_string())),
            Value::Time(value) => Ok(SetValueKey::Time(value.to_string())),
            Value::Timestamp(value) => Ok(SetValueKey::Timestamp(value.to_string())),
            Value::Interval(value) => Ok(SetValueKey::Interval(
                value.months,
                value.days,
                value.microseconds,
            )),
            Value::Array(value) => serde_json::to_string(value)
                .map(SetValueKey::Array)
                .map_err(|error| internal_error(format!("failed to canonicalize array: {error}"))),
            Value::Json(_) => Err(DbError::new(
                "42883",
                "could not identify an equality operator for type json",
            )),
            Value::Jsonb(value) => serde_json::to_string(value)
                .map(SetValueKey::Jsonb)
                .map_err(|error| internal_error(format!("failed to canonicalize jsonb: {error}"))),
            Value::Uuid(value) => Ok(SetValueKey::Uuid(*value.as_bytes())),
            Value::Vector(values) => Ok(SetValueKey::Vector(
                values
                    .iter()
                    .map(|value| canonical_float32(*value))
                    .collect(),
            )),
        })
        .collect::<Result<Vec<_>>>()
        .map(SetRowKey)
}

fn canonical_float32(value: f32) -> u32 {
    if value == 0.0 {
        0.0_f32.to_bits()
    } else if value.is_nan() {
        f32::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

fn canonical_float64(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

fn estimated_set_key_bytes(key: &SetRowKey) -> usize {
    mem::size_of::<SetRowKey>()
        .saturating_add(key.0.iter().map(estimated_set_value_key_bytes).sum())
}

fn estimated_set_value_key_bytes(key: &SetValueKey) -> usize {
    mem::size_of::<SetValueKey>()
        + match key {
            SetValueKey::Decimal(value)
            | SetValueKey::Text(value)
            | SetValueKey::Date(value)
            | SetValueKey::Time(value)
            | SetValueKey::Timestamp(value)
            | SetValueKey::Array(value)
            | SetValueKey::Jsonb(value) => value.len(),
            SetValueKey::Binary(value) => value.len(),
            SetValueKey::Vector(values) => values.len().saturating_mul(mem::size_of::<u32>()),
            SetValueKey::Null
            | SetValueKey::Boolean(_)
            | SetValueKey::Int16(_)
            | SetValueKey::Int32(_)
            | SetValueKey::Int64(_)
            | SetValueKey::Float32(_)
            | SetValueKey::Float64(_)
            | SetValueKey::Interval(_, _, _)
            | SetValueKey::Uuid(_) => 0,
        }
}
