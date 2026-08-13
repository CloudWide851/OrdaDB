
fn rewrite_expr_correlations(
    expression: &mut BoundExpr,
    parameter_base: usize,
    slots: &mut BTreeMap<usize, usize>,
    correlation_indexes: &mut Vec<usize>,
    ancestor_slots: &[BTreeMap<usize, usize>],
) -> Result<()> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        if let BoundExprKind::Correlation { depth, index } = &expression.kind {
            let depth = *depth;
            let outer_index = *index;
            let parameter_index = if depth == 1 {
                correlation_parameter_slot(outer_index, parameter_base, slots, correlation_indexes)?
            } else if depth > 1 {
                ancestor_slots
                    .get(depth - 2)
                    .and_then(|slots| slots.get(&outer_index))
                    .copied()
                    .ok_or_else(|| {
                        DbError::internal(
                            "nested correlation parameter was not forwarded by its parent Apply",
                        )
                    })?
            } else {
                return Err(DbError::internal("correlation depth must be positive"));
            };
            expression.kind = BoundExprKind::Parameter {
                index: parameter_index,
            };
            continue;
        }
        match &mut expression.kind {
            BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => pending.push(expr),
            BoundExprKind::Array { elements, .. } => pending.extend(elements.iter_mut().rev()),
            BoundExprKind::Function { arguments, .. } => {
                pending.extend(arguments.iter_mut().rev());
            }
            BoundExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundExprKind::InList { expr, list, .. } => {
                for candidate in list.iter_mut().rev() {
                    pending.push(candidate);
                }
                pending.push(expr);
            }
            BoundExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            BoundExprKind::Column { .. }
            | BoundExprKind::Literal(_)
            | BoundExprKind::Parameter { .. }
            | BoundExprKind::Correlation { .. }
            | BoundExprKind::ApplyValue { .. } => {}
        }
    }
    Ok(())
}

fn correlation_parameter_slot(
    outer_index: usize,
    parameter_base: usize,
    slots: &mut BTreeMap<usize, usize>,
    correlation_indexes: &mut Vec<usize>,
) -> Result<usize> {
    if let Some(parameter_index) = slots.get(&outer_index) {
        return Ok(*parameter_index);
    }
    let parameter_index = parameter_base
        .checked_add(correlation_indexes.len())
        .and_then(|index| index.checked_add(1))
        .ok_or_else(|| DbError::new("54001", "correlation parameter depth overflowed"))?;
    slots.insert(outer_index, parameter_index);
    correlation_indexes.push(outer_index);
    Ok(parameter_index)
}

fn equi_join_columns(expr: &BoundExpr, right_offset: usize) -> Option<(usize, usize)> {
    let BoundExprKind::Binary {
        left,
        op: BinaryOperator::Eq,
        right,
    } = &expr.kind
    else {
        return None;
    };
    let (BoundExprKind::Column { index: left_index }, BoundExprKind::Column { index: right_index }) =
        (&left.kind, &right.kind)
    else {
        return None;
    };
    if *left_index < right_offset && *right_index >= right_offset {
        Some((*left_index, *right_index))
    } else if *right_index < right_offset && *left_index >= right_offset {
        Some((*right_index, *left_index))
    } else {
        None
    }
}

fn execute_explain(
    state: &DatabaseState,
    statement: BoundStatement,
) -> Result<(Vec<QueryEvent>, bool)> {
    let lines = match statement {
        BoundStatement::Select {
            table_id,
            projection,
            filter,
            order_by,
            offset,
            limit,
            ..
        } => explain_plan(&optimize_select(
            table_definition(state, table_id)?,
            projection,
            filter,
            order_by,
            offset,
            limit,
        )),
        BoundStatement::AdvancedSelect {
            table,
            joins,
            applies,
            windows,
            filter,
            distinct,
            aggregate,
            ..
        } => explain_advanced(
            state,
            &table,
            &joins,
            &applies,
            AdvancedExplainFeatures {
                window_count: windows.len(),
                filtered: filter.is_some(),
                distinct,
                aggregate,
            },
        )?,
        _ => {
            return Err(DbError::new(
                "0A000",
                "EXPLAIN supports SELECT statements only",
            ));
        }
    };
    let schema = Schema::new(vec![Field::new("QUERY PLAN", ScalarType::Text, false)]);
    let count = lines.len() as u64;
    let batch = Batch {
        schema: schema.clone(),
        rows: lines
            .into_iter()
            .map(|line| Row::new(vec![Value::Text(line)]))
            .collect(),
    };
    Ok((
        command_events(schema, format!("EXPLAIN {count}"), count, Some(batch)),
        false,
    ))
}

struct AdvancedExplainFeatures {
    window_count: usize,
    filtered: bool,
    distinct: bool,
    aggregate: bool,
}

fn explain_advanced(
    state: &DatabaseState,
    table: &BoundTable,
    joins: &[BoundJoin],
    applies: &[BoundApply],
    features: AdvancedExplainFeatures,
) -> Result<Vec<String>> {
    let base = table_definition(state, table.table_id)?;
    let mut estimated_rows = base.statistics().row_count;
    let mut lines = vec!["Projection  (cost=0.00 rows=1)".to_owned()];
    if features.distinct {
        lines.push("  Unique  (cost=0.00 rows=1)".to_owned());
    }
    if features.aggregate {
        lines.push("  Aggregate  (cost=0.00 rows=1)".to_owned());
    }
    if features.window_count > 0 {
        lines.push(format!(
            "  WindowAgg  (cost=0.00 rows=1 windows={})",
            features.window_count
        ));
    }
    if features.filtered {
        lines.push(format!(
            "  Filter  (cost={:.2} rows={})",
            estimated_rows as f64 * 0.01,
            estimated_rows
        ));
    }
    for apply in applies {
        let kind = match &apply.kind {
            BoundApplyKind::Scalar => "Scalar Apply",
            BoundApplyKind::Exists { negated: false } => "Exists Apply",
            BoundApplyKind::Exists { negated: true } => "Not Exists Apply",
            BoundApplyKind::In { negated: false, .. } => "In Apply",
            BoundApplyKind::In { negated: true, .. } => "Not In Apply",
            BoundApplyKind::Quantified {
                quantifier: SubqueryQuantifier::Any,
                ..
            } => "Any Apply",
            BoundApplyKind::Quantified {
                quantifier: SubqueryQuantifier::All,
                ..
            } => "All Apply",
            BoundApplyKind::RowScalar { .. } => "Row Scalar Apply",
            BoundApplyKind::RowQuantified {
                quantifier: SubqueryQuantifier::Any,
                ..
            } => "Row Any Apply",
            BoundApplyKind::RowQuantified {
                quantifier: SubqueryQuantifier::All,
                ..
            } => "Row All Apply",
        };
        lines.push(format!("  {kind}  (cost=0.00 rows=1)"));
    }
    for join in joins {
        let (right_rows, equi) = match &join.source {
            BoundJoinSource::Table(table) => {
                let right = table_definition(state, table.table_id)?;
                (
                    right.statistics().row_count,
                    equi_join_columns(&join.on, table.offset).is_some(),
                )
            }
            BoundJoinSource::Derived { .. } => (1, false),
        };
        let choice = choose_join_strategy(estimated_rows, right_rows, equi);
        let name = match choice.strategy {
            JoinStrategy::NestedLoop => "Nested Loop",
            JoinStrategy::Hash => "Hash Join",
        };
        let kind = if join.kind == JoinKind::Left {
            "Left"
        } else {
            "Inner"
        };
        lines.push(format!(
            "  {name} {kind}  (cost={:.2} rows={:.0})",
            choice.estimated_cost, choice.estimated_rows
        ));
        estimated_rows = choice.estimated_rows as u64;
    }
    lines.push(format!(
        "    Seq Scan on {}  (cost={:.2} rows={})",
        table.binding,
        estimated_rows as f64 * 0.01,
        base.statistics().row_count
    ));
    for join in joins {
        match &join.source {
            BoundJoinSource::Table(table) => {
                let right = table_definition(state, table.table_id)?;
                lines.push(format!(
                    "    Seq Scan on {}  (cost={:.2} rows={})",
                    table.binding,
                    right.statistics().row_count as f64 * 0.01,
                    right.statistics().row_count
                ));
            }
            BoundJoinSource::Derived {
                lateral, binding, ..
            } => {
                let label = if *lateral {
                    "Lateral Subquery Scan"
                } else {
                    "Subquery Scan"
                };
                lines.push(format!("    {label} on {binding}  (cost=0.01 rows=1)"));
            }
        }
    }
    Ok(lines)
}

fn execute_update(
    state: &mut DatabaseState,
    table_id: TableId,
    assignments: Vec<(usize, BoundExpr)>,
    filter: Option<BoundExpr>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    table_definition(state, table_id)?;
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::BeforeStatement,
        TriggerEvent::Update,
    )?;
    let source_rows = state
        .rows
        .get(&table_id)
        .map(|rows| (**rows).clone())
        .unwrap_or_default();
    let mut updated = 0u64;
    let mut returned_rows = Vec::new();
    for old_row in source_rows {
        if filter
            .as_ref()
            .map(|filter| execution_predicate_matches(filter, &old_row, params))
            .transpose()?
            .unwrap_or(true)
        {
            let original = old_row.values.clone();
            let mut replacements = Vec::with_capacity(assignments.len());
            for (column_index, expression) in &assignments {
                replacements.push((
                    *column_index,
                    evaluate_scalar(expression, &original, params)?,
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
                RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
            };
            let position = state
                .rows
                .get(&table_id)
                .and_then(|rows| rows.iter().position(|row| row == &old_row))
                .ok_or_else(|| {
                    DbError::new(
                        "55000",
                        "BEFORE trigger changed the row targeted by the outer UPDATE",
                    )
                    .with_hint(
                        "Return a replacement NEW row instead of updating the same row recursively.",
                    )
                })?;
            Arc::make_mut(
                state
                    .rows
                    .get_mut(&table_id)
                    .ok_or_else(|| internal_error("updated table rows disappeared"))?,
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
            if let Some(returning) = &returning {
                returned_rows.push(evaluate_returning(returning, &replacement, params)?);
            }
            updated = updated.saturating_add(1);
        }
    }
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::AfterStatement,
        TriggerEvent::Update,
    )?;
    validate_database_rows(state)?;
    Ok((
        dml_command_events(
            returning.as_ref(),
            format!("UPDATE {updated}"),
            updated,
            returned_rows,
        ),
        true,
    ))
}

fn execute_view_update(
    state: &mut DatabaseState,
    view_id: ViewId,
    source: BoundStatement,
    assignments: Vec<(usize, BoundExpr)>,
    filter: Option<BoundExpr>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let view = state
        .catalog
        .view_by_id(view_id)
        .cloned()
        .ok_or_else(|| internal_error("view UPDATE target disappeared"))?;
    if view.kind != ViewKind::Regular {
        return Err(DbError::new("42809", "cannot modify a materialized view"));
    }
    let (source_rows, notices) = execute_view_source_rows(state, source, &view.output, params)?;
    let mut updated = 0_u64;
    let mut returned_rows = Vec::new();
    for old_row in source_rows {
        ensure_statement_not_cancelled(state)?;
        if !filter
            .as_ref()
            .map(|filter| execution_predicate_matches(filter, &old_row, params))
            .transpose()?
            .unwrap_or(true)
        {
            continue;
        }
        let mut proposed = old_row.clone();
        for (column_index, expression) in &assignments {
            let value = evaluate_scalar(expression, &old_row.values, params)?;
            let target = proposed
                .values
                .get_mut(*column_index)
                .ok_or_else(|| internal_error("view UPDATE column is out of bounds"))?;
            *target = value;
        }
        let returned = match fire_view_row_triggers_with_rows(
            state,
            view_id,
            TriggerEvent::Update,
            Some(&old_row),
            Some(&proposed),
        )? {
            RowTriggerOutcome::Proceed(Some(row)) => row,
            RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
        };
        if let Some(returning) = &returning {
            returned_rows.push(evaluate_returning(returning, &returned, params)?);
        }
        updated = updated.saturating_add(1);
    }
    validate_database_rows(state)?;
    let mut events = dml_command_events(
        returning.as_ref(),
        format!("UPDATE {updated}"),
        updated,
        returned_rows,
    );
    insert_pending_notices(&mut events, notices);
    Ok((events, true))
}

fn execute_delete(
    state: &mut DatabaseState,
    table_id: TableId,
    filter: Option<BoundExpr>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    table_definition(state, table_id)?;
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::BeforeStatement,
        TriggerEvent::Delete,
    )?;
    let source_rows = state
        .rows
        .get(&table_id)
        .map(|rows| (**rows).clone())
        .unwrap_or_default();
    let mut deleted = 0u64;
    let mut returned_rows = Vec::new();
    for old_row in source_rows {
        let matches = filter
            .as_ref()
            .map(|filter| execution_predicate_matches(filter, &old_row, params))
            .transpose()?
            .unwrap_or(true);
        if !matches {
            continue;
        }
        if matches!(
            fire_row_triggers_with_rows(
                state,
                table_id,
                TriggerTiming::Before,
                TriggerEvent::Delete,
                Some(&old_row),
                None,
            )?,
            RowTriggerOutcome::Suppress
        ) {
            continue;
        }
        let position = state
            .rows
            .get(&table_id)
            .and_then(|rows| rows.iter().position(|row| row == &old_row))
            .ok_or_else(|| {
                DbError::new(
                    "55000",
                    "BEFORE trigger changed the row targeted by the outer DELETE",
                )
                .with_hint("Return OLD instead of deleting the same row recursively.")
            })?;
        Arc::make_mut(
            state
                .rows
                .get_mut(&table_id)
                .ok_or_else(|| internal_error("deleted table rows disappeared"))?,
        )
        .remove(position);
        apply_referential_actions(
            state,
            vec![ReferentialChange::Delete {
                table_id,
                old: old_row.clone(),
            }],
        )?;
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        let _ = fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::After,
            TriggerEvent::Delete,
            Some(&old_row),
            None,
        )?;
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        if let Some(returning) = &returning {
            returned_rows.push(evaluate_returning(returning, &old_row, params)?);
        }
        deleted = deleted.saturating_add(1);
    }
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::AfterStatement,
        TriggerEvent::Delete,
    )?;
    validate_database_rows(state)?;
    Ok((
        dml_command_events(
            returning.as_ref(),
            format!("DELETE {deleted}"),
            deleted,
            returned_rows,
        ),
        true,
    ))
}

fn execute_view_delete(
    state: &mut DatabaseState,
    view_id: ViewId,
    source: BoundStatement,
    filter: Option<BoundExpr>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let view = state
        .catalog
        .view_by_id(view_id)
        .cloned()
        .ok_or_else(|| internal_error("view DELETE target disappeared"))?;
    if view.kind != ViewKind::Regular {
        return Err(DbError::new("42809", "cannot modify a materialized view"));
    }
    let (source_rows, notices) = execute_view_source_rows(state, source, &view.output, params)?;
    let mut deleted = 0_u64;
    let mut returned_rows = Vec::new();
    for old_row in source_rows {
        ensure_statement_not_cancelled(state)?;
        if !filter
            .as_ref()
            .map(|filter| execution_predicate_matches(filter, &old_row, params))
            .transpose()?
            .unwrap_or(true)
        {
            continue;
        }
        let returned = match fire_view_row_triggers_with_rows(
            state,
            view_id,
            TriggerEvent::Delete,
            Some(&old_row),
            None,
        )? {
            RowTriggerOutcome::Proceed(Some(row)) => row,
            RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
        };
        if let Some(returning) = &returning {
            returned_rows.push(evaluate_returning(returning, &returned, params)?);
        }
        deleted = deleted.saturating_add(1);
    }
    validate_database_rows(state)?;
    let mut events = dml_command_events(
        returning.as_ref(),
        format!("DELETE {deleted}"),
        deleted,
        returned_rows,
    );
    insert_pending_notices(&mut events, notices);
    Ok((events, true))
}

fn evaluate_returning(returning: &BoundReturning, row: &Row, params: &[Value]) -> Result<Row> {
    returning
        .projection
        .iter()
        .map(|projection| evaluate_scalar(&projection.expr, &row.values, params))
        .collect::<Result<Vec<_>>>()
        .map(Row::new)
}

fn dml_command_events(
    returning: Option<&BoundReturning>,
    tag: impl Into<String>,
    rows_affected: u64,
    rows: Vec<Row>,
) -> Vec<QueryEvent> {
    match returning {
        Some(returning) => {
            let schema = returning.schema.clone();
            let mut events = vec![QueryEvent::Schema(schema.clone())];
            let mut batch_rows = Vec::with_capacity(DEFAULT_BATCH_ROWS.min(rows.len()));
            for row in rows {
                batch_rows.push(row);
                if batch_rows.len() == DEFAULT_BATCH_ROWS {
                    events.push(QueryEvent::Batch(Batch {
                        schema: schema.clone(),
                        rows: mem::take(&mut batch_rows),
                    }));
                }
            }
            if !batch_rows.is_empty() {
                events.push(QueryEvent::Batch(Batch {
                    schema: schema.clone(),
                    rows: batch_rows,
                }));
            }
            events.push(QueryEvent::Progress(QueryProgress {
                rows_processed: rows_affected,
            }));
            events.push(QueryEvent::Complete(CommandComplete {
                tag: tag.into(),
                rows_affected,
            }));
            events
        }
        None => command_events(Schema::empty(), tag, rows_affected, None),
    }
}

fn removed_rows(before: &[Row], after: &[Row]) -> Vec<Row> {
    let mut matched = vec![false; after.len()];
    let mut removed = Vec::new();
    for row in before {
        if let Some((index, _)) = after
            .iter()
            .enumerate()
            .find(|(index, candidate)| !matched[*index] && *candidate == row)
        {
            matched[index] = true;
        } else {
            removed.push(row.clone());
        }
    }
    removed
}

fn apply_referential_actions(
    state: &mut DatabaseState,
    changes: Vec<ReferentialChange>,
) -> Result<()> {
    let mut queue = VecDeque::from(changes);
    let mut applied = 0usize;
    while let Some(change) = queue.pop_front() {
        applied = applied.saturating_add(1);
        if applied > MAX_REFERENTIAL_ACTIONS {
            return Err(DbError::new(
                "54001",
                "referential action work exceeds the configured limit",
            ));
        }
        let (referenced_table_id, old_row, new_row) = match &change {
            ReferentialChange::Delete { table_id, old } => (*table_id, old, None),
            ReferentialChange::Update { table_id, old, new } => (*table_id, old, Some(new)),
        };
        let referenced_table = table_definition(state, referenced_table_id)?.clone();
        let referencing = state
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .flat_map(|table| {
                table.constraints().filter_map(|constraint| {
                    let ConstraintKind::ForeignKey {
                        columns,
                        referenced_table,
                        referenced_columns,
                        on_delete,
                        on_update,
                    } = &constraint.kind
                    else {
                        return None;
                    };
                    (*referenced_table == referenced_table_id).then(|| {
                        (
                            table.id,
                            constraint.name.clone(),
                            columns.clone(),
                            referenced_columns.clone(),
                            *on_delete,
                            *on_update,
                        )
                    })
                })
            })
            .collect::<Vec<_>>();

        for (
            child_table_id,
            constraint_name,
            local_columns,
            referenced_columns,
            on_delete,
            on_update,
        ) in referencing
        {
            let child_table = table_definition(state, child_table_id)?.clone();
            let parent_positions = referenced_columns
                .iter()
                .map(|column_id| {
                    referenced_table
                        .column_index_by_id(*column_id)
                        .ok_or_else(|| {
                            internal_error("foreign-key parent column is absent during action")
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let child_positions = local_columns
                .iter()
                .map(|column_id| {
                    child_table.column_index_by_id(*column_id).ok_or_else(|| {
                        internal_error("foreign-key child column is absent during action")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            if let Some(new_row) = new_row
                && parent_positions
                    .iter()
                    .all(|position| old_row.values[*position] == new_row.values[*position])
            {
                continue;
            }
            let action = if new_row.is_some() {
                on_update
            } else {
                on_delete
            };
            let child_rows = Arc::make_mut(
                state
                    .rows
                    .entry(child_table_id)
                    .or_insert_with(|| Arc::new(Vec::new())),
            );
            let matches_parent = |row: &Row| {
                child_positions
                    .iter()
                    .zip(&parent_positions)
                    .all(|(child, parent)| row.values[*child] == old_row.values[*parent])
            };
            if matches!(
                action,
                ReferentialAction::NoAction | ReferentialAction::Restrict
            ) && child_rows.iter().any(matches_parent)
            {
                return Err(DbError::new(
                    "23503",
                    format!("update or delete violates foreign-key constraint {constraint_name}"),
                ));
            }
            match action {
                ReferentialAction::NoAction | ReferentialAction::Restrict => {}
                ReferentialAction::Cascade if new_row.is_none() => {
                    let before = child_rows.clone();
                    child_rows.retain(|row| !matches_parent(row));
                    queue.extend(removed_rows(&before, child_rows).into_iter().map(|old| {
                        ReferentialChange::Delete {
                            table_id: child_table_id,
                            old,
                        }
                    }));
                }
                ReferentialAction::Cascade => {
                    let new_row = new_row.ok_or_else(|| {
                        internal_error("update cascade has no replacement parent row")
                    })?;
                    for child_row in child_rows.iter_mut().filter(|row| matches_parent(row)) {
                        let old = child_row.clone();
                        for (child, parent) in child_positions.iter().zip(&parent_positions) {
                            child_row.values[*child] = new_row.values[*parent].clone();
                        }
                        queue.push_back(ReferentialChange::Update {
                            table_id: child_table_id,
                            old,
                            new: child_row.clone(),
                        });
                    }
                }
                ReferentialAction::SetNull | ReferentialAction::SetDefault => {
                    for child_row in child_rows.iter_mut().filter(|row| matches_parent(row)) {
                        let old = child_row.clone();
                        for child in &child_positions {
                            child_row.values[*child] = if action == ReferentialAction::SetNull {
                                Value::Null
                            } else {
                                let column = &child_table.columns()[*child];
                                column_default_value(&state.catalog, column)?
                            };
                        }
                        queue.push_back(ReferentialChange::Update {
                            table_id: child_table_id,
                            old,
                            new: child_row.clone(),
                        });
                    }
                }
            }
            rebuild_table_derived(state, child_table_id)?;
        }
    }
    Ok(())
}
