
impl PlpgsqlHost for EnginePlpgsqlHost<'_> {
    fn execute_sql(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>>>> {
        let (sql, parameters, _) =
            expand_trigger_record_fields(sql, parameters, self.trigger.as_deref())?;
        let statement = resolve_sequence_currval(
            bind(parse(&sql)?, &self.state.catalog)?,
            &self.state.sequence_currvals,
        )?;
        if let BoundStatement::PgNotify {
            channel,
            payload,
            schema,
        } = &statement
        {
            let (channel, payload) = evaluate_pg_notify(channel, payload, &parameters)?;
            self.state.pending_notifications.notify(channel, payload);
            return Ok(Box::new(
                pg_notify_events(schema.clone()).into_iter().map(Ok),
            ));
        }
        if let BoundStatement::Notify { channel, payload } = &statement {
            self.state
                .pending_notifications
                .notify(channel.clone(), payload.clone());
            return Ok(Box::new(transaction_events("NOTIFY").into_iter().map(Ok)));
        }
        if matches!(
            statement,
            BoundStatement::Commit { .. } | BoundStatement::Rollback { .. }
        ) {
            return Err(DbError::new(
                "2D000",
                "invalid transaction termination inside this PL/pgSQL invocation",
            )
            .with_hint(
                "transaction termination requires an eligible top-level procedure CALL in autocommit mode",
            ));
        }
        if matches!(
            statement,
            BoundStatement::Begin { .. }
                | BoundStatement::Savepoint { .. }
                | BoundStatement::RollbackTo { .. }
                | BoundStatement::ReleaseSavepoint { .. }
        ) {
            return Err(DbError::new(
                "0A000",
                "this transaction control command is not allowed inside PL/pgSQL",
            ));
        }
        if let Some(stream) = prepare_read_stream(self.state, statement.clone(), &parameters, None)?
        {
            return Ok(Box::new(stream));
        }
        let (events, dirty) = execute_bound_with_ownership(self.state, statement, &parameters)?;
        self.sql_dirty |= dirty;
        Ok(Box::new(events.into_iter().map(Ok)))
    }

    fn evaluate_expression(&mut self, sql: &str, parameters: &[Value]) -> Result<Value> {
        if let Some(trigger) = self.trigger.as_deref()
            && let Some((slot, field)) = trigger_field_reference(sql)
        {
            return trigger.value(slot, field);
        }
        if let Some(index) = sql
            .trim()
            .strip_prefix('$')
            .and_then(|index| index.parse::<usize>().ok())
        {
            return parameters
                .get(index.saturating_sub(1))
                .cloned()
                .ok_or_else(|| DbError::new("42P02", format!("there is no parameter ${index}")));
        }
        let (sql, parameters, mut parameter_types) =
            expand_trigger_record_fields(sql, parameters, self.trigger.as_deref())?;
        for (index, value) in parameters.iter().enumerate() {
            if let Some(data_type) = scalar_type_of_value(value) {
                parameter_types.entry(index + 1).or_insert(data_type);
            }
        }
        let expression = CatalogExpression::new(sql);
        let bound = bind_catalog_expression_with_parameter_types_and_catalog(
            &expression,
            None,
            None,
            &parameter_types,
            Some(&self.state.catalog),
        )?;
        evaluate_scalar(&bound, &[], &parameters)
    }

    fn assign_composite_field(&mut self, slot: usize, field: &str, value: Value) -> Result<()> {
        self.trigger
            .as_deref_mut()
            .ok_or_else(|| {
                DbError::new(
                    "0A000",
                    "composite assignment is only available in row triggers",
                )
            })?
            .assign(slot, field, value)
    }

    fn resolve_row_type(&mut self, relation: &str) -> Result<Vec<String>> {
        let statement = bind(
            parse(&format!("SELECT * FROM {relation} LIMIT 0"))?,
            &self.state.catalog,
        )?;
        let schema = match statement {
            BoundStatement::Select { schema, .. }
            | BoundStatement::AdvancedSelect { schema, .. }
            | BoundStatement::ViewSelect { schema, .. } => schema,
            _ => {
                return Err(DbError::new(
                    "42809",
                    format!("relation {relation} does not expose a row type"),
                ));
            }
        };
        Ok(schema.fields.into_iter().map(|field| field.name).collect())
    }

    fn begin_exception_block(&mut self) -> Result<()> {
        if self.exception_states.len() >= 128 {
            return Err(DbError::new(
                "54001",
                "PL/pgSQL exception block depth exceeds the maximum of 128",
            ));
        }
        let charge = estimated_database_state_snapshot_bytes(self.state)?
            .saturating_add(estimated_trigger_savepoint_bytes(self.trigger.as_deref()))
            .saturating_add(std::mem::size_of::<usize>());
        let next = self
            .exception_memory
            .bytes()
            .checked_add(charge)
            .ok_or_else(|| {
                DbError::new(
                    "53200",
                    "PL/pgSQL exception savepoint memory accounting overflowed",
                )
            })?;
        self.exception_memory.resize(next)?;
        self.exception_states.push(self.state.clone());
        self.exception_triggers
            .push(self.trigger.as_deref().map(|trigger| TriggerRowSavepoint {
                old: trigger.old.clone(),
                new: trigger.new.clone(),
            }));
        self.exception_charges.push(charge);
        Ok(())
    }

    fn commit_exception_block(&mut self) -> Result<()> {
        self.exception_states
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL exception savepoint stack is empty"))?;
        self.exception_triggers
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL trigger savepoint stack is empty"))?;
        let charge = self
            .exception_charges
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL exception memory stack is empty"))?;
        self.exception_memory
            .resize(self.exception_memory.bytes().saturating_sub(charge))?;
        Ok(())
    }

    fn rollback_exception_block(&mut self) -> Result<()> {
        let pending_notices = mem::take(&mut self.state.pending_notices);
        let saved = self
            .exception_states
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL exception savepoint is not active"))?;
        *self.state = saved;
        self.state.pending_notices = pending_notices;
        let saved_trigger = self
            .exception_triggers
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL trigger savepoint stack is empty"))?;
        let charge = self
            .exception_charges
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL exception memory stack is empty"))?;
        self.exception_memory
            .resize(self.exception_memory.bytes().saturating_sub(charge))?;
        if let Some(trigger) = self.trigger.as_deref_mut()
            && let Some(saved) = saved_trigger
        {
            trigger.old = saved.old;
            trigger.new = saved.new;
        }
        Ok(())
    }

    fn emit_notice(&mut self, notice: DbNotice) -> Result<()> {
        if notice.message.len() > MAX_PLPGSQL_NOTICE_BYTES {
            return Err(DbError::new(
                "54000",
                "PL/pgSQL notice message exceeds the configured byte limit",
            ));
        }
        if self.state.pending_notices.len() >= MAX_PLPGSQL_NOTICES {
            return Err(DbError::new(
                "54001",
                "PL/pgSQL notice count exceeds the configured limit",
            ));
        }
        self.state.pending_notices.push(notice);
        Ok(())
    }

    fn check_cancelled(&self) -> Result<()> {
        if self
            .state
            .cancellation
            .as_ref()
            .is_some_and(|cancellation| cancellation.load(Ordering::Acquire))
        {
            Err(DbError::new("57014", "query was cancelled"))
        } else {
            Ok(())
        }
    }
}

fn scalar_type_of_value(value: &Value) -> Option<ScalarType> {
    value.scalar_type()
}

fn trigger_field_reference(expression: &str) -> Option<(usize, &str)> {
    let (parameter, field) = expression.trim().strip_prefix('$')?.split_once('.')?;
    if field.is_empty()
        || !field
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '_')
    {
        return None;
    }
    Some((parameter.parse::<usize>().ok()?.checked_sub(1)?, field))
}

fn expand_trigger_record_fields(
    sql: &str,
    parameters: &[Value],
    trigger: Option<&TriggerRowContext>,
) -> Result<(String, Vec<Value>, BTreeMap<usize, ScalarType>)> {
    let Some(trigger) = trigger else {
        return Ok((sql.to_owned(), parameters.to_vec(), BTreeMap::new()));
    };
    let characters = sql.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(sql.len());
    let mut expanded = parameters.to_vec();
    let mut parameter_types = BTreeMap::new();
    let mut quote = None;
    let mut index = 0usize;
    while index < characters.len() {
        let character = characters[index];
        if let Some(delimiter) = quote {
            output.push(character);
            if character == delimiter {
                if characters.get(index + 1) == Some(&delimiter) {
                    output.push(delimiter);
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character);
            output.push(character);
            index += 1;
            continue;
        }
        if character != '$' {
            output.push(character);
            index += 1;
            continue;
        }
        let digits_start = index + 1;
        let mut cursor = digits_start;
        while characters.get(cursor).is_some_and(char::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == digits_start || characters.get(cursor) != Some(&'.') {
            output.push(character);
            index += 1;
            continue;
        }
        let field_start = cursor + 1;
        cursor = field_start;
        while characters
            .get(cursor)
            .is_some_and(|value| value.is_ascii_alphanumeric() || *value == '_')
        {
            cursor += 1;
        }
        if cursor == field_start {
            return Err(DbError::new(
                "42601",
                "trigger record access requires an unquoted field name",
            ));
        }
        let parameter = characters[digits_start..field_start - 1]
            .iter()
            .collect::<String>()
            .parse::<usize>()
            .map_err(|_| DbError::new("42P02", "invalid trigger record parameter"))?
            .checked_sub(1)
            .ok_or_else(|| DbError::new("42P02", "trigger parameters are one-based"))?;
        let field = characters[field_start..cursor].iter().collect::<String>();
        let (value, data_type) = trigger.value_and_type(parameter, &field)?;
        expanded.push(value);
        parameter_types.insert(expanded.len(), data_type);
        output.push('$');
        output.push_str(&expanded.len().to_string());
        index = cursor;
    }
    Ok((output, expanded, parameter_types))
}

#[derive(Debug, Clone, Copy)]
struct PlannedMergeAction {
    clause_index: usize,
    target_position: Option<usize>,
    source_position: Option<usize>,
}

fn execute_merge(
    state: &mut DatabaseState,
    merge: BoundMerge,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let BoundMerge {
        target,
        source,
        on,
        clauses,
        returning,
    } = merge;
    if target.offset != 0 || source.offset != target.width {
        return Err(internal_error(
            "MERGE input column offsets are inconsistent",
        ));
    }
    let statement_events = clauses
        .iter()
        .filter_map(|clause| match clause.action {
            BoundMergeAction::Insert { .. } => Some(TriggerEvent::Insert),
            BoundMergeAction::Update { .. } => Some(TriggerEvent::Update),
            BoundMergeAction::Delete => Some(TriggerEvent::Delete),
            BoundMergeAction::DoNothing => None,
        })
        .collect::<BTreeSet<_>>();
    let mut statement_trigger_fired = false;
    for event in &statement_events {
        statement_trigger_fired |= fire_statement_triggers(
            state,
            target.table_id,
            TriggerTiming::BeforeStatement,
            *event,
        )?;
    }
    let target_definition = table_definition(state, target.table_id)?.clone();
    table_definition(state, source.table_id)?;
    let target_rows = state
        .rows
        .get(&target.table_id)
        .cloned()
        .unwrap_or_default();
    let source_rows = state
        .rows
        .get(&source.table_id)
        .cloned()
        .unwrap_or_default();
    let memory = MemoryGrant::new(DEFAULT_SOFT_MEMORY_BYTES, DEFAULT_HARD_MEMORY_BYTES)?;
    let mut plan_reservation = memory.try_reserve(0)?;
    let mut input_reservation = memory.try_reserve(0)?;
    let mut returning_reservation = memory.try_reserve(0)?;
    let mut planned = Vec::new();
    let mut affected_targets = BTreeSet::new();
    let mut matched_targets = BTreeSet::new();

    for (source_position, source_row) in source_rows.iter().enumerate() {
        ensure_statement_not_cancelled(state)?;
        let mut matched = false;
        for (target_position, target_row) in target_rows.iter().enumerate() {
            ensure_statement_not_cancelled(state)?;
            let input = merge_input_row(
                Some(target_row),
                target.width,
                source_row,
                &mut input_reservation,
            )?;
            if !execution_predicate_matches(&on, &input, params)? {
                continue;
            }
            matched = true;
            if matched_targets.insert(target_position) {
                plan_reservation.grow(mem::size_of::<usize>() * 4)?;
            }
            let Some(clause_index) = first_matching_merge_clause(
                &clauses,
                BoundMergeClauseKind::Matched,
                &input,
                params,
            )?
            else {
                continue;
            };
            if matches!(clauses[clause_index].action, BoundMergeAction::DoNothing) {
                continue;
            }
            if !matches!(
                clauses[clause_index].action,
                BoundMergeAction::Update { .. } | BoundMergeAction::Delete
            ) {
                return Err(internal_error(
                    "MERGE matched clause contains an invalid action",
                ));
            }
            if !affected_targets.insert(target_position) {
                return Err(
                    DbError::new("21000", "MERGE command cannot affect row a second time")
                        .with_hint(
                            "Ensure that no more than one source row matches each target row.",
                        ),
                );
            }
            plan_reservation
                .grow(mem::size_of::<PlannedMergeAction>() + mem::size_of::<usize>() * 4)?;
            planned.push(PlannedMergeAction {
                clause_index,
                target_position: Some(target_position),
                source_position: Some(source_position),
            });
        }
        if matched {
            continue;
        }
        let input = merge_input_row(None, target.width, source_row, &mut input_reservation)?;
        let Some(clause_index) = first_matching_merge_clause(
            &clauses,
            BoundMergeClauseKind::NotMatchedByTarget,
            &input,
            params,
        )?
        else {
            continue;
        };
        if matches!(clauses[clause_index].action, BoundMergeAction::DoNothing) {
            continue;
        }
        if !matches!(
            clauses[clause_index].action,
            BoundMergeAction::Insert { .. }
        ) {
            return Err(internal_error(
                "MERGE not-matched clause contains an invalid action",
            ));
        }
        plan_reservation.grow(mem::size_of::<PlannedMergeAction>())?;
        planned.push(PlannedMergeAction {
            clause_index,
            target_position: None,
            source_position: Some(source_position),
        });
    }

    let null_source = Row::new(vec![Value::Null; source.width]);
    input_reservation.grow(estimated_row_bytes(&null_source))?;
    for (target_position, target_row) in target_rows.iter().enumerate() {
        ensure_statement_not_cancelled(state)?;
        if matched_targets.contains(&target_position) {
            continue;
        }
        let input = merge_input_row(
            Some(target_row),
            target.width,
            &null_source,
            &mut input_reservation,
        )?;
        let Some(clause_index) = first_matching_merge_clause(
            &clauses,
            BoundMergeClauseKind::NotMatchedBySource,
            &input,
            params,
        )?
        else {
            continue;
        };
        if matches!(clauses[clause_index].action, BoundMergeAction::DoNothing) {
            continue;
        }
        if !matches!(
            clauses[clause_index].action,
            BoundMergeAction::Update { .. } | BoundMergeAction::Delete
        ) {
            return Err(internal_error(
                "MERGE not-matched-by-source clause contains an invalid action",
            ));
        }
        if !affected_targets.insert(target_position) {
            return Err(internal_error("MERGE planned a target row more than once"));
        }
        plan_reservation
            .grow(mem::size_of::<PlannedMergeAction>() + mem::size_of::<usize>() * 4)?;
        planned.push(PlannedMergeAction {
            clause_index,
            target_position: Some(target_position),
            source_position: None,
        });
    }

    let mut affected = 0_u64;
    let mut returned_rows = Vec::new();
    let mut deleted_targets = BTreeSet::new();
    for action in planned {
        ensure_statement_not_cancelled(state)?;
        let source_row = action
            .source_position
            .map_or(Ok(&null_source), |source_position| {
                source_rows.get(source_position).ok_or_else(|| {
                    internal_error("MERGE source row is outside its statement snapshot")
                })
            })?;
        let clause = clauses
            .get(action.clause_index)
            .ok_or_else(|| internal_error("MERGE clause index is out of bounds"))?;
        let changed_row = match (&clause.action, action.target_position) {
            (BoundMergeAction::Update { assignments }, Some(target_position)) => {
                let old_row = target_rows.get(target_position).ok_or_else(|| {
                    internal_error("MERGE target row is outside its statement snapshot")
                })?;
                let current_position =
                    current_merge_target_position(target_position, &deleted_targets)?;
                let input = merge_input_row(
                    Some(old_row),
                    target.width,
                    source_row,
                    &mut input_reservation,
                )?;
                execute_merge_update(
                    state,
                    target.table_id,
                    current_position,
                    old_row,
                    &input,
                    assignments,
                    params,
                )?
            }
            (BoundMergeAction::Delete, Some(target_position)) => {
                let old_row = target_rows.get(target_position).ok_or_else(|| {
                    internal_error("MERGE target row is outside its statement snapshot")
                })?;
                let current_position =
                    current_merge_target_position(target_position, &deleted_targets)?;
                let deleted =
                    execute_merge_delete(state, target.table_id, current_position, old_row)?;
                if deleted.is_some() {
                    plan_reservation.grow(mem::size_of::<usize>() * 4)?;
                    deleted_targets.insert(target_position);
                }
                deleted
            }
            (
                BoundMergeAction::Insert {
                    column_indexes,
                    values,
                },
                None,
            ) => {
                let input =
                    merge_input_row(None, target.width, source_row, &mut input_reservation)?;
                execute_merge_insert(
                    state,
                    &target_definition,
                    column_indexes,
                    values,
                    &input,
                    params,
                )?
            }
            (BoundMergeAction::DoNothing, _) => None,
            _ => {
                return Err(internal_error(
                    "MERGE action does not match its clause kind",
                ));
            }
        };
        let Some(changed_row) = changed_row else {
            continue;
        };
        if let Some(returning) = &returning {
            let returned = evaluate_returning(returning, &changed_row, params)?;
            returning_reservation.grow(estimated_row_bytes(&returned))?;
            returned_rows.push(returned);
        }
        affected = affected.saturating_add(1);
    }

    for event in &statement_events {
        statement_trigger_fired |= fire_statement_triggers(
            state,
            target.table_id,
            TriggerTiming::AfterStatement,
            *event,
        )?;
    }
    validate_database_rows(state)?;

    Ok((
        dml_command_events(
            returning.as_ref(),
            format!("MERGE {affected}"),
            affected,
            returned_rows,
        ),
        affected != 0 || statement_trigger_fired,
    ))
}

fn first_matching_merge_clause(
    clauses: &[ordadb_sql::BoundMergeClause],
    kind: BoundMergeClauseKind,
    input: &Row,
    params: &[Value],
) -> Result<Option<usize>> {
    for (index, clause) in clauses.iter().enumerate() {
        if clause.kind != kind {
            continue;
        }
        if clause
            .predicate
            .as_ref()
            .map(|predicate| execution_predicate_matches(predicate, input, params))
            .transpose()?
            .unwrap_or(true)
        {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn merge_input_row(
    target: Option<&Row>,
    target_width: usize,
    source: &Row,
    reservation: &mut Reservation,
) -> Result<Row> {
    let target_bytes = target.map_or_else(
        || target_width.saturating_mul(mem::size_of::<Value>()),
        estimated_row_bytes,
    );
    reservation.resize(
        target_bytes
            .saturating_add(estimated_row_bytes(source))
            .saturating_add(mem::size_of::<Row>()),
    )?;
    let mut values = Vec::with_capacity(target_width.saturating_add(source.values.len()));
    match target {
        Some(target) => values.extend(target.values.iter().cloned()),
        None => values.resize(target_width, Value::Null),
    }
    values.extend(source.values.iter().cloned());
    Ok(Row::new(values))
}

fn current_merge_target_position(
    original_position: usize,
    deleted_targets: &BTreeSet<usize>,
) -> Result<usize> {
    if deleted_targets.contains(&original_position) {
        return Err(internal_error("MERGE target row was already deleted"));
    }
    original_position
        .checked_sub(deleted_targets.range(..original_position).count())
        .ok_or_else(|| internal_error("MERGE target position underflowed"))
}

fn execute_merge_update(
    state: &mut DatabaseState,
    table_id: TableId,
    position: usize,
    old_row: &Row,
    input: &Row,
    assignments: &[(usize, BoundExpr)],
    params: &[Value],
) -> Result<Option<Row>> {
    let mut proposed = old_row.clone();
    for (column_index, expression) in assignments {
        proposed.values[*column_index] = evaluate_scalar(expression, &input.values, params)?;
    }
    let replacement = match fire_row_triggers_with_rows(
        state,
        table_id,
        TriggerTiming::Before,
        TriggerEvent::Update,
        Some(old_row),
        Some(&proposed),
    )? {
        RowTriggerOutcome::Proceed(Some(row)) => row,
        RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => return Ok(None),
    };
    let current = state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.get(position))
        .ok_or_else(|| DbError::new("55000", "MERGE target row disappeared before update"))?;
    if current != old_row {
        return Err(DbError::new(
            "55000",
            "BEFORE trigger changed the row targeted by MERGE UPDATE",
        )
        .with_hint("Return a replacement NEW row instead of updating the same row recursively."));
    }
    Arc::make_mut(
        state
            .rows
            .get_mut(&table_id)
            .ok_or_else(|| internal_error("MERGE target rows disappeared"))?,
    )[position] = replacement.clone();
    if replacement != *old_row {
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
        Some(old_row),
        Some(&replacement),
    )?;
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    Ok(Some(replacement))
}

fn execute_merge_delete(
    state: &mut DatabaseState,
    table_id: TableId,
    position: usize,
    old_row: &Row,
) -> Result<Option<Row>> {
    if matches!(
        fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::Before,
            TriggerEvent::Delete,
            Some(old_row),
            None,
        )?,
        RowTriggerOutcome::Suppress
    ) {
        return Ok(None);
    }
    let current = state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.get(position))
        .ok_or_else(|| DbError::new("55000", "MERGE target row disappeared before delete"))?;
    if current != old_row {
        return Err(DbError::new(
            "55000",
            "BEFORE trigger changed the row targeted by MERGE DELETE",
        )
        .with_hint("Return OLD instead of deleting the same row recursively."));
    }
    Arc::make_mut(
        state
            .rows
            .get_mut(&table_id)
            .ok_or_else(|| internal_error("MERGE target rows disappeared"))?,
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
        Some(old_row),
        None,
    )?;
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    Ok(Some(old_row.clone()))
}

fn execute_merge_insert(
    state: &mut DatabaseState,
    table: &TableDefinition,
    column_indexes: &[usize],
    expressions: &[BoundExpr],
    input: &Row,
    params: &[Value],
) -> Result<Option<Row>> {
    let mut values = table
        .columns()
        .iter()
        .map(|column| column_default_value(&state.catalog, column))
        .collect::<Result<Vec<_>>>()?;
    for (expression, column_index) in expressions.iter().zip(column_indexes) {
        values[*column_index] = evaluate_scalar(expression, &input.values, params)?;
    }
    let proposed = Row::new(values);
    let inserted = match fire_row_triggers_with_rows(
        state,
        table.id,
        TriggerTiming::Before,
        TriggerEvent::Insert,
        None,
        Some(&proposed),
    )? {
        RowTriggerOutcome::Proceed(Some(row)) => row,
        RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => return Ok(None),
    };
    validate_rows(&state.catalog, table, std::slice::from_ref(&inserted))?;
    Arc::make_mut(
        state
            .rows
            .entry(table.id)
            .or_insert_with(|| Arc::new(Vec::new())),
    )
    .push(inserted.clone());
    validate_database_rows(state)?;
    rebuild_table_derived(state, table.id)?;
    let _ = fire_row_triggers_with_rows(
        state,
        table.id,
        TriggerTiming::After,
        TriggerEvent::Insert,
        None,
        Some(&inserted),
    )?;
    validate_database_rows(state)?;
    rebuild_table_derived(state, table.id)?;
    Ok(Some(inserted))
}
