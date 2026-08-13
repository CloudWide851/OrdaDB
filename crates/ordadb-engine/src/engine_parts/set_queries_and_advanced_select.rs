
fn sort_set_rows(rows: &mut [Row], order_by: &[BoundOrder]) -> Result<()> {
    let mut error = None;
    rows.sort_by(|left, right| {
        compare_set_rows(left, right, order_by).unwrap_or_else(|sort_error| {
            error = Some(sort_error);
            std::cmp::Ordering::Equal
        })
    });
    error.map_or(Ok(()), Err)
}

fn compare_set_rows(
    left: &Row,
    right: &Row,
    order_by: &[BoundOrder],
) -> Result<std::cmp::Ordering> {
    for order in order_by {
        let left = left
            .values
            .get(order.column_index)
            .ok_or_else(|| internal_error("set-operation sort column is out of bounds"))?;
        let right = right
            .values
            .get(order.column_index)
            .ok_or_else(|| internal_error("set-operation sort column is out of bounds"))?;
        let ordering = match (left.is_null(), right.is_null()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => {
                if order.nulls_first.unwrap_or(!order.ascending) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (false, true) => {
                if order.nulls_first.unwrap_or(!order.ascending) {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (false, false) => {
                let ordering = compare_execution_values(left, right)?;
                if order.ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            }
        };
        if ordering != std::cmp::Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(std::cmp::Ordering::Equal)
}

fn evaluate_set_offset(offset: Option<&BoundExpr>, params: &[Value]) -> Result<usize> {
    let Some(offset) = offset else {
        return Ok(0);
    };
    match evaluate_scalar(offset, &[], params)? {
        Value::Int64(value) if value >= 0 => usize::try_from(value)
            .map_err(|_| DbError::new("22003", "OFFSET value is out of range")),
        Value::Null => Ok(0),
        _ => Err(DbError::new(
            "2201X",
            "OFFSET must be a non-negative integer",
        )),
    }
}

fn evaluate_set_limit(limit: Option<&BoundExpr>, params: &[Value]) -> Result<usize> {
    let Some(limit) = limit else {
        return Ok(usize::MAX);
    };
    match evaluate_scalar(limit, &[], params)? {
        Value::Int64(value) if value >= 0 => {
            usize::try_from(value).map_err(|_| DbError::new("22003", "LIMIT value is out of range"))
        }
        Value::Null => Ok(usize::MAX),
        _ => Err(DbError::new(
            "2201W",
            "LIMIT must be a non-negative integer",
        )),
    }
}

fn select_rows_events(schema: Schema, rows: Vec<Row>) -> Vec<QueryEvent> {
    let count = rows.len() as u64;
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
    } else if count == 0 {
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
    events
}

fn prepare_select_cursor(
    state: &DatabaseState,
    execution: SelectExecution,
    params: &[Value],
    table_provider: Option<&dyn TableProvider>,
    options: &ExecutionOptions,
) -> Result<(Schema, ExecutionCursor)> {
    let SelectExecution {
        table_id,
        schema,
        projection,
        filter,
        order_by,
        offset,
        limit,
    } = execution;
    let plan = optimize_select(
        table_definition(state, table_id)?,
        projection,
        filter,
        order_by,
        offset,
        limit,
    );
    let context = ExecutionContext {
        tables: &state.rows,
        indexes: &state.indexes,
        params,
    };
    let cursor = match table_provider {
        Some(table_provider) => ExecutionCursor::with_options_and_table_provider(
            &plan,
            &context,
            schema.clone(),
            options.clone(),
            Some(table_provider),
        )?,
        None => ExecutionCursor::with_options(&plan, &context, schema.clone(), options.clone())?,
    };
    Ok((schema, cursor))
}

fn execute_advanced_select(
    state: &DatabaseState,
    execution: AdvancedExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let (schema, mut cursor) =
        prepare_advanced_cursor(state, execution, params, &ExecutionOptions::default())?;
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

fn prepare_advanced_cursor(
    state: &DatabaseState,
    execution: AdvancedExecution,
    params: &[Value],
    options: &ExecutionOptions,
) -> Result<(Schema, AdvancedExecutionCursor)> {
    let AdvancedExecution {
        table,
        joins,
        applies,
        windows,
        schema,
        projection,
        distinct,
        filter,
        group_by,
        having,
        order_by,
        offset,
        limit,
        aggregate,
    } = execution;
    let context = ExecutionContext {
        tables: &state.rows,
        indexes: &state.indexes,
        params,
    };
    let applies = applies
        .into_iter()
        .map(|apply| build_apply_execution_plan(state, apply, params.len(), &[]))
        .collect::<Result<Vec<_>>>()?;
    let joins = joins
        .into_iter()
        .map(|join| build_join_execution_plan(state, join, params.len(), &[]))
        .collect::<Result<Vec<_>>>()?;
    let cursor = AdvancedExecutionCursor::with_options_and_cancellation(
        AdvancedExecutionPlan {
            table,
            joins,
            applies,
            windows,
            schema: schema.clone(),
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
            aggregate,
        },
        &context,
        options.clone(),
        state.cancellation.clone(),
    )?;
    Ok((schema, cursor))
}

fn build_join_execution_plan(
    state: &DatabaseState,
    join: BoundJoin,
    parameter_base: usize,
    ancestor_slots: &[BTreeMap<usize, usize>],
) -> Result<JoinExecutionPlan> {
    let BoundJoin { source, kind, on } = join;
    let source = match source {
        BoundJoinSource::Table(table) => JoinExecutionSource::Table(table),
        BoundJoinSource::Derived {
            mut query,
            offset,
            width,
            ..
        } => {
            let correlation =
                rewrite_statement_correlations(&mut query, parameter_base, ancestor_slots)?;
            let nested_parameter_base = parameter_base
                .checked_add(correlation.indexes.len())
                .ok_or_else(|| DbError::new("54001", "LATERAL parameter depth overflowed"))?;
            let mut nested_ancestors = Vec::with_capacity(ancestor_slots.len().saturating_add(1));
            nested_ancestors.push(correlation.parameter_slots);
            nested_ancestors.extend_from_slice(ancestor_slots);
            JoinExecutionSource::Derived {
                query: Box::new(build_query_execution_plan(
                    state,
                    *query,
                    nested_parameter_base,
                    &nested_ancestors,
                )?),
                correlation_indexes: correlation.indexes,
                offset,
                width,
            }
        }
    };
    Ok(JoinExecutionPlan { source, kind, on })
}

fn build_apply_execution_plan(
    state: &DatabaseState,
    apply: BoundApply,
    parameter_base: usize,
    ancestor_slots: &[BTreeMap<usize, usize>],
) -> Result<ApplyExecutionPlan> {
    let BoundApply { kind, mut query } = apply;
    let correlation = rewrite_statement_correlations(&mut query, parameter_base, ancestor_slots)?;
    let nested_parameter_base = parameter_base
        .checked_add(correlation.indexes.len())
        .ok_or_else(|| DbError::new("54001", "correlation parameter depth overflowed"))?;
    let mut nested_ancestors = Vec::with_capacity(ancestor_slots.len().saturating_add(1));
    nested_ancestors.push(correlation.parameter_slots);
    nested_ancestors.extend_from_slice(ancestor_slots);
    let kind = match kind {
        BoundApplyKind::Scalar => ApplyExecutionKind::Scalar,
        BoundApplyKind::Exists { negated } => ApplyExecutionKind::Exists { negated },
        BoundApplyKind::In { left, negated } => ApplyExecutionKind::In { left, negated },
        BoundApplyKind::Quantified {
            left,
            op,
            quantifier,
        } => ApplyExecutionKind::Quantified {
            left,
            op,
            quantifier,
        },
        BoundApplyKind::RowScalar {
            left,
            op,
            operand_types,
        } => ApplyExecutionKind::RowScalar {
            left,
            op,
            operand_types,
        },
        BoundApplyKind::RowQuantified {
            left,
            op,
            quantifier,
            negated,
            operand_types,
        } => ApplyExecutionKind::RowQuantified {
            left,
            op,
            quantifier,
            negated,
            operand_types,
        },
    };
    Ok(ApplyExecutionPlan {
        kind,
        query: Box::new(build_query_execution_plan(
            state,
            *query,
            nested_parameter_base,
            &nested_ancestors,
        )?),
        correlation_indexes: correlation.indexes,
    })
}

fn build_query_execution_plan(
    state: &DatabaseState,
    statement: BoundStatement,
    parameter_base: usize,
    ancestor_slots: &[BTreeMap<usize, usize>],
) -> Result<QueryExecutionPlan> {
    match statement {
        BoundStatement::Select {
            table_id,
            schema,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => Ok(QueryExecutionPlan::Simple {
            plan: Box::new(optimize_select(
                table_definition(state, table_id)?,
                projection,
                filter,
                order_by,
                offset,
                limit,
            )),
            schema,
        }),
        BoundStatement::AdvancedSelect {
            table,
            joins,
            applies,
            windows,
            schema,
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
            aggregate,
        } => Ok(QueryExecutionPlan::Advanced(Box::new(
            AdvancedExecutionPlan {
                table,
                joins: joins
                    .into_iter()
                    .map(|join| {
                        build_join_execution_plan(state, join, parameter_base, ancestor_slots)
                    })
                    .collect::<Result<Vec<_>>>()?,
                applies: applies
                    .into_iter()
                    .map(|apply| {
                        build_apply_execution_plan(state, apply, parameter_base, ancestor_slots)
                    })
                    .collect::<Result<Vec<_>>>()?,
                windows,
                schema,
                projection,
                distinct,
                filter,
                group_by,
                having,
                order_by,
                offset,
                limit: limit.map(|limit| *limit),
                aggregate,
            },
        ))),
        _ => Err(DbError::new(
            "0A000",
            "Apply subqueries currently support SELECT query bodies only",
        )),
    }
}

struct CorrelationRewrite {
    indexes: Vec<usize>,
    parameter_slots: BTreeMap<usize, usize>,
}

fn collect_forwarded_correlation_indexes(statement: &BoundStatement) -> Result<BTreeSet<usize>> {
    let mut indexes = BTreeSet::new();
    let mut statements = vec![(statement, 0_usize)];
    while let Some((statement, query_depth)) = statements.pop() {
        let target_depth = query_depth.checked_add(1).ok_or_else(|| {
            DbError::new(
                "54001",
                "correlation scope depth exceeds the implementation limit",
            )
        })?;
        match statement {
            BoundStatement::Select {
                projection,
                filter,
                order_by,
                offset,
                limit,
                ..
            } => {
                for projection in projection {
                    collect_expr_correlations(&projection.expr, target_depth, &mut indexes);
                }
                if let Some(filter) = filter {
                    collect_expr_correlations(filter, target_depth, &mut indexes);
                }
                for order in order_by {
                    if let Some(expression) = &order.expression {
                        collect_expr_correlations(expression, target_depth, &mut indexes);
                    }
                }
                if let Some(offset) = offset {
                    collect_expr_correlations(offset, target_depth, &mut indexes);
                }
                if let Some(limit) = limit {
                    collect_expr_correlations(limit, target_depth, &mut indexes);
                }
            }
            BoundStatement::AdvancedSelect {
                joins,
                applies,
                windows,
                projection,
                filter,
                group_by,
                having,
                order_by,
                offset,
                limit,
                ..
            } => {
                for join in joins {
                    collect_expr_correlations(&join.on, target_depth, &mut indexes);
                    if let BoundJoinSource::Derived { query, .. } = &join.source {
                        statements.push((query, target_depth));
                    }
                }
                for apply in applies {
                    match &apply.kind {
                        BoundApplyKind::In { left, .. }
                        | BoundApplyKind::Quantified { left, .. } => {
                            collect_expr_correlations(left, target_depth, &mut indexes);
                        }
                        BoundApplyKind::RowScalar { left, .. }
                        | BoundApplyKind::RowQuantified { left, .. } => {
                            for expression in left {
                                collect_expr_correlations(expression, target_depth, &mut indexes);
                            }
                        }
                        BoundApplyKind::Scalar | BoundApplyKind::Exists { .. } => {}
                    }
                    statements.push((&apply.query, target_depth));
                }
                for window in windows {
                    for argument in &window.arguments {
                        collect_expr_correlations(argument, target_depth, &mut indexes);
                    }
                    if let Some(filter) = &window.filter {
                        collect_expr_correlations(filter, target_depth, &mut indexes);
                    }
                    for expression in &window.partition_by {
                        collect_expr_correlations(expression, target_depth, &mut indexes);
                    }
                    for order in &window.order_by {
                        if let Some(expression) = &order.expression {
                            collect_expr_correlations(expression, target_depth, &mut indexes);
                        }
                    }
                    if let Some(frame) = &window.frame {
                        for bound in [&frame.start_bound, &frame.end_bound] {
                            if let BoundWindowFrameBound::Preceding(expression)
                            | BoundWindowFrameBound::Following(expression) = bound
                            {
                                collect_expr_correlations(expression, target_depth, &mut indexes);
                            }
                        }
                    }
                }
                for projection in projection {
                    collect_expr_correlations(&projection.expr, target_depth, &mut indexes);
                }
                if let Some(filter) = filter {
                    collect_expr_correlations(filter, target_depth, &mut indexes);
                }
                for expression in group_by {
                    collect_expr_correlations(expression, target_depth, &mut indexes);
                }
                if let Some(having) = having {
                    collect_expr_correlations(having, target_depth, &mut indexes);
                }
                for order in order_by {
                    if let Some(expression) = &order.expression {
                        collect_expr_correlations(expression, target_depth, &mut indexes);
                    }
                }
                if let Some(offset) = offset {
                    collect_expr_correlations(offset, target_depth, &mut indexes);
                }
                if let Some(limit) = limit {
                    collect_expr_correlations(limit, target_depth, &mut indexes);
                }
            }
            _ => {}
        }
    }
    Ok(indexes)
}

fn collect_expr_correlations(
    expression: &BoundExpr,
    target_depth: usize,
    indexes: &mut BTreeSet<usize>,
) {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        match &expression.kind {
            BoundExprKind::Correlation { depth, index } => {
                if *depth == target_depth {
                    indexes.insert(*index);
                }
            }
            BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => pending.push(expr),
            BoundExprKind::Array { elements, .. } => pending.extend(elements.iter().rev()),
            BoundExprKind::Function { arguments, .. } => pending.extend(arguments.iter().rev()),
            BoundExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundExprKind::InList { expr, list, .. } => {
                pending.extend(list.iter().rev());
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
            | BoundExprKind::ApplyValue { .. } => {}
        }
    }
}

fn rewrite_statement_correlations(
    statement: &mut BoundStatement,
    parameter_base: usize,
    ancestor_slots: &[BTreeMap<usize, usize>],
) -> Result<CorrelationRewrite> {
    let mut correlation_indexes = Vec::new();
    let mut slots = BTreeMap::new();
    for index in collect_forwarded_correlation_indexes(statement)? {
        correlation_parameter_slot(index, parameter_base, &mut slots, &mut correlation_indexes)?;
    }
    match statement {
        BoundStatement::Select {
            projection,
            filter,
            order_by,
            offset,
            limit,
            ..
        } => {
            for projection in projection {
                rewrite_expr_correlations(
                    &mut projection.expr,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            if let Some(filter) = filter {
                rewrite_expr_correlations(
                    filter,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            for order in order_by {
                if let Some(expression) = &mut order.expression {
                    rewrite_expr_correlations(
                        expression,
                        parameter_base,
                        &mut slots,
                        &mut correlation_indexes,
                        ancestor_slots,
                    )?;
                }
            }
            if let Some(offset) = offset {
                rewrite_expr_correlations(
                    offset,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            if let Some(limit) = limit {
                rewrite_expr_correlations(
                    limit,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
        }
        BoundStatement::AdvancedSelect {
            joins,
            applies,
            windows,
            projection,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
            ..
        } => {
            for join in joins {
                rewrite_expr_correlations(
                    &mut join.on,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            for apply in applies {
                match &mut apply.kind {
                    BoundApplyKind::In { left, .. } | BoundApplyKind::Quantified { left, .. } => {
                        rewrite_expr_correlations(
                            left,
                            parameter_base,
                            &mut slots,
                            &mut correlation_indexes,
                            ancestor_slots,
                        )?;
                    }
                    BoundApplyKind::RowScalar { left, .. }
                    | BoundApplyKind::RowQuantified { left, .. } => {
                        for expression in left {
                            rewrite_expr_correlations(
                                expression,
                                parameter_base,
                                &mut slots,
                                &mut correlation_indexes,
                                ancestor_slots,
                            )?;
                        }
                    }
                    BoundApplyKind::Scalar | BoundApplyKind::Exists { .. } => {}
                }
            }
            for window in windows {
                for argument in &mut window.arguments {
                    rewrite_expr_correlations(
                        argument,
                        parameter_base,
                        &mut slots,
                        &mut correlation_indexes,
                        ancestor_slots,
                    )?;
                }
                if let Some(filter) = &mut window.filter {
                    rewrite_expr_correlations(
                        filter,
                        parameter_base,
                        &mut slots,
                        &mut correlation_indexes,
                        ancestor_slots,
                    )?;
                }
                for expression in &mut window.partition_by {
                    rewrite_expr_correlations(
                        expression,
                        parameter_base,
                        &mut slots,
                        &mut correlation_indexes,
                        ancestor_slots,
                    )?;
                }
                for order in &mut window.order_by {
                    if let Some(expression) = &mut order.expression {
                        rewrite_expr_correlations(
                            expression,
                            parameter_base,
                            &mut slots,
                            &mut correlation_indexes,
                            ancestor_slots,
                        )?;
                    }
                }
                if let Some(frame) = &mut window.frame {
                    for bound in [&mut frame.start_bound, &mut frame.end_bound] {
                        if let BoundWindowFrameBound::Preceding(expression)
                        | BoundWindowFrameBound::Following(expression) = bound
                        {
                            rewrite_expr_correlations(
                                expression,
                                parameter_base,
                                &mut slots,
                                &mut correlation_indexes,
                                ancestor_slots,
                            )?;
                        }
                    }
                }
            }
            for projection in projection {
                rewrite_expr_correlations(
                    &mut projection.expr,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            if let Some(filter) = filter {
                rewrite_expr_correlations(
                    filter,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            for expression in group_by {
                rewrite_expr_correlations(
                    expression,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            if let Some(having) = having {
                rewrite_expr_correlations(
                    having,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            for order in order_by {
                if let Some(expression) = &mut order.expression {
                    rewrite_expr_correlations(
                        expression,
                        parameter_base,
                        &mut slots,
                        &mut correlation_indexes,
                        ancestor_slots,
                    )?;
                }
            }
            if let Some(offset) = offset {
                rewrite_expr_correlations(
                    offset,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            if let Some(limit) = limit {
                rewrite_expr_correlations(
                    limit,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
        }
        _ => {}
    }
    Ok(CorrelationRewrite {
        indexes: correlation_indexes,
        parameter_slots: slots,
    })
}
