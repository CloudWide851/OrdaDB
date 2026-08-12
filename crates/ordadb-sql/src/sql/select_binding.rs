
fn lower_window_expr(
    mut expr: ParsedExpr,
    inputs: &[InputColumn],
    windows: &mut Vec<BoundWindow>,
) -> Result<ParsedExpr> {
    let position = expr.position;
    expr.kind = match expr.kind {
        ParsedExprKind::Unary { op, expr } => ParsedExprKind::Unary {
            op,
            expr: Box::new(lower_window_expr(*expr, inputs, windows)?),
        },
        ParsedExprKind::Cast {
            expr,
            data_type,
            declared_type,
        } => ParsedExprKind::Cast {
            expr: Box::new(lower_window_expr(*expr, inputs, windows)?),
            data_type,
            declared_type,
        },
        ParsedExprKind::Array {
            elements,
            dimensions,
        } => ParsedExprKind::Array {
            elements: elements
                .into_iter()
                .map(|element| lower_window_expr(element, inputs, windows))
                .collect::<Result<Vec<_>>>()?,
            dimensions,
        },
        ParsedExprKind::Function {
            function,
            arguments,
        } => ParsedExprKind::Function {
            function,
            arguments: arguments
                .into_iter()
                .map(|argument| lower_window_expr(argument, inputs, windows))
                .collect::<Result<Vec<_>>>()?,
        },
        ParsedExprKind::Binary { left, op, right } => ParsedExprKind::Binary {
            left: Box::new(lower_window_expr(*left, inputs, windows)?),
            op,
            right: Box::new(lower_window_expr(*right, inputs, windows)?),
        },
        ParsedExprKind::InList {
            expr,
            list,
            negated,
        } => ParsedExprKind::InList {
            expr: Box::new(lower_window_expr(*expr, inputs, windows)?),
            list: list
                .into_iter()
                .map(|candidate| lower_window_expr(candidate, inputs, windows))
                .collect::<Result<Vec<_>>>()?,
            negated,
        },
        ParsedExprKind::InSubquery {
            expr,
            subquery,
            negated,
        } => ParsedExprKind::InSubquery {
            expr: Box::new(lower_window_expr(*expr, inputs, windows)?),
            subquery,
            negated,
        },
        ParsedExprKind::QuantifiedSubquery {
            left,
            op,
            quantifier,
            subquery,
        } => ParsedExprKind::QuantifiedSubquery {
            left: Box::new(lower_window_expr(*left, inputs, windows)?),
            op,
            quantifier,
            subquery,
        },
        ParsedExprKind::RowSubquery {
            left,
            op,
            quantifier,
            negated,
            subquery,
        } => ParsedExprKind::RowSubquery {
            left: left
                .into_iter()
                .map(|expression| lower_window_expr(expression, inputs, windows))
                .collect::<Result<Vec<_>>>()?,
            op,
            quantifier,
            negated,
            subquery,
        },
        ParsedExprKind::Aggregate {
            argument,
            distinct,
            filter,
            function,
        } => {
            if argument.as_deref().is_some_and(expr_has_window)
                || filter.as_deref().is_some_and(expr_has_window)
            {
                return Err(
                    DbError::new("42P20", "window function calls cannot be nested")
                        .with_position_opt(position),
                );
            }
            ParsedExprKind::Aggregate {
                function,
                argument,
                distinct,
                filter,
            }
        }
        ParsedExprKind::Window { call, spec } => {
            let call = *call;
            let spec = *spec;
            if spec.window_name.is_some() {
                return Err(DbError::internal(
                    "window inheritance was not resolved before binding",
                ));
            }
            if spec.partition_by.iter().any(expr_has_window)
                || spec
                    .order_by
                    .iter()
                    .any(|order| expr_has_window(&order.expr))
            {
                return Err(
                    DbError::new("42P20", "window function calls cannot be nested")
                        .with_position_opt(position),
                );
            }
            if spec.partition_by.iter().any(expr_has_subquery)
                || spec
                    .order_by
                    .iter()
                    .any(|order| expr_has_subquery(&order.expr))
            {
                return unsupported_at(
                    "subquery expressions in window definitions are not supported yet",
                    position,
                );
            }
            let call = bind_window_call(call, inputs, position)?;
            let partition_by = spec
                .partition_by
                .into_iter()
                .map(|expr| bind_expr_multi(expr, inputs, None, true))
                .collect::<Result<Vec<_>>>()?;
            let order_by = spec
                .order_by
                .into_iter()
                .map(|order| {
                    let expression = bind_expr_multi(order.expr.clone(), inputs, None, true)?;
                    bound_expression_order(order, expression)
                })
                .collect::<Result<Vec<_>>>()?;
            let frame = spec
                .frame
                .map(|frame| bind_window_frame(frame, inputs, &order_by, position))
                .transpose()?;
            let ordinal = windows.len();
            windows.push(BoundWindow {
                function: call.function,
                value_index: usize::MAX,
                arguments: call.arguments,
                count_star: call.count_star,
                filter: call.filter,
                partition_by,
                order_by,
                frame,
                data_type: call.data_type,
                nullable: call.nullable,
            });
            ParsedExprKind::WindowValue { ordinal }
        }
        ParsedExprKind::NamedWindow { .. } => {
            return Err(DbError::internal("named window reference was not resolved"));
        }
        ParsedExprKind::WindowValue { .. } => {
            return Err(DbError::internal(
                "window expression was lowered more than once",
            ));
        }
        kind => kind,
    };
    Ok(expr)
}

fn finalize_window_values(
    expression: &mut ParsedExpr,
    window_base: usize,
    windows: &[BoundWindow],
) -> Result<()> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        if let ParsedExprKind::WindowValue { ordinal } = &expression.kind {
            let index = window_base
                .checked_add(*ordinal)
                .ok_or_else(|| DbError::new("54001", "window value index overflowed"))?;
            let window = windows.get(*ordinal).ok_or_else(|| {
                DbError::internal("window value ordinal is outside the bound window list")
            })?;
            expression.kind = ParsedExprKind::ApplyValue {
                index,
                data_type: window.data_type.clone(),
                nullable: window.nullable,
            };
            continue;
        }
        match &mut expression.kind {
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                pending.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => pending.extend(elements.iter_mut().rev()),
            ParsedExprKind::Function { arguments, .. } => {
                pending.extend(arguments.iter_mut().rev());
            }
            ParsedExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                pending.extend(list.iter_mut().rev());
                pending.push(expr);
            }
            ParsedExprKind::InSubquery { expr, .. }
            | ParsedExprKind::QuantifiedSubquery { left: expr, .. } => pending.push(expr),
            ParsedExprKind::RowSubquery { left, .. } => pending.extend(left.iter_mut().rev()),
            ParsedExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            ParsedExprKind::Window { .. } => {
                return Err(DbError::internal("window expression was not lowered"));
            }
            ParsedExprKind::NamedWindow { .. } => {
                return Err(DbError::internal("named window reference was not resolved"));
            }
            ParsedExprKind::WindowValue { .. } => unreachable!("handled above"),
            ParsedExprKind::Column(_)
            | ParsedExprKind::Literal(_)
            | ParsedExprKind::Parameter(_)
            | ParsedExprKind::ResolvedParameter { .. }
            | ParsedExprKind::ScalarSubquery(_)
            | ParsedExprKind::Exists { .. }
            | ParsedExprKind::ApplyValue { .. } => {}
        }
    }
    Ok(())
}

fn bind_advanced_select(
    input: AdvancedSelectInput,
    catalog: &Catalog,
    view_depth: usize,
    outer_inputs: &[InputColumn],
) -> Result<BoundStatement> {
    let AdvancedSelectInput {
        table,
        joins,
        mut projection,
        distinct,
        mut filter,
        mut group_by,
        mut having,
        mut order_by,
        offset,
        limit,
    } = input;
    let mut local_inputs = Vec::new();
    let table = bind_input_table(table, false, catalog, &mut local_inputs)?;
    let mut bound_joins = Vec::new();
    for join in joins {
        if expr_has_window(&join.on) {
            return Err(DbError::new(
                "42P20",
                "window functions are not allowed in JOIN conditions",
            ));
        }
        let nullable = join.kind == JoinKind::Left;
        let source = bind_join_source(
            join.source,
            nullable,
            catalog,
            view_depth,
            outer_inputs,
            &mut local_inputs,
        )?;
        let inputs = inputs_with_outer(&local_inputs, outer_inputs)?;
        let on = bind_multi_boolean(join.on, &inputs)?;
        if bound_expr_has_aggregate(&on) {
            return Err(DbError::new(
                "42803",
                "aggregate functions are not allowed in JOIN conditions",
            ));
        }
        bound_joins.push(BoundJoin {
            source,
            kind: join.kind,
            on,
        });
    }
    let inputs = inputs_with_outer(&local_inputs, outer_inputs)?;
    let apply_base = local_inputs.len();
    let mut applies = Vec::new();
    let mut windows = Vec::new();

    if filter.as_ref().is_some_and(expr_has_window) {
        return Err(DbError::new(
            "42P20",
            "window functions are not allowed in WHERE",
        ));
    }
    if group_by.iter().any(expr_has_window) {
        return Err(DbError::new(
            "42P20",
            "window functions are not allowed in GROUP BY",
        ));
    }
    if having.as_ref().is_some_and(expr_has_window) {
        return Err(DbError::new(
            "42P20",
            "window functions are not allowed in HAVING",
        ));
    }
    if limit.as_ref().is_some_and(expr_has_window) || offset.as_ref().is_some_and(expr_has_window) {
        return Err(DbError::new(
            "42P20",
            "window functions are not allowed in LIMIT or OFFSET",
        ));
    }

    projection = projection
        .into_iter()
        .map(|projection| match projection {
            ParsedProjection::Wildcard => Ok(ParsedProjection::Wildcard),
            ParsedProjection::Expression { expr, alias } => Ok(ParsedProjection::Expression {
                expr: lower_window_expr(expr, &inputs, &mut windows)?,
                alias,
            }),
        })
        .collect::<Result<Vec<_>>>()?;
    order_by = order_by
        .into_iter()
        .map(|mut order| {
            order.expr = lower_window_expr(order.expr, &inputs, &mut windows)?;
            Ok(order)
        })
        .collect::<Result<Vec<_>>>()?;

    projection = projection
        .into_iter()
        .map(|projection| match projection {
            ParsedProjection::Wildcard => Ok(ParsedProjection::Wildcard),
            ParsedProjection::Expression { expr, alias } => Ok(ParsedProjection::Expression {
                expr: lower_apply_expr(
                    expr,
                    catalog,
                    &inputs,
                    apply_base,
                    &mut applies,
                    view_depth,
                )?,
                alias,
            }),
        })
        .collect::<Result<Vec<_>>>()?;
    filter = filter
        .map(|expr| lower_apply_expr(expr, catalog, &inputs, apply_base, &mut applies, view_depth))
        .transpose()?;
    group_by = group_by
        .into_iter()
        .map(|expr| lower_apply_expr(expr, catalog, &inputs, apply_base, &mut applies, view_depth))
        .collect::<Result<Vec<_>>>()?;
    having = having
        .map(|expr| lower_apply_expr(expr, catalog, &inputs, apply_base, &mut applies, view_depth))
        .transpose()?;
    order_by = order_by
        .into_iter()
        .map(|mut order| {
            order.expr = lower_apply_expr(
                order.expr,
                catalog,
                &inputs,
                apply_base,
                &mut applies,
                view_depth,
            )?;
            Ok(order)
        })
        .collect::<Result<Vec<_>>>()?;

    let window_base = apply_base
        .checked_add(applies.len())
        .ok_or_else(|| DbError::new("54001", "window value index overflowed"))?;
    for (ordinal, window) in windows.iter_mut().enumerate() {
        window.value_index = window_base
            .checked_add(ordinal)
            .ok_or_else(|| DbError::new("54001", "window value index overflowed"))?;
    }
    for projection in &mut projection {
        if let ParsedProjection::Expression { expr, .. } = projection {
            finalize_window_values(expr, window_base, &windows)?;
        }
    }
    for order in &mut order_by {
        finalize_window_values(&mut order.expr, window_base, &windows)?;
    }

    let mut bound_projection = Vec::new();
    for item in projection {
        match item {
            ParsedProjection::Wildcard => {
                for input in &local_inputs {
                    bound_projection.push(BoundProjection {
                        expr: BoundExpr {
                            kind: BoundExprKind::Column { index: input.index },
                            data_type: input.data_type.clone(),
                            nullable: input.nullable,
                        },
                        field: Field::new(
                            input.name.as_str(),
                            input.data_type.clone(),
                            input.nullable,
                        ),
                    });
                }
            }
            ParsedProjection::Expression { expr, alias } => {
                let default_name = projection_name(&expr);
                let bound = bind_expr_multi(expr, &inputs, None, true)?;
                bound_projection.push(BoundProjection {
                    field: Field::new(
                        alias
                            .as_ref()
                            .map_or(default_name.as_str(), |alias| alias.name.as_str()),
                        bound.data_type.clone(),
                        bound.nullable,
                    ),
                    expr: bound,
                });
            }
        }
    }
    if bound_projection.is_empty() {
        return Err(DbError::new(SYNTAX_ERROR, "SELECT projection is empty"));
    }
    if distinct
        && bound_projection
            .iter()
            .any(|projection| projection.expr.data_type == ScalarType::Json)
    {
        return Err(DbError::new(
            "42883",
            "could not identify an equality operator for type json",
        ));
    }

    let filter = filter
        .map(|expr| bind_multi_boolean(expr, &inputs))
        .transpose()?;
    if filter.as_ref().is_some_and(bound_expr_has_aggregate) {
        return Err(DbError::new(
            "42803",
            "aggregate functions are not allowed in WHERE",
        ));
    }
    let group_by = group_by
        .into_iter()
        .map(|expr| bind_expr_multi(expr, &inputs, None, false))
        .collect::<Result<Vec<_>>>()?;
    let having = having
        .map(|expr| bind_multi_boolean(expr, &inputs))
        .transpose()?;
    let aggregate = !group_by.is_empty()
        || bound_projection
            .iter()
            .any(|projection| bound_expr_has_aggregate(&projection.expr))
        || having.as_ref().is_some_and(bound_expr_has_aggregate)
        || windows.iter().any(bound_window_input_has_aggregate);
    if aggregate {
        for projection in &bound_projection {
            if window_ordinal_for_expr(&projection.expr, &windows).is_some() {
                continue;
            }
            if bound_expr_has_window_slot(&projection.expr, &windows) {
                return unsupported(
                    "grouped window functions must be top-level SELECT expressions",
                );
            }
            validate_grouped_expr(&projection.expr, &group_by)?;
        }
        if let Some(having) = &having {
            validate_grouped_expr(having, &group_by)?;
        }
        remap_grouped_window_inputs(&mut windows, &bound_projection, &group_by)?;
    } else if having.is_some() {
        return Err(DbError::new(
            "42803",
            "HAVING requires grouping or an aggregate",
        ));
    }

    let order_by = order_by
        .into_iter()
        .map(|order| {
            if aggregate {
                bind_projected_order(order, &bound_projection, &inputs, &group_by)
            } else if distinct {
                bind_distinct_order(order, &bound_projection, &inputs)
            } else {
                bind_multi_order(order, &bound_projection, &inputs)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if limit.as_ref().is_some_and(expr_has_subquery)
        || offset.as_ref().is_some_and(expr_has_subquery)
    {
        return unsupported("subqueries in LIMIT or OFFSET are not supported yet");
    }
    let limit = limit
        .map(|expr| bind_expr_multi(expr, &inputs, Some(&ScalarType::Int64), false))
        .transpose()?;
    let offset = offset
        .map(|expr| bind_expr_multi(expr, &inputs, Some(&ScalarType::Int64), false))
        .transpose()?;
    let schema = Schema::new(
        bound_projection
            .iter()
            .map(|projection| projection.field.clone())
            .collect(),
    );
    Ok(BoundStatement::AdvancedSelect {
        table,
        joins: bound_joins,
        applies,
        windows,
        schema,
        projection: bound_projection,
        distinct,
        filter,
        group_by,
        having,
        order_by,
        offset,
        limit: limit.map(Box::new),
        aggregate,
    })
}

fn bind_join_source(
    source: ParsedJoinSource,
    nullable: bool,
    catalog: &Catalog,
    view_depth: usize,
    outer_inputs: &[InputColumn],
    local_inputs: &mut Vec<InputColumn>,
) -> Result<BoundJoinSource> {
    match source {
        ParsedJoinSource::Table(table) => {
            bind_input_table(table, nullable, catalog, local_inputs).map(BoundJoinSource::Table)
        }
        ParsedJoinSource::Derived {
            lateral,
            query,
            alias,
            columns,
        } => {
            let alias_position = alias.position;
            let binding = alias.name;
            if local_inputs.iter().any(|input| input.binding == binding) {
                return Err(DbError::new(
                    "42712",
                    format!("table name {binding} specified more than once"),
                )
                .with_position_opt(alias_position));
            }
            let visible_inputs = if lateral {
                inputs_with_outer(local_inputs, outer_inputs)?
            } else {
                Vec::new()
            };
            let nested_depth = view_depth.checked_add(1).ok_or_else(|| {
                DbError::new(
                    "54001",
                    "derived table nesting exceeds the implementation limit",
                )
            })?;
            let query = bind_apply_query(*query, catalog, nested_depth, &visible_inputs)?;
            let schema = bound_query_schema(&query)?;
            if columns.len() > schema.fields.len() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "derived table has more column aliases than output columns",
                )
                .with_position_opt(alias_position));
            }
            let offset = local_inputs.len();
            let width = schema.fields.len();
            local_inputs.extend(schema.fields.iter().enumerate().map(|(index, field)| {
                let name = columns.get(index).map_or_else(
                    || Identifier::unquoted(&field.name),
                    |alias| alias.name.clone(),
                );
                InputColumn {
                    binding: binding.clone(),
                    name,
                    index: offset + index,
                    data_type: field.data_type.clone(),
                    nullable: nullable || field.nullable,
                    outer_depth: 0,
                }
            }));
            Ok(BoundJoinSource::Derived {
                lateral,
                query: Box::new(query),
                binding,
                offset,
                width,
                nullable,
            })
        }
    }
}

fn inputs_with_outer(
    local_inputs: &[InputColumn],
    outer_inputs: &[InputColumn],
) -> Result<Vec<InputColumn>> {
    let mut inputs = Vec::with_capacity(local_inputs.len().saturating_add(outer_inputs.len()));
    inputs.extend_from_slice(local_inputs);
    for mut input in outer_inputs.iter().cloned() {
        input.outer_depth = input.outer_depth.checked_add(1).ok_or_else(|| {
            DbError::new(
                "54001",
                "correlation scope depth exceeds the implementation limit",
            )
        })?;
        inputs.push(input);
    }
    Ok(inputs)
}

fn bind_input_table(
    parsed: ParsedTable,
    nullable: bool,
    catalog: &Catalog,
    inputs: &mut Vec<InputColumn>,
) -> Result<BoundTable> {
    let table = resolve_table(&parsed.name, catalog)?;
    let binding = parsed
        .alias
        .as_ref()
        .map_or_else(|| table.name.clone(), |alias| alias.name.clone());
    if inputs.iter().any(|input| input.binding == binding) {
        return Err(DbError::new(
            "42712",
            format!("table name {binding} specified more than once"),
        ));
    }
    let offset = inputs.len();
    inputs.extend(
        table
            .columns()
            .iter()
            .enumerate()
            .map(|(column_offset, column)| InputColumn {
                binding: binding.clone(),
                name: column.name.clone(),
                index: offset + column_offset,
                data_type: column.data_type.clone(),
                nullable: nullable || column.nullable,
                outer_depth: 0,
            }),
    );
    Ok(BoundTable {
        table_id: table.id,
        binding,
        offset,
        width: table.columns().len(),
        nullable,
    })
}

fn bind_multi_boolean(expr: ParsedExpr, inputs: &[InputColumn]) -> Result<BoundExpr> {
    let position = expr.position;
    let bound = bind_expr_multi(expr, inputs, Some(&ScalarType::Boolean), true)?;
    if bound.data_type != ScalarType::Boolean {
        return Err(DbError::new(DATATYPE_MISMATCH, "predicate must be boolean")
            .with_position_opt(position));
    }
    Ok(bound)
}
