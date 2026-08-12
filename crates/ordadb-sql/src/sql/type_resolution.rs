
fn resolve_statement_types(
    statement: &mut ParsedStatement,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    if depth >= MAX_PARAMETER_SOLVER_DEPTH {
        return Err(DbError::new(
            "54001",
            "type resolution exceeded its statement depth limit",
        ));
    }
    match statement {
        ParsedStatement::CreateTable {
            columns,
            constraints,
            ..
        } => {
            for column in columns {
                if let Some(default) = &mut column.default {
                    resolve_expr_types(
                        &mut default.expression,
                        parameter_types,
                        catalog,
                        depth + 1,
                        session,
                    )?;
                }
            }
            for constraint in constraints {
                resolve_constraint_types(constraint, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedStatement::AlterTable { operations, .. } => {
            for operation in operations {
                match operation {
                    ParsedAlterTableOperation::AddColumn { column, .. } => {
                        if let Some(default) = &mut column.default {
                            resolve_expr_types(
                                &mut default.expression,
                                parameter_types,
                                catalog,
                                depth + 1,
                                session,
                            )?;
                        }
                    }
                    ParsedAlterTableOperation::SetDefault { default, .. } => {
                        resolve_expr_types(
                            &mut default.expression,
                            parameter_types,
                            catalog,
                            depth + 1,
                            session,
                        )?;
                    }
                    ParsedAlterTableOperation::AddConstraint { constraint } => {
                        resolve_constraint_types(
                            constraint,
                            parameter_types,
                            catalog,
                            depth + 1,
                            session,
                        )?;
                    }
                    _ => {}
                }
            }
        }
        ParsedStatement::CreateDomain {
            default: Some(default),
            ..
        } => {
            resolve_expr_types(
                &mut default.expression,
                parameter_types,
                catalog,
                depth + 1,
                session,
            )?;
        }
        ParsedStatement::AlterDomain {
            operation: ParsedAlterDomainOperation::SetDefault(default),
            ..
        } => {
            resolve_expr_types(
                &mut default.expression,
                parameter_types,
                catalog,
                depth + 1,
                session,
            )?;
        }
        ParsedStatement::CreateView { query, .. }
        | ParsedStatement::Explain { statement: query } => {
            resolve_statement_types(query, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Call { arguments, .. }
        | ParsedStatement::RoutineSelect { arguments, .. } => {
            for argument in arguments {
                resolve_expr_types(argument, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedStatement::PgNotify {
            channel,
            payload,
            alias: _,
        } => {
            resolve_expr_types(channel, parameter_types, catalog, depth + 1, session)?;
            resolve_expr_types(payload, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::ScalarSelect { projection } => {
            resolve_projection_types(projection, parameter_types, catalog, depth + 1, session)?
        }
        ParsedStatement::SequenceValue {
            operation: ParsedSequenceOperation::SetValue { value, .. },
            ..
        } => {
            resolve_expr_types(value, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Insert {
            rows,
            on_conflict,
            returning,
            ..
        } => {
            for expression in rows.iter_mut().flatten() {
                resolve_expr_types(expression, parameter_types, catalog, depth + 1, session)?;
            }
            if let Some(on_conflict) = on_conflict
                && let ParsedConflictAction::DoUpdate {
                    assignments,
                    filter,
                } = &mut on_conflict.action
            {
                for (_, expression) in assignments {
                    resolve_expr_types(expression, parameter_types, catalog, depth + 1, session)?;
                }
                if let Some(filter) = filter {
                    resolve_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
                }
            }
            resolve_projection_types(returning, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Merge(merge) => {
            resolve_expr_types(&mut merge.on, parameter_types, catalog, depth + 1, session)?;
            for clause in &mut merge.clauses {
                if let Some(predicate) = &mut clause.predicate {
                    resolve_expr_types(predicate, parameter_types, catalog, depth + 1, session)?;
                }
                match &mut clause.action {
                    ParsedMergeAction::Update { assignments } => {
                        for (_, expression) in assignments {
                            resolve_expr_types(
                                expression,
                                parameter_types,
                                catalog,
                                depth + 1,
                                session,
                            )?;
                        }
                    }
                    ParsedMergeAction::Insert { values, .. } => {
                        for expression in values {
                            resolve_expr_types(
                                expression,
                                parameter_types,
                                catalog,
                                depth + 1,
                                session,
                            )?;
                        }
                    }
                    ParsedMergeAction::Delete | ParsedMergeAction::DoNothing => {}
                }
            }
            resolve_projection_types(
                &mut merge.returning,
                parameter_types,
                catalog,
                depth + 1,
                session,
            )?;
        }
        ParsedStatement::With { ctes, body, .. } => {
            for cte in ctes {
                resolve_statement_types(
                    &mut cte.query,
                    parameter_types,
                    catalog,
                    depth + 1,
                    session,
                )?;
            }
            resolve_statement_types(body, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::SetOperation {
            left,
            right,
            order_by,
            offset,
            limit,
            ..
        } => {
            resolve_statement_types(left, parameter_types, catalog, depth + 1, session)?;
            resolve_statement_types(right, parameter_types, catalog, depth + 1, session)?;
            resolve_orders_types(order_by, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(offset, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(limit, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Select {
            projection,
            filter,
            order_by,
            offset,
            limit,
            ..
        } => {
            resolve_projection_types(projection, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            resolve_orders_types(order_by, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(offset, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(limit, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::AdvancedSelect {
            joins,
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
                if let ParsedJoinSource::Derived { query, .. } = &mut join.source {
                    resolve_statement_types(query, parameter_types, catalog, depth + 1, session)?;
                }
                resolve_expr_types(&mut join.on, parameter_types, catalog, depth + 1, session)?;
            }
            resolve_projection_types(projection, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            for expression in group_by {
                resolve_expr_types(expression, parameter_types, catalog, depth + 1, session)?;
            }
            resolve_optional_expr_types(having, parameter_types, catalog, depth + 1, session)?;
            resolve_orders_types(order_by, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(offset, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(limit, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Update {
            assignments,
            filter,
            returning,
            ..
        } => {
            for (_, expression) in assignments {
                resolve_expr_types(expression, parameter_types, catalog, depth + 1, session)?;
            }
            resolve_optional_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            resolve_projection_types(returning, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Delete {
            filter, returning, ..
        } => {
            resolve_optional_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            resolve_projection_types(returning, parameter_types, catalog, depth + 1, session)?;
        }
        _ => {}
    }
    Ok(())
}

fn resolve_constraint_types(
    constraint: &mut ParsedTableConstraint,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    if let ParsedTableConstraint::Check { expression, .. } = constraint {
        resolve_expr_types(expression, parameter_types, catalog, depth, session)?;
    }
    Ok(())
}

fn resolve_projection_types(
    projection: &mut [ParsedProjection],
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    for item in projection {
        if let ParsedProjection::Expression { expr, alias } = item {
            if alias.is_none()
                && let Some(name) = session_function_name(expr)
            {
                *alias = Some(ParsedIdentifier {
                    name: Identifier::unquoted(name),
                    position: expr.position,
                });
            }
            resolve_expr_types(expr, parameter_types, catalog, depth, session)?;
        }
    }
    Ok(())
}

fn session_function_name(expression: &ParsedExpr) -> Option<&'static str> {
    let ParsedExprKind::Function { function, .. } = &expression.kind else {
        return None;
    };
    match function {
        ScalarFunction::Version => Some("version"),
        ScalarFunction::CurrentDatabase => Some("current_database"),
        ScalarFunction::CurrentUser => Some("current_user"),
        ScalarFunction::SessionUser => Some("session_user"),
        ScalarFunction::CurrentSetting => Some("current_setting"),
        _ => None,
    }
}

fn resolve_orders_types(
    order_by: &mut [ParsedOrder],
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    for order in order_by {
        resolve_expr_types(&mut order.expr, parameter_types, catalog, depth, session)?;
    }
    Ok(())
}

fn resolve_optional_expr_types(
    expression: &mut Option<ParsedExpr>,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    if let Some(expression) = expression {
        resolve_expr_types(expression, parameter_types, catalog, depth, session)?;
    }
    Ok(())
}

fn session_function_value(
    function: ScalarFunction,
    arguments: &[ParsedExpr],
    session: Option<SessionBindValues<'_>>,
    position: Option<usize>,
) -> Result<Option<Value>> {
    let Some(session) = session else {
        return match function {
            ScalarFunction::Version
            | ScalarFunction::CurrentDatabase
            | ScalarFunction::CurrentUser
            | ScalarFunction::SessionUser
            | ScalarFunction::CurrentSetting => Err(DbError::new(
                "55000",
                "session scalar function requires database session metadata",
            )
            .with_position_opt(position)),
            _ => Ok(None),
        };
    };
    let value = match function {
        ScalarFunction::Version => Value::Text(session.version.to_owned()),
        ScalarFunction::CurrentDatabase => Value::Text(session.current_database.to_owned()),
        ScalarFunction::CurrentUser => Value::Text(session.current_user.to_owned()),
        ScalarFunction::SessionUser => Value::Text(session.session_user.to_owned()),
        ScalarFunction::CurrentSetting => {
            let Some(ParsedExpr {
                kind: ParsedExprKind::Literal(Value::Text(name)),
                ..
            }) = arguments.first()
            else {
                return unsupported_at("current_setting requires a literal setting name", position);
            };
            let missing_ok = match arguments.get(1) {
                None => false,
                Some(ParsedExpr {
                    kind: ParsedExprKind::Literal(Value::Boolean(value)),
                    ..
                }) => *value,
                Some(_) => {
                    return unsupported_at(
                        "current_setting missing_ok must be a boolean literal",
                        position,
                    );
                }
            };
            let name = name.trim().to_ascii_lowercase();
            match session.settings.get(&name) {
                Some(value) => Value::Text(value.clone()),
                None if missing_ok => Value::Null,
                None => {
                    return Err(DbError::new(
                        "42704",
                        format!("unrecognized configuration parameter {name}"),
                    )
                    .with_position_opt(position));
                }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

fn resolve_expr_types(
    expression: &mut ParsedExpr,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    if depth >= MAX_PARAMETER_SOLVER_DEPTH {
        return Err(DbError::new(
            "54001",
            "type resolution exceeded its expression depth limit",
        ));
    }
    if let ParsedExprKind::Cast {
        data_type,
        declared_type: Some(type_name),
        ..
    } = &mut expression.kind
        && let Some(catalog) = catalog
    {
        let (resolved, _) = resolve_declared_data_type(catalog, data_type, type_name)?;
        *data_type = resolved;
    }
    if let ParsedExprKind::Parameter(index) = expression.kind
        && let Some(data_type) = parameter_types.get(&index)
    {
        expression.kind = ParsedExprKind::ResolvedParameter {
            index,
            data_type: data_type.clone(),
        };
        return Ok(());
    }
    let session_value = match &expression.kind {
        ParsedExprKind::Function {
            function,
            arguments,
        } => session_function_value(*function, arguments, session, expression.position)?,
        _ => None,
    };
    if let Some(value) = session_value {
        expression.kind = ParsedExprKind::Literal(value);
        return Ok(());
    }
    match &mut expression.kind {
        ParsedExprKind::Unary { expr, .. } => {
            resolve_expr_types(expr, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::Cast { expr, .. } => {
            resolve_expr_types(expr, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::Array { elements, .. } => {
            for element in elements {
                resolve_expr_types(element, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedExprKind::Function { arguments, .. } => {
            for argument in arguments {
                resolve_expr_types(argument, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedExprKind::Binary { left, right, .. } => {
            resolve_expr_types(left, parameter_types, catalog, depth + 1, session)?;
            resolve_expr_types(right, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::InList { expr, list, .. } => {
            resolve_expr_types(expr, parameter_types, catalog, depth + 1, session)?;
            for candidate in list {
                resolve_expr_types(candidate, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedExprKind::ScalarSubquery(subquery) | ParsedExprKind::Exists { subquery, .. } => {
            resolve_statement_types(subquery, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::InSubquery { expr, subquery, .. }
        | ParsedExprKind::QuantifiedSubquery {
            left: expr,
            subquery,
            ..
        } => {
            resolve_expr_types(expr, parameter_types, catalog, depth + 1, session)?;
            resolve_statement_types(subquery, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::RowSubquery { left, subquery, .. } => {
            for expression in left {
                resolve_expr_types(expression, parameter_types, catalog, depth + 1, session)?;
            }
            resolve_statement_types(subquery, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::Aggregate {
            argument, filter, ..
        } => {
            if let Some(argument) = argument {
                resolve_expr_types(argument, parameter_types, catalog, depth + 1, session)?;
            }
            if let Some(filter) = filter {
                resolve_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedExprKind::Window { call, spec } => {
            resolve_window_types(call, spec, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::NamedWindow { call, .. } => {
            for argument in &mut call.arguments {
                resolve_expr_types(argument, parameter_types, catalog, depth + 1, session)?;
            }
            if let Some(filter) = &mut call.filter {
                resolve_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedExprKind::Column(_)
        | ParsedExprKind::Literal(_)
        | ParsedExprKind::Parameter(_)
        | ParsedExprKind::ResolvedParameter { .. }
        | ParsedExprKind::ApplyValue { .. }
        | ParsedExprKind::WindowValue { .. } => {}
    }
    Ok(())
}

fn resolve_window_types(
    call: &mut ParsedWindowCall,
    spec: &mut ParsedWindowSpec,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    for argument in &mut call.arguments {
        resolve_expr_types(argument, parameter_types, catalog, depth, session)?;
    }
    if let Some(filter) = &mut call.filter {
        resolve_expr_types(filter, parameter_types, catalog, depth, session)?;
    }
    for expression in &mut spec.partition_by {
        resolve_expr_types(expression, parameter_types, catalog, depth, session)?;
    }
    resolve_orders_types(&mut spec.order_by, parameter_types, catalog, depth, session)?;
    if let Some(frame) = &mut spec.frame {
        for bound in [&mut frame.start_bound, &mut frame.end_bound] {
            if let ParsedWindowFrameBound::Preceding(expression)
            | ParsedWindowFrameBound::Following(expression) = bound
            {
                resolve_expr_types(expression, parameter_types, catalog, depth, session)?;
            }
        }
    }
    Ok(())
}
