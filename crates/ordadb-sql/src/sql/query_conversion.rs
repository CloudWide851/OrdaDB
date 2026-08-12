
fn convert_select_query(query: Query, sql: &str) -> Result<ParsedStatement> {
    let with = query.with;
    if !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return unsupported("this SELECT query form is not supported yet");
    }
    let (body, top_limit) = match *query.body {
        SetExpr::Select(select) => {
            let mut select = *select;
            let top_limit = match select.top.take() {
                None => None,
                Some(top) if top.with_ties || top.percent => {
                    return unsupported("TOP PERCENT and TOP WITH TIES are not supported");
                }
                Some(top) => match top.quantity {
                    Some(TopQuantity::Expr(expression)) => Some(convert_expr(expression, sql)?),
                    Some(TopQuantity::Constant(value)) => {
                        let value = i64::try_from(value)
                            .map_err(|_| DbError::new("22003", "TOP value is out of range"))?;
                        Some(ParsedExpr {
                            kind: ParsedExprKind::Literal(Value::Int64(value)),
                            position: None,
                        })
                    }
                    None => return unsupported("TOP requires an explicit row count"),
                },
            };
            (SetExpr::Select(Box::new(select)), top_limit)
        }
        body => (body, None),
    };

    let order_by = match query.order_by {
        None => Vec::new(),
        Some(order) => {
            if order.interpolate.is_some() {
                return unsupported("ORDER BY INTERPOLATE is not supported");
            }
            let OrderByKind::Expressions(expressions) = order.kind else {
                return unsupported("ORDER BY ALL is not supported");
            };
            expressions
                .into_iter()
                .map(|order| {
                    if order.with_fill.is_some() {
                        return unsupported("ORDER BY WITH FILL is not supported");
                    }
                    Ok(ParsedOrder {
                        expr: convert_expr(order.expr, sql)?,
                        ascending: order.options.asc.unwrap_or(true),
                        nulls_first: order.options.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?
        }
    };

    let fetch_limit = match query.fetch {
        None => None,
        Some(fetch) if fetch.with_ties || fetch.percent => {
            return unsupported("FETCH PERCENT and FETCH WITH TIES are not supported");
        }
        Some(fetch) => Some(
            fetch
                .quantity
                .map(|expression| convert_expr(expression, sql))
                .transpose()?
                .unwrap_or(ParsedExpr {
                    kind: ParsedExprKind::Literal(Value::Int64(1)),
                    position: None,
                }),
        ),
    };
    let (limit, offset) = match query.limit_clause {
        None => (None, None),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) if limit_by.is_empty() => (
            limit.map(|expr| convert_expr(expr, sql)).transpose()?,
            offset
                .map(|offset| convert_expr(offset.value, sql))
                .transpose()?,
        ),
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => (
            Some(convert_expr(limit, sql)?),
            Some(convert_expr(offset, sql)?),
        ),
        Some(_) => {
            return unsupported("LIMIT BY and unrepresentable row-limit forms are not supported");
        }
    };
    let limit = match (top_limit, limit, fetch_limit) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) | (_, Some(_), Some(_)) => {
            return unsupported("a query may specify only one row-limit form");
        }
        (top, limit, fetch) => top.or(limit).or(fetch),
    };

    let statement = match body {
        SetExpr::Select(select) => convert_select(*select, order_by, offset, limit, sql),
        SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } => convert_set_operation(
            *left,
            op,
            set_quantifier,
            *right,
            order_by,
            offset,
            limit,
            sql,
            0,
        ),
        SetExpr::Query(query) if order_by.is_empty() && offset.is_none() && limit.is_none() => {
            convert_select_query(*query, sql)
        }
        SetExpr::Query(_) => unsupported(
            "outer ORDER BY, OFFSET, and LIMIT on a parenthesized query are not supported yet",
        ),
        SetExpr::Values(_) => unsupported("standalone VALUES queries are not supported yet"),
        SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Delete(_)
        | SetExpr::Merge(_)
        | SetExpr::Table(_) => unsupported("this query body is not supported in a set operation"),
    }?;
    if let Some(with) = with {
        convert_with_clause(with, statement, sql)
    } else {
        Ok(statement)
    }
}

fn convert_with_clause(with: SqlWith, body: ParsedStatement, sql: &str) -> Result<ParsedStatement> {
    let recursive = with.recursive;
    let ctes = with
        .cte_tables
        .into_iter()
        .map(|cte| {
            if cte.from.is_some() {
                return unsupported("CTE FROM modifiers are not supported");
            }
            Ok(ParsedCte {
                name: convert_ident(cte.alias.name, sql),
                columns: cte
                    .alias
                    .columns
                    .into_iter()
                    .map(|column| convert_ident(column.name, sql))
                    .collect(),
                query: Box::new(convert_select_query(*cte.query, sql)?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if ctes.is_empty() {
        return Err(DbError::new(SYNTAX_ERROR, "WITH requires at least one CTE"));
    }
    Ok(ParsedStatement::With {
        recursive,
        ctes,
        body: Box::new(body),
    })
}

#[allow(clippy::too_many_arguments)]
fn convert_set_operation(
    left: SetExpr,
    operator: SqlSetOperator,
    quantifier: SqlSetQuantifier,
    right: SetExpr,
    order_by: Vec<ParsedOrder>,
    offset: Option<ParsedExpr>,
    limit: Option<ParsedExpr>,
    sql: &str,
    depth: usize,
) -> Result<ParsedStatement> {
    if depth >= 64 {
        return Err(DbError::new(
            "54001",
            "set operation nesting exceeds the maximum depth of 64",
        ));
    }
    let operator = match operator {
        SqlSetOperator::Union => QuerySetOperator::Union,
        SqlSetOperator::Intersect => QuerySetOperator::Intersect,
        SqlSetOperator::Except => QuerySetOperator::Except,
        SqlSetOperator::Minus => return unsupported("MINUS set operations are not supported"),
    };
    let all = match quantifier {
        SqlSetQuantifier::All => true,
        SqlSetQuantifier::None | SqlSetQuantifier::Distinct => false,
        SqlSetQuantifier::ByName
        | SqlSetQuantifier::AllByName
        | SqlSetQuantifier::DistinctByName => {
            return unsupported("BY NAME set operations are not supported");
        }
    };
    Ok(ParsedStatement::SetOperation {
        left: Box::new(convert_set_operand(left, sql, depth + 1)?),
        operator,
        all,
        right: Box::new(convert_set_operand(right, sql, depth + 1)?),
        order_by,
        offset,
        limit,
    })
}

fn convert_set_operand(expr: SetExpr, sql: &str, depth: usize) -> Result<ParsedStatement> {
    match expr {
        SetExpr::Select(select) => convert_select(*select, Vec::new(), None, None, sql),
        SetExpr::Query(query) => convert_select_query(*query, sql),
        SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } => convert_set_operation(
            *left,
            op,
            set_quantifier,
            *right,
            Vec::new(),
            None,
            None,
            sql,
            depth,
        ),
        SetExpr::Values(_) => {
            unsupported("VALUES operands are not supported in set operations yet")
        }
        SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Delete(_)
        | SetExpr::Merge(_)
        | SetExpr::Table(_) => unsupported("this query body is not supported in a set operation"),
    }
}

fn convert_select(
    select: Select,
    mut order_by: Vec<ParsedOrder>,
    offset: Option<ParsedExpr>,
    limit: Option<ParsedExpr>,
    sql: &str,
) -> Result<ParsedStatement> {
    let distinct = match select.distinct.as_ref() {
        None | Some(SqlDistinct::All) => false,
        Some(SqlDistinct::Distinct) => true,
        Some(SqlDistinct::On(_)) => return unsupported("DISTINCT ON is not supported yet"),
    };
    if !select.optimizer_hints.is_empty()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return unsupported("extended SELECT clauses are not supported yet");
    }
    if select.from.is_empty() {
        if distinct {
            return unsupported("SELECT DISTINCT without FROM is not supported yet");
        }
        if !select.named_window.is_empty() {
            return unsupported("named WINDOW clauses without FROM are not supported yet");
        }
        return convert_routine_select(select, order_by, offset, limit, sql);
    }
    let named_windows = convert_named_windows(select.named_window, sql)?;
    if select.from.len() != 1 {
        return unsupported("SELECT supports exactly one table");
    }

    let mut projection = convert_projection_items(select.projection, sql)?;
    let mut filter = select
        .selection
        .map(|expr| convert_expr(expr, sql))
        .transpose()?;
    let mut group_by = match select.group_by {
        GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => expressions
            .into_iter()
            .map(|expr| convert_expr(expr, sql))
            .collect::<Result<Vec<_>>>()?,
        GroupByExpr::Expressions(_, _) => {
            return unsupported("GROUP BY modifiers are not supported yet");
        }
        GroupByExpr::All(_) => return unsupported("GROUP BY ALL is not supported yet"),
    };
    let mut having = select
        .having
        .map(|expr| convert_expr(expr, sql))
        .transpose()?;
    for projection in &mut projection {
        if let ParsedProjection::Expression { expr, .. } = projection {
            resolve_named_window_expr(expr, &named_windows)?;
        }
    }
    if let Some(filter) = &mut filter {
        resolve_named_window_expr(filter, &named_windows)?;
    }
    for expression in &mut group_by {
        resolve_named_window_expr(expression, &named_windows)?;
    }
    if let Some(having) = &mut having {
        resolve_named_window_expr(having, &named_windows)?;
    }
    for order in &mut order_by {
        resolve_named_window_expr(&mut order.expr, &named_windows)?;
    }
    let from = select
        .from
        .into_iter()
        .next()
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "SELECT requires a table"))?;
    let advanced = distinct
        || matches!(&from.relation, TableFactor::Table { alias: Some(_), .. })
        || !from.joins.is_empty()
        || !group_by.is_empty()
        || having.is_some()
        || projection.iter().any(|projection| {
            projection_has_aggregate(projection)
                || projection_has_subquery(projection)
                || projection_has_window(projection)
        })
        || filter
            .as_ref()
            .is_some_and(|expr| expr_has_subquery(expr) || expr_has_window(expr))
        || order_by.iter().any(|order| expr_has_window(&order.expr));
    if advanced {
        let (table, joins) = convert_select_from(from, sql)?;
        Ok(ParsedStatement::AdvancedSelect {
            table,
            joins,
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
        })
    } else {
        Ok(ParsedStatement::Select {
            table: convert_table_with_joins(from, sql)?,
            projection,
            filter,
            order_by,
            offset,
            limit,
        })
    }
}

fn convert_on_conflict(on: Option<SqlOnInsert>, sql: &str) -> Result<Option<ParsedOnConflict>> {
    let Some(on) = on else {
        return Ok(None);
    };
    let SqlOnInsert::OnConflict(conflict) = on else {
        return unsupported("ON DUPLICATE KEY UPDATE is not PostgreSQL ON CONFLICT");
    };
    let target = conflict
        .conflict_target
        .map(|target| match target {
            SqlConflictTarget::Columns(columns) => columns
                .into_iter()
                .map(|column| convert_single_identifier(column.into(), sql))
                .collect::<Result<Vec<_>>>()
                .map(ParsedConflictTarget::Columns),
            SqlConflictTarget::OnConstraint(name) => {
                convert_object_name(name, sql).map(ParsedConflictTarget::Constraint)
            }
        })
        .transpose()?;
    let action = match conflict.action {
        SqlOnConflictAction::DoNothing => ParsedConflictAction::DoNothing,
        SqlOnConflictAction::DoUpdate(update) => ParsedConflictAction::DoUpdate {
            assignments: convert_assignments(update.assignments, sql)?,
            filter: update
                .selection
                .map(|expr| convert_expr(expr, sql))
                .transpose()?,
        },
    };
    Ok(Some(ParsedOnConflict { target, action }))
}

fn convert_merge(merge: SqlMerge, sql: &str) -> Result<ParsedStatement> {
    let SqlMerge {
        merge_token: _,
        optimizer_hints,
        into,
        table,
        source,
        on,
        clauses,
        output,
    } = merge;
    if !optimizer_hints.is_empty() {
        return unsupported("MERGE optimizer hints are not supported");
    }
    if !into {
        return Err(DbError::new(SYNTAX_ERROR, "MERGE requires INTO"));
    }
    if clauses.is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "MERGE requires at least one WHEN clause",
        ));
    }
    let returning = match output {
        None => Vec::new(),
        Some(SqlOutputClause::Returning { select_items, .. }) => {
            convert_projection_items(select_items, sql)?
        }
        Some(SqlOutputClause::Output { .. }) => {
            return unsupported("MERGE OUTPUT is not supported");
        }
    };
    let clause_tokens = merge_clause_token_info(&significant_tokens(sql)).ok_or_else(|| {
        DbError::internal("MERGE token audit could not identify the statement clauses")
    })?;
    if clause_tokens.len() != clauses.len() {
        return Err(DbError::internal(
            "MERGE token audit disagrees with the parsed clause count",
        ));
    }
    let clauses = clauses
        .into_iter()
        .enumerate()
        .map(|(clause_index, clause)| {
            let kind = match clause.clause_kind {
                SqlMergeClauseKind::Matched => ParsedMergeClauseKind::Matched,
                SqlMergeClauseKind::NotMatched | SqlMergeClauseKind::NotMatchedByTarget => {
                    ParsedMergeClauseKind::NotMatchedByTarget
                }
                SqlMergeClauseKind::NotMatchedBySource => ParsedMergeClauseKind::NotMatchedBySource,
            };
            let action = if clause_tokens[clause_index].do_nothing.is_some() {
                ParsedMergeAction::DoNothing
            } else {
                match clause.action {
                    SqlMergeAction::Update(update) => {
                        if kind == ParsedMergeClauseKind::NotMatchedByTarget {
                            return Err(DbError::new(
                                SYNTAX_ERROR,
                                "MERGE UPDATE requires WHEN MATCHED or WHEN NOT MATCHED BY SOURCE",
                            ));
                        }
                        if update.update_predicate.is_some() || update.delete_predicate.is_some() {
                            return unsupported("Oracle MERGE UPDATE predicates are not supported");
                        }
                        ParsedMergeAction::Update {
                            assignments: convert_assignments(update.assignments, sql)?,
                        }
                    }
                    SqlMergeAction::Delete { .. } => {
                        if kind == ParsedMergeClauseKind::NotMatchedByTarget {
                            return Err(DbError::new(
                                SYNTAX_ERROR,
                                "MERGE DELETE requires WHEN MATCHED or WHEN NOT MATCHED BY SOURCE",
                            ));
                        }
                        ParsedMergeAction::Delete
                    }
                    SqlMergeAction::Insert(mut insert) => {
                        if kind != ParsedMergeClauseKind::NotMatchedByTarget {
                            return Err(DbError::new(
                                SYNTAX_ERROR,
                                "MERGE INSERT requires WHEN NOT MATCHED",
                            ));
                        }
                        if insert.insert_predicate.is_some() {
                            return unsupported("Oracle MERGE INSERT predicates are not supported");
                        }
                        let columns = insert
                            .columns
                            .into_iter()
                            .map(|column| convert_single_identifier(column, sql))
                            .collect::<Result<Vec<_>>>()?;
                        let values = match insert.kind {
                            SqlMergeInsertKind::Values(ref mut values)
                                if !values.explicit_row
                                    && !values.value_keyword
                                    && values.rows.len() == 1 =>
                            {
                                values
                                    .rows
                                    .pop()
                                    .ok_or_else(|| {
                                        DbError::new(SYNTAX_ERROR, "MERGE INSERT VALUES is empty")
                                    })?
                                    .content
                                    .into_iter()
                                    .map(|expr| convert_expr(expr, sql))
                                    .collect::<Result<Vec<_>>>()?
                            }
                            SqlMergeInsertKind::Values(_) => {
                                return unsupported(
                                    "MERGE INSERT requires exactly one standard VALUES row",
                                );
                            }
                            SqlMergeInsertKind::Row => {
                                return unsupported("MERGE INSERT ROW is not supported");
                            }
                        };
                        ParsedMergeAction::Insert { columns, values }
                    }
                }
            };
            Ok(ParsedMergeClause {
                kind,
                predicate: clause
                    .predicate
                    .map(|predicate| convert_expr(predicate, sql))
                    .transpose()?,
                action,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ParsedStatement::Merge(ParsedMerge {
        target: convert_select_table(table, sql)?,
        source: convert_select_table(source, sql)?,
        on: convert_expr(*on, sql)?,
        clauses,
        returning,
    }))
}

fn convert_assignments(
    assignments: Vec<SqlAssignment>,
    sql: &str,
) -> Result<Vec<(ParsedIdentifier, ParsedExpr)>> {
    assignments
        .into_iter()
        .map(|assignment| {
            let AssignmentTarget::ColumnName(name) = assignment.target else {
                return unsupported("tuple assignments are not supported yet");
            };
            Ok((
                convert_single_identifier(name, sql)?,
                convert_expr(assignment.value, sql)?,
            ))
        })
        .collect()
}

fn convert_projection_items(
    projection: Vec<SelectItem>,
    sql: &str,
) -> Result<Vec<ParsedProjection>> {
    projection
        .into_iter()
        .map(|item| match item {
            SelectItem::Wildcard(_) => Ok(ParsedProjection::Wildcard),
            SelectItem::UnnamedExpr(expr) => Ok(ParsedProjection::Expression {
                expr: convert_expr(expr, sql)?,
                alias: None,
            }),
            SelectItem::ExprWithAlias { expr, alias } => Ok(ParsedProjection::Expression {
                expr: convert_expr(expr, sql)?,
                alias: Some(convert_ident(alias, sql)),
            }),
            _ => unsupported("qualified wildcards and multiple aliases are not supported yet"),
        })
        .collect()
}

fn convert_routine_select(
    select: Select,
    order_by: Vec<ParsedOrder>,
    offset: Option<ParsedExpr>,
    limit: Option<ParsedExpr>,
    sql: &str,
) -> Result<ParsedStatement> {
    if select.selection.is_some()
        || select.having.is_some()
        || !order_by.is_empty()
        || offset.is_some()
        || limit.is_some()
        || !matches!(
            select.group_by,
            GroupByExpr::Expressions(ref expressions, ref modifiers)
                if expressions.is_empty() && modifiers.is_empty()
        )
    {
        return unsupported("scalar routine SELECT does not support query clauses");
    }
    if let [projection] = select.projection.as_slice() {
        let (expression, alias) = match projection {
            SelectItem::UnnamedExpr(expression) => (expression.clone(), None),
            SelectItem::ExprWithAlias { expr, alias } => {
                (expr.clone(), Some(convert_ident(alias.clone(), sql)))
            }
            _ => return unsupported("scalar SELECT does not support wildcards"),
        };
        if let SqlExpr::Function(function) = &expression {
            let function_name = function.name.to_string().to_ascii_lowercase();
            if matches!(function_name.as_str(), "pg_notify" | "pg_catalog.pg_notify") {
                let (_, mut arguments) = convert_routine_invocation(function.clone(), sql)?;
                if arguments.len() != 2 {
                    return Err(DbError::new(
                        "42883",
                        format!(
                            "function pg_notify does not accept {} arguments",
                            arguments.len()
                        ),
                    ));
                }
                let payload = arguments
                    .pop()
                    .ok_or_else(|| DbError::internal("pg_notify payload argument is missing"))?;
                let channel = arguments
                    .pop()
                    .ok_or_else(|| DbError::internal("pg_notify channel argument is missing"))?;
                return Ok(ParsedStatement::PgNotify {
                    channel,
                    payload,
                    alias,
                });
            }
            if scalar_function_from_name(&function_name).is_none() {
                let (name, arguments) = convert_routine_invocation(function.clone(), sql)?;
                if let Some(operation_name) = sequence_operation_name(&name) {
                    return convert_sequence_value_select(operation_name, arguments, alias);
                }
                return Ok(ParsedStatement::RoutineSelect {
                    name,
                    arguments,
                    alias,
                });
            }
        }
    }
    let projection = convert_projection_items(select.projection, sql)?;
    if projection
        .iter()
        .any(|item| matches!(item, ParsedProjection::Wildcard))
    {
        return unsupported("SELECT without FROM does not support wildcards");
    }
    Ok(ParsedStatement::ScalarSelect { projection })
}

fn sequence_operation_name(name: &ParsedObjectName) -> Option<&str> {
    if name.parts.is_empty() || name.parts.len() > 2 {
        return None;
    }
    let operation = name.parts.last()?.name.as_str();
    operation
        .eq_ignore_ascii_case("nextval")
        .then_some("nextval")
        .or_else(|| {
            operation
                .eq_ignore_ascii_case("currval")
                .then_some("currval")
        })
        .or_else(|| operation.eq_ignore_ascii_case("setval").then_some("setval"))
}

fn convert_sequence_value_select(
    operation: &str,
    mut arguments: Vec<ParsedExpr>,
    alias: Option<ParsedIdentifier>,
) -> Result<ParsedStatement> {
    let expected = if operation == "setval" {
        "two or three"
    } else {
        "one"
    };
    let valid_count = if operation == "setval" {
        matches!(arguments.len(), 2 | 3)
    } else {
        arguments.len() == 1
    };
    if !valid_count {
        return Err(DbError::new(
            "42883",
            format!("{operation} requires {expected} positional argument(s)"),
        ));
    }
    let name_argument = arguments.remove(0);
    let name = parsed_sequence_regclass(&name_argument)?;
    let operation = match operation {
        "nextval" => ParsedSequenceOperation::NextValue,
        "currval" => ParsedSequenceOperation::CurrentValue,
        "setval" => {
            let value = arguments.remove(0);
            let is_called = if arguments.is_empty() {
                true
            } else {
                match arguments.remove(0).kind {
                    ParsedExprKind::Literal(Value::Boolean(value)) => value,
                    _ => {
                        return Err(DbError::new(
                            "42804",
                            "setval third argument must be a boolean literal",
                        ));
                    }
                }
            };
            ParsedSequenceOperation::SetValue { value, is_called }
        }
        _ => return Err(DbError::internal("unknown sequence operation")),
    };
    Ok(ParsedStatement::SequenceValue {
        name,
        operation,
        alias,
    })
}

fn parsed_sequence_regclass(argument: &ParsedExpr) -> Result<ParsedObjectName> {
    let ParsedExprKind::Literal(Value::Text(value)) = &argument.kind else {
        return Err(
            DbError::new("42804", "sequence name must be a text regclass literal")
                .with_position_opt(argument.position),
        );
    };
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.is_empty()
        || parts.len() > 2
        || parts
            .iter()
            .any(|part| part.is_empty() || !is_simple_identifier(part))
    {
        return Err(DbError::new(
            "42602",
            "sequence regclass must be an unquoted name or schema.name",
        )
        .with_position_opt(argument.position));
    }
    Ok(ParsedObjectName {
        parts: parts
            .into_iter()
            .map(|part| ParsedIdentifier {
                name: Identifier::unquoted(part),
                position: argument.position,
            })
            .collect(),
    })
}

fn is_simple_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn convert_select_from(table: TableWithJoins, sql: &str) -> Result<(ParsedTable, Vec<ParsedJoin>)> {
    let first = convert_select_table(table.relation, sql)?;
    let joins = table
        .joins
        .into_iter()
        .map(|join| {
            if join.global {
                return unsupported("GLOBAL joins are not supported");
            }
            let (kind, constraint) = match join.join_operator {
                JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                    (JoinKind::Inner, constraint)
                }
                JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                    (JoinKind::Left, constraint)
                }
                _ => return unsupported("only INNER and LEFT joins are supported"),
            };
            let JoinConstraint::On(on) = constraint else {
                return unsupported("joins require an ON predicate");
            };
            Ok(ParsedJoin {
                source: convert_join_source(join.relation, sql)?,
                kind,
                on: convert_expr(on, sql)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((first, joins))
}
