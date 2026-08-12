
fn convert_join_source(source: TableFactor, sql: &str) -> Result<ParsedJoinSource> {
    match source {
        source @ TableFactor::Table { .. } => {
            convert_select_table(source, sql).map(ParsedJoinSource::Table)
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if sample.is_some() {
                return unsupported("TABLESAMPLE on derived tables is not supported yet");
            }
            let alias = alias.ok_or_else(|| {
                DbError::new(
                    SYNTAX_ERROR,
                    "derived tables require an explicit relation alias",
                )
            })?;
            if alias.at.is_some() {
                return unsupported("AT aliases on derived tables are not supported");
            }
            let columns = alias
                .columns
                .into_iter()
                .map(|column| {
                    if column.data_type.is_some() {
                        return unsupported(
                            "typed column aliases on derived tables are not supported",
                        );
                    }
                    Ok(convert_ident(column.name, sql))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ParsedJoinSource::Derived {
                lateral,
                query: Box::new(convert_select_query(*subquery, sql)?),
                alias: convert_ident(alias.name, sql),
                columns,
            })
        }
        _ => unsupported("table functions and this join source are not supported yet"),
    }
}

fn convert_select_table(table: TableFactor, sql: &str) -> Result<ParsedTable> {
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = table
    else {
        return unsupported("derived tables and table functions are not supported yet");
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return unsupported("table modifiers are not supported yet");
    }
    Ok(ParsedTable {
        name: convert_object_name(name, sql)?,
        alias: alias
            .map(|alias| convert_table_alias(alias, sql))
            .transpose()?,
    })
}

fn convert_table_alias(alias: TableAlias, sql: &str) -> Result<ParsedIdentifier> {
    if !alias.columns.is_empty() || alias.at.is_some() {
        return unsupported("column aliases on table bindings are not supported yet");
    }
    Ok(convert_ident(alias.name, sql))
}

fn projection_has_aggregate(projection: &ParsedProjection) -> bool {
    match projection {
        ParsedProjection::Wildcard => false,
        ParsedProjection::Expression { expr, .. } => expr_has_aggregate(expr),
    }
}

fn projection_has_subquery(projection: &ParsedProjection) -> bool {
    match projection {
        ParsedProjection::Wildcard => false,
        ParsedProjection::Expression { expr, .. } => expr_has_subquery(expr),
    }
}

fn projection_has_window(projection: &ParsedProjection) -> bool {
    match projection {
        ParsedProjection::Wildcard => false,
        ParsedProjection::Expression { expr, .. } => expr_has_window(expr),
    }
}

fn expr_has_window(expr: &ParsedExpr) -> bool {
    match &expr.kind {
        ParsedExprKind::Window { .. }
        | ParsedExprKind::NamedWindow { .. }
        | ParsedExprKind::WindowValue { .. } => true,
        ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
            expr_has_window(expr)
        }
        ParsedExprKind::Array { elements, .. } => elements.iter().any(expr_has_window),
        ParsedExprKind::Function { arguments, .. } => arguments.iter().any(expr_has_window),
        ParsedExprKind::Binary { left, right, .. } => {
            expr_has_window(left) || expr_has_window(right)
        }
        ParsedExprKind::InList { expr, list, .. } => {
            expr_has_window(expr) || list.iter().any(expr_has_window)
        }
        ParsedExprKind::InSubquery { expr, .. } => expr_has_window(expr),
        ParsedExprKind::QuantifiedSubquery { left, .. } => expr_has_window(left),
        ParsedExprKind::RowSubquery { left, .. } => left.iter().any(expr_has_window),
        ParsedExprKind::Aggregate {
            argument, filter, ..
        } => {
            argument.as_deref().is_some_and(expr_has_window)
                || filter.as_deref().is_some_and(expr_has_window)
        }
        ParsedExprKind::ScalarSubquery(_)
        | ParsedExprKind::Exists { .. }
        | ParsedExprKind::Column(_)
        | ParsedExprKind::Literal(_)
        | ParsedExprKind::Parameter(_)
        | ParsedExprKind::ResolvedParameter { .. }
        | ParsedExprKind::ApplyValue { .. } => false,
    }
}

fn expr_has_subquery(expr: &ParsedExpr) -> bool {
    match &expr.kind {
        ParsedExprKind::ScalarSubquery(_)
        | ParsedExprKind::Exists { .. }
        | ParsedExprKind::InSubquery { .. }
        | ParsedExprKind::QuantifiedSubquery { .. }
        | ParsedExprKind::RowSubquery { .. } => true,
        ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
            expr_has_subquery(expr)
        }
        ParsedExprKind::Array { elements, .. } => elements.iter().any(expr_has_subquery),
        ParsedExprKind::Function { arguments, .. } => arguments.iter().any(expr_has_subquery),
        ParsedExprKind::Binary { left, right, .. } => {
            expr_has_subquery(left) || expr_has_subquery(right)
        }
        ParsedExprKind::InList { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        ParsedExprKind::Aggregate {
            argument, filter, ..
        } => {
            argument.as_deref().is_some_and(expr_has_subquery)
                || filter.as_deref().is_some_and(expr_has_subquery)
        }
        ParsedExprKind::Window { call, spec } => {
            call.arguments.iter().any(expr_has_subquery)
                || call.filter.as_deref().is_some_and(expr_has_subquery)
                || spec.partition_by.iter().any(expr_has_subquery)
                || spec
                    .order_by
                    .iter()
                    .any(|order| expr_has_subquery(&order.expr))
        }
        ParsedExprKind::NamedWindow { call, .. } => {
            call.arguments.iter().any(expr_has_subquery)
                || call.filter.as_deref().is_some_and(expr_has_subquery)
        }
        ParsedExprKind::Column(_)
        | ParsedExprKind::Literal(_)
        | ParsedExprKind::Parameter(_)
        | ParsedExprKind::ResolvedParameter { .. }
        | ParsedExprKind::ApplyValue { .. }
        | ParsedExprKind::WindowValue { .. } => false,
    }
}

fn expr_has_aggregate(expr: &ParsedExpr) -> bool {
    match &expr.kind {
        ParsedExprKind::Aggregate { .. } => true,
        ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
            expr_has_aggregate(expr)
        }
        ParsedExprKind::Array { elements, .. } => elements.iter().any(expr_has_aggregate),
        ParsedExprKind::Function { arguments, .. } => arguments.iter().any(expr_has_aggregate),
        ParsedExprKind::Binary { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        ParsedExprKind::InList { expr, list, .. } => {
            expr_has_aggregate(expr) || list.iter().any(expr_has_aggregate)
        }
        ParsedExprKind::InSubquery { expr, .. } => expr_has_aggregate(expr),
        ParsedExprKind::QuantifiedSubquery { left, .. } => expr_has_aggregate(left),
        ParsedExprKind::RowSubquery { left, .. } => left.iter().any(expr_has_aggregate),
        ParsedExprKind::ScalarSubquery(_) | ParsedExprKind::Exists { .. } => false,
        ParsedExprKind::Column(_)
        | ParsedExprKind::Literal(_)
        | ParsedExprKind::Parameter(_)
        | ParsedExprKind::ResolvedParameter { .. }
        | ParsedExprKind::ApplyValue { .. }
        | ParsedExprKind::WindowValue { .. } => false,
        ParsedExprKind::NamedWindow { call, .. } => {
            call.arguments.iter().any(expr_has_aggregate)
                || call.filter.as_deref().is_some_and(expr_has_aggregate)
        }
        ParsedExprKind::Window { call, spec } => {
            call.arguments.iter().any(expr_has_aggregate)
                || call.filter.as_deref().is_some_and(expr_has_aggregate)
                || spec.partition_by.iter().any(expr_has_aggregate)
                || spec
                    .order_by
                    .iter()
                    .any(|order| expr_has_aggregate(&order.expr))
        }
    }
}

fn bind_routine_candidate(
    arguments: &[ParsedExpr],
    expected: &[&RoutineArgument],
    catalog: &Catalog,
) -> Result<Option<(Vec<BoundExpr>, usize)>> {
    let mut bound = Vec::with_capacity(arguments.len());
    let mut exact_declared_matches = 0_usize;
    for (argument, expected) in arguments.iter().zip(expected) {
        let declared_type = match &argument.kind {
            ParsedExprKind::Cast {
                declared_type: Some(name),
                ..
            } => Some(resolve_user_defined_type(name, catalog)?.id),
            _ => None,
        };
        if declared_type.is_some() && declared_type == expected.declared_type {
            exact_declared_matches = exact_declared_matches.saturating_add(1);
        }
        let Ok(argument) = bind_expr(argument.clone(), None, Some(&expected.data_type)) else {
            return Ok(None);
        };
        bound.push(argument);
    }
    Ok(Some((bound, exact_declared_matches)))
}

fn routine_output_schema(routine: &ordadb_catalog::RoutineDefinition) -> Schema {
    Schema::new(
        routine
            .output_arguments()
            .enumerate()
            .map(|(index, argument)| {
                let name = argument.name.as_ref().map_or_else(
                    || format!("column{}", index + 1),
                    |name| name.as_str().to_owned(),
                );
                Field::new(name, argument.data_type.clone(), true)
            })
            .collect(),
    )
}

fn retain_best_routine_matches<T>(matches: &mut Vec<T>, score: impl Fn(&T) -> usize) {
    let Some(best) = matches.iter().map(&score).max() else {
        return;
    };
    matches.retain(|candidate| score(candidate) == best);
}

fn convert_values_query(query: Query, sql: &str) -> Result<Vec<Vec<ParsedExpr>>> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return unsupported("INSERT ... VALUES cannot contain query clauses");
    }
    let SetExpr::Values(values) = *query.body else {
        return unsupported("INSERT ... SELECT is not supported yet");
    };
    if values.explicit_row || values.value_keyword {
        return unsupported("dialect-specific VALUES forms are not supported");
    }
    values
        .rows
        .into_iter()
        .map(|row| {
            row.content
                .into_iter()
                .map(|expr| convert_expr(expr, sql))
                .collect()
        })
        .collect()
}

fn convert_table_with_joins(table: TableWithJoins, sql: &str) -> Result<ParsedObjectName> {
    if !table.joins.is_empty() {
        return unsupported("joins are not supported yet");
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = table.relation
    else {
        return unsupported("derived tables and table functions are not supported yet");
    };
    if alias.is_some()
        || args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return unsupported("table aliases and table modifiers are not supported yet");
    }
    convert_object_name(name, sql)
}

fn convert_expr(expr: SqlExpr, sql: &str) -> Result<ParsedExpr> {
    let position = span_position(sql, expr.span());
    let kind = match expr {
        SqlExpr::Identifier(ident) => {
            if ident.quote_style.is_none()
                && let Some(index) = named_at_parameter_index(&ident.value)
            {
                ParsedExprKind::Parameter(index)
            } else {
                ParsedExprKind::Column(ParsedObjectName {
                    parts: vec![convert_ident(ident, sql)],
                })
            }
        }
        SqlExpr::CompoundIdentifier(parts) => ParsedExprKind::Column(ParsedObjectName {
            parts: parts
                .into_iter()
                .map(|ident| convert_ident(ident, sql))
                .collect(),
        }),
        SqlExpr::Nested(expr) => return convert_expr(*expr, sql),
        SqlExpr::Value(value) => convert_sql_value(value.value, position)?,
        SqlExpr::TypedString(typed) => {
            let value = typed.value.into_string().ok_or_else(|| {
                DbError::new(SYNTAX_ERROR, "typed literal requires a string value")
                    .with_position_opt(position)
            })?;
            ParsedExprKind::Literal(parse_temporal_literal(typed.data_type, &value, position)?)
        }
        SqlExpr::Interval(interval) => {
            if interval.leading_field.is_some()
                || interval.leading_precision.is_some()
                || interval.last_field.is_some()
                || interval.fractional_seconds_precision.is_some()
            {
                return unsupported_at(
                    "INTERVAL field and precision qualifiers are not supported yet",
                    position,
                );
            }
            let value = interval_literal_text(*interval.value, position)?;
            ParsedExprKind::Literal(Value::Interval(
                PgInterval::from_str(&value).map_err(|error| error.with_position_opt(position))?,
            ))
        }
        SqlExpr::Cast {
            kind,
            expr,
            data_type,
            array,
            format,
        } => {
            if !matches!(kind, CastKind::Cast | CastKind::DoubleColon) {
                return unsupported_at("TRY_CAST and SAFE_CAST are not supported", position);
            }
            if array || format.is_some() {
                return unsupported_at("this CAST option is not supported", position);
            }
            let (data_type, declared_type) = convert_column_data_type(data_type, sql)?;
            ParsedExprKind::Cast {
                expr: Box::new(convert_expr(*expr, sql)?),
                data_type,
                declared_type,
            }
        }
        SqlExpr::Array(array) => convert_array_expression(array, sql, position)?,
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            special: _,
            shorthand: _,
        } => {
            let from = substring_from.ok_or_else(|| {
                DbError::new(SYNTAX_ERROR, "SUBSTRING requires a start position")
                    .with_position_opt(position)
            })?;
            let mut arguments = vec![convert_expr(*expr, sql)?, convert_expr(*from, sql)?];
            if let Some(length) = substring_for {
                arguments.push(convert_expr(*length, sql)?);
            }
            ParsedExprKind::Function {
                function: ScalarFunction::Substring,
                arguments,
            }
        }
        SqlExpr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_characters.is_some() {
                return unsupported_at(
                    "comma-separated TRIM characters are not supported",
                    position,
                );
            }
            let function = match trim_where.unwrap_or(TrimWhereField::Both) {
                TrimWhereField::Both => ScalarFunction::Btrim,
                TrimWhereField::Leading => ScalarFunction::Ltrim,
                TrimWhereField::Trailing => ScalarFunction::Rtrim,
            };
            let mut arguments = vec![convert_expr(*expr, sql)?];
            if let Some(trim_what) = trim_what {
                arguments.push(convert_expr(*trim_what, sql)?);
            }
            ParsedExprKind::Function {
                function,
                arguments,
            }
        }
        SqlExpr::Position { expr, r#in } => ParsedExprKind::Function {
            function: ScalarFunction::Strpos,
            arguments: vec![convert_expr(*r#in, sql)?, convert_expr(*expr, sql)?],
        },
        SqlExpr::UnaryOp { op, expr } => {
            let op = match op {
                SqlUnaryOperator::Not => UnaryOperator::Not,
                SqlUnaryOperator::Minus => UnaryOperator::Negate,
                SqlUnaryOperator::Plus => return convert_expr(*expr, sql),
                _ => return unsupported_at("this unary operator is not supported yet", position),
            };
            ParsedExprKind::Unary {
                op,
                expr: Box::new(convert_expr(*expr, sql)?),
            }
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let op = convert_binary_operator(op, position)?;
            match (*left, *right) {
                (SqlExpr::Tuple(left), SqlExpr::Tuple(right)) => {
                    return convert_row_comparison(left, op, right, sql, position);
                }
                (SqlExpr::Tuple(left), SqlExpr::Subquery(subquery)) => {
                    ParsedExprKind::RowSubquery {
                        left: convert_row_items(left, sql, position)?,
                        op: row_comparison_operator(op, position)?,
                        quantifier: None,
                        negated: false,
                        subquery: Box::new(convert_select_query(*subquery, sql)?),
                    }
                }
                (SqlExpr::Subquery(subquery), SqlExpr::Tuple(right)) => {
                    ParsedExprKind::RowSubquery {
                        left: convert_row_items(right, sql, position)?,
                        op: row_comparison_operator(op, position)?,
                        quantifier: None,
                        negated: false,
                        subquery: Box::new(convert_select_query(*subquery, sql)?),
                    }
                }
                (left, right) => ParsedExprKind::Binary {
                    left: Box::new(convert_expr(left, sql)?),
                    op,
                    right: Box::new(convert_expr(right, sql)?),
                },
            }
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            if list.is_empty() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "IN list must contain at least one expression",
                )
                .with_position_opt(position));
            }
            match *expr {
                SqlExpr::Tuple(left) => {
                    return convert_row_in_list(left, list, negated, sql, position);
                }
                expr => ParsedExprKind::InList {
                    expr: Box::new(convert_expr(expr, sql)?),
                    list: list
                        .into_iter()
                        .map(|expr| convert_expr(expr, sql))
                        .collect::<Result<Vec<_>>>()?,
                    negated,
                },
            }
        }
        SqlExpr::Subquery(subquery) => {
            ParsedExprKind::ScalarSubquery(Box::new(convert_select_query(*subquery, sql)?))
        }
        SqlExpr::Exists { subquery, negated } => ParsedExprKind::Exists {
            subquery: Box::new(convert_select_query(*subquery, sql)?),
            negated,
        },
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => match *expr {
            SqlExpr::Tuple(left) => ParsedExprKind::RowSubquery {
                left: convert_row_items(left, sql, position)?,
                op: BinaryOperator::Eq,
                quantifier: Some(SubqueryQuantifier::Any),
                negated,
                subquery: Box::new(convert_select_query(*subquery, sql)?),
            },
            expr => ParsedExprKind::InSubquery {
                expr: Box::new(convert_expr(expr, sql)?),
                subquery: Box::new(convert_select_query(*subquery, sql)?),
                negated,
            },
        },
        SqlExpr::AnyOp {
            left,
            compare_op,
            right,
            is_some: _,
        } => {
            let SqlExpr::Subquery(subquery) = *right else {
                return unsupported_at(
                    "ANY over arrays or non-subquery expressions is not supported yet",
                    position,
                );
            };
            let op = convert_comparison_operator(compare_op, position)?;
            match *left {
                SqlExpr::Tuple(left) => ParsedExprKind::RowSubquery {
                    left: convert_row_items(left, sql, position)?,
                    op: row_comparison_operator(op, position)?,
                    quantifier: Some(SubqueryQuantifier::Any),
                    negated: false,
                    subquery: Box::new(convert_select_query(*subquery, sql)?),
                },
                left => ParsedExprKind::QuantifiedSubquery {
                    left: Box::new(convert_expr(left, sql)?),
                    op,
                    quantifier: SubqueryQuantifier::Any,
                    subquery: Box::new(convert_select_query(*subquery, sql)?),
                },
            }
        }
        SqlExpr::AllOp {
            left,
            compare_op,
            right,
        } => {
            let SqlExpr::Subquery(subquery) = *right else {
                return unsupported_at(
                    "ALL over arrays or non-subquery expressions is not supported yet",
                    position,
                );
            };
            let op = convert_comparison_operator(compare_op, position)?;
            match *left {
                SqlExpr::Tuple(left) => ParsedExprKind::RowSubquery {
                    left: convert_row_items(left, sql, position)?,
                    op: row_comparison_operator(op, position)?,
                    quantifier: Some(SubqueryQuantifier::All),
                    negated: false,
                    subquery: Box::new(convert_select_query(*subquery, sql)?),
                },
                left => ParsedExprKind::QuantifiedSubquery {
                    left: Box::new(convert_expr(left, sql)?),
                    op,
                    quantifier: SubqueryQuantifier::All,
                    subquery: Box::new(convert_select_query(*subquery, sql)?),
                },
            }
        }
        SqlExpr::Tuple(_) => {
            return unsupported_at(
                "row values are supported only in comparisons and IN predicates",
                position,
            );
        }
        SqlExpr::Function(function) => {
            if function.over.is_some() {
                convert_window_function(function, sql, position)?
            } else {
                if function.uses_odbc_syntax
                    || !matches!(function.parameters, FunctionArguments::None)
                    || function.null_treatment.is_some()
                    || !function.within_group.is_empty()
                {
                    return unsupported_at("aggregate options are not supported yet", position);
                }
                let filter = function
                    .filter
                    .map(|filter| convert_expr(*filter, sql).map(Box::new))
                    .transpose()?;
                let function_name = function.name.to_string().to_ascii_lowercase();
                if let Some(scalar_function) = scalar_function_from_name(&function_name) {
                    if filter.is_some() {
                        return unsupported_at(
                            "FILTER is supported only for aggregate functions",
                            position,
                        );
                    }
                    let arguments =
                        convert_scalar_function_arguments(function.args, sql, position)?;
                    validate_scalar_function_arity(scalar_function, arguments.len(), position)?;
                    ParsedExprKind::Function {
                        function: scalar_function,
                        arguments,
                    }
                } else {
                    let aggregate_function = match function_name.as_str() {
                        "count" => AggregateFunction::Count,
                        "sum" => AggregateFunction::Sum,
                        "avg" => AggregateFunction::Avg,
                        "min" => AggregateFunction::Min,
                        "max" => AggregateFunction::Max,
                        _ => {
                            return unsupported_at(
                                "this SQL function is not supported yet",
                                position,
                            );
                        }
                    };
                    let FunctionArguments::List(arguments) = function.args else {
                        return unsupported_at(
                            "aggregate arguments must use parentheses",
                            position,
                        );
                    };
                    if !arguments.clauses.is_empty() {
                        return unsupported_at(
                            "ordered aggregate arguments are not supported yet",
                            position,
                        );
                    }
                    let distinct = matches!(
                        arguments.duplicate_treatment,
                        Some(DuplicateTreatment::Distinct)
                    );
                    let argument = match arguments.args.as_slice() {
                        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                            if aggregate_function == AggregateFunction::Count && !distinct =>
                        {
                            None
                        }
                        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] => {
                            return Err(DbError::new(
                                SYNTAX_ERROR,
                                "DISTINCT aggregate requires an expression",
                            )
                            .with_position_opt(position));
                        }
                        [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] => {
                            Some(Box::new(convert_expr(argument.clone(), sql)?))
                        }
                        _ => {
                            return unsupported_at(
                                "aggregate requires one expression, or COUNT(*)",
                                position,
                            );
                        }
                    };
                    ParsedExprKind::Aggregate {
                        function: aggregate_function,
                        argument,
                        distinct,
                        filter,
                    }
                }
            }
        }
        _ => return unsupported_at("this SQL expression is not supported yet", position),
    };
    Ok(ParsedExpr { kind, position })
}

fn scalar_function_from_name(name: &str) -> Option<ScalarFunction> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    match name {
        "version" => Some(ScalarFunction::Version),
        "current_database" | "current_catalog" => Some(ScalarFunction::CurrentDatabase),
        "current_user" | "user" => Some(ScalarFunction::CurrentUser),
        "session_user" => Some(ScalarFunction::SessionUser),
        "current_setting" => Some(ScalarFunction::CurrentSetting),
        "lower" => Some(ScalarFunction::Lower),
        "upper" => Some(ScalarFunction::Upper),
        "length" | "char_length" | "character_length" => Some(ScalarFunction::CharacterLength),
        "octet_length" => Some(ScalarFunction::OctetLength),
        "abs" => Some(ScalarFunction::Abs),
        "coalesce" => Some(ScalarFunction::Coalesce),
        "nullif" => Some(ScalarFunction::NullIf),
        "concat" => Some(ScalarFunction::Concat),
        "substring" | "substr" => Some(ScalarFunction::Substring),
        "btrim" | "trim" => Some(ScalarFunction::Btrim),
        "ltrim" => Some(ScalarFunction::Ltrim),
        "rtrim" => Some(ScalarFunction::Rtrim),
        "replace" => Some(ScalarFunction::Replace),
        "strpos" => Some(ScalarFunction::Strpos),
        "greatest" => Some(ScalarFunction::Greatest),
        "least" => Some(ScalarFunction::Least),
        "jsonb_typeof" => Some(ScalarFunction::JsonbTypeof),
        "array_length" => Some(ScalarFunction::ArrayLength),
        "cardinality" => Some(ScalarFunction::Cardinality),
        _ => None,
    }
}
