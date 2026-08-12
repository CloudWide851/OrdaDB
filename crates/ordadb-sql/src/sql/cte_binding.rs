
fn bind_with_clause(
    recursive: bool,
    ctes: Vec<ParsedCte>,
    mut body: ParsedStatement,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let mut transient_catalog = catalog.clone();
    let temporary_schema = (0_u64..)
        .map(|suffix| Identifier::unquoted(format!("__ordadb_cte_{view_depth}_{suffix}")))
        .find(|candidate| transient_catalog.schema(candidate).is_none())
        .ok_or_else(|| DbError::new("54000", "could not allocate a transient CTE namespace"))?;
    transient_catalog.create_schema(temporary_schema.clone())?;

    let mut names = BTreeSet::new();
    let mut replacements = BTreeMap::new();
    let mut bound_ctes = Vec::with_capacity(ctes.len());
    for cte in ctes {
        if !names.insert(cte.name.name.clone()) {
            return Err(DbError::new(
                "42712",
                format!("WITH query name {} specified more than once", cte.name.name),
            )
            .with_position_opt(cte.name.position));
        }
        let cte_name = cte.name.clone();
        let cte_columns = cte.columns.clone();
        let query = *cte.query;
        let self_recursive = recursive && parsed_query_references_table(&query, &cte_name.name, 0)?;
        let (mut seed, recursive_term, union_all) = if self_recursive {
            match query {
                ParsedStatement::SetOperation {
                    left,
                    operator: QuerySetOperator::Union,
                    all,
                    right,
                    order_by,
                    offset,
                    limit,
                } if order_by.is_empty() && offset.is_none() && limit.is_none() => {
                    (*left, Some(*right), all)
                }
                _ => {
                    return Err(DbError::new(
                        FEATURE_NOT_SUPPORTED,
                        "recursive CTEs require a top-level UNION or UNION ALL",
                    ));
                }
            }
        } else {
            (query, None, false)
        };
        rewrite_cte_references(&mut seed, &replacements, 0)?;
        if self_recursive && parsed_query_references_table(&seed, &cte_name.name, 0)? {
            return Err(DbError::new(
                "42P19",
                "recursive reference must not appear within the non-recursive term",
            ));
        }
        let seed = bind_with_view_depth(seed, &transient_catalog, view_depth + 1)?;
        let mut output = bound_query_schema(&seed)?;
        apply_cte_column_aliases(&cte_name, &cte_columns, &mut output)?;
        let table_id = create_cte_relation(
            &mut transient_catalog,
            &temporary_schema,
            &cte_name,
            &output,
        )?;
        replacements.insert(
            cte_name.name.clone(),
            cte_replacement_name(&temporary_schema, &cte_name),
        );
        let recursive = recursive_term
            .map(|mut recursive_term| {
                rewrite_cte_references(&mut recursive_term, &replacements, 0)?;
                let recursive_term =
                    bind_with_view_depth(recursive_term, &transient_catalog, view_depth + 1)?;
                let recursive_schema = bound_query_schema(&recursive_term)?;
                ensure_recursive_cte_schema(&output, &recursive_schema)?;
                Ok(Box::new(recursive_term))
            })
            .transpose()?;
        bound_ctes.push(BoundCte {
            table_id,
            seed: Box::new(seed),
            recursive,
            union_all,
        });
    }
    rewrite_cte_references(&mut body, &replacements, 0)?;
    let body = bind_with_view_depth(body, &transient_catalog, view_depth + 1)?;
    let schema = bound_query_schema(&body)?;
    Ok(BoundStatement::With {
        ctes: bound_ctes,
        body: Box::new(body),
        catalog: Box::new(transient_catalog),
        schema,
    })
}

fn apply_cte_column_aliases(
    name: &ParsedIdentifier,
    columns: &[ParsedIdentifier],
    output: &mut Schema,
) -> Result<()> {
    if columns.is_empty() {
        return Ok(());
    }
    if columns.len() != output.fields.len() {
        return Err(DbError::new(
            "42601",
            format!(
                "WITH query {} has {} columns available but {} columns specified",
                name.name,
                output.fields.len(),
                columns.len()
            ),
        ));
    }
    for (field, column) in output.fields.iter_mut().zip(columns) {
        field.name = column.name.as_str().to_owned();
    }
    Ok(())
}

fn create_cte_relation(
    catalog: &mut Catalog,
    schema: &Identifier,
    name: &ParsedIdentifier,
    output: &Schema,
) -> Result<TableId> {
    catalog.create_table(
        schema,
        name.name.clone(),
        output
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
            .collect(),
    )
}

fn cte_replacement_name(schema: &Identifier, name: &ParsedIdentifier) -> ParsedObjectName {
    ParsedObjectName {
        parts: vec![
            ParsedIdentifier {
                name: schema.clone(),
                position: name.position,
            },
            name.clone(),
        ],
    }
}

fn ensure_recursive_cte_schema(seed: &Schema, recursive: &Schema) -> Result<()> {
    if seed.fields.len() != recursive.fields.len() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "recursive UNION queries must have the same number of columns",
        ));
    }
    for (seed, recursive) in seed.fields.iter().zip(&recursive.fields) {
        let Some(common) = common_type(&seed.data_type, &recursive.data_type) else {
            return Err(DbError::new(
                DATATYPE_MISMATCH,
                format!(
                    "recursive UNION types {:?} and {:?} cannot be matched",
                    seed.data_type, recursive.data_type
                ),
            ));
        };
        if common != seed.data_type {
            return Err(DbError::new(
                DATATYPE_MISMATCH,
                "recursive query column type must match the non-recursive term",
            ));
        }
    }
    Ok(())
}

fn parsed_query_references_table(
    statement: &ParsedStatement,
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    if depth >= 64 {
        return Err(DbError::new(
            "54001",
            "recursive CTE analysis exceeds the maximum depth of 64",
        ));
    }
    let references = match statement {
        ParsedStatement::Select {
            table: source,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => {
            cte_table_matches(source, table)
                || parsed_projections_reference_table(projection, table, depth)?
                || parsed_optional_expr_references_table(filter.as_ref(), table, depth)?
                || parsed_orders_reference_table(order_by, table, depth)?
                || parsed_optional_expr_references_table(offset.as_ref(), table, depth)?
                || parsed_optional_expr_references_table(limit.as_ref(), table, depth)?
        }
        ParsedStatement::AdvancedSelect {
            table: source,
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
            cte_table_matches(&source.name, table)
                || joins.iter().try_fold(false, |found, join| {
                    Ok(found
                        || match &join.source {
                            ParsedJoinSource::Table(source) => {
                                cte_table_matches(&source.name, table)
                            }
                            ParsedJoinSource::Derived { query, .. } => {
                                parsed_query_references_table(query, table, depth + 1)?
                            }
                        })
                })?
                || parsed_exprs_reference_table(joins.iter().map(|join| &join.on), table, depth)?
                || parsed_projections_reference_table(projection, table, depth)?
                || parsed_optional_expr_references_table(filter.as_ref(), table, depth)?
                || parsed_exprs_reference_table(group_by.iter(), table, depth)?
                || parsed_optional_expr_references_table(having.as_ref(), table, depth)?
                || parsed_orders_reference_table(order_by, table, depth)?
                || parsed_optional_expr_references_table(offset.as_ref(), table, depth)?
                || parsed_optional_expr_references_table(limit.as_ref(), table, depth)?
        }
        ParsedStatement::SetOperation {
            left,
            right,
            order_by,
            offset,
            limit,
            ..
        } => {
            parsed_query_references_table(left, table, depth + 1)?
                || parsed_query_references_table(right, table, depth + 1)?
                || parsed_orders_reference_table(order_by, table, depth)?
                || parsed_optional_expr_references_table(offset.as_ref(), table, depth)?
                || parsed_optional_expr_references_table(limit.as_ref(), table, depth)?
        }
        ParsedStatement::With { ctes, body, .. } => {
            if ctes.iter().any(|cte| &cte.name.name == table) {
                false
            } else {
                ctes.iter().try_fold(false, |found, cte| {
                    Ok(found || parsed_query_references_table(&cte.query, table, depth + 1)?)
                })? || parsed_query_references_table(body, table, depth + 1)?
            }
        }
        ParsedStatement::Explain { statement } => {
            parsed_query_references_table(statement, table, depth + 1)?
        }
        _ => false,
    };
    Ok(references)
}

fn parsed_projections_reference_table(
    projections: &[ParsedProjection],
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    parsed_exprs_reference_table(
        projections
            .iter()
            .filter_map(|projection| match projection {
                ParsedProjection::Wildcard => None,
                ParsedProjection::Expression { expr, .. } => Some(expr),
            }),
        table,
        depth,
    )
}

fn parsed_orders_reference_table(
    orders: &[ParsedOrder],
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    parsed_exprs_reference_table(orders.iter().map(|order| &order.expr), table, depth)
}

fn parsed_optional_expr_references_table(
    expression: Option<&ParsedExpr>,
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    match expression {
        Some(expression) => parsed_expr_references_table(expression, table, depth),
        None => Ok(false),
    }
}

fn parsed_exprs_reference_table<'a>(
    expressions: impl IntoIterator<Item = &'a ParsedExpr>,
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    for expression in expressions {
        if parsed_expr_references_table(expression, table, depth)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parsed_expr_references_table(
    expression: &ParsedExpr,
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        match &expression.kind {
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                pending.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => pending.extend(elements),
            ParsedExprKind::Function { arguments, .. } => pending.extend(arguments),
            ParsedExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                pending.extend(list);
                pending.push(expr);
            }
            ParsedExprKind::ScalarSubquery(query)
            | ParsedExprKind::Exists {
                subquery: query, ..
            } => {
                if parsed_query_references_table(query, table, depth + 1)? {
                    return Ok(true);
                }
            }
            ParsedExprKind::InSubquery { expr, subquery, .. } => {
                pending.push(expr);
                if parsed_query_references_table(subquery, table, depth + 1)? {
                    return Ok(true);
                }
            }
            ParsedExprKind::QuantifiedSubquery { left, subquery, .. } => {
                pending.push(left);
                if parsed_query_references_table(subquery, table, depth + 1)? {
                    return Ok(true);
                }
            }
            ParsedExprKind::RowSubquery { left, subquery, .. } => {
                pending.extend(left);
                if parsed_query_references_table(subquery, table, depth + 1)? {
                    return Ok(true);
                }
            }
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
            ParsedExprKind::Window { call, spec } => {
                if let Some(filter) = &call.filter {
                    pending.push(filter);
                }
                pending.extend(&call.arguments);
                pending.extend(spec.order_by.iter().map(|order| &order.expr));
                pending.extend(&spec.partition_by);
            }
            ParsedExprKind::NamedWindow { call, .. } => {
                if let Some(filter) = &call.filter {
                    pending.push(filter);
                }
                pending.extend(&call.arguments);
            }
            ParsedExprKind::Column(_)
            | ParsedExprKind::Literal(_)
            | ParsedExprKind::Parameter(_)
            | ParsedExprKind::ResolvedParameter { .. }
            | ParsedExprKind::ApplyValue { .. }
            | ParsedExprKind::WindowValue { .. } => {}
        }
    }
    Ok(false)
}

fn cte_table_matches(name: &ParsedObjectName, table: &Identifier) -> bool {
    matches!(name.parts.as_slice(), [name] if &name.name == table)
}

fn rewrite_cte_references(
    statement: &mut ParsedStatement,
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
    depth: usize,
) -> Result<()> {
    if depth >= 64 {
        return Err(DbError::new(
            "54001",
            "CTE scope nesting exceeds the maximum depth of 64",
        ));
    }
    match statement {
        ParsedStatement::Select {
            table,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => {
            rewrite_cte_table(table, replacements);
            rewrite_cte_projections(projection, replacements, depth)?;
            rewrite_cte_optional_expr(filter.as_mut(), replacements, depth)?;
            rewrite_cte_orders(order_by, replacements, depth)?;
            rewrite_cte_optional_expr(offset.as_mut(), replacements, depth)?;
            rewrite_cte_optional_expr(limit.as_mut(), replacements, depth)?;
        }
        ParsedStatement::AdvancedSelect {
            table,
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
            rewrite_cte_table(&mut table.name, replacements);
            for join in joins {
                match &mut join.source {
                    ParsedJoinSource::Table(table) => {
                        rewrite_cte_table(&mut table.name, replacements);
                    }
                    ParsedJoinSource::Derived { query, .. } => {
                        rewrite_cte_references(query, replacements, depth + 1)?;
                    }
                }
                rewrite_cte_expr(&mut join.on, replacements, depth)?;
            }
            rewrite_cte_projections(projection, replacements, depth)?;
            rewrite_cte_optional_expr(filter.as_mut(), replacements, depth)?;
            for expression in group_by {
                rewrite_cte_expr(expression, replacements, depth)?;
            }
            rewrite_cte_optional_expr(having.as_mut(), replacements, depth)?;
            rewrite_cte_orders(order_by, replacements, depth)?;
            rewrite_cte_optional_expr(offset.as_mut(), replacements, depth)?;
            rewrite_cte_optional_expr(limit.as_mut(), replacements, depth)?;
        }
        ParsedStatement::SetOperation {
            left,
            right,
            order_by,
            offset,
            limit,
            ..
        } => {
            rewrite_cte_references(left, replacements, depth + 1)?;
            rewrite_cte_references(right, replacements, depth + 1)?;
            rewrite_cte_orders(order_by, replacements, depth)?;
            rewrite_cte_optional_expr(offset.as_mut(), replacements, depth)?;
            rewrite_cte_optional_expr(limit.as_mut(), replacements, depth)?;
        }
        ParsedStatement::With { ctes, body, .. } => {
            let mut outer = replacements.clone();
            for cte in ctes.iter() {
                outer.remove(&cte.name.name);
            }
            for cte in ctes {
                rewrite_cte_references(&mut cte.query, &outer, depth + 1)?;
            }
            rewrite_cte_references(body, &outer, depth + 1)?;
        }
        ParsedStatement::Explain { statement } => {
            rewrite_cte_references(statement, replacements, depth + 1)?;
        }
        _ => {}
    }
    Ok(())
}

fn rewrite_cte_projections(
    projections: &mut [ParsedProjection],
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
    depth: usize,
) -> Result<()> {
    for projection in projections {
        if let ParsedProjection::Expression { expr, .. } = projection {
            rewrite_cte_expr(expr, replacements, depth)?;
        }
    }
    Ok(())
}

fn rewrite_cte_orders(
    orders: &mut [ParsedOrder],
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
    depth: usize,
) -> Result<()> {
    for order in orders {
        rewrite_cte_expr(&mut order.expr, replacements, depth)?;
    }
    Ok(())
}

fn rewrite_cte_optional_expr(
    expression: Option<&mut ParsedExpr>,
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
    depth: usize,
) -> Result<()> {
    if let Some(expression) = expression {
        rewrite_cte_expr(expression, replacements, depth)?;
    }
    Ok(())
}

fn rewrite_cte_expr(
    expression: &mut ParsedExpr,
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
    depth: usize,
) -> Result<()> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        match &mut expression.kind {
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                pending.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => pending.extend(elements),
            ParsedExprKind::Function { arguments, .. } => pending.extend(arguments),
            ParsedExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                pending.extend(list);
                pending.push(expr);
            }
            ParsedExprKind::ScalarSubquery(query)
            | ParsedExprKind::Exists {
                subquery: query, ..
            } => rewrite_cte_references(query, replacements, depth + 1)?,
            ParsedExprKind::InSubquery { expr, subquery, .. } => {
                pending.push(expr);
                rewrite_cte_references(subquery, replacements, depth + 1)?;
            }
            ParsedExprKind::QuantifiedSubquery { left, subquery, .. } => {
                pending.push(left);
                rewrite_cte_references(subquery, replacements, depth + 1)?;
            }
            ParsedExprKind::RowSubquery { left, subquery, .. } => {
                pending.extend(left);
                rewrite_cte_references(subquery, replacements, depth + 1)?;
            }
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
            ParsedExprKind::Window { call, spec } => {
                if let Some(filter) = &mut call.filter {
                    pending.push(filter);
                }
                pending.extend(&mut call.arguments);
                pending.extend(spec.order_by.iter_mut().map(|order| &mut order.expr));
                pending.extend(&mut spec.partition_by);
            }
            ParsedExprKind::NamedWindow { call, .. } => {
                if let Some(filter) = &mut call.filter {
                    pending.push(filter);
                }
                pending.extend(&mut call.arguments);
            }
            ParsedExprKind::Column(_)
            | ParsedExprKind::Literal(_)
            | ParsedExprKind::Parameter(_)
            | ParsedExprKind::ResolvedParameter { .. }
            | ParsedExprKind::ApplyValue { .. }
            | ParsedExprKind::WindowValue { .. } => {}
        }
    }
    Ok(())
}

fn rewrite_cte_table(
    table: &mut ParsedObjectName,
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
) {
    if let [name] = table.parts.as_slice()
        && let Some(replacement) = replacements.get(&name.name)
    {
        *table = replacement.clone();
    }
}

#[allow(clippy::too_many_arguments)]
fn bind_set_operation(
    left: ParsedStatement,
    operator: QuerySetOperator,
    all: bool,
    right: ParsedStatement,
    order_by: Vec<ParsedOrder>,
    offset: Option<ParsedExpr>,
    limit: Option<ParsedExpr>,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let left = bind_with_view_depth(left, catalog, view_depth + 1)?;
    let right = bind_with_view_depth(right, catalog, view_depth + 1)?;
    let left_schema = bound_query_schema(&left)?;
    let right_schema = bound_query_schema(&right)?;
    if left_schema.fields.len() != right_schema.fields.len() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "each set-operation query must have the same number of columns",
        ));
    }
    let schema = Schema::new(
        left_schema
            .fields
            .iter()
            .zip(&right_schema.fields)
            .map(|(left, right)| {
                let data_type =
                    common_type(&left.data_type, &right.data_type).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "set-operation types {:?} and {:?} cannot be matched",
                                left.data_type, right.data_type
                            ),
                        )
                    })?;
                Ok(Field::new(
                    left.name.clone(),
                    data_type,
                    left.nullable || right.nullable,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    );
    if !(operator == QuerySetOperator::Union && all)
        && schema
            .fields
            .iter()
            .any(|field| field.data_type == ScalarType::Json)
    {
        return Err(DbError::new(
            "42883",
            "could not identify an equality operator for type json",
        ));
    }
    let order_by = order_by
        .into_iter()
        .map(|order| bind_set_order(order, &schema))
        .collect::<Result<Vec<_>>>()?;
    let offset = offset
        .map(|expr| bind_expr(expr, None, Some(&ScalarType::Int64)))
        .transpose()?;
    let limit = limit
        .map(|expr| bind_expr(expr, None, Some(&ScalarType::Int64)))
        .transpose()?;
    Ok(BoundStatement::SetOperation {
        left: Box::new(left),
        operator,
        all,
        right: Box::new(right),
        schema,
        order_by,
        offset,
        limit,
    })
}

fn bind_set_order(order: ParsedOrder, schema: &Schema) -> Result<BoundOrder> {
    let column_index = match order.expr.kind {
        ParsedExprKind::Literal(Value::Int16(value)) if value > 0 => usize::from(value as u16) - 1,
        ParsedExprKind::Literal(Value::Int32(value)) if value > 0 => usize::try_from(value - 1)
            .map_err(|_| DbError::new("22003", "ORDER BY position is out of range"))?,
        ParsedExprKind::Literal(Value::Int64(value)) if value > 0 => usize::try_from(value - 1)
            .map_err(|_| DbError::new("22003", "ORDER BY position is out of range"))?,
        ParsedExprKind::Column(name) if name.parts.len() == 1 => {
            let column = &name.parts[0];
            let matches = schema
                .fields
                .iter()
                .enumerate()
                .filter(|(_, field)| field.name == column.name.as_str())
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [index] => *index,
                [] => {
                    return Err(DbError::new(
                        UNDEFINED_COLUMN,
                        format!("column {} does not exist", column.name),
                    )
                    .with_position_opt(column.position));
                }
                _ => {
                    return Err(DbError::new(
                        "42702",
                        format!("column reference {} is ambiguous", column.name),
                    )
                    .with_position_opt(column.position));
                }
            }
        }
        _ => {
            return unsupported_at(
                "ORDER BY on a set operation supports output columns or ordinals only",
                order.expr.position,
            );
        }
    };
    if column_index >= schema.fields.len() {
        return Err(DbError::new(
            "42P10",
            format!(
                "ORDER BY position {} is not in select list",
                column_index + 1
            ),
        )
        .with_position_opt(order.expr.position));
    }
    Ok(BoundOrder {
        column_index,
        expression: None,
        data_type: schema.fields[column_index].data_type.clone(),
        ascending: order.ascending,
        nulls_first: order.nulls_first,
    })
}

fn bind_simple_order(
    order: ParsedOrder,
    projection: &[BoundProjection],
    table: &TableDefinition,
) -> Result<BoundOrder> {
    let expression = match projected_order_position(&order.expr, projection)? {
        Some(position) => projection[position].expr.clone(),
        None => bind_expr(order.expr.clone(), Some(table), None)?,
    };
    bound_expression_order(order, expression)
}

fn bind_multi_order(
    order: ParsedOrder,
    projection: &[BoundProjection],
    inputs: &[InputColumn],
) -> Result<BoundOrder> {
    let expression = match projected_order_position(&order.expr, projection)? {
        Some(position) => projection[position].expr.clone(),
        None => bind_expr_multi(order.expr.clone(), inputs, None, false)?,
    };
    bound_expression_order(order, expression)
}

fn bind_distinct_order(
    order: ParsedOrder,
    projection: &[BoundProjection],
    inputs: &[InputColumn],
) -> Result<BoundOrder> {
    let expression = match projected_order_position(&order.expr, projection)? {
        Some(position) => projection[position].expr.clone(),
        None => {
            let expression = bind_expr_multi(order.expr.clone(), inputs, None, false)?;
            if !projection
                .iter()
                .any(|projected| projected.expr == expression)
            {
                return Err(DbError::new(
                    "42P10",
                    "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
                )
                .with_position_opt(order.expr.position));
            }
            expression
        }
    };
    bound_expression_order(order, expression)
}

fn bind_projected_order(
    order: ParsedOrder,
    projection: &[BoundProjection],
    inputs: &[InputColumn],
    group_by: &[BoundExpr],
) -> Result<BoundOrder> {
    let position = if let Some(position) = projected_order_position(&order.expr, projection)? {
        position
    } else {
        let expression = bind_expr_multi(order.expr.clone(), inputs, None, true)?;
        validate_grouped_expr(&expression, group_by)?;
        projection
            .iter()
            .position(|projected| projected.expr == expression)
            .ok_or_else(|| {
                DbError::new(
                    FEATURE_NOT_SUPPORTED,
                    "ORDER BY on grouped queries requires a selected grouped expression",
                )
                .with_position_opt(order.expr.position)
            })?
    };
    Ok(BoundOrder {
        column_index: position,
        expression: None,
        data_type: projection[position].expr.data_type.clone(),
        ascending: order.ascending,
        nulls_first: order.nulls_first,
    })
}
