
fn remap_grouped_window_expr(
    expression: &BoundExpr,
    base_projection: &[&BoundProjection],
    group_by: &[BoundExpr],
) -> Result<BoundExpr> {
    validate_grouped_expr(expression, group_by)?;
    if let Some((index, projected)) = base_projection
        .iter()
        .enumerate()
        .find(|(_, projected)| projected.expr == *expression)
    {
        return Ok(BoundExpr {
            kind: BoundExprKind::Column { index },
            data_type: projected.expr.data_type.clone(),
            nullable: projected.expr.nullable,
        });
    }
    if matches!(
        expression.kind,
        BoundExprKind::Literal(_)
            | BoundExprKind::Parameter { .. }
            | BoundExprKind::Correlation { .. }
    ) {
        return Ok(expression.clone());
    }
    Err(DbError::new(
        FEATURE_NOT_SUPPORTED,
        "grouped window input expression must appear in the select list",
    ))
}

fn validate_grouped_expr(expr: &BoundExpr, group_by: &[BoundExpr]) -> Result<()> {
    if group_by.iter().any(|group| group == expr) {
        return Ok(());
    }
    match &expr.kind {
        BoundExprKind::Aggregate { .. }
        | BoundExprKind::Literal(_)
        | BoundExprKind::Parameter { .. }
        | BoundExprKind::Correlation { .. }
        | BoundExprKind::ApplyValue { .. } => Ok(()),
        BoundExprKind::Column { .. } => Err(DbError::new(
            "42803",
            "column must appear in GROUP BY or be used in an aggregate function",
        )),
        BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => {
            validate_grouped_expr(expr, group_by)
        }
        BoundExprKind::Array { elements, .. } => {
            for element in elements {
                validate_grouped_expr(element, group_by)?;
            }
            Ok(())
        }
        BoundExprKind::Function { arguments, .. } => {
            for argument in arguments {
                validate_grouped_expr(argument, group_by)?;
            }
            Ok(())
        }
        BoundExprKind::Binary { left, right, .. } => {
            validate_grouped_expr(left, group_by)?;
            validate_grouped_expr(right, group_by)
        }
        BoundExprKind::InList { expr, list, .. } => {
            validate_grouped_expr(expr, group_by)?;
            for candidate in list {
                validate_grouped_expr(candidate, group_by)?;
            }
            Ok(())
        }
    }
}

fn bind_select(input: SelectInput, catalog: &Catalog, view_depth: usize) -> Result<BoundStatement> {
    let SelectInput {
        table_name,
        projection,
        filter,
        order_by,
        offset,
        limit,
    } = input;
    let (schema_name, relation_name, _) = split_table_name(&table_name)?;
    if let Some(view) = catalog.view(&schema_name, &relation_name) {
        return bind_view_select(
            view,
            SelectInput {
                table_name,
                projection,
                filter,
                order_by,
                offset,
                limit,
            },
            catalog,
            view_depth,
        );
    }
    let table = resolve_table(&table_name, catalog)?.clone();
    let mut bound_projection = Vec::new();
    for item in projection {
        match item {
            ParsedProjection::Wildcard => {
                for (index, column) in table.columns().iter().enumerate() {
                    bound_projection.push(BoundProjection {
                        expr: BoundExpr {
                            kind: BoundExprKind::Column { index },
                            data_type: column.data_type.clone(),
                            nullable: column.nullable,
                        },
                        field: Field::new(
                            column.name.as_str(),
                            column.data_type.clone(),
                            column.nullable,
                        ),
                    });
                }
            }
            ParsedProjection::Expression { expr, alias } => {
                let default_name = projection_name(&expr);
                let bound = bind_expr(expr, Some(&table), None)?;
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
    let filter = filter
        .map(|expr| bind_boolean_expr(expr, &table))
        .transpose()?;
    let order_by = order_by
        .into_iter()
        .map(|order| bind_simple_order(order, &bound_projection, &table))
        .collect::<Result<Vec<_>>>()?;
    let limit = limit
        .map(|expr| bind_expr(expr, Some(&table), Some(&ScalarType::Int64)))
        .transpose()?;
    let offset = offset
        .map(|expr| bind_expr(expr, Some(&table), Some(&ScalarType::Int64)))
        .transpose()?;
    let schema = Schema::new(
        bound_projection
            .iter()
            .map(|projection| projection.field.clone())
            .collect(),
    );
    Ok(BoundStatement::Select {
        table_id: table.id,
        schema,
        projection: bound_projection,
        filter,
        order_by,
        offset,
        limit,
    })
}

fn bind_view_select(
    view: &ViewDefinition,
    input: SelectInput,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let SelectInput {
        table_name: _,
        projection,
        filter,
        order_by,
        offset,
        limit,
    } = input;
    if filter.is_some() || !order_by.is_empty() || offset.is_some() || limit.is_some() {
        return unsupported(
            "WHERE, ORDER BY, OFFSET, and LIMIT on views are not supported in this milestone",
        );
    }
    let source = bind_view_source(view, catalog, view_depth)?;
    let source_schema = bound_query_schema(&source)?;
    if source_schema.fields.len() != view.output.fields.len() {
        return Err(DbError::new(
            "42P16",
            "stored view query output no longer matches its catalog definition",
        ));
    }

    let mut positions = Vec::new();
    let mut fields = Vec::new();
    for item in projection {
        match item {
            ParsedProjection::Wildcard => {
                positions.extend(0..view.output.fields.len());
                fields.extend(view.output.fields.iter().cloned());
            }
            ParsedProjection::Expression { expr, alias } => {
                let ParsedExprKind::Column(name) = expr.kind else {
                    return unsupported_at(
                        "view projection supports columns and wildcard only",
                        expr.position,
                    );
                };
                let column = name
                    .parts
                    .last()
                    .ok_or_else(|| DbError::new(SYNTAX_ERROR, "view column reference is empty"))?;
                let position = view
                    .output
                    .fields
                    .iter()
                    .position(|field| field.name == column.name.as_str())
                    .ok_or_else(|| {
                        DbError::new(
                            UNDEFINED_COLUMN,
                            format!("column {} does not exist", column.name),
                        )
                        .with_position_opt(column.position)
                    })?;
                let mut field = view.output.fields[position].clone();
                if let Some(alias) = alias {
                    field.name = alias.name.as_str().to_owned();
                }
                positions.push(position);
                fields.push(field);
            }
        }
    }
    if fields.is_empty() {
        return Err(DbError::new(SYNTAX_ERROR, "SELECT projection is empty"));
    }
    Ok(BoundStatement::ViewSelect {
        view_id: view.id,
        source: Box::new(source),
        schema: Schema::new(fields),
        projection: positions,
    })
}

fn bind_view_source(
    view: &ViewDefinition,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let source = match view.kind {
        ViewKind::Regular => {
            bind_with_view_depth(parse(&view.query)?, catalog, view_depth.saturating_add(1))?
        }
        ViewKind::Materialized => {
            if !view.populated {
                return Err(DbError::new(
                    "55000",
                    format!("materialized view {} has not been populated", view.name),
                )
                .with_hint("run REFRESH MATERIALIZED VIEW before querying it"));
            }
            let table_id = view.materialized_table_id.ok_or_else(|| {
                DbError::internal("materialized view is missing its backing table")
            })?;
            let projection = view
                .output
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| BoundProjection {
                    expr: BoundExpr {
                        kind: BoundExprKind::Column { index },
                        data_type: field.data_type.clone(),
                        nullable: field.nullable,
                    },
                    field: field.clone(),
                })
                .collect();
            BoundStatement::Select {
                table_id,
                schema: view.output.clone(),
                projection,
                filter: None,
                order_by: Vec::new(),
                offset: None,
                limit: None,
            }
        }
    };
    let source_schema = bound_query_schema(&source)?;
    if source_schema.fields.len() != view.output.fields.len()
        || source_schema
            .fields
            .iter()
            .zip(&view.output.fields)
            .any(|(source, target)| source.data_type != target.data_type)
    {
        return Err(DbError::new(
            "42P16",
            "stored view query output no longer matches its catalog definition",
        ));
    }
    Ok(source)
}

fn bind_update(
    table_name: ParsedObjectName,
    assignments: Vec<(ParsedIdentifier, ParsedExpr)>,
    filter: Option<ParsedExpr>,
    returning: Vec<ParsedProjection>,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let relation = resolve_dml_relation(&table_name, CatalogTriggerEvent::Update, catalog)?;
    let table = relation.scope;
    let mut seen = BTreeSet::new();
    let assignments = assignments
        .into_iter()
        .map(|(column, expr)| {
            let index = table.column_index(&column.name).ok_or_else(|| {
                DbError::new(
                    UNDEFINED_COLUMN,
                    format!("column {} does not exist", column.name),
                )
                .with_position_opt(column.position)
            })?;
            if !seen.insert(index) {
                return Err(DbError::new(
                    "42701",
                    format!("column {} assigned more than once", column.name),
                )
                .with_position_opt(column.position));
            }
            Ok((
                index,
                bind_expr(expr, Some(&table), Some(&table.columns()[index].data_type))?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let returning = bind_returning(returning, &table)?;
    let filter = filter
        .map(|expr| bind_boolean_expr(expr, &table))
        .transpose()?;
    match relation.target {
        DmlTarget::Table(table_id) => Ok(BoundStatement::Update {
            table_id,
            assignments,
            filter,
            returning,
        }),
        DmlTarget::View(view_id) => {
            let view = catalog
                .view_by_id(view_id)
                .ok_or_else(|| DbError::internal("bound view target disappeared"))?;
            Ok(BoundStatement::ViewUpdate {
                view_id,
                source: Box::new(bind_view_source(view, catalog, view_depth)?),
                assignments,
                filter,
                returning,
            })
        }
    }
}

fn bind_delete(
    table_name: ParsedObjectName,
    filter: Option<ParsedExpr>,
    returning: Vec<ParsedProjection>,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let relation = resolve_dml_relation(&table_name, CatalogTriggerEvent::Delete, catalog)?;
    let table = relation.scope;
    let returning = bind_returning(returning, &table)?;
    let filter = filter
        .map(|expr| bind_boolean_expr(expr, &table))
        .transpose()?;
    match relation.target {
        DmlTarget::Table(table_id) => Ok(BoundStatement::Delete {
            table_id,
            filter,
            returning,
        }),
        DmlTarget::View(view_id) => {
            let view = catalog
                .view_by_id(view_id)
                .ok_or_else(|| DbError::internal("bound view target disappeared"))?;
            Ok(BoundStatement::ViewDelete {
                view_id,
                source: Box::new(bind_view_source(view, catalog, view_depth)?),
                filter,
                returning,
            })
        }
    }
}

fn bind_returning(
    returning: Vec<ParsedProjection>,
    table: &TableDefinition,
) -> Result<Option<BoundReturning>> {
    if returning.is_empty() {
        return Ok(None);
    }
    let mut projection = Vec::new();
    for item in returning {
        match item {
            ParsedProjection::Wildcard => {
                for (index, column) in table.columns().iter().enumerate() {
                    projection.push(BoundProjection {
                        expr: BoundExpr {
                            kind: BoundExprKind::Column { index },
                            data_type: column.data_type.clone(),
                            nullable: column.nullable,
                        },
                        field: Field::new(
                            column.name.as_str(),
                            column.data_type.clone(),
                            column.nullable,
                        ),
                    });
                }
            }
            ParsedProjection::Expression { expr, alias } => {
                let default_name = projection_name(&expr);
                let bound = bind_expr(expr, Some(table), None)?;
                if bound_expr_has_aggregate(&bound) {
                    return Err(DbError::new(
                        "42803",
                        "aggregate functions are not allowed in RETURNING",
                    ));
                }
                projection.push(BoundProjection {
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
    let schema = Schema::new(
        projection
            .iter()
            .map(|projection| projection.field.clone())
            .collect(),
    );
    Ok(Some(BoundReturning { schema, projection }))
}

fn bind_boolean_expr(expr: ParsedExpr, table: &TableDefinition) -> Result<BoundExpr> {
    let position = expr.position;
    let bound = bind_expr(expr, Some(table), Some(&ScalarType::Boolean))?;
    if bound.data_type != ScalarType::Boolean {
        return Err(DbError::new(DATATYPE_MISMATCH, "predicate must be boolean")
            .with_position_opt(position));
    }
    Ok(bound)
}

fn bind_expr(
    expr: ParsedExpr,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
) -> Result<BoundExpr> {
    bind_expr_with_parameter_types(expr, table, expected, &BTreeMap::new())
}

fn bind_expr_with_parameter_types(
    expr: ParsedExpr,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
    parameter_types: &BTreeMap<usize, ScalarType>,
) -> Result<BoundExpr> {
    let position = expr.position;
    match expr.kind {
        ParsedExprKind::Column(column) => {
            let table = table.ok_or_else(|| {
                DbError::new(UNDEFINED_COLUMN, "column reference is not valid here")
                    .with_position_opt(position)
            })?;
            let index = resolve_column(&column, table)?;
            let column = &table.columns()[index];
            if let Some(expected) = expected {
                ensure_types_compatible(&column.data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::Column { index },
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
        }
        ParsedExprKind::Literal(value) => bind_literal(value, expected, position),
        ParsedExprKind::Parameter(index) => {
            let declared = parameter_types.get(&index);
            if let (Some(declared), Some(expected)) = (declared, expected) {
                ensure_types_compatible(declared, expected, position)?;
            }
            let data_type = expected
                .cloned()
                .or_else(|| declared.cloned())
                .ok_or_else(|| {
                    DbError::new(
                        INDETERMINATE_DATATYPE,
                        format!("could not determine data type of parameter ${index}"),
                    )
                    .with_position_opt(position)
                })?;
            Ok(BoundExpr {
                kind: BoundExprKind::Parameter { index },
                data_type,
                nullable: true,
            })
        }
        ParsedExprKind::ResolvedParameter { index, data_type } => {
            let declared = parameter_types.get(&index);
            if let Some(declared) = declared {
                ensure_types_compatible(&data_type, declared, position)?;
            }
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::Parameter { index },
                data_type,
                nullable: true,
            })
        }
        ParsedExprKind::ApplyValue {
            index,
            data_type,
            nullable,
        } => {
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::ApplyValue { index },
                data_type,
                nullable,
            })
        }
        ParsedExprKind::Unary { op, expr } => match op {
            UnaryOperator::Not => {
                let expr = bind_expr_with_parameter_types(
                    *expr,
                    table,
                    Some(&ScalarType::Boolean),
                    parameter_types,
                )?;
                if expr.data_type != ScalarType::Boolean {
                    return Err(
                        DbError::new(DATATYPE_MISMATCH, "NOT operand must be boolean")
                            .with_position_opt(position),
                    );
                }
                Ok(BoundExpr {
                    kind: BoundExprKind::Unary {
                        op,
                        expr: Box::new(expr),
                    },
                    data_type: ScalarType::Boolean,
                    nullable: true,
                })
            }
            UnaryOperator::Negate => {
                let expr = bind_expr_with_parameter_types(*expr, table, expected, parameter_types)?;
                if !is_numeric(&expr.data_type) {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        "unary minus requires a numeric operand",
                    )
                    .with_position_opt(position));
                }
                let data_type = expr.data_type.clone();
                Ok(BoundExpr {
                    kind: BoundExprKind::Unary {
                        op,
                        expr: Box::new(expr),
                    },
                    data_type,
                    nullable: false,
                })
            }
        },
        ParsedExprKind::Cast {
            expr, data_type, ..
        } => {
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            let source_type = infer_expr_type(&expr, table, parameter_types)?;
            let bound = bind_expr_with_parameter_types(
                *expr,
                table,
                source_type.is_none().then_some(&data_type),
                parameter_types,
            )?;
            ensure_explicit_cast_supported(&bound.data_type, &data_type, position)?;
            let nullable = bound.nullable;
            Ok(BoundExpr {
                kind: BoundExprKind::Cast {
                    expr: Box::new(bound),
                },
                data_type,
                nullable,
            })
        }
        ParsedExprKind::Array {
            elements,
            dimensions,
        } => {
            let expected_element = match expected {
                Some(ScalarType::Array { element }) => Some(element.as_ref().clone()),
                Some(expected) => {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        format!("array cannot be assigned to {expected:?}"),
                    )
                    .with_position_opt(position));
                }
                None => None,
            };
            let mut element_type = expected_element;
            for element in &elements {
                let Some(candidate) = infer_expr_type(element, table, parameter_types)? else {
                    continue;
                };
                element_type = Some(match element_type {
                    Some(current) => common_type_with_literal(&current, &candidate, None, element)
                        .ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "array element types {current:?} and {candidate:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(position)
                    })?,
                    None => candidate,
                });
            }
            let element_type = element_type.ok_or_else(|| {
                DbError::new(
                    INDETERMINATE_DATATYPE,
                    "cannot determine type of empty array",
                )
                .with_hint("Explicitly cast the array, for example ARRAY[]::integer[].")
                .with_position_opt(position)
            })?;
            if matches!(element_type, ScalarType::Array { .. }) {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "nested array values must use one flattened PostgreSQL array type",
                )
                .with_position_opt(position));
            }
            let elements = elements
                .into_iter()
                .map(|element| {
                    bind_expr_with_parameter_types(
                        element,
                        table,
                        Some(&element_type),
                        parameter_types,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(BoundExpr {
                kind: BoundExprKind::Array {
                    elements,
                    dimensions,
                },
                data_type: ScalarType::Array {
                    element: Box::new(element_type),
                },
                nullable: false,
            })
        }
        ParsedExprKind::Function {
            function,
            arguments,
        } => bind_scalar_function(
            function,
            arguments,
            table,
            expected,
            parameter_types,
            position,
        ),
        ParsedExprKind::Binary { left, op, right } => bind_binary(
            *left,
            op,
            *right,
            table,
            position,
            expected,
            parameter_types,
        ),
        ParsedExprKind::InList {
            expr,
            list,
            negated,
        } => {
            if expected.is_some_and(|expected| expected != &ScalarType::Boolean) {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "IN predicate produces a boolean result",
                )
                .with_position_opt(position));
            }
            let mut operand_type = infer_expr_type(&expr, table, parameter_types)?;
            for candidate in &list {
                let Some(candidate_type) = infer_expr_type(candidate, table, parameter_types)?
                else {
                    continue;
                };
                operand_type = Some(match operand_type {
                    Some(current) => {
                        common_type_with_literal(&current, &candidate_type, Some(&expr), candidate)
                            .ok_or_else(|| {
                                DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "IN types {current:?} and {candidate_type:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(position)
                            })?
                    }
                    None => candidate_type,
                });
            }
            let operand_type = operand_type.ok_or_else(|| {
                DbError::new(
                    INDETERMINATE_DATATYPE,
                    "could not determine data type of IN operands",
                )
                .with_position_opt(position)
            })?;
            if operand_type == ScalarType::Json {
                return Err(DbError::new(
                    "42883",
                    "could not identify an equality operator for type json",
                )
                .with_position_opt(position));
            }
            let expr =
                bind_expr_with_parameter_types(*expr, table, Some(&operand_type), parameter_types)?;
            let list = list
                .into_iter()
                .map(|candidate| {
                    bind_expr_with_parameter_types(
                        candidate,
                        table,
                        Some(&operand_type),
                        parameter_types,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let nullable = expr.nullable || list.iter().any(|candidate| candidate.nullable);
            Ok(BoundExpr {
                kind: BoundExprKind::InList {
                    expr: Box::new(expr),
                    list,
                    negated,
                },
                data_type: ScalarType::Boolean,
                nullable,
            })
        }
        ParsedExprKind::ScalarSubquery(_) => unsupported_at(
            "scalar subquery Apply execution is not supported yet",
            position,
        ),
        ParsedExprKind::Exists { .. } => {
            unsupported_at("EXISTS Apply execution is not supported yet", position)
        }
        ParsedExprKind::InSubquery { .. } => {
            unsupported_at("IN subquery Apply execution is not supported yet", position)
        }
        ParsedExprKind::QuantifiedSubquery { .. } => unsupported_at(
            "ANY/ALL subquery Apply execution is not supported yet",
            position,
        ),
        ParsedExprKind::RowSubquery { .. } => unsupported_at(
            "row subquery Apply execution is not supported in this context",
            position,
        ),
        ParsedExprKind::Aggregate { .. } => {
            unsupported_at("aggregate is not valid in this statement", position)
        }
        ParsedExprKind::Window { .. }
        | ParsedExprKind::NamedWindow { .. }
        | ParsedExprKind::WindowValue { .. } => Err(DbError::new(
            "42P20",
            "window functions are not allowed in this statement",
        )
        .with_position_opt(position)),
    }
}
