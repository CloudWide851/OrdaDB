
fn resolve_dml_relation(
    name: &ParsedObjectName,
    event: CatalogTriggerEvent,
    catalog: &Catalog,
) -> Result<DmlRelation> {
    let (schema_name, relation_name, position) = split_table_name(name)?;
    let schema = catalog.schema(&schema_name).ok_or_else(|| {
        DbError::new(
            UNDEFINED_SCHEMA,
            format!("schema {schema_name} does not exist"),
        )
        .with_position_opt(position)
    })?;
    if let Some(table) = schema.table(&relation_name) {
        return Ok(DmlRelation {
            target: DmlTarget::Table(table.id),
            scope: table.clone(),
        });
    }
    let view = schema.view(&relation_name).ok_or_else(|| {
        DbError::new(
            UNDEFINED_TABLE,
            format!("relation {schema_name}.{relation_name} does not exist"),
        )
        .with_position_opt(position)
    })?;
    if view.kind != ViewKind::Regular {
        return Err(
            DbError::new("42809", "cannot modify a materialized view").with_position_opt(position)
        );
    }
    let has_instead_of_trigger = view.triggers().any(|trigger| {
        trigger.enabled
            && trigger.timing == TriggerTiming::InsteadOf
            && trigger.level == TriggerLevel::Row
            && trigger.events.contains(&event)
    });
    if !has_instead_of_trigger {
        return Err(DbError::new(
            "55000",
            format!("cannot modify view {schema_name}.{relation_name}"),
        )
        .with_detail(format!(
            "no enabled INSTEAD OF ROW trigger handles {event:?}"
        ))
        .with_hint("Create a matching INSTEAD OF trigger on the view.")
        .with_position_opt(position));
    }
    Ok(DmlRelation {
        target: DmlTarget::View(view.id),
        scope: TableDefinition::expression_scope_for_schema(view.name.clone(), &view.output)?,
    })
}

fn bind_insert(
    table_name: ParsedObjectName,
    columns: Vec<ParsedIdentifier>,
    rows: Vec<Vec<ParsedExpr>>,
    on_conflict: Option<ParsedOnConflict>,
    returning: Vec<ParsedProjection>,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let relation = resolve_dml_relation(&table_name, CatalogTriggerEvent::Insert, catalog)?;
    let table = relation.scope;
    let column_indexes = if columns.is_empty() {
        (0..table.columns().len()).collect::<Vec<_>>()
    } else {
        let mut seen = BTreeSet::new();
        columns
            .into_iter()
            .map(|column| {
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
                        format!("column {} specified more than once", column.name),
                    )
                    .with_position_opt(column.position));
                }
                Ok(index)
            })
            .collect::<Result<Vec<_>>>()?
    };
    if rows.is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "INSERT requires at least one row",
        ));
    }
    let rows = rows
        .into_iter()
        .map(|row| {
            if row.len() != column_indexes.len() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "INSERT has more target columns than expressions",
                ));
            }
            row.into_iter()
                .zip(&column_indexes)
                .map(|(expr, index)| {
                    bind_expr(expr, None, Some(&table.columns()[*index].data_type))
                })
                .collect()
        })
        .collect::<Result<Vec<_>>>()?;
    if matches!(relation.target, DmlTarget::View(_)) && on_conflict.is_some() {
        return unsupported("ON CONFLICT is not supported for view DML");
    }
    let on_conflict = on_conflict
        .map(|on_conflict| bind_on_conflict(on_conflict, &table))
        .transpose()?;
    let returning = bind_returning(returning, &table)?;
    match relation.target {
        DmlTarget::Table(table_id) => Ok(BoundStatement::Insert {
            table_id,
            column_indexes,
            rows,
            on_conflict,
            returning,
        }),
        DmlTarget::View(view_id) => {
            let view = catalog
                .view_by_id(view_id)
                .ok_or_else(|| DbError::internal("bound view target disappeared"))?;
            Ok(BoundStatement::ViewInsert {
                view_id,
                source: Box::new(bind_view_source(view, catalog, view_depth)?),
                column_indexes,
                rows,
                returning,
            })
        }
    }
}

fn bind_merge(merge: ParsedMerge, catalog: &Catalog) -> Result<BoundStatement> {
    let ParsedMerge {
        target,
        source,
        on,
        clauses,
        returning,
    } = merge;
    let target_definition = resolve_table(&target.name, catalog)?.clone();
    let mut inputs = Vec::new();
    let target = bind_input_table(target, false, catalog, &mut inputs)?;
    let source = bind_input_table(source, false, catalog, &mut inputs)?;
    let on = bind_merge_boolean(on, &inputs)?;
    let clauses = clauses
        .into_iter()
        .map(|clause| {
            let kind = match clause.kind {
                ParsedMergeClauseKind::Matched => BoundMergeClauseKind::Matched,
                ParsedMergeClauseKind::NotMatchedByTarget => {
                    BoundMergeClauseKind::NotMatchedByTarget
                }
                ParsedMergeClauseKind::NotMatchedBySource => {
                    BoundMergeClauseKind::NotMatchedBySource
                }
            };
            let predicate = clause
                .predicate
                .map(|predicate| bind_merge_boolean(predicate, &inputs))
                .transpose()?;
            if kind == BoundMergeClauseKind::NotMatchedBySource
                && predicate.as_ref().is_some_and(|predicate| {
                    bound_expr_references_column_at_or_after(predicate, source.offset)
                })
            {
                return Err(invalid_merge_source_reference());
            }
            let action = match clause.action {
                ParsedMergeAction::Update { assignments } => {
                    if kind == BoundMergeClauseKind::NotMatchedByTarget {
                        return Err(DbError::new(
                            SYNTAX_ERROR,
                            "MERGE UPDATE requires WHEN MATCHED or WHEN NOT MATCHED BY SOURCE",
                        ));
                    }
                    let mut seen = BTreeSet::new();
                    let assignments = assignments
                        .into_iter()
                        .map(|(column, expr)| {
                            let index =
                                target_definition
                                    .column_index(&column.name)
                                    .ok_or_else(|| {
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
                                bind_expr_multi(
                                    expr,
                                    &inputs,
                                    Some(&target_definition.columns()[index].data_type),
                                    false,
                                )?,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if kind == BoundMergeClauseKind::NotMatchedBySource
                        && assignments.iter().any(|(_, expression)| {
                            bound_expr_references_column_at_or_after(expression, source.offset)
                        })
                    {
                        return Err(invalid_merge_source_reference());
                    }
                    BoundMergeAction::Update { assignments }
                }
                ParsedMergeAction::Delete => {
                    if kind == BoundMergeClauseKind::NotMatchedByTarget {
                        return Err(DbError::new(
                            SYNTAX_ERROR,
                            "MERGE DELETE requires WHEN MATCHED or WHEN NOT MATCHED BY SOURCE",
                        ));
                    }
                    BoundMergeAction::Delete
                }
                ParsedMergeAction::Insert { columns, values } => {
                    if kind != BoundMergeClauseKind::NotMatchedByTarget {
                        return Err(DbError::new(
                            SYNTAX_ERROR,
                            "MERGE INSERT requires WHEN NOT MATCHED",
                        ));
                    }
                    let column_indexes = if columns.is_empty() {
                        (0..target_definition.columns().len()).collect::<Vec<_>>()
                    } else {
                        let mut seen = BTreeSet::new();
                        columns
                            .into_iter()
                            .map(|column| {
                                let index = target_definition
                                    .column_index(&column.name)
                                    .ok_or_else(|| {
                                        DbError::new(
                                            UNDEFINED_COLUMN,
                                            format!("column {} does not exist", column.name),
                                        )
                                        .with_position_opt(column.position)
                                    })?;
                                if !seen.insert(index) {
                                    return Err(DbError::new(
                                        "42701",
                                        format!("column {} specified more than once", column.name),
                                    )
                                    .with_position_opt(column.position));
                                }
                                Ok(index)
                            })
                            .collect::<Result<Vec<_>>>()?
                    };
                    if values.len() != column_indexes.len() {
                        return Err(DbError::new(
                            SYNTAX_ERROR,
                            "MERGE INSERT has more target columns than expressions",
                        ));
                    }
                    let values = values
                        .into_iter()
                        .zip(&column_indexes)
                        .map(|(expr, index)| {
                            bind_expr_multi(
                                expr,
                                &inputs,
                                Some(&target_definition.columns()[*index].data_type),
                                false,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    BoundMergeAction::Insert {
                        column_indexes,
                        values,
                    }
                }
                ParsedMergeAction::DoNothing => BoundMergeAction::DoNothing,
            };
            Ok(BoundMergeClause {
                kind,
                predicate,
                action,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundStatement::Merge(BoundMerge {
        target,
        source,
        on,
        clauses,
        returning: bind_returning(returning, &target_definition)?,
    }))
}

fn bound_expr_references_column_at_or_after(expr: &BoundExpr, first_index: usize) -> bool {
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match &expr.kind {
            BoundExprKind::Column { index } if *index >= first_index => return true,
            BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => pending.push(expr),
            BoundExprKind::Array { elements, .. } => pending.extend(elements),
            BoundExprKind::Function { arguments, .. } => pending.extend(arguments),
            BoundExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundExprKind::InList { expr, list, .. } => {
                pending.extend(list);
                pending.push(expr);
            }
            BoundExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(argument) = argument {
                    pending.push(argument);
                }
                if let Some(filter) = filter {
                    pending.push(filter);
                }
            }
            BoundExprKind::Column { .. }
            | BoundExprKind::Literal(_)
            | BoundExprKind::Parameter { .. }
            | BoundExprKind::Correlation { .. }
            | BoundExprKind::ApplyValue { .. } => {}
        }
    }
    false
}

fn invalid_merge_source_reference() -> DbError {
    DbError::new(
        UNDEFINED_TABLE,
        "MERGE source columns are not available in WHEN NOT MATCHED BY SOURCE",
    )
    .with_hint("Reference only target columns in this MERGE branch.")
}

fn bind_merge_boolean(expr: ParsedExpr, inputs: &[InputColumn]) -> Result<BoundExpr> {
    let position = expr.position;
    let bound = bind_expr_multi(expr, inputs, Some(&ScalarType::Boolean), false)?;
    if bound.data_type != ScalarType::Boolean {
        return Err(DbError::new(DATATYPE_MISMATCH, "predicate must be boolean")
            .with_position_opt(position));
    }
    Ok(bound)
}

fn bind_on_conflict(
    on_conflict: ParsedOnConflict,
    table: &TableDefinition,
) -> Result<BoundOnConflict> {
    let target_columns = on_conflict
        .target
        .map(|target| bind_conflict_target(target, table))
        .transpose()?;
    let action =
        match on_conflict.action {
            ParsedConflictAction::DoNothing => BoundConflictAction::DoNothing,
            ParsedConflictAction::DoUpdate {
                assignments,
                filter,
            } => {
                if target_columns.is_none() {
                    return Err(DbError::new(
                        SYNTAX_ERROR,
                        "ON CONFLICT DO UPDATE requires a conflict target",
                    ));
                }
                let excluded = Identifier::unquoted("excluded");
                if table.name == excluded {
                    return Err(DbError::new(
                        "42712",
                        "table name excluded conflicts with the ON CONFLICT pseudo-relation",
                    )
                    .with_hint("Alias the target table when target aliases are supported."));
                }
                let width = table.columns().len();
                let mut inputs = table
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(index, column)| InputColumn {
                        binding: table.name.clone(),
                        name: column.name.clone(),
                        index,
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        outer_depth: 0,
                    })
                    .collect::<Vec<_>>();
                inputs.extend(table.columns().iter().enumerate().map(|(index, column)| {
                    InputColumn {
                        binding: excluded.clone(),
                        name: column.name.clone(),
                        index: width + index,
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        outer_depth: 0,
                    }
                }));
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
                                format!("column {} specified more than once", column.name),
                            )
                            .with_position_opt(column.position));
                        }
                        let expr = qualify_conflict_expr(expr, &table.name);
                        let bound = bind_expr_multi(
                            expr,
                            &inputs,
                            Some(&table.columns()[index].data_type),
                            false,
                        )?;
                        Ok((index, bound))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let filter = filter
                    .map(|expr| {
                        bind_multi_boolean(qualify_conflict_expr(expr, &table.name), &inputs)
                    })
                    .transpose()?;
                BoundConflictAction::DoUpdate {
                    assignments,
                    filter,
                }
            }
        };
    Ok(BoundOnConflict {
        target_columns,
        action,
    })
}

fn bind_conflict_target(
    target: ParsedConflictTarget,
    table: &TableDefinition,
) -> Result<Vec<usize>> {
    let column_ids = match target {
        ParsedConflictTarget::Columns(columns) => {
            if columns.is_empty() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "ON CONFLICT column target is empty",
                ));
            }
            let mut seen = BTreeSet::new();
            let column_ids = columns
                .into_iter()
                .map(|column| {
                    let definition = table.column(&column.name).ok_or_else(|| {
                        DbError::new(
                            UNDEFINED_COLUMN,
                            format!("column {} does not exist", column.name),
                        )
                        .with_position_opt(column.position)
                    })?;
                    if !seen.insert(definition.id) {
                        return Err(DbError::new(
                            "42701",
                            format!("column {} specified more than once", column.name),
                        )
                        .with_position_opt(column.position));
                    }
                    Ok(definition.id)
                })
                .collect::<Result<Vec<_>>>()?;
            let matches_unique_index = table.indexes().any(|index| {
                index.unique
                    && index.method == IndexMethod::BTree
                    && same_column_set(&index.key_columns, &column_ids)
            });
            if !matches_unique_index {
                return Err(DbError::new(
                    "42P10",
                    "there is no unique or exclusion constraint matching ON CONFLICT",
                ));
            }
            column_ids
        }
        ParsedConflictTarget::Constraint(name) => {
            let [name] = name.parts.as_slice() else {
                return unsupported_at(
                    "ON CONFLICT constraint names must be unqualified",
                    name.parts.first().and_then(|part| part.position),
                );
            };
            let constraint = table.constraint(&name.name).ok_or_else(|| {
                DbError::new(
                    "42704",
                    format!(
                        "constraint {} for table {} does not exist",
                        name.name, table.name
                    ),
                )
                .with_position_opt(name.position)
            })?;
            match &constraint.kind {
                ConstraintKind::PrimaryKey { columns } | ConstraintKind::Unique { columns } => {
                    columns.clone()
                }
                _ => {
                    return Err(DbError::new(
                        "42809",
                        format!("constraint {} is not unique", constraint.name),
                    )
                    .with_position_opt(name.position));
                }
            }
        }
    };
    column_ids
        .into_iter()
        .map(|column_id| {
            table.column_index_by_id(column_id).ok_or_else(|| {
                DbError::internal("ON CONFLICT target column is absent from its table")
            })
        })
        .collect()
}

fn same_column_set(left: &[ColumnId], right: &[ColumnId]) -> bool {
    left.len() == right.len() && left.iter().all(|column| right.contains(column))
}

fn qualify_conflict_expr(mut expr: ParsedExpr, target_binding: &Identifier) -> ParsedExpr {
    expr.kind = match expr.kind {
        ParsedExprKind::Column(mut name) if name.parts.len() == 1 => {
            let position = name.parts.first().and_then(|part| part.position);
            name.parts.insert(
                0,
                ParsedIdentifier {
                    name: target_binding.clone(),
                    position,
                },
            );
            ParsedExprKind::Column(name)
        }
        ParsedExprKind::Unary { op, expr } => ParsedExprKind::Unary {
            op,
            expr: Box::new(qualify_conflict_expr(*expr, target_binding)),
        },
        ParsedExprKind::Cast {
            expr,
            data_type,
            declared_type,
        } => ParsedExprKind::Cast {
            expr: Box::new(qualify_conflict_expr(*expr, target_binding)),
            data_type,
            declared_type,
        },
        ParsedExprKind::Array {
            elements,
            dimensions,
        } => ParsedExprKind::Array {
            elements: elements
                .into_iter()
                .map(|expr| qualify_conflict_expr(expr, target_binding))
                .collect(),
            dimensions,
        },
        ParsedExprKind::Function {
            function,
            arguments,
        } => ParsedExprKind::Function {
            function,
            arguments: arguments
                .into_iter()
                .map(|expr| qualify_conflict_expr(expr, target_binding))
                .collect(),
        },
        ParsedExprKind::Binary { left, op, right } => ParsedExprKind::Binary {
            left: Box::new(qualify_conflict_expr(*left, target_binding)),
            op,
            right: Box::new(qualify_conflict_expr(*right, target_binding)),
        },
        ParsedExprKind::InList {
            expr,
            list,
            negated,
        } => ParsedExprKind::InList {
            expr: Box::new(qualify_conflict_expr(*expr, target_binding)),
            list: list
                .into_iter()
                .map(|expr| qualify_conflict_expr(expr, target_binding))
                .collect(),
            negated,
        },
        ParsedExprKind::Aggregate {
            function,
            argument,
            distinct,
            filter,
        } => ParsedExprKind::Aggregate {
            function,
            argument: argument
                .map(|argument| Box::new(qualify_conflict_expr(*argument, target_binding))),
            distinct,
            filter: filter.map(|filter| Box::new(qualify_conflict_expr(*filter, target_binding))),
        },
        kind => kind,
    };
    expr
}

fn bind_create_index(index: ParsedCreateIndex, catalog: &Catalog) -> Result<BoundStatement> {
    let ParsedCreateIndex {
        name,
        table: table_name,
        key_columns,
        include_columns,
        unique,
        method,
        options,
        if_not_exists,
    } = index;
    let table = resolve_index_relation(&table_name, catalog)?;
    let schema = catalog
        .schema_by_id(table.schema_id)
        .ok_or_else(|| DbError::internal("index owner schema disappeared"))?;
    if schema
        .tables()
        .any(|candidate| candidate.index(&name.name).is_some())
    {
        if if_not_exists {
            return Ok(BoundStatement::NoOp {
                tag: "CREATE INDEX".to_owned(),
            });
        }
        return Err(
            DbError::new("42P07", format!("relation {} already exists", name.name))
                .with_position_opt(name.position),
        );
    }
    if key_columns.is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "CREATE INDEX requires at least one key column",
        ));
    }
    let mut seen = BTreeSet::new();
    for column in key_columns.iter().chain(&include_columns) {
        let definition = table.column(&column.name).ok_or_else(|| {
            DbError::new(
                UNDEFINED_COLUMN,
                format!("column {} does not exist", column.name),
            )
            .with_position_opt(column.position)
        })?;
        if !seen.insert(definition.id) {
            return Err(DbError::new(
                "42701",
                format!("column {} specified more than once", column.name),
            )
            .with_position_opt(column.position));
        }
    }
    let options = bind_index_options(
        method,
        table,
        &key_columns,
        &include_columns,
        unique,
        options,
    )?;
    Ok(BoundStatement::CreateIndex {
        table_id: table.id,
        index: NewIndex {
            name: name.name,
            key_columns: key_columns.into_iter().map(|column| column.name).collect(),
            include_columns: include_columns
                .into_iter()
                .map(|column| column.name)
                .collect(),
            unique,
            method,
            options,
        },
        if_not_exists,
    })
}
