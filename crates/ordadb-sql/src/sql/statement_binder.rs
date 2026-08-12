
fn bind_with_view_depth(
    statement: ParsedStatement,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    if view_depth > 64 {
        return Err(DbError::new(
            "54001",
            "view expansion exceeds the maximum depth of 64",
        ));
    }
    match statement {
        ParsedStatement::Begin { characteristics } => Ok(BoundStatement::Begin { characteristics }),
        ParsedStatement::Commit { chain } => Ok(BoundStatement::Commit { chain }),
        ParsedStatement::Rollback { chain } => Ok(BoundStatement::Rollback { chain }),
        ParsedStatement::Savepoint { name } => Ok(BoundStatement::Savepoint { name: name.name }),
        ParsedStatement::RollbackTo { name } => Ok(BoundStatement::RollbackTo { name: name.name }),
        ParsedStatement::ReleaseSavepoint { name } => {
            Ok(BoundStatement::ReleaseSavepoint { name: name.name })
        }
        ParsedStatement::Analyze { table } => Ok(BoundStatement::Analyze {
            table_id: table
                .as_ref()
                .map(|table| resolve_table(table, catalog).map(|table| table.id))
                .transpose()?,
        }),
        ParsedStatement::Vacuum { table, analyze } => Ok(BoundStatement::Vacuum {
            table_id: table
                .as_ref()
                .map(|table| resolve_table(table, catalog).map(|table| table.id))
                .transpose()?,
            analyze,
        }),
        ParsedStatement::Reindex { target } => {
            let target = match target {
                ParsedReindexTarget::Index(name) => {
                    let (schema, name, position) = split_table_name(&name)?;
                    let index = catalog.index(&schema, &name).ok_or_else(|| {
                        DbError::new("42704", format!("index {schema}.{name} does not exist"))
                            .with_position_opt(position)
                    })?;
                    BoundReindexTarget::Index(index.id)
                }
                ParsedReindexTarget::Table(name) => {
                    BoundReindexTarget::Table(resolve_table(&name, catalog)?.id)
                }
                ParsedReindexTarget::Schema(name) => {
                    let schema = catalog.schema(&name.name).ok_or_else(|| {
                        DbError::new(
                            UNDEFINED_SCHEMA,
                            format!("schema {} does not exist", name.name),
                        )
                        .with_position_opt(name.position)
                    })?;
                    BoundReindexTarget::Schema(schema.id)
                }
                ParsedReindexTarget::Database(name) => {
                    if catalog.database().name != name.name {
                        return Err(DbError::new(
                            "3D000",
                            format!("database {} does not exist", name.name),
                        )
                        .with_position_opt(name.position));
                    }
                    BoundReindexTarget::Database
                }
            };
            Ok(BoundStatement::Reindex { target })
        }
        ParsedStatement::Listen { channel } => Ok(BoundStatement::Listen {
            channel: channel.name,
        }),
        ParsedStatement::Unlisten { channel } => Ok(BoundStatement::Unlisten {
            channel: channel.map(|channel| channel.name),
        }),
        ParsedStatement::Notify { channel, payload } => Ok(BoundStatement::Notify {
            channel: channel.name,
            payload,
        }),
        ParsedStatement::Do { body } => Ok(BoundStatement::Do { body }),
        ParsedStatement::DiscardAll => Ok(BoundStatement::DiscardAll),
        ParsedStatement::DeallocateAll => Ok(BoundStatement::DeallocateAll),
        ParsedStatement::CreateSchema {
            name,
            if_not_exists,
        } => {
            if catalog.schema(&name.name).is_some() && !if_not_exists {
                return Err(
                    DbError::new("42P06", format!("schema {} already exists", name.name))
                        .with_position_opt(name.position),
                );
            }
            Ok(BoundStatement::CreateSchema {
                name: name.name,
                if_not_exists,
            })
        }
        ParsedStatement::CreateEnumType { name, labels } => {
            let (schema, name, position) = split_table_name(&name)?;
            if catalog.schema(&schema).is_none() {
                return Err(DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {schema} does not exist"),
                )
                .with_position_opt(position));
            }
            if catalog.user_defined_type(&schema, &name).is_some() {
                return Err(
                    DbError::new("42710", format!("type {schema}.{name} already exists"))
                        .with_position_opt(position),
                );
            }
            Ok(BoundStatement::CreateEnumType {
                schema,
                name,
                labels,
            })
        }
        ParsedStatement::CreateDomain {
            name,
            base_type,
            base_declared_type,
            not_null,
            default,
            checks,
        } => {
            let (schema, name, position) = split_table_name(&name)?;
            if catalog.schema(&schema).is_none() {
                return Err(DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {schema} does not exist"),
                )
                .with_position_opt(position));
            }
            if catalog.user_defined_type(&schema, &name).is_some() {
                return Err(
                    DbError::new("42710", format!("type {schema}.{name} already exists"))
                        .with_position_opt(position),
                );
            }
            let (base_type, base_declared_type) = match base_declared_type {
                Some(type_name) => {
                    let definition = resolve_user_defined_type(&type_name, catalog)?;
                    if matches!(
                        definition.definition,
                        ordadb_catalog::UserDefinedTypeKind::Domain { .. }
                    ) {
                        return unsupported(
                            "domains whose base type is another domain are not supported yet",
                        );
                    }
                    let (base_type, type_id) =
                        resolve_declared_data_type(catalog, &base_type, &type_name)?;
                    (base_type, Some(type_id))
                }
                None => (base_type, None),
            };
            let default = default
                .map(|default| {
                    bind_expr(default.expression, None, Some(&base_type))?;
                    Ok(CatalogExpression::new(default.sql))
                })
                .transpose()?;
            let scope =
                TableDefinition::expression_scope(Identifier::unquoted("value"), base_type.clone());
            for constraint in &checks {
                bind_catalog_expression_with_catalog(
                    &constraint.expression,
                    Some(&scope),
                    Some(&ScalarType::Boolean),
                    catalog,
                )?;
            }
            Ok(BoundStatement::CreateDomain {
                schema,
                name,
                base_type,
                base_declared_type,
                not_null,
                default,
                checks,
            })
        }
        ParsedStatement::AlterEnumAddValue {
            name,
            label,
            position,
            if_not_exists,
        } => {
            let definition = resolve_user_defined_type(&name, catalog)?;
            if !matches!(
                definition.definition,
                ordadb_catalog::UserDefinedTypeKind::Enum { .. }
            ) {
                return Err(DbError::new(
                    "42809",
                    "ALTER TYPE ADD VALUE requires an enum type",
                ));
            }
            Ok(BoundStatement::AlterEnumAddValue {
                type_id: definition.id,
                label,
                position,
                if_not_exists,
            })
        }
        ParsedStatement::AlterEnumRenameValue {
            name,
            old_label,
            new_label,
        } => {
            let definition = resolve_user_defined_type(&name, catalog)?;
            if !matches!(
                definition.definition,
                ordadb_catalog::UserDefinedTypeKind::Enum { .. }
            ) {
                return Err(DbError::new(
                    "42809",
                    "ALTER TYPE RENAME VALUE requires an enum type",
                ));
            }
            Ok(BoundStatement::AlterEnumRenameValue {
                type_id: definition.id,
                old_label,
                new_label,
            })
        }
        ParsedStatement::AlterDomain { name, operation } => {
            let definition = resolve_user_defined_type(&name, catalog)?;
            let ordadb_catalog::UserDefinedTypeKind::Domain {
                base_type, checks, ..
            } = &definition.definition
            else {
                return Err(DbError::new("42809", "ALTER DOMAIN requires a domain type"));
            };
            let operation = match operation {
                ParsedAlterDomainOperation::SetDefault(default) => {
                    bind_expr(default.expression, None, Some(base_type))?;
                    BoundAlterDomainOperation::SetDefault(CatalogExpression::new(default.sql))
                }
                ParsedAlterDomainOperation::DropDefault => BoundAlterDomainOperation::DropDefault,
                ParsedAlterDomainOperation::SetNotNull => BoundAlterDomainOperation::SetNotNull,
                ParsedAlterDomainOperation::DropNotNull => BoundAlterDomainOperation::DropNotNull,
                ParsedAlterDomainOperation::AddConstraint(constraint) => {
                    let scope = TableDefinition::expression_scope(
                        Identifier::unquoted("value"),
                        base_type.clone(),
                    );
                    bind_catalog_expression_with_catalog(
                        &constraint.expression,
                        Some(&scope),
                        Some(&ScalarType::Boolean),
                        catalog,
                    )?;
                    BoundAlterDomainOperation::AddConstraint(constraint)
                }
                ParsedAlterDomainOperation::DropConstraint { name, if_exists } => {
                    if !if_exists
                        && !checks
                            .iter()
                            .any(|constraint| constraint.name.as_ref() == Some(&name.name))
                    {
                        return Err(DbError::new(
                            "42704",
                            format!("constraint {} does not exist", name.name),
                        )
                        .with_position_opt(name.position));
                    }
                    BoundAlterDomainOperation::DropConstraint {
                        name: name.name,
                        if_exists,
                    }
                }
            };
            Ok(BoundStatement::AlterDomain {
                type_id: definition.id,
                operation,
            })
        }
        ParsedStatement::AlterSchemaRename {
            name,
            new_name,
            if_exists,
        } => {
            let Some(schema) = catalog.schema(&name.name) else {
                if if_exists {
                    return Ok(BoundStatement::NoOp {
                        tag: "ALTER SCHEMA".to_owned(),
                    });
                }
                return Err(DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {} does not exist", name.name),
                )
                .with_position_opt(name.position));
            };
            if catalog.schema(&new_name.name).is_some() {
                return Err(DbError::new(
                    "42P06",
                    format!("schema {} already exists", new_name.name),
                )
                .with_position_opt(new_name.position));
            }
            Ok(BoundStatement::AlterSchemaRename {
                schema_id: schema.id,
                new_name: new_name.name,
            })
        }
        ParsedStatement::DropObjects {
            kind,
            names,
            if_exists,
            behavior,
        } => bind_drop_objects(kind, names, if_exists, behavior, catalog),
        ParsedStatement::CreateTable {
            name,
            columns,
            constraints,
            if_not_exists,
        } => bind_create_table(name, columns, constraints, if_not_exists, catalog),
        ParsedStatement::AlterTable {
            name,
            if_exists,
            operations,
        } => bind_alter_table(name, if_exists, operations, catalog),
        ParsedStatement::CreateIndex(index) => bind_create_index(index, catalog),
        ParsedStatement::AlterIndexRename { name, new_name } => {
            let (schema, index, position) = split_table_name(&name)?;
            let index = catalog.index(&schema, &index).ok_or_else(|| {
                DbError::new("42704", format!("index {schema}.{index} does not exist"))
                    .with_position_opt(position)
            })?;
            Ok(BoundStatement::AlterIndexRename {
                index_id: index.id,
                new_name: new_name.name,
            })
        }
        ParsedStatement::CreateSequence {
            name,
            mut sequence,
            if_not_exists,
            owner,
        } => {
            let (schema, sequence_name, position) = split_table_name(&name)?;
            if catalog.schema(&schema).is_none() {
                return Err(DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {schema} does not exist"),
                )
                .with_position_opt(position));
            }
            if catalog.sequence(&schema, &sequence_name).is_some() && !if_not_exists {
                return Err(DbError::new(
                    "42P07",
                    format!("relation {schema}.{sequence_name} already exists"),
                )
                .with_position_opt(position));
            }
            sequence.name = sequence_name;
            sequence.owner = owner
                .map(|(table, column)| {
                    let table = resolve_table(&table, catalog)?;
                    let column = table.column(&column.name).ok_or_else(|| {
                        DbError::new(
                            UNDEFINED_COLUMN,
                            format!("column {} does not exist", column.name),
                        )
                        .with_position_opt(column.position)
                    })?;
                    Ok((table.id, column.id))
                })
                .transpose()?;
            Ok(BoundStatement::CreateSequence {
                schema,
                sequence,
                if_not_exists,
            })
        }
        ParsedStatement::AlterSequenceRename {
            name,
            if_exists,
            new_name,
        } => {
            let (schema_name, sequence_name, position) = split_table_name(&name)?;
            let Some(sequence) = catalog.sequence(&schema_name, &sequence_name) else {
                if if_exists {
                    return Ok(BoundStatement::NoOp {
                        tag: "ALTER SEQUENCE".to_owned(),
                    });
                }
                return Err(DbError::new(
                    "42P01",
                    format!("sequence {schema_name}.{sequence_name} does not exist"),
                )
                .with_position_opt(position));
            };
            Ok(BoundStatement::AlterSequenceRename {
                sequence_id: sequence.id,
                new_name: new_name.name,
            })
        }
        ParsedStatement::AlterSequence {
            name,
            if_exists,
            options,
        } => {
            let (schema_name, sequence_name, position) = split_table_name(&name)?;
            let Some(sequence) = catalog.sequence(&schema_name, &sequence_name) else {
                if if_exists {
                    return Ok(BoundStatement::NoOp {
                        tag: "ALTER SEQUENCE".to_owned(),
                    });
                }
                return Err(DbError::new(
                    "42P01",
                    format!("sequence {schema_name}.{sequence_name} does not exist"),
                )
                .with_position_opt(position));
            };
            let owner = options
                .owner
                .map(|owner| {
                    owner
                        .map(|(table, column)| {
                            let table = resolve_table(&table, catalog)?;
                            let column = table.column(&column.name).ok_or_else(|| {
                                DbError::new(
                                    UNDEFINED_COLUMN,
                                    format!("column {} does not exist", column.name),
                                )
                                .with_position_opt(column.position)
                            })?;
                            Ok((table.id, column.id))
                        })
                        .transpose()
                })
                .transpose()?;
            Ok(BoundStatement::AlterSequence {
                sequence_id: sequence.id,
                increment: options.increment,
                min_value: options.min_value,
                max_value: options.max_value,
                restart: options.restart,
                cycle: options.cycle,
                owner,
            })
        }
        ParsedStatement::CreateView {
            name,
            kind,
            query,
            query_sql,
            columns,
            replace,
            if_not_exists,
            with_data,
        } => bind_create_view(
            CreateViewBindingInput {
                name,
                kind,
                query: *query,
                query_sql,
                columns,
                replace,
                if_not_exists,
                with_data,
            },
            catalog,
            view_depth,
        ),
        ParsedStatement::AlterViewRename {
            name,
            kind,
            if_exists,
            new_name,
        } => {
            let (schema, name, position) = split_table_name(&name)?;
            let Some(view) = catalog.view(&schema, &name) else {
                if if_exists {
                    let tag = match kind {
                        ViewKind::Regular => "ALTER VIEW",
                        ViewKind::Materialized => "ALTER MATERIALIZED VIEW",
                    };
                    return Ok(BoundStatement::NoOp {
                        tag: tag.to_owned(),
                    });
                }
                return Err(DbError::new(
                    UNDEFINED_TABLE,
                    format!("view {schema}.{name} does not exist"),
                )
                .with_position_opt(position));
            };
            if view.kind != kind {
                let expected = match kind {
                    ViewKind::Regular => "view",
                    ViewKind::Materialized => "materialized view",
                };
                return Err(DbError::new(
                    "42809",
                    format!("{schema}.{name} is not a {expected}"),
                ));
            }
            Ok(BoundStatement::AlterViewRename {
                view_id: view.id,
                new_name: new_name.name,
            })
        }
        ParsedStatement::RefreshMaterializedView { name, with_data } => {
            let (schema, name, position) = split_table_name(&name)?;
            let view = catalog.view(&schema, &name).ok_or_else(|| {
                DbError::new(
                    UNDEFINED_TABLE,
                    format!("materialized view {schema}.{name} does not exist"),
                )
                .with_position_opt(position)
            })?;
            if view.kind != ViewKind::Materialized {
                return Err(DbError::new(
                    "42809",
                    format!("{schema}.{name} is not a materialized view"),
                ));
            }
            let table_id = view.materialized_table_id.ok_or_else(|| {
                DbError::internal("materialized view is missing its backing table")
            })?;
            let query =
                bind_with_view_depth(parse(&view.query)?, catalog, view_depth.saturating_add(1))?;
            Ok(BoundStatement::RefreshMaterializedView {
                view_id: view.id,
                table_id,
                query: Box::new(query),
                with_data,
            })
        }
        ParsedStatement::CreateRoutine {
            name,
            kind,
            arguments,
            return_type,
            return_declared_type,
            returns_set,
            language,
            body,
            replace,
        } => bind_routine_statement(
            ParsedStatement::CreateRoutine {
                name,
                kind,
                arguments,
                return_type,
                return_declared_type,
                returns_set,
                language,
                body,
                replace,
            },
            catalog,
        ),
        ParsedStatement::DropRoutine {
            name,
            kind,
            argument_types,
            if_exists,
            behavior,
        } => bind_routine_statement(
            ParsedStatement::DropRoutine {
                name,
                kind,
                argument_types,
                if_exists,
                behavior,
            },
            catalog,
        ),
        ParsedStatement::Call { name, arguments } => {
            bind_routine_statement(ParsedStatement::Call { name, arguments }, catalog)
        }
        ParsedStatement::RoutineSelect {
            name,
            arguments,
            alias,
        } => bind_routine_statement(
            ParsedStatement::RoutineSelect {
                name,
                arguments,
                alias,
            },
            catalog,
        ),
        ParsedStatement::PgNotify {
            channel,
            payload,
            alias,
        } => {
            let text = ScalarType::Text;
            Ok(BoundStatement::PgNotify {
                channel: bind_expr(channel, None, Some(&text))?,
                payload: bind_expr(payload, None, Some(&text))?,
                schema: Schema::new(vec![Field::new(
                    alias.map_or_else(|| "pg_notify".to_owned(), |alias| alias.name.to_string()),
                    ScalarType::Text,
                    true,
                )]),
            })
        }
        ParsedStatement::ScalarSelect { projection } => {
            let mut bound_projection = Vec::with_capacity(projection.len());
            let mut fields = Vec::with_capacity(projection.len());
            for projection in projection {
                let ParsedProjection::Expression { expr, alias } = projection else {
                    return unsupported("SELECT without FROM does not support wildcards");
                };
                let field_name = alias
                    .as_ref()
                    .map(|alias| alias.name.as_str().to_owned())
                    .unwrap_or_else(|| projection_name(&expr));
                let expr = bind_expr(expr, None, None)?;
                let field = Field::new(field_name, expr.data_type.clone(), expr.nullable);
                bound_projection.push(BoundProjection {
                    expr,
                    field: field.clone(),
                });
                fields.push(field);
            }
            Ok(BoundStatement::ScalarSelect {
                projection: bound_projection,
                schema: Schema::new(fields),
            })
        }
        ParsedStatement::SequenceValue {
            name,
            operation,
            alias,
        } => {
            let (schema_name, sequence_name, position) = split_table_name(&name)?;
            let sequence = catalog
                .sequence(&schema_name, &sequence_name)
                .ok_or_else(|| {
                    DbError::new(
                        "42P01",
                        format!("sequence {schema_name}.{sequence_name} does not exist"),
                    )
                    .with_position_opt(position)
                })?;
            let operation = match operation {
                ParsedSequenceOperation::NextValue => BoundSequenceOperation::NextValue,
                ParsedSequenceOperation::CurrentValue => {
                    BoundSequenceOperation::CurrentValue { value: None }
                }
                ParsedSequenceOperation::SetValue { value, is_called } => {
                    BoundSequenceOperation::SetValue {
                        value: bind_expr(value, None, Some(&ScalarType::Int64))?,
                        is_called,
                    }
                }
            };
            let field_name = alias.as_ref().map_or_else(
                || match &operation {
                    BoundSequenceOperation::NextValue => "nextval",
                    BoundSequenceOperation::CurrentValue { .. } => "currval",
                    BoundSequenceOperation::SetValue { .. } => "setval",
                },
                |alias| alias.name.as_str(),
            );
            Ok(BoundStatement::SequenceValue {
                sequence_id: sequence.id,
                operation,
                schema: Schema::new(vec![Field::new(field_name, ScalarType::Int64, false)]),
            })
        }
        ParsedStatement::CreateTrigger {
            name,
            table,
            timing,
            level,
            events,
            routine,
        } => {
            let target = resolve_trigger_target(&table, catalog)?;
            let (routine_schema, routine_name, routine_position) = split_table_name(&routine)?;
            let schema = catalog.schema(&routine_schema).ok_or_else(|| {
                DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {routine_schema} does not exist"),
                )
                .with_position_opt(routine_position)
            })?;
            let matches = schema
                .routines_named(&routine_name)
                .iter()
                .filter(|routine| {
                    routine.kind == RoutineKind::Function
                        && routine.arguments.is_empty()
                        && routine.return_type.is_none()
                })
                .collect::<Vec<_>>();
            let routine_id = match matches.as_slice() {
                [routine] => routine.id,
                [] => {
                    return Err(DbError::new(
                        "42883",
                        format!(
                            "trigger function {routine_schema}.{routine_name}() does not exist"
                        ),
                    )
                    .with_position_opt(routine_position));
                }
                _ => {
                    return Err(DbError::new(
                        "42725",
                        format!("trigger function {routine_schema}.{routine_name}() is ambiguous"),
                    )
                    .with_position_opt(routine_position));
                }
            };
            Ok(BoundStatement::CreateTrigger {
                target,
                name: name.name,
                timing,
                level,
                events,
                routine_id,
            })
        }
        ParsedStatement::DropTrigger {
            name,
            table,
            if_exists,
            behavior,
        } => {
            let target = resolve_trigger_target(&table, catalog)?;
            let trigger = match target {
                TriggerTarget::Table(table_id) => catalog
                    .table_by_id(table_id)
                    .and_then(|table| table.trigger(&name.name)),
                TriggerTarget::View(view_id) => catalog
                    .view_by_id(view_id)
                    .and_then(|view| view.trigger(&name.name)),
            };
            let Some(trigger) = trigger else {
                if if_exists {
                    return Ok(BoundStatement::NoOp {
                        tag: "DROP TRIGGER".to_owned(),
                    });
                }
                return Err(DbError::new(
                    "42704",
                    format!(
                        "trigger {} for relation {} does not exist",
                        name.name,
                        trigger_target_name(target, catalog)?
                    ),
                )
                .with_position_opt(name.position));
            };
            Ok(BoundStatement::DropTrigger {
                trigger_id: trigger.id,
                behavior,
            })
        }
        ParsedStatement::Insert {
            table,
            columns,
            rows,
            on_conflict,
            returning,
        } => bind_insert(
            table,
            columns,
            rows,
            on_conflict,
            returning,
            catalog,
            view_depth,
        ),
        ParsedStatement::Merge(merge) => bind_merge(merge, catalog),
        ParsedStatement::With {
            recursive,
            ctes,
            body,
        } => bind_with_clause(recursive, ctes, *body, catalog, view_depth),
        ParsedStatement::SetOperation {
            left,
            operator,
            all,
            right,
            order_by,
            offset,
            limit,
        } => bind_set_operation(
            *left, operator, all, *right, order_by, offset, limit, catalog, view_depth,
        ),
        ParsedStatement::Select {
            table,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => bind_select(
            SelectInput {
                table_name: table,
                projection,
                filter,
                order_by,
                offset,
                limit,
            },
            catalog,
            view_depth,
        ),
        ParsedStatement::AdvancedSelect {
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
        } => bind_advanced_select(
            AdvancedSelectInput {
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
            },
            catalog,
            view_depth,
            &[],
        ),
        ParsedStatement::Explain { statement } => {
            let statement = bind_with_view_depth(*statement, catalog, view_depth)?;
            if !matches!(
                statement,
                BoundStatement::Select { .. } | BoundStatement::AdvancedSelect { .. }
            ) {
                return unsupported("EXPLAIN supports SELECT statements only");
            }
            Ok(BoundStatement::Explain {
                statement: Box::new(statement),
            })
        }
        ParsedStatement::Update {
            table,
            assignments,
            filter,
            returning,
        } => bind_update(table, assignments, filter, returning, catalog, view_depth),
        ParsedStatement::Delete {
            table,
            filter,
            returning,
        } => bind_delete(table, filter, returning, catalog, view_depth),
    }
}
