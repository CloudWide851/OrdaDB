
fn execute_bound(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    match statement {
        BoundStatement::NoOp { tag } => Ok((command_events(Schema::empty(), tag, 0, None), false)),
        BoundStatement::Do { body } => {
            let program = compile_plpgsql(&body, &[])?;
            let limits = ordadb_plpgsql::ResourceLimits::default();
            let memory = VmMemoryGrant::new(limits.max_cursor_bytes)?;
            let output = {
                let mut host = EnginePlpgsqlHost {
                    state,
                    trigger: None,
                    exception_states: Vec::new(),
                    exception_triggers: Vec::new(),
                    exception_charges: Vec::new(),
                    exception_memory: memory.try_reserve(0)?,
                    sql_dirty: false,
                };
                execute_plpgsql_with_memory(&program, &mut host, &[], limits, memory)?
            };
            if output.return_value.is_some()
                || !output.returned_rows.is_empty()
                || output.return_parameter.is_some()
            {
                return Err(DbError::new(
                    "42601",
                    "DO blocks cannot return a value or result row",
                ));
            }
            Ok((command_events(Schema::empty(), "DO", 0, None), true))
        }
        BoundStatement::CreateSchema {
            name,
            if_not_exists,
        } => {
            if state.catalog.schema(&name).is_some() && if_not_exists {
                return Ok((
                    command_events(Schema::empty(), "CREATE SCHEMA", 0, None),
                    false,
                ));
            }
            Arc::make_mut(&mut state.catalog).create_schema(name)?;
            Ok((
                command_events(Schema::empty(), "CREATE SCHEMA", 0, None),
                true,
            ))
        }
        BoundStatement::CreateEnumType {
            schema,
            name,
            labels,
        } => {
            Arc::make_mut(&mut state.catalog).create_enum_type(&schema, name, labels)?;
            Ok((
                command_events(Schema::empty(), "CREATE TYPE", 0, None),
                true,
            ))
        }
        BoundStatement::CreateDomain {
            schema,
            name,
            base_type,
            base_declared_type,
            not_null,
            default,
            checks,
        } => {
            Arc::make_mut(&mut state.catalog).create_domain_with_declared_type(
                &schema,
                name,
                DomainBaseType::new(base_type, base_declared_type),
                not_null,
                default,
                checks,
            )?;
            Ok((
                command_events(Schema::empty(), "CREATE DOMAIN", 0, None),
                true,
            ))
        }
        BoundStatement::AlterEnumAddValue {
            type_id,
            label,
            position,
            if_not_exists,
        } => {
            let changed = Arc::make_mut(&mut state.catalog).alter_enum_add_value(
                type_id,
                label,
                position,
                if_not_exists,
            )?;
            if changed {
                rewrite_enum_values(state, type_id, None)?;
            }
            Ok((
                command_events(Schema::empty(), "ALTER TYPE", 0, None),
                changed,
            ))
        }
        BoundStatement::AlterEnumRenameValue {
            type_id,
            old_label,
            new_label,
        } => {
            Arc::make_mut(&mut state.catalog).alter_enum_rename_value(
                type_id,
                &old_label,
                new_label.clone(),
            )?;
            rewrite_enum_values(state, type_id, Some((&old_label, &new_label)))?;
            Ok((command_events(Schema::empty(), "ALTER TYPE", 0, None), true))
        }
        BoundStatement::AlterDomain { type_id, operation } => {
            let changed = match operation {
                BoundAlterDomainOperation::SetDefault(default) => {
                    Arc::make_mut(&mut state.catalog)
                        .alter_domain_default(type_id, Some(default))?;
                    true
                }
                BoundAlterDomainOperation::DropDefault => {
                    Arc::make_mut(&mut state.catalog).alter_domain_default(type_id, None)?;
                    true
                }
                BoundAlterDomainOperation::SetNotNull => {
                    Arc::make_mut(&mut state.catalog).alter_domain_not_null(type_id, true)?;
                    true
                }
                BoundAlterDomainOperation::DropNotNull => {
                    Arc::make_mut(&mut state.catalog).alter_domain_not_null(type_id, false)?;
                    true
                }
                BoundAlterDomainOperation::AddConstraint(constraint) => {
                    Arc::make_mut(&mut state.catalog).add_domain_constraint(type_id, constraint)?;
                    true
                }
                BoundAlterDomainOperation::DropConstraint { name, if_exists } => {
                    Arc::make_mut(&mut state.catalog)
                        .drop_domain_constraint(type_id, &name, if_exists)?
                }
            };
            validate_database_rows(state)?;
            Ok((
                command_events(Schema::empty(), "ALTER DOMAIN", 0, None),
                changed,
            ))
        }
        BoundStatement::AlterSchemaRename {
            schema_id,
            new_name,
        } => {
            Arc::make_mut(&mut state.catalog).rename_schema(schema_id, new_name)?;
            Ok((
                command_events(Schema::empty(), "ALTER SCHEMA", 0, None),
                true,
            ))
        }
        BoundStatement::DropObjects {
            kind,
            objects,
            behavior,
        } => execute_drop_objects(state, kind, objects, behavior),
        BoundStatement::CreateTable {
            schema,
            name,
            columns,
            constraints,
            if_not_exists,
        } => {
            if state.catalog.table(&schema, &name).is_some() && if_not_exists {
                return Ok((
                    command_events(Schema::empty(), "CREATE TABLE", 0, None),
                    false,
                ));
            }
            let table_id =
                Arc::make_mut(&mut state.catalog).create_table(&schema, name, columns)?;
            for constraint in constraints {
                Arc::make_mut(&mut state.catalog).create_constraint(table_id, constraint)?;
            }
            state.rows.insert(table_id, Arc::new(Vec::new()));
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "CREATE TABLE", 0, None),
                true,
            ))
        }
        BoundStatement::AlterTable {
            table_id,
            operations,
        } => execute_alter_table(state, table_id, operations),
        BoundStatement::CreateIndex {
            table_id,
            index,
            if_not_exists,
        } => {
            if table_definition(state, table_id)?
                .index(&index.name)
                .is_some()
                && if_not_exists
            {
                return Ok((
                    command_events(Schema::empty(), "CREATE INDEX", 0, None),
                    false,
                ));
            }
            Arc::make_mut(&mut state.catalog).create_index(table_id, index)?;
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "CREATE INDEX", 0, None),
                true,
            ))
        }
        BoundStatement::AlterIndexRename { index_id, new_name } => {
            Arc::make_mut(&mut state.catalog).rename_index(index_id, new_name)?;
            Ok((
                command_events(Schema::empty(), "ALTER INDEX", 0, None),
                true,
            ))
        }
        BoundStatement::CreateSequence {
            schema,
            sequence,
            if_not_exists,
        } => {
            if state.catalog.sequence(&schema, &sequence.name).is_some() && if_not_exists {
                return Ok((
                    command_events(Schema::empty(), "CREATE SEQUENCE", 0, None),
                    false,
                ));
            }
            Arc::make_mut(&mut state.catalog).create_sequence(&schema, sequence)?;
            Ok((
                command_events(Schema::empty(), "CREATE SEQUENCE", 0, None),
                true,
            ))
        }
        BoundStatement::AlterSequenceRename {
            sequence_id,
            new_name,
        } => {
            Arc::make_mut(&mut state.catalog).rename_sequence(sequence_id, new_name)?;
            Ok((
                command_events(Schema::empty(), "ALTER SEQUENCE", 0, None),
                true,
            ))
        }
        BoundStatement::AlterSequence {
            sequence_id,
            increment,
            min_value,
            max_value,
            restart,
            cycle,
            owner,
        } => {
            Arc::make_mut(&mut state.catalog).alter_sequence(
                sequence_id,
                SequenceAlteration {
                    increment,
                    min_value,
                    max_value,
                    restart,
                    cycle,
                    owner,
                },
            )?;
            Ok((
                command_events(Schema::empty(), "ALTER SEQUENCE", 0, None),
                true,
            ))
        }
        BoundStatement::CreateView {
            schema,
            name,
            kind,
            query,
            query_sql,
            output,
            references,
            replace,
            if_not_exists,
            with_data,
            existing,
        } => execute_create_view(
            state,
            CreateViewExecution {
                schema,
                name,
                kind,
                query: *query,
                query_sql,
                output,
                references,
                replace,
                if_not_exists,
                with_data,
                existing,
            },
            params,
        ),
        BoundStatement::AlterViewRename { view_id, new_name } => {
            let kind = state
                .catalog
                .view_by_id(view_id)
                .map(|view| view.kind)
                .ok_or_else(|| internal_error("view disappeared before rename"))?;
            Arc::make_mut(&mut state.catalog).rename_view(view_id, new_name)?;
            let tag = match kind {
                ordadb_catalog::ViewKind::Regular => "ALTER VIEW",
                ordadb_catalog::ViewKind::Materialized => "ALTER MATERIALIZED VIEW",
            };
            Ok((command_events(Schema::empty(), tag, 0, None), true))
        }
        BoundStatement::RefreshMaterializedView {
            view_id,
            table_id,
            query,
            with_data,
        } => {
            let rows = if with_data {
                materialize_statement_rows(state, *query, params)?
            } else {
                Vec::new()
            };
            state.rows.insert(table_id, Arc::new(rows));
            Arc::make_mut(&mut state.catalog)
                .set_materialized_view_populated(view_id, with_data)?;
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "REFRESH MATERIALIZED VIEW", 0, None),
                true,
            ))
        }
        BoundStatement::CreateRoutine {
            schema,
            name,
            kind,
            arguments,
            return_type,
            return_declared_type,
            returns_set,
            language,
            body,
            replace,
        } => {
            let argument_names = routine_argument_names(&arguments);
            let compile_names = if kind == ordadb_catalog::RoutineKind::Function
                && return_type.is_none()
                && arguments.is_empty()
            {
                vec!["old".to_owned(), "new".to_owned()]
            } else {
                argument_names
            };
            compile_plpgsql(&body, &compile_names)?;
            let tag = match kind {
                ordadb_catalog::RoutineKind::Function => "CREATE FUNCTION",
                ordadb_catalog::RoutineKind::Procedure => "CREATE PROCEDURE",
            };
            let referenced_types = arguments
                .iter()
                .filter_map(|argument| argument.declared_type)
                .chain(return_declared_type)
                .collect::<BTreeSet<_>>();
            Arc::make_mut(&mut state.catalog).create_or_replace_routine(
                &schema,
                NewRoutine {
                    name,
                    kind,
                    arguments,
                    return_type,
                    return_declared_type,
                    returns_set,
                    language,
                    body,
                    replace,
                    references: referenced_types
                        .into_iter()
                        .map(CatalogObjectRef::Type)
                        .collect(),
                },
            )?;
            Ok((command_events(Schema::empty(), tag, 0, None), true))
        }
        BoundStatement::DropRoutine {
            routine_id,
            behavior,
        } => {
            let kind = state
                .catalog
                .routine_by_id(routine_id)
                .map(|routine| routine.kind)
                .ok_or_else(|| DbError::new("42883", "routine does not exist"))?;
            let removed = Arc::make_mut(&mut state.catalog).drop_routine(routine_id, behavior)?;
            cleanup_removed_state(state, &removed);
            let tag = match kind {
                ordadb_catalog::RoutineKind::Function => "DROP FUNCTION",
                ordadb_catalog::RoutineKind::Procedure => "DROP PROCEDURE",
            };
            Ok((command_events(Schema::empty(), tag, 0, None), true))
        }
        BoundStatement::Call {
            routine_id,
            arguments,
            schema,
        } => {
            let output = execute_routine_program(state, routine_id, &arguments, params)?;
            let row_count = u64::from(!schema.fields.is_empty());
            let batch = (!schema.fields.is_empty()).then(|| Batch {
                schema: schema.clone(),
                rows: vec![Row::new(output.output_parameters)],
            });
            Ok((command_events(schema, "CALL", row_count, batch), true))
        }
        BoundStatement::ScalarSelect { projection, schema } => {
            let values = projection
                .iter()
                .map(|projection| evaluate_scalar(&projection.expr, &[], params))
                .collect::<Result<Vec<_>>>()?;
            Ok((
                command_events(
                    schema.clone(),
                    "SELECT 1",
                    1,
                    Some(Batch {
                        schema,
                        rows: vec![Row::new(values)],
                    }),
                ),
                false,
            ))
        }
        BoundStatement::RoutineSelect {
            routine_id,
            arguments,
            schema,
            returns_set,
        } => {
            let output = execute_routine_program(state, routine_id, &arguments, params)?;
            let values = if returns_set {
                output.returned_rows
            } else {
                vec![output.return_value.unwrap_or(Value::Null)]
            };
            let row_count = values.len() as u64;
            Ok((
                vec![
                    QueryEvent::Schema(schema.clone()),
                    QueryEvent::Batch(Batch {
                        schema,
                        rows: values
                            .into_iter()
                            .map(|value| Row::new(vec![value]))
                            .collect(),
                    }),
                    QueryEvent::Progress(QueryProgress {
                        rows_processed: row_count,
                    }),
                    QueryEvent::Complete(CommandComplete {
                        tag: format!("SELECT {row_count}"),
                        rows_affected: row_count,
                    }),
                ],
                false,
            ))
        }
        BoundStatement::SequenceValue {
            sequence_id,
            operation,
            schema,
        } => {
            let (value, dirty) = match operation {
                BoundSequenceOperation::NextValue => (
                    Arc::make_mut(&mut state.catalog).next_sequence_value(sequence_id)?,
                    true,
                ),
                BoundSequenceOperation::CurrentValue { value } => (
                    value.ok_or_else(|| {
                        DbError::new(
                            "55000",
                            "currval of sequence is not yet defined in this session",
                        )
                    })?,
                    false,
                ),
                BoundSequenceOperation::SetValue { value, is_called } => {
                    let value = evaluate_scalar(&value, &[], params)?;
                    let Value::Int64(value) = value else {
                        return Err(internal_error(
                            "bound setval expression did not produce BIGINT",
                        ));
                    };
                    Arc::make_mut(&mut state.catalog).set_sequence_value(
                        sequence_id,
                        value,
                        is_called,
                    )?;
                    (value, true)
                }
            };
            if dirty {
                state.sequence_currvals.insert(sequence_id, value);
            }
            Ok((
                command_events(
                    schema.clone(),
                    "SELECT 1",
                    1,
                    Some(Batch {
                        schema,
                        rows: vec![Row::new(vec![Value::Int64(value)])],
                    }),
                ),
                dirty,
            ))
        }
        BoundStatement::CreateTrigger {
            target,
            name,
            timing,
            level,
            events,
            routine_id,
        } => {
            Arc::make_mut(&mut state.catalog).create_trigger_on_target_with_level(
                target,
                name,
                timing,
                level,
                events.into_iter().collect(),
                routine_id,
            )?;
            Ok((
                command_events(Schema::empty(), "CREATE TRIGGER", 0, None),
                true,
            ))
        }
        BoundStatement::DropTrigger {
            trigger_id,
            behavior,
        } => {
            let removed = Arc::make_mut(&mut state.catalog).drop_trigger(trigger_id, behavior)?;
            cleanup_removed_state(state, &removed);
            Ok((
                command_events(Schema::empty(), "DROP TRIGGER", 0, None),
                true,
            ))
        }
        BoundStatement::Insert {
            table_id,
            column_indexes,
            rows,
            on_conflict,
            returning,
        } => execute_insert(
            state,
            table_id,
            column_indexes,
            rows,
            on_conflict,
            returning,
            params,
        ),
        BoundStatement::ViewInsert {
            view_id,
            source,
            column_indexes,
            rows,
            returning,
        } => execute_view_insert(
            state,
            view_id,
            *source,
            column_indexes,
            rows,
            returning,
            params,
        ),
        BoundStatement::Merge(merge) => execute_merge(state, merge, params),
        BoundStatement::With {
            ctes,
            body,
            catalog,
            schema,
        } => execute_with_clause(state, ctes, *body, *catalog, schema, params),
        BoundStatement::SetOperation {
            left,
            operator,
            all,
            right,
            schema,
            order_by,
            offset,
            limit,
        } => execute_set_operation(
            state,
            SetExecution {
                left,
                operator,
                all,
                right,
                schema,
                order_by,
                offset,
                limit,
            },
            params,
        ),
        BoundStatement::Select {
            table_id,
            schema,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => execute_select(
            state,
            SelectExecution {
                table_id,
                schema,
                projection,
                filter,
                order_by,
                offset,
                limit,
            },
            params,
        ),
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
        } => execute_advanced_select(
            state,
            AdvancedExecution {
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
                limit: limit.map(|limit| *limit),
                aggregate,
            },
            params,
        ),
        BoundStatement::ViewSelect {
            source,
            schema,
            projection,
            ..
        } => execute_view_select(state, *source, schema, projection, params),
        BoundStatement::Explain { statement } => execute_explain(state, *statement),
        BoundStatement::Update {
            table_id,
            assignments,
            filter,
            returning,
        } => execute_update(state, table_id, assignments, filter, returning, params),
        BoundStatement::ViewUpdate {
            view_id,
            source,
            assignments,
            filter,
            returning,
        } => execute_view_update(
            state,
            view_id,
            *source,
            assignments,
            filter,
            returning,
            params,
        ),
        BoundStatement::Delete {
            table_id,
            filter,
            returning,
        } => execute_delete(state, table_id, filter, returning, params),
        BoundStatement::ViewDelete {
            view_id,
            source,
            filter,
            returning,
        } => execute_view_delete(state, view_id, *source, filter, returning, params),
        BoundStatement::Analyze { .. }
        | BoundStatement::Vacuum { .. }
        | BoundStatement::Reindex { .. } => Err(internal_error(
            "maintenance statement was not routed through the root executor",
        )),
        BoundStatement::Listen { .. }
        | BoundStatement::Unlisten { .. }
        | BoundStatement::Notify { .. }
        | BoundStatement::PgNotify { .. }
        | BoundStatement::DiscardAll
        | BoundStatement::DeallocateAll => Err(internal_error(
            "session command was not routed through the session executor",
        )),
        BoundStatement::Begin { .. }
        | BoundStatement::Commit { .. }
        | BoundStatement::Rollback { .. }
        | BoundStatement::Savepoint { .. }
        | BoundStatement::RollbackTo { .. }
        | BoundStatement::ReleaseSavepoint { .. } => Err(DbError::new(
            "25000",
            "transaction control was not routed through the session",
        )
        .with_hint("execute transaction control through Session")),
    }
}

fn routine_argument_names(arguments: &[ordadb_catalog::RoutineArgument]) -> Vec<String> {
    arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            argument
                .name
                .as_ref()
                .map_or_else(|| format!("__arg{}", index + 1), |name| name.to_string())
        })
        .collect()
}

fn execute_routine_program(
    state: &mut DatabaseState,
    routine_id: ordadb_types::RoutineId,
    arguments: &[BoundExpr],
    params: &[Value],
) -> Result<ordadb_plpgsql::VmOutput> {
    execute_routine_program_with_boundaries(state, routine_id, arguments, params, None)
        .map(|(output, _)| output)
}
