
fn execute_routine_program_with_boundaries(
    state: &mut DatabaseState,
    routine_id: ordadb_types::RoutineId,
    arguments: &[BoundExpr],
    params: &[Value],
    mut boundary_handler: Option<&mut ProcedureBoundaryHandler<'_>>,
) -> Result<(ordadb_plpgsql::VmOutput, bool)> {
    let routine_limits = ordadb_plpgsql::ResourceLimits::default();
    let routine_memory = VmMemoryGrant::new(routine_limits.max_cursor_bytes)?;
    let root = prepare_routine_vm_frame(
        state,
        routine_id,
        arguments,
        params,
        RoutineCompletion::Root,
        &routine_memory,
    )?;
    let mut frames = vec![root];
    let mut segment_dirty = false;
    loop {
        let resumed = {
            let frame = frames
                .last_mut()
                .ok_or_else(|| internal_error("PL/pgSQL VM frame stack is empty"))?;
            let mut host = EnginePlpgsqlHost {
                state,
                trigger: None,
                exception_states: mem::take(&mut frame.exception_states),
                exception_triggers: mem::take(&mut frame.exception_triggers),
                exception_charges: mem::take(&mut frame.exception_charges),
                exception_memory: mem::replace(
                    &mut frame.exception_memory,
                    routine_memory.try_reserve(0)?,
                ),
                sql_dirty: false,
            };
            let resumed = frame.machine.resume(&mut host, frame.response.take());
            frame.exception_states = host.exception_states;
            frame.exception_triggers = host.exception_triggers;
            frame.exception_charges = host.exception_charges;
            frame.exception_memory = host.exception_memory;
            resumed
        };
        let resumed = match resumed {
            Ok(resumed) => resumed,
            Err(error) => {
                let failed = frames
                    .pop()
                    .ok_or_else(|| internal_error("PL/pgSQL failed frame is missing"))?;
                state.routine_frames.pop(failed.id)?;
                match failed.completion {
                    RoutineCompletion::Root => return Err(error),
                    RoutineCompletion::Call { .. } | RoutineCompletion::Select { .. } => {
                        frames
                            .last_mut()
                            .ok_or_else(|| internal_error("PL/pgSQL parent frame is missing"))?
                            .response = Some(Err(error));
                        continue;
                    }
                }
            }
        };
        match resumed {
            VmRunState::Sql(request) => {
                let statement =
                    match parse(&request.sql).and_then(|parsed| bind(parsed, &state.catalog)) {
                        Ok(statement) => statement,
                        Err(error) => {
                            frames
                                .last_mut()
                                .ok_or_else(|| internal_error("PL/pgSQL SQL frame is missing"))?
                                .response = Some(Err(error));
                            continue;
                        }
                    };
                let boundary = match &statement {
                    BoundStatement::Commit { chain } => Some(ProcedureBoundary::Commit(*chain)),
                    BoundStatement::Rollback { chain } => Some(ProcedureBoundary::Rollback(*chain)),
                    _ => None,
                };
                if let Some(boundary) = boundary {
                    let response = if frames.len() != 1
                        || !frames.first().is_some_and(|frame| {
                            frame.routine.kind == ordadb_catalog::RoutineKind::Procedure
                        }) {
                        Err(DbError::new(
                            "2D000",
                            "invalid transaction termination inside this PL/pgSQL invocation",
                        )
                        .with_hint(
                            "transaction termination is allowed only in an eligible top-level procedure CALL",
                        ))
                    } else if let Err(error) = frames
                        .last()
                        .ok_or_else(|| internal_error("PL/pgSQL SQL frame is missing"))?
                        .machine
                        .ensure_transaction_boundary_ready()
                    {
                        Err(error)
                    } else if let Some(handler) = boundary_handler.as_deref_mut() {
                        match handler(boundary, state, segment_dirty) {
                            Ok(()) => {
                                segment_dirty = false;
                                let tag = match boundary {
                                    ProcedureBoundary::Commit(_) => "COMMIT",
                                    ProcedureBoundary::Rollback(_) => "ROLLBACK",
                                };
                                Ok(Box::new(transaction_events(tag).into_iter().map(Ok))
                                    as VmSqlStream)
                            }
                            Err(error) => Err(error),
                        }
                    } else {
                        Err(DbError::new(
                            "2D000",
                            "invalid transaction termination inside this PL/pgSQL invocation",
                        )
                        .with_hint(
                            "transaction termination requires an eligible top-level procedure CALL in autocommit mode",
                        ))
                    };
                    frames
                        .last_mut()
                        .ok_or_else(|| internal_error("PL/pgSQL SQL frame is missing"))?
                        .response = Some(response);
                    continue;
                }
                let child = match statement {
                    BoundStatement::Call {
                        routine_id,
                        arguments,
                        schema,
                    } => Some(prepare_routine_vm_frame(
                        state,
                        routine_id,
                        &arguments,
                        &request.parameters,
                        RoutineCompletion::Call { schema },
                        &routine_memory,
                    )),
                    BoundStatement::RoutineSelect {
                        routine_id,
                        arguments,
                        schema,
                        returns_set,
                    } => Some(prepare_routine_vm_frame(
                        state,
                        routine_id,
                        &arguments,
                        &request.parameters,
                        RoutineCompletion::Select {
                            schema,
                            returns_set,
                        },
                        &routine_memory,
                    )),
                    _ => None,
                };
                if let Some(child) = child {
                    match child {
                        Ok(child) => frames.push(child),
                        Err(error) => {
                            frames
                                .last_mut()
                                .ok_or_else(|| internal_error("PL/pgSQL parent frame is missing"))?
                                .response = Some(Err(error));
                        }
                    }
                    continue;
                }
                let (response, dirty) = {
                    let mut host = EnginePlpgsqlHost {
                        state,
                        trigger: None,
                        exception_states: Vec::new(),
                        exception_triggers: Vec::new(),
                        exception_charges: Vec::new(),
                        exception_memory: routine_memory.try_reserve(0)?,
                        sql_dirty: false,
                    };
                    let response = host.execute_sql(&request.sql, &request.parameters);
                    (response, host.sql_dirty)
                };
                segment_dirty |= dirty;
                frames
                    .last_mut()
                    .ok_or_else(|| internal_error("PL/pgSQL SQL frame is missing"))?
                    .response = Some(response);
            }
            VmRunState::Complete(output) => {
                let completed = frames
                    .pop()
                    .ok_or_else(|| internal_error("PL/pgSQL completed frame is missing"))?;
                state.routine_frames.pop(completed.id)?;
                let output = match finish_routine_output(&completed.routine, output) {
                    Ok(output) => output,
                    Err(error) => match completed.completion {
                        RoutineCompletion::Root => return Err(error),
                        RoutineCompletion::Call { .. } | RoutineCompletion::Select { .. } => {
                            frames
                                .last_mut()
                                .ok_or_else(|| internal_error("PL/pgSQL parent frame is missing"))?
                                .response = Some(Err(error));
                            continue;
                        }
                    },
                };
                match completed.completion {
                    RoutineCompletion::Root => return Ok((output, segment_dirty)),
                    completion => {
                        let events = routine_completion_events(completion, output);
                        frames
                            .last_mut()
                            .ok_or_else(|| internal_error("PL/pgSQL parent frame is missing"))?
                            .response = Some(Ok(events));
                    }
                }
            }
        }
    }
}

fn prepare_routine_vm_frame(
    state: &mut DatabaseState,
    routine_id: ordadb_types::RoutineId,
    arguments: &[BoundExpr],
    params: &[Value],
    completion: RoutineCompletion,
    memory: &VmMemoryGrant,
) -> Result<RoutineVmFrame> {
    let routine = state
        .catalog
        .routine_by_id(routine_id)
        .cloned()
        .ok_or_else(|| DbError::new("42883", "routine does not exist"))?;
    let program = compile_plpgsql(&routine.body, &routine_argument_names(&routine.arguments))?;
    let mut input_arguments = arguments.iter();
    let values = routine
        .arguments
        .iter()
        .map(|argument| {
            if argument.mode.accepts_input() {
                input_arguments
                    .next()
                    .ok_or_else(|| internal_error("routine input argument is missing"))
                    .and_then(|argument| evaluate_scalar(argument, &[], params))
            } else {
                Ok(Value::Null)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if input_arguments.next().is_some() {
        return Err(internal_error("routine received too many input arguments"));
    }
    let machine = {
        let mut host = EnginePlpgsqlHost {
            state,
            trigger: None,
            exception_states: Vec::new(),
            exception_triggers: Vec::new(),
            exception_charges: Vec::new(),
            exception_memory: memory.try_reserve(0)?,
            sql_dirty: false,
        };
        VmMachine::new_with_memory_grant(
            &program,
            &mut host,
            &values,
            ordadb_plpgsql::ResourceLimits::default(),
            memory.clone(),
        )?
    };
    let id = state.routine_frames.push_routine(routine_id)?;
    Ok(RoutineVmFrame {
        id,
        routine,
        machine,
        response: None,
        completion,
        exception_states: Vec::new(),
        exception_triggers: Vec::new(),
        exception_charges: Vec::new(),
        exception_memory: memory.try_reserve(0)?,
    })
}

fn finish_routine_output(routine: &RoutineDefinition, mut output: VmOutput) -> Result<VmOutput> {
    output.output_parameters = routine
        .arguments
        .iter()
        .enumerate()
        .filter(|(_, argument)| argument.mode.produces_output())
        .map(|(index, _)| {
            output
                .final_locals
                .get(index)
                .cloned()
                .ok_or_else(|| internal_error("routine output parameter local is missing"))
        })
        .collect::<Result<Vec<_>>>()?;
    if routine.return_type.is_none()
        && let [value] = output.output_parameters.as_slice()
    {
        output.return_value = Some(value.clone());
    }
    if let Some(return_type) = &routine.return_type {
        output.return_value = output
            .return_value
            .map(|value| coerce_execution_value(value, return_type))
            .transpose()?;
        output.returned_rows = output
            .returned_rows
            .into_iter()
            .map(|value| coerce_execution_value(value, return_type))
            .collect::<Result<Vec<_>>>()?;
    }
    output.refresh_retained_memory()?;
    Ok(output)
}

fn routine_completion_events(completion: RoutineCompletion, mut output: VmOutput) -> VmSqlStream {
    let memory = output.take_memory_hold();
    let events = match completion {
        RoutineCompletion::Root => Vec::new(),
        RoutineCompletion::Call { schema } => {
            let row_count = u64::from(!schema.fields.is_empty());
            let batch = (!schema.fields.is_empty()).then(|| Batch {
                schema: schema.clone(),
                rows: vec![Row::new(output.output_parameters)],
            });
            command_events(schema, "CALL", row_count, batch)
        }
        RoutineCompletion::Select {
            schema,
            returns_set,
        } => {
            let values = if returns_set {
                output.returned_rows
            } else {
                vec![output.return_value.unwrap_or(Value::Null)]
            };
            let row_count = values.len() as u64;
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
            ]
        }
    };
    Box::new(RoutineCompletionStream {
        events: events.into_iter(),
        _memory: memory,
    })
}

#[derive(Debug, Clone)]
struct TriggerRowContext {
    table: TableDefinition,
    old: Option<Row>,
    new: Option<Row>,
}

#[derive(Debug, Clone)]
struct TriggerRowSavepoint {
    old: Option<Row>,
    new: Option<Row>,
}

impl TriggerRowContext {
    fn value_and_type(&self, slot: usize, field: &str) -> Result<(Value, ScalarType)> {
        let row = match slot {
            0 => self.old.as_ref(),
            1 => self.new.as_ref(),
            _ => {
                return Err(DbError::new(
                    "42P02",
                    format!("trigger record parameter ${} does not exist", slot + 1),
                ));
            }
        };
        let field = Identifier::unquoted(field);
        let (column_index, data_type) = self
            .table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == field)
            .map(|(index, column)| (index, column.data_type.clone()))
            .ok_or_else(|| {
                DbError::new(
                    "42703",
                    format!("trigger record field {field} does not exist"),
                )
            })?;
        let value = match row {
            Some(row) => row.values.get(column_index).cloned().ok_or_else(|| {
                internal_error("trigger record width does not match its table definition")
            })?,
            None => Value::Null,
        };
        Ok((value, data_type))
    }

    fn value(&self, slot: usize, field: &str) -> Result<Value> {
        self.value_and_type(slot, field).map(|(value, _)| value)
    }

    fn assign(&mut self, slot: usize, field: &str, value: Value) -> Result<()> {
        if slot != 1 {
            return Err(DbError::new("25006", "OLD is read-only in a row trigger"));
        }
        let field = Identifier::unquoted(field);
        let (column_index, data_type) = self
            .table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == field)
            .map(|(index, column)| (index, column.data_type.clone()))
            .ok_or_else(|| {
                DbError::new(
                    "42703",
                    format!("trigger record field {field} does not exist"),
                )
            })?;
        let row = self
            .new
            .as_mut()
            .ok_or_else(|| DbError::new("55000", "NEW is not available for this trigger event"))?;
        let target = row.values.get_mut(column_index).ok_or_else(|| {
            internal_error("trigger record width does not match its table definition")
        })?;
        *target = coerce_execution_value(value, &data_type)?;
        Ok(())
    }
}

enum RowTriggerOutcome {
    Proceed(Option<Row>),
    Suppress,
}

fn trigger_argument_names() -> Vec<String> {
    [
        "old",
        "new",
        "tg_op",
        "tg_when",
        "tg_level",
        "tg_name",
        "tg_relid",
        "tg_table_schema",
        "tg_table_name",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[derive(Debug, Clone)]
struct TriggerRelation {
    target: TriggerTarget,
    schema_id: ordadb_types::SchemaId,
    name: Identifier,
    row_scope: TableDefinition,
}

fn trigger_relation(state: &DatabaseState, target: TriggerTarget) -> Result<TriggerRelation> {
    match target {
        TriggerTarget::Table(table_id) => {
            let table = table_definition(state, table_id)?.clone();
            Ok(TriggerRelation {
                target,
                schema_id: table.schema_id,
                name: table.name.clone(),
                row_scope: table,
            })
        }
        TriggerTarget::View(view_id) => {
            let view = state
                .catalog
                .view_by_id(view_id)
                .cloned()
                .ok_or_else(|| internal_error("trigger view does not exist"))?;
            Ok(TriggerRelation {
                target,
                schema_id: view.schema_id,
                name: view.name.clone(),
                row_scope: TableDefinition::expression_scope_for_schema(
                    view.name.clone(),
                    &view.output,
                )?,
            })
        }
    }
}

fn trigger_argument_values(
    state: &DatabaseState,
    relation: &TriggerRelation,
    trigger: &TriggerDefinition,
    timing: TriggerTiming,
    level: TriggerLevel,
    event: TriggerEvent,
) -> Result<Vec<Value>> {
    let operation = match event {
        TriggerEvent::Insert => "INSERT",
        TriggerEvent::Update => "UPDATE",
        TriggerEvent::Delete => "DELETE",
    };
    let when = match timing {
        TriggerTiming::Before | TriggerTiming::BeforeStatement => "BEFORE",
        TriggerTiming::After | TriggerTiming::AfterStatement => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    };
    let level = match level {
        TriggerLevel::Row => "ROW",
        TriggerLevel::Statement => "STATEMENT",
    };
    let schema = state
        .catalog
        .schema_by_id(relation.schema_id)
        .ok_or_else(|| internal_error("trigger relation schema does not exist"))?;
    let relation_oid = state.catalog.postgres_oid(match relation.target {
        TriggerTarget::Table(table_id) => PostgresOidObject::Table(table_id),
        TriggerTarget::View(view_id) => PostgresOidObject::View(view_id),
    })?;
    Ok(vec![
        Value::Null,
        Value::Null,
        Value::Text(operation.to_owned()),
        Value::Text(when.to_owned()),
        Value::Text(level.to_owned()),
        Value::Text(trigger.name.as_str().to_owned()),
        Value::Int64(i64::from(relation_oid.get())),
        Value::Text(schema.name.as_str().to_owned()),
        Value::Text(relation.name.as_str().to_owned()),
    ])
}

struct TriggerInvocation<'a> {
    timing: TriggerTiming,
    level: TriggerLevel,
    event: TriggerEvent,
    old: Option<&'a Row>,
    new: Option<&'a Row>,
}

fn execute_trigger(
    state: &mut DatabaseState,
    relation: &TriggerRelation,
    trigger_definition: &TriggerDefinition,
    invocation: TriggerInvocation<'_>,
) -> Result<(VmOutput, TriggerRowContext)> {
    if state.triggers_fired >= 16_384 {
        return Err(DbError::new("54001", "fired-trigger limit exceeded"));
    }
    let routine = state
        .catalog
        .routine_by_id(trigger_definition.routine_id)
        .cloned()
        .ok_or_else(|| DbError::new("42883", "trigger routine does not exist"))?;
    let program = compile_plpgsql(&routine.body, &trigger_argument_names())?;
    let parameters = trigger_argument_values(
        state,
        relation,
        trigger_definition,
        invocation.timing,
        invocation.level,
        invocation.event,
    )?;
    let frame = state.routine_frames.push_trigger(trigger_definition.id)?;
    state.triggers_fired += 1;
    let mut trigger = TriggerRowContext {
        table: relation.row_scope.clone(),
        old: invocation.old.cloned(),
        new: invocation.new.cloned(),
    };
    let limits = ordadb_plpgsql::ResourceLimits::default();
    let memory = VmMemoryGrant::new(limits.max_cursor_bytes)?;
    let result = {
        let mut host = EnginePlpgsqlHost {
            state,
            trigger: Some(&mut trigger),
            exception_states: Vec::new(),
            exception_triggers: Vec::new(),
            exception_charges: Vec::new(),
            exception_memory: memory.try_reserve(0)?,
            sql_dirty: false,
        };
        execute_plpgsql_with_memory(&program, &mut host, &parameters, limits, memory)
    };
    state.routine_frames.pop(frame)?;
    result.map(|output| (output, trigger))
}

fn fire_statement_triggers(
    state: &mut DatabaseState,
    table_id: TableId,
    timing: TriggerTiming,
    event: TriggerEvent,
) -> Result<bool> {
    let table = table_definition(state, table_id)?.clone();
    let relation = trigger_relation(state, TriggerTarget::Table(table_id))?;
    let mut triggers = table
        .triggers()
        .filter(|trigger| {
            trigger.enabled
                && trigger.level == TriggerLevel::Statement
                && trigger.timing == timing
                && trigger.events.contains(&event)
        })
        .cloned()
        .collect::<Vec<_>>();
    triggers.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    let fired = !triggers.is_empty();
    for trigger in triggers {
        let _ = execute_trigger(
            state,
            &relation,
            &trigger,
            TriggerInvocation {
                timing,
                level: TriggerLevel::Statement,
                event,
                old: None,
                new: None,
            },
        )?;
    }
    Ok(fired)
}

fn fire_row_triggers_with_rows(
    state: &mut DatabaseState,
    table_id: TableId,
    timing: TriggerTiming,
    event: TriggerEvent,
    old: Option<&Row>,
    new: Option<&Row>,
) -> Result<RowTriggerOutcome> {
    fire_relation_row_triggers_with_rows(
        state,
        TriggerTarget::Table(table_id),
        timing,
        event,
        old,
        new,
    )
}

fn fire_view_row_triggers_with_rows(
    state: &mut DatabaseState,
    view_id: ViewId,
    event: TriggerEvent,
    old: Option<&Row>,
    new: Option<&Row>,
) -> Result<RowTriggerOutcome> {
    fire_relation_row_triggers_with_rows(
        state,
        TriggerTarget::View(view_id),
        TriggerTiming::InsteadOf,
        event,
        old,
        new,
    )
}

fn fire_relation_row_triggers_with_rows(
    state: &mut DatabaseState,
    target: TriggerTarget,
    timing: TriggerTiming,
    event: TriggerEvent,
    old: Option<&Row>,
    new: Option<&Row>,
) -> Result<RowTriggerOutcome> {
    let relation = trigger_relation(state, target)?;
    let mut current_old = old.cloned();
    let mut current_new = new.cloned();
    let triggers = match target {
        TriggerTarget::Table(table_id) => state
            .catalog
            .table_by_id(table_id)
            .map(|table| table.triggers().cloned().collect::<Vec<_>>())
            .ok_or_else(|| internal_error("trigger table does not exist"))?,
        TriggerTarget::View(view_id) => state
            .catalog
            .view_by_id(view_id)
            .map(|view| view.triggers().cloned().collect::<Vec<_>>())
            .ok_or_else(|| internal_error("trigger view does not exist"))?,
    };
    if triggers.iter().any(|trigger| trigger.target != target) {
        return Err(DbError::new(
            "XX001",
            "trigger is stored under a different target relation",
        ));
    }
    let mut triggers = triggers
        .into_iter()
        .filter(|trigger| {
            trigger.enabled
                && trigger.level == TriggerLevel::Row
                && trigger.timing == timing
                && trigger.events.contains(&event)
        })
        .collect::<Vec<_>>();
    triggers.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    for trigger in triggers {
        let (output, trigger) = execute_trigger(
            state,
            &relation,
            &trigger,
            TriggerInvocation {
                timing,
                level: TriggerLevel::Row,
                event,
                old: current_old.as_ref(),
                new: current_new.as_ref(),
            },
        )?;
        if timing == TriggerTiming::After {
            continue;
        }
        match output.return_parameter {
            Some(parameter @ 0..=1) => {
                let returned = if parameter == 0 {
                    trigger.old
                } else {
                    trigger.new
                };
                let Some(returned) = returned else {
                    return Ok(RowTriggerOutcome::Suppress);
                };
                if event == TriggerEvent::Delete {
                    current_old = Some(returned);
                } else {
                    current_new = Some(returned);
                }
            }
            Some(parameter) => {
                return Err(DbError::new(
                    "42P02",
                    format!(
                        "trigger function returned unknown record parameter ${}",
                        parameter + 1
                    ),
                ));
            }
            None if output.return_value.is_none()
                || output.return_value.as_ref().is_some_and(Value::is_null) =>
            {
                return Ok(RowTriggerOutcome::Suppress);
            }
            None => {
                return Err(DbError::new(
                    "42804",
                    "row trigger functions must return OLD, NEW, or NULL",
                ));
            }
        }
    }
    Ok(RowTriggerOutcome::Proceed(
        if event == TriggerEvent::Delete {
            current_old
        } else {
            current_new
        },
    ))
}

fn execute_view_select(
    state: &mut DatabaseState,
    source: BoundStatement,
    schema: Schema,
    projection: Vec<usize>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let (mut events, dirty) = execute_bound(state, source, params)?;
    if dirty {
        return Err(internal_error(
            "a stored view query attempted to mutate state",
        ));
    }
    for event in &mut events {
        match event {
            QueryEvent::Schema(event_schema) => *event_schema = schema.clone(),
            QueryEvent::Batch(batch) => {
                for row in &mut batch.rows {
                    row.values = projection
                        .iter()
                        .map(|position| {
                            row.values.get(*position).cloned().ok_or_else(|| {
                                internal_error("stored view projection is outside its row width")
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                }
            }
            QueryEvent::Progress(_) | QueryEvent::Notice(_) | QueryEvent::Complete(_) => {}
        }
    }
    Ok((events, false))
}
