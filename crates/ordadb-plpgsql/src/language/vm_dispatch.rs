impl VmMachine {

    pub fn resume(
        &mut self,
        host: &mut impl PlpgsqlHost,
        response: Option<Result<VmSqlStream>>,
    ) -> Result<VmRunState> {
        let mut state = self
            .state
            .take()
            .ok_or_else(|| DbError::new("55000", "PL/pgSQL VM is already complete"))?;
        match (self.pending_sql.take(), response) {
            (Some(pending), Some(response)) => {
                if let Err(error) = state.apply_sql_response(pending, response) {
                    state.handle_error(host, error)?;
                }
            }
            (Some(_), None) => {
                return Err(DbError::internal(
                    "PL/pgSQL VM resumed without its pending SQL response",
                ));
            }
            (None, Some(_)) => {
                return Err(DbError::internal(
                    "PL/pgSQL VM received an unexpected SQL response",
                ));
            }
            (None, None) => {}
        }
        if let Err(error) = state.refresh_memory(None) {
            state.handle_error(host, error)?;
            state.refresh_memory(None)?;
        }
        let VmState {
            program,
            limits,
            mut locals,
            mut records,
            mut instruction_pointer,
            mut steps,
            mut returned_rows,
            mut query_loops,
            mut integer_loops,
            mut foreach_loops,
            mut cursors,
            mut active_exception,
            exception_regions,
            mut active_exception_regions,
            mut memory_reservation,
        } = state;
        while instruction_pointer < program.instructions.len() {
            query_loops.retain(|_, state| instruction_pointer < state.end);
            integer_loops.retain(|_, state| instruction_pointer < state.end);
            foreach_loops.retain(|_, state| instruction_pointer < state.end);
            while active_exception_regions
                .last()
                .is_some_and(|(_, end)| instruction_pointer >= *end)
            {
                host.commit_exception_block()?;
                active_exception_regions.pop();
            }
            for region in exception_regions
                .iter()
                .copied()
                .filter(|(start, _)| *start == instruction_pointer)
            {
                if !active_exception_regions.contains(&region) {
                    host.begin_exception_block()?;
                    active_exception_regions.push(region);
                }
            }
            steps = steps.saturating_add(1);
            if steps > limits.max_steps {
                return limit_error("PL/pgSQL execution step limit exceeded");
            }
            host.check_cancelled()?;
            let mut yielded_sql = None::<(PendingSql, VmSqlRequest)>;
            let step = (|| -> Result<Option<VmOutput>> {
                match &program.instructions[instruction_pointer] {
                    Instruction::Assign { slot, expression } => {
                        let already_assigned = records
                            .get(slot)
                            .map_or_else(|| !locals[*slot].is_null(), RuntimeRecord::is_assigned);
                        if program
                            .locals
                            .get(*slot)
                            .is_some_and(|local| local.constant && already_assigned)
                        {
                            return Err(DbError::new(
                                "22005",
                                format!(
                                    "constant {} cannot be reassigned",
                                    program.locals[*slot].name
                                ),
                            ));
                        }
                        if records.contains_key(slot) {
                            let source = positional_parameter_index(expression)
                                .and_then(|source| records.get(&source))
                                .cloned()
                                .ok_or_else(|| {
                                    DbError::new(
                                        "42804",
                                        "a record variable can only be assigned another record row",
                                    )
                                })?;
                            ensure_runtime_record_limit(
                                &records,
                                Some((*slot, &source)),
                                limits.max_cursor_bytes,
                            )?;
                            records.insert(*slot, source);
                        } else {
                            locals[*slot] =
                                evaluate_runtime_expression(host, expression, &locals, &records)?;
                        }
                        instruction_pointer += 1;
                    }
                    Instruction::AssignField {
                        slot,
                        field,
                        expression,
                    } => {
                        let value =
                            evaluate_runtime_expression(host, expression, &locals, &records)?;
                        if let Some(record) = records.get(slot) {
                            let mut candidate = record.clone();
                            candidate.assign_field(field, value)?;
                            ensure_runtime_record_limit(
                                &records,
                                Some((*slot, &candidate)),
                                limits.max_cursor_bytes,
                            )?;
                            records.insert(*slot, candidate);
                        } else {
                            host.assign_composite_field(*slot, field, value)?;
                        }
                        instruction_pointer += 1;
                    }
                    Instruction::JumpIfFalse { expression, target } => {
                        let value =
                            evaluate_runtime_expression(host, expression, &locals, &records)?;
                        instruction_pointer = if value == Value::Boolean(true) {
                            instruction_pointer + 1
                        } else {
                            checked_target(*target, program.instructions.len())?
                        };
                    }
                    Instruction::Jump { target } => {
                        instruction_pointer = checked_target(*target, program.instructions.len())?;
                    }
                    Instruction::ExecuteSql { sql, into } => {
                        let (sql, parameters) =
                            expand_runtime_record_fields(sql, &locals, &records)?;
                        yielded_sql = Some((
                            PendingSql::Execute { into: *into },
                            VmSqlRequest { sql, parameters },
                        ));
                    }
                    Instruction::DynamicExecute {
                        query,
                        using,
                        into,
                        strict,
                    } => {
                        let query = evaluate_runtime_expression(host, query, &locals, &records)?;
                        let Value::Text(query) = query else {
                            return Err(DbError::new(
                                "42804",
                                "dynamic EXECUTE query must evaluate to text",
                            ));
                        };
                        if query.len() > limits.max_dynamic_sql_bytes {
                            return limit_error("dynamic SQL exceeds the configured byte limit");
                        }
                        let parameters = using
                            .iter()
                            .map(|expression| {
                                evaluate_runtime_expression(host, expression, &locals, &records)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        yielded_sql = Some((
                            PendingSql::Dynamic {
                                into: *into,
                                strict: *strict,
                            },
                            VmSqlRequest {
                                sql: query,
                                parameters,
                            },
                        ));
                    }
                    Instruction::OpenCursor { cursor, query } => {
                        if cursors.contains_key(cursor) {
                            let name = program
                                .cursor_declarations
                                .get(*cursor)
                                .map_or("<unknown>", |cursor| cursor.name.as_str());
                            return Err(DbError::new(
                                "42P03",
                                format!("cursor {name} is already open"),
                            ));
                        }
                        if cursors.len() >= limits.max_open_cursors {
                            return Err(DbError::new(
                                "54000",
                                "PL/pgSQL open-cursor limit exceeded",
                            ));
                        }
                        let (sql, parameters) = match query {
                            CursorQuery::Bound => {
                                let sql = program
                                    .cursor_declarations
                                    .get(*cursor)
                                    .and_then(|cursor| cursor.bound_query.clone())
                                    .ok_or_else(|| {
                                        DbError::internal(
                                            "bound cursor query is missing from bytecode",
                                        )
                                    })?;
                                expand_runtime_record_fields(&sql, &locals, &records)?
                            }
                            CursorQuery::Static(query) => {
                                expand_runtime_record_fields(query, &locals, &records)?
                            }
                            CursorQuery::Dynamic { query, using } => {
                                let value =
                                    evaluate_runtime_expression(host, query, &locals, &records)?;
                                let Value::Text(query) = value else {
                                    return Err(DbError::new(
                                        "42804",
                                        "OPEN FOR EXECUTE query must evaluate to text",
                                    ));
                                };
                                if query.len() > limits.max_dynamic_sql_bytes {
                                    return limit_error(
                                        "OPEN FOR EXECUTE query exceeds the configured byte limit",
                                    );
                                }
                                let parameters = using
                                    .iter()
                                    .map(|expression| {
                                        evaluate_runtime_expression(
                                            host, expression, &locals, &records,
                                        )
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                (query, parameters)
                            }
                        };
                        yielded_sql = Some((
                            PendingSql::OpenCursor { cursor: *cursor },
                            VmSqlRequest { sql, parameters },
                        ));
                    }
                    Instruction::FetchCursor {
                        cursor,
                        direction,
                        into,
                    } => {
                        let direction =
                            evaluate_cursor_direction(host, direction, &locals, &records)?;
                        let state = cursors.get_mut(cursor).ok_or_else(|| {
                            let name = program
                                .cursor_declarations
                                .get(*cursor)
                                .map_or("<unknown>", |cursor| cursor.name.as_str());
                            DbError::new("34000", format!("cursor {name} is not open"))
                        })?;
                        let row = state.seek(direction, limits)?;
                        assign_runtime_row(
                            *into,
                            row,
                            &mut locals,
                            &mut records,
                            limits.max_cursor_bytes,
                        )?;
                        instruction_pointer += 1;
                    }
                    Instruction::MoveCursor { cursor, direction } => {
                        let direction =
                            evaluate_cursor_direction(host, direction, &locals, &records)?;
                        let state = cursors.get_mut(cursor).ok_or_else(|| {
                            let name = program
                                .cursor_declarations
                                .get(*cursor)
                                .map_or("<unknown>", |cursor| cursor.name.as_str());
                            DbError::new("34000", format!("cursor {name} is not open"))
                        })?;
                        state.seek(direction, limits)?;
                        instruction_pointer += 1;
                    }
                    Instruction::CloseCursor { cursor } => {
                        if cursors.remove(cursor).is_none() {
                            let name = program
                                .cursor_declarations
                                .get(*cursor)
                                .map_or("<unknown>", |cursor| cursor.name.as_str());
                            return Err(DbError::new(
                                "34000",
                                format!("cursor {name} is not open"),
                            ));
                        }
                        instruction_pointer += 1;
                    }
                    Instruction::Raise {
                        level,
                        message,
                        sql_state,
                    } => {
                        let Some(message) = message else {
                            return Err(active_exception.clone().ok_or_else(|| {
                                DbError::new(
                                    "0Z002",
                                    "RAISE without parameters is outside an exception handler",
                                )
                            })?);
                        };
                        let message = evaluate_message(host, message, &locals, &records, "RAISE")?;
                        let default_state = match level {
                            RaiseLevel::Info | RaiseLevel::Notice => "00000",
                            RaiseLevel::Warning => "01000",
                            RaiseLevel::Exception => "P0001",
                        };
                        let sql_state = sql_state.as_deref().unwrap_or(default_state);
                        match level {
                            RaiseLevel::Exception => {
                                return Err(DbError::new(sql_state, message));
                            }
                            RaiseLevel::Info | RaiseLevel::Notice | RaiseLevel::Warning => {
                                let severity = match level {
                                    RaiseLevel::Info => DbNoticeSeverity::Info,
                                    RaiseLevel::Notice => DbNoticeSeverity::Notice,
                                    RaiseLevel::Warning => DbNoticeSeverity::Warning,
                                    RaiseLevel::Exception => {
                                        return Err(DbError::internal(
                                            "exception raise reached the notice path",
                                        ));
                                    }
                                };
                                host.emit_notice(DbNotice {
                                    severity,
                                    sql_state: sql_state.to_owned(),
                                    message,
                                    detail: None,
                                    hint: None,
                                    position: None,
                                    object_identity: None,
                                })?;
                                instruction_pointer += 1;
                            }
                        }
                    }
                    Instruction::Assert { condition, message } => {
                        let condition =
                            evaluate_runtime_expression(host, condition, &locals, &records)?;
                        if condition == Value::Boolean(true) {
                            instruction_pointer += 1;
                        } else if condition == Value::Boolean(false) || condition.is_null() {
                            let message = message
                                .as_deref()
                                .map(|message| {
                                    evaluate_message(host, message, &locals, &records, "ASSERT")
                                })
                                .transpose()?
                                .unwrap_or_else(|| "assertion failed".to_owned());
                            return Err(DbError::new("P0004", message));
                        } else {
                            return Err(DbError::new(
                                "42804",
                                "ASSERT condition must evaluate to boolean",
                            ));
                        }
                    }
                    Instruction::QueryForStart { slot, sql, end } => {
                        let (sql, parameters) =
                            expand_runtime_record_fields(sql, &locals, &records)?;
                        yielded_sql = Some((
                            PendingSql::QueryForStart {
                                start: instruction_pointer,
                                slot: *slot,
                                end: checked_target(*end, program.instructions.len())?,
                            },
                            VmSqlRequest { sql, parameters },
                        ));
                    }
                    Instruction::QueryForNext { start, body } => {
                        let Some(state) = query_loops.get_mut(start) else {
                            return Err(DbError::internal(
                                "PL/pgSQL query FOR iterator state is missing",
                            ));
                        };
                        let slot = state.slot;
                        if let Some(row) = state.next_row(limits.max_returned_rows)? {
                            assign_runtime_row(
                                slot,
                                Some(row),
                                &mut locals,
                                &mut records,
                                limits.max_cursor_bytes,
                            )?;
                            instruction_pointer =
                                checked_target(*body, program.instructions.len())?;
                        } else {
                            query_loops.remove(start);
                            instruction_pointer += 1;
                        }
                    }
                    Instruction::IntegerForStart {
                        slot,
                        lower,
                        upper,
                        step,
                        reverse,
                        end,
                    } => {
                        let lower = evaluate_integer_expression(
                            host,
                            lower,
                            &locals,
                            &records,
                            "lower bound",
                        )?;
                        let upper = evaluate_integer_expression(
                            host,
                            upper,
                            &locals,
                            &records,
                            "upper bound",
                        )?;
                        let step =
                            evaluate_integer_expression(host, step, &locals, &records, "BY value")?;
                        if step <= 0 {
                            return Err(DbError::new(
                                "22023",
                                "BY value of PL/pgSQL integer FOR loop must be greater than zero",
                            ));
                        }
                        let has_first = if *reverse {
                            lower >= upper
                        } else {
                            lower <= upper
                        };
                        if has_first {
                            locals[*slot] = Value::Int64(lower);
                            integer_loops.insert(
                                instruction_pointer,
                                IntegerLoopState {
                                    slot: *slot,
                                    current: lower,
                                    bound: upper,
                                    step,
                                    reverse: *reverse,
                                    end: checked_target(*end, program.instructions.len())?,
                                },
                            );
                            instruction_pointer += 1;
                        } else {
                            instruction_pointer = checked_target(*end, program.instructions.len())?;
                        }
                    }
                    Instruction::IntegerForNext { start, body } => {
                        let Some(state) = integer_loops.get_mut(start) else {
                            return Err(DbError::internal(
                                "PL/pgSQL integer FOR iterator state is missing",
                            ));
                        };
                        if let Some(value) = state.advance()? {
                            locals[state.slot] = value;
                            instruction_pointer =
                                checked_target(*body, program.instructions.len())?;
                        } else {
                            integer_loops.remove(start);
                            instruction_pointer += 1;
                        }
                    }
                    Instruction::ForeachStart { slot, array, end } => {
                        let array = evaluate_runtime_expression(host, array, &locals, &records)?;
                        let Value::Array(array) = array else {
                            if array.is_null() {
                                return Err(DbError::new(
                                    "22004",
                                    "FOREACH expression must not be NULL",
                                ));
                            }
                            return Err(DbError::new(
                                "42804",
                                "FOREACH expression must evaluate to an array",
                            ));
                        };
                        let mut values = VecDeque::from(array.into_values());
                        if let Some(value) = values.pop_front() {
                            locals[*slot] = value;
                            foreach_loops.insert(
                                instruction_pointer,
                                ForeachLoopState {
                                    slot: *slot,
                                    values,
                                    end: checked_target(*end, program.instructions.len())?,
                                },
                            );
                            instruction_pointer += 1;
                        } else {
                            instruction_pointer = checked_target(*end, program.instructions.len())?;
                        }
                    }
                    Instruction::ForeachNext { start, body } => {
                        let Some(state) = foreach_loops.get_mut(start) else {
                            return Err(DbError::internal(
                                "PL/pgSQL FOREACH iterator state is missing",
                            ));
                        };
                        if let Some(value) = state.values.pop_front() {
                            locals[state.slot] = value;
                            instruction_pointer =
                                checked_target(*body, program.instructions.len())?;
                        } else {
                            foreach_loops.remove(start);
                            instruction_pointer += 1;
                        }
                    }
                    Instruction::Return { expression, next } => {
                        let value = expression
                            .as_ref()
                            .map(|expression| {
                                evaluate_runtime_expression(host, expression, &locals, &records)
                            })
                            .transpose()?
                            .unwrap_or(Value::Null);
                        if *next {
                            returned_rows.push(value);
                            if returned_rows.len() > limits.max_returned_rows {
                                return limit_error("PL/pgSQL returned-row limit exceeded");
                            }
                            instruction_pointer += 1;
                        } else {
                            return Ok(Some(VmOutput {
                                return_value: Some(value),
                                returned_rows: std::mem::take(&mut returned_rows),
                                return_parameter: expression
                                    .as_deref()
                                    .and_then(positional_parameter_index),
                                final_locals: locals.clone(),
                                output_parameters: Vec::new(),
                                retained_memory: None,
                            }));
                        }
                    }
                    Instruction::Checkpoint => {
                        instruction_pointer += 1;
                    }
                }
                Ok(None)
            })();
            let step = step.and_then(|output| {
                let bytes = if let Some(output) = &output {
                    estimated_vm_output_bytes(output)?
                } else {
                    estimated_vm_runtime_bytes(
                        &program,
                        &locals,
                        &records,
                        &returned_rows,
                        &query_loops,
                        &integer_loops,
                        &foreach_loops,
                        &cursors,
                        active_exception.as_ref(),
                        &exception_regions,
                        &active_exception_regions,
                        yielded_sql.as_ref().map(|(_, request)| request),
                    )?
                };
                memory_reservation.resize(bytes)?;
                Ok(output)
            });
            match step {
                Ok(Some(output)) => {
                    while active_exception_regions.pop().is_some() {
                        host.commit_exception_block()?;
                    }
                    return Ok(VmRunState::Complete(attach_output_memory(
                        output,
                        memory_reservation,
                    )));
                }
                Ok(None) => {
                    if let Some((pending, request)) = yielded_sql {
                        self.state = Some(VmState {
                            program,
                            limits,
                            locals,
                            records,
                            instruction_pointer,
                            steps,
                            returned_rows,
                            query_loops,
                            integer_loops,
                            foreach_loops,
                            cursors,
                            active_exception,
                            exception_regions,
                            active_exception_regions,
                            memory_reservation,
                        });
                        self.pending_sql = Some(pending);
                        return Ok(VmRunState::Sql(request));
                    }
                }
                Err(error) => {
                    handle_runtime_step_error(
                        host,
                        &program,
                        &mut instruction_pointer,
                        error,
                        &mut locals,
                        &records,
                        &returned_rows,
                        &mut query_loops,
                        &mut integer_loops,
                        &mut foreach_loops,
                        &mut cursors,
                        &mut active_exception,
                        &exception_regions,
                        &mut active_exception_regions,
                        &mut memory_reservation,
                    )?;
                }
            }
        }
        while active_exception_regions.pop().is_some() {
            host.commit_exception_block()?;
        }
        let output = VmOutput {
            return_value: None,
            returned_rows,
            return_parameter: None,
            final_locals: locals,
            output_parameters: Vec::new(),
            retained_memory: None,
        };
        memory_reservation.resize(estimated_vm_output_bytes(&output)?)?;
        Ok(VmRunState::Complete(attach_output_memory(
            output,
            memory_reservation,
        )))
    }
}
