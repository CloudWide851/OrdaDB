impl VmMachine {

    pub fn ensure_transaction_boundary_ready(&self) -> Result<()> {
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| DbError::new("55000", "PL/pgSQL VM is already complete"))?;
        if !state.active_exception_regions.is_empty()
            || !state.query_loops.is_empty()
            || !state.cursors.is_empty()
        {
            return Err(DbError::new(
                "2D000",
                "cannot end a transaction while a PL/pgSQL subtransaction or cursor is active",
            )
            .with_hint(
                "close active cursors and leave exception blocks before transaction control",
            ));
        }
        Ok(())
    }
}

impl VmState {
    fn refresh_memory(&mut self, pending_request: Option<&VmSqlRequest>) -> Result<()> {
        let bytes = estimated_vm_runtime_bytes(
            &self.program,
            &self.locals,
            &self.records,
            &self.returned_rows,
            &self.query_loops,
            &self.integer_loops,
            &self.foreach_loops,
            &self.cursors,
            self.active_exception.as_ref(),
            &self.exception_regions,
            &self.active_exception_regions,
            pending_request,
        )?;
        self.memory_reservation.resize(bytes)
    }

    fn apply_sql_response(
        &mut self,
        pending: PendingSql,
        response: Result<VmSqlStream>,
    ) -> Result<()> {
        let events = response?;
        match pending {
            PendingSql::Execute { into } => {
                let (first, _) = collect_sql_result(events, false)?;
                if let Some(slot) = into {
                    assign_runtime_row(
                        slot,
                        first,
                        &mut self.locals,
                        &mut self.records,
                        self.limits.max_cursor_bytes,
                    )?;
                }
                self.instruction_pointer += 1;
            }
            PendingSql::Dynamic { into, strict } => {
                let (first, row_count) = collect_sql_result(events, strict)?;
                if let Some(slot) = into {
                    if strict && row_count == 0 {
                        return Err(DbError::new(
                            "P0002",
                            "dynamic EXECUTE INTO STRICT returned no rows",
                        ));
                    }
                    assign_runtime_row(
                        slot,
                        first,
                        &mut self.locals,
                        &mut self.records,
                        self.limits.max_cursor_bytes,
                    )?;
                }
                self.instruction_pointer += 1;
            }
            PendingSql::OpenCursor { cursor } => {
                self.cursors.insert(cursor, CursorState::new(events));
                self.instruction_pointer += 1;
            }
            PendingSql::QueryForStart { start, slot, end } => {
                let mut state = QueryLoopState {
                    slot,
                    end,
                    events,
                    current_rows: VecDeque::new(),
                    fields: None,
                    rows_seen: 0,
                };
                if let Some(row) = state.next_row(self.limits.max_returned_rows)? {
                    assign_runtime_row(
                        slot,
                        Some(row),
                        &mut self.locals,
                        &mut self.records,
                        self.limits.max_cursor_bytes,
                    )?;
                    self.query_loops.insert(start, state);
                    self.instruction_pointer += 1;
                } else {
                    self.instruction_pointer = end;
                }
            }
        }
        Ok(())
    }

    fn handle_error(&mut self, host: &mut impl PlpgsqlHost, error: DbError) -> Result<()> {
        let handler = self
            .program
            .exception_handlers
            .iter()
            .enumerate()
            .filter(|(_, handler)| {
                handler.protected_start <= self.instruction_pointer
                    && self.instruction_pointer < handler.protected_end
                    && match &handler.matcher {
                        ExceptionMatcher::SqlState(state) => {
                            state.eq_ignore_ascii_case(&error.sql_state)
                        }
                        ExceptionMatcher::Others => {
                            !matches!(error.sql_state.as_str(), "57014" | "P0004")
                        }
                    }
            })
            .max_by_key(|(index, handler)| {
                (
                    handler.protected_start,
                    usize::MAX.saturating_sub(handler.protected_end),
                    usize::MAX.saturating_sub(*index),
                )
            })
            .map(|(_, handler)| {
                (
                    handler.protected_start,
                    handler.protected_end,
                    handler.target,
                )
            });
        let Some((protected_start, protected_end, handler_target)) = handler else {
            while self.active_exception_regions.pop().is_some() {
                host.rollback_exception_block()?;
            }
            return Err(error);
        };
        let selected_region = (protected_start, protected_end);
        let mut selected_rolled_back = false;
        while let Some(region) = self.active_exception_regions.pop() {
            host.rollback_exception_block()?;
            if region == selected_region {
                selected_rolled_back = true;
                break;
            }
        }
        if !selected_rolled_back {
            return Err(DbError::internal(
                "PL/pgSQL exception handler region was not active",
            ));
        }
        self.query_loops.clear();
        self.integer_loops.clear();
        self.foreach_loops.clear();
        self.cursors.clear();
        if let Some(slot) = self.program.sqlstate_slot {
            self.locals[slot] = Value::Text(error.sql_state.clone());
        }
        if let Some(slot) = self.program.sqlerrm_slot {
            self.locals[slot] = Value::Text(error.message.clone());
        }
        self.active_exception = Some(error);
        self.instruction_pointer = checked_target(handler_target, self.program.instructions.len())?;
        Ok(())
    }
}

fn collect_sql_result(
    events: VmSqlStream,
    strict: bool,
) -> Result<(Option<RuntimeQueryRow>, usize)> {
    let mut fields = None::<Vec<String>>;
    let mut first = None::<RuntimeQueryRow>;
    let mut row_count = 0_usize;
    for event in events {
        match event? {
            QueryEvent::Schema(schema) => {
                fields = Some(schema.fields.into_iter().map(|field| field.name).collect());
            }
            QueryEvent::Batch(batch) => {
                let row_fields = fields.get_or_insert_with(|| {
                    batch
                        .schema
                        .fields
                        .iter()
                        .map(|field| field.name.clone())
                        .collect()
                });
                for row in batch.rows {
                    row_count = row_count.saturating_add(1);
                    if first.is_none() {
                        first = Some(RuntimeQueryRow {
                            fields: row_fields.clone(),
                            row,
                        });
                    }
                    if strict && row_count > 1 {
                        return Err(DbError::new(
                            "P0003",
                            "dynamic EXECUTE INTO STRICT returned more than one row",
                        ));
                    }
                }
            }
            QueryEvent::Progress(_) | QueryEvent::Notice(_) | QueryEvent::Complete(_) => {}
        }
    }
    Ok((first, row_count))
}

fn evaluate_integer_expression(
    host: &mut impl PlpgsqlHost,
    expression: &str,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
    context: &str,
) -> Result<i64> {
    match evaluate_runtime_expression(host, expression, locals, records)? {
        Value::Int16(value) => Ok(i64::from(value)),
        Value::Int32(value) => Ok(i64::from(value)),
        Value::Int64(value) => Ok(value),
        _ => Err(DbError::new(
            "42804",
            format!("PL/pgSQL integer FOR {context} must evaluate to an integer"),
        )),
    }
}

fn evaluate_cursor_direction(
    host: &mut impl PlpgsqlHost,
    direction: &CursorDirection,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
) -> Result<EvaluatedCursorDirection> {
    match direction {
        CursorDirection::Next => Ok(EvaluatedCursorDirection::Next),
        CursorDirection::Prior => Ok(EvaluatedCursorDirection::Prior),
        CursorDirection::First => Ok(EvaluatedCursorDirection::First),
        CursorDirection::Last => Ok(EvaluatedCursorDirection::Last),
        CursorDirection::Absolute(expression) => Ok(EvaluatedCursorDirection::Absolute(
            evaluate_cursor_integer_expression(host, expression, locals, records)?,
        )),
        CursorDirection::Relative(expression) => Ok(EvaluatedCursorDirection::Relative(
            evaluate_cursor_integer_expression(host, expression, locals, records)?,
        )),
        CursorDirection::Forward(expression) => Ok(EvaluatedCursorDirection::Forward(
            evaluate_cursor_count(host, expression.as_deref(), locals, records, "FORWARD")?,
        )),
        CursorDirection::ForwardAll => Ok(EvaluatedCursorDirection::ForwardAll),
        CursorDirection::Backward(expression) => Ok(EvaluatedCursorDirection::Backward(
            evaluate_cursor_count(host, expression.as_deref(), locals, records, "BACKWARD")?,
        )),
        CursorDirection::BackwardAll => Ok(EvaluatedCursorDirection::BackwardAll),
    }
}

fn evaluate_cursor_count(
    host: &mut impl PlpgsqlHost,
    expression: Option<&str>,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
    direction: &str,
) -> Result<i64> {
    let count = expression.map_or(Ok(1), |expression| {
        evaluate_cursor_integer_expression(host, expression, locals, records)
    })?;
    if count < 0 {
        return Err(DbError::new(
            "22023",
            format!("cursor {direction} count must not be negative"),
        ));
    }
    Ok(count)
}

fn evaluate_cursor_integer_expression(
    host: &mut impl PlpgsqlHost,
    expression: &str,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
) -> Result<i64> {
    match evaluate_runtime_expression(host, expression, locals, records)? {
        Value::Int16(value) => Ok(i64::from(value)),
        Value::Int32(value) => Ok(i64::from(value)),
        Value::Int64(value) => Ok(value),
        _ => Err(DbError::new(
            "42804",
            "cursor direction value must evaluate to an integer",
        )),
    }
}

fn evaluate_message(
    host: &mut impl PlpgsqlHost,
    expression: &str,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
    statement: &str,
) -> Result<String> {
    match evaluate_runtime_expression(host, expression, locals, records)? {
        Value::Text(message) => Ok(message),
        Value::Null => Ok(String::new()),
        _ => Err(DbError::new(
            "42804",
            format!("{statement} message must evaluate to text"),
        )),
    }
}

fn positional_parameter_index(expression: &str) -> Option<usize> {
    expression
        .trim()
        .strip_prefix('$')?
        .parse::<usize>()
        .ok()?
        .checked_sub(1)
}

fn logical_lines(source: &str) -> Result<Vec<String>> {
    let mut lines = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut parenthesis_depth = 0_usize;
    let mut characters = source.chars().peekable();
    while let Some(character) = characters.next() {
        match quote {
            Some(delimiter) if character == delimiter => {
                current.push(character);
                if characters.peek() == Some(&delimiter) {
                    current.push(characters.next().unwrap_or(delimiter));
                } else {
                    quote = None;
                }
            }
            Some(_) => current.push(character),
            None if matches!(character, '\'' | '"') => {
                quote = Some(character);
                current.push(character);
            }
            None if character == '(' => {
                parenthesis_depth = parenthesis_depth.saturating_add(1);
                current.push(character);
            }
            None if character == ')' => {
                parenthesis_depth = parenthesis_depth.checked_sub(1).ok_or_else(|| {
                    DbError::new(
                        "42601",
                        "PL/pgSQL source has an unmatched closing parenthesis",
                    )
                })?;
                current.push(character);
            }
            None if matches!(character, ';' | '\n' | '\r') && parenthesis_depth == 0 => {
                push_logical_segment(&mut lines, &current);
                current.clear();
            }
            None if matches!(character, '\n' | '\r') => current.push(' '),
            None => current.push(character),
        }
    }
    if quote.is_some() {
        return syntax_error("unterminated quoted string in PL/pgSQL source");
    }
    if parenthesis_depth != 0 {
        return syntax_error("PL/pgSQL source has an unmatched opening parenthesis");
    }
    push_logical_segment(&mut lines, &current);
    Ok(lines)
}

fn push_logical_segment(lines: &mut Vec<String>, segment: &str) {
    let trimmed = segment.trim();
    if trimmed.is_empty() {
        return;
    }
    for keyword in ["DECLARE", "BEGIN"] {
        if let Some(rest) = strip_leading_keyword(trimmed, keyword) {
            lines.push(keyword.to_owned());
            if !rest.is_empty() {
                lines.push(rest.to_owned());
            }
            return;
        }
    }
    lines.push(trimmed.to_owned());
}

fn strip_leading_keyword<'a>(value: &'a str, keyword: &str) -> Option<&'a str> {
    let prefix = value.get(..keyword.len())?;
    if !prefix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = value.get(keyword.len()..)?;
    rest.chars()
        .next()
        .is_some_and(char::is_whitespace)
        .then(|| rest.trim_start())
}

fn rewrite_locals(expression: &str, locals: &BTreeMap<String, usize>) -> String {
    let mut output = String::with_capacity(expression.len());
    let mut identifier = String::new();
    let mut quote = None;
    let flush = |output: &mut String, identifier: &mut String| {
        if identifier.is_empty() {
            return;
        }
        if let Some(slot) = locals.get(&identifier.to_ascii_lowercase()) {
            output.push('$');
            output.push_str(&(slot + 1).to_string());
        } else {
            output.push_str(identifier);
        }
        identifier.clear();
    };
    for character in expression.chars() {
        match quote {
            Some(delimiter) => {
                output.push(character);
                if character == delimiter {
                    quote = None;
                }
            }
            None if matches!(character, '\'' | '"') => {
                flush(&mut output, &mut identifier);
                quote = Some(character);
                output.push(character);
            }
            None if character.is_ascii_alphanumeric() || character == '_' => {
                identifier.push(character);
            }
            None => {
                flush(&mut output, &mut identifier);
                output.push(character);
            }
        }
    }
    flush(&mut output, &mut identifier);
    output
}

fn split_keyword<'a>(value: &'a str, keyword: &str) -> (&'a str, Option<&'a str>) {
    let bytes = value.as_bytes();
    let keyword = keyword.as_bytes();
    let mut quote = None;
    let mut depth = 0_usize;
    let mut index = 0_usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(delimiter) = quote {
            if byte == delimiter {
                if bytes.get(index + 1) == Some(&delimiter) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'\'' | b'"' => quote = Some(byte),
            b'(' => depth = depth.saturating_add(1),
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        let end = index.saturating_add(keyword.len());
        if depth == 0
            && index > 0
            && end < bytes.len()
            && bytes[index - 1].is_ascii_whitespace()
            && bytes[end].is_ascii_whitespace()
            && bytes[index..end].eq_ignore_ascii_case(keyword)
        {
            return (value[..index].trim_end(), Some(value[end..].trim_start()));
        }
        index += 1;
    }
    (value, None)
}

fn parse_exception_matcher(value: &str) -> Result<ExceptionMatcher> {
    if value.eq_ignore_ascii_case("OTHERS") {
        return Ok(ExceptionMatcher::Others);
    }
    if let Some(state) = value
        .get(8..)
        .filter(|_| {
            value
                .get(.."SQLSTATE".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SQLSTATE"))
        })
        .map(str::trim)
        .and_then(|value| value.strip_prefix('\''))
        .and_then(|value| value.strip_suffix('\''))
    {
        validate_sql_state(state)?;
        return Ok(ExceptionMatcher::SqlState(state.to_ascii_uppercase()));
    }
    let state = match value.trim().to_ascii_lowercase().as_str() {
        "unique_violation" => "23505",
        "division_by_zero" => "22012",
        "null_value_not_allowed" => "22004",
        "no_data_found" => "P0002",
        "too_many_rows" => "P0003",
        "assert_failure" => "P0004",
        "raise_exception" => "P0001",
        _ => {
            return Err(DbError::new(
                "42704",
                format!("unrecognized exception condition {value}"),
            ));
        }
    };
    Ok(ExceptionMatcher::SqlState(state.to_owned()))
}

fn validate_sql_state(state: &str) -> Result<()> {
    if state.len() != 5 || !state.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return syntax_error("SQLSTATE must contain five ASCII letters or digits");
    }
    if state.starts_with("00") {
        return syntax_error("SQLSTATE class 00 cannot be raised as an error");
    }
    Ok(())
}

fn parse_label(value: &str) -> Result<Option<String>> {
    if !value.starts_with("<<") && !value.ends_with(">>") {
        return Ok(None);
    }
    let label = value
        .strip_prefix("<<")
        .and_then(|value| value.strip_suffix(">>"))
        .ok_or_else(|| DbError::new("42601", "PL/pgSQL label is malformed"))?;
    normalize_label(label).map(Some)
}

fn normalize_label(label: &str) -> Result<String> {
    let label = label.trim();
    let mut characters = label.chars();
    if label.is_empty()
        || label.len() > 63
        || !characters
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        || !characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return syntax_error("PL/pgSQL label must be a bounded unquoted identifier");
    }
    Ok(label.to_ascii_lowercase())
}

fn parse_block_end_label(value: &str) -> Result<Option<Option<String>>> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("END") {
        return Ok(Some(None));
    }
    let Some(tail) = value
        .get("END".len()..)
        .filter(|_| {
            value
                .get(.."END".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("END"))
        })
        .map(str::trim)
        .filter(|tail| !tail.is_empty())
    else {
        return Ok(None);
    };
    if tail.split_whitespace().next().is_some_and(|word| {
        ["IF", "CASE", "LOOP"]
            .iter()
            .any(|keyword| word.eq_ignore_ascii_case(keyword))
    }) {
        return Ok(None);
    }
    normalize_label(tail).map(|label| Some(Some(label)))
}

fn parse_loop_control<'a>(
    statement: &'a str,
    keyword: &str,
) -> Result<(Option<String>, Option<&'a str>)> {
    let rest = statement
        .get(keyword.len()..)
        .ok_or_else(|| DbError::internal("loop-control keyword length is invalid"))?
        .trim();
    if rest.is_empty() {
        return Ok((None, None));
    }
    let mut parts = rest.splitn(2, char::is_whitespace);
    let first = parts.next().unwrap_or_default();
    let tail = parts.next().map(str::trim).unwrap_or_default();
    if first.eq_ignore_ascii_case("WHEN") {
        return Ok((None, Some(tail)));
    }
    let label = normalize_label(first)?;
    if tail.is_empty() {
        return Ok((Some(label), None));
    }
    let condition = tail
        .get("WHEN".len()..)
        .filter(|_| {
            tail.get(.."WHEN".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("WHEN"))
        })
        .map(str::trim)
        .ok_or_else(|| {
            DbError::new(
                "42601",
                format!("{keyword} label may only be followed by WHEN"),
            )
        })?;
    Ok((Some(label), Some(condition)))
}

fn loop_control_target_mut<'a>(
    controls: &'a mut [ControlFrame],
    label: Option<&str>,
) -> Result<&'a mut ControlFrame> {
    for frame in controls.iter_mut().rev() {
        if let ControlFrame::Loop {
            label: frame_label, ..
        } = frame
            && label.is_none_or(|label| frame_label.as_deref() == Some(label))
        {
            return Ok(frame);
        }
    }
    match label {
        Some(label) => syntax_error(format!("loop label {label} does not exist")),
        None => syntax_error("loop control statement is outside a loop"),
    }
}

fn patch_query_for_end(
    instructions: &mut [Instruction],
    instruction: usize,
    target: usize,
) -> Result<()> {
    match instructions.get_mut(instruction) {
        Some(Instruction::QueryForStart { end, .. }) => {
            *end = target;
            Ok(())
        }
        _ => Err(DbError::internal(
            "PL/pgSQL compiler query FOR patch target is invalid",
        )),
    }
}

fn patch_integer_for_end(
    instructions: &mut [Instruction],
    instruction: usize,
    target: usize,
) -> Result<()> {
    match instructions.get_mut(instruction) {
        Some(Instruction::IntegerForStart { end, .. }) => {
            *end = target;
            Ok(())
        }
        _ => Err(DbError::internal(
            "PL/pgSQL compiler integer FOR patch target is invalid",
        )),
    }
}

fn patch_foreach_end(
    instructions: &mut [Instruction],
    instruction: usize,
    target: usize,
) -> Result<()> {
    match instructions.get_mut(instruction) {
        Some(Instruction::ForeachStart { end, .. }) => {
            *end = target;
            Ok(())
        }
        _ => Err(DbError::internal(
            "PL/pgSQL compiler FOREACH patch target is invalid",
        )),
    }
}

fn patch_target(
    instructions: &mut [Instruction],
    instruction: Option<usize>,
    target: usize,
) -> Result<()> {
    let Some(instruction) = instruction else {
        return Ok(());
    };
    match instructions.get_mut(instruction) {
        Some(Instruction::JumpIfFalse {
            target: destination,
            ..
        })
        | Some(Instruction::Jump {
            target: destination,
        }) => {
            *destination = target;
            Ok(())
        }
        _ => Err(DbError::internal(
            "PL/pgSQL compiler patch target is not a jump",
        )),
    }
}

fn checked_target(target: usize, instruction_count: usize) -> Result<usize> {
    if target <= instruction_count {
        Ok(target)
    } else {
        Err(DbError::internal(
            "PL/pgSQL bytecode jump target is outside the program",
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_runtime_step_error(
    host: &mut impl PlpgsqlHost,
    program: &Program,
    instruction_pointer: &mut usize,
    error: DbError,
    locals: &mut Vec<Value>,
    records: &BTreeMap<usize, RuntimeRecord>,
    returned_rows: &Vec<Value>,
    query_loops: &mut BTreeMap<usize, QueryLoopState>,
    integer_loops: &mut BTreeMap<usize, IntegerLoopState>,
    foreach_loops: &mut BTreeMap<usize, ForeachLoopState>,
    cursors: &mut BTreeMap<usize, CursorState>,
    active_exception: &mut Option<DbError>,
    exception_regions: &Vec<(usize, usize)>,
    active_exception_regions: &mut Vec<(usize, usize)>,
    memory_reservation: &mut VmMemoryReservation,
) -> Result<()> {
    let handler = program
        .exception_handlers
        .iter()
        .enumerate()
        .filter(|(_, handler)| {
            handler.protected_start <= *instruction_pointer
                && *instruction_pointer < handler.protected_end
                && match &handler.matcher {
                    ExceptionMatcher::SqlState(state) => {
                        state.eq_ignore_ascii_case(&error.sql_state)
                    }
                    ExceptionMatcher::Others => {
                        !matches!(error.sql_state.as_str(), "57014" | "P0004")
                    }
                }
        })
        .max_by_key(|(index, handler)| {
            (
                handler.protected_start,
                usize::MAX.saturating_sub(handler.protected_end),
                usize::MAX.saturating_sub(*index),
            )
        })
        .map(|(_, handler)| {
            (
                handler.protected_start,
                handler.protected_end,
                handler.target,
            )
        });
    let Some((protected_start, protected_end, handler_target)) = handler else {
        while active_exception_regions.pop().is_some() {
            host.rollback_exception_block()?;
        }
        return Err(error);
    };
    let selected_region = (protected_start, protected_end);
    let mut selected_rolled_back = false;
    while let Some(region) = active_exception_regions.pop() {
        host.rollback_exception_block()?;
        if region == selected_region {
            selected_rolled_back = true;
            break;
        }
    }
    if !selected_rolled_back {
        return Err(DbError::internal(
            "PL/pgSQL exception handler region was not active",
        ));
    }
    query_loops.clear();
    integer_loops.clear();
    foreach_loops.clear();
    cursors.clear();
    if let Some(slot) = program.sqlstate_slot {
        locals[slot] = Value::Text(error.sql_state.clone());
    }
    if let Some(slot) = program.sqlerrm_slot {
        locals[slot] = Value::Text(error.message.clone());
    }
    *active_exception = Some(error);
    *instruction_pointer = checked_target(handler_target, program.instructions.len())?;
    let bytes = estimated_vm_runtime_bytes(
        program,
        locals,
        records,
        returned_rows,
        query_loops,
        integer_loops,
        foreach_loops,
        cursors,
        active_exception.as_ref(),
        exception_regions,
        active_exception_regions,
        None,
    )?;
    memory_reservation.resize(bytes)
}

fn ensure_nesting<T>(controls: &[T], limits: ResourceLimits) -> Result<()> {
    if controls.len() >= limits.max_nesting {
        limit_error("PL/pgSQL nesting exceeds the configured limit")
    } else {
        Ok(())
    }
}

fn ensure_instruction_limit(instructions: &[Instruction], limits: ResourceLimits) -> Result<()> {
    if instructions.len() > limits.max_instructions {
        limit_error("PL/pgSQL bytecode exceeds the configured instruction limit")
    } else {
        Ok(())
    }
}

fn syntax_error<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::new("42601", message))
}

fn unsupported_feature<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::new("0A000", message))
}

fn limit_error<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::new("54001", message))
}

#[cfg(test)]
mod tests {
    include!("tests_runtime.rs");
    include!("tests_limits.rs");
}
