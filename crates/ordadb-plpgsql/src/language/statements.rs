
fn compile_cursor_declaration(
    declaration: &str,
    locals: &mut Vec<LocalSlot>,
    names: &mut BTreeMap<String, usize>,
    cursor_declarations: &mut Vec<CursorDeclaration>,
    cursor_names: &mut BTreeMap<String, usize>,
    scope: &mut DeclarationScope,
) -> Result<bool> {
    let (cursor_head, bound_query) = split_keyword(declaration, "CURSOR FOR");
    let is_refcursor = declaration
        .split_whitespace()
        .any(|part| part.eq_ignore_ascii_case("REFCURSOR"));
    if bound_query.is_none() && !is_refcursor {
        return Ok(false);
    }
    if declaration.contains(":=") {
        return unsupported_feature("cursor declaration initializers are not supported");
    }
    let mut parts = cursor_head.split_whitespace();
    let name = parts
        .next()
        .ok_or_else(|| DbError::new("42601", "cursor declaration requires a name"))?;
    if name.contains(['(', ')']) {
        return unsupported_feature("cursor declaration arguments are not supported");
    }
    let modifiers = parts.collect::<Vec<_>>();
    if let Some(query) = bound_query {
        let modifiers_valid = modifiers.is_empty()
            || (modifiers.len() == 1 && modifiers[0].eq_ignore_ascii_case("SCROLL"))
            || (modifiers.len() == 2
                && modifiers[0].eq_ignore_ascii_case("NO")
                && modifiers[1].eq_ignore_ascii_case("SCROLL"));
        if !modifiers_valid {
            return syntax_error("bound cursor declaration accepts only SCROLL or NO SCROLL");
        }
        if query.trim().is_empty() {
            return syntax_error("bound cursor declaration requires a query");
        }
    } else if modifiers.len() != 1 || !modifiers[0].eq_ignore_ascii_case("REFCURSOR") {
        return syntax_error("unbound cursor declaration must use REFCURSOR");
    }
    let key = name.to_ascii_lowercase();
    if !scope.declared_names.insert(key.clone())
        || (!scope.allow_shadowing && (names.contains_key(&key) || cursor_names.contains_key(&key)))
    {
        return Err(DbError::new(
            "42710",
            format!("PL/pgSQL variable {name} is declared more than once"),
        ));
    }
    let slot = locals.len();
    names.insert(key.clone(), slot);
    locals.push(LocalSlot {
        name: name.to_owned(),
        constant: false,
        kind: LocalKind::Scalar,
    });
    let cursor = cursor_declarations.len();
    cursor_names.insert(key, cursor);
    cursor_declarations.push(CursorDeclaration {
        name: name.to_owned(),
        bound_query: bound_query.map(|query| rewrite_locals(query.trim(), names)),
    });
    Ok(true)
}

fn parse_open_cursor(
    statement: &str,
    locals: &BTreeMap<String, usize>,
    cursor_names: &BTreeMap<String, usize>,
    cursor_declarations: &[CursorDeclaration],
) -> Result<Instruction> {
    let rest = statement["OPEN".len()..].trim();
    let (name, tail) = rest
        .split_once(char::is_whitespace)
        .map_or((rest, ""), |(name, tail)| (name, tail.trim_start()));
    let cursor = lookup_cursor(name, cursor_names)?;
    let declaration = cursor_declarations
        .get(cursor)
        .ok_or_else(|| DbError::internal("cursor declaration index is invalid"))?;
    let query = if tail.is_empty() {
        if declaration.bound_query.is_none() {
            return syntax_error(format!("OPEN {name} requires FOR followed by a query"));
        }
        CursorQuery::Bound
    } else {
        let body = strip_leading_keyword(tail, "FOR")
            .ok_or_else(|| DbError::new("42601", "OPEN cursor syntax requires FOR"))?;
        if declaration.bound_query.is_some() {
            return syntax_error("bound cursor OPEN must not specify another query");
        }
        if let Some(dynamic) = strip_leading_keyword(body, "EXECUTE") {
            let (query, using) = split_keyword(dynamic, "USING");
            if query.trim().is_empty() {
                return syntax_error("OPEN FOR EXECUTE requires a query expression");
            }
            let using = using
                .map(|values| -> Result<Vec<String>> {
                    Ok(
                        split_top_level_expressions(values, "OPEN FOR EXECUTE USING")?
                            .into_iter()
                            .map(|value| rewrite_locals(value.trim(), locals))
                            .collect(),
                    )
                })
                .transpose()?
                .unwrap_or_default();
            CursorQuery::Dynamic {
                query: rewrite_locals(query.trim(), locals),
                using,
            }
        } else {
            if body.trim().is_empty() {
                return syntax_error("OPEN FOR requires a query");
            }
            CursorQuery::Static(rewrite_locals(body.trim(), locals))
        }
    };
    Ok(Instruction::OpenCursor { cursor, query })
}

fn parse_fetch_cursor(
    statement: &str,
    locals: &BTreeMap<String, usize>,
    cursor_names: &BTreeMap<String, usize>,
) -> Result<Instruction> {
    let rest = statement["FETCH".len()..].trim();
    let (cursor_clause, into) = split_keyword(rest, "INTO");
    let into = into.ok_or_else(|| DbError::new("42601", "FETCH requires INTO"))?;
    if into.split_whitespace().count() != 1 {
        return unsupported_feature("FETCH INTO multiple targets is not supported");
    }
    let target = *locals
        .get(&into.to_ascii_lowercase())
        .ok_or_else(|| DbError::new("42703", format!("FETCH target {into} does not exist")))?;
    let (cursor, direction) = parse_cursor_reference(cursor_clause, locals, cursor_names)?;
    Ok(Instruction::FetchCursor {
        cursor,
        direction,
        into: target,
    })
}

fn parse_move_cursor(
    statement: &str,
    locals: &BTreeMap<String, usize>,
    cursor_names: &BTreeMap<String, usize>,
) -> Result<Instruction> {
    let rest = statement["MOVE".len()..].trim();
    let (cursor, direction) = parse_cursor_reference(rest, locals, cursor_names)?;
    Ok(Instruction::MoveCursor { cursor, direction })
}

fn parse_close_cursor(
    statement: &str,
    cursor_names: &BTreeMap<String, usize>,
) -> Result<Instruction> {
    let name = statement["CLOSE".len()..].trim();
    if name.split_whitespace().count() != 1 {
        return syntax_error("CLOSE requires one cursor name");
    }
    Ok(Instruction::CloseCursor {
        cursor: lookup_cursor(name, cursor_names)?,
    })
}

fn parse_cursor_reference(
    value: &str,
    locals: &BTreeMap<String, usize>,
    cursor_names: &BTreeMap<String, usize>,
) -> Result<(usize, CursorDirection)> {
    let (direction, cursor_name) = split_keyword(value, "FROM");
    let (direction, cursor_name) = if let Some(cursor_name) = cursor_name {
        (direction, cursor_name)
    } else {
        let (direction, cursor_name) = split_keyword(value, "IN");
        cursor_name.map_or(("", direction), |cursor_name| (direction, cursor_name))
    };
    if cursor_name.split_whitespace().count() != 1 {
        return syntax_error("cursor reference requires one cursor name");
    }
    Ok((
        lookup_cursor(cursor_name, cursor_names)?,
        parse_cursor_direction(direction, locals)?,
    ))
}

fn parse_cursor_direction(
    value: &str,
    locals: &BTreeMap<String, usize>,
) -> Result<CursorDirection> {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("NEXT") {
        return Ok(CursorDirection::Next);
    }
    if value.eq_ignore_ascii_case("PRIOR") {
        return Ok(CursorDirection::Prior);
    }
    if value.eq_ignore_ascii_case("FIRST") {
        return Ok(CursorDirection::First);
    }
    if value.eq_ignore_ascii_case("LAST") {
        return Ok(CursorDirection::Last);
    }
    let (kind, amount) = value
        .split_once(char::is_whitespace)
        .map_or((value, ""), |(kind, amount)| (kind, amount.trim()));
    if kind.eq_ignore_ascii_case("ABSOLUTE") {
        return cursor_direction_expression(amount, locals, "ABSOLUTE")
            .map(CursorDirection::Absolute);
    }
    if kind.eq_ignore_ascii_case("RELATIVE") {
        return cursor_direction_expression(amount, locals, "RELATIVE")
            .map(CursorDirection::Relative);
    }
    if kind.eq_ignore_ascii_case("FORWARD") {
        if amount.eq_ignore_ascii_case("ALL") {
            return Ok(CursorDirection::ForwardAll);
        }
        return Ok(CursorDirection::Forward(
            (!amount.is_empty()).then(|| rewrite_locals(amount, locals)),
        ));
    }
    if kind.eq_ignore_ascii_case("BACKWARD") {
        if amount.eq_ignore_ascii_case("ALL") {
            return Ok(CursorDirection::BackwardAll);
        }
        return Ok(CursorDirection::Backward(
            (!amount.is_empty()).then(|| rewrite_locals(amount, locals)),
        ));
    }
    syntax_error(format!("unsupported cursor direction {value}"))
}

fn cursor_direction_expression(
    value: &str,
    locals: &BTreeMap<String, usize>,
    direction: &str,
) -> Result<String> {
    if value.is_empty() {
        return syntax_error(format!("{direction} requires a position"));
    }
    Ok(rewrite_locals(value, locals))
}

fn lookup_cursor(name: &str, cursor_names: &BTreeMap<String, usize>) -> Result<usize> {
    cursor_names
        .get(&name.to_ascii_lowercase())
        .copied()
        .ok_or_else(|| DbError::new("34000", format!("cursor {name} does not exist")))
}

fn compile_declaration(
    declaration: &str,
    locals: &mut Vec<LocalSlot>,
    names: &mut BTreeMap<String, usize>,
    cursor_declarations: &mut Vec<CursorDeclaration>,
    cursor_names: &mut BTreeMap<String, usize>,
    instructions: &mut Vec<Instruction>,
    scope: &mut DeclarationScope,
) -> Result<()> {
    if compile_cursor_declaration(
        declaration,
        locals,
        names,
        cursor_declarations,
        cursor_names,
        scope,
    )? {
        return Ok(());
    }
    let (head, initializer) = declaration
        .split_once(":=")
        .map_or((declaration, None), |(head, value)| {
            (head, Some(value.trim()))
        });
    let mut parts = head.split_whitespace();
    let name = parts
        .next()
        .ok_or_else(|| DbError::new("42601", "variable declaration requires a name"))?;
    let declaration_parts = parts.collect::<Vec<_>>();
    let constant = declaration_parts
        .iter()
        .any(|part| part.eq_ignore_ascii_case("CONSTANT"));
    let kind = declaration_parts
        .iter()
        .find_map(|part| {
            if part.eq_ignore_ascii_case("RECORD") {
                Some(LocalKind::Record)
            } else {
                part.to_ascii_uppercase()
                    .strip_suffix("%ROWTYPE")
                    .map(|_| LocalKind::RowType(part[..part.len() - "%ROWTYPE".len()].to_owned()))
            }
        })
        .unwrap_or(LocalKind::Scalar);
    let key = name.to_ascii_lowercase();
    if !scope.declared_names.insert(key.clone())
        || (!scope.allow_shadowing && (names.contains_key(&key) || cursor_names.contains_key(&key)))
    {
        return Err(DbError::new(
            "42710",
            format!("PL/pgSQL variable {name} is declared more than once"),
        ));
    }
    let slot = locals.len();
    names.insert(key, slot);
    locals.push(LocalSlot {
        name: name.to_owned(),
        constant,
        kind,
    });
    if let Some(initializer) = initializer {
        instructions.push(Instruction::Assign {
            slot,
            expression: rewrite_locals(initializer, names),
        });
    }
    Ok(())
}

fn compile_statement(
    statement: &str,
    locals: &BTreeMap<String, usize>,
    cursor_names: &BTreeMap<String, usize>,
    cursor_declarations: &[CursorDeclaration],
    instructions: &mut Vec<Instruction>,
) -> Result<()> {
    let uppercase = statement.to_ascii_uppercase();
    if uppercase.starts_with("OPEN ") {
        instructions.push(parse_open_cursor(
            statement,
            locals,
            cursor_names,
            cursor_declarations,
        )?);
    } else if uppercase.starts_with("FETCH ") {
        instructions.push(parse_fetch_cursor(statement, locals, cursor_names)?);
    } else if uppercase.starts_with("MOVE ") {
        instructions.push(parse_move_cursor(statement, locals, cursor_names)?);
    } else if uppercase.starts_with("CLOSE ") {
        instructions.push(parse_close_cursor(statement, cursor_names)?);
    } else if uppercase == "RAISE" || uppercase.starts_with("RAISE ") {
        instructions.push(parse_raise_instruction(statement, locals)?);
    } else if uppercase.starts_with("ASSERT ") {
        let body = statement[7..].trim();
        let (condition, message) = split_top_level_comma(body)?;
        if condition.trim().is_empty() {
            return syntax_error("ASSERT requires a condition");
        }
        instructions.push(Instruction::Assert {
            condition: rewrite_locals(condition.trim(), locals),
            message: message.map(|message| rewrite_locals(message.trim(), locals)),
        });
    } else if uppercase.starts_with("RETURN NEXT ") {
        instructions.push(Instruction::Return {
            expression: Some(rewrite_locals(statement[12..].trim(), locals)),
            next: true,
        });
    } else if uppercase == "RETURN" {
        instructions.push(Instruction::Return {
            expression: None,
            next: false,
        });
    } else if uppercase.starts_with("RETURN ") {
        instructions.push(Instruction::Return {
            expression: Some(rewrite_locals(statement[7..].trim(), locals)),
            next: false,
        });
    } else if uppercase.starts_with("PERFORM ") {
        instructions.push(Instruction::ExecuteSql {
            sql: format!("SELECT {}", rewrite_locals(statement[8..].trim(), locals)),
            into: None,
        });
    } else if uppercase.starts_with("EXECUTE ") {
        let rest = statement[8..].trim();
        let (head, using) = split_keyword(rest, "USING");
        let (query, into) = split_keyword(head, "INTO");
        if query.trim().is_empty() {
            return syntax_error("dynamic EXECUTE requires a query expression");
        }
        let (into, strict) = parse_dynamic_into(into, locals)?;
        let using = using
            .map(|values| -> Result<Vec<String>> {
                Ok(split_top_level_expressions(values, "EXECUTE USING")?
                    .into_iter()
                    .map(|value| rewrite_locals(value.trim(), locals))
                    .collect::<Vec<_>>())
            })
            .transpose()?
            .unwrap_or_default();
        instructions.push(Instruction::DynamicExecute {
            query: rewrite_locals(query.trim(), locals),
            using,
            into,
            strict,
        });
    } else if let Some((name, expression)) = statement.split_once(":=") {
        let target = name.trim();
        if let Some((record, field)) = target.split_once('.') {
            if field.is_empty() || field.contains('.') {
                return syntax_error("composite assignment requires one field name");
            }
            let slot = *locals
                .get(&record.trim().to_ascii_lowercase())
                .ok_or_else(|| {
                    DbError::new("42703", format!("variable {record} does not exist"))
                })?;
            instructions.push(Instruction::AssignField {
                slot,
                field: field.trim().to_owned(),
                expression: rewrite_locals(expression.trim(), locals),
            });
        } else {
            let key = target.to_ascii_lowercase();
            let slot = *locals
                .get(&key)
                .ok_or_else(|| DbError::new("42703", format!("variable {name} does not exist")))?;
            instructions.push(Instruction::Assign {
                slot,
                expression: rewrite_locals(expression.trim(), locals),
            });
        }
    } else {
        let (sql, into) = extract_select_into(statement, locals)?;
        instructions.push(Instruction::ExecuteSql {
            sql: rewrite_locals(&sql, locals),
            into,
        });
    }
    Ok(())
}

fn parse_dynamic_into(
    into: Option<&str>,
    locals: &BTreeMap<String, usize>,
) -> Result<(Option<usize>, bool)> {
    let Some(into) = into else {
        return Ok((None, false));
    };
    if into.contains(',') {
        return unsupported_feature("dynamic EXECUTE INTO multiple targets is not supported");
    }
    let mut parts = into.split_whitespace();
    let first = parts
        .next()
        .ok_or_else(|| DbError::new("42601", "dynamic EXECUTE INTO requires a target"))?;
    let (strict, target) = if first.eq_ignore_ascii_case("STRICT") {
        (
            true,
            parts.next().ok_or_else(|| {
                DbError::new("42601", "dynamic EXECUTE INTO STRICT requires a target")
            })?,
        )
    } else {
        (false, first)
    };
    if parts.next().is_some() {
        return syntax_error("dynamic EXECUTE INTO accepts one target variable");
    }
    let slot = locals
        .get(&target.to_ascii_lowercase())
        .copied()
        .ok_or_else(|| {
            DbError::new(
                "42703",
                format!("dynamic EXECUTE INTO variable {target} does not exist"),
            )
        })?;
    Ok((Some(slot), strict))
}

fn split_top_level_expressions<'a>(value: &'a str, context: &str) -> Result<Vec<&'a str>> {
    let mut expressions = Vec::new();
    let mut quote = None;
    let mut depth = 0_usize;
    let mut start = 0_usize;
    for (index, character) in value.char_indices() {
        match quote {
            Some(delimiter) if character == delimiter => quote = None,
            Some(_) => {}
            None if matches!(character, '\'' | '"') => quote = Some(character),
            None if character == '(' => depth = depth.saturating_add(1),
            None if character == ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    DbError::new(
                        "42601",
                        format!("{context} has an unmatched closing parenthesis"),
                    )
                })?;
            }
            None if character == ',' && depth == 0 => {
                let expression = value[start..index].trim();
                if expression.is_empty() {
                    return syntax_error(format!("{context} contains an empty expression"));
                }
                expressions.push(expression);
                start = index + 1;
            }
            None => {}
        }
    }
    if quote.is_some() || depth != 0 {
        return syntax_error(format!("{context} expressions are not balanced"));
    }
    let expression = value[start..].trim();
    if expression.is_empty() {
        return syntax_error(format!("{context} requires at least one expression"));
    }
    expressions.push(expression);
    Ok(expressions)
}

fn parse_raise_instruction(
    statement: &str,
    locals: &BTreeMap<String, usize>,
) -> Result<Instruction> {
    let rest = statement["RAISE".len()..].trim();
    if rest.is_empty() {
        return Ok(Instruction::Raise {
            level: RaiseLevel::Exception,
            message: None,
            sql_state: None,
        });
    }
    let (first, tail) = rest
        .split_once(char::is_whitespace)
        .map_or((rest, ""), |(first, tail)| (first, tail.trim_start()));
    let (level, body) = if first.eq_ignore_ascii_case("INFO") {
        (RaiseLevel::Info, tail)
    } else if first.eq_ignore_ascii_case("NOTICE") {
        (RaiseLevel::Notice, tail)
    } else if first.eq_ignore_ascii_case("WARNING") {
        (RaiseLevel::Warning, tail)
    } else if first.eq_ignore_ascii_case("EXCEPTION") {
        (RaiseLevel::Exception, tail)
    } else {
        (RaiseLevel::Exception, rest)
    };
    let (message, options) = split_keyword(body, "USING");
    if message.trim().is_empty() {
        return unsupported_feature(
            "RAISE USING MESSAGE without a message expression is not supported",
        );
    }
    let sql_state = options.map(parse_raise_options).transpose()?.flatten();
    Ok(Instruction::Raise {
        level,
        message: Some(rewrite_locals(message.trim(), locals)),
        sql_state,
    })
}

fn parse_raise_options(options: &str) -> Result<Option<String>> {
    let mut sql_state = None;
    for option in options.split(',') {
        let (name, value) = option
            .split_once('=')
            .ok_or_else(|| DbError::new("42601", "RAISE USING options require name = value"))?;
        if !name.trim().eq_ignore_ascii_case("ERRCODE") {
            return unsupported_feature(format!("RAISE USING {} is not supported", name.trim()));
        }
        if sql_state.is_some() {
            return syntax_error("RAISE specifies ERRCODE more than once");
        }
        let value = value
            .trim()
            .strip_prefix('\'')
            .and_then(|value| value.strip_suffix('\''))
            .ok_or_else(|| DbError::new("42601", "RAISE ERRCODE must be a string literal"))?;
        validate_sql_state(value)?;
        sql_state = Some(value.to_ascii_uppercase());
    }
    Ok(sql_state)
}

fn split_top_level_comma(value: &str) -> Result<(&str, Option<&str>)> {
    let mut quote = None;
    let mut depth = 0_usize;
    for (index, character) in value.char_indices() {
        match quote {
            Some(delimiter) if character == delimiter => quote = None,
            Some(_) => {}
            None if matches!(character, '\'' | '"') => quote = Some(character),
            None if character == '(' => depth = depth.saturating_add(1),
            None if character == ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    DbError::new("42601", "ASSERT has an unmatched closing parenthesis")
                })?;
            }
            None if character == ',' && depth == 0 => {
                return Ok((&value[..index], Some(&value[index + 1..])));
            }
            None => {}
        }
    }
    if quote.is_some() || depth != 0 {
        return syntax_error("ASSERT condition or message is not balanced");
    }
    Ok((value, None))
}

fn extract_select_into(
    statement: &str,
    locals: &BTreeMap<String, usize>,
) -> Result<(String, Option<usize>)> {
    if !statement
        .get(.."SELECT ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SELECT "))
    {
        return Ok((statement.to_owned(), None));
    }
    let uppercase = statement.to_ascii_uppercase();
    let Some(into_start) = uppercase.find(" INTO ") else {
        return Ok((statement.to_owned(), None));
    };
    let tail = &statement[into_start + 6..];
    let variable_end = tail.find(char::is_whitespace).unwrap_or(tail.len());
    let variable = tail[..variable_end].trim();
    let slot = *locals.get(&variable.to_ascii_lowercase()).ok_or_else(|| {
        DbError::new(
            "42703",
            format!("SELECT INTO variable {variable} does not exist"),
        )
    })?;
    let before = statement[..into_start].trim_end();
    let after = tail[variable_end..].trim_start();
    let sql = if after.is_empty() {
        before.to_owned()
    } else {
        format!("{before} {after}")
    };
    Ok((sql, Some(slot)))
}

struct VmState {
    program: Program,
    limits: ResourceLimits,
    locals: Vec<Value>,
    records: BTreeMap<usize, RuntimeRecord>,
    instruction_pointer: usize,
    steps: usize,
    returned_rows: Vec<Value>,
    query_loops: BTreeMap<usize, QueryLoopState>,
    integer_loops: BTreeMap<usize, IntegerLoopState>,
    foreach_loops: BTreeMap<usize, ForeachLoopState>,
    cursors: BTreeMap<usize, CursorState>,
    active_exception: Option<DbError>,
    exception_regions: Vec<(usize, usize)>,
    active_exception_regions: Vec<(usize, usize)>,
    memory_reservation: VmMemoryReservation,
}

enum PendingSql {
    Execute {
        into: Option<usize>,
    },
    Dynamic {
        into: Option<usize>,
        strict: bool,
    },
    OpenCursor {
        cursor: usize,
    },
    QueryForStart {
        start: usize,
        slot: usize,
        end: usize,
    },
}

pub fn execute(
    program: &Program,
    host: &mut impl PlpgsqlHost,
    arguments: &[Value],
) -> Result<VmOutput> {
    execute_with_limits(program, host, arguments, ResourceLimits::default())
}

pub fn execute_with_limits(
    program: &Program,
    host: &mut impl PlpgsqlHost,
    arguments: &[Value],
    limits: ResourceLimits,
) -> Result<VmOutput> {
    let memory = VmMemoryGrant::new(limits.max_cursor_bytes)?;
    execute_with_memory_grant(program, host, arguments, limits, memory)
}

pub fn execute_with_memory_grant(
    program: &Program,
    host: &mut impl PlpgsqlHost,
    arguments: &[Value],
    limits: ResourceLimits,
    memory: VmMemoryGrant,
) -> Result<VmOutput> {
    let mut machine = VmMachine::new_with_memory_grant(program, host, arguments, limits, memory)?;
    let mut response = None;
    loop {
        match machine.resume(host, response.take())? {
            VmRunState::Sql(request) => {
                response = Some(host.execute_sql(&request.sql, &request.parameters));
            }
            VmRunState::Complete(output) => return Ok(output),
        }
    }
}
impl VmMachine {
    pub fn new(
        program: &Program,
        host: &mut impl PlpgsqlHost,
        arguments: &[Value],
        limits: ResourceLimits,
    ) -> Result<Self> {
        let memory = VmMemoryGrant::new(limits.max_cursor_bytes)?;
        Self::new_with_memory_grant(program, host, arguments, limits, memory)
    }

    pub fn new_with_memory_grant(
        program: &Program,
        host: &mut impl PlpgsqlHost,
        arguments: &[Value],
        limits: ResourceLimits,
        memory: VmMemoryGrant,
    ) -> Result<Self> {
        if program.version != BYTECODE_VERSION {
            return Err(DbError::new(
                "0A000",
                format!("unsupported PL/pgSQL bytecode version {}", program.version),
            ));
        }
        let mut locals = vec![Value::Null; program.locals.len()];
        for (slot, value) in locals.iter_mut().zip(arguments) {
            *slot = value.clone();
        }
        let records = initialize_runtime_records(program, host, limits.max_cursor_bytes)?;
        let mut exception_regions = program
            .exception_handlers
            .iter()
            .map(|handler| (handler.protected_start, handler.protected_end))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        exception_regions
            .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| right.1.cmp(&left.1)));
        let memory_reservation = memory.try_reserve(0)?;
        let mut state = VmState {
            program: program.clone(),
            limits,
            locals,
            records,
            instruction_pointer: 0,
            steps: 0,
            returned_rows: Vec::new(),
            query_loops: BTreeMap::new(),
            integer_loops: BTreeMap::new(),
            foreach_loops: BTreeMap::new(),
            cursors: BTreeMap::new(),
            active_exception: None,
            exception_regions,
            active_exception_regions: Vec::new(),
            memory_reservation,
        };
        state.refresh_memory(None)?;
        Ok(Self {
            state: Some(state),
            pending_sql: None,
        })
    }
}
