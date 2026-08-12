
fn compile_with_arguments_and_limits(
    source: &str,
    argument_names: &[String],
    limits: ResourceLimits,
) -> Result<Program> {
    if source.len() > limits.max_source_bytes {
        return limit_error("PL/pgSQL source exceeds the configured byte limit");
    }
    let lines = logical_lines(source)?;
    let token_count = lines
        .iter()
        .map(|line| line.split_whitespace().count())
        .sum::<usize>();
    if token_count > limits.max_tokens {
        return limit_error("PL/pgSQL source exceeds the configured token limit");
    }

    let mut locals = Vec::with_capacity(argument_names.len());
    let mut local_names = BTreeMap::new();
    let mut cursor_declarations = Vec::new();
    let mut cursor_names = BTreeMap::new();
    for name in argument_names {
        let key = name.to_ascii_lowercase();
        if local_names.insert(key, locals.len()).is_some() {
            return Err(DbError::new(
                "42710",
                format!("PL/pgSQL argument {name} is declared more than once"),
            ));
        }
        locals.push(LocalSlot {
            name: name.clone(),
            constant: false,
            kind: LocalKind::Scalar,
        });
    }
    let mut instructions = Vec::new();
    let mut controls = Vec::new();
    let mut declaring = false;
    let mut exception_handlers = Vec::<ExceptionHandler>::new();
    let mut blocks = Vec::<ExceptionCompileFrame>::new();
    let mut sqlstate_slot = None;
    let mut sqlerrm_slot = None;
    let mut pending_label = None::<String>;
    let mut pending_block_label = None::<String>;
    let mut pending_scope = None::<(BTreeMap<String, usize>, BTreeMap<String, usize>)>;
    let mut declaration_scope = DeclarationScope::default();

    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let uppercase = trimmed.to_ascii_uppercase();
        if let Some(label) = parse_label(trimmed)? {
            if declaring || pending_block_label.is_some() || pending_label.replace(label).is_some()
            {
                return syntax_error("a PL/pgSQL label must be followed by one block or loop");
            }
            continue;
        }
        let starts_loop = ((uppercase.starts_with("WHILE ")
            || uppercase.starts_with("FOREACH ")
            || uppercase.starts_with("FOR "))
            && uppercase.ends_with(" LOOP"))
            || uppercase == "LOOP";
        if pending_label.is_some() && !starts_loop && uppercase != "DECLARE" && uppercase != "BEGIN"
        {
            return syntax_error("a PL/pgSQL label must be followed by one block or loop");
        }
        if uppercase == "DECLARE" {
            if blocks.last().is_some_and(|block| block.in_handlers) {
                return syntax_error("DECLARE is not valid inside an exception handler");
            }
            if declaring || pending_scope.is_some() {
                return syntax_error("DECLARE must be followed by one BEGIN block");
            }
            if blocks
                .last()
                .is_some_and(|block| controls.len() != block.control_depth)
            {
                return syntax_error("DECLARE is not valid inside an open control structure");
            }
            pending_scope = Some((local_names.clone(), cursor_names.clone()));
            pending_block_label = pending_label.take();
            declaration_scope.declared_names.clear();
            declaration_scope.allow_shadowing = !blocks.is_empty();
            declaring = true;
            continue;
        }
        if uppercase == "BEGIN" {
            let (outer_local_names, outer_cursor_names) = pending_scope
                .take()
                .unwrap_or_else(|| (local_names.clone(), cursor_names.clone()));
            declaring = false;
            declaration_scope.declared_names.clear();
            ensure_nesting(&blocks, limits)?;
            blocks.push(ExceptionCompileFrame::new(
                pending_block_label.take().or_else(|| pending_label.take()),
                instructions.len(),
                controls.len(),
                outer_local_names,
                outer_cursor_names,
            ));
            continue;
        }
        if uppercase == "EXCEPTION" {
            let block = blocks
                .last_mut()
                .ok_or_else(|| DbError::new("42601", "EXCEPTION has no matching BEGIN"))?;
            if block.in_handlers {
                return syntax_error("an exception block can contain one EXCEPTION section");
            }
            if controls.len() != block.control_depth {
                return syntax_error("EXCEPTION cannot begin inside an open control structure");
            }
            declaring = false;
            block.in_handlers = true;
            sqlstate_slot = Some(ensure_diagnostic_local(
                "sqlstate",
                &mut locals,
                &mut local_names,
            )?);
            sqlerrm_slot = Some(ensure_diagnostic_local(
                "sqlerrm",
                &mut locals,
                &mut local_names,
            )?);
            block.protected_end = Some(instructions.len());
            let skip = instructions.len();
            instructions.push(Instruction::Jump { target: usize::MAX });
            block.skip_handlers = Some(skip);
            continue;
        }
        if blocks
            .last()
            .is_some_and(|block| block.in_handlers && controls.len() == block.control_depth)
            && uppercase.starts_with("WHEN ")
            && uppercase.ends_with(" THEN")
        {
            let block = blocks
                .last_mut()
                .ok_or_else(|| DbError::internal("exception block stack is empty"))?;
            let protected_end = block
                .protected_end
                .ok_or_else(|| DbError::internal("exception region lost its protected end"))?;
            if !block.handler_indexes.is_empty() {
                let jump = instructions.len();
                instructions.push(Instruction::Jump { target: usize::MAX });
                block.end_jumps.push(jump);
            }
            let matcher_text = trimmed[5..trimmed.len() - 5].trim();
            let matcher = parse_exception_matcher(matcher_text)?;
            if block
                .handler_indexes
                .iter()
                .any(|index| exception_handlers[*index].matcher == ExceptionMatcher::Others)
            {
                return syntax_error("WHEN OTHERS must be the final exception handler");
            }
            let handler_index = exception_handlers.len();
            exception_handlers.push(ExceptionHandler {
                protected_start: block.protected_start,
                protected_end,
                matcher,
                target: instructions.len(),
            });
            block.handler_indexes.push(handler_index);
            ensure_instruction_limit(&instructions, limits)?;
            continue;
        }
        if let Some(closing_label) = parse_block_end_label(trimmed)? {
            let block = blocks
                .pop()
                .ok_or_else(|| DbError::new("42601", "END has no matching BEGIN"))?;
            if controls.len() != block.control_depth {
                return syntax_error("END closes a block with an open control structure");
            }
            if closing_label.is_some() && closing_label != block.label {
                return syntax_error("END label does not match its opening block label");
            }
            if block.in_handlers {
                if block.handler_indexes.is_empty() {
                    return syntax_error("EXCEPTION requires at least one WHEN handler");
                }
                let end = instructions.len();
                patch_target(&mut instructions, block.skip_handlers, end)?;
                for jump in block.end_jumps {
                    patch_target(&mut instructions, Some(jump), end)?;
                }
            }
            let end = instructions.len();
            for jump in block.exits {
                patch_target(&mut instructions, Some(jump), end)?;
            }
            local_names = block.outer_local_names;
            cursor_names = block.outer_cursor_names;
            continue;
        }
        if declaring {
            compile_declaration(
                trimmed,
                &mut locals,
                &mut local_names,
                &mut cursor_declarations,
                &mut cursor_names,
                &mut instructions,
                &mut declaration_scope,
            )?;
            ensure_instruction_limit(&instructions, limits)?;
            continue;
        }

        if uppercase.starts_with("IF ") && uppercase.ends_with(" THEN") {
            ensure_nesting(&controls, limits)?;
            let expression = trimmed[3..trimmed.len() - 5].trim();
            let expression = rewrite_locals(expression, &local_names);
            let jump = instructions.len();
            instructions.push(Instruction::JumpIfFalse {
                expression,
                target: usize::MAX,
            });
            controls.push(ControlFrame::If {
                pending_false: Some(jump),
                end_jumps: Vec::new(),
            });
        } else if uppercase.starts_with("ELSIF ") && uppercase.ends_with(" THEN") {
            let current = instructions.len();
            let frame = controls
                .last_mut()
                .ok_or_else(|| DbError::new("42601", "ELSIF has no matching IF"))?;
            let ControlFrame::If {
                pending_false,
                end_jumps,
            } = frame
            else {
                return syntax_error("ELSIF is only valid inside IF");
            };
            let end_jump = current;
            instructions.push(Instruction::Jump { target: usize::MAX });
            end_jumps.push(end_jump);
            let next_branch = instructions.len();
            patch_target(&mut instructions, pending_false.take(), next_branch)?;
            let expression = trimmed[6..trimmed.len() - 5].trim();
            let false_jump = instructions.len();
            instructions.push(Instruction::JumpIfFalse {
                expression: rewrite_locals(expression, &local_names),
                target: usize::MAX,
            });
            *pending_false = Some(false_jump);
        } else if uppercase == "ELSE" {
            let current = instructions.len();
            let frame = controls
                .last_mut()
                .ok_or_else(|| DbError::new("42601", "ELSE has no matching control structure"))?;
            let (pending_false, end_jumps) = match frame {
                ControlFrame::If {
                    pending_false,
                    end_jumps,
                } => (pending_false, end_jumps),
                ControlFrame::Case {
                    pending_false,
                    end_jumps,
                    branch_started,
                    ..
                } if *branch_started => (pending_false, end_jumps),
                ControlFrame::Case { .. } => {
                    return syntax_error("CASE ELSE requires a preceding WHEN");
                }
                ControlFrame::Loop { .. } => {
                    return syntax_error("ELSE is only valid inside IF or CASE");
                }
            };
            let end_jump = current;
            instructions.push(Instruction::Jump { target: usize::MAX });
            end_jumps.push(end_jump);
            let else_target = instructions.len();
            patch_target(&mut instructions, pending_false.take(), else_target)?;
        } else if uppercase == "END IF" {
            let end = instructions.len();
            let frame = controls
                .pop()
                .ok_or_else(|| DbError::new("42601", "END IF has no matching IF"))?;
            let ControlFrame::If {
                pending_false,
                end_jumps,
            } = frame
            else {
                return syntax_error("END IF closes a loop");
            };
            patch_target(&mut instructions, pending_false, end)?;
            for jump in end_jumps {
                patch_target(&mut instructions, Some(jump), end)?;
            }
        } else if uppercase == "CASE" || uppercase.starts_with("CASE ") {
            ensure_nesting(&controls, limits)?;
            let operand =
                (uppercase != "CASE").then(|| rewrite_locals(trimmed[5..].trim(), &local_names));
            controls.push(ControlFrame::Case {
                operand,
                pending_false: None,
                end_jumps: Vec::new(),
                branch_started: false,
            });
        } else if uppercase.starts_with("WHEN ") && uppercase.ends_with(" THEN") {
            let frame = controls
                .last_mut()
                .ok_or_else(|| DbError::new("42601", "WHEN has no matching CASE"))?;
            let ControlFrame::Case {
                operand,
                pending_false,
                end_jumps,
                branch_started,
            } = frame
            else {
                return syntax_error("WHEN is only valid inside CASE");
            };
            if *branch_started {
                let end_jump = instructions.len();
                instructions.push(Instruction::Jump { target: usize::MAX });
                end_jumps.push(end_jump);
                let next_branch = instructions.len();
                patch_target(&mut instructions, pending_false.take(), next_branch)?;
            }
            let branch = trimmed[5..trimmed.len() - 5].trim();
            let branch = rewrite_locals(branch, &local_names);
            let expression = operand.as_ref().map_or(branch.clone(), |operand| {
                format!("({operand}) = ({branch})")
            });
            let false_jump = instructions.len();
            instructions.push(Instruction::JumpIfFalse {
                expression,
                target: usize::MAX,
            });
            *pending_false = Some(false_jump);
            *branch_started = true;
        } else if uppercase == "END CASE" {
            let end = instructions.len();
            let frame = controls
                .pop()
                .ok_or_else(|| DbError::new("42601", "END CASE has no matching CASE"))?;
            let ControlFrame::Case {
                operand: _,
                pending_false,
                end_jumps,
                branch_started,
            } = frame
            else {
                return syntax_error("END CASE closes a non-CASE control structure");
            };
            if !branch_started {
                return syntax_error("CASE requires at least one WHEN branch");
            }
            patch_target(&mut instructions, pending_false, end)?;
            for jump in end_jumps {
                patch_target(&mut instructions, Some(jump), end)?;
            }
        } else if uppercase.starts_with("WHILE ") && uppercase.ends_with(" LOOP") {
            ensure_nesting(&controls, limits)?;
            let start = instructions.len();
            let expression = trimmed[6..trimmed.len() - 5].trim();
            instructions.push(Instruction::JumpIfFalse {
                expression: rewrite_locals(expression, &local_names),
                target: usize::MAX,
            });
            controls.push(ControlFrame::Loop {
                label: pending_label.take(),
                start,
                false_jump: Some(start),
                exits: Vec::new(),
                continues: Vec::new(),
                query_start: None,
                integer_start: None,
                foreach_start: None,
            });
        } else if uppercase.starts_with("FOREACH ") && uppercase.ends_with(" LOOP") {
            ensure_nesting(&controls, limits)?;
            let rest = trimmed[8..trimmed.len() - 5].trim();
            let (target, array) = split_keyword(rest, "IN ARRAY");
            let array = array.ok_or_else(|| {
                DbError::new(
                    "42601",
                    "FOREACH requires IN ARRAY followed by an expression",
                )
            })?;
            if target
                .split_whitespace()
                .any(|part| part.eq_ignore_ascii_case("SLICE"))
            {
                return unsupported_feature("FOREACH SLICE is not supported");
            }
            if target.split_whitespace().count() != 1 || array.trim().is_empty() {
                return syntax_error("FOREACH requires one target and one array expression");
            }
            let slot = *local_names
                .get(&target.to_ascii_lowercase())
                .ok_or_else(|| {
                    DbError::new("42703", format!("FOREACH variable {target} does not exist"))
                })?;
            let start = instructions.len();
            instructions.push(Instruction::ForeachStart {
                slot,
                array: rewrite_locals(array.trim(), &local_names),
                end: usize::MAX,
            });
            controls.push(ControlFrame::Loop {
                label: pending_label.take(),
                start,
                false_jump: None,
                exits: Vec::new(),
                continues: Vec::new(),
                query_start: None,
                integer_start: None,
                foreach_start: Some(start),
            });
        } else if uppercase.starts_with("FOR ") && uppercase.ends_with(" LOOP") {
            ensure_nesting(&controls, limits)?;
            let rest = trimmed[4..trimmed.len() - 5].trim();
            let (target, source) = split_keyword(rest, "IN");
            let source = source.ok_or_else(|| {
                DbError::new("42601", "FOR requires IN followed by a range or query")
            })?;
            let slot = *local_names
                .get(&target.trim().to_ascii_lowercase())
                .ok_or_else(|| {
                    DbError::new(
                        "42703",
                        format!("FOR variable {} does not exist", target.trim()),
                    )
                })?;
            let start = instructions.len();
            let (reverse, source) = strip_leading_keyword(source.trim(), "REVERSE")
                .map_or((false, source.trim()), |source| (true, source));
            let query_source = source
                .get(.."SELECT".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SELECT"));
            if query_source {
                if reverse {
                    return syntax_error("REVERSE requires an integer FOR range");
                }
                instructions.push(Instruction::QueryForStart {
                    slot,
                    sql: rewrite_locals(source, &local_names),
                    end: usize::MAX,
                });
                controls.push(ControlFrame::Loop {
                    label: pending_label.take(),
                    start,
                    false_jump: None,
                    exits: Vec::new(),
                    continues: Vec::new(),
                    query_start: Some(start),
                    integer_start: None,
                    foreach_start: None,
                });
            } else {
                let (range, step) = split_keyword(source, "BY");
                let (lower, upper) = range.split_once("..").ok_or_else(|| {
                    DbError::new(
                        "0A000",
                        "only query SELECT and integer FOR loops are supported",
                    )
                })?;
                if lower.trim().is_empty() || upper.trim().is_empty() || upper.contains("..") {
                    return syntax_error("integer FOR requires lower .. upper bounds");
                }
                instructions.push(Instruction::IntegerForStart {
                    slot,
                    lower: rewrite_locals(lower.trim(), &local_names),
                    upper: rewrite_locals(upper.trim(), &local_names),
                    step: rewrite_locals(step.unwrap_or("1").trim(), &local_names),
                    reverse,
                    end: usize::MAX,
                });
                controls.push(ControlFrame::Loop {
                    label: pending_label.take(),
                    start,
                    false_jump: None,
                    exits: Vec::new(),
                    continues: Vec::new(),
                    query_start: None,
                    integer_start: Some(start),
                    foreach_start: None,
                });
            }
        } else if uppercase == "LOOP" {
            ensure_nesting(&controls, limits)?;
            controls.push(ControlFrame::Loop {
                label: pending_label.take(),
                start: instructions.len(),
                false_jump: None,
                exits: Vec::new(),
                continues: Vec::new(),
                query_start: None,
                integer_start: None,
                foreach_start: None,
            });
        } else if uppercase == "EXIT"
            || uppercase.starts_with("EXIT ")
            || uppercase == "CONTINUE"
            || uppercase.starts_with("CONTINUE ")
        {
            let is_exit = uppercase == "EXIT" || uppercase.starts_with("EXIT ");
            let keyword = if is_exit { "EXIT" } else { "CONTINUE" };
            let (label, condition) = parse_loop_control(trimmed, keyword)?;
            if let Some(condition) = condition {
                let condition = condition.trim();
                if condition.is_empty() {
                    return syntax_error(format!("{keyword} WHEN requires a condition"));
                }
                let skip = instructions.len();
                instructions.push(Instruction::JumpIfFalse {
                    expression: rewrite_locals(condition, &local_names),
                    target: skip + 2,
                });
            }
            let matching_loop = label.as_deref().is_some_and(|label| {
                controls.iter().rev().any(|frame| {
                    matches!(
                        frame,
                        ControlFrame::Loop {
                            label: Some(frame_label),
                            ..
                        } if frame_label == label
                    )
                })
            });
            if is_exit && label.is_some() && !matching_loop {
                let label = label.as_deref().unwrap_or_default();
                let block = blocks
                    .iter_mut()
                    .rev()
                    .find(|block| block.label.as_deref() == Some(label))
                    .ok_or_else(|| {
                        DbError::new("42601", format!("label {label} does not exist"))
                    })?;
                let jump = instructions.len();
                instructions.push(Instruction::Jump { target: usize::MAX });
                block.exits.push(jump);
                ensure_instruction_limit(&instructions, limits)?;
                continue;
            }
            let frame = loop_control_target_mut(&mut controls, label.as_deref())?;
            let ControlFrame::Loop {
                start,
                exits,
                continues,
                query_start,
                integer_start,
                foreach_start,
                ..
            } = frame
            else {
                return Err(DbError::internal("nearest loop is not a loop frame"));
            };
            let jump = instructions.len();
            if is_exit {
                instructions.push(Instruction::Jump { target: usize::MAX });
                exits.push(jump);
            } else {
                let deferred =
                    query_start.is_some() || integer_start.is_some() || foreach_start.is_some();
                instructions.push(Instruction::Jump {
                    target: if deferred { usize::MAX } else { *start },
                });
                if deferred {
                    continues.push(jump);
                }
            }
        } else if uppercase == "END LOOP" || uppercase.starts_with("END LOOP ") {
            let closing_label = trimmed
                .get("END LOOP".len()..)
                .map(str::trim)
                .filter(|label| !label.is_empty())
                .map(normalize_label)
                .transpose()?;
            let frame = controls
                .pop()
                .ok_or_else(|| DbError::new("42601", "END LOOP has no matching LOOP"))?;
            let ControlFrame::Loop {
                label,
                start,
                false_jump,
                exits,
                continues,
                query_start,
                integer_start,
                foreach_start,
            } = frame
            else {
                return syntax_error("END LOOP closes a non-loop control structure");
            };
            if closing_label.is_some() && closing_label != label {
                return syntax_error("END LOOP label does not match its opening loop label");
            }
            let continue_target = instructions.len();
            match (query_start, integer_start, foreach_start) {
                (Some(query_start), None, None) => {
                    instructions.push(Instruction::QueryForNext {
                        start: query_start,
                        body: query_start + 1,
                    });
                }
                (None, Some(integer_start), None) => {
                    instructions.push(Instruction::IntegerForNext {
                        start: integer_start,
                        body: integer_start + 1,
                    });
                }
                (None, None, Some(foreach_start)) => {
                    instructions.push(Instruction::ForeachNext {
                        start: foreach_start,
                        body: foreach_start + 1,
                    });
                }
                (None, None, None) => instructions.push(Instruction::Jump { target: start }),
                _ => {
                    return Err(DbError::internal(
                        "loop has multiple iterator advance states",
                    ));
                }
            }
            let end = instructions.len();
            patch_target(&mut instructions, false_jump, end)?;
            if query_start.is_some() {
                patch_query_for_end(&mut instructions, start, end)?;
            }
            if integer_start.is_some() {
                patch_integer_for_end(&mut instructions, start, end)?;
            }
            if foreach_start.is_some() {
                patch_foreach_end(&mut instructions, start, end)?;
            }
            for jump in exits {
                patch_target(&mut instructions, Some(jump), end)?;
            }
            for jump in continues {
                patch_target(&mut instructions, Some(jump), continue_target)?;
            }
        } else {
            if blocks
                .last()
                .is_some_and(|block| block.in_handlers && block.handler_indexes.is_empty())
            {
                return syntax_error("EXCEPTION requires WHEN before handler statements");
            }
            compile_statement(
                trimmed,
                &local_names,
                &cursor_names,
                &cursor_declarations,
                &mut instructions,
            )?;
        }
        ensure_instruction_limit(&instructions, limits)?;
    }

    if !controls.is_empty() {
        return syntax_error("PL/pgSQL block has an unclosed control structure");
    }
    if pending_label.is_some() || pending_block_label.is_some() {
        return syntax_error("a PL/pgSQL label must be followed by one block or loop");
    }
    if declaring || pending_scope.is_some() {
        return syntax_error("DECLARE must be followed by one BEGIN block");
    }
    if !blocks.is_empty() {
        return syntax_error("PL/pgSQL block has an unmatched BEGIN");
    }
    instructions.push(Instruction::Checkpoint);
    ensure_instruction_limit(&instructions, limits)?;
    Ok(Program {
        version: BYTECODE_VERSION,
        instructions,
        locals,
        cursor_declarations,
        exception_handlers,
        sqlstate_slot,
        sqlerrm_slot,
    })
}

fn ensure_diagnostic_local(
    name: &str,
    locals: &mut Vec<LocalSlot>,
    names: &mut BTreeMap<String, usize>,
) -> Result<usize> {
    if let Some(slot) = names.get(name) {
        return Ok(*slot);
    }
    let slot = locals.len();
    names.insert(name.to_owned(), slot);
    locals.push(LocalSlot {
        name: name.to_owned(),
        constant: false,
        kind: LocalKind::Scalar,
    });
    Ok(slot)
}

#[derive(Debug, Default)]
struct DeclarationScope {
    declared_names: BTreeSet<String>,
    allow_shadowing: bool,
}
