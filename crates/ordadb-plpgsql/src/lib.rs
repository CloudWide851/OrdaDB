//! Bounded PL/pgSQL compiler and explicit-frame virtual machine.

use std::collections::BTreeMap;

use ordadb_types::{DbError, QueryEvent, Result, Value};

pub const BYTECODE_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceLimits {
    pub max_source_bytes: usize,
    pub max_tokens: usize,
    pub max_nesting: usize,
    pub max_instructions: usize,
    pub max_steps: usize,
    pub max_returned_rows: usize,
    pub max_dynamic_sql_bytes: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_source_bytes: 256 * 1024,
            max_tokens: 65_536,
            max_nesting: 128,
            max_instructions: 65_536,
            max_steps: 1_000_000,
            max_returned_rows: 100_000,
            max_dynamic_sql_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSlot {
    pub name: String,
    pub constant: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Instruction {
    Assign {
        slot: usize,
        expression: String,
    },
    AssignField {
        slot: usize,
        field: String,
        expression: String,
    },
    JumpIfFalse {
        expression: String,
        target: usize,
    },
    Jump {
        target: usize,
    },
    ExecuteSql {
        sql: String,
        into: Option<usize>,
    },
    DynamicExecute {
        query: String,
        using: Vec<String>,
    },
    QueryForStart {
        slot: usize,
        sql: String,
        end: usize,
    },
    QueryForNext {
        start: usize,
        body: usize,
    },
    Return {
        expression: Option<String>,
        next: bool,
    },
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExceptionMatcher {
    SqlState(String),
    Others,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExceptionHandler {
    pub protected_start: usize,
    pub protected_end: usize,
    pub matcher: ExceptionMatcher,
    pub target: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    pub version: u16,
    pub instructions: Vec<Instruction>,
    pub locals: Vec<LocalSlot>,
    pub exception_handlers: Vec<ExceptionHandler>,
}

pub trait PlpgsqlHost {
    fn execute_sql(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>> + '_>>;

    fn evaluate_expression(&mut self, sql: &str, parameters: &[Value]) -> Result<Value>;

    fn assign_composite_field(&mut self, slot: usize, field: &str, value: Value) -> Result<()> {
        let _ = (slot, field, value);
        Err(DbError::new(
            "0A000",
            "composite assignment is not supported in this PL/pgSQL context",
        ))
    }

    fn begin_exception_block(&mut self) -> Result<()> {
        Ok(())
    }

    fn commit_exception_block(&mut self) -> Result<()> {
        Ok(())
    }

    fn rollback_exception_block(&mut self) -> Result<()> {
        Ok(())
    }

    fn check_cancelled(&self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct VmOutput {
    pub return_value: Option<Value>,
    pub returned_rows: Vec<Value>,
    pub return_parameter: Option<usize>,
}

enum ControlFrame {
    If {
        pending_false: Option<usize>,
        end_jumps: Vec<usize>,
    },
    Case {
        operand: Option<String>,
        pending_false: Option<usize>,
        end_jumps: Vec<usize>,
        branch_started: bool,
    },
    Loop {
        start: usize,
        false_jump: Option<usize>,
        exits: Vec<usize>,
        continues: Vec<usize>,
        query_start: Option<usize>,
    },
}

struct QueryLoopState {
    slot: usize,
    values: Vec<Value>,
    next: usize,
}

pub fn compile(source: &str) -> Result<Program> {
    compile_with_limits(source, ResourceLimits::default())
}

pub fn compile_with_limits(source: &str, limits: ResourceLimits) -> Result<Program> {
    compile_with_arguments_and_limits(source, &[], limits)
}

pub fn compile_with_arguments(source: &str, argument_names: &[String]) -> Result<Program> {
    compile_with_arguments_and_limits(source, argument_names, ResourceLimits::default())
}

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
        });
    }
    let mut instructions = Vec::new();
    let mut controls = Vec::new();
    let mut declaring = false;
    let mut exception_handlers = Vec::new();
    let mut exception_skip = None;
    let mut exception_end_jumps = Vec::new();
    let mut protected_end = None;
    let mut in_exception = false;

    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let uppercase = trimmed.to_ascii_uppercase();
        if uppercase == "DECLARE" {
            if in_exception {
                return syntax_error("DECLARE is not valid inside an exception handler");
            }
            declaring = true;
            continue;
        }
        if uppercase == "BEGIN" {
            declaring = false;
            continue;
        }
        if uppercase == "EXCEPTION" {
            if in_exception || protected_end.is_some() {
                return syntax_error("only one top-level EXCEPTION block is supported");
            }
            if !controls.is_empty() {
                return syntax_error("EXCEPTION cannot begin inside an open control structure");
            }
            declaring = false;
            in_exception = true;
            protected_end = Some(instructions.len());
            let skip = instructions.len();
            instructions.push(Instruction::Jump { target: usize::MAX });
            exception_skip = Some(skip);
            continue;
        }
        if in_exception
            && controls.is_empty()
            && uppercase.starts_with("WHEN ")
            && uppercase.ends_with(" THEN")
        {
            let protected_end = protected_end
                .ok_or_else(|| DbError::internal("exception region lost its protected end"))?;
            if !exception_handlers.is_empty() {
                let jump = instructions.len();
                instructions.push(Instruction::Jump { target: usize::MAX });
                exception_end_jumps.push(jump);
            }
            let matcher_text = trimmed[5..trimmed.len() - 5].trim();
            let matcher = parse_exception_matcher(matcher_text)?;
            if exception_handlers
                .iter()
                .any(|handler: &ExceptionHandler| handler.matcher == ExceptionMatcher::Others)
            {
                return syntax_error("WHEN OTHERS must be the final exception handler");
            }
            exception_handlers.push(ExceptionHandler {
                protected_start: 0,
                protected_end,
                matcher,
                target: instructions.len(),
            });
            ensure_instruction_limit(&instructions, limits)?;
            continue;
        }
        if uppercase == "END" || uppercase == "END;" {
            continue;
        }
        if declaring {
            compile_declaration(trimmed, &mut locals, &mut local_names, &mut instructions)?;
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
                start,
                false_jump: Some(start),
                exits: Vec::new(),
                continues: Vec::new(),
                query_start: None,
            });
        } else if uppercase.starts_with("FOR ") && uppercase.ends_with(" LOOP") {
            ensure_nesting(&controls, limits)?;
            let rest = trimmed[4..trimmed.len() - 5].trim();
            let (target, query) = split_keyword(rest, "IN");
            let query = query.ok_or_else(|| {
                DbError::new("42601", "query FOR requires IN followed by a query")
            })?;
            if !query
                .trim_start()
                .get(.."SELECT".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SELECT"))
            {
                return unsupported_feature("only query SELECT FOR loops are supported");
            }
            let slot = *local_names
                .get(&target.trim().to_ascii_lowercase())
                .ok_or_else(|| {
                    DbError::new(
                        "42703",
                        format!("query FOR variable {} does not exist", target.trim()),
                    )
                })?;
            let start = instructions.len();
            instructions.push(Instruction::QueryForStart {
                slot,
                sql: rewrite_locals(query.trim(), &local_names),
                end: usize::MAX,
            });
            controls.push(ControlFrame::Loop {
                start,
                false_jump: None,
                exits: Vec::new(),
                continues: Vec::new(),
                query_start: Some(start),
            });
        } else if uppercase == "LOOP" {
            ensure_nesting(&controls, limits)?;
            controls.push(ControlFrame::Loop {
                start: instructions.len(),
                false_jump: None,
                exits: Vec::new(),
                continues: Vec::new(),
                query_start: None,
            });
        } else if uppercase == "EXIT" {
            let jump = instructions.len();
            instructions.push(Instruction::Jump { target: usize::MAX });
            let ControlFrame::Loop { exits, .. } = nearest_loop_mut(&mut controls)? else {
                return Err(DbError::internal("nearest loop is not a loop frame"));
            };
            exits.push(jump);
        } else if uppercase == "CONTINUE" {
            let frame = nearest_loop_mut(&mut controls)?;
            let ControlFrame::Loop {
                start,
                continues,
                query_start,
                ..
            } = frame
            else {
                return Err(DbError::internal("nearest loop is not a loop frame"));
            };
            let jump = instructions.len();
            instructions.push(Instruction::Jump {
                target: if query_start.is_some() {
                    usize::MAX
                } else {
                    *start
                },
            });
            if query_start.is_some() {
                continues.push(jump);
            }
        } else if uppercase == "END LOOP" {
            let frame = controls
                .pop()
                .ok_or_else(|| DbError::new("42601", "END LOOP has no matching LOOP"))?;
            let ControlFrame::Loop {
                start,
                false_jump,
                exits,
                continues,
                query_start,
            } = frame
            else {
                return syntax_error("END LOOP closes a non-loop control structure");
            };
            let continue_target = instructions.len();
            if let Some(query_start) = query_start {
                instructions.push(Instruction::QueryForNext {
                    start: query_start,
                    body: query_start + 1,
                });
            } else {
                instructions.push(Instruction::Jump { target: start });
            }
            let end = instructions.len();
            patch_target(&mut instructions, false_jump, end)?;
            if query_start.is_some() {
                patch_query_for_end(&mut instructions, start, end)?;
            }
            for jump in exits {
                patch_target(&mut instructions, Some(jump), end)?;
            }
            for jump in continues {
                patch_target(&mut instructions, Some(jump), continue_target)?;
            }
        } else {
            if in_exception && exception_handlers.is_empty() {
                return syntax_error("EXCEPTION requires WHEN before handler statements");
            }
            compile_statement(trimmed, &local_names, &mut instructions)?;
        }
        ensure_instruction_limit(&instructions, limits)?;
    }

    if !controls.is_empty() {
        return syntax_error("PL/pgSQL block has an unclosed control structure");
    }
    if in_exception && exception_handlers.is_empty() {
        return syntax_error("EXCEPTION requires at least one WHEN handler");
    }
    let end = instructions.len();
    patch_target(&mut instructions, exception_skip, end)?;
    for jump in exception_end_jumps {
        patch_target(&mut instructions, Some(jump), end)?;
    }
    instructions.push(Instruction::Checkpoint);
    ensure_instruction_limit(&instructions, limits)?;
    Ok(Program {
        version: BYTECODE_VERSION,
        instructions,
        locals,
        exception_handlers,
    })
}

fn compile_declaration(
    declaration: &str,
    locals: &mut Vec<LocalSlot>,
    names: &mut BTreeMap<String, usize>,
    instructions: &mut Vec<Instruction>,
) -> Result<()> {
    let (head, initializer) = declaration
        .split_once(":=")
        .map_or((declaration, None), |(head, value)| {
            (head, Some(value.trim()))
        });
    let mut parts = head.split_whitespace();
    let name = parts
        .next()
        .ok_or_else(|| DbError::new("42601", "variable declaration requires a name"))?;
    let constant = parts.any(|part| part.eq_ignore_ascii_case("CONSTANT"));
    let key = name.to_ascii_lowercase();
    if names.contains_key(&key) {
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
    instructions: &mut Vec<Instruction>,
) -> Result<()> {
    let uppercase = statement.to_ascii_uppercase();
    if uppercase.starts_with("RETURN NEXT ") {
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
            sql: rewrite_locals(statement[8..].trim(), locals),
            into: None,
        });
    } else if uppercase.starts_with("EXECUTE ") {
        let rest = statement[8..].trim();
        let (query, using) = split_keyword(rest, "USING");
        let using = using
            .map(|values| {
                values
                    .split(',')
                    .map(|value| rewrite_locals(value.trim(), locals))
                    .collect()
            })
            .unwrap_or_default();
        instructions.push(Instruction::DynamicExecute {
            query: rewrite_locals(query.trim(), locals),
            using,
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
    let mut instruction_pointer = 0usize;
    let mut steps = 0usize;
    let mut returned_rows = Vec::new();
    let mut query_loops = BTreeMap::<usize, QueryLoopState>::new();
    let exception_end = program
        .exception_handlers
        .first()
        .map(|handler| handler.protected_end);
    let mut exception_savepoint_active = exception_end.is_some();
    if exception_savepoint_active {
        host.begin_exception_block()?;
    }
    while instruction_pointer < program.instructions.len() {
        if exception_savepoint_active && exception_end.is_some_and(|end| instruction_pointer >= end)
        {
            host.commit_exception_block()?;
            exception_savepoint_active = false;
        }
        steps = steps.saturating_add(1);
        if steps > limits.max_steps {
            return limit_error("PL/pgSQL execution step limit exceeded");
        }
        host.check_cancelled()?;
        let step = (|| -> Result<Option<VmOutput>> {
            match &program.instructions[instruction_pointer] {
                Instruction::Assign { slot, expression } => {
                    if program
                        .locals
                        .get(*slot)
                        .is_some_and(|local| local.constant && !locals[*slot].is_null())
                    {
                        return Err(DbError::new(
                            "22005",
                            format!(
                                "constant {} cannot be reassigned",
                                program.locals[*slot].name
                            ),
                        ));
                    }
                    locals[*slot] = host.evaluate_expression(expression, &locals)?;
                    instruction_pointer += 1;
                }
                Instruction::AssignField {
                    slot,
                    field,
                    expression,
                } => {
                    let value = host.evaluate_expression(expression, &locals)?;
                    host.assign_composite_field(*slot, field, value)?;
                    instruction_pointer += 1;
                }
                Instruction::JumpIfFalse { expression, target } => {
                    let value = host.evaluate_expression(expression, &locals)?;
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
                    let mut first = None;
                    for event in host.execute_sql(sql, &locals)? {
                        if let QueryEvent::Batch(batch) = event?
                            && first.is_none()
                        {
                            first = batch
                                .rows
                                .first()
                                .and_then(|row| row.values.first())
                                .cloned();
                        }
                    }
                    if let Some(slot) = into {
                        locals[*slot] = first.unwrap_or(Value::Null);
                    }
                    instruction_pointer += 1;
                }
                Instruction::DynamicExecute { query, using } => {
                    let query = host.evaluate_expression(query, &locals)?;
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
                        .map(|expression| host.evaluate_expression(expression, &locals))
                        .collect::<Result<Vec<_>>>()?;
                    for event in host.execute_sql(&query, &parameters)? {
                        event?;
                    }
                    instruction_pointer += 1;
                }
                Instruction::QueryForStart { slot, sql, end } => {
                    let mut values = Vec::new();
                    for event in host.execute_sql(sql, &locals)? {
                        if let QueryEvent::Batch(batch) = event? {
                            for row in batch.rows {
                                values.push(row.values.into_iter().next().unwrap_or(Value::Null));
                                if values.len() > limits.max_returned_rows {
                                    return limit_error("PL/pgSQL query FOR row limit exceeded");
                                }
                            }
                        }
                    }
                    if values.is_empty() {
                        instruction_pointer = checked_target(*end, program.instructions.len())?;
                    } else {
                        locals[*slot] = values[0].clone();
                        query_loops.insert(
                            instruction_pointer,
                            QueryLoopState {
                                slot: *slot,
                                values,
                                next: 1,
                            },
                        );
                        instruction_pointer += 1;
                    }
                }
                Instruction::QueryForNext { start, body } => {
                    let Some(state) = query_loops.get_mut(start) else {
                        return Err(DbError::internal(
                            "PL/pgSQL query FOR iterator state is missing",
                        ));
                    };
                    if let Some(value) = state.values.get(state.next).cloned() {
                        locals[state.slot] = value;
                        state.next = state.next.saturating_add(1);
                        instruction_pointer = checked_target(*body, program.instructions.len())?;
                    } else {
                        query_loops.remove(start);
                        instruction_pointer += 1;
                    }
                }
                Instruction::Return { expression, next } => {
                    let value = expression
                        .as_ref()
                        .map(|expression| host.evaluate_expression(expression, &locals))
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
                        }));
                    }
                }
                Instruction::Checkpoint => {
                    instruction_pointer += 1;
                }
            }
            Ok(None)
        })();
        match step {
            Ok(Some(output)) => {
                if exception_savepoint_active {
                    host.commit_exception_block()?;
                }
                return Ok(output);
            }
            Ok(None) => {}
            Err(error) => {
                let handler = program.exception_handlers.iter().find(|handler| {
                    handler.protected_start <= instruction_pointer
                        && instruction_pointer < handler.protected_end
                        && match &handler.matcher {
                            ExceptionMatcher::SqlState(state) => {
                                state.eq_ignore_ascii_case(&error.sql_state)
                            }
                            ExceptionMatcher::Others => {
                                !matches!(error.sql_state.as_str(), "57014" | "P0004")
                            }
                        }
                });
                let Some(handler) = handler else {
                    return Err(error);
                };
                if exception_savepoint_active {
                    host.rollback_exception_block()?;
                    exception_savepoint_active = false;
                }
                query_loops.clear();
                instruction_pointer = checked_target(handler.target, program.instructions.len())?;
            }
        }
    }
    if exception_savepoint_active {
        host.commit_exception_block()?;
    }
    Ok(VmOutput {
        return_value: None,
        returned_rows,
        return_parameter: None,
    })
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
            None if matches!(character, ';' | '\n' | '\r') => {
                push_logical_segment(&mut lines, &current);
                current.clear();
            }
            None => current.push(character),
        }
    }
    if quote.is_some() {
        return syntax_error("unterminated quoted string in PL/pgSQL source");
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
    let uppercase = value.to_ascii_uppercase();
    let needle = format!(" {keyword} ");
    uppercase.find(&needle).map_or((value, None), |position| {
        (&value[..position], Some(&value[position + needle.len()..]))
    })
}

fn parse_exception_matcher(value: &str) -> Result<ExceptionMatcher> {
    if value.eq_ignore_ascii_case("OTHERS") {
        return Ok(ExceptionMatcher::Others);
    }
    let state = value
        .get(8..)
        .filter(|_| {
            value
                .get(.."SQLSTATE".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SQLSTATE"))
        })
        .map(str::trim)
        .and_then(|value| value.strip_prefix('\''))
        .and_then(|value| value.strip_suffix('\''))
        .ok_or_else(|| {
            DbError::new(
                "42601",
                "exception handlers support WHEN SQLSTATE 'xxxxx' or WHEN OTHERS",
            )
        })?;
    if state.len() != 5 || !state.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return syntax_error("exception SQLSTATE must contain five ASCII letters or digits");
    }
    Ok(ExceptionMatcher::SqlState(state.to_ascii_uppercase()))
}

fn nearest_loop_mut(controls: &mut [ControlFrame]) -> Result<&mut ControlFrame> {
    for frame in controls.iter_mut().rev() {
        if matches!(frame, ControlFrame::Loop { .. }) {
            return Ok(frame);
        }
    }
    syntax_error("loop control statement is outside a loop")
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

fn ensure_nesting(controls: &[ControlFrame], limits: ResourceLimits) -> Result<()> {
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
    use super::*;
    use ordadb_types::{Batch, Row, Schema};

    struct Host {
        cancelled: bool,
    }

    impl PlpgsqlHost for Host {
        fn execute_sql(
            &mut self,
            sql: &str,
            _parameters: &[Value],
        ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>> + '_>> {
            let events = if sql == "FAIL" {
                vec![Err(DbError::new("23505", "test unique violation"))]
            } else if sql == "SELECT many" {
                vec![Ok(QueryEvent::Batch(Batch {
                    schema: Schema::empty(),
                    rows: vec![
                        Row::new(vec![Value::Int64(1)]),
                        Row::new(vec![Value::Int64(2)]),
                    ],
                }))]
            } else if sql.starts_with("SELECT") {
                vec![Ok(QueryEvent::Batch(Batch {
                    schema: Schema::empty(),
                    rows: vec![Row::new(vec![Value::Int64(9)])],
                }))]
            } else {
                Vec::new()
            };
            Ok(Box::new(events.into_iter()))
        }

        fn evaluate_expression(&mut self, sql: &str, parameters: &[Value]) -> Result<Value> {
            match sql.trim() {
                "TRUE" | "true" => Ok(Value::Boolean(true)),
                "FALSE" | "false" => Ok(Value::Boolean(false)),
                "$1" => Ok(parameters.first().cloned().unwrap_or(Value::Null)),
                "$2" => Ok(parameters.get(1).cloned().unwrap_or(Value::Null)),
                "($1) = (1)" => Ok(Value::Boolean(parameters.first() == Some(&Value::Int64(1)))),
                "($1) = (2)" => Ok(Value::Boolean(parameters.first() == Some(&Value::Int64(2)))),
                "1" => Ok(Value::Int64(1)),
                "2" => Ok(Value::Int64(2)),
                "3" => Ok(Value::Int64(3)),
                "'SELECT 1'" => Ok(Value::Text("SELECT 1".into())),
                other => Err(DbError::new(
                    "0A000",
                    format!("test host cannot evaluate {other}"),
                )),
            }
        }

        fn check_cancelled(&self) -> Result<()> {
            if self.cancelled {
                Err(DbError::new("57014", "query was cancelled"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn compiles_and_executes_explicit_control_flow() {
        let program = compile(
            "DECLARE
             answer BIGINT := 1;
             BEGIN
             IF true THEN
             answer := 2;
             ELSE
             answer := 1;
             END IF;
             RETURN answer;
             END;",
        )
        .expect("compile");
        let output = execute(&program, &mut Host { cancelled: false }, &[]).expect("execute");
        assert_eq!(output.return_value, Some(Value::Int64(2)));
    }

    #[test]
    fn compiles_block_introducers_without_line_breaks() {
        let program =
            compile("DECLARE answer BIGINT := 1; BEGIN RETURN answer; END;").expect("compile");
        let output = execute(&program, &mut Host { cancelled: false }, &[]).expect("execute");
        assert_eq!(output.return_value, Some(Value::Int64(1)));
    }

    #[test]
    fn select_into_dynamic_using_and_limits_are_bounded() {
        let program = compile(
            "DECLARE
             value BIGINT;
             BEGIN
             SELECT 9 INTO value;
             EXECUTE 'SELECT 1' USING value;
             RETURN NEXT value;
             RETURN;
             END;",
        )
        .expect("compile");
        let output = execute(&program, &mut Host { cancelled: false }, &[]).expect("execute");
        assert_eq!(output.returned_rows, vec![Value::Int64(9)]);

        let limits = ResourceLimits {
            max_source_bytes: 4,
            ..ResourceLimits::default()
        };
        assert_eq!(
            compile_with_limits("BEGIN RETURN; END;", limits)
                .expect_err("source limit")
                .sql_state,
            "54001"
        );
        let _ = Schema::empty();
    }

    #[test]
    fn false_branches_case_query_for_and_exception_handlers_are_explicit() {
        let false_branch = compile(
            "BEGIN
             IF false THEN
             RETURN 1;
             ELSE
             RETURN 2;
             END IF;
             END;",
        )
        .expect("compile false branch");
        assert_eq!(
            execute(&false_branch, &mut Host { cancelled: false }, &[])
                .expect("execute false branch")
                .return_value,
            Some(Value::Int64(2))
        );

        let query_for = compile(
            "DECLARE
             item BIGINT;
             answer BIGINT := 3;
             BEGIN
             FOR item IN SELECT many LOOP
             CASE item
             WHEN 1 THEN
             answer := 1;
             WHEN 2 THEN
             answer := 2;
             ELSE
             answer := 3;
             END CASE;
             END LOOP;
             RETURN answer;
             END;",
        )
        .expect("compile query for");
        assert_eq!(
            execute(&query_for, &mut Host { cancelled: false }, &[])
                .expect("execute query for")
                .return_value,
            Some(Value::Int64(2))
        );

        let exception = compile(
            "BEGIN
             FAIL;
             EXCEPTION
             WHEN SQLSTATE '23505' THEN
             RETURN 2;
             WHEN OTHERS THEN
             RETURN 3;
             END;",
        )
        .expect("compile exception");
        assert_eq!(
            execute(&exception, &mut Host { cancelled: false }, &[])
                .expect("execute exception")
                .return_value,
            Some(Value::Int64(2))
        );
        assert_eq!(
            execute(&exception, &mut Host { cancelled: true }, &[])
                .expect_err("cancellation bypasses exception handlers")
                .sql_state,
            "57014"
        );
    }

    #[test]
    fn step_dynamic_sql_query_loop_and_returned_row_limits_fail_explicitly() {
        let loop_program = compile(
            "BEGIN
             LOOP
             CONTINUE;
             END LOOP;
             END;",
        )
        .expect("compile loop program");
        let step_error = execute_with_limits(
            &loop_program,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_steps: 4,
                ..ResourceLimits::default()
            },
        )
        .expect_err("step limit");
        assert_eq!(step_error.sql_state, "54001");

        let dynamic_program = compile(
            "BEGIN
             EXECUTE 'SELECT 1';
             END;",
        )
        .expect("compile dynamic SQL");
        let dynamic_error = execute_with_limits(
            &dynamic_program,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_dynamic_sql_bytes: 4,
                ..ResourceLimits::default()
            },
        )
        .expect_err("dynamic SQL byte limit");
        assert_eq!(dynamic_error.sql_state, "54001");

        let query_loop = compile(
            "DECLARE
             item BIGINT;
             BEGIN
             FOR item IN SELECT many LOOP
             CONTINUE;
             END LOOP;
             END;",
        )
        .expect("compile query loop");
        let query_loop_error = execute_with_limits(
            &query_loop,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_returned_rows: 1,
                ..ResourceLimits::default()
            },
        )
        .expect_err("query loop row limit");
        assert_eq!(query_loop_error.sql_state, "54001");

        let returns = compile(
            "BEGIN
             RETURN NEXT 1;
             RETURN NEXT 2;
             RETURN;",
        )
        .expect("compile returned rows");
        let return_error = execute_with_limits(
            &returns,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_returned_rows: 1,
                ..ResourceLimits::default()
            },
        )
        .expect_err("returned row limit");
        assert_eq!(return_error.sql_state, "54001");
    }
}
