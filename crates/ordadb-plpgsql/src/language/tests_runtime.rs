use super::*;
use ordadb_types::{Batch, Field, PgArray, Row, ScalarType, Schema};

struct Host {
    cancelled: bool,
}

fn row_pair_schema(second_field: &str) -> Schema {
    Schema::new(vec![
        Field::new("id", ScalarType::Int64, false),
        Field::new(second_field, ScalarType::Text, false),
    ])
}

impl PlpgsqlHost for Host {
    fn execute_sql(
        &mut self,
        sql: &str,
        _parameters: &[Value],
    ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>>>> {
        let events = if sql == "FAIL" {
            vec![Err(DbError::new("23505", "test unique violation"))]
        } else if sql == "SELECT none" {
            Vec::new()
        } else if matches!(
            sql,
            "SELECT id, name FROM row_pair" | "SELECT id, label FROM wrong_pair"
        ) {
            let second_field = if sql.contains("wrong_pair") {
                "label"
            } else {
                "name"
            };
            let schema = row_pair_schema(second_field);
            vec![
                Ok(QueryEvent::Schema(schema.clone())),
                Ok(QueryEvent::Batch(Batch {
                    schema,
                    rows: vec![Row::new(vec![Value::Int64(7), Value::Text("seven".into())])],
                })),
            ]
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
            "$1 = 2" => Ok(Value::Boolean(parameters.first() == Some(&Value::Int64(2)))),
            "$1 = 1" => Ok(Value::Boolean(parameters.first() == Some(&Value::Int64(1)))),
            "$1 = 4" => Ok(Value::Boolean(parameters.first() == Some(&Value::Int64(4)))),
            "0" => Ok(Value::Int64(0)),
            "1" => Ok(Value::Int64(1)),
            "2" => Ok(Value::Int64(2)),
            "3" => Ok(Value::Int64(3)),
            "4" => Ok(Value::Int64(4)),
            "5" => Ok(Value::Int64(5)),
            other if other.parse::<i64>().is_ok() => {
                Ok(Value::Int64(other.parse::<i64>().map_err(|_| {
                    DbError::new("22003", "integer literal is out of range")
                })?))
            }
            "ARRAY_TEST" => Ok(Value::Array(PgArray::one_dimensional(
                ScalarType::Int64,
                vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
            )?)),
            "'SELECT 1'" => Ok(Value::Text("SELECT 1".into())),
            other if other.starts_with('\'') && other.ends_with('\'') && other.len() >= 2 => {
                Ok(Value::Text(other[1..other.len() - 1].to_owned()))
            }
            other if other.starts_with('$') => {
                let index = other[1..]
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| index.checked_sub(1))
                    .ok_or_else(|| DbError::new("42P02", "invalid positional parameter"))?;
                Ok(parameters.get(index).cloned().unwrap_or(Value::Null))
            }
            other => Err(DbError::new(
                "0A000",
                format!("test host cannot evaluate {other}"),
            )),
        }
    }

    fn resolve_row_type(&mut self, relation: &str) -> Result<Vec<String>> {
        if relation.eq_ignore_ascii_case("public.items") {
            Ok(vec!["id".into(), "name".into()])
        } else {
            Err(DbError::new(
                "42P01",
                format!("relation {relation} does not exist"),
            ))
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
fn resumable_vm_yields_sql_and_preserves_exception_state() {
    let program = compile(
        "BEGIN
         BEGIN
         FAIL;
         EXCEPTION
         WHEN SQLSTATE '23505' THEN
         RETURN 4;
         END;
         END;",
    )
    .expect("compile");
    let mut host = Host { cancelled: false };
    let mut machine =
        VmMachine::new(&program, &mut host, &[], ResourceLimits::default()).expect("create VM");
    let VmRunState::Sql(request) = machine.resume(&mut host, None).expect("yield SQL") else {
        panic!("expected SQL yield");
    };
    assert_eq!(request.sql, "FAIL");
    let response = host.execute_sql(&request.sql, &request.parameters);
    let VmRunState::Complete(output) = machine
        .resume(&mut host, Some(response))
        .expect("resume through exception handler")
    else {
        panic!("expected completion");
    };
    assert_eq!(output.return_value, Some(Value::Int64(4)));
    assert_eq!(
        machine
            .resume(&mut host, None)
            .expect_err("completed VM cannot resume")
            .sql_state,
        "55000"
    );
}

#[test]
fn compiles_block_introducers_without_line_breaks() {
    let program =
        compile("DECLARE answer BIGINT := 1; BEGIN RETURN answer; END;").expect("compile");
    let output = execute(&program, &mut Host { cancelled: false }, &[]).expect("execute");
    assert_eq!(output.return_value, Some(Value::Int64(1)));
}

#[test]
fn multiline_sql_inside_parentheses_is_one_instruction() {
    let program = compile_with_arguments(
        "BEGIN
         INSERT INTO audit VALUES (
           tg_op,
           tg_name
         );
         RETURN NULL;
         END;",
        &["tg_op".into(), "tg_name".into()],
    )
    .expect("compile multiline SQL");
    let sql = program
        .instructions
        .iter()
        .find_map(|instruction| match instruction {
            Instruction::ExecuteSql { sql, into: None } => Some(sql.as_str()),
            _ => None,
        })
        .expect("SQL instruction");
    assert!(sql.starts_with("INSERT INTO audit VALUES ("), "{sql}");
    assert!(sql.contains("$1"), "{sql}");
    assert!(sql.contains("$2"), "{sql}");
}

#[test]
fn perform_compiles_expression_as_a_select_statement() {
    let program = compile_with_arguments(
        "BEGIN PERFORM pg_notify('core_events', event_payload); END;",
        &["event_payload".into()],
    )
    .expect("compile PERFORM");
    let sql = program
        .instructions
        .iter()
        .find_map(|instruction| match instruction {
            Instruction::ExecuteSql { sql, into: None } => Some(sql.as_str()),
            _ => None,
        })
        .expect("PERFORM SQL instruction");
    assert_eq!(sql, "SELECT pg_notify('core_events', $1)");
}

#[test]
fn select_into_dynamic_using_and_limits_are_bounded() {
    let program = compile(
        "DECLARE
         value BIGINT;
         BEGIN
         SELECT 9 INTO value;
         EXECUTE 'SELECT 1' INTO STRICT value USING value;
         RETURN NEXT value;
         RETURN;
         END;",
    )
    .expect("compile");
    let output = execute(&program, &mut Host { cancelled: false }, &[]).expect("execute");
    assert_eq!(output.returned_rows, vec![Value::Int64(9)]);

    let no_rows = compile(
        "DECLARE value BIGINT;
         BEGIN
         EXECUTE 'SELECT none' INTO STRICT value;
         RETURN value;
         END;",
    )
    .expect("compile no-row strict execute");
    assert_eq!(
        execute(&no_rows, &mut Host { cancelled: false }, &[])
            .expect_err("strict no rows")
            .sql_state,
        "P0002"
    );

    let many_rows = compile(
        "DECLARE value BIGINT;
         BEGIN
         EXECUTE 'SELECT many' INTO STRICT value;
         RETURN value;
         END;",
    )
    .expect("compile many-row strict execute");
    assert_eq!(
        execute(&many_rows, &mut Host { cancelled: false }, &[])
            .expect_err("strict many rows")
            .sql_state,
        "P0003"
    );

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
fn integer_for_reverse_by_and_conditional_loop_control_are_explicit() {
    let forward = compile(
        "DECLARE
         item BIGINT;
         answer BIGINT;
         BEGIN
         FOR item IN 1..5 BY 1 LOOP
         CONTINUE WHEN item = 2;
         EXIT WHEN item = 4;
         answer := item;
         END LOOP;
         RETURN answer;
         END;",
    )
    .expect("compile integer FOR");
    assert_eq!(
        execute(&forward, &mut Host { cancelled: false }, &[])
            .expect("execute integer FOR")
            .return_value,
        Some(Value::Int64(3))
    );

    let reverse = compile(
        "DECLARE
         item BIGINT;
         answer BIGINT;
         BEGIN
         FOR item IN REVERSE 3..1 BY 1 LOOP
         answer := item;
         END LOOP;
         RETURN answer;
         END;",
    )
    .expect("compile reverse integer FOR");
    assert_eq!(
        execute(&reverse, &mut Host { cancelled: false }, &[])
            .expect("execute reverse integer FOR")
            .return_value,
        Some(Value::Int64(1))
    );

    let invalid_step = compile(
        "DECLARE item BIGINT;
         BEGIN
         FOR item IN 1..3 BY 0 LOOP
         RETURN item;
         END LOOP;
         END;",
    )
    .expect("compile invalid step");
    assert_eq!(
        execute(&invalid_step, &mut Host { cancelled: false }, &[])
            .expect_err("zero step")
            .sql_state,
        "22023"
    );
}

#[test]
fn labeled_nested_loops_patch_exit_and_continue_to_the_named_frame() {
    let program = compile(
        "DECLARE
         outer_value BIGINT;
         inner_value BIGINT;
         answer BIGINT := 0;
         BEGIN
         <<outer_loop>>
         FOR outer_value IN 1..3 LOOP
         <<inner_loop>>
         FOR inner_value IN 1..3 LOOP
         CONTINUE outer_loop WHEN outer_value = 1;
         EXIT outer_loop WHEN outer_value = 2;
         answer := 99;
         END LOOP inner_loop;
         END LOOP outer_loop;
         RETURN outer_value;
         END;",
    )
    .expect("compile labeled loops");
    assert_eq!(
        execute(&program, &mut Host { cancelled: false }, &[])
            .expect("execute labeled loops")
            .return_value,
        Some(Value::Int64(2))
    );

    assert_eq!(
        compile(
            "BEGIN
             <<actual_loop>>
             LOOP
             EXIT;
             END LOOP wrong_loop;
             END;",
        )
        .expect_err("mismatched closing label")
        .sql_state,
        "42601"
    );
    assert_eq!(
        compile(
            "BEGIN
             LOOP
             EXIT missing_loop;
             END LOOP;
             END;",
        )
        .expect_err("missing loop label")
        .sql_state,
        "42601"
    );
}

#[test]
fn labeled_blocks_patch_exit_and_validate_closing_labels() {
    let program = compile(
        "<<outer_block>>
         DECLARE
         answer BIGINT := 1;
         BEGIN
         <<inner_block>>
         BEGIN
         answer := 2;
         EXIT outer_block WHEN true;
         answer := 99;
         END inner_block;
         answer := 100;
         END outer_block;",
    )
    .expect("compile labeled blocks");
    let output =
        execute(&program, &mut Host { cancelled: false }, &[]).expect("execute labeled blocks");
    assert_eq!(output.final_locals.first(), Some(&Value::Int64(2)));

    assert_eq!(
        compile(
            "<<actual_block>>
             BEGIN
             END wrong_block;",
        )
        .expect_err("mismatched block label")
        .sql_state,
        "42601"
    );
    assert_eq!(
        compile(
            "<<actual_block>>
             BEGIN
             CONTINUE actual_block;
             END actual_block;",
        )
        .expect_err("block label cannot be a continue target")
        .sql_state,
        "42601"
    );
}

#[test]
fn nested_declare_blocks_restore_outer_variable_bindings() {
    let program = compile(
        "DECLARE
         scoped_value BIGINT := 1;
         BEGIN
         DECLARE
         scoped_value BIGINT := 2;
         BEGIN
         scoped_value := 3;
         RETURN NEXT scoped_value;
         END;
         RETURN scoped_value;
         END;",
    )
    .expect("compile nested declaration scope");
    let output = execute(&program, &mut Host { cancelled: false }, &[])
        .expect("execute nested declaration scope");
    assert_eq!(output.returned_rows, vec![Value::Int64(3)]);
    assert_eq!(output.return_value, Some(Value::Int64(1)));

    assert_eq!(
        compile(
            "BEGIN
             DECLARE
             duplicate_value BIGINT;
             duplicate_value BIGINT;
             BEGIN
             RETURN;
             END;
             END;",
        )
        .expect_err("duplicate nested declaration")
        .sql_state,
        "42710"
    );
}

#[test]
fn foreach_array_uses_owned_iterator_state_and_rejects_non_arrays() {
    let foreach = compile(
        "DECLARE
         item BIGINT;
         answer BIGINT;
         BEGIN
         FOREACH item IN ARRAY ARRAY_TEST LOOP
         answer := item;
         END LOOP;
         RETURN answer;
         END;",
    )
    .expect("compile FOREACH");
    assert_eq!(
        execute(&foreach, &mut Host { cancelled: false }, &[])
            .expect("execute FOREACH")
            .return_value,
        Some(Value::Int64(3))
    );

    let non_array = compile(
        "DECLARE item BIGINT;
         BEGIN
         FOREACH item IN ARRAY 1 LOOP
         RETURN item;
         END LOOP;
         END;",
    )
    .expect("compile non-array FOREACH");
    assert_eq!(
        execute(&non_array, &mut Host { cancelled: false }, &[])
            .expect_err("non-array FOREACH")
            .sql_state,
        "42804"
    );
}

#[test]
fn raise_assert_and_handler_diagnostics_preserve_sqlstate() {
    let diagnostics = compile(
        "BEGIN
         RAISE EXCEPTION 'duplicate' USING ERRCODE = '23505';
         EXCEPTION
         WHEN unique_violation THEN
         RETURN sqlerrm;
         END;",
    )
    .expect("compile named exception handler");
    assert_eq!(
        execute(&diagnostics, &mut Host { cancelled: false }, &[])
            .expect("handle raised exception")
            .return_value,
        Some(Value::Text("duplicate".into()))
    );

    let sqlstate = compile(
        "BEGIN
         FAIL;
         EXCEPTION
         WHEN SQLSTATE '23505' THEN
         RETURN sqlstate;
         END;",
    )
    .expect("compile SQLSTATE diagnostic");
    assert_eq!(
        execute(&sqlstate, &mut Host { cancelled: false }, &[])
            .expect("read SQLSTATE")
            .return_value,
        Some(Value::Text("23505".into()))
    );

    let rethrow = compile(
        "BEGIN
         FAIL;
         EXCEPTION
         WHEN unique_violation THEN
         RAISE;
         END;",
    )
    .expect("compile rethrow");
    assert_eq!(
        execute(&rethrow, &mut Host { cancelled: false }, &[])
            .expect_err("rethrow active exception")
            .sql_state,
        "23505"
    );

    let assertion = compile(
        "BEGIN
         ASSERT false, 'invariant failed';
         EXCEPTION
         WHEN OTHERS THEN
         RETURN 1;
         END;",
    )
    .expect("compile assertion");
    let error = execute(&assertion, &mut Host { cancelled: false }, &[])
        .expect_err("OTHERS does not catch assertion failures");
    assert_eq!(error.sql_state, "P0004");
    assert_eq!(error.message, "invariant failed");
}

#[test]
fn nested_exception_blocks_select_the_innermost_matching_handler() {
    let outer_fallback = compile(
        "BEGIN
         BEGIN
         RAISE EXCEPTION 'inner' USING ERRCODE = '23505';
         EXCEPTION
         WHEN division_by_zero THEN
         RETURN 1;
         END;
         EXCEPTION
         WHEN unique_violation THEN
         RETURN sqlstate;
         END;",
    )
    .expect("compile nested outer fallback");
    assert_eq!(
        execute(&outer_fallback, &mut Host { cancelled: false }, &[])
            .expect("outer handler")
            .return_value,
        Some(Value::Text("23505".into()))
    );

    let inner_match = compile(
        "DECLARE answer BIGINT;
         BEGIN
         BEGIN
         RAISE EXCEPTION 'inner' USING ERRCODE = '23505';
         EXCEPTION
         WHEN unique_violation THEN
         answer := 2;
         END;
         RETURN answer;
         END;",
    )
    .expect("compile nested inner match");
    assert_eq!(
        execute(&inner_match, &mut Host { cancelled: false }, &[])
            .expect("inner handler")
            .return_value,
        Some(Value::Int64(2))
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

    let early_exit = compile(
        "DECLARE
         item BIGINT;
         BEGIN
         FOR item IN SELECT many LOOP
         EXIT;
         END LOOP;
         RETURN item;
         END;",
    )
    .expect("compile early-exit query loop");
    let early_exit_output = execute_with_limits(
        &early_exit,
        &mut Host { cancelled: false },
        &[],
        ResourceLimits {
            max_returned_rows: 1,
            ..ResourceLimits::default()
        },
    )
    .expect("early exit does not drain the query cursor");
    assert_eq!(early_exit_output.return_value, Some(Value::Int64(1)));

    let returns = compile(
        "BEGIN
         RETURN NEXT 1;
         RETURN NEXT 2;
         RETURN;
         END;",
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

#[test]
fn declared_cursor_supports_every_direction_with_bounded_owned_rows() {
    let program = compile(
        "DECLARE
         values_cursor SCROLL CURSOR FOR SELECT many;
         item BIGINT;
         BEGIN
         OPEN values_cursor;
         FETCH NEXT FROM values_cursor INTO item;
         RETURN NEXT item;
         FETCH LAST FROM values_cursor INTO item;
         RETURN NEXT item;
         FETCH PRIOR FROM values_cursor INTO item;
         RETURN NEXT item;
         FETCH FIRST FROM values_cursor INTO item;
         RETURN NEXT item;
         FETCH ABSOLUTE 2 FROM values_cursor INTO item;
         RETURN NEXT item;
         FETCH RELATIVE -1 FROM values_cursor INTO item;
         RETURN NEXT item;
         MOVE FORWARD 1 FROM values_cursor;
         FETCH BACKWARD 1 FROM values_cursor INTO item;
         RETURN NEXT item;
         MOVE FORWARD ALL FROM values_cursor;
         FETCH PRIOR FROM values_cursor INTO item;
         RETURN NEXT item;
         CLOSE values_cursor;
         RETURN;
         END;",
    )
    .expect("compile directional cursor");

    let output = execute(&program, &mut Host { cancelled: false }, &[])
        .expect("execute directional cursor");
    assert_eq!(
        output.returned_rows,
        vec![1, 2, 1, 1, 2, 1, 1, 2]
            .into_iter()
            .map(Value::Int64)
            .collect::<Vec<_>>()
    );
}

#[test]
fn unbound_cursor_opens_static_and_dynamic_queries_and_enforces_limits() {
    let program = compile(
        "DECLARE
         values_cursor REFCURSOR;
         item BIGINT;
         query_text TEXT := 'SELECT many';
         BEGIN
         OPEN values_cursor FOR EXECUTE query_text USING 1;
         FETCH FIRST FROM values_cursor INTO item;
         RETURN NEXT item;
         CLOSE values_cursor;
         OPEN values_cursor FOR SELECT many;
         FETCH LAST FROM values_cursor INTO item;
         RETURN NEXT item;
         CLOSE values_cursor;
         RETURN;
         END;",
    )
    .expect("compile unbound cursor");
    let output =
        execute(&program, &mut Host { cancelled: false }, &[]).expect("execute unbound cursor");
    assert_eq!(output.returned_rows, vec![Value::Int64(1), Value::Int64(2)]);

    let too_many_open = compile(
        "DECLARE
         first_cursor CURSOR FOR SELECT many;
         second_cursor CURSOR FOR SELECT many;
         BEGIN
         OPEN first_cursor;
         OPEN second_cursor;
         END;",
    )
    .expect("compile open cursor limit");
    assert_eq!(
        execute_with_limits(
            &too_many_open,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_open_cursors: 1,
                ..ResourceLimits::default()
            },
        )
        .expect_err("open cursor limit")
        .sql_state,
        "54000"
    );

    let retained_memory = compile(
        "DECLARE
         values_cursor CURSOR FOR SELECT many;
         item BIGINT;
         BEGIN
         OPEN values_cursor;
         FETCH NEXT FROM values_cursor INTO item;
         END;",
    )
    .expect("compile cursor memory limit");
    assert_eq!(
        execute_with_limits(
            &retained_memory,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_cursor_bytes: 1,
                ..ResourceLimits::default()
            },
        )
        .expect_err("cursor retained-memory limit")
        .sql_state,
        "53200"
    );
}

#[test]
fn directional_cursor_spills_and_removes_its_owned_page_store() {
    let limits = ResourceLimits {
        max_cursor_bytes: 256,
        ..ResourceLimits::default()
    };
    let mut store = CursorPageStore::Memory {
        rows: Vec::new(),
        bytes: 0,
    };
    let first = ordadb_types::Row::new(vec![Value::Text("a".repeat(64))]);
    let second = ordadb_types::Row::new(vec![Value::Text("b".repeat(64))]);
    store.push(first.clone(), limits).expect("first value");
    store.push(second.clone(), limits).expect("spill value");
    let spill_path = match &store {
        CursorPageStore::Spilled(spill) => spill.file.path().to_path_buf(),
        CursorPageStore::Memory { .. } => panic!("cursor did not spill"),
    };
    assert_eq!(store.get(0, limits).expect("first read"), Some(first));
    assert_eq!(store.get(1, limits).expect("second read"), Some(second));
    assert!(spill_path.exists());
    drop(store);
    assert!(!spill_path.exists());
}

#[test]
fn record_and_rowtype_rows_flow_through_select_loops_and_cursors() {
    let record = compile(
        "DECLARE
         source RECORD;
         copied RECORD;
         BEGIN
         SELECT id, name INTO source FROM row_pair;
         source.name := 'updated';
         copied := source;
         RETURN copied.name;
         END;",
    )
    .expect("compile record assignment");
    assert_eq!(
        execute(&record, &mut Host { cancelled: false }, &[])
            .expect("execute record assignment")
            .return_value,
        Some(Value::Text("updated".into()))
    );

    let query_loop = compile(
        "DECLARE
         item RECORD;
         answer TEXT;
         BEGIN
         FOR item IN SELECT id, name FROM row_pair LOOP
         answer := item.name;
         END LOOP;
         RETURN answer;
         END;",
    )
    .expect("compile record query loop");
    assert_eq!(
        execute(&query_loop, &mut Host { cancelled: false }, &[])
            .expect("execute record query loop")
            .return_value,
        Some(Value::Text("seven".into()))
    );

    let rowtype_cursor = compile(
        "DECLARE
         values_cursor CURSOR FOR SELECT id, name FROM row_pair;
         item public.items%ROWTYPE;
         BEGIN
         OPEN values_cursor;
         FETCH NEXT FROM values_cursor INTO item;
         CLOSE values_cursor;
         RETURN item.name;
         END;",
    )
    .expect("compile rowtype cursor");
    assert_eq!(
        execute(&rowtype_cursor, &mut Host { cancelled: false }, &[])
            .expect("execute rowtype cursor")
            .return_value,
        Some(Value::Text("seven".into()))
    );

    let unassigned = compile(
        "DECLARE
         item RECORD;
         BEGIN
         RETURN item.id;
         END;",
    )
    .expect("compile unassigned record");
    assert_eq!(
        execute(&unassigned, &mut Host { cancelled: false }, &[])
            .expect_err("unassigned record field")
            .sql_state,
        "55000"
    );

    let mismatched = compile(
        "DECLARE
         item public.items%ROWTYPE;
         BEGIN
         SELECT id, label INTO item FROM wrong_pair;
         END;",
    )
    .expect("compile mismatched rowtype");
    assert_eq!(
        execute(&mismatched, &mut Host { cancelled: false }, &[])
            .expect_err("rowtype field mismatch")
            .sql_state,
        "42804"
    );

    assert_eq!(
        execute_with_limits(
            &record,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_cursor_bytes: 1,
                ..ResourceLimits::default()
            },
        )
        .expect_err("record retained-memory limit")
        .sql_state,
        "53200"
    );
}
