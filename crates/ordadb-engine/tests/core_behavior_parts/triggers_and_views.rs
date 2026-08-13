
#[test]
fn statement_triggers_cover_on_conflict_and_merge_action_events() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE TABLE trigger_target (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)",
    );
    execute(
        &mut session,
        "CREATE TABLE trigger_source (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)",
    );
    execute(
        &mut session,
        "CREATE TABLE trigger_audit (
            operation TEXT NOT NULL,
            timing TEXT NOT NULL,
            trigger_name TEXT NOT NULL
         )",
    );
    execute(
        &mut session,
        "CREATE FUNCTION capture_statement_action() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO trigger_audit VALUES (TG_OP, TG_WHEN, TG_NAME);
           RETURN NULL;
         END
         $$",
    );
    for timing in ["BEFORE", "AFTER"] {
        for event in ["INSERT", "UPDATE", "DELETE"] {
            execute(
                &mut session,
                &format!(
                    "CREATE TRIGGER {}_{} {timing} {event} ON trigger_target \
                     FOR EACH STATEMENT EXECUTE FUNCTION capture_statement_action()",
                    timing.to_ascii_lowercase(),
                    event.to_ascii_lowercase(),
                ),
            );
        }
    }

    execute(
        &mut session,
        "INSERT INTO trigger_target VALUES (1, 'inserted')
         ON CONFLICT (id) DO UPDATE SET payload = excluded.payload",
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT operation, timing, trigger_name FROM trigger_audit",
        ),
        vec![
            Row::new(vec![
                Value::Text("INSERT".into()),
                Value::Text("BEFORE".into()),
                Value::Text("before_insert".into()),
            ]),
            Row::new(vec![
                Value::Text("UPDATE".into()),
                Value::Text("BEFORE".into()),
                Value::Text("before_update".into()),
            ]),
            Row::new(vec![
                Value::Text("UPDATE".into()),
                Value::Text("AFTER".into()),
                Value::Text("after_update".into()),
            ]),
            Row::new(vec![
                Value::Text("INSERT".into()),
                Value::Text("AFTER".into()),
                Value::Text("after_insert".into()),
            ]),
        ]
    );

    execute(&mut session, "DELETE FROM trigger_audit");
    execute(
        &mut session,
        "MERGE INTO trigger_target AS target
         USING trigger_source AS source ON target.id = source.id
         WHEN MATCHED THEN UPDATE SET payload = source.payload
         WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (source.id, source.payload)
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT operation, timing, trigger_name FROM trigger_audit",
        ),
        vec![
            Row::new(vec![
                Value::Text("INSERT".into()),
                Value::Text("BEFORE".into()),
                Value::Text("before_insert".into()),
            ]),
            Row::new(vec![
                Value::Text("UPDATE".into()),
                Value::Text("BEFORE".into()),
                Value::Text("before_update".into()),
            ]),
            Row::new(vec![
                Value::Text("DELETE".into()),
                Value::Text("BEFORE".into()),
                Value::Text("before_delete".into()),
            ]),
            Row::new(vec![
                Value::Text("INSERT".into()),
                Value::Text("AFTER".into()),
                Value::Text("after_insert".into()),
            ]),
            Row::new(vec![
                Value::Text("UPDATE".into()),
                Value::Text("AFTER".into()),
                Value::Text("after_update".into()),
            ]),
            Row::new(vec![
                Value::Text("DELETE".into()),
                Value::Text("AFTER".into()),
                Value::Text("after_delete".into()),
            ]),
        ]
    );
}

#[test]
fn row_triggers_expose_old_new_and_stable_tg_context() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE TABLE trigger_target (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)",
    );
    execute(
        &mut session,
        "CREATE TABLE trigger_audit (
            old_payload TEXT NOT NULL,
            new_payload TEXT NOT NULL,
            operation TEXT NOT NULL,
            timing TEXT NOT NULL,
            level_name TEXT NOT NULL,
            trigger_name TEXT NOT NULL,
            schema_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            relation_oid BIGINT NOT NULL
         )",
    );
    execute(
        &mut session,
        "CREATE FUNCTION capture_row_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO trigger_audit VALUES (
             OLD.payload, NEW.payload, TG_OP, TG_WHEN, TG_LEVEL, TG_NAME,
             TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_RELID
           );
           RETURN NEW;
         END
         $$",
    );
    for (name, timing) in [
        ("z_before_update", "BEFORE"),
        ("a_before_update", "BEFORE"),
        ("after_update", "AFTER"),
    ] {
        execute(
            &mut session,
            &format!(
                "CREATE TRIGGER {name} {timing} UPDATE ON trigger_target \
                 FOR EACH ROW EXECUTE FUNCTION capture_row_trigger()"
            ),
        );
    }
    execute(
        &mut session,
        "INSERT INTO trigger_target VALUES (1, 'before')",
    );
    execute(
        &mut session,
        "UPDATE trigger_target SET payload = 'after' WHERE id = 1",
    );

    let audit = rows(
        &mut session,
        "SELECT old_payload, new_payload, operation, timing, level_name, trigger_name,
                schema_name, table_name, relation_oid
         FROM trigger_audit",
    );
    assert_eq!(audit.len(), 3);
    assert_eq!(
        audit
            .iter()
            .map(|row| row.values[5].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Text("a_before_update".into()),
            Value::Text("z_before_update".into()),
            Value::Text("after_update".into()),
        ]
    );
    for row in audit {
        assert_eq!(row.values[0], Value::Text("before".into()));
        assert_eq!(row.values[1], Value::Text("after".into()));
        assert_eq!(row.values[2], Value::Text("UPDATE".into()));
        assert_eq!(row.values[4], Value::Text("ROW".into()));
        assert_eq!(row.values[6], Value::Text("public".into()));
        assert_eq!(row.values[7], Value::Text("trigger_target".into()));
        assert!(matches!(row.values[8], Value::Int64(value) if value > 0));
    }
}

#[test]
#[cfg(not(debug_assertions))]
fn recursive_trigger_depth_returns_54001_on_a_small_native_stack() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut setup = engine.connect().expect("setup session");
    execute(
        &mut setup,
        "CREATE TABLE recursive_trigger_rows (id BIGINT PRIMARY KEY)",
    );
    execute(
        &mut setup,
        "CREATE FUNCTION recurse_row_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO recursive_trigger_rows VALUES (1);
           RETURN NEW;
         END $$",
    );
    execute(
        &mut setup,
        "CREATE TRIGGER recursive_row_trigger BEFORE INSERT ON recursive_trigger_rows
         FOR EACH ROW EXECUTE FUNCTION recurse_row_trigger()",
    );
    drop(setup);

    let mut small_stack_session = engine.connect().expect("small-stack session");
    let sql_state = std::thread::Builder::new()
        .name("trigger-small-stack".into())
        .stack_size(128 * 1024)
        .spawn(move || {
            small_stack_session
                .execute("INSERT INTO recursive_trigger_rows VALUES (1)", &[])
                .expect_err("recursive trigger depth limit")
                .sql_state
        })
        .expect("spawn small-stack worker")
        .join()
        .expect("join small-stack worker");
    assert_eq!(sql_state, "54001");

    let mut verification = engine.connect().expect("verification session");
    assert!(rows(&mut verification, "SELECT * FROM recursive_trigger_rows").is_empty());
}

#[test]
fn regular_view_instead_of_triggers_route_dml_returning_and_survive_reopen() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE TABLE view_rows (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)",
    );
    execute(
        &mut session,
        "CREATE VIEW editable_rows AS SELECT id, payload FROM view_rows",
    );
    let unavailable = session
        .execute("INSERT INTO editable_rows VALUES (1, 'blocked')", &[])
        .expect_err("view DML requires a matching INSTEAD OF trigger");
    assert_eq!(unavailable.sql_state, "55000");

    execute(
        &mut session,
        "CREATE FUNCTION editable_rows_insert() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO view_rows VALUES (NEW.id, NEW.payload);
           RETURN NEW;
         END
         $$",
    );
    execute(
        &mut session,
        "CREATE FUNCTION editable_rows_update() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           UPDATE view_rows SET id = NEW.id, payload = NEW.payload WHERE id = OLD.id;
           RETURN NEW;
         END
         $$",
    );
    execute(
        &mut session,
        "CREATE FUNCTION editable_rows_delete() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           DELETE FROM view_rows WHERE id = OLD.id;
           RETURN OLD;
         END
         $$",
    );
    let invalid_view_timing = session
        .execute(
            "CREATE TRIGGER invalid_view_before BEFORE INSERT ON editable_rows
             FOR EACH ROW EXECUTE FUNCTION editable_rows_insert()",
            &[],
        )
        .expect_err("regular views accept only INSTEAD OF ROW triggers");
    assert_eq!(invalid_view_timing.sql_state, "0A000");
    let invalid_table_timing = session
        .execute(
            "CREATE TRIGGER invalid_table_instead INSTEAD OF INSERT ON view_rows
             FOR EACH ROW EXECUTE FUNCTION editable_rows_insert()",
            &[],
        )
        .expect_err("tables reject INSTEAD OF triggers");
    assert_eq!(invalid_table_timing.sql_state, "0A000");
    for (name, event, function) in [
        ("editable_rows_insert", "INSERT", "editable_rows_insert"),
        ("editable_rows_update", "UPDATE", "editable_rows_update"),
        ("editable_rows_delete", "DELETE", "editable_rows_delete"),
    ] {
        execute(
            &mut session,
            &format!(
                "CREATE TRIGGER {name}_trigger INSTEAD OF {event} ON editable_rows \
                 FOR EACH ROW EXECUTE FUNCTION {function}()"
            ),
        );
    }
    assert_eq!(
        rows(
            &mut session,
            "SELECT tgname, tgtype FROM pg_catalog.pg_trigger ORDER BY tgname",
        ),
        vec![
            Row::new(vec![
                Value::Text("editable_rows_delete_trigger".into()),
                Value::Int16(73),
            ]),
            Row::new(vec![
                Value::Text("editable_rows_insert_trigger".into()),
                Value::Int16(69),
            ]),
            Row::new(vec![
                Value::Text("editable_rows_update_trigger".into()),
                Value::Int16(81),
            ]),
        ]
    );

    assert_eq!(
        rows(
            &mut session,
            "INSERT INTO editable_rows VALUES (1, 'inserted') RETURNING id, payload",
        ),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("inserted".into()),
        ])]
    );
    assert_eq!(
        rows(
            &mut session,
            "UPDATE editable_rows SET payload = 'updated' WHERE id = 1 RETURNING payload",
        ),
        vec![Row::new(vec![Value::Text("updated".into())])]
    );
    assert_eq!(
        rows(
            &mut session,
            "DELETE FROM editable_rows WHERE id = 1 RETURNING id, payload",
        ),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("updated".into()),
        ])]
    );
    assert!(rows(&mut session, "SELECT * FROM view_rows").is_empty());

    drop(session);
    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut session = reopened.connect().expect("reopened session");
    execute(
        &mut session,
        "INSERT INTO editable_rows VALUES (2, 'after reopen')",
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM view_rows"),
        vec![Row::new(vec![
            Value::Int64(2),
            Value::Text("after reopen".into()),
        ])]
    );
}
