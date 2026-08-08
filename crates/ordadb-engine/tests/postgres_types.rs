use ordadb_engine::{Engine, EngineConfig, Session, SessionAuthorization};
use ordadb_types::{Identifier, QueryEvent, Row, Value};
use tempfile::tempdir;

fn execute(session: &mut Session, sql: &str) -> Vec<QueryEvent> {
    session
        .execute(sql, &[])
        .unwrap_or_else(|error| panic!("failed to execute {sql:?}: {error:?}"))
        .collect()
}

fn rows(events: &[QueryEvent]) -> Vec<Row> {
    events
        .iter()
        .filter_map(|event| match event {
            QueryEvent::Batch(batch) => Some(batch.rows.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

fn sql_state(session: &mut Session, sql: &str) -> String {
    match session.execute(sql, &[]) {
        Ok(_) => panic!("statement unexpectedly succeeded: {sql}"),
        Err(error) => error.sql_state.to_string(),
    }
}

#[test]
fn common_scalar_functions_follow_postgresql_text_and_null_semantics() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE function_inputs (id bigint, value text)",
    );
    execute(
        &mut session,
        "INSERT INTO function_inputs VALUES (7, 'xyhelloxy')",
    );

    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT TRIM(BOTH 'xy' FROM value), LTRIM('  left'), RTRIM('right  '), \
             REPLACE('café', 'fé', 'ke'), POSITION('c' IN 'åbcå'), \
             GREATEST(NULL, id, 3), LEAST(NULL, id, 3) FROM function_inputs",
        )),
        vec![Row::new(vec![
            Value::Text("hello".into()),
            Value::Text("left".into()),
            Value::Text("right".into()),
            Value::Text("cake".into()),
            Value::Int32(3),
            Value::Int64(7),
            Value::Int64(3),
        ])]
    );
}

#[test]
fn enum_domain_arrays_reopen_and_type_cascade_preserve_rows() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')",
    );
    execute(
        &mut session,
        "CREATE DOMAIN positive_int AS integer DEFAULT 1 NOT NULL \
         CONSTRAINT positive CHECK (VALUE > 0)",
    );
    execute(
        &mut session,
        "CREATE FUNCTION echo_mood(value mood) RETURNS mood \
         LANGUAGE plpgsql AS $$ BEGIN RETURN value; END $$",
    );
    execute(
        &mut session,
        "CREATE PROCEDURE accept_mood(value mood) \
         LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$",
    );
    execute(
        &mut session,
        "CREATE TABLE feelings (current_mood mood NOT NULL, score positive_int)",
    );
    execute(
        &mut session,
        "CREATE TABLE typed_arrays (id bigint, moods mood[], scores positive_int[])",
    );
    execute(
        &mut session,
        "CREATE TABLE converted_values (id bigint, value text)",
    );
    execute(
        &mut session,
        "INSERT INTO feelings (current_mood) VALUES ('happy')",
    );
    execute(
        &mut session,
        "INSERT INTO typed_arrays VALUES (1, ARRAY['sad', 'happy'], ARRAY[1, 2])",
    );
    execute(
        &mut session,
        "INSERT INTO converted_values VALUES (1, 'sad')",
    );
    execute(
        &mut session,
        "ALTER TABLE converted_values ALTER COLUMN value TYPE mood",
    );

    let cast_rows = rows(&execute(
        &mut session,
        "SELECT 'ok'::mood, 7::positive_int, ARRAY['sad', 'happy']::mood[] \
         FROM feelings",
    ));
    assert_eq!(cast_rows.len(), 1);
    assert_eq!(cast_rows[0].values[0], Value::Text("ok".into()));
    assert_eq!(cast_rows[0].values[1], Value::Int32(7));
    let Value::Array(array) = &cast_rows[0].values[2] else {
        panic!("named enum array cast did not return an array");
    };
    assert_eq!(
        array.values(),
        &[Value::Text("sad".into()), Value::Text("happy".into())]
    );

    let parameter_rows = session
        .execute(
            "SELECT $1::mood FROM feelings",
            &[Value::Text("happy".into())],
        )
        .expect("execute named enum parameter cast")
        .collect::<Vec<_>>();
    assert_eq!(
        rows(&parameter_rows),
        vec![Row::new(vec![Value::Text("happy".into())])]
    );
    assert_eq!(
        sql_state(&mut session, "SELECT 'angry'::mood FROM feelings"),
        "22P02"
    );
    assert_eq!(
        rows(&execute(&mut session, "SELECT echo_mood('ok')")),
        vec![Row::new(vec![Value::Text("ok".into())])]
    );

    assert_eq!(
        sql_state(
            &mut session,
            "INSERT INTO typed_arrays VALUES (2, ARRAY['ok', 'angry'], ARRAY[1])",
        ),
        "22P02"
    );
    assert_eq!(
        sql_state(
            &mut session,
            "INSERT INTO typed_arrays VALUES (2, ARRAY['ok'], ARRAY[1, 0])",
        ),
        "23514"
    );
    assert_eq!(sql_state(&mut session, "DROP TYPE mood"), "2BP01");

    drop(session);
    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen engine");
    let mut session = reopened.connect().expect("reconnect");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT current_mood, score FROM feelings",
        )),
        vec![Row::new(
            vec![Value::Text("happy".into()), Value::Int32(1),]
        )]
    );
    assert_eq!(
        rows(&execute(&mut session, "SELECT echo_mood('sad')")),
        vec![Row::new(vec![Value::Text("sad".into())])]
    );
    assert_eq!(
        rows(&execute(&mut session, "SELECT value FROM converted_values",)),
        vec![Row::new(vec![Value::Text("sad".into())])]
    );

    execute(&mut session, "DROP TYPE mood CASCADE");
    assert_eq!(
        rows(&execute(&mut session, "SELECT score FROM feelings")),
        vec![Row::new(vec![Value::Int32(1)])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id, scores FROM typed_arrays",
        ))
        .len(),
        1
    );
    assert_eq!(
        sql_state(&mut session, "SELECT current_mood FROM feelings"),
        "42703"
    );
    assert_eq!(sql_state(&mut session, "SELECT echo_mood('sad')"), "42883");
}

#[test]
fn enum_comparison_indexes_aggregates_and_windows_use_declaration_order() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TYPE workflow_state AS ENUM ('zeta', 'alpha', 'middle')",
    );
    execute(
        &mut session,
        "CREATE TABLE jobs (id bigint PRIMARY KEY, state workflow_state NOT NULL)",
    );
    execute(&mut session, "CREATE INDEX jobs_state_idx ON jobs (state)");
    execute(
        &mut session,
        "INSERT INTO jobs VALUES (1, 'zeta'), (2, 'alpha'), (3, 'middle')",
    );

    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT state FROM jobs ORDER BY state",
        )),
        vec![
            Row::new(vec![Value::Text("zeta".into())]),
            Row::new(vec![Value::Text("alpha".into())]),
            Row::new(vec![Value::Text("middle".into())]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id FROM jobs WHERE state >= 'alpha' ORDER BY state",
        )),
        vec![
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(3)]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT MIN(DISTINCT state), MAX(DISTINCT state) FROM jobs",
        )),
        vec![Row::new(vec![
            Value::Text("zeta".into()),
            Value::Text("middle".into()),
        ])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id, MIN(state) OVER () FROM jobs ORDER BY id",
        )),
        vec![
            Row::new(vec![Value::Int64(1), Value::Text("zeta".into())]),
            Row::new(vec![Value::Int64(2), Value::Text("zeta".into())]),
            Row::new(vec![Value::Int64(3), Value::Text("zeta".into())]),
        ]
    );

    drop(session);
    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen engine");
    let mut session = reopened.connect().expect("reconnect");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id FROM jobs WHERE state = 'alpha'",
        )),
        vec![Row::new(vec![Value::Int64(2)])]
    );
}

#[test]
fn alter_enum_and_domain_rewrite_validate_and_reopen_atomically() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TYPE workflow_state AS ENUM ('new', 'done')",
    );
    execute(
        &mut session,
        "CREATE DOMAIN score_value AS integer DEFAULT 1 CHECK (VALUE > 0)",
    );
    execute(
        &mut session,
        "CREATE TABLE work_items (\
             id bigint PRIMARY KEY, \
             state workflow_state, \
             history workflow_state[], \
             score score_value\
         )",
    );
    execute(
        &mut session,
        "CREATE INDEX work_items_state_idx ON work_items (state)",
    );
    execute(
        &mut session,
        "INSERT INTO work_items (id, state, history) \
         VALUES (1, 'new', ARRAY['new', 'done'])",
    );

    execute(
        &mut session,
        "ALTER TYPE workflow_state ADD VALUE 'blocked' BEFORE 'done'",
    );
    execute(
        &mut session,
        "ALTER TYPE workflow_state ADD VALUE IF NOT EXISTS 'blocked'",
    );
    execute(
        &mut session,
        "INSERT INTO work_items (id, state, history) \
         VALUES (2, 'blocked', ARRAY['blocked'])",
    );
    execute(
        &mut session,
        "ALTER TYPE workflow_state RENAME VALUE 'new' TO 'queued'",
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id FROM work_items WHERE state = 'queued'",
        )),
        vec![Row::new(vec![Value::Int64(1)])]
    );
    let renamed = rows(&execute(
        &mut session,
        "SELECT state, history FROM work_items WHERE id = 1",
    ));
    assert_eq!(renamed[0].values[0], Value::Text("queued".into()));
    let Value::Array(history) = &renamed[0].values[1] else {
        panic!("enum history did not remain an array");
    };
    assert_eq!(
        history.values(),
        &[Value::Text("queued".into()), Value::Text("done".into())]
    );
    assert_eq!(
        sql_state(
            &mut session,
            "INSERT INTO work_items VALUES (3, 'new', ARRAY['new'], 1)",
        ),
        "22P02"
    );

    execute(&mut session, "ALTER DOMAIN score_value SET DEFAULT 2");
    execute(
        &mut session,
        "INSERT INTO work_items (id, state, history) \
         VALUES (3, 'done', ARRAY['done'])",
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT score FROM work_items WHERE id = 3",
        )),
        vec![Row::new(vec![Value::Int32(2)])]
    );
    execute(
        &mut session,
        "ALTER DOMAIN score_value ADD CONSTRAINT below_ten CHECK (VALUE < 10)",
    );
    assert_eq!(
        sql_state(
            &mut session,
            "INSERT INTO work_items VALUES (4, 'done', ARRAY['done'], 10)",
        ),
        "23514"
    );
    execute(&mut session, "ALTER DOMAIN score_value SET NOT NULL");
    execute(&mut session, "ALTER DOMAIN score_value DROP DEFAULT");
    assert_eq!(
        sql_state(
            &mut session,
            "INSERT INTO work_items (id, state, history) \
             VALUES (4, 'done', ARRAY['done'])",
        ),
        "23502"
    );
    execute(&mut session, "ALTER DOMAIN score_value DROP NOT NULL");
    execute(
        &mut session,
        "INSERT INTO work_items (id, state, history) \
         VALUES (4, 'done', ARRAY['done'])",
    );
    execute(
        &mut session,
        "ALTER DOMAIN score_value DROP CONSTRAINT below_ten",
    );
    execute(
        &mut session,
        "INSERT INTO work_items VALUES (5, 'done', ARRAY['done'], 10)",
    );
    execute(
        &mut session,
        "ALTER DOMAIN score_value DROP CONSTRAINT IF EXISTS missing_constraint",
    );

    drop(session);
    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen engine");
    let mut session = reopened.connect().expect("reconnect");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id FROM work_items WHERE state = 'queued'",
        )),
        vec![Row::new(vec![Value::Int64(1)])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT score FROM work_items WHERE id = 4",
        )),
        vec![Row::new(vec![Value::Null])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT score FROM work_items WHERE id = 5",
        )),
        vec![Row::new(vec![Value::Int32(10)])]
    );
}

#[test]
fn named_catalog_expressions_enum_domains_and_routine_overloads_reopen() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')",
    );
    execute(
        &mut session,
        "CREATE DOMAIN cheerful_mood AS mood DEFAULT 'ok'::mood \
         CHECK (VALUE <> 'sad'::mood)",
    );
    execute(
        &mut session,
        "CREATE TABLE mood_defaults (id bigint PRIMARY KEY, current_mood cheerful_mood)",
    );
    execute(
        &mut session,
        "CREATE TABLE mood_checks (id bigint PRIMARY KEY, current_mood mood \
         CHECK (current_mood <> 'sad'::mood))",
    );
    execute(&mut session, "INSERT INTO mood_defaults (id) VALUES (1)");
    execute(&mut session, "INSERT INTO mood_checks VALUES (1, 'happy')");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT current_mood FROM mood_defaults WHERE id = 1",
        )),
        vec![Row::new(vec![Value::Text("ok".into())])]
    );
    assert_eq!(
        sql_state(&mut session, "INSERT INTO mood_defaults VALUES (2, 'sad')",),
        "23514"
    );
    assert_eq!(
        sql_state(&mut session, "INSERT INTO mood_checks VALUES (2, 'sad')"),
        "23514"
    );
    assert_eq!(
        sql_state(&mut session, "CREATE DOMAIN nested_mood AS cheerful_mood",),
        "0A000"
    );

    execute(
        &mut session,
        "CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)",
    );
    execute(
        &mut session,
        "CREATE DOMAIN nonnegative_int AS integer CHECK (VALUE >= 0)",
    );
    execute(
        &mut session,
        "CREATE FUNCTION choose_value(value positive_int) RETURNS text \
         LANGUAGE plpgsql AS $$ BEGIN RETURN 'positive'; END $$",
    );
    execute(
        &mut session,
        "CREATE FUNCTION choose_value(value nonnegative_int) RETURNS text \
         LANGUAGE plpgsql AS $$ BEGIN RETURN 'nonnegative'; END $$",
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT choose_value(1::positive_int)",
        )),
        vec![Row::new(vec![Value::Text("positive".into())])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT choose_value(0::nonnegative_int)",
        )),
        vec![Row::new(vec![Value::Text("nonnegative".into())])]
    );
    assert_eq!(sql_state(&mut session, "SELECT choose_value(1)"), "42725");
    execute(&mut session, "DROP FUNCTION choose_value(positive_int)");

    drop(session);
    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen engine");
    let mut session = reopened.connect().expect("reconnect");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT current_mood FROM mood_defaults WHERE id = 1",
        )),
        vec![Row::new(vec![Value::Text("ok".into())])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT choose_value(0::nonnegative_int)",
        )),
        vec![Row::new(vec![Value::Text("nonnegative".into())])]
    );
}

#[test]
fn authenticated_catalog_ownership_survives_reopen_and_rolls_back_atomically() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
    let mut alice = engine
        .connect_authenticated(SessionAuthorization::new("alice", false).expect("alice identity"))
        .expect("connect alice");
    execute(&mut alice, "CREATE SCHEMA workspace");
    execute(
        &mut alice,
        "CREATE TABLE workspace.items (id bigint PRIMARY KEY, value text)",
    );

    let catalog = engine.catalog_snapshot().expect("catalog snapshot");
    let schema = catalog
        .schema(&Identifier::unquoted("workspace"))
        .expect("workspace schema");
    let table = schema
        .table(&Identifier::unquoted("items"))
        .expect("items table");
    let table_id = table.id;
    let owned_objects = catalog
        .object_refs()
        .into_iter()
        .filter(|object| catalog.owner_of(*object).is_some())
        .collect::<Vec<_>>();
    assert!(
        owned_objects.len() >= 5,
        "schema, table, columns, constraint, and implicit index must be owned"
    );
    assert!(owned_objects.iter().all(|object| {
        catalog
            .owner_of(*object)
            .is_some_and(|owner| owner.as_str() == "alice")
    }));
    drop(catalog);

    let mut bob = engine
        .connect_authenticated(SessionAuthorization::new("bob", false).expect("bob identity"))
        .expect("connect bob");
    assert_eq!(
        sql_state(
            &mut bob,
            "ALTER TABLE workspace.items ADD COLUMN rejected text",
        ),
        "42501"
    );

    let mut administrator = engine
        .connect_authenticated(
            SessionAuthorization::new("administrator", true).expect("administrator identity"),
        )
        .expect("connect administrator");
    execute(
        &mut administrator,
        "ALTER TABLE workspace.items ADD COLUMN managed text",
    );

    execute(&mut alice, "BEGIN");
    execute(
        &mut alice,
        "CREATE TABLE workspace.transient (id bigint PRIMARY KEY)",
    );
    execute(&mut alice, "ROLLBACK");
    let catalog = engine.catalog_snapshot().expect("catalog after rollback");
    assert!(
        catalog
            .table(
                &Identifier::unquoted("workspace"),
                &Identifier::unquoted("transient"),
            )
            .is_none(),
        "rolled-back table must not remain in the catalog"
    );
    assert_eq!(
        catalog
            .table_by_id(table_id)
            .expect("owned table after rollback")
            .columns()
            .len(),
        3
    );
    let all_owned_objects = catalog
        .object_refs()
        .into_iter()
        .filter(|object| catalog.owner_of(*object).is_some())
        .collect::<Vec<_>>();
    drop(catalog);

    drop(administrator);
    drop(bob);
    drop(alice);
    drop(engine);

    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen engine");
    let catalog = reopened.catalog_snapshot().expect("reopened catalog");
    assert!(owned_objects.iter().all(|object| {
        catalog
            .owner_of(*object)
            .is_some_and(|owner| owner.as_str() == "alice")
    }));
    drop(catalog);
    let mut bob = reopened
        .connect_authenticated(SessionAuthorization::new("bob", false).expect("bob identity"))
        .expect("reconnect bob");
    assert_eq!(
        sql_state(
            &mut bob,
            "ALTER TABLE workspace.items ADD COLUMN still_rejected text",
        ),
        "42501"
    );
    drop(bob);

    let mut alice = reopened
        .connect_authenticated(SessionAuthorization::new("alice", false).expect("alice identity"))
        .expect("reconnect alice");
    execute(&mut alice, "DROP SCHEMA workspace CASCADE");
    let catalog = reopened.catalog_snapshot().expect("catalog after cascade");
    assert!(
        all_owned_objects
            .iter()
            .all(|object| catalog.owner_of(*object).is_none()),
        "cascading drop must remove ownership entries"
    );
}
