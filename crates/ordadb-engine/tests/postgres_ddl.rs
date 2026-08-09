use std::sync::{Arc, atomic::AtomicBool};

use ordadb_engine::{Engine, EngineConfig, Session};
use ordadb_types::{Identifier, QueryEvent, Row, Value};
use tempfile::tempdir;

fn execute(engine: &Engine, sql: &str) {
    engine
        .connect()
        .expect("connect")
        .execute(sql, &[])
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .for_each(drop);
}

fn rows(engine: &Engine, sql: &str) -> Vec<Row> {
    engine
        .connect()
        .expect("connect")
        .execute(sql, &[])
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .filter_map(|event| match event {
            QueryEvent::Batch(batch) => Some(batch.rows),
            _ => None,
        })
        .flatten()
        .collect()
}

fn scalar_i64(session: &mut Session, sql: &str) -> i64 {
    session
        .execute(sql, &[])
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .find_map(|event| match event {
            QueryEvent::Batch(batch) => batch
                .rows
                .first()
                .and_then(|row| row.values.first())
                .cloned(),
            _ => None,
        })
        .and_then(|value| match value {
            Value::Int64(value) => Some(value),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: expected one BIGINT scalar"))
}

#[test]
fn durable_ddl_defaults_checks_composite_keys_and_foreign_keys_are_atomic() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    execute(&engine, "CREATE SCHEMA app");
    execute(
        &engine,
        "CREATE TABLE app.parent (
            tenant BIGINT,
            id BIGINT,
            label TEXT DEFAULT 'ready',
            CONSTRAINT parent_pk PRIMARY KEY (tenant, id),
            CONSTRAINT parent_label_unique UNIQUE (tenant, label),
            CONSTRAINT parent_label_check CHECK (label <> 'blocked')
        )",
    );
    execute(&engine, "INSERT INTO app.parent (tenant, id) VALUES (7, 1)");
    assert_eq!(
        rows(&engine, "SELECT * FROM app.parent"),
        vec![Row::new(vec![
            Value::Int64(7),
            Value::Int64(1),
            Value::Text("ready".into()),
        ])]
    );

    execute(
        &engine,
        "CREATE TABLE app.child (
            tenant BIGINT,
            parent_id BIGINT,
            CONSTRAINT child_parent_fk FOREIGN KEY (tenant, parent_id)
                REFERENCES app.parent (tenant, id)
        )",
    );
    execute(&engine, "INSERT INTO app.child VALUES (7, 1)");
    let foreign_key = engine
        .connect()
        .expect("connect")
        .execute("INSERT INTO app.child VALUES (7, 99)", &[])
        .expect_err("foreign-key violation");
    assert_eq!(foreign_key.sql_state, "23503");

    let check = engine
        .connect()
        .expect("connect")
        .execute("INSERT INTO app.parent VALUES (7, 2, 'blocked')", &[])
        .expect_err("check violation");
    assert_eq!(check.sql_state, "23514");
    assert_eq!(rows(&engine, "SELECT * FROM app.parent").len(), 1);

    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    assert_eq!(rows(&reopened, "SELECT * FROM app.parent").len(), 1);
    assert_eq!(rows(&reopened, "SELECT * FROM app.child").len(), 1);
}

#[test]
fn alter_drop_and_views_publish_catalog_and_rows_together() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    execute(&engine, "CREATE SCHEMA app");
    execute(
        &engine,
        "CREATE TABLE app.items (id BIGINT PRIMARY KEY, name TEXT)",
    );
    execute(&engine, "INSERT INTO app.items VALUES (1, 'first')");
    execute(
        &engine,
        "ALTER TABLE app.items ADD COLUMN state TEXT DEFAULT 'active' NOT NULL",
    );
    execute(
        &engine,
        "ALTER TABLE app.items RENAME COLUMN state TO lifecycle",
    );
    execute(
        &engine,
        "CREATE VIEW app.item_names AS SELECT id, name FROM app.items",
    );
    execute(
        &engine,
        "ALTER VIEW app.item_names RENAME TO current_item_names",
    );
    let catalog = engine
        .catalog_snapshot()
        .expect("catalog after view rename");
    assert!(
        catalog
            .view(
                &Identifier::unquoted("app"),
                &Identifier::unquoted("current_item_names"),
            )
            .is_some()
    );
    assert!(
        catalog
            .view(
                &Identifier::unquoted("app"),
                &Identifier::unquoted("item_names"),
            )
            .is_none()
    );
    execute(
        &engine,
        "CREATE VIEW app.nested_item_names AS SELECT * FROM app.current_item_names",
    );
    let incompatible_replace = engine
        .connect()
        .expect("connect")
        .execute(
            "CREATE OR REPLACE VIEW app.current_item_names AS SELECT id FROM app.items",
            &[],
        )
        .expect_err("incompatible view replacement");
    assert_eq!(incompatible_replace.sql_state, "42P16");
    execute(
        &engine,
        "CREATE OR REPLACE VIEW app.current_item_names AS SELECT id, name FROM app.items",
    );
    execute(
        &engine,
        "CREATE MATERIALIZED VIEW app.item_snapshot AS SELECT id, name FROM app.items WITH DATA",
    );
    execute(
        &engine,
        "ALTER MATERIALIZED VIEW app.item_snapshot RENAME TO current_item_snapshot",
    );
    execute(
        &engine,
        "CREATE INDEX current_item_snapshot_id_idx ON app.current_item_snapshot (id)",
    );

    assert_eq!(
        rows(&engine, "SELECT * FROM app.items"),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("first".into()),
            Value::Text("active".into()),
        ])]
    );
    assert_eq!(
        rows(&engine, "SELECT name FROM app.nested_item_names"),
        vec![Row::new(vec![Value::Text("first".into())])]
    );
    assert_eq!(
        rows(&engine, "SELECT * FROM app.current_item_snapshot"),
        vec![Row::new(
            vec![Value::Int64(1), Value::Text("first".into()),]
        )]
    );
    execute(
        &engine,
        "INSERT INTO app.items VALUES (2, 'second', 'active')",
    );
    assert_eq!(
        rows(&engine, "SELECT * FROM app.current_item_snapshot").len(),
        1
    );
    execute(
        &engine,
        "REFRESH MATERIALIZED VIEW app.current_item_snapshot WITH DATA",
    );
    assert_eq!(
        rows(&engine, "SELECT * FROM app.current_item_snapshot").len(),
        2
    );
    execute(
        &engine,
        "REFRESH MATERIALIZED VIEW app.current_item_snapshot WITH NO DATA",
    );
    let unpopulated = engine
        .connect()
        .expect("connect")
        .execute("SELECT * FROM app.current_item_snapshot", &[])
        .expect_err("unpopulated materialized view");
    assert_eq!(unpopulated.sql_state, "55000");
    execute(
        &engine,
        "REFRESH MATERIALIZED VIEW app.current_item_snapshot",
    );
    let catalog = engine.catalog_snapshot().expect("catalog");
    let schema = catalog
        .schema(&Identifier::unquoted("app"))
        .expect("app schema");
    assert!(
        schema
            .view(&Identifier::unquoted("current_item_names"))
            .is_some()
    );
    let materialized = schema
        .view(&Identifier::unquoted("current_item_snapshot"))
        .expect("materialized view");
    assert!(materialized.populated);
    let materialized_table_id = materialized
        .materialized_table_id
        .expect("materialized backing table");
    assert!(
        catalog
            .table_by_id(materialized_table_id)
            .and_then(|table| table.index(&Identifier::unquoted("current_item_snapshot_id_idx")))
            .is_some()
    );
    drop(catalog);
    drop(engine);

    let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen renamed views");
    assert_eq!(
        rows(&engine, "SELECT name FROM app.nested_item_names"),
        vec![
            Row::new(vec![Value::Text("first".into())]),
            Row::new(vec![Value::Text("second".into())]),
        ]
    );
    assert_eq!(
        rows(&engine, "SELECT * FROM app.current_item_snapshot").len(),
        2
    );
    execute(
        &engine,
        "ALTER VIEW IF EXISTS app.missing RENAME TO ignored",
    );
    execute(
        &engine,
        "ALTER MATERIALIZED VIEW IF EXISTS app.missing RENAME TO ignored",
    );
    let wrong_kind = engine
        .connect()
        .expect("connect")
        .execute(
            "ALTER MATERIALIZED VIEW app.current_item_names RENAME TO invalid",
            &[],
        )
        .expect_err("regular view cannot be renamed as materialized");
    assert_eq!(wrong_kind.sql_state, "42809");

    let restricted = engine
        .connect()
        .expect("connect")
        .execute("DROP VIEW app.current_item_names", &[])
        .expect_err("dependent nested view");
    assert_eq!(restricted.sql_state, "2BP01");
    execute(&engine, "DROP VIEW app.current_item_names CASCADE");
    execute(&engine, "DROP MATERIALIZED VIEW app.current_item_snapshot");
    let catalog = engine.catalog_snapshot().expect("catalog after drop");
    let schema = catalog
        .schema(&Identifier::unquoted("app"))
        .expect("app schema");
    assert!(
        schema
            .view(&Identifier::unquoted("current_item_names"))
            .is_none()
    );
    assert!(
        schema
            .view(&Identifier::unquoted("nested_item_names"))
            .is_none()
    );
    assert!(
        schema
            .view(&Identifier::unquoted("current_item_snapshot"))
            .is_none()
    );
}

#[test]
fn foreign_key_actions_use_a_bounded_candidate_work_queue() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    execute(
        &engine,
        "CREATE TABLE parent (id BIGINT PRIMARY KEY, replacement BIGINT DEFAULT 0 UNIQUE)",
    );
    execute(
        &engine,
        "CREATE TABLE child (
            id BIGINT PRIMARY KEY,
            parent_id BIGINT,
            CONSTRAINT child_parent_fk FOREIGN KEY (parent_id)
                REFERENCES parent (id) ON UPDATE CASCADE ON DELETE CASCADE
        )",
    );
    execute(&engine, "INSERT INTO parent VALUES (1, 0)");
    execute(&engine, "INSERT INTO child VALUES (10, 1)");
    execute(&engine, "UPDATE parent SET id = 2 WHERE id = 1");
    assert_eq!(
        rows(&engine, "SELECT * FROM child"),
        vec![Row::new(vec![Value::Int64(10), Value::Int64(2)])]
    );
    execute(&engine, "DELETE FROM parent WHERE id = 2");
    assert!(rows(&engine, "SELECT * FROM child").is_empty());
}

#[test]
fn plpgsql_routine_source_is_compiled_before_durable_catalog_publication() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    execute(&engine, "CREATE SCHEMA app");
    execute(
        &engine,
        "CREATE FUNCTION app.answer()
         RETURNS BIGINT
         LANGUAGE plpgsql
         AS $$
         BEGIN
         RETURN 1;
         END;
         $$",
    );
    let catalog = engine.catalog_snapshot().expect("catalog");
    let routines = catalog
        .schema(&Identifier::unquoted("app"))
        .expect("schema")
        .routines_named(&Identifier::unquoted("answer"));
    assert_eq!(routines.len(), 1);
    assert_eq!(
        routines[0].return_type,
        Some(ordadb_types::ScalarType::Int64)
    );
    drop(catalog);
    assert_eq!(
        rows(&engine, "SELECT app.answer() AS value"),
        vec![Row::new(vec![Value::Int64(1)])]
    );

    let invalid = engine
        .connect()
        .expect("connect")
        .execute(
            "CREATE FUNCTION app.invalid()
             RETURNS BIGINT
             LANGUAGE plpgsql
             AS $$
             BEGIN
             IF true THEN
             RETURN 1;
             END;
             $$",
            &[],
        )
        .expect_err("invalid block");
    assert_eq!(invalid.sql_state, "42601");
    assert!(
        engine
            .catalog_snapshot()
            .expect("catalog")
            .schema(&Identifier::unquoted("app"))
            .expect("schema")
            .routines_named(&Identifier::unquoted("invalid"))
            .is_empty()
    );

    execute(
        &engine,
        "CREATE TABLE app.procedure_items (id BIGINT PRIMARY KEY, label TEXT)",
    );
    execute(
        &engine,
        "CREATE PROCEDURE app.add_item(item_id BIGINT, item_label TEXT)
         LANGUAGE plpgsql
         AS $$
         BEGIN
         INSERT INTO app.procedure_items VALUES (item_id, item_label);
         END;
         $$",
    );
    execute(&engine, "CALL app.add_item(3, 'from procedure')");
    assert_eq!(
        rows(&engine, "SELECT * FROM app.procedure_items"),
        vec![Row::new(vec![
            Value::Int64(3),
            Value::Text("from procedure".into()),
        ])]
    );
}

#[test]
fn plpgsql_call_depth_and_cancellation_are_bounded_and_atomic() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    execute(&engine, "CREATE SCHEMA app");
    execute(&engine, "CREATE TABLE app.audit (value BIGINT)");
    execute(
        &engine,
        "CREATE PROCEDURE app.recurse()
         LANGUAGE plpgsql
         AS $$
         BEGIN
         INSERT INTO app.audit VALUES (1);
         CALL app.recurse();
         END;
         $$",
    );

    let depth = engine
        .connect()
        .expect("connect")
        .execute("CALL app.recurse()", &[])
        .expect_err("routine depth limit");
    assert_eq!(depth.sql_state, "54001");
    assert!(rows(&engine, "SELECT * FROM app.audit").is_empty());

    #[cfg(not(debug_assertions))]
    {
        let mut small_stack_session = engine.connect().expect("small-stack session");
        let small_stack_state = std::thread::Builder::new()
            .name("plpgsql-small-stack".into())
            .stack_size(128 * 1024)
            .spawn(move || {
                small_stack_session
                    .execute("CALL app.recurse()", &[])
                    .expect_err("small-stack routine depth limit")
                    .sql_state
            })
            .expect("spawn small-stack worker")
            .join()
            .expect("join small-stack worker");
        assert_eq!(small_stack_state, "54001");
        assert!(rows(&engine, "SELECT * FROM app.audit").is_empty());
    }

    execute(
        &engine,
        "CREATE PROCEDURE app.spin()
         LANGUAGE plpgsql
         AS $$
         BEGIN
         LOOP
         CONTINUE;
         END LOOP;
         END;
         $$",
    );
    let mut session = engine.connect().expect("connect");
    let cancellation = Arc::new(AtomicBool::new(true));
    let cancelled = session
        .execute_stream_with_cancellation("CALL app.spin()", &[], cancellation)
        .expect_err("routine cancellation");
    assert_eq!(cancelled.sql_state, "57014");
    assert!(rows(&engine, "SELECT * FROM app.audit").is_empty());
}

#[test]
fn row_trigger_side_effects_are_atomic_and_survive_restart() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    execute(&engine, "CREATE SCHEMA app");
    execute(
        &engine,
        "CREATE TABLE app.items (id BIGINT PRIMARY KEY, label TEXT)",
    );
    execute(
        &engine,
        "CREATE TABLE app.audit (message TEXT NOT NULL UNIQUE)",
    );
    execute(
        &engine,
        "CREATE FUNCTION app.audit_insert()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
         INSERT INTO app.audit VALUES ('inserted');
         RETURN NEW;
         END;
         $$",
    );
    execute(
        &engine,
        "CREATE TRIGGER items_audit
         AFTER INSERT ON app.items
         FOR EACH ROW
         EXECUTE FUNCTION app.audit_insert()",
    );

    execute(&engine, "INSERT INTO app.items VALUES (1, 'first')");
    assert_eq!(
        rows(&engine, "SELECT * FROM app.audit"),
        vec![Row::new(vec![Value::Text("inserted".into())])]
    );
    execute(&engine, "ALTER TABLE app.items DISABLE TRIGGER items_audit");
    drop(engine);

    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    execute(
        &reopened,
        "INSERT INTO app.items VALUES (2, 'trigger disabled')",
    );
    assert_eq!(rows(&reopened, "SELECT * FROM app.audit").len(), 1);
    execute(
        &reopened,
        "ALTER TABLE app.items ENABLE TRIGGER items_audit",
    );
    let failed = reopened
        .connect()
        .expect("connect")
        .execute("INSERT INTO app.items VALUES (3, 'trigger enabled')", &[])
        .expect_err("trigger side effect violates unique constraint");
    assert_eq!(failed.sql_state, "23505");
    assert_eq!(rows(&reopened, "SELECT * FROM app.items").len(), 2);
    assert_eq!(rows(&reopened, "SELECT * FROM app.audit").len(), 1);
}

#[test]
fn before_row_triggers_expose_old_and_new_and_can_replace_or_suppress_rows() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    execute(&engine, "CREATE SCHEMA app");
    execute(
        &engine,
        "CREATE TABLE app.items (id BIGINT PRIMARY KEY, label TEXT)",
    );
    execute(&engine, "CREATE TABLE app.audit (message TEXT NOT NULL)");
    execute(
        &engine,
        "CREATE FUNCTION app.before_insert()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
         IF NEW.label = 'skip' THEN
         RETURN NULL;
         END IF;
         NEW.label := 'normalized';
         RETURN NEW;
         END;
         $$",
    );
    execute(
        &engine,
        "CREATE TRIGGER items_before_insert
         BEFORE INSERT ON app.items
         FOR EACH ROW
         EXECUTE FUNCTION app.before_insert()",
    );
    execute(
        &engine,
        "CREATE FUNCTION app.after_insert()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
         INSERT INTO app.audit VALUES (NEW.label);
         RETURN NEW;
         END;
         $$",
    );
    execute(
        &engine,
        "CREATE TRIGGER items_after_insert
         AFTER INSERT ON app.items
         FOR EACH ROW
         EXECUTE FUNCTION app.after_insert()",
    );
    execute(&engine, "INSERT INTO app.items VALUES (1, 'raw')");
    execute(&engine, "INSERT INTO app.items VALUES (2, 'skip')");
    assert_eq!(
        rows(&engine, "SELECT * FROM app.items"),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("normalized".into()),
        ])]
    );
    assert_eq!(
        rows(&engine, "SELECT * FROM app.audit"),
        vec![Row::new(vec![Value::Text("normalized".into())])]
    );

    execute(
        &engine,
        "CREATE FUNCTION app.before_update()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
         NEW.label := OLD.label;
         RETURN NEW;
         END;
         $$",
    );
    execute(
        &engine,
        "CREATE TRIGGER items_before_update
         BEFORE UPDATE ON app.items
         FOR EACH ROW
         EXECUTE FUNCTION app.before_update()",
    );
    execute(
        &engine,
        "UPDATE app.items SET label = 'changed' WHERE id = 1",
    );
    assert_eq!(
        rows(&engine, "SELECT * FROM app.items"),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("normalized".into()),
        ])]
    );

    execute(
        &engine,
        "CREATE FUNCTION app.before_delete()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
         IF OLD.id = 1 THEN
         RETURN NULL;
         END IF;
         RETURN OLD;
         END;
         $$",
    );
    execute(
        &engine,
        "CREATE TRIGGER items_before_delete
         BEFORE DELETE ON app.items
         FOR EACH ROW
         EXECUTE FUNCTION app.before_delete()",
    );
    execute(&engine, "DELETE FROM app.items WHERE id = 1");
    assert_eq!(rows(&engine, "SELECT * FROM app.items").len(), 1);
}

#[test]
fn sequences_are_transactional_session_local_owned_and_durable() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    execute(&engine, "CREATE SCHEMA app");
    execute(
        &engine,
        "CREATE SEQUENCE app.order_ids INCREMENT BY 2 START WITH 10",
    );

    let mut first = engine.connect().expect("first session");
    let undefined = first
        .execute("SELECT currval('app.order_ids')", &[])
        .expect_err("currval before nextval");
    assert_eq!(undefined.sql_state, "55000");
    assert_eq!(
        scalar_i64(&mut first, "SELECT nextval('app.order_ids')"),
        10
    );
    assert_eq!(
        scalar_i64(&mut first, "SELECT currval('app.order_ids')"),
        10
    );

    let mut second = engine.connect().expect("second session");
    let undefined = second
        .execute("SELECT currval('app.order_ids')", &[])
        .expect_err("currval is session local");
    assert_eq!(undefined.sql_state, "55000");

    {
        let mut transaction = first.begin().expect("begin");
        assert_eq!(
            transaction
                .execute("SELECT nextval('app.order_ids')", &[])
                .expect("transactional nextval")
                .find_map(|event| match event {
                    QueryEvent::Batch(batch) => batch.rows.first().cloned(),
                    _ => None,
                }),
            Some(Row::new(vec![Value::Int64(12)]))
        );
        transaction.rollback().expect("rollback");
    }
    assert_eq!(
        scalar_i64(&mut first, "SELECT nextval('app.order_ids')"),
        12
    );
    assert_eq!(
        scalar_i64(&mut first, "SELECT setval('app.order_ids', 20, false)"),
        20
    );
    assert_eq!(
        scalar_i64(&mut first, "SELECT nextval('app.order_ids')"),
        20
    );
    assert_eq!(
        scalar_i64(&mut first, "SELECT nextval('app.order_ids')"),
        22
    );
    execute(
        &engine,
        "ALTER SEQUENCE app.order_ids INCREMENT BY 3 RESTART WITH 30 NO CYCLE",
    );
    assert_eq!(
        scalar_i64(&mut first, "SELECT nextval('app.order_ids')"),
        30
    );
    execute(
        &engine,
        "ALTER SEQUENCE app.order_ids RENAME TO durable_order_ids",
    );
    execute(
        &engine,
        "ALTER SEQUENCE IF EXISTS app.missing RESTART WITH 1",
    );

    execute(&engine, "CREATE TABLE app.owned (id BIGINT PRIMARY KEY)");
    execute(&engine, "CREATE SEQUENCE app.owned_sequence");
    execute(
        &engine,
        "ALTER SEQUENCE app.owned_sequence OWNED BY app.owned.id",
    );
    execute(&engine, "DROP TABLE app.owned CASCADE");
    assert!(
        engine
            .catalog_snapshot()
            .expect("catalog")
            .sequence(
                &Identifier::unquoted("app"),
                &Identifier::unquoted("owned_sequence"),
            )
            .is_none()
    );

    drop(first);
    drop(second);
    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut reopened_session = reopened.connect().expect("reopened session");
    assert_eq!(
        scalar_i64(
            &mut reopened_session,
            "SELECT nextval('app.durable_order_ids')"
        ),
        33
    );
}

#[test]
fn routine_and_trigger_drops_resolve_signatures_dependencies_and_restart() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    execute(&engine, "CREATE SCHEMA app");
    execute(&engine, "CREATE TABLE app.items (id BIGINT PRIMARY KEY)");
    execute(
        &engine,
        "CREATE FUNCTION app.overloaded()
         RETURNS BIGINT
         LANGUAGE plpgsql
         AS $$
         BEGIN
         RETURN 1;
         END;
         $$",
    );
    execute(
        &engine,
        "CREATE FUNCTION app.overloaded(value BIGINT)
         RETURNS BIGINT
         LANGUAGE plpgsql
         AS $$
         BEGIN
         RETURN value;
         END;
         $$",
    );
    let ambiguous = engine
        .connect()
        .expect("connect")
        .execute("DROP FUNCTION app.overloaded", &[])
        .expect_err("an omitted overloaded signature is ambiguous");
    assert_eq!(ambiguous.sql_state, "42725");

    execute(&engine, "DROP FUNCTION app.overloaded(BIGINT)");
    assert_eq!(
        rows(&engine, "SELECT app.overloaded()"),
        vec![Row::new(vec![Value::Int64(1)])]
    );
    let missing_overload = engine
        .connect()
        .expect("connect")
        .execute("SELECT app.overloaded(2)", &[])
        .expect_err("dropped overload");
    assert_eq!(missing_overload.sql_state, "42883");

    execute(
        &engine,
        "CREATE PROCEDURE app.noop()
         LANGUAGE plpgsql
         AS $$
         BEGIN
         PERFORM 1;
         END;
         $$",
    );
    execute(&engine, "DROP PROCEDURE app.noop()");
    execute(&engine, "DROP PROCEDURE IF EXISTS app.noop()");

    execute(
        &engine,
        "CREATE FUNCTION app.guard_insert()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
         RETURN NEW;
         END;
         $$",
    );
    execute(
        &engine,
        "CREATE TRIGGER items_guard
         BEFORE INSERT ON app.items
         FOR EACH ROW
         EXECUTE FUNCTION app.guard_insert()",
    );
    let dependent = engine
        .connect()
        .expect("connect")
        .execute("DROP FUNCTION app.guard_insert() RESTRICT", &[])
        .expect_err("trigger depends on function");
    assert_eq!(dependent.sql_state, "2BP01");
    execute(&engine, "DROP TRIGGER items_guard ON app.items RESTRICT");
    execute(&engine, "DROP TRIGGER IF EXISTS items_guard ON app.items");
    execute(&engine, "DROP FUNCTION app.guard_insert()");
    execute(&engine, "DROP FUNCTION IF EXISTS app.missing()");

    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let catalog = reopened.catalog_snapshot().expect("catalog");
    let schema = catalog
        .schema(&Identifier::unquoted("app"))
        .expect("schema");
    assert_eq!(
        schema
            .routines_named(&Identifier::unquoted("overloaded"))
            .len(),
        1
    );
    assert!(
        schema
            .routines_named(&Identifier::unquoted("guard_insert"))
            .is_empty()
    );
    assert!(
        schema
            .table(&Identifier::unquoted("items"))
            .expect("items")
            .trigger(&Identifier::unquoted("items_guard"))
            .is_none()
    );
}

#[test]
fn plpgsql_case_query_for_and_exception_savepoints_use_candidate_state() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    execute(&engine, "CREATE SCHEMA app");
    execute(
        &engine,
        "CREATE TABLE app.loop_items (id BIGINT PRIMARY KEY)",
    );
    execute(&engine, "INSERT INTO app.loop_items VALUES (1)");
    execute(&engine, "INSERT INTO app.loop_items VALUES (2)");
    execute(
        &engine,
        "CREATE FUNCTION app.last_loop_value()
         RETURNS BIGINT
         LANGUAGE plpgsql
         AS $$
         DECLARE
         item BIGINT;
         answer BIGINT := 0;
         BEGIN
         FOR item IN SELECT id FROM app.loop_items ORDER BY id LOOP
         CASE item
         WHEN 1 THEN
         answer := 1;
         WHEN 2 THEN
         answer := 2;
         ELSE
         answer := 0;
         END CASE;
         END LOOP;
         RETURN answer;
         END;
         $$",
    );
    assert_eq!(
        rows(&engine, "SELECT app.last_loop_value()"),
        vec![Row::new(vec![Value::Int64(2)])]
    );

    execute(
        &engine,
        "CREATE FUNCTION app.catch_duplicate()
         RETURNS BIGINT
         LANGUAGE plpgsql
         AS $$
         BEGIN
         INSERT INTO app.loop_items VALUES (2);
         RETURN 0;
         EXCEPTION
         WHEN SQLSTATE '23505' THEN
         RETURN 7;
         WHEN OTHERS THEN
         RETURN 9;
         END;
         $$",
    );
    assert_eq!(
        rows(&engine, "SELECT app.catch_duplicate()"),
        vec![Row::new(vec![Value::Int64(7)])]
    );
    assert_eq!(rows(&engine, "SELECT * FROM app.loop_items").len(), 2);
}
