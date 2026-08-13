use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ordadb_engine::{DatabaseNotification, Engine, EngineConfig, Session, SessionAuthorization};
use ordadb_transaction::{DeterministicFaultInjector, FaultInjector, FaultPoint};
use ordadb_types::{DbNoticeSeverity, QueryEvent, Row, Value};
use tempfile::tempdir;

fn execute(session: &mut Session, sql: &str) {
    session
        .execute(sql, &[])
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .for_each(drop);
}

fn rows(session: &mut Session, sql: &str) -> Vec<Row> {
    session
        .execute(sql, &[])
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .filter_map(|event| match event {
            QueryEvent::Batch(batch) => Some(batch.rows),
            _ => None,
        })
        .flatten()
        .collect()
}

#[test]
fn notifications_publish_only_after_commit_and_respect_session_state() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut listener = engine.connect().expect("listener");
    let mut sender = engine.connect().expect("sender");
    listener.set_backend_process_id(101).expect("listener pid");
    sender.set_backend_process_id(202).expect("sender pid");

    execute(&mut listener, "LISTEN core_events");
    execute(&mut sender, "BEGIN");
    execute(&mut sender, "NOTIFY core_events, 'committed'");
    assert!(
        listener
            .drain_notifications()
            .expect("precommit drain")
            .is_empty()
    );
    execute(&mut sender, "COMMIT");
    assert_eq!(
        listener.drain_notifications().expect("committed drain"),
        vec![DatabaseNotification {
            sender_process_id: 202,
            channel: "core_events".into(),
            payload: "committed".into(),
        }]
    );

    execute(&mut sender, "BEGIN");
    assert_eq!(
        rows(
            &mut sender,
            "SELECT pg_notify('core_events', 'from-function')",
        ),
        vec![Row::new(vec![Value::Null])]
    );
    assert!(
        listener
            .drain_notifications()
            .expect("pg_notify precommit drain")
            .is_empty()
    );
    execute(&mut sender, "COMMIT");
    assert_eq!(
        listener.drain_notifications().expect("pg_notify drain"),
        vec![DatabaseNotification {
            sender_process_id: 202,
            channel: "core_events".into(),
            payload: "from-function".into(),
        }]
    );

    execute(
        &mut sender,
        "CREATE PROCEDURE emit_core_event(IN event_payload TEXT) LANGUAGE plpgsql AS $$
         BEGIN
           PERFORM pg_notify('core_events', event_payload);
           PERFORM pg_notify('core_events', event_payload);
         END
         $$",
    );
    execute(&mut sender, "BEGIN");
    execute(&mut sender, "CALL emit_core_event('from-procedure')");
    assert!(
        listener
            .drain_notifications()
            .expect("procedure precommit drain")
            .is_empty()
    );
    execute(&mut sender, "COMMIT");
    assert_eq!(
        listener
            .drain_notifications()
            .expect("procedure commit drain"),
        vec![DatabaseNotification {
            sender_process_id: 202,
            channel: "core_events".into(),
            payload: "from-procedure".into(),
        }]
    );

    execute(&mut sender, "BEGIN");
    execute(&mut sender, "CALL emit_core_event('procedure-rollback')");
    execute(&mut sender, "ROLLBACK");
    assert!(
        listener
            .drain_notifications()
            .expect("procedure rollback drain")
            .is_empty()
    );

    execute(
        &mut sender,
        "DO LANGUAGE plpgsql $$
         BEGIN
           BEGIN
             NOTIFY core_events, 'exception-rollback';
             RAISE EXCEPTION 'rollback notification' USING ERRCODE = '23505';
           EXCEPTION
           WHEN unique_violation THEN
             PERFORM 1;
           END;
         END
         $$",
    );
    assert!(
        listener
            .drain_notifications()
            .expect("exception rollback drain")
            .is_empty()
    );

    execute(&mut sender, "BEGIN");
    execute(&mut sender, "NOTIFY core_events, 'rolled-back'");
    execute(&mut sender, "ROLLBACK");
    assert!(
        listener
            .drain_notifications()
            .expect("rollback drain")
            .is_empty()
    );

    execute(&mut sender, "BEGIN");
    execute(&mut sender, "NOTIFY core_events, 'deduplicated'");
    execute(&mut sender, "SAVEPOINT notification_savepoint");
    execute(&mut sender, "NOTIFY core_events, 'discarded-by-savepoint'");
    execute(&mut sender, "ROLLBACK TO SAVEPOINT notification_savepoint");
    execute(&mut sender, "NOTIFY core_events, 'deduplicated'");
    execute(&mut sender, "COMMIT");
    assert_eq!(
        listener.drain_notifications().expect("savepoint drain"),
        vec![DatabaseNotification {
            sender_process_id: 202,
            channel: "core_events".into(),
            payload: "deduplicated".into(),
        }]
    );

    execute(&mut listener, "DISCARD ALL");
    execute(&mut sender, "NOTIFY core_events, 'after-discard'");
    assert!(
        listener
            .drain_notifications()
            .expect("discard drain")
            .is_empty()
    );

    execute(&mut listener, "BEGIN");
    let error = listener
        .execute("DISCARD ALL", &[])
        .expect_err("DISCARD ALL in transaction");
    assert_eq!(error.sql_state, "25001");
    execute(&mut listener, "ROLLBACK");
}

#[test]
fn do_and_reindex_use_atomic_engine_candidates() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE TABLE maintenance_probe (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)",
    );
    execute(
        &mut session,
        "CREATE INDEX maintenance_probe_payload ON maintenance_probe (payload)",
    );
    execute(
        &mut session,
        "DO LANGUAGE plpgsql $$ BEGIN INSERT INTO maintenance_probe VALUES (1, 'ready'); END $$",
    );
    execute(
        &mut session,
        "REINDEX INDEX public.maintenance_probe_payload",
    );
    execute(&mut session, "REINDEX TABLE public.maintenance_probe");
    assert_eq!(
        rows(
            &mut session,
            "SELECT id, payload FROM maintenance_probe WHERE payload = 'ready'",
        ),
        vec![Row::new(
            vec![Value::Int64(1), Value::Text("ready".into()),]
        )]
    );
}

#[test]
fn reindex_enforces_ownership_cancellation_and_reopen() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut alice = engine
        .connect_authenticated(SessionAuthorization::new("alice", false).expect("alice"))
        .expect("alice session");
    execute(
        &mut alice,
        "CREATE TABLE owned_reindex (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)",
    );
    execute(
        &mut alice,
        "CREATE INDEX owned_reindex_payload ON owned_reindex (payload)",
    );
    execute(
        &mut alice,
        "INSERT INTO owned_reindex VALUES (1, 'durable')",
    );

    let mut bob = engine
        .connect_authenticated(SessionAuthorization::new("bob", false).expect("bob"))
        .expect("bob session");
    let denied = bob
        .execute("REINDEX INDEX public.owned_reindex_payload", &[])
        .expect_err("non-owner reindex");
    assert_eq!(denied.sql_state, "42501");

    let cancellation = Arc::new(AtomicBool::new(true));
    let cancelled = alice
        .execute_stream_with_cancellation(
            "REINDEX TABLE public.owned_reindex",
            &[],
            Arc::clone(&cancellation),
        )
        .expect_err("cancelled reindex");
    assert_eq!(cancelled.sql_state, "57014");
    cancellation.store(false, Ordering::Release);

    let mut administrator = engine
        .connect_authenticated(
            SessionAuthorization::new("administrator", true).expect("administrator"),
        )
        .expect("administrator session");
    execute(
        &mut administrator,
        "REINDEX INDEX public.owned_reindex_payload",
    );
    drop(administrator);
    drop(bob);
    drop(alice);
    drop(engine);

    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut session = reopened.connect().expect("reconnect");
    assert_eq!(
        rows(
            &mut session,
            "SELECT id, payload FROM owned_reindex WHERE payload = 'durable'",
        ),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("durable".into()),
        ])]
    );
}

#[test]
fn reindex_faults_reopen_to_one_complete_authority() {
    for (fault_point, commit_is_durable) in [
        (FaultPoint::AfterDataSync, false),
        (FaultPoint::AfterCommitFlush, true),
    ] {
        let directory = tempdir().expect("tempdir");
        let baseline_generation = {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open baseline");
            let mut session = engine.connect().expect("baseline session");
            execute(
                &mut session,
                "CREATE TABLE reindex_recovery (
                    id BIGINT PRIMARY KEY,
                    payload TEXT NOT NULL
                 )",
            );
            execute(
                &mut session,
                "CREATE INDEX reindex_recovery_payload ON reindex_recovery (payload)",
            );
            execute(
                &mut session,
                "INSERT INTO reindex_recovery VALUES (1, 'durable')",
            );
            engine
                .status_snapshot()
                .expect("baseline status")
                .generation
        };

        let injector = DeterministicFaultInjector::new();
        let fault_injector: Arc<dyn FaultInjector> = injector.clone();
        let engine =
            Engine::open_with_fault_injector(EngineConfig::new(directory.path()), fault_injector)
                .expect("open fault engine");
        let mut session = engine.connect().expect("fault session");
        injector.arm(fault_point, 1).expect("arm reindex fault");
        let error = session
            .execute("REINDEX INDEX public.reindex_recovery_payload", &[])
            .expect_err("injected reindex failure");
        assert_eq!(error.sql_state, "58030");
        drop(session);
        drop(engine);

        let recovered = Engine::open(EngineConfig::new(directory.path())).expect("recover reindex");
        let expected_generation = baseline_generation + u64::from(commit_is_durable);
        assert_eq!(
            recovered
                .status_snapshot()
                .expect("recovered status")
                .generation,
            expected_generation
        );
        let mut session = recovered.connect().expect("recovered session");
        assert_eq!(
            rows(
                &mut session,
                "SELECT id, payload FROM reindex_recovery WHERE payload = 'durable'",
            ),
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("durable".into()),
            ])]
        );
    }
}

#[test]
fn plpgsql_raise_emits_typed_notices_and_asserts_with_sqlstate() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    let events = session
        .execute(
            "DO LANGUAGE plpgsql $$ BEGIN
             RAISE NOTICE 'ready';
             RAISE WARNING 'careful';
             ASSERT true;
             END $$",
            &[],
        )
        .expect("execute notices")
        .collect::<Vec<_>>();
    let notices = events
        .iter()
        .filter_map(|event| match event {
            QueryEvent::Notice(notice) => Some(notice),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(notices.len(), 2);
    assert_eq!(notices[0].severity, DbNoticeSeverity::Notice);
    assert_eq!(notices[0].message, "ready");
    assert_eq!(notices[1].severity, DbNoticeSeverity::Warning);
    assert_eq!(notices[1].sql_state, "01000");
    assert_eq!(notices[1].message, "careful");

    let error = session
        .execute(
            "DO LANGUAGE plpgsql $$ BEGIN ASSERT false, 'broken invariant'; END $$",
            &[],
        )
        .expect_err("assert failure");
    assert_eq!(error.sql_state, "P0004");
    assert_eq!(error.message, "broken invariant");
}

#[test]
fn plpgsql_transaction_termination_rejects_atomic_invocations_with_2d000() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");

    let do_error = session
        .execute("DO LANGUAGE plpgsql $$ BEGIN COMMIT; END $$", &[])
        .expect_err("DO transaction termination");
    assert_eq!(do_error.sql_state, "2D000");

    execute(
        &mut session,
        "CREATE FUNCTION invalid_commit() RETURNS BIGINT LANGUAGE plpgsql AS $$
         BEGIN
           COMMIT;
           RETURN 1;
         END
         $$",
    );
    let function_error = session
        .execute("SELECT invalid_commit()", &[])
        .expect_err("function transaction termination");
    assert_eq!(function_error.sql_state, "2D000");
}

#[test]
fn nested_plpgsql_exception_savepoints_roll_back_to_the_matching_block() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE TABLE nested_effects (id BIGINT PRIMARY KEY)",
    );
    execute(
        &mut session,
        "DO LANGUAGE plpgsql $$ BEGIN
         BEGIN
         INSERT INTO nested_effects VALUES (1);
         RAISE EXCEPTION 'rollback inner and outer' USING ERRCODE = '23505';
         EXCEPTION
         WHEN division_by_zero THEN
         INSERT INTO nested_effects VALUES (99);
         END;
         EXCEPTION
         WHEN unique_violation THEN
         INSERT INTO nested_effects VALUES (2);
         END $$",
    );
    assert_eq!(
        rows(&mut session, "SELECT id FROM nested_effects ORDER BY id"),
        vec![Row::new(vec![Value::Int64(2)])]
    );
}

#[test]
fn routine_output_modes_return_typed_call_and_function_rows() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE PROCEDURE mode_probe(IN input_value BIGINT, INOUT counter BIGINT, OUT doubled BIGINT) \
         LANGUAGE plpgsql AS $$ BEGIN counter := counter + input_value; doubled := input_value * 2; RETURN; END $$",
    );
    assert_eq!(
        rows(&mut session, "CALL mode_probe(3, 4)"),
        vec![Row::new(vec![Value::Int64(7), Value::Int64(6)])]
    );

    execute(
        &mut session,
        "CREATE FUNCTION output_probe(IN input_value BIGINT, OUT doubled BIGINT) \
         LANGUAGE plpgsql AS $$ BEGIN doubled := input_value * 2; RETURN; END $$",
    );
    assert_eq!(
        rows(&mut session, "SELECT output_probe(5)"),
        vec![Row::new(vec![Value::Int64(10)])]
    );
}

#[test]
fn top_level_procedure_transaction_boundaries_are_durable_and_context_checked() {
    let directory = tempdir().expect("tempdir");
    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
        let mut session = engine.connect().expect("session");
        execute(
            &mut session,
            "CREATE TABLE procedure_tx_rows (id BIGINT PRIMARY KEY)",
        );
        execute(
            &mut session,
            "CREATE PROCEDURE transaction_steps() LANGUAGE plpgsql AS $$
             BEGIN
             INSERT INTO procedure_tx_rows VALUES (1);
             COMMIT;
             INSERT INTO procedure_tx_rows VALUES (2);
             ROLLBACK;
             INSERT INTO procedure_tx_rows VALUES (3);
             COMMIT AND CHAIN;
             INSERT INTO procedure_tx_rows VALUES (4);
             END $$",
        );
        execute(&mut session, "CALL transaction_steps()");
        assert_eq!(
            rows(&mut session, "SELECT id FROM procedure_tx_rows ORDER BY id",),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(3)]),
                Row::new(vec![Value::Int64(4)]),
            ]
        );

        execute(
            &mut session,
            "CREATE PROCEDURE commit_then_fail() LANGUAGE plpgsql AS $$
             BEGIN
             INSERT INTO procedure_tx_rows VALUES (5);
             COMMIT;
             INSERT INTO procedure_tx_rows VALUES (1);
             END $$",
        );
        let failed = session
            .execute("CALL commit_then_fail()", &[])
            .expect_err("post-commit segment fails");
        assert_eq!(failed.sql_state, "23505");
        assert_eq!(
            rows(&mut session, "SELECT id FROM procedure_tx_rows ORDER BY id",),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(3)]),
                Row::new(vec![Value::Int64(4)]),
                Row::new(vec![Value::Int64(5)]),
            ]
        );

        execute(
            &mut session,
            "CREATE PROCEDURE forbidden_boundary() LANGUAGE plpgsql AS $$
             BEGIN
             COMMIT;
             END $$",
        );
        execute(&mut session, "BEGIN");
        let explicit = session
            .execute("CALL forbidden_boundary()", &[])
            .expect_err("explicit transaction rejects procedure commit");
        assert_eq!(explicit.sql_state, "2D000");
        execute(&mut session, "ROLLBACK");

        execute(
            &mut session,
            "CREATE FUNCTION forbidden_function_boundary() RETURNS BIGINT LANGUAGE plpgsql AS $$
             BEGIN
             COMMIT;
             RETURN 1;
             END $$",
        );
        let function = session
            .execute("SELECT forbidden_function_boundary()", &[])
            .expect_err("function rejects transaction control");
        assert_eq!(function.sql_state, "2D000");
    }

    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut session = reopened.connect().expect("reopened session");
    assert_eq!(
        rows(&mut session, "SELECT id FROM procedure_tx_rows ORDER BY id",),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(3)]),
            Row::new(vec![Value::Int64(4)]),
            Row::new(vec![Value::Int64(5)]),
        ]
    );
}

#[test]
fn procedure_transaction_segments_preserve_session_currval_at_commit_boundaries() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE SEQUENCE procedure_sequence START WITH 100",
    );
    execute(
        &mut session,
        "CREATE TABLE procedure_sequence_audit (
            step BIGINT PRIMARY KEY,
            observed BIGINT NOT NULL
         )",
    );
    execute(
        &mut session,
        "CREATE PROCEDURE sequence_segments() LANGUAGE plpgsql AS $$
         DECLARE current_value BIGINT;
         BEGIN
           SELECT nextval('procedure_sequence') INTO current_value;
           SELECT currval('procedure_sequence') INTO current_value;
           INSERT INTO procedure_sequence_audit VALUES (1, current_value);
           COMMIT;

           SELECT currval('procedure_sequence') INTO current_value;
           INSERT INTO procedure_sequence_audit VALUES (2, current_value);
           SELECT nextval('procedure_sequence') INTO current_value;
           INSERT INTO procedure_sequence_audit VALUES (3, current_value);
           ROLLBACK;

           SELECT currval('procedure_sequence') INTO current_value;
           INSERT INTO procedure_sequence_audit VALUES (4, current_value);
         END $$",
    );
    execute(&mut session, "CALL sequence_segments()");
    assert_eq!(
        rows(
            &mut session,
            "SELECT step, observed FROM procedure_sequence_audit ORDER BY step",
        ),
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(100)]),
            Row::new(vec![Value::Int64(4), Value::Int64(100)]),
        ]
    );
    assert_eq!(
        rows(&mut session, "SELECT currval('procedure_sequence')"),
        vec![Row::new(vec![Value::Int64(100)])]
    );

    execute(
        &mut session,
        "CREATE PROCEDURE sequence_commit_then_fail() LANGUAGE plpgsql AS $$
         DECLARE current_value BIGINT;
         BEGIN
           SELECT nextval('procedure_sequence') INTO current_value;
           INSERT INTO procedure_sequence_audit VALUES (10, current_value);
           COMMIT;
           SELECT nextval('procedure_sequence') INTO current_value;
           INSERT INTO procedure_sequence_audit VALUES (10, current_value);
         END $$",
    );
    let failure = session
        .execute("CALL sequence_commit_then_fail()", &[])
        .expect_err("later procedure segment fails");
    assert_eq!(failure.sql_state, "23505");
    assert_eq!(
        rows(
            &mut session,
            "SELECT observed FROM procedure_sequence_audit WHERE step = 10",
        ),
        vec![Row::new(vec![Value::Int64(101)])]
    );
    assert_eq!(
        rows(&mut session, "SELECT currval('procedure_sequence')"),
        vec![Row::new(vec![Value::Int64(101)])]
    );
}

#[test]
fn plpgsql_records_and_rowtypes_preserve_query_schemas_across_iterators() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE TABLE record_source (
            id BIGINT PRIMARY KEY,
            payload TEXT NOT NULL
         )",
    );
    execute(
        &mut session,
        "CREATE TABLE record_audit (
            sequence_id BIGINT PRIMARY KEY,
            source_id BIGINT NOT NULL,
            payload TEXT NOT NULL
         )",
    );
    execute(
        &mut session,
        "INSERT INTO record_source VALUES (1, 'one'), (2, 'two')",
    );
    execute(
        &mut session,
        "DO LANGUAGE plpgsql $$
         DECLARE
           item RECORD;
           typed public.record_source%ROWTYPE;
           source_cursor SCROLL CURSOR FOR SELECT id, payload FROM record_source ORDER BY id;
         BEGIN
           SELECT id, payload INTO item FROM record_source WHERE id = 1;
           item.payload := 'updated';
           INSERT INTO record_audit VALUES (1, item.id, item.payload);

           FOR item IN SELECT id, payload FROM record_source ORDER BY id LOOP
             IF item.id = 1 THEN
               INSERT INTO record_audit VALUES (11, item.id, item.payload);
             ELSE
               INSERT INTO record_audit VALUES (12, item.id, item.payload);
             END IF;
           END LOOP;

           OPEN source_cursor;
           FETCH LAST FROM source_cursor INTO typed;
           CLOSE source_cursor;
           INSERT INTO record_audit VALUES (20, typed.id, typed.payload);
         END
         $$",
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT sequence_id, source_id, payload FROM record_audit ORDER BY sequence_id",
        ),
        vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::Text("updated".into()),
            ]),
            Row::new(vec![
                Value::Int64(11),
                Value::Int64(1),
                Value::Text("one".into()),
            ]),
            Row::new(vec![
                Value::Int64(12),
                Value::Int64(2),
                Value::Text("two".into()),
            ]),
            Row::new(vec![
                Value::Int64(20),
                Value::Int64(2),
                Value::Text("two".into()),
            ]),
        ]
    );

    let unassigned = session
        .execute(
            "DO LANGUAGE plpgsql $$
             DECLARE missing RECORD;
             BEGIN
               INSERT INTO record_audit VALUES (30, missing.id, 'invalid');
             END
             $$",
            &[],
        )
        .expect_err("unassigned record field");
    assert_eq!(unassigned.sql_state, "55000");
    assert!(
        rows(
            &mut session,
            "SELECT source_id FROM record_audit WHERE sequence_id = 30"
        )
        .is_empty()
    );
}

#[test]
fn statement_triggers_fire_for_zero_rows_in_stable_order_with_tg_context() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE TABLE trigger_target (id BIGINT PRIMARY KEY)",
    );
    execute(
        &mut session,
        "CREATE TABLE trigger_audit (
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
        "CREATE FUNCTION capture_statement_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO trigger_audit VALUES (
             TG_OP, TG_WHEN, TG_LEVEL, TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_RELID
           );
           RETURN NULL;
         END
         $$",
    );
    execute(
        &mut session,
        "CREATE TRIGGER z_before_update BEFORE UPDATE ON trigger_target
         FOR EACH STATEMENT EXECUTE FUNCTION capture_statement_trigger()",
    );
    execute(
        &mut session,
        "CREATE TRIGGER a_before_update BEFORE UPDATE ON trigger_target
         FOR EACH STATEMENT EXECUTE FUNCTION capture_statement_trigger()",
    );
    execute(
        &mut session,
        "CREATE TRIGGER after_update AFTER UPDATE ON trigger_target
         FOR EACH STATEMENT EXECUTE FUNCTION capture_statement_trigger()",
    );

    execute(
        &mut session,
        "UPDATE trigger_target SET id = 2 WHERE id = 999",
    );
    let audit = rows(
        &mut session,
        "SELECT operation, timing, level_name, trigger_name, schema_name, table_name, relation_oid
         FROM trigger_audit",
    );
    assert_eq!(audit.len(), 3);
    assert_eq!(
        audit
            .iter()
            .map(|row| row.values[3].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Text("a_before_update".into()),
            Value::Text("z_before_update".into()),
            Value::Text("after_update".into()),
        ]
    );
    for row in audit {
        assert_eq!(row.values[0], Value::Text("UPDATE".into()));
        assert_eq!(row.values[2], Value::Text("STATEMENT".into()));
        assert_eq!(row.values[4], Value::Text("public".into()));
        assert_eq!(row.values[5], Value::Text("trigger_target".into()));
        assert!(matches!(row.values[6], Value::Int64(value) if value > 0));
    }
}
