use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::Arc;

use ordadb_engine::{Engine, EngineConfig, TransactionStatus};
use ordadb_transaction::{DeterministicFaultInjector, FaultInjector, FaultPoint};
use ordadb_types::{QueryEvent, Row, Value};
use tempfile::tempdir;

fn rows(events: impl Iterator<Item = QueryEvent>) -> Vec<Row> {
    events
        .filter_map(|event| match event {
            QueryEvent::Batch(batch) => Some(batch.rows),
            _ => None,
        })
        .flatten()
        .collect()
}

fn execute(session: &mut ordadb_engine::Session, sql: &str) {
    session
        .execute(sql, &[])
        .expect("execute statement")
        .for_each(drop);
}

#[test]
fn programmatic_transactions_use_read_committed_and_release_the_single_writer() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut first = engine.connect().expect("first session");
    let mut second = engine.connect().expect("second session");
    execute(
        &mut first,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    );

    let mut transaction = first.begin().expect("begin read-only transaction");
    assert!(
        rows(
            transaction
                .execute("SELECT * FROM events", &[])
                .expect("initial read")
        )
        .is_empty()
    );

    execute(
        &mut second,
        "INSERT INTO events VALUES (1, 'committed after begin')",
    );
    assert_eq!(
        rows(
            transaction
                .execute("SELECT id FROM events ORDER BY id", &[])
                .expect("read committed refresh")
        ),
        vec![Row::new(vec![Value::Int64(1)])]
    );

    transaction
        .execute("INSERT INTO events VALUES (2, 'own write')", &[])
        .expect("first transaction write")
        .for_each(drop);
    assert_eq!(
        rows(
            transaction
                .execute("SELECT id FROM events ORDER BY id", &[])
                .expect("own-write read")
        ),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );

    assert_eq!(
        second
            .execute("INSERT INTO events VALUES (3, 'blocked')", &[])
            .expect_err("single writer must reject a competitor")
            .sql_state,
        "55P03"
    );
    transaction.rollback().expect("rollback and release");

    execute(
        &mut second,
        "INSERT INTO events VALUES (3, 'writer released')",
    );
    assert_eq!(
        rows(
            second
                .execute("SELECT id FROM events ORDER BY id", &[])
                .expect("committed rows")
        ),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(3)]),
        ]
    );
}

#[test]
fn dropping_a_programmatic_writer_discards_work_and_releases_the_lease() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut first = engine.connect().expect("first session");
    let mut second = engine.connect().expect("second session");
    execute(
        &mut first,
        "CREATE TABLE leases (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    );

    {
        let mut transaction = first.begin().expect("begin");
        transaction
            .execute("INSERT INTO leases VALUES (1, 'discarded')", &[])
            .expect("uncommitted insert")
            .for_each(drop);
    }

    execute(&mut second, "INSERT INTO leases VALUES (2, 'committed')");
    assert_eq!(
        rows(
            second
                .execute("SELECT id FROM leases", &[])
                .expect("select committed rows")
        ),
        vec![Row::new(vec![Value::Int64(2)])]
    );
}

#[test]
fn sql_transaction_control_tracks_failed_state_and_rolls_back_all_work() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE TABLE accounts (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    );

    execute(&mut session, "BEGIN");
    assert_eq!(session.transaction_status(), TransactionStatus::Active);
    execute(&mut session, "INSERT INTO accounts VALUES (1, 'candidate')");

    assert_eq!(
        session
            .execute("INSERT INTO accounts VALUES (1, 'duplicate')", &[])
            .expect_err("duplicate must fail the SQL transaction")
            .sql_state,
        "23505"
    );
    assert_eq!(session.transaction_status(), TransactionStatus::Failed);
    assert_eq!(
        session
            .execute("SELECT * FROM accounts", &[])
            .expect_err("failed transaction rejects statements")
            .sql_state,
        "25P02"
    );
    assert_eq!(
        session
            .execute("COMMIT", &[])
            .expect_err("failed transaction cannot commit")
            .sql_state,
        "25P02"
    );

    execute(&mut session, "ROLLBACK");
    assert_eq!(session.transaction_status(), TransactionStatus::Idle);
    assert!(
        rows(
            session
                .execute("SELECT * FROM accounts", &[])
                .expect("rolled back table")
        )
        .is_empty()
    );

    assert_eq!(
        session
            .execute("COMMIT", &[])
            .expect_err("no active transaction")
            .sql_state,
        "25P01"
    );
    execute(&mut session, "BEGIN");
    assert_eq!(
        session
            .execute("START TRANSACTION", &[])
            .expect_err("duplicate begin")
            .sql_state,
        "25001"
    );
    execute(&mut session, "ROLLBACK");
}

#[test]
fn durable_wal_reopens_commits_and_repairs_only_an_incomplete_tail() {
    let directory = tempdir().expect("tempdir");
    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
        let mut session = engine.connect().expect("session");
        execute(
            &mut session,
            "CREATE TABLE durable (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
        );
        execute(&mut session, "BEGIN");
        execute(&mut session, "INSERT INTO durable VALUES (1, 'wal backed')");
        execute(&mut session, "COMMIT");
        engine.checkpoint().expect("checkpoint");
    }

    let wal_path = directory.path().join("ordadb.wal");
    let valid_length = fs::metadata(&wal_path).expect("WAL metadata").len();
    assert!(valid_length > 0);
    OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .expect("open WAL tail")
        .write_all(b"ORDA")
        .expect("append incomplete WAL header");

    let engine = Engine::open(EngineConfig::new(directory.path())).expect("recover WAL tail");
    let mut session = engine.connect().expect("session");
    assert_eq!(
        rows(
            session
                .execute("SELECT * FROM durable", &[])
                .expect("recovered row")
        ),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("wal backed".into()),
        ])]
    );
}

#[test]
fn complete_wal_corruption_refuses_to_open() {
    let directory = tempdir().expect("tempdir");
    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
        let mut session = engine.connect().expect("session");
        execute(
            &mut session,
            "CREATE TABLE checksums (id BIGINT PRIMARY KEY)",
        );
        execute(&mut session, "INSERT INTO checksums VALUES (1)");
    }

    let wal_path = directory.path().join("ordadb.wal");
    let mut wal = fs::read(&wal_path).expect("read WAL");
    let last = wal.last_mut().expect("non-empty WAL");
    *last ^= 0xff;
    fs::write(&wal_path, wal).expect("corrupt final complete record");

    assert_eq!(
        Engine::open(EngineConfig::new(directory.path()))
            .expect_err("complete WAL corruption must fail")
            .sql_state,
        "XX001"
    );
}

#[test]
fn injected_commit_crashes_converge_to_exact_loser_or_winner_state() {
    for (point, row_is_committed) in [
        (FaultPoint::BeforeDataPageWrite, false),
        (FaultPoint::AfterDataSync, false),
        (FaultPoint::AfterCommitFlush, true),
    ] {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open baseline");
            let mut session = engine.connect().expect("baseline session");
            execute(
                &mut session,
                "CREATE TABLE crash_matrix (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
            );
        }

        let injector = DeterministicFaultInjector::new();
        let fault_injector: Arc<dyn FaultInjector> = injector.clone();
        let engine =
            Engine::open_with_fault_injector(EngineConfig::new(directory.path()), fault_injector)
                .expect("open injected engine");
        let mut session = engine.connect().expect("injected session");
        injector.arm(point, 1).expect("arm fault");
        let error = session
            .execute("INSERT INTO crash_matrix VALUES (1, 'boundary')", &[])
            .expect_err("fault must interrupt commit");
        assert_eq!(error.sql_state, "58030", "fault point {point:?}");
        drop(session);
        drop(engine);

        let recovered = Engine::open(EngineConfig::new(directory.path())).expect("recover");
        let mut recovered_session = recovered.connect().expect("recovered session");
        let recovered_rows = rows(
            recovered_session
                .execute("SELECT * FROM crash_matrix", &[])
                .expect("read recovered state"),
        );
        let expected = if row_is_committed {
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("boundary".into()),
            ])]
        } else {
            Vec::new()
        };
        assert_eq!(recovered_rows, expected, "fault point {point:?}");
    }
}
