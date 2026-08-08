use ordadb_engine::{Engine, EngineConfig};
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

#[test]
fn public_api_reopens_persisted_catalog_rows_and_generation() {
    let directory = tempdir().expect("tempdir");
    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
        let mut session = engine.connect().expect("connect");
        session
            .execute(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
                &[],
            )
            .expect("create table")
            .for_each(drop);
        session
            .execute("INSERT INTO items VALUES (1, 'first'), (2, 'second')", &[])
            .expect("insert")
            .for_each(drop);
    }

    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
        let mut session = engine.connect().expect("connect");
        assert_eq!(
            rows(
                session
                    .execute("SELECT * FROM items ORDER BY id", &[])
                    .expect("select")
            ),
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("first".into())]),
                Row::new(vec![Value::Int64(2), Value::Text("second".into())]),
            ]
        );
        session
            .execute("UPDATE items SET name = 'updated' WHERE id = 2", &[])
            .expect("update")
            .for_each(drop);
        session
            .execute("DELETE FROM items WHERE id = 1", &[])
            .expect("delete")
            .for_each(drop);
        session
            .execute("INSERT INTO items VALUES (3, 'third')", &[])
            .expect("continue insert")
            .for_each(drop);
    }

    let engine = Engine::open(EngineConfig::new(directory.path())).expect("second reopen");
    let mut session = engine.connect().expect("connect");
    assert_eq!(
        rows(
            session
                .execute("SELECT * FROM items ORDER BY id", &[])
                .expect("select")
        ),
        vec![
            Row::new(vec![Value::Int64(2), Value::Text("updated".into())]),
            Row::new(vec![Value::Int64(3), Value::Text("third".into())]),
        ]
    );
}

#[test]
fn rolled_back_and_failed_writes_do_not_reappear_but_disjoint_writer_does() {
    let directory = tempdir().expect("tempdir");
    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
        let mut first = engine.connect().expect("first");
        let mut second = engine.connect().expect("second");
        first
            .execute(
                "CREATE TABLE events (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
                &[],
            )
            .expect("create table")
            .for_each(drop);

        {
            let mut transaction = first.begin().expect("begin rollback");
            transaction
                .execute("INSERT INTO events VALUES (1, 'rolled back')", &[])
                .expect("insert rollback")
                .for_each(drop);
            transaction.rollback().expect("rollback");
        }

        first
            .execute("INSERT INTO events VALUES (2, 'committed')", &[])
            .expect("seed")
            .for_each(drop);
        let duplicate = first
            .execute(
                "INSERT INTO events VALUES (3, 'candidate'), (2, 'duplicate')",
                &[],
            )
            .expect_err("statement atomicity");
        assert_eq!(duplicate.sql_state, "23505");

        let mut transaction = first.begin().expect("begin writer");
        transaction
            .execute("INSERT INTO events VALUES (4, 'rolled back writer')", &[])
            .expect("transaction insert")
            .for_each(drop);
        second
            .execute("INSERT INTO events VALUES (5, 'concurrent')", &[])
            .expect("disjoint concurrent writer")
            .for_each(drop);
        transaction.rollback().expect("rollback writer");
    }

    let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut session = engine.connect().expect("connect");
    assert_eq!(
        rows(
            session
                .execute("SELECT id FROM events ORDER BY id", &[])
                .expect("select")
        ),
        vec![
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(5)]),
        ]
    );
}

#[test]
fn persistence_failure_does_not_publish_candidate_state() {
    let directory = tempdir().expect("tempdir");
    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
        let mut session = engine.connect().expect("connect");
        session
            .execute(
                "CREATE TABLE payloads (id BIGINT PRIMARY KEY, body TEXT NOT NULL)",
                &[],
            )
            .expect("create table")
            .for_each(drop);
        let statement = format!(
            "INSERT INTO payloads VALUES (1, '{}')",
            "x".repeat(ordadb_storage::PAGE_SIZE)
        );
        assert_eq!(
            session
                .execute(&statement, &[])
                .expect_err("oversized tuple")
                .sql_state,
            "54000"
        );
        assert!(
            rows(
                session
                    .execute("SELECT * FROM payloads", &[])
                    .expect("select")
            )
            .is_empty()
        );
    }

    let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut session = engine.connect().expect("connect");
    assert!(
        rows(
            session
                .execute("SELECT * FROM payloads", &[])
                .expect("select")
        )
        .is_empty()
    );
}
