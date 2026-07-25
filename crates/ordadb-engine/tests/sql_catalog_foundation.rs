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
fn public_api_executes_committed_and_rolled_back_work() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open persistent engine");
    let mut session = engine.connect().expect("connect");
    session
        .execute("CREATE SCHEMA app", &[])
        .expect("create schema")
        .for_each(drop);
    session
        .execute(
            "CREATE TABLE app.items (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
            &[],
        )
        .expect("create table")
        .for_each(drop);

    {
        let mut transaction = session.begin().expect("begin");
        transaction
            .execute("INSERT INTO app.items VALUES (1, 'discarded')", &[])
            .expect("insert in transaction")
            .for_each(drop);
        transaction.rollback().expect("rollback");
    }
    assert!(
        rows(
            session
                .execute("SELECT * FROM app.items", &[])
                .expect("select after rollback")
        )
        .is_empty()
    );

    {
        let mut transaction = session.begin().expect("begin");
        transaction
            .execute(
                "INSERT INTO app.items VALUES ($1, $2)",
                &[Value::Int64(2), Value::Text("committed".into())],
            )
            .expect("insert in transaction")
            .for_each(drop);
        transaction.commit().expect("commit");
    }
    assert_eq!(
        rows(
            session
                .execute("SELECT * FROM app.items", &[])
                .expect("select after commit")
        ),
        vec![Row::new(vec![
            Value::Int64(2),
            Value::Text("committed".into())
        ])]
    );
}

#[test]
fn public_api_preserves_errors_and_statement_atomicity() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open persistent engine");
    let mut session = engine.connect().expect("connect");
    session
        .execute(
            "CREATE TABLE accounts (id BIGINT PRIMARY KEY, handle TEXT UNIQUE NOT NULL)",
            &[],
        )
        .expect("create table")
        .for_each(drop);
    session
        .execute("INSERT INTO accounts VALUES (1, 'first')", &[])
        .expect("seed row")
        .for_each(drop);

    let duplicate = session
        .execute(
            "INSERT INTO accounts VALUES (2, 'second'), (1, 'duplicate')",
            &[],
        )
        .expect_err("duplicate key");
    assert_eq!(duplicate.sql_state, "23505");
    assert!(!duplicate.query_id.is_empty());

    let missing_parameter = session
        .execute("SELECT * FROM accounts WHERE id = $1", &[])
        .expect_err("missing parameter");
    assert_eq!(missing_parameter.sql_state, "42P02");

    assert_eq!(
        rows(
            session
                .execute("SELECT * FROM accounts", &[])
                .expect("select after failed insert")
        )
        .len(),
        1
    );
}
