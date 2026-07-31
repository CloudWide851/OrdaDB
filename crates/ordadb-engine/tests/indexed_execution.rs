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
fn public_api_executes_join_aggregate_and_explain() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("engine");
    let mut session = engine.connect().expect("session");
    session
        .execute("CREATE TABLE teams (id BIGINT PRIMARY KEY, name TEXT)", &[])
        .expect("teams");
    session
        .execute(
            "CREATE TABLE tasks (id BIGINT PRIMARY KEY, team_id BIGINT, points BIGINT)",
            &[],
        )
        .expect("tasks");
    session
        .execute(
            "INSERT INTO teams VALUES (1, 'Kernel'), (2, 'Console')",
            &[],
        )
        .expect("team rows");
    session
        .execute("INSERT INTO tasks VALUES (1, 1, 3), (2, 1, 5)", &[])
        .expect("task rows");

    let result = rows(
        session
            .execute(
                "SELECT t.name, COUNT(k.id), SUM(k.points) \
                 FROM teams t LEFT JOIN tasks k ON t.id = k.team_id \
                 GROUP BY t.name ORDER BY t.name",
                &[],
            )
            .expect("grouped join"),
    );
    assert_eq!(
        result,
        vec![
            Row::new(vec![
                Value::Text("Console".into()),
                Value::Int64(0),
                Value::Null,
            ]),
            Row::new(vec![
                Value::Text("Kernel".into()),
                Value::Int64(2),
                Value::Int64(8),
            ]),
        ]
    );

    let plan = rows(
        session
            .execute("EXPLAIN SELECT id FROM tasks WHERE id = 1", &[])
            .expect("explain"),
    );
    assert!(plan.iter().any(|row| {
        matches!(
            row.values.as_slice(),
            [Value::Text(line)] if line.contains("Scan")
        )
    }));
}

#[test]
fn failed_unique_index_build_is_atomic_through_public_api() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("engine");
    let mut session = engine.connect().expect("session");
    session
        .execute("CREATE TABLE valueset (id BIGINT, category BIGINT)", &[])
        .expect("table");
    session
        .execute("INSERT INTO valueset VALUES (1, 7), (2, 7)", &[])
        .expect("rows");
    let error = session
        .execute(
            "CREATE UNIQUE INDEX valueset_category_unique ON valueset (category)",
            &[],
        )
        .expect_err("duplicate index");
    assert_eq!(error.sql_state, "23505");

    session
        .execute(
            "CREATE INDEX valueset_category_unique ON valueset (category)",
            &[],
        )
        .expect("name remains available after failed candidate");
    assert_eq!(
        rows(
            session
                .execute(
                    "SELECT id FROM valueset WHERE category = 7 ORDER BY id",
                    &[],
                )
                .expect("query"),
        )
        .len(),
        2
    );
}
