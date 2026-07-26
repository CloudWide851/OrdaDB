use ordadb_engine::{Engine, EngineConfig, SessionOptions};
use ordadb_sql::SqlDialect;
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
fn sessions_execute_the_verified_dialect_subset_without_changing_defaults() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");

    let mut postgres = engine.connect().expect("postgres session");
    assert_eq!(postgres.options(), SessionOptions::default());
    postgres
        .execute(
            "CREATE TABLE postgres_items (id BIGINT PRIMARY KEY, name TEXT)",
            &[],
        )
        .expect("create postgres table")
        .for_each(drop);
    postgres
        .execute(
            "INSERT INTO postgres_items VALUES ($1, $2)",
            &[Value::Int64(1), Value::Text("postgres".into())],
        )
        .expect("insert postgres")
        .for_each(drop);

    let mut mysql = engine
        .connect_with_options(SessionOptions {
            dialect: SqlDialect::MySql,
        })
        .expect("mysql session");
    mysql
        .execute(
            "CREATE TABLE `mysql_items` (`id` BIGINT PRIMARY KEY, `name` TEXT)",
            &[],
        )
        .expect("create mysql table")
        .for_each(drop);
    mysql
        .execute(
            "INSERT INTO `mysql_items` VALUES (?, ?)",
            &[Value::Int64(2), Value::Text("mysql".into())],
        )
        .expect("insert mysql")
        .for_each(drop);
    assert_eq!(
        rows(
            mysql
                .execute(
                    "SELECT `id`, `name` FROM `mysql_items` WHERE `id` = ? LIMIT 1",
                    &[Value::Int64(2)],
                )
                .expect("select mysql"),
        ),
        vec![Row::new(vec![Value::Int64(2), Value::Text("mysql".into())])]
    );

    let mut sqlite = engine
        .connect_with_options(SessionOptions {
            dialect: SqlDialect::Sqlite,
        })
        .expect("sqlite session");
    sqlite
        .execute(
            "CREATE TABLE \"sqlite_items\" (\"id\" INTEGER PRIMARY KEY, \"name\" TEXT)",
            &[],
        )
        .expect("create sqlite table")
        .for_each(drop);
    sqlite
        .execute(
            "INSERT INTO \"sqlite_items\" VALUES (?, ?)",
            &[Value::Int32(3), Value::Text("sqlite".into())],
        )
        .expect("insert sqlite")
        .for_each(drop);

    let mut sql_server = engine
        .connect_with_options(SessionOptions {
            dialect: SqlDialect::SqlServer,
        })
        .expect("sql server session");
    sql_server
        .execute(
            "CREATE TABLE [sqlserver_items] (\
                [id] BIGINT PRIMARY KEY,\
                [name] NVARCHAR(32)\
            )",
            &[],
        )
        .expect("create sql server table")
        .for_each(drop);
    sql_server
        .execute(
            "INSERT INTO [sqlserver_items] VALUES (@p1, @p2)",
            &[Value::Int64(4), Value::Text("sqlserver".into())],
        )
        .expect("insert sql server")
        .for_each(drop);
    assert_eq!(
        rows(
            sql_server
                .execute(
                    "SELECT TOP 1 [id], [name] FROM [sqlserver_items] WHERE [id] = @p1",
                    &[Value::Int64(4)],
                )
                .expect("select sql server"),
        ),
        vec![Row::new(vec![
            Value::Int64(4),
            Value::Text("sqlserver".into())
        ])]
    );
}

#[test]
fn describe_stream_and_explicit_transactions_share_the_session_dialect() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
    let mut session = engine
        .connect_with_options(SessionOptions {
            dialect: SqlDialect::MySql,
        })
        .expect("mysql session");
    session
        .execute(
            "CREATE TABLE `items` (`id` BIGINT PRIMARY KEY, `name` TEXT)",
            &[],
        )
        .expect("create table")
        .for_each(drop);
    assert_eq!(
        session
            .describe("SELECT `id`, `name` FROM `items` WHERE `id` = ?")
            .expect("describe")
            .fields
            .len(),
        2
    );

    {
        let mut transaction = session.begin().expect("begin");
        transaction
            .execute(
                "INSERT INTO `items` VALUES (?, ?)",
                &[Value::Int64(1), Value::Text("committed".into())],
            )
            .expect("transaction insert")
            .for_each(drop);
        transaction.commit().expect("commit");
    }

    let events = session
        .execute_stream(
            "SELECT `id`, `name` FROM `items` WHERE `id` = ?",
            &[Value::Int64(1)],
        )
        .expect("stream")
        .collect::<ordadb_types::Result<Vec<_>>>()
        .expect("stream events");
    assert_eq!(
        rows(events.into_iter()),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("committed".into())
        ])]
    );
}

#[test]
fn unsupported_vendor_semantics_are_explicit_and_dialect_named() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
    let mut session = engine
        .connect_with_options(SessionOptions {
            dialect: SqlDialect::MySql,
        })
        .expect("mysql session");
    let error = session
        .execute("CREATE TABLE unsupported (id BIGINT) ENGINE = InnoDB", &[])
        .expect_err("storage engine clause");
    assert_eq!(error.sql_state, "0A000");
    assert!(error.message.contains("MySQL"), "{error:?}");
}
