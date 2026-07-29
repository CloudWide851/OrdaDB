use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ordadb_connector_sdk::{
    ConnectorBatchV2, ConnectorCapabilitiesV2, ConnectorCatalogColumnV2,
    ConnectorCatalogObjectKindV2, ConnectorCatalogObjectV2, ConnectorColumnV2,
    ConnectorCredentialV2, ConnectorDriver, ConnectorEndpointV2, ConnectorEventSink,
    ConnectorIsolationLevelV2, ConnectorLogicalTypeV2, ConnectorParameterV2, ConnectorQueryEventV2,
    ConnectorSession, ConnectorTlsModeV2, ConnectorTypeV2, ConnectorValueV2,
    connector_pipe_argument, run_named_pipe_helper,
};
use ordadb_types::{DbError, Result};
use rusqlite::{
    Connection, OpenFlags, Row, params_from_iter,
    types::{Value as SqliteValue, ValueRef},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const PLUGIN_ID: &str = "sqlite";
const EVENT_CHANNEL_CAPACITY: usize = 8;

#[derive(Debug, Default)]
struct SqliteDriver;

struct SqliteSession {
    connection: Arc<Mutex<Connection>>,
    capabilities: ConnectorCapabilitiesV2,
}

#[tokio::main]
async fn main() {
    let result = async {
        let pipe = connector_pipe_argument()?;
        run_named_pipe_helper(&pipe, PLUGIN_ID, env!("CARGO_PKG_VERSION"), SqliteDriver).await
    }
    .await;
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[async_trait]
impl ConnectorDriver for SqliteDriver {
    fn capabilities(&self) -> ConnectorCapabilitiesV2 {
        capabilities()
    }

    async fn connect(
        &self,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        _credential: Option<ConnectorCredentialV2>,
    ) -> Result<Box<dyn ConnectorSession>> {
        if tls_mode != ConnectorTlsModeV2::Disable {
            return Err(DbError::unsupported("TLS for SQLite file connections"));
        }
        let ConnectorEndpointV2::File {
            path,
            read_only,
            create,
            ..
        } = endpoint
        else {
            return Err(invalid("SQLite requires a file endpoint"));
        };
        let mut flags = if read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE
        };
        if create && !read_only {
            flags |= OpenFlags::SQLITE_OPEN_CREATE;
        }
        let connection = tokio::task::spawn_blocking(move || {
            let connection = Connection::open_with_flags(path, flags).map_err(sqlite_error)?;
            connection
                .busy_timeout(Duration::from_secs(30))
                .map_err(sqlite_error)?;
            connection
                .execute_batch("PRAGMA foreign_keys = ON")
                .map_err(sqlite_error)?;
            Ok(connection)
        })
        .await
        .map_err(join_error)??;
        Ok(Box::new(SqliteSession {
            connection: Arc::new(Mutex::new(connection)),
            capabilities: capabilities(),
        }))
    }
}

#[async_trait]
impl ConnectorSession for SqliteSession {
    fn capabilities(&self) -> &ConnectorCapabilitiesV2 {
        &self.capabilities
    }

    async fn catalog(&mut self) -> Result<Vec<ConnectorCatalogObjectV2>> {
        let connection = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || load_catalog(&connection))
            .await
            .map_err(join_error)?
    }

    async fn execute(
        &mut self,
        _request_id: &str,
        sql: &str,
        params: &[ConnectorParameterV2],
        batch_size: u32,
        cancellation: &CancellationToken,
        sink: &mut dyn ConnectorEventSink,
    ) -> Result<()> {
        if batch_size == 0 || batch_size > self.capabilities.maximum_batch_rows {
            return Err(invalid(
                "SQLite connector batch size is outside its capability",
            ));
        }
        let connection = Arc::clone(&self.connection);
        let sql = sql.to_owned();
        let params = params
            .iter()
            .map(sqlite_parameter)
            .collect::<Result<Vec<_>>>()?;
        let cancellation = cancellation.clone();
        let interrupt = lock(&self.connection)?.get_interrupt_handle();
        let interrupt_cancellation = cancellation.clone();
        let interrupter = tokio::spawn(async move {
            interrupt_cancellation.cancelled().await;
            interrupt.interrupt();
        });
        let (events, mut event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let worker_cancellation = cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            execute_sqlite(
                &connection,
                &sql,
                params,
                batch_size,
                &events,
                &worker_cancellation,
            )
        });
        while let Some(event) = event_rx.recv().await {
            if let Err(error) = sink.send(event).await {
                cancellation.cancel();
                interrupter.abort();
                let _ = worker.await;
                return Err(error);
            }
        }
        let result = worker.await.map_err(join_error)?;
        interrupter.abort();
        if cancellation.is_cancelled() {
            return Err(DbError::new("57014", "SQLite query was cancelled"));
        }
        result
    }

    async fn cancel(&mut self, _request_id: &str) -> Result<()> {
        lock(&self.connection)?.get_interrupt_handle().interrupt();
        Ok(())
    }

    async fn begin(&mut self, isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
        if matches!(
            isolation,
            Some(
                ConnectorIsolationLevelV2::RepeatableRead | ConnectorIsolationLevelV2::Serializable
            )
        ) {
            self.execute_batch("BEGIN IMMEDIATE").await
        } else {
            self.execute_batch("BEGIN").await
        }
    }

    async fn commit(&mut self) -> Result<()> {
        self.execute_batch("COMMIT").await
    }

    async fn rollback(&mut self) -> Result<()> {
        self.execute_batch("ROLLBACK").await
    }
}

impl SqliteSession {
    async fn execute_batch(&self, sql: &'static str) -> Result<()> {
        let connection = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || {
            lock(&connection)?.execute_batch(sql).map_err(sqlite_error)
        })
        .await
        .map_err(join_error)?
    }
}

fn capabilities() -> ConnectorCapabilitiesV2 {
    ConnectorCapabilitiesV2 {
        catalog: true,
        cancellation: true,
        transactions: true,
        savepoints: true,
        batch_query: true,
        maximum_batch_rows: 1024,
        tls_modes: vec![ConnectorTlsModeV2::Disable],
    }
}

fn execute_sqlite(
    connection: &Arc<Mutex<Connection>>,
    sql: &str,
    params: Vec<SqliteValue>,
    batch_size: u32,
    events: &mpsc::Sender<ConnectorQueryEventV2>,
    cancellation: &CancellationToken,
) -> Result<()> {
    let connection = lock(connection)?;
    let mut statement = connection.prepare(sql).map_err(sqlite_error)?;
    let column_count = statement.column_count();
    let columns = (0..column_count)
        .map(|index| {
            let name = statement
                .column_name(index)
                .map(str::to_owned)
                .unwrap_or_else(|_| format!("column_{}", index + 1));
            ConnectorColumnV2 {
                name,
                data_type: sqlite_type(""),
                nullable: true,
            }
        })
        .collect::<Vec<_>>();
    send_blocking(events, ConnectorQueryEventV2::Schema { columns })?;
    if column_count == 0 {
        let affected = statement
            .execute(params_from_iter(params.iter()))
            .map_err(sqlite_error)?;
        send_blocking(
            events,
            ConnectorQueryEventV2::Complete {
                command_tag: command_tag(sql),
                affected_rows: Some(u64::try_from(affected).unwrap_or(u64::MAX)),
            },
        )?;
        return Ok(());
    }

    let mut rows = statement
        .query(params_from_iter(params.iter()))
        .map_err(sqlite_error)?;
    let batch_size = usize::try_from(batch_size).unwrap_or(1024);
    let mut batch = Vec::with_capacity(batch_size);
    let mut processed = 0_u64;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        if cancellation.is_cancelled() {
            return Err(DbError::new("57014", "SQLite query was cancelled"));
        }
        batch.push(sqlite_row(row, column_count)?);
        processed = processed.saturating_add(1);
        if batch.len() == batch_size {
            send_blocking(
                events,
                ConnectorQueryEventV2::Batch {
                    batch: ConnectorBatchV2 {
                        rows: std::mem::take(&mut batch),
                    },
                },
            )?;
            send_blocking(
                events,
                ConnectorQueryEventV2::Progress {
                    rows_processed: processed,
                },
            )?;
        }
    }
    if !batch.is_empty() {
        send_blocking(
            events,
            ConnectorQueryEventV2::Batch {
                batch: ConnectorBatchV2 { rows: batch },
            },
        )?;
    }
    send_blocking(
        events,
        ConnectorQueryEventV2::Progress {
            rows_processed: processed,
        },
    )?;
    send_blocking(
        events,
        ConnectorQueryEventV2::Complete {
            command_tag: command_tag(sql),
            affected_rows: Some(processed),
        },
    )
}

fn sqlite_row(row: &Row<'_>, column_count: usize) -> Result<Vec<ConnectorValueV2>> {
    (0..column_count)
        .map(|index| row.get_ref(index).map_err(sqlite_error).map(sqlite_value))
        .collect()
}

fn sqlite_value(value: ValueRef<'_>) -> ConnectorValueV2 {
    match value {
        ValueRef::Null => ConnectorValueV2::Null,
        ValueRef::Integer(value) => ConnectorValueV2::SignedInteger(value),
        ValueRef::Real(value) => ConnectorValueV2::FloatingPoint(value),
        ValueRef::Text(value) => {
            ConnectorValueV2::Text(String::from_utf8_lossy(value).into_owned())
        }
        ValueRef::Blob(value) => ConnectorValueV2::Binary(BASE64.encode(value)),
    }
}

fn sqlite_parameter(parameter: &ConnectorParameterV2) -> Result<SqliteValue> {
    match &parameter.value {
        ConnectorValueV2::Null => Ok(SqliteValue::Null),
        ConnectorValueV2::Boolean(value) => Ok(SqliteValue::Integer(i64::from(*value))),
        ConnectorValueV2::SignedInteger(value) => Ok(SqliteValue::Integer(*value)),
        ConnectorValueV2::UnsignedInteger(value) => i64::try_from(*value)
            .map(SqliteValue::Integer)
            .map_err(|_| DbError::new("22003", "SQLite parameter exceeds int64")),
        ConnectorValueV2::FloatingPoint(value) => Ok(SqliteValue::Real(*value)),
        ConnectorValueV2::Binary(value) => BASE64
            .decode(value)
            .map(SqliteValue::Blob)
            .map_err(|_| invalid("SQLite binary parameter is not valid base64")),
        ConnectorValueV2::Json(value) => Ok(SqliteValue::Text(value.to_string())),
        ConnectorValueV2::Array(value) => serde_json::to_string(value)
            .map(SqliteValue::Text)
            .map_err(|error| {
                invalid("SQLite array parameter is invalid").with_detail(error.to_string())
            }),
        ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Text(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::Uuid(value) => Ok(SqliteValue::Text(value.clone())),
    }
}

fn load_catalog(connection: &Arc<Mutex<Connection>>) -> Result<Vec<ConnectorCatalogObjectV2>> {
    let connection = lock(connection)?;
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, COALESCE(sql, '')
             FROM sqlite_schema
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY type, name",
        )
        .map_err(sqlite_error)?;
    let entries = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    drop(statement);

    let mut objects = Vec::with_capacity(entries.len() + 2);
    objects.push(catalog_object(
        "sqlite:database:main",
        ConnectorCatalogObjectKindV2::Database,
        None,
        "main",
        None,
    ));
    objects.push(catalog_object(
        "sqlite:schema:main",
        ConnectorCatalogObjectKindV2::Schema,
        Some("main"),
        "main",
        Some("sqlite:database:main"),
    ));
    for (kind, name, table_name, sql) in entries {
        let object_kind = match kind.as_str() {
            "table" => ConnectorCatalogObjectKindV2::Table,
            "view" => ConnectorCatalogObjectKindV2::View,
            "index" => ConnectorCatalogObjectKindV2::Index,
            "trigger" => ConnectorCatalogObjectKindV2::Procedure,
            _ => continue,
        };
        let mut object = catalog_object(
            &format!("sqlite:{kind}:{name}"),
            object_kind,
            Some("main"),
            &name,
            Some("sqlite:schema:main"),
        );
        if !sql.is_empty() {
            object.attributes.insert("sql".into(), sql);
        }
        if matches!(
            object_kind,
            ConnectorCatalogObjectKindV2::Table | ConnectorCatalogObjectKindV2::View
        ) {
            object.columns = table_columns(&connection, &name)?;
        } else if !table_name.is_empty() {
            object.attributes.insert("table".into(), table_name);
        }
        objects.push(object);
    }
    Ok(objects)
}

fn table_columns(connection: &Connection, table: &str) -> Result<Vec<ConnectorCatalogColumnV2>> {
    let escaped = table.replace('"', "\"\"");
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info(\"{escaped}\")"))
        .map_err(sqlite_error)?;
    statement
        .query_map([], |row| {
            let declared = row.get::<_, String>(2)?;
            Ok(ConnectorCatalogColumnV2 {
                name: row.get(1)?,
                ordinal: u32::try_from(row.get::<_, i64>(0)? + 1).unwrap_or(u32::MAX),
                data_type: sqlite_type(&declared),
                nullable: row.get::<_, i64>(3)? == 0,
                default_expression: row.get(4)?,
            })
        })
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)
}

fn catalog_object(
    id: &str,
    kind: ConnectorCatalogObjectKindV2,
    schema: Option<&str>,
    name: &str,
    parent_id: Option<&str>,
) -> ConnectorCatalogObjectV2 {
    ConnectorCatalogObjectV2 {
        id: id.into(),
        kind,
        catalog: Some("main".into()),
        schema: schema.map(str::to_owned),
        name: name.into(),
        parent_id: parent_id.map(str::to_owned),
        comment: None,
        columns: Vec::new(),
        attributes: BTreeMap::new(),
    }
}

fn sqlite_type(declared: &str) -> ConnectorTypeV2 {
    let normalized = declared.trim().to_ascii_uppercase();
    let logical_type = if normalized.contains("INT") {
        ConnectorLogicalTypeV2::SignedInteger
    } else if normalized.contains("CHAR")
        || normalized.contains("CLOB")
        || normalized.contains("TEXT")
    {
        ConnectorLogicalTypeV2::Text
    } else if normalized.contains("BLOB") || normalized.is_empty() {
        ConnectorLogicalTypeV2::Binary
    } else if normalized.contains("REAL")
        || normalized.contains("FLOA")
        || normalized.contains("DOUB")
    {
        ConnectorLogicalTypeV2::FloatingPoint
    } else if normalized.contains("BOOL") {
        ConnectorLogicalTypeV2::Boolean
    } else if normalized.contains("DATE") && !normalized.contains("TIME") {
        ConnectorLogicalTypeV2::Date
    } else if normalized.contains("TIME") {
        ConnectorLogicalTypeV2::Timestamp
    } else if normalized.contains("JSON") {
        ConnectorLogicalTypeV2::Json
    } else {
        ConnectorLogicalTypeV2::Decimal
    };
    ConnectorTypeV2 {
        vendor_name: if declared.is_empty() {
            "BLOB".into()
        } else {
            declared.into()
        },
        logical_type,
        element_type: None,
        precision: None,
        scale: None,
        length: None,
    }
}

fn send_blocking(
    events: &mpsc::Sender<ConnectorQueryEventV2>,
    event: ConnectorQueryEventV2,
) -> Result<()> {
    events.blocking_send(event).map_err(|_| {
        DbError::new(
            "08006",
            "SQLite connector event receiver closed before completion",
        )
    })
}

fn command_tag(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .unwrap_or("SQLITE")
        .to_ascii_uppercase()
}

fn lock(connection: &Arc<Mutex<Connection>>) -> Result<MutexGuard<'_, Connection>> {
    connection
        .lock()
        .map_err(|_| DbError::internal("SQLite connection lock was poisoned"))
}

fn sqlite_error(error: rusqlite::Error) -> DbError {
    let sql_state = match error {
        rusqlite::Error::SqliteFailure(ref failure, _)
            if failure.code == rusqlite::ErrorCode::OperationInterrupted =>
        {
            "57014"
        }
        rusqlite::Error::SqliteFailure(ref failure, _)
            if failure.code == rusqlite::ErrorCode::DatabaseBusy
                || failure.code == rusqlite::ErrorCode::DatabaseLocked =>
        {
            "55P03"
        }
        rusqlite::Error::SqliteFailure(..) => "HY000",
        _ => "XX000",
    };
    DbError::new(sql_state, "SQLite connector operation failed").with_detail(error.to_string())
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn join_error(error: tokio::task::JoinError) -> DbError {
    DbError::internal("SQLite connector worker failed").with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<ConnectorQueryEventV2>,
    }

    impl ConnectorEventSink for RecordingSink {
        fn send(
            &mut self,
            event: ConnectorQueryEventV2,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async move {
                self.events.push(event);
                Ok(())
            })
        }
    }

    #[test]
    fn sqlite_affinity_mapping_is_stable() {
        assert_eq!(
            sqlite_type("INTEGER").logical_type,
            ConnectorLogicalTypeV2::SignedInteger
        );
        assert_eq!(
            sqlite_type("VARCHAR(100)").logical_type,
            ConnectorLogicalTypeV2::Text
        );
        assert_eq!(
            sqlite_type("DOUBLE").logical_type,
            ConnectorLogicalTypeV2::FloatingPoint
        );
    }

    #[test]
    fn sqlite_catalog_and_values_use_real_database_metadata() {
        let connection = Arc::new(Mutex::new(
            Connection::open_in_memory().expect("in-memory database"),
        ));
        lock(&connection)
            .expect("connection")
            .execute_batch(
                "CREATE TABLE items (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    payload BLOB
                );
                CREATE INDEX items_name_idx ON items(name);",
            )
            .expect("schema");
        let catalog = load_catalog(&connection).expect("catalog");
        let table = catalog
            .iter()
            .find(|object| {
                object.kind == ConnectorCatalogObjectKindV2::Table && object.name == "items"
            })
            .expect("items table");
        assert_eq!(table.columns.len(), 3);
        assert_eq!(table.columns[1].name, "name");
        assert!(!table.columns[1].nullable);
        assert!(catalog.iter().any(|object| {
            object.kind == ConnectorCatalogObjectKindV2::Index && object.name == "items_name_idx"
        }));
    }

    #[tokio::test]
    async fn sqlite_driver_covers_transactions_streaming_and_cancellation() {
        let mut session = SqliteDriver
            .connect(
                ConnectorEndpointV2::File {
                    path: ":memory:".into(),
                    read_only: false,
                    create: true,
                    options: BTreeMap::new(),
                },
                ConnectorTlsModeV2::Disable,
                None,
            )
            .await
            .expect("connect SQLite");
        session
            .begin(Some(ConnectorIsolationLevelV2::Serializable))
            .await
            .expect("begin");
        session
            .execute(
                "sqlite-create",
                "CREATE TABLE items (
                    id INTEGER PRIMARY KEY,
                    active BOOLEAN,
                    amount DECIMAL,
                    payload BLOB,
                    document JSON
                 )",
                &[],
                64,
                &CancellationToken::new(),
                &mut RecordingSink::default(),
            )
            .await
            .expect("create table");
        session.commit().await.expect("commit");
        let catalog = session.catalog().await.expect("SQLite Catalog");
        assert!(catalog.iter().any(|object| object.name == "items"));

        let mut stream_sink = RecordingSink::default();
        session
            .execute(
                "sqlite-large",
                "WITH RECURSIVE seq(value) AS (
                    SELECT 1
                    UNION ALL
                    SELECT value + 1 FROM seq WHERE value < 2048
                 )
                 SELECT value FROM seq",
                &[],
                128,
                &CancellationToken::new(),
                &mut stream_sink,
            )
            .await
            .expect("large stream");
        assert_eq!(streamed_rows(&stream_sink.events), 2048);

        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            trigger.cancel();
        });
        let mut cancel_sink = RecordingSink::default();
        let error = session
            .execute(
                "sqlite-cancel",
                "WITH RECURSIVE seq(value) AS (
                    SELECT 1
                    UNION ALL
                    SELECT value + 1 FROM seq WHERE value < 100000000
                 )
                 SELECT sum(value) FROM seq",
                &[],
                64,
                &cancellation,
                &mut cancel_sink,
            )
            .await
            .expect_err("cancelled SQLite query");
        assert_eq!(error.sql_state, "57014");
    }

    fn streamed_rows(events: &[ConnectorQueryEventV2]) -> usize {
        events
            .iter()
            .filter_map(|event| match event {
                ConnectorQueryEventV2::Batch { batch } => Some(batch.rows.len()),
                _ => None,
            })
            .sum()
    }
}
