use std::{collections::BTreeMap, path::PathBuf, sync::Arc, thread};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use oracle::{
    Connection, Error as OracleError, ErrorKind as OracleErrorKind, Row, Version,
    sql_type::{OracleType, ToSql},
};
use ordadb_connector_sdk::{
    ConnectorCapabilitiesV3, ConnectorCatalogNodeKindV3, ConnectorCatalogNodeV3,
    ConnectorCatalogPageV3, ConnectorColumnV2, ConnectorCommandInputModeV3,
    ConnectorCommandLanguageV3, ConnectorCommandV3, ConnectorCredentialV2, ConnectorDriverV3,
    ConnectorEndpointV2, ConnectorEventSinkV3, ConnectorIsolationLevelV2, ConnectorKindV3,
    ConnectorLogicalTypeV2, ConnectorParameterV2, ConnectorResultBatchV3, ConnectorResultEventV3,
    ConnectorSessionV3, ConnectorTlsModeV2, ConnectorTypeV2, ConnectorValueV2,
    connector_pipe_argument, run_named_pipe_helper_v3,
};
use ordadb_types::{DbError, Result};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

const PLUGIN_ID: &str = "oracle";
const LANGUAGE_ID: &str = "oracle-sql";
const MINIMUM_ORACLE_CLIENT_MAJOR: i32 = 19;
const MAX_BATCH_ROWS: u32 = 512;
const MAX_CATALOG_PAGE_SIZE: u32 = 512;
const WORKER_CHANNEL_CAPACITY: usize = 8;
const EVENT_CHANNEL_CAPACITY: usize = 2;
const MAX_PARAMETER_BYTES: usize = 1024 * 1024;
const MAX_PARAMETER_TOTAL_BYTES: usize = 4 * 1024 * 1024;
const MAX_CELL_BYTES: usize = 1024 * 1024;
const MAX_BATCH_BYTES: usize = 6 * 1024 * 1024;
const MAX_IDENTIFIER_BYTES: usize = 512;

#[derive(Debug, Default)]
struct OracleDriver;

struct OracleSession {
    commands: mpsc::Sender<WorkerCommand>,
    breaker: Arc<Connection>,
    capabilities: ConnectorCapabilitiesV3,
}

struct OracleConnectOptions {
    username: String,
    password: Zeroizing<String>,
    connect_string: String,
}

struct WorkerReady {
    breaker: Arc<Connection>,
}

enum WorkerCommand {
    Catalog {
        parent_id: Option<String>,
        page_size: u32,
        cursor: Option<String>,
        response: oneshot::Sender<Result<ConnectorCatalogPageV3>>,
    },
    Execute {
        command: OracleCommand,
        batch_size: u32,
        events: mpsc::Sender<Result<ConnectorResultEventV3>>,
    },
    Begin {
        isolation: Option<ConnectorIsolationLevelV2>,
        response: oneshot::Sender<Result<()>>,
    },
    Commit {
        response: oneshot::Sender<Result<()>>,
    },
    Rollback {
        response: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

struct OracleCommand {
    sql: String,
    parameters: Vec<OracleBind>,
}

enum OracleBind {
    Null(Option<String>),
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    Floating(f64),
    Text(String),
    Binary(Vec<u8>),
}

impl OracleBind {
    fn as_to_sql(&self) -> &dyn ToSql {
        match self {
            Self::Null(value) => value,
            Self::Boolean(value) => value,
            Self::Signed(value) => value,
            Self::Unsigned(value) => value,
            Self::Floating(value) => value,
            Self::Text(value) => value,
            Self::Binary(value) => value,
        }
    }
}

#[derive(Debug)]
enum CatalogParent {
    Root,
    Schema(String),
    Object { schema: String, name: String },
}

#[tokio::main]
async fn main() {
    let result = async {
        let pipe = connector_pipe_argument()?;
        run_named_pipe_helper_v3(&pipe, PLUGIN_ID, env!("CARGO_PKG_VERSION"), OracleDriver).await
    }
    .await;
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[async_trait]
impl ConnectorDriverV3 for OracleDriver {
    fn capabilities(&self) -> ConnectorCapabilitiesV3 {
        capabilities()
    }

    async fn connect(
        &self,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<Box<dyn ConnectorSessionV3>> {
        let helper_directory = std::env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(PathBuf::from));
        ordadb_windows::discover_amd64_oracle_client(helper_directory.as_deref())?;
        let options = connection_options(endpoint, tls_mode, credential)?;
        let (commands, command_rx) = mpsc::channel(WORKER_CHANNEL_CAPACITY);
        let (ready_tx, ready_rx) = oneshot::channel();
        thread::Builder::new()
            .name("ordadb-oracle-oci".into())
            .spawn(move || run_worker(options, command_rx, ready_tx))
            .map_err(|error| {
                DbError::new("58000", "Oracle connector worker could not start")
                    .with_detail(error.to_string())
            })?;
        let ready = ready_rx.await.map_err(|_| {
            DbError::new("08006", "Oracle connector worker exited during connection")
        })??;
        Ok(Box::new(OracleSession {
            commands,
            breaker: ready.breaker,
            capabilities: capabilities(),
        }))
    }
}

#[async_trait]
impl ConnectorSessionV3 for OracleSession {
    fn capabilities(&self) -> &ConnectorCapabilitiesV3 {
        &self.capabilities
    }

    async fn catalog_page(
        &mut self,
        parent_id: Option<&str>,
        page_size: u32,
        cursor: Option<&str>,
    ) -> Result<ConnectorCatalogPageV3> {
        if page_size == 0 || page_size > self.capabilities.maximum_catalog_page_size {
            return Err(invalid(
                "Oracle Catalog page size is outside its capability",
            ));
        }
        let (response, result) = oneshot::channel();
        self.commands
            .send(WorkerCommand::Catalog {
                parent_id: parent_id.map(str::to_owned),
                page_size,
                cursor: cursor.map(str::to_owned),
                response,
            })
            .await
            .map_err(|_| worker_closed())?;
        result.await.map_err(|_| worker_closed())?
    }

    async fn execute(
        &mut self,
        _request_id: &str,
        command: &ConnectorCommandV3,
        batch_size: u32,
        cancellation: &CancellationToken,
        sink: &mut dyn ConnectorEventSinkV3,
    ) -> Result<()> {
        if batch_size == 0 || batch_size > self.capabilities.maximum_batch_rows {
            return Err(invalid("Oracle batch size is outside its capability"));
        }
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let command = oracle_command(command)?;
        let (events, mut event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        self.commands
            .send(WorkerCommand::Execute {
                command,
                batch_size,
                events,
            })
            .await
            .map_err(|_| worker_closed())?;

        let mut break_requested = false;
        let mut break_tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled(), if !break_requested => {
                    break_requested = true;
                    let breaker = Arc::clone(&self.breaker);
                    break_tasks.spawn_blocking(move || {
                        breaker.break_execution().map_err(map_oracle_error)
                    });
                }
                result = break_tasks.join_next(), if !break_tasks.is_empty() => {
                    finish_break_result(result)?;
                }
                event = event_rx.recv() => {
                    let Some(event) = event else {
                        return Err(worker_closed());
                    };
                    match event {
                        Ok(event) => {
                            let terminal = matches!(event, ConnectorResultEventV3::Complete { .. });
                            if terminal && break_requested {
                                finish_break_tasks(&mut break_tasks).await?;
                                return Err(cancelled());
                            }
                            if let Err(error) = sink.send(event).await {
                                finish_break_tasks(&mut break_tasks).await?;
                                return Err(error);
                            }
                            if terminal {
                                return Ok(());
                            }
                        }
                        Err(_) if break_requested || cancellation.is_cancelled() => {
                            finish_break_tasks(&mut break_tasks).await?;
                            return Err(cancelled());
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    async fn cancel(&mut self, _request_id: &str) -> Result<()> {
        let breaker = Arc::clone(&self.breaker);
        tokio::task::spawn_blocking(move || breaker.break_execution())
            .await
            .map_err(|error| {
                DbError::internal("Oracle cancellation worker failed")
                    .with_detail(error.to_string())
            })?
            .map_err(map_oracle_error)
    }

    async fn begin(&mut self, isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(WorkerCommand::Begin {
                isolation,
                response,
            })
            .await
            .map_err(|_| worker_closed())?;
        result.await.map_err(|_| worker_closed())?
    }

    async fn commit(&mut self) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(WorkerCommand::Commit { response })
            .await
            .map_err(|_| worker_closed())?;
        result.await.map_err(|_| worker_closed())?
    }

    async fn rollback(&mut self) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(WorkerCommand::Rollback { response })
            .await
            .map_err(|_| worker_closed())?;
        result.await.map_err(|_| worker_closed())?
    }
}

async fn finish_break_tasks(tasks: &mut tokio::task::JoinSet<Result<()>>) -> Result<()> {
    while let Some(result) = tasks.join_next().await {
        finish_break_result(Some(result))?;
    }
    Ok(())
}

fn finish_break_result(
    result: Option<std::result::Result<Result<()>, tokio::task::JoinError>>,
) -> Result<()> {
    let Some(result) = result else {
        return Ok(());
    };
    result.map_err(|error| {
        DbError::internal("Oracle cancellation worker failed").with_detail(error.to_string())
    })??;
    Ok(())
}

impl Drop for OracleSession {
    fn drop(&mut self) {
        let _ = self.commands.try_send(WorkerCommand::Shutdown);
    }
}

fn run_worker(
    options: OracleConnectOptions,
    mut commands: mpsc::Receiver<WorkerCommand>,
    ready: oneshot::Sender<Result<WorkerReady>>,
) {
    let connection = match open_connection(&options) {
        Ok(connection) => Arc::new(connection),
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    if ready
        .send(Ok(WorkerReady {
            breaker: Arc::clone(&connection),
        }))
        .is_err()
    {
        let _ = connection.close();
        return;
    }

    while let Some(command) = commands.blocking_recv() {
        match command {
            WorkerCommand::Catalog {
                parent_id,
                page_size,
                cursor,
                response,
            } => {
                let result = catalog_page(
                    &connection,
                    parent_id.as_deref(),
                    page_size,
                    cursor.as_deref(),
                );
                let _ = response.send(result);
            }
            WorkerCommand::Execute {
                command,
                batch_size,
                events,
            } => execute_command(&connection, command, batch_size, &events),
            WorkerCommand::Begin {
                isolation,
                response,
            } => {
                let _ = response.send(begin_transaction(&connection, isolation));
            }
            WorkerCommand::Commit { response } => {
                let _ = response.send(connection.commit().map_err(map_oracle_error));
            }
            WorkerCommand::Rollback { response } => {
                let _ = response.send(connection.rollback().map_err(map_oracle_error));
            }
            WorkerCommand::Shutdown => break,
        }
    }
    let _ = connection.close();
}

fn open_connection(options: &OracleConnectOptions) -> Result<Connection> {
    let client_version = Version::client().map_err(map_oracle_error)?;
    if client_version.major() < MINIMUM_ORACLE_CLIENT_MAJOR {
        return Err(DbError::new(
            "0A000",
            "Oracle Instant Client version is not supported",
        )
        .with_detail(format!(
            "detected client major version {}; version {MINIMUM_ORACLE_CLIENT_MAJOR} or newer is required",
            client_version.major()
        ))
        .with_hint("Install a current Windows x64 Oracle Instant Client release."));
    }
    let connection = Connection::connect(
        &options.username,
        options.password.as_str(),
        &options.connect_string,
    )
    .map_err(map_oracle_error)?;
    connection.server_version().map_err(map_oracle_error)?;
    Ok(connection)
}

fn connection_options(
    endpoint: ConnectorEndpointV2,
    tls_mode: ConnectorTlsModeV2,
    credential: Option<ConnectorCredentialV2>,
) -> Result<OracleConnectOptions> {
    let ConnectorEndpointV2::Network {
        host,
        port,
        database,
        instance,
        options,
    } = endpoint
    else {
        return Err(invalid("Oracle requires a network endpoint"));
    };
    if instance.is_some() || !options.is_empty() {
        return Err(DbError::unsupported("Oracle endpoint instance or options"));
    }
    validate_descriptor_component(&host, "host")?;
    if port == 0 {
        return Err(invalid("Oracle port is required"));
    }
    let service = database.ok_or_else(|| invalid("Oracle service name is required"))?;
    validate_descriptor_component(&service, "service name")?;
    let credential = credential
        .ok_or_else(|| DbError::new("28P01", "Oracle username and password are required"))?;
    let username = credential
        .username
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| DbError::new("28P01", "Oracle username is required"))?;
    if username.len() > MAX_IDENTIFIER_BYTES || username.chars().any(char::is_control) {
        return Err(invalid("Oracle username is invalid"));
    }
    if credential.secret.is_empty() || credential.secret.len() > MAX_PARAMETER_BYTES {
        return Err(DbError::new(
            "28P01",
            "Oracle password is required or too large",
        ));
    }
    let connect_string = match tls_mode {
        ConnectorTlsModeV2::Disable => format!("{host}:{port}/{service}"),
        ConnectorTlsModeV2::Require => format!(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST={host})(PORT={port}))(CONNECT_DATA=(SERVICE_NAME={service})))"
        ),
        ConnectorTlsModeV2::Prefer
        | ConnectorTlsModeV2::VerifyCa
        | ConnectorTlsModeV2::VerifyFull => {
            return Err(DbError::unsupported("Oracle TLS mode").with_hint(
                "Select disable or require; certificate policy remains controlled by Oracle Net configuration.",
            ));
        }
    };
    Ok(OracleConnectOptions {
        username,
        password: credential.secret,
        connect_string,
    })
}

fn validate_descriptor_component(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > MAX_IDENTIFIER_BYTES
        || value.chars().any(|character| {
            character.is_control() || matches!(character, '(' | ')' | '=' | '\'' | '"')
        })
    {
        return Err(invalid(format!("Oracle {name} is invalid")));
    }
    Ok(())
}

fn oracle_command(command: &ConnectorCommandV3) -> Result<OracleCommand> {
    let ConnectorCommandV3::Text {
        language_id,
        text,
        params,
    } = command
    else {
        return Err(DbError::unsupported("Oracle non-SQL command input"));
    };
    if language_id != LANGUAGE_ID {
        return Err(DbError::unsupported(format!(
            "Oracle command language {language_id}"
        )));
    }
    if text.trim().is_empty() {
        return Err(invalid("Oracle SQL is empty"));
    }
    let mut parameter_bytes = 0_usize;
    let parameters = params
        .iter()
        .map(|parameter| oracle_bind(parameter, &mut parameter_bytes))
        .collect::<Result<Vec<_>>>()?;
    Ok(OracleCommand {
        sql: text.clone(),
        parameters,
    })
}

fn oracle_bind(parameter: &ConnectorParameterV2, total_bytes: &mut usize) -> Result<OracleBind> {
    let (bind, bytes) = match &parameter.value {
        ConnectorValueV2::Null => (OracleBind::Null(None), 0),
        ConnectorValueV2::Boolean(value) => (OracleBind::Boolean(*value), 1),
        ConnectorValueV2::SignedInteger(value) => (OracleBind::Signed(*value), 8),
        ConnectorValueV2::UnsignedInteger(value) => (OracleBind::Unsigned(*value), 8),
        ConnectorValueV2::FloatingPoint(value) => (OracleBind::Floating(*value), 8),
        ConnectorValueV2::Text(value)
        | ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::Uuid(value) => (OracleBind::Text(value.clone()), value.len()),
        ConnectorValueV2::Binary(value) => {
            let bytes = BASE64
                .decode(value)
                .map_err(|_| invalid("Oracle binary parameter is not valid base64"))?;
            let length = bytes.len();
            (OracleBind::Binary(bytes), length)
        }
        ConnectorValueV2::Json(value) => {
            let value = serde_json::to_string(value)
                .map_err(|error| invalid(format!("Oracle JSON parameter is invalid: {error}")))?;
            let length = value.len();
            (OracleBind::Text(value), length)
        }
        ConnectorValueV2::Array(_) => {
            return Err(DbError::unsupported("Oracle array parameters"));
        }
    };
    if bytes > MAX_PARAMETER_BYTES {
        return Err(limit_error("Oracle parameter exceeds its byte limit"));
    }
    *total_bytes = total_bytes.saturating_add(bytes);
    if *total_bytes > MAX_PARAMETER_TOTAL_BYTES {
        return Err(limit_error(
            "Oracle parameters exceed their aggregate byte limit",
        ));
    }
    Ok(bind)
}

fn execute_command(
    connection: &Connection,
    command: OracleCommand,
    batch_size: u32,
    events: &mpsc::Sender<Result<ConnectorResultEventV3>>,
) {
    let result = execute_command_inner(connection, command, batch_size, events);
    if let Err(error) = result {
        let _ = events.blocking_send(Err(error));
    }
}

fn execute_command_inner(
    connection: &Connection,
    command: OracleCommand,
    batch_size: u32,
    events: &mpsc::Sender<Result<ConnectorResultEventV3>>,
) -> Result<()> {
    let parameters = command
        .parameters
        .iter()
        .map(OracleBind::as_to_sql)
        .collect::<Vec<_>>();
    let mut statement = connection
        .statement(&command.sql)
        .fetch_array_size(batch_size)
        .prefetch_rows(batch_size.min(64))
        .build()
        .map_err(map_oracle_error)?;
    let statement_type = statement.statement_type();
    if statement.is_query() {
        let mut rows = statement.query(&parameters).map_err(map_oracle_error)?;
        let column_info = rows.column_info().to_vec();
        send_worker_event(
            events,
            ConnectorResultEventV3::Schema {
                columns: column_info
                    .iter()
                    .map(|column| ConnectorColumnV2 {
                        name: column.name().to_owned(),
                        data_type: connector_type(column.oracle_type()),
                        nullable: column.nullable(),
                    })
                    .collect(),
            },
        )?;
        let mut batch = Vec::with_capacity(usize::try_from(batch_size).unwrap_or(1));
        let mut batch_bytes = 0_usize;
        let mut processed = 0_u64;
        for row in &mut rows {
            let row = row.map_err(map_oracle_error)?;
            let converted = convert_row(&row, &column_info)?;
            let row_bytes = converted.iter().map(value_size).sum::<usize>();
            if row_bytes > MAX_BATCH_BYTES {
                return Err(limit_error(
                    "Oracle result row exceeds the batch byte limit",
                ));
            }
            if !batch.is_empty()
                && (batch.len() == usize::try_from(batch_size).unwrap_or(1)
                    || batch_bytes.saturating_add(row_bytes) > MAX_BATCH_BYTES)
            {
                send_row_batch(events, &mut batch, processed)?;
                batch_bytes = 0;
            }
            batch_bytes = batch_bytes.saturating_add(row_bytes);
            batch.push(converted);
            processed = processed.saturating_add(1);
        }
        if !batch.is_empty() {
            send_row_batch(events, &mut batch, processed)?;
        }
        send_worker_event(
            events,
            ConnectorResultEventV3::Complete {
                command_tag: statement_type.to_string().to_ascii_uppercase(),
                affected_items: Some(processed),
            },
        )
    } else {
        statement.execute(&parameters).map_err(map_oracle_error)?;
        let affected = statement.row_count().map_err(map_oracle_error)?;
        send_worker_event(
            events,
            ConnectorResultEventV3::Complete {
                command_tag: statement_type.to_string().to_ascii_uppercase(),
                affected_items: Some(affected),
            },
        )
    }
}

fn send_row_batch(
    events: &mpsc::Sender<Result<ConnectorResultEventV3>>,
    rows: &mut Vec<Vec<ConnectorValueV2>>,
    processed: u64,
) -> Result<()> {
    send_worker_event(
        events,
        ConnectorResultEventV3::Batch {
            batch: ConnectorResultBatchV3::Rows {
                rows: std::mem::take(rows),
            },
        },
    )?;
    send_worker_event(
        events,
        ConnectorResultEventV3::Progress {
            items_processed: processed,
        },
    )
}

fn send_worker_event(
    events: &mpsc::Sender<Result<ConnectorResultEventV3>>,
    event: ConnectorResultEventV3,
) -> Result<()> {
    events.blocking_send(Ok(event)).map_err(|_| {
        DbError::new(
            "08006",
            "Oracle result consumer closed before query completion",
        )
    })
}

fn convert_row(row: &Row, columns: &[oracle::ColumnInfo]) -> Result<Vec<ConnectorValueV2>> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| convert_value(row, index, column.oracle_type()))
        .collect()
}

fn convert_value(row: &Row, index: usize, data_type: &OracleType) -> Result<ConnectorValueV2> {
    if row
        .sql_values()
        .get(index)
        .ok_or_else(|| protocol_error("Oracle row width does not match its schema"))?
        .is_null()
        .map_err(map_oracle_error)?
    {
        return Ok(ConnectorValueV2::Null);
    }
    match data_type {
        OracleType::Boolean => row
            .get::<_, bool>(index)
            .map(ConnectorValueV2::Boolean)
            .map_err(map_oracle_error),
        OracleType::Int64 => row
            .get::<_, i64>(index)
            .map(ConnectorValueV2::SignedInteger)
            .map_err(map_oracle_error),
        OracleType::UInt64 => row
            .get::<_, u64>(index)
            .map(ConnectorValueV2::UnsignedInteger)
            .map_err(map_oracle_error),
        OracleType::BinaryFloat | OracleType::BinaryDouble => row
            .get::<_, f64>(index)
            .map(ConnectorValueV2::FloatingPoint)
            .map_err(map_oracle_error),
        OracleType::Raw(_) | OracleType::LongRaw | OracleType::BLOB => {
            let value = row.get::<_, Vec<u8>>(index).map_err(map_oracle_error)?;
            ensure_cell_size(value.len())?;
            Ok(ConnectorValueV2::Binary(BASE64.encode(value)))
        }
        OracleType::Number(_, _) | OracleType::Float(_) => {
            mapped_string(row, index, ConnectorValueV2::Decimal)
        }
        OracleType::Date => mapped_string(row, index, ConnectorValueV2::Timestamp),
        OracleType::Timestamp(_) => mapped_string(row, index, ConnectorValueV2::Timestamp),
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
            mapped_string(row, index, ConnectorValueV2::TimestampWithTimeZone)
        }
        OracleType::IntervalDS(_, _) | OracleType::IntervalYM(_) => {
            mapped_string(row, index, ConnectorValueV2::Interval)
        }
        OracleType::Json => {
            let value = bounded_string(row, index)?;
            let value = serde_json::from_str(&value)
                .map_err(|_| protocol_error("Oracle JSON result is not valid JSON"))?;
            Ok(ConnectorValueV2::Json(value))
        }
        OracleType::RefCursor => Err(DbError::unsupported("Oracle REF CURSOR result columns")),
        _ => mapped_string(row, index, ConnectorValueV2::Text),
    }
}

fn mapped_string(
    row: &Row,
    index: usize,
    constructor: impl FnOnce(String) -> ConnectorValueV2,
) -> Result<ConnectorValueV2> {
    bounded_string(row, index).map(constructor)
}

fn bounded_string(row: &Row, index: usize) -> Result<String> {
    let value = row.get::<_, String>(index).map_err(map_oracle_error)?;
    ensure_cell_size(value.len())?;
    Ok(value)
}

fn ensure_cell_size(bytes: usize) -> Result<()> {
    if bytes > MAX_CELL_BYTES {
        return Err(limit_error("Oracle result cell exceeds its byte limit"));
    }
    Ok(())
}

fn value_size(value: &ConnectorValueV2) -> usize {
    match value {
        ConnectorValueV2::Null => 1,
        ConnectorValueV2::Boolean(_) => 1,
        ConnectorValueV2::SignedInteger(_)
        | ConnectorValueV2::UnsignedInteger(_)
        | ConnectorValueV2::FloatingPoint(_) => 8,
        ConnectorValueV2::Text(value)
        | ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Binary(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::Uuid(value) => value.len(),
        ConnectorValueV2::Json(value) => value.to_string().len(),
        ConnectorValueV2::Array(values) => values.iter().map(value_size).sum(),
    }
}

fn connector_type(data_type: &OracleType) -> ConnectorTypeV2 {
    let (logical_type, precision, scale, length) = match data_type {
        OracleType::Varchar2(length)
        | OracleType::NVarchar2(length)
        | OracleType::Char(length)
        | OracleType::NChar(length) => (
            ConnectorLogicalTypeV2::Text,
            None,
            None,
            Some(u64::from(*length)),
        ),
        OracleType::Raw(length) => (
            ConnectorLogicalTypeV2::Binary,
            None,
            None,
            Some(u64::from(*length)),
        ),
        OracleType::BinaryFloat | OracleType::BinaryDouble => {
            (ConnectorLogicalTypeV2::FloatingPoint, None, None, None)
        }
        OracleType::Number(precision, scale) => (
            ConnectorLogicalTypeV2::Decimal,
            Some(u32::from(*precision)),
            u32::try_from(*scale).ok(),
            None,
        ),
        OracleType::Float(precision) => (
            ConnectorLogicalTypeV2::Decimal,
            Some(u32::from(*precision)),
            None,
            None,
        ),
        OracleType::Date | OracleType::Timestamp(_) => {
            (ConnectorLogicalTypeV2::Timestamp, None, None, None)
        }
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => (
            ConnectorLogicalTypeV2::TimestampWithTimeZone,
            None,
            None,
            None,
        ),
        OracleType::IntervalDS(_, _) | OracleType::IntervalYM(_) => {
            (ConnectorLogicalTypeV2::Interval, None, None, None)
        }
        OracleType::BLOB | OracleType::BFILE | OracleType::LongRaw => {
            (ConnectorLogicalTypeV2::Binary, None, None, None)
        }
        OracleType::Json => (ConnectorLogicalTypeV2::Json, None, None, None),
        OracleType::Boolean => (ConnectorLogicalTypeV2::Boolean, None, None, None),
        OracleType::Int64 => (ConnectorLogicalTypeV2::SignedInteger, None, None, None),
        OracleType::UInt64 => (ConnectorLogicalTypeV2::UnsignedInteger, None, None, None),
        OracleType::Rowid
        | OracleType::CLOB
        | OracleType::NCLOB
        | OracleType::Long
        | OracleType::Xml => (ConnectorLogicalTypeV2::Text, None, None, None),
        OracleType::Object(_) | OracleType::RefCursor => {
            (ConnectorLogicalTypeV2::Other, None, None, None)
        }
    };
    ConnectorTypeV2 {
        vendor_name: data_type.to_string(),
        logical_type,
        element_type: None,
        precision,
        scale,
        length,
    }
}
