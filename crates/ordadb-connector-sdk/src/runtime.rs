use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    sync::Arc,
};

use ordadb_types::{DbError, Result};
use tokio::{
    io::{ReadHalf, WriteHalf, split},
    net::windows::named_pipe::{ClientOptions, NamedPipeClient},
    sync::{Mutex, mpsc},
};
use tokio_util::sync::CancellationToken;

use crate::driver::ConnectorEventFuture;
use crate::{
    CONNECTOR_PROTOCOL_V2, ConnectorDriver, ConnectorErrorV2, ConnectorEventSink,
    ConnectorIsolationLevelV2, ConnectorParameterV2, ConnectorQueryEventV2, ConnectorRequestV2,
    ConnectorResponseV2, ConnectorSession, ConnectorTransactionStateV2, ProtocolReadyV2,
    read_connector_frame, validate_capabilities, validate_endpoint, write_connector_frame,
};

const RESPONSE_CHANNEL_CAPACITY: usize = 64;
const SESSION_CHANNEL_CAPACITY: usize = 16;

type ActiveRequests = Arc<Mutex<BTreeMap<String, CancellationToken>>>;

pub fn connector_pipe_argument() -> Result<OsString> {
    let mut arguments = std::env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == OsStr::new("--ordadb-pipe") {
            return arguments
                .next()
                .ok_or_else(|| invalid("--ordadb-pipe requires a pipe name"));
        }
    }
    Err(invalid("connector helper requires --ordadb-pipe"))
}

enum SessionCommand {
    Catalog {
        request_id: String,
    },
    Execute {
        request_id: String,
        sql: String,
        params: Vec<ConnectorParameterV2>,
        batch_size: u32,
        cancellation: CancellationToken,
    },
    Begin {
        request_id: String,
        isolation: Option<ConnectorIsolationLevelV2>,
    },
    Commit {
        request_id: String,
    },
    Rollback {
        request_id: String,
    },
    Shutdown,
}

struct ChannelEventSink {
    request_id: String,
    responses: mpsc::Sender<ConnectorResponseV2>,
}

impl ConnectorEventSink for ChannelEventSink {
    fn send(&mut self, event: ConnectorQueryEventV2) -> ConnectorEventFuture<'_> {
        Box::pin(async move {
            self.responses
                .send(ConnectorResponseV2::QueryEvent {
                    request_id: self.request_id.clone(),
                    event,
                })
                .await
                .map_err(|_| connection_closed())
        })
    }
}

pub async fn run_named_pipe_helper<D>(
    pipe_name: &OsStr,
    plugin_id: &str,
    plugin_version: &str,
    driver: D,
) -> Result<()>
where
    D: ConnectorDriver,
{
    validate_helper_identity(plugin_id, plugin_version)?;
    let pipe_name = pipe_name
        .to_str()
        .ok_or_else(|| invalid("connector pipe name is not valid UTF-8"))?;
    if !pipe_name.starts_with(r"\\.\pipe\ordadb-connector-") {
        return Err(invalid(
            "connector pipe name is outside the OrdaDB namespace",
        ));
    }
    let mut pipe = ClientOptions::new()
        .open(pipe_name)
        .map_err(|error| io_error("failed to open connector named pipe", error))?;
    let capabilities = driver.capabilities();
    validate_capabilities(&capabilities)?;
    negotiate(&mut pipe, plugin_id, plugin_version, capabilities.clone()).await?;

    let (reader, writer) = split(pipe);
    let (responses, response_rx) = mpsc::channel(RESPONSE_CHANNEL_CAPACITY);
    let writer_task = tokio::spawn(write_responses(writer, response_rx));
    let result = run_requests(reader, Arc::new(driver), responses.clone()).await;
    drop(responses);
    let writer_result = writer_task.await.map_err(|error| {
        DbError::internal("connector response writer task failed").with_detail(error.to_string())
    })?;
    result.and(writer_result)
}

async fn negotiate(
    pipe: &mut NamedPipeClient,
    plugin_id: &str,
    plugin_version: &str,
    capabilities: crate::ConnectorCapabilitiesV2,
) -> Result<()> {
    let request: ConnectorRequestV2 = read_connector_frame(pipe).await?;
    let ConnectorRequestV2::Hello { hello } = request else {
        return Err(protocol_error(
            "connector host did not begin with a Hello request",
        ));
    };
    if hello.minimum_api_version > CONNECTOR_PROTOCOL_V2
        || hello.maximum_api_version < CONNECTOR_PROTOCOL_V2
    {
        return Err(DbError::unsupported(format!(
            "connector host protocol range {}-{}",
            hello.minimum_api_version, hello.maximum_api_version
        )));
    }
    if hello.plugin_id != plugin_id || hello.plugin_version != plugin_version {
        return Err(protocol_error(
            "connector host identity does not match the helper",
        ));
    }
    write_connector_frame(
        pipe,
        &ConnectorResponseV2::Ready {
            ready: ProtocolReadyV2 {
                api_version: CONNECTOR_PROTOCOL_V2,
                plugin_id: plugin_id.into(),
                plugin_version: plugin_version.into(),
                capabilities,
            },
        },
    )
    .await
}

async fn run_requests<D>(
    mut reader: ReadHalf<NamedPipeClient>,
    driver: Arc<D>,
    responses: mpsc::Sender<ConnectorResponseV2>,
) -> Result<()>
where
    D: ConnectorDriver,
{
    let mut sessions = BTreeMap::<String, mpsc::Sender<SessionCommand>>::new();
    let active = Arc::new(Mutex::new(BTreeMap::new()));
    loop {
        let request: ConnectorRequestV2 = read_connector_frame(&mut reader).await?;
        match request {
            ConnectorRequestV2::Hello { .. } => {
                send_error(
                    &responses,
                    None,
                    protocol_error("connector Hello was repeated"),
                )
                .await?;
            }
            ConnectorRequestV2::Connect {
                connection_id,
                endpoint,
                tls_mode,
                credential,
            } => {
                validate_id(&connection_id, "connection ID")?;
                validate_endpoint(&endpoint)?;
                if sessions.contains_key(&connection_id) {
                    send_error(
                        &responses,
                        None,
                        DbError::new("42P04", "connector connection already exists"),
                    )
                    .await?;
                    continue;
                }
                match driver.connect(endpoint, tls_mode, credential).await {
                    Ok(session) => {
                        let capabilities = session.capabilities().clone();
                        validate_capabilities(&capabilities)?;
                        let (commands, command_rx) = mpsc::channel(SESSION_CHANNEL_CAPACITY);
                        tokio::spawn(run_session(
                            session,
                            command_rx,
                            responses.clone(),
                            Arc::clone(&active),
                        ));
                        sessions.insert(connection_id.clone(), commands);
                        responses
                            .send(ConnectorResponseV2::Connected {
                                connection_id,
                                capabilities,
                            })
                            .await
                            .map_err(|_| connection_closed())?;
                    }
                    Err(error) => send_error(&responses, None, error).await?,
                }
            }
            ConnectorRequestV2::Disconnect { connection_id } => {
                let Some(session) = sessions.remove(&connection_id) else {
                    send_error(
                        &responses,
                        None,
                        DbError::new("08003", "connector connection does not exist"),
                    )
                    .await?;
                    continue;
                };
                let _ = session.send(SessionCommand::Shutdown).await;
                responses
                    .send(ConnectorResponseV2::Disconnected { connection_id })
                    .await
                    .map_err(|_| connection_closed())?;
            }
            ConnectorRequestV2::Catalog {
                request_id,
                connection_id,
            } => {
                send_session_command(
                    &sessions,
                    &connection_id,
                    &responses,
                    Some(request_id.clone()),
                    SessionCommand::Catalog { request_id },
                )
                .await?;
            }
            ConnectorRequestV2::Execute {
                request_id,
                connection_id,
                sql,
                params,
                batch_size,
            } => {
                validate_id(&request_id, "request ID")?;
                if sql.trim().is_empty() || sql.len() > crate::MAX_CONNECTOR_TEXT_BYTES {
                    send_error(
                        &responses,
                        Some(request_id),
                        invalid("connector SQL text is empty or exceeds 1 MiB"),
                    )
                    .await?;
                    continue;
                }
                let cancellation = CancellationToken::new();
                {
                    let mut active = active.lock().await;
                    if active.contains_key(&request_id) {
                        send_error(
                            &responses,
                            Some(request_id),
                            DbError::new("42P04", "connector request already exists"),
                        )
                        .await?;
                        continue;
                    }
                    active.insert(request_id.clone(), cancellation.clone());
                }
                if let Err(error) = send_session_command(
                    &sessions,
                    &connection_id,
                    &responses,
                    Some(request_id.clone()),
                    SessionCommand::Execute {
                        request_id: request_id.clone(),
                        sql,
                        params,
                        batch_size,
                        cancellation,
                    },
                )
                .await
                {
                    active.lock().await.remove(&request_id);
                    return Err(error);
                }
            }
            ConnectorRequestV2::Cancel { request_id } => {
                let cancellation = active.lock().await.get(&request_id).cloned();
                if let Some(cancellation) = cancellation {
                    cancellation.cancel();
                } else {
                    send_error(
                        &responses,
                        Some(request_id),
                        DbError::new("42704", "connector request does not exist"),
                    )
                    .await?;
                }
            }
            ConnectorRequestV2::Begin {
                request_id,
                connection_id,
                isolation,
            } => {
                send_session_command(
                    &sessions,
                    &connection_id,
                    &responses,
                    Some(request_id.clone()),
                    SessionCommand::Begin {
                        request_id,
                        isolation,
                    },
                )
                .await?;
            }
            ConnectorRequestV2::Commit {
                request_id,
                connection_id,
            } => {
                send_session_command(
                    &sessions,
                    &connection_id,
                    &responses,
                    Some(request_id.clone()),
                    SessionCommand::Commit { request_id },
                )
                .await?;
            }
            ConnectorRequestV2::Rollback {
                request_id,
                connection_id,
            } => {
                send_session_command(
                    &sessions,
                    &connection_id,
                    &responses,
                    Some(request_id.clone()),
                    SessionCommand::Rollback { request_id },
                )
                .await?;
            }
            ConnectorRequestV2::Shutdown => {
                for session in sessions.into_values() {
                    let _ = session.send(SessionCommand::Shutdown).await;
                }
                responses
                    .send(ConnectorResponseV2::Shutdown)
                    .await
                    .map_err(|_| connection_closed())?;
                return Ok(());
            }
        }
    }
}

async fn run_session(
    mut session: Box<dyn ConnectorSession>,
    mut commands: mpsc::Receiver<SessionCommand>,
    responses: mpsc::Sender<ConnectorResponseV2>,
    active: ActiveRequests,
) {
    while let Some(command) = commands.recv().await {
        match command {
            SessionCommand::Catalog { request_id } => {
                let response = match session.catalog().await {
                    Ok(objects) => ConnectorResponseV2::Catalog {
                        request_id: request_id.clone(),
                        objects,
                    },
                    Err(error) => error_response(Some(request_id.clone()), error),
                };
                if responses.send(response).await.is_err() {
                    return;
                }
            }
            SessionCommand::Execute {
                request_id,
                sql,
                params,
                batch_size,
                cancellation,
            } => {
                let mut sink = ChannelEventSink {
                    request_id: request_id.clone(),
                    responses: responses.clone(),
                };
                let result = session
                    .execute(
                        &request_id,
                        &sql,
                        &params,
                        batch_size,
                        &cancellation,
                        &mut sink,
                    )
                    .await;
                active.lock().await.remove(&request_id);
                if let Err(error) = result {
                    let response = if error.sql_state == "57014" || cancellation.is_cancelled() {
                        ConnectorResponseV2::Cancelled { request_id }
                    } else {
                        error_response(Some(request_id), error)
                    };
                    if responses.send(response).await.is_err() {
                        return;
                    }
                }
            }
            SessionCommand::Begin {
                request_id,
                isolation,
            } => {
                let response = transaction_response(
                    request_id,
                    session.begin(isolation).await,
                    ConnectorTransactionStateV2::Active,
                );
                if responses.send(response).await.is_err() {
                    return;
                }
            }
            SessionCommand::Commit { request_id } => {
                let response = transaction_response(
                    request_id,
                    session.commit().await,
                    ConnectorTransactionStateV2::Idle,
                );
                if responses.send(response).await.is_err() {
                    return;
                }
            }
            SessionCommand::Rollback { request_id } => {
                let response = transaction_response(
                    request_id,
                    session.rollback().await,
                    ConnectorTransactionStateV2::Idle,
                );
                if responses.send(response).await.is_err() {
                    return;
                }
            }
            SessionCommand::Shutdown => return,
        }
    }
}

fn transaction_response(
    request_id: String,
    result: Result<()>,
    state: ConnectorTransactionStateV2,
) -> ConnectorResponseV2 {
    match result {
        Ok(()) => ConnectorResponseV2::Transaction { request_id, state },
        Err(error) => error_response(Some(request_id), error),
    }
}

async fn send_session_command(
    sessions: &BTreeMap<String, mpsc::Sender<SessionCommand>>,
    connection_id: &str,
    responses: &mpsc::Sender<ConnectorResponseV2>,
    request_id: Option<String>,
    command: SessionCommand,
) -> Result<()> {
    let Some(session) = sessions.get(connection_id) else {
        send_error(
            responses,
            request_id,
            DbError::new("08003", "connector connection does not exist"),
        )
        .await?;
        return Ok(());
    };
    session
        .send(command)
        .await
        .map_err(|_| DbError::new("08003", "connector connection is closed"))
}

async fn write_responses(
    mut writer: WriteHalf<NamedPipeClient>,
    mut responses: mpsc::Receiver<ConnectorResponseV2>,
) -> Result<()> {
    while let Some(response) = responses.recv().await {
        write_connector_frame(&mut writer, &response).await?;
    }
    Ok(())
}

async fn send_error(
    responses: &mpsc::Sender<ConnectorResponseV2>,
    request_id: Option<String>,
    error: DbError,
) -> Result<()> {
    responses
        .send(error_response(request_id, error))
        .await
        .map_err(|_| connection_closed())
}

fn error_response(request_id: Option<String>, error: DbError) -> ConnectorResponseV2 {
    ConnectorResponseV2::Error {
        request_id,
        error: ConnectorErrorV2::from_db_error(&error),
    }
}

fn validate_helper_identity(plugin_id: &str, plugin_version: &str) -> Result<()> {
    validate_id(plugin_id, "connector plugin ID")?;
    if plugin_version.is_empty()
        || plugin_version.len() > 64
        || plugin_version.chars().any(char::is_control)
    {
        return Err(invalid("connector plugin version is invalid"));
    }
    Ok(())
}

fn validate_id(value: &str, name: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value.chars().any(|character| {
            !character.is_ascii_alphanumeric() && !matches!(character, '-' | '_' | '.')
        })
    {
        return Err(invalid(format!("{name} is invalid")));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::new("08P01", message)
}

fn io_error(context: impl Into<String>, error: impl std::fmt::Display) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

fn connection_closed() -> DbError {
    DbError::new("08006", "connector host closed the named pipe")
}
