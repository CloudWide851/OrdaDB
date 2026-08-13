use std::{collections::BTreeMap, ffi::OsStr, sync::Arc};

use ordadb_types::{DbError, Result};
use tokio::{
    io::{ReadHalf, WriteHalf, split},
    net::windows::named_pipe::{ClientOptions, NamedPipeClient},
    sync::{Mutex, mpsc},
};
use tokio_util::sync::CancellationToken;

use crate::driver_v3::ConnectorEventFutureV3;
use crate::{
    CONNECTOR_PROTOCOL_V3, ConnectorCapabilitiesV3, ConnectorCommandV3, ConnectorDriverV3,
    ConnectorErrorV3, ConnectorEventSinkV3, ConnectorIsolationLevelV2, ConnectorKindV3,
    ConnectorRequestV3, ConnectorResponseV3, ConnectorResultEventV3,
    ConnectorResultStreamValidatorV3, ConnectorSessionV3, ConnectorTransactionStateV2,
    ProtocolReadyV3, read_connector_frame_v3, validate_capabilities_v3,
    validate_capability_subset_v3, validate_catalog_page_v3, validate_catalog_request_v3,
    validate_command_v3, validate_endpoint, write_connector_frame_v3,
};

const RESPONSE_CHANNEL_CAPACITY: usize = 64;
const SESSION_CHANNEL_CAPACITY: usize = 16;

type ActiveRequests = Arc<Mutex<BTreeMap<String, CancellationToken>>>;

enum SessionCommandV3 {
    Catalog {
        request_id: String,
        parent_id: Option<String>,
        page_size: u32,
        cursor: Option<String>,
    },
    Execute {
        request_id: String,
        command: ConnectorCommandV3,
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

struct ChannelEventSinkV3 {
    request_id: String,
    responses: mpsc::Sender<ConnectorResponseV3>,
    validator: ConnectorResultStreamValidatorV3,
}

impl ConnectorEventSinkV3 for ChannelEventSinkV3 {
    fn send(&mut self, event: ConnectorResultEventV3) -> ConnectorEventFutureV3<'_> {
        Box::pin(async move {
            self.validator.validate(&event)?;
            self.responses
                .send(ConnectorResponseV3::ResultEvent {
                    request_id: self.request_id.clone(),
                    event,
                })
                .await
                .map_err(|_| connection_closed())
        })
    }
}

pub async fn run_named_pipe_helper_v3<D>(
    pipe_name: &OsStr,
    plugin_id: &str,
    plugin_version: &str,
    driver: D,
) -> Result<()>
where
    D: ConnectorDriverV3,
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
    validate_capabilities_v3(&capabilities)?;
    negotiate_v3(&mut pipe, plugin_id, plugin_version, capabilities.clone()).await?;

    let (reader, writer) = split(pipe);
    let (responses, response_rx) = mpsc::channel(RESPONSE_CHANNEL_CAPACITY);
    let writer_task = tokio::spawn(write_responses_v3(writer, response_rx));
    let result = run_requests_v3(reader, Arc::new(driver), capabilities, responses.clone()).await;
    drop(responses);
    let writer_result = writer_task.await.map_err(|error| {
        DbError::internal("connector response writer task failed").with_detail(error.to_string())
    })?;
    result.and(writer_result)
}

async fn negotiate_v3(
    pipe: &mut NamedPipeClient,
    plugin_id: &str,
    plugin_version: &str,
    capabilities: ConnectorCapabilitiesV3,
) -> Result<()> {
    let request: ConnectorRequestV3 = read_connector_frame_v3(pipe).await?;
    let ConnectorRequestV3::Hello { hello } = request else {
        return Err(protocol_error(
            "connector host did not begin with a Hello request",
        ));
    };
    if hello.minimum_api_version > CONNECTOR_PROTOCOL_V3
        || hello.maximum_api_version < CONNECTOR_PROTOCOL_V3
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
    write_connector_frame_v3(
        pipe,
        &ConnectorResponseV3::Ready {
            ready: ProtocolReadyV3 {
                api_version: CONNECTOR_PROTOCOL_V3,
                plugin_id: plugin_id.into(),
                plugin_version: plugin_version.into(),
                capabilities,
            },
        },
    )
    .await
}

async fn run_requests_v3<D>(
    mut reader: ReadHalf<NamedPipeClient>,
    driver: Arc<D>,
    capabilities: ConnectorCapabilitiesV3,
    responses: mpsc::Sender<ConnectorResponseV3>,
) -> Result<()>
where
    D: ConnectorDriverV3,
{
    let mut sessions = BTreeMap::<String, mpsc::Sender<SessionCommandV3>>::new();
    let active = Arc::new(Mutex::new(BTreeMap::new()));
    loop {
        let request: ConnectorRequestV3 = read_connector_frame_v3(&mut reader).await?;
        match request {
            ConnectorRequestV3::Hello { .. } => {
                send_error_v3(
                    &responses,
                    None,
                    capabilities.kind,
                    protocol_error("connector Hello was repeated"),
                )
                .await?;
            }
            ConnectorRequestV3::Connect {
                connection_id,
                endpoint,
                tls_mode,
                credential,
            } => {
                validate_id(&connection_id, "connection ID")?;
                validate_endpoint(&endpoint)?;
                if !capabilities.tls_modes.contains(&tls_mode) {
                    send_error_v3(
                        &responses,
                        None,
                        capabilities.kind,
                        DbError::unsupported("connector TLS mode"),
                    )
                    .await?;
                    continue;
                }
                if sessions.contains_key(&connection_id) {
                    send_error_v3(
                        &responses,
                        None,
                        capabilities.kind,
                        DbError::new("42P04", "connector connection already exists"),
                    )
                    .await?;
                    continue;
                }
                match driver.connect(endpoint, tls_mode, credential).await {
                    Ok(session) => {
                        let session_capabilities = session.capabilities().clone();
                        if let Err(error) =
                            validate_capability_subset_v3(&capabilities, &session_capabilities)
                        {
                            send_error_v3(&responses, None, capabilities.kind, error).await?;
                            continue;
                        }
                        let (commands, command_rx) = mpsc::channel(SESSION_CHANNEL_CAPACITY);
                        tokio::spawn(run_session_v3(
                            session,
                            command_rx,
                            responses.clone(),
                            Arc::clone(&active),
                            session_capabilities.clone(),
                        ));
                        sessions.insert(connection_id.clone(), commands);
                        responses
                            .send(ConnectorResponseV3::Connected {
                                connection_id,
                                capabilities: session_capabilities,
                            })
                            .await
                            .map_err(|_| connection_closed())?;
                    }
                    Err(error) => {
                        send_error_v3(&responses, None, capabilities.kind, error).await?;
                    }
                }
            }
            ConnectorRequestV3::Disconnect { connection_id } => {
                let Some(session) = sessions.remove(&connection_id) else {
                    send_error_v3(
                        &responses,
                        None,
                        capabilities.kind,
                        DbError::new("08003", "connector connection does not exist"),
                    )
                    .await?;
                    continue;
                };
                session
                    .send(SessionCommandV3::Shutdown)
                    .await
                    .map_err(|_| DbError::new("08003", "connector connection is closed"))?;
                responses
                    .send(ConnectorResponseV3::Disconnected { connection_id })
                    .await
                    .map_err(|_| connection_closed())?;
            }
            ConnectorRequestV3::Catalog {
                request_id,
                connection_id,
                parent_id,
                page_size,
                cursor,
            } => {
                validate_id(&request_id, "request ID")?;
                if let Err(error) = validate_catalog_request_v3(
                    parent_id.as_deref(),
                    page_size,
                    cursor.as_deref(),
                    &capabilities,
                ) {
                    send_error_v3(&responses, Some(request_id), capabilities.kind, error).await?;
                    continue;
                }
                send_session_command_v3(
                    &sessions,
                    &connection_id,
                    &responses,
                    Some(request_id.clone()),
                    capabilities.kind,
                    SessionCommandV3::Catalog {
                        request_id,
                        parent_id,
                        page_size,
                        cursor,
                    },
                )
                .await?;
            }
            ConnectorRequestV3::Execute {
                request_id,
                connection_id,
                command,
                batch_size,
            } => {
                validate_id(&request_id, "request ID")?;
                if batch_size == 0 || batch_size > capabilities.maximum_batch_rows {
                    send_error_v3(
                        &responses,
                        Some(request_id),
                        capabilities.kind,
                        invalid("connector batch size is outside its capability"),
                    )
                    .await?;
                    continue;
                }
                if let Err(error) = validate_command_v3(&command, &capabilities) {
                    send_error_v3(&responses, Some(request_id), capabilities.kind, error).await?;
                    continue;
                }
                let cancellation = CancellationToken::new();
                {
                    let mut active = active.lock().await;
                    if active.contains_key(&request_id) {
                        drop(active);
                        send_error_v3(
                            &responses,
                            Some(request_id),
                            capabilities.kind,
                            DbError::new("42P04", "connector request already exists"),
                        )
                        .await?;
                        continue;
                    }
                    active.insert(request_id.clone(), cancellation.clone());
                }
                if let Err(error) = send_session_command_v3(
                    &sessions,
                    &connection_id,
                    &responses,
                    Some(request_id.clone()),
                    capabilities.kind,
                    SessionCommandV3::Execute {
                        request_id: request_id.clone(),
                        command,
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
            ConnectorRequestV3::Cancel { request_id } => {
                if !capabilities.cancellation {
                    send_error_v3(
                        &responses,
                        Some(request_id),
                        capabilities.kind,
                        DbError::unsupported("connector cancellation"),
                    )
                    .await?;
                    continue;
                }
                let cancellation = active.lock().await.get(&request_id).cloned();
                if let Some(cancellation) = cancellation {
                    cancellation.cancel();
                } else {
                    send_error_v3(
                        &responses,
                        Some(request_id),
                        capabilities.kind,
                        DbError::new("42704", "connector request does not exist"),
                    )
                    .await?;
                }
            }
            ConnectorRequestV3::Begin {
                request_id,
                connection_id,
                isolation,
            } => {
                if !capabilities.transactions {
                    send_error_v3(
                        &responses,
                        Some(request_id),
                        capabilities.kind,
                        DbError::unsupported("connector transactions"),
                    )
                    .await?;
                    continue;
                }
                send_session_command_v3(
                    &sessions,
                    &connection_id,
                    &responses,
                    Some(request_id.clone()),
                    capabilities.kind,
                    SessionCommandV3::Begin {
                        request_id,
                        isolation,
                    },
                )
                .await?;
            }
            ConnectorRequestV3::Commit {
                request_id,
                connection_id,
            } => {
                send_session_command_v3(
                    &sessions,
                    &connection_id,
                    &responses,
                    Some(request_id.clone()),
                    capabilities.kind,
                    SessionCommandV3::Commit { request_id },
                )
                .await?;
            }
            ConnectorRequestV3::Rollback {
                request_id,
                connection_id,
            } => {
                send_session_command_v3(
                    &sessions,
                    &connection_id,
                    &responses,
                    Some(request_id.clone()),
                    capabilities.kind,
                    SessionCommandV3::Rollback { request_id },
                )
                .await?;
            }
            ConnectorRequestV3::Shutdown => {
                for session in sessions.into_values() {
                    session
                        .send(SessionCommandV3::Shutdown)
                        .await
                        .map_err(|_| DbError::new("08003", "connector connection is closed"))?;
                }
                responses
                    .send(ConnectorResponseV3::Shutdown)
                    .await
                    .map_err(|_| connection_closed())?;
                return Ok(());
            }
        }
    }
}

async fn run_session_v3(
    mut session: Box<dyn ConnectorSessionV3>,
    mut commands: mpsc::Receiver<SessionCommandV3>,
    responses: mpsc::Sender<ConnectorResponseV3>,
    active: ActiveRequests,
    capabilities: ConnectorCapabilitiesV3,
) {
    while let Some(command) = commands.recv().await {
        match command {
            SessionCommandV3::Catalog {
                request_id,
                parent_id,
                page_size,
                cursor,
            } => {
                let response = match session
                    .catalog_page(parent_id.as_deref(), page_size, cursor.as_deref())
                    .await
                {
                    Ok(page) => match validate_catalog_page_v3(&page, page_size) {
                        Ok(()) => ConnectorResponseV3::CatalogPage {
                            request_id: request_id.clone(),
                            page,
                        },
                        Err(error) => {
                            error_response_v3(Some(request_id.clone()), capabilities.kind, error)
                        }
                    },
                    Err(error) => {
                        error_response_v3(Some(request_id.clone()), capabilities.kind, error)
                    }
                };
                if responses.send(response).await.is_err() {
                    return;
                }
            }
            SessionCommandV3::Execute {
                request_id,
                command,
                batch_size,
                cancellation,
            } => {
                let mut sink = ChannelEventSinkV3 {
                    request_id: request_id.clone(),
                    responses: responses.clone(),
                    validator: ConnectorResultStreamValidatorV3::new(capabilities.kind, batch_size),
                };
                let result = session
                    .execute(&request_id, &command, batch_size, &cancellation, &mut sink)
                    .await;
                let terminal = sink.validator.is_terminal();
                active.lock().await.remove(&request_id);
                let response = match result {
                    Ok(()) if terminal => None,
                    Ok(()) => Some(error_response_v3(
                        Some(request_id),
                        capabilities.kind,
                        DbError::internal(
                            "connector execution ended without a terminal result event",
                        ),
                    )),
                    Err(_) if terminal => None,
                    Err(error) if error.sql_state == "57014" || cancellation.is_cancelled() => {
                        Some(ConnectorResponseV3::Cancelled { request_id })
                    }
                    Err(error) => Some(error_response_v3(
                        Some(request_id),
                        capabilities.kind,
                        error,
                    )),
                };
                if let Some(response) = response
                    && responses.send(response).await.is_err()
                {
                    return;
                }
            }
            SessionCommandV3::Begin {
                request_id,
                isolation,
            } => {
                let response = transaction_response_v3(
                    request_id,
                    session.begin(isolation).await,
                    ConnectorTransactionStateV2::Active,
                    capabilities.kind,
                );
                if responses.send(response).await.is_err() {
                    return;
                }
            }
            SessionCommandV3::Commit { request_id } => {
                let response = transaction_response_v3(
                    request_id,
                    session.commit().await,
                    ConnectorTransactionStateV2::Idle,
                    capabilities.kind,
                );
                if responses.send(response).await.is_err() {
                    return;
                }
            }
            SessionCommandV3::Rollback { request_id } => {
                let response = transaction_response_v3(
                    request_id,
                    session.rollback().await,
                    ConnectorTransactionStateV2::Idle,
                    capabilities.kind,
                );
                if responses.send(response).await.is_err() {
                    return;
                }
            }
            SessionCommandV3::Shutdown => return,
        }
    }
}

fn transaction_response_v3(
    request_id: String,
    result: Result<()>,
    state: ConnectorTransactionStateV2,
    kind: ConnectorKindV3,
) -> ConnectorResponseV3 {
    match result {
        Ok(()) => ConnectorResponseV3::Transaction { request_id, state },
        Err(error) => error_response_v3(Some(request_id), kind, error),
    }
}

async fn send_session_command_v3(
    sessions: &BTreeMap<String, mpsc::Sender<SessionCommandV3>>,
    connection_id: &str,
    responses: &mpsc::Sender<ConnectorResponseV3>,
    request_id: Option<String>,
    kind: ConnectorKindV3,
    command: SessionCommandV3,
) -> Result<()> {
    let Some(session) = sessions.get(connection_id) else {
        send_error_v3(
            responses,
            request_id,
            kind,
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

async fn write_responses_v3(
    mut writer: WriteHalf<NamedPipeClient>,
    mut responses: mpsc::Receiver<ConnectorResponseV3>,
) -> Result<()> {
    while let Some(response) = responses.recv().await {
        write_connector_frame_v3(&mut writer, &response).await?;
    }
    Ok(())
}

async fn send_error_v3(
    responses: &mpsc::Sender<ConnectorResponseV3>,
    request_id: Option<String>,
    kind: ConnectorKindV3,
    error: DbError,
) -> Result<()> {
    responses
        .send(error_response_v3(request_id, kind, error))
        .await
        .map_err(|_| connection_closed())
}

fn error_response_v3(
    request_id: Option<String>,
    kind: ConnectorKindV3,
    error: DbError,
) -> ConnectorResponseV3 {
    ConnectorResponseV3::Error {
        request_id,
        error: ConnectorErrorV3::from_db_error(&error, kind),
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
