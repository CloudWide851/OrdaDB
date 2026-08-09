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

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    use async_trait::async_trait;
    use serde_json::json;
    use tokio::net::windows::named_pipe::ServerOptions;

    use super::*;
    use crate::{
        ConnectorCatalogNodeKindV3, ConnectorCatalogNodeV3, ConnectorCommandInputModeV3,
        ConnectorCommandLanguageV3, ConnectorEndpointV2, ConnectorKeyValueV3,
        ConnectorResultBatchV3, ConnectorTlsModeV2, ConnectorValueV2, ProtocolHelloV3,
        read_connector_frame_v3, write_connector_frame_v3,
    };

    static PIPE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    #[derive(Clone)]
    struct FakeDriver {
        capabilities: ConnectorCapabilitiesV3,
    }

    struct FakeSession {
        capabilities: ConnectorCapabilitiesV3,
    }

    #[async_trait]
    impl ConnectorDriverV3 for FakeDriver {
        fn capabilities(&self) -> ConnectorCapabilitiesV3 {
            self.capabilities.clone()
        }

        async fn connect(
            &self,
            _endpoint: ConnectorEndpointV2,
            _tls_mode: ConnectorTlsModeV2,
            _credential: Option<crate::ConnectorCredentialV2>,
        ) -> Result<Box<dyn ConnectorSessionV3>> {
            Ok(Box::new(FakeSession {
                capabilities: self.capabilities.clone(),
            }))
        }
    }

    #[async_trait]
    impl ConnectorSessionV3 for FakeSession {
        fn capabilities(&self) -> &ConnectorCapabilitiesV3 {
            &self.capabilities
        }

        async fn catalog_page(
            &mut self,
            parent_id: Option<&str>,
            _page_size: u32,
            _cursor: Option<&str>,
        ) -> Result<crate::ConnectorCatalogPageV3> {
            Ok(crate::ConnectorCatalogPageV3 {
                nodes: vec![ConnectorCatalogNodeV3 {
                    id: "root/items".into(),
                    parent_id: parent_id.map(str::to_owned),
                    kind: match self.capabilities.kind {
                        ConnectorKindV3::Sql => ConnectorCatalogNodeKindV3::Table,
                        ConnectorKindV3::Document => ConnectorCatalogNodeKindV3::Collection,
                        ConnectorKindV3::KeyValue => ConnectorCatalogNodeKindV3::Keyspace,
                    },
                    name: "items".into(),
                    namespace: Some("root".into()),
                    has_children: false,
                    columns: Vec::new(),
                    attributes: BTreeMap::new(),
                }],
                next_cursor: None,
            })
        }

        async fn execute(
            &mut self,
            _request_id: &str,
            command: &ConnectorCommandV3,
            _batch_size: u32,
            cancellation: &CancellationToken,
            sink: &mut dyn ConnectorEventSinkV3,
        ) -> Result<()> {
            if matches!(
                command,
                ConnectorCommandV3::Document { document, .. }
                    if document.get("wait").and_then(serde_json::Value::as_bool) == Some(true)
            ) {
                tokio::select! {
                    () = cancellation.cancelled() => {
                        return Err(DbError::new("57014", "fake connector query was cancelled"));
                    }
                    () = tokio::time::sleep(Duration::from_secs(10)) => {
                        return Err(DbError::new("57014", "fake connector cancellation timed out"));
                    }
                }
            }
            match self.capabilities.kind {
                ConnectorKindV3::Sql => {
                    sink.send(ConnectorResultEventV3::Schema {
                        columns: Vec::new(),
                    })
                    .await?;
                    sink.send(ConnectorResultEventV3::Batch {
                        batch: ConnectorResultBatchV3::Rows {
                            rows: vec![Vec::new()],
                        },
                    })
                    .await?;
                }
                ConnectorKindV3::Document => {
                    sink.send(ConnectorResultEventV3::Batch {
                        batch: ConnectorResultBatchV3::Documents {
                            documents: vec![json!({ "ok": true })],
                        },
                    })
                    .await?;
                }
                ConnectorKindV3::KeyValue => {
                    sink.send(ConnectorResultEventV3::Batch {
                        batch: ConnectorResultBatchV3::KeyValues {
                            entries: vec![ConnectorKeyValueV3 {
                                key: ConnectorValueV2::Text("key".into()),
                                value: ConnectorValueV2::Text("value".into()),
                            }],
                        },
                    })
                    .await?;
                }
            }
            sink.send(ConnectorResultEventV3::Complete {
                command_tag: "OK".into(),
                affected_items: Some(1),
            })
            .await
        }

        async fn cancel(&mut self, _request_id: &str) -> Result<()> {
            Ok(())
        }

        async fn begin(&mut self, _isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
            Ok(())
        }

        async fn commit(&mut self) -> Result<()> {
            Ok(())
        }

        async fn rollback(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn capabilities(kind: ConnectorKindV3) -> ConnectorCapabilitiesV3 {
        let (id, input_modes) = match kind {
            ConnectorKindV3::Sql => ("postgresql-sql", vec![ConnectorCommandInputModeV3::Text]),
            ConnectorKindV3::Document => ("mql", vec![ConnectorCommandInputModeV3::Document]),
            ConnectorKindV3::KeyValue => ("resp3", vec![ConnectorCommandInputModeV3::Arguments]),
        };
        ConnectorCapabilitiesV3 {
            kind,
            command_languages: vec![ConnectorCommandLanguageV3 {
                id: id.into(),
                display_name: id.into(),
                input_modes,
            }],
            catalog: true,
            cancellation: true,
            transactions: true,
            savepoints: false,
            batch_query: true,
            maximum_batch_rows: 16,
            maximum_catalog_page_size: 16,
            tls_modes: vec![ConnectorTlsModeV2::Disable],
        }
    }

    fn command(kind: ConnectorKindV3, wait: bool) -> ConnectorCommandV3 {
        match kind {
            ConnectorKindV3::Sql => ConnectorCommandV3::Text {
                language_id: "postgresql-sql".into(),
                text: "SELECT 1".into(),
                params: Vec::new(),
            },
            ConnectorKindV3::Document => ConnectorCommandV3::Document {
                language_id: "mql".into(),
                document: json!({ "find": "items", "wait": wait }),
            },
            ConnectorKindV3::KeyValue => ConnectorCommandV3::Arguments {
                language_id: "resp3".into(),
                arguments: vec![
                    ConnectorValueV2::Text("GET".into()),
                    ConnectorValueV2::Text("key".into()),
                ],
            },
        }
    }

    async fn exercise_kind(kind: ConnectorKindV3) {
        let sequence = PIPE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let pipe_name = format!(
            r"\\.\pipe\ordadb-connector-sdk-v3-{}-{sequence}",
            std::process::id()
        );
        let mut server = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .create(&pipe_name)
            .expect("server pipe");
        let expected = capabilities(kind);
        let helper = tokio::spawn({
            let pipe_name = OsString::from(&pipe_name);
            let driver = FakeDriver {
                capabilities: expected.clone(),
            };
            async move {
                run_named_pipe_helper_v3(&pipe_name, "fake-v3", "1.0.0", driver)
                    .await
                    .expect("helper runtime");
            }
        });
        server.connect().await.expect("connect helper");
        write_connector_frame_v3(
            &mut server,
            &ConnectorRequestV3::Hello {
                hello: ProtocolHelloV3 {
                    minimum_api_version: CONNECTOR_PROTOCOL_V3,
                    maximum_api_version: CONNECTOR_PROTOCOL_V3,
                    plugin_id: "fake-v3".into(),
                    plugin_version: "1.0.0".into(),
                },
            },
        )
        .await
        .expect("hello");
        let ready: ConnectorResponseV3 = read_connector_frame_v3(&mut server).await.expect("ready");
        assert!(matches!(ready, ConnectorResponseV3::Ready { .. }));

        write_connector_frame_v3(
            &mut server,
            &ConnectorRequestV3::Connect {
                connection_id: "connection-1".into(),
                endpoint: ConnectorEndpointV2::Network {
                    host: "127.0.0.1".into(),
                    port: 1,
                    database: None,
                    instance: None,
                    options: BTreeMap::new(),
                },
                tls_mode: ConnectorTlsModeV2::Disable,
                credential: None,
            },
        )
        .await
        .expect("connect request");
        let connected: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
            .await
            .expect("connected");
        assert!(matches!(connected, ConnectorResponseV3::Connected { .. }));

        write_connector_frame_v3(
            &mut server,
            &ConnectorRequestV3::Catalog {
                request_id: "catalog-1".into(),
                connection_id: "connection-1".into(),
                parent_id: Some("root".into()),
                page_size: 16,
                cursor: None,
            },
        )
        .await
        .expect("catalog request");
        let catalog: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
            .await
            .expect("catalog page");
        assert!(matches!(catalog, ConnectorResponseV3::CatalogPage { .. }));

        write_connector_frame_v3(
            &mut server,
            &ConnectorRequestV3::Execute {
                request_id: "execute-1".into(),
                connection_id: "connection-1".into(),
                command: command(kind, false),
                batch_size: 16,
            },
        )
        .await
        .expect("execute request");
        loop {
            let response: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
                .await
                .expect("result event");
            if matches!(
                response,
                ConnectorResponseV3::ResultEvent {
                    event: ConnectorResultEventV3::Complete { .. },
                    ..
                }
            ) {
                break;
            }
        }

        write_connector_frame_v3(
            &mut server,
            &ConnectorRequestV3::Begin {
                request_id: "begin-1".into(),
                connection_id: "connection-1".into(),
                isolation: None,
            },
        )
        .await
        .expect("begin request");
        let transaction: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
            .await
            .expect("transaction response");
        assert!(matches!(
            transaction,
            ConnectorResponseV3::Transaction { .. }
        ));

        if kind == ConnectorKindV3::Document {
            write_connector_frame_v3(
                &mut server,
                &ConnectorRequestV3::Execute {
                    request_id: "cancel-1".into(),
                    connection_id: "connection-1".into(),
                    command: command(kind, true),
                    batch_size: 16,
                },
            )
            .await
            .expect("cancellable request");
            write_connector_frame_v3(
                &mut server,
                &ConnectorRequestV3::Cancel {
                    request_id: "cancel-1".into(),
                },
            )
            .await
            .expect("cancel request");
            let cancelled: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
                .await
                .expect("cancelled response");
            assert!(matches!(cancelled, ConnectorResponseV3::Cancelled { .. }));
        }

        write_connector_frame_v3(&mut server, &ConnectorRequestV3::Shutdown)
            .await
            .expect("shutdown request");
        let shutdown: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
            .await
            .expect("shutdown response");
        assert!(matches!(shutdown, ConnectorResponseV3::Shutdown));
        helper.await.expect("helper task");
    }

    #[tokio::test]
    async fn fake_v3_runtime_exercises_sql_document_and_key_value_models() {
        tokio::time::timeout(Duration::from_secs(10), exercise_kind(ConnectorKindV3::Sql))
            .await
            .expect("SQL v3 runtime exceeded its test deadline");
        tokio::time::timeout(
            Duration::from_secs(10),
            exercise_kind(ConnectorKindV3::Document),
        )
        .await
        .expect("document v3 runtime exceeded its test deadline");
        tokio::time::timeout(
            Duration::from_secs(10),
            exercise_kind(ConnectorKindV3::KeyValue),
        )
        .await
        .expect("key/value v3 runtime exceeded its test deadline");
    }
}
