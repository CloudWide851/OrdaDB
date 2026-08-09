use std::{
    collections::{BTreeMap, VecDeque},
    path::Path,
    process::{ExitStatus, Stdio},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use ordadb_connector_sdk::{
    CONNECTOR_PROTOCOL_V2, CONNECTOR_PROTOCOL_V3, ConnectorCapabilitiesV3,
    ConnectorCatalogNodeKindV3, ConnectorCatalogObjectKindV2, ConnectorCommandV3,
    ConnectorCredentialV2, ConnectorEndpointV2, ConnectorKindV3, ConnectorLogicalTypeV2,
    ConnectorParameterV2, ConnectorQueryEventV2, ConnectorRequestV2, ConnectorRequestV3,
    ConnectorResponseV2, ConnectorResponseV3, ConnectorResultBatchV3, ConnectorResultEventV3,
    ConnectorResultStreamValidatorV3, ConnectorTlsModeV2, ConnectorTypeV2, ConnectorValueV2,
    ProtocolHelloV2, ProtocolHelloV3, read_connector_frame as read_connector_frame_v2,
    read_connector_frame_v3, validate_catalog_page_v3, validate_catalog_request_v3,
    validate_command_v3, validate_endpoint, validate_error_v3,
    validate_protocol_ready as validate_protocol_ready_v2, validate_protocol_ready_v3,
    write_connector_frame as write_connector_frame_v2, write_connector_frame_v3,
};
use ordadb_types::{
    Batch, CommandComplete, DbError, DbNotice, Field, QueryEvent, QueryProgress, Result, Row,
    ScalarType, Schema, Value,
};
use rust_decimal::Decimal;
use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
use tokio::process::{Child, Command};
use uuid::Uuid;

use crate::{
    CatalogEntry, ConnectorRequestV1, ConnectorResponseV1, CredentialPayload,
    MIN_CONNECTOR_API_VERSION, PluginManager, io_error, network_error, read_connector_frame,
    validate_protocol_ready, write_connector_frame,
};

const PIPE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_HOST_ACTIVE_V3_REQUESTS: usize = 64;

#[derive(Debug, Clone, PartialEq)]
enum NegotiatedProtocol {
    V1,
    V2,
    V3(ConnectorCapabilitiesV3),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingV3Request {
    Catalog { maximum_nodes: u32 },
    Execute { maximum_batch_rows: u32 },
    Transaction,
}

pub struct ConnectorHost {
    child: Child,
    pipe: NamedPipeServer,
    plugin_id: String,
    plugin_version: String,
    protocol: NegotiatedProtocol,
    query_schemas: BTreeMap<String, Schema>,
    v3_result_validators: BTreeMap<String, ConnectorResultStreamValidatorV3>,
    v3_pending_requests: BTreeMap<String, PendingV3Request>,
    queued_responses: VecDeque<ConnectorResponseV1>,
}

impl std::fmt::Debug for ConnectorHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorHost")
            .field("plugin_id", &self.plugin_id)
            .field("plugin_version", &self.plugin_version)
            .field("protocol", &self.protocol)
            .field("process_id", &self.child.id())
            .finish_non_exhaustive()
    }
}

impl ConnectorHost {
    pub async fn launch(manager: &Arc<PluginManager>, plugin_id: &str) -> Result<Self> {
        let installation = manager.active_installation(plugin_id)?;
        Self::launch_entry(
            &installation.entry,
            &installation.manifest.id,
            &installation.manifest.version,
            installation.manifest.api_version,
        )
        .await
    }

    async fn launch_entry(
        entry: &Path,
        plugin_id: &str,
        plugin_version: &str,
        api_version: u32,
    ) -> Result<Self> {
        let pipe_name = connector_pipe_name();
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true);
        let mut pipe = options
            .create(&pipe_name)
            .map_err(|error| io_error("failed to create connector named pipe", error))?;
        ordadb_windows::restrict_named_pipe_acl(&pipe)?;

        let mut command = Command::new(entry);
        command
            .arg("--ordadb-pipe")
            .arg(&pipe_name)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        configure_hidden_process(&mut command);
        let mut child = command
            .spawn()
            .map_err(|error| io_error("failed to start connector helper process", error))?;

        let connected = tokio::select! {
            connected = pipe.connect() => connected.map_err(|error| {
                io_error("connector named-pipe connection failed", error)
            }),
            status = child.wait() => {
                return Err(helper_exit_error(status));
            }
            () = tokio::time::sleep(PIPE_CONNECT_TIMEOUT) => {
                return Err(network_error(
                    "connector helper did not connect before the deadline",
                    "named-pipe connection timeout",
                ));
            }
        };
        if let Err(error) = connected {
            let _ = child.kill().await;
            return Err(error);
        }

        let protocol = match negotiate(&mut pipe, plugin_id, plugin_version, api_version).await {
            Ok(protocol) => protocol,
            Err(error) => {
                let _ = child.kill().await;
                return Err(error);
            }
        };
        Ok(Self {
            child,
            pipe,
            plugin_id: plugin_id.into(),
            plugin_version: plugin_version.into(),
            protocol,
            query_schemas: BTreeMap::new(),
            v3_result_validators: BTreeMap::new(),
            v3_pending_requests: BTreeMap::new(),
            queued_responses: VecDeque::new(),
        })
    }

    pub async fn connect(
        &mut self,
        connection_id: impl Into<String>,
        endpoint: impl Into<String>,
        database: Option<String>,
        credential: CredentialPayload,
    ) -> Result<()> {
        let connection_id = connection_id.into();
        self.send(&ConnectorRequestV1::Connect {
            connection_id: connection_id.clone(),
            endpoint: endpoint.into(),
            database,
            credential,
        })
        .await?;
        match self.receive().await? {
            ConnectorResponseV1::Connected {
                connection_id: actual,
            } if actual == connection_id => Ok(()),
            ConnectorResponseV1::Error { error, .. } => Err(error),
            _ => Err(DbError::new(
                "08P01",
                "connector returned an unexpected connect response",
            )),
        }
    }

    #[must_use]
    pub const fn protocol_version(&self) -> u32 {
        match &self.protocol {
            NegotiatedProtocol::V1 => MIN_CONNECTOR_API_VERSION,
            NegotiatedProtocol::V2 => CONNECTOR_PROTOCOL_V2,
            NegotiatedProtocol::V3(_) => CONNECTOR_PROTOCOL_V3,
        }
    }

    #[must_use]
    pub fn capabilities_v3(&self) -> Option<&ConnectorCapabilitiesV3> {
        match &self.protocol {
            NegotiatedProtocol::V3(capabilities) => Some(capabilities),
            NegotiatedProtocol::V1 | NegotiatedProtocol::V2 => None,
        }
    }

    pub async fn connect_v3(
        &mut self,
        connection_id: impl Into<String>,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<ConnectorCapabilitiesV3> {
        let connection_id = connection_id.into();
        self.send_v3(&ConnectorRequestV3::Connect {
            connection_id: connection_id.clone(),
            endpoint,
            tls_mode,
            credential,
        })
        .await?;
        match self.receive_v3().await? {
            ConnectorResponseV3::Connected {
                connection_id: actual,
                capabilities,
            } if actual == connection_id => Ok(capabilities),
            ConnectorResponseV3::Error { error, .. } => Err(error.into_db_error()),
            _ => Err(DbError::new(
                "08P01",
                "connector returned an unexpected v3 connect response",
            )),
        }
    }

    pub async fn send(&mut self, request: &ConnectorRequestV1) -> Result<()> {
        self.ensure_running()?;
        match self.protocol_version() {
            MIN_CONNECTOR_API_VERSION => write_connector_frame(&mut self.pipe, request).await,
            CONNECTOR_PROTOCOL_V2 => {
                let request_id = request_id(request).map(str::to_owned);
                match translate_request_v2(request, &self.plugin_id)? {
                    Some(request) => write_connector_frame_v2(&mut self.pipe, &request).await,
                    None => {
                        self.queued_responses.push_back(ConnectorResponseV1::Error {
                            request_id,
                            error: DbError::unsupported("monitoring through connector protocol v2"),
                        });
                        Ok(())
                    }
                }
            }
            CONNECTOR_PROTOCOL_V3 => {
                let capabilities = self
                    .capabilities_v3()
                    .cloned()
                    .ok_or_else(|| DbError::internal("connector v3 capabilities are missing"))?;
                ensure_legacy_sql_v3(&capabilities)?;
                let request_id = request_id(request).map(str::to_owned);
                match translate_request_v3(request, &self.plugin_id, &capabilities)? {
                    Some(request) => self.send_v3(&request).await,
                    None => {
                        self.queued_responses.push_back(ConnectorResponseV1::Error {
                            request_id,
                            error: DbError::unsupported("monitoring through connector protocol v3"),
                        });
                        Ok(())
                    }
                }
            }
            unsupported => Err(DbError::unsupported(format!(
                "connector protocol version {unsupported}"
            ))),
        }
    }

    pub async fn send_v3(&mut self, request: &ConnectorRequestV3) -> Result<()> {
        self.ensure_running()?;
        let capabilities = self
            .capabilities_v3()
            .cloned()
            .ok_or_else(|| DbError::unsupported("native connector protocol v3"))?;
        validate_request_v3(request, &capabilities)?;
        let pending = prepare_v3_request(request, &self.v3_pending_requests)?;
        write_connector_frame_v3(&mut self.pipe, request).await?;
        if let Some((request_id, pending)) = pending {
            self.v3_pending_requests.insert(request_id, pending);
        }
        Ok(())
    }

    pub async fn receive(&mut self) -> Result<ConnectorResponseV1> {
        if let Some(response) = self.queued_responses.pop_front() {
            return Ok(response);
        }
        match self.protocol_version() {
            MIN_CONNECTOR_API_VERSION => tokio::select! {
                response = read_connector_frame(&mut self.pipe) => response,
                status = self.child.wait() => Err(helper_exit_error(status)),
            },
            CONNECTOR_PROTOCOL_V2 => {
                let response = tokio::select! {
                    response = read_connector_frame_v2(&mut self.pipe) => response,
                    status = self.child.wait() => return Err(helper_exit_error(status)),
                }?;
                translate_response_v2(response, &mut self.query_schemas)
            }
            CONNECTOR_PROTOCOL_V3 => {
                let response = self.receive_v3().await?;
                translate_response_v3(response, &mut self.query_schemas)
            }
            unsupported => Err(DbError::unsupported(format!(
                "connector protocol version {unsupported}"
            ))),
        }
    }

    pub async fn receive_v3(&mut self) -> Result<ConnectorResponseV3> {
        let capabilities = self
            .capabilities_v3()
            .cloned()
            .ok_or_else(|| DbError::unsupported("native connector protocol v3"))?;
        let response = tokio::select! {
            response = read_connector_frame_v3(&mut self.pipe) => response,
            status = self.child.wait() => return Err(helper_exit_error(status)),
        }?;
        validate_v3_response(
            &response,
            &capabilities,
            &mut self.v3_pending_requests,
            &mut self.v3_result_validators,
        )?;
        Ok(response)
    }

    pub async fn shutdown(mut self) -> Result<()> {
        let send_result = if self.protocol_version() == CONNECTOR_PROTOCOL_V3 {
            self.send_v3(&ConnectorRequestV3::Shutdown).await
        } else {
            self.send(&ConnectorRequestV1::Shutdown).await
        };
        match tokio::time::timeout(SHUTDOWN_TIMEOUT, self.child.wait()).await {
            Ok(Ok(_)) => send_result,
            Ok(Err(error)) => Err(io_error(
                "failed to wait for connector helper shutdown",
                error,
            )),
            Err(_) => {
                self.child
                    .kill()
                    .await
                    .map_err(|error| io_error("failed to stop connector helper process", error))?;
                Ok(())
            }
        }
    }

    fn ensure_running(&mut self) -> Result<()> {
        if self
            .child
            .try_wait()
            .map_err(|error| io_error("failed to inspect connector helper process", error))?
            .is_some()
        {
            return Err(network_error(
                "connector helper process exited",
                "process is no longer running",
            ));
        }
        Ok(())
    }
}

async fn negotiate(
    pipe: &mut NamedPipeServer,
    plugin_id: &str,
    plugin_version: &str,
    api_version: u32,
) -> Result<NegotiatedProtocol> {
    match api_version {
        MIN_CONNECTOR_API_VERSION => {
            write_connector_frame(
                pipe,
                &ConnectorRequestV1::Hello {
                    api_version,
                    plugin_id: plugin_id.into(),
                    plugin_version: plugin_version.into(),
                },
            )
            .await?;
            let response = tokio::time::timeout(
                HANDSHAKE_TIMEOUT,
                read_connector_frame::<_, ConnectorResponseV1>(pipe),
            )
            .await
            .map_err(|_| handshake_timeout())??;
            match response {
                ConnectorResponseV1::Ready(ready) => {
                    validate_protocol_ready(&ready, plugin_id, plugin_version)?;
                    Ok(NegotiatedProtocol::V1)
                }
                ConnectorResponseV1::Error { error, .. } => Err(error),
                _ => Err(handshake_response_error()),
            }
        }
        CONNECTOR_PROTOCOL_V2 => {
            write_connector_frame_v2(
                pipe,
                &ConnectorRequestV2::Hello {
                    hello: ProtocolHelloV2 {
                        minimum_api_version: CONNECTOR_PROTOCOL_V2,
                        maximum_api_version: CONNECTOR_PROTOCOL_V2,
                        plugin_id: plugin_id.into(),
                        plugin_version: plugin_version.into(),
                    },
                },
            )
            .await?;
            let response = tokio::time::timeout(
                HANDSHAKE_TIMEOUT,
                read_connector_frame_v2::<_, ConnectorResponseV2>(pipe),
            )
            .await
            .map_err(|_| handshake_timeout())??;
            match response {
                ConnectorResponseV2::Ready { ready } => {
                    validate_protocol_ready_v2(&ready, plugin_id, plugin_version)?;
                    Ok(NegotiatedProtocol::V2)
                }
                ConnectorResponseV2::Error { error, .. } => Err(error.into_db_error()),
                _ => Err(handshake_response_error()),
            }
        }
        CONNECTOR_PROTOCOL_V3 => {
            write_connector_frame_v3(
                pipe,
                &ConnectorRequestV3::Hello {
                    hello: ProtocolHelloV3 {
                        minimum_api_version: CONNECTOR_PROTOCOL_V3,
                        maximum_api_version: CONNECTOR_PROTOCOL_V3,
                        plugin_id: plugin_id.into(),
                        plugin_version: plugin_version.into(),
                    },
                },
            )
            .await?;
            let response = tokio::time::timeout(
                HANDSHAKE_TIMEOUT,
                read_connector_frame_v3::<_, ConnectorResponseV3>(pipe),
            )
            .await
            .map_err(|_| handshake_timeout())??;
            match response {
                ConnectorResponseV3::Ready { ready } => {
                    validate_protocol_ready_v3(&ready, plugin_id, plugin_version)?;
                    Ok(NegotiatedProtocol::V3(ready.capabilities))
                }
                ConnectorResponseV3::Error { error, .. } => Err(error.into_db_error()),
                _ => Err(handshake_response_error()),
            }
        }
        unsupported => Err(DbError::unsupported(format!(
            "connector protocol version {unsupported}"
        ))),
    }
}

fn translate_request_v2(
    request: &ConnectorRequestV1,
    plugin_id: &str,
) -> Result<Option<ConnectorRequestV2>> {
    let request = match request {
        ConnectorRequestV1::Hello { .. } => {
            return Err(DbError::new(
                "08P01",
                "connector handshake cannot be repeated",
            ));
        }
        ConnectorRequestV1::Connect {
            connection_id,
            endpoint,
            database,
            credential,
        } => ConnectorRequestV2::Connect {
            connection_id: connection_id.clone(),
            endpoint: structured_endpoint(plugin_id, endpoint, database.clone())?,
            tls_mode: default_tls_mode(plugin_id),
            credential: Some(ordadb_connector_sdk::ConnectorCredentialV2::new(
                Some(credential.username.clone()),
                credential.password.to_string(),
            )),
        },
        ConnectorRequestV1::Catalog {
            request_id,
            connection_id,
        } => ConnectorRequestV2::Catalog {
            request_id: request_id.clone(),
            connection_id: connection_id.clone(),
        },
        ConnectorRequestV1::Execute {
            request_id,
            connection_id,
            sql,
            params,
        } => ConnectorRequestV2::Execute {
            request_id: request_id.clone(),
            connection_id: connection_id.clone(),
            sql: sql.clone(),
            params: params.iter().map(parameter_v2).collect(),
            batch_size: 1024,
        },
        ConnectorRequestV1::Cancel { request_id } => ConnectorRequestV2::Cancel {
            request_id: request_id.clone(),
        },
        ConnectorRequestV1::Begin {
            request_id,
            connection_id,
        } => ConnectorRequestV2::Begin {
            request_id: request_id.clone(),
            connection_id: connection_id.clone(),
            isolation: None,
        },
        ConnectorRequestV1::Commit {
            request_id,
            connection_id,
        } => ConnectorRequestV2::Commit {
            request_id: request_id.clone(),
            connection_id: connection_id.clone(),
        },
        ConnectorRequestV1::Rollback {
            request_id,
            connection_id,
        } => ConnectorRequestV2::Rollback {
            request_id: request_id.clone(),
            connection_id: connection_id.clone(),
        },
        ConnectorRequestV1::Monitor { .. } => return Ok(None),
        ConnectorRequestV1::Shutdown => ConnectorRequestV2::Shutdown,
    };
    Ok(Some(request))
}

fn translate_request_v3(
    request: &ConnectorRequestV1,
    plugin_id: &str,
    capabilities: &ConnectorCapabilitiesV3,
) -> Result<Option<ConnectorRequestV3>> {
    let language_id = capabilities
        .command_languages
        .first()
        .ok_or_else(|| DbError::new("08P01", "connector has no command language"))?
        .id
        .clone();
    let request = match request {
        ConnectorRequestV1::Hello { .. } => {
            return Err(DbError::new(
                "08P01",
                "connector handshake cannot be repeated",
            ));
        }
        ConnectorRequestV1::Connect {
            connection_id,
            endpoint,
            database,
            credential,
        } => ConnectorRequestV3::Connect {
            connection_id: connection_id.clone(),
            endpoint: structured_endpoint(plugin_id, endpoint, database.clone())?,
            tls_mode: default_tls_mode(plugin_id),
            credential: Some(ConnectorCredentialV2::new(
                Some(credential.username.clone()),
                credential.password.to_string(),
            )),
        },
        ConnectorRequestV1::Catalog {
            request_id,
            connection_id,
        } => ConnectorRequestV3::Catalog {
            request_id: request_id.clone(),
            connection_id: connection_id.clone(),
            parent_id: None,
            page_size: capabilities.maximum_catalog_page_size.min(1024),
            cursor: None,
        },
        ConnectorRequestV1::Execute {
            request_id,
            connection_id,
            sql,
            params,
        } => ConnectorRequestV3::Execute {
            request_id: request_id.clone(),
            connection_id: connection_id.clone(),
            command: ConnectorCommandV3::Text {
                language_id,
                text: sql.clone(),
                params: params.iter().map(parameter_v2).collect(),
            },
            batch_size: 1024_u32.min(capabilities.maximum_batch_rows),
        },
        ConnectorRequestV1::Cancel { request_id } => ConnectorRequestV3::Cancel {
            request_id: request_id.clone(),
        },
        ConnectorRequestV1::Begin {
            request_id,
            connection_id,
        } => ConnectorRequestV3::Begin {
            request_id: request_id.clone(),
            connection_id: connection_id.clone(),
            isolation: None,
        },
        ConnectorRequestV1::Commit {
            request_id,
            connection_id,
        } => ConnectorRequestV3::Commit {
            request_id: request_id.clone(),
            connection_id: connection_id.clone(),
        },
        ConnectorRequestV1::Rollback {
            request_id,
            connection_id,
        } => ConnectorRequestV3::Rollback {
            request_id: request_id.clone(),
            connection_id: connection_id.clone(),
        },
        ConnectorRequestV1::Monitor { .. } => return Ok(None),
        ConnectorRequestV1::Shutdown => ConnectorRequestV3::Shutdown,
    };
    Ok(Some(request))
}

fn ensure_legacy_sql_v3(capabilities: &ConnectorCapabilitiesV3) -> Result<()> {
    if capabilities.kind != ConnectorKindV3::Sql {
        return Err(DbError::unsupported(
            "legacy SQL execution for document or key/value connectors",
        )
        .with_hint("Use the native connector protocol v3 command API."));
    }
    Ok(())
}

fn validate_request_v3(
    request: &ConnectorRequestV3,
    capabilities: &ConnectorCapabilitiesV3,
) -> Result<()> {
    match request {
        ConnectorRequestV3::Hello { .. } => Err(DbError::new(
            "08P01",
            "connector handshake cannot be repeated",
        )),
        ConnectorRequestV3::Connect {
            connection_id,
            endpoint,
            tls_mode,
            ..
        } => {
            validate_v3_id(connection_id, "connection ID")?;
            validate_endpoint(endpoint)?;
            if !capabilities.tls_modes.contains(tls_mode) {
                return Err(DbError::unsupported("connector TLS mode"));
            }
            Ok(())
        }
        ConnectorRequestV3::Disconnect { connection_id } => {
            validate_v3_id(connection_id, "connection ID")
        }
        ConnectorRequestV3::Catalog {
            request_id,
            connection_id,
            parent_id,
            page_size,
            cursor,
        } => {
            validate_v3_id(request_id, "request ID")?;
            validate_v3_id(connection_id, "connection ID")?;
            validate_catalog_request_v3(
                parent_id.as_deref(),
                *page_size,
                cursor.as_deref(),
                capabilities,
            )
        }
        ConnectorRequestV3::Execute {
            request_id,
            connection_id,
            command,
            batch_size,
        } => {
            validate_v3_id(request_id, "request ID")?;
            validate_v3_id(connection_id, "connection ID")?;
            if *batch_size == 0 || *batch_size > capabilities.maximum_batch_rows {
                return Err(DbError::new(
                    "22023",
                    "connector batch size is outside its capability",
                ));
            }
            validate_command_v3(command, capabilities)
        }
        ConnectorRequestV3::Cancel { request_id } => {
            validate_v3_id(request_id, "request ID")?;
            if !capabilities.cancellation {
                return Err(DbError::unsupported("connector cancellation"));
            }
            Ok(())
        }
        ConnectorRequestV3::Begin {
            request_id,
            connection_id,
            ..
        }
        | ConnectorRequestV3::Commit {
            request_id,
            connection_id,
        }
        | ConnectorRequestV3::Rollback {
            request_id,
            connection_id,
        } => {
            validate_v3_id(request_id, "request ID")?;
            validate_v3_id(connection_id, "connection ID")?;
            if !capabilities.transactions {
                return Err(DbError::unsupported("connector transactions"));
            }
            Ok(())
        }
        ConnectorRequestV3::Shutdown => Ok(()),
    }
}

fn prepare_v3_request(
    request: &ConnectorRequestV3,
    pending: &BTreeMap<String, PendingV3Request>,
) -> Result<Option<(String, PendingV3Request)>> {
    let prepared = match request {
        ConnectorRequestV3::Catalog {
            request_id,
            page_size,
            ..
        } => Some((
            request_id.clone(),
            PendingV3Request::Catalog {
                maximum_nodes: *page_size,
            },
        )),
        ConnectorRequestV3::Execute {
            request_id,
            batch_size,
            ..
        } => Some((
            request_id.clone(),
            PendingV3Request::Execute {
                maximum_batch_rows: *batch_size,
            },
        )),
        ConnectorRequestV3::Begin { request_id, .. }
        | ConnectorRequestV3::Commit { request_id, .. }
        | ConnectorRequestV3::Rollback { request_id, .. } => {
            Some((request_id.clone(), PendingV3Request::Transaction))
        }
        ConnectorRequestV3::Cancel { request_id } => {
            if !matches!(
                pending.get(request_id),
                Some(PendingV3Request::Execute { .. })
            ) {
                return Err(DbError::new(
                    "42704",
                    "connector execution request does not exist",
                ));
            }
            None
        }
        ConnectorRequestV3::Hello { .. }
        | ConnectorRequestV3::Connect { .. }
        | ConnectorRequestV3::Disconnect { .. }
        | ConnectorRequestV3::Shutdown => None,
    };
    if let Some((request_id, _)) = &prepared {
        if pending.contains_key(request_id) {
            return Err(DbError::new("42P04", "connector request already exists"));
        }
        if pending.len() >= MAX_HOST_ACTIVE_V3_REQUESTS {
            return Err(DbError::new(
                "54000",
                "connector has too many active v3 requests",
            ));
        }
    }
    Ok(prepared)
}

fn validate_v3_response(
    response: &ConnectorResponseV3,
    capabilities: &ConnectorCapabilitiesV3,
    pending: &mut BTreeMap<String, PendingV3Request>,
    validators: &mut BTreeMap<String, ConnectorResultStreamValidatorV3>,
) -> Result<()> {
    match response {
        ConnectorResponseV3::Ready { .. } => Err(handshake_response_error()),
        ConnectorResponseV3::Connected {
            capabilities: actual,
            ..
        } if actual != capabilities => Err(DbError::new(
            "08P01",
            "connector session capabilities differ from the handshake",
        )),
        ConnectorResponseV3::CatalogPage { request_id, page } => {
            let Some(PendingV3Request::Catalog { maximum_nodes }) =
                pending.get(request_id).copied()
            else {
                return Err(unexpected_v3_request_id(request_id, "Catalog"));
            };
            validate_catalog_page_v3(page, maximum_nodes)?;
            pending.remove(request_id);
            Ok(())
        }
        ConnectorResponseV3::ResultEvent { request_id, event } => {
            let Some(PendingV3Request::Execute { maximum_batch_rows }) =
                pending.get(request_id).copied()
            else {
                return Err(unexpected_v3_request_id(request_id, "result"));
            };
            let terminal = matches!(event, ConnectorResultEventV3::Complete { .. });
            validators
                .entry(request_id.clone())
                .or_insert_with(|| {
                    ConnectorResultStreamValidatorV3::new(capabilities.kind, maximum_batch_rows)
                })
                .validate(event)?;
            if terminal {
                validators.remove(request_id);
                pending.remove(request_id);
            }
            Ok(())
        }
        ConnectorResponseV3::Cancelled { request_id } => {
            if !matches!(
                pending.get(request_id),
                Some(PendingV3Request::Execute { .. })
            ) {
                return Err(unexpected_v3_request_id(request_id, "cancellation"));
            }
            validators.remove(request_id);
            pending.remove(request_id);
            Ok(())
        }
        ConnectorResponseV3::Transaction { request_id, .. } => {
            if pending.get(request_id) != Some(&PendingV3Request::Transaction) {
                return Err(unexpected_v3_request_id(request_id, "transaction"));
            }
            pending.remove(request_id);
            Ok(())
        }
        ConnectorResponseV3::Error { request_id, error } => {
            validate_error_v3(error, capabilities.kind)?;
            if let Some(request_id) = request_id {
                if !pending.contains_key(request_id) {
                    return Err(unexpected_v3_request_id(request_id, "error"));
                }
                validators.remove(request_id);
                pending.remove(request_id);
            }
            Ok(())
        }
        ConnectorResponseV3::Connected { .. }
        | ConnectorResponseV3::Disconnected { .. }
        | ConnectorResponseV3::Shutdown => Ok(()),
    }
}

fn unexpected_v3_request_id(request_id: &str, response_kind: &str) -> DbError {
    DbError::new(
        "08P01",
        format!("connector returned {response_kind} for unknown v3 request {request_id}"),
    )
}

fn validate_v3_id(value: &str, context: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value.chars().any(|character| {
            !character.is_ascii_alphanumeric() && !matches!(character, '-' | '_' | '.')
        })
    {
        return Err(DbError::new(
            "22023",
            format!("connector {context} is invalid"),
        ));
    }
    Ok(())
}

fn translate_response_v2(
    response: ConnectorResponseV2,
    query_schemas: &mut BTreeMap<String, Schema>,
) -> Result<ConnectorResponseV1> {
    match response {
        ConnectorResponseV2::Ready { .. } => Err(handshake_response_error()),
        ConnectorResponseV2::Connected { connection_id, .. } => {
            Ok(ConnectorResponseV1::Connected { connection_id })
        }
        ConnectorResponseV2::Disconnected { connection_id } => Ok(ConnectorResponseV1::Error {
            request_id: None,
            error: DbError::new(
                "08003",
                format!("connector connection {connection_id} was closed"),
            ),
        }),
        ConnectorResponseV2::Catalog {
            request_id,
            objects,
        } => Ok(ConnectorResponseV1::Catalog {
            request_id,
            entries: objects
                .into_iter()
                .map(|object| CatalogEntry {
                    kind: catalog_kind(object.kind).into(),
                    schema: object.schema.unwrap_or_default(),
                    name: object.name,
                })
                .collect(),
        }),
        ConnectorResponseV2::QueryEvent { request_id, event } => {
            let event = query_event_v1(&request_id, event, query_schemas)?;
            Ok(ConnectorResponseV1::QueryEvent { request_id, event })
        }
        ConnectorResponseV2::Cancelled { request_id } => Ok(ConnectorResponseV1::Error {
            request_id: Some(request_id),
            error: DbError::new("57014", "connector query was cancelled"),
        }),
        ConnectorResponseV2::Transaction { request_id, state } => {
            Ok(ConnectorResponseV1::QueryEvent {
                request_id,
                event: QueryEvent::Complete(CommandComplete {
                    tag: format!("{state:?}").to_ascii_uppercase(),
                    rows_affected: 0,
                }),
            })
        }
        ConnectorResponseV2::Error { request_id, error } => Ok(ConnectorResponseV1::Error {
            request_id,
            error: error.into_db_error(),
        }),
        ConnectorResponseV2::Shutdown => Ok(ConnectorResponseV1::Shutdown),
    }
}

fn translate_response_v3(
    response: ConnectorResponseV3,
    query_schemas: &mut BTreeMap<String, Schema>,
) -> Result<ConnectorResponseV1> {
    match response {
        ConnectorResponseV3::Ready { .. } => Err(handshake_response_error()),
        ConnectorResponseV3::Connected { connection_id, .. } => {
            Ok(ConnectorResponseV1::Connected { connection_id })
        }
        ConnectorResponseV3::Disconnected { connection_id } => Ok(ConnectorResponseV1::Error {
            request_id: None,
            error: DbError::new(
                "08003",
                format!("connector connection {connection_id} was closed"),
            ),
        }),
        ConnectorResponseV3::CatalogPage { request_id, page } => Ok(ConnectorResponseV1::Catalog {
            request_id,
            entries: page
                .nodes
                .into_iter()
                .map(|node| CatalogEntry {
                    kind: catalog_kind_v3(node.kind).into(),
                    schema: node.namespace.unwrap_or_default(),
                    name: node.name,
                })
                .collect(),
        }),
        ConnectorResponseV3::ResultEvent { request_id, event } => {
            let event = query_event_v3(&request_id, event, query_schemas)?;
            Ok(ConnectorResponseV1::QueryEvent { request_id, event })
        }
        ConnectorResponseV3::Cancelled { request_id } => Ok(ConnectorResponseV1::Error {
            request_id: Some(request_id),
            error: DbError::new("57014", "connector query was cancelled"),
        }),
        ConnectorResponseV3::Transaction { request_id, state } => {
            Ok(ConnectorResponseV1::QueryEvent {
                request_id,
                event: QueryEvent::Complete(CommandComplete {
                    tag: format!("{state:?}").to_ascii_uppercase(),
                    rows_affected: 0,
                }),
            })
        }
        ConnectorResponseV3::Error { request_id, error } => Ok(ConnectorResponseV1::Error {
            request_id,
            error: error.into_db_error(),
        }),
        ConnectorResponseV3::Shutdown => Ok(ConnectorResponseV1::Shutdown),
    }
}

fn query_event_v3(
    request_id: &str,
    event: ConnectorResultEventV3,
    query_schemas: &mut BTreeMap<String, Schema>,
) -> Result<QueryEvent> {
    match event {
        ConnectorResultEventV3::Schema { columns } => {
            let schema = Schema::new(
                columns
                    .into_iter()
                    .map(|column| {
                        Field::new(column.name, scalar_type(&column.data_type), column.nullable)
                    })
                    .collect(),
            );
            query_schemas.insert(request_id.to_owned(), schema.clone());
            Ok(QueryEvent::Schema(schema))
        }
        ConnectorResultEventV3::Batch {
            batch: ConnectorResultBatchV3::Rows { rows },
        } => {
            let schema = query_schemas.get(request_id).cloned().ok_or_else(|| {
                DbError::new(
                    "08P01",
                    "connector sent a row batch before the query schema",
                )
            })?;
            let rows = rows
                .into_iter()
                .map(|values| {
                    values
                        .into_iter()
                        .map(value_v1)
                        .collect::<Result<Vec<_>>>()
                        .map(Row::new)
                })
                .collect::<Result<Vec<_>>>()?;
            if rows
                .iter()
                .any(|row| row.values.len() != schema.fields.len())
            {
                return Err(DbError::new(
                    "08P01",
                    "connector row width does not match the query schema",
                ));
            }
            Ok(QueryEvent::Batch(Batch { schema, rows }))
        }
        ConnectorResultEventV3::Batch {
            batch:
                ConnectorResultBatchV3::Documents { .. } | ConnectorResultBatchV3::KeyValues { .. },
        } => Err(DbError::unsupported(
            "document or key/value results through the legacy SQL event adapter",
        )),
        ConnectorResultEventV3::Progress { items_processed } => {
            Ok(QueryEvent::Progress(QueryProgress {
                rows_processed: items_processed,
            }))
        }
        ConnectorResultEventV3::Notice { notice } => Ok(QueryEvent::Notice(DbNotice {
            severity: ordadb_types::DbNoticeSeverity::Notice,
            sql_state: notice.code.unwrap_or_else(|| "00000".into()),
            message: notice.message,
            detail: None,
            hint: None,
            position: None,
            object_identity: None,
        })),
        ConnectorResultEventV3::Complete {
            command_tag,
            affected_items,
        } => {
            query_schemas.remove(request_id);
            Ok(QueryEvent::Complete(CommandComplete {
                tag: command_tag,
                rows_affected: affected_items.unwrap_or(0),
            }))
        }
    }
}

fn query_event_v1(
    request_id: &str,
    event: ConnectorQueryEventV2,
    query_schemas: &mut BTreeMap<String, Schema>,
) -> Result<QueryEvent> {
    match event {
        ConnectorQueryEventV2::Schema { columns } => {
            let schema = Schema::new(
                columns
                    .into_iter()
                    .map(|column| {
                        Field::new(column.name, scalar_type(&column.data_type), column.nullable)
                    })
                    .collect(),
            );
            query_schemas.insert(request_id.to_owned(), schema.clone());
            Ok(QueryEvent::Schema(schema))
        }
        ConnectorQueryEventV2::Batch { batch } => {
            let schema = query_schemas.get(request_id).cloned().ok_or_else(|| {
                DbError::new(
                    "08P01",
                    "connector sent a row batch before the query schema",
                )
            })?;
            let rows = batch
                .rows
                .into_iter()
                .map(|values| {
                    values
                        .into_iter()
                        .map(value_v1)
                        .collect::<Result<Vec<_>>>()
                        .map(Row::new)
                })
                .collect::<Result<Vec<_>>>()?;
            if rows
                .iter()
                .any(|row| row.values.len() != schema.fields.len())
            {
                return Err(DbError::new(
                    "08P01",
                    "connector row width does not match the query schema",
                ));
            }
            Ok(QueryEvent::Batch(Batch { schema, rows }))
        }
        ConnectorQueryEventV2::Progress { rows_processed } => {
            Ok(QueryEvent::Progress(QueryProgress { rows_processed }))
        }
        ConnectorQueryEventV2::Notice { notice } => Ok(QueryEvent::Notice(DbNotice {
            severity: ordadb_types::DbNoticeSeverity::Notice,
            sql_state: notice.code.unwrap_or_else(|| "00000".into()),
            message: notice.message,
            detail: None,
            hint: None,
            position: None,
            object_identity: None,
        })),
        ConnectorQueryEventV2::Complete {
            command_tag,
            affected_rows,
        } => {
            query_schemas.remove(request_id);
            Ok(QueryEvent::Complete(CommandComplete {
                tag: command_tag,
                rows_affected: affected_rows.unwrap_or(0),
            }))
        }
    }
}

fn structured_endpoint(
    plugin_id: &str,
    endpoint: &str,
    database: Option<String>,
) -> Result<ConnectorEndpointV2> {
    if matches!(plugin_id, "sqlite" | "ordadb-sqlite") {
        return Ok(ConnectorEndpointV2::File {
            path: endpoint.to_owned(),
            read_only: false,
            create: true,
            options: BTreeMap::new(),
        });
    }
    let default_port = match plugin_id {
        "mysql" | "ordadb-mysql" => 3306,
        "sql-server" | "ordadb-sql-server" => 1433,
        _ => 5432,
    };
    let (host_and_instance, port) = split_host_port(endpoint, default_port)?;
    let (host, instance) = host_and_instance
        .split_once('\\')
        .map_or((host_and_instance.as_str(), None), |(host, instance)| {
            (host, Some(instance.to_owned()))
        });
    if host.trim().is_empty() {
        return Err(DbError::new("22023", "connector endpoint host is empty"));
    }
    Ok(ConnectorEndpointV2::Network {
        host: host.to_owned(),
        port,
        database,
        instance,
        options: BTreeMap::new(),
    })
}

fn split_host_port(endpoint: &str, default_port: u16) -> Result<(String, u16)> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() || endpoint.chars().any(char::is_control) {
        return Err(DbError::new("22023", "connector endpoint is invalid"));
    }
    if let Some(rest) = endpoint.strip_prefix('[') {
        let (host, suffix) = rest
            .split_once(']')
            .ok_or_else(|| DbError::new("22023", "connector IPv6 endpoint is invalid"))?;
        let port = suffix
            .strip_prefix(':')
            .map(str::parse)
            .transpose()
            .map_err(|_| DbError::new("22023", "connector endpoint port is invalid"))?
            .unwrap_or(default_port);
        return Ok((host.to_owned(), port));
    }
    if let Some((host, port)) = endpoint.rsplit_once(':')
        && !host.contains(':')
        && let Ok(port) = port.parse::<u16>()
    {
        if port == 0 {
            return Err(DbError::new(
                "22023",
                "connector endpoint port must be positive",
            ));
        }
        return Ok((host.to_owned(), port));
    }
    if endpoint.contains(':') {
        return Err(DbError::new(
            "22023",
            "IPv6 connector endpoints must use brackets",
        ));
    }
    Ok((endpoint.to_owned(), default_port))
}

fn default_tls_mode(plugin_id: &str) -> ConnectorTlsModeV2 {
    if matches!(plugin_id, "sqlite" | "ordadb-sqlite") {
        ConnectorTlsModeV2::Disable
    } else {
        ConnectorTlsModeV2::Prefer
    }
}

fn parameter_v2(value: &Value) -> ConnectorParameterV2 {
    ConnectorParameterV2 {
        data_type: value.scalar_type().map(|scalar| connector_type(&scalar)),
        value: value_v2(value),
    }
}

fn value_v2(value: &Value) -> ConnectorValueV2 {
    match value {
        Value::Null => ConnectorValueV2::Null,
        Value::Boolean(value) => ConnectorValueV2::Boolean(*value),
        Value::Int16(value) => ConnectorValueV2::SignedInteger(i64::from(*value)),
        Value::Int32(value) => ConnectorValueV2::SignedInteger(i64::from(*value)),
        Value::Int64(value) => ConnectorValueV2::SignedInteger(*value),
        Value::Float32(value) => ConnectorValueV2::FloatingPoint(f64::from(*value)),
        Value::Float64(value) => ConnectorValueV2::FloatingPoint(*value),
        Value::Decimal(value) => ConnectorValueV2::Decimal(value.to_string()),
        Value::Text(value) => ConnectorValueV2::Text(value.clone()),
        Value::Binary(value) => ConnectorValueV2::Binary(BASE64.encode(value)),
        Value::Date(value) => ConnectorValueV2::Date(value.to_string()),
        Value::Time(value) => ConnectorValueV2::Time(value.to_string()),
        Value::Timestamp(value) => ConnectorValueV2::Timestamp(value.to_string()),
        Value::Interval(value) => ConnectorValueV2::Interval(value.to_string()),
        Value::Array(array) => {
            ConnectorValueV2::Array(array.values().iter().map(value_v2).collect())
        }
        Value::Json(value) | Value::Jsonb(value) => ConnectorValueV2::Json(value.clone()),
        Value::Uuid(value) => ConnectorValueV2::Uuid(value.to_string()),
        Value::Vector(values) => ConnectorValueV2::Array(
            values
                .iter()
                .map(|value| ConnectorValueV2::FloatingPoint(f64::from(*value)))
                .collect(),
        ),
    }
}

fn value_v1(value: ConnectorValueV2) -> Result<Value> {
    match value {
        ConnectorValueV2::Null => Ok(Value::Null),
        ConnectorValueV2::Boolean(value) => Ok(Value::Boolean(value)),
        ConnectorValueV2::SignedInteger(value) => Ok(Value::Int64(value)),
        ConnectorValueV2::UnsignedInteger(value) => i64::try_from(value)
            .map(Value::Int64)
            .map_err(|_| DbError::new("22003", "connector unsigned integer exceeds int64")),
        ConnectorValueV2::FloatingPoint(value) => Ok(Value::Float64(value)),
        ConnectorValueV2::Decimal(value) => {
            Decimal::from_str(&value)
                .map(Value::Decimal)
                .map_err(|error| {
                    DbError::new("22P02", "connector decimal value is invalid")
                        .with_detail(error.to_string())
                })
        }
        ConnectorValueV2::Text(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::TimestampWithTimeZone(value) => Ok(Value::Text(value)),
        ConnectorValueV2::Binary(value) => BASE64
            .decode(value)
            .map(Value::Binary)
            .map_err(|_| DbError::new("22P02", "connector binary value is not valid base64")),
        ConnectorValueV2::Date(value) => NaiveDate::parse_from_str(&value, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|error| temporal_error("date", error)),
        ConnectorValueV2::Time(value) => NaiveTime::parse_from_str(&value, "%H:%M:%S%.f")
            .map(Value::Time)
            .map_err(|error| temporal_error("time", error)),
        ConnectorValueV2::Timestamp(value) => parse_timestamp(&value).map(Value::Timestamp),
        ConnectorValueV2::Uuid(value) => Uuid::parse_str(&value)
            .map(Value::Uuid)
            .map_err(|error| temporal_error("UUID", error)),
        ConnectorValueV2::Json(value) => Ok(Value::Jsonb(value)),
        ConnectorValueV2::Array(values) => Ok(Value::Jsonb(serde_json::Value::Array(
            values
                .into_iter()
                .map(connector_value_json)
                .collect::<Result<Vec<_>>>()?,
        ))),
    }
}

fn connector_value_json(value: ConnectorValueV2) -> Result<serde_json::Value> {
    match value {
        ConnectorValueV2::Null => Ok(serde_json::Value::Null),
        ConnectorValueV2::Boolean(value) => Ok(value.into()),
        ConnectorValueV2::SignedInteger(value) => Ok(value.into()),
        ConnectorValueV2::UnsignedInteger(value) => Ok(value.into()),
        ConnectorValueV2::FloatingPoint(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| DbError::new("22003", "connector floating-point value is not finite")),
        ConnectorValueV2::Json(value) => Ok(value),
        ConnectorValueV2::Array(values) => Ok(serde_json::Value::Array(
            values
                .into_iter()
                .map(connector_value_json)
                .collect::<Result<Vec<_>>>()?,
        )),
        ConnectorValueV2::Binary(value) => Ok(value.into()),
        ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Text(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::Uuid(value) => Ok(value.into()),
    }
}

fn parse_timestamp(value: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.naive_utc()))
        .map_err(|error| temporal_error("timestamp", error))
}

fn temporal_error(name: &str, error: impl std::fmt::Display) -> DbError {
    DbError::new("22007", format!("connector {name} value is invalid"))
        .with_detail(error.to_string())
}

fn connector_type(data_type: &ScalarType) -> ConnectorTypeV2 {
    let element_type = match data_type {
        ScalarType::Array { element } => Some(Box::new(connector_type(element))),
        _ => None,
    };
    let (vendor_name, logical_type, precision, scale, length) = match data_type {
        ScalarType::Boolean => ("boolean", ConnectorLogicalTypeV2::Boolean, None, None, None),
        ScalarType::Int16 => (
            "smallint",
            ConnectorLogicalTypeV2::SignedInteger,
            None,
            None,
            None,
        ),
        ScalarType::Int32 => (
            "integer",
            ConnectorLogicalTypeV2::SignedInteger,
            None,
            None,
            None,
        ),
        ScalarType::Int64 => (
            "bigint",
            ConnectorLogicalTypeV2::SignedInteger,
            None,
            None,
            None,
        ),
        ScalarType::Oid => (
            "oid",
            ConnectorLogicalTypeV2::UnsignedInteger,
            None,
            None,
            None,
        ),
        ScalarType::Name => ("name", ConnectorLogicalTypeV2::Text, None, None, Some(63)),
        ScalarType::InternalChar => ("char", ConnectorLogicalTypeV2::Text, None, None, Some(1)),
        ScalarType::Float32 => (
            "real",
            ConnectorLogicalTypeV2::FloatingPoint,
            None,
            None,
            None,
        ),
        ScalarType::Float64 => (
            "double precision",
            ConnectorLogicalTypeV2::FloatingPoint,
            None,
            None,
            None,
        ),
        ScalarType::Decimal { precision, scale } => (
            "numeric",
            ConnectorLogicalTypeV2::Decimal,
            precision.map(u32::from),
            scale.map(u32::from),
            None,
        ),
        ScalarType::Char { length } => (
            "char",
            ConnectorLogicalTypeV2::Text,
            None,
            None,
            length.map(u64::from),
        ),
        ScalarType::Varchar { length } => (
            "varchar",
            ConnectorLogicalTypeV2::Text,
            None,
            None,
            length.map(u64::from),
        ),
        ScalarType::Enum { .. } => ("enum", ConnectorLogicalTypeV2::Text, None, None, None),
        ScalarType::Text => ("text", ConnectorLogicalTypeV2::Text, None, None, None),
        ScalarType::Binary => ("bytea", ConnectorLogicalTypeV2::Binary, None, None, None),
        ScalarType::Date => ("date", ConnectorLogicalTypeV2::Date, None, None, None),
        ScalarType::Time => ("time", ConnectorLogicalTypeV2::Time, None, None, None),
        ScalarType::Interval => (
            "interval",
            ConnectorLogicalTypeV2::Interval,
            None,
            None,
            None,
        ),
        ScalarType::Timestamp {
            with_timezone: true,
        } => (
            "timestamptz",
            ConnectorLogicalTypeV2::TimestampWithTimeZone,
            None,
            None,
            None,
        ),
        ScalarType::Timestamp {
            with_timezone: false,
        } => (
            "timestamp",
            ConnectorLogicalTypeV2::Timestamp,
            None,
            None,
            None,
        ),
        ScalarType::Json => ("json", ConnectorLogicalTypeV2::Json, None, None, None),
        ScalarType::Jsonb => ("jsonb", ConnectorLogicalTypeV2::Json, None, None, None),
        ScalarType::Uuid => ("uuid", ConnectorLogicalTypeV2::Uuid, None, None, None),
        ScalarType::Array { .. } => ("array", ConnectorLogicalTypeV2::Array, None, None, None),
        ScalarType::Vector { dimensions } => (
            "vector",
            ConnectorLogicalTypeV2::Array,
            None,
            None,
            dimensions.and_then(|value| u64::try_from(value).ok()),
        ),
    };
    ConnectorTypeV2 {
        vendor_name: vendor_name.into(),
        logical_type,
        element_type,
        precision,
        scale,
        length,
    }
}

fn scalar_type(data_type: &ConnectorTypeV2) -> ScalarType {
    match data_type.logical_type {
        ConnectorLogicalTypeV2::Boolean => ScalarType::Boolean,
        ConnectorLogicalTypeV2::SignedInteger | ConnectorLogicalTypeV2::UnsignedInteger => {
            ScalarType::Int64
        }
        ConnectorLogicalTypeV2::FloatingPoint => ScalarType::Float64,
        ConnectorLogicalTypeV2::Decimal => ScalarType::Decimal {
            precision: data_type
                .precision
                .and_then(|precision| u8::try_from(precision).ok()),
            scale: data_type.scale.and_then(|scale| u8::try_from(scale).ok()),
        },
        ConnectorLogicalTypeV2::Binary => ScalarType::Binary,
        ConnectorLogicalTypeV2::Date => ScalarType::Date,
        ConnectorLogicalTypeV2::Time => ScalarType::Time,
        ConnectorLogicalTypeV2::Timestamp => ScalarType::Timestamp {
            with_timezone: false,
        },
        ConnectorLogicalTypeV2::TimestampWithTimeZone => ScalarType::Timestamp {
            with_timezone: true,
        },
        ConnectorLogicalTypeV2::Uuid => ScalarType::Uuid,
        ConnectorLogicalTypeV2::Json | ConnectorLogicalTypeV2::Array => ScalarType::Jsonb,
        ConnectorLogicalTypeV2::Null
        | ConnectorLogicalTypeV2::Text
        | ConnectorLogicalTypeV2::Interval
        | ConnectorLogicalTypeV2::Other => ScalarType::Text,
    }
}

const fn catalog_kind(kind: ConnectorCatalogObjectKindV2) -> &'static str {
    match kind {
        ConnectorCatalogObjectKindV2::Database => "database",
        ConnectorCatalogObjectKindV2::Schema => "schema",
        ConnectorCatalogObjectKindV2::Table => "table",
        ConnectorCatalogObjectKindV2::View => "view",
        ConnectorCatalogObjectKindV2::MaterializedView => "materializedView",
        ConnectorCatalogObjectKindV2::Column => "column",
        ConnectorCatalogObjectKindV2::Index => "index",
        ConnectorCatalogObjectKindV2::Constraint => "constraint",
        ConnectorCatalogObjectKindV2::Sequence => "sequence",
        ConnectorCatalogObjectKindV2::Function => "function",
        ConnectorCatalogObjectKindV2::Procedure => "procedure",
    }
}

const fn catalog_kind_v3(kind: ConnectorCatalogNodeKindV3) -> &'static str {
    match kind {
        ConnectorCatalogNodeKindV3::Server => "server",
        ConnectorCatalogNodeKindV3::Cluster => "cluster",
        ConnectorCatalogNodeKindV3::Database => "database",
        ConnectorCatalogNodeKindV3::Schema => "schema",
        ConnectorCatalogNodeKindV3::Table => "table",
        ConnectorCatalogNodeKindV3::View => "view",
        ConnectorCatalogNodeKindV3::MaterializedView => "materializedView",
        ConnectorCatalogNodeKindV3::Column => "column",
        ConnectorCatalogNodeKindV3::Index => "index",
        ConnectorCatalogNodeKindV3::Constraint => "constraint",
        ConnectorCatalogNodeKindV3::Sequence => "sequence",
        ConnectorCatalogNodeKindV3::Function => "function",
        ConnectorCatalogNodeKindV3::Procedure => "procedure",
        ConnectorCatalogNodeKindV3::Collection => "collection",
        ConnectorCatalogNodeKindV3::Keyspace => "keyspace",
        ConnectorCatalogNodeKindV3::Key => "key",
        ConnectorCatalogNodeKindV3::Stream => "stream",
        ConnectorCatalogNodeKindV3::Other => "other",
    }
}

fn request_id(request: &ConnectorRequestV1) -> Option<&str> {
    match request {
        ConnectorRequestV1::Catalog { request_id, .. }
        | ConnectorRequestV1::Execute { request_id, .. }
        | ConnectorRequestV1::Cancel { request_id }
        | ConnectorRequestV1::Begin { request_id, .. }
        | ConnectorRequestV1::Commit { request_id, .. }
        | ConnectorRequestV1::Rollback { request_id, .. }
        | ConnectorRequestV1::Monitor { request_id, .. } => Some(request_id),
        ConnectorRequestV1::Hello { .. }
        | ConnectorRequestV1::Connect { .. }
        | ConnectorRequestV1::Shutdown => None,
    }
}

fn handshake_timeout() -> DbError {
    network_error(
        "connector handshake timed out",
        "no protocol response before the deadline",
    )
}

fn handshake_response_error() -> DbError {
    DbError::new("08P01", "connector did not begin with a Ready response")
}

fn connector_pipe_name() -> String {
    format!(r"\\.\pipe\ordadb-connector-{}", Uuid::new_v4())
}

fn configure_hidden_process(command: &mut Command) {
    use std::os::windows::process::CommandExt as _;
    use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

    command.as_std_mut().creation_flags(CREATE_NO_WINDOW);
}

fn helper_exit_error(status: std::io::Result<ExitStatus>) -> DbError {
    match status {
        Ok(status) => network_error(
            "connector helper process exited before completing the protocol",
            format!("exit status {status}"),
        ),
        Err(error) => io_error("failed to wait for connector helper process", error),
    }
}

#[cfg(test)]
mod tests {
    use ordadb_connector_sdk::{
        ConnectorCapabilitiesV2, ConnectorCapabilitiesV3, ConnectorCatalogNodeV3,
        ConnectorCatalogPageV3, ConnectorColumnV2, ConnectorCommandInputModeV3,
        ConnectorCommandLanguageV3, ConnectorErrorV2, ConnectorKindV3, ConnectorLogicalTypeV2,
        ConnectorResponseV2, ConnectorResponseV3, ConnectorTlsModeV2, ConnectorTypeV2,
        ProtocolReadyV2, ProtocolReadyV3,
    };
    use ordadb_types::{PgArray, PgInterval, TypeId};
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

    use super::*;
    use crate::ProtocolReady;

    fn capabilities_v3(kind: ConnectorKindV3) -> ConnectorCapabilitiesV3 {
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
            maximum_batch_rows: 1024,
            maximum_catalog_page_size: 256,
            tls_modes: vec![ConnectorTlsModeV2::Disable, ConnectorTlsModeV2::Require],
        }
    }

    #[tokio::test]
    async fn restricted_pipe_negotiates_protocol_v1_with_current_process_client() {
        let pipe_name = connector_pipe_name();
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true);
        let mut server = options.create(&pipe_name).expect("server pipe");
        ordadb_windows::restrict_named_pipe_acl(&server).expect("pipe ACL");
        let client = tokio::spawn({
            let pipe_name = pipe_name.clone();
            async move {
                let mut client = ClientOptions::new().open(&pipe_name).expect("client pipe");
                let hello: ConnectorRequestV1 =
                    read_connector_frame(&mut client).await.expect("hello");
                let ConnectorRequestV1::Hello {
                    api_version,
                    plugin_id,
                    plugin_version,
                } = hello
                else {
                    panic!("expected hello");
                };
                assert_eq!(api_version, MIN_CONNECTOR_API_VERSION);
                write_connector_frame(
                    &mut client,
                    &ConnectorResponseV1::Ready(ProtocolReady {
                        api_version,
                        plugin_id,
                        plugin_version,
                    }),
                )
                .await
                .expect("ready");
            }
        });
        server.connect().await.expect("connect");
        assert_eq!(
            negotiate(
                &mut server,
                "ordadb-postgresql",
                "1.0.0",
                MIN_CONNECTOR_API_VERSION,
            )
            .await
            .expect("negotiate"),
            NegotiatedProtocol::V1
        );
        client.await.expect("client task");
    }

    #[tokio::test]
    async fn restricted_pipe_negotiates_protocol_v2_with_current_process_client() {
        let pipe_name = connector_pipe_name();
        let mut server = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true)
            .create(&pipe_name)
            .expect("server pipe");
        ordadb_windows::restrict_named_pipe_acl(&server).expect("pipe ACL");
        let client = tokio::spawn({
            let pipe_name = pipe_name.clone();
            async move {
                let mut client = ClientOptions::new().open(&pipe_name).expect("client pipe");
                let hello: ConnectorRequestV2 =
                    read_connector_frame_v2(&mut client).await.expect("hello");
                let ConnectorRequestV2::Hello { hello } = hello else {
                    panic!("expected hello");
                };
                write_connector_frame_v2(
                    &mut client,
                    &ConnectorResponseV2::Ready {
                        ready: ProtocolReadyV2 {
                            api_version: CONNECTOR_PROTOCOL_V2,
                            plugin_id: hello.plugin_id,
                            plugin_version: hello.plugin_version,
                            capabilities: ConnectorCapabilitiesV2 {
                                catalog: true,
                                cancellation: true,
                                transactions: true,
                                savepoints: false,
                                batch_query: true,
                                maximum_batch_rows: 1024,
                                tls_modes: vec![
                                    ConnectorTlsModeV2::Disable,
                                    ConnectorTlsModeV2::Require,
                                ],
                            },
                        },
                    },
                )
                .await
                .expect("ready");
            }
        });
        server.connect().await.expect("connect");
        assert_eq!(
            negotiate(&mut server, "postgresql", "1.0.0", CONNECTOR_PROTOCOL_V2)
                .await
                .expect("negotiate"),
            NegotiatedProtocol::V2
        );
        client.await.expect("client task");
    }

    #[tokio::test]
    async fn restricted_pipe_negotiates_protocol_v3_with_current_process_client() {
        let pipe_name = connector_pipe_name();
        let mut server = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true)
            .create(&pipe_name)
            .expect("server pipe");
        ordadb_windows::restrict_named_pipe_acl(&server).expect("pipe ACL");
        let expected = capabilities_v3(ConnectorKindV3::Sql);
        let client = tokio::spawn({
            let pipe_name = pipe_name.clone();
            let expected = expected.clone();
            async move {
                let mut client = ClientOptions::new().open(&pipe_name).expect("client pipe");
                let hello: ConnectorRequestV3 =
                    read_connector_frame_v3(&mut client).await.expect("hello");
                let ConnectorRequestV3::Hello { hello } = hello else {
                    panic!("expected hello");
                };
                write_connector_frame_v3(
                    &mut client,
                    &ConnectorResponseV3::Ready {
                        ready: ProtocolReadyV3 {
                            api_version: CONNECTOR_PROTOCOL_V3,
                            plugin_id: hello.plugin_id,
                            plugin_version: hello.plugin_version,
                            capabilities: expected,
                        },
                    },
                )
                .await
                .expect("ready");
            }
        });
        server.connect().await.expect("connect");
        assert_eq!(
            negotiate(&mut server, "postgresql-v3", "1.0.0", CONNECTOR_PROTOCOL_V3)
                .await
                .expect("negotiate"),
            NegotiatedProtocol::V3(expected)
        );
        client.await.expect("client task");
    }

    #[test]
    fn v3_legacy_adapter_accepts_sql_and_rejects_non_sql() {
        let sql = capabilities_v3(ConnectorKindV3::Sql);
        let translated = translate_request_v3(
            &ConnectorRequestV1::Execute {
                request_id: "request-1".into(),
                connection_id: "connection-1".into(),
                sql: "SELECT 1".into(),
                params: Vec::new(),
            },
            "postgresql-v3",
            &sql,
        )
        .expect("translate")
        .expect("supported request");
        assert!(matches!(
            translated,
            ConnectorRequestV3::Execute {
                command: ConnectorCommandV3::Text { text, .. },
                ..
            } if text == "SELECT 1"
        ));

        let document = capabilities_v3(ConnectorKindV3::Document);
        assert_eq!(
            ensure_legacy_sql_v3(&document)
                .expect_err("document must use native v3")
                .sql_state,
            "0A000"
        );
    }

    #[test]
    fn v3_host_state_binds_response_ids_and_request_limits() {
        let capabilities = capabilities_v3(ConnectorKindV3::Sql);
        let mut pending = BTreeMap::new();
        let mut validators = BTreeMap::new();
        let catalog_request = ConnectorRequestV3::Catalog {
            request_id: "catalog-1".into(),
            connection_id: "connection-1".into(),
            parent_id: None,
            page_size: 1,
            cursor: None,
        };
        let (request_id, request) = prepare_v3_request(&catalog_request, &pending)
            .expect("prepare Catalog")
            .expect("tracked Catalog");
        pending.insert(request_id, request);

        let node = ConnectorCatalogNodeV3 {
            id: "public/items".into(),
            parent_id: Some("public".into()),
            kind: ConnectorCatalogNodeKindV3::Table,
            name: "items".into(),
            namespace: Some("public".into()),
            has_children: false,
            columns: Vec::new(),
            attributes: BTreeMap::new(),
        };
        let oversized = ConnectorResponseV3::CatalogPage {
            request_id: "catalog-1".into(),
            page: ConnectorCatalogPageV3 {
                nodes: vec![
                    node.clone(),
                    ConnectorCatalogNodeV3 {
                        id: "public/other".into(),
                        name: "other".into(),
                        ..node.clone()
                    },
                ],
                next_cursor: None,
            },
        };
        assert_eq!(
            validate_v3_response(&oversized, &capabilities, &mut pending, &mut validators,)
                .expect_err("requested page size is authoritative")
                .sql_state,
            "54000"
        );
        assert!(pending.contains_key("catalog-1"));

        let valid = ConnectorResponseV3::CatalogPage {
            request_id: "catalog-1".into(),
            page: ConnectorCatalogPageV3 {
                nodes: vec![node],
                next_cursor: None,
            },
        };
        validate_v3_response(&valid, &capabilities, &mut pending, &mut validators)
            .expect("valid Catalog response");
        assert!(pending.is_empty());
        assert_eq!(
            validate_v3_response(&valid, &capabilities, &mut pending, &mut validators)
                .expect_err("repeated response ID")
                .sql_state,
            "08P01"
        );
        assert_eq!(
            prepare_v3_request(
                &ConnectorRequestV3::Cancel {
                    request_id: "unknown".into(),
                },
                &pending,
            )
            .expect_err("unknown cancellation")
            .sql_state,
            "42704"
        );

        for index in 0..MAX_HOST_ACTIVE_V3_REQUESTS {
            pending.insert(format!("request-{index}"), PendingV3Request::Transaction);
        }
        let overflow = ConnectorRequestV3::Begin {
            request_id: "overflow".into(),
            connection_id: "connection-1".into(),
            isolation: None,
        };
        assert_eq!(
            prepare_v3_request(&overflow, &pending)
                .expect_err("active request limit")
                .sql_state,
            "54000"
        );
    }

    #[test]
    fn v3_sql_results_preserve_the_legacy_query_event_contract() {
        let column_type = ConnectorTypeV2 {
            vendor_name: "text".into(),
            logical_type: ConnectorLogicalTypeV2::Text,
            element_type: None,
            precision: None,
            scale: None,
            length: None,
        };
        let mut schemas = BTreeMap::new();
        let schema = translate_response_v3(
            ConnectorResponseV3::ResultEvent {
                request_id: "request-1".into(),
                event: ConnectorResultEventV3::Schema {
                    columns: vec![ConnectorColumnV2 {
                        name: "value".into(),
                        data_type: column_type,
                        nullable: false,
                    }],
                },
            },
            &mut schemas,
        )
        .expect("schema response");
        assert!(matches!(
            schema,
            ConnectorResponseV1::QueryEvent {
                event: QueryEvent::Schema(_),
                ..
            }
        ));

        let batch = translate_response_v3(
            ConnectorResponseV3::ResultEvent {
                request_id: "request-1".into(),
                event: ConnectorResultEventV3::Batch {
                    batch: ConnectorResultBatchV3::Rows {
                        rows: vec![vec![ConnectorValueV2::Text("one".into())]],
                    },
                },
            },
            &mut schemas,
        )
        .expect("batch response");
        assert!(matches!(
            batch,
            ConnectorResponseV1::QueryEvent {
                event: QueryEvent::Batch(Batch { rows, .. }),
                ..
            } if rows[0].values == vec![Value::Text("one".into())]
        ));

        let complete = translate_response_v3(
            ConnectorResponseV3::ResultEvent {
                request_id: "request-1".into(),
                event: ConnectorResultEventV3::Complete {
                    command_tag: "SELECT".into(),
                    affected_items: Some(1),
                },
            },
            &mut schemas,
        )
        .expect("complete response");
        assert!(matches!(
            complete,
            ConnectorResponseV1::QueryEvent {
                event: QueryEvent::Complete(CommandComplete {
                    rows_affected: 1,
                    ..
                }),
                ..
            }
        ));
        assert!(schemas.is_empty());
    }

    #[tokio::test]
    async fn unsupported_protocol_version_fails_without_pipe_or_credentials() {
        let pipe_name = connector_pipe_name();
        let mut server = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .create(&pipe_name)
            .expect("server pipe");
        let error = negotiate(
            &mut server,
            "future-connector",
            "1.0.0",
            CONNECTOR_PROTOCOL_V3 + 1,
        )
        .await
        .expect_err("future protocol must fail");
        assert_eq!(error.sql_state, "0A000");
    }

    #[test]
    fn v2_translation_preserves_batches_errors_and_endpoint_kinds() {
        let endpoint = structured_endpoint("postgresql", "db.example:5433", Some("app".into()))
            .expect("network endpoint");
        assert!(matches!(
            endpoint,
            ConnectorEndpointV2::Network { port: 5433, .. }
        ));
        let endpoint =
            structured_endpoint("sqlite", "C:\\data\\app.db", None).expect("file endpoint");
        assert!(matches!(endpoint, ConnectorEndpointV2::File { .. }));

        let error = ConnectorErrorV2 {
            sql_state: "40001".into(),
            vendor_code: Some("1213".into()),
            message: "deadlock".into(),
            detail: None,
            hint: None,
            position: None,
            retryable: true,
        };
        let response = translate_response_v2(
            ConnectorResponseV2::Error {
                request_id: Some("request-1".into()),
                error,
            },
            &mut BTreeMap::new(),
        )
        .expect("error response");
        assert!(matches!(
            response,
            ConnectorResponseV1::Error { error, .. } if error.sql_state == "40001"
        ));
    }

    #[test]
    fn v2_parameter_translation_preserves_interval_array_and_enum_types() {
        let interval = PgInterval::new(2, 3, 4);
        let interval_text = interval.to_string();
        assert_eq!(
            value_v2(&Value::Interval(interval)),
            ConnectorValueV2::Interval(interval_text)
        );

        let array = PgArray::one_dimensional(ScalarType::Int32, vec![Value::Int32(7), Value::Null])
            .expect("array");
        assert_eq!(
            value_v2(&Value::Array(array)),
            ConnectorValueV2::Array(vec![
                ConnectorValueV2::SignedInteger(7),
                ConnectorValueV2::Null,
            ])
        );

        let interval_type = connector_type(&ScalarType::Interval);
        assert_eq!(interval_type.logical_type, ConnectorLogicalTypeV2::Interval);
        let oid_type = connector_type(&ScalarType::Oid);
        assert_eq!(
            oid_type.logical_type,
            ConnectorLogicalTypeV2::UnsignedInteger
        );
        assert_eq!(oid_type.vendor_name, "oid");
        let name_type = connector_type(&ScalarType::Name);
        assert_eq!(name_type.logical_type, ConnectorLogicalTypeV2::Text);
        assert_eq!(name_type.length, Some(63));
        let internal_char_type = connector_type(&ScalarType::InternalChar);
        assert_eq!(
            internal_char_type.logical_type,
            ConnectorLogicalTypeV2::Text
        );
        assert_eq!(internal_char_type.length, Some(1));
        let enum_type = connector_type(&ScalarType::Enum {
            type_id: TypeId::new(42),
            labels: vec!["queued".into(), "done".into()],
        });
        assert_eq!(enum_type.logical_type, ConnectorLogicalTypeV2::Text);
        assert_eq!(enum_type.vendor_name, "enum");

        let array_type = connector_type(&ScalarType::Array {
            element: Box::new(ScalarType::Int32),
        });
        assert_eq!(array_type.logical_type, ConnectorLogicalTypeV2::Array);
        assert_eq!(
            array_type
                .element_type
                .expect("array element type")
                .logical_type,
            ConnectorLogicalTypeV2::SignedInteger
        );
    }

    #[tokio::test]
    async fn helper_exit_is_reported_without_sending_credentials() {
        let command = std::env::var_os("ComSpec").expect("ComSpec");
        let error = ConnectorHost::launch_entry(
            Path::new(&command),
            "ordadb-test",
            "1.0.0",
            MIN_CONNECTOR_API_VERSION,
        )
        .await
        .expect_err("cmd does not speak connector protocol");
        assert!(matches!(error.sql_state.as_str(), "08006" | "58030"));
    }
}
