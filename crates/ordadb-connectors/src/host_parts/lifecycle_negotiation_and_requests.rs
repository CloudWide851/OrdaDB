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
    CONNECTOR_PROTOCOL_V2, CONNECTOR_PROTOCOL_V3, ConnectorCapabilitiesV2, ConnectorCapabilitiesV3,
    ConnectorCatalogNodeKindV3, ConnectorCatalogObjectKindV2, ConnectorCommandV3,
    ConnectorCredentialV2, ConnectorEndpointV2, ConnectorKindV3, ConnectorLogicalTypeV2,
    ConnectorParameterV2, ConnectorQueryEventV2, ConnectorRequestV2, ConnectorRequestV3,
    ConnectorResponseV2, ConnectorResponseV3, ConnectorResultBatchV3, ConnectorResultEventV3,
    ConnectorResultStreamValidatorV3, ConnectorTlsModeV2, ConnectorTypeV2, ConnectorValueV2,
    ProtocolHelloV2, ProtocolHelloV3, read_connector_frame as read_connector_frame_v2,
    read_connector_frame_v3, validate_capability_subset_v3, validate_catalog_page_v3,
    validate_catalog_request_v3, validate_command_v3, validate_endpoint, validate_error_v3,
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
        if plugin_id == "oracle" {
            let client = ordadb_windows::discover_amd64_oracle_client(entry.parent())?;
            command.env("PATH", client.directory());
        }
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

    pub fn structured_endpoint(
        &self,
        endpoint: &str,
        database: Option<String>,
    ) -> Result<ConnectorEndpointV2> {
        structured_endpoint(&self.plugin_id, endpoint, database)
    }

    pub async fn connect_v2(
        &mut self,
        connection_id: impl Into<String>,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<ConnectorCapabilitiesV2> {
        if self.protocol_version() != CONNECTOR_PROTOCOL_V2 {
            return Err(DbError::unsupported("connector protocol v2 connection"));
        }
        let connection_id = connection_id.into();
        write_connector_frame_v2(
            &mut self.pipe,
            &ConnectorRequestV2::Connect {
                connection_id: connection_id.clone(),
                endpoint,
                tls_mode,
                credential,
            },
        )
        .await?;
        let response = tokio::select! {
            response = read_connector_frame_v2(&mut self.pipe) => response,
            status = self.child.wait() => return Err(helper_exit_error(status)),
        }?;
        match response {
            ConnectorResponseV2::Connected {
                connection_id: actual,
                capabilities,
            } if actual == connection_id => Ok(capabilities),
            ConnectorResponseV2::Error { error, .. } => Err(error.into_db_error()),
            _ => Err(DbError::new(
                "08P01",
                "connector returned an unexpected v2 connect response",
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
            } if actual == connection_id => {
                self.protocol = NegotiatedProtocol::V3(capabilities.clone());
                Ok(capabilities)
            }
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
