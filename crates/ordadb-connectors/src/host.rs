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
    CONNECTOR_PROTOCOL_V2, ConnectorCatalogObjectKindV2, ConnectorEndpointV2,
    ConnectorLogicalTypeV2, ConnectorParameterV2, ConnectorQueryEventV2, ConnectorRequestV2,
    ConnectorResponseV2, ConnectorTlsModeV2, ConnectorTypeV2, ConnectorValueV2, ProtocolHelloV2,
    read_connector_frame as read_connector_frame_v2,
    validate_protocol_ready as validate_protocol_ready_v2,
    write_connector_frame as write_connector_frame_v2,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NegotiatedProtocol {
    V1,
    V2,
}

pub struct ConnectorHost {
    child: Child,
    pipe: NamedPipeServer,
    plugin_id: String,
    plugin_version: String,
    protocol: NegotiatedProtocol,
    query_schemas: BTreeMap<String, Schema>,
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

    pub async fn send(&mut self, request: &ConnectorRequestV1) -> Result<()> {
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
        match self.protocol {
            NegotiatedProtocol::V1 => write_connector_frame(&mut self.pipe, request).await,
            NegotiatedProtocol::V2 => {
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
        }
    }

    pub async fn receive(&mut self) -> Result<ConnectorResponseV1> {
        if let Some(response) = self.queued_responses.pop_front() {
            return Ok(response);
        }
        match self.protocol {
            NegotiatedProtocol::V1 => tokio::select! {
                response = read_connector_frame(&mut self.pipe) => response,
                status = self.child.wait() => Err(helper_exit_error(status)),
            },
            NegotiatedProtocol::V2 => {
                let response = tokio::select! {
                    response = read_connector_frame_v2(&mut self.pipe) => response,
                    status = self.child.wait() => return Err(helper_exit_error(status)),
                }?;
                translate_response_v2(response, &mut self.query_schemas)
            }
        }
    }

    pub async fn shutdown(mut self) -> Result<()> {
        let _ = self.send(&ConnectorRequestV1::Shutdown).await;
        match tokio::time::timeout(SHUTDOWN_TIMEOUT, self.child.wait()).await {
            Ok(Ok(_)) => Ok(()),
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
        ConnectorCapabilitiesV2, ConnectorErrorV2, ConnectorResponseV2, ConnectorTlsModeV2,
        ProtocolReadyV2,
    };
    use ordadb_types::{PgArray, PgInterval, TypeId};
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

    use super::*;
    use crate::ProtocolReady;

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
