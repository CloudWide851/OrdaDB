use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ordadb_connector_sdk::{
    ConnectorCapabilitiesV3, ConnectorCatalogNodeKindV3, ConnectorCatalogNodeV3,
    ConnectorCatalogPageV3, ConnectorCommandInputModeV3, ConnectorCommandLanguageV3,
    ConnectorCommandV3, ConnectorCredentialV2, ConnectorDriverV3, ConnectorEndpointV2,
    ConnectorEventSinkV3, ConnectorIsolationLevelV2, ConnectorKeyValueV3, ConnectorKindV3,
    ConnectorResultBatchV3, ConnectorResultEventV3, ConnectorSessionV3, ConnectorTlsModeV2,
    ConnectorValueV2, connector_pipe_argument, run_named_pipe_helper_v3,
};
use ordadb_types::{DbError, Result};
use redis::{
    Client, Cmd, ConnectionInfo, ErrorKind as RedisErrorKind, ProtocolVersion, RedisConnectionInfo,
    RedisError, Value as RedisValue, aio::MultiplexedConnection, cluster::ClusterClient,
    cluster_async::ClusterConnection,
};
use serde_json::{Value as JsonValue, json};
use tokio_util::sync::CancellationToken;

const PLUGIN_ID: &str = "redis";
const LANGUAGE_ID: &str = "redis-resp3";
const ROOT_NODE_ID: &str = "redis:server";
const MAX_CLUSTER_NODES: usize = 16;
const MAX_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_RESULT_ENTRIES: usize = 10_000;
const MAX_KEY_BYTES: usize = 128;
const MAX_JSON_DEPTH: usize = 64;

#[derive(Debug, Default)]
struct RedisDriver;

struct RedisSession {
    connection: RedisConnection,
    mode: RedisMode,
    database: i64,
    capabilities: ConnectorCapabilitiesV3,
}

enum RedisConnection {
    Standalone(MultiplexedConnection),
    Cluster(ClusterConnection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedisMode {
    Standalone,
    Cluster,
}

#[derive(Debug)]
struct RedisEndpointOptions {
    mode: RedisMode,
    nodes: Vec<(String, u16)>,
    database: i64,
}

#[tokio::main]
async fn main() {
    let result = async {
        let pipe = connector_pipe_argument()?;
        run_named_pipe_helper_v3(&pipe, PLUGIN_ID, env!("CARGO_PKG_VERSION"), RedisDriver).await
    }
    .await;
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[async_trait]
impl ConnectorDriverV3 for RedisDriver {
    fn capabilities(&self) -> ConnectorCapabilitiesV3 {
        capabilities()
    }

    async fn connect(
        &self,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<Box<dyn ConnectorSessionV3>> {
        let ConnectorEndpointV2::Network {
            host,
            port,
            database,
            instance,
            options,
        } = endpoint
        else {
            return Err(invalid("Redis requires a network endpoint"));
        };
        if instance.as_deref() == Some("sentinel") {
            return Err(sentinel_unsupported());
        }
        if instance.is_some() {
            return Err(invalid(
                "Redis endpoint instance must be omitted or sentinel",
            ));
        }
        validate_host(&host)?;
        let endpoint = RedisEndpointOptions::parse(host, port, database.as_deref(), options)?;
        let connection = match endpoint.mode {
            RedisMode::Standalone => {
                let info = connection_info(
                    &endpoint.nodes[0].0,
                    endpoint.nodes[0].1,
                    endpoint.database,
                    credential.as_ref(),
                    tls_mode,
                )?;
                let client = Client::open(info).map_err(redis_error)?;
                RedisConnection::Standalone(
                    client
                        .get_multiplexed_async_connection()
                        .await
                        .map_err(redis_error)?,
                )
            }
            RedisMode::Cluster => {
                let infos = endpoint
                    .nodes
                    .iter()
                    .map(|(host, port)| {
                        connection_info(host, *port, 0, credential.as_ref(), tls_mode)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let client = ClusterClient::new(infos).map_err(redis_error)?;
                RedisConnection::Cluster(client.get_async_connection().await.map_err(redis_error)?)
            }
        };
        let mut session = RedisSession {
            connection,
            mode: endpoint.mode,
            database: endpoint.database,
            capabilities: capabilities(),
        };
        let mut ping = redis::cmd("PING");
        session.connection.query_value(&mut ping).await?;
        Ok(Box::new(session))
    }
}

#[async_trait]
impl ConnectorSessionV3 for RedisSession {
    fn capabilities(&self) -> &ConnectorCapabilitiesV3 {
        &self.capabilities
    }

    async fn catalog_page(
        &mut self,
        parent_id: Option<&str>,
        page_size: u32,
        cursor: Option<&str>,
    ) -> Result<ConnectorCatalogPageV3> {
        match parent_id {
            None => Ok(ConnectorCatalogPageV3 {
                nodes: vec![ConnectorCatalogNodeV3 {
                    id: ROOT_NODE_ID.into(),
                    parent_id: None,
                    kind: match self.mode {
                        RedisMode::Standalone => ConnectorCatalogNodeKindV3::Server,
                        RedisMode::Cluster => ConnectorCatalogNodeKindV3::Cluster,
                    },
                    name: match self.mode {
                        RedisMode::Standalone => "Redis".into(),
                        RedisMode::Cluster => "Redis Cluster".into(),
                    },
                    namespace: None,
                    has_children: true,
                    columns: Vec::new(),
                    attributes: BTreeMap::from([(
                        "mode".into(),
                        match self.mode {
                            RedisMode::Standalone => "standalone".into(),
                            RedisMode::Cluster => "cluster".into(),
                        },
                    )]),
                }],
                next_cursor: None,
            }),
            Some(ROOT_NODE_ID) => Ok(ConnectorCatalogPageV3 {
                nodes: vec![ConnectorCatalogNodeV3 {
                    id: keyspace_node_id(self.database),
                    parent_id: Some(ROOT_NODE_ID.into()),
                    kind: ConnectorCatalogNodeKindV3::Keyspace,
                    name: match self.mode {
                        RedisMode::Standalone => format!("db{}", self.database),
                        RedisMode::Cluster => "cluster-keyspace".into(),
                    },
                    namespace: None,
                    has_children: true,
                    columns: Vec::new(),
                    attributes: BTreeMap::new(),
                }],
                next_cursor: None,
            }),
            Some(parent) if parent == keyspace_node_id(self.database) => {
                self.scan_catalog_page(parent, page_size, cursor).await
            }
            Some(_) => Err(invalid("unknown Redis Catalog parent")),
        }
    }

    async fn execute(
        &mut self,
        _request_id: &str,
        command: &ConnectorCommandV3,
        batch_size: u32,
        cancellation: &CancellationToken,
        sink: &mut dyn ConnectorEventSinkV3,
    ) -> Result<()> {
        let ConnectorCommandV3::Arguments {
            language_id,
            arguments,
        } = command
        else {
            return Err(DbError::unsupported("non-argument Redis commands"));
        };
        if language_id != LANGUAGE_ID {
            return Err(DbError::unsupported(format!(
                "Redis command language {language_id}",
            )));
        }
        let (command_name, mut command) = redis_command(arguments)?;
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(cancelled()),
            result = self.connection.query_raw(&mut command) => result?,
        };
        let entries = response_entries(&command_name, result)?;
        let total = u64::try_from(entries.len()).unwrap_or(u64::MAX);
        let batch_size = usize::try_from(batch_size).unwrap_or(1_024);
        for batch in entries.chunks(batch_size) {
            sink.send(ConnectorResultEventV3::Batch {
                batch: ConnectorResultBatchV3::KeyValues {
                    entries: batch.to_vec(),
                },
            })
            .await?;
        }
        sink.send(ConnectorResultEventV3::Progress {
            items_processed: total,
        })
        .await?;
        sink.send(ConnectorResultEventV3::Complete {
            command_tag: command_name,
            affected_items: Some(total),
        })
        .await
    }

    async fn cancel(&mut self, _request_id: &str) -> Result<()> {
        Ok(())
    }

    async fn begin(&mut self, _isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
        Err(DbError::unsupported("Redis transaction requests"))
    }

    async fn commit(&mut self) -> Result<()> {
        Err(DbError::unsupported("Redis transaction requests"))
    }

    async fn rollback(&mut self) -> Result<()> {
        Err(DbError::unsupported("Redis transaction requests"))
    }
}

impl RedisSession {
    async fn scan_catalog_page(
        &mut self,
        parent_id: &str,
        page_size: u32,
        cursor: Option<&str>,
    ) -> Result<ConnectorCatalogPageV3> {
        let cursor = parse_scan_cursor(cursor)?;
        let mut command = redis::cmd("SCAN");
        command.arg(cursor).arg("COUNT").arg(u64::from(page_size));
        let value = self.connection.query_raw(&mut command).await?;
        let (next, keys) = scan_response(value)?;
        let nodes = keys
            .into_iter()
            .map(|key| key_catalog_node(parent_id, key))
            .collect::<Result<Vec<_>>>()?;
        Ok(ConnectorCatalogPageV3 {
            nodes,
            next_cursor: (next != 0).then(|| next.to_string()),
        })
    }
}

impl RedisConnection {
    async fn query_raw(&mut self, command: &mut Cmd) -> Result<RedisValue> {
        match self {
            Self::Standalone(connection) => {
                command.query_async(connection).await.map_err(redis_error)
            }
            Self::Cluster(connection) => command.query_async(connection).await.map_err(redis_error),
        }
    }

    async fn query_value(&mut self, command: &mut Cmd) -> Result<ConnectorValueV2> {
        let value = self.query_raw(command).await?;
        connector_value(&value)
    }
}

impl RedisEndpointOptions {
    fn parse(
        host: String,
        port: u16,
        database: Option<&str>,
        mut options: BTreeMap<String, String>,
    ) -> Result<Self> {
        let mode = match options
            .remove("mode")
            .unwrap_or_else(|| "standalone".into())
            .as_str()
        {
            "standalone" => RedisMode::Standalone,
            "cluster" => RedisMode::Cluster,
            "sentinel" => return Err(sentinel_unsupported()),
            _ => {
                return Err(invalid(
                    "Redis mode must be standalone, cluster, or sentinel",
                ));
            }
        };
        let database = database
            .unwrap_or("0")
            .parse::<i64>()
            .map_err(|_| invalid("Redis database must be a non-negative integer"))?;
        if !(0..=1_000_000).contains(&database) {
            return Err(invalid("Redis database is outside the supported range"));
        }
        if mode == RedisMode::Cluster && database != 0 {
            return Err(invalid("Redis Cluster supports only database 0"));
        }
        let mut nodes = vec![(host, port)];
        if let Some(extra) = options.remove("clusterNodes") {
            if mode != RedisMode::Cluster {
                return Err(invalid("Redis clusterNodes requires cluster mode"));
            }
            for value in extra.split(',').filter(|value| !value.trim().is_empty()) {
                nodes.push(parse_node(value.trim())?);
            }
        }
        if let Some(name) = options.keys().next() {
            return Err(DbError::unsupported(format!(
                "Redis endpoint option {name}"
            )));
        }
        let mut unique = BTreeSet::new();
        nodes.retain(|node| unique.insert(node.clone()));
        if nodes.is_empty() || nodes.len() > MAX_CLUSTER_NODES {
            return Err(resource(format!(
                "Redis endpoint requires 1-{MAX_CLUSTER_NODES} unique nodes",
            )));
        }
        for (host, _) in &nodes {
            validate_host(host)?;
        }
        Ok(Self {
            mode,
            nodes,
            database,
        })
    }
}

fn capabilities() -> ConnectorCapabilitiesV3 {
    ConnectorCapabilitiesV3 {
        kind: ConnectorKindV3::KeyValue,
        command_languages: vec![ConnectorCommandLanguageV3 {
            id: LANGUAGE_ID.into(),
            display_name: "Redis RESP3".into(),
            input_modes: vec![ConnectorCommandInputModeV3::Arguments],
        }],
        catalog: true,
        cancellation: true,
        transactions: false,
        savepoints: false,
        batch_query: true,
        maximum_batch_rows: 1_024,
        maximum_catalog_page_size: 512,
        tls_modes: vec![
            ConnectorTlsModeV2::Disable,
            ConnectorTlsModeV2::Require,
            ConnectorTlsModeV2::VerifyFull,
        ],
    }
}

fn connection_info(
    host: &str,
    port: u16,
    database: i64,
    credential: Option<&ConnectorCredentialV2>,
    tls_mode: ConnectorTlsModeV2,
) -> Result<ConnectionInfo> {
    let scheme = match tls_mode {
        ConnectorTlsModeV2::Disable => "redis",
        ConnectorTlsModeV2::Require | ConnectorTlsModeV2::VerifyFull => "rediss",
        ConnectorTlsModeV2::Prefer => {
            return Err(DbError::unsupported(
                "Redis TLS prefer mode because fail-open TLS is forbidden",
            ));
        }
        ConnectorTlsModeV2::VerifyCa => {
            return Err(DbError::unsupported(
                "Redis CA-only TLS with the bundled rustls transport",
            ));
        }
    };
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.into()
    };
    let mut info = format!("{scheme}://{host}:{port}")
        .parse::<ConnectionInfo>()
        .map_err(redis_error)?;
    let mut settings = RedisConnectionInfo::default()
        .set_db(database)
        .set_protocol(ProtocolVersion::RESP3)
        .set_lib_name("OrdaDB", env!("CARGO_PKG_VERSION"));
    if let Some(credential) = credential {
        if let Some(username) = credential
            .username
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            settings = settings.set_username(username);
        }
        settings = settings.set_password(credential.secret.as_str());
    }
    info = info.set_redis_settings(settings);
    Ok(info)
}

fn redis_command(arguments: &[ConnectorValueV2]) -> Result<(String, Cmd)> {
    let Some(first) = arguments.first() else {
        return Err(invalid("Redis command requires at least one argument"));
    };
    let ConnectorValueV2::Text(command_name) = first else {
        return Err(invalid("Redis command name must be text"));
    };
    if command_name.is_empty()
        || command_name.len() > 64
        || !command_name.bytes().all(|byte| byte.is_ascii_alphabetic())
    {
        return Err(invalid(
            "Redis command name must contain 1-64 ASCII letters",
        ));
    }
    let command_name = command_name.to_ascii_uppercase();
    reject_unsafe_command(&command_name, &arguments[1..])?;
    let mut command = redis::cmd(&command_name);
    let mut total = command_name.len();
    for argument in &arguments[1..] {
        let bytes = redis_argument(argument)?;
        total = total.saturating_add(bytes.len());
        if total > MAX_ARGUMENT_BYTES {
            return Err(resource(format!(
                "Redis command arguments exceed {MAX_ARGUMENT_BYTES} bytes",
            )));
        }
        command.arg(bytes);
    }
    Ok((command_name, command))
}

fn reject_unsafe_command(command: &str, arguments: &[ConnectorValueV2]) -> Result<()> {
    if matches!(
        command,
        "AUTH"
            | "HELLO"
            | "MONITOR"
            | "SUBSCRIBE"
            | "PSUBSCRIBE"
            | "SSUBSCRIBE"
            | "MULTI"
            | "EXEC"
            | "WATCH"
            | "UNWATCH"
            | "BLPOP"
            | "BRPOP"
            | "BRPOPLPUSH"
            | "BLMOVE"
            | "BLMPOP"
            | "BZPOPMIN"
            | "BZPOPMAX"
    ) {
        return Err(DbError::unsupported(format!(
            "Redis command {command} in connector protocol v3",
        )));
    }
    if matches!(command, "XREAD" | "XREADGROUP")
        && arguments.iter().any(|argument| {
            matches!(argument, ConnectorValueV2::Text(value) if value.eq_ignore_ascii_case("BLOCK"))
        })
    {
        return Err(DbError::unsupported("blocking Redis stream reads"));
    }
    if arguments.iter().any(|argument| {
        matches!(argument, ConnectorValueV2::Text(value)
            if (value.starts_with("redis://") || value.starts_with("rediss://"))
                && value.contains('@'))
    }) {
        return Err(invalid(
            "Redis command arguments must not contain credential-bearing URLs",
        ));
    }
    Ok(())
}

fn redis_argument(value: &ConnectorValueV2) -> Result<Vec<u8>> {
    match value {
        ConnectorValueV2::Null => Err(invalid("Redis command arguments cannot be NULL")),
        ConnectorValueV2::Boolean(value) => Ok(if *value { b"1" } else { b"0" }.to_vec()),
        ConnectorValueV2::SignedInteger(value) => Ok(value.to_string().into_bytes()),
        ConnectorValueV2::UnsignedInteger(value) => Ok(value.to_string().into_bytes()),
        ConnectorValueV2::FloatingPoint(value) => Ok(value.to_string().into_bytes()),
        ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Text(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::Uuid(value) => Ok(value.as_bytes().to_vec()),
        ConnectorValueV2::Binary(value) => BASE64
            .decode(value)
            .map_err(|_| invalid("Redis binary argument is not valid base64")),
        ConnectorValueV2::Json(value) => serde_json::to_vec(value).map_err(|error| {
            DbError::internal("failed to encode Redis JSON argument").with_detail(error.to_string())
        }),
        ConnectorValueV2::Array(_) => Err(DbError::unsupported("Redis array arguments")),
    }
}

fn response_entries(command: &str, value: RedisValue) -> Result<Vec<ConnectorKeyValueV3>> {
    let entries = match value {
        RedisValue::Map(values) => values
            .into_iter()
            .map(|(key, value)| {
                Ok(ConnectorKeyValueV3 {
                    key: connector_value(&key)?,
                    value: connector_value(&value)?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        RedisValue::Array(values) | RedisValue::Set(values) => values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                Ok(ConnectorKeyValueV3 {
                    key: ConnectorValueV2::UnsignedInteger(
                        u64::try_from(index).unwrap_or(u64::MAX),
                    ),
                    value: connector_value(value)?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        value => vec![ConnectorKeyValueV3 {
            key: ConnectorValueV2::Text(command.into()),
            value: connector_value(&value)?,
        }],
    };
    if entries.len() > MAX_RESULT_ENTRIES {
        return Err(resource(format!(
            "Redis response exceeds {MAX_RESULT_ENTRIES} top-level entries",
        )));
    }
    Ok(entries)
}

fn connector_value(value: &RedisValue) -> Result<ConnectorValueV2> {
    match value {
        RedisValue::Nil => Ok(ConnectorValueV2::Null),
        RedisValue::Int(value) => Ok(ConnectorValueV2::SignedInteger(*value)),
        RedisValue::BulkString(value) => bytes_value(value),
        RedisValue::SimpleString(value) => Ok(ConnectorValueV2::Text(value.clone())),
        RedisValue::Okay => Ok(ConnectorValueV2::Text("OK".into())),
        RedisValue::Double(value) => Ok(ConnectorValueV2::FloatingPoint(*value)),
        RedisValue::Boolean(value) => Ok(ConnectorValueV2::Boolean(*value)),
        RedisValue::BigNumber(value) => Ok(bytes_value(value)?),
        RedisValue::VerbatimString { text, .. } => Ok(ConnectorValueV2::Text(text.clone())),
        RedisValue::Array(_)
        | RedisValue::Map(_)
        | RedisValue::Attribute { .. }
        | RedisValue::Set(_) => Ok(ConnectorValueV2::Json(redis_json(value, 0)?)),
        RedisValue::Push { .. } => Err(DbError::unsupported("Redis push responses")),
        RedisValue::ServerError(error) => Err(DbError::new(
            "HV000",
            "Redis server returned an error value",
        )
        .with_detail(bounded_detail(&error.to_string()))),
        _ => Err(DbError::unsupported("unknown Redis RESP3 value")),
    }
}

fn redis_json(value: &RedisValue, depth: usize) -> Result<JsonValue> {
    if depth >= MAX_JSON_DEPTH {
        return Err(resource(
            "Redis nested response exceeds the JSON depth limit",
        ));
    }
    let next = depth + 1;
    match value {
        RedisValue::Nil => Ok(JsonValue::Null),
        RedisValue::Int(value) => Ok(json!(value)),
        RedisValue::BulkString(value) => bytes_json(value),
        RedisValue::SimpleString(value) => Ok(json!(value)),
        RedisValue::Okay => Ok(json!("OK")),
        RedisValue::Array(values) => values
            .iter()
            .map(|value| redis_json(value, next))
            .collect::<Result<Vec<_>>>()
            .map(JsonValue::Array),
        RedisValue::Map(values) => Ok(json!({
            "$map": values
                .iter()
                .map(|(key, value)| Ok(vec![redis_json(key, next)?, redis_json(value, next)?]))
                .collect::<Result<Vec<_>>>()?
        })),
        RedisValue::Attribute { data, attributes } => Ok(json!({
            "$value": redis_json(data, next)?,
            "$attributes": attributes
                .iter()
                .map(|(key, value)| Ok(vec![redis_json(key, next)?, redis_json(value, next)?]))
                .collect::<Result<Vec<_>>>()?
        })),
        RedisValue::Set(values) => Ok(json!({
            "$set": values
                .iter()
                .map(|value| redis_json(value, next))
                .collect::<Result<Vec<_>>>()?
        })),
        RedisValue::Double(value) => Ok(json!(value)),
        RedisValue::Boolean(value) => Ok(json!(value)),
        RedisValue::VerbatimString { text, .. } => Ok(json!({ "$verbatim": text })),
        RedisValue::BigNumber(value) => bytes_json(value),
        RedisValue::Push { .. } => Err(DbError::unsupported("Redis push responses")),
        RedisValue::ServerError(error) => Err(DbError::new(
            "HV000",
            "Redis server returned an error value",
        )
        .with_detail(bounded_detail(&error.to_string()))),
        _ => Err(DbError::unsupported("unknown Redis RESP3 value")),
    }
}

fn bytes_value(value: &[u8]) -> Result<ConnectorValueV2> {
    Ok(match std::str::from_utf8(value) {
        Ok(value) => ConnectorValueV2::Text(value.into()),
        Err(_) => ConnectorValueV2::Binary(BASE64.encode(value)),
    })
}

fn bytes_json(value: &[u8]) -> Result<JsonValue> {
    Ok(match std::str::from_utf8(value) {
        Ok(value) => json!(value),
        Err(_) => json!({ "$binary": BASE64.encode(value) }),
    })
}

fn scan_response(value: RedisValue) -> Result<(u64, Vec<Vec<u8>>)> {
    let RedisValue::Array(mut values) = value else {
        return Err(DbError::new("08P01", "Redis SCAN response is not an array"));
    };
    if values.len() != 2 {
        return Err(DbError::new(
            "08P01",
            "Redis SCAN response must contain cursor and keys",
        ));
    }
    let keys = match values.pop() {
        Some(RedisValue::Array(keys)) => keys
            .into_iter()
            .map(redis_key_bytes)
            .collect::<Result<Vec<_>>>()?,
        _ => return Err(DbError::new("08P01", "Redis SCAN keys are not an array")),
    };
    let cursor = values
        .pop()
        .ok_or_else(|| DbError::new("08P01", "Redis SCAN cursor is missing"))?;
    let cursor = std::str::from_utf8(&redis_key_bytes(cursor)?)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| DbError::new("08P01", "Redis SCAN cursor is invalid"))?;
    if keys.len() > MAX_RESULT_ENTRIES {
        return Err(resource("Redis SCAN page exceeds its result bound"));
    }
    Ok((cursor, keys))
}

fn redis_key_bytes(value: RedisValue) -> Result<Vec<u8>> {
    match value {
        RedisValue::BulkString(value) => Ok(value),
        RedisValue::SimpleString(value) => Ok(value.into_bytes()),
        RedisValue::Int(value) => Ok(value.to_string().into_bytes()),
        _ => Err(DbError::new("08P01", "Redis key is not a scalar string")),
    }
}

fn key_catalog_node(parent_id: &str, key: Vec<u8>) -> Result<ConnectorCatalogNodeV3> {
    if key.len() > MAX_KEY_BYTES {
        return Err(resource(format!(
            "Redis Catalog key exceeds {MAX_KEY_BYTES} bytes",
        )));
    }
    let name = match std::str::from_utf8(&key) {
        Ok(value) => value.into(),
        Err(_) => format!("base64:{}", BASE64.encode(&key)),
    };
    Ok(ConnectorCatalogNodeV3 {
        id: format!("redis:key:{}", hex_bytes(&key)),
        parent_id: Some(parent_id.into()),
        kind: ConnectorCatalogNodeKindV3::Key,
        name,
        namespace: None,
        has_children: false,
        columns: Vec::new(),
        attributes: BTreeMap::new(),
    })
}

fn keyspace_node_id(database: i64) -> String {
    format!("redis:keyspace:{database}")
}

fn hex_bytes(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len().saturating_mul(2));
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn parse_scan_cursor(value: Option<&str>) -> Result<u64> {
    let value = value.unwrap_or("0");
    if value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid("invalid Redis SCAN cursor"));
    }
    value
        .parse()
        .map_err(|_| invalid("invalid Redis SCAN cursor"))
}

fn parse_node(value: &str) -> Result<(String, u16)> {
    let (host, port) = value
        .rsplit_once(':')
        .ok_or_else(|| invalid("Redis cluster node must be host:port"))?;
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host)
        .to_owned();
    let port = port
        .parse::<u16>()
        .map_err(|_| invalid("Redis cluster node port is invalid"))?;
    validate_host(&host)?;
    Ok((host, port))
}

fn validate_host(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 253
        || value.contains('\0')
        || value.contains(['/', '?', '#', '@'])
    {
        return Err(invalid("Redis host is invalid"));
    }
    Ok(())
}

fn redis_error(error: RedisError) -> DbError {
    let (sql_state, message) = match error.kind() {
        RedisErrorKind::AuthenticationFailed => ("28000", "Redis authentication failed"),
        RedisErrorKind::InvalidClientConfig | RedisErrorKind::Client => {
            ("22023", "Redis client configuration is invalid")
        }
        RedisErrorKind::Io
        | RedisErrorKind::ClusterConnectionNotFound
        | RedisErrorKind::Server(redis::ServerErrorKind::ClusterDown)
        | RedisErrorKind::Server(redis::ServerErrorKind::MasterDown) => {
            ("08006", "Redis connection failed")
        }
        RedisErrorKind::MasterNameNotFoundBySentinel
        | RedisErrorKind::NoValidReplicasFoundBySentinel
        | RedisErrorKind::EmptySentinelList => ("0A000", "Redis Sentinel is unavailable"),
        RedisErrorKind::Server(redis::ServerErrorKind::NoPerm) => {
            ("42501", "Redis operation is not authorized")
        }
        RedisErrorKind::UnexpectedReturnType | RedisErrorKind::Parse => {
            ("08P01", "Redis returned an invalid RESP3 value")
        }
        _ => ("HV000", "Redis vendor operation failed"),
    };
    let detail = error
        .detail()
        .map(bounded_detail)
        .unwrap_or_else(|| bounded_detail(&error.to_string()));
    DbError::new(sql_state, message).with_detail(detail)
}

fn bounded_detail(value: &str) -> String {
    value.chars().take(4_096).collect()
}

fn sentinel_unsupported() -> DbError {
    DbError::unsupported("Redis Sentinel in the first connector version")
        .with_hint("Use a standalone or cluster Redis endpoint.")
}

fn cancelled() -> DbError {
    DbError::new("57014", "Redis operation was cancelled")
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn resource(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}
