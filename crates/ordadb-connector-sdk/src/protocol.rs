use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Formatter},
};

use ordadb_types::{DbError, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zeroize::Zeroizing;

pub const CONNECTOR_PROTOCOL_V2: u32 = 2;
pub const MAX_CONNECTOR_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CONNECTOR_TEXT_BYTES: usize = 1024 * 1024;
pub const MAX_CONNECTOR_BATCH_ROWS: u32 = 4096;
const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_OPTIONS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolHelloV2 {
    pub minimum_api_version: u32,
    pub maximum_api_version: u32,
    pub plugin_id: String,
    pub plugin_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolReadyV2 {
    pub api_version: u32,
    pub plugin_id: String,
    pub plugin_version: String,
    pub capabilities: ConnectorCapabilitiesV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorCapabilitiesV2 {
    pub catalog: bool,
    pub cancellation: bool,
    pub transactions: bool,
    pub savepoints: bool,
    pub batch_query: bool,
    pub maximum_batch_rows: u32,
    pub tls_modes: Vec<ConnectorTlsModeV2>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorTlsModeV2 {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ConnectorEndpointV2 {
    Network {
        host: String,
        port: u16,
        database: Option<String>,
        instance: Option<String>,
        options: BTreeMap<String, String>,
    },
    File {
        path: String,
        read_only: bool,
        create: bool,
        options: BTreeMap<String, String>,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConnectorCredentialV2 {
    pub username: Option<String>,
    pub secret: Zeroizing<String>,
}

impl ConnectorCredentialV2 {
    #[must_use]
    pub fn new(username: Option<String>, secret: impl Into<String>) -> Self {
        Self {
            username,
            secret: Zeroizing::new(secret.into()),
        }
    }
}

impl Debug for ConnectorCredentialV2 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorCredentialV2")
            .field("username", &self.username)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl Serialize for ConnectorCredentialV2 {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Wire<'a> {
            username: &'a Option<String>,
            secret: &'a str,
        }
        Wire {
            username: &self.username,
            secret: self.secret.as_str(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ConnectorCredentialV2 {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Wire {
            username: Option<String>,
            secret: String,
        }
        let wire = Wire::deserialize(deserializer)?;
        Ok(Self::new(wire.username, wire.secret))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorCatalogObjectKindV2 {
    Database,
    Schema,
    Table,
    View,
    MaterializedView,
    Column,
    Index,
    Constraint,
    Sequence,
    Function,
    Procedure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorCatalogObjectV2 {
    pub id: String,
    pub kind: ConnectorCatalogObjectKindV2,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub name: String,
    pub parent_id: Option<String>,
    pub comment: Option<String>,
    pub columns: Vec<ConnectorCatalogColumnV2>,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorCatalogColumnV2 {
    pub name: String,
    pub ordinal: u32,
    pub data_type: ConnectorTypeV2,
    pub nullable: bool,
    pub default_expression: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorLogicalTypeV2 {
    Null,
    Boolean,
    SignedInteger,
    UnsignedInteger,
    FloatingPoint,
    Decimal,
    Text,
    Binary,
    Date,
    Time,
    Timestamp,
    TimestampWithTimeZone,
    Interval,
    Uuid,
    Json,
    Array,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorTypeV2 {
    pub vendor_name: String,
    pub logical_type: ConnectorLogicalTypeV2,
    pub element_type: Option<Box<ConnectorTypeV2>>,
    pub precision: Option<u32>,
    pub scale: Option<u32>,
    pub length: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorColumnV2 {
    pub name: String,
    pub data_type: ConnectorTypeV2,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum ConnectorValueV2 {
    Null,
    Boolean(bool),
    SignedInteger(i64),
    UnsignedInteger(u64),
    FloatingPoint(f64),
    Decimal(String),
    Text(String),
    Binary(String),
    Date(String),
    Time(String),
    Timestamp(String),
    TimestampWithTimeZone(String),
    Interval(String),
    Uuid(String),
    Json(Value),
    Array(Vec<ConnectorValueV2>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorParameterV2 {
    pub data_type: Option<ConnectorTypeV2>,
    pub value: ConnectorValueV2,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorBatchV2 {
    pub rows: Vec<Vec<ConnectorValueV2>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorNoticeV2 {
    pub severity: String,
    pub message: String,
    pub code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ConnectorQueryEventV2 {
    Schema {
        columns: Vec<ConnectorColumnV2>,
    },
    Batch {
        batch: ConnectorBatchV2,
    },
    Progress {
        rows_processed: u64,
    },
    Notice {
        notice: ConnectorNoticeV2,
    },
    Complete {
        command_tag: String,
        affected_rows: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorIsolationLevelV2 {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorTransactionStateV2 {
    Idle,
    Active,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorErrorV2 {
    pub sql_state: String,
    pub vendor_code: Option<String>,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<usize>,
    pub retryable: bool,
}

impl ConnectorErrorV2 {
    #[must_use]
    pub fn from_db_error(error: &DbError) -> Self {
        Self {
            sql_state: error.sql_state.clone(),
            vendor_code: None,
            message: error.message.clone(),
            detail: error.detail.as_deref().map(str::to_owned),
            hint: error.hint.as_deref().map(str::to_owned),
            position: error.position,
            retryable: error.sql_state.starts_with("08") || error.sql_state == "40001",
        }
    }

    #[must_use]
    pub fn into_db_error(self) -> DbError {
        let mut error = DbError::new(self.sql_state, self.message);
        if let Some(detail) = self.detail {
            error = error.with_detail(detail);
        }
        if let Some(hint) = self.hint {
            error = error.with_hint(hint);
        }
        if let Some(position) = self.position {
            error.position = Some(position);
        }
        error
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ConnectorRequestV2 {
    Hello {
        hello: ProtocolHelloV2,
    },
    Connect {
        connection_id: String,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    },
    Disconnect {
        connection_id: String,
    },
    Catalog {
        request_id: String,
        connection_id: String,
    },
    Execute {
        request_id: String,
        connection_id: String,
        sql: String,
        params: Vec<ConnectorParameterV2>,
        batch_size: u32,
    },
    Cancel {
        request_id: String,
    },
    Begin {
        request_id: String,
        connection_id: String,
        isolation: Option<ConnectorIsolationLevelV2>,
    },
    Commit {
        request_id: String,
        connection_id: String,
    },
    Rollback {
        request_id: String,
        connection_id: String,
    },
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ConnectorResponseV2 {
    Ready {
        ready: ProtocolReadyV2,
    },
    Connected {
        connection_id: String,
        capabilities: ConnectorCapabilitiesV2,
    },
    Disconnected {
        connection_id: String,
    },
    Catalog {
        request_id: String,
        objects: Vec<ConnectorCatalogObjectV2>,
    },
    QueryEvent {
        request_id: String,
        event: ConnectorQueryEventV2,
    },
    Cancelled {
        request_id: String,
    },
    Transaction {
        request_id: String,
        state: ConnectorTransactionStateV2,
    },
    Error {
        request_id: Option<String>,
        error: ConnectorErrorV2,
    },
    Shutdown,
}

pub fn validate_protocol_ready(
    ready: &ProtocolReadyV2,
    plugin_id: &str,
    plugin_version: &str,
) -> Result<()> {
    if ready.api_version != CONNECTOR_PROTOCOL_V2 {
        return Err(DbError::unsupported(format!(
            "connector protocol version {}",
            ready.api_version
        ))
        .with_hint("Install a connector built for this OrdaDB host."));
    }
    if ready.plugin_id != plugin_id || ready.plugin_version != plugin_version {
        return Err(protocol_error(
            "connector handshake identity does not match the installed manifest",
        ));
    }
    validate_capabilities(&ready.capabilities)
}

pub fn validate_capabilities(capabilities: &ConnectorCapabilitiesV2) -> Result<()> {
    if !capabilities.batch_query
        || capabilities.maximum_batch_rows == 0
        || capabilities.maximum_batch_rows > MAX_CONNECTOR_BATCH_ROWS
    {
        return Err(protocol_error(format!(
            "connector maximum batch rows must be between 1 and {MAX_CONNECTOR_BATCH_ROWS}"
        )));
    }
    let unique = capabilities
        .tls_modes
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if unique.len() != capabilities.tls_modes.len() {
        return Err(protocol_error(
            "connector capabilities contain duplicate TLS modes",
        ));
    }
    if capabilities.savepoints && !capabilities.transactions {
        return Err(protocol_error(
            "connector savepoints require transaction capability",
        ));
    }
    Ok(())
}

pub fn validate_endpoint(endpoint: &ConnectorEndpointV2) -> Result<()> {
    match endpoint {
        ConnectorEndpointV2::Network {
            host,
            port,
            database,
            instance,
            options,
        } => {
            validate_text("connector host", host, MAX_IDENTIFIER_BYTES)?;
            if *port == 0 {
                return Err(invalid("connector network port must be positive"));
            }
            validate_optional_identifier("connector database", database.as_deref())?;
            validate_optional_identifier("connector instance", instance.as_deref())?;
            validate_options(options)?;
        }
        ConnectorEndpointV2::File { path, options, .. } => {
            validate_text("connector database path", path, 32 * 1024)?;
            if path.contains('\0') {
                return Err(invalid("connector database path contains a NUL byte"));
            }
            validate_options(options)?;
        }
    }
    Ok(())
}

fn validate_optional_identifier(name: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        validate_text(name, value, MAX_IDENTIFIER_BYTES)?;
    }
    Ok(())
}

fn validate_options(options: &BTreeMap<String, String>) -> Result<()> {
    if options.len() > MAX_OPTIONS {
        return Err(invalid(format!(
            "connector endpoint supports at most {MAX_OPTIONS} options"
        )));
    }
    for (key, value) in options {
        validate_text("connector option name", key, MAX_IDENTIFIER_BYTES)?;
        validate_text("connector option value", value, 4096)?;
    }
    Ok(())
}

fn validate_text(name: &str, value: &str, maximum_bytes: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > maximum_bytes || value.chars().any(char::is_control)
    {
        return Err(invalid(format!(
            "{name} must contain 1-{maximum_bytes} printable UTF-8 bytes"
        )));
    }
    Ok(())
}

pub async fn read_connector_frame<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let length = reader.read_u32_le().await.map_err(|error| {
        protocol_error(format!("failed to read connector frame length: {error}"))
    })?;
    let length =
        usize::try_from(length).map_err(|_| protocol_error("connector frame length overflowed"))?;
    if length == 0 || length > MAX_CONNECTOR_FRAME_BYTES {
        return Err(protocol_error(format!(
            "connector frame length must be between 1 and {MAX_CONNECTOR_FRAME_BYTES} bytes"
        )));
    }
    let mut bytes = vec![0_u8; length];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|error| protocol_error(format!("failed to read connector frame: {error}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| protocol_error(format!("connector frame is invalid JSON: {error}")))
}

pub async fn write_connector_frame<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes =
        Zeroizing::new(serde_json::to_vec(value).map_err(|error| {
            protocol_error(format!("failed to encode connector frame: {error}"))
        })?);
    if bytes.is_empty() || bytes.len() > MAX_CONNECTOR_FRAME_BYTES {
        return Err(protocol_error(format!(
            "connector frame length must be between 1 and {MAX_CONNECTOR_FRAME_BYTES} bytes"
        )));
    }
    let length = u32::try_from(bytes.len())
        .map_err(|_| protocol_error("connector frame length overflowed"))?;
    writer.write_u32_le(length).await.map_err(|error| {
        protocol_error(format!("failed to write connector frame length: {error}"))
    })?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| protocol_error(format!("failed to write connector frame: {error}")))?;
    writer
        .flush()
        .await
        .map_err(|error| protocol_error(format!("failed to flush connector frame: {error}")))
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::new("08P01", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capabilities() -> ConnectorCapabilitiesV2 {
        ConnectorCapabilitiesV2 {
            catalog: true,
            cancellation: true,
            transactions: true,
            savepoints: false,
            batch_query: true,
            maximum_batch_rows: 1024,
            tls_modes: vec![
                ConnectorTlsModeV2::Disable,
                ConnectorTlsModeV2::Require,
                ConnectorTlsModeV2::VerifyFull,
            ],
        }
    }

    #[test]
    fn hello_and_ready_have_stable_camel_case_json() {
        let request = ConnectorRequestV2::Hello {
            hello: ProtocolHelloV2 {
                minimum_api_version: CONNECTOR_PROTOCOL_V2,
                maximum_api_version: CONNECTOR_PROTOCOL_V2,
                plugin_id: "postgresql".into(),
                plugin_version: "1.0.0".into(),
            },
        };
        let json = serde_json::to_string(&request).expect("request JSON");
        assert_eq!(
            json,
            r#"{"kind":"hello","hello":{"minimumApiVersion":2,"maximumApiVersion":2,"pluginId":"postgresql","pluginVersion":"1.0.0"}}"#
        );

        let response = ConnectorResponseV2::Ready {
            ready: ProtocolReadyV2 {
                api_version: CONNECTOR_PROTOCOL_V2,
                plugin_id: "postgresql".into(),
                plugin_version: "1.0.0".into(),
                capabilities: capabilities(),
            },
        };
        let json = serde_json::to_value(response).expect("response JSON");
        assert_eq!(json["kind"], "ready");
        assert_eq!(json["ready"]["apiVersion"], CONNECTOR_PROTOCOL_V2);
        assert!(
            json["ready"]["capabilities"]["batchQuery"]
                .as_bool()
                .unwrap()
        );
    }

    #[test]
    fn credentials_are_redacted_and_unknown_fields_fail_closed() {
        let credential = ConnectorCredentialV2::new(Some("alice".into()), "secret-value");
        assert!(!format!("{credential:?}").contains("secret-value"));
        let json = serde_json::to_string(&credential).expect("credential JSON");
        assert!(json.contains("secret-value"));
        assert!(
            serde_json::from_str::<ConnectorCredentialV2>(
                r#"{"username":"alice","secret":"value","extra":true}"#
            )
            .is_err()
        );
    }

    #[test]
    fn endpoint_and_capability_validation_is_bounded() {
        let endpoint = ConnectorEndpointV2::Network {
            host: "localhost".into(),
            port: 5432,
            database: Some("ordadb".into()),
            instance: None,
            options: BTreeMap::new(),
        };
        validate_endpoint(&endpoint).expect("endpoint");
        validate_capabilities(&capabilities()).expect("capabilities");

        let mut invalid_capabilities = capabilities();
        invalid_capabilities
            .tls_modes
            .push(ConnectorTlsModeV2::Require);
        assert_eq!(
            validate_capabilities(&invalid_capabilities)
                .expect_err("duplicate TLS")
                .sql_state,
            "08P01"
        );
    }

    #[tokio::test]
    async fn frame_round_trip_and_bounds_are_stable() {
        let request = ConnectorRequestV2::Catalog {
            request_id: "request-1".into(),
            connection_id: "connection-1".into(),
        };
        let mut bytes = Vec::new();
        write_connector_frame(&mut bytes, &request)
            .await
            .expect("write frame");
        let decoded: ConnectorRequestV2 = read_connector_frame(&mut bytes.as_slice())
            .await
            .expect("read frame");
        assert!(matches!(
            decoded,
            ConnectorRequestV2::Catalog { request_id, .. } if request_id == "request-1"
        ));

        let mut oversized = Vec::new();
        oversized.extend_from_slice(
            &u32::try_from(MAX_CONNECTOR_FRAME_BYTES + 1)
                .expect("frame maximum")
                .to_le_bytes(),
        );
        let error = read_connector_frame::<_, ConnectorRequestV2>(&mut oversized.as_slice())
            .await
            .expect_err("oversized frame");
        assert_eq!(error.sql_state, "08P01");
    }

    #[test]
    fn structured_connector_error_round_trips_without_losing_sqlstate() {
        let source = DbError::new("40001", "serialization failure")
            .with_detail("vendor deadlock")
            .with_hint("retry transaction");
        let converted = ConnectorErrorV2::from_db_error(&source);
        assert!(converted.retryable);
        let restored = converted.into_db_error();
        assert_eq!(restored.sql_state, source.sql_state);
        assert_eq!(restored.message, source.message);
        assert_eq!(restored.detail, source.detail);
        assert_eq!(restored.hint, source.hint);
    }
}
