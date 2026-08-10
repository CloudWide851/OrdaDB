use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use ordadb_ai::AiToolLimits;
use ordadb_connector_sdk::{
    CONNECTOR_PROTOCOL_V2, CONNECTOR_PROTOCOL_V3, ConnectorCapabilitiesV2, ConnectorCapabilitiesV3,
    ConnectorCatalogNodeKindV3, ConnectorCatalogNodeV3, ConnectorCommandV3, ConnectorCredentialV2,
    ConnectorKindV3, ConnectorParameterV2, ConnectorRequestV3, ConnectorResponseV3,
    ConnectorResultBatchV3, ConnectorResultEventV3, ConnectorTlsModeV2,
    ConnectorTransactionStateV2, ConnectorValueV2, MAX_CONNECTOR_CATALOG_PAGE_NODES,
    MAX_CONNECTOR_COMMAND_ARGUMENTS, MAX_CONNECTOR_TEXT_BYTES,
};
use ordadb_connectors::{
    CatalogEntry, ConnectorHost, ConnectorRequestV1, ConnectorResponseV1, CredentialPayload,
    PluginManager,
};
use ordadb_protocol::{ClientConfig, PgCancelToken, PgClient, PgQueryEvent};
use ordadb_types::{DbError, QueryEvent, Value};
use ordadb_windows::{CredentialVault, PromptedCredential, prompt_for_credential};
use reqwest::{Client, Method, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use tauri::{AppHandle, Emitter, State};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use windows_service::service::{ServiceAccess, ServiceState};
use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};
use zeroize::Zeroizing;

use crate::workspace::CredentialAccess;

pub const DBMS_QUERY_EVENT: &str = "dbms://query";
const NATIVE_CONNECTOR_ID: &str = "ordadb-native";
const QUERY_BATCH_ROWS: usize = 1_024;
const MAX_ADMIN_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const BOOTSTRAP_TICKET_TTL: Duration = Duration::from_secs(120);
const MAX_BOOTSTRAP_TICKETS: usize = 32;
const MAX_DESKTOP_CATALOG_NODES: usize = 10_000;
const VALID_CONNECTOR_IDS: [&str; 10] = [
    NATIVE_CONNECTOR_ID,
    "postgresql",
    "mysql",
    "sqlite",
    "sql-server",
    "mongodb",
    "redis",
    "mariadb",
    "clickhouse",
    "oracle",
];

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialSaved {
    credential_id: String,
    username: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PromptCredentialRequest {
    credential_id: String,
    connector_id: String,
    suggested_username: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectRequest {
    connector_id: String,
    connector_kind: String,
    command_language: String,
    dialect: Option<String>,
    endpoint: String,
    admin_endpoint: Option<String>,
    database: Option<String>,
    tls_mode: ConnectorTlsModeV2,
    credential_id: String,
    #[serde(default)]
    credential_access: CredentialAccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionProbeStageName {
    Service,
    PgPort,
    AdminApi,
    Initialization,
    Authentication,
    Catalog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionProbeStageStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProbeStage {
    stage: ConnectionProbeStageName,
    status: ConnectionProbeStageStatus,
    error: Option<DbmsError>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProbe {
    ready: bool,
    stages: Vec<ConnectionProbeStage>,
    bootstrap_ticket: Option<LocalBootstrapTicket>,
}

impl ConnectionProbe {
    fn new() -> Self {
        Self {
            ready: false,
            stages: Vec::with_capacity(6),
            bootstrap_ticket: None,
        }
    }

    fn passed(&mut self, stage: ConnectionProbeStageName) {
        self.stages.push(ConnectionProbeStage {
            stage,
            status: ConnectionProbeStageStatus::Passed,
            error: None,
        });
    }

    fn failed(&mut self, stage: ConnectionProbeStageName, error: DbError) {
        self.stages.push(ConnectionProbeStage {
            stage,
            status: ConnectionProbeStageStatus::Failed,
            error: Some(error.into()),
        });
    }

    fn skipped(&mut self, stage: ConnectionProbeStageName) {
        self.stages.push(ConnectionProbeStage {
            stage,
            status: ConnectionProbeStageStatus::Skipped,
            error: None,
        });
    }

    fn finish(&mut self) {
        self.ready = self.stages.iter().all(|stage| {
            stage.stage == ConnectionProbeStageName::Service
                || stage.status == ConnectionProbeStageStatus::Passed
        });
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalBootstrapTicket {
    ticket: String,
    expires_in_ms: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BootstrapAdminRequest {
    ticket: String,
    connection: ConnectRequest,
    suggested_username: String,
}

impl std::fmt::Debug for BootstrapAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BootstrapAdminRequest")
            .field("ticket", &"<redacted>")
            .field("connection", &self.connection)
            .field("suggested_username", &self.suggested_username)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapAdminResult {
    success: bool,
    user: Option<String>,
    error: Option<DbmsError>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DbmsCapabilities {
    catalog: bool,
    transactions: bool,
    cancel: bool,
    explain: bool,
    sessions: bool,
    locks: bool,
    metrics: bool,
    wal: bool,
    checkpoint: bool,
    backup: bool,
    import_export: bool,
    service_control: bool,
}

impl DbmsCapabilities {
    const fn native() -> Self {
        Self {
            catalog: true,
            transactions: true,
            cancel: true,
            explain: true,
            sessions: true,
            locks: true,
            metrics: true,
            wal: true,
            checkpoint: true,
            backup: true,
            import_export: true,
            service_control: false,
        }
    }

    const fn plugin() -> Self {
        Self {
            catalog: true,
            transactions: true,
            cancel: true,
            explain: true,
            sessions: true,
            locks: false,
            metrics: false,
            wal: false,
            checkpoint: false,
            backup: false,
            import_export: false,
            service_control: false,
        }
    }

    fn plugin_v2(capabilities: &ConnectorCapabilitiesV2) -> Self {
        Self {
            catalog: capabilities.catalog,
            transactions: capabilities.transactions,
            cancel: capabilities.cancellation,
            explain: true,
            sessions: false,
            locks: false,
            metrics: false,
            wal: false,
            checkpoint: false,
            backup: false,
            import_export: false,
            service_control: false,
        }
    }

    fn plugin_v3(capabilities: &ConnectorCapabilitiesV3) -> Self {
        Self {
            catalog: capabilities.catalog,
            transactions: capabilities.transactions,
            cancel: capabilities.cancellation,
            explain: capabilities.kind == ConnectorKindV3::Sql,
            sessions: false,
            locks: false,
            metrics: false,
            wal: false,
            checkpoint: false,
            backup: false,
            import_export: false,
            service_control: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSnapshot {
    connection_id: String,
    connector_id: String,
    connector_kind: String,
    command_language: String,
    dialect: Option<String>,
    endpoint: String,
    database: String,
    credential_access: CredentialAccess,
    mode: &'static str,
    capabilities: DbmsCapabilities,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogObject {
    id: Option<String>,
    kind: String,
    schema: String,
    namespace: Option<String>,
    name: String,
    parent: Option<String>,
    details: JsonValue,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogSnapshot {
    connection_id: String,
    objects: Vec<CatalogObject>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecuteRequest {
    connection_id: String,
    command: DesktopCommand,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DesktopCommand {
    Text {
        language_id: String,
        text: String,
        #[serde(default)]
        params: Vec<Option<String>>,
    },
    Document {
        language_id: String,
        document: JsonValue,
    },
    Arguments {
        language_id: String,
        arguments: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationStarted {
    request_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResult {
    command_tag: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DbmsError {
    sql_state: String,
    message: String,
    detail: Option<Box<str>>,
    hint: Option<Box<str>>,
    position: Option<usize>,
    query_id: Box<str>,
}

impl From<DbError> for DbmsError {
    fn from(error: DbError) -> Self {
        Self {
            sql_state: error.sql_state,
            message: error.message,
            detail: error.detail,
            hint: error.hint,
            position: error.position,
            query_id: error.query_id,
        }
    }
}

type DesktopResult<T> = std::result::Result<T, DbmsError>;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryColumn {
    name: String,
    data_type: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DbmsQueryEvent {
    Schema {
        columns: Vec<QueryColumn>,
    },
    Batch {
        rows: Vec<Vec<Option<String>>>,
    },
    Documents {
        documents: Vec<JsonValue>,
    },
    KeyValues {
        entries: Vec<DbmsKeyValue>,
    },
    Progress {
        rows_processed: u64,
    },
    Notice {
        severity: String,
        sql_state: String,
        message: String,
    },
    Complete {
        command_tag: String,
        duration_ms: u64,
    },
    Error {
        error: DbmsError,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DbmsKeyValue {
    key: JsonValue,
    value: JsonValue,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryUpdate {
    request_id: String,
    event: DbmsQueryEvent,
}

#[derive(Debug)]
pub(crate) struct BoundedAiQueryResult {
    pub content: JsonValue,
    pub columns: Vec<String>,
    pub rows_retained: usize,
    pub total_rows: usize,
    pub bytes_retained: usize,
    pub truncated: bool,
    pub command_tag: String,
}

#[derive(Debug, Clone)]
pub(crate) struct AiConnectionPolicy {
    pub connector_kind: String,
    pub command_language: String,
    pub credential_access: CredentialAccess,
    pub native: bool,
}

struct BoundedAiCollector {
    limits: AiToolLimits,
    columns: Vec<QueryColumn>,
    items: Vec<JsonValue>,
    notices: Vec<JsonValue>,
    result_kind: &'static str,
    item_bytes: usize,
    total_rows: usize,
    truncated: bool,
    command_tag: String,
}

impl BoundedAiCollector {
    fn new(limits: AiToolLimits) -> Self {
        Self {
            limits,
            columns: Vec::new(),
            items: Vec::new(),
            notices: Vec::new(),
            result_kind: "rows",
            item_bytes: 0,
            total_rows: 0,
            truncated: false,
            command_tag: String::new(),
        }
    }

    fn push(&mut self, event: DbmsQueryEvent) {
        match event {
            DbmsQueryEvent::Schema { columns } => self.columns = columns,
            DbmsQueryEvent::Batch { rows } => {
                self.result_kind = "rows";
                for row in rows {
                    self.push_item(JsonValue::Array(
                        row.into_iter()
                            .map(|value| value.map_or(JsonValue::Null, JsonValue::String))
                            .collect(),
                    ));
                }
            }
            DbmsQueryEvent::Documents { documents } => {
                self.result_kind = "documents";
                for document in documents {
                    self.push_item(document);
                }
            }
            DbmsQueryEvent::KeyValues { entries } => {
                self.result_kind = "keyValues";
                for entry in entries {
                    self.push_item(serde_json::json!({
                        "key": entry.key,
                        "value": entry.value,
                    }));
                }
            }
            DbmsQueryEvent::Progress { rows_processed } => {
                self.total_rows = self
                    .total_rows
                    .max(usize::try_from(rows_processed).unwrap_or(usize::MAX));
            }
            DbmsQueryEvent::Notice {
                severity,
                sql_state,
                message,
            } => {
                if self.notices.len() < 64 {
                    self.notices.push(serde_json::json!({
                        "severity": severity,
                        "sqlState": sql_state,
                        "message": message,
                    }));
                } else {
                    self.truncated = true;
                }
            }
            DbmsQueryEvent::Complete { command_tag, .. } => self.command_tag = command_tag,
            DbmsQueryEvent::Error { .. } => {}
        }
    }

    fn push_item(&mut self, item: JsonValue) {
        self.total_rows = self.total_rows.saturating_add(1);
        if self.items.len() >= self.limits.max_rows {
            self.truncated = true;
            return;
        }
        let item_bytes = serde_json::to_vec(&item).map_or(usize::MAX, |bytes| bytes.len());
        if self.item_bytes.saturating_add(item_bytes) > self.limits.max_result_bytes {
            self.truncated = true;
            return;
        }
        self.item_bytes = self.item_bytes.saturating_add(item_bytes);
        self.items.push(item);
    }

    fn finish(mut self) -> Result<BoundedAiQueryResult, DbError> {
        loop {
            let content = serde_json::json!({
                "kind": self.result_kind,
                "columns": self.columns,
                "items": self.items,
                "notices": self.notices,
                "commandTag": self.command_tag,
            });
            let bytes_retained = serde_json::to_vec(&content)
                .map_err(|error| {
                    DbError::internal("failed to encode bounded AI query result")
                        .with_detail(error.to_string())
                })?
                .len();
            if bytes_retained <= self.limits.max_result_bytes {
                return Ok(BoundedAiQueryResult {
                    columns: self
                        .columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect(),
                    rows_retained: self.items.len(),
                    total_rows: self.total_rows,
                    truncated: self.truncated,
                    command_tag: self.command_tag,
                    content,
                    bytes_retained,
                });
            }
            if self.items.pop().is_some() {
                self.truncated = true;
                continue;
            }
            return Err(DbError::new(
                "54000",
                "AI query metadata exceeds the 2 MiB result limit",
            ));
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineStatus {
    generation: u64,
    table_count: usize,
    row_count: u64,
    index_count: usize,
    durable_lsn: Option<u64>,
    dirty_page_count: usize,
    commits_since_checkpoint: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    process_id: u32,
    user: String,
    database: String,
    application_name: Option<String>,
    connected_at: JsonValue,
    remote_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryInfo {
    query_id: String,
    process_id: u32,
    sql: String,
    started_at: JsonValue,
    finished_at: Option<JsonValue>,
    rows_processed: u64,
    outcome: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LockStatus {
    single_writer: bool,
    active_locks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    active_sessions: usize,
    active_queries: usize,
    engine: EngineStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilityStatus {
    supported: bool,
    reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicConfig {
    data_dir: String,
    pg_bind: String,
    admin_bind: String,
    remote_requires_tls: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MonitorSnapshot {
    connection_id: String,
    sessions: Vec<SessionInfo>,
    queries: Vec<QueryInfo>,
    locks: LockStatus,
    metrics: Metrics,
    storage: EngineStatus,
    wal: EngineStatus,
    backups: CapabilityStatus,
    config: PublicConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdministrationOperationKind {
    Backup,
    Restore,
    Import,
    Export,
}

impl AdministrationOperationKind {
    const fn endpoint(self) -> &'static str {
        match self {
            Self::Backup => "/v1/backups",
            Self::Restore => "/v1/restores",
            Self::Import => "/v1/imports",
            Self::Export => "/v1/exports",
        }
    }

    const fn requires_table(self) -> bool {
        matches!(self, Self::Import | Self::Export)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AdministrationTransferFormat {
    Csv,
    JsonLines,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartAdministrationOperationRequest {
    pub(crate) connection_id: String,
    pub(crate) kind: AdministrationOperationKind,
    pub(crate) path: String,
    pub(crate) schema: Option<String>,
    pub(crate) table: Option<String>,
    pub(crate) format: Option<AdministrationTransferFormat>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdministrationOperationResponse {
    operation_id: Uuid,
    kind: AdministrationOperationKind,
    state: String,
    path: String,
    schema: Option<String>,
    table: Option<String>,
    started_at: Option<JsonValue>,
    finished_at: Option<JsonValue>,
    rows: Option<u64>,
    bytes: Option<u64>,
    error: Option<DbError>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdministrationOperation {
    operation_id: Uuid,
    kind: AdministrationOperationKind,
    state: String,
    path: String,
    schema: Option<String>,
    table: Option<String>,
    started_at: Option<JsonValue>,
    finished_at: Option<JsonValue>,
    rows: Option<u64>,
    bytes: Option<u64>,
    error: Option<DbmsError>,
}

impl From<AdministrationOperationResponse> for AdministrationOperation {
    fn from(operation: AdministrationOperationResponse) -> Self {
        Self {
            operation_id: operation.operation_id,
            kind: operation.kind,
            state: operation.state,
            path: operation.path,
            schema: operation.schema,
            table: operation.table,
            started_at: operation.started_at,
            finished_at: operation.finished_at,
            rows: operation.rows,
            bytes: operation.bytes,
            error: operation.error.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdministrationServiceStatus {
    name: String,
    process_running: bool,
    windows_service_supported: bool,
    data_dir: String,
    operations_root: String,
}

impl AdministrationServiceStatus {
    pub(crate) fn data_dir(&self) -> &str {
        &self.data_dir
    }

    pub(crate) fn operations_root(&self) -> &str {
        &self.operations_root
    }
}

#[derive(Debug)]
struct ConnectionHandle {
    connector_kind: String,
    command_language: String,
    credential_access: CredentialAccess,
    transport: ConnectionTransport,
}

#[derive(Debug)]
enum ConnectionTransport {
    Native(NativeConnection),
    Plugin(Box<PluginConnection>),
}

struct NativeConnection {
    pg: Arc<Mutex<PgClient>>,
    cancel: PgCancelToken,
    admin: AdminSession,
    address: SocketAddr,
    database: String,
    credential_id: String,
}

impl std::fmt::Debug for NativeConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeConnection")
            .field("pg", &"<authenticated PostgreSQL connection>")
            .field("cancel", &self.cancel)
            .field("admin", &self.admin)
            .finish()
    }
}

#[derive(Debug)]
struct PluginConnection {
    host: AsyncMutex<Option<ConnectorHost>>,
    capabilities_v3: Option<ConnectorCapabilitiesV3>,
}

struct AdminSession {
    base_url: String,
    bearer: Zeroizing<String>,
}

impl std::fmt::Debug for AdminSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminSession")
            .field("base_url", &self.base_url)
            .field("bearer", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone)]
enum RequestCancellation {
    Native(PgCancelToken),
    Plugin(CancellationToken),
}

#[derive(Debug, Clone)]
struct ActiveRequest {
    connection_id: String,
    cancellation: RequestCancellation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServiceIdentity {
    process_id: u32,
    data_dir: PathBuf,
    pipe_name: String,
}

#[derive(Debug)]
struct BootstrapTicketRecord {
    expires_at: Instant,
    request_fingerprint: [u8; 32],
    service: ServiceIdentity,
}

#[derive(Debug)]
pub struct DbmsRuntime {
    credentials: CredentialVault,
    plugin_manager: Arc<PluginManager>,
    connections: RwLock<BTreeMap<String, Arc<ConnectionHandle>>>,
    requests: RwLock<BTreeMap<String, ActiveRequest>>,
    bootstrap_tickets: Mutex<BTreeMap<String, BootstrapTicketRecord>>,
    http: Client,
}

impl DbmsRuntime {
    pub fn new(plugin_manager: Arc<PluginManager>) -> Result<Arc<Self>, DbError> {
        let http = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| {
                DbError::new("58030", "failed to build administration client")
                    .with_detail(error.to_string())
            })?;
        Ok(Arc::new(Self {
            credentials: CredentialVault::new("OrdaDB/Console")?,
            plugin_manager,
            connections: RwLock::new(BTreeMap::new()),
            requests: RwLock::new(BTreeMap::new()),
            bootstrap_tickets: Mutex::new(BTreeMap::new()),
            http,
        }))
    }

    async fn prompt_and_store_credential(
        &self,
        request: PromptCredentialRequest,
    ) -> Result<Option<CredentialSaved>, DbError> {
        validate_id(&request.credential_id, "credential ID")?;
        if !VALID_CONNECTOR_IDS.contains(&request.connector_id.as_str()) {
            return Err(invalid("unknown connector ID"));
        }
        validate_text(
            &request.suggested_username,
            1,
            256,
            "suggested credential username",
        )?;
        let prompted =
            prompt_database_credential(request.connector_id, request.suggested_username, false)
                .await?;
        let Some(prompted) = prompted else {
            return Ok(None);
        };
        validate_text(&prompted.username, 1, 256, "credential username")?;
        validate_text(prompted.password.as_str(), 1, 1_024, "credential password")?;
        self.credentials.store(
            &request.credential_id,
            &prompted.username,
            &prompted.password,
        )?;
        Ok(Some(CredentialSaved {
            credential_id: request.credential_id,
            username: prompted.username,
        }))
    }

    fn connection(&self, connection_id: &str) -> Result<Arc<ConnectionHandle>, DbError> {
        validate_id(connection_id, "connection ID")?;
        read_lock(&self.connections)?
            .get(connection_id)
            .cloned()
            .ok_or_else(|| DbError::new("08003", "database connection does not exist"))
    }

    pub(crate) fn ai_connection_policy(
        &self,
        connection_id: &str,
    ) -> Result<AiConnectionPolicy, DbError> {
        let connection = self.connection(connection_id)?;
        Ok(AiConnectionPolicy {
            connector_kind: connection.connector_kind.clone(),
            command_language: connection.command_language.clone(),
            credential_access: connection.credential_access,
            native: matches!(&connection.transport, ConnectionTransport::Native(_)),
        })
    }

    async fn connect(&self, request: ConnectRequest) -> Result<ConnectionSnapshot, DbError> {
        validate_connect_request(&request)?;
        let stored = self.credentials.load(&request.credential_id)?;
        let connection_id = Uuid::new_v4().to_string();
        let database = request.database.clone().unwrap_or_else(|| {
            if request.connector_id == NATIVE_CONNECTOR_ID {
                "ordadb".to_owned()
            } else {
                String::new()
            }
        });

        let (transport, mode, capabilities) = if request.connector_id == NATIVE_CONNECTOR_ID {
            let address: SocketAddr = request.endpoint.parse().map_err(|_| {
                DbError::new(
                    "22023",
                    "native OrdaDB endpoint must be an IP socket address",
                )
            })?;
            let admin_endpoint =
                validate_admin_endpoint(request.admin_endpoint.as_deref().ok_or_else(|| {
                    DbError::new(
                        "22023",
                        "native OrdaDB connection requires an administration endpoint",
                    )
                })?)?;
            let username = stored.username.clone();
            let pg_password = Zeroizing::new(stored.password.to_string());
            let pg_database = database.clone();
            let pg = tokio::task::spawn_blocking(move || {
                PgClient::connect(ClientConfig {
                    address,
                    user: username,
                    database: pg_database,
                    password: pg_password,
                    application_name: "OrdaDB Console".into(),
                    query_memory_bytes: None,
                    timeout: None,
                })
            })
            .await
            .map_err(join_error)??;
            let cancel = pg.cancellation_token();
            let bearer = issue_admin_token(
                &self.http,
                &admin_endpoint,
                &stored.username,
                stored.password.as_str(),
            )
            .await?;
            let admin = AdminSession {
                base_url: admin_endpoint,
                bearer,
            };
            let health = admin_get::<Health>(&self.http, &admin, "/v1/health/ready", false).await?;
            if health.bootstrap_required || health.status != "ready" {
                return Err(DbError::new(
                    "55000",
                    "OrdaDB service is not ready for authenticated connections",
                ));
            }
            if health.version.is_empty() {
                return Err(DbError::new(
                    "08P01",
                    "OrdaDB health response has no server version",
                ));
            }
            let _uptime_seconds = health.uptime_seconds;
            (
                ConnectionTransport::Native(NativeConnection {
                    pg: Arc::new(Mutex::new(pg)),
                    cancel,
                    admin,
                    address,
                    database: database.clone(),
                    credential_id: request.credential_id.clone(),
                }),
                "native",
                DbmsCapabilities::native(),
            )
        } else {
            let mut host =
                ConnectorHost::launch(&self.plugin_manager, &request.connector_id).await?;
            let protocol_version = host.protocol_version();
            let username = stored.username;
            let secret = stored.password.to_string();
            let (capabilities_v3, capabilities) = match protocol_version {
                CONNECTOR_PROTOCOL_V3 => {
                    let endpoint =
                        host.structured_endpoint(&request.endpoint, request.database.clone())?;
                    let negotiated = host
                        .connect_v3(
                            connection_id.clone(),
                            endpoint,
                            request.tls_mode,
                            Some(ConnectorCredentialV2::new(Some(username), secret)),
                        )
                        .await?;
                    validate_negotiated_v3(&request, &negotiated)?;
                    let desktop = DbmsCapabilities::plugin_v3(&negotiated);
                    (Some(negotiated), desktop)
                }
                CONNECTOR_PROTOCOL_V2 => {
                    let endpoint =
                        host.structured_endpoint(&request.endpoint, request.database.clone())?;
                    let negotiated = host
                        .connect_v2(
                            connection_id.clone(),
                            endpoint,
                            request.tls_mode,
                            Some(ConnectorCredentialV2::new(Some(username), secret)),
                        )
                        .await?;
                    (None, DbmsCapabilities::plugin_v2(&negotiated))
                }
                _ => {
                    if request.connector_kind != "sql" {
                        return Err(DbError::unsupported(
                            "non-SQL connectors require connector protocol v3",
                        ));
                    }
                    host.connect(
                        connection_id.clone(),
                        request.endpoint.clone(),
                        request.database.clone(),
                        CredentialPayload::new(username, secret),
                    )
                    .await?;
                    (None, DbmsCapabilities::plugin())
                }
            };
            (
                ConnectionTransport::Plugin(Box::new(PluginConnection {
                    host: AsyncMutex::new(Some(host)),
                    capabilities_v3,
                })),
                "plugin",
                capabilities,
            )
        };

        let connector_kind = request.connector_kind.clone();
        let command_language = request.command_language.clone();
        let snapshot = ConnectionSnapshot {
            connection_id: connection_id.clone(),
            connector_id: request.connector_id,
            connector_kind: connector_kind.clone(),
            command_language: command_language.clone(),
            dialect: request.dialect,
            endpoint: request.endpoint,
            database,
            credential_access: request.credential_access,
            mode,
            capabilities,
        };
        let handle = Arc::new(ConnectionHandle {
            connector_kind,
            command_language,
            credential_access: request.credential_access,
            transport,
        });
        write_lock(&self.connections)?.insert(connection_id, handle);
        Ok(snapshot)
    }

    async fn probe_connection(&self, request: ConnectRequest) -> ConnectionProbe {
        let mut probe = ConnectionProbe::new();
        if let Err(error) = validate_connect_request(&request) {
            probe.failed(ConnectionProbeStageName::Service, error);
            for stage in [
                ConnectionProbeStageName::PgPort,
                ConnectionProbeStageName::AdminApi,
                ConnectionProbeStageName::Initialization,
                ConnectionProbeStageName::Authentication,
                ConnectionProbeStageName::Catalog,
            ] {
                probe.skipped(stage);
            }
            probe.finish();
            return probe;
        }
        if request.connector_id != NATIVE_CONNECTOR_ID {
            for stage in [
                ConnectionProbeStageName::Service,
                ConnectionProbeStageName::PgPort,
                ConnectionProbeStageName::AdminApi,
                ConnectionProbeStageName::Initialization,
                ConnectionProbeStageName::Authentication,
                ConnectionProbeStageName::Catalog,
            ] {
                probe.skipped(stage);
            }
            probe.ready = true;
            return probe;
        }

        let service_identity =
            match tauri::async_runtime::spawn_blocking(probe_windows_service).await {
                Ok(Ok(identity)) => {
                    probe.passed(ConnectionProbeStageName::Service);
                    Some(identity)
                }
                Ok(Err(error)) => {
                    probe.failed(ConnectionProbeStageName::Service, error);
                    None
                }
                Err(error) => {
                    probe.failed(
                        ConnectionProbeStageName::Service,
                        task_error("Windows service probe task failed", error),
                    );
                    None
                }
            };

        let address = match request.endpoint.parse::<SocketAddr>() {
            Ok(address) => {
                let socket_address = address;
                let reachable = match tauri::async_runtime::spawn_blocking(move || {
                    TcpStream::connect_timeout(&socket_address, Duration::from_secs(2))
                })
                .await
                {
                    Ok(Ok(_)) => {
                        probe.passed(ConnectionProbeStageName::PgPort);
                        true
                    }
                    Ok(Err(error)) => {
                        probe.failed(
                            ConnectionProbeStageName::PgPort,
                            network_error("PostgreSQL port is not reachable", error),
                        );
                        false
                    }
                    Err(error) => {
                        probe.failed(
                            ConnectionProbeStageName::PgPort,
                            task_error("PostgreSQL port probe task failed", error),
                        );
                        false
                    }
                };
                reachable.then_some(address)
            }
            Err(_) => {
                probe.failed(
                    ConnectionProbeStageName::PgPort,
                    invalid("native OrdaDB endpoint must be an IP socket address"),
                );
                None
            }
        };

        let admin_endpoint = match request
            .admin_endpoint
            .as_deref()
            .ok_or_else(|| invalid("native OrdaDB connection requires an administration endpoint"))
            .and_then(validate_admin_endpoint)
        {
            Ok(endpoint) => Some(endpoint),
            Err(error) => {
                probe.failed(ConnectionProbeStageName::AdminApi, error);
                probe.skipped(ConnectionProbeStageName::Initialization);
                None
            }
        };

        if let Some(endpoint) = &admin_endpoint {
            let public = AdminSession {
                base_url: endpoint.clone(),
                bearer: Zeroizing::new(String::new()),
            };
            match admin_get::<Health>(&self.http, &public, "/v1/health/live", false).await {
                Ok(health) if !health.status.is_empty() => {
                    probe.passed(ConnectionProbeStageName::AdminApi)
                }
                Ok(_) => probe.failed(
                    ConnectionProbeStageName::AdminApi,
                    DbError::new("08P01", "OrdaDB live health response is invalid"),
                ),
                Err(error) => probe.failed(ConnectionProbeStageName::AdminApi, error),
            }

            match admin_get::<Health>(&self.http, &public, "/v1/health/ready", false).await {
                Ok(health) if health.bootstrap_required => {
                    if let Some(identity) = &service_identity {
                        match self.issue_bootstrap_ticket(&request, identity) {
                            Ok(ticket) => {
                                probe.bootstrap_ticket = Some(ticket);
                                probe.failed(
                                    ConnectionProbeStageName::Initialization,
                                    DbError::new(
                                        "55000",
                                        "OrdaDB requires its first administrator",
                                    )
                                    .with_hint(
                                        "complete the local administrator setup, then retry",
                                    ),
                                );
                            }
                            Err(error) => {
                                probe.failed(ConnectionProbeStageName::Initialization, error);
                            }
                        }
                    } else {
                        probe.failed(
                            ConnectionProbeStageName::Initialization,
                            DbError::new(
                                "55000",
                                "OrdaDB bootstrap requires a verified local service",
                            ),
                        );
                    }
                }
                Ok(health) if health.status == "ready" && !health.version.is_empty() => {
                    probe.passed(ConnectionProbeStageName::Initialization)
                }
                Ok(_) => probe.failed(
                    ConnectionProbeStageName::Initialization,
                    DbError::new("55000", "OrdaDB service is not ready"),
                ),
                Err(error) => probe.failed(ConnectionProbeStageName::Initialization, error),
            }
        }

        let can_authenticate = address.is_some()
            && admin_endpoint.is_some()
            && probe.stages.iter().any(|stage| {
                stage.stage == ConnectionProbeStageName::AdminApi
                    && stage.status == ConnectionProbeStageStatus::Passed
            })
            && probe.stages.iter().any(|stage| {
                stage.stage == ConnectionProbeStageName::Initialization
                    && stage.status == ConnectionProbeStageStatus::Passed
            });
        let stored = if can_authenticate {
            match self.credentials.load(&request.credential_id) {
                Ok(stored) => Some(stored),
                Err(error) => {
                    probe.failed(ConnectionProbeStageName::Authentication, error);
                    None
                }
            }
        } else {
            probe.skipped(ConnectionProbeStageName::Authentication);
            None
        };
        let mut bearer = None;
        if let (Some(address), Some(endpoint), Some(stored)) =
            (address, admin_endpoint.as_deref(), stored)
        {
            let username = stored.username.clone();
            let password = Zeroizing::new(stored.password.to_string());
            let database = request
                .database
                .clone()
                .unwrap_or_else(|| "ordadb".to_owned());
            let pg_password = Zeroizing::new(stored.password.to_string());
            let pg_result = tauri::async_runtime::spawn_blocking(move || {
                PgClient::connect(ClientConfig {
                    address,
                    user: username,
                    database,
                    password: pg_password,
                    application_name: "OrdaDB Console Probe".into(),
                    query_memory_bytes: None,
                    timeout: None,
                })
            })
            .await;
            match pg_result {
                Ok(Ok(_)) => {
                    match issue_admin_token(&self.http, endpoint, &stored.username, &password).await
                    {
                        Ok(token) => {
                            bearer = Some(token);
                            probe.passed(ConnectionProbeStageName::Authentication);
                        }
                        Err(error) => probe.failed(ConnectionProbeStageName::Authentication, error),
                    }
                }
                Ok(Err(error)) => probe.failed(ConnectionProbeStageName::Authentication, error),
                Err(error) => probe.failed(
                    ConnectionProbeStageName::Authentication,
                    task_error("database authentication probe task failed", error),
                ),
            }
        } else if !probe
            .stages
            .iter()
            .any(|stage| stage.stage == ConnectionProbeStageName::Authentication)
        {
            probe.skipped(ConnectionProbeStageName::Authentication);
        }

        if let (Some(endpoint), Some(bearer)) = (admin_endpoint, bearer) {
            let session = AdminSession {
                base_url: endpoint,
                bearer,
            };
            match admin_get::<JsonValue>(&self.http, &session, "/v1/catalog", true).await {
                Ok(_) => probe.passed(ConnectionProbeStageName::Catalog),
                Err(error) => probe.failed(ConnectionProbeStageName::Catalog, error),
            }
        } else {
            probe.skipped(ConnectionProbeStageName::Catalog);
        }
        probe.finish();
        probe
    }

    fn issue_bootstrap_ticket(
        &self,
        request: &ConnectRequest,
        service: &ServiceIdentity,
    ) -> Result<LocalBootstrapTicket, DbError> {
        let token = Uuid::new_v4().to_string();
        let now = Instant::now();
        let mut tickets = mutex_lock(&self.bootstrap_tickets)?;
        tickets.retain(|_, record| record.expires_at > now);
        if tickets.len() >= MAX_BOOTSTRAP_TICKETS
            && let Some(oldest) = tickets
                .iter()
                .min_by_key(|(_, record)| record.expires_at)
                .map(|(ticket, _)| ticket.clone())
        {
            tickets.remove(&oldest);
        }
        tickets.insert(
            token.clone(),
            BootstrapTicketRecord {
                expires_at: now + BOOTSTRAP_TICKET_TTL,
                request_fingerprint: connection_fingerprint(request),
                service: service.clone(),
            },
        );
        Ok(LocalBootstrapTicket {
            ticket: token,
            expires_in_ms: BOOTSTRAP_TICKET_TTL.as_millis() as u64,
        })
    }

    fn consume_bootstrap_ticket(
        &self,
        request: &BootstrapAdminRequest,
    ) -> Result<BootstrapTicketRecord, DbError> {
        validate_id(&request.ticket, "local bootstrap ticket")?;
        let record = mutex_lock(&self.bootstrap_tickets)?
            .remove(&request.ticket)
            .ok_or_else(|| {
                DbError::new(
                    "55000",
                    "local bootstrap ticket is invalid or already consumed",
                )
                .with_hint("run the local OrdaDB connection probe again")
            })?;
        if record.expires_at <= Instant::now() {
            return Err(DbError::new("55000", "local bootstrap ticket expired")
                .with_hint("run the local OrdaDB connection probe again"));
        }
        if record.request_fingerprint != connection_fingerprint(&request.connection) {
            return Err(DbError::new(
                "28000",
                "local bootstrap ticket does not match this connection",
            ));
        }
        Ok(record)
    }

    async fn bootstrap_admin(
        &self,
        request: BootstrapAdminRequest,
    ) -> Result<BootstrapAdminResult, DbError> {
        validate_connect_request(&request.connection)?;
        if request.connection.connector_id != NATIVE_CONNECTOR_ID {
            return Err(invalid(
                "administrator bootstrap is available only for native OrdaDB",
            ));
        }
        validate_text(
            &request.suggested_username,
            1,
            128,
            "suggested administrator username",
        )?;
        let prompted = prompt_database_credential(
            NATIVE_CONNECTOR_ID.to_owned(),
            request.suggested_username.clone(),
            true,
        )
        .await?
        .ok_or_else(|| DbError::new("57014", "administrator credential prompt was cancelled"))?;
        validate_text(&prompted.username, 1, 128, "administrator username")?;
        validate_text(
            prompted.password.as_str(),
            8,
            1_024,
            "administrator password",
        )?;
        let record = self.consume_bootstrap_ticket(&request)?;
        let current = tauri::async_runtime::spawn_blocking(probe_windows_service)
            .await
            .map_err(|error| task_error("Windows service identity check failed", error))??;
        if current != record.service {
            return Err(
                DbError::new("55000", "OrdaDB service changed after the bootstrap probe")
                    .with_hint("run the local OrdaDB connection probe again"),
            );
        }
        let response = ordadb_server::request_bootstrap(
            &record.service.pipe_name,
            prompted.username.clone(),
            Zeroizing::new(prompted.password.to_string()),
        )
        .await?;
        if response.success {
            self.credentials.store(
                &request.connection.credential_id,
                &prompted.username,
                &prompted.password,
            )?;
        }
        Ok(BootstrapAdminResult {
            success: response.success,
            user: response.user,
            error: response.error.map(Into::into),
        })
    }

    async fn disconnect(&self, connection_id: &str) -> Result<(), DbError> {
        validate_id(connection_id, "connection ID")?;
        let requests = read_lock(&self.requests)?
            .values()
            .filter(|request| request.connection_id == connection_id)
            .map(|request| request.cancellation.clone())
            .collect::<Vec<_>>();
        for cancellation in requests {
            cancel_request(cancellation).await?;
        }
        let connection = write_lock(&self.connections)?
            .remove(connection_id)
            .ok_or_else(|| DbError::new("08003", "database connection does not exist"))?;
        if let ConnectionTransport::Plugin(plugin) = &connection.transport
            && let Some(host) = plugin.host.lock().await.take()
        {
            host.shutdown().await?;
        }
        Ok(())
    }

    pub(crate) async fn catalog(&self, connection_id: &str) -> Result<CatalogSnapshot, DbError> {
        let connection = self.connection(connection_id)?;
        let objects = match &connection.transport {
            ConnectionTransport::Native(native) => {
                let projection: JsonValue =
                    admin_get(&self.http, &native.admin, "/v1/catalog", true).await?;
                flatten_catalog(&projection)?
            }
            ConnectionTransport::Plugin(plugin) => {
                let capabilities_v3 = plugin.capabilities_v3.clone();
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                if let Some(capabilities) = capabilities_v3 {
                    connector_catalog_v3(host, connection_id, &capabilities).await?
                } else {
                    let request_id = Uuid::new_v4().to_string();
                    host.send(&ConnectorRequestV1::Catalog {
                        request_id: request_id.clone(),
                        connection_id: connection_id.to_owned(),
                    })
                    .await?;
                    match host.receive().await? {
                        ConnectorResponseV1::Catalog {
                            request_id: actual,
                            entries,
                        } if actual == request_id => {
                            entries.into_iter().map(catalog_entry).collect()
                        }
                        ConnectorResponseV1::Error { error, .. } => return Err(error),
                        _ => {
                            return Err(DbError::new(
                                "08P01",
                                "connector returned an unexpected catalog response",
                            ));
                        }
                    }
                }
            }
        };
        Ok(CatalogSnapshot {
            connection_id: connection_id.to_owned(),
            objects,
        })
    }

    fn start_execute(
        self: &Arc<Self>,
        app: AppHandle,
        request: ExecuteRequest,
    ) -> Result<OperationStarted, DbError> {
        validate_execute_request(&request)?;
        let connection = self.connection(&request.connection_id)?;
        validate_command_for_connection(
            &request.command,
            &connection.connector_kind,
            &connection.command_language,
        )?;
        let request_id = Uuid::new_v4().to_string();
        let cancellation = match &connection.transport {
            ConnectionTransport::Native(native) => {
                RequestCancellation::Native(native.cancel.clone())
            }
            ConnectionTransport::Plugin(_) => RequestCancellation::Plugin(CancellationToken::new()),
        };
        write_lock(&self.requests)?.insert(
            request_id.clone(),
            ActiveRequest {
                connection_id: request.connection_id.clone(),
                cancellation: cancellation.clone(),
            },
        );
        let runtime = Arc::clone(self);
        let task_request_id = request_id.clone();
        tauri::async_runtime::spawn(async move {
            let result = runtime
                .run_execute(&app, &task_request_id, connection, request, cancellation)
                .await;
            if let Err(error) = result {
                emit_query(
                    &app,
                    &task_request_id,
                    DbmsQueryEvent::Error {
                        error: error.into(),
                    },
                );
            }
            if let Ok(mut requests) = runtime.requests.write() {
                requests.remove(&task_request_id);
            }
        });
        Ok(OperationStarted { request_id })
    }

    async fn run_execute(
        &self,
        app: &AppHandle,
        request_id: &str,
        connection: Arc<ConnectionHandle>,
        request: ExecuteRequest,
        cancellation: RequestCancellation,
    ) -> Result<(), DbError> {
        let started = Instant::now();
        let ExecuteRequest {
            connection_id,
            command,
        } = request;
        match &connection.transport {
            ConnectionTransport::Native(native) => {
                let DesktopCommand::Text {
                    text: sql, params, ..
                } = command
                else {
                    return Err(DbError::unsupported(
                        "native OrdaDB accepts only SQL text commands",
                    ));
                };
                let client = Arc::clone(&native.pg);
                let params = params
                    .into_iter()
                    .map(|value| value.map(String::into_bytes))
                    .collect::<Vec<_>>();
                let task_app = app.clone();
                let task_request_id = request_id.to_owned();
                tokio::task::spawn_blocking(move || {
                    let mut client = mutex_lock(&client)?;
                    let mut processed = 0_u64;
                    let mut on_event = |event| {
                        emit_native_pg_event(&task_app, &task_request_id, event, &mut processed);
                        Ok(())
                    };
                    let summary = if params.is_empty() {
                        client.query_batches(&sql, QUERY_BATCH_ROWS, &mut on_event)?
                    } else {
                        client.query_prepared_batches(
                            &sql,
                            &[],
                            &params,
                            QUERY_BATCH_ROWS as u32,
                            &mut on_event,
                        )?
                    };
                    emit_query(
                        &task_app,
                        &task_request_id,
                        DbmsQueryEvent::Complete {
                            command_tag: if summary.command_tags.is_empty() {
                                "OK".into()
                            } else {
                                summary.command_tags.join(" · ")
                            },
                            duration_ms: elapsed_ms(started.elapsed()),
                        },
                    );
                    Ok::<(), DbError>(())
                })
                .await
                .map_err(join_error)??;
            }
            ConnectionTransport::Plugin(plugin) => {
                let RequestCancellation::Plugin(token) = cancellation else {
                    return Err(DbError::internal(
                        "plugin request received a native cancellation handle",
                    ));
                };
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                if plugin.capabilities_v3.is_some() {
                    run_connector_execute_v3(
                        host,
                        app,
                        request_id,
                        connection_id,
                        desktop_command_v3(command),
                        token,
                        started,
                    )
                    .await?;
                    return Ok(());
                }
                let DesktopCommand::Text {
                    text: sql, params, ..
                } = command
                else {
                    return Err(DbError::unsupported(
                        "connector protocols v1 and v2 accept only SQL text commands",
                    ));
                };
                host.send(&ConnectorRequestV1::Execute {
                    request_id: request_id.to_owned(),
                    connection_id,
                    sql,
                    params: params
                        .into_iter()
                        .map(|value| value.map_or(Value::Null, Value::Text))
                        .collect(),
                })
                .await?;
                let mut cancel_sent = false;
                loop {
                    let response = if cancel_sent {
                        host.receive().await?
                    } else {
                        tokio::select! {
                            response = host.receive() => response?,
                            () = token.cancelled() => {
                                host.send(&ConnectorRequestV1::Cancel {
                                    request_id: request_id.to_owned(),
                                }).await?;
                                cancel_sent = true;
                                continue;
                            }
                        }
                    };
                    match response {
                        ConnectorResponseV1::QueryEvent {
                            request_id: actual,
                            event,
                        } if actual == request_id => {
                            let terminal = matches!(event, QueryEvent::Complete(_));
                            emit_query(
                                app,
                                request_id,
                                map_connector_event(event, started.elapsed()),
                            );
                            if terminal {
                                break;
                            }
                        }
                        ConnectorResponseV1::Error {
                            request_id: actual,
                            error,
                        } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                            return Err(error);
                        }
                        _ => {
                            return Err(DbError::new(
                                "08P01",
                                "connector returned an unexpected query response",
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_ai_command(
        &self,
        connection_id: &str,
        command: DesktopCommand,
        limits: AiToolLimits,
        cancellation: CancellationToken,
        isolated_read: bool,
    ) -> Result<BoundedAiQueryResult, DbError> {
        if limits.max_rows == 0
            || limits.max_rows > 1_000
            || limits.max_result_bytes == 0
            || limits.max_result_bytes > 2 * 1024 * 1024
            || limits.query_memory_bytes == 0
            || limits.query_memory_bytes > 64 * 1024 * 1024
        {
            return Err(DbError::new(
                "22023",
                "AI query limits exceed the desktop safety contract",
            ));
        }
        let connection = self.connection(connection_id)?;
        validate_command_for_connection(
            &command,
            &connection.connector_kind,
            &connection.command_language,
        )?;
        match &connection.transport {
            ConnectionTransport::Native(native) => {
                let DesktopCommand::Text {
                    text: sql, params, ..
                } = command
                else {
                    return Err(DbError::unsupported(
                        "native OrdaDB accepts only SQL text commands",
                    ));
                };
                let stored = self.credentials.load(&native.credential_id)?;
                let config = ClientConfig {
                    address: native.address,
                    user: stored.username,
                    database: native.database.clone(),
                    password: stored.password,
                    application_name: "OrdaDB AI".to_owned(),
                    query_memory_bytes: Some(limits.query_memory_bytes),
                    timeout: Some(Duration::from_millis(limits.timeout_ms)),
                };
                let client = tokio::select! {
                    () = cancellation.cancelled() => return Err(ai_cancelled()),
                    client = tokio::task::spawn_blocking(move || PgClient::connect(config)) => {
                        client.map_err(join_error)??
                    }
                };
                let cancel = client.cancellation_token();
                let task = tokio::task::spawn_blocking(move || {
                    run_native_ai_query(client, sql, params, limits, isolated_read)
                });
                tokio::pin!(task);
                tokio::select! {
                    result = &mut task => result.map_err(join_error)?,
                    () = cancellation.cancelled() => {
                        let cancel_result = tokio::task::spawn_blocking(move || cancel.cancel())
                            .await
                            .map_err(join_error)?;
                        let _ = (&mut task).await;
                        cancel_result?;
                        Err(ai_cancelled())
                    }
                }
            }
            ConnectionTransport::Plugin(plugin) => {
                let mut collector = BoundedAiCollector::new(limits);
                let request_id = Uuid::new_v4().to_string();
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                if plugin.capabilities_v3.is_some() {
                    host.send_v3(&ConnectorRequestV3::Execute {
                        request_id: request_id.clone(),
                        connection_id: connection_id.to_owned(),
                        command: desktop_command_v3(command),
                        batch_size: QUERY_BATCH_ROWS as u32,
                    })
                    .await?;
                    let mut cancel_sent = false;
                    loop {
                        let response = if cancel_sent {
                            host.receive_v3().await?
                        } else {
                            tokio::select! {
                                response = host.receive_v3() => response?,
                                () = cancellation.cancelled() => {
                                    host.send_v3(&ConnectorRequestV3::Cancel {
                                        request_id: request_id.clone(),
                                    }).await?;
                                    cancel_sent = true;
                                    continue;
                                }
                            }
                        };
                        match response {
                            ConnectorResponseV3::ResultEvent {
                                request_id: actual,
                                event,
                            } if actual == request_id => {
                                let terminal =
                                    matches!(event, ConnectorResultEventV3::Complete { .. });
                                collector.push(map_connector_event_v3(event, Duration::ZERO)?);
                                if terminal {
                                    return collector.finish();
                                }
                            }
                            ConnectorResponseV3::Cancelled { request_id: actual }
                                if actual == request_id =>
                            {
                                return Err(ai_cancelled());
                            }
                            ConnectorResponseV3::Error {
                                request_id: actual,
                                error,
                            } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                                return Err(error.into_db_error());
                            }
                            _ => {
                                return Err(DbError::new(
                                    "08P01",
                                    "connector returned an unexpected v3 AI result response",
                                ));
                            }
                        }
                    }
                }
                let DesktopCommand::Text {
                    text: sql, params, ..
                } = command
                else {
                    return Err(DbError::unsupported(
                        "connector protocols v1 and v2 accept only SQL text commands",
                    ));
                };
                host.send(&ConnectorRequestV1::Execute {
                    request_id: request_id.clone(),
                    connection_id: connection_id.to_owned(),
                    sql,
                    params: params
                        .into_iter()
                        .map(|value| value.map_or(Value::Null, Value::Text))
                        .collect(),
                })
                .await?;
                let mut cancel_sent = false;
                loop {
                    let response = if cancel_sent {
                        host.receive().await?
                    } else {
                        tokio::select! {
                            response = host.receive() => response?,
                            () = cancellation.cancelled() => {
                                host.send(&ConnectorRequestV1::Cancel {
                                    request_id: request_id.clone(),
                                }).await?;
                                cancel_sent = true;
                                continue;
                            }
                        }
                    };
                    match response {
                        ConnectorResponseV1::QueryEvent {
                            request_id: actual,
                            event,
                        } if actual == request_id => {
                            let terminal = matches!(event, QueryEvent::Complete(_));
                            collector.push(map_connector_event(event, Duration::ZERO));
                            if terminal {
                                return collector.finish();
                            }
                        }
                        ConnectorResponseV1::Error {
                            request_id: actual,
                            error,
                        } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                            return Err(error);
                        }
                        _ => {
                            return Err(DbError::new(
                                "08P01",
                                "connector returned an unexpected AI query response",
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn cancel(&self, request_id: &str) -> Result<(), DbError> {
        validate_id(request_id, "request ID")?;
        let cancellation = read_lock(&self.requests)?
            .get(request_id)
            .map(|request| request.cancellation.clone());
        if let Some(cancellation) = cancellation {
            cancel_request(cancellation).await?;
        }
        Ok(())
    }

    async fn transaction(
        &self,
        connection_id: &str,
        action: TransactionAction,
    ) -> Result<CommandResult, DbError> {
        let connection = self.connection(connection_id)?;
        let sql = action.sql();
        match &connection.transport {
            ConnectionTransport::Native(native) => {
                let client = Arc::clone(&native.pg);
                let result = tokio::task::spawn_blocking(move || mutex_lock(&client)?.query(sql))
                    .await
                    .map_err(join_error)??;
                Ok(CommandResult {
                    command_tag: result
                        .command_tags
                        .last()
                        .cloned()
                        .unwrap_or_else(|| action.label().to_owned()),
                })
            }
            ConnectionTransport::Plugin(plugin) => {
                let request_id = Uuid::new_v4().to_string();
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                if plugin.capabilities_v3.is_some() {
                    host.send_v3(&action.connector_request_v3(&request_id, connection_id))
                        .await?;
                    return match host.receive_v3().await? {
                        ConnectorResponseV3::Transaction {
                            request_id: actual,
                            state,
                        } if actual == request_id => {
                            validate_transaction_state(action, state)?;
                            Ok(CommandResult {
                                command_tag: action.label().to_owned(),
                            })
                        }
                        ConnectorResponseV3::Error {
                            request_id: actual,
                            error,
                        } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                            Err(error.into_db_error())
                        }
                        _ => Err(DbError::new(
                            "08P01",
                            "connector returned an unexpected v3 transaction response",
                        )),
                    };
                }
                let request = action.connector_request(&request_id, connection_id);
                host.send(&request).await?;
                loop {
                    match host.receive().await? {
                        ConnectorResponseV1::QueryEvent {
                            request_id: actual,
                            event: QueryEvent::Complete(complete),
                        } if actual == request_id => {
                            return Ok(CommandResult {
                                command_tag: complete.tag,
                            });
                        }
                        ConnectorResponseV1::QueryEvent {
                            request_id: actual, ..
                        } if actual == request_id => {}
                        ConnectorResponseV1::Error { error, .. } => return Err(error),
                        _ => {
                            return Err(DbError::new(
                                "08P01",
                                "connector returned an unexpected transaction response",
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn monitor(&self, connection_id: &str) -> Result<MonitorSnapshot, DbError> {
        let connection = self.connection(connection_id)?;
        match &connection.transport {
            ConnectionTransport::Native(native) => {
                let sessions = admin_get(&self.http, &native.admin, "/v1/sessions", true);
                let queries = admin_get(&self.http, &native.admin, "/v1/queries", true);
                let locks = admin_get(&self.http, &native.admin, "/v1/locks", true);
                let metrics = admin_get(&self.http, &native.admin, "/v1/metrics", true);
                let storage = admin_get(&self.http, &native.admin, "/v1/storage", true);
                let wal = admin_get(&self.http, &native.admin, "/v1/wal", true);
                let config = admin_get(&self.http, &native.admin, "/v1/config", true);
                let (sessions, queries, locks, metrics, storage, wal, config) =
                    tokio::try_join!(sessions, queries, locks, metrics, storage, wal, config)?;
                Ok(MonitorSnapshot {
                    connection_id: connection_id.to_owned(),
                    sessions,
                    queries,
                    locks,
                    metrics,
                    storage,
                    wal,
                    backups: CapabilityStatus {
                        supported: true,
                        reason: String::new(),
                    },
                    config,
                })
            }
            ConnectionTransport::Plugin(plugin) => {
                let request_id = Uuid::new_v4().to_string();
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                host.send(&ConnectorRequestV1::Monitor {
                    request_id: request_id.clone(),
                    connection_id: connection_id.to_owned(),
                })
                .await?;
                match host.receive().await? {
                    ConnectorResponseV1::Monitor {
                        request_id: actual,
                        sessions,
                        active_queries,
                    } if actual == request_id => Ok(MonitorSnapshot {
                        connection_id: connection_id.to_owned(),
                        sessions: Vec::new(),
                        queries: Vec::new(),
                        locks: LockStatus {
                            single_writer: false,
                            active_locks: Vec::new(),
                        },
                        metrics: Metrics {
                            active_sessions: sessions as usize,
                            active_queries: active_queries as usize,
                            engine: empty_engine_status(),
                        },
                        storage: empty_engine_status(),
                        wal: empty_engine_status(),
                        backups: CapabilityStatus {
                            supported: false,
                            reason: "connector does not expose OrdaDB backup administration".into(),
                        },
                        config: PublicConfig {
                            data_dir: String::new(),
                            pg_bind: String::new(),
                            admin_bind: String::new(),
                            remote_requires_tls: true,
                        },
                    }),
                    ConnectorResponseV1::Error { error, .. } => Err(error),
                    _ => Err(DbError::new(
                        "08P01",
                        "connector returned an unexpected monitor response",
                    )),
                }
            }
        }
    }

    pub(crate) async fn checkpoint(&self, connection_id: &str) -> Result<EngineStatus, DbError> {
        let connection = self.connection(connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "checkpoint is available only for native OrdaDB connections",
            ));
        };
        let completed: CheckpointResult =
            admin_post(&self.http, &native.admin, "/v1/checkpoint").await?;
        if !completed.completed {
            return Err(DbError::internal(
                "administration API returned an incomplete checkpoint",
            ));
        }
        admin_get(&self.http, &native.admin, "/v1/storage", true).await
    }

    async fn administration_operations(
        &self,
        connection_id: &str,
    ) -> Result<Vec<AdministrationOperation>, DbError> {
        let connection = self.connection(connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "administration operations are available only for native OrdaDB connections",
            ));
        };
        let operations: Vec<AdministrationOperationResponse> =
            admin_get(&self.http, &native.admin, "/v1/operations", true).await?;
        Ok(operations.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn start_administration_operation(
        &self,
        request: StartAdministrationOperationRequest,
    ) -> Result<AdministrationOperation, DbError> {
        validate_administration_operation_request(&request)?;
        let connection = self.connection(&request.connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "administration operations are available only for native OrdaDB connections",
            ));
        };
        let body = if request.kind.requires_table() {
            serde_json::json!({
                "path": request.path,
                "schema": request.schema,
                "table": request.table,
                "format": request.format,
            })
        } else {
            serde_json::json!({ "path": request.path })
        };
        let operation: AdministrationOperationResponse =
            admin_post_json(&self.http, &native.admin, request.kind.endpoint(), &body).await?;
        Ok(operation.into())
    }

    async fn administration_operation(
        &self,
        connection_id: &str,
        operation_id: &str,
    ) -> Result<AdministrationOperation, DbError> {
        let operation_id = validate_operation_id(operation_id)?;
        let connection = self.connection(connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "administration operations are available only for native OrdaDB connections",
            ));
        };
        let operation: AdministrationOperationResponse = admin_get(
            &self.http,
            &native.admin,
            &format!("/v1/operations/{operation_id}"),
            true,
        )
        .await?;
        Ok(operation.into())
    }

    async fn cancel_administration_operation(
        &self,
        connection_id: &str,
        operation_id: &str,
    ) -> Result<AdministrationOperation, DbError> {
        let operation_id = validate_operation_id(operation_id)?;
        let connection = self.connection(connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "administration operations are available only for native OrdaDB connections",
            ));
        };
        let operation: AdministrationOperationResponse = admin_post(
            &self.http,
            &native.admin,
            &format!("/v1/operations/{operation_id}/cancel"),
        )
        .await?;
        Ok(operation.into())
    }

    pub(crate) async fn administration_service(
        &self,
        connection_id: &str,
    ) -> Result<AdministrationServiceStatus, DbError> {
        let connection = self.connection(connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "service status is available only for native OrdaDB connections",
            ));
        };
        admin_get(&self.http, &native.admin, "/v1/service", true).await
    }
}

#[derive(Debug, Clone, Copy)]
enum TransactionAction {
    Begin,
    Commit,
    Rollback,
}

impl TransactionAction {
    const fn sql(self) -> &'static str {
        match self {
            Self::Begin => "BEGIN",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Begin => "BEGIN",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
        }
    }

    fn connector_request(self, request_id: &str, connection_id: &str) -> ConnectorRequestV1 {
        match self {
            Self::Begin => ConnectorRequestV1::Begin {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
            },
            Self::Commit => ConnectorRequestV1::Commit {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
            },
            Self::Rollback => ConnectorRequestV1::Rollback {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
            },
        }
    }

    fn connector_request_v3(self, request_id: &str, connection_id: &str) -> ConnectorRequestV3 {
        match self {
            Self::Begin => ConnectorRequestV3::Begin {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
                isolation: None,
            },
            Self::Commit => ConnectorRequestV3::Commit {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
            },
            Self::Rollback => ConnectorRequestV3::Rollback {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
            },
        }
    }
}

fn validate_transaction_state(
    action: TransactionAction,
    state: ConnectorTransactionStateV2,
) -> Result<(), DbError> {
    let valid = matches!(
        (action, state),
        (
            TransactionAction::Begin,
            ConnectorTransactionStateV2::Active
        ) | (
            TransactionAction::Commit | TransactionAction::Rollback,
            ConnectorTransactionStateV2::Idle
        )
    );
    if valid {
        Ok(())
    } else if state == ConnectorTransactionStateV2::Failed {
        Err(DbError::new(
            "25P02",
            "connector transaction entered the failed state",
        ))
    } else {
        Err(DbError::new(
            "08P01",
            "connector returned an unexpected transaction state",
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Health {
    status: String,
    version: String,
    uptime_seconds: u64,
    bootstrap_required: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TokenRequest<'a> {
    username: &'a str,
    password: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointResult {
    completed: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiEnvelope<T> {
    api_version: String,
    data: T,
}

#[derive(Deserialize)]
struct ApiErrorEnvelope {
    error: DbError,
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_prompt_credential(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: PromptCredentialRequest,
) -> DesktopResult<Option<CredentialSaved>> {
    runtime
        .prompt_and_store_credential(request)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn dbms_delete_credential(
    runtime: State<'_, Arc<DbmsRuntime>>,
    credential_id: String,
) -> DesktopResult<()> {
    runtime
        .credentials
        .delete(&credential_id)
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_connect(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: ConnectRequest,
) -> DesktopResult<ConnectionSnapshot> {
    runtime.connect(request).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_probe_connection(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: ConnectRequest,
) -> DesktopResult<ConnectionProbe> {
    Ok(runtime.probe_connection(request).await)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_bootstrap_admin(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: BootstrapAdminRequest,
) -> DesktopResult<BootstrapAdminResult> {
    runtime.bootstrap_admin(request).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_disconnect(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<()> {
    runtime.disconnect(&connection_id).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_catalog(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<CatalogSnapshot> {
    runtime.catalog(&connection_id).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn dbms_execute(
    app: AppHandle,
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: ExecuteRequest,
) -> DesktopResult<OperationStarted> {
    runtime
        .inner()
        .start_execute(app, request)
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_cancel(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request_id: String,
) -> DesktopResult<()> {
    runtime.cancel(&request_id).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_begin(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<CommandResult> {
    runtime
        .transaction(&connection_id, TransactionAction::Begin)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_commit(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<CommandResult> {
    runtime
        .transaction(&connection_id, TransactionAction::Commit)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_rollback(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<CommandResult> {
    runtime
        .transaction(&connection_id, TransactionAction::Rollback)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_monitor(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<MonitorSnapshot> {
    runtime.monitor(&connection_id).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_checkpoint(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<EngineStatus> {
    runtime.checkpoint(&connection_id).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_operations(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<Vec<AdministrationOperation>> {
    runtime
        .administration_operations(&connection_id)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_start_operation(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: StartAdministrationOperationRequest,
) -> DesktopResult<AdministrationOperation> {
    runtime
        .start_administration_operation(request)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_operation(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
    operation_id: String,
) -> DesktopResult<AdministrationOperation> {
    runtime
        .administration_operation(&connection_id, &operation_id)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_cancel_operation(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
    operation_id: String,
) -> DesktopResult<AdministrationOperation> {
    runtime
        .cancel_administration_operation(&connection_id, &operation_id)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_service(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<AdministrationServiceStatus> {
    runtime
        .administration_service(&connection_id)
        .await
        .map_err(Into::into)
}

#[derive(Debug, Clone, Copy)]
struct ConnectorContract {
    kind: &'static str,
    command_language: &'static str,
    dialect: Option<&'static str>,
}

fn connector_contract(connector_id: &str) -> Option<ConnectorContract> {
    let contract = match connector_id {
        NATIVE_CONNECTOR_ID | "postgresql" => ConnectorContract {
            kind: "sql",
            command_language: "postgresql-sql",
            dialect: Some("postgresql"),
        },
        "mysql" => ConnectorContract {
            kind: "sql",
            command_language: "mysql-sql",
            dialect: Some("mysql"),
        },
        "sqlite" => ConnectorContract {
            kind: "sql",
            command_language: "sqlite-sql",
            dialect: Some("sqlite"),
        },
        "sql-server" => ConnectorContract {
            kind: "sql",
            command_language: "sql-server-sql",
            dialect: Some("sqlServer"),
        },
        "mongodb" => ConnectorContract {
            kind: "document",
            command_language: "mongodb-json",
            dialect: None,
        },
        "redis" => ConnectorContract {
            kind: "keyValue",
            command_language: "redis-resp3",
            dialect: None,
        },
        "mariadb" => ConnectorContract {
            kind: "sql",
            command_language: "mariadb-sql",
            dialect: Some("mariadb"),
        },
        "clickhouse" => ConnectorContract {
            kind: "sql",
            command_language: "clickhouse-sql",
            dialect: Some("clickhouse"),
        },
        "oracle" => ConnectorContract {
            kind: "sql",
            command_language: "oracle-sql",
            dialect: Some("oracle"),
        },
        _ => return None,
    };
    Some(contract)
}

fn validate_connect_request(request: &ConnectRequest) -> Result<(), DbError> {
    let contract = connector_contract(&request.connector_id)
        .ok_or_else(|| DbError::new("22023", "unknown connector ID"))?;
    if request.connector_kind != contract.kind
        || request.command_language != contract.command_language
        || request.dialect.as_deref() != contract.dialect
    {
        return Err(DbError::new(
            "22023",
            "connection metadata does not match the connector identity",
        ));
    }
    validate_text(&request.endpoint, 1, 2_048, "connection endpoint")?;
    validate_text(
        &request.command_language,
        1,
        64,
        "connector command language",
    )?;
    validate_id(&request.credential_id, "credential ID")?;
    if let Some(database) = &request.database {
        validate_text(database, 1, 256, "database name")?;
    }
    if let Some(admin_endpoint) = &request.admin_endpoint {
        validate_text(admin_endpoint, 1, 2_048, "administration endpoint")?;
    }
    if request.connector_id == NATIVE_CONNECTOR_ID
        && (request.admin_endpoint.is_none() || request.tls_mode != ConnectorTlsModeV2::Disable)
    {
        return Err(DbError::new(
            "22023",
            "native OrdaDB requires its administration endpoint and local TLS mode",
        ));
    }
    if request.connector_id != NATIVE_CONNECTOR_ID && request.admin_endpoint.is_some() {
        return Err(DbError::new(
            "22023",
            "external connectors do not accept an OrdaDB administration endpoint",
        ));
    }
    Ok(())
}

fn validate_negotiated_v3(
    request: &ConnectRequest,
    capabilities: &ConnectorCapabilitiesV3,
) -> Result<(), DbError> {
    let expected_kind = match request.connector_kind.as_str() {
        "sql" => ConnectorKindV3::Sql,
        "document" => ConnectorKindV3::Document,
        "keyValue" => ConnectorKindV3::KeyValue,
        _ => return Err(DbError::new("22023", "unknown connector kind")),
    };
    if capabilities.kind != expected_kind {
        return Err(DbError::new(
            "08P01",
            "connector negotiated a different data model than its profile",
        ));
    }
    if !capabilities
        .command_languages
        .iter()
        .any(|language| language.id == request.command_language)
    {
        return Err(DbError::new(
            "08P01",
            "connector did not negotiate the configured command language",
        ));
    }
    Ok(())
}

fn validate_administration_operation_request(
    request: &StartAdministrationOperationRequest,
) -> Result<(), DbError> {
    validate_id(&request.connection_id, "connection ID")?;
    validate_operation_path(&request.path)?;
    if request.kind.requires_table() {
        let schema = request
            .schema
            .as_deref()
            .ok_or_else(|| DbError::new("22023", "table operation requires a schema"))?;
        let table = request
            .table
            .as_deref()
            .ok_or_else(|| DbError::new("22023", "table operation requires a table"))?;
        validate_text(schema, 1, 256, "schema name")?;
        validate_text(table, 1, 256, "table name")?;
        if request.format.is_none() {
            return Err(DbError::new(
                "22023",
                "table operation requires CSV or JSON Lines format",
            ));
        }
    } else if request.schema.is_some() || request.table.is_some() || request.format.is_some() {
        return Err(DbError::new(
            "22023",
            "backup and restore requests do not accept table fields",
        ));
    }
    Ok(())
}

fn validate_operation_path(value: &str) -> Result<(), DbError> {
    validate_text(value, 1, 512, "operation path")?;
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(DbError::new(
            "22023",
            "operation path must be relative to the server operations root",
        ));
    }
    Ok(())
}

fn validate_operation_id(value: &str) -> Result<Uuid, DbError> {
    value
        .parse()
        .map_err(|_| DbError::new("22023", "operation ID must be a UUID"))
}

fn validate_execute_request(request: &ExecuteRequest) -> Result<(), DbError> {
    validate_id(&request.connection_id, "connection ID")?;
    match &request.command {
        DesktopCommand::Text {
            language_id,
            text,
            params,
        } => {
            validate_text(language_id, 1, 64, "connector command language")?;
            validate_text(text, 1, 4 * 1024 * 1024, "command text")?;
            if params.len() > 65_535 {
                return Err(DbError::new("54000", "parameter count exceeds 65,535"));
            }
        }
        DesktopCommand::Document {
            language_id,
            document,
        } => {
            validate_text(language_id, 1, 64, "connector command language")?;
            let encoded = serde_json::to_vec(document)
                .map_err(|error| DbError::internal(error.to_string()))?;
            if encoded.len() > MAX_CONNECTOR_TEXT_BYTES {
                return Err(DbError::new(
                    "54000",
                    "document command exceeds the connector size limit",
                ));
            }
        }
        DesktopCommand::Arguments {
            language_id,
            arguments,
        } => {
            validate_text(language_id, 1, 64, "connector command language")?;
            if arguments.is_empty() || arguments.len() > MAX_CONNECTOR_COMMAND_ARGUMENTS {
                return Err(DbError::new(
                    "54000",
                    "argument command count is outside the connector limit",
                ));
            }
            let total_bytes = arguments.iter().try_fold(0_usize, |total, argument| {
                total
                    .checked_add(argument.len())
                    .ok_or_else(|| DbError::new("54000", "argument command size overflowed"))
            })?;
            if total_bytes > MAX_CONNECTOR_TEXT_BYTES {
                return Err(DbError::new(
                    "54000",
                    "argument command exceeds the connector size limit",
                ));
            }
        }
    }
    Ok(())
}

fn validate_command_for_connection(
    command: &DesktopCommand,
    connector_kind: &str,
    command_language: &str,
) -> Result<(), DbError> {
    let (actual_kind, actual_language) = match command {
        DesktopCommand::Text { language_id, .. } => ("sql", language_id),
        DesktopCommand::Document { language_id, .. } => ("document", language_id),
        DesktopCommand::Arguments { language_id, .. } => ("keyValue", language_id),
    };
    if actual_kind != connector_kind || actual_language != command_language {
        return Err(DbError::new(
            "22023",
            "command shape or language does not match the active connection",
        ));
    }
    Ok(())
}

fn validate_id(value: &str, context: &str) -> Result<(), DbError> {
    if value.is_empty()
        || value.len() > 128
        || value.starts_with('.')
        || value.ends_with('.')
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(DbError::new(
            "22023",
            format!(
                "{context} must use 1-128 ASCII letters, digits, dots, hyphens, or underscores"
            ),
        ));
    }
    Ok(())
}

fn validate_text(
    value: &str,
    minimum: usize,
    maximum: usize,
    context: &str,
) -> Result<(), DbError> {
    if !(minimum..=maximum).contains(&value.len())
        || value.contains('\0')
        || value.chars().any(char::is_control)
    {
        return Err(DbError::new(
            "22023",
            format!("{context} must contain {minimum}-{maximum} printable UTF-8 bytes"),
        ));
    }
    Ok(())
}

fn validate_admin_endpoint(value: &str) -> Result<String, DbError> {
    validate_text(value, 1, 2_048, "administration endpoint")?;
    let mut url = Url::parse(value).map_err(|error| {
        DbError::new("22023", "administration endpoint is invalid").with_detail(error.to_string())
    })?;
    if url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DbError::new(
            "22023",
            "administration endpoint must not contain credentials, query, or fragment",
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| DbError::new("22023", "administration endpoint has no host"))?;
    if url.scheme() == "http" && !is_loopback_host(host) {
        return Err(DbError::new(
            "22023",
            "remote administration endpoints require HTTPS",
        ));
    }
    if !matches!(url.scheme(), "http" | "https") {
        return Err(DbError::new(
            "22023",
            "administration endpoint must use HTTP or HTTPS",
        ));
    }
    url.set_path("");
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

async fn issue_admin_token(
    client: &Client,
    base_url: &str,
    username: &str,
    password: &str,
) -> Result<Zeroizing<String>, DbError> {
    let url = format!("{base_url}/v1/auth/token");
    let response = client
        .post(url)
        .json(&TokenRequest { username, password })
        .send()
        .await
        .map_err(|error| network_error("administration authentication failed", error))?;
    let envelope: ApiEnvelope<TokenResponse> = decode_admin_response(response).await?;
    validate_api_version(&envelope.api_version)?;
    if envelope.data.token_type != "Bearer" || envelope.data.expires_in_seconds == 0 {
        return Err(DbError::new(
            "08P01",
            "administration token response is invalid",
        ));
    }
    Ok(Zeroizing::new(envelope.data.access_token))
}

async fn admin_get<T: DeserializeOwned>(
    client: &Client,
    session: &AdminSession,
    path: &str,
    authenticated: bool,
) -> Result<T, DbError> {
    admin_request(client, session, Method::GET, path, authenticated).await
}

async fn admin_post<T: DeserializeOwned>(
    client: &Client,
    session: &AdminSession,
    path: &str,
) -> Result<T, DbError> {
    admin_request(client, session, Method::POST, path, true).await
}

async fn admin_post_json<T: DeserializeOwned>(
    client: &Client,
    session: &AdminSession,
    path: &str,
    body: &JsonValue,
) -> Result<T, DbError> {
    let response = client
        .post(format!("{}{}", session.base_url, path))
        .bearer_auth(session.bearer.as_str())
        .json(body)
        .send()
        .await
        .map_err(|error| network_error("administration request failed", error))?;
    let envelope: ApiEnvelope<T> = decode_admin_response(response).await?;
    validate_api_version(&envelope.api_version)?;
    Ok(envelope.data)
}

async fn admin_request<T: DeserializeOwned>(
    client: &Client,
    session: &AdminSession,
    method: Method,
    path: &str,
    authenticated: bool,
) -> Result<T, DbError> {
    let mut request = client.request(method, format!("{}{}", session.base_url, path));
    if authenticated {
        request = request.bearer_auth(session.bearer.as_str());
    }
    let response = request
        .send()
        .await
        .map_err(|error| network_error("administration request failed", error))?;
    let envelope: ApiEnvelope<T> = decode_admin_response(response).await?;
    validate_api_version(&envelope.api_version)?;
    Ok(envelope.data)
}

async fn decode_admin_response<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, DbError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_ADMIN_RESPONSE_BYTES)
    {
        return Err(DbError::new(
            "54000",
            "administration response exceeds 8 MiB",
        ));
    }
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|error| network_error("failed to read administration response", error))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_ADMIN_RESPONSE_BYTES {
        return Err(DbError::new(
            "54000",
            "administration response exceeds 8 MiB",
        ));
    }
    if !status.is_success() {
        return serde_json::from_slice::<ApiErrorEnvelope>(&bytes)
            .map(|envelope| envelope.error)
            .map_err(|error| {
                DbError::new(
                    "08P01",
                    format!("administration API returned HTTP {status}"),
                )
                .with_detail(error.to_string())
            })
            .and_then(Err);
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        DbError::new("08P01", "administration response is invalid JSON")
            .with_detail(error.to_string())
    })
}

fn validate_api_version(version: &str) -> Result<(), DbError> {
    if version == "v1" {
        Ok(())
    } else {
        Err(DbError::new(
            "0A000",
            format!("administration API version {version} is unsupported"),
        ))
    }
}

async fn connector_catalog_v3(
    host: &mut ConnectorHost,
    connection_id: &str,
    capabilities: &ConnectorCapabilitiesV3,
) -> Result<Vec<CatalogObject>, DbError> {
    if !capabilities.catalog {
        return Err(DbError::unsupported("connector Catalog discovery"));
    }
    let page_size = capabilities
        .maximum_catalog_page_size
        .min(MAX_CONNECTOR_CATALOG_PAGE_NODES)
        .min(1_024);
    if page_size == 0 {
        return Err(DbError::new(
            "08P01",
            "connector negotiated a zero Catalog page size",
        ));
    }
    let mut pending_parents = VecDeque::from([None]);
    let mut seen = BTreeSet::new();
    let mut objects = Vec::new();
    while let Some(parent_id) = pending_parents.pop_front() {
        let mut cursor = None;
        loop {
            let request_id = Uuid::new_v4().to_string();
            host.send_v3(&ConnectorRequestV3::Catalog {
                request_id: request_id.clone(),
                connection_id: connection_id.to_owned(),
                parent_id: parent_id.clone(),
                page_size,
                cursor: cursor.take(),
            })
            .await?;
            let page = match host.receive_v3().await? {
                ConnectorResponseV3::CatalogPage {
                    request_id: actual,
                    page,
                } if actual == request_id => page,
                ConnectorResponseV3::Error {
                    request_id: actual,
                    error,
                } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                    return Err(error.into_db_error());
                }
                _ => {
                    return Err(DbError::new(
                        "08P01",
                        "connector returned an unexpected v3 Catalog response",
                    ));
                }
            };
            cursor = page.next_cursor;
            for node in page.nodes {
                if node.parent_id != parent_id {
                    return Err(DbError::new(
                        "08P01",
                        "connector Catalog node does not belong to the requested parent",
                    ));
                }
                if !seen.insert(node.id.clone()) {
                    return Err(DbError::new(
                        "08P01",
                        "connector Catalog contains a duplicate node ID",
                    ));
                }
                if objects.len() >= MAX_DESKTOP_CATALOG_NODES {
                    return Err(DbError::new(
                        "54000",
                        "connector Catalog exceeds the desktop node limit",
                    ));
                }
                if node.has_children {
                    pending_parents.push_back(Some(node.id.clone()));
                }
                objects.push(catalog_node_v3(node)?);
            }
            if cursor.is_none() {
                break;
            }
        }
    }
    Ok(objects)
}

fn catalog_node_v3(node: ConnectorCatalogNodeV3) -> Result<CatalogObject, DbError> {
    let details = serde_json::to_value(&node).map_err(|error| {
        DbError::internal("failed to project connector Catalog node").with_detail(error.to_string())
    })?;
    Ok(CatalogObject {
        id: Some(node.id),
        kind: catalog_node_kind_v3(node.kind).into(),
        schema: node.namespace.clone().unwrap_or_default(),
        namespace: node.namespace,
        name: node.name,
        parent: node.parent_id,
        details,
    })
}

const fn catalog_node_kind_v3(kind: ConnectorCatalogNodeKindV3) -> &'static str {
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

fn flatten_catalog(projection: &JsonValue) -> Result<Vec<CatalogObject>, DbError> {
    let database = projection
        .get("database")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| DbError::new("08P01", "catalog response has no database"))?;
    let database_name = identifier(
        database
            .get("name")
            .ok_or_else(|| DbError::new("08P01", "catalog database has no name"))?,
    )?;
    let mut objects = vec![CatalogObject {
        id: None,
        kind: "database".into(),
        schema: String::new(),
        namespace: None,
        name: database_name.clone(),
        parent: None,
        details: JsonValue::Object(database.clone()),
    }];
    for schema in json_array(database.get("schemas"), "catalog schemas")? {
        let schema_object = schema
            .as_object()
            .ok_or_else(|| DbError::new("08P01", "catalog schema is not an object"))?;
        let schema_name = identifier(
            schema_object
                .get("name")
                .ok_or_else(|| DbError::new("08P01", "catalog schema has no name"))?,
        )?;
        objects.push(CatalogObject {
            id: None,
            kind: "schema".into(),
            schema: schema_name.clone(),
            namespace: Some(schema_name.clone()),
            name: schema_name.clone(),
            parent: Some(database_name.clone()),
            details: schema.clone(),
        });
        flatten_named(
            &mut objects,
            schema_object.get("tables"),
            "table",
            &schema_name,
            None,
        )?;
        flatten_named(
            &mut objects,
            schema_object.get("sequences"),
            "sequence",
            &schema_name,
            None,
        )?;
        flatten_views(&mut objects, schema_object.get("views"), &schema_name)?;
        flatten_named(
            &mut objects,
            schema_object.get("routines"),
            "routine",
            &schema_name,
            None,
        )?;
        for table in json_array(schema_object.get("tables"), "catalog tables")? {
            let table_name = identifier(
                table
                    .get("name")
                    .ok_or_else(|| DbError::new("08P01", "catalog table has no name"))?,
            )?;
            for (field, kind) in [
                ("indexes", "index"),
                ("constraints", "constraint"),
                ("triggers", "trigger"),
            ] {
                flatten_named(
                    &mut objects,
                    table.get(field),
                    kind,
                    &schema_name,
                    Some(&table_name),
                )?;
            }
        }
    }
    Ok(objects)
}

fn flatten_views(
    objects: &mut Vec<CatalogObject>,
    value: Option<&JsonValue>,
    schema: &str,
) -> Result<(), DbError> {
    for view in json_array(value, "catalog views")? {
        let kind = view
            .get("kind")
            .and_then(JsonValue::as_str)
            .filter(|kind| kind.to_ascii_lowercase().contains("material"))
            .map_or("view", |_| "materializedView");
        let name = identifier(
            view.get("name")
                .ok_or_else(|| DbError::new("08P01", "catalog view has no name"))?,
        )?;
        objects.push(CatalogObject {
            id: None,
            kind: kind.into(),
            schema: schema.into(),
            namespace: Some(schema.into()),
            name,
            parent: None,
            details: view.clone(),
        });
    }
    Ok(())
}

fn flatten_named(
    objects: &mut Vec<CatalogObject>,
    value: Option<&JsonValue>,
    kind: &str,
    schema: &str,
    parent: Option<&str>,
) -> Result<(), DbError> {
    for entry in json_array(value, "catalog object collection")? {
        let name = identifier(
            entry
                .get("name")
                .ok_or_else(|| DbError::new("08P01", "catalog object has no name"))?,
        )?;
        objects.push(CatalogObject {
            id: None,
            kind: kind.into(),
            schema: schema.into(),
            namespace: Some(schema.into()),
            name,
            parent: parent.map(str::to_owned),
            details: entry.clone(),
        });
    }
    Ok(())
}

fn json_array<'a>(value: Option<&'a JsonValue>, context: &str) -> Result<&'a [JsonValue], DbError> {
    value
        .and_then(JsonValue::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| DbError::new("08P01", format!("{context} is not an array")))
}

fn identifier(value: &JsonValue) -> Result<String, DbError> {
    let encoded = value
        .as_str()
        .ok_or_else(|| DbError::new("08P01", "catalog identifier is not a string"))?;
    encoded
        .strip_prefix("u:")
        .or_else(|| encoded.strip_prefix("q:"))
        .map(str::to_owned)
        .ok_or_else(|| DbError::new("08P01", "catalog identifier has no version marker"))
}

fn catalog_entry(entry: CatalogEntry) -> CatalogObject {
    CatalogObject {
        id: None,
        kind: entry.kind,
        namespace: (!entry.schema.is_empty()).then(|| entry.schema.clone()),
        schema: entry.schema,
        name: entry.name,
        parent: None,
        details: JsonValue::Null,
    }
}

fn run_native_ai_query(
    mut client: PgClient,
    sql: String,
    params: Vec<Option<String>>,
    limits: AiToolLimits,
    isolated_read: bool,
) -> Result<BoundedAiQueryResult, DbError> {
    if isolated_read {
        client.query("BEGIN TRANSACTION READ ONLY")?;
    }
    let mut collector = BoundedAiCollector::new(limits);
    let mut processed = 0_u64;
    let mut on_event = |event| {
        collect_native_pg_event(&mut collector, event, &mut processed);
        Ok(())
    };
    let params = params
        .into_iter()
        .map(|value| value.map(String::into_bytes))
        .collect::<Vec<_>>();
    let query_result = if params.is_empty() {
        client.query_batches(&sql, QUERY_BATCH_ROWS, &mut on_event)
    } else {
        client.query_prepared_batches(&sql, &[], &params, QUERY_BATCH_ROWS as u32, &mut on_event)
    };
    let rollback_result = isolated_read.then(|| client.query("ROLLBACK"));
    match (query_result, rollback_result) {
        (Ok(_), None | Some(Ok(_))) => collector.finish(),
        (Ok(_), Some(Err(error))) => Err(error),
        (Err(error), None | Some(Ok(_))) => Err(error),
        (Err(error), Some(Err(rollback))) => Err(error.with_hint(format!(
            "the read-only query failed and rollback also failed: {}",
            rollback.message
        ))),
    }
}

fn collect_native_pg_event(
    collector: &mut BoundedAiCollector,
    event: PgQueryEvent,
    processed: &mut u64,
) {
    let event = match event {
        PgQueryEvent::Schema(columns) => DbmsQueryEvent::Schema {
            columns: columns
                .into_iter()
                .map(|name| QueryColumn {
                    name,
                    data_type: "text".into(),
                })
                .collect(),
        },
        PgQueryEvent::Batch(rows) => {
            *processed = processed.saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
            collector.push(DbmsQueryEvent::Batch { rows });
            DbmsQueryEvent::Progress {
                rows_processed: *processed,
            }
        }
        PgQueryEvent::Notice(notice) => DbmsQueryEvent::Notice {
            severity: notice.severity.as_str().into(),
            sql_state: notice.sql_state,
            message: notice.message,
        },
        PgQueryEvent::Complete(command_tag) => DbmsQueryEvent::Complete {
            command_tag,
            duration_ms: 0,
        },
        PgQueryEvent::Notification(notification) => DbmsQueryEvent::Notice {
            severity: "NOTICE".into(),
            sql_state: "00000".into(),
            message: format!(
                "notification {} from backend {}: {}",
                notification.channel, notification.sender_process_id, notification.payload
            ),
        },
    };
    collector.push(event);
}

fn emit_native_pg_event(
    app: &AppHandle,
    request_id: &str,
    event: PgQueryEvent,
    processed: &mut u64,
) {
    match event {
        PgQueryEvent::Schema(columns) => emit_query(
            app,
            request_id,
            DbmsQueryEvent::Schema {
                columns: columns
                    .into_iter()
                    .map(|name| QueryColumn {
                        name,
                        data_type: "text".into(),
                    })
                    .collect(),
            },
        ),
        PgQueryEvent::Batch(rows) => {
            *processed = processed.saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
            emit_query(app, request_id, DbmsQueryEvent::Batch { rows });
            emit_query(
                app,
                request_id,
                DbmsQueryEvent::Progress {
                    rows_processed: *processed,
                },
            );
        }
        PgQueryEvent::Notice(notice) => emit_query(
            app,
            request_id,
            DbmsQueryEvent::Notice {
                severity: notice.severity.as_str().into(),
                sql_state: notice.sql_state,
                message: notice.message,
            },
        ),
        PgQueryEvent::Complete(_) => {}
        PgQueryEvent::Notification(notification) => emit_query(
            app,
            request_id,
            DbmsQueryEvent::Notice {
                severity: "NOTICE".into(),
                sql_state: "00000".into(),
                message: format!(
                    "notification {} from backend {}: {}",
                    notification.channel, notification.sender_process_id, notification.payload
                ),
            },
        ),
    }
}

fn desktop_command_v3(command: DesktopCommand) -> ConnectorCommandV3 {
    match command {
        DesktopCommand::Text {
            language_id,
            text,
            params,
        } => ConnectorCommandV3::Text {
            language_id,
            text,
            params: params
                .into_iter()
                .map(|value| ConnectorParameterV2 {
                    data_type: None,
                    value: value.map_or(ConnectorValueV2::Null, ConnectorValueV2::Text),
                })
                .collect(),
        },
        DesktopCommand::Document {
            language_id,
            document,
        } => ConnectorCommandV3::Document {
            language_id,
            document,
        },
        DesktopCommand::Arguments {
            language_id,
            arguments,
        } => ConnectorCommandV3::Arguments {
            language_id,
            arguments: arguments.into_iter().map(ConnectorValueV2::Text).collect(),
        },
    }
}

async fn run_connector_execute_v3(
    host: &mut ConnectorHost,
    app: &AppHandle,
    request_id: &str,
    connection_id: String,
    command: ConnectorCommandV3,
    cancellation: CancellationToken,
    started: Instant,
) -> Result<(), DbError> {
    host.send_v3(&ConnectorRequestV3::Execute {
        request_id: request_id.to_owned(),
        connection_id,
        command,
        batch_size: QUERY_BATCH_ROWS as u32,
    })
    .await?;
    let mut cancel_sent = false;
    loop {
        let response = if cancel_sent {
            host.receive_v3().await?
        } else {
            tokio::select! {
                response = host.receive_v3() => response?,
                () = cancellation.cancelled() => {
                    host.send_v3(&ConnectorRequestV3::Cancel {
                        request_id: request_id.to_owned(),
                    }).await?;
                    cancel_sent = true;
                    continue;
                }
            }
        };
        match response {
            ConnectorResponseV3::ResultEvent {
                request_id: actual,
                event,
            } if actual == request_id => {
                let terminal = matches!(event, ConnectorResultEventV3::Complete { .. });
                emit_query(
                    app,
                    request_id,
                    map_connector_event_v3(event, started.elapsed())?,
                );
                if terminal {
                    return Ok(());
                }
            }
            ConnectorResponseV3::Cancelled { request_id: actual } if actual == request_id => {
                return Err(DbError::new("57014", "connector command was cancelled"));
            }
            ConnectorResponseV3::Error {
                request_id: actual,
                error,
            } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                return Err(error.into_db_error());
            }
            _ => {
                return Err(DbError::new(
                    "08P01",
                    "connector returned an unexpected v3 result response",
                ));
            }
        }
    }
}

fn map_connector_event_v3(
    event: ConnectorResultEventV3,
    elapsed: Duration,
) -> Result<DbmsQueryEvent, DbError> {
    match event {
        ConnectorResultEventV3::Schema { columns } => Ok(DbmsQueryEvent::Schema {
            columns: columns
                .into_iter()
                .map(|column| QueryColumn {
                    name: column.name,
                    data_type: column.data_type.vendor_name,
                })
                .collect(),
        }),
        ConnectorResultEventV3::Batch {
            batch: ConnectorResultBatchV3::Rows { rows },
        } => Ok(DbmsQueryEvent::Batch {
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(connector_value_text).collect())
                .collect(),
        }),
        ConnectorResultEventV3::Batch {
            batch: ConnectorResultBatchV3::Documents { documents },
        } => Ok(DbmsQueryEvent::Documents { documents }),
        ConnectorResultEventV3::Batch {
            batch: ConnectorResultBatchV3::KeyValues { entries },
        } => Ok(DbmsQueryEvent::KeyValues {
            entries: entries
                .into_iter()
                .map(|entry| {
                    Ok(DbmsKeyValue {
                        key: connector_value_json(entry.key)?,
                        value: connector_value_json(entry.value)?,
                    })
                })
                .collect::<Result<Vec<_>, DbError>>()?,
        }),
        ConnectorResultEventV3::Progress { items_processed } => Ok(DbmsQueryEvent::Progress {
            rows_processed: items_processed,
        }),
        ConnectorResultEventV3::Notice { notice } => Ok(DbmsQueryEvent::Notice {
            severity: notice.severity,
            sql_state: notice.code.unwrap_or_default(),
            message: notice.message,
        }),
        ConnectorResultEventV3::Complete { command_tag, .. } => Ok(DbmsQueryEvent::Complete {
            command_tag,
            duration_ms: elapsed_ms(elapsed),
        }),
    }
}

fn connector_value_text(value: ConnectorValueV2) -> Option<String> {
    match value {
        ConnectorValueV2::Null => None,
        ConnectorValueV2::Boolean(value) => Some(value.to_string()),
        ConnectorValueV2::SignedInteger(value) => Some(value.to_string()),
        ConnectorValueV2::UnsignedInteger(value) => Some(value.to_string()),
        ConnectorValueV2::FloatingPoint(value) => Some(value.to_string()),
        ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Text(value)
        | ConnectorValueV2::Binary(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::Uuid(value) => Some(value),
        ConnectorValueV2::Json(value) => Some(value.to_string()),
        ConnectorValueV2::Array(values) => Some(
            JsonValue::Array(
                values
                    .into_iter()
                    .map(|value| {
                        connector_value_text(value).map_or(JsonValue::Null, JsonValue::String)
                    })
                    .collect(),
            )
            .to_string(),
        ),
    }
}

fn connector_value_json(value: ConnectorValueV2) -> Result<JsonValue, DbError> {
    let value = match value {
        ConnectorValueV2::Null => JsonValue::Null,
        ConnectorValueV2::Boolean(value) => JsonValue::Bool(value),
        ConnectorValueV2::SignedInteger(value) => value.into(),
        ConnectorValueV2::UnsignedInteger(value) => value.into(),
        ConnectorValueV2::FloatingPoint(value) => serde_json::Number::from_f64(value)
            .map(JsonValue::Number)
            .ok_or_else(|| DbError::new("08P01", "connector returned a non-finite number"))?,
        ConnectorValueV2::Decimal(value) => typed_connector_value("decimal", value),
        ConnectorValueV2::Text(value) => JsonValue::String(value),
        ConnectorValueV2::Binary(value) => typed_connector_value("binary", value),
        ConnectorValueV2::Date(value) => typed_connector_value("date", value),
        ConnectorValueV2::Time(value) => typed_connector_value("time", value),
        ConnectorValueV2::Timestamp(value) => typed_connector_value("timestamp", value),
        ConnectorValueV2::TimestampWithTimeZone(value) => {
            typed_connector_value("timestampWithTimeZone", value)
        }
        ConnectorValueV2::Interval(value) => typed_connector_value("interval", value),
        ConnectorValueV2::Uuid(value) => typed_connector_value("uuid", value),
        ConnectorValueV2::Json(value) => value,
        ConnectorValueV2::Array(values) => JsonValue::Array(
            values
                .into_iter()
                .map(connector_value_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    };
    Ok(value)
}

fn typed_connector_value(kind: &'static str, value: String) -> JsonValue {
    serde_json::json!({ "kind": kind, "value": value })
}

fn map_connector_event(event: QueryEvent, elapsed: Duration) -> DbmsQueryEvent {
    match event {
        QueryEvent::Schema(schema) => DbmsQueryEvent::Schema {
            columns: schema
                .fields
                .into_iter()
                .map(|field| QueryColumn {
                    name: field.name,
                    data_type: format!("{:?}", field.data_type),
                })
                .collect(),
        },
        QueryEvent::Batch(batch) => DbmsQueryEvent::Batch {
            rows: batch
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(value_text).collect())
                .collect(),
        },
        QueryEvent::Progress(progress) => DbmsQueryEvent::Progress {
            rows_processed: progress.rows_processed,
        },
        QueryEvent::Notice(notice) => DbmsQueryEvent::Notice {
            severity: notice.severity.as_str().into(),
            sql_state: notice.sql_state,
            message: notice.message,
        },
        QueryEvent::Complete(complete) => DbmsQueryEvent::Complete {
            command_tag: complete.tag,
            duration_ms: elapsed_ms(elapsed),
        },
    }
}

fn value_text(value: Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Boolean(value) => Some(value.to_string()),
        Value::Int16(value) => Some(value.to_string()),
        Value::Int32(value) => Some(value.to_string()),
        Value::Int64(value) => Some(value.to_string()),
        Value::Float32(value) => Some(value.to_string()),
        Value::Float64(value) => Some(value.to_string()),
        Value::Decimal(value) => Some(value.to_string()),
        Value::Text(value) => Some(value),
        Value::Binary(value) => Some(format!("{} bytes", value.len())),
        Value::Date(value) => Some(value.to_string()),
        Value::Time(value) => Some(value.to_string()),
        Value::Timestamp(value) => Some(value.to_string()),
        Value::Interval(value) => Some(value.to_string()),
        Value::Array(array) => Some(
            serde_json::Value::Array(
                array
                    .values()
                    .iter()
                    .cloned()
                    .map(|value| value_text(value).map_or(serde_json::Value::Null, Into::into))
                    .collect(),
            )
            .to_string(),
        ),
        Value::Json(value) | Value::Jsonb(value) => Some(value.to_string()),
        Value::Uuid(value) => Some(value.to_string()),
        Value::Vector(value) => Some(format!(
            "[{}]",
            value
                .into_iter()
                .map(|number| number.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn emit_query(app: &AppHandle, request_id: &str, event: DbmsQueryEvent) {
    let _ = app.emit(
        DBMS_QUERY_EVENT,
        QueryUpdate {
            request_id: request_id.to_owned(),
            event,
        },
    );
}

async fn cancel_request(cancellation: RequestCancellation) -> Result<(), DbError> {
    match cancellation {
        RequestCancellation::Native(token) => tokio::task::spawn_blocking(move || token.cancel())
            .await
            .map_err(join_error)?,
        RequestCancellation::Plugin(token) => {
            token.cancel();
            Ok(())
        }
    }
}

fn elapsed_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn empty_engine_status() -> EngineStatus {
    EngineStatus {
        generation: 0,
        table_count: 0,
        row_count: 0,
        index_count: 0,
        durable_lsn: None,
        dirty_page_count: 0,
        commits_since_checkpoint: 0,
    }
}

fn network_error(context: &str, error: impl std::fmt::Display) -> DbError {
    DbError::new("08006", context).with_detail(error.to_string())
}

fn ai_cancelled() -> DbError {
    DbError::new("57014", "AI database operation was cancelled")
}

fn join_error(error: tokio::task::JoinError) -> DbError {
    DbError::new("XX000", "database worker task failed").with_detail(error.to_string())
}

fn task_error(context: &str, error: impl std::fmt::Display) -> DbError {
    DbError::new("XX000", context).with_detail(error.to_string())
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, DbError> {
    mutex
        .lock()
        .map_err(|_| DbError::internal("database connection lock was poisoned"))
}

fn read_lock<T>(lock: &RwLock<T>) -> Result<RwLockReadGuard<'_, T>, DbError> {
    lock.read()
        .map_err(|_| DbError::internal("desktop DBMS state lock was poisoned"))
}

fn write_lock<T>(lock: &RwLock<T>) -> Result<RwLockWriteGuard<'_, T>, DbError> {
    lock.write()
        .map_err(|_| DbError::internal("desktop DBMS state lock was poisoned"))
}

async fn prompt_database_credential(
    connector_id: String,
    suggested_username: String,
    first_administrator: bool,
) -> Result<Option<PromptedCredential>, DbError> {
    let (caption, message) = if first_administrator {
        (
            "创建 OrdaDB 首位管理员".to_owned(),
            "请输入首位管理员用户名和密码。凭据只在受保护的本机窗口与 Windows 凭据库中处理。"
                .to_owned(),
        )
    } else {
        let display_name = match connector_id.as_str() {
            NATIVE_CONNECTOR_ID => "OrdaDB",
            "postgresql" => "PostgreSQL",
            "mysql" => "MySQL",
            "sqlite" => "SQLite",
            "sql-server" => "SQL Server",
            "mongodb" => "MongoDB",
            "redis" => "Redis",
            "mariadb" => "MariaDB",
            "clickhouse" => "ClickHouse",
            "oracle" => "Oracle",
            _ => return Err(invalid("unknown connector ID")),
        };
        (
            format!("连接 {display_name}"),
            format!("请输入 {display_name} 用户名和密码。密码不会进入 OrdaDB 网页界面或状态文件。"),
        )
    };
    let target = format!("OrdaDB Console/{connector_id}");
    tauri::async_runtime::spawn_blocking(move || {
        prompt_for_credential(&target, &suggested_username, &caption, &message)
    })
    .await
    .map_err(|error| task_error("Windows credential prompt task failed", error))?
}

fn probe_windows_service() -> Result<ServiceIdentity, DbError> {
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
        .map_err(|error| {
            DbError::new("58030", "failed to open Windows Service Control Manager")
                .with_detail(error.to_string())
        })?;
    let service = manager
        .open_service(
            ordadb_server::SERVICE_NAME,
            ServiceAccess::QUERY_STATUS | ServiceAccess::QUERY_CONFIG,
        )
        .map_err(|error| {
            DbError::new("55000", "OrdaDB Windows service is not installed")
                .with_detail(error.to_string())
                .with_hint("install or repair OrdaDB, then retry the local connection")
        })?;
    let status = service.query_status().map_err(|error| {
        DbError::new("58030", "failed to query OrdaDB Windows service")
            .with_detail(error.to_string())
    })?;
    if status.current_state != ServiceState::Running {
        return Err(
            DbError::new("55000", "OrdaDB Windows service is not running")
                .with_detail(format!("current service state: {:?}", status.current_state))
                .with_hint("start the OrdaDB service, then retry"),
        );
    }
    let process_id = status
        .process_id
        .filter(|process_id| *process_id != 0)
        .ok_or_else(|| {
            DbError::new(
                "55000",
                "OrdaDB Windows service has no running process identity",
            )
        })?;
    let configuration = service.query_config().map_err(|error| {
        DbError::new(
            "58030",
            "failed to query OrdaDB Windows service configuration",
        )
        .with_detail(error.to_string())
    })?;
    let command_line = configuration.executable_path.to_string_lossy();
    let arguments = split_windows_command_line(&command_line)?;
    let data_dir = arguments
        .windows(2)
        .find_map(|pair| (pair[0] == "--data-dir").then(|| PathBuf::from(&pair[1])))
        .ok_or_else(|| {
            DbError::new(
                "55000",
                "OrdaDB Windows service configuration has no data directory",
            )
            .with_hint("repair the OrdaDB service registration, then retry")
        })?;
    let data_dir = fs::canonicalize(&data_dir).map_err(|error| {
        DbError::new("58030", "failed to resolve OrdaDB service data directory")
            .with_detail(error.to_string())
    })?;
    let pipe_name = ordadb_server::bootstrap_pipe_name(&data_dir);
    Ok(ServiceIdentity {
        process_id,
        data_dir,
        pipe_name,
    })
}

fn split_windows_command_line(command_line: &str) -> Result<Vec<String>, DbError> {
    let characters = command_line.chars().collect::<Vec<_>>();
    let mut arguments = Vec::new();
    let mut offset = 0_usize;
    while offset < characters.len() {
        while offset < characters.len() && characters[offset].is_whitespace() {
            offset += 1;
        }
        if offset == characters.len() {
            break;
        }
        let mut argument = String::new();
        let mut quoted = false;
        while offset < characters.len() {
            match characters[offset] {
                character if character.is_whitespace() && !quoted => break,
                '"' => {
                    quoted = !quoted;
                    offset += 1;
                }
                '\\' => {
                    let start = offset;
                    while offset < characters.len() && characters[offset] == '\\' {
                        offset += 1;
                    }
                    let count = offset - start;
                    if offset < characters.len() && characters[offset] == '"' {
                        argument.extend(std::iter::repeat_n('\\', count / 2));
                        if count % 2 == 0 {
                            quoted = !quoted;
                        } else {
                            argument.push('"');
                        }
                        offset += 1;
                    } else {
                        argument.extend(std::iter::repeat_n('\\', count));
                    }
                }
                character => {
                    argument.push(character);
                    offset += 1;
                }
            }
        }
        if quoted {
            return Err(invalid(
                "OrdaDB Windows service command line contains an unmatched quote",
            ));
        }
        arguments.push(argument);
        while offset < characters.len() && characters[offset].is_whitespace() {
            offset += 1;
        }
    }
    if arguments.is_empty() {
        return Err(invalid("OrdaDB Windows service command line is empty"));
    }
    Ok(arguments)
}

fn connection_fingerprint(request: &ConnectRequest) -> [u8; 32] {
    let mut hash = Sha256::new();
    for value in [
        Some(request.connector_id.as_str()),
        Some(request.connector_kind.as_str()),
        Some(request.command_language.as_str()),
        request.dialect.as_deref(),
        Some(request.endpoint.as_str()),
        request.admin_endpoint.as_deref(),
        request.database.as_deref(),
        Some(connector_tls_mode_name(request.tls_mode)),
        Some(request.credential_id.as_str()),
        Some(request.credential_access.as_str()),
    ] {
        match value {
            Some(value) => {
                hash.update([1]);
                hash.update((value.len() as u64).to_le_bytes());
                hash.update(value.as_bytes());
            }
            None => hash.update([0]),
        }
    }
    hash.finalize().into()
}

const fn connector_tls_mode_name(mode: ConnectorTlsModeV2) -> &'static str {
    match mode {
        ConnectorTlsModeV2::Disable => "disable",
        ConnectorTlsModeV2::Prefer => "prefer",
        ConnectorTlsModeV2::Require => "require",
        ConnectorTlsModeV2::VerifyCa => "verifyCa",
        ConnectorTlsModeV2::VerifyFull => "verifyFull",
    }
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

#[cfg(test)]
mod tests {
    use ordadb_types::{PgArray, PgInterval, ScalarType};

    use super::*;

    #[test]
    fn credential_prompt_request_contains_no_secret_field() {
        let request = PromptCredentialRequest {
            credential_id: "local".into(),
            connector_id: NATIVE_CONNECTOR_ID.into(),
            suggested_username: "dba".into(),
        };
        let debug = format!("{request:?}");
        assert!(debug.contains("suggested_username"));
        assert!(!debug.contains("password"));
        assert!(
            serde_json::from_value::<PromptCredentialRequest>(serde_json::json!({
                "credentialId": "local",
                "connectorId": NATIVE_CONNECTOR_ID,
                "suggestedUsername": "dba",
                "password": null
            }))
            .is_err()
        );
    }

    #[test]
    fn bootstrap_request_debug_is_redacted_and_probe_stages_are_structured() {
        let connection = native_connect_request();
        let request = BootstrapAdminRequest {
            ticket: "bootstrap-ticket-secret".into(),
            connection,
            suggested_username: "ordadb_admin".into(),
        };
        let debug = format!("{request:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bootstrap-ticket-secret"));
        assert!(!debug.contains("password"));

        let mut probe = ConnectionProbe::new();
        probe.passed(ConnectionProbeStageName::Service);
        probe.passed(ConnectionProbeStageName::PgPort);
        probe.passed(ConnectionProbeStageName::AdminApi);
        probe.failed(
            ConnectionProbeStageName::Initialization,
            DbError::new("55000", "administrator bootstrap required"),
        );
        probe.skipped(ConnectionProbeStageName::Authentication);
        probe.skipped(ConnectionProbeStageName::Catalog);
        probe.finish();
        let value = serde_json::to_value(probe).expect("serialize probe");
        assert_eq!(value["ready"], false);
        assert_eq!(value["stages"][1]["stage"], "pgPort");
        assert_eq!(value["stages"][3]["status"], "failed");
        assert_eq!(value["stages"][3]["error"]["sqlState"], "55000");
        assert!(value["bootstrapTicket"].is_null());
    }

    #[test]
    fn bootstrap_ticket_is_bound_expires_and_can_be_consumed_only_once() {
        let (_root, runtime) = test_runtime();
        let request = native_connect_request();
        let service = ServiceIdentity {
            process_id: 41,
            data_dir: PathBuf::from(r"C:\ProgramData\OrdaDB\data"),
            pipe_name: r"\\.\pipe\ordadb-bootstrap-test".into(),
        };

        let issued = runtime
            .issue_bootstrap_ticket(&request, &service)
            .expect("issue ticket");
        let payload = BootstrapAdminRequest {
            ticket: issued.ticket.clone(),
            connection: request.clone(),
            suggested_username: "ordadb_admin".into(),
        };
        assert_eq!(
            runtime
                .consume_bootstrap_ticket(&payload)
                .expect("consume ticket")
                .service,
            service
        );
        assert_eq!(
            runtime
                .consume_bootstrap_ticket(&payload)
                .expect_err("ticket replay")
                .sql_state,
            "55000"
        );

        let issued = runtime
            .issue_bootstrap_ticket(&request, &service)
            .expect("issue fingerprint ticket");
        let mut mismatched_connection = request.clone();
        mismatched_connection.endpoint = "127.0.0.1:54330".into();
        let mismatched = BootstrapAdminRequest {
            ticket: issued.ticket.clone(),
            connection: mismatched_connection,
            suggested_username: "ordadb_admin".into(),
        };
        assert_eq!(
            runtime
                .consume_bootstrap_ticket(&mismatched)
                .expect_err("fingerprint mismatch")
                .sql_state,
            "28000"
        );
        assert_eq!(
            runtime
                .consume_bootstrap_ticket(&BootstrapAdminRequest {
                    ticket: issued.ticket,
                    connection: request.clone(),
                    suggested_username: "ordadb_admin".into(),
                })
                .expect_err("mismatched ticket is consumed")
                .sql_state,
            "55000"
        );

        let issued = runtime
            .issue_bootstrap_ticket(&request, &service)
            .expect("issue expiring ticket");
        mutex_lock(&runtime.bootstrap_tickets)
            .expect("ticket lock")
            .get_mut(&issued.ticket)
            .expect("issued ticket")
            .expires_at = Instant::now() - Duration::from_millis(1);
        assert_eq!(
            runtime
                .consume_bootstrap_ticket(&BootstrapAdminRequest {
                    ticket: issued.ticket,
                    connection: request,
                    suggested_username: "ordadb_admin".into(),
                })
                .expect_err("expired ticket")
                .sql_state,
            "55000"
        );
    }

    #[test]
    fn bootstrap_ticket_serialization_exposes_only_the_bounded_capability() {
        let ticket = LocalBootstrapTicket {
            ticket: "ticket-1".into(),
            expires_in_ms: 120_000,
        };
        let mut probe = ConnectionProbe::new();
        probe.bootstrap_ticket = Some(ticket);
        let value = serde_json::to_value(probe).expect("serialize ticket");
        assert_eq!(value["bootstrapTicket"]["ticket"], "ticket-1");
        assert_eq!(value["bootstrapTicket"]["expiresInMs"], 120_000);
        assert_eq!(
            value["bootstrapTicket"]
                .as_object()
                .expect("ticket object")
                .len(),
            2
        );
    }

    #[test]
    fn windows_service_command_line_parser_preserves_quoted_data_directories() {
        assert_eq!(
            split_windows_command_line(
                r#""C:\Program Files\OrdaDB\ordadb-server.exe" service --data-dir "D:\Orda Data\cluster""#
            )
            .expect("quoted command line"),
            vec![
                r"C:\Program Files\OrdaDB\ordadb-server.exe",
                "service",
                "--data-dir",
                r"D:\Orda Data\cluster",
            ]
        );
        assert_eq!(
            split_windows_command_line(
                r#""C:\OrdaDB\ordadb-server.exe" service --data-dir "D:\quoted\\\"name""#
            )
            .expect("escaped quote"),
            vec![
                r"C:\OrdaDB\ordadb-server.exe",
                "service",
                "--data-dir",
                "D:\\quoted\\\"name",
            ]
        );
        assert_eq!(
            split_windows_command_line(r#""C:\OrdaDB\ordadb-server.exe --data-dir D:\data"#)
                .expect_err("unmatched quote")
                .sql_state,
            "22023"
        );
    }

    #[test]
    fn native_endpoint_and_identifiers_are_bounded() {
        assert_eq!(
            validate_admin_endpoint("http://127.0.0.1:9080").expect("loopback"),
            "http://127.0.0.1:9080"
        );
        assert_eq!(
            validate_admin_endpoint("http://db.example.test:9080")
                .expect_err("remote plaintext")
                .sql_state,
            "22023"
        );
        assert!(validate_admin_endpoint("https://db.example.test:9080").is_ok());
        assert!(validate_id("connection-1", "connection ID").is_ok());
        assert!(validate_id("../escape", "connection ID").is_err());
    }

    #[test]
    fn catalog_projection_is_flattened_without_leaking_identifier_markers() {
        let projection = serde_json::json!({
            "database": {
                "name": "u:ordadb",
                "schemas": [{
                    "name": "u:public",
                    "tables": [{
                        "name": "u:documents",
                        "indexes": [{"name": "u:documents_pk"}],
                        "constraints": [],
                        "triggers": []
                    }],
                    "sequences": [],
                    "views": [{
                        "name": "u:recent_documents",
                        "kind": "plain",
                        "indexes": []
                    }],
                    "routines": []
                }]
            }
        });
        let objects = flatten_catalog(&projection).expect("catalog");
        assert!(objects.iter().any(|object| object.name == "documents"));
        assert!(objects.iter().any(|object| object.name == "documents_pk"));
        assert!(objects.iter().all(|object| !object.name.starts_with("u:")));
    }

    #[test]
    fn connector_values_are_projected_as_display_cells() {
        assert_eq!(value_text(Value::Null), None);
        assert_eq!(
            value_text(Value::Text("document".into())),
            Some("document".into())
        );
        assert_eq!(
            value_text(Value::Vector(vec![1.0, 2.0])),
            Some("[1, 2]".into())
        );
        let interval = PgInterval::new(2, 3, 4);
        let interval_text = interval.to_string();
        assert_eq!(value_text(Value::Interval(interval)), Some(interval_text));
        let array = PgArray::one_dimensional(ScalarType::Int32, vec![Value::Int32(7), Value::Null])
            .expect("array");
        assert_eq!(
            value_text(Value::Array(array)),
            Some(r#"["7",null]"#.into())
        );
    }

    #[test]
    fn ten_connector_identities_validate_without_sql_aliases_for_non_sql_sources() {
        let cases = [
            (
                NATIVE_CONNECTOR_ID,
                "sql",
                "postgresql-sql",
                Some("postgresql"),
            ),
            ("postgresql", "sql", "postgresql-sql", Some("postgresql")),
            ("mysql", "sql", "mysql-sql", Some("mysql")),
            ("sqlite", "sql", "sqlite-sql", Some("sqlite")),
            ("sql-server", "sql", "sql-server-sql", Some("sqlServer")),
            ("mongodb", "document", "mongodb-json", None),
            ("redis", "keyValue", "redis-resp3", None),
            ("mariadb", "sql", "mariadb-sql", Some("mariadb")),
            ("clickhouse", "sql", "clickhouse-sql", Some("clickhouse")),
            ("oracle", "sql", "oracle-sql", Some("oracle")),
        ];
        for (connector_id, connector_kind, command_language, dialect) in cases {
            let native = connector_id == NATIVE_CONNECTOR_ID;
            let request = ConnectRequest {
                connector_id: connector_id.into(),
                connector_kind: connector_kind.into(),
                command_language: command_language.into(),
                dialect: dialect.map(str::to_owned),
                endpoint: "127.0.0.1:15432".into(),
                admin_endpoint: native.then(|| "http://127.0.0.1:9080".into()),
                database: Some(if connector_id == "redis" { "0" } else { "test" }.into()),
                tls_mode: if native {
                    ConnectorTlsModeV2::Disable
                } else {
                    ConnectorTlsModeV2::Require
                },
                credential_id: format!("credential-{connector_id}"),
                credential_access: CredentialAccess::Unspecified,
            };
            validate_connect_request(&request).expect(connector_id);

            let mut mismatched = request;
            mismatched.command_language = "postgresql-sql".into();
            if command_language != "postgresql-sql" {
                assert_eq!(
                    validate_connect_request(&mismatched)
                        .expect_err("identity mismatch")
                        .sql_state,
                    "22023"
                );
            }
        }
    }

    #[test]
    fn desktop_command_shapes_are_bound_to_the_negotiated_data_model() {
        let mongodb = DesktopCommand::Document {
            language_id: "mongodb-json".into(),
            document: serde_json::json!({"operation": "find"}),
        };
        validate_command_for_connection(&mongodb, "document", "mongodb-json")
            .expect("MongoDB document command");
        assert_eq!(
            validate_command_for_connection(&mongodb, "sql", "postgresql-sql")
                .expect_err("document cannot become SQL")
                .sql_state,
            "22023"
        );

        let redis = DesktopCommand::Arguments {
            language_id: "redis-resp3".into(),
            arguments: vec!["GET".into(), "key".into()],
        };
        validate_command_for_connection(&redis, "keyValue", "redis-resp3")
            .expect("Redis argument command");
        assert!(matches!(
            desktop_command_v3(redis),
            ConnectorCommandV3::Arguments { .. }
        ));
    }

    #[test]
    fn v3_catalog_and_key_values_preserve_native_identity_and_types() {
        let object = catalog_node_v3(ConnectorCatalogNodeV3 {
            id: "collection:orders".into(),
            parent_id: Some("database:shop".into()),
            kind: ConnectorCatalogNodeKindV3::Collection,
            name: "orders".into(),
            namespace: Some("shop".into()),
            has_children: true,
            columns: Vec::new(),
            attributes: BTreeMap::from([("capped".into(), "false".into())]),
        })
        .expect("Catalog projection");
        assert_eq!(object.id.as_deref(), Some("collection:orders"));
        assert_eq!(object.parent.as_deref(), Some("database:shop"));
        assert_eq!(object.namespace.as_deref(), Some("shop"));
        assert_eq!(object.details["attributes"]["capped"], "false");

        assert_eq!(
            connector_value_json(ConnectorValueV2::Decimal("123456789.0123".into()))
                .expect("decimal"),
            serde_json::json!({"kind": "decimal", "value": "123456789.0123"})
        );
        assert_eq!(
            connector_value_json(ConnectorValueV2::Array(vec![
                ConnectorValueV2::Text("value".into()),
                ConnectorValueV2::Null,
            ]))
            .expect("array"),
            serde_json::json!(["value", null])
        );
        assert_eq!(
            connector_value_json(ConnectorValueV2::FloatingPoint(f64::NAN))
                .expect_err("non-finite value")
                .sql_state,
            "08P01"
        );
    }

    #[test]
    fn bootstrap_fingerprint_covers_connector_model_language_and_tls() {
        let request = native_connect_request();
        let baseline = connection_fingerprint(&request);
        let mut changed = request.clone();
        changed.command_language = "other-sql".into();
        assert_ne!(baseline, connection_fingerprint(&changed));
        changed = request.clone();
        changed.tls_mode = ConnectorTlsModeV2::Require;
        assert_ne!(baseline, connection_fingerprint(&changed));
    }

    #[test]
    fn query_updates_serialize_event_fields_in_camel_case() {
        let progress = serde_json::to_value(QueryUpdate {
            request_id: "request-1".into(),
            event: DbmsQueryEvent::Progress { rows_processed: 7 },
        })
        .expect("serialize progress");
        assert_eq!(
            progress,
            serde_json::json!({
                "requestId": "request-1",
                "event": {
                    "kind": "progress",
                    "rowsProcessed": 7
                }
            })
        );

        let notice = serde_json::to_value(QueryUpdate {
            request_id: "request-notice".into(),
            event: DbmsQueryEvent::Notice {
                severity: "WARNING".into(),
                sql_state: "01000".into(),
                message: "careful".into(),
            },
        })
        .expect("serialize notice");
        assert_eq!(
            notice,
            serde_json::json!({
                "requestId": "request-notice",
                "event": {
                    "kind": "notice",
                    "severity": "WARNING",
                    "sqlState": "01000",
                    "message": "careful"
                }
            })
        );

        let complete = serde_json::to_value(QueryUpdate {
            request_id: "request-2".into(),
            event: DbmsQueryEvent::Complete {
                command_tag: "SELECT 1".into(),
                duration_ms: 12,
            },
        })
        .expect("serialize completion");
        assert_eq!(
            complete,
            serde_json::json!({
                "requestId": "request-2",
                "event": {
                    "kind": "complete",
                    "commandTag": "SELECT 1",
                    "durationMs": 12
                }
            })
        );

        let error = serde_json::to_value(QueryUpdate {
            request_id: "request-3".into(),
            event: DbmsQueryEvent::Error {
                error: DbmsError {
                    sql_state: "57014".into(),
                    message: "query cancelled".into(),
                    detail: None,
                    hint: Some("retry the query".into()),
                    position: Some(9),
                    query_id: "query-3".into(),
                },
            },
        })
        .expect("serialize error");
        assert_eq!(
            error,
            serde_json::json!({
                "requestId": "request-3",
                "event": {
                    "kind": "error",
                    "error": {
                        "sqlState": "57014",
                        "message": "query cancelled",
                        "detail": null,
                        "hint": "retry the query",
                        "position": 9,
                        "queryId": "query-3"
                    }
                }
            })
        );
    }

    #[test]
    fn administration_requests_are_relative_and_shape_checked() {
        let backup = StartAdministrationOperationRequest {
            connection_id: "connection-1".into(),
            kind: AdministrationOperationKind::Backup,
            path: "nightly/ordadb.ordbak".into(),
            schema: None,
            table: None,
            format: None,
        };
        assert!(validate_administration_operation_request(&backup).is_ok());

        let mut absolute = backup.clone();
        absolute.path = r"C:\ProgramData\ordadb.ordbak".into();
        assert_eq!(
            validate_administration_operation_request(&absolute)
                .expect_err("absolute path")
                .sql_state,
            "22023"
        );
        let mut traversal = backup.clone();
        traversal.path = "../escape.ordbak".into();
        assert!(validate_administration_operation_request(&traversal).is_err());

        let mut missing_table = backup;
        missing_table.kind = AdministrationOperationKind::Import;
        missing_table.format = Some(AdministrationTransferFormat::Csv);
        assert!(validate_administration_operation_request(&missing_table).is_err());
    }

    #[test]
    fn administration_operation_errors_serialize_recursively_in_camel_case() {
        let operation = AdministrationOperation::from(AdministrationOperationResponse {
            operation_id: Uuid::nil(),
            kind: AdministrationOperationKind::Restore,
            state: "failed".into(),
            path: "broken.ordbak".into(),
            schema: None,
            table: None,
            started_at: None,
            finished_at: None,
            rows: None,
            bytes: None,
            error: Some(DbError::new("XX001", "archive checksum mismatch")),
        });
        let value = serde_json::to_value(operation).expect("serialize operation");
        assert_eq!(value["operationId"], Uuid::nil().to_string());
        assert_eq!(value["kind"], "restore");
        assert_eq!(value["error"]["sqlState"], "XX001");
        assert!(value["error"].get("sql_state").is_none());
        assert!(value["error"].get("queryId").is_some());
    }

    fn native_connect_request() -> ConnectRequest {
        ConnectRequest {
            connector_id: NATIVE_CONNECTOR_ID.into(),
            connector_kind: "sql".into(),
            command_language: "postgresql-sql".into(),
            dialect: Some("postgresql".into()),
            endpoint: "127.0.0.1:54329".into(),
            admin_endpoint: Some("http://127.0.0.1:9080".into()),
            database: Some("ordadb".into()),
            tls_mode: ConnectorTlsModeV2::Disable,
            credential_id: "ordadb-local".into(),
            credential_access: CredentialAccess::Unspecified,
        }
    }

    fn test_runtime() -> (tempfile::TempDir, Arc<DbmsRuntime>) {
        let root = tempfile::tempdir().expect("temporary plugin root");
        let manager =
            PluginManager::open_https(ordadb_connectors::PluginManagerOptions::new(root.path()))
                .expect("plugin manager");
        let runtime = DbmsRuntime::new(manager).expect("DBMS runtime");
        (root, runtime)
    }
}
