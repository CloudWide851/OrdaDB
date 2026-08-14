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
use ordadb_windows::DatabaseCredentialStore;
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

use credentials::{CredentialSaved, PromptCredentialRequest, prompt_database_credential};

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
    credentials: DatabaseCredentialStore,
    plugin_manager: Arc<PluginManager>,
    connections: RwLock<BTreeMap<String, Arc<ConnectionHandle>>>,
    requests: RwLock<BTreeMap<String, ActiveRequest>>,
    bootstrap_tickets: Mutex<BTreeMap<String, BootstrapTicketRecord>>,
    http: Client,
}
