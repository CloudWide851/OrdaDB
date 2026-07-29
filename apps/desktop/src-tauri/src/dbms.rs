use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::{Component, Path};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use ordadb_connectors::{
    CatalogEntry, ConnectorHost, ConnectorRequestV1, ConnectorResponseV1, CredentialPayload,
    CredentialVault, PluginManager,
};
use ordadb_protocol::{ClientConfig, PgCancelToken, PgClient, QueryResult};
use ordadb_types::{DbError, QueryEvent, Value};
use reqwest::{Client, Method, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tauri::{AppHandle, Emitter, State};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use windows_service::service::{ServiceAccess, ServiceState};
use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};
use zeroize::Zeroizing;

pub const DBMS_QUERY_EVENT: &str = "dbms://query";
const NATIVE_CONNECTOR_ID: &str = "ordadb-native";
const QUERY_BATCH_ROWS: usize = 1_024;
const MAX_ADMIN_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const VALID_CONNECTOR_IDS: [&str; 5] = [
    NATIVE_CONNECTOR_ID,
    "postgresql",
    "mysql",
    "sqlite",
    "sql-server",
];

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialSaved {
    credential_id: String,
    username: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SaveCredentialRequest {
    credential_id: String,
    username: String,
    password: String,
}

impl std::fmt::Debug for SaveCredentialRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SaveCredentialRequest")
            .field("credential_id", &self.credential_id)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectRequest {
    connector_id: String,
    dialect: String,
    endpoint: String,
    admin_endpoint: Option<String>,
    database: Option<String>,
    credential_id: String,
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
}

impl ConnectionProbe {
    fn new() -> Self {
        Self {
            ready: false,
            stages: Vec::with_capacity(6),
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BootstrapAdminRequest {
    username: String,
    password: String,
}

impl std::fmt::Debug for BootstrapAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BootstrapAdminRequest")
            .field("username", &self.username)
            .field("password", &"<redacted>")
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
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSnapshot {
    connection_id: String,
    connector_id: String,
    dialect: String,
    endpoint: String,
    database: String,
    mode: &'static str,
    capabilities: DbmsCapabilities,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogObject {
    kind: String,
    schema: String,
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
    sql: String,
    #[serde(default)]
    params: Vec<Option<String>>,
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
    Progress {
        rows_processed: u64,
    },
    Notice {
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
pub struct QueryUpdate {
    request_id: String,
    event: DbmsQueryEvent,
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
    connection_id: String,
    kind: AdministrationOperationKind,
    path: String,
    schema: Option<String>,
    table: Option<String>,
    format: Option<AdministrationTransferFormat>,
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

#[derive(Debug)]
struct ConnectionHandle {
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

#[derive(Debug)]
pub struct DbmsRuntime {
    credentials: CredentialVault,
    plugin_manager: Arc<PluginManager>,
    connections: RwLock<BTreeMap<String, Arc<ConnectionHandle>>>,
    requests: RwLock<BTreeMap<String, ActiveRequest>>,
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
            http,
        }))
    }

    fn connection(&self, connection_id: &str) -> Result<Arc<ConnectionHandle>, DbError> {
        validate_id(connection_id, "connection ID")?;
        read_lock(&self.connections)?
            .get(connection_id)
            .cloned()
            .ok_or_else(|| DbError::new("08003", "database connection does not exist"))
    }

    async fn connect(&self, request: ConnectRequest) -> Result<ConnectionSnapshot, DbError> {
        validate_connect_request(&request)?;
        let stored = self.credentials.load(&request.credential_id)?;
        let connection_id = Uuid::new_v4().to_string();
        let database = request
            .database
            .clone()
            .unwrap_or_else(|| "ordadb".to_owned());

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
                }),
                "native",
                DbmsCapabilities::native(),
            )
        } else {
            let mut host =
                ConnectorHost::launch(&self.plugin_manager, &request.connector_id).await?;
            host.connect(
                connection_id.clone(),
                request.endpoint.clone(),
                request.database.clone(),
                CredentialPayload::new(stored.username, stored.password.to_string()),
            )
            .await?;
            (
                ConnectionTransport::Plugin(Box::new(PluginConnection {
                    host: AsyncMutex::new(Some(host)),
                })),
                "plugin",
                DbmsCapabilities::plugin(),
            )
        };

        let snapshot = ConnectionSnapshot {
            connection_id: connection_id.clone(),
            connector_id: request.connector_id,
            dialect: request.dialect,
            endpoint: request.endpoint,
            database,
            mode,
            capabilities,
        };
        let handle = Arc::new(ConnectionHandle { transport });
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

        match tauri::async_runtime::spawn_blocking(probe_windows_service).await {
            Ok(Ok(())) => probe.passed(ConnectionProbeStageName::Service),
            Ok(Err(error)) => probe.failed(ConnectionProbeStageName::Service, error),
            Err(error) => probe.failed(
                ConnectionProbeStageName::Service,
                task_error("Windows service probe task failed", error),
            ),
        }

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
                Ok(health) if health.bootstrap_required => probe.failed(
                    ConnectionProbeStageName::Initialization,
                    DbError::new("55000", "OrdaDB requires its first administrator")
                        .with_hint("complete the local administrator setup, then retry"),
                ),
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

    async fn catalog(&self, connection_id: &str) -> Result<CatalogSnapshot, DbError> {
        let connection = self.connection(connection_id)?;
        let objects = match &connection.transport {
            ConnectionTransport::Native(native) => {
                let projection: JsonValue =
                    admin_get(&self.http, &native.admin, "/v1/catalog", true).await?;
                flatten_catalog(&projection)?
            }
            ConnectionTransport::Plugin(plugin) => {
                let request_id = Uuid::new_v4().to_string();
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                host.send(&ConnectorRequestV1::Catalog {
                    request_id: request_id.clone(),
                    connection_id: connection_id.to_owned(),
                })
                .await?;
                match host.receive().await? {
                    ConnectorResponseV1::Catalog {
                        request_id: actual,
                        entries,
                    } if actual == request_id => entries.into_iter().map(catalog_entry).collect(),
                    ConnectorResponseV1::Error { error, .. } => return Err(error),
                    _ => {
                        return Err(DbError::new(
                            "08P01",
                            "connector returned an unexpected catalog response",
                        ));
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
        match &connection.transport {
            ConnectionTransport::Native(native) => {
                let client = Arc::clone(&native.pg);
                let sql = request.sql;
                let params = request
                    .params
                    .into_iter()
                    .map(|value| value.map(String::into_bytes))
                    .collect::<Vec<_>>();
                let result = tokio::task::spawn_blocking(move || {
                    let mut client = mutex_lock(&client)?;
                    if params.is_empty() {
                        client.query(&sql)
                    } else {
                        client.query_prepared(&sql, &[], &params, QUERY_BATCH_ROWS as u32)
                    }
                })
                .await
                .map_err(join_error)??;
                emit_native_result(app, request_id, result, started.elapsed());
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
                host.send(&ConnectorRequestV1::Execute {
                    request_id: request_id.to_owned(),
                    connection_id: request.connection_id,
                    sql: request.sql,
                    params: request
                        .params
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
                let request = action.connector_request(&request_id, connection_id);
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
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

    async fn checkpoint(&self, connection_id: &str) -> Result<EngineStatus, DbError> {
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

    async fn start_administration_operation(
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

    async fn administration_service(
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
pub async fn dbms_save_credential(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: SaveCredentialRequest,
) -> DesktopResult<CredentialSaved> {
    validate_id(&request.credential_id, "credential ID")?;
    validate_text(&request.username, 1, 256, "credential username")?;
    let password = Zeroizing::new(request.password);
    runtime
        .credentials
        .store(&request.credential_id, &request.username, &password)?;
    Ok(CredentialSaved {
        credential_id: request.credential_id,
        username: request.username,
    })
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
    request: BootstrapAdminRequest,
) -> DesktopResult<BootstrapAdminResult> {
    validate_text(&request.username, 1, 128, "administrator username")?;
    validate_text(&request.password, 8, 1_024, "administrator password")?;
    let data_dir = ordadb_server::default_data_dir();
    let pipe = ordadb_server::bootstrap_pipe_name(&data_dir);
    let response =
        ordadb_server::request_bootstrap(&pipe, request.username, Zeroizing::new(request.password))
            .await?;
    Ok(BootstrapAdminResult {
        success: response.success,
        user: response.user,
        error: response.error.map(Into::into),
    })
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

fn validate_connect_request(request: &ConnectRequest) -> Result<(), DbError> {
    if !VALID_CONNECTOR_IDS.contains(&request.connector_id.as_str()) {
        return Err(DbError::new("22023", "unknown connector ID"));
    }
    if !matches!(
        request.dialect.as_str(),
        "postgresql" | "mysql" | "sqlite" | "sqlServer"
    ) {
        return Err(DbError::new("22023", "unknown SQL dialect"));
    }
    validate_text(&request.endpoint, 1, 2_048, "connection endpoint")?;
    validate_id(&request.credential_id, "credential ID")?;
    if let Some(database) = &request.database {
        validate_text(database, 1, 256, "database name")?;
    }
    if request.connector_id == NATIVE_CONNECTOR_ID && request.dialect != "postgresql" {
        return Err(DbError::new(
            "22023",
            "native OrdaDB connections use the PostgreSQL dialect",
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
    validate_text(&request.sql, 1, 4 * 1024 * 1024, "SQL text")?;
    if request.params.len() > 65_535 {
        return Err(DbError::new("54000", "parameter count exceeds 65,535"));
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
        kind: "database".into(),
        schema: String::new(),
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
            kind: "schema".into(),
            schema: schema_name.clone(),
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
            kind: kind.into(),
            schema: schema.into(),
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
            kind: kind.into(),
            schema: schema.into(),
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
        kind: entry.kind,
        schema: entry.schema,
        name: entry.name,
        parent: None,
        details: JsonValue::Null,
    }
}

fn emit_native_result(app: &AppHandle, request_id: &str, result: QueryResult, elapsed: Duration) {
    emit_query(
        app,
        request_id,
        DbmsQueryEvent::Schema {
            columns: result
                .columns
                .into_iter()
                .map(|name| QueryColumn {
                    name,
                    data_type: "text".into(),
                })
                .collect(),
        },
    );
    let mut processed = 0_u64;
    for rows in result.rows.chunks(QUERY_BATCH_ROWS) {
        processed = processed.saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
        emit_query(
            app,
            request_id,
            DbmsQueryEvent::Batch {
                rows: rows.to_vec(),
            },
        );
        emit_query(
            app,
            request_id,
            DbmsQueryEvent::Progress {
                rows_processed: processed,
            },
        );
    }
    emit_query(
        app,
        request_id,
        DbmsQueryEvent::Complete {
            command_tag: if result.command_tags.is_empty() {
                "OK".into()
            } else {
                result.command_tags.join(" · ")
            },
            duration_ms: elapsed_ms(elapsed),
        },
    );
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
            message: format!("{} · {}", notice.sql_state, notice.message),
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

fn probe_windows_service() -> Result<(), DbError> {
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
        .map_err(|error| {
            DbError::new("58030", "failed to open Windows Service Control Manager")
                .with_detail(error.to_string())
        })?;
    let service = manager
        .open_service(ordadb_server::SERVICE_NAME, ServiceAccess::QUERY_STATUS)
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
    Ok(())
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_request_debug_is_redacted() {
        let request = SaveCredentialRequest {
            credential_id: "local".into(),
            username: "dba".into(),
            password: "never-print-this".into(),
        };
        let debug = format!("{request:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("never-print-this"));
    }

    #[test]
    fn bootstrap_request_debug_is_redacted_and_probe_stages_are_structured() {
        let request = BootstrapAdminRequest {
            username: "ordadb_admin".into(),
            password: "never-print-bootstrap".into(),
        };
        let debug = format!("{request:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("never-print-bootstrap"));

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
}
