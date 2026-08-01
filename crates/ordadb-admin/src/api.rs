use std::collections::BTreeSet;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::stream;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;
use zeroize::Zeroizing;

use ordadb_backup::{TableTransferRequest, TransferFormat};
use ordadb_catalog::{
    Catalog, ColumnDefinition, ConstraintDefinition, IndexDefinition, RoutineArgument, RoutineKind,
    SequenceDefinition, TriggerDefinition, ViewKind,
};
use ordadb_engine::{Engine, EngineStatusSnapshot};
use ordadb_types::{
    DatabaseId, DbError, Identifier, RoutineId, ScalarType, Schema, SchemaId, TableId, ViewId,
};

use crate::{
    Action, AuthStore, Authorizer, DbObject, OperationManager, Principal, SessionRegistry,
    StartOperation, TokenStore,
};

const API_VERSION: &str = "v1";

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiEnvelope<T> {
    pub api_version: &'static str,
    pub data: T,
}

impl<T> ApiEnvelope<T> {
    fn new(data: T) -> Self {
        Self {
            api_version: API_VERSION,
            data,
        }
    }
}

#[derive(Clone)]
pub struct AdminState {
    pub engine: Arc<Engine>,
    pub auth: Arc<AuthStore>,
    pub tokens: Arc<TokenStore>,
    pub registry: Arc<SessionRegistry>,
    pub operations: Arc<OperationManager>,
    started_at: Instant,
}

impl std::fmt::Debug for AdminState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminState")
            .field("engine", &self.engine)
            .field("auth", &"<redacted>")
            .field("registry", &self.registry)
            .field("operations", &self.operations)
            .finish_non_exhaustive()
    }
}

impl AdminState {
    #[must_use]
    pub fn new(engine: Arc<Engine>, auth: Arc<AuthStore>, registry: Arc<SessionRegistry>) -> Self {
        let operations_root = engine.config().cluster_root.join("operations");
        Self {
            operations: OperationManager::new(Arc::clone(&engine), operations_root),
            engine,
            auth,
            tokens: Arc::new(TokenStore::default()),
            registry,
            started_at: Instant::now(),
        }
    }
}

pub fn api_router(state: AdminState) -> Router {
    Router::new()
        .route("/v1/health/live", get(live))
        .route("/v1/health/ready", get(ready))
        .route("/v1/auth/token", post(issue_token))
        .route("/v1/catalog", get(catalog))
        .route("/v1/sessions", get(sessions))
        .route("/v1/locks", get(locks))
        .route("/v1/queries", get(queries))
        .route("/v1/metrics", get(metrics))
        .route("/v1/storage", get(storage))
        .route("/v1/wal", get(wal))
        .route("/v1/checkpoint", post(checkpoint))
        .route("/v1/backups", get(backups).post(start_backup))
        .route("/v1/restores", post(start_restore))
        .route("/v1/imports", post(start_import))
        .route("/v1/exports", post(start_export))
        .route("/v1/operations", get(operations))
        .route("/v1/operations/{operation_id}", get(operation))
        .route(
            "/v1/operations/{operation_id}/cancel",
            post(cancel_operation),
        )
        .route("/v1/service", get(service))
        .route("/v1/config", get(config).post(config_unsupported))
        .route("/v1/logs/stream", get(log_stream))
        .with_state(state)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Health {
    status: &'static str,
    version: &'static str,
    uptime_seconds: u64,
    bootstrap_required: bool,
}

async fn live(State(state): State<AdminState>) -> Json<ApiEnvelope<Health>> {
    Json(ApiEnvelope::new(Health {
        status: "live",
        version: env!("CARGO_PKG_VERSION"),
        uptime_seconds: state.started_at.elapsed().as_secs(),
        bootstrap_required: false,
    }))
}

async fn ready(
    State(state): State<AdminState>,
) -> std::result::Result<Json<ApiEnvelope<Health>>, ApiError> {
    let bootstrap_required = !state.auth.has_users().map_err(ApiError::from)?;
    let status = if bootstrap_required {
        "bootstrap_required"
    } else {
        "ready"
    };
    Ok(Json(ApiEnvelope::new(Health {
        status,
        version: env!("CARGO_PKG_VERSION"),
        uptime_seconds: state.started_at.elapsed().as_secs(),
        bootstrap_required,
    })))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenRequest {
    username: String,
    password: String,
}

async fn issue_token(
    State(state): State<AdminState>,
    Json(request): Json<TokenRequest>,
) -> std::result::Result<impl IntoResponse, ApiError> {
    let password = Zeroizing::new(request.password.into_bytes());
    let token = state
        .tokens
        .issue(&state.auth, &request.username, &password)
        .map_err(ApiError::from)?;
    Ok((StatusCode::CREATED, Json(ApiEnvelope::new(token))))
}

async fn catalog(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<Json<ApiEnvelope<CatalogProjection>>, ApiError> {
    require(
        &state,
        &headers,
        Action::Read,
        DbObject::Database("ordadb".into()),
    )?;
    let catalog = state.engine.catalog_snapshot().map_err(ApiError::from)?;
    Ok(Json(ApiEnvelope::new(CatalogProjection::from_catalog(
        &catalog,
    ))))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogProjection {
    database: DatabaseProjection,
}

impl CatalogProjection {
    fn from_catalog(catalog: &Catalog) -> Self {
        let database = catalog.database();
        Self {
            database: DatabaseProjection {
                id: database.id,
                name: database.name.clone(),
                schemas: database
                    .schemas()
                    .map(|schema| {
                        let materialized_tables = schema
                            .views()
                            .filter_map(|view| view.materialized_table_id)
                            .collect::<BTreeSet<_>>();
                        SchemaProjection {
                            id: schema.id,
                            name: schema.name.clone(),
                            tables: schema
                                .tables()
                                .filter(|table| !materialized_tables.contains(&table.id))
                                .map(|table| TableProjection {
                                    id: table.id,
                                    name: table.name.clone(),
                                    columns: table.columns().to_vec(),
                                    indexes: table.indexes().cloned().collect(),
                                    constraints: table.constraints().cloned().collect(),
                                    triggers: table.triggers().cloned().collect(),
                                })
                                .collect(),
                            sequences: schema.sequences().cloned().collect(),
                            views: schema
                                .views()
                                .map(|view| ViewProjection {
                                    id: view.id,
                                    name: view.name.clone(),
                                    kind: view.kind,
                                    output: view.output.clone(),
                                    populated: view.populated,
                                    indexes: view
                                        .materialized_table_id
                                        .and_then(|table_id| catalog.table_by_id(table_id))
                                        .map(|table| table.indexes().cloned().collect())
                                        .unwrap_or_default(),
                                })
                                .collect(),
                            routines: schema
                                .routines()
                                .map(|routine| RoutineProjection {
                                    id: routine.id,
                                    name: routine.name.clone(),
                                    kind: routine.kind,
                                    arguments: routine.arguments.clone(),
                                    return_type: routine.return_type.clone(),
                                    returns_set: routine.returns_set,
                                    language: routine.language.clone(),
                                })
                                .collect(),
                        }
                    })
                    .collect(),
            },
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DatabaseProjection {
    id: DatabaseId,
    name: Identifier,
    schemas: Vec<SchemaProjection>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SchemaProjection {
    id: SchemaId,
    name: Identifier,
    tables: Vec<TableProjection>,
    sequences: Vec<SequenceDefinition>,
    views: Vec<ViewProjection>,
    routines: Vec<RoutineProjection>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TableProjection {
    id: TableId,
    name: Identifier,
    columns: Vec<ColumnDefinition>,
    indexes: Vec<IndexDefinition>,
    constraints: Vec<ConstraintDefinition>,
    triggers: Vec<TriggerDefinition>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ViewProjection {
    id: ViewId,
    name: Identifier,
    kind: ViewKind,
    output: Schema,
    populated: bool,
    indexes: Vec<IndexDefinition>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RoutineProjection {
    id: RoutineId,
    name: Identifier,
    kind: RoutineKind,
    arguments: Vec<RoutineArgument>,
    return_type: Option<ScalarType>,
    returns_set: bool,
    language: String,
}

async fn sessions(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Monitor, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(
        state.registry.sessions().map_err(ApiError::from)?,
    )))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LockStatus {
    single_writer: bool,
    active_locks: Vec<String>,
}

async fn locks(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Monitor, DbObject::Server)?;
    let (granted, waiting) = state.engine.lock_snapshot().map_err(ApiError::from)?;
    let mut active_locks = Vec::with_capacity(granted.len() + waiting.len());
    active_locks.extend(granted.into_iter().map(|lock| {
        format!(
            "granted transaction={} mode={:?} resource={:?}",
            lock.transaction_id, lock.mode, lock.key
        )
    }));
    active_locks.extend(waiting.into_iter().map(|lock| {
        let blocked_by = lock
            .blocked_by
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "waiting transaction={} mode={:?} resource={:?} blockedBy={blocked_by}",
            lock.transaction_id, lock.mode, lock.key
        )
    }));
    Ok(Json(ApiEnvelope::new(LockStatus {
        single_writer: false,
        active_locks,
    })))
}

async fn queries(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Monitor, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(
        state.registry.queries().map_err(ApiError::from)?,
    )))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Metrics {
    active_sessions: usize,
    active_queries: usize,
    engine: SerializableEngineStatus,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SerializableEngineStatus {
    data_format_version: u16,
    generation: u64,
    table_count: usize,
    row_count: u64,
    index_count: usize,
    durable_lsn: Option<u64>,
    dirty_page_count: usize,
    commits_since_checkpoint: u64,
}

impl From<EngineStatusSnapshot> for SerializableEngineStatus {
    fn from(status: EngineStatusSnapshot) -> Self {
        Self {
            data_format_version: status.data_format.version(),
            generation: status.generation,
            table_count: status.table_count,
            row_count: status.row_count,
            index_count: status.index_count,
            durable_lsn: status.durable_lsn,
            dirty_page_count: status.dirty_page_count,
            commits_since_checkpoint: status.commits_since_checkpoint,
        }
    }
}

async fn metrics(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Monitor, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(Metrics {
        active_sessions: state
            .registry
            .active_session_count()
            .map_err(ApiError::from)?,
        active_queries: state
            .registry
            .active_query_count()
            .map_err(ApiError::from)?,
        engine: state
            .engine
            .status_snapshot()
            .map_err(ApiError::from)?
            .into(),
    })))
}

async fn storage(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Monitor, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(SerializableEngineStatus::from(
        state.engine.status_snapshot().map_err(ApiError::from)?,
    ))))
}

async fn wal(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    storage(State(state), headers).await
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointResult {
    completed: bool,
}

async fn checkpoint(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Manage, DbObject::Server)?;
    state.engine.checkpoint().map_err(ApiError::from)?;
    state.registry.notice("manual checkpoint completed");
    Ok(Json(ApiEnvelope::new(CheckpointResult { completed: true })))
}

async fn backups(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Backup, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(
        state.operations.backups().map_err(ApiError::from)?,
    )))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PathOperationRequest {
    path: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransferOperationRequest {
    schema: String,
    table: String,
    path: PathBuf,
    format: TransferFormat,
}

async fn start_backup(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<PathOperationRequest>,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Backup, DbObject::Server)?;
    let operation = state
        .operations
        .start(StartOperation::Backup { path: request.path })
        .map_err(ApiError::from)?;
    state
        .registry
        .notice(format!("logical backup {} queued", operation.operation_id));
    Ok((StatusCode::ACCEPTED, Json(ApiEnvelope::new(operation))))
}

async fn start_restore(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<PathOperationRequest>,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Manage, DbObject::Server)?;
    let operation = state
        .operations
        .start(StartOperation::Restore { path: request.path })
        .map_err(ApiError::from)?;
    state
        .registry
        .notice(format!("logical restore {} queued", operation.operation_id));
    Ok((StatusCode::ACCEPTED, Json(ApiEnvelope::new(operation))))
}

async fn start_import(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<TransferOperationRequest>,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(
        &state,
        &headers,
        Action::Write,
        DbObject::Table(format!("{}.{}", request.schema, request.table)),
    )?;
    let operation = state
        .operations
        .start(StartOperation::Import {
            request: TableTransferRequest {
                schema: request.schema,
                table: request.table,
                path: request.path,
                format: request.format,
            },
        })
        .map_err(ApiError::from)?;
    Ok((StatusCode::ACCEPTED, Json(ApiEnvelope::new(operation))))
}

async fn start_export(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<TransferOperationRequest>,
) -> std::result::Result<impl IntoResponse, ApiError> {
    let object = DbObject::Table(format!("{}.{}", request.schema, request.table));
    let principal = authenticate(&state, &headers)?;
    Authorizer::from_store(&state.auth)
        .and_then(|authorizer| {
            authorizer.authorize_all(&principal, &[Action::Read, Action::Backup], &object)
        })
        .map_err(ApiError::from)?;
    let operation = state
        .operations
        .start(StartOperation::Export {
            request: TableTransferRequest {
                schema: request.schema,
                table: request.table,
                path: request.path,
                format: request.format,
            },
        })
        .map_err(ApiError::from)?;
    Ok((StatusCode::ACCEPTED, Json(ApiEnvelope::new(operation))))
}

async fn operations(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Monitor, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(
        state.operations.list().map_err(ApiError::from)?,
    )))
}

async fn operation(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(operation_id): AxumPath<Uuid>,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Monitor, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(
        state.operations.get(operation_id).map_err(ApiError::from)?,
    )))
}

async fn cancel_operation(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(operation_id): AxumPath<Uuid>,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Manage, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(
        state
            .operations
            .cancel(operation_id)
            .map_err(ApiError::from)?,
    )))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ServiceStatus {
    name: &'static str,
    process_running: bool,
    windows_service_supported: bool,
    data_dir: PathBuf,
    operations_root: PathBuf,
}

async fn service(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Monitor, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(ServiceStatus {
        name: "OrdaDB",
        process_running: true,
        windows_service_supported: cfg!(windows),
        data_dir: state.engine.config().cluster_root.clone(),
        operations_root: state.operations.operations_root().to_path_buf(),
    })))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicConfig {
    data_dir: PathBuf,
    pg_bind: &'static str,
    admin_bind: &'static str,
    remote_requires_tls: bool,
}

async fn config(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Manage, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(PublicConfig {
        data_dir: state.engine.config().data_dir.clone(),
        pg_bind: "127.0.0.1:54329",
        admin_bind: "127.0.0.1:9080",
        remote_requires_tls: true,
    })))
}

async fn config_unsupported(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<Response, ApiError> {
    require(&state, &headers, Action::Manage, DbObject::Server)?;
    Err(ApiError::unsupported(
        "runtime configuration mutation is not implemented in this milestone",
    ))
}

async fn log_stream(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<
    Sse<impl futures_core::Stream<Item = std::result::Result<Event, Infallible>>>,
    ApiError,
> {
    require(&state, &headers, Action::Monitor, DbObject::Server)?;
    let mut receiver = state.registry.subscribe();
    let events = stream! {
        loop {
            match receiver.recv().await {
                Ok(event) => {
                    let payload = match serde_json::to_string(&event) {
                        Ok(payload) => payload,
                        Err(_) => break,
                    };
                    yield Ok(Event::default().event("ordadb").data(payload));
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    yield Ok(Event::default().event("gap").data(skipped.to_string()));
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(events).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

fn require(
    state: &AdminState,
    headers: &HeaderMap,
    action: Action,
    object: DbObject,
) -> std::result::Result<Principal, ApiError> {
    let principal = authenticate(state, headers)?;
    Authorizer::from_store(&state.auth)
        .and_then(|authorizer| authorizer.authorize(&principal, action, &object))
        .map_err(ApiError::from)?;
    Ok(principal)
}

fn authenticate(
    state: &AdminState,
    headers: &HeaderMap,
) -> std::result::Result<Principal, ApiError> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError::from(DbError::new("28000", "authentication required")))?;
    state.tokens.authenticate(token).map_err(ApiError::from)
}

struct ApiError {
    status: StatusCode,
    error: DbError,
}

impl ApiError {
    fn unsupported(message: &str) -> Self {
        Self {
            status: StatusCode::NOT_IMPLEMENTED,
            error: DbError::new("0A000", message),
        }
    }
}

impl From<DbError> for ApiError {
    fn from(error: DbError) -> Self {
        let status = if error.sql_state == "42501" {
            StatusCode::FORBIDDEN
        } else {
            match error.sql_state.get(0..2) {
                Some("22" | "42") => StatusCode::BAD_REQUEST,
                Some("28") => StatusCode::UNAUTHORIZED,
                Some("3F") => StatusCode::NOT_FOUND,
                Some("0A") => StatusCode::NOT_IMPLEMENTED,
                Some("53") => StatusCode::INSUFFICIENT_STORAGE,
                Some("55") => StatusCode::CONFLICT,
                Some("58" | "XX") => StatusCode::INTERNAL_SERVER_ERROR,
                _ => StatusCode::FORBIDDEN,
            }
        };
        Self { status, error }
    }
}

#[derive(Serialize)]
struct ApiErrorEnvelope {
    api_version: &'static str,
    error: DbError,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ApiErrorEnvelope {
                api_version: API_VERSION,
                error: self.error,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, header};
    use serde_json::Value;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use ordadb_engine::EngineConfig;

    use super::*;

    fn state() -> (tempfile::TempDir, AdminState) {
        let directory = tempdir().expect("tempdir");
        let engine = Arc::new(
            Engine::open(EngineConfig::new(directory.path().join("data"))).expect("engine"),
        );
        let auth = Arc::new(AuthStore::open(directory.path().join("data")).expect("auth store"));
        auth.bootstrap_admin("dba", b"correct horse battery staple")
            .expect("bootstrap");
        let registry = Arc::new(SessionRegistry::default());
        (directory, AdminState::new(engine, auth, registry))
    }

    #[tokio::test]
    async fn health_is_public_but_catalog_requires_a_bearer_token() {
        let (_directory, state) = state();
        let app = api_router(state);
        let live = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/health/live")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("live");
        assert_eq!(live.status(), StatusCode::OK);
        let catalog = app
            .oneshot(
                Request::builder()
                    .uri("/v1/catalog")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("catalog");
        assert_eq!(catalog.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn issued_token_can_read_metrics() {
        let (_directory, state) = state();
        let app = api_router(state);
        let token_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/auth/token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"username":"dba","password":"correct horse battery staple"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("token");
        assert_eq!(token_response.status(), StatusCode::CREATED);
        let bytes = to_bytes(token_response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        let token = body["data"]["accessToken"].as_str().expect("token");
        let metrics = app
            .oneshot(
                Request::builder()
                    .uri("/v1/metrics")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("metrics");
        assert_eq!(metrics.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn lock_route_projects_active_engine_locks() {
        let (_directory, state) = state();
        let token = state
            .tokens
            .issue(&state.auth, "dba", b"correct horse battery staple")
            .expect("token")
            .access_token;
        let mut session = state.engine.connect().expect("session");
        session
            .execute("CREATE TABLE lock_probe (id INT PRIMARY KEY)", &[])
            .expect("create table");
        let mut transaction = session.begin().expect("transaction");
        transaction
            .execute("INSERT INTO lock_probe VALUES (1)", &[])
            .expect("insert");
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/locks")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("locks");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(body["data"]["singleWriter"], false);
        assert!(
            body["data"]["activeLocks"]
                .as_array()
                .expect("active locks")
                .iter()
                .any(|lock| lock.as_str().is_some_and(|lock| {
                    lock.starts_with("granted transaction=") && lock.contains("resource=")
                }))
        );
        transaction.rollback().expect("rollback");
    }

    #[test]
    fn catalog_projection_exposes_only_safe_search_index_metadata() {
        let (_directory, state) = state();
        let mut session = state.engine.connect().expect("session");
        for sql in [
            "CREATE TABLE documents (title TEXT, embedding VECTOR(3))",
            "CREATE INDEX documents_fts ON documents USING fulltext (title) \
             WITH (analyzer = 'whitespace')",
            "CREATE INDEX documents_hnsw ON documents USING hnsw (embedding) \
             WITH (metric = 'cosine', m = 8, ef_construction = 32, ef_search = 24)",
        ] {
            session
                .execute_stream(sql, &[])
                .expect("execute")
                .collect::<ordadb_types::Result<Vec<_>>>()
                .expect("drain");
        }
        let catalog = state.engine.catalog_snapshot().expect("catalog");
        let projection =
            serde_json::to_value(CatalogProjection::from_catalog(&catalog)).expect("projection");
        let indexes = projection["database"]["schemas"][0]["tables"][0]["indexes"]
            .as_array()
            .expect("indexes");
        assert_eq!(indexes[0]["method"], "full_text");
        assert_eq!(indexes[0]["options"]["kind"], "full_text");
        assert_eq!(indexes[0]["options"]["analyzer"], "whitespace");
        assert_eq!(indexes[1]["method"], "hnsw");
        assert_eq!(indexes[1]["options"]["kind"], "hnsw");
        assert!(indexes[1].get("path").is_none());
        assert!(indexes[1].get("graph").is_none());
    }

    #[tokio::test]
    async fn every_management_route_has_an_authenticated_contract() {
        let (_directory, state) = state();
        let token = state
            .tokens
            .issue(&state.auth, "dba", b"correct horse battery staple")
            .expect("token")
            .access_token;
        let app = api_router(state);

        let ready = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/health/ready")
                    .body(Body::empty())
                    .expect("ready request"),
            )
            .await
            .expect("ready");
        assert_eq!(ready.status(), StatusCode::OK);

        for path in [
            "/v1/catalog",
            "/v1/sessions",
            "/v1/locks",
            "/v1/queries",
            "/v1/metrics",
            "/v1/storage",
            "/v1/wal",
            "/v1/backups",
            "/v1/operations",
            "/v1/service",
            "/v1/config",
            "/v1/logs/stream",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .expect("authorized request"),
                )
                .await
                .expect("authorized response");
            assert_eq!(response.status(), StatusCode::OK, "{path}");
        }

        let checkpoint = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoint")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("checkpoint request"),
            )
            .await
            .expect("checkpoint");
        assert_eq!(checkpoint.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/config")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("unsupported request"),
            )
            .await
            .expect("unsupported response");
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn backup_route_starts_a_real_bounded_operation() {
        let (_directory, state) = state();
        let token = state
            .tokens
            .issue(&state.auth, "dba", b"correct horse battery staple")
            .expect("token")
            .access_token;
        let app = api_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/backups")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"path":"api-backup.orda"}"#))
                    .expect("backup request"),
            )
            .await
            .expect("backup response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&body).expect("json");
        let operation_id =
            Uuid::parse_str(body["data"]["operationId"].as_str().expect("operation ID"))
                .expect("UUID");
        for _ in 0..100 {
            let operation = state.operations.get(operation_id).expect("operation");
            if !matches!(
                operation.state,
                crate::OperationState::Queued | crate::OperationState::Running
            ) {
                assert_eq!(operation.state, crate::OperationState::Succeeded);
                assert_eq!(operation.path, PathBuf::from("api-backup.orda"));
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("backup operation did not finish");
    }
}
