use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::stream;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use zeroize::Zeroizing;

use ordadb_catalog::Catalog;
use ordadb_engine::{Engine, EngineStatusSnapshot};
use ordadb_types::DbError;

use crate::{Action, AuthStore, Authorizer, DbObject, Principal, SessionRegistry, TokenStore};

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
    started_at: Instant,
}

impl std::fmt::Debug for AdminState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminState")
            .field("engine", &self.engine)
            .field("auth", &"<redacted>")
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

impl AdminState {
    #[must_use]
    pub fn new(engine: Arc<Engine>, auth: Arc<AuthStore>, registry: Arc<SessionRegistry>) -> Self {
        Self {
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
        .route("/v1/backups", get(backups).post(backups_unsupported))
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
) -> std::result::Result<Json<ApiEnvelope<Catalog>>, ApiError> {
    require(
        &state,
        &headers,
        Action::Read,
        DbObject::Database("ordadb".into()),
    )?;
    let catalog = (*state.engine.catalog_snapshot().map_err(ApiError::from)?).clone();
    Ok(Json(ApiEnvelope::new(catalog)))
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
    Ok(Json(ApiEnvelope::new(LockStatus {
        single_writer: true,
        active_locks: Vec::new(),
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CapabilityStatus {
    supported: bool,
    reason: &'static str,
}

async fn backups(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    require(&state, &headers, Action::Backup, DbObject::Server)?;
    Ok(Json(ApiEnvelope::new(CapabilityStatus {
        supported: false,
        reason: "logical backup jobs are implemented in the packaging milestone",
    })))
}

async fn backups_unsupported(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> std::result::Result<Response, ApiError> {
    require(&state, &headers, Action::Backup, DbObject::Server)?;
    Err(ApiError::unsupported(
        "backup job creation is not implemented in this milestone",
    ))
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
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError::from(DbError::new("28000", "authentication required")))?;
    let principal = state.tokens.authenticate(token).map_err(ApiError::from)?;
    Authorizer::from_store(&state.auth)
        .and_then(|authorizer| authorizer.authorize(&principal, action, &object))
        .map_err(ApiError::from)?;
    Ok(principal)
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

        for path in ["/v1/backups", "/v1/config"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .expect("unsupported request"),
                )
                .await
                .expect("unsupported response");
            assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED, "{path}");
        }
    }
}
