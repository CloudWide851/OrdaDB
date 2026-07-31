//! OrdaDB service configuration and listener lifecycle.

mod bootstrap;
#[cfg(windows)]
mod windows_service;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use ordadb_admin::{AdminState, AuthStore, SessionRegistry, api_router};
use ordadb_cluster::{RootAuthority, initialize_empty_v2, inspect_root, legacy_requires_migration};
use ordadb_engine::{Engine, EngineConfig};
use ordadb_protocol::{
    PgConnectionContext, PgServerConfig, load_tls_config, serve_tcp_connection_with_shutdown,
};
use ordadb_types::{DbError, Result};

pub use bootstrap::{
    BootstrapResponse, bootstrap_pipe_name, request_bootstrap, run_bootstrap_listener,
};
pub use ordadb_protocol::TlsPaths;
#[cfg(windows)]
pub use windows_service::{
    SERVICE_ACCOUNT, SERVICE_DISPLAY_NAME, SERVICE_FAILURE_ACTIONS, SERVICE_NAME,
    SERVICE_START_MODE, ServiceCommand, ServiceStartupFailureV1, ServiceStartupPhase,
    WindowsServiceStatus, dispatch_windows_service, manage_windows_service,
};

pub const DEFAULT_PG_PORT: u16 = 54_329;
pub const DEFAULT_ADMIN_PORT: u16 = 9_080;
const SERVER_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

#[must_use]
pub fn default_data_dir() -> PathBuf {
    std::env::var_os("ProgramData")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
        .join("OrdaDB")
        .join("data")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub data_dir: PathBuf,
    pub pg_bind: SocketAddr,
    pub admin_bind: SocketAddr,
    pub tls: Option<TlsPaths>,
    pub bootstrap_pipe: String,
}

impl ServerConfig {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        Self {
            bootstrap_pipe: bootstrap_pipe_name(&data_dir),
            data_dir,
            pg_bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_PG_PORT),
            admin_bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_ADMIN_PORT),
            tls: None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(invalid("data_dir must not be empty"));
        }
        if self.pg_bind == self.admin_bind && self.pg_bind.port() != 0 {
            return Err(invalid(
                "PostgreSQL and management listeners cannot share an address",
            ));
        }
        if self.bootstrap_pipe.is_empty()
            || !self
                .bootstrap_pipe
                .starts_with(r"\\.\pipe\ordadb-bootstrap-")
        {
            return Err(invalid(
                "bootstrap_pipe must use the OrdaDB local named-pipe namespace",
            ));
        }
        let remote = !self.pg_bind.ip().is_loopback() || !self.admin_bind.ip().is_loopback();
        if remote && self.tls.is_none() {
            return Err(DbError::new(
                "28000",
                "remote listeners require an explicit Rustls certificate and private key",
            ));
        }
        if let Some(tls) = &self.tls
            && (!tls.certificate.is_file() || !tls.private_key.is_file())
        {
            return Err(invalid(
                "TLS certificate and private key paths must be existing files",
            ));
        }
        Ok(())
    }
}

pub struct RunningServer {
    pub pg_address: SocketAddr,
    pub admin_address: SocketAddr,
    pub bootstrap_pipe: Option<String>,
    shutdown: CancellationToken,
    task: JoinHandle<Result<()>>,
}

impl std::fmt::Debug for RunningServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunningServer")
            .field("pg_address", &self.pg_address)
            .field("admin_address", &self.admin_address)
            .field("bootstrap_pipe", &self.bootstrap_pipe)
            .finish_non_exhaustive()
    }
}

impl RunningServer {
    pub async fn shutdown(self) -> Result<()> {
        self.shutdown.cancel();
        self.task
            .await
            .map_err(|error| internal(format!("server task failed to join: {error}")))?
    }

    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }
}

pub async fn start_server(config: ServerConfig) -> Result<RunningServer> {
    config.validate()?;
    let pg_tls = config.tls.as_ref().map(load_tls_config).transpose()?;
    let cluster = match inspect_root(&config.data_dir)? {
        RootAuthority::Empty => initialize_empty_v2(&config.data_dir)?,
        RootAuthority::LegacyV1(_) => return Err(legacy_requires_migration(&config.data_dir)),
        RootAuthority::V2(cluster) => *cluster,
    };
    let engine = Arc::new(Engine::open(EngineConfig::for_cluster(
        &cluster.database_dir,
        &config.data_dir,
        cluster.transaction_state.next_transaction_id,
    ))?);
    let auth = Arc::new(AuthStore::open(&cluster.roles_dir)?);
    let registry = Arc::new(SessionRegistry::default());
    let bootstrap_required = !auth.has_users()?;

    let pg_listener = TcpListener::bind(config.pg_bind)
        .await
        .map_err(|error| io_error("failed to bind PostgreSQL listener", error))?;
    let pg_address = pg_listener
        .local_addr()
        .map_err(|error| io_error("failed to inspect PostgreSQL listener", error))?;
    let admin_listener = std::net::TcpListener::bind(config.admin_bind)
        .map_err(|error| io_error("failed to bind management listener", error))?;
    admin_listener
        .set_nonblocking(true)
        .map_err(|error| io_error("failed to configure management listener", error))?;
    let admin_address = admin_listener
        .local_addr()
        .map_err(|error| io_error("failed to inspect management listener", error))?;

    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let bootstrap_pipe = bootstrap_required.then(|| config.bootstrap_pipe.clone());
    let bootstrap_pipe_for_task = bootstrap_pipe.clone();
    let (bootstrap_ready_sender, bootstrap_ready_receiver) = if bootstrap_required {
        let (sender, receiver) = oneshot::channel();
        (Some(sender), Some(receiver))
    } else {
        (None, None)
    };
    let pg_config = PgServerConfig::default();
    let admin_state = AdminState::new(
        Arc::clone(&engine),
        Arc::clone(&auth),
        Arc::clone(&registry),
    );
    let tls_paths = config.tls.clone();
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::<Result<()>>::new();
        tasks.spawn(run_pg_listener(
            pg_listener,
            Arc::clone(&engine),
            Arc::clone(&auth),
            Arc::clone(&registry),
            pg_config,
            pg_tls,
            task_shutdown.clone(),
        ));
        tasks.spawn(run_admin_listener(
            admin_listener,
            admin_state,
            tls_paths,
            task_shutdown.clone(),
        ));
        if let Some(pipe) = bootstrap_pipe_for_task {
            tasks.spawn(bootstrap::run_bootstrap_listener_with_ready(
                pipe,
                Arc::clone(&auth),
                task_shutdown.clone(),
                bootstrap_ready_sender,
            ));
        }

        let first = tasks.join_next().await;
        task_shutdown.cancel();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => {
                    return Err(internal(format!("server listener task failed: {error}")));
                }
            }
        }
        if let Some(result) = first {
            result.map_err(|error| internal(format!("server listener task failed: {error}")))??;
        }
        engine.checkpoint()
    });
    if let Some(receiver) = bootstrap_ready_receiver {
        let readiness = receiver.await.unwrap_or_else(|_| {
            Err(internal(
                "bootstrap listener stopped before reporting readiness",
            ))
        });
        if let Err(error) = readiness {
            shutdown.cancel();
            let _ = task.await;
            return Err(error);
        }
    }
    Ok(RunningServer {
        pg_address,
        admin_address,
        bootstrap_pipe,
        shutdown,
        task,
    })
}

async fn run_pg_listener(
    listener: TcpListener,
    engine: Arc<Engine>,
    auth: Arc<AuthStore>,
    registry: Arc<SessionRegistry>,
    config: PgServerConfig,
    tls: Option<Arc<rustls::ServerConfig>>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut connections = JoinSet::<Result<()>>::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer) = accepted
                    .map_err(|error| io_error("PostgreSQL accept failed", error))?;
                let stream = stream
                    .into_std()
                    .map_err(|error| io_error("failed to convert PostgreSQL socket", error))?;
                stream
                    .set_nonblocking(false)
                    .map_err(|error| io_error("failed to configure PostgreSQL socket", error))?;
                let engine = Arc::clone(&engine);
                let auth = Arc::clone(&auth);
                let registry = Arc::clone(&registry);
                let config = config.clone();
                let tls = tls.clone();
                let connection_shutdown = shutdown.clone();
                connections.spawn_blocking(move || {
                    let context = PgConnectionContext::new(engine, auth, registry, config, tls);
                    serve_tcp_connection_with_shutdown(
                        stream,
                        peer.to_string(),
                        context,
                        connection_shutdown,
                    )
                });
            }
        }
        while let Some(result) = connections.try_join_next() {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error))
                    if error.sql_state.starts_with("08") || error.sql_state == "28P01" =>
                {
                    registry.notice(format!("PostgreSQL connection closed: {}", error.message));
                }
                Ok(Err(error)) => {
                    registry.notice(format!("PostgreSQL connection failed: {}", error.message));
                }
                Err(error) => {
                    registry.notice(format!("PostgreSQL connection task failed: {error}"));
                }
            }
        }
    }
    while let Some(result) = connections.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.sql_state.starts_with("08") || error.sql_state == "57P01" => {}
            Ok(Err(error)) => {
                registry.notice(format!(
                    "PostgreSQL connection failed during shutdown: {}",
                    error.message
                ));
            }
            Err(error) => {
                registry.notice(format!(
                    "PostgreSQL connection task failed during shutdown: {error}"
                ));
            }
        }
    }
    Ok(())
}

async fn run_admin_listener(
    listener: std::net::TcpListener,
    state: AdminState,
    tls_paths: Option<TlsPaths>,
    shutdown: CancellationToken,
) -> Result<()> {
    let router = api_router(state);
    match tls_paths {
        Some(paths) => {
            let tls = RustlsConfig::from_pem_file(paths.certificate, paths.private_key)
                .await
                .map_err(|error| io_error("failed to load management TLS config", error))?;
            let handle = Handle::new();
            let shutdown_handle = handle.clone();
            tokio::spawn(async move {
                shutdown.cancelled().await;
                shutdown_handle.graceful_shutdown(Some(SERVER_SHUTDOWN_GRACE));
            });
            axum_server::from_tcp_rustls(listener, tls)
                .map_err(|error| io_error("failed to configure management TLS listener", error))?
                .handle(handle)
                .serve(router.into_make_service())
                .await
                .map_err(|error| io_error("management TLS listener failed", error))
        }
        None => {
            let listener = TcpListener::from_std(listener)
                .map_err(|error| io_error("failed to configure management listener", error))?;
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .map_err(|error| io_error("management listener failed", error))
        }
    }
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn internal(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message).with_hint("restart the service before retrying")
}

fn io_error(context: impl Into<String>, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use ordadb_admin::AuthStore;
    use ordadb_backup::{MigrationRunOptionsV2, migrate_v1_to_v2};
    use ordadb_cluster::resolve_active_v2;
    use ordadb_storage::DatabaseStore;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn remote_bind_requires_tls_and_ports_must_not_collide() {
        let directory = tempdir().expect("tempdir");
        let mut config = ServerConfig::new(directory.path());
        config.pg_bind = "0.0.0.0:54329".parse().expect("address");
        assert_eq!(config.validate().expect_err("tls").sql_state, "28000");
        config.pg_bind = config.admin_bind;
        assert_eq!(config.validate().expect_err("collision").sql_state, "22023");
    }

    #[tokio::test]
    async fn server_starts_on_ephemeral_loopback_ports_and_stops_cleanly() {
        let directory = tempdir().expect("tempdir");
        let mut config = ServerConfig::new(directory.path());
        config.pg_bind = "127.0.0.1:0".parse().expect("pg");
        config.admin_bind = "127.0.0.1:0".parse().expect("admin");
        let server = start_server(config).await.expect("start");
        assert!(server.pg_address.port() > 0);
        assert!(server.admin_address.port() > 0);
        assert!(server.bootstrap_pipe.is_some());
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn legacy_v1_startup_is_rejected_before_wal_or_listener_creation() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("legacy");
        drop(DatabaseStore::open(&root).expect("legacy v1 store"));
        let data_path = root.join("ordadb.data");
        let before = std::fs::read(&data_path).expect("legacy bytes");
        let mut config = ServerConfig::new(&root);
        config.pg_bind = "127.0.0.1:0".parse().expect("pg");
        config.admin_bind = "127.0.0.1:0".parse().expect("admin");

        let error = start_server(config)
            .await
            .expect_err("legacy startup refused");

        assert_eq!(error.sql_state, "0A000");
        assert!(error.message.contains("migration"));
        assert!(!root.join("ordadb.wal").exists());
        assert_eq!(std::fs::read(data_path).expect("legacy after"), before);
    }

    #[tokio::test]
    async fn migrated_shared_roles_reopen_with_the_v2_server() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("legacy");
        drop(DatabaseStore::open(&root).expect("legacy v1 store"));
        let auth = AuthStore::open(&root).expect("legacy auth");
        auth.bootstrap_admin("migration_admin", b"StrongMigrationPassword-29")
            .expect("legacy administrator");
        drop(auth);

        migrate_v1_to_v2(
            &root,
            MigrationRunOptionsV2 {
                available_bytes_override: Some(u64::MAX),
                ..MigrationRunOptionsV2::default()
            },
        )
        .expect("migrate v1 cluster");
        let active = resolve_active_v2(&root).expect("active v2 cluster");
        AuthStore::open(&active.roles_dir)
            .expect("migrated auth")
            .authenticate_password("migration_admin", b"StrongMigrationPassword-29")
            .expect("migrated administrator authenticates");

        let mut config = ServerConfig::new(&root);
        config.pg_bind = "127.0.0.1:0".parse().expect("pg");
        config.admin_bind = "127.0.0.1:0".parse().expect("admin");
        let server = start_server(config).await.expect("start migrated server");
        assert!(server.bootstrap_pipe.is_none());
        server.shutdown().await.expect("shutdown");
    }
}
