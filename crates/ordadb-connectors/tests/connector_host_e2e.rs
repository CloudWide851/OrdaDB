#![cfg(windows)]

use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::{Signer as _, SigningKey};
use ordadb_connectors::{
    CONNECTOR_API_VERSION, CONNECTOR_MANIFEST_VERSION, ConnectorArchitecture, ConnectorDialect,
    ConnectorHost, ConnectorPermission, ConnectorRequestV1, ConnectorResponseV1, CredentialPayload,
    DownloadReceipt, OperationStarted, PluginManager, PluginManagerOptions, PluginManifestV1,
    PluginProgress, PluginProgressPhase, ProtocolReady, RegistryCatalogV1, RegistryTransport,
    manifest_signing_payload, read_connector_frame, write_connector_frame,
};
use ordadb_types::{
    Batch, CommandComplete, DbError, Field, QueryEvent, QueryProgress, Row, ScalarType, Schema,
    Value,
};
use reqwest::Url;
use sha2::{Digest as _, Sha256};
use tempfile::tempdir;
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

type BoxError = Box<dyn Error + Send + Sync>;
type ProgressCallback = Arc<dyn Fn(u64, Option<u64>) + Send + Sync>;

fn main() {
    let pipe_name = connector_pipe_argument();
    if let Err(error) = run(pipe_name.as_deref()) {
        if pipe_name.is_some()
            && let Ok(executable) = std::env::current_exe()
        {
            let _ = std::fs::write(
                executable.with_extension("fixture-error.txt"),
                format!("{error:?}"),
            );
        }
        eprintln!("connector host lifecycle test failed: {error:?}");
        process::exit(1);
    }
}

fn run(pipe_name: Option<&OsStr>) -> Result<(), BoxError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    if let Some(pipe_name) = pipe_name {
        runtime.block_on(run_fixture(pipe_name))
    } else {
        runtime.block_on(run_host_lifecycle())
    }
}

fn connector_pipe_argument() -> Option<OsString> {
    let mut arguments = std::env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == OsStr::new("--ordadb-pipe") {
            return arguments.next();
        }
    }
    None
}

async fn run_host_lifecycle() -> Result<(), BoxError> {
    let executable = std::env::current_exe()?;
    let artifact = std::fs::read(&executable)?;
    let signing_key = SigningKey::from_bytes(&[29_u8; 32]);
    let manifest = signed_manifest(&signing_key, &artifact);
    let transport = Arc::new(FixtureTransport {
        catalog: serde_json::to_vec(&RegistryCatalogV1 {
            schema_version: CONNECTOR_MANIFEST_VERSION,
            plugins: vec![manifest.clone()],
        })?,
        artifact,
    });
    let directory = tempdir()?;
    let manager = PluginManager::open(
        manager_options(directory.path(), &signing_key, manifest.size),
        transport,
    )?;
    let mut progress = manager.subscribe_progress();
    let started = manager.install(&manifest.id).await?;
    let terminal = wait_terminal(&mut progress, &started).await?;
    if terminal.phase != PluginProgressPhase::Complete {
        return Err(format!("signed fixture installation failed: {terminal:?}").into());
    }

    let installed_entry = manager.active_entry(&manifest.id)?;
    let mut host = ConnectorHost::launch(&manager, &manifest.id)
        .await
        .map_err(|error| {
            let fixture_error =
                std::fs::read_to_string(installed_entry.with_extension("fixture-error.txt"))
                    .unwrap_or_else(|_| "<fixture emitted no diagnostic>".into());
            format!("{error:?}; fixture error: {fixture_error}")
        })?;
    host.connect(
        "connection-1",
        "127.0.0.1:5432",
        Some("ordadb".into()),
        CredentialPayload::new("fixture-dba", "fixture-secret"),
    )
    .await?;

    host.send(&ConnectorRequestV1::Execute {
        request_id: "query-1".into(),
        connection_id: "connection-1".into(),
        sql: "SELECT 1".into(),
        params: Vec::new(),
    })
    .await?;
    assert_schema(host.receive().await?, "query-1")?;
    assert_batch(host.receive().await?, "query-1")?;
    assert_progress(host.receive().await?, "query-1", 1)?;
    assert_complete(host.receive().await?, "query-1", "SELECT 1")?;

    host.send(&ConnectorRequestV1::Execute {
        request_id: "query-2".into(),
        connection_id: "connection-1".into(),
        sql: "SELECT wait_for_cancel".into(),
        params: Vec::new(),
    })
    .await?;
    assert_schema(host.receive().await?, "query-2")?;
    host.send(&ConnectorRequestV1::Cancel {
        request_id: "query-2".into(),
    })
    .await?;
    match host.receive().await? {
        ConnectorResponseV1::Error {
            request_id: Some(request_id),
            error,
        } if request_id == "query-2" && error.sql_state == "57014" => {}
        response => return Err(format!("unexpected cancellation response: {response:?}").into()),
    }

    host.shutdown().await?;
    Ok(())
}

async fn run_fixture(pipe_name: &OsStr) -> Result<(), BoxError> {
    let pipe_name = pipe_name
        .to_str()
        .ok_or("connector pipe name is not UTF-8")?;
    let mut pipe = ClientOptions::new().open(pipe_name)?;
    let hello: ConnectorRequestV1 = read_connector_frame(&mut pipe).await?;
    let (plugin_id, plugin_version) = match hello {
        ConnectorRequestV1::Hello {
            api_version,
            plugin_id,
            plugin_version,
        } if api_version == CONNECTOR_API_VERSION => (plugin_id, plugin_version),
        request => return Err(format!("unexpected connector handshake: {request:?}").into()),
    };
    write_connector_frame(
        &mut pipe,
        &ConnectorResponseV1::Ready(ProtocolReady {
            api_version: CONNECTOR_API_VERSION,
            plugin_id,
            plugin_version,
        }),
    )
    .await?;

    let mut pending_cancellation = None;
    loop {
        let request: ConnectorRequestV1 = read_connector_frame(&mut pipe).await?;
        match request {
            ConnectorRequestV1::Connect {
                connection_id,
                endpoint,
                database,
                credential,
            } => {
                if endpoint != "127.0.0.1:5432"
                    || database.as_deref() != Some("ordadb")
                    || credential.username != "fixture-dba"
                    || credential.password.as_str() != "fixture-secret"
                {
                    return Err("connector received an unexpected connection payload".into());
                }
                write_connector_frame(&mut pipe, &ConnectorResponseV1::Connected { connection_id })
                    .await?;
            }
            ConnectorRequestV1::Execute {
                request_id,
                connection_id,
                sql,
                params,
            } => {
                if connection_id != "connection-1" || !params.is_empty() {
                    return Err("connector received an unexpected query payload".into());
                }
                send_schema(&mut pipe, &request_id).await?;
                if sql == "SELECT 1" {
                    send_successful_query(&mut pipe, &request_id).await?;
                } else if sql == "SELECT wait_for_cancel" {
                    pending_cancellation = Some(request_id);
                } else {
                    return Err(format!("unexpected fixture SQL: {sql}").into());
                }
            }
            ConnectorRequestV1::Cancel { request_id }
                if pending_cancellation.as_deref() == Some(request_id.as_str()) =>
            {
                pending_cancellation = None;
                write_connector_frame(
                    &mut pipe,
                    &ConnectorResponseV1::Error {
                        request_id: Some(request_id),
                        error: DbError::new("57014", "query cancelled"),
                    },
                )
                .await?;
            }
            ConnectorRequestV1::Shutdown => {
                write_connector_frame(&mut pipe, &ConnectorResponseV1::Shutdown).await?;
                return Ok(());
            }
            request => return Err(format!("unexpected connector request: {request:?}").into()),
        }
    }
}

async fn send_schema(pipe: &mut NamedPipeClient, request_id: &str) -> Result<(), DbError> {
    write_connector_frame(
        pipe,
        &ConnectorResponseV1::QueryEvent {
            request_id: request_id.into(),
            event: QueryEvent::Schema(Schema::new(vec![Field::new(
                "value",
                ScalarType::Int32,
                false,
            )])),
        },
    )
    .await
}

async fn send_successful_query(
    pipe: &mut NamedPipeClient,
    request_id: &str,
) -> Result<(), DbError> {
    let schema = Schema::new(vec![Field::new("value", ScalarType::Int32, false)]);
    for event in [
        QueryEvent::Batch(Batch {
            schema,
            rows: vec![Row::new(vec![Value::Int32(1)])],
        }),
        QueryEvent::Progress(QueryProgress { rows_processed: 1 }),
        QueryEvent::Complete(CommandComplete {
            tag: "SELECT 1".into(),
            rows_affected: 1,
        }),
    ] {
        write_connector_frame(
            pipe,
            &ConnectorResponseV1::QueryEvent {
                request_id: request_id.into(),
                event,
            },
        )
        .await?;
    }
    Ok(())
}

fn assert_schema(response: ConnectorResponseV1, request_id: &str) -> Result<(), BoxError> {
    match response {
        ConnectorResponseV1::QueryEvent {
            request_id: actual,
            event: QueryEvent::Schema(schema),
        } if actual == request_id && schema.fields.len() == 1 => Ok(()),
        response => Err(format!("unexpected schema response: {response:?}").into()),
    }
}

fn assert_batch(response: ConnectorResponseV1, request_id: &str) -> Result<(), BoxError> {
    match response {
        ConnectorResponseV1::QueryEvent {
            request_id: actual,
            event: QueryEvent::Batch(batch),
        } if actual == request_id && batch.rows == vec![Row::new(vec![Value::Int32(1)])] => Ok(()),
        response => Err(format!("unexpected batch response: {response:?}").into()),
    }
}

fn assert_progress(
    response: ConnectorResponseV1,
    request_id: &str,
    rows_processed: u64,
) -> Result<(), BoxError> {
    match response {
        ConnectorResponseV1::QueryEvent {
            request_id: actual,
            event: QueryEvent::Progress(progress),
        } if actual == request_id && progress.rows_processed == rows_processed => Ok(()),
        response => Err(format!("unexpected progress response: {response:?}").into()),
    }
}

fn assert_complete(
    response: ConnectorResponseV1,
    request_id: &str,
    tag: &str,
) -> Result<(), BoxError> {
    match response {
        ConnectorResponseV1::QueryEvent {
            request_id: actual,
            event: QueryEvent::Complete(complete),
        } if actual == request_id && complete.tag == tag && complete.rows_affected == 1 => Ok(()),
        response => Err(format!("unexpected completion response: {response:?}").into()),
    }
}

async fn wait_terminal(
    receiver: &mut broadcast::Receiver<PluginProgress>,
    operation: &OperationStarted,
) -> Result<PluginProgress, BoxError> {
    Ok(tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let progress = receiver.recv().await?;
            if progress.operation_id == operation.operation_id
                && matches!(
                    progress.phase,
                    PluginProgressPhase::Complete
                        | PluginProgressPhase::Cancelled
                        | PluginProgressPhase::Failed
                )
            {
                return Ok::<_, broadcast::error::RecvError>(progress);
            }
        }
    })
    .await??)
}

fn signed_manifest(signing_key: &SigningKey, artifact: &[u8]) -> PluginManifestV1 {
    let mut manifest = PluginManifestV1 {
        schema_version: CONNECTOR_MANIFEST_VERSION,
        id: "ordadb-postgresql".into(),
        display_name: "OrdaDB PostgreSQL Fixture".into(),
        version: "1.0.0".into(),
        api_version: CONNECTOR_API_VERSION,
        architecture: ConnectorArchitecture::WindowsX64,
        dialect: ConnectorDialect::PostgreSql,
        publisher: "OrdaDB Test".into(),
        permissions: vec![ConnectorPermission::Network],
        entry: "ordadb-postgresql.exe".into(),
        size: u64::try_from(artifact.len()).expect("fixture executable size"),
        sha256: Sha256::digest(artifact)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        signature: String::new(),
        minimum_host_version: "0.1.0".into(),
        download_url: "https://plugins.ordadb.test/v1/ordadb-postgresql.exe".into(),
    };
    manifest.signature = BASE64.encode(
        signing_key
            .sign(&manifest_signing_payload(&manifest).expect("manifest payload"))
            .to_bytes(),
    );
    manifest
}

fn manager_options(
    root: &Path,
    signing_key: &SigningKey,
    artifact_size: u64,
) -> PluginManagerOptions {
    let mut options = PluginManagerOptions::new(root);
    options.registry_url = Some("https://plugins.ordadb.test/catalog.json".into());
    options.registry_public_key = Some(BASE64.encode(signing_key.verifying_key().to_bytes()));
    options.maximum_artifact_bytes = artifact_size.saturating_add(1);
    options
}

#[derive(Debug)]
struct FixtureTransport {
    catalog: Vec<u8>,
    artifact: Vec<u8>,
}

#[async_trait]
impl RegistryTransport for FixtureTransport {
    async fn fetch(
        &self,
        _url: &Url,
        maximum_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, DbError> {
        if cancellation.is_cancelled() {
            return Err(DbError::new("57014", "fixture catalog fetch cancelled"));
        }
        if u64::try_from(self.catalog.len()).unwrap_or(u64::MAX) > maximum_bytes {
            return Err(DbError::new("54000", "fixture catalog exceeds size limit"));
        }
        Ok(self.catalog.clone())
    }

    async fn download_to(
        &self,
        _url: &Url,
        destination: &Path,
        maximum_bytes: u64,
        cancellation: &CancellationToken,
        progress: ProgressCallback,
    ) -> Result<DownloadReceipt, DbError> {
        if cancellation.is_cancelled() {
            return Err(DbError::new("57014", "fixture download cancelled"));
        }
        let bytes = u64::try_from(self.artifact.len()).unwrap_or(u64::MAX);
        if bytes > maximum_bytes {
            return Err(DbError::new("54000", "fixture artifact exceeds size limit"));
        }
        std::fs::write(destination, &self.artifact).map_err(|error| {
            DbError::new("58030", "failed to write fixture").with_detail(error.to_string())
        })?;
        progress(bytes, Some(bytes));
        Ok(DownloadReceipt {
            bytes,
            sha256: Sha256::digest(&self.artifact).into(),
        })
    }
}
