use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt as _;
use ordadb_types::{DbError, Result};
use reqwest::{Client, Url};
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::manifest::{decode_sha256, validate_plugin_id};
use crate::{
    CONNECTOR_API_VERSION, CONNECTOR_MANIFEST_VERSION, ManifestPolicy, PluginManifestV1,
    RegistryCatalogV1, decode_public_key, invalid, io_error, network_error, security_error,
    validate_manifest,
};

const STATE_VERSION: u32 = 1;
const MAXIMUM_CATALOG_BYTES: u64 = 1024 * 1024;
const MAXIMUM_STATE_BYTES: u64 = 4 * 1024 * 1024;
const DEFAULT_MAXIMUM_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_CATALOG_PLUGINS: usize = 64;
const REGISTRY_TIMEOUT: Duration = Duration::from_secs(30);
const STATE_FILE: &str = "state-v1.json";
const MANIFEST_FILE: &str = "manifest-v1.json";

type ProgressCallback = Arc<dyn Fn(u64, Option<u64>) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RegistryAvailability {
    Configured,
    NotConfigured,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegistryStatus {
    pub availability: RegistryAvailability,
    pub api_version: u32,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PluginLifecycle {
    Available,
    Downloading,
    Verifying,
    Installing,
    Installed,
    UpdateAvailable,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PluginProgressPhase {
    Resolving,
    Downloading,
    Verifying,
    Installing,
    Complete,
    Cancelled,
    Failed,
}

impl PluginProgressPhase {
    const fn lifecycle(self) -> PluginLifecycle {
        match self {
            Self::Resolving | Self::Downloading => PluginLifecycle::Downloading,
            Self::Verifying => PluginLifecycle::Verifying,
            Self::Installing => PluginLifecycle::Installing,
            Self::Complete => PluginLifecycle::Installed,
            Self::Cancelled => PluginLifecycle::Available,
            Self::Failed => PluginLifecycle::Failed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OperationKind {
    Install,
    Retry,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationStarted {
    pub operation_id: String,
    pub plugin_id: String,
    pub kind: OperationKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginProgress {
    pub operation_id: String,
    pub plugin_id: String,
    pub kind: OperationKind,
    pub phase: PluginProgressPhase,
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
    pub error: Option<DbError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginCatalogItem {
    pub id: String,
    pub display_name: String,
    pub version: String,
    pub dialect: crate::ConnectorDialect,
    pub publisher: String,
    pub permissions: Vec<crate::ConnectorPermission>,
    pub size: u64,
    pub lifecycle: PluginLifecycle,
    pub installed_version: Option<String>,
    pub previous_version: Option<String>,
    pub operation_id: Option<String>,
    pub downloaded_bytes: u64,
    pub error: Option<DbError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginCatalogSnapshot {
    pub registry: RegistryStatus,
    pub plugins: Vec<PluginCatalogItem>,
}

#[derive(Debug, Clone)]
pub struct PluginManagerOptions {
    pub plugin_root: PathBuf,
    pub registry_url: Option<String>,
    pub registry_public_key: Option<String>,
    pub host_version: String,
    pub maximum_artifact_bytes: u64,
}

impl PluginManagerOptions {
    #[must_use]
    pub fn new(plugin_root: impl Into<PathBuf>) -> Self {
        Self {
            plugin_root: plugin_root.into(),
            registry_url: None,
            registry_public_key: None,
            host_version: env!("CARGO_PKG_VERSION").into(),
            maximum_artifact_bytes: DEFAULT_MAXIMUM_ARTIFACT_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadReceipt {
    pub bytes: u64,
    pub sha256: [u8; 32],
}

#[async_trait]
pub trait RegistryTransport: Send + Sync {
    async fn fetch(
        &self,
        url: &Url,
        maximum_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>>;

    async fn download_to(
        &self,
        url: &Url,
        destination: &Path,
        maximum_bytes: u64,
        cancellation: &CancellationToken,
        progress: ProgressCallback,
    ) -> Result<DownloadReceipt>;
}

#[derive(Debug, Clone)]
pub struct HttpsRegistryTransport {
    client: Client,
}

impl HttpsRegistryTransport {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REGISTRY_TIMEOUT)
            .build()
            .map_err(|error| {
                network_error("failed to configure connector Registry client", error)
            })?;
        Ok(Self { client })
    }

    async fn response(&self, url: &Url) -> Result<reqwest::Response> {
        self.client
            .get(url.clone())
            .send()
            .await
            .map_err(|error| network_error("connector Registry request failed", error))?
            .error_for_status()
            .map_err(|error| network_error("connector Registry returned an error status", error))
    }
}

#[async_trait]
impl RegistryTransport for HttpsRegistryTransport {
    async fn fetch(
        &self,
        url: &Url,
        maximum_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        let response = self.response(url).await?;
        reject_oversize_content_length(response.content_length(), maximum_bytes)?;
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        loop {
            let chunk = tokio::select! {
                () = cancellation.cancelled() => return Err(cancelled()),
                chunk = stream.next() => chunk,
            };
            let Some(chunk) = chunk else {
                break;
            };
            let chunk =
                chunk.map_err(|error| network_error("connector Registry stream failed", error))?;
            let next = u64::try_from(bytes.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            if next > maximum_bytes {
                return Err(invalid(format!(
                    "connector Registry response exceeds {maximum_bytes} bytes"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(network_error(
                "connector Registry returned an empty response",
                "empty body",
            ));
        }
        Ok(bytes)
    }

    async fn download_to(
        &self,
        url: &Url,
        destination: &Path,
        maximum_bytes: u64,
        cancellation: &CancellationToken,
        progress: ProgressCallback,
    ) -> Result<DownloadReceipt> {
        let response = self.response(url).await?;
        let content_length = response.content_length();
        reject_oversize_content_length(content_length, maximum_bytes)?;
        let mut stream = response.bytes_stream();
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(destination)
            .await
            .map_err(|error| io_error("failed to create connector staging file", error))?;
        let mut digest = Sha256::new();
        let mut downloaded = 0_u64;
        loop {
            let chunk = tokio::select! {
                () = cancellation.cancelled() => return Err(cancelled()),
                chunk = stream.next() => chunk,
            };
            let Some(chunk) = chunk else {
                break;
            };
            let chunk = chunk
                .map_err(|error| network_error("connector artifact download failed", error))?;
            downloaded = downloaded.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            if downloaded > maximum_bytes {
                return Err(invalid(format!(
                    "connector artifact exceeds {maximum_bytes} bytes"
                )));
            }
            file.write_all(&chunk)
                .await
                .map_err(|error| io_error("failed to write connector staging file", error))?;
            digest.update(&chunk);
            progress(downloaded, content_length);
        }
        file.sync_all()
            .await
            .map_err(|error| io_error("failed to flush connector staging file", error))?;
        if content_length.is_some_and(|length| length != downloaded) {
            return Err(network_error(
                "connector artifact download was truncated",
                format!("expected {content_length:?} bytes, received {downloaded}"),
            ));
        }
        Ok(DownloadReceipt {
            bytes: downloaded,
            sha256: digest.finalize().into(),
        })
    }
}

#[derive(Debug, Clone)]
enum RegistryConfiguration {
    Configured {
        catalog_url: Url,
        policy: Box<ManifestPolicy>,
    },
    NotConfigured,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PersistedStateV1 {
    #[serde(default = "state_version")]
    schema_version: u32,
    #[serde(default)]
    plugins: BTreeMap<String, InstalledPluginV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InstalledPluginV1 {
    active_version: String,
    previous_version: Option<String>,
    versions: BTreeMap<String, InstalledVersionV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InstalledVersionV1 {
    manifest: PluginManifestV1,
}

#[derive(Debug, Clone)]
struct ActiveOperation {
    plugin_id: String,
    cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveInstallation {
    pub manifest: PluginManifestV1,
    pub entry: PathBuf,
}

pub struct PluginManager {
    root: PathBuf,
    configuration: RegistryConfiguration,
    transport: Arc<dyn RegistryTransport>,
    state: Mutex<PersistedStateV1>,
    catalog: Mutex<Vec<PluginManifestV1>>,
    operations: Mutex<BTreeMap<String, ActiveOperation>>,
    latest_progress: Mutex<BTreeMap<String, PluginProgress>>,
    progress: broadcast::Sender<PluginProgress>,
}

impl std::fmt::Debug for PluginManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginManager")
            .field("root", &self.root)
            .field("configuration", &self.configuration)
            .field("transport", &"<registry transport>")
            .finish_non_exhaustive()
    }
}

impl PluginManager {
    pub fn open_https(options: PluginManagerOptions) -> Result<Arc<Self>> {
        Self::open(options, Arc::new(HttpsRegistryTransport::new()?))
    }

    pub fn open(
        options: PluginManagerOptions,
        transport: Arc<dyn RegistryTransport>,
    ) -> Result<Arc<Self>> {
        if options.plugin_root.as_os_str().is_empty() {
            return Err(invalid("connector plugin root must not be empty"));
        }
        fs::create_dir_all(&options.plugin_root)
            .map_err(|error| io_error("failed to create connector plugin root", error))?;
        let root = fs::canonicalize(&options.plugin_root)
            .map_err(|error| io_error("failed to resolve connector plugin root", error))?;
        fs::create_dir_all(root.join("plugins"))
            .map_err(|error| io_error("failed to create connector versions directory", error))?;
        let staging = root.join("staging");
        fs::create_dir_all(&staging)
            .map_err(|error| io_error("failed to create connector staging directory", error))?;
        clean_staging(&staging)?;

        let configuration = registry_configuration(&options)?;
        let state = load_state(&root)?;
        validate_persisted_state(&state, configuration_policy(&configuration))?;
        let (progress, _) = broadcast::channel(128);
        Ok(Arc::new(Self {
            root,
            configuration,
            transport,
            state: Mutex::new(state),
            catalog: Mutex::new(Vec::new()),
            operations: Mutex::new(BTreeMap::new()),
            latest_progress: Mutex::new(BTreeMap::new()),
            progress,
        }))
    }

    #[must_use]
    pub fn registry_status(&self) -> RegistryStatus {
        match self.configuration {
            RegistryConfiguration::Configured { .. } => RegistryStatus {
                availability: RegistryAvailability::Configured,
                api_version: CONNECTOR_API_VERSION,
                message: "官方插件仓库已配置".into(),
            },
            RegistryConfiguration::NotConfigured => RegistryStatus {
                availability: RegistryAvailability::NotConfigured,
                api_version: CONNECTOR_API_VERSION,
                message: "插件仓库未配置".into(),
            },
        }
    }

    #[must_use]
    pub fn subscribe_progress(&self) -> broadcast::Receiver<PluginProgress> {
        self.progress.subscribe()
    }

    pub async fn catalog_snapshot(&self) -> Result<PluginCatalogSnapshot> {
        if matches!(self.configuration, RegistryConfiguration::NotConfigured) {
            return Ok(PluginCatalogSnapshot {
                registry: self.registry_status(),
                plugins: Vec::new(),
            });
        }
        self.refresh_catalog().await?;
        let catalog = lock(&self.catalog)?.clone();
        let state = lock(&self.state)?.clone();
        let latest = lock(&self.latest_progress)?.clone();
        let plugins = catalog
            .iter()
            .map(|manifest| catalog_item(manifest, &state, latest.get(&manifest.id)))
            .collect::<Result<Vec<_>>>()?;
        Ok(PluginCatalogSnapshot {
            registry: self.registry_status(),
            plugins,
        })
    }

    pub async fn install(self: &Arc<Self>, plugin_id: &str) -> Result<OperationStarted> {
        self.start_operation(plugin_id, OperationKind::Install)
    }

    pub async fn retry(self: &Arc<Self>, plugin_id: &str) -> Result<OperationStarted> {
        self.start_operation(plugin_id, OperationKind::Retry)
    }

    pub async fn update(self: &Arc<Self>, plugin_id: &str) -> Result<OperationStarted> {
        self.start_operation(plugin_id, OperationKind::Update)
    }

    pub fn cancel(&self, operation_id: &str) -> Result<()> {
        let operations = lock(&self.operations)?;
        let operation = operations
            .get(operation_id)
            .ok_or_else(|| DbError::new("42704", "connector operation does not exist"))?;
        operation.cancellation.cancel();
        Ok(())
    }

    pub fn rollback(&self, plugin_id: &str) -> Result<PluginCatalogItem> {
        validate_plugin_id(plugin_id)?;
        let policy =
            configuration_policy(&self.configuration).ok_or_else(registry_not_configured)?;
        let current = lock(&self.state)?.clone();
        let plugin = current
            .plugins
            .get(plugin_id)
            .ok_or_else(|| DbError::new("42704", "connector is not installed"))?;
        let previous = plugin.previous_version.clone().ok_or_else(|| {
            DbError::new("55000", "connector has no previous version to roll back")
        })?;
        let previous_manifest = plugin
            .versions
            .get(&previous)
            .ok_or_else(|| DbError::internal("connector previous version metadata is missing"))?
            .manifest
            .clone();
        validate_manifest(&previous_manifest, policy)?;
        verify_installed_version(&self.root, &previous_manifest)?;

        let mut candidate = current;
        let plugin = candidate
            .plugins
            .get_mut(plugin_id)
            .ok_or_else(|| DbError::internal("connector state changed during rollback"))?;
        let old_active = std::mem::replace(&mut plugin.active_version, previous);
        plugin.previous_version = Some(old_active);
        persist_state(&self.root, &candidate)?;
        *lock(&self.state)? = candidate.clone();
        let catalog = lock(&self.catalog)?;
        let manifest = catalog
            .iter()
            .find(|manifest| manifest.id == plugin_id)
            .unwrap_or(&previous_manifest);
        catalog_item(manifest, &candidate, None)
    }

    pub fn active_entry(&self, plugin_id: &str) -> Result<PathBuf> {
        Ok(self.active_installation(plugin_id)?.entry)
    }

    pub(crate) fn active_installation(&self, plugin_id: &str) -> Result<ActiveInstallation> {
        validate_plugin_id(plugin_id)?;
        let policy =
            configuration_policy(&self.configuration).ok_or_else(registry_not_configured)?;
        let state = lock(&self.state)?;
        let plugin = state
            .plugins
            .get(plugin_id)
            .ok_or_else(|| DbError::new("42704", "connector is not installed"))?;
        let version = plugin
            .versions
            .get(&plugin.active_version)
            .ok_or_else(|| DbError::internal("connector active version metadata is missing"))?;
        validate_manifest(&version.manifest, policy)?;
        Ok(ActiveInstallation {
            entry: verify_installed_version(&self.root, &version.manifest)?,
            manifest: version.manifest.clone(),
        })
    }

    fn start_operation(
        self: &Arc<Self>,
        plugin_id: &str,
        kind: OperationKind,
    ) -> Result<OperationStarted> {
        validate_plugin_id(plugin_id)?;
        if matches!(self.configuration, RegistryConfiguration::NotConfigured) {
            return Err(registry_not_configured());
        }
        let operation_id = Uuid::new_v4().to_string();
        let cancellation = CancellationToken::new();
        {
            let mut operations = lock(&self.operations)?;
            if operations
                .values()
                .any(|operation| operation.plugin_id == plugin_id)
            {
                return Err(DbError::new(
                    "55P03",
                    "another connector operation is already running",
                ));
            }
            operations.insert(
                operation_id.clone(),
                ActiveOperation {
                    plugin_id: plugin_id.into(),
                    cancellation: cancellation.clone(),
                },
            );
        }
        let started = OperationStarted {
            operation_id: operation_id.clone(),
            plugin_id: plugin_id.into(),
            kind,
        };
        self.emit(PluginProgress {
            operation_id: operation_id.clone(),
            plugin_id: plugin_id.into(),
            kind,
            phase: PluginProgressPhase::Resolving,
            downloaded_bytes: 0,
            total_bytes: None,
            error: None,
        });
        let manager = Arc::clone(self);
        let plugin_id = plugin_id.to_owned();
        tokio::spawn(async move {
            let result = manager
                .run_operation(&operation_id, &plugin_id, kind, &cancellation)
                .await;
            if let Err(error) = result {
                let phase = if error.sql_state == "57014" {
                    PluginProgressPhase::Cancelled
                } else {
                    PluginProgressPhase::Failed
                };
                manager.emit(PluginProgress {
                    operation_id: operation_id.clone(),
                    plugin_id,
                    kind,
                    phase,
                    downloaded_bytes: 0,
                    total_bytes: None,
                    error: Some(error),
                });
            }
            if let Ok(mut operations) = manager.operations.lock() {
                operations.remove(&operation_id);
            }
        });
        Ok(started)
    }

    async fn run_operation(
        self: &Arc<Self>,
        operation_id: &str,
        plugin_id: &str,
        kind: OperationKind,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.refresh_catalog_with(cancellation).await?;
        let manifest = lock(&self.catalog)?
            .iter()
            .find(|manifest| manifest.id == plugin_id)
            .cloned()
            .ok_or_else(|| DbError::new("42704", "connector is not present in the Registry"))?;
        self.validate_operation(kind, &manifest)?;

        let staging = self.root.join("staging").join(operation_id);
        ensure_under_root(&self.root, &staging)?;
        if staging.exists() {
            fs::remove_dir_all(&staging)
                .map_err(|error| io_error("failed to reset connector staging directory", error))?;
        }
        fs::create_dir(&staging)
            .map_err(|error| io_error("failed to create connector operation directory", error))?;
        let destination = staging.join(&manifest.entry);
        let progress_manager = Arc::clone(self);
        let progress_operation = operation_id.to_owned();
        let progress_plugin = plugin_id.to_owned();
        let progress = Arc::new(move |downloaded_bytes, total_bytes| {
            progress_manager.emit(PluginProgress {
                operation_id: progress_operation.clone(),
                plugin_id: progress_plugin.clone(),
                kind,
                phase: PluginProgressPhase::Downloading,
                downloaded_bytes,
                total_bytes,
                error: None,
            });
        });

        let result = self
            .download_and_activate(
                operation_id,
                &manifest,
                kind,
                &destination,
                cancellation,
                progress,
            )
            .await;
        if result.is_err() && staging.exists() {
            let _ = fs::remove_dir_all(&staging);
        }
        result
    }

    async fn download_and_activate(
        &self,
        operation_id: &str,
        manifest: &PluginManifestV1,
        kind: OperationKind,
        destination: &Path,
        cancellation: &CancellationToken,
        progress: ProgressCallback,
    ) -> Result<()> {
        let policy =
            configuration_policy(&self.configuration).ok_or_else(registry_not_configured)?;
        let url = Url::parse(&manifest.download_url)
            .map_err(|error| invalid(format!("connector download URL is invalid: {error}")))?;
        let receipt = self
            .transport
            .download_to(
                &url,
                destination,
                policy.maximum_artifact_bytes,
                cancellation,
                progress,
            )
            .await?;
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        self.emit(PluginProgress {
            operation_id: operation_id.into(),
            plugin_id: manifest.id.clone(),
            kind,
            phase: PluginProgressPhase::Verifying,
            downloaded_bytes: receipt.bytes,
            total_bytes: Some(manifest.size),
            error: None,
        });
        if receipt.bytes != manifest.size {
            return Err(network_error(
                "connector artifact download was truncated",
                format!(
                    "expected {} bytes, received {}",
                    manifest.size, receipt.bytes
                ),
            ));
        }
        let expected_hash = decode_sha256(&manifest.sha256)?;
        if receipt.sha256 != expected_hash {
            return Err(
                security_error("connector artifact SHA-256 verification failed")
                    .with_hint("Discard the download, refresh the official Registry, and retry."),
            );
        }
        validate_manifest(manifest, policy)?;
        let operation_directory = destination
            .parent()
            .ok_or_else(|| DbError::internal("connector staging path has no parent"))?;
        write_json_synced(&operation_directory.join(MANIFEST_FILE), manifest)?;

        self.emit(PluginProgress {
            operation_id: operation_id.into(),
            plugin_id: manifest.id.clone(),
            kind,
            phase: PluginProgressPhase::Installing,
            downloaded_bytes: receipt.bytes,
            total_bytes: Some(manifest.size),
            error: None,
        });
        let version_parent = self
            .root
            .join("plugins")
            .join(&manifest.id)
            .join("versions");
        ensure_under_root(&self.root, &version_parent)?;
        fs::create_dir_all(&version_parent)
            .map_err(|error| io_error("failed to create connector version directory", error))?;
        let version_directory = version_parent.join(&manifest.version);
        ensure_under_root(&self.root, &version_directory)?;
        if version_directory.exists() {
            verify_installed_version(&self.root, manifest)?;
            fs::remove_dir_all(operation_directory)
                .map_err(|error| io_error("failed to clean duplicate connector staging", error))?;
        } else {
            fs::rename(operation_directory, &version_directory).map_err(|error| {
                io_error("failed to atomically install connector version", error)
            })?;
        }

        let current = lock(&self.state)?.clone();
        let mut candidate = current;
        let plugin = candidate
            .plugins
            .entry(manifest.id.clone())
            .or_insert_with(|| InstalledPluginV1 {
                active_version: manifest.version.clone(),
                previous_version: None,
                versions: BTreeMap::new(),
            });
        let previous = if plugin.active_version == manifest.version {
            plugin.previous_version.clone()
        } else {
            Some(plugin.active_version.clone())
        };
        plugin.versions.insert(
            manifest.version.clone(),
            InstalledVersionV1 {
                manifest: manifest.clone(),
            },
        );
        plugin.active_version = manifest.version.clone();
        plugin.previous_version = previous;
        persist_state(&self.root, &candidate)?;
        *lock(&self.state)? = candidate;
        self.emit(PluginProgress {
            operation_id: operation_id.into(),
            plugin_id: manifest.id.clone(),
            kind,
            phase: PluginProgressPhase::Complete,
            downloaded_bytes: receipt.bytes,
            total_bytes: Some(manifest.size),
            error: None,
        });
        Ok(())
    }

    fn validate_operation(&self, kind: OperationKind, manifest: &PluginManifestV1) -> Result<()> {
        let state = lock(&self.state)?;
        let installed = state.plugins.get(&manifest.id);
        match kind {
            OperationKind::Install | OperationKind::Retry => {
                if installed.is_some_and(|plugin| plugin.active_version == manifest.version) {
                    return Err(DbError::new(
                        "42710",
                        "connector version is already installed",
                    ));
                }
            }
            OperationKind::Update => {
                let plugin =
                    installed.ok_or_else(|| DbError::new("42704", "connector is not installed"))?;
                let active = Version::parse(&plugin.active_version).map_err(|error| {
                    DbError::internal("installed connector version is invalid")
                        .with_detail(error.to_string())
                })?;
                let available = Version::parse(&manifest.version)
                    .map_err(|error| invalid(format!("connector version is invalid: {error}")))?;
                if available <= active {
                    return Err(DbError::new(
                        "55000",
                        "connector Registry has no newer version",
                    ));
                }
            }
        }
        Ok(())
    }

    async fn refresh_catalog(&self) -> Result<()> {
        self.refresh_catalog_with(&CancellationToken::new()).await
    }

    async fn refresh_catalog_with(&self, cancellation: &CancellationToken) -> Result<()> {
        let RegistryConfiguration::Configured {
            catalog_url,
            policy,
        } = &self.configuration
        else {
            return Err(registry_not_configured());
        };
        let bytes = self
            .transport
            .fetch(catalog_url, MAXIMUM_CATALOG_BYTES, cancellation)
            .await?;
        let registry: RegistryCatalogV1 = serde_json::from_slice(&bytes)
            .map_err(|error| invalid(format!("connector Registry catalog is invalid: {error}")))?;
        if registry.schema_version != CONNECTOR_MANIFEST_VERSION {
            return Err(DbError::unsupported(format!(
                "connector Registry catalog version {}",
                registry.schema_version
            )));
        }
        if registry.plugins.len() > MAXIMUM_CATALOG_PLUGINS {
            return Err(invalid(format!(
                "connector Registry catalog exceeds {MAXIMUM_CATALOG_PLUGINS} plugins"
            )));
        }
        let mut ids = BTreeSet::new();
        for manifest in &registry.plugins {
            validate_manifest(manifest, policy)?;
            if !ids.insert(manifest.id.as_str()) {
                return Err(invalid("connector Registry contains duplicate plugin IDs"));
            }
        }
        let mut plugins = registry.plugins;
        plugins.sort_by(|left, right| left.id.cmp(&right.id));
        *lock(&self.catalog)? = plugins;
        Ok(())
    }

    fn emit(&self, progress: PluginProgress) {
        if let Ok(mut latest) = self.latest_progress.lock() {
            latest.insert(progress.plugin_id.clone(), progress.clone());
        }
        let _ = self.progress.send(progress);
    }
}

fn registry_configuration(options: &PluginManagerOptions) -> Result<RegistryConfiguration> {
    let (Some(registry_url), Some(public_key)) = (
        options
            .registry_url
            .as_deref()
            .filter(|value| !value.is_empty()),
        options
            .registry_public_key
            .as_deref()
            .filter(|value| !value.is_empty()),
    ) else {
        return Ok(RegistryConfiguration::NotConfigured);
    };
    let catalog_url = Url::parse(registry_url)
        .map_err(|error| invalid(format!("connector Registry URL is invalid: {error}")))?;
    let policy = ManifestPolicy::new(
        &options.host_version,
        options.maximum_artifact_bytes,
        catalog_url.clone(),
        decode_public_key(public_key)?,
    )?;
    Ok(RegistryConfiguration::Configured {
        catalog_url,
        policy: Box::new(policy),
    })
}

fn configuration_policy(configuration: &RegistryConfiguration) -> Option<&ManifestPolicy> {
    match configuration {
        RegistryConfiguration::Configured { policy, .. } => Some(policy),
        RegistryConfiguration::NotConfigured => None,
    }
}

fn load_state(root: &Path) -> Result<PersistedStateV1> {
    let path = root.join(STATE_FILE);
    if !path.exists() {
        return Ok(PersistedStateV1 {
            schema_version: STATE_VERSION,
            plugins: BTreeMap::new(),
        });
    }
    let metadata = fs::metadata(&path)
        .map_err(|error| io_error("failed to inspect connector state", error))?;
    if metadata.len() == 0 || metadata.len() > MAXIMUM_STATE_BYTES {
        return Err(invalid(format!(
            "connector state must be between 1 and {MAXIMUM_STATE_BYTES} bytes"
        )));
    }
    let mut file =
        File::open(&path).map_err(|error| io_error("failed to open connector state", error))?;
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.read_to_end(&mut bytes)
        .map_err(|error| io_error("failed to read connector state", error))?;
    let state: PersistedStateV1 = serde_json::from_slice(&bytes)
        .map_err(|error| invalid(format!("connector state is invalid: {error}")))?;
    if state.schema_version != STATE_VERSION {
        return Err(DbError::unsupported(format!(
            "connector state version {}",
            state.schema_version
        ))
        .with_hint("Back up the plugin directory and run an explicit OrdaDB migration."));
    }
    Ok(state)
}

fn validate_persisted_state(
    state: &PersistedStateV1,
    policy: Option<&ManifestPolicy>,
) -> Result<()> {
    if state.schema_version != STATE_VERSION {
        return Err(DbError::unsupported(format!(
            "connector state version {}",
            state.schema_version
        )));
    }
    for (plugin_id, plugin) in &state.plugins {
        validate_plugin_id(plugin_id)?;
        if !plugin.versions.contains_key(&plugin.active_version) {
            return Err(DbError::internal(
                "connector active version is absent from persisted state",
            ));
        }
        if plugin
            .previous_version
            .as_ref()
            .is_some_and(|version| !plugin.versions.contains_key(version))
        {
            return Err(DbError::internal(
                "connector previous version is absent from persisted state",
            ));
        }
        for (version, installed) in &plugin.versions {
            if installed.manifest.id != *plugin_id || installed.manifest.version != *version {
                return Err(DbError::internal(
                    "connector persisted manifest identity is inconsistent",
                ));
            }
            if let Some(policy) = policy {
                validate_manifest(&installed.manifest, policy)?;
            }
        }
    }
    Ok(())
}

fn persist_state(root: &Path, state: &PersistedStateV1) -> Result<()> {
    let temporary = root.join(format!("{STATE_FILE}.{}.tmp", Uuid::new_v4()));
    ensure_under_root(root, &temporary)?;
    write_json_synced(&temporary, state)?;
    let destination = root.join(STATE_FILE);
    atomic_replace(&temporary, &destination).map_err(|error| {
        let _ = fs::remove_file(&temporary);
        io_error("failed to atomically replace connector state", error)
    })
}

fn write_json_synced(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
        DbError::internal("failed to encode connector state").with_detail(error.to_string())
    })?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| io_error("failed to create connector metadata file", error))?;
    file.write_all(&bytes)
        .map_err(|error| io_error("failed to write connector metadata file", error))?;
    file.sync_all()
        .map_err(|error| io_error("failed to flush connector metadata file", error))
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::rename(source, destination)
}

fn clean_staging(staging: &Path) -> Result<()> {
    for entry in fs::read_dir(staging)
        .map_err(|error| io_error("failed to inspect connector staging directory", error))?
    {
        let path = entry
            .map_err(|error| io_error("failed to inspect connector staging entry", error))?
            .path();
        ensure_under_root(staging, &path)?;
        if path.is_dir() {
            fs::remove_dir_all(&path)
                .map_err(|error| io_error("failed to clean connector staging directory", error))?;
        } else {
            fs::remove_file(&path)
                .map_err(|error| io_error("failed to clean connector staging file", error))?;
        }
    }
    Ok(())
}

fn ensure_under_root(root: &Path, candidate: &Path) -> Result<()> {
    if !candidate.starts_with(root) || candidate == root {
        return Err(security_error(
            "connector filesystem path escaped the manager-owned root",
        ));
    }
    Ok(())
}

fn verify_installed_version(root: &Path, manifest: &PluginManifestV1) -> Result<PathBuf> {
    let directory = root
        .join("plugins")
        .join(&manifest.id)
        .join("versions")
        .join(&manifest.version);
    ensure_under_root(root, &directory)?;
    let manifest_path = directory.join(MANIFEST_FILE);
    let bytes = fs::read(&manifest_path)
        .map_err(|error| io_error("failed to read installed connector manifest", error))?;
    let stored: PluginManifestV1 = serde_json::from_slice(&bytes)
        .map_err(|error| invalid(format!("installed connector manifest is invalid: {error}")))?;
    if stored != *manifest {
        return Err(security_error(
            "installed connector manifest does not match trusted state",
        ));
    }
    let entry = directory.join(&manifest.entry);
    ensure_under_root(root, &entry)?;
    let (bytes, hash) = hash_file(&entry)?;
    if bytes != manifest.size || hash != decode_sha256(&manifest.sha256)? {
        return Err(security_error(
            "installed connector executable failed integrity verification",
        ));
    }
    Ok(entry)
}

fn hash_file(path: &Path) -> Result<(u64, [u8; 32])> {
    let mut file =
        File::open(path).map_err(|error| io_error("failed to open connector executable", error))?;
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| io_error("failed to read connector executable", error))?;
        if count == 0 {
            break;
        }
        total = total.saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
        digest.update(&buffer[..count]);
    }
    Ok((total, digest.finalize().into()))
}

fn catalog_item(
    manifest: &PluginManifestV1,
    state: &PersistedStateV1,
    progress: Option<&PluginProgress>,
) -> Result<PluginCatalogItem> {
    let installed = state.plugins.get(&manifest.id);
    let (installed_version, previous_version) = installed
        .map(|plugin| {
            (
                Some(plugin.active_version.clone()),
                plugin.previous_version.clone(),
            )
        })
        .unwrap_or_default();
    let lifecycle = if let Some(progress) = progress {
        progress.phase.lifecycle()
    } else if let Some(installed_version) = installed_version.as_deref() {
        let installed = Version::parse(installed_version).map_err(|error| {
            DbError::internal("installed connector version is invalid")
                .with_detail(error.to_string())
        })?;
        let available = Version::parse(&manifest.version)
            .map_err(|error| invalid(format!("connector version is invalid: {error}")))?;
        if available > installed {
            PluginLifecycle::UpdateAvailable
        } else {
            PluginLifecycle::Installed
        }
    } else {
        PluginLifecycle::Available
    };
    Ok(PluginCatalogItem {
        id: manifest.id.clone(),
        display_name: manifest.display_name.clone(),
        version: manifest.version.clone(),
        dialect: manifest.dialect,
        publisher: manifest.publisher.clone(),
        permissions: manifest.permissions.clone(),
        size: manifest.size,
        lifecycle,
        installed_version,
        previous_version,
        operation_id: progress.map(|progress| progress.operation_id.clone()),
        downloaded_bytes: progress.map_or(0, |progress| progress.downloaded_bytes),
        error: progress.and_then(|progress| progress.error.clone()),
    })
}

fn reject_oversize_content_length(length: Option<u64>, maximum: u64) -> Result<()> {
    if length.is_some_and(|length| length == 0 || length > maximum) {
        return Err(invalid(format!(
            "connector response size must be between 1 and {maximum} bytes"
        )));
    }
    Ok(())
}

fn state_version() -> u32 {
    STATE_VERSION
}

fn registry_not_configured() -> DbError {
    DbError::unsupported("connector Registry")
        .with_detail("the production Registry URL and Ed25519 public key are not configured")
        .with_hint("Configure the official signed plugin Registry in the Windows package.")
}

fn cancelled() -> DbError {
    DbError::new("57014", "connector operation was cancelled")
}

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| DbError::internal("connector manager lock was poisoned"))
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use ed25519_dalek::{Signer, SigningKey};
    use sha2::Digest as _;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        ConnectorArchitecture, ConnectorDialect, ConnectorPermission, manifest_signing_payload,
    };

    #[derive(Debug)]
    struct FakeTransport {
        catalog: Mutex<Vec<u8>>,
        artifact: Mutex<Vec<u8>>,
        fail_download: Mutex<Option<DbError>>,
        block_download: Mutex<bool>,
    }

    impl FakeTransport {
        fn new(catalog: Vec<u8>, artifact: Vec<u8>) -> Self {
            Self {
                catalog: Mutex::new(catalog),
                artifact: Mutex::new(artifact),
                fail_download: Mutex::new(None),
                block_download: Mutex::new(false),
            }
        }
    }

    #[async_trait]
    impl RegistryTransport for FakeTransport {
        async fn fetch(
            &self,
            _url: &Url,
            _maximum_bytes: u64,
            cancellation: &CancellationToken,
        ) -> Result<Vec<u8>> {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            Ok(self.catalog.lock().expect("catalog").clone())
        }

        async fn download_to(
            &self,
            _url: &Url,
            destination: &Path,
            maximum_bytes: u64,
            cancellation: &CancellationToken,
            progress: ProgressCallback,
        ) -> Result<DownloadReceipt> {
            if let Some(error) = self.fail_download.lock().expect("failure").clone() {
                return Err(error);
            }
            while *self.block_download.lock().expect("block") {
                tokio::select! {
                    () = cancellation.cancelled() => return Err(cancelled()),
                    () = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
            }
            let artifact = self.artifact.lock().expect("artifact").clone();
            if u64::try_from(artifact.len()).unwrap_or(u64::MAX) > maximum_bytes {
                return Err(invalid("fake artifact exceeded limit"));
            }
            fs::write(destination, &artifact).expect("write fake artifact");
            let bytes = u64::try_from(artifact.len()).expect("artifact length");
            progress(bytes, Some(bytes));
            Ok(DownloadReceipt {
                bytes,
                sha256: Sha256::digest(&artifact).into(),
            })
        }
    }

    fn signed_manifest(
        signing_key: &SigningKey,
        artifact: &[u8],
        version: &str,
    ) -> PluginManifestV1 {
        let mut manifest = PluginManifestV1 {
            schema_version: CONNECTOR_MANIFEST_VERSION,
            id: "ordadb-postgresql".into(),
            display_name: "OrdaDB / PostgreSQL".into(),
            version: version.into(),
            api_version: CONNECTOR_API_VERSION,
            architecture: ConnectorArchitecture::WindowsX64,
            dialect: ConnectorDialect::PostgreSql,
            publisher: "OrdaDB".into(),
            permissions: vec![ConnectorPermission::Network],
            entry: "ordadb-postgresql.exe".into(),
            size: u64::try_from(artifact.len()).expect("artifact length"),
            sha256: Sha256::digest(artifact)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            signature: String::new(),
            minimum_host_version: "0.1.0".into(),
            download_url: format!("https://plugins.ordadb.test/v1/ordadb-postgresql-{version}.exe"),
        };
        manifest.signature = BASE64.encode(
            signing_key
                .sign(&manifest_signing_payload(&manifest).expect("payload"))
                .to_bytes(),
        );
        manifest
    }

    fn catalog(manifest: PluginManifestV1) -> Vec<u8> {
        serde_json::to_vec(&RegistryCatalogV1 {
            schema_version: CONNECTOR_MANIFEST_VERSION,
            plugins: vec![manifest],
        })
        .expect("catalog")
    }

    fn options(root: &Path, signing_key: &SigningKey) -> PluginManagerOptions {
        let mut options = PluginManagerOptions::new(root);
        options.registry_url = Some("https://plugins.ordadb.test/catalog.json".into());
        options.registry_public_key = Some(BASE64.encode(signing_key.verifying_key().to_bytes()));
        options.maximum_artifact_bytes = 1024 * 1024;
        options
    }

    async fn wait_terminal(
        receiver: &mut broadcast::Receiver<PluginProgress>,
        operation_id: &str,
    ) -> PluginProgress {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let progress = receiver.recv().await.expect("progress");
                if progress.operation_id == operation_id
                    && matches!(
                        progress.phase,
                        PluginProgressPhase::Complete
                            | PluginProgressPhase::Cancelled
                            | PluginProgressPhase::Failed
                    )
                {
                    return progress;
                }
            }
        })
        .await
        .expect("operation timeout")
    }

    #[tokio::test]
    async fn installs_updates_reopens_and_rolls_back_atomically() {
        let directory = tempdir().expect("tempdir");
        let signing_key = SigningKey::from_bytes(&[11_u8; 32]);
        let first_artifact = b"connector-v1".to_vec();
        let first = signed_manifest(&signing_key, &first_artifact, "1.0.0");
        let transport = Arc::new(FakeTransport::new(catalog(first.clone()), first_artifact));
        let manager =
            PluginManager::open(options(directory.path(), &signing_key), transport.clone())
                .expect("manager");
        let mut progress = manager.subscribe_progress();
        let started = manager.install(&first.id).await.expect("install start");
        let completed = wait_terminal(&mut progress, &started.operation_id).await;
        assert_eq!(completed.phase, PluginProgressPhase::Complete);
        assert!(manager.active_entry(&first.id).expect("active").is_file());

        let second_artifact = b"connector-v2".to_vec();
        let second = signed_manifest(&signing_key, &second_artifact, "2.0.0");
        *transport.catalog.lock().expect("catalog") = catalog(second.clone());
        *transport.artifact.lock().expect("artifact") = second_artifact;
        let started = manager.update(&second.id).await.expect("update start");
        let completed = wait_terminal(&mut progress, &started.operation_id).await;
        assert_eq!(completed.phase, PluginProgressPhase::Complete);
        let snapshot = manager.catalog_snapshot().await.expect("snapshot");
        assert_eq!(
            snapshot.plugins[0].installed_version.as_deref(),
            Some("2.0.0")
        );
        assert_eq!(
            snapshot.plugins[0].previous_version.as_deref(),
            Some("1.0.0")
        );

        let rolled_back = manager.rollback(&second.id).expect("rollback");
        assert_eq!(rolled_back.installed_version.as_deref(), Some("1.0.0"));
        assert_eq!(rolled_back.previous_version.as_deref(), Some("2.0.0"));

        drop(manager);
        let reopened = PluginManager::open(options(directory.path(), &signing_key), transport)
            .expect("reopen");
        assert!(
            reopened
                .active_entry(&first.id)
                .expect("reopened active")
                .is_file()
        );
    }

    #[tokio::test]
    async fn hash_mismatch_and_truncation_never_activate_a_plugin() {
        let directory = tempdir().expect("tempdir");
        let signing_key = SigningKey::from_bytes(&[12_u8; 32]);
        let expected = b"expected-artifact".to_vec();
        let manifest = signed_manifest(&signing_key, &expected, "1.0.0");
        let transport = Arc::new(FakeTransport::new(
            catalog(manifest.clone()),
            b"tampered-artifact".to_vec(),
        ));
        let manager =
            PluginManager::open(options(directory.path(), &signing_key), transport.clone())
                .expect("manager");
        let mut progress = manager.subscribe_progress();
        let started = manager.install(&manifest.id).await.expect("install start");
        let failed = wait_terminal(&mut progress, &started.operation_id).await;
        assert_eq!(failed.phase, PluginProgressPhase::Failed);
        assert!(manager.active_entry(&manifest.id).is_err());

        *transport.artifact.lock().expect("artifact") = expected[..4].to_vec();
        let started = manager.retry(&manifest.id).await.expect("retry start");
        let failed = wait_terminal(&mut progress, &started.operation_id).await;
        assert_eq!(failed.phase, PluginProgressPhase::Failed);
        assert_eq!(
            failed.error.expect("error").message,
            "connector artifact download was truncated"
        );
        assert!(manager.active_entry(&manifest.id).is_err());
    }

    #[tokio::test]
    async fn cancellation_cleans_staging_and_releases_the_operation() {
        let directory = tempdir().expect("tempdir");
        let signing_key = SigningKey::from_bytes(&[13_u8; 32]);
        let artifact = b"connector".to_vec();
        let manifest = signed_manifest(&signing_key, &artifact, "1.0.0");
        let transport = Arc::new(FakeTransport::new(catalog(manifest.clone()), artifact));
        *transport.block_download.lock().expect("block") = true;
        let manager =
            PluginManager::open(options(directory.path(), &signing_key), transport.clone())
                .expect("manager");
        let mut progress = manager.subscribe_progress();
        let started = manager.install(&manifest.id).await.expect("install start");
        manager.cancel(&started.operation_id).expect("cancel");
        let cancelled = wait_terminal(&mut progress, &started.operation_id).await;
        assert_eq!(cancelled.phase, PluginProgressPhase::Cancelled);
        assert!(
            fs::read_dir(directory.path().join("staging"))
                .expect("staging")
                .next()
                .is_none()
        );
        *transport.block_download.lock().expect("block") = false;
        let retry = manager.retry(&manifest.id).await.expect("retry");
        let completed = wait_terminal(&mut progress, &retry.operation_id).await;
        assert_eq!(completed.phase, PluginProgressPhase::Complete);
    }

    #[tokio::test]
    async fn unconfigured_registry_is_explicit_and_fails_closed() {
        let directory = tempdir().expect("tempdir");
        let transport = Arc::new(FakeTransport::new(Vec::new(), Vec::new()));
        let manager = PluginManager::open(PluginManagerOptions::new(directory.path()), transport)
            .expect("manager");
        let snapshot = manager.catalog_snapshot().await.expect("snapshot");
        assert_eq!(
            snapshot.registry.availability,
            RegistryAvailability::NotConfigured
        );
        assert_eq!(snapshot.registry.message, "插件仓库未配置");
        assert_eq!(
            manager
                .install("ordadb-postgresql")
                .await
                .expect_err("install disabled")
                .sql_state,
            "0A000"
        );
    }

    #[test]
    fn unknown_state_version_is_refused_without_rewrite() {
        let directory = tempdir().expect("tempdir");
        fs::write(
            directory.path().join(STATE_FILE),
            br#"{"schemaVersion":2,"plugins":{}}"#,
        )
        .expect("state");
        let transport = Arc::new(FakeTransport::new(Vec::new(), Vec::new()));
        let error = PluginManager::open(PluginManagerOptions::new(directory.path()), transport)
            .expect_err("unknown state");
        assert_eq!(error.sql_state, "0A000");
        assert_eq!(
            fs::read_to_string(directory.path().join(STATE_FILE)).expect("state unchanged"),
            r#"{"schemaVersion":2,"plugins":{}}"#
        );
    }
}
