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
    CONNECTOR_API_VERSION, CONNECTOR_MANIFEST_VERSION, ManifestPolicy,
    OFFICIAL_CONNECTOR_DESCRIPTORS, PluginManifestV1, RegistryCatalogV1, decode_public_key,
    invalid, io_error, network_error, security_error, validate_manifest,
};

const STATE_VERSION: u32 = 1;
const MAXIMUM_CATALOG_BYTES: u64 = 1024 * 1024;
const MAXIMUM_STATE_BYTES: u64 = 4 * 1024 * 1024;
const DEFAULT_MAXIMUM_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_CATALOG_PLUGINS: usize = 64;
const REGISTRY_TIMEOUT: Duration = Duration::from_secs(30);
const STATE_FILE: &str = "state-v1.json";
const MANIFEST_FILE: &str = "manifest-v1.json";
const BUNDLED_CATALOG_FILE: &str = "catalog-v1.json";

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
    pub bundled_root: Option<PathBuf>,
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
            bundled_root: None,
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
