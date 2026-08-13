
fn read_bounded_file(path: &Path, maximum_bytes: u64, name: &str) -> Result<Vec<u8>> {
    let metadata =
        fs::metadata(path).map_err(|error| io_error(format!("failed to inspect {name}"), error))?;
    if metadata.len() == 0 || metadata.len() > maximum_bytes {
        return Err(invalid(format!(
            "{name} must be between 1 and {maximum_bytes} bytes"
        )));
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| invalid(format!("{name} size does not fit this host")))?;
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|error| io_error(format!("failed to read {name}"), error))?;
    Ok(bytes)
}

fn copy_file_synced(source: &Path, destination: &Path) -> Result<()> {
    fs::copy(source, destination)
        .map_err(|error| io_error("failed to stage bundled connector executable", error))?;
    OpenOptions::new()
        .write(true)
        .open(destination)
        .and_then(|file| file.sync_all())
        .map_err(|error| io_error("failed to flush bundled connector executable", error))
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

fn parse_registry_catalog(bytes: &[u8], policy: &ManifestPolicy) -> Result<Vec<PluginManifestV1>> {
    let registry: RegistryCatalogV1 = serde_json::from_slice(bytes)
        .map_err(|error| invalid(format!("connector Registry catalog is invalid: {error}")))?;
    if registry.schema_version != CONNECTOR_MANIFEST_VERSION {
        return Err(DbError::unsupported(format!(
            "connector Registry catalog version {}",
            registry.schema_version
        )));
    }
    if registry.plugins.is_empty() || registry.plugins.len() > MAXIMUM_CATALOG_PLUGINS {
        return Err(invalid(format!(
            "connector Registry catalog must contain 1-{MAXIMUM_CATALOG_PLUGINS} plugins"
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
    Ok(plugins)
}

fn legacy_plugin_id(plugin_id: &str) -> Option<&'static str> {
    match plugin_id {
        "postgresql" => Some("ordadb-postgresql"),
        "mysql" => Some("ordadb-mysql"),
        "sqlite" => Some("ordadb-sqlite"),
        "sql-server" => Some("ordadb-sql-server"),
        _ => None,
    }
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
        signed_manifest_for(
            signing_key,
            artifact,
            version,
            "ordadb-postgresql",
            "ordadb-postgresql.exe",
            ConnectorDialect::PostgreSql,
            vec![ConnectorPermission::Network],
        )
    }

    fn signed_manifest_for(
        signing_key: &SigningKey,
        artifact: &[u8],
        version: &str,
        id: &str,
        entry: &str,
        dialect: ConnectorDialect,
        permissions: Vec<ConnectorPermission>,
    ) -> PluginManifestV1 {
        let mut manifest = PluginManifestV1 {
            schema_version: CONNECTOR_MANIFEST_VERSION,
            id: id.into(),
            display_name: "OrdaDB / PostgreSQL".into(),
            version: version.into(),
            api_version: CONNECTOR_API_VERSION,
            architecture: ConnectorArchitecture::WindowsX64,
            dialect,
            publisher: "OrdaDB".into(),
            permissions,
            entry: entry.into(),
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
        catalog_with(vec![manifest])
    }

    fn catalog_with(plugins: Vec<PluginManifestV1>) -> Vec<u8> {
        serde_json::to_vec(&RegistryCatalogV1 {
            schema_version: CONNECTOR_MANIFEST_VERSION,
            plugins,
        })
        .expect("catalog")
    }

    fn write_official_bundle(
        root: &Path,
        signing_key: &SigningKey,
        version: &str,
        overrides: &[(&str, &[u8])],
    ) -> Vec<PluginManifestV1> {
        fs::create_dir_all(root).expect("bundle directory");
        let manifests = OFFICIAL_CONNECTOR_DESCRIPTORS
            .iter()
            .map(|descriptor| {
                let artifact = overrides
                    .iter()
                    .find_map(|(id, artifact)| (*id == descriptor.id).then_some(*artifact))
                    .unwrap_or(descriptor.id.as_bytes());
                let entry = format!("{}.exe", descriptor.package);
                let mut manifest = PluginManifestV1 {
                    schema_version: CONNECTOR_MANIFEST_VERSION,
                    id: descriptor.id.into(),
                    display_name: descriptor.display_name.into(),
                    version: version.into(),
                    api_version: descriptor.api_version,
                    architecture: ConnectorArchitecture::WindowsX64,
                    dialect: descriptor.dialect,
                    publisher: "OrdaDB".into(),
                    permissions: descriptor.permissions.to_vec(),
                    entry: entry.clone(),
                    size: u64::try_from(artifact.len()).expect("artifact length"),
                    sha256: Sha256::digest(artifact)
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect(),
                    signature: String::new(),
                    minimum_host_version: "0.1.0".into(),
                    download_url: format!(
                        "https://plugins.ordadb.test/v1/artifacts/{version}/{entry}"
                    ),
                };
                manifest.signature = BASE64.encode(
                    signing_key
                        .sign(&manifest_signing_payload(&manifest).expect("payload"))
                        .to_bytes(),
                );
                fs::write(root.join(&entry), artifact).expect("bundle artifact");
                manifest
            })
            .collect::<Vec<_>>();
        fs::write(
            root.join(BUNDLED_CATALOG_FILE),
            catalog_with(manifests.clone()),
        )
        .expect("bundle catalog");
        manifests
    }

    fn options(root: &Path, signing_key: &SigningKey) -> PluginManagerOptions {
        let mut options = PluginManagerOptions::new(root);
        options.registry_url = Some("https://plugins.ordadb.test/catalog.json".into());
        options.registry_public_key = Some(BASE64.encode(signing_key.verifying_key().to_bytes()));
        options.maximum_artifact_bytes = 1024 * 1024;
        options
    }

    fn bundled_options(
        root: &Path,
        bundled_root: &Path,
        signing_key: &SigningKey,
    ) -> PluginManagerOptions {
        let mut options = options(root, signing_key);
        options.bundled_root = Some(bundled_root.to_path_buf());
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

    #[test]
    fn bundled_connectors_are_verified_and_activated_as_one_state_change() {
        let directory = tempdir().expect("tempdir");
        let bundled = tempdir().expect("bundled");
        let signing_key = SigningKey::from_bytes(&[21_u8; 32]);
        let postgresql_artifact = b"postgresql-connector".as_slice();
        let sqlite_artifact = b"sqlite-connector".as_slice();
        let manifests = write_official_bundle(
            bundled.path(),
            &signing_key,
            "1.0.0",
            &[
                ("postgresql", postgresql_artifact),
                ("sqlite", sqlite_artifact),
            ],
        );

        let manager = PluginManager::open(
            bundled_options(directory.path(), bundled.path(), &signing_key),
            Arc::new(FakeTransport::new(Vec::new(), Vec::new())),
        )
        .expect("bundled manager");
        assert!(
            manager
                .active_entry("postgresql")
                .expect("postgresql")
                .is_file()
        );
        assert!(manager.active_entry("sqlite").expect("sqlite").is_file());
        let state = load_state(directory.path()).expect("state");
        assert_eq!(state.plugins.len(), 9);
        assert_eq!(state.plugins["postgresql"].active_version, "1.0.0");
        assert_eq!(state.plugins["sqlite"].active_version, "1.0.0");
        for descriptor in OFFICIAL_CONNECTOR_DESCRIPTORS {
            assert!(
                manager
                    .active_entry(descriptor.id)
                    .expect("official helper")
                    .is_file()
            );
        }
        assert_eq!(manifests.len(), OFFICIAL_CONNECTOR_DESCRIPTORS.len());
    }

    #[test]
    fn a_tampered_bundle_keeps_the_existing_active_state() {
        let directory = tempdir().expect("tempdir");
        let bundled = tempdir().expect("bundled");
        let signing_key = SigningKey::from_bytes(&[22_u8; 32]);
        let first_artifact = b"connector-v1".as_slice();
        write_official_bundle(
            bundled.path(),
            &signing_key,
            "1.0.0",
            &[("postgresql", first_artifact)],
        );
        let manager = PluginManager::open(
            bundled_options(directory.path(), bundled.path(), &signing_key),
            Arc::new(FakeTransport::new(Vec::new(), Vec::new())),
        )
        .expect("initial bundle");
        drop(manager);

        let second_artifact = b"connector-v2".as_slice();
        let second = write_official_bundle(
            bundled.path(),
            &signing_key,
            "2.0.0",
            &[("postgresql", second_artifact)],
        );
        let second_postgresql = second
            .iter()
            .find(|manifest| manifest.id == "postgresql")
            .expect("postgresql manifest");
        fs::write(
            bundled.path().join(&second_postgresql.entry),
            b"tampered-v2",
        )
        .expect("tamper bundle");
        let error = PluginManager::open(
            bundled_options(directory.path(), bundled.path(), &signing_key),
            Arc::new(FakeTransport::new(Vec::new(), Vec::new())),
        )
        .expect_err("tampered bundle");
        assert_eq!(error.sql_state, "28000");

        let reopened = PluginManager::open(
            options(directory.path(), &signing_key),
            Arc::new(FakeTransport::new(Vec::new(), Vec::new())),
        )
        .expect("reopen existing state");
        let state = load_state(directory.path()).expect("state");
        assert_eq!(state.plugins["postgresql"].active_version, "1.0.0");
        assert!(
            reopened
                .active_entry("postgresql")
                .expect("active")
                .is_file()
        );
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
