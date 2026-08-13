
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
        let mut state = load_state(&root)?;
        validate_persisted_state(&state, configuration_policy(&configuration))?;
        verify_persisted_artifacts(&root, &state)?;
        if let Some(bundled_root) = options.bundled_root.as_deref() {
            let policy =
                configuration_policy(&configuration).ok_or_else(registry_not_configured)?;
            activate_bundled_connectors(&root, bundled_root, policy, &mut state)?;
        }
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
        let installed_id = state
            .plugins
            .contains_key(plugin_id)
            .then_some(plugin_id)
            .or_else(|| legacy_plugin_id(plugin_id));
        let plugin = installed_id
            .and_then(|installed_id| state.plugins.get(installed_id))
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
        let plugins = parse_registry_catalog(&bytes, policy)?;
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

fn verify_persisted_artifacts(root: &Path, state: &PersistedStateV1) -> Result<()> {
    for plugin in state.plugins.values() {
        for installed in plugin.versions.values() {
            verify_installed_version(root, &installed.manifest)?;
        }
    }
    Ok(())
}

fn activate_bundled_connectors(
    root: &Path,
    bundled_root: &Path,
    policy: &ManifestPolicy,
    state: &mut PersistedStateV1,
) -> Result<()> {
    let bundled_root = fs::canonicalize(bundled_root)
        .map_err(|error| io_error("failed to resolve bundled connector directory", error))?;
    if !bundled_root.is_dir() {
        return Err(invalid(
            "bundled connector resource path must be a directory",
        ));
    }
    let catalog_path =
        canonical_bundled_file(&bundled_root, &bundled_root.join(BUNDLED_CATALOG_FILE))?;
    let catalog_bytes = read_bounded_file(
        &catalog_path,
        MAXIMUM_CATALOG_BYTES,
        "bundled connector catalog",
    )?;
    let manifests = parse_registry_catalog(&catalog_bytes, policy)?;
    validate_bundled_manifest_set(&manifests)?;
    let verified = manifests
        .into_iter()
        .map(|manifest| {
            let source =
                canonical_bundled_file(&bundled_root, &bundled_root.join(&manifest.entry))?;
            let (bytes, hash) = hash_file(&source)?;
            if bytes != manifest.size || hash != decode_sha256(&manifest.sha256)? {
                return Err(security_error(
                    "bundled connector executable failed integrity verification",
                ));
            }
            Ok((manifest, source))
        })
        .collect::<Result<Vec<_>>>()?;

    let staging = root
        .join("staging")
        .join(format!("bundled-{}", Uuid::new_v4()));
    ensure_under_root(root, &staging)?;
    fs::create_dir(&staging).map_err(|error| {
        io_error(
            "failed to create bundled connector staging directory",
            error,
        )
    })?;
    let activation = stage_and_activate_bundled(root, &staging, &verified, state);
    if staging.exists() {
        let _ = fs::remove_dir_all(&staging);
    }
    activation
}

fn validate_bundled_manifest_set(manifests: &[PluginManifestV1]) -> Result<()> {
    if manifests.len() != OFFICIAL_CONNECTOR_DESCRIPTORS.len() {
        return Err(security_error(format!(
            "bundled connector catalog must contain exactly {} official helpers",
            OFFICIAL_CONNECTOR_DESCRIPTORS.len()
        )));
    }
    let manifests_by_id = manifests
        .iter()
        .map(|manifest| (manifest.id.as_str(), manifest))
        .collect::<BTreeMap<_, _>>();
    for descriptor in OFFICIAL_CONNECTOR_DESCRIPTORS {
        let manifest = manifests_by_id.get(descriptor.id).ok_or_else(|| {
            security_error(format!(
                "bundled connector catalog is missing official helper {}",
                descriptor.id
            ))
        })?;
        let expected_entry = format!("{}.exe", descriptor.package);
        if manifest.display_name != descriptor.display_name
            || manifest.api_version != descriptor.api_version
            || manifest.dialect != descriptor.dialect
            || manifest.permissions.as_slice() != descriptor.permissions
            || manifest.entry != expected_entry
        {
            return Err(security_error(format!(
                "bundled connector identity does not match the official descriptor for {}",
                descriptor.id
            )));
        }
    }
    Ok(())
}

fn stage_and_activate_bundled(
    root: &Path,
    staging: &Path,
    verified: &[(PluginManifestV1, PathBuf)],
    state: &mut PersistedStateV1,
) -> Result<()> {
    for (manifest, source) in verified {
        let version_directory = staging.join(&manifest.id).join(&manifest.version);
        ensure_under_root(root, &version_directory)?;
        fs::create_dir_all(&version_directory).map_err(|error| {
            io_error(
                "failed to create bundled connector version staging directory",
                error,
            )
        })?;
        copy_file_synced(source, &version_directory.join(&manifest.entry))?;
        write_json_synced(&version_directory.join(MANIFEST_FILE), manifest)?;
    }

    let mut candidate = state.clone();
    for (manifest, _) in verified {
        let staged_version = staging.join(&manifest.id).join(&manifest.version);
        let version_parent = root.join("plugins").join(&manifest.id).join("versions");
        ensure_under_root(root, &version_parent)?;
        fs::create_dir_all(&version_parent).map_err(|error| {
            io_error(
                "failed to create bundled connector version directory",
                error,
            )
        })?;
        let installed_version = version_parent.join(&manifest.version);
        ensure_under_root(root, &installed_version)?;
        if installed_version.exists() {
            verify_installed_version(root, manifest)?;
        } else {
            fs::rename(&staged_version, &installed_version).map_err(|error| {
                io_error(
                    "failed to atomically install bundled connector version",
                    error,
                )
            })?;
        }

        let plugin = candidate
            .plugins
            .entry(manifest.id.clone())
            .or_insert_with(|| InstalledPluginV1 {
                active_version: manifest.version.clone(),
                previous_version: None,
                versions: BTreeMap::new(),
            });
        let active = Version::parse(&plugin.active_version).map_err(|error| {
            DbError::internal("installed connector version is invalid")
                .with_detail(error.to_string())
        })?;
        let bundled = Version::parse(&manifest.version)
            .map_err(|error| invalid(format!("connector version is invalid: {error}")))?;
        plugin.versions.insert(
            manifest.version.clone(),
            InstalledVersionV1 {
                manifest: manifest.clone(),
            },
        );
        if bundled > active {
            plugin.previous_version = Some(plugin.active_version.clone());
            plugin.active_version.clone_from(&manifest.version);
        }
    }
    validate_persisted_state(&candidate, None)?;
    persist_state(root, &candidate)?;
    *state = candidate;
    Ok(())
}

fn canonical_bundled_file(root: &Path, candidate: &Path) -> Result<PathBuf> {
    let candidate = fs::canonicalize(candidate)
        .map_err(|error| io_error("failed to resolve bundled connector resource", error))?;
    if candidate.parent() != Some(root) || !candidate.is_file() {
        return Err(security_error(
            "bundled connector resource escaped its immutable directory",
        ));
    }
    Ok(candidate)
}
