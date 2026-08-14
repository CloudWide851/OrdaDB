
impl ConsoleRuntime {
    pub fn open(root: PathBuf) -> Result<Arc<Self>, DbError> {
        fs::create_dir_all(&root)
            .map_err(|error| io_error("failed to create console state directory", error))?;
        let runtime = Arc::new(Self {
            root,
            write_lock: Mutex::new(()),
        });
        let (settings, settings_migrated) = runtime.load_settings_with_migration()?;
        settings.validate()?;
        runtime.load_session()?;
        runtime.load_recent_files()?;
        let (profiles, profiles_migrated) = runtime.load_profiles_with_migration()?;
        if settings_migrated || profiles_migrated {
            let _guard = runtime.lock_writes()?;
            if settings_migrated {
                write_json_atomic(&runtime.root.join(SETTINGS_FILE), &settings)?;
            }
            if profiles_migrated {
                write_json_atomic(&runtime.root.join(PROFILES_FILE), &profiles)?;
            }
        }
        Ok(runtime)
    }

    fn bootstrap(&self) -> Result<ConsoleBootstrap, DbError> {
        let settings = self.load_settings()?;
        settings.validate()?;
        let session = self.load_session()?;
        let recovery = if !session.open_documents.is_empty() {
            Some(session)
        } else {
            None
        };
        Ok(ConsoleBootstrap {
            settings,
            recovery,
            recent_files: self.load_recent_files()?.entries,
            connection_profiles: self.load_profiles()?.profiles,
            connector_descriptors: connector_descriptors(),
        })
    }

    fn save_settings(&self, settings: ConsoleSettingsV2) -> Result<ConsoleSettingsV2, DbError> {
        settings.validate()?;
        let _guard = self.lock_writes()?;
        write_json_atomic(&self.root.join(SETTINGS_FILE), &settings)?;
        Ok(settings)
    }

    fn load_settings(&self) -> Result<ConsoleSettingsV2, DbError> {
        self.load_settings_with_migration()
            .map(|(settings, _)| settings)
    }

    fn load_settings_with_migration(&self) -> Result<(ConsoleSettingsV2, bool), DbError> {
        let current = self.root.join(SETTINGS_FILE);
        if current.exists() {
            let settings: ConsoleSettingsV2 = read_json_or_default(&current)?;
            settings.validate()?;
            return Ok((settings, false));
        }
        let legacy = self.root.join(LEGACY_SETTINGS_FILE);
        if legacy.exists() {
            let settings: ConsoleSettingsV1 = read_json_or_default(&legacy)?;
            settings.validate()?;
            let migrated = ConsoleSettingsV2::from(settings);
            migrated.validate()?;
            return Ok((migrated, true));
        }
        Ok((ConsoleSettingsV2::default(), false))
    }

    fn save_session(&self, session: WorkspaceSessionV1) -> Result<(), DbError> {
        validate_session(&session)?;
        let _guard = self.lock_writes()?;
        write_json_atomic(&self.root.join(SESSION_FILE), &session)
    }

    fn load_session(&self) -> Result<WorkspaceSessionV1, DbError> {
        let session: WorkspaceSessionV1 = read_json_or_default(&self.root.join(SESSION_FILE))?;
        validate_session(&session)?;
        Ok(session)
    }

    fn load_recent_files(&self) -> Result<RecentFilesV1, DbError> {
        let recent: RecentFilesV1 = read_json_or_default(&self.root.join(RECENT_FILES_FILE))?;
        validate_recent_files(&recent)?;
        Ok(recent)
    }

    fn record_recent_document(
        &self,
        document: &SqlDocument,
    ) -> Result<Vec<RecentFileEntry>, DbError> {
        let _guard = self.lock_writes()?;
        let mut recent = self.load_recent_files()?;
        let entry = RecentFileEntry {
            locator: document.locator.clone(),
            name: document.name.clone(),
            opened_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| invalid("current time is before the Unix epoch"))?
                .as_millis()
                .try_into()
                .map_err(|_| resource("recent-file timestamp overflowed"))?,
        };
        validate_recent_entry(&entry)?;
        recent
            .entries
            .retain(|candidate| candidate.locator != entry.locator);
        recent.entries.insert(0, entry);
        recent.entries.truncate(MAX_RECENT_FILES);
        write_json_atomic(&self.root.join(RECENT_FILES_FILE), &recent)?;
        Ok(recent.entries)
    }

    fn save_profile(
        &self,
        profile: ConnectionProfileV3,
    ) -> Result<Vec<ConnectionProfileV3>, DbError> {
        validate_profile_v3(&profile)?;
        let _guard = self.lock_writes()?;
        let mut document = self.load_profiles()?;
        if let Some(current) = document
            .profiles
            .iter_mut()
            .find(|current| current.profile_id == profile.profile_id)
        {
            *current = profile;
        } else {
            if document.profiles.len() >= MAX_CONNECTION_PROFILES {
                return Err(resource("connection profile limit exceeded"));
            }
            document.profiles.push(profile);
        }
        document
            .profiles
            .sort_by(|left, right| left.label.cmp(&right.label));
        write_json_atomic(&self.root.join(PROFILES_FILE), &document)?;
        Ok(document.profiles)
    }

    fn delete_profile(&self, profile_id: &str) -> Result<Vec<ConnectionProfileV3>, DbError> {
        validate_id(profile_id, "connection profile ID")?;
        let _guard = self.lock_writes()?;
        let mut document = self.load_profiles()?;
        document
            .profiles
            .retain(|profile| profile.profile_id != profile_id);
        write_json_atomic(&self.root.join(PROFILES_FILE), &document)?;
        Ok(document.profiles)
    }

    fn load_profiles(&self) -> Result<ConnectionProfilesV3, DbError> {
        self.load_profiles_with_migration()
            .map(|(document, _)| document)
    }

    fn load_profiles_with_migration(&self) -> Result<(ConnectionProfilesV3, bool), DbError> {
        let current = self.root.join(PROFILES_FILE);
        if current.exists() {
            let document: ConnectionProfilesV3 = read_json_or_default(&current)?;
            validate_profiles_v3(&document)?;
            return Ok((document, false));
        }
        let legacy_v2_path = self.root.join(LEGACY_PROFILES_V2_FILE);
        if legacy_v2_path.exists() {
            let legacy: ConnectionProfilesV2 = read_json_or_default(&legacy_v2_path)?;
            validate_profiles_v2(&legacy)?;
            let document = ConnectionProfilesV3 {
                format_version: PROFILES_VERSION,
                profiles: legacy
                    .profiles
                    .into_iter()
                    .map(ConnectionProfileV3::from)
                    .collect(),
            };
            validate_profiles_v3(&document)?;
            return Ok((document, true));
        }
        let legacy_path = self.root.join(LEGACY_PROFILES_FILE);
        if !legacy_path.exists() {
            return Ok((ConnectionProfilesV3::default(), false));
        }
        let legacy: ConnectionProfilesV1 = read_json_or_default(&legacy_path)?;
        if legacy.format_version != 1 {
            return Err(unsupported_version(
                "connection profiles",
                legacy.format_version,
            ));
        }
        if legacy.profiles.len() > MAX_CONNECTION_PROFILES {
            return Err(resource("connection profile limit exceeded"));
        }
        let profiles = legacy
            .profiles
            .into_iter()
            .map(ConnectionProfileV2::from)
            .map(ConnectionProfileV3::from)
            .collect();
        let document = ConnectionProfilesV3 {
            format_version: PROFILES_VERSION,
            profiles,
        };
        validate_profiles_v3(&document)?;
        Ok((document, true))
    }

    pub(crate) fn load_ai_state(&self) -> Result<AiPersistenceV1, DbError> {
        let path = self.root.join(AI_STATE_FILE);
        if !path.exists() {
            return Ok(AiPersistenceV1::default());
        }
        let metadata =
            fs::metadata(&path).map_err(|error| io_error("failed to inspect AI state", error))?;
        if metadata.len() > MAX_PERSISTED_STATE_BYTES as u64 {
            return Err(resource("AI state exceeds the 2 MiB limit"));
        }
        let bytes = fs::read(&path).map_err(|error| io_error("failed to read AI state", error))?;
        decode_persistence(&bytes)
    }

    pub(crate) fn save_ai_state(
        &self,
        state: &AiPersistenceV1,
    ) -> Result<AiPersistenceV1, DbError> {
        let projected = project_persistence(state.history.clone(), state.audit.clone())?;
        if projected != *state {
            return Err(invalid("AI state exceeds a retention limit"));
        }
        let _guard = self.lock_writes()?;
        write_json_atomic(&self.root.join(AI_STATE_FILE), &projected)?;
        Ok(projected)
    }

    fn snapshot(&self, root_path: &str) -> Result<WorkspaceSnapshot, DbError> {
        let root = canonical_workspace_root(root_path)?;
        let mut entries = Vec::new();
        let mut pending = vec![(root.clone(), 0_usize)];
        while let Some((directory, depth)) = pending.pop() {
            if depth >= MAX_DIRECTORY_DEPTH {
                continue;
            }
            let children = fs::read_dir(&directory)
                .map_err(|error| io_error("failed to read SQL workspace directory", error))?;
            for child in children {
                let child =
                    child.map_err(|error| io_error("failed to read workspace entry", error))?;
                let path = child.path();
                let metadata = fs::symlink_metadata(&path)
                    .map_err(|error| io_error("failed to inspect workspace entry", error))?;
                if is_reparse_point(&metadata) {
                    continue;
                }
                let child_depth = depth + 1;
                let kind = if metadata.file_type().is_dir() {
                    pending.push((path.clone(), child_depth));
                    WorkspaceEntryKind::Directory
                } else if metadata.file_type().is_file() && is_sql_path(&path) {
                    WorkspaceEntryKind::SqlFile
                } else {
                    continue;
                };
                entries.push(WorkspaceEntry {
                    path: relative_display_path(&root, &path)?,
                    name: child.file_name().to_string_lossy().into_owned(),
                    kind,
                    depth: child_depth,
                });
                if entries.len() > MAX_WORKSPACE_ENTRIES {
                    return Err(resource("SQL workspace contains more than 10,000 entries"));
                }
            }
        }
        entries.sort_by(|left, right| {
            left.path
                .to_lowercase()
                .cmp(&right.path.to_lowercase())
                .then_with(|| left.path.cmp(&right.path))
        });
        Ok(WorkspaceSnapshot {
            format_version: SESSION_VERSION,
            root_path: root.display().to_string(),
            entries,
        })
    }

    fn open_document(&self, request: &DocumentRequest) -> Result<SqlDocument, DbError> {
        let root = canonical_workspace_root(&request.root_path)?;
        let path = resolve_workspace_entry(&root, &request.path)?;
        let document = read_workspace_document(&root, &path)?;
        self.record_recent_document(&document)?;
        Ok(document)
    }

    fn new_document(&self, request: &NewDocumentRequest) -> Result<SqlDocument, DbError> {
        validate_file_name(&request.file_name)?;
        let _guard = self.lock_writes()?;
        let root = canonical_workspace_root(&request.root_path)?;
        let parent = if request.parent_path.is_empty() {
            root.clone()
        } else {
            resolve_workspace_entry(&root, &request.parent_path)?
        };
        if !parent.is_dir() {
            return Err(invalid("new SQL file parent must be a directory"));
        }
        let path = parent.join(&request.file_name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| io_error("failed to create SQL file", error))?;
        file.write_all(b"")
            .and_then(|_| file.sync_all())
            .map_err(|error| io_error("failed to synchronize new SQL file", error))?;
        let document = read_workspace_document(&root, &path)?;
        drop(_guard);
        self.record_recent_document(&document)?;
        Ok(document)
    }

    fn save_document(&self, request: &SaveDocumentRequest) -> Result<SqlDocument, DbError> {
        if request.content.len() as u64 > MAX_SQL_FILE_BYTES {
            return Err(resource("SQL file exceeds the 4 MiB limit"));
        }
        let _guard = self.lock_writes()?;
        let root = canonical_workspace_root(&request.root_path)?;
        let path = resolve_workspace_entry(&root, &request.path)?;
        if !path.is_file() || !is_sql_path(&path) {
            return Err(invalid("only existing UTF-8 .sql files can be saved"));
        }
        let current = file_revision(&path)?;
        if !request.force
            && request
                .expected_revision
                .as_ref()
                .is_some_and(|expected| expected != &current)
        {
            return Err(DbError::new("40001", "SQL file changed outside OrdaDB")
                .with_detail("the on-disk revision no longer matches the opened document")
                .with_hint("reload the file or explicitly overwrite the external revision"));
        }
        write_bytes_atomic(&path, request.content.as_bytes())?;
        let document = read_workspace_document(&root, &path)?;
        drop(_guard);
        self.record_recent_document(&document)?;
        Ok(document)
    }

    fn open_external_document(
        &self,
        request: &ExternalDocumentRequest,
    ) -> Result<SqlDocument, DbError> {
        let path = canonical_external_sql_file(&request.path)?;
        let document = read_external_document(&path)?;
        self.record_recent_document(&document)?;
        Ok(document)
    }

    fn save_external_document(
        &self,
        request: &SaveExternalDocumentRequest,
    ) -> Result<SqlDocument, DbError> {
        if request.content.len() as u64 > MAX_SQL_FILE_BYTES {
            return Err(resource("SQL file exceeds the 4 MiB limit"));
        }
        let _guard = self.lock_writes()?;
        let path = canonical_external_sql_file(&request.path)?;
        let current = file_revision(&path)?;
        if !request.force
            && request
                .expected_revision
                .as_ref()
                .is_some_and(|expected| expected != &current)
        {
            return Err(DbError::new("40001", "SQL file changed outside OrdaDB")
                .with_detail("the on-disk revision no longer matches the opened document")
                .with_hint("reload the file or explicitly overwrite the external revision"));
        }
        write_bytes_atomic(&path, request.content.as_bytes())?;
        let document = read_external_document(&path)?;
        drop(_guard);
        self.record_recent_document(&document)?;
        Ok(document)
    }

    fn save_document_as_path(
        &self,
        request: &SaveDocumentAsRequest,
        selected_path: &Path,
    ) -> Result<SqlDocument, DbError> {
        if request.content.len() as u64 > MAX_SQL_FILE_BYTES {
            return Err(resource("SQL file exceeds the 4 MiB limit"));
        }
        validate_file_name(&request.suggested_name)?;
        let _guard = self.lock_writes()?;
        let path = normalize_save_destination(selected_path)?;
        write_bytes_atomic(&path, request.content.as_bytes())?;
        let path = canonical_external_sql_file(&path.display().to_string())?;
        let document = read_external_document(&path)?;
        drop(_guard);
        self.record_recent_document(&document)?;
        Ok(document)
    }

    fn rename_entry(&self, request: &RenameEntryRequest) -> Result<WorkspaceSnapshot, DbError> {
        validate_entry_name(&request.new_name)?;
        let _guard = self.lock_writes()?;
        let root = canonical_workspace_root(&request.root_path)?;
        let path = resolve_workspace_entry(&root, &request.path)?;
        if path == root {
            return Err(invalid("the workspace root cannot be renamed"));
        }
        if path.is_file() && (!is_sql_path(&path) || !request.new_name.ends_with(".sql")) {
            return Err(invalid("SQL files must retain a .sql extension"));
        }
        let destination = path
            .parent()
            .ok_or_else(|| invalid("workspace entry has no parent"))?
            .join(&request.new_name);
        if destination.exists() {
            return Err(DbError::new(
                "42P07",
                "a workspace entry with that name already exists",
            ));
        }
        fs::rename(&path, &destination)
            .map_err(|error| io_error("failed to rename workspace entry", error))?;
        self.snapshot(&request.root_path)
    }

    fn trash_entry(&self, request: &DocumentRequest) -> Result<WorkspaceSnapshot, DbError> {
        let _guard = self.lock_writes()?;
        let root = canonical_workspace_root(&request.root_path)?;
        let path = resolve_workspace_entry(&root, &request.path)?;
        if path == root {
            return Err(invalid(
                "the workspace root cannot be moved to the Recycle Bin",
            ));
        }
        trash::delete(&path).map_err(|error| {
            DbError::new("58030", "failed to move workspace entry to the Recycle Bin")
                .with_detail(error.to_string())
        })?;
        self.snapshot(&request.root_path)
    }

    fn lock_writes(&self) -> Result<MutexGuard<'_, ()>, DbError> {
        self.write_lock
            .lock()
            .map_err(|_| DbError::internal("console state write lock is poisoned"))
    }
}

#[tauri::command]
pub fn console_bootstrap(
    runtime: State<'_, Arc<ConsoleRuntime>>,
) -> DesktopResult<ConsoleBootstrap> {
    runtime.bootstrap().map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn console_save_settings(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    settings: ConsoleSettingsV2,
) -> DesktopResult<ConsoleSettingsV2> {
    runtime.save_settings(settings).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn workspace_open(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    request: OpenWorkspaceRequest,
) -> DesktopResult<WorkspaceSnapshot> {
    runtime.snapshot(&request.root_path).map_err(Into::into)
}

#[tauri::command]
pub async fn workspace_pick_folder(
    runtime: State<'_, Arc<ConsoleRuntime>>,
) -> DesktopResult<Option<WorkspaceSnapshot>> {
    let selected = tauri::async_runtime::spawn_blocking(|| {
        rfd::FileDialog::new()
            .set_title("打开 SQL 项目")
            .pick_folder()
    })
    .await
    .map_err(|error| {
        DbmsError::from(
            DbError::internal("folder picker task failed").with_detail(error.to_string()),
        )
    })?;
    selected
        .map(|path| {
            runtime
                .snapshot(&path.display().to_string())
                .map_err(Into::into)
        })
        .transpose()
}

#[tauri::command]
pub async fn workspace_pick_document(
    runtime: State<'_, Arc<ConsoleRuntime>>,
) -> DesktopResult<Option<SqlDocument>> {
    let selected = tauri::async_runtime::spawn_blocking(|| {
        rfd::FileDialog::new()
            .set_title("打开 SQL 文件")
            .add_filter("SQL", &["sql"])
            .pick_file()
    })
    .await
    .map_err(|error| {
        DbmsError::from(DbError::internal("file picker task failed").with_detail(error.to_string()))
    })?;
    selected
        .map(|path| {
            runtime
                .open_external_document(&ExternalDocumentRequest {
                    path: path.display().to_string(),
                })
                .map_err(Into::into)
        })
        .transpose()
}

#[tauri::command(rename_all = "camelCase")]
pub fn workspace_open_document(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    request: DocumentRequest,
) -> DesktopResult<SqlDocument> {
    runtime.open_document(&request).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn workspace_open_external_document(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    request: ExternalDocumentRequest,
) -> DesktopResult<SqlDocument> {
    runtime.open_external_document(&request).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn workspace_new_document(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    request: NewDocumentRequest,
) -> DesktopResult<SqlDocument> {
    runtime.new_document(&request).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn workspace_save_document(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    request: SaveDocumentRequest,
) -> DesktopResult<SqlDocument> {
    runtime.save_document(&request).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn workspace_save_external_document(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    request: SaveExternalDocumentRequest,
) -> DesktopResult<SqlDocument> {
    runtime.save_external_document(&request).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn workspace_save_document_as(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    request: SaveDocumentAsRequest,
) -> DesktopResult<Option<SqlDocument>> {
    validate_file_name(&request.suggested_name).map_err(DbmsError::from)?;
    let suggested_name = request.suggested_name.clone();
    let selected = tauri::async_runtime::spawn_blocking(move || {
        rfd::FileDialog::new()
            .set_title("保存 SQL 文件")
            .add_filter("SQL", &["sql"])
            .set_file_name(&suggested_name)
            .save_file()
    })
    .await
    .map_err(|error| {
        DbmsError::from(
            DbError::internal("Save As picker task failed").with_detail(error.to_string()),
        )
    })?;
    selected
        .map(|path| {
            runtime
                .save_document_as_path(&request, &path)
                .map_err(Into::into)
        })
        .transpose()
}

#[tauri::command(rename_all = "camelCase")]
pub fn workspace_rename_entry(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    request: RenameEntryRequest,
) -> DesktopResult<WorkspaceSnapshot> {
    runtime.rename_entry(&request).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn workspace_trash_entry(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    request: DocumentRequest,
) -> DesktopResult<WorkspaceSnapshot> {
    runtime.trash_entry(&request).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn workspace_save_session(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    session: WorkspaceSessionV1,
) -> DesktopResult<()> {
    runtime.save_session(session).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn console_save_connection_profile(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    profile: ConnectionProfileV3,
) -> DesktopResult<Vec<ConnectionProfileV3>> {
    runtime.save_profile(profile).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn console_delete_connection_profile(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    profile_id: String,
) -> DesktopResult<Vec<ConnectionProfileV3>> {
    runtime.delete_profile(&profile_id).map_err(Into::into)
}

fn validate_session(session: &WorkspaceSessionV1) -> Result<(), DbError> {
    if session.format_version != SESSION_VERSION {
        return Err(unsupported_version(
            "workspace session",
            session.format_version,
        ));
    }
    if session.open_documents.len() > MAX_OPEN_DOCUMENTS {
        return Err(resource("workspace open-document limit exceeded"));
    }
    let total_bytes = session
        .open_documents
        .iter()
        .try_fold(0_usize, |total, draft| {
            validate_text(&draft.path, 1, 32_768, "document identity")?;
            if let Some(locator) = &draft.locator {
                validate_document_locator(locator)?;
                if matches!(locator, DocumentLocator::Untitled { .. })
                    && draft.base_revision.is_some()
                {
                    return Err(invalid(
                        "untitled recovery drafts cannot carry a file revision",
                    ));
                }
            } else {
                if session.root_path.is_none() {
                    return Err(invalid(
                        "legacy workspace recovery documents require a workspace root",
                    ));
                }
                validate_relative_sql_path(&draft.path)?;
            }
            if let Some(name) = &draft.name {
                validate_text(name, 1, 255, "document name")?;
            }
            total
                .checked_add(draft.content.len())
                .ok_or_else(|| resource("workspace draft size overflowed"))
        })?;
    if total_bytes > MAX_DRAFT_BYTES {
        return Err(resource("workspace recovery drafts exceed 16 MiB"));
    }
    if let Some(active_path) = &session.active_path {
        validate_text(active_path, 1, 32_768, "active document identity")?;
        if !session
            .open_documents
            .iter()
            .any(|draft| draft.path == *active_path)
        {
            return Err(invalid(
                "active workspace document must be present in open documents",
            ));
        }
    }
    Ok(())
}

fn validate_recent_files(recent: &RecentFilesV1) -> Result<(), DbError> {
    if recent.format_version != 1 {
        return Err(unsupported_version("recent files", recent.format_version));
    }
    if recent.entries.len() > MAX_RECENT_FILES {
        return Err(resource("recent-file limit exceeded"));
    }
    for (index, entry) in recent.entries.iter().enumerate() {
        validate_recent_entry(entry)?;
        if recent.entries[..index]
            .iter()
            .any(|candidate| candidate.locator == entry.locator)
        {
            return Err(invalid("recent-file locators must be unique"));
        }
    }
    Ok(())
}

fn validate_recent_entry(entry: &RecentFileEntry) -> Result<(), DbError> {
    validate_text(&entry.name, 1, 255, "recent file name")?;
    validate_document_locator(&entry.locator)?;
    if matches!(entry.locator, DocumentLocator::Untitled { .. }) {
        return Err(invalid("untitled documents cannot enter recent files"));
    }
    Ok(())
}

fn validate_document_locator(locator: &DocumentLocator) -> Result<(), DbError> {
    match locator {
        DocumentLocator::Workspace { root_path, path } => {
            validate_absolute_path_text(root_path, "workspace root")?;
            validate_relative_sql_path(path)
        }
        DocumentLocator::External { path } => {
            validate_absolute_path_text(path, "external SQL file")?;
            if !is_sql_path(Path::new(path)) {
                return Err(invalid("external documents must use the .sql extension"));
            }
            Ok(())
        }
        DocumentLocator::Untitled { id } => validate_id(id, "untitled document ID"),
    }
}

fn validate_profiles_v2(document: &ConnectionProfilesV2) -> Result<(), DbError> {
    if document.format_version != 2 {
        return Err(unsupported_version(
            "connection profiles",
            document.format_version,
        ));
    }
    if document.profiles.len() > MAX_CONNECTION_PROFILES {
        return Err(resource("connection profile limit exceeded"));
    }
    let mut ids = BTreeSet::new();
    for profile in &document.profiles {
        validate_profile_v2(profile)?;
        if !ids.insert(profile.profile_id.as_str()) {
            return Err(invalid("connection profile IDs must be unique"));
        }
    }
    Ok(())
}
