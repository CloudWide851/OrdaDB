use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::windows::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::UNIX_EPOCH;

use ordadb_types::DbError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::State;
use uuid::Uuid;
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_REPARSE_POINT, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
};

use crate::dbms::DbmsError;

const SETTINGS_FILE: &str = "console-settings-v1.json";
const SESSION_FILE: &str = "workspace-session-v1.json";
const PROFILES_FILE: &str = "connection-profiles-v1.json";
const SETTINGS_VERSION: u32 = 1;
const SESSION_VERSION: u32 = 1;
const PROFILES_VERSION: u32 = 1;
const MAX_SQL_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_DIRECTORY_DEPTH: usize = 32;
const MAX_WORKSPACE_ENTRIES: usize = 10_000;
const MAX_OPEN_DOCUMENTS: usize = 32;
const MAX_DRAFT_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONNECTION_PROFILES: usize = 32;
const NATIVE_CONNECTOR_ID: &str = "ordadb-native";

type DesktopResult<T> = std::result::Result<T, DbmsError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConsoleSettingsV1 {
    format_version: u32,
    ui_font_size: u8,
    data_font_size: u8,
    editor_font_size: u8,
    density: String,
    reopen_last_project: bool,
    hide_empty_catalog: bool,
}

impl Default for ConsoleSettingsV1 {
    fn default() -> Self {
        Self {
            format_version: SETTINGS_VERSION,
            ui_font_size: 11,
            data_font_size: 12,
            editor_font_size: 12,
            density: "compact".to_owned(),
            reopen_last_project: false,
            hide_empty_catalog: true,
        }
    }
}

impl ConsoleSettingsV1 {
    fn validate(&self) -> Result<(), DbError> {
        if self.format_version != SETTINGS_VERSION {
            return Err(unsupported_version("console settings", self.format_version));
        }
        if !(9..=16).contains(&self.ui_font_size)
            || !(10..=18).contains(&self.data_font_size)
            || !(10..=18).contains(&self.editor_font_size)
        {
            return Err(invalid(
                "console font sizes are outside the supported compact range",
            ));
        }
        if self.density != "compact" {
            return Err(invalid("console density must be compact"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileRevision {
    size_bytes: u64,
    modified_at_ms: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SqlDocument {
    path: String,
    name: String,
    content: String,
    revision: FileRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceEntry {
    path: String,
    name: String,
    kind: WorkspaceEntryKind,
    depth: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkspaceEntryKind {
    Directory,
    SqlFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceSnapshot {
    format_version: u32,
    root_path: String,
    entries: Vec<WorkspaceEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceDraft {
    path: String,
    content: String,
    base_revision: Option<FileRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceSessionV1 {
    format_version: u32,
    root_path: Option<String>,
    active_path: Option<String>,
    open_documents: Vec<WorkspaceDraft>,
}

impl Default for WorkspaceSessionV1 {
    fn default() -> Self {
        Self {
            format_version: SESSION_VERSION,
            root_path: None,
            active_path: None,
            open_documents: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectionProfileV1 {
    format_version: u32,
    profile_id: String,
    label: String,
    connector_id: String,
    dialect: String,
    endpoint: String,
    admin_endpoint: Option<String>,
    database: Option<String>,
    credential_id: String,
    auto_reconnect: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConnectionProfilesV1 {
    format_version: u32,
    profiles: Vec<ConnectionProfileV1>,
}

impl Default for ConnectionProfilesV1 {
    fn default() -> Self {
        Self {
            format_version: PROFILES_VERSION,
            profiles: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsoleBootstrap {
    settings: ConsoleSettingsV1,
    recovery: Option<WorkspaceSessionV1>,
    connection_profiles: Vec<ConnectionProfileV1>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OpenWorkspaceRequest {
    root_path: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DocumentRequest {
    root_path: String,
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NewDocumentRequest {
    root_path: String,
    parent_path: String,
    file_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SaveDocumentRequest {
    root_path: String,
    path: String,
    content: String,
    expected_revision: Option<FileRevision>,
    force: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RenameEntryRequest {
    root_path: String,
    path: String,
    new_name: String,
}

#[derive(Debug)]
pub struct ConsoleRuntime {
    root: PathBuf,
    write_lock: Mutex<()>,
}

impl ConsoleRuntime {
    pub fn open(root: PathBuf) -> Result<Arc<Self>, DbError> {
        fs::create_dir_all(&root)
            .map_err(|error| io_error("failed to create console state directory", error))?;
        let runtime = Arc::new(Self {
            root,
            write_lock: Mutex::new(()),
        });
        runtime.load_settings()?.validate()?;
        runtime.load_session()?;
        let (profiles, migrated) = runtime.load_profiles_with_migration()?;
        if migrated {
            let _guard = runtime.lock_writes()?;
            write_json_atomic(&runtime.root.join(PROFILES_FILE), &profiles)?;
        }
        Ok(runtime)
    }

    fn bootstrap(&self) -> Result<ConsoleBootstrap, DbError> {
        let settings = self.load_settings()?;
        settings.validate()?;
        let session = self.load_session()?;
        let recovery = if session.root_path.is_some() && !session.open_documents.is_empty() {
            Some(session)
        } else {
            None
        };
        Ok(ConsoleBootstrap {
            settings,
            recovery,
            connection_profiles: self.load_profiles()?.profiles,
        })
    }

    fn save_settings(&self, settings: ConsoleSettingsV1) -> Result<ConsoleSettingsV1, DbError> {
        settings.validate()?;
        let _guard = self.lock_writes()?;
        write_json_atomic(&self.root.join(SETTINGS_FILE), &settings)?;
        Ok(settings)
    }

    fn load_settings(&self) -> Result<ConsoleSettingsV1, DbError> {
        read_json_or_default(&self.root.join(SETTINGS_FILE))
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

    fn save_profile(
        &self,
        profile: ConnectionProfileV1,
    ) -> Result<Vec<ConnectionProfileV1>, DbError> {
        validate_profile(&profile)?;
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

    fn delete_profile(&self, profile_id: &str) -> Result<Vec<ConnectionProfileV1>, DbError> {
        validate_id(profile_id, "connection profile ID")?;
        let _guard = self.lock_writes()?;
        let mut document = self.load_profiles()?;
        document
            .profiles
            .retain(|profile| profile.profile_id != profile_id);
        write_json_atomic(&self.root.join(PROFILES_FILE), &document)?;
        Ok(document.profiles)
    }

    fn load_profiles(&self) -> Result<ConnectionProfilesV1, DbError> {
        self.load_profiles_with_migration()
            .map(|(document, _)| document)
    }

    fn load_profiles_with_migration(&self) -> Result<(ConnectionProfilesV1, bool), DbError> {
        let mut document: ConnectionProfilesV1 =
            read_json_or_default(&self.root.join(PROFILES_FILE))?;
        if document.format_version != PROFILES_VERSION {
            return Err(unsupported_version(
                "connection profiles",
                document.format_version,
            ));
        }
        if document.profiles.len() > MAX_CONNECTION_PROFILES {
            return Err(resource("connection profile limit exceeded"));
        }
        let mut ids = BTreeSet::new();
        let mut migrated = false;
        for profile in &mut document.profiles {
            let connector_id = migrate_connector_id(&profile.connector_id);
            if connector_id != profile.connector_id {
                profile.connector_id = connector_id.into();
                migrated = true;
            }
            validate_profile(profile)?;
            if !ids.insert(profile.profile_id.as_str()) {
                return Err(invalid("connection profile IDs must be unique"));
            }
        }
        Ok((document, migrated))
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
        read_sql_document(&root, &path)
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
        read_sql_document(&root, &path)
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
        read_sql_document(&root, &path)
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
    settings: ConsoleSettingsV1,
) -> DesktopResult<ConsoleSettingsV1> {
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

#[tauri::command(rename_all = "camelCase")]
pub fn workspace_open_document(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    request: DocumentRequest,
) -> DesktopResult<SqlDocument> {
    runtime.open_document(&request).map_err(Into::into)
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
    profile: ConnectionProfileV1,
) -> DesktopResult<Vec<ConnectionProfileV1>> {
    runtime.save_profile(profile).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn console_delete_connection_profile(
    runtime: State<'_, Arc<ConsoleRuntime>>,
    profile_id: String,
) -> DesktopResult<Vec<ConnectionProfileV1>> {
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
            validate_relative_sql_path(&draft.path)?;
            total
                .checked_add(draft.content.len())
                .ok_or_else(|| resource("workspace draft size overflowed"))
        })?;
    if total_bytes > MAX_DRAFT_BYTES {
        return Err(resource("workspace recovery drafts exceed 16 MiB"));
    }
    if let Some(active_path) = &session.active_path {
        validate_relative_sql_path(active_path)?;
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
    if session.root_path.is_none()
        && (session.active_path.is_some() || !session.open_documents.is_empty())
    {
        return Err(invalid(
            "workspace recovery documents require a workspace root",
        ));
    }
    Ok(())
}

fn validate_profile(profile: &ConnectionProfileV1) -> Result<(), DbError> {
    if profile.format_version != PROFILES_VERSION {
        return Err(unsupported_version(
            "connection profile",
            profile.format_version,
        ));
    }
    validate_id(&profile.profile_id, "connection profile ID")?;
    validate_id(&profile.credential_id, "credential ID")?;
    validate_text(&profile.label, 1, 128, "connection profile label")?;
    validate_text(&profile.connector_id, 1, 128, "connector ID")?;
    validate_text(&profile.dialect, 1, 32, "SQL dialect")?;
    validate_text(&profile.endpoint, 1, 2_048, "database endpoint")?;
    if let Some(admin_endpoint) = &profile.admin_endpoint {
        validate_text(admin_endpoint, 1, 2_048, "administration endpoint")?;
    }
    if let Some(database) = &profile.database {
        validate_text(database, 1, 256, "database name")?;
    }
    Ok(())
}

fn migrate_connector_id(connector_id: &str) -> &str {
    match connector_id {
        "ordadb-postgresql" => NATIVE_CONNECTOR_ID,
        "ordadb-mysql" => "mysql",
        "ordadb-sqlite" => "sqlite",
        "ordadb-sql-server" => "sql-server",
        current => current,
    }
}

fn canonical_workspace_root(value: &str) -> Result<PathBuf, DbError> {
    validate_text(value, 1, 32_768, "workspace root")?;
    let root = fs::canonicalize(value)
        .map_err(|error| io_error("failed to open SQL workspace root", error))?;
    if !root.is_dir() {
        return Err(invalid("SQL workspace root must be a directory"));
    }
    Ok(root)
}

fn resolve_workspace_entry(root: &Path, relative: &str) -> Result<PathBuf, DbError> {
    validate_relative_path(relative)?;
    let normalized = relative.replace('/', "\\");
    let joined = root.join(&normalized);
    reject_reparse_components(root, Path::new(&normalized))?;
    let canonical = fs::canonicalize(&joined)
        .map_err(|error| io_error("failed to resolve workspace entry", error))?;
    if !canonical.starts_with(root) {
        return Err(invalid("workspace entry escapes the selected root"));
    }
    Ok(canonical)
}

fn reject_reparse_components(root: &Path, relative: &Path) -> Result<(), DbError> {
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(invalid("workspace path must be relative and normalized"));
        };
        current.push(component);
        let metadata = fs::symlink_metadata(&current)
            .map_err(|error| io_error("failed to inspect workspace entry", error))?;
        if is_reparse_point(&metadata) {
            return Err(invalid(
                "workspace paths cannot pass through symbolic links or reparse points",
            ));
        }
    }
    Ok(())
}

fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

fn relative_display_path(root: &Path, path: &Path) -> Result<String, DbError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| invalid("workspace entry escapes the selected root"))?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

fn validate_relative_path(value: &str) -> Result<(), DbError> {
    if value.is_empty() || value.len() > 32_768 || value.contains('\0') {
        return Err(invalid("workspace path is empty or too long"));
    }
    let path = Path::new(value);
    let mut depth = 0_usize;
    for component in path.components() {
        match component {
            Component::Normal(_) => depth += 1,
            _ => return Err(invalid("workspace path must be relative and normalized")),
        }
    }
    if depth == 0 || depth > MAX_DIRECTORY_DEPTH {
        return Err(invalid("workspace path exceeds the supported depth"));
    }
    Ok(())
}

fn validate_relative_sql_path(value: &str) -> Result<(), DbError> {
    validate_relative_path(value)?;
    if !value.to_ascii_lowercase().ends_with(".sql") {
        return Err(invalid("workspace documents must use the .sql extension"));
    }
    Ok(())
}

fn validate_file_name(value: &str) -> Result<(), DbError> {
    validate_entry_name(value)?;
    if !value.to_ascii_lowercase().ends_with(".sql") {
        return Err(invalid("SQL file name must end with .sql"));
    }
    Ok(())
}

fn validate_entry_name(value: &str) -> Result<(), DbError> {
    if value.is_empty()
        || value.len() > 255
        || value.ends_with(['.', ' '])
        || value.contains(['\0', '/', '\\', ':', '*', '?', '"', '<', '>', '|'])
        || matches!(value, "." | "..")
        || value.chars().any(char::is_control)
    {
        return Err(invalid("workspace entry name is not a valid Windows name"));
    }
    Ok(())
}

fn is_sql_path(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("sql"))
}

fn read_sql_document(root: &Path, path: &Path) -> Result<SqlDocument, DbError> {
    if !path.is_file() || !is_sql_path(path) {
        return Err(invalid("workspace document must be an existing .sql file"));
    }
    let (bytes, revision) = read_sql_file_snapshot(path)?;
    let content = String::from_utf8(bytes)
        .map_err(|error| invalid("SQL file must be valid UTF-8").with_detail(error.to_string()))?;
    Ok(SqlDocument {
        path: relative_display_path(root, path)?,
        name: path
            .file_name()
            .ok_or_else(|| invalid("SQL file has no name"))?
            .to_string_lossy()
            .into_owned(),
        content,
        revision,
    })
}

fn file_revision(path: &Path) -> Result<FileRevision, DbError> {
    read_sql_file_snapshot(path).map(|(_, revision)| revision)
}

fn read_sql_file_snapshot(path: &Path) -> Result<(Vec<u8>, FileRevision), DbError> {
    let mut file = File::open(path).map_err(|error| io_error("failed to open SQL file", error))?;
    let before = file
        .metadata()
        .map_err(|error| io_error("failed to inspect SQL file", error))?;
    if before.len() > MAX_SQL_FILE_BYTES {
        return Err(resource("SQL file exceeds the 4 MiB limit"));
    }
    let capacity = usize::try_from(before.len())
        .map_err(|_| resource("SQL file size does not fit this process"))?;
    let mut bytes = Vec::with_capacity(capacity);
    Read::take(&mut file, MAX_SQL_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io_error("failed to read SQL file", error))?;
    if bytes.len() as u64 > MAX_SQL_FILE_BYTES {
        return Err(resource("SQL file exceeds the 4 MiB limit"));
    }
    let after = file
        .metadata()
        .map_err(|error| io_error("failed to inspect SQL file after reading", error))?;
    if before.len() != after.len()
        || before.modified().ok() != after.modified().ok()
        || after.len() != bytes.len() as u64
    {
        return Err(
            DbError::new("40001", "SQL file changed while OrdaDB was reading it")
                .with_hint("retry after the external editor finishes writing the file"),
        );
    }
    let modified_at_ms = after
        .modified()
        .map_err(|error| io_error("failed to read SQL file timestamp", error))?
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("SQL file timestamp is before the Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| resource("SQL file timestamp overflowed"))?;
    let sha256 = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok((
        bytes,
        FileRevision {
            size_bytes: after.len(),
            modified_at_ms,
            sha256,
        },
    ))
}

fn read_json_or_default<T>(path: &Path) -> Result<T, DbError>
where
    T: DeserializeOwned + Default,
{
    if !path.exists() {
        return Ok(T::default());
    }
    let metadata =
        fs::metadata(path).map_err(|error| io_error("failed to inspect console state", error))?;
    if metadata.len() > MAX_DRAFT_BYTES as u64 {
        return Err(resource("console state file exceeds 16 MiB"));
    }
    let file = File::open(path).map_err(|error| io_error("failed to open console state", error))?;
    serde_json::from_reader(file)
        .map_err(|error| invalid("console state JSON is invalid").with_detail(error.to_string()))
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), DbError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
        DbError::internal("failed to encode console state").with_detail(error.to_string())
    })?;
    if bytes.len() > MAX_DRAFT_BYTES {
        return Err(resource("console state file exceeds 16 MiB"));
    }
    write_bytes_atomic(path, &bytes)
}

fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<(), DbError> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("atomic state destination has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error("failed to create state destination", error))?;
    let extension = format!("tmp-{}", Uuid::new_v4());
    let temporary = path.with_extension(extension);
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| io_error("failed to create atomic state file", error))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| io_error("failed to synchronize atomic state file", error))?;
        move_file_replace(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn move_file_replace(source: &Path, destination: &Path) -> Result<(), DbError> {
    let source_wide = wide(source);
    let destination_wide = wide(destination);
    // SAFETY: both buffers are live NUL-terminated UTF-16 paths and flags
    // request a same-volume, write-through replacement.
    let moved = unsafe {
        MoveFileExW(
            source_wide.as_ptr(),
            destination_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(io_error(
            "failed to publish atomic state file",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

fn wide(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .to_string_lossy()
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect()
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
        return Err(invalid(format!(
            "{context} must use 1-128 ASCII letters, digits, dots, hyphens, or underscores"
        )));
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
        return Err(invalid(format!(
            "{context} must contain {minimum}-{maximum} printable UTF-8 bytes"
        )));
    }
    Ok(())
}

fn unsupported_version(context: &str, version: u32) -> DbError {
    DbError::new(
        "0A000",
        format!("{context} format version {version} is not supported"),
    )
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn resource(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::os::windows::fs::symlink_dir;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn settings_and_recovery_are_atomic_and_secret_free() {
        let directory = tempdir().expect("tempdir");
        let runtime = ConsoleRuntime::open(directory.path().join("state")).expect("runtime");
        assert_eq!(
            runtime
                .bootstrap()
                .expect("bootstrap")
                .settings
                .ui_font_size,
            11
        );

        let root = directory.path().join("project");
        fs::create_dir_all(&root).expect("project");
        fs::write(root.join("draft.sql"), "select 1;").expect("sql");
        runtime
            .save_session(WorkspaceSessionV1 {
                format_version: 1,
                root_path: Some(root.display().to_string()),
                active_path: Some("draft.sql".into()),
                open_documents: vec![WorkspaceDraft {
                    path: "draft.sql".into(),
                    content: "select 2;".into(),
                    base_revision: None,
                }],
            })
            .expect("session");
        let encoded =
            fs::read_to_string(runtime.root.join(SESSION_FILE)).expect("read session state");
        assert!(encoded.contains("select 2;"));
        assert!(!encoded.to_ascii_lowercase().contains("password"));
        assert!(runtime.bootstrap().expect("bootstrap").recovery.is_some());
    }

    #[test]
    fn workspace_enforces_utf8_limits_and_external_revision_conflicts() {
        let directory = tempdir().expect("tempdir");
        let runtime = ConsoleRuntime::open(directory.path().join("state")).expect("runtime");
        let project = directory.path().join("project");
        fs::create_dir_all(project.join("nested")).expect("project");
        fs::write(project.join("nested").join("query.sql"), "select 1;").expect("sql");
        fs::write(project.join("ignored.txt"), "ignored").expect("text");

        let snapshot = runtime
            .snapshot(&project.display().to_string())
            .expect("snapshot");
        assert_eq!(snapshot.entries.len(), 2);
        assert_eq!(snapshot.entries[1].path, "nested/query.sql");

        let document = runtime
            .open_document(&DocumentRequest {
                root_path: project.display().to_string(),
                path: "nested/query.sql".into(),
            })
            .expect("document");
        assert_eq!(document.revision.size_bytes, document.content.len() as u64);
        assert_eq!(
            document.revision.sha256,
            Sha256::digest(document.content.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        fs::write(project.join("nested").join("query.sql"), "select 2;").expect("external");
        let error = runtime
            .save_document(&SaveDocumentRequest {
                root_path: project.display().to_string(),
                path: "nested/query.sql".into(),
                content: "select 3;".into(),
                expected_revision: Some(document.revision),
                force: false,
            })
            .expect_err("conflict");
        assert_eq!(error.sql_state, "40001");
    }

    #[test]
    fn workspace_rejects_intermediate_reparse_points() {
        let directory = tempdir().expect("tempdir");
        let runtime = ConsoleRuntime::open(directory.path().join("state")).expect("runtime");
        let project = directory.path().join("project");
        let real = project.join("real");
        fs::create_dir_all(&real).expect("project");
        fs::write(real.join("query.sql"), "select 1;").expect("sql");
        let alias = project.join("alias");
        if let Err(error) = symlink_dir(&real, &alias) {
            if error.raw_os_error() == Some(1314) {
                return;
            }
            panic!("create directory symlink: {error}");
        }

        let snapshot = runtime
            .snapshot(&project.display().to_string())
            .expect("snapshot");
        assert!(
            snapshot
                .entries
                .iter()
                .all(|entry| !entry.path.starts_with("alias"))
        );
        let error = runtime
            .open_document(&DocumentRequest {
                root_path: project.display().to_string(),
                path: "alias/query.sql".into(),
            })
            .expect_err("reparse point");
        assert_eq!(error.sql_state, "22023");
        assert!(error.message.contains("reparse"));
    }

    #[test]
    fn profiles_persist_only_credential_references() {
        let directory = tempdir().expect("tempdir");
        let runtime = ConsoleRuntime::open(directory.path().join("state")).expect("runtime");
        let profiles = runtime
            .save_profile(ConnectionProfileV1 {
                format_version: 1,
                profile_id: "local".into(),
                label: "本地 OrdaDB".into(),
                connector_id: NATIVE_CONNECTOR_ID.into(),
                dialect: "postgresql".into(),
                endpoint: "127.0.0.1:54329".into(),
                admin_endpoint: Some("http://127.0.0.1:9080".into()),
                database: Some("ordadb".into()),
                credential_id: "local-credential".into(),
                auto_reconnect: true,
            })
            .expect("profile");
        assert_eq!(profiles.len(), 1);
        let encoded = fs::read_to_string(runtime.root.join(PROFILES_FILE)).expect("read profiles");
        assert!(encoded.contains("local-credential"));
        assert!(!encoded.to_ascii_lowercase().contains("password"));
        assert!(!encoded.to_ascii_lowercase().contains("api key"));
    }

    #[test]
    fn legacy_connector_ids_migrate_atomically_without_changing_credentials() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("state");
        fs::create_dir_all(&root).expect("state directory");
        let document = ConnectionProfilesV1 {
            format_version: PROFILES_VERSION,
            profiles: vec![ConnectionProfileV1 {
                format_version: PROFILES_VERSION,
                profile_id: "legacy-local".into(),
                label: "Legacy local".into(),
                connector_id: "ordadb-postgresql".into(),
                dialect: "postgresql".into(),
                endpoint: "127.0.0.1:54329".into(),
                admin_endpoint: Some("http://127.0.0.1:9080".into()),
                database: Some("ordadb".into()),
                credential_id: "credential-reference".into(),
                auto_reconnect: true,
            }],
        };
        write_json_atomic(&root.join(PROFILES_FILE), &document).expect("legacy document");

        let runtime = ConsoleRuntime::open(root).expect("runtime");
        let migrated = runtime.load_profiles().expect("migrated profiles");
        assert_eq!(migrated.profiles[0].connector_id, NATIVE_CONNECTOR_ID);
        assert_eq!(
            migrated.profiles[0].credential_id, "credential-reference",
            "the Credential Manager reference must survive ID migration"
        );
        let persisted =
            fs::read_to_string(runtime.root.join(PROFILES_FILE)).expect("persisted migration");
        assert!(persisted.contains("\"connectorId\": \"ordadb-native\""));
        assert!(!persisted.contains("ordadb-postgresql"));
    }
}
