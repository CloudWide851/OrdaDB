use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::windows::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use ordadb_ai::{
    AiPersistenceV1, MAX_PERSISTED_STATE_BYTES, decode_persistence, project_persistence,
};
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

const SETTINGS_FILE: &str = "console-settings-v2.json";
const LEGACY_SETTINGS_FILE: &str = "console-settings-v1.json";
const SESSION_FILE: &str = "workspace-session-v1.json";
const RECENT_FILES_FILE: &str = "recent-files-v1.json";
const PROFILES_FILE: &str = "connection-profiles-v3.json";
const LEGACY_PROFILES_V2_FILE: &str = "connection-profiles-v2.json";
const LEGACY_PROFILES_FILE: &str = "connection-profiles-v1.json";
const AI_STATE_FILE: &str = "ai-state-v1.json";
const SETTINGS_VERSION: u32 = 2;
const SESSION_VERSION: u32 = 1;
const PROFILES_VERSION: u32 = 3;
const MAX_SQL_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_DIRECTORY_DEPTH: usize = 32;
const MAX_WORKSPACE_ENTRIES: usize = 10_000;
const MAX_OPEN_DOCUMENTS: usize = 32;
const MAX_DRAFT_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONNECTION_PROFILES: usize = 32;
const MAX_RECENT_FILES: usize = 50;
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
            format_version: 1,
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
        if self.format_version != 1 {
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
pub struct ConsoleSettingsV2 {
    format_version: u32,
    appearance: AppearanceSettingsV2,
    editor: EditorSettingsV2,
    files: FileSettingsV2,
    results: ResultSettingsV2,
    connections: ConnectionSettingsV2,
    ai: AiSettingsV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppearanceSettingsV2 {
    theme: String,
    zoom_percent: u16,
    ui_font_size: u8,
    data_font_size: u8,
    density: String,
    reduce_motion: bool,
    hide_empty_catalog: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EditorSettingsV2 {
    font_family: String,
    font_size: u8,
    tab_size: u8,
    word_wrap: String,
    minimap: bool,
    format_on_save: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileSettingsV2 {
    recovery_policy: String,
    auto_save: String,
    auto_save_delay_ms: u32,
    confirm_dirty_close: bool,
    reopen_last_project: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResultSettingsV2 {
    page_size: u16,
    resident_row_limit: u32,
    resident_memory_bytes: u64,
    null_display: String,
    query_timeout_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectionSettingsV2 {
    timeout_ms: u32,
    auto_reconnect_local: bool,
    confirm_dangerous_writes: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiSettingsV2 {
    provider: String,
    model: String,
    endpoint: Option<String>,
    reasoning: String,
    data_sharing: String,
    credential_id: Option<String>,
}

impl Default for ConsoleSettingsV2 {
    fn default() -> Self {
        Self {
            format_version: SETTINGS_VERSION,
            appearance: AppearanceSettingsV2 {
                theme: "system".to_owned(),
                zoom_percent: 100,
                ui_font_size: 11,
                data_font_size: 12,
                density: "compact".to_owned(),
                reduce_motion: false,
                hide_empty_catalog: true,
            },
            editor: EditorSettingsV2 {
                font_family: "Cascadia Mono".to_owned(),
                font_size: 12,
                tab_size: 2,
                word_wrap: "off".to_owned(),
                minimap: false,
                format_on_save: false,
            },
            files: FileSettingsV2 {
                recovery_policy: "prompt".to_owned(),
                auto_save: "off".to_owned(),
                auto_save_delay_ms: 1_000,
                confirm_dirty_close: true,
                reopen_last_project: false,
            },
            results: ResultSettingsV2 {
                page_size: 256,
                resident_row_limit: 10_000,
                resident_memory_bytes: 16 * 1024 * 1024,
                null_display: "NULL".to_owned(),
                query_timeout_ms: 30_000,
            },
            connections: ConnectionSettingsV2 {
                timeout_ms: 30_000,
                auto_reconnect_local: true,
                confirm_dangerous_writes: true,
            },
            ai: AiSettingsV2 {
                provider: "openai".to_owned(),
                model: "gpt-5.6".to_owned(),
                endpoint: None,
                reasoning: "medium".to_owned(),
                data_sharing: "schemaOnly".to_owned(),
                credential_id: None,
            },
        }
    }
}

impl From<ConsoleSettingsV1> for ConsoleSettingsV2 {
    fn from(legacy: ConsoleSettingsV1) -> Self {
        let mut settings = Self::default();
        settings.appearance.ui_font_size = legacy.ui_font_size;
        settings.appearance.data_font_size = legacy.data_font_size;
        settings.appearance.density = legacy.density;
        settings.appearance.hide_empty_catalog = legacy.hide_empty_catalog;
        settings.editor.font_size = legacy.editor_font_size;
        settings.files.reopen_last_project = legacy.reopen_last_project;
        settings
    }
}

impl ConsoleSettingsV2 {
    fn validate(&self) -> Result<(), DbError> {
        if self.format_version != SETTINGS_VERSION {
            return Err(unsupported_version("console settings", self.format_version));
        }
        validate_choice(
            &self.appearance.theme,
            &["system", "light", "dark"],
            "theme",
        )?;
        if !(80..=150).contains(&self.appearance.zoom_percent)
            || !(9..=16).contains(&self.appearance.ui_font_size)
            || !(10..=18).contains(&self.appearance.data_font_size)
        {
            return Err(invalid("appearance values are outside the supported range"));
        }
        validate_choice(
            &self.appearance.density,
            &["compact", "comfortable"],
            "density",
        )?;
        validate_text(&self.editor.font_family, 1, 128, "editor font family")?;
        if !(10..=24).contains(&self.editor.font_size) || !(1..=8).contains(&self.editor.tab_size) {
            return Err(invalid("editor values are outside the supported range"));
        }
        validate_choice(
            &self.editor.word_wrap,
            &["off", "on", "bounded"],
            "editor word wrap",
        )?;
        validate_choice(
            &self.files.recovery_policy,
            &["prompt", "never", "automatic"],
            "recovery policy",
        )?;
        validate_choice(
            &self.files.auto_save,
            &["off", "afterDelay", "onFocusChange"],
            "auto save",
        )?;
        if !(250..=60_000).contains(&self.files.auto_save_delay_ms) {
            return Err(invalid("auto-save delay is outside the supported range"));
        }
        if !(50..=1_000).contains(&self.results.page_size)
            || !(100..=100_000).contains(&self.results.resident_row_limit)
            || !(1024 * 1024..=64 * 1024 * 1024).contains(&self.results.resident_memory_bytes)
            || !(1_000..=600_000).contains(&self.results.query_timeout_ms)
        {
            return Err(invalid("result settings are outside the supported range"));
        }
        validate_text(&self.results.null_display, 1, 32, "NULL display")?;
        if !(1_000..=120_000).contains(&self.connections.timeout_ms) {
            return Err(invalid("connection timeout is outside the supported range"));
        }
        validate_choice(
            &self.ai.provider,
            &["openai", "openaiCompatible", "ollama"],
            "AI provider",
        )?;
        validate_text(&self.ai.model, 1, 128, "AI model")?;
        if let Some(endpoint) = &self.ai.endpoint {
            validate_text(endpoint, 1, 2_048, "AI endpoint")?;
        }
        validate_choice(
            &self.ai.reasoning,
            &["low", "medium", "high"],
            "AI reasoning",
        )?;
        validate_choice(
            &self.ai.data_sharing,
            &["schemaOnly", "askEachTime", "allowSamples"],
            "AI data sharing",
        )?;
        if let Some(credential_id) = &self.ai.credential_id {
            validate_id(credential_id, "AI credential ID")?;
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
    locator: DocumentLocator,
    path: String,
    name: String,
    content: String,
    revision: FileRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DocumentLocator {
    Workspace { root_path: String, path: String },
    External { path: String },
    Untitled { id: String },
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
    #[serde(default)]
    locator: Option<DocumentLocator>,
    #[serde(default)]
    name: Option<String>,
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
pub struct RecentFileEntry {
    locator: DocumentLocator,
    name: String,
    opened_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecentFilesV1 {
    format_version: u32,
    entries: Vec<RecentFileEntry>,
}

impl Default for RecentFilesV1 {
    fn default() -> Self {
        Self {
            format_version: 1,
            entries: Vec::new(),
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
            format_version: 1,
            profiles: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DataSourceKind {
    OrdadbNative,
    Postgresql,
    Mysql,
    Sqlite,
    SqlServer,
    Mongodb,
    Redis,
    Mariadb,
    Clickhouse,
    Oracle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorKind {
    Sql,
    Document,
    KeyValue,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CredentialAccess {
    #[default]
    Unspecified,
    ReadOnly,
    ReadWrite,
}

impl CredentialAccess {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::ReadOnly => "readOnly",
            Self::ReadWrite => "readWrite",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConnectionProfileV2 {
    format_version: u32,
    profile_id: String,
    label: String,
    data_source_kind: DataSourceKind,
    connector_id: String,
    dialect: String,
    endpoint: String,
    admin_endpoint: Option<String>,
    database: Option<String>,
    tls_mode: String,
    credential_id: String,
    auto_reconnect: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConnectionProfilesV2 {
    format_version: u32,
    profiles: Vec<ConnectionProfileV2>,
}

impl Default for ConnectionProfilesV2 {
    fn default() -> Self {
        Self {
            format_version: 2,
            profiles: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectionProfileV3 {
    format_version: u32,
    profile_id: String,
    label: String,
    data_source_kind: DataSourceKind,
    connector_id: String,
    connector_kind: ConnectorKind,
    command_language: String,
    dialect: Option<String>,
    endpoint: String,
    admin_endpoint: Option<String>,
    database: Option<String>,
    tls_mode: String,
    credential_id: String,
    #[serde(default)]
    credential_access: CredentialAccess,
    auto_reconnect: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConnectionProfilesV3 {
    format_version: u32,
    profiles: Vec<ConnectionProfileV3>,
}

impl Default for ConnectionProfilesV3 {
    fn default() -> Self {
        Self {
            format_version: PROFILES_VERSION,
            profiles: Vec::new(),
        }
    }
}

impl From<ConnectionProfileV2> for ConnectionProfileV3 {
    fn from(legacy: ConnectionProfileV2) -> Self {
        let connector_id = migrate_connector_id(&legacy.connector_id);
        Self {
            format_version: PROFILES_VERSION,
            profile_id: legacy.profile_id,
            label: legacy.label,
            data_source_kind: data_source_kind(connector_id),
            connector_id: connector_id.to_owned(),
            connector_kind: ConnectorKind::Sql,
            command_language: command_language(connector_id).to_owned(),
            dialect: Some(legacy.dialect),
            endpoint: legacy.endpoint,
            admin_endpoint: legacy.admin_endpoint,
            database: legacy.database,
            tls_mode: legacy.tls_mode,
            credential_id: legacy.credential_id,
            credential_access: CredentialAccess::Unspecified,
            auto_reconnect: legacy.auto_reconnect,
        }
    }
}

impl From<ConnectionProfileV1> for ConnectionProfileV2 {
    fn from(legacy: ConnectionProfileV1) -> Self {
        let connector_id = migrate_connector_id(&legacy.connector_id);
        let data_source_kind = data_source_kind(connector_id);
        let tls_mode = if data_source_kind == DataSourceKind::OrdadbNative {
            "disable"
        } else {
            "prefer"
        };
        Self {
            format_version: 2,
            profile_id: legacy.profile_id,
            label: legacy.label,
            data_source_kind,
            connector_id: connector_id.to_owned(),
            dialect: legacy.dialect,
            endpoint: legacy.endpoint,
            admin_endpoint: legacy.admin_endpoint,
            database: legacy.database,
            tls_mode: tls_mode.to_owned(),
            credential_id: legacy.credential_id,
            auto_reconnect: legacy.auto_reconnect,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorDescriptor {
    data_source_kind: DataSourceKind,
    connector_id: &'static str,
    connector_kind: ConnectorKind,
    command_language: &'static str,
    editor_mode: &'static str,
    dialect: Option<&'static str>,
    display_name: &'static str,
    default_endpoint: &'static str,
    default_admin_endpoint: Option<&'static str>,
    default_database: Option<&'static str>,
    default_tls_mode: &'static str,
    logo_asset: &'static str,
}

fn connector_descriptors() -> Vec<ConnectorDescriptor> {
    vec![
        ConnectorDescriptor {
            data_source_kind: DataSourceKind::OrdadbNative,
            connector_id: NATIVE_CONNECTOR_ID,
            connector_kind: ConnectorKind::Sql,
            command_language: "postgresql-sql",
            editor_mode: "sql",
            dialect: Some("postgresql"),
            display_name: "OrdaDB",
            default_endpoint: "127.0.0.1:54329",
            default_admin_endpoint: Some("http://127.0.0.1:9080"),
            default_database: Some("ordadb"),
            default_tls_mode: "disable",
            logo_asset: "ordadb",
        },
        ConnectorDescriptor {
            data_source_kind: DataSourceKind::Postgresql,
            connector_id: "postgresql",
            connector_kind: ConnectorKind::Sql,
            command_language: "postgresql-sql",
            editor_mode: "sql",
            dialect: Some("postgresql"),
            display_name: "PostgreSQL",
            default_endpoint: "127.0.0.1:5432",
            default_admin_endpoint: None,
            default_database: Some("postgres"),
            default_tls_mode: "prefer",
            logo_asset: "postgresql",
        },
        ConnectorDescriptor {
            data_source_kind: DataSourceKind::Mysql,
            connector_id: "mysql",
            connector_kind: ConnectorKind::Sql,
            command_language: "mysql-sql",
            editor_mode: "sql",
            dialect: Some("mysql"),
            display_name: "MySQL",
            default_endpoint: "127.0.0.1:3306",
            default_admin_endpoint: None,
            default_database: None,
            default_tls_mode: "prefer",
            logo_asset: "mysql",
        },
        ConnectorDescriptor {
            data_source_kind: DataSourceKind::Sqlite,
            connector_id: "sqlite",
            connector_kind: ConnectorKind::Sql,
            command_language: "sqlite-sql",
            editor_mode: "sql",
            dialect: Some("sqlite"),
            display_name: "SQLite",
            default_endpoint: "",
            default_admin_endpoint: None,
            default_database: None,
            default_tls_mode: "disable",
            logo_asset: "sqlite",
        },
        ConnectorDescriptor {
            data_source_kind: DataSourceKind::SqlServer,
            connector_id: "sql-server",
            connector_kind: ConnectorKind::Sql,
            command_language: "sql-server-sql",
            editor_mode: "sql",
            dialect: Some("sqlServer"),
            display_name: "SQL Server",
            default_endpoint: "127.0.0.1:1433",
            default_admin_endpoint: None,
            default_database: None,
            default_tls_mode: "require",
            logo_asset: "sql-server",
        },
        ConnectorDescriptor {
            data_source_kind: DataSourceKind::Mongodb,
            connector_id: "mongodb",
            connector_kind: ConnectorKind::Document,
            command_language: "mongodb-json",
            editor_mode: "json",
            dialect: None,
            display_name: "MongoDB",
            default_endpoint: "127.0.0.1:27017",
            default_admin_endpoint: None,
            default_database: Some("admin"),
            default_tls_mode: "prefer",
            logo_asset: "mongodb",
        },
        ConnectorDescriptor {
            data_source_kind: DataSourceKind::Redis,
            connector_id: "redis",
            connector_kind: ConnectorKind::KeyValue,
            command_language: "redis-resp3",
            editor_mode: "plaintext",
            dialect: None,
            display_name: "Redis",
            default_endpoint: "127.0.0.1:6379",
            default_admin_endpoint: None,
            default_database: Some("0"),
            default_tls_mode: "disable",
            logo_asset: "redis",
        },
        ConnectorDescriptor {
            data_source_kind: DataSourceKind::Mariadb,
            connector_id: "mariadb",
            connector_kind: ConnectorKind::Sql,
            command_language: "mariadb-sql",
            editor_mode: "sql",
            dialect: Some("mariadb"),
            display_name: "MariaDB",
            default_endpoint: "127.0.0.1:3306",
            default_admin_endpoint: None,
            default_database: None,
            default_tls_mode: "require",
            logo_asset: "mariadb",
        },
        ConnectorDescriptor {
            data_source_kind: DataSourceKind::Clickhouse,
            connector_id: "clickhouse",
            connector_kind: ConnectorKind::Sql,
            command_language: "clickhouse-sql",
            editor_mode: "sql",
            dialect: Some("clickhouse"),
            display_name: "ClickHouse",
            default_endpoint: "127.0.0.1:8123",
            default_admin_endpoint: None,
            default_database: Some("default"),
            default_tls_mode: "disable",
            logo_asset: "clickhouse",
        },
        ConnectorDescriptor {
            data_source_kind: DataSourceKind::Oracle,
            connector_id: "oracle",
            connector_kind: ConnectorKind::Sql,
            command_language: "oracle-sql",
            editor_mode: "sql",
            dialect: Some("oracle"),
            display_name: "Oracle",
            default_endpoint: "127.0.0.1:1521",
            default_admin_endpoint: None,
            default_database: Some("ORCLPDB1"),
            default_tls_mode: "disable",
            logo_asset: "oracle",
        },
    ]
}

impl From<ConnectionProfileV1> for ConnectionProfileV3 {
    fn from(legacy: ConnectionProfileV1) -> Self {
        Self::from(ConnectionProfileV2::from(legacy))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsoleBootstrap {
    settings: ConsoleSettingsV2,
    recovery: Option<WorkspaceSessionV1>,
    recent_files: Vec<RecentFileEntry>,
    connection_profiles: Vec<ConnectionProfileV3>,
    connector_descriptors: Vec<ConnectorDescriptor>,
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
pub struct ExternalDocumentRequest {
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SaveExternalDocumentRequest {
    path: String,
    content: String,
    expected_revision: Option<FileRevision>,
    force: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SaveDocumentAsRequest {
    content: String,
    suggested_name: String,
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

fn validate_profile_v2(profile: &ConnectionProfileV2) -> Result<(), DbError> {
    if profile.format_version != 2 {
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
    validate_choice(
        &profile.tls_mode,
        &["disable", "prefer", "require", "verifyCa", "verifyFull"],
        "TLS mode",
    )?;
    if profile.data_source_kind != data_source_kind(&profile.connector_id) {
        return Err(invalid(
            "data source kind does not match the connector identity",
        ));
    }
    match profile.data_source_kind {
        DataSourceKind::OrdadbNative => {
            if profile.connector_id != NATIVE_CONNECTOR_ID
                || profile.dialect != "postgresql"
                || profile.admin_endpoint.is_none()
                || profile.tls_mode != "disable"
            {
                return Err(invalid("native OrdaDB profile fields are inconsistent"));
            }
        }
        DataSourceKind::Postgresql => {
            if profile.connector_id != "postgresql"
                || profile.dialect != "postgresql"
                || profile.admin_endpoint.is_some()
            {
                return Err(invalid(
                    "external PostgreSQL profile fields are inconsistent",
                ));
            }
        }
        DataSourceKind::Mysql => {
            if profile.connector_id != "mysql" || profile.dialect != "mysql" {
                return Err(invalid("MySQL profile fields are inconsistent"));
            }
        }
        DataSourceKind::Sqlite => {
            if profile.connector_id != "sqlite" || profile.dialect != "sqlite" {
                return Err(invalid("SQLite profile fields are inconsistent"));
            }
        }
        DataSourceKind::SqlServer => {
            if profile.connector_id != "sql-server" || profile.dialect != "sqlServer" {
                return Err(invalid("SQL Server profile fields are inconsistent"));
            }
        }
        DataSourceKind::Mongodb
        | DataSourceKind::Redis
        | DataSourceKind::Mariadb
        | DataSourceKind::Clickhouse
        | DataSourceKind::Oracle => {
            return Err(invalid(
                "connection profile v2 does not support this data source kind",
            ));
        }
    }
    Ok(())
}

fn validate_profiles_v3(document: &ConnectionProfilesV3) -> Result<(), DbError> {
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
    for profile in &document.profiles {
        validate_profile_v3(profile)?;
        if !ids.insert(profile.profile_id.as_str()) {
            return Err(invalid("connection profile IDs must be unique"));
        }
    }
    Ok(())
}

fn validate_profile_v3(profile: &ConnectionProfileV3) -> Result<(), DbError> {
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
    validate_text(
        &profile.command_language,
        1,
        64,
        "connector command language",
    )?;
    validate_text(&profile.endpoint, 1, 2_048, "database endpoint")?;
    if !matches!(
        profile.connector_id.as_str(),
        NATIVE_CONNECTOR_ID
            | "postgresql"
            | "mysql"
            | "sqlite"
            | "sql-server"
            | "mongodb"
            | "redis"
            | "mariadb"
            | "clickhouse"
            | "oracle"
    ) {
        return Err(invalid("connection profile has an unknown connector ID"));
    }
    if let Some(dialect) = &profile.dialect {
        validate_text(dialect, 1, 32, "SQL dialect")?;
    }
    if let Some(admin_endpoint) = &profile.admin_endpoint {
        validate_text(admin_endpoint, 1, 2_048, "administration endpoint")?;
    }
    if let Some(database) = &profile.database {
        validate_text(database, 1, 256, "database name")?;
    }
    validate_choice(
        &profile.tls_mode,
        &["disable", "prefer", "require", "verifyCa", "verifyFull"],
        "TLS mode",
    )?;
    if profile.data_source_kind != data_source_kind(&profile.connector_id)
        || profile.connector_kind != connector_kind(&profile.connector_id)
        || profile.command_language != command_language(&profile.connector_id)
    {
        return Err(invalid(
            "connection profile metadata does not match the connector identity",
        ));
    }
    let expected_dialect = connector_dialect(&profile.connector_id);
    if profile.dialect.as_deref() != expected_dialect {
        return Err(invalid(
            "connection profile SQL dialect does not match the connector identity",
        ));
    }
    if profile.connector_id == NATIVE_CONNECTOR_ID
        && (profile.admin_endpoint.is_none() || profile.tls_mode != "disable")
    {
        return Err(invalid("native OrdaDB profile fields are inconsistent"));
    }
    if profile.connector_id != NATIVE_CONNECTOR_ID && profile.admin_endpoint.is_some() {
        return Err(invalid(
            "external connector profiles do not accept an administration endpoint",
        ));
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

fn data_source_kind(connector_id: &str) -> DataSourceKind {
    match migrate_connector_id(connector_id) {
        NATIVE_CONNECTOR_ID => DataSourceKind::OrdadbNative,
        "postgresql" => DataSourceKind::Postgresql,
        "mysql" => DataSourceKind::Mysql,
        "sqlite" => DataSourceKind::Sqlite,
        "sql-server" => DataSourceKind::SqlServer,
        "mongodb" => DataSourceKind::Mongodb,
        "redis" => DataSourceKind::Redis,
        "mariadb" => DataSourceKind::Mariadb,
        "clickhouse" => DataSourceKind::Clickhouse,
        "oracle" => DataSourceKind::Oracle,
        _ => DataSourceKind::Postgresql,
    }
}

fn connector_kind(connector_id: &str) -> ConnectorKind {
    match connector_id {
        "mongodb" => ConnectorKind::Document,
        "redis" => ConnectorKind::KeyValue,
        _ => ConnectorKind::Sql,
    }
}

fn command_language(connector_id: &str) -> &'static str {
    match connector_id {
        NATIVE_CONNECTOR_ID | "postgresql" => "postgresql-sql",
        "mysql" => "mysql-sql",
        "sqlite" => "sqlite-sql",
        "sql-server" => "sql-server-sql",
        "mongodb" => "mongodb-json",
        "redis" => "redis-resp3",
        "mariadb" => "mariadb-sql",
        "clickhouse" => "clickhouse-sql",
        "oracle" => "oracle-sql",
        _ => "unknown",
    }
}

fn connector_dialect(connector_id: &str) -> Option<&'static str> {
    match connector_id {
        NATIVE_CONNECTOR_ID | "postgresql" => Some("postgresql"),
        "mysql" => Some("mysql"),
        "sqlite" => Some("sqlite"),
        "sql-server" => Some("sqlServer"),
        "mariadb" => Some("mariadb"),
        "clickhouse" => Some("clickhouse"),
        "oracle" => Some("oracle"),
        "mongodb" | "redis" => None,
        _ => None,
    }
}

fn validate_choice(value: &str, allowed: &[&str], field: &str) -> Result<(), DbError> {
    if allowed.contains(&value) {
        Ok(())
    } else {
        Err(invalid(format!("{field} is not supported")))
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

fn canonical_external_sql_file(value: &str) -> Result<PathBuf, DbError> {
    validate_absolute_path_text(value, "external SQL file")?;
    let path = fs::canonicalize(value)
        .map_err(|error| io_error("failed to open external SQL file", error))?;
    if !path.is_file() || !is_sql_path(&path) {
        return Err(invalid("external document must be an existing .sql file"));
    }
    Ok(path)
}

fn normalize_save_destination(selected_path: &Path) -> Result<PathBuf, DbError> {
    if !selected_path.is_absolute() {
        return Err(invalid("Save As destination must be an absolute path"));
    }
    let mut destination = selected_path.to_path_buf();
    if destination.extension().is_none() {
        destination.set_extension("sql");
    }
    if !is_sql_path(&destination) {
        return Err(invalid("Save As destination must use the .sql extension"));
    }
    let file_name = destination
        .file_name()
        .ok_or_else(|| invalid("Save As destination has no file name"))?
        .to_string_lossy()
        .into_owned();
    validate_file_name(&file_name)?;
    if destination.exists() {
        return canonical_external_sql_file(&destination.display().to_string());
    }
    let parent = destination
        .parent()
        .ok_or_else(|| invalid("Save As destination has no parent directory"))?;
    let parent = fs::canonicalize(parent)
        .map_err(|error| io_error("failed to resolve Save As directory", error))?;
    if !parent.is_dir() {
        return Err(invalid("Save As parent must be a directory"));
    }
    Ok(parent.join(file_name))
}

fn validate_absolute_path_text(value: &str, context: &str) -> Result<(), DbError> {
    validate_text(value, 1, 32_768, context)?;
    if !Path::new(value).is_absolute() {
        return Err(invalid(format!("{context} must be an absolute path")));
    }
    Ok(())
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

fn read_workspace_document(root: &Path, path: &Path) -> Result<SqlDocument, DbError> {
    if !path.is_file() || !is_sql_path(path) {
        return Err(invalid("workspace document must be an existing .sql file"));
    }
    let relative = relative_display_path(root, path)?;
    read_sql_document_at(
        path,
        DocumentLocator::Workspace {
            root_path: root.display().to_string(),
            path: relative.clone(),
        },
        relative,
    )
}

fn read_external_document(path: &Path) -> Result<SqlDocument, DbError> {
    let absolute = path.display().to_string();
    read_sql_document_at(
        path,
        DocumentLocator::External {
            path: absolute.clone(),
        },
        absolute,
    )
}

fn read_sql_document_at(
    path: &Path,
    locator: DocumentLocator,
    display_path: String,
) -> Result<SqlDocument, DbError> {
    if !path.is_file() || !is_sql_path(path) {
        return Err(invalid("SQL document must be an existing .sql file"));
    }
    let (bytes, revision) = read_sql_file_snapshot(path)?;
    let content = String::from_utf8(bytes)
        .map_err(|error| invalid("SQL file must be valid UTF-8").with_detail(error.to_string()))?;
    Ok(SqlDocument {
        locator,
        path: display_path,
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
                .appearance
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
                    locator: None,
                    name: None,
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
    fn external_documents_save_as_recent_files_and_untitled_recovery_are_bounded() {
        let directory = tempdir().expect("tempdir");
        let runtime = ConsoleRuntime::open(directory.path().join("state")).expect("runtime");
        let external = directory.path().join("outside.sql");
        fs::write(&external, "select 1;").expect("external SQL");

        let opened = runtime
            .open_external_document(&ExternalDocumentRequest {
                path: external.display().to_string(),
            })
            .expect("open external");
        assert!(matches!(
            &opened.locator,
            DocumentLocator::External { path } if Path::new(path).is_absolute()
        ));
        runtime
            .open_external_document(&ExternalDocumentRequest {
                path: external.display().to_string(),
            })
            .expect("reopen external");
        assert_eq!(
            runtime
                .load_recent_files()
                .expect("recent files")
                .entries
                .len(),
            1,
            "normalized absolute paths must be deduplicated"
        );

        fs::write(&external, "select 2;").expect("external edit");
        let error = runtime
            .save_external_document(&SaveExternalDocumentRequest {
                path: external.display().to_string(),
                content: "select 3;".into(),
                expected_revision: Some(opened.revision),
                force: false,
            })
            .expect_err("external conflict");
        assert_eq!(error.sql_state, "40001");

        let saved = runtime
            .save_document_as_path(
                &SaveDocumentAsRequest {
                    content: "select 42;".into(),
                    suggested_name: "query.sql".into(),
                },
                &directory.path().join("saved-query"),
            )
            .expect("Save As");
        assert_eq!(saved.name, "saved-query.sql");
        assert_eq!(
            fs::read_to_string(directory.path().join("saved-query.sql")).expect("saved SQL"),
            "select 42;"
        );

        runtime
            .save_session(WorkspaceSessionV1 {
                format_version: 1,
                root_path: None,
                active_path: Some("untitled:1".into()),
                open_documents: vec![WorkspaceDraft {
                    path: "untitled:1".into(),
                    locator: Some(DocumentLocator::Untitled {
                        id: "untitled-1".into(),
                    }),
                    name: Some("未命名-1.sql".into()),
                    content: "select now();".into(),
                    base_revision: None,
                }],
            })
            .expect("untitled recovery");
        assert!(runtime.bootstrap().expect("bootstrap").recovery.is_some());
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
            .save_profile(
                ConnectionProfileV1 {
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
                }
                .into(),
            )
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
            format_version: 1,
            profiles: vec![ConnectionProfileV1 {
                format_version: 1,
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
        write_json_atomic(&root.join(LEGACY_PROFILES_FILE), &document).expect("legacy document");

        let runtime = ConsoleRuntime::open(root).expect("runtime");
        let migrated = runtime.load_profiles().expect("migrated profiles");
        assert_eq!(migrated.profiles[0].connector_id, NATIVE_CONNECTOR_ID);
        assert_eq!(
            migrated.profiles[0].data_source_kind,
            DataSourceKind::OrdadbNative
        );
        assert_eq!(
            migrated.profiles[0].credential_id, "credential-reference",
            "the Credential Manager reference must survive ID migration"
        );
        assert_eq!(
            migrated.profiles[0].credential_access,
            CredentialAccess::Unspecified,
            "legacy credentials must never be assumed read-only"
        );
        let persisted =
            fs::read_to_string(runtime.root.join(PROFILES_FILE)).expect("persisted migration");
        assert!(persisted.contains("\"connectorId\": \"ordadb-native\""));
        assert!(persisted.contains("\"credentialAccess\": \"unspecified\""));
        assert!(!persisted.contains("ordadb-postgresql"));
    }

    #[test]
    fn legacy_settings_migrate_to_v2_without_deleting_the_source() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("state");
        fs::create_dir_all(&root).expect("state directory");
        let legacy = ConsoleSettingsV1 {
            format_version: 1,
            ui_font_size: 10,
            data_font_size: 13,
            editor_font_size: 14,
            density: "compact".into(),
            reopen_last_project: true,
            hide_empty_catalog: false,
        };
        write_json_atomic(&root.join(LEGACY_SETTINGS_FILE), &legacy).expect("legacy settings");

        let runtime = ConsoleRuntime::open(root).expect("runtime");
        let migrated = runtime.load_settings().expect("migrated settings");
        assert_eq!(migrated.format_version, 2);
        assert_eq!(migrated.appearance.ui_font_size, 10);
        assert_eq!(migrated.appearance.data_font_size, 13);
        assert_eq!(migrated.editor.font_size, 14);
        assert!(migrated.files.reopen_last_project);
        assert!(!migrated.appearance.hide_empty_catalog);
        assert!(runtime.root.join(LEGACY_SETTINGS_FILE).exists());
        assert!(runtime.root.join(SETTINGS_FILE).exists());
    }

    #[test]
    fn postgresql_profiles_cannot_carry_native_admin_fields() {
        let mut profile = ConnectionProfileV3 {
            format_version: PROFILES_VERSION,
            profile_id: "external-pg".into(),
            label: "PostgreSQL".into(),
            data_source_kind: DataSourceKind::Postgresql,
            connector_id: "postgresql".into(),
            connector_kind: ConnectorKind::Sql,
            command_language: "postgresql-sql".into(),
            dialect: Some("postgresql".into()),
            endpoint: "127.0.0.1:5432".into(),
            admin_endpoint: None,
            database: Some("postgres".into()),
            tls_mode: "prefer".into(),
            credential_id: "external-credential".into(),
            credential_access: CredentialAccess::ReadOnly,
            auto_reconnect: false,
        };
        validate_profile_v3(&profile).expect("valid external PostgreSQL");
        profile.admin_endpoint = Some("http://127.0.0.1:9080".into());
        let error = validate_profile_v3(&profile).expect_err("native admin endpoint rejected");
        assert_eq!(error.sql_state, "22023");
    }

    #[test]
    fn connector_descriptors_cover_ten_unique_native_data_models() {
        let descriptors = connector_descriptors();
        assert_eq!(descriptors.len(), 10);
        assert_eq!(
            descriptors
                .iter()
                .map(|descriptor| descriptor.connector_id)
                .collect::<BTreeSet<_>>()
                .len(),
            10
        );
        let mongodb = descriptors
            .iter()
            .find(|descriptor| descriptor.connector_id == "mongodb")
            .expect("MongoDB descriptor");
        assert_eq!(mongodb.connector_kind, ConnectorKind::Document);
        assert_eq!(mongodb.command_language, "mongodb-json");
        assert_eq!(mongodb.editor_mode, "json");
        assert_eq!(mongodb.dialect, None);
        let redis = descriptors
            .iter()
            .find(|descriptor| descriptor.connector_id == "redis")
            .expect("Redis descriptor");
        assert_eq!(redis.connector_kind, ConnectorKind::KeyValue);
        assert_eq!(redis.command_language, "redis-resp3");
        assert_eq!(redis.editor_mode, "plaintext");
        assert_eq!(redis.dialect, None);
    }

    #[test]
    fn profile_v3_rejects_unknown_and_sql_shaped_non_sql_connectors() {
        let profile = ConnectionProfileV3 {
            format_version: PROFILES_VERSION,
            profile_id: "mongodb-local".into(),
            label: "MongoDB".into(),
            data_source_kind: DataSourceKind::Mongodb,
            connector_id: "mongodb".into(),
            connector_kind: ConnectorKind::Document,
            command_language: "mongodb-json".into(),
            dialect: None,
            endpoint: "127.0.0.1:27017".into(),
            admin_endpoint: None,
            database: Some("admin".into()),
            tls_mode: "prefer".into(),
            credential_id: "mongodb-credential".into(),
            credential_access: CredentialAccess::Unspecified,
            auto_reconnect: false,
        };
        validate_profile_v3(&profile).expect("native MongoDB profile");

        let mut sql_shaped = profile.clone();
        sql_shaped.connector_kind = ConnectorKind::Sql;
        sql_shaped.command_language = "postgresql-sql".into();
        sql_shaped.dialect = Some("postgresql".into());
        assert_eq!(
            validate_profile_v3(&sql_shaped)
                .expect_err("MongoDB cannot be represented as SQL")
                .sql_state,
            "22023"
        );

        let mut unknown = profile;
        unknown.connector_id = "unknown-database".into();
        unknown.data_source_kind = DataSourceKind::Postgresql;
        unknown.connector_kind = ConnectorKind::Sql;
        unknown.command_language = "unknown".into();
        assert_eq!(
            validate_profile_v3(&unknown)
                .expect_err("unknown connector")
                .sql_state,
            "22023"
        );
    }
}
