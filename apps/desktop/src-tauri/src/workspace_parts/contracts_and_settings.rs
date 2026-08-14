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
