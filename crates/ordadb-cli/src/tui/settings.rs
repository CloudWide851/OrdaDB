use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use ordadb_ai::{
    AiAuditEntry, AiDataSharingPolicy, AiHistoryEntry, AiPersistenceV1, AiProviderKind,
    AiProviderSettings, AiReasoningEffort, DEFAULT_OPENAI_MODEL, MAX_PERSISTED_STATE_BYTES,
    decode_persistence, project_persistence, validate_provider_settings,
};
use ordadb_types::{DbError, Result};
use ordadb_windows::write_file_atomic;
use serde::{Deserialize, Serialize};

const SETTINGS_VERSION: u32 = 1;
const MAX_SETTINGS_BYTES: usize = 2 * 1024 * 1024;
const PRODUCT_DIRECTORY: &str = "com.ordadb.desktop";
const DEFAULT_DATABASE_CREDENTIAL_ID: &str = "ordadb-local";
const DEFAULT_AI_CREDENTIAL_ID: &str = "provider-openAi-default";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TuiSettingsV1 {
    pub version: u32,
    pub connection: NativeConnectionSettings,
    pub provider: AiProviderSettings,
    pub ui: TuiUiSettings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeConnectionSettings {
    pub address: String,
    pub user: String,
    pub database: String,
    pub credential_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TuiUiSettings {
    pub history_limit: usize,
    pub transcript_limit: usize,
    pub reduce_motion: bool,
}

impl Default for TuiSettingsV1 {
    fn default() -> Self {
        Self {
            version: SETTINGS_VERSION,
            connection: NativeConnectionSettings {
                address: "127.0.0.1:54329".to_owned(),
                user: "dba".to_owned(),
                database: "ordadb".to_owned(),
                credential_id: DEFAULT_DATABASE_CREDENTIAL_ID.to_owned(),
            },
            provider: AiProviderSettings {
                kind: AiProviderKind::OpenAi,
                model: DEFAULT_OPENAI_MODEL.to_owned(),
                endpoint: None,
                reasoning: AiReasoningEffort::Medium,
                data_sharing: AiDataSharingPolicy::SchemaOnly,
                credential_id: Some(DEFAULT_AI_CREDENTIAL_ID.to_owned()),
            },
            ui: TuiUiSettings {
                history_limit: 64,
                transcript_limit: 128,
                reduce_motion: false,
            },
        }
    }
}

impl TuiSettingsV1 {
    pub fn validate(&self) -> Result<()> {
        if self.version != SETTINGS_VERSION {
            return Err(invalid("unsupported TUI settings version"));
        }
        self.connection
            .address
            .parse::<SocketAddr>()
            .map_err(|_| invalid("TUI native address must be an IP socket address"))?;
        validate_text(&self.connection.user, 1, 256, "TUI database user")?;
        validate_text(&self.connection.database, 1, 256, "TUI database name")?;
        validate_text(
            &self.connection.credential_id,
            1,
            256,
            "TUI database credential ID",
        )?;
        validate_provider_settings(&self.provider)?;
        if !(1..=256).contains(&self.ui.history_limit) {
            return Err(invalid("TUI history limit must be between 1 and 256"));
        }
        if !(16..=512).contains(&self.ui.transcript_limit) {
            return Err(invalid("TUI transcript limit must be between 16 and 512"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TuiStateStore {
    root: PathBuf,
}

impl TuiStateStore {
    pub fn discover() -> Result<Self> {
        let local_app_data = std::env::var_os("LOCALAPPDATA")
            .ok_or_else(|| DbError::new("58030", "LOCALAPPDATA is unavailable"))?;
        Ok(Self::new(
            PathBuf::from(local_app_data)
                .join(PRODUCT_DIRECTORY)
                .join("tui"),
        ))
    }

    pub const fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn load_settings(&self) -> Result<TuiSettingsV1> {
        let path = self.settings_path();
        if !path.exists() {
            return Ok(TuiSettingsV1::default());
        }
        let bytes = read_bounded(&path, MAX_SETTINGS_BYTES, "TUI settings")?;
        let settings: TuiSettingsV1 = serde_json::from_slice(&bytes).map_err(|error| {
            invalid("TUI settings JSON is invalid").with_detail(error.to_string())
        })?;
        settings.validate()?;
        Ok(settings)
    }

    pub fn save_settings(&self, settings: &TuiSettingsV1) -> Result<()> {
        settings.validate()?;
        let bytes = serde_json::to_vec_pretty(settings).map_err(|error| {
            DbError::internal("failed to encode TUI settings").with_detail(error.to_string())
        })?;
        if bytes.len() > MAX_SETTINGS_BYTES {
            return Err(limit("TUI settings exceed the 2 MiB limit"));
        }
        write_file_atomic(&self.settings_path(), &bytes)
    }

    pub fn load_persistence(&self) -> Result<AiPersistenceV1> {
        let path = self.persistence_path();
        if !path.exists() {
            return project_persistence(Vec::new(), Vec::new());
        }
        decode_persistence(&read_bounded(
            &path,
            MAX_PERSISTED_STATE_BYTES,
            "TUI visible history",
        )?)
    }

    pub fn save_persistence(
        &self,
        history: impl IntoIterator<Item = AiHistoryEntry>,
        audit: impl IntoIterator<Item = AiAuditEntry>,
    ) -> Result<AiPersistenceV1> {
        let projected = project_persistence(history, audit)?;
        let bytes = serde_json::to_vec_pretty(&projected).map_err(|error| {
            DbError::internal("failed to encode TUI visible history").with_detail(error.to_string())
        })?;
        if bytes.len() > MAX_PERSISTED_STATE_BYTES {
            return Err(limit("TUI visible history exceeds the shared AI limit"));
        }
        write_file_atomic(&self.persistence_path(), &bytes)?;
        Ok(projected)
    }

    fn settings_path(&self) -> PathBuf {
        self.root.join("settings-v1.json")
    }

    fn persistence_path(&self) -> PathBuf {
        self.root.join("persistence-v1.json")
    }
}

fn read_bounded(path: &Path, maximum: usize, label: &str) -> Result<Vec<u8>> {
    let metadata = fs::metadata(path)
        .map_err(|error| io_error(&format!("failed to inspect {label}"), error))?;
    if metadata.len() > maximum as u64 {
        return Err(limit(format!("{label} exceeds its byte limit")));
    }
    let bytes =
        fs::read(path).map_err(|error| io_error(&format!("failed to read {label}"), error))?;
    if bytes.len() > maximum {
        return Err(limit(format!("{label} exceeds its byte limit")));
    }
    Ok(bytes)
}

fn validate_text(value: &str, minimum: usize, maximum: usize, label: &str) -> Result<()> {
    if value.len() < minimum
        || value.len() > maximum
        || value.as_bytes().contains(&0)
        || value.chars().any(char::is_control)
    {
        return Err(invalid(format!("{label} is invalid")));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn limit(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_round_trip_contains_only_credential_references() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = TuiStateStore::new(directory.path().to_path_buf());
        let settings = TuiSettingsV1::default();
        store.save_settings(&settings).expect("save");

        assert_eq!(store.load_settings().expect("load"), settings);
        let raw = fs::read_to_string(store.settings_path()).expect("raw settings");
        assert!(!raw.contains("apiKey"));
        assert!(!raw.contains("password"));
        assert!(raw.contains("credentialId"));
    }

    #[test]
    fn oversized_and_unknown_version_settings_fail_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = TuiStateStore::new(directory.path().to_path_buf());
        fs::create_dir_all(directory.path()).expect("create");
        fs::write(store.settings_path(), vec![b'x'; MAX_SETTINGS_BYTES + 1]).expect("large");
        assert_eq!(store.load_settings().expect_err("large").sql_state, "54000");

        let settings = TuiSettingsV1 {
            version: 2,
            ..TuiSettingsV1::default()
        };
        fs::write(
            store.settings_path(),
            serde_json::to_vec(&settings).expect("encode"),
        )
        .expect("write");
        assert_eq!(
            store.load_settings().expect_err("version").sql_state,
            "22023"
        );
    }
}
