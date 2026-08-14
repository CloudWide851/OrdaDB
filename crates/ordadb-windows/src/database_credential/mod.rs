use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ordadb_types::{DbError, Result};
use zeroize::Zeroizing;

use crate::{CredentialVault, StoredCredential};

mod acl;
mod crypto;
mod store;

const PRODUCT_DIRECTORY: &str = "com.ordadb.desktop";
const CREDENTIAL_DIRECTORY: &str = "credentials";
const DATABASE_FILE: &str = "credentials-v1.sqlite3";
const LEGACY_NAMESPACE: &str = "OrdaDB/Console";
const MAX_CREDENTIAL_ID_BYTES: usize = 128;
const MAX_USERNAME_BYTES: usize = 256;
const MAX_PASSWORD_BYTES: usize = 4_096;

trait LegacyCredentialBackend: Send + Sync {
    fn load(&self, credential_id: &str) -> Result<StoredCredential>;
    fn delete(&self, credential_id: &str) -> Result<()>;
}

impl LegacyCredentialBackend for CredentialVault {
    fn load(&self, credential_id: &str) -> Result<StoredCredential> {
        self.load(credential_id)
    }

    fn delete(&self, credential_id: &str) -> Result<()> {
        self.delete(credential_id)
    }
}

#[derive(Clone)]
pub struct DatabaseCredentialStore {
    path: PathBuf,
    legacy: Arc<dyn LegacyCredentialBackend>,
}

impl Debug for DatabaseCredentialStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DatabaseCredentialStore")
            .field("path", &"<redacted>")
            .field("legacy", &"<redacted>")
            .finish()
    }
}

impl DatabaseCredentialStore {
    pub fn open() -> Result<Self> {
        let local_app_data = std::env::var_os("LOCALAPPDATA")
            .ok_or_else(|| io_error("LOCALAPPDATA is unavailable"))?;
        Self::open_path(
            PathBuf::from(local_app_data)
                .join(PRODUCT_DIRECTORY)
                .join(CREDENTIAL_DIRECTORY)
                .join(DATABASE_FILE),
        )
    }

    pub fn open_path(path: PathBuf) -> Result<Self> {
        Self::open_with_legacy(path, Arc::new(CredentialVault::new(LEGACY_NAMESPACE)?))
    }

    fn open_with_legacy(path: PathBuf, legacy: Arc<dyn LegacyCredentialBackend>) -> Result<Self> {
        let store = Self { path, legacy };
        store.initialize()?;
        Ok(store)
    }

    pub fn store(
        &self,
        credential_id: &str,
        username: &str,
        password: &Zeroizing<String>,
    ) -> Result<()> {
        store::store(self, credential_id, username, password)
    }

    pub fn load(&self, credential_id: &str) -> Result<StoredCredential> {
        store::load(self, credential_id)
    }

    pub fn delete(&self, credential_id: &str) -> Result<()> {
        store::delete(self, credential_id)
    }

    fn initialize(&self) -> Result<()> {
        store::initialize(self)
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

fn validate_credential_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_CREDENTIAL_ID_BYTES
        || value.starts_with('.')
        || value.ends_with('.')
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(invalid(
            "credential ID must use 1-128 ASCII letters, digits, dots, hyphens, or underscores",
        ));
    }
    Ok(())
}

fn validate_secret_text(value: &str, maximum: usize, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(invalid(format!(
            "{label} is outside its credential-store bounds"
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn io_error(message: impl Into<String>) -> DbError {
    DbError::new("58030", message)
}

fn not_found() -> DbError {
    DbError::new("42704", "database credential does not exist")
}

fn sqlite_error(context: &'static str, _error: rusqlite::Error) -> DbError {
    DbError::new("58030", context)
}

#[cfg(test)]
mod tests;
