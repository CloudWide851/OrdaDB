use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;
use zeroize::Zeroizing;

use ordadb_types::{DbError, Result};

use crate::rbac::{Action, DbObject, Grant, Role};

pub const AUTH_FORMAT_VERSION: u32 = 1;
const AUTH_FILE_NAME: &str = "ordadb.auth.json";
const MAX_AUTH_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MIN_PASSWORD_BYTES: usize = 12;
const MAX_PASSWORD_BYTES: usize = 1024;
const SCRAM_ITERATIONS: u32 = 4096;
const SCRAM_KEY_BYTES: usize = 32;
const TOKEN_BYTES: usize = 32;
const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScramVerifier {
    iterations: u32,
    salt: String,
    stored_key: String,
    server_key: String,
}

impl fmt::Debug for ScramVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramVerifier")
            .field("iterations", &self.iterations)
            .field("salt", &"<redacted>")
            .field("stored_key", &"<redacted>")
            .field("server_key", &"<redacted>")
            .finish()
    }
}

impl ScramVerifier {
    pub fn derive(password: &[u8]) -> Result<Self> {
        validate_password(password)?;
        let mut salt = [0_u8; 16];
        OsRng.fill_bytes(&mut salt);
        let mut salted_password = Zeroizing::new([0_u8; SCRAM_KEY_BYTES]);
        pbkdf2_hmac::<Sha256>(password, &salt, SCRAM_ITERATIONS, salted_password.as_mut());
        let client_key = hmac_sha256(&salted_password[..], b"Client Key");
        let stored_key: [u8; SCRAM_KEY_BYTES] = Sha256::digest(client_key).into();
        let server_key = hmac_sha256(&salted_password[..], b"Server Key");
        Ok(Self {
            iterations: SCRAM_ITERATIONS,
            salt: STANDARD.encode(salt),
            stored_key: STANDARD.encode(stored_key),
            server_key: STANDARD.encode(server_key),
        })
    }

    #[must_use]
    pub const fn iterations(&self) -> u32 {
        self.iterations
    }

    pub fn salt(&self) -> Result<Vec<u8>> {
        decode_fixed_or_variable(&self.salt, "SCRAM salt", None)
    }

    pub fn stored_key(&self) -> Result<[u8; SCRAM_KEY_BYTES]> {
        decode_key(&self.stored_key, "SCRAM stored key")
    }

    pub fn server_key(&self) -> Result<[u8; SCRAM_KEY_BYTES]> {
        decode_key(&self.server_key, "SCRAM server key")
    }

    pub fn verify_client_proof(&self, auth_message: &[u8], client_proof: &[u8]) -> Result<bool> {
        if client_proof.len() != SCRAM_KEY_BYTES {
            return Ok(false);
        }
        let stored_key = self.stored_key()?;
        let client_signature = hmac_sha256(&stored_key, auth_message);
        let mut client_key = [0_u8; SCRAM_KEY_BYTES];
        for (target, (proof, signature)) in client_key
            .iter_mut()
            .zip(client_proof.iter().zip(client_signature))
        {
            *target = proof ^ signature;
        }
        let candidate: [u8; SCRAM_KEY_BYTES] = Sha256::digest(client_key).into();
        Ok(bool::from(candidate.ct_eq(&stored_key)))
    }

    pub fn server_signature(&self, auth_message: &[u8]) -> Result<[u8; SCRAM_KEY_BYTES]> {
        Ok(hmac_sha256(&self.server_key()?, auth_message))
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserRecord {
    name: String,
    enabled: bool,
    scram: ScramVerifier,
    argon2id: String,
    roles: BTreeSet<String>,
}

impl fmt::Debug for UserRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UserRecord")
            .field("name", &self.name)
            .field("enabled", &self.enabled)
            .field("scram", &self.scram)
            .field("argon2id", &"<redacted>")
            .field("roles", &self.roles)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthDocument {
    format_version: u32,
    users: BTreeMap<String, UserRecord>,
    roles: BTreeMap<String, Role>,
    grants: Vec<Grant>,
}

impl Default for AuthDocument {
    fn default() -> Self {
        Self {
            format_version: AUTH_FORMAT_VERSION,
            users: BTreeMap::new(),
            roles: BTreeMap::new(),
            grants: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub user: String,
    pub roles: BTreeSet<String>,
}

pub struct AuthStore {
    path: PathBuf,
    write_lock: Mutex<()>,
    document: RwLock<AuthDocument>,
}

impl fmt::Debug for AuthStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthStore")
            .field("path", &self.path)
            .field("document", &"<redacted>")
            .finish()
    }
}

impl AuthStore {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir)
            .map_err(|error| io_error("failed to create authentication directory", error))?;
        let path = data_dir.join(AUTH_FILE_NAME);
        let document = if path.exists() {
            read_document(&path)?
        } else {
            AuthDocument::default()
        };
        validate_document(&document)?;
        Ok(Self {
            path,
            write_lock: Mutex::new(()),
            document: RwLock::new(document),
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn has_users(&self) -> Result<bool> {
        Ok(!self.read_document()?.users.is_empty())
    }

    pub fn bootstrap_admin(&self, username: &str, password: &[u8]) -> Result<Principal> {
        let username = normalize_name(username, "user")?;
        validate_password(password)?;
        let _write = self
            .write_lock
            .lock()
            .map_err(|_| internal("authentication write lock is poisoned"))?;
        let current = self.read_document()?.clone();
        if !current.users.is_empty() {
            return Err(DbError::new(
                "55000",
                "the administrator bootstrap channel is already closed",
            )
            .with_hint("authenticate as an existing administrator"));
        }
        let mut candidate = current;
        let role_name = "ordadb_admin".to_owned();
        candidate.roles.insert(
            role_name.clone(),
            Role {
                name: role_name.clone(),
                inherits: BTreeSet::new(),
            },
        );
        candidate.grants.push(Grant {
            role: role_name.clone(),
            action: Action::Manage,
            object: DbObject::Server,
        });
        candidate.users.insert(
            username.clone(),
            UserRecord {
                name: username.clone(),
                enabled: true,
                scram: ScramVerifier::derive(password)?,
                argon2id: hash_argon2(password)?,
                roles: BTreeSet::from([role_name.clone()]),
            },
        );
        validate_document(&candidate)?;
        persist_document(&self.path, &candidate)?;
        *self.write_document()? = candidate;
        Ok(Principal {
            user: username,
            roles: BTreeSet::from([role_name]),
        })
    }

    pub fn authenticate_password(&self, username: &str, password: &[u8]) -> Result<Principal> {
        let username = normalize_name(username, "user")?;
        if password.len() > MAX_PASSWORD_BYTES {
            return Err(authentication_failed());
        }
        let document = self.read_document()?;
        let Some(user) = document.users.get(&username) else {
            consume_fake_argon2(password);
            return Err(authentication_failed());
        };
        if !user.enabled {
            consume_fake_argon2(password);
            return Err(authentication_failed());
        }
        let parsed =
            PasswordHash::new(&user.argon2id).map_err(|_| corrupt("invalid Argon2id verifier"))?;
        if Argon2::default()
            .verify_password(password, &parsed)
            .is_err()
        {
            return Err(authentication_failed());
        }
        Ok(Principal {
            user: user.name.clone(),
            roles: user.roles.clone(),
        })
    }

    pub fn scram_verifier(&self, username: &str) -> Result<Option<(Principal, ScramVerifier)>> {
        let username = normalize_name(username, "user")?;
        let document = self.read_document()?;
        Ok(document.users.get(&username).and_then(|user| {
            user.enabled.then(|| {
                (
                    Principal {
                        user: user.name.clone(),
                        roles: user.roles.clone(),
                    },
                    user.scram.clone(),
                )
            })
        }))
    }

    pub fn authorization_snapshot(&self) -> Result<(BTreeMap<String, Role>, Vec<Grant>)> {
        let document = self.read_document()?;
        Ok((document.roles.clone(), document.grants.clone()))
    }

    fn read_document(&self) -> Result<std::sync::RwLockReadGuard<'_, AuthDocument>> {
        self.document
            .read()
            .map_err(|_| internal("authentication store lock is poisoned"))
    }

    fn write_document(&self) -> Result<std::sync::RwLockWriteGuard<'_, AuthDocument>> {
        self.document
            .write()
            .map_err(|_| internal("authentication store lock is poisoned"))
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: &'static str,
    pub expires_in_seconds: u64,
}

struct TokenRecord {
    principal: Principal,
    expires_at: Instant,
}

pub struct TokenStore {
    records: Mutex<BTreeMap<[u8; SCRAM_KEY_BYTES], TokenRecord>>,
    ttl: Duration,
}

impl fmt::Debug for TokenStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenStore")
            .field("records", &"<redacted>")
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl Default for TokenStore {
    fn default() -> Self {
        Self::new(DEFAULT_TOKEN_TTL)
    }
}

impl TokenStore {
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            records: Mutex::new(BTreeMap::new()),
            ttl,
        }
    }

    pub fn issue(
        &self,
        auth: &AuthStore,
        username: &str,
        password: &[u8],
    ) -> Result<TokenResponse> {
        let principal = auth.authenticate_password(username, password)?;
        let mut token = Zeroizing::new([0_u8; TOKEN_BYTES]);
        OsRng.fill_bytes(token.as_mut());
        let encoded = URL_SAFE_NO_PAD.encode(&token[..]);
        let digest: [u8; SCRAM_KEY_BYTES] = Sha256::digest(&token[..]).into();
        let expires_at = Instant::now()
            .checked_add(self.ttl)
            .ok_or_else(|| internal("token expiry overflowed"))?;
        let mut records = self
            .records
            .lock()
            .map_err(|_| internal("token store lock is poisoned"))?;
        prune_tokens(&mut records);
        records.insert(
            digest,
            TokenRecord {
                principal,
                expires_at,
            },
        );
        Ok(TokenResponse {
            access_token: encoded,
            token_type: "Bearer",
            expires_in_seconds: self.ttl.as_secs(),
        })
    }

    pub fn authenticate(&self, token: &str) -> Result<Principal> {
        let decoded = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| authentication_failed())?;
        if decoded.len() != TOKEN_BYTES {
            return Err(authentication_failed());
        }
        let digest: [u8; SCRAM_KEY_BYTES] = Sha256::digest(decoded).into();
        let mut records = self
            .records
            .lock()
            .map_err(|_| internal("token store lock is poisoned"))?;
        prune_tokens(&mut records);
        records
            .get(&digest)
            .map(|record| record.principal.clone())
            .ok_or_else(authentication_failed)
    }
}

fn prune_tokens(records: &mut BTreeMap<[u8; SCRAM_KEY_BYTES], TokenRecord>) {
    let now = Instant::now();
    records.retain(|_, record| record.expires_at > now);
}

fn hash_argon2(password: &[u8]) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let params =
        Params::new(19_456, 2, 1, None).map_err(|_| internal("invalid Argon2id parameters"))?;
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password(password, &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| internal("failed to derive Argon2id verifier"))
}

fn consume_fake_argon2(password: &[u8]) {
    let salt = SaltString::from_b64("b3JkYWRiLWZha2Utc2FsdA").expect("static fake salt");
    let _ = Argon2::default().hash_password(password, &salt);
}

fn validate_password(password: &[u8]) -> Result<()> {
    if !(MIN_PASSWORD_BYTES..=MAX_PASSWORD_BYTES).contains(&password.len()) {
        return Err(DbError::new(
            "22023",
            format!(
                "password length must be between {MIN_PASSWORD_BYTES} and {MAX_PASSWORD_BYTES} bytes"
            ),
        ));
    }
    if std::str::from_utf8(password).is_err() {
        return Err(DbError::new("22021", "password must be valid UTF-8"));
    }
    Ok(())
}

fn normalize_name(value: &str, kind: &str) -> Result<String> {
    let value = value.trim().to_ascii_lowercase();
    if value.is_empty()
        || value.len() > 63
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(DbError::new(
            "22023",
            format!("{kind} name must be 1-63 ASCII letters, digits, '.', '-' or '_'"),
        ));
    }
    Ok(value)
}

fn validate_document(document: &AuthDocument) -> Result<()> {
    if document.format_version != AUTH_FORMAT_VERSION {
        return Err(DbError::new(
            "0A000",
            format!(
                "authentication format version {} is unsupported",
                document.format_version
            ),
        )
        .with_hint("back up the authentication catalog and run an explicit migration"));
    }
    for (key, user) in &document.users {
        if normalize_name(key, "user")? != *key || user.name != *key {
            return Err(corrupt("authentication user key/name mismatch"));
        }
        let _ = user.scram.salt()?;
        let _ = user.scram.stored_key()?;
        let _ = user.scram.server_key()?;
        PasswordHash::new(&user.argon2id)
            .map_err(|_| corrupt("invalid persisted Argon2id verifier"))?;
        for role in &user.roles {
            if !document.roles.contains_key(role) {
                return Err(corrupt(format!("user references unknown role {role}")));
            }
        }
    }
    for (key, role) in &document.roles {
        if normalize_name(key, "role")? != *key || role.name != *key {
            return Err(corrupt("authentication role key/name mismatch"));
        }
        if role
            .inherits
            .iter()
            .any(|parent| !document.roles.contains_key(parent))
        {
            return Err(corrupt(format!("role {key} inherits an unknown role")));
        }
    }
    if document
        .grants
        .iter()
        .any(|grant| !document.roles.contains_key(&grant.role))
    {
        return Err(corrupt("grant references an unknown role"));
    }
    Ok(())
}

fn read_document(path: &Path) -> Result<AuthDocument> {
    let metadata = fs::metadata(path)
        .map_err(|error| io_error("failed to inspect authentication catalog", error))?;
    if metadata.len() > MAX_AUTH_FILE_BYTES {
        return Err(corrupt("authentication catalog exceeds its size limit"));
    }
    let mut file = File::open(path)
        .map_err(|error| io_error("failed to open authentication catalog", error))?;
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| corrupt("authentication catalog length cannot fit in memory"))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)
        .map_err(|error| io_error("failed to read authentication catalog", error))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| corrupt(format!("authentication catalog is invalid JSON: {error}")))
}

fn persist_document(path: &Path, document: &AuthDocument) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(document)
        .map_err(|error| internal(format!("failed to encode authentication catalog: {error}")))?;
    if bytes.len() as u64 > MAX_AUTH_FILE_BYTES {
        return Err(DbError::new(
            "54000",
            "authentication catalog exceeds its size limit",
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| internal("authentication catalog path has no parent"))?;
    let temporary = parent.join(format!(".{AUTH_FILE_NAME}.{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| io_error("failed to create authentication temporary file", error))?;
        file.write_all(&bytes)
            .map_err(|error| io_error("failed to write authentication temporary file", error))?;
        file.sync_all()
            .map_err(|error| io_error("failed to sync authentication temporary file", error))?;
        drop(file);
        atomic_replace(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let mut source: Vec<u16> = source.as_os_str().encode_wide().collect();
    source.push(0);
    let mut destination: Vec<u16> = destination.as_os_str().encode_wide().collect();
    destination.push(0);
    let flags = MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH;
    // SAFETY: both paths are valid, owned, NUL-terminated UTF-16 buffers for the
    // duration of the call; flags request an atomic same-volume replacement.
    let moved = unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), flags) };
    if moved == 0 {
        return Err(io_error(
            "failed to atomically replace authentication catalog",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination)
        .map_err(|error| io_error("failed to atomically replace authentication catalog", error))
}

fn decode_key(value: &str, label: &str) -> Result<[u8; SCRAM_KEY_BYTES]> {
    let decoded = decode_fixed_or_variable(value, label, Some(SCRAM_KEY_BYTES))?;
    decoded
        .try_into()
        .map_err(|_| corrupt(format!("{label} has the wrong length")))
}

fn decode_fixed_or_variable(value: &str, label: &str, expected: Option<usize>) -> Result<Vec<u8>> {
    let decoded = STANDARD
        .decode(value)
        .map_err(|_| corrupt(format!("{label} is not valid base64")))?;
    if expected.is_some_and(|length| decoded.len() != length) {
        return Err(corrupt(format!("{label} has the wrong length")));
    }
    Ok(decoded)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; SCRAM_KEY_BYTES] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of every size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn authentication_failed() -> DbError {
    DbError::new("28P01", "authentication failed")
}

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

fn corrupt(message: impl Into<String>) -> DbError {
    DbError::new("XX001", message)
}

fn internal(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message).with_hint("restart the service before retrying")
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn bootstrap_persists_both_verifiers_and_is_single_use() {
        let directory = tempdir().expect("tempdir");
        let store = AuthStore::open(directory.path()).expect("open");
        let principal = store
            .bootstrap_admin("DBA", b"correct horse battery staple")
            .expect("bootstrap");
        assert_eq!(principal.user, "dba");
        assert!(
            store
                .bootstrap_admin("other", b"another correct horse battery")
                .is_err()
        );
        let reopened = AuthStore::open(directory.path()).expect("reopen");
        assert!(
            reopened
                .authenticate_password("dba", b"correct horse battery staple")
                .is_ok()
        );
        assert!(
            reopened
                .authenticate_password("dba", b"wrong password value")
                .is_err()
        );
        let contents = fs::read_to_string(reopened.path()).expect("read auth");
        assert!(!contents.contains("correct horse battery staple"));
        assert!(contents.contains("argon2id"));
        assert!(contents.contains("storedKey"));
    }

    #[test]
    fn scram_verifier_accepts_a_valid_client_proof() {
        let verifier = ScramVerifier::derive(b"correct horse battery staple").expect("verifier");
        let auth_message = b"n=dba,r=client,r=clientserver,s=salt,i=4096,c=biws,r=clientserver";
        let stored_key = verifier.stored_key().expect("stored");
        let client_signature = hmac_sha256(&stored_key, auth_message);
        let password = b"correct horse battery staple";
        let salt = verifier.salt().expect("salt");
        let mut salted = [0_u8; SCRAM_KEY_BYTES];
        pbkdf2_hmac::<Sha256>(password, &salt, SCRAM_ITERATIONS, &mut salted);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature)
            .map(|(key, signature)| key ^ signature)
            .collect();
        assert!(
            verifier
                .verify_client_proof(auth_message, &proof)
                .expect("verify")
        );
        assert_eq!(
            STANDARD.decode(&verifier.salt).expect("base64"),
            verifier.salt().expect("salt")
        );
    }

    #[test]
    fn token_store_exposes_only_opaque_bearer_tokens() {
        let directory = tempdir().expect("tempdir");
        let auth = AuthStore::open(directory.path()).expect("open");
        auth.bootstrap_admin("dba", b"correct horse battery staple")
            .expect("bootstrap");
        let tokens = TokenStore::default();
        let response = tokens
            .issue(&auth, "dba", b"correct horse battery staple")
            .expect("issue");
        assert_eq!(response.token_type, "Bearer");
        assert_eq!(
            tokens
                .authenticate(&response.access_token)
                .expect("authenticate")
                .user,
            "dba"
        );
    }
}
