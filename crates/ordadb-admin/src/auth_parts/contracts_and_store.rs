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
pub const POSTGRES_ROLE_OID_FIRST_USER: u32 = 16_384;
const AUTH_FILE_NAME: &str = "ordadb.auth.json";
const MAX_AUTH_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MIN_PASSWORD_BYTES: usize = 12;
const MAX_PASSWORD_BYTES: usize = 1024;
const SCRAM_ITERATIONS: u32 = 4096;
const SCRAM_KEY_BYTES: usize = 32;
const TOKEN_BYTES: usize = 32;
const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_ROLE_DEPTH: usize = 64;

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
    #[serde(default = "missing_postgres_role_oid_registry")]
    postgres_role_oids: PostgresRoleOidRegistry,
}

impl Default for AuthDocument {
    fn default() -> Self {
        Self {
            format_version: AUTH_FORMAT_VERSION,
            users: BTreeMap::new(),
            roles: BTreeMap::new(),
            grants: Vec::new(),
            postgres_role_oids: PostgresRoleOidRegistry::empty(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PostgresRoleOidRegistry {
    first_user_oid: u32,
    next_oid: u64,
    mappings: BTreeMap<String, u32>,
    #[serde(default)]
    retired_oids: BTreeSet<u32>,
}

impl PostgresRoleOidRegistry {
    fn empty() -> Self {
        Self {
            first_user_oid: POSTGRES_ROLE_OID_FIRST_USER,
            next_oid: u64::from(POSTGRES_ROLE_OID_FIRST_USER),
            mappings: BTreeMap::new(),
            retired_oids: BTreeSet::new(),
        }
    }

    fn reconstruct(names: &BTreeSet<String>) -> Result<Self> {
        let mut registry = Self::empty();
        for name in names {
            registry.allocate(name)?;
        }
        Ok(registry)
    }

    fn allocate(&mut self, name: &str) -> Result<u32> {
        if self.mappings.contains_key(name) {
            return Err(corrupt("PostgreSQL role OID mapping already exists"));
        }
        let oid = u32::try_from(self.next_oid).map_err(|_| {
            DbError::new("54000", "PostgreSQL role OID space is exhausted")
                .with_hint("restore into a new isolated role authority before creating more roles")
        })?;
        if oid < self.first_user_oid || self.retired_oids.contains(&oid) {
            return Err(corrupt("PostgreSQL role OID allocator state is invalid"));
        }
        self.next_oid = self
            .next_oid
            .checked_add(1)
            .ok_or_else(|| corrupt("PostgreSQL role OID high-water mark overflowed"))?;
        self.mappings.insert(name.to_owned(), oid);
        Ok(oid)
    }

    fn retire(&mut self, name: &str) -> Result<u32> {
        let oid = self
            .mappings
            .remove(name)
            .ok_or_else(|| corrupt("PostgreSQL role OID mapping is missing"))?;
        if !self.retired_oids.insert(oid) {
            return Err(corrupt("PostgreSQL role OID was retired more than once"));
        }
        Ok(oid)
    }

    fn validate(&self, names: &BTreeSet<String>) -> Result<()> {
        let terminal = u64::from(u32::MAX) + 1;
        if self.first_user_oid != POSTGRES_ROLE_OID_FIRST_USER {
            return Err(corrupt(
                "PostgreSQL role OID first-user boundary is invalid",
            ));
        }
        if self.next_oid < u64::from(self.first_user_oid) || self.next_oid > terminal {
            return Err(corrupt("PostgreSQL role OID high-water mark is invalid"));
        }
        if self.mappings.keys().collect::<BTreeSet<_>>() != names.iter().collect() {
            return Err(corrupt(
                "PostgreSQL role OID mappings do not match the role authority",
            ));
        }
        let mut allocated = BTreeSet::new();
        for oid in self
            .mappings
            .values()
            .copied()
            .chain(self.retired_oids.iter().copied())
        {
            if oid < self.first_user_oid || u64::from(oid) >= self.next_oid {
                return Err(corrupt(
                    "PostgreSQL role OID is outside its declared high-water mark",
                ));
            }
            if !allocated.insert(oid) {
                return Err(corrupt("PostgreSQL role OID is duplicated"));
            }
        }
        Ok(())
    }
}

fn missing_postgres_role_oid_registry() -> PostgresRoleOidRegistry {
    PostgresRoleOidRegistry {
        first_user_oid: 0,
        next_oid: 0,
        mappings: BTreeMap::new(),
        retired_oids: BTreeSet::new(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SafeRoleMetadata {
    pub postgres_oid: u32,
    pub name: String,
    pub can_login: bool,
    pub login_enabled: bool,
    pub member_of: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SafeRoleMetadataSnapshot {
    pub roles: Vec<SafeRoleMetadata>,
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

    pub fn create_role(&self, name: &str, if_not_exists: bool) -> Result<bool> {
        let name = normalize_name(name, "role")?;
        self.mutate_document(|document| {
            if document.roles.contains_key(&name) || document.users.contains_key(&name) {
                if if_not_exists {
                    return Ok(false);
                }
                return Err(DbError::new("42710", format!("role {name} already exists")));
            }
            document.postgres_role_oids.allocate(&name)?;
            document.roles.insert(
                name.clone(),
                Role {
                    name,
                    inherits: BTreeSet::new(),
                },
            );
            Ok(true)
        })
    }

    pub fn create_user(&self, name: &str, password: &[u8], if_not_exists: bool) -> Result<bool> {
        let name = normalize_name(name, "user")?;
        validate_password(password)?;
        let scram = ScramVerifier::derive(password)?;
        let argon2id = hash_argon2(password)?;
        self.mutate_document(|document| {
            if document.users.contains_key(&name) || document.roles.contains_key(&name) {
                if if_not_exists {
                    return Ok(false);
                }
                return Err(DbError::new("42710", format!("user {name} already exists")));
            }
            document.postgres_role_oids.allocate(&name)?;
            document.users.insert(
                name.clone(),
                UserRecord {
                    name,
                    enabled: true,
                    scram,
                    argon2id,
                    roles: BTreeSet::new(),
                },
            );
            Ok(true)
        })
    }

    pub fn alter_user_password(&self, name: &str, password: &[u8]) -> Result<()> {
        let name = normalize_name(name, "user")?;
        validate_password(password)?;
        let scram = ScramVerifier::derive(password)?;
        let argon2id = hash_argon2(password)?;
        self.mutate_document(|document| {
            let user = document
                .users
                .get_mut(&name)
                .ok_or_else(|| DbError::new("42704", format!("user {name} does not exist")))?;
            user.scram = scram;
            user.argon2id = argon2id;
            Ok(())
        })
    }

    pub fn set_user_enabled(&self, name: &str, enabled: bool) -> Result<()> {
        let name = normalize_name(name, "user")?;
        self.mutate_document(|document| {
            let user = document
                .users
                .get_mut(&name)
                .ok_or_else(|| DbError::new("42704", format!("user {name} does not exist")))?;
            user.enabled = enabled;
            Ok(())
        })
    }

    pub fn drop_user(&self, name: &str, if_exists: bool) -> Result<bool> {
        let name = normalize_name(name, "user")?;
        self.mutate_document(|document| {
            if document.users.remove(&name).is_some() {
                document.postgres_role_oids.retire(&name)?;
                return Ok(true);
            }
            if if_exists {
                Ok(false)
            } else {
                Err(DbError::new("42704", format!("user {name} does not exist")))
            }
        })
    }

    pub fn drop_role(&self, name: &str, if_exists: bool) -> Result<bool> {
        let name = normalize_name(name, "role")?;
        self.mutate_document(|document| {
            if !document.roles.contains_key(&name) {
                return if if_exists {
                    Ok(false)
                } else {
                    Err(DbError::new("42704", format!("role {name} does not exist")))
                };
            }
            let referenced_by_user = document
                .users
                .values()
                .any(|user| user.roles.contains(&name));
            let referenced_by_role = document
                .roles
                .values()
                .any(|role| role.inherits.contains(&name));
            let has_grants = document.grants.iter().any(|grant| grant.role == name);
            if referenced_by_user || referenced_by_role || has_grants {
                return Err(DbError::new(
                    "2BP01",
                    format!("role {name} has dependent memberships or grants"),
                )
                .with_hint("revoke memberships and privileges before dropping the role"));
            }
            document.roles.remove(&name);
            document.postgres_role_oids.retire(&name)?;
            Ok(true)
        })
    }

    pub fn grant_role(&self, role: &str, member: &str) -> Result<bool> {
        let role = normalize_name(role, "role")?;
        let member = normalize_name(member, "member")?;
        self.mutate_document(|document| {
            if !document.roles.contains_key(&role) {
                return Err(DbError::new("42704", format!("role {role} does not exist")));
            }
            if let Some(user) = document.users.get_mut(&member) {
                return Ok(user.roles.insert(role));
            }
            if !document.roles.contains_key(&member) {
                return Err(DbError::new(
                    "42704",
                    format!("member {member} does not exist"),
                ));
            }
            if role == member || role_reaches(&document.roles, &role, &member)? {
                return Err(DbError::new(
                    "0LP01",
                    "role membership would create a cycle",
                ));
            }
            Ok(document
                .roles
                .get_mut(&member)
                .ok_or_else(|| DbError::internal("role member disappeared"))?
                .inherits
                .insert(role))
        })
    }

    pub fn revoke_role(&self, role: &str, member: &str) -> Result<bool> {
        let role = normalize_name(role, "role")?;
        let member = normalize_name(member, "member")?;
        self.mutate_document(|document| {
            if let Some(user) = document.users.get_mut(&member) {
                return Ok(user.roles.remove(&role));
            }
            if let Some(member_role) = document.roles.get_mut(&member) {
                return Ok(member_role.inherits.remove(&role));
            }
            Err(DbError::new(
                "42704",
                format!("member {member} does not exist"),
            ))
        })
    }

    pub fn grant_privilege(&self, role: &str, action: Action, object: DbObject) -> Result<bool> {
        let role = normalize_name(role, "role")?;
        self.mutate_document(|document| {
            if !document.roles.contains_key(&role) {
                return Err(DbError::new("42704", format!("role {role} does not exist")));
            }
            let grant = Grant {
                role,
                action,
                object,
            };
            if document.grants.contains(&grant) {
                return Ok(false);
            }
            document.grants.push(grant);
            Ok(true)
        })
    }

    pub fn revoke_privilege(&self, role: &str, action: Action, object: &DbObject) -> Result<bool> {
        let role = normalize_name(role, "role")?;
        self.mutate_document(|document| {
            let before = document.grants.len();
            document.grants.retain(|grant| {
                grant.role != role || grant.action != action || &grant.object != object
            });
            Ok(document.grants.len() != before)
        })
    }

    pub fn principal(&self, username: &str) -> Result<Principal> {
        let username = normalize_name(username, "user")?;
        let document = self.read_document()?;
        let user = document
            .users
            .get(&username)
            .ok_or_else(authentication_failed)?;
        Ok(Principal {
            user: user.name.clone(),
            roles: user.roles.clone(),
        })
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
        if username == role_name {
            return Err(DbError::new(
                "42710",
                "administrator user name conflicts with the bootstrap role",
            ));
        }
        if !candidate.roles.contains_key(&role_name) {
            candidate.postgres_role_oids.allocate(&role_name)?;
        }
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
        if candidate.roles.contains_key(&username) {
            return Err(DbError::new(
                "42710",
                format!("user {username} already exists as a role"),
            ));
        }
        candidate.postgres_role_oids.allocate(&username)?;
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

    pub fn safe_role_metadata_snapshot(&self) -> Result<SafeRoleMetadataSnapshot> {
        let document = self.read_document()?;
        let mut roles = Vec::with_capacity(document.users.len() + document.roles.len());
        for user in document.users.values() {
            roles.push(SafeRoleMetadata {
                postgres_oid: postgres_role_oid(&document, &user.name)?,
                name: user.name.clone(),
                can_login: true,
                login_enabled: user.enabled,
                member_of: user.roles.clone(),
            });
        }
        for role in document.roles.values() {
            roles.push(SafeRoleMetadata {
                postgres_oid: postgres_role_oid(&document, &role.name)?,
                name: role.name.clone(),
                can_login: false,
                login_enabled: false,
                member_of: role.inherits.clone(),
            });
        }
        roles.sort_by_key(|role| role.postgres_oid);
        Ok(SafeRoleMetadataSnapshot { roles })
    }

    fn mutate_document<T>(
        &self,
        mutation: impl FnOnce(&mut AuthDocument) -> Result<T>,
    ) -> Result<T> {
        let _write = self
            .write_lock
            .lock()
            .map_err(|_| internal("authentication write lock is poisoned"))?;
        let mut candidate = self.read_document()?.clone();
        let output = mutation(&mut candidate)?;
        validate_document(&candidate)?;
        persist_document(&self.path, &candidate)?;
        *self.write_document()? = candidate;
        Ok(output)
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

fn postgres_role_names(document: &AuthDocument) -> Result<BTreeSet<String>> {
    if document
        .users
        .keys()
        .any(|name| document.roles.contains_key(name))
    {
        return Err(corrupt(
            "authentication user and role names share the same authority key",
        ));
    }
    Ok(document
        .users
        .keys()
        .chain(document.roles.keys())
        .cloned()
        .collect())
}

fn postgres_role_oid(document: &AuthDocument, name: &str) -> Result<u32> {
    document
        .postgres_role_oids
        .mappings
        .get(name)
        .copied()
        .ok_or_else(|| corrupt("PostgreSQL role OID mapping is missing"))
}
