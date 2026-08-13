
fn role_reaches(roles: &BTreeMap<String, Role>, start: &str, target: &str) -> Result<bool> {
    let mut visited = BTreeSet::new();
    let mut stack = vec![(start.to_owned(), 0usize)];
    while let Some((role, depth)) = stack.pop() {
        if depth > MAX_ROLE_DEPTH {
            return Err(DbError::new(
                "54001",
                "role membership depth exceeds the configured limit",
            ));
        }
        if role == target {
            return Ok(true);
        }
        if !visited.insert(role.clone()) {
            continue;
        }
        if let Some(definition) = roles.get(&role) {
            stack.extend(
                definition
                    .inherits
                    .iter()
                    .cloned()
                    .map(|parent| (parent, depth + 1)),
            );
        }
    }
    Ok(false)
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
    document
        .postgres_role_oids
        .validate(&postgres_role_names(document)?)?;
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
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| corrupt(format!("authentication catalog is invalid JSON: {error}")))?;
    let is_legacy = value
        .as_object()
        .is_some_and(|object| !object.contains_key("postgresRoleOids"));
    let mut document: AuthDocument = serde_json::from_value(value)
        .map_err(|error| corrupt(format!("authentication catalog is invalid JSON: {error}")))?;
    if is_legacy {
        document.postgres_role_oids =
            PostgresRoleOidRegistry::reconstruct(&postgres_role_names(&document)?)?;
    }
    Ok(document)
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

    #[test]
    fn role_user_membership_and_grants_mutate_atomically_without_plaintext() {
        let directory = tempdir().expect("tempdir");
        let auth = AuthStore::open(directory.path()).expect("open");
        auth.create_role("analyst", false).expect("create role");
        auth.create_role("reader", false).expect("create reader");
        auth.create_user("alice", b"initial password value", false)
            .expect("create user");
        auth.grant_role("analyst", "alice").expect("grant role");
        auth.grant_role("reader", "analyst")
            .expect("grant inherited role");
        assert_eq!(
            auth.grant_role("analyst", "reader")
                .expect_err("cycle")
                .sql_state,
            "0LP01"
        );
        auth.grant_privilege("reader", Action::Read, DbObject::Table("app.items".into()))
            .expect("grant privilege");
        assert!(
            auth.principal("alice")
                .expect("principal")
                .roles
                .contains("analyst")
        );
        auth.alter_user_password("alice", b"replacement password value")
            .expect("alter password");
        assert!(
            auth.authenticate_password("alice", b"initial password value")
                .is_err()
        );
        assert!(
            auth.authenticate_password("alice", b"replacement password value")
                .is_ok()
        );
        assert_eq!(
            auth.drop_role("reader", false)
                .expect_err("dependent role")
                .sql_state,
            "2BP01"
        );
        auth.revoke_privilege("reader", Action::Read, &DbObject::Table("app.items".into()))
            .expect("revoke privilege");
        auth.revoke_role("reader", "analyst")
            .expect("revoke inheritance");
        auth.drop_role("reader", false).expect("drop reader");

        let contents = fs::read_to_string(auth.path()).expect("read auth");
        assert!(!contents.contains("initial password value"));
        assert!(!contents.contains("replacement password value"));
        let reopened = AuthStore::open(directory.path()).expect("reopen");
        assert!(
            reopened
                .authenticate_password("alice", b"replacement password value")
                .is_ok()
        );
    }

    #[test]
    fn safe_role_snapshot_covers_users_and_roles_without_secret_fields() {
        let directory = tempdir().expect("tempdir");
        let auth = AuthStore::open(directory.path()).expect("open");
        auth.bootstrap_admin("dba", b"correct horse battery staple")
            .expect("bootstrap");
        auth.create_role("analyst", false).expect("create role");
        auth.create_user("alice", b"initial password value", false)
            .expect("create user");
        auth.grant_role("analyst", "alice").expect("grant role");
        auth.set_user_enabled("alice", false).expect("disable user");

        let snapshot = auth.safe_role_metadata_snapshot().expect("snapshot");
        assert_eq!(snapshot.roles.len(), 4);
        let alice = snapshot
            .roles
            .iter()
            .find(|role| role.name == "alice")
            .expect("alice metadata");
        assert!(alice.can_login);
        assert!(!alice.login_enabled);
        assert_eq!(alice.member_of, BTreeSet::from(["analyst".into()]));
        let analyst = snapshot
            .roles
            .iter()
            .find(|role| role.name == "analyst")
            .expect("analyst metadata");
        assert!(!analyst.can_login);
        assert!(!analyst.login_enabled);

        let serialized = serde_json::to_string(&snapshot).expect("serialize safe snapshot");
        let debug = format!("{snapshot:?}");
        for forbidden in [
            "argon2id",
            "password",
            "scram",
            "storedKey",
            "serverKey",
            "accessToken",
            "correct horse battery staple",
            "initial password value",
        ] {
            assert!(!serialized.contains(forbidden));
            assert!(!debug.contains(forbidden));
        }

        let reopened = AuthStore::open(directory.path()).expect("reopen");
        assert_eq!(
            reopened
                .safe_role_metadata_snapshot()
                .expect("reopened snapshot"),
            snapshot
        );
    }

    #[test]
    fn postgres_role_oids_are_persistent_monotonic_and_never_reused() {
        let directory = tempdir().expect("tempdir");
        let auth = AuthStore::open(directory.path()).expect("open");
        auth.create_role("first", false).expect("create first");
        let first_oid = auth
            .safe_role_metadata_snapshot()
            .expect("first snapshot")
            .roles[0]
            .postgres_oid;
        assert_eq!(first_oid, POSTGRES_ROLE_OID_FIRST_USER);
        auth.drop_role("first", false).expect("drop first");
        auth.create_user("second", b"initial password value", false)
            .expect("create second");
        let second_oid = auth
            .safe_role_metadata_snapshot()
            .expect("second snapshot")
            .roles[0]
            .postgres_oid;
        assert!(second_oid > first_oid);
        auth.drop_user("second", false).expect("drop second");
        auth.create_role("third", false).expect("create third");
        let third_oid = auth
            .safe_role_metadata_snapshot()
            .expect("third snapshot")
            .roles[0]
            .postgres_oid;
        assert!(third_oid > second_oid);

        let reopened = AuthStore::open(directory.path()).expect("reopen");
        assert_eq!(
            reopened
                .safe_role_metadata_snapshot()
                .expect("reopened snapshot")
                .roles[0]
                .postgres_oid,
            third_oid
        );
        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(reopened.path()).expect("read auth"))
                .expect("auth JSON");
        assert_eq!(
            persisted["postgresRoleOids"]["retiredOids"]
                .as_array()
                .expect("retired OIDs")
                .len(),
            2
        );
    }

    #[test]
    fn legacy_role_oids_reconstruct_deterministically_and_persist_on_mutation() {
        let directory = tempdir().expect("tempdir");
        let auth = AuthStore::open(directory.path()).expect("open");
        auth.create_role("zeta", false).expect("create zeta");
        auth.create_user("alpha", b"initial password value", false)
            .expect("create alpha");
        let path = auth.path().to_path_buf();
        drop(auth);

        let mut legacy: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("read auth")).expect("auth JSON");
        legacy
            .as_object_mut()
            .expect("auth object")
            .remove("postgresRoleOids");
        fs::write(
            &path,
            serde_json::to_vec_pretty(&legacy).expect("encode legacy auth"),
        )
        .expect("write legacy auth");

        let reopened = AuthStore::open(directory.path()).expect("open legacy");
        let snapshot = reopened
            .safe_role_metadata_snapshot()
            .expect("legacy snapshot");
        assert_eq!(
            snapshot
                .roles
                .iter()
                .map(|role| (role.name.as_str(), role.postgres_oid))
                .collect::<Vec<_>>(),
            vec![
                ("alpha", POSTGRES_ROLE_OID_FIRST_USER),
                ("zeta", POSTGRES_ROLE_OID_FIRST_USER + 1),
            ]
        );
        reopened
            .create_role("middle", false)
            .expect("persist reconstructed registry");
        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("read migrated auth"))
                .expect("migrated auth JSON");
        assert!(persisted.get("postgresRoleOids").is_some());
        assert_eq!(
            AuthStore::open(directory.path())
                .expect("reopen migrated")
                .safe_role_metadata_snapshot()
                .expect("migrated snapshot")
                .roles,
            reopened
                .safe_role_metadata_snapshot()
                .expect("current snapshot")
                .roles
        );
    }

    #[test]
    fn duplicate_or_null_postgres_role_oid_registry_fails_closed() {
        let directory = tempdir().expect("tempdir");
        let auth = AuthStore::open(directory.path()).expect("open");
        auth.create_role("one", false).expect("create one");
        auth.create_role("two", false).expect("create two");
        let path = auth.path().to_path_buf();
        drop(auth);

        let original = fs::read(&path).expect("read auth");
        let mut duplicate: serde_json::Value =
            serde_json::from_slice(&original).expect("auth JSON");
        let first_oid = duplicate["postgresRoleOids"]["mappings"]["one"]
            .as_u64()
            .expect("first OID");
        duplicate["postgresRoleOids"]["mappings"]["two"] = first_oid.into();
        fs::write(
            &path,
            serde_json::to_vec_pretty(&duplicate).expect("encode duplicate auth"),
        )
        .expect("write duplicate auth");
        assert_eq!(
            AuthStore::open(directory.path())
                .expect_err("duplicate OID must fail")
                .sql_state,
            "XX001"
        );

        let mut null_registry: serde_json::Value =
            serde_json::from_slice(&original).expect("auth JSON");
        null_registry["postgresRoleOids"] = serde_json::Value::Null;
        fs::write(
            &path,
            serde_json::to_vec_pretty(&null_registry).expect("encode null auth"),
        )
        .expect("write null auth");
        assert_eq!(
            AuthStore::open(directory.path())
                .expect_err("null registry must fail")
                .sql_state,
            "XX001"
        );
    }

    #[test]
    fn exhausted_postgres_role_oid_registry_rejects_creation_atomically() {
        let directory = tempdir().expect("tempdir");
        let auth = AuthStore::open(directory.path()).expect("open");
        auth.create_role("retained", false)
            .expect("create retained");
        let retained_oid =
            auth.safe_role_metadata_snapshot().expect("snapshot").roles[0].postgres_oid;
        let path = auth.path().to_path_buf();
        drop(auth);

        let mut exhausted: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("read auth")).expect("auth JSON");
        exhausted["postgresRoleOids"]["nextOid"] = (u64::from(u32::MAX) + 1).into();
        fs::write(
            &path,
            serde_json::to_vec_pretty(&exhausted).expect("encode exhausted auth"),
        )
        .expect("write exhausted auth");

        let reopened = AuthStore::open(directory.path()).expect("open exhausted");
        assert_eq!(
            reopened
                .create_role("rejected", false)
                .expect_err("OID exhaustion must fail")
                .sql_state,
            "54000"
        );
        let snapshot = reopened
            .safe_role_metadata_snapshot()
            .expect("snapshot after failure");
        assert_eq!(snapshot.roles.len(), 1);
        assert_eq!(snapshot.roles[0].name, "retained");
        assert_eq!(snapshot.roles[0].postgres_oid, retained_oid);
        assert!(
            !fs::read_to_string(&path)
                .expect("read auth after failure")
                .contains("rejected")
        );
    }
}
