use std::collections::BTreeMap;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use tempfile::TempDir;
use zeroize::Zeroizing;

use super::acl::{private_directory_sddl, private_file_sddl};
use super::crypto::unprotect_with_wrong_context;
use super::store::{insert_pending, persisted_files, read_ciphertext, tamper};
use super::*;

#[derive(Default)]
struct FakeLegacy {
    records: Mutex<BTreeMap<String, StoredCredential>>,
    fail_delete: AtomicBool,
}

impl FakeLegacy {
    fn insert(&self, id: &str, username: &str, password: &str) {
        self.records.lock().expect("legacy records").insert(
            id.to_owned(),
            StoredCredential {
                username: Zeroizing::new(username.to_owned()),
                password: Zeroizing::new(password.to_owned()),
            },
        );
    }
}

impl LegacyCredentialBackend for FakeLegacy {
    fn load(&self, credential_id: &str) -> Result<StoredCredential> {
        self.records
            .lock()
            .expect("legacy records")
            .get(credential_id)
            .cloned()
            .ok_or_else(not_found)
    }

    fn delete(&self, credential_id: &str) -> Result<()> {
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err(DbError::new("58030", "injected legacy deletion failure"));
        }
        self.records
            .lock()
            .expect("legacy records")
            .remove(credential_id)
            .map(|_| ())
            .ok_or_else(not_found)
    }
}

fn fixture(legacy: Arc<FakeLegacy>) -> (TempDir, DatabaseCredentialStore) {
    let root = tempfile::tempdir().expect("credential fixture");
    let path = root.path().join("credentials").join(DATABASE_FILE);
    let store = DatabaseCredentialStore::open_with_legacy(path, legacy).expect("open store");
    (root, store)
}

#[test]
fn dpapi_round_trip_is_random_redacted_and_context_bound() {
    let (_root, store) = fixture(Arc::new(FakeLegacy::default()));
    let password = Zeroizing::new("database-password-one".to_owned());
    store
        .store("primary", "database-user-one", &password)
        .expect("first store");
    let first = read_ciphertext(store.path(), "primary").expect("first ciphertext");
    store
        .store("primary", "database-user-one", &password)
        .expect("second store");
    let second = read_ciphertext(store.path(), "primary").expect("second ciphertext");
    assert_ne!(first, second);
    assert!(unprotect_with_wrong_context(&second).is_err());
    let loaded = store.load("primary").expect("load");
    assert_eq!(loaded.username.as_str(), "database-user-one");
    assert_eq!(loaded.password.as_str(), "database-password-one");
    let debug = format!("{loaded:?} {store:?}");
    assert!(!debug.contains("database-user-one"));
    assert!(!debug.contains("database-password-one"));
    assert!(!debug.contains(store.path().to_string_lossy().as_ref()));
}

#[test]
fn dpapi_rejects_an_anonymous_security_context() {
    use windows_sys::Win32::Security::{ImpersonateAnonymousToken, RevertToSelf};
    use windows_sys::Win32::System::Threading::GetCurrentThread;

    let ciphertext = super::crypto::encrypt(
        "context-user",
        &Zeroizing::new("context-password".to_owned()),
    )
    .expect("encrypt as current user");
    let impersonated = unsafe { ImpersonateAnonymousToken(GetCurrentThread()) };
    if impersonated == 0 {
        return;
    }
    struct Revert;
    impl Drop for Revert {
        fn drop(&mut self) {
            unsafe {
                RevertToSelf();
            }
        }
    }
    let _revert = Revert;
    assert!(super::crypto::decrypt(&ciphertext).is_err());
}

#[test]
fn tampering_and_deleted_tombstones_fail_closed() {
    let legacy = Arc::new(FakeLegacy::default());
    let (_root, store) = fixture(Arc::clone(&legacy));
    let password = Zeroizing::new("database-password-two".to_owned());
    store
        .store("tampered", "database-user-two", &password)
        .expect("store");
    tamper(store.path(), "tampered").expect("tamper");
    assert_eq!(
        store.load("tampered").expect_err("tampered").sql_state,
        "58030"
    );

    legacy.insert("deleted", "legacy-user", "legacy-password");
    store.delete("deleted").expect("tombstone");
    legacy.insert("deleted", "reappeared-user", "reappeared-password");
    assert_eq!(
        store.load("deleted").expect_err("deleted").sql_state,
        "42704"
    );
}

#[test]
fn migration_cleanup_is_verified_pending_and_idempotent() {
    let legacy = Arc::new(FakeLegacy::default());
    legacy.insert("migrate", "legacy-user", "legacy-password");
    legacy.fail_delete.store(true, Ordering::SeqCst);
    let (_root, store) = fixture(Arc::clone(&legacy));
    let error = store.load("migrate").expect_err("cleanup failure");
    assert_eq!(error.sql_state, "58030");
    legacy.fail_delete.store(false, Ordering::SeqCst);
    let loaded = store.load("migrate").expect("retry migration");
    assert_eq!(loaded.username.as_str(), "legacy-user");
    assert_eq!(loaded.password.as_str(), "legacy-password");
    assert!(legacy.records.lock().expect("legacy records").is_empty());
    let reloaded = store.load("migrate").expect("active reload");
    assert_eq!(reloaded.username.as_str(), "legacy-user");
}

#[test]
fn interrupted_pending_state_resumes_without_rewriting_ciphertext() {
    let legacy = Arc::new(FakeLegacy::default());
    legacy.insert("pending", "pending-user", "pending-password");
    let (_root, store) = fixture(Arc::clone(&legacy));
    let password = Zeroizing::new("pending-password".to_owned());
    insert_pending(store.path(), "pending", "pending-user", &password).expect("pending row");
    let before = read_ciphertext(store.path(), "pending").expect("pending ciphertext");
    let loaded = store.load("pending").expect("resume");
    let after = read_ciphertext(store.path(), "pending").expect("active ciphertext");
    assert_eq!(loaded.username.as_str(), "pending-user");
    assert_eq!(before, after);
    assert!(legacy.records.lock().expect("legacy records").is_empty());
}

#[test]
fn damaged_pending_state_preserves_the_legacy_source() {
    let legacy = Arc::new(FakeLegacy::default());
    legacy.insert("damaged-pending", "legacy-user", "legacy-password");
    let (_root, store) = fixture(Arc::clone(&legacy));
    insert_pending(
        store.path(),
        "damaged-pending",
        "legacy-user",
        &Zeroizing::new("legacy-password".to_owned()),
    )
    .expect("pending row");
    tamper(store.path(), "damaged-pending").expect("tamper pending row");

    assert!(store.load("damaged-pending").is_err());
    assert!(
        legacy
            .records
            .lock()
            .expect("legacy records")
            .contains_key("damaged-pending")
    );
}

#[test]
fn persisted_artifacts_contain_no_plaintext_secrets() {
    let (_root, store) = fixture(Arc::new(FakeLegacy::default()));
    let username = "unique-persisted-database-user";
    let password_text = "unique-persisted-database-password";
    store
        .store(
            "persisted",
            username,
            &Zeroizing::new(password_text.to_owned()),
        )
        .expect("store");
    for bytes in persisted_files(store.path()).expect("artifacts") {
        assert!(
            !bytes
                .windows(username.len())
                .any(|window| window == username.as_bytes())
        );
        assert!(
            !bytes
                .windows(password_text.len())
                .any(|window| window == password_text.as_bytes())
        );
    }
}

#[test]
fn desktop_and_tui_style_concurrent_access_uses_one_database() {
    let (_root, store) = fixture(Arc::new(FakeLegacy::default()));
    let desktop = store.clone();
    let tui = store.clone();
    let first = thread::spawn(move || {
        desktop.store(
            "desktop",
            "desktop-user",
            &Zeroizing::new("desktop-password".to_owned()),
        )
    });
    let second = thread::spawn(move || {
        tui.store(
            "tui",
            "tui-user",
            &Zeroizing::new("tui-password".to_owned()),
        )
    });
    first.join().expect("desktop join").expect("desktop store");
    second.join().expect("tui join").expect("tui store");
    assert_eq!(
        store
            .load("desktop")
            .expect("desktop load")
            .username
            .as_str(),
        "desktop-user"
    );
    assert_eq!(
        store.load("tui").expect("tui load").username.as_str(),
        "tui-user"
    );
}

#[test]
fn desktop_and_tui_processes_share_the_busy_bounded_database() {
    const CHILD_TEST: &str = "database_credential::tests::database_credential_process_worker";
    let root = tempfile::tempdir().expect("credential process fixture");
    let path = root.path().join("credentials").join(DATABASE_FILE);
    let executable = std::env::current_exe().expect("current test executable");
    let mut desktop = Command::new(&executable)
        .arg(CHILD_TEST)
        .arg("--exact")
        .env("ORDADB_CREDENTIAL_PROCESS_PATH", &path)
        .env("ORDADB_CREDENTIAL_PROCESS_SLOT", "desktop")
        .spawn()
        .expect("spawn desktop credential process");
    let mut tui = Command::new(executable)
        .arg(CHILD_TEST)
        .arg("--exact")
        .env("ORDADB_CREDENTIAL_PROCESS_PATH", &path)
        .env("ORDADB_CREDENTIAL_PROCESS_SLOT", "tui")
        .spawn()
        .expect("spawn TUI credential process");

    assert!(desktop.wait().expect("desktop process status").success());
    assert!(tui.wait().expect("TUI process status").success());
    let store = DatabaseCredentialStore::open_path(path).expect("reopen shared store");
    assert_eq!(
        store
            .load("process-desktop")
            .expect("desktop process credential")
            .username
            .as_str(),
        "process-user-desktop"
    );
    assert_eq!(
        store
            .load("process-tui")
            .expect("TUI process credential")
            .username
            .as_str(),
        "process-user-tui"
    );
}

#[test]
fn database_credential_process_worker() {
    let Some(path) = std::env::var_os("ORDADB_CREDENTIAL_PROCESS_PATH") else {
        return;
    };
    let Some(slot) = std::env::var_os("ORDADB_CREDENTIAL_PROCESS_SLOT") else {
        return;
    };
    let slot = slot.to_string_lossy();
    let store = DatabaseCredentialStore::open_path(PathBuf::from(path)).expect("child store");
    store
        .store(
            &format!("process-{slot}"),
            &format!("process-user-{slot}"),
            &Zeroizing::new(format!("process-password-{slot}")),
        )
        .expect("child credential store");
}

#[test]
fn directory_acl_allows_only_system_and_current_user() {
    let sid = crate::current_process_user_sid().expect("current SID");
    let sddl = private_directory_sddl(&sid);
    assert_eq!(sddl, format!("D:P(A;OICI;GA;;;SY)(A;OICI;GA;;;{sid})"));
    assert!(!sddl.contains(";;;BA"));
    assert!(!sddl.contains(";;;WD"));
    assert_eq!(
        private_file_sddl(&sid),
        format!("D:P(A;;GA;;;SY)(A;;GA;;;{sid})")
    );
}
