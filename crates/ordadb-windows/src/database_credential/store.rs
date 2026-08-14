#[cfg(test)]
use std::fs;
use std::path::Path;
use std::time::Duration;

use ordadb_types::{DbError, Result};
use rusqlite::config::DbConfig;
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use zeroize::Zeroizing;

use super::acl::{ensure_private_directory, restrict_private_file};
use super::crypto::{decrypt, encrypt};
use super::{DatabaseCredentialStore, not_found, sqlite_error, validate_credential_id};
use crate::StoredCredential;

const RECORD_VERSION: i64 = 1;
const STATE_ACTIVE: &str = "active";
const STATE_PENDING: &str = "legacyCleanupPending";
const STATE_DELETED: &str = "deleted";
const BUSY_TIMEOUT: Duration = Duration::from_secs(15);

struct CredentialRow {
    state: String,
    encrypted_payload: Option<Vec<u8>>,
}

pub(super) fn initialize(store: &DatabaseCredentialStore) -> Result<()> {
    let directory = store
        .path()
        .parent()
        .ok_or_else(|| DbError::new("58030", "credential database path has no parent"))?;
    ensure_private_directory(directory)?;
    let connection = open_raw(store.path())?;
    restrict_private_file(store.path())?;
    harden_connection(&connection)?;
    let schema_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|error| sqlite_error("failed to read credential schema version", error))?;
    if !matches!(schema_version, 0 | 1) {
        return Err(DbError::new(
            "0A000",
            "unsupported database credential schema version",
        ));
    }
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS database_credentials (
                credential_id TEXT PRIMARY KEY NOT NULL,
                record_version INTEGER NOT NULL CHECK (record_version = 1),
                state TEXT NOT NULL CHECK (state IN ('active', 'legacyCleanupPending', 'deleted')),
                encrypted_payload BLOB,
                CHECK ((state = 'deleted' AND encrypted_payload IS NULL)
                    OR (state != 'deleted' AND encrypted_payload IS NOT NULL))
            ) STRICT, WITHOUT ROWID;
            PRAGMA user_version = 1;",
        )
        .map_err(|error| sqlite_error("failed to initialize credential database", error))?;
    verify_configuration(&connection)?;
    Ok(())
}

pub(super) fn store(
    store: &DatabaseCredentialStore,
    credential_id: &str,
    username: &str,
    password: &Zeroizing<String>,
) -> Result<()> {
    validate_credential_id(credential_id)?;
    let encrypted = encrypt(username, password)?;
    let mut connection = open(store.path())?;
    let transaction = transaction(&mut connection)?;
    write_row(&transaction, credential_id, STATE_PENDING, Some(&encrypted))?;
    commit(transaction)?;
    let loaded = load_local(&connection, credential_id)?;
    if loaded.username.as_str() != username || loaded.password.as_str() != password.as_str() {
        return Err(DbError::new(
            "XX001",
            "database credential readback verification failed",
        ));
    }
    cleanup_legacy(store, credential_id)?;
    mark_active(&mut connection, credential_id)?;
    Ok(())
}

pub(super) fn load(
    store: &DatabaseCredentialStore,
    credential_id: &str,
) -> Result<StoredCredential> {
    validate_credential_id(credential_id)?;
    let mut connection = open(store.path())?;
    match read_row(&connection, credential_id)? {
        Some(row) if row.state == STATE_ACTIVE => decrypt_payload(row),
        Some(row) if row.state == STATE_PENDING => {
            let credential = decrypt_payload(row)?;
            cleanup_legacy(store, credential_id)?;
            mark_active(&mut connection, credential_id)?;
            Ok(credential)
        }
        Some(row) if row.state == STATE_DELETED => {
            debug_assert!(row.encrypted_payload.is_none());
            cleanup_legacy(store, credential_id)?;
            Err(not_found())
        }
        Some(_) => Err(DbError::new(
            "XX001",
            "credential record has an invalid state",
        )),
        None => migrate_legacy(store, &mut connection, credential_id),
    }
}

pub(super) fn delete(store: &DatabaseCredentialStore, credential_id: &str) -> Result<()> {
    validate_credential_id(credential_id)?;
    let mut connection = open(store.path())?;
    let transaction = transaction(&mut connection)?;
    write_row(&transaction, credential_id, STATE_DELETED, None)?;
    commit(transaction)?;
    cleanup_legacy(store, credential_id)
}

fn migrate_legacy(
    store: &DatabaseCredentialStore,
    connection: &mut Connection,
    credential_id: &str,
) -> Result<StoredCredential> {
    let legacy = match store.legacy.load(credential_id) {
        Ok(legacy) => legacy,
        Err(error) if error.sql_state == "42704" => {
            let transaction = transaction(connection)?;
            write_row(&transaction, credential_id, STATE_DELETED, None)?;
            commit(transaction)?;
            return Err(not_found());
        }
        Err(error) => return Err(error),
    };
    let encrypted = encrypt(&legacy.username, &legacy.password)?;
    let transaction = transaction(connection)?;
    write_row(&transaction, credential_id, STATE_PENDING, Some(&encrypted))?;
    commit(transaction)?;
    let verified = load_local(connection, credential_id)?;
    if verified.username != legacy.username || verified.password != legacy.password {
        return Err(DbError::new(
            "XX001",
            "migrated database credential readback verification failed",
        ));
    }
    cleanup_legacy(store, credential_id)?;
    mark_active(connection, credential_id)?;
    Ok(verified)
}

fn cleanup_legacy(store: &DatabaseCredentialStore, credential_id: &str) -> Result<()> {
    match store.legacy.delete(credential_id) {
        Ok(()) => Ok(()),
        Err(error) if error.sql_state == "42704" => Ok(()),
        Err(error) => Err(
            DbError::new("58030", "failed to remove migrated Windows credential")
                .with_detail(format!(
                    "legacy cleanup failed with SQLSTATE {}",
                    error.sql_state
                ))
                .with_hint("retry the database credential operation to finish local migration"),
        ),
    }
}

fn mark_active(connection: &mut Connection, credential_id: &str) -> Result<()> {
    let transaction = transaction(connection)?;
    let updated = transaction
        .execute(
            "UPDATE database_credentials SET state = ?1
             WHERE credential_id = ?2 AND state = ?3",
            params![STATE_ACTIVE, credential_id, STATE_PENDING],
        )
        .map_err(|error| sqlite_error("failed to commit credential migration", error))?;
    if updated != 1 {
        let state: Option<String> = transaction
            .query_row(
                "SELECT state FROM database_credentials WHERE credential_id = ?1",
                [credential_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| sqlite_error("failed to verify credential migration state", error))?;
        if state.as_deref() != Some(STATE_ACTIVE) {
            return Err(DbError::new(
                "XX001",
                "credential migration state changed unexpectedly",
            ));
        }
    }
    commit(transaction)
}

fn load_local(connection: &Connection, credential_id: &str) -> Result<StoredCredential> {
    let row = read_row(connection, credential_id)?.ok_or_else(not_found)?;
    if row.state == STATE_DELETED {
        return Err(not_found());
    }
    decrypt_payload(row)
}

fn decrypt_payload(row: CredentialRow) -> Result<StoredCredential> {
    let encrypted = row
        .encrypted_payload
        .ok_or_else(|| DbError::new("XX001", "credential record has no encrypted payload"))?;
    decrypt(&encrypted)
}

fn read_row(connection: &Connection, credential_id: &str) -> Result<Option<CredentialRow>> {
    connection
        .query_row(
            "SELECT state, encrypted_payload FROM database_credentials
             WHERE credential_id = ?1 AND record_version = ?2",
            params![credential_id, RECORD_VERSION],
            |row| {
                Ok(CredentialRow {
                    state: row.get(0)?,
                    encrypted_payload: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(|error| sqlite_error("failed to read database credential", error))
}

fn write_row(
    transaction: &Transaction<'_>,
    credential_id: &str,
    state: &str,
    encrypted_payload: Option<&[u8]>,
) -> Result<()> {
    transaction
        .execute(
            "INSERT INTO database_credentials
                (credential_id, record_version, state, encrypted_payload)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(credential_id) DO UPDATE SET
                record_version = excluded.record_version,
                state = excluded.state,
                encrypted_payload = excluded.encrypted_payload",
            params![credential_id, RECORD_VERSION, state, encrypted_payload],
        )
        .map_err(|error| sqlite_error("failed to write database credential", error))?;
    Ok(())
}

fn transaction(connection: &mut Connection) -> Result<Transaction<'_>> {
    connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("failed to begin credential transaction", error))
}

fn commit(transaction: Transaction<'_>) -> Result<()> {
    transaction
        .commit()
        .map_err(|error| sqlite_error("failed to commit credential transaction", error))
}

fn open(path: &Path) -> Result<Connection> {
    let connection = open_raw(path)?;
    harden_connection(&connection)?;
    verify_configuration(&connection)?;
    Ok(connection)
}

fn open_raw(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| sqlite_error("failed to open credential database", error))?;
    restrict_private_file(path)?;
    Ok(connection)
}

fn harden_connection(connection: &Connection) -> Result<()> {
    connection
        .busy_timeout(BUSY_TIMEOUT)
        .map_err(|error| sqlite_error("failed to set credential database busy timeout", error))?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .and_then(|()| connection.pragma_update(None, "temp_store", "MEMORY"))
        .and_then(|()| connection.pragma_update(None, "secure_delete", "ON"))
        .and_then(|()| connection.pragma_update(None, "foreign_keys", "ON"))
        .and_then(|()| connection.pragma_update(None, "trusted_schema", "OFF"))
        .and_then(|()| connection.pragma_update(None, "journal_mode", "DELETE"))
        .map_err(|error| sqlite_error("failed to harden credential database", error))?;
    connection
        .set_db_config(DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA, false)
        .and_then(|_| connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DQS_DDL, false))
        .and_then(|_| connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DQS_DML, false))
        .and_then(|_| connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true))
        .map_err(|error| sqlite_error("failed to restrict credential database schema", error))?;
    Ok(())
}

fn verify_configuration(connection: &Connection) -> Result<()> {
    let synchronous: i64 = connection
        .pragma_query_value(None, "synchronous", |row| row.get(0))
        .map_err(|error| sqlite_error("failed to verify credential database sync mode", error))?;
    let temp_store: i64 = connection
        .pragma_query_value(None, "temp_store", |row| row.get(0))
        .map_err(|error| {
            sqlite_error("failed to verify credential database temp storage", error)
        })?;
    let secure_delete: i64 = connection
        .pragma_query_value(None, "secure_delete", |row| row.get(0))
        .map_err(|error| {
            sqlite_error("failed to verify credential database deletion mode", error)
        })?;
    let trusted = connection
        .db_config(DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA)
        .map_err(|error| {
            sqlite_error("failed to verify credential database schema trust", error)
        })?;
    let defensive = connection
        .db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE)
        .map_err(|error| {
            sqlite_error("failed to verify credential database defensive mode", error)
        })?;
    if synchronous < 2 || temp_store != 2 || secure_delete != 1 || trusted || !defensive {
        return Err(DbError::new(
            "58030",
            "credential database security configuration was not applied",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn read_ciphertext(path: &Path, credential_id: &str) -> Result<Vec<u8>> {
    let connection = open(path)?;
    read_row(&connection, credential_id)?
        .and_then(|row| row.encrypted_payload)
        .ok_or_else(not_found)
}

#[cfg(test)]
pub(super) fn insert_pending(
    path: &Path,
    credential_id: &str,
    username: &str,
    password: &Zeroizing<String>,
) -> Result<()> {
    let encrypted = encrypt(username, password)?;
    let mut connection = open(path)?;
    let transaction = transaction(&mut connection)?;
    write_row(&transaction, credential_id, STATE_PENDING, Some(&encrypted))?;
    commit(transaction)
}

#[cfg(test)]
pub(super) fn tamper(path: &Path, credential_id: &str) -> Result<()> {
    let connection = open(path)?;
    connection
        .execute(
            "UPDATE database_credentials
             SET encrypted_payload = randomblob(length(encrypted_payload))
             WHERE credential_id = ?1",
            [credential_id],
        )
        .map_err(|error| sqlite_error("failed to tamper test credential", error))?;
    Ok(())
}

#[cfg(test)]
pub(super) fn persisted_files(path: &Path) -> Result<Vec<Vec<u8>>> {
    let directory = path
        .parent()
        .ok_or_else(|| DbError::internal("test credential path has no parent"))?;
    fs::read_dir(directory)
        .map_err(|_| DbError::new("58030", "failed to inspect test credential directory"))?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .map(|entry| {
            fs::read(entry.path())
                .map_err(|_| DbError::new("58030", "failed to read test credential artifact"))
        })
        .collect()
}
