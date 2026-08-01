use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use ordadb_storage::FROZEN_TRANSACTION_ID;
use ordadb_types::{DbError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    TransactionId, TransactionOutcome, TransactionStatusProvider, WalManager, corruption, io_error,
};

pub const TRANSACTION_STATUS_FILE_NAME: &str = "ordadb.transaction-status";
const TRANSACTION_STATUS_SCHEMA_VERSION: u16 = 1;
const TRANSACTION_STATUS_ENVELOPE_VERSION: u16 = 1;
const MAXIMUM_STATUS_FILE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct TransactionStatusStore {
    path: PathBuf,
    state: Mutex<TransactionStatusDocument>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TransactionStatusEnvelope {
    envelope_version: u16,
    sha256: String,
    document: TransactionStatusDocument,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TransactionStatusDocument {
    schema_version: u16,
    managed_transaction_floor: u64,
    retained_transaction_floor: u64,
    next_transaction_id: u64,
    statuses: BTreeMap<u64, TransactionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionStatusSnapshot {
    pub managed_transaction_floor: u64,
    pub retained_transaction_floor: u64,
    pub next_transaction_id: u64,
    pub statuses: BTreeMap<TransactionId, TransactionOutcome>,
}

impl TransactionStatusStore {
    pub fn open(data_dir: impl AsRef<Path>, next_transaction_floor: u64) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir)
            .map_err(|error| io_error("failed to create transaction status directory", error))?;
        let path = data_dir.join(TRANSACTION_STATUS_FILE_NAME);
        let minimum_next = next_transaction_floor.max(FROZEN_TRANSACTION_ID + 1);
        let mut document = if path.exists() {
            read_document(&path)?
        } else {
            TransactionStatusDocument {
                schema_version: TRANSACTION_STATUS_SCHEMA_VERSION,
                managed_transaction_floor: minimum_next,
                retained_transaction_floor: minimum_next,
                next_transaction_id: minimum_next,
                statuses: BTreeMap::new(),
            }
        };
        validate_document(&document)?;
        document.next_transaction_id = document.next_transaction_id.max(minimum_next);
        document
            .statuses
            .insert(FROZEN_TRANSACTION_ID, TransactionOutcome::Committed);
        write_document_atomic(&path, &document)?;
        Ok(Self {
            path,
            state: Mutex::new(document),
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn begin(&self) -> Result<TransactionId> {
        self.update(allocate_transaction)
    }

    pub fn begin_durable(&self, wal: &WalManager) -> Result<TransactionId> {
        let mut state = self.lock_state()?;
        let mut candidate = state.clone();
        let transaction_id = allocate_transaction(&mut candidate)?;
        wal.begin_transaction(transaction_id)?;
        let published = write_document_atomic(&self.path, &candidate);
        *state = candidate;
        published?;
        Ok(transaction_id)
    }

    pub fn commit(&self, transaction_id: TransactionId) -> Result<()> {
        self.finish(transaction_id, TransactionOutcome::Committed)
    }

    pub fn abort(&self, transaction_id: TransactionId) -> Result<()> {
        self.finish(transaction_id, TransactionOutcome::Aborted)
    }

    pub fn compact_before(&self, horizon: TransactionId) -> Result<usize> {
        self.update(|candidate| {
            let before = candidate.statuses.len();
            candidate.statuses.retain(|transaction_id, outcome| {
                *transaction_id == FROZEN_TRANSACTION_ID
                    || *transaction_id >= horizon.get()
                    || *outcome == TransactionOutcome::InProgress
            });
            candidate.retained_transaction_floor =
                candidate.retained_transaction_floor.max(horizon.get());
            Ok(before.saturating_sub(candidate.statuses.len()))
        })
    }

    pub fn reconcile_with_wal(
        &self,
        wal_outcomes: &BTreeMap<TransactionId, TransactionOutcome>,
    ) -> Result<usize> {
        self.update(|candidate| {
            let mut corrected = 0_usize;
            let highest_wal = wal_outcomes.keys().next_back().copied();
            if let Some(highest_wal) = highest_wal {
                candidate.next_transaction_id = candidate.next_transaction_id.max(
                    highest_wal
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| DbError::new("54000", "transaction ID space is exhausted"))?,
                );
            }

            let retained_floor = candidate.retained_transaction_floor;
            let status_ids = candidate
                .statuses
                .keys()
                .copied()
                .filter(|transaction_id| {
                    *transaction_id != FROZEN_TRANSACTION_ID
                        && *transaction_id >= retained_floor
                })
                .collect::<Vec<_>>();
            for transaction_id in status_ids {
                let transaction_id = TransactionId::new(transaction_id)
                    .ok_or_else(|| corruption("transaction status contains transaction ID zero"))?;
                let status = candidate
                    .statuses
                    .get(&transaction_id.get())
                    .copied()
                    .ok_or_else(|| corruption("transaction status disappeared during recovery"))?;
                let wal_status = wal_outcomes.get(&transaction_id).copied();
                match (status, wal_status) {
                    (TransactionOutcome::Committed, Some(TransactionOutcome::Committed))
                    | (
                        TransactionOutcome::Aborted,
                        None
                        | Some(
                            TransactionOutcome::InProgress | TransactionOutcome::Aborted,
                        ),
                    ) => {}
                    (
                        TransactionOutcome::Committed | TransactionOutcome::InProgress,
                        None | Some(TransactionOutcome::Aborted | TransactionOutcome::InProgress),
                    ) => {
                        candidate
                            .statuses
                            .insert(transaction_id.get(), TransactionOutcome::Aborted);
                        corrected = corrected.saturating_add(1);
                    }
                    (
                        TransactionOutcome::InProgress | TransactionOutcome::Aborted,
                        Some(TransactionOutcome::Committed),
                    ) => {
                        return Err(corruption(format!(
                            "transaction {transaction_id} has durable WAL Commit without committed status"
                        )));
                    }
                }
            }

            for (transaction_id, wal_status) in wal_outcomes {
                if transaction_id.get() < candidate.managed_transaction_floor
                    || transaction_id.get() < candidate.retained_transaction_floor
                    || candidate.statuses.contains_key(&transaction_id.get())
                {
                    continue;
                }
                match wal_status {
                    TransactionOutcome::Committed => {
                        return Err(corruption(format!(
                            "transaction {transaction_id} has durable WAL Commit without a transaction status"
                        )));
                    }
                    TransactionOutcome::InProgress | TransactionOutcome::Aborted => {
                        candidate
                            .statuses
                            .insert(transaction_id.get(), TransactionOutcome::Aborted);
                        corrected = corrected.saturating_add(1);
                    }
                }
            }
            Ok(corrected)
        })
    }

    pub fn snapshot(&self) -> Result<TransactionStatusSnapshot> {
        let state = self.lock_state()?;
        let statuses = state
            .statuses
            .iter()
            .map(|(transaction_id, outcome)| {
                TransactionId::new(*transaction_id)
                    .map(|transaction_id| (transaction_id, *outcome))
                    .ok_or_else(|| corruption("transaction status contains transaction ID zero"))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(TransactionStatusSnapshot {
            managed_transaction_floor: state.managed_transaction_floor,
            retained_transaction_floor: state.retained_transaction_floor,
            next_transaction_id: state.next_transaction_id,
            statuses,
        })
    }

    fn finish(&self, transaction_id: TransactionId, outcome: TransactionOutcome) -> Result<()> {
        if outcome == TransactionOutcome::InProgress {
            return Err(DbError::new(
                "22023",
                "terminal transaction status cannot be in progress",
            ));
        }
        self.update(
            |candidate| match candidate.statuses.get(&transaction_id.get()) {
                Some(TransactionOutcome::InProgress) => {
                    candidate.statuses.insert(transaction_id.get(), outcome);
                    Ok(())
                }
                Some(existing) if *existing == outcome => Ok(()),
                Some(_) => Err(DbError::new(
                    "25000",
                    "transaction already has a different terminal outcome",
                )),
                None => Err(DbError::new(
                    "25P01",
                    format!("transaction {transaction_id} is not registered"),
                )),
            },
        )
    }

    fn update<T>(
        &self,
        update: impl FnOnce(&mut TransactionStatusDocument) -> Result<T>,
    ) -> Result<T> {
        let mut state = self.lock_state()?;
        let mut candidate = state.clone();
        let output = update(&mut candidate)?;
        validate_document(&candidate)?;
        write_document_atomic(&self.path, &candidate)?;
        *state = candidate;
        Ok(output)
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, TransactionStatusDocument>> {
        self.state.lock().map_err(|_| {
            DbError::internal("transaction status store lock is poisoned")
                .with_hint("restart the process before retrying transaction work")
        })
    }
}

impl TransactionStatusProvider for TransactionStatusStore {
    fn transaction_outcome(&self, transaction_id: TransactionId) -> Result<TransactionOutcome> {
        self.lock_state()?
            .statuses
            .get(&transaction_id.get())
            .copied()
            .ok_or_else(|| {
                corruption(format!(
                    "transaction {transaction_id} has no durable transaction status"
                ))
            })
    }
}

fn validate_document(document: &TransactionStatusDocument) -> Result<()> {
    if document.schema_version != TRANSACTION_STATUS_SCHEMA_VERSION {
        return Err(DbError::new(
            "0A000",
            format!(
                "transaction status format version {} is not supported",
                document.schema_version
            ),
        )
        .with_hint("back up the database and run an explicit supported migration"));
    }
    if document.next_transaction_id <= FROZEN_TRANSACTION_ID {
        return Err(corruption(
            "transaction status next transaction ID is below the frozen boundary",
        ));
    }
    if document.managed_transaction_floor <= FROZEN_TRANSACTION_ID
        || document.managed_transaction_floor > document.next_transaction_id
        || document.retained_transaction_floor < document.managed_transaction_floor
        || document.retained_transaction_floor > document.next_transaction_id
    {
        return Err(corruption(
            "transaction status floor is outside its declared high-water mark",
        ));
    }
    for (transaction_id, outcome) in &document.statuses {
        if *transaction_id == 0 || *transaction_id >= document.next_transaction_id {
            return Err(corruption(
                "transaction status ID is outside its declared high-water mark",
            ));
        }
        if *transaction_id == FROZEN_TRANSACTION_ID && *outcome != TransactionOutcome::Committed {
            return Err(corruption(
                "frozen transaction must always be marked committed",
            ));
        }
    }
    Ok(())
}

fn allocate_transaction(document: &mut TransactionStatusDocument) -> Result<TransactionId> {
    let transaction_id = TransactionId::new(document.next_transaction_id)
        .ok_or_else(|| corruption("transaction status high-water mark is zero"))?;
    document.next_transaction_id = document
        .next_transaction_id
        .checked_add(1)
        .ok_or_else(|| DbError::new("54000", "transaction ID space is exhausted"))?;
    if document
        .statuses
        .insert(transaction_id.get(), TransactionOutcome::InProgress)
        .is_some()
    {
        return Err(corruption(format!(
            "transaction status already contains newly allocated transaction {transaction_id}"
        )));
    }
    Ok(transaction_id)
}

fn read_document(path: &Path) -> Result<TransactionStatusDocument> {
    let metadata = fs::metadata(path)
        .map_err(|error| io_error("failed to inspect transaction status file", error))?;
    if metadata.len() == 0 || metadata.len() > MAXIMUM_STATUS_FILE_BYTES {
        return Err(corruption(format!(
            "transaction status file is {} bytes; expected 1..={MAXIMUM_STATUS_FILE_BYTES}",
            metadata.len()
        )));
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| corruption("transaction status file exceeds the platform size limit"))?;
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|error| io_error("failed to read transaction status file", error))?;
    let envelope: TransactionStatusEnvelope = serde_json::from_slice(&bytes)
        .map_err(|error| corruption(format!("transaction status JSON is invalid: {error}")))?;
    if envelope.envelope_version != TRANSACTION_STATUS_ENVELOPE_VERSION {
        return Err(DbError::new(
            "0A000",
            format!(
                "transaction status envelope version {} is not supported",
                envelope.envelope_version
            ),
        )
        .with_hint("back up the database and run an explicit supported migration"));
    }
    let expected = document_sha256(&envelope.document)?;
    if envelope.sha256 != expected {
        return Err(corruption(
            "transaction status checksum does not match its document",
        ));
    }
    validate_document(&envelope.document)?;
    Ok(envelope.document)
}

fn write_document_atomic(path: &Path, document: &TransactionStatusDocument) -> Result<()> {
    let envelope = TransactionStatusEnvelope {
        envelope_version: TRANSACTION_STATUS_ENVELOPE_VERSION,
        sha256: document_sha256(document)?,
        document: document.clone(),
    };
    let bytes = serde_json::to_vec(&envelope).map_err(|error| {
        DbError::internal(format!("failed to encode transaction status: {error}"))
    })?;
    if bytes.is_empty()
        || u64::try_from(bytes.len())
            .map_err(|_| corruption("transaction status size exceeds u64"))?
            > MAXIMUM_STATUS_FILE_BYTES
    {
        return Err(
            DbError::new("54000", "transaction status file exceeds its 64 MiB limit")
                .with_hint("VACUUM the database to advance the transaction status horizon"),
        );
    }
    let parent = path
        .parent()
        .ok_or_else(|| DbError::internal("transaction status path has no parent"))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| DbError::new("22023", "transaction status file name is invalid"))?;
    let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| {
                io_error("failed to create transaction status temporary file", error)
            })?;
        file.write_all(&bytes).map_err(|error| {
            io_error("failed to write transaction status temporary file", error)
        })?;
        file.sync_all().map_err(|error| {
            io_error(
                "failed to synchronize transaction status temporary file",
                error,
            )
        })?;
        drop(file);
        atomic_replace(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn document_sha256(document: &TransactionStatusDocument) -> Result<String> {
    let bytes = serde_json::to_vec(document).map_err(|error| {
        DbError::internal(format!(
            "failed to encode transaction status checksum input: {error}"
        ))
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
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
    // SAFETY: both buffers are owned, live, and NUL-terminated for the
    // duration of this same-volume write-through replacement.
    let moved = unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), flags) };
    if moved == 0 {
        return Err(io_error(
            "failed to atomically replace transaction status file",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        fs::remove_file(destination)
            .map_err(|error| io_error("failed to replace transaction status file", error))?;
    }
    fs::rename(source, destination)
        .map_err(|error| io_error("failed to publish transaction status file", error))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::Value;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn status_transitions_are_atomic_and_reopen_with_high_water_mark() {
        let directory = tempdir().expect("tempdir");
        let store = TransactionStatusStore::open(directory.path(), 41).expect("open");
        let transaction_id = store.begin().expect("begin");
        assert_eq!(transaction_id.get(), 41);
        assert_eq!(
            store
                .transaction_outcome(transaction_id)
                .expect("in progress"),
            TransactionOutcome::InProgress
        );
        store.commit(transaction_id).expect("commit");
        drop(store);

        let reopened = TransactionStatusStore::open(directory.path(), 3).expect("reopen");
        assert_eq!(
            reopened
                .transaction_outcome(transaction_id)
                .expect("committed"),
            TransactionOutcome::Committed
        );
        assert_eq!(reopened.begin().expect("next transaction").get(), 42);
    }

    #[test]
    fn checksum_corruption_is_rejected_without_rewrite() {
        let directory = tempdir().expect("tempdir");
        let store = TransactionStatusStore::open(directory.path(), 3).expect("open");
        let path = store.path().to_path_buf();
        drop(store);
        let original = fs::read(&path).expect("read");
        let mut value: Value = serde_json::from_slice(&original).expect("JSON");
        value["sha256"] = Value::String("0".repeat(64));
        fs::write(&path, serde_json::to_vec(&value).expect("encode")).expect("corrupt");
        let corrupted = fs::read(&path).expect("corrupted bytes");
        let error =
            TransactionStatusStore::open(directory.path(), 3).expect_err("checksum corruption");
        assert_eq!(error.sql_state, "XX001");
        assert_eq!(fs::read(&path).expect("unchanged"), corrupted);
    }

    #[test]
    fn compaction_retains_frozen_active_and_newer_statuses() {
        let directory = tempdir().expect("tempdir");
        let store = TransactionStatusStore::open(directory.path(), 10).expect("open");
        let committed = store.begin().expect("committed begin");
        store.commit(committed).expect("commit");
        let active = store.begin().expect("active begin");
        let removed = store
            .compact_before(TransactionId::new(12).expect("horizon"))
            .expect("compact");
        assert_eq!(removed, 1);
        let snapshot = store.snapshot().expect("snapshot");
        assert_eq!(
            snapshot
                .statuses
                .get(&TransactionId::new(FROZEN_TRANSACTION_ID).expect("frozen")),
            Some(&TransactionOutcome::Committed)
        );
        assert_eq!(
            snapshot.statuses.get(&active),
            Some(&TransactionOutcome::InProgress)
        );
    }

    #[test]
    fn terminal_transition_is_idempotent_but_cannot_change_outcome() {
        let directory = tempdir().expect("tempdir");
        let store = TransactionStatusStore::open(directory.path(), 3).expect("open");
        let transaction_id = store.begin().expect("begin");
        store.abort(transaction_id).expect("abort");
        store.abort(transaction_id).expect("repeat abort");
        assert_eq!(
            store
                .commit(transaction_id)
                .expect_err("cannot change outcome")
                .sql_state,
            "25000"
        );
    }

    #[test]
    fn durable_begin_orders_wal_before_in_progress_status() {
        let directory = tempdir().expect("tempdir");
        let wal = WalManager::open(directory.path()).expect("wal");
        let store = TransactionStatusStore::open(directory.path(), 9).expect("status");
        let transaction_id = store.begin_durable(&wal).expect("durable begin");
        assert_eq!(transaction_id.get(), 9);
        assert_eq!(
            wal.transaction_outcomes()
                .expect("wal outcomes")
                .get(&transaction_id),
            Some(&TransactionOutcome::InProgress)
        );
        assert_eq!(
            store
                .transaction_outcome(transaction_id)
                .expect("status outcome"),
            TransactionOutcome::InProgress
        );
    }

    #[test]
    fn recovery_reconciles_status_commit_without_wal_commit_to_abort() {
        let directory = tempdir().expect("tempdir");
        let wal = WalManager::open(directory.path()).expect("wal");
        let store = TransactionStatusStore::open(directory.path(), 9).expect("status");
        let transaction_id = store.begin_durable(&wal).expect("durable begin");
        store.commit(transaction_id).expect("status commit");
        wal.abort(transaction_id).expect("recovery abort");

        assert_eq!(
            store
                .reconcile_with_wal(&wal.transaction_outcomes().expect("wal outcomes"))
                .expect("reconcile"),
            1
        );
        assert_eq!(
            store
                .transaction_outcome(transaction_id)
                .expect("reconciled status"),
            TransactionOutcome::Aborted
        );
    }
}
