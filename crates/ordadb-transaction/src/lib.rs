mod fault;
mod ids;
mod locks;
mod mvcc;
mod record;
mod recovery;
mod ssi;
mod status;
mod wal;
mod writer;

pub use ids::{Lsn, TransactionId};
pub use locks::{
    LockGuard, LockKey, LockManager, LockManagerOptions, LockMode, LockSnapshot, LockWaitSnapshot,
};
pub use mvcc::{
    DurableTransaction, IsolationLevel, ManagedTransaction, Savepoint, SavepointId, SavepointStack,
    TransactionAccessMode, TransactionCharacteristics, TransactionManager, TransactionOutcome,
    TransactionSnapshot, TransactionStatusProvider, tuple_visible,
};
pub use record::{
    CheckpointBegin, CheckpointEnd, RecordKind, ScanResult, WAL_FORMAT_VERSION, WAL_HEADER_LEN,
    WAL_MAGIC, WAL_MAX_RECORD_LEN, WalPayload, WalRecord,
};
pub use recovery::RecoveryReport;
pub use ssi::{PredicateLock, SsiManager, SsiManagerOptions, SsiSavepoint, SsiTransactionSnapshot};
pub use status::{TRANSACTION_STATUS_FILE_NAME, TransactionStatusSnapshot, TransactionStatusStore};
pub use wal::{
    CheckpointState, LoggedTransaction, WAL_FILE_NAME, WalInspection, WalManager,
    inspect_wal_read_only,
};
pub use writer::{WriterCoordinator, WriterLease};

use std::io;

use ordadb_types::DbError;

fn corruption(message: impl Into<String>) -> DbError {
    DbError::new("XX001", message)
        .with_hint("restore from a known-good backup or recreate the database explicitly")
}

fn unsupported_version(version: u16) -> DbError {
    DbError::new(
        "0A000",
        format!("WAL format version {version} is not supported"),
    )
    .with_detail(format!(
        "this OrdaDB build supports WAL format version {WAL_FORMAT_VERSION}"
    ))
    .with_hint("back up the database and run an explicit supported migration")
}

fn io_error(context: &str, error: io::Error) -> DbError {
    DbError::new("58030", format!("{context}: {error}"))
        .with_hint("check the database path, permissions, and available disk space")
}
pub use fault::{DeterministicFaultInjector, FaultInjector, FaultPoint, NoFaultInjector};
