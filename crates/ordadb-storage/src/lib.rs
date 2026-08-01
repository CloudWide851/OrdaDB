mod buffer;
mod disk;
mod page;
mod recovery;
mod store;
mod tuple;

pub use buffer::{BufferPool, DurabilityBarrier, NoWalBarrier, PageGuard};
pub use disk::DiskManager;
pub use page::{
    FILE_FORMAT_VERSION, MAX_RECORD_BYTES, PAGE_SIZE, PageId, PageType, SLOT_SIZE, SlottedPage,
};
pub use recovery::{RecoveryDataFile, RecoveryFileState, RecoveryPlan};
pub use store::{
    ApplyPoint, DATABASE_FILE_NAME, DATABASE_FORMAT_V1, DATABASE_FORMAT_V2, DataFormat,
    DatabaseStore, IndexManifest, IndexRebuildContractV2, IndexRebuildModeV2, PageDelta,
    PersistentState, PreparedCommit, StorageEstimate, StorageInspection, StorageTableCursorV2,
    TableManifest, VersionedRow,
};
pub use tuple::{
    FROZEN_TRANSACTION_ID, TUPLE_FORMAT_V1, TUPLE_FORMAT_V2, TUPLE_HEADER_V2_BYTES, TupleHeaderV2,
    decode_row, decode_row_v2, encode_row, encode_row_v2,
};

use std::io;

use ordadb_types::DbError;

pub(crate) fn corruption(message: impl Into<String>) -> DbError {
    DbError::new("XX001", message)
        .with_hint("restore from a known-good backup or recreate the database explicitly")
}

pub(crate) fn unsupported_version(version: u16) -> DbError {
    DbError::new(
        "0A000",
        format!("database file format version {version} is not supported"),
    )
    .with_detail(format!(
        "this OrdaDB build supports file format version {FILE_FORMAT_VERSION}"
    ))
    .with_hint("back up the database and run an explicit supported migration")
}

pub(crate) fn io_error(context: &str, error: io::Error) -> DbError {
    DbError::new("58030", format!("{context}: {error}"))
        .with_hint("check the database path, permissions, and available disk space")
}
