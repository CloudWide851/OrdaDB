//! Versioned logical backup, restore, and table-transfer boundaries.

mod archive;
mod migration;
mod transfer;

pub use archive::{
    ARCHIVE_FORMAT_VERSION, ARCHIVE_HEADER_BYTES, ArchiveLimits, BackupSummary, RestoreSummary,
    estimate_snapshot_archive_bytes, read_archive, restore_archive_atomic,
    restore_archive_into_engine, restore_archive_to_new, write_archive, write_snapshot_archive,
};
pub use migration::{
    MigrationBytesV2, MigrationFaultInjector, MigrationInventoryV2, MigrationJournalV2,
    MigrationPathsV2, MigrationPhaseV2, MigrationPlanV2, MigrationReportV2, MigrationRunOptionsV2,
    NoMigrationFaults, migrate_v1_to_v2, migrate_v1_to_v2_with_faults, plan_v1_to_v2,
    rollback_v2_to_v1,
};
pub use transfer::{
    TableTransferRequest, TransferFormat, TransferLimits, TransferSummary, export_table,
    import_table, resolve_operation_path,
};
