//! Versioned logical backup, restore, and table-transfer boundaries.

mod archive;
mod installer;
mod migration;
mod transfer;

pub use archive::{
    ARCHIVE_FORMAT_VERSION, ARCHIVE_HEADER_BYTES, ArchiveLimits, BackupSummary, RestoreSummary,
    estimate_snapshot_archive_bytes, read_archive, restore_archive_atomic,
    restore_archive_into_engine, restore_archive_to_new, write_archive, write_snapshot_archive,
};
pub use installer::{
    INSTALLER_STORAGE_SCHEMA_VERSION, InstallerMigrationIncompatibilityV1,
    InstallerMigrationReceiptV1, InstallerSourceFingerprintV1, InstallerStorageApplyActionV1,
    InstallerStorageApplyReportV1, InstallerStorageInventoryV1, InstallerStorageOptionsV1,
    InstallerStoragePreflightV1, MAX_INSTALLER_RECEIPT_BYTES, MAX_INSTALLER_REPORT_BYTES,
    MAX_INSTALLER_STATE_BYTES, apply_installer_storage_receipt,
    apply_installer_storage_receipt_with_options, decode_installer_migration_receipt,
    installer_storage_preflight, installer_storage_preflight_with_options,
    validate_installer_migration_receipt,
};
pub use migration::{
    MigrationBytesV2, MigrationFaultInjector, MigrationInventoryV2, MigrationJournalV2,
    MigrationPathsV2, MigrationPhaseV2, MigrationPlanV2, MigrationReportV2, MigrationRunOptionsV2,
    NoMigrationFaults, apply_migration_plan_v2, migrate_v1_to_v2, migrate_v1_to_v2_with_faults,
    plan_v1_to_v2, rollback_v2_to_v1,
};
pub use transfer::{
    TableTransferRequest, TransferFormat, TransferLimits, TransferSummary, export_table,
    import_table, resolve_operation_path,
};
