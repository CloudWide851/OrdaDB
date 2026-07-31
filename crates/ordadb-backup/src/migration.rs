use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use ordadb_cluster::{
    ACTIVE_POINTER_FILE, AUTH_FILE_NAME, CLUSTER_MANIFEST_FILE, DATABASE_MANIFEST_FILE,
    DEFAULT_DATABASE_NAME, FinalizeClusterOptions, LEGACY_DATA_FILE, LEGACY_WAL_FILE,
    MigrationProvenanceV2, RootAuthority, TRANSACTION_STATE_FILE, activate_published_cluster,
    available_space, estimate_cluster_documents_v2, finalize_cluster_layout, inspect_root,
    migration_journal_path, prepare_cluster_layout, publish_cluster_directory,
    remove_active_pointer_for_rollback, resolve_active_v2, validate_cluster_directory,
    write_migration_journal,
};
use ordadb_engine::{Engine, EngineConfig, LOGICAL_SNAPSHOT_VERSION, LogicalDatabaseSnapshot};
use ordadb_storage::{DataFormat, DatabaseStore, PersistentState, StorageEstimate};
use ordadb_transaction::inspect_wal_read_only;
use ordadb_types::{DbError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    ARCHIVE_HEADER_BYTES, ArchiveLimits, BackupSummary, estimate_snapshot_archive_bytes,
    read_archive, write_snapshot_archive,
};

const MIGRATION_FORMAT_VERSION: u16 = 2;
const COPY_BUFFER_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MigrationPhaseV2 {
    Inspected,
    LogicalBackupWritten,
    CandidateBuilt,
    CandidateValidated,
    CandidateReopened,
    RollbackRetained,
    CandidatePublished,
    PointerSwitched,
    Completed,
}

impl MigrationPhaseV2 {
    #[must_use]
    pub const fn ordered() -> [Self; 9] {
        [
            Self::Inspected,
            Self::LogicalBackupWritten,
            Self::CandidateBuilt,
            Self::CandidateValidated,
            Self::CandidateReopened,
            Self::RollbackRetained,
            Self::CandidatePublished,
            Self::PointerSwitched,
            Self::Completed,
        ]
    }
}

pub trait MigrationFaultInjector: std::fmt::Debug + Send + Sync {
    fn check(&self, phase: MigrationPhaseV2) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoMigrationFaults;

impl MigrationFaultInjector for NoMigrationFaults {
    fn check(&self, _phase: MigrationPhaseV2) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MigrationRunOptionsV2 {
    pub archive_limits: ArchiveLimits,
    pub available_bytes_override: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MigrationInventoryV2 {
    pub database_name: String,
    pub source_generation: u64,
    pub schema_count: u64,
    pub table_count: u64,
    pub index_count: u64,
    pub row_count: u64,
    pub data_file_bytes: u64,
    pub wal_file_bytes: u64,
    pub auth_file_bytes: u64,
    pub data_file_sha256: String,
    pub wal_file_sha256: Option<String>,
    pub auth_file_sha256: Option<String>,
    pub wal_record_count: u64,
    pub maximum_transaction_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MigrationPathsV2 {
    pub cluster_root: PathBuf,
    pub logical_backup: PathBuf,
    pub candidate_cluster: PathBuf,
    pub published_cluster: PathBuf,
    pub rollback_directory: PathBuf,
    pub journal: PathBuf,
    pub active_pointer: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MigrationBytesV2 {
    pub logical_archive_bytes: u64,
    pub candidate_database_bytes: u64,
    pub candidate_role_bytes: u64,
    pub rollback_copy_bytes: u64,
    pub rollback_manifest_bytes: u64,
    pub cluster_document_bytes: u64,
    pub migration_journal_bytes: u64,
    pub atomic_temporary_bytes: u64,
    pub required_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MigrationPlanV2 {
    pub format_version: u16,
    pub migration_id: Uuid,
    pub cluster_id: Uuid,
    pub database_id: Uuid,
    pub planned_at: DateTime<Utc>,
    pub source_format_version: u16,
    pub target_format_version: u16,
    pub inventory: MigrationInventoryV2,
    pub candidate_storage: StorageEstimate,
    pub next_transaction_id: u64,
    pub paths: MigrationPathsV2,
    pub bytes: MigrationBytesV2,
    pub incompatibilities: Vec<String>,
    pub phases: Vec<MigrationPhaseV2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MigrationJournalV2 {
    pub format_version: u16,
    pub migration_id: Uuid,
    pub phase: MigrationPhaseV2,
    pub updated_at: DateTime<Utc>,
    pub plan: MigrationPlanV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MigrationReportV2 {
    pub migration_id: Uuid,
    pub cluster_id: Uuid,
    pub database_id: Uuid,
    pub completed_at: DateTime<Utc>,
    pub source_generation: u64,
    pub activation_generation: u64,
    pub table_count: u64,
    pub row_count: u64,
    pub backup: BackupSummary,
    pub paths: MigrationPathsV2,
    pub final_phase: MigrationPhaseV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RollbackFileV1 {
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RollbackManifestV1 {
    format_version: u16,
    migration_id: Uuid,
    created_at: DateTime<Utc>,
    files: BTreeMap<String, RollbackFileV1>,
}

pub fn plan_v1_to_v2(
    root: impl AsRef<Path>,
    options: MigrationRunOptionsV2,
) -> Result<MigrationPlanV2> {
    let root = absolute_existing_root(root.as_ref())?;
    let inspection = match inspect_root(&root)? {
        RootAuthority::LegacyV1(inspection) => *inspection,
        RootAuthority::Empty => {
            return Err(DbError::new(
                "55000",
                "cluster root is empty; there is no v1 database to migrate",
            ));
        }
        RootAuthority::V2(_) => {
            return Err(DbError::new(
                "55000",
                "cluster root already has an active v2 cluster",
            ));
        }
    };
    let snapshot = snapshot_from_persistent(&inspection.persistent_state);
    let candidate_storage =
        DatabaseStore::estimate_state(&inspection.persistent_state, DataFormat::V2)?;
    let logical_archive_bytes = estimate_snapshot_archive_bytes(&snapshot, options.archive_limits)?;
    let mut incompatibilities = Vec::new();
    let database_name = inspection.catalog.database().name.to_string();
    if database_name != DEFAULT_DATABASE_NAME {
        incompatibilities.push(format!(
            "legacy database name {database_name:?} must be {DEFAULT_DATABASE_NAME:?}"
        ));
    }

    let wal_path = root.join(LEGACY_WAL_FILE);
    let wal_file_bytes = optional_file_bytes(&wal_path)?;
    let (wal_record_count, maximum_transaction_id, next_transaction_id) =
        match inspect_wal_read_only(&root) {
            Ok(wal) => {
                let next = wal
                    .max_transaction_id
                    .map(|transaction_id| transaction_id.get())
                    .unwrap_or(2)
                    .checked_add(1);
                match next {
                    Some(next) => (
                        u64::try_from(wal.record_count)
                            .map_err(|_| resource_limit("WAL record count exceeds u64"))?,
                        wal.max_transaction_id
                            .map(|transaction_id| transaction_id.get()),
                        next,
                    ),
                    None => {
                        incompatibilities
                            .push("legacy WAL transaction ID space is exhausted".to_owned());
                        (
                            u64::try_from(wal.record_count)
                                .map_err(|_| resource_limit("WAL record count exceeds u64"))?,
                            wal.max_transaction_id
                                .map(|transaction_id| transaction_id.get()),
                            3,
                        )
                    }
                }
            }
            Err(error) => {
                incompatibilities.push(format!(
                    "legacy WAL cannot be inspected read-only: {} ({})",
                    error.message, error.sql_state
                ));
                (0, None, 3)
            }
        };
    let auth_path = root.join(AUTH_FILE_NAME);
    let auth_file_bytes = optional_file_bytes(&auth_path)?;
    let data_file_sha256 = hash_file(&root.join(LEGACY_DATA_FILE))?;
    let wal_file_sha256 = optional_file_sha256(&wal_path)?;
    let auth_file_sha256 = optional_file_sha256(&auth_path)?;
    let schema_count = usize_to_u64(inspection.catalog.database().schemas().count(), "schema")?;
    let table_count = usize_to_u64(inspection.table_rows.len(), "table")?;
    let index_count = inspection
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .map(|table| table.indexes().count())
        .try_fold(0_u64, |total, count| {
            total
                .checked_add(usize_to_u64(count, "index")?)
                .ok_or_else(|| resource_limit("index count overflowed"))
        })?;
    let row_count = inspection
        .table_rows
        .values()
        .try_fold(0_u64, |total, rows| {
            total
                .checked_add(*rows)
                .ok_or_else(|| resource_limit("row count overflowed"))
        })?;
    let inventory = MigrationInventoryV2 {
        database_name,
        source_generation: inspection.generation,
        schema_count,
        table_count,
        index_count,
        row_count,
        data_file_bytes: inspection.file_bytes,
        wal_file_bytes,
        auth_file_bytes,
        data_file_sha256,
        wal_file_sha256,
        auth_file_sha256,
        wal_record_count,
        maximum_transaction_id,
    };

    let migration_id = Uuid::new_v4();
    let cluster_id = Uuid::new_v4();
    let database_id = Uuid::new_v4();
    let planned_at = truncate_to_seconds(Utc::now())?;
    let migration_root = root.join("migration");
    let logical_backup = migration_root
        .join("backups")
        .join(format!("{migration_id}.ordbak"));
    let candidate_cluster = migration_root
        .join("candidates")
        .join(migration_id.to_string())
        .join("cluster");
    let published_cluster = root.join("clusters").join(cluster_id.to_string());
    let rollback_directory = root.join("rollback").join(format!("v1-{migration_id}"));
    let paths = MigrationPathsV2 {
        cluster_root: root.clone(),
        logical_backup,
        candidate_cluster,
        published_cluster,
        rollback_directory,
        journal: migration_journal_path(&root)?,
        active_pointer: root.join(ACTIVE_POINTER_FILE),
    };
    let rollback_copy_bytes = inspection
        .file_bytes
        .checked_add(wal_file_bytes)
        .and_then(|bytes| bytes.checked_add(auth_file_bytes))
        .ok_or_else(|| resource_limit("rollback byte requirement overflowed"))?;
    let rollback_manifest = rollback_manifest_from_inventory(migration_id, planned_at, &inventory)?;
    let rollback_manifest_bytes =
        serialized_pretty_len(&rollback_manifest, "failed to estimate rollback manifest")?;
    let rollback_relative_path = relative_forward(&root, &paths.rollback_directory)?;
    let cluster_documents = estimate_cluster_documents_v2(
        cluster_id,
        database_id,
        inspection.catalog.database().id.get(),
        next_transaction_id,
        planned_at,
        Some(MigrationProvenanceV2 {
            migration_id,
            source_format_version: 1,
            source_generation: inspection.generation,
            logical_backup_sha256: "0".repeat(64),
            activation_generation: inspection.generation,
            rollback_relative_path,
        }),
    )?;
    let persistent_bytes_without_journal = logical_archive_bytes
        .checked_add(candidate_storage.file_bytes)
        .and_then(|bytes| bytes.checked_add(auth_file_bytes))
        .and_then(|bytes| bytes.checked_add(rollback_copy_bytes))
        .and_then(|bytes| bytes.checked_add(rollback_manifest_bytes))
        .and_then(|bytes| bytes.checked_add(cluster_documents.total_bytes))
        .ok_or_else(|| resource_limit("migration byte requirement overflowed"))?;
    let available_bytes = options
        .available_bytes_override
        .unwrap_or(available_space(&root)?);
    let plan = MigrationPlanV2 {
        format_version: MIGRATION_FORMAT_VERSION,
        migration_id,
        cluster_id,
        database_id,
        planned_at,
        source_format_version: 1,
        target_format_version: 2,
        inventory,
        candidate_storage,
        next_transaction_id,
        paths,
        bytes: MigrationBytesV2 {
            logical_archive_bytes,
            candidate_database_bytes: candidate_storage.file_bytes,
            candidate_role_bytes: auth_file_bytes,
            rollback_copy_bytes,
            rollback_manifest_bytes,
            cluster_document_bytes: cluster_documents.total_bytes,
            migration_journal_bytes: 0,
            atomic_temporary_bytes: 0,
            required_bytes: persistent_bytes_without_journal,
            available_bytes,
        },
        incompatibilities,
        phases: MigrationPhaseV2::ordered().to_vec(),
    };
    finalize_exact_byte_plan(plan, cluster_documents)
}

fn finalize_exact_byte_plan(
    mut plan: MigrationPlanV2,
    cluster_documents: ordadb_cluster::ClusterDocumentEstimateV2,
) -> Result<MigrationPlanV2> {
    for _ in 0..16 {
        let journal_bytes = MigrationPhaseV2::ordered()
            .into_iter()
            .map(|phase| {
                serialized_pretty_len(
                    &MigrationJournalV2 {
                        format_version: MIGRATION_FORMAT_VERSION,
                        migration_id: plan.migration_id,
                        phase,
                        updated_at: plan.planned_at,
                        plan: plan.clone(),
                    },
                    "failed to estimate migration journal",
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let migration_journal_bytes = journal_bytes.iter().copied().max().unwrap_or(0);
        let archive_payload_bytes = plan
            .bytes
            .logical_archive_bytes
            .checked_sub(ARCHIVE_HEADER_BYTES)
            .ok_or_else(|| corrupt("logical archive estimate is smaller than its header"))?;
        let cluster_without_pointer = cluster_documents
            .total_bytes
            .checked_sub(cluster_documents.active_pointer_bytes)
            .ok_or_else(|| corrupt("cluster document estimate is internally inconsistent"))?;
        let archive_bytes = plan.bytes.logical_archive_bytes;
        let candidate_base = checked_sum(
            &[
                archive_bytes,
                plan.bytes.candidate_database_bytes,
                plan.bytes.candidate_role_bytes,
                cluster_without_pointer,
            ],
            "candidate migration byte peak",
        )?;
        let rollback_base = checked_sum(
            &[
                candidate_base,
                plan.bytes.rollback_copy_bytes,
                plan.bytes.rollback_manifest_bytes,
            ],
            "rollback migration byte peak",
        )?;
        let pointer_base = checked_sum(
            &[rollback_base, cluster_documents.active_pointer_bytes],
            "pointer migration byte peak",
        )?;
        let mut peaks = vec![
            journal_bytes[0],
            checked_sum(
                &[journal_bytes[0], archive_bytes, archive_payload_bytes],
                "logical archive write peak",
            )?,
            checked_sum(
                &[archive_bytes, journal_bytes[0], journal_bytes[1]],
                "logical backup journal peak",
            )?,
        ];
        for index in 2..=4 {
            peaks.push(checked_sum(
                &[
                    candidate_base,
                    journal_bytes[index - 1],
                    journal_bytes[index],
                ],
                "candidate journal peak",
            )?);
        }
        for index in 5..=6 {
            peaks.push(checked_sum(
                &[
                    rollback_base,
                    journal_bytes[index - 1],
                    journal_bytes[index],
                ],
                "rollback journal peak",
            )?);
        }
        for index in 7..=8 {
            peaks.push(checked_sum(
                &[pointer_base, journal_bytes[index - 1], journal_bytes[index]],
                "pointer journal peak",
            )?);
        }
        let required_bytes = peaks
            .into_iter()
            .max()
            .ok_or_else(|| internal("migration phase list is empty"))?;
        let atomic_temporary_bytes = migration_journal_bytes.max(archive_payload_bytes);
        if plan.bytes.migration_journal_bytes == migration_journal_bytes
            && plan.bytes.atomic_temporary_bytes == atomic_temporary_bytes
            && plan.bytes.required_bytes == required_bytes
        {
            return Ok(plan);
        }
        plan.bytes.migration_journal_bytes = migration_journal_bytes;
        plan.bytes.atomic_temporary_bytes = atomic_temporary_bytes;
        plan.bytes.required_bytes = required_bytes;
    }
    Err(internal(
        "migration journal byte estimate did not reach a fixed point",
    ))
}

fn checked_sum(values: &[u64], context: &str) -> Result<u64> {
    values.iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| resource_limit(format!("{context} overflowed")))
    })
}

fn rollback_manifest_from_inventory(
    migration_id: Uuid,
    created_at: DateTime<Utc>,
    inventory: &MigrationInventoryV2,
) -> Result<RollbackManifestV1> {
    let mut files = BTreeMap::from([(
        LEGACY_DATA_FILE.to_owned(),
        RollbackFileV1 {
            bytes: inventory.data_file_bytes,
            sha256: inventory.data_file_sha256.clone(),
        },
    )]);
    for (name, bytes, sha256) in [
        (
            LEGACY_WAL_FILE,
            inventory.wal_file_bytes,
            inventory.wal_file_sha256.as_ref(),
        ),
        (
            AUTH_FILE_NAME,
            inventory.auth_file_bytes,
            inventory.auth_file_sha256.as_ref(),
        ),
    ] {
        match (bytes, sha256) {
            (0, None) => {}
            (_, Some(sha256)) => {
                files.insert(
                    name.to_owned(),
                    RollbackFileV1 {
                        bytes,
                        sha256: sha256.clone(),
                    },
                );
            }
            _ => {
                return Err(corrupt(format!(
                    "migration inventory for {name} has inconsistent bytes and checksum"
                )));
            }
        }
    }
    Ok(RollbackManifestV1 {
        format_version: 1,
        migration_id,
        created_at,
        files,
    })
}

fn serialized_pretty_len<T: Serialize>(value: &T, context: &str) -> Result<u64> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| internal(format!("{context}: {error}")))?;
    u64::try_from(bytes.len()).map_err(|_| resource_limit(format!("{context}: length exceeds u64")))
}

pub fn migrate_v1_to_v2(
    root: impl AsRef<Path>,
    options: MigrationRunOptionsV2,
) -> Result<MigrationReportV2> {
    migrate_v1_to_v2_with_faults(root, options, &NoMigrationFaults)
}

pub fn migrate_v1_to_v2_with_faults(
    root: impl AsRef<Path>,
    options: MigrationRunOptionsV2,
    faults: &dyn MigrationFaultInjector,
) -> Result<MigrationReportV2> {
    let plan = plan_v1_to_v2(root, options)?;
    apply_migration_plan_v2_with_faults(plan, options, faults)
}

pub fn apply_migration_plan_v2(
    plan: MigrationPlanV2,
    options: MigrationRunOptionsV2,
) -> Result<MigrationReportV2> {
    apply_migration_plan_v2_with_faults(plan, options, &NoMigrationFaults)
}

fn apply_migration_plan_v2_with_faults(
    plan: MigrationPlanV2,
    options: MigrationRunOptionsV2,
    faults: &dyn MigrationFaultInjector,
) -> Result<MigrationReportV2> {
    validate_migration_plan(&plan)?;
    if !plan.incompatibilities.is_empty() {
        return Err(
            DbError::new("0A000", "legacy database has migration incompatibilities")
                .with_detail(plan.incompatibilities.join("; "))
                .with_hint("resolve every dry-run incompatibility before migration"),
        );
    }
    if plan.bytes.available_bytes < plan.bytes.required_bytes {
        return Err(
            DbError::new("53100", "insufficient disk space for storage migration")
                .with_detail(format!(
                    "required {} bytes, available {} bytes",
                    plan.bytes.required_bytes, plan.bytes.available_bytes
                ))
                .with_hint("free space on the cluster volume and rerun the dry-run"),
        );
    }
    let current_available_bytes = options
        .available_bytes_override
        .unwrap_or(available_space(&plan.paths.cluster_root)?);
    if current_available_bytes < plan.bytes.required_bytes {
        return Err(
            DbError::new("53100", "insufficient disk space for storage migration")
                .with_detail(format!(
                    "required {} bytes, currently available {} bytes",
                    plan.bytes.required_bytes, current_available_bytes
                ))
                .with_hint("free space on the cluster volume and rerun installer preflight"),
        );
    }
    let root = &plan.paths.cluster_root;
    let inspection = match inspect_root(root)? {
        RootAuthority::LegacyV1(inspection) => *inspection,
        _ => {
            return Err(DbError::new(
                "55000",
                "legacy v1 authority changed after migration planning",
            ));
        }
    };
    validate_source_unchanged(root, &plan, &inspection)?;
    let snapshot = snapshot_from_persistent(&inspection.persistent_state);
    persist_phase(&plan, MigrationPhaseV2::Inspected, faults)?;

    let backup_parent =
        plan.paths.logical_backup.parent().ok_or_else(|| {
            DbError::new("22023", "logical backup path must have a parent directory")
        })?;
    fs::create_dir_all(backup_parent)
        .map_err(|error| io_error("failed to create logical backup directory", error))?;
    let backup = write_snapshot_archive(
        snapshot.clone(),
        &plan.paths.logical_backup,
        options.archive_limits,
    )?;
    if backup.bytes != plan.bytes.logical_archive_bytes {
        return Err(corrupt(format!(
            "logical backup wrote {} bytes; dry-run planned {}",
            backup.bytes, plan.bytes.logical_archive_bytes
        )));
    }
    let archived_snapshot =
        read_archive(&plan.paths.logical_backup, options.archive_limits)?.into_snapshot();
    if archived_snapshot != snapshot {
        return Err(corrupt(
            "logical backup snapshot does not match the read-only v1 inventory",
        ));
    }
    persist_phase(&plan, MigrationPhaseV2::LogicalBackupWritten, faults)?;

    let layout = prepare_cluster_layout(
        &plan.paths.candidate_cluster,
        plan.cluster_id,
        plan.database_id,
        plan.next_transaction_id,
    )?;
    copy_optional_file(
        &root.join(AUTH_FILE_NAME),
        &layout.roles_dir.join(AUTH_FILE_NAME),
    )?;
    {
        let mut store = DatabaseStore::open_with_format(&layout.database_dir, DataFormat::V2)?;
        store.commit(&inspection.persistent_state)?;
    }
    let candidate_inspection = DatabaseStore::inspect_read_only(&layout.database_dir)?;
    validate_candidate_storage(&plan, &inspection.persistent_state, &candidate_inspection)?;
    let rollback_relative_path = relative_forward(root, &plan.paths.rollback_directory)?;
    let provenance = MigrationProvenanceV2 {
        migration_id: plan.migration_id,
        source_format_version: plan.source_format_version,
        source_generation: plan.inventory.source_generation,
        logical_backup_sha256: backup.sha256.clone(),
        activation_generation: candidate_inspection.generation,
        rollback_relative_path,
    };
    finalize_cluster_layout(
        &layout,
        FinalizeClusterOptions {
            database_name: DEFAULT_DATABASE_NAME.to_owned(),
            catalog_database_id: candidate_inspection.catalog.database().id.get(),
            created_at: plan.planned_at,
            migration: Some(provenance),
        },
    )?;
    persist_phase(&plan, MigrationPhaseV2::CandidateBuilt, faults)?;

    let validated = validate_cluster_directory(&layout.cluster_dir)?;
    validate_candidate_storage(&plan, &inspection.persistent_state, &validated.storage)?;
    persist_phase(&plan, MigrationPhaseV2::CandidateValidated, faults)?;

    let engine = Engine::open(EngineConfig::new(&layout.database_dir))?;
    let status = engine.status_snapshot()?;
    let reopened_snapshot = engine.logical_snapshot()?;
    if status.generation != candidate_inspection.generation
        || usize_to_u64(status.table_count, "reopened table")? != plan.inventory.table_count
        || status.row_count != plan.inventory.row_count
        || usize_to_u64(status.index_count, "reopened index")? != plan.inventory.index_count
        || reopened_snapshot.source_generation != snapshot.source_generation
        || reopened_snapshot.tables != snapshot.tables
        || reopened_snapshot.catalog.database().id != snapshot.catalog.database().id
        || reopened_snapshot.catalog.database().name != snapshot.catalog.database().name
    {
        return Err(corrupt(
            "reopened v2 candidate does not match the v1 logical snapshot",
        ));
    }
    drop(engine);
    persist_phase(&plan, MigrationPhaseV2::CandidateReopened, faults)?;

    retain_rollback(root, &plan)?;
    persist_phase(&plan, MigrationPhaseV2::RollbackRetained, faults)?;

    let published = publish_cluster_directory(root, &layout.cluster_dir, plan.cluster_id)?;
    if published != plan.paths.published_cluster {
        return Err(corrupt(
            "published cluster path does not match the migration plan",
        ));
    }
    persist_phase(&plan, MigrationPhaseV2::CandidatePublished, faults)?;

    activate_published_cluster(root, &published)?;
    let active = resolve_active_v2(root)?;
    if active.cluster_manifest.cluster_id != plan.cluster_id
        || active.database_manifest.database_id != plan.database_id
        || active.storage.generation != candidate_inspection.generation
    {
        return Err(corrupt(
            "active v2 cluster does not match the validated migration candidate",
        ));
    }
    let cluster_document_bytes = [
        active.database_dir.join(TRANSACTION_STATE_FILE),
        active.database_dir.join(DATABASE_MANIFEST_FILE),
        active.cluster_dir.join(CLUSTER_MANIFEST_FILE),
        root.join(ACTIVE_POINTER_FILE),
    ]
    .into_iter()
    .try_fold(0_u64, |total, path| {
        total
            .checked_add(
                fs::metadata(&path)
                    .map_err(|error| io_error("failed to inspect cluster document", error))?
                    .len(),
            )
            .ok_or_else(|| resource_limit("cluster document byte count overflowed"))
    })?;
    if cluster_document_bytes != plan.bytes.cluster_document_bytes {
        return Err(corrupt(
            "published cluster document bytes do not match the dry-run plan",
        ));
    }
    persist_phase(&plan, MigrationPhaseV2::PointerSwitched, faults)?;
    persist_phase(&plan, MigrationPhaseV2::Completed, faults)?;

    Ok(MigrationReportV2 {
        migration_id: plan.migration_id,
        cluster_id: plan.cluster_id,
        database_id: plan.database_id,
        completed_at: truncate_to_seconds(Utc::now())?,
        source_generation: plan.inventory.source_generation,
        activation_generation: active.storage.generation,
        table_count: plan.inventory.table_count,
        row_count: plan.inventory.row_count,
        backup,
        paths: plan.paths,
        final_phase: MigrationPhaseV2::Completed,
    })
}

pub(crate) fn validate_migration_plan(plan: &MigrationPlanV2) -> Result<()> {
    if plan.format_version != MIGRATION_FORMAT_VERSION {
        return Err(DbError::new(
            "0A000",
            format!(
                "migration plan format version {} is not supported",
                plan.format_version
            ),
        )
        .with_hint("rerun installer storage preflight with this OrdaDB build"));
    }
    if plan.source_format_version != 1 || plan.target_format_version != 2 {
        return Err(DbError::new(
            "0A000",
            "migration plan source or target format is not supported",
        ));
    }
    if plan.phases != MigrationPhaseV2::ordered() {
        return Err(corrupt("migration plan phase sequence is invalid"));
    }
    let root = absolute_existing_root(&plan.paths.cluster_root)?;
    if root != plan.paths.cluster_root {
        return Err(invalid(
            "migration plan cluster root is not the exact normalized data directory",
        ));
    }
    let migration_root = root.join("migration");
    let expected = MigrationPathsV2 {
        cluster_root: root.clone(),
        logical_backup: migration_root
            .join("backups")
            .join(format!("{}.ordbak", plan.migration_id)),
        candidate_cluster: migration_root
            .join("candidates")
            .join(plan.migration_id.to_string())
            .join("cluster"),
        published_cluster: root.join("clusters").join(plan.cluster_id.to_string()),
        rollback_directory: root
            .join("rollback")
            .join(format!("v1-{}", plan.migration_id)),
        journal: migration_journal_path(&root)?,
        active_pointer: root.join(ACTIVE_POINTER_FILE),
    };
    if plan.paths != expected {
        return Err(invalid(
            "migration plan contains a path outside its canonical installer layout",
        ));
    }
    if plan.candidate_storage.data_format != DataFormat::V2 {
        return Err(corrupt(
            "migration plan candidate storage does not use database format v2",
        ));
    }
    for (label, digest) in [
        ("data", Some(plan.inventory.data_file_sha256.as_str())),
        ("WAL", plan.inventory.wal_file_sha256.as_deref()),
        ("auth", plan.inventory.auth_file_sha256.as_deref()),
    ] {
        if let Some(digest) = digest
            && (digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        {
            return Err(corrupt(format!(
                "migration plan {label} fingerprint is invalid"
            )));
        }
    }
    if plan.incompatibilities.len() > 64
        || plan
            .incompatibilities
            .iter()
            .any(|value| value.len() > 1024)
    {
        return Err(DbError::new(
            "54000",
            "migration plan incompatibility list exceeds installer bounds",
        ));
    }
    Ok(())
}

pub fn rollback_v2_to_v1(root: impl AsRef<Path>) -> Result<()> {
    remove_active_pointer_for_rollback(root)
}

fn persist_phase(
    plan: &MigrationPlanV2,
    phase: MigrationPhaseV2,
    faults: &dyn MigrationFaultInjector,
) -> Result<()> {
    let updated_at = truncate_to_seconds(Utc::now())?;
    let journal = MigrationJournalV2 {
        format_version: MIGRATION_FORMAT_VERSION,
        migration_id: plan.migration_id,
        phase,
        updated_at,
        plan: plan.clone(),
    };
    let expected_bytes = serialized_pretty_len(&journal, "failed to encode migration journal")?;
    if expected_bytes > plan.bytes.migration_journal_bytes {
        return Err(corrupt("migration journal exceeds the dry-run byte plan"));
    }
    write_migration_journal(&plan.paths.cluster_root, &journal)?;
    if fs::metadata(&plan.paths.journal)
        .map_err(|error| io_error("failed to inspect migration journal", error))?
        .len()
        != expected_bytes
    {
        return Err(corrupt(
            "persisted migration journal length does not match its encoding",
        ));
    }
    faults.check(phase)
}

fn snapshot_from_persistent(state: &PersistentState) -> LogicalDatabaseSnapshot {
    LogicalDatabaseSnapshot {
        format_version: LOGICAL_SNAPSHOT_VERSION,
        source_generation: state.generation,
        catalog: Arc::new(state.catalog.clone()),
        tables: state
            .tables
            .iter()
            .map(|(table_id, rows)| (*table_id, Arc::new(rows.clone())))
            .collect(),
    }
}

fn validate_source_unchanged(
    root: &Path,
    plan: &MigrationPlanV2,
    inspection: &ordadb_storage::StorageInspection,
) -> Result<()> {
    let wal = inspect_wal_read_only(root)?;
    let wal_record_count = u64::try_from(wal.record_count)
        .map_err(|_| resource_limit("WAL record count exceeds u64"))?;
    let row_count = inspection
        .table_rows
        .values()
        .try_fold(0_u64, |total, rows| {
            total
                .checked_add(*rows)
                .ok_or_else(|| resource_limit("source row count overflowed"))
        })?;
    let unchanged = inspection.data_format == DataFormat::V1
        && inspection.generation == plan.inventory.source_generation
        && inspection.file_bytes == plan.inventory.data_file_bytes
        && usize_to_u64(inspection.table_rows.len(), "source table")? == plan.inventory.table_count
        && row_count == plan.inventory.row_count
        && hash_file(&root.join(LEGACY_DATA_FILE))? == plan.inventory.data_file_sha256
        && wal.file_bytes == plan.inventory.wal_file_bytes
        && wal_record_count == plan.inventory.wal_record_count
        && wal
            .max_transaction_id
            .map(|transaction_id| transaction_id.get())
            == plan.inventory.maximum_transaction_id
        && optional_file_sha256(&root.join(LEGACY_WAL_FILE))? == plan.inventory.wal_file_sha256
        && optional_file_bytes(&root.join(AUTH_FILE_NAME))? == plan.inventory.auth_file_bytes
        && optional_file_sha256(&root.join(AUTH_FILE_NAME))? == plan.inventory.auth_file_sha256;
    if unchanged {
        Ok(())
    } else {
        Err(DbError::new(
            "55000",
            "legacy migration inputs changed after dry-run planning",
        )
        .with_hint("stop every writer and rerun the storage migration dry-run"))
    }
}

fn validate_candidate_storage(
    plan: &MigrationPlanV2,
    source: &PersistentState,
    candidate: &ordadb_storage::StorageInspection,
) -> Result<()> {
    if candidate.data_format != DataFormat::V2
        || candidate.generation != source.generation
        || candidate.catalog != source.catalog
        || candidate.persistent_state.tables != source.tables
        || !candidate.persistent_state.indexes.is_empty()
        || candidate.file_bytes != plan.candidate_storage.file_bytes
        || candidate.page_count != plan.candidate_storage.page_count
    {
        return Err(corrupt(
            "v2 candidate storage does not match its dry-run and v1 source",
        ));
    }
    let row_count = candidate
        .table_rows
        .values()
        .try_fold(0_u64, |total, rows| {
            total
                .checked_add(*rows)
                .ok_or_else(|| resource_limit("candidate row count overflowed"))
        })?;
    let table_count = usize_to_u64(candidate.table_rows.len(), "candidate table")?;
    if row_count != plan.inventory.row_count || table_count != plan.inventory.table_count {
        return Err(corrupt(
            "v2 candidate logical row or table counts do not match v1",
        ));
    }
    Ok(())
}

fn retain_rollback(root: &Path, plan: &MigrationPlanV2) -> Result<()> {
    if plan.paths.rollback_directory.exists() {
        return Err(DbError::new(
            "55000",
            "migration rollback directory already exists",
        ));
    }
    fs::create_dir_all(&plan.paths.rollback_directory)
        .map_err(|error| io_error("failed to create rollback directory", error))?;
    let manifest =
        rollback_manifest_from_inventory(plan.migration_id, plan.planned_at, &plan.inventory)?;
    for (name, expected) in &manifest.files {
        let source = root.join(name);
        let destination = plan.paths.rollback_directory.join(name);
        copy_file_durable(&source, &destination)?;
        let bytes = fs::metadata(&destination)
            .map_err(|error| io_error("failed to inspect retained rollback file", error))?
            .len();
        let sha256 = hash_file(&destination)?;
        if bytes != expected.bytes || sha256 != expected.sha256 {
            return Err(corrupt("retained rollback file does not match its source"));
        }
    }
    let bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|error| internal(format!("failed to encode rollback manifest: {error}")))?;
    if u64::try_from(bytes.len())
        .map_err(|_| resource_limit("rollback manifest length exceeds u64"))?
        != plan.bytes.rollback_manifest_bytes
    {
        return Err(corrupt(
            "rollback manifest length does not match the dry-run plan",
        ));
    }
    let path = plan
        .paths
        .rollback_directory
        .join("rollback-manifest-v1.json");
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| io_error("failed to create rollback manifest", error))?;
    file.write_all(&bytes)
        .map_err(|error| io_error("failed to write rollback manifest", error))?;
    file.sync_all()
        .map_err(|error| io_error("failed to synchronize rollback manifest", error))
}

fn copy_optional_file(source: &Path, destination: &Path) -> Result<()> {
    if !source.exists() {
        return Ok(());
    }
    copy_file_durable(source, destination)
}

fn copy_file_durable(source: &Path, destination: &Path) -> Result<()> {
    let parent = destination
        .parent()
        .ok_or_else(|| internal("copy destination has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error("failed to create copy destination directory", error))?;
    let mut input =
        File::open(source).map_err(|error| io_error("failed to open copy source", error))?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .map_err(|error| io_error("failed to create copy destination", error))?;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = input
            .read(&mut buffer)
            .map_err(|error| io_error("failed to read copy source", error))?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .map_err(|error| io_error("failed to write copy destination", error))?;
    }
    output
        .sync_all()
        .map_err(|error| io_error("failed to synchronize copied file", error))
}

pub(crate) fn optional_file_bytes(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(metadata.len()),
        Ok(_) => Err(invalid(format!(
            "migration source {} is not a regular file",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(io_error("failed to inspect optional migration file", error)),
    }
}

pub(crate) fn optional_file_sha256(path: &Path) -> Result<Option<String>> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => hash_file(path).map(Some),
        Ok(_) => Err(DbError::new(
            "22023",
            format!("migration input {} is not a file", path.display()),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("failed to inspect optional migration file", error)),
    }
}

pub(crate) fn hash_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).map_err(|error| io_error("failed to open migration file", error))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| io_error("failed to hash migration file", error))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = digest.finalize();
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

fn relative_forward(root: &Path, child: &Path) -> Result<String> {
    let relative = child
        .strip_prefix(root)
        .map_err(|_| invalid("migration path is outside its cluster root"))?;
    let parts = relative
        .components()
        .map(|component| {
            component
                .as_os_str()
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid("migration path is not UTF-8"))
        })
        .collect::<Result<Vec<_>>>()?;
    if parts.is_empty()
        || parts
            .iter()
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(invalid("migration relative path is invalid"));
    }
    Ok(parts.join("/"))
}

fn absolute_existing_root(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| io_error("failed to resolve current directory", error))?
            .join(path)
    };
    path.canonicalize()
        .map_err(|error| io_error("failed to resolve migration cluster root", error))
}

fn truncate_to_seconds(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(value.timestamp(), 0)
        .ok_or_else(|| internal("migration timestamp is out of range"))
}

fn usize_to_u64(value: usize, label: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| resource_limit(format!("{label} count exceeds u64")))
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn resource_limit(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn corrupt(message: impl Into<String>) -> DbError {
    DbError::new("XX001", message)
        .with_hint("retain v1 authority and restore from the verified logical backup")
}

fn internal(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message)
}

fn io_error(context: impl Into<String>, error: std::io::Error) -> DbError {
    DbError::new("58030", context)
        .with_detail(error.to_string())
        .with_hint("check the cluster path, permissions, and available disk space")
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use ordadb_catalog::NewColumn;
    use ordadb_cluster::{RootAuthority, inspect_root};
    use ordadb_storage::DatabaseStore;
    use ordadb_transaction::{TransactionId, WalManager, WalPayload};
    use ordadb_types::{Identifier, Row, ScalarType, Value};
    use tempfile::tempdir;

    use super::*;

    #[derive(Debug)]
    struct FailAfter {
        target: MigrationPhaseV2,
        seen: Mutex<Vec<MigrationPhaseV2>>,
    }

    impl MigrationFaultInjector for FailAfter {
        fn check(&self, phase: MigrationPhaseV2) -> Result<()> {
            self.seen.lock().expect("seen").push(phase);
            if phase == self.target {
                return Err(DbError::new(
                    "58030",
                    format!("injected migration failure after {phase:?}"),
                ));
            }
            Ok(())
        }
    }

    fn legacy_root() -> (tempfile::TempDir, PathBuf) {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("data");
        let mut store = DatabaseStore::open(&root).expect("v1 store");
        let mut state = store.committed_state().clone();
        let table_id = state
            .catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("migration_items"),
                vec![
                    NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                    NewColumn::new(Identifier::unquoted("payload"), ScalarType::Text),
                ],
            )
            .expect("legacy table");
        state.tables.insert(
            table_id,
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("legacy one".into())]),
                Row::new(vec![Value::Int64(2), Value::Text("legacy two".into())]),
            ],
        );
        state.generation = 5;
        store.commit(&state).expect("v1 generation");
        drop(store);
        (directory, root)
    }

    #[test]
    fn dry_run_is_non_mutating_and_reports_exact_components() {
        let (_directory, root) = legacy_root();
        let before = fs::read(root.join(LEGACY_DATA_FILE)).expect("before");
        let plan = plan_v1_to_v2(
            &root,
            MigrationRunOptionsV2 {
                available_bytes_override: Some(u64::MAX),
                ..MigrationRunOptionsV2::default()
            },
        )
        .expect("plan");
        assert!(plan.incompatibilities.is_empty());
        assert_eq!(plan.inventory.source_generation, 5);
        assert_eq!(plan.inventory.table_count, 1);
        assert_eq!(plan.inventory.row_count, 2);
        assert_eq!(plan.candidate_storage.data_format, DataFormat::V2);
        let retained_without_journal = plan.bytes.logical_archive_bytes
            + plan.bytes.candidate_database_bytes
            + plan.bytes.candidate_role_bytes
            + plan.bytes.rollback_copy_bytes
            + plan.bytes.rollback_manifest_bytes
            + plan.bytes.cluster_document_bytes;
        assert!(
            plan.bytes.required_bytes > retained_without_journal,
            "the exact peak must include a durable journal and its atomic replacement overlap"
        );
        assert!(plan.bytes.migration_journal_bytes > 0);
        assert!(plan.bytes.atomic_temporary_bytes > 0);
        assert_eq!(
            fs::read(root.join(LEGACY_DATA_FILE)).expect("after"),
            before
        );
        assert!(!plan.paths.journal.exists());
        assert!(!plan.paths.logical_backup.exists());
    }

    #[test]
    fn dry_run_preserves_and_carries_forward_the_legacy_wal_high_water_mark() {
        let (_directory, root) = legacy_root();
        let wal = WalManager::open(&root).expect("legacy WAL");
        let transaction_id = TransactionId::new(41).expect("transaction ID");
        let begin = wal
            .append(Some(transaction_id), None, WalPayload::Begin)
            .expect("append Begin");
        let commit = wal
            .append(Some(transaction_id), Some(begin), WalPayload::Commit)
            .expect("append Commit");
        wal.flush_lsn(commit).expect("flush transaction");
        drop(wal);
        let path = root.join(LEGACY_WAL_FILE);
        let before = fs::read(&path).expect("WAL before dry-run");

        let plan = plan_v1_to_v2(
            &root,
            MigrationRunOptionsV2 {
                available_bytes_override: Some(u64::MAX),
                ..MigrationRunOptionsV2::default()
            },
        )
        .expect("plan");

        assert_eq!(plan.inventory.maximum_transaction_id, Some(41));
        assert_eq!(plan.next_transaction_id, 42);
        assert_eq!(fs::read(path).expect("WAL after dry-run"), before);
    }

    #[test]
    fn changed_inputs_are_rejected_before_the_first_journal_write() {
        let (_directory, root) = legacy_root();
        let plan = plan_v1_to_v2(
            &root,
            MigrationRunOptionsV2 {
                available_bytes_override: Some(u64::MAX),
                ..MigrationRunOptionsV2::default()
            },
        )
        .expect("plan");
        fs::write(root.join(AUTH_FILE_NAME), b"changed after dry-run").expect("change auth");
        let RootAuthority::LegacyV1(inspection) = inspect_root(&root).expect("legacy") else {
            panic!("expected legacy authority");
        };

        let error =
            validate_source_unchanged(&root, &plan, &inspection).expect_err("change refused");

        assert_eq!(error.sql_state, "55000");
        assert!(!plan.paths.journal.exists());
    }

    #[test]
    fn migrates_reopens_and_refuses_rollback_after_a_v2_write() {
        let (_directory, root) = legacy_root();
        let report = migrate_v1_to_v2(
            &root,
            MigrationRunOptionsV2 {
                available_bytes_override: Some(u64::MAX),
                ..MigrationRunOptionsV2::default()
            },
        )
        .expect("migrate");
        assert_eq!(report.final_phase, MigrationPhaseV2::Completed);
        let active = resolve_active_v2(&root).expect("active");
        assert_eq!(active.storage.generation, 5);
        assert_eq!(active.storage.table_rows.values().copied().sum::<u64>(), 2);
        assert!(report.backup.path.is_file());
        assert!(
            report
                .paths
                .rollback_directory
                .join(LEGACY_DATA_FILE)
                .is_file()
        );

        let engine = Engine::open(EngineConfig::new(&active.database_dir)).expect("engine");
        engine
            .replace_logical_snapshot(engine.logical_snapshot().expect("snapshot"))
            .expect("v2 write");
        drop(engine);
        assert_eq!(
            rollback_v2_to_v1(&root)
                .expect_err("rollback forbidden")
                .sql_state,
            "55000"
        );
    }

    #[test]
    fn rollback_restores_v1_authority_before_any_v2_write() {
        let (_directory, root) = legacy_root();
        migrate_v1_to_v2(
            &root,
            MigrationRunOptionsV2 {
                available_bytes_override: Some(u64::MAX),
                ..MigrationRunOptionsV2::default()
            },
        )
        .expect("migrate");

        rollback_v2_to_v1(&root).expect("rollback");

        assert!(!root.join(ACTIVE_POINTER_FILE).exists());
        let RootAuthority::LegacyV1(inspection) = inspect_root(&root).expect("legacy authority")
        else {
            panic!("rollback did not restore v1 authority");
        };
        assert_eq!(inspection.data_format, DataFormat::V1);
        assert_eq!(inspection.generation, 5);
    }

    #[test]
    fn every_injected_phase_leaves_exactly_one_authority() {
        for phase in MigrationPhaseV2::ordered() {
            let (_directory, root) = legacy_root();
            let faults = FailAfter {
                target: phase,
                seen: Mutex::new(Vec::new()),
            };
            let result = migrate_v1_to_v2_with_faults(
                &root,
                MigrationRunOptionsV2 {
                    available_bytes_override: Some(u64::MAX),
                    ..MigrationRunOptionsV2::default()
                },
                &faults,
            );
            assert!(result.is_err(), "{phase:?}");
            let authority = inspect_root(&root).expect("authority");
            if phase >= MigrationPhaseV2::PointerSwitched {
                assert!(matches!(authority, RootAuthority::V2(_)), "{phase:?}");
            } else {
                assert!(matches!(authority, RootAuthority::LegacyV1(_)), "{phase:?}");
            }
        }
    }

    #[test]
    fn rerun_after_a_pre_pointer_failure_uses_a_fresh_isolated_candidate() {
        let (_directory, root) = legacy_root();
        let faults = FailAfter {
            target: MigrationPhaseV2::CandidatePublished,
            seen: Mutex::new(Vec::new()),
        };
        migrate_v1_to_v2_with_faults(
            &root,
            MigrationRunOptionsV2 {
                available_bytes_override: Some(u64::MAX),
                ..MigrationRunOptionsV2::default()
            },
            &faults,
        )
        .expect_err("injected failure");
        assert!(matches!(
            inspect_root(&root).expect("legacy authority"),
            RootAuthority::LegacyV1(_)
        ));

        let report = migrate_v1_to_v2(
            &root,
            MigrationRunOptionsV2 {
                available_bytes_override: Some(u64::MAX),
                ..MigrationRunOptionsV2::default()
            },
        )
        .expect("rerun");

        assert_eq!(report.final_phase, MigrationPhaseV2::Completed);
        assert!(matches!(
            inspect_root(&root).expect("v2 authority"),
            RootAuthority::V2(_)
        ));
    }

    #[test]
    fn insufficient_space_fails_before_creating_the_journal() {
        let (_directory, root) = legacy_root();
        let error = migrate_v1_to_v2(
            &root,
            MigrationRunOptionsV2 {
                available_bytes_override: Some(1),
                ..MigrationRunOptionsV2::default()
            },
        )
        .expect_err("space");
        assert_eq!(error.sql_state, "53100");
        assert!(!migration_journal_path(&root).expect("journal").exists());
        assert!(matches!(
            inspect_root(&root).expect("authority"),
            RootAuthority::LegacyV1(_)
        ));
    }
}
