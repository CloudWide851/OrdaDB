use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use chrono::{DateTime, Utc};
use ordadb_storage::{
    DATABASE_FORMAT_V1, DataFormat, DatabaseStore, FILE_FORMAT_VERSION, IndexRebuildContractV2,
    StorageInspection, TUPLE_FORMAT_V2,
};
use ordadb_types::{DbError, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const CLUSTER_FORMAT_V2: u16 = 2;
pub const DATABASE_MANIFEST_FORMAT_V2: u16 = 2;
pub const TRANSACTION_STATE_FORMAT_V2: u16 = 2;
pub const AUTH_FORMAT_V1: u32 = 1;
pub const DEFAULT_DATABASE_NAME: &str = "ordadb";
pub const AUTH_FILE_NAME: &str = "ordadb.auth.json";
pub const ACTIVE_POINTER_FILE: &str = "active-cluster-v2.json";
pub const CLUSTER_MANIFEST_FILE: &str = "cluster-manifest-v2.json";
pub const DATABASE_MANIFEST_FILE: &str = "database-manifest-v2.json";
pub const TRANSACTION_STATE_FILE: &str = "transaction-state-v2.json";
pub const MIGRATION_JOURNAL_FILE: &str = "migration-v2.json";
pub const LEGACY_DATA_FILE: &str = "ordadb.data";
pub const LEGACY_WAL_FILE: &str = "ordadb.wal";
pub const INSTALLER_STORAGE_CLASSIFICATION_VERSION: u32 = 1;

const MAX_POINTER_BYTES: u64 = 64 * 1024;
const MAX_CLUSTER_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_DATABASE_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_TRANSACTION_STATE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_JOURNAL_BYTES: u64 = 4 * 1024 * 1024;
const FROZEN_TRANSACTION_ID: u64 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActiveClusterPointerV2 {
    pub format_version: u16,
    pub cluster_id: Uuid,
    pub relative_path: String,
    pub manifest_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DatabaseLifecycleV2 {
    Online,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterDatabaseEntryV2 {
    pub database_id: Uuid,
    pub relative_path: String,
    pub manifest_sha256: String,
    pub state: DatabaseLifecycleV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RoleCatalogContractV2 {
    pub relative_path: String,
    pub auth_file: String,
    pub auth_format_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterManifestV2 {
    pub format_version: u16,
    pub cluster_id: Uuid,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub active_database: String,
    pub roles: RoleCatalogContractV2,
    pub databases: BTreeMap<String, ClusterDatabaseEntryV2>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransactionStatusV2 {
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransactionStateV2 {
    pub format_version: u16,
    pub next_transaction_id: u64,
    pub statuses: BTreeMap<u64, TransactionStatusV2>,
}

impl TransactionStateV2 {
    #[must_use]
    pub fn bootstrap(next_transaction_id: u64) -> Self {
        Self {
            format_version: TRANSACTION_STATE_FORMAT_V2,
            next_transaction_id: next_transaction_id.max(FROZEN_TRANSACTION_ID + 1),
            statuses: BTreeMap::from([(FROZEN_TRANSACTION_ID, TransactionStatusV2::Committed)]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransactionStateContractV2 {
    pub format_version: u16,
    pub relative_path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MigrationProvenanceV2 {
    pub migration_id: Uuid,
    pub source_format_version: u16,
    pub source_generation: u64,
    pub logical_backup_sha256: String,
    pub activation_generation: u64,
    pub rollback_relative_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatabaseManifestV2 {
    pub format_version: u16,
    pub database_id: Uuid,
    pub database_name: String,
    pub catalog_database_id: u64,
    pub page_format_version: u16,
    pub tuple_format_version: u16,
    pub data_file: String,
    pub wal_file: String,
    pub transaction_state: TransactionStateContractV2,
    pub index_rebuild: IndexRebuildContractV2,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration: Option<MigrationProvenanceV2>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterLayoutV2 {
    pub cluster_id: Uuid,
    pub database_id: Uuid,
    pub cluster_dir: PathBuf,
    pub database_dir: PathBuf,
    pub roles_dir: PathBuf,
    pub transaction_state_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterDocumentEstimateV2 {
    pub transaction_state_bytes: u64,
    pub database_manifest_bytes: u64,
    pub cluster_manifest_bytes: u64,
    pub active_pointer_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedClusterV2 {
    pub root: PathBuf,
    pub cluster_dir: PathBuf,
    pub database_dir: PathBuf,
    pub roles_dir: PathBuf,
    pub pointer: ActiveClusterPointerV2,
    pub cluster_manifest: ClusterManifestV2,
    pub database_manifest: DatabaseManifestV2,
    pub transaction_state: TransactionStateV2,
    pub storage: StorageInspection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RootAuthority {
    Empty,
    LegacyV1(Box<StorageInspection>),
    V2(Box<ResolvedClusterV2>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InstallerStorageDisposition {
    Empty,
    LegacyV1,
    ActiveV2,
    Mixed,
    Corrupt,
    IncompleteMigration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstallerStorageMarkersV1 {
    pub legacy_data: bool,
    pub legacy_wal: bool,
    pub legacy_auth: bool,
    pub active_pointer: bool,
    pub clusters_directory: bool,
    pub migration_directory: bool,
    pub migration_journal: bool,
    pub top_level_entries: u64,
}

#[derive(Debug, Clone)]
pub struct InstallerStorageClassificationV1 {
    pub schema_version: u32,
    pub disposition: InstallerStorageDisposition,
    pub data_dir: PathBuf,
    pub markers: InstallerStorageMarkersV1,
    pub authority: Option<RootAuthority>,
    pub issue: Option<DbError>,
}

pub fn normalize_installer_data_dir(path: impl AsRef<Path>) -> Result<PathBuf> {
    let absolute = std::path::absolute(path.as_ref())
        .map_err(|error| io_error("failed to normalize installer data directory", error))?;
    if absolute.exists() {
        let canonical = absolute
            .canonicalize()
            .map_err(|error| io_error("failed to resolve installer data directory", error))?;
        if !canonical.is_dir() {
            return Err(invalid("installer data directory path is not a directory"));
        }
        return Ok(canonical);
    }

    let existing_ancestor = absolute
        .ancestors()
        .find(|candidate| candidate.exists())
        .ok_or_else(|| invalid("installer data directory has no existing ancestor"))?;
    let canonical_ancestor = existing_ancestor
        .canonicalize()
        .map_err(|error| io_error("failed to resolve installer data directory ancestor", error))?;
    if !canonical_ancestor.is_dir() {
        return Err(invalid(
            "installer data directory ancestor is not a directory",
        ));
    }
    let suffix = absolute
        .strip_prefix(existing_ancestor)
        .map_err(|_| internal("normalized installer data directory lost its ancestor"))?;
    if suffix
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid(
            "installer data directory contains an unsafe path component",
        ));
    }
    Ok(canonical_ancestor.join(suffix))
}

pub fn classify_installer_storage(
    root: impl AsRef<Path>,
) -> Result<InstallerStorageClassificationV1> {
    let data_dir = normalize_installer_data_dir(root)?;
    if !data_dir.exists() {
        return Ok(installer_classification(
            InstallerStorageDisposition::Empty,
            data_dir,
            InstallerStorageMarkersV1 {
                legacy_data: false,
                legacy_wal: false,
                legacy_auth: false,
                active_pointer: false,
                clusters_directory: false,
                migration_directory: false,
                migration_journal: false,
                top_level_entries: 0,
            },
            None,
            None,
        ));
    }

    let markers = installer_storage_markers(&data_dir)?;
    let journal_completed = match installer_journal_completed(&data_dir, &markers) {
        Ok(completed) => completed,
        Err(error) => {
            return Ok(installer_classification(
                InstallerStorageDisposition::Corrupt,
                data_dir,
                markers,
                None,
                Some(error),
            ));
        }
    };

    if markers.active_pointer {
        return match resolve_active_v2(&data_dir) {
            Ok(cluster) => {
                let disposition =
                    if markers.legacy_data && cluster.database_manifest.migration.is_none() {
                        InstallerStorageDisposition::Mixed
                    } else {
                        InstallerStorageDisposition::ActiveV2
                    };
                let issue = (disposition == InstallerStorageDisposition::Mixed).then(|| {
                    DbError::new(
                        "55000",
                        "legacy and independently initialized v2 authorities are both present",
                    )
                    .with_hint("select one authoritative source before retrying the installer")
                });
                Ok(installer_classification(
                    disposition,
                    data_dir,
                    markers,
                    Some(RootAuthority::V2(Box::new(cluster))),
                    issue,
                ))
            }
            Err(error) => Ok(installer_classification(
                if markers.legacy_data {
                    InstallerStorageDisposition::Mixed
                } else {
                    InstallerStorageDisposition::Corrupt
                },
                data_dir,
                markers,
                None,
                Some(error),
            )),
        };
    }

    if markers.migration_journal && !journal_completed {
        return Ok(installer_classification(
            InstallerStorageDisposition::IncompleteMigration,
            data_dir,
            markers,
            None,
            Some(
                DbError::new("55000", "a storage migration did not reach completion")
                    .with_hint("retain the authoritative source and inspect the migration journal"),
            ),
        ));
    }

    if markers.legacy_data {
        return match inspect_root(&data_dir) {
            Ok(RootAuthority::LegacyV1(inspection)) => {
                let mixed = markers.clusters_directory && !journal_completed;
                let disposition = if mixed {
                    InstallerStorageDisposition::Mixed
                } else {
                    InstallerStorageDisposition::LegacyV1
                };
                let issue = mixed.then(|| {
                    DbError::new(
                        "55000",
                        "legacy authority and untracked v2 cluster files are both present",
                    )
                    .with_hint("remove abandoned staging files only after preserving the v1 source")
                });
                Ok(installer_classification(
                    disposition,
                    data_dir,
                    markers,
                    Some(RootAuthority::LegacyV1(inspection)),
                    issue,
                ))
            }
            Ok(_) => Ok(installer_classification(
                InstallerStorageDisposition::Corrupt,
                data_dir,
                markers,
                None,
                Some(corrupt(
                    "the legacy data path does not contain database format v1",
                )),
            )),
            Err(error) => Ok(installer_classification(
                InstallerStorageDisposition::Corrupt,
                data_dir,
                markers,
                None,
                Some(error),
            )),
        };
    }

    if markers.migration_directory || markers.clusters_directory {
        return Ok(installer_classification(
            InstallerStorageDisposition::IncompleteMigration,
            data_dir,
            markers,
            None,
            Some(
                DbError::new(
                    "55000",
                    "storage migration artifacts exist without an authoritative source",
                )
                .with_hint("restore a verified authority before retrying the installer"),
            ),
        ));
    }

    if markers.top_level_entries == 0 {
        Ok(installer_classification(
            InstallerStorageDisposition::Empty,
            data_dir,
            markers,
            Some(RootAuthority::Empty),
            None,
        ))
    } else {
        Ok(installer_classification(
            InstallerStorageDisposition::Corrupt,
            data_dir,
            markers,
            None,
            Some(
                corrupt("data directory is non-empty but has no authoritative database")
                    .with_hint("move unrelated files away or restore a verified database"),
            ),
        ))
    }
}

fn installer_storage_markers(root: &Path) -> Result<InstallerStorageMarkersV1> {
    for path in [
        root.join(LEGACY_DATA_FILE),
        root.join(LEGACY_WAL_FILE),
        root.join(AUTH_FILE_NAME),
        root.join(ACTIVE_POINTER_FILE),
        root.join("clusters"),
        root.join("migration"),
        root.join("migration").join(MIGRATION_JOURNAL_FILE),
    ] {
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid(format!(
                    "installer storage path {} is a symbolic link",
                    path.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(io_error(
                    "failed to inspect installer storage marker",
                    error,
                ));
            }
        }
    }
    let mut top_level_entries = 0_u64;
    for entry in
        fs::read_dir(root).map_err(|error| io_error("failed to inspect data directory", error))?
    {
        entry.map_err(|error| io_error("failed to inspect data directory entry", error))?;
        top_level_entries = top_level_entries
            .checked_add(1)
            .ok_or_else(|| invalid("data directory entry count overflowed"))?;
        if top_level_entries > 4096 {
            return Err(DbError::new(
                "54000",
                "data directory contains more than 4096 top-level entries",
            ));
        }
    }
    Ok(InstallerStorageMarkersV1 {
        legacy_data: root.join(LEGACY_DATA_FILE).is_file(),
        legacy_wal: root.join(LEGACY_WAL_FILE).is_file(),
        legacy_auth: root.join(AUTH_FILE_NAME).is_file(),
        active_pointer: root.join(ACTIVE_POINTER_FILE).is_file(),
        clusters_directory: root.join("clusters").is_dir(),
        migration_directory: root.join("migration").is_dir(),
        migration_journal: root
            .join("migration")
            .join(MIGRATION_JOURNAL_FILE)
            .is_file(),
        top_level_entries,
    })
}

fn installer_journal_completed(root: &Path, markers: &InstallerStorageMarkersV1) -> Result<bool> {
    if !markers.migration_journal {
        return Ok(false);
    }
    let document: serde_json::Value = read_json_bounded(
        &root.join("migration").join(MIGRATION_JOURNAL_FILE),
        MAX_JOURNAL_BYTES,
    )?;
    let version = document
        .get("formatVersion")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| corrupt("migration journal has no formatVersion"))?;
    if version != u64::from(CLUSTER_FORMAT_V2) {
        return Err(unsupported(format!(
            "migration journal format version {version}"
        )));
    }
    let phase = document
        .get("phase")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| corrupt("migration journal has no phase"))?;
    Ok(phase == "completed")
}

fn installer_classification(
    disposition: InstallerStorageDisposition,
    data_dir: PathBuf,
    markers: InstallerStorageMarkersV1,
    authority: Option<RootAuthority>,
    issue: Option<DbError>,
) -> InstallerStorageClassificationV1 {
    InstallerStorageClassificationV1 {
        schema_version: INSTALLER_STORAGE_CLASSIFICATION_VERSION,
        disposition,
        data_dir,
        markers,
        authority,
        issue,
    }
}

pub fn estimate_cluster_documents_v2(
    cluster_id: Uuid,
    database_id: Uuid,
    catalog_database_id: u64,
    next_transaction_id: u64,
    created_at: DateTime<Utc>,
    migration: Option<MigrationProvenanceV2>,
) -> Result<ClusterDocumentEstimateV2> {
    let transaction_state = TransactionStateV2::bootstrap(next_transaction_id);
    validate_transaction_state(&transaction_state)?;
    let (transaction_state_bytes, transaction_state_sha256) =
        encoded_document_info(&transaction_state, MAX_TRANSACTION_STATE_BYTES)?;
    let database_name = DEFAULT_DATABASE_NAME.to_owned();
    let database_manifest = DatabaseManifestV2 {
        format_version: DATABASE_MANIFEST_FORMAT_V2,
        database_id,
        database_name: database_name.clone(),
        catalog_database_id,
        page_format_version: FILE_FORMAT_VERSION,
        tuple_format_version: TUPLE_FORMAT_V2,
        data_file: LEGACY_DATA_FILE.to_owned(),
        wal_file: LEGACY_WAL_FILE.to_owned(),
        transaction_state: TransactionStateContractV2 {
            format_version: TRANSACTION_STATE_FORMAT_V2,
            relative_path: TRANSACTION_STATE_FILE.to_owned(),
            sha256: transaction_state_sha256,
        },
        index_rebuild: IndexRebuildContractV2::default(),
        migration,
    };
    let database_entry = ClusterDatabaseEntryV2 {
        database_id,
        relative_path: format!("databases/{database_name}"),
        manifest_sha256: String::new(),
        state: DatabaseLifecycleV2::Online,
    };
    validate_database_manifest(&database_manifest, &database_entry, &database_name)?;
    let (database_manifest_bytes, database_manifest_sha256) =
        encoded_document_info(&database_manifest, MAX_DATABASE_MANIFEST_BYTES)?;
    let cluster_manifest = ClusterManifestV2 {
        format_version: CLUSTER_FORMAT_V2,
        cluster_id,
        generation: 1,
        created_at,
        active_database: database_name.clone(),
        roles: RoleCatalogContractV2 {
            relative_path: "roles".to_owned(),
            auth_file: AUTH_FILE_NAME.to_owned(),
            auth_format_version: AUTH_FORMAT_V1,
        },
        databases: BTreeMap::from([(
            database_name,
            ClusterDatabaseEntryV2 {
                manifest_sha256: database_manifest_sha256,
                ..database_entry
            },
        )]),
    };
    validate_cluster_manifest(&cluster_manifest)?;
    let (cluster_manifest_bytes, cluster_manifest_sha256) =
        encoded_document_info(&cluster_manifest, MAX_CLUSTER_MANIFEST_BYTES)?;
    let pointer = ActiveClusterPointerV2 {
        format_version: CLUSTER_FORMAT_V2,
        cluster_id,
        relative_path: format!("clusters/{cluster_id}"),
        manifest_sha256: cluster_manifest_sha256,
    };
    let (active_pointer_bytes, _) = encoded_document_info(&pointer, MAX_POINTER_BYTES)?;
    let total_bytes = transaction_state_bytes
        .checked_add(database_manifest_bytes)
        .and_then(|bytes| bytes.checked_add(cluster_manifest_bytes))
        .and_then(|bytes| bytes.checked_add(active_pointer_bytes))
        .ok_or_else(|| internal("cluster document byte estimate overflowed"))?;
    Ok(ClusterDocumentEstimateV2 {
        transaction_state_bytes,
        database_manifest_bytes,
        cluster_manifest_bytes,
        active_pointer_bytes,
        total_bytes,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeClusterOptions {
    pub database_name: String,
    pub catalog_database_id: u64,
    pub created_at: DateTime<Utc>,
    pub migration: Option<MigrationProvenanceV2>,
}

pub fn inspect_root(root: impl AsRef<Path>) -> Result<RootAuthority> {
    let root = absolute_path(root.as_ref())?;
    if !root.exists() {
        return Ok(RootAuthority::Empty);
    }
    if !root.is_dir() {
        return Err(invalid("cluster root must be a directory"));
    }
    let pointer_path = root.join(ACTIVE_POINTER_FILE);
    if pointer_path.exists() {
        return resolve_active_v2(&root).map(|cluster| RootAuthority::V2(Box::new(cluster)));
    }
    let legacy_path = root.join(LEGACY_DATA_FILE);
    if legacy_path.exists() {
        let inspection = DatabaseStore::inspect_read_only(&root)?;
        if inspection.data_format != DataFormat::V1 {
            return Err(corrupt(
                "a database exists at the legacy root without an active v2 pointer",
            ));
        }
        return Ok(RootAuthority::LegacyV1(Box::new(inspection)));
    }
    if fs::read_dir(&root)
        .map_err(|error| io_error("failed to inspect cluster root", error))?
        .next()
        .transpose()
        .map_err(|error| io_error("failed to inspect cluster root", error))?
        .is_none()
    {
        Ok(RootAuthority::Empty)
    } else {
        Err(DbError::new(
            "55000",
            "cluster root is non-empty but has no authoritative database",
        )
        .with_hint("remove incomplete staging files or restore a verified cluster pointer"))
    }
}

pub fn initialize_empty_v2(root: impl AsRef<Path>) -> Result<ResolvedClusterV2> {
    let root = absolute_path(root.as_ref())?;
    match inspect_root(&root)? {
        RootAuthority::Empty => {}
        RootAuthority::LegacyV1(_) => {
            return Err(legacy_requires_migration(&root));
        }
        RootAuthority::V2(cluster) => return Ok(*cluster),
    }
    fs::create_dir_all(&root).map_err(|error| io_error("failed to create cluster root", error))?;
    let cluster_id = Uuid::new_v4();
    let database_id = Uuid::new_v4();
    let candidate = root
        .join("clusters")
        .join(format!(".{cluster_id}.initializing"));
    let layout = prepare_cluster_layout(&candidate, cluster_id, database_id, 3)?;
    {
        let store = DatabaseStore::open_with_format(&layout.database_dir, DataFormat::V2)?;
        if store.data_format() != DataFormat::V2 {
            return Err(corrupt("new cluster database did not use format v2"));
        }
    }
    let inspection = DatabaseStore::inspect_read_only(&layout.database_dir)?;
    let catalog_database_id = inspection.catalog.database().id.get();
    finalize_cluster_layout(
        &layout,
        FinalizeClusterOptions {
            database_name: DEFAULT_DATABASE_NAME.to_owned(),
            catalog_database_id,
            created_at: Utc::now(),
            migration: None,
        },
    )?;
    let published = publish_cluster_directory(&root, &candidate, cluster_id)?;
    activate_published_cluster(&root, &published)?;
    resolve_active_v2(&root)
}

pub fn prepare_cluster_layout(
    cluster_dir: impl AsRef<Path>,
    cluster_id: Uuid,
    database_id: Uuid,
    next_transaction_id: u64,
) -> Result<ClusterLayoutV2> {
    let cluster_dir = absolute_path(cluster_dir.as_ref())?;
    if cluster_dir.exists() {
        return Err(DbError::new(
            "55000",
            "cluster candidate directory already exists",
        ));
    }
    let database_dir = cluster_dir.join("databases").join(DEFAULT_DATABASE_NAME);
    let roles_dir = cluster_dir.join("roles");
    fs::create_dir_all(&database_dir)
        .and_then(|_| fs::create_dir_all(&roles_dir))
        .map_err(|error| io_error("failed to create v2 cluster directory layout", error))?;
    let transaction_state = TransactionStateV2::bootstrap(next_transaction_id);
    let transaction_state_sha256 = write_json_atomic(
        &database_dir.join(TRANSACTION_STATE_FILE),
        &transaction_state,
        MAX_TRANSACTION_STATE_BYTES,
    )?;
    Ok(ClusterLayoutV2 {
        cluster_id,
        database_id,
        cluster_dir,
        database_dir,
        roles_dir,
        transaction_state_sha256,
    })
}

pub fn finalize_cluster_layout(
    layout: &ClusterLayoutV2,
    options: FinalizeClusterOptions,
) -> Result<ClusterManifestV2> {
    let database_name = normalize_database_name(&options.database_name)?;
    let inspection = DatabaseStore::inspect_read_only(&layout.database_dir)?;
    if inspection.data_format != DataFormat::V2 {
        return Err(DbError::new(
            "0A000",
            "cluster candidates must contain database format v2",
        ));
    }
    if inspection.catalog.database().name.as_str() != database_name
        || inspection.catalog.database().id.get() != options.catalog_database_id
    {
        return Err(corrupt(
            "candidate database identity does not match its finalize options",
        ));
    }
    let transaction_path = layout.database_dir.join(TRANSACTION_STATE_FILE);
    let transaction_state: TransactionStateV2 =
        read_json_bounded(&transaction_path, MAX_TRANSACTION_STATE_BYTES)?;
    validate_transaction_state(&transaction_state)?;
    let transaction_sha256 = hash_file(&transaction_path)?;
    if transaction_sha256 != layout.transaction_state_sha256 {
        return Err(corrupt(
            "candidate transaction state changed before finalization",
        ));
    }
    let database_manifest = DatabaseManifestV2 {
        format_version: DATABASE_MANIFEST_FORMAT_V2,
        database_id: layout.database_id,
        database_name: database_name.clone(),
        catalog_database_id: options.catalog_database_id,
        page_format_version: FILE_FORMAT_VERSION,
        tuple_format_version: TUPLE_FORMAT_V2,
        data_file: LEGACY_DATA_FILE.to_owned(),
        wal_file: LEGACY_WAL_FILE.to_owned(),
        transaction_state: TransactionStateContractV2 {
            format_version: TRANSACTION_STATE_FORMAT_V2,
            relative_path: TRANSACTION_STATE_FILE.to_owned(),
            sha256: transaction_sha256,
        },
        index_rebuild: IndexRebuildContractV2::default(),
        migration: options.migration,
    };
    let database_manifest_sha256 = write_json_atomic(
        &layout.database_dir.join(DATABASE_MANIFEST_FILE),
        &database_manifest,
        MAX_DATABASE_MANIFEST_BYTES,
    )?;
    let roles = RoleCatalogContractV2 {
        relative_path: "roles".to_owned(),
        auth_file: AUTH_FILE_NAME.to_owned(),
        auth_format_version: AUTH_FORMAT_V1,
    };
    let database_relative_path = format!("databases/{database_name}");
    let manifest = ClusterManifestV2 {
        format_version: CLUSTER_FORMAT_V2,
        cluster_id: layout.cluster_id,
        generation: 1,
        created_at: options.created_at,
        active_database: database_name.clone(),
        roles,
        databases: BTreeMap::from([(
            database_name,
            ClusterDatabaseEntryV2 {
                database_id: layout.database_id,
                relative_path: database_relative_path,
                manifest_sha256: database_manifest_sha256,
                state: DatabaseLifecycleV2::Online,
            },
        )]),
    };
    write_json_atomic(
        &layout.cluster_dir.join(CLUSTER_MANIFEST_FILE),
        &manifest,
        MAX_CLUSTER_MANIFEST_BYTES,
    )?;
    validate_cluster_directory(&layout.cluster_dir)?;
    Ok(manifest)
}
