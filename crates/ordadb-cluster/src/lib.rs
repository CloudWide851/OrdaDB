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

pub fn validate_cluster_directory(cluster_dir: impl AsRef<Path>) -> Result<ResolvedClusterV2> {
    let cluster_dir = absolute_existing_directory(cluster_dir.as_ref())?;
    let manifest_path = cluster_dir.join(CLUSTER_MANIFEST_FILE);
    let manifest: ClusterManifestV2 =
        read_json_bounded(&manifest_path, MAX_CLUSTER_MANIFEST_BYTES)?;
    validate_cluster_manifest(&manifest)?;
    let database_entry = manifest
        .databases
        .get(&manifest.active_database)
        .ok_or_else(|| corrupt("active database is absent from the cluster directory"))?;
    let database_dir = safe_existing_join(&cluster_dir, &database_entry.relative_path)?;
    let database_manifest_path = database_dir.join(DATABASE_MANIFEST_FILE);
    if hash_file(&database_manifest_path)? != database_entry.manifest_sha256 {
        return Err(corrupt(
            "database manifest SHA-256 does not match the cluster manifest",
        ));
    }
    let database_manifest: DatabaseManifestV2 =
        read_json_bounded(&database_manifest_path, MAX_DATABASE_MANIFEST_BYTES)?;
    validate_database_manifest(
        &database_manifest,
        database_entry,
        &manifest.active_database,
    )?;
    let transaction_path = safe_existing_join(
        &database_dir,
        &database_manifest.transaction_state.relative_path,
    )?;
    if hash_file(&transaction_path)? != database_manifest.transaction_state.sha256 {
        return Err(corrupt(
            "transaction state SHA-256 does not match the database manifest",
        ));
    }
    let transaction_state: TransactionStateV2 =
        read_json_bounded(&transaction_path, MAX_TRANSACTION_STATE_BYTES)?;
    validate_transaction_state(&transaction_state)?;
    let storage = DatabaseStore::inspect_read_only(&database_dir)?;
    if storage.data_format != DataFormat::V2
        || storage.catalog.database().id.get() != database_manifest.catalog_database_id
        || storage.catalog.database().name.as_str() != database_manifest.database_name
    {
        return Err(corrupt(
            "database storage identity does not match the database manifest",
        ));
    }
    if let Some(migration) = &database_manifest.migration
        && storage.generation < migration.activation_generation
    {
        return Err(corrupt(
            "database generation precedes its migration activation generation",
        ));
    }
    let roles_dir = safe_existing_join(&cluster_dir, &manifest.roles.relative_path)?;
    Ok(ResolvedClusterV2 {
        root: cluster_dir.clone(),
        cluster_dir,
        database_dir,
        roles_dir,
        pointer: ActiveClusterPointerV2 {
            format_version: CLUSTER_FORMAT_V2,
            cluster_id: manifest.cluster_id,
            relative_path: String::new(),
            manifest_sha256: hash_file(&manifest_path)?,
        },
        cluster_manifest: manifest,
        database_manifest,
        transaction_state,
        storage,
    })
}

pub fn publish_cluster_directory(
    root: impl AsRef<Path>,
    candidate_dir: impl AsRef<Path>,
    cluster_id: Uuid,
) -> Result<PathBuf> {
    let root = absolute_path(root.as_ref())?;
    let candidate_dir = absolute_existing_directory(candidate_dir.as_ref())?;
    ensure_descendant(&root, &candidate_dir)?;
    let clusters_dir = root.join("clusters");
    fs::create_dir_all(&clusters_dir)
        .map_err(|error| io_error("failed to create published cluster directory", error))?;
    let published = clusters_dir.join(cluster_id.to_string());
    if published.exists() {
        return Err(DbError::new(
            "55000",
            "published cluster directory already exists",
        ));
    }
    atomic_move_new(&candidate_dir, &published)?;
    sync_directory(&clusters_dir)?;
    Ok(published)
}

pub fn activate_published_cluster(
    root: impl AsRef<Path>,
    cluster_dir: impl AsRef<Path>,
) -> Result<ActiveClusterPointerV2> {
    let root = absolute_existing_directory(root.as_ref())?;
    let cluster_dir = absolute_existing_directory(cluster_dir.as_ref())?;
    ensure_descendant(&root, &cluster_dir)?;
    let validated = validate_cluster_directory(&cluster_dir)?;
    let relative_path = relative_forward(&root, &cluster_dir)?;
    let pointer = ActiveClusterPointerV2 {
        format_version: CLUSTER_FORMAT_V2,
        cluster_id: validated.cluster_manifest.cluster_id,
        relative_path,
        manifest_sha256: hash_file(&cluster_dir.join(CLUSTER_MANIFEST_FILE))?,
    };
    write_json_atomic(&root.join(ACTIVE_POINTER_FILE), &pointer, MAX_POINTER_BYTES)?;
    Ok(pointer)
}

pub fn resolve_active_v2(root: impl AsRef<Path>) -> Result<ResolvedClusterV2> {
    let root = absolute_existing_directory(root.as_ref())?;
    let pointer_path = root.join(ACTIVE_POINTER_FILE);
    let pointer: ActiveClusterPointerV2 = read_json_bounded(&pointer_path, MAX_POINTER_BYTES)?;
    if pointer.format_version != CLUSTER_FORMAT_V2 {
        return Err(unsupported(format!(
            "active cluster pointer version {}",
            pointer.format_version
        )));
    }
    let cluster_dir = safe_existing_join(&root, &pointer.relative_path)?;
    let manifest_path = cluster_dir.join(CLUSTER_MANIFEST_FILE);
    if hash_file(&manifest_path)? != pointer.manifest_sha256 {
        return Err(corrupt(
            "cluster manifest SHA-256 does not match the active pointer",
        ));
    }
    let mut resolved = validate_cluster_directory(&cluster_dir)?;
    if resolved.cluster_manifest.cluster_id != pointer.cluster_id {
        return Err(corrupt(
            "active pointer cluster ID does not match the cluster manifest",
        ));
    }
    resolved.root = root;
    resolved.pointer = pointer;
    Ok(resolved)
}

pub fn remove_active_pointer_for_rollback(root: impl AsRef<Path>) -> Result<()> {
    let root = absolute_existing_directory(root.as_ref())?;
    let resolved = resolve_active_v2(&root)?;
    let migration = resolved
        .database_manifest
        .migration
        .as_ref()
        .ok_or_else(|| {
            DbError::new(
                "55000",
                "active v2 cluster has no legacy rollback provenance",
            )
        })?;
    if resolved.storage.generation != migration.activation_generation {
        return Err(DbError::new(
            "55000",
            "v1 rollback is forbidden after a v2 database write",
        )
        .with_detail(format!(
            "activation generation {}, current generation {}",
            migration.activation_generation, resolved.storage.generation
        ))
        .with_hint("restore the verified logical backup into a fresh v2 cluster"));
    }
    let rollback_dir = safe_existing_join(&root, &migration.rollback_relative_path)?;
    if !rollback_dir.join(LEGACY_DATA_FILE).is_file() {
        return Err(corrupt(
            "retained v1 rollback directory has no database file",
        ));
    }
    fs::remove_file(root.join(ACTIVE_POINTER_FILE))
        .map_err(|error| io_error("failed to remove active pointer for rollback", error))?;
    sync_directory(&root)
}

pub fn migration_journal_path(root: impl AsRef<Path>) -> Result<PathBuf> {
    let root = absolute_path(root.as_ref())?;
    Ok(root.join("migration").join(MIGRATION_JOURNAL_FILE))
}

pub fn write_migration_journal<T: Serialize>(
    root: impl AsRef<Path>,
    journal: &T,
) -> Result<String> {
    write_json_atomic(&migration_journal_path(root)?, journal, MAX_JOURNAL_BYTES)
}

pub fn read_migration_journal<T: DeserializeOwned>(root: impl AsRef<Path>) -> Result<T> {
    read_json_bounded(&migration_journal_path(root)?, MAX_JOURNAL_BYTES)
}

pub fn available_space(path: impl AsRef<Path>) -> Result<u64> {
    available_space_impl(path.as_ref())
}

pub fn legacy_requires_migration(root: &Path) -> DbError {
    DbError::new(
        "0A000",
        "legacy database format v1 is read-only and requires explicit migration",
    )
    .with_detail(format!("legacy cluster root: {}", root.display()))
    .with_hint(format!(
        "run ordadb-cli storage-migrate --data-dir \"{}\" --dry-run, then rerun without --dry-run",
        root.display()
    ))
}

fn validate_cluster_manifest(manifest: &ClusterManifestV2) -> Result<()> {
    if manifest.format_version != CLUSTER_FORMAT_V2 {
        return Err(unsupported(format!(
            "cluster manifest version {}",
            manifest.format_version
        )));
    }
    let active = normalize_database_name(&manifest.active_database)?;
    if active != manifest.active_database
        || manifest.generation == 0
        || manifest.databases.is_empty()
    {
        return Err(corrupt(
            "cluster manifest identity or generation is invalid",
        ));
    }
    validate_relative_path(&manifest.roles.relative_path)?;
    if manifest.roles.auth_file != AUTH_FILE_NAME
        || manifest.roles.auth_format_version != AUTH_FORMAT_V1
    {
        return Err(unsupported("cluster role catalog contract"));
    }
    for (name, database) in &manifest.databases {
        if normalize_database_name(name)? != *name {
            return Err(corrupt("cluster database key is not normalized"));
        }
        validate_relative_path(&database.relative_path)?;
        validate_sha256(&database.manifest_sha256)?;
    }
    Ok(())
}

fn validate_database_manifest(
    manifest: &DatabaseManifestV2,
    entry: &ClusterDatabaseEntryV2,
    expected_name: &str,
) -> Result<()> {
    if manifest.format_version != DATABASE_MANIFEST_FORMAT_V2
        || manifest.page_format_version != FILE_FORMAT_VERSION
        || manifest.tuple_format_version != TUPLE_FORMAT_V2
        || manifest.transaction_state.format_version != TRANSACTION_STATE_FORMAT_V2
        || manifest.index_rebuild != IndexRebuildContractV2::default()
    {
        return Err(unsupported("database v2 manifest contract"));
    }
    if manifest.database_id != entry.database_id
        || manifest.database_name != expected_name
        || normalize_database_name(&manifest.database_name)? != manifest.database_name
        || manifest.catalog_database_id == 0
        || manifest.data_file != LEGACY_DATA_FILE
        || manifest.wal_file != LEGACY_WAL_FILE
    {
        return Err(corrupt("database v2 manifest identity is inconsistent"));
    }
    validate_relative_path(&manifest.transaction_state.relative_path)?;
    validate_sha256(&manifest.transaction_state.sha256)?;
    if let Some(migration) = &manifest.migration {
        if migration.source_format_version != DATABASE_FORMAT_V1
            || migration.activation_generation < migration.source_generation
        {
            return Err(corrupt("database migration provenance is inconsistent"));
        }
        validate_sha256(&migration.logical_backup_sha256)?;
        validate_relative_path(&migration.rollback_relative_path)?;
    }
    Ok(())
}

fn validate_transaction_state(state: &TransactionStateV2) -> Result<()> {
    if state.format_version != TRANSACTION_STATE_FORMAT_V2 {
        return Err(unsupported(format!(
            "transaction state version {}",
            state.format_version
        )));
    }
    if state.next_transaction_id <= FROZEN_TRANSACTION_ID
        || state.statuses.get(&FROZEN_TRANSACTION_ID) != Some(&TransactionStatusV2::Committed)
        || state
            .statuses
            .keys()
            .any(|transaction_id| *transaction_id >= state.next_transaction_id)
    {
        return Err(corrupt("transaction state v2 boundaries are invalid"));
    }
    Ok(())
}

fn normalize_database_name(name: &str) -> Result<String> {
    let normalized = name.to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.len() > 63
        || !normalized.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase() || byte == b'_' || (index > 0 && byte.is_ascii_digit())
        })
    {
        return Err(invalid(
            "database name must be 1..=63 ASCII lowercase letters, digits, or underscores and cannot start with a digit",
        ));
    }
    Ok(normalized)
}

fn validate_relative_path(relative: &str) -> Result<()> {
    let path = Path::new(relative);
    if relative.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid("cluster relative path is not a safe normal path"));
    }
    Ok(())
}

fn safe_existing_join(root: &Path, relative: &str) -> Result<PathBuf> {
    validate_relative_path(relative)?;
    let joined = root.join(relative);
    let existing = joined
        .canonicalize()
        .map_err(|error| io_error("failed to resolve cluster path", error))?;
    let canonical_root = root
        .canonicalize()
        .map_err(|error| io_error("failed to resolve cluster root", error))?;
    if !existing.starts_with(&canonical_root) {
        return Err(invalid("cluster path escapes its owning root"));
    }
    Ok(existing)
}

fn ensure_descendant(root: &Path, child: &Path) -> Result<()> {
    let canonical_root = root
        .canonicalize()
        .map_err(|error| io_error("failed to resolve cluster root", error))?;
    let canonical_child = child
        .canonicalize()
        .map_err(|error| io_error("failed to resolve cluster child path", error))?;
    if canonical_child == canonical_root || !canonical_child.starts_with(&canonical_root) {
        return Err(invalid("cluster child path is outside its owning root"));
    }
    Ok(())
}

fn relative_forward(root: &Path, child: &Path) -> Result<String> {
    let relative = child
        .strip_prefix(root)
        .map_err(|_| invalid("cluster path is outside its root"))?;
    let components = relative
        .components()
        .map(|component| match component {
            Component::Normal(value) => value
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid("cluster path is not UTF-8")),
            _ => Err(invalid("cluster path contains a non-normal component")),
        })
        .collect::<Result<Vec<_>>>()?;
    let value = components.join("/");
    validate_relative_path(&value)?;
    Ok(value)
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(invalid("cluster path cannot be empty"));
    }
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|current| current.join(path))
            .map_err(|error| io_error("failed to resolve current directory", error))
    }
}

fn absolute_existing_directory(path: &Path) -> Result<PathBuf> {
    let path = absolute_path(path)?;
    let canonical = path
        .canonicalize()
        .map_err(|error| io_error("failed to resolve cluster directory", error))?;
    if !canonical.is_dir() {
        return Err(invalid("cluster path is not a directory"));
    }
    Ok(canonical)
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T, maximum: u64) -> Result<String> {
    let bytes = encode_json_document(value, maximum)?;
    let parent = path
        .parent()
        .ok_or_else(|| internal("cluster document path has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error("failed to create cluster document directory", error))?;
    let temporary = parent.join(format!(".{}.{}.tmp", file_name(path)?, Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| io_error("failed to create cluster temporary document", error))?;
        file.write_all(&bytes)
            .map_err(|error| io_error("failed to write cluster temporary document", error))?;
        file.sync_all()
            .map_err(|error| io_error("failed to synchronize cluster temporary document", error))?;
        drop(file);
        atomic_replace(&temporary, path)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result?;
    Ok(sha256_hex(&bytes))
}

fn encoded_document_info<T: Serialize>(value: &T, maximum: u64) -> Result<(u64, String)> {
    let bytes = encode_json_document(value, maximum)?;
    let length =
        u64::try_from(bytes.len()).map_err(|_| internal("cluster document length exceeds u64"))?;
    Ok((length, sha256_hex(&bytes)))
}

fn encode_json_document<T: Serialize>(value: &T, maximum: u64) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| internal(format!("failed to encode cluster document: {error}")))?;
    if bytes.is_empty()
        || u64::try_from(bytes.len())
            .map(|length| length > maximum)
            .unwrap_or(true)
    {
        return Err(DbError::new(
            "54000",
            format!("cluster document must be between 1 and {maximum} bytes"),
        ));
    }
    Ok(bytes)
}

fn read_json_bounded<T: DeserializeOwned>(path: &Path, maximum: u64) -> Result<T> {
    let metadata = fs::metadata(path)
        .map_err(|error| io_error("failed to inspect cluster document", error))?;
    if metadata.len() == 0 || metadata.len() > maximum {
        return Err(corrupt(format!(
            "cluster document must be between 1 and {maximum} bytes"
        )));
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| corrupt("cluster document length exceeds this platform"))?;
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|error| io_error("failed to read cluster document", error))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| corrupt(format!("cluster document is malformed: {error}")))
}

fn hash_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).map_err(|error| io_error("failed to open file for SHA-256", error))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| io_error("failed to hash cluster file", error))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex_digest(&digest.finalize()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_digest(&Sha256::digest(bytes))
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(corrupt("persisted SHA-256 is not lowercase hexadecimal"));
    }
    Ok(())
}

fn file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(str::to_owned)
        .ok_or_else(|| invalid("cluster document filename is not UTF-8"))
}

#[cfg(not(windows))]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("failed to synchronize cluster directory", error))
}

#[cfg(windows)]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
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
    // SAFETY: both owned buffers are valid NUL-terminated UTF-16 paths for the
    // duration of the call and the flags request same-volume durable replace.
    let moved = unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), flags) };
    if moved == 0 {
        return Err(io_error(
            "failed to atomically replace cluster document",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn atomic_move_new(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MOVEFILE_WRITE_THROUGH, MoveFileExW};

    if destination.exists() {
        return Err(DbError::new(
            "55000",
            "cluster publication destination already exists",
        ));
    }
    let mut source: Vec<u16> = source.as_os_str().encode_wide().collect();
    source.push(0);
    let mut destination: Vec<u16> = destination.as_os_str().encode_wide().collect();
    destination.push(0);
    // SAFETY: both buffers are valid NUL-terminated UTF-16 paths for the call;
    // the destination is absent and WRITE_THROUGH makes publication durable.
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(io_error(
            "failed to atomically publish cluster directory",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

#[cfg(not(windows))]
fn atomic_move_new(source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination)
        .map_err(|error| io_error("failed to atomically publish cluster directory", error))
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        fs::remove_file(destination)
            .map_err(|error| io_error("failed to replace cluster document", error))?;
    }
    fs::rename(source, destination)
        .map_err(|error| io_error("failed to publish cluster document", error))
}

#[cfg(windows)]
fn available_space_impl(path: &Path) -> Result<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let path = absolute_path(path)?;
    let probe = path
        .ancestors()
        .find(|candidate| candidate.exists())
        .ok_or_else(|| invalid("no existing ancestor is available for the space probe"))?;
    let mut wide: Vec<u16> = probe.as_os_str().encode_wide().collect();
    wide.push(0);
    let mut available = 0_u64;
    // SAFETY: `wide` is a valid NUL-terminated UTF-16 directory path and only
    // the caller-available byte count output pointer is non-null.
    let success = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if success == 0 {
        return Err(io_error(
            "failed to inspect available cluster disk space",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(available)
}

#[cfg(not(windows))]
fn available_space_impl(_path: &Path) -> Result<u64> {
    Ok(u64::MAX)
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn unsupported(subject: impl Into<String>) -> DbError {
    DbError::new("0A000", format!("{} is not supported", subject.into()))
        .with_hint("use a compatible OrdaDB build or perform an explicit migration")
}

fn corrupt(message: impl Into<String>) -> DbError {
    DbError::new("XX001", message)
        .with_hint("restore a verified logical backup or rebuild the cluster explicitly")
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
    use ordadb_catalog::Catalog;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn initializes_resolves_and_reopens_an_empty_v2_cluster() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("data");
        assert_eq!(inspect_root(&root).expect("inspect"), RootAuthority::Empty);

        let initialized = initialize_empty_v2(&root).expect("initialize");
        assert_eq!(initialized.storage.data_format, DataFormat::V2);
        assert_eq!(
            initialized.cluster_manifest.active_database,
            DEFAULT_DATABASE_NAME
        );
        assert_eq!(initialized.database_manifest.tuple_format_version, 2);
        assert_eq!(
            initialized.transaction_state.statuses[&FROZEN_TRANSACTION_ID],
            TransactionStatusV2::Committed
        );
        assert!(initialized.roles_dir.is_dir());

        let reopened = resolve_active_v2(&root).expect("resolve");
        assert_eq!(reopened.pointer, initialized.pointer);
        assert_eq!(reopened.database_dir, initialized.database_dir);
        assert!(matches!(
            inspect_root(&root).expect("authority"),
            RootAuthority::V2(_)
        ));
    }

    #[test]
    fn cluster_document_estimate_matches_the_exact_published_file_lengths() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("data");
        fs::create_dir_all(&root).expect("cluster root");
        let cluster_id = Uuid::from_u128(1);
        let database_id = Uuid::from_u128(2);
        let created_at = DateTime::from_timestamp(1_774_915_200, 0).expect("timestamp");
        let layout = prepare_cluster_layout(root.join("candidate"), cluster_id, database_id, 42)
            .expect("layout");
        drop(
            DatabaseStore::open_with_format(&layout.database_dir, DataFormat::V2)
                .expect("database"),
        );
        let storage = DatabaseStore::inspect_read_only(&layout.database_dir).expect("storage");
        finalize_cluster_layout(
            &layout,
            FinalizeClusterOptions {
                database_name: DEFAULT_DATABASE_NAME.to_owned(),
                catalog_database_id: storage.catalog.database().id.get(),
                created_at,
                migration: None,
            },
        )
        .expect("finalize");
        let published =
            publish_cluster_directory(&root, &layout.cluster_dir, cluster_id).expect("publish");
        activate_published_cluster(&root, &published).expect("activate");
        let resolved = resolve_active_v2(&root).expect("resolve");
        let estimate = estimate_cluster_documents_v2(
            cluster_id,
            database_id,
            storage.catalog.database().id.get(),
            42,
            created_at,
            None,
        )
        .expect("estimate");

        assert_eq!(
            fs::metadata(resolved.database_dir.join(TRANSACTION_STATE_FILE))
                .expect("transaction state")
                .len(),
            estimate.transaction_state_bytes
        );
        assert_eq!(
            fs::metadata(resolved.database_dir.join(DATABASE_MANIFEST_FILE))
                .expect("database manifest")
                .len(),
            estimate.database_manifest_bytes
        );
        assert_eq!(
            fs::metadata(published.join(CLUSTER_MANIFEST_FILE))
                .expect("cluster manifest")
                .len(),
            estimate.cluster_manifest_bytes
        );
        assert_eq!(
            fs::metadata(root.join(ACTIVE_POINTER_FILE))
                .expect("active pointer")
                .len(),
            estimate.active_pointer_bytes
        );
        assert_eq!(
            estimate.total_bytes,
            estimate.transaction_state_bytes
                + estimate.database_manifest_bytes
                + estimate.cluster_manifest_bytes
                + estimate.active_pointer_bytes
        );
    }

    #[test]
    fn detects_legacy_v1_without_mutating_it() {
        let directory = tempdir().expect("tempdir");
        let mut store = DatabaseStore::open(directory.path()).expect("legacy store");
        let mut state = store.committed_state().clone();
        state.catalog = Catalog::default();
        state.generation = 7;
        store.commit(&state).expect("commit");
        drop(store);
        let path = directory.path().join(LEGACY_DATA_FILE);
        let before = fs::read(&path).expect("before");
        let authority = inspect_root(directory.path()).expect("inspect");
        let RootAuthority::LegacyV1(inspection) = authority else {
            panic!("expected legacy");
        };
        assert_eq!(inspection.generation, 7);
        assert_eq!(fs::read(path).expect("after"), before);
        assert_eq!(
            initialize_empty_v2(directory.path())
                .expect_err("migration required")
                .sql_state,
            "0A000"
        );
    }

    #[test]
    fn pointer_and_manifest_checksums_fail_closed() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("data");
        let resolved = initialize_empty_v2(&root).expect("initialize");
        let manifest_path = resolved.cluster_dir.join(CLUSTER_MANIFEST_FILE);
        let mut manifest: ClusterManifestV2 =
            read_json_bounded(&manifest_path, MAX_CLUSTER_MANIFEST_BYTES).expect("manifest");
        manifest.generation += 1;
        write_json_atomic(&manifest_path, &manifest, MAX_CLUSTER_MANIFEST_BYTES).expect("tamper");
        assert_eq!(
            resolve_active_v2(&root)
                .expect_err("checksum mismatch")
                .sql_state,
            "XX001"
        );
    }

    #[test]
    fn transaction_state_has_stable_json_and_safe_paths_are_enforced() {
        let state = TransactionStateV2::bootstrap(9);
        let encoded = serde_json::to_string(&state).expect("json");
        assert_eq!(
            encoded,
            r#"{"formatVersion":2,"nextTransactionId":9,"statuses":{"2":"committed"}}"#
        );
        assert!(validate_relative_path("databases/ordadb").is_ok());
        assert!(validate_relative_path("../escape").is_err());
        assert!(validate_relative_path("C:\\escape").is_err());
        assert!(normalize_database_name("OrdaDB").is_ok_and(|name| name == "ordadb"));
        assert!(normalize_database_name("9bad").is_err());
        let mut unsupported = state;
        unsupported.format_version += 1;
        assert_eq!(
            validate_transaction_state(&unsupported)
                .expect_err("unsupported transaction state")
                .sql_state,
            "0A000"
        );
    }
}
