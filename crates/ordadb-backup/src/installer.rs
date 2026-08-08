use std::path::{Path, PathBuf};

use chrono::Utc;
use ordadb_cluster::{
    ACTIVE_POINTER_FILE, AUTH_FILE_NAME, CLUSTER_MANIFEST_FILE, InstallerStorageClassificationV1,
    InstallerStorageDisposition, InstallerStorageMarkersV1, LEGACY_DATA_FILE, LEGACY_WAL_FILE,
    RootAuthority, available_space, classify_installer_storage, normalize_installer_data_dir,
};
use ordadb_types::{DbError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::migration::{
    MigrationInventoryV2, MigrationPhaseV2, MigrationPlanV2, MigrationReportV2,
    MigrationRunOptionsV2, apply_migration_plan_v2, hash_file, optional_file_bytes,
    optional_file_sha256, plan_v1_to_v2, validate_migration_plan,
};

pub const INSTALLER_STORAGE_SCHEMA_VERSION: u32 = 1;
pub const MAX_INSTALLER_RECEIPT_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_INSTALLER_REPORT_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_INSTALLER_STATE_BYTES: u64 = 64 * 1024;

const MAX_GUIDANCE_BYTES: usize = 2048;
const MAX_INCOMPATIBILITIES: usize = 64;
const MAX_INCOMPATIBILITY_BYTES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstallerStorageInventoryV1 {
    pub markers: InstallerStorageMarkersV1,
    pub migration: Option<MigrationInventoryV2>,
    pub active_cluster_id: Option<Uuid>,
    pub active_cluster_generation: Option<u64>,
    pub active_database_id: Option<Uuid>,
    pub active_database_generation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstallerSourceFingerprintV1 {
    pub disposition: InstallerStorageDisposition,
    pub markers: InstallerStorageMarkersV1,
    pub source_generation: Option<u64>,
    pub data_file_bytes: u64,
    pub wal_file_bytes: u64,
    pub auth_file_bytes: u64,
    pub data_file_sha256: Option<String>,
    pub wal_file_sha256: Option<String>,
    pub auth_file_sha256: Option<String>,
    pub active_pointer_sha256: Option<String>,
    pub active_cluster_id: Option<Uuid>,
    pub active_cluster_generation: Option<u64>,
    pub active_database_id: Option<Uuid>,
    pub planned_paths_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstallerMigrationIncompatibilityV1 {
    pub code: String,
    pub message: String,
    pub hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstallerStoragePreflightV1 {
    pub schema_version: u32,
    pub disposition: InstallerStorageDisposition,
    pub data_dir: PathBuf,
    pub inventory: InstallerStorageInventoryV1,
    pub source_fingerprint: InstallerSourceFingerprintV1,
    pub required_bytes: u64,
    pub free_bytes: u64,
    pub backup_path: Option<PathBuf>,
    pub rollback_path: Option<PathBuf>,
    pub phases: Vec<MigrationPhaseV2>,
    pub incompatibilities: Vec<InstallerMigrationIncompatibilityV1>,
    pub safe_to_apply: bool,
    pub guidance: String,
    pub migration_plan: Option<MigrationPlanV2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstallerMigrationReceiptV1 {
    pub schema_version: u32,
    pub preflight: InstallerStoragePreflightV1,
    pub receipt_digest: String,
}

impl InstallerMigrationReceiptV1 {
    pub fn from_preflight(preflight: InstallerStoragePreflightV1) -> Result<Self> {
        validate_preflight(&preflight)?;
        let receipt_digest = digest_json(&preflight, "installer storage preflight")?;
        Ok(Self {
            schema_version: INSTALLER_STORAGE_SCHEMA_VERSION,
            preflight,
            receipt_digest,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InstallerStorageApplyActionV1 {
    NoChange,
    MigratedV1ToV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstallerStorageApplyReportV1 {
    pub schema_version: u32,
    pub completed_at_unix_ms: u64,
    pub data_dir: PathBuf,
    pub disposition_before: InstallerStorageDisposition,
    pub disposition_after: InstallerStorageDisposition,
    pub action: InstallerStorageApplyActionV1,
    pub source_fingerprint: InstallerSourceFingerprintV1,
    pub migration: Option<MigrationReportV2>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InstallerStorageOptionsV1 {
    pub migration: MigrationRunOptionsV2,
}

pub fn installer_storage_preflight(
    data_dir: impl AsRef<Path>,
) -> Result<InstallerMigrationReceiptV1> {
    installer_storage_preflight_with_options(data_dir, InstallerStorageOptionsV1::default())
}

pub fn installer_storage_preflight_with_options(
    data_dir: impl AsRef<Path>,
    options: InstallerStorageOptionsV1,
) -> Result<InstallerMigrationReceiptV1> {
    let classification = classify_installer_storage(data_dir)?;
    let mut free_bytes = available_space(&classification.data_dir)?;
    let mut required_bytes = 0;
    let mut backup_path = None;
    let mut rollback_path = None;
    let mut phases = Vec::new();
    let mut incompatibilities = classification
        .issue
        .as_ref()
        .map(incompatibility_from_error)
        .into_iter()
        .collect::<Vec<_>>();
    let mut migration_plan = None;

    if classification.disposition == InstallerStorageDisposition::LegacyV1 {
        let plan = plan_v1_to_v2(&classification.data_dir, options.migration)?;
        required_bytes = plan.bytes.required_bytes;
        free_bytes = plan.bytes.available_bytes;
        backup_path = Some(plan.paths.logical_backup.clone());
        rollback_path = Some(plan.paths.rollback_directory.clone());
        phases = plan.phases.clone();
        incompatibilities.extend(plan.incompatibilities.iter().map(|message| {
            InstallerMigrationIncompatibilityV1 {
                code: "0A000".to_owned(),
                message: bounded(message, MAX_INCOMPATIBILITY_BYTES),
                hint: Some("resolve the incompatibility before installation".to_owned()),
            }
        }));
        if free_bytes < required_bytes {
            incompatibilities.push(InstallerMigrationIncompatibilityV1 {
                code: "53100".to_owned(),
                message: format!(
                    "migration requires {required_bytes} bytes but only {free_bytes} bytes are free"
                ),
                hint: Some("free disk space and rerun installer preflight".to_owned()),
            });
        }
        migration_plan = Some(plan);
    }

    let safe_to_apply = matches!(
        classification.disposition,
        InstallerStorageDisposition::Empty | InstallerStorageDisposition::ActiveV2
    ) || (classification.disposition == InstallerStorageDisposition::LegacyV1
        && incompatibilities.is_empty()
        && free_bytes >= required_bytes);
    let inventory = inventory_from_classification(&classification, migration_plan.as_ref());
    let source_fingerprint =
        fingerprint_from_classification(&classification, migration_plan.as_ref())?;
    let guidance = guidance_for(classification.disposition, safe_to_apply);
    InstallerMigrationReceiptV1::from_preflight(InstallerStoragePreflightV1 {
        schema_version: INSTALLER_STORAGE_SCHEMA_VERSION,
        disposition: classification.disposition,
        data_dir: classification.data_dir,
        inventory,
        source_fingerprint,
        required_bytes,
        free_bytes,
        backup_path,
        rollback_path,
        phases,
        incompatibilities,
        safe_to_apply,
        guidance,
        migration_plan,
    })
}

pub fn apply_installer_storage_receipt(
    data_dir: impl AsRef<Path>,
    receipt: &InstallerMigrationReceiptV1,
) -> Result<InstallerStorageApplyReportV1> {
    apply_installer_storage_receipt_with_options(
        data_dir,
        receipt,
        InstallerStorageOptionsV1::default(),
    )
}

pub fn apply_installer_storage_receipt_with_options(
    data_dir: impl AsRef<Path>,
    receipt: &InstallerMigrationReceiptV1,
    options: InstallerStorageOptionsV1,
) -> Result<InstallerStorageApplyReportV1> {
    validate_installer_migration_receipt(receipt)?;
    if !receipt.preflight.safe_to_apply {
        return Err(
            DbError::new("55000", "installer storage receipt is not safe to apply")
                .with_hint(&receipt.preflight.guidance),
        );
    }
    let normalized = normalize_installer_data_dir(data_dir)?;
    if normalized != receipt.preflight.data_dir {
        return Err(DbError::new(
            "22023",
            "installer storage receipt belongs to a different data directory",
        ));
    }
    if let Some(plan) = receipt.preflight.migration_plan.as_ref() {
        validate_migration_plan(plan)?;
    }
    let classification = classify_installer_storage(&normalized)?;
    let current_fingerprint = fingerprint_from_classification(
        &classification,
        receipt.preflight.migration_plan.as_ref(),
    )?;
    if classification.disposition != receipt.preflight.disposition
        || current_fingerprint != receipt.preflight.source_fingerprint
    {
        return Err(source_changed());
    }

    let (action, migration) = match receipt.preflight.disposition {
        InstallerStorageDisposition::Empty | InstallerStorageDisposition::ActiveV2 => {
            (InstallerStorageApplyActionV1::NoChange, None)
        }
        InstallerStorageDisposition::LegacyV1 => {
            let plan = receipt
                .preflight
                .migration_plan
                .clone()
                .ok_or_else(|| corrupt("legacy installer receipt has no migration plan"))?;
            (
                InstallerStorageApplyActionV1::MigratedV1ToV2,
                Some(apply_migration_plan_v2(plan, options.migration)?),
            )
        }
        InstallerStorageDisposition::Mixed
        | InstallerStorageDisposition::Corrupt
        | InstallerStorageDisposition::IncompleteMigration => {
            return Err(DbError::new(
                "55000",
                "unsafe installer storage disposition cannot be applied",
            ));
        }
    };

    let final_classification = classify_installer_storage(&normalized)?;
    let final_fingerprint = fingerprint_from_classification(&final_classification, None)?;
    let completed_at_unix_ms = u64::try_from(Utc::now().timestamp_millis())
        .map_err(|_| internal("installer completion timestamp predates the Unix epoch"))?;
    Ok(InstallerStorageApplyReportV1 {
        schema_version: INSTALLER_STORAGE_SCHEMA_VERSION,
        completed_at_unix_ms,
        data_dir: normalized,
        disposition_before: receipt.preflight.disposition,
        disposition_after: final_classification.disposition,
        action,
        source_fingerprint: final_fingerprint,
        migration,
    })
}

pub fn decode_installer_migration_receipt(bytes: &[u8]) -> Result<InstallerMigrationReceiptV1> {
    if u64::try_from(bytes.len())
        .map_err(|_| resource_limit("installer receipt length exceeds u64"))?
        > MAX_INSTALLER_RECEIPT_BYTES
    {
        return Err(resource_limit("installer receipt exceeds 8 MiB"));
    }
    let receipt = serde_json::from_slice(bytes)
        .map_err(|error| corrupt(format!("installer receipt is invalid JSON: {error}")))?;
    validate_installer_migration_receipt(&receipt)?;
    Ok(receipt)
}

pub fn validate_installer_migration_receipt(receipt: &InstallerMigrationReceiptV1) -> Result<()> {
    if receipt.schema_version != INSTALLER_STORAGE_SCHEMA_VERSION
        || receipt.preflight.schema_version != INSTALLER_STORAGE_SCHEMA_VERSION
    {
        return Err(DbError::new(
            "0A000",
            "installer storage receipt schema version is not supported",
        )
        .with_hint("rerun preflight with this OrdaDB installer"));
    }
    validate_preflight(&receipt.preflight)?;
    if !valid_digest(&receipt.receipt_digest) {
        return Err(corrupt("installer storage receipt digest is malformed"));
    }
    let expected = digest_json(&receipt.preflight, "installer storage preflight")?;
    if expected != receipt.receipt_digest {
        return Err(corrupt(
            "installer storage receipt digest does not match its preflight",
        ));
    }
    Ok(())
}

fn inventory_from_classification(
    classification: &InstallerStorageClassificationV1,
    plan: Option<&MigrationPlanV2>,
) -> InstallerStorageInventoryV1 {
    let active = match classification.authority.as_ref() {
        Some(RootAuthority::V2(active)) => Some(active.as_ref()),
        _ => None,
    };
    InstallerStorageInventoryV1 {
        markers: classification.markers.clone(),
        migration: plan.map(|value| value.inventory.clone()),
        active_cluster_id: active.map(|value| value.cluster_manifest.cluster_id),
        active_cluster_generation: active.map(|value| value.cluster_manifest.generation),
        active_database_id: active.map(|value| value.database_manifest.database_id),
        active_database_generation: active.map(|value| value.storage.generation),
    }
}

fn fingerprint_from_classification(
    classification: &InstallerStorageClassificationV1,
    plan: Option<&MigrationPlanV2>,
) -> Result<InstallerSourceFingerprintV1> {
    let mut fingerprint = InstallerSourceFingerprintV1 {
        disposition: classification.disposition,
        markers: classification.markers.clone(),
        source_generation: None,
        data_file_bytes: 0,
        wal_file_bytes: 0,
        auth_file_bytes: 0,
        data_file_sha256: None,
        wal_file_sha256: None,
        auth_file_sha256: None,
        active_pointer_sha256: None,
        active_cluster_id: None,
        active_cluster_generation: None,
        active_database_id: None,
        planned_paths_sha256: plan
            .map(|value| digest_json(&value.paths, "installer migration paths"))
            .transpose()?,
    };
    match classification.authority.as_ref() {
        Some(RootAuthority::LegacyV1(inspection)) => {
            fingerprint.source_generation = Some(inspection.generation);
            fingerprint.data_file_bytes = inspection.file_bytes;
            fingerprint.wal_file_bytes =
                optional_file_bytes(&classification.data_dir.join(LEGACY_WAL_FILE))?;
            fingerprint.auth_file_bytes =
                optional_file_bytes(&classification.data_dir.join(AUTH_FILE_NAME))?;
            fingerprint.data_file_sha256 =
                Some(hash_file(&classification.data_dir.join(LEGACY_DATA_FILE))?);
            fingerprint.wal_file_sha256 =
                optional_file_sha256(&classification.data_dir.join(LEGACY_WAL_FILE))?;
            fingerprint.auth_file_sha256 =
                optional_file_sha256(&classification.data_dir.join(AUTH_FILE_NAME))?;
        }
        Some(RootAuthority::V2(active)) => {
            let data = active.database_dir.join(LEGACY_DATA_FILE);
            let wal = active.database_dir.join(LEGACY_WAL_FILE);
            let auth = active.roles_dir.join(AUTH_FILE_NAME);
            fingerprint.source_generation = Some(active.storage.generation);
            fingerprint.data_file_bytes = active.storage.file_bytes;
            fingerprint.wal_file_bytes = optional_file_bytes(&wal)?;
            fingerprint.auth_file_bytes = optional_file_bytes(&auth)?;
            fingerprint.data_file_sha256 = Some(hash_file(&data)?);
            fingerprint.wal_file_sha256 = optional_file_sha256(&wal)?;
            fingerprint.auth_file_sha256 = optional_file_sha256(&auth)?;
            fingerprint.active_pointer_sha256 = Some(hash_file(
                &classification.data_dir.join(ACTIVE_POINTER_FILE),
            )?);
            fingerprint.active_cluster_id = Some(active.cluster_manifest.cluster_id);
            fingerprint.active_cluster_generation = Some(active.cluster_manifest.generation);
            fingerprint.active_database_id = Some(active.database_manifest.database_id);
            let manifest_sha256 = hash_file(&active.cluster_dir.join(CLUSTER_MANIFEST_FILE))?;
            if manifest_sha256 != active.pointer.manifest_sha256 {
                return Err(corrupt(
                    "active cluster manifest changed during installer fingerprinting",
                ));
            }
        }
        Some(RootAuthority::Empty) | None => {}
    }
    Ok(fingerprint)
}

fn validate_preflight(preflight: &InstallerStoragePreflightV1) -> Result<()> {
    if preflight.schema_version != INSTALLER_STORAGE_SCHEMA_VERSION {
        return Err(DbError::new(
            "0A000",
            "installer storage preflight schema version is not supported",
        ));
    }
    if preflight.data_dir.as_os_str().is_empty()
        || preflight.data_dir.to_string_lossy().len() > 32_768
    {
        return Err(DbError::new(
            "22023",
            "installer storage data directory is empty or too long",
        ));
    }
    if preflight.phases.len() > MigrationPhaseV2::ordered().len()
        || preflight.incompatibilities.len() > MAX_INCOMPATIBILITIES
        || preflight.guidance.len() > MAX_GUIDANCE_BYTES
    {
        return Err(resource_limit(
            "installer storage preflight exceeds collection bounds",
        ));
    }
    for incompatibility in &preflight.incompatibilities {
        if incompatibility.code.len() > 64
            || incompatibility.message.len() > MAX_INCOMPATIBILITY_BYTES
            || incompatibility
                .hint
                .as_ref()
                .is_some_and(|value| value.len() > MAX_INCOMPATIBILITY_BYTES)
        {
            return Err(resource_limit(
                "installer storage incompatibility exceeds string bounds",
            ));
        }
    }
    if preflight.inventory.markers != preflight.source_fingerprint.markers
        || preflight.disposition != preflight.source_fingerprint.disposition
    {
        return Err(corrupt(
            "installer storage preflight inventory and fingerprint disagree",
        ));
    }
    let expected_safe = match preflight.disposition {
        InstallerStorageDisposition::Empty | InstallerStorageDisposition::ActiveV2 => {
            if preflight.migration_plan.is_some()
                || preflight.inventory.migration.is_some()
                || preflight.required_bytes != 0
                || preflight.backup_path.is_some()
                || preflight.rollback_path.is_some()
                || !preflight.phases.is_empty()
                || !preflight.incompatibilities.is_empty()
            {
                return Err(corrupt(
                    "non-migrating installer preflight contains migration state",
                ));
            }
            true
        }
        InstallerStorageDisposition::LegacyV1 => {
            let plan = preflight
                .migration_plan
                .as_ref()
                .ok_or_else(|| corrupt("legacy installer preflight has no migration plan"))?;
            validate_migration_plan(plan)?;
            let planned_paths_sha256 = digest_json(&plan.paths, "installer migration paths")?;
            if preflight.data_dir != plan.paths.cluster_root
                || preflight.inventory.migration.as_ref() != Some(&plan.inventory)
                || preflight.required_bytes != plan.bytes.required_bytes
                || preflight.free_bytes != plan.bytes.available_bytes
                || preflight.backup_path.as_ref() != Some(&plan.paths.logical_backup)
                || preflight.rollback_path.as_ref() != Some(&plan.paths.rollback_directory)
                || preflight.phases != plan.phases
                || preflight.source_fingerprint.planned_paths_sha256.as_deref()
                    != Some(planned_paths_sha256.as_str())
            {
                return Err(corrupt(
                    "legacy installer preflight does not match its migration plan",
                ));
            }
            preflight.incompatibilities.is_empty()
                && preflight.free_bytes >= preflight.required_bytes
        }
        InstallerStorageDisposition::Mixed
        | InstallerStorageDisposition::Corrupt
        | InstallerStorageDisposition::IncompleteMigration => {
            if preflight.migration_plan.is_some() || preflight.inventory.migration.is_some() {
                return Err(corrupt(
                    "unsafe installer preflight contains an executable migration plan",
                ));
            }
            false
        }
    };
    if preflight.safe_to_apply != expected_safe {
        return Err(corrupt(
            "installer storage preflight safe-to-apply flag is inconsistent",
        ));
    }
    let bytes = serde_json::to_vec(preflight)
        .map_err(|error| internal(format!("failed to encode installer preflight: {error}")))?;
    if u64::try_from(bytes.len())
        .map_err(|_| resource_limit("installer preflight length exceeds u64"))?
        > MAX_INSTALLER_RECEIPT_BYTES
    {
        return Err(resource_limit("installer storage preflight exceeds 8 MiB"));
    }
    Ok(())
}

fn incompatibility_from_error(error: &DbError) -> InstallerMigrationIncompatibilityV1 {
    InstallerMigrationIncompatibilityV1 {
        code: error.sql_state.clone(),
        message: bounded(&error.message, MAX_INCOMPATIBILITY_BYTES),
        hint: error
            .hint
            .as_deref()
            .map(|value| bounded(value, MAX_INCOMPATIBILITY_BYTES)),
    }
}

fn guidance_for(disposition: InstallerStorageDisposition, safe: bool) -> String {
    if safe {
        return match disposition {
            InstallerStorageDisposition::Empty => {
                "The data directory is empty; installation can continue without migration."
            }
            InstallerStorageDisposition::LegacyV1 => {
                "Confirm the displayed backup, rollback, and space plan before stopping the service."
            }
            InstallerStorageDisposition::ActiveV2 => {
                "The active v2 cluster is valid; installation can continue without storage changes."
            }
            _ => "Installer storage preflight completed.",
        }
        .to_owned();
    }
    match disposition {
        InstallerStorageDisposition::LegacyV1 => {
            "Resolve every incompatibility and free the required disk space before continuing."
        }
        InstallerStorageDisposition::Mixed => {
            "Select and preserve one authoritative data source before continuing."
        }
        InstallerStorageDisposition::Corrupt => {
            "Restore a verified backup or repair the durable authority before continuing."
        }
        InstallerStorageDisposition::IncompleteMigration => {
            "Inspect the migration journal and retain the authoritative source before continuing."
        }
        _ => "Installer storage is not safe to apply.",
    }
    .to_owned()
}

fn digest_json<T: Serialize>(value: &T, label: &str) -> Result<String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| internal(format!("failed to encode {label}: {error}")))?;
    Ok(digest_bytes(&bytes))
}

fn digest_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn bounded(value: &str, maximum: usize) -> String {
    let mut output = String::new();
    for character in value.chars() {
        if output.len() + character.len_utf8() > maximum {
            break;
        }
        output.push(character);
    }
    output
}

fn source_changed() -> DbError {
    DbError::new("55000", "installer storage source changed after preflight")
        .with_hint("keep the service stopped and rerun installer storage preflight")
}

fn resource_limit(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn corrupt(message: impl Into<String>) -> DbError {
    DbError::new("XX001", message)
        .with_hint("discard the receipt and rerun installer storage preflight")
}

fn internal(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use ordadb_cluster::{ACTIVE_POINTER_FILE, initialize_empty_v2};
    use ordadb_storage::DatabaseStore;
    use tempfile::tempdir;

    use super::*;

    fn unlimited() -> InstallerStorageOptionsV1 {
        InstallerStorageOptionsV1 {
            migration: MigrationRunOptionsV2 {
                available_bytes_override: Some(u64::MAX),
                ..MigrationRunOptionsV2::default()
            },
        }
    }

    #[test]
    fn legacy_receipt_is_versioned_bounded_and_revalidates_before_apply() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("legacy");
        drop(DatabaseStore::open(&root).expect("legacy store"));

        let receipt =
            installer_storage_preflight_with_options(&root, unlimited()).expect("preflight");
        assert_eq!(
            receipt.preflight.disposition,
            InstallerStorageDisposition::LegacyV1
        );
        assert!(receipt.preflight.safe_to_apply);
        assert!(receipt.preflight.backup_path.is_some());
        assert!(receipt.preflight.rollback_path.is_some());
        assert_eq!(receipt.receipt_digest.len(), 64);
        let bytes = serde_json::to_vec(&receipt).expect("receipt json");
        let decoded = decode_installer_migration_receipt(&bytes).expect("decode");
        assert_eq!(decoded, receipt);

        fs::write(root.join(AUTH_FILE_NAME), b"changed after preflight").expect("change auth");
        let error = apply_installer_storage_receipt_with_options(&root, &receipt, unlimited())
            .expect_err("source race");
        assert_eq!(error.sql_state, "55000");
        assert!(
            !receipt
                .preflight
                .migration_plan
                .as_ref()
                .expect("plan")
                .paths
                .journal
                .exists()
        );
    }

    #[test]
    fn empty_and_active_v2_apply_are_idempotent() {
        let directory = tempdir().expect("tempdir");
        let empty = directory.path().join("empty");
        let empty_receipt = installer_storage_preflight(&empty).expect("empty preflight");
        let first =
            apply_installer_storage_receipt(&empty, &empty_receipt).expect("empty apply first");
        let second =
            apply_installer_storage_receipt(&empty, &empty_receipt).expect("empty apply second");
        assert_eq!(first.action, InstallerStorageApplyActionV1::NoChange);
        assert_eq!(second.action, InstallerStorageApplyActionV1::NoChange);
        assert!(!empty.exists());

        let active = directory.path().join("active");
        initialize_empty_v2(&active).expect("active cluster");
        let receipt = installer_storage_preflight(&active).expect("active preflight");
        let pointer_before = fs::read(active.join(ACTIVE_POINTER_FILE)).expect("pointer before");
        let report = apply_installer_storage_receipt(&active, &receipt).expect("active v2 apply");
        assert_eq!(report.action, InstallerStorageApplyActionV1::NoChange);
        assert_eq!(
            fs::read(active.join(ACTIVE_POINTER_FILE)).expect("pointer after"),
            pointer_before
        );
    }

    #[test]
    fn exact_receipt_plan_migrates_and_untrusted_path_changes_are_rejected() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("legacy");
        drop(DatabaseStore::open(&root).expect("legacy store"));
        let receipt =
            installer_storage_preflight_with_options(&root, unlimited()).expect("preflight");

        let mut unsafe_preflight = receipt.preflight.clone();
        unsafe_preflight
            .migration_plan
            .as_mut()
            .expect("plan")
            .paths
            .logical_backup = directory.path().join("outside.ordbak");
        let unsafe_receipt = InstallerMigrationReceiptV1 {
            schema_version: INSTALLER_STORAGE_SCHEMA_VERSION,
            receipt_digest: digest_json(&unsafe_preflight, "tampered preflight").expect("redigest"),
            preflight: unsafe_preflight,
        };
        let error =
            apply_installer_storage_receipt_with_options(&root, &unsafe_receipt, unlimited())
                .expect_err("unsafe path");
        assert_eq!(error.sql_state, "22023");

        let report = apply_installer_storage_receipt_with_options(&root, &receipt, unlimited())
            .expect("apply");
        assert_eq!(report.action, InstallerStorageApplyActionV1::MigratedV1ToV2);
        assert_eq!(
            report.disposition_after,
            InstallerStorageDisposition::ActiveV2
        );
    }

    #[test]
    fn receipt_versions_digests_and_insufficient_space_fail_closed() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("legacy");
        drop(DatabaseStore::open(&root).expect("legacy store"));
        let limited = InstallerStorageOptionsV1 {
            migration: MigrationRunOptionsV2 {
                available_bytes_override: Some(1),
                ..MigrationRunOptionsV2::default()
            },
        };
        let receipt = installer_storage_preflight_with_options(&root, limited).expect("preflight");
        assert!(!receipt.preflight.safe_to_apply);
        assert!(
            receipt
                .preflight
                .incompatibilities
                .iter()
                .any(|value| value.code == "53100")
        );

        let mut unsupported = receipt.clone();
        unsupported.schema_version += 1;
        assert_eq!(
            validate_installer_migration_receipt(&unsupported)
                .expect_err("version")
                .sql_state,
            "0A000"
        );
        let mut corrupt = receipt;
        let replacement = if corrupt.receipt_digest.starts_with('0') {
            "1"
        } else {
            "0"
        };
        corrupt.receipt_digest.replace_range(..1, replacement);
        assert_eq!(
            validate_installer_migration_receipt(&corrupt)
                .expect_err("digest")
                .sql_state,
            "XX001"
        );
    }
}
