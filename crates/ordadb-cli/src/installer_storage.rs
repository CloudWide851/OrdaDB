use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use ordadb_backup::{
    InstallerMigrationReceiptV1, InstallerStorageApplyReportV1, MAX_INSTALLER_RECEIPT_BYTES,
    MAX_INSTALLER_REPORT_BYTES, MAX_INSTALLER_STATE_BYTES, apply_installer_storage_receipt,
    decode_installer_migration_receipt, installer_storage_preflight,
};
use ordadb_cluster::InstallerStorageDisposition;
use ordadb_types::{DbError, Result};
use serde::Serialize;
use serde_json::Value;

use super::{ensure_empty, internal, invalid, take, take_required};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InstallerApplyFileV1<'a> {
    schema_version: u32,
    ok: bool,
    data_dir: &'a Path,
    receipt_digest: Option<&'a str>,
    result: Option<&'a InstallerStorageApplyReportV1>,
    failure: Option<InstallerApplyFailureV1>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InstallerApplyFailureV1 {
    sql_state: String,
    phase: String,
    reason: String,
    hint: Option<String>,
}

pub(super) fn run(mut options: BTreeMap<String, Option<String>>) -> Result<Value> {
    let data_dir = PathBuf::from(take_required(&mut options, "--data-dir")?);
    let preflight_path = take(&mut options, "--preflight").map(PathBuf::from);
    let apply_path = take(&mut options, "--apply").map(PathBuf::from);
    match (preflight_path, apply_path) {
        (Some(receipt_path), None) => {
            let state_path = PathBuf::from(take_required(&mut options, "--state")?);
            ensure_empty(&options)?;
            if same_output_path(&receipt_path, &state_path)? {
                return Err(invalid("--preflight and --state must name different files"));
            }
            run_preflight(&data_dir, &receipt_path, &state_path)
        }
        (None, Some(receipt_path)) => {
            let report_path = PathBuf::from(take_required(&mut options, "--report")?);
            ensure_empty(&options)?;
            if same_output_path(&receipt_path, &report_path)? {
                return Err(invalid("--apply and --report must name different files"));
            }
            run_apply(&data_dir, &receipt_path, &report_path)
        }
        (Some(_), Some(_)) => Err(invalid("--preflight and --apply are mutually exclusive")),
        (None, None) => Err(invalid(
            "installer-storage requires exactly one of --preflight or --apply",
        )),
    }
}

fn run_preflight(data_dir: &Path, receipt_path: &Path, state_path: &Path) -> Result<Value> {
    let receipt = installer_storage_preflight(data_dir)?;
    write_json_atomic(
        receipt_path,
        &receipt,
        MAX_INSTALLER_RECEIPT_BYTES,
        "installer receipt",
    )?;
    let state = encode_state_ini(&receipt)?;
    write_atomic(
        state_path,
        state.as_bytes(),
        MAX_INSTALLER_STATE_BYTES,
        "installer state",
    )?;
    if receipt.preflight.disposition == InstallerStorageDisposition::Corrupt {
        let failure = receipt.preflight.incompatibilities.first();
        let sql_state = failure
            .map(|value| value.code.as_str())
            .filter(|value| value.len() == 5)
            .unwrap_or("XX001");
        let mut error = DbError::new(
            sql_state,
            "installer storage preflight found corrupt durable state",
        )
        .with_hint(receipt.preflight.guidance.clone());
        if let Some(incompatibility) = failure {
            error = error.with_detail(incompatibility.message.clone());
        }
        return Err(error);
    }
    serde_json::to_value(&receipt.preflight).map_err(|error| {
        internal(format!(
            "failed to encode installer preflight output: {error}"
        ))
    })
}

fn run_apply(data_dir: &Path, receipt_path: &Path, report_path: &Path) -> Result<Value> {
    let mut receipt_digest = None;
    let result = (|| {
        let bytes = read_bounded(
            receipt_path,
            MAX_INSTALLER_RECEIPT_BYTES,
            "installer receipt",
        )?;
        let receipt = decode_installer_migration_receipt(&bytes)?;
        receipt_digest = Some(receipt.receipt_digest.clone());
        apply_installer_storage_receipt(data_dir, &receipt)
    })();
    match result {
        Ok(report) => {
            let output = InstallerApplyFileV1 {
                schema_version: 1,
                ok: true,
                data_dir,
                receipt_digest: receipt_digest.as_deref(),
                result: Some(&report),
                failure: None,
            };
            write_json_atomic(
                report_path,
                &output,
                MAX_INSTALLER_REPORT_BYTES,
                "installer apply report",
            )?;
            serde_json::to_value(report).map_err(|error| {
                internal(format!("failed to encode installer apply output: {error}"))
            })
        }
        Err(error) => {
            let output = InstallerApplyFileV1 {
                schema_version: 1,
                ok: false,
                data_dir,
                receipt_digest: receipt_digest.as_deref(),
                result: None,
                failure: Some(apply_failure(&error)),
            };
            write_json_atomic(
                report_path,
                &output,
                MAX_INSTALLER_REPORT_BYTES,
                "installer apply failure report",
            )?;
            Err(error)
        }
    }
}

fn encode_state_ini(receipt: &InstallerMigrationReceiptV1) -> Result<String> {
    let preflight = &receipt.preflight;
    let source_bytes = preflight
        .source_fingerprint
        .data_file_bytes
        .checked_add(preflight.source_fingerprint.wal_file_bytes)
        .and_then(|value| value.checked_add(preflight.source_fingerprint.auth_file_bytes))
        .ok_or_else(|| DbError::new("54000", "installer source byte count overflowed"))?;
    let phases = preflight
        .phases
        .iter()
        .map(|phase| {
            serde_json::to_value(phase)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .ok_or_else(|| internal("failed to encode installer migration phase"))
        })
        .collect::<Result<Vec<_>>>()?
        .join(",");
    let incompatibilities = preflight
        .incompatibilities
        .iter()
        .map(|value| format!("{}: {}", value.code, value.message))
        .collect::<Vec<_>>()
        .join(" | ");
    let failure = (!preflight.safe_to_apply)
        .then(|| preflight.incompatibilities.first())
        .flatten();
    let summary = format!(
        "{}: {}",
        disposition_name(preflight.disposition),
        preflight.guidance
    );
    let values = [
        ("schemaVersion", preflight.schema_version.to_string()),
        (
            "disposition",
            disposition_name(preflight.disposition).to_owned(),
        ),
        (
            "safeToApply",
            if preflight.safe_to_apply { "1" } else { "0" }.to_owned(),
        ),
        ("dataDir", preflight.data_dir.display().to_string()),
        ("summary", summary),
        ("sourceBytes", source_bytes.to_string()),
        ("requiredBytes", preflight.required_bytes.to_string()),
        ("freeBytes", preflight.free_bytes.to_string()),
        (
            "backupPath",
            preflight
                .backup_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
        ),
        (
            "rollbackPath",
            preflight
                .rollback_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
        ),
        (
            "failureSqlState",
            failure.map(|value| value.code.clone()).unwrap_or_default(),
        ),
        (
            "failurePhase",
            if failure.is_some() {
                "preflight".to_owned()
            } else {
                String::new()
            },
        ),
        (
            "failureReason",
            failure
                .map(|value| value.message.clone())
                .unwrap_or_default(),
        ),
        (
            "failureHint",
            failure
                .and_then(|value| value.hint.clone())
                .unwrap_or_else(|| {
                    if preflight.safe_to_apply {
                        String::new()
                    } else {
                        preflight.guidance.clone()
                    }
                }),
        ),
        ("phases", phases),
        ("incompatibilities", incompatibilities),
        ("guidance", preflight.guidance.clone()),
        ("receiptDigest", receipt.receipt_digest.clone()),
    ];
    let mut output = String::from("[installer]\r\n");
    for (key, value) in values {
        output.push_str(key);
        output.push('=');
        output.push_str(&escape_ini_value(&value)?);
        output.push_str("\r\n");
    }
    if u64::try_from(output.len())
        .map_err(|_| DbError::new("54000", "installer state length exceeds u64"))?
        > MAX_INSTALLER_STATE_BYTES
    {
        return Err(DbError::new("54000", "installer state INI exceeds 64 KiB"));
    }
    Ok(output)
}

fn disposition_name(disposition: InstallerStorageDisposition) -> &'static str {
    match disposition {
        InstallerStorageDisposition::Empty => "empty",
        InstallerStorageDisposition::LegacyV1 => "legacyV1",
        InstallerStorageDisposition::ActiveV2 => "activeV2",
        InstallerStorageDisposition::Mixed => "mixed",
        InstallerStorageDisposition::Corrupt => "corrupt",
        InstallerStorageDisposition::IncompleteMigration => "incompleteMigration",
    }
}

fn escape_ini_value(value: &str) -> Result<String> {
    let mut escaped = String::new();
    for character in value.chars() {
        match character {
            '\r' => escaped.push_str("%0D"),
            '\n' => escaped.push_str("%0A"),
            '%' => escaped.push_str("%25"),
            '=' => escaped.push_str("%3D"),
            ';' => escaped.push_str("%3B"),
            character if character.is_control() => escaped.push('?'),
            character => escaped.push(character),
        }
        if escaped.len() > 8192 {
            return Err(DbError::new(
                "54000",
                "installer state value exceeds 8192 bytes",
            ));
        }
    }
    Ok(escaped)
}

fn apply_failure(error: &DbError) -> InstallerApplyFailureV1 {
    let phase = match error.sql_state.as_str() {
        "22023" | "0A000" | "XX001" => "validateReceipt",
        "53100" => "revalidateSpace",
        "55000" => "revalidateSource",
        _ => "migration",
    };
    InstallerApplyFailureV1 {
        sql_state: bounded_text(&error.sql_state, 5),
        phase: phase.to_owned(),
        reason: bounded_text(&error.message, 1024),
        hint: error.hint.as_deref().map(|value| bounded_text(value, 1024)),
    }
}

fn bounded_text(value: &str, maximum: usize) -> String {
    let mut output = String::new();
    for character in value.chars() {
        if output.len() + character.len_utf8() > maximum {
            break;
        }
        output.push(character);
    }
    output
}

fn read_bounded(path: &Path, maximum: u64, label: &str) -> Result<Vec<u8>> {
    let file =
        File::open(path).map_err(|error| io_error(format!("failed to open {label}"), error))?;
    let length = file
        .metadata()
        .map_err(|error| io_error(format!("failed to inspect {label}"), error))?
        .len();
    if length > maximum {
        return Err(DbError::new(
            "54000",
            format!("{label} exceeds its {maximum}-byte limit"),
        ));
    }
    let take = maximum
        .checked_add(1)
        .ok_or_else(|| DbError::new("54000", format!("{label} read limit overflowed")))?;
    let mut bytes = Vec::new();
    file.take(take)
        .read_to_end(&mut bytes)
        .map_err(|error| io_error(format!("failed to read {label}"), error))?;
    if u64::try_from(bytes.len())
        .map_err(|_| DbError::new("54000", format!("{label} length exceeds u64")))?
        > maximum
    {
        return Err(DbError::new(
            "54000",
            format!("{label} exceeds its {maximum}-byte limit"),
        ));
    }
    Ok(bytes)
}

fn write_json_atomic<T: Serialize>(
    path: &Path,
    value: &T,
    maximum: u64,
    label: &str,
) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| internal(format!("failed to encode {label}: {error}")))?;
    write_atomic(path, &bytes, maximum, label)
}

fn write_atomic(path: &Path, bytes: &[u8], maximum: u64, label: &str) -> Result<()> {
    if u64::try_from(bytes.len())
        .map_err(|_| DbError::new("54000", format!("{label} length exceeds u64")))?
        > maximum
    {
        return Err(DbError::new(
            "54000",
            format!("{label} exceeds its {maximum}-byte limit"),
        ));
    }
    let destination = normalized_output_path(path)?;
    if destination.exists() {
        return Err(
            DbError::new("55000", format!("{label} destination already exists"))
                .with_hint("choose a fresh installer temporary output path"),
        );
    }
    let parent = destination
        .parent()
        .ok_or_else(|| invalid("installer output path has no parent directory"))?;
    let file_name = destination
        .file_name()
        .ok_or_else(|| invalid("installer output path has no file name"))?
        .to_string_lossy();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let temporary = parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| io_error(format!("failed to create temporary {label}"), error))?;
        file.write_all(bytes)
            .map_err(|error| io_error(format!("failed to write temporary {label}"), error))?;
        file.sync_all()
            .map_err(|error| io_error(format!("failed to synchronize temporary {label}"), error))?;
        fs::rename(&temporary, &destination)
            .map_err(|error| io_error(format!("failed to publish {label}"), error))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn normalized_output_path(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(invalid("installer output path cannot be empty"));
    }
    let absolute = std::path::absolute(path)
        .map_err(|error| io_error("failed to normalize installer output path", error))?;
    let parent = absolute
        .parent()
        .ok_or_else(|| invalid("installer output path has no parent directory"))?
        .canonicalize()
        .map_err(|error| io_error("failed to resolve installer output parent", error))?;
    if !parent.is_dir() {
        return Err(invalid("installer output parent is not a directory"));
    }
    let file_name = absolute
        .file_name()
        .ok_or_else(|| invalid("installer output path has no file name"))?;
    let destination = parent.join(file_name);
    match fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(invalid("installer output destination is a symbolic link"))
        }
        Ok(_) => Ok(destination),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(destination),
        Err(error) => Err(io_error(
            "failed to inspect installer output destination",
            error,
        )),
    }
}

fn same_output_path(left: &Path, right: &Path) -> Result<bool> {
    Ok(normalized_output_path(left)? == normalized_output_path(right)?)
}

fn io_error(context: impl Into<String>, error: std::io::Error) -> DbError {
    DbError::new("58030", context)
        .with_detail(error.to_string())
        .with_hint("check the installer temporary path and filesystem permissions")
}

#[cfg(test)]
mod tests {
    use ordadb_storage::DatabaseStore;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn preflight_and_apply_write_bounded_machine_readable_files() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("legacy");
        drop(DatabaseStore::open(&root).expect("legacy store"));
        let receipt = directory.path().join("receipt.json");
        let state = directory.path().join("state.ini");
        let report = directory.path().join("report.json");

        let result = run(BTreeMap::from([
            ("--data-dir".into(), Some(root.display().to_string())),
            ("--preflight".into(), Some(receipt.display().to_string())),
            ("--state".into(), Some(state.display().to_string())),
        ]))
        .expect("preflight");
        assert_eq!(result["disposition"], "legacyV1");
        let state_text = fs::read_to_string(&state).expect("state");
        assert!(state_text.starts_with("[installer]\r\n"));
        assert!(state_text.contains("safeToApply=1\r\n"));
        assert!(state_text.contains("summary=legacyV1:"));
        assert!(state_text.contains("failureSqlState=\r\n"));
        assert!(state_text.contains("failurePhase=\r\n"));
        assert!(state_text.contains("failureReason=\r\n"));
        assert!(state_text.contains("failureHint=\r\n"));
        assert!(state_text.contains("receiptDigest="));
        assert!(!state_text.contains('\n') || !state_text.contains("\n\n"));

        let result = run(BTreeMap::from([
            ("--data-dir".into(), Some(root.display().to_string())),
            ("--apply".into(), Some(receipt.display().to_string())),
            ("--report".into(), Some(report.display().to_string())),
        ]))
        .expect("apply");
        assert_eq!(result["action"], "migratedV1ToV2");
        let report: Value =
            serde_json::from_slice(&fs::read(report).expect("report")).expect("report json");
        assert_eq!(report["ok"], true);
        assert_eq!(
            report["result"]["dispositionAfter"],
            serde_json::json!("activeV2")
        );
    }

    #[test]
    fn corrupt_preflight_writes_state_then_returns_nonzero_error() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("corrupt");
        fs::create_dir_all(&root).expect("root");
        fs::write(root.join("ordadb.data"), b"corrupt").expect("data");
        let receipt = directory.path().join("receipt.json");
        let state = directory.path().join("state.ini");

        let error = run(BTreeMap::from([
            ("--data-dir".into(), Some(root.display().to_string())),
            ("--preflight".into(), Some(receipt.display().to_string())),
            ("--state".into(), Some(state.display().to_string())),
        ]))
        .expect_err("corrupt");

        assert_eq!(error.sql_state, "XX001");
        assert!(receipt.is_file());
        assert!(state.is_file());
        assert!(
            fs::read_to_string(state)
                .expect("state")
                .contains("disposition=corrupt")
        );
    }

    #[test]
    fn modes_are_strict_and_atomic_outputs_are_create_new() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("empty");
        let receipt = directory.path().join("receipt.json");
        let state = directory.path().join("state.ini");
        let report = directory.path().join("report.json");
        let both = run(BTreeMap::from([
            ("--data-dir".into(), Some(root.display().to_string())),
            ("--preflight".into(), Some(receipt.display().to_string())),
            ("--apply".into(), Some(receipt.display().to_string())),
            ("--state".into(), Some(state.display().to_string())),
            ("--report".into(), Some(report.display().to_string())),
        ]))
        .expect_err("mutually exclusive");
        assert_eq!(both.sql_state, "22023");

        run(BTreeMap::from([
            ("--data-dir".into(), Some(root.display().to_string())),
            ("--preflight".into(), Some(receipt.display().to_string())),
            ("--state".into(), Some(state.display().to_string())),
        ]))
        .expect("first");
        let second = run(BTreeMap::from([
            ("--data-dir".into(), Some(root.display().to_string())),
            ("--preflight".into(), Some(receipt.display().to_string())),
            ("--state".into(), Some(state.display().to_string())),
        ]))
        .expect_err("create new");
        assert_eq!(second.sql_state, "55000");
    }

    #[test]
    fn apply_failure_writes_a_bounded_report_and_returns_the_source_error() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("legacy");
        drop(DatabaseStore::open(&root).expect("legacy store"));
        let receipt = directory.path().join("receipt.json");
        let state = directory.path().join("state.ini");
        let report = directory.path().join("failure.json");
        run(BTreeMap::from([
            ("--data-dir".into(), Some(root.display().to_string())),
            ("--preflight".into(), Some(receipt.display().to_string())),
            ("--state".into(), Some(state.display().to_string())),
        ]))
        .expect("preflight");
        fs::write(root.join("ordadb.auth.json"), b"changed").expect("source change");

        let error = run(BTreeMap::from([
            ("--data-dir".into(), Some(root.display().to_string())),
            ("--apply".into(), Some(receipt.display().to_string())),
            ("--report".into(), Some(report.display().to_string())),
        ]))
        .expect_err("apply source race");

        assert_eq!(error.sql_state, "55000");
        let report: Value =
            serde_json::from_slice(&fs::read(report).expect("failure report")).expect("json");
        assert_eq!(report["ok"], false);
        assert_eq!(report["failure"]["sqlState"], "55000");
        assert_eq!(report["failure"]["phase"], "revalidateSource");
    }
}
