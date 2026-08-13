
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
