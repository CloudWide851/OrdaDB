
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
    fn installer_classification_covers_all_six_dispositions_without_mutation() {
        let directory = tempdir().expect("tempdir");

        let empty = directory.path().join("empty");
        let classified = classify_installer_storage(&empty).expect("empty classification");
        assert_eq!(classified.disposition, InstallerStorageDisposition::Empty);
        assert!(!empty.exists());

        let legacy = directory.path().join("legacy");
        drop(DatabaseStore::open(&legacy).expect("legacy store"));
        let legacy_before = fs::read(legacy.join(LEGACY_DATA_FILE)).expect("legacy before");
        let classified = classify_installer_storage(&legacy).expect("legacy classification");
        assert_eq!(
            classified.disposition,
            InstallerStorageDisposition::LegacyV1
        );
        assert_eq!(
            fs::read(legacy.join(LEGACY_DATA_FILE)).expect("legacy after"),
            legacy_before
        );

        let active = directory.path().join("active");
        initialize_empty_v2(&active).expect("active cluster");
        let active_pointer_before =
            fs::read(active.join(ACTIVE_POINTER_FILE)).expect("active pointer before");
        let classified = classify_installer_storage(&active).expect("active classification");
        assert_eq!(
            classified.disposition,
            InstallerStorageDisposition::ActiveV2
        );
        assert_eq!(
            fs::read(active.join(ACTIVE_POINTER_FILE)).expect("active pointer after"),
            active_pointer_before
        );

        let mixed = directory.path().join("mixed");
        initialize_empty_v2(&mixed).expect("mixed cluster");
        drop(DatabaseStore::open(&mixed).expect("mixed legacy store"));
        let classified = classify_installer_storage(&mixed).expect("mixed classification");
        assert_eq!(classified.disposition, InstallerStorageDisposition::Mixed);

        let corrupt_root = directory.path().join("corrupt");
        fs::create_dir_all(&corrupt_root).expect("corrupt root");
        fs::write(corrupt_root.join(LEGACY_DATA_FILE), b"not a database").expect("corrupt data");
        let corrupt_before = fs::read(corrupt_root.join(LEGACY_DATA_FILE)).expect("corrupt before");
        let classified = classify_installer_storage(&corrupt_root).expect("corrupt classification");
        assert_eq!(classified.disposition, InstallerStorageDisposition::Corrupt);
        assert_eq!(
            fs::read(corrupt_root.join(LEGACY_DATA_FILE)).expect("corrupt after"),
            corrupt_before
        );

        let incomplete = directory.path().join("incomplete");
        drop(DatabaseStore::open(&incomplete).expect("incomplete legacy store"));
        fs::create_dir_all(incomplete.join("migration")).expect("migration directory");
        fs::write(
            incomplete.join("migration").join(MIGRATION_JOURNAL_FILE),
            serde_json::to_vec(&serde_json::json!({
                "formatVersion": 2,
                "phase": "candidateBuilt"
            }))
            .expect("journal json"),
        )
        .expect("journal");
        let journal_before = fs::read(incomplete.join("migration").join(MIGRATION_JOURNAL_FILE))
            .expect("journal before");
        let classified =
            classify_installer_storage(&incomplete).expect("incomplete classification");
        assert_eq!(
            classified.disposition,
            InstallerStorageDisposition::IncompleteMigration
        );
        assert_eq!(
            fs::read(incomplete.join("migration").join(MIGRATION_JOURNAL_FILE))
                .expect("journal after"),
            journal_before
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
