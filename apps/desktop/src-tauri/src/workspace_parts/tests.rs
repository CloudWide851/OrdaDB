
#[cfg(test)]
mod tests {
    use std::os::windows::fs::symlink_dir;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn settings_and_recovery_are_atomic_and_secret_free() {
        let directory = tempdir().expect("tempdir");
        let runtime = ConsoleRuntime::open(directory.path().join("state")).expect("runtime");
        assert_eq!(
            runtime
                .bootstrap()
                .expect("bootstrap")
                .settings
                .appearance
                .ui_font_size,
            11
        );

        let root = directory.path().join("project");
        fs::create_dir_all(&root).expect("project");
        fs::write(root.join("draft.sql"), "select 1;").expect("sql");
        runtime
            .save_session(WorkspaceSessionV1 {
                format_version: 1,
                root_path: Some(root.display().to_string()),
                active_path: Some("draft.sql".into()),
                open_documents: vec![WorkspaceDraft {
                    path: "draft.sql".into(),
                    locator: None,
                    name: None,
                    content: "select 2;".into(),
                    base_revision: None,
                }],
            })
            .expect("session");
        let encoded =
            fs::read_to_string(runtime.root.join(SESSION_FILE)).expect("read session state");
        assert!(encoded.contains("select 2;"));
        assert!(!encoded.to_ascii_lowercase().contains("password"));
        assert!(runtime.bootstrap().expect("bootstrap").recovery.is_some());
    }

    #[test]
    fn workspace_enforces_utf8_limits_and_external_revision_conflicts() {
        let directory = tempdir().expect("tempdir");
        let runtime = ConsoleRuntime::open(directory.path().join("state")).expect("runtime");
        let project = directory.path().join("project");
        fs::create_dir_all(project.join("nested")).expect("project");
        fs::write(project.join("nested").join("query.sql"), "select 1;").expect("sql");
        fs::write(project.join("ignored.txt"), "ignored").expect("text");

        let snapshot = runtime
            .snapshot(&project.display().to_string())
            .expect("snapshot");
        assert_eq!(snapshot.entries.len(), 2);
        assert_eq!(snapshot.entries[1].path, "nested/query.sql");

        let document = runtime
            .open_document(&DocumentRequest {
                root_path: project.display().to_string(),
                path: "nested/query.sql".into(),
            })
            .expect("document");
        assert_eq!(document.revision.size_bytes, document.content.len() as u64);
        assert_eq!(
            document.revision.sha256,
            Sha256::digest(document.content.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        fs::write(project.join("nested").join("query.sql"), "select 2;").expect("external");
        let error = runtime
            .save_document(&SaveDocumentRequest {
                root_path: project.display().to_string(),
                path: "nested/query.sql".into(),
                content: "select 3;".into(),
                expected_revision: Some(document.revision),
                force: false,
            })
            .expect_err("conflict");
        assert_eq!(error.sql_state, "40001");
    }

    #[test]
    fn external_documents_save_as_recent_files_and_untitled_recovery_are_bounded() {
        let directory = tempdir().expect("tempdir");
        let runtime = ConsoleRuntime::open(directory.path().join("state")).expect("runtime");
        let external = directory.path().join("outside.sql");
        fs::write(&external, "select 1;").expect("external SQL");

        let opened = runtime
            .open_external_document(&ExternalDocumentRequest {
                path: external.display().to_string(),
            })
            .expect("open external");
        assert!(matches!(
            &opened.locator,
            DocumentLocator::External { path } if Path::new(path).is_absolute()
        ));
        runtime
            .open_external_document(&ExternalDocumentRequest {
                path: external.display().to_string(),
            })
            .expect("reopen external");
        assert_eq!(
            runtime
                .load_recent_files()
                .expect("recent files")
                .entries
                .len(),
            1,
            "normalized absolute paths must be deduplicated"
        );

        fs::write(&external, "select 2;").expect("external edit");
        let error = runtime
            .save_external_document(&SaveExternalDocumentRequest {
                path: external.display().to_string(),
                content: "select 3;".into(),
                expected_revision: Some(opened.revision),
                force: false,
            })
            .expect_err("external conflict");
        assert_eq!(error.sql_state, "40001");

        let saved = runtime
            .save_document_as_path(
                &SaveDocumentAsRequest {
                    content: "select 42;".into(),
                    suggested_name: "query.sql".into(),
                },
                &directory.path().join("saved-query"),
            )
            .expect("Save As");
        assert_eq!(saved.name, "saved-query.sql");
        assert_eq!(
            fs::read_to_string(directory.path().join("saved-query.sql")).expect("saved SQL"),
            "select 42;"
        );

        runtime
            .save_session(WorkspaceSessionV1 {
                format_version: 1,
                root_path: None,
                active_path: Some("untitled:1".into()),
                open_documents: vec![WorkspaceDraft {
                    path: "untitled:1".into(),
                    locator: Some(DocumentLocator::Untitled {
                        id: "untitled-1".into(),
                    }),
                    name: Some("未命名-1.sql".into()),
                    content: "select now();".into(),
                    base_revision: None,
                }],
            })
            .expect("untitled recovery");
        assert!(runtime.bootstrap().expect("bootstrap").recovery.is_some());
    }

    #[test]
    fn workspace_rejects_intermediate_reparse_points() {
        let directory = tempdir().expect("tempdir");
        let runtime = ConsoleRuntime::open(directory.path().join("state")).expect("runtime");
        let project = directory.path().join("project");
        let real = project.join("real");
        fs::create_dir_all(&real).expect("project");
        fs::write(real.join("query.sql"), "select 1;").expect("sql");
        let alias = project.join("alias");
        if let Err(error) = symlink_dir(&real, &alias) {
            if error.raw_os_error() == Some(1314) {
                return;
            }
            panic!("create directory symlink: {error}");
        }

        let snapshot = runtime
            .snapshot(&project.display().to_string())
            .expect("snapshot");
        assert!(
            snapshot
                .entries
                .iter()
                .all(|entry| !entry.path.starts_with("alias"))
        );
        let error = runtime
            .open_document(&DocumentRequest {
                root_path: project.display().to_string(),
                path: "alias/query.sql".into(),
            })
            .expect_err("reparse point");
        assert_eq!(error.sql_state, "22023");
        assert!(error.message.contains("reparse"));
    }

    #[test]
    fn profiles_persist_only_credential_references() {
        let directory = tempdir().expect("tempdir");
        let runtime = ConsoleRuntime::open(directory.path().join("state")).expect("runtime");
        let profiles = runtime
            .save_profile(
                ConnectionProfileV1 {
                    format_version: 1,
                    profile_id: "local".into(),
                    label: "本地 OrdaDB".into(),
                    connector_id: NATIVE_CONNECTOR_ID.into(),
                    dialect: "postgresql".into(),
                    endpoint: "127.0.0.1:54329".into(),
                    admin_endpoint: Some("http://127.0.0.1:9080".into()),
                    database: Some("ordadb".into()),
                    credential_id: "local-credential".into(),
                    auto_reconnect: true,
                }
                .into(),
            )
            .expect("profile");
        assert_eq!(profiles.len(), 1);
        let encoded = fs::read_to_string(runtime.root.join(PROFILES_FILE)).expect("read profiles");
        assert!(encoded.contains("local-credential"));
        assert!(!encoded.to_ascii_lowercase().contains("password"));
        assert!(!encoded.to_ascii_lowercase().contains("api key"));
    }

    #[test]
    fn legacy_connector_ids_migrate_atomically_without_changing_credentials() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("state");
        fs::create_dir_all(&root).expect("state directory");
        let document = ConnectionProfilesV1 {
            format_version: 1,
            profiles: vec![ConnectionProfileV1 {
                format_version: 1,
                profile_id: "legacy-local".into(),
                label: "Legacy local".into(),
                connector_id: "ordadb-postgresql".into(),
                dialect: "postgresql".into(),
                endpoint: "127.0.0.1:54329".into(),
                admin_endpoint: Some("http://127.0.0.1:9080".into()),
                database: Some("ordadb".into()),
                credential_id: "credential-reference".into(),
                auto_reconnect: true,
            }],
        };
        write_json_atomic(&root.join(LEGACY_PROFILES_FILE), &document).expect("legacy document");

        let runtime = ConsoleRuntime::open(root).expect("runtime");
        let migrated = runtime.load_profiles().expect("migrated profiles");
        assert_eq!(migrated.profiles[0].connector_id, NATIVE_CONNECTOR_ID);
        assert_eq!(
            migrated.profiles[0].data_source_kind,
            DataSourceKind::OrdadbNative
        );
        assert_eq!(
            migrated.profiles[0].credential_id, "credential-reference",
            "the Credential Manager reference must survive ID migration"
        );
        assert_eq!(
            migrated.profiles[0].credential_access,
            CredentialAccess::Unspecified,
            "legacy credentials must never be assumed read-only"
        );
        let persisted =
            fs::read_to_string(runtime.root.join(PROFILES_FILE)).expect("persisted migration");
        assert!(persisted.contains("\"connectorId\": \"ordadb-native\""));
        assert!(persisted.contains("\"credentialAccess\": \"unspecified\""));
        assert!(!persisted.contains("ordadb-postgresql"));
    }

    #[test]
    fn legacy_settings_migrate_to_v2_without_deleting_the_source() {
        let directory = tempdir().expect("tempdir");
        let root = directory.path().join("state");
        fs::create_dir_all(&root).expect("state directory");
        let legacy = ConsoleSettingsV1 {
            format_version: 1,
            ui_font_size: 10,
            data_font_size: 13,
            editor_font_size: 14,
            density: "compact".into(),
            reopen_last_project: true,
            hide_empty_catalog: false,
        };
        write_json_atomic(&root.join(LEGACY_SETTINGS_FILE), &legacy).expect("legacy settings");

        let runtime = ConsoleRuntime::open(root).expect("runtime");
        let migrated = runtime.load_settings().expect("migrated settings");
        assert_eq!(migrated.format_version, 2);
        assert_eq!(migrated.appearance.ui_font_size, 10);
        assert_eq!(migrated.appearance.data_font_size, 13);
        assert_eq!(migrated.editor.font_size, 14);
        assert!(migrated.files.reopen_last_project);
        assert!(!migrated.appearance.hide_empty_catalog);
        assert!(runtime.root.join(LEGACY_SETTINGS_FILE).exists());
        assert!(runtime.root.join(SETTINGS_FILE).exists());
    }

    #[test]
    fn postgresql_profiles_cannot_carry_native_admin_fields() {
        let mut profile = ConnectionProfileV3 {
            format_version: PROFILES_VERSION,
            profile_id: "external-pg".into(),
            label: "PostgreSQL".into(),
            data_source_kind: DataSourceKind::Postgresql,
            connector_id: "postgresql".into(),
            connector_kind: ConnectorKind::Sql,
            command_language: "postgresql-sql".into(),
            dialect: Some("postgresql".into()),
            endpoint: "127.0.0.1:5432".into(),
            admin_endpoint: None,
            database: Some("postgres".into()),
            tls_mode: "prefer".into(),
            credential_id: "external-credential".into(),
            credential_access: CredentialAccess::ReadOnly,
            auto_reconnect: false,
        };
        validate_profile_v3(&profile).expect("valid external PostgreSQL");
        profile.admin_endpoint = Some("http://127.0.0.1:9080".into());
        let error = validate_profile_v3(&profile).expect_err("native admin endpoint rejected");
        assert_eq!(error.sql_state, "22023");
    }

    #[test]
    fn connector_descriptors_cover_ten_unique_native_data_models() {
        let descriptors = connector_descriptors();
        assert_eq!(descriptors.len(), 10);
        assert_eq!(
            descriptors
                .iter()
                .map(|descriptor| descriptor.connector_id)
                .collect::<BTreeSet<_>>()
                .len(),
            10
        );
        let mongodb = descriptors
            .iter()
            .find(|descriptor| descriptor.connector_id == "mongodb")
            .expect("MongoDB descriptor");
        assert_eq!(mongodb.connector_kind, ConnectorKind::Document);
        assert_eq!(mongodb.command_language, "mongodb-json");
        assert_eq!(mongodb.editor_mode, "json");
        assert_eq!(mongodb.dialect, None);
        let redis = descriptors
            .iter()
            .find(|descriptor| descriptor.connector_id == "redis")
            .expect("Redis descriptor");
        assert_eq!(redis.connector_kind, ConnectorKind::KeyValue);
        assert_eq!(redis.command_language, "redis-resp3");
        assert_eq!(redis.editor_mode, "plaintext");
        assert_eq!(redis.dialect, None);
    }

    #[test]
    fn profile_v3_rejects_unknown_and_sql_shaped_non_sql_connectors() {
        let profile = ConnectionProfileV3 {
            format_version: PROFILES_VERSION,
            profile_id: "mongodb-local".into(),
            label: "MongoDB".into(),
            data_source_kind: DataSourceKind::Mongodb,
            connector_id: "mongodb".into(),
            connector_kind: ConnectorKind::Document,
            command_language: "mongodb-json".into(),
            dialect: None,
            endpoint: "127.0.0.1:27017".into(),
            admin_endpoint: None,
            database: Some("admin".into()),
            tls_mode: "prefer".into(),
            credential_id: "mongodb-credential".into(),
            credential_access: CredentialAccess::Unspecified,
            auto_reconnect: false,
        };
        validate_profile_v3(&profile).expect("native MongoDB profile");

        let mut sql_shaped = profile.clone();
        sql_shaped.connector_kind = ConnectorKind::Sql;
        sql_shaped.command_language = "postgresql-sql".into();
        sql_shaped.dialect = Some("postgresql".into());
        assert_eq!(
            validate_profile_v3(&sql_shaped)
                .expect_err("MongoDB cannot be represented as SQL")
                .sql_state,
            "22023"
        );

        let mut unknown = profile;
        unknown.connector_id = "unknown-database".into();
        unknown.data_source_kind = DataSourceKind::Postgresql;
        unknown.connector_kind = ConnectorKind::Sql;
        unknown.command_language = "unknown".into();
        assert_eq!(
            validate_profile_v3(&unknown)
                .expect_err("unknown connector")
                .sql_state,
            "22023"
        );
    }
}
