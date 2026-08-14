
#[cfg(test)]
mod tests {
    use ordadb_types::{PgArray, PgInterval, ScalarType};

    use super::*;

    #[test]
    fn credential_prompt_request_contains_no_secret_field() {
        let request = PromptCredentialRequest {
            credential_id: "local".into(),
            connector_id: NATIVE_CONNECTOR_ID.into(),
            suggested_username: "dba".into(),
        };
        let debug = format!("{request:?}");
        assert!(debug.contains("suggested_username"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("dba"));
        assert!(!debug.contains("password"));

        let saved = CredentialSaved {
            credential_id: "local".to_owned(),
        };
        assert_eq!(
            serde_json::to_value(saved).expect("serialize saved credential"),
            serde_json::json!({"credentialId": "local"})
        );
        assert!(
            serde_json::from_value::<PromptCredentialRequest>(serde_json::json!({
                "credentialId": "local",
                "connectorId": NATIVE_CONNECTOR_ID,
                "suggestedUsername": "dba",
                "password": null
            }))
            .is_err()
        );
    }

    #[test]
    fn bootstrap_request_debug_is_redacted_and_probe_stages_are_structured() {
        let connection = native_connect_request();
        let request = BootstrapAdminRequest {
            ticket: "bootstrap-ticket-secret".into(),
            connection,
            suggested_username: "ordadb_admin".into(),
        };
        let debug = format!("{request:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("bootstrap-ticket-secret"));
        assert!(!debug.contains("password"));

        let mut probe = ConnectionProbe::new();
        probe.passed(ConnectionProbeStageName::Service);
        probe.passed(ConnectionProbeStageName::PgPort);
        probe.passed(ConnectionProbeStageName::AdminApi);
        probe.failed(
            ConnectionProbeStageName::Initialization,
            DbError::new("55000", "administrator bootstrap required"),
        );
        probe.skipped(ConnectionProbeStageName::Authentication);
        probe.skipped(ConnectionProbeStageName::Catalog);
        probe.finish();
        let value = serde_json::to_value(probe).expect("serialize probe");
        assert_eq!(value["ready"], false);
        assert_eq!(value["stages"][1]["stage"], "pgPort");
        assert_eq!(value["stages"][3]["status"], "failed");
        assert_eq!(value["stages"][3]["error"]["sqlState"], "55000");
        assert!(value["bootstrapTicket"].is_null());
    }

    #[test]
    fn bootstrap_ticket_is_bound_expires_and_can_be_consumed_only_once() {
        let (_root, runtime) = test_runtime();
        let request = native_connect_request();
        let service = ServiceIdentity {
            process_id: 41,
            data_dir: PathBuf::from(r"C:\ProgramData\OrdaDB\data"),
            pipe_name: r"\\.\pipe\ordadb-bootstrap-test".into(),
        };

        let issued = runtime
            .issue_bootstrap_ticket(&request, &service)
            .expect("issue ticket");
        let payload = BootstrapAdminRequest {
            ticket: issued.ticket.clone(),
            connection: request.clone(),
            suggested_username: "ordadb_admin".into(),
        };
        assert_eq!(
            runtime
                .consume_bootstrap_ticket(&payload)
                .expect("consume ticket")
                .service,
            service
        );
        assert_eq!(
            runtime
                .consume_bootstrap_ticket(&payload)
                .expect_err("ticket replay")
                .sql_state,
            "55000"
        );

        let issued = runtime
            .issue_bootstrap_ticket(&request, &service)
            .expect("issue fingerprint ticket");
        let mut mismatched_connection = request.clone();
        mismatched_connection.endpoint = "127.0.0.1:54330".into();
        let mismatched = BootstrapAdminRequest {
            ticket: issued.ticket.clone(),
            connection: mismatched_connection,
            suggested_username: "ordadb_admin".into(),
        };
        assert_eq!(
            runtime
                .consume_bootstrap_ticket(&mismatched)
                .expect_err("fingerprint mismatch")
                .sql_state,
            "28000"
        );
        assert_eq!(
            runtime
                .consume_bootstrap_ticket(&BootstrapAdminRequest {
                    ticket: issued.ticket,
                    connection: request.clone(),
                    suggested_username: "ordadb_admin".into(),
                })
                .expect_err("mismatched ticket is consumed")
                .sql_state,
            "55000"
        );

        let issued = runtime
            .issue_bootstrap_ticket(&request, &service)
            .expect("issue expiring ticket");
        mutex_lock(&runtime.bootstrap_tickets)
            .expect("ticket lock")
            .get_mut(&issued.ticket)
            .expect("issued ticket")
            .expires_at = Instant::now() - Duration::from_millis(1);
        assert_eq!(
            runtime
                .consume_bootstrap_ticket(&BootstrapAdminRequest {
                    ticket: issued.ticket,
                    connection: request,
                    suggested_username: "ordadb_admin".into(),
                })
                .expect_err("expired ticket")
                .sql_state,
            "55000"
        );
    }

    #[test]
    fn bootstrap_ticket_serialization_exposes_only_the_bounded_capability() {
        let ticket = LocalBootstrapTicket {
            ticket: "ticket-1".into(),
            expires_in_ms: 120_000,
        };
        let mut probe = ConnectionProbe::new();
        probe.bootstrap_ticket = Some(ticket);
        let value = serde_json::to_value(probe).expect("serialize ticket");
        assert_eq!(value["bootstrapTicket"]["ticket"], "ticket-1");
        assert_eq!(value["bootstrapTicket"]["expiresInMs"], 120_000);
        assert_eq!(
            value["bootstrapTicket"]
                .as_object()
                .expect("ticket object")
                .len(),
            2
        );
    }

    #[test]
    fn windows_service_command_line_parser_preserves_quoted_data_directories() {
        assert_eq!(
            split_windows_command_line(
                r#""C:\Program Files\OrdaDB\ordadb-server.exe" service --data-dir "D:\Orda Data\cluster""#
            )
            .expect("quoted command line"),
            vec![
                r"C:\Program Files\OrdaDB\ordadb-server.exe",
                "service",
                "--data-dir",
                r"D:\Orda Data\cluster",
            ]
        );
        assert_eq!(
            split_windows_command_line(
                r#""C:\OrdaDB\ordadb-server.exe" service --data-dir "D:\quoted\\\"name""#
            )
            .expect("escaped quote"),
            vec![
                r"C:\OrdaDB\ordadb-server.exe",
                "service",
                "--data-dir",
                "D:\\quoted\\\"name",
            ]
        );
        assert_eq!(
            split_windows_command_line(r#""C:\OrdaDB\ordadb-server.exe --data-dir D:\data"#)
                .expect_err("unmatched quote")
                .sql_state,
            "22023"
        );
    }

    #[test]
    fn native_endpoint_and_identifiers_are_bounded() {
        assert_eq!(
            validate_admin_endpoint("http://127.0.0.1:9080").expect("loopback"),
            "http://127.0.0.1:9080"
        );
        assert_eq!(
            validate_admin_endpoint("http://db.example.test:9080")
                .expect_err("remote plaintext")
                .sql_state,
            "22023"
        );
        assert!(validate_admin_endpoint("https://db.example.test:9080").is_ok());
        assert!(validate_id("connection-1", "connection ID").is_ok());
        assert!(validate_id("../escape", "connection ID").is_err());
    }

    #[test]
    fn catalog_projection_is_flattened_without_leaking_identifier_markers() {
        let projection = serde_json::json!({
            "database": {
                "name": "u:ordadb",
                "schemas": [{
                    "name": "u:public",
                    "tables": [{
                        "name": "u:documents",
                        "indexes": [{"name": "u:documents_pk"}],
                        "constraints": [],
                        "triggers": []
                    }],
                    "sequences": [],
                    "views": [{
                        "name": "u:recent_documents",
                        "kind": "plain",
                        "indexes": []
                    }],
                    "routines": []
                }]
            }
        });
        let objects = flatten_catalog(&projection).expect("catalog");
        assert!(objects.iter().any(|object| object.name == "documents"));
        assert!(objects.iter().any(|object| object.name == "documents_pk"));
        assert!(objects.iter().all(|object| !object.name.starts_with("u:")));
    }

    #[test]
    fn connector_values_are_projected_as_display_cells() {
        assert_eq!(value_text(Value::Null), None);
        assert_eq!(
            value_text(Value::Text("document".into())),
            Some("document".into())
        );
        assert_eq!(
            value_text(Value::Vector(vec![1.0, 2.0])),
            Some("[1, 2]".into())
        );
        let interval = PgInterval::new(2, 3, 4);
        let interval_text = interval.to_string();
        assert_eq!(value_text(Value::Interval(interval)), Some(interval_text));
        let array = PgArray::one_dimensional(ScalarType::Int32, vec![Value::Int32(7), Value::Null])
            .expect("array");
        assert_eq!(
            value_text(Value::Array(array)),
            Some(r#"["7",null]"#.into())
        );
    }

    #[test]
    fn ten_connector_identities_validate_without_sql_aliases_for_non_sql_sources() {
        let cases = [
            (
                NATIVE_CONNECTOR_ID,
                "sql",
                "postgresql-sql",
                Some("postgresql"),
            ),
            ("postgresql", "sql", "postgresql-sql", Some("postgresql")),
            ("mysql", "sql", "mysql-sql", Some("mysql")),
            ("sqlite", "sql", "sqlite-sql", Some("sqlite")),
            ("sql-server", "sql", "sql-server-sql", Some("sqlServer")),
            ("mongodb", "document", "mongodb-json", None),
            ("redis", "keyValue", "redis-resp3", None),
            ("mariadb", "sql", "mariadb-sql", Some("mariadb")),
            ("clickhouse", "sql", "clickhouse-sql", Some("clickhouse")),
            ("oracle", "sql", "oracle-sql", Some("oracle")),
        ];
        for (connector_id, connector_kind, command_language, dialect) in cases {
            let native = connector_id == NATIVE_CONNECTOR_ID;
            let request = ConnectRequest {
                connector_id: connector_id.into(),
                connector_kind: connector_kind.into(),
                command_language: command_language.into(),
                dialect: dialect.map(str::to_owned),
                endpoint: "127.0.0.1:15432".into(),
                admin_endpoint: native.then(|| "http://127.0.0.1:9080".into()),
                database: Some(if connector_id == "redis" { "0" } else { "test" }.into()),
                tls_mode: if native {
                    ConnectorTlsModeV2::Disable
                } else {
                    ConnectorTlsModeV2::Require
                },
                credential_id: format!("credential-{connector_id}"),
                credential_access: CredentialAccess::Unspecified,
            };
            validate_connect_request(&request).expect(connector_id);

            let mut mismatched = request;
            mismatched.command_language = "postgresql-sql".into();
            if command_language != "postgresql-sql" {
                assert_eq!(
                    validate_connect_request(&mismatched)
                        .expect_err("identity mismatch")
                        .sql_state,
                    "22023"
                );
            }
        }
    }

    #[test]
    fn desktop_command_shapes_are_bound_to_the_negotiated_data_model() {
        let mongodb = DesktopCommand::Document {
            language_id: "mongodb-json".into(),
            document: serde_json::json!({"operation": "find"}),
        };
        validate_command_for_connection(&mongodb, "document", "mongodb-json")
            .expect("MongoDB document command");
        assert_eq!(
            validate_command_for_connection(&mongodb, "sql", "postgresql-sql")
                .expect_err("document cannot become SQL")
                .sql_state,
            "22023"
        );

        let redis = DesktopCommand::Arguments {
            language_id: "redis-resp3".into(),
            arguments: vec!["GET".into(), "key".into()],
        };
        validate_command_for_connection(&redis, "keyValue", "redis-resp3")
            .expect("Redis argument command");
        assert!(matches!(
            desktop_command_v3(redis),
            ConnectorCommandV3::Arguments { .. }
        ));
    }

    #[test]
    fn v3_catalog_and_key_values_preserve_native_identity_and_types() {
        let object = catalog_node_v3(ConnectorCatalogNodeV3 {
            id: "collection:orders".into(),
            parent_id: Some("database:shop".into()),
            kind: ConnectorCatalogNodeKindV3::Collection,
            name: "orders".into(),
            namespace: Some("shop".into()),
            has_children: true,
            columns: Vec::new(),
            attributes: BTreeMap::from([("capped".into(), "false".into())]),
        })
        .expect("Catalog projection");
        assert_eq!(object.id.as_deref(), Some("collection:orders"));
        assert_eq!(object.parent.as_deref(), Some("database:shop"));
        assert_eq!(object.namespace.as_deref(), Some("shop"));
        assert_eq!(object.details["attributes"]["capped"], "false");

        assert_eq!(
            connector_value_json(ConnectorValueV2::Decimal("123456789.0123".into()))
                .expect("decimal"),
            serde_json::json!({"kind": "decimal", "value": "123456789.0123"})
        );
        assert_eq!(
            connector_value_json(ConnectorValueV2::Array(vec![
                ConnectorValueV2::Text("value".into()),
                ConnectorValueV2::Null,
            ]))
            .expect("array"),
            serde_json::json!(["value", null])
        );
        assert_eq!(
            connector_value_json(ConnectorValueV2::FloatingPoint(f64::NAN))
                .expect_err("non-finite value")
                .sql_state,
            "08P01"
        );
    }

    #[test]
    fn bootstrap_fingerprint_covers_connector_model_language_and_tls() {
        let request = native_connect_request();
        let baseline = connection_fingerprint(&request);
        let mut changed = request.clone();
        changed.command_language = "other-sql".into();
        assert_ne!(baseline, connection_fingerprint(&changed));
        changed = request.clone();
        changed.tls_mode = ConnectorTlsModeV2::Require;
        assert_ne!(baseline, connection_fingerprint(&changed));
    }

    #[test]
    fn query_updates_serialize_event_fields_in_camel_case() {
        let progress = serde_json::to_value(QueryUpdate {
            request_id: "request-1".into(),
            event: DbmsQueryEvent::Progress { rows_processed: 7 },
        })
        .expect("serialize progress");
        assert_eq!(
            progress,
            serde_json::json!({
                "requestId": "request-1",
                "event": {
                    "kind": "progress",
                    "rowsProcessed": 7
                }
            })
        );

        let notice = serde_json::to_value(QueryUpdate {
            request_id: "request-notice".into(),
            event: DbmsQueryEvent::Notice {
                severity: "WARNING".into(),
                sql_state: "01000".into(),
                message: "careful".into(),
            },
        })
        .expect("serialize notice");
        assert_eq!(
            notice,
            serde_json::json!({
                "requestId": "request-notice",
                "event": {
                    "kind": "notice",
                    "severity": "WARNING",
                    "sqlState": "01000",
                    "message": "careful"
                }
            })
        );

        let complete = serde_json::to_value(QueryUpdate {
            request_id: "request-2".into(),
            event: DbmsQueryEvent::Complete {
                command_tag: "SELECT 1".into(),
                duration_ms: 12,
            },
        })
        .expect("serialize completion");
        assert_eq!(
            complete,
            serde_json::json!({
                "requestId": "request-2",
                "event": {
                    "kind": "complete",
                    "commandTag": "SELECT 1",
                    "durationMs": 12
                }
            })
        );

        let error = serde_json::to_value(QueryUpdate {
            request_id: "request-3".into(),
            event: DbmsQueryEvent::Error {
                error: DbmsError {
                    sql_state: "57014".into(),
                    message: "query cancelled".into(),
                    detail: None,
                    hint: Some("retry the query".into()),
                    position: Some(9),
                    query_id: "query-3".into(),
                },
            },
        })
        .expect("serialize error");
        assert_eq!(
            error,
            serde_json::json!({
                "requestId": "request-3",
                "event": {
                    "kind": "error",
                    "error": {
                        "sqlState": "57014",
                        "message": "query cancelled",
                        "detail": null,
                        "hint": "retry the query",
                        "position": 9,
                        "queryId": "query-3"
                    }
                }
            })
        );
    }

    #[test]
    fn administration_requests_are_relative_and_shape_checked() {
        let backup = StartAdministrationOperationRequest {
            connection_id: "connection-1".into(),
            kind: AdministrationOperationKind::Backup,
            path: "nightly/ordadb.ordbak".into(),
            schema: None,
            table: None,
            format: None,
        };
        assert!(validate_administration_operation_request(&backup).is_ok());

        let mut absolute = backup.clone();
        absolute.path = r"C:\ProgramData\ordadb.ordbak".into();
        assert_eq!(
            validate_administration_operation_request(&absolute)
                .expect_err("absolute path")
                .sql_state,
            "22023"
        );
        let mut traversal = backup.clone();
        traversal.path = "../escape.ordbak".into();
        assert!(validate_administration_operation_request(&traversal).is_err());

        let mut missing_table = backup;
        missing_table.kind = AdministrationOperationKind::Import;
        missing_table.format = Some(AdministrationTransferFormat::Csv);
        assert!(validate_administration_operation_request(&missing_table).is_err());
    }

    #[test]
    fn administration_operation_errors_serialize_recursively_in_camel_case() {
        let operation = AdministrationOperation::from(AdministrationOperationResponse {
            operation_id: Uuid::nil(),
            kind: AdministrationOperationKind::Restore,
            state: "failed".into(),
            path: "broken.ordbak".into(),
            schema: None,
            table: None,
            started_at: None,
            finished_at: None,
            rows: None,
            bytes: None,
            error: Some(DbError::new("XX001", "archive checksum mismatch")),
        });
        let value = serde_json::to_value(operation).expect("serialize operation");
        assert_eq!(value["operationId"], Uuid::nil().to_string());
        assert_eq!(value["kind"], "restore");
        assert_eq!(value["error"]["sqlState"], "XX001");
        assert!(value["error"].get("sql_state").is_none());
        assert!(value["error"].get("queryId").is_some());
    }

    fn native_connect_request() -> ConnectRequest {
        ConnectRequest {
            connector_id: NATIVE_CONNECTOR_ID.into(),
            connector_kind: "sql".into(),
            command_language: "postgresql-sql".into(),
            dialect: Some("postgresql".into()),
            endpoint: "127.0.0.1:54329".into(),
            admin_endpoint: Some("http://127.0.0.1:9080".into()),
            database: Some("ordadb".into()),
            tls_mode: ConnectorTlsModeV2::Disable,
            credential_id: "ordadb-local".into(),
            credential_access: CredentialAccess::Unspecified,
        }
    }

    fn test_runtime() -> (tempfile::TempDir, Arc<DbmsRuntime>) {
        let root = tempfile::tempdir().expect("temporary plugin root");
        let manager =
            PluginManager::open_https(ordadb_connectors::PluginManagerOptions::new(root.path()))
                .expect("plugin manager");
        let credentials = DatabaseCredentialStore::open_path(
            root.path()
                .join("credentials")
                .join("credentials-v1.sqlite3"),
        )
        .expect("credential store");
        let runtime =
            DbmsRuntime::new_with_credentials(manager, credentials).expect("DBMS runtime");
        (root, runtime)
    }
}
