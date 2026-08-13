
const fn catalog_kind(kind: ConnectorCatalogObjectKindV2) -> &'static str {
    match kind {
        ConnectorCatalogObjectKindV2::Database => "database",
        ConnectorCatalogObjectKindV2::Schema => "schema",
        ConnectorCatalogObjectKindV2::Table => "table",
        ConnectorCatalogObjectKindV2::View => "view",
        ConnectorCatalogObjectKindV2::MaterializedView => "materializedView",
        ConnectorCatalogObjectKindV2::Column => "column",
        ConnectorCatalogObjectKindV2::Index => "index",
        ConnectorCatalogObjectKindV2::Constraint => "constraint",
        ConnectorCatalogObjectKindV2::Sequence => "sequence",
        ConnectorCatalogObjectKindV2::Function => "function",
        ConnectorCatalogObjectKindV2::Procedure => "procedure",
    }
}

const fn catalog_kind_v3(kind: ConnectorCatalogNodeKindV3) -> &'static str {
    match kind {
        ConnectorCatalogNodeKindV3::Server => "server",
        ConnectorCatalogNodeKindV3::Cluster => "cluster",
        ConnectorCatalogNodeKindV3::Database => "database",
        ConnectorCatalogNodeKindV3::Schema => "schema",
        ConnectorCatalogNodeKindV3::Table => "table",
        ConnectorCatalogNodeKindV3::View => "view",
        ConnectorCatalogNodeKindV3::MaterializedView => "materializedView",
        ConnectorCatalogNodeKindV3::Column => "column",
        ConnectorCatalogNodeKindV3::Index => "index",
        ConnectorCatalogNodeKindV3::Constraint => "constraint",
        ConnectorCatalogNodeKindV3::Sequence => "sequence",
        ConnectorCatalogNodeKindV3::Function => "function",
        ConnectorCatalogNodeKindV3::Procedure => "procedure",
        ConnectorCatalogNodeKindV3::Collection => "collection",
        ConnectorCatalogNodeKindV3::Keyspace => "keyspace",
        ConnectorCatalogNodeKindV3::Key => "key",
        ConnectorCatalogNodeKindV3::Stream => "stream",
        ConnectorCatalogNodeKindV3::Other => "other",
    }
}

fn request_id(request: &ConnectorRequestV1) -> Option<&str> {
    match request {
        ConnectorRequestV1::Catalog { request_id, .. }
        | ConnectorRequestV1::Execute { request_id, .. }
        | ConnectorRequestV1::Cancel { request_id }
        | ConnectorRequestV1::Begin { request_id, .. }
        | ConnectorRequestV1::Commit { request_id, .. }
        | ConnectorRequestV1::Rollback { request_id, .. }
        | ConnectorRequestV1::Monitor { request_id, .. } => Some(request_id),
        ConnectorRequestV1::Hello { .. }
        | ConnectorRequestV1::Connect { .. }
        | ConnectorRequestV1::Shutdown => None,
    }
}

fn handshake_timeout() -> DbError {
    network_error(
        "connector handshake timed out",
        "no protocol response before the deadline",
    )
}

fn handshake_response_error() -> DbError {
    DbError::new("08P01", "connector did not begin with a Ready response")
}

fn connector_pipe_name() -> String {
    format!(r"\\.\pipe\ordadb-connector-{}", Uuid::new_v4())
}

fn configure_hidden_process(command: &mut Command) {
    use std::os::windows::process::CommandExt as _;
    use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

    command.as_std_mut().creation_flags(CREATE_NO_WINDOW);
}

fn helper_exit_error(status: std::io::Result<ExitStatus>) -> DbError {
    match status {
        Ok(status) => network_error(
            "connector helper process exited before completing the protocol",
            format!("exit status {status}"),
        ),
        Err(error) => io_error("failed to wait for connector helper process", error),
    }
}

#[cfg(test)]
mod tests {
    use ordadb_connector_sdk::{
        ConnectorCapabilitiesV2, ConnectorCapabilitiesV3, ConnectorCatalogNodeV3,
        ConnectorCatalogPageV3, ConnectorColumnV2, ConnectorCommandInputModeV3,
        ConnectorCommandLanguageV3, ConnectorErrorV2, ConnectorKindV3, ConnectorLogicalTypeV2,
        ConnectorResponseV2, ConnectorResponseV3, ConnectorTlsModeV2, ConnectorTypeV2,
        ProtocolReadyV2, ProtocolReadyV3,
    };
    use ordadb_types::{PgArray, PgInterval, TypeId};
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

    use super::*;
    use crate::ProtocolReady;

    fn capabilities_v3(kind: ConnectorKindV3) -> ConnectorCapabilitiesV3 {
        let (id, input_modes) = match kind {
            ConnectorKindV3::Sql => ("postgresql-sql", vec![ConnectorCommandInputModeV3::Text]),
            ConnectorKindV3::Document => ("mql", vec![ConnectorCommandInputModeV3::Document]),
            ConnectorKindV3::KeyValue => ("resp3", vec![ConnectorCommandInputModeV3::Arguments]),
        };
        ConnectorCapabilitiesV3 {
            kind,
            command_languages: vec![ConnectorCommandLanguageV3 {
                id: id.into(),
                display_name: id.into(),
                input_modes,
            }],
            catalog: true,
            cancellation: true,
            transactions: true,
            savepoints: false,
            batch_query: true,
            maximum_batch_rows: 1024,
            maximum_catalog_page_size: 256,
            tls_modes: vec![ConnectorTlsModeV2::Disable, ConnectorTlsModeV2::Require],
        }
    }

    #[tokio::test]
    async fn restricted_pipe_negotiates_protocol_v1_with_current_process_client() {
        let pipe_name = connector_pipe_name();
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true);
        let mut server = options.create(&pipe_name).expect("server pipe");
        ordadb_windows::restrict_named_pipe_acl(&server).expect("pipe ACL");
        let client = tokio::spawn({
            let pipe_name = pipe_name.clone();
            async move {
                let mut client = ClientOptions::new().open(&pipe_name).expect("client pipe");
                let hello: ConnectorRequestV1 =
                    read_connector_frame(&mut client).await.expect("hello");
                let ConnectorRequestV1::Hello {
                    api_version,
                    plugin_id,
                    plugin_version,
                } = hello
                else {
                    panic!("expected hello");
                };
                assert_eq!(api_version, MIN_CONNECTOR_API_VERSION);
                write_connector_frame(
                    &mut client,
                    &ConnectorResponseV1::Ready(ProtocolReady {
                        api_version,
                        plugin_id,
                        plugin_version,
                    }),
                )
                .await
                .expect("ready");
            }
        });
        server.connect().await.expect("connect");
        assert_eq!(
            negotiate(
                &mut server,
                "ordadb-postgresql",
                "1.0.0",
                MIN_CONNECTOR_API_VERSION,
            )
            .await
            .expect("negotiate"),
            NegotiatedProtocol::V1
        );
        client.await.expect("client task");
    }

    #[tokio::test]
    async fn restricted_pipe_negotiates_protocol_v2_with_current_process_client() {
        let pipe_name = connector_pipe_name();
        let mut server = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true)
            .create(&pipe_name)
            .expect("server pipe");
        ordadb_windows::restrict_named_pipe_acl(&server).expect("pipe ACL");
        let client = tokio::spawn({
            let pipe_name = pipe_name.clone();
            async move {
                let mut client = ClientOptions::new().open(&pipe_name).expect("client pipe");
                let hello: ConnectorRequestV2 =
                    read_connector_frame_v2(&mut client).await.expect("hello");
                let ConnectorRequestV2::Hello { hello } = hello else {
                    panic!("expected hello");
                };
                write_connector_frame_v2(
                    &mut client,
                    &ConnectorResponseV2::Ready {
                        ready: ProtocolReadyV2 {
                            api_version: CONNECTOR_PROTOCOL_V2,
                            plugin_id: hello.plugin_id,
                            plugin_version: hello.plugin_version,
                            capabilities: ConnectorCapabilitiesV2 {
                                catalog: true,
                                cancellation: true,
                                transactions: true,
                                savepoints: false,
                                batch_query: true,
                                maximum_batch_rows: 1024,
                                tls_modes: vec![
                                    ConnectorTlsModeV2::Disable,
                                    ConnectorTlsModeV2::Require,
                                ],
                            },
                        },
                    },
                )
                .await
                .expect("ready");
            }
        });
        server.connect().await.expect("connect");
        assert_eq!(
            negotiate(&mut server, "postgresql", "1.0.0", CONNECTOR_PROTOCOL_V2)
                .await
                .expect("negotiate"),
            NegotiatedProtocol::V2
        );
        client.await.expect("client task");
    }

    #[tokio::test]
    async fn restricted_pipe_negotiates_protocol_v3_with_current_process_client() {
        let pipe_name = connector_pipe_name();
        let mut server = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .write_dac(true)
            .create(&pipe_name)
            .expect("server pipe");
        ordadb_windows::restrict_named_pipe_acl(&server).expect("pipe ACL");
        let expected = capabilities_v3(ConnectorKindV3::Sql);
        let client = tokio::spawn({
            let pipe_name = pipe_name.clone();
            let expected = expected.clone();
            async move {
                let mut client = ClientOptions::new().open(&pipe_name).expect("client pipe");
                let hello: ConnectorRequestV3 =
                    read_connector_frame_v3(&mut client).await.expect("hello");
                let ConnectorRequestV3::Hello { hello } = hello else {
                    panic!("expected hello");
                };
                write_connector_frame_v3(
                    &mut client,
                    &ConnectorResponseV3::Ready {
                        ready: ProtocolReadyV3 {
                            api_version: CONNECTOR_PROTOCOL_V3,
                            plugin_id: hello.plugin_id,
                            plugin_version: hello.plugin_version,
                            capabilities: expected,
                        },
                    },
                )
                .await
                .expect("ready");
            }
        });
        server.connect().await.expect("connect");
        assert_eq!(
            negotiate(&mut server, "postgresql-v3", "1.0.0", CONNECTOR_PROTOCOL_V3)
                .await
                .expect("negotiate"),
            NegotiatedProtocol::V3(expected)
        );
        client.await.expect("client task");
    }

    #[test]
    fn v3_legacy_adapter_accepts_sql_and_rejects_non_sql() {
        let sql = capabilities_v3(ConnectorKindV3::Sql);
        let translated = translate_request_v3(
            &ConnectorRequestV1::Execute {
                request_id: "request-1".into(),
                connection_id: "connection-1".into(),
                sql: "SELECT 1".into(),
                params: Vec::new(),
            },
            "postgresql-v3",
            &sql,
        )
        .expect("translate")
        .expect("supported request");
        assert!(matches!(
            translated,
            ConnectorRequestV3::Execute {
                command: ConnectorCommandV3::Text { text, .. },
                ..
            } if text == "SELECT 1"
        ));

        let document = capabilities_v3(ConnectorKindV3::Document);
        assert_eq!(
            ensure_legacy_sql_v3(&document)
                .expect_err("document must use native v3")
                .sql_state,
            "0A000"
        );
    }

    #[test]
    fn v3_host_state_binds_response_ids_and_request_limits() {
        let capabilities = capabilities_v3(ConnectorKindV3::Sql);
        let mut pending = BTreeMap::new();
        let mut validators = BTreeMap::new();
        let catalog_request = ConnectorRequestV3::Catalog {
            request_id: "catalog-1".into(),
            connection_id: "connection-1".into(),
            parent_id: None,
            page_size: 1,
            cursor: None,
        };
        let (request_id, request) = prepare_v3_request(&catalog_request, &pending)
            .expect("prepare Catalog")
            .expect("tracked Catalog");
        pending.insert(request_id, request);

        let node = ConnectorCatalogNodeV3 {
            id: "public/items".into(),
            parent_id: Some("public".into()),
            kind: ConnectorCatalogNodeKindV3::Table,
            name: "items".into(),
            namespace: Some("public".into()),
            has_children: false,
            columns: Vec::new(),
            attributes: BTreeMap::new(),
        };
        let oversized = ConnectorResponseV3::CatalogPage {
            request_id: "catalog-1".into(),
            page: ConnectorCatalogPageV3 {
                nodes: vec![
                    node.clone(),
                    ConnectorCatalogNodeV3 {
                        id: "public/other".into(),
                        name: "other".into(),
                        ..node.clone()
                    },
                ],
                next_cursor: None,
            },
        };
        assert_eq!(
            validate_v3_response(&oversized, &capabilities, &mut pending, &mut validators,)
                .expect_err("requested page size is authoritative")
                .sql_state,
            "54000"
        );
        assert!(pending.contains_key("catalog-1"));

        let valid = ConnectorResponseV3::CatalogPage {
            request_id: "catalog-1".into(),
            page: ConnectorCatalogPageV3 {
                nodes: vec![node],
                next_cursor: None,
            },
        };
        validate_v3_response(&valid, &capabilities, &mut pending, &mut validators)
            .expect("valid Catalog response");
        assert!(pending.is_empty());
        assert_eq!(
            validate_v3_response(&valid, &capabilities, &mut pending, &mut validators)
                .expect_err("repeated response ID")
                .sql_state,
            "08P01"
        );
        assert_eq!(
            prepare_v3_request(
                &ConnectorRequestV3::Cancel {
                    request_id: "unknown".into(),
                },
                &pending,
            )
            .expect_err("unknown cancellation")
            .sql_state,
            "42704"
        );

        for index in 0..MAX_HOST_ACTIVE_V3_REQUESTS {
            pending.insert(format!("request-{index}"), PendingV3Request::Transaction);
        }
        let overflow = ConnectorRequestV3::Begin {
            request_id: "overflow".into(),
            connection_id: "connection-1".into(),
            isolation: None,
        };
        assert_eq!(
            prepare_v3_request(&overflow, &pending)
                .expect_err("active request limit")
                .sql_state,
            "54000"
        );
    }

    #[test]
    fn v3_sql_results_preserve_the_legacy_query_event_contract() {
        let column_type = ConnectorTypeV2 {
            vendor_name: "text".into(),
            logical_type: ConnectorLogicalTypeV2::Text,
            element_type: None,
            precision: None,
            scale: None,
            length: None,
        };
        let mut schemas = BTreeMap::new();
        let schema = translate_response_v3(
            ConnectorResponseV3::ResultEvent {
                request_id: "request-1".into(),
                event: ConnectorResultEventV3::Schema {
                    columns: vec![ConnectorColumnV2 {
                        name: "value".into(),
                        data_type: column_type,
                        nullable: false,
                    }],
                },
            },
            &mut schemas,
        )
        .expect("schema response");
        assert!(matches!(
            schema,
            ConnectorResponseV1::QueryEvent {
                event: QueryEvent::Schema(_),
                ..
            }
        ));

        let batch = translate_response_v3(
            ConnectorResponseV3::ResultEvent {
                request_id: "request-1".into(),
                event: ConnectorResultEventV3::Batch {
                    batch: ConnectorResultBatchV3::Rows {
                        rows: vec![vec![ConnectorValueV2::Text("one".into())]],
                    },
                },
            },
            &mut schemas,
        )
        .expect("batch response");
        assert!(matches!(
            batch,
            ConnectorResponseV1::QueryEvent {
                event: QueryEvent::Batch(Batch { rows, .. }),
                ..
            } if rows[0].values == vec![Value::Text("one".into())]
        ));

        let complete = translate_response_v3(
            ConnectorResponseV3::ResultEvent {
                request_id: "request-1".into(),
                event: ConnectorResultEventV3::Complete {
                    command_tag: "SELECT".into(),
                    affected_items: Some(1),
                },
            },
            &mut schemas,
        )
        .expect("complete response");
        assert!(matches!(
            complete,
            ConnectorResponseV1::QueryEvent {
                event: QueryEvent::Complete(CommandComplete {
                    rows_affected: 1,
                    ..
                }),
                ..
            }
        ));
        assert!(schemas.is_empty());
    }

    #[tokio::test]
    async fn unsupported_protocol_version_fails_without_pipe_or_credentials() {
        let pipe_name = connector_pipe_name();
        let mut server = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .create(&pipe_name)
            .expect("server pipe");
        let error = negotiate(
            &mut server,
            "future-connector",
            "1.0.0",
            CONNECTOR_PROTOCOL_V3 + 1,
        )
        .await
        .expect_err("future protocol must fail");
        assert_eq!(error.sql_state, "0A000");
    }

    #[test]
    fn v2_translation_preserves_batches_errors_and_endpoint_kinds() {
        let endpoint = structured_endpoint("postgresql", "db.example:5433", Some("app".into()))
            .expect("network endpoint");
        assert!(matches!(
            endpoint,
            ConnectorEndpointV2::Network { port: 5433, .. }
        ));
        let endpoint =
            structured_endpoint("sqlite", "C:\\data\\app.db", None).expect("file endpoint");
        assert!(matches!(endpoint, ConnectorEndpointV2::File { .. }));
        for (plugin_id, expected_port) in [
            ("mongodb", 27017),
            ("redis", 6379),
            ("mariadb", 3306),
            ("clickhouse", 8123),
            ("oracle", 1521),
        ] {
            assert!(matches!(
                structured_endpoint(plugin_id, "db.example", None)
                    .expect("v3 network endpoint"),
                ConnectorEndpointV2::Network { port, .. } if port == expected_port
            ));
        }

        let error = ConnectorErrorV2 {
            sql_state: "40001".into(),
            vendor_code: Some("1213".into()),
            message: "deadlock".into(),
            detail: None,
            hint: None,
            position: None,
            retryable: true,
        };
        let response = translate_response_v2(
            ConnectorResponseV2::Error {
                request_id: Some("request-1".into()),
                error,
            },
            &mut BTreeMap::new(),
        )
        .expect("error response");
        assert!(matches!(
            response,
            ConnectorResponseV1::Error { error, .. } if error.sql_state == "40001"
        ));
    }

    #[test]
    fn v2_parameter_translation_preserves_interval_array_and_enum_types() {
        let interval = PgInterval::new(2, 3, 4);
        let interval_text = interval.to_string();
        assert_eq!(
            value_v2(&Value::Interval(interval)),
            ConnectorValueV2::Interval(interval_text)
        );

        let array = PgArray::one_dimensional(ScalarType::Int32, vec![Value::Int32(7), Value::Null])
            .expect("array");
        assert_eq!(
            value_v2(&Value::Array(array)),
            ConnectorValueV2::Array(vec![
                ConnectorValueV2::SignedInteger(7),
                ConnectorValueV2::Null,
            ])
        );

        let interval_type = connector_type(&ScalarType::Interval);
        assert_eq!(interval_type.logical_type, ConnectorLogicalTypeV2::Interval);
        let oid_type = connector_type(&ScalarType::Oid);
        assert_eq!(
            oid_type.logical_type,
            ConnectorLogicalTypeV2::UnsignedInteger
        );
        assert_eq!(oid_type.vendor_name, "oid");
        let name_type = connector_type(&ScalarType::Name);
        assert_eq!(name_type.logical_type, ConnectorLogicalTypeV2::Text);
        assert_eq!(name_type.length, Some(63));
        let internal_char_type = connector_type(&ScalarType::InternalChar);
        assert_eq!(
            internal_char_type.logical_type,
            ConnectorLogicalTypeV2::Text
        );
        assert_eq!(internal_char_type.length, Some(1));
        let enum_type = connector_type(&ScalarType::Enum {
            type_id: TypeId::new(42),
            labels: vec!["queued".into(), "done".into()],
        });
        assert_eq!(enum_type.logical_type, ConnectorLogicalTypeV2::Text);
        assert_eq!(enum_type.vendor_name, "enum");

        let array_type = connector_type(&ScalarType::Array {
            element: Box::new(ScalarType::Int32),
        });
        assert_eq!(array_type.logical_type, ConnectorLogicalTypeV2::Array);
        assert_eq!(
            array_type
                .element_type
                .expect("array element type")
                .logical_type,
            ConnectorLogicalTypeV2::SignedInteger
        );
    }

    #[tokio::test]
    async fn helper_exit_is_reported_without_sending_credentials() {
        let command = std::env::var_os("ComSpec").expect("ComSpec");
        let error = ConnectorHost::launch_entry(
            Path::new(&command),
            "ordadb-test",
            "1.0.0",
            MIN_CONNECTOR_API_VERSION,
        )
        .await
        .expect_err("cmd does not speak connector protocol");
        assert!(matches!(error.sql_state.as_str(), "08006" | "58030"));
    }
}
