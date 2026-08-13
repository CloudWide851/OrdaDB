
#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    use async_trait::async_trait;
    use serde_json::json;
    use tokio::net::windows::named_pipe::ServerOptions;

    use super::*;
    use crate::{
        ConnectorCatalogNodeKindV3, ConnectorCatalogNodeV3, ConnectorCommandInputModeV3,
        ConnectorCommandLanguageV3, ConnectorEndpointV2, ConnectorKeyValueV3,
        ConnectorResultBatchV3, ConnectorTlsModeV2, ConnectorValueV2, ProtocolHelloV3,
        read_connector_frame_v3, write_connector_frame_v3,
    };

    static PIPE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    #[derive(Clone)]
    struct FakeDriver {
        capabilities: ConnectorCapabilitiesV3,
    }

    struct FakeSession {
        capabilities: ConnectorCapabilitiesV3,
    }

    #[async_trait]
    impl ConnectorDriverV3 for FakeDriver {
        fn capabilities(&self) -> ConnectorCapabilitiesV3 {
            self.capabilities.clone()
        }

        async fn connect(
            &self,
            _endpoint: ConnectorEndpointV2,
            _tls_mode: ConnectorTlsModeV2,
            _credential: Option<crate::ConnectorCredentialV2>,
        ) -> Result<Box<dyn ConnectorSessionV3>> {
            Ok(Box::new(FakeSession {
                capabilities: self.capabilities.clone(),
            }))
        }
    }

    #[async_trait]
    impl ConnectorSessionV3 for FakeSession {
        fn capabilities(&self) -> &ConnectorCapabilitiesV3 {
            &self.capabilities
        }

        async fn catalog_page(
            &mut self,
            parent_id: Option<&str>,
            _page_size: u32,
            _cursor: Option<&str>,
        ) -> Result<crate::ConnectorCatalogPageV3> {
            Ok(crate::ConnectorCatalogPageV3 {
                nodes: vec![ConnectorCatalogNodeV3 {
                    id: "root/items".into(),
                    parent_id: parent_id.map(str::to_owned),
                    kind: match self.capabilities.kind {
                        ConnectorKindV3::Sql => ConnectorCatalogNodeKindV3::Table,
                        ConnectorKindV3::Document => ConnectorCatalogNodeKindV3::Collection,
                        ConnectorKindV3::KeyValue => ConnectorCatalogNodeKindV3::Keyspace,
                    },
                    name: "items".into(),
                    namespace: Some("root".into()),
                    has_children: false,
                    columns: Vec::new(),
                    attributes: BTreeMap::new(),
                }],
                next_cursor: None,
            })
        }

        async fn execute(
            &mut self,
            _request_id: &str,
            command: &ConnectorCommandV3,
            _batch_size: u32,
            cancellation: &CancellationToken,
            sink: &mut dyn ConnectorEventSinkV3,
        ) -> Result<()> {
            if matches!(
                command,
                ConnectorCommandV3::Document { document, .. }
                    if document.get("wait").and_then(serde_json::Value::as_bool) == Some(true)
            ) {
                tokio::select! {
                    () = cancellation.cancelled() => {
                        return Err(DbError::new("57014", "fake connector query was cancelled"));
                    }
                    () = tokio::time::sleep(Duration::from_secs(10)) => {
                        return Err(DbError::new("57014", "fake connector cancellation timed out"));
                    }
                }
            }
            match self.capabilities.kind {
                ConnectorKindV3::Sql => {
                    sink.send(ConnectorResultEventV3::Schema {
                        columns: Vec::new(),
                    })
                    .await?;
                    sink.send(ConnectorResultEventV3::Batch {
                        batch: ConnectorResultBatchV3::Rows {
                            rows: vec![Vec::new()],
                        },
                    })
                    .await?;
                }
                ConnectorKindV3::Document => {
                    sink.send(ConnectorResultEventV3::Batch {
                        batch: ConnectorResultBatchV3::Documents {
                            documents: vec![json!({ "ok": true })],
                        },
                    })
                    .await?;
                }
                ConnectorKindV3::KeyValue => {
                    sink.send(ConnectorResultEventV3::Batch {
                        batch: ConnectorResultBatchV3::KeyValues {
                            entries: vec![ConnectorKeyValueV3 {
                                key: ConnectorValueV2::Text("key".into()),
                                value: ConnectorValueV2::Text("value".into()),
                            }],
                        },
                    })
                    .await?;
                }
            }
            sink.send(ConnectorResultEventV3::Complete {
                command_tag: "OK".into(),
                affected_items: Some(1),
            })
            .await
        }

        async fn cancel(&mut self, _request_id: &str) -> Result<()> {
            Ok(())
        }

        async fn begin(&mut self, _isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
            Ok(())
        }

        async fn commit(&mut self) -> Result<()> {
            Ok(())
        }

        async fn rollback(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn capabilities(kind: ConnectorKindV3) -> ConnectorCapabilitiesV3 {
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
            maximum_batch_rows: 16,
            maximum_catalog_page_size: 16,
            tls_modes: vec![ConnectorTlsModeV2::Disable],
        }
    }

    fn command(kind: ConnectorKindV3, wait: bool) -> ConnectorCommandV3 {
        match kind {
            ConnectorKindV3::Sql => ConnectorCommandV3::Text {
                language_id: "postgresql-sql".into(),
                text: "SELECT 1".into(),
                params: Vec::new(),
            },
            ConnectorKindV3::Document => ConnectorCommandV3::Document {
                language_id: "mql".into(),
                document: json!({ "find": "items", "wait": wait }),
            },
            ConnectorKindV3::KeyValue => ConnectorCommandV3::Arguments {
                language_id: "resp3".into(),
                arguments: vec![
                    ConnectorValueV2::Text("GET".into()),
                    ConnectorValueV2::Text("key".into()),
                ],
            },
        }
    }

    async fn exercise_kind(kind: ConnectorKindV3) {
        let sequence = PIPE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let pipe_name = format!(
            r"\\.\pipe\ordadb-connector-sdk-v3-{}-{sequence}",
            std::process::id()
        );
        let mut server = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .max_instances(1)
            .create(&pipe_name)
            .expect("server pipe");
        let expected = capabilities(kind);
        let helper = tokio::spawn({
            let pipe_name = OsString::from(&pipe_name);
            let driver = FakeDriver {
                capabilities: expected.clone(),
            };
            async move {
                run_named_pipe_helper_v3(&pipe_name, "fake-v3", "1.0.0", driver)
                    .await
                    .expect("helper runtime");
            }
        });
        server.connect().await.expect("connect helper");
        write_connector_frame_v3(
            &mut server,
            &ConnectorRequestV3::Hello {
                hello: ProtocolHelloV3 {
                    minimum_api_version: CONNECTOR_PROTOCOL_V3,
                    maximum_api_version: CONNECTOR_PROTOCOL_V3,
                    plugin_id: "fake-v3".into(),
                    plugin_version: "1.0.0".into(),
                },
            },
        )
        .await
        .expect("hello");
        let ready: ConnectorResponseV3 = read_connector_frame_v3(&mut server).await.expect("ready");
        assert!(matches!(ready, ConnectorResponseV3::Ready { .. }));

        write_connector_frame_v3(
            &mut server,
            &ConnectorRequestV3::Connect {
                connection_id: "connection-1".into(),
                endpoint: ConnectorEndpointV2::Network {
                    host: "127.0.0.1".into(),
                    port: 1,
                    database: None,
                    instance: None,
                    options: BTreeMap::new(),
                },
                tls_mode: ConnectorTlsModeV2::Disable,
                credential: None,
            },
        )
        .await
        .expect("connect request");
        let connected: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
            .await
            .expect("connected");
        assert!(matches!(connected, ConnectorResponseV3::Connected { .. }));

        write_connector_frame_v3(
            &mut server,
            &ConnectorRequestV3::Catalog {
                request_id: "catalog-1".into(),
                connection_id: "connection-1".into(),
                parent_id: Some("root".into()),
                page_size: 16,
                cursor: None,
            },
        )
        .await
        .expect("catalog request");
        let catalog: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
            .await
            .expect("catalog page");
        assert!(matches!(catalog, ConnectorResponseV3::CatalogPage { .. }));

        write_connector_frame_v3(
            &mut server,
            &ConnectorRequestV3::Execute {
                request_id: "execute-1".into(),
                connection_id: "connection-1".into(),
                command: command(kind, false),
                batch_size: 16,
            },
        )
        .await
        .expect("execute request");
        loop {
            let response: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
                .await
                .expect("result event");
            if matches!(
                response,
                ConnectorResponseV3::ResultEvent {
                    event: ConnectorResultEventV3::Complete { .. },
                    ..
                }
            ) {
                break;
            }
        }

        write_connector_frame_v3(
            &mut server,
            &ConnectorRequestV3::Begin {
                request_id: "begin-1".into(),
                connection_id: "connection-1".into(),
                isolation: None,
            },
        )
        .await
        .expect("begin request");
        let transaction: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
            .await
            .expect("transaction response");
        assert!(matches!(
            transaction,
            ConnectorResponseV3::Transaction { .. }
        ));

        if kind == ConnectorKindV3::Document {
            write_connector_frame_v3(
                &mut server,
                &ConnectorRequestV3::Execute {
                    request_id: "cancel-1".into(),
                    connection_id: "connection-1".into(),
                    command: command(kind, true),
                    batch_size: 16,
                },
            )
            .await
            .expect("cancellable request");
            write_connector_frame_v3(
                &mut server,
                &ConnectorRequestV3::Cancel {
                    request_id: "cancel-1".into(),
                },
            )
            .await
            .expect("cancel request");
            let cancelled: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
                .await
                .expect("cancelled response");
            assert!(matches!(cancelled, ConnectorResponseV3::Cancelled { .. }));
        }

        write_connector_frame_v3(&mut server, &ConnectorRequestV3::Shutdown)
            .await
            .expect("shutdown request");
        let shutdown: ConnectorResponseV3 = read_connector_frame_v3(&mut server)
            .await
            .expect("shutdown response");
        assert!(matches!(shutdown, ConnectorResponseV3::Shutdown));
        helper.await.expect("helper task");
    }

    #[tokio::test]
    async fn fake_v3_runtime_exercises_sql_document_and_key_value_models() {
        tokio::time::timeout(Duration::from_secs(10), exercise_kind(ConnectorKindV3::Sql))
            .await
            .expect("SQL v3 runtime exceeded its test deadline");
        tokio::time::timeout(
            Duration::from_secs(10),
            exercise_kind(ConnectorKindV3::Document),
        )
        .await
        .expect("document v3 runtime exceeded its test deadline");
        tokio::time::timeout(
            Duration::from_secs(10),
            exercise_kind(ConnectorKindV3::KeyValue),
        )
        .await
        .expect("key/value v3 runtime exceeded its test deadline");
    }
}
