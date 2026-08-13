
#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{DateTime, oid::ObjectId};
    use ordadb_connector_sdk::{validate_capabilities_v3, validate_capability_subset_v3};

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<ConnectorResultEventV3>,
    }

    impl ConnectorEventSinkV3 for RecordingSink {
        fn send(
            &mut self,
            event: ConnectorResultEventV3,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async move {
                self.events.push(event);
                Ok(())
            })
        }
    }

    #[test]
    fn capabilities_allow_topology_transaction_downgrade() {
        let advertised = capabilities(true);
        let standalone = capabilities(false);
        validate_capabilities_v3(&advertised).expect("advertised capabilities");
        validate_capability_subset_v3(&advertised, &standalone)
            .expect("standalone capability subset");
        assert_eq!(advertised.kind, ConnectorKindV3::Document);
        assert_eq!(advertised.command_languages[0].id, LANGUAGE_ID);
    }

    #[test]
    fn command_shape_and_limits_fail_closed() {
        let command = parse_command(&json!({
            "operation": "find",
            "database": "app",
            "collection": "items",
            "filter": { "active": true }
        }))
        .expect("find command");
        assert!(matches!(command, MongoCommand::Find { .. }));
        assert_eq!(
            parse_command(&json!({
                "operation": "find",
                "collection": "items",
                "unknown": true
            }))
            .expect_err("unknown field")
            .sql_state,
            "22023"
        );
        assert_eq!(
            parse_command(&json!({
                "operation": "aggregate",
                "collection": "items",
                "pipeline": [],
                "limit": MAX_RESULT_ITEMS + 1
            }))
            .expect_err("oversized limit")
            .sql_state,
            "54000"
        );
    }

    #[test]
    fn extended_json_and_catalog_identifiers_round_trip() {
        let object_id = ObjectId::parse_str("507f1f77bcf86cd799439011").expect("object ID");
        let value = json_document_value(doc! {
            "_id": object_id,
            "createdAt": DateTime::from_millis(1_700_000_000_000),
        })
        .expect("extended JSON");
        assert_eq!(value["_id"]["$oid"], "507f1f77bcf86cd799439011");
        assert!(value["createdAt"].get("$date").is_some());

        let id = collection_node_id("数据库", "items:2026");
        assert_eq!(
            decode_collection_node(&id).expect("decode collection ID"),
            ("数据库".into(), "items:2026".into())
        );
    }

    #[test]
    fn topology_and_catalog_pagination_are_deterministic() {
        assert!(!transaction_supported(&doc! { "ok": 1 }));
        assert!(transaction_supported(&doc! {
            "logicalSessionTimeoutMinutes": 30,
            "setName": "rs0"
        }));
        assert!(transaction_supported(&doc! {
            "logicalSessionTimeoutMinutes": 30,
            "msg": "isdbgrid"
        }));

        let nodes = (0..5)
            .map(|index| ConnectorCatalogNodeV3 {
                id: format!("node-{index}"),
                parent_id: None,
                kind: ConnectorCatalogNodeKindV3::Collection,
                name: index.to_string(),
                namespace: None,
                has_children: false,
                columns: Vec::new(),
                attributes: BTreeMap::new(),
            })
            .collect();
        let first = paginate_nodes(nodes, 1, 2).expect("page");
        assert_eq!(first.nodes[0].id, "node-1");
        assert_eq!(first.next_cursor.as_deref(), Some("3"));
    }

    #[test]
    fn credentials_are_not_debuggable_and_tls_never_fails_open() {
        let credential = ConnectorCredentialV2::new(Some("user".into()), "secret-value");
        assert!(!format!("{credential:?}").contains("secret-value"));
        assert_eq!(
            mongodb_tls(ConnectorTlsModeV2::Prefer)
                .expect_err("prefer forbidden")
                .sql_state,
            "0A000"
        );
    }

    #[tokio::test]
    async fn real_mongodb_matrix_when_configured() {
        let Some(host) = std::env::var("ORDADB_TEST_MONGODB_HOST").ok() else {
            assert!(
                std::env::var("ORDADB_REQUIRE_REAL_CONNECTOR_TESTS").as_deref() != Ok("1"),
                "ORDADB_TEST_MONGODB_HOST is required for the real connector matrix"
            );
            return;
        };
        let database = std::env::var("ORDADB_TEST_MONGODB_DATABASE")
            .expect("ORDADB_TEST_MONGODB_DATABASE must accompany the host");
        let username = std::env::var("ORDADB_TEST_MONGODB_USER")
            .expect("ORDADB_TEST_MONGODB_USER must accompany the host");
        let password = std::env::var("ORDADB_TEST_MONGODB_PASSWORD")
            .expect("ORDADB_TEST_MONGODB_PASSWORD must accompany the host");
        let mut options = BTreeMap::new();
        if let Ok(auth_source) = std::env::var("ORDADB_TEST_MONGODB_AUTH_SOURCE") {
            options.insert("authSource".into(), auth_source);
        }
        let session = MongoDbDriver.connect(
            ConnectorEndpointV2::Network {
                host,
                port: env_port("ORDADB_TEST_MONGODB_PORT", 27017),
                database: Some(database.clone()),
                instance: None,
                options,
            },
            env_tls_mode("ORDADB_TEST_MONGODB_TLS"),
            Some(ConnectorCredentialV2::new(Some(username), password)),
        );
        let mut session = tokio::time::timeout(Duration::from_secs(45), session)
            .await
            .expect("MongoDB connection exceeded its deadline")
            .expect("connect MongoDB");

        let page = session
            .catalog_page(None, 64, None)
            .await
            .expect("MongoDB Catalog root");
        assert!(!page.nodes.is_empty());

        let command = ConnectorCommandV3::Document {
            language_id: LANGUAGE_ID.into(),
            document: json!({
                "operation": "command",
                "database": database,
                "command": { "ping": 1 }
            }),
        };
        let mut sink = RecordingSink::default();
        session
            .execute(
                "mongodb-ping",
                &command,
                64,
                &CancellationToken::new(),
                &mut sink,
            )
            .await
            .expect("MongoDB ping command");
        assert!(sink.events.iter().any(|event| matches!(
            event,
            ConnectorResultEventV3::Batch {
                batch: ConnectorResultBatchV3::Documents { documents }
            } if documents.len() == 1
        )));

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = session
            .execute(
                "mongodb-cancel",
                &command,
                64,
                &cancellation,
                &mut RecordingSink::default(),
            )
            .await
            .expect_err("cancelled MongoDB command");
        assert_eq!(error.sql_state, "57014");

        if session.capabilities().transactions {
            session
                .begin(None)
                .await
                .expect("begin MongoDB transaction");
            session
                .rollback()
                .await
                .expect("rollback MongoDB transaction");
        }
    }

    fn env_port(name: &str, default: u16) -> u16 {
        std::env::var(name)
            .ok()
            .map(|value| value.parse().expect("valid connector test port"))
            .unwrap_or(default)
    }

    fn env_tls_mode(name: &str) -> ConnectorTlsModeV2 {
        match std::env::var(name)
            .unwrap_or_else(|_| "verifyFull".into())
            .as_str()
        {
            "disable" => ConnectorTlsModeV2::Disable,
            "require" => ConnectorTlsModeV2::Require,
            "verifyFull" => ConnectorTlsModeV2::VerifyFull,
            value => panic!("unsupported connector test TLS mode {value}"),
        }
    }
}
