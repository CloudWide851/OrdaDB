
#[cfg(test)]
mod tests {
    use super::*;
    use ordadb_connector_sdk::validate_capabilities_v3;

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
    fn capabilities_and_endpoint_modes_are_stable() {
        let capabilities = capabilities();
        validate_capabilities_v3(&capabilities).expect("valid capabilities");
        assert_eq!(capabilities.kind, ConnectorKindV3::KeyValue);
        assert!(!capabilities.transactions);
        let cluster = RedisEndpointOptions::parse(
            "127.0.0.1".into(),
            6379,
            Some("0"),
            BTreeMap::from([
                ("mode".into(), "cluster".into()),
                ("clusterNodes".into(), "127.0.0.2:6380".into()),
            ]),
        )
        .expect("cluster endpoint");
        assert_eq!(cluster.mode, RedisMode::Cluster);
        assert_eq!(cluster.nodes.len(), 2);
        assert_eq!(
            RedisEndpointOptions::parse(
                "127.0.0.1".into(),
                26379,
                None,
                BTreeMap::from([("mode".into(), "sentinel".into())]),
            )
            .expect_err("sentinel")
            .sql_state,
            "0A000"
        );
    }

    #[test]
    fn command_policy_rejects_secrets_blocking_and_transactions() {
        for command in ["AUTH", "SUBSCRIBE", "BLPOP", "MULTI"] {
            assert_eq!(
                redis_command(&[ConnectorValueV2::Text(command.into())])
                    .expect_err("unsafe command")
                    .sql_state,
                "0A000"
            );
        }
        assert_eq!(
            redis_command(&[
                ConnectorValueV2::Text("GET".into()),
                ConnectorValueV2::Text("redis://user:secret@example/0".into()),
            ])
            .expect_err("credential URL")
            .sql_state,
            "22023"
        );
        redis_command(&[
            ConnectorValueV2::Text("SET".into()),
            ConnectorValueV2::Text("key".into()),
            ConnectorValueV2::Binary(BASE64.encode([0_u8, 255])),
        ])
        .expect("binary command");
    }

    #[test]
    fn resp3_values_preserve_binary_maps_and_sets() {
        let value = RedisValue::Map(vec![
            (
                RedisValue::BulkString(b"text".to_vec()),
                RedisValue::Int(42),
            ),
            (
                RedisValue::BulkString(vec![0, 255]),
                RedisValue::Set(vec![RedisValue::Boolean(true)]),
            ),
        ]);
        let json = redis_json(&value, 0).expect("RESP3 JSON");
        assert!(json["$map"].is_array());
        let entries = response_entries("HGETALL", value).expect("entries");
        assert_eq!(entries.len(), 2);
        assert!(matches!(entries[1].key, ConnectorValueV2::Binary(_)));
        assert!(matches!(entries[1].value, ConnectorValueV2::Json(_)));
    }

    #[test]
    fn scan_cursor_and_binary_keys_are_bounded() {
        let (cursor, keys) = scan_response(RedisValue::Array(vec![
            RedisValue::BulkString(b"12".to_vec()),
            RedisValue::Array(vec![
                RedisValue::BulkString(b"alpha".to_vec()),
                RedisValue::BulkString(vec![0, 255]),
            ]),
        ]))
        .expect("SCAN response");
        assert_eq!(cursor, 12);
        let node = key_catalog_node("redis:keyspace:0", keys[1].clone()).expect("key node");
        assert!(node.name.starts_with("base64:"));
        assert_eq!(
            parse_scan_cursor(Some("invalid")).unwrap_err().sql_state,
            "22023"
        );
    }

    #[test]
    fn credentials_are_redacted_and_tls_cannot_fail_open() {
        let credential = ConnectorCredentialV2::new(Some("default".into()), "secret-value");
        let info = connection_info(
            "127.0.0.1",
            6379,
            0,
            Some(&credential),
            ConnectorTlsModeV2::Disable,
        )
        .expect("connection info");
        assert!(!format!("{info:?}").contains("secret-value"));
        assert_eq!(
            connection_info("127.0.0.1", 6379, 0, None, ConnectorTlsModeV2::Prefer,)
                .expect_err("prefer")
                .sql_state,
            "0A000"
        );
    }

    #[tokio::test]
    async fn real_redis_matrix_when_configured() {
        let Some(host) = std::env::var("ORDADB_TEST_REDIS_HOST").ok() else {
            assert!(
                std::env::var("ORDADB_REQUIRE_REAL_CONNECTOR_TESTS").as_deref() != Ok("1"),
                "ORDADB_TEST_REDIS_HOST is required for the real connector matrix"
            );
            return;
        };
        let password = std::env::var("ORDADB_TEST_REDIS_PASSWORD")
            .expect("ORDADB_TEST_REDIS_PASSWORD must accompany the host");
        let mode = std::env::var("ORDADB_TEST_REDIS_MODE").unwrap_or_else(|_| "standalone".into());
        let mut options = BTreeMap::from([("mode".into(), mode)]);
        if let Ok(nodes) = std::env::var("ORDADB_TEST_REDIS_CLUSTER_NODES") {
            options.insert("clusterNodes".into(), nodes);
        }
        let username = std::env::var("ORDADB_TEST_REDIS_USER").ok();
        let database = std::env::var("ORDADB_TEST_REDIS_DATABASE").unwrap_or_else(|_| "0".into());
        let mut session = tokio::time::timeout(
            std::time::Duration::from_secs(45),
            RedisDriver.connect(
                ConnectorEndpointV2::Network {
                    host,
                    port: env_port("ORDADB_TEST_REDIS_PORT", 6379),
                    database: Some(database),
                    instance: None,
                    options,
                },
                env_tls_mode("ORDADB_TEST_REDIS_TLS"),
                Some(ConnectorCredentialV2::new(username, password)),
            ),
        )
        .await
        .expect("Redis connection exceeded its deadline")
        .expect("connect Redis");

        let page = session
            .catalog_page(None, 64, None)
            .await
            .expect("Redis Catalog root");
        assert!(!page.nodes.is_empty());

        let ping = ConnectorCommandV3::Arguments {
            language_id: LANGUAGE_ID.into(),
            arguments: vec![ConnectorValueV2::Text("PING".into())],
        };
        let mut sink = RecordingSink::default();
        session
            .execute(
                "redis-ping",
                &ping,
                64,
                &CancellationToken::new(),
                &mut sink,
            )
            .await
            .expect("Redis PING");
        assert!(sink.events.iter().any(|event| matches!(
            event,
            ConnectorResultEventV3::Batch {
                batch: ConnectorResultBatchV3::KeyValues { entries }
            } if !entries.is_empty()
        )));

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = session
            .execute(
                "redis-cancel",
                &ping,
                64,
                &cancellation,
                &mut RecordingSink::default(),
            )
            .await
            .expect_err("cancelled Redis command");
        assert_eq!(error.sql_state, "57014");
        assert_eq!(
            session
                .begin(None)
                .await
                .expect_err("Redis transactions are unsupported")
                .sql_state,
            "0A000"
        );
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
