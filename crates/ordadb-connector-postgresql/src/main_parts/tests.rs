
#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<ConnectorQueryEventV2>,
    }

    impl ConnectorEventSink for RecordingSink {
        fn send(
            &mut self,
            event: ConnectorQueryEventV2,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async move {
                self.events.push(event);
                Ok(())
            })
        }
    }

    #[test]
    fn postgres_type_mapping_covers_core_wire_types() {
        assert_eq!(
            postgres_type(&Type::INT8).logical_type,
            ConnectorLogicalTypeV2::SignedInteger
        );
        assert_eq!(
            postgres_type(&Type::TIMESTAMPTZ).logical_type,
            ConnectorLogicalTypeV2::TimestampWithTimeZone
        );
        assert_eq!(
            postgres_type(&Type::JSONB).logical_type,
            ConnectorLogicalTypeV2::Json
        );
    }

    #[test]
    fn postgres_catalog_projection_groups_columns() {
        let data_type = postgres_named_type("integer", "int4");
        assert_eq!(
            data_type.logical_type,
            ConnectorLogicalTypeV2::SignedInteger
        );
        assert_eq!(data_type.vendor_name, "int4");
    }

    #[tokio::test]
    async fn real_postgresql_matrix_when_configured() {
        let Some(host) = std::env::var("ORDADB_TEST_POSTGRESQL_HOST").ok() else {
            assert!(
                std::env::var("ORDADB_REQUIRE_REAL_CONNECTOR_TESTS").as_deref() != Ok("1"),
                "ORDADB_TEST_POSTGRESQL_HOST is required for the real connector matrix"
            );
            return;
        };
        let port = env_port("ORDADB_TEST_POSTGRESQL_PORT", 5432);
        let database = std::env::var("ORDADB_TEST_POSTGRESQL_DATABASE").ok();
        let username = std::env::var("ORDADB_TEST_POSTGRESQL_USER")
            .expect("ORDADB_TEST_POSTGRESQL_USER must accompany the host");
        let password = std::env::var("ORDADB_TEST_POSTGRESQL_PASSWORD")
            .expect("ORDADB_TEST_POSTGRESQL_PASSWORD must accompany the host");
        let tls_mode = env_tls_mode("ORDADB_TEST_POSTGRESQL_TLS");
        let mut session = PostgresDriver
            .connect(
                ConnectorEndpointV2::Network {
                    host,
                    port,
                    database,
                    instance: None,
                    options: BTreeMap::from([(
                        "applicationName".into(),
                        "ordadb-real-connector-test".into(),
                    )]),
                },
                tls_mode,
                Some(ConnectorCredentialV2::new(Some(username), password)),
            )
            .await
            .expect("connect PostgreSQL");

        let mut version_sink = RecordingSink::default();
        session
            .execute(
                "postgresql-version",
                "SELECT current_setting('server_version')",
                &[],
                1,
                &CancellationToken::new(),
                &mut version_sink,
            )
            .await
            .expect("PostgreSQL version");
        assert!(
            first_text(&version_sink.events).starts_with("18."),
            "the real connector matrix requires PostgreSQL 18"
        );
        session.catalog().await.expect("PostgreSQL Catalog");
        session
            .begin(Some(ConnectorIsolationLevelV2::ReadCommitted))
            .await
            .expect("begin");
        let mut sink = RecordingSink::default();
        session
            .execute(
                "postgresql-types",
                "SELECT TRUE,
                        42::bigint,
                        1.25::numeric,
                        'text'::text,
                        decode('00ff', 'hex')::bytea,
                        DATE '2026-01-02',
                        TIMESTAMP '2026-01-02 03:04:05',
                        TIMESTAMPTZ '2026-01-02 03:04:05+00',
                        '00000000-0000-0000-0000-000000000001'::uuid,
                        '{\"ok\":true}'::jsonb",
                &[],
                64,
                &CancellationToken::new(),
                &mut sink,
            )
            .await
            .expect("typed query");
        session.rollback().await.expect("rollback");
        assert!(
            sink.events
                .iter()
                .any(|event| matches!(event, ConnectorQueryEventV2::Complete { .. }))
        );

        let mut stream_sink = RecordingSink::default();
        session
            .execute(
                "postgresql-large",
                "SELECT value FROM generate_series(1, 4096) AS value",
                &[],
                128,
                &CancellationToken::new(),
                &mut stream_sink,
            )
            .await
            .expect("large stream");
        let rows = streamed_rows(&stream_sink.events);
        assert_eq!(rows, 4096);

        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let error = session
            .execute(
                "postgresql-cancel",
                "SELECT pg_sleep(30)",
                &[],
                64,
                &cancellation,
                &mut RecordingSink::default(),
            )
            .await
            .expect_err("cancelled PostgreSQL query");
        assert_eq!(error.sql_state, "57014");
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
            "prefer" => ConnectorTlsModeV2::Prefer,
            "require" => ConnectorTlsModeV2::Require,
            "verifyFull" => ConnectorTlsModeV2::VerifyFull,
            value => panic!("unsupported connector test TLS mode {value}"),
        }
    }

    fn streamed_rows(events: &[ConnectorQueryEventV2]) -> usize {
        events
            .iter()
            .filter_map(|event| match event {
                ConnectorQueryEventV2::Batch { batch } => Some(batch.rows.len()),
                _ => None,
            })
            .sum()
    }

    fn first_text(events: &[ConnectorQueryEventV2]) -> &str {
        events
            .iter()
            .find_map(|event| match event {
                ConnectorQueryEventV2::Batch { batch } => batch.rows.first(),
                _ => None,
            })
            .and_then(|row| row.first())
            .and_then(|value| match value {
                ConnectorValueV2::Text(value) => Some(value.as_str()),
                _ => None,
            })
            .expect("text result")
    }
}
