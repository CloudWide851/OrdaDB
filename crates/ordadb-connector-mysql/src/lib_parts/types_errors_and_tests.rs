
fn mysql_value(value: &MySqlValue, data_type: &ConnectorTypeV2) -> Result<ConnectorValueV2> {
    match value {
        MySqlValue::NULL => Ok(ConnectorValueV2::Null),
        MySqlValue::Int(value) => {
            if data_type.logical_type == ConnectorLogicalTypeV2::Boolean {
                Ok(ConnectorValueV2::Boolean(*value != 0))
            } else {
                Ok(ConnectorValueV2::SignedInteger(*value))
            }
        }
        MySqlValue::UInt(value) => Ok(ConnectorValueV2::UnsignedInteger(*value)),
        MySqlValue::Float(value) => Ok(ConnectorValueV2::FloatingPoint(f64::from(*value))),
        MySqlValue::Double(value) => Ok(ConnectorValueV2::FloatingPoint(*value)),
        MySqlValue::Bytes(value) => mysql_bytes(value, data_type),
        MySqlValue::Date(year, month, day, hour, minute, second, micros) => {
            let value = format!(
                "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}"
            );
            if data_type.logical_type == ConnectorLogicalTypeV2::Date {
                Ok(ConnectorValueV2::Date(value[..10].to_owned()))
            } else {
                Ok(ConnectorValueV2::Timestamp(value))
            }
        }
        MySqlValue::Time(negative, days, hour, minute, second, micros) => {
            let total_hours = days.saturating_mul(24).saturating_add(u32::from(*hour));
            Ok(ConnectorValueV2::Time(format!(
                "{}{total_hours:02}:{minute:02}:{second:02}.{micros:06}",
                if *negative { "-" } else { "" }
            )))
        }
    }
}

fn mysql_bytes(value: &[u8], data_type: &ConnectorTypeV2) -> Result<ConnectorValueV2> {
    if data_type.logical_type == ConnectorLogicalTypeV2::Binary {
        return Ok(ConnectorValueV2::Binary(BASE64.encode(value)));
    }
    let text = std::str::from_utf8(value)
        .map_err(|_| DbError::new("22021", "MySQL returned non-UTF-8 text"))?
        .to_owned();
    match data_type.logical_type {
        ConnectorLogicalTypeV2::Boolean => Ok(ConnectorValueV2::Boolean(
            text != "0" && !text.eq_ignore_ascii_case("false"),
        )),
        ConnectorLogicalTypeV2::Decimal => Ok(ConnectorValueV2::Decimal(text)),
        ConnectorLogicalTypeV2::Date => Ok(ConnectorValueV2::Date(text)),
        ConnectorLogicalTypeV2::Time => Ok(ConnectorValueV2::Time(text)),
        ConnectorLogicalTypeV2::Timestamp => Ok(ConnectorValueV2::Timestamp(text)),
        ConnectorLogicalTypeV2::Json => serde_json::from_str::<JsonValue>(&text)
            .map(ConnectorValueV2::Json)
            .map_err(|error| {
                DbError::new("22032", "MySQL returned invalid JSON").with_detail(error.to_string())
            }),
        _ => Ok(ConnectorValueV2::Text(text)),
    }
}

fn mysql_column(column: &Column) -> ConnectorColumnV2 {
    ConnectorColumnV2 {
        name: column.name_str().into_owned(),
        data_type: mysql_type(column),
        nullable: !format!("{:?}", column.flags()).contains("NOT_NULL_FLAG"),
    }
}

fn mysql_type(column: &Column) -> ConnectorTypeV2 {
    let vendor_name = format!("{:?}", column.column_type())
        .trim_start_matches("MYSQL_TYPE_")
        .to_owned();
    let logical_type = mysql_logical_type(&vendor_name, column.column_length());
    ConnectorTypeV2 {
        vendor_name,
        logical_type,
        element_type: None,
        precision: None,
        scale: (column.decimals() <= 0x51).then(|| u32::from(column.decimals())),
        length: Some(u64::from(column.column_length())),
    }
}

fn mysql_named_type(data_type: &str, column_type: &str) -> ConnectorTypeV2 {
    ConnectorTypeV2 {
        vendor_name: column_type.to_owned(),
        logical_type: mysql_named_logical_type(data_type),
        element_type: None,
        precision: None,
        scale: None,
        length: None,
    }
}

fn mysql_logical_type(vendor_name: &str, length: u32) -> ConnectorLogicalTypeV2 {
    match vendor_name {
        "NULL" => ConnectorLogicalTypeV2::Null,
        "TINY" if length == 1 => ConnectorLogicalTypeV2::Boolean,
        "TINY" | "SHORT" | "LONG" | "INT24" | "LONGLONG" | "YEAR" => {
            ConnectorLogicalTypeV2::SignedInteger
        }
        "FLOAT" | "DOUBLE" => ConnectorLogicalTypeV2::FloatingPoint,
        "DECIMAL" | "NEWDECIMAL" => ConnectorLogicalTypeV2::Decimal,
        "BIT" if length == 1 => ConnectorLogicalTypeV2::Boolean,
        "BIT" | "TINY_BLOB" | "MEDIUM_BLOB" | "LONG_BLOB" | "BLOB" | "GEOMETRY" => {
            ConnectorLogicalTypeV2::Binary
        }
        "DATE" | "NEWDATE" => ConnectorLogicalTypeV2::Date,
        "TIME" | "TIME2" => ConnectorLogicalTypeV2::Time,
        "TIMESTAMP" | "TIMESTAMP2" | "DATETIME" | "DATETIME2" => ConnectorLogicalTypeV2::Timestamp,
        "JSON" => ConnectorLogicalTypeV2::Json,
        _ => ConnectorLogicalTypeV2::Text,
    }
}

fn mysql_named_logical_type(data_type: &str) -> ConnectorLogicalTypeV2 {
    match data_type.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => ConnectorLogicalTypeV2::Boolean,
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
            ConnectorLogicalTypeV2::SignedInteger
        }
        "float" | "double" | "real" => ConnectorLogicalTypeV2::FloatingPoint,
        "decimal" | "numeric" => ConnectorLogicalTypeV2::Decimal,
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "geometry" => {
            ConnectorLogicalTypeV2::Binary
        }
        "date" => ConnectorLogicalTypeV2::Date,
        "time" => ConnectorLogicalTypeV2::Time,
        "datetime" | "timestamp" => ConnectorLogicalTypeV2::Timestamp,
        "json" => ConnectorLogicalTypeV2::Json,
        _ => ConnectorLogicalTypeV2::Text,
    }
}

fn mysql_row_text(row: &Row, index: usize, name: &str) -> Result<String> {
    match row.as_ref(index) {
        Some(MySqlValue::Bytes(value)) => String::from_utf8(value.clone())
            .map_err(|_| DbError::new("22021", format!("MySQL Catalog field {name} is not UTF-8"))),
        Some(value) => Err(DbError::new(
            "08P01",
            format!("MySQL Catalog field {name} had unexpected value {value:?}"),
        )),
        None => Err(DbError::new(
            "08P01",
            format!("MySQL Catalog field {name} is missing"),
        )),
    }
}

fn mysql_row_optional_text(row: &Row, index: usize) -> Result<Option<String>> {
    match row.as_ref(index) {
        Some(MySqlValue::NULL) => Ok(None),
        Some(MySqlValue::Bytes(value)) => String::from_utf8(value.clone())
            .map(Some)
            .map_err(|_| DbError::new("22021", "MySQL Catalog default is not UTF-8")),
        Some(_) => Err(DbError::new(
            "08P01",
            "MySQL Catalog default had an unexpected value",
        )),
        None => Err(DbError::new("08P01", "MySQL Catalog default is missing")),
    }
}

fn mysql_row_u32(row: &Row, index: usize, name: &str) -> Result<u32> {
    let value = match row.as_ref(index) {
        Some(MySqlValue::Int(value)) => u64::try_from(*value).ok(),
        Some(MySqlValue::UInt(value)) => Some(*value),
        Some(MySqlValue::Bytes(value)) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse().ok()),
        _ => None,
    };
    value
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            DbError::new(
                "08P01",
                format!("MySQL Catalog field {name} is not a valid u32"),
            )
        })
}

fn command_tag(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .unwrap_or("MYSQL")
        .to_ascii_uppercase()
}

fn mysql_error(error: MySqlError) -> DbError {
    family_error(error, MySqlFamily::MySql)
}

fn mariadb_error(error: MySqlError) -> DbError {
    family_error(error, MySqlFamily::MariaDb)
}

fn family_error(error: MySqlError, family: MySqlFamily) -> DbError {
    match error {
        MySqlError::Server(server) => {
            let sql_state = if server.state.len() == 5 {
                server.state
            } else {
                "HY000".into()
            };
            DbError::new(
                sql_state,
                format!("{} connector operation failed", family.display_name()),
            )
            .with_detail(format!("{} (vendor code {})", server.message, server.code))
        }
        MySqlError::Io(error) => DbError::new(
            "08006",
            format!("{} connection failed", family.display_name()),
        )
        .with_detail(error.to_string()),
        MySqlError::Driver(error) => DbError::new(
            "08006",
            format!("{} driver operation failed", family.display_name()),
        )
        .with_detail(error.to_string()),
        error => DbError::new(
            "58000",
            format!("{} connector operation failed", family.display_name()),
        )
        .with_detail(error.to_string()),
    }
}

fn is_mariadb_version(version: &str) -> bool {
    version.to_ascii_lowercase().contains("mariadb")
}

fn cancelled(family: MySqlFamily) -> DbError {
    DbError::new(
        "57014",
        format!("{} query was cancelled", family.display_name()),
    )
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

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

    #[derive(Default)]
    struct RecordingSinkV3 {
        events: Vec<ConnectorResultEventV3>,
    }

    impl ConnectorEventSinkV3 for RecordingSinkV3 {
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
    fn capabilities_and_type_mapping_are_stable() {
        let capabilities = mysql_capabilities_v2();
        assert!(capabilities.catalog);
        assert!(capabilities.cancellation);
        assert_eq!(capabilities.maximum_batch_rows, 1024);
        assert_eq!(
            mysql_named_logical_type("json"),
            ConnectorLogicalTypeV2::Json
        );
        assert_eq!(
            mysql_named_logical_type("varbinary"),
            ConnectorLogicalTypeV2::Binary
        );
    }

    #[test]
    fn mariadb_identity_capabilities_and_version_check_are_distinct() {
        let capabilities = mariadb_capabilities_v3();
        assert_eq!(capabilities.kind, ConnectorKindV3::Sql);
        assert_eq!(capabilities.command_languages.len(), 1);
        assert_eq!(capabilities.command_languages[0].id, MARIADB_LANGUAGE_ID);
        assert!(is_mariadb_version("11.8.2-MariaDB"));
        assert!(is_mariadb_version("10.11.8-mariadb-log"));
        assert!(!is_mariadb_version("8.4.5"));

        let node = catalog_node_v3(ConnectorCatalogObjectV2 {
            id: "mariadb:database:app".into(),
            kind: ConnectorCatalogObjectKindV2::Database,
            catalog: Some("app".into()),
            schema: None,
            name: "app".into(),
            parent_id: None,
            comment: None,
            columns: Vec::new(),
            attributes: BTreeMap::new(),
        });
        assert_eq!(node.id, "mariadb:database:app");
        assert_eq!(node.kind, ConnectorCatalogNodeKindV3::Database);
        assert!(node.has_children);
    }

    #[test]
    fn mariadb_catalog_paging_and_tls_policy_fail_closed() {
        let nodes = (0..3)
            .map(|index| ConnectorCatalogNodeV3 {
                id: format!("mariadb:database:{index}"),
                parent_id: None,
                kind: ConnectorCatalogNodeKindV3::Database,
                name: index.to_string(),
                namespace: None,
                has_children: true,
                columns: Vec::new(),
                attributes: BTreeMap::new(),
            })
            .collect();
        let first = paginate_catalog(nodes, 2, None).expect("first page");
        assert_eq!(first.nodes.len(), 2);
        assert_eq!(first.next_cursor.as_deref(), Some("2"));

        let error = match connection_options(
            MySqlFamily::MariaDb,
            ConnectorEndpointV2::Network {
                host: "localhost".into(),
                port: 3306,
                database: None,
                instance: None,
                options: BTreeMap::new(),
            },
            ConnectorTlsModeV2::Prefer,
            Some(ConnectorCredentialV2::new(Some("user".into()), "secret")),
        ) {
            Err(error) => error,
            Ok(_) => panic!("opportunistic TLS must be rejected"),
        };
        assert_eq!(error.sql_state, "0A000");
        assert!(!format!("{error:?}").contains("secret"));
    }

    #[test]
    fn parameters_preserve_unsigned_and_binary_values() {
        assert_eq!(
            mysql_parameter(&ConnectorParameterV2 {
                data_type: None,
                value: ConnectorValueV2::UnsignedInteger(u64::MAX),
            })
            .expect("unsigned"),
            MySqlValue::UInt(u64::MAX)
        );
        assert_eq!(
            mysql_parameter(&ConnectorParameterV2 {
                data_type: None,
                value: ConnectorValueV2::Binary(BASE64.encode([0_u8, 255])),
            })
            .expect("binary"),
            MySqlValue::Bytes(vec![0, 255])
        );
    }

    #[tokio::test]
    async fn real_mysql_matrix_when_configured() {
        let Some(host) = std::env::var("ORDADB_TEST_MYSQL_HOST").ok() else {
            assert!(
                std::env::var("ORDADB_REQUIRE_REAL_CONNECTOR_TESTS").as_deref() != Ok("1"),
                "ORDADB_TEST_MYSQL_HOST is required for the real connector matrix"
            );
            return;
        };
        let username = std::env::var("ORDADB_TEST_MYSQL_USER")
            .expect("ORDADB_TEST_MYSQL_USER must accompany the host");
        let password = std::env::var("ORDADB_TEST_MYSQL_PASSWORD")
            .expect("ORDADB_TEST_MYSQL_PASSWORD must accompany the host");
        let mut session = MySqlDriver
            .connect(
                ConnectorEndpointV2::Network {
                    host,
                    port: env_port("ORDADB_TEST_MYSQL_PORT", 3306),
                    database: std::env::var("ORDADB_TEST_MYSQL_DATABASE").ok(),
                    instance: None,
                    options: BTreeMap::new(),
                },
                env_tls_mode("ORDADB_TEST_MYSQL_TLS"),
                Some(ConnectorCredentialV2::new(Some(username), password)),
            )
            .await
            .expect("connect MySQL");

        let mut version_sink = RecordingSink::default();
        session
            .execute(
                "mysql-version",
                "SELECT VERSION()",
                &[],
                1,
                &CancellationToken::new(),
                &mut version_sink,
            )
            .await
            .expect("MySQL version");
        assert!(
            first_text(&version_sink.events).starts_with("8.4."),
            "the real connector matrix requires MySQL 8.4 LTS"
        );
        session.catalog().await.expect("MySQL Catalog");
        session
            .begin(Some(ConnectorIsolationLevelV2::ReadCommitted))
            .await
            .expect("begin");
        let mut sink = RecordingSink::default();
        session
            .execute(
                "mysql-types",
                "SELECT TRUE,
                        CAST(42 AS SIGNED),
                        CAST(42 AS UNSIGNED),
                        CAST(1.25 AS DECIMAL(10,2)),
                        CAST('text' AS CHAR),
                        X'00FF',
                        DATE '2026-01-02',
                        TIMESTAMP '2026-01-02 03:04:05',
                        JSON_OBJECT('ok', TRUE)",
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
                "mysql-large",
                "WITH RECURSIVE seq(value) AS (
                    SELECT 1
                    UNION ALL
                    SELECT value + 1 FROM seq WHERE value < 512
                 )
                 SELECT value FROM seq",
                &[],
                64,
                &CancellationToken::new(),
                &mut stream_sink,
            )
            .await
            .expect("large stream");
        assert_eq!(streamed_rows(&stream_sink.events), 512);

        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let mut cancel_sink = RecordingSink::default();
        let error = session
            .execute(
                "mysql-cancel",
                "SELECT SLEEP(30)",
                &[],
                64,
                &cancellation,
                &mut cancel_sink,
            )
            .await
            .expect_err("cancelled MySQL query");
        assert_eq!(error.sql_state, "57014");
    }

    #[tokio::test]
    async fn real_mariadb_matrix_when_configured() {
        let Some(host) = std::env::var("ORDADB_TEST_MARIADB_HOST").ok() else {
            assert!(
                std::env::var("ORDADB_REQUIRE_REAL_CONNECTOR_TESTS").as_deref() != Ok("1"),
                "ORDADB_TEST_MARIADB_HOST is required for the real connector matrix"
            );
            return;
        };
        let username = std::env::var("ORDADB_TEST_MARIADB_USER")
            .expect("ORDADB_TEST_MARIADB_USER must accompany the host");
        let password = std::env::var("ORDADB_TEST_MARIADB_PASSWORD")
            .expect("ORDADB_TEST_MARIADB_PASSWORD must accompany the host");
        let mut session = MariaDbDriver
            .connect(
                ConnectorEndpointV2::Network {
                    host,
                    port: env_port("ORDADB_TEST_MARIADB_PORT", 3306),
                    database: std::env::var("ORDADB_TEST_MARIADB_DATABASE").ok(),
                    instance: None,
                    options: BTreeMap::new(),
                },
                env_tls_mode("ORDADB_TEST_MARIADB_TLS"),
                Some(ConnectorCredentialV2::new(Some(username), password)),
            )
            .await
            .expect("connect MariaDB");

        let mut version_sink = RecordingSinkV3::default();
        session
            .execute(
                "mariadb-version",
                &ConnectorCommandV3::Text {
                    language_id: MARIADB_LANGUAGE_ID.into(),
                    text: "SELECT VERSION()".into(),
                    params: Vec::new(),
                },
                1,
                &CancellationToken::new(),
                &mut version_sink,
            )
            .await
            .expect("MariaDB version");
        assert!(is_mariadb_version(first_text_v3(&version_sink.events)));
        session
            .catalog_page(None, 128, None)
            .await
            .expect("MariaDB Catalog");
        session
            .begin(Some(ConnectorIsolationLevelV2::ReadCommitted))
            .await
            .expect("begin");
        session.rollback().await.expect("rollback");

        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let error = session
            .execute(
                "mariadb-cancel",
                &ConnectorCommandV3::Text {
                    language_id: MARIADB_LANGUAGE_ID.into(),
                    text: "SELECT SLEEP(30)".into(),
                    params: Vec::new(),
                },
                64,
                &cancellation,
                &mut RecordingSinkV3::default(),
            )
            .await
            .expect_err("cancelled MariaDB query");
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
            "require" => ConnectorTlsModeV2::Require,
            "verifyCa" => ConnectorTlsModeV2::VerifyCa,
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

    fn first_text_v3(events: &[ConnectorResultEventV3]) -> &str {
        events
            .iter()
            .find_map(|event| match event {
                ConnectorResultEventV3::Batch {
                    batch: ConnectorResultBatchV3::Rows { rows },
                } => rows.first(),
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
