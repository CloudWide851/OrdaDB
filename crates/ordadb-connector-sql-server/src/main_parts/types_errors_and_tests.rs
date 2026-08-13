
fn sql_server_type(column_type: ColumnType) -> ConnectorTypeV2 {
    ConnectorTypeV2 {
        vendor_name: format!("{column_type:?}"),
        logical_type: match column_type {
            ColumnType::Null => ConnectorLogicalTypeV2::Null,
            ColumnType::Bit | ColumnType::Bitn => ConnectorLogicalTypeV2::Boolean,
            ColumnType::Int1 => ConnectorLogicalTypeV2::UnsignedInteger,
            ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 | ColumnType::Intn => {
                ConnectorLogicalTypeV2::SignedInteger
            }
            ColumnType::Float4 | ColumnType::Float8 | ColumnType::Floatn => {
                ConnectorLogicalTypeV2::FloatingPoint
            }
            ColumnType::Money
            | ColumnType::Money4
            | ColumnType::Decimaln
            | ColumnType::Numericn => ConnectorLogicalTypeV2::Decimal,
            ColumnType::Guid => ConnectorLogicalTypeV2::Uuid,
            ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image | ColumnType::Udt => {
                ConnectorLogicalTypeV2::Binary
            }
            ColumnType::Daten => ConnectorLogicalTypeV2::Date,
            ColumnType::Timen => ConnectorLogicalTypeV2::Time,
            ColumnType::Datetime4
            | ColumnType::Datetime
            | ColumnType::Datetimen
            | ColumnType::Datetime2 => ConnectorLogicalTypeV2::Timestamp,
            ColumnType::DatetimeOffsetn => ConnectorLogicalTypeV2::TimestampWithTimeZone,
            ColumnType::Xml => ConnectorLogicalTypeV2::Other,
            ColumnType::BigVarChar
            | ColumnType::BigChar
            | ColumnType::NVarchar
            | ColumnType::NChar
            | ColumnType::Text
            | ColumnType::NText
            | ColumnType::SSVariant => ConnectorLogicalTypeV2::Text,
        },
        element_type: None,
        precision: None,
        scale: None,
        length: None,
    }
}

fn sql_server_named_type(
    data_type: &str,
    max_length: i16,
    precision: u8,
    scale: u8,
) -> ConnectorTypeV2 {
    let normalized = data_type.to_ascii_lowercase();
    let logical_type = match normalized.as_str() {
        "bit" => ConnectorLogicalTypeV2::Boolean,
        "tinyint" => ConnectorLogicalTypeV2::UnsignedInteger,
        "smallint" | "int" | "bigint" => ConnectorLogicalTypeV2::SignedInteger,
        "real" | "float" => ConnectorLogicalTypeV2::FloatingPoint,
        "money" | "smallmoney" | "decimal" | "numeric" => ConnectorLogicalTypeV2::Decimal,
        "binary" | "varbinary" | "image" | "rowversion" | "timestamp" => {
            ConnectorLogicalTypeV2::Binary
        }
        "date" => ConnectorLogicalTypeV2::Date,
        "time" => ConnectorLogicalTypeV2::Time,
        "datetime" | "datetime2" | "smalldatetime" => ConnectorLogicalTypeV2::Timestamp,
        "datetimeoffset" => ConnectorLogicalTypeV2::TimestampWithTimeZone,
        "uniqueidentifier" => ConnectorLogicalTypeV2::Uuid,
        "xml" => ConnectorLogicalTypeV2::Other,
        _ => ConnectorLogicalTypeV2::Text,
    };
    ConnectorTypeV2 {
        vendor_name: data_type.to_owned(),
        logical_type,
        element_type: None,
        precision: matches!(normalized.as_str(), "decimal" | "numeric")
            .then(|| u32::from(precision)),
        scale: matches!(
            normalized.as_str(),
            "decimal" | "numeric" | "time" | "datetime2"
        )
        .then(|| u32::from(scale)),
        length: u64::try_from(max_length).ok(),
    }
}

fn required_text(row: &Row, index: usize, name: &str) -> Result<String> {
    row.try_get::<&str, _>(index)
        .map_err(tds_error)?
        .map(str::to_owned)
        .ok_or_else(|| DbError::new("08P01", format!("SQL Server Catalog field {name} is null")))
}

fn returns_rows(sql: &str) -> bool {
    let normalized = sql.trim_start().to_ascii_uppercase();
    let command = normalized.split_whitespace().next().unwrap_or("");
    matches!(command, "SELECT" | "WITH" | "VALUES" | "EXEC" | "EXECUTE")
        || (matches!(command, "INSERT" | "UPDATE" | "DELETE" | "MERGE")
            && normalized.contains(" OUTPUT "))
}

fn command_tag(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .unwrap_or("SQLSERVER")
        .to_ascii_uppercase()
}

fn cancelled() -> DbError {
    DbError::new("57014", "SQL Server query was cancelled")
}

fn tds_error(error: tiberius::error::Error) -> DbError {
    let sql_state = match error.code() {
        Some(1205) => "40001",
        Some(1222) => "55P03",
        Some(18456) => "28000",
        Some(229) => "42501",
        Some(207) => "42S22",
        Some(208) => "42S02",
        Some(2601 | 2627) => "23505",
        Some(547) => "23503",
        Some(596) => "57014",
        Some(_) => "HY000",
        None if matches!(
            error,
            tiberius::error::Error::Io { .. }
                | tiberius::error::Error::Tls(_)
                | tiberius::error::Error::Routing { .. }
        ) =>
        {
            "08006"
        }
        None => "58000",
    };
    let detail = match error.code() {
        Some(code) => format!("{error} (vendor code {code})"),
        None => error.to_string(),
    };
    DbError::new(sql_state, "SQL Server connector operation failed").with_detail(detail)
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

    #[test]
    fn capabilities_and_type_mapping_are_stable() {
        let capabilities = capabilities();
        assert!(capabilities.catalog);
        assert!(capabilities.cancellation);
        assert_eq!(capabilities.maximum_batch_rows, 1024);
        assert_eq!(
            sql_server_type(ColumnType::Guid).logical_type,
            ConnectorLogicalTypeV2::Uuid
        );
        assert_eq!(
            sql_server_named_type("varbinary", 64, 0, 0).logical_type,
            ConnectorLogicalTypeV2::Binary
        );
    }

    #[test]
    fn parameters_preserve_binary_and_reject_oversized_unsigned_values() {
        let parameter = sql_server_parameter(&ConnectorParameterV2 {
            data_type: None,
            value: ConnectorValueV2::Binary(BASE64.encode([0_u8, 255])),
        })
        .expect("binary");
        assert!(matches!(
            parameter.to_sql(),
            tiberius::ColumnData::Binary(Some(value)) if value.as_ref() == [0, 255]
        ));
        let overflow = match sql_server_parameter(&ConnectorParameterV2 {
            data_type: None,
            value: ConnectorValueV2::UnsignedInteger(u64::MAX),
        }) {
            Ok(_) => panic!("oversized unsigned parameter was accepted"),
            Err(error) => error,
        };
        assert_eq!(overflow.sql_state, "22023");
    }

    #[test]
    fn row_producing_statement_detection_handles_output() {
        assert!(returns_rows("SELECT 1"));
        assert!(returns_rows("UPDATE dbo.t SET x = 1 OUTPUT inserted.x"));
        assert!(!returns_rows("UPDATE dbo.t SET x = 1"));
    }

    #[tokio::test]
    async fn real_sql_server_matrix_when_configured() {
        let Some(host) = std::env::var("ORDADB_TEST_SQL_SERVER_HOST").ok() else {
            assert!(
                std::env::var("ORDADB_REQUIRE_REAL_CONNECTOR_TESTS").as_deref() != Ok("1"),
                "ORDADB_TEST_SQL_SERVER_HOST is required for the real connector matrix"
            );
            return;
        };
        let username = std::env::var("ORDADB_TEST_SQL_SERVER_USER")
            .expect("ORDADB_TEST_SQL_SERVER_USER must accompany the host");
        let password = std::env::var("ORDADB_TEST_SQL_SERVER_PASSWORD")
            .expect("ORDADB_TEST_SQL_SERVER_PASSWORD must accompany the host");
        let mut session = SqlServerDriver
            .connect(
                ConnectorEndpointV2::Network {
                    host,
                    port: env_port("ORDADB_TEST_SQL_SERVER_PORT", 1433),
                    database: std::env::var("ORDADB_TEST_SQL_SERVER_DATABASE").ok(),
                    instance: std::env::var("ORDADB_TEST_SQL_SERVER_INSTANCE").ok(),
                    options: BTreeMap::new(),
                },
                env_tls_mode("ORDADB_TEST_SQL_SERVER_TLS"),
                Some(ConnectorCredentialV2::new(Some(username), password)),
            )
            .await
            .expect("connect SQL Server");

        let mut version_sink = RecordingSink::default();
        session
            .execute(
                "sql-server-version",
                "SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128))",
                &[],
                1,
                &CancellationToken::new(),
                &mut version_sink,
            )
            .await
            .expect("SQL Server version");
        let major = first_text(&version_sink.events)
            .split('.')
            .next()
            .expect("SQL Server major version");
        assert!(
            matches!(major, "16" | "17"),
            "the real connector matrix requires SQL Server 2022 or 2025"
        );
        session.catalog().await.expect("SQL Server Catalog");
        session
            .begin(Some(ConnectorIsolationLevelV2::ReadCommitted))
            .await
            .expect("begin");
        let mut sink = RecordingSink::default();
        session
            .execute(
                "sql-server-types",
                "SELECT CAST(1 AS bit),
                        CAST(42 AS bigint),
                        CAST(1.25 AS decimal(10,2)),
                        CAST(N'text' AS nvarchar(20)),
                        CAST(0x00FF AS varbinary(2)),
                        CAST('2026-01-02' AS date),
                        CAST('03:04:05' AS time),
                        CAST('2026-01-02T03:04:05' AS datetime2),
                        CAST('2026-01-02T03:04:05+00:00' AS datetimeoffset),
                        CAST('00000000-0000-0000-0000-000000000001'
                             AS uniqueidentifier)",
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
                "sql-server-large",
                "WITH seq(value) AS (
                    SELECT 1
                    UNION ALL
                    SELECT value + 1 FROM seq WHERE value < 512
                 )
                 SELECT value FROM seq OPTION (MAXRECURSION 512)",
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
                "sql-server-cancel",
                "WAITFOR DELAY '00:00:30'; SELECT 1",
                &[],
                64,
                &cancellation,
                &mut cancel_sink,
            )
            .await
            .expect_err("cancelled SQL Server query");
        assert_eq!(error.sql_state, "57014");

        let mut reconnect_sink = RecordingSink::default();
        session
            .execute(
                "sql-server-reconnected",
                "SELECT 1",
                &[],
                64,
                &CancellationToken::new(),
                &mut reconnect_sink,
            )
            .await
            .expect("query after cancellation reconnect");
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
