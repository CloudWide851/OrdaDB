
async fn send_rows(
    sink: &mut dyn ConnectorEventSinkV3,
    rows: &mut Vec<Vec<ConnectorValueV2>>,
    processed: u64,
) -> Result<()> {
    sink.send(ConnectorResultEventV3::Batch {
        batch: ConnectorResultBatchV3::Rows {
            rows: std::mem::take(rows),
        },
    })
    .await?;
    sink.send(ConnectorResultEventV3::Progress {
        items_processed: processed,
    })
    .await
}

fn parse_catalog_cursor(cursor: Option<&str>) -> Result<u64> {
    cursor
        .map(|value| {
            if value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid("invalid ClickHouse Catalog cursor"));
            }
            value
                .parse::<u64>()
                .map_err(|_| invalid("invalid ClickHouse Catalog cursor"))
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn parse_catalog_parent(parent_id: &str, limit: u64, offset: u64) -> Result<CatalogQuery> {
    if let Some(encoded) = parent_id.strip_prefix("clickhouse:database:") {
        let database = decode_id_component(encoded)?;
        return Ok(CatalogQuery {
            sql: format!(
                "SELECT database, name, engine, total_rows, total_bytes FROM system.tables \
                 WHERE database = {{database:String}} ORDER BY name LIMIT {limit} OFFSET {offset}"
            ),
            parameters: vec![("param_database".into(), database.clone())],
            parent: CatalogParent::Database { database },
        });
    }
    if let Some(encoded) = parent_id.strip_prefix("clickhouse:table:") {
        let (database, table) = encoded
            .split_once(':')
            .ok_or_else(|| invalid("invalid ClickHouse table Catalog ID"))?;
        let database = decode_id_component(database)?;
        let table = decode_id_component(table)?;
        return Ok(CatalogQuery {
            sql: format!(
                "SELECT database, table, name, type, position, default_kind, default_expression \
                 FROM system.columns WHERE database = {{database:String}} AND table = {{table:String}} \
                 ORDER BY position LIMIT {limit} OFFSET {offset}"
            ),
            parameters: vec![
                ("param_database".into(), database.clone()),
                ("param_table".into(), table.clone()),
            ],
            parent: CatalogParent::Table { database, table },
        });
    }
    Err(DbError::new(
        "42704",
        "ClickHouse Catalog parent does not exist",
    ))
}

fn catalog_nodes(
    parent: CatalogParent,
    rows: Vec<Vec<ConnectorValueV2>>,
    server_version: &str,
) -> Result<Vec<ConnectorCatalogNodeV3>> {
    rows.into_iter()
        .map(|row| match &parent {
            CatalogParent::Root => {
                let name = required_row_text(&row, 0, "database name")?;
                let engine = required_row_text(&row, 1, "database engine")?;
                Ok(ConnectorCatalogNodeV3 {
                    id: database_id(&name),
                    parent_id: None,
                    kind: ConnectorCatalogNodeKindV3::Database,
                    name,
                    namespace: None,
                    has_children: true,
                    columns: Vec::new(),
                    attributes: BTreeMap::from([
                        ("engine".into(), engine),
                        ("serverVersion".into(), server_version.to_owned()),
                    ]),
                })
            }
            CatalogParent::Database { database } => {
                let row_database = required_row_text(&row, 0, "table database")?;
                if &row_database != database {
                    return Err(protocol_error(
                        "ClickHouse table Catalog row escaped its parent database",
                    ));
                }
                let name = required_row_text(&row, 1, "table name")?;
                let engine = required_row_text(&row, 2, "table engine")?;
                let kind = match engine.as_str() {
                    "View" => ConnectorCatalogNodeKindV3::View,
                    "MaterializedView" => ConnectorCatalogNodeKindV3::MaterializedView,
                    _ => ConnectorCatalogNodeKindV3::Table,
                };
                let mut attributes = BTreeMap::from([("engine".into(), engine)]);
                if let Some(value) = row.get(3).and_then(display_value) {
                    attributes.insert("totalRows".into(), value);
                }
                if let Some(value) = row.get(4).and_then(display_value) {
                    attributes.insert("totalBytes".into(), value);
                }
                Ok(ConnectorCatalogNodeV3 {
                    id: table_id(database, &name),
                    parent_id: Some(database_id(database)),
                    kind,
                    name,
                    namespace: Some(database.clone()),
                    has_children: true,
                    columns: Vec::new(),
                    attributes,
                })
            }
            CatalogParent::Table { database, table } => {
                let row_database = required_row_text(&row, 0, "column database")?;
                let row_table = required_row_text(&row, 1, "column table")?;
                if &row_database != database || &row_table != table {
                    return Err(protocol_error(
                        "ClickHouse column Catalog row escaped its parent table",
                    ));
                }
                let name = required_row_text(&row, 2, "column name")?;
                let vendor_type = required_row_text(&row, 3, "column type")?;
                let position = required_row_text(&row, 4, "column position")?;
                let default_kind = required_row_text(&row, 5, "column default kind")?;
                let default_expression = required_row_text(&row, 6, "column default expression")?;
                Ok(ConnectorCatalogNodeV3 {
                    id: format!(
                        "{}:{}",
                        table_id(database, table),
                        encode_id_component(&name)
                    ),
                    parent_id: Some(table_id(database, table)),
                    kind: ConnectorCatalogNodeKindV3::Column,
                    name,
                    namespace: Some(format!("{database}.{table}")),
                    has_children: false,
                    columns: Vec::new(),
                    attributes: BTreeMap::from([
                        ("type".into(), vendor_type),
                        ("position".into(), position),
                        ("defaultKind".into(), default_kind),
                        ("defaultExpression".into(), default_expression),
                    ]),
                })
            }
        })
        .collect()
}

fn required_row_text(row: &[ConnectorValueV2], index: usize, field: &str) -> Result<String> {
    row.get(index)
        .and_then(display_value)
        .ok_or_else(|| protocol_error(format!("ClickHouse Catalog {field} is invalid")))
}

fn display_value(value: &ConnectorValueV2) -> Option<String> {
    match value {
        ConnectorValueV2::Null => None,
        ConnectorValueV2::Boolean(value) => Some(value.to_string()),
        ConnectorValueV2::SignedInteger(value) => Some(value.to_string()),
        ConnectorValueV2::UnsignedInteger(value) => Some(value.to_string()),
        ConnectorValueV2::FloatingPoint(value) => Some(value.to_string()),
        ConnectorValueV2::Text(value)
        | ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Uuid(value) => Some(value.clone()),
        ConnectorValueV2::Json(value) => serde_json::to_string(value).ok(),
        ConnectorValueV2::Binary(_)
        | ConnectorValueV2::Interval(_)
        | ConnectorValueV2::Array(_) => None,
    }
}

fn value_text(value: &ConnectorValueV2) -> Option<&str> {
    match value {
        ConnectorValueV2::Text(value)
        | ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Uuid(value) => Some(value),
        _ => None,
    }
}

fn database_id(database: &str) -> String {
    format!("clickhouse:database:{}", encode_id_component(database))
}

fn table_id(database: &str, table: &str) -> String {
    format!(
        "clickhouse:table:{}:{}",
        encode_id_component(database),
        encode_id_component(table)
    )
}

fn encode_id_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn decode_id_component(value: &str) -> Result<String> {
    if value.is_empty() || !value.len().is_multiple_of(2) || value.len() > 1_024 {
        return Err(invalid("invalid ClickHouse Catalog ID component"));
    }
    let bytes = value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair)
                .map_err(|_| invalid("invalid ClickHouse Catalog ID component"))?;
            u8::from_str_radix(pair, 16)
                .map_err(|_| invalid("invalid ClickHouse Catalog ID component"))
        })
        .collect::<Result<Vec<_>>>()?;
    String::from_utf8(bytes).map_err(|_| invalid("invalid ClickHouse Catalog ID component"))
}

fn response_summary(response: &Response) -> Option<u64> {
    let summary = response
        .headers()
        .get("x-clickhouse-summary")?
        .to_str()
        .ok()?;
    if summary.len() > 4_096 {
        return None;
    }
    let values = serde_json::from_str::<BTreeMap<String, String>>(summary).ok()?;
    values
        .get("written_rows")
        .or_else(|| values.get("read_rows"))?
        .parse()
        .ok()
}

fn ensure_success(response: Response) -> Result<Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let sql_state = match status {
        StatusCode::UNAUTHORIZED => "28P01",
        StatusCode::FORBIDDEN => "42501",
        StatusCode::NOT_FOUND => "42704",
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => "57014",
        StatusCode::TOO_MANY_REQUESTS => "53300",
        status if status.is_redirection() => "08001",
        status if status.is_server_error() => "58000",
        _ => "22023",
    };
    let vendor_code = response
        .headers()
        .get("x-clickhouse-exception-code")
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() <= 32);
    let mut error = DbError::new(sql_state, "ClickHouse HTTP request failed")
        .with_detail(format!("HTTP status {}", status.as_u16()));
    if let Some(vendor_code) = vendor_code {
        error = error.with_hint(format!("ClickHouse exception code: {vendor_code}"));
    } else if status.is_redirection() {
        error = error.with_hint("Configure the final ClickHouse endpoint; redirects are disabled.");
    }
    Err(error)
}

fn clickhouse_network_error(error: reqwest::Error) -> DbError {
    let detail = if error.is_timeout() {
        "request timed out"
    } else if error.is_connect() {
        "connection failed"
    } else if error.is_body() {
        "response stream failed"
    } else {
        "HTTP transport failed"
    };
    DbError::new("08006", "ClickHouse connection failed").with_detail(detail)
}

fn command_tag(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .unwrap_or("CLICKHOUSE")
        .to_ascii_uppercase()
}

fn cancelled() -> DbError {
    DbError::new("57014", "ClickHouse query was cancelled")
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn limit_error(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::new("08P01", message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::Mutex,
    };

    use super::*;

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
    fn capabilities_types_and_catalog_ids_are_stable() {
        let capabilities = capabilities();
        assert_eq!(capabilities.kind, ConnectorKindV3::Sql);
        assert!(!capabilities.transactions);
        assert!(!capabilities.savepoints);
        assert_eq!(capabilities.command_languages[0].id, LANGUAGE_ID);

        let array = clickhouse_type("Nullable(Array(UInt64))");
        assert_eq!(array.logical_type, ConnectorLogicalTypeV2::Array);
        assert_eq!(
            array
                .element_type
                .as_deref()
                .map(|value| value.logical_type),
            Some(ConnectorLogicalTypeV2::UnsignedInteger)
        );
        let decimal = clickhouse_type("Decimal(18, 4)");
        assert_eq!(decimal.precision, Some(18));
        assert_eq!(decimal.scale, Some(4));

        let name = "数据库:metrics";
        let encoded = encode_id_component(name);
        assert_eq!(decode_id_component(&encoded).expect("decode"), name);
        assert_eq!(
            table_id("default", "events"),
            "clickhouse:table:64656661756c74:6576656e7473"
        );
    }

    #[test]
    fn compact_json_decoder_handles_split_chunks_and_typed_rows() {
        let mut decoder = CompactJsonDecoder::default();
        let mut lines = decoder
            .push_chunk(b"[\"id\",\"name\",\"amount\"]\n[\"UInt64\",\"Str")
            .expect("first chunk");
        assert!(lines.is_empty());
        lines.extend(
            decoder
                .push_chunk(b"ing\",\"Decimal(10,2)\"]\n[\"42\",\"item\",\"1.25\"]\n")
                .expect("second chunk"),
        );
        assert!(decoder.finish().expect("finish").is_none());
        assert!(matches!(lines[0], DecodedLine::Schema(_)));
        let DecodedLine::Row(row) = &lines[1] else {
            panic!("row expected");
        };
        assert_eq!(row[0], ConnectorValueV2::UnsignedInteger(42));
        assert_eq!(row[1], ConnectorValueV2::Text("item".into()));
        assert_eq!(row[2], ConnectorValueV2::Decimal("1.25".into()));
    }

    #[tokio::test]
    async fn http_transport_streams_catalog_and_kills_the_exact_query() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
            let address = listener.local_addr().expect("address");
            let observed = Arc::new(Mutex::new(Vec::<String>::new()));
            let server_observed = Arc::clone(&observed);
            let server = tokio::spawn(async move {
                let (mut probe, _) = listener.accept().await.expect("probe accept");
                let probe_request = read_request(&mut probe).await;
                server_observed.lock().await.push(probe_request.clone());
                write_response(
                    &mut probe,
                    StatusCode::OK,
                    b"[\"version\"]\n[\"String\"]\n[\"25.3.1.1\"]\n",
                    &[],
                )
                .await;

                let (mut catalog, _) = listener.accept().await.expect("catalog accept");
                let catalog_request = read_request(&mut catalog).await;
                server_observed.lock().await.push(catalog_request.clone());
                write_response(
                    &mut catalog,
                    StatusCode::OK,
                    b"[\"name\",\"engine\"]\n[\"String\",\"String\"]\n[\"default\",\"Atomic\"]\n[\"system\",\"Atomic\"]\n[\"third\",\"Atomic\"]\n",
                    &[],
                )
                .await;

                let (mut query, _) = listener.accept().await.expect("query accept");
                let query_request = read_request(&mut query).await;
                let query_id = request_query_id(&query_request).expect("query ID");
                server_observed.lock().await.push(query_request);
                query
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .expect("query headers");

                let (mut kill, _) = listener.accept().await.expect("kill accept");
                let kill_request = read_request(&mut kill).await;
                assert!(kill_request.contains(&format!(
                    "KILL QUERY WHERE query_id = '{query_id}' SYNC"
                )));
                server_observed.lock().await.push(kill_request);
                write_response(&mut kill, StatusCode::OK, b"", &[]).await;
                query
                    .write_all(b"0\r\n\r\n")
                    .await
                    .expect("finish query body");
            });

            let mut session = ClickHouseDriver
                .connect(
                    ConnectorEndpointV2::Network {
                        host: address.ip().to_string(),
                        port: address.port(),
                        database: Some("default".into()),
                        instance: None,
                        options: BTreeMap::new(),
                    },
                    ConnectorTlsModeV2::Disable,
                    None,
                )
                .await
                .expect("connect fake ClickHouse");
            let page = session
                .catalog_page(None, 2, None)
                .await
                .expect("Catalog page");
            assert_eq!(page.nodes.len(), 2);
            assert_eq!(page.next_cursor.as_deref(), Some("2"));

            let cancellation = CancellationToken::new();
            let trigger = cancellation.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                trigger.cancel();
            });
            let error = session
                .execute(
                    "clickhouse-cancel",
                    &ConnectorCommandV3::Text {
                        language_id: LANGUAGE_ID.into(),
                        text: "SELECT sleep(30)".into(),
                        params: Vec::new(),
                    },
                    16,
                    &cancellation,
                    &mut RecordingSink::default(),
                )
                .await
                .expect_err("cancel query");
            assert_eq!(error.sql_state, "57014");
            server.await.expect("server task");
            let observed = observed.lock().await;
            assert!(observed[1].contains("FROM system.databases"));
            assert_eq!(observed.len(), 4);
        })
        .await
        .expect("ClickHouse fake transport exceeded its deadline");
    }

    #[tokio::test]
    async fn real_clickhouse_matrix_when_configured() {
        let Some(host) = std::env::var("ORDADB_TEST_CLICKHOUSE_HOST").ok() else {
            assert!(
                std::env::var("ORDADB_REQUIRE_REAL_CONNECTOR_TESTS").as_deref() != Ok("1"),
                "ORDADB_TEST_CLICKHOUSE_HOST is required for the real connector matrix"
            );
            return;
        };
        let username = std::env::var("ORDADB_TEST_CLICKHOUSE_USER")
            .expect("ORDADB_TEST_CLICKHOUSE_USER must accompany the host");
        let password = std::env::var("ORDADB_TEST_CLICKHOUSE_PASSWORD")
            .expect("ORDADB_TEST_CLICKHOUSE_PASSWORD must accompany the host");
        let mut session = tokio::time::timeout(
            Duration::from_secs(45),
            ClickHouseDriver.connect(
                ConnectorEndpointV2::Network {
                    host,
                    port: env_port("ORDADB_TEST_CLICKHOUSE_PORT", 8123),
                    database: std::env::var("ORDADB_TEST_CLICKHOUSE_DATABASE").ok(),
                    instance: None,
                    options: BTreeMap::new(),
                },
                env_tls_mode("ORDADB_TEST_CLICKHOUSE_TLS"),
                Some(ConnectorCredentialV2::new(Some(username), password)),
            ),
        )
        .await
        .expect("ClickHouse connection exceeded its deadline")
        .expect("connect ClickHouse");

        let page = session
            .catalog_page(None, 64, None)
            .await
            .expect("ClickHouse Catalog root");
        assert!(!page.nodes.is_empty());

        let mut sink = RecordingSink::default();
        session
            .execute(
                "clickhouse-stream",
                &ConnectorCommandV3::Text {
                    language_id: LANGUAGE_ID.into(),
                    text: "SELECT number FROM numbers(2048) ORDER BY number".into(),
                    params: Vec::new(),
                },
                128,
                &CancellationToken::new(),
                &mut sink,
            )
            .await
            .expect("ClickHouse streamed query");
        let rows = sink
            .events
            .iter()
            .filter_map(|event| match event {
                ConnectorResultEventV3::Batch {
                    batch: ConnectorResultBatchV3::Rows { rows },
                } => Some(rows.len()),
                _ => None,
            })
            .sum::<usize>();
        assert_eq!(rows, 2_048);
        assert_eq!(
            session
                .begin(None)
                .await
                .expect_err("ClickHouse transactions are unsupported")
                .sql_state,
            "0A000"
        );
    }

    #[tokio::test]
    async fn http_redirects_are_not_followed() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
            let address = listener.local_addr().expect("address");
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let _ = read_request(&mut stream).await;
                write_response(
                    &mut stream,
                    StatusCode::FOUND,
                    b"",
                    &[("Location", "http://127.0.0.1:1/")],
                )
                .await;
            });
            let result = ClickHouseDriver
                .connect(
                    ConnectorEndpointV2::Network {
                        host: address.ip().to_string(),
                        port: address.port(),
                        database: None,
                        instance: None,
                        options: BTreeMap::new(),
                    },
                    ConnectorTlsModeV2::Disable,
                    None,
                )
                .await;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("redirect must be rejected"),
            };
            assert_eq!(error.sql_state, "08001");
            server.await.expect("server task");
        })
        .await
        .expect("redirect test exceeded its deadline");
    }

    async fn read_request(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4_096];
        let header_end = loop {
            let read = stream.read(&mut buffer).await.expect("read request");
            assert!(read > 0, "request ended before its headers");
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(position) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or_default();
        while bytes.len().saturating_sub(header_end) < content_length {
            let read = stream.read(&mut buffer).await.expect("read request body");
            assert!(read > 0, "request body was truncated");
            bytes.extend_from_slice(&buffer[..read]);
        }
        String::from_utf8_lossy(&bytes).into_owned()
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

    async fn write_response(
        stream: &mut TcpStream,
        status: StatusCode,
        body: &[u8],
        headers: &[(&str, &str)],
    ) {
        let mut response = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Response"),
            body.len()
        );
        for (name, value) in headers {
            response.push_str(name);
            response.push_str(": ");
            response.push_str(value);
            response.push_str("\r\n");
        }
        response.push_str("\r\n");
        stream
            .write_all(response.as_bytes())
            .await
            .expect("response headers");
        stream.write_all(body).await.expect("response body");
    }

    fn request_query_id(request: &str) -> Option<String> {
        let request_line = request.lines().next()?;
        let path = request_line.split_whitespace().nth(1)?;
        path.split('?')
            .nth(1)?
            .split('&')
            .find_map(|pair| pair.strip_prefix("query_id=").map(str::to_owned))
    }
}
