use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use futures_util::StreamExt;
use ordadb_connector_sdk::{
    ConnectorCapabilitiesV3, ConnectorCatalogNodeKindV3, ConnectorCatalogNodeV3,
    ConnectorCatalogPageV3, ConnectorColumnV2, ConnectorCommandInputModeV3,
    ConnectorCommandLanguageV3, ConnectorCommandV3, ConnectorCredentialV2, ConnectorDriverV3,
    ConnectorEndpointV2, ConnectorEventSinkV3, ConnectorIsolationLevelV2, ConnectorKindV3,
    ConnectorLogicalTypeV2, ConnectorResultBatchV3, ConnectorResultEventV3, ConnectorSessionV3,
    ConnectorTlsModeV2, ConnectorTypeV2, ConnectorValueV2, connector_pipe_argument,
    run_named_pipe_helper_v3,
};
use ordadb_types::{DbError, Result};
use reqwest::{Client, Response, StatusCode, Url, redirect::Policy};
use serde_json::Value as JsonValue;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

const PLUGIN_ID: &str = "clickhouse";
const LANGUAGE_ID: &str = "clickhouse-sql";
const DEFAULT_DATABASE: &str = "default";
const MAX_RESULT_LINE_BYTES: usize = 2 * 1024 * 1024;
const MAX_CATALOG_RESPONSE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_HTTP_ERROR_TEXT_BYTES: usize = 256;
const CATALOG_TIMEOUT: Duration = Duration::from_secs(30);
const CANCEL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Default)]
struct ClickHouseDriver;

struct ClickHouseSession {
    client: Client,
    base_url: Url,
    database: String,
    username: String,
    secret: Zeroizing<String>,
    server_version: String,
    capabilities: ConnectorCapabilitiesV3,
    active_query: Option<(String, String)>,
}

#[derive(Debug)]
enum DecodedLine {
    Schema(Vec<ConnectorColumnV2>),
    Row(Vec<ConnectorValueV2>),
}

#[derive(Default)]
struct CompactJsonDecoder {
    pending: Vec<u8>,
    names: Option<Vec<String>>,
    types: Option<Vec<ConnectorTypeV2>>,
}

#[tokio::main]
async fn main() {
    let result = async {
        let pipe = connector_pipe_argument()?;
        run_named_pipe_helper_v3(
            &pipe,
            PLUGIN_ID,
            env!("CARGO_PKG_VERSION"),
            ClickHouseDriver,
        )
        .await
    }
    .await;
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[async_trait]
impl ConnectorDriverV3 for ClickHouseDriver {
    fn capabilities(&self) -> ConnectorCapabilitiesV3 {
        capabilities()
    }

    async fn connect(
        &self,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<Box<dyn ConnectorSessionV3>> {
        let (base_url, database) = endpoint_url(endpoint, tls_mode)?;
        let (username, secret) = match credential {
            Some(credential) => (
                credential
                    .username
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| "default".into()),
                credential.secret,
            ),
            None => ("default".into(), Zeroizing::new(String::new())),
        };
        let client = http_client(tls_mode)?;
        let mut session = ClickHouseSession {
            client,
            base_url,
            database,
            username,
            secret,
            server_version: String::new(),
            capabilities: capabilities(),
            active_query: None,
        };
        let probe = session
            .fetch_rows(
                "SELECT version() AS version",
                &[],
                MAX_CATALOG_RESPONSE_BYTES,
                CATALOG_TIMEOUT,
            )
            .await?;
        let version = probe
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(value_text)
            .ok_or_else(|| DbError::new("08006", "ClickHouse did not return a server version"))?;
        if version.len() > MAX_HTTP_ERROR_TEXT_BYTES {
            return Err(protocol_error("ClickHouse server version is too long"));
        }
        session.server_version = version.to_owned();
        Ok(Box::new(session))
    }
}

#[async_trait]
impl ConnectorSessionV3 for ClickHouseSession {
    fn capabilities(&self) -> &ConnectorCapabilitiesV3 {
        &self.capabilities
    }

    async fn catalog_page(
        &mut self,
        parent_id: Option<&str>,
        page_size: u32,
        cursor: Option<&str>,
    ) -> Result<ConnectorCatalogPageV3> {
        if page_size == 0 || page_size > self.capabilities.maximum_catalog_page_size {
            return Err(invalid(
                "ClickHouse Catalog page size is outside its capability",
            ));
        }
        let offset = parse_catalog_cursor(cursor)?;
        let limit = u64::from(page_size).saturating_add(1);
        let catalog_query = match parent_id {
            None => CatalogQuery {
                sql: format!(
                    "SELECT name, engine FROM system.databases ORDER BY name LIMIT {limit} OFFSET {offset}"
                ),
                parameters: Vec::new(),
                parent: CatalogParent::Root,
            },
            Some(parent_id) => parse_catalog_parent(parent_id, limit, offset)?,
        };
        let result = self
            .fetch_rows(
                &catalog_query.sql,
                &catalog_query.parameters,
                MAX_CATALOG_RESPONSE_BYTES,
                CATALOG_TIMEOUT,
            )
            .await?;
        let mut nodes = catalog_nodes(catalog_query.parent, result.rows, &self.server_version)?;
        let has_more = nodes.len() > usize::try_from(page_size).unwrap_or(512);
        if has_more {
            nodes.pop();
        }
        Ok(ConnectorCatalogPageV3 {
            nodes,
            next_cursor: has_more.then(|| offset.saturating_add(u64::from(page_size)).to_string()),
        })
    }

    async fn execute(
        &mut self,
        request_id: &str,
        command: &ConnectorCommandV3,
        batch_size: u32,
        cancellation: &CancellationToken,
        sink: &mut dyn ConnectorEventSinkV3,
    ) -> Result<()> {
        if batch_size == 0 || batch_size > self.capabilities.maximum_batch_rows {
            return Err(invalid(
                "ClickHouse connector batch size is outside its capability",
            ));
        }
        let ConnectorCommandV3::Text {
            language_id,
            text,
            params,
        } = command
        else {
            return Err(DbError::unsupported("ClickHouse non-SQL command input"));
        };
        if language_id != LANGUAGE_ID {
            return Err(DbError::unsupported(format!(
                "ClickHouse command language {language_id}",
            )));
        }
        if !params.is_empty() {
            return Err(DbError::unsupported("ClickHouse positional parameters")
                .with_hint("Use ClickHouse typed named parameters in a future connector update."));
        }
        let query_id = Uuid::new_v4().to_string();
        self.active_query = Some((request_id.to_owned(), query_id.clone()));
        let result = self
            .execute_query(text, batch_size, cancellation, sink, &query_id)
            .await;
        self.active_query = None;
        if result
            .as_ref()
            .is_err_and(|error| error.sql_state != "57014")
        {
            let _ = self.kill_query(&query_id).await;
        }
        result
    }

    async fn cancel(&mut self, request_id: &str) -> Result<()> {
        let query_id = self
            .active_query
            .as_ref()
            .filter(|(active_request, _)| active_request == request_id)
            .map(|(_, query_id)| query_id.clone())
            .ok_or_else(|| DbError::new("42704", "ClickHouse request is not active"))?;
        self.kill_query(&query_id).await
    }

    async fn begin(&mut self, _isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
        Err(DbError::unsupported("ClickHouse transactions"))
    }

    async fn commit(&mut self) -> Result<()> {
        Err(DbError::unsupported("ClickHouse transactions"))
    }

    async fn rollback(&mut self) -> Result<()> {
        Err(DbError::unsupported("ClickHouse transactions"))
    }
}

impl ClickHouseSession {
    async fn execute_query(
        &self,
        sql: &str,
        batch_size: u32,
        cancellation: &CancellationToken,
        sink: &mut dyn ConnectorEventSinkV3,
        query_id: &str,
    ) -> Result<()> {
        let response = tokio::select! {
            () = cancellation.cancelled() => {
                self.kill_query(query_id).await?;
                return Err(cancelled());
            }
            response = self.query_request(sql, query_id, &[], None).send() => {
                response.map_err(clickhouse_network_error)?
            }
        };
        let summary = response_summary(&response);
        let response = ensure_success(response)?;
        let mut stream = response.bytes_stream();
        let mut decoder = CompactJsonDecoder::default();
        let batch_size = usize::try_from(batch_size).unwrap_or(1_024);
        let mut rows = Vec::with_capacity(batch_size);
        let mut processed = 0_u64;
        loop {
            let chunk = tokio::select! {
                () = cancellation.cancelled() => {
                    self.kill_query(query_id).await?;
                    return Err(cancelled());
                }
                chunk = stream.next() => chunk,
            };
            let Some(chunk) = chunk else {
                break;
            };
            let chunk = chunk.map_err(clickhouse_network_error)?;
            for line in decoder.push_chunk(&chunk)? {
                match line {
                    DecodedLine::Schema(columns) => {
                        sink.send(ConnectorResultEventV3::Schema { columns })
                            .await?;
                    }
                    DecodedLine::Row(row) => {
                        rows.push(row);
                        processed = processed.saturating_add(1);
                        if rows.len() == batch_size {
                            send_rows(sink, &mut rows, processed).await?;
                        }
                    }
                }
            }
        }
        if let Some(line) = decoder.finish()? {
            match line {
                DecodedLine::Schema(columns) => {
                    sink.send(ConnectorResultEventV3::Schema { columns })
                        .await?;
                }
                DecodedLine::Row(row) => {
                    rows.push(row);
                    processed = processed.saturating_add(1);
                }
            }
        }
        if !rows.is_empty() {
            send_rows(sink, &mut rows, processed).await?;
        }
        let affected_items = summary.or(Some(processed));
        sink.send(ConnectorResultEventV3::Progress {
            items_processed: affected_items.unwrap_or_default(),
        })
        .await?;
        sink.send(ConnectorResultEventV3::Complete {
            command_tag: command_tag(sql),
            affected_items,
        })
        .await
    }

    async fn fetch_rows(
        &self,
        sql: &str,
        parameters: &[(String, String)],
        maximum_bytes: u64,
        timeout: Duration,
    ) -> Result<CollectedRows> {
        let query_id = Uuid::new_v4().to_string();
        let response = self
            .query_request(sql, &query_id, parameters, Some(timeout))
            .send()
            .await
            .map_err(clickhouse_network_error)?;
        let response = ensure_success(response)?;
        if response
            .content_length()
            .is_some_and(|length| length > maximum_bytes)
        {
            return Err(limit_error("ClickHouse Catalog response is too large"));
        }
        let mut stream = response.bytes_stream();
        let mut decoder = CompactJsonDecoder::default();
        let mut rows = Vec::new();
        let mut received = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(clickhouse_network_error)?;
            received = received.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            if received > maximum_bytes {
                return Err(limit_error("ClickHouse Catalog response is too large"));
            }
            for line in decoder.push_chunk(&chunk)? {
                match line {
                    DecodedLine::Schema(_) => {}
                    DecodedLine::Row(row) => rows.push(row),
                }
            }
        }
        if let Some(line) = decoder.finish()? {
            match line {
                DecodedLine::Schema(_) => {}
                DecodedLine::Row(row) => rows.push(row),
            }
        }
        Ok(CollectedRows { rows })
    }

    fn query_request(
        &self,
        sql: &str,
        query_id: &str,
        parameters: &[(String, String)],
        timeout: Option<Duration>,
    ) -> reqwest::RequestBuilder {
        let mut query = vec![
            ("database".to_owned(), self.database.clone()),
            ("query_id".to_owned(), query_id.to_owned()),
            (
                "default_format".to_owned(),
                "JSONCompactEachRowWithNamesAndTypes".to_owned(),
            ),
        ];
        query.extend(parameters.iter().cloned());
        let request = self
            .client
            .post(self.base_url.clone())
            .query(&query)
            .basic_auth(&self.username, Some(self.secret.as_str()))
            .body(sql.to_owned());
        match timeout {
            Some(timeout) => request.timeout(timeout),
            None => request,
        }
    }

    async fn kill_query(&self, query_id: &str) -> Result<()> {
        if !query_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
        {
            return Err(protocol_error("ClickHouse query ID is invalid"));
        }
        let kill_id = Uuid::new_v4().to_string();
        let sql = format!("KILL QUERY WHERE query_id = '{query_id}' SYNC");
        let response = self
            .query_request(&sql, &kill_id, &[], Some(CANCEL_TIMEOUT))
            .send()
            .await
            .map_err(clickhouse_network_error)?;
        ensure_success(response)?;
        Ok(())
    }
}

#[derive(Debug)]
struct CollectedRows {
    rows: Vec<Vec<ConnectorValueV2>>,
}

#[derive(Debug)]
enum CatalogParent {
    Root,
    Database { database: String },
    Table { database: String, table: String },
}

#[derive(Debug)]
struct CatalogQuery {
    sql: String,
    parameters: Vec<(String, String)>,
    parent: CatalogParent,
}

fn capabilities() -> ConnectorCapabilitiesV3 {
    ConnectorCapabilitiesV3 {
        kind: ConnectorKindV3::Sql,
        command_languages: vec![ConnectorCommandLanguageV3 {
            id: LANGUAGE_ID.into(),
            display_name: "ClickHouse SQL".into(),
            input_modes: vec![ConnectorCommandInputModeV3::Text],
        }],
        catalog: true,
        cancellation: true,
        transactions: false,
        savepoints: false,
        batch_query: true,
        maximum_batch_rows: 1_024,
        maximum_catalog_page_size: 512,
        tls_modes: vec![
            ConnectorTlsModeV2::Disable,
            ConnectorTlsModeV2::Require,
            ConnectorTlsModeV2::VerifyCa,
            ConnectorTlsModeV2::VerifyFull,
        ],
    }
}

fn endpoint_url(
    endpoint: ConnectorEndpointV2,
    tls_mode: ConnectorTlsModeV2,
) -> Result<(Url, String)> {
    let ConnectorEndpointV2::Network {
        host,
        port,
        database,
        instance,
        options,
    } = endpoint
    else {
        return Err(invalid("ClickHouse requires a network endpoint"));
    };
    if instance.is_some() {
        return Err(invalid(
            "ClickHouse endpoints do not accept an instance name",
        ));
    }
    if !options.is_empty() {
        return Err(DbError::unsupported(
            "ClickHouse connector endpoint options",
        ));
    }
    if host.trim().is_empty() || port == 0 {
        return Err(invalid("ClickHouse host and port are required"));
    }
    let scheme = match tls_mode {
        ConnectorTlsModeV2::Disable => "http",
        ConnectorTlsModeV2::Require
        | ConnectorTlsModeV2::VerifyCa
        | ConnectorTlsModeV2::VerifyFull => "https",
        ConnectorTlsModeV2::Prefer => {
            return Err(DbError::unsupported("ClickHouse opportunistic TLS")
                .with_hint("Select disable or an enforced TLS mode."));
        }
    };
    let mut url = Url::parse(&format!("{scheme}://localhost:{port}/"))
        .map_err(|_| invalid("ClickHouse endpoint URL is invalid"))?;
    url.set_host(Some(&host))
        .map_err(|_| invalid("ClickHouse host is invalid"))?;
    let database = database.unwrap_or_else(|| DEFAULT_DATABASE.into());
    if database.trim().is_empty() || database.len() > 512 || database.contains('\0') {
        return Err(invalid("ClickHouse database name is invalid"));
    }
    Ok((url, database))
}

fn http_client(tls_mode: ConnectorTlsModeV2) -> Result<Client> {
    let builder = Client::builder()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(15));
    let builder = match tls_mode {
        ConnectorTlsModeV2::Require => builder
            .tls_danger_accept_invalid_certs(true)
            .tls_danger_accept_invalid_hostnames(true),
        ConnectorTlsModeV2::VerifyCa => builder.tls_danger_accept_invalid_hostnames(true),
        ConnectorTlsModeV2::Disable | ConnectorTlsModeV2::VerifyFull => builder,
        ConnectorTlsModeV2::Prefer => {
            return Err(DbError::unsupported("ClickHouse opportunistic TLS"));
        }
    };
    builder.build().map_err(clickhouse_network_error)
}

impl CompactJsonDecoder {
    fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<DecodedLine>> {
        let mut lines = Vec::new();
        for segment in chunk.split_inclusive(|byte| *byte == b'\n') {
            let complete = segment.last() == Some(&b'\n');
            let content = if complete {
                &segment[..segment.len().saturating_sub(1)]
            } else {
                segment
            };
            self.pending.extend_from_slice(content);
            if self.pending.len() > MAX_RESULT_LINE_BYTES {
                return Err(limit_error("ClickHouse result line exceeds 2 MiB"));
            }
            if complete && let Some(line) = self.decode_pending()? {
                lines.push(line);
            }
        }
        Ok(lines)
    }

    fn finish(&mut self) -> Result<Option<DecodedLine>> {
        if self.pending.is_empty() {
            if self.names.is_some() && self.types.is_none() {
                return Err(protocol_error(
                    "ClickHouse result ended before its type row",
                ));
            }
            return Ok(None);
        }
        let line = self.decode_pending()?;
        if self.names.is_some() && self.types.is_none() {
            return Err(protocol_error(
                "ClickHouse result ended before its type row",
            ));
        }
        Ok(line)
    }

    fn decode_pending(&mut self) -> Result<Option<DecodedLine>> {
        if self.pending.last() == Some(&b'\r') {
            self.pending.pop();
        }
        if self.pending.is_empty() {
            return Ok(None);
        }
        let value = serde_json::from_slice::<JsonValue>(&self.pending)
            .map_err(|_| protocol_error("ClickHouse returned malformed compact JSON"))?;
        self.pending.clear();
        let values = value
            .as_array()
            .ok_or_else(|| protocol_error("ClickHouse compact JSON line is not an array"))?;
        if self.names.is_none() {
            let names = values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|name| !name.is_empty() && name.len() <= 512)
                        .map(str::to_owned)
                        .ok_or_else(|| protocol_error("ClickHouse column name is invalid"))
                })
                .collect::<Result<Vec<_>>>()?;
            self.names = Some(names);
            return Ok(None);
        }
        if self.types.is_none() {
            let names = self
                .names
                .as_ref()
                .ok_or_else(|| protocol_error("ClickHouse result names are missing"))?;
            if values.len() != names.len() {
                return Err(protocol_error(
                    "ClickHouse type row width does not match its names",
                ));
            }
            let types = values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(clickhouse_type)
                        .ok_or_else(|| protocol_error("ClickHouse column type is invalid"))
                })
                .collect::<Result<Vec<_>>>()?;
            let columns = names
                .iter()
                .cloned()
                .zip(types.iter().cloned())
                .map(|(name, data_type)| ConnectorColumnV2 {
                    name,
                    nullable: data_type.vendor_name.starts_with("Nullable("),
                    data_type,
                })
                .collect();
            self.types = Some(types);
            return Ok(Some(DecodedLine::Schema(columns)));
        }
        let types = self
            .types
            .as_ref()
            .ok_or_else(|| protocol_error("ClickHouse result types are missing"))?;
        if values.len() != types.len() {
            return Err(protocol_error(
                "ClickHouse result row width does not match its schema",
            ));
        }
        let row = values
            .iter()
            .zip(types)
            .map(|(value, data_type)| clickhouse_value(value, data_type))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(DecodedLine::Row(row)))
    }
}

fn clickhouse_type(vendor_name: &str) -> ConnectorTypeV2 {
    let core = unwrap_clickhouse_type(vendor_name);
    let element_type = array_inner(core).map(|inner| Box::new(clickhouse_type(inner)));
    let logical_type = if element_type.is_some() {
        ConnectorLogicalTypeV2::Array
    } else if matches!(core, "Nothing" | "Null") {
        ConnectorLogicalTypeV2::Null
    } else if core == "Bool" {
        ConnectorLogicalTypeV2::Boolean
    } else if core.starts_with("UInt") && !matches!(core, "UInt128" | "UInt256") {
        ConnectorLogicalTypeV2::UnsignedInteger
    } else if core.starts_with("Int") && !matches!(core, "Int128" | "Int256") {
        ConnectorLogicalTypeV2::SignedInteger
    } else if core.starts_with("Float") {
        ConnectorLogicalTypeV2::FloatingPoint
    } else if core.starts_with("Decimal")
        || matches!(core, "Int128" | "Int256" | "UInt128" | "UInt256")
    {
        ConnectorLogicalTypeV2::Decimal
    } else if matches!(core, "Date" | "Date32") {
        ConnectorLogicalTypeV2::Date
    } else if core.starts_with("DateTime") {
        if core.contains('\'') {
            ConnectorLogicalTypeV2::TimestampWithTimeZone
        } else {
            ConnectorLogicalTypeV2::Timestamp
        }
    } else if core == "UUID" {
        ConnectorLogicalTypeV2::Uuid
    } else if core.starts_with("JSON")
        || core.starts_with("Object(")
        || core.starts_with("Map(")
        || core.starts_with("Tuple(")
    {
        ConnectorLogicalTypeV2::Json
    } else {
        ConnectorLogicalTypeV2::Text
    };
    let (precision, scale) = decimal_precision_scale(core);
    let length = fixed_string_length(core);
    ConnectorTypeV2 {
        vendor_name: vendor_name.to_owned(),
        logical_type,
        element_type,
        precision,
        scale,
        length,
    }
}

fn clickhouse_value(value: &JsonValue, data_type: &ConnectorTypeV2) -> Result<ConnectorValueV2> {
    if value.is_null() {
        return Ok(ConnectorValueV2::Null);
    }
    match data_type.logical_type {
        ConnectorLogicalTypeV2::Null => Ok(ConnectorValueV2::Null),
        ConnectorLogicalTypeV2::Boolean => match value {
            JsonValue::Bool(value) => Ok(ConnectorValueV2::Boolean(*value)),
            JsonValue::Number(value) => Ok(ConnectorValueV2::Boolean(value.as_u64() != Some(0))),
            JsonValue::String(value) if matches!(value.as_str(), "0" | "false") => {
                Ok(ConnectorValueV2::Boolean(false))
            }
            JsonValue::String(value) if matches!(value.as_str(), "1" | "true") => {
                Ok(ConnectorValueV2::Boolean(true))
            }
            _ => Err(protocol_error("ClickHouse returned an invalid Boolean")),
        },
        ConnectorLogicalTypeV2::SignedInteger => parse_i64(value)
            .map(ConnectorValueV2::SignedInteger)
            .ok_or_else(|| protocol_error("ClickHouse returned an invalid signed integer")),
        ConnectorLogicalTypeV2::UnsignedInteger => parse_u64(value)
            .map(ConnectorValueV2::UnsignedInteger)
            .ok_or_else(|| protocol_error("ClickHouse returned an invalid unsigned integer")),
        ConnectorLogicalTypeV2::FloatingPoint => {
            let number = parse_f64(value)
                .filter(|value| value.is_finite())
                .ok_or_else(|| protocol_error("ClickHouse returned an invalid floating point"))?;
            Ok(ConnectorValueV2::FloatingPoint(number))
        }
        ConnectorLogicalTypeV2::Decimal => scalar_text(value)
            .map(ConnectorValueV2::Decimal)
            .ok_or_else(|| protocol_error("ClickHouse returned an invalid decimal")),
        ConnectorLogicalTypeV2::Date => string_value(value)
            .map(ConnectorValueV2::Date)
            .ok_or_else(|| protocol_error("ClickHouse returned an invalid date")),
        ConnectorLogicalTypeV2::Time => string_value(value)
            .map(ConnectorValueV2::Time)
            .ok_or_else(|| protocol_error("ClickHouse returned an invalid time")),
        ConnectorLogicalTypeV2::Timestamp => string_value(value)
            .map(ConnectorValueV2::Timestamp)
            .ok_or_else(|| protocol_error("ClickHouse returned an invalid timestamp")),
        ConnectorLogicalTypeV2::TimestampWithTimeZone => string_value(value)
            .map(ConnectorValueV2::TimestampWithTimeZone)
            .ok_or_else(|| protocol_error("ClickHouse returned an invalid zoned timestamp")),
        ConnectorLogicalTypeV2::Uuid => string_value(value)
            .map(ConnectorValueV2::Uuid)
            .ok_or_else(|| protocol_error("ClickHouse returned an invalid UUID")),
        ConnectorLogicalTypeV2::Array => {
            let values = value
                .as_array()
                .ok_or_else(|| protocol_error("ClickHouse returned an invalid array"))?;
            let element_type = data_type
                .element_type
                .as_deref()
                .ok_or_else(|| protocol_error("ClickHouse array element type is missing"))?;
            values
                .iter()
                .map(|value| clickhouse_value(value, element_type))
                .collect::<Result<Vec<_>>>()
                .map(ConnectorValueV2::Array)
        }
        ConnectorLogicalTypeV2::Json => Ok(ConnectorValueV2::Json(value.clone())),
        ConnectorLogicalTypeV2::Text | ConnectorLogicalTypeV2::Other => string_value(value)
            .map(ConnectorValueV2::Text)
            .ok_or_else(|| protocol_error("ClickHouse returned invalid text")),
        ConnectorLogicalTypeV2::Binary | ConnectorLogicalTypeV2::Interval => {
            Err(DbError::unsupported("ClickHouse result type mapping"))
        }
    }
}

fn unwrap_clickhouse_type(mut value: &str) -> &str {
    loop {
        let trimmed = value.trim();
        if let Some(inner) =
            wrapped_inner(trimmed, "Nullable").or_else(|| wrapped_inner(trimmed, "LowCardinality"))
        {
            value = inner;
        } else {
            return trimmed;
        }
    }
}

fn wrapped_inner<'a>(value: &'a str, wrapper: &str) -> Option<&'a str> {
    value
        .strip_prefix(wrapper)?
        .strip_prefix('(')?
        .strip_suffix(')')
}

fn array_inner(value: &str) -> Option<&str> {
    wrapped_inner(value, "Array")
}

fn decimal_precision_scale(value: &str) -> (Option<u32>, Option<u32>) {
    let Some(arguments) = value
        .strip_prefix("Decimal(")
        .and_then(|value| value.strip_suffix(')'))
    else {
        return (None, None);
    };
    let mut values = arguments.split(',').map(str::trim);
    let precision = values.next().and_then(|value| value.parse().ok());
    let scale = values.next().and_then(|value| value.parse().ok());
    (precision, scale)
}

fn fixed_string_length(value: &str) -> Option<u64> {
    value
        .strip_prefix("FixedString(")?
        .strip_suffix(')')?
        .parse()
        .ok()
}

fn parse_i64(value: &JsonValue) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn parse_u64(value: &JsonValue) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn parse_f64(value: &JsonValue) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn scalar_text(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn string_value(value: &JsonValue) -> Option<String> {
    value.as_str().map(str::to_owned)
}

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
