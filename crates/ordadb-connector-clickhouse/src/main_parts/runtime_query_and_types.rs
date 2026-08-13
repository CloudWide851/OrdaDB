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
