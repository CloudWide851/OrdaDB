use std::collections::{BTreeMap, BTreeSet};

use ordadb_types::{DbError, Result};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::protocol::{
    ConnectorColumnV2, ConnectorCredentialV2, ConnectorEndpointV2, ConnectorIsolationLevelV2,
    ConnectorNoticeV2, ConnectorParameterV2, ConnectorTlsModeV2, ConnectorTransactionStateV2,
    ConnectorValueV2, MAX_CONNECTOR_BATCH_ROWS, MAX_CONNECTOR_TEXT_BYTES,
};

pub const CONNECTOR_PROTOCOL_V3: u32 = 3;
pub const MAX_CONNECTOR_LANGUAGES: usize = 16;
pub const MAX_CONNECTOR_COMMAND_ARGUMENTS: usize = 4096;
pub const MAX_CONNECTOR_CATALOG_PAGE_NODES: u32 = 4096;
pub const MAX_CONNECTOR_CURSOR_BYTES: usize = 4096;
pub const MAX_CONNECTOR_JSON_DEPTH: usize = 64;
const MAX_CONNECTOR_IDENTIFIER_BYTES: usize = 512;
const MAX_CONNECTOR_DISPLAY_BYTES: usize = 256;
const MAX_CONNECTOR_ATTRIBUTES: usize = 64;
const MAX_CONNECTOR_ATTRIBUTE_BYTES: usize = 16 * 1024;
const MAX_CONNECTOR_ERROR_TEXT_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolHelloV3 {
    pub minimum_api_version: u32,
    pub maximum_api_version: u32,
    pub plugin_id: String,
    pub plugin_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolReadyV3 {
    pub api_version: u32,
    pub plugin_id: String,
    pub plugin_version: String,
    pub capabilities: ConnectorCapabilitiesV3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorKindV3 {
    Sql,
    Document,
    KeyValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorCommandInputModeV3 {
    Text,
    Document,
    Arguments,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorCommandLanguageV3 {
    pub id: String,
    pub display_name: String,
    pub input_modes: Vec<ConnectorCommandInputModeV3>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorCapabilitiesV3 {
    pub kind: ConnectorKindV3,
    pub command_languages: Vec<ConnectorCommandLanguageV3>,
    pub catalog: bool,
    pub cancellation: bool,
    pub transactions: bool,
    pub savepoints: bool,
    pub batch_query: bool,
    pub maximum_batch_rows: u32,
    pub maximum_catalog_page_size: u32,
    pub tls_modes: Vec<ConnectorTlsModeV2>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ConnectorCommandV3 {
    Text {
        language_id: String,
        text: String,
        params: Vec<ConnectorParameterV2>,
    },
    Document {
        language_id: String,
        document: Value,
    },
    Arguments {
        language_id: String,
        arguments: Vec<ConnectorValueV2>,
    },
}

impl ConnectorCommandV3 {
    #[must_use]
    pub fn language_id(&self) -> &str {
        match self {
            Self::Text { language_id, .. }
            | Self::Document { language_id, .. }
            | Self::Arguments { language_id, .. } => language_id,
        }
    }

    #[must_use]
    pub const fn input_mode(&self) -> ConnectorCommandInputModeV3 {
        match self {
            Self::Text { .. } => ConnectorCommandInputModeV3::Text,
            Self::Document { .. } => ConnectorCommandInputModeV3::Document,
            Self::Arguments { .. } => ConnectorCommandInputModeV3::Arguments,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorCatalogNodeKindV3 {
    Server,
    Cluster,
    Database,
    Schema,
    Table,
    View,
    MaterializedView,
    Column,
    Index,
    Constraint,
    Sequence,
    Function,
    Procedure,
    Collection,
    Keyspace,
    Key,
    Stream,
    Other,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorCatalogNodeV3 {
    pub id: String,
    pub parent_id: Option<String>,
    pub kind: ConnectorCatalogNodeKindV3,
    pub name: String,
    pub namespace: Option<String>,
    pub has_children: bool,
    pub columns: Vec<ConnectorColumnV2>,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorCatalogPageV3 {
    pub nodes: Vec<ConnectorCatalogNodeV3>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorKeyValueV3 {
    pub key: ConnectorValueV2,
    pub value: ConnectorValueV2,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ConnectorResultBatchV3 {
    Rows { rows: Vec<Vec<ConnectorValueV2>> },
    Documents { documents: Vec<Value> },
    KeyValues { entries: Vec<ConnectorKeyValueV3> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ConnectorResultEventV3 {
    Schema {
        columns: Vec<ConnectorColumnV2>,
    },
    Batch {
        batch: ConnectorResultBatchV3,
    },
    Progress {
        items_processed: u64,
    },
    Notice {
        notice: ConnectorNoticeV2,
    },
    Complete {
        command_tag: String,
        affected_items: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorErrorCategoryV3 {
    Authentication,
    Authorization,
    InvalidInput,
    NotFound,
    Conflict,
    ResourceLimit,
    Unsupported,
    Cancelled,
    Timeout,
    Unavailable,
    Vendor,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorErrorV3 {
    pub category: ConnectorErrorCategoryV3,
    pub code: Option<String>,
    pub sql_state: Option<String>,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<usize>,
    pub retryable: bool,
}

impl ConnectorErrorV3 {
    #[must_use]
    pub fn from_db_error(error: &DbError, kind: ConnectorKindV3) -> Self {
        Self {
            category: category_from_sql_state(&error.sql_state),
            code: None,
            sql_state: (kind == ConnectorKindV3::Sql).then(|| error.sql_state.clone()),
            message: error.message.clone(),
            detail: error.detail.as_deref().map(str::to_owned),
            hint: error.hint.as_deref().map(str::to_owned),
            position: error.position,
            retryable: error.sql_state.starts_with("08")
                || matches!(error.sql_state.as_str(), "40001" | "55P03" | "57014"),
        }
    }

    #[must_use]
    pub fn into_db_error(self) -> DbError {
        let sql_state = self
            .sql_state
            .filter(|value| valid_sql_state(value))
            .unwrap_or_else(|| category_sql_state(self.category).into());
        let mut error = DbError::new(sql_state, self.message);
        if let Some(detail) = self.detail {
            error = error.with_detail(detail);
        }
        if let Some(hint) = self.hint {
            error = error.with_hint(hint);
        }
        error.position = self.position;
        error
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ConnectorRequestV3 {
    Hello {
        hello: ProtocolHelloV3,
    },
    Connect {
        connection_id: String,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    },
    Disconnect {
        connection_id: String,
    },
    Catalog {
        request_id: String,
        connection_id: String,
        parent_id: Option<String>,
        page_size: u32,
        cursor: Option<String>,
    },
    Execute {
        request_id: String,
        connection_id: String,
        command: ConnectorCommandV3,
        batch_size: u32,
    },
    Cancel {
        request_id: String,
    },
    Begin {
        request_id: String,
        connection_id: String,
        isolation: Option<ConnectorIsolationLevelV2>,
    },
    Commit {
        request_id: String,
        connection_id: String,
    },
    Rollback {
        request_id: String,
        connection_id: String,
    },
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ConnectorResponseV3 {
    Ready {
        ready: ProtocolReadyV3,
    },
    Connected {
        connection_id: String,
        capabilities: ConnectorCapabilitiesV3,
    },
    Disconnected {
        connection_id: String,
    },
    CatalogPage {
        request_id: String,
        page: ConnectorCatalogPageV3,
    },
    ResultEvent {
        request_id: String,
        event: ConnectorResultEventV3,
    },
    Cancelled {
        request_id: String,
    },
    Transaction {
        request_id: String,
        state: ConnectorTransactionStateV2,
    },
    Error {
        request_id: Option<String>,
        error: ConnectorErrorV3,
    },
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct ConnectorResultStreamValidatorV3 {
    kind: ConnectorKindV3,
    maximum_batch_rows: u32,
    schema_width: Option<usize>,
    terminal: bool,
}

impl ConnectorResultStreamValidatorV3 {
    #[must_use]
    pub const fn new(kind: ConnectorKindV3, maximum_batch_rows: u32) -> Self {
        Self {
            kind,
            maximum_batch_rows,
            schema_width: None,
            terminal: false,
        }
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn validate(&mut self, event: &ConnectorResultEventV3) -> Result<()> {
        if self.terminal {
            return Err(protocol_error(
                "connector sent an event after the terminal result",
            ));
        }
        match event {
            ConnectorResultEventV3::Schema { columns } => {
                if self.kind != ConnectorKindV3::Sql {
                    return Err(protocol_error(
                        "only SQL connectors may send a relational schema",
                    ));
                }
                if columns.len() > MAX_CONNECTOR_COMMAND_ARGUMENTS {
                    return Err(limit_error("connector result schema is too wide"));
                }
                self.schema_width = Some(columns.len());
            }
            ConnectorResultEventV3::Batch { batch } => self.validate_batch(batch)?,
            ConnectorResultEventV3::Progress { .. } => {}
            ConnectorResultEventV3::Notice { notice } => {
                validate_bounded_text(&notice.severity, "notice severity", 128)?;
                validate_bounded_text(
                    &notice.message,
                    "notice message",
                    MAX_CONNECTOR_ERROR_TEXT_BYTES,
                )?;
                if let Some(code) = &notice.code {
                    validate_bounded_text(code, "notice code", 128)?;
                }
            }
            ConnectorResultEventV3::Complete { command_tag, .. } => {
                validate_bounded_text(command_tag, "command tag", MAX_CONNECTOR_DISPLAY_BYTES)?;
                self.terminal = true;
            }
        }
        Ok(())
    }

    fn validate_batch(&self, batch: &ConnectorResultBatchV3) -> Result<()> {
        let maximum = usize::try_from(self.maximum_batch_rows)
            .map_err(|_| limit_error("connector batch limit is invalid"))?;
        match batch {
            ConnectorResultBatchV3::Rows { rows } => {
                if self.kind != ConnectorKindV3::Sql {
                    return Err(protocol_error(
                        "non-SQL connector sent a relational row batch",
                    ));
                }
                let width = self.schema_width.ok_or_else(|| {
                    protocol_error("connector sent a row batch before its schema")
                })?;
                validate_item_count(rows.len(), maximum, "connector row batch")?;
                for row in rows {
                    if row.len() != width {
                        return Err(protocol_error(
                            "connector row width does not match its schema",
                        ));
                    }
                    for value in row {
                        validate_connector_value(value, 0)?;
                    }
                }
            }
            ConnectorResultBatchV3::Documents { documents } => {
                if self.kind != ConnectorKindV3::Document {
                    return Err(protocol_error(
                        "non-document connector sent a document batch",
                    ));
                }
                validate_item_count(documents.len(), maximum, "connector document batch")?;
                for document in documents {
                    validate_json_document(document)?;
                }
            }
            ConnectorResultBatchV3::KeyValues { entries } => {
                if self.kind != ConnectorKindV3::KeyValue {
                    return Err(protocol_error(
                        "non-key/value connector sent a key/value batch",
                    ));
                }
                validate_item_count(entries.len(), maximum, "connector key/value batch")?;
                for entry in entries {
                    validate_connector_value(&entry.key, 0)?;
                    validate_connector_value(&entry.value, 0)?;
                }
            }
        }
        Ok(())
    }
}

pub fn validate_protocol_ready_v3(
    ready: &ProtocolReadyV3,
    plugin_id: &str,
    plugin_version: &str,
) -> Result<()> {
    if ready.api_version != CONNECTOR_PROTOCOL_V3 {
        return Err(DbError::unsupported(format!(
            "connector protocol version {}",
            ready.api_version
        ))
        .with_hint("Install a connector built for this OrdaDB host."));
    }
    if ready.plugin_id != plugin_id || ready.plugin_version != plugin_version {
        return Err(protocol_error(
            "connector handshake identity does not match the installed manifest",
        ));
    }
    validate_capabilities_v3(&ready.capabilities)
}

pub fn validate_capabilities_v3(capabilities: &ConnectorCapabilitiesV3) -> Result<()> {
    if !capabilities.batch_query
        || capabilities.maximum_batch_rows == 0
        || capabilities.maximum_batch_rows > MAX_CONNECTOR_BATCH_ROWS
    {
        return Err(protocol_error(format!(
            "connector maximum batch rows must be between 1 and {MAX_CONNECTOR_BATCH_ROWS}"
        )));
    }
    if capabilities.maximum_catalog_page_size == 0
        || capabilities.maximum_catalog_page_size > MAX_CONNECTOR_CATALOG_PAGE_NODES
    {
        return Err(protocol_error(format!(
            "connector Catalog page size must be between 1 and {MAX_CONNECTOR_CATALOG_PAGE_NODES}"
        )));
    }
    if capabilities.savepoints && !capabilities.transactions {
        return Err(protocol_error(
            "connector savepoints require transaction capability",
        ));
    }
    let tls_modes = capabilities
        .tls_modes
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if tls_modes.len() != capabilities.tls_modes.len() {
        return Err(protocol_error(
            "connector capabilities contain duplicate TLS modes",
        ));
    }
    if capabilities.command_languages.is_empty()
        || capabilities.command_languages.len() > MAX_CONNECTOR_LANGUAGES
    {
        return Err(protocol_error(format!(
            "connector must advertise 1-{MAX_CONNECTOR_LANGUAGES} command languages"
        )));
    }
    let mut language_ids = BTreeSet::new();
    for language in &capabilities.command_languages {
        validate_language_id(&language.id)?;
        validate_bounded_text(&language.display_name, "command language display name", 128)?;
        if !language_ids.insert(language.id.as_str()) {
            return Err(protocol_error(
                "connector capabilities contain duplicate command language IDs",
            ));
        }
        let modes = language
            .input_modes
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if modes.is_empty() || modes.len() != language.input_modes.len() {
            return Err(protocol_error(
                "connector command language input modes must be non-empty and unique",
            ));
        }
        let valid = match capabilities.kind {
            ConnectorKindV3::Sql => {
                modes.len() == 1 && modes.contains(&ConnectorCommandInputModeV3::Text)
            }
            ConnectorKindV3::Document => {
                modes.contains(&ConnectorCommandInputModeV3::Document)
                    && !modes.contains(&ConnectorCommandInputModeV3::Arguments)
            }
            ConnectorKindV3::KeyValue => {
                modes.contains(&ConnectorCommandInputModeV3::Arguments)
                    && !modes.contains(&ConnectorCommandInputModeV3::Document)
            }
        };
        if !valid {
            return Err(protocol_error(
                "connector command language input modes do not match its kind",
            ));
        }
    }
    Ok(())
}

pub fn validate_capability_subset_v3(
    advertised: &ConnectorCapabilitiesV3,
    session: &ConnectorCapabilitiesV3,
) -> Result<()> {
    validate_capabilities_v3(advertised)?;
    validate_capabilities_v3(session)?;
    if advertised.kind != session.kind
        || advertised.command_languages != session.command_languages
        || advertised.tls_modes != session.tls_modes
    {
        return Err(protocol_error(
            "connector session identity capabilities differ from the handshake",
        ));
    }
    if session.maximum_batch_rows > advertised.maximum_batch_rows
        || session.maximum_catalog_page_size > advertised.maximum_catalog_page_size
    {
        return Err(protocol_error(
            "connector session resource limits exceed the handshake",
        ));
    }
    for (advertised_value, session_value, name) in [
        (advertised.catalog, session.catalog, "Catalog"),
        (
            advertised.cancellation,
            session.cancellation,
            "cancellation",
        ),
        (
            advertised.transactions,
            session.transactions,
            "transactions",
        ),
        (advertised.savepoints, session.savepoints, "savepoints"),
        (advertised.batch_query, session.batch_query, "batch query"),
    ] {
        if session_value && !advertised_value {
            return Err(protocol_error(format!(
                "connector session enables {name} beyond the handshake",
            )));
        }
    }
    Ok(())
}

pub fn validate_command_v3(
    command: &ConnectorCommandV3,
    capabilities: &ConnectorCapabilitiesV3,
) -> Result<()> {
    let language = capabilities
        .command_languages
        .iter()
        .find(|language| language.id == command.language_id())
        .ok_or_else(|| DbError::unsupported("connector command language"))?;
    if !language.input_modes.contains(&command.input_mode()) {
        return Err(DbError::unsupported(format!(
            "connector command input mode {:?}",
            command.input_mode()
        )));
    }
    match command {
        ConnectorCommandV3::Text { text, params, .. } => {
            if text.trim().is_empty() || text.len() > MAX_CONNECTOR_TEXT_BYTES {
                return Err(invalid("connector command text is empty or exceeds 1 MiB"));
            }
            validate_item_count(
                params.len(),
                MAX_CONNECTOR_COMMAND_ARGUMENTS,
                "connector parameter list",
            )?;
            for parameter in params {
                validate_connector_value(&parameter.value, 0)?;
            }
        }
        ConnectorCommandV3::Document { document, .. } => validate_json_document(document)?,
        ConnectorCommandV3::Arguments { arguments, .. } => {
            if arguments.is_empty() {
                return Err(invalid("connector argument command must not be empty"));
            }
            validate_item_count(
                arguments.len(),
                MAX_CONNECTOR_COMMAND_ARGUMENTS,
                "connector argument command",
            )?;
            for argument in arguments {
                validate_connector_value(argument, 0)?;
            }
        }
    }
    Ok(())
}

pub fn validate_catalog_request_v3(
    parent_id: Option<&str>,
    page_size: u32,
    cursor: Option<&str>,
    capabilities: &ConnectorCapabilitiesV3,
) -> Result<()> {
    if !capabilities.catalog {
        return Err(DbError::unsupported("connector Catalog discovery"));
    }
    if page_size == 0 || page_size > capabilities.maximum_catalog_page_size {
        return Err(invalid(
            "connector Catalog page size is outside its capability",
        ));
    }
    if let Some(parent_id) = parent_id {
        validate_opaque_id(parent_id, "Catalog parent ID")?;
    }
    if let Some(cursor) = cursor {
        validate_bounded_text(cursor, "Catalog cursor", MAX_CONNECTOR_CURSOR_BYTES)?;
    }
    Ok(())
}

pub fn validate_catalog_page_v3(page: &ConnectorCatalogPageV3, maximum_nodes: u32) -> Result<()> {
    let maximum = usize::try_from(maximum_nodes)
        .map_err(|_| limit_error("connector Catalog page limit is invalid"))?;
    validate_item_count(page.nodes.len(), maximum, "connector Catalog page")?;
    if let Some(cursor) = &page.next_cursor {
        validate_bounded_text(cursor, "Catalog cursor", MAX_CONNECTOR_CURSOR_BYTES)?;
    }
    let mut ids = BTreeSet::new();
    for node in &page.nodes {
        validate_opaque_id(&node.id, "Catalog node ID")?;
        validate_bounded_text(&node.name, "Catalog node name", MAX_CONNECTOR_DISPLAY_BYTES)?;
        if !ids.insert(node.id.as_str()) {
            return Err(protocol_error(
                "connector Catalog page contains duplicate node IDs",
            ));
        }
        if node.parent_id.as_deref() == Some(node.id.as_str()) {
            return Err(protocol_error(
                "connector Catalog node cannot be its own parent",
            ));
        }
        if let Some(parent_id) = &node.parent_id {
            validate_opaque_id(parent_id, "Catalog parent ID")?;
        }
        if let Some(namespace) = &node.namespace {
            validate_bounded_text(namespace, "Catalog namespace", MAX_CONNECTOR_DISPLAY_BYTES)?;
        }
        validate_item_count(
            node.columns.len(),
            MAX_CONNECTOR_COMMAND_ARGUMENTS,
            "connector Catalog columns",
        )?;
        for column in &node.columns {
            validate_bounded_text(
                &column.name,
                "connector Catalog column name",
                MAX_CONNECTOR_DISPLAY_BYTES,
            )?;
        }
        if node.attributes.len() > MAX_CONNECTOR_ATTRIBUTES {
            return Err(limit_error(
                "connector Catalog node has too many attributes",
            ));
        }
        let mut attribute_bytes = 0_usize;
        for (key, value) in &node.attributes {
            validate_language_id(key)?;
            if value.chars().any(char::is_control) {
                return Err(protocol_error(
                    "connector Catalog attribute contains control characters",
                ));
            }
            attribute_bytes = attribute_bytes
                .checked_add(key.len())
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or_else(|| limit_error("connector Catalog attributes are too large"))?;
        }
        if attribute_bytes > MAX_CONNECTOR_ATTRIBUTE_BYTES {
            return Err(limit_error("connector Catalog attributes are too large"));
        }
    }
    Ok(())
}

pub fn validate_error_v3(error: &ConnectorErrorV3, kind: ConnectorKindV3) -> Result<()> {
    validate_bounded_text(
        &error.message,
        "connector error message",
        MAX_CONNECTOR_ERROR_TEXT_BYTES,
    )?;
    if let Some(code) = &error.code {
        validate_bounded_text(code, "connector error code", 128)?;
    }
    for (name, value) in [
        ("connector error detail", error.detail.as_deref()),
        ("connector error hint", error.hint.as_deref()),
    ] {
        if let Some(value) = value {
            validate_bounded_text(value, name, MAX_CONNECTOR_ERROR_TEXT_BYTES)?;
        }
    }
    match (&error.sql_state, kind) {
        (Some(sql_state), _) if !valid_sql_state(sql_state) => {
            Err(protocol_error("connector SQLSTATE is invalid"))
        }
        (None, ConnectorKindV3::Sql) => Err(protocol_error(
            "SQL connector errors must include a SQLSTATE",
        )),
        _ => Ok(()),
    }
}

pub async fn read_connector_frame_v3<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    crate::protocol::read_connector_frame(reader).await
}

pub async fn write_connector_frame_v3<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    crate::protocol::write_connector_frame(writer, value).await
}

fn validate_json_document(value: &Value) -> Result<()> {
    if !value.is_object() {
        return Err(invalid("connector document command must be a JSON object"));
    }
    validate_json_value(value, 0)?;
    let bytes = serde_json::to_vec(value)
        .map_err(|error| protocol_error(format!("connector JSON is invalid: {error}")))?;
    if bytes.len() > MAX_CONNECTOR_TEXT_BYTES {
        return Err(limit_error("connector JSON document exceeds 1 MiB"));
    }
    Ok(())
}

fn validate_json_value(value: &Value, depth: usize) -> Result<()> {
    if depth > MAX_CONNECTOR_JSON_DEPTH {
        return Err(limit_error("connector JSON nesting exceeds 64 levels"));
    }
    match value {
        Value::Array(values) => {
            validate_item_count(
                values.len(),
                MAX_CONNECTOR_COMMAND_ARGUMENTS,
                "connector JSON array",
            )?;
            for value in values {
                validate_json_value(value, depth + 1)?;
            }
        }
        Value::Object(values) => {
            validate_item_count(
                values.len(),
                MAX_CONNECTOR_COMMAND_ARGUMENTS,
                "connector JSON object",
            )?;
            for (key, value) in values {
                validate_bounded_text(key, "connector JSON key", MAX_CONNECTOR_IDENTIFIER_BYTES)?;
                validate_json_value(value, depth + 1)?;
            }
        }
        Value::String(value) => {
            if value.len() > MAX_CONNECTOR_TEXT_BYTES {
                return Err(limit_error("connector JSON string exceeds 1 MiB"));
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn validate_connector_value(value: &ConnectorValueV2, depth: usize) -> Result<()> {
    if depth > MAX_CONNECTOR_JSON_DEPTH {
        return Err(limit_error("connector value nesting exceeds 64 levels"));
    }
    match value {
        ConnectorValueV2::Array(values) => {
            validate_item_count(
                values.len(),
                MAX_CONNECTOR_COMMAND_ARGUMENTS,
                "connector value array",
            )?;
            for value in values {
                validate_connector_value(value, depth + 1)?;
            }
        }
        ConnectorValueV2::Json(value) => validate_json_value(value, depth + 1)?,
        ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Text(value)
        | ConnectorValueV2::Binary(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::Uuid(value) => {
            if value.len() > MAX_CONNECTOR_TEXT_BYTES {
                return Err(limit_error("connector value text exceeds 1 MiB"));
            }
        }
        ConnectorValueV2::Null
        | ConnectorValueV2::Boolean(_)
        | ConnectorValueV2::SignedInteger(_)
        | ConnectorValueV2::UnsignedInteger(_) => {}
        ConnectorValueV2::FloatingPoint(value) if !value.is_finite() => {
            return Err(invalid("connector floating-point value must be finite"));
        }
        ConnectorValueV2::FloatingPoint(_) => {}
    }
    Ok(())
}

fn validate_item_count(actual: usize, maximum: usize, name: &str) -> Result<()> {
    if actual > maximum {
        return Err(limit_error(format!(
            "{name} contains {actual} items; maximum is {maximum}"
        )));
    }
    Ok(())
}
