use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use mongodb::{
    Client, ClientSession,
    bson::{Document, doc},
    error::{Error as MongoError, ErrorKind as MongoErrorKind},
    options::{ClientOptions, Credential, ServerAddress, Tls, TlsOptions},
};
use ordadb_connector_sdk::{
    ConnectorCapabilitiesV3, ConnectorCatalogNodeKindV3, ConnectorCatalogNodeV3,
    ConnectorCatalogPageV3, ConnectorCommandInputModeV3, ConnectorCommandLanguageV3,
    ConnectorCommandV3, ConnectorCredentialV2, ConnectorDriverV3, ConnectorEndpointV2,
    ConnectorEventSinkV3, ConnectorIsolationLevelV2, ConnectorKindV3, ConnectorResultBatchV3,
    ConnectorResultEventV3, ConnectorSessionV3, ConnectorTlsModeV2, connector_pipe_argument,
    run_named_pipe_helper_v3,
};
use ordadb_types::{DbError, Result};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tokio_util::sync::CancellationToken;

const PLUGIN_ID: &str = "mongodb";
const LANGUAGE_ID: &str = "mongodb-json";
const MAX_CATALOG_ITEMS: usize = 10_000;
const MAX_RESULT_ITEMS: u32 = 100_000;
const DEFAULT_RESULT_ITEMS: u32 = 10_000;
const MAX_PIPELINE_STAGES: usize = 128;
const MAX_NAME_BYTES: usize = 200;
const SERVER_NODE_ID: &str = "mongodb:server";

#[derive(Debug, Default)]
struct MongoDbDriver;

struct MongoDbSession {
    client: Client,
    default_database: Option<String>,
    topology: String,
    capabilities: ConnectorCapabilitiesV3,
    transaction: Option<ClientSession>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "camelCase", deny_unknown_fields)]
enum MongoCommand {
    Find {
        database: Option<String>,
        collection: String,
        #[serde(default = "empty_json_object")]
        filter: JsonValue,
        projection: Option<JsonValue>,
        sort: Option<JsonValue>,
        skip: Option<u64>,
        limit: Option<u32>,
    },
    Aggregate {
        database: Option<String>,
        collection: String,
        pipeline: Vec<JsonValue>,
        limit: Option<u32>,
    },
    Command {
        database: Option<String>,
        command: JsonValue,
    },
}

#[tokio::main]
async fn main() {
    let result = async {
        let pipe = connector_pipe_argument()?;
        run_named_pipe_helper_v3(&pipe, PLUGIN_ID, env!("CARGO_PKG_VERSION"), MongoDbDriver).await
    }
    .await;
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[async_trait]
impl ConnectorDriverV3 for MongoDbDriver {
    fn capabilities(&self) -> ConnectorCapabilitiesV3 {
        capabilities(true)
    }

    async fn connect(
        &self,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<Box<dyn ConnectorSessionV3>> {
        let ConnectorEndpointV2::Network {
            host,
            port,
            database,
            instance,
            options,
        } = endpoint
        else {
            return Err(invalid("MongoDB requires a network endpoint"));
        };
        if instance.is_some() {
            return Err(invalid("MongoDB endpoints do not accept an instance name"));
        }
        validate_name(&host, "MongoDB host")?;
        if let Some(database) = &database {
            validate_name(database, "MongoDB database")?;
        }

        let endpoint_options = MongoEndpointOptions::parse(options, database.as_deref())?;
        let mut client_options = ClientOptions::builder()
            .hosts(vec![ServerAddress::Tcp {
                host,
                port: Some(port),
            }])
            .build();
        client_options.app_name = Some("OrdaDB MongoDB Connector".into());
        client_options.connect_timeout = Some(Duration::from_secs(10));
        client_options.server_selection_timeout = Some(Duration::from_secs(10));
        client_options.direct_connection = endpoint_options.direct_connection;
        client_options.repl_set_name = endpoint_options.replica_set;
        client_options.tls = Some(mongodb_tls(tls_mode)?);
        client_options.credential = credential
            .map(|credential| mongodb_credential(credential, endpoint_options.auth_source))
            .transpose()?;

        let client = Client::with_options(client_options).map_err(mongodb_error)?;
        let hello = client
            .database("admin")
            .run_command(doc! { "hello": 1 })
            .await
            .map_err(mongodb_error)?;
        let transactions = transaction_supported(&hello);
        Ok(Box::new(MongoDbSession {
            client,
            default_database: database,
            topology: topology_name(&hello),
            capabilities: capabilities(transactions),
            transaction: None,
        }))
    }
}

#[async_trait]
impl ConnectorSessionV3 for MongoDbSession {
    fn capabilities(&self) -> &ConnectorCapabilitiesV3 {
        &self.capabilities
    }

    async fn catalog_page(
        &mut self,
        parent_id: Option<&str>,
        page_size: u32,
        cursor: Option<&str>,
    ) -> Result<ConnectorCatalogPageV3> {
        let offset = parse_cursor(cursor)?;
        let nodes = match parent_id {
            None => vec![ConnectorCatalogNodeV3 {
                id: SERVER_NODE_ID.into(),
                parent_id: None,
                kind: ConnectorCatalogNodeKindV3::Server,
                name: "MongoDB".into(),
                namespace: None,
                has_children: true,
                columns: Vec::new(),
                attributes: BTreeMap::from([("topology".into(), self.topology.clone())]),
            }],
            Some(SERVER_NODE_ID) => {
                let mut names = self
                    .client
                    .list_database_names()
                    .await
                    .map_err(mongodb_error)?;
                bounded_sorted_names(&mut names, "MongoDB databases")?;
                names
                    .into_iter()
                    .map(|name| ConnectorCatalogNodeV3 {
                        id: database_node_id(&name),
                        parent_id: Some(SERVER_NODE_ID.into()),
                        kind: ConnectorCatalogNodeKindV3::Database,
                        name: name.clone(),
                        namespace: Some(name),
                        has_children: true,
                        columns: Vec::new(),
                        attributes: BTreeMap::new(),
                    })
                    .collect()
            }
            Some(parent) if parent.starts_with("mongodb:database:") => {
                let database = decode_database_node(parent)?;
                let mut names = self
                    .client
                    .database(&database)
                    .list_collection_names()
                    .await
                    .map_err(mongodb_error)?;
                bounded_sorted_names(&mut names, "MongoDB collections")?;
                names
                    .into_iter()
                    .map(|name| ConnectorCatalogNodeV3 {
                        id: collection_node_id(&database, &name),
                        parent_id: Some(parent.into()),
                        kind: ConnectorCatalogNodeKindV3::Collection,
                        name: name.clone(),
                        namespace: Some(database.clone()),
                        has_children: true,
                        columns: Vec::new(),
                        attributes: BTreeMap::new(),
                    })
                    .collect()
            }
            Some(parent) if parent.starts_with("mongodb:collection:") => {
                let (database, collection) = decode_collection_node(parent)?;
                let mut names = self
                    .client
                    .database(&database)
                    .collection::<Document>(&collection)
                    .list_index_names()
                    .await
                    .map_err(mongodb_error)?;
                bounded_sorted_names(&mut names, "MongoDB indexes")?;
                names
                    .into_iter()
                    .map(|name| ConnectorCatalogNodeV3 {
                        id: index_node_id(&database, &collection, &name),
                        parent_id: Some(parent.into()),
                        kind: ConnectorCatalogNodeKindV3::Index,
                        name,
                        namespace: Some(format!("{database}.{collection}")),
                        has_children: false,
                        columns: Vec::new(),
                        attributes: BTreeMap::new(),
                    })
                    .collect()
            }
            Some(_) => return Err(invalid("unknown MongoDB Catalog parent")),
        };
        paginate_nodes(nodes, offset, page_size)
    }

    async fn execute(
        &mut self,
        _request_id: &str,
        command: &ConnectorCommandV3,
        batch_size: u32,
        cancellation: &CancellationToken,
        sink: &mut dyn ConnectorEventSinkV3,
    ) -> Result<()> {
        let ConnectorCommandV3::Document {
            language_id,
            document,
        } = command
        else {
            return Err(DbError::unsupported("non-document MongoDB commands"));
        };
        if language_id != LANGUAGE_ID {
            return Err(DbError::unsupported(format!(
                "MongoDB command language {language_id}",
            )));
        }
        let command = parse_command(document)?;
        match command {
            MongoCommand::Find {
                database,
                collection,
                filter,
                projection,
                sort,
                skip,
                limit,
            } => {
                let database = self.database_name(database.as_deref())?;
                validate_name(&collection, "MongoDB collection")?;
                let filter = json_document(&filter, "MongoDB find filter")?;
                let projection = projection
                    .as_ref()
                    .map(|value| json_document(value, "MongoDB projection"))
                    .transpose()?;
                let sort = sort
                    .as_ref()
                    .map(|value| json_document(value, "MongoDB sort"))
                    .transpose()?;
                let limit = result_limit(limit)?;
                let collection = self
                    .client
                    .database(&database)
                    .collection::<Document>(&collection);
                let mut find = collection
                    .find(filter)
                    .batch_size(batch_size)
                    .limit(i64::from(limit));
                if let Some(projection) = projection {
                    find = find.projection(projection);
                }
                if let Some(sort) = sort {
                    find = find.sort(sort);
                }
                if let Some(skip) = skip {
                    find = find.skip(skip);
                }
                if let Some(session) = self.transaction.as_mut() {
                    let mut cursor = find.session(&mut *session).await.map_err(mongodb_error)?;
                    let mut stream = cursor.stream(&mut *session);
                    stream_documents(&mut stream, batch_size, cancellation, sink, "FIND").await
                } else {
                    let mut cursor = find.await.map_err(mongodb_error)?;
                    stream_documents(&mut cursor, batch_size, cancellation, sink, "FIND").await
                }
            }
            MongoCommand::Aggregate {
                database,
                collection,
                pipeline,
                limit,
            } => {
                let database = self.database_name(database.as_deref())?;
                validate_name(&collection, "MongoDB collection")?;
                if pipeline.len() > MAX_PIPELINE_STAGES {
                    return Err(resource(format!(
                        "MongoDB aggregate exceeds {MAX_PIPELINE_STAGES} stages",
                    )));
                }
                let mut pipeline = pipeline
                    .iter()
                    .map(|stage| json_document(stage, "MongoDB aggregate stage"))
                    .collect::<Result<Vec<_>>>()?;
                pipeline.push(doc! { "$limit": i64::from(result_limit(limit)?) });
                let collection = self
                    .client
                    .database(&database)
                    .collection::<Document>(&collection);
                let aggregate = collection.aggregate(pipeline).batch_size(batch_size);
                if let Some(session) = self.transaction.as_mut() {
                    let mut cursor = aggregate
                        .session(&mut *session)
                        .await
                        .map_err(mongodb_error)?;
                    let mut stream = cursor.stream(&mut *session);
                    stream_documents(&mut stream, batch_size, cancellation, sink, "AGGREGATE").await
                } else {
                    let mut cursor = aggregate.await.map_err(mongodb_error)?;
                    stream_documents(&mut cursor, batch_size, cancellation, sink, "AGGREGATE").await
                }
            }
            MongoCommand::Command { database, command } => {
                let database = self.database_name(database.as_deref())?;
                let command = json_document(&command, "MongoDB command")?;
                let database = self.client.database(&database);
                let result = if let Some(session) = self.transaction.as_mut() {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return Err(cancelled()),
                        result = database.run_command(command).session(&mut *session) => {
                            result.map_err(mongodb_error)?
                        }
                    }
                } else {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return Err(cancelled()),
                        result = database.run_command(command) => result.map_err(mongodb_error)?,
                    }
                };
                let document = json_document_value(result)?;
                sink.send(ConnectorResultEventV3::Batch {
                    batch: ConnectorResultBatchV3::Documents {
                        documents: vec![document],
                    },
                })
                .await?;
                sink.send(ConnectorResultEventV3::Progress { items_processed: 1 })
                    .await?;
                sink.send(ConnectorResultEventV3::Complete {
                    command_tag: "COMMAND".into(),
                    affected_items: Some(1),
                })
                .await
            }
        }
    }

    async fn cancel(&mut self, _request_id: &str) -> Result<()> {
        Ok(())
    }

    async fn begin(&mut self, isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
        if isolation.is_some() {
            return Err(DbError::unsupported(
                "SQL isolation levels for MongoDB transactions",
            ));
        }
        if !self.capabilities.transactions {
            return Err(DbError::unsupported(
                "MongoDB transactions on this deployment topology",
            ));
        }
        if self.transaction.is_some() {
            return Err(DbError::new(
                "25001",
                "MongoDB transaction is already active",
            ));
        }
        let mut session = self.client.start_session().await.map_err(mongodb_error)?;
        session.start_transaction().await.map_err(mongodb_error)?;
        self.transaction = Some(session);
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        let session = self
            .transaction
            .as_mut()
            .ok_or_else(|| DbError::new("25P01", "MongoDB transaction is not active"))?;
        session.commit_transaction().await.map_err(mongodb_error)?;
        self.transaction = None;
        Ok(())
    }

    async fn rollback(&mut self) -> Result<()> {
        let session = self
            .transaction
            .as_mut()
            .ok_or_else(|| DbError::new("25P01", "MongoDB transaction is not active"))?;
        session.abort_transaction().await.map_err(mongodb_error)?;
        self.transaction = None;
        Ok(())
    }
}

impl MongoDbSession {
    fn database_name(&self, requested: Option<&str>) -> Result<String> {
        let database = requested
            .map(str::to_owned)
            .or_else(|| self.default_database.clone())
            .ok_or_else(|| invalid("MongoDB command requires a database"))?;
        validate_name(&database, "MongoDB database")?;
        Ok(database)
    }
}

struct MongoEndpointOptions {
    direct_connection: Option<bool>,
    replica_set: Option<String>,
    auth_source: Option<String>,
}

impl MongoEndpointOptions {
    fn parse(mut options: BTreeMap<String, String>, database: Option<&str>) -> Result<Self> {
        let direct_connection = options
            .remove("directConnection")
            .map(|value| parse_bool(&value, "MongoDB directConnection"))
            .transpose()?;
        let replica_set = options.remove("replicaSet");
        let auth_source = options
            .remove("authSource")
            .or_else(|| database.map(str::to_owned));
        if let Some(replica_set) = &replica_set {
            validate_name(replica_set, "MongoDB replica set")?;
        }
        if let Some(auth_source) = &auth_source {
            validate_name(auth_source, "MongoDB authentication database")?;
        }
        if let Some(name) = options.keys().next() {
            return Err(DbError::unsupported(format!(
                "MongoDB endpoint option {name}",
            )));
        }
        Ok(Self {
            direct_connection,
            replica_set,
            auth_source,
        })
    }
}

fn capabilities(transactions: bool) -> ConnectorCapabilitiesV3 {
    ConnectorCapabilitiesV3 {
        kind: ConnectorKindV3::Document,
        command_languages: vec![ConnectorCommandLanguageV3 {
            id: LANGUAGE_ID.into(),
            display_name: "MongoDB JSON".into(),
            input_modes: vec![ConnectorCommandInputModeV3::Document],
        }],
        catalog: true,
        cancellation: true,
        transactions,
        savepoints: false,
        batch_query: true,
        maximum_batch_rows: 1_024,
        maximum_catalog_page_size: 512,
        tls_modes: vec![
            ConnectorTlsModeV2::Disable,
            ConnectorTlsModeV2::Require,
            ConnectorTlsModeV2::VerifyFull,
        ],
    }
}

fn mongodb_tls(mode: ConnectorTlsModeV2) -> Result<Tls> {
    match mode {
        ConnectorTlsModeV2::Disable => Ok(Tls::Disabled),
        ConnectorTlsModeV2::Require | ConnectorTlsModeV2::VerifyFull => {
            Ok(Tls::Enabled(TlsOptions::default()))
        }
        ConnectorTlsModeV2::Prefer => Err(DbError::unsupported(
            "MongoDB TLS prefer mode because fail-open TLS is forbidden",
        )),
        ConnectorTlsModeV2::VerifyCa => Err(DbError::unsupported(
            "MongoDB CA-only TLS with the bundled rustls transport",
        )),
    }
}

fn mongodb_credential(
    credential: ConnectorCredentialV2,
    source: Option<String>,
) -> Result<Credential> {
    let username = credential
        .username
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| DbError::new("28000", "MongoDB username is required"))?;
    Ok(Credential::builder()
        .username(username)
        .password(credential.secret.to_string())
        .source(source.unwrap_or_else(|| "admin".into()))
        .build())
}

async fn stream_documents<S>(
    stream: &mut S,
    batch_size: u32,
    cancellation: &CancellationToken,
    sink: &mut dyn ConnectorEventSinkV3,
    command_tag: &str,
) -> Result<()>
where
    S: Stream<Item = std::result::Result<Document, MongoError>> + Unpin + Send,
{
    let batch_size = usize::try_from(batch_size).unwrap_or(1_024);
    let mut batch = Vec::with_capacity(batch_size);
    let mut processed = 0_u64;
    loop {
        let next = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(cancelled()),
            next = stream.next() => next,
        };
        let Some(document) = next else {
            break;
        };
        batch.push(json_document_value(document.map_err(mongodb_error)?)?);
        processed = processed.saturating_add(1);
        if batch.len() == batch_size {
            send_document_batch(sink, &mut batch).await?;
            sink.send(ConnectorResultEventV3::Progress {
                items_processed: processed,
            })
            .await?;
        }
    }
    if !batch.is_empty() {
        send_document_batch(sink, &mut batch).await?;
    }
    sink.send(ConnectorResultEventV3::Progress {
        items_processed: processed,
    })
    .await?;
    sink.send(ConnectorResultEventV3::Complete {
        command_tag: command_tag.into(),
        affected_items: Some(processed),
    })
    .await
}

async fn send_document_batch(
    sink: &mut dyn ConnectorEventSinkV3,
    batch: &mut Vec<JsonValue>,
) -> Result<()> {
    sink.send(ConnectorResultEventV3::Batch {
        batch: ConnectorResultBatchV3::Documents {
            documents: std::mem::take(batch),
        },
    })
    .await
}

fn parse_command(value: &JsonValue) -> Result<MongoCommand> {
    let command: MongoCommand = serde_json::from_value(value.clone())
        .map_err(|error| invalid("invalid MongoDB JSON command").with_detail(error.to_string()))?;
    match &command {
        MongoCommand::Find {
            collection, limit, ..
        }
        | MongoCommand::Aggregate {
            collection, limit, ..
        } => {
            validate_name(collection, "MongoDB collection")?;
            result_limit(*limit)?;
        }
        MongoCommand::Command { .. } => {}
    }
    Ok(command)
}

fn json_document(value: &JsonValue, context: &str) -> Result<Document> {
    mongodb::bson::to_document(value).map_err(|error| {
        invalid(format!("{context} must be a BSON-compatible object"))
            .with_detail(error.to_string())
    })
}

fn json_document_value(document: Document) -> Result<JsonValue> {
    serde_json::to_value(document).map_err(|error| {
        DbError::internal("failed to encode MongoDB extended JSON").with_detail(error.to_string())
    })
}

fn result_limit(limit: Option<u32>) -> Result<u32> {
    let limit = limit.unwrap_or(DEFAULT_RESULT_ITEMS);
    if limit == 0 || limit > MAX_RESULT_ITEMS {
        return Err(resource(format!(
            "MongoDB result limit must be between 1 and {MAX_RESULT_ITEMS}",
        )));
    }
    Ok(limit)
}

fn transaction_supported(hello: &Document) -> bool {
    hello.contains_key("logicalSessionTimeoutMinutes")
        && (hello.contains_key("setName") || hello.get_str("msg") == Ok("isdbgrid"))
}

fn topology_name(hello: &Document) -> String {
    if hello.get_str("msg") == Ok("isdbgrid") {
        "sharded".into()
    } else if hello.contains_key("setName") {
        "replicaSet".into()
    } else {
        "standalone".into()
    }
}

fn database_node_id(database: &str) -> String {
    format!("mongodb:database:{}", encode_component(database))
}

fn collection_node_id(database: &str, collection: &str) -> String {
    format!(
        "mongodb:collection:{}:{}",
        encode_component(database),
        encode_component(collection),
    )
}

fn index_node_id(database: &str, collection: &str, index: &str) -> String {
    format!(
        "mongodb:index:{}:{}:{}",
        encode_component(database),
        encode_component(collection),
        encode_component(index),
    )
}

fn decode_database_node(value: &str) -> Result<String> {
    decode_component(
        value
            .strip_prefix("mongodb:database:")
            .ok_or_else(|| invalid("invalid MongoDB database Catalog ID"))?,
    )
}

fn decode_collection_node(value: &str) -> Result<(String, String)> {
    let encoded = value
        .strip_prefix("mongodb:collection:")
        .ok_or_else(|| invalid("invalid MongoDB collection Catalog ID"))?;
    let (database, collection) = encoded
        .split_once(':')
        .ok_or_else(|| invalid("invalid MongoDB collection Catalog ID"))?;
    Ok((decode_component(database)?, decode_component(collection)?))
}

fn encode_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value.as_bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_component(value: &str) -> Result<String> {
    if value.len() % 2 != 0 || value.len() > MAX_NAME_BYTES.saturating_mul(2) {
        return Err(invalid("invalid MongoDB Catalog ID component"));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        bytes.push((high << 4) | low);
    }
    String::from_utf8(bytes).map_err(|_| invalid("MongoDB Catalog ID is not valid UTF-8"))
}

fn hex_value(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(invalid("MongoDB Catalog ID is not lowercase hexadecimal")),
    }
}

fn paginate_nodes(
    nodes: Vec<ConnectorCatalogNodeV3>,
    offset: usize,
    page_size: u32,
) -> Result<ConnectorCatalogPageV3> {
    if offset > nodes.len() {
        return Err(invalid("MongoDB Catalog cursor is outside the result set"));
    }
    let page_size = usize::try_from(page_size).unwrap_or(512);
    let end = offset.saturating_add(page_size).min(nodes.len());
    let next_cursor = (end < nodes.len()).then(|| end.to_string());
    Ok(ConnectorCatalogPageV3 {
        nodes: nodes.into_iter().skip(offset).take(page_size).collect(),
        next_cursor,
    })
}

fn parse_cursor(cursor: Option<&str>) -> Result<usize> {
    cursor
        .map(|value| {
            if value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid("invalid MongoDB Catalog cursor"));
            }
            value
                .parse::<usize>()
                .map_err(|_| invalid("invalid MongoDB Catalog cursor"))
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn bounded_sorted_names(names: &mut Vec<String>, context: &str) -> Result<()> {
    if names.len() > MAX_CATALOG_ITEMS {
        return Err(resource(format!(
            "{context} exceed the {MAX_CATALOG_ITEMS}-item bound",
        )));
    }
    for name in names.iter() {
        validate_name(name, context)?;
    }
    names.sort();
    names.dedup();
    Ok(())
}

fn validate_name(value: &str, context: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > MAX_NAME_BYTES || value.contains('\0') {
        return Err(invalid(format!(
            "{context} must contain 1-{MAX_NAME_BYTES} bytes without NUL",
        )));
    }
    Ok(())
}

fn parse_bool(value: &str, context: &str) -> Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid(format!("{context} must be true or false"))),
    }
}

fn mongodb_error(error: MongoError) -> DbError {
    let (sql_state, message) = match error.kind.as_ref() {
        MongoErrorKind::Authentication { .. } => ("28000", "MongoDB authentication failed"),
        MongoErrorKind::InvalidArgument { .. }
        | MongoErrorKind::BsonDeserialization(_)
        | MongoErrorKind::BsonSerialization(_) => ("22023", "MongoDB input is invalid"),
        MongoErrorKind::ServerSelection { .. } => ("08001", "MongoDB server is unavailable"),
        MongoErrorKind::Io(_) | MongoErrorKind::ConnectionPoolCleared { .. } => {
            ("08006", "MongoDB connection failed")
        }
        MongoErrorKind::SessionsNotSupported => ("0A000", "MongoDB sessions are unavailable"),
        MongoErrorKind::Command(command) if command.code == 13 => {
            ("42501", "MongoDB operation is not authorized")
        }
        MongoErrorKind::Command(command) if command.code == 26 => {
            ("42P01", "MongoDB namespace does not exist")
        }
        MongoErrorKind::Command(_) | MongoErrorKind::Write(_) => {
            ("HV000", "MongoDB vendor operation failed")
        }
        _ => ("58000", "MongoDB connector operation failed"),
    };
    DbError::new(sql_state, message).with_detail(bounded_detail(&error.to_string()))
}

fn bounded_detail(value: &str) -> String {
    value.chars().take(4_096).collect()
}

fn empty_json_object() -> JsonValue {
    json!({})
}

fn cancelled() -> DbError {
    DbError::new("57014", "MongoDB operation was cancelled")
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn resource(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

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
