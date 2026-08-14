
fn validate_transaction_state(
    action: TransactionAction,
    state: ConnectorTransactionStateV2,
) -> Result<(), DbError> {
    let valid = matches!(
        (action, state),
        (
            TransactionAction::Begin,
            ConnectorTransactionStateV2::Active
        ) | (
            TransactionAction::Commit | TransactionAction::Rollback,
            ConnectorTransactionStateV2::Idle
        )
    );
    if valid {
        Ok(())
    } else if state == ConnectorTransactionStateV2::Failed {
        Err(DbError::new(
            "25P02",
            "connector transaction entered the failed state",
        ))
    } else {
        Err(DbError::new(
            "08P01",
            "connector returned an unexpected transaction state",
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Health {
    status: String,
    version: String,
    uptime_seconds: u64,
    bootstrap_required: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TokenRequest<'a> {
    username: &'a str,
    password: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointResult {
    completed: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiEnvelope<T> {
    api_version: String,
    data: T,
}

#[derive(Deserialize)]
struct ApiErrorEnvelope {
    error: DbError,
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_prompt_credential(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: PromptCredentialRequest,
) -> DesktopResult<Option<CredentialSaved>> {
    runtime
        .prompt_and_store_credential(request)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn dbms_delete_credential(
    runtime: State<'_, Arc<DbmsRuntime>>,
    credential_id: String,
) -> DesktopResult<()> {
    runtime
        .credentials
        .delete(&credential_id)
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_connect(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: ConnectRequest,
) -> DesktopResult<ConnectionSnapshot> {
    runtime.connect(request).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_probe_connection(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: ConnectRequest,
) -> DesktopResult<ConnectionProbe> {
    Ok(runtime.probe_connection(request).await)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_bootstrap_admin(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: BootstrapAdminRequest,
) -> DesktopResult<BootstrapAdminResult> {
    runtime.bootstrap_admin(request).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_disconnect(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<()> {
    runtime.disconnect(&connection_id).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_catalog(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<CatalogSnapshot> {
    runtime.catalog(&connection_id).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn dbms_execute(
    app: AppHandle,
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: ExecuteRequest,
) -> DesktopResult<OperationStarted> {
    runtime
        .inner()
        .start_execute(app, request)
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_cancel(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request_id: String,
) -> DesktopResult<()> {
    runtime.cancel(&request_id).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_begin(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<CommandResult> {
    runtime
        .transaction(&connection_id, TransactionAction::Begin)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_commit(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<CommandResult> {
    runtime
        .transaction(&connection_id, TransactionAction::Commit)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_rollback(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<CommandResult> {
    runtime
        .transaction(&connection_id, TransactionAction::Rollback)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_monitor(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<MonitorSnapshot> {
    runtime.monitor(&connection_id).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_checkpoint(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<EngineStatus> {
    runtime.checkpoint(&connection_id).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_operations(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<Vec<AdministrationOperation>> {
    runtime
        .administration_operations(&connection_id)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_start_operation(
    runtime: State<'_, Arc<DbmsRuntime>>,
    request: StartAdministrationOperationRequest,
) -> DesktopResult<AdministrationOperation> {
    runtime
        .start_administration_operation(request)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_operation(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
    operation_id: String,
) -> DesktopResult<AdministrationOperation> {
    runtime
        .administration_operation(&connection_id, &operation_id)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_cancel_operation(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
    operation_id: String,
) -> DesktopResult<AdministrationOperation> {
    runtime
        .cancel_administration_operation(&connection_id, &operation_id)
        .await
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn dbms_service(
    runtime: State<'_, Arc<DbmsRuntime>>,
    connection_id: String,
) -> DesktopResult<AdministrationServiceStatus> {
    runtime
        .administration_service(&connection_id)
        .await
        .map_err(Into::into)
}

#[derive(Debug, Clone, Copy)]
struct ConnectorContract {
    kind: &'static str,
    command_language: &'static str,
    dialect: Option<&'static str>,
}

fn connector_contract(connector_id: &str) -> Option<ConnectorContract> {
    let contract = match connector_id {
        NATIVE_CONNECTOR_ID | "postgresql" => ConnectorContract {
            kind: "sql",
            command_language: "postgresql-sql",
            dialect: Some("postgresql"),
        },
        "mysql" => ConnectorContract {
            kind: "sql",
            command_language: "mysql-sql",
            dialect: Some("mysql"),
        },
        "sqlite" => ConnectorContract {
            kind: "sql",
            command_language: "sqlite-sql",
            dialect: Some("sqlite"),
        },
        "sql-server" => ConnectorContract {
            kind: "sql",
            command_language: "sql-server-sql",
            dialect: Some("sqlServer"),
        },
        "mongodb" => ConnectorContract {
            kind: "document",
            command_language: "mongodb-json",
            dialect: None,
        },
        "redis" => ConnectorContract {
            kind: "keyValue",
            command_language: "redis-resp3",
            dialect: None,
        },
        "mariadb" => ConnectorContract {
            kind: "sql",
            command_language: "mariadb-sql",
            dialect: Some("mariadb"),
        },
        "clickhouse" => ConnectorContract {
            kind: "sql",
            command_language: "clickhouse-sql",
            dialect: Some("clickhouse"),
        },
        "oracle" => ConnectorContract {
            kind: "sql",
            command_language: "oracle-sql",
            dialect: Some("oracle"),
        },
        _ => return None,
    };
    Some(contract)
}

fn validate_connect_request(request: &ConnectRequest) -> Result<(), DbError> {
    let contract = connector_contract(&request.connector_id)
        .ok_or_else(|| DbError::new("22023", "unknown connector ID"))?;
    if request.connector_kind != contract.kind
        || request.command_language != contract.command_language
        || request.dialect.as_deref() != contract.dialect
    {
        return Err(DbError::new(
            "22023",
            "connection metadata does not match the connector identity",
        ));
    }
    validate_text(&request.endpoint, 1, 2_048, "connection endpoint")?;
    validate_text(
        &request.command_language,
        1,
        64,
        "connector command language",
    )?;
    validate_id(&request.credential_id, "credential ID")?;
    if let Some(database) = &request.database {
        validate_text(database, 1, 256, "database name")?;
    }
    if let Some(admin_endpoint) = &request.admin_endpoint {
        validate_text(admin_endpoint, 1, 2_048, "administration endpoint")?;
    }
    if request.connector_id == NATIVE_CONNECTOR_ID
        && (request.admin_endpoint.is_none() || request.tls_mode != ConnectorTlsModeV2::Disable)
    {
        return Err(DbError::new(
            "22023",
            "native OrdaDB requires its administration endpoint and local TLS mode",
        ));
    }
    if request.connector_id != NATIVE_CONNECTOR_ID && request.admin_endpoint.is_some() {
        return Err(DbError::new(
            "22023",
            "external connectors do not accept an OrdaDB administration endpoint",
        ));
    }
    Ok(())
}

fn validate_negotiated_v3(
    request: &ConnectRequest,
    capabilities: &ConnectorCapabilitiesV3,
) -> Result<(), DbError> {
    let expected_kind = match request.connector_kind.as_str() {
        "sql" => ConnectorKindV3::Sql,
        "document" => ConnectorKindV3::Document,
        "keyValue" => ConnectorKindV3::KeyValue,
        _ => return Err(DbError::new("22023", "unknown connector kind")),
    };
    if capabilities.kind != expected_kind {
        return Err(DbError::new(
            "08P01",
            "connector negotiated a different data model than its profile",
        ));
    }
    if !capabilities
        .command_languages
        .iter()
        .any(|language| language.id == request.command_language)
    {
        return Err(DbError::new(
            "08P01",
            "connector did not negotiate the configured command language",
        ));
    }
    Ok(())
}

fn validate_administration_operation_request(
    request: &StartAdministrationOperationRequest,
) -> Result<(), DbError> {
    validate_id(&request.connection_id, "connection ID")?;
    validate_operation_path(&request.path)?;
    if request.kind.requires_table() {
        let schema = request
            .schema
            .as_deref()
            .ok_or_else(|| DbError::new("22023", "table operation requires a schema"))?;
        let table = request
            .table
            .as_deref()
            .ok_or_else(|| DbError::new("22023", "table operation requires a table"))?;
        validate_text(schema, 1, 256, "schema name")?;
        validate_text(table, 1, 256, "table name")?;
        if request.format.is_none() {
            return Err(DbError::new(
                "22023",
                "table operation requires CSV or JSON Lines format",
            ));
        }
    } else if request.schema.is_some() || request.table.is_some() || request.format.is_some() {
        return Err(DbError::new(
            "22023",
            "backup and restore requests do not accept table fields",
        ));
    }
    Ok(())
}

fn validate_operation_path(value: &str) -> Result<(), DbError> {
    validate_text(value, 1, 512, "operation path")?;
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(DbError::new(
            "22023",
            "operation path must be relative to the server operations root",
        ));
    }
    Ok(())
}

fn validate_operation_id(value: &str) -> Result<Uuid, DbError> {
    value
        .parse()
        .map_err(|_| DbError::new("22023", "operation ID must be a UUID"))
}

fn validate_execute_request(request: &ExecuteRequest) -> Result<(), DbError> {
    validate_id(&request.connection_id, "connection ID")?;
    match &request.command {
        DesktopCommand::Text {
            language_id,
            text,
            params,
        } => {
            validate_text(language_id, 1, 64, "connector command language")?;
            validate_text(text, 1, 4 * 1024 * 1024, "command text")?;
            if params.len() > 65_535 {
                return Err(DbError::new("54000", "parameter count exceeds 65,535"));
            }
        }
        DesktopCommand::Document {
            language_id,
            document,
        } => {
            validate_text(language_id, 1, 64, "connector command language")?;
            let encoded = serde_json::to_vec(document)
                .map_err(|error| DbError::internal(error.to_string()))?;
            if encoded.len() > MAX_CONNECTOR_TEXT_BYTES {
                return Err(DbError::new(
                    "54000",
                    "document command exceeds the connector size limit",
                ));
            }
        }
        DesktopCommand::Arguments {
            language_id,
            arguments,
        } => {
            validate_text(language_id, 1, 64, "connector command language")?;
            if arguments.is_empty() || arguments.len() > MAX_CONNECTOR_COMMAND_ARGUMENTS {
                return Err(DbError::new(
                    "54000",
                    "argument command count is outside the connector limit",
                ));
            }
            let total_bytes = arguments.iter().try_fold(0_usize, |total, argument| {
                total
                    .checked_add(argument.len())
                    .ok_or_else(|| DbError::new("54000", "argument command size overflowed"))
            })?;
            if total_bytes > MAX_CONNECTOR_TEXT_BYTES {
                return Err(DbError::new(
                    "54000",
                    "argument command exceeds the connector size limit",
                ));
            }
        }
    }
    Ok(())
}

fn validate_command_for_connection(
    command: &DesktopCommand,
    connector_kind: &str,
    command_language: &str,
) -> Result<(), DbError> {
    let (actual_kind, actual_language) = match command {
        DesktopCommand::Text { language_id, .. } => ("sql", language_id),
        DesktopCommand::Document { language_id, .. } => ("document", language_id),
        DesktopCommand::Arguments { language_id, .. } => ("keyValue", language_id),
    };
    if actual_kind != connector_kind || actual_language != command_language {
        return Err(DbError::new(
            "22023",
            "command shape or language does not match the active connection",
        ));
    }
    Ok(())
}

fn validate_id(value: &str, context: &str) -> Result<(), DbError> {
    if value.is_empty()
        || value.len() > 128
        || value.starts_with('.')
        || value.ends_with('.')
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(DbError::new(
            "22023",
            format!(
                "{context} must use 1-128 ASCII letters, digits, dots, hyphens, or underscores"
            ),
        ));
    }
    Ok(())
}

fn validate_text(
    value: &str,
    minimum: usize,
    maximum: usize,
    context: &str,
) -> Result<(), DbError> {
    if !(minimum..=maximum).contains(&value.len())
        || value.contains('\0')
        || value.chars().any(char::is_control)
    {
        return Err(DbError::new(
            "22023",
            format!("{context} must contain {minimum}-{maximum} printable UTF-8 bytes"),
        ));
    }
    Ok(())
}

fn validate_admin_endpoint(value: &str) -> Result<String, DbError> {
    validate_text(value, 1, 2_048, "administration endpoint")?;
    let mut url = Url::parse(value).map_err(|error| {
        DbError::new("22023", "administration endpoint is invalid").with_detail(error.to_string())
    })?;
    if url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DbError::new(
            "22023",
            "administration endpoint must not contain credentials, query, or fragment",
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| DbError::new("22023", "administration endpoint has no host"))?;
    if url.scheme() == "http" && !is_loopback_host(host) {
        return Err(DbError::new(
            "22023",
            "remote administration endpoints require HTTPS",
        ));
    }
    if !matches!(url.scheme(), "http" | "https") {
        return Err(DbError::new(
            "22023",
            "administration endpoint must use HTTP or HTTPS",
        ));
    }
    url.set_path("");
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

async fn issue_admin_token(
    client: &Client,
    base_url: &str,
    username: &str,
    password: &str,
) -> Result<Zeroizing<String>, DbError> {
    let url = format!("{base_url}/v1/auth/token");
    let response = client
        .post(url)
        .json(&TokenRequest { username, password })
        .send()
        .await
        .map_err(|error| network_error("administration authentication failed", error))?;
    let envelope: ApiEnvelope<TokenResponse> = decode_admin_response(response).await?;
    validate_api_version(&envelope.api_version)?;
    if envelope.data.token_type != "Bearer" || envelope.data.expires_in_seconds == 0 {
        return Err(DbError::new(
            "08P01",
            "administration token response is invalid",
        ));
    }
    Ok(Zeroizing::new(envelope.data.access_token))
}

async fn admin_get<T: DeserializeOwned>(
    client: &Client,
    session: &AdminSession,
    path: &str,
    authenticated: bool,
) -> Result<T, DbError> {
    admin_request(client, session, Method::GET, path, authenticated).await
}

async fn admin_post<T: DeserializeOwned>(
    client: &Client,
    session: &AdminSession,
    path: &str,
) -> Result<T, DbError> {
    admin_request(client, session, Method::POST, path, true).await
}

async fn admin_post_json<T: DeserializeOwned>(
    client: &Client,
    session: &AdminSession,
    path: &str,
    body: &JsonValue,
) -> Result<T, DbError> {
    let response = client
        .post(format!("{}{}", session.base_url, path))
        .bearer_auth(session.bearer.as_str())
        .json(body)
        .send()
        .await
        .map_err(|error| network_error("administration request failed", error))?;
    let envelope: ApiEnvelope<T> = decode_admin_response(response).await?;
    validate_api_version(&envelope.api_version)?;
    Ok(envelope.data)
}

async fn admin_request<T: DeserializeOwned>(
    client: &Client,
    session: &AdminSession,
    method: Method,
    path: &str,
    authenticated: bool,
) -> Result<T, DbError> {
    let mut request = client.request(method, format!("{}{}", session.base_url, path));
    if authenticated {
        request = request.bearer_auth(session.bearer.as_str());
    }
    let response = request
        .send()
        .await
        .map_err(|error| network_error("administration request failed", error))?;
    let envelope: ApiEnvelope<T> = decode_admin_response(response).await?;
    validate_api_version(&envelope.api_version)?;
    Ok(envelope.data)
}

async fn decode_admin_response<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, DbError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_ADMIN_RESPONSE_BYTES)
    {
        return Err(DbError::new(
            "54000",
            "administration response exceeds 8 MiB",
        ));
    }
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|error| network_error("failed to read administration response", error))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_ADMIN_RESPONSE_BYTES {
        return Err(DbError::new(
            "54000",
            "administration response exceeds 8 MiB",
        ));
    }
    if !status.is_success() {
        return serde_json::from_slice::<ApiErrorEnvelope>(&bytes)
            .map(|envelope| envelope.error)
            .map_err(|error| {
                DbError::new(
                    "08P01",
                    format!("administration API returned HTTP {status}"),
                )
                .with_detail(error.to_string())
            })
            .and_then(Err);
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        DbError::new("08P01", "administration response is invalid JSON")
            .with_detail(error.to_string())
    })
}

fn validate_api_version(version: &str) -> Result<(), DbError> {
    if version == "v1" {
        Ok(())
    } else {
        Err(DbError::new(
            "0A000",
            format!("administration API version {version} is unsupported"),
        ))
    }
}

async fn connector_catalog_v3(
    host: &mut ConnectorHost,
    connection_id: &str,
    capabilities: &ConnectorCapabilitiesV3,
) -> Result<Vec<CatalogObject>, DbError> {
    if !capabilities.catalog {
        return Err(DbError::unsupported("connector Catalog discovery"));
    }
    let page_size = capabilities
        .maximum_catalog_page_size
        .min(MAX_CONNECTOR_CATALOG_PAGE_NODES)
        .min(1_024);
    if page_size == 0 {
        return Err(DbError::new(
            "08P01",
            "connector negotiated a zero Catalog page size",
        ));
    }
    let mut pending_parents = VecDeque::from([None]);
    let mut seen = BTreeSet::new();
    let mut objects = Vec::new();
    while let Some(parent_id) = pending_parents.pop_front() {
        let mut cursor = None;
        loop {
            let request_id = Uuid::new_v4().to_string();
            host.send_v3(&ConnectorRequestV3::Catalog {
                request_id: request_id.clone(),
                connection_id: connection_id.to_owned(),
                parent_id: parent_id.clone(),
                page_size,
                cursor: cursor.take(),
            })
            .await?;
            let page = match host.receive_v3().await? {
                ConnectorResponseV3::CatalogPage {
                    request_id: actual,
                    page,
                } if actual == request_id => page,
                ConnectorResponseV3::Error {
                    request_id: actual,
                    error,
                } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                    return Err(error.into_db_error());
                }
                _ => {
                    return Err(DbError::new(
                        "08P01",
                        "connector returned an unexpected v3 Catalog response",
                    ));
                }
            };
            cursor = page.next_cursor;
            for node in page.nodes {
                if node.parent_id != parent_id {
                    return Err(DbError::new(
                        "08P01",
                        "connector Catalog node does not belong to the requested parent",
                    ));
                }
                if !seen.insert(node.id.clone()) {
                    return Err(DbError::new(
                        "08P01",
                        "connector Catalog contains a duplicate node ID",
                    ));
                }
                if objects.len() >= MAX_DESKTOP_CATALOG_NODES {
                    return Err(DbError::new(
                        "54000",
                        "connector Catalog exceeds the desktop node limit",
                    ));
                }
                if node.has_children {
                    pending_parents.push_back(Some(node.id.clone()));
                }
                objects.push(catalog_node_v3(node)?);
            }
            if cursor.is_none() {
                break;
            }
        }
    }
    Ok(objects)
}

fn catalog_node_v3(node: ConnectorCatalogNodeV3) -> Result<CatalogObject, DbError> {
    let details = serde_json::to_value(&node).map_err(|error| {
        DbError::internal("failed to project connector Catalog node").with_detail(error.to_string())
    })?;
    Ok(CatalogObject {
        id: Some(node.id),
        kind: catalog_node_kind_v3(node.kind).into(),
        schema: node.namespace.clone().unwrap_or_default(),
        namespace: node.namespace,
        name: node.name,
        parent: node.parent_id,
        details,
    })
}

const fn catalog_node_kind_v3(kind: ConnectorCatalogNodeKindV3) -> &'static str {
    match kind {
        ConnectorCatalogNodeKindV3::Server => "server",
        ConnectorCatalogNodeKindV3::Cluster => "cluster",
        ConnectorCatalogNodeKindV3::Database => "database",
        ConnectorCatalogNodeKindV3::Schema => "schema",
        ConnectorCatalogNodeKindV3::Table => "table",
        ConnectorCatalogNodeKindV3::View => "view",
        ConnectorCatalogNodeKindV3::MaterializedView => "materializedView",
        ConnectorCatalogNodeKindV3::Column => "column",
        ConnectorCatalogNodeKindV3::Index => "index",
        ConnectorCatalogNodeKindV3::Constraint => "constraint",
        ConnectorCatalogNodeKindV3::Sequence => "sequence",
        ConnectorCatalogNodeKindV3::Function => "function",
        ConnectorCatalogNodeKindV3::Procedure => "procedure",
        ConnectorCatalogNodeKindV3::Collection => "collection",
        ConnectorCatalogNodeKindV3::Keyspace => "keyspace",
        ConnectorCatalogNodeKindV3::Key => "key",
        ConnectorCatalogNodeKindV3::Stream => "stream",
        ConnectorCatalogNodeKindV3::Other => "other",
    }
}

fn flatten_catalog(projection: &JsonValue) -> Result<Vec<CatalogObject>, DbError> {
    let database = projection
        .get("database")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| DbError::new("08P01", "catalog response has no database"))?;
    let database_name = identifier(
        database
            .get("name")
            .ok_or_else(|| DbError::new("08P01", "catalog database has no name"))?,
    )?;
    let mut objects = vec![CatalogObject {
        id: None,
        kind: "database".into(),
        schema: String::new(),
        namespace: None,
        name: database_name.clone(),
        parent: None,
        details: JsonValue::Object(database.clone()),
    }];
    for schema in json_array(database.get("schemas"), "catalog schemas")? {
        let schema_object = schema
            .as_object()
            .ok_or_else(|| DbError::new("08P01", "catalog schema is not an object"))?;
        let schema_name = identifier(
            schema_object
                .get("name")
                .ok_or_else(|| DbError::new("08P01", "catalog schema has no name"))?,
        )?;
        objects.push(CatalogObject {
            id: None,
            kind: "schema".into(),
            schema: schema_name.clone(),
            namespace: Some(schema_name.clone()),
            name: schema_name.clone(),
            parent: Some(database_name.clone()),
            details: schema.clone(),
        });
        flatten_named(
            &mut objects,
            schema_object.get("tables"),
            "table",
            &schema_name,
            None,
        )?;
        flatten_named(
            &mut objects,
            schema_object.get("sequences"),
            "sequence",
            &schema_name,
            None,
        )?;
        flatten_views(&mut objects, schema_object.get("views"), &schema_name)?;
        flatten_named(
            &mut objects,
            schema_object.get("routines"),
            "routine",
            &schema_name,
            None,
        )?;
        for table in json_array(schema_object.get("tables"), "catalog tables")? {
            let table_name = identifier(
                table
                    .get("name")
                    .ok_or_else(|| DbError::new("08P01", "catalog table has no name"))?,
            )?;
            for (field, kind) in [
                ("indexes", "index"),
                ("constraints", "constraint"),
                ("triggers", "trigger"),
            ] {
                flatten_named(
                    &mut objects,
                    table.get(field),
                    kind,
                    &schema_name,
                    Some(&table_name),
                )?;
            }
        }
    }
    Ok(objects)
}
