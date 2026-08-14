impl DbmsRuntime {
    pub fn new(plugin_manager: Arc<PluginManager>) -> Result<Arc<Self>, DbError> {
        Self::new_with_credentials(plugin_manager, DatabaseCredentialStore::open()?)
    }

    fn new_with_credentials(
        plugin_manager: Arc<PluginManager>,
        credentials: DatabaseCredentialStore,
    ) -> Result<Arc<Self>, DbError> {
        let http = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| {
                DbError::new("58030", "failed to build administration client")
                    .with_detail(error.to_string())
            })?;
        Ok(Arc::new(Self {
            credentials,
            plugin_manager,
            connections: RwLock::new(BTreeMap::new()),
            requests: RwLock::new(BTreeMap::new()),
            bootstrap_tickets: Mutex::new(BTreeMap::new()),
            http,
        }))
    }

    async fn prompt_and_store_credential(
        &self,
        request: PromptCredentialRequest,
    ) -> Result<Option<CredentialSaved>, DbError> {
        validate_id(&request.credential_id, "credential ID")?;
        if !VALID_CONNECTOR_IDS.contains(&request.connector_id.as_str()) {
            return Err(invalid("unknown connector ID"));
        }
        validate_text(
            &request.suggested_username,
            1,
            256,
            "suggested credential username",
        )?;
        let prompted =
            prompt_database_credential(request.connector_id, request.suggested_username, false)
                .await?;
        let Some(prompted) = prompted else {
            return Ok(None);
        };
        validate_text(&prompted.username, 1, 256, "credential username")?;
        validate_text(prompted.password.as_str(), 1, 1_024, "credential password")?;
        self.credentials.store(
            &request.credential_id,
            &prompted.username,
            &prompted.password,
        )?;
        Ok(Some(CredentialSaved {
            credential_id: request.credential_id,
        }))
    }

    fn connection(&self, connection_id: &str) -> Result<Arc<ConnectionHandle>, DbError> {
        validate_id(connection_id, "connection ID")?;
        read_lock(&self.connections)?
            .get(connection_id)
            .cloned()
            .ok_or_else(|| DbError::new("08003", "database connection does not exist"))
    }

    pub(crate) fn ai_connection_policy(
        &self,
        connection_id: &str,
    ) -> Result<AiConnectionPolicy, DbError> {
        let connection = self.connection(connection_id)?;
        Ok(AiConnectionPolicy {
            connector_kind: connection.connector_kind.clone(),
            command_language: connection.command_language.clone(),
            credential_access: connection.credential_access,
            native: matches!(&connection.transport, ConnectionTransport::Native(_)),
        })
    }

    async fn connect(&self, request: ConnectRequest) -> Result<ConnectionSnapshot, DbError> {
        validate_connect_request(&request)?;
        let stored = self.credentials.load(&request.credential_id)?;
        let connection_id = Uuid::new_v4().to_string();
        let database = request.database.clone().unwrap_or_else(|| {
            if request.connector_id == NATIVE_CONNECTOR_ID {
                "ordadb".to_owned()
            } else {
                String::new()
            }
        });

        let (transport, mode, capabilities) = if request.connector_id == NATIVE_CONNECTOR_ID {
            let address: SocketAddr = request.endpoint.parse().map_err(|_| {
                DbError::new(
                    "22023",
                    "native OrdaDB endpoint must be an IP socket address",
                )
            })?;
            let admin_endpoint =
                validate_admin_endpoint(request.admin_endpoint.as_deref().ok_or_else(|| {
                    DbError::new(
                        "22023",
                        "native OrdaDB connection requires an administration endpoint",
                    )
                })?)?;
            let username = stored.username.to_string();
            let pg_password = Zeroizing::new(stored.password.to_string());
            let pg_database = database.clone();
            let pg = tokio::task::spawn_blocking(move || {
                PgClient::connect(ClientConfig {
                    address,
                    user: username,
                    database: pg_database,
                    password: pg_password,
                    application_name: "OrdaDB Console".into(),
                    query_memory_bytes: None,
                    timeout: None,
                })
            })
            .await
            .map_err(join_error)??;
            let cancel = pg.cancellation_token();
            let bearer = issue_admin_token(
                &self.http,
                &admin_endpoint,
                &stored.username,
                stored.password.as_str(),
            )
            .await?;
            let admin = AdminSession {
                base_url: admin_endpoint,
                bearer,
            };
            let health = admin_get::<Health>(&self.http, &admin, "/v1/health/ready", false).await?;
            if health.bootstrap_required || health.status != "ready" {
                return Err(DbError::new(
                    "55000",
                    "OrdaDB service is not ready for authenticated connections",
                ));
            }
            if health.version.is_empty() {
                return Err(DbError::new(
                    "08P01",
                    "OrdaDB health response has no server version",
                ));
            }
            let _uptime_seconds = health.uptime_seconds;
            (
                ConnectionTransport::Native(NativeConnection {
                    pg: Arc::new(Mutex::new(pg)),
                    cancel,
                    admin,
                    address,
                    database: database.clone(),
                    credential_id: request.credential_id.clone(),
                }),
                "native",
                DbmsCapabilities::native(),
            )
        } else {
            let mut host =
                ConnectorHost::launch(&self.plugin_manager, &request.connector_id).await?;
            let protocol_version = host.protocol_version();
            let username = stored.username.to_string();
            let secret = stored.password.to_string();
            let (capabilities_v3, capabilities) = match protocol_version {
                CONNECTOR_PROTOCOL_V3 => {
                    let endpoint =
                        host.structured_endpoint(&request.endpoint, request.database.clone())?;
                    let negotiated = host
                        .connect_v3(
                            connection_id.clone(),
                            endpoint,
                            request.tls_mode,
                            Some(ConnectorCredentialV2::new(
                                Some(username.to_string()),
                                secret,
                            )),
                        )
                        .await?;
                    validate_negotiated_v3(&request, &negotiated)?;
                    let desktop = DbmsCapabilities::plugin_v3(&negotiated);
                    (Some(negotiated), desktop)
                }
                CONNECTOR_PROTOCOL_V2 => {
                    let endpoint =
                        host.structured_endpoint(&request.endpoint, request.database.clone())?;
                    let negotiated = host
                        .connect_v2(
                            connection_id.clone(),
                            endpoint,
                            request.tls_mode,
                            Some(ConnectorCredentialV2::new(
                                Some(username.to_string()),
                                secret,
                            )),
                        )
                        .await?;
                    (None, DbmsCapabilities::plugin_v2(&negotiated))
                }
                _ => {
                    if request.connector_kind != "sql" {
                        return Err(DbError::unsupported(
                            "non-SQL connectors require connector protocol v3",
                        ));
                    }
                    host.connect(
                        connection_id.clone(),
                        request.endpoint.clone(),
                        request.database.clone(),
                        CredentialPayload::new(username.to_string(), secret),
                    )
                    .await?;
                    (None, DbmsCapabilities::plugin())
                }
            };
            (
                ConnectionTransport::Plugin(Box::new(PluginConnection {
                    host: AsyncMutex::new(Some(host)),
                    capabilities_v3,
                })),
                "plugin",
                capabilities,
            )
        };

        let connector_kind = request.connector_kind.clone();
        let command_language = request.command_language.clone();
        let snapshot = ConnectionSnapshot {
            connection_id: connection_id.clone(),
            connector_id: request.connector_id,
            connector_kind: connector_kind.clone(),
            command_language: command_language.clone(),
            dialect: request.dialect,
            endpoint: request.endpoint,
            database,
            credential_access: request.credential_access,
            mode,
            capabilities,
        };
        let handle = Arc::new(ConnectionHandle {
            connector_kind,
            command_language,
            credential_access: request.credential_access,
            transport,
        });
        write_lock(&self.connections)?.insert(connection_id, handle);
        Ok(snapshot)
    }

    async fn probe_connection(&self, request: ConnectRequest) -> ConnectionProbe {
        let mut probe = ConnectionProbe::new();
        if let Err(error) = validate_connect_request(&request) {
            probe.failed(ConnectionProbeStageName::Service, error);
            for stage in [
                ConnectionProbeStageName::PgPort,
                ConnectionProbeStageName::AdminApi,
                ConnectionProbeStageName::Initialization,
                ConnectionProbeStageName::Authentication,
                ConnectionProbeStageName::Catalog,
            ] {
                probe.skipped(stage);
            }
            probe.finish();
            return probe;
        }
        if request.connector_id != NATIVE_CONNECTOR_ID {
            for stage in [
                ConnectionProbeStageName::Service,
                ConnectionProbeStageName::PgPort,
                ConnectionProbeStageName::AdminApi,
                ConnectionProbeStageName::Initialization,
                ConnectionProbeStageName::Authentication,
                ConnectionProbeStageName::Catalog,
            ] {
                probe.skipped(stage);
            }
            probe.ready = true;
            return probe;
        }

        let service_identity =
            match tauri::async_runtime::spawn_blocking(probe_windows_service).await {
                Ok(Ok(identity)) => {
                    probe.passed(ConnectionProbeStageName::Service);
                    Some(identity)
                }
                Ok(Err(error)) => {
                    probe.failed(ConnectionProbeStageName::Service, error);
                    None
                }
                Err(error) => {
                    probe.failed(
                        ConnectionProbeStageName::Service,
                        task_error("Windows service probe task failed", error),
                    );
                    None
                }
            };

        let address = match request.endpoint.parse::<SocketAddr>() {
            Ok(address) => {
                let socket_address = address;
                let reachable = match tauri::async_runtime::spawn_blocking(move || {
                    TcpStream::connect_timeout(&socket_address, Duration::from_secs(2))
                })
                .await
                {
                    Ok(Ok(_)) => {
                        probe.passed(ConnectionProbeStageName::PgPort);
                        true
                    }
                    Ok(Err(error)) => {
                        probe.failed(
                            ConnectionProbeStageName::PgPort,
                            network_error("PostgreSQL port is not reachable", error),
                        );
                        false
                    }
                    Err(error) => {
                        probe.failed(
                            ConnectionProbeStageName::PgPort,
                            task_error("PostgreSQL port probe task failed", error),
                        );
                        false
                    }
                };
                reachable.then_some(address)
            }
            Err(_) => {
                probe.failed(
                    ConnectionProbeStageName::PgPort,
                    invalid("native OrdaDB endpoint must be an IP socket address"),
                );
                None
            }
        };

        let admin_endpoint = match request
            .admin_endpoint
            .as_deref()
            .ok_or_else(|| invalid("native OrdaDB connection requires an administration endpoint"))
            .and_then(validate_admin_endpoint)
        {
            Ok(endpoint) => Some(endpoint),
            Err(error) => {
                probe.failed(ConnectionProbeStageName::AdminApi, error);
                probe.skipped(ConnectionProbeStageName::Initialization);
                None
            }
        };

        if let Some(endpoint) = &admin_endpoint {
            let public = AdminSession {
                base_url: endpoint.clone(),
                bearer: Zeroizing::new(String::new()),
            };
            match admin_get::<Health>(&self.http, &public, "/v1/health/live", false).await {
                Ok(health) if !health.status.is_empty() => {
                    probe.passed(ConnectionProbeStageName::AdminApi)
                }
                Ok(_) => probe.failed(
                    ConnectionProbeStageName::AdminApi,
                    DbError::new("08P01", "OrdaDB live health response is invalid"),
                ),
                Err(error) => probe.failed(ConnectionProbeStageName::AdminApi, error),
            }

            match admin_get::<Health>(&self.http, &public, "/v1/health/ready", false).await {
                Ok(health) if health.bootstrap_required => {
                    if let Some(identity) = &service_identity {
                        match self.issue_bootstrap_ticket(&request, identity) {
                            Ok(ticket) => {
                                probe.bootstrap_ticket = Some(ticket);
                                probe.failed(
                                    ConnectionProbeStageName::Initialization,
                                    DbError::new(
                                        "55000",
                                        "OrdaDB requires its first administrator",
                                    )
                                    .with_hint(
                                        "complete the local administrator setup, then retry",
                                    ),
                                );
                            }
                            Err(error) => {
                                probe.failed(ConnectionProbeStageName::Initialization, error);
                            }
                        }
                    } else {
                        probe.failed(
                            ConnectionProbeStageName::Initialization,
                            DbError::new(
                                "55000",
                                "OrdaDB bootstrap requires a verified local service",
                            ),
                        );
                    }
                }
                Ok(health) if health.status == "ready" && !health.version.is_empty() => {
                    probe.passed(ConnectionProbeStageName::Initialization)
                }
                Ok(_) => probe.failed(
                    ConnectionProbeStageName::Initialization,
                    DbError::new("55000", "OrdaDB service is not ready"),
                ),
                Err(error) => probe.failed(ConnectionProbeStageName::Initialization, error),
            }
        }

        let can_authenticate = address.is_some()
            && admin_endpoint.is_some()
            && probe.stages.iter().any(|stage| {
                stage.stage == ConnectionProbeStageName::AdminApi
                    && stage.status == ConnectionProbeStageStatus::Passed
            })
            && probe.stages.iter().any(|stage| {
                stage.stage == ConnectionProbeStageName::Initialization
                    && stage.status == ConnectionProbeStageStatus::Passed
            });
        let stored = if can_authenticate {
            match self.credentials.load(&request.credential_id) {
                Ok(stored) => Some(stored),
                Err(error) => {
                    probe.failed(ConnectionProbeStageName::Authentication, error);
                    None
                }
            }
        } else {
            probe.skipped(ConnectionProbeStageName::Authentication);
            None
        };
        let mut bearer = None;
        if let (Some(address), Some(endpoint), Some(stored)) =
            (address, admin_endpoint.as_deref(), stored)
        {
            let username = stored.username.to_string();
            let password = Zeroizing::new(stored.password.to_string());
            let database = request
                .database
                .clone()
                .unwrap_or_else(|| "ordadb".to_owned());
            let pg_password = Zeroizing::new(stored.password.to_string());
            let pg_result = tauri::async_runtime::spawn_blocking(move || {
                PgClient::connect(ClientConfig {
                    address,
                    user: username,
                    database,
                    password: pg_password,
                    application_name: "OrdaDB Console Probe".into(),
                    query_memory_bytes: None,
                    timeout: None,
                })
            })
            .await;
            match pg_result {
                Ok(Ok(_)) => {
                    match issue_admin_token(&self.http, endpoint, &stored.username, &password).await
                    {
                        Ok(token) => {
                            bearer = Some(token);
                            probe.passed(ConnectionProbeStageName::Authentication);
                        }
                        Err(error) => probe.failed(ConnectionProbeStageName::Authentication, error),
                    }
                }
                Ok(Err(error)) => probe.failed(ConnectionProbeStageName::Authentication, error),
                Err(error) => probe.failed(
                    ConnectionProbeStageName::Authentication,
                    task_error("database authentication probe task failed", error),
                ),
            }
        } else if !probe
            .stages
            .iter()
            .any(|stage| stage.stage == ConnectionProbeStageName::Authentication)
        {
            probe.skipped(ConnectionProbeStageName::Authentication);
        }

        if let (Some(endpoint), Some(bearer)) = (admin_endpoint, bearer) {
            let session = AdminSession {
                base_url: endpoint,
                bearer,
            };
            match admin_get::<JsonValue>(&self.http, &session, "/v1/catalog", true).await {
                Ok(_) => probe.passed(ConnectionProbeStageName::Catalog),
                Err(error) => probe.failed(ConnectionProbeStageName::Catalog, error),
            }
        } else {
            probe.skipped(ConnectionProbeStageName::Catalog);
        }
        probe.finish();
        probe
    }

    fn issue_bootstrap_ticket(
        &self,
        request: &ConnectRequest,
        service: &ServiceIdentity,
    ) -> Result<LocalBootstrapTicket, DbError> {
        let token = Uuid::new_v4().to_string();
        let now = Instant::now();
        let mut tickets = mutex_lock(&self.bootstrap_tickets)?;
        tickets.retain(|_, record| record.expires_at > now);
        if tickets.len() >= MAX_BOOTSTRAP_TICKETS
            && let Some(oldest) = tickets
                .iter()
                .min_by_key(|(_, record)| record.expires_at)
                .map(|(ticket, _)| ticket.clone())
        {
            tickets.remove(&oldest);
        }
        tickets.insert(
            token.clone(),
            BootstrapTicketRecord {
                expires_at: now + BOOTSTRAP_TICKET_TTL,
                request_fingerprint: connection_fingerprint(request),
                service: service.clone(),
            },
        );
        Ok(LocalBootstrapTicket {
            ticket: token,
            expires_in_ms: BOOTSTRAP_TICKET_TTL.as_millis() as u64,
        })
    }

    fn consume_bootstrap_ticket(
        &self,
        request: &BootstrapAdminRequest,
    ) -> Result<BootstrapTicketRecord, DbError> {
        validate_id(&request.ticket, "local bootstrap ticket")?;
        let record = mutex_lock(&self.bootstrap_tickets)?
            .remove(&request.ticket)
            .ok_or_else(|| {
                DbError::new(
                    "55000",
                    "local bootstrap ticket is invalid or already consumed",
                )
                .with_hint("run the local OrdaDB connection probe again")
            })?;
        if record.expires_at <= Instant::now() {
            return Err(DbError::new("55000", "local bootstrap ticket expired")
                .with_hint("run the local OrdaDB connection probe again"));
        }
        if record.request_fingerprint != connection_fingerprint(&request.connection) {
            return Err(DbError::new(
                "28000",
                "local bootstrap ticket does not match this connection",
            ));
        }
        Ok(record)
    }

    async fn bootstrap_admin(
        &self,
        request: BootstrapAdminRequest,
    ) -> Result<BootstrapAdminResult, DbError> {
        validate_connect_request(&request.connection)?;
        if request.connection.connector_id != NATIVE_CONNECTOR_ID {
            return Err(invalid(
                "administrator bootstrap is available only for native OrdaDB",
            ));
        }
        validate_text(
            &request.suggested_username,
            1,
            128,
            "suggested administrator username",
        )?;
        let prompted = prompt_database_credential(
            NATIVE_CONNECTOR_ID.to_owned(),
            request.suggested_username.clone(),
            true,
        )
        .await?
        .ok_or_else(|| DbError::new("57014", "administrator credential prompt was cancelled"))?;
        validate_text(&prompted.username, 1, 128, "administrator username")?;
        validate_text(
            prompted.password.as_str(),
            8,
            1_024,
            "administrator password",
        )?;
        let record = self.consume_bootstrap_ticket(&request)?;
        let current = tauri::async_runtime::spawn_blocking(probe_windows_service)
            .await
            .map_err(|error| task_error("Windows service identity check failed", error))??;
        if current != record.service {
            return Err(
                DbError::new("55000", "OrdaDB service changed after the bootstrap probe")
                    .with_hint("run the local OrdaDB connection probe again"),
            );
        }
        let response = ordadb_server::request_bootstrap(
            &record.service.pipe_name,
            prompted.username.clone(),
            Zeroizing::new(prompted.password.to_string()),
        )
        .await?;
        if response.success {
            self.credentials.store(
                &request.connection.credential_id,
                &prompted.username,
                &prompted.password,
            )?;
        }
        Ok(BootstrapAdminResult {
            success: response.success,
            user: response.user,
            error: response.error.map(Into::into),
        })
    }

    async fn disconnect(&self, connection_id: &str) -> Result<(), DbError> {
        validate_id(connection_id, "connection ID")?;
        let requests = read_lock(&self.requests)?
            .values()
            .filter(|request| request.connection_id == connection_id)
            .map(|request| request.cancellation.clone())
            .collect::<Vec<_>>();
        for cancellation in requests {
            cancel_request(cancellation).await?;
        }
        let connection = write_lock(&self.connections)?
            .remove(connection_id)
            .ok_or_else(|| DbError::new("08003", "database connection does not exist"))?;
        if let ConnectionTransport::Plugin(plugin) = &connection.transport
            && let Some(host) = plugin.host.lock().await.take()
        {
            host.shutdown().await?;
        }
        Ok(())
    }

    pub(crate) async fn catalog(&self, connection_id: &str) -> Result<CatalogSnapshot, DbError> {
        let connection = self.connection(connection_id)?;
        let objects = match &connection.transport {
            ConnectionTransport::Native(native) => {
                let projection: JsonValue =
                    admin_get(&self.http, &native.admin, "/v1/catalog", true).await?;
                flatten_catalog(&projection)?
            }
            ConnectionTransport::Plugin(plugin) => {
                let capabilities_v3 = plugin.capabilities_v3.clone();
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                if let Some(capabilities) = capabilities_v3 {
                    connector_catalog_v3(host, connection_id, &capabilities).await?
                } else {
                    let request_id = Uuid::new_v4().to_string();
                    host.send(&ConnectorRequestV1::Catalog {
                        request_id: request_id.clone(),
                        connection_id: connection_id.to_owned(),
                    })
                    .await?;
                    match host.receive().await? {
                        ConnectorResponseV1::Catalog {
                            request_id: actual,
                            entries,
                        } if actual == request_id => {
                            entries.into_iter().map(catalog_entry).collect()
                        }
                        ConnectorResponseV1::Error { error, .. } => return Err(error),
                        _ => {
                            return Err(DbError::new(
                                "08P01",
                                "connector returned an unexpected catalog response",
                            ));
                        }
                    }
                }
            }
        };
        Ok(CatalogSnapshot {
            connection_id: connection_id.to_owned(),
            objects,
        })
    }

    fn start_execute(
        self: &Arc<Self>,
        app: AppHandle,
        request: ExecuteRequest,
    ) -> Result<OperationStarted, DbError> {
        validate_execute_request(&request)?;
        let connection = self.connection(&request.connection_id)?;
        validate_command_for_connection(
            &request.command,
            &connection.connector_kind,
            &connection.command_language,
        )?;
        let request_id = Uuid::new_v4().to_string();
        let cancellation = match &connection.transport {
            ConnectionTransport::Native(native) => {
                RequestCancellation::Native(native.cancel.clone())
            }
            ConnectionTransport::Plugin(_) => RequestCancellation::Plugin(CancellationToken::new()),
        };
        write_lock(&self.requests)?.insert(
            request_id.clone(),
            ActiveRequest {
                connection_id: request.connection_id.clone(),
                cancellation: cancellation.clone(),
            },
        );
        let runtime = Arc::clone(self);
        let task_request_id = request_id.clone();
        tauri::async_runtime::spawn(async move {
            let result = runtime
                .run_execute(&app, &task_request_id, connection, request, cancellation)
                .await;
            if let Err(error) = result {
                emit_query(
                    &app,
                    &task_request_id,
                    DbmsQueryEvent::Error {
                        error: error.into(),
                    },
                );
            }
            if let Ok(mut requests) = runtime.requests.write() {
                requests.remove(&task_request_id);
            }
        });
        Ok(OperationStarted { request_id })
    }
}
