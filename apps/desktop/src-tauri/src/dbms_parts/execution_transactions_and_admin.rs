impl DbmsRuntime {

    async fn run_execute(
        &self,
        app: &AppHandle,
        request_id: &str,
        connection: Arc<ConnectionHandle>,
        request: ExecuteRequest,
        cancellation: RequestCancellation,
    ) -> Result<(), DbError> {
        let started = Instant::now();
        let ExecuteRequest {
            connection_id,
            command,
        } = request;
        match &connection.transport {
            ConnectionTransport::Native(native) => {
                let DesktopCommand::Text {
                    text: sql, params, ..
                } = command
                else {
                    return Err(DbError::unsupported(
                        "native OrdaDB accepts only SQL text commands",
                    ));
                };
                let client = Arc::clone(&native.pg);
                let params = params
                    .into_iter()
                    .map(|value| value.map(String::into_bytes))
                    .collect::<Vec<_>>();
                let task_app = app.clone();
                let task_request_id = request_id.to_owned();
                tokio::task::spawn_blocking(move || {
                    let mut client = mutex_lock(&client)?;
                    let mut processed = 0_u64;
                    let mut on_event = |event| {
                        emit_native_pg_event(&task_app, &task_request_id, event, &mut processed);
                        Ok(())
                    };
                    let summary = if params.is_empty() {
                        client.query_batches(&sql, QUERY_BATCH_ROWS, &mut on_event)?
                    } else {
                        client.query_prepared_batches(
                            &sql,
                            &[],
                            &params,
                            QUERY_BATCH_ROWS as u32,
                            &mut on_event,
                        )?
                    };
                    emit_query(
                        &task_app,
                        &task_request_id,
                        DbmsQueryEvent::Complete {
                            command_tag: if summary.command_tags.is_empty() {
                                "OK".into()
                            } else {
                                summary.command_tags.join(" · ")
                            },
                            duration_ms: elapsed_ms(started.elapsed()),
                        },
                    );
                    Ok::<(), DbError>(())
                })
                .await
                .map_err(join_error)??;
            }
            ConnectionTransport::Plugin(plugin) => {
                let RequestCancellation::Plugin(token) = cancellation else {
                    return Err(DbError::internal(
                        "plugin request received a native cancellation handle",
                    ));
                };
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                if plugin.capabilities_v3.is_some() {
                    run_connector_execute_v3(
                        host,
                        app,
                        request_id,
                        connection_id,
                        desktop_command_v3(command),
                        token,
                        started,
                    )
                    .await?;
                    return Ok(());
                }
                let DesktopCommand::Text {
                    text: sql, params, ..
                } = command
                else {
                    return Err(DbError::unsupported(
                        "connector protocols v1 and v2 accept only SQL text commands",
                    ));
                };
                host.send(&ConnectorRequestV1::Execute {
                    request_id: request_id.to_owned(),
                    connection_id,
                    sql,
                    params: params
                        .into_iter()
                        .map(|value| value.map_or(Value::Null, Value::Text))
                        .collect(),
                })
                .await?;
                let mut cancel_sent = false;
                loop {
                    let response = if cancel_sent {
                        host.receive().await?
                    } else {
                        tokio::select! {
                            response = host.receive() => response?,
                            () = token.cancelled() => {
                                host.send(&ConnectorRequestV1::Cancel {
                                    request_id: request_id.to_owned(),
                                }).await?;
                                cancel_sent = true;
                                continue;
                            }
                        }
                    };
                    match response {
                        ConnectorResponseV1::QueryEvent {
                            request_id: actual,
                            event,
                        } if actual == request_id => {
                            let terminal = matches!(event, QueryEvent::Complete(_));
                            emit_query(
                                app,
                                request_id,
                                map_connector_event(event, started.elapsed()),
                            );
                            if terminal {
                                break;
                            }
                        }
                        ConnectorResponseV1::Error {
                            request_id: actual,
                            error,
                        } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                            return Err(error);
                        }
                        _ => {
                            return Err(DbError::new(
                                "08P01",
                                "connector returned an unexpected query response",
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_ai_command(
        &self,
        connection_id: &str,
        command: DesktopCommand,
        limits: AiToolLimits,
        cancellation: CancellationToken,
        isolated_read: bool,
    ) -> Result<BoundedAiQueryResult, DbError> {
        if limits.max_rows == 0
            || limits.max_rows > 1_000
            || limits.max_result_bytes == 0
            || limits.max_result_bytes > 2 * 1024 * 1024
            || limits.query_memory_bytes == 0
            || limits.query_memory_bytes > 64 * 1024 * 1024
        {
            return Err(DbError::new(
                "22023",
                "AI query limits exceed the desktop safety contract",
            ));
        }
        let connection = self.connection(connection_id)?;
        validate_command_for_connection(
            &command,
            &connection.connector_kind,
            &connection.command_language,
        )?;
        match &connection.transport {
            ConnectionTransport::Native(native) => {
                let DesktopCommand::Text {
                    text: sql, params, ..
                } = command
                else {
                    return Err(DbError::unsupported(
                        "native OrdaDB accepts only SQL text commands",
                    ));
                };
                let stored = self.credentials.load(&native.credential_id)?;
                let config = ClientConfig {
                    address: native.address,
                    user: stored.username.to_string(),
                    database: native.database.clone(),
                    password: stored.password,
                    application_name: "OrdaDB AI".to_owned(),
                    query_memory_bytes: Some(limits.query_memory_bytes),
                    timeout: Some(Duration::from_millis(limits.timeout_ms)),
                };
                let client = tokio::select! {
                    () = cancellation.cancelled() => return Err(ai_cancelled()),
                    client = tokio::task::spawn_blocking(move || PgClient::connect(config)) => {
                        client.map_err(join_error)??
                    }
                };
                let cancel = client.cancellation_token();
                let task = tokio::task::spawn_blocking(move || {
                    run_native_ai_query(client, sql, params, limits, isolated_read)
                });
                tokio::pin!(task);
                tokio::select! {
                    result = &mut task => result.map_err(join_error)?,
                    () = cancellation.cancelled() => {
                        let cancel_result = tokio::task::spawn_blocking(move || cancel.cancel())
                            .await
                            .map_err(join_error)?;
                        let _ = (&mut task).await;
                        cancel_result?;
                        Err(ai_cancelled())
                    }
                }
            }
            ConnectionTransport::Plugin(plugin) => {
                let mut collector = BoundedAiCollector::new(limits);
                let request_id = Uuid::new_v4().to_string();
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                if plugin.capabilities_v3.is_some() {
                    host.send_v3(&ConnectorRequestV3::Execute {
                        request_id: request_id.clone(),
                        connection_id: connection_id.to_owned(),
                        command: desktop_command_v3(command),
                        batch_size: QUERY_BATCH_ROWS as u32,
                    })
                    .await?;
                    let mut cancel_sent = false;
                    loop {
                        let response = if cancel_sent {
                            host.receive_v3().await?
                        } else {
                            tokio::select! {
                                response = host.receive_v3() => response?,
                                () = cancellation.cancelled() => {
                                    host.send_v3(&ConnectorRequestV3::Cancel {
                                        request_id: request_id.clone(),
                                    }).await?;
                                    cancel_sent = true;
                                    continue;
                                }
                            }
                        };
                        match response {
                            ConnectorResponseV3::ResultEvent {
                                request_id: actual,
                                event,
                            } if actual == request_id => {
                                let terminal =
                                    matches!(event, ConnectorResultEventV3::Complete { .. });
                                collector.push(map_connector_event_v3(event, Duration::ZERO)?);
                                if terminal {
                                    return collector.finish();
                                }
                            }
                            ConnectorResponseV3::Cancelled { request_id: actual }
                                if actual == request_id =>
                            {
                                return Err(ai_cancelled());
                            }
                            ConnectorResponseV3::Error {
                                request_id: actual,
                                error,
                            } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                                return Err(error.into_db_error());
                            }
                            _ => {
                                return Err(DbError::new(
                                    "08P01",
                                    "connector returned an unexpected v3 AI result response",
                                ));
                            }
                        }
                    }
                }
                let DesktopCommand::Text {
                    text: sql, params, ..
                } = command
                else {
                    return Err(DbError::unsupported(
                        "connector protocols v1 and v2 accept only SQL text commands",
                    ));
                };
                host.send(&ConnectorRequestV1::Execute {
                    request_id: request_id.clone(),
                    connection_id: connection_id.to_owned(),
                    sql,
                    params: params
                        .into_iter()
                        .map(|value| value.map_or(Value::Null, Value::Text))
                        .collect(),
                })
                .await?;
                let mut cancel_sent = false;
                loop {
                    let response = if cancel_sent {
                        host.receive().await?
                    } else {
                        tokio::select! {
                            response = host.receive() => response?,
                            () = cancellation.cancelled() => {
                                host.send(&ConnectorRequestV1::Cancel {
                                    request_id: request_id.clone(),
                                }).await?;
                                cancel_sent = true;
                                continue;
                            }
                        }
                    };
                    match response {
                        ConnectorResponseV1::QueryEvent {
                            request_id: actual,
                            event,
                        } if actual == request_id => {
                            let terminal = matches!(event, QueryEvent::Complete(_));
                            collector.push(map_connector_event(event, Duration::ZERO));
                            if terminal {
                                return collector.finish();
                            }
                        }
                        ConnectorResponseV1::Error {
                            request_id: actual,
                            error,
                        } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                            return Err(error);
                        }
                        _ => {
                            return Err(DbError::new(
                                "08P01",
                                "connector returned an unexpected AI query response",
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn cancel(&self, request_id: &str) -> Result<(), DbError> {
        validate_id(request_id, "request ID")?;
        let cancellation = read_lock(&self.requests)?
            .get(request_id)
            .map(|request| request.cancellation.clone());
        if let Some(cancellation) = cancellation {
            cancel_request(cancellation).await?;
        }
        Ok(())
    }

    async fn transaction(
        &self,
        connection_id: &str,
        action: TransactionAction,
    ) -> Result<CommandResult, DbError> {
        let connection = self.connection(connection_id)?;
        let sql = action.sql();
        match &connection.transport {
            ConnectionTransport::Native(native) => {
                let client = Arc::clone(&native.pg);
                let result = tokio::task::spawn_blocking(move || mutex_lock(&client)?.query(sql))
                    .await
                    .map_err(join_error)??;
                Ok(CommandResult {
                    command_tag: result
                        .command_tags
                        .last()
                        .cloned()
                        .unwrap_or_else(|| action.label().to_owned()),
                })
            }
            ConnectionTransport::Plugin(plugin) => {
                let request_id = Uuid::new_v4().to_string();
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                if plugin.capabilities_v3.is_some() {
                    host.send_v3(&action.connector_request_v3(&request_id, connection_id))
                        .await?;
                    return match host.receive_v3().await? {
                        ConnectorResponseV3::Transaction {
                            request_id: actual,
                            state,
                        } if actual == request_id => {
                            validate_transaction_state(action, state)?;
                            Ok(CommandResult {
                                command_tag: action.label().to_owned(),
                            })
                        }
                        ConnectorResponseV3::Error {
                            request_id: actual,
                            error,
                        } if actual.as_deref().is_none_or(|actual| actual == request_id) => {
                            Err(error.into_db_error())
                        }
                        _ => Err(DbError::new(
                            "08P01",
                            "connector returned an unexpected v3 transaction response",
                        )),
                    };
                }
                let request = action.connector_request(&request_id, connection_id);
                host.send(&request).await?;
                loop {
                    match host.receive().await? {
                        ConnectorResponseV1::QueryEvent {
                            request_id: actual,
                            event: QueryEvent::Complete(complete),
                        } if actual == request_id => {
                            return Ok(CommandResult {
                                command_tag: complete.tag,
                            });
                        }
                        ConnectorResponseV1::QueryEvent {
                            request_id: actual, ..
                        } if actual == request_id => {}
                        ConnectorResponseV1::Error { error, .. } => return Err(error),
                        _ => {
                            return Err(DbError::new(
                                "08P01",
                                "connector returned an unexpected transaction response",
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn monitor(&self, connection_id: &str) -> Result<MonitorSnapshot, DbError> {
        let connection = self.connection(connection_id)?;
        match &connection.transport {
            ConnectionTransport::Native(native) => {
                let sessions = admin_get(&self.http, &native.admin, "/v1/sessions", true);
                let queries = admin_get(&self.http, &native.admin, "/v1/queries", true);
                let locks = admin_get(&self.http, &native.admin, "/v1/locks", true);
                let metrics = admin_get(&self.http, &native.admin, "/v1/metrics", true);
                let storage = admin_get(&self.http, &native.admin, "/v1/storage", true);
                let wal = admin_get(&self.http, &native.admin, "/v1/wal", true);
                let config = admin_get(&self.http, &native.admin, "/v1/config", true);
                let (sessions, queries, locks, metrics, storage, wal, config) =
                    tokio::try_join!(sessions, queries, locks, metrics, storage, wal, config)?;
                Ok(MonitorSnapshot {
                    connection_id: connection_id.to_owned(),
                    sessions,
                    queries,
                    locks,
                    metrics,
                    storage,
                    wal,
                    backups: CapabilityStatus {
                        supported: true,
                        reason: String::new(),
                    },
                    config,
                })
            }
            ConnectionTransport::Plugin(plugin) => {
                let request_id = Uuid::new_v4().to_string();
                let mut host = plugin.host.lock().await;
                let host = host
                    .as_mut()
                    .ok_or_else(|| DbError::new("08003", "connector host is closed"))?;
                host.send(&ConnectorRequestV1::Monitor {
                    request_id: request_id.clone(),
                    connection_id: connection_id.to_owned(),
                })
                .await?;
                match host.receive().await? {
                    ConnectorResponseV1::Monitor {
                        request_id: actual,
                        sessions,
                        active_queries,
                    } if actual == request_id => Ok(MonitorSnapshot {
                        connection_id: connection_id.to_owned(),
                        sessions: Vec::new(),
                        queries: Vec::new(),
                        locks: LockStatus {
                            single_writer: false,
                            active_locks: Vec::new(),
                        },
                        metrics: Metrics {
                            active_sessions: sessions as usize,
                            active_queries: active_queries as usize,
                            engine: empty_engine_status(),
                        },
                        storage: empty_engine_status(),
                        wal: empty_engine_status(),
                        backups: CapabilityStatus {
                            supported: false,
                            reason: "connector does not expose OrdaDB backup administration".into(),
                        },
                        config: PublicConfig {
                            data_dir: String::new(),
                            pg_bind: String::new(),
                            admin_bind: String::new(),
                            remote_requires_tls: true,
                        },
                    }),
                    ConnectorResponseV1::Error { error, .. } => Err(error),
                    _ => Err(DbError::new(
                        "08P01",
                        "connector returned an unexpected monitor response",
                    )),
                }
            }
        }
    }

    pub(crate) async fn checkpoint(&self, connection_id: &str) -> Result<EngineStatus, DbError> {
        let connection = self.connection(connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "checkpoint is available only for native OrdaDB connections",
            ));
        };
        let completed: CheckpointResult =
            admin_post(&self.http, &native.admin, "/v1/checkpoint").await?;
        if !completed.completed {
            return Err(DbError::internal(
                "administration API returned an incomplete checkpoint",
            ));
        }
        admin_get(&self.http, &native.admin, "/v1/storage", true).await
    }

    async fn administration_operations(
        &self,
        connection_id: &str,
    ) -> Result<Vec<AdministrationOperation>, DbError> {
        let connection = self.connection(connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "administration operations are available only for native OrdaDB connections",
            ));
        };
        let operations: Vec<AdministrationOperationResponse> =
            admin_get(&self.http, &native.admin, "/v1/operations", true).await?;
        Ok(operations.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn start_administration_operation(
        &self,
        request: StartAdministrationOperationRequest,
    ) -> Result<AdministrationOperation, DbError> {
        validate_administration_operation_request(&request)?;
        let connection = self.connection(&request.connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "administration operations are available only for native OrdaDB connections",
            ));
        };
        let body = if request.kind.requires_table() {
            serde_json::json!({
                "path": request.path,
                "schema": request.schema,
                "table": request.table,
                "format": request.format,
            })
        } else {
            serde_json::json!({ "path": request.path })
        };
        let operation: AdministrationOperationResponse =
            admin_post_json(&self.http, &native.admin, request.kind.endpoint(), &body).await?;
        Ok(operation.into())
    }

    async fn administration_operation(
        &self,
        connection_id: &str,
        operation_id: &str,
    ) -> Result<AdministrationOperation, DbError> {
        let operation_id = validate_operation_id(operation_id)?;
        let connection = self.connection(connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "administration operations are available only for native OrdaDB connections",
            ));
        };
        let operation: AdministrationOperationResponse = admin_get(
            &self.http,
            &native.admin,
            &format!("/v1/operations/{operation_id}"),
            true,
        )
        .await?;
        Ok(operation.into())
    }

    async fn cancel_administration_operation(
        &self,
        connection_id: &str,
        operation_id: &str,
    ) -> Result<AdministrationOperation, DbError> {
        let operation_id = validate_operation_id(operation_id)?;
        let connection = self.connection(connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "administration operations are available only for native OrdaDB connections",
            ));
        };
        let operation: AdministrationOperationResponse = admin_post(
            &self.http,
            &native.admin,
            &format!("/v1/operations/{operation_id}/cancel"),
        )
        .await?;
        Ok(operation.into())
    }

    pub(crate) async fn administration_service(
        &self,
        connection_id: &str,
    ) -> Result<AdministrationServiceStatus, DbError> {
        let connection = self.connection(connection_id)?;
        let ConnectionTransport::Native(native) = &connection.transport else {
            return Err(DbError::new(
                "0A000",
                "service status is available only for native OrdaDB connections",
            ));
        };
        admin_get(&self.http, &native.admin, "/v1/service", true).await
    }
}

#[derive(Debug, Clone, Copy)]
enum TransactionAction {
    Begin,
    Commit,
    Rollback,
}

impl TransactionAction {
    const fn sql(self) -> &'static str {
        match self {
            Self::Begin => "BEGIN",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Begin => "BEGIN",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
        }
    }

    fn connector_request(self, request_id: &str, connection_id: &str) -> ConnectorRequestV1 {
        match self {
            Self::Begin => ConnectorRequestV1::Begin {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
            },
            Self::Commit => ConnectorRequestV1::Commit {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
            },
            Self::Rollback => ConnectorRequestV1::Rollback {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
            },
        }
    }

    fn connector_request_v3(self, request_id: &str, connection_id: &str) -> ConnectorRequestV3 {
        match self {
            Self::Begin => ConnectorRequestV3::Begin {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
                isolation: None,
            },
            Self::Commit => ConnectorRequestV3::Commit {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
            },
            Self::Rollback => ConnectorRequestV3::Rollback {
                request_id: request_id.to_owned(),
                connection_id: connection_id.to_owned(),
            },
        }
    }
}
