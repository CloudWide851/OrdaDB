
fn flatten_views(
    objects: &mut Vec<CatalogObject>,
    value: Option<&JsonValue>,
    schema: &str,
) -> Result<(), DbError> {
    for view in json_array(value, "catalog views")? {
        let kind = view
            .get("kind")
            .and_then(JsonValue::as_str)
            .filter(|kind| kind.to_ascii_lowercase().contains("material"))
            .map_or("view", |_| "materializedView");
        let name = identifier(
            view.get("name")
                .ok_or_else(|| DbError::new("08P01", "catalog view has no name"))?,
        )?;
        objects.push(CatalogObject {
            id: None,
            kind: kind.into(),
            schema: schema.into(),
            namespace: Some(schema.into()),
            name,
            parent: None,
            details: view.clone(),
        });
    }
    Ok(())
}

fn flatten_named(
    objects: &mut Vec<CatalogObject>,
    value: Option<&JsonValue>,
    kind: &str,
    schema: &str,
    parent: Option<&str>,
) -> Result<(), DbError> {
    for entry in json_array(value, "catalog object collection")? {
        let name = identifier(
            entry
                .get("name")
                .ok_or_else(|| DbError::new("08P01", "catalog object has no name"))?,
        )?;
        objects.push(CatalogObject {
            id: None,
            kind: kind.into(),
            schema: schema.into(),
            namespace: Some(schema.into()),
            name,
            parent: parent.map(str::to_owned),
            details: entry.clone(),
        });
    }
    Ok(())
}

fn json_array<'a>(value: Option<&'a JsonValue>, context: &str) -> Result<&'a [JsonValue], DbError> {
    value
        .and_then(JsonValue::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| DbError::new("08P01", format!("{context} is not an array")))
}

fn identifier(value: &JsonValue) -> Result<String, DbError> {
    let encoded = value
        .as_str()
        .ok_or_else(|| DbError::new("08P01", "catalog identifier is not a string"))?;
    encoded
        .strip_prefix("u:")
        .or_else(|| encoded.strip_prefix("q:"))
        .map(str::to_owned)
        .ok_or_else(|| DbError::new("08P01", "catalog identifier has no version marker"))
}

fn catalog_entry(entry: CatalogEntry) -> CatalogObject {
    CatalogObject {
        id: None,
        kind: entry.kind,
        namespace: (!entry.schema.is_empty()).then(|| entry.schema.clone()),
        schema: entry.schema,
        name: entry.name,
        parent: None,
        details: JsonValue::Null,
    }
}

fn run_native_ai_query(
    mut client: PgClient,
    sql: String,
    params: Vec<Option<String>>,
    limits: AiToolLimits,
    isolated_read: bool,
) -> Result<BoundedAiQueryResult, DbError> {
    if isolated_read {
        client.query("BEGIN TRANSACTION READ ONLY")?;
    }
    let mut collector = BoundedAiCollector::new(limits);
    let mut processed = 0_u64;
    let mut on_event = |event| {
        collect_native_pg_event(&mut collector, event, &mut processed);
        Ok(())
    };
    let params = params
        .into_iter()
        .map(|value| value.map(String::into_bytes))
        .collect::<Vec<_>>();
    let query_result = if params.is_empty() {
        client.query_batches(&sql, QUERY_BATCH_ROWS, &mut on_event)
    } else {
        client.query_prepared_batches(&sql, &[], &params, QUERY_BATCH_ROWS as u32, &mut on_event)
    };
    let rollback_result = isolated_read.then(|| client.query("ROLLBACK"));
    match (query_result, rollback_result) {
        (Ok(_), None | Some(Ok(_))) => collector.finish(),
        (Ok(_), Some(Err(error))) => Err(error),
        (Err(error), None | Some(Ok(_))) => Err(error),
        (Err(error), Some(Err(rollback))) => Err(error.with_hint(format!(
            "the read-only query failed and rollback also failed: {}",
            rollback.message
        ))),
    }
}

fn collect_native_pg_event(
    collector: &mut BoundedAiCollector,
    event: PgQueryEvent,
    processed: &mut u64,
) {
    let event = match event {
        PgQueryEvent::Schema(columns) => DbmsQueryEvent::Schema {
            columns: columns
                .into_iter()
                .map(|name| QueryColumn {
                    name,
                    data_type: "text".into(),
                })
                .collect(),
        },
        PgQueryEvent::Batch(rows) => {
            *processed = processed.saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
            collector.push(DbmsQueryEvent::Batch { rows });
            DbmsQueryEvent::Progress {
                rows_processed: *processed,
            }
        }
        PgQueryEvent::Notice(notice) => DbmsQueryEvent::Notice {
            severity: notice.severity.as_str().into(),
            sql_state: notice.sql_state,
            message: notice.message,
        },
        PgQueryEvent::Complete(command_tag) => DbmsQueryEvent::Complete {
            command_tag,
            duration_ms: 0,
        },
        PgQueryEvent::Notification(notification) => DbmsQueryEvent::Notice {
            severity: "NOTICE".into(),
            sql_state: "00000".into(),
            message: format!(
                "notification {} from backend {}: {}",
                notification.channel, notification.sender_process_id, notification.payload
            ),
        },
    };
    collector.push(event);
}

fn emit_native_pg_event(
    app: &AppHandle,
    request_id: &str,
    event: PgQueryEvent,
    processed: &mut u64,
) {
    match event {
        PgQueryEvent::Schema(columns) => emit_query(
            app,
            request_id,
            DbmsQueryEvent::Schema {
                columns: columns
                    .into_iter()
                    .map(|name| QueryColumn {
                        name,
                        data_type: "text".into(),
                    })
                    .collect(),
            },
        ),
        PgQueryEvent::Batch(rows) => {
            *processed = processed.saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
            emit_query(app, request_id, DbmsQueryEvent::Batch { rows });
            emit_query(
                app,
                request_id,
                DbmsQueryEvent::Progress {
                    rows_processed: *processed,
                },
            );
        }
        PgQueryEvent::Notice(notice) => emit_query(
            app,
            request_id,
            DbmsQueryEvent::Notice {
                severity: notice.severity.as_str().into(),
                sql_state: notice.sql_state,
                message: notice.message,
            },
        ),
        PgQueryEvent::Complete(_) => {}
        PgQueryEvent::Notification(notification) => emit_query(
            app,
            request_id,
            DbmsQueryEvent::Notice {
                severity: "NOTICE".into(),
                sql_state: "00000".into(),
                message: format!(
                    "notification {} from backend {}: {}",
                    notification.channel, notification.sender_process_id, notification.payload
                ),
            },
        ),
    }
}

fn desktop_command_v3(command: DesktopCommand) -> ConnectorCommandV3 {
    match command {
        DesktopCommand::Text {
            language_id,
            text,
            params,
        } => ConnectorCommandV3::Text {
            language_id,
            text,
            params: params
                .into_iter()
                .map(|value| ConnectorParameterV2 {
                    data_type: None,
                    value: value.map_or(ConnectorValueV2::Null, ConnectorValueV2::Text),
                })
                .collect(),
        },
        DesktopCommand::Document {
            language_id,
            document,
        } => ConnectorCommandV3::Document {
            language_id,
            document,
        },
        DesktopCommand::Arguments {
            language_id,
            arguments,
        } => ConnectorCommandV3::Arguments {
            language_id,
            arguments: arguments.into_iter().map(ConnectorValueV2::Text).collect(),
        },
    }
}

async fn run_connector_execute_v3(
    host: &mut ConnectorHost,
    app: &AppHandle,
    request_id: &str,
    connection_id: String,
    command: ConnectorCommandV3,
    cancellation: CancellationToken,
    started: Instant,
) -> Result<(), DbError> {
    host.send_v3(&ConnectorRequestV3::Execute {
        request_id: request_id.to_owned(),
        connection_id,
        command,
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
                        request_id: request_id.to_owned(),
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
                let terminal = matches!(event, ConnectorResultEventV3::Complete { .. });
                emit_query(
                    app,
                    request_id,
                    map_connector_event_v3(event, started.elapsed())?,
                );
                if terminal {
                    return Ok(());
                }
            }
            ConnectorResponseV3::Cancelled { request_id: actual } if actual == request_id => {
                return Err(DbError::new("57014", "connector command was cancelled"));
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
                    "connector returned an unexpected v3 result response",
                ));
            }
        }
    }
}

fn map_connector_event_v3(
    event: ConnectorResultEventV3,
    elapsed: Duration,
) -> Result<DbmsQueryEvent, DbError> {
    match event {
        ConnectorResultEventV3::Schema { columns } => Ok(DbmsQueryEvent::Schema {
            columns: columns
                .into_iter()
                .map(|column| QueryColumn {
                    name: column.name,
                    data_type: column.data_type.vendor_name,
                })
                .collect(),
        }),
        ConnectorResultEventV3::Batch {
            batch: ConnectorResultBatchV3::Rows { rows },
        } => Ok(DbmsQueryEvent::Batch {
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(connector_value_text).collect())
                .collect(),
        }),
        ConnectorResultEventV3::Batch {
            batch: ConnectorResultBatchV3::Documents { documents },
        } => Ok(DbmsQueryEvent::Documents { documents }),
        ConnectorResultEventV3::Batch {
            batch: ConnectorResultBatchV3::KeyValues { entries },
        } => Ok(DbmsQueryEvent::KeyValues {
            entries: entries
                .into_iter()
                .map(|entry| {
                    Ok(DbmsKeyValue {
                        key: connector_value_json(entry.key)?,
                        value: connector_value_json(entry.value)?,
                    })
                })
                .collect::<Result<Vec<_>, DbError>>()?,
        }),
        ConnectorResultEventV3::Progress { items_processed } => Ok(DbmsQueryEvent::Progress {
            rows_processed: items_processed,
        }),
        ConnectorResultEventV3::Notice { notice } => Ok(DbmsQueryEvent::Notice {
            severity: notice.severity,
            sql_state: notice.code.unwrap_or_default(),
            message: notice.message,
        }),
        ConnectorResultEventV3::Complete { command_tag, .. } => Ok(DbmsQueryEvent::Complete {
            command_tag,
            duration_ms: elapsed_ms(elapsed),
        }),
    }
}

fn connector_value_text(value: ConnectorValueV2) -> Option<String> {
    match value {
        ConnectorValueV2::Null => None,
        ConnectorValueV2::Boolean(value) => Some(value.to_string()),
        ConnectorValueV2::SignedInteger(value) => Some(value.to_string()),
        ConnectorValueV2::UnsignedInteger(value) => Some(value.to_string()),
        ConnectorValueV2::FloatingPoint(value) => Some(value.to_string()),
        ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Text(value)
        | ConnectorValueV2::Binary(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::Uuid(value) => Some(value),
        ConnectorValueV2::Json(value) => Some(value.to_string()),
        ConnectorValueV2::Array(values) => Some(
            JsonValue::Array(
                values
                    .into_iter()
                    .map(|value| {
                        connector_value_text(value).map_or(JsonValue::Null, JsonValue::String)
                    })
                    .collect(),
            )
            .to_string(),
        ),
    }
}

fn connector_value_json(value: ConnectorValueV2) -> Result<JsonValue, DbError> {
    let value = match value {
        ConnectorValueV2::Null => JsonValue::Null,
        ConnectorValueV2::Boolean(value) => JsonValue::Bool(value),
        ConnectorValueV2::SignedInteger(value) => value.into(),
        ConnectorValueV2::UnsignedInteger(value) => value.into(),
        ConnectorValueV2::FloatingPoint(value) => serde_json::Number::from_f64(value)
            .map(JsonValue::Number)
            .ok_or_else(|| DbError::new("08P01", "connector returned a non-finite number"))?,
        ConnectorValueV2::Decimal(value) => typed_connector_value("decimal", value),
        ConnectorValueV2::Text(value) => JsonValue::String(value),
        ConnectorValueV2::Binary(value) => typed_connector_value("binary", value),
        ConnectorValueV2::Date(value) => typed_connector_value("date", value),
        ConnectorValueV2::Time(value) => typed_connector_value("time", value),
        ConnectorValueV2::Timestamp(value) => typed_connector_value("timestamp", value),
        ConnectorValueV2::TimestampWithTimeZone(value) => {
            typed_connector_value("timestampWithTimeZone", value)
        }
        ConnectorValueV2::Interval(value) => typed_connector_value("interval", value),
        ConnectorValueV2::Uuid(value) => typed_connector_value("uuid", value),
        ConnectorValueV2::Json(value) => value,
        ConnectorValueV2::Array(values) => JsonValue::Array(
            values
                .into_iter()
                .map(connector_value_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    };
    Ok(value)
}

fn typed_connector_value(kind: &'static str, value: String) -> JsonValue {
    serde_json::json!({ "kind": kind, "value": value })
}

fn map_connector_event(event: QueryEvent, elapsed: Duration) -> DbmsQueryEvent {
    match event {
        QueryEvent::Schema(schema) => DbmsQueryEvent::Schema {
            columns: schema
                .fields
                .into_iter()
                .map(|field| QueryColumn {
                    name: field.name,
                    data_type: format!("{:?}", field.data_type),
                })
                .collect(),
        },
        QueryEvent::Batch(batch) => DbmsQueryEvent::Batch {
            rows: batch
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(value_text).collect())
                .collect(),
        },
        QueryEvent::Progress(progress) => DbmsQueryEvent::Progress {
            rows_processed: progress.rows_processed,
        },
        QueryEvent::Notice(notice) => DbmsQueryEvent::Notice {
            severity: notice.severity.as_str().into(),
            sql_state: notice.sql_state,
            message: notice.message,
        },
        QueryEvent::Complete(complete) => DbmsQueryEvent::Complete {
            command_tag: complete.tag,
            duration_ms: elapsed_ms(elapsed),
        },
    }
}

fn value_text(value: Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Boolean(value) => Some(value.to_string()),
        Value::Int16(value) => Some(value.to_string()),
        Value::Int32(value) => Some(value.to_string()),
        Value::Int64(value) => Some(value.to_string()),
        Value::Float32(value) => Some(value.to_string()),
        Value::Float64(value) => Some(value.to_string()),
        Value::Decimal(value) => Some(value.to_string()),
        Value::Text(value) => Some(value),
        Value::Binary(value) => Some(format!("{} bytes", value.len())),
        Value::Date(value) => Some(value.to_string()),
        Value::Time(value) => Some(value.to_string()),
        Value::Timestamp(value) => Some(value.to_string()),
        Value::Interval(value) => Some(value.to_string()),
        Value::Array(array) => Some(
            serde_json::Value::Array(
                array
                    .values()
                    .iter()
                    .cloned()
                    .map(|value| value_text(value).map_or(serde_json::Value::Null, Into::into))
                    .collect(),
            )
            .to_string(),
        ),
        Value::Json(value) | Value::Jsonb(value) => Some(value.to_string()),
        Value::Uuid(value) => Some(value.to_string()),
        Value::Vector(value) => Some(format!(
            "[{}]",
            value
                .into_iter()
                .map(|number| number.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn emit_query(app: &AppHandle, request_id: &str, event: DbmsQueryEvent) {
    let _ = app.emit(
        DBMS_QUERY_EVENT,
        QueryUpdate {
            request_id: request_id.to_owned(),
            event,
        },
    );
}

async fn cancel_request(cancellation: RequestCancellation) -> Result<(), DbError> {
    match cancellation {
        RequestCancellation::Native(token) => tokio::task::spawn_blocking(move || token.cancel())
            .await
            .map_err(join_error)?,
        RequestCancellation::Plugin(token) => {
            token.cancel();
            Ok(())
        }
    }
}

fn elapsed_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn empty_engine_status() -> EngineStatus {
    EngineStatus {
        generation: 0,
        table_count: 0,
        row_count: 0,
        index_count: 0,
        durable_lsn: None,
        dirty_page_count: 0,
        commits_since_checkpoint: 0,
    }
}

fn network_error(context: &str, error: impl std::fmt::Display) -> DbError {
    DbError::new("08006", context).with_detail(error.to_string())
}

fn ai_cancelled() -> DbError {
    DbError::new("57014", "AI database operation was cancelled")
}

fn join_error(error: tokio::task::JoinError) -> DbError {
    DbError::new("XX000", "database worker task failed").with_detail(error.to_string())
}

fn task_error(context: &str, error: impl std::fmt::Display) -> DbError {
    DbError::new("XX000", context).with_detail(error.to_string())
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, DbError> {
    mutex
        .lock()
        .map_err(|_| DbError::internal("database connection lock was poisoned"))
}

fn read_lock<T>(lock: &RwLock<T>) -> Result<RwLockReadGuard<'_, T>, DbError> {
    lock.read()
        .map_err(|_| DbError::internal("desktop DBMS state lock was poisoned"))
}

fn write_lock<T>(lock: &RwLock<T>) -> Result<RwLockWriteGuard<'_, T>, DbError> {
    lock.write()
        .map_err(|_| DbError::internal("desktop DBMS state lock was poisoned"))
}

fn probe_windows_service() -> Result<ServiceIdentity, DbError> {
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
        .map_err(|error| {
            DbError::new("58030", "failed to open Windows Service Control Manager")
                .with_detail(error.to_string())
        })?;
    let service = manager
        .open_service(
            ordadb_server::SERVICE_NAME,
            ServiceAccess::QUERY_STATUS | ServiceAccess::QUERY_CONFIG,
        )
        .map_err(|error| {
            DbError::new("55000", "OrdaDB Windows service is not installed")
                .with_detail(error.to_string())
                .with_hint("install or repair OrdaDB, then retry the local connection")
        })?;
    let status = service.query_status().map_err(|error| {
        DbError::new("58030", "failed to query OrdaDB Windows service")
            .with_detail(error.to_string())
    })?;
    if status.current_state != ServiceState::Running {
        return Err(
            DbError::new("55000", "OrdaDB Windows service is not running")
                .with_detail(format!("current service state: {:?}", status.current_state))
                .with_hint("start the OrdaDB service, then retry"),
        );
    }
    let process_id = status
        .process_id
        .filter(|process_id| *process_id != 0)
        .ok_or_else(|| {
            DbError::new(
                "55000",
                "OrdaDB Windows service has no running process identity",
            )
        })?;
    let configuration = service.query_config().map_err(|error| {
        DbError::new(
            "58030",
            "failed to query OrdaDB Windows service configuration",
        )
        .with_detail(error.to_string())
    })?;
    let command_line = configuration.executable_path.to_string_lossy();
    let arguments = split_windows_command_line(&command_line)?;
    let data_dir = arguments
        .windows(2)
        .find_map(|pair| (pair[0] == "--data-dir").then(|| PathBuf::from(&pair[1])))
        .ok_or_else(|| {
            DbError::new(
                "55000",
                "OrdaDB Windows service configuration has no data directory",
            )
            .with_hint("repair the OrdaDB service registration, then retry")
        })?;
    let data_dir = fs::canonicalize(&data_dir).map_err(|error| {
        DbError::new("58030", "failed to resolve OrdaDB service data directory")
            .with_detail(error.to_string())
    })?;
    let pipe_name = ordadb_server::bootstrap_pipe_name(&data_dir);
    Ok(ServiceIdentity {
        process_id,
        data_dir,
        pipe_name,
    })
}

fn split_windows_command_line(command_line: &str) -> Result<Vec<String>, DbError> {
    let characters = command_line.chars().collect::<Vec<_>>();
    let mut arguments = Vec::new();
    let mut offset = 0_usize;
    while offset < characters.len() {
        while offset < characters.len() && characters[offset].is_whitespace() {
            offset += 1;
        }
        if offset == characters.len() {
            break;
        }
        let mut argument = String::new();
        let mut quoted = false;
        while offset < characters.len() {
            match characters[offset] {
                character if character.is_whitespace() && !quoted => break,
                '"' => {
                    quoted = !quoted;
                    offset += 1;
                }
                '\\' => {
                    let start = offset;
                    while offset < characters.len() && characters[offset] == '\\' {
                        offset += 1;
                    }
                    let count = offset - start;
                    if offset < characters.len() && characters[offset] == '"' {
                        argument.extend(std::iter::repeat_n('\\', count / 2));
                        if count % 2 == 0 {
                            quoted = !quoted;
                        } else {
                            argument.push('"');
                        }
                        offset += 1;
                    } else {
                        argument.extend(std::iter::repeat_n('\\', count));
                    }
                }
                character => {
                    argument.push(character);
                    offset += 1;
                }
            }
        }
        if quoted {
            return Err(invalid(
                "OrdaDB Windows service command line contains an unmatched quote",
            ));
        }
        arguments.push(argument);
        while offset < characters.len() && characters[offset].is_whitespace() {
            offset += 1;
        }
    }
    if arguments.is_empty() {
        return Err(invalid("OrdaDB Windows service command line is empty"));
    }
    Ok(arguments)
}

fn connection_fingerprint(request: &ConnectRequest) -> [u8; 32] {
    let mut hash = Sha256::new();
    for value in [
        Some(request.connector_id.as_str()),
        Some(request.connector_kind.as_str()),
        Some(request.command_language.as_str()),
        request.dialect.as_deref(),
        Some(request.endpoint.as_str()),
        request.admin_endpoint.as_deref(),
        request.database.as_deref(),
        Some(connector_tls_mode_name(request.tls_mode)),
        Some(request.credential_id.as_str()),
        Some(request.credential_access.as_str()),
    ] {
        match value {
            Some(value) => {
                hash.update([1]);
                hash.update((value.len() as u64).to_le_bytes());
                hash.update(value.as_bytes());
            }
            None => hash.update([0]),
        }
    }
    hash.finalize().into()
}

const fn connector_tls_mode_name(mode: ConnectorTlsModeV2) -> &'static str {
    match mode {
        ConnectorTlsModeV2::Disable => "disable",
        ConnectorTlsModeV2::Prefer => "prefer",
        ConnectorTlsModeV2::Require => "require",
        ConnectorTlsModeV2::VerifyCa => "verifyCa",
        ConnectorTlsModeV2::VerifyFull => "verifyFull",
    }
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}
