
impl DesktopAiExecutor {
    async fn execute_audited(
        &self,
        context: AiToolExecutionContext,
        call: ValidatedAiToolCall,
        limits: AiToolLimits,
        cancellation: CancellationToken,
        approved: bool,
    ) -> Result<AiToolOutput> {
        self.persist_audit(
            &context,
            &call,
            AiAuditStatus::Started,
            "tool execution started",
        )?;
        let result = self
            .execute_tool(&context, &call, limits, cancellation, approved)
            .await;
        match &result {
            Ok(output) => {
                self.persist_audit(&context, &call, AiAuditStatus::Completed, &output.summary)?
            }
            Err(error) => self.persist_audit(
                &context,
                &call,
                if error.sql_state == "57014" {
                    AiAuditStatus::Cancelled
                } else {
                    AiAuditStatus::Error
                },
                &format!("tool failed with SQLSTATE {}", error.sql_state),
            )?,
        }
        result
    }

    async fn execute_tool(
        &self,
        context: &AiToolExecutionContext,
        call: &ValidatedAiToolCall,
        limits: AiToolLimits,
        cancellation: CancellationToken,
        approved: bool,
    ) -> Result<AiToolOutput> {
        match call.definition().name.as_str() {
            TOOL_CATALOG => self.catalog(context, call, limits).await,
            TOOL_DESCRIBE => self.describe(context, call, limits).await,
            TOOL_EXPLAIN => {
                let sql = required_string(call, "command")?;
                self.execute_query(context, format!("EXPLAIN {sql}"), limits, cancellation)
                    .await
            }
            TOOL_QUERY => {
                let command = required_string(call, "command")?.to_owned();
                self.execute_query(context, command, limits, cancellation)
                    .await
            }
            TOOL_VALIDATE_SQL | TOOL_REPAIR_SQL => self.validate_sql(call),
            TOOL_EXECUTE_SQL | TOOL_CONFIGURE => {
                ensure_approved(approved)?;
                let command = required_string(call, "command")?.to_owned();
                let result = self
                    .execute_command(&context.connection_id, command, limits, cancellation, false)
                    .await?;
                local_output(
                    json!({
                        "commandTag": result.command_tag,
                        "totalRows": result.total_rows,
                        "returnedValuesWithheld": true,
                    }),
                    "approved command completed; returned database values were withheld",
                )
            }
            TOOL_BACKUP => {
                ensure_approved(approved)?;
                self.start_file_operation(
                    context,
                    AdministrationOperationKind::Backup,
                    None,
                    None,
                    None,
                )
                .await
            }
            TOOL_RESTORE => {
                ensure_approved(approved)?;
                self.start_file_operation(
                    context,
                    AdministrationOperationKind::Restore,
                    None,
                    None,
                    None,
                )
                .await
            }
            TOOL_IMPORT | TOOL_EXPORT => {
                ensure_approved(approved)?;
                let schema = required_string(call, "schema")?.to_owned();
                let table = required_string(call, "table")?.to_owned();
                let format = parse_transfer_format(required_string(call, "format")?)?;
                let kind = if call.definition().name == TOOL_IMPORT {
                    AdministrationOperationKind::Import
                } else {
                    AdministrationOperationKind::Export
                };
                self.start_file_operation(context, kind, Some(schema), Some(table), Some(format))
                    .await
            }
            TOOL_CHECKPOINT => {
                ensure_approved(approved)?;
                let status = self.dbms.checkpoint(&context.connection_id).await?;
                local_output(
                    serde_json::to_value(status).map_err(json_error)?,
                    "checkpoint completed",
                )
            }
            TOOL_SERVICE => {
                ensure_approved(approved)?;
                self.manage_service(context, required_string(call, "action")?)
                    .await
            }
            _ => Err(DbError::new("22023", "AI tool is not registered")),
        }
    }

    async fn catalog(
        &self,
        context: &AiToolExecutionContext,
        call: &ValidatedAiToolCall,
        limits: AiToolLimits,
    ) -> Result<AiToolOutput> {
        let filter = required_string_allow_empty(call, "filter", 512)?.to_ascii_lowercase();
        let requested_limit = required_usize(call, "limit")?;
        let limit = requested_limit.min(MAX_CATALOG_RESULT_OBJECTS);
        if limit == 0 {
            return Err(DbError::new("22023", "catalog limit must be positive"));
        }
        let snapshot = self.dbms.catalog(&context.connection_id).await?;
        let mut value = serde_json::to_value(snapshot).map_err(json_error)?;
        let objects = value
            .get_mut("objects")
            .and_then(JsonValue::as_array_mut)
            .ok_or_else(|| DbError::internal("catalog projection has no objects array"))?;
        objects.retain(|object| {
            filter.is_empty()
                || ["name", "schema", "namespace", "kind"]
                    .iter()
                    .filter_map(|field| object.get(field).and_then(JsonValue::as_str))
                    .any(|text| text.to_ascii_lowercase().contains(&filter))
        });
        let total = objects.len();
        objects.truncate(limit);
        while encoded_len(&value)? > limits.max_result_bytes {
            let objects = value
                .get_mut("objects")
                .and_then(JsonValue::as_array_mut)
                .ok_or_else(|| DbError::internal("catalog projection has no objects array"))?;
            if objects.pop().is_none() {
                return Err(DbError::new(
                    "54000",
                    "catalog metadata exceeds the AI result limit",
                ));
            }
        }
        let retained = value["objects"].as_array().map_or(0, Vec::len);
        let disclosure = AiContextDisclosure {
            categories: vec!["catalogMetadata".to_owned()],
            columns: Vec::new(),
            item_count: retained,
            estimated_bytes: encoded_len(&value)?,
            redaction_summary: "schema and object metadata only; no row values".to_owned(),
            values_included: false,
        };
        let bytes_retained = encoded_len(&value)?;
        Ok(AiToolOutput {
            content: value,
            rows_retained: retained,
            total_rows: total,
            bytes_retained,
            truncated: retained < total,
            summary: format!("returned {retained} of {total} matching catalog objects"),
            disclosure: Some(disclosure),
        })
    }

    async fn describe(
        &self,
        context: &AiToolExecutionContext,
        call: &ValidatedAiToolCall,
        limits: AiToolLimits,
    ) -> Result<AiToolOutput> {
        let name = required_string(call, "name")?.to_ascii_lowercase();
        let snapshot = self.dbms.catalog(&context.connection_id).await?;
        let value = serde_json::to_value(snapshot).map_err(json_error)?;
        let matches = value["objects"]
            .as_array()
            .ok_or_else(|| DbError::internal("catalog projection has no objects array"))?
            .iter()
            .filter(|object| {
                let object_name = object["name"].as_str().unwrap_or_default();
                let schema = object["schema"].as_str().unwrap_or_default();
                object_name.eq_ignore_ascii_case(&name)
                    || format!("{schema}.{object_name}").eq_ignore_ascii_case(&name)
            })
            .take(32)
            .cloned()
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(DbError::new("42704", "catalog object does not exist"));
        }
        let content = json!({"objects": matches});
        let bytes_retained = encoded_len(&content)?;
        if bytes_retained > limits.max_result_bytes {
            return Err(DbError::new(
                "54000",
                "object description exceeds the AI result limit",
            ));
        }
        Ok(AiToolOutput {
            rows_retained: matches.len(),
            total_rows: matches.len(),
            content,
            bytes_retained,
            truncated: false,
            summary: format!("described {} matching catalog object(s)", matches.len()),
            disclosure: Some(AiContextDisclosure {
                categories: vec!["objectMetadata".to_owned()],
                columns: Vec::new(),
                item_count: matches.len(),
                estimated_bytes: bytes_retained,
                redaction_summary: "object metadata only; no row values".to_owned(),
                values_included: false,
            }),
        })
    }

    async fn execute_query(
        &self,
        context: &AiToolExecutionContext,
        command: String,
        limits: AiToolLimits,
        cancellation: CancellationToken,
    ) -> Result<AiToolOutput> {
        let result = self
            .execute_command(&context.connection_id, command, limits, cancellation, true)
            .await?;
        let include_values = context.include_sample_values
            && context.data_sharing != AiDataSharingPolicy::SchemaOnly;
        let disclosure = AiContextDisclosure {
            categories: vec![if include_values {
                "redactedResultSamples".to_owned()
            } else {
                "resultShape".to_owned()
            }],
            columns: result.columns.clone(),
            item_count: if include_values {
                result.rows_retained
            } else {
                0
            },
            estimated_bytes: if include_values {
                result.bytes_retained
            } else {
                0
            },
            redaction_summary: if include_values {
                "sensitive-looking columns masked; strings bounded to 512 bytes".to_owned()
            } else {
                "database values withheld; only columns, counts, and completion are shared"
                    .to_owned()
            },
            values_included: include_values,
        };
        let disclosure = authorize_context_disclosure(
            context.data_sharing,
            disclosure,
            context.include_sample_values,
        )?;
        let content = if include_values {
            let policy = AiRedactionPolicy {
                masked_columns: sensitive_columns(&result.columns),
                ..AiRedactionPolicy::default()
            };
            let masked =
                mask_positional_columns(result.content, &result.columns, &policy.masked_columns);
            redact_sample(&masked, &policy)?
        } else {
            json!({
                "kind": "resultShape",
                "columns": result.columns,
                "rowsRetainedLocally": result.rows_retained,
                "totalRows": result.total_rows,
                "commandTag": result.command_tag,
                "valuesWithheld": true,
            })
        };
        let bytes_retained = encoded_len(&content)?;
        Ok(AiToolOutput {
            content,
            rows_retained: if include_values {
                result.rows_retained
            } else {
                0
            },
            total_rows: result.total_rows,
            bytes_retained,
            truncated: result.truncated,
            summary: format!(
                "{} rows observed; {} retained locally{}",
                result.total_rows,
                result.rows_retained,
                if include_values {
                    " and disclosed under the selected policy"
                } else {
                    "; values withheld"
                }
            ),
            disclosure: Some(disclosure),
        })
    }

    async fn execute_command(
        &self,
        connection_id: &str,
        command: String,
        limits: AiToolLimits,
        cancellation: CancellationToken,
        isolated_read: bool,
    ) -> Result<crate::dbms::BoundedAiQueryResult> {
        let command = desktop_command(&self.policy, command)?;
        self.dbms
            .execute_ai_command(connection_id, command, limits, cancellation, isolated_read)
            .await
    }

    fn validate_sql(&self, call: &ValidatedAiToolCall) -> Result<AiToolOutput> {
        let sql = required_string(call, "command")?;
        let dialect = policy_dialect(&self.policy).ok_or_else(|| {
            DbError::new(
                "0A000",
                "SQL validation is unavailable for this connector command language",
            )
        })?;
        let parsed = if self.policy.native || dialect == SqlDialect::PostgreSql {
            parse(sql)
        } else {
            parse_with_dialect(sql, dialect)
        };
        match parsed {
            Ok(statement) => local_output(
                json!({
                    "valid": true,
                    "effect": classify_statement_effect(&statement),
                    "commandLength": sql.len(),
                }),
                "SQL parsed successfully; use the reported conservative effect classification",
            ),
            Err(error) => local_output(
                json!({
                    "valid": false,
                    "error": {
                        "sqlState": error.sql_state,
                        "message": error.message,
                        "position": error.position,
                    }
                }),
                "SQL requires repair before execution",
            ),
        }
    }

    async fn start_file_operation(
        &self,
        context: &AiToolExecutionContext,
        kind: AdministrationOperationKind,
        schema: Option<String>,
        table: Option<String>,
        format: Option<AdministrationTransferFormat>,
    ) -> Result<AiToolOutput> {
        let service = self
            .dbms
            .administration_service(&context.connection_id)
            .await?;
        let operations_root = PathBuf::from(service.operations_root());
        let selected = choose_operation_file(&operations_root, kind, format).await?;
        let relative = relative_operation_path(&operations_root, &selected, kind)?;
        let operation = self
            .dbms
            .start_administration_operation(StartAdministrationOperationRequest {
                connection_id: context.connection_id.clone(),
                kind,
                path: relative,
                schema,
                table,
                format,
            })
            .await?;
        local_output(
            serde_json::to_value(operation).map_err(json_error)?,
            "administration operation accepted after native file selection",
        )
    }

    async fn manage_service(
        &self,
        context: &AiToolExecutionContext,
        action: &str,
    ) -> Result<AiToolOutput> {
        let status = self
            .dbms
            .administration_service(&context.connection_id)
            .await?;
        let data_dir = PathBuf::from(status.data_dir());
        let executable = std::env::current_exe()
            .map_err(|error| io_error("failed to resolve desktop executable", error))?
            .parent()
            .ok_or_else(|| DbError::new("58030", "desktop executable has no parent directory"))?
            .join("ordadb-server.exe");
        let action = action.to_owned();
        let result = tokio::task::spawn_blocking(move || match action.as_str() {
            "start" => manage_windows_service(ServiceCommand::Start, executable, data_dir),
            "stop" => manage_windows_service(ServiceCommand::Stop, executable, data_dir),
            "restart" => {
                manage_windows_service(ServiceCommand::Stop, &executable, &data_dir)?;
                manage_windows_service(ServiceCommand::Start, executable, data_dir)
            }
            _ => Err(DbError::new(
                "22023",
                "AI service action must be start, stop, or restart",
            )),
        })
        .await
        .map_err(join_error)??;
        local_output(
            serde_json::to_value(result).map_err(json_error)?,
            "Windows service action completed",
        )
    }

    fn persist_audit(
        &self,
        context: &AiToolExecutionContext,
        call: &ValidatedAiToolCall,
        status: AiAuditStatus,
        summary: &str,
    ) -> Result<()> {
        let entry = project_audit_entry(
            &context.run_id,
            call,
            status,
            &bounded_text(summary, 512),
            unix_time_millis(),
        )?;
        update_persistence(&self.console, &self.persistence, |state| {
            state.audit.push(entry);
        })
    }
}

struct DesktopAiEventSink {
    app: AppHandle,
    console: Arc<ConsoleRuntime>,
    persistence: Arc<Mutex<AiPersistenceV1>>,
    assistant_text: Mutex<String>,
}

#[async_trait]
impl AiRunEventSink for DesktopAiEventSink {
    async fn emit(&self, event: AiRunEvent) -> Result<()> {
        match &event.payload {
            AiRunEventPayload::TextDelta { delta } => {
                mutex_lock(&self.assistant_text, "AI assistant text")?.push_str(delta);
            }
            payload if payload.is_terminal() => {
                let assistant =
                    std::mem::take(&mut *mutex_lock(&self.assistant_text, "AI assistant text")?);
                if !assistant.is_empty() {
                    append_history(
                        &self.console,
                        &self.persistence,
                        AiHistoryEntry {
                            role: AiHistoryRole::Assistant,
                            text: bounded_history_text(&assistant),
                            created_at_ms: unix_time_millis(),
                        },
                    )?;
                }
            }
            _ => {}
        }
        self.app.emit(AI_RUN_EVENT, event).map_err(|error| {
            DbError::new("58030", "failed to emit AI run event").with_detail(error.to_string())
        })
    }
}

fn tool_registry() -> Result<AiToolRegistry> {
    AiToolRegistry::new(vec![
        tool(
            TOOL_CATALOG,
            "Search bounded database catalog metadata",
            object_schema(vec![
                ("filter", string_schema()),
                ("limit", integer_schema()),
            ]),
            AiToolRisk::ReadOnly,
        ),
        tool(
            TOOL_DESCRIBE,
            "Describe one bounded database object",
            object_schema(vec![("name", string_schema())]),
            AiToolRisk::ReadOnly,
        ),
        tool(
            TOOL_EXPLAIN,
            "Explain a conservatively read-only SQL command",
            command_schema(),
            AiToolRisk::Dynamic,
        ),
        tool(
            TOOL_QUERY,
            "Execute a bounded command; unsafe or unproven reads require approval",
            command_schema(),
            AiToolRisk::Dynamic,
        ),
        tool(
            TOOL_VALIDATE_SQL,
            "Parse SQL and return its conservative effect classification",
            command_schema(),
            AiToolRisk::Local,
        ),
        tool(
            TOOL_REPAIR_SQL,
            "Validate a repaired SQL candidate and return structured parser feedback",
            command_schema(),
            AiToolRisk::Local,
        ),
        tool(
            TOOL_EXECUTE_SQL,
            "Execute DML or DDL only after exact user approval",
            command_schema(),
            AiToolRisk::RequiresApproval,
        ),
        tool(
            TOOL_CONFIGURE,
            "Execute a configuration/session command only after exact user approval",
            command_schema(),
            AiToolRisk::RequiresApproval,
        ),
        tool(
            TOOL_BACKUP,
            "Choose a native destination and start a logical backup",
            empty_schema(),
            AiToolRisk::RequiresApproval,
        ),
        tool(
            TOOL_RESTORE,
            "Choose a native source and start a logical restore",
            empty_schema(),
            AiToolRisk::RequiresApproval,
        ),
        tool(
            TOOL_IMPORT,
            "Choose a native source and import a table",
            transfer_schema(),
            AiToolRisk::RequiresApproval,
        ),
        tool(
            TOOL_EXPORT,
            "Choose a native destination and export a table",
            transfer_schema(),
            AiToolRisk::RequiresApproval,
        ),
        tool(
            TOOL_CHECKPOINT,
            "Create a native database checkpoint after approval",
            empty_schema(),
            AiToolRisk::RequiresApproval,
        ),
        tool(
            TOOL_SERVICE,
            "Start, stop, or restart the native Windows service after approval",
            object_schema(vec![(
                "action",
                json!({"type": "string", "enum": ["start", "stop", "restart"]}),
            )]),
            AiToolRisk::RequiresApproval,
        ),
    ])
}

fn tool(
    name: &str,
    description: &str,
    parameters: JsonValue,
    risk: AiToolRisk,
) -> AiToolDefinition {
    AiToolDefinition {
        name: name.to_owned(),
        version: 1,
        description: description.to_owned(),
        parameters,
        risk,
    }
}

fn empty_schema() -> JsonValue {
    object_schema(Vec::new())
}

fn command_schema() -> JsonValue {
    object_schema(vec![("command", string_schema())])
}

fn transfer_schema() -> JsonValue {
    object_schema(vec![
        ("schema", string_schema()),
        ("table", string_schema()),
        (
            "format",
            json!({"type": "string", "enum": ["csv", "jsonLines"]}),
        ),
    ])
}

fn object_schema(properties: Vec<(&str, JsonValue)>) -> JsonValue {
    let required = properties
        .iter()
        .map(|(name, _)| JsonValue::String((*name).to_owned()))
        .collect::<Vec<_>>();
    let properties = properties
        .into_iter()
        .map(|(name, schema)| (name.to_owned(), schema))
        .collect::<serde_json::Map<_, _>>();
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn string_schema() -> JsonValue {
    json!({"type": "string"})
}

fn integer_schema() -> JsonValue {
    json!({"type": "integer"})
}

fn authorization_mode(
    policy: &AiConnectionPolicy,
    call: &ValidatedAiToolCall,
) -> Result<AiToolExecutionMode> {
    let name = call.definition().name.as_str();
    if matches!(name, TOOL_VALIDATE_SQL | TOOL_REPAIR_SQL) {
        return Ok(AiToolExecutionMode::ReadOnly);
    }
    if matches!(
        name,
        TOOL_EXECUTE_SQL
            | TOOL_CONFIGURE
            | TOOL_BACKUP
            | TOOL_RESTORE
            | TOOL_IMPORT
            | TOOL_EXPORT
            | TOOL_CHECKPOINT
            | TOOL_SERVICE
    ) {
        return Ok(AiToolExecutionMode::Mutation);
    }
    if matches!(name, TOOL_CATALOG | TOOL_DESCRIBE) {
        return Ok(
            if policy.native || policy.credential_access == CredentialAccess::ReadOnly {
                AiToolExecutionMode::ReadOnly
            } else {
                AiToolExecutionMode::Mutation
            },
        );
    }
    let command = required_string(call, "command")?;
    let classified = if name == TOOL_EXPLAIN {
        format!("EXPLAIN {command}")
    } else {
        command.to_owned()
    };
    Ok(if command_is_auto_read(policy, &classified) {
        AiToolExecutionMode::ReadOnly
    } else {
        AiToolExecutionMode::Mutation
    })
}

fn command_is_auto_read(policy: &AiConnectionPolicy, command: &str) -> bool {
    if !policy.native && policy.credential_access != CredentialAccess::ReadOnly {
        return false;
    }
    let Some(dialect) = policy_dialect(policy) else {
        return false;
    };
    let parsed = if policy.native || dialect == SqlDialect::PostgreSql {
        parse(command)
    } else {
        parse_with_dialect(command, dialect)
    };
    parsed
        .as_ref()
        .is_ok_and(|statement| classify_statement_effect(statement) == StatementEffect::ReadOnly)
}

fn policy_dialect(policy: &AiConnectionPolicy) -> Option<SqlDialect> {
    match policy.command_language.as_str() {
        "postgresql" | "ordadb-sql" => Some(SqlDialect::PostgreSql),
        "mysql" | "mysql-sql" | "mariadb" | "mariadb-sql" => Some(SqlDialect::MySql),
        "sqlite" | "sqlite-sql" => Some(SqlDialect::Sqlite),
        "sql-server" | "sqlserver" | "sql-server-sql" => Some(SqlDialect::SqlServer),
        _ => None,
    }
}

fn desktop_command(policy: &AiConnectionPolicy, command: String) -> Result<DesktopCommand> {
    match policy.connector_kind.as_str() {
        "sql" => Ok(DesktopCommand::Text {
            language_id: policy.command_language.clone(),
            text: command,
            params: Vec::new(),
        }),
        "document" => Ok(DesktopCommand::Document {
            language_id: policy.command_language.clone(),
            document: serde_json::from_str(&command).map_err(|error| {
                DbError::new("22023", "document database command must be valid JSON")
                    .with_detail(error.to_string())
            })?,
        }),
        "keyValue" => Ok(DesktopCommand::Arguments {
            language_id: policy.command_language.clone(),
            arguments: split_command_arguments(&command)?,
        }),
        _ => Err(DbError::new("22023", "unknown AI connector kind")),
    }
}

fn split_command_arguments(command: &str) -> Result<Vec<String>> {
    validate_text(command, 1024 * 1024, "key/value command")?;
    let mut arguments = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in command.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
            continue;
        }
        if let Some(expected) = quote {
            if character == expected {
                quote = None;
            } else {
                current.push(character);
            }
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character);
        } else if character.is_whitespace() {
            if !current.is_empty() {
                arguments.push(std::mem::take(&mut current));
            }
        } else {
            current.push(character);
        }
    }
    if escaped || quote.is_some() {
        return Err(DbError::new(
            "22023",
            "key/value command contains an incomplete escape or quote",
        ));
    }
    if !current.is_empty() {
        arguments.push(current);
    }
    if arguments.is_empty() || arguments.len() > 256 {
        return Err(DbError::new(
            "22023",
            "key/value command must contain between 1 and 256 arguments",
        ));
    }
    Ok(arguments)
}

fn tool_preview(call: &ValidatedAiToolCall) -> Result<String> {
    let preview = match call.definition().name.as_str() {
        TOOL_BACKUP | TOOL_RESTORE | TOOL_CHECKPOINT => call.definition().name.clone(),
        TOOL_IMPORT | TOOL_EXPORT => format!(
            "{} {}.{} as {} (path selected locally)",
            call.definition().name,
            required_string(call, "schema")?,
            required_string(call, "table")?,
            required_string(call, "format")?,
        ),
        TOOL_SERVICE => format!("service {}", required_string(call, "action")?),
        TOOL_CATALOG => format!(
            "catalog filter={:?} limit={}",
            bounded_text(required_string(call, "filter")?, 256),
            required_usize(call, "limit")?,
        ),
        TOOL_DESCRIBE => format!("describe {}", required_string(call, "name")?),
        _ => required_string(call, "command")?.to_owned(),
    };
    Ok(bounded_text(&preview, MAX_TOOL_PREVIEW_BYTES))
}

fn mutation_impact(call: &ValidatedAiToolCall) -> String {
    match call.definition().name.as_str() {
        TOOL_BACKUP => "Create a logical backup in a user-selected native location".to_owned(),
        TOOL_RESTORE => "Replace database state from a user-selected logical backup".to_owned(),
        TOOL_IMPORT => "Insert imported records into the selected table".to_owned(),
        TOOL_EXPORT => "Write selected table data to a user-selected native file".to_owned(),
        TOOL_CHECKPOINT => "Flush a durable database checkpoint".to_owned(),
        TOOL_SERVICE => "Change the OrdaDB Windows service lifecycle state".to_owned(),
        TOOL_CATALOG | TOOL_DESCRIBE | TOOL_EXPLAIN | TOOL_QUERY => {
            "Use a database credential that is not proven read-only".to_owned()
        }
        _ => "Execute a database or session mutation exactly as previewed".to_owned(),
    }
}
