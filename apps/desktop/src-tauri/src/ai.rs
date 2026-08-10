use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use ordadb_ai::{
    AiApprovalBroker, AiApprovalDecision, AiAuditStatus, AiContextDisclosure, AiDataSharingPolicy,
    AiHistoryEntry, AiHistoryRole, AiPersistenceV1, AiProvider, AiProviderKind, AiProviderSettings,
    AiRedactionPolicy, AiRunEngine, AiRunEvent, AiRunEventPayload, AiRunEventSink, AiRunRequest,
    AiToolAuthorization, AiToolDefinition, AiToolExecutionContext, AiToolExecutionMode,
    AiToolExecutor, AiToolLimits, AiToolOutput, AiToolRegistry, AiToolRisk, ApprovedAiToolCall,
    FakeProvider, OllamaProvider, OpenAiProvider, ValidatedAiToolCall, anonymous_safety_identifier,
    authorize_context_disclosure, project_audit_entry, project_persistence, redact_sample,
    validate_provider_settings,
};
use ordadb_server::{ServiceCommand, manage_windows_service};
use ordadb_sql::{
    SqlDialect, StatementEffect, classify_statement_effect, parse, parse_with_dialect,
};
use ordadb_types::{DbError, Result};
use ordadb_windows::{CredentialVault, prompt_for_credential};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tauri::{AppHandle, Emitter, State};
use tokio_util::sync::CancellationToken;

use crate::dbms::{
    AdministrationOperationKind, AdministrationTransferFormat, AiConnectionPolicy, DbmsError,
    DbmsRuntime, DesktopCommand, StartAdministrationOperationRequest,
};
use crate::workspace::{ConsoleRuntime, CredentialAccess};

pub const AI_RUN_EVENT: &str = "ai://run";

const TOOL_CATALOG: &str = "catalog";
const TOOL_DESCRIBE: &str = "describe_object";
const TOOL_EXPLAIN: &str = "explain";
const TOOL_QUERY: &str = "query";
const TOOL_VALIDATE_SQL: &str = "validate_sql";
const TOOL_REPAIR_SQL: &str = "repair_sql";
const TOOL_EXECUTE_SQL: &str = "execute_sql";
const TOOL_CONFIGURE: &str = "configure_session";
const TOOL_BACKUP: &str = "backup";
const TOOL_RESTORE: &str = "restore";
const TOOL_IMPORT: &str = "import";
const TOOL_EXPORT: &str = "export";
const TOOL_CHECKPOINT: &str = "checkpoint";
const TOOL_SERVICE: &str = "service";

const MAX_HISTORY_TEXT_BYTES: usize = 64 * 1024;
const MAX_TOOL_PREVIEW_BYTES: usize = 4 * 1024;
const MAX_CATALOG_RESULT_OBJECTS: usize = 1_000;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiRunStarted {
    run_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiCredentialPromptRequest {
    credential_id: String,
    provider_label: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiCredentialStatus {
    credential_id: String,
    configured: bool,
    account_label: Option<String>,
}

#[derive(Debug, Clone)]
struct ActiveAiRun {
    cancellation: CancellationToken,
}

pub struct DesktopAiRuntime {
    dbms: Arc<DbmsRuntime>,
    console: Arc<ConsoleRuntime>,
    credentials: CredentialVault,
    approvals: Arc<AiApprovalBroker>,
    persistence: Arc<Mutex<AiPersistenceV1>>,
    runs: Mutex<BTreeMap<String, ActiveAiRun>>,
    safety_identifier: String,
}

impl std::fmt::Debug for DesktopAiRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DesktopAiRuntime")
            .field("credentials", &"<Windows Credential Manager>")
            .field("approvals", &self.approvals)
            .field("persistence", &"<bounded visible projection>")
            .field("runs", &"<active runs>")
            .field("safety_identifier", &"<anonymous hash>")
            .finish()
    }
}

impl DesktopAiRuntime {
    pub fn open(
        dbms: Arc<DbmsRuntime>,
        console: Arc<ConsoleRuntime>,
        stable_local_state_root: &Path,
    ) -> Result<Arc<Self>> {
        let stable_source = stable_local_state_root.as_os_str().to_string_lossy();
        let safety_identifier = anonymous_safety_identifier(stable_source.as_bytes())?;
        let persistence = console.load_ai_state()?;
        Ok(Arc::new(Self {
            dbms,
            console,
            credentials: CredentialVault::new("OrdaDB/AI")?,
            approvals: Arc::new(AiApprovalBroker::default()),
            persistence: Arc::new(Mutex::new(persistence)),
            runs: Mutex::new(BTreeMap::new()),
            safety_identifier,
        }))
    }

    pub fn state(&self) -> Result<AiPersistenceV1> {
        Ok(mutex_lock(&self.persistence, "AI persistence")?.clone())
    }

    pub async fn prompt_credential(
        &self,
        request: AiCredentialPromptRequest,
    ) -> Result<Option<AiCredentialStatus>> {
        validate_identifier(&request.credential_id, "AI credential ID")?;
        validate_text(&request.provider_label, 128, "AI provider label")?;
        let target = format!("OrdaDB AI {}", request.credential_id);
        let suggested = request.provider_label.clone();
        let prompted = tokio::task::spawn_blocking(move || {
            prompt_for_credential(
                &target,
                &suggested,
                "OrdaDB AI Provider",
                "Enter the provider API key. The secret is stored only in Windows Credential Manager.",
            )
        })
        .await
        .map_err(join_error)??;
        let Some(prompted) = prompted else {
            return Ok(None);
        };
        self.credentials.store(
            &request.credential_id,
            &prompted.username,
            &prompted.password,
        )?;
        Ok(Some(AiCredentialStatus {
            credential_id: request.credential_id,
            configured: true,
            account_label: Some(prompted.username),
        }))
    }

    pub fn credential_status(&self, credential_id: &str) -> Result<AiCredentialStatus> {
        validate_identifier(credential_id, "AI credential ID")?;
        match self.credentials.load(credential_id) {
            Ok(stored) => Ok(AiCredentialStatus {
                credential_id: credential_id.to_owned(),
                configured: true,
                account_label: Some(stored.username),
            }),
            Err(error) if error.sql_state == "42704" => Ok(AiCredentialStatus {
                credential_id: credential_id.to_owned(),
                configured: false,
                account_label: None,
            }),
            Err(error) => Err(error),
        }
    }

    pub fn delete_credential(&self, credential_id: &str) -> Result<()> {
        validate_identifier(credential_id, "AI credential ID")?;
        match self.credentials.delete(credential_id) {
            Ok(()) => Ok(()),
            Err(error) if error.sql_state == "42704" => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub fn start_run(
        self: &Arc<Self>,
        app: AppHandle,
        request: AiRunRequest,
    ) -> Result<AiRunStarted> {
        validate_identifier(&request.run_id, "AI run ID")?;
        validate_identifier(&request.connection_id, "AI connection ID")?;
        validate_provider_settings(&request.settings)?;
        if request.settings.kind == AiProviderKind::Fake {
            return Err(DbError::new(
                "0A000",
                "the deterministic fake provider is available only in Browser Preview tests",
            ));
        }
        let policy = self.dbms.ai_connection_policy(&request.connection_id)?;
        let provider = self.provider(&request.settings)?;
        let registry = tool_registry()?;
        let executor = Arc::new(DesktopAiExecutor {
            dbms: Arc::clone(&self.dbms),
            console: Arc::clone(&self.console),
            persistence: Arc::clone(&self.persistence),
            policy,
        });
        let engine = Arc::new(AiRunEngine::new(
            provider,
            executor,
            registry,
            Arc::clone(&self.approvals),
            self.safety_identifier.clone(),
        )?);
        let cancellation = CancellationToken::new();
        {
            let mut runs = mutex_lock(&self.runs, "AI run registry")?;
            if runs.contains_key(&request.run_id) {
                return Err(DbError::new("55000", "AI run ID is already active"));
            }
            runs.insert(
                request.run_id.clone(),
                ActiveAiRun {
                    cancellation: cancellation.clone(),
                },
            );
        }
        if let Err(error) = append_history(
            &self.console,
            &self.persistence,
            AiHistoryEntry {
                role: AiHistoryRole::User,
                text: bounded_history_text(&request.user_text),
                created_at_ms: unix_time_millis(),
            },
        ) {
            mutex_lock(&self.runs, "AI run registry")?.remove(&request.run_id);
            return Err(error);
        }
        let sink = Arc::new(DesktopAiEventSink {
            app,
            console: Arc::clone(&self.console),
            persistence: Arc::clone(&self.persistence),
            assistant_text: Mutex::new(String::new()),
        });
        let runtime = Arc::clone(self);
        let run_id = request.run_id.clone();
        let started_run_id = run_id.clone();
        tauri::async_runtime::spawn(async move {
            let _reported_outcome = engine.run(request, sink, cancellation).await;
            if let Ok(mut runs) = runtime.runs.lock() {
                runs.remove(&run_id);
            }
        });
        Ok(AiRunStarted {
            run_id: started_run_id,
        })
    }

    pub fn cancel_run(&self, run_id: &str) -> Result<()> {
        validate_identifier(run_id, "AI run ID")?;
        let run = mutex_lock(&self.runs, "AI run registry")?
            .get(run_id)
            .cloned()
            .ok_or_else(|| DbError::new("42704", "AI run is not active"))?;
        run.cancellation.cancel();
        self.approvals.cancel_run(run_id)?;
        Ok(())
    }

    pub fn decide(&self, decision: AiApprovalDecision) -> Result<()> {
        self.approvals.decide(decision)
    }

    fn provider(&self, settings: &AiProviderSettings) -> Result<Arc<dyn AiProvider>> {
        match settings.kind {
            AiProviderKind::OpenAi => {
                let credential_id = settings.credential_id.as_deref().ok_or_else(|| {
                    DbError::new("22023", "OpenAI provider credential ID is required")
                })?;
                let stored = self.credentials.load(credential_id)?;
                Ok(Arc::new(OpenAiProvider::official(stored.password)?))
            }
            AiProviderKind::OpenAiCompatible => {
                let key = settings
                    .credential_id
                    .as_deref()
                    .map(|credential_id| self.credentials.load(credential_id))
                    .transpose()?
                    .map(|stored| stored.password);
                Ok(Arc::new(OpenAiProvider::compatible(
                    settings.endpoint.as_deref().ok_or_else(|| {
                        DbError::new("22023", "compatible provider endpoint is required")
                    })?,
                    key,
                )?))
            }
            AiProviderKind::Ollama => {
                Ok(Arc::new(OllamaProvider::new(settings.endpoint.as_deref())?))
            }
            AiProviderKind::Fake => Ok(Arc::new(FakeProvider::new(Vec::new()))),
        }
    }
}

#[derive(Clone)]
struct DesktopAiExecutor {
    dbms: Arc<DbmsRuntime>,
    console: Arc<ConsoleRuntime>,
    persistence: Arc<Mutex<AiPersistenceV1>>,
    policy: AiConnectionPolicy,
}

#[async_trait]
impl AiToolExecutor for DesktopAiExecutor {
    async fn authorize(
        &self,
        context: &AiToolExecutionContext,
        call: &ValidatedAiToolCall,
    ) -> Result<AiToolAuthorization> {
        let preview = tool_preview(call)?;
        let mode = authorization_mode(&self.policy, call)?;
        let impact_summary = match mode {
            AiToolExecutionMode::ReadOnly => {
                "Read bounded metadata or query results without changing database state".to_owned()
            }
            AiToolExecutionMode::Mutation => mutation_impact(call),
        };
        self.persist_audit(
            context,
            call,
            AiAuditStatus::Proposed,
            "tool proposed and validated",
        )?;
        if mode == AiToolExecutionMode::Mutation {
            self.persist_audit(
                context,
                call,
                AiAuditStatus::ApprovalRequired,
                "fresh payload-bound approval required",
            )?;
        }
        Ok(AiToolAuthorization {
            mode,
            preview,
            impact_summary,
        })
    }

    async fn inspect(
        &self,
        context: AiToolExecutionContext,
        call: ValidatedAiToolCall,
        limits: AiToolLimits,
        cancellation: CancellationToken,
    ) -> Result<AiToolOutput> {
        self.execute_audited(context, call, limits, cancellation, false)
            .await
    }

    async fn mutate(
        &self,
        context: AiToolExecutionContext,
        call: ApprovedAiToolCall,
        limits: AiToolLimits,
        cancellation: CancellationToken,
    ) -> Result<AiToolOutput> {
        self.persist_audit(
            &context,
            call.call(),
            AiAuditStatus::Approved,
            "user approved the exact operation payload",
        )?;
        self.execute_audited(context, call.call().clone(), limits, cancellation, true)
            .await
    }

    async fn cancel_run(&self, _run_id: &str) -> Result<()> {
        Ok(())
    }
}

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

fn required_string<'a>(call: &'a ValidatedAiToolCall, field: &str) -> Result<&'a str> {
    let value = call
        .arguments()
        .get(field)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| DbError::new("22023", format!("AI tool field {field} must be a string")))?;
    validate_text(value, 1024 * 1024, &format!("AI tool field {field}"))?;
    Ok(value)
}

fn required_string_allow_empty<'a>(
    call: &'a ValidatedAiToolCall,
    field: &str,
    maximum: usize,
) -> Result<&'a str> {
    let value = call
        .arguments()
        .get(field)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| DbError::new("22023", format!("AI tool field {field} must be a string")))?;
    if value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(DbError::new(
            "22023",
            format!("AI tool field {field} is invalid"),
        ));
    }
    Ok(value)
}

fn required_usize(call: &ValidatedAiToolCall, field: &str) -> Result<usize> {
    let value = call
        .arguments()
        .get(field)
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| DbError::new("22023", format!("AI tool field {field} must be positive")))?;
    usize::try_from(value).map_err(|_| DbError::new("22023", "AI integer is too large"))
}

fn parse_transfer_format(value: &str) -> Result<AdministrationTransferFormat> {
    match value {
        "csv" => Ok(AdministrationTransferFormat::Csv),
        "jsonLines" => Ok(AdministrationTransferFormat::JsonLines),
        _ => Err(DbError::new(
            "22023",
            "transfer format must be csv or jsonLines",
        )),
    }
}

async fn choose_operation_file(
    root: &Path,
    kind: AdministrationOperationKind,
    format: Option<AdministrationTransferFormat>,
) -> Result<PathBuf> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut dialog = rfd::FileDialog::new().set_directory(&root);
        dialog = match (kind, format) {
            (AdministrationOperationKind::Backup, _) => dialog
                .add_filter("OrdaDB backup", &["ordbak"])
                .set_file_name("ordadb-backup.ordbak"),
            (AdministrationOperationKind::Restore, _) => {
                dialog.add_filter("OrdaDB backup", &["ordbak"])
            }
            (
                AdministrationOperationKind::Import | AdministrationOperationKind::Export,
                Some(AdministrationTransferFormat::Csv),
            ) => dialog
                .add_filter("CSV", &["csv"])
                .set_file_name("ordadb-data.csv"),
            (
                AdministrationOperationKind::Import | AdministrationOperationKind::Export,
                Some(AdministrationTransferFormat::JsonLines),
            ) => dialog
                .add_filter("JSON Lines", &["jsonl", "ndjson"])
                .set_file_name("ordadb-data.jsonl"),
            _ => dialog,
        };
        let selected = if matches!(
            kind,
            AdministrationOperationKind::Restore | AdministrationOperationKind::Import
        ) {
            dialog.pick_file()
        } else {
            dialog.save_file()
        };
        selected.ok_or_else(|| DbError::new("57014", "native file selection was cancelled"))
    })
    .await
    .map_err(join_error)?
}

fn relative_operation_path(
    root: &Path,
    selected: &Path,
    kind: AdministrationOperationKind,
) -> Result<String> {
    let root = fs::canonicalize(root)
        .map_err(|error| io_error("failed to resolve operations root", error))?;
    let selected = if matches!(
        kind,
        AdministrationOperationKind::Restore | AdministrationOperationKind::Import
    ) {
        fs::canonicalize(selected)
            .map_err(|error| io_error("failed to resolve selected operation source", error))?
    } else {
        let parent = selected
            .parent()
            .ok_or_else(|| DbError::new("22023", "selected destination has no parent"))?;
        fs::canonicalize(parent)
            .map_err(|error| io_error("failed to resolve selected destination directory", error))?
            .join(
                selected.file_name().ok_or_else(|| {
                    DbError::new("22023", "selected destination has no file name")
                })?,
            )
    };
    let relative = selected.strip_prefix(&root).map_err(|_| {
        DbError::new(
            "22023",
            "selected operation file must stay within the server operations root",
        )
        .with_hint(format!("choose a file under {}", root.display()))
    })?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(DbError::new("22023", "selected operation path is invalid"));
    }
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn local_output(content: JsonValue, summary: impl Into<String>) -> Result<AiToolOutput> {
    let bytes_retained = encoded_len(&content)?;
    Ok(AiToolOutput {
        content,
        rows_retained: 0,
        total_rows: 0,
        bytes_retained,
        truncated: false,
        summary: bounded_text(&summary.into(), 8 * 1024),
        disclosure: None,
    })
}

fn append_history(
    console: &ConsoleRuntime,
    persistence: &Mutex<AiPersistenceV1>,
    entry: AiHistoryEntry,
) -> Result<()> {
    update_persistence(console, persistence, |state| state.history.push(entry))
}

fn update_persistence(
    console: &ConsoleRuntime,
    persistence: &Mutex<AiPersistenceV1>,
    update: impl FnOnce(&mut AiPersistenceV1),
) -> Result<()> {
    let mut state = mutex_lock(persistence, "AI persistence")?;
    let mut candidate = state.clone();
    update(&mut candidate);
    let candidate = project_persistence(candidate.history, candidate.audit)?;
    console.save_ai_state(&candidate)?;
    *state = candidate;
    Ok(())
}

fn sensitive_columns(columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .filter(|column| {
            let normalized = column.to_ascii_lowercase();
            [
                "password",
                "secret",
                "token",
                "credential",
                "api_key",
                "apikey",
            ]
            .iter()
            .any(|marker| normalized.contains(marker))
        })
        .cloned()
        .collect()
}

fn mask_positional_columns(
    mut content: JsonValue,
    columns: &[String],
    masked_columns: &[String],
) -> JsonValue {
    let masked = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            masked_columns
                .iter()
                .any(|masked| masked.eq_ignore_ascii_case(column))
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if let Some(items) = content.get_mut("items").and_then(JsonValue::as_array_mut) {
        for item in items {
            if let Some(row) = item.as_array_mut() {
                for index in &masked {
                    if let Some(value) = row.get_mut(*index)
                        && !value.is_null()
                    {
                        *value = JsonValue::String("<redacted>".to_owned());
                    }
                }
            }
        }
    }
    content
}

fn ensure_approved(approved: bool) -> Result<()> {
    if approved {
        Ok(())
    } else {
        Err(DbError::new(
            "42501",
            "AI mutation requires a fresh payload-bound approval",
        ))
    }
}

fn bounded_history_text(value: &str) -> String {
    bounded_text(value, MAX_HISTORY_TEXT_BYTES)
}

fn bounded_text(value: &str, maximum: usize) -> String {
    let mut end = value.len().min(maximum);
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value[..end]
        .chars()
        .map(|character| {
            if character.is_control() && !matches!(character, '\n' | '\t') {
                ' '
            } else {
                character
            }
        })
        .collect()
}

fn encoded_len(value: &JsonValue) -> Result<usize> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(json_error)
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn validate_identifier(value: &str, context: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(DbError::new("22023", format!("{context} is invalid")));
    }
    Ok(())
}

fn validate_text(value: &str, maximum: usize, context: &str) -> Result<()> {
    if value.is_empty() || value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(DbError::new("22023", format!("{context} is invalid")));
    }
    Ok(())
}

fn mutex_lock<'a, T>(mutex: &'a Mutex<T>, context: &str) -> Result<MutexGuard<'a, T>> {
    mutex
        .lock()
        .map_err(|_| DbError::new("XX000", format!("{context} lock was poisoned")))
}

fn join_error(error: tokio::task::JoinError) -> DbError {
    DbError::new("XX000", "AI desktop task failed").with_detail(error.to_string())
}

fn json_error(error: serde_json::Error) -> DbError {
    DbError::new("XX000", "failed to encode AI desktop projection").with_detail(error.to_string())
}

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[tauri::command(rename_all = "camelCase")]
pub fn ai_start_run(
    app: AppHandle,
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    request: AiRunRequest,
) -> std::result::Result<AiRunStarted, DbmsError> {
    runtime.start_run(app, request).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn ai_cancel_run(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    run_id: String,
) -> std::result::Result<(), DbmsError> {
    runtime.cancel_run(&run_id).map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn ai_decide(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    decision: AiApprovalDecision,
) -> std::result::Result<(), DbmsError> {
    runtime.decide(decision).map_err(Into::into)
}

#[tauri::command]
pub fn ai_state(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
) -> std::result::Result<AiPersistenceV1, DbmsError> {
    runtime.state().map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn ai_prompt_credential(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    request: AiCredentialPromptRequest,
) -> std::result::Result<Option<AiCredentialStatus>, DbmsError> {
    runtime.prompt_credential(request).await.map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn ai_credential_status(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    credential_id: String,
) -> std::result::Result<AiCredentialStatus, DbmsError> {
    runtime
        .credential_status(&credential_id)
        .map_err(Into::into)
}

#[tauri::command(rename_all = "camelCase")]
pub fn ai_delete_credential(
    runtime: State<'_, Arc<DesktopAiRuntime>>,
    credential_id: String,
) -> std::result::Result<(), DbmsError> {
    runtime
        .delete_credential(&credential_id)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use ordadb_ai::{AiToolCall, validate_tool_arguments};

    use super::*;

    #[test]
    fn fixed_tool_registry_is_strict_and_complete() {
        let registry = tool_registry().expect("registry");
        let definitions = registry.definitions();
        assert_eq!(definitions.len(), 14);
        assert!(definitions.iter().all(|definition| {
            definition.parameters["additionalProperties"] == JsonValue::Bool(false)
        }));
        let query = definitions
            .iter()
            .find(|definition| definition.name == TOOL_QUERY)
            .expect("query tool");
        assert!(
            validate_tool_arguments(
                query,
                AiToolCall {
                    call_id: "call-1".to_owned(),
                    name: TOOL_QUERY.to_owned(),
                    arguments: json!({"command": "SELECT 1", "extra": true}),
                },
            )
            .is_err()
        );
    }

    #[test]
    fn automatic_sql_reads_fail_closed_for_access_and_effect() {
        let native = AiConnectionPolicy {
            connector_kind: "sql".to_owned(),
            command_language: "postgresql".to_owned(),
            credential_access: CredentialAccess::ReadWrite,
            native: true,
        };
        assert!(command_is_auto_read(&native, "SELECT 1"));
        assert!(!command_is_auto_read(&native, "DELETE FROM public.items"));

        let external = AiConnectionPolicy {
            native: false,
            credential_access: CredentialAccess::Unspecified,
            ..native.clone()
        };
        assert!(!command_is_auto_read(&external, "SELECT 1"));
        let read_only = AiConnectionPolicy {
            credential_access: CredentialAccess::ReadOnly,
            ..external
        };
        assert!(command_is_auto_read(&read_only, "SELECT 1"));
        assert!(!command_is_auto_read(
            &read_only,
            "SELECT nextval('items_id_seq')"
        ));
    }

    #[test]
    fn redis_like_arguments_are_parsed_without_invoking_a_shell() {
        assert_eq!(
            split_command_arguments(r#"SET "display name" "Ada Lovelace""#).expect("arguments"),
            vec!["SET", "display name", "Ada Lovelace"]
        );
        assert!(split_command_arguments("SET 'unterminated").is_err());
    }

    #[test]
    fn operation_paths_must_remain_under_the_server_root() {
        let root = tempfile::tempdir().expect("root");
        let inside = root.path().join("backup.ordbak");
        assert_eq!(
            relative_operation_path(root.path(), &inside, AdministrationOperationKind::Backup)
                .expect("relative"),
            "backup.ordbak"
        );
        let outside = tempfile::tempdir().expect("outside");
        assert!(
            relative_operation_path(
                root.path(),
                &outside.path().join("backup.ordbak"),
                AdministrationOperationKind::Backup,
            )
            .is_err()
        );
    }
}
