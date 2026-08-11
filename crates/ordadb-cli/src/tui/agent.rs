use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use ordadb_ai::{
    AiApprovalBroker, AiAuditEntry, AiAuditStatus, AiContextDisclosure, AiDataSharingPolicy,
    AiProvider, AiProviderEvent, AiProviderKind, AiProviderSettings, AiRunEngine,
    AiToolAuthorization, AiToolDefinition, AiToolExecutionContext, AiToolExecutionMode,
    AiToolExecutor, AiToolLimits, AiToolOutput, AiToolRegistry, AiToolRisk, ApprovedAiToolCall,
    FakeProvider, OllamaProvider, OpenAiProvider, ValidatedAiToolCall, anonymous_safety_identifier,
    canonical_json_hash,
};
use ordadb_sql::{StatementEffect, classify_statement_effect, parse};
use ordadb_types::{DbError, Result};
use ordadb_windows::CredentialVault;
use serde_json::{Value as JsonValue, json};
use tokio_util::sync::CancellationToken;

use super::app::unix_time_millis;
use super::native::{NativeExecutor, NativeQueryResult};

const TOOL_CATALOG: &str = "catalog";
const TOOL_EXPLAIN: &str = "explain";
const TOOL_QUERY: &str = "query";
const TOOL_VALIDATE_SQL: &str = "validate_sql";
const TOOL_EXECUTE_SQL: &str = "execute_sql";
const MAX_PREVIEW_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub struct TuiAgentKernel {
    executor: Arc<TuiToolExecutor>,
    approvals: Arc<AiApprovalBroker>,
    provider_credentials: CredentialVault,
    safety_identifier: String,
}

impl TuiAgentKernel {
    pub fn new(native: NativeExecutor) -> Result<Self> {
        let safety_identifier = anonymous_safety_identifier(native.connection_id().as_bytes())?;
        Ok(Self {
            executor: Arc::new(TuiToolExecutor::new(native)),
            approvals: Arc::new(AiApprovalBroker::default()),
            provider_credentials: CredentialVault::new("OrdaDB/AI")?,
            safety_identifier,
        })
    }

    pub fn natural_language_engine(
        &self,
        settings: &AiProviderSettings,
    ) -> Result<Arc<AiRunEngine>> {
        self.engine(self.provider(settings)?)
    }

    pub fn sql_engine(&self, sql: &str) -> Result<Arc<AiRunEngine>> {
        let statement = parse(sql)?;
        let tool_name = if classify_statement_effect(&statement) == StatementEffect::ReadOnly {
            TOOL_QUERY
        } else {
            TOOL_EXECUTE_SQL
        };
        let provider = FakeProvider::new(vec![
            vec![
                AiProviderEvent::ToolCall(ordadb_ai::AiToolCall {
                    call_id: "sql-mode-call".to_owned(),
                    name: tool_name.to_owned(),
                    arguments: json!({"command": sql}),
                }),
                AiProviderEvent::Completed,
            ],
            vec![
                AiProviderEvent::TextDelta("本地 SQL 执行已完成。".to_owned()),
                AiProviderEvent::Completed,
            ],
        ]);
        self.engine(Arc::new(provider))
    }

    pub fn take_result(&self) -> Result<Option<NativeQueryResult>> {
        Ok(mutex_lock(&self.executor.latest_result, "TUI latest result")?.take())
    }

    pub fn take_preview(&self) -> Result<Option<String>> {
        Ok(mutex_lock(&self.executor.latest_preview, "TUI command preview")?.take())
    }

    pub fn audit(&self) -> Result<Vec<AiAuditEntry>> {
        Ok(mutex_lock(&self.executor.audit, "TUI AI audit")?.clone())
    }

    fn engine(&self, provider: Arc<dyn AiProvider>) -> Result<Arc<AiRunEngine>> {
        Ok(Arc::new(AiRunEngine::new(
            provider,
            Arc::clone(&self.executor) as Arc<dyn AiToolExecutor>,
            tool_registry()?,
            Arc::clone(&self.approvals),
            self.safety_identifier.clone(),
        )?))
    }

    fn provider(&self, settings: &AiProviderSettings) -> Result<Arc<dyn AiProvider>> {
        match settings.kind {
            AiProviderKind::OpenAi => {
                let credential_id = settings
                    .credential_id
                    .as_deref()
                    .ok_or_else(|| invalid("OpenAI provider credential ID is required"))?;
                Ok(Arc::new(OpenAiProvider::official(
                    self.provider_credentials.load(credential_id)?.password,
                )?))
            }
            AiProviderKind::OpenAiCompatible => {
                let api_key = settings
                    .credential_id
                    .as_deref()
                    .map(|credential_id| self.provider_credentials.load(credential_id))
                    .transpose()?
                    .map(|stored| stored.password);
                Ok(Arc::new(OpenAiProvider::compatible(
                    settings
                        .endpoint
                        .as_deref()
                        .ok_or_else(|| invalid("compatible provider endpoint is required"))?,
                    api_key,
                )?))
            }
            AiProviderKind::Ollama => {
                Ok(Arc::new(OllamaProvider::new(settings.endpoint.as_deref())?))
            }
            AiProviderKind::Fake => Ok(Arc::new(FakeProvider::new(vec![vec![
                AiProviderEvent::TextDelta(
                    "Fake provider 已启用；该模式只返回确定性本地响应。".to_owned(),
                ),
                AiProviderEvent::Completed,
            ]]))),
        }
    }
}

struct TuiToolExecutor {
    native: NativeExecutor,
    latest_result: Mutex<Option<NativeQueryResult>>,
    latest_preview: Mutex<Option<String>>,
    audit: Mutex<Vec<AiAuditEntry>>,
}

impl TuiToolExecutor {
    fn new(native: NativeExecutor) -> Self {
        Self {
            native,
            latest_result: Mutex::new(None),
            latest_preview: Mutex::new(None),
            audit: Mutex::new(Vec::new()),
        }
    }

    async fn execute_call(
        &self,
        context: &AiToolExecutionContext,
        call: &ValidatedAiToolCall,
        limits: AiToolLimits,
        cancellation: CancellationToken,
        approved: bool,
    ) -> Result<AiToolOutput> {
        match call.definition().name.as_str() {
            TOOL_VALIDATE_SQL => validate_sql(call),
            TOOL_CATALOG => {
                let limit = required_u64(call, "limit")?.clamp(1, 1_000);
                let filter = required_string(call, "filter", 512)?.to_ascii_lowercase();
                let command = format!(
                    "SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY table_schema, table_name LIMIT {limit}"
                );
                let mut result = self
                    .native
                    .execute(command, true, limits, cancellation)
                    .await?;
                if !filter.is_empty() {
                    result.rows.retain(|row| {
                        row.iter()
                            .flatten()
                            .any(|value| value.to_ascii_lowercase().contains(&filter))
                    });
                    result.total_rows = result.rows.len();
                }
                self.output_result(context, result, true)
            }
            TOOL_EXPLAIN => {
                let command = required_command(call)?;
                ensure_read_only(&command)?;
                self.execute_query(
                    context,
                    format!("EXPLAIN {command}"),
                    limits,
                    cancellation,
                    true,
                    false,
                )
                .await
            }
            TOOL_QUERY => {
                let command = required_command(call)?;
                let read_only = ensure_read_only(&command).is_ok();
                if !read_only && !approved {
                    return Err(DbError::new(
                        "25006",
                        "mutation-capable query requires exact approval",
                    ));
                }
                self.execute_query(context, command, limits, cancellation, read_only, false)
                    .await
            }
            TOOL_EXECUTE_SQL => {
                if !approved {
                    return Err(DbError::new(
                        "42501",
                        "TUI mutation execution requires exact approval",
                    ));
                }
                let command = required_command(call)?;
                self.execute_query(context, command, limits, cancellation, false, false)
                    .await
            }
            _ => Err(invalid("AI tool is not registered for the TUI")),
        }
    }

    async fn execute_query(
        &self,
        context: &AiToolExecutionContext,
        command: String,
        limits: AiToolLimits,
        cancellation: CancellationToken,
        isolated_read: bool,
        catalog: bool,
    ) -> Result<AiToolOutput> {
        let result = self
            .native
            .execute(command, isolated_read, limits, cancellation)
            .await?;
        self.output_result(context, result, catalog)
    }

    fn output_result(
        &self,
        context: &AiToolExecutionContext,
        result: NativeQueryResult,
        catalog: bool,
    ) -> Result<AiToolOutput> {
        *mutex_lock(&self.latest_result, "TUI latest result")? = Some(result.clone());
        let include_values = context.include_sample_values
            && context.data_sharing == AiDataSharingPolicy::AllowSamples;
        let content = if include_values || catalog {
            json!({
                "columns": result.columns,
                "rows": result.rows,
                "totalRows": result.total_rows,
                "commandTags": result.command_tags,
            })
        } else {
            json!({
                "columns": result.columns,
                "totalRows": result.total_rows,
                "commandTags": result.command_tags,
                "valuesWithheld": true,
            })
        };
        let bytes_retained = serde_json::to_vec(&content).map_err(json_error)?.len();
        let rows_retained = if include_values || catalog {
            result.rows.len()
        } else {
            0
        };
        Ok(AiToolOutput {
            content,
            rows_retained,
            total_rows: result.total_rows,
            bytes_retained,
            truncated: result.truncated,
            summary: format!(
                "observed {} row(s); retained {} locally{}",
                result.total_rows,
                result.rows.len(),
                if include_values || catalog {
                    ""
                } else {
                    "; values withheld from provider"
                }
            ),
            disclosure: Some(AiContextDisclosure {
                categories: vec![if catalog {
                    "catalogMetadata".to_owned()
                } else if include_values {
                    "redactedResultSamples".to_owned()
                } else {
                    "resultShape".to_owned()
                }],
                columns: result.columns,
                item_count: if include_values || catalog {
                    result.rows.len()
                } else {
                    0
                },
                estimated_bytes: bytes_retained,
                redaction_summary: if include_values {
                    "bounded result samples allowed by the active TUI policy".to_owned()
                } else {
                    "database values remain local to the terminal".to_owned()
                },
                values_included: include_values,
            }),
        })
    }

    fn record(
        &self,
        context: &AiToolExecutionContext,
        call: &ValidatedAiToolCall,
        status: AiAuditStatus,
        summary: impl Into<String>,
    ) -> Result<()> {
        let hash = canonical_json_hash(call.arguments())?;
        let argument_hash = hash.iter().map(|byte| format!("{byte:02x}")).collect();
        let mut audit = mutex_lock(&self.audit, "TUI AI audit")?;
        audit.push(AiAuditEntry {
            run_id: context.run_id.clone(),
            tool_call_id: call.call_id().to_owned(),
            tool_name: call.definition().name.clone(),
            argument_hash,
            status,
            summary: bounded(&summary.into(), 8 * 1024),
            created_at_ms: unix_time_millis(),
        });
        if audit.len() > 1_024 {
            let remove = audit.len() - 1_024;
            audit.drain(..remove);
        }
        Ok(())
    }
}

#[async_trait]
impl AiToolExecutor for TuiToolExecutor {
    async fn authorize(
        &self,
        context: &AiToolExecutionContext,
        call: &ValidatedAiToolCall,
    ) -> Result<AiToolAuthorization> {
        let mode = authorization_mode(call)?;
        let preview = preview(call)?;
        *mutex_lock(&self.latest_preview, "TUI command preview")? = Some(preview.clone());
        self.record(context, call, AiAuditStatus::Proposed, "tool proposed")?;
        if mode == AiToolExecutionMode::Mutation {
            self.record(
                context,
                call,
                AiAuditStatus::ApprovalRequired,
                "exact payload-bound approval required",
            )?;
        }
        Ok(AiToolAuthorization {
            mode,
            preview,
            impact_summary: if mode == AiToolExecutionMode::ReadOnly {
                "Run one bounded native OrdaDB read in an isolated read-only transaction".to_owned()
            } else {
                "Execute the exact displayed SQL under the connected user's permissions".to_owned()
            },
        })
    }

    async fn inspect(
        &self,
        context: AiToolExecutionContext,
        call: ValidatedAiToolCall,
        limits: AiToolLimits,
        cancellation: CancellationToken,
    ) -> Result<AiToolOutput> {
        self.record(&context, &call, AiAuditStatus::Started, "read started")?;
        let result = self
            .execute_call(&context, &call, limits, cancellation, false)
            .await;
        self.record(
            &context,
            &call,
            if result.is_ok() {
                AiAuditStatus::Completed
            } else {
                AiAuditStatus::Error
            },
            if result.is_ok() {
                "read completed"
            } else {
                "read failed"
            },
        )?;
        result
    }

    async fn mutate(
        &self,
        context: AiToolExecutionContext,
        call: ApprovedAiToolCall,
        limits: AiToolLimits,
        cancellation: CancellationToken,
    ) -> Result<AiToolOutput> {
        self.record(
            &context,
            call.call(),
            AiAuditStatus::Approved,
            "exact operation approved",
        )?;
        self.record(
            &context,
            call.call(),
            AiAuditStatus::Started,
            "mutation started",
        )?;
        let result = self
            .execute_call(&context, call.call(), limits, cancellation, true)
            .await;
        self.record(
            &context,
            call.call(),
            if result.is_ok() {
                AiAuditStatus::Completed
            } else {
                AiAuditStatus::Error
            },
            if result.is_ok() {
                "approved mutation completed"
            } else {
                "approved mutation failed"
            },
        )?;
        result
    }

    async fn cancel_run(&self, _run_id: &str) -> Result<()> {
        Ok(())
    }
}

fn tool_registry() -> Result<AiToolRegistry> {
    AiToolRegistry::new(vec![
        tool(
            TOOL_CATALOG,
            "Read bounded native OrdaDB catalog metadata",
            object_schema(vec![
                ("filter", string_schema()),
                ("limit", integer_schema()),
            ]),
            AiToolRisk::ReadOnly,
        ),
        tool(
            TOOL_EXPLAIN,
            "Explain one conservatively read-only SQL statement",
            command_schema(),
            AiToolRisk::Dynamic,
        ),
        tool(
            TOOL_QUERY,
            "Run one bounded SQL statement; mutations require approval",
            command_schema(),
            AiToolRisk::Dynamic,
        ),
        tool(
            TOOL_VALIDATE_SQL,
            "Parse SQL and report its conservative effect",
            command_schema(),
            AiToolRisk::Local,
        ),
        tool(
            TOOL_EXECUTE_SQL,
            "Execute one DML or DDL statement after exact approval",
            command_schema(),
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

fn object_schema(properties: Vec<(&str, JsonValue)>) -> JsonValue {
    let required = properties
        .iter()
        .map(|(name, _)| JsonValue::String((*name).to_owned()))
        .collect::<Vec<_>>();
    let properties = properties
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect::<serde_json::Map<_, _>>();
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn command_schema() -> JsonValue {
    object_schema(vec![("command", string_schema())])
}

fn string_schema() -> JsonValue {
    json!({"type": "string"})
}

fn integer_schema() -> JsonValue {
    json!({"type": "integer", "minimum": 1, "maximum": 1000})
}

fn authorization_mode(call: &ValidatedAiToolCall) -> Result<AiToolExecutionMode> {
    match call.definition().name.as_str() {
        TOOL_CATALOG | TOOL_VALIDATE_SQL => Ok(AiToolExecutionMode::ReadOnly),
        TOOL_EXECUTE_SQL => Ok(AiToolExecutionMode::Mutation),
        TOOL_EXPLAIN | TOOL_QUERY => Ok(if ensure_read_only(&required_command(call)?).is_ok() {
            AiToolExecutionMode::ReadOnly
        } else {
            AiToolExecutionMode::Mutation
        }),
        _ => Err(invalid("AI tool is not registered for the TUI")),
    }
}

fn validate_sql(call: &ValidatedAiToolCall) -> Result<AiToolOutput> {
    let command = required_command(call)?;
    let parsed = parse(&command);
    let content = match parsed {
        Ok(statement) => json!({
            "valid": true,
            "effect": classify_statement_effect(&statement),
        }),
        Err(error) => json!({
            "valid": false,
            "error": {"sqlState": error.sql_state, "message": error.message, "position": error.position},
        }),
    };
    let bytes_retained = serde_json::to_vec(&content).map_err(json_error)?.len();
    Ok(AiToolOutput {
        content,
        rows_retained: 0,
        total_rows: 0,
        bytes_retained,
        truncated: false,
        summary: "SQL validation completed".to_owned(),
        disclosure: None,
    })
}

fn ensure_read_only(sql: &str) -> Result<()> {
    if classify_statement_effect(&parse(sql)?) != StatementEffect::ReadOnly {
        return Err(DbError::new(
            "25006",
            "statement is not conservatively read-only",
        ));
    }
    Ok(())
}

fn preview(call: &ValidatedAiToolCall) -> Result<String> {
    let value = match call.definition().name.as_str() {
        TOOL_CATALOG => format!("catalog limit={}", required_u64(call, "limit")?),
        _ => required_command(call)?,
    };
    Ok(bounded(&value, MAX_PREVIEW_BYTES))
}

fn required_command(call: &ValidatedAiToolCall) -> Result<String> {
    let command = call
        .arguments()
        .get("command")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| invalid("AI tool command must be a string"))?;
    if command.is_empty() || command.len() > 1024 * 1024 || command.as_bytes().contains(&0) {
        return Err(invalid("AI tool command is invalid"));
    }
    Ok(command.to_owned())
}

fn required_u64(call: &ValidatedAiToolCall, field: &str) -> Result<u64> {
    call.arguments()
        .get(field)
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| invalid(format!("AI tool field {field} must be a positive integer")))
}

fn required_string<'a>(
    call: &'a ValidatedAiToolCall,
    field: &str,
    maximum: usize,
) -> Result<&'a str> {
    let value = call
        .arguments()
        .get(field)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| invalid(format!("AI tool field {field} must be a string")))?;
    if value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(invalid(format!("AI tool field {field} is invalid")));
    }
    Ok(value)
}

fn bounded(value: &str, maximum: usize) -> String {
    let mut output = String::new();
    for character in value.chars() {
        if output.len().saturating_add(character.len_utf8()) > maximum {
            break;
        }
        if character == '\n' || character == '\t' || !character.is_control() {
            output.push(character);
        }
    }
    output
}

fn mutex_lock<'a, T>(mutex: &'a Mutex<T>, label: &str) -> Result<MutexGuard<'a, T>> {
    mutex
        .lock()
        .map_err(|_| DbError::new("XX000", format!("{label} is poisoned")))
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn json_error(error: serde_json::Error) -> DbError {
    DbError::new("XX000", "failed to encode TUI AI result").with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_tui_registry_exposes_only_the_five_native_tools() {
        let definitions = tool_registry().expect("registry").definitions();
        assert_eq!(definitions.len(), 5);
        assert!(definitions.iter().all(|definition| {
            definition.parameters["additionalProperties"] == JsonValue::Bool(false)
        }));
    }
}
