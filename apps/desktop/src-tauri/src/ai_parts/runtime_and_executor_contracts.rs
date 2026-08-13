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
