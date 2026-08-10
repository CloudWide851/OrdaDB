use std::fmt;

use ordadb_types::DbError;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const DEFAULT_OPENAI_MODEL: &str = "gpt-5.6";
pub const MAX_TOOL_CALLS_PER_RUN: u32 = 16;
pub const MAX_CONCURRENT_READ_TOOLS: usize = 3;
pub const MAX_TOOL_DURATION_MS: u64 = 30_000;
pub const MAX_TOOL_ROWS: usize = 1_000;
pub const MAX_TOOL_RESULT_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_QUERY_MEMORY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AiProviderKind {
    OpenAi,
    OpenAiCompatible,
    Ollama,
    Fake,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AiReasoningEffort {
    Low,
    Medium,
    High,
}

impl AiReasoningEffort {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AiDataSharingPolicy {
    SchemaOnly,
    AskEachTime,
    AllowSamples,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiProviderSettings {
    pub kind: AiProviderKind,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub reasoning: AiReasoningEffort,
    pub data_sharing: AiDataSharingPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
}

impl Default for AiProviderSettings {
    fn default() -> Self {
        Self {
            kind: AiProviderKind::OpenAi,
            model: DEFAULT_OPENAI_MODEL.to_owned(),
            endpoint: None,
            reasoning: AiReasoningEffort::Medium,
            data_sharing: AiDataSharingPolicy::SchemaOnly,
            credential_id: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AiToolRisk {
    Local,
    ReadOnly,
    RequiresApproval,
    Dynamic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AiToolExecutionMode {
    ReadOnly,
    Mutation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiToolAuthorization {
    pub mode: AiToolExecutionMode,
    pub preview: String,
    pub impact_summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiToolExecutionContext {
    pub run_id: String,
    pub connection_id: String,
    pub data_sharing: AiDataSharingPolicy,
    pub include_sample_values: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiToolDefinition {
    pub name: String,
    pub version: u32,
    pub description: String,
    pub parameters: JsonValue,
    pub risk: AiToolRisk,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiToolCall {
    pub call_id: String,
    pub name: String,
    pub arguments: JsonValue,
}

#[derive(Clone, PartialEq)]
pub struct ValidatedAiToolCall {
    call_id: String,
    definition: AiToolDefinition,
    arguments: JsonValue,
    canonical_arguments: String,
}

impl ValidatedAiToolCall {
    pub(crate) fn new(
        call_id: String,
        definition: AiToolDefinition,
        arguments: JsonValue,
        canonical_arguments: String,
    ) -> Self {
        Self {
            call_id,
            definition,
            arguments,
            canonical_arguments,
        }
    }

    #[must_use]
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    #[must_use]
    pub fn definition(&self) -> &AiToolDefinition {
        &self.definition
    }

    #[must_use]
    pub fn arguments(&self) -> &JsonValue {
        &self.arguments
    }

    #[must_use]
    pub fn canonical_arguments(&self) -> &str {
        &self.canonical_arguments
    }
}

impl fmt::Debug for ValidatedAiToolCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedAiToolCall")
            .field("call_id", &self.call_id)
            .field("tool", &self.definition.name)
            .field("arguments", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct ApprovedAiToolCall {
    call: ValidatedAiToolCall,
    approval_id: String,
}

impl ApprovedAiToolCall {
    pub(crate) fn new(call: ValidatedAiToolCall, approval_id: String) -> Self {
        Self { call, approval_id }
    }

    #[must_use]
    pub fn call(&self) -> &ValidatedAiToolCall {
        &self.call
    }

    #[must_use]
    pub fn approval_id(&self) -> &str {
        &self.approval_id
    }
}

impl fmt::Debug for ApprovedAiToolCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApprovedAiToolCall")
            .field("call", &self.call)
            .field("approval_id", &"<opaque>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiToolLimits {
    pub timeout_ms: u64,
    pub max_rows: usize,
    pub max_result_bytes: usize,
    pub query_memory_bytes: usize,
}

impl Default for AiToolLimits {
    fn default() -> Self {
        Self {
            timeout_ms: MAX_TOOL_DURATION_MS,
            max_rows: MAX_TOOL_ROWS,
            max_result_bytes: MAX_TOOL_RESULT_BYTES,
            query_memory_bytes: MAX_QUERY_MEMORY_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiToolOutput {
    pub content: JsonValue,
    pub rows_retained: usize,
    pub total_rows: usize,
    pub bytes_retained: usize,
    pub truncated: bool,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disclosure: Option<AiContextDisclosure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiContextDisclosure {
    pub categories: Vec<String>,
    pub columns: Vec<String>,
    pub item_count: usize,
    pub estimated_bytes: usize,
    pub redaction_summary: String,
    pub values_included: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiApprovalRequest {
    pub approval_id: String,
    pub expires_in_ms: u64,
    pub connection_id: String,
    pub tool_name: String,
    pub preview: String,
    pub impact_summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiApprovalDecision {
    pub approval_id: String,
    pub approve: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum AiProviderInput {
    Message {
        role: String,
        text: String,
    },
    FunctionCall {
        call_id: String,
        name: String,
        arguments: JsonValue,
    },
    FunctionOutput {
        call_id: String,
        output: JsonValue,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiProviderRequest {
    pub model: String,
    pub reasoning: AiReasoningEffort,
    pub safety_identifier: String,
    pub input: Vec<AiProviderInput>,
    pub tools: Vec<AiToolDefinition>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AiProviderEvent {
    TextDelta(String),
    ToolCall(AiToolCall),
    Usage(AiUsage),
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiRunRequest {
    pub run_id: String,
    pub connection_id: String,
    pub user_text: String,
    pub settings: AiProviderSettings,
    #[serde(default)]
    pub history: Vec<AiHistoryEntry>,
    #[serde(default)]
    pub include_sample_values: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AiHistoryRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiHistoryEntry {
    pub role: AiHistoryRole,
    pub text: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiAuditEntry {
    pub run_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub argument_hash: String,
    pub status: AiAuditStatus,
    pub summary: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AiAuditStatus {
    Proposed,
    ApprovalRequired,
    Approved,
    Denied,
    Started,
    Completed,
    Cancelled,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiError {
    pub sql_state: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<usize>,
    pub query_id: String,
}

impl From<DbError> for AiError {
    fn from(error: DbError) -> Self {
        Self {
            sql_state: error.sql_state,
            message: error.message,
            detail: error.detail.map(Into::into),
            hint: error.hint.map(Into::into),
            position: error.position,
            query_id: error.query_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiRunEvent {
    pub run_id: String,
    pub sequence: u64,
    #[serde(flatten)]
    pub payload: AiRunEventPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum AiRunEventPayload {
    Started,
    TextDelta {
        delta: String,
    },
    ContextDisclosure {
        disclosure: AiContextDisclosure,
    },
    ToolProposed {
        call_id: String,
        tool_name: String,
    },
    ToolStarted {
        call_id: String,
        tool_name: String,
    },
    ToolCompleted {
        call_id: String,
        tool_name: String,
        summary: String,
        truncated: bool,
    },
    ApprovalRequired {
        request: AiApprovalRequest,
    },
    ApprovalResolved {
        approval_id: String,
        approved: bool,
    },
    Usage {
        usage: AiUsage,
    },
    Cancelled,
    Completed,
    Error {
        error: AiError,
    },
}

impl AiRunEventPayload {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Cancelled | Self::Completed | Self::Error { .. })
    }
}
