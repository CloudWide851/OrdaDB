//! Shared, UI-independent safety and provider kernel for OrdaDB AI clients.

mod approval;
mod contracts;
mod projection;
mod provider;
mod runner;
mod schema;

pub use approval::{
    AiApprovalBinding, AiApprovalBroker, AiApprovalClock, AiApprovalEntropy, SystemApprovalClock,
    UuidApprovalEntropy,
};
pub use contracts::{
    AiApprovalDecision, AiApprovalRequest, AiAuditEntry, AiAuditStatus, AiContextDisclosure,
    AiDataSharingPolicy, AiError, AiHistoryEntry, AiHistoryRole, AiProviderEvent, AiProviderInput,
    AiProviderKind, AiProviderRequest, AiProviderSettings, AiReasoningEffort, AiRunEvent,
    AiRunEventPayload, AiRunRequest, AiToolAuthorization, AiToolCall, AiToolDefinition,
    AiToolExecutionContext, AiToolExecutionMode, AiToolLimits, AiToolOutput, AiToolRisk, AiUsage,
    ApprovedAiToolCall, DEFAULT_OPENAI_MODEL, MAX_CONCURRENT_READ_TOOLS, MAX_QUERY_MEMORY_BYTES,
    MAX_TOOL_CALLS_PER_RUN, MAX_TOOL_DURATION_MS, MAX_TOOL_RESULT_BYTES, MAX_TOOL_ROWS,
    ValidatedAiToolCall,
};
pub use projection::{
    AI_PERSISTENCE_VERSION, AiPersistenceV1, AiRedactionPolicy, MAX_PERSISTED_AUDIT_ENTRIES,
    MAX_PERSISTED_HISTORY_ENTRIES, MAX_PERSISTED_STATE_BYTES, authorize_context_disclosure,
    decode_persistence, project_audit_entry, project_persistence, redact_sample,
    validate_context_disclosure,
};
pub use provider::{
    AiProvider, AiProviderEventSink, FakeProvider, OllamaProvider, OpenAiProvider,
    RecordingProviderSink, anonymous_safety_identifier, build_ollama_request,
    build_openai_responses_request, validate_provider_settings,
};
pub use runner::{
    AiRunEngine, AiRunEventSink, AiToolExecutor, AiToolRegistry, RecordingRunEventSink,
};
pub use schema::{
    canonical_json, canonical_json_hash, validate_tool_arguments, validate_tool_definition,
};
