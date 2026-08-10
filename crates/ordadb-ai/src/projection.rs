use std::collections::BTreeSet;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ordadb_types::{DbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    AiAuditEntry, AiAuditStatus, AiContextDisclosure, AiDataSharingPolicy, AiHistoryEntry,
    ValidatedAiToolCall, canonical_json_hash,
};

pub const AI_PERSISTENCE_VERSION: u32 = 1;
pub const MAX_PERSISTED_HISTORY_ENTRIES: usize = 64;
pub const MAX_PERSISTED_AUDIT_ENTRIES: usize = 256;
pub const MAX_PERSISTED_STATE_BYTES: usize = 2 * 1024 * 1024;

const MAX_HISTORY_TEXT_BYTES: usize = 64 * 1024;
const MAX_HISTORY_TOTAL_BYTES: usize = 1024 * 1024;
const MAX_AUDIT_SUMMARY_BYTES: usize = 8 * 1024;
const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_DISCLOSURE_CATEGORIES: usize = 32;
const MAX_DISCLOSURE_COLUMNS: usize = 256;
const MAX_DISCLOSURE_LABEL_BYTES: usize = 512;
const MAX_REDACTION_COLUMNS: usize = 256;
const MAX_REDACTION_DEPTH: usize = 32;
const MAX_REDACTION_NODES: usize = 65_536;
const DEFAULT_MAX_STRING_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiPersistenceV1 {
    pub version: u32,
    pub history: Vec<AiHistoryEntry>,
    pub audit: Vec<AiAuditEntry>,
}

impl Default for AiPersistenceV1 {
    fn default() -> Self {
        Self {
            version: AI_PERSISTENCE_VERSION,
            history: Vec::new(),
            audit: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiRedactionPolicy {
    #[serde(default)]
    pub masked_columns: Vec<String>,
    pub max_string_bytes: usize,
}

impl Default for AiRedactionPolicy {
    fn default() -> Self {
        Self {
            masked_columns: Vec::new(),
            max_string_bytes: DEFAULT_MAX_STRING_BYTES,
        }
    }
}

pub fn authorize_context_disclosure(
    policy: AiDataSharingPolicy,
    disclosure: AiContextDisclosure,
    per_use_values_approved: bool,
) -> Result<AiContextDisclosure> {
    validate_context_disclosure(&disclosure)?;
    if !disclosure.values_included {
        return Ok(disclosure);
    }
    match policy {
        AiDataSharingPolicy::SchemaOnly => Err(DbError::new(
            "42501",
            "AI data-sharing policy does not permit database values",
        )),
        AiDataSharingPolicy::AskEachTime if !per_use_values_approved => Err(DbError::new(
            "42501",
            "AI database values require per-use disclosure approval",
        )),
        AiDataSharingPolicy::AskEachTime | AiDataSharingPolicy::AllowSamples => Ok(disclosure),
    }
}

pub fn validate_context_disclosure(disclosure: &AiContextDisclosure) -> Result<()> {
    if disclosure.categories.len() > MAX_DISCLOSURE_CATEGORIES {
        return Err(limit("AI disclosure exceeds the category limit"));
    }
    if disclosure.columns.len() > MAX_DISCLOSURE_COLUMNS {
        return Err(limit("AI disclosure exceeds the column limit"));
    }
    for category in &disclosure.categories {
        validate_text(
            category,
            MAX_DISCLOSURE_LABEL_BYTES,
            "AI disclosure category",
        )?;
    }
    for column in &disclosure.columns {
        validate_text(column, MAX_DISCLOSURE_LABEL_BYTES, "AI disclosure column")?;
    }
    validate_text(
        &disclosure.redaction_summary,
        MAX_AUDIT_SUMMARY_BYTES,
        "AI disclosure redaction summary",
    )?;
    if disclosure.estimated_bytes > MAX_PERSISTED_STATE_BYTES {
        return Err(limit("AI disclosure exceeds the byte limit"));
    }
    if disclosure.values_included && disclosure.item_count == 0 {
        return Err(invalid(
            "AI disclosure cannot include values when the item count is zero",
        ));
    }
    Ok(())
}

pub fn redact_sample(value: &JsonValue, policy: &AiRedactionPolicy) -> Result<JsonValue> {
    if policy.masked_columns.len() > MAX_REDACTION_COLUMNS {
        return Err(limit("AI redaction policy exceeds the column limit"));
    }
    if policy.max_string_bytes == 0 || policy.max_string_bytes > MAX_HISTORY_TEXT_BYTES {
        return Err(invalid(
            "AI redaction string limit is outside the safe range",
        ));
    }
    let mut masked = BTreeSet::new();
    for column in &policy.masked_columns {
        validate_text(column, MAX_DISCLOSURE_LABEL_BYTES, "AI redaction column")?;
        masked.insert(column.to_lowercase());
    }
    let mut nodes = 0_usize;
    redact_value(value, None, &masked, policy.max_string_bytes, 0, &mut nodes)
}

pub fn project_audit_entry(
    run_id: &str,
    call: &ValidatedAiToolCall,
    status: AiAuditStatus,
    summary: &str,
    created_at_ms: u64,
) -> Result<AiAuditEntry> {
    validate_identifier(run_id, MAX_IDENTIFIER_BYTES, "AI audit run ID")?;
    validate_text(summary, MAX_AUDIT_SUMMARY_BYTES, "AI audit summary")?;
    let argument_hash = URL_SAFE_NO_PAD.encode(canonical_json_hash(call.arguments())?);
    Ok(AiAuditEntry {
        run_id: run_id.to_owned(),
        tool_call_id: call.call_id().to_owned(),
        tool_name: call.definition().name.clone(),
        argument_hash,
        status,
        summary: summary.to_owned(),
        created_at_ms,
    })
}

pub fn project_persistence(
    history: impl IntoIterator<Item = AiHistoryEntry>,
    audit: impl IntoIterator<Item = AiAuditEntry>,
) -> Result<AiPersistenceV1> {
    let history = retain_history(history)?;
    let audit = retain_audit(audit)?;
    let mut projected = AiPersistenceV1 {
        version: AI_PERSISTENCE_VERSION,
        history,
        audit,
    };
    while encoded_len(&projected)? > MAX_PERSISTED_STATE_BYTES {
        if !projected.history.is_empty() {
            projected.history.remove(0);
        } else if !projected.audit.is_empty() {
            projected.audit.remove(0);
        } else {
            return Err(limit("AI persistence envelope exceeds the byte limit"));
        }
    }
    Ok(projected)
}

pub fn decode_persistence(bytes: &[u8]) -> Result<AiPersistenceV1> {
    if bytes.len() > MAX_PERSISTED_STATE_BYTES {
        return Err(limit("AI persistence envelope exceeds the byte limit"));
    }
    let decoded: AiPersistenceV1 = serde_json::from_slice(bytes)
        .map_err(|error| invalid(format!("AI persistence envelope is invalid: {error}")))?;
    if decoded.version != AI_PERSISTENCE_VERSION {
        return Err(DbError::new(
            "0A000",
            "AI persistence envelope version is not supported",
        ));
    }
    let projected = project_persistence(decoded.history.clone(), decoded.audit.clone())?;
    if projected != decoded {
        return Err(invalid("AI persistence envelope exceeds a retention limit"));
    }
    Ok(decoded)
}

fn retain_history(
    history: impl IntoIterator<Item = AiHistoryEntry>,
) -> Result<Vec<AiHistoryEntry>> {
    let entries = history.into_iter().collect::<Vec<_>>();
    let mut retained = Vec::new();
    let mut bytes = 0_usize;
    for entry in entries
        .into_iter()
        .rev()
        .take(MAX_PERSISTED_HISTORY_ENTRIES)
    {
        validate_text(
            &entry.text,
            MAX_HISTORY_TEXT_BYTES,
            "AI persisted history entry",
        )?;
        if bytes.saturating_add(entry.text.len()) > MAX_HISTORY_TOTAL_BYTES {
            continue;
        }
        bytes = bytes.saturating_add(entry.text.len());
        retained.push(entry);
    }
    retained.reverse();
    Ok(retained)
}

fn retain_audit(audit: impl IntoIterator<Item = AiAuditEntry>) -> Result<Vec<AiAuditEntry>> {
    let entries = audit.into_iter().collect::<Vec<_>>();
    let mut retained = Vec::new();
    for entry in entries.into_iter().rev().take(MAX_PERSISTED_AUDIT_ENTRIES) {
        validate_identifier(&entry.run_id, MAX_IDENTIFIER_BYTES, "AI audit run ID")?;
        validate_identifier(
            &entry.tool_call_id,
            MAX_IDENTIFIER_BYTES,
            "AI audit tool call ID",
        )?;
        validate_identifier(&entry.tool_name, MAX_IDENTIFIER_BYTES, "AI audit tool name")?;
        validate_hash(&entry.argument_hash)?;
        validate_text(&entry.summary, MAX_AUDIT_SUMMARY_BYTES, "AI audit summary")?;
        retained.push(entry);
    }
    retained.reverse();
    Ok(retained)
}

fn redact_value(
    value: &JsonValue,
    field: Option<&str>,
    masked: &BTreeSet<String>,
    max_string_bytes: usize,
    depth: usize,
    nodes: &mut usize,
) -> Result<JsonValue> {
    if depth > MAX_REDACTION_DEPTH {
        return Err(limit("AI sample exceeds the redaction depth limit"));
    }
    *nodes = nodes.saturating_add(1);
    if *nodes > MAX_REDACTION_NODES {
        return Err(limit("AI sample exceeds the redaction node limit"));
    }
    if field.is_some_and(|field| should_mask(field, masked)) {
        return Ok(JsonValue::String("<redacted>".to_owned()));
    }
    match value {
        JsonValue::String(value) => Ok(JsonValue::String(truncate_utf8(value, max_string_bytes))),
        JsonValue::Array(values) => values
            .iter()
            .map(|value| redact_value(value, None, masked, max_string_bytes, depth + 1, nodes))
            .collect(),
        JsonValue::Object(values) => values
            .iter()
            .map(|(key, value)| {
                redact_value(value, Some(key), masked, max_string_bytes, depth + 1, nodes)
                    .map(|value| (key.clone(), value))
            })
            .collect(),
        _ => Ok(value.clone()),
    }
}

fn should_mask(field: &str, masked: &BTreeSet<String>) -> bool {
    let field = field.to_lowercase();
    masked.contains(&field)
        || [
            "password",
            "passwd",
            "secret",
            "token",
            "api_key",
            "apikey",
            "credential",
        ]
        .iter()
        .any(|sensitive| field.contains(sensitive))
}

fn truncate_utf8(value: &str, maximum: usize) -> String {
    if value.len() <= maximum {
        return value.to_owned();
    }
    let mut end = maximum.saturating_sub(3).min(value.len());
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}...", &value[..end])
}

fn encoded_len(value: &AiPersistenceV1) -> Result<usize> {
    serde_json::to_vec(value)
        .map(|value| value.len())
        .map_err(|error| DbError::internal(format!("failed to encode AI persistence: {error}")))
}

fn validate_hash(value: &str) -> Result<()> {
    let decoded = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| invalid("AI audit argument hash is invalid"))?;
    if decoded.len() != 32 {
        return Err(invalid("AI audit argument hash is invalid"));
    }
    Ok(())
}

fn validate_identifier(value: &str, maximum: usize, context: &str) -> Result<()> {
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(invalid(format!("{context} is invalid")));
    }
    Ok(())
}

fn validate_text(value: &str, maximum: usize, context: &str) -> Result<()> {
    if value.len() > maximum || value.contains('\0') {
        return Err(limit(format!("{context} exceeds its limit")));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn limit(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        AiAuditStatus, AiContextDisclosure, AiDataSharingPolicy, AiHistoryEntry, AiHistoryRole,
        AiRedactionPolicy, AiToolCall, AiToolDefinition, AiToolRisk, authorize_context_disclosure,
        decode_persistence, project_audit_entry, project_persistence, redact_sample,
        validate_tool_arguments,
    };

    #[test]
    fn disclosure_values_follow_policy_and_per_use_approval() {
        let disclosure = AiContextDisclosure {
            categories: vec!["querySample".to_owned()],
            columns: vec!["email".to_owned()],
            item_count: 2,
            estimated_bytes: 128,
            redaction_summary: "email masked".to_owned(),
            values_included: true,
        };
        assert_eq!(
            authorize_context_disclosure(AiDataSharingPolicy::SchemaOnly, disclosure.clone(), true)
                .expect_err("schema-only must reject values")
                .sql_state,
            "42501"
        );
        assert_eq!(
            authorize_context_disclosure(
                AiDataSharingPolicy::AskEachTime,
                disclosure.clone(),
                false
            )
            .expect_err("per-use approval must be required")
            .sql_state,
            "42501"
        );
        assert_eq!(
            authorize_context_disclosure(AiDataSharingPolicy::AskEachTime, disclosure, true)
                .expect("approved disclosure")
                .item_count,
            2
        );
    }

    #[test]
    fn sample_redaction_masks_explicit_and_secret_like_fields_and_truncates() {
        let redacted = redact_sample(
            &json!({
                "email": "alice@example.test",
                "passwordHint": "canary-password",
                "note": "abcdefghij",
                "nested": {"api_key": "canary-api-key"}
            }),
            &AiRedactionPolicy {
                masked_columns: vec!["email".to_owned()],
                max_string_bytes: 8,
            },
        )
        .expect("redacted sample");
        let encoded = serde_json::to_string(&redacted).expect("encoded redaction");
        assert_eq!(redacted["email"], "<redacted>");
        assert_eq!(redacted["note"], "abcde...");
        assert!(!encoded.contains("canary-password"));
        assert!(!encoded.contains("canary-api-key"));
    }

    #[test]
    fn persistence_is_bounded_strict_and_contains_only_visible_projections() {
        let definition = AiToolDefinition {
            name: "execute_mutation".to_owned(),
            version: 1,
            description: "Execute one confirmed mutation".to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {"sql": {"type": "string"}, "password": {"type": "string"}},
                "required": ["sql", "password"],
                "additionalProperties": false
            }),
            risk: AiToolRisk::RequiresApproval,
        };
        let call = validate_tool_arguments(
            &definition,
            AiToolCall {
                call_id: "call-1".to_owned(),
                name: definition.name.clone(),
                arguments: json!({
                    "sql": "DELETE FROM items",
                    "password": "canary-database-password"
                }),
            },
        )
        .expect("validated call");
        let audit = project_audit_entry(
            "run-1",
            &call,
            AiAuditStatus::Completed,
            "one mutation completed",
            42,
        )
        .expect("audit projection");
        let history = (0..80)
            .map(|index| AiHistoryEntry {
                role: AiHistoryRole::Assistant,
                text: format!("visible-{index}"),
                created_at_ms: index,
            })
            .collect::<Vec<_>>();
        let projected = project_persistence(history, vec![audit]).expect("projection");
        assert_eq!(projected.history.len(), 64);
        assert_eq!(projected.history[0].text, "visible-16");
        let encoded = serde_json::to_vec(&projected).expect("encoded projection");
        assert!(!String::from_utf8_lossy(&encoded).contains("canary-database-password"));
        assert_eq!(decode_persistence(&encoded).expect("decoded"), projected);

        let mut unknown = serde_json::to_value(&projected).expect("projection JSON");
        unknown["approvalToken"] = json!("canary-approval-token");
        assert_eq!(
            decode_persistence(&serde_json::to_vec(&unknown).expect("unknown JSON"))
                .expect_err("unknown persistence fields must fail")
                .sql_state,
            "22023"
        );
    }
}
