use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use ordadb_types::{DbError, Result};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{AiApprovalDecision, AiApprovalRequest, ApprovedAiToolCall, ValidatedAiToolCall};

const APPROVAL_TTL_MS: u64 = 120_000;
const MAX_PENDING_APPROVALS: usize = 64;

pub trait AiApprovalClock: Send + Sync {
    fn now_millis(&self) -> u64;
}

pub trait AiApprovalEntropy: Send + Sync {
    fn approval_id(&self) -> String;
}

pub struct SystemApprovalClock {
    started: Instant,
}

impl Default for SystemApprovalClock {
    fn default() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

impl AiApprovalClock for SystemApprovalClock {
    fn now_millis(&self) -> u64 {
        self.started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Default)]
pub struct UuidApprovalEntropy;

impl AiApprovalEntropy for UuidApprovalEntropy {
    fn approval_id(&self) -> String {
        Uuid::new_v4().to_string()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AiApprovalBinding {
    pub run_id: String,
    pub connection_id: String,
    pub tool_name: String,
    pub tool_version: u32,
    pub argument_hash: [u8; 32],
    pub impact_hash: [u8; 32],
}

impl AiApprovalBinding {
    #[must_use]
    pub fn new(
        run_id: impl Into<String>,
        connection_id: impl Into<String>,
        call: &ValidatedAiToolCall,
        impact_summary: &str,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            connection_id: connection_id.into(),
            tool_name: call.definition().name.clone(),
            tool_version: call.definition().version,
            argument_hash: Sha256::digest(call.canonical_arguments().as_bytes()).into(),
            impact_hash: Sha256::digest(impact_summary.as_bytes()).into(),
        }
    }
}

impl fmt::Debug for AiApprovalBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiApprovalBinding")
            .field("run_id", &self.run_id)
            .field("connection_id", &self.connection_id)
            .field("tool_name", &self.tool_name)
            .field("tool_version", &self.tool_version)
            .field("argument_hash", &"<hash>")
            .field("impact_hash", &"<hash>")
            .finish()
    }
}

struct PendingApproval {
    binding: AiApprovalBinding,
    call: ValidatedAiToolCall,
    expires_at_ms: u64,
    decision: Option<bool>,
}

pub struct AiApprovalBroker {
    clock: Arc<dyn AiApprovalClock>,
    entropy: Arc<dyn AiApprovalEntropy>,
    pending: Mutex<BTreeMap<String, PendingApproval>>,
    changed: Notify,
}

impl fmt::Debug for AiApprovalBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiApprovalBroker")
            .field("pending", &"<redacted>")
            .finish()
    }
}

impl Default for AiApprovalBroker {
    fn default() -> Self {
        Self::new(
            Arc::new(SystemApprovalClock::default()),
            Arc::new(UuidApprovalEntropy),
        )
    }
}

impl AiApprovalBroker {
    #[must_use]
    pub fn new(clock: Arc<dyn AiApprovalClock>, entropy: Arc<dyn AiApprovalEntropy>) -> Self {
        Self {
            clock,
            entropy,
            pending: Mutex::new(BTreeMap::new()),
            changed: Notify::new(),
        }
    }

    pub fn issue(
        &self,
        binding: AiApprovalBinding,
        call: ValidatedAiToolCall,
        preview: impl Into<String>,
        impact_summary: impl Into<String>,
    ) -> Result<AiApprovalRequest> {
        let impact_summary = impact_summary.into();
        let actual_argument_hash: [u8; 32] =
            Sha256::digest(call.canonical_arguments().as_bytes()).into();
        let actual_impact_hash: [u8; 32] = Sha256::digest(impact_summary.as_bytes()).into();
        if binding.tool_name != call.definition().name
            || binding.tool_version != call.definition().version
            || binding.argument_hash != actual_argument_hash
            || binding.impact_hash != actual_impact_hash
        {
            return Err(DbError::new(
                "55000",
                "AI approval binding does not match the proposed operation",
            ));
        }
        let now = self.clock.now_millis();
        let mut pending = mutex_lock(&self.pending)?;
        pending.retain(|_, record| record.expires_at_ms > now);
        if pending.len() >= MAX_PENDING_APPROVALS {
            return Err(DbError::new(
                "54000",
                "too many AI mutation approvals are pending",
            ));
        }
        let approval_id = self.entropy.approval_id();
        validate_approval_id(&approval_id)?;
        if pending.contains_key(&approval_id) {
            return Err(DbError::new(
                "XX000",
                "AI approval entropy produced a duplicate identifier",
            ));
        }
        let request = AiApprovalRequest {
            approval_id: approval_id.clone(),
            expires_in_ms: APPROVAL_TTL_MS,
            connection_id: binding.connection_id.clone(),
            tool_name: binding.tool_name.clone(),
            preview: bounded_text(preview.into(), 8 * 1024, "approval preview")?,
            impact_summary: bounded_text(impact_summary, 8 * 1024, "approval impact summary")?,
        };
        pending.insert(
            approval_id,
            PendingApproval {
                binding,
                call,
                expires_at_ms: now.saturating_add(APPROVAL_TTL_MS),
                decision: None,
            },
        );
        Ok(request)
    }

    pub fn decide(&self, decision: AiApprovalDecision) -> Result<()> {
        validate_approval_id(&decision.approval_id)?;
        let now = self.clock.now_millis();
        let mut pending = mutex_lock(&self.pending)?;
        let record = pending.get_mut(&decision.approval_id).ok_or_else(|| {
            DbError::new("55000", "AI approval is missing or was already consumed")
        })?;
        if record.expires_at_ms <= now {
            pending.remove(&decision.approval_id);
            return Err(DbError::new(
                "57014",
                "AI approval expired before it was decided",
            ));
        }
        if record.decision.is_some() {
            return Err(DbError::new("55000", "AI approval was already decided"));
        }
        record.decision = Some(decision.approve);
        drop(pending);
        self.changed.notify_waiters();
        Ok(())
    }

    pub async fn wait(
        &self,
        approval_id: &str,
        expected: &AiApprovalBinding,
        cancellation: &CancellationToken,
    ) -> Result<ApprovedAiToolCall> {
        validate_approval_id(approval_id)?;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            if let Some(result) = self.take_decided(approval_id, expected)? {
                return result;
            }
            tokio::select! {
                () = cancellation.cancelled() => {
                    self.remove(approval_id)?;
                    return Err(DbError::new("57014", "AI run was cancelled while awaiting approval"));
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(APPROVAL_TTL_MS)) => {
                    if let Some(result) = self.take_decided(approval_id, expected)? {
                        return result;
                    }
                    self.remove(approval_id)?;
                    return Err(DbError::new("57014", "AI approval expired before it was consumed"));
                }
                () = &mut notified => {}
            }
        }
    }

    pub fn resolve(
        &self,
        decision: AiApprovalDecision,
        expected: &AiApprovalBinding,
    ) -> Result<ApprovedAiToolCall> {
        validate_approval_id(&decision.approval_id)?;
        let record = mutex_lock(&self.pending)?
            .remove(&decision.approval_id)
            .ok_or_else(|| {
                DbError::new("55000", "AI approval is missing or was already consumed")
            })?;
        if record.expires_at_ms <= self.clock.now_millis() {
            return Err(DbError::new(
                "57014",
                "AI approval expired before it was consumed",
            ));
        }
        if record.binding != *expected {
            return Err(DbError::new(
                "55000",
                "AI approval no longer matches the connection or proposed operation",
            ));
        }
        if !decision.approve {
            return Err(DbError::new("57014", "AI mutation was denied by the user"));
        }
        Ok(ApprovedAiToolCall::new(record.call, decision.approval_id))
    }

    pub fn cancel_run(&self, run_id: &str) -> Result<usize> {
        let mut pending = mutex_lock(&self.pending)?;
        let before = pending.len();
        pending.retain(|_, record| record.binding.run_id != run_id);
        let removed = before.saturating_sub(pending.len());
        drop(pending);
        if removed > 0 {
            self.changed.notify_waiters();
        }
        Ok(removed)
    }

    pub fn pending_count(&self) -> Result<usize> {
        Ok(mutex_lock(&self.pending)?.len())
    }

    fn take_decided(
        &self,
        approval_id: &str,
        expected: &AiApprovalBinding,
    ) -> Result<Option<Result<ApprovedAiToolCall>>> {
        let now = self.clock.now_millis();
        let mut pending = mutex_lock(&self.pending)?;
        let Some(record) = pending.get(approval_id) else {
            return Ok(Some(Err(DbError::new(
                "55000",
                "AI approval is missing or was already consumed",
            ))));
        };
        if record.expires_at_ms <= now {
            pending.remove(approval_id);
            return Ok(Some(Err(DbError::new(
                "57014",
                "AI approval expired before it was consumed",
            ))));
        }
        let Some(approved) = record.decision else {
            return Ok(None);
        };
        let record = pending.remove(approval_id).ok_or_else(|| {
            DbError::new(
                "XX000",
                "AI approval disappeared during decision consumption",
            )
        })?;
        if record.binding != *expected {
            return Ok(Some(Err(DbError::new(
                "55000",
                "AI approval no longer matches the connection or proposed operation",
            ))));
        }
        if !approved {
            return Ok(Some(Err(DbError::new(
                "57014",
                "AI mutation was denied by the user",
            ))));
        }
        Ok(Some(Ok(ApprovedAiToolCall::new(
            record.call,
            approval_id.to_owned(),
        ))))
    }

    fn remove(&self, approval_id: &str) -> Result<()> {
        mutex_lock(&self.pending)?.remove(approval_id);
        Ok(())
    }
}

fn validate_approval_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(DbError::new("22023", "AI approval ID is invalid"));
    }
    Ok(())
}

fn bounded_text(value: String, maximum: usize, context: &str) -> Result<String> {
    if value.is_empty() || value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(DbError::new("22023", format!("AI {context} is invalid")));
    }
    Ok(value)
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| DbError::new("XX000", "AI approval state lock was poisoned"))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::json;

    use super::*;
    use crate::{AiToolCall, AiToolDefinition, AiToolRisk, validate_tool_arguments};

    #[derive(Default)]
    struct ManualClock(AtomicU64);

    impl ManualClock {
        fn advance(&self, millis: u64) {
            self.0.fetch_add(millis, Ordering::SeqCst);
        }
    }

    impl AiApprovalClock for ManualClock {
        fn now_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    struct FixedEntropy;

    impl AiApprovalEntropy for FixedEntropy {
        fn approval_id(&self) -> String {
            "approval-1".to_owned()
        }
    }

    fn call() -> ValidatedAiToolCall {
        let definition = AiToolDefinition {
            name: "execute_mutation".to_owned(),
            version: 1,
            description: "Execute one confirmed mutation".to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {"sql": {"type": "string"}},
                "required": ["sql"],
                "additionalProperties": false
            }),
            risk: AiToolRisk::RequiresApproval,
        };
        validate_tool_arguments(
            &definition,
            AiToolCall {
                call_id: "call-1".to_owned(),
                name: definition.name.clone(),
                arguments: json!({"sql": "DELETE FROM items"}),
            },
        )
        .expect("validated call")
    }

    #[test]
    fn approval_is_bound_consumed_once_and_debug_redacted() {
        let clock = Arc::new(ManualClock::default());
        let broker = AiApprovalBroker::new(clock, Arc::new(FixedEntropy));
        let call = call();
        let binding = AiApprovalBinding::new("run-1", "connection-1", &call, "delete rows");
        let request = broker
            .issue(binding.clone(), call, "DELETE FROM items", "delete rows")
            .expect("request");
        assert!(!format!("{binding:?}").contains("DELETE"));
        let approved = broker
            .resolve(
                AiApprovalDecision {
                    approval_id: request.approval_id.clone(),
                    approve: true,
                },
                &binding,
            )
            .expect("approved");
        assert_eq!(approved.call().definition().name, "execute_mutation");
        assert_eq!(
            broker
                .resolve(
                    AiApprovalDecision {
                        approval_id: request.approval_id,
                        approve: true,
                    },
                    &binding,
                )
                .expect_err("single use")
                .sql_state,
            "55000"
        );
    }

    #[test]
    fn changed_binding_and_expiry_fail_closed_and_consume_record() {
        let clock = Arc::new(ManualClock::default());
        let broker = AiApprovalBroker::new(
            Arc::clone(&clock) as Arc<dyn AiApprovalClock>,
            Arc::new(FixedEntropy),
        );
        let call = call();
        let binding = AiApprovalBinding::new("run-1", "connection-1", &call, "delete rows");
        let request = broker
            .issue(binding.clone(), call.clone(), "preview", "delete rows")
            .expect("request");
        let changed = AiApprovalBinding::new("run-1", "connection-2", &call, "delete rows");
        assert_eq!(
            broker
                .resolve(
                    AiApprovalDecision {
                        approval_id: request.approval_id,
                        approve: true,
                    },
                    &changed,
                )
                .expect_err("connection mismatch")
                .sql_state,
            "55000"
        );

        let request = broker
            .issue(binding.clone(), call, "preview", "delete rows")
            .expect("second request");
        clock.advance(APPROVAL_TTL_MS);
        assert_eq!(
            broker
                .resolve(
                    AiApprovalDecision {
                        approval_id: request.approval_id,
                        approve: true,
                    },
                    &binding,
                )
                .expect_err("expired")
                .sql_state,
            "57014"
        );
        assert_eq!(broker.pending_count().expect("count"), 0);
    }

    #[test]
    fn denied_and_cancelled_approvals_cannot_be_reused() {
        let broker =
            AiApprovalBroker::new(Arc::new(ManualClock::default()), Arc::new(FixedEntropy));
        let call = call();
        let binding = AiApprovalBinding::new("run-1", "connection-1", &call, "delete rows");
        let request = broker
            .issue(binding.clone(), call, "preview", "delete rows")
            .expect("request");
        assert_eq!(broker.cancel_run("run-1").expect("cancel"), 1);
        assert_eq!(
            broker
                .resolve(
                    AiApprovalDecision {
                        approval_id: request.approval_id,
                        approve: false,
                    },
                    &binding,
                )
                .expect_err("cancelled")
                .sql_state,
            "55000"
        );
    }
}
