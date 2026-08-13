
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;
    use tokio::time::sleep;

    use super::*;
    use crate::{
        AiDataSharingPolicy, AiProviderKind, AiProviderSettings, AiReasoningEffort, FakeProvider,
        anonymous_safety_identifier,
    };

    #[derive(Default)]
    struct TestExecutor {
        active_reads: AtomicUsize,
        peak_reads: AtomicUsize,
        active_writes: AtomicUsize,
        peak_writes: AtomicUsize,
        reads: AtomicUsize,
        writes: AtomicUsize,
    }

    #[async_trait]
    impl AiToolExecutor for TestExecutor {
        async fn authorize(
            &self,
            _context: &AiToolExecutionContext,
            call: &ValidatedAiToolCall,
        ) -> Result<AiToolAuthorization> {
            Ok(AiToolAuthorization {
                mode: if call.definition().risk == AiToolRisk::RequiresApproval {
                    AiToolExecutionMode::Mutation
                } else {
                    AiToolExecutionMode::ReadOnly
                },
                preview: "bounded preview".to_owned(),
                impact_summary: "bounded impact".to_owned(),
            })
        }

        async fn inspect(
            &self,
            _context: AiToolExecutionContext,
            _call: ValidatedAiToolCall,
            _limits: AiToolLimits,
            _cancellation: CancellationToken,
        ) -> Result<AiToolOutput> {
            let active = self.active_reads.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_reads.fetch_max(active, Ordering::SeqCst);
            sleep(Duration::from_millis(20)).await;
            self.active_reads.fetch_sub(1, Ordering::SeqCst);
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(output("read complete"))
        }

        async fn mutate(
            &self,
            _context: AiToolExecutionContext,
            _call: ApprovedAiToolCall,
            _limits: AiToolLimits,
            _cancellation: CancellationToken,
        ) -> Result<AiToolOutput> {
            let active = self.active_writes.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_writes.fetch_max(active, Ordering::SeqCst);
            sleep(Duration::from_millis(10)).await;
            self.active_writes.fetch_sub(1, Ordering::SeqCst);
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(output("write complete"))
        }

        async fn cancel_run(&self, _run_id: &str) -> Result<()> {
            Ok(())
        }
    }

    fn output(summary: &str) -> AiToolOutput {
        AiToolOutput {
            content: json!({"ok": true}),
            rows_retained: 1,
            total_rows: 1,
            bytes_retained: 11,
            truncated: false,
            summary: summary.to_owned(),
            disclosure: None,
        }
    }

    fn definition(name: &str, risk: AiToolRisk) -> AiToolDefinition {
        AiToolDefinition {
            name: name.to_owned(),
            version: 1,
            description: format!("Execute {name}"),
            parameters: json!({
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
                "additionalProperties": false
            }),
            risk,
        }
    }

    fn call(id: usize, name: &str) -> AiProviderEvent {
        AiProviderEvent::ToolCall(AiToolCall {
            call_id: format!("call-{id}"),
            name: name.to_owned(),
            arguments: json!({"value": format!("value-{id}")}),
        })
    }

    fn run_request() -> AiRunRequest {
        AiRunRequest {
            run_id: "run-1".to_owned(),
            connection_id: "connection-1".to_owned(),
            user_text: "Inspect the database".to_owned(),
            settings: AiProviderSettings {
                kind: AiProviderKind::Fake,
                model: "fake-model".to_owned(),
                endpoint: None,
                reasoning: AiReasoningEffort::Medium,
                data_sharing: AiDataSharingPolicy::SchemaOnly,
                credential_id: None,
            },
            history: Vec::new(),
            include_sample_values: false,
        }
    }

    fn engine(
        provider: Arc<FakeProvider>,
        executor: Arc<TestExecutor>,
        definitions: Vec<AiToolDefinition>,
        approvals: Arc<AiApprovalBroker>,
    ) -> AiRunEngine {
        AiRunEngine::new(
            provider,
            executor,
            AiToolRegistry::new(definitions).expect("registry"),
            approvals,
            anonymous_safety_identifier(b"stable-user").expect("identifier"),
        )
        .expect("engine")
    }

    #[tokio::test]
    async fn read_tools_are_bounded_to_three_and_outputs_preserve_call_order() {
        let provider = Arc::new(FakeProvider::new(vec![
            vec![
                call(1, "read"),
                call(2, "read"),
                call(3, "read"),
                call(4, "read"),
                AiProviderEvent::Completed,
            ],
            vec![
                AiProviderEvent::TextDelta("done".to_owned()),
                AiProviderEvent::Completed,
            ],
        ]));
        let executor = Arc::new(TestExecutor::default());
        let engine = engine(
            Arc::clone(&provider),
            Arc::clone(&executor),
            vec![definition("read", AiToolRisk::ReadOnly)],
            Arc::new(AiApprovalBroker::default()),
        );
        let sink = Arc::new(RecordingRunEventSink::default());
        engine
            .run(
                run_request(),
                Arc::clone(&sink) as Arc<dyn AiRunEventSink>,
                CancellationToken::new(),
            )
            .await
            .expect("run");
        assert_eq!(executor.reads.load(Ordering::SeqCst), 4);
        assert_eq!(executor.peak_reads.load(Ordering::SeqCst), 3);
        let requests = provider.requests().expect("requests");
        let output_ids = requests[1]
            .input
            .iter()
            .filter_map(|input| match input {
                AiProviderInput::FunctionOutput { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(output_ids, vec!["call-1", "call-2", "call-3", "call-4"]);
        let events = sink.events().expect("events");
        assert!(
            events
                .windows(2)
                .all(|pair| pair[0].sequence < pair[1].sequence)
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.payload.is_terminal())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn seventeenth_tool_call_fails_before_execution() {
        let mut round = (1..=17).map(|id| call(id, "read")).collect::<Vec<_>>();
        round.push(AiProviderEvent::Completed);
        let provider = Arc::new(FakeProvider::new(vec![round]));
        let executor = Arc::new(TestExecutor::default());
        let engine = engine(
            provider,
            Arc::clone(&executor),
            vec![definition("read", AiToolRisk::ReadOnly)],
            Arc::new(AiApprovalBroker::default()),
        );
        let sink = Arc::new(RecordingRunEventSink::default());
        let error = engine
            .run(
                run_request(),
                Arc::clone(&sink) as Arc<dyn AiRunEventSink>,
                CancellationToken::new(),
            )
            .await
            .expect_err("limit");
        assert_eq!(error.sql_state, "54000");
        assert_eq!(executor.reads.load(Ordering::SeqCst), 0);
        assert!(matches!(
            sink.events()
                .expect("events")
                .last()
                .map(|event| &event.payload),
            Some(AiRunEventPayload::Error { .. })
        ));
    }

    #[tokio::test]
    async fn mutation_waits_for_one_time_approval_and_then_executes() {
        let provider = Arc::new(FakeProvider::new(vec![
            vec![call(1, "write"), AiProviderEvent::Completed],
            vec![AiProviderEvent::Completed],
        ]));
        let executor = Arc::new(TestExecutor::default());
        let approvals = Arc::new(AiApprovalBroker::default());
        let engine = Arc::new(engine(
            provider,
            Arc::clone(&executor),
            vec![definition("write", AiToolRisk::RequiresApproval)],
            Arc::clone(&approvals),
        ));
        let sink = Arc::new(RecordingRunEventSink::default());
        let task = {
            let engine = Arc::clone(&engine);
            let sink = Arc::clone(&sink);
            tokio::spawn(async move {
                engine
                    .run(
                        run_request(),
                        sink as Arc<dyn AiRunEventSink>,
                        CancellationToken::new(),
                    )
                    .await
            })
        };
        let approval_id =
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if let Some(id) = sink.events().expect("events").iter().find_map(|event| {
                        match &event.payload {
                            AiRunEventPayload::ApprovalRequired { request } => {
                                Some(request.approval_id.clone())
                            }
                            _ => None,
                        }
                    }) {
                        break id;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("approval request");
        engine
            .decide(AiApprovalDecision {
                approval_id: approval_id.clone(),
                approve: true,
            })
            .expect("approve");
        task.await.expect("join").expect("run");
        assert_eq!(executor.writes.load(Ordering::SeqCst), 1);
        assert_eq!(executor.peak_writes.load(Ordering::SeqCst), 1);
        assert_eq!(
            engine
                .decide(AiApprovalDecision {
                    approval_id,
                    approve: true,
                })
                .expect_err("replay")
                .sql_state,
            "55000"
        );
    }
}
