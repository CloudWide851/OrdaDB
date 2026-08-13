
fn provider_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| {
            DbError::new("58030", "failed to construct AI provider client")
                .with_detail(error.to_string())
        })
}

fn validate_responses_endpoint(value: &str) -> Result<Url> {
    let endpoint = parse_clean_url(value, "compatible AI endpoint")?;
    if endpoint.scheme() != "https" && !is_loopback_url(&endpoint) {
        return Err(invalid(
            "compatible AI endpoint requires HTTPS unless it is loopback",
        ));
    }
    Ok(endpoint)
}

fn validate_loopback_endpoint(value: &str) -> Result<Url> {
    let endpoint = parse_clean_url(value, "Ollama endpoint")?;
    if !is_loopback_url(&endpoint) {
        return Err(invalid("Ollama endpoint must use a loopback host"));
    }
    if !matches!(endpoint.scheme(), "http" | "https") {
        return Err(invalid("Ollama endpoint must use HTTP or HTTPS"));
    }
    Ok(endpoint)
}

fn parse_clean_url(value: &str, context: &str) -> Result<Url> {
    if value.len() > 2_048 || value.as_bytes().contains(&0) {
        return Err(invalid(format!("{context} is invalid")));
    }
    let endpoint = Url::parse(value)
        .map_err(|error| invalid(format!("{context} is invalid")).with_detail(error.to_string()))?;
    if !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(invalid(format!(
            "{context} must not contain credentials, query, or fragment"
        )));
    }
    Ok(endpoint)
}

fn is_loopback_url(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    })
}

fn validate_secret(value: &str, context: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 16 * 1024
        || value.as_bytes().iter().any(|byte| byte.is_ascii_control())
    {
        return Err(invalid(format!("{context} is invalid")));
    }
    Ok(())
}

fn validate_identifier(value: &str, maximum: usize, context: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > maximum
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
    {
        return Err(invalid(format!("{context} is invalid")));
    }
    Ok(())
}

fn validate_text(value: &str, maximum: usize, context: &str) -> Result<()> {
    if value.is_empty() || value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(invalid(format!("{context} is invalid")));
    }
    Ok(())
}

fn bounded_external_text(value: &str) -> String {
    let mut end = value.len().min(4_096);
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value[..end]
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
}

fn internal_url_error(context: &str, error: impl std::fmt::Display) -> DbError {
    DbError::new("XX000", format!("{context} is invalid")).with_detail(error.to_string())
}

fn provider_network_error(error: impl std::fmt::Display) -> DbError {
    DbError::new("08006", "AI provider network request failed").with_detail(error.to_string())
}

fn cancelled() -> DbError {
    DbError::new("57014", "AI run was cancelled")
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn limit(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::new("08P01", message)
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| DbError::new("XX000", "AI provider event lock was poisoned"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{AiReasoningEffort, AiToolDefinition, AiToolRisk};

    fn request() -> AiProviderRequest {
        AiProviderRequest {
            model: "gpt-5.6".to_owned(),
            reasoning: AiReasoningEffort::Medium,
            safety_identifier: anonymous_safety_identifier(b"stable-user").expect("identifier"),
            input: vec![AiProviderInput::Message {
                role: "user".to_owned(),
                text: "Describe public.items".to_owned(),
            }],
            tools: vec![AiToolDefinition {
                name: "describe_object".to_owned(),
                version: 1,
                description: "Describe one database object".to_owned(),
                parameters: json!({
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                    "required": ["name"],
                    "additionalProperties": false
                }),
                risk: AiToolRisk::ReadOnly,
            }],
        }
    }

    #[test]
    fn official_request_is_stateless_strict_and_call_ids_are_replayed() {
        let mut request = request();
        request.input.push(AiProviderInput::FunctionCall {
            call_id: "call-1".to_owned(),
            name: "describe_object".to_owned(),
            arguments: json!({"name": "public.items"}),
        });
        request.input.push(AiProviderInput::FunctionOutput {
            call_id: "call-1".to_owned(),
            output: json!({"columns": 3}),
        });
        let body = build_openai_responses_request(&request).expect("body");
        assert_eq!(body["model"], "gpt-5.6");
        assert_eq!(body["reasoning"]["effort"], "medium");
        assert_eq!(body["stream"], true);
        assert_eq!(body["store"], false);
        assert_eq!(body["tools"][0]["strict"], true);
        assert_eq!(
            body["tools"][0]["parameters"]["additionalProperties"],
            false
        );
        assert_eq!(body["input"][1]["call_id"], "call-1");
        assert_eq!(body["input"][2]["call_id"], "call-1");
        assert_eq!(request.safety_identifier.len(), 64);
        assert!(body.get("previous_response_id").is_none());
    }

    #[test]
    fn openai_sse_handles_fragmentation_tool_calls_usage_and_terminal() {
        let fixture = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\"}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"call-1\"}}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"call-1\",\"name\":\"describe_object\",\"arguments\":\"{\\\"name\\\":\\\"public.items\\\"}\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":4,\"output_tokens_details\":{\"reasoning_tokens\":2}}}}\n\n"
        );
        let mut decoder = OpenAiSseDecoder::default();
        let mut events = Vec::new();
        for chunk in fixture.as_bytes().chunks(17) {
            events.extend(decoder.push(chunk).expect("chunk"));
        }
        events.extend(decoder.finish().expect("finish"));
        assert_eq!(events[0], AiProviderEvent::TextDelta("Hello".to_owned()));
        assert!(matches!(&events[1], AiProviderEvent::ToolCall(call) if call.call_id == "call-1"));
        assert_eq!(
            events[2],
            AiProviderEvent::Usage(AiUsage {
                input_tokens: 10,
                output_tokens: 4,
                reasoning_tokens: 2
            })
        );
        assert_eq!(events[3], AiProviderEvent::Completed);
    }

    #[test]
    fn provider_decoders_fail_closed_on_tool_ambiguity_or_missing_terminal() {
        let mut decoder = OpenAiSseDecoder::default();
        decoder
            .push(b"data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\"}}\n\n")
            .expect("added");
        assert_eq!(decoder.finish().expect_err("incomplete").sql_state, "08P01");

        let mut decoder = OpenAiSseDecoder::default();
        assert_eq!(
            decoder
                .push(b"data: {\"type\":\"response.function_call.unknown\"}\n\n")
                .expect_err("unknown tool event")
                .sql_state,
            "08P01"
        );
    }

    #[test]
    fn ollama_request_and_fragmented_ndjson_are_normalized() {
        let body = build_ollama_request(&request()).expect("body");
        assert_eq!(body["stream"], true);
        assert!(body.get("store").is_none());
        assert!(body.get("safety_identifier").is_none());

        let fixture = concat!(
            "{\"message\":{\"content\":\"Hi\"},\"done\":false}\n",
            "{\"message\":{\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"describe_object\",\"arguments\":{\"name\":\"public.items\"}}}]},\"done\":false}\n",
            "{\"done\":true,\"prompt_eval_count\":8,\"eval_count\":3}\n"
        );
        let mut decoder = OllamaNdjsonDecoder::default();
        let mut events = Vec::new();
        for chunk in fixture.as_bytes().chunks(11) {
            events.extend(decoder.push(chunk).expect("chunk"));
        }
        events.extend(decoder.finish().expect("finish"));
        assert_eq!(events[0], AiProviderEvent::TextDelta("Hi".to_owned()));
        assert!(
            matches!(&events[1], AiProviderEvent::ToolCall(call) if call.call_id == "ollama-1")
        );
        assert_eq!(events.last(), Some(&AiProviderEvent::Completed));
    }

    #[test]
    fn endpoint_policy_is_fail_closed() {
        assert!(validate_responses_endpoint("https://example.com/v1/responses").is_ok());
        assert!(validate_responses_endpoint("http://127.0.0.1:9000/v1/responses").is_ok());
        assert!(validate_responses_endpoint("http://example.com/v1/responses").is_err());
        assert!(validate_responses_endpoint("https://key@example.com/v1/responses").is_err());
        assert!(validate_loopback_endpoint("http://localhost:11434").is_ok());
        assert!(validate_loopback_endpoint("https://example.com").is_err());
    }
}
