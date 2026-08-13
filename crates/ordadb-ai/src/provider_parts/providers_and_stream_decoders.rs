use std::{
    collections::{BTreeSet, VecDeque},
    net::IpAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use async_trait::async_trait;
use futures_util::StreamExt;
use ordadb_types::{DbError, Result};
use reqwest::{Client, StatusCode, Url, header};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::{
    AiProviderEvent, AiProviderInput, AiProviderKind, AiProviderRequest, AiProviderSettings,
    AiToolCall, AiUsage, canonical_json, validate_tool_definition,
};

const OFFICIAL_RESPONSES_ENDPOINT: &str = "https://api.openai.com/v1/responses";
const DEFAULT_OLLAMA_ENDPOINT: &str = "http://127.0.0.1:11434";
const MAX_PROVIDER_EVENT_BYTES: usize = 256 * 1024;
const MAX_PROVIDER_ERROR_BYTES: usize = 64 * 1024;
const MAX_PROVIDER_STREAM_BYTES: usize = 16 * 1024 * 1024;
const MAX_PROVIDER_EVENTS: usize = 16_384;
const MAX_MODEL_BYTES: usize = 256;
const MAX_PROVIDER_INPUTS: usize = 512;

#[async_trait]
pub trait AiProviderEventSink: Send + Sync {
    async fn emit(&self, event: AiProviderEvent) -> Result<()>;
}

#[async_trait]
pub trait AiProvider: Send + Sync {
    async fn stream(
        &self,
        request: AiProviderRequest,
        sink: Arc<dyn AiProviderEventSink>,
        cancellation: CancellationToken,
    ) -> Result<()>;
}

pub struct FakeProvider {
    rounds: Mutex<VecDeque<Result<Vec<AiProviderEvent>>>>,
    requests: Mutex<Vec<AiProviderRequest>>,
}

impl FakeProvider {
    #[must_use]
    pub fn new(rounds: Vec<Vec<AiProviderEvent>>) -> Self {
        Self {
            rounds: Mutex::new(rounds.into_iter().map(Ok).collect()),
            requests: Mutex::new(Vec::new()),
        }
    }

    #[must_use]
    pub fn with_failure(error: DbError) -> Self {
        Self {
            rounds: Mutex::new(VecDeque::from([Err(error)])),
            requests: Mutex::new(Vec::new()),
        }
    }

    pub fn requests(&self) -> Result<Vec<AiProviderRequest>> {
        Ok(mutex_lock(&self.requests)?.clone())
    }
}

#[async_trait]
impl AiProvider for FakeProvider {
    async fn stream(
        &self,
        request: AiProviderRequest,
        sink: Arc<dyn AiProviderEventSink>,
        cancellation: CancellationToken,
    ) -> Result<()> {
        mutex_lock(&self.requests)?.push(request);
        let round = mutex_lock(&self.rounds)?
            .pop_front()
            .ok_or_else(|| DbError::new("XX000", "fake AI provider has no scripted round"))??;
        for event in round {
            tokio::select! {
                () = cancellation.cancelled() => return Err(cancelled()),
                result = sink.emit(event) => result?,
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct RecordingProviderSink {
    events: Mutex<Vec<AiProviderEvent>>,
}

impl RecordingProviderSink {
    pub fn events(&self) -> Result<Vec<AiProviderEvent>> {
        Ok(mutex_lock(&self.events)?.clone())
    }
}

#[async_trait]
impl AiProviderEventSink for RecordingProviderSink {
    async fn emit(&self, event: AiProviderEvent) -> Result<()> {
        mutex_lock(&self.events)?.push(event);
        Ok(())
    }
}

pub struct OpenAiProvider {
    client: Client,
    endpoint: Url,
    api_key: Option<Zeroizing<String>>,
}

impl OpenAiProvider {
    pub fn official(api_key: Zeroizing<String>) -> Result<Self> {
        validate_secret(api_key.as_str(), "OpenAI API key")?;
        Ok(Self {
            client: provider_client()?,
            endpoint: Url::parse(OFFICIAL_RESPONSES_ENDPOINT)
                .map_err(|error| internal_url_error("official OpenAI endpoint", error))?,
            api_key: Some(api_key),
        })
    }

    pub fn compatible(endpoint: &str, api_key: Option<Zeroizing<String>>) -> Result<Self> {
        let endpoint = validate_responses_endpoint(endpoint)?;
        if let Some(api_key) = api_key.as_deref() {
            validate_secret(api_key, "compatible provider API key")?;
        }
        Ok(Self {
            client: provider_client()?,
            endpoint,
            api_key,
        })
    }
}

#[async_trait]
impl AiProvider for OpenAiProvider {
    async fn stream(
        &self,
        request: AiProviderRequest,
        sink: Arc<dyn AiProviderEventSink>,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let body = build_openai_responses_request(&request)?;
        let mut request_builder = self
            .client
            .post(self.endpoint.clone())
            .header(header::ACCEPT, "text/event-stream")
            .json(&body);
        if let Some(api_key) = self.api_key.as_deref() {
            request_builder = request_builder.bearer_auth(api_key);
        }
        let response = tokio::select! {
            () = cancellation.cancelled() => return Err(cancelled()),
            response = request_builder.send() => response.map_err(provider_network_error)?,
        };
        if response.status() != StatusCode::OK {
            return Err(read_provider_error(response, &cancellation).await?);
        }
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if !content_type
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"))
        {
            return Err(protocol_error(
                "AI provider did not return a typed SSE stream",
            ));
        }
        let mut decoder = OpenAiSseDecoder::default();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = tokio::select! {
            () = cancellation.cancelled() => return Err(cancelled()),
            chunk = stream.next() => chunk,
        } {
            for event in decoder.push(&chunk.map_err(provider_network_error)?)? {
                sink.emit(event).await?;
            }
        }
        for event in decoder.finish()? {
            sink.emit(event).await?;
        }
        Ok(())
    }
}

pub struct OllamaProvider {
    client: Client,
    endpoint: Url,
}

impl OllamaProvider {
    pub fn new(endpoint: Option<&str>) -> Result<Self> {
        let mut endpoint = validate_loopback_endpoint(endpoint.unwrap_or(DEFAULT_OLLAMA_ENDPOINT))?;
        endpoint.set_path("/api/chat");
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        Ok(Self {
            client: provider_client()?,
            endpoint,
        })
    }
}

#[async_trait]
impl AiProvider for OllamaProvider {
    async fn stream(
        &self,
        request: AiProviderRequest,
        sink: Arc<dyn AiProviderEventSink>,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let body = build_ollama_request(&request)?;
        let response = tokio::select! {
            () = cancellation.cancelled() => return Err(cancelled()),
            response = self.client.post(self.endpoint.clone()).json(&body).send() => {
                response.map_err(provider_network_error)?
            }
        };
        if response.status() != StatusCode::OK {
            return Err(read_provider_error(response, &cancellation).await?);
        }
        let mut decoder = OllamaNdjsonDecoder::default();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = tokio::select! {
            () = cancellation.cancelled() => return Err(cancelled()),
            chunk = stream.next() => chunk,
        } {
            for event in decoder.push(&chunk.map_err(provider_network_error)?)? {
                sink.emit(event).await?;
            }
        }
        for event in decoder.finish()? {
            sink.emit(event).await?;
        }
        Ok(())
    }
}

pub fn validate_provider_settings(settings: &AiProviderSettings) -> Result<()> {
    validate_text(&settings.model, MAX_MODEL_BYTES, "AI model")?;
    match settings.kind {
        AiProviderKind::OpenAi => {
            if settings.endpoint.is_some() {
                return Err(invalid(
                    "the official OpenAI provider does not accept a custom endpoint",
                ));
            }
            if settings.credential_id.is_none() {
                return Err(invalid(
                    "the official OpenAI provider requires a credential ID",
                ));
            }
        }
        AiProviderKind::OpenAiCompatible => {
            validate_responses_endpoint(
                settings
                    .endpoint
                    .as_deref()
                    .ok_or_else(|| invalid("compatible provider endpoint is required"))?,
            )?;
        }
        AiProviderKind::Ollama => {
            validate_loopback_endpoint(
                settings
                    .endpoint
                    .as_deref()
                    .unwrap_or(DEFAULT_OLLAMA_ENDPOINT),
            )?;
            if settings.credential_id.is_some() {
                return Err(invalid(
                    "Ollama does not accept a Credential Manager API key",
                ));
            }
        }
        AiProviderKind::Fake => {
            if settings.endpoint.is_some() || settings.credential_id.is_some() {
                return Err(invalid(
                    "the fake AI provider accepts no endpoint or credential",
                ));
            }
        }
    }
    if let Some(credential_id) = settings.credential_id.as_deref() {
        validate_identifier(credential_id, 256, "AI credential ID")?;
    }
    Ok(())
}

pub fn anonymous_safety_identifier(stable_local_identifier: &[u8]) -> Result<String> {
    if stable_local_identifier.is_empty() || stable_local_identifier.len() > 16 * 1024 {
        return Err(invalid("stable AI safety identifier source is invalid"));
    }
    let digest = Sha256::digest(stable_local_identifier);
    let mut identifier = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        write!(&mut identifier, "{byte:02x}").map_err(|_| {
            DbError::new("XX000", "failed to encode anonymous AI safety identifier")
        })?;
    }
    Ok(identifier)
}

pub fn build_openai_responses_request(request: &AiProviderRequest) -> Result<JsonValue> {
    validate_provider_request(request)?;
    let input = request
        .input
        .iter()
        .map(openai_input)
        .collect::<Result<Vec<_>>>()?;
    let tools = request
        .tools
        .iter()
        .map(|tool| {
            Ok(json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
                "strict": true
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "model": request.model,
        "input": input,
        "tools": tools,
        "parallel_tool_calls": true,
        "reasoning": {"effort": request.reasoning.as_str()},
        "safety_identifier": request.safety_identifier,
        "stream": true,
        "store": false
    }))
}

pub fn build_ollama_request(request: &AiProviderRequest) -> Result<JsonValue> {
    validate_provider_request(request)?;
    let messages = request
        .input
        .iter()
        .map(ollama_input)
        .collect::<Result<Vec<_>>>()?;
    let tools = request
        .tools
        .iter()
        .map(|tool| {
            Ok(json!({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters
                }
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "model": request.model,
        "messages": messages,
        "tools": tools,
        "stream": true
    }))
}

fn validate_provider_request(request: &AiProviderRequest) -> Result<()> {
    validate_text(&request.model, MAX_MODEL_BYTES, "AI model")?;
    if request.safety_identifier.is_empty() || request.safety_identifier.len() > 64 {
        return Err(invalid(
            "AI safety identifier must contain at most 64 bytes",
        ));
    }
    if request.input.is_empty() || request.input.len() > MAX_PROVIDER_INPUTS {
        return Err(limit(
            "AI provider input count is outside the supported limit",
        ));
    }
    if request.tools.len() > 128 {
        return Err(limit("AI provider tool count exceeds the supported limit"));
    }
    let mut names = BTreeSet::new();
    for tool in &request.tools {
        validate_tool_definition(tool)?;
        if !names.insert(tool.name.as_str()) {
            return Err(invalid(
                "AI provider request contains a duplicate tool name",
            ));
        }
    }
    for item in &request.input {
        match item {
            AiProviderInput::Message { role, text } => {
                if !matches!(role.as_str(), "user" | "assistant" | "developer") {
                    return Err(invalid("AI provider message role is invalid"));
                }
                validate_text(text, 1024 * 1024, "AI provider message")?;
            }
            AiProviderInput::FunctionCall {
                call_id,
                name,
                arguments,
            } => {
                validate_identifier(call_id, 256, "AI provider function call ID")?;
                validate_identifier(name, 64, "AI provider function name")?;
                if canonical_json(arguments)?.len() > 1024 * 1024 {
                    return Err(limit("AI provider function arguments exceed 1 MiB"));
                }
            }
            AiProviderInput::FunctionOutput { call_id, output } => {
                validate_identifier(call_id, 256, "AI provider function output ID")?;
                if canonical_json(output)?.len() > 2 * 1024 * 1024 {
                    return Err(limit("AI provider function output exceeds 2 MiB"));
                }
            }
        }
    }
    Ok(())
}

fn openai_input(input: &AiProviderInput) -> Result<JsonValue> {
    match input {
        AiProviderInput::Message { role, text } => Ok(json!({"role": role, "content": text})),
        AiProviderInput::FunctionCall {
            call_id,
            name,
            arguments,
        } => Ok(json!({
            "type": "function_call",
            "call_id": call_id,
            "name": name,
            "arguments": canonical_json(arguments)?
        })),
        AiProviderInput::FunctionOutput { call_id, output } => Ok(json!({
            "type": "function_call_output",
            "call_id": call_id,
            "output": canonical_json(output)?
        })),
    }
}

fn ollama_input(input: &AiProviderInput) -> Result<JsonValue> {
    match input {
        AiProviderInput::Message { role, text } => Ok(json!({"role": role, "content": text})),
        AiProviderInput::FunctionCall {
            call_id,
            name,
            arguments,
        } => Ok(json!({
            "role": "assistant",
            "content": "",
            "tool_calls": [{
                "id": call_id,
                "function": {"name": name, "arguments": arguments}
            }]
        })),
        AiProviderInput::FunctionOutput { call_id, output } => Ok(json!({
            "role": "tool",
            "tool_call_id": call_id,
            "content": canonical_json(output)?
        })),
    }
}

#[derive(Default)]
struct OpenAiSseDecoder {
    buffer: Vec<u8>,
    bytes_seen: usize,
    events_seen: usize,
    pending_function_items: BTreeSet<String>,
    terminal: bool,
}

impl OpenAiSseDecoder {
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<AiProviderEvent>> {
        self.bytes_seen = self.bytes_seen.saturating_add(chunk.len());
        if self.bytes_seen > MAX_PROVIDER_STREAM_BYTES {
            return Err(limit("AI provider stream exceeds the 16 MiB limit"));
        }
        self.buffer.extend_from_slice(chunk);
        if self.buffer.len() > MAX_PROVIDER_EVENT_BYTES && find_sse_boundary(&self.buffer).is_none()
        {
            return Err(limit("AI provider SSE event exceeds the 256 KiB limit"));
        }
        let mut emitted = Vec::new();
        while let Some((end, separator)) = find_sse_boundary(&self.buffer) {
            let frame = self.buffer[..end].to_vec();
            self.buffer.drain(..end + separator);
            if let Some(data) = parse_sse_frame(&frame)? {
                self.events_seen = self.events_seen.saturating_add(1);
                if self.events_seen > MAX_PROVIDER_EVENTS {
                    return Err(limit("AI provider stream contains too many events"));
                }
                if data != "[DONE]" {
                    self.process_event(&data, &mut emitted)?;
                }
            }
        }
        Ok(emitted)
    }

    fn finish(&mut self) -> Result<Vec<AiProviderEvent>> {
        if !self.buffer.iter().all(u8::is_ascii_whitespace) {
            return Err(protocol_error(
                "AI provider SSE stream ended inside an event",
            ));
        }
        if !self.pending_function_items.is_empty() {
            return Err(protocol_error(
                "AI provider ended with an incomplete function call",
            ));
        }
        if !self.terminal {
            return Err(protocol_error(
                "AI provider SSE stream ended without response.completed",
            ));
        }
        Ok(Vec::new())
    }

    fn process_event(&mut self, data: &str, emitted: &mut Vec<AiProviderEvent>) -> Result<()> {
        if self.terminal {
            return Err(protocol_error(
                "AI provider emitted data after its terminal event",
            ));
        }
        let event: JsonValue = serde_json::from_str(data).map_err(|error| {
            protocol_error("AI provider emitted invalid SSE JSON").with_detail(error.to_string())
        })?;
        let event_type = event
            .get("type")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| protocol_error("AI provider SSE event has no type"))?;
        match event_type {
            "response.output_text.delta" => {
                let delta = event
                    .get("delta")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| protocol_error("AI text delta is missing text"))?;
                if !delta.is_empty() {
                    emitted.push(AiProviderEvent::TextDelta(delta.to_owned()));
                }
            }
            "response.output_item.added" => {
                if event.pointer("/item/type").and_then(JsonValue::as_str) == Some("function_call")
                {
                    self.pending_function_items
                        .insert(function_item_key(&event)?);
                }
            }
            "response.output_item.done" => {
                if event.pointer("/item/type").and_then(JsonValue::as_str) == Some("function_call")
                {
                    let key = function_item_key(&event)?;
                    self.pending_function_items.remove(&key);
                    let call_id = required_event_text(&event, "/item/call_id", "function call ID")?;
                    let name = required_event_text(&event, "/item/name", "function name")?;
                    let arguments =
                        required_event_text(&event, "/item/arguments", "function arguments")?;
                    let arguments = serde_json::from_str(arguments).map_err(|error| {
                        protocol_error("AI provider emitted invalid function arguments")
                            .with_detail(error.to_string())
                    })?;
                    emitted.push(AiProviderEvent::ToolCall(AiToolCall {
                        call_id: call_id.to_owned(),
                        name: name.to_owned(),
                        arguments,
                    }));
                }
            }
            "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {}
            "response.completed" => {
                if !self.pending_function_items.is_empty() {
                    return Err(protocol_error(
                        "AI provider completed with an incomplete function call",
                    ));
                }
                if let Some(usage) = parse_openai_usage(event.pointer("/response/usage"))? {
                    emitted.push(AiProviderEvent::Usage(usage));
                }
                emitted.push(AiProviderEvent::Completed);
                self.terminal = true;
            }
            "response.failed" | "response.incomplete" | "error" => {
                return Err(provider_event_error(&event, event_type));
            }
            unknown if unknown.contains("function_call") || unknown.contains("tool_call") => {
                return Err(protocol_error(format!(
                    "AI provider emitted unsupported tool event {unknown}"
                )));
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Default)]
struct OllamaNdjsonDecoder {
    buffer: Vec<u8>,
    bytes_seen: usize,
    events_seen: usize,
    tool_sequence: u64,
    terminal: bool,
}

impl OllamaNdjsonDecoder {
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<AiProviderEvent>> {
        self.bytes_seen = self.bytes_seen.saturating_add(chunk.len());
        if self.bytes_seen > MAX_PROVIDER_STREAM_BYTES {
            return Err(limit("Ollama stream exceeds the 16 MiB limit"));
        }
        self.buffer.extend_from_slice(chunk);
        if self.buffer.len() > MAX_PROVIDER_EVENT_BYTES && !self.buffer.contains(&b'\n') {
            return Err(limit("Ollama event exceeds the 256 KiB limit"));
        }
        let mut emitted = Vec::new();
        while let Some(end) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line = self.buffer[..end].to_vec();
            self.buffer.drain(..=end);
            self.process_line(&line, &mut emitted)?;
        }
        Ok(emitted)
    }

    fn finish(&mut self) -> Result<Vec<AiProviderEvent>> {
        let mut emitted = Vec::new();
        if !self.buffer.iter().all(u8::is_ascii_whitespace) {
            let line = std::mem::take(&mut self.buffer);
            self.process_line(&line, &mut emitted)?;
        }
        if !self.terminal {
            return Err(protocol_error("Ollama stream ended without done=true"));
        }
        Ok(emitted)
    }

    fn process_line(&mut self, line: &[u8], emitted: &mut Vec<AiProviderEvent>) -> Result<()> {
        if line.iter().all(u8::is_ascii_whitespace) {
            return Ok(());
        }
        if self.terminal {
            return Err(protocol_error("Ollama emitted data after done=true"));
        }
        self.events_seen = self.events_seen.saturating_add(1);
        if self.events_seen > MAX_PROVIDER_EVENTS {
            return Err(limit("Ollama stream contains too many events"));
        }
        let event: JsonValue = serde_json::from_slice(line).map_err(|error| {
            protocol_error("Ollama emitted invalid NDJSON").with_detail(error.to_string())
        })?;
        if let Some(error) = event.get("error").and_then(JsonValue::as_str) {
            return Err(DbError::new("08006", "Ollama request failed")
                .with_detail(bounded_external_text(error)));
        }
        if let Some(content) = event
            .pointer("/message/content")
            .and_then(JsonValue::as_str)
            && !content.is_empty()
        {
            emitted.push(AiProviderEvent::TextDelta(content.to_owned()));
        }
        if let Some(tool_calls) = event
            .pointer("/message/tool_calls")
            .and_then(JsonValue::as_array)
        {
            for tool_call in tool_calls {
                self.tool_sequence = self.tool_sequence.saturating_add(1);
                let name = tool_call
                    .pointer("/function/name")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| protocol_error("Ollama tool call has no function name"))?;
                let arguments = tool_call
                    .pointer("/function/arguments")
                    .cloned()
                    .ok_or_else(|| protocol_error("Ollama tool call has no arguments"))?;
                let call_id = tool_call
                    .get("id")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("ollama-{}", self.tool_sequence));
                emitted.push(AiProviderEvent::ToolCall(AiToolCall {
                    call_id,
                    name: name.to_owned(),
                    arguments,
                }));
            }
        }
        if event.get("done").and_then(JsonValue::as_bool) == Some(true) {
            let input_tokens = event
                .get("prompt_eval_count")
                .and_then(JsonValue::as_u64)
                .unwrap_or(0);
            let output_tokens = event
                .get("eval_count")
                .and_then(JsonValue::as_u64)
                .unwrap_or(0);
            emitted.push(AiProviderEvent::Usage(AiUsage {
                input_tokens,
                output_tokens,
                reasoning_tokens: 0,
            }));
            emitted.push(AiProviderEvent::Completed);
            self.terminal = true;
        }
        Ok(())
    }
}

fn find_sse_boundary(bytes: &[u8]) -> Option<(usize, usize)> {
    let lf = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| (index, 2));
    let crlf = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| (index, 4));
    match (lf, crlf) {
        (Some(left), Some(right)) => Some(if left.0 <= right.0 { left } else { right }),
        (Some(boundary), None) | (None, Some(boundary)) => Some(boundary),
        (None, None) => None,
    }
}

fn parse_sse_frame(frame: &[u8]) -> Result<Option<String>> {
    if frame.len() > MAX_PROVIDER_EVENT_BYTES {
        return Err(limit("AI provider SSE event exceeds the 256 KiB limit"));
    }
    let text = std::str::from_utf8(frame)
        .map_err(|_| protocol_error("AI provider SSE event is not UTF-8"))?;
    let mut data = Vec::new();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("data:") {
            data.push(value.strip_prefix(' ').unwrap_or(value));
        }
    }
    if data.is_empty() {
        Ok(None)
    } else {
        Ok(Some(data.join("\n")))
    }
}

fn function_item_key(event: &JsonValue) -> Result<String> {
    if let Some(index) = event.get("output_index").and_then(JsonValue::as_u64) {
        return Ok(format!("index:{index}"));
    }
    for pointer in ["/item/id", "/item/call_id"] {
        if let Some(value) = event.pointer(pointer).and_then(JsonValue::as_str) {
            return Ok(format!("item:{value}"));
        }
    }
    Err(protocol_error("AI function item has no stable identity"))
}

fn required_event_text<'a>(event: &'a JsonValue, pointer: &str, context: &str) -> Result<&'a str> {
    event
        .pointer(pointer)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| protocol_error(format!("AI provider event is missing {context}")))
}

fn parse_openai_usage(usage: Option<&JsonValue>) -> Result<Option<AiUsage>> {
    let Some(usage) = usage else {
        return Ok(None);
    };
    let object = usage
        .as_object()
        .ok_or_else(|| protocol_error("AI provider usage is not an object"))?;
    Ok(Some(AiUsage {
        input_tokens: object
            .get("input_tokens")
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
        output_tokens: object
            .get("output_tokens")
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
        reasoning_tokens: usage
            .pointer("/output_tokens_details/reasoning_tokens")
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
    }))
}

fn provider_event_error(event: &JsonValue, event_type: &str) -> DbError {
    let message = event
        .pointer("/response/error/message")
        .or_else(|| event.pointer("/error/message"))
        .and_then(JsonValue::as_str)
        .map(bounded_external_text)
        .unwrap_or_else(|| format!("AI provider emitted {event_type}"));
    DbError::new("08006", "AI provider response failed").with_detail(message)
}

async fn read_provider_error(
    response: reqwest::Response,
    cancellation: &CancellationToken,
) -> Result<DbError> {
    let status = response.status();
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = tokio::select! {
        () = cancellation.cancelled() => return Ok(cancelled()),
        chunk = stream.next() => chunk,
    } {
        let chunk = chunk.map_err(provider_network_error)?;
        let remaining = MAX_PROVIDER_ERROR_BYTES.saturating_sub(bytes.len());
        bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if bytes.len() == MAX_PROVIDER_ERROR_BYTES {
            break;
        }
    }
    let detail = serde_json::from_slice::<JsonValue>(&bytes)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .and_then(JsonValue::as_str)
                .map(bounded_external_text)
        })
        .unwrap_or_else(|| format!("provider returned HTTP {status}"));
    Ok(DbError::new("08006", "AI provider request failed").with_detail(detail))
}
