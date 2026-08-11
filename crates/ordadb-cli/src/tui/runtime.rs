use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use crossterm::event::{self, Event, KeyEventKind, MouseEventKind};
use ordadb_ai::{
    AiApprovalDecision, AiProviderKind, AiProviderSettings, AiReasoningEffort, AiRunEngine,
    AiRunEvent, AiRunEventPayload, AiRunEventSink, AiRunRequest,
};
use ordadb_types::{DbError, Result};
use ordadb_windows::{CredentialVault, prompt_for_credential};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::agent::TuiAgentKernel;
use super::app::{AppIntent, AppState, InputMode, MessageRole, unix_time_millis};
use super::native::NativeExecutor;
use super::settings::{TuiSettingsV1, TuiStateStore};
use super::terminal::TerminalSession;

const EVENT_CHANNEL_CAPACITY: usize = 256;
const INPUT_POLL: Duration = Duration::from_millis(50);

pub async fn run() -> Result<()> {
    let store = TuiStateStore::discover()?;
    let mut settings = store.load_settings()?;
    let persistence = store.load_persistence()?;
    let mut kernel = build_kernel(&settings)?;
    let mut app = AppState::new(settings.ui.history_limit, settings.ui.transcript_limit);
    app.restore_history(&persistence.history);
    app.push_message(
        MessageRole::System,
        "自然语言模式已就绪。输入 /help 查看命令，/connect 设置本地凭据。".to_owned(),
        unix_time_millis(),
    );
    let mut terminal = TerminalSession::enter()?;
    let (events_tx, mut events_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let mut active: Option<ActiveRun> = None;

    loop {
        while let Ok(runtime_event) = events_rx.try_recv() {
            apply_runtime_event(&mut app, &kernel, &mut active, runtime_event)?;
        }
        terminal.draw(&app)?;
        if !event::poll(INPUT_POLL)
            .map_err(|error| io_error("failed to poll terminal input", error))?
        {
            continue;
        }
        let terminal_event =
            event::read().map_err(|error| io_error("failed to read terminal input", error))?;
        let intent = match terminal_event {
            Event::Key(key) if key.kind == KeyEventKind::Press => app.handle_key(key),
            Event::Mouse(mouse) => {
                match mouse.kind {
                    MouseEventKind::ScrollUp => app.scroll = app.scroll.saturating_add(3),
                    MouseEventKind::ScrollDown => app.scroll = app.scroll.saturating_sub(3),
                    _ => {}
                }
                AppIntent::None
            }
            Event::Resize(_, _) | Event::FocusGained | Event::FocusLost | Event::Paste(_) => {
                AppIntent::None
            }
            Event::Key(_) => AppIntent::None,
        };
        match intent {
            AppIntent::None => {}
            AppIntent::Submit(text, mode) => {
                match start_run(&settings, &kernel, &app, &events_tx, text, mode) {
                    Ok(run) => active = Some(run),
                    Err(error) => app.set_error(&error.sql_state, &error.message),
                }
            }
            AppIntent::LocalCommand(command) => {
                if handle_local_command(
                    &command,
                    &mut terminal,
                    &store,
                    &mut settings,
                    &mut kernel,
                    &mut app,
                    active.as_ref(),
                )? {
                    break;
                }
            }
            AppIntent::Cancel => {
                if let Some(run) = active.as_ref() {
                    run.cancellation.cancel();
                    app.status = "正在取消…".to_owned();
                }
            }
            AppIntent::Decide {
                approval_id,
                approve,
            } => {
                let Some(run) = active.as_ref() else {
                    app.approval = None;
                    continue;
                };
                match run.engine.decide(AiApprovalDecision {
                    approval_id,
                    approve,
                }) {
                    Ok(()) => {
                        app.approval = None;
                        app.status = if approve {
                            "已批准，正在执行…".to_owned()
                        } else {
                            "已拒绝写入".to_owned()
                        };
                    }
                    Err(error) => app.set_error(&error.sql_state, &error.message),
                }
            }
            AppIntent::Exit => {
                reject_and_cancel(&mut app, active.as_ref());
                break;
            }
        }
    }

    reject_and_cancel(&mut app, active.as_ref());
    let audit = kernel.audit()?;
    store.save_persistence(app.visible_history(), audit)?;
    Ok(())
}

struct ActiveRun {
    run_id: String,
    engine: Arc<AiRunEngine>,
    cancellation: CancellationToken,
}

enum RuntimeEvent {
    Ai(AiRunEvent),
    Finished { run_id: String, result: Result<()> },
}

struct ChannelEventSink {
    sender: mpsc::Sender<RuntimeEvent>,
}

#[async_trait]
impl AiRunEventSink for ChannelEventSink {
    async fn emit(&self, event: AiRunEvent) -> Result<()> {
        self.sender
            .send(RuntimeEvent::Ai(event))
            .await
            .map_err(|_| DbError::new("57014", "TUI event channel is closed"))
    }
}

fn start_run(
    settings: &TuiSettingsV1,
    kernel: &TuiAgentKernel,
    app: &AppState,
    sender: &mpsc::Sender<RuntimeEvent>,
    text: String,
    mode: InputMode,
) -> Result<ActiveRun> {
    let engine = match mode {
        InputMode::Agent => kernel.natural_language_engine(&settings.provider)?,
        InputMode::Sql => kernel.sql_engine(&text)?,
    };
    let run_id = Uuid::new_v4().to_string();
    let mut history = app.visible_history();
    if history.last().is_some_and(|entry| entry.text == text) {
        history.pop();
    }
    let provider_settings = if mode == InputMode::Sql {
        AiProviderSettings {
            kind: AiProviderKind::Fake,
            model: "ordadb-local-sql".to_owned(),
            endpoint: None,
            reasoning: AiReasoningEffort::Low,
            data_sharing: settings.provider.data_sharing,
            credential_id: None,
        }
    } else {
        settings.provider.clone()
    };
    let request = AiRunRequest {
        run_id: run_id.clone(),
        connection_id: kernel_connection_id(settings),
        user_text: text,
        settings: provider_settings,
        history,
        include_sample_values: false,
    };
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let run_engine = Arc::clone(&engine);
    let sink = Arc::new(ChannelEventSink {
        sender: sender.clone(),
    });
    let finished_sender = sender.clone();
    let finished_run_id = run_id.clone();
    tokio::spawn(async move {
        let result = run_engine.run(request, sink, run_cancellation).await;
        let _ = finished_sender
            .send(RuntimeEvent::Finished {
                run_id: finished_run_id,
                result,
            })
            .await;
    });
    Ok(ActiveRun {
        run_id,
        engine,
        cancellation,
    })
}

fn apply_runtime_event(
    app: &mut AppState,
    kernel: &TuiAgentKernel,
    active: &mut Option<ActiveRun>,
    event: RuntimeEvent,
) -> Result<()> {
    match event {
        RuntimeEvent::Ai(event) => match event.payload {
            AiRunEventPayload::Started => app.status = "Agent 已启动".to_owned(),
            AiRunEventPayload::TextDelta { delta } => {
                app.append_assistant_delta(&delta, unix_time_millis())
            }
            AiRunEventPayload::ContextDisclosure { disclosure } => {
                app.status = format!("数据披露：{}", disclosure.redaction_summary)
            }
            AiRunEventPayload::ToolProposed { tool_name, .. } => {
                app.status = format!("命令预览：{tool_name}")
            }
            AiRunEventPayload::ToolStarted { tool_name, .. } => {
                app.status = format!("正在执行：{tool_name}");
                if let Some(preview) = kernel.take_preview()? {
                    app.push_message(
                        MessageRole::System,
                        format!("命令预览\n{preview}"),
                        unix_time_millis(),
                    );
                }
            }
            AiRunEventPayload::ToolCompleted { summary, .. } => {
                app.status = summary;
                if let Some(result) = kernel.take_result()? {
                    app.set_result(result);
                }
            }
            AiRunEventPayload::ApprovalRequired { request } => {
                app.status = "等待写入确认".to_owned();
                app.reject_focused = true;
                app.approval = Some(request);
            }
            AiRunEventPayload::ApprovalResolved { approved, .. } => {
                app.approval = None;
                app.status = if approved {
                    "写入已获一次性批准".to_owned()
                } else {
                    "写入已拒绝".to_owned()
                };
            }
            AiRunEventPayload::Usage { .. } => {}
            AiRunEventPayload::Cancelled => {
                app.busy = false;
                app.approval = None;
                app.status = "操作已取消".to_owned();
            }
            AiRunEventPayload::Completed => {
                app.busy = false;
                app.approval = None;
                app.status = "Agent 已完成".to_owned();
            }
            AiRunEventPayload::Error { error } => app.set_error(&error.sql_state, &error.message),
        },
        RuntimeEvent::Finished { run_id, result } => {
            if active.as_ref().is_some_and(|run| run.run_id == run_id) {
                *active = None;
            }
            app.busy = false;
            if let Err(error) = result
                && error.sql_state != "57014"
                && !app.status.starts_with("错误 ")
            {
                app.set_error(&error.sql_state, &error.message);
            }
        }
    }
    Ok(())
}

fn handle_local_command(
    command: &str,
    terminal: &mut TerminalSession,
    store: &TuiStateStore,
    settings: &mut TuiSettingsV1,
    kernel: &mut TuiAgentKernel,
    app: &mut AppState,
    active: Option<&ActiveRun>,
) -> Result<bool> {
    let arguments = command.split_whitespace().collect::<Vec<_>>();
    match arguments.first().copied().unwrap_or_default() {
        "/quit" | "/exit" => return Ok(true),
        "/help" => app.push_message(
            MessageRole::System,
            "/connect [地址] [用户] [数据库] · /provider <openai|compatible|ollama|fake> [端点/模型] · /sql · /agent · /history · /clear · /cancel · /quit".to_owned(),
            unix_time_millis(),
        ),
        "/sql" => {
            app.mode = InputMode::Sql;
            app.status = "已切换到 SQL 模式".to_owned();
        }
        "/agent" => {
            app.mode = InputMode::Agent;
            app.status = "已切换到自然语言模式".to_owned();
        }
        "/clear" => {
            app.transcript.clear();
            app.result = None;
        }
        "/history" => app.push_message(
            MessageRole::System,
            format!("当前保留 {} 条可见消息", app.visible_history().len()),
            unix_time_millis(),
        ),
        "/cancel" => {
            if let Some(run) = active {
                run.cancellation.cancel();
                app.status = "正在取消…".to_owned();
            } else {
                app.status = "当前没有可取消的操作".to_owned();
            }
        }
        "/connect" => {
            if let Some(address) = arguments.get(1) {
                settings.connection.address = (*address).to_owned();
            }
            if let Some(user) = arguments.get(2) {
                settings.connection.user = (*user).to_owned();
            }
            if let Some(database) = arguments.get(3) {
                settings.connection.database = (*database).to_owned();
            }
            settings.validate()?;
            terminal.suspend()?;
            let prompted = prompt_for_credential(
                &format!("OrdaDB/Console/{}", settings.connection.credential_id),
                &settings.connection.user,
                "OrdaDB 本地数据库凭据",
                "凭据只会保存到 Windows Credential Manager，不会进入终端历史。",
            );
            let resumed = terminal.resume();
            resumed?;
            if let Some(prompted) = prompted? {
                settings.connection.user = prompted.username.clone();
                CredentialVault::new("OrdaDB/Console")?.store(
                    &settings.connection.credential_id,
                    &prompted.username,
                    &prompted.password,
                )?;
                store.save_settings(settings)?;
                *kernel = build_kernel(settings)?;
                app.status = format!("已保存本地凭据 · {}", settings.connection.address);
            } else {
                app.status = "已取消凭据设置".to_owned();
            }
        }
        "/provider" => configure_provider(&arguments, terminal, store, settings, app)?,
        _ => app.push_message(
            MessageRole::Error,
            format!("未知本地命令：{command}"),
            unix_time_millis(),
        ),
    }
    Ok(false)
}

fn configure_provider(
    arguments: &[&str],
    terminal: &mut TerminalSession,
    store: &TuiStateStore,
    settings: &mut TuiSettingsV1,
    app: &mut AppState,
) -> Result<()> {
    let kind = arguments.get(1).copied().unwrap_or("openai");
    match kind {
        "openai" => {
            settings.provider.kind = AiProviderKind::OpenAi;
            settings.provider.endpoint = None;
            settings.provider.model = arguments
                .get(2)
                .map_or_else(|| "gpt-5.6".to_owned(), |model| (*model).to_owned());
            settings.provider.credential_id = Some("provider-openAi-default".to_owned());
        }
        "compatible" => {
            settings.provider.kind = AiProviderKind::OpenAiCompatible;
            settings.provider.endpoint = Some(
                arguments
                    .get(2)
                    .ok_or_else(|| invalid("/provider compatible requires an HTTPS endpoint"))?
                    .to_string(),
            );
            if let Some(model) = arguments.get(3) {
                settings.provider.model = (*model).to_owned();
            }
            settings.provider.credential_id = Some("provider-compatible-default".to_owned());
        }
        "ollama" => {
            settings.provider.kind = AiProviderKind::Ollama;
            settings.provider.endpoint = arguments.get(2).map(|value| (*value).to_owned());
            settings.provider.model = arguments
                .get(3)
                .map_or_else(|| "gpt-oss:20b".to_owned(), |model| (*model).to_owned());
            settings.provider.credential_id = None;
        }
        "fake" => {
            settings.provider.kind = AiProviderKind::Fake;
            settings.provider.endpoint = None;
            settings.provider.model = "ordadb-fake".to_owned();
            settings.provider.credential_id = None;
        }
        _ => {
            return Err(invalid(
                "provider must be openai, compatible, ollama, or fake",
            ));
        }
    }
    settings.validate()?;
    if matches!(
        settings.provider.kind,
        AiProviderKind::OpenAi | AiProviderKind::OpenAiCompatible
    ) {
        let credential_id = settings
            .provider
            .credential_id
            .clone()
            .ok_or_else(|| invalid("provider credential ID is missing"))?;
        terminal.suspend()?;
        let prompted = prompt_for_credential(
            &format!("OrdaDB/AI/{credential_id}"),
            "OpenAI",
            "OrdaDB AI Provider",
            "API Key 只会保存到 Windows Credential Manager。",
        );
        terminal.resume()?;
        if let Some(prompted) = prompted? {
            CredentialVault::new("OrdaDB/AI")?.store(
                &credential_id,
                &prompted.username,
                &prompted.password,
            )?;
        } else {
            app.status = "已取消 Provider 凭据设置".to_owned();
            return Ok(());
        }
    }
    store.save_settings(settings)?;
    app.status = format!("AI Provider 已设置为 {kind}");
    Ok(())
}

fn build_kernel(settings: &TuiSettingsV1) -> Result<TuiAgentKernel> {
    TuiAgentKernel::new(NativeExecutor::new(settings.connection.clone())?)
}

fn kernel_connection_id(settings: &TuiSettingsV1) -> String {
    format!(
        "ordadb-native://{}/{}?user={}",
        settings.connection.address, settings.connection.database, settings.connection.user
    )
}

fn reject_and_cancel(app: &mut AppState, active: Option<&ActiveRun>) {
    if let Some(run) = active {
        if let Some(approval) = app.approval.take() {
            let _ = run.engine.decide(AiApprovalDecision {
                approval_id: approval.approval_id,
                approve: false,
            });
        }
        run.cancellation.cancel();
    }
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}
