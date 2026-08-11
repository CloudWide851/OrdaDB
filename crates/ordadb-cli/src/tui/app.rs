use std::collections::VecDeque;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ordadb_ai::{AiApprovalRequest, AiHistoryEntry, AiHistoryRole};

use super::native::NativeQueryResult;

const MAX_INPUT_BYTES: usize = 64 * 1024;
const MAX_TRANSCRIPT_BYTES: usize = 1024 * 1024;
const MAX_MESSAGE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Agent,
    Sql,
}

impl InputMode {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Agent => "AGENT",
            Self::Sql => "SQL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageRole {
    User,
    Assistant,
    System,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptMessage {
    pub role: MessageRole,
    pub text: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppIntent {
    None,
    Submit(String, InputMode),
    LocalCommand(String),
    Cancel,
    Decide { approval_id: String, approve: bool },
    Exit,
}

#[derive(Debug)]
pub struct AppState {
    pub mode: InputMode,
    pub input: String,
    pub cursor: usize,
    pub transcript: VecDeque<TranscriptMessage>,
    pub history: VecDeque<String>,
    pub history_cursor: Option<usize>,
    pub result: Option<NativeQueryResult>,
    pub approval: Option<AiApprovalRequest>,
    pub reject_focused: bool,
    pub busy: bool,
    pub status: String,
    pub scroll: u16,
    transcript_limit: usize,
    history_limit: usize,
}

impl AppState {
    pub fn new(history_limit: usize, transcript_limit: usize) -> Self {
        Self {
            mode: InputMode::Agent,
            input: String::new(),
            cursor: 0,
            transcript: VecDeque::new(),
            history: VecDeque::new(),
            history_cursor: None,
            result: None,
            approval: None,
            reject_focused: true,
            busy: false,
            status: "未连接 · 输入 /connect 配置本地 OrdaDB".to_owned(),
            scroll: 0,
            transcript_limit,
            history_limit,
        }
    }

    pub fn restore_history(&mut self, entries: &[AiHistoryEntry]) {
        for entry in entries {
            self.push_message(
                match entry.role {
                    AiHistoryRole::User => MessageRole::User,
                    AiHistoryRole::Assistant => MessageRole::Assistant,
                },
                entry.text.clone(),
                entry.created_at_ms,
            );
        }
    }

    pub fn visible_history(&self) -> Vec<AiHistoryEntry> {
        self.transcript
            .iter()
            .filter_map(|message| {
                let role = match message.role {
                    MessageRole::User => AiHistoryRole::User,
                    MessageRole::Assistant => AiHistoryRole::Assistant,
                    MessageRole::System | MessageRole::Error => return None,
                };
                Some(AiHistoryEntry {
                    role,
                    text: message.text.clone(),
                    created_at_ms: message.created_at_ms,
                })
            })
            .collect()
    }

    pub fn push_message(&mut self, role: MessageRole, text: String, created_at_ms: u64) {
        let text = sanitize_bounded(&text, MAX_MESSAGE_BYTES);
        self.transcript.push_back(TranscriptMessage {
            role,
            text,
            created_at_ms,
        });
        self.enforce_transcript_bounds();
    }

    pub fn append_assistant_delta(&mut self, delta: &str, created_at_ms: u64) {
        let delta = sanitize_bounded(delta, MAX_MESSAGE_BYTES);
        if let Some(last) = self.transcript.back_mut()
            && last.role == MessageRole::Assistant
            && last.text.len().saturating_add(delta.len()) <= MAX_MESSAGE_BYTES
        {
            last.text.push_str(&delta);
        } else {
            self.push_message(MessageRole::Assistant, delta, created_at_ms);
        }
        self.enforce_transcript_bounds();
    }

    pub fn set_result(&mut self, result: NativeQueryResult) {
        self.status = format!(
            "查询完成 · {} 行{}",
            result.total_rows,
            if result.truncated {
                "（已截断）"
            } else {
                ""
            }
        );
        self.result = Some(result);
    }

    pub fn set_error(&mut self, sql_state: &str, message: &str) {
        self.busy = false;
        self.status = format!("错误 {sql_state}");
        self.push_message(
            MessageRole::Error,
            format!("SQLSTATE {sql_state} · {message}"),
            unix_time_millis(),
        );
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> AppIntent {
        if let Some(approval) = self.approval.as_ref() {
            return self.handle_approval_key(key, approval.approval_id.clone());
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return match key.code {
                KeyCode::Char('c') => {
                    if self.busy {
                        AppIntent::Cancel
                    } else {
                        AppIntent::Exit
                    }
                }
                KeyCode::Char('q') => AppIntent::Exit,
                _ => AppIntent::None,
            };
        }
        match key.code {
            KeyCode::F(2) => {
                self.mode = match self.mode {
                    InputMode::Agent => InputMode::Sql,
                    InputMode::Sql => InputMode::Agent,
                };
                self.status = format!("已切换到 {} 模式", self.mode.label());
                AppIntent::None
            }
            KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.insert_char('\n');
                AppIntent::None
            }
            KeyCode::Enter => self.submit(),
            KeyCode::Char(character) => {
                self.insert_char(character);
                AppIntent::None
            }
            KeyCode::Backspace => {
                self.backspace();
                AppIntent::None
            }
            KeyCode::Delete => {
                self.delete();
                AppIntent::None
            }
            KeyCode::Left => {
                self.cursor = previous_boundary(&self.input, self.cursor);
                AppIntent::None
            }
            KeyCode::Right => {
                self.cursor = next_boundary(&self.input, self.cursor);
                AppIntent::None
            }
            KeyCode::Home => {
                self.cursor = 0;
                AppIntent::None
            }
            KeyCode::End => {
                self.cursor = self.input.len();
                AppIntent::None
            }
            KeyCode::Up if !self.input.contains('\n') => {
                self.navigate_history(true);
                AppIntent::None
            }
            KeyCode::Down if !self.input.contains('\n') => {
                self.navigate_history(false);
                AppIntent::None
            }
            KeyCode::PageUp => {
                self.scroll = self.scroll.saturating_add(5);
                AppIntent::None
            }
            KeyCode::PageDown => {
                self.scroll = self.scroll.saturating_sub(5);
                AppIntent::None
            }
            KeyCode::Esc if self.busy => AppIntent::Cancel,
            KeyCode::Esc => {
                self.input.clear();
                self.cursor = 0;
                self.result = None;
                AppIntent::None
            }
            _ => AppIntent::None,
        }
    }

    fn handle_approval_key(&mut self, key: KeyEvent, approval_id: String) -> AppIntent {
        match key.code {
            KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
                self.reject_focused = !self.reject_focused;
                AppIntent::None
            }
            KeyCode::Char('y') | KeyCode::Char('Y') => AppIntent::Decide {
                approval_id,
                approve: true,
            },
            KeyCode::Enter => AppIntent::Decide {
                approval_id,
                approve: !self.reject_focused,
            },
            KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => AppIntent::Decide {
                approval_id,
                approve: false,
            },
            KeyCode::Char('q') if key.modifiers.contains(KeyModifiers::CONTROL) => AppIntent::Exit,
            _ => AppIntent::None,
        }
    }

    fn submit(&mut self) -> AppIntent {
        let submitted = self.input.trim().to_owned();
        if submitted.is_empty() || (self.busy && !submitted.starts_with('/')) {
            return AppIntent::None;
        }
        self.input.clear();
        self.cursor = 0;
        self.history_cursor = None;
        self.history.push_back(submitted.clone());
        while self.history.len() > self.history_limit {
            self.history.pop_front();
        }
        if submitted.starts_with('/') {
            return AppIntent::LocalCommand(submitted);
        }
        self.busy = true;
        self.status = "正在执行… Esc/Ctrl+C 取消".to_owned();
        self.push_message(MessageRole::User, submitted.clone(), unix_time_millis());
        AppIntent::Submit(submitted, self.mode)
    }

    fn insert_char(&mut self, character: char) {
        if self.input.len().saturating_add(character.len_utf8()) > MAX_INPUT_BYTES {
            return;
        }
        self.input.insert(self.cursor, character);
        self.cursor += character.len_utf8();
        self.history_cursor = None;
    }

    fn backspace(&mut self) {
        let previous = previous_boundary(&self.input, self.cursor);
        if previous < self.cursor {
            self.input.drain(previous..self.cursor);
            self.cursor = previous;
        }
    }

    fn delete(&mut self) {
        let next = next_boundary(&self.input, self.cursor);
        if next > self.cursor {
            self.input.drain(self.cursor..next);
        }
    }

    fn navigate_history(&mut self, older: bool) {
        if self.history.is_empty() {
            return;
        }
        let next = match (self.history_cursor, older) {
            (None, true) => Some(self.history.len() - 1),
            (Some(index), true) => Some(index.saturating_sub(1)),
            (Some(index), false) if index + 1 < self.history.len() => Some(index + 1),
            (_, false) => None,
        };
        self.history_cursor = next;
        self.input = next
            .and_then(|index| self.history.get(index).cloned())
            .unwrap_or_default();
        self.cursor = self.input.len();
    }

    fn enforce_transcript_bounds(&mut self) {
        while self.transcript.len() > self.transcript_limit
            || transcript_bytes(&self.transcript) > MAX_TRANSCRIPT_BYTES
        {
            self.transcript.pop_front();
        }
    }
}

fn transcript_bytes(messages: &VecDeque<TranscriptMessage>) -> usize {
    messages.iter().map(|message| message.text.len()).sum()
}

fn sanitize_bounded(value: &str, maximum: usize) -> String {
    let mut output = String::with_capacity(value.len().min(maximum));
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

fn previous_boundary(value: &str, cursor: usize) -> usize {
    value[..cursor]
        .char_indices()
        .next_back()
        .map_or(0, |(index, _)| index)
}

fn next_boundary(value: &str, cursor: usize) -> usize {
    value[cursor..]
        .char_indices()
        .nth(1)
        .map_or(value.len(), |(index, _)| cursor + index)
}

pub fn unix_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn unicode_editing_and_history_are_bounded_and_safe() {
        let mut app = AppState::new(2, 16);
        app.handle_key(key(KeyCode::Char('数')));
        app.handle_key(key(KeyCode::Char('据')));
        app.handle_key(key(KeyCode::Left));
        app.handle_key(key(KeyCode::Backspace));
        assert_eq!(app.input, "据");
        assert_eq!(app.cursor, 0);

        assert!(matches!(
            app.handle_key(key(KeyCode::Enter)),
            AppIntent::Submit(_, InputMode::Agent)
        ));
        app.busy = false;
        app.input = "second".to_owned();
        app.cursor = app.input.len();
        let _ = app.handle_key(key(KeyCode::Enter));
        app.busy = false;
        app.input = "third".to_owned();
        app.cursor = app.input.len();
        let _ = app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.history.len(), 2);
    }

    #[test]
    fn approval_defaults_to_reject_and_escape_never_approves() {
        let mut app = AppState::new(4, 16);
        app.approval = Some(AiApprovalRequest {
            approval_id: "approval".to_owned(),
            expires_in_ms: 120_000,
            connection_id: "connection".to_owned(),
            tool_name: "execute_sql".to_owned(),
            preview: "DELETE FROM items".to_owned(),
            impact_summary: "delete rows".to_owned(),
        });
        assert!(app.reject_focused);
        assert_eq!(
            app.handle_key(key(KeyCode::Enter)),
            AppIntent::Decide {
                approval_id: "approval".to_owned(),
                approve: false,
            }
        );
        assert_eq!(
            app.handle_key(key(KeyCode::Esc)),
            AppIntent::Decide {
                approval_id: "approval".to_owned(),
                approve: false,
            }
        );
    }

    #[test]
    fn control_characters_do_not_enter_visible_transcript() {
        let mut app = AppState::new(4, 16);
        app.push_message(MessageRole::System, "ok\u{1b}[31m\0".to_owned(), 0);
        assert_eq!(app.transcript[0].text, "ok[31m");
    }

    #[test]
    fn cancel_command_remains_available_during_active_work() {
        let mut app = AppState::new(4, 16);
        app.busy = true;
        app.input = "/cancel".to_owned();
        app.cursor = app.input.len();
        assert_eq!(
            app.handle_key(key(KeyCode::Enter)),
            AppIntent::LocalCommand("/cancel".to_owned())
        );
    }
}
