use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use serde::Serialize;
use tokio::sync::broadcast;

use ordadb_types::{DbError, Result};

const MAX_QUERY_HISTORY: usize = 512;
const EVENT_CHANNEL_CAPACITY: usize = 256;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub process_id: u32,
    pub user: String,
    pub database: String,
    pub application_name: Option<String>,
    pub connected_at: SystemTime,
    pub remote_address: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryOutcome {
    Running,
    Complete,
    Error,
    Cancelled,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryInfo {
    pub query_id: String,
    pub process_id: u32,
    pub sql: String,
    pub started_at: SystemTime,
    pub finished_at: Option<SystemTime>,
    pub rows_processed: u64,
    pub outcome: QueryOutcome,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum ServerEvent {
    SessionOpened(SessionInfo),
    SessionClosed { process_id: u32 },
    QueryChanged(QueryInfo),
    Notice { message: String },
}

#[derive(Debug, Clone)]
pub struct CancellationHandle {
    process_id: u32,
    secret_key: u32,
    cancelled: Arc<AtomicBool>,
}

impl CancellationHandle {
    #[must_use]
    pub const fn process_id(&self) -> u32 {
        self.process_id
    }

    #[must_use]
    pub const fn secret_key(&self) -> u32 {
        self.secret_key
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn cancellation_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }
}

struct RegistryState {
    sessions: BTreeMap<u32, SessionInfo>,
    cancellations: BTreeMap<u32, (u32, Arc<AtomicBool>)>,
    active_queries: BTreeMap<String, QueryInfo>,
    history: VecDeque<QueryInfo>,
}

pub struct SessionRegistry {
    next_process_id: AtomicU32,
    state: Mutex<RegistryState>,
    events: broadcast::Sender<ServerEvent>,
}

impl std::fmt::Debug for SessionRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionRegistry")
            .field("next_process_id", &self.next_process_id)
            .finish_non_exhaustive()
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            next_process_id: AtomicU32::new(10_000),
            state: Mutex::new(RegistryState {
                sessions: BTreeMap::new(),
                cancellations: BTreeMap::new(),
                active_queries: BTreeMap::new(),
                history: VecDeque::new(),
            }),
            events,
        }
    }
}

impl SessionRegistry {
    pub fn register_session(
        &self,
        user: String,
        database: String,
        application_name: Option<String>,
        remote_address: String,
        secret_key: u32,
    ) -> Result<CancellationHandle> {
        let process_id = self
            .next_process_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1).or(Some(10_000))
            })
            .map_err(|_| internal("session process ID allocation failed"))?;
        let info = SessionInfo {
            process_id,
            user,
            database,
            application_name,
            connected_at: SystemTime::now(),
            remote_address,
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut state = self.lock()?;
        state.sessions.insert(process_id, info.clone());
        state
            .cancellations
            .insert(process_id, (secret_key, Arc::clone(&cancelled)));
        drop(state);
        let _ = self.events.send(ServerEvent::SessionOpened(info));
        Ok(CancellationHandle {
            process_id,
            secret_key,
            cancelled,
        })
    }

    pub fn unregister_session(&self, process_id: u32) -> Result<()> {
        let mut state = self.lock()?;
        state.sessions.remove(&process_id);
        state.cancellations.remove(&process_id);
        let active: Vec<String> = state
            .active_queries
            .iter()
            .filter_map(|(query_id, query)| {
                (query.process_id == process_id).then_some(query_id.clone())
            })
            .collect();
        for query_id in active {
            if let Some(mut query) = state.active_queries.remove(&query_id) {
                query.finished_at = Some(SystemTime::now());
                query.outcome = QueryOutcome::Cancelled;
                push_history(&mut state.history, query);
            }
        }
        drop(state);
        let _ = self.events.send(ServerEvent::SessionClosed { process_id });
        Ok(())
    }

    pub fn cancel(&self, process_id: u32, secret_key: u32) -> Result<bool> {
        let state = self.lock()?;
        let Some((expected, flag)) = state.cancellations.get(&process_id) else {
            return Ok(false);
        };
        if *expected != secret_key {
            return Ok(false);
        }
        flag.store(true, Ordering::Release);
        Ok(true)
    }

    pub fn reset_cancellation(&self, process_id: u32) -> Result<()> {
        if let Some((_, flag)) = self.lock()?.cancellations.get(&process_id) {
            flag.store(false, Ordering::Release);
        }
        Ok(())
    }

    pub fn begin_query(&self, process_id: u32, query_id: String, sql: String) -> Result<()> {
        let query = QueryInfo {
            query_id: query_id.clone(),
            process_id,
            sql,
            started_at: SystemTime::now(),
            finished_at: None,
            rows_processed: 0,
            outcome: QueryOutcome::Running,
        };
        self.lock()?.active_queries.insert(query_id, query.clone());
        let _ = self.events.send(ServerEvent::QueryChanged(query));
        Ok(())
    }

    pub fn update_query_rows(&self, query_id: &str, rows_processed: u64) -> Result<()> {
        let mut state = self.lock()?;
        let Some(query) = state.active_queries.get_mut(query_id) else {
            return Ok(());
        };
        query.rows_processed = rows_processed;
        let event = query.clone();
        drop(state);
        let _ = self.events.send(ServerEvent::QueryChanged(event));
        Ok(())
    }

    pub fn finish_query(&self, query_id: &str, outcome: QueryOutcome) -> Result<()> {
        let mut state = self.lock()?;
        let Some(mut query) = state.active_queries.remove(query_id) else {
            return Ok(());
        };
        query.finished_at = Some(SystemTime::now());
        query.outcome = outcome;
        push_history(&mut state.history, query.clone());
        drop(state);
        let _ = self.events.send(ServerEvent::QueryChanged(query));
        Ok(())
    }

    pub fn sessions(&self) -> Result<Vec<SessionInfo>> {
        Ok(self.lock()?.sessions.values().cloned().collect())
    }

    pub fn queries(&self) -> Result<Vec<QueryInfo>> {
        let state = self.lock()?;
        let mut queries: Vec<QueryInfo> = state.active_queries.values().cloned().collect();
        queries.extend(state.history.iter().rev().cloned());
        Ok(queries)
    }

    pub fn active_session_count(&self) -> Result<usize> {
        Ok(self.lock()?.sessions.len())
    }

    pub fn active_query_count(&self) -> Result<usize> {
        Ok(self.lock()?.active_queries.len())
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ServerEvent> {
        self.events.subscribe()
    }

    pub fn notice(&self, message: impl Into<String>) {
        let _ = self.events.send(ServerEvent::Notice {
            message: message.into(),
        });
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, RegistryState>> {
        self.state
            .lock()
            .map_err(|_| internal("session registry lock is poisoned"))
    }
}

fn push_history(history: &mut VecDeque<QueryInfo>, query: QueryInfo) {
    history.push_back(query);
    while history.len() > MAX_QUERY_HISTORY {
        history.pop_front();
    }
}

fn internal(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message).with_hint("restart the service before retrying")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_requires_both_process_and_secret() {
        let registry = SessionRegistry::default();
        let handle = registry
            .register_session("dba".into(), "ordadb".into(), None, "127.0.0.1".into(), 42)
            .expect("register");
        assert!(!registry.cancel(handle.process_id(), 7).expect("wrong"));
        assert!(!handle.is_cancelled());
        assert!(registry.cancel(handle.process_id(), 42).expect("cancel"));
        assert!(handle.is_cancelled());
    }

    #[test]
    fn completed_query_moves_to_bounded_history() {
        let registry = SessionRegistry::default();
        let handle = registry
            .register_session("dba".into(), "ordadb".into(), None, "127.0.0.1".into(), 42)
            .expect("register");
        registry
            .begin_query(handle.process_id(), "query-1".into(), "SELECT 1".into())
            .expect("begin");
        registry.update_query_rows("query-1", 1).expect("rows");
        registry
            .finish_query("query-1", QueryOutcome::Complete)
            .expect("finish");
        let queries = registry.queries().expect("queries");
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].rows_processed, 1);
    }
}
