use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use ordadb_types::{DbError, Result};
use serde::{Deserialize, Serialize};

use crate::TransactionId;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LockKey {
    Database,
    Schema {
        schema_id: u64,
    },
    Table {
        table_id: u64,
    },
    Row {
        table_id: u64,
        version_id: u64,
    },
    IndexKey {
        index_id: u64,
        fingerprint: [u8; 32],
    },
    Maintenance {
        table_id: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LockMode {
    Shared,
    Update,
    Exclusive,
}

impl LockMode {
    const fn compatible(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Shared, Self::Shared)
                | (Self::Shared, Self::Update)
                | (Self::Update, Self::Shared)
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LockManagerOptions {
    pub default_timeout: Duration,
    pub poll_interval: Duration,
    pub maximum_waiters: usize,
}

impl Default for LockManagerOptions {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(50),
            maximum_waiters: 4096,
        }
    }
}

#[derive(Debug)]
pub struct LockManager {
    options: LockManagerOptions,
    default_timeout: RwLock<Duration>,
    next_request_id: AtomicU64,
    state: Mutex<LockState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct LockState {
    granted: BTreeMap<LockKey, Vec<GrantedLock>>,
    waiting: BTreeMap<LockKey, VecDeque<WaitRequest>>,
    deadlock_victims: BTreeSet<TransactionId>,
}

#[derive(Debug, Clone, Copy)]
struct GrantedLock {
    request_id: u64,
    transaction_id: TransactionId,
    mode: LockMode,
}

#[derive(Debug, Clone, Copy)]
struct WaitRequest {
    request_id: u64,
    transaction_id: TransactionId,
    mode: LockMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockSnapshot {
    pub transaction_id: TransactionId,
    pub key: LockKey,
    pub mode: LockMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockWaitSnapshot {
    pub transaction_id: TransactionId,
    pub key: LockKey,
    pub mode: LockMode,
    pub blocked_by: BTreeSet<TransactionId>,
}

impl LockManager {
    pub fn new(options: LockManagerOptions) -> Result<Arc<Self>> {
        if options.default_timeout.is_zero()
            || options.poll_interval.is_zero()
            || options.maximum_waiters == 0
        {
            return Err(DbError::new(
                "22023",
                "lock timeouts, polling interval, and waiter limit must be non-zero",
            ));
        }
        Ok(Arc::new(Self {
            default_timeout: RwLock::new(options.default_timeout),
            options,
            next_request_id: AtomicU64::new(1),
            state: Mutex::new(LockState::default()),
            changed: Condvar::new(),
        }))
    }

    pub fn acquire(
        self: &Arc<Self>,
        transaction_id: TransactionId,
        key: LockKey,
        mode: LockMode,
        timeout: Option<Duration>,
        cancelled: Option<&AtomicBool>,
    ) -> Result<LockGuard> {
        let timeout = match timeout {
            Some(timeout) => timeout,
            None => *self.default_timeout.read().map_err(|_| {
                DbError::internal("lock timeout configuration is poisoned")
                    .with_hint("restart the process before retrying lock acquisition")
            })?,
        };
        if timeout.is_zero() {
            return Err(DbError::new("22023", "lock timeout must be non-zero"));
        }
        let request_id = self
            .next_request_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| DbError::new("54000", "lock request ID space is exhausted"))?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| DbError::new("22023", "lock timeout is too large"))?;
        let mut state = self.lock_state()?;
        if can_grant_immediately(&state, transaction_id, &key, mode) {
            grant(&mut state, request_id, transaction_id, key.clone(), mode);
            return Ok(LockGuard::new(
                Arc::clone(self),
                request_id,
                transaction_id,
                key,
                mode,
            ));
        }
        let waiter_count = state.waiting.values().map(VecDeque::len).sum::<usize>();
        if waiter_count >= self.options.maximum_waiters {
            return Err(DbError::new("54000", "lock wait queue limit exceeded"));
        }
        state
            .waiting
            .entry(key.clone())
            .or_default()
            .push_back(WaitRequest {
                request_id,
                transaction_id,
                mode,
            });
        if let Some(victim) = deadlock_victim(&state) {
            state.deadlock_victims.insert(victim);
            self.changed.notify_all();
        }

        loop {
            if state.deadlock_victims.remove(&transaction_id) {
                remove_waiter(&mut state, &key, request_id);
                self.changed.notify_all();
                return Err(DbError::new(
                    "40P01",
                    format!("deadlock detected for transaction {transaction_id}"),
                )
                .with_hint("retry the transaction"));
            }
            if cancelled.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                remove_waiter(&mut state, &key, request_id);
                self.changed.notify_all();
                return Err(DbError::new("57014", "lock wait was cancelled"));
            }
            if waiter_can_run(&state, &key, request_id)? {
                remove_waiter(&mut state, &key, request_id);
                grant(&mut state, request_id, transaction_id, key.clone(), mode);
                return Ok(LockGuard::new(
                    Arc::clone(self),
                    request_id,
                    transaction_id,
                    key,
                    mode,
                ));
            }
            let now = Instant::now();
            if now >= deadline {
                remove_waiter(&mut state, &key, request_id);
                self.changed.notify_all();
                return Err(DbError::new(
                    "55P03",
                    format!("lock timeout for transaction {transaction_id}"),
                )
                .with_hint("retry after the conflicting transaction completes"));
            }
            let remaining = deadline.saturating_duration_since(now);
            let wait_for = remaining.min(self.options.poll_interval);
            let (next, _) = self
                .changed
                .wait_timeout(state, wait_for)
                .map_err(|_| DbError::internal("lock manager condition variable is poisoned"))?;
            state = next;
        }
    }

    pub fn release_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let mut state = self.lock_state()?;
        for granted in state.granted.values_mut() {
            granted.retain(|lock| lock.transaction_id != transaction_id);
        }
        state.granted.retain(|_, locks| !locks.is_empty());
        for waiting in state.waiting.values_mut() {
            waiting.retain(|request| request.transaction_id != transaction_id);
        }
        state.waiting.retain(|_, requests| !requests.is_empty());
        state.deadlock_victims.remove(&transaction_id);
        drop(state);
        self.changed.notify_all();
        Ok(())
    }

    pub fn set_default_timeout(&self, timeout: Duration) -> Result<()> {
        if timeout.is_zero() {
            return Err(DbError::new("22023", "lock timeout must be non-zero"));
        }
        *self.default_timeout.write().map_err(|_| {
            DbError::internal("lock timeout configuration is poisoned")
                .with_hint("restart the process before retrying lock configuration")
        })? = timeout;
        Ok(())
    }

    pub fn snapshot(&self) -> Result<(Vec<LockSnapshot>, Vec<LockWaitSnapshot>)> {
        let state = self.lock_state()?;
        let mut granted = Vec::new();
        for (key, locks) in &state.granted {
            granted.extend(locks.iter().map(|lock| LockSnapshot {
                transaction_id: lock.transaction_id,
                key: key.clone(),
                mode: lock.mode,
            }));
        }
        let graph = wait_for_graph(&state);
        let mut waiting = Vec::new();
        for (key, requests) in &state.waiting {
            waiting.extend(requests.iter().map(|request| {
                LockWaitSnapshot {
                    transaction_id: request.transaction_id,
                    key: key.clone(),
                    mode: request.mode,
                    blocked_by: graph
                        .get(&request.transaction_id)
                        .cloned()
                        .unwrap_or_default(),
                }
            }));
        }
        Ok((granted, waiting))
    }

    fn release_request(&self, key: &LockKey, request_id: u64) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(granted) = state.granted.get_mut(key) {
                granted.retain(|lock| lock.request_id != request_id);
                if granted.is_empty() {
                    state.granted.remove(key);
                }
            }
            drop(state);
            self.changed.notify_all();
        }
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, LockState>> {
        self.state.lock().map_err(|_| {
            DbError::internal("lock manager state is poisoned")
                .with_hint("restart the process before retrying transaction work")
        })
    }
}

#[derive(Debug)]
pub struct LockGuard {
    manager: Option<Arc<LockManager>>,
    request_id: u64,
    transaction_id: TransactionId,
    key: LockKey,
    mode: LockMode,
}

impl LockGuard {
    fn new(
        manager: Arc<LockManager>,
        request_id: u64,
        transaction_id: TransactionId,
        key: LockKey,
        mode: LockMode,
    ) -> Self {
        Self {
            manager: Some(manager),
            request_id,
            transaction_id,
            key,
            mode,
        }
    }

    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[must_use]
    pub const fn mode(&self) -> LockMode {
        self.mode
    }

    #[must_use]
    pub const fn key(&self) -> &LockKey {
        &self.key
    }

    pub fn release(&mut self) {
        if let Some(manager) = self.manager.take() {
            manager.release_request(&self.key, self.request_id);
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        self.release();
    }
}

fn can_grant_immediately(
    state: &LockState,
    transaction_id: TransactionId,
    key: &LockKey,
    mode: LockMode,
) -> bool {
    state.waiting.get(key).is_none_or(VecDeque::is_empty)
        && holders_compatible(state, transaction_id, key, mode)
}

fn holders_compatible(
    state: &LockState,
    transaction_id: TransactionId,
    key: &LockKey,
    mode: LockMode,
) -> bool {
    state.granted.get(key).is_none_or(|granted| {
        granted
            .iter()
            .all(|lock| lock.transaction_id == transaction_id || mode.compatible(lock.mode))
    })
}

fn waiter_can_run(state: &LockState, key: &LockKey, request_id: u64) -> Result<bool> {
    let requests = state
        .waiting
        .get(key)
        .ok_or_else(|| DbError::internal("lock wait request disappeared"))?;
    let position = requests
        .iter()
        .position(|request| request.request_id == request_id)
        .ok_or_else(|| DbError::internal("lock wait request disappeared"))?;
    let request = requests
        .get(position)
        .ok_or_else(|| DbError::internal("lock wait request position is invalid"))?;
    let earlier_compatible = requests.iter().take(position).all(|earlier| {
        earlier.transaction_id == request.transaction_id || request.mode.compatible(earlier.mode)
    });
    Ok(earlier_compatible && holders_compatible(state, request.transaction_id, key, request.mode))
}

fn grant(
    state: &mut LockState,
    request_id: u64,
    transaction_id: TransactionId,
    key: LockKey,
    mode: LockMode,
) {
    state.granted.entry(key).or_default().push(GrantedLock {
        request_id,
        transaction_id,
        mode,
    });
}

fn remove_waiter(state: &mut LockState, key: &LockKey, request_id: u64) {
    if let Some(waiting) = state.waiting.get_mut(key) {
        waiting.retain(|request| request.request_id != request_id);
        if waiting.is_empty() {
            state.waiting.remove(key);
        }
    }
}

fn wait_for_graph(state: &LockState) -> BTreeMap<TransactionId, BTreeSet<TransactionId>> {
    let mut graph = BTreeMap::<TransactionId, BTreeSet<TransactionId>>::new();
    for (key, requests) in &state.waiting {
        for (position, request) in requests.iter().enumerate() {
            let edges = graph.entry(request.transaction_id).or_default();
            if let Some(granted) = state.granted.get(key) {
                edges.extend(
                    granted
                        .iter()
                        .filter(|lock| {
                            lock.transaction_id != request.transaction_id
                                && !request.mode.compatible(lock.mode)
                        })
                        .map(|lock| lock.transaction_id),
                );
            }
            edges.extend(
                requests
                    .iter()
                    .take(position)
                    .filter(|earlier| {
                        earlier.transaction_id != request.transaction_id
                            && !request.mode.compatible(earlier.mode)
                    })
                    .map(|earlier| earlier.transaction_id),
            );
        }
    }
    graph
}

fn deadlock_victim(state: &LockState) -> Option<TransactionId> {
    let graph = wait_for_graph(state);
    let mut cycle_members = BTreeSet::new();
    for origin in graph.keys().copied() {
        let mut stack = vec![(origin, vec![origin])];
        while let Some((current, path)) = stack.pop() {
            let Some(next_nodes) = graph.get(&current) else {
                continue;
            };
            for next in next_nodes.iter().copied() {
                if next == origin {
                    cycle_members.extend(path.iter().copied());
                    cycle_members.insert(next);
                    continue;
                }
                if path.contains(&next) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.push(next);
                stack.push((next, next_path));
            }
        }
    }
    cycle_members.into_iter().max()
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;

    use super::*;

    fn manager() -> Arc<LockManager> {
        LockManager::new(LockManagerOptions {
            default_timeout: Duration::from_millis(250),
            poll_interval: Duration::from_millis(5),
            maximum_waiters: 32,
        })
        .expect("lock manager")
    }

    #[test]
    fn compatible_readers_share_and_writer_times_out() {
        let manager = manager();
        let first = TransactionId::new(10).expect("first ID");
        let second = TransactionId::new(11).expect("second ID");
        let third = TransactionId::new(12).expect("third ID");
        let key = LockKey::Table { table_id: 1 };
        let _first = manager
            .acquire(first, key.clone(), LockMode::Shared, None, None)
            .expect("first reader");
        let _second = manager
            .acquire(second, key.clone(), LockMode::Shared, None, None)
            .expect("second reader");
        let error = manager
            .acquire(
                third,
                key,
                LockMode::Exclusive,
                Some(Duration::from_millis(20)),
                None,
            )
            .expect_err("writer timeout");
        assert_eq!(error.sql_state, "55P03");
    }

    #[test]
    fn default_timeout_is_runtime_configurable() {
        let manager = manager();
        assert_eq!(
            manager
                .set_default_timeout(Duration::ZERO)
                .expect_err("zero timeout")
                .sql_state,
            "22023"
        );
        manager
            .set_default_timeout(Duration::from_millis(5))
            .expect("configure timeout");
        let owner = TransactionId::new(12).expect("owner");
        let waiter = TransactionId::new(13).expect("waiter");
        let key = LockKey::Row {
            table_id: 1,
            version_id: 1,
        };
        let _guard = manager
            .acquire(owner, key.clone(), LockMode::Exclusive, None, None)
            .expect("owner lock");
        assert_eq!(
            manager
                .acquire(waiter, key, LockMode::Exclusive, None, None)
                .expect_err("configured timeout")
                .sql_state,
            "55P03"
        );
    }

    #[test]
    fn fifo_waiter_acquires_after_guard_drop() {
        let manager = manager();
        let first = TransactionId::new(20).expect("first ID");
        let second = TransactionId::new(21).expect("second ID");
        let key = LockKey::Row {
            table_id: 1,
            version_id: 2,
        };
        let guard = manager
            .acquire(first, key.clone(), LockMode::Exclusive, None, None)
            .expect("owner");
        let worker_manager = Arc::clone(&manager);
        let (send, receive) = mpsc::channel();
        let worker = thread::spawn(move || {
            let acquired = worker_manager.acquire(
                second,
                key,
                LockMode::Exclusive,
                Some(Duration::from_secs(1)),
                None,
            );
            send.send(acquired.map(|guard| guard.transaction_id()))
                .expect("send result");
        });
        thread::sleep(Duration::from_millis(20));
        drop(guard);
        assert_eq!(
            receive
                .recv_timeout(Duration::from_secs(1))
                .expect("worker result")
                .expect("acquired"),
            second
        );
        worker.join().expect("worker join");
    }

    #[test]
    fn deadlock_selects_highest_transaction_as_victim() {
        let manager = manager();
        let low = TransactionId::new(30).expect("low ID");
        let high = TransactionId::new(31).expect("high ID");
        let left = LockKey::Row {
            table_id: 1,
            version_id: 1,
        };
        let right = LockKey::Row {
            table_id: 1,
            version_id: 2,
        };
        let low_left = manager
            .acquire(low, left.clone(), LockMode::Exclusive, None, None)
            .expect("low owns left");
        let high_right = manager
            .acquire(high, right.clone(), LockMode::Exclusive, None, None)
            .expect("high owns right");
        let high_manager = Arc::clone(&manager);
        let (send, receive) = mpsc::channel();
        let high_wait = thread::spawn(move || {
            let result = high_manager.acquire(
                high,
                left,
                LockMode::Exclusive,
                Some(Duration::from_secs(1)),
                None,
            );
            send.send(result.map(|_| ())).expect("send high result");
        });
        thread::sleep(Duration::from_millis(20));
        let low_manager = Arc::clone(&manager);
        let (send_low, receive_low) = mpsc::channel();
        let low_wait = thread::spawn(move || {
            let result = low_manager.acquire(
                low,
                right,
                LockMode::Exclusive,
                Some(Duration::from_secs(1)),
                None,
            );
            send_low
                .send(result.map(|guard| guard.transaction_id()))
                .expect("send low result");
        });
        let high_error = receive
            .recv_timeout(Duration::from_secs(1))
            .expect("high result")
            .expect_err("high transaction is victim");
        assert_eq!(high_error.sql_state, "40P01");
        manager
            .release_transaction(high)
            .expect("aborted victim releases all locks");
        assert_eq!(
            receive_low
                .recv_timeout(Duration::from_secs(1))
                .expect("low result")
                .expect("survivor acquires lock"),
            low
        );
        drop(low_left);
        drop(high_right);
        high_wait.join().expect("high waiter join");
        low_wait.join().expect("low waiter join");
    }

    #[test]
    fn cancellation_removes_wait_registration() {
        let manager = manager();
        let first = TransactionId::new(40).expect("first ID");
        let second = TransactionId::new(41).expect("second ID");
        let key = LockKey::Database;
        let _owner = manager
            .acquire(first, key.clone(), LockMode::Exclusive, None, None)
            .expect("owner");
        let cancelled = AtomicBool::new(true);
        let error = manager
            .acquire(second, key, LockMode::Exclusive, None, Some(&cancelled))
            .expect_err("cancelled");
        assert_eq!(error.sql_state, "57014");
        assert!(manager.snapshot().expect("snapshot").1.is_empty());
    }
}
