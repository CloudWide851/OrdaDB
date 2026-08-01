use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use ordadb_storage::{FROZEN_TRANSACTION_ID, TupleHeaderV2};
use ordadb_types::{DbError, Result};
use serde::{Deserialize, Serialize};

use crate::{TransactionId, TransactionStatusStore, WalManager};

const DEFAULT_MAXIMUM_SNAPSHOT_AGE_MILLIS: u64 = 30 * 60 * 1_000;
const SAFE_SNAPSHOT_CANCELLATION_POLL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IsolationLevel {
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransactionAccessMode {
    #[default]
    ReadWrite,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionCharacteristics {
    pub isolation_level: IsolationLevel,
    pub access_mode: TransactionAccessMode,
    pub deferrable: bool,
}

impl Default for TransactionCharacteristics {
    fn default() -> Self {
        Self {
            isolation_level: IsolationLevel::ReadCommitted,
            access_mode: TransactionAccessMode::ReadWrite,
            deferrable: false,
        }
    }
}

impl TransactionCharacteristics {
    pub fn validate(self) -> Result<Self> {
        if self.deferrable
            && (self.isolation_level != IsolationLevel::Serializable
                || self.access_mode != TransactionAccessMode::ReadOnly)
        {
            return Err(DbError::new(
                "25001",
                "DEFERRABLE requires a SERIALIZABLE READ ONLY transaction",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransactionOutcome {
    InProgress,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionSnapshot {
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub in_progress: Arc<BTreeSet<TransactionId>>,
    pub command_id: u32,
}

impl TransactionSnapshot {
    #[must_use]
    pub fn with_command_id(&self, command_id: u32) -> Self {
        Self {
            xmin: self.xmin,
            xmax: self.xmax,
            in_progress: Arc::clone(&self.in_progress),
            command_id,
        }
    }

    #[must_use]
    pub fn sees_transaction(&self, transaction_id: TransactionId) -> bool {
        transaction_id < self.xmax && !self.in_progress.contains(&transaction_id)
    }
}

pub trait TransactionStatusProvider {
    fn transaction_outcome(&self, transaction_id: TransactionId) -> Result<TransactionOutcome>;
}

#[derive(Debug)]
pub struct TransactionManager {
    next_transaction_id: AtomicU64,
    maximum_snapshot_age_millis: AtomicU64,
    state: Mutex<TransactionManagerState>,
    changed: Condvar,
}

#[derive(Debug)]
struct TransactionManagerState {
    statuses: BTreeMap<TransactionId, TransactionOutcome>,
    active: BTreeMap<TransactionId, ActiveTransaction>,
}

#[derive(Debug, Clone, Copy)]
struct ActiveTransaction {
    snapshot_xmin: TransactionId,
    snapshot_acquired_at: Instant,
    characteristics: TransactionCharacteristics,
}

impl TransactionManager {
    pub fn from_next_transaction_id(next_transaction_id: u64) -> Result<Arc<Self>> {
        let next_transaction_id = TransactionId::new(next_transaction_id)
            .ok_or_else(|| DbError::new("22023", "next transaction ID must be non-zero"))?;
        let frozen = TransactionId::new(FROZEN_TRANSACTION_ID)
            .ok_or_else(|| DbError::internal("frozen transaction ID must be non-zero"))?;
        Ok(Arc::new(Self {
            next_transaction_id: AtomicU64::new(next_transaction_id.get()),
            maximum_snapshot_age_millis: AtomicU64::new(DEFAULT_MAXIMUM_SNAPSHOT_AGE_MILLIS),
            state: Mutex::new(TransactionManagerState {
                statuses: BTreeMap::from([(frozen, TransactionOutcome::Committed)]),
                active: BTreeMap::new(),
            }),
            changed: Condvar::new(),
        }))
    }

    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_transaction_id: AtomicU64::new(FROZEN_TRANSACTION_ID + 1),
            maximum_snapshot_age_millis: AtomicU64::new(DEFAULT_MAXIMUM_SNAPSHOT_AGE_MILLIS),
            state: Mutex::new(TransactionManagerState {
                statuses: BTreeMap::new(),
                active: BTreeMap::new(),
            }),
            changed: Condvar::new(),
        })
    }

    pub fn from_status_snapshot(
        next_transaction_id: u64,
        statuses: BTreeMap<TransactionId, TransactionOutcome>,
    ) -> Result<Arc<Self>> {
        let next_transaction_id = TransactionId::new(next_transaction_id)
            .ok_or_else(|| DbError::new("22023", "next transaction ID must be non-zero"))?;
        Ok(Arc::new(Self {
            next_transaction_id: AtomicU64::new(next_transaction_id.get()),
            maximum_snapshot_age_millis: AtomicU64::new(DEFAULT_MAXIMUM_SNAPSHOT_AGE_MILLIS),
            state: Mutex::new(TransactionManagerState {
                statuses,
                active: BTreeMap::new(),
            }),
            changed: Condvar::new(),
        }))
    }

    pub fn begin(
        self: &Arc<Self>,
        characteristics: TransactionCharacteristics,
    ) -> Result<ManagedTransaction> {
        let transaction_id = self.allocate_transaction_id()?;
        self.register(transaction_id, characteristics)
    }

    pub fn register(
        self: &Arc<Self>,
        transaction_id: TransactionId,
        characteristics: TransactionCharacteristics,
    ) -> Result<ManagedTransaction> {
        let characteristics = characteristics.validate()?;
        let next_transaction_id = transaction_id
            .get()
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "transaction ID space is exhausted"))?;
        self.next_transaction_id
            .fetch_max(next_transaction_id, Ordering::AcqRel);
        let mut state = self.lock_state()?;
        if state.active.contains_key(&transaction_id)
            || state.statuses.contains_key(&transaction_id)
        {
            return Err(DbError::new(
                "25000",
                format!("transaction {transaction_id} is already registered"),
            ));
        }
        let snapshot = self.snapshot_locked(&state, transaction_id, 0)?;
        state
            .statuses
            .insert(transaction_id, TransactionOutcome::InProgress);
        state.active.insert(
            transaction_id,
            ActiveTransaction {
                snapshot_xmin: snapshot.xmin,
                snapshot_acquired_at: Instant::now(),
                characteristics,
            },
        );
        drop(state);
        Ok(ManagedTransaction {
            manager: Some(Arc::clone(self)),
            transaction_id,
            characteristics,
            snapshot,
            command_id: 0,
            safe_snapshot_acquired: !characteristics.deferrable,
        })
    }

    pub fn global_xmin(&self) -> Result<TransactionId> {
        let state = self.lock_state()?;
        state
            .active
            .values()
            .map(|active| active.snapshot_xmin)
            .min()
            .or_else(|| TransactionId::new(self.next_transaction_id.load(Ordering::Acquire)))
            .ok_or_else(|| DbError::internal("transaction ID high-water mark became zero"))
    }

    pub fn global_xmin_excluding(&self, transaction_id: TransactionId) -> Result<TransactionId> {
        let state = self.lock_state()?;
        state
            .active
            .iter()
            .filter(|(active_id, _)| **active_id != transaction_id)
            .map(|(_, active)| active.snapshot_xmin)
            .min()
            .or_else(|| TransactionId::new(self.next_transaction_id.load(Ordering::Acquire)))
            .ok_or_else(|| DbError::internal("transaction ID high-water mark became zero"))
    }

    pub fn active_transactions(&self) -> Result<BTreeSet<TransactionId>> {
        Ok(self.lock_state()?.active.keys().copied().collect())
    }

    pub fn compact_before(&self, horizon: TransactionId) -> Result<usize> {
        let mut state = self.lock_state()?;
        let before = state.statuses.len();
        state.statuses.retain(|transaction_id, outcome| {
            transaction_id.get() == FROZEN_TRANSACTION_ID
                || *transaction_id >= horizon
                || *outcome == TransactionOutcome::InProgress
        });
        Ok(before.saturating_sub(state.statuses.len()))
    }

    pub fn set_maximum_snapshot_age(&self, maximum_age: Duration) -> Result<()> {
        let maximum_age_millis = u64::try_from(maximum_age.as_millis()).map_err(|_| {
            DbError::new(
                "22023",
                "maximum snapshot age exceeds the supported duration",
            )
        })?;
        if maximum_age_millis == 0 {
            return Err(DbError::new(
                "22023",
                "maximum snapshot age must be at least one millisecond",
            ));
        }
        self.maximum_snapshot_age_millis
            .store(maximum_age_millis, Ordering::Release);
        Ok(())
    }

    pub fn expired_snapshot(&self) -> Result<Option<TransactionId>> {
        let maximum_age =
            Duration::from_millis(self.maximum_snapshot_age_millis.load(Ordering::Acquire));
        Ok(self
            .lock_state()?
            .active
            .iter()
            .filter(|(_, active)| {
                active.characteristics.isolation_level != IsolationLevel::ReadCommitted
                    && active.snapshot_acquired_at.elapsed() > maximum_age
            })
            .min_by_key(|(_, active)| active.snapshot_acquired_at)
            .map(|(transaction_id, _)| *transaction_id))
    }

    pub fn characteristics(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionCharacteristics>> {
        Ok(self
            .lock_state()?
            .active
            .get(&transaction_id)
            .map(|active| active.characteristics))
    }

    fn allocate_transaction_id(&self) -> Result<TransactionId> {
        let value = self
            .next_transaction_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| DbError::new("54000", "transaction ID space is exhausted"))?;
        TransactionId::new(value)
            .ok_or_else(|| DbError::internal("transaction manager generated transaction ID zero"))
    }

    fn refresh_snapshot(
        &self,
        transaction_id: TransactionId,
        command_id: u32,
    ) -> Result<TransactionSnapshot> {
        let mut state = self.lock_state()?;
        if !state.active.contains_key(&transaction_id) {
            return Err(DbError::new("25P01", "transaction is no longer active"));
        }
        let snapshot = self.snapshot_locked(&state, transaction_id, command_id)?;
        if let Some(active) = state.active.get_mut(&transaction_id) {
            active.snapshot_xmin = snapshot.xmin;
            active.snapshot_acquired_at = Instant::now();
        }
        Ok(snapshot)
    }

    fn wait_for_safe_snapshot(
        &self,
        transaction_id: TransactionId,
        command_id: u32,
        cancellation: Option<&AtomicBool>,
    ) -> Result<TransactionSnapshot> {
        let mut state = self.lock_state()?;
        loop {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err(DbError::new(
                    "57014",
                    "canceling statement while waiting for a safe snapshot",
                ));
            }
            if !state.active.contains_key(&transaction_id) {
                return Err(DbError::new("25P01", "transaction is no longer active"));
            }
            let unsafe_writer_active = state.active.iter().any(|(active_id, active)| {
                *active_id != transaction_id
                    && active.characteristics.isolation_level == IsolationLevel::Serializable
                    && active.characteristics.access_mode == TransactionAccessMode::ReadWrite
            });
            if !unsafe_writer_active {
                break;
            }
            (state, _) = self
                .changed
                .wait_timeout(state, SAFE_SNAPSHOT_CANCELLATION_POLL)
                .map_err(|_| {
                    DbError::internal("transaction safe-snapshot wait lock is poisoned")
                        .with_hint("restart the process before retrying the transaction")
                })?;
        }
        let snapshot = self.snapshot_locked(&state, transaction_id, command_id)?;
        if let Some(active) = state.active.get_mut(&transaction_id) {
            active.snapshot_xmin = snapshot.xmin;
            active.snapshot_acquired_at = Instant::now();
        }
        Ok(snapshot)
    }

    fn snapshot_locked(
        &self,
        state: &TransactionManagerState,
        transaction_id: TransactionId,
        command_id: u32,
    ) -> Result<TransactionSnapshot> {
        let xmax_value = self.next_transaction_id.load(Ordering::Acquire);
        let xmax = TransactionId::new(xmax_value)
            .ok_or_else(|| DbError::internal("transaction high-water mark became zero"))?;
        let in_progress = state
            .active
            .keys()
            .copied()
            .filter(|active| *active != transaction_id)
            .collect::<BTreeSet<_>>();
        let xmin = in_progress.iter().copied().min().unwrap_or(transaction_id);
        Ok(TransactionSnapshot {
            xmin,
            xmax,
            in_progress: Arc::new(in_progress),
            command_id,
        })
    }

    fn complete(&self, transaction_id: TransactionId, outcome: TransactionOutcome) -> Result<()> {
        let mut state = self.lock_state()?;
        match state.statuses.get(&transaction_id) {
            Some(TransactionOutcome::InProgress) => {}
            Some(existing) if *existing == outcome => return Ok(()),
            Some(_) => {
                return Err(DbError::new(
                    "25000",
                    "transaction already has a different terminal outcome",
                ));
            }
            None => return Err(DbError::new("25P01", "transaction is not registered")),
        }
        state.active.remove(&transaction_id);
        state.statuses.insert(transaction_id, outcome);
        drop(state);
        self.changed.notify_all();
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, TransactionManagerState>> {
        self.state.lock().map_err(|_| {
            DbError::internal("transaction manager lock is poisoned")
                .with_hint("restart the process before retrying transaction work")
        })
    }
}

impl TransactionStatusProvider for TransactionManager {
    fn transaction_outcome(&self, transaction_id: TransactionId) -> Result<TransactionOutcome> {
        self.lock_state()?
            .statuses
            .get(&transaction_id)
            .copied()
            .ok_or_else(|| {
                DbError::new(
                    "XX001",
                    format!("transaction {transaction_id} has no durable status"),
                )
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableTransactionPhase {
    Active,
    StatusCommitted,
    Finished,
}

#[derive(Debug)]
pub struct DurableTransaction {
    managed: Option<ManagedTransaction>,
    transaction_id: TransactionId,
    status: Arc<TransactionStatusStore>,
    wal: Arc<WalManager>,
    phase: DurableTransactionPhase,
}

impl DurableTransaction {
    pub fn begin(
        manager: &Arc<TransactionManager>,
        status: Arc<TransactionStatusStore>,
        wal: Arc<WalManager>,
        characteristics: TransactionCharacteristics,
    ) -> Result<Self> {
        let transaction_id = status.begin_durable(&wal)?;
        let managed = match manager.register(transaction_id, characteristics) {
            Ok(managed) => managed,
            Err(error) => {
                let _ = wal.abort(transaction_id);
                let _ = status.abort(transaction_id);
                return Err(error);
            }
        };
        Ok(Self {
            managed: Some(managed),
            transaction_id,
            status,
            wal,
            phase: DurableTransactionPhase::Active,
        })
    }

    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[must_use]
    pub fn characteristics(&self) -> Option<TransactionCharacteristics> {
        self.managed
            .as_ref()
            .map(ManagedTransaction::characteristics)
    }

    #[must_use]
    pub fn snapshot(&self) -> Option<&TransactionSnapshot> {
        self.managed.as_ref().map(ManagedTransaction::snapshot)
    }

    pub fn begin_statement(&mut self) -> Result<&TransactionSnapshot> {
        self.managed_mut()?.begin_statement()
    }

    pub fn begin_statement_with_cancellation(
        &mut self,
        cancellation: &AtomicBool,
    ) -> Result<&TransactionSnapshot> {
        self.managed_mut()?
            .begin_statement_with_cancellation(cancellation)
    }

    pub fn finish_statement(&mut self) -> Result<()> {
        self.managed_mut()?.finish_statement()
    }

    pub fn mark_status_committed(&mut self) -> Result<()> {
        if self.phase != DurableTransactionPhase::Active {
            return Err(DbError::new("25000", "transaction status is not active"));
        }
        self.status.commit(self.transaction_id())?;
        self.phase = DurableTransactionPhase::StatusCommitted;
        Ok(())
    }

    pub fn finish_commit(&mut self) -> Result<()> {
        if self.phase != DurableTransactionPhase::StatusCommitted {
            return Err(DbError::new(
                "25000",
                "transaction status was not committed before WAL Commit",
            ));
        }
        self.managed
            .take()
            .ok_or_else(|| DbError::new("25P01", "transaction is no longer active"))?
            .commit()?;
        self.phase = DurableTransactionPhase::Finished;
        Ok(())
    }

    pub fn commit_empty(mut self) -> Result<()> {
        self.mark_status_committed()?;
        self.wal.commit_transaction(self.transaction_id())?;
        self.finish_commit()
    }

    pub fn abort(mut self) -> Result<()> {
        self.abort_active()
    }

    fn abort_active(&mut self) -> Result<()> {
        if self.phase == DurableTransactionPhase::Finished {
            return Ok(());
        }
        if self.phase == DurableTransactionPhase::StatusCommitted {
            return Err(
                DbError::new("25000", "transaction already published committed status").with_hint(
                    "reopen the database so crash recovery can reconcile the WAL outcome",
                ),
            );
        }
        let transaction_id = self.transaction_id();
        self.wal.abort(transaction_id)?;
        self.status.abort(transaction_id)?;
        if let Some(managed) = self.managed.take() {
            managed.abort()?;
        }
        self.phase = DurableTransactionPhase::Finished;
        Ok(())
    }

    fn managed_mut(&mut self) -> Result<&mut ManagedTransaction> {
        self.managed
            .as_mut()
            .ok_or_else(|| DbError::new("25P01", "transaction is no longer active"))
    }
}

impl Drop for DurableTransaction {
    fn drop(&mut self) {
        match self.phase {
            DurableTransactionPhase::Active => {
                let _ = self.abort_active();
            }
            DurableTransactionPhase::StatusCommitted => {
                if let Some(managed) = self.managed.take() {
                    drop(managed);
                }
            }
            DurableTransactionPhase::Finished => {}
        }
    }
}

#[derive(Debug)]
pub struct ManagedTransaction {
    manager: Option<Arc<TransactionManager>>,
    transaction_id: TransactionId,
    characteristics: TransactionCharacteristics,
    snapshot: TransactionSnapshot,
    command_id: u32,
    safe_snapshot_acquired: bool,
}

impl ManagedTransaction {
    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[must_use]
    pub const fn characteristics(&self) -> TransactionCharacteristics {
        self.characteristics
    }

    #[must_use]
    pub const fn snapshot(&self) -> &TransactionSnapshot {
        &self.snapshot
    }

    pub fn begin_statement(&mut self) -> Result<&TransactionSnapshot> {
        self.begin_statement_controlled(None)
    }

    pub fn begin_statement_with_cancellation(
        &mut self,
        cancellation: &AtomicBool,
    ) -> Result<&TransactionSnapshot> {
        self.begin_statement_controlled(Some(cancellation))
    }

    fn begin_statement_controlled(
        &mut self,
        cancellation: Option<&AtomicBool>,
    ) -> Result<&TransactionSnapshot> {
        if self.characteristics.deferrable && !self.safe_snapshot_acquired {
            self.snapshot = self.manager()?.wait_for_safe_snapshot(
                self.transaction_id,
                self.command_id,
                cancellation,
            )?;
            self.safe_snapshot_acquired = true;
        } else if self.characteristics.isolation_level == IsolationLevel::ReadCommitted {
            self.snapshot = self
                .manager()?
                .refresh_snapshot(self.transaction_id, self.command_id)?;
        }
        Ok(&self.snapshot)
    }

    pub fn finish_statement(&mut self) -> Result<()> {
        self.command_id = self
            .command_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "transaction command ID space is exhausted"))?;
        self.snapshot = self.snapshot.with_command_id(self.command_id);
        Ok(())
    }

    pub fn commit(mut self) -> Result<()> {
        self.manager()?
            .complete(self.transaction_id, TransactionOutcome::Committed)?;
        self.manager = None;
        Ok(())
    }

    pub fn abort(mut self) -> Result<()> {
        self.manager()?
            .complete(self.transaction_id, TransactionOutcome::Aborted)?;
        self.manager = None;
        Ok(())
    }

    fn manager(&self) -> Result<&TransactionManager> {
        self.manager
            .as_deref()
            .ok_or_else(|| DbError::new("25P01", "transaction is no longer active"))
    }
}

impl Drop for ManagedTransaction {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.take() {
            let _ = manager.complete(self.transaction_id, TransactionOutcome::Aborted);
        }
    }
}

pub fn tuple_visible(
    header: TupleHeaderV2,
    snapshot: &TransactionSnapshot,
    current_transaction: TransactionId,
    statuses: &impl TransactionStatusProvider,
) -> Result<bool> {
    let creator = TransactionId::new(header.xmin)
        .ok_or_else(|| DbError::new("XX001", "tuple creator transaction ID is zero"))?;
    let creator_visible = if creator.get() == FROZEN_TRANSACTION_ID {
        true
    } else if creator == current_transaction {
        header.command_id < snapshot.command_id
    } else {
        statuses.transaction_outcome(creator)? == TransactionOutcome::Committed
            && snapshot.sees_transaction(creator)
    };
    if !creator_visible {
        return Ok(false);
    }
    let Some(deleter) = TransactionId::new(header.xmax) else {
        return Ok(true);
    };
    if deleter == current_transaction {
        return Ok(header.command_id >= snapshot.command_id);
    }
    let deletion_visible = statuses.transaction_outcome(deleter)? == TransactionOutcome::Committed
        && snapshot.sees_transaction(deleter);
    Ok(!deletion_visible)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SavepointId(NonZeroU64);

impl SavepointId {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Savepoint {
    pub id: SavepointId,
    pub name: String,
    pub command_id: u32,
    pub mutation_len: usize,
    pub lock_len: usize,
}

#[derive(Debug, Default)]
pub struct SavepointStack {
    next_id: u64,
    frames: Vec<Savepoint>,
}

impl SavepointStack {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next_id: 1,
            frames: Vec::new(),
        }
    }

    pub fn push(
        &mut self,
        name: impl Into<String>,
        command_id: u32,
        mutation_len: usize,
        lock_len: usize,
    ) -> Result<SavepointId> {
        let id = NonZeroU64::new(self.next_id)
            .map(SavepointId)
            .ok_or_else(|| DbError::new("54000", "savepoint ID space is exhausted"))?;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "savepoint ID space is exhausted"))?;
        self.frames.push(Savepoint {
            id,
            name: name.into(),
            command_id,
            mutation_len,
            lock_len,
        });
        Ok(id)
    }

    pub fn rollback_to(&mut self, name: &str) -> Result<Savepoint> {
        let index = self.find(name)?;
        self.frames.truncate(index + 1);
        self.frames
            .get(index)
            .cloned()
            .ok_or_else(|| DbError::internal("savepoint disappeared during rollback"))
    }

    pub fn release(&mut self, name: &str) -> Result<Savepoint> {
        let index = self.find(name)?;
        let released = self
            .frames
            .get(index)
            .cloned()
            .ok_or_else(|| DbError::internal("savepoint disappeared during release"))?;
        self.frames.truncate(index);
        Ok(released)
    }

    #[must_use]
    pub fn frames(&self) -> &[Savepoint] {
        &self.frames
    }

    fn find(&self, name: &str) -> Result<usize> {
        self.frames
            .iter()
            .rposition(|frame| frame.name == name)
            .ok_or_else(|| DbError::new("3B001", format!("savepoint \"{name}\" does not exist")))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use ordadb_storage::{FROZEN_TRANSACTION_ID, TupleHeaderV2};
    use tempfile::tempdir;

    use super::*;

    #[derive(Debug, Default)]
    struct Statuses(BTreeMap<TransactionId, TransactionOutcome>);

    impl TransactionStatusProvider for Statuses {
        fn transaction_outcome(&self, transaction_id: TransactionId) -> Result<TransactionOutcome> {
            self.0
                .get(&transaction_id)
                .copied()
                .ok_or_else(|| DbError::new("XX001", "missing test transaction status"))
        }
    }

    #[test]
    fn read_committed_refreshes_while_repeatable_read_retains_snapshot() {
        let manager = TransactionManager::new();
        let mut read_committed = manager
            .begin(TransactionCharacteristics::default())
            .expect("read committed");
        let mut repeatable = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::RepeatableRead,
                ..TransactionCharacteristics::default()
            })
            .expect("repeatable read");
        let writer = manager
            .begin(TransactionCharacteristics::default())
            .expect("writer");
        let writer_id = writer.transaction_id();
        assert!(
            read_committed
                .begin_statement()
                .expect("first snapshot")
                .in_progress
                .contains(&writer_id)
        );
        let repeatable_xmax = repeatable.snapshot().xmax;
        writer.commit().expect("writer commit");
        assert!(
            !read_committed
                .begin_statement()
                .expect("refreshed snapshot")
                .in_progress
                .contains(&writer_id)
        );
        assert_eq!(
            repeatable
                .begin_statement()
                .expect("retained snapshot")
                .xmax,
            repeatable_xmax
        );
    }

    #[test]
    fn dropped_transaction_is_aborted_and_removed_from_horizon() {
        let manager = TransactionManager::new();
        let transaction = manager
            .begin(TransactionCharacteristics::default())
            .expect("transaction");
        let transaction_id = transaction.transaction_id();
        drop(transaction);
        assert_eq!(
            manager
                .transaction_outcome(transaction_id)
                .expect("terminal status"),
            TransactionOutcome::Aborted
        );
        assert!(
            !manager
                .active_transactions()
                .expect("active set")
                .contains(&transaction_id)
        );
    }

    #[test]
    fn repeatable_snapshot_age_is_bounded_and_configurable() {
        let manager = TransactionManager::new();
        assert_eq!(
            manager
                .set_maximum_snapshot_age(Duration::ZERO)
                .expect_err("zero age")
                .sql_state,
            "22023"
        );
        manager
            .set_maximum_snapshot_age(Duration::from_millis(1))
            .expect("configure maximum age");
        let transaction = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::RepeatableRead,
                ..TransactionCharacteristics::default()
            })
            .expect("repeatable transaction");
        let transaction_id = transaction.transaction_id();
        thread::sleep(Duration::from_millis(5));
        assert_eq!(
            manager.expired_snapshot().expect("expired snapshot"),
            Some(transaction_id)
        );
        transaction.abort().expect("abort");
        assert_eq!(manager.expired_snapshot().expect("empty horizon"), None);
    }

    #[test]
    fn deferrable_requires_serializable_read_only() {
        let error = TransactionCharacteristics {
            deferrable: true,
            ..TransactionCharacteristics::default()
        }
        .validate()
        .expect_err("invalid deferrable");
        assert_eq!(error.sql_state, "25001");
        TransactionCharacteristics {
            isolation_level: IsolationLevel::Serializable,
            access_mode: TransactionAccessMode::ReadOnly,
            deferrable: true,
        }
        .validate()
        .expect("valid deferrable");
    }

    #[test]
    fn deferrable_reader_waits_for_a_safe_serializable_snapshot() {
        let manager = TransactionManager::new();
        let writer = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::Serializable,
                ..TransactionCharacteristics::default()
            })
            .expect("serializable writer");
        let writer_id = writer.transaction_id();
        let mut reader = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::Serializable,
                access_mode: TransactionAccessMode::ReadOnly,
                deferrable: true,
            })
            .expect("deferrable reader");
        let (send, receive) = mpsc::channel();
        let worker = thread::spawn(move || {
            let result = reader.begin_statement().cloned();
            let _ = reader.abort();
            send.send(result).expect("send snapshot");
        });
        thread::sleep(Duration::from_millis(20));
        assert!(matches!(receive.try_recv(), Err(mpsc::TryRecvError::Empty)));
        writer.commit().expect("writer commit");
        let snapshot = receive
            .recv_timeout(Duration::from_secs(1))
            .expect("safe snapshot result")
            .expect("safe snapshot");
        assert!(!snapshot.in_progress.contains(&writer_id));
        worker.join().expect("reader join");
    }

    #[test]
    fn deferrable_safe_snapshot_wait_is_cancellable() {
        let manager = TransactionManager::new();
        let writer = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::Serializable,
                ..TransactionCharacteristics::default()
            })
            .expect("serializable writer");
        let mut reader = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::Serializable,
                access_mode: TransactionAccessMode::ReadOnly,
                deferrable: true,
            })
            .expect("deferrable reader");
        let cancellation = Arc::new(AtomicBool::new(false));
        let worker_cancellation = Arc::clone(&cancellation);
        let (send, receive) = mpsc::channel();
        let worker = thread::spawn(move || {
            let result = reader
                .begin_statement_with_cancellation(worker_cancellation.as_ref())
                .map(|_| ());
            let _ = reader.abort();
            send.send(result).expect("send cancellation result");
        });
        thread::sleep(Duration::from_millis(20));
        cancellation.store(true, Ordering::Release);

        let error = receive
            .recv_timeout(Duration::from_secs(1))
            .expect("cancellation result")
            .expect_err("safe snapshot wait cancelled");
        assert_eq!(error.sql_state, "57014");
        writer.abort().expect("writer abort");
        worker.join().expect("reader join");
    }

    #[test]
    fn tuple_visibility_respects_snapshot_and_terminal_status() {
        let current = TransactionId::new(20).expect("current ID");
        let creator = TransactionId::new(10).expect("creator ID");
        let deleter = TransactionId::new(12).expect("deleter ID");
        let mut statuses = Statuses::default();
        statuses.0.insert(creator, TransactionOutcome::Committed);
        statuses.0.insert(deleter, TransactionOutcome::Aborted);
        let snapshot = TransactionSnapshot {
            xmin: creator,
            xmax: current,
            in_progress: Arc::new(BTreeSet::new()),
            command_id: 3,
        };
        let header = TupleHeaderV2 {
            flags: 0,
            column_count: 1,
            xmin: creator.get(),
            xmax: deleter.get(),
            command_id: 1,
            previous_version: 0,
        };
        assert!(tuple_visible(header, &snapshot, current, &statuses).expect("visible"));
        statuses.0.insert(deleter, TransactionOutcome::Committed);
        assert!(!tuple_visible(header, &snapshot, current, &statuses).expect("deleted"));
        let frozen = TupleHeaderV2 {
            xmin: FROZEN_TRANSACTION_ID,
            xmax: 0,
            ..header
        };
        assert!(tuple_visible(frozen, &snapshot, current, &statuses).expect("frozen"));
    }

    #[test]
    fn savepoint_names_use_nearest_scope_and_release_descendants() {
        let mut stack = SavepointStack::new();
        let first = stack.push("same", 1, 2, 3).expect("first savepoint");
        stack.push("nested", 2, 4, 5).expect("nested savepoint");
        let nearest = stack.push("same", 3, 6, 7).expect("nearest savepoint");
        assert_ne!(first, nearest);
        assert_eq!(stack.rollback_to("same").expect("rollback").id, nearest);
        assert_eq!(stack.frames().len(), 3);
        assert_eq!(stack.release("nested").expect("release").name, "nested");
        assert_eq!(stack.frames().len(), 1);
        assert_eq!(
            stack
                .rollback_to("missing")
                .expect_err("unknown savepoint")
                .sql_state,
            "3B001"
        );
    }

    #[test]
    fn durable_transaction_commit_and_drop_keep_status_and_wal_aligned() {
        let directory = tempdir().expect("tempdir");
        let wal = WalManager::open(directory.path()).expect("wal");
        let status = Arc::new(TransactionStatusStore::open(directory.path(), 9).expect("status"));
        let snapshot = status.snapshot().expect("status snapshot");
        let manager = TransactionManager::from_status_snapshot(
            snapshot.next_transaction_id,
            snapshot.statuses,
        )
        .expect("manager");

        let committed = DurableTransaction::begin(
            &manager,
            Arc::clone(&status),
            Arc::clone(&wal),
            TransactionCharacteristics::default(),
        )
        .expect("committed transaction");
        let committed_id = committed.transaction_id();
        committed.commit_empty().expect("empty commit");
        assert_eq!(
            status
                .transaction_outcome(committed_id)
                .expect("committed status"),
            TransactionOutcome::Committed
        );
        assert_eq!(
            wal.transaction_outcomes()
                .expect("wal outcomes")
                .get(&committed_id),
            Some(&TransactionOutcome::Committed)
        );

        let aborted = DurableTransaction::begin(
            &manager,
            Arc::clone(&status),
            Arc::clone(&wal),
            TransactionCharacteristics::default(),
        )
        .expect("aborted transaction");
        let aborted_id = aborted.transaction_id();
        drop(aborted);
        assert_eq!(
            status
                .transaction_outcome(aborted_id)
                .expect("aborted status"),
            TransactionOutcome::Aborted
        );
        assert_eq!(
            wal.transaction_outcomes()
                .expect("wal outcomes")
                .get(&aborted_id),
            Some(&TransactionOutcome::Aborted)
        );
    }
}
