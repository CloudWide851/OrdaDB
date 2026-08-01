use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use ordadb_types::{DbError, Result};
use serde::{Deserialize, Serialize};

use crate::{TransactionId, TransactionSnapshot};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum PredicateLock {
    Table {
        table_id: u64,
    },
    Row {
        table_id: u64,
        version_id: u64,
    },
    IndexRange {
        index_id: u64,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
    },
}

impl PredicateLock {
    fn overlaps(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Table { table_id: left }, Self::Table { table_id: right }) => left == right,
            (
                Self::Table { table_id },
                Self::Row {
                    table_id: row_table,
                    ..
                },
            )
            | (
                Self::Row {
                    table_id: row_table,
                    ..
                },
                Self::Table { table_id },
            ) => table_id == row_table,
            (
                Self::Row {
                    table_id: left_table,
                    version_id: left_version,
                },
                Self::Row {
                    table_id: right_table,
                    version_id: right_version,
                },
            ) => left_table == right_table && left_version == right_version,
            (
                Self::IndexRange {
                    index_id: left,
                    lower: left_lower,
                    upper: left_upper,
                },
                Self::IndexRange {
                    index_id: right,
                    lower: right_lower,
                    upper: right_upper,
                },
            ) => {
                left == right
                    && ranges_overlap(
                        left_lower.as_deref(),
                        left_upper.as_deref(),
                        right_lower.as_deref(),
                        right_upper.as_deref(),
                    )
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SsiManagerOptions {
    pub maximum_transactions: usize,
    pub maximum_predicates_per_transaction: usize,
}

impl Default for SsiManagerOptions {
    fn default() -> Self {
        Self {
            maximum_transactions: 4096,
            maximum_predicates_per_transaction: 65_536,
        }
    }
}

#[derive(Debug)]
pub struct SsiManager {
    options: SsiManagerOptions,
    state: Mutex<SsiState>,
}

#[derive(Debug, Default)]
struct SsiState {
    transactions: BTreeMap<TransactionId, SsiTransaction>,
}

#[derive(Debug)]
struct SsiTransaction {
    snapshot_xmax: TransactionId,
    snapshot_in_progress: Arc<BTreeSet<TransactionId>>,
    read_only: bool,
    committed: bool,
    reads: BTreeSet<PredicateLock>,
    writes: BTreeSet<PredicateLock>,
    incoming: BTreeSet<TransactionId>,
    outgoing: BTreeSet<TransactionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsiSavepoint {
    reads: BTreeSet<PredicateLock>,
    writes: BTreeSet<PredicateLock>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsiTransactionSnapshot {
    pub transaction_id: TransactionId,
    pub committed: bool,
    pub read_only: bool,
    pub incoming: BTreeSet<TransactionId>,
    pub outgoing: BTreeSet<TransactionId>,
    pub read_predicates: usize,
    pub write_predicates: usize,
}

impl SsiManager {
    pub fn new(options: SsiManagerOptions) -> Result<Arc<Self>> {
        if options.maximum_transactions == 0 || options.maximum_predicates_per_transaction == 0 {
            return Err(DbError::new(
                "22023",
                "SSI transaction and predicate limits must be non-zero",
            ));
        }
        Ok(Arc::new(Self {
            options,
            state: Mutex::new(SsiState::default()),
        }))
    }

    pub fn begin(
        &self,
        transaction_id: TransactionId,
        snapshot: &TransactionSnapshot,
        read_only: bool,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        if state.transactions.len() >= self.options.maximum_transactions {
            return Err(DbError::new("54000", "SSI transaction limit exceeded"));
        }
        if state
            .transactions
            .insert(
                transaction_id,
                SsiTransaction {
                    snapshot_xmax: snapshot.xmax,
                    snapshot_in_progress: Arc::clone(&snapshot.in_progress),
                    read_only,
                    committed: false,
                    reads: BTreeSet::new(),
                    writes: BTreeSet::new(),
                    incoming: BTreeSet::new(),
                    outgoing: BTreeSet::new(),
                },
            )
            .is_some()
        {
            return Err(DbError::new(
                "25001",
                "SSI transaction is already registered",
            ));
        }
        Ok(())
    }

    pub fn record_read(
        &self,
        transaction_id: TransactionId,
        predicate: PredicateLock,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        self.ensure_predicate_capacity(&state, transaction_id, true)?;
        let reader = state
            .transactions
            .get(&transaction_id)
            .ok_or_else(|| DbError::new("25P01", "SSI transaction is not registered"))?;
        let conflicting_writers = state
            .transactions
            .iter()
            .filter(|(other_id, transaction)| {
                **other_id != transaction_id
                    && (!transaction.committed
                        || **other_id >= reader.snapshot_xmax
                        || reader.snapshot_in_progress.contains(other_id))
                    && transaction
                        .writes
                        .iter()
                        .any(|written| predicate.overlaps(written))
            })
            .map(|(other_id, _)| *other_id)
            .collect::<Vec<_>>();
        state
            .transactions
            .get_mut(&transaction_id)
            .ok_or_else(|| DbError::new("25P01", "SSI transaction is not registered"))?
            .reads
            .insert(predicate);
        for writer in conflicting_writers {
            add_dependency(&mut state, transaction_id, writer)?;
        }
        Ok(())
    }

    pub fn refresh_snapshot(
        &self,
        transaction_id: TransactionId,
        snapshot: &TransactionSnapshot,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let transaction = state
            .transactions
            .get_mut(&transaction_id)
            .ok_or_else(|| DbError::new("25P01", "SSI transaction is not registered"))?;
        if transaction.committed {
            return Err(DbError::new(
                "25000",
                "cannot refresh an SSI snapshot after commit validation",
            ));
        }
        if transaction.snapshot_xmax == snapshot.xmax
            && transaction.snapshot_in_progress.as_ref() == snapshot.in_progress.as_ref()
        {
            return Ok(());
        }
        if !transaction.reads.is_empty() || !transaction.writes.is_empty() {
            return Err(DbError::new(
                "25001",
                "cannot refresh an SSI snapshot after predicate tracking begins",
            ));
        }
        transaction.snapshot_xmax = snapshot.xmax;
        transaction.snapshot_in_progress = Arc::clone(&snapshot.in_progress);
        Ok(())
    }

    pub fn record_write(
        &self,
        transaction_id: TransactionId,
        predicate: PredicateLock,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        self.ensure_predicate_capacity(&state, transaction_id, false)?;
        let conflicting_readers = state
            .transactions
            .iter()
            .filter(|(other_id, transaction)| {
                **other_id != transaction_id
                    && transaction
                        .reads
                        .iter()
                        .any(|read| predicate.overlaps(read))
            })
            .map(|(other_id, _)| *other_id)
            .collect::<Vec<_>>();
        state
            .transactions
            .get_mut(&transaction_id)
            .ok_or_else(|| DbError::new("25P01", "SSI transaction is not registered"))?
            .writes
            .insert(predicate);
        for reader in conflicting_readers {
            add_dependency(&mut state, reader, transaction_id)?;
        }
        Ok(())
    }

    pub fn commit(&self, transaction_id: TransactionId) -> Result<()> {
        let mut state = self.lock_state()?;
        let transaction = state
            .transactions
            .get(&transaction_id)
            .ok_or_else(|| DbError::new("25P01", "SSI transaction is not registered"))?;
        let has_committed_incoming = transaction.incoming.iter().any(|incoming| {
            state
                .transactions
                .get(incoming)
                .is_some_and(|dependency| dependency.committed)
        });
        let has_outgoing = !transaction.outgoing.is_empty();
        if has_committed_incoming && has_outgoing {
            state.transactions.remove(&transaction_id);
            return Err(DbError::new(
                "40001",
                "could not serialize access due to read/write dependencies among transactions",
            )
            .with_hint("retry the transaction"));
        }
        if let Some(transaction) = state.transactions.get_mut(&transaction_id) {
            transaction.committed = true;
        }
        Ok(())
    }

    pub fn savepoint(&self, transaction_id: TransactionId) -> Result<SsiSavepoint> {
        let state = self.lock_state()?;
        let transaction = state
            .transactions
            .get(&transaction_id)
            .ok_or_else(|| DbError::new("25P01", "SSI transaction is not registered"))?;
        if transaction.committed {
            return Err(DbError::new(
                "25000",
                "cannot create an SSI savepoint after commit validation",
            ));
        }
        Ok(SsiSavepoint {
            reads: transaction.reads.clone(),
            writes: transaction.writes.clone(),
        })
    }

    pub fn rollback_to(
        &self,
        transaction_id: TransactionId,
        savepoint: &SsiSavepoint,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let transaction = state
            .transactions
            .get_mut(&transaction_id)
            .ok_or_else(|| DbError::new("25P01", "SSI transaction is not registered"))?;
        if transaction.committed {
            return Err(DbError::new(
                "25000",
                "cannot roll back SSI state after commit validation",
            ));
        }
        transaction.reads = savepoint.reads.clone();
        transaction.writes = savepoint.writes.clone();
        let reads = transaction.reads.clone();
        let writes = transaction.writes.clone();
        transaction.incoming.clear();
        transaction.outgoing.clear();
        for transaction in state.transactions.values_mut() {
            transaction.incoming.remove(&transaction_id);
            transaction.outgoing.remove(&transaction_id);
        }
        let mut dependencies = Vec::new();
        for (other_id, other) in &state.transactions {
            if *other_id == transaction_id {
                continue;
            }
            if predicates_overlap(&reads, &other.writes) {
                dependencies.push((transaction_id, *other_id));
            }
            if predicates_overlap(&other.reads, &writes) {
                dependencies.push((*other_id, transaction_id));
            }
        }
        for (reader, writer) in dependencies {
            add_dependency(&mut state, reader, writer)?;
        }
        Ok(())
    }

    pub fn abort(&self, transaction_id: TransactionId) -> Result<()> {
        let mut state = self.lock_state()?;
        remove_transaction(&mut state, transaction_id);
        Ok(())
    }

    pub fn cleanup_before(&self, horizon: TransactionId) -> Result<()> {
        let mut state = self.lock_state()?;
        let removable = state
            .transactions
            .iter()
            .filter(|(transaction_id, transaction)| {
                transaction.committed
                    && **transaction_id < horizon
                    && transaction.snapshot_xmax < horizon
            })
            .map(|(transaction_id, _)| *transaction_id)
            .collect::<Vec<_>>();
        for transaction_id in removable {
            remove_transaction(&mut state, transaction_id);
        }
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Vec<SsiTransactionSnapshot>> {
        Ok(self
            .lock_state()?
            .transactions
            .iter()
            .map(|(transaction_id, transaction)| SsiTransactionSnapshot {
                transaction_id: *transaction_id,
                committed: transaction.committed,
                read_only: transaction.read_only,
                incoming: transaction.incoming.clone(),
                outgoing: transaction.outgoing.clone(),
                read_predicates: transaction.reads.len(),
                write_predicates: transaction.writes.len(),
            })
            .collect())
    }

    fn ensure_predicate_capacity(
        &self,
        state: &SsiState,
        transaction_id: TransactionId,
        read: bool,
    ) -> Result<()> {
        let transaction = state
            .transactions
            .get(&transaction_id)
            .ok_or_else(|| DbError::new("25P01", "SSI transaction is not registered"))?;
        let length = if read {
            transaction.reads.len()
        } else {
            transaction.writes.len()
        };
        if length >= self.options.maximum_predicates_per_transaction {
            return Err(DbError::new("54000", "SSI predicate lock limit exceeded"));
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, SsiState>> {
        self.state.lock().map_err(|_| {
            DbError::internal("SSI manager state is poisoned")
                .with_hint("restart the process before retrying serializable work")
        })
    }
}

fn add_dependency(
    state: &mut SsiState,
    reader: TransactionId,
    writer: TransactionId,
) -> Result<()> {
    state
        .transactions
        .get_mut(&reader)
        .ok_or_else(|| DbError::new("25P01", "SSI reader is not registered"))?
        .outgoing
        .insert(writer);
    state
        .transactions
        .get_mut(&writer)
        .ok_or_else(|| DbError::new("25P01", "SSI writer is not registered"))?
        .incoming
        .insert(reader);
    Ok(())
}

fn remove_transaction(state: &mut SsiState, transaction_id: TransactionId) {
    state.transactions.remove(&transaction_id);
    for transaction in state.transactions.values_mut() {
        transaction.incoming.remove(&transaction_id);
        transaction.outgoing.remove(&transaction_id);
    }
}

fn predicates_overlap(reads: &BTreeSet<PredicateLock>, writes: &BTreeSet<PredicateLock>) -> bool {
    reads
        .iter()
        .any(|read| writes.iter().any(|write| read.overlaps(write)))
}

fn ranges_overlap(
    left_lower: Option<&[u8]>,
    left_upper: Option<&[u8]>,
    right_lower: Option<&[u8]>,
    right_upper: Option<&[u8]>,
) -> bool {
    let left_before_right = match (left_upper, right_lower) {
        (Some(left_upper), Some(right_lower)) => left_upper < right_lower,
        _ => false,
    };
    let right_before_left = match (right_upper, left_lower) {
        (Some(right_upper), Some(left_lower)) => right_upper < left_lower,
        _ => false,
    };
    !left_before_right && !right_before_left
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn snapshot(xmax: u64) -> TransactionSnapshot {
        let xmax = TransactionId::new(xmax).expect("xmax");
        TransactionSnapshot {
            xmin: TransactionId::new(3).expect("xmin"),
            xmax,
            in_progress: Arc::new(BTreeSet::new()),
            command_id: 0,
        }
    }

    #[test]
    fn row_read_then_write_registers_rw_dependency() {
        let manager = SsiManager::new(SsiManagerOptions::default()).expect("manager");
        let reader = TransactionId::new(10).expect("reader");
        let writer = TransactionId::new(11).expect("writer");
        manager
            .begin(reader, &snapshot(12), false)
            .expect("reader begin");
        manager
            .begin(writer, &snapshot(12), false)
            .expect("writer begin");
        let predicate = PredicateLock::Row {
            table_id: 1,
            version_id: 2,
        };
        manager
            .record_read(reader, predicate.clone())
            .expect("record read");
        manager
            .record_write(writer, predicate)
            .expect("record write");
        let state = manager.snapshot().expect("snapshot");
        let reader_state = state
            .iter()
            .find(|transaction| transaction.transaction_id == reader)
            .expect("reader state");
        assert!(reader_state.outgoing.contains(&writer));
    }

    #[test]
    fn committed_incoming_and_outgoing_dependency_aborts_pivot() {
        let manager = SsiManager::new(SsiManagerOptions::default()).expect("manager");
        let first = TransactionId::new(20).expect("first");
        let pivot = TransactionId::new(21).expect("pivot");
        let third = TransactionId::new(22).expect("third");
        for transaction_id in [first, pivot, third] {
            manager
                .begin(transaction_id, &snapshot(23), false)
                .expect("begin");
        }
        let left = PredicateLock::Row {
            table_id: 1,
            version_id: 1,
        };
        let right = PredicateLock::Row {
            table_id: 1,
            version_id: 2,
        };
        manager
            .record_read(first, left.clone())
            .expect("first read");
        manager.record_write(pivot, left).expect("pivot write");
        manager.commit(first).expect("first commit");
        manager
            .record_read(pivot, right.clone())
            .expect("pivot read");
        manager.record_write(third, right).expect("third write");
        let error = manager.commit(pivot).expect_err("dangerous pivot");
        assert_eq!(error.sql_state, "40001");
    }

    #[test]
    fn non_overlapping_index_ranges_do_not_conflict() {
        let manager = SsiManager::new(SsiManagerOptions::default()).expect("manager");
        let reader = TransactionId::new(30).expect("reader");
        let writer = TransactionId::new(31).expect("writer");
        manager
            .begin(reader, &snapshot(32), false)
            .expect("reader begin");
        manager
            .begin(writer, &snapshot(32), false)
            .expect("writer begin");
        manager
            .record_read(
                reader,
                PredicateLock::IndexRange {
                    index_id: 1,
                    lower: Some(vec![0]),
                    upper: Some(vec![10]),
                },
            )
            .expect("read range");
        manager
            .record_write(
                writer,
                PredicateLock::IndexRange {
                    index_id: 1,
                    lower: Some(vec![11]),
                    upper: Some(vec![20]),
                },
            )
            .expect("write range");
        assert!(
            manager
                .snapshot()
                .expect("snapshot")
                .iter()
                .all(|transaction| transaction.incoming.is_empty()
                    && transaction.outgoing.is_empty())
        );
    }

    #[test]
    fn refreshed_safe_snapshot_ignores_a_previously_committed_writer() {
        let manager = SsiManager::new(SsiManagerOptions::default()).expect("manager");
        let writer = TransactionId::new(33).expect("writer");
        let reader = TransactionId::new(34).expect("reader");
        manager
            .begin(writer, &snapshot(35), false)
            .expect("writer begin");
        manager
            .record_write(writer, PredicateLock::Table { table_id: 1 })
            .expect("writer predicate");
        let mut waiting_snapshot = snapshot(35);
        waiting_snapshot.in_progress = Arc::new(BTreeSet::from([writer]));
        manager
            .begin(reader, &waiting_snapshot, true)
            .expect("reader begin");
        manager.commit(writer).expect("writer commit");
        manager
            .refresh_snapshot(reader, &snapshot(36))
            .expect("safe snapshot");
        manager
            .record_read(reader, PredicateLock::Table { table_id: 1 })
            .expect("reader predicate");
        let state = manager.snapshot().expect("SSI snapshot");
        let reader_state = state
            .iter()
            .find(|transaction| transaction.transaction_id == reader)
            .expect("reader state");
        assert!(reader_state.outgoing.is_empty());
    }

    #[test]
    fn savepoint_rollback_restores_predicates_and_dependencies() {
        let manager = SsiManager::new(SsiManagerOptions::default()).expect("manager");
        let reader = TransactionId::new(40).expect("reader");
        let writer = TransactionId::new(41).expect("writer");
        manager
            .begin(reader, &snapshot(42), false)
            .expect("reader begin");
        manager
            .begin(writer, &snapshot(42), false)
            .expect("writer begin");
        manager
            .record_read(reader, PredicateLock::Table { table_id: 1 })
            .expect("pre-savepoint read");
        let savepoint = manager.savepoint(reader).expect("savepoint");
        manager
            .record_read(reader, PredicateLock::Table { table_id: 2 })
            .expect("post-savepoint read");
        manager
            .record_write(writer, PredicateLock::Table { table_id: 2 })
            .expect("conflicting write");

        let before = manager.snapshot().expect("before rollback");
        assert!(
            before
                .iter()
                .find(|transaction| transaction.transaction_id == reader)
                .expect("reader state")
                .outgoing
                .contains(&writer)
        );
        manager
            .rollback_to(reader, &savepoint)
            .expect("rollback SSI state");

        let after = manager.snapshot().expect("after rollback");
        let reader_state = after
            .iter()
            .find(|transaction| transaction.transaction_id == reader)
            .expect("reader state");
        let writer_state = after
            .iter()
            .find(|transaction| transaction.transaction_id == writer)
            .expect("writer state");
        assert_eq!(reader_state.read_predicates, 1);
        assert!(reader_state.outgoing.is_empty());
        assert!(writer_state.incoming.is_empty());
    }
}
