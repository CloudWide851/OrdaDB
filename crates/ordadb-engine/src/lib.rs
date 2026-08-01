//! SQL execution, transaction coordination, and durable publication for OrdaDB.
//!
//! This crate owns SQL semantics and candidate-state atomicity. Physical page
//! encoding belongs to `ordadb-storage`; WAL and crash recovery belong to
//! `ordadb-transaction`.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

use ordadb_catalog::{
    Catalog, CatalogExpression, CatalogObjectRef, ColumnStatistics, ConstraintKind, DropBehavior,
    IndexMethod, NewColumn, NewRoutine, NewView, ReferentialAction, SequenceAlteration,
    TableDefinition, TableStatistics, TriggerEvent, TriggerTiming, ViewKind, indexable_type,
};
use ordadb_execution::{
    AdvancedExecutionCursor, AdvancedExecutionPlan, ExecutionContext, ExecutionCursor,
    LeasedDataChunk, MemoryGrant, TableProvider, TableScan, coerce_value as coerce_execution_value,
    evaluate as evaluate_scalar, predicate_matches as execution_predicate_matches,
};
use ordadb_index::{BPlusTree, IndexEntry, IndexKey, RowId};
use ordadb_optimizer::{
    JoinStrategy, choose_join_strategy, explain as explain_plan, optimize_select,
};
use ordadb_plpgsql::{
    PlpgsqlHost, compile_with_arguments as compile_plpgsql, execute as execute_plpgsql,
};
pub use ordadb_search::{
    AllowedRows, HybridSearchRequest, SearchRowId, TextSearchRequest, VectorSearchRequest,
};
use ordadb_search::{SearchCatalog, SearchLimits};
use ordadb_sql::{
    BinaryOperator, BoundAlterTableOperation, BoundExpr, BoundExprKind, BoundJoin, BoundOrder,
    BoundProjection, BoundSequenceOperation, BoundStatement, BoundTable, DdlObjectKind, JoinKind,
    ParsedStatement, SqlDialect, TransactionChain, bind, bind_catalog_expression,
    bind_catalog_expression_with_parameter_types, parse, parse_with_dialect,
};
use ordadb_storage::{
    ApplyPoint, DataFormat, DatabaseStore, DurabilityBarrier, FROZEN_TRANSACTION_ID,
    PersistentState, StorageTableCursorV2, TupleHeaderV2, VersionedRow, encode_row,
};
use ordadb_transaction::{
    CheckpointState, DurableTransaction, FaultInjector, FaultPoint, IsolationLevel, LockGuard,
    LockKey, LockManager, LockManagerOptions, LockMode, LockSnapshot, LockWaitSnapshot,
    NoFaultInjector, PredicateLock, SavepointId, SavepointStack, SsiManager, SsiManagerOptions,
    SsiSavepoint, TransactionAccessMode, TransactionCharacteristics, TransactionId,
    TransactionManager, TransactionOutcome, TransactionSnapshot, TransactionStatusProvider,
    TransactionStatusStore, WalManager, WriterCoordinator, WriterLease, tuple_visible,
};
use ordadb_types::{
    Batch, CommandComplete, DbError, Field, Identifier, IndexId, QueryEvent, QueryProgress, Result,
    Row, ScalarType, Schema, SequenceId, TableId, Value, ViewId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const AUTOMATIC_CHECKPOINT_INTERVAL: u64 = 64;
const MAX_REFERENTIAL_ACTIONS: usize = 16_384;
const PLPGSQL_EXECUTION_STACK_BYTES: usize = 8 * 1024 * 1024;
pub const LOGICAL_SNAPSHOT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub cluster_root: PathBuf,
    pub minimum_next_transaction_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionOptions {
    pub dialect: SqlDialect,
}

#[derive(Debug, Clone)]
pub enum SearchRequest {
    Text(TextSearchRequest),
    Vector(VectorSearchRequest),
    Hybrid(HybridSearchRequest),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScalarSearchFilter {
    pub expression: String,
    pub parameters: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub row_id: SearchRowId,
    pub row: Row,
    pub text_score: Option<f32>,
    pub vector_score: Option<f32>,
    pub distance: Option<f32>,
    pub combined_score: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineStatusSnapshot {
    pub data_format: DataFormat,
    pub generation: u64,
    pub table_count: usize,
    pub row_count: u64,
    pub index_count: usize,
    pub durable_lsn: Option<u64>,
    pub dirty_page_count: usize,
    pub commits_since_checkpoint: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogicalDatabaseSnapshot {
    pub format_version: u16,
    pub source_generation: u64,
    pub catalog: Arc<Catalog>,
    pub tables: BTreeMap<TableId, Arc<Vec<Row>>>,
}

impl EngineConfig {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        Self {
            cluster_root: data_dir.clone(),
            data_dir,
            minimum_next_transaction_id: None,
        }
    }

    #[must_use]
    pub fn for_cluster(
        data_dir: impl Into<PathBuf>,
        cluster_root: impl Into<PathBuf>,
        minimum_next_transaction_id: u64,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            cluster_root: cluster_root.into(),
            minimum_next_transaction_id: Some(minimum_next_transaction_id),
        }
    }
}

#[derive(Debug, Default)]
struct StorageAccessGate {
    state: Mutex<StorageAccessState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct StorageAccessState {
    active_readers: usize,
    waiting_writers: usize,
    writer_active: bool,
}

impl StorageAccessGate {
    fn acquire_read(self: &Arc<Self>) -> Result<StorageReadLease> {
        let mut state = self.lock_state()?;
        while state.writer_active || state.waiting_writers > 0 {
            state = self
                .changed
                .wait(state)
                .map_err(|_| internal_error("storage access gate is poisoned"))?;
        }
        state.active_readers = state.active_readers.checked_add(1).ok_or_else(|| {
            DbError::new("54000", "storage reader lease count overflowed")
                .with_hint("restart the database service before retrying")
        })?;
        drop(state);
        Ok(StorageReadLease {
            gate: Arc::clone(self),
        })
    }

    fn acquire_write(self: &Arc<Self>) -> Result<StorageWriteLease> {
        let mut state = self.lock_state()?;
        state.waiting_writers = state.waiting_writers.checked_add(1).ok_or_else(|| {
            DbError::new("54000", "storage writer wait count overflowed")
                .with_hint("restart the database service before retrying")
        })?;
        while state.writer_active || state.active_readers > 0 {
            state = self
                .changed
                .wait(state)
                .map_err(|_| internal_error("storage access gate is poisoned"))?;
        }
        state.waiting_writers = state.waiting_writers.saturating_sub(1);
        state.writer_active = true;
        drop(state);
        Ok(StorageWriteLease {
            gate: Arc::clone(self),
        })
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, StorageAccessState>> {
        self.state
            .lock()
            .map_err(|_| internal_error("storage access gate is poisoned"))
    }

    #[cfg(test)]
    fn active_readers(&self) -> Result<usize> {
        Ok(self.lock_state()?.active_readers)
    }

    #[cfg(test)]
    fn waiting_writers(&self) -> Result<usize> {
        Ok(self.lock_state()?.waiting_writers)
    }
}

#[derive(Debug)]
struct StorageReadLease {
    gate: Arc<StorageAccessGate>,
}

impl Drop for StorageReadLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.active_readers = state.active_readers.saturating_sub(1);
            self.gate.changed.notify_all();
        }
    }
}

#[derive(Debug)]
struct StorageWriteLease {
    gate: Arc<StorageAccessGate>,
}

impl Drop for StorageWriteLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.writer_active = false;
            self.gate.changed.notify_all();
        }
    }
}

#[derive(Debug, Clone)]
pub struct Engine {
    config: Arc<EngineConfig>,
    state: Arc<RwLock<DatabaseState>>,
    store: Arc<Mutex<DatabaseStore>>,
    storage_access: Arc<StorageAccessGate>,
    wal: Arc<WalManager>,
    transaction_status: Arc<TransactionStatusStore>,
    transactions: Arc<TransactionManager>,
    locks: Arc<LockManager>,
    ssi: Arc<SsiManager>,
    writer: Arc<WriterCoordinator>,
    commits_since_checkpoint: Arc<AtomicU64>,
}

impl Engine {
    pub fn open(config: EngineConfig) -> Result<Self> {
        Self::open_with_fault_injector(config, Arc::new(NoFaultInjector))
    }

    #[doc(hidden)]
    pub fn open_with_fault_injector(
        config: EngineConfig,
        fault_injector: Arc<dyn FaultInjector>,
    ) -> Result<Self> {
        if DatabaseStore::detect_format_read_only(&config.data_dir)? == Some(DataFormat::V1) {
            return Err(DbError::new(
                "0A000",
                "legacy database format v1 cannot be opened for normal execution",
            )
            .with_hint("run the offline storage migration before starting the database service"));
        }
        let wal = WalManager::open_with_fault_injector(&config.data_dir, fault_injector)?;
        wal.recover(&config.data_dir)?;
        let wal_next_transaction_id = wal
            .last_transaction_id()?
            .map(|transaction_id| {
                transaction_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| DbError::new("54000", "transaction ID space is exhausted"))
            })
            .transpose()?
            .unwrap_or(1);
        let next_transaction_id = config
            .minimum_next_transaction_id
            .unwrap_or(1)
            .max(wal_next_transaction_id);
        let transaction_status = Arc::new(TransactionStatusStore::open(
            &config.data_dir,
            next_transaction_id,
        )?);
        transaction_status.reconcile_with_wal(&wal.transaction_outcomes()?)?;
        let transaction_status_snapshot = transaction_status.snapshot()?;
        let transactions = TransactionManager::from_status_snapshot(
            transaction_status_snapshot.next_transaction_id,
            transaction_status_snapshot.statuses,
        )?;
        let writer = WriterCoordinator::from_next_transaction_id(
            transaction_status_snapshot.next_transaction_id,
        )?;
        let locks = LockManager::new(LockManagerOptions::default())?;
        let ssi = SsiManager::new(SsiManagerOptions::default())?;
        let barrier: Arc<dyn DurabilityBarrier> = wal.clone();
        let store =
            DatabaseStore::open_with_barrier_and_format(&config.data_dir, barrier, DataFormat::V2)?;
        let state = DatabaseState::from_persistent(
            store.committed_state().clone(),
            store.data_format(),
            transaction_status.as_ref(),
            transaction_status_snapshot.next_transaction_id,
        )?;
        Ok(Self {
            config: Arc::new(config),
            state: Arc::new(RwLock::new(state)),
            store: Arc::new(Mutex::new(store)),
            storage_access: Arc::new(StorageAccessGate::default()),
            wal,
            transaction_status,
            transactions,
            locks,
            ssi,
            writer,
            commits_since_checkpoint: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn connect(&self) -> Result<Session> {
        self.connect_with_options(SessionOptions::default())
    }

    pub fn connect_with_options(&self, options: SessionOptions) -> Result<Session> {
        Ok(Session {
            state: Arc::clone(&self.state),
            store: Arc::clone(&self.store),
            storage_access: Arc::clone(&self.storage_access),
            wal: Arc::clone(&self.wal),
            transaction_status: Arc::clone(&self.transaction_status),
            transactions: Arc::clone(&self.transactions),
            locks: Arc::clone(&self.locks),
            ssi: Arc::clone(&self.ssi),
            writer: Arc::clone(&self.writer),
            commits_since_checkpoint: Arc::clone(&self.commits_since_checkpoint),
            sql_transaction: SqlTransactionState::Idle,
            sequence_currvals: BTreeMap::new(),
            options,
        })
    }

    pub fn checkpoint(&self) -> Result<()> {
        checkpoint_shared(&self.state, &self.store, &self.wal, &self.transactions)?;
        self.commits_since_checkpoint.store(0, Ordering::Release);
        Ok(())
    }

    pub fn lock_snapshot(&self) -> Result<(Vec<LockSnapshot>, Vec<LockWaitSnapshot>)> {
        self.locks.snapshot()
    }

    pub fn set_maximum_snapshot_age(&self, maximum_age: Duration) -> Result<()> {
        self.transactions.set_maximum_snapshot_age(maximum_age)
    }

    pub fn set_default_lock_timeout(&self, timeout: Duration) -> Result<()> {
        self.locks.set_default_timeout(timeout)
    }

    #[must_use]
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn catalog_snapshot(&self) -> Result<Arc<Catalog>> {
        self.state
            .read()
            .map(|state| Arc::clone(&state.catalog))
            .map_err(|_| internal_error("engine state lock is poisoned"))
    }

    pub fn logical_snapshot(&self) -> Result<LogicalDatabaseSnapshot> {
        let state = self
            .state
            .read()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let tables = state
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .map(|table| {
                (
                    table.id,
                    state
                        .rows
                        .get(&table.id)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(Vec::new())),
                )
            })
            .collect();
        Ok(LogicalDatabaseSnapshot {
            format_version: LOGICAL_SNAPSHOT_VERSION,
            source_generation: state.generation,
            catalog: Arc::clone(&state.catalog),
            tables,
        })
    }

    pub fn replace_logical_snapshot(&self, snapshot: LogicalDatabaseSnapshot) -> Result<()> {
        let candidate = DatabaseState::from_logical_snapshot(snapshot)?;
        let mut transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            TransactionCharacteristics::default(),
        )?;
        let mut lease = self.writer.try_acquire(transaction.transaction_id())?;
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        persist_candidate(
            &mut state,
            &self.store,
            &self.storage_access,
            &self.wal,
            &mut transaction,
            candidate,
        )?;
        drop(state);
        lease.release();
        record_commit_and_maybe_checkpoint(
            &self.state,
            &self.store,
            &self.wal,
            &self.transactions,
            &self.commits_since_checkpoint,
        )
    }

    pub fn restore_logical_snapshot(
        config: EngineConfig,
        snapshot: LogicalDatabaseSnapshot,
    ) -> Result<Self> {
        if config.data_dir.exists() {
            let mut entries = std::fs::read_dir(&config.data_dir).map_err(|error| {
                DbError::new("58030", "failed to inspect logical restore target")
                    .with_detail(error.to_string())
            })?;
            if entries
                .next()
                .transpose()
                .map_err(|error| {
                    DbError::new("58030", "failed to inspect logical restore target")
                        .with_detail(error.to_string())
                })?
                .is_some()
            {
                return Err(DbError::new(
                    "55000",
                    "logical restore target must be absent or empty",
                )
                .with_hint("restore into a new candidate directory before replacing active data"));
            }
        }
        let engine = Self::open(config)?;
        engine.replace_logical_snapshot(snapshot)?;
        engine.checkpoint()?;
        Ok(engine)
    }

    pub fn status_snapshot(&self) -> Result<EngineStatusSnapshot> {
        let state = self
            .state
            .read()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let table_count = state
            .catalog
            .database()
            .schemas()
            .map(|schema| schema.tables().count())
            .sum();
        let row_count = state.rows.values().try_fold(0_u64, |total, rows| {
            let rows = u64::try_from(rows.len())
                .map_err(|_| internal_error("table row count does not fit in u64"))?;
            total
                .checked_add(rows)
                .ok_or_else(|| internal_error("database row count overflowed"))
        })?;
        let durable_lsn = self.wal.durable_lsn()?.map(|lsn| lsn.get());
        let dirty_page_count = self.wal.dirty_pages()?.len();
        let index_count = state
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .flat_map(|table| table.indexes())
            .count();
        Ok(EngineStatusSnapshot {
            data_format: DataFormat::V2,
            generation: state.generation,
            table_count,
            row_count,
            index_count,
            durable_lsn,
            dirty_page_count,
            commits_since_checkpoint: self.commits_since_checkpoint.load(Ordering::Acquire),
        })
    }
}

#[derive(Debug)]
pub struct Session {
    state: Arc<RwLock<DatabaseState>>,
    store: Arc<Mutex<DatabaseStore>>,
    storage_access: Arc<StorageAccessGate>,
    wal: Arc<WalManager>,
    transaction_status: Arc<TransactionStatusStore>,
    transactions: Arc<TransactionManager>,
    locks: Arc<LockManager>,
    ssi: Arc<SsiManager>,
    writer: Arc<WriterCoordinator>,
    commits_since_checkpoint: Arc<AtomicU64>,
    sql_transaction: SqlTransactionState,
    sequence_currvals: BTreeMap<SequenceId, i64>,
    options: SessionOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Idle,
    Active,
    Failed,
}

#[derive(Debug)]
enum SqlTransactionState {
    Idle,
    Active(Box<ActiveSqlTransaction>),
    Failed(TransactionCharacteristics),
}

#[derive(Debug)]
struct ActiveSqlTransaction {
    transaction: DurableTransaction,
    base: Option<DatabaseState>,
    working: Option<DatabaseState>,
    locks: Vec<LockGuard>,
    lease: Option<WriterLease>,
    dml_only: bool,
    ssi: Option<SsiTransactionGuard>,
    savepoints: SavepointStack,
    savepoint_states: BTreeMap<SavepointId, SqlSavepointState>,
    failed: bool,
    stream_failed: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
struct SqlSavepointState {
    base: Option<DatabaseState>,
    working: Option<DatabaseState>,
    sequence_currvals: BTreeMap<SequenceId, i64>,
    lock_len: usize,
    dml_only: bool,
    ssi: Option<SsiSavepoint>,
}

#[derive(Debug)]
struct SsiTransactionGuard {
    manager: Arc<SsiManager>,
    transaction_id: TransactionId,
    validated: bool,
    finished: bool,
}

impl SsiTransactionGuard {
    fn begin(manager: Arc<SsiManager>, transaction: &DurableTransaction) -> Result<Option<Self>> {
        let Some(characteristics) = transaction.characteristics() else {
            return Err(DbError::new("25P01", "transaction is no longer active"));
        };
        if characteristics.isolation_level != IsolationLevel::Serializable {
            return Ok(None);
        }
        let snapshot = transaction
            .snapshot()
            .ok_or_else(|| DbError::new("25P01", "transaction is no longer active"))?;
        manager.begin(
            transaction.transaction_id(),
            snapshot,
            characteristics.access_mode == TransactionAccessMode::ReadOnly,
        )?;
        Ok(Some(Self {
            manager,
            transaction_id: transaction.transaction_id(),
            validated: false,
            finished: false,
        }))
    }

    fn record_read(&self, predicate: PredicateLock) -> Result<()> {
        self.manager.record_read(self.transaction_id, predicate)
    }

    fn record_write(&self, predicate: PredicateLock) -> Result<()> {
        self.manager.record_write(self.transaction_id, predicate)
    }

    fn refresh_snapshot(&self, snapshot: &TransactionSnapshot) -> Result<()> {
        self.manager.refresh_snapshot(self.transaction_id, snapshot)
    }

    fn savepoint(&self) -> Result<SsiSavepoint> {
        self.manager.savepoint(self.transaction_id)
    }

    fn rollback_to(&self, savepoint: &SsiSavepoint) -> Result<()> {
        self.manager.rollback_to(self.transaction_id, savepoint)
    }

    fn validate_commit(&mut self, cleanup_horizon: TransactionId) -> Result<()> {
        if !self.validated {
            self.manager.commit(self.transaction_id)?;
            self.manager.cleanup_before(cleanup_horizon)?;
            self.validated = true;
        }
        Ok(())
    }

    fn finish(&mut self) {
        self.finished = true;
    }
}

impl Drop for SsiTransactionGuard {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.manager.abort(self.transaction_id);
        }
    }
}

impl Session {
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        match self
            .execute_stream(sql, params)?
            .collect::<Result<Vec<_>>>()
        {
            Ok(events) => Ok(QueryStream::new(events)),
            Err(error) => {
                self.fail_sql_transaction();
                Err(error)
            }
        }
    }

    /// Bind a statement against the session's current catalog snapshot without
    /// executing it and return the row schema exposed to protocol clients.
    pub fn describe(&mut self, sql: &str) -> Result<Schema> {
        self.normalize_sql_transaction_failure();
        if self.transaction_status() == TransactionStatus::Failed {
            return Err(failed_transaction_error());
        }
        let snapshot = self.statement_snapshot()?;
        let described = parse_with_dialect(sql, self.options.dialect)
            .and_then(|statement| bind(statement, &snapshot.catalog))
            .map(|statement| bound_statement_schema(&statement));
        if described.is_err() {
            self.fail_sql_transaction();
        }
        described
    }

    pub fn execute_stream(&mut self, sql: &str, params: &[Value]) -> Result<TryQueryStream> {
        self.execute_stream_controlled(sql, params, None)
    }

    pub fn execute_stream_with_cancellation(
        &mut self,
        sql: &str,
        params: &[Value],
        cancellation: Arc<AtomicBool>,
    ) -> Result<TryQueryStream> {
        self.execute_stream_controlled(sql, params, Some(cancellation))
    }

    fn execute_stream_controlled(
        &mut self,
        sql: &str,
        params: &[Value],
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Result<TryQueryStream> {
        self.normalize_sql_transaction_failure();
        let transaction_was_failed = self.transaction_status() == TransactionStatus::Failed;
        let parsed = match parse_with_dialect(sql, self.options.dialect) {
            Ok(parsed) => parsed,
            Err(_) if transaction_was_failed => {
                return Err(failed_transaction_error());
            }
            Err(error) => {
                self.fail_sql_transaction();
                return Err(error);
            }
        };
        if transaction_was_failed {
            return match parsed {
                ParsedStatement::Rollback { chain } => self.rollback_sql_transaction(chain),
                ParsedStatement::RollbackTo { name } => self.rollback_to_sql_savepoint(&name.name),
                _ => Err(failed_transaction_error()),
            };
        }
        if !parsed_is_transaction_control(&parsed)
            && let Err(error) = self.begin_active_statement(cancellation.as_deref())
        {
            self.fail_sql_transaction();
            return Err(error);
        }
        let mut snapshot = self.statement_snapshot()?;
        snapshot.cancellation = cancellation;
        let statement = match bind(parsed, &snapshot.catalog)
            .and_then(|statement| resolve_sequence_currval(statement, &self.sequence_currvals))
        {
            Ok(statement) => statement,
            Err(error) => {
                self.fail_sql_transaction();
                return Err(error);
            }
        };

        match &statement {
            BoundStatement::Begin { characteristics } => {
                return self.begin_sql_transaction(*characteristics);
            }
            BoundStatement::Commit { chain } => return self.commit_sql_transaction(*chain),
            BoundStatement::Rollback { chain } => return self.rollback_sql_transaction(*chain),
            BoundStatement::Savepoint { name } => return self.create_sql_savepoint(name),
            BoundStatement::RollbackTo { name } => return self.rollback_to_sql_savepoint(name),
            BoundStatement::ReleaseSavepoint { name } => {
                return self.release_sql_savepoint(name);
            }
            _ => {}
        }
        match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
            SqlTransactionState::Idle => self.execute_auto_commit(sql, params, snapshot, statement),
            SqlTransactionState::Active(mut transaction) => {
                match self.execute_in_sql_transaction(
                    &mut transaction,
                    sql,
                    params,
                    snapshot,
                    statement,
                ) {
                    Ok(stream) => {
                        if let Err(error) = transaction.transaction.finish_statement() {
                            transaction.failed = true;
                            self.sql_transaction = SqlTransactionState::Active(transaction);
                            return Err(error);
                        }
                        let stream =
                            stream.with_failure_flag(Arc::clone(&transaction.stream_failed));
                        self.sql_transaction = SqlTransactionState::Active(transaction);
                        Ok(stream)
                    }
                    Err(error) => {
                        transaction.failed = true;
                        self.sql_transaction = SqlTransactionState::Active(transaction);
                        Err(error)
                    }
                }
            }
            SqlTransactionState::Failed(characteristics) => {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                Err(failed_transaction_error())
            }
        }
    }

    pub fn begin(&mut self) -> Result<Transaction<'_>> {
        if self.transaction_status() != TransactionStatus::Idle {
            return Err(DbError::new(
                "25001",
                "a SQL transaction is already active in this session",
            )
            .with_hint("commit or roll back the SQL transaction before using Session::begin"));
        }
        let transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            TransactionCharacteristics::default(),
        )?;
        Ok(Transaction {
            state: &self.state,
            store: &self.store,
            storage_access: &self.storage_access,
            wal: &self.wal,
            transaction_status: &self.transaction_status,
            transactions: &self.transactions,
            locks: &self.locks,
            writer: &self.writer,
            commits_since_checkpoint: &self.commits_since_checkpoint,
            sequence_currvals: &mut self.sequence_currvals,
            dialect: self.options.dialect,
            transaction,
            base: None,
            working: None,
            lock_guards: Vec::new(),
            lease: None,
            dml_only: true,
            failed: false,
        })
    }

    #[must_use]
    pub fn transaction_status(&self) -> TransactionStatus {
        match &self.sql_transaction {
            SqlTransactionState::Idle => TransactionStatus::Idle,
            SqlTransactionState::Active(transaction)
                if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) =>
            {
                TransactionStatus::Failed
            }
            SqlTransactionState::Active(_) => TransactionStatus::Active,
            SqlTransactionState::Failed(_) => TransactionStatus::Failed,
        }
    }

    pub fn search(&self, request: SearchRequest) -> Result<Vec<SearchResult>> {
        self.search_with_filter(request, None)
    }

    pub fn search_with_filter(
        &self,
        request: SearchRequest,
        filter: Option<&ScalarSearchFilter>,
    ) -> Result<Vec<SearchResult>> {
        let snapshot = self.statement_snapshot()?;
        match request {
            SearchRequest::Text(mut request) => {
                let table_id =
                    search_index_table(&snapshot, request.index_id, IndexMethod::FullText)?;
                if let Some(filter) = filter {
                    let allowed = evaluate_search_filter(&snapshot, table_id, filter)?;
                    request.allowed_rows = intersect_allowed_rows(request.allowed_rows, allowed);
                }
                snapshot
                    .searches
                    .text_search(&request)?
                    .into_iter()
                    .map(|hit| {
                        Ok(SearchResult {
                            row_id: hit.row_id,
                            row: search_result_row(&snapshot, table_id, hit.row_id)?,
                            text_score: Some(hit.score),
                            vector_score: None,
                            distance: None,
                            combined_score: None,
                        })
                    })
                    .collect()
            }
            SearchRequest::Vector(mut request) => {
                let table_id = search_index_table(&snapshot, request.index_id, IndexMethod::Hnsw)?;
                if let Some(filter) = filter {
                    let allowed = evaluate_search_filter(&snapshot, table_id, filter)?;
                    request.allowed_rows = intersect_allowed_rows(request.allowed_rows, allowed);
                }
                snapshot
                    .searches
                    .vector_search(&request)?
                    .into_iter()
                    .map(|hit| {
                        Ok(SearchResult {
                            row_id: hit.row_id,
                            row: search_result_row(&snapshot, table_id, hit.row_id)?,
                            text_score: None,
                            vector_score: Some(hit.score),
                            distance: Some(hit.distance),
                            combined_score: None,
                        })
                    })
                    .collect()
            }
            SearchRequest::Hybrid(mut request) => {
                let text_table =
                    search_index_table(&snapshot, request.text.index_id, IndexMethod::FullText)?;
                let vector_table =
                    search_index_table(&snapshot, request.vector.index_id, IndexMethod::Hnsw)?;
                if text_table != vector_table {
                    return Err(DbError::new(
                        "22023",
                        "hybrid search indexes must belong to the same table",
                    ));
                }
                if let Some(filter) = filter {
                    let allowed = evaluate_search_filter(&snapshot, text_table, filter)?;
                    request.text.allowed_rows =
                        intersect_allowed_rows(request.text.allowed_rows, Arc::clone(&allowed));
                    request.vector.allowed_rows =
                        intersect_allowed_rows(request.vector.allowed_rows, allowed);
                }
                snapshot
                    .searches
                    .hybrid_search(&request)?
                    .into_iter()
                    .map(|hit| {
                        Ok(SearchResult {
                            row_id: hit.row_id,
                            row: search_result_row(&snapshot, text_table, hit.row_id)?,
                            text_score: Some(hit.text_score),
                            vector_score: Some(hit.vector_score),
                            distance: None,
                            combined_score: Some(hit.combined_score),
                        })
                    })
                    .collect()
            }
        }
    }

    #[must_use]
    pub const fn options(&self) -> SessionOptions {
        self.options
    }

    fn statement_snapshot(&self) -> Result<DatabaseState> {
        if let SqlTransactionState::Active(transaction) = &self.sql_transaction {
            if let Some(working) = &transaction.working {
                return Ok(working.clone());
            }
            let committed = committed_snapshot(&self.state)?;
            let snapshot = transaction
                .transaction
                .snapshot()
                .ok_or_else(|| no_active_transaction_error("take a statement snapshot"))?;
            return project_database_visibility(
                committed,
                snapshot,
                transaction.transaction.transaction_id(),
                self.transaction_status.as_ref(),
            );
        }
        committed_snapshot(&self.state)
    }

    fn begin_active_statement(&mut self, cancellation: Option<&AtomicBool>) -> Result<()> {
        let state = Arc::clone(&self.state);
        let transaction_status = Arc::clone(&self.transaction_status);
        if let SqlTransactionState::Active(transaction) = &mut self.sql_transaction {
            let snapshot = match cancellation {
                Some(cancellation) => transaction
                    .transaction
                    .begin_statement_with_cancellation(cancellation)?,
                None => transaction.transaction.begin_statement()?,
            }
            .clone();
            if let Some(ssi) = &transaction.ssi {
                ssi.refresh_snapshot(&snapshot)?;
            }
            refresh_read_committed_candidate(
                &state,
                transaction_status.as_ref(),
                &transaction.transaction,
                &mut transaction.base,
                &mut transaction.working,
                transaction.dml_only,
            )?;
        }
        Ok(())
    }

    fn begin_sql_transaction(
        &mut self,
        characteristics: TransactionCharacteristics,
    ) -> Result<TryQueryStream> {
        match self.transaction_status() {
            TransactionStatus::Idle => {
                self.sql_transaction =
                    SqlTransactionState::Active(self.new_active_sql_transaction(characteristics)?);
                Ok(TryQueryStream::new(transaction_events("BEGIN")))
            }
            TransactionStatus::Active => {
                Err(DbError::new("25001", "a transaction is already active")
                    .with_hint("commit or roll back the current transaction first"))
            }
            TransactionStatus::Failed => Err(failed_transaction_error()),
        }
    }

    fn new_active_sql_transaction(
        &self,
        characteristics: TransactionCharacteristics,
    ) -> Result<Box<ActiveSqlTransaction>> {
        let transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            characteristics,
        )?;
        let ssi = SsiTransactionGuard::begin(Arc::clone(&self.ssi), &transaction)?;
        Ok(Box::new(ActiveSqlTransaction {
            transaction,
            base: None,
            working: None,
            locks: Vec::new(),
            lease: None,
            dml_only: true,
            ssi,
            savepoints: SavepointStack::new(),
            savepoint_states: BTreeMap::new(),
            failed: false,
            stream_failed: Arc::new(AtomicBool::new(false)),
        }))
    }

    fn commit_sql_transaction(&mut self, chain: TransactionChain) -> Result<TryQueryStream> {
        let transaction = match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
            SqlTransactionState::Idle => return Err(no_active_transaction_error("commit")),
            SqlTransactionState::Failed(characteristics) => {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                return Err(failed_transaction_error());
            }
            SqlTransactionState::Active(transaction)
                if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) =>
            {
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(failed_transaction_error());
            }
            SqlTransactionState::Active(transaction) => transaction,
        };
        let characteristics = transaction
            .transaction
            .characteristics()
            .ok_or_else(|| no_active_transaction_error("commit"))?;
        let ActiveSqlTransaction {
            transaction: mut durable,
            base,
            working,
            mut locks,
            lease,
            dml_only,
            mut ssi,
            ..
        } = *transaction;
        if let Some(ssi) = &mut ssi
            && let Err(error) = self
                .transactions
                .global_xmin_excluding(durable.transaction_id())
                .and_then(|horizon| ssi.validate_commit(horizon))
        {
            self.sql_transaction = SqlTransactionState::Failed(characteristics);
            return Err(error);
        }
        if let Some(candidate) = working {
            let mut state = match self.state.write() {
                Ok(state) => state,
                Err(_) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(internal_error("engine state lock is poisoned"));
                }
            };
            let candidate = if dml_only {
                let base = base.as_ref().ok_or_else(|| {
                    internal_error("DML transaction is missing its base snapshot")
                })?;
                match merge_dml_candidate(
                    &state,
                    base,
                    &candidate,
                    &durable,
                    self.transaction_status.as_ref(),
                ) {
                    Ok(candidate) => candidate,
                    Err(error) => {
                        self.sql_transaction = SqlTransactionState::Failed(characteristics);
                        return Err(error);
                    }
                }
            } else {
                candidate
            };
            if let Err(error) = persist_candidate(
                &mut state,
                &self.store,
                &self.storage_access,
                &self.wal,
                &mut durable,
                candidate,
            ) {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                return Err(error);
            }
            if let Some(ssi) = &mut ssi {
                ssi.finish();
            }
            drop(state);
            locks.clear();
            drop(lease);
            record_commit_and_maybe_checkpoint(
                &self.state,
                &self.store,
                &self.wal,
                &self.transactions,
                &self.commits_since_checkpoint,
            )?;
        } else {
            if let Err(error) = durable.commit_empty() {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                return Err(error);
            }
            if let Some(ssi) = &mut ssi {
                ssi.finish();
            }
        }
        self.start_chained_sql_transaction(chain, characteristics)?;
        Ok(TryQueryStream::new(transaction_events("COMMIT")))
    }

    fn rollback_sql_transaction(&mut self, chain: TransactionChain) -> Result<TryQueryStream> {
        let characteristics =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => return Err(no_active_transaction_error("roll back")),
                SqlTransactionState::Active(transaction) => {
                    let characteristics = transaction
                        .transaction
                        .characteristics()
                        .ok_or_else(|| no_active_transaction_error("roll back"))?;
                    transaction.transaction.abort()?;
                    characteristics
                }
                SqlTransactionState::Failed(characteristics) => characteristics,
            };
        self.start_chained_sql_transaction(chain, characteristics)?;
        Ok(TryQueryStream::new(transaction_events("ROLLBACK")))
    }

    fn start_chained_sql_transaction(
        &mut self,
        chain: TransactionChain,
        characteristics: TransactionCharacteristics,
    ) -> Result<()> {
        if chain == TransactionChain::Chain {
            self.sql_transaction =
                SqlTransactionState::Active(self.new_active_sql_transaction(characteristics)?);
        }
        Ok(())
    }

    fn create_sql_savepoint(&mut self, name: &Identifier) -> Result<TryQueryStream> {
        let mut transaction =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => {
                    return Err(no_active_transaction_error("create a savepoint"));
                }
                SqlTransactionState::Failed(characteristics) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(failed_transaction_error());
                }
                SqlTransactionState::Active(transaction) => transaction,
            };
        if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) {
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(failed_transaction_error());
        }
        let command_id = transaction
            .transaction
            .snapshot()
            .ok_or_else(|| no_active_transaction_error("create a savepoint"))?
            .command_id;
        let ssi = match transaction
            .ssi
            .as_ref()
            .map(SsiTransactionGuard::savepoint)
            .transpose()
        {
            Ok(ssi) => ssi,
            Err(error) => {
                transaction.failed = true;
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(error);
            }
        };
        let id =
            match transaction
                .savepoints
                .push(name.as_str(), command_id, 0, transaction.locks.len())
            {
                Ok(id) => id,
                Err(error) => {
                    self.sql_transaction = SqlTransactionState::Active(transaction);
                    return Err(error);
                }
            };
        transaction.savepoint_states.insert(
            id,
            SqlSavepointState {
                base: transaction.base.clone(),
                working: transaction.working.clone(),
                sequence_currvals: self.sequence_currvals.clone(),
                lock_len: transaction.locks.len(),
                dml_only: transaction.dml_only,
                ssi,
            },
        );
        if let Err(error) = transaction.transaction.finish_statement() {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        self.sql_transaction = SqlTransactionState::Active(transaction);
        Ok(TryQueryStream::new(transaction_events("SAVEPOINT")))
    }

    fn rollback_to_sql_savepoint(&mut self, name: &Identifier) -> Result<TryQueryStream> {
        let mut transaction =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => {
                    return Err(no_active_transaction_error("roll back to a savepoint"));
                }
                SqlTransactionState::Failed(characteristics) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(DbError::new(
                        "3B001",
                        format!("savepoint \"{name}\" does not exist"),
                    ));
                }
                SqlTransactionState::Active(transaction) => transaction,
            };
        let savepoint = match transaction.savepoints.rollback_to(name.as_str()) {
            Ok(savepoint) => savepoint,
            Err(error) => {
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(error);
            }
        };
        let saved = match transaction.savepoint_states.get(&savepoint.id).cloned() {
            Some(saved) => saved,
            None => {
                transaction.failed = true;
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(internal_error("savepoint state is missing"));
            }
        };
        let ssi_rollback = match (&transaction.ssi, &saved.ssi) {
            (Some(ssi), Some(savepoint)) => ssi.rollback_to(savepoint),
            (None, None) => Ok(()),
            _ => Err(internal_error("savepoint SSI state is inconsistent")),
        };
        if let Err(error) = ssi_rollback {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        transaction.base = saved.base;
        transaction.working = saved.working;
        transaction.locks.truncate(saved.lock_len);
        transaction.dml_only = saved.dml_only;
        if transaction.working.is_none() {
            transaction.lease = None;
        }
        self.sequence_currvals = saved.sequence_currvals;
        let retained = transaction
            .savepoints
            .frames()
            .iter()
            .map(|frame| frame.id)
            .collect::<BTreeSet<_>>();
        transaction
            .savepoint_states
            .retain(|id, _| retained.contains(id));
        transaction.failed = false;
        transaction.stream_failed.store(false, Ordering::Release);
        if let Err(error) = transaction.transaction.finish_statement() {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        self.sql_transaction = SqlTransactionState::Active(transaction);
        Ok(TryQueryStream::new(transaction_events("ROLLBACK")))
    }

    fn release_sql_savepoint(&mut self, name: &Identifier) -> Result<TryQueryStream> {
        let mut transaction =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => {
                    return Err(no_active_transaction_error("release a savepoint"));
                }
                SqlTransactionState::Failed(characteristics) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(failed_transaction_error());
                }
                SqlTransactionState::Active(transaction) => transaction,
            };
        if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) {
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(failed_transaction_error());
        }
        if let Err(error) = transaction.savepoints.release(name.as_str()) {
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        let retained = transaction
            .savepoints
            .frames()
            .iter()
            .map(|frame| frame.id)
            .collect::<BTreeSet<_>>();
        transaction
            .savepoint_states
            .retain(|id, _| retained.contains(id));
        if let Err(error) = transaction.transaction.finish_statement() {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        self.sql_transaction = SqlTransactionState::Active(transaction);
        Ok(TryQueryStream::new(transaction_events("RELEASE")))
    }

    fn execute_auto_commit(
        &mut self,
        sql: &str,
        params: &[Value],
        snapshot: DatabaseState,
        statement: BoundStatement,
    ) -> Result<TryQueryStream> {
        let table_provider = StorageTableProviderV2::new(
            Arc::clone(&self.store),
            Arc::clone(&self.storage_access),
            snapshot.generation,
            &snapshot.rows,
        );
        if let Some(stream) =
            prepare_read_stream(&snapshot, statement.clone(), params, Some(&table_provider))?
        {
            return Ok(stream);
        }
        let compacts_transaction_status =
            matches!(&statement, BoundStatement::Vacuum { table_id: None, .. });
        let sequence_id = sequence_mutation_id(&statement);
        let write_scope = statement_write_scope(&statement);
        let maintenance =
            maintenance_context(self.transactions.as_ref(), self.transaction_status.as_ref())?;
        let (_, events, dirty) =
            execute_bound_candidate(&snapshot, statement, params, None, maintenance)?;
        if !dirty {
            return Ok(TryQueryStream::new(events));
        }

        let mut transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            TransactionCharacteristics::default(),
        )?;
        let mut lease = None;
        let mut write_locks = Vec::new();
        match write_scope {
            StatementWriteScope::Dml => {
                let (lock_candidate, _, lock_dirty) = execute_candidate(
                    &snapshot,
                    sql,
                    params,
                    self.options.dialect,
                    Some(version_mutation_context(&transaction)?),
                    maintenance,
                )?;
                if lock_dirty {
                    write_locks = acquire_dml_locks(
                        &self.locks,
                        &transaction,
                        &snapshot,
                        &lock_candidate,
                        &[],
                        snapshot.cancellation.as_deref(),
                    )?;
                }
            }
            StatementWriteScope::Exclusive => {
                lease = Some(self.writer.try_acquire(transaction.transaction_id())?);
                write_locks.push(acquire_compatibility_write_lock(
                    &self.locks,
                    &transaction,
                    snapshot.cancellation.as_deref(),
                )?);
            }
            StatementWriteScope::ReadOnly => {
                return Err(internal_error(
                    "read-only statement unexpectedly produced a dirty candidate",
                ));
            }
        }
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let mut committed = state.clone();
        committed.cancellation = snapshot.cancellation.clone();
        let (candidate, events, dirty) = execute_candidate(
            &committed,
            sql,
            params,
            self.options.dialect,
            Some(version_mutation_context(&transaction)?),
            maintenance,
        )?;
        if dirty {
            let sequence_value = sequence_id
                .map(|sequence_id| candidate_sequence_value(&candidate, sequence_id))
                .transpose()?;
            persist_candidate(
                &mut state,
                &self.store,
                &self.storage_access,
                &self.wal,
                &mut transaction,
                candidate,
            )?;
            drop(state);
            drop(write_locks);
            drop(lease.take());
            record_commit_and_maybe_checkpoint(
                &self.state,
                &self.store,
                &self.wal,
                &self.transactions,
                &self.commits_since_checkpoint,
            )?;
            if compacts_transaction_status {
                self.transaction_status
                    .compact_before(maintenance.horizon)?;
                self.transactions.compact_before(maintenance.horizon)?;
            }
            if let Some((sequence_id, value)) = sequence_id.zip(sequence_value) {
                self.sequence_currvals.insert(sequence_id, value);
            }
        }
        Ok(TryQueryStream::new(events))
    }

    fn execute_in_sql_transaction(
        &mut self,
        transaction: &mut ActiveSqlTransaction,
        sql: &str,
        params: &[Value],
        snapshot: DatabaseState,
        statement: BoundStatement,
    ) -> Result<TryQueryStream> {
        if matches!(&statement, BoundStatement::Vacuum { .. }) {
            return Err(DbError::new(
                "25001",
                "VACUUM cannot run inside a transaction block",
            ));
        }
        if let Some(ssi) = &transaction.ssi {
            for predicate in statement_read_predicates(&statement) {
                ssi.record_read(predicate)?;
            }
        }
        if let Some(stream) = prepare_read_stream(&snapshot, statement.clone(), params, None)? {
            return Ok(stream);
        }
        let sequence_id = sequence_mutation_id(&statement);
        let write_scope = statement_write_scope(&statement);
        let maintenance =
            maintenance_context(self.transactions.as_ref(), self.transaction_status.as_ref())?;
        let (candidate, events, dirty) = execute_bound_candidate(
            &snapshot,
            statement,
            params,
            Some(version_mutation_context(&transaction.transaction)?),
            maintenance,
        )?;
        if !dirty {
            return Ok(TryQueryStream::new(events));
        }
        if transaction
            .transaction
            .characteristics()
            .is_some_and(|characteristics| {
                characteristics.access_mode == TransactionAccessMode::ReadOnly
            })
        {
            return Err(DbError::new(
                "25006",
                "cannot execute a write in a read-only transaction",
            ));
        }
        match write_scope {
            StatementWriteScope::Dml => {
                let mut acquired = acquire_dml_locks(
                    &self.locks,
                    &transaction.transaction,
                    &snapshot,
                    &candidate,
                    &transaction.locks,
                    snapshot.cancellation.as_deref(),
                )?;
                if let Some(ssi) = &transaction.ssi {
                    for table_id in changed_table_ids(&snapshot, &candidate) {
                        ssi.record_write(PredicateLock::Table {
                            table_id: table_id.get(),
                        })?;
                    }
                }
                transaction.locks.append(&mut acquired);
                if transaction.base.is_none() {
                    transaction.base = Some(snapshot.clone());
                }
            }
            StatementWriteScope::Exclusive => {
                if transaction.working.is_some() && transaction.dml_only {
                    transaction.locks.clear();
                    let base = transaction.base.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its base snapshot")
                    })?;
                    let working = transaction.working.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its working state")
                    })?;
                    let (upgraded_base, upgraded_working, lease, lock) =
                        upgrade_dml_candidate_to_exclusive(
                            DmlUpgradeAuthorities {
                                state: &self.state,
                                statuses: self.transaction_status.as_ref(),
                                locks: &self.locks,
                                writer: &self.writer,
                            },
                            &transaction.transaction,
                            base,
                            working,
                            snapshot.cancellation.clone(),
                        )?;
                    let (candidate, events, dirty) = execute_candidate(
                        &upgraded_working,
                        sql,
                        params,
                        self.options.dialect,
                        Some(version_mutation_context(&transaction.transaction)?),
                        maintenance,
                    )?;
                    if !dirty {
                        return Err(internal_error(
                            "exclusive statement unexpectedly produced a clean candidate",
                        ));
                    }
                    if let Some(sequence_id) = sequence_id {
                        self.sequence_currvals.insert(
                            sequence_id,
                            candidate_sequence_value(&candidate, sequence_id)?,
                        );
                    }
                    transaction.base = Some(upgraded_base);
                    transaction.working = Some(candidate);
                    transaction.lease = Some(lease);
                    transaction.locks.push(lock);
                    transaction.dml_only = false;
                    return Ok(TryQueryStream::new(events));
                }
                if transaction.lease.is_none() {
                    transaction.lease = Some(
                        self.writer
                            .try_acquire(transaction.transaction.transaction_id())?,
                    );
                    transaction.locks.push(acquire_compatibility_write_lock(
                        &self.locks,
                        &transaction.transaction,
                        snapshot.cancellation.as_deref(),
                    )?);
                }
                transaction.dml_only = false;
                if transaction.base.is_none() {
                    transaction.base = Some(snapshot.clone());
                }
            }
            StatementWriteScope::ReadOnly => {
                return Err(internal_error(
                    "read-only statement unexpectedly produced a dirty candidate",
                ));
            }
        }
        if transaction.working.is_some() || write_scope == StatementWriteScope::Dml {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            transaction.working = Some(candidate);
            return Ok(TryQueryStream::new(events));
        }

        let mut committed = committed_snapshot(&self.state)?;
        committed.cancellation = snapshot.cancellation.clone();
        let (candidate, events, dirty) = execute_candidate(
            &committed,
            sql,
            params,
            self.options.dialect,
            Some(version_mutation_context(&transaction.transaction)?),
            maintenance,
        )?;
        if dirty {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            transaction.working = Some(candidate);
        }
        Ok(TryQueryStream::new(events))
    }

    fn fail_sql_transaction(&mut self) {
        if let SqlTransactionState::Active(transaction) = &mut self.sql_transaction {
            transaction.failed = true;
        }
    }

    fn normalize_sql_transaction_failure(&mut self) {
        if let SqlTransactionState::Active(transaction) = &mut self.sql_transaction
            && transaction.stream_failed.load(Ordering::Acquire)
        {
            transaction.failed = true;
        }
    }
}

fn resolve_sequence_currval(
    statement: BoundStatement,
    currvals: &BTreeMap<SequenceId, i64>,
) -> Result<BoundStatement> {
    match statement {
        BoundStatement::SequenceValue {
            sequence_id,
            operation: BoundSequenceOperation::CurrentValue { .. },
            schema,
        } => Ok(BoundStatement::SequenceValue {
            sequence_id,
            operation: BoundSequenceOperation::CurrentValue {
                value: currvals.get(&sequence_id).copied(),
            },
            schema,
        }),
        statement => Ok(statement),
    }
}

fn sequence_mutation_id(statement: &BoundStatement) -> Option<SequenceId> {
    match statement {
        BoundStatement::SequenceValue {
            sequence_id,
            operation: BoundSequenceOperation::NextValue | BoundSequenceOperation::SetValue { .. },
            ..
        } => Some(*sequence_id),
        _ => None,
    }
}

fn candidate_sequence_value(state: &DatabaseState, sequence_id: SequenceId) -> Result<i64> {
    state
        .catalog
        .sequence_by_id(sequence_id)
        .map(|sequence| sequence.last_value)
        .ok_or_else(|| internal_error("mutated sequence disappeared from the candidate catalog"))
}

fn bound_statement_schema(statement: &BoundStatement) -> Schema {
    match statement {
        BoundStatement::Select { schema, .. }
        | BoundStatement::AdvancedSelect { schema, .. }
        | BoundStatement::ViewSelect { schema, .. }
        | BoundStatement::RoutineSelect { schema, .. }
        | BoundStatement::SequenceValue { schema, .. } => schema.clone(),
        BoundStatement::Explain { .. } => {
            Schema::new(vec![Field::new("QUERY PLAN", ScalarType::Text, false)])
        }
        BoundStatement::Begin { .. }
        | BoundStatement::Commit { .. }
        | BoundStatement::Rollback { .. }
        | BoundStatement::Savepoint { .. }
        | BoundStatement::RollbackTo { .. }
        | BoundStatement::ReleaseSavepoint { .. }
        | BoundStatement::Analyze { .. }
        | BoundStatement::Vacuum { .. }
        | BoundStatement::NoOp { .. }
        | BoundStatement::CreateSchema { .. }
        | BoundStatement::AlterSchemaRename { .. }
        | BoundStatement::DropObjects { .. }
        | BoundStatement::CreateTable { .. }
        | BoundStatement::AlterTable { .. }
        | BoundStatement::CreateIndex { .. }
        | BoundStatement::AlterIndexRename { .. }
        | BoundStatement::CreateSequence { .. }
        | BoundStatement::AlterSequenceRename { .. }
        | BoundStatement::AlterSequence { .. }
        | BoundStatement::CreateView { .. }
        | BoundStatement::AlterViewRename { .. }
        | BoundStatement::RefreshMaterializedView { .. }
        | BoundStatement::CreateRoutine { .. }
        | BoundStatement::DropRoutine { .. }
        | BoundStatement::Call { .. }
        | BoundStatement::CreateTrigger { .. }
        | BoundStatement::DropTrigger { .. }
        | BoundStatement::Insert { .. }
        | BoundStatement::Update { .. }
        | BoundStatement::Delete { .. } => Schema::empty(),
    }
}

#[derive(Debug)]
pub struct Transaction<'session> {
    state: &'session Arc<RwLock<DatabaseState>>,
    store: &'session Arc<Mutex<DatabaseStore>>,
    storage_access: &'session Arc<StorageAccessGate>,
    wal: &'session Arc<WalManager>,
    transaction_status: &'session Arc<TransactionStatusStore>,
    transactions: &'session Arc<TransactionManager>,
    locks: &'session Arc<LockManager>,
    writer: &'session Arc<WriterCoordinator>,
    commits_since_checkpoint: &'session Arc<AtomicU64>,
    sequence_currvals: &'session mut BTreeMap<SequenceId, i64>,
    dialect: SqlDialect,
    transaction: DurableTransaction,
    base: Option<DatabaseState>,
    working: Option<DatabaseState>,
    lock_guards: Vec<LockGuard>,
    lease: Option<WriterLease>,
    dml_only: bool,
    failed: bool,
}

impl Transaction<'_> {
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        if self.failed {
            return Err(failed_transaction_error());
        }
        self.transaction.begin_statement()?;
        if let Err(error) = refresh_read_committed_candidate(
            self.state,
            self.transaction_status.as_ref(),
            &self.transaction,
            &mut self.base,
            &mut self.working,
            self.dml_only,
        ) {
            self.working = None;
            self.lock_guards.clear();
            self.lease = None;
            self.failed = true;
            return Err(error);
        }
        match self.execute_inner(sql, params) {
            Ok(stream) => {
                self.transaction.finish_statement()?;
                Ok(stream)
            }
            Err(error) => {
                self.working = None;
                self.lock_guards.clear();
                self.lease = None;
                self.failed = true;
                Err(error)
            }
        }
    }

    pub fn commit(mut self) -> Result<()> {
        if self.failed {
            return Err(failed_transaction_error());
        }
        let Some(candidate) = self.working else {
            return self.transaction.commit_empty();
        };
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let candidate = if self.dml_only {
            let base = self
                .base
                .as_ref()
                .ok_or_else(|| internal_error("DML transaction is missing its base snapshot"))?;
            merge_dml_candidate(
                &state,
                base,
                &candidate,
                &self.transaction,
                self.transaction_status.as_ref(),
            )?
        } else {
            candidate
        };
        persist_candidate(
            &mut state,
            self.store,
            self.storage_access,
            self.wal,
            &mut self.transaction,
            candidate,
        )?;
        drop(state);
        self.lock_guards.clear();
        self.lease = None;
        record_commit_and_maybe_checkpoint(
            self.state,
            self.store,
            self.wal,
            self.transactions,
            self.commits_since_checkpoint,
        )
    }

    pub fn rollback(self) -> Result<()> {
        self.transaction.abort()
    }

    fn execute_inner(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        let snapshot = match &self.working {
            Some(working) => working.clone(),
            None => {
                let committed = committed_snapshot(self.state)?;
                let transaction_snapshot = self
                    .transaction
                    .snapshot()
                    .ok_or_else(|| no_active_transaction_error("take a statement snapshot"))?;
                project_database_visibility(
                    committed,
                    transaction_snapshot,
                    self.transaction.transaction_id(),
                    self.transaction_status.as_ref(),
                )?
            }
        };
        let statement = resolve_sequence_currval(
            bind(parse_with_dialect(sql, self.dialect)?, &snapshot.catalog)?,
            self.sequence_currvals,
        )?;
        if matches!(
            &statement,
            BoundStatement::Begin { .. }
                | BoundStatement::Commit { .. }
                | BoundStatement::Rollback { .. }
                | BoundStatement::Savepoint { .. }
                | BoundStatement::RollbackTo { .. }
                | BoundStatement::ReleaseSavepoint { .. }
        ) {
            return Err(DbError::new(
                "25001",
                "SQL transaction control is not allowed inside Session::begin",
            )
            .with_hint("use Transaction::commit or Transaction::rollback"));
        }
        if matches!(&statement, BoundStatement::Vacuum { .. }) {
            return Err(DbError::new(
                "25001",
                "VACUUM cannot run inside a transaction block",
            ));
        }
        if let Some(stream) = prepare_read_stream(&snapshot, statement.clone(), params, None)? {
            return stream.collect::<Result<Vec<_>>>().map(QueryStream::new);
        }
        let sequence_id = sequence_mutation_id(&statement);
        let write_scope = statement_write_scope(&statement);
        let maintenance =
            maintenance_context(self.transactions.as_ref(), self.transaction_status.as_ref())?;
        let (candidate, events, dirty) = execute_bound_candidate(
            &snapshot,
            statement,
            params,
            Some(version_mutation_context(&self.transaction)?),
            maintenance,
        )?;
        if !dirty {
            return Ok(QueryStream::new(events));
        }
        match write_scope {
            StatementWriteScope::Dml => {
                let mut acquired = acquire_dml_locks(
                    self.locks,
                    &self.transaction,
                    &snapshot,
                    &candidate,
                    &self.lock_guards,
                    None,
                )?;
                self.lock_guards.append(&mut acquired);
                if self.base.is_none() {
                    self.base = Some(snapshot.clone());
                }
            }
            StatementWriteScope::Exclusive => {
                if self.working.is_some() && self.dml_only {
                    self.lock_guards.clear();
                    let base = self.base.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its base snapshot")
                    })?;
                    let working = self.working.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its working state")
                    })?;
                    let (upgraded_base, upgraded_working, lease, lock) =
                        upgrade_dml_candidate_to_exclusive(
                            DmlUpgradeAuthorities {
                                state: self.state,
                                statuses: self.transaction_status.as_ref(),
                                locks: self.locks,
                                writer: self.writer,
                            },
                            &self.transaction,
                            base,
                            working,
                            None,
                        )?;
                    let (candidate, events, dirty) = execute_candidate(
                        &upgraded_working,
                        sql,
                        params,
                        self.dialect,
                        Some(version_mutation_context(&self.transaction)?),
                        maintenance,
                    )?;
                    if !dirty {
                        return Err(internal_error(
                            "exclusive statement unexpectedly produced a clean candidate",
                        ));
                    }
                    if let Some(sequence_id) = sequence_id {
                        self.sequence_currvals.insert(
                            sequence_id,
                            candidate_sequence_value(&candidate, sequence_id)?,
                        );
                    }
                    self.base = Some(upgraded_base);
                    self.working = Some(candidate);
                    self.lease = Some(lease);
                    self.lock_guards.push(lock);
                    self.dml_only = false;
                    return Ok(QueryStream::new(events));
                }
                if self.lease.is_none() {
                    self.lease = Some(self.writer.try_acquire(self.transaction.transaction_id())?);
                    self.lock_guards.push(acquire_compatibility_write_lock(
                        self.locks,
                        &self.transaction,
                        None,
                    )?);
                }
                self.dml_only = false;
                if self.base.is_none() {
                    self.base = Some(snapshot.clone());
                }
            }
            StatementWriteScope::ReadOnly => {
                return Err(internal_error(
                    "read-only statement unexpectedly produced a dirty candidate",
                ));
            }
        }
        if self.working.is_some() || write_scope == StatementWriteScope::Dml {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            self.working = Some(candidate);
            return Ok(QueryStream::new(events));
        }

        let committed = committed_snapshot(self.state)?;
        let (candidate, events, dirty) = execute_candidate(
            &committed,
            sql,
            params,
            self.dialect,
            Some(version_mutation_context(&self.transaction)?),
            maintenance,
        )?;
        if dirty {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            self.working = Some(candidate);
        }
        Ok(QueryStream::new(events))
    }
}

#[derive(Debug)]
pub struct QueryStream {
    events: std::vec::IntoIter<QueryEvent>,
}

pub struct TryQueryStream {
    state: TryQueryStreamState,
    failed: bool,
    failure_flag: Option<Arc<AtomicBool>>,
    cancellation: Option<Arc<AtomicBool>>,
    execution_memory_peak_bytes: Option<usize>,
}

enum TryQueryStreamState {
    Events(std::vec::IntoIter<Result<QueryEvent>>),
    Select(Box<SelectStreamState>),
    Done,
}

struct SelectStreamState {
    schema: Schema,
    cursor: StreamBatchCursor,
    phase: SelectStreamPhase,
    rows_processed: u64,
    emitted_batch: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectStreamPhase {
    Schema,
    Batches,
    EmptyBatch,
    Progress,
    Complete,
    Done,
}

enum StreamBatchCursor {
    Simple(Box<ExecutionCursor>),
    Advanced(Box<AdvancedExecutionCursor>),
}

impl StreamBatchCursor {
    fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Self::Simple(cursor) => cursor.next_batch(),
            Self::Advanced(cursor) => cursor.next_batch(),
        }
    }

    fn memory_peak_bytes(&self) -> usize {
        match self {
            Self::Simple(cursor) => cursor.memory().peak_bytes(),
            Self::Advanced(cursor) => cursor.memory().peak_bytes(),
        }
    }
}

impl std::fmt::Debug for TryQueryStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TryQueryStream")
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl TryQueryStream {
    fn new(events: Vec<QueryEvent>) -> Self {
        Self {
            state: TryQueryStreamState::Events(
                events.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
            ),
            failed: false,
            failure_flag: None,
            cancellation: None,
            execution_memory_peak_bytes: None,
        }
    }

    fn select(
        schema: Schema,
        cursor: StreamBatchCursor,
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Self {
        Self {
            state: TryQueryStreamState::Select(Box::new(SelectStreamState {
                schema,
                cursor,
                phase: SelectStreamPhase::Schema,
                rows_processed: 0,
                emitted_batch: false,
            })),
            failed: false,
            failure_flag: None,
            cancellation,
            execution_memory_peak_bytes: Some(0),
        }
    }

    fn with_failure_flag(mut self, failure_flag: Arc<AtomicBool>) -> Self {
        self.failure_flag = Some(failure_flag);
        self
    }

    /// Returns the highest number of query-accounted bytes observed by this
    /// SELECT stream. Non-SELECT statements do not own an execution cursor.
    #[must_use]
    pub const fn execution_memory_peak_bytes(&self) -> Option<usize> {
        self.execution_memory_peak_bytes
    }
}

impl Iterator for TryQueryStream {
    type Item = Result<QueryEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if self
            .cancellation
            .as_ref()
            .is_some_and(|cancellation| cancellation.load(Ordering::Acquire))
        {
            self.failed = true;
            if let Some(failure_flag) = &self.failure_flag {
                failure_flag.store(true, Ordering::Release);
            }
            self.state = TryQueryStreamState::Done;
            return Some(Err(DbError::new("57014", "query was cancelled")));
        }
        let event = match &mut self.state {
            TryQueryStreamState::Events(events) => events.next().transpose(),
            TryQueryStreamState::Select(stream) => stream.next_event(),
            TryQueryStreamState::Done => Ok(None),
        };
        if let TryQueryStreamState::Select(stream) = &self.state {
            self.execution_memory_peak_bytes = Some(stream.cursor.memory_peak_bytes());
        }
        match event {
            Ok(Some(event)) => Some(Ok(event)),
            Ok(None) => {
                self.state = TryQueryStreamState::Done;
                None
            }
            Err(error) => {
                self.failed = true;
                if let Some(failure_flag) = &self.failure_flag {
                    failure_flag.store(true, Ordering::Release);
                }
                self.state = TryQueryStreamState::Done;
                Some(Err(error))
            }
        }
    }
}

impl SelectStreamState {
    fn next_event(&mut self) -> Result<Option<QueryEvent>> {
        match self.phase {
            SelectStreamPhase::Schema => {
                self.phase = SelectStreamPhase::Batches;
                Ok(Some(QueryEvent::Schema(self.schema.clone())))
            }
            SelectStreamPhase::Batches => match self.cursor.next_batch()? {
                Some(batch) => {
                    self.rows_processed =
                        self.rows_processed.saturating_add(batch.rows.len() as u64);
                    self.emitted_batch = true;
                    Ok(Some(QueryEvent::Batch(batch)))
                }
                None if !self.emitted_batch => {
                    self.phase = SelectStreamPhase::EmptyBatch;
                    Ok(Some(QueryEvent::Batch(Batch {
                        schema: self.schema.clone(),
                        rows: Vec::new(),
                    })))
                }
                None => {
                    self.phase = SelectStreamPhase::Progress;
                    self.next_event()
                }
            },
            SelectStreamPhase::EmptyBatch => {
                self.phase = SelectStreamPhase::Progress;
                self.next_event()
            }
            SelectStreamPhase::Progress => {
                self.phase = SelectStreamPhase::Complete;
                Ok(Some(QueryEvent::Progress(QueryProgress {
                    rows_processed: self.rows_processed,
                })))
            }
            SelectStreamPhase::Complete => {
                self.phase = SelectStreamPhase::Done;
                Ok(Some(QueryEvent::Complete(CommandComplete {
                    tag: format!("SELECT {}", self.rows_processed),
                    rows_affected: self.rows_processed,
                })))
            }
            SelectStreamPhase::Done => Ok(None),
        }
    }
}

impl QueryStream {
    fn new(events: Vec<QueryEvent>) -> Self {
        Self {
            events: events.into_iter(),
        }
    }
}

impl Iterator for QueryStream {
    type Item = QueryEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.events.next()
    }
}

#[derive(Debug, Clone, Default)]
struct DatabaseState {
    catalog: Arc<Catalog>,
    rows: BTreeMap<TableId, Arc<Vec<Row>>>,
    versions: BTreeMap<TableId, Arc<Vec<VersionedRow>>>,
    visible_versions: BTreeMap<TableId, Arc<Vec<u32>>>,
    indexes: BTreeMap<IndexId, Arc<BPlusTree>>,
    searches: Arc<SearchCatalog>,
    generation: u64,
    trigger_depth: usize,
    triggers_fired: usize,
    routine_depth: usize,
    cancellation: Option<Arc<AtomicBool>>,
}

struct SelectExecution {
    table_id: TableId,
    schema: Schema,
    projection: Vec<BoundProjection>,
    filter: Option<BoundExpr>,
    order_by: Vec<BoundOrder>,
    limit: Option<BoundExpr>,
}

struct CreateViewExecution {
    schema: Identifier,
    name: Identifier,
    kind: ViewKind,
    query: BoundStatement,
    query_sql: String,
    output: Schema,
    references: Vec<CatalogObjectRef>,
    replace: bool,
    if_not_exists: bool,
    with_data: bool,
    existing: Option<ViewId>,
}

enum ReferentialChange {
    Delete {
        table_id: TableId,
        old: Row,
    },
    Update {
        table_id: TableId,
        old: Row,
        new: Row,
    },
}

struct AdvancedExecution {
    table: BoundTable,
    joins: Vec<BoundJoin>,
    schema: Schema,
    projection: Vec<BoundProjection>,
    filter: Option<BoundExpr>,
    group_by: Vec<BoundExpr>,
    having: Option<BoundExpr>,
    order_by: Vec<BoundOrder>,
    limit: Option<BoundExpr>,
    aggregate: bool,
}

impl DatabaseState {
    fn from_persistent(
        state: PersistentState,
        data_format: DataFormat,
        statuses: &impl TransactionStatusProvider,
        next_transaction_id: u64,
    ) -> Result<Self> {
        let PersistentState {
            generation,
            catalog,
            tables,
            mut versions,
            indexes,
        } = state;
        if data_format == DataFormat::V2 {
            if !indexes.is_empty() {
                return Err(DbError::new(
                    "XX001",
                    "database v2 contains durable derived index state",
                ));
            }
            let catalog_table_ids = catalog
                .database()
                .schemas()
                .flat_map(|schema| schema.tables())
                .map(|table| table.id)
                .collect::<BTreeSet<_>>();
            if tables
                .keys()
                .chain(versions.keys())
                .any(|table_id| !catalog_table_ids.contains(table_id))
            {
                return Err(DbError::new(
                    "XX001",
                    "database v2 contains rows for a table absent from its catalog",
                ));
            }
            let snapshot_transaction = TransactionId::new(next_transaction_id)
                .ok_or_else(|| internal_error("transaction high-water mark is zero"))?;
            let visibility_snapshot = TransactionSnapshot {
                xmin: snapshot_transaction,
                xmax: snapshot_transaction,
                in_progress: Arc::new(BTreeSet::new()),
                command_id: u32::MAX,
            };
            let mut rows = BTreeMap::new();
            let mut version_rows = BTreeMap::new();
            let mut visible_versions = BTreeMap::new();
            for table_id in &catalog_table_ids {
                let table_versions = match versions.remove(table_id) {
                    Some(versions) => versions,
                    None => tables
                        .get(table_id)
                        .into_iter()
                        .flatten()
                        .enumerate()
                        .map(|(index, row)| {
                            let version_id = u32::try_from(index)
                                .ok()
                                .and_then(|index| index.checked_add(1))
                                .ok_or_else(|| {
                                    DbError::new(
                                        "54000",
                                        "table exceeds the u32 version ordinal limit",
                                    )
                                })?;
                            Ok(VersionedRow {
                                version_id,
                                header: TupleHeaderV2::frozen(row)?,
                                row: row.clone(),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                };
                let mut visible_rows = Vec::new();
                let mut visible_ids = Vec::new();
                for (index, version) in table_versions.iter().enumerate() {
                    let expected_version = u32::try_from(index)
                        .ok()
                        .and_then(|index| index.checked_add(1))
                        .ok_or_else(|| {
                            DbError::new("54000", "table exceeds the u32 version ordinal limit")
                        })?;
                    if version.version_id != expected_version
                        || version.header.previous_version >= version.version_id
                    {
                        return Err(DbError::new(
                            "XX001",
                            format!(
                                "table {} has an invalid version chain at ordinal {}",
                                table_id.get(),
                                version.version_id
                            ),
                        ));
                    }
                    if tuple_visible(
                        version.header,
                        &visibility_snapshot,
                        snapshot_transaction,
                        statuses,
                    )? {
                        visible_rows.push(version.row.clone());
                        visible_ids.push(version.version_id);
                    }
                }
                rows.insert(*table_id, Arc::new(visible_rows));
                version_rows.insert(*table_id, Arc::new(table_versions));
                visible_versions.insert(*table_id, Arc::new(visible_ids));
            }
            let mut state = Self {
                catalog: Arc::new(catalog),
                rows,
                versions: version_rows,
                visible_versions,
                indexes: BTreeMap::new(),
                searches: Arc::new(SearchCatalog::default()),
                generation,
                trigger_depth: 0,
                triggers_fired: 0,
                routine_depth: 0,
                cancellation: None,
            };
            validate_database_rows(&state)?;
            for table_id in catalog_table_ids {
                rebuild_table_derived(&mut state, table_id)?;
            }
            return Ok(state);
        }
        let indexes = indexes
            .into_iter()
            .map(|(index_id, entries)| {
                let definition = catalog
                    .index_by_id(index_id)
                    .ok_or_else(|| internal_error("persistent index is absent from the catalog"))?;
                BPlusTree::from_entries(definition.unique, entries)
                    .map(|tree| (index_id, Arc::new(tree)))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let rows = tables
            .into_iter()
            .map(|(table_id, rows)| (table_id, Arc::new(rows)))
            .collect::<BTreeMap<_, _>>();
        let (versions, visible_versions) = frozen_version_state(&rows)?;
        let searches = SearchCatalog::build(&catalog, &rows, SearchLimits::default())?;
        Ok(Self {
            catalog: Arc::new(catalog),
            rows,
            versions,
            visible_versions,
            indexes,
            searches: Arc::new(searches),
            generation,
            trigger_depth: 0,
            triggers_fired: 0,
            routine_depth: 0,
            cancellation: None,
        })
    }

    fn from_logical_snapshot(snapshot: LogicalDatabaseSnapshot) -> Result<Self> {
        if snapshot.format_version != LOGICAL_SNAPSHOT_VERSION {
            return Err(DbError::new(
                "0A000",
                format!(
                    "logical snapshot version {} is not supported",
                    snapshot.format_version
                ),
            )
            .with_hint("use a compatible OrdaDB version or perform an explicit migration"));
        }
        let catalog_table_ids = snapshot
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .map(|table| table.id)
            .collect::<BTreeSet<_>>();
        if snapshot
            .tables
            .keys()
            .any(|table_id| !catalog_table_ids.contains(table_id))
        {
            return Err(DbError::new(
                "XX001",
                "logical snapshot contains rows for a table absent from its catalog",
            ));
        }
        let rows = catalog_table_ids
            .iter()
            .map(|table_id| {
                (
                    *table_id,
                    snapshot
                        .tables
                        .get(table_id)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(Vec::new())),
                )
            })
            .collect();
        let mut state = Self {
            catalog: snapshot.catalog,
            rows,
            versions: BTreeMap::new(),
            visible_versions: BTreeMap::new(),
            indexes: BTreeMap::new(),
            searches: Arc::new(SearchCatalog::default()),
            generation: snapshot.source_generation,
            trigger_depth: 0,
            triggers_fired: 0,
            routine_depth: 0,
            cancellation: None,
        };
        (state.versions, state.visible_versions) = frozen_version_state(&state.rows)?;
        validate_database_rows(&state)?;
        for table_id in catalog_table_ids {
            rebuild_table_derived(&mut state, table_id)?;
        }
        Ok(state)
    }
}

type VersionRowsByTable = BTreeMap<TableId, Arc<Vec<VersionedRow>>>;
type VisibleVersionsByTable = BTreeMap<TableId, Arc<Vec<u32>>>;

fn frozen_version_state(
    rows: &BTreeMap<TableId, Arc<Vec<Row>>>,
) -> Result<(VersionRowsByTable, VisibleVersionsByTable)> {
    let mut versions = BTreeMap::new();
    let mut visible_versions = BTreeMap::new();
    for (table_id, table_rows) in rows {
        let mut table_versions = Vec::with_capacity(table_rows.len());
        let mut visible_ids = Vec::with_capacity(table_rows.len());
        for (index, row) in table_rows.iter().enumerate() {
            let version_id = u32::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .ok_or_else(|| {
                    DbError::new("54000", "table exceeds the u32 version ordinal limit")
                })?;
            table_versions.push(VersionedRow {
                version_id,
                header: TupleHeaderV2::frozen(row)?,
                row: row.clone(),
            });
            visible_ids.push(version_id);
        }
        versions.insert(*table_id, Arc::new(table_versions));
        visible_versions.insert(*table_id, Arc::new(visible_ids));
    }
    Ok((versions, visible_versions))
}

impl From<&DatabaseState> for PersistentState {
    fn from(state: &DatabaseState) -> Self {
        Self {
            generation: state.generation,
            catalog: (*state.catalog).clone(),
            tables: state
                .rows
                .iter()
                .map(|(table_id, rows)| (*table_id, (**rows).clone()))
                .collect(),
            versions: state
                .versions
                .iter()
                .map(|(table_id, versions)| (*table_id, (**versions).clone()))
                .collect(),
            indexes: state
                .indexes
                .iter()
                .map(|(index_id, tree)| (*index_id, tree.iter().cloned().collect::<Vec<_>>()))
                .collect(),
        }
    }
}

fn committed_snapshot(state: &Arc<RwLock<DatabaseState>>) -> Result<DatabaseState> {
    state
        .read()
        .map(|state| state.clone())
        .map_err(|_| internal_error("engine state lock is poisoned"))
}

fn project_database_visibility(
    mut state: DatabaseState,
    snapshot: &TransactionSnapshot,
    current_transaction: TransactionId,
    statuses: &impl TransactionStatusProvider,
) -> Result<DatabaseState> {
    let table_ids = state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .map(|table| table.id)
        .collect::<Vec<_>>();
    state.rows.clear();
    state.visible_versions.clear();
    state.indexes.clear();
    state.searches = Arc::new(SearchCatalog::default());
    for table_id in &table_ids {
        let versions = state
            .versions
            .get(table_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()));
        let mut rows = Vec::new();
        let mut visible_versions = Vec::new();
        for version in versions.iter() {
            if tuple_visible(version.header, snapshot, current_transaction, statuses)? {
                rows.push(version.row.clone());
                visible_versions.push(version.version_id);
            }
        }
        state.rows.insert(*table_id, Arc::new(rows));
        state
            .visible_versions
            .insert(*table_id, Arc::new(visible_versions));
    }
    validate_database_rows(&state)?;
    for table_id in table_ids {
        rebuild_table_derived(&mut state, table_id)?;
    }
    Ok(state)
}

fn parsed_is_transaction_control(statement: &ParsedStatement) -> bool {
    matches!(
        statement,
        ParsedStatement::Begin { .. }
            | ParsedStatement::Commit { .. }
            | ParsedStatement::Rollback { .. }
            | ParsedStatement::Savepoint { .. }
            | ParsedStatement::RollbackTo { .. }
            | ParsedStatement::ReleaseSavepoint { .. }
    )
}

/// Bridges the execution scan contract to authoritative v2 heap pages.
pub struct StorageTableProviderV2<'a> {
    store: Arc<Mutex<DatabaseStore>>,
    storage_access: Arc<StorageAccessGate>,
    generation: u64,
    rows: &'a BTreeMap<TableId, Arc<Vec<Row>>>,
}

impl<'a> StorageTableProviderV2<'a> {
    fn new(
        store: Arc<Mutex<DatabaseStore>>,
        storage_access: Arc<StorageAccessGate>,
        generation: u64,
        rows: &'a BTreeMap<TableId, Arc<Vec<Row>>>,
    ) -> Self {
        Self {
            store,
            storage_access,
            generation,
            rows,
        }
    }
}

impl TableProvider for StorageTableProviderV2<'_> {
    fn scan(&self, table_id: TableId) -> Result<Box<dyn TableScan>> {
        let lease = self.storage_access.acquire_read()?;
        let cursor = self
            .store
            .lock()
            .map_err(|_| internal_error("database store lock is poisoned"))?
            .open_table_cursor_v2(table_id, self.generation)?;
        let rows = self
            .rows
            .get(&table_id)
            .cloned()
            .ok_or_else(|| internal_error("v2 table is absent from the resident snapshot"))?;
        let resident_rows = u64::try_from(rows.len()).map_err(|_| {
            DbError::new("54000", "resident table row count exceeds storage limits")
        })?;
        if resident_rows != cursor.expected_visible_rows() {
            return Err(DbError::new(
                "XX001",
                "resident visible row count does not match v2 heap metadata",
            )
            .with_detail(format!(
                "table {} has {resident_rows} visible rows but the v2 heap declares {}",
                table_id.get(),
                cursor.expected_visible_rows()
            )));
        }
        Ok(Box::new(StorageTableScanV2 {
            _cursor: cursor,
            rows,
            offset: 0,
            lease: Some(lease),
        }))
    }
}

struct StorageTableScanV2 {
    _cursor: StorageTableCursorV2,
    rows: Arc<Vec<Row>>,
    offset: usize,
    lease: Option<StorageReadLease>,
}

impl TableScan for StorageTableScanV2 {
    fn next_chunk(
        &mut self,
        max_rows: usize,
        grant: &MemoryGrant,
    ) -> Result<Option<LeasedDataChunk>> {
        if max_rows == 0 {
            self.lease = None;
            return Err(DbError::new(
                "22023",
                "table scan chunk size must be positive",
            ));
        }
        let expected_rows = self.rows.len();
        if self.offset >= expected_rows {
            self.lease = None;
            return Ok(None);
        }
        let end = self.offset.saturating_add(max_rows).min(expected_rows);
        match LeasedDataChunk::from_snapshot(Arc::clone(&self.rows), self.offset, end, grant) {
            Ok(chunk) => {
                self.offset = end;
                if self.offset == expected_rows {
                    self.lease = None;
                }
                Ok(Some(chunk))
            }
            Err(error) => {
                self.lease = None;
                Err(error)
            }
        }
    }
}

fn prepare_read_stream(
    state: &DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    table_provider: Option<&dyn TableProvider>,
) -> Result<Option<TryQueryStream>> {
    match statement {
        BoundStatement::Select {
            table_id,
            schema,
            projection,
            filter,
            order_by,
            limit,
        } => {
            let (schema, cursor) = prepare_select_cursor(
                state,
                SelectExecution {
                    table_id,
                    schema,
                    projection,
                    filter,
                    order_by,
                    limit,
                },
                params,
                table_provider,
            )?;
            Ok(Some(TryQueryStream::select(
                schema,
                StreamBatchCursor::Simple(Box::new(cursor)),
                state.cancellation.clone(),
            )))
        }
        BoundStatement::AdvancedSelect {
            table,
            joins,
            schema,
            projection,
            filter,
            group_by,
            having,
            order_by,
            limit,
            aggregate,
        } => {
            let (schema, cursor) = prepare_advanced_cursor(
                state,
                AdvancedExecution {
                    table,
                    joins,
                    schema,
                    projection,
                    filter,
                    group_by,
                    having,
                    order_by,
                    limit,
                    aggregate,
                },
                params,
            )?;
            Ok(Some(TryQueryStream::select(
                schema,
                StreamBatchCursor::Advanced(Box::new(cursor)),
                state.cancellation.clone(),
            )))
        }
        _ => Ok(None),
    }
}

#[derive(Debug, Clone, Copy)]
struct VersionMutationContext {
    transaction_id: TransactionId,
    command_id: u32,
}

#[derive(Clone, Copy)]
struct MaintenanceContext<'a> {
    horizon: TransactionId,
    expired_snapshot: Option<TransactionId>,
    statuses: &'a TransactionStatusStore,
}

fn maintenance_context<'a>(
    transactions: &TransactionManager,
    statuses: &'a TransactionStatusStore,
) -> Result<MaintenanceContext<'a>> {
    Ok(MaintenanceContext {
        horizon: transactions.global_xmin()?,
        expired_snapshot: transactions.expired_snapshot()?,
        statuses,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementWriteScope {
    ReadOnly,
    Dml,
    Exclusive,
}

fn statement_write_scope(statement: &BoundStatement) -> StatementWriteScope {
    match statement {
        BoundStatement::Insert { .. }
        | BoundStatement::Update { .. }
        | BoundStatement::Delete { .. } => StatementWriteScope::Dml,
        BoundStatement::Select { .. }
        | BoundStatement::AdvancedSelect { .. }
        | BoundStatement::ViewSelect { .. }
        | BoundStatement::RoutineSelect { .. }
        | BoundStatement::Explain { .. }
        | BoundStatement::NoOp { .. } => StatementWriteScope::ReadOnly,
        _ => StatementWriteScope::Exclusive,
    }
}

fn statement_read_predicates(statement: &BoundStatement) -> Vec<PredicateLock> {
    let mut table_ids = BTreeSet::new();
    let mut pending = vec![statement];
    while let Some(statement) = pending.pop() {
        match statement {
            BoundStatement::Select { table_id, .. } => {
                table_ids.insert(*table_id);
            }
            BoundStatement::AdvancedSelect { table, joins, .. } => {
                table_ids.insert(table.table_id);
                table_ids.extend(joins.iter().map(|join| join.table.table_id));
            }
            BoundStatement::ViewSelect { source, .. }
            | BoundStatement::Explain { statement: source } => {
                pending.push(source);
            }
            _ => {}
        }
    }
    table_ids
        .into_iter()
        .map(|table_id| PredicateLock::Table {
            table_id: table_id.get(),
        })
        .collect()
}

fn changed_table_ids(before: &DatabaseState, after: &DatabaseState) -> BTreeSet<TableId> {
    before
        .versions
        .keys()
        .chain(after.versions.keys())
        .copied()
        .filter(|table_id| {
            before.versions.get(table_id) != after.versions.get(table_id)
                || before.visible_versions.get(table_id) != after.visible_versions.get(table_id)
        })
        .collect()
}

fn acquire_compatibility_write_lock(
    locks: &Arc<LockManager>,
    transaction: &DurableTransaction,
    cancelled: Option<&AtomicBool>,
) -> Result<LockGuard> {
    locks.acquire(
        transaction.transaction_id(),
        LockKey::Database,
        LockMode::Exclusive,
        None,
        cancelled,
    )
}

fn acquire_dml_locks(
    locks: &Arc<LockManager>,
    transaction: &DurableTransaction,
    before: &DatabaseState,
    after: &DatabaseState,
    existing: &[LockGuard],
    cancelled: Option<&AtomicBool>,
) -> Result<Vec<LockGuard>> {
    let mut keys = BTreeSet::from([LockKey::Database]);
    let transaction_id = transaction.transaction_id();
    let table_ids = before
        .versions
        .keys()
        .chain(after.versions.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for table_id in table_ids {
        let before_versions = before
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        let after_versions = after
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        let changed = before_versions != after_versions
            || before.visible_versions.get(&table_id) != after.visible_versions.get(&table_id);
        if !changed {
            continue;
        }
        keys.insert(LockKey::Table {
            table_id: table_id.get(),
        });
        for (before_version, after_version) in before_versions.iter().zip(after_versions) {
            if before_version.header.xmax == 0 && after_version.header.xmax == transaction_id.get()
            {
                keys.insert(LockKey::Row {
                    table_id: table_id.get(),
                    version_id: u64::from(before_version.version_id),
                });
            }
        }
        let before_visible = before
            .visible_versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice())
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let Some(table) = after.catalog.table_by_id(table_id) else {
            continue;
        };
        let after_rows = after
            .rows
            .get(&table_id)
            .map_or(&[][..], |rows| rows.as_slice());
        let after_visible = after
            .visible_versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        for (row, version_id) in after_rows.iter().zip(after_visible) {
            if before_visible.contains(version_id) {
                continue;
            }
            for index in table.indexes().filter(|index| index.unique) {
                let key_values = index
                    .key_columns
                    .iter()
                    .map(|column_id| {
                        table
                            .column_index_by_id(*column_id)
                            .and_then(|position| row.values.get(position))
                            .cloned()
                            .ok_or_else(|| {
                                internal_error("unique-index lock column is outside its row")
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if key_values.iter().any(Value::is_null) {
                    continue;
                }
                let encoded = serde_json::to_vec(&key_values)
                    .map_err(|error| internal_error(error.to_string()))?;
                let fingerprint: [u8; 32] = Sha256::digest(encoded).into();
                keys.insert(LockKey::IndexKey {
                    index_id: index.id.get(),
                    fingerprint,
                });
            }
        }
    }
    let existing = existing
        .iter()
        .map(|guard| guard.key().clone())
        .collect::<BTreeSet<_>>();
    let mut acquired = Vec::new();
    for key in keys {
        if existing.contains(&key) {
            continue;
        }
        let mode = if key == LockKey::Database || matches!(key, LockKey::Table { .. }) {
            LockMode::Shared
        } else {
            LockMode::Exclusive
        };
        acquired.push(locks.acquire(transaction_id, key, mode, None, cancelled)?);
    }
    Ok(acquired)
}

fn version_mutation_context(transaction: &DurableTransaction) -> Result<VersionMutationContext> {
    Ok(VersionMutationContext {
        transaction_id: transaction.transaction_id(),
        command_id: transaction
            .snapshot()
            .ok_or_else(|| DbError::new("25P01", "transaction is no longer active"))?
            .command_id,
    })
}

fn execute_bound_candidate(
    state: &DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    version_context: Option<VersionMutationContext>,
    maintenance: MaintenanceContext<'_>,
) -> Result<(DatabaseState, Vec<QueryEvent>, bool)> {
    let mut candidate = state.clone();
    candidate.trigger_depth = 0;
    candidate.triggers_fired = 0;
    candidate.routine_depth = 0;
    let reconciles_versions = !matches!(
        &statement,
        BoundStatement::Analyze { .. } | BoundStatement::Vacuum { .. }
    );
    let (events, dirty) = execute_root_bound(&mut candidate, statement, params, maintenance)?;
    if dirty
        && reconciles_versions
        && let Some(version_context) = version_context
    {
        reconcile_version_changes(state, &mut candidate, version_context)?;
    }
    candidate.cancellation = None;
    Ok((candidate, events, dirty))
}

fn execute_candidate(
    state: &DatabaseState,
    sql: &str,
    params: &[Value],
    dialect: SqlDialect,
    version_context: Option<VersionMutationContext>,
    maintenance: MaintenanceContext<'_>,
) -> Result<(DatabaseState, Vec<QueryEvent>, bool)> {
    let parsed = parse_with_dialect(sql, dialect)?;
    let statement = bind(parsed, &state.catalog)?;
    let mut candidate = state.clone();
    candidate.trigger_depth = 0;
    candidate.triggers_fired = 0;
    candidate.routine_depth = 0;
    let reconciles_versions = !matches!(
        &statement,
        BoundStatement::Analyze { .. } | BoundStatement::Vacuum { .. }
    );
    let (events, dirty) = execute_root_bound(&mut candidate, statement, params, maintenance)?;
    if dirty
        && reconciles_versions
        && let Some(version_context) = version_context
    {
        reconcile_version_changes(state, &mut candidate, version_context)?;
    }
    candidate.cancellation = None;
    Ok((candidate, events, dirty))
}

fn reconcile_version_changes(
    before: &DatabaseState,
    after: &mut DatabaseState,
    context: VersionMutationContext,
) -> Result<()> {
    let table_ids = before
        .rows
        .keys()
        .chain(after.rows.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for table_id in table_ids {
        let Some(after_rows) = after.rows.get(&table_id).map(|rows| (**rows).clone()) else {
            after.versions.remove(&table_id);
            after.visible_versions.remove(&table_id);
            continue;
        };
        let before_rows = before
            .rows
            .get(&table_id)
            .map(|rows| (**rows).clone())
            .unwrap_or_default();
        let before_ids = before
            .visible_versions
            .get(&table_id)
            .map(|versions| (**versions).clone())
            .unwrap_or_default();
        if before_rows.len() != before_ids.len() {
            return Err(internal_error(format!(
                "table {} visible row/version state is not aligned",
                table_id.get()
            )));
        }
        let mut versions = before
            .versions
            .get(&table_id)
            .map(|versions| (**versions).clone())
            .unwrap_or_default();
        let visible_ids = reconcile_table_version_changes(
            &before_rows,
            &before_ids,
            &after_rows,
            &mut versions,
            context,
        )?;
        after.versions.insert(table_id, Arc::new(versions));
        after
            .visible_versions
            .insert(table_id, Arc::new(visible_ids));
    }
    Ok(())
}

fn reconcile_table_version_changes(
    before_rows: &[Row],
    before_ids: &[u32],
    after_rows: &[Row],
    versions: &mut Vec<VersionedRow>,
    context: VersionMutationContext,
) -> Result<Vec<u32>> {
    if before_rows.len() == after_rows.len() {
        return before_rows
            .iter()
            .zip(before_ids)
            .zip(after_rows)
            .map(|((before, version_id), after)| {
                if before == after {
                    Ok(*version_id)
                } else {
                    update_version(versions, *version_id, after, context)
                }
            })
            .collect();
    }
    if is_subsequence(before_rows, after_rows) {
        let mut before_index = 0_usize;
        let mut visible = Vec::with_capacity(after_rows.len());
        for row in after_rows {
            if before_index < before_rows.len() && row == &before_rows[before_index] {
                visible.push(before_ids[before_index]);
                before_index += 1;
            } else {
                visible.push(append_version(versions, row, 0, context)?);
            }
        }
        return Ok(visible);
    }
    if is_subsequence(after_rows, before_rows) {
        let mut before_index = 0_usize;
        let mut visible = Vec::with_capacity(after_rows.len());
        for row in after_rows {
            while before_index < before_rows.len() && &before_rows[before_index] != row {
                delete_version(versions, before_ids[before_index], context)?;
                before_index += 1;
            }
            if before_index == before_rows.len() {
                return Err(internal_error(
                    "row subsequence changed while deriving version deletes",
                ));
            }
            visible.push(before_ids[before_index]);
            before_index += 1;
        }
        for version_id in &before_ids[before_index..] {
            delete_version(versions, *version_id, context)?;
        }
        return Ok(visible);
    }

    let shared = before_rows.len().min(after_rows.len());
    let mut visible = Vec::with_capacity(after_rows.len());
    for index in 0..shared {
        if before_rows[index] == after_rows[index] {
            visible.push(before_ids[index]);
        } else {
            visible.push(update_version(
                versions,
                before_ids[index],
                &after_rows[index],
                context,
            )?);
        }
    }
    for version_id in &before_ids[shared..] {
        delete_version(versions, *version_id, context)?;
    }
    for row in &after_rows[shared..] {
        visible.push(append_version(versions, row, 0, context)?);
    }
    Ok(visible)
}

fn is_subsequence(needle: &[Row], haystack: &[Row]) -> bool {
    let mut index = 0_usize;
    for row in haystack {
        if index < needle.len() && row == &needle[index] {
            index += 1;
        }
    }
    index == needle.len()
}

fn update_version(
    versions: &mut Vec<VersionedRow>,
    previous_version: u32,
    row: &Row,
    context: VersionMutationContext,
) -> Result<u32> {
    delete_version(versions, previous_version, context)?;
    append_version(versions, row, previous_version, context)
}

fn delete_version(
    versions: &mut [VersionedRow],
    version_id: u32,
    context: VersionMutationContext,
) -> Result<()> {
    let version = version_id
        .checked_sub(1)
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| versions.get_mut(index))
        .ok_or_else(|| internal_error("visible version ID is outside its table version state"))?;
    if version.header.xmax != 0 {
        return Err(DbError::new(
            "40001",
            "tuple version changed since the transaction snapshot",
        )
        .with_hint("retry the transaction with a fresh snapshot"));
    }
    version.header.xmax = context.transaction_id.get();
    version.header.command_id = context.command_id;
    Ok(())
}

fn append_version(
    versions: &mut Vec<VersionedRow>,
    row: &Row,
    previous_version: u32,
    context: VersionMutationContext,
) -> Result<u32> {
    let version_id = u32::try_from(versions.len())
        .ok()
        .and_then(|version_id| version_id.checked_add(1))
        .ok_or_else(|| DbError::new("54000", "table version ordinal space is exhausted"))?;
    if previous_version >= version_id {
        return Err(internal_error(
            "new tuple predecessor is not earlier than its version ordinal",
        ));
    }
    let mut header = TupleHeaderV2::frozen(row)?;
    header.xmin = context.transaction_id.get();
    header.command_id = context.command_id;
    header.previous_version = previous_version;
    versions.push(VersionedRow {
        version_id,
        header,
        row: row.clone(),
    });
    Ok(version_id)
}

fn merge_dml_candidate(
    latest: &DatabaseState,
    base: &DatabaseState,
    candidate: &DatabaseState,
    transaction: &DurableTransaction,
    statuses: &TransactionStatusStore,
) -> Result<DatabaseState> {
    let transaction_id = transaction.transaction_id();
    let mut merged = latest.clone();
    let table_ids = base
        .versions
        .keys()
        .chain(candidate.versions.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for table_id in table_ids {
        let base_versions = base
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        let candidate_versions = candidate
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        if base_versions == candidate_versions
            && base.visible_versions.get(&table_id) == candidate.visible_versions.get(&table_id)
        {
            continue;
        }
        if candidate_versions.len() < base_versions.len() {
            return Err(internal_error(
                "DML candidate removed authoritative tuple versions",
            ));
        }
        let mut latest_versions = merged
            .versions
            .get(&table_id)
            .map(|versions| (**versions).clone())
            .ok_or_else(|| {
                DbError::new(
                    "40001",
                    format!("table {} changed during the transaction", table_id.get()),
                )
            })?;
        if latest_versions.len() < base_versions.len() {
            return Err(internal_error(
                "latest tuple-version state is shorter than the transaction base",
            ));
        }
        for (index, (base_version, candidate_version)) in
            base_versions.iter().zip(candidate_versions).enumerate()
        {
            if base_version == candidate_version {
                continue;
            }
            if base_version.row != candidate_version.row
                || base_version.version_id != candidate_version.version_id
                || base_version.header.xmax != 0
                || candidate_version.header.xmax != transaction_id.get()
            {
                return Err(internal_error(
                    "DML candidate changed an existing tuple outside its deletion header",
                ));
            }
            let latest_version = latest_versions
                .get_mut(index)
                .ok_or_else(|| internal_error("latest tuple version disappeared during merge"))?;
            if latest_version != base_version {
                return Err(DbError::new(
                    "40001",
                    "tuple version changed since the transaction snapshot",
                )
                .with_hint("retry the transaction with a fresh snapshot"));
            }
            latest_version.header = candidate_version.header;
        }
        let mut remapped = BTreeMap::<u32, u32>::new();
        for candidate_version in &candidate_versions[base_versions.len()..] {
            if candidate_version.header.xmin != transaction_id.get() {
                return Err(internal_error(
                    "DML candidate appended a version owned by another transaction",
                ));
            }
            let version_id = u32::try_from(latest_versions.len())
                .ok()
                .and_then(|version_id| version_id.checked_add(1))
                .ok_or_else(|| DbError::new("54000", "table version ordinal space is exhausted"))?;
            let mut appended = candidate_version.clone();
            let previous = appended.header.previous_version;
            if previous > u32::try_from(base_versions.len()).unwrap_or(u32::MAX) {
                appended.header.previous_version = *remapped
                    .get(&previous)
                    .ok_or_else(|| internal_error("DML candidate predecessor was not remapped"))?;
            }
            appended.version_id = version_id;
            remapped.insert(candidate_version.version_id, version_id);
            latest_versions.push(appended);
        }
        merged.versions.insert(table_id, Arc::new(latest_versions));
    }
    project_current_database_visibility(merged, transaction_id, statuses)
}

fn refresh_read_committed_candidate(
    state: &Arc<RwLock<DatabaseState>>,
    statuses: &TransactionStatusStore,
    transaction: &DurableTransaction,
    base: &mut Option<DatabaseState>,
    working: &mut Option<DatabaseState>,
    dml_only: bool,
) -> Result<()> {
    if !dml_only
        || transaction.characteristics().is_none_or(|characteristics| {
            characteristics.isolation_level != IsolationLevel::ReadCommitted
        })
        || working.is_none()
    {
        return Ok(());
    }
    let previous_base = base
        .as_ref()
        .ok_or_else(|| internal_error("DML transaction is missing its base snapshot"))?;
    let previous_working = working
        .as_ref()
        .ok_or_else(|| internal_error("DML transaction is missing its working state"))?;
    let transaction_snapshot = transaction
        .snapshot()
        .ok_or_else(|| no_active_transaction_error("refresh a statement snapshot"))?;
    let refreshed_base = project_database_visibility(
        committed_snapshot(state)?,
        transaction_snapshot,
        transaction.transaction_id(),
        statuses,
    )?;
    let refreshed_working = merge_dml_candidate(
        &refreshed_base,
        previous_base,
        previous_working,
        transaction,
        statuses,
    )?;
    *base = Some(refreshed_base);
    *working = Some(refreshed_working);
    Ok(())
}

struct DmlUpgradeAuthorities<'a> {
    state: &'a Arc<RwLock<DatabaseState>>,
    statuses: &'a TransactionStatusStore,
    locks: &'a Arc<LockManager>,
    writer: &'a Arc<WriterCoordinator>,
}

fn upgrade_dml_candidate_to_exclusive(
    authorities: DmlUpgradeAuthorities<'_>,
    transaction: &DurableTransaction,
    base: &DatabaseState,
    working: &DatabaseState,
    cancellation: Option<Arc<AtomicBool>>,
) -> Result<(DatabaseState, DatabaseState, WriterLease, LockGuard)> {
    let lease = authorities
        .writer
        .try_acquire(transaction.transaction_id())?;
    let lock =
        acquire_compatibility_write_lock(authorities.locks, transaction, cancellation.as_deref())?;
    let mut latest = committed_snapshot(authorities.state)?;
    latest.cancellation = cancellation;
    let characteristics = transaction
        .characteristics()
        .ok_or_else(|| no_active_transaction_error("upgrade transaction locks"))?;
    let upgraded = if characteristics.isolation_level == IsolationLevel::ReadCommitted {
        merge_dml_candidate(&latest, base, working, transaction, authorities.statuses)?
    } else {
        if latest.generation != base.generation {
            return Err(DbError::new(
                "40001",
                "could not serialize DDL after concurrent database changes",
            )
            .with_hint("retry the transaction"));
        }
        let mut working = working.clone();
        working.cancellation = latest.cancellation.clone();
        working
    };
    Ok((latest, upgraded, lease, lock))
}

fn project_current_database_visibility(
    state: DatabaseState,
    current_transaction: TransactionId,
    statuses: &TransactionStatusStore,
) -> Result<DatabaseState> {
    let status = statuses.snapshot()?;
    let xmax = TransactionId::new(status.next_transaction_id)
        .ok_or_else(|| internal_error("transaction status high-water mark is zero"))?;
    let snapshot = TransactionSnapshot {
        xmin: xmax,
        xmax,
        in_progress: Arc::new(BTreeSet::new()),
        command_id: u32::MAX,
    };
    project_database_visibility(state, &snapshot, current_transaction, statuses)
}

fn persist_candidate(
    state: &mut DatabaseState,
    store: &Arc<Mutex<DatabaseStore>>,
    storage_access: &Arc<StorageAccessGate>,
    wal: &Arc<WalManager>,
    transaction: &mut DurableTransaction,
    mut candidate: DatabaseState,
) -> Result<()> {
    candidate.generation = state.generation.checked_add(1).ok_or_else(|| {
        DbError::new("54000", "database generation space is exhausted")
            .with_hint("create a logical backup before retrying on a fresh database")
    })?;
    let persistent = PersistentState::from(&candidate);
    let _storage_write_lease = storage_access.acquire_write()?;
    let mut store = store
        .lock()
        .map_err(|_| internal_error("database store lock is poisoned"))?;
    let mut prepared = store.prepare_commit(&persistent)?;
    let transaction_id = transaction.transaction_id();
    let logged = wal.log_prepared(transaction_id, &mut prepared)?;
    transaction.mark_status_committed()?;
    store.apply_prepared_with_observer(&prepared, |point| {
        wal.check_fault(match point {
            ApplyPoint::BeforePageWrite(_) => FaultPoint::BeforeDataPageWrite,
            ApplyPoint::AfterPageWrite(_) => FaultPoint::AfterDataPageWrite,
            ApplyPoint::BeforeResize { .. } => FaultPoint::BeforeDataResize,
            ApplyPoint::AfterResize { .. } => FaultPoint::AfterDataResize,
            ApplyPoint::BeforeSync => FaultPoint::BeforeDataSync,
            ApplyPoint::AfterSync => FaultPoint::AfterDataSync,
        })
    })?;
    wal.commit(&logged)?;
    store.publish_prepared(prepared)?;
    transaction.finish_commit()?;
    *state = candidate;
    Ok(())
}

fn checkpoint_shared(
    state: &Arc<RwLock<DatabaseState>>,
    store: &Arc<Mutex<DatabaseStore>>,
    wal: &Arc<WalManager>,
    transactions: &Arc<TransactionManager>,
) -> Result<()> {
    let durable_data_generation = state
        .read()
        .map_err(|_| internal_error("engine state lock is poisoned"))?
        .generation;
    let data_file_page_count = store
        .lock()
        .map_err(|_| internal_error("database store lock is poisoned"))?
        .page_count()?;
    let mut active_transactions = BTreeMap::new();
    for transaction_id in transactions.active_transactions()? {
        if let Some(last_lsn) = wal.last_lsn(transaction_id)? {
            active_transactions.insert(transaction_id, last_lsn);
        }
    }
    wal.checkpoint(CheckpointState {
        active_transactions,
        dirty_pages: wal.dirty_pages()?,
        visibility_horizon: Some(transactions.global_xmin()?),
        durable_data_generation,
        durable_wal_lsn: wal.durable_lsn()?,
        data_file_page_count,
    })?;
    Ok(())
}

fn record_commit_and_maybe_checkpoint(
    state: &Arc<RwLock<DatabaseState>>,
    store: &Arc<Mutex<DatabaseStore>>,
    wal: &Arc<WalManager>,
    transactions: &Arc<TransactionManager>,
    commits_since_checkpoint: &AtomicU64,
) -> Result<()> {
    let count = commits_since_checkpoint
        .fetch_add(1, Ordering::AcqRel)
        .checked_add(1)
        .ok_or_else(|| DbError::new("54000", "automatic checkpoint commit counter overflowed"))?;
    if count < AUTOMATIC_CHECKPOINT_INTERVAL {
        return Ok(());
    }
    checkpoint_shared(state, store, wal, transactions)?;
    commits_since_checkpoint.store(0, Ordering::Release);
    Ok(())
}

fn transaction_events(tag: &str) -> Vec<QueryEvent> {
    command_events(Schema::empty(), tag, 0, None)
}

fn no_active_transaction_error(action: &str) -> DbError {
    DbError::new(
        "25P01",
        format!("cannot {action} because no transaction is active"),
    )
}

fn failed_transaction_error() -> DbError {
    DbError::new(
        "25P02",
        "the current transaction is aborted; commands are ignored until ROLLBACK",
    )
    .with_hint("issue ROLLBACK before starting new work")
}

fn execute_root_bound(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    maintenance: MaintenanceContext<'_>,
) -> Result<(Vec<QueryEvent>, bool)> {
    match statement {
        BoundStatement::Analyze { table_id } => execute_analyze(state, table_id),
        BoundStatement::Vacuum { table_id, analyze } => {
            execute_vacuum(state, table_id, analyze, maintenance)
        }
        statement => execute_bound(state, statement, params),
    }
}

fn execute_analyze(
    state: &mut DatabaseState,
    table_id: Option<TableId>,
) -> Result<(Vec<QueryEvent>, bool)> {
    let table_ids = maintenance_table_ids(state, table_id)?;
    for table_id in &table_ids {
        rebuild_table_derived(state, *table_id)?;
    }
    Ok((
        command_events(
            Schema::empty(),
            "ANALYZE",
            u64::try_from(table_ids.len()).unwrap_or(u64::MAX),
            None,
        ),
        true,
    ))
}

fn execute_vacuum(
    state: &mut DatabaseState,
    table_id: Option<TableId>,
    _analyze: bool,
    maintenance: MaintenanceContext<'_>,
) -> Result<(Vec<QueryEvent>, bool)> {
    if let Some(transaction_id) = maintenance.expired_snapshot {
        return Err(DbError::new(
            "55000",
            format!(
                "VACUUM cannot proceed while transaction {transaction_id} holds an expired snapshot"
            ),
        )
        .with_hint("commit or roll back the long-running transaction before retrying VACUUM"));
    }
    let table_ids = maintenance_table_ids(state, table_id)?;
    let mut reclaimed = 0_u64;
    for table_id in &table_ids {
        reclaimed = reclaimed
            .checked_add(vacuum_table_versions(state, *table_id, maintenance)?)
            .ok_or_else(|| DbError::new("54000", "VACUUM reclaimed-row count overflow"))?;
        rebuild_table_derived(state, *table_id)?;
    }
    Ok((
        command_events(Schema::empty(), "VACUUM", reclaimed, None),
        true,
    ))
}

fn maintenance_table_ids(state: &DatabaseState, table_id: Option<TableId>) -> Result<Vec<TableId>> {
    if let Some(table_id) = table_id {
        table_definition(state, table_id)?;
        return Ok(vec![table_id]);
    }
    Ok(state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .map(|table| table.id)
        .collect())
}

fn vacuum_table_versions(
    state: &mut DatabaseState,
    table_id: TableId,
    maintenance: MaintenanceContext<'_>,
) -> Result<u64> {
    let original = state
        .versions
        .get(&table_id)
        .map(|versions| (**versions).clone())
        .unwrap_or_default();
    let mut retained = Vec::with_capacity(original.len());
    let mut id_map = BTreeMap::new();
    for version in &original {
        if version_reclaimable(version, maintenance)? {
            continue;
        }
        let new_id = u32::try_from(retained.len())
            .ok()
            .and_then(|id| id.checked_add(1))
            .ok_or_else(|| DbError::new("54000", "table version ordinal space is exhausted"))?;
        let mut retained_version = version.clone();
        freeze_retained_version(&mut retained_version, maintenance)?;
        id_map.insert(version.version_id, new_id);
        retained.push((version.version_id, retained_version));
    }
    for (old_id, version) in &mut retained {
        let mut predecessor = version.header.previous_version;
        let mut traversed = 0_usize;
        while predecessor != 0 && !id_map.contains_key(&predecessor) {
            traversed = traversed
                .checked_add(1)
                .ok_or_else(|| DbError::new("54001", "tuple predecessor depth overflow"))?;
            if traversed > original.len() {
                return Err(DbError::new(
                    "XX001",
                    "tuple predecessor chain is cyclic during VACUUM",
                ));
            }
            predecessor = original
                .get(usize::try_from(predecessor.saturating_sub(1)).unwrap_or(usize::MAX))
                .ok_or_else(|| {
                    DbError::new(
                        "XX001",
                        "tuple predecessor points outside the version sequence",
                    )
                })?
                .header
                .previous_version;
        }
        version.version_id = *id_map
            .get(old_id)
            .ok_or_else(|| internal_error("retained tuple version was not remapped"))?;
        version.header.previous_version = if predecessor == 0 {
            0
        } else {
            *id_map
                .get(&predecessor)
                .ok_or_else(|| internal_error("retained tuple predecessor was not remapped"))?
        };
    }
    let visible = state
        .visible_versions
        .get(&table_id)
        .map_or(&[][..], |versions| versions.as_slice())
        .iter()
        .map(|old_id| {
            id_map.get(old_id).copied().ok_or_else(|| {
                internal_error("VACUUM attempted to reclaim a currently visible tuple")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let reclaimed = original.len().saturating_sub(retained.len());
    state.versions.insert(
        table_id,
        Arc::new(retained.into_iter().map(|(_, version)| version).collect()),
    );
    state.visible_versions.insert(table_id, Arc::new(visible));
    u64::try_from(reclaimed).map_err(|_| DbError::new("54000", "VACUUM count overflow"))
}

fn version_reclaimable(
    version: &VersionedRow,
    maintenance: MaintenanceContext<'_>,
) -> Result<bool> {
    let creator = TransactionId::new(version.header.xmin)
        .ok_or_else(|| DbError::new("XX001", "tuple creator transaction ID is zero"))?;
    let creator_outcome = if version.header.xmin == FROZEN_TRANSACTION_ID {
        TransactionOutcome::Committed
    } else {
        maintenance.statuses.transaction_outcome(creator)?
    };
    if creator < maintenance.horizon && creator_outcome == TransactionOutcome::Aborted {
        return Ok(true);
    }
    if creator_outcome != TransactionOutcome::Committed || version.header.xmax == 0 {
        return Ok(false);
    }
    let deleter = TransactionId::new(version.header.xmax)
        .ok_or_else(|| DbError::new("XX001", "tuple deleter transaction ID is zero"))?;
    Ok(deleter < maintenance.horizon
        && maintenance.statuses.transaction_outcome(deleter)? == TransactionOutcome::Committed)
}

fn freeze_retained_version(
    version: &mut VersionedRow,
    maintenance: MaintenanceContext<'_>,
) -> Result<()> {
    if version.header.xmin != FROZEN_TRANSACTION_ID {
        let creator = TransactionId::new(version.header.xmin)
            .ok_or_else(|| DbError::new("XX001", "tuple creator transaction ID is zero"))?;
        if creator < maintenance.horizon
            && maintenance.statuses.transaction_outcome(creator)? == TransactionOutcome::Committed
        {
            version.header.xmin = FROZEN_TRANSACTION_ID;
        }
    }
    let Some(deleter) = TransactionId::new(version.header.xmax) else {
        return Ok(());
    };
    if deleter < maintenance.horizon {
        match maintenance.statuses.transaction_outcome(deleter)? {
            TransactionOutcome::Aborted => version.header.xmax = 0,
            TransactionOutcome::Committed => {
                return Err(internal_error(
                    "VACUUM retained a tuple deleted before the safe horizon",
                ));
            }
            TransactionOutcome::InProgress => {}
        }
    }
    Ok(())
}

fn execute_bound(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    match statement {
        BoundStatement::NoOp { tag } => Ok((command_events(Schema::empty(), tag, 0, None), false)),
        BoundStatement::CreateSchema {
            name,
            if_not_exists,
        } => {
            if state.catalog.schema(&name).is_some() && if_not_exists {
                return Ok((
                    command_events(Schema::empty(), "CREATE SCHEMA", 0, None),
                    false,
                ));
            }
            Arc::make_mut(&mut state.catalog).create_schema(name)?;
            Ok((
                command_events(Schema::empty(), "CREATE SCHEMA", 0, None),
                true,
            ))
        }
        BoundStatement::AlterSchemaRename {
            schema_id,
            new_name,
        } => {
            Arc::make_mut(&mut state.catalog).rename_schema(schema_id, new_name)?;
            Ok((
                command_events(Schema::empty(), "ALTER SCHEMA", 0, None),
                true,
            ))
        }
        BoundStatement::DropObjects {
            kind,
            objects,
            behavior,
        } => execute_drop_objects(state, kind, objects, behavior),
        BoundStatement::CreateTable {
            schema,
            name,
            columns,
            constraints,
            if_not_exists,
        } => {
            if state.catalog.table(&schema, &name).is_some() && if_not_exists {
                return Ok((
                    command_events(Schema::empty(), "CREATE TABLE", 0, None),
                    false,
                ));
            }
            let table_id =
                Arc::make_mut(&mut state.catalog).create_table(&schema, name, columns)?;
            for constraint in constraints {
                Arc::make_mut(&mut state.catalog).create_constraint(table_id, constraint)?;
            }
            state.rows.insert(table_id, Arc::new(Vec::new()));
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "CREATE TABLE", 0, None),
                true,
            ))
        }
        BoundStatement::AlterTable {
            table_id,
            operations,
        } => execute_alter_table(state, table_id, operations),
        BoundStatement::CreateIndex {
            table_id,
            index,
            if_not_exists,
        } => {
            if table_definition(state, table_id)?
                .index(&index.name)
                .is_some()
                && if_not_exists
            {
                return Ok((
                    command_events(Schema::empty(), "CREATE INDEX", 0, None),
                    false,
                ));
            }
            Arc::make_mut(&mut state.catalog).create_index(table_id, index)?;
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "CREATE INDEX", 0, None),
                true,
            ))
        }
        BoundStatement::AlterIndexRename { index_id, new_name } => {
            Arc::make_mut(&mut state.catalog).rename_index(index_id, new_name)?;
            Ok((
                command_events(Schema::empty(), "ALTER INDEX", 0, None),
                true,
            ))
        }
        BoundStatement::CreateSequence {
            schema,
            sequence,
            if_not_exists,
        } => {
            if state.catalog.sequence(&schema, &sequence.name).is_some() && if_not_exists {
                return Ok((
                    command_events(Schema::empty(), "CREATE SEQUENCE", 0, None),
                    false,
                ));
            }
            Arc::make_mut(&mut state.catalog).create_sequence(&schema, sequence)?;
            Ok((
                command_events(Schema::empty(), "CREATE SEQUENCE", 0, None),
                true,
            ))
        }
        BoundStatement::AlterSequenceRename {
            sequence_id,
            new_name,
        } => {
            Arc::make_mut(&mut state.catalog).rename_sequence(sequence_id, new_name)?;
            Ok((
                command_events(Schema::empty(), "ALTER SEQUENCE", 0, None),
                true,
            ))
        }
        BoundStatement::AlterSequence {
            sequence_id,
            increment,
            min_value,
            max_value,
            restart,
            cycle,
            owner,
        } => {
            Arc::make_mut(&mut state.catalog).alter_sequence(
                sequence_id,
                SequenceAlteration {
                    increment,
                    min_value,
                    max_value,
                    restart,
                    cycle,
                    owner,
                },
            )?;
            Ok((
                command_events(Schema::empty(), "ALTER SEQUENCE", 0, None),
                true,
            ))
        }
        BoundStatement::CreateView {
            schema,
            name,
            kind,
            query,
            query_sql,
            output,
            references,
            replace,
            if_not_exists,
            with_data,
            existing,
        } => execute_create_view(
            state,
            CreateViewExecution {
                schema,
                name,
                kind,
                query: *query,
                query_sql,
                output,
                references,
                replace,
                if_not_exists,
                with_data,
                existing,
            },
            params,
        ),
        BoundStatement::AlterViewRename { view_id, new_name } => {
            let kind = state
                .catalog
                .view_by_id(view_id)
                .map(|view| view.kind)
                .ok_or_else(|| internal_error("view disappeared before rename"))?;
            Arc::make_mut(&mut state.catalog).rename_view(view_id, new_name)?;
            let tag = match kind {
                ordadb_catalog::ViewKind::Regular => "ALTER VIEW",
                ordadb_catalog::ViewKind::Materialized => "ALTER MATERIALIZED VIEW",
            };
            Ok((command_events(Schema::empty(), tag, 0, None), true))
        }
        BoundStatement::RefreshMaterializedView {
            view_id,
            table_id,
            query,
            with_data,
        } => {
            let rows = if with_data {
                materialize_statement_rows(state, *query, params)?
            } else {
                Vec::new()
            };
            state.rows.insert(table_id, Arc::new(rows));
            Arc::make_mut(&mut state.catalog)
                .set_materialized_view_populated(view_id, with_data)?;
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "REFRESH MATERIALIZED VIEW", 0, None),
                true,
            ))
        }
        BoundStatement::CreateRoutine {
            schema,
            name,
            kind,
            arguments,
            return_type,
            returns_set,
            language,
            body,
            replace,
        } => {
            let argument_names = routine_argument_names(&arguments);
            let compile_names =
                if kind == ordadb_catalog::RoutineKind::Function && return_type.is_none() {
                    vec!["old".to_owned(), "new".to_owned()]
                } else {
                    argument_names
                };
            compile_plpgsql(&body, &compile_names)?;
            let tag = match kind {
                ordadb_catalog::RoutineKind::Function => "CREATE FUNCTION",
                ordadb_catalog::RoutineKind::Procedure => "CREATE PROCEDURE",
            };
            Arc::make_mut(&mut state.catalog).create_or_replace_routine(
                &schema,
                NewRoutine {
                    name,
                    kind,
                    arguments,
                    return_type,
                    returns_set,
                    language,
                    body,
                    replace,
                    references: Vec::new(),
                },
            )?;
            Ok((command_events(Schema::empty(), tag, 0, None), true))
        }
        BoundStatement::DropRoutine {
            routine_id,
            behavior,
        } => {
            let kind = state
                .catalog
                .routine_by_id(routine_id)
                .map(|routine| routine.kind)
                .ok_or_else(|| DbError::new("42883", "routine does not exist"))?;
            let removed = Arc::make_mut(&mut state.catalog).drop_routine(routine_id, behavior)?;
            cleanup_removed_state(state, &removed);
            let tag = match kind {
                ordadb_catalog::RoutineKind::Function => "DROP FUNCTION",
                ordadb_catalog::RoutineKind::Procedure => "DROP PROCEDURE",
            };
            Ok((command_events(Schema::empty(), tag, 0, None), true))
        }
        BoundStatement::Call {
            routine_id,
            arguments,
        } => {
            execute_routine_program(state, routine_id, &arguments, params)?;
            Ok((command_events(Schema::empty(), "CALL", 0, None), true))
        }
        BoundStatement::RoutineSelect {
            routine_id,
            arguments,
            schema,
            returns_set,
        } => {
            let output = execute_routine_program(state, routine_id, &arguments, params)?;
            let values = if returns_set {
                output.returned_rows
            } else {
                vec![output.return_value.unwrap_or(Value::Null)]
            };
            let row_count = values.len() as u64;
            Ok((
                vec![
                    QueryEvent::Schema(schema.clone()),
                    QueryEvent::Batch(Batch {
                        schema,
                        rows: values
                            .into_iter()
                            .map(|value| Row::new(vec![value]))
                            .collect(),
                    }),
                    QueryEvent::Progress(QueryProgress {
                        rows_processed: row_count,
                    }),
                    QueryEvent::Complete(CommandComplete {
                        tag: format!("SELECT {row_count}"),
                        rows_affected: row_count,
                    }),
                ],
                false,
            ))
        }
        BoundStatement::SequenceValue {
            sequence_id,
            operation,
            schema,
        } => {
            let (value, dirty) = match operation {
                BoundSequenceOperation::NextValue => (
                    Arc::make_mut(&mut state.catalog).next_sequence_value(sequence_id)?,
                    true,
                ),
                BoundSequenceOperation::CurrentValue { value } => (
                    value.ok_or_else(|| {
                        DbError::new(
                            "55000",
                            "currval of sequence is not yet defined in this session",
                        )
                    })?,
                    false,
                ),
                BoundSequenceOperation::SetValue { value, is_called } => {
                    let value = evaluate_scalar(&value, &[], params)?;
                    let Value::Int64(value) = value else {
                        return Err(internal_error(
                            "bound setval expression did not produce BIGINT",
                        ));
                    };
                    Arc::make_mut(&mut state.catalog).set_sequence_value(
                        sequence_id,
                        value,
                        is_called,
                    )?;
                    (value, true)
                }
            };
            Ok((
                command_events(
                    schema.clone(),
                    "SELECT 1",
                    1,
                    Some(Batch {
                        schema,
                        rows: vec![Row::new(vec![Value::Int64(value)])],
                    }),
                ),
                dirty,
            ))
        }
        BoundStatement::CreateTrigger {
            table_id,
            name,
            timing,
            events,
            routine_id,
        } => {
            Arc::make_mut(&mut state.catalog).create_trigger(
                table_id,
                name,
                timing,
                events.into_iter().collect(),
                routine_id,
            )?;
            Ok((
                command_events(Schema::empty(), "CREATE TRIGGER", 0, None),
                true,
            ))
        }
        BoundStatement::DropTrigger {
            trigger_id,
            behavior,
        } => {
            let removed = Arc::make_mut(&mut state.catalog).drop_trigger(trigger_id, behavior)?;
            cleanup_removed_state(state, &removed);
            Ok((
                command_events(Schema::empty(), "DROP TRIGGER", 0, None),
                true,
            ))
        }
        BoundStatement::Insert {
            table_id,
            column_indexes,
            rows,
        } => execute_insert(state, table_id, column_indexes, rows, params),
        BoundStatement::Select {
            table_id,
            schema,
            projection,
            filter,
            order_by,
            limit,
        } => execute_select(
            state,
            SelectExecution {
                table_id,
                schema,
                projection,
                filter,
                order_by,
                limit,
            },
            params,
        ),
        BoundStatement::AdvancedSelect {
            table,
            joins,
            schema,
            projection,
            filter,
            group_by,
            having,
            order_by,
            limit,
            aggregate,
        } => execute_advanced_select(
            state,
            AdvancedExecution {
                table,
                joins,
                schema,
                projection,
                filter,
                group_by,
                having,
                order_by,
                limit,
                aggregate,
            },
            params,
        ),
        BoundStatement::ViewSelect {
            source,
            schema,
            projection,
            ..
        } => execute_view_select(state, *source, schema, projection, params),
        BoundStatement::Explain { statement } => execute_explain(state, *statement),
        BoundStatement::Update {
            table_id,
            assignments,
            filter,
        } => execute_update(state, table_id, assignments, filter, params),
        BoundStatement::Delete { table_id, filter } => {
            execute_delete(state, table_id, filter, params)
        }
        BoundStatement::Analyze { .. } | BoundStatement::Vacuum { .. } => Err(internal_error(
            "maintenance statement was not routed through the root executor",
        )),
        BoundStatement::Begin { .. }
        | BoundStatement::Commit { .. }
        | BoundStatement::Rollback { .. }
        | BoundStatement::Savepoint { .. }
        | BoundStatement::RollbackTo { .. }
        | BoundStatement::ReleaseSavepoint { .. } => Err(DbError::new(
            "25000",
            "transaction control was not routed through the session",
        )
        .with_hint("execute transaction control through Session")),
    }
}

fn routine_argument_names(arguments: &[ordadb_catalog::RoutineArgument]) -> Vec<String> {
    arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            argument
                .name
                .as_ref()
                .map_or_else(|| format!("__arg{}", index + 1), |name| name.to_string())
        })
        .collect()
}

fn execute_routine_program(
    state: &mut DatabaseState,
    routine_id: ordadb_types::RoutineId,
    arguments: &[BoundExpr],
    params: &[Value],
) -> Result<ordadb_plpgsql::VmOutput> {
    if state.routine_depth == 0 {
        return std::thread::scope(|scope| {
            let worker = std::thread::Builder::new()
                .name("ordadb-plpgsql".to_owned())
                .stack_size(PLPGSQL_EXECUTION_STACK_BYTES)
                .spawn_scoped(scope, || {
                    execute_routine_program_on_stack(state, routine_id, arguments, params)
                })
                .map_err(|error| {
                    DbError::new("58030", "failed to start the PL/pgSQL execution worker")
                        .with_detail(error.to_string())
                })?;
            worker
                .join()
                .map_err(|_| internal_error("PL/pgSQL execution worker terminated unexpectedly"))?
        });
    }
    execute_routine_program_on_stack(state, routine_id, arguments, params)
}

fn execute_routine_program_on_stack(
    state: &mut DatabaseState,
    routine_id: ordadb_types::RoutineId,
    arguments: &[BoundExpr],
    params: &[Value],
) -> Result<ordadb_plpgsql::VmOutput> {
    let routine = state
        .catalog
        .routine_by_id(routine_id)
        .cloned()
        .ok_or_else(|| DbError::new("42883", "routine does not exist"))?;
    let program = compile_plpgsql(&routine.body, &routine_argument_names(&routine.arguments))?;
    let values = arguments
        .iter()
        .map(|argument| evaluate_scalar(argument, &[], params))
        .collect::<Result<Vec<_>>>()?;
    if state.routine_depth >= 64 {
        return Err(DbError::new(
            "54001",
            "PL/pgSQL routine-call depth exceeds the maximum of 64",
        ));
    }
    state.routine_depth += 1;
    let result = {
        let mut host = EnginePlpgsqlHost {
            state,
            trigger: None,
            exception_state: None,
            exception_trigger: None,
        };
        execute_plpgsql(&program, &mut host, &values)
    };
    state.routine_depth = state.routine_depth.saturating_sub(1);
    let mut output = result?;
    if let Some(return_type) = &routine.return_type {
        output.return_value = output
            .return_value
            .map(|value| coerce_execution_value(value, return_type))
            .transpose()?;
        output.returned_rows = output
            .returned_rows
            .into_iter()
            .map(|value| coerce_execution_value(value, return_type))
            .collect::<Result<Vec<_>>>()?;
    }
    Ok(output)
}

#[derive(Debug, Clone)]
struct TriggerRowContext {
    table: TableDefinition,
    old: Option<Row>,
    new: Option<Row>,
}

impl TriggerRowContext {
    fn value_and_type(&self, slot: usize, field: &str) -> Result<(Value, ScalarType)> {
        let row = match slot {
            0 => self.old.as_ref(),
            1 => self.new.as_ref(),
            _ => {
                return Err(DbError::new(
                    "42P02",
                    format!("trigger record parameter ${} does not exist", slot + 1),
                ));
            }
        };
        let field = Identifier::unquoted(field);
        let (column_index, data_type) = self
            .table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == field)
            .map(|(index, column)| (index, column.data_type.clone()))
            .ok_or_else(|| {
                DbError::new(
                    "42703",
                    format!("trigger record field {field} does not exist"),
                )
            })?;
        let value = match row {
            Some(row) => row.values.get(column_index).cloned().ok_or_else(|| {
                internal_error("trigger record width does not match its table definition")
            })?,
            None => Value::Null,
        };
        Ok((value, data_type))
    }

    fn value(&self, slot: usize, field: &str) -> Result<Value> {
        self.value_and_type(slot, field).map(|(value, _)| value)
    }

    fn assign(&mut self, slot: usize, field: &str, value: Value) -> Result<()> {
        if slot != 1 {
            return Err(DbError::new("25006", "OLD is read-only in a row trigger"));
        }
        let field = Identifier::unquoted(field);
        let (column_index, data_type) = self
            .table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == field)
            .map(|(index, column)| (index, column.data_type.clone()))
            .ok_or_else(|| {
                DbError::new(
                    "42703",
                    format!("trigger record field {field} does not exist"),
                )
            })?;
        let row = self
            .new
            .as_mut()
            .ok_or_else(|| DbError::new("55000", "NEW is not available for this trigger event"))?;
        let target = row.values.get_mut(column_index).ok_or_else(|| {
            internal_error("trigger record width does not match its table definition")
        })?;
        *target = coerce_execution_value(value, &data_type)?;
        Ok(())
    }
}

enum RowTriggerOutcome {
    Proceed(Option<Row>),
    Suppress,
}

fn fire_row_triggers_with_rows(
    state: &mut DatabaseState,
    table_id: TableId,
    timing: TriggerTiming,
    event: TriggerEvent,
    old: Option<&Row>,
    new: Option<&Row>,
) -> Result<RowTriggerOutcome> {
    let table = table_definition(state, table_id)?.clone();
    let triggers = table
        .triggers()
        .filter(|trigger| {
            trigger.enabled && trigger.timing == timing && trigger.events.contains(&event)
        })
        .cloned()
        .collect::<Vec<_>>();
    for trigger in triggers {
        if state.trigger_depth >= 64 || state.triggers_fired >= 16_384 {
            return Err(DbError::new(
                "54001",
                "trigger recursion or fired-trigger limit exceeded",
            ));
        }
        let routine = state
            .catalog
            .routine_by_id(trigger.routine_id)
            .cloned()
            .ok_or_else(|| DbError::new("42883", "trigger routine does not exist"))?;
        let program = compile_plpgsql(&routine.body, &["old".into(), "new".into()])?;
        state.trigger_depth += 1;
        state.triggers_fired += 1;
        let mut trigger = TriggerRowContext {
            table: table.clone(),
            old: old.cloned(),
            new: new.cloned(),
        };
        let result = {
            let mut host = EnginePlpgsqlHost {
                state,
                trigger: Some(&mut trigger),
                exception_state: None,
                exception_trigger: None,
            };
            execute_plpgsql(&program, &mut host, &[Value::Null, Value::Null])
        };
        state.trigger_depth = state.trigger_depth.saturating_sub(1);
        let output = result?;
        if timing == TriggerTiming::After {
            continue;
        }
        match output.return_parameter {
            Some(0) => {
                if event != TriggerEvent::Delete {
                    return Ok(match trigger.old {
                        Some(row) => RowTriggerOutcome::Proceed(Some(row)),
                        None => RowTriggerOutcome::Suppress,
                    });
                }
            }
            Some(1) => {
                if event == TriggerEvent::Delete {
                    if trigger.new.is_none() {
                        return Ok(RowTriggerOutcome::Suppress);
                    }
                } else {
                    return Ok(match trigger.new {
                        Some(row) => RowTriggerOutcome::Proceed(Some(row)),
                        None => RowTriggerOutcome::Suppress,
                    });
                }
            }
            Some(parameter) => {
                return Err(DbError::new(
                    "42P02",
                    format!(
                        "trigger function returned unknown record parameter ${}",
                        parameter + 1
                    ),
                ));
            }
            None if output.return_value.is_none()
                || output.return_value.as_ref().is_some_and(Value::is_null) =>
            {
                return Ok(RowTriggerOutcome::Suppress);
            }
            None => {
                return Err(DbError::new(
                    "42804",
                    "row trigger functions must return OLD, NEW, or NULL",
                ));
            }
        }
    }
    Ok(RowTriggerOutcome::Proceed(new.cloned()))
}

fn execute_view_select(
    state: &mut DatabaseState,
    source: BoundStatement,
    schema: Schema,
    projection: Vec<usize>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let (mut events, dirty) = execute_bound(state, source, params)?;
    if dirty {
        return Err(internal_error(
            "a stored view query attempted to mutate state",
        ));
    }
    for event in &mut events {
        match event {
            QueryEvent::Schema(event_schema) => *event_schema = schema.clone(),
            QueryEvent::Batch(batch) => {
                for row in &mut batch.rows {
                    row.values = projection
                        .iter()
                        .map(|position| {
                            row.values.get(*position).cloned().ok_or_else(|| {
                                internal_error("stored view projection is outside its row width")
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                }
            }
            QueryEvent::Progress(_) | QueryEvent::Notice(_) | QueryEvent::Complete(_) => {}
        }
    }
    Ok((events, false))
}

fn execute_drop_objects(
    state: &mut DatabaseState,
    kind: DdlObjectKind,
    objects: Vec<CatalogObjectRef>,
    behavior: DropBehavior,
) -> Result<(Vec<QueryEvent>, bool)> {
    if objects.is_empty() {
        return Ok((
            command_events(Schema::empty(), drop_command_tag(kind), 0, None),
            false,
        ));
    }
    let catalog_before = Arc::clone(&state.catalog);
    let mut removed = Vec::new();
    for object in objects {
        let dropped = drop_catalog_root(Arc::make_mut(&mut state.catalog), object, behavior)?;
        for object in dropped {
            if !removed.contains(&object) {
                removed.push(object);
            }
        }
    }

    let backing_tables = removed
        .iter()
        .filter_map(|object| match object {
            CatalogObjectRef::View(view_id) => catalog_before
                .view_by_id(*view_id)
                .and_then(|view| view.materialized_table_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    for table_id in backing_tables {
        if state.catalog.table_by_id(table_id).is_some() {
            for object in
                Arc::make_mut(&mut state.catalog).drop_table(table_id, DropBehavior::Cascade)?
            {
                if !removed.contains(&object) {
                    removed.push(object);
                }
            }
        }
    }
    cleanup_removed_state(state, &removed);
    reconcile_search_catalog(state)?;
    Ok((
        command_events(Schema::empty(), drop_command_tag(kind), 0, None),
        true,
    ))
}

fn drop_catalog_root(
    catalog: &mut Catalog,
    object: CatalogObjectRef,
    behavior: DropBehavior,
) -> Result<Vec<CatalogObjectRef>> {
    match object {
        CatalogObjectRef::Schema(id) if catalog.schema_by_id(id).is_some() => {
            catalog.drop_schema(id, behavior)
        }
        CatalogObjectRef::Table(id) if catalog.table_by_id(id).is_some() => {
            catalog.drop_table(id, behavior)
        }
        CatalogObjectRef::Index(id) if catalog.index_by_id(id).is_some() => {
            catalog.drop_index(id, behavior)
        }
        CatalogObjectRef::Sequence(id) if catalog.sequence_by_id(id).is_some() => {
            catalog.drop_sequence(id, behavior)
        }
        CatalogObjectRef::View(id) if catalog.view_by_id(id).is_some() => {
            catalog.drop_view(id, behavior)
        }
        CatalogObjectRef::Constraint(id) if catalog.constraint_by_id(id).is_some() => {
            catalog.drop_constraint(id, behavior)
        }
        CatalogObjectRef::Routine(id) if catalog.routine_by_id(id).is_some() => {
            catalog.drop_routine(id, behavior)
        }
        CatalogObjectRef::Trigger(id) if catalog.trigger_by_id(id).is_some() => {
            catalog.drop_trigger(id, behavior)
        }
        CatalogObjectRef::Column(_, _) => Err(internal_error(
            "column drops must be routed through ALTER TABLE",
        )),
        _ => Ok(Vec::new()),
    }
}

fn cleanup_removed_state(state: &mut DatabaseState, removed: &[CatalogObjectRef]) {
    for object in removed {
        match object {
            CatalogObjectRef::Table(table_id) => {
                state.rows.remove(table_id);
            }
            CatalogObjectRef::Index(index_id) => {
                state.indexes.remove(index_id);
            }
            _ => {}
        }
    }
    state
        .rows
        .retain(|table_id, _| state.catalog.table_by_id(*table_id).is_some());
    state
        .indexes
        .retain(|index_id, _| state.catalog.index_by_id(*index_id).is_some());
}

fn drop_command_tag(kind: DdlObjectKind) -> &'static str {
    match kind {
        DdlObjectKind::Schema => "DROP SCHEMA",
        DdlObjectKind::Table => "DROP TABLE",
        DdlObjectKind::Index => "DROP INDEX",
        DdlObjectKind::Sequence => "DROP SEQUENCE",
        DdlObjectKind::View => "DROP VIEW",
        DdlObjectKind::MaterializedView => "DROP MATERIALIZED VIEW",
    }
}

fn execute_alter_table(
    state: &mut DatabaseState,
    table_id: TableId,
    operations: Vec<BoundAlterTableOperation>,
) -> Result<(Vec<QueryEvent>, bool)> {
    for operation in operations {
        match operation {
            BoundAlterTableOperation::RenameTable { new_name } => {
                Arc::make_mut(&mut state.catalog).rename_table(table_id, new_name)?;
            }
            BoundAlterTableOperation::RenameColumn {
                column_id,
                new_name,
            } => {
                Arc::make_mut(&mut state.catalog).rename_column(table_id, column_id, new_name)?;
            }
            BoundAlterTableOperation::AddColumn {
                column,
                if_not_exists,
            } => {
                if table_definition(state, table_id)?
                    .column(&column.name)
                    .is_some()
                    && if_not_exists
                {
                    continue;
                }
                let value = catalog_default_value(column.default.as_ref(), &column.data_type)?;
                Arc::make_mut(&mut state.catalog).add_column(table_id, column)?;
                for row in Arc::make_mut(
                    state
                        .rows
                        .entry(table_id)
                        .or_insert_with(|| Arc::new(Vec::new())),
                ) {
                    row.values.push(value.clone());
                }
            }
            BoundAlterTableOperation::DropColumns {
                column_ids,
                if_exists: _,
                behavior,
            } => {
                let table = table_definition(state, table_id)?.clone();
                let mut positions = column_ids
                    .iter()
                    .filter_map(|column_id| table.column_index_by_id(*column_id))
                    .collect::<Vec<_>>();
                for column_id in column_ids {
                    if state
                        .catalog
                        .table_by_id(table_id)
                        .is_some_and(|table| table.column_index_by_id(column_id).is_some())
                    {
                        Arc::make_mut(&mut state.catalog)
                            .drop_column(table_id, column_id, behavior)?;
                    }
                }
                positions.sort_unstable_by(|left, right| right.cmp(left));
                for row in Arc::make_mut(
                    state
                        .rows
                        .entry(table_id)
                        .or_insert_with(|| Arc::new(Vec::new())),
                ) {
                    for position in &positions {
                        row.values.remove(*position);
                    }
                }
            }
            BoundAlterTableOperation::SetNotNull { column_id } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    Some(false),
                    None,
                )?;
            }
            BoundAlterTableOperation::DropNotNull { column_id } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    Some(true),
                    None,
                )?;
            }
            BoundAlterTableOperation::SetDefault { column_id, default } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    None,
                    Some(Some(default)),
                )?;
            }
            BoundAlterTableOperation::DropDefault { column_id } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    None,
                    Some(None),
                )?;
            }
            BoundAlterTableOperation::SetDataType {
                column_id,
                data_type,
            } => {
                let position = table_definition(state, table_id)?
                    .column_index_by_id(column_id)
                    .ok_or_else(|| DbError::new("42703", "column does not exist"))?;
                for row in Arc::make_mut(
                    state
                        .rows
                        .entry(table_id)
                        .or_insert_with(|| Arc::new(Vec::new())),
                ) {
                    row.values[position] =
                        coerce_execution_value(row.values[position].clone(), &data_type)?;
                }
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    Some(data_type),
                    None,
                    None,
                )?;
            }
            BoundAlterTableOperation::AddConstraint { constraint } => {
                Arc::make_mut(&mut state.catalog).create_constraint(table_id, constraint)?;
            }
            BoundAlterTableOperation::DropConstraint {
                constraint_id,
                if_exists: _,
                behavior,
            } => {
                if let Some(constraint_id) = constraint_id {
                    let removed = Arc::make_mut(&mut state.catalog)
                        .drop_constraint(constraint_id, behavior)?;
                    cleanup_removed_state(state, &removed);
                }
            }
            BoundAlterTableOperation::SetTriggerEnabled {
                trigger_id,
                name,
                enabled,
            } => {
                let trigger_id = trigger_id.ok_or_else(|| {
                    DbError::new("42704", format!("trigger {name} does not exist"))
                })?;
                Arc::make_mut(&mut state.catalog).set_trigger_enabled(trigger_id, enabled)?;
            }
        }
    }
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    Ok((
        command_events(Schema::empty(), "ALTER TABLE", 0, None),
        true,
    ))
}

fn catalog_default_value(
    expression: Option<&CatalogExpression>,
    data_type: &ScalarType,
) -> Result<Value> {
    let Some(expression) = expression else {
        return Ok(Value::Null);
    };
    let bound = bind_catalog_expression(expression, None, Some(data_type))?;
    evaluate_scalar(&bound, &[], &[])
}

fn execute_create_view(
    state: &mut DatabaseState,
    view: CreateViewExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let tag = match view.kind {
        ViewKind::Regular => "CREATE VIEW",
        ViewKind::Materialized => "CREATE MATERIALIZED VIEW",
    };
    if view.existing.is_some() && view.if_not_exists && !view.replace {
        return Ok((command_events(Schema::empty(), tag, 0, None), false));
    }
    let materialized_rows = if view.kind == ViewKind::Materialized && view.with_data {
        Some(materialize_statement_rows(
            state,
            view.query.clone(),
            params,
        )?)
    } else {
        None
    };

    if let Some(view_id) = view.existing {
        let current = state
            .catalog
            .view_by_id(view_id)
            .cloned()
            .ok_or_else(|| DbError::new("42P01", "view does not exist"))?;
        if current.kind != view.kind {
            return Err(DbError::new(
                "42809",
                "cannot replace a view with a different relation kind",
            ));
        }
        Arc::make_mut(&mut state.catalog).replace_view(
            view_id,
            view.query_sql,
            view.output,
            view.kind == ViewKind::Regular || view.with_data,
            view.references,
        )?;
        if let Some(table_id) = current.materialized_table_id {
            state
                .rows
                .insert(table_id, Arc::new(materialized_rows.unwrap_or_default()));
            rebuild_table_derived(state, table_id)?;
        }
        return Ok((command_events(Schema::empty(), tag, 0, None), true));
    }

    let materialized_table_id = if view.kind == ViewKind::Materialized {
        let backing_name = Identifier::unquoted(format!("__ordadb_mv_{}", view.name.as_str()));
        let columns = view
            .output
            .fields
            .iter()
            .map(|field| NewColumn {
                name: Identifier::unquoted(field.name.clone()),
                data_type: field.data_type.clone(),
                nullable: field.nullable,
                primary_key: false,
                unique: false,
                default: None,
            })
            .collect();
        let table_id =
            Arc::make_mut(&mut state.catalog).create_table(&view.schema, backing_name, columns)?;
        state
            .rows
            .insert(table_id, Arc::new(materialized_rows.unwrap_or_default()));
        rebuild_table_derived(state, table_id)?;
        Some(table_id)
    } else {
        None
    };
    Arc::make_mut(&mut state.catalog).create_view(
        &view.schema,
        NewView {
            name: view.name,
            kind: view.kind,
            query: view.query_sql,
            output: view.output,
            materialized_table_id,
            populated: view.kind == ViewKind::Regular || view.with_data,
            references: view.references,
        },
    )?;
    Ok((command_events(Schema::empty(), tag, 0, None), true))
}

fn materialize_statement_rows(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
) -> Result<Vec<Row>> {
    let (events, dirty) = execute_bound(state, statement, params)?;
    if dirty {
        return Err(internal_error(
            "a materialized query attempted to mutate database state",
        ));
    }
    Ok(events
        .into_iter()
        .filter_map(|event| match event {
            QueryEvent::Batch(batch) => Some(batch.rows),
            _ => None,
        })
        .flatten()
        .collect())
}

struct EnginePlpgsqlHost<'a> {
    state: &'a mut DatabaseState,
    trigger: Option<&'a mut TriggerRowContext>,
    exception_state: Option<DatabaseState>,
    exception_trigger: Option<TriggerRowContext>,
}

impl PlpgsqlHost for EnginePlpgsqlHost<'_> {
    fn execute_sql(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>> + '_>> {
        let (sql, parameters, _) =
            expand_trigger_record_fields(sql, parameters, self.trigger.as_deref())?;
        let statement = bind(parse(&sql)?, &self.state.catalog)?;
        if matches!(
            statement,
            BoundStatement::Begin { .. }
                | BoundStatement::Commit { .. }
                | BoundStatement::Rollback { .. }
                | BoundStatement::Savepoint { .. }
                | BoundStatement::RollbackTo { .. }
                | BoundStatement::ReleaseSavepoint { .. }
        ) {
            return Err(DbError::new(
                "0A000",
                "transaction control is not allowed inside PL/pgSQL routines",
            ));
        }
        let (events, _) = execute_bound(self.state, statement, &parameters)?;
        Ok(Box::new(events.into_iter().map(Ok)))
    }

    fn evaluate_expression(&mut self, sql: &str, parameters: &[Value]) -> Result<Value> {
        if let Some(trigger) = self.trigger.as_deref()
            && let Some((slot, field)) = trigger_field_reference(sql)
        {
            return trigger.value(slot, field);
        }
        if let Some(index) = sql
            .trim()
            .strip_prefix('$')
            .and_then(|index| index.parse::<usize>().ok())
        {
            return parameters
                .get(index.saturating_sub(1))
                .cloned()
                .ok_or_else(|| DbError::new("42P02", format!("there is no parameter ${index}")));
        }
        let (sql, parameters, mut parameter_types) =
            expand_trigger_record_fields(sql, parameters, self.trigger.as_deref())?;
        for (index, value) in parameters.iter().enumerate() {
            if let Some(data_type) = scalar_type_of_value(value) {
                parameter_types.entry(index + 1).or_insert(data_type);
            }
        }
        let expression = CatalogExpression::new(sql);
        let bound = bind_catalog_expression_with_parameter_types(
            &expression,
            None,
            None,
            &parameter_types,
        )?;
        evaluate_scalar(&bound, &[], &parameters)
    }

    fn assign_composite_field(&mut self, slot: usize, field: &str, value: Value) -> Result<()> {
        self.trigger
            .as_deref_mut()
            .ok_or_else(|| {
                DbError::new(
                    "0A000",
                    "composite assignment is only available in row triggers",
                )
            })?
            .assign(slot, field, value)
    }

    fn begin_exception_block(&mut self) -> Result<()> {
        if self.exception_state.is_some() {
            return Err(internal_error(
                "PL/pgSQL exception savepoint is already active",
            ));
        }
        self.exception_state = Some(self.state.clone());
        self.exception_trigger = self.trigger.as_deref().cloned();
        Ok(())
    }

    fn commit_exception_block(&mut self) -> Result<()> {
        self.exception_state = None;
        self.exception_trigger = None;
        Ok(())
    }

    fn rollback_exception_block(&mut self) -> Result<()> {
        let saved = self
            .exception_state
            .take()
            .ok_or_else(|| internal_error("PL/pgSQL exception savepoint is not active"))?;
        *self.state = saved;
        if let Some(trigger) = self.trigger.as_deref_mut()
            && let Some(saved) = self.exception_trigger.take()
        {
            *trigger = saved;
        }
        Ok(())
    }

    fn check_cancelled(&self) -> Result<()> {
        if self
            .state
            .cancellation
            .as_ref()
            .is_some_and(|cancellation| cancellation.load(Ordering::Acquire))
        {
            Err(DbError::new("57014", "query was cancelled"))
        } else {
            Ok(())
        }
    }
}

fn scalar_type_of_value(value: &Value) -> Option<ScalarType> {
    match value {
        Value::Null => None,
        Value::Boolean(_) => Some(ScalarType::Boolean),
        Value::Int16(_) => Some(ScalarType::Int16),
        Value::Int32(_) => Some(ScalarType::Int32),
        Value::Int64(_) => Some(ScalarType::Int64),
        Value::Float32(_) => Some(ScalarType::Float32),
        Value::Float64(_) => Some(ScalarType::Float64),
        Value::Decimal(_) => Some(ScalarType::Decimal {
            precision: None,
            scale: None,
        }),
        Value::Text(_) => Some(ScalarType::Text),
        Value::Binary(_) => Some(ScalarType::Binary),
        Value::Date(_) => Some(ScalarType::Date),
        Value::Time(_) => Some(ScalarType::Time),
        Value::Timestamp(_) => Some(ScalarType::Timestamp {
            with_timezone: false,
        }),
        Value::Json(_) => Some(ScalarType::Json),
        Value::Jsonb(_) => Some(ScalarType::Jsonb),
        Value::Uuid(_) => Some(ScalarType::Uuid),
        Value::Vector(values) => Some(ScalarType::Vector {
            dimensions: Some(values.len()),
        }),
    }
}

fn trigger_field_reference(expression: &str) -> Option<(usize, &str)> {
    let (parameter, field) = expression.trim().strip_prefix('$')?.split_once('.')?;
    if field.is_empty()
        || !field
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '_')
    {
        return None;
    }
    Some((parameter.parse::<usize>().ok()?.checked_sub(1)?, field))
}

fn expand_trigger_record_fields(
    sql: &str,
    parameters: &[Value],
    trigger: Option<&TriggerRowContext>,
) -> Result<(String, Vec<Value>, BTreeMap<usize, ScalarType>)> {
    let Some(trigger) = trigger else {
        return Ok((sql.to_owned(), parameters.to_vec(), BTreeMap::new()));
    };
    let characters = sql.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(sql.len());
    let mut expanded = parameters.to_vec();
    let mut parameter_types = BTreeMap::new();
    let mut quote = None;
    let mut index = 0usize;
    while index < characters.len() {
        let character = characters[index];
        if let Some(delimiter) = quote {
            output.push(character);
            if character == delimiter {
                if characters.get(index + 1) == Some(&delimiter) {
                    output.push(delimiter);
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character);
            output.push(character);
            index += 1;
            continue;
        }
        if character != '$' {
            output.push(character);
            index += 1;
            continue;
        }
        let digits_start = index + 1;
        let mut cursor = digits_start;
        while characters.get(cursor).is_some_and(char::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == digits_start || characters.get(cursor) != Some(&'.') {
            output.push(character);
            index += 1;
            continue;
        }
        let field_start = cursor + 1;
        cursor = field_start;
        while characters
            .get(cursor)
            .is_some_and(|value| value.is_ascii_alphanumeric() || *value == '_')
        {
            cursor += 1;
        }
        if cursor == field_start {
            return Err(DbError::new(
                "42601",
                "trigger record access requires an unquoted field name",
            ));
        }
        let parameter = characters[digits_start..field_start - 1]
            .iter()
            .collect::<String>()
            .parse::<usize>()
            .map_err(|_| DbError::new("42P02", "invalid trigger record parameter"))?
            .checked_sub(1)
            .ok_or_else(|| DbError::new("42P02", "trigger parameters are one-based"))?;
        let field = characters[field_start..cursor].iter().collect::<String>();
        let (value, data_type) = trigger.value_and_type(parameter, &field)?;
        expanded.push(value);
        parameter_types.insert(expanded.len(), data_type);
        output.push('$');
        output.push_str(&expanded.len().to_string());
        index = cursor;
    }
    Ok((output, expanded, parameter_types))
}

fn execute_insert(
    state: &mut DatabaseState,
    table_id: TableId,
    column_indexes: Vec<usize>,
    expressions: Vec<Vec<BoundExpr>>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let table = table_definition(state, table_id)?.clone();
    let mut inserted = 0u64;
    for expressions in expressions {
        let mut values = table
            .columns()
            .iter()
            .map(|column| catalog_default_value(column.default.as_ref(), &column.data_type))
            .collect::<Result<Vec<_>>>()?;
        for (expression, column_index) in expressions.into_iter().zip(&column_indexes) {
            values[*column_index] = evaluate_scalar(&expression, &[], params)?;
        }
        let proposed = Row::new(values);
        let inserted_row = match fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::Before,
            TriggerEvent::Insert,
            None,
            Some(&proposed),
        )? {
            RowTriggerOutcome::Proceed(Some(row)) => row,
            RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
        };
        Arc::make_mut(
            state
                .rows
                .entry(table_id)
                .or_insert_with(|| Arc::new(Vec::new())),
        )
        .push(inserted_row.clone());
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        let _ = fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::After,
            TriggerEvent::Insert,
            None,
            Some(&inserted_row),
        )?;
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        inserted = inserted.saturating_add(1);
    }
    Ok((
        command_events(
            Schema::empty(),
            format!("INSERT 0 {inserted}"),
            inserted,
            None,
        ),
        true,
    ))
}

fn execute_select(
    state: &DatabaseState,
    execution: SelectExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let (schema, mut cursor) = prepare_select_cursor(state, execution, params, None)?;
    let mut events = vec![QueryEvent::Schema(schema.clone())];
    let mut count = 0_u64;
    let mut emitted_batch = false;
    while let Some(batch) = cursor.next_batch()? {
        count = count.saturating_add(batch.rows.len() as u64);
        emitted_batch = true;
        events.push(QueryEvent::Batch(batch));
    }
    if !emitted_batch {
        events.push(QueryEvent::Batch(Batch {
            schema,
            rows: Vec::new(),
        }));
    }
    events.push(QueryEvent::Progress(QueryProgress {
        rows_processed: count,
    }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: format!("SELECT {count}"),
        rows_affected: count,
    }));
    Ok((events, false))
}

fn prepare_select_cursor(
    state: &DatabaseState,
    execution: SelectExecution,
    params: &[Value],
    table_provider: Option<&dyn TableProvider>,
) -> Result<(Schema, ExecutionCursor)> {
    let SelectExecution {
        table_id,
        schema,
        projection,
        filter,
        order_by,
        limit,
    } = execution;
    let plan = optimize_select(
        table_definition(state, table_id)?,
        projection,
        filter,
        order_by,
        limit,
    );
    let context = ExecutionContext {
        tables: &state.rows,
        indexes: &state.indexes,
        params,
    };
    let cursor = match table_provider {
        Some(table_provider) => ExecutionCursor::new_with_table_provider(
            &plan,
            &context,
            schema.clone(),
            table_provider,
        )?,
        None => ExecutionCursor::new(&plan, &context, schema.clone())?,
    };
    Ok((schema, cursor))
}

fn execute_advanced_select(
    state: &DatabaseState,
    execution: AdvancedExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let (schema, mut cursor) = prepare_advanced_cursor(state, execution, params)?;
    let mut events = vec![QueryEvent::Schema(schema.clone())];
    let mut count = 0_u64;
    let mut emitted_batch = false;
    while let Some(batch) = cursor.next_batch()? {
        count = count.saturating_add(batch.rows.len() as u64);
        emitted_batch = true;
        events.push(QueryEvent::Batch(batch));
    }
    if !emitted_batch {
        events.push(QueryEvent::Batch(Batch {
            schema,
            rows: Vec::new(),
        }));
    }
    events.push(QueryEvent::Progress(QueryProgress {
        rows_processed: count,
    }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: format!("SELECT {count}"),
        rows_affected: count,
    }));
    Ok((events, false))
}

fn prepare_advanced_cursor(
    state: &DatabaseState,
    execution: AdvancedExecution,
    params: &[Value],
) -> Result<(Schema, AdvancedExecutionCursor)> {
    let AdvancedExecution {
        table,
        joins,
        schema,
        projection,
        filter,
        group_by,
        having,
        order_by,
        limit,
        aggregate,
    } = execution;
    let context = ExecutionContext {
        tables: &state.rows,
        indexes: &state.indexes,
        params,
    };
    let cursor = AdvancedExecutionCursor::new(
        AdvancedExecutionPlan {
            table,
            joins,
            schema: schema.clone(),
            projection,
            filter,
            group_by,
            having,
            order_by,
            limit,
            aggregate,
        },
        &context,
    )?;
    Ok((schema, cursor))
}

fn equi_join_columns(expr: &BoundExpr, right_offset: usize) -> Option<(usize, usize)> {
    let BoundExprKind::Binary {
        left,
        op: BinaryOperator::Eq,
        right,
    } = &expr.kind
    else {
        return None;
    };
    let (BoundExprKind::Column { index: left_index }, BoundExprKind::Column { index: right_index }) =
        (&left.kind, &right.kind)
    else {
        return None;
    };
    if *left_index < right_offset && *right_index >= right_offset {
        Some((*left_index, *right_index))
    } else if *right_index < right_offset && *left_index >= right_offset {
        Some((*right_index, *left_index))
    } else {
        None
    }
}

fn execute_explain(
    state: &DatabaseState,
    statement: BoundStatement,
) -> Result<(Vec<QueryEvent>, bool)> {
    let lines = match statement {
        BoundStatement::Select {
            table_id,
            projection,
            filter,
            order_by,
            limit,
            ..
        } => explain_plan(&optimize_select(
            table_definition(state, table_id)?,
            projection,
            filter,
            order_by,
            limit,
        )),
        BoundStatement::AdvancedSelect {
            table,
            joins,
            filter,
            aggregate,
            ..
        } => explain_advanced(state, &table, &joins, filter.is_some(), aggregate)?,
        _ => {
            return Err(DbError::new(
                "0A000",
                "EXPLAIN supports SELECT statements only",
            ));
        }
    };
    let schema = Schema::new(vec![Field::new("QUERY PLAN", ScalarType::Text, false)]);
    let count = lines.len() as u64;
    let batch = Batch {
        schema: schema.clone(),
        rows: lines
            .into_iter()
            .map(|line| Row::new(vec![Value::Text(line)]))
            .collect(),
    };
    Ok((
        command_events(schema, format!("EXPLAIN {count}"), count, Some(batch)),
        false,
    ))
}

fn explain_advanced(
    state: &DatabaseState,
    table: &BoundTable,
    joins: &[BoundJoin],
    filtered: bool,
    aggregate: bool,
) -> Result<Vec<String>> {
    let base = table_definition(state, table.table_id)?;
    let mut estimated_rows = base.statistics().row_count;
    let mut lines = vec!["Projection  (cost=0.00 rows=1)".to_owned()];
    if aggregate {
        lines.push("  Aggregate  (cost=0.00 rows=1)".to_owned());
    }
    if filtered {
        lines.push(format!(
            "  Filter  (cost={:.2} rows={})",
            estimated_rows as f64 * 0.01,
            estimated_rows
        ));
    }
    for join in joins {
        let right = table_definition(state, join.table.table_id)?;
        let choice = choose_join_strategy(
            estimated_rows,
            right.statistics().row_count,
            equi_join_columns(&join.on, join.table.offset).is_some(),
        );
        let name = match choice.strategy {
            JoinStrategy::NestedLoop => "Nested Loop",
            JoinStrategy::Hash => "Hash Join",
        };
        let kind = if join.kind == JoinKind::Left {
            "Left"
        } else {
            "Inner"
        };
        lines.push(format!(
            "  {name} {kind}  (cost={:.2} rows={:.0})",
            choice.estimated_cost, choice.estimated_rows
        ));
        estimated_rows = choice.estimated_rows as u64;
    }
    lines.push(format!(
        "    Seq Scan on {}  (cost={:.2} rows={})",
        table.binding,
        estimated_rows as f64 * 0.01,
        base.statistics().row_count
    ));
    for join in joins {
        let right = table_definition(state, join.table.table_id)?;
        lines.push(format!(
            "    Seq Scan on {}  (cost={:.2} rows={})",
            join.table.binding,
            right.statistics().row_count as f64 * 0.01,
            right.statistics().row_count
        ));
    }
    Ok(lines)
}

fn execute_update(
    state: &mut DatabaseState,
    table_id: TableId,
    assignments: Vec<(usize, BoundExpr)>,
    filter: Option<BoundExpr>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    table_definition(state, table_id)?;
    let source_rows = state
        .rows
        .get(&table_id)
        .map(|rows| (**rows).clone())
        .unwrap_or_default();
    let mut updated = 0u64;
    for old_row in source_rows {
        if filter
            .as_ref()
            .map(|filter| execution_predicate_matches(filter, &old_row, params))
            .transpose()?
            .unwrap_or(true)
        {
            let original = old_row.values.clone();
            let mut replacements = Vec::with_capacity(assignments.len());
            for (column_index, expression) in &assignments {
                replacements.push((
                    *column_index,
                    evaluate_scalar(expression, &original, params)?,
                ));
            }
            let mut proposed = old_row.clone();
            for (column_index, value) in replacements {
                proposed.values[column_index] = value;
            }
            let replacement = match fire_row_triggers_with_rows(
                state,
                table_id,
                TriggerTiming::Before,
                TriggerEvent::Update,
                Some(&old_row),
                Some(&proposed),
            )? {
                RowTriggerOutcome::Proceed(Some(row)) => row,
                RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
            };
            let position = state
                .rows
                .get(&table_id)
                .and_then(|rows| rows.iter().position(|row| row == &old_row))
                .ok_or_else(|| {
                    DbError::new(
                        "55000",
                        "BEFORE trigger changed the row targeted by the outer UPDATE",
                    )
                    .with_hint(
                        "Return a replacement NEW row instead of updating the same row recursively.",
                    )
                })?;
            Arc::make_mut(
                state
                    .rows
                    .get_mut(&table_id)
                    .ok_or_else(|| internal_error("updated table rows disappeared"))?,
            )[position] = replacement.clone();
            if replacement != old_row {
                apply_referential_actions(
                    state,
                    vec![ReferentialChange::Update {
                        table_id,
                        old: old_row.clone(),
                        new: replacement.clone(),
                    }],
                )?;
            }
            validate_database_rows(state)?;
            rebuild_table_derived(state, table_id)?;
            let _ = fire_row_triggers_with_rows(
                state,
                table_id,
                TriggerTiming::After,
                TriggerEvent::Update,
                Some(&old_row),
                Some(&replacement),
            )?;
            validate_database_rows(state)?;
            rebuild_table_derived(state, table_id)?;
            updated = updated.saturating_add(1);
        }
    }
    Ok((
        command_events(Schema::empty(), format!("UPDATE {updated}"), updated, None),
        true,
    ))
}

fn execute_delete(
    state: &mut DatabaseState,
    table_id: TableId,
    filter: Option<BoundExpr>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    table_definition(state, table_id)?;
    let source_rows = state
        .rows
        .get(&table_id)
        .map(|rows| (**rows).clone())
        .unwrap_or_default();
    let mut deleted = 0u64;
    for old_row in source_rows {
        let matches = filter
            .as_ref()
            .map(|filter| execution_predicate_matches(filter, &old_row, params))
            .transpose()?
            .unwrap_or(true);
        if !matches {
            continue;
        }
        if matches!(
            fire_row_triggers_with_rows(
                state,
                table_id,
                TriggerTiming::Before,
                TriggerEvent::Delete,
                Some(&old_row),
                None,
            )?,
            RowTriggerOutcome::Suppress
        ) {
            continue;
        }
        let position = state
            .rows
            .get(&table_id)
            .and_then(|rows| rows.iter().position(|row| row == &old_row))
            .ok_or_else(|| {
                DbError::new(
                    "55000",
                    "BEFORE trigger changed the row targeted by the outer DELETE",
                )
                .with_hint("Return OLD instead of deleting the same row recursively.")
            })?;
        Arc::make_mut(
            state
                .rows
                .get_mut(&table_id)
                .ok_or_else(|| internal_error("deleted table rows disappeared"))?,
        )
        .remove(position);
        apply_referential_actions(
            state,
            vec![ReferentialChange::Delete {
                table_id,
                old: old_row.clone(),
            }],
        )?;
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        let _ = fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::After,
            TriggerEvent::Delete,
            Some(&old_row),
            None,
        )?;
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        deleted = deleted.saturating_add(1);
    }
    Ok((
        command_events(Schema::empty(), format!("DELETE {deleted}"), deleted, None),
        true,
    ))
}

fn removed_rows(before: &[Row], after: &[Row]) -> Vec<Row> {
    let mut matched = vec![false; after.len()];
    let mut removed = Vec::new();
    for row in before {
        if let Some((index, _)) = after
            .iter()
            .enumerate()
            .find(|(index, candidate)| !matched[*index] && *candidate == row)
        {
            matched[index] = true;
        } else {
            removed.push(row.clone());
        }
    }
    removed
}

fn apply_referential_actions(
    state: &mut DatabaseState,
    changes: Vec<ReferentialChange>,
) -> Result<()> {
    let mut queue = VecDeque::from(changes);
    let mut applied = 0usize;
    while let Some(change) = queue.pop_front() {
        applied = applied.saturating_add(1);
        if applied > MAX_REFERENTIAL_ACTIONS {
            return Err(DbError::new(
                "54001",
                "referential action work exceeds the configured limit",
            ));
        }
        let (referenced_table_id, old_row, new_row) = match &change {
            ReferentialChange::Delete { table_id, old } => (*table_id, old, None),
            ReferentialChange::Update { table_id, old, new } => (*table_id, old, Some(new)),
        };
        let referenced_table = table_definition(state, referenced_table_id)?.clone();
        let referencing = state
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .flat_map(|table| {
                table.constraints().filter_map(|constraint| {
                    let ConstraintKind::ForeignKey {
                        columns,
                        referenced_table,
                        referenced_columns,
                        on_delete,
                        on_update,
                    } = &constraint.kind
                    else {
                        return None;
                    };
                    (*referenced_table == referenced_table_id).then(|| {
                        (
                            table.id,
                            constraint.name.clone(),
                            columns.clone(),
                            referenced_columns.clone(),
                            *on_delete,
                            *on_update,
                        )
                    })
                })
            })
            .collect::<Vec<_>>();

        for (
            child_table_id,
            constraint_name,
            local_columns,
            referenced_columns,
            on_delete,
            on_update,
        ) in referencing
        {
            let child_table = table_definition(state, child_table_id)?.clone();
            let parent_positions = referenced_columns
                .iter()
                .map(|column_id| {
                    referenced_table
                        .column_index_by_id(*column_id)
                        .ok_or_else(|| {
                            internal_error("foreign-key parent column is absent during action")
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let child_positions = local_columns
                .iter()
                .map(|column_id| {
                    child_table.column_index_by_id(*column_id).ok_or_else(|| {
                        internal_error("foreign-key child column is absent during action")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            if let Some(new_row) = new_row
                && parent_positions
                    .iter()
                    .all(|position| old_row.values[*position] == new_row.values[*position])
            {
                continue;
            }
            let action = if new_row.is_some() {
                on_update
            } else {
                on_delete
            };
            let child_rows = Arc::make_mut(
                state
                    .rows
                    .entry(child_table_id)
                    .or_insert_with(|| Arc::new(Vec::new())),
            );
            let matches_parent = |row: &Row| {
                child_positions
                    .iter()
                    .zip(&parent_positions)
                    .all(|(child, parent)| row.values[*child] == old_row.values[*parent])
            };
            if matches!(
                action,
                ReferentialAction::NoAction | ReferentialAction::Restrict
            ) && child_rows.iter().any(matches_parent)
            {
                return Err(DbError::new(
                    "23503",
                    format!("update or delete violates foreign-key constraint {constraint_name}"),
                ));
            }
            match action {
                ReferentialAction::NoAction | ReferentialAction::Restrict => {}
                ReferentialAction::Cascade if new_row.is_none() => {
                    let before = child_rows.clone();
                    child_rows.retain(|row| !matches_parent(row));
                    queue.extend(removed_rows(&before, child_rows).into_iter().map(|old| {
                        ReferentialChange::Delete {
                            table_id: child_table_id,
                            old,
                        }
                    }));
                }
                ReferentialAction::Cascade => {
                    let new_row = new_row.ok_or_else(|| {
                        internal_error("update cascade has no replacement parent row")
                    })?;
                    for child_row in child_rows.iter_mut().filter(|row| matches_parent(row)) {
                        let old = child_row.clone();
                        for (child, parent) in child_positions.iter().zip(&parent_positions) {
                            child_row.values[*child] = new_row.values[*parent].clone();
                        }
                        queue.push_back(ReferentialChange::Update {
                            table_id: child_table_id,
                            old,
                            new: child_row.clone(),
                        });
                    }
                }
                ReferentialAction::SetNull | ReferentialAction::SetDefault => {
                    for child_row in child_rows.iter_mut().filter(|row| matches_parent(row)) {
                        let old = child_row.clone();
                        for child in &child_positions {
                            child_row.values[*child] = if action == ReferentialAction::SetNull {
                                Value::Null
                            } else {
                                let column = &child_table.columns()[*child];
                                catalog_default_value(column.default.as_ref(), &column.data_type)?
                            };
                        }
                        queue.push_back(ReferentialChange::Update {
                            table_id: child_table_id,
                            old,
                            new: child_row.clone(),
                        });
                    }
                }
            }
            rebuild_table_derived(state, child_table_id)?;
        }
    }
    Ok(())
}

fn validate_rows(table: &TableDefinition, rows: &[Row]) -> Result<()> {
    for row in rows {
        if row.values.len() != table.columns().len() {
            return Err(internal_error("row width does not match table metadata"));
        }
        for (column, value) in table.columns().iter().zip(&row.values) {
            if !column.nullable && value.is_null() {
                return Err(DbError::new(
                    "23502",
                    format!(
                        "null value in column {} violates not-null constraint",
                        column.name
                    ),
                ));
            }
            coerce_execution_value(value.clone(), &column.data_type)?;
        }
    }
    for (column_index, column) in table.columns().iter().enumerate() {
        if !column.unique {
            continue;
        }
        for left in 0..rows.len() {
            let left_value = &rows[left].values[column_index];
            if left_value.is_null() {
                continue;
            }
            for right_row in rows.iter().skip(left + 1) {
                if left_value == &right_row.values[column_index] {
                    return Err(DbError::new(
                        "23505",
                        format!(
                            "duplicate value violates unique constraint on {}",
                            column.name
                        ),
                    ));
                }
            }
        }
    }
    for constraint in table.constraints() {
        match &constraint.kind {
            ConstraintKind::PrimaryKey { columns } | ConstraintKind::Unique { columns } => {
                let positions = columns
                    .iter()
                    .map(|column_id| {
                        table.column_index_by_id(*column_id).ok_or_else(|| {
                            internal_error("constraint column is absent from its table")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                for (left, left_row) in rows.iter().enumerate() {
                    let left_key = positions
                        .iter()
                        .map(|position| &left_row.values[*position])
                        .collect::<Vec<_>>();
                    if matches!(constraint.kind, ConstraintKind::PrimaryKey { .. })
                        && left_key.iter().any(|value| value.is_null())
                    {
                        return Err(DbError::new(
                            "23502",
                            format!(
                                "null value violates primary-key constraint {}",
                                constraint.name
                            ),
                        ));
                    }
                    if left_key.iter().any(|value| value.is_null()) {
                        continue;
                    }
                    for right_row in rows.iter().skip(left + 1) {
                        if positions.iter().all(|position| {
                            left_row.values[*position] == right_row.values[*position]
                        }) {
                            return Err(DbError::new(
                                "23505",
                                format!(
                                    "duplicate value violates unique constraint {}",
                                    constraint.name
                                ),
                            ));
                        }
                    }
                }
            }
            ConstraintKind::Check { expression } => {
                let bound =
                    bind_catalog_expression(expression, Some(table), Some(&ScalarType::Boolean))?;
                for row in rows {
                    if evaluate_scalar(&bound, &row.values, &[])? == Value::Boolean(false) {
                        return Err(DbError::new(
                            "23514",
                            format!("row violates check constraint {}", constraint.name),
                        ));
                    }
                }
            }
            ConstraintKind::ForeignKey { .. } => {}
        }
    }
    Ok(())
}

fn validate_database_rows(state: &DatabaseState) -> Result<()> {
    for schema in state.catalog.database().schemas() {
        for table in schema.tables() {
            let rows = state
                .rows
                .get(&table.id)
                .map_or(&[][..], |rows| rows.as_slice());
            validate_rows(table, rows)?;
            for constraint in table.constraints() {
                let ConstraintKind::ForeignKey {
                    columns,
                    referenced_table,
                    referenced_columns,
                    ..
                } = &constraint.kind
                else {
                    continue;
                };
                let local_positions = columns
                    .iter()
                    .map(|column_id| {
                        table.column_index_by_id(*column_id).ok_or_else(|| {
                            internal_error("foreign-key column is absent from its table")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let referenced = table_definition(state, *referenced_table)?;
                let referenced_positions = referenced_columns
                    .iter()
                    .map(|column_id| {
                        referenced.column_index_by_id(*column_id).ok_or_else(|| {
                            internal_error("foreign-key referenced column is absent from its table")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let referenced_rows = state
                    .rows
                    .get(referenced_table)
                    .map_or(&[][..], |rows| rows.as_slice());
                for row in rows {
                    if local_positions
                        .iter()
                        .any(|position| row.values[*position].is_null())
                    {
                        continue;
                    }
                    if !referenced_rows.iter().any(|candidate| {
                        local_positions
                            .iter()
                            .zip(&referenced_positions)
                            .all(|(local, remote)| row.values[*local] == candidate.values[*remote])
                    }) {
                        return Err(DbError::new(
                            "23503",
                            format!(
                                "insert or update violates foreign-key constraint {}",
                                constraint.name
                            ),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn rebuild_table_derived(state: &mut DatabaseState, table_id: TableId) -> Result<()> {
    let table = table_definition(state, table_id)?.clone();
    let rows = state.rows.get(&table_id).cloned().unwrap_or_default();
    let mut rebuilt = Vec::new();
    for definition in table.indexes() {
        if definition.method != IndexMethod::BTree {
            continue;
        }
        let key_positions = definition
            .key_columns
            .iter()
            .map(|column_id| {
                table
                    .column_index_by_id(*column_id)
                    .ok_or_else(|| internal_error("index key column is absent from its table"))
            })
            .collect::<Result<Vec<_>>>()?;
        let include_positions = definition
            .include_columns
            .iter()
            .map(|column_id| {
                table
                    .column_index_by_id(*column_id)
                    .ok_or_else(|| internal_error("index include column is absent from its table"))
            })
            .collect::<Result<Vec<_>>>()?;
        let entries = rows
            .iter()
            .enumerate()
            .map(|(row_index, row)| {
                let row_id = u64::try_from(row_index)
                    .map(RowId::new)
                    .map_err(|_| DbError::new("54000", "table row count exceeds index limits"))?;
                let key_values = key_positions
                    .iter()
                    .map(|position| row.values[*position].clone())
                    .collect::<Vec<_>>();
                let included = include_positions
                    .iter()
                    .map(|position| row.values[*position].clone())
                    .collect();
                IndexEntry::new(&key_values, row_id, included)
            })
            .collect::<Result<Vec<_>>>()?;
        let tree = BPlusTree::from_entries(definition.unique, entries)?;
        rebuilt.push((definition.id, tree));
    }
    let catalog = Arc::clone(&state.catalog);
    state.indexes.retain(|index_id, _| {
        catalog
            .index_by_id(*index_id)
            .is_some_and(|definition| definition.method == IndexMethod::BTree)
    });
    for (index_id, tree) in rebuilt {
        state.indexes.insert(index_id, Arc::new(tree));
    }
    Arc::make_mut(&mut state.catalog)
        .set_table_statistics(table_id, compute_statistics(&table, &rows)?)?;
    rebuild_search_catalog_for_table(state, table_id)?;
    Ok(())
}

fn rebuild_search_catalog_for_table(state: &mut DatabaseState, table_id: TableId) -> Result<()> {
    let searches = state
        .searches
        .rebuild_table(&state.catalog, &state.rows, table_id)?;
    state.searches = Arc::new(searches);
    Ok(())
}

fn reconcile_search_catalog(state: &mut DatabaseState) -> Result<()> {
    let searches = state.searches.reconcile(&state.catalog, &state.rows)?;
    state.searches = Arc::new(searches);
    Ok(())
}

fn compute_statistics(table: &TableDefinition, rows: &[Row]) -> Result<TableStatistics> {
    let mut columns = BTreeMap::new();
    for (column_index, column) in table.columns().iter().enumerate() {
        let values = rows
            .iter()
            .filter_map(|row| row.values.get(column_index))
            .collect::<Vec<_>>();
        let null_count = values.iter().filter(|value| value.is_null()).count() as u64;
        let mut distinct = HashSet::new();
        for value in values.iter().filter(|value| !value.is_null()) {
            distinct.insert(encode_row(&Row::new(vec![(*value).clone()]))?);
        }
        let (min, max) = if indexable_type(&column.data_type) {
            let mut minimum: Option<(IndexKey, Value)> = None;
            let mut maximum: Option<(IndexKey, Value)> = None;
            for value in values.iter().filter(|value| !value.is_null()) {
                let key = IndexKey::from_values(&[(*value).clone()])?;
                if minimum.as_ref().is_none_or(|(minimum, _)| key < *minimum) {
                    minimum = Some((key.clone(), (*value).clone()));
                }
                if maximum.as_ref().is_none_or(|(maximum, _)| key > *maximum) {
                    maximum = Some((key, (*value).clone()));
                }
            }
            (
                minimum.map(|(_, value)| value),
                maximum.map(|(_, value)| value),
            )
        } else {
            (None, None)
        };
        columns.insert(
            column.id,
            ColumnStatistics {
                null_count,
                distinct_count: distinct.len() as u64,
                min,
                max,
            },
        );
    }
    Ok(TableStatistics {
        row_count: rows.len() as u64,
        columns,
    })
}

fn table_definition(state: &DatabaseState, table_id: TableId) -> Result<&TableDefinition> {
    state
        .catalog
        .table_by_id(table_id)
        .ok_or_else(|| internal_error(format!("bound table ID {table_id:?} does not exist")))
}

fn search_index_table(
    state: &DatabaseState,
    index_id: IndexId,
    expected_method: IndexMethod,
) -> Result<TableId> {
    let definition = state
        .catalog
        .index_by_id(index_id)
        .ok_or_else(|| DbError::new("42704", format!("index {} does not exist", index_id.get())))?;
    if definition.method != expected_method {
        return Err(DbError::new(
            "42809",
            format!(
                "index {} uses {:?}, expected {expected_method:?}",
                definition.name, definition.method
            ),
        ));
    }
    Ok(definition.table_id)
}

fn search_result_row(state: &DatabaseState, table_id: TableId, row_id: SearchRowId) -> Result<Row> {
    let row_index = usize::try_from(row_id.get())
        .map_err(|_| internal_error("search Row ID exceeds the platform limit"))?;
    state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.get(row_index))
        .cloned()
        .ok_or_else(|| internal_error("search index returned a Row ID outside its table snapshot"))
}

fn evaluate_search_filter(
    state: &DatabaseState,
    table_id: TableId,
    filter: &ScalarSearchFilter,
) -> Result<AllowedRows> {
    let table = table_definition(state, table_id)?;
    let expression = bind_catalog_expression(
        &CatalogExpression::new(&filter.expression),
        Some(table),
        Some(&ScalarType::Boolean),
    )?;
    let rows = state
        .rows
        .get(&table_id)
        .map_or(&[][..], |rows| rows.as_slice());
    let mut allowed = BTreeSet::new();
    for (row_index, row) in rows.iter().enumerate() {
        if execution_predicate_matches(&expression, row, &filter.parameters)? {
            allowed.insert(
                u64::try_from(row_index)
                    .map(SearchRowId::new)
                    .map_err(|_| DbError::new("54000", "table row count exceeds search limits"))?,
            );
        }
    }
    Ok(Arc::new(allowed))
}

fn intersect_allowed_rows(
    current: Option<AllowedRows>,
    filter: AllowedRows,
) -> Option<AllowedRows> {
    match current {
        Some(current) => Some(Arc::new(current.intersection(&filter).copied().collect())),
        None => Some(filter),
    }
}

fn command_events(
    schema: Schema,
    tag: impl Into<String>,
    rows_affected: u64,
    batch: Option<Batch>,
) -> Vec<QueryEvent> {
    let mut events = vec![QueryEvent::Schema(schema)];
    if let Some(batch) = batch {
        events.push(QueryEvent::Batch(batch));
    }
    events.push(QueryEvent::Progress(QueryProgress {
        rows_processed: rows_affected,
    }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: tag.into(),
        rows_affected,
    }));
    events
}

fn internal_error(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message).with_hint("restart the session and retry")
}

#[must_use]
pub fn configured_data_dir(config: &EngineConfig) -> &Path {
    &config.data_dir
}

#[cfg(test)]
mod tests {
    use tempfile::{TempDir, tempdir};

    use super::*;

    fn engine() -> (TempDir, Engine) {
        let directory = tempdir().expect("tempdir");
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
        (directory, engine)
    }

    fn execute(session: &mut Session, sql: &str, params: &[Value]) -> Vec<QueryEvent> {
        session
            .execute(sql, params)
            .expect("execute statement")
            .collect()
    }

    fn rows(events: &[QueryEvent]) -> Vec<Row> {
        events
            .iter()
            .filter_map(|event| match event {
                QueryEvent::Batch(batch) => Some(batch.rows.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    fn create_documents(session: &mut Session) {
        execute(
            session,
            "CREATE TABLE documents (\
                id BIGINT PRIMARY KEY,\
                title TEXT NOT NULL,\
                score INTEGER\
            )",
            &[],
        );
    }

    #[test]
    fn executes_crud_with_parameters_ordering_and_limits() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        create_documents(&mut session);
        execute(
            &mut session,
            "INSERT INTO documents (id, title, score) VALUES \
             ($1, 'first', 10), ($2, 'second', 20), ($3, 'third', 30)",
            &[Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        );

        let events = execute(
            &mut session,
            "SELECT id, title FROM documents WHERE score >= $1 ORDER BY id DESC LIMIT 2",
            &[Value::Int32(15)],
        );
        assert_eq!(
            rows(&events),
            vec![
                Row::new(vec![Value::Int64(3), Value::Text("third".into())]),
                Row::new(vec![Value::Int64(2), Value::Text("second".into())]),
            ]
        );

        execute(
            &mut session,
            "UPDATE documents SET title = 'updated' WHERE id = $1",
            &[Value::Int64(2)],
        );
        execute(
            &mut session,
            "DELETE FROM documents WHERE id = $1",
            &[Value::Int64(1)],
        );
        let events = execute(
            &mut session,
            "SELECT id, title FROM documents ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&events),
            vec![
                Row::new(vec![Value::Int64(2), Value::Text("updated".into()),]),
                Row::new(vec![Value::Int64(3), Value::Text("third".into())]),
            ]
        );
    }

    #[test]
    fn committed_versions_reopen_with_stable_predecessors_and_visible_storage_scan() {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
            let mut session = engine.connect().expect("connect");
            create_documents(&mut session);
            execute(
                &mut session,
                "INSERT INTO documents VALUES (1, 'original', 10), (2, 'deleted', 20)",
                &[],
            );
            execute(
                &mut session,
                "UPDATE documents SET title = 'updated' WHERE id = 1",
                &[],
            );
            execute(&mut session, "DELETE FROM documents WHERE id = 2", &[]);

            let state = engine.state.read().expect("state");
            let table_id = state
                .catalog
                .table(
                    &Identifier::unquoted("public"),
                    &Identifier::unquoted("documents"),
                )
                .expect("documents")
                .id;
            let versions = state.versions.get(&table_id).expect("versions");
            let visible = state
                .visible_versions
                .get(&table_id)
                .expect("visible versions");
            assert_eq!(versions.len(), 3);
            assert_eq!(visible.as_slice(), &[3]);
            assert_eq!(versions[0].version_id, 1);
            assert_eq!(versions[0].header.previous_version, 0);
            assert_ne!(versions[0].header.xmax, 0);
            assert_eq!(versions[1].version_id, 2);
            assert_eq!(versions[1].header.previous_version, 0);
            assert_ne!(versions[1].header.xmax, 0);
            assert_eq!(versions[2].version_id, 3);
            assert_eq!(versions[2].header.previous_version, 1);
            assert_eq!(versions[2].header.xmin, versions[0].header.xmax);
            assert_eq!(versions[2].header.xmax, 0);
            assert_eq!(
                engine
                    .transaction_status
                    .transaction_outcome(
                        TransactionId::new(versions[2].header.xmin).expect("creator transaction")
                    )
                    .expect("creator outcome"),
                ordadb_transaction::TransactionOutcome::Committed
            );
        }

        let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
        let mut session = engine.connect().expect("connect");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id, title FROM documents ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("updated".into())
            ])]
        );
        let state = engine.state.read().expect("state");
        let table_id = state
            .catalog
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        assert_eq!(state.versions.get(&table_id).expect("versions").len(), 3);
        assert_eq!(
            state
                .visible_versions
                .get(&table_id)
                .expect("visible versions")
                .as_slice(),
            &[3]
        );
    }

    #[test]
    fn aborted_update_keeps_the_original_version_visible_after_reopen() {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
            let mut session = engine.connect().expect("connect");
            create_documents(&mut session);
            execute(
                &mut session,
                "INSERT INTO documents VALUES (1, 'original', 10)",
                &[],
            );
            let mut transaction = session.begin().expect("begin");
            transaction
                .execute("UPDATE documents SET title = 'aborted' WHERE id = 1", &[])
                .expect("update");
            transaction.rollback().expect("rollback");
        }

        let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
        let mut session = engine.connect().expect("connect");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id, title FROM documents",
                &[],
            )),
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("original".into())
            ])]
        );
        let state = engine.state.read().expect("state");
        let table_id = state
            .catalog
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        let versions = state.versions.get(&table_id).expect("versions");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].header.xmax, 0);
        assert_eq!(
            state
                .visible_versions
                .get(&table_id)
                .expect("visible versions")
                .as_slice(),
            &[1]
        );
    }

    #[test]
    fn compares_jsonb_parameters_by_equality_without_requiring_ordering() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE payloads (id BIGINT PRIMARY KEY, body JSONB NOT NULL)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO payloads VALUES (1, $1), (2, $2)",
            &[
                Value::Jsonb(serde_json::json!({"kind": "match"})),
                Value::Jsonb(serde_json::json!({"kind": "other"})),
            ],
        );

        let equal = execute(
            &mut session,
            "SELECT id FROM payloads WHERE body = $1 ORDER BY id",
            &[Value::Jsonb(serde_json::json!({"kind": "match"}))],
        );
        assert_eq!(rows(&equal), vec![Row::new(vec![Value::Int64(1)])]);

        let not_equal = execute(
            &mut session,
            "SELECT id FROM payloads WHERE body <> $1 ORDER BY id",
            &[Value::Jsonb(serde_json::json!({"kind": "match"}))],
        );
        assert_eq!(rows(&not_equal), vec![Row::new(vec![Value::Int64(2)])]);
    }

    #[test]
    fn enforces_not_null_primary_key_and_unique_constraints_atomically() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE NOT NULL)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO users VALUES (1, 'a@example.test')",
            &[],
        );

        let error = session
            .execute(
                "INSERT INTO users VALUES (2, 'b@example.test'), (1, 'c@example.test')",
                &[],
            )
            .expect_err("duplicate primary key");
        assert_eq!(error.sql_state, "23505");
        let events = execute(&mut session, "SELECT * FROM users", &[]);
        assert_eq!(rows(&events).len(), 1);

        let error = session
            .execute("INSERT INTO users VALUES (2, NULL)", &[])
            .expect_err("not null");
        assert_eq!(error.sql_state, "23502");
    }

    #[test]
    fn commits_rolls_back_and_allows_disjoint_writers() {
        let (_directory, engine) = engine();
        let mut first = engine.connect().expect("first");
        let mut second = engine.connect().expect("second");
        create_documents(&mut first);

        {
            let mut transaction = first.begin().expect("begin");
            transaction
                .execute("INSERT INTO documents VALUES (1, 'rolled back', 1)", &[])
                .expect("insert");
            transaction.rollback().expect("rollback");
        }
        assert!(rows(&execute(&mut first, "SELECT * FROM documents", &[])).is_empty());

        {
            let mut transaction = first.begin().expect("begin");
            transaction
                .execute("INSERT INTO documents VALUES (1, 'committed', 1)", &[])
                .expect("insert");
            transaction.commit().expect("commit");
        }
        assert_eq!(
            rows(&execute(&mut first, "SELECT * FROM documents", &[])).len(),
            1
        );

        let mut transaction = first.begin().expect("begin writer");
        transaction
            .execute("INSERT INTO documents VALUES (2, 'rolled back', 2)", &[])
            .expect("transaction insert");
        execute(
            &mut second,
            "INSERT INTO documents VALUES (3, 'concurrent', 3)",
            &[],
        );
        transaction.rollback().expect("rollback writer");
        assert_eq!(
            rows(&execute(
                &mut second,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
    }

    #[test]
    fn dml_locks_are_scoped_and_released_on_transaction_rollback() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        let mut transaction = session.begin().expect("begin");
        transaction
            .execute("INSERT INTO documents VALUES (1, 'locked', 1)", &[])
            .expect("insert");

        let (granted, waiting) = engine.locks.snapshot().expect("lock snapshot");
        assert!(waiting.is_empty());
        assert!(
            granted
                .iter()
                .any(|lock| { lock.key == LockKey::Database && lock.mode == LockMode::Shared })
        );
        assert!(granted.iter().any(|lock| {
            matches!(lock.key, LockKey::Table { .. }) && lock.mode == LockMode::Shared
        }));
        assert!(granted.iter().any(|lock| {
            matches!(lock.key, LockKey::IndexKey { .. }) && lock.mode == LockMode::Exclusive
        }));

        transaction.rollback().expect("rollback");
        let (granted, waiting) = engine.locks.snapshot().expect("released snapshot");
        assert!(granted.is_empty());
        assert!(waiting.is_empty());
    }

    #[test]
    fn concurrent_disjoint_writers_merge_without_lost_updates() {
        let (_directory, engine) = engine();
        let mut first = engine.connect().expect("first");
        let mut second = engine.connect().expect("second");
        create_documents(&mut first);

        let mut first_transaction = first.begin().expect("first begin");
        let mut second_transaction = second.begin().expect("second begin");
        first_transaction
            .execute("INSERT INTO documents VALUES (1, 'first', 1)", &[])
            .expect("first insert");
        second_transaction
            .execute("INSERT INTO documents VALUES (2, 'second', 2)", &[])
            .expect("second insert");
        first_transaction.commit().expect("first commit");
        second_transaction.commit().expect("second commit");

        assert_eq!(
            rows(&execute(
                &mut first,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
    }

    #[test]
    fn row_and_unique_conflicts_timeout_then_report_committed_duplicates() {
        let (_directory, engine) = engine();
        engine
            .set_default_lock_timeout(Duration::from_millis(20))
            .expect("configure lock timeout");
        let mut first = engine.connect().expect("first");
        let mut second = engine.connect().expect("second");
        create_documents(&mut first);
        execute(
            &mut first,
            "INSERT INTO documents VALUES (1, 'base', 1)",
            &[],
        );

        execute(&mut first, "BEGIN", &[]);
        execute(&mut second, "BEGIN", &[]);
        execute(
            &mut first,
            "UPDATE documents SET title = 'first' WHERE id = 1",
            &[],
        );
        assert_eq!(
            second
                .execute("UPDATE documents SET title = 'second' WHERE id = 1", &[],)
                .expect_err("row lock timeout")
                .sql_state,
            "55P03"
        );
        execute(&mut second, "ROLLBACK", &[]);
        execute(&mut first, "COMMIT", &[]);

        execute(&mut first, "BEGIN", &[]);
        execute(&mut second, "BEGIN", &[]);
        execute(
            &mut first,
            "INSERT INTO documents VALUES (2, 'first unique', 2)",
            &[],
        );
        assert_eq!(
            second
                .execute("INSERT INTO documents VALUES (2, 'second unique', 3)", &[],)
                .expect_err("unique key lock timeout")
                .sql_state,
            "55P03"
        );
        execute(&mut second, "ROLLBACK", &[]);
        execute(&mut first, "COMMIT", &[]);
        assert_eq!(
            second
                .execute(
                    "INSERT INTO documents VALUES (2, 'committed duplicate', 4)",
                    &[],
                )
                .expect_err("committed duplicate")
                .sql_state,
            "23505"
        );
    }

    #[test]
    fn engine_deadlock_aborts_the_youngest_transaction_and_releases_waiters() {
        let (_directory, engine) = engine();
        engine
            .set_default_lock_timeout(Duration::from_secs(1))
            .expect("configure lock timeout");
        let mut first = engine.connect().expect("first");
        let mut second = engine.connect().expect("second");
        create_documents(&mut first);
        execute(
            &mut first,
            "INSERT INTO documents VALUES (1, 'one', 1), (2, 'two', 2)",
            &[],
        );
        execute(&mut first, "BEGIN", &[]);
        execute(&mut second, "BEGIN", &[]);
        execute(
            &mut first,
            "UPDATE documents SET title = 'first' WHERE id = 1",
            &[],
        );
        execute(
            &mut second,
            "UPDATE documents SET title = 'second' WHERE id = 2",
            &[],
        );

        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = first
                .execute("UPDATE documents SET title = 'first' WHERE id = 2", &[])
                .map(|_| ());
            if result.is_ok() {
                execute(&mut first, "COMMIT", &[]);
            }
            send.send(result).expect("send first result");
        });
        let mut waiting_observed = false;
        for _ in 0..100 {
            if !engine.lock_snapshot().expect("lock snapshot").1.is_empty() {
                waiting_observed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            waiting_observed,
            "first transaction did not enter lock wait"
        );
        assert_eq!(
            second
                .execute("UPDATE documents SET title = 'second' WHERE id = 1", &[],)
                .expect_err("deadlock victim")
                .sql_state,
            "40P01"
        );
        execute(&mut second, "ROLLBACK", &[]);
        receive
            .recv_timeout(Duration::from_secs(1))
            .expect("first result")
            .expect("surviving transaction");
        worker.join().expect("first transaction join");
        assert!(
            engine
                .lock_snapshot()
                .expect("released lock snapshot")
                .0
                .is_empty()
        );
    }

    #[test]
    fn read_committed_refreshes_and_repeatable_read_retains_visibility() {
        let (_directory, engine) = engine();
        let mut reader = engine.connect().expect("reader");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut reader);
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (1, 'v1', 1)",
            &[],
        );

        execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v1".to_owned())])]
        );
        execute(
            &mut writer,
            "UPDATE documents SET title = 'v2' WHERE id = 1",
            &[],
        );
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v1".to_owned())])]
        );
        execute(&mut reader, "COMMIT", &[]);

        execute(&mut reader, "BEGIN ISOLATION LEVEL READ COMMITTED", &[]);
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v2".to_owned())])]
        );
        execute(
            &mut writer,
            "UPDATE documents SET title = 'v3' WHERE id = 1",
            &[],
        );
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v3".to_owned())])]
        );
        execute(&mut reader, "COMMIT", &[]);
    }

    #[test]
    fn read_committed_rebases_private_writes_over_disjoint_commits() {
        let (_directory, engine) = engine();
        let mut reader = engine.connect().expect("reader");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut reader);

        execute(&mut reader, "BEGIN ISOLATION LEVEL READ COMMITTED", &[]);
        execute(
            &mut reader,
            "INSERT INTO documents VALUES (1, 'private', 1)",
            &[],
        );
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (2, 'concurrent', 2)",
            &[],
        );
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        execute(
            &mut reader,
            "INSERT INTO documents VALUES (3, 'after-refresh', 3)",
            &[],
        );
        execute(&mut reader, "COMMIT", &[]);

        assert_eq!(
            rows(&execute(
                &mut writer,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
    }

    #[test]
    fn programmatic_read_committed_rebases_private_writes() {
        let (_directory, engine) = engine();
        let mut reader = engine.connect().expect("reader");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut reader);

        let mut transaction = reader.begin().expect("begin");
        transaction
            .execute("INSERT INTO documents VALUES (1, 'private', 1)", &[])
            .expect("private insert");
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (2, 'concurrent', 2)",
            &[],
        );
        let selected = transaction
            .execute("SELECT id FROM documents ORDER BY id", &[])
            .expect("refreshed select")
            .collect::<Vec<_>>();
        assert_eq!(
            rows(&selected),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        transaction.commit().expect("commit");
    }

    #[test]
    fn sql_transaction_upgrades_staged_dml_before_ddl() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut session);

        execute(&mut session, "BEGIN ISOLATION LEVEL READ COMMITTED", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'private', 1)",
            &[],
        );
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (2, 'concurrent', 2)",
            &[],
        );
        execute(
            &mut session,
            "CREATE INDEX documents_score_idx ON documents (score)",
            &[],
        );
        execute(&mut session, "COMMIT", &[]);

        assert_eq!(
            rows(&execute(
                &mut writer,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        execute(&mut writer, "DROP INDEX documents_score_idx", &[]);
    }

    #[test]
    fn programmatic_transaction_upgrades_staged_dml_before_ddl() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);

        let mut transaction = session.begin().expect("begin");
        transaction
            .execute("INSERT INTO documents VALUES (1, 'private', 1)", &[])
            .expect("insert");
        transaction
            .execute("CREATE INDEX documents_score_idx ON documents (score)", &[])
            .expect("create index after DML");
        transaction.commit().expect("commit");

        execute(&mut session, "DROP INDEX documents_score_idx", &[]);
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![Value::Int64(1)])]
        );
    }

    #[test]
    fn sql_savepoint_restores_candidate_and_recovers_failed_transaction() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(&mut session, "BEGIN", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'before', 1)",
            &[],
        );
        execute(&mut session, "SAVEPOINT keep_before", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (2, 'after', 2)",
            &[],
        );
        let duplicate = session
            .execute("INSERT INTO documents VALUES (1, 'duplicate', 3)", &[])
            .expect_err("duplicate");
        assert_eq!(duplicate.sql_state, "23505");
        assert_eq!(session.transaction_status(), TransactionStatus::Failed);
        assert_eq!(
            session
                .execute("SELECT id FROM documents", &[])
                .expect_err("failed transaction")
                .sql_state,
            "25P02"
        );

        execute(&mut session, "ROLLBACK TO SAVEPOINT keep_before", &[]);
        assert_eq!(session.transaction_status(), TransactionStatus::Active);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (3, 'recovered', 3)",
            &[],
        );
        execute(&mut session, "COMMIT", &[]);
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
    }

    #[test]
    fn sql_savepoint_rollback_restores_ssi_predicates() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(&mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
        execute(&mut session, "SAVEPOINT before_read", &[]);
        execute(&mut session, "SELECT id FROM documents", &[]);

        let before = engine.ssi.snapshot().expect("SSI before rollback");
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].read_predicates, 1);
        execute(&mut session, "ROLLBACK TO SAVEPOINT before_read", &[]);

        let after = engine.ssi.snapshot().expect("SSI after rollback");
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].read_predicates, 0);
        execute(&mut session, "ROLLBACK", &[]);
    }

    #[test]
    fn serializable_ssi_rejects_write_skew() {
        let (_directory, engine) = engine();
        let mut first = engine.connect().expect("first session");
        let mut second = engine.connect().expect("second session");
        execute(
            &mut first,
            "CREATE TABLE doctors (id INT PRIMARY KEY, on_call BOOLEAN NOT NULL)",
            &[],
        );
        execute(
            &mut first,
            "INSERT INTO doctors VALUES (1, TRUE), (2, TRUE)",
            &[],
        );
        execute(&mut first, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
        execute(&mut second, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
        assert_eq!(
            rows(&execute(
                &mut first,
                "SELECT id FROM doctors WHERE on_call = TRUE ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int32(1)]),
                Row::new(vec![Value::Int32(2)]),
            ]
        );
        execute(
            &mut second,
            "SELECT id FROM doctors WHERE on_call = TRUE ORDER BY id",
            &[],
        );
        execute(
            &mut first,
            "UPDATE doctors SET on_call = FALSE WHERE id = 1",
            &[],
        );
        execute(&mut first, "COMMIT", &[]);
        execute(
            &mut second,
            "UPDATE doctors SET on_call = FALSE WHERE id = 2",
            &[],
        );
        let error = second.execute("COMMIT", &[]).expect_err("write skew");
        assert_eq!(error.sql_state, "40001");

        let mut verifier = engine.connect().expect("verifier");
        assert_eq!(
            rows(&execute(
                &mut verifier,
                "SELECT id FROM doctors WHERE on_call = TRUE ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![Value::Int32(2)])]
        );
    }

    #[test]
    fn isolation_snapshots_prevent_dirty_reads_and_repeatable_read_phantoms() {
        let (_directory, engine) = engine();
        let mut writer = engine.connect().expect("writer");
        let mut reader = engine.connect().expect("reader");
        create_documents(&mut writer);

        execute(&mut writer, "BEGIN", &[]);
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (1, 'uncommitted', 1)",
            &[],
        );
        assert!(rows(&execute(&mut reader, "SELECT id FROM documents", &[])).is_empty());
        execute(&mut writer, "COMMIT", &[]);

        execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![Value::Int64(1)])]
        );
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (2, 'phantom', 2)",
            &[],
        );
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![Value::Int64(1)])]
        );
        execute(&mut reader, "COMMIT", &[]);
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
    }

    #[test]
    fn repeatable_read_rejects_stale_update_and_delete_targets() {
        let (_directory, engine) = engine();
        let mut stale = engine.connect().expect("stale session");
        let mut concurrent = engine.connect().expect("concurrent session");
        create_documents(&mut stale);
        execute(
            &mut stale,
            "INSERT INTO documents VALUES (1, 'first', 1), (2, 'second', 2)",
            &[],
        );

        execute(&mut stale, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(&mut stale, "SELECT title FROM documents WHERE id = 1", &[]);
        execute(
            &mut concurrent,
            "UPDATE documents SET title = 'concurrent' WHERE id = 1",
            &[],
        );
        assert_eq!(
            stale
                .execute("UPDATE documents SET title = 'stale' WHERE id = 1", &[],)
                .expect_err("stale update conflict")
                .sql_state,
            "40001"
        );
        execute(&mut stale, "ROLLBACK", &[]);

        execute(&mut stale, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(&mut stale, "SELECT title FROM documents WHERE id = 2", &[]);
        execute(
            &mut concurrent,
            "UPDATE documents SET title = 'changed' WHERE id = 2",
            &[],
        );
        assert_eq!(
            stale
                .execute("DELETE FROM documents WHERE id = 2", &[])
                .expect_err("stale delete conflict")
                .sql_state,
            "40001"
        );
        execute(&mut stale, "ROLLBACK", &[]);
    }

    #[test]
    fn repeatable_read_writers_merge_unrelated_row_changes() {
        let (_directory, engine) = engine();
        let mut first = engine.connect().expect("first session");
        let mut second = engine.connect().expect("second session");
        create_documents(&mut first);
        execute(
            &mut first,
            "INSERT INTO documents VALUES (1, 'first', 1), (2, 'second', 2)",
            &[],
        );
        execute(&mut first, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(&mut second, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(
            &mut first,
            "UPDATE documents SET title = 'first-committed' WHERE id = 1",
            &[],
        );
        execute(
            &mut second,
            "UPDATE documents SET title = 'second-committed' WHERE id = 2",
            &[],
        );
        execute(&mut first, "COMMIT", &[]);
        execute(&mut second, "COMMIT", &[]);

        let mut verifier = engine.connect().expect("verifier");
        assert_eq!(
            rows(&execute(
                &mut verifier,
                "SELECT title FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Text("first-committed".to_owned())]),
                Row::new(vec![Value::Text("second-committed".to_owned())]),
            ]
        );
    }

    #[test]
    fn vacuum_protects_active_snapshots_then_reclaims_and_analyzes() {
        let (_directory, engine) = engine();
        let mut reader = engine.connect().expect("reader");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut writer);
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (1, 'v1', 1)",
            &[],
        );
        execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(&mut reader, "SELECT title FROM documents WHERE id = 1", &[]);
        execute(
            &mut writer,
            "UPDATE documents SET title = 'v2' WHERE id = 1",
            &[],
        );
        let table_id = {
            let state = engine.state.read().expect("state");
            state
                .catalog
                .table(
                    &Identifier::unquoted("public"),
                    &Identifier::unquoted("documents"),
                )
                .expect("documents")
                .id
        };
        execute(&mut writer, "VACUUM documents", &[]);
        assert_eq!(
            engine
                .state
                .read()
                .expect("protected state")
                .versions
                .get(&table_id)
                .expect("versions")
                .len(),
            2
        );

        execute(&mut reader, "ROLLBACK", &[]);
        execute(&mut writer, "VACUUM ANALYZE documents", &[]);
        let state = engine.state.read().expect("vacuumed state");
        let versions = state.versions.get(&table_id).expect("versions");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version_id, 1);
        assert_eq!(versions[0].header.previous_version, 0);
        assert_eq!(
            state
                .catalog
                .table_by_id(table_id)
                .expect("documents")
                .statistics()
                .row_count,
            1
        );
        drop(state);
        assert_eq!(
            rows(&execute(
                &mut writer,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v2".to_owned())])]
        );
    }

    #[test]
    fn vacuum_rejects_an_expired_protected_snapshot() {
        let (_directory, engine) = engine();
        engine
            .set_maximum_snapshot_age(Duration::from_millis(1))
            .expect("configure snapshot age");
        let mut reader = engine.connect().expect("reader");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut writer);
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (1, 'visible', 1)",
            &[],
        );
        execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(&mut reader, "SELECT id FROM documents", &[]);
        std::thread::sleep(Duration::from_millis(5));
        let error = writer
            .execute("VACUUM documents", &[])
            .expect_err("expired snapshot");
        assert_eq!(error.sql_state, "55000");
        assert!(error.message.contains("expired snapshot"));
        execute(&mut reader, "ROLLBACK", &[]);
        execute(&mut writer, "VACUUM documents", &[]);
    }

    #[test]
    fn full_vacuum_freezes_live_versions_and_compacts_transaction_status() {
        let (directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'v1', 1)",
            &[],
        );
        execute(
            &mut session,
            "UPDATE documents SET title = 'v2' WHERE id = 1",
            &[],
        );
        let before = engine
            .transaction_status
            .snapshot()
            .expect("status before vacuum");
        execute(&mut session, "VACUUM", &[]);
        let after = engine
            .transaction_status
            .snapshot()
            .expect("status after vacuum");
        assert!(after.retained_transaction_floor > before.retained_transaction_floor);
        assert!(after.statuses.len() < before.statuses.len());
        let table_id = engine
            .catalog_snapshot()
            .expect("catalog")
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        assert!(
            engine
                .state
                .read()
                .expect("state")
                .versions
                .get(&table_id)
                .expect("versions")
                .iter()
                .all(|version| version.header.xmin == FROZEN_TRANSACTION_ID)
        );
        drop(session);
        drop(engine);

        let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
        let mut session = reopened.connect().expect("reopened session");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v2".to_owned())])]
        );
    }

    #[test]
    fn vacuum_is_rejected_inside_sql_and_programmatic_transactions() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(&mut session, "BEGIN", &[]);
        assert_eq!(
            session
                .execute("VACUUM", &[])
                .expect_err("SQL transaction VACUUM")
                .sql_state,
            "25001"
        );
        execute(&mut session, "ROLLBACK", &[]);

        let mut transaction = session.begin().expect("programmatic transaction");
        assert_eq!(
            transaction
                .execute("VACUUM", &[])
                .expect_err("programmatic transaction VACUUM")
                .sql_state,
            "25001"
        );
        transaction.rollback().expect("rollback");
    }

    #[test]
    fn failed_vacuum_and_analyze_reopen_without_publishing_candidates() {
        let directory = tempdir().expect("tempdir");
        {
            let engine =
                Engine::open(EngineConfig::new(directory.path())).expect("baseline engine");
            let mut session = engine.connect().expect("baseline session");
            create_documents(&mut session);
            execute(
                &mut session,
                "INSERT INTO documents VALUES (1, 'v1', 1)",
                &[],
            );
            execute(
                &mut session,
                "UPDATE documents SET title = 'v2' WHERE id = 1",
                &[],
            );
        }

        let vacuum_fault = ordadb_transaction::DeterministicFaultInjector::new();
        let vacuum_injector: Arc<dyn FaultInjector> = vacuum_fault.clone();
        let engine =
            Engine::open_with_fault_injector(EngineConfig::new(directory.path()), vacuum_injector)
                .expect("vacuum engine");
        let baseline_generation = engine
            .status_snapshot()
            .expect("baseline status")
            .generation;
        let mut session = engine.connect().expect("vacuum session");
        vacuum_fault
            .arm(FaultPoint::AfterDataSync, 1)
            .expect("arm vacuum fault");
        assert_eq!(
            session
                .execute("VACUUM documents", &[])
                .expect_err("vacuum fault")
                .sql_state,
            "58030"
        );
        drop(session);
        drop(engine);

        let recovered = Engine::open(EngineConfig::new(directory.path())).expect("recover vacuum");
        assert_eq!(
            recovered
                .status_snapshot()
                .expect("recovered status")
                .generation,
            baseline_generation
        );
        let table_id = recovered
            .catalog_snapshot()
            .expect("catalog")
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        assert_eq!(
            recovered
                .state
                .read()
                .expect("recovered state")
                .versions
                .get(&table_id)
                .expect("version chain")
                .len(),
            2
        );
        drop(recovered);

        let analyze_fault = ordadb_transaction::DeterministicFaultInjector::new();
        let analyze_injector: Arc<dyn FaultInjector> = analyze_fault.clone();
        let engine =
            Engine::open_with_fault_injector(EngineConfig::new(directory.path()), analyze_injector)
                .expect("analyze engine");
        let baseline_generation = engine
            .status_snapshot()
            .expect("baseline status")
            .generation;
        let mut session = engine.connect().expect("analyze session");
        analyze_fault
            .arm(FaultPoint::AfterDataSync, 1)
            .expect("arm analyze fault");
        assert_eq!(
            session
                .execute("ANALYZE documents", &[])
                .expect_err("analyze fault")
                .sql_state,
            "58030"
        );
        drop(session);
        drop(engine);

        let recovered = Engine::open(EngineConfig::new(directory.path())).expect("recover analyze");
        assert_eq!(
            recovered
                .status_snapshot()
                .expect("recovered status")
                .generation,
            baseline_generation
        );
        let mut session = recovered.connect().expect("final session");
        execute(&mut session, "VACUUM ANALYZE documents", &[]);
        assert_eq!(
            recovered
                .state
                .read()
                .expect("vacuumed state")
                .versions
                .get(&table_id)
                .expect("compacted version chain")
                .len(),
            1
        );
    }

    #[test]
    fn repeated_savepoint_names_use_the_nearest_frame_and_release_it() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(&mut session, "BEGIN", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'base', 1)",
            &[],
        );
        execute(&mut session, "SAVEPOINT repeated", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (2, 'middle', 2)",
            &[],
        );
        execute(&mut session, "SAVEPOINT repeated", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (3, 'latest', 3)",
            &[],
        );

        execute(&mut session, "ROLLBACK TO repeated", &[]);
        execute(&mut session, "RELEASE SAVEPOINT repeated", &[]);
        execute(&mut session, "ROLLBACK TO repeated", &[]);
        execute(&mut session, "COMMIT", &[]);
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![Value::Int64(1)])]
        );
    }

    #[test]
    fn read_only_and_chained_transactions_preserve_characteristics() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(
            &mut session,
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE",
            &[],
        );
        let first_id = match &session.sql_transaction {
            SqlTransactionState::Active(transaction) => {
                assert_eq!(
                    transaction.transaction.characteristics(),
                    Some(TransactionCharacteristics {
                        isolation_level: ordadb_transaction::IsolationLevel::Serializable,
                        access_mode: TransactionAccessMode::ReadOnly,
                        deferrable: true,
                    })
                );
                transaction.transaction.transaction_id()
            }
            _ => panic!("expected active transaction"),
        };
        assert_eq!(
            session
                .execute("INSERT INTO documents VALUES (1, 'blocked', 1)", &[])
                .expect_err("read only")
                .sql_state,
            "25006"
        );
        execute(&mut session, "ROLLBACK AND CHAIN", &[]);
        let second_id = match &session.sql_transaction {
            SqlTransactionState::Active(transaction) => {
                assert_eq!(
                    transaction.transaction.characteristics(),
                    Some(TransactionCharacteristics {
                        isolation_level: ordadb_transaction::IsolationLevel::Serializable,
                        access_mode: TransactionAccessMode::ReadOnly,
                        deferrable: true,
                    })
                );
                transaction.transaction.transaction_id()
            }
            _ => panic!("expected chained transaction"),
        };
        assert!(second_id > first_id);
        execute(&mut session, "ROLLBACK AND NO CHAIN", &[]);
        assert_eq!(session.transaction_status(), TransactionStatus::Idle);
    }

    #[test]
    fn deferrable_safe_snapshot_wait_cancels_through_the_session_boundary() {
        let (_directory, engine) = engine();
        let mut writer = engine.connect().expect("writer");
        let mut reader = engine.connect().expect("reader");
        create_documents(&mut writer);
        execute(&mut writer, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
        execute(
            &mut reader,
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE",
            &[],
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let worker_cancellation = Arc::clone(&cancellation);
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = reader
                .execute_stream_with_cancellation(
                    "SELECT id FROM documents",
                    &[],
                    worker_cancellation,
                )
                .map(|_| ());
            send.send((result, reader.transaction_status()))
                .expect("send cancellation result");
        });
        std::thread::sleep(Duration::from_millis(20));
        cancellation.store(true, Ordering::Release);

        let (result, status) = receive
            .recv_timeout(Duration::from_secs(1))
            .expect("cancelled statement result");
        assert_eq!(
            result.expect_err("safe snapshot cancellation").sql_state,
            "57014"
        );
        assert_eq!(status, TransactionStatus::Failed);
        execute(&mut writer, "ROLLBACK", &[]);
        worker.join().expect("reader worker");
    }

    #[test]
    fn emits_schema_then_work_then_exactly_one_completion() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        create_documents(&mut session);
        let events = execute(&mut session, "SELECT * FROM documents", &[]);

        assert!(matches!(events.first(), Some(QueryEvent::Schema(_))));
        assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, QueryEvent::Complete(_)))
                .count(),
            1
        );
        assert!(events[1..events.len() - 1].iter().all(|event| matches!(
            event,
            QueryEvent::Batch(_) | QueryEvent::Progress(_) | QueryEvent::Notice(_)
        )));
    }

    #[test]
    fn open_bootstraps_and_reopens_the_persistent_store() {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
            assert_eq!(configured_data_dir(engine.config()), directory.path());
        }
        assert!(directory.path().join("ordadb.data").is_file());
        assert!(Engine::open(EngineConfig::new(directory.path())).is_ok());
    }

    #[test]
    fn cluster_transaction_floor_seeds_the_first_durable_transaction() {
        let directory = tempdir().expect("tempdir");
        let engine = Engine::open(EngineConfig::for_cluster(
            directory.path().join("database"),
            directory.path(),
            42,
        ))
        .expect("open cluster database");
        let mut session = engine.connect().expect("session");
        execute(
            &mut session,
            "CREATE TABLE migration_floor (id BIGINT)",
            &[],
        );
        drop(session);
        drop(engine);

        assert_eq!(
            ordadb_transaction::inspect_wal_read_only(directory.path().join("database"))
                .expect("inspect WAL")
                .max_transaction_id
                .expect("transaction ID")
                .get(),
            42
        );
    }

    #[test]
    fn executes_inner_left_join_grouped_aggregates_and_having() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT, amount BIGINT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO orders VALUES (10, 1, 5), (11, 1, 7)",
            &[],
        );

        let grouped = execute(
            &mut session,
            "SELECT c.id, COUNT(o.id) AS order_count, SUM(o.amount) AS total \
             FROM customers c LEFT JOIN orders o ON c.id = o.customer_id \
             GROUP BY c.id ORDER BY c.id",
            &[],
        );
        assert_eq!(
            rows(&grouped),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(2), Value::Int64(12)]),
                Row::new(vec![Value::Int64(2), Value::Int64(0), Value::Null]),
            ]
        );

        let having = execute(
            &mut session,
            "SELECT c.id, COUNT(o.id) AS order_count \
             FROM customers c INNER JOIN orders o ON c.id = o.customer_id \
             GROUP BY c.id HAVING COUNT(o.id) > 1",
            &[],
        );
        assert_eq!(
            rows(&having),
            vec![Row::new(vec![Value::Int64(1), Value::Int64(2)])]
        );

        let aggregate = execute(
            &mut session,
            "SELECT COUNT(*), AVG(amount), MIN(amount), MAX(amount) FROM orders",
            &[],
        );
        assert_eq!(
            rows(&aggregate),
            vec![Row::new(vec![
                Value::Int64(2),
                Value::Float64(6.0),
                Value::Int64(5),
                Value::Int64(7),
            ])]
        );
    }

    #[test]
    fn persists_covering_indexes_statistics_and_explains_real_access_paths() {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
            let mut session = engine.connect().expect("connect");
            execute(
                &mut session,
                "CREATE TABLE metrics (id BIGINT PRIMARY KEY, bucket BIGINT, score BIGINT, payload TEXT)",
                &[],
            );
            let values = (0..512)
                .map(|value| format!("({value}, {}, {value}, 'p{value}')", value % 8))
                .collect::<Vec<_>>()
                .join(", ");
            execute(
                &mut session,
                &format!("INSERT INTO metrics VALUES {values}"),
                &[],
            );
            let duplicate = session
                .execute(
                    "CREATE UNIQUE INDEX metrics_bucket_unique ON metrics (bucket)",
                    &[],
                )
                .expect_err("duplicate unique build");
            assert_eq!(duplicate.sql_state, "23505");
            execute(
                &mut session,
                "CREATE INDEX metrics_score_idx ON metrics (score) INCLUDE (payload)",
                &[],
            );
        }

        let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
        let mut session = engine.connect().expect("connect");
        let explain = execute(
            &mut session,
            "EXPLAIN SELECT payload FROM metrics WHERE score = 511",
            &[],
        );
        let plan = rows(&explain)
            .into_iter()
            .filter_map(|row| match row.values.as_slice() {
                [Value::Text(line)] => Some(line.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            plan.iter().any(|line| line.contains("Index Scan")),
            "{plan:?}"
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT payload FROM metrics WHERE score = 511",
                &[],
            )),
            vec![Row::new(vec![Value::Text("p511".into())])]
        );
    }

    #[test]
    fn fallible_stream_preserves_event_order_and_legacy_adapter() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE stream_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO stream_items VALUES (1), (2), (3)",
            &[],
        );

        let events = session
            .execute_stream("SELECT id FROM stream_items ORDER BY id", &[])
            .expect("stream")
            .collect::<Result<Vec<_>>>()
            .expect("fallible events");
        assert!(matches!(events.first(), Some(QueryEvent::Schema(_))));
        assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, QueryEvent::Complete(_)))
                .count(),
            1
        );
        assert_eq!(rows(&events).len(), 3);
        assert_eq!(
            session
                .execute("SELECT id FROM stream_items", &[])
                .expect("legacy")
                .filter(|event| matches!(event, QueryEvent::Complete(_)))
                .count(),
            1
        );
    }

    #[test]
    fn storage_backed_stream_holds_one_generation_until_exhaustion() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE lazy_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(&mut session, "INSERT INTO lazy_items VALUES (1), (2)", &[]);

        let snapshot = session
            .execute_stream("SELECT id FROM lazy_items ORDER BY id", &[])
            .expect("lazy stream");
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            1
        );

        let writer_engine = engine.clone();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel(0);
        let writer = std::thread::spawn(move || {
            let mut writer_session = writer_engine.connect().expect("writer connect");
            started_tx.send(()).expect("signal writer start");
            let result = writer_session
                .execute("INSERT INTO lazy_items VALUES (3)", &[])
                .map(|_| ());
            finished_tx.send(result).expect("send writer result");
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("writer started");
        let waiting_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while engine
            .storage_access
            .waiting_writers()
            .expect("waiting writers")
            == 0
        {
            assert!(
                std::time::Instant::now() < waiting_deadline,
                "writer did not reach the storage gate"
            );
            std::thread::yield_now();
        }
        assert!(matches!(
            finished_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));

        let events = snapshot
            .collect::<Result<Vec<_>>>()
            .expect("snapshot events");
        assert_eq!(
            rows(&events),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            0
        );
        finished_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("writer finished after stream exhaustion")
            .expect("writer commit");
        writer.join().expect("writer thread");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id FROM lazy_items ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
    }

    #[test]
    fn storage_access_gate_prefers_a_waiting_writer_over_new_readers() {
        let gate = Arc::new(StorageAccessGate::default());
        let first_reader = gate.acquire_read().expect("first reader");
        let writer_gate = Arc::clone(&gate);
        let (writer_acquired_tx, writer_acquired_rx) = std::sync::mpsc::sync_channel(0);
        let (release_writer_tx, release_writer_rx) = std::sync::mpsc::sync_channel(0);
        let writer = std::thread::spawn(move || {
            let lease = writer_gate.acquire_write().expect("writer lease");
            writer_acquired_tx.send(()).expect("writer acquired");
            release_writer_rx.recv().expect("release writer");
            drop(lease);
        });

        let waiting_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while gate.waiting_writers().expect("waiting writers") == 0 {
            assert!(
                std::time::Instant::now() < waiting_deadline,
                "writer did not reach the storage gate"
            );
            std::thread::yield_now();
        }

        let second_reader_gate = Arc::clone(&gate);
        let (reader_acquired_tx, reader_acquired_rx) = std::sync::mpsc::sync_channel(0);
        let second_reader = std::thread::spawn(move || {
            let lease = second_reader_gate.acquire_read().expect("second reader");
            reader_acquired_tx.send(()).expect("reader acquired");
            drop(lease);
        });

        drop(first_reader);
        writer_acquired_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("writer wins after readers drain");
        assert!(matches!(
            reader_acquired_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        release_writer_tx.send(()).expect("release writer");
        reader_acquired_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("reader proceeds after writer");
        writer.join().expect("writer thread");
        second_reader.join().expect("reader thread");
    }

    #[test]
    fn storage_scan_open_error_and_stream_drop_release_the_read_lease() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE lease_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(&mut session, "INSERT INTO lease_items VALUES (1)", &[]);

        let generation = session.state.read().expect("state").generation;
        let snapshot = session.state.read().expect("state").clone();
        let provider = StorageTableProviderV2::new(
            Arc::clone(&engine.store),
            Arc::clone(&engine.storage_access),
            generation,
            &snapshot.rows,
        );
        let error = match provider.scan(TableId::new(u64::MAX)) {
            Ok(_) => panic!("unknown table scan must fail"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "42P01");
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            0
        );

        let stream = session
            .execute_stream("SELECT id FROM lease_items", &[])
            .expect("stream");
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            1
        );
        drop(stream);
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            0
        );
    }

    #[test]
    fn storage_scan_rejects_resident_row_count_mismatch_and_releases_lease() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE resident_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(&mut session, "INSERT INTO resident_items VALUES (1)", &[]);

        let snapshot = session.state.read().expect("state").clone();
        let table_id = *snapshot.rows.keys().next().expect("resident table");
        let mismatched_rows = BTreeMap::from([(table_id, Arc::new(Vec::<Row>::new()))]);
        let provider = StorageTableProviderV2::new(
            Arc::clone(&engine.store),
            Arc::clone(&engine.storage_access),
            snapshot.generation,
            &mismatched_rows,
        );

        let error = match provider.scan(table_id) {
            Ok(_) => panic!("row-count mismatch must fail"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "XX001");
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            0
        );
    }

    #[test]
    fn cancelled_storage_stream_releases_resources_without_terminal_success_events() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE cancelled_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO cancelled_items VALUES (1), (2), (3)",
            &[],
        );

        let cancellation = Arc::new(AtomicBool::new(false));
        let mut stream = session
            .execute_stream_with_cancellation(
                "SELECT id FROM cancelled_items",
                &[],
                Arc::clone(&cancellation),
            )
            .expect("stream");
        assert!(matches!(
            stream.next().expect("schema").expect("schema event"),
            QueryEvent::Schema(_)
        ));
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            1
        );

        cancellation.store(true, Ordering::Release);
        assert_eq!(
            stream
                .next()
                .expect("cancellation error")
                .expect_err("cancelled")
                .sql_state,
            "57014"
        );
        assert!(stream.next().is_none());
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            0
        );
    }

    #[test]
    fn fallible_stream_retains_query_accounted_peak_after_completion() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE memory_items (id BIGINT, payload TEXT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO memory_items VALUES (1, 'alpha'), (2, 'beta')",
            &[],
        );

        let mut stream = session
            .execute_stream("SELECT id, payload FROM memory_items", &[])
            .expect("stream");
        assert_eq!(stream.execution_memory_peak_bytes(), Some(0));
        while stream.next().is_some() {}
        assert!(
            stream
                .execution_memory_peak_bytes()
                .is_some_and(|peak| peak > 0 && peak <= 256 * 1024 * 1024)
        );
    }

    #[test]
    fn read_snapshots_share_arcs_and_writes_copy_only_the_affected_table() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE cow_a (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE cow_b (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(&mut session, "INSERT INTO cow_a VALUES (1)", &[]);
        execute(&mut session, "INSERT INTO cow_b VALUES (2)", &[]);

        let (a_before, b_before, catalog_before) = {
            let state = session.state.read().expect("state");
            (
                state.rows.get(&TableId::new(1)).expect("a").clone(),
                state.rows.get(&TableId::new(2)).expect("b").clone(),
                state.catalog.clone(),
            )
        };
        execute(&mut session, "SELECT id FROM cow_a", &[]);
        {
            let state = session.state.read().expect("state");
            assert!(Arc::ptr_eq(
                &a_before,
                state.rows.get(&TableId::new(1)).expect("a")
            ));
            assert!(Arc::ptr_eq(
                &b_before,
                state.rows.get(&TableId::new(2)).expect("b")
            ));
            assert!(Arc::ptr_eq(&catalog_before, &state.catalog));
        }

        execute(&mut session, "UPDATE cow_a SET id = 3", &[]);
        let state = session.state.read().expect("state");
        assert!(!Arc::ptr_eq(
            &a_before,
            state.rows.get(&TableId::new(1)).expect("a")
        ));
        assert!(Arc::ptr_eq(
            &b_before,
            state.rows.get(&TableId::new(2)).expect("b")
        ));
    }

    #[test]
    fn a_lazy_stream_error_marks_its_sql_transaction_failed() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE stream_failures (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(&mut session, "BEGIN", &[]);
        let failure_flag = match &session.sql_transaction {
            SqlTransactionState::Active(transaction) => Arc::clone(&transaction.stream_failed),
            _ => panic!("expected active SQL transaction"),
        };
        let mut stream = TryQueryStream {
            state: TryQueryStreamState::Events(
                vec![Err(DbError::new("53200", "query memory limit exceeded"))].into_iter(),
            ),
            failed: false,
            failure_flag: Some(failure_flag),
            cancellation: None,
            execution_memory_peak_bytes: None,
        };

        assert_eq!(
            stream
                .next()
                .expect("stream error")
                .expect_err("error")
                .sql_state,
            "53200"
        );
        assert_eq!(session.transaction_status(), TransactionStatus::Failed);
        assert_eq!(
            session
                .execute("SELECT * FROM stream_failures", &[])
                .expect_err("failed transaction")
                .sql_state,
            "25P02"
        );
        execute(&mut session, "ROLLBACK", &[]);
        assert_eq!(session.transaction_status(), TransactionStatus::Idle);
    }

    #[test]
    fn durable_commits_trigger_the_conservative_automatic_checkpoint() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        engine
            .commits_since_checkpoint
            .store(AUTOMATIC_CHECKPOINT_INTERVAL - 1, Ordering::Release);
        execute(
            &mut session,
            "CREATE TABLE checkpoint_rows (id BIGINT PRIMARY KEY)",
            &[],
        );

        let records = engine.wal.scan().expect("scan WAL").records;
        assert!(
            records
                .iter()
                .any(|record| { record.kind() == ordadb_transaction::RecordKind::CheckpointEnd })
        );
        assert!(records.iter().any(|record| {
            matches!(
                record.payload(),
                ordadb_transaction::WalPayload::CheckpointBegin(checkpoint)
                    if checkpoint.visibility_horizon.is_some()
            )
        }));
        assert_eq!(
            engine.writer.active_transaction().expect("writer state"),
            None
        );
        assert_eq!(engine.commits_since_checkpoint.load(Ordering::Acquire), 0);
    }
}
