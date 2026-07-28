//! SQL execution, transaction coordination, and durable publication for OrdaDB.
//!
//! This crate owns SQL semantics and candidate-state atomicity. Physical page
//! encoding belongs to `ordadb-storage`; WAL and crash recovery belong to
//! `ordadb-transaction`.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use ordadb_catalog::{
    Catalog, CatalogExpression, CatalogObjectRef, ColumnStatistics, ConstraintKind, DropBehavior,
    IndexMethod, NewColumn, NewRoutine, NewView, ReferentialAction, SequenceAlteration,
    TableDefinition, TableStatistics, TriggerEvent, TriggerTiming, ViewKind, indexable_type,
};
use ordadb_execution::{
    AdvancedExecutionCursor, AdvancedExecutionPlan, ExecutionContext, ExecutionCursor,
    coerce_value as coerce_execution_value, evaluate as evaluate_scalar,
    predicate_matches as execution_predicate_matches,
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
    ParsedStatement, SqlDialect, bind, bind_catalog_expression,
    bind_catalog_expression_with_parameter_types, parse, parse_with_dialect,
};
use ordadb_storage::{ApplyPoint, DatabaseStore, DurabilityBarrier, PersistentState, encode_row};
use ordadb_transaction::{
    CheckpointState, FaultInjector, FaultPoint, NoFaultInjector, TransactionId, WalManager,
    WriterCoordinator, WriterLease,
};
use ordadb_types::{
    Batch, CommandComplete, DbError, Field, Identifier, IndexId, QueryEvent, QueryProgress, Result,
    Row, ScalarType, Schema, SequenceId, TableId, Value, ViewId,
};
use serde::{Deserialize, Serialize};

const AUTOMATIC_CHECKPOINT_INTERVAL: u64 = 64;
const MAX_REFERENTIAL_ACTIONS: usize = 16_384;
const PLPGSQL_EXECUTION_STACK_BYTES: usize = 8 * 1024 * 1024;
pub const LOGICAL_SNAPSHOT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
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
        Self {
            data_dir: data_dir.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Engine {
    config: Arc<EngineConfig>,
    state: Arc<RwLock<DatabaseState>>,
    store: Arc<Mutex<DatabaseStore>>,
    wal: Arc<WalManager>,
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
        let wal = WalManager::open_with_fault_injector(&config.data_dir, fault_injector)?;
        wal.recover(&config.data_dir)?;
        let writer = WriterCoordinator::from_last_transaction_id(wal.last_transaction_id()?)?;
        let barrier: Arc<dyn DurabilityBarrier> = wal.clone();
        let store = DatabaseStore::open_with_barrier(&config.data_dir, barrier)?;
        let state = DatabaseState::from_persistent(store.committed_state().clone())?;
        Ok(Self {
            config: Arc::new(config),
            state: Arc::new(RwLock::new(state)),
            store: Arc::new(Mutex::new(store)),
            wal,
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
            wal: Arc::clone(&self.wal),
            writer: Arc::clone(&self.writer),
            commits_since_checkpoint: Arc::clone(&self.commits_since_checkpoint),
            sql_transaction: SqlTransactionState::Idle,
            sequence_currvals: BTreeMap::new(),
            options,
        })
    }

    pub fn checkpoint(&self) -> Result<()> {
        checkpoint_shared(&self.state, &self.store, &self.wal, &self.writer)?;
        self.commits_since_checkpoint.store(0, Ordering::Release);
        Ok(())
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
        let transaction_id = self.writer.next_transaction_id()?;
        let mut lease = self.writer.try_acquire(transaction_id)?;
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        persist_candidate(
            &mut state,
            &self.store,
            &self.wal,
            transaction_id,
            candidate,
        )?;
        drop(state);
        lease.release();
        record_commit_and_maybe_checkpoint(
            &self.state,
            &self.store,
            &self.wal,
            &self.writer,
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
    wal: Arc<WalManager>,
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
    Active(ActiveSqlTransaction),
    Failed,
}

#[derive(Debug)]
struct ActiveSqlTransaction {
    transaction_id: TransactionId,
    working: Option<DatabaseState>,
    lease: Option<WriterLease>,
    stream_failed: Arc<AtomicBool>,
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
                ParsedStatement::Rollback => self.rollback_sql_transaction(),
                _ => Err(failed_transaction_error()),
            };
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
            BoundStatement::Begin => return self.begin_sql_transaction(),
            BoundStatement::Commit => return self.commit_sql_transaction(),
            BoundStatement::Rollback => return self.rollback_sql_transaction(),
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
                        let stream =
                            stream.with_failure_flag(Arc::clone(&transaction.stream_failed));
                        self.sql_transaction = SqlTransactionState::Active(transaction);
                        Ok(stream)
                    }
                    Err(error) => {
                        self.sql_transaction = SqlTransactionState::Failed;
                        Err(error)
                    }
                }
            }
            SqlTransactionState::Failed => {
                self.sql_transaction = SqlTransactionState::Failed;
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
        Ok(Transaction {
            state: &self.state,
            store: &self.store,
            wal: &self.wal,
            writer: &self.writer,
            commits_since_checkpoint: &self.commits_since_checkpoint,
            sequence_currvals: &mut self.sequence_currvals,
            dialect: self.options.dialect,
            transaction_id: self.writer.next_transaction_id()?,
            working: None,
            lease: None,
            failed: false,
        })
    }

    #[must_use]
    pub fn transaction_status(&self) -> TransactionStatus {
        match &self.sql_transaction {
            SqlTransactionState::Idle => TransactionStatus::Idle,
            SqlTransactionState::Active(transaction)
                if transaction.stream_failed.load(Ordering::Acquire) =>
            {
                TransactionStatus::Failed
            }
            SqlTransactionState::Active(_) => TransactionStatus::Active,
            SqlTransactionState::Failed => TransactionStatus::Failed,
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
        if let SqlTransactionState::Active(transaction) = &self.sql_transaction
            && let Some(working) = &transaction.working
        {
            return Ok(working.clone());
        }
        committed_snapshot(&self.state)
    }

    fn begin_sql_transaction(&mut self) -> Result<TryQueryStream> {
        match self.transaction_status() {
            TransactionStatus::Idle => {
                self.sql_transaction = SqlTransactionState::Active(ActiveSqlTransaction {
                    transaction_id: self.writer.next_transaction_id()?,
                    working: None,
                    lease: None,
                    stream_failed: Arc::new(AtomicBool::new(false)),
                });
                Ok(TryQueryStream::new(transaction_events("BEGIN")))
            }
            TransactionStatus::Active => {
                Err(DbError::new("25001", "a transaction is already active")
                    .with_hint("commit or roll back the current transaction first"))
            }
            TransactionStatus::Failed => Err(failed_transaction_error()),
        }
    }

    fn commit_sql_transaction(&mut self) -> Result<TryQueryStream> {
        match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
            SqlTransactionState::Idle => Err(no_active_transaction_error("commit")),
            SqlTransactionState::Failed => {
                self.sql_transaction = SqlTransactionState::Failed;
                Err(failed_transaction_error())
            }
            SqlTransactionState::Active(transaction) => {
                let transaction_id = transaction.transaction_id;
                if let Some(candidate) = transaction.working {
                    let mut state = self
                        .state
                        .write()
                        .map_err(|_| internal_error("engine state lock is poisoned"))?;
                    if let Err(error) = persist_candidate(
                        &mut state,
                        &self.store,
                        &self.wal,
                        transaction_id,
                        candidate,
                    ) {
                        self.sql_transaction = SqlTransactionState::Failed;
                        return Err(error);
                    }
                    drop(state);
                    drop(transaction.lease);
                    record_commit_and_maybe_checkpoint(
                        &self.state,
                        &self.store,
                        &self.wal,
                        &self.writer,
                        &self.commits_since_checkpoint,
                    )?;
                }
                Ok(TryQueryStream::new(transaction_events("COMMIT")))
            }
        }
    }

    fn rollback_sql_transaction(&mut self) -> Result<TryQueryStream> {
        match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
            SqlTransactionState::Idle => Err(no_active_transaction_error("roll back")),
            SqlTransactionState::Active(_) | SqlTransactionState::Failed => {
                Ok(TryQueryStream::new(transaction_events("ROLLBACK")))
            }
        }
    }

    fn execute_auto_commit(
        &mut self,
        sql: &str,
        params: &[Value],
        snapshot: DatabaseState,
        statement: BoundStatement,
    ) -> Result<TryQueryStream> {
        if let Some(stream) = prepare_read_stream(&snapshot, statement.clone(), params)? {
            return Ok(stream);
        }
        let sequence_id = sequence_mutation_id(&statement);
        let (_, events, dirty) = execute_bound_candidate(&snapshot, statement, params)?;
        if !dirty {
            return Ok(TryQueryStream::new(events));
        }

        let transaction_id = self.writer.next_transaction_id()?;
        let lease = self.writer.try_acquire(transaction_id)?;
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let mut committed = state.clone();
        committed.cancellation = snapshot.cancellation.clone();
        let (candidate, events, dirty) =
            execute_candidate(&committed, sql, params, self.options.dialect)?;
        if dirty {
            let sequence_value = sequence_id
                .map(|sequence_id| candidate_sequence_value(&candidate, sequence_id))
                .transpose()?;
            persist_candidate(
                &mut state,
                &self.store,
                &self.wal,
                transaction_id,
                candidate,
            )?;
            drop(state);
            drop(lease);
            record_commit_and_maybe_checkpoint(
                &self.state,
                &self.store,
                &self.wal,
                &self.writer,
                &self.commits_since_checkpoint,
            )?;
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
        if let Some(stream) = prepare_read_stream(&snapshot, statement.clone(), params)? {
            return Ok(stream);
        }
        let sequence_id = sequence_mutation_id(&statement);
        let (candidate, events, dirty) = execute_bound_candidate(&snapshot, statement, params)?;
        if !dirty {
            return Ok(TryQueryStream::new(events));
        }
        if transaction.working.is_some() {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            transaction.working = Some(candidate);
            return Ok(TryQueryStream::new(events));
        }

        let lease = self.writer.try_acquire(transaction.transaction_id)?;
        let mut committed = committed_snapshot(&self.state)?;
        committed.cancellation = snapshot.cancellation.clone();
        let (candidate, events, dirty) =
            execute_candidate(&committed, sql, params, self.options.dialect)?;
        if dirty {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            transaction.working = Some(candidate);
            transaction.lease = Some(lease);
        }
        Ok(TryQueryStream::new(events))
    }

    fn fail_sql_transaction(&mut self) {
        if matches!(&self.sql_transaction, SqlTransactionState::Active(_)) {
            self.sql_transaction = SqlTransactionState::Failed;
        }
    }

    fn normalize_sql_transaction_failure(&mut self) {
        if self.transaction_status() == TransactionStatus::Failed
            && matches!(&self.sql_transaction, SqlTransactionState::Active(_))
        {
            self.sql_transaction = SqlTransactionState::Failed;
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
        BoundStatement::Begin
        | BoundStatement::Commit
        | BoundStatement::Rollback
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
    wal: &'session Arc<WalManager>,
    writer: &'session Arc<WriterCoordinator>,
    commits_since_checkpoint: &'session Arc<AtomicU64>,
    sequence_currvals: &'session mut BTreeMap<SequenceId, i64>,
    dialect: SqlDialect,
    transaction_id: TransactionId,
    working: Option<DatabaseState>,
    lease: Option<WriterLease>,
    failed: bool,
}

impl Transaction<'_> {
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        if self.failed {
            return Err(failed_transaction_error());
        }
        match self.execute_inner(sql, params) {
            Ok(stream) => Ok(stream),
            Err(error) => {
                self.working = None;
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
            return Ok(());
        };
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        persist_candidate(
            &mut state,
            self.store,
            self.wal,
            self.transaction_id,
            candidate,
        )?;
        drop(state);
        self.lease = None;
        record_commit_and_maybe_checkpoint(
            self.state,
            self.store,
            self.wal,
            self.writer,
            self.commits_since_checkpoint,
        )
    }

    pub fn rollback(self) -> Result<()> {
        Ok(())
    }

    fn execute_inner(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        let snapshot = match &self.working {
            Some(working) => working.clone(),
            None => committed_snapshot(self.state)?,
        };
        let statement = resolve_sequence_currval(
            bind(parse_with_dialect(sql, self.dialect)?, &snapshot.catalog)?,
            self.sequence_currvals,
        )?;
        if matches!(
            &statement,
            BoundStatement::Begin | BoundStatement::Commit | BoundStatement::Rollback
        ) {
            return Err(DbError::new(
                "25001",
                "SQL transaction control is not allowed inside Session::begin",
            )
            .with_hint("use Transaction::commit or Transaction::rollback"));
        }
        if let Some(stream) = prepare_read_stream(&snapshot, statement.clone(), params)? {
            return stream.collect::<Result<Vec<_>>>().map(QueryStream::new);
        }
        let sequence_id = sequence_mutation_id(&statement);
        let (candidate, events, dirty) = execute_bound_candidate(&snapshot, statement, params)?;
        if !dirty {
            return Ok(QueryStream::new(events));
        }
        if self.working.is_some() {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            self.working = Some(candidate);
            return Ok(QueryStream::new(events));
        }

        let lease = self.writer.try_acquire(self.transaction_id)?;
        let committed = committed_snapshot(self.state)?;
        let (candidate, events, dirty) = execute_candidate(&committed, sql, params, self.dialect)?;
        if dirty {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            self.working = Some(candidate);
            self.lease = Some(lease);
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
            execution_memory_peak_bytes: None,
        }
    }

    fn select(schema: Schema, cursor: StreamBatchCursor) -> Self {
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
    fn from_persistent(state: PersistentState) -> Result<Self> {
        let PersistentState {
            generation,
            catalog,
            tables,
            indexes,
        } = state;
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
        let searches = SearchCatalog::build(&catalog, &rows, SearchLimits::default())?;
        Ok(Self {
            catalog: Arc::new(catalog),
            rows,
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
            indexes: BTreeMap::new(),
            searches: Arc::new(SearchCatalog::default()),
            generation: snapshot.source_generation,
            trigger_depth: 0,
            triggers_fired: 0,
            routine_depth: 0,
            cancellation: None,
        };
        validate_database_rows(&state)?;
        for table_id in catalog_table_ids {
            rebuild_table_derived(&mut state, table_id)?;
        }
        Ok(state)
    }
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

fn prepare_read_stream(
    state: &DatabaseState,
    statement: BoundStatement,
    params: &[Value],
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
            )?;
            Ok(Some(TryQueryStream::select(
                schema,
                StreamBatchCursor::Simple(Box::new(cursor)),
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
            )))
        }
        _ => Ok(None),
    }
}

fn execute_bound_candidate(
    state: &DatabaseState,
    statement: BoundStatement,
    params: &[Value],
) -> Result<(DatabaseState, Vec<QueryEvent>, bool)> {
    let mut candidate = state.clone();
    candidate.trigger_depth = 0;
    candidate.triggers_fired = 0;
    candidate.routine_depth = 0;
    let (events, dirty) = execute_bound(&mut candidate, statement, params)?;
    candidate.cancellation = None;
    Ok((candidate, events, dirty))
}

fn execute_candidate(
    state: &DatabaseState,
    sql: &str,
    params: &[Value],
    dialect: SqlDialect,
) -> Result<(DatabaseState, Vec<QueryEvent>, bool)> {
    let parsed = parse_with_dialect(sql, dialect)?;
    let statement = bind(parsed, &state.catalog)?;
    let mut candidate = state.clone();
    candidate.trigger_depth = 0;
    candidate.triggers_fired = 0;
    candidate.routine_depth = 0;
    let (events, dirty) = execute_bound(&mut candidate, statement, params)?;
    candidate.cancellation = None;
    Ok((candidate, events, dirty))
}

fn persist_candidate(
    state: &mut DatabaseState,
    store: &Arc<Mutex<DatabaseStore>>,
    wal: &Arc<WalManager>,
    transaction_id: TransactionId,
    mut candidate: DatabaseState,
) -> Result<()> {
    candidate.generation = state.generation.checked_add(1).ok_or_else(|| {
        DbError::new("54000", "database generation space is exhausted")
            .with_hint("create a logical backup before retrying on a fresh database")
    })?;
    let persistent = PersistentState::from(&candidate);
    let mut store = store
        .lock()
        .map_err(|_| internal_error("database store lock is poisoned"))?;
    let mut prepared = store.prepare_commit(&persistent)?;
    let logged = wal.log_prepared(transaction_id, &mut prepared)?;
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
    *state = candidate;
    Ok(())
}

fn checkpoint_shared(
    state: &Arc<RwLock<DatabaseState>>,
    store: &Arc<Mutex<DatabaseStore>>,
    wal: &Arc<WalManager>,
    writer: &Arc<WriterCoordinator>,
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
    if let Some(transaction_id) = writer.active_transaction()?
        && let Some(last_lsn) = wal.last_lsn(transaction_id)?
    {
        active_transactions.insert(transaction_id, last_lsn);
    }
    wal.checkpoint(CheckpointState {
        active_transactions,
        dirty_pages: wal.dirty_pages()?,
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
    writer: &Arc<WriterCoordinator>,
    commits_since_checkpoint: &AtomicU64,
) -> Result<()> {
    let count = commits_since_checkpoint
        .fetch_add(1, Ordering::AcqRel)
        .checked_add(1)
        .ok_or_else(|| DbError::new("54000", "automatic checkpoint commit counter overflowed"))?;
    if count < AUTOMATIC_CHECKPOINT_INTERVAL {
        return Ok(());
    }
    checkpoint_shared(state, store, wal, writer)?;
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
        BoundStatement::Begin | BoundStatement::Commit | BoundStatement::Rollback => {
            Err(DbError::new(
                "25000",
                "transaction control was not routed through the session",
            )
            .with_hint("execute transaction control through Session"))
        }
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
            BoundStatement::Begin | BoundStatement::Commit | BoundStatement::Rollback
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
    let (schema, mut cursor) = prepare_select_cursor(state, execution, params)?;
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
    let cursor = ExecutionCursor::new(&plan, &context, schema.clone())?;
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
    fn commits_rolls_back_and_rejects_competing_writers() {
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
        let error = second
            .execute("INSERT INTO documents VALUES (3, 'blocked', 3)", &[])
            .expect_err("competing writer");
        assert_eq!(error.sql_state, "55P03");
        transaction.rollback().expect("rollback writer");
        execute(
            &mut second,
            "INSERT INTO documents VALUES (3, 'after release', 3)",
            &[],
        );
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
    fn fallible_stream_owns_a_lazy_arc_snapshot() {
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
        execute(&mut session, "INSERT INTO lazy_items VALUES (3)", &[]);

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
        assert_eq!(
            engine.writer.active_transaction().expect("writer state"),
            None
        );
        assert_eq!(engine.commits_since_checkpoint.load(Ordering::Acquire), 0);
    }
}
