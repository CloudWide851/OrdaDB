
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
            notifications: Arc::new(NotificationBroker::default()),
        })
    }

    pub fn connect(&self) -> Result<Session> {
        self.connect_with_options(SessionOptions::default())
    }

    pub fn connect_with_options(&self, options: SessionOptions) -> Result<Session> {
        self.connect_session(options, None)
    }

    pub fn connect_authenticated(&self, authorization: SessionAuthorization) -> Result<Session> {
        self.connect_authenticated_with_options(SessionOptions::default(), authorization)
    }

    pub fn connect_authenticated_with_options(
        &self,
        options: SessionOptions,
        authorization: SessionAuthorization,
    ) -> Result<Session> {
        self.connect_session(options, Some(authorization))
    }

    fn connect_session(
        &self,
        options: SessionOptions,
        authorization: Option<SessionAuthorization>,
    ) -> Result<Session> {
        let session_user = authorization
            .as_ref()
            .map_or("ordadb", |authorization| authorization.owner().as_str());
        let runtime_metadata = SessionRuntimeMetadata::postgres_compatible(
            "18",
            "ordadb",
            session_user,
            session_user,
        )?;
        let notification_session_id = self.notifications.register();
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
            notifications: Arc::clone(&self.notifications),
            notification_session_id,
            sql_transaction: SqlTransactionState::Idle,
            sequence_currvals: BTreeMap::new(),
            options,
            execution_options: ExecutionOptions::default(),
            authorization,
            runtime_metadata,
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
    notifications: Arc<NotificationBroker>,
    notification_session_id: u64,
    sql_transaction: SqlTransactionState,
    sequence_currvals: BTreeMap<SequenceId, i64>,
    options: SessionOptions,
    execution_options: ExecutionOptions,
    authorization: Option<SessionAuthorization>,
    runtime_metadata: SessionRuntimeMetadata,
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
    notification_state: NotificationTransactionState,
}

#[derive(Debug, Clone)]
struct SqlSavepointState {
    base: Option<DatabaseState>,
    working: Option<DatabaseState>,
    sequence_currvals: BTreeMap<SequenceId, i64>,
    lock_len: usize,
    dml_only: bool,
    ssi: Option<SsiSavepoint>,
    notification_state: NotificationTransactionState,
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

struct ProcedureTransactionCoordinator {
    state: Arc<RwLock<DatabaseState>>,
    store: Arc<Mutex<DatabaseStore>>,
    storage_access: Arc<StorageAccessGate>,
    wal: Arc<WalManager>,
    transaction_status: Arc<TransactionStatusStore>,
    transactions: Arc<TransactionManager>,
    locks: Arc<LockManager>,
    writer: Arc<WriterCoordinator>,
    commits_since_checkpoint: Arc<AtomicU64>,
    notifications: Arc<NotificationBroker>,
    notification_session_id: u64,
    authorization: Option<SessionAuthorization>,
    cancellation: Option<Arc<AtomicBool>>,
    base: DatabaseState,
    transaction: Option<DurableTransaction>,
    locks_held: Vec<LockGuard>,
    lease: Option<WriterLease>,
    notices: Vec<DbNotice>,
    sequence_currvals: BTreeMap<SequenceId, i64>,
}

impl ProcedureTransactionCoordinator {
    fn new(session: &Session, base: DatabaseState) -> Result<Self> {
        let mut coordinator = Self {
            state: Arc::clone(&session.state),
            store: Arc::clone(&session.store),
            storage_access: Arc::clone(&session.storage_access),
            wal: Arc::clone(&session.wal),
            transaction_status: Arc::clone(&session.transaction_status),
            transactions: Arc::clone(&session.transactions),
            locks: Arc::clone(&session.locks),
            writer: Arc::clone(&session.writer),
            commits_since_checkpoint: Arc::clone(&session.commits_since_checkpoint),
            notifications: Arc::clone(&session.notifications),
            notification_session_id: session.notification_session_id,
            authorization: session.authorization.clone(),
            cancellation: base.cancellation.clone(),
            base,
            transaction: None,
            locks_held: Vec::new(),
            lease: None,
            notices: Vec::new(),
            sequence_currvals: session.sequence_currvals.clone(),
        };
        coordinator.start_segment(TransactionCharacteristics::default())?;
        let committed = committed_snapshot(&coordinator.state)?;
        coordinator.base = coordinator.prepare_candidate(committed);
        Ok(coordinator)
    }

    fn prepare_candidate(&self, mut candidate: DatabaseState) -> DatabaseState {
        candidate.triggers_fired = 0;
        candidate.routine_frames.clear();
        candidate.pending_notices.clear();
        candidate.pending_notifications = NotificationTransactionState::default();
        candidate.cancellation = self.cancellation.clone();
        candidate.authorization = self.authorization.clone();
        candidate.sequence_currvals = self.sequence_currvals.clone();
        candidate
    }

    fn start_segment(&mut self, characteristics: TransactionCharacteristics) -> Result<()> {
        let transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            characteristics,
        )?;
        let lease = self.writer.try_acquire(transaction.transaction_id())?;
        let lock = acquire_compatibility_write_lock(
            &self.locks,
            &transaction,
            self.cancellation.as_deref(),
        )?;
        self.transaction = Some(transaction);
        self.lease = Some(lease);
        self.locks_held.push(lock);
        Ok(())
    }

    fn boundary(
        &mut self,
        boundary: ProcedureBoundary,
        candidate: &mut DatabaseState,
        dirty: bool,
    ) -> Result<()> {
        let characteristics = self
            .transaction
            .as_ref()
            .and_then(DurableTransaction::characteristics)
            .ok_or_else(|| no_active_transaction_error("end a procedure transaction"))?;
        match boundary {
            ProcedureBoundary::Commit(_) => self.finish_segment(candidate, dirty, true)?,
            ProcedureBoundary::Rollback(_) => self.finish_segment(candidate, dirty, false)?,
        }
        let next_characteristics = match boundary {
            ProcedureBoundary::Commit(TransactionChain::Chain)
            | ProcedureBoundary::Rollback(TransactionChain::Chain) => characteristics,
            ProcedureBoundary::Commit(TransactionChain::NoChain)
            | ProcedureBoundary::Commit(TransactionChain::Default)
            | ProcedureBoundary::Rollback(TransactionChain::NoChain)
            | ProcedureBoundary::Rollback(TransactionChain::Default) => {
                TransactionCharacteristics::default()
            }
        };
        let runtime_frames = candidate.routine_frames.clone();
        let committed = committed_snapshot(&self.state)?;
        self.base = self.prepare_candidate(committed);
        *candidate = self.base.clone();
        candidate.routine_frames = runtime_frames;
        self.start_segment(next_characteristics)
    }

    fn finish_final(&mut self, candidate: &mut DatabaseState, dirty: bool) -> Result<()> {
        self.finish_segment(candidate, dirty, true)
    }

    fn abort(&mut self) {
        self.notices
            .append(&mut mem::take(&mut self.base.pending_notices));
        if let Some(transaction) = self.transaction.take() {
            let _ = transaction.abort();
        }
        self.locks_held.clear();
        self.lease.take();
    }

    fn runtime_sequence_currvals(&self) -> BTreeMap<SequenceId, i64> {
        self.sequence_currvals.clone()
    }

    fn finish_segment(
        &mut self,
        candidate: &mut DatabaseState,
        dirty: bool,
        commit: bool,
    ) -> Result<()> {
        self.notices
            .append(&mut mem::take(&mut candidate.pending_notices));
        let pending_notifications = mem::take(&mut candidate.pending_notifications);
        let mut transaction = self
            .transaction
            .take()
            .ok_or_else(|| no_active_transaction_error("end a procedure transaction"))?;
        if commit {
            if dirty {
                reconcile_version_changes(
                    &self.base,
                    candidate,
                    version_mutation_context(&transaction)?,
                )?;
                let mut durable_candidate = candidate.clone();
                durable_candidate.routine_frames.clear();
                durable_candidate.pending_notices.clear();
                durable_candidate.pending_notifications = NotificationTransactionState::default();
                durable_candidate.cancellation = None;
                durable_candidate.authorization = None;
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
                    durable_candidate,
                )?;
                drop(state);
            } else {
                transaction.commit_empty()?;
            }
            self.sequence_currvals = candidate.sequence_currvals.clone();
        } else {
            transaction.abort()?;
        }
        self.locks_held.clear();
        self.lease.take();
        if commit {
            self.notifications
                .commit(self.notification_session_id, pending_notifications);
            if dirty {
                record_commit_and_maybe_checkpoint(
                    &self.state,
                    &self.store,
                    &self.wal,
                    &self.transactions,
                    &self.commits_since_checkpoint,
                )?;
            }
        }
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.notifications.unregister(self.notification_session_id);
    }
}
