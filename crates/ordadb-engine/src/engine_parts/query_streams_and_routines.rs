
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
            _event_reservation: None,
        }
    }

    fn buffered(events: Vec<QueryEvent>) -> Result<Self> {
        let memory = MemoryGrant::new(DEFAULT_SOFT_MEMORY_BYTES, DEFAULT_HARD_MEMORY_BYTES)?;
        let bytes = events.iter().try_fold(0usize, |total, event| {
            let QueryEvent::Batch(batch) = event else {
                return Ok(total);
            };
            batch.rows.iter().try_fold(total, |total, row| {
                total.checked_add(estimated_row_bytes(row)).ok_or_else(|| {
                    DbError::new("53200", "query memory limit exceeded")
                        .with_detail("RETURNING row memory accounting overflow")
                })
            })
        })?;
        if bytes == 0 {
            return Ok(Self::new(events));
        }
        let reservation = memory.try_reserve(bytes)?;
        let peak = memory.peak_bytes();
        Ok(Self {
            state: TryQueryStreamState::Events(
                events.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
            ),
            failed: false,
            failure_flag: None,
            cancellation: None,
            execution_memory_peak_bytes: Some(peak),
            _event_reservation: Some(reservation),
        })
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
            _event_reservation: None,
        }
    }

    fn with_failure_flag(mut self, failure_flag: Arc<AtomicBool>) -> Self {
        self.failure_flag = Some(failure_flag);
        self
    }

    /// Returns the highest number of query-accounted bytes observed by this
    /// SELECT or row-returning DML stream. Other statements return `None`.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoutineFrameId(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoutineFrameKind {
    Routine(ordadb_types::RoutineId),
    Trigger(ordadb_types::TriggerId),
}

#[derive(Debug, Clone)]
struct RoutineFrame {
    kind: RoutineFrameKind,
}

#[derive(Debug, Clone, Default)]
struct RoutineFrameStack {
    arena: Vec<Option<RoutineFrame>>,
    free: Vec<usize>,
    active: Vec<RoutineFrameId>,
}

impl RoutineFrameStack {
    fn push_routine(&mut self, routine_id: ordadb_types::RoutineId) -> Result<RoutineFrameId> {
        self.push(RoutineFrameKind::Routine(routine_id))
    }

    fn push_trigger(&mut self, trigger_id: ordadb_types::TriggerId) -> Result<RoutineFrameId> {
        self.push(RoutineFrameKind::Trigger(trigger_id))
    }

    fn push(&mut self, kind: RoutineFrameKind) -> Result<RoutineFrameId> {
        let (current, maximum, label) = match kind {
            RoutineFrameKind::Routine(_) => (
                self.active_kind_count(|kind| matches!(kind, RoutineFrameKind::Routine(_))),
                MAX_ROUTINE_FRAMES,
                "routine-call",
            ),
            RoutineFrameKind::Trigger(_) => (
                self.active_kind_count(|kind| matches!(kind, RoutineFrameKind::Trigger(_))),
                MAX_TRIGGER_FRAMES,
                "trigger",
            ),
        };
        if current >= maximum {
            return Err(DbError::new(
                "54001",
                format!("PL/pgSQL {label} depth exceeds the maximum of {maximum}"),
            ));
        }
        let frame = RoutineFrame { kind };
        let index = if let Some(index) = self.free.pop() {
            self.arena[index] = Some(frame);
            index
        } else {
            let index = self.arena.len();
            self.arena.push(Some(frame));
            index
        };
        let id = RoutineFrameId(index);
        self.active.push(id);
        Ok(id)
    }

    fn pop(&mut self, id: RoutineFrameId) -> Result<()> {
        if self.active.last().copied() != Some(id) {
            return Err(internal_error("PL/pgSQL routine frame stack is not LIFO"));
        }
        self.active.pop();
        let frame = self
            .arena
            .get_mut(id.0)
            .and_then(Option::take)
            .ok_or_else(|| internal_error("PL/pgSQL routine frame is missing"))?;
        let _ = frame.kind;
        self.free.push(id.0);
        Ok(())
    }

    fn active_kind_count(&self, matches: impl Fn(RoutineFrameKind) -> bool) -> usize {
        self.active
            .iter()
            .filter_map(|id| self.arena.get(id.0).and_then(Option::as_ref))
            .filter(|frame| matches(frame.kind))
            .count()
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

enum RoutineCompletion {
    Root,
    Call { schema: Schema },
    Select { schema: Schema, returns_set: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcedureBoundary {
    Commit(TransactionChain),
    Rollback(TransactionChain),
}

type ProcedureBoundaryHandler<'a> =
    dyn FnMut(ProcedureBoundary, &mut DatabaseState, bool) -> Result<()> + 'a;

struct RoutineVmFrame {
    id: RoutineFrameId,
    routine: RoutineDefinition,
    machine: VmMachine,
    response: Option<Result<VmSqlStream>>,
    completion: RoutineCompletion,
    exception_states: Vec<DatabaseState>,
    exception_triggers: Vec<Option<TriggerRowSavepoint>>,
    exception_charges: Vec<usize>,
    exception_memory: VmMemoryReservation,
}

struct RoutineCompletionStream {
    events: std::vec::IntoIter<QueryEvent>,
    _memory: Option<VmMemoryHold>,
}

impl Iterator for RoutineCompletionStream {
    type Item = Result<QueryEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.events.next().map(Ok)
    }
}

#[derive(Debug, Clone, Default)]
struct DatabaseState {
    catalog: Arc<Catalog>,
    rows: BTreeMap<TableId, Arc<Vec<Row>>>,
    system_catalog: Option<Arc<system_catalog::SystemCatalogSnapshot>>,
    versions: BTreeMap<TableId, Arc<Vec<VersionedRow>>>,
    visible_versions: BTreeMap<TableId, Arc<Vec<u32>>>,
    indexes: BTreeMap<IndexId, Arc<BPlusTree>>,
    searches: Arc<SearchCatalog>,
    generation: u64,
    triggers_fired: usize,
    routine_frames: RoutineFrameStack,
    pending_notices: Vec<DbNotice>,
    pending_notifications: NotificationTransactionState,
    cancellation: Option<Arc<AtomicBool>>,
    authorization: Option<SessionAuthorization>,
    sequence_currvals: BTreeMap<SequenceId, i64>,
}

struct SelectExecution {
    table_id: TableId,
    schema: Schema,
    projection: Vec<BoundProjection>,
    filter: Option<BoundExpr>,
    order_by: Vec<BoundOrder>,
    offset: Option<BoundExpr>,
    limit: Option<BoundExpr>,
}

struct SetExecution {
    left: Box<BoundStatement>,
    operator: QuerySetOperator,
    all: bool,
    right: Box<BoundStatement>,
    schema: Schema,
    order_by: Vec<BoundOrder>,
    offset: Option<BoundExpr>,
    limit: Option<BoundExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SetRowKey(Vec<SetValueKey>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SetValueKey {
    Null,
    Boolean(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(u32),
    Float64(u64),
    Decimal(String),
    Text(String),
    Binary(Vec<u8>),
    Date(String),
    Time(String),
    Timestamp(String),
    Interval(i32, i32, i64),
    Array(String),
    Jsonb(String),
    Uuid([u8; 16]),
    Vector(Vec<u32>),
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
    applies: Vec<BoundApply>,
    windows: Vec<BoundWindow>,
    schema: Schema,
    projection: Vec<BoundProjection>,
    distinct: bool,
    filter: Option<BoundExpr>,
    group_by: Vec<BoundExpr>,
    having: Option<BoundExpr>,
    order_by: Vec<BoundOrder>,
    offset: Option<BoundExpr>,
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
                system_catalog: None,
                versions: version_rows,
                visible_versions,
                indexes: BTreeMap::new(),
                searches: Arc::new(SearchCatalog::default()),
                generation,
                triggers_fired: 0,
                routine_frames: RoutineFrameStack::default(),
                pending_notices: Vec::new(),
                pending_notifications: NotificationTransactionState::default(),
                cancellation: None,
                authorization: None,
                sequence_currvals: BTreeMap::new(),
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
            system_catalog: None,
            versions,
            visible_versions,
            indexes,
            searches: Arc::new(searches),
            generation,
            triggers_fired: 0,
            routine_frames: RoutineFrameStack::default(),
            pending_notices: Vec::new(),
            pending_notifications: NotificationTransactionState::default(),
            cancellation: None,
            authorization: None,
            sequence_currvals: BTreeMap::new(),
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
            system_catalog: None,
            versions: BTreeMap::new(),
            visible_versions: BTreeMap::new(),
            indexes: BTreeMap::new(),
            searches: Arc::new(SearchCatalog::default()),
            generation: snapshot.source_generation,
            triggers_fired: 0,
            routine_frames: RoutineFrameStack::default(),
            pending_notices: Vec::new(),
            pending_notifications: NotificationTransactionState::default(),
            cancellation: None,
            authorization: None,
            sequence_currvals: BTreeMap::new(),
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
    system_catalog: Option<&'a system_catalog::SystemCatalogSnapshot>,
}

impl<'a> StorageTableProviderV2<'a> {
    fn new(
        store: Arc<Mutex<DatabaseStore>>,
        storage_access: Arc<StorageAccessGate>,
        generation: u64,
        rows: &'a BTreeMap<TableId, Arc<Vec<Row>>>,
        system_catalog: Option<&'a system_catalog::SystemCatalogSnapshot>,
    ) -> Self {
        Self {
            store,
            storage_access,
            generation,
            rows,
            system_catalog,
        }
    }
}

impl TableProvider for StorageTableProviderV2<'_> {
    fn scan(&self, table_id: TableId) -> Result<Box<dyn TableScan>> {
        if Catalog::is_system_table(table_id) {
            return self
                .system_catalog
                .ok_or_else(|| internal_error("system catalog snapshot is unavailable"))?
                .scan(table_id);
        }
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
