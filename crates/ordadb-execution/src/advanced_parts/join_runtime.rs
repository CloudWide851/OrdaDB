
impl JoinedSource {
    fn new(
        plan: &AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
        options: &ExecutionOptions,
    ) -> Result<Self> {
        let base = context
            .tables
            .get(&plan.table.table_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()));
        let joins = plan
            .joins
            .iter()
            .map(|join| JoinRuntime::new(join.clone(), context, base.len(), options))
            .collect::<Result<Vec<_>>>()?;
        let frames = (0..joins.len()).map(|_| JoinFrame::default()).collect();
        Ok(Self {
            base,
            base_offset: 0,
            joins,
            prefixes: Vec::new(),
            frames,
            depth: 0,
            nested_memory_peak: 0,
        })
    }

    fn nested_memory_peak(&self) -> usize {
        self.nested_memory_peak
    }

    fn take_unjoined_rows(&mut self) -> Option<Arc<Vec<Row>>> {
        if !self.joins.is_empty() || self.base_offset != 0 {
            return None;
        }
        self.base_offset = self.base.len();
        Some(Arc::clone(&self.base))
    }

    fn next_row(
        &mut self,
        params: &[Value],
        memory: &mut QueryMemoryContext,
        spill: &mut SpillManager,
        expression_stack: &mut ExpressionStack,
    ) -> Result<Option<Row>> {
        if self.joins.is_empty() {
            let row = self.base.get(self.base_offset).cloned();
            self.base_offset = self.base_offset.saturating_add(1);
            return Ok(row);
        }
        if self.prefixes.is_empty() && self.joins.len() == 1 && !self.joins[0].predicate_required {
            match self.next_single_hash_row(memory, spill)? {
                FastJoinStep::Row(row) => return Ok(Some(row)),
                FastJoinStep::Exhausted => return Ok(None),
                FastJoinStep::Fallback => {}
            }
        }
        loop {
            if self.prefixes.is_empty() {
                let Some(base) = self.base.get(self.base_offset).cloned() else {
                    return Ok(None);
                };
                self.base_offset = self.base_offset.saturating_add(1);
                self.prefixes.push(base);
                self.depth = 0;
            }
            if self.depth == self.joins.len() {
                let row = self
                    .prefixes
                    .pop()
                    .ok_or_else(|| DbError::internal("joined row disappeared"))?;
                self.depth = self.depth.saturating_sub(1);
                self.prefixes.truncate(self.depth + 1);
                return Ok(Some(row));
            }

            if !self.frames[self.depth].initialized {
                let prefix = self
                    .prefixes
                    .get(self.depth)
                    .ok_or_else(|| DbError::internal("join prefix is unavailable"))?;
                let candidates =
                    self.joins[self.depth].candidates(prefix, params, memory, spill)?;
                self.frames[self.depth].install(candidates);
            }

            let right =
                self.frames[self.depth].next_candidate(self.joins[self.depth].rows(), memory)?;
            self.nested_memory_peak = self
                .nested_memory_peak
                .max(self.frames[self.depth].nested_memory_peak(memory));
            if let Some(right) = right {
                let mut values = self.prefixes[self.depth].values.clone();
                values.extend(right.values.iter().cloned());
                let joined = Row::new(values);
                let matches = if self.joins[self.depth].predicate_required {
                    match self.joins[self.depth].predicate.evaluate_reusing(
                        &joined.values,
                        params,
                        expression_stack,
                    )? {
                        Value::Boolean(matches) => matches,
                        Value::Null => false,
                        _ => {
                            return Err(DbError::new(
                                "42804",
                                "join predicate must evaluate to boolean",
                            ));
                        }
                    }
                } else {
                    true
                };
                if matches {
                    self.frames[self.depth].matched = true;
                    self.prefixes.truncate(self.depth + 1);
                    self.prefixes.push(joined);
                    self.depth += 1;
                    if self.depth < self.frames.len() {
                        self.frames[self.depth].reset();
                    }
                }
                continue;
            }

            if self.joins[self.depth].kind == JoinKind::Left
                && !self.frames[self.depth].matched
                && !self.frames[self.depth].null_emitted
            {
                self.frames[self.depth].null_emitted = true;
                let mut values = self.prefixes[self.depth].values.clone();
                values.extend(std::iter::repeat_n(
                    Value::Null,
                    self.joins[self.depth].width,
                ));
                self.prefixes.truncate(self.depth + 1);
                self.prefixes.push(Row::new(values));
                self.depth += 1;
                if self.depth < self.frames.len() {
                    self.frames[self.depth].reset();
                }
                continue;
            }

            self.frames[self.depth].reset();
            if self.depth == 0 {
                self.prefixes.clear();
            } else {
                self.prefixes.truncate(self.depth);
                self.depth -= 1;
            }
        }
    }

    fn next_single_hash_row(
        &mut self,
        memory: &mut QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<FastJoinStep> {
        loop {
            let Some(base) = self.base.get(self.base_offset) else {
                return Ok(FastJoinStep::Exhausted);
            };
            self.base_offset = self.base_offset.saturating_add(1);
            let candidates = self.joins[0].candidates(base, &[], memory, spill)?;
            match candidates {
                CandidateSet::Empty | CandidateSet::One { value: None } => {
                    if self.joins[0].kind == JoinKind::Left {
                        let mut values =
                            Vec::with_capacity(base.values.len() + self.joins[0].width);
                        values.extend(base.values.iter().cloned());
                        values.extend(std::iter::repeat_n(Value::Null, self.joins[0].width));
                        return Ok(FastJoinStep::Row(Row::new(values)));
                    }
                }
                CandidateSet::One { value: Some(index) } => {
                    let right = self.joins[0].rows().get(index).ok_or_else(|| {
                        DbError::internal("hash join candidate index is out of bounds")
                    })?;
                    let mut values = Vec::with_capacity(base.values.len() + right.values.len());
                    values.extend(base.values.iter().cloned());
                    values.extend(right.values.iter().cloned());
                    return Ok(FastJoinStep::Row(Row::new(values)));
                }
                candidates => {
                    self.prefixes.push(base.clone());
                    self.frames[0].install(candidates);
                    self.depth = 0;
                    return Ok(FastJoinStep::Fallback);
                }
            }
        }
    }
}

struct JoinRuntime {
    kind: JoinKind,
    width: usize,
    predicate: ExpressionProgram,
    predicate_required: bool,
    source: JoinRuntimeSource,
}

enum JoinRuntimeSource {
    Table {
        rows: Arc<Vec<Row>>,
        lookup: JoinLookup,
    },
    Derived(Box<DerivedJoinRuntime>),
}

struct DerivedJoinRuntime {
    query: QueryExecutionPlan,
    correlation_indexes: Vec<usize>,
    tables: BTreeMap<TableId, Arc<Vec<Row>>>,
    indexes: BTreeMap<IndexId, Arc<BPlusTree>>,
    options: ExecutionOptions,
}

impl JoinRuntime {
    fn new(
        join: JoinExecutionPlan,
        context: &ExecutionContext<'_>,
        left_rows: usize,
        options: &ExecutionOptions,
    ) -> Result<Self> {
        let JoinExecutionPlan { source, kind, on } = join;
        let predicate =
            ExpressionProgram::compile_with_limit(&on, false, options.max_expression_depth)?;
        let (width, predicate_required, source) = match source {
            JoinExecutionSource::Table(table) => {
                let rows = context
                    .tables
                    .get(&table.table_id)
                    .cloned()
                    .unwrap_or_else(|| Arc::new(Vec::new()));
                let equi = equi_join_columns(&on, table.offset)
                    .map(|(left, right)| (left, right - table.offset));
                let strategy =
                    choose_join_strategy(left_rows as u64, rows.len() as u64, equi.is_some())
                        .strategy;
                let lookup = match (strategy, equi) {
                    (JoinStrategy::Hash, Some((left, right))) => JoinLookup::Hash {
                        left,
                        right,
                        state: HashLookup::Uninitialized,
                    },
                    _ => JoinLookup::Nested,
                };
                let predicate_required = !matches!(&lookup, JoinLookup::Hash { .. });
                (
                    table.width,
                    predicate_required,
                    JoinRuntimeSource::Table { rows, lookup },
                )
            }
            JoinExecutionSource::Derived {
                query,
                correlation_indexes,
                width,
                ..
            } => (
                width,
                true,
                JoinRuntimeSource::Derived(Box::new(DerivedJoinRuntime {
                    query: *query,
                    correlation_indexes,
                    tables: context.tables.clone(),
                    indexes: context.indexes.clone(),
                    options: options.clone(),
                })),
            ),
        };
        Ok(Self {
            kind,
            width,
            predicate,
            predicate_required,
            source,
        })
    }

    fn rows(&self) -> &[Row] {
        match &self.source {
            JoinRuntimeSource::Table { rows, .. } => rows,
            JoinRuntimeSource::Derived(_) => &[],
        }
    }

    fn candidates(
        &mut self,
        prefix: &Row,
        params: &[Value],
        memory: &mut QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<CandidateSet> {
        match &mut self.source {
            JoinRuntimeSource::Table { rows, lookup } => match lookup {
                JoinLookup::Nested => Ok(CandidateSet::All {
                    offset: 0,
                    len: rows.len(),
                }),
                JoinLookup::Hash { left, right, state } => {
                    ensure_hash_lookup(state, rows, *right, memory, spill)?;
                    let value = prefix
                        .values
                        .get(*left)
                        .ok_or_else(|| DbError::internal("hash join left key is out of bounds"))?;
                    if value.is_null() {
                        return Ok(CandidateSet::Empty);
                    }
                    match state {
                        HashLookup::Memory { buckets, .. } => {
                            let key = JoinHashKey::new(value)?;
                            let Some(matches) = buckets.get(&key) else {
                                return Ok(CandidateSet::Empty);
                            };
                            match matches {
                                HashBucket::One(value) => Ok(CandidateSet::One {
                                    value: Some(*value),
                                }),
                                HashBucket::Many(matches) => {
                                    let values = matches.clone();
                                    let bytes =
                                        values.len().saturating_mul(std::mem::size_of::<usize>());
                                    let reservation = memory.try_reserve(bytes)?;
                                    Ok(CandidateSet::Indexes {
                                        values,
                                        offset: 0,
                                        _reservation: reservation,
                                    })
                                }
                            }
                        }
                        HashLookup::Spilled { paths } => {
                            let key = encode_hash_value(value)?;
                            let partition = stable_partition(&key, paths.len());
                            let rows = spill.read_matching_rows(
                                &paths[partition],
                                *right,
                                &key,
                                memory,
                            )?;
                            Ok(CandidateSet::Rows {
                                values: rows.values,
                                offset: 0,
                                _reservation: rows.reservation,
                            })
                        }
                        HashLookup::Uninitialized => {
                            Err(DbError::internal("hash join lookup was not initialized"))
                        }
                    }
                }
            },
            JoinRuntimeSource::Derived(derived) => {
                let mut reservation = memory.try_reserve(0)?;
                let mut inner_params = params.to_vec();
                reservation.resize(
                    inner_params
                        .iter()
                        .map(estimated_value_bytes)
                        .sum::<usize>(),
                )?;
                for index in &derived.correlation_indexes {
                    let value = prefix.values.get(*index).cloned().ok_or_else(|| {
                        DbError::internal(format!(
                            "LATERAL outer column index {index} is out of range"
                        ))
                    })?;
                    reservation.grow(estimated_value_bytes(&value))?;
                    inner_params.push(value);
                }
                let context = ExecutionContext {
                    tables: &derived.tables,
                    indexes: &derived.indexes,
                    params: &inner_params,
                };
                let cursor = QueryExecutionCursor::new(
                    derived.query.clone(),
                    &context,
                    nested_execution_options(&derived.options, memory)?,
                )?;
                Ok(CandidateSet::Cursor {
                    cursor: Box::new(cursor),
                    batch: None,
                    offset: 0,
                    _reservation: reservation,
                })
            }
        }
    }
}

enum JoinLookup {
    Nested,
    Hash {
        left: usize,
        right: usize,
        state: HashLookup,
    },
}

enum HashLookup {
    Uninitialized,
    Memory {
        buckets: HashMap<JoinHashKey, HashBucket>,
        _reservation: Reservation,
    },
    Spilled {
        paths: Vec<PathBuf>,
    },
}

enum HashBucket {
    One(usize),
    Many(Vec<usize>),
}

impl HashBucket {
    fn additional_bytes_for_push(&self) -> usize {
        match self {
            Self::One(_) => 2 * std::mem::size_of::<usize>(),
            Self::Many(values) if values.len() == values.capacity() => values
                .capacity()
                .max(1)
                .saturating_mul(std::mem::size_of::<usize>()),
            Self::Many(_) => 0,
        }
    }

    fn push(&mut self, value: usize) {
        match self {
            Self::One(first) => {
                let first = *first;
                *self = Self::Many(vec![first, value]);
            }
            Self::Many(values) => values.push(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum JoinHashKey {
    Boolean(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Encoded(Vec<u8>),
}

impl JoinHashKey {
    fn new(value: &Value) -> Result<Self> {
        match value {
            Value::Boolean(value) => Ok(Self::Boolean(*value)),
            Value::Int16(value) => Ok(Self::Int16(*value)),
            Value::Int32(value) => Ok(Self::Int32(*value)),
            Value::Int64(value) => Ok(Self::Int64(*value)),
            _ => encode_hash_value(value).map(Self::Encoded),
        }
    }

    fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Encoded(value) => value.len(),
                Self::Boolean(_) | Self::Int16(_) | Self::Int32(_) | Self::Int64(_) => 0,
            }
    }
}

fn ensure_hash_lookup(
    state: &mut HashLookup,
    rows: &[Row],
    key_index: usize,
    memory: &mut QueryMemoryContext,
    spill: &mut SpillManager,
) -> Result<()> {
    if !matches!(state, HashLookup::Uninitialized) {
        return Ok(());
    }
    let entry_bytes = std::mem::size_of::<JoinHashKey>()
        .saturating_add(std::mem::size_of::<HashBucket>())
        .saturating_add(16);
    let table_bytes = rows.len().checked_mul(entry_bytes).ok_or_else(|| {
        DbError::new("53200", "query memory limit exceeded")
            .with_detail("hash join table estimate overflow")
    })?;
    if !rows.is_empty() && memory.would_cross_soft_limit(table_bytes) {
        let paths =
            spill.write_partitioned_rows("hash-join", rows, key_index, HASH_PARTITIONS, memory)?;
        *state = HashLookup::Spilled { paths };
        return Ok(());
    }
    let mut reservation = memory.try_reserve(table_bytes)?;
    let mut buckets = HashMap::<JoinHashKey, HashBucket>::new();
    buckets.try_reserve(rows.len()).map_err(|error| {
        DbError::new("53200", "query memory limit exceeded")
            .with_detail(format!("failed to allocate hash join table: {error}"))
    })?;
    for (index, row) in rows.iter().enumerate() {
        let value = row
            .values
            .get(key_index)
            .ok_or_else(|| DbError::internal("hash join right key is out of bounds"))?;
        if value.is_null() {
            continue;
        }
        let key = JoinHashKey::new(value)?;
        let bytes = key
            .estimated_bytes()
            .saturating_sub(std::mem::size_of::<JoinHashKey>());
        if !buckets.is_empty() && memory.would_cross_soft_limit(bytes) {
            let paths = spill.write_partitioned_rows(
                "hash-join",
                rows,
                key_index,
                HASH_PARTITIONS,
                memory,
            )?;
            *state = HashLookup::Spilled { paths };
            return Ok(());
        }
        reservation.grow(bytes)?;
        match buckets.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(HashBucket::One(index));
            }
            Entry::Occupied(mut entry) => {
                reservation.grow(entry.get().additional_bytes_for_push())?;
                entry.get_mut().push(index);
            }
        }
    }
    *state = HashLookup::Memory {
        buckets,
        _reservation: reservation,
    };
    Ok(())
}

#[derive(Default)]
struct JoinFrame {
    candidates: Option<CandidateSet>,
    initialized: bool,
    matched: bool,
    null_emitted: bool,
}

impl JoinFrame {
    fn install(&mut self, candidates: CandidateSet) {
        self.candidates = Some(candidates);
        self.initialized = true;
        self.matched = false;
        self.null_emitted = false;
    }

    fn next_candidate(&mut self, rows: &[Row], memory: &QueryMemoryContext) -> Result<Option<Row>> {
        let Some(candidates) = self.candidates.as_mut() else {
            return Ok(None);
        };
        candidates.next(rows, memory)
    }

    fn nested_memory_peak(&self, memory: &QueryMemoryContext) -> usize {
        self.candidates
            .as_ref()
            .map_or(0, |candidates| candidates.nested_memory_peak(memory))
    }

    fn reset(&mut self) {
        self.candidates = None;
        self.initialized = false;
        self.matched = false;
        self.null_emitted = false;
    }
}

enum CandidateSet {
    Empty,
    One {
        value: Option<usize>,
    },
    All {
        offset: usize,
        len: usize,
    },
    Indexes {
        values: Vec<usize>,
        offset: usize,
        _reservation: Reservation,
    },
    Rows {
        values: Vec<Row>,
        offset: usize,
        _reservation: Reservation,
    },
    Cursor {
        cursor: Box<QueryExecutionCursor>,
        batch: Option<Batch>,
        offset: usize,
        _reservation: Reservation,
    },
}

const WINDOW_SPILL_INDEX_BYTES: u64 = std::mem::size_of::<u64>() as u64;

struct ReservedRow {
    row: Row,
    reservation: Reservation,
}

struct IndexedRowStoreWriter {
    data_path: PathBuf,
    index_path: PathBuf,
    data: ReservedSpillWriter,
    index: File,
    next_offset: u64,
    len: usize,
}

impl IndexedRowStoreWriter {
    fn new(spill: &mut SpillManager, memory: &QueryMemoryContext) -> Result<Self> {
        let data_path = spill.next_run_path()?;
        let index_path = spill.next_run_path()?;
        let data = create_spill_writer(&data_path, memory)?;
        let index = File::create(&index_path).map_err(spill_io_error)?;
        Ok(Self {
            data_path,
            index_path,
            data,
            index,
            next_offset: u64::try_from(SPILL_MAGIC.len() + std::mem::size_of::<u16>())
                .map_err(|_| DbError::internal("spill header size is out of range"))?,
            len: 0,
        })
    }

    fn push(&mut self, row: &Row, memory: &QueryMemoryContext) -> Result<()> {
        self.index
            .write_all(&self.next_offset.to_le_bytes())
            .map_err(spill_io_error)?;
        let written = write_spill_record(&mut self.data, row, memory)?;
        self.next_offset =
            self.next_offset
                .checked_add(u64::try_from(written).map_err(|_| {
                    DbError::new("53200", "window spill record length is out of range")
                })?)
                .ok_or_else(|| DbError::new("53200", "window spill offset is out of range"))?;
        self.len = self
            .len
            .checked_add(1)
            .ok_or_else(|| DbError::new("54001", "window row count is out of range"))?;
        Ok(())
    }

    fn finish(mut self, memory: &QueryMemoryContext) -> Result<IndexedRowStore> {
        self.data.flush().map_err(spill_io_error)?;
        self.index.flush().map_err(spill_io_error)?;
        let data_path = self.data_path.clone();
        let index_path = self.index_path.clone();
        let len = self.len;
        drop(self.data);
        drop(self.index);
        IndexedRowStore::open(data_path, index_path, len, memory)
    }
}

struct IndexedResultWriter {
    data_path: PathBuf,
    index_path: PathBuf,
    data: ReservedSpillWriter,
    index: File,
    next_offset: u64,
    len: usize,
    written: usize,
}

impl IndexedResultWriter {
    fn new(spill: &mut SpillManager, len: usize, memory: &QueryMemoryContext) -> Result<Self> {
        let data_path = spill.next_run_path()?;
        let index_path = spill.next_run_path()?;
        let data = create_spill_writer(&data_path, memory)?;
        let index = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&index_path)
            .map_err(spill_io_error)?;
        let index_bytes = u64::try_from(len)
            .ok()
            .and_then(|len| len.checked_mul(WINDOW_SPILL_INDEX_BYTES))
            .ok_or_else(|| DbError::new("53200", "window result index is out of range"))?;
        index.set_len(index_bytes).map_err(spill_io_error)?;
        Ok(Self {
            data_path,
            index_path,
            data,
            index,
            next_offset: u64::try_from(SPILL_MAGIC.len() + std::mem::size_of::<u16>())
                .map_err(|_| DbError::internal("spill header size is out of range"))?,
            len,
            written: 0,
        })
    }

    fn push_at(
        &mut self,
        source_index: usize,
        result: Value,
        memory: &QueryMemoryContext,
    ) -> Result<()> {
        if source_index >= self.len {
            return Err(DbError::internal(
                "window result source index is out of bounds",
            ));
        }
        let index_position = u64::try_from(source_index)
            .ok()
            .and_then(|index| index.checked_mul(WINDOW_SPILL_INDEX_BYTES))
            .ok_or_else(|| DbError::new("53200", "window result index is out of range"))?;
        self.index
            .seek(SeekFrom::Start(index_position))
            .map_err(spill_io_error)?;
        let mut existing = [0_u8; std::mem::size_of::<u64>()];
        self.index
            .read_exact(&mut existing)
            .map_err(spill_io_error)?;
        if u64::from_le_bytes(existing) != 0 {
            return Err(DbError::internal(
                "window result was written more than once",
            ));
        }
        self.index
            .seek(SeekFrom::Start(index_position))
            .map_err(spill_io_error)?;
        self.index
            .write_all(&self.next_offset.to_le_bytes())
            .map_err(spill_io_error)?;

        let row = Row::new(vec![result]);
        let _row_reservation = memory.try_reserve(estimated_row_bytes(&row))?;
        let written = write_spill_record(&mut self.data, &row, memory)?;
        self.next_offset = self
            .next_offset
            .checked_add(
                u64::try_from(written)
                    .map_err(|_| DbError::new("53200", "window result length is out of range"))?,
            )
            .ok_or_else(|| DbError::new("53200", "window result offset is out of range"))?;
        self.written = self
            .written
            .checked_add(1)
            .ok_or_else(|| DbError::new("54001", "window result count is out of range"))?;
        Ok(())
    }

    fn finish(mut self, memory: &QueryMemoryContext) -> Result<IndexedRowStore> {
        if self.written != self.len {
            return Err(DbError::internal("window result index is incomplete"));
        }
        self.data.flush().map_err(spill_io_error)?;
        self.index.flush().map_err(spill_io_error)?;
        let data_path = self.data_path.clone();
        let index_path = self.index_path.clone();
        let len = self.len;
        drop(self.data);
        drop(self.index);
        IndexedRowStore::open(data_path, index_path, len, memory)
    }
}

struct ReservedValue {
    value: Value,
    reservation: Reservation,
}

enum WindowResultWriter {
    Memory {
        values: Vec<Option<Value>>,
        reservation: Reservation,
    },
    Spill(IndexedResultWriter),
}
