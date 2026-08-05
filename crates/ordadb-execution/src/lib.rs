//! Physical relational operators and typed scalar evaluation for OrdaDB.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use ordadb_index::{BPlusTree, BPlusTreeOwnedIter, IndexKey};
use ordadb_optimizer::{AccessPath, PlanKind, PlanNode};
use ordadb_sql::{
    AggregateFunction, BinaryOperator, BoundExpr, BoundExprKind, BoundOrder, BoundProjection,
    ScalarFunction, UnaryOperator,
};
use ordadb_types::{
    ArrayDimension, Batch, DbError, IndexId, PgArray, PgInterval, Result, Row, ScalarType, Schema,
    TableId, Value,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::Serialize;
use serde::de::DeserializeOwned;

mod advanced;
mod columnar;
mod memory;
mod scan;
pub use advanced::{
    AdvancedExecutionCursor, AdvancedExecutionPlan, ApplyExecutionKind, ApplyExecutionPlan,
    JoinExecutionPlan, JoinExecutionSource, QueryExecutionPlan,
};
pub use columnar::{
    ChunkPool, ColumnVector, ColumnVectorKind, DataChunk, RowColumnView, SelectionVector,
};
pub use memory::{MemoryGrant, Reservation};
pub use scan::{LeasedDataChunk, SnapshotTableProvider, TableProvider, TableScan};

pub const DEFAULT_BATCH_ROWS: usize = 1024;
pub const DEFAULT_SOFT_MEMORY_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_HARD_MEMORY_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_MAX_PLAN_DEPTH: usize = 256;
pub const DEFAULT_MAX_EXPRESSION_DEPTH: usize = 256;

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
const SPILL_MAGIC: [u8; 8] = *b"ORDBSPL1";
const SPILL_VERSION: u16 = 1;
const MAX_SPILL_MERGE_FAN_IN: usize = 8;
const DEFAULT_SPILL_IO_BUFFER_BYTES: usize = 8 * 1024;
const MAX_CONCURRENT_SPILL_STREAMS: usize = 32;

pub struct ExecutionContext<'a> {
    pub tables: &'a BTreeMap<TableId, Arc<Vec<Row>>>,
    pub indexes: &'a BTreeMap<IndexId, Arc<BPlusTree>>,
    pub params: &'a [Value],
}

#[derive(Debug, Clone)]
pub struct ExecutionOptions {
    pub batch_rows: usize,
    pub soft_memory_bytes: usize,
    pub hard_memory_bytes: usize,
    pub max_plan_depth: usize,
    pub max_expression_depth: usize,
    pub spill_root: PathBuf,
}

impl Default for ExecutionOptions {
    fn default() -> Self {
        Self {
            batch_rows: DEFAULT_BATCH_ROWS,
            soft_memory_bytes: DEFAULT_SOFT_MEMORY_BYTES,
            hard_memory_bytes: DEFAULT_HARD_MEMORY_BYTES,
            max_plan_depth: DEFAULT_MAX_PLAN_DEPTH,
            max_expression_depth: DEFAULT_MAX_EXPRESSION_DEPTH,
            spill_root: std::env::temp_dir().join("ordadb-spill"),
        }
    }
}

impl ExecutionOptions {
    fn validate(&self) -> Result<()> {
        if self.batch_rows == 0
            || self.soft_memory_bytes == 0
            || self.hard_memory_bytes == 0
            || self.soft_memory_bytes > self.hard_memory_bytes
            || self.max_plan_depth == 0
            || self.max_expression_depth == 0
        {
            return Err(DbError::new(
                "22023",
                "execution options contain an invalid zero or memory limit",
            )
            .with_hint("Use a positive batch/depth limit and soft memory no larger than hard."));
        }
        fs::create_dir_all(&self.spill_root).map_err(|error| {
            DbError::new("22023", "query spill root cannot be created")
                .with_detail(error.to_string())
                .with_hint("Choose a writable local directory for query spill files.")
        })?;
        if !self.spill_root.is_dir() {
            return Err(DbError::new("22023", "query spill root is not a directory"));
        }
        Ok(())
    }
}

pub type QueryMemoryContext = MemoryGrant;

#[derive(Debug)]
struct ExpressionStack {
    values: Vec<Value>,
    reservation: Reservation,
}

impl ExpressionStack {
    fn new(memory: &MemoryGrant) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            reservation: memory.try_reserve(0)?,
        })
    }

    fn prepare(&mut self, slots: usize) -> Result<()> {
        self.values.clear();
        let slot_bytes = slots
            .checked_mul(std::mem::size_of::<Value>())
            .ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("expression stack capacity overflow")
            })?;
        self.reservation.resize(slot_bytes)?;
        if self.values.capacity() < slots {
            // `try_reserve_exact` measures `additional` from the current
            // length, not from the current capacity. `prepare` has cleared
            // the stack, so reserving only the capacity delta can still
            // leave a reused stack smaller than the compiled program needs.
            let additional = slots - self.values.len();
            if let Err(error) = self.values.try_reserve_exact(additional) {
                self.reservation
                    .resize(self.values.capacity() * std::mem::size_of::<Value>())?;
                return Err(DbError::new("53200", "query memory limit exceeded")
                    .with_detail(format!("failed to allocate expression stack: {error}")));
            }
        }
        let actual_bytes = self
            .values
            .capacity()
            .checked_mul(std::mem::size_of::<Value>())
            .ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("expression stack capacity overflow")
            })?;
        self.reservation.resize(actual_bytes)
    }

    fn push(&mut self, value: Value) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            return Err(program_limit_error(
                "expression value stack exceeded its compiled capacity",
            ));
        }
        self.reservation
            .grow(estimated_value_bytes(&value).saturating_sub(std::mem::size_of::<Value>()))?;
        self.values.push(value);
        Ok(())
    }

    fn pop(&mut self) -> Option<Value> {
        self.values.pop()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn collapse_in_list(
        &mut self,
        count: usize,
        negated: bool,
        operand_type: &ScalarType,
    ) -> Result<()> {
        <Self as ExpressionValues>::collapse_in_list(self, count, negated, operand_type)
    }
}

trait ExpressionValues {
    fn reset(&mut self) -> Result<()>;
    fn push_value(&mut self, value: Value) -> Result<()>;
    fn pop_value(&mut self) -> Option<Value>;
    fn value_count(&self) -> usize;
    fn collapse_in_list(
        &mut self,
        count: usize,
        negated: bool,
        operand_type: &ScalarType,
    ) -> Result<()>;
}

impl ExpressionValues for Vec<Value> {
    fn reset(&mut self) -> Result<()> {
        self.clear();
        Ok(())
    }

    fn push_value(&mut self, value: Value) -> Result<()> {
        self.push(value);
        Ok(())
    }

    fn pop_value(&mut self) -> Option<Value> {
        self.pop()
    }

    fn value_count(&self) -> usize {
        self.len()
    }

    fn collapse_in_list(
        &mut self,
        count: usize,
        negated: bool,
        operand_type: &ScalarType,
    ) -> Result<()> {
        let result = evaluate_in_list_stack(self, count, negated, operand_type)?;
        self.truncate(self.len().saturating_sub(count.saturating_add(1)));
        self.push(result);
        Ok(())
    }
}

impl ExpressionValues for ExpressionStack {
    fn reset(&mut self) -> Result<()> {
        self.values.clear();
        let capacity_bytes = self
            .values
            .capacity()
            .checked_mul(std::mem::size_of::<Value>())
            .ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("expression stack capacity overflow")
            })?;
        self.reservation.resize(capacity_bytes)
    }

    fn push_value(&mut self, value: Value) -> Result<()> {
        self.push(value)
    }

    fn pop_value(&mut self) -> Option<Value> {
        self.pop()
    }

    fn value_count(&self) -> usize {
        self.len()
    }

    fn collapse_in_list(
        &mut self,
        count: usize,
        negated: bool,
        operand_type: &ScalarType,
    ) -> Result<()> {
        let result = evaluate_in_list_stack(&self.values, count, negated, operand_type)?;
        self.values
            .truncate(self.values.len().saturating_sub(count.saturating_add(1)));
        self.push(result)
    }
}

#[derive(Debug)]
pub struct BatchPool {
    rows: Vec<Vec<Row>>,
    max_retained: usize,
    batch_rows: usize,
}

impl BatchPool {
    fn new(batch_rows: usize) -> Self {
        Self {
            rows: Vec::new(),
            max_retained: 4,
            batch_rows,
        }
    }

    fn take(&mut self) -> Vec<Row> {
        self.rows
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.batch_rows))
    }

    fn recycle(&mut self, mut rows: Vec<Row>) {
        rows.clear();
        if rows.capacity() <= self.batch_rows && self.rows.len() < self.max_retained {
            self.rows.push(rows);
        }
    }
}

#[derive(Debug, Clone)]
enum OperatorFrame {
    Filter(Box<ExpressionProgram>),
    Projection(Vec<ExpressionProgram>),
    Sort(Vec<BoundOrder>),
    Offset { remaining: usize },
    Limit { remaining: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperatorId(usize);

#[derive(Debug, Default)]
pub struct OperatorArena {
    operators: Vec<OperatorFrame>,
}

impl OperatorArena {
    fn insert(&mut self, operator: OperatorFrame) -> OperatorId {
        let id = OperatorId(self.operators.len());
        self.operators.push(operator);
        id
    }

    fn get(&self, id: OperatorId) -> Result<&OperatorFrame> {
        self.operators
            .get(id.0)
            .ok_or_else(|| DbError::internal("operator ID is outside the arena"))
    }

    fn get_mut(&mut self, id: OperatorId) -> Result<&mut OperatorFrame> {
        self.operators
            .get_mut(id.0)
            .ok_or_else(|| DbError::internal("operator ID is outside the arena"))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.operators.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }
}

#[derive(Debug, Default)]
pub struct FrameStack {
    frames: Vec<OperatorId>,
}

impl FrameStack {
    fn push(&mut self, operator: OperatorId) {
        self.frames.push(operator);
    }

    fn reverse(&mut self) {
        self.frames.reverse();
    }

    fn as_slice(&self) -> &[OperatorId] {
        &self.frames
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

enum SourceCursor {
    Empty,
    Sequential(Box<dyn TableScan>),
    Index {
        rows: Arc<Vec<Row>>,
        entries: BPlusTreeOwnedIter,
    },
}

impl SourceCursor {
    fn next_chunk(
        &mut self,
        batch_rows: usize,
        memory: &MemoryGrant,
    ) -> Result<Option<LeasedDataChunk>> {
        match self {
            Self::Empty => Ok(None),
            Self::Sequential(scan) => scan.next_chunk(batch_rows, memory),
            Self::Index { rows, entries } => {
                let mut selected = Vec::with_capacity(batch_rows);
                while selected.len() < batch_rows {
                    let Some(entry) = entries.next() else {
                        break;
                    };
                    let row = usize::try_from(entry.row_id.get())
                        .ok()
                        .and_then(|row_id| rows.get(row_id))
                        .ok_or_else(|| DbError::internal("index row reference is out of bounds"))?;
                    selected.push(row.clone());
                }
                if selected.is_empty() {
                    Ok(None)
                } else {
                    LeasedDataChunk::from_rows(&selected, memory).map(Some)
                }
            }
        }
    }
}

struct SpillRun {
    reader: ReservedSpillReader,
    current: Option<Row>,
    current_reservation: Option<Reservation>,
}

impl SpillRun {
    fn open(path: &Path, memory: &MemoryGrant) -> Result<Self> {
        let reader = open_spill_reader(path, memory)?;
        let mut run = Self {
            reader,
            current: None,
            current_reservation: None,
        };
        run.advance(memory)?;
        Ok(run)
    }

    fn advance(&mut self, memory: &MemoryGrant) -> Result<()> {
        self.current = None;
        self.current_reservation = None;
        if let Some(record) = read_spill_record(&mut self.reader, memory)? {
            let reservation = memory.try_reserve(estimated_row_bytes(&record.value))?;
            self.current = Some(record.value);
            self.current_reservation = Some(reservation);
        }
        Ok(())
    }
}

struct SpillMergeCursor {
    runs: Vec<SpillRun>,
    heap: Vec<usize>,
    _heap_reservation: Reservation,
}

impl SpillMergeCursor {
    fn open(paths: &[PathBuf], order_by: &[BoundOrder], memory: &MemoryGrant) -> Result<Self> {
        if paths.len() > MAX_SPILL_MERGE_FAN_IN {
            return Err(DbError::new(
                "54000",
                "spill merge fan-in exceeds the configured implementation limit",
            )
            .with_detail(format!(
                "{} runs requested; maximum is {MAX_SPILL_MERGE_FAN_IN}",
                paths.len()
            )));
        }
        let runs = paths
            .iter()
            .map(|path| SpillRun::open(path, memory))
            .collect::<Result<Vec<_>>>()?;
        let requested_bytes = paths
            .len()
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("spill merge heap capacity overflow")
            })?;
        let mut heap_reservation = memory.try_reserve(requested_bytes)?;
        let mut heap = Vec::new();
        if let Err(error) = heap.try_reserve_exact(paths.len()) {
            return Err(DbError::new("53200", "query memory limit exceeded")
                .with_detail(format!("failed to allocate spill merge heap: {error}")));
        }
        heap_reservation.resize(
            heap.capacity()
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or_else(|| {
                    DbError::new("53200", "query memory limit exceeded")
                        .with_detail("spill merge heap capacity overflow")
                })?,
        )?;
        for index in 0..runs.len() {
            if runs[index].current.is_some() {
                spill_heap_push(&mut heap, &runs, index, order_by)?;
            }
        }
        Ok(Self {
            runs,
            heap,
            _heap_reservation: heap_reservation,
        })
    }

    fn pop_next(&mut self, order_by: &[BoundOrder], memory: &MemoryGrant) -> Result<Option<Row>> {
        let Some(index) = spill_heap_pop(&mut self.heap, &self.runs, order_by)? else {
            return Ok(None);
        };
        let row = self.runs[index]
            .current
            .take()
            .ok_or_else(|| DbError::internal("spill merge row disappeared"))?;
        self.runs[index].advance(memory)?;
        if self.runs[index].current.is_some() {
            spill_heap_push(&mut self.heap, &self.runs, index, order_by)?;
        }
        Ok(Some(row))
    }

    #[cfg(test)]
    fn run_count(&self) -> usize {
        self.runs.len()
    }
}

fn spill_heap_push(
    heap: &mut Vec<usize>,
    runs: &[SpillRun],
    index: usize,
    order_by: &[BoundOrder],
) -> Result<()> {
    heap.push(index);
    let mut child = heap.len().saturating_sub(1);
    while child > 0 {
        let parent = (child - 1) / 2;
        if !spill_run_precedes(runs, heap[child], heap[parent], order_by)? {
            break;
        }
        heap.swap(child, parent);
        child = parent;
    }
    Ok(())
}

fn spill_heap_pop(
    heap: &mut Vec<usize>,
    runs: &[SpillRun],
    order_by: &[BoundOrder],
) -> Result<Option<usize>> {
    let Some(last) = heap.pop() else {
        return Ok(None);
    };
    if heap.is_empty() {
        return Ok(Some(last));
    }
    let minimum = std::mem::replace(&mut heap[0], last);
    let mut parent = 0;
    loop {
        let left = parent * 2 + 1;
        if left >= heap.len() {
            break;
        }
        let right = left + 1;
        let child =
            if right < heap.len() && spill_run_precedes(runs, heap[right], heap[left], order_by)? {
                right
            } else {
                left
            };
        if !spill_run_precedes(runs, heap[child], heap[parent], order_by)? {
            break;
        }
        heap.swap(parent, child);
        parent = child;
    }
    Ok(Some(minimum))
}

fn spill_run_precedes(
    runs: &[SpillRun],
    left: usize,
    right: usize,
    order_by: &[BoundOrder],
) -> Result<bool> {
    let left_row = runs
        .get(left)
        .and_then(|run| run.current.as_ref())
        .ok_or_else(|| DbError::internal("spill merge heap references an exhausted run"))?;
    let right_row = runs
        .get(right)
        .and_then(|run| run.current.as_ref())
        .ok_or_else(|| DbError::internal("spill merge heap references an exhausted run"))?;
    Ok(match compare_rows(left_row, right_row, order_by)? {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => left < right,
    })
}

struct SpillManager {
    root: PathBuf,
    query_dir: Option<PathBuf>,
    run_count: usize,
}

impl SpillManager {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            query_dir: None,
            run_count: 0,
        }
    }

    fn ensure_query_dir(&mut self) -> Result<PathBuf> {
        if let Some(query_dir) = &self.query_dir {
            return Ok(query_dir.clone());
        }
        let query_id = NEXT_QUERY_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let query_dir = self
            .root
            .join(format!("ordadb-query-{}-{query_id}", std::process::id()));
        fs::create_dir(&query_dir).map_err(spill_io_error)?;
        self.query_dir = Some(query_dir.clone());
        Ok(query_dir)
    }

    fn next_run_path(&mut self) -> Result<PathBuf> {
        let path = self
            .ensure_query_dir()?
            .join(format!("run-{}.spill", self.run_count));
        self.run_count = self.run_count.saturating_add(1);
        Ok(path)
    }

    fn write_sorted_run(&mut self, rows: &[Row], memory: &MemoryGrant) -> Result<PathBuf> {
        let path = self.next_run_path()?;
        let mut writer = create_spill_writer(&path, memory)?;
        for row in rows {
            write_spill_record(&mut writer, row, memory)?;
        }
        writer.flush().map_err(spill_io_error)?;
        Ok(path)
    }

    fn compact_sorted_runs(
        &mut self,
        mut paths: Vec<PathBuf>,
        order_by: &[BoundOrder],
        memory: &MemoryGrant,
    ) -> Result<Vec<PathBuf>> {
        while paths.len() > MAX_SPILL_MERGE_FAN_IN {
            let mut merged = Vec::with_capacity(paths.len().div_ceil(MAX_SPILL_MERGE_FAN_IN));
            for group in paths.chunks(MAX_SPILL_MERGE_FAN_IN) {
                merged.push(self.merge_sorted_group(group, order_by, memory)?);
            }
            paths = merged;
        }
        Ok(paths)
    }

    fn merge_sorted_group(
        &mut self,
        paths: &[PathBuf],
        order_by: &[BoundOrder],
        memory: &MemoryGrant,
    ) -> Result<PathBuf> {
        let mut merge = SpillMergeCursor::open(paths, order_by, memory)?;
        let output_path = self.next_run_path()?;
        let mut writer = create_spill_writer(&output_path, memory)?;
        while let Some(row) = merge.pop_next(order_by, memory)? {
            write_spill_record(&mut writer, &row, memory)?;
        }
        writer.flush().map_err(spill_io_error)?;
        Ok(output_path)
    }
}

impl Drop for SpillManager {
    fn drop(&mut self) {
        if let Some(query_dir) = self.query_dir.take() {
            let _ = fs::remove_dir_all(query_dir);
        }
    }
}

enum SortOutput {
    Memory {
        rows: std::vec::IntoIter<Row>,
        _reservation: Reservation,
    },
    Runs(SpillMergeCursor),
}

pub struct ExecutionCursor {
    source: SourceCursor,
    arena: OperatorArena,
    frames: FrameStack,
    sort_position: Option<usize>,
    sort_output: Option<SortOutput>,
    schema: Schema,
    params: Vec<Value>,
    options: ExecutionOptions,
    memory: QueryMemoryContext,
    pool: BatchPool,
    spill: SpillManager,
    expression_stack: ExpressionStack,
    in_flight: Option<Reservation>,
    exhausted: bool,
}

impl ExecutionCursor {
    pub fn new(plan: &PlanNode, context: &ExecutionContext<'_>, schema: Schema) -> Result<Self> {
        Self::with_options(plan, context, schema, ExecutionOptions::default())
    }

    pub fn new_with_table_provider(
        plan: &PlanNode,
        context: &ExecutionContext<'_>,
        schema: Schema,
        provider: &dyn TableProvider,
    ) -> Result<Self> {
        Self::with_options_and_table_provider(
            plan,
            context,
            schema,
            ExecutionOptions::default(),
            Some(provider),
        )
    }

    pub fn with_options(
        plan: &PlanNode,
        context: &ExecutionContext<'_>,
        schema: Schema,
        options: ExecutionOptions,
    ) -> Result<Self> {
        Self::with_options_and_table_provider(plan, context, schema, options, None)
    }

    fn with_options_and_table_provider(
        plan: &PlanNode,
        context: &ExecutionContext<'_>,
        schema: Schema,
        options: ExecutionOptions,
        table_provider: Option<&dyn TableProvider>,
    ) -> Result<Self> {
        options.validate()?;
        let (source, arena, frames) = build_pipeline(plan, context, &options, table_provider)?;
        let sort_positions = frames
            .as_slice()
            .iter()
            .enumerate()
            .filter_map(|(position, id)| {
                arena
                    .get(*id)
                    .ok()
                    .is_some_and(|operator| matches!(operator, OperatorFrame::Sort(_)))
                    .then_some(position)
            })
            .collect::<Vec<_>>();
        if sort_positions.len() > 1 {
            return Err(program_limit_error(
                "a physical pipeline may contain at most one Sort frame",
            ));
        }
        let sort_position = sort_positions.first().copied();
        let memory = QueryMemoryContext::new(options.soft_memory_bytes, options.hard_memory_bytes)?;
        let expression_stack = ExpressionStack::new(&memory)?;
        Ok(Self {
            source,
            arena,
            frames,
            sort_position,
            sort_output: None,
            schema,
            params: context.params.to_vec(),
            memory,
            pool: BatchPool::new(options.batch_rows),
            spill: SpillManager::new(options.spill_root.clone()),
            expression_stack,
            in_flight: None,
            options,
            exhausted: false,
        })
    }

    #[must_use]
    pub const fn memory(&self) -> &QueryMemoryContext {
        &self.memory
    }

    pub fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.in_flight = None;
        if self.exhausted {
            return Ok(None);
        }
        if self.sort_position.is_some() && self.sort_output.is_none() {
            self.initialize_sort()?;
        }

        if let Some(sort_position) = self.sort_position {
            let mut output = self.pool.take();
            let mut reservation = self.memory.try_reserve(0)?;
            while output.len() < self.options.batch_rows {
                let Some(row) = self.next_sorted_row()? else {
                    break;
                };
                let row = apply_row_frames(
                    &mut self.arena,
                    &self.frames.as_slice()[sort_position + 1..],
                    &self.params,
                    &mut self.expression_stack,
                    Cow::Owned(row),
                )?;
                if let Some(row) = row {
                    reservation.grow(estimated_row_bytes(&row))?;
                    output.push(row);
                }
            }
            if output.is_empty() {
                self.exhausted = true;
                self.pool.recycle(output);
                return Ok(None);
            }
            self.in_flight = Some(reservation);
            return Ok(Some(Batch {
                schema: self.schema.clone(),
                rows: output,
            }));
        }

        loop {
            let Some(mut leased) = self
                .source
                .next_chunk(self.options.batch_rows, &self.memory)?
            else {
                self.exhausted = true;
                return Ok(None);
            };
            if !apply_chunk_frames(
                &mut self.arena,
                self.frames.as_slice(),
                &self.params,
                &mut self.expression_stack,
                &mut leased,
            )? {
                leased.recycle()?;
                continue;
            }
            if leased.chunk().is_empty() {
                leased.recycle()?;
                continue;
            }
            let output_reservation = self
                .memory
                .try_reserve(leased.chunk().estimated_selected_row_bytes()?)?;
            let rows = leased.take_rows()?;
            leased.recycle()?;
            if rows.is_empty() {
                continue;
            }
            self.in_flight = Some(output_reservation);
            return Ok(Some(Batch {
                schema: self.schema.clone(),
                rows,
            }));
        }
    }

    fn initialize_sort(&mut self) -> Result<()> {
        let sort_position = self
            .sort_position
            .ok_or_else(|| DbError::internal("Sort initialization has no Sort frame"))?;
        let sort_id = *self
            .frames
            .as_slice()
            .get(sort_position)
            .ok_or_else(|| DbError::internal("Sort frame position is invalid"))?;
        let OperatorFrame::Sort(order_by) = self.arena.get(sort_id)? else {
            return Err(DbError::internal("Sort frame index is invalid"));
        };
        let (mut order_by, sort_programs) =
            compile_sort_orders(order_by, self.options.max_expression_depth)?;
        let mut rows = Vec::new();
        let mut rows_reservation = self.memory.try_reserve(0)?;
        let mut run_paths = Vec::new();
        loop {
            let Some(mut input) = self
                .source
                .next_chunk(self.options.batch_rows, &self.memory)?
            else {
                break;
            };
            if !apply_chunk_frames(
                &mut self.arena,
                &self.frames.as_slice()[..sort_position],
                &self.params,
                &mut self.expression_stack,
                &mut input,
            )? {
                input.recycle()?;
                continue;
            }
            for logical_row in 0..input.chunk().len() {
                let row = input.chunk().row(logical_row)?;
                let Some(mut row) = apply_row_frames(
                    &mut self.arena,
                    &[],
                    &self.params,
                    &mut self.expression_stack,
                    Cow::Owned(row),
                )?
                else {
                    continue;
                };
                materialize_sort_keys(
                    &mut row,
                    &mut order_by,
                    &sort_programs,
                    &self.params,
                    &mut self.expression_stack,
                )?;
                let row_bytes = estimated_row_bytes(&row);
                if !rows.is_empty() && self.memory.would_cross_soft_limit(row_bytes) {
                    sort_rows(&mut rows, &order_by)?;
                    run_paths.push(self.spill.write_sorted_run(&rows, &self.memory)?);
                    rows.clear();
                    rows_reservation.resize(0)?;
                }
                rows_reservation.grow(row_bytes)?;
                rows.push(row);
            }
            input.recycle()?;
        }

        if run_paths.is_empty() {
            sort_rows(&mut rows, &order_by)?;
            self.sort_output = Some(SortOutput::Memory {
                rows: rows.into_iter(),
                _reservation: rows_reservation,
            });
        } else {
            if !rows.is_empty() {
                sort_rows(&mut rows, &order_by)?;
                run_paths.push(self.spill.write_sorted_run(&rows, &self.memory)?);
            }
            drop(rows_reservation);
            let run_paths = self
                .spill
                .compact_sorted_runs(run_paths, &order_by, &self.memory)?;
            self.sort_output = Some(SortOutput::Runs(SpillMergeCursor::open(
                &run_paths,
                &order_by,
                &self.memory,
            )?));
        }
        Ok(())
    }

    fn next_sorted_row(&mut self) -> Result<Option<Row>> {
        let Some(output) = &mut self.sort_output else {
            return Ok(None);
        };
        match output {
            SortOutput::Memory {
                rows,
                _reservation: _,
            } => Ok(rows.next()),
            SortOutput::Runs(merge) => {
                let sort_position = self
                    .sort_position
                    .ok_or_else(|| DbError::internal("Sort output has no frame"))?;
                let sort_id = *self
                    .frames
                    .as_slice()
                    .get(sort_position)
                    .ok_or_else(|| DbError::internal("Sort output position is invalid"))?;
                let OperatorFrame::Sort(order_by) = self.arena.get(sort_id)? else {
                    return Err(DbError::internal("Sort output frame is invalid"));
                };
                merge.pop_next(order_by, &self.memory)
            }
        }
    }
}

fn apply_chunk_frames(
    arena: &mut OperatorArena,
    frames: &[OperatorId],
    params: &[Value],
    expression_stack: &mut ExpressionStack,
    chunk: &mut LeasedDataChunk,
) -> Result<bool> {
    for id in frames {
        match arena.get_mut(*id)? {
            OperatorFrame::Filter(program) => {
                let direct = match &program.fast_path {
                    Some(FastExpression::ColumnLiteralBinary {
                        column,
                        column_type,
                        operator,
                        literal,
                        literal_type,
                        target,
                    }) if column_type == literal_type
                        && matches!(target, ScalarType::Boolean)
                        && !matches!(column_type, ScalarType::Enum { .. }) =>
                    {
                        chunk
                            .chunk_mut()
                            .retain_literal_comparison(*column, literal, *operator)
                    }
                    _ => None,
                };
                if let Some(result) = direct {
                    result?;
                } else {
                    chunk.chunk_mut().retain_selected(|chunk, physical_row| {
                        match program.evaluate_chunk_row(
                            chunk,
                            physical_row,
                            params,
                            expression_stack,
                        )? {
                            Value::Boolean(matches) => Ok(matches),
                            Value::Null => Ok(false),
                            _ => Err(DbError::new("42804", "predicate must evaluate to boolean")),
                        }
                    })?;
                }
                if chunk.chunk().is_empty() {
                    return Ok(false);
                }
            }
            OperatorFrame::Projection(programs) => {
                let direct = programs
                    .iter()
                    .map(ExpressionProgram::column_projection)
                    .collect::<Option<Vec<_>>>();
                let projected_in_place = if let Some(projections) = direct {
                    chunk.chunk_mut().project_columns_in_place(&projections)?
                } else {
                    false
                };
                if projected_in_place {
                    chunk.refresh_reservation()?;
                } else {
                    let rows = (0..chunk.chunk().len())
                        .map(|logical_row| {
                            let row = chunk.chunk().row(logical_row)?;
                            programs
                                .iter()
                                .map(|program| {
                                    program.evaluate_reusing(&row.values, params, expression_stack)
                                })
                                .collect::<Result<Vec<_>>>()
                                .map(Row::new)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    chunk.replace(DataChunk::from_rows(&rows)?)?;
                }
            }
            OperatorFrame::Limit { remaining } => {
                if *remaining == 0 {
                    return Ok(false);
                }
                let emitted = chunk.chunk().len().min(*remaining);
                chunk.chunk_mut().selection_mut().truncate(emitted);
                *remaining -= emitted;
                if emitted == 0 {
                    return Ok(false);
                }
            }
            OperatorFrame::Offset { remaining } => {
                let skipped = chunk.chunk().len().min(*remaining);
                chunk.chunk_mut().selection_mut().discard_prefix(skipped);
                *remaining -= skipped;
                if chunk.chunk().is_empty() {
                    return Ok(false);
                }
            }
            OperatorFrame::Sort(_) => {
                return Err(DbError::internal(
                    "Sort frame reached the streaming chunk evaluator",
                ));
            }
        }
    }
    Ok(!chunk.chunk().is_empty())
}

fn apply_row_frames(
    arena: &mut OperatorArena,
    frames: &[OperatorId],
    params: &[Value],
    expression_stack: &mut ExpressionStack,
    mut row: Cow<'_, Row>,
) -> Result<Option<Row>> {
    for id in frames {
        match arena.get_mut(*id)? {
            OperatorFrame::Filter(program) => {
                match program.evaluate_reusing(&row.values, params, expression_stack)? {
                    Value::Boolean(true) => {}
                    Value::Boolean(false) | Value::Null => return Ok(None),
                    _ => {
                        return Err(DbError::new("42804", "predicate must evaluate to boolean"));
                    }
                }
            }
            OperatorFrame::Projection(programs) => {
                let mut values = Vec::with_capacity(programs.len());
                for program in programs {
                    values.push(program.evaluate_reusing(&row.values, params, expression_stack)?);
                }
                row = Cow::Owned(Row::new(values));
            }
            OperatorFrame::Limit { remaining } => {
                if *remaining == 0 {
                    return Ok(None);
                }
                *remaining -= 1;
            }
            OperatorFrame::Offset { remaining } => {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Ok(None);
                }
            }
            OperatorFrame::Sort(_) => {
                return Err(DbError::internal(
                    "Sort frame reached the streaming row evaluator",
                ));
            }
        }
    }
    Ok(Some(row.into_owned()))
}

fn build_pipeline(
    plan: &PlanNode,
    context: &ExecutionContext<'_>,
    options: &ExecutionOptions,
    table_provider: Option<&dyn TableProvider>,
) -> Result<(SourceCursor, OperatorArena, FrameStack)> {
    let mut node = plan;
    let mut arena = OperatorArena::default();
    let mut frames = FrameStack::default();
    let source = loop {
        if frames.len() >= options.max_plan_depth {
            return Err(program_limit_error(format!(
                "physical plan exceeds the depth limit of {}",
                options.max_plan_depth
            )));
        }
        match &node.kind {
            PlanKind::Scan {
                table_id, access, ..
            } => {
                break build_source(
                    *table_id,
                    access,
                    context,
                    options.max_expression_depth,
                    table_provider,
                )?;
            }
            PlanKind::Filter { predicate, input } => {
                let id = arena.insert(OperatorFrame::Filter(Box::new(
                    ExpressionProgram::compile_with_limit(
                        predicate,
                        false,
                        options.max_expression_depth,
                    )?,
                )));
                frames.push(id);
                node = input;
            }
            PlanKind::Projection { expressions, input } => {
                let id = arena.insert(OperatorFrame::Projection(compile_projections(
                    expressions,
                    options.max_expression_depth,
                )?));
                frames.push(id);
                node = input;
            }
            PlanKind::Sort { order_by, input } => {
                let id = arena.insert(OperatorFrame::Sort(order_by.clone()));
                frames.push(id);
                node = input;
            }
            PlanKind::Offset { offset, input } => {
                let id = arena.insert(OperatorFrame::Offset {
                    remaining: evaluate_offset_program(
                        &ExpressionProgram::compile_with_limit(
                            offset,
                            false,
                            options.max_expression_depth,
                        )?,
                        context.params,
                    )?,
                });
                frames.push(id);
                node = input;
            }
            PlanKind::Limit { limit, input } => {
                let id = arena.insert(OperatorFrame::Limit {
                    remaining: evaluate_limit_program(
                        &ExpressionProgram::compile_with_limit(
                            limit,
                            false,
                            options.max_expression_depth,
                        )?,
                        context.params,
                    )?,
                });
                frames.push(id);
                node = input;
            }
        }
    };
    frames.reverse();
    Ok((source, arena, frames))
}

fn build_source(
    table_id: TableId,
    access: &AccessPath,
    context: &ExecutionContext<'_>,
    max_expression_depth: usize,
    table_provider: Option<&dyn TableProvider>,
) -> Result<SourceCursor> {
    match access {
        AccessPath::Empty => Ok(SourceCursor::Empty),
        AccessPath::Sequential => match table_provider {
            Some(provider) => provider.scan(table_id).map(SourceCursor::Sequential),
            None => SnapshotTableProvider::new(context.tables)
                .scan(table_id)
                .map(SourceCursor::Sequential),
        },
        AccessPath::Index {
            index_id,
            data_type,
            operator,
            value,
            ..
        } => {
            let program =
                ExpressionProgram::compile_with_limit(value, false, max_expression_depth)?;
            let value = coerce_value(program.evaluate(&[], context.params)?, data_type)?;
            if value.is_null() {
                return Ok(SourceCursor::Empty);
            }
            let rows = context
                .tables
                .get(&table_id)
                .cloned()
                .unwrap_or_else(|| Arc::new(Vec::new()));
            let key = IndexKey::from_typed_values(
                std::slice::from_ref(&value),
                std::slice::from_ref(data_type),
            )?;
            let tree = context
                .indexes
                .get(index_id)
                .ok_or_else(|| DbError::internal("planned index is unavailable"))?;
            let entries = match operator {
                BinaryOperator::Eq => tree.owned_get_iter(key),
                BinaryOperator::Lt => tree.owned_range_iter(Bound::Unbounded, Bound::Excluded(key)),
                BinaryOperator::LtEq => {
                    tree.owned_range_iter(Bound::Unbounded, Bound::Included(key))
                }
                BinaryOperator::Gt => tree.owned_range_iter(Bound::Excluded(key), Bound::Unbounded),
                BinaryOperator::GtEq => {
                    tree.owned_range_iter(Bound::Included(key), Bound::Unbounded)
                }
                _ => {
                    return Err(DbError::internal(
                        "optimizer selected an unsupported index operator",
                    ));
                }
            };
            Ok(SourceCursor::Index { rows, entries })
        }
    }
}

fn compile_projections(
    projections: &[BoundProjection],
    max_expression_depth: usize,
) -> Result<Vec<ExpressionProgram>> {
    projections
        .iter()
        .map(|projection| {
            ExpressionProgram::compile_with_limit(&projection.expr, false, max_expression_depth)
        })
        .collect()
}

fn compile_sort_orders(
    order_by: &[BoundOrder],
    max_expression_depth: usize,
) -> Result<(Vec<BoundOrder>, Vec<Option<ExpressionProgram>>)> {
    let mut effective = order_by.to_vec();
    let programs = effective
        .iter_mut()
        .map(|order| {
            order
                .expression
                .take()
                .map(|expression| {
                    order.column_index = usize::MAX;
                    ExpressionProgram::compile_with_limit(&expression, false, max_expression_depth)
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((effective, programs))
}

fn materialize_sort_keys(
    row: &mut Row,
    order_by: &mut [BoundOrder],
    programs: &[Option<ExpressionProgram>],
    params: &[Value],
    stack: &mut ExpressionStack,
) -> Result<()> {
    let base_width = row.values.len();
    let keys = programs
        .iter()
        .map(|program| {
            program
                .as_ref()
                .map(|program| program.evaluate_reusing(&row.values, params, stack))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    for (ordinal, (order, key)) in order_by.iter_mut().zip(keys).enumerate() {
        let Some(key) = key else {
            continue;
        };
        let expected_index = base_width.saturating_add(ordinal);
        if order.column_index == usize::MAX {
            order.column_index = expected_index;
        } else if order.column_index != expected_index {
            return Err(DbError::internal(
                "materialized sort-key layout changed between rows",
            ));
        }
        while row.values.len() < expected_index {
            row.values.push(Value::Null);
        }
        row.values.push(key);
    }
    Ok(())
}

fn sort_rows(rows: &mut [Row], order_by: &[BoundOrder]) -> Result<()> {
    let mut error = None;
    rows.sort_by(|left, right| {
        compare_rows(left, right, order_by).unwrap_or_else(|sort_error| {
            error = Some(sort_error);
            Ordering::Equal
        })
    });
    error.map_or(Ok(()), Err)
}

fn spill_io_error(error: std::io::Error) -> DbError {
    DbError::new("58030", "query spill I/O failed")
        .with_detail(error.to_string())
        .with_hint("Check free disk space and permissions for the configured spill directory.")
}

struct ReservedSpillWriter {
    writer: BufWriter<File>,
    _reservation: Reservation,
}

impl Write for ReservedSpillWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.writer.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

struct ReservedSpillReader {
    reader: BufReader<File>,
    _reservation: Reservation,
}

impl Read for ReservedSpillReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buffer)
    }
}

impl Seek for ReservedSpillReader {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.reader.seek(position)
    }
}

fn spill_io_buffer_bytes(memory: &MemoryGrant) -> usize {
    memory
        .hard_limit_bytes()
        .checked_div(MAX_CONCURRENT_SPILL_STREAMS.saturating_mul(4))
        .unwrap_or(0)
        .clamp(1, DEFAULT_SPILL_IO_BUFFER_BYTES)
}

fn reserve_spill_writer(file: File, memory: &MemoryGrant) -> Result<ReservedSpillWriter> {
    let capacity = spill_io_buffer_bytes(memory);
    let reservation = memory.try_reserve(capacity)?;
    Ok(ReservedSpillWriter {
        writer: BufWriter::with_capacity(capacity, file),
        _reservation: reservation,
    })
}

fn create_spill_writer(path: &Path, memory: &MemoryGrant) -> Result<ReservedSpillWriter> {
    let file = File::create(path).map_err(spill_io_error)?;
    let mut writer = reserve_spill_writer(file, memory)?;
    writer.write_all(&SPILL_MAGIC).map_err(spill_io_error)?;
    writer
        .write_all(&SPILL_VERSION.to_le_bytes())
        .map_err(spill_io_error)?;
    Ok(writer)
}

fn open_spill_reader(path: &Path, memory: &MemoryGrant) -> Result<ReservedSpillReader> {
    let file = File::open(path).map_err(spill_io_error)?;
    let capacity = spill_io_buffer_bytes(memory);
    let reservation = memory.try_reserve(capacity)?;
    let mut reader = ReservedSpillReader {
        reader: BufReader::with_capacity(capacity, file),
        _reservation: reservation,
    };
    let mut magic = [0_u8; SPILL_MAGIC.len()];
    reader
        .read_exact(&mut magic)
        .map_err(|error| spill_corruption("spill header is truncated", error))?;
    if magic != SPILL_MAGIC {
        return Err(DbError::new("XX001", "query spill magic is invalid"));
    }
    let mut version = [0_u8; 2];
    reader
        .read_exact(&mut version)
        .map_err(|error| spill_corruption("spill version is truncated", error))?;
    if u16::from_le_bytes(version) != SPILL_VERSION {
        return Err(DbError::new(
            "XX001",
            "query spill format version is unsupported",
        ));
    }
    Ok(reader)
}

struct SpillRecord<T> {
    value: T,
    _reservation: Reservation,
}

struct ReservedSpillBuffer {
    bytes: Vec<u8>,
    reservation: Reservation,
    failure: Option<DbError>,
}

impl ReservedSpillBuffer {
    fn new(memory: &MemoryGrant) -> Result<Self> {
        Ok(Self {
            bytes: Vec::new(),
            reservation: memory.try_reserve(0)?,
            failure: None,
        })
    }
}

impl Write for ReservedSpillBuffer {
    fn write(&mut self, source: &[u8]) -> std::io::Result<usize> {
        let required = self
            .bytes
            .len()
            .checked_add(source.len())
            .ok_or_else(|| std::io::Error::other("spill buffer length overflow"))?;
        if required > self.bytes.capacity() {
            let old_capacity = self.bytes.capacity();
            let requested = required - old_capacity;
            if let Err(error) = self.reservation.grow(requested) {
                self.failure = Some(error);
                return Err(std::io::Error::other(
                    "spill buffer exceeds query memory grant",
                ));
            }
            if let Err(error) = self
                .bytes
                .try_reserve_exact(required.saturating_sub(self.bytes.len()))
            {
                let _ = self.reservation.resize(old_capacity);
                let error = DbError::new("53200", "query memory limit exceeded")
                    .with_detail(format!("failed to allocate spill buffer: {error}"));
                self.failure = Some(error);
                return Err(std::io::Error::other("failed to allocate spill buffer"));
            }
            let actual_capacity = self.bytes.capacity();
            if actual_capacity > old_capacity.saturating_add(requested)
                && let Err(error) = self
                    .reservation
                    .grow(actual_capacity - old_capacity - requested)
            {
                self.failure = Some(error);
                return Err(std::io::Error::other(
                    "spill buffer exceeds query memory grant",
                ));
            }
        }
        self.bytes.extend_from_slice(source);
        Ok(source.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn write_spill_record<T: Serialize>(
    writer: &mut impl Write,
    value: &T,
    memory: &MemoryGrant,
) -> Result<usize> {
    let mut payload = ReservedSpillBuffer::new(memory)?;
    if let Err(error) = serde_json::to_writer(&mut payload, value) {
        if let Some(memory_error) = payload.failure.take() {
            return Err(memory_error);
        }
        return Err(
            DbError::new("58030", "query spill encoding failed").with_detail(error.to_string())
        );
    }
    let length = u32::try_from(payload.bytes.len())
        .map_err(|_| DbError::new("53200", "query spill record length is out of range"))?;
    writer
        .write_all(&length.to_le_bytes())
        .map_err(spill_io_error)?;
    writer.write_all(&payload.bytes).map_err(spill_io_error)?;
    Ok(std::mem::size_of::<u32>().saturating_add(payload.bytes.len()))
}

fn read_spill_record<T: DeserializeOwned>(
    reader: &mut impl Read,
    memory: &MemoryGrant,
) -> Result<Option<SpillRecord<T>>> {
    let mut length = [0_u8; 4];
    let first = reader.read(&mut length[..1]).map_err(spill_io_error)?;
    if first == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut length[1..])
        .map_err(|error| spill_corruption("spill record length is truncated", error))?;
    let length = u32::from_le_bytes(length) as usize;
    if length > memory.hard_limit_bytes() {
        return Err(DbError::new(
            "53200",
            "query spill record exceeds the hard memory limit",
        ));
    }
    let reservation = memory.try_reserve(length)?;
    let mut payload = vec![0_u8; length];
    reader
        .read_exact(&mut payload)
        .map_err(|error| spill_corruption("spill record payload is truncated", error))?;
    serde_json::from_slice(&payload)
        .map(|value| {
            Some(SpillRecord {
                value,
                _reservation: reservation,
            })
        })
        .map_err(|error| {
            DbError::new("XX001", "query spill record is corrupt").with_detail(error.to_string())
        })
}

fn spill_corruption(message: &str, error: std::io::Error) -> DbError {
    DbError::new("XX001", message).with_detail(error.to_string())
}

fn program_limit_error(detail: impl Into<String>) -> DbError {
    DbError::new("54001", "statement complexity limit exceeded")
        .with_detail(detail)
        .with_hint("Reduce nested expressions or split the query into simpler statements.")
}

/// Returns the conservative query-memory charge for one public compatibility row.
#[must_use]
pub fn estimated_row_bytes(row: &Row) -> usize {
    std::mem::size_of::<Row>() + row.values.iter().map(estimated_value_bytes).sum::<usize>()
}

pub(crate) fn estimated_value_bytes(value: &Value) -> usize {
    std::mem::size_of::<Value>()
        + match value {
            Value::Text(value) => value.len(),
            Value::Binary(value) => value.len(),
            Value::Json(value) | Value::Jsonb(value) => value.to_string().len(),
            Value::Array(value) => value
                .dimensions()
                .len()
                .saturating_mul(std::mem::size_of::<ordadb_types::ArrayDimension>())
                .saturating_add(
                    value
                        .values()
                        .iter()
                        .map(estimated_value_bytes)
                        .sum::<usize>(),
                ),
            Value::Vector(value) => value.len().saturating_mul(std::mem::size_of::<f32>()),
            _ => 0,
        }
}

pub fn execute(plan: &PlanNode, context: &ExecutionContext<'_>) -> Result<Vec<Row>> {
    let mut cursor = ExecutionCursor::new(plan, context, Schema::empty())?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch()? {
        rows.extend(batch.rows);
    }
    Ok(rows)
}

#[derive(Debug, Clone)]
enum ExpressionInstruction {
    LoadColumn(usize),
    LoadLiteral(Value),
    LoadParameter(usize),
    Unary(UnaryOperator),
    Binary {
        operator: BinaryOperator,
        operand_type: ScalarType,
    },
    InList {
        count: usize,
        negated: bool,
        operand_type: ScalarType,
    },
    Cast(ScalarType),
    MakeArray {
        count: usize,
        element_type: ScalarType,
        dimensions: Vec<ArrayDimension>,
    },
    Function {
        function: ScalarFunction,
        count: usize,
    },
    Aggregate {
        function: AggregateFunction,
        argument: Option<Vec<ExpressionInstruction>>,
        argument_type: Option<ScalarType>,
    },
    Coerce(ScalarType),
}

#[derive(Debug, Clone)]
pub struct ExpressionProgram {
    instructions: Vec<ExpressionInstruction>,
    fast_path: Option<FastExpression>,
    max_stack_slots: usize,
    result_type: ScalarType,
}

#[derive(Debug, Clone)]
enum FastExpression {
    Column {
        index: usize,
        target: ScalarType,
    },
    Literal {
        value: Value,
        target: ScalarType,
    },
    Parameter {
        index: usize,
        target: ScalarType,
    },
    ColumnLiteralBinary {
        column: usize,
        column_type: ScalarType,
        operator: BinaryOperator,
        literal: Value,
        literal_type: ScalarType,
        target: ScalarType,
    },
}

impl ExpressionProgram {
    pub fn compile(expr: &BoundExpr) -> Result<Self> {
        Self::compile_with_limit(expr, false, DEFAULT_MAX_EXPRESSION_DEPTH)
    }

    fn compile_with_limit(
        expr: &BoundExpr,
        allow_aggregate: bool,
        max_depth: usize,
    ) -> Result<Self> {
        let mut instructions = Vec::new();
        let mut pending = vec![(expr, false, 0_usize)];
        while let Some((expression, emitted_children, depth)) = pending.pop() {
            if depth > max_depth {
                return Err(program_limit_error(format!(
                    "expression exceeds the depth limit of {max_depth}"
                )));
            }
            if emitted_children {
                match &expression.kind {
                    BoundExprKind::Unary { op, .. } => {
                        instructions.push(ExpressionInstruction::Unary(*op));
                    }
                    BoundExprKind::Binary { left, op, .. } => {
                        instructions.push(ExpressionInstruction::Binary {
                            operator: *op,
                            operand_type: left.data_type.clone(),
                        });
                    }
                    BoundExprKind::InList {
                        expr,
                        list,
                        negated,
                    } => {
                        instructions.push(ExpressionInstruction::InList {
                            count: list.len(),
                            negated: *negated,
                            operand_type: expr.data_type.clone(),
                        });
                    }
                    BoundExprKind::Cast { .. } => {
                        instructions
                            .push(ExpressionInstruction::Cast(expression.data_type.clone()));
                    }
                    BoundExprKind::Array {
                        elements,
                        dimensions,
                    } => {
                        let ScalarType::Array { element } = &expression.data_type else {
                            return Err(DbError::internal(
                                "array expression lost its array result type",
                            ));
                        };
                        instructions.push(ExpressionInstruction::MakeArray {
                            count: elements.len(),
                            element_type: element.as_ref().clone(),
                            dimensions: dimensions.clone(),
                        });
                    }
                    BoundExprKind::Function {
                        function,
                        arguments,
                    } => {
                        instructions.push(ExpressionInstruction::Function {
                            function: *function,
                            count: arguments.len(),
                        });
                    }
                    _ => {
                        return Err(DbError::internal(
                            "expression compiler emitted an invalid parent frame",
                        ));
                    }
                }
                instructions.push(ExpressionInstruction::Coerce(expression.data_type.clone()));
                continue;
            }
            match &expression.kind {
                BoundExprKind::Column { index } => {
                    instructions.push(ExpressionInstruction::LoadColumn(*index));
                    instructions.push(ExpressionInstruction::Coerce(expression.data_type.clone()));
                }
                BoundExprKind::ApplyValue { index } => {
                    instructions.push(ExpressionInstruction::LoadColumn(*index));
                    instructions.push(ExpressionInstruction::Coerce(expression.data_type.clone()));
                }
                BoundExprKind::Literal(value) => {
                    instructions.push(ExpressionInstruction::LoadLiteral(value.clone()));
                    instructions.push(ExpressionInstruction::Coerce(expression.data_type.clone()));
                }
                BoundExprKind::Parameter { index } => {
                    instructions.push(ExpressionInstruction::LoadParameter(*index));
                    instructions.push(ExpressionInstruction::Coerce(expression.data_type.clone()));
                }
                BoundExprKind::Correlation { .. } => {
                    return Err(DbError::internal(
                        "correlated expression reached execution without a parameter frame",
                    ));
                }
                BoundExprKind::Unary { expr, .. } => {
                    pending.push((expression, true, depth));
                    pending.push((expr, false, depth + 1));
                }
                BoundExprKind::Cast { expr } => {
                    pending.push((expression, true, depth));
                    pending.push((expr, false, depth + 1));
                }
                BoundExprKind::Array { elements, .. } => {
                    pending.push((expression, true, depth));
                    for element in elements.iter().rev() {
                        pending.push((element, false, depth + 1));
                    }
                }
                BoundExprKind::Function { arguments, .. } => {
                    pending.push((expression, true, depth));
                    for argument in arguments.iter().rev() {
                        pending.push((argument, false, depth + 1));
                    }
                }
                BoundExprKind::Binary { left, right, .. } => {
                    pending.push((expression, true, depth));
                    pending.push((right, false, depth + 1));
                    pending.push((left, false, depth + 1));
                }
                BoundExprKind::InList { expr, list, .. } => {
                    pending.push((expression, true, depth));
                    for candidate in list.iter().rev() {
                        pending.push((candidate, false, depth + 1));
                    }
                    pending.push((expr, false, depth + 1));
                }
                BoundExprKind::Aggregate {
                    function, argument, ..
                } => {
                    if !allow_aggregate {
                        return Err(DbError::internal(
                            "aggregate expression requires a grouped execution context",
                        ));
                    }
                    let argument = argument
                        .as_deref()
                        .map(|argument| {
                            Self::compile_with_limit(argument, false, max_depth)
                                .map(|program| program.instructions)
                        })
                        .transpose()?;
                    let argument_type = argument.as_ref().and_then(|_| match &expression.kind {
                        BoundExprKind::Aggregate { argument, .. } => argument
                            .as_deref()
                            .map(|argument| argument.data_type.clone()),
                        _ => None,
                    });
                    instructions.push(ExpressionInstruction::Aggregate {
                        function: *function,
                        argument,
                        argument_type,
                    });
                    instructions.push(ExpressionInstruction::Coerce(expression.data_type.clone()));
                }
            }
            if instructions.len() > max_depth.saturating_mul(8) {
                return Err(program_limit_error(format!(
                    "expression instruction count exceeds {}",
                    max_depth.saturating_mul(8)
                )));
            }
        }
        let max_stack_slots = expression_stack_slots(&instructions)?;
        Ok(Self {
            instructions,
            fast_path: detect_fast_expression(expr),
            max_stack_slots,
            result_type: expr.data_type.clone(),
        })
    }

    fn result_type(&self) -> &ScalarType {
        &self.result_type
    }

    fn column_projection(&self) -> Option<(usize, ScalarType)> {
        let Some(FastExpression::Column { index, target }) = &self.fast_path else {
            return None;
        };
        Some((*index, target.clone()))
    }

    fn evaluate_chunk_row(
        &self,
        chunk: &DataChunk,
        physical_row: usize,
        params: &[Value],
        values: &mut ExpressionStack,
    ) -> Result<Value> {
        match &self.fast_path {
            Some(FastExpression::Column { index, target }) => {
                coerce_value(chunk.value(*index, physical_row)?, target)
            }
            Some(FastExpression::ColumnLiteralBinary {
                column,
                column_type,
                operator,
                literal,
                literal_type,
                target,
            }) => {
                if column_type == literal_type
                    && matches!(target, ScalarType::Boolean)
                    && !matches!(column_type, ScalarType::Enum { .. })
                    && let Some(value) =
                        chunk.compare_literal(*column, physical_row, literal, *operator)
                {
                    return value;
                }
                let left = coerce_value(chunk.value(*column, physical_row)?, column_type)?;
                let right = coerce_value(literal.clone(), literal_type)?;
                coerce_value(
                    evaluate_binary_as(left, *operator, right, column_type)?,
                    target,
                )
            }
            Some(
                fast_path @ (FastExpression::Literal { .. } | FastExpression::Parameter { .. }),
            ) => evaluate_fast_expression(fast_path, &[], params),
            None => {
                let row = chunk.physical_row(physical_row)?;
                self.evaluate_reusing(&row.values, params, values)
            }
        }
    }

    pub fn evaluate(&self, row: &[Value], params: &[Value]) -> Result<Value> {
        if let Some(fast_path) = &self.fast_path {
            return evaluate_fast_expression(fast_path, row, params);
        }
        evaluate_instructions(&self.instructions, row, params, None)
    }

    fn evaluate_reusing(
        &self,
        row: &[Value],
        params: &[Value],
        values: &mut ExpressionStack,
    ) -> Result<Value> {
        if let Some(fast_path) = &self.fast_path {
            return evaluate_fast_expression(fast_path, row, params);
        }
        values.prepare(self.max_stack_slots)?;
        evaluate_instructions_reusing(&self.instructions, row, params, None, values)
    }

    fn evaluate_group(
        &self,
        rows: &[Row],
        representative: &[Value],
        params: &[Value],
    ) -> Result<Value> {
        evaluate_instructions(&self.instructions, representative, params, Some(rows))
    }
}

fn expression_stack_slots(instructions: &[ExpressionInstruction]) -> Result<usize> {
    let mut depth = 0_usize;
    let mut maximum = 0_usize;
    for instruction in instructions {
        match instruction {
            ExpressionInstruction::LoadColumn(_)
            | ExpressionInstruction::LoadLiteral(_)
            | ExpressionInstruction::LoadParameter(_)
            | ExpressionInstruction::Aggregate { .. } => {
                depth = depth.checked_add(1).ok_or_else(|| {
                    program_limit_error("expression value stack depth overflowed")
                })?;
                maximum = maximum.max(depth);
            }
            ExpressionInstruction::Unary(_)
            | ExpressionInstruction::Cast(_)
            | ExpressionInstruction::Coerce(_) => {
                if depth == 0 {
                    return Err(DbError::internal(
                        "expression compiler produced a stack underflow",
                    ));
                }
            }
            ExpressionInstruction::Binary { .. } => {
                if depth < 2 {
                    return Err(DbError::internal(
                        "expression compiler produced a stack underflow",
                    ));
                }
                depth -= 1;
            }
            ExpressionInstruction::InList { count, .. } => {
                let required = count.saturating_add(1);
                if depth < required {
                    return Err(DbError::internal(
                        "expression compiler produced an IN list stack underflow",
                    ));
                }
                depth -= *count;
            }
            ExpressionInstruction::MakeArray { count, .. } => {
                if *count == 0 {
                    depth = depth.checked_add(1).ok_or_else(|| {
                        program_limit_error("expression value stack depth overflowed")
                    })?;
                    maximum = maximum.max(depth);
                } else {
                    if depth < *count {
                        return Err(DbError::internal(
                            "expression compiler produced an array stack underflow",
                        ));
                    }
                    depth = depth - *count + 1;
                }
            }
            ExpressionInstruction::Function { count, .. } => {
                if *count == 0 || depth < *count {
                    return Err(DbError::internal(
                        "expression compiler produced a function stack underflow",
                    ));
                }
                depth = depth - *count + 1;
            }
        }
    }
    if depth != 1 {
        return Err(DbError::internal(
            "expression compiler did not produce one stack result",
        ));
    }
    Ok(maximum)
}

fn detect_fast_expression(expr: &BoundExpr) -> Option<FastExpression> {
    match &expr.kind {
        BoundExprKind::Column { index } => Some(FastExpression::Column {
            index: *index,
            target: expr.data_type.clone(),
        }),
        BoundExprKind::Literal(value) => Some(FastExpression::Literal {
            value: value.clone(),
            target: expr.data_type.clone(),
        }),
        BoundExprKind::Parameter { index } => Some(FastExpression::Parameter {
            index: *index,
            target: expr.data_type.clone(),
        }),
        BoundExprKind::Binary { left, op, right } => {
            let BoundExprKind::Column { index } = &left.kind else {
                return None;
            };
            let BoundExprKind::Literal(literal) = &right.kind else {
                return None;
            };
            Some(FastExpression::ColumnLiteralBinary {
                column: *index,
                column_type: left.data_type.clone(),
                operator: *op,
                literal: literal.clone(),
                literal_type: right.data_type.clone(),
                target: expr.data_type.clone(),
            })
        }
        _ => None,
    }
}

fn evaluate_fast_expression(
    expression: &FastExpression,
    row: &[Value],
    params: &[Value],
) -> Result<Value> {
    match expression {
        FastExpression::Column { index, target } => {
            let value = row.get(*index).cloned().ok_or_else(|| {
                DbError::internal(format!("bound column index {index} is out of range"))
            })?;
            coerce_value(value, target)
        }
        FastExpression::Literal { value, target } => coerce_value(value.clone(), target),
        FastExpression::Parameter { index, target } => {
            let value = params.get(index - 1).cloned().ok_or_else(|| {
                DbError::new("42P02", format!("no value supplied for parameter ${index}"))
            })?;
            coerce_value(value, target)
        }
        FastExpression::ColumnLiteralBinary {
            column,
            column_type,
            operator,
            literal,
            literal_type,
            target,
        } => {
            let left = row.get(*column).cloned().ok_or_else(|| {
                DbError::internal(format!("bound column index {column} is out of range"))
            })?;
            let left = coerce_value(left, column_type)?;
            let right = coerce_value(literal.clone(), literal_type)?;
            coerce_value(
                evaluate_binary_as(left, *operator, right, column_type)?,
                target,
            )
        }
    }
}

fn evaluate_in_list_stack(
    values: &[Value],
    count: usize,
    negated: bool,
    operand_type: &ScalarType,
) -> Result<Value> {
    let required = count
        .checked_add(1)
        .ok_or_else(|| program_limit_error("IN list stack width overflowed"))?;
    if values.len() < required {
        return Err(DbError::internal(
            "expression compiler produced an IN list stack underflow",
        ));
    }
    let start = values.len() - required;
    let operand = &values[start];
    if operand.is_null() {
        return Ok(Value::Null);
    }
    let mut saw_null = false;
    for candidate in &values[start + 1..] {
        match evaluate_binary_as(
            operand.clone(),
            BinaryOperator::Eq,
            candidate.clone(),
            operand_type,
        )? {
            Value::Boolean(true) => return Ok(Value::Boolean(!negated)),
            Value::Boolean(false) => {}
            Value::Null => saw_null = true,
            _ => return Err(DbError::internal("IN equality did not return boolean")),
        }
    }
    if saw_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Boolean(negated))
    }
}

fn evaluate_instructions(
    instructions: &[ExpressionInstruction],
    row: &[Value],
    params: &[Value],
    group_rows: Option<&[Row]>,
) -> Result<Value> {
    let mut values = Vec::with_capacity(instructions.len().min(32));
    evaluate_instructions_reusing(instructions, row, params, group_rows, &mut values)
}

fn evaluate_instructions_reusing<S: ExpressionValues>(
    instructions: &[ExpressionInstruction],
    row: &[Value],
    params: &[Value],
    group_rows: Option<&[Row]>,
    values: &mut S,
) -> Result<Value> {
    values.reset()?;
    for instruction in instructions {
        match instruction {
            ExpressionInstruction::LoadColumn(index) => {
                values.push_value(row.get(*index).cloned().ok_or_else(|| {
                    DbError::internal(format!("bound column index {index} is out of range"))
                })?)?;
            }
            ExpressionInstruction::LoadLiteral(value) => values.push_value(value.clone())?,
            ExpressionInstruction::LoadParameter(index) => {
                values.push_value(params.get(index - 1).cloned().ok_or_else(|| {
                    DbError::new("42P02", format!("no value supplied for parameter ${index}"))
                })?)?;
            }
            ExpressionInstruction::Unary(operator) => {
                let value = values
                    .pop_value()
                    .ok_or_else(|| DbError::internal("expression value stack underflow"))?;
                values.push_value(evaluate_unary(*operator, value)?)?;
            }
            ExpressionInstruction::Binary {
                operator,
                operand_type,
            } => {
                let right = values
                    .pop_value()
                    .ok_or_else(|| DbError::internal("expression value stack underflow"))?;
                let left = values
                    .pop_value()
                    .ok_or_else(|| DbError::internal("expression value stack underflow"))?;
                values.push_value(evaluate_binary_as(left, *operator, right, operand_type)?)?;
            }
            ExpressionInstruction::InList {
                count,
                negated,
                operand_type,
            } => {
                values.collapse_in_list(*count, *negated, operand_type)?;
            }
            ExpressionInstruction::Cast(target) => {
                let value = values
                    .pop_value()
                    .ok_or_else(|| DbError::internal("expression value stack underflow"))?;
                values.push_value(cast_value(value, target)?)?;
            }
            ExpressionInstruction::MakeArray {
                count,
                element_type,
                dimensions,
            } => {
                let mut elements = Vec::with_capacity(*count);
                for _ in 0..*count {
                    elements.push(values.pop_value().ok_or_else(|| {
                        DbError::internal("expression array value stack underflow")
                    })?);
                }
                elements.reverse();
                values.push_value(Value::Array(PgArray::new(
                    element_type.clone(),
                    dimensions.clone(),
                    elements,
                )?))?;
            }
            ExpressionInstruction::Function { function, count } => {
                let mut arguments = Vec::with_capacity(*count);
                for _ in 0..*count {
                    arguments.push(values.pop_value().ok_or_else(|| {
                        DbError::internal("expression function value stack underflow")
                    })?);
                }
                arguments.reverse();
                values.push_value(evaluate_scalar_function(*function, arguments)?)?;
            }
            ExpressionInstruction::Aggregate {
                function,
                argument,
                argument_type,
            } => {
                let rows = group_rows.ok_or_else(|| {
                    DbError::internal("aggregate expression requires grouped rows")
                })?;
                values.push_value(evaluate_aggregate_program(
                    *function,
                    argument.as_deref(),
                    argument_type.as_ref(),
                    rows,
                    params,
                )?)?;
            }
            ExpressionInstruction::Coerce(target) => {
                let value = values
                    .pop_value()
                    .ok_or_else(|| DbError::internal("expression value stack underflow"))?;
                values.push_value(coerce_value(value, target)?)?;
            }
        }
    }
    if values.value_count() != 1 {
        return Err(DbError::internal(
            "expression program did not produce exactly one value",
        ));
    }
    values
        .pop_value()
        .ok_or_else(|| DbError::internal("expression result disappeared"))
}

pub fn evaluate(expr: &BoundExpr, row: &[Value], params: &[Value]) -> Result<Value> {
    ExpressionProgram::compile(expr)?.evaluate(row, params)
}

pub fn evaluate_group(
    expr: &BoundExpr,
    rows: &[Row],
    representative: &[Value],
    params: &[Value],
) -> Result<Value> {
    ExpressionProgram::compile_with_limit(expr, true, DEFAULT_MAX_EXPRESSION_DEPTH)?.evaluate_group(
        rows,
        representative,
        params,
    )
}

fn evaluate_aggregate_program(
    function: AggregateFunction,
    argument: Option<&[ExpressionInstruction]>,
    argument_type: Option<&ScalarType>,
    rows: &[Row],
    params: &[Value],
) -> Result<Value> {
    if function == AggregateFunction::Count {
        let count = if let Some(argument) = argument {
            rows.iter()
                .map(|row| evaluate_instructions(argument, &row.values, params, None))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .filter(|value| !value.is_null())
                .count()
        } else {
            rows.len()
        };
        return i64::try_from(count)
            .map(Value::Int64)
            .map_err(|_| DbError::new("22003", "COUNT result is out of range"));
    }
    let argument = argument.ok_or_else(|| DbError::internal("aggregate argument is missing"))?;
    let values = rows
        .iter()
        .map(|row| evaluate_instructions(argument, &row.values, params, None))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|value| !value.is_null())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Ok(Value::Null);
    }
    match function {
        AggregateFunction::Count => unreachable!("handled above"),
        AggregateFunction::Sum => sum_values(&values),
        AggregateFunction::Avg => {
            let sum = values.iter().try_fold(0.0, |sum, value| {
                numeric_f64(value).map(|value| sum + value)
            })?;
            Ok(Value::Float64(sum / values.len() as f64))
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let argument_type = argument_type.ok_or_else(|| {
                DbError::internal("MIN/MAX aggregate argument type is unavailable")
            })?;
            let mut selected = values[0].clone();
            for value in values.iter().skip(1) {
                let ordering = compare_values_as(value, &selected, argument_type)?;
                let replace = if function == AggregateFunction::Min {
                    ordering == Ordering::Less
                } else {
                    ordering == Ordering::Greater
                };
                if replace {
                    selected = value.clone();
                }
            }
            Ok(selected)
        }
    }
}

fn sum_values(values: &[Value]) -> Result<Value> {
    match &values[0] {
        Value::Int16(_) | Value::Int32(_) | Value::Int64(_) => values
            .iter()
            .try_fold(0_i64, |sum, value| {
                let value = match value {
                    Value::Int16(value) => i64::from(*value),
                    Value::Int32(value) => i64::from(*value),
                    Value::Int64(value) => *value,
                    _ => return Err(DbError::new("42804", "SUM values have mixed types")),
                };
                sum.checked_add(value)
                    .ok_or_else(|| DbError::new("22003", "SUM result is out of range"))
            })
            .map(Value::Int64),
        Value::Float32(_) | Value::Float64(_) => values
            .iter()
            .try_fold(0.0, |sum, value| {
                numeric_f64(value).map(|value| sum + value)
            })
            .map(Value::Float64),
        Value::Decimal(_) => values
            .iter()
            .try_fold(Decimal::ZERO, |sum, value| match value {
                Value::Decimal(value) => sum
                    .checked_add(*value)
                    .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
                _ => Err(DbError::new("42804", "SUM values have mixed types")),
            })
            .map(Value::Decimal),
        _ => Err(DbError::new("42804", "SUM requires numeric values")),
    }
}

fn numeric_f64(value: &Value) -> Result<f64> {
    match value {
        Value::Int16(value) => Ok(f64::from(*value)),
        Value::Int32(value) => Ok(f64::from(*value)),
        Value::Int64(value) => Ok(*value as f64),
        Value::Float32(value) => Ok(f64::from(*value)),
        Value::Float64(value) => Ok(*value),
        Value::Decimal(value) => value
            .to_string()
            .parse()
            .map_err(|_| DbError::new("22003", "decimal cannot be represented as FLOAT8")),
        _ => Err(DbError::new("42804", "numeric value required")),
    }
}

fn evaluate_unary(operator: UnaryOperator, value: Value) -> Result<Value> {
    match (operator, value) {
        (_, Value::Null) => Ok(Value::Null),
        (UnaryOperator::Not, Value::Boolean(value)) => Ok(Value::Boolean(!value)),
        (UnaryOperator::Negate, Value::Int16(value)) => value
            .checked_neg()
            .map(Value::Int16)
            .ok_or_else(|| DbError::new("22003", "numeric value out of range")),
        (UnaryOperator::Negate, Value::Int32(value)) => value
            .checked_neg()
            .map(Value::Int32)
            .ok_or_else(|| DbError::new("22003", "numeric value out of range")),
        (UnaryOperator::Negate, Value::Int64(value)) => value
            .checked_neg()
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "numeric value out of range")),
        (UnaryOperator::Negate, Value::Float32(value)) => Ok(Value::Float32(-value)),
        (UnaryOperator::Negate, Value::Float64(value)) => Ok(Value::Float64(-value)),
        (UnaryOperator::Negate, Value::Decimal(value)) => Ok(Value::Decimal(-value)),
        (UnaryOperator::Negate, Value::Interval(value)) => {
            Ok(Value::Interval(ordadb_types::PgInterval::new(
                value
                    .months
                    .checked_neg()
                    .ok_or_else(|| DbError::new("22015", "interval field is out of range"))?,
                value
                    .days
                    .checked_neg()
                    .ok_or_else(|| DbError::new("22015", "interval field is out of range"))?,
                value
                    .microseconds
                    .checked_neg()
                    .ok_or_else(|| DbError::new("22015", "interval field is out of range"))?,
            )))
        }
        _ => Err(DbError::new(
            "42804",
            "unary operator received an incompatible value",
        )),
    }
}

fn evaluate_binary(left: Value, operator: BinaryOperator, right: Value) -> Result<Value> {
    let operand_type = left
        .scalar_type()
        .or_else(|| right.scalar_type())
        .unwrap_or(ScalarType::Text);
    evaluate_binary_as(left, operator, right, &operand_type)
}

fn evaluate_binary_as(
    left: Value,
    operator: BinaryOperator,
    right: Value,
    operand_type: &ScalarType,
) -> Result<Value> {
    if matches!(operator, BinaryOperator::And | BinaryOperator::Or) {
        return evaluate_boolean_binary(left, operator, right);
    }
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    if matches!(
        operator,
        BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
    ) {
        return evaluate_arithmetic_binary(left, operator, right);
    }
    let equals = match (&left, &right) {
        (Value::Interval(left), Value::Interval(right)) => {
            interval_comparison_key(*left) == interval_comparison_key(*right)
        }
        _ => left == right,
    };
    match operator {
        BinaryOperator::Eq => return Ok(Value::Boolean(equals)),
        BinaryOperator::NotEq => return Ok(Value::Boolean(!equals)),
        _ => {}
    }
    let ordering = compare_values_as(&left, &right, operand_type)?;
    Ok(Value::Boolean(match operator {
        BinaryOperator::Lt => ordering == Ordering::Less,
        BinaryOperator::LtEq => ordering != Ordering::Greater,
        BinaryOperator::Gt => ordering == Ordering::Greater,
        BinaryOperator::GtEq => ordering != Ordering::Less,
        _ => unreachable!("handled above"),
    }))
}

fn evaluate_arithmetic_binary(
    left: Value,
    operator: BinaryOperator,
    right: Value,
) -> Result<Value> {
    macro_rules! checked_integer {
        ($left:expr, $right:expr, $variant:ident) => {{
            let value = match operator {
                BinaryOperator::Add => $left.checked_add($right),
                BinaryOperator::Subtract => $left.checked_sub($right),
                BinaryOperator::Multiply => $left.checked_mul($right),
                BinaryOperator::Divide if $right == 0 => return Err(division_by_zero()),
                BinaryOperator::Divide => $left.checked_div($right),
                BinaryOperator::Modulo if $right == 0 => return Err(division_by_zero()),
                BinaryOperator::Modulo => $left.checked_rem($right),
                _ => unreachable!("arithmetic operator checked by caller"),
            }
            .ok_or_else(numeric_out_of_range)?;
            Ok(Value::$variant(value))
        }};
    }

    match (left, right) {
        (Value::Int16(left), Value::Int16(right)) => checked_integer!(left, right, Int16),
        (Value::Int32(left), Value::Int32(right)) => checked_integer!(left, right, Int32),
        (Value::Int64(left), Value::Int64(right)) => checked_integer!(left, right, Int64),
        (Value::Float32(left), Value::Float32(right)) => {
            evaluate_float32_arithmetic(left, operator, right)
        }
        (Value::Float64(left), Value::Float64(right)) => {
            evaluate_float64_arithmetic(left, operator, right)
        }
        (Value::Decimal(left), Value::Decimal(right)) => {
            let value = match operator {
                BinaryOperator::Add => left.checked_add(right),
                BinaryOperator::Subtract => left.checked_sub(right),
                BinaryOperator::Multiply => left.checked_mul(right),
                BinaryOperator::Divide if right.is_zero() => return Err(division_by_zero()),
                BinaryOperator::Divide => left.checked_div(right),
                BinaryOperator::Modulo if right.is_zero() => return Err(division_by_zero()),
                BinaryOperator::Modulo => left.checked_rem(right),
                _ => unreachable!("arithmetic operator checked by caller"),
            }
            .ok_or_else(numeric_out_of_range)?;
            Ok(Value::Decimal(value))
        }
        _ => Err(DbError::new(
            "42883",
            "arithmetic operands do not have a common numeric type",
        )),
    }
}

fn evaluate_float32_arithmetic(left: f32, operator: BinaryOperator, right: f32) -> Result<Value> {
    if matches!(operator, BinaryOperator::Divide | BinaryOperator::Modulo) && right == 0.0 {
        return Err(division_by_zero());
    }
    let value = match operator {
        BinaryOperator::Add => left + right,
        BinaryOperator::Subtract => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => left / right,
        BinaryOperator::Modulo => left % right,
        _ => unreachable!("arithmetic operator checked by caller"),
    };
    if value.is_infinite() && left.is_finite() && right.is_finite() {
        return Err(numeric_out_of_range());
    }
    Ok(Value::Float32(value))
}

fn evaluate_float64_arithmetic(left: f64, operator: BinaryOperator, right: f64) -> Result<Value> {
    if matches!(operator, BinaryOperator::Divide | BinaryOperator::Modulo) && right == 0.0 {
        return Err(division_by_zero());
    }
    let value = match operator {
        BinaryOperator::Add => left + right,
        BinaryOperator::Subtract => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => left / right,
        BinaryOperator::Modulo => left % right,
        _ => unreachable!("arithmetic operator checked by caller"),
    };
    if value.is_infinite() && left.is_finite() && right.is_finite() {
        return Err(numeric_out_of_range());
    }
    Ok(Value::Float64(value))
}

fn division_by_zero() -> DbError {
    DbError::new("22012", "division by zero")
}

fn numeric_out_of_range() -> DbError {
    DbError::new("22003", "numeric value is out of range")
}

fn evaluate_boolean_binary(left: Value, operator: BinaryOperator, right: Value) -> Result<Value> {
    let left = boolean_or_null(left)?;
    let right = boolean_or_null(right)?;
    let value = match operator {
        BinaryOperator::And => match (left, right) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        BinaryOperator::Or => match (left, right) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        _ => unreachable!("only boolean operators are accepted"),
    };
    Ok(value.map_or(Value::Null, Value::Boolean))
}

fn boolean_or_null(value: Value) -> Result<Option<bool>> {
    match value {
        Value::Null => Ok(None),
        Value::Boolean(value) => Ok(Some(value)),
        _ => Err(DbError::new("42804", "boolean value required")),
    }
}

pub fn predicate_matches(expr: &BoundExpr, row: &Row, params: &[Value]) -> Result<bool> {
    match evaluate(expr, &row.values, params)? {
        Value::Boolean(value) => Ok(value),
        Value::Null => Ok(false),
        _ => Err(DbError::new("42804", "predicate must evaluate to boolean")),
    }
}

fn evaluate_limit_program(program: &ExpressionProgram, params: &[Value]) -> Result<usize> {
    match program.evaluate(&[], params)? {
        Value::Int64(value) if value >= 0 => {
            usize::try_from(value).map_err(|_| DbError::new("22003", "LIMIT value is out of range"))
        }
        Value::Null => Ok(usize::MAX),
        _ => Err(DbError::new(
            "2201W",
            "LIMIT must be a non-negative integer",
        )),
    }
}

fn evaluate_offset_program(program: &ExpressionProgram, params: &[Value]) -> Result<usize> {
    match program.evaluate(&[], params)? {
        Value::Int64(value) if value >= 0 => usize::try_from(value)
            .map_err(|_| DbError::new("22003", "OFFSET value is out of range")),
        Value::Null => Ok(0),
        _ => Err(DbError::new(
            "2201X",
            "OFFSET must be a non-negative integer",
        )),
    }
}

fn evaluate_scalar_function(function: ScalarFunction, arguments: Vec<Value>) -> Result<Value> {
    match function {
        ScalarFunction::Coalesce => Ok(arguments
            .into_iter()
            .find(|value| !value.is_null())
            .unwrap_or(Value::Null)),
        ScalarFunction::NullIf => {
            let mut arguments = arguments.into_iter();
            let left = arguments
                .next()
                .ok_or_else(|| DbError::internal("NULLIF lost its first argument"))?;
            let right = arguments
                .next()
                .ok_or_else(|| DbError::internal("NULLIF lost its second argument"))?;
            if left.is_null() || right.is_null() {
                return Ok(left);
            }
            match evaluate_binary(left.clone(), BinaryOperator::Eq, right)? {
                Value::Boolean(true) => Ok(Value::Null),
                Value::Boolean(false) | Value::Null => Ok(left),
                _ => Err(DbError::internal("NULLIF equality did not return boolean")),
            }
        }
        ScalarFunction::Concat => {
            let mut output = String::new();
            for argument in arguments {
                if !argument.is_null() {
                    output.push_str(&value_to_cast_text(&argument)?);
                }
            }
            Ok(Value::Text(output))
        }
        ScalarFunction::Greatest | ScalarFunction::Least => {
            let select_greatest = function == ScalarFunction::Greatest;
            let mut best = None;
            for argument in arguments {
                if argument.is_null() {
                    continue;
                }
                best = Some(match best {
                    None => argument,
                    Some(current) => {
                        let ordering = compare_values(&argument, &current)?;
                        if (select_greatest && ordering == Ordering::Greater)
                            || (!select_greatest && ordering == Ordering::Less)
                        {
                            argument
                        } else {
                            current
                        }
                    }
                });
            }
            Ok(best.unwrap_or(Value::Null))
        }
        _ if arguments.iter().any(Value::is_null) => Ok(Value::Null),
        ScalarFunction::Lower | ScalarFunction::Upper => {
            let Value::Text(value) = &arguments[0] else {
                return Err(DbError::new(
                    "42883",
                    "text function requires a textual argument",
                ));
            };
            Ok(Value::Text(if function == ScalarFunction::Lower {
                value.to_lowercase()
            } else {
                value.to_uppercase()
            }))
        }
        ScalarFunction::CharacterLength => match &arguments[0] {
            Value::Text(value) => i32::try_from(value.chars().count())
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "character length exceeds INTEGER range")),
            Value::Binary(value) => i32::try_from(value.len())
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "bytea length exceeds INTEGER range")),
            _ => Err(DbError::new("42883", "length requires text or bytea")),
        },
        ScalarFunction::OctetLength => match &arguments[0] {
            Value::Text(value) => i32::try_from(value.len())
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "octet length exceeds INTEGER range")),
            Value::Binary(value) => i32::try_from(value.len())
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "bytea length exceeds INTEGER range")),
            _ => Err(DbError::new("42883", "octet_length requires text or bytea")),
        },
        ScalarFunction::Abs => match arguments[0] {
            Value::Int16(value) => value
                .checked_abs()
                .map(Value::Int16)
                .ok_or_else(|| DbError::new("22003", "smallint out of range")),
            Value::Int32(value) => value
                .checked_abs()
                .map(Value::Int32)
                .ok_or_else(|| DbError::new("22003", "integer out of range")),
            Value::Int64(value) => value
                .checked_abs()
                .map(Value::Int64)
                .ok_or_else(|| DbError::new("22003", "bigint out of range")),
            Value::Float32(value) => Ok(Value::Float32(value.abs())),
            Value::Float64(value) => Ok(Value::Float64(value.abs())),
            Value::Decimal(value) => Ok(Value::Decimal(value.abs())),
            _ => Err(DbError::new("42883", "ABS requires a numeric argument")),
        },
        ScalarFunction::Substring => evaluate_substring(&arguments),
        ScalarFunction::Btrim | ScalarFunction::Ltrim | ScalarFunction::Rtrim => {
            let Value::Text(value) = &arguments[0] else {
                return Err(DbError::new("42883", "trim requires textual arguments"));
            };
            let characters = arguments
                .get(1)
                .map(|argument| match argument {
                    Value::Text(characters) => Ok(characters.as_str()),
                    _ => Err(DbError::new("42883", "trim characters must be text")),
                })
                .transpose()?;
            let trimmed = match (function, characters) {
                (ScalarFunction::Btrim, Some(characters)) => {
                    value.trim_matches(|character| characters.contains(character))
                }
                (ScalarFunction::Ltrim, Some(characters)) => {
                    value.trim_start_matches(|character| characters.contains(character))
                }
                (ScalarFunction::Rtrim, Some(characters)) => {
                    value.trim_end_matches(|character| characters.contains(character))
                }
                (ScalarFunction::Btrim, None) => value.trim(),
                (ScalarFunction::Ltrim, None) => value.trim_start(),
                (ScalarFunction::Rtrim, None) => value.trim_end(),
                _ => return Err(DbError::internal("unexpected trim function")),
            };
            Ok(Value::Text(trimmed.to_owned()))
        }
        ScalarFunction::Replace => {
            let (Value::Text(value), Value::Text(from), Value::Text(to)) =
                (&arguments[0], &arguments[1], &arguments[2])
            else {
                return Err(DbError::new(
                    "42883",
                    "replace requires three textual arguments",
                ));
            };
            Ok(Value::Text(value.replace(from, to)))
        }
        ScalarFunction::Strpos => {
            let (Value::Text(value), Value::Text(needle)) = (&arguments[0], &arguments[1]) else {
                return Err(DbError::new(
                    "42883",
                    "strpos requires two textual arguments",
                ));
            };
            let position = value.find(needle).map_or(0_usize, |byte_offset| {
                value[..byte_offset].chars().count().saturating_add(1)
            });
            i32::try_from(position)
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "text position exceeds INTEGER range"))
        }
        ScalarFunction::JsonbTypeof => {
            let Value::Jsonb(value) = &arguments[0] else {
                return Err(DbError::new("42883", "jsonb_typeof requires jsonb"));
            };
            let name = match value {
                serde_json::Value::Null => "null",
                serde_json::Value::Bool(_) => "boolean",
                serde_json::Value::Number(_) => "number",
                serde_json::Value::String(_) => "string",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Object(_) => "object",
            };
            Ok(Value::Text(name.to_owned()))
        }
        ScalarFunction::ArrayLength => {
            let (Value::Array(array), Value::Int32(dimension)) = (&arguments[0], &arguments[1])
            else {
                return Err(DbError::new(
                    "42883",
                    "array_length requires array, integer",
                ));
            };
            let Some(index) = dimension
                .checked_sub(1)
                .and_then(|value| usize::try_from(value).ok())
            else {
                return Ok(Value::Null);
            };
            array
                .dimensions()
                .get(index)
                .map_or(Ok(Value::Null), |dimension| {
                    i32::try_from(dimension.length)
                        .map(Value::Int32)
                        .map_err(|_| DbError::new("22003", "array length exceeds INTEGER range"))
                })
        }
        ScalarFunction::Cardinality => {
            let Value::Array(array) = &arguments[0] else {
                return Err(DbError::new("42883", "cardinality requires an array"));
            };
            i32::try_from(array.values().len())
                .map(Value::Int32)
                .map_err(|_| DbError::new("22003", "array cardinality exceeds INTEGER range"))
        }
    }
}

fn evaluate_substring(arguments: &[Value]) -> Result<Value> {
    let (Value::Text(value), Value::Int32(start)) = (&arguments[0], &arguments[1]) else {
        return Err(DbError::new("42883", "substring requires text, integer"));
    };
    let requested_length = arguments
        .get(2)
        .map(|value| match value {
            Value::Int32(length) if *length >= 0 => Ok(i64::from(*length)),
            Value::Int32(_) => Err(DbError::new(
                "22011",
                "negative substring length not allowed",
            )),
            _ => Err(DbError::new("42883", "substring length must be integer")),
        })
        .transpose()?;
    let start = i64::from(*start) - 1;
    let end = requested_length.and_then(|length| start.checked_add(length));
    let begin = usize::try_from(start.max(0)).unwrap_or(usize::MAX);
    let take = end.map_or(usize::MAX, |end| {
        usize::try_from(end.max(0).saturating_sub(start.max(0))).unwrap_or(usize::MAX)
    });
    Ok(Value::Text(value.chars().skip(begin).take(take).collect()))
}

pub fn cast_value(value: Value, target: &ScalarType) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    if matches!(target, ScalarType::Enum { .. })
        || matches!(target, ScalarType::Array { element } if matches!(element.as_ref(), ScalarType::Enum { .. }))
    {
        return coerce_value(value, target);
    }
    if let Ok(value) = coerce_value(value.clone(), target) {
        return Ok(value);
    }
    if matches!(
        target,
        ScalarType::Text | ScalarType::Char { .. } | ScalarType::Varchar { .. }
    ) {
        return cast_text_result(value_to_cast_text(&value)?, target);
    }
    if value.scalar_type().as_ref().is_some_and(is_numeric_type) && is_numeric_type(target) {
        return cast_numeric(value, target);
    }
    match (value, target) {
        (Value::Text(value), ScalarType::Boolean) => parse_boolean_text(&value).map(Value::Boolean),
        (Value::Text(value), target) if is_numeric_type(target) => {
            cast_numeric_text(&value, target)
        }
        (Value::Text(value), ScalarType::Binary) => parse_bytea_text(&value).map(Value::Binary),
        (Value::Text(value), ScalarType::Date) => NaiveDate::parse_from_str(&value, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|_| invalid_text_cast(&value, target)),
        (Value::Text(value), ScalarType::Time) => NaiveTime::parse_from_str(&value, "%H:%M:%S%.f")
            .map(Value::Time)
            .map_err(|_| invalid_text_cast(&value, target)),
        (Value::Text(value), ScalarType::Timestamp { .. }) => {
            NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%.f"))
                .map(Value::Timestamp)
                .map_err(|_| invalid_text_cast(&value, target))
        }
        (Value::Text(value), ScalarType::Interval) => PgInterval::from_str(&value)
            .map(Value::Interval)
            .map_err(|error| DbError::new(error.sql_state, error.message)),
        (Value::Text(value), ScalarType::Json) => serde_json::from_str(&value)
            .map(Value::Json)
            .map_err(|_| invalid_text_cast(&value, target)),
        (Value::Text(value), ScalarType::Jsonb) => serde_json::from_str(&value)
            .map(Value::Jsonb)
            .map_err(|_| invalid_text_cast(&value, target)),
        (Value::Text(value), ScalarType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(Value::Uuid)
            .map_err(|_| invalid_text_cast(&value, target)),
        (Value::Date(value), ScalarType::Timestamp { .. }) => value
            .and_hms_opt(0, 0, 0)
            .map(Value::Timestamp)
            .ok_or_else(|| DbError::new("22008", "date is outside timestamp range")),
        (Value::Timestamp(value), ScalarType::Date) => Ok(Value::Date(value.date())),
        (Value::Timestamp(value), ScalarType::Time) => Ok(Value::Time(value.time())),
        (Value::Timestamp(value), ScalarType::Timestamp { .. }) => Ok(Value::Timestamp(value)),
        (Value::Json(value), ScalarType::Jsonb) => Ok(Value::Jsonb(value)),
        (Value::Jsonb(value), ScalarType::Json) => Ok(Value::Json(value)),
        (Value::Array(value), ScalarType::Array { element }) => {
            let dimensions = value.dimensions().to_vec();
            let values = value
                .values()
                .iter()
                .cloned()
                .map(|value| cast_value(value, element))
                .collect::<Result<Vec<_>>>()?;
            PgArray::new(element.as_ref().clone(), dimensions, values).map(Value::Array)
        }
        (value, target) => Err(DbError::new(
            "42846",
            format!("cannot cast value {value:?} to {target:?}"),
        )),
    }
}

fn is_numeric_type(data_type: &ScalarType) -> bool {
    matches!(
        data_type,
        ScalarType::Int16
            | ScalarType::Int32
            | ScalarType::Int64
            | ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Decimal { .. }
    )
}

fn cast_numeric(value: Value, target: &ScalarType) -> Result<Value> {
    match target {
        ScalarType::Int16 => numeric_to_i128(&value)
            .and_then(|value| i16::try_from(value).map_err(|_| cast_numeric_out_of_range(target)))
            .map(Value::Int16),
        ScalarType::Int32 => numeric_to_i128(&value)
            .and_then(|value| i32::try_from(value).map_err(|_| cast_numeric_out_of_range(target)))
            .map(Value::Int32),
        ScalarType::Int64 => numeric_to_i128(&value)
            .and_then(|value| i64::try_from(value).map_err(|_| cast_numeric_out_of_range(target)))
            .map(Value::Int64),
        ScalarType::Float32 => {
            let value = numeric_to_f64(&value)?;
            let narrowed = value as f32;
            if narrowed.is_finite() || value == 0.0 {
                Ok(Value::Float32(narrowed))
            } else {
                Err(cast_numeric_out_of_range(target))
            }
        }
        ScalarType::Float64 => numeric_to_f64(&value).map(Value::Float64),
        ScalarType::Decimal { .. } => numeric_to_decimal(&value).map(Value::Decimal),
        _ => Err(DbError::internal(
            "numeric cast received a non-numeric target",
        )),
    }
}

fn numeric_to_i128(value: &Value) -> Result<i128> {
    match value {
        Value::Int16(value) => Ok(i128::from(*value)),
        Value::Int32(value) => Ok(i128::from(*value)),
        Value::Int64(value) => Ok(i128::from(*value)),
        Value::Float32(value) if value.is_finite() => (*value as f64)
            .round()
            .to_i128()
            .ok_or_else(|| cast_numeric_out_of_range(&ScalarType::Int64)),
        Value::Float64(value) if value.is_finite() => value
            .round()
            .to_i128()
            .ok_or_else(|| cast_numeric_out_of_range(&ScalarType::Int64)),
        Value::Decimal(value) => value
            .round()
            .to_i128()
            .ok_or_else(|| cast_numeric_out_of_range(&ScalarType::Int64)),
        _ => Err(cast_numeric_out_of_range(&ScalarType::Int64)),
    }
}

fn numeric_to_f64(value: &Value) -> Result<f64> {
    let value = match value {
        Value::Int16(value) => f64::from(*value),
        Value::Int32(value) => f64::from(*value),
        Value::Int64(value) => *value as f64,
        Value::Float32(value) => f64::from(*value),
        Value::Float64(value) => *value,
        Value::Decimal(value) => value
            .to_f64()
            .ok_or_else(|| cast_numeric_out_of_range(&ScalarType::Float64))?,
        _ => {
            return Err(DbError::internal(
                "numeric conversion received a non-numeric value",
            ));
        }
    };
    if value.is_finite() {
        Ok(value)
    } else {
        Err(cast_numeric_out_of_range(&ScalarType::Float64))
    }
}

fn numeric_to_decimal(value: &Value) -> Result<Decimal> {
    match value {
        Value::Int16(value) => Ok(Decimal::from(*value)),
        Value::Int32(value) => Ok(Decimal::from(*value)),
        Value::Int64(value) => Ok(Decimal::from(*value)),
        Value::Float32(value) => Decimal::from_f32(*value).ok_or_else(|| {
            cast_numeric_out_of_range(&ScalarType::Decimal {
                precision: None,
                scale: None,
            })
        }),
        Value::Float64(value) => Decimal::from_f64(*value).ok_or_else(|| {
            cast_numeric_out_of_range(&ScalarType::Decimal {
                precision: None,
                scale: None,
            })
        }),
        Value::Decimal(value) => Ok(*value),
        _ => Err(DbError::internal(
            "decimal conversion received a non-numeric value",
        )),
    }
}

fn cast_numeric_text(value: &str, target: &ScalarType) -> Result<Value> {
    let invalid = || invalid_text_cast(value, target);
    match target {
        ScalarType::Int16 => value
            .parse::<i16>()
            .map(Value::Int16)
            .map_err(|_| invalid()),
        ScalarType::Int32 => value
            .parse::<i32>()
            .map(Value::Int32)
            .map_err(|_| invalid()),
        ScalarType::Int64 => value
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|_| invalid()),
        ScalarType::Float32 => value
            .parse::<f32>()
            .map_err(|_| invalid())
            .and_then(|value| {
                value
                    .is_finite()
                    .then_some(Value::Float32(value))
                    .ok_or_else(invalid)
            }),
        ScalarType::Float64 => value
            .parse::<f64>()
            .map_err(|_| invalid())
            .and_then(|value| {
                value
                    .is_finite()
                    .then_some(Value::Float64(value))
                    .ok_or_else(invalid)
            }),
        ScalarType::Decimal { .. } => Decimal::from_str(value)
            .map(Value::Decimal)
            .map_err(|_| invalid()),
        _ => Err(DbError::internal(
            "text numeric cast received a non-numeric target",
        )),
    }
}

fn parse_boolean_text(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Ok(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Ok(false),
        _ => Err(DbError::new(
            "22P02",
            format!("invalid input syntax for type boolean: {value:?}"),
        )),
    }
}

fn cast_text_result(value: String, target: &ScalarType) -> Result<Value> {
    let limit = match target {
        ScalarType::Char { length } | ScalarType::Varchar { length } => *length,
        ScalarType::Text => None,
        _ => return Err(DbError::internal("text cast received a non-text target")),
    };
    let value = limit.map_or(value.clone(), |length| {
        value
            .chars()
            .take(usize::try_from(length).unwrap_or(usize::MAX))
            .collect()
    });
    Ok(Value::Text(value))
}

fn value_to_cast_text(value: &Value) -> Result<String> {
    match value {
        Value::Null => Ok(String::new()),
        Value::Boolean(value) => Ok(if *value { "true" } else { "false" }.to_owned()),
        Value::Int16(value) => Ok(value.to_string()),
        Value::Int32(value) => Ok(value.to_string()),
        Value::Int64(value) => Ok(value.to_string()),
        Value::Float32(value) => Ok(value.to_string()),
        Value::Float64(value) => Ok(value.to_string()),
        Value::Decimal(value) => Ok(value.to_string()),
        Value::Text(value) => Ok(value.clone()),
        Value::Binary(value) => Ok(format_bytea(value)),
        Value::Date(value) => Ok(value.format("%Y-%m-%d").to_string()),
        Value::Time(value) => Ok(value.format("%H:%M:%S%.f").to_string()),
        Value::Timestamp(value) => Ok(value.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
        Value::Interval(value) => Ok(value.to_string()),
        Value::Array(value) => array_to_text(value),
        Value::Json(value) | Value::Jsonb(value) => serde_json::to_string(value)
            .map_err(|error| DbError::internal(format!("JSON serialization failed: {error}"))),
        Value::Uuid(value) => Ok(value.to_string()),
        Value::Vector(values) => Ok(format!(
            "[{}]",
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )),
    }
}

fn parse_bytea_text(value: &str) -> Result<Vec<u8>> {
    let Some(hex) = value.strip_prefix("\\x") else {
        return Ok(value.as_bytes().to_vec());
    };
    if hex.len() % 2 != 0 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DbError::new("22P02", "invalid hexadecimal bytea input"));
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)
                .map_err(|_| DbError::new("22P02", "invalid hexadecimal bytea input"))?;
            u8::from_str_radix(text, 16)
                .map_err(|_| DbError::new("22P02", "invalid hexadecimal bytea input"))
        })
        .collect()
}

fn format_bytea(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len().saturating_mul(2).saturating_add(2));
    output.push_str("\\x");
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn array_to_text(array: &PgArray) -> Result<String> {
    if array.dimensions().is_empty() {
        return Ok("{}".to_owned());
    }
    let mut output = String::new();
    if array
        .dimensions()
        .iter()
        .any(|dimension| dimension.lower_bound != 1)
    {
        for dimension in array.dimensions() {
            let upper = i64::from(dimension.lower_bound)
                .checked_add(i64::from(dimension.length))
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| DbError::new("2202E", "array bounds overflow"))?;
            output.push_str(&format!("[{}:{upper}]", dimension.lower_bound));
        }
        output.push('=');
    }
    let mut cursor = 0_usize;
    write_array_dimension(array, 0, &mut cursor, &mut output)?;
    Ok(output)
}

fn write_array_dimension(
    array: &PgArray,
    dimension: usize,
    cursor: &mut usize,
    output: &mut String,
) -> Result<()> {
    let current = array
        .dimensions()
        .get(dimension)
        .ok_or_else(|| DbError::internal("array text writer lost its dimension"))?;
    output.push('{');
    for index in 0..current.length {
        if index > 0 {
            output.push(',');
        }
        if dimension + 1 < array.dimensions().len() {
            write_array_dimension(array, dimension + 1, cursor, output)?;
        } else {
            let value = array
                .values()
                .get(*cursor)
                .ok_or_else(|| DbError::internal("array text writer exceeded its values"))?;
            *cursor += 1;
            if value.is_null() {
                output.push_str("NULL");
            } else {
                let text = value_to_cast_text(value)?;
                output.push('"');
                for character in text.chars() {
                    if matches!(character, '"' | '\\') {
                        output.push('\\');
                    }
                    output.push(character);
                }
                output.push('"');
            }
        }
    }
    output.push('}');
    Ok(())
}

fn invalid_text_cast(value: &str, target: &ScalarType) -> DbError {
    DbError::new(
        "22P02",
        format!("invalid input syntax for type {target:?}: {value:?}"),
    )
}

fn cast_numeric_out_of_range(target: &ScalarType) -> DbError {
    DbError::new(
        "22003",
        format!("numeric value is out of range for {target:?}"),
    )
}

pub fn coerce_value(value: Value, target: &ScalarType) -> Result<Value> {
    if value.is_null() {
        return Ok(value);
    }
    match (value, target) {
        (Value::Boolean(value), ScalarType::Boolean) => Ok(Value::Boolean(value)),
        (Value::Int16(value), ScalarType::Int16) => Ok(Value::Int16(value)),
        (Value::Int16(value), ScalarType::Int32) => Ok(Value::Int32(i32::from(value))),
        (Value::Int16(value), ScalarType::Int64) => Ok(Value::Int64(i64::from(value))),
        (Value::Int16(value), ScalarType::Float32) => Ok(Value::Float32(f32::from(value))),
        (Value::Int16(value), ScalarType::Float64) => Ok(Value::Float64(f64::from(value))),
        (Value::Int16(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Int32(value), ScalarType::Int32) => Ok(Value::Int32(value)),
        (Value::Int32(value), ScalarType::Int64) => Ok(Value::Int64(i64::from(value))),
        (Value::Int32(value), ScalarType::Float64) => Ok(Value::Float64(f64::from(value))),
        (Value::Int32(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Int64(value), ScalarType::Int64) => Ok(Value::Int64(value)),
        (Value::Int64(value), ScalarType::Float64) => Ok(Value::Float64(value as f64)),
        (Value::Int64(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Float32(value), ScalarType::Float32) => Ok(Value::Float32(value)),
        (Value::Float64(value), ScalarType::Float64) => Ok(Value::Float64(value)),
        (Value::Decimal(value), ScalarType::Decimal { .. }) => Ok(Value::Decimal(value)),
        (
            Value::Text(value),
            ScalarType::Text | ScalarType::Char { .. } | ScalarType::Varchar { .. },
        ) => Ok(Value::Text(value)),
        (Value::Text(value), ScalarType::Enum { labels, .. }) => {
            if labels.iter().any(|label| label == &value) {
                Ok(Value::Text(value))
            } else {
                Err(DbError::new(
                    "22P02",
                    format!("invalid input value for enum: {value}"),
                ))
            }
        }
        (Value::Binary(value), ScalarType::Binary) => Ok(Value::Binary(value)),
        (Value::Date(value), ScalarType::Date) => Ok(Value::Date(value)),
        (Value::Time(value), ScalarType::Time) => Ok(Value::Time(value)),
        (
            Value::Timestamp(value),
            ScalarType::Timestamp {
                with_timezone: false,
            },
        ) => Ok(Value::Timestamp(value)),
        (
            Value::Timestamp(value),
            ScalarType::Timestamp {
                with_timezone: true,
            },
        ) => Ok(Value::Timestamp(value)),
        (Value::Interval(value), ScalarType::Interval) => Ok(Value::Interval(value)),
        (Value::Array(value), ScalarType::Array { element }) => {
            let dimensions = value.dimensions().to_vec();
            let values = value
                .values()
                .iter()
                .cloned()
                .map(|value| coerce_value(value, element))
                .collect::<Result<Vec<_>>>()?;
            ordadb_types::PgArray::new(element.as_ref().clone(), dimensions, values)
                .map(Value::Array)
        }
        (Value::Json(value), ScalarType::Json) => Ok(Value::Json(value)),
        (Value::Jsonb(value), ScalarType::Jsonb) => Ok(Value::Jsonb(value)),
        (Value::Uuid(value), ScalarType::Uuid) => Ok(Value::Uuid(value)),
        (Value::Vector(value), ScalarType::Vector { dimensions })
            if dimensions.is_none_or(|dimensions| dimensions == value.len()) =>
        {
            Ok(Value::Vector(value))
        }
        (value, target) => Err(DbError::new(
            "42804",
            format!("value {value:?} cannot be assigned to {target:?}"),
        )),
    }
}

fn compare_rows(left: &Row, right: &Row, order_by: &[BoundOrder]) -> Result<Ordering> {
    for order in order_by {
        let left_value = &left.values[order.column_index];
        let right_value = &right.values[order.column_index];
        let ordering = match (left_value.is_null(), right_value.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if order.nulls_first.unwrap_or(!order.ascending) {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if order.nulls_first.unwrap_or(!order.ascending) {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = compare_values_as(left_value, right_value, &order.data_type)?;
                if order.ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            }
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

pub fn compare_values(left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        (Value::Boolean(left), Value::Boolean(right)) => Ok(left.cmp(right)),
        (Value::Int16(left), Value::Int16(right)) => Ok(left.cmp(right)),
        (Value::Int32(left), Value::Int32(right)) => Ok(left.cmp(right)),
        (Value::Int64(left), Value::Int64(right)) => Ok(left.cmp(right)),
        (Value::Float32(left), Value::Float32(right)) => left
            .partial_cmp(right)
            .ok_or_else(|| DbError::new("22000", "NaN values are not orderable")),
        (Value::Float64(left), Value::Float64(right)) => left
            .partial_cmp(right)
            .ok_or_else(|| DbError::new("22000", "NaN values are not orderable")),
        (Value::Decimal(left), Value::Decimal(right)) => Ok(left.cmp(right)),
        (Value::Text(left), Value::Text(right)) => Ok(left.cmp(right)),
        (Value::Binary(left), Value::Binary(right)) => Ok(left.cmp(right)),
        (Value::Date(left), Value::Date(right)) => Ok(left.cmp(right)),
        (Value::Time(left), Value::Time(right)) => Ok(left.cmp(right)),
        (Value::Timestamp(left), Value::Timestamp(right)) => Ok(left.cmp(right)),
        (Value::Interval(left), Value::Interval(right)) => {
            Ok(interval_comparison_key(*left).cmp(&interval_comparison_key(*right)))
        }
        (Value::Array(left), Value::Array(right)) => compare_arrays(left, right),
        (Value::Uuid(left), Value::Uuid(right)) => Ok(left.cmp(right)),
        _ => Err(DbError::new(
            "42883",
            "values do not have a compatible ordering operator",
        )),
    }
}

pub fn compare_values_as(left: &Value, right: &Value, data_type: &ScalarType) -> Result<Ordering> {
    match data_type {
        ScalarType::Enum { labels, .. } => {
            let (Value::Text(left), Value::Text(right)) = (left, right) else {
                return Err(DbError::new(
                    "42804",
                    "enum comparison requires enum text values",
                ));
            };
            let left = labels
                .iter()
                .position(|label| label == left)
                .ok_or_else(|| {
                    DbError::new("22P02", format!("invalid input value for enum: {left}"))
                })?;
            let right = labels
                .iter()
                .position(|label| label == right)
                .ok_or_else(|| {
                    DbError::new("22P02", format!("invalid input value for enum: {right}"))
                })?;
            Ok(left.cmp(&right))
        }
        ScalarType::Array { element } => compare_arrays_as(left, right, element),
        _ => compare_values(left, right),
    }
}

fn compare_arrays_as(left: &Value, right: &Value, element: &ScalarType) -> Result<Ordering> {
    let (Value::Array(left), Value::Array(right)) = (left, right) else {
        return Err(DbError::new(
            "42804",
            "array comparison requires array values",
        ));
    };
    if left.element_type() != element || right.element_type() != element {
        return Err(DbError::new(
            "42883",
            "arrays do not have compatible element types",
        ));
    }
    for (left, right) in left.values().iter().zip(right.values()) {
        let ordering = match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => compare_values_as(left, right, element)?,
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    let length_ordering = left.values().len().cmp(&right.values().len());
    if length_ordering != Ordering::Equal {
        return Ok(length_ordering);
    }
    Ok(left
        .dimensions()
        .iter()
        .map(|dimension| (dimension.length, dimension.lower_bound))
        .cmp(
            right
                .dimensions()
                .iter()
                .map(|dimension| (dimension.length, dimension.lower_bound)),
        ))
}

fn interval_comparison_key(value: ordadb_types::PgInterval) -> i128 {
    const MICROS_PER_DAY: i128 = 86_400_000_000;
    (i128::from(value.months) * 30 + i128::from(value.days)) * MICROS_PER_DAY
        + i128::from(value.microseconds)
}

fn compare_arrays(left: &ordadb_types::PgArray, right: &ordadb_types::PgArray) -> Result<Ordering> {
    if left.element_type() != right.element_type() {
        return Err(DbError::new(
            "42883",
            "arrays do not have compatible element types",
        ));
    }
    for (left, right) in left.values().iter().zip(right.values()) {
        let ordering = match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => compare_values(left, right)?,
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    let length_ordering = left.values().len().cmp(&right.values().len());
    if length_ordering != Ordering::Equal {
        return Ok(length_ordering);
    }
    Ok(left
        .dimensions()
        .iter()
        .map(|dimension| (dimension.length, dimension.lower_bound))
        .cmp(
            right
                .dimensions()
                .iter()
                .map(|dimension| (dimension.length, dimension.lower_bound)),
        ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use ordadb_catalog::{Catalog, NewColumn};
    use ordadb_optimizer::{AccessPath, PlanKind, PlanNode, optimize_select};
    use ordadb_sql::{
        BinaryOperator, BoundExpr, BoundExprKind, BoundOrder, BoundStatement, ScalarFunction,
        UnaryOperator, bind, parse,
    };
    use ordadb_types::{
        ArrayDimension, Identifier, Row, ScalarType, Schema, TableId, TypeId, Value,
    };
    use tempfile::TempDir;

    use super::{
        DEFAULT_MAX_EXPRESSION_DEPTH, DEFAULT_MAX_PLAN_DEPTH, ExecutionContext, ExecutionCursor,
        ExecutionOptions, ExpressionProgram, ExpressionStack, MAX_SPILL_MERGE_FAN_IN, MemoryGrant,
        SPILL_MAGIC, SPILL_VERSION, SpillManager, SpillMergeCursor, SpillRun, evaluate, execute,
    };

    type TestTables = BTreeMap<TableId, Arc<Vec<Row>>>;
    type TestIndexes = BTreeMap<ordadb_types::IndexId, Arc<ordadb_index::BPlusTree>>;
    type TestFixture = (PlanNode, Schema, TestTables, TestIndexes);

    #[test]
    fn evaluates_explicit_scalar_and_multidimensional_array_casts() {
        let scalar = BoundExpr {
            kind: BoundExprKind::Cast {
                expr: Box::new(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Text("42".to_owned())),
                    data_type: ScalarType::Text,
                    nullable: false,
                }),
            },
            data_type: ScalarType::Int64,
            nullable: false,
        };
        assert_eq!(
            evaluate(&scalar, &[], &[]).expect("cast scalar"),
            Value::Int64(42)
        );

        let source_type = ScalarType::Array {
            element: Box::new(ScalarType::Int32),
        };
        let array = BoundExpr {
            kind: BoundExprKind::Array {
                elements: [1, 2, 3, 4]
                    .into_iter()
                    .map(|value| BoundExpr {
                        kind: BoundExprKind::Literal(Value::Int32(value)),
                        data_type: ScalarType::Int32,
                        nullable: false,
                    })
                    .collect(),
                dimensions: vec![ArrayDimension::new(2, 1), ArrayDimension::new(2, 1)],
            },
            data_type: source_type,
            nullable: false,
        };
        let cast = BoundExpr {
            kind: BoundExprKind::Cast {
                expr: Box::new(array),
            },
            data_type: ScalarType::Array {
                element: Box::new(ScalarType::Int64),
            },
            nullable: false,
        };
        let Value::Array(value) = evaluate(&cast, &[], &[]).expect("cast array") else {
            panic!("array result");
        };
        assert_eq!(
            value.values(),
            [
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(3),
                Value::Int64(4),
            ]
        );
        assert_eq!(
            value.dimensions(),
            [ArrayDimension::new(2, 1), ArrayDimension::new(2, 1)]
        );

        let enum_type = ScalarType::Enum {
            type_id: TypeId::new(7),
            labels: vec!["sad".into(), "happy".into()],
        };
        let invalid_enum = BoundExpr {
            kind: BoundExprKind::Cast {
                expr: Box::new(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Text("angry".into())),
                    data_type: ScalarType::Text,
                    nullable: false,
                }),
            },
            data_type: enum_type.clone(),
            nullable: false,
        };
        assert_eq!(
            evaluate(&invalid_enum, &[], &[])
                .expect_err("invalid enum cast")
                .sql_state,
            "22P02"
        );

        let invalid_enum_array = BoundExpr {
            kind: BoundExprKind::Cast {
                expr: Box::new(BoundExpr {
                    kind: BoundExprKind::Array {
                        elements: vec![BoundExpr {
                            kind: BoundExprKind::Literal(Value::Text("angry".into())),
                            data_type: ScalarType::Text,
                            nullable: false,
                        }],
                        dimensions: vec![ArrayDimension::new(1, 1)],
                    },
                    data_type: ScalarType::Array {
                        element: Box::new(ScalarType::Text),
                    },
                    nullable: false,
                }),
            },
            data_type: ScalarType::Array {
                element: Box::new(enum_type),
            },
            nullable: false,
        };
        assert_eq!(
            evaluate(&invalid_enum_array, &[], &[])
                .expect_err("invalid enum array cast")
                .sql_state,
            "22P02"
        );
    }

    #[test]
    fn evaluates_common_scalar_functions_on_the_expression_stack() {
        let text = |value: &str| BoundExpr {
            kind: BoundExprKind::Literal(Value::Text(value.to_owned())),
            data_type: ScalarType::Text,
            nullable: false,
        };
        let integer = |value| BoundExpr {
            kind: BoundExprKind::Literal(Value::Int32(value)),
            data_type: ScalarType::Int32,
            nullable: false,
        };
        let lower = BoundExpr {
            kind: BoundExprKind::Function {
                function: ScalarFunction::Lower,
                arguments: vec![text("ÄBC")],
            },
            data_type: ScalarType::Text,
            nullable: false,
        };
        assert_eq!(
            evaluate(&lower, &[], &[]).expect("lower"),
            Value::Text("äbc".to_owned())
        );

        let substring = BoundExpr {
            kind: BoundExprKind::Function {
                function: ScalarFunction::Substring,
                arguments: vec![text("abcdef"), integer(2), integer(3)],
            },
            data_type: ScalarType::Text,
            nullable: false,
        };
        assert_eq!(
            evaluate(&substring, &[], &[]).expect("substring"),
            Value::Text("bcd".to_owned())
        );

        let coalesce = BoundExpr {
            kind: BoundExprKind::Function {
                function: ScalarFunction::Coalesce,
                arguments: vec![
                    BoundExpr {
                        kind: BoundExprKind::Literal(Value::Null),
                        data_type: ScalarType::Text,
                        nullable: true,
                    },
                    text("fallback"),
                ],
            },
            data_type: ScalarType::Text,
            nullable: false,
        };
        assert_eq!(
            evaluate(&coalesce, &[], &[]).expect("coalesce"),
            Value::Text("fallback".to_owned())
        );

        let scalar = |function, arguments, data_type| BoundExpr {
            kind: BoundExprKind::Function {
                function,
                arguments,
            },
            data_type,
            nullable: false,
        };
        assert_eq!(
            evaluate(
                &scalar(
                    ScalarFunction::Btrim,
                    vec![text("xyhelloxy"), text("xy")],
                    ScalarType::Text,
                ),
                &[],
                &[],
            )
            .expect("btrim"),
            Value::Text("hello".to_owned())
        );
        assert_eq!(
            evaluate(
                &scalar(
                    ScalarFunction::Replace,
                    vec![text("café"), text("fé"), text("ke")],
                    ScalarType::Text,
                ),
                &[],
                &[],
            )
            .expect("replace"),
            Value::Text("cake".to_owned())
        );
        assert_eq!(
            evaluate(
                &scalar(
                    ScalarFunction::Strpos,
                    vec![text("åbcå"), text("c")],
                    ScalarType::Int32,
                ),
                &[],
                &[],
            )
            .expect("strpos"),
            Value::Int32(3)
        );
        assert_eq!(
            evaluate(
                &scalar(
                    ScalarFunction::Greatest,
                    vec![
                        integer(3),
                        BoundExpr {
                            kind: BoundExprKind::Literal(Value::Null),
                            data_type: ScalarType::Int32,
                            nullable: true,
                        },
                        integer(8),
                    ],
                    ScalarType::Int32,
                ),
                &[],
                &[],
            )
            .expect("greatest"),
            Value::Int32(8)
        );
    }

    fn fixture(query: &str, rows: Vec<Row>) -> TestFixture {
        let mut catalog = Catalog::default();
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("items"),
                vec![
                    NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                    NewColumn::new(Identifier::unquoted("payload"), ScalarType::Text),
                ],
            )
            .expect("table");
        let BoundStatement::Select {
            schema,
            projection,
            filter,
            order_by,
            offset,
            limit,
            ..
        } = bind(parse(query).expect("parse"), &catalog).expect("bind")
        else {
            panic!("simple SELECT");
        };
        let plan = optimize_select(
            catalog.table_by_id(table_id).expect("definition"),
            projection,
            filter,
            order_by,
            offset,
            limit,
        );
        (
            plan,
            schema,
            BTreeMap::from([(table_id, Arc::new(rows))]),
            BTreeMap::new(),
        )
    }

    fn numbered_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|value| {
                Row::new(vec![
                    Value::Int64(i64::try_from(value).expect("test value")),
                    Value::Text(format!("row-{value}")),
                ])
            })
            .collect()
    }

    #[test]
    fn cursor_emits_default_sized_ordered_batches() {
        let (plan, schema, tables, indexes) =
            fixture("SELECT id FROM items WHERE id >= 0", numbered_rows(2_500));
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let mut cursor = ExecutionCursor::new(&plan, &context, schema).expect("cursor");
        let mut sizes = Vec::new();
        let mut ids = Vec::new();
        while let Some(batch) = cursor.next_batch().expect("batch") {
            sizes.push(batch.rows.len());
            ids.extend(batch.rows.into_iter().map(|row| row.values[0].clone()));
        }
        assert_eq!(sizes, vec![1_024, 1_024, 452]);
        assert_eq!(ids.first(), Some(&Value::Int64(0)));
        assert_eq!(ids.last(), Some(&Value::Int64(2_499)));
        assert!(cursor.next_batch().expect("exhausted").is_none());
    }

    #[test]
    fn compatibility_execute_collects_the_cursor() {
        let (plan, _schema, tables, indexes) =
            fixture("SELECT id FROM items LIMIT 17", numbered_rows(100));
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let rows = execute(&plan, &context).expect("execute");
        assert_eq!(rows.len(), 17);
        assert_eq!(rows[16].values, vec![Value::Int64(16)]);
    }

    #[test]
    fn offset_streams_after_sort_and_limit_null_is_unbounded() {
        let (plan, _schema, tables, indexes) = fixture(
            "SELECT id FROM items ORDER BY id DESC OFFSET 10 LIMIT NULL",
            numbered_rows(100),
        );
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let rows = execute(&plan, &context).expect("execute offset");
        assert_eq!(rows.len(), 90);
        assert_eq!(rows[0].values, vec![Value::Int64(89)]);
        assert_eq!(rows[89].values, vec![Value::Int64(0)]);
    }

    #[test]
    fn negative_offset_fails_before_scanning() {
        let (plan, schema, tables, indexes) =
            fixture("SELECT id FROM items OFFSET -1", numbered_rows(3));
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let error = match ExecutionCursor::new(&plan, &context, schema) {
            Ok(_) => panic!("negative offset must fail"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "2201X");
    }

    #[test]
    fn sort_spills_and_drop_cleans_the_query_directory() {
        let temp = TempDir::new().expect("temp");
        let spill_root = temp.path().join("spill");
        let (plan, schema, tables, indexes) =
            fixture("SELECT id FROM items ORDER BY id DESC", numbered_rows(200));
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let options = ExecutionOptions {
            batch_rows: 32,
            soft_memory_bytes: 256,
            hard_memory_bytes: 16_384,
            spill_root: spill_root.clone(),
            ..ExecutionOptions::default()
        };
        let mut cursor =
            ExecutionCursor::with_options(&plan, &context, schema, options).expect("cursor");
        let first = cursor.next_batch().expect("batch").expect("first batch");
        assert_eq!(first.rows[0].values, vec![Value::Int64(199)]);
        assert_eq!(
            std::fs::read_dir(&spill_root).expect("spill root").count(),
            1
        );
        drop(cursor);
        assert_eq!(
            std::fs::read_dir(&spill_root)
                .expect("clean spill root")
                .count(),
            0
        );
    }

    #[test]
    fn spill_reader_rejects_truncated_versioned_records() {
        let temp = TempDir::new().expect("temp");
        let path = temp.path().join("truncated.spill");
        let mut bytes = Vec::from(SPILL_MAGIC);
        bytes.extend_from_slice(&SPILL_VERSION.to_le_bytes());
        bytes.extend_from_slice(&16_u32.to_le_bytes());
        bytes.extend_from_slice(b"short");
        std::fs::write(&path, bytes).expect("write truncated spill");

        let memory = super::MemoryGrant::new(1024, 1024).expect("memory grant");
        let error = match SpillRun::open(&path, &memory) {
            Ok(_) => panic!("truncated spill must fail"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "XX001");
    }

    #[test]
    fn spill_write_failure_cleans_the_partial_query_directory() {
        let temp = TempDir::new().expect("temp");
        let spill_root = temp.path().join("spill");
        std::fs::create_dir(&spill_root).expect("spill root");
        let query_dir = {
            let mut spill = SpillManager::new(spill_root);
            let memory = MemoryGrant::new(32, 64).expect("memory grant");
            let error = spill
                .write_sorted_run(&[Row::new(vec![Value::Text("x".repeat(1_024))])], &memory)
                .expect_err("oversized spill record");
            assert_eq!(error.sql_state, "53200");
            let query_dir = spill.query_dir.clone().expect("query directory");
            assert_eq!(
                std::fs::read_dir(&query_dir)
                    .expect("partial query directory")
                    .count(),
                1
            );
            query_dir
        };
        assert!(!query_dir.exists());
    }

    #[test]
    fn spill_heap_merge_compacts_multiple_levels_and_preserves_stable_ties() {
        let temp = TempDir::new().expect("temp");
        let spill_root = temp.path().join("spill");
        std::fs::create_dir(&spill_root).expect("spill root");
        let query_dir = {
            let memory = MemoryGrant::new(1024 * 1024, 8 * 1024 * 1024).expect("memory");
            let order_by = vec![BoundOrder {
                column_index: 0,
                expression: None,
                data_type: ScalarType::Int64,
                ascending: true,
                nulls_first: None,
            }];
            let mut spill = SpillManager::new(spill_root);
            let run_count = MAX_SPILL_MERGE_FAN_IN * MAX_SPILL_MERGE_FAN_IN + 1;
            let mut paths = Vec::new();
            for run in 0..run_count {
                paths.push(
                    spill
                        .write_sorted_run(
                            &[Row::new(vec![
                                Value::Int64(1),
                                Value::Int64(i64::try_from(run).expect("run index")),
                            ])],
                            &memory,
                        )
                        .expect("write run"),
                );
            }
            let paths = spill
                .compact_sorted_runs(paths, &order_by, &memory)
                .expect("compact runs");
            assert!(paths.len() <= MAX_SPILL_MERGE_FAN_IN);
            let mut merge =
                SpillMergeCursor::open(&paths, &order_by, &memory).expect("merge cursor");
            assert!(merge.run_count() <= MAX_SPILL_MERGE_FAN_IN);
            let mut actual = Vec::new();
            while let Some(row) = merge.pop_next(&order_by, &memory).expect("merge row") {
                actual.push(row.values[1].clone());
            }
            assert_eq!(
                actual,
                (0..run_count)
                    .map(|run| Value::Int64(i64::try_from(run).expect("run index")))
                    .collect::<Vec<_>>()
            );
            drop(merge);
            assert_eq!(memory.current_bytes(), 0);
            let query_dir = spill.query_dir.clone().expect("query directory");
            assert!(
                std::fs::read_dir(&query_dir)
                    .expect("query directory")
                    .count()
                    > run_count
            );
            query_dir
        };
        assert!(!query_dir.exists());
        assert_eq!(
            std::fs::read_dir(temp.path().join("spill"))
                .expect("clean spill root")
                .count(),
            0
        );
    }

    #[test]
    fn spill_heap_merge_propagates_compare_errors_and_hard_limits() {
        let temp = TempDir::new().expect("temp");
        let spill_root = temp.path().join("spill");
        std::fs::create_dir(&spill_root).expect("spill root");
        let memory = MemoryGrant::new(1024, 16 * 1024).expect("memory");
        let order_by = vec![BoundOrder {
            column_index: 0,
            expression: None,
            data_type: ScalarType::Json,
            ascending: true,
            nulls_first: None,
        }];
        let query_dir = {
            let mut spill = SpillManager::new(spill_root);
            let json_paths = vec![
                spill
                    .write_sorted_run(
                        &[Row::new(vec![Value::Json(serde_json::json!({"run": 1}))])],
                        &memory,
                    )
                    .expect("first JSON run"),
                spill
                    .write_sorted_run(
                        &[Row::new(vec![Value::Json(serde_json::json!({"run": 2}))])],
                        &memory,
                    )
                    .expect("second JSON run"),
            ];
            let error = match SpillMergeCursor::open(&json_paths, &order_by, &memory) {
                Ok(_) => panic!("JSON spill ordering must fail"),
                Err(error) => error,
            };
            assert_eq!(error.sql_state, "42883");
            assert_eq!(memory.current_bytes(), 0);

            let empty_paths = vec![
                spill.write_sorted_run(&[], &memory).expect("empty run 1"),
                spill.write_sorted_run(&[], &memory).expect("empty run 2"),
            ];
            let tiny = MemoryGrant::new(8, 8).expect("tiny memory");
            let error = match SpillMergeCursor::open(&empty_paths, &order_by, &tiny) {
                Ok(_) => panic!("heap reservation must respect the hard limit"),
                Err(error) => error,
            };
            assert_eq!(error.sql_state, "53200");
            assert_eq!(tiny.current_bytes(), 0);
            spill.query_dir.clone().expect("query directory")
        };
        assert!(!query_dir.exists());
    }

    #[test]
    fn hard_memory_limit_returns_out_of_memory_sqlstate() {
        let (plan, schema, tables, indexes) = fixture(
            "SELECT payload FROM items",
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("x".repeat(2_048)),
            ])],
        );
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let options = ExecutionOptions {
            batch_rows: 1,
            soft_memory_bytes: 128,
            hard_memory_bytes: 256,
            ..ExecutionOptions::default()
        };
        let mut cursor =
            ExecutionCursor::with_options(&plan, &context, schema, options).expect("cursor");
        let error = cursor.next_batch().expect_err("hard memory limit");
        assert_eq!(error.sql_state, "53200");
    }

    #[test]
    fn expression_stack_owns_capacity_and_variable_values_through_raii() {
        let expression = BoundExpr {
            kind: BoundExprKind::Binary {
                left: Box::new(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Text("x".repeat(512))),
                    data_type: ScalarType::Text,
                    nullable: false,
                }),
                op: BinaryOperator::Eq,
                right: Box::new(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Text("x".repeat(512))),
                    data_type: ScalarType::Text,
                    nullable: false,
                }),
            },
            data_type: ScalarType::Boolean,
            nullable: false,
        };
        let program = ExpressionProgram::compile(&expression).expect("compile");
        let memory = MemoryGrant::new(128, 256).expect("grant");
        let mut stack = ExpressionStack::new(&memory).expect("stack");
        let error = program
            .evaluate_reusing(&[], &[], &mut stack)
            .expect_err("variable-width stack exceeds grant");
        assert_eq!(error.sql_state, "53200");
        assert!(memory.current_bytes() > 0);
        drop(stack);
        assert_eq!(memory.current_bytes(), 0);
    }

    #[test]
    fn explicit_expression_and_plan_limits_return_program_limit() {
        let mut expression = BoundExpr {
            kind: BoundExprKind::Literal(Value::Boolean(true)),
            data_type: ScalarType::Boolean,
            nullable: false,
        };
        for _ in 0..8 {
            expression = BoundExpr {
                kind: BoundExprKind::Unary {
                    op: UnaryOperator::Not,
                    expr: Box::new(expression),
                },
                data_type: ScalarType::Boolean,
                nullable: false,
            };
        }
        let error = ExpressionProgram::compile_with_limit(&expression, false, 4)
            .expect_err("expression limit");
        assert_eq!(error.sql_state, "54001");

        let table_id = TableId::new(1);
        let mut plan = PlanNode {
            kind: PlanKind::Scan {
                table_id,
                access: AccessPath::Sequential,
                required_columns: vec![0],
            },
            estimated_rows: 0.0,
            estimated_cost: 0.0,
        };
        for _ in 0..8 {
            plan = PlanNode {
                kind: PlanKind::Limit {
                    limit: BoundExpr {
                        kind: BoundExprKind::Literal(Value::Int64(1)),
                        data_type: ScalarType::Int64,
                        nullable: false,
                    },
                    input: Box::new(plan),
                },
                estimated_rows: 0.0,
                estimated_cost: 0.0,
            };
        }
        let tables = BTreeMap::from([(table_id, Arc::new(Vec::new()))]);
        let indexes = BTreeMap::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let error = match ExecutionCursor::with_options(
            &plan,
            &context,
            Schema::empty(),
            ExecutionOptions {
                max_plan_depth: 4,
                ..ExecutionOptions::default()
            },
        ) {
            Ok(_) => panic!("plan limit"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "54001");
    }

    #[test]
    fn bounded_deep_expression_and_plan_execute_on_small_native_stack() {
        let mut expression = BoundExpr {
            kind: BoundExprKind::Literal(Value::Boolean(true)),
            data_type: ScalarType::Boolean,
            nullable: false,
        };
        for _ in 0..DEFAULT_MAX_EXPRESSION_DEPTH {
            expression = BoundExpr {
                kind: BoundExprKind::Unary {
                    op: UnaryOperator::Not,
                    expr: Box::new(expression),
                },
                data_type: ScalarType::Boolean,
                nullable: false,
            };
        }

        let table_id = TableId::new(1);
        let mut plan = PlanNode {
            kind: PlanKind::Scan {
                table_id,
                access: AccessPath::Sequential,
                required_columns: vec![0],
            },
            estimated_rows: 0.0,
            estimated_cost: 0.0,
        };
        for _ in 0..DEFAULT_MAX_PLAN_DEPTH - 1 {
            plan = PlanNode {
                kind: PlanKind::Limit {
                    limit: BoundExpr {
                        kind: BoundExprKind::Literal(Value::Int64(1)),
                        data_type: ScalarType::Int64,
                        nullable: false,
                    },
                    input: Box::new(plan),
                },
                estimated_rows: 0.0,
                estimated_cost: 0.0,
            };
        }
        let tables = BTreeMap::from([(table_id, Arc::new(Vec::new()))]);
        let indexes = BTreeMap::new();

        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .name("bounded-query-stack".to_owned())
                .stack_size(128 * 1_024)
                .spawn_scoped(scope, move || {
                    let program = ExpressionProgram::compile_with_limit(
                        &expression,
                        false,
                        DEFAULT_MAX_EXPRESSION_DEPTH,
                    )
                    .expect("deep expression compiles iteratively");
                    assert_eq!(
                        program.evaluate(&[], &[]).expect("deep expression"),
                        Value::Boolean(true)
                    );
                    let context = ExecutionContext {
                        tables: &tables,
                        indexes: &indexes,
                        params: &[],
                    };
                    let mut cursor = ExecutionCursor::with_options(
                        &plan,
                        &context,
                        Schema::empty(),
                        ExecutionOptions::default(),
                    )
                    .expect("deep plan builds iteratively");
                    assert!(cursor.next_batch().expect("deep plan executes").is_none());
                })
                .expect("spawn bounded-stack thread")
                .join()
                .expect("bounded-stack thread");
        });
    }
}
