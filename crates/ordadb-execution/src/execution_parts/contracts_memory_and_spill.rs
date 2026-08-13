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
    ArrayDimension, Batch, DbError, IndexId, MAX_POSTGRES_NAME_BYTES, PgArray, PgInterval, Result,
    Row, ScalarType, Schema, TableId, Value,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::Serialize;
use serde::de::DeserializeOwned;

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
