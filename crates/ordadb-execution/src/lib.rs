//! Physical relational operators and typed scalar evaluation for OrdaDB.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use ordadb_index::{BPlusTree, BPlusTreeOwnedIter, IndexKey};
use ordadb_optimizer::{AccessPath, PlanKind, PlanNode};
use ordadb_sql::{
    AggregateFunction, BinaryOperator, BoundExpr, BoundExprKind, BoundOrder, BoundProjection,
    UnaryOperator,
};
use ordadb_types::{Batch, DbError, IndexId, Result, Row, ScalarType, Schema, TableId, Value};
use rust_decimal::Decimal;
use serde::Serialize;
use serde::de::DeserializeOwned;

mod advanced;
pub use advanced::{AdvancedExecutionCursor, AdvancedExecutionPlan};

pub const DEFAULT_BATCH_ROWS: usize = 1024;
pub const DEFAULT_SOFT_MEMORY_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_HARD_MEMORY_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_MAX_PLAN_DEPTH: usize = 256;
pub const DEFAULT_MAX_EXPRESSION_DEPTH: usize = 256;

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
const SPILL_MAGIC: [u8; 8] = *b"ORDBSPL1";
const SPILL_VERSION: u16 = 1;

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

#[derive(Debug, Clone)]
pub struct QueryMemoryContext {
    soft_limit: usize,
    hard_limit: usize,
    current: usize,
    peak: usize,
}

impl QueryMemoryContext {
    fn new(soft_limit: usize, hard_limit: usize) -> Self {
        Self {
            soft_limit,
            hard_limit,
            current: 0,
            peak: 0,
        }
    }

    pub fn reserve(&mut self, bytes: usize) -> Result<()> {
        let next = self.current.checked_add(bytes).ok_or_else(|| {
            DbError::new("53200", "query memory limit exceeded")
                .with_detail("memory accounting overflow")
        })?;
        if next > self.hard_limit {
            return Err(DbError::new("53200", "query memory limit exceeded")
                .with_detail(format!(
                    "requested {bytes} bytes with {} of {} bytes already in use",
                    self.current, self.hard_limit
                ))
                .with_hint("Reduce result width, add a LIMIT, or raise the query memory grant."));
        }
        self.current = next;
        self.peak = self.peak.max(next);
        Ok(())
    }

    pub fn release(&mut self, bytes: usize) {
        self.current = self.current.saturating_sub(bytes);
    }

    #[must_use]
    pub const fn current_bytes(&self) -> usize {
        self.current
    }

    #[must_use]
    pub const fn peak_bytes(&self) -> usize {
        self.peak
    }

    #[must_use]
    pub const fn soft_limit_bytes(&self) -> usize {
        self.soft_limit
    }

    #[must_use]
    pub const fn hard_limit_bytes(&self) -> usize {
        self.hard_limit
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
    Filter(ExpressionProgram),
    Projection(Vec<ExpressionProgram>),
    Sort(Vec<BoundOrder>),
    Limit { remaining: usize },
}

enum SourceCursor {
    Empty,
    Sequential {
        rows: Arc<Vec<Row>>,
        offset: usize,
    },
    Index {
        rows: Arc<Vec<Row>>,
        entries: BPlusTreeOwnedIter,
    },
}

impl SourceCursor {
    fn next_row(&mut self) -> Result<Option<&Row>> {
        match self {
            Self::Empty => Ok(None),
            Self::Sequential { rows, offset } => {
                let row = rows.get(*offset);
                *offset = offset.saturating_add(1);
                Ok(row)
            }
            Self::Index { rows, entries } => entries
                .next()
                .map(|entry| {
                    usize::try_from(entry.row_id.get())
                        .ok()
                        .and_then(|row_id| rows.get(row_id))
                        .ok_or_else(|| DbError::internal("index row reference is out of bounds"))
                })
                .transpose(),
        }
    }
}

struct SpillRun {
    reader: BufReader<File>,
    current: Option<Row>,
}

impl SpillRun {
    fn open(path: &Path, hard_limit: usize) -> Result<Self> {
        let reader = open_spill_reader(path)?;
        let mut run = Self {
            reader,
            current: None,
        };
        run.advance(hard_limit)?;
        Ok(run)
    }

    fn advance(&mut self, hard_limit: usize) -> Result<()> {
        self.current = read_spill_record(&mut self.reader, hard_limit)?;
        Ok(())
    }
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

    fn write_sorted_run(&mut self, rows: &[Row], hard_limit: usize) -> Result<PathBuf> {
        let query_dir = if let Some(query_dir) = &self.query_dir {
            query_dir.clone()
        } else {
            let query_id = NEXT_QUERY_ID.fetch_add(1, AtomicOrdering::Relaxed);
            let query_dir = self
                .root
                .join(format!("ordadb-query-{}-{query_id}", std::process::id()));
            fs::create_dir(&query_dir).map_err(spill_io_error)?;
            self.query_dir = Some(query_dir.clone());
            query_dir
        };
        let path = query_dir.join(format!("run-{}.spill", self.run_count));
        self.run_count = self.run_count.saturating_add(1);
        let mut writer = create_spill_writer(&path)?;
        for row in rows {
            write_spill_record(&mut writer, row, hard_limit)?;
        }
        writer.flush().map_err(spill_io_error)?;
        Ok(path)
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
        rows: Vec<Row>,
        offset: usize,
        reserved_bytes: usize,
    },
    Runs(Vec<SpillRun>),
}

pub struct ExecutionCursor {
    source: SourceCursor,
    frames: Vec<OperatorFrame>,
    sort_index: Option<usize>,
    sort_output: Option<SortOutput>,
    schema: Schema,
    params: Vec<Value>,
    options: ExecutionOptions,
    memory: QueryMemoryContext,
    pool: BatchPool,
    spill: SpillManager,
    expression_stack: Vec<Value>,
    exhausted: bool,
}

impl ExecutionCursor {
    pub fn new(plan: &PlanNode, context: &ExecutionContext<'_>, schema: Schema) -> Result<Self> {
        Self::with_options(plan, context, schema, ExecutionOptions::default())
    }

    pub fn with_options(
        plan: &PlanNode,
        context: &ExecutionContext<'_>,
        schema: Schema,
        options: ExecutionOptions,
    ) -> Result<Self> {
        options.validate()?;
        let (source, frames) = build_pipeline(plan, context, &options)?;
        let sort_indexes = frames
            .iter()
            .enumerate()
            .filter_map(|(index, frame)| matches!(frame, OperatorFrame::Sort(_)).then_some(index))
            .collect::<Vec<_>>();
        if sort_indexes.len() > 1 {
            return Err(program_limit_error(
                "a physical pipeline may contain at most one Sort frame",
            ));
        }
        let sort_index = sort_indexes.first().copied();
        Ok(Self {
            source,
            frames,
            sort_index,
            sort_output: None,
            schema,
            params: context.params.to_vec(),
            memory: QueryMemoryContext::new(options.soft_memory_bytes, options.hard_memory_bytes),
            pool: BatchPool::new(options.batch_rows),
            spill: SpillManager::new(options.spill_root.clone()),
            expression_stack: Vec::with_capacity(16),
            options,
            exhausted: false,
        })
    }

    #[must_use]
    pub const fn memory(&self) -> &QueryMemoryContext {
        &self.memory
    }

    pub fn next_batch(&mut self) -> Result<Option<Batch>> {
        if self.exhausted {
            return Ok(None);
        }
        if self.sort_index.is_some() && self.sort_output.is_none() {
            self.initialize_sort()?;
        }

        let mut output = self.pool.take();
        let mut output_bytes = 0_usize;
        while output.len() < self.options.batch_rows {
            let row = if let Some(sort_index) = self.sort_index {
                let Some(row) = self.next_sorted_row()? else {
                    break;
                };
                apply_frames(
                    &mut self.frames[sort_index + 1..],
                    &self.params,
                    &mut self.expression_stack,
                    Cow::Owned(row),
                )?
            } else {
                let Some(row) = self.source.next_row()? else {
                    break;
                };
                apply_frames(
                    &mut self.frames,
                    &self.params,
                    &mut self.expression_stack,
                    Cow::Borrowed(row),
                )?
            };
            if let Some(row) = row {
                let row_bytes = estimated_row_bytes(&row);
                self.memory.reserve(row_bytes)?;
                output_bytes = output_bytes.saturating_add(row_bytes);
                output.push(row);
            }
        }
        if output.is_empty() {
            self.exhausted = true;
            self.pool.recycle(output);
            return Ok(None);
        }
        self.memory.release(output_bytes);
        Ok(Some(Batch {
            schema: self.schema.clone(),
            rows: output,
        }))
    }

    fn initialize_sort(&mut self) -> Result<()> {
        let sort_index = self
            .sort_index
            .ok_or_else(|| DbError::internal("Sort initialization has no Sort frame"))?;
        let OperatorFrame::Sort(order_by) = &self.frames[sort_index] else {
            return Err(DbError::internal("Sort frame index is invalid"));
        };
        let order_by = order_by.clone();
        let mut rows = Vec::new();
        let mut reserved_bytes = 0_usize;
        let mut run_paths = Vec::new();
        loop {
            let mut input = self.pool.take();
            let mut input_bytes = 0_usize;
            while input.len() < self.options.batch_rows {
                let Some(row) = self.source.next_row()? else {
                    break;
                };
                let row_bytes = estimated_row_bytes(row);
                self.memory.reserve(row_bytes)?;
                input_bytes = input_bytes.saturating_add(row_bytes);
                input.push(row.clone());
            }
            if input.is_empty() {
                self.pool.recycle(input);
                break;
            }
            for row in input.drain(..) {
                let source_bytes = estimated_row_bytes(&row);
                self.memory.release(source_bytes);
                input_bytes = input_bytes.saturating_sub(source_bytes);
                let Some(row) = apply_frames(
                    &mut self.frames[..sort_index],
                    &self.params,
                    &mut self.expression_stack,
                    Cow::Owned(row),
                )?
                else {
                    continue;
                };
                let row_bytes = estimated_row_bytes(&row);
                if !rows.is_empty()
                    && reserved_bytes.saturating_add(row_bytes) > self.memory.soft_limit_bytes()
                {
                    sort_rows(&mut rows, &order_by)?;
                    run_paths.push(
                        self.spill
                            .write_sorted_run(&rows, self.options.hard_memory_bytes)?,
                    );
                    rows.clear();
                    self.memory.release(reserved_bytes);
                    reserved_bytes = 0;
                }
                self.memory.reserve(row_bytes)?;
                reserved_bytes = reserved_bytes.saturating_add(row_bytes);
                rows.push(row);
            }
            self.memory.release(input_bytes);
            self.pool.recycle(input);
        }

        if run_paths.is_empty() {
            sort_rows(&mut rows, &order_by)?;
            self.sort_output = Some(SortOutput::Memory {
                rows,
                offset: 0,
                reserved_bytes,
            });
        } else {
            if !rows.is_empty() {
                sort_rows(&mut rows, &order_by)?;
                run_paths.push(
                    self.spill
                        .write_sorted_run(&rows, self.options.hard_memory_bytes)?,
                );
                self.memory.release(reserved_bytes);
            }
            let runs = run_paths
                .iter()
                .map(|path| SpillRun::open(path, self.options.hard_memory_bytes))
                .collect::<Result<Vec<_>>>()?;
            self.sort_output = Some(SortOutput::Runs(runs));
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
                offset,
                reserved_bytes,
            } => {
                let row = rows.get(*offset).cloned();
                *offset = offset.saturating_add(1);
                if row.is_none() && *reserved_bytes > 0 {
                    self.memory.release(*reserved_bytes);
                    *reserved_bytes = 0;
                }
                Ok(row)
            }
            SortOutput::Runs(runs) => {
                let sort_index = self
                    .sort_index
                    .ok_or_else(|| DbError::internal("Sort output has no frame"))?;
                let OperatorFrame::Sort(order_by) = &self.frames[sort_index] else {
                    return Err(DbError::internal("Sort output frame is invalid"));
                };
                let mut selected: Option<usize> = None;
                for (index, run) in runs.iter().enumerate() {
                    let Some(candidate) = &run.current else {
                        continue;
                    };
                    let replace = match selected {
                        None => true,
                        Some(selected_index) => {
                            let selected_row = runs[selected_index]
                                .current
                                .as_ref()
                                .ok_or_else(|| DbError::internal("Sort merge row disappeared"))?;
                            compare_rows(candidate, selected_row, order_by)? == Ordering::Less
                        }
                    };
                    if replace {
                        selected = Some(index);
                    }
                }
                let Some(selected) = selected else {
                    return Ok(None);
                };
                let row = runs[selected]
                    .current
                    .take()
                    .ok_or_else(|| DbError::internal("Sort merge row disappeared"))?;
                runs[selected].advance(self.options.hard_memory_bytes)?;
                Ok(Some(row))
            }
        }
    }
}

fn apply_frames(
    frames: &mut [OperatorFrame],
    params: &[Value],
    expression_stack: &mut Vec<Value>,
    mut row: Cow<'_, Row>,
) -> Result<Option<Row>> {
    for frame in frames {
        match frame {
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
) -> Result<(SourceCursor, Vec<OperatorFrame>)> {
    let mut node = plan;
    let mut frames = Vec::new();
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
            } => break build_source(*table_id, access, context, options.max_expression_depth)?,
            PlanKind::Filter { predicate, input } => {
                frames.push(OperatorFrame::Filter(
                    ExpressionProgram::compile_with_limit(
                        predicate,
                        false,
                        options.max_expression_depth,
                    )?,
                ));
                node = input;
            }
            PlanKind::Projection { expressions, input } => {
                frames.push(OperatorFrame::Projection(compile_projections(
                    expressions,
                    options.max_expression_depth,
                )?));
                node = input;
            }
            PlanKind::Sort { order_by, input } => {
                frames.push(OperatorFrame::Sort(order_by.clone()));
                node = input;
            }
            PlanKind::Limit { limit, input } => {
                frames.push(OperatorFrame::Limit {
                    remaining: evaluate_limit_program(
                        &ExpressionProgram::compile_with_limit(
                            limit,
                            false,
                            options.max_expression_depth,
                        )?,
                        context.params,
                    )?,
                });
                node = input;
            }
        }
    };
    frames.reverse();
    Ok((source, frames))
}

fn build_source(
    table_id: TableId,
    access: &AccessPath,
    context: &ExecutionContext<'_>,
    max_expression_depth: usize,
) -> Result<SourceCursor> {
    let rows = context
        .tables
        .get(&table_id)
        .cloned()
        .unwrap_or_else(|| Arc::new(Vec::new()));
    match access {
        AccessPath::Empty => Ok(SourceCursor::Empty),
        AccessPath::Sequential => Ok(SourceCursor::Sequential { rows, offset: 0 }),
        AccessPath::Index {
            index_id,
            operator,
            value,
            ..
        } => {
            let program =
                ExpressionProgram::compile_with_limit(value, false, max_expression_depth)?;
            let value = program.evaluate(&[], context.params)?;
            if value.is_null() {
                return Ok(SourceCursor::Empty);
            }
            let key = IndexKey::from_values(&[value])?;
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

fn create_spill_writer(path: &Path) -> Result<BufWriter<File>> {
    let file = File::create(path).map_err(spill_io_error)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&SPILL_MAGIC).map_err(spill_io_error)?;
    writer
        .write_all(&SPILL_VERSION.to_le_bytes())
        .map_err(spill_io_error)?;
    Ok(writer)
}

fn open_spill_reader(path: &Path) -> Result<BufReader<File>> {
    let file = File::open(path).map_err(spill_io_error)?;
    let mut reader = BufReader::new(file);
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

fn write_spill_record<T: Serialize>(
    writer: &mut BufWriter<File>,
    value: &T,
    hard_limit: usize,
) -> Result<()> {
    let payload = serde_json::to_vec(value).map_err(|error| {
        DbError::new("58030", "query spill encoding failed").with_detail(error.to_string())
    })?;
    if payload.len() > hard_limit {
        return Err(DbError::new(
            "53200",
            "query spill record exceeds the hard memory limit",
        ));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| DbError::new("53200", "query spill record length is out of range"))?;
    writer
        .write_all(&length.to_le_bytes())
        .map_err(spill_io_error)?;
    writer.write_all(&payload).map_err(spill_io_error)
}

fn read_spill_record<T: DeserializeOwned>(
    reader: &mut BufReader<File>,
    hard_limit: usize,
) -> Result<Option<T>> {
    let mut length = [0_u8; 4];
    let first = reader.read(&mut length[..1]).map_err(spill_io_error)?;
    if first == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut length[1..])
        .map_err(|error| spill_corruption("spill record length is truncated", error))?;
    let length = u32::from_le_bytes(length) as usize;
    if length > hard_limit {
        return Err(DbError::new(
            "53200",
            "query spill record exceeds the hard memory limit",
        ));
    }
    let mut payload = vec![0_u8; length];
    reader
        .read_exact(&mut payload)
        .map_err(|error| spill_corruption("spill record payload is truncated", error))?;
    serde_json::from_slice(&payload).map(Some).map_err(|error| {
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

fn estimated_row_bytes(row: &Row) -> usize {
    std::mem::size_of::<Row>() + row.values.iter().map(estimated_value_bytes).sum::<usize>()
}

fn estimated_value_bytes(value: &Value) -> usize {
    std::mem::size_of::<Value>()
        + match value {
            Value::Text(value) => value.len(),
            Value::Binary(value) => value.len(),
            Value::Json(value) | Value::Jsonb(value) => value.to_string().len(),
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
    Binary(BinaryOperator),
    Aggregate {
        function: AggregateFunction,
        argument: Option<Vec<ExpressionInstruction>>,
    },
    Coerce(ScalarType),
}

#[derive(Debug, Clone)]
pub struct ExpressionProgram {
    instructions: Vec<ExpressionInstruction>,
    fast_path: Option<FastExpression>,
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
                    BoundExprKind::Binary { op, .. } => {
                        instructions.push(ExpressionInstruction::Binary(*op));
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
                BoundExprKind::Literal(value) => {
                    instructions.push(ExpressionInstruction::LoadLiteral(value.clone()));
                    instructions.push(ExpressionInstruction::Coerce(expression.data_type.clone()));
                }
                BoundExprKind::Parameter { index } => {
                    instructions.push(ExpressionInstruction::LoadParameter(*index));
                    instructions.push(ExpressionInstruction::Coerce(expression.data_type.clone()));
                }
                BoundExprKind::Unary { expr, .. } => {
                    pending.push((expression, true, depth));
                    pending.push((expr, false, depth + 1));
                }
                BoundExprKind::Binary { left, right, .. } => {
                    pending.push((expression, true, depth));
                    pending.push((right, false, depth + 1));
                    pending.push((left, false, depth + 1));
                }
                BoundExprKind::Aggregate { function, argument } => {
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
                    instructions.push(ExpressionInstruction::Aggregate {
                        function: *function,
                        argument,
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
        Ok(Self {
            instructions,
            fast_path: detect_fast_expression(expr),
        })
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
        values: &mut Vec<Value>,
    ) -> Result<Value> {
        if let Some(fast_path) = &self.fast_path {
            return evaluate_fast_expression(fast_path, row, params);
        }
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
            coerce_value(evaluate_binary(left, *operator, right)?, target)
        }
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

fn evaluate_instructions_reusing(
    instructions: &[ExpressionInstruction],
    row: &[Value],
    params: &[Value],
    group_rows: Option<&[Row]>,
    values: &mut Vec<Value>,
) -> Result<Value> {
    values.clear();
    for instruction in instructions {
        match instruction {
            ExpressionInstruction::LoadColumn(index) => {
                values.push(row.get(*index).cloned().ok_or_else(|| {
                    DbError::internal(format!("bound column index {index} is out of range"))
                })?);
            }
            ExpressionInstruction::LoadLiteral(value) => values.push(value.clone()),
            ExpressionInstruction::LoadParameter(index) => {
                values.push(params.get(index - 1).cloned().ok_or_else(|| {
                    DbError::new("42P02", format!("no value supplied for parameter ${index}"))
                })?);
            }
            ExpressionInstruction::Unary(operator) => {
                let value = values
                    .pop()
                    .ok_or_else(|| DbError::internal("expression value stack underflow"))?;
                values.push(evaluate_unary(*operator, value)?);
            }
            ExpressionInstruction::Binary(operator) => {
                let right = values
                    .pop()
                    .ok_or_else(|| DbError::internal("expression value stack underflow"))?;
                let left = values
                    .pop()
                    .ok_or_else(|| DbError::internal("expression value stack underflow"))?;
                values.push(evaluate_binary(left, *operator, right)?);
            }
            ExpressionInstruction::Aggregate { function, argument } => {
                let rows = group_rows.ok_or_else(|| {
                    DbError::internal("aggregate expression requires grouped rows")
                })?;
                values.push(evaluate_aggregate_program(
                    *function,
                    argument.as_deref(),
                    rows,
                    params,
                )?);
            }
            ExpressionInstruction::Coerce(target) => {
                let value = values
                    .pop()
                    .ok_or_else(|| DbError::internal("expression value stack underflow"))?;
                values.push(coerce_value(value, target)?);
            }
        }
    }
    if values.len() != 1 {
        return Err(DbError::internal(
            "expression program did not produce exactly one value",
        ));
    }
    values
        .pop()
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
            let mut selected = values[0].clone();
            for value in values.iter().skip(1) {
                let ordering = compare_values(value, &selected)?;
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
        _ => Err(DbError::new(
            "42804",
            "unary operator received an incompatible value",
        )),
    }
}

fn evaluate_binary(left: Value, operator: BinaryOperator, right: Value) -> Result<Value> {
    if matches!(operator, BinaryOperator::And | BinaryOperator::Or) {
        return evaluate_boolean_binary(left, operator, right);
    }
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    match operator {
        BinaryOperator::Eq => return Ok(Value::Boolean(left == right)),
        BinaryOperator::NotEq => return Ok(Value::Boolean(left != right)),
        _ => {}
    }
    let ordering = compare_values(&left, &right)?;
    Ok(Value::Boolean(match operator {
        BinaryOperator::Lt => ordering == Ordering::Less,
        BinaryOperator::LtEq => ordering != Ordering::Greater,
        BinaryOperator::Gt => ordering == Ordering::Greater,
        BinaryOperator::GtEq => ordering != Ordering::Less,
        _ => unreachable!("handled above"),
    }))
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
        Value::Null => Err(DbError::new("22004", "LIMIT cannot be null")),
        _ => Err(DbError::new(
            "2201W",
            "LIMIT must be a non-negative integer",
        )),
    }
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
        (Value::Binary(value), ScalarType::Binary) => Ok(Value::Binary(value)),
        (Value::Date(value), ScalarType::Date) => Ok(Value::Date(value)),
        (Value::Time(value), ScalarType::Time) => Ok(Value::Time(value)),
        (
            Value::Timestamp(value),
            ScalarType::Timestamp {
                with_timezone: false,
            },
        ) => Ok(Value::Timestamp(value)),
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
                let ordering = compare_values(left_value, right_value)?;
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
        (Value::Uuid(left), Value::Uuid(right)) => Ok(left.cmp(right)),
        _ => Err(DbError::new(
            "42883",
            "values do not have a compatible ordering operator",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use ordadb_catalog::{Catalog, NewColumn};
    use ordadb_optimizer::{AccessPath, PlanKind, PlanNode, optimize_select};
    use ordadb_sql::{BoundExpr, BoundExprKind, BoundStatement, UnaryOperator, bind, parse};
    use ordadb_types::{Identifier, Row, ScalarType, Schema, TableId, Value};
    use tempfile::TempDir;

    use super::{
        ExecutionContext, ExecutionCursor, ExecutionOptions, ExpressionProgram, SPILL_MAGIC,
        SPILL_VERSION, SpillRun, execute,
    };

    type TestTables = BTreeMap<TableId, Arc<Vec<Row>>>;
    type TestIndexes = BTreeMap<ordadb_types::IndexId, Arc<ordadb_index::BPlusTree>>;
    type TestFixture = (PlanNode, Schema, TestTables, TestIndexes);

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
            hard_memory_bytes: 4_096,
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

        let error = match SpillRun::open(&path, 1024) {
            Ok(_) => panic!("truncated spill must fail"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "XX001");
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
}
