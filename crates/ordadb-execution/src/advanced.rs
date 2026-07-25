use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ordadb_optimizer::{JoinStrategy, choose_join_strategy};
use ordadb_sql::{
    AggregateFunction, BinaryOperator, BoundExpr, BoundExprKind, BoundJoin, BoundOrder,
    BoundProjection, BoundTable, JoinKind, UnaryOperator,
};
use ordadb_types::{Batch, DbError, Result, Row, Schema, Value};
use serde::{Deserialize, Serialize};

use super::{
    BatchPool, ExecutionContext, ExecutionOptions, ExpressionProgram, QueryMemoryContext,
    SpillManager, SpillRun, compare_rows, create_spill_writer, estimated_row_bytes,
    estimated_value_bytes, evaluate_binary, evaluate_unary, open_spill_reader, program_limit_error,
    read_spill_record, sort_rows, spill_io_error, write_spill_record,
};

const HASH_PARTITIONS: usize = 32;

#[derive(Debug, Clone)]
pub struct AdvancedExecutionPlan {
    pub table: BoundTable,
    pub joins: Vec<BoundJoin>,
    pub schema: Schema,
    pub projection: Vec<BoundProjection>,
    pub filter: Option<BoundExpr>,
    pub group_by: Vec<BoundExpr>,
    pub having: Option<BoundExpr>,
    pub order_by: Vec<BoundOrder>,
    pub limit: Option<BoundExpr>,
    pub aggregate: bool,
}

pub struct AdvancedExecutionCursor {
    source: JoinedSource,
    schema: Schema,
    filter: Option<ExpressionProgram>,
    projection: Vec<ExpressionProgram>,
    group_programs: Option<GroupPrograms>,
    order_by: Vec<BoundOrder>,
    limit: Option<usize>,
    emitted: usize,
    params: Vec<Value>,
    options: ExecutionOptions,
    memory: QueryMemoryContext,
    pool: BatchPool,
    spill: SpillManager,
    expression_stack: Vec<Value>,
    output: Option<RowsOutput>,
    aggregate: bool,
    exhausted: bool,
}

impl AdvancedExecutionCursor {
    pub fn new(
        plan: AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
    ) -> Result<AdvancedExecutionCursor> {
        Self::with_options(plan, context, ExecutionOptions::default())
    }

    pub fn with_options(
        plan: AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
        options: ExecutionOptions,
    ) -> Result<AdvancedExecutionCursor> {
        options.validate()?;
        if plan.joins.len().saturating_add(1) > options.max_plan_depth {
            return Err(program_limit_error(format!(
                "advanced plan exceeds the depth limit of {}",
                options.max_plan_depth
            )));
        }
        let source = JoinedSource::new(&plan, context, options.max_expression_depth)?;
        let filter = plan
            .filter
            .as_ref()
            .map(|expr| {
                ExpressionProgram::compile_with_limit(expr, false, options.max_expression_depth)
            })
            .transpose()?;
        let projection = if plan.aggregate {
            Vec::new()
        } else {
            plan.projection
                .iter()
                .map(|projection| {
                    ExpressionProgram::compile_with_limit(
                        &projection.expr,
                        false,
                        options.max_expression_depth,
                    )
                })
                .collect::<Result<Vec<_>>>()?
        };
        let group_programs = plan
            .aggregate
            .then(|| GroupPrograms::compile(&plan, options.max_expression_depth))
            .transpose()?;
        let limit = plan
            .limit
            .as_ref()
            .map(|limit| {
                ExpressionProgram::compile_with_limit(limit, false, options.max_expression_depth)?
                    .evaluate(&[], context.params)
                    .and_then(limit_from_value)
            })
            .transpose()?;
        Ok(Self {
            source,
            schema: plan.schema,
            filter,
            projection,
            group_programs,
            order_by: plan.order_by,
            limit,
            emitted: 0,
            params: context.params.to_vec(),
            memory: QueryMemoryContext::new(options.soft_memory_bytes, options.hard_memory_bytes),
            pool: BatchPool::new(options.batch_rows),
            spill: SpillManager::new(options.spill_root.clone()),
            expression_stack: Vec::with_capacity(32),
            output: None,
            aggregate: plan.aggregate,
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
        if self.aggregate && self.output.is_none() {
            self.initialize_aggregate()?;
        } else if !self.aggregate && !self.order_by.is_empty() && self.output.is_none() {
            self.initialize_sorted_source()?;
        }

        let mut rows = self.pool.take();
        let mut output_bytes = 0_usize;
        while rows.len() < self.options.batch_rows {
            if self.limit.is_some_and(|limit| self.emitted >= limit) {
                break;
            }
            let Some(row) = self.next_output_row()? else {
                break;
            };
            let bytes = estimated_row_bytes(&row);
            self.memory.reserve(bytes)?;
            output_bytes = output_bytes.saturating_add(bytes);
            rows.push(row);
            self.emitted = self.emitted.saturating_add(1);
        }
        if rows.is_empty() {
            self.exhausted = true;
            self.pool.recycle(rows);
            return Ok(None);
        }
        self.memory.release(output_bytes);
        Ok(Some(Batch {
            schema: self.schema.clone(),
            rows,
        }))
    }

    fn next_output_row(&mut self) -> Result<Option<Row>> {
        if let Some(output) = &mut self.output {
            let row = output.next_row(
                &self.order_by,
                self.options.hard_memory_bytes,
                &mut self.memory,
            )?;
            if self.aggregate {
                return Ok(row);
            }
            return row.map(|row| self.project_row(row)).transpose();
        }
        loop {
            let Some(row) = self.source.next_row(
                &self.params,
                &mut self.memory,
                &mut self.spill,
                &mut self.expression_stack,
            )?
            else {
                return Ok(None);
            };
            if !self.matches_filter(&row)? {
                continue;
            }
            return self.project_row(row).map(Some);
        }
    }

    fn matches_filter(&mut self, row: &Row) -> Result<bool> {
        let Some(filter) = &self.filter else {
            return Ok(true);
        };
        match filter.evaluate_reusing(&row.values, &self.params, &mut self.expression_stack)? {
            Value::Boolean(matches) => Ok(matches),
            Value::Null => Ok(false),
            _ => Err(DbError::new("42804", "predicate must evaluate to boolean")),
        }
    }

    fn project_row(&mut self, row: Row) -> Result<Row> {
        self.projection
            .iter()
            .map(|program| {
                program.evaluate_reusing(&row.values, &self.params, &mut self.expression_stack)
            })
            .collect::<Result<Vec<_>>>()
            .map(Row::new)
    }

    fn next_filtered_source_row(&mut self) -> Result<Option<Row>> {
        loop {
            let Some(row) = self.source.next_row(
                &self.params,
                &mut self.memory,
                &mut self.spill,
                &mut self.expression_stack,
            )?
            else {
                return Ok(None);
            };
            if self.matches_filter(&row)? {
                return Ok(Some(row));
            }
        }
    }

    fn initialize_sorted_source(&mut self) -> Result<()> {
        let mut builder = RowsOutputBuilder::new(&self.order_by);
        while let Some(row) = self.next_filtered_source_row()? {
            builder.push(row, &mut self.memory, &mut self.spill)?;
        }
        self.output = Some(builder.finish(&mut self.memory, &mut self.spill)?);
        Ok(())
    }

    fn initialize_aggregate(&mut self) -> Result<()> {
        let programs = self
            .group_programs
            .take()
            .ok_or_else(|| DbError::internal("aggregate programs are unavailable"))?;
        let mut groups = Vec::<GroupAccumulator>::new();
        let mut group_bytes = 0_usize;
        let mut spill_paths = None;
        let mut ordinal = 0_u64;
        while let Some(row) = self.next_filtered_source_row()? {
            let key = programs.group_key(&row, &self.params, &mut self.expression_stack)?;
            if let Some(group) = groups.iter_mut().find(|group| group.key == key) {
                group.update(
                    &programs.aggregate_specs,
                    &row,
                    &self.params,
                    &mut self.expression_stack,
                )?;
            } else {
                let group = GroupAccumulator::new(
                    key,
                    row,
                    ordinal,
                    &programs.aggregate_specs,
                    &self.params,
                    &mut self.expression_stack,
                )?;
                let bytes = group.estimated_bytes();
                if !groups.is_empty()
                    && self.memory.current_bytes().saturating_add(bytes)
                        > self.memory.soft_limit_bytes()
                {
                    if spill_paths.is_none() {
                        spill_paths =
                            Some(self.spill.partition_paths("aggregate", HASH_PARTITIONS)?);
                    }
                    let paths = spill_paths
                        .as_ref()
                        .ok_or_else(|| DbError::internal("aggregate spill paths disappeared"))?;
                    self.spill.write_group_partials(
                        paths,
                        &groups,
                        self.options.hard_memory_bytes,
                    )?;
                    groups.clear();
                    self.memory.release(group_bytes);
                    group_bytes = 0;
                }
                self.memory.reserve(bytes)?;
                group_bytes = group_bytes.saturating_add(bytes);
                groups.push(group);
            }
            ordinal = ordinal.saturating_add(1);
        }

        if programs.group_by.is_empty() && groups.is_empty() {
            let group = GroupAccumulator::empty(&programs.aggregate_specs);
            let bytes = group.estimated_bytes();
            self.memory.reserve(bytes)?;
            group_bytes = bytes;
            groups.push(group);
        }

        if let Some(paths) = &spill_paths
            && !groups.is_empty()
        {
            self.spill
                .write_group_partials(paths, &groups, self.options.hard_memory_bytes)?;
            groups.clear();
            self.memory.release(group_bytes);
        }

        let mut output = RowsOutputBuilder::new(&self.order_by);
        if let Some(paths) = spill_paths {
            for path in paths {
                if !path.exists() {
                    continue;
                }
                let (partition_groups, bytes) = self.spill.read_and_merge_groups(
                    &path,
                    &mut self.memory,
                    &programs.aggregate_specs,
                )?;
                for group in partition_groups {
                    if let Some(row) =
                        programs.project_group(&group, &self.params, &mut self.expression_stack)?
                    {
                        output.push(row, &mut self.memory, &mut self.spill)?;
                    }
                }
                self.memory.release(bytes);
            }
        } else {
            for group in groups {
                if let Some(row) =
                    programs.project_group(&group, &self.params, &mut self.expression_stack)?
                {
                    output.push(row, &mut self.memory, &mut self.spill)?;
                }
            }
            self.memory.release(group_bytes);
        }
        self.output = Some(output.finish(&mut self.memory, &mut self.spill)?);
        self.group_programs = Some(programs);
        Ok(())
    }
}

struct JoinedSource {
    base: Arc<Vec<Row>>,
    base_offset: usize,
    joins: Vec<JoinRuntime>,
    prefixes: Vec<Row>,
    frames: Vec<JoinFrame>,
    depth: usize,
}

impl JoinedSource {
    fn new(
        plan: &AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
        max_expression_depth: usize,
    ) -> Result<Self> {
        let base = context
            .tables
            .get(&plan.table.table_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()));
        let joins = plan
            .joins
            .iter()
            .map(|join| {
                let rows = context
                    .tables
                    .get(&join.table.table_id)
                    .cloned()
                    .unwrap_or_else(|| Arc::new(Vec::new()));
                JoinRuntime::new(join.clone(), rows, base.len(), max_expression_depth)
            })
            .collect::<Result<Vec<_>>>()?;
        let frames = (0..joins.len()).map(|_| JoinFrame::default()).collect();
        Ok(Self {
            base,
            base_offset: 0,
            joins,
            prefixes: Vec::new(),
            frames,
            depth: 0,
        })
    }

    fn next_row(
        &mut self,
        params: &[Value],
        memory: &mut QueryMemoryContext,
        spill: &mut SpillManager,
        expression_stack: &mut Vec<Value>,
    ) -> Result<Option<Row>> {
        if self.joins.is_empty() {
            let row = self.base.get(self.base_offset).cloned();
            self.base_offset = self.base_offset.saturating_add(1);
            return Ok(row);
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
                    .last()
                    .cloned()
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
                let candidates = self.joins[self.depth].candidates(prefix, memory, spill)?;
                self.frames[self.depth].install(candidates);
            }

            if let Some(right) =
                self.frames[self.depth].next_candidate(&self.joins[self.depth].rows)
            {
                let mut values = self.prefixes[self.depth].values.clone();
                values.extend(right.values);
                let joined = Row::new(values);
                let matches = match self.joins[self.depth].predicate.evaluate_reusing(
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
                };
                if matches {
                    self.frames[self.depth].matched = true;
                    self.prefixes.truncate(self.depth + 1);
                    self.prefixes.push(joined);
                    self.depth += 1;
                    if self.depth < self.frames.len() {
                        self.frames[self.depth].reset(memory);
                    }
                }
                continue;
            }

            if self.joins[self.depth].join.kind == JoinKind::Left
                && !self.frames[self.depth].matched
                && !self.frames[self.depth].null_emitted
            {
                self.frames[self.depth].null_emitted = true;
                let mut values = self.prefixes[self.depth].values.clone();
                values.extend(std::iter::repeat_n(
                    Value::Null,
                    self.joins[self.depth].join.table.width,
                ));
                self.prefixes.truncate(self.depth + 1);
                self.prefixes.push(Row::new(values));
                self.depth += 1;
                if self.depth < self.frames.len() {
                    self.frames[self.depth].reset(memory);
                }
                continue;
            }

            self.frames[self.depth].reset(memory);
            if self.depth == 0 {
                self.prefixes.clear();
            } else {
                self.prefixes.truncate(self.depth);
                self.depth -= 1;
            }
        }
    }
}

struct JoinRuntime {
    join: BoundJoin,
    rows: Arc<Vec<Row>>,
    predicate: ExpressionProgram,
    lookup: JoinLookup,
}

impl JoinRuntime {
    fn new(
        join: BoundJoin,
        rows: Arc<Vec<Row>>,
        left_rows: usize,
        max_expression_depth: usize,
    ) -> Result<Self> {
        let equi = equi_join_columns(&join.on, join.table.offset)
            .map(|(left, right)| (left, right - join.table.offset));
        let strategy =
            choose_join_strategy(left_rows as u64, rows.len() as u64, equi.is_some()).strategy;
        let lookup = match (strategy, equi) {
            (JoinStrategy::Hash, Some((left, right))) => JoinLookup::Hash {
                left,
                right,
                state: HashLookup::Uninitialized,
            },
            _ => JoinLookup::Nested,
        };
        let predicate =
            ExpressionProgram::compile_with_limit(&join.on, false, max_expression_depth)?;
        Ok(Self {
            join,
            rows,
            predicate,
            lookup,
        })
    }

    fn candidates(
        &mut self,
        prefix: &Row,
        memory: &mut QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<CandidateSet> {
        match &mut self.lookup {
            JoinLookup::Nested => Ok(CandidateSet::All {
                offset: 0,
                len: self.rows.len(),
            }),
            JoinLookup::Hash { left, right, state } => {
                ensure_hash_lookup(state, &self.rows, *right, memory, spill)?;
                let value = prefix
                    .values
                    .get(*left)
                    .ok_or_else(|| DbError::internal("hash join left key is out of bounds"))?;
                if value.is_null() {
                    return Ok(CandidateSet::Indexes {
                        values: Vec::new(),
                        offset: 0,
                        reserved_bytes: 0,
                    });
                }
                let key = encode_hash_value(value)?;
                match state {
                    HashLookup::Memory { buckets, .. } => {
                        let values = buckets.get(&key).cloned().unwrap_or_default();
                        let bytes = values.len().saturating_mul(std::mem::size_of::<usize>());
                        memory.reserve(bytes)?;
                        Ok(CandidateSet::Indexes {
                            values,
                            offset: 0,
                            reserved_bytes: bytes,
                        })
                    }
                    HashLookup::Spilled { paths } => {
                        let partition = stable_partition(&key, paths.len());
                        let rows =
                            spill.read_matching_rows(&paths[partition], *right, &key, memory)?;
                        let bytes = rows.iter().map(estimated_row_bytes).sum();
                        Ok(CandidateSet::Rows {
                            values: rows,
                            offset: 0,
                            reserved_bytes: bytes,
                        })
                    }
                    HashLookup::Uninitialized => {
                        Err(DbError::internal("hash join lookup was not initialized"))
                    }
                }
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
        buckets: HashMap<Vec<u8>, Vec<usize>>,
        _reserved_bytes: usize,
    },
    Spilled {
        paths: Vec<PathBuf>,
    },
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
    let mut buckets = HashMap::<Vec<u8>, Vec<usize>>::new();
    let mut reserved_bytes = 0_usize;
    for (index, row) in rows.iter().enumerate() {
        let value = row
            .values
            .get(key_index)
            .ok_or_else(|| DbError::internal("hash join right key is out of bounds"))?;
        if value.is_null() {
            continue;
        }
        let key = encode_hash_value(value)?;
        let bytes = key
            .len()
            .saturating_add(std::mem::size_of::<usize>())
            .saturating_add(24);
        if !buckets.is_empty()
            && memory.current_bytes().saturating_add(bytes) > memory.soft_limit_bytes()
        {
            memory.release(reserved_bytes);
            let paths = spill.write_partitioned_rows(
                "hash-join",
                rows,
                key_index,
                HASH_PARTITIONS,
                memory.hard_limit_bytes(),
            )?;
            *state = HashLookup::Spilled { paths };
            return Ok(());
        }
        memory.reserve(bytes)?;
        reserved_bytes = reserved_bytes.saturating_add(bytes);
        buckets.entry(key).or_default().push(index);
    }
    *state = HashLookup::Memory {
        buckets,
        _reserved_bytes: reserved_bytes,
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

    fn next_candidate(&mut self, rows: &[Row]) -> Option<Row> {
        self.candidates.as_mut()?.next(rows)
    }

    fn reset(&mut self, memory: &mut QueryMemoryContext) {
        if let Some(candidates) = self.candidates.take() {
            memory.release(candidates.reserved_bytes());
        }
        self.initialized = false;
        self.matched = false;
        self.null_emitted = false;
    }
}

enum CandidateSet {
    All {
        offset: usize,
        len: usize,
    },
    Indexes {
        values: Vec<usize>,
        offset: usize,
        reserved_bytes: usize,
    },
    Rows {
        values: Vec<Row>,
        offset: usize,
        reserved_bytes: usize,
    },
}

impl CandidateSet {
    fn next(&mut self, rows: &[Row]) -> Option<Row> {
        match self {
            Self::All { offset, len } => {
                if *offset >= *len {
                    return None;
                }
                let row = rows.get(*offset).cloned();
                *offset = offset.saturating_add(1);
                row
            }
            Self::Indexes { values, offset, .. } => {
                let index = values.get(*offset).copied()?;
                *offset = offset.saturating_add(1);
                rows.get(index).cloned()
            }
            Self::Rows { values, offset, .. } => {
                let row = values.get(*offset).cloned();
                *offset = offset.saturating_add(1);
                row
            }
        }
    }

    const fn reserved_bytes(&self) -> usize {
        match self {
            Self::All { .. } => 0,
            Self::Indexes { reserved_bytes, .. } | Self::Rows { reserved_bytes, .. } => {
                *reserved_bytes
            }
        }
    }
}

struct GroupPrograms {
    group_by: Vec<ExpressionProgram>,
    projection: Vec<GroupProgram>,
    having: Option<GroupProgram>,
    aggregate_specs: Vec<AggregateSpec>,
}

impl GroupPrograms {
    fn compile(plan: &AdvancedExecutionPlan, max_depth: usize) -> Result<Self> {
        let group_by = plan
            .group_by
            .iter()
            .map(|expr| ExpressionProgram::compile_with_limit(expr, false, max_depth))
            .collect::<Result<Vec<_>>>()?;
        let mut aggregate_specs = Vec::new();
        let projection = plan
            .projection
            .iter()
            .map(|projection| {
                GroupProgram::compile(&projection.expr, &mut aggregate_specs, max_depth)
            })
            .collect::<Result<Vec<_>>>()?;
        let having = plan
            .having
            .as_ref()
            .map(|expr| GroupProgram::compile(expr, &mut aggregate_specs, max_depth))
            .transpose()?;
        Ok(Self {
            group_by,
            projection,
            having,
            aggregate_specs,
        })
    }

    fn group_key(&self, row: &Row, params: &[Value], stack: &mut Vec<Value>) -> Result<Vec<Value>> {
        self.group_by
            .iter()
            .map(|program| program.evaluate_reusing(&row.values, params, stack))
            .collect()
    }

    fn project_group(
        &self,
        group: &GroupAccumulator,
        params: &[Value],
        stack: &mut Vec<Value>,
    ) -> Result<Option<Row>> {
        let aggregate_values = group
            .aggregates
            .iter()
            .map(AggregateState::value)
            .collect::<Result<Vec<_>>>()?;
        if let Some(having) = &self.having {
            match having.evaluate(
                &group.representative.values,
                params,
                &aggregate_values,
                stack,
            )? {
                Value::Boolean(true) => {}
                Value::Boolean(false) | Value::Null => return Ok(None),
                _ => return Err(DbError::new("42804", "HAVING must evaluate to boolean")),
            }
        }
        self.projection
            .iter()
            .map(|program| {
                program.evaluate(
                    &group.representative.values,
                    params,
                    &aggregate_values,
                    stack,
                )
            })
            .collect::<Result<Vec<_>>>()
            .map(Row::new)
            .map(Some)
    }
}

#[derive(Clone)]
struct AggregateSpec {
    function: AggregateFunction,
    argument: Option<ExpressionProgram>,
    source: Option<BoundExpr>,
}

#[derive(Debug, Clone)]
struct GroupProgram {
    instructions: Vec<GroupInstruction>,
}

#[derive(Debug, Clone)]
enum GroupInstruction {
    LoadColumn(usize),
    LoadLiteral(Value),
    LoadParameter(usize),
    Unary(UnaryOperator),
    Binary(BinaryOperator),
    AggregateValue(usize),
    Coerce(ordadb_types::ScalarType),
}

impl GroupProgram {
    fn compile(
        expr: &BoundExpr,
        aggregate_specs: &mut Vec<AggregateSpec>,
        max_depth: usize,
    ) -> Result<Self> {
        let mut instructions = Vec::new();
        let mut pending = vec![(expr, false, 0_usize)];
        while let Some((expression, emitted_children, depth)) = pending.pop() {
            if depth > max_depth {
                return Err(program_limit_error(format!(
                    "group expression exceeds the depth limit of {max_depth}"
                )));
            }
            if emitted_children {
                match &expression.kind {
                    BoundExprKind::Unary { op, .. } => {
                        instructions.push(GroupInstruction::Unary(*op));
                    }
                    BoundExprKind::Binary { op, .. } => {
                        instructions.push(GroupInstruction::Binary(*op));
                    }
                    _ => {
                        return Err(DbError::internal(
                            "group expression compiler emitted an invalid parent",
                        ));
                    }
                }
                instructions.push(GroupInstruction::Coerce(expression.data_type.clone()));
                continue;
            }
            match &expression.kind {
                BoundExprKind::Column { index } => {
                    instructions.push(GroupInstruction::LoadColumn(*index));
                    instructions.push(GroupInstruction::Coerce(expression.data_type.clone()));
                }
                BoundExprKind::Literal(value) => {
                    instructions.push(GroupInstruction::LoadLiteral(value.clone()));
                    instructions.push(GroupInstruction::Coerce(expression.data_type.clone()));
                }
                BoundExprKind::Parameter { index } => {
                    instructions.push(GroupInstruction::LoadParameter(*index));
                    instructions.push(GroupInstruction::Coerce(expression.data_type.clone()));
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
                    let source = argument.as_deref().cloned();
                    let existing = aggregate_specs
                        .iter()
                        .position(|spec| spec.function == *function && spec.source == source);
                    let slot = if let Some(existing) = existing {
                        existing
                    } else {
                        let argument = source
                            .as_ref()
                            .map(|argument| {
                                ExpressionProgram::compile_with_limit(argument, false, max_depth)
                            })
                            .transpose()?;
                        aggregate_specs.push(AggregateSpec {
                            function: *function,
                            argument,
                            source: source.clone(),
                        });
                        aggregate_specs.len() - 1
                    };
                    instructions.push(GroupInstruction::AggregateValue(slot));
                    instructions.push(GroupInstruction::Coerce(expression.data_type.clone()));
                }
            }
            if instructions.len() > max_depth.saturating_mul(8) {
                return Err(program_limit_error(format!(
                    "group expression instruction count exceeds {}",
                    max_depth.saturating_mul(8)
                )));
            }
        }
        Ok(Self { instructions })
    }

    fn evaluate(
        &self,
        row: &[Value],
        params: &[Value],
        aggregates: &[Value],
        values: &mut Vec<Value>,
    ) -> Result<Value> {
        values.clear();
        for instruction in &self.instructions {
            match instruction {
                GroupInstruction::LoadColumn(index) => {
                    values.push(
                        row.get(*index).cloned().ok_or_else(|| {
                            DbError::internal("group column index is out of bounds")
                        })?,
                    );
                }
                GroupInstruction::LoadLiteral(value) => values.push(value.clone()),
                GroupInstruction::LoadParameter(index) => {
                    values.push(params.get(index - 1).cloned().ok_or_else(|| {
                        DbError::new("42P02", format!("no value supplied for parameter ${index}"))
                    })?);
                }
                GroupInstruction::Unary(operator) => {
                    let value = values
                        .pop()
                        .ok_or_else(|| DbError::internal("group value stack underflow"))?;
                    values.push(evaluate_unary(*operator, value)?);
                }
                GroupInstruction::Binary(operator) => {
                    let right = values
                        .pop()
                        .ok_or_else(|| DbError::internal("group value stack underflow"))?;
                    let left = values
                        .pop()
                        .ok_or_else(|| DbError::internal("group value stack underflow"))?;
                    values.push(evaluate_binary(left, *operator, right)?);
                }
                GroupInstruction::AggregateValue(slot) => {
                    values.push(
                        aggregates
                            .get(*slot)
                            .cloned()
                            .ok_or_else(|| DbError::internal("aggregate slot is out of bounds"))?,
                    );
                }
                GroupInstruction::Coerce(target) => {
                    let value = values
                        .pop()
                        .ok_or_else(|| DbError::internal("group value stack underflow"))?;
                    values.push(super::coerce_value(value, target)?);
                }
            }
        }
        if values.len() != 1 {
            return Err(DbError::internal(
                "group expression did not produce exactly one value",
            ));
        }
        values
            .pop()
            .ok_or_else(|| DbError::internal("group expression result disappeared"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupAccumulator {
    key: Vec<Value>,
    representative: Row,
    aggregates: Vec<AggregateState>,
    first_ordinal: u64,
}

impl GroupAccumulator {
    fn new(
        key: Vec<Value>,
        representative: Row,
        first_ordinal: u64,
        specs: &[AggregateSpec],
        params: &[Value],
        stack: &mut Vec<Value>,
    ) -> Result<Self> {
        let mut group = Self {
            key,
            representative: representative.clone(),
            aggregates: specs.iter().map(AggregateState::new).collect(),
            first_ordinal,
        };
        group.update(specs, &representative, params, stack)?;
        Ok(group)
    }

    fn empty(specs: &[AggregateSpec]) -> Self {
        Self {
            key: Vec::new(),
            representative: Row::new(Vec::new()),
            aggregates: specs.iter().map(AggregateState::new).collect(),
            first_ordinal: 0,
        }
    }

    fn update(
        &mut self,
        specs: &[AggregateSpec],
        row: &Row,
        params: &[Value],
        stack: &mut Vec<Value>,
    ) -> Result<()> {
        for (state, spec) in self.aggregates.iter_mut().zip(specs) {
            state.update(spec, row, params, stack)?;
        }
        Ok(())
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        if other.first_ordinal < self.first_ordinal {
            self.first_ordinal = other.first_ordinal;
            self.representative = other.representative.clone();
        }
        for (state, incoming) in self.aggregates.iter_mut().zip(other.aggregates) {
            state.merge(incoming)?;
        }
        Ok(())
    }

    fn estimated_bytes(&self) -> usize {
        estimated_row_bytes(&self.representative)
            .saturating_add(self.key.iter().map(estimated_value_bytes).sum::<usize>())
            .saturating_add(self.aggregates.len().saturating_mul(64))
            .saturating_add(64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum AggregateState {
    Count(u64),
    Sum(Option<Value>),
    Avg { sum: f64, count: u64 },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl AggregateState {
    fn new(spec: &AggregateSpec) -> Self {
        match spec.function {
            AggregateFunction::Count => Self::Count(0),
            AggregateFunction::Sum => Self::Sum(None),
            AggregateFunction::Avg => Self::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Min => Self::Min(None),
            AggregateFunction::Max => Self::Max(None),
        }
    }

    fn update(
        &mut self,
        spec: &AggregateSpec,
        row: &Row,
        params: &[Value],
        stack: &mut Vec<Value>,
    ) -> Result<()> {
        let value = spec
            .argument
            .as_ref()
            .map(|argument| argument.evaluate_reusing(&row.values, params, stack))
            .transpose()?;
        match self {
            Self::Count(count) => {
                if value.as_ref().is_none_or(|value| !value.is_null()) {
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| DbError::new("22003", "COUNT result is out of range"))?;
                }
            }
            Self::Sum(sum) => {
                if let Some(value) = value.filter(|value| !value.is_null()) {
                    *sum = Some(match sum.take() {
                        None => value,
                        Some(existing) => add_values(existing, value)?,
                    });
                }
            }
            Self::Avg { sum, count } => {
                if let Some(value) = value.filter(|value| !value.is_null()) {
                    *sum += numeric_value(&value)?;
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| DbError::new("22003", "AVG count is out of range"))?;
                }
            }
            Self::Min(selected) => select_value(selected, value, Ordering::Less)?,
            Self::Max(selected) => select_value(selected, value, Ordering::Greater)?,
        }
        Ok(())
    }

    fn merge(&mut self, incoming: Self) -> Result<()> {
        match (self, incoming) {
            (Self::Count(left), Self::Count(right)) => {
                *left = left
                    .checked_add(right)
                    .ok_or_else(|| DbError::new("22003", "COUNT result is out of range"))?;
            }
            (Self::Sum(left), Self::Sum(right)) => {
                if let Some(right) = right {
                    *left = Some(match left.take() {
                        None => right,
                        Some(existing) => add_values(existing, right)?,
                    });
                }
            }
            (
                Self::Avg {
                    sum: left_sum,
                    count: left_count,
                },
                Self::Avg {
                    sum: right_sum,
                    count: right_count,
                },
            ) => {
                *left_sum += right_sum;
                *left_count = left_count
                    .checked_add(right_count)
                    .ok_or_else(|| DbError::new("22003", "AVG count is out of range"))?;
            }
            (Self::Min(left), Self::Min(right)) => {
                select_value(left, right, Ordering::Less)?;
            }
            (Self::Max(left), Self::Max(right)) => {
                select_value(left, right, Ordering::Greater)?;
            }
            _ => return Err(DbError::internal("aggregate spill state kind changed")),
        }
        Ok(())
    }

    fn value(&self) -> Result<Value> {
        match self {
            Self::Count(count) => i64::try_from(*count)
                .map(Value::Int64)
                .map_err(|_| DbError::new("22003", "COUNT result is out of range")),
            Self::Sum(value) | Self::Min(value) | Self::Max(value) => {
                Ok(value.clone().unwrap_or(Value::Null))
            }
            Self::Avg { sum: _, count } if *count == 0 => Ok(Value::Null),
            Self::Avg { sum, count } => Ok(Value::Float64(*sum / *count as f64)),
        }
    }
}

fn select_value(
    selected: &mut Option<Value>,
    value: Option<Value>,
    desired: Ordering,
) -> Result<()> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let replace = selected
        .as_ref()
        .map(|current| super::compare_values(&value, current).map(|order| order == desired))
        .transpose()?
        .unwrap_or(true);
    if replace {
        *selected = Some(value);
    }
    Ok(())
}

fn add_values(left: Value, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Int16(left), Value::Int16(right)) => i64::from(left)
            .checked_add(i64::from(right))
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        (Value::Int32(left), Value::Int32(right)) => i64::from(left)
            .checked_add(i64::from(right))
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        (Value::Int64(left), Value::Int64(right)) => left
            .checked_add(right)
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        (Value::Int64(left), Value::Int16(right)) => left
            .checked_add(i64::from(right))
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        (Value::Int64(left), Value::Int32(right)) => left
            .checked_add(i64::from(right))
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        (Value::Float32(left), Value::Float32(right)) => {
            Ok(Value::Float64(f64::from(left) + f64::from(right)))
        }
        (Value::Float64(left), Value::Float32(right)) => {
            Ok(Value::Float64(left + f64::from(right)))
        }
        (Value::Float64(left), Value::Float64(right)) => Ok(Value::Float64(left + right)),
        (Value::Decimal(left), Value::Decimal(right)) => left
            .checked_add(right)
            .map(Value::Decimal)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        _ => Err(DbError::new("42804", "SUM values have mixed types")),
    }
}

fn numeric_value(value: &Value) -> Result<f64> {
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

struct RowsOutputBuilder {
    order_by: Vec<BoundOrder>,
    rows: Vec<Row>,
    reserved_bytes: usize,
    run_paths: Vec<PathBuf>,
}

impl RowsOutputBuilder {
    fn new(order_by: &[BoundOrder]) -> Self {
        Self {
            order_by: order_by.to_vec(),
            rows: Vec::new(),
            reserved_bytes: 0,
            run_paths: Vec::new(),
        }
    }

    fn push(
        &mut self,
        row: Row,
        memory: &mut QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<()> {
        let bytes = estimated_row_bytes(&row);
        if !self.rows.is_empty()
            && memory.current_bytes().saturating_add(bytes) > memory.soft_limit_bytes()
        {
            sort_rows(&mut self.rows, &self.order_by)?;
            self.run_paths
                .push(spill.write_sorted_run(&self.rows, memory.hard_limit_bytes())?);
            self.rows.clear();
            memory.release(self.reserved_bytes);
            self.reserved_bytes = 0;
        }
        memory.reserve(bytes)?;
        self.reserved_bytes = self.reserved_bytes.saturating_add(bytes);
        self.rows.push(row);
        Ok(())
    }

    fn finish(
        mut self,
        memory: &mut QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<RowsOutput> {
        if self.run_paths.is_empty() {
            sort_rows(&mut self.rows, &self.order_by)?;
            return Ok(RowsOutput::Memory {
                rows: self.rows,
                offset: 0,
                reserved_bytes: self.reserved_bytes,
            });
        }
        if !self.rows.is_empty() {
            sort_rows(&mut self.rows, &self.order_by)?;
            self.run_paths
                .push(spill.write_sorted_run(&self.rows, memory.hard_limit_bytes())?);
            memory.release(self.reserved_bytes);
        }
        let runs = self
            .run_paths
            .iter()
            .map(|path| SpillRun::open(path, memory.hard_limit_bytes()))
            .collect::<Result<Vec<_>>>()?;
        Ok(RowsOutput::Runs(runs))
    }
}

enum RowsOutput {
    Memory {
        rows: Vec<Row>,
        offset: usize,
        reserved_bytes: usize,
    },
    Runs(Vec<SpillRun>),
}

impl RowsOutput {
    fn next_row(
        &mut self,
        order_by: &[BoundOrder],
        hard_limit: usize,
        memory: &mut QueryMemoryContext,
    ) -> Result<Option<Row>> {
        match self {
            Self::Memory {
                rows,
                offset,
                reserved_bytes,
            } => {
                let row = rows.get(*offset).cloned();
                *offset = offset.saturating_add(1);
                if row.is_none() && *reserved_bytes > 0 {
                    memory.release(*reserved_bytes);
                    *reserved_bytes = 0;
                }
                Ok(row)
            }
            Self::Runs(runs) => {
                let mut selected: Option<usize> = None;
                for (index, run) in runs.iter().enumerate() {
                    let Some(candidate) = &run.current else {
                        continue;
                    };
                    let replace = match selected {
                        None => true,
                        Some(selected_index) => {
                            let current = runs[selected_index]
                                .current
                                .as_ref()
                                .ok_or_else(|| DbError::internal("spill merge row disappeared"))?;
                            compare_rows(candidate, current, order_by)? == Ordering::Less
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
                    .ok_or_else(|| DbError::internal("spill merge row disappeared"))?;
                runs[selected].advance(hard_limit)?;
                Ok(Some(row))
            }
        }
    }
}

impl SpillManager {
    fn ensure_query_dir(&mut self) -> Result<PathBuf> {
        if let Some(query_dir) = &self.query_dir {
            return Ok(query_dir.clone());
        }
        let query_id = super::NEXT_QUERY_ID.fetch_add(1, super::AtomicOrdering::Relaxed);
        let query_dir = self
            .root
            .join(format!("ordadb-query-{}-{query_id}", std::process::id()));
        std::fs::create_dir(&query_dir).map_err(spill_io_error)?;
        self.query_dir = Some(query_dir.clone());
        Ok(query_dir)
    }

    fn partition_paths(&mut self, label: &str, count: usize) -> Result<Vec<PathBuf>> {
        let query_dir = self.ensure_query_dir()?;
        Ok((0..count)
            .map(|partition| query_dir.join(format!("{label}-{partition}.spill")))
            .collect())
    }

    fn write_partitioned_rows(
        &mut self,
        label: &str,
        rows: &[Row],
        key_index: usize,
        count: usize,
        hard_limit: usize,
    ) -> Result<Vec<PathBuf>> {
        let paths = self.partition_paths(label, count)?;
        let mut writers = paths
            .iter()
            .map(|path| create_spill_writer(path))
            .collect::<Result<Vec<_>>>()?;
        for row in rows {
            let value = row
                .values
                .get(key_index)
                .ok_or_else(|| DbError::internal("spill key is out of bounds"))?;
            if value.is_null() {
                continue;
            }
            let key = encode_hash_value(value)?;
            let partition = stable_partition(&key, count);
            write_spill_record(&mut writers[partition], row, hard_limit)?;
        }
        for writer in &mut writers {
            writer.flush().map_err(spill_io_error)?;
        }
        Ok(paths)
    }

    fn read_matching_rows(
        &self,
        path: &Path,
        key_index: usize,
        key: &[u8],
        memory: &mut QueryMemoryContext,
    ) -> Result<Vec<Row>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        let mut reader = open_spill_reader(path)?;
        while let Some(row) = read_spill_record::<Row>(&mut reader, memory.hard_limit_bytes())? {
            let value = row
                .values
                .get(key_index)
                .ok_or_else(|| DbError::new("XX001", "hash join spill key is missing"))?;
            if encode_hash_value(value)? == key {
                let row_bytes = estimated_row_bytes(&row);
                memory.reserve(row_bytes)?;
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn write_group_partials(
        &self,
        paths: &[PathBuf],
        groups: &[GroupAccumulator],
        hard_limit: usize,
    ) -> Result<()> {
        let mut writers = paths
            .iter()
            .map(|path| {
                if path.exists() {
                    OpenOptions::new()
                        .append(true)
                        .open(path)
                        .map(BufWriter::new)
                        .map_err(spill_io_error)
                } else {
                    create_spill_writer(path)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        for group in groups {
            let key = serde_json::to_vec(&group.key).map_err(|error| {
                DbError::new("58030", "aggregate spill key encoding failed")
                    .with_detail(error.to_string())
            })?;
            let partition = stable_partition(&key, paths.len());
            write_spill_record(&mut writers[partition], group, hard_limit)?;
        }
        for writer in &mut writers {
            writer.flush().map_err(spill_io_error)?;
        }
        Ok(())
    }

    fn read_and_merge_groups(
        &self,
        path: &Path,
        memory: &mut QueryMemoryContext,
        specs: &[AggregateSpec],
    ) -> Result<(Vec<GroupAccumulator>, usize)> {
        let mut reader = open_spill_reader(path)?;
        let mut groups = Vec::<GroupAccumulator>::new();
        let mut reserved_bytes = 0_usize;
        while let Some(incoming) =
            read_spill_record::<GroupAccumulator>(&mut reader, memory.hard_limit_bytes())?
        {
            if incoming.aggregates.len() != specs.len() {
                return Err(DbError::new(
                    "XX001",
                    "aggregate spill state width is invalid",
                ));
            }
            if let Some(group) = groups.iter_mut().find(|group| group.key == incoming.key) {
                group.merge(incoming)?;
            } else {
                let group_bytes = incoming.estimated_bytes();
                memory.reserve(group_bytes)?;
                reserved_bytes = reserved_bytes.saturating_add(group_bytes);
                groups.push(incoming);
            }
        }
        groups.sort_by_key(|group| group.first_ordinal);
        Ok((groups, reserved_bytes))
    }
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

fn encode_hash_value(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| {
        DbError::internal("hash key encoding failed").with_detail(error.to_string())
    })
}

fn stable_partition(key: &[u8], count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % count.max(1)
}

fn limit_from_value(value: Value) -> Result<usize> {
    match value {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use ordadb_sql::{BoundExpr, BoundExprKind, BoundJoin, BoundProjection, BoundTable, JoinKind};
    use ordadb_types::{Field, Identifier, IndexId, ScalarType, TableId};
    use tempfile::tempdir;

    use super::*;

    fn column(index: usize, data_type: ScalarType) -> BoundExpr {
        BoundExpr {
            kind: BoundExprKind::Column { index },
            data_type,
            nullable: false,
        }
    }

    fn projection(index: usize, name: &str) -> BoundProjection {
        BoundProjection {
            expr: column(index, ScalarType::Int64),
            field: Field::new(name, ScalarType::Int64, false),
        }
    }

    fn table(table_id: TableId, binding: &str, offset: usize) -> BoundTable {
        BoundTable {
            table_id,
            binding: Identifier::unquoted(binding),
            offset,
            width: 1,
            nullable: false,
        }
    }

    #[test]
    fn hash_join_spills_and_cleans_its_query_directory() {
        let spill_root = tempdir().expect("spill root");
        let left_id = TableId::new(1);
        let right_id = TableId::new(2);
        let left = (0..128)
            .map(|value| Row::new(vec![Value::Int64(value)]))
            .collect::<Vec<_>>();
        let right = left.clone();
        let tables = BTreeMap::from([(left_id, Arc::new(left)), (right_id, Arc::new(right))]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let join = BoundJoin {
            table: table(right_id, "right_items", 1),
            kind: JoinKind::Inner,
            on: BoundExpr {
                kind: BoundExprKind::Binary {
                    left: Box::new(column(0, ScalarType::Int64)),
                    op: BinaryOperator::Eq,
                    right: Box::new(column(1, ScalarType::Int64)),
                },
                data_type: ScalarType::Boolean,
                nullable: false,
            },
        };
        let plan = AdvancedExecutionPlan {
            table: table(left_id, "left_items", 0),
            joins: vec![join],
            schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
            projection: vec![projection(0, "id")],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            aggregate: false,
        };
        let options = ExecutionOptions {
            batch_rows: 17,
            soft_memory_bytes: 512,
            hard_memory_bytes: 1024 * 1024,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        };
        let mut cursor =
            AdvancedExecutionCursor::with_options(plan, &context, options).expect("cursor");
        let mut count = 0;
        while let Some(batch) = cursor.next_batch().expect("batch") {
            count += batch.rows.len();
        }
        assert_eq!(count, 128);
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("spill root entries")
                .next()
                .is_some()
        );
        drop(cursor);
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("clean spill root")
                .next()
                .is_none()
        );
    }

    #[test]
    fn hash_aggregate_spills_partial_states_and_streams_batches() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let rows = (0..96)
            .map(|value| Row::new(vec![Value::Int64(value)]))
            .collect::<Vec<_>>();
        let tables = BTreeMap::from([(table_id, Arc::new(rows))]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let count = BoundExpr {
            kind: BoundExprKind::Aggregate {
                function: AggregateFunction::Count,
                argument: None,
            },
            data_type: ScalarType::Int64,
            nullable: false,
        };
        let plan = AdvancedExecutionPlan {
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            schema: Schema::new(vec![
                Field::new("id", ScalarType::Int64, false),
                Field::new("count", ScalarType::Int64, false),
            ]),
            projection: vec![
                projection(0, "id"),
                BoundProjection {
                    expr: count,
                    field: Field::new("count", ScalarType::Int64, false),
                },
            ],
            filter: None,
            group_by: vec![column(0, ScalarType::Int64)],
            having: None,
            order_by: vec![BoundOrder {
                column_index: 0,
                ascending: true,
                nulls_first: None,
            }],
            limit: None,
            aggregate: true,
        };
        let options = ExecutionOptions {
            batch_rows: 13,
            soft_memory_bytes: 768,
            hard_memory_bytes: 1024 * 1024,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        };
        let mut cursor =
            AdvancedExecutionCursor::with_options(plan, &context, options).expect("cursor");
        let mut output = Vec::new();
        while let Some(batch) = cursor.next_batch().expect("batch") {
            assert!(batch.rows.len() <= 13);
            output.extend(batch.rows);
        }
        assert_eq!(output.len(), 96);
        assert_eq!(
            output.first(),
            Some(&Row::new(vec![Value::Int64(0), Value::Int64(1)]))
        );
        assert_eq!(
            output.last(),
            Some(&Row::new(vec![Value::Int64(95), Value::Int64(1)]))
        );
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("spill root entries")
                .next()
                .is_some()
        );
        drop(cursor);
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("clean spill root")
                .next()
                .is_none()
        );
    }

    #[test]
    fn nested_left_join_streams_matches_and_null_extensions() {
        let spill_root = tempdir().expect("spill root");
        let left_id = TableId::new(1);
        let right_id = TableId::new(2);
        let tables = BTreeMap::from([
            (
                left_id,
                Arc::new(vec![
                    Row::new(vec![Value::Int64(1)]),
                    Row::new(vec![Value::Int64(2)]),
                ]),
            ),
            (right_id, Arc::new(vec![Row::new(vec![Value::Int64(1)])])),
        ]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let plan = AdvancedExecutionPlan {
            table: table(left_id, "left_items", 0),
            joins: vec![BoundJoin {
                table: BoundTable {
                    nullable: true,
                    ..table(right_id, "right_items", 1)
                },
                kind: JoinKind::Left,
                on: BoundExpr {
                    kind: BoundExprKind::Binary {
                        left: Box::new(column(0, ScalarType::Int64)),
                        op: BinaryOperator::Eq,
                        right: Box::new(column(1, ScalarType::Int64)),
                    },
                    data_type: ScalarType::Boolean,
                    nullable: false,
                },
            }],
            schema: Schema::new(vec![
                Field::new("left_id", ScalarType::Int64, false),
                Field::new("right_id", ScalarType::Int64, true),
            ]),
            projection: vec![
                projection(0, "left_id"),
                BoundProjection {
                    expr: BoundExpr {
                        nullable: true,
                        ..column(1, ScalarType::Int64)
                    },
                    field: Field::new("right_id", ScalarType::Int64, true),
                },
            ],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            aggregate: false,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("cursor");
        let rows = cursor.next_batch().expect("batch").expect("rows").rows;
        assert_eq!(
            rows,
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(1)]),
                Row::new(vec![Value::Int64(2), Value::Null]),
            ]
        );
        assert!(cursor.next_batch().expect("end").is_none());
    }

    #[test]
    fn non_aggregate_sort_spills_then_applies_limit() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let rows = (0..200)
            .rev()
            .map(|value| Row::new(vec![Value::Int64(value)]))
            .collect::<Vec<_>>();
        let tables = BTreeMap::from([(table_id, Arc::new(rows))]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let plan = AdvancedExecutionPlan {
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
            projection: vec![projection(0, "id")],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: vec![BoundOrder {
                column_index: 0,
                ascending: true,
                nulls_first: None,
            }],
            limit: Some(BoundExpr {
                kind: BoundExprKind::Literal(Value::Int64(5)),
                data_type: ScalarType::Int64,
                nullable: false,
            }),
            aggregate: false,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                batch_rows: 3,
                soft_memory_bytes: 256,
                hard_memory_bytes: 4096,
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("cursor");
        let mut output = Vec::new();
        while let Some(batch) = cursor.next_batch().expect("batch") {
            output.extend(batch.rows);
        }
        assert_eq!(
            output,
            (0..5)
                .map(|value| Row::new(vec![Value::Int64(value)]))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn empty_global_aggregate_returns_count_zero_and_null_average() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let tables = BTreeMap::from([(table_id, Arc::new(Vec::new()))]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let plan = AdvancedExecutionPlan {
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            schema: Schema::new(vec![
                Field::new("count", ScalarType::Int64, false),
                Field::new("average", ScalarType::Float64, true),
            ]),
            projection: vec![
                BoundProjection {
                    expr: BoundExpr {
                        kind: BoundExprKind::Aggregate {
                            function: AggregateFunction::Count,
                            argument: None,
                        },
                        data_type: ScalarType::Int64,
                        nullable: false,
                    },
                    field: Field::new("count", ScalarType::Int64, false),
                },
                BoundProjection {
                    expr: BoundExpr {
                        kind: BoundExprKind::Aggregate {
                            function: AggregateFunction::Avg,
                            argument: Some(Box::new(column(0, ScalarType::Int64))),
                        },
                        data_type: ScalarType::Float64,
                        nullable: true,
                    },
                    field: Field::new("average", ScalarType::Float64, true),
                },
            ],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            aggregate: true,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("cursor");
        assert_eq!(
            cursor
                .next_batch()
                .expect("batch")
                .expect("aggregate row")
                .rows,
            vec![Row::new(vec![Value::Int64(0), Value::Null])]
        );
        assert!(cursor.next_batch().expect("end").is_none());
    }

    #[test]
    fn hash_join_uses_memory_lookup_when_the_grant_is_sufficient() {
        let spill_root = tempdir().expect("spill root");
        let left_id = TableId::new(1);
        let right_id = TableId::new(2);
        let rows = (0..128)
            .map(|value| Row::new(vec![Value::Int64(value)]))
            .collect::<Vec<_>>();
        let tables = BTreeMap::from([
            (left_id, Arc::new(rows.clone())),
            (right_id, Arc::new(rows)),
        ]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let plan = AdvancedExecutionPlan {
            table: table(left_id, "left_items", 0),
            joins: vec![BoundJoin {
                table: table(right_id, "right_items", 1),
                kind: JoinKind::Inner,
                on: BoundExpr {
                    kind: BoundExprKind::Binary {
                        left: Box::new(column(0, ScalarType::Int64)),
                        op: BinaryOperator::Eq,
                        right: Box::new(column(1, ScalarType::Int64)),
                    },
                    data_type: ScalarType::Boolean,
                    nullable: false,
                },
            }],
            schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
            projection: vec![projection(0, "id")],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            aggregate: false,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                batch_rows: 31,
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("cursor");
        let mut count = 0;
        while let Some(batch) = cursor.next_batch().expect("batch") {
            count += batch.rows.len();
        }
        assert_eq!(count, 128);
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("spill root")
                .next()
                .is_none()
        );
    }

    #[test]
    fn advanced_filter_uses_parameters_without_materializing_rejected_rows() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let tables = BTreeMap::from([(
            table_id,
            Arc::new(vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)]),
            ]),
        )]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let params = vec![Value::Int64(2)];
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &params,
        };
        let plan = AdvancedExecutionPlan {
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
            projection: vec![projection(0, "id")],
            filter: Some(BoundExpr {
                kind: BoundExprKind::Binary {
                    left: Box::new(column(0, ScalarType::Int64)),
                    op: BinaryOperator::Eq,
                    right: Box::new(BoundExpr {
                        kind: BoundExprKind::Parameter { index: 1 },
                        data_type: ScalarType::Int64,
                        nullable: false,
                    }),
                },
                data_type: ScalarType::Boolean,
                nullable: false,
            }),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            aggregate: false,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("cursor");
        assert_eq!(
            cursor.next_batch().expect("batch").expect("row").rows,
            vec![Row::new(vec![Value::Int64(2)])]
        );
        assert!(cursor.next_batch().expect("end").is_none());
    }

    #[test]
    fn aggregate_having_false_emits_no_rows() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let tables = BTreeMap::from([(
            table_id,
            Arc::new(vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]),
        )]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let count = BoundExpr {
            kind: BoundExprKind::Aggregate {
                function: AggregateFunction::Count,
                argument: None,
            },
            data_type: ScalarType::Int64,
            nullable: false,
        };
        let plan = AdvancedExecutionPlan {
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            schema: Schema::new(vec![Field::new("count", ScalarType::Int64, false)]),
            projection: vec![BoundProjection {
                expr: count.clone(),
                field: Field::new("count", ScalarType::Int64, false),
            }],
            filter: None,
            group_by: Vec::new(),
            having: Some(BoundExpr {
                kind: BoundExprKind::Binary {
                    left: Box::new(count),
                    op: BinaryOperator::Gt,
                    right: Box::new(BoundExpr {
                        kind: BoundExprKind::Literal(Value::Int64(5)),
                        data_type: ScalarType::Int64,
                        nullable: false,
                    }),
                },
                data_type: ScalarType::Boolean,
                nullable: false,
            }),
            order_by: Vec::new(),
            limit: None,
            aggregate: true,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("cursor");
        assert!(cursor.next_batch().expect("no rows").is_none());
    }
}
