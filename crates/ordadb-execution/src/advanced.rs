use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, hash_map::Entry};
use std::fs::{File, OpenOptions};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering as AtomicOrdering},
};

use ordadb_index::BPlusTree;
use ordadb_optimizer::{JoinStrategy, PlanNode, choose_join_strategy};
use ordadb_sql::{
    AggregateFunction, BinaryOperator, BoundExpr, BoundExprKind, BoundOrder, BoundProjection,
    BoundTable, BoundWindow, BoundWindowFrame, BoundWindowFrameBound, JoinKind, ScalarFunction,
    SubqueryQuantifier, UnaryOperator, WindowFrameUnits, WindowFunction,
};
use ordadb_types::{Batch, DbError, IndexId, Result, Row, ScalarType, Schema, TableId, Value};
use serde::{Deserialize, Serialize};

use super::{
    BatchPool, ExecutionContext, ExecutionCursor, ExecutionOptions, ExpressionProgram,
    ExpressionStack, QueryMemoryContext, Reservation, ReservedSpillReader, ReservedSpillWriter,
    SPILL_MAGIC, SpillManager, SpillMergeCursor, compare_rows, compare_values, create_spill_writer,
    estimated_row_bytes, estimated_value_bytes, evaluate_binary, evaluate_unary, open_spill_reader,
    program_limit_error, read_spill_record, reserve_spill_writer, sort_rows, spill_io_error,
    write_spill_record,
};

const HASH_PARTITIONS: usize = 32;

#[derive(Debug, Clone)]
pub enum QueryExecutionPlan {
    Simple { plan: Box<PlanNode>, schema: Schema },
    Advanced(Box<AdvancedExecutionPlan>),
}

#[derive(Debug, Clone)]
pub enum ApplyExecutionKind {
    Scalar,
    Exists {
        negated: bool,
    },
    In {
        left: BoundExpr,
        negated: bool,
    },
    Quantified {
        left: BoundExpr,
        op: BinaryOperator,
        quantifier: SubqueryQuantifier,
    },
    RowScalar {
        left: Vec<BoundExpr>,
        op: BinaryOperator,
        operand_types: Vec<ScalarType>,
    },
    RowQuantified {
        left: Vec<BoundExpr>,
        op: BinaryOperator,
        quantifier: SubqueryQuantifier,
        negated: bool,
        operand_types: Vec<ScalarType>,
    },
}

#[derive(Debug, Clone)]
pub struct ApplyExecutionPlan {
    pub kind: ApplyExecutionKind,
    pub query: Box<QueryExecutionPlan>,
    pub correlation_indexes: Vec<usize>,
}

#[derive(Debug, Clone)]
pub enum JoinExecutionSource {
    Table(BoundTable),
    Derived {
        query: Box<QueryExecutionPlan>,
        correlation_indexes: Vec<usize>,
        offset: usize,
        width: usize,
    },
}

#[derive(Debug, Clone)]
pub struct JoinExecutionPlan {
    pub source: JoinExecutionSource,
    pub kind: JoinKind,
    pub on: BoundExpr,
}

#[derive(Debug, Clone)]
pub struct AdvancedExecutionPlan {
    pub table: BoundTable,
    pub joins: Vec<JoinExecutionPlan>,
    pub applies: Vec<ApplyExecutionPlan>,
    pub windows: Vec<BoundWindow>,
    pub schema: Schema,
    pub projection: Vec<BoundProjection>,
    pub distinct: bool,
    pub filter: Option<BoundExpr>,
    pub group_by: Vec<BoundExpr>,
    pub having: Option<BoundExpr>,
    pub order_by: Vec<BoundOrder>,
    pub offset: Option<BoundExpr>,
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
    offset_remaining: usize,
    limit: Option<usize>,
    emitted: usize,
    params: Vec<Value>,
    options: ExecutionOptions,
    memory: QueryMemoryContext,
    pool: BatchPool,
    spill: SpillManager,
    expression_stack: ExpressionStack,
    applies: Vec<ApplyRuntime>,
    windows: Vec<WindowProgram>,
    aggregate_window_projection: Option<Vec<usize>>,
    cancellation: Option<Arc<AtomicBool>>,
    apply_row_reservation: Reservation,
    nested_memory_peak: usize,
    output: Option<RowsOutput>,
    in_flight: Option<Reservation>,
    distinct: bool,
    distinct_rows: HashSet<DistinctRowKey>,
    distinct_reservation: Reservation,
    aggregate: bool,
    exhausted: bool,
}

enum QueryExecutionCursor {
    Simple(Box<ExecutionCursor>),
    Advanced(Box<AdvancedExecutionCursor>),
}

impl QueryExecutionCursor {
    fn new(
        plan: QueryExecutionPlan,
        context: &ExecutionContext<'_>,
        options: ExecutionOptions,
    ) -> Result<Self> {
        match plan {
            QueryExecutionPlan::Simple { plan, schema } => Ok(Self::Simple(Box::new(
                ExecutionCursor::with_options(&plan, context, schema, options)?,
            ))),
            QueryExecutionPlan::Advanced(plan) => Ok(Self::Advanced(Box::new(
                AdvancedExecutionCursor::with_options(*plan, context, options)?,
            ))),
        }
    }

    fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Self::Simple(cursor) => cursor.next_batch(),
            Self::Advanced(cursor) => cursor.next_batch(),
        }
    }

    fn memory_peak_bytes(&self) -> usize {
        match self {
            Self::Simple(cursor) => cursor.memory().peak_bytes(),
            Self::Advanced(cursor) => cursor.memory_peak_bytes(),
        }
    }

    fn memory_current_bytes(&self) -> usize {
        match self {
            Self::Simple(cursor) => cursor.memory().current_bytes(),
            Self::Advanced(cursor) => cursor.memory().current_bytes(),
        }
    }
}

enum CachedApplyKind {
    Scalar(Value),
    Exists(bool),
    Quantified {
        left: Box<ExpressionProgram>,
        op: BinaryOperator,
        quantifier: SubqueryQuantifier,
        negated: bool,
        candidates: Vec<Value>,
    },
    RowScalar {
        left: Vec<ExpressionProgram>,
        op: BinaryOperator,
        operand_types: Vec<ScalarType>,
        candidate: Option<Row>,
    },
    RowQuantified {
        left: Vec<ExpressionProgram>,
        op: BinaryOperator,
        quantifier: SubqueryQuantifier,
        negated: bool,
        operand_types: Vec<ScalarType>,
        candidates: Vec<Row>,
    },
}

enum DynamicApplyKind {
    Scalar,
    Exists {
        negated: bool,
    },
    Quantified {
        left: Box<ExpressionProgram>,
        op: BinaryOperator,
        quantifier: SubqueryQuantifier,
        negated: bool,
    },
    RowScalar {
        left: Vec<ExpressionProgram>,
        op: BinaryOperator,
        operand_types: Vec<ScalarType>,
    },
    RowQuantified {
        left: Vec<ExpressionProgram>,
        op: BinaryOperator,
        quantifier: SubqueryQuantifier,
        negated: bool,
        operand_types: Vec<ScalarType>,
    },
}

enum CollectedApplyRows {
    Scalar(Value),
    ScalarRow(Option<Row>),
    Exists(bool),
    Candidates(Vec<Value>),
    CandidateRows(Vec<Row>),
}

struct CorrelatedApplyRuntime {
    kind: DynamicApplyKind,
    query: QueryExecutionPlan,
    correlation_indexes: Vec<usize>,
    tables: BTreeMap<TableId, Arc<Vec<Row>>>,
    indexes: BTreeMap<IndexId, Arc<BPlusTree>>,
    options: ExecutionOptions,
}

enum ApplyRuntimeState {
    Cached(CachedApplyKind),
    Correlated(Box<CorrelatedApplyRuntime>),
}

struct ApplyRuntime {
    state: ApplyRuntimeState,
    memory: QueryMemoryContext,
    reservation: Reservation,
}

impl ApplyRuntime {
    fn new(
        plan: ApplyExecutionPlan,
        context: &ExecutionContext<'_>,
        options: &ExecutionOptions,
        memory: &QueryMemoryContext,
    ) -> Result<(Self, usize)> {
        let ApplyExecutionPlan {
            kind,
            query,
            correlation_indexes,
        } = plan;
        let kind = DynamicApplyKind::compile(kind, options.max_expression_depth)?;
        let mut reservation = memory.try_reserve(0)?;
        if !correlation_indexes.is_empty() {
            return Ok((
                Self {
                    state: ApplyRuntimeState::Correlated(Box::new(CorrelatedApplyRuntime {
                        kind,
                        query: *query,
                        correlation_indexes,
                        tables: context.tables.clone(),
                        indexes: context.indexes.clone(),
                        options: options.clone(),
                    })),
                    memory: memory.clone(),
                    reservation,
                },
                0,
            ));
        }

        let mut cursor =
            QueryExecutionCursor::new(*query, context, nested_execution_options(options, memory)?)?;
        let (collected, nested_peak) =
            collect_apply_rows(&kind, &mut cursor, memory, &mut reservation)?;
        Ok((
            Self {
                state: ApplyRuntimeState::Cached(kind.into_cached(collected)?),
                memory: memory.clone(),
                reservation,
            },
            nested_peak,
        ))
    }

    fn evaluate(
        &mut self,
        row: &[Value],
        params: &[Value],
        stack: &mut ExpressionStack,
    ) -> Result<(Value, usize)> {
        let memory = self.memory.clone();
        match &mut self.state {
            ApplyRuntimeState::Cached(kind) => {
                evaluate_cached_apply(kind, row, params, stack).map(|value| (value, 0))
            }
            ApplyRuntimeState::Correlated(correlated) => {
                let result = (|| {
                    self.reservation.resize(0)?;
                    let mut inner_params = params.to_vec();
                    let parameter_bytes = inner_params
                        .iter()
                        .map(estimated_value_bytes)
                        .sum::<usize>();
                    self.reservation.resize(parameter_bytes)?;
                    for index in &correlated.correlation_indexes {
                        let value = row.get(*index).cloned().ok_or_else(|| {
                            DbError::internal(format!(
                                "correlated outer column index {index} is out of range"
                            ))
                        })?;
                        self.reservation.grow(estimated_value_bytes(&value))?;
                        inner_params.push(value);
                    }
                    let context = ExecutionContext {
                        tables: &correlated.tables,
                        indexes: &correlated.indexes,
                        params: &inner_params,
                    };
                    let mut cursor = QueryExecutionCursor::new(
                        correlated.query.clone(),
                        &context,
                        nested_execution_options(&correlated.options, &memory)?,
                    )?;
                    let (collected, nested_peak) = collect_apply_rows(
                        &correlated.kind,
                        &mut cursor,
                        &memory,
                        &mut self.reservation,
                    )?;
                    correlated
                        .kind
                        .evaluate(collected, row, params, stack)
                        .map(|value| (value, nested_peak))
                })();
                let cleanup = self.reservation.resize(0);
                match (result, cleanup) {
                    (Err(error), _) => Err(error),
                    (Ok(_), Err(error)) => Err(error),
                    (Ok(value), Ok(())) => Ok(value),
                }
            }
        }
    }
}

impl DynamicApplyKind {
    fn compile(kind: ApplyExecutionKind, max_depth: usize) -> Result<Self> {
        match kind {
            ApplyExecutionKind::Scalar => Ok(Self::Scalar),
            ApplyExecutionKind::Exists { negated } => Ok(Self::Exists { negated }),
            ApplyExecutionKind::In { left, negated } => Ok(Self::Quantified {
                left: Box::new(ExpressionProgram::compile_with_limit(
                    &left, false, max_depth,
                )?),
                op: BinaryOperator::Eq,
                quantifier: SubqueryQuantifier::Any,
                negated,
            }),
            ApplyExecutionKind::Quantified {
                left,
                op,
                quantifier,
            } => Ok(Self::Quantified {
                left: Box::new(ExpressionProgram::compile_with_limit(
                    &left, false, max_depth,
                )?),
                op,
                quantifier,
                negated: false,
            }),
            ApplyExecutionKind::RowScalar {
                left,
                op,
                operand_types,
            } => Ok(Self::RowScalar {
                left: left
                    .iter()
                    .map(|expression| {
                        ExpressionProgram::compile_with_limit(expression, false, max_depth)
                    })
                    .collect::<Result<Vec<_>>>()?,
                op,
                operand_types,
            }),
            ApplyExecutionKind::RowQuantified {
                left,
                op,
                quantifier,
                negated,
                operand_types,
            } => Ok(Self::RowQuantified {
                left: left
                    .iter()
                    .map(|expression| {
                        ExpressionProgram::compile_with_limit(expression, false, max_depth)
                    })
                    .collect::<Result<Vec<_>>>()?,
                op,
                quantifier,
                negated,
                operand_types,
            }),
        }
    }

    fn into_cached(self, collected: CollectedApplyRows) -> Result<CachedApplyKind> {
        match (self, collected) {
            (Self::Scalar, CollectedApplyRows::Scalar(value)) => Ok(CachedApplyKind::Scalar(value)),
            (Self::Exists { negated }, CollectedApplyRows::Exists(value)) => {
                Ok(CachedApplyKind::Exists(value != negated))
            }
            (
                Self::Quantified {
                    left,
                    op,
                    quantifier,
                    negated,
                },
                CollectedApplyRows::Candidates(candidates),
            ) => Ok(CachedApplyKind::Quantified {
                left,
                op,
                quantifier,
                negated,
                candidates,
            }),
            (
                Self::RowScalar {
                    left,
                    op,
                    operand_types,
                },
                CollectedApplyRows::ScalarRow(candidate),
            ) => Ok(CachedApplyKind::RowScalar {
                left,
                op,
                operand_types,
                candidate,
            }),
            (
                Self::RowQuantified {
                    left,
                    op,
                    quantifier,
                    negated,
                    operand_types,
                },
                CollectedApplyRows::CandidateRows(candidates),
            ) => Ok(CachedApplyKind::RowQuantified {
                left,
                op,
                quantifier,
                negated,
                operand_types,
                candidates,
            }),
            _ => Err(DbError::internal(
                "Apply result shape does not match its execution kind",
            )),
        }
    }

    fn evaluate(
        &self,
        collected: CollectedApplyRows,
        row: &[Value],
        params: &[Value],
        stack: &mut ExpressionStack,
    ) -> Result<Value> {
        match (self, collected) {
            (Self::Scalar, CollectedApplyRows::Scalar(value)) => Ok(value),
            (Self::Exists { negated }, CollectedApplyRows::Exists(value)) => {
                Ok(Value::Boolean(value != *negated))
            }
            (
                Self::Quantified {
                    left,
                    op,
                    quantifier,
                    negated,
                },
                CollectedApplyRows::Candidates(candidates),
            ) => {
                let left = left.evaluate_reusing(row, params, stack)?;
                evaluate_apply_quantifier(left, *op, *quantifier, *negated, &candidates)
            }
            (
                Self::RowScalar {
                    left,
                    op,
                    operand_types,
                },
                CollectedApplyRows::ScalarRow(candidate),
            ) => evaluate_row_scalar(
                left,
                *op,
                operand_types,
                candidate.as_ref(),
                row,
                params,
                stack,
            ),
            (
                Self::RowQuantified {
                    left,
                    op,
                    quantifier,
                    negated,
                    operand_types,
                },
                CollectedApplyRows::CandidateRows(candidates),
            ) => evaluate_row_apply_quantifier(
                left,
                *op,
                *quantifier,
                *negated,
                operand_types,
                &candidates,
                row,
                params,
                stack,
            ),
            _ => Err(DbError::internal(
                "Apply result shape does not match its execution kind",
            )),
        }
    }
}

fn evaluate_cached_apply(
    kind: &CachedApplyKind,
    row: &[Value],
    params: &[Value],
    stack: &mut ExpressionStack,
) -> Result<Value> {
    match kind {
        CachedApplyKind::Scalar(value) => Ok(value.clone()),
        CachedApplyKind::Exists(value) => Ok(Value::Boolean(*value)),
        CachedApplyKind::Quantified {
            left,
            op,
            quantifier,
            negated,
            candidates,
        } => {
            let left = left.evaluate_reusing(row, params, stack)?;
            evaluate_apply_quantifier(left, *op, *quantifier, *negated, candidates)
        }
        CachedApplyKind::RowScalar {
            left,
            op,
            operand_types,
            candidate,
        } => evaluate_row_scalar(
            left,
            *op,
            operand_types,
            candidate.as_ref(),
            row,
            params,
            stack,
        ),
        CachedApplyKind::RowQuantified {
            left,
            op,
            quantifier,
            negated,
            operand_types,
            candidates,
        } => evaluate_row_apply_quantifier(
            left,
            *op,
            *quantifier,
            *negated,
            operand_types,
            candidates,
            row,
            params,
            stack,
        ),
    }
}

fn aggregate_window_projection(plan: &AdvancedExecutionPlan) -> Result<Option<Vec<usize>>> {
    if !plan.aggregate || plan.windows.is_empty() {
        return Ok(None);
    }
    let mut base_count = 0_usize;
    let mut ordinals = Vec::with_capacity(plan.projection.len());
    for projection in &plan.projection {
        let window = match &projection.expr.kind {
            BoundExprKind::ApplyValue { index } => plan
                .windows
                .iter()
                .position(|window| window.value_index == *index),
            _ => None,
        };
        if let Some(window) = window {
            ordinals.push(Some(window));
        } else {
            ordinals.push(None);
            base_count = base_count
                .checked_add(1)
                .ok_or_else(|| DbError::new("54001", "grouped window projection overflowed"))?;
        }
    }
    let mut next_base = 0_usize;
    let projection = ordinals
        .into_iter()
        .map(|window| {
            if let Some(window) = window {
                base_count
                    .checked_add(window)
                    .ok_or_else(|| DbError::new("54001", "grouped window projection overflowed"))
            } else {
                let index = next_base;
                next_base = next_base.saturating_add(1);
                Ok(index)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(projection))
}

fn nested_execution_options(
    options: &ExecutionOptions,
    memory: &QueryMemoryContext,
) -> Result<ExecutionOptions> {
    let remaining = options
        .hard_memory_bytes
        .checked_sub(memory.current_bytes())
        .filter(|remaining| *remaining > 0)
        .ok_or_else(|| DbError::new("53200", "query memory limit exceeded"))?;
    let remaining_depth = options
        .max_plan_depth
        .checked_sub(1)
        .filter(|remaining| *remaining > 0)
        .ok_or_else(|| program_limit_error("nested query plan depth limit exceeded"))?;
    let mut nested = options.clone();
    nested.hard_memory_bytes = remaining;
    nested.soft_memory_bytes = nested.soft_memory_bytes.min(remaining);
    nested.max_plan_depth = remaining_depth;
    Ok(nested)
}

fn collect_apply_rows(
    kind: &DynamicApplyKind,
    cursor: &mut QueryExecutionCursor,
    memory: &QueryMemoryContext,
    reservation: &mut Reservation,
) -> Result<(CollectedApplyRows, usize)> {
    let mut first = None;
    let mut first_row = None;
    let mut candidates = Vec::new();
    let mut candidate_rows = Vec::new();
    let exists = matches!(kind, DynamicApplyKind::Exists { .. });
    let mut combined_peak = combined_apply_memory(memory, cursor);
    while let Some(batch) = cursor.next_batch()? {
        combined_peak = combined_peak.max(combined_apply_memory(memory, cursor));
        for row in batch.rows {
            if exists {
                return Ok((CollectedApplyRows::Exists(true), combined_peak));
            }
            match kind {
                DynamicApplyKind::Scalar => {
                    let [value] = row.values.as_slice() else {
                        return Err(DbError::internal(
                            "scalar Apply subquery row width does not match its bound schema",
                        ));
                    };
                    if first.is_some() {
                        return Err(DbError::new(
                            "21000",
                            "more than one row returned by a subquery used as an expression",
                        ));
                    }
                    reserve_nested_apply_value(memory, cursor, reservation, value)?;
                    combined_peak = combined_peak.max(combined_apply_memory(memory, cursor));
                    first = Some(value.clone());
                }
                DynamicApplyKind::Quantified { .. } => {
                    let [value] = row.values.as_slice() else {
                        return Err(DbError::internal(
                            "quantified Apply subquery row width does not match its bound schema",
                        ));
                    };
                    reserve_nested_apply_value(memory, cursor, reservation, value)?;
                    combined_peak = combined_peak.max(combined_apply_memory(memory, cursor));
                    candidates.push(value.clone());
                }
                DynamicApplyKind::RowScalar { .. } => {
                    if first_row.is_some() {
                        return Err(DbError::new(
                            "21000",
                            "more than one row returned by a subquery used as an expression",
                        ));
                    }
                    reserve_nested_apply_row(memory, cursor, reservation, &row)?;
                    combined_peak = combined_peak.max(combined_apply_memory(memory, cursor));
                    first_row = Some(row);
                }
                DynamicApplyKind::RowQuantified { .. } => {
                    reserve_nested_apply_row(memory, cursor, reservation, &row)?;
                    combined_peak = combined_peak.max(combined_apply_memory(memory, cursor));
                    candidate_rows.push(row);
                }
                DynamicApplyKind::Exists { .. } => unreachable!("handled above"),
            }
        }
    }
    match kind {
        DynamicApplyKind::Scalar => Ok((
            CollectedApplyRows::Scalar(first.unwrap_or(Value::Null)),
            combined_peak,
        )),
        DynamicApplyKind::Exists { .. } => Ok((CollectedApplyRows::Exists(false), combined_peak)),
        DynamicApplyKind::Quantified { .. } => {
            Ok((CollectedApplyRows::Candidates(candidates), combined_peak))
        }
        DynamicApplyKind::RowScalar { .. } => {
            Ok((CollectedApplyRows::ScalarRow(first_row), combined_peak))
        }
        DynamicApplyKind::RowQuantified { .. } => Ok((
            CollectedApplyRows::CandidateRows(candidate_rows),
            combined_peak,
        )),
    }
}

fn combined_apply_memory(memory: &QueryMemoryContext, cursor: &QueryExecutionCursor) -> usize {
    let outer = memory.current_bytes();
    outer
        .saturating_add(cursor.memory_current_bytes())
        .max(outer.saturating_add(cursor.memory_peak_bytes()))
}

fn reserve_nested_apply_value(
    memory: &QueryMemoryContext,
    cursor: &QueryExecutionCursor,
    reservation: &mut Reservation,
    value: &Value,
) -> Result<()> {
    reserve_nested_apply_bytes(memory, cursor, reservation, estimated_value_bytes(value))
}

fn reserve_nested_apply_row(
    memory: &QueryMemoryContext,
    cursor: &QueryExecutionCursor,
    reservation: &mut Reservation,
    row: &Row,
) -> Result<()> {
    reserve_nested_apply_bytes(memory, cursor, reservation, estimated_row_bytes(row))
}

fn reserve_nested_apply_bytes(
    memory: &QueryMemoryContext,
    cursor: &QueryExecutionCursor,
    reservation: &mut Reservation,
    bytes: usize,
) -> Result<()> {
    let combined = memory
        .current_bytes()
        .checked_add(cursor.memory_current_bytes())
        .and_then(|current| current.checked_add(bytes))
        .ok_or_else(|| DbError::new("53200", "query memory limit exceeded"))?;
    if combined > memory.hard_limit_bytes() {
        return Err(DbError::new("53200", "query memory limit exceeded")
            .with_detail("correlated Apply exceeded the shared query memory grant"));
    }
    reservation.grow(bytes)
}

fn evaluate_apply_quantifier(
    left: Value,
    op: BinaryOperator,
    quantifier: SubqueryQuantifier,
    negated: bool,
    candidates: &[Value],
) -> Result<Value> {
    let result = evaluate_quantified_subquery(left, op, quantifier, candidates)?;
    if negated {
        match result {
            Value::Boolean(value) => Ok(Value::Boolean(!value)),
            Value::Null => Ok(Value::Null),
            _ => Err(DbError::internal(
                "quantified Apply produced a non-boolean value",
            )),
        }
    } else {
        Ok(result)
    }
}

fn evaluate_quantified_subquery(
    left: Value,
    op: BinaryOperator,
    quantifier: SubqueryQuantifier,
    candidates: &[Value],
) -> Result<Value> {
    let mut saw_null = false;
    for candidate in candidates {
        match evaluate_binary(left.clone(), op, candidate.clone())? {
            Value::Boolean(true) if quantifier == SubqueryQuantifier::Any => {
                return Ok(Value::Boolean(true));
            }
            Value::Boolean(false) if quantifier == SubqueryQuantifier::All => {
                return Ok(Value::Boolean(false));
            }
            Value::Boolean(_) => {}
            Value::Null => saw_null = true,
            _ => {
                return Err(DbError::internal(
                    "quantified comparison produced a non-boolean value",
                ));
            }
        }
    }
    if saw_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Boolean(quantifier == SubqueryQuantifier::All))
    }
}

fn evaluate_row_scalar(
    left: &[ExpressionProgram],
    op: BinaryOperator,
    operand_types: &[ScalarType],
    candidate: Option<&Row>,
    row: &[Value],
    params: &[Value],
    stack: &mut ExpressionStack,
) -> Result<Value> {
    let Some(candidate) = candidate else {
        return Ok(Value::Null);
    };
    let left = evaluate_row_programs(left, row, params, stack)?;
    evaluate_row_comparison(&left, op, &candidate.values, operand_types)
}

#[allow(clippy::too_many_arguments)]
fn evaluate_row_apply_quantifier(
    left: &[ExpressionProgram],
    op: BinaryOperator,
    quantifier: SubqueryQuantifier,
    negated: bool,
    operand_types: &[ScalarType],
    candidates: &[Row],
    row: &[Value],
    params: &[Value],
    stack: &mut ExpressionStack,
) -> Result<Value> {
    let left = evaluate_row_programs(left, row, params, stack)?;
    let mut saw_null = false;
    for candidate in candidates {
        match evaluate_row_comparison(&left, op, &candidate.values, operand_types)? {
            Value::Boolean(true) if quantifier == SubqueryQuantifier::Any => {
                return Ok(Value::Boolean(!negated));
            }
            Value::Boolean(false) if quantifier == SubqueryQuantifier::All => {
                return Ok(Value::Boolean(negated));
            }
            Value::Boolean(_) => {}
            Value::Null => saw_null = true,
            _ => {
                return Err(DbError::internal(
                    "row quantified comparison produced a non-boolean value",
                ));
            }
        }
    }
    if saw_null {
        Ok(Value::Null)
    } else {
        let value = quantifier == SubqueryQuantifier::All;
        Ok(Value::Boolean(if negated { !value } else { value }))
    }
}

fn evaluate_row_programs(
    programs: &[ExpressionProgram],
    row: &[Value],
    params: &[Value],
    stack: &mut ExpressionStack,
) -> Result<Vec<Value>> {
    programs
        .iter()
        .map(|program| program.evaluate_reusing(row, params, stack))
        .collect()
}

fn evaluate_row_comparison(
    left: &[Value],
    op: BinaryOperator,
    right: &[Value],
    operand_types: &[ScalarType],
) -> Result<Value> {
    if left.len() != right.len() || left.len() != operand_types.len() {
        return Err(DbError::internal(
            "row comparison width does not match its bound schema",
        ));
    }
    if !matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq) {
        return Err(DbError::internal(
            "ordered row comparison reached the equality-only executor",
        ));
    }
    let mut saw_null = false;
    for ((left, right), operand_type) in left.iter().zip(right).zip(operand_types) {
        let left = super::coerce_value(left.clone(), operand_type)?;
        let right = super::coerce_value(right.clone(), operand_type)?;
        match evaluate_binary(left, BinaryOperator::Eq, right)? {
            Value::Boolean(false) => {
                return Ok(Value::Boolean(op == BinaryOperator::NotEq));
            }
            Value::Boolean(true) => {}
            Value::Null => saw_null = true,
            _ => {
                return Err(DbError::internal(
                    "row equality produced a non-boolean value",
                ));
            }
        }
    }
    if saw_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Boolean(op == BinaryOperator::Eq))
    }
}

impl AdvancedExecutionCursor {
    pub fn new(
        plan: AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
    ) -> Result<AdvancedExecutionCursor> {
        Self::with_options_and_cancellation(plan, context, ExecutionOptions::default(), None)
    }

    pub fn new_with_cancellation(
        plan: AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Result<AdvancedExecutionCursor> {
        Self::with_options_and_cancellation(
            plan,
            context,
            ExecutionOptions::default(),
            cancellation,
        )
    }

    pub fn with_options(
        plan: AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
        options: ExecutionOptions,
    ) -> Result<AdvancedExecutionCursor> {
        Self::with_options_and_cancellation(plan, context, options, None)
    }

    fn with_options_and_cancellation(
        plan: AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
        options: ExecutionOptions,
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Result<AdvancedExecutionCursor> {
        options.validate()?;
        let plan_depth = plan
            .joins
            .len()
            .saturating_add(plan.applies.len())
            .saturating_add(plan.windows.len())
            .saturating_add(1);
        if plan_depth > options.max_plan_depth {
            return Err(program_limit_error(format!(
                "advanced plan exceeds the depth limit of {}",
                options.max_plan_depth
            )));
        }
        let source = JoinedSource::new(&plan, context, &options)?;
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
        let aggregate_window_projection = aggregate_window_projection(&plan)?;
        let windows = plan
            .windows
            .iter()
            .map(|window| WindowProgram::compile(window, options.max_expression_depth))
            .collect::<Result<Vec<_>>>()?;
        let limit = plan
            .limit
            .as_ref()
            .map(|limit| {
                ExpressionProgram::compile_with_limit(limit, false, options.max_expression_depth)?
                    .evaluate(&[], context.params)
                    .and_then(limit_from_value)
            })
            .transpose()?
            .flatten();
        let offset_remaining = plan
            .offset
            .as_ref()
            .map(|offset| {
                ExpressionProgram::compile_with_limit(offset, false, options.max_expression_depth)?
                    .evaluate(&[], context.params)
                    .and_then(offset_from_value)
            })
            .transpose()?
            .unwrap_or(0);
        let memory = QueryMemoryContext::new(options.soft_memory_bytes, options.hard_memory_bytes)?;
        let mut nested_memory_peak = 0_usize;
        let applies = plan
            .applies
            .into_iter()
            .map(|apply| {
                let (apply, peak) = ApplyRuntime::new(apply, context, &options, &memory)?;
                nested_memory_peak = nested_memory_peak.max(peak);
                Ok(apply)
            })
            .collect::<Result<Vec<_>>>()?;
        let expression_stack = ExpressionStack::new(&memory)?;
        let apply_row_reservation = memory.try_reserve(0)?;
        let distinct_reservation = memory.try_reserve(0)?;
        Ok(Self {
            source,
            schema: plan.schema,
            filter,
            projection,
            group_programs,
            order_by: plan.order_by,
            offset_remaining,
            limit,
            emitted: 0,
            params: context.params.to_vec(),
            memory,
            pool: BatchPool::new(options.batch_rows),
            spill: SpillManager::new(options.spill_root.clone()),
            expression_stack,
            applies,
            windows,
            aggregate_window_projection,
            cancellation,
            apply_row_reservation,
            nested_memory_peak,
            output: None,
            in_flight: None,
            distinct: plan.distinct,
            distinct_rows: HashSet::new(),
            distinct_reservation,
            aggregate: plan.aggregate,
            options,
            exhausted: false,
        })
    }

    #[must_use]
    pub const fn memory(&self) -> &QueryMemoryContext {
        &self.memory
    }

    #[must_use]
    pub fn memory_peak_bytes(&self) -> usize {
        self.memory.peak_bytes().max(self.nested_memory_peak)
    }

    pub fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.check_cancelled()?;
        self.in_flight = None;
        if self.exhausted {
            return Ok(None);
        }
        if self.aggregate && self.output.is_none() {
            self.initialize_aggregate()?;
        }
        if self.aggregate_window_projection.is_some() && !self.windows.is_empty() {
            self.initialize_aggregate_windowed_source()?;
        } else if !self.aggregate && !self.windows.is_empty() && self.output.is_none() {
            self.initialize_windowed_source()?;
        } else if !self.aggregate && !self.order_by.is_empty() && self.output.is_none() {
            self.initialize_sorted_source()?;
        }

        let mut rows = self.pool.take();
        let mut reservation = self.memory.try_reserve(0)?;
        while rows.len() < self.options.batch_rows {
            if self.limit.is_some_and(|limit| self.emitted >= limit) {
                break;
            }
            let Some(row) = self.next_output_row()? else {
                break;
            };
            if self.offset_remaining > 0 {
                self.offset_remaining -= 1;
                continue;
            }
            let bytes = estimated_row_bytes(&row);
            reservation.grow(bytes)?;
            rows.push(row);
            self.emitted = self.emitted.saturating_add(1);
        }
        if rows.is_empty() {
            self.exhausted = true;
            self.output = None;
            self.pool.recycle(rows);
            return Ok(None);
        }
        self.in_flight = Some(reservation);
        Ok(Some(Batch {
            schema: self.schema.clone(),
            rows,
        }))
    }

    fn next_output_row(&mut self) -> Result<Option<Row>> {
        loop {
            let row = if let Some(output) = &mut self.output {
                let Some(row) = output.next_row(&self.memory)? else {
                    return Ok(None);
                };
                if self.aggregate {
                    row
                } else {
                    self.project_row(row)?
                }
            } else {
                let Some(row) = self.next_filtered_source_row()? else {
                    return Ok(None);
                };
                self.project_row(row)?
            };
            if self.keep_distinct_row(&row)? {
                return Ok(Some(row));
            }
        }
    }

    fn keep_distinct_row(&mut self, row: &Row) -> Result<bool> {
        if !self.distinct {
            return Ok(true);
        }
        let key = DistinctRowKey(row.values.iter().map(distinct_value_key).collect());
        if self.distinct_rows.contains(&key) {
            return Ok(false);
        }
        self.distinct_reservation
            .grow(estimated_distinct_row_key_bytes(&key))?;
        self.distinct_rows.insert(key);
        Ok(true)
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
            self.check_cancelled()?;
            let row = self.source.next_row(
                &self.params,
                &mut self.memory,
                &mut self.spill,
                &mut self.expression_stack,
            )?;
            self.nested_memory_peak = self
                .nested_memory_peak
                .max(self.source.nested_memory_peak());
            let Some(mut row) = row else {
                return Ok(None);
            };
            self.apply_row_reservation.resize(0)?;
            for apply in &mut self.applies {
                let (value, nested_peak) =
                    apply.evaluate(&row.values, &self.params, &mut self.expression_stack)?;
                self.nested_memory_peak = self.nested_memory_peak.max(nested_peak);
                self.apply_row_reservation
                    .grow(estimated_value_bytes(&value))?;
                row.values.push(value);
            }
            if self.matches_filter(&row)? {
                return Ok(Some(row));
            }
        }
    }

    fn initialize_sorted_source(&mut self) -> Result<()> {
        let mut builder = RowsOutputBuilder::new(
            &self.order_by,
            &self.memory,
            self.options.max_expression_depth,
        )?;
        while let Some(row) = self.next_filtered_source_row()? {
            builder.push(
                row,
                &self.params,
                &mut self.expression_stack,
                &self.memory,
                &mut self.spill,
            )?;
        }
        self.output = Some(builder.finish(&self.memory, &mut self.spill)?);
        Ok(())
    }

    fn initialize_windowed_source(&mut self) -> Result<()> {
        let windows = std::mem::take(&mut self.windows);
        let mut builder = WindowRowStoreBuilder::new(&self.memory)?;
        while let Some(row) = self.next_filtered_source_row()? {
            self.check_cancelled()?;
            builder.push(row, &self.memory, &mut self.spill)?;
        }
        let mut rows = builder.finish(&self.memory)?;
        for window in &windows {
            rows = window.apply(
                rows,
                &self.params,
                &mut self.expression_stack,
                &self.memory,
                &mut self.spill,
                self.cancellation.as_deref(),
            )?;
        }
        self.install_window_output(rows)
    }

    fn initialize_aggregate_windowed_source(&mut self) -> Result<()> {
        let output = self
            .output
            .take()
            .ok_or_else(|| DbError::internal("grouped window input is unavailable"))?;
        let mut rows = output.into_window_store(&self.memory, &mut self.spill)?;
        let windows = std::mem::take(&mut self.windows);
        for window in &windows {
            rows = window.apply(
                rows,
                &self.params,
                &mut self.expression_stack,
                &self.memory,
                &mut self.spill,
                self.cancellation.as_deref(),
            )?;
        }
        let projection = self
            .aggregate_window_projection
            .take()
            .ok_or_else(|| DbError::internal("grouped window projection is unavailable"))?;
        let mut projected = WindowRowStoreBuilder::new(&self.memory)?;
        let row_count = rows.len();
        for index in 0..row_count {
            self.check_cancelled()?;
            let ReservedRow {
                mut row,
                mut reservation,
            } = rows.read(index, &self.memory)?;
            let mut values = std::mem::take(&mut row.values)
                .into_iter()
                .map(Some)
                .collect::<Vec<_>>();
            row.values = projection
                .iter()
                .map(|index| {
                    values
                        .get_mut(*index)
                        .and_then(Option::take)
                        .ok_or_else(|| {
                            DbError::internal(
                                "grouped window projection index is out of bounds or duplicated",
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            reservation.resize(estimated_row_bytes(&row))?;
            projected.push_transferred(row, &mut reservation, &self.memory, &mut self.spill)?;
        }
        let rows = projected.finish(&self.memory)?;
        self.install_window_output(rows)
    }

    fn install_window_output(&mut self, rows: WindowRowStore) -> Result<()> {
        if self.order_by.is_empty() {
            self.output = Some(match rows {
                WindowRowStore::Memory { rows, reservation } => RowsOutput::Memory {
                    rows,
                    offset: 0,
                    reservation: Some(reservation),
                },
                WindowRowStore::Spill(store) => RowsOutput::Indexed {
                    store,
                    offset: 0,
                    current_reservation: None,
                },
            });
            return Ok(());
        }
        let mut builder = RowsOutputBuilder::new(
            &self.order_by,
            &self.memory,
            self.options.max_expression_depth,
        )?;
        match rows {
            WindowRowStore::Memory {
                rows,
                mut reservation,
            } => {
                for row in rows {
                    builder.push_transferred(
                        row,
                        &self.params,
                        &mut self.expression_stack,
                        &self.memory,
                        &mut self.spill,
                        &mut reservation,
                    )?;
                }
                if reservation.bytes() != 0 {
                    return Err(DbError::internal(
                        "window row reservation was not fully transferred",
                    ));
                }
            }
            WindowRowStore::Spill(mut store) => {
                for index in 0..store.len {
                    self.check_cancelled()?;
                    let ReservedRow {
                        row,
                        mut reservation,
                    } = store.read(index, &self.memory)?;
                    builder.push_transferred(
                        row,
                        &self.params,
                        &mut self.expression_stack,
                        &self.memory,
                        &mut self.spill,
                        &mut reservation,
                    )?;
                }
            }
        }
        self.output = Some(builder.finish(&self.memory, &mut self.spill)?);
        Ok(())
    }

    fn check_cancelled(&self) -> Result<()> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(|cancellation| cancellation.load(AtomicOrdering::Acquire))
        {
            Err(DbError::new("57014", "query was cancelled"))
        } else {
            Ok(())
        }
    }

    fn initialize_aggregate(&mut self) -> Result<()> {
        let programs = self
            .group_programs
            .take()
            .ok_or_else(|| DbError::internal("aggregate programs are unavailable"))?;
        let mut groups = Vec::<GroupAccumulator>::new();
        let mut group_reservation = self.memory.try_reserve(0)?;
        let mut spill_paths = None;
        let unjoined_rows = if self.applies.is_empty()
            && self.filter.is_none()
            && programs.group_by.is_empty()
            && programs.aggregate_specs.iter().all(|spec| !spec.distinct)
        {
            self.source.take_unjoined_rows()
        } else {
            None
        };
        if let Some(rows) = unjoined_rows {
            if let Some((first, remaining)) = rows.split_first() {
                let mut group = GroupAccumulator::new(
                    Vec::new(),
                    first.clone(),
                    0,
                    &programs.aggregate_specs,
                    &self.params,
                    &mut self.expression_stack,
                )?;
                for row in remaining {
                    group.update(
                        &programs.aggregate_specs,
                        row,
                        &self.params,
                        &mut self.expression_stack,
                    )?;
                }
                group_reservation.grow(group.estimated_bytes())?;
                groups.push(group);
            } else {
                let group = GroupAccumulator::empty(&programs.aggregate_specs);
                group_reservation.grow(group.estimated_bytes())?;
                groups.push(group);
            }
        } else {
            let mut ordinal = 0_u64;
            while let Some(row) = self.next_filtered_source_row()? {
                let key = programs.group_key(&row, &self.params, &mut self.expression_stack)?;
                if let Some(group) = groups.iter_mut().find(|group| group.key == key) {
                    let before = group.estimated_bytes();
                    group.update(
                        &programs.aggregate_specs,
                        &row,
                        &self.params,
                        &mut self.expression_stack,
                    )?;
                    let after = group.estimated_bytes();
                    if after > before {
                        group_reservation.grow(after - before)?;
                    } else if before > after {
                        group_reservation
                            .resize(group_reservation.bytes().saturating_sub(before - after))?;
                    }
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
                        let paths = spill_paths.as_ref().ok_or_else(|| {
                            DbError::internal("aggregate spill paths disappeared")
                        })?;
                        self.spill
                            .write_group_partials(paths, &groups, &self.memory)?;
                        groups.clear();
                        group_reservation.resize(0)?;
                    }
                    group_reservation.grow(bytes)?;
                    groups.push(group);
                }
                if self.memory.current_bytes() > self.memory.soft_limit_bytes() {
                    if spill_paths.is_none() {
                        spill_paths =
                            Some(self.spill.partition_paths("aggregate", HASH_PARTITIONS)?);
                    }
                    let paths = spill_paths
                        .as_ref()
                        .ok_or_else(|| DbError::internal("aggregate spill paths disappeared"))?;
                    self.spill
                        .write_group_partials(paths, &groups, &self.memory)?;
                    groups.clear();
                    group_reservation.resize(0)?;
                }
                ordinal = ordinal.saturating_add(1);
            }
        }

        if programs.group_by.is_empty() && groups.is_empty() {
            let group = GroupAccumulator::empty(&programs.aggregate_specs);
            let bytes = group.estimated_bytes();
            group_reservation.grow(bytes)?;
            groups.push(group);
        }

        if let Some(paths) = &spill_paths
            && !groups.is_empty()
        {
            self.spill
                .write_group_partials(paths, &groups, &self.memory)?;
            groups.clear();
            group_reservation.resize(0)?;
        }

        let aggregate_order_by = if self.aggregate_window_projection.is_some() {
            &[][..]
        } else {
            self.order_by.as_slice()
        };
        let mut output = RowsOutputBuilder::new(
            aggregate_order_by,
            &self.memory,
            self.options.max_expression_depth,
        )?;
        if let Some(paths) = spill_paths {
            for path in paths {
                if !path.exists() {
                    continue;
                }
                let partition_groups = self.spill.read_and_merge_groups(
                    &path,
                    &self.memory,
                    &programs.aggregate_specs,
                )?;
                for group in partition_groups.values {
                    if let Some(row) =
                        programs.project_group(&group, &self.params, &mut self.expression_stack)?
                    {
                        output.push(
                            row,
                            &self.params,
                            &mut self.expression_stack,
                            &self.memory,
                            &mut self.spill,
                        )?;
                    }
                }
            }
        } else {
            for group in groups {
                if let Some(row) =
                    programs.project_group(&group, &self.params, &mut self.expression_stack)?
                {
                    output.push(
                        row,
                        &self.params,
                        &mut self.expression_stack,
                        &self.memory,
                        &mut self.spill,
                    )?;
                }
            }
        }
        drop(group_reservation);
        self.output = Some(output.finish(&self.memory, &mut self.spill)?);
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
    nested_memory_peak: usize,
}

enum FastJoinStep {
    Row(Row),
    Exhausted,
    Fallback,
}

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

impl WindowResultWriter {
    fn new(spill: &mut SpillManager, len: usize, memory: &QueryMemoryContext) -> Result<Self> {
        let requested = len
            .checked_mul(std::mem::size_of::<Option<Value>>())
            .ok_or_else(|| DbError::new("53200", "window result slots are out of range"))?;
        if memory.would_cross_soft_limit(requested) {
            return IndexedResultWriter::new(spill, len, memory).map(Self::Spill);
        }
        let mut reservation = memory.try_reserve(requested)?;
        let mut values = Vec::new();
        if let Err(error) = values.try_reserve_exact(len) {
            return Err(DbError::new("53200", "query memory limit exceeded")
                .with_detail(format!("failed to allocate window result slots: {error}")));
        }
        let actual = values
            .capacity()
            .checked_mul(std::mem::size_of::<Option<Value>>())
            .ok_or_else(|| DbError::new("53200", "window result slots are out of range"))?;
        reservation.resize(actual)?;
        values.resize_with(len, || None);
        Ok(Self::Memory {
            values,
            reservation,
        })
    }

    fn push_at(
        &mut self,
        source_index: usize,
        result: Value,
        spill: &mut SpillManager,
        memory: &QueryMemoryContext,
    ) -> Result<()> {
        let result_bytes = estimated_value_bytes(&result);
        let should_spill = match self {
            Self::Memory { values, .. } => {
                let slot = values.get(source_index).ok_or_else(|| {
                    DbError::internal("window result source index is out of bounds")
                })?;
                if slot.is_some() {
                    return Err(DbError::internal(
                        "window result was written more than once",
                    ));
                }
                memory.would_cross_soft_limit(result_bytes)
            }
            Self::Spill(_) => false,
        };
        if should_spill {
            let len = match self {
                Self::Memory { values, .. } => values.len(),
                Self::Spill(_) => unreachable!("spill transition checked above"),
            };
            let mut writer = IndexedResultWriter::new(spill, len, memory)?;
            if let Self::Memory { values, .. } = self {
                for (index, value) in values.iter_mut().enumerate() {
                    if let Some(value) = value.take() {
                        writer.push_at(index, value, memory)?;
                    }
                }
            }
            *self = Self::Spill(writer);
        }
        match self {
            Self::Memory {
                values,
                reservation,
            } => {
                reservation.grow(result_bytes)?;
                values[source_index] = Some(result);
                Ok(())
            }
            Self::Spill(writer) => writer.push_at(source_index, result, memory),
        }
    }

    fn finish(self, memory: &QueryMemoryContext) -> Result<WindowResults> {
        match self {
            Self::Memory {
                values,
                reservation,
            } => {
                if values.iter().any(Option::is_none) {
                    return Err(DbError::internal("window result index is incomplete"));
                }
                Ok(WindowResults::Memory {
                    values,
                    reservation,
                })
            }
            Self::Spill(writer) => writer.finish(memory).map(WindowResults::Spill),
        }
    }
}

enum WindowResults {
    Memory {
        values: Vec<Option<Value>>,
        reservation: Reservation,
    },
    Spill(IndexedRowStore),
}

impl WindowResults {
    fn take(&mut self, index: usize, memory: &QueryMemoryContext) -> Result<ReservedValue> {
        match self {
            Self::Memory {
                values,
                reservation,
            } => {
                let value = values
                    .get_mut(index)
                    .and_then(Option::take)
                    .ok_or_else(|| DbError::internal("window result is missing"))?;
                let bytes = estimated_value_bytes(&value);
                let mut value_reservation = memory.try_reserve(0)?;
                reservation.transfer_to(&mut value_reservation, bytes)?;
                Ok(ReservedValue {
                    value,
                    reservation: value_reservation,
                })
            }
            Self::Spill(store) => {
                let mut result = store.read(index, memory)?;
                if result.row.values.len() != 1 {
                    return Err(DbError::new(
                        "XX001",
                        "window result spill row has an invalid width",
                    ));
                }
                let value = result
                    .row
                    .values
                    .pop()
                    .ok_or_else(|| DbError::new("XX001", "window result spill row is empty"))?;
                Ok(ReservedValue {
                    value,
                    reservation: result.reservation,
                })
            }
        }
    }
}

struct IndexedRowStore {
    reader: ReservedSpillReader,
    index: File,
    len: usize,
}

impl IndexedRowStore {
    fn open(
        data_path: PathBuf,
        index_path: PathBuf,
        len: usize,
        memory: &QueryMemoryContext,
    ) -> Result<Self> {
        Ok(Self {
            reader: open_spill_reader(&data_path, memory)?,
            index: File::open(index_path).map_err(spill_io_error)?,
            len,
        })
    }

    fn read(&mut self, index: usize, memory: &QueryMemoryContext) -> Result<ReservedRow> {
        if index >= self.len {
            return Err(DbError::internal("window spill row index is out of bounds"));
        }
        let index_position = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_mul(WINDOW_SPILL_INDEX_BYTES))
            .ok_or_else(|| DbError::new("53200", "window spill index is out of range"))?;
        self.index
            .seek(SeekFrom::Start(index_position))
            .map_err(spill_io_error)?;
        let mut offset = [0_u8; std::mem::size_of::<u64>()];
        self.index.read_exact(&mut offset).map_err(|error| {
            DbError::new("XX001", "window spill index is truncated").with_detail(error.to_string())
        })?;
        let offset = u64::from_le_bytes(offset);
        if offset == 0 {
            return Err(DbError::new(
                "XX001",
                "window spill index contains an empty entry",
            ));
        }
        self.reader
            .seek(SeekFrom::Start(offset))
            .map_err(spill_io_error)?;
        let record = read_spill_record::<Row>(&mut self.reader, memory)?
            .ok_or_else(|| DbError::new("XX001", "window spill row is missing"))?;
        let reservation = memory.try_reserve(estimated_row_bytes(&record.value))?;
        Ok(ReservedRow {
            row: record.value,
            reservation,
        })
    }
}

enum WindowRowStore {
    Memory {
        rows: Vec<Row>,
        reservation: Reservation,
    },
    Spill(IndexedRowStore),
}

impl WindowRowStore {
    fn len(&self) -> usize {
        match self {
            Self::Memory { rows, .. } => rows.len(),
            Self::Spill(store) => store.len,
        }
    }

    fn read(&mut self, index: usize, memory: &QueryMemoryContext) -> Result<ReservedRow> {
        match self {
            Self::Memory { rows, .. } => {
                let row = rows
                    .get(index)
                    .ok_or_else(|| DbError::internal("window memory row index is out of bounds"))?;
                let reservation = memory.try_reserve(estimated_row_bytes(row))?;
                Ok(ReservedRow {
                    row: row.clone(),
                    reservation,
                })
            }
            Self::Spill(store) => store.read(index, memory),
        }
    }
}

struct WindowRowStoreBuilder {
    rows: Vec<Row>,
    reservation: Reservation,
    writer: Option<IndexedRowStoreWriter>,
}

impl WindowRowStoreBuilder {
    fn new(memory: &QueryMemoryContext) -> Result<Self> {
        Ok(Self {
            rows: Vec::new(),
            reservation: memory.try_reserve(0)?,
            writer: None,
        })
    }

    fn push(
        &mut self,
        row: Row,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<()> {
        let mut source_reservation = memory.try_reserve(estimated_row_bytes(&row))?;
        self.push_transferred(row, &mut source_reservation, memory, spill)?;
        if source_reservation.bytes() != 0 {
            return Err(DbError::internal(
                "window input reservation was not fully transferred",
            ));
        }
        Ok(())
    }

    fn push_transferred(
        &mut self,
        row: Row,
        source_reservation: &mut Reservation,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<()> {
        let bytes = estimated_row_bytes(&row);
        if source_reservation.bytes() < bytes {
            return Err(DbError::internal(
                "window source reservation is smaller than its row",
            ));
        }
        if self.writer.is_none() && memory.current_bytes() > memory.soft_limit_bytes() {
            let mut writer = IndexedRowStoreWriter::new(spill, memory)?;
            for existing in &self.rows {
                writer.push(existing, memory)?;
            }
            self.rows.clear();
            self.reservation.resize(0)?;
            self.writer = Some(writer);
        }
        if let Some(writer) = &mut self.writer {
            writer.push(&row, memory)?;
            source_reservation.resize(source_reservation.bytes().saturating_sub(bytes))?;
        } else {
            source_reservation.transfer_to(&mut self.reservation, bytes)?;
            self.rows.push(row);
        }
        Ok(())
    }

    fn finish(self, memory: &QueryMemoryContext) -> Result<WindowRowStore> {
        if let Some(writer) = self.writer {
            writer.finish(memory).map(WindowRowStore::Spill)
        } else {
            Ok(WindowRowStore::Memory {
                rows: self.rows,
                reservation: self.reservation,
            })
        }
    }
}

struct WindowProgram {
    function: WindowFunction,
    arguments: Vec<ExpressionProgram>,
    filter: Option<ExpressionProgram>,
    partition_by: Vec<ExpressionProgram>,
    order_by: Vec<BoundOrder>,
    order_programs: Vec<Option<ExpressionProgram>>,
    frame: Option<WindowFrameProgram>,
}

struct WindowRowStores<'a> {
    keyed: &'a mut WindowRowStore,
    source: &'a mut WindowRowStore,
}

struct WindowFrameProgram {
    units: WindowFrameUnits,
    start_bound: WindowFrameBoundProgram,
    end_bound: WindowFrameBoundProgram,
}

enum WindowFrameBoundProgram {
    UnboundedPreceding,
    Preceding(ExpressionProgram),
    CurrentRow,
    Following(ExpressionProgram),
    UnboundedFollowing,
}

#[derive(Clone, Copy)]
enum AggregateWindowMode {
    WholePartition,
    RowsRunning,
    RangeRunning,
}

impl WindowProgram {
    fn compile(window: &BoundWindow, max_depth: usize) -> Result<Self> {
        let partition_by = window
            .partition_by
            .iter()
            .map(|expression| ExpressionProgram::compile_with_limit(expression, false, max_depth))
            .collect::<Result<Vec<_>>>()?;
        let arguments = window
            .arguments
            .iter()
            .map(|argument| ExpressionProgram::compile_with_limit(argument, false, max_depth))
            .collect::<Result<Vec<_>>>()?;
        let filter = window
            .filter
            .as_ref()
            .map(|filter| ExpressionProgram::compile_with_limit(filter, false, max_depth))
            .transpose()?;
        let order_programs = window
            .order_by
            .iter()
            .map(|order| {
                order
                    .expression
                    .as_ref()
                    .map(|expression| {
                        ExpressionProgram::compile_with_limit(expression, false, max_depth)
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        let frame = window
            .frame
            .as_ref()
            .map(|frame| WindowFrameProgram::compile(frame, max_depth))
            .transpose()?;
        Ok(Self {
            function: window.function,
            arguments,
            filter,
            partition_by,
            order_by: window.order_by.clone(),
            order_programs,
            frame,
        })
    }

    fn apply(
        &self,
        mut rows: WindowRowStore,
        params: &[Value],
        stack: &mut ExpressionStack,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
        cancellation: Option<&AtomicBool>,
    ) -> Result<WindowRowStore> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(rows);
        }
        let partition_count = self.partition_by.len();
        let order_count = self.order_by.len();
        let mut sort_orders = self
            .partition_by
            .iter()
            .enumerate()
            .map(|(column_index, program)| BoundOrder {
                column_index,
                expression: None,
                data_type: program.result_type().clone(),
                ascending: true,
                nulls_first: Some(true),
            })
            .collect::<Vec<_>>();
        sort_orders.extend(
            self.order_by
                .iter()
                .enumerate()
                .map(|(ordinal, order)| BoundOrder {
                    column_index: partition_count.saturating_add(ordinal),
                    expression: None,
                    data_type: order.data_type.clone(),
                    ascending: order.ascending,
                    nulls_first: order.nulls_first,
                }),
        );
        sort_orders.push(BoundOrder {
            column_index: partition_count.saturating_add(order_count),
            expression: None,
            data_type: ScalarType::Int64,
            ascending: true,
            nulls_first: Some(false),
        });

        let mut keyed_builder = RowsOutputBuilder::new(&sort_orders, memory, 1)?;
        for index in 0..row_count {
            ensure_window_not_cancelled(cancellation)?;
            let row = rows.read(index, memory)?;
            let mut values = self
                .partition_by
                .iter()
                .map(|program| program.evaluate_reusing(&row.row.values, params, stack))
                .collect::<Result<Vec<_>>>()?;
            for (order, program) in self.order_by.iter().zip(&self.order_programs) {
                let value = if let Some(program) = program {
                    program.evaluate_reusing(&row.row.values, params, stack)?
                } else {
                    row.row
                        .values
                        .get(order.column_index)
                        .cloned()
                        .ok_or_else(|| {
                            DbError::internal("window ORDER BY column index is out of bounds")
                        })?
                };
                values.push(value);
            }
            let index = i64::try_from(index)
                .map_err(|_| DbError::new("54001", "window row index is out of range"))?;
            values.push(Value::Int64(index));
            keyed_builder.push(Row::new(values), params, stack, memory, spill)?;
        }
        let mut keyed = keyed_builder
            .finish(memory, spill)?
            .into_window_store(memory, spill)?;

        let frame = self
            .frame
            .as_ref()
            .map(|frame| frame.evaluate(params, stack))
            .transpose()?
            .unwrap_or_else(|| EvaluatedWindowFrame::default_for_order(order_count));
        let mut state_reservation = memory.try_reserve(0)?;
        let mut results = WindowResultWriter::new(spill, row_count, memory)?;
        let mut partition_start = 0_usize;
        while partition_start < keyed.len() {
            ensure_window_not_cancelled(cancellation)?;
            let mut partition_end = partition_start.saturating_add(1);
            while partition_end < keyed.len() {
                let left = keyed.read(partition_start, memory)?;
                let right = keyed.read(partition_end, memory)?;
                if !window_key_slices_equal(
                    &left.row.values[..partition_count],
                    &right.row.values[..partition_count],
                )? {
                    break;
                }
                partition_end = partition_end.saturating_add(1);
            }

            if let WindowFunction::Aggregate(function) = self.function
                && self.apply_optimized_aggregate(
                    function,
                    &frame,
                    partition_start..partition_end,
                    partition_count,
                    order_count,
                    &mut keyed,
                    &mut rows,
                    params,
                    stack,
                    spill,
                    memory,
                    &mut state_reservation,
                    &mut results,
                    cancellation,
                )?
            {
                partition_start = partition_end;
                continue;
            }

            let mut peer_rank = 1_usize;
            let mut dense_rank = 1_usize;
            for index in partition_start..partition_end {
                ensure_window_not_cancelled(cancellation)?;
                let partition_position = index.saturating_sub(partition_start).saturating_add(1);
                if index > partition_start {
                    let previous = keyed.read(index - 1, memory)?;
                    let current = keyed.read(index, memory)?;
                    if !window_key_slices_equal(
                        &previous.row.values
                            [partition_count..partition_count.saturating_add(order_count)],
                        &current.row.values
                            [partition_count..partition_count.saturating_add(order_count)],
                    )? {
                        peer_rank = partition_position;
                        dense_rank = dense_rank.saturating_add(1);
                    }
                }
                state_reservation.resize(0)?;
                let result = self.evaluate_result(
                    index,
                    partition_start,
                    partition_end,
                    partition_position,
                    peer_rank,
                    dense_rank,
                    partition_count,
                    &mut keyed,
                    &mut rows,
                    params,
                    stack,
                    memory,
                    &frame,
                    &mut state_reservation,
                    cancellation,
                )?;
                append_window_result(index, result, &mut keyed, &mut results, spill, memory)?;
            }
            partition_start = partition_end;
        }

        let mut results = results.finish(memory)?;
        let mut next = WindowRowStoreBuilder::new(memory)?;
        for source_index in 0..row_count {
            ensure_window_not_cancelled(cancellation)?;
            let ReservedRow {
                mut row,
                mut reservation,
            } = rows.read(source_index, memory)?;
            let ReservedValue {
                value,
                reservation: _result_reservation,
            } = results.take(source_index, memory)?;
            reservation.grow(estimated_value_bytes(&value))?;
            row.values.push(value);
            next.push_transferred(row, &mut reservation, memory, spill)?;
        }
        next.finish(memory)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_optimized_aggregate(
        &self,
        function: AggregateFunction,
        frame: &EvaluatedWindowFrame,
        partition: std::ops::Range<usize>,
        partition_count: usize,
        order_count: usize,
        keyed: &mut WindowRowStore,
        rows: &mut WindowRowStore,
        params: &[Value],
        stack: &mut ExpressionStack,
        spill: &mut SpillManager,
        memory: &QueryMemoryContext,
        state_reservation: &mut Reservation,
        results: &mut WindowResultWriter,
        cancellation: Option<&AtomicBool>,
    ) -> Result<bool> {
        let Some(mode) = aggregate_window_mode(frame) else {
            return Ok(false);
        };
        state_reservation.resize(0)?;
        let spec = AggregateSpec {
            function,
            argument: self.arguments.first().cloned(),
            distinct: false,
            filter: self.filter.clone(),
            source: None,
            source_filter: None,
        };
        let mut state = AggregateState::new(&spec);
        match mode {
            AggregateWindowMode::WholePartition => {
                for target in partition.clone() {
                    update_window_aggregate(
                        &mut state,
                        &spec,
                        target,
                        keyed,
                        rows,
                        params,
                        stack,
                        memory,
                        state_reservation,
                        cancellation,
                    )?;
                }
                let result = state.value(&spec)?;
                for index in partition {
                    ensure_window_not_cancelled(cancellation)?;
                    append_window_result(index, result.clone(), keyed, results, spill, memory)?;
                }
            }
            AggregateWindowMode::RowsRunning => {
                for index in partition {
                    update_window_aggregate(
                        &mut state,
                        &spec,
                        index,
                        keyed,
                        rows,
                        params,
                        stack,
                        memory,
                        state_reservation,
                        cancellation,
                    )?;
                    append_window_result(
                        index,
                        state.value(&spec)?,
                        keyed,
                        results,
                        spill,
                        memory,
                    )?;
                }
            }
            AggregateWindowMode::RangeRunning => {
                let order_start = partition_count;
                let order_end = partition_count.saturating_add(order_count);
                let mut peer_start = partition.start;
                while peer_start < partition.end {
                    ensure_window_not_cancelled(cancellation)?;
                    let mut peer_end = peer_start.saturating_add(1);
                    while peer_end < partition.end {
                        let left = keyed.read(peer_start, memory)?;
                        let right = keyed.read(peer_end, memory)?;
                        if !window_key_slices_equal(
                            &left.row.values[order_start..order_end],
                            &right.row.values[order_start..order_end],
                        )? {
                            break;
                        }
                        peer_end = peer_end.saturating_add(1);
                    }
                    for target in peer_start..peer_end {
                        update_window_aggregate(
                            &mut state,
                            &spec,
                            target,
                            keyed,
                            rows,
                            params,
                            stack,
                            memory,
                            state_reservation,
                            cancellation,
                        )?;
                    }
                    let result = state.value(&spec)?;
                    for index in peer_start..peer_end {
                        append_window_result(index, result.clone(), keyed, results, spill, memory)?;
                    }
                    peer_start = peer_end;
                }
            }
        }
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_result(
        &self,
        index: usize,
        partition_start: usize,
        partition_end: usize,
        partition_position: usize,
        peer_rank: usize,
        dense_rank: usize,
        partition_count: usize,
        keyed: &mut WindowRowStore,
        rows: &mut WindowRowStore,
        params: &[Value],
        stack: &mut ExpressionStack,
        memory: &QueryMemoryContext,
        frame: &EvaluatedWindowFrame,
        state_reservation: &mut Reservation,
        cancellation: Option<&AtomicBool>,
    ) -> Result<Value> {
        match self.function {
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                let value = match self.function {
                    WindowFunction::RowNumber => partition_position,
                    WindowFunction::Rank => peer_rank,
                    WindowFunction::DenseRank => dense_rank,
                    _ => unreachable!("ranking match guarded above"),
                };
                i64::try_from(value)
                    .map(Value::Int64)
                    .map_err(|_| DbError::new("22003", "window rank is out of range"))
            }
            WindowFunction::Lag | WindowFunction::Lead => self.evaluate_offset_value(
                index,
                partition_start..partition_end,
                WindowRowStores {
                    keyed,
                    source: rows,
                },
                params,
                stack,
                memory,
            ),
            WindowFunction::FirstValue | WindowFunction::LastValue | WindowFunction::NthValue => {
                let Some((frame_start, frame_end)) = window_frame_range(
                    frame,
                    index,
                    partition_start..partition_end,
                    partition_count,
                    &self.order_by,
                    keyed,
                    memory,
                )?
                else {
                    return Ok(Value::Null);
                };
                let target = match self.function {
                    WindowFunction::FirstValue => Some(frame_start),
                    WindowFunction::LastValue => Some(frame_end),
                    WindowFunction::NthValue => {
                        let current = window_source_row(index, keyed, rows, memory)?;
                        let nth = self.arguments.get(1).ok_or_else(|| {
                            DbError::internal("NTH_VALUE ordinal program is unavailable")
                        })?;
                        let nth = positive_window_offset(
                            nth.evaluate_reusing(&current.row.values, params, stack)?,
                            "NTH_VALUE argument",
                            false,
                        )?;
                        frame_start
                            .checked_add(nth.saturating_sub(1))
                            .filter(|target| *target <= frame_end)
                    }
                    _ => unreachable!("value match guarded above"),
                };
                let Some(target) = target else {
                    return Ok(Value::Null);
                };
                self.arguments
                    .first()
                    .ok_or_else(|| DbError::internal("window value argument is unavailable"))?
                    .evaluate_reusing(
                        &window_source_row(target, keyed, rows, memory)?.row.values,
                        params,
                        stack,
                    )
            }
            WindowFunction::Aggregate(function) => {
                let spec = AggregateSpec {
                    function,
                    argument: self.arguments.first().cloned(),
                    distinct: false,
                    filter: self.filter.clone(),
                    source: None,
                    source_filter: None,
                };
                let mut state = AggregateState::new(&spec);
                if let Some((frame_start, frame_end)) = window_frame_range(
                    frame,
                    index,
                    partition_start..partition_end,
                    partition_count,
                    &self.order_by,
                    keyed,
                    memory,
                )? {
                    for target in frame_start..=frame_end {
                        ensure_window_not_cancelled(cancellation)?;
                        let source = window_source_row(target, keyed, rows, memory)?;
                        state.update(&spec, &source.row, params, stack)?;
                        state_reservation.resize(state.estimated_bytes())?;
                    }
                }
                state_reservation.resize(state.estimated_bytes())?;
                state.value(&spec)
            }
        }
    }

    fn evaluate_offset_value(
        &self,
        index: usize,
        partition: std::ops::Range<usize>,
        stores: WindowRowStores<'_>,
        params: &[Value],
        stack: &mut ExpressionStack,
        memory: &QueryMemoryContext,
    ) -> Result<Value> {
        let current = window_source_row(index, stores.keyed, stores.source, memory)?;
        let offset = self
            .arguments
            .get(1)
            .map(|offset| offset.evaluate_reusing(&current.row.values, params, stack))
            .transpose()?
            .map_or(Ok(Some(1_i128)), |offset| {
                signed_window_offset(offset, "window offset")
            })?;
        let Some(offset) = offset else {
            return Ok(Value::Null);
        };
        let current_index = i128::try_from(index)
            .map_err(|_| DbError::new("54001", "window row index is out of range"))?;
        let target = match self.function {
            WindowFunction::Lag => current_index.checked_sub(offset),
            WindowFunction::Lead => current_index.checked_add(offset),
            _ => unreachable!("offset match guarded by caller"),
        }
        .and_then(|target| usize::try_from(target).ok())
        .filter(|target| partition.contains(target));
        if let Some(target) = target {
            return self
                .arguments
                .first()
                .ok_or_else(|| DbError::internal("offset window value is unavailable"))?
                .evaluate_reusing(
                    &window_source_row(target, stores.keyed, stores.source, memory)?
                        .row
                        .values,
                    params,
                    stack,
                );
        }
        self.arguments
            .get(2)
            .map(|default| default.evaluate_reusing(&current.row.values, params, stack))
            .transpose()
            .map(|default| default.unwrap_or(Value::Null))
    }
}

impl WindowFrameProgram {
    fn compile(frame: &BoundWindowFrame, max_depth: usize) -> Result<Self> {
        Ok(Self {
            units: frame.units,
            start_bound: WindowFrameBoundProgram::compile(&frame.start_bound, max_depth)?,
            end_bound: WindowFrameBoundProgram::compile(&frame.end_bound, max_depth)?,
        })
    }

    fn evaluate(
        &self,
        params: &[Value],
        stack: &mut ExpressionStack,
    ) -> Result<EvaluatedWindowFrame> {
        let start = self.start_bound.evaluate(params, stack, self.units)?;
        let end = self.end_bound.evaluate(params, stack, self.units)?;
        match (&start, &end) {
            (
                EvaluatedWindowFrameBound::Preceding(start),
                EvaluatedWindowFrameBound::Preceding(end),
            ) if compare_values(start, end)? == Ordering::Less => Err(DbError::new(
                "42P20",
                "frame starting offset must not follow the ending offset",
            )),
            (
                EvaluatedWindowFrameBound::Following(start),
                EvaluatedWindowFrameBound::Following(end),
            ) if compare_values(start, end)? == Ordering::Greater => Err(DbError::new(
                "42P20",
                "frame starting offset must not follow the ending offset",
            )),
            _ => Ok(EvaluatedWindowFrame {
                units: self.units,
                start_bound: start,
                end_bound: end,
            }),
        }
    }
}

struct EvaluatedWindowFrame {
    units: WindowFrameUnits,
    start_bound: EvaluatedWindowFrameBound,
    end_bound: EvaluatedWindowFrameBound,
}

enum EvaluatedWindowFrameBound {
    UnboundedPreceding,
    Preceding(Value),
    CurrentRow,
    Following(Value),
    UnboundedFollowing,
}

impl EvaluatedWindowFrame {
    fn default_for_order(order_count: usize) -> Self {
        Self {
            units: WindowFrameUnits::Range,
            start_bound: EvaluatedWindowFrameBound::UnboundedPreceding,
            end_bound: if order_count == 0 {
                EvaluatedWindowFrameBound::UnboundedFollowing
            } else {
                EvaluatedWindowFrameBound::CurrentRow
            },
        }
    }
}

impl WindowFrameBoundProgram {
    fn compile(bound: &BoundWindowFrameBound, max_depth: usize) -> Result<Self> {
        Ok(match bound {
            BoundWindowFrameBound::UnboundedPreceding => Self::UnboundedPreceding,
            BoundWindowFrameBound::Preceding(offset) => Self::Preceding(
                ExpressionProgram::compile_with_limit(offset, false, max_depth)?,
            ),
            BoundWindowFrameBound::CurrentRow => Self::CurrentRow,
            BoundWindowFrameBound::Following(offset) => Self::Following(
                ExpressionProgram::compile_with_limit(offset, false, max_depth)?,
            ),
            BoundWindowFrameBound::UnboundedFollowing => Self::UnboundedFollowing,
        })
    }

    fn evaluate(
        &self,
        params: &[Value],
        stack: &mut ExpressionStack,
        units: WindowFrameUnits,
    ) -> Result<EvaluatedWindowFrameBound> {
        let program = match self {
            Self::Preceding(program) | Self::Following(program) => program,
            Self::UnboundedPreceding => {
                return Ok(EvaluatedWindowFrameBound::UnboundedPreceding);
            }
            Self::CurrentRow => return Ok(EvaluatedWindowFrameBound::CurrentRow),
            Self::UnboundedFollowing => {
                return Ok(EvaluatedWindowFrameBound::UnboundedFollowing);
            }
        };
        let value = program.evaluate_reusing(&[], params, stack)?;
        let valid = match &value {
            Value::Int16(value) => *value >= 0,
            Value::Int32(value) => *value >= 0,
            Value::Int64(value) => *value >= 0,
            Value::Float32(value) => value.is_finite() && *value >= 0.0,
            Value::Float64(value) => value.is_finite() && *value >= 0.0,
            Value::Decimal(value) => !value.is_sign_negative(),
            Value::Null => false,
            _ => false,
        };
        if !valid {
            let unit = match units {
                WindowFrameUnits::Rows => "ROWS",
                WindowFrameUnits::Range => "RANGE",
            };
            return Err(DbError::new(
                "22013",
                format!("{unit} frame offset must not be negative or null"),
            ));
        }
        Ok(match self {
            Self::Preceding(_) => EvaluatedWindowFrameBound::Preceding(value),
            Self::Following(_) => EvaluatedWindowFrameBound::Following(value),
            Self::UnboundedPreceding | Self::CurrentRow | Self::UnboundedFollowing => {
                unreachable!("non-offset frame bound returned above")
            }
        })
    }
}

fn window_source_index(key: &Row) -> Result<usize> {
    match key.values.last() {
        Some(Value::Int64(index)) => {
            usize::try_from(*index).map_err(|_| DbError::internal("window row index is invalid"))
        }
        _ => Err(DbError::internal("window row index is missing")),
    }
}

fn ensure_window_not_cancelled(cancellation: Option<&AtomicBool>) -> Result<()> {
    if cancellation.is_some_and(|cancellation| cancellation.load(AtomicOrdering::Acquire)) {
        Err(DbError::new("57014", "query was cancelled"))
    } else {
        Ok(())
    }
}

fn aggregate_window_mode(frame: &EvaluatedWindowFrame) -> Option<AggregateWindowMode> {
    match (&frame.start_bound, &frame.end_bound) {
        (
            EvaluatedWindowFrameBound::UnboundedPreceding,
            EvaluatedWindowFrameBound::UnboundedFollowing,
        ) => Some(AggregateWindowMode::WholePartition),
        (EvaluatedWindowFrameBound::UnboundedPreceding, EvaluatedWindowFrameBound::CurrentRow)
            if frame.units == WindowFrameUnits::Rows =>
        {
            Some(AggregateWindowMode::RowsRunning)
        }
        (EvaluatedWindowFrameBound::UnboundedPreceding, EvaluatedWindowFrameBound::CurrentRow)
            if frame.units == WindowFrameUnits::Range =>
        {
            Some(AggregateWindowMode::RangeRunning)
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn update_window_aggregate(
    state: &mut AggregateState,
    spec: &AggregateSpec,
    index: usize,
    keyed: &mut WindowRowStore,
    rows: &mut WindowRowStore,
    params: &[Value],
    stack: &mut ExpressionStack,
    memory: &QueryMemoryContext,
    state_reservation: &mut Reservation,
    cancellation: Option<&AtomicBool>,
) -> Result<()> {
    ensure_window_not_cancelled(cancellation)?;
    let source = window_source_row(index, keyed, rows, memory)?;
    state.update(spec, &source.row, params, stack)?;
    state_reservation.resize(state.estimated_bytes())
}

fn append_window_result(
    index: usize,
    result: Value,
    keyed: &mut WindowRowStore,
    results: &mut WindowResultWriter,
    spill: &mut SpillManager,
    memory: &QueryMemoryContext,
) -> Result<()> {
    let key = keyed.read(index, memory)?;
    let source_index = window_source_index(&key.row)?;
    results.push_at(source_index, result, spill, memory)
}

fn window_source_row(
    index: usize,
    keyed: &mut WindowRowStore,
    rows: &mut WindowRowStore,
    memory: &QueryMemoryContext,
) -> Result<ReservedRow> {
    let key = keyed.read(index, memory)?;
    let source_index = window_source_index(&key.row)?;
    rows.read(source_index, memory)
}

fn positive_window_offset(value: Value, label: &str, zero_allowed: bool) -> Result<usize> {
    let value = match value {
        Value::Int16(value) => i64::from(value),
        Value::Int32(value) => i64::from(value),
        Value::Int64(value) => value,
        Value::Null => {
            return Err(DbError::new("22013", format!("{label} must not be null")));
        }
        _ => return Err(DbError::new("42804", format!("{label} must be an integer"))),
    };
    if value < 0 || (!zero_allowed && value == 0) {
        return Err(DbError::new(
            "22013",
            format!(
                "{label} must be {}",
                if zero_allowed {
                    "nonnegative"
                } else {
                    "positive"
                }
            ),
        ));
    }
    usize::try_from(value).map_err(|_| DbError::new("22003", format!("{label} is out of range")))
}

fn signed_window_offset(value: Value, label: &str) -> Result<Option<i128>> {
    match value {
        Value::Int16(value) => Ok(Some(i128::from(value))),
        Value::Int32(value) => Ok(Some(i128::from(value))),
        Value::Int64(value) => Ok(Some(i128::from(value))),
        Value::Null => Ok(None),
        _ => Err(DbError::new("42804", format!("{label} must be an integer"))),
    }
}

fn window_frame_range(
    frame: &EvaluatedWindowFrame,
    index: usize,
    partition: std::ops::Range<usize>,
    partition_count: usize,
    order_by: &[BoundOrder],
    keyed: &mut WindowRowStore,
    memory: &QueryMemoryContext,
) -> Result<Option<(usize, usize)>> {
    let start = window_frame_bound_index(
        &frame.start_bound,
        true,
        frame.units,
        index,
        partition.start,
        partition.end,
        partition_count,
        order_by,
        keyed,
        memory,
    )?;
    let end = window_frame_bound_index(
        &frame.end_bound,
        false,
        frame.units,
        index,
        partition.start,
        partition.end,
        partition_count,
        order_by,
        keyed,
        memory,
    )?;
    Ok(match (start, end) {
        (Some(start), Some(end)) if start <= end => Some((start, end)),
        _ => None,
    })
}

#[allow(clippy::too_many_arguments)]
fn window_frame_bound_index(
    bound: &EvaluatedWindowFrameBound,
    is_start: bool,
    units: WindowFrameUnits,
    index: usize,
    partition_start: usize,
    partition_end: usize,
    partition_count: usize,
    order_by: &[BoundOrder],
    keyed: &mut WindowRowStore,
    memory: &QueryMemoryContext,
) -> Result<Option<usize>> {
    if units == WindowFrameUnits::Rows {
        let current = i128::try_from(index)
            .map_err(|_| DbError::new("54001", "window row index is out of range"))?;
        let position = match bound {
            EvaluatedWindowFrameBound::UnboundedPreceding => {
                i128::try_from(partition_start).unwrap_or(i128::MAX)
            }
            EvaluatedWindowFrameBound::Preceding(offset) => current.saturating_sub(
                i128::try_from(positive_window_offset(
                    offset.clone(),
                    "ROWS frame offset",
                    true,
                )?)
                .unwrap_or(i128::MAX),
            ),
            EvaluatedWindowFrameBound::CurrentRow => current,
            EvaluatedWindowFrameBound::Following(offset) => current.saturating_add(
                i128::try_from(positive_window_offset(
                    offset.clone(),
                    "ROWS frame offset",
                    true,
                )?)
                .unwrap_or(i128::MAX),
            ),
            EvaluatedWindowFrameBound::UnboundedFollowing => {
                i128::try_from(partition_end.saturating_sub(1)).unwrap_or(i128::MAX)
            }
        };
        let first = i128::try_from(partition_start).unwrap_or(i128::MAX);
        let last = i128::try_from(partition_end.saturating_sub(1)).unwrap_or(i128::MAX);
        let position = if is_start {
            if position > last {
                return Ok(None);
            }
            position.max(first)
        } else {
            if position < first {
                return Ok(None);
            }
            position.min(last)
        };
        return usize::try_from(position)
            .map(Some)
            .map_err(|_| DbError::new("54001", "window frame index is out of range"));
    }

    match bound {
        EvaluatedWindowFrameBound::UnboundedPreceding => Ok(Some(partition_start)),
        EvaluatedWindowFrameBound::UnboundedFollowing => Ok(Some(partition_end.saturating_sub(1))),
        EvaluatedWindowFrameBound::CurrentRow => peer_boundary(
            index,
            partition_start..partition_end,
            partition_count,
            order_by.len(),
            keyed,
            memory,
            is_start,
        )
        .map(Some),
        EvaluatedWindowFrameBound::Preceding(offset)
        | EvaluatedWindowFrameBound::Following(offset) => {
            let [order] = order_by else {
                return Err(DbError::internal(
                    "RANGE offset reached execution without one ORDER BY expression",
                ));
            };
            let current = keyed
                .read(index, memory)?
                .row
                .values
                .get(partition_count)
                .cloned()
                .ok_or_else(|| DbError::internal("RANGE ORDER BY value is unavailable"))?;
            if current.is_null() {
                return peer_boundary(
                    index,
                    partition_start..partition_end,
                    partition_count,
                    1,
                    keyed,
                    memory,
                    is_start,
                )
                .map(Some);
            }
            let subtract =
                matches!(bound, EvaluatedWindowFrameBound::Preceding(_)) == order.ascending;
            let threshold = evaluate_binary(
                current,
                if subtract {
                    BinaryOperator::Subtract
                } else {
                    BinaryOperator::Add
                },
                offset.clone(),
            )?;
            range_threshold_boundary(
                &threshold,
                order,
                partition_start..partition_end,
                partition_count,
                keyed,
                memory,
                is_start,
            )
        }
    }
}

fn peer_boundary(
    index: usize,
    partition: std::ops::Range<usize>,
    partition_count: usize,
    order_count: usize,
    keyed: &mut WindowRowStore,
    memory: &QueryMemoryContext,
    is_start: bool,
) -> Result<usize> {
    if order_count == 0 {
        return Ok(if is_start {
            partition.start
        } else {
            partition.end.saturating_sub(1)
        });
    }
    let current = keyed.read(index, memory)?;
    let current = &current.row.values[partition_count..partition_count.saturating_add(order_count)];
    if is_start {
        let mut candidate = index;
        while candidate > partition.start {
            let previous = keyed.read(candidate - 1, memory)?;
            if !window_key_slices_equal(
                &previous.row.values[partition_count..partition_count.saturating_add(order_count)],
                current,
            )? {
                break;
            }
            candidate -= 1;
        }
        Ok(candidate)
    } else {
        let mut candidate = index;
        while candidate.saturating_add(1) < partition.end {
            let next = keyed.read(candidate + 1, memory)?;
            if !window_key_slices_equal(
                &next.row.values[partition_count..partition_count.saturating_add(order_count)],
                current,
            )? {
                break;
            }
            candidate += 1;
        }
        Ok(candidate)
    }
}

fn range_threshold_boundary(
    threshold: &Value,
    order: &BoundOrder,
    partition: std::ops::Range<usize>,
    partition_count: usize,
    keyed: &mut WindowRowStore,
    memory: &QueryMemoryContext,
    is_start: bool,
) -> Result<Option<usize>> {
    let compare_order = BoundOrder {
        column_index: 0,
        expression: None,
        data_type: order.data_type.clone(),
        ascending: order.ascending,
        nulls_first: order.nulls_first,
    };
    let threshold = Row::new(vec![threshold.clone()]);
    if is_start {
        for index in partition.clone() {
            let key = keyed.read(index, memory)?;
            let candidate = Row::new(vec![key.row.values[partition_count].clone()]);
            if compare_rows(&candidate, &threshold, std::slice::from_ref(&compare_order))?
                != Ordering::Less
            {
                return Ok(Some(index));
            }
        }
    } else {
        for index in partition.rev() {
            let key = keyed.read(index, memory)?;
            let candidate = Row::new(vec![key.row.values[partition_count].clone()]);
            if compare_rows(&candidate, &threshold, std::slice::from_ref(&compare_order))?
                != Ordering::Greater
            {
                return Ok(Some(index));
            }
        }
    }
    Ok(None)
}

fn window_key_slices_equal(left: &[Value], right: &[Value]) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        let equal = if left.is_null() || right.is_null() {
            left.is_null() && right.is_null()
        } else {
            compare_values(left, right)? == std::cmp::Ordering::Equal
        };
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

impl CandidateSet {
    fn next(&mut self, rows: &[Row], _memory: &QueryMemoryContext) -> Result<Option<Row>> {
        match self {
            Self::Empty => Ok(None),
            Self::One { value } => Ok(value.take().and_then(|index| rows.get(index).cloned())),
            Self::All { offset, len } => {
                if *offset >= *len {
                    return Ok(None);
                }
                let row = rows.get(*offset).cloned();
                *offset = offset.saturating_add(1);
                Ok(row)
            }
            Self::Indexes { values, offset, .. } => {
                let Some(index) = values.get(*offset).copied() else {
                    return Ok(None);
                };
                *offset = offset.saturating_add(1);
                Ok(rows.get(index).cloned())
            }
            Self::Rows { values, offset, .. } => {
                let row = values.get(*offset).cloned();
                *offset = offset.saturating_add(1);
                Ok(row)
            }
            Self::Cursor {
                cursor,
                batch,
                offset,
                ..
            } => loop {
                if let Some(current) = batch
                    && let Some(row) = current.rows.get(*offset).cloned()
                {
                    *offset = offset.saturating_add(1);
                    return Ok(Some(row));
                }
                *batch = cursor.next_batch()?;
                *offset = 0;
                if batch.is_none() {
                    return Ok(None);
                }
            },
        }
    }

    fn nested_memory_peak(&self, memory: &QueryMemoryContext) -> usize {
        match self {
            Self::Cursor { cursor, .. } => combined_apply_memory(memory, cursor),
            Self::Empty
            | Self::One { .. }
            | Self::All { .. }
            | Self::Indexes { .. }
            | Self::Rows { .. } => 0,
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
            .filter(|projection| {
                !matches!(
                    &projection.expr.kind,
                    BoundExprKind::ApplyValue { index }
                        if plan.windows.iter().any(|window| window.value_index == *index)
                )
            })
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

    fn group_key(
        &self,
        row: &Row,
        params: &[Value],
        stack: &mut ExpressionStack,
    ) -> Result<Vec<Value>> {
        self.group_by
            .iter()
            .map(|program| program.evaluate_reusing(&row.values, params, stack))
            .collect()
    }

    fn project_group(
        &self,
        group: &GroupAccumulator,
        params: &[Value],
        stack: &mut ExpressionStack,
    ) -> Result<Option<Row>> {
        let aggregate_values = group
            .aggregates
            .iter()
            .zip(&self.aggregate_specs)
            .map(|(state, spec)| state.value(spec))
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
    distinct: bool,
    filter: Option<ExpressionProgram>,
    source: Option<BoundExpr>,
    source_filter: Option<BoundExpr>,
}

#[derive(Debug, Clone)]
struct GroupProgram {
    instructions: Vec<GroupInstruction>,
    max_stack_slots: usize,
}

#[derive(Debug, Clone)]
enum GroupInstruction {
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
        dimensions: Vec<ordadb_types::ArrayDimension>,
    },
    Function {
        function: ScalarFunction,
        count: usize,
    },
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
                    BoundExprKind::Binary { left, op, .. } => {
                        instructions.push(GroupInstruction::Binary {
                            operator: *op,
                            operand_type: left.data_type.clone(),
                        });
                    }
                    BoundExprKind::InList {
                        expr,
                        list,
                        negated,
                    } => {
                        instructions.push(GroupInstruction::InList {
                            count: list.len(),
                            negated: *negated,
                            operand_type: expr.data_type.clone(),
                        });
                    }
                    BoundExprKind::Cast { .. } => {
                        instructions.push(GroupInstruction::Cast(expression.data_type.clone()));
                    }
                    BoundExprKind::Array {
                        elements,
                        dimensions,
                    } => {
                        let ScalarType::Array { element } = &expression.data_type else {
                            return Err(DbError::internal(
                                "array group expression lost its array result type",
                            ));
                        };
                        instructions.push(GroupInstruction::MakeArray {
                            count: elements.len(),
                            element_type: element.as_ref().clone(),
                            dimensions: dimensions.clone(),
                        });
                    }
                    BoundExprKind::Function {
                        function,
                        arguments,
                    } => {
                        instructions.push(GroupInstruction::Function {
                            function: *function,
                            count: arguments.len(),
                        });
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
                BoundExprKind::ApplyValue { index } => {
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
                BoundExprKind::Correlation { .. } => {
                    return Err(DbError::internal(
                        "correlated group expression reached execution without a parameter frame",
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
                    function,
                    argument,
                    distinct,
                    filter,
                } => {
                    let source = argument.as_deref().cloned();
                    let source_filter = filter.as_deref().cloned();
                    let existing = aggregate_specs.iter().position(|spec| {
                        spec.function == *function
                            && spec.distinct == *distinct
                            && spec.source == source
                            && spec.source_filter == source_filter
                    });
                    let slot = if let Some(existing) = existing {
                        existing
                    } else {
                        let argument = source
                            .as_ref()
                            .map(|argument| {
                                ExpressionProgram::compile_with_limit(argument, false, max_depth)
                            })
                            .transpose()?;
                        let filter = source_filter
                            .as_ref()
                            .map(|filter| {
                                ExpressionProgram::compile_with_limit(filter, false, max_depth)
                            })
                            .transpose()?;
                        aggregate_specs.push(AggregateSpec {
                            function: *function,
                            argument,
                            distinct: *distinct,
                            filter,
                            source: source.clone(),
                            source_filter: source_filter.clone(),
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
        let max_stack_slots = group_stack_slots(&instructions)?;
        Ok(Self {
            instructions,
            max_stack_slots,
        })
    }

    fn evaluate(
        &self,
        row: &[Value],
        params: &[Value],
        aggregates: &[Value],
        values: &mut ExpressionStack,
    ) -> Result<Value> {
        values.prepare(self.max_stack_slots)?;
        for instruction in &self.instructions {
            match instruction {
                GroupInstruction::LoadColumn(index) => {
                    values.push(row.get(*index).cloned().ok_or_else(|| {
                        DbError::internal("group column index is out of bounds")
                    })?)?;
                }
                GroupInstruction::LoadLiteral(value) => values.push(value.clone())?,
                GroupInstruction::LoadParameter(index) => {
                    values.push(params.get(index - 1).cloned().ok_or_else(|| {
                        DbError::new("42P02", format!("no value supplied for parameter ${index}"))
                    })?)?;
                }
                GroupInstruction::Unary(operator) => {
                    let value = values
                        .pop()
                        .ok_or_else(|| DbError::internal("group value stack underflow"))?;
                    values.push(evaluate_unary(*operator, value)?)?;
                }
                GroupInstruction::Binary {
                    operator,
                    operand_type,
                } => {
                    let right = values
                        .pop()
                        .ok_or_else(|| DbError::internal("group value stack underflow"))?;
                    let left = values
                        .pop()
                        .ok_or_else(|| DbError::internal("group value stack underflow"))?;
                    values.push(super::evaluate_binary_as(
                        left,
                        *operator,
                        right,
                        operand_type,
                    )?)?;
                }
                GroupInstruction::InList {
                    count,
                    negated,
                    operand_type,
                } => {
                    values.collapse_in_list(*count, *negated, operand_type)?;
                }
                GroupInstruction::Cast(target) => {
                    let value = values
                        .pop()
                        .ok_or_else(|| DbError::internal("group value stack underflow"))?;
                    values.push(super::cast_value(value, target)?)?;
                }
                GroupInstruction::MakeArray {
                    count,
                    element_type,
                    dimensions,
                } => {
                    let mut elements = Vec::with_capacity(*count);
                    for _ in 0..*count {
                        elements.push(values.pop().ok_or_else(|| {
                            DbError::internal("group array value stack underflow")
                        })?);
                    }
                    elements.reverse();
                    values.push(Value::Array(ordadb_types::PgArray::new(
                        element_type.clone(),
                        dimensions.clone(),
                        elements,
                    )?))?;
                }
                GroupInstruction::Function { function, count } => {
                    let mut arguments = Vec::with_capacity(*count);
                    for _ in 0..*count {
                        arguments.push(values.pop().ok_or_else(|| {
                            DbError::internal("group function value stack underflow")
                        })?);
                    }
                    arguments.reverse();
                    values.push(super::evaluate_scalar_function(*function, arguments)?)?;
                }
                GroupInstruction::AggregateValue(slot) => {
                    values.push(
                        aggregates
                            .get(*slot)
                            .cloned()
                            .ok_or_else(|| DbError::internal("aggregate slot is out of bounds"))?,
                    )?;
                }
                GroupInstruction::Coerce(target) => {
                    let value = values
                        .pop()
                        .ok_or_else(|| DbError::internal("group value stack underflow"))?;
                    values.push(super::coerce_value(value, target)?)?;
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

fn group_stack_slots(instructions: &[GroupInstruction]) -> Result<usize> {
    let mut depth = 0_usize;
    let mut maximum = 0_usize;
    for instruction in instructions {
        match instruction {
            GroupInstruction::LoadColumn(_)
            | GroupInstruction::LoadLiteral(_)
            | GroupInstruction::LoadParameter(_)
            | GroupInstruction::AggregateValue(_) => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| program_limit_error("group value stack depth overflowed"))?;
                maximum = maximum.max(depth);
            }
            GroupInstruction::Unary(_)
            | GroupInstruction::Cast(_)
            | GroupInstruction::Coerce(_) => {
                if depth == 0 {
                    return Err(DbError::internal(
                        "group expression compiler produced a stack underflow",
                    ));
                }
            }
            GroupInstruction::Binary { .. } => {
                if depth < 2 {
                    return Err(DbError::internal(
                        "group expression compiler produced a stack underflow",
                    ));
                }
                depth -= 1;
            }
            GroupInstruction::InList { count, .. } => {
                let required = count.saturating_add(1);
                if depth < required {
                    return Err(DbError::internal(
                        "group expression compiler produced an IN list stack underflow",
                    ));
                }
                depth -= *count;
            }
            GroupInstruction::MakeArray { count, .. } => {
                if *count == 0 {
                    depth = depth
                        .checked_add(1)
                        .ok_or_else(|| program_limit_error("group value stack depth overflowed"))?;
                    maximum = maximum.max(depth);
                } else {
                    if depth < *count {
                        return Err(DbError::internal(
                            "group expression compiler produced an array stack underflow",
                        ));
                    }
                    depth = depth - *count + 1;
                }
            }
            GroupInstruction::Function { count, .. } => {
                if *count == 0 || depth < *count {
                    return Err(DbError::internal(
                        "group expression compiler produced a function stack underflow",
                    ));
                }
                depth = depth - *count + 1;
            }
        }
    }
    if depth != 1 {
        return Err(DbError::internal(
            "group expression compiler did not produce one stack result",
        ));
    }
    Ok(maximum)
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
        stack: &mut ExpressionStack,
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
        stack: &mut ExpressionStack,
    ) -> Result<()> {
        for (state, spec) in self.aggregates.iter_mut().zip(specs) {
            state.update(spec, row, params, stack)?;
        }
        Ok(())
    }

    fn merge(&mut self, other: Self, specs: &[AggregateSpec]) -> Result<()> {
        if other.first_ordinal < self.first_ordinal {
            self.first_ordinal = other.first_ordinal;
            self.representative = other.representative.clone();
        }
        if self.aggregates.len() != specs.len() || other.aggregates.len() != specs.len() {
            return Err(DbError::internal("aggregate spill state width changed"));
        }
        for ((state, incoming), spec) in self.aggregates.iter_mut().zip(other.aggregates).zip(specs)
        {
            state.merge(incoming, spec)?;
        }
        Ok(())
    }

    fn estimated_bytes(&self) -> usize {
        estimated_row_bytes(&self.representative)
            .saturating_add(self.key.iter().map(estimated_value_bytes).sum::<usize>())
            .saturating_add(
                self.aggregates
                    .iter()
                    .map(AggregateState::estimated_bytes)
                    .sum::<usize>(),
            )
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
    Distinct(#[serde(with = "distinct_values_serde")] BTreeMap<DistinctValueKey, Value>),
}

impl AggregateState {
    fn new(spec: &AggregateSpec) -> Self {
        if spec.distinct {
            return Self::Distinct(BTreeMap::new());
        }
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
        stack: &mut ExpressionStack,
    ) -> Result<()> {
        if let Some(filter) = &spec.filter {
            match filter.evaluate_reusing(&row.values, params, stack)? {
                Value::Boolean(true) => {}
                Value::Boolean(false) | Value::Null => return Ok(()),
                _ => {
                    return Err(DbError::new(
                        "42804",
                        "aggregate FILTER predicate must be boolean",
                    ));
                }
            }
        }
        let value = spec
            .argument
            .as_ref()
            .map(|argument| argument.evaluate_reusing(&row.values, params, stack))
            .transpose()?;
        if let Self::Distinct(values) = self {
            let value = value
                .ok_or_else(|| DbError::internal("DISTINCT aggregate argument is unavailable"))?;
            if value.is_null() {
                return Ok(());
            }
            values.entry(distinct_value_key(&value)).or_insert(value);
            return Ok(());
        }
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
            Self::Min(selected) => select_value(
                selected,
                value,
                Ordering::Less,
                aggregate_argument_type(spec)?,
            )?,
            Self::Max(selected) => select_value(
                selected,
                value,
                Ordering::Greater,
                aggregate_argument_type(spec)?,
            )?,
            Self::Distinct(_) => unreachable!("DISTINCT aggregate handled before state update"),
        }
        Ok(())
    }

    fn merge(&mut self, incoming: Self, spec: &AggregateSpec) -> Result<()> {
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
                select_value(left, right, Ordering::Less, aggregate_argument_type(spec)?)?;
            }
            (Self::Max(left), Self::Max(right)) => {
                select_value(
                    left,
                    right,
                    Ordering::Greater,
                    aggregate_argument_type(spec)?,
                )?;
            }
            (Self::Distinct(left), Self::Distinct(right)) => {
                for (key, value) in right {
                    left.entry(key).or_insert(value);
                }
            }
            _ => return Err(DbError::internal("aggregate spill state kind changed")),
        }
        Ok(())
    }

    fn value(&self, spec: &AggregateSpec) -> Result<Value> {
        match self {
            Self::Count(count) => i64::try_from(*count)
                .map(Value::Int64)
                .map_err(|_| DbError::new("22003", "COUNT result is out of range")),
            Self::Sum(value) | Self::Min(value) | Self::Max(value) => {
                Ok(value.clone().unwrap_or(Value::Null))
            }
            Self::Avg { sum: _, count } if *count == 0 => Ok(Value::Null),
            Self::Avg { sum, count } => Ok(Value::Float64(*sum / *count as f64)),
            Self::Distinct(values) => distinct_aggregate_value(spec, values),
        }
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Distinct(values) => values.iter().fold(64_usize, |total, (key, value)| {
                total
                    .saturating_add(std::mem::size_of::<DistinctValueKey>())
                    .saturating_add(distinct_key_dynamic_bytes(key))
                    .saturating_add(estimated_value_bytes(value))
                    .saturating_add(std::mem::size_of::<usize>().saturating_mul(3))
            }),
            _ => 64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
enum DistinctValueKey {
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
    Array {
        element_type: String,
        dimensions: Vec<(u32, i32)>,
        values: Vec<DistinctValueKey>,
    },
    Json(String),
    Jsonb(String),
    Uuid([u8; 16]),
    Vector(Vec<u32>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DistinctRowKey(Vec<DistinctValueKey>);

mod distinct_values_serde {
    use super::{BTreeMap, DistinctValueKey, Value};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        values: &BTreeMap<DistinctValueKey, Value>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        values.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<DistinctValueKey, Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<(DistinctValueKey, Value)>::deserialize(deserializer)
            .map(|entries| entries.into_iter().collect())
    }
}

fn distinct_value_key(value: &Value) -> DistinctValueKey {
    match value {
        Value::Null => DistinctValueKey::Null,
        Value::Boolean(value) => DistinctValueKey::Boolean(*value),
        Value::Int16(value) => DistinctValueKey::Int16(*value),
        Value::Int32(value) => DistinctValueKey::Int32(*value),
        Value::Int64(value) => DistinctValueKey::Int64(*value),
        Value::Float32(value) => DistinctValueKey::Float32(canonical_f32_bits(*value)),
        Value::Float64(value) => DistinctValueKey::Float64(canonical_f64_bits(*value)),
        Value::Decimal(value) => DistinctValueKey::Decimal(value.normalize().to_string()),
        Value::Text(value) => DistinctValueKey::Text(value.clone()),
        Value::Binary(value) => DistinctValueKey::Binary(value.clone()),
        Value::Date(value) => DistinctValueKey::Date(value.to_string()),
        Value::Time(value) => DistinctValueKey::Time(value.to_string()),
        Value::Timestamp(value) => DistinctValueKey::Timestamp(value.to_string()),
        Value::Interval(value) => {
            DistinctValueKey::Interval(value.months, value.days, value.microseconds)
        }
        Value::Array(value) => DistinctValueKey::Array {
            element_type: format!("{:?}", value.element_type()),
            dimensions: value
                .dimensions()
                .iter()
                .map(|dimension| (dimension.length, dimension.lower_bound))
                .collect(),
            values: value.values().iter().map(distinct_value_key).collect(),
        },
        Value::Json(value) => DistinctValueKey::Json(value.to_string()),
        Value::Jsonb(value) => DistinctValueKey::Jsonb(value.to_string()),
        Value::Uuid(value) => DistinctValueKey::Uuid(*value.as_bytes()),
        Value::Vector(values) => DistinctValueKey::Vector(
            values
                .iter()
                .map(|value| canonical_f32_bits(*value))
                .collect(),
        ),
    }
}

fn canonical_f32_bits(value: f32) -> u32 {
    if value == 0.0 {
        0.0_f32.to_bits()
    } else if value.is_nan() {
        f32::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

fn distinct_key_dynamic_bytes(key: &DistinctValueKey) -> usize {
    match key {
        DistinctValueKey::Decimal(value)
        | DistinctValueKey::Text(value)
        | DistinctValueKey::Date(value)
        | DistinctValueKey::Time(value)
        | DistinctValueKey::Timestamp(value)
        | DistinctValueKey::Json(value)
        | DistinctValueKey::Jsonb(value) => value.len(),
        DistinctValueKey::Array {
            element_type,
            dimensions,
            values,
        } => element_type
            .len()
            .saturating_add(
                dimensions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u32, i32)>()),
            )
            .saturating_add(
                values
                    .iter()
                    .map(|value| {
                        std::mem::size_of::<DistinctValueKey>()
                            .saturating_add(distinct_key_dynamic_bytes(value))
                    })
                    .sum::<usize>(),
            ),
        DistinctValueKey::Binary(value) => value.len(),
        DistinctValueKey::Vector(value) => value.len().saturating_mul(std::mem::size_of::<u32>()),
        DistinctValueKey::Boolean(_)
        | DistinctValueKey::Null
        | DistinctValueKey::Int16(_)
        | DistinctValueKey::Int32(_)
        | DistinctValueKey::Int64(_)
        | DistinctValueKey::Float32(_)
        | DistinctValueKey::Float64(_)
        | DistinctValueKey::Interval(_, _, _)
        | DistinctValueKey::Uuid(_) => 0,
    }
}

fn estimated_distinct_row_key_bytes(key: &DistinctRowKey) -> usize {
    std::mem::size_of::<DistinctRowKey>()
        .saturating_add(
            key.0
                .len()
                .saturating_mul(std::mem::size_of::<DistinctValueKey>()),
        )
        .saturating_add(key.0.iter().map(distinct_key_dynamic_bytes).sum::<usize>())
        .saturating_add(std::mem::size_of::<usize>().saturating_mul(2))
}

fn distinct_aggregate_value(
    spec: &AggregateSpec,
    values: &BTreeMap<DistinctValueKey, Value>,
) -> Result<Value> {
    match spec.function {
        AggregateFunction::Count => i64::try_from(values.len())
            .map(Value::Int64)
            .map_err(|_| DbError::new("22003", "COUNT result is out of range")),
        AggregateFunction::Sum => {
            let mut sum = None;
            for value in values.values().cloned() {
                sum = Some(match sum {
                    None => value,
                    Some(existing) => add_values(existing, value)?,
                });
            }
            Ok(sum.unwrap_or(Value::Null))
        }
        AggregateFunction::Avg => {
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let mut sum = 0.0;
            for value in values.values() {
                sum += numeric_value(value)?;
            }
            Ok(Value::Float64(sum / values.len() as f64))
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let desired = if spec.function == AggregateFunction::Min {
                Ordering::Less
            } else {
                Ordering::Greater
            };
            let mut selected = None;
            for value in values.values().cloned() {
                select_value(
                    &mut selected,
                    Some(value),
                    desired,
                    aggregate_argument_type(spec)?,
                )?;
            }
            Ok(selected.unwrap_or(Value::Null))
        }
    }
}

fn aggregate_argument_type(spec: &AggregateSpec) -> Result<&ScalarType> {
    spec.argument
        .as_ref()
        .map(ExpressionProgram::result_type)
        .ok_or_else(|| DbError::internal("aggregate argument type is unavailable"))
}

fn select_value(
    selected: &mut Option<Value>,
    value: Option<Value>,
    desired: Ordering,
    data_type: &ScalarType,
) -> Result<()> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let replace = selected
        .as_ref()
        .map(|current| {
            super::compare_values_as(&value, current, data_type).map(|order| order == desired)
        })
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
    sort_programs: Vec<Option<ExpressionProgram>>,
    rows: Vec<Row>,
    reservation: Reservation,
    run_paths: Vec<PathBuf>,
}

impl RowsOutputBuilder {
    fn new(
        order_by: &[BoundOrder],
        memory: &QueryMemoryContext,
        max_expression_depth: usize,
    ) -> Result<Self> {
        let (order_by, sort_programs) = super::compile_sort_orders(order_by, max_expression_depth)?;
        Ok(Self {
            order_by,
            sort_programs,
            rows: Vec::new(),
            reservation: memory.try_reserve(0)?,
            run_paths: Vec::new(),
        })
    }

    fn push(
        &mut self,
        mut row: Row,
        params: &[Value],
        stack: &mut ExpressionStack,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<()> {
        super::materialize_sort_keys(
            &mut row,
            &mut self.order_by,
            &self.sort_programs,
            params,
            stack,
        )?;
        let bytes = estimated_row_bytes(&row);
        if !self.rows.is_empty() && memory.would_cross_soft_limit(bytes) {
            sort_rows(&mut self.rows, &self.order_by)?;
            self.run_paths
                .push(spill.write_sorted_run(&self.rows, memory)?);
            self.rows.clear();
            self.reservation.resize(0)?;
        }
        self.reservation.grow(bytes)?;
        self.rows.push(row);
        Ok(())
    }

    fn push_transferred(
        &mut self,
        mut row: Row,
        params: &[Value],
        stack: &mut ExpressionStack,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
        source_reservation: &mut Reservation,
    ) -> Result<()> {
        let transferred_bytes = estimated_row_bytes(&row);
        super::materialize_sort_keys(
            &mut row,
            &mut self.order_by,
            &self.sort_programs,
            params,
            stack,
        )?;
        let bytes = estimated_row_bytes(&row);
        let additional = bytes.saturating_sub(transferred_bytes);
        if !self.rows.is_empty() && memory.would_cross_soft_limit(additional) {
            sort_rows(&mut self.rows, &self.order_by)?;
            self.run_paths
                .push(spill.write_sorted_run(&self.rows, memory)?);
            self.rows.clear();
            self.reservation.resize(0)?;
        }
        source_reservation.transfer_to(&mut self.reservation, transferred_bytes)?;
        self.reservation.grow(additional)?;
        self.rows.push(row);
        Ok(())
    }

    fn finish(
        mut self,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<RowsOutput> {
        if self.run_paths.is_empty() {
            sort_rows(&mut self.rows, &self.order_by)?;
            return Ok(RowsOutput::Memory {
                rows: self.rows,
                offset: 0,
                reservation: Some(self.reservation),
            });
        }
        if !self.rows.is_empty() {
            sort_rows(&mut self.rows, &self.order_by)?;
            self.run_paths
                .push(spill.write_sorted_run(&self.rows, memory)?);
            self.rows.clear();
            self.reservation.resize(0)?;
        }
        let run_paths = spill.compact_sorted_runs(self.run_paths, &self.order_by, memory)?;
        Ok(RowsOutput::Runs {
            merge: SpillMergeCursor::open(&run_paths, &self.order_by, memory)?,
            order_by: self.order_by,
        })
    }
}

enum RowsOutput {
    Memory {
        rows: Vec<Row>,
        offset: usize,
        reservation: Option<Reservation>,
    },
    Runs {
        merge: SpillMergeCursor,
        order_by: Vec<BoundOrder>,
    },
    Indexed {
        store: IndexedRowStore,
        offset: usize,
        current_reservation: Option<Reservation>,
    },
}

impl RowsOutput {
    fn into_window_store(
        self,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<WindowRowStore> {
        match self {
            Self::Memory {
                rows,
                offset,
                reservation,
            } => {
                if offset != 0 {
                    return Err(DbError::internal(
                        "cannot materialize a partially consumed grouped window input",
                    ));
                }
                Ok(WindowRowStore::Memory {
                    rows,
                    reservation: reservation.ok_or_else(|| {
                        DbError::internal("grouped window input reservation is unavailable")
                    })?,
                })
            }
            Self::Runs {
                mut merge,
                order_by,
            } => {
                let mut rows = WindowRowStoreBuilder::new(memory)?;
                while let Some(row) = merge.pop_next(&order_by, memory)? {
                    rows.push(row, memory, spill)?;
                }
                rows.finish(memory)
            }
            Self::Indexed {
                store,
                offset,
                current_reservation,
            } => {
                if offset != 0 || current_reservation.is_some() {
                    return Err(DbError::internal(
                        "cannot reuse a partially consumed indexed window input",
                    ));
                }
                Ok(WindowRowStore::Spill(store))
            }
        }
    }

    fn next_row(&mut self, memory: &QueryMemoryContext) -> Result<Option<Row>> {
        match self {
            Self::Memory {
                rows,
                offset,
                reservation,
            } => {
                let row = rows.get(*offset).cloned();
                *offset = offset.saturating_add(1);
                if row.is_none() {
                    *reservation = None;
                }
                Ok(row)
            }
            Self::Runs { merge, order_by } => merge.pop_next(order_by, memory),
            Self::Indexed {
                store,
                offset,
                current_reservation,
            } => {
                if *offset >= store.len {
                    *current_reservation = None;
                    return Ok(None);
                }
                let ReservedRow { row, reservation } = store.read(*offset, memory)?;
                *offset = offset.saturating_add(1);
                *current_reservation = Some(reservation);
                Ok(Some(row))
            }
        }
    }
}

struct ReservedValues<T> {
    values: Vec<T>,
    reservation: Reservation,
}

impl SpillManager {
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
        memory: &QueryMemoryContext,
    ) -> Result<Vec<PathBuf>> {
        let paths = self.partition_paths(label, count)?;
        let mut writers = paths
            .iter()
            .map(|path| create_spill_writer(path, memory))
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
            write_spill_record(&mut writers[partition], row, memory)?;
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
        memory: &QueryMemoryContext,
    ) -> Result<ReservedValues<Row>> {
        let mut reservation = memory.try_reserve(0)?;
        if !path.exists() {
            return Ok(ReservedValues {
                values: Vec::new(),
                reservation,
            });
        }
        let mut rows = Vec::new();
        let mut reader = open_spill_reader(path, memory)?;
        while let Some(record) = read_spill_record::<Row>(&mut reader, memory)? {
            let row = record.value;
            let value = row
                .values
                .get(key_index)
                .ok_or_else(|| DbError::new("XX001", "hash join spill key is missing"))?;
            if encode_hash_value(value)? == key {
                let row_bytes = estimated_row_bytes(&row);
                reservation.grow(row_bytes)?;
                rows.push(row);
            }
        }
        Ok(ReservedValues {
            values: rows,
            reservation,
        })
    }

    fn write_group_partials(
        &self,
        paths: &[PathBuf],
        groups: &[GroupAccumulator],
        memory: &QueryMemoryContext,
    ) -> Result<()> {
        let mut writers = paths
            .iter()
            .map(|path| {
                if path.exists() {
                    let mut writer = OpenOptions::new()
                        .write(true)
                        .open(path)
                        .map_err(spill_io_error)?;
                    writer.seek(SeekFrom::End(0)).map_err(spill_io_error)?;
                    reserve_spill_writer(writer, memory)
                } else {
                    create_spill_writer(path, memory)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        for group in groups {
            let key = serde_json::to_vec(&group.key).map_err(|error| {
                DbError::new("58030", "aggregate spill key encoding failed")
                    .with_detail(error.to_string())
            })?;
            let partition = stable_partition(&key, paths.len());
            write_spill_record(&mut writers[partition], group, memory)?;
        }
        for writer in &mut writers {
            writer.flush().map_err(spill_io_error)?;
        }
        Ok(())
    }

    fn read_and_merge_groups(
        &self,
        path: &Path,
        memory: &QueryMemoryContext,
        specs: &[AggregateSpec],
    ) -> Result<ReservedValues<GroupAccumulator>> {
        let mut reader = open_spill_reader(path, memory)?;
        let mut groups = Vec::<GroupAccumulator>::new();
        let mut reservation = memory.try_reserve(0)?;
        while let Some(record) = read_spill_record::<GroupAccumulator>(&mut reader, memory)? {
            let incoming = record.value;
            if incoming.aggregates.len() != specs.len() {
                return Err(DbError::new(
                    "XX001",
                    "aggregate spill state width is invalid",
                ));
            }
            if let Some(group) = groups.iter_mut().find(|group| group.key == incoming.key) {
                let before = group.estimated_bytes();
                group.merge(incoming, specs)?;
                let after = group.estimated_bytes();
                if after > before {
                    reservation.grow(after - before)?;
                } else if before > after {
                    reservation.resize(reservation.bytes().saturating_sub(before - after))?;
                }
            } else {
                let group_bytes = incoming.estimated_bytes();
                reservation.grow(group_bytes)?;
                groups.push(incoming);
            }
        }
        groups.sort_by_key(|group| group.first_ordinal);
        Ok(ReservedValues {
            values: groups,
            reservation,
        })
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

fn limit_from_value(value: Value) -> Result<Option<usize>> {
    match value {
        Value::Int64(value) if value >= 0 => usize::try_from(value)
            .map(Some)
            .map_err(|_| DbError::new("22003", "LIMIT value is out of range")),
        Value::Null => Ok(None),
        _ => Err(DbError::new(
            "2201W",
            "LIMIT must be a non-negative integer",
        )),
    }
}

fn offset_from_value(value: Value) -> Result<usize> {
    match value {
        Value::Int64(value) if value >= 0 => usize::try_from(value)
            .map_err(|_| DbError::new("22003", "OFFSET value is out of range")),
        Value::Null => Ok(0),
        _ => Err(DbError::new(
            "2201X",
            "OFFSET must be a non-negative integer",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use ordadb_sql::{BoundExpr, BoundExprKind, BoundProjection, BoundTable, JoinKind};
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
    fn nested_query_options_decrement_and_enforce_the_plan_depth_budget() {
        let memory = QueryMemoryContext::new(1024, 4096).expect("memory grant");
        let nested = nested_execution_options(
            &ExecutionOptions {
                max_plan_depth: 2,
                ..ExecutionOptions::default()
            },
            &memory,
        )
        .expect("one nested query level");
        assert_eq!(nested.max_plan_depth, 1);

        let error = nested_execution_options(&nested, &memory)
            .expect_err("nested query depth must be exhausted");
        assert_eq!(error.sql_state, "54001");
    }

    #[test]
    fn ranking_windows_partition_rank_and_preserve_stable_source_order() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let rows = vec![
            Row::new(vec![Value::Int64(2), Value::Text("a".to_owned())]),
            Row::new(vec![Value::Int64(1), Value::Text("a".to_owned())]),
            Row::new(vec![Value::Int64(5), Value::Text("b".to_owned())]),
            Row::new(vec![Value::Int64(2), Value::Text("a".to_owned())]),
        ];
        let tables = BTreeMap::from([(table_id, Arc::new(rows))]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let window = |function| BoundWindow {
            function,
            value_index: 2,
            arguments: Vec::new(),
            count_star: false,
            filter: None,
            partition_by: vec![column(1, ScalarType::Text)],
            order_by: vec![BoundOrder {
                column_index: 0,
                expression: None,
                data_type: ScalarType::Int64,
                ascending: true,
                nulls_first: None,
            }],
            frame: None,
            data_type: ScalarType::Int64,
            nullable: false,
        };
        let plan = AdvancedExecutionPlan {
            table: BoundTable {
                table_id,
                binding: Identifier::unquoted("items"),
                offset: 0,
                width: 2,
                nullable: false,
            },
            joins: Vec::new(),
            applies: Vec::new(),
            windows: vec![
                window(WindowFunction::RowNumber),
                window(WindowFunction::Rank),
                window(WindowFunction::DenseRank),
            ],
            schema: Schema::new(vec![
                Field::new("id", ScalarType::Int64, false),
                Field::new("group", ScalarType::Text, false),
                Field::new("row_no", ScalarType::Int64, false),
                Field::new("rank_no", ScalarType::Int64, false),
                Field::new("dense_no", ScalarType::Int64, false),
            ]),
            projection: vec![
                projection(0, "id"),
                BoundProjection {
                    expr: column(1, ScalarType::Text),
                    field: Field::new("group", ScalarType::Text, false),
                },
                projection(2, "row_no"),
                projection(3, "rank_no"),
                projection(4, "dense_no"),
            ],
            distinct: false,
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
            limit: None,
            aggregate: false,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan.clone(),
            &context,
            ExecutionOptions {
                batch_rows: 2,
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("ranking cursor");
        let mut actual = Vec::new();
        while let Some(batch) = cursor.next_batch().expect("ranking batch") {
            actual.extend(batch.rows);
        }
        assert_eq!(
            actual,
            vec![
                Row::new(vec![
                    Value::Int64(2),
                    Value::Text("a".to_owned()),
                    Value::Int64(2),
                    Value::Int64(2),
                    Value::Int64(2),
                ]),
                Row::new(vec![
                    Value::Int64(1),
                    Value::Text("a".to_owned()),
                    Value::Int64(1),
                    Value::Int64(1),
                    Value::Int64(1),
                ]),
                Row::new(vec![
                    Value::Int64(5),
                    Value::Text("b".to_owned()),
                    Value::Int64(1),
                    Value::Int64(1),
                    Value::Int64(1),
                ]),
                Row::new(vec![
                    Value::Int64(2),
                    Value::Text("a".to_owned()),
                    Value::Int64(3),
                    Value::Int64(2),
                    Value::Int64(2),
                ]),
            ]
        );
        assert_eq!(cursor.memory().current_bytes(), 0);
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("in-memory window spill root")
                .next()
                .is_none()
        );

        let mut ordered = plan;
        ordered.order_by = vec![BoundOrder {
            column_index: 2,
            expression: None,
            data_type: ScalarType::Int64,
            ascending: false,
            nulls_first: None,
        }];
        let mut ordered_cursor =
            AdvancedExecutionCursor::new(ordered, &context).expect("ordered ranking cursor");
        let first = ordered_cursor
            .next_batch()
            .expect("ordered ranking batch")
            .expect("ordered rows");
        assert_eq!(first.rows[0].values[2], Value::Int64(3));
    }

    #[test]
    fn ranking_windows_spill_one_large_partition_across_multiple_programs() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let rows = (0..128)
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
        let window = |function, value_index| BoundWindow {
            function,
            value_index,
            arguments: Vec::new(),
            count_star: false,
            filter: None,
            partition_by: Vec::new(),
            order_by: vec![BoundOrder {
                column_index: 0,
                expression: None,
                data_type: ScalarType::Int64,
                ascending: true,
                nulls_first: None,
            }],
            frame: None,
            data_type: ScalarType::Int64,
            nullable: false,
        };
        let plan = AdvancedExecutionPlan {
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            applies: Vec::new(),
            windows: vec![
                window(WindowFunction::RowNumber, 1),
                window(WindowFunction::Rank, 2),
            ],
            schema: Schema::new(vec![
                Field::new("id", ScalarType::Int64, false),
                Field::new("row_no", ScalarType::Int64, false),
                Field::new("rank_no", ScalarType::Int64, false),
            ]),
            projection: vec![
                projection(0, "id"),
                projection(1, "row_no"),
                projection(2, "rank_no"),
            ],
            distinct: false,
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
            limit: None,
            aggregate: false,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                batch_rows: 17,
                soft_memory_bytes: 512,
                hard_memory_bytes: 256 * 1024,
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("spilling window cursor");
        let mut actual = Vec::new();
        while let Some(batch) = cursor.next_batch().expect("spilling window batch") {
            actual.extend(batch.rows);
        }
        assert_eq!(actual.len(), 128);
        for (ordinal, row) in actual.iter().enumerate() {
            let value = 127_i64.saturating_sub(i64::try_from(ordinal).expect("row ordinal"));
            assert_eq!(
                row.values,
                vec![
                    Value::Int64(value),
                    Value::Int64(value.saturating_add(1)),
                    Value::Int64(value.saturating_add(1)),
                ]
            );
        }
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("spill root entries")
                .next()
                .is_some()
        );
        let memory = cursor.memory().clone();
        drop(cursor);
        assert_eq!(memory.current_bytes(), 0);
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("clean spill root")
                .next()
                .is_none()
        );
    }

    #[test]
    fn spilling_window_cancellation_releases_grants_and_drop_cleans_files() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let rows = (0..50_000)
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
            applies: Vec::new(),
            windows: vec![BoundWindow {
                function: WindowFunction::RowNumber,
                value_index: 1,
                arguments: Vec::new(),
                count_star: false,
                filter: None,
                partition_by: Vec::new(),
                order_by: vec![BoundOrder {
                    column_index: 0,
                    expression: None,
                    data_type: ScalarType::Int64,
                    ascending: true,
                    nulls_first: None,
                }],
                frame: None,
                data_type: ScalarType::Int64,
                nullable: false,
            }],
            schema: Schema::new(vec![Field::new("row_no", ScalarType::Int64, false)]),
            projection: vec![projection(1, "row_no")],
            distinct: false,
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
            limit: None,
            aggregate: false,
        };
        let cancellation = Arc::new(AtomicBool::new(false));
        let watcher_cancellation = Arc::clone(&cancellation);
        let watcher_root = spill_root.path().to_path_buf();
        let watcher = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                if std::fs::read_dir(&watcher_root)
                    .ok()
                    .is_some_and(|mut entries| entries.next().is_some())
                {
                    watcher_cancellation.store(true, AtomicOrdering::Release);
                    return true;
                }
                std::thread::yield_now();
            }
            false
        });
        let mut cursor = AdvancedExecutionCursor::with_options_and_cancellation(
            plan,
            &context,
            ExecutionOptions {
                soft_memory_bytes: 512,
                hard_memory_bytes: 16 * 1024 * 1024,
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
            Some(cancellation),
        )
        .expect("cancellable spilling window cursor");
        let error = cursor
            .next_batch()
            .expect_err("spilling window must observe cancellation");
        assert!(watcher.join().expect("cancellation watcher"));
        assert_eq!(error.sql_state, "57014");
        assert_eq!(cursor.memory().current_bytes(), 0);
        drop(cursor);
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("clean spill root")
                .next()
                .is_none()
        );
    }

    #[test]
    fn ranking_window_hard_limit_fails_and_releases_state() {
        let table_id = TableId::new(1);
        let rows = (0..16)
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
            applies: Vec::new(),
            windows: vec![BoundWindow {
                function: WindowFunction::RowNumber,
                value_index: 1,
                arguments: Vec::new(),
                count_star: false,
                filter: None,
                partition_by: Vec::new(),
                order_by: vec![BoundOrder {
                    column_index: 0,
                    expression: None,
                    data_type: ScalarType::Int64,
                    ascending: true,
                    nulls_first: None,
                }],
                frame: None,
                data_type: ScalarType::Int64,
                nullable: false,
            }],
            schema: Schema::new(vec![Field::new("row_no", ScalarType::Int64, false)]),
            projection: vec![projection(1, "row_no")],
            distinct: false,
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
            limit: None,
            aggregate: false,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan.clone(),
            &context,
            ExecutionOptions {
                soft_memory_bytes: 128,
                hard_memory_bytes: 256,
                ..ExecutionOptions::default()
            },
        )
        .expect("bounded ranking cursor");
        let error = cursor.next_batch().expect_err("window must hit hard limit");
        assert_eq!(error.sql_state, "53200");
        assert_eq!(cursor.memory().current_bytes(), 0);

        let cancellation = Arc::new(AtomicBool::new(true));
        let mut cancelled =
            AdvancedExecutionCursor::new_with_cancellation(plan, &context, Some(cancellation))
                .expect("cancellable ranking cursor");
        let error = cancelled
            .next_batch()
            .expect_err("cancelled window must stop before initialization");
        assert_eq!(error.sql_state, "57014");
        assert_eq!(cancelled.memory().current_bytes(), 0);
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
        let join = JoinExecutionPlan {
            source: JoinExecutionSource::Table(table(right_id, "right_items", 1)),
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
            distinct: false,
            table: table(left_id, "left_items", 0),
            joins: vec![join],
            applies: Vec::new(),
            windows: Vec::new(),
            schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
            projection: vec![projection(0, "id")],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
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
    fn lateral_parameter_frames_obey_the_outer_hard_memory_limit() {
        let spill_root = tempdir().expect("spill root");
        let left_id = TableId::new(1);
        let right_id = TableId::new(2);
        let payload = "x".repeat(8 * 1024);
        let tables = BTreeMap::from([
            (
                left_id,
                Arc::new(vec![Row::new(vec![Value::Text(payload.clone())])]),
            ),
            (
                right_id,
                Arc::new(vec![Row::new(vec![Value::Text(payload)])]),
            ),
        ]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let inner_column = column(0, ScalarType::Text);
        let inner = QueryExecutionPlan::Advanced(Box::new(AdvancedExecutionPlan {
            distinct: false,
            table: table(right_id, "right_items", 0),
            joins: Vec::new(),
            applies: Vec::new(),
            windows: Vec::new(),
            schema: Schema::new(vec![Field::new("payload", ScalarType::Text, false)]),
            projection: vec![BoundProjection {
                expr: inner_column.clone(),
                field: Field::new("payload", ScalarType::Text, false),
            }],
            filter: Some(BoundExpr {
                kind: BoundExprKind::Binary {
                    left: Box::new(inner_column),
                    op: BinaryOperator::Eq,
                    right: Box::new(BoundExpr {
                        kind: BoundExprKind::Parameter { index: 1 },
                        data_type: ScalarType::Text,
                        nullable: true,
                    }),
                },
                data_type: ScalarType::Boolean,
                nullable: true,
            }),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
            limit: None,
            aggregate: false,
        }));
        let plan = AdvancedExecutionPlan {
            distinct: false,
            table: table(left_id, "left_items", 0),
            joins: vec![JoinExecutionPlan {
                source: JoinExecutionSource::Derived {
                    query: Box::new(inner),
                    correlation_indexes: vec![0],
                    offset: 1,
                    width: 1,
                },
                kind: JoinKind::Inner,
                on: BoundExpr {
                    kind: BoundExprKind::Literal(Value::Boolean(true)),
                    data_type: ScalarType::Boolean,
                    nullable: false,
                },
            }],
            applies: Vec::new(),
            windows: Vec::new(),
            schema: Schema::new(vec![Field::new("payload", ScalarType::Text, false)]),
            projection: vec![BoundProjection {
                expr: column(0, ScalarType::Text),
                field: Field::new("payload", ScalarType::Text, false),
            }],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
            limit: None,
            aggregate: false,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                soft_memory_bytes: 512,
                hard_memory_bytes: 2 * 1024,
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("LATERAL cursor");
        let baseline = cursor.memory().current_bytes();
        let error = cursor.next_batch().expect_err("LATERAL parameter memory");
        assert_eq!(error.sql_state, "53200");
        assert_eq!(cursor.memory().current_bytes(), baseline);
        drop(cursor);
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("spill root entries")
                .next()
                .is_none()
        );
    }

    #[test]
    fn correlated_row_apply_memory_errors_and_cancellation_release_state() {
        let outer_id = TableId::new(1);
        let inner_id = TableId::new(2);
        let tables = BTreeMap::from([
            (outer_id, Arc::new(vec![Row::new(vec![Value::Int64(1)])])),
            (
                inner_id,
                Arc::new(vec![Row::new(vec![
                    Value::Int32(1),
                    Value::Text("x".repeat(8 * 1024)),
                ])]),
            ),
        ]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let inner = QueryExecutionPlan::Advanced(Box::new(AdvancedExecutionPlan {
            distinct: false,
            table: BoundTable {
                table_id: inner_id,
                binding: Identifier::unquoted("inner_items"),
                offset: 0,
                width: 2,
                nullable: false,
            },
            joins: Vec::new(),
            applies: Vec::new(),
            windows: Vec::new(),
            schema: Schema::new(vec![
                Field::new("id", ScalarType::Int32, false),
                Field::new("payload", ScalarType::Text, false),
            ]),
            projection: vec![
                BoundProjection {
                    expr: column(0, ScalarType::Int32),
                    field: Field::new("id", ScalarType::Int32, false),
                },
                BoundProjection {
                    expr: column(1, ScalarType::Text),
                    field: Field::new("payload", ScalarType::Text, false),
                },
            ],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
            limit: None,
            aggregate: false,
        }));
        let plan = AdvancedExecutionPlan {
            distinct: false,
            table: table(outer_id, "outer_items", 0),
            joins: Vec::new(),
            applies: vec![ApplyExecutionPlan {
                kind: ApplyExecutionKind::RowQuantified {
                    left: vec![
                        BoundExpr {
                            kind: BoundExprKind::Literal(Value::Int64(1)),
                            data_type: ScalarType::Int64,
                            nullable: false,
                        },
                        BoundExpr {
                            kind: BoundExprKind::Literal(Value::Text("x".to_owned())),
                            data_type: ScalarType::Text,
                            nullable: false,
                        },
                    ],
                    op: BinaryOperator::Eq,
                    quantifier: SubqueryQuantifier::Any,
                    negated: false,
                    operand_types: vec![ScalarType::Int64, ScalarType::Text],
                },
                query: Box::new(inner),
                correlation_indexes: vec![0],
            }],
            windows: Vec::new(),
            schema: Schema::new(vec![Field::new("matched", ScalarType::Boolean, true)]),
            projection: vec![BoundProjection {
                expr: BoundExpr {
                    kind: BoundExprKind::ApplyValue { index: 1 },
                    data_type: ScalarType::Boolean,
                    nullable: true,
                },
                field: Field::new("matched", ScalarType::Boolean, true),
            }],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
            limit: None,
            aggregate: false,
        };

        let mut bounded = AdvancedExecutionCursor::with_options(
            plan.clone(),
            &context,
            ExecutionOptions {
                soft_memory_bytes: 512,
                hard_memory_bytes: 2 * 1024,
                ..ExecutionOptions::default()
            },
        )
        .expect("bounded row Apply cursor");
        let baseline = bounded.memory().current_bytes();
        let error = bounded
            .next_batch()
            .expect_err("row Apply candidate memory");
        assert_eq!(error.sql_state, "53200");
        assert_eq!(bounded.memory().current_bytes(), baseline);

        let cancellation = Arc::new(AtomicBool::new(true));
        let mut cancelled =
            AdvancedExecutionCursor::new_with_cancellation(plan, &context, Some(cancellation))
                .expect("cancellable row Apply cursor");
        let baseline = cancelled.memory().current_bytes();
        let error = cancelled
            .next_batch()
            .expect_err("cancelled row Apply must stop");
        assert_eq!(error.sql_state, "57014");
        assert_eq!(cancelled.memory().current_bytes(), baseline);
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
                distinct: false,
                filter: None,
            },
            data_type: ScalarType::Int64,
            nullable: false,
        };
        let plan = AdvancedExecutionPlan {
            distinct: false,
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            applies: Vec::new(),
            windows: Vec::new(),
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
                expression: None,
                data_type: ScalarType::Int64,
                ascending: true,
                nulls_first: None,
            }],
            offset: None,
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
    fn grouped_window_consumes_spilled_rows_without_full_materialization() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let rows = (0..96)
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
            distinct: false,
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            applies: Vec::new(),
            windows: vec![BoundWindow {
                function: WindowFunction::Rank,
                value_index: 1,
                arguments: Vec::new(),
                count_star: false,
                filter: None,
                partition_by: Vec::new(),
                order_by: vec![BoundOrder {
                    column_index: 0,
                    expression: None,
                    data_type: ScalarType::Int64,
                    ascending: true,
                    nulls_first: None,
                }],
                frame: None,
                data_type: ScalarType::Int64,
                nullable: false,
            }],
            schema: Schema::new(vec![
                Field::new("id", ScalarType::Int64, false),
                Field::new("rank_no", ScalarType::Int64, false),
            ]),
            projection: vec![
                projection(0, "id"),
                BoundProjection {
                    expr: BoundExpr {
                        kind: BoundExprKind::ApplyValue { index: 1 },
                        data_type: ScalarType::Int64,
                        nullable: false,
                    },
                    field: Field::new("rank_no", ScalarType::Int64, false),
                },
            ],
            filter: None,
            group_by: vec![column(0, ScalarType::Int64)],
            having: None,
            order_by: vec![BoundOrder {
                column_index: 0,
                expression: None,
                data_type: ScalarType::Int64,
                ascending: true,
                nulls_first: None,
            }],
            offset: None,
            limit: None,
            aggregate: true,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                batch_rows: 11,
                soft_memory_bytes: 768,
                hard_memory_bytes: 1024 * 1024,
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("grouped spilling window cursor");
        let mut actual = Vec::new();
        while let Some(batch) = cursor.next_batch().expect("grouped window batch") {
            actual.extend(batch.rows);
        }
        assert_eq!(actual.len(), 96);
        for (value, row) in actual.iter().enumerate() {
            let value = i64::try_from(value).expect("group value");
            assert_eq!(
                row.values,
                vec![Value::Int64(value), Value::Int64(value.saturating_add(1))]
            );
        }
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("spill root entries")
                .next()
                .is_some()
        );
        let memory = cursor.memory().clone();
        drop(cursor);
        assert_eq!(memory.current_bytes(), 0);
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("clean spill root")
                .next()
                .is_none()
        );
    }

    #[test]
    fn distinct_aggregate_spill_merges_overlapping_partial_values() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let rows = (0..96)
            .map(|value| Row::new(vec![Value::Int64(value % 8)]))
            .collect::<Vec<_>>();
        let tables = BTreeMap::from([(table_id, Arc::new(rows))]);
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let aggregate = |function, name: &str, data_type: ScalarType, nullable| BoundProjection {
            expr: BoundExpr {
                kind: BoundExprKind::Aggregate {
                    function,
                    argument: Some(Box::new(column(0, ScalarType::Int64))),
                    distinct: true,
                    filter: None,
                },
                data_type: data_type.clone(),
                nullable,
            },
            field: Field::new(name, data_type, nullable),
        };
        let plan = AdvancedExecutionPlan {
            distinct: false,
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            applies: Vec::new(),
            windows: Vec::new(),
            schema: Schema::new(vec![
                Field::new("count", ScalarType::Int64, false),
                Field::new("sum", ScalarType::Int64, true),
            ]),
            projection: vec![
                aggregate(AggregateFunction::Count, "count", ScalarType::Int64, false),
                aggregate(AggregateFunction::Sum, "sum", ScalarType::Int64, true),
            ],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
            limit: None,
            aggregate: true,
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                soft_memory_bytes: 768,
                hard_memory_bytes: 1024 * 1024,
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("cursor");
        assert_eq!(
            cursor
                .next_batch()
                .expect("batch")
                .expect("DISTINCT aggregate row")
                .rows,
            vec![Row::new(vec![Value::Int64(8), Value::Int64(28)])]
        );
        assert!(cursor.next_batch().expect("end").is_none());
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
    fn select_distinct_charges_only_new_keys_to_the_hard_limit() {
        let spill_root = tempdir().expect("spill root");
        let table_id = TableId::new(1);
        let duplicate_rows = (0..64)
            .map(|value| Row::new(vec![Value::Int64(value % 2)]))
            .collect::<Vec<_>>();
        let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
        let plan = AdvancedExecutionPlan {
            distinct: true,
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            applies: Vec::new(),
            windows: Vec::new(),
            schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
            projection: vec![projection(0, "id")],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
            limit: None,
            aggregate: false,
        };
        let tables = BTreeMap::from([(table_id, Arc::new(duplicate_rows))]);
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan.clone(),
            &context,
            ExecutionOptions {
                batch_rows: 64,
                soft_memory_bytes: 1024,
                hard_memory_bytes: 2048,
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("duplicate cursor");
        assert_eq!(
            cursor
                .next_batch()
                .expect("duplicate batch")
                .expect("duplicate rows")
                .rows,
            vec![
                Row::new(vec![Value::Int64(0)]),
                Row::new(vec![Value::Int64(1)]),
            ]
        );
        assert!(cursor.next_batch().expect("duplicate end").is_none());

        let unique_rows = (0..64)
            .map(|value| Row::new(vec![Value::Int64(value)]))
            .collect::<Vec<_>>();
        let tables = BTreeMap::from([(table_id, Arc::new(unique_rows))]);
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let mut cursor = AdvancedExecutionCursor::with_options(
            plan,
            &context,
            ExecutionOptions {
                batch_rows: 64,
                soft_memory_bytes: 1024,
                hard_memory_bytes: 2048,
                spill_root: spill_root.path().to_path_buf(),
                ..ExecutionOptions::default()
            },
        )
        .expect("unique cursor");
        let error = cursor
            .next_batch()
            .expect_err("unique DISTINCT keys must exhaust the hard grant");
        assert_eq!(error.sql_state, "53200");
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
            distinct: false,
            table: table(left_id, "left_items", 0),
            joins: vec![JoinExecutionPlan {
                source: JoinExecutionSource::Table(BoundTable {
                    nullable: true,
                    ..table(right_id, "right_items", 1)
                }),
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
            applies: Vec::new(),
            windows: Vec::new(),
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
            offset: None,
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
    fn non_aggregate_sort_spills_then_applies_offset_and_limit() {
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
            distinct: false,
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            applies: Vec::new(),
            windows: Vec::new(),
            schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
            projection: vec![projection(0, "id")],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: vec![BoundOrder {
                column_index: 0,
                expression: None,
                data_type: ScalarType::Int64,
                ascending: true,
                nulls_first: None,
            }],
            offset: Some(BoundExpr {
                kind: BoundExprKind::Literal(Value::Int64(7)),
                data_type: ScalarType::Int64,
                nullable: false,
            }),
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
            (7..12)
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
            distinct: false,
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            applies: Vec::new(),
            windows: Vec::new(),
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
                            distinct: false,
                            filter: None,
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
                            distinct: false,
                            filter: None,
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
            offset: None,
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
            distinct: false,
            table: table(left_id, "left_items", 0),
            joins: vec![JoinExecutionPlan {
                source: JoinExecutionSource::Table(table(right_id, "right_items", 1)),
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
            applies: Vec::new(),
            windows: Vec::new(),
            schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
            projection: vec![projection(0, "id")],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            offset: None,
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
            distinct: false,
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            applies: Vec::new(),
            windows: Vec::new(),
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
            offset: None,
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
                distinct: false,
                filter: None,
            },
            data_type: ScalarType::Int64,
            nullable: false,
        };
        let plan = AdvancedExecutionPlan {
            distinct: false,
            table: table(table_id, "items", 0),
            joins: Vec::new(),
            applies: Vec::new(),
            windows: Vec::new(),
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
            offset: None,
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
