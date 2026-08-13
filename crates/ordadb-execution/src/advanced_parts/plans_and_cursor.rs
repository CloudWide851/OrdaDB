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
