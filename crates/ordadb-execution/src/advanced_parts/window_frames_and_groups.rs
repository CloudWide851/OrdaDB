
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
