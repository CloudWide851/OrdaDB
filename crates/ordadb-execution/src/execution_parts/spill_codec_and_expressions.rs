
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
