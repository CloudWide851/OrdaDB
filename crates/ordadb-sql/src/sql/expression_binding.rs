
fn bind_expr_multi(
    expr: ParsedExpr,
    inputs: &[InputColumn],
    expected: Option<&ScalarType>,
    allow_aggregate: bool,
) -> Result<BoundExpr> {
    let position = expr.position;
    match expr.kind {
        ParsedExprKind::Column(name) => {
            let column = resolve_input_column(&name, inputs)?;
            if let Some(expected) = expected {
                ensure_types_compatible(&column.data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: if column.outer_depth > 0 {
                    BoundExprKind::Correlation {
                        depth: column.outer_depth,
                        index: column.index,
                    }
                } else {
                    BoundExprKind::Column {
                        index: column.index,
                    }
                },
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
        }
        ParsedExprKind::Literal(value) => bind_literal(value, expected, position),
        ParsedExprKind::Parameter(index) => {
            let data_type = expected.cloned().ok_or_else(|| {
                DbError::new(
                    INDETERMINATE_DATATYPE,
                    format!("could not determine data type of parameter ${index}"),
                )
                .with_position_opt(position)
            })?;
            Ok(BoundExpr {
                kind: BoundExprKind::Parameter { index },
                data_type,
                nullable: true,
            })
        }
        ParsedExprKind::ResolvedParameter { index, data_type } => {
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::Parameter { index },
                data_type,
                nullable: true,
            })
        }
        ParsedExprKind::ApplyValue {
            index,
            data_type,
            nullable,
        } => {
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::ApplyValue { index },
                data_type,
                nullable,
            })
        }
        ParsedExprKind::Unary { op, expr } => {
            let expected_type = match op {
                UnaryOperator::Not => Some(&ScalarType::Boolean),
                UnaryOperator::Negate => expected,
            };
            let bound = bind_expr_multi(*expr, inputs, expected_type, allow_aggregate)?;
            match op {
                UnaryOperator::Not if bound.data_type != ScalarType::Boolean => Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "NOT operand must be boolean",
                )
                .with_position_opt(position)),
                UnaryOperator::Negate if !is_numeric(&bound.data_type) => Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "unary minus requires a numeric operand",
                )
                .with_position_opt(position)),
                _ => {
                    let data_type = bound.data_type.clone();
                    let nullable = bound.nullable;
                    Ok(BoundExpr {
                        kind: BoundExprKind::Unary {
                            op,
                            expr: Box::new(bound),
                        },
                        data_type,
                        nullable,
                    })
                }
            }
        }
        ParsedExprKind::Cast {
            expr, data_type, ..
        } => {
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            let source_type = infer_multi_type(&expr, inputs)?;
            let bound = bind_expr_multi(
                *expr,
                inputs,
                source_type.is_none().then_some(&data_type),
                allow_aggregate,
            )?;
            ensure_explicit_cast_supported(&bound.data_type, &data_type, position)?;
            let nullable = bound.nullable;
            Ok(BoundExpr {
                kind: BoundExprKind::Cast {
                    expr: Box::new(bound),
                },
                data_type,
                nullable,
            })
        }
        ParsedExprKind::Array {
            elements,
            dimensions,
        } => {
            let expected_element = match expected {
                Some(ScalarType::Array { element }) => Some(element.as_ref().clone()),
                Some(expected) => {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        format!("array cannot be assigned to {expected:?}"),
                    )
                    .with_position_opt(position));
                }
                None => None,
            };
            let mut element_type = expected_element;
            for element in &elements {
                let Some(candidate) = infer_multi_type(element, inputs)? else {
                    continue;
                };
                element_type = Some(match element_type {
                    Some(current) => common_type(&current, &candidate).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "array element types {current:?} and {candidate:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(position)
                    })?,
                    None => candidate,
                });
            }
            let element_type = element_type.ok_or_else(|| {
                DbError::new(
                    INDETERMINATE_DATATYPE,
                    "cannot determine type of empty array",
                )
                .with_hint("Explicitly cast the array, for example ARRAY[]::integer[].")
                .with_position_opt(position)
            })?;
            if matches!(element_type, ScalarType::Array { .. }) {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "nested array values must use one flattened PostgreSQL array type",
                )
                .with_position_opt(position));
            }
            let elements = elements
                .into_iter()
                .map(|element| {
                    bind_expr_multi(element, inputs, Some(&element_type), allow_aggregate)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(BoundExpr {
                kind: BoundExprKind::Array {
                    elements,
                    dimensions,
                },
                data_type: ScalarType::Array {
                    element: Box::new(element_type),
                },
                nullable: false,
            })
        }
        ParsedExprKind::Function {
            function,
            arguments,
        } => bind_scalar_function_multi(
            function,
            arguments,
            inputs,
            expected,
            allow_aggregate,
            position,
        ),
        ParsedExprKind::Binary { left, op, right } => bind_multi_binary(
            *left,
            op,
            *right,
            inputs,
            position,
            expected,
            allow_aggregate,
        ),
        ParsedExprKind::InList {
            expr,
            list,
            negated,
        } => {
            if expected.is_some_and(|expected| expected != &ScalarType::Boolean) {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "IN predicate produces a boolean result",
                )
                .with_position_opt(position));
            }
            let mut operand_type = infer_multi_type(&expr, inputs)?;
            for candidate in &list {
                let Some(candidate_type) = infer_multi_type(candidate, inputs)? else {
                    continue;
                };
                operand_type = Some(match operand_type {
                    Some(current) => common_type(&current, &candidate_type).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "IN types {current:?} and {candidate_type:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(position)
                    })?,
                    None => candidate_type,
                });
            }
            let operand_type = operand_type.ok_or_else(|| {
                DbError::new(
                    INDETERMINATE_DATATYPE,
                    "could not determine data type of IN operands",
                )
                .with_position_opt(position)
            })?;
            if operand_type == ScalarType::Json {
                return Err(DbError::new(
                    "42883",
                    "could not identify an equality operator for type json",
                )
                .with_position_opt(position));
            }
            let expr = bind_expr_multi(*expr, inputs, Some(&operand_type), allow_aggregate)?;
            let list = list
                .into_iter()
                .map(|candidate| {
                    bind_expr_multi(candidate, inputs, Some(&operand_type), allow_aggregate)
                })
                .collect::<Result<Vec<_>>>()?;
            let nullable = expr.nullable || list.iter().any(|candidate| candidate.nullable);
            Ok(BoundExpr {
                kind: BoundExprKind::InList {
                    expr: Box::new(expr),
                    list,
                    negated,
                },
                data_type: ScalarType::Boolean,
                nullable,
            })
        }
        ParsedExprKind::ScalarSubquery(_) => unsupported_at(
            "scalar subquery Apply execution is not supported yet",
            position,
        ),
        ParsedExprKind::Exists { .. } => {
            unsupported_at("EXISTS Apply execution is not supported yet", position)
        }
        ParsedExprKind::InSubquery { .. } => {
            unsupported_at("IN subquery Apply execution is not supported yet", position)
        }
        ParsedExprKind::QuantifiedSubquery { .. } => unsupported_at(
            "ANY/ALL subquery Apply execution is not supported yet",
            position,
        ),
        ParsedExprKind::RowSubquery { .. } => unsupported_at(
            "row subquery Apply execution is not supported in this context",
            position,
        ),
        ParsedExprKind::Aggregate {
            function,
            argument,
            distinct,
            filter,
        } => {
            if !allow_aggregate {
                return Err(DbError::new(
                    "42803",
                    "aggregate functions are not allowed in this clause",
                )
                .with_position_opt(position));
            }
            let argument = argument
                .map(|argument| bind_expr_multi(*argument, inputs, None, false))
                .transpose()?;
            if distinct
                && argument
                    .as_ref()
                    .is_some_and(|argument| argument.data_type == ScalarType::Json)
            {
                return Err(DbError::new(
                    "42883",
                    "could not identify an equality operator for type json",
                )
                .with_position_opt(position));
            }
            let filter = filter
                .map(|filter| bind_expr_multi(*filter, inputs, Some(&ScalarType::Boolean), false))
                .transpose()?;
            if filter
                .as_ref()
                .is_some_and(|filter| filter.data_type != ScalarType::Boolean)
            {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "aggregate FILTER predicate must be boolean",
                )
                .with_position_opt(position));
            }
            let (data_type, nullable) = match (function, argument.as_ref()) {
                (AggregateFunction::Count, _) => (ScalarType::Int64, false),
                (AggregateFunction::Avg, Some(argument)) if is_numeric(&argument.data_type) => {
                    (ScalarType::Float64, true)
                }
                (AggregateFunction::Sum, Some(argument)) if is_numeric(&argument.data_type) => {
                    let data_type = match argument.data_type {
                        ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64 => {
                            ScalarType::Int64
                        }
                        ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
                        ScalarType::Decimal { .. } => argument.data_type.clone(),
                        _ => unreachable!("numeric guard"),
                    };
                    (data_type, true)
                }
                (AggregateFunction::Min | AggregateFunction::Max, Some(argument))
                    if indexable_type(&argument.data_type) =>
                {
                    (argument.data_type.clone(), true)
                }
                _ => {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        "aggregate argument has an incompatible type",
                    )
                    .with_position_opt(position));
                }
            };
            Ok(BoundExpr {
                kind: BoundExprKind::Aggregate {
                    function,
                    argument: argument.map(Box::new),
                    distinct,
                    filter: filter.map(Box::new),
                },
                data_type,
                nullable,
            })
        }
        ParsedExprKind::Window { .. }
        | ParsedExprKind::NamedWindow { .. }
        | ParsedExprKind::WindowValue { .. } => Err(DbError::internal(
            "window expression reached binding before lowering",
        )),
    }
}

fn bind_scalar_function_multi(
    function: ScalarFunction,
    arguments: Vec<ParsedExpr>,
    inputs: &[InputColumn],
    expected: Option<&ScalarType>,
    allow_aggregate: bool,
    position: Option<usize>,
) -> Result<BoundExpr> {
    let inferred = infer_scalar_function_type(
        function,
        &arguments,
        |argument| infer_multi_type(argument, inputs),
        position,
    )?;
    if let (Some(actual), Some(expected)) = (&inferred, expected) {
        ensure_types_compatible(actual, expected, position)?;
    }
    let common = matches!(
        function,
        ScalarFunction::Coalesce
            | ScalarFunction::NullIf
            | ScalarFunction::Greatest
            | ScalarFunction::Least
    )
    .then_some(inferred.as_ref())
    .flatten();
    let arguments = arguments
        .into_iter()
        .enumerate()
        .map(|(index, argument)| {
            let expected = scalar_function_argument_type(function, index, common);
            bind_expr_multi(argument, inputs, expected, allow_aggregate)
        })
        .collect::<Result<Vec<_>>>()?;
    let (data_type, nullable) = validate_bound_scalar_function(function, &arguments, position)?;
    Ok(BoundExpr {
        kind: BoundExprKind::Function {
            function,
            arguments,
        },
        data_type,
        nullable,
    })
}

fn bind_multi_binary(
    left: ParsedExpr,
    op: BinaryOperator,
    right: ParsedExpr,
    inputs: &[InputColumn],
    position: Option<usize>,
    expected: Option<&ScalarType>,
    allow_aggregate: bool,
) -> Result<BoundExpr> {
    if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
        let left = bind_expr_multi(left, inputs, Some(&ScalarType::Boolean), allow_aggregate)?;
        let right = bind_expr_multi(right, inputs, Some(&ScalarType::Boolean), allow_aggregate)?;
        return Ok(BoundExpr {
            nullable: left.nullable || right.nullable,
            data_type: ScalarType::Boolean,
            kind: BoundExprKind::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
        });
    }
    let left_type = infer_multi_type(&left, inputs)?;
    let right_type = infer_multi_type(&right, inputs)?;
    let mut operand_type = match (left_type, right_type) {
        (Some(left), Some(right)) => common_type(&left, &right).ok_or_else(|| {
            DbError::new(
                DATATYPE_MISMATCH,
                format!("operator cannot match {left:?} with {right:?}"),
            )
            .with_position_opt(position)
        })?,
        (Some(data_type), None) | (None, Some(data_type)) => data_type,
        (None, None) => {
            return Err(DbError::new(
                INDETERMINATE_DATATYPE,
                "could not determine comparison operand types",
            )
            .with_position_opt(position));
        }
    };
    if is_arithmetic_operator(op) && !is_numeric(&operand_type) {
        return Err(DbError::new(
            "42883",
            format!("arithmetic operator is not defined for {operand_type:?}"),
        )
        .with_position_opt(position));
    }
    if is_arithmetic_operator(op)
        && let Some(expected) = expected
    {
        ensure_types_compatible(&operand_type, expected, position)?;
        operand_type = expected.clone();
    }
    let left = bind_expr_multi(left, inputs, Some(&operand_type), allow_aggregate)?;
    let right = bind_expr_multi(right, inputs, Some(&operand_type), allow_aggregate)?;
    Ok(BoundExpr {
        nullable: left.nullable || right.nullable,
        data_type: if is_arithmetic_operator(op) {
            operand_type
        } else {
            ScalarType::Boolean
        },
        kind: BoundExprKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
    })
}

fn infer_multi_type(expr: &ParsedExpr, inputs: &[InputColumn]) -> Result<Option<ScalarType>> {
    match &expr.kind {
        ParsedExprKind::Column(column) => Ok(Some(
            resolve_input_column(column, inputs)?.data_type.clone(),
        )),
        ParsedExprKind::Literal(value) => Ok(value.scalar_type()),
        ParsedExprKind::Parameter(_) => Ok(None),
        ParsedExprKind::ResolvedParameter { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::Unary { op, expr } => match op {
            UnaryOperator::Not => Ok(Some(ScalarType::Boolean)),
            UnaryOperator::Negate => infer_multi_type(expr, inputs),
        },
        ParsedExprKind::Cast { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::Array { elements, .. } => {
            let mut element_type = None;
            for element in elements {
                let Some(candidate) = infer_multi_type(element, inputs)? else {
                    continue;
                };
                element_type = Some(match element_type {
                    Some(current) => common_type(&current, &candidate).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "array element types {current:?} and {candidate:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(expr.position)
                    })?,
                    None => candidate,
                });
            }
            Ok(element_type.map(|element| ScalarType::Array {
                element: Box::new(element),
            }))
        }
        ParsedExprKind::Function {
            function,
            arguments,
        } => infer_scalar_function_type(
            *function,
            arguments,
            |argument| infer_multi_type(argument, inputs),
            expr.position,
        ),
        ParsedExprKind::Binary { left, op, right } => {
            if is_arithmetic_operator(*op) {
                let left = infer_multi_type(left, inputs)?;
                let right = infer_multi_type(right, inputs)?;
                Ok(match (left, right) {
                    (Some(left), Some(right)) => common_type(&left, &right),
                    (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                    (None, None) => None,
                })
            } else {
                Ok(Some(ScalarType::Boolean))
            }
        }
        ParsedExprKind::InList { .. }
        | ParsedExprKind::Exists { .. }
        | ParsedExprKind::InSubquery { .. }
        | ParsedExprKind::QuantifiedSubquery { .. }
        | ParsedExprKind::RowSubquery { .. } => Ok(Some(ScalarType::Boolean)),
        ParsedExprKind::ScalarSubquery(_) => Ok(None),
        ParsedExprKind::ApplyValue { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::WindowValue { .. } => Ok(Some(ScalarType::Int64)),
        ParsedExprKind::Window { .. } => Err(DbError::internal(
            "window expression reached type inference before lowering",
        )),
        ParsedExprKind::NamedWindow { .. } => {
            Err(DbError::internal("named window reference was not resolved"))
        }
        ParsedExprKind::Aggregate {
            function, argument, ..
        } => match function {
            AggregateFunction::Count => Ok(Some(ScalarType::Int64)),
            AggregateFunction::Avg => Ok(Some(ScalarType::Float64)),
            AggregateFunction::Sum => {
                let data_type = argument
                    .as_ref()
                    .ok_or_else(|| {
                        DbError::new(DATATYPE_MISMATCH, "aggregate requires an argument")
                            .with_position_opt(expr.position)
                    })
                    .and_then(|argument| infer_multi_type(argument, inputs))?;
                Ok(data_type.map(|data_type| match data_type {
                    ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64 => ScalarType::Int64,
                    ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
                    other => other,
                }))
            }
            AggregateFunction::Min | AggregateFunction::Max => argument
                .as_ref()
                .ok_or_else(|| {
                    DbError::new(DATATYPE_MISMATCH, "aggregate requires an argument")
                        .with_position_opt(expr.position)
                })
                .and_then(|argument| infer_multi_type(argument, inputs)),
        },
    }
}

fn resolve_input_column<'a>(
    name: &ParsedObjectName,
    inputs: &'a [InputColumn],
) -> Result<&'a InputColumn> {
    let (qualifier, column, position) = match name.parts.as_slice() {
        [column] => (None, &column.name, column.position),
        [qualifier, column] => (Some(&qualifier.name), &column.name, column.position),
        _ => {
            return unsupported_at(
                "column references may contain at most a table qualifier",
                name.parts.first().and_then(|part| part.position),
            );
        }
    };
    let matches = inputs
        .iter()
        .filter(|input| {
            &input.name == column && qualifier.is_none_or(|qualifier| &input.binding == qualifier)
        })
        .collect::<Vec<_>>();
    let local = matches
        .iter()
        .copied()
        .filter(|input| input.outer_depth == 0)
        .collect::<Vec<_>>();
    let visible = if local.is_empty() { &matches } else { &local };
    match visible.as_slice() {
        [column] => Ok(*column),
        [] => Err(
            DbError::new(UNDEFINED_COLUMN, format!("column {column} does not exist"))
                .with_position_opt(position),
        ),
        _ => Err(
            DbError::new("42702", format!("column reference {column} is ambiguous"))
                .with_position_opt(position),
        ),
    }
}

fn bound_expr_has_aggregate(expr: &BoundExpr) -> bool {
    match &expr.kind {
        BoundExprKind::Aggregate { .. } => true,
        BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => {
            bound_expr_has_aggregate(expr)
        }
        BoundExprKind::Array { elements, .. } => elements.iter().any(bound_expr_has_aggregate),
        BoundExprKind::Function { arguments, .. } => arguments.iter().any(bound_expr_has_aggregate),
        BoundExprKind::Binary { left, right, .. } => {
            bound_expr_has_aggregate(left) || bound_expr_has_aggregate(right)
        }
        BoundExprKind::InList { expr, list, .. } => {
            bound_expr_has_aggregate(expr) || list.iter().any(bound_expr_has_aggregate)
        }
        BoundExprKind::Column { .. }
        | BoundExprKind::Literal(_)
        | BoundExprKind::Parameter { .. }
        | BoundExprKind::Correlation { .. }
        | BoundExprKind::ApplyValue { .. } => false,
    }
}

fn bound_window_input_has_aggregate(window: &BoundWindow) -> bool {
    window.arguments.iter().any(bound_expr_has_aggregate)
        || window.filter.as_ref().is_some_and(bound_expr_has_aggregate)
        || window.partition_by.iter().any(bound_expr_has_aggregate)
        || window.order_by.iter().any(|order| {
            order
                .expression
                .as_ref()
                .is_some_and(bound_expr_has_aggregate)
        })
}

fn window_ordinal_for_expr(expr: &BoundExpr, windows: &[BoundWindow]) -> Option<usize> {
    let BoundExprKind::ApplyValue { index } = expr.kind else {
        return None;
    };
    windows
        .iter()
        .position(|window| window.value_index == index)
}

fn bound_expr_has_window_slot(expr: &BoundExpr, windows: &[BoundWindow]) -> bool {
    let mut pending = vec![expr];
    while let Some(expression) = pending.pop() {
        match &expression.kind {
            BoundExprKind::ApplyValue { index }
                if windows.iter().any(|window| window.value_index == *index) =>
            {
                return true;
            }
            BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => pending.push(expr),
            BoundExprKind::Array { elements, .. } => pending.extend(elements),
            BoundExprKind::Function { arguments, .. } => pending.extend(arguments),
            BoundExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundExprKind::InList { expr, list, .. } => {
                pending.extend(list);
                pending.push(expr);
            }
            BoundExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            BoundExprKind::Column { .. }
            | BoundExprKind::Literal(_)
            | BoundExprKind::Parameter { .. }
            | BoundExprKind::Correlation { .. }
            | BoundExprKind::ApplyValue { .. } => {}
        }
    }
    false
}

fn remap_grouped_window_inputs(
    windows: &mut [BoundWindow],
    projection: &[BoundProjection],
    group_by: &[BoundExpr],
) -> Result<()> {
    let base_projection = projection
        .iter()
        .filter(|projection| window_ordinal_for_expr(&projection.expr, windows).is_none())
        .collect::<Vec<_>>();
    for window in windows {
        for argument in &mut window.arguments {
            *argument = remap_grouped_window_expr(argument, &base_projection, group_by)?;
        }
        if let Some(filter) = &mut window.filter {
            *filter = remap_grouped_window_expr(filter, &base_projection, group_by)?;
        }
        for expression in &mut window.partition_by {
            *expression = remap_grouped_window_expr(expression, &base_projection, group_by)?;
        }
        for order in &mut window.order_by {
            let expression = if let Some(expression) = &order.expression {
                expression.clone()
            } else {
                base_projection
                    .iter()
                    .find_map(|projection| match projection.expr.kind {
                        BoundExprKind::Column { index } if index == order.column_index => {
                            Some(projection.expr.clone())
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        DbError::new(
                            FEATURE_NOT_SUPPORTED,
                            "grouped window ORDER BY expression must appear in the select list",
                        )
                    })?
            };
            let expression = remap_grouped_window_expr(&expression, &base_projection, group_by)?;
            if let BoundExprKind::Column { index } = expression.kind {
                order.column_index = index;
                order.expression = None;
            } else {
                order.column_index = usize::MAX;
                order.expression = Some(expression);
            }
        }
    }
    Ok(())
}
