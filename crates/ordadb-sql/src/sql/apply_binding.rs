
fn projected_order_position(
    expr: &ParsedExpr,
    projection: &[BoundProjection],
) -> Result<Option<usize>> {
    let ordinal = match &expr.kind {
        ParsedExprKind::Literal(Value::Int16(value)) => Some(i64::from(*value)),
        ParsedExprKind::Literal(Value::Int32(value)) => Some(i64::from(*value)),
        ParsedExprKind::Literal(Value::Int64(value)) => Some(*value),
        _ => None,
    };
    if let Some(ordinal) = ordinal {
        if ordinal <= 0 {
            return Err(
                DbError::new("42P10", "ORDER BY position must be greater than zero")
                    .with_position_opt(expr.position),
            );
        }
        let position = usize::try_from(ordinal - 1)
            .map_err(|_| DbError::new("22003", "ORDER BY position is out of range"))?;
        if position >= projection.len() {
            return Err(DbError::new(
                "42P10",
                format!("ORDER BY position {ordinal} is not in select list"),
            )
            .with_position_opt(expr.position));
        }
        return Ok(Some(position));
    }
    let ParsedExprKind::Column(name) = &expr.kind else {
        return Ok(None);
    };
    let [name] = name.parts.as_slice() else {
        return Ok(None);
    };
    let matches = projection
        .iter()
        .enumerate()
        .filter(|(_, projection)| projection.field.name == name.name.as_str())
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [position] => Ok(Some(*position)),
        _ => Err(
            DbError::new("42702", format!("ORDER BY {} is ambiguous", name.name))
                .with_position_opt(name.position),
        ),
    }
}

fn bound_expression_order(order: ParsedOrder, expression: BoundExpr) -> Result<BoundOrder> {
    if expression.data_type == ScalarType::Json {
        return Err(DbError::new(
            "42883",
            "could not identify an ordering operator for type json",
        )
        .with_position_opt(order.expr.position));
    }
    let data_type = expression.data_type.clone();
    let (column_index, expression) = match &expression.kind {
        BoundExprKind::Column { index } => (*index, None),
        _ => (usize::MAX, Some(expression)),
    };
    Ok(BoundOrder {
        column_index,
        expression,
        data_type,
        ascending: order.ascending,
        nulls_first: order.nulls_first,
    })
}

fn bind_apply_query(
    statement: ParsedStatement,
    catalog: &Catalog,
    view_depth: usize,
    outer_inputs: &[InputColumn],
) -> Result<BoundStatement> {
    match statement {
        ParsedStatement::Select {
            table,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => bind_advanced_select(
            AdvancedSelectInput {
                table: ParsedTable {
                    name: table,
                    alias: None,
                },
                joins: Vec::new(),
                projection,
                distinct: false,
                filter,
                group_by: Vec::new(),
                having: None,
                order_by,
                offset,
                limit,
            },
            catalog,
            view_depth,
            outer_inputs,
        ),
        ParsedStatement::AdvancedSelect {
            table,
            joins,
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
        } => bind_advanced_select(
            AdvancedSelectInput {
                table,
                joins,
                projection,
                distinct,
                filter,
                group_by,
                having,
                order_by,
                offset,
                limit,
            },
            catalog,
            view_depth,
            outer_inputs,
        ),
        statement => bind_with_view_depth(statement, catalog, view_depth),
    }
}

#[allow(clippy::too_many_arguments)]
fn lower_apply_expr(
    mut expr: ParsedExpr,
    catalog: &Catalog,
    inputs: &[InputColumn],
    apply_base: usize,
    applies: &mut Vec<BoundApply>,
    view_depth: usize,
) -> Result<ParsedExpr> {
    let position = expr.position;
    expr.kind = match expr.kind {
        ParsedExprKind::Unary { op, expr } => ParsedExprKind::Unary {
            op,
            expr: Box::new(lower_apply_expr(
                *expr, catalog, inputs, apply_base, applies, view_depth,
            )?),
        },
        ParsedExprKind::Cast {
            expr,
            data_type,
            declared_type,
        } => ParsedExprKind::Cast {
            expr: Box::new(lower_apply_expr(
                *expr, catalog, inputs, apply_base, applies, view_depth,
            )?),
            data_type,
            declared_type,
        },
        ParsedExprKind::Array {
            elements,
            dimensions,
        } => ParsedExprKind::Array {
            elements: elements
                .into_iter()
                .map(|expr| {
                    lower_apply_expr(expr, catalog, inputs, apply_base, applies, view_depth)
                })
                .collect::<Result<Vec<_>>>()?,
            dimensions,
        },
        ParsedExprKind::Function {
            function,
            arguments,
        } => ParsedExprKind::Function {
            function,
            arguments: arguments
                .into_iter()
                .map(|expr| {
                    lower_apply_expr(expr, catalog, inputs, apply_base, applies, view_depth)
                })
                .collect::<Result<Vec<_>>>()?,
        },
        ParsedExprKind::Binary { left, op, right } => ParsedExprKind::Binary {
            left: Box::new(lower_apply_expr(
                *left, catalog, inputs, apply_base, applies, view_depth,
            )?),
            op,
            right: Box::new(lower_apply_expr(
                *right, catalog, inputs, apply_base, applies, view_depth,
            )?),
        },
        ParsedExprKind::InList {
            expr,
            list,
            negated,
        } => ParsedExprKind::InList {
            expr: Box::new(lower_apply_expr(
                *expr, catalog, inputs, apply_base, applies, view_depth,
            )?),
            list: list
                .into_iter()
                .map(|expr| {
                    lower_apply_expr(expr, catalog, inputs, apply_base, applies, view_depth)
                })
                .collect::<Result<Vec<_>>>()?,
            negated,
        },
        ParsedExprKind::Aggregate {
            function,
            argument,
            distinct,
            filter,
        } => ParsedExprKind::Aggregate {
            function,
            argument: argument
                .map(|argument| {
                    lower_apply_expr(*argument, catalog, inputs, apply_base, applies, view_depth)
                        .map(Box::new)
                })
                .transpose()?,
            distinct,
            filter: filter
                .map(|filter| {
                    lower_apply_expr(*filter, catalog, inputs, apply_base, applies, view_depth)
                        .map(Box::new)
                })
                .transpose()?,
        },
        ParsedExprKind::ScalarSubquery(subquery) => {
            let query = bind_apply_query(*subquery, catalog, view_depth.saturating_add(1), inputs)?;
            let field = scalar_subquery_field(&query, position)?;
            let index = push_bound_apply(applies, apply_base, BoundApplyKind::Scalar, query)?;
            ParsedExprKind::ApplyValue {
                index,
                data_type: field.data_type,
                nullable: true,
            }
        }
        ParsedExprKind::Exists { subquery, negated } => {
            let query = bind_apply_query(*subquery, catalog, view_depth.saturating_add(1), inputs)?;
            let index = push_bound_apply(
                applies,
                apply_base,
                BoundApplyKind::Exists { negated },
                query,
            )?;
            ParsedExprKind::ApplyValue {
                index,
                data_type: ScalarType::Boolean,
                nullable: false,
            }
        }
        ParsedExprKind::InSubquery {
            expr: left,
            subquery,
            negated,
        } => {
            let left = lower_apply_expr(*left, catalog, inputs, apply_base, applies, view_depth)?;
            let query = bind_apply_query(*subquery, catalog, view_depth.saturating_add(1), inputs)?;
            let field = scalar_subquery_field(&query, position)?;
            let left_type = infer_multi_type(&left, inputs)?;
            let operand_type = left_type
                .as_ref()
                .map(|left_type| common_type(left_type, &field.data_type))
                .unwrap_or_else(|| Some(field.data_type.clone()))
                .ok_or_else(|| {
                    DbError::new(
                        DATATYPE_MISMATCH,
                        format!(
                            "IN types {:?} and {:?} cannot be matched",
                            left_type, field.data_type
                        ),
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
            let left = bind_expr_multi(left, inputs, Some(&operand_type), false)?;
            let index = push_bound_apply(
                applies,
                apply_base,
                BoundApplyKind::In { left, negated },
                query,
            )?;
            ParsedExprKind::ApplyValue {
                index,
                data_type: ScalarType::Boolean,
                nullable: true,
            }
        }
        ParsedExprKind::QuantifiedSubquery {
            left,
            op,
            quantifier,
            subquery,
        } => {
            let left = lower_apply_expr(*left, catalog, inputs, apply_base, applies, view_depth)?;
            let query = bind_apply_query(*subquery, catalog, view_depth.saturating_add(1), inputs)?;
            let field = scalar_subquery_field(&query, position)?;
            let left_type = infer_multi_type(&left, inputs)?;
            let operand_type = left_type
                .as_ref()
                .map(|left_type| common_type(left_type, &field.data_type))
                .unwrap_or_else(|| Some(field.data_type.clone()))
                .ok_or_else(|| {
                    DbError::new(
                        DATATYPE_MISMATCH,
                        format!(
                            "quantified comparison types {:?} and {:?} cannot be matched",
                            left_type, field.data_type
                        ),
                    )
                    .with_position_opt(position)
                })?;
            if operand_type == ScalarType::Json {
                return Err(DbError::new(
                    "42883",
                    "could not identify a comparison operator for type json",
                )
                .with_position_opt(position));
            }
            let left = bind_expr_multi(left, inputs, Some(&operand_type), false)?;
            let index = push_bound_apply(
                applies,
                apply_base,
                BoundApplyKind::Quantified {
                    left,
                    op,
                    quantifier,
                },
                query,
            )?;
            ParsedExprKind::ApplyValue {
                index,
                data_type: ScalarType::Boolean,
                nullable: true,
            }
        }
        ParsedExprKind::RowSubquery {
            left,
            op,
            quantifier,
            negated,
            subquery,
        } => {
            let left = left
                .into_iter()
                .map(|expression| {
                    lower_apply_expr(expression, catalog, inputs, apply_base, applies, view_depth)
                })
                .collect::<Result<Vec<_>>>()?;
            let query = bind_apply_query(*subquery, catalog, view_depth.saturating_add(1), inputs)?;
            let schema = bound_query_schema(&query)?;
            if left.len() != schema.fields.len() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "unequal number of entries in row expressions",
                )
                .with_position_opt(position));
            }
            let mut bound_left = Vec::with_capacity(left.len());
            let mut operand_types = Vec::with_capacity(left.len());
            for (expression, field) in left.into_iter().zip(&schema.fields) {
                let left_type = infer_multi_type(&expression, inputs)?;
                let operand_type = left_type
                    .as_ref()
                    .map(|left_type| common_type(left_type, &field.data_type))
                    .unwrap_or_else(|| Some(field.data_type.clone()))
                    .ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "row comparison types {:?} and {:?} cannot be matched",
                                left_type, field.data_type
                            ),
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
                bound_left.push(bind_expr_multi(
                    expression,
                    inputs,
                    Some(&operand_type),
                    false,
                )?);
                operand_types.push(operand_type);
            }
            let kind = match quantifier {
                Some(quantifier) => BoundApplyKind::RowQuantified {
                    left: bound_left,
                    op,
                    quantifier,
                    negated,
                    operand_types,
                },
                None if !negated => BoundApplyKind::RowScalar {
                    left: bound_left,
                    op,
                    operand_types,
                },
                None => {
                    return Err(DbError::internal(
                        "scalar row subquery retained a negated quantifier flag",
                    ));
                }
            };
            let index = push_bound_apply(applies, apply_base, kind, query)?;
            ParsedExprKind::ApplyValue {
                index,
                data_type: ScalarType::Boolean,
                nullable: true,
            }
        }
        kind => kind,
    };
    Ok(expr)
}

fn scalar_subquery_field(statement: &BoundStatement, position: Option<usize>) -> Result<Field> {
    let schema = bound_query_schema(statement)?;
    let [field] = schema.fields.as_slice() else {
        return Err(
            DbError::new(SYNTAX_ERROR, "subquery must return only one column")
                .with_position_opt(position),
        );
    };
    Ok(field.clone())
}

fn push_bound_apply(
    applies: &mut Vec<BoundApply>,
    apply_base: usize,
    kind: BoundApplyKind,
    query: BoundStatement,
) -> Result<usize> {
    let index = apply_base
        .checked_add(applies.len())
        .ok_or_else(|| DbError::new("54001", "Apply value index overflowed"))?;
    applies.push(BoundApply {
        kind,
        query: Box::new(query),
    });
    Ok(index)
}

struct BoundWindowCall {
    function: WindowFunction,
    arguments: Vec<BoundExpr>,
    count_star: bool,
    filter: Option<BoundExpr>,
    data_type: ScalarType,
    nullable: bool,
}

fn bind_window_call(
    call: ParsedWindowCall,
    inputs: &[InputColumn],
    position: Option<usize>,
) -> Result<BoundWindowCall> {
    if call.arguments.iter().any(expr_has_window)
        || call.filter.as_deref().is_some_and(expr_has_window)
    {
        return Err(
            DbError::new("42P20", "window function calls cannot be nested")
                .with_position_opt(position),
        );
    }
    if call.arguments.iter().any(expr_has_subquery)
        || call.filter.as_deref().is_some_and(expr_has_subquery)
    {
        return unsupported_at(
            "subquery expressions in window function arguments are not supported yet",
            position,
        );
    }
    if let WindowFunction::Aggregate(function) = call.function {
        let argument = match call.arguments.into_iter().collect::<Vec<_>>().as_slice() {
            [] if call.count_star && function == AggregateFunction::Count => None,
            [argument] if !call.count_star => {
                Some(bind_expr_multi(argument.clone(), inputs, None, true)?)
            }
            _ => {
                return Err(DbError::internal(
                    "aggregate window argument shape changed after parsing",
                ));
            }
        };
        let filter = call
            .filter
            .map(|filter| bind_expr_multi(*filter, inputs, Some(&ScalarType::Boolean), true))
            .transpose()?;
        let (data_type, nullable) = match (function, argument.as_ref()) {
            (AggregateFunction::Count, _) => (ScalarType::Int64, false),
            (AggregateFunction::Avg, Some(argument)) if is_numeric(&argument.data_type) => {
                (ScalarType::Float64, true)
            }
            (AggregateFunction::Sum, Some(argument)) if is_numeric(&argument.data_type) => {
                let data_type = match argument.data_type {
                    ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64 => ScalarType::Int64,
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
                    "aggregate window argument has an incompatible type",
                )
                .with_position_opt(position));
            }
        };
        return Ok(BoundWindowCall {
            function: call.function,
            arguments: argument.into_iter().collect(),
            count_star: call.count_star,
            filter,
            data_type,
            nullable,
        });
    }

    let mut arguments = call.arguments.into_iter();
    let first = match call.function {
        WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => None,
        WindowFunction::Lag
        | WindowFunction::Lead
        | WindowFunction::FirstValue
        | WindowFunction::LastValue
        | WindowFunction::NthValue => Some(bind_expr_multi(
            arguments
                .next()
                .ok_or_else(|| DbError::internal("value window function argument disappeared"))?,
            inputs,
            None,
            true,
        )?),
        WindowFunction::Aggregate(_) => unreachable!("aggregate handled above"),
    };
    let mut bound_arguments = first.iter().cloned().collect::<Vec<_>>();
    match call.function {
        WindowFunction::Lag | WindowFunction::Lead => {
            if let Some(offset) = arguments.next() {
                bound_arguments.push(bind_expr_multi(
                    offset,
                    inputs,
                    Some(&ScalarType::Int64),
                    false,
                )?);
            }
            if let Some(default) = arguments.next() {
                let data_type = &first
                    .as_ref()
                    .ok_or_else(|| DbError::internal("window value type disappeared"))?
                    .data_type;
                bound_arguments.push(bind_expr_multi(default, inputs, Some(data_type), false)?);
            }
        }
        WindowFunction::NthValue => {
            bound_arguments.push(bind_expr_multi(
                arguments
                    .next()
                    .ok_or_else(|| DbError::internal("NTH_VALUE offset disappeared"))?,
                inputs,
                Some(&ScalarType::Int64),
                false,
            )?);
        }
        WindowFunction::RowNumber
        | WindowFunction::Rank
        | WindowFunction::DenseRank
        | WindowFunction::FirstValue
        | WindowFunction::LastValue => {}
        WindowFunction::Aggregate(_) => unreachable!("aggregate handled above"),
    }
    if arguments.next().is_some() {
        return Err(DbError::internal(
            "window function retained an unexpected argument",
        ));
    }
    let (data_type, nullable) = first.map_or((ScalarType::Int64, false), |argument| {
        (argument.data_type, true)
    });
    Ok(BoundWindowCall {
        function: call.function,
        arguments: bound_arguments,
        count_star: false,
        filter: None,
        data_type,
        nullable,
    })
}

fn bind_window_frame(
    frame: ParsedWindowFrame,
    inputs: &[InputColumn],
    order_by: &[BoundOrder],
    position: Option<usize>,
) -> Result<BoundWindowFrame> {
    let offset_type = match frame.units {
        WindowFrameUnits::Rows => ScalarType::Int64,
        WindowFrameUnits::Range => {
            let has_offset = matches!(
                frame.start_bound,
                ParsedWindowFrameBound::Preceding(_) | ParsedWindowFrameBound::Following(_)
            ) || matches!(
                frame.end_bound,
                ParsedWindowFrameBound::Preceding(_) | ParsedWindowFrameBound::Following(_)
            );
            if !has_offset {
                ScalarType::Int64
            } else {
                let [order] = order_by else {
                    return Err(DbError::new(
                        "42P20",
                        "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column",
                    )
                    .with_position_opt(position));
                };
                let data_type = if let Some(expression) = &order.expression {
                    expression.data_type.clone()
                } else {
                    inputs
                        .get(order.column_index)
                        .map(|input| input.data_type.clone())
                        .ok_or_else(|| {
                            DbError::internal("window ORDER BY type index is out of bounds")
                        })?
                };
                if !is_numeric(&data_type) {
                    return Err(DbError::new(
                        "42883",
                        "RANGE offset is supported only for numeric ORDER BY expressions",
                    )
                    .with_position_opt(position));
                }
                data_type
            }
        }
    };
    Ok(BoundWindowFrame {
        units: frame.units,
        start_bound: bind_window_frame_bound(frame.start_bound, &offset_type, position)?,
        end_bound: bind_window_frame_bound(frame.end_bound, &offset_type, position)?,
    })
}

fn bind_window_frame_bound(
    bound: ParsedWindowFrameBound,
    offset_type: &ScalarType,
    position: Option<usize>,
) -> Result<BoundWindowFrameBound> {
    let bind_offset = |offset: ParsedExpr| {
        if expr_has_aggregate(&offset) || expr_has_subquery(&offset) || expr_has_window(&offset) {
            return Err(DbError::new(
                "42P20",
                "window frame offset cannot contain aggregate, window, or subquery expressions",
            )
            .with_position_opt(offset.position.or(position)));
        }
        bind_expr_multi(offset, &[], Some(offset_type), false).map_err(|error| {
            if matches!(error.sql_state.as_str(), "42703" | "42P01") {
                DbError::new("42P20", "window frame offset cannot contain variables")
                    .with_position_opt(error.position.or(position))
            } else {
                error
            }
        })
    };
    Ok(match bound {
        ParsedWindowFrameBound::UnboundedPreceding => BoundWindowFrameBound::UnboundedPreceding,
        ParsedWindowFrameBound::Preceding(offset) => {
            BoundWindowFrameBound::Preceding(bind_offset(*offset)?)
        }
        ParsedWindowFrameBound::CurrentRow => BoundWindowFrameBound::CurrentRow,
        ParsedWindowFrameBound::Following(offset) => {
            BoundWindowFrameBound::Following(bind_offset(*offset)?)
        }
        ParsedWindowFrameBound::UnboundedFollowing => BoundWindowFrameBound::UnboundedFollowing,
    })
}
