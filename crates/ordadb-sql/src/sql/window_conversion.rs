
fn convert_scalar_function_arguments(
    arguments: FunctionArguments,
    sql: &str,
    position: Option<usize>,
) -> Result<Vec<ParsedExpr>> {
    let arguments = match arguments {
        FunctionArguments::None => return Ok(Vec::new()),
        FunctionArguments::List(arguments) => arguments,
        _ => {
            return unsupported_at("scalar function arguments must use parentheses", position);
        }
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return unsupported_at(
            "this scalar function argument option is not supported",
            position,
        );
    }
    arguments
        .args
        .into_iter()
        .map(|argument| match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => {
                convert_expr(expression, sql)
            }
            _ => unsupported_at(
                "scalar functions require positional expression arguments",
                position,
            ),
        })
        .collect()
}

fn validate_scalar_function_arity(
    function: ScalarFunction,
    count: usize,
    position: Option<usize>,
) -> Result<()> {
    let valid = match function {
        ScalarFunction::Version
        | ScalarFunction::CurrentDatabase
        | ScalarFunction::CurrentUser
        | ScalarFunction::SessionUser => count == 0,
        ScalarFunction::CurrentSetting => matches!(count, 1 | 2),
        ScalarFunction::Lower
        | ScalarFunction::Upper
        | ScalarFunction::CharacterLength
        | ScalarFunction::OctetLength
        | ScalarFunction::Abs
        | ScalarFunction::JsonbTypeof
        | ScalarFunction::Cardinality => count == 1,
        ScalarFunction::NullIf | ScalarFunction::ArrayLength | ScalarFunction::Strpos => count == 2,
        ScalarFunction::Btrim | ScalarFunction::Ltrim | ScalarFunction::Rtrim => {
            matches!(count, 1 | 2)
        }
        ScalarFunction::Replace => count == 3,
        ScalarFunction::Substring => matches!(count, 2 | 3),
        ScalarFunction::Coalesce
        | ScalarFunction::Concat
        | ScalarFunction::Greatest
        | ScalarFunction::Least => count > 0,
    };
    if valid {
        Ok(())
    } else {
        Err(DbError::new(
            "42883",
            format!("function {function:?} does not accept {count} arguments"),
        )
        .with_position_opt(position))
    }
}

fn interval_literal_text(expression: SqlExpr, position: Option<usize>) -> Result<String> {
    let SqlExpr::Value(value) = expression else {
        return unsupported_at("INTERVAL requires a string literal", position);
    };
    match value.value {
        SqlValue::SingleQuotedString(value)
        | SqlValue::EscapedStringLiteral(value)
        | SqlValue::UnicodeStringLiteral(value)
        | SqlValue::NationalStringLiteral(value) => Ok(value),
        _ => unsupported_at("INTERVAL requires a string literal", position),
    }
}

fn convert_array_expression(
    array: SqlArray,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedExprKind> {
    if !array.named {
        return unsupported_at("array constructors must use ARRAY[...]", position);
    }
    let (elements, dimensions) = flatten_array_elements(array.elem, sql, position, 0)?;
    Ok(ParsedExprKind::Array {
        elements,
        dimensions,
    })
}

fn flatten_array_elements(
    expressions: Vec<SqlExpr>,
    sql: &str,
    position: Option<usize>,
    depth: usize,
) -> Result<(Vec<ParsedExpr>, Vec<ArrayDimension>)> {
    const MAX_ARRAY_DIMENSIONS: usize = 6;
    const MAX_ARRAY_ELEMENTS: usize = 1_000_000;
    if depth >= MAX_ARRAY_DIMENSIONS {
        return Err(DbError::new(
            "54000",
            format!("array exceeds the maximum of {MAX_ARRAY_DIMENSIONS} dimensions"),
        )
        .with_position_opt(position));
    }
    if expressions.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let nested = matches!(expressions.first(), Some(SqlExpr::Array(_)));
    if expressions
        .iter()
        .any(|expression| matches!(expression, SqlExpr::Array(_)) != nested)
    {
        return Err(DbError::new(
            "2202E",
            "multidimensional arrays must have matching dimensions",
        )
        .with_position_opt(position));
    }
    let length = u32::try_from(expressions.len()).map_err(|_| {
        DbError::new("54000", "array dimension is too large").with_position_opt(position)
    })?;
    if !nested {
        if expressions.len() > MAX_ARRAY_ELEMENTS {
            return Err(DbError::new(
                "54000",
                format!("array exceeds the maximum of {MAX_ARRAY_ELEMENTS} elements"),
            )
            .with_position_opt(position));
        }
        return Ok((
            expressions
                .into_iter()
                .map(|expression| convert_expr(expression, sql))
                .collect::<Result<Vec<_>>>()?,
            vec![ArrayDimension::new(length, 1)],
        ));
    }

    let mut flattened = Vec::new();
    let mut child_dimensions: Option<Vec<ArrayDimension>> = None;
    for expression in expressions {
        let SqlExpr::Array(child) = expression else {
            return Err(DbError::internal(
                "validated nested array lost its child array",
            ));
        };
        let (mut child_elements, dimensions) =
            flatten_array_elements(child.elem, sql, position, depth + 1)?;
        if child_dimensions
            .as_ref()
            .is_some_and(|expected| expected != &dimensions)
        {
            return Err(DbError::new(
                "2202E",
                "multidimensional arrays must have matching dimensions",
            )
            .with_position_opt(position));
        }
        child_dimensions.get_or_insert(dimensions);
        flattened.append(&mut child_elements);
        if flattened.len() > MAX_ARRAY_ELEMENTS {
            return Err(DbError::new(
                "54000",
                format!("array exceeds the maximum of {MAX_ARRAY_ELEMENTS} elements"),
            )
            .with_position_opt(position));
        }
    }
    let mut dimensions = vec![ArrayDimension::new(length, 1)];
    dimensions.extend(child_dimensions.unwrap_or_default());
    Ok((flattened, dimensions))
}

fn convert_row_items(
    expressions: Vec<SqlExpr>,
    sql: &str,
    position: Option<usize>,
) -> Result<Vec<ParsedExpr>> {
    if expressions.is_empty() {
        return Err(
            DbError::new(SYNTAX_ERROR, "row value must not be empty").with_position_opt(position)
        );
    }
    expressions
        .into_iter()
        .map(|expression| convert_expr(expression, sql))
        .collect()
}

fn row_comparison_operator(
    operator: BinaryOperator,
    position: Option<usize>,
) -> Result<BinaryOperator> {
    match operator {
        BinaryOperator::Eq | BinaryOperator::NotEq => Ok(operator),
        _ => unsupported_at("ordered row comparisons are not supported yet", position),
    }
}

fn convert_row_comparison(
    left: Vec<SqlExpr>,
    operator: BinaryOperator,
    right: Vec<SqlExpr>,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedExpr> {
    let operator = row_comparison_operator(operator, position)?;
    build_row_comparison(
        convert_row_items(left, sql, position)?,
        operator,
        convert_row_items(right, sql, position)?,
        position,
    )
}

fn convert_row_in_list(
    left: Vec<SqlExpr>,
    list: Vec<SqlExpr>,
    negated: bool,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedExpr> {
    let left = convert_row_items(left, sql, position)?;
    let mut comparisons = Vec::with_capacity(list.len());
    for candidate in list {
        let SqlExpr::Tuple(candidate) = candidate else {
            return Err(
                DbError::new(SYNTAX_ERROR, "row IN list entries must all be row values")
                    .with_position_opt(position),
            );
        };
        comparisons.push(build_row_comparison(
            left.clone(),
            BinaryOperator::Eq,
            convert_row_items(candidate, sql, position)?,
            position,
        )?);
    }
    let mut comparisons = comparisons.into_iter();
    let mut expression = comparisons
        .next()
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "row IN list must not be empty"))?;
    for candidate in comparisons {
        expression = ParsedExpr {
            position,
            kind: ParsedExprKind::Binary {
                left: Box::new(expression),
                op: BinaryOperator::Or,
                right: Box::new(candidate),
            },
        };
    }
    if negated {
        expression = ParsedExpr {
            position,
            kind: ParsedExprKind::Unary {
                op: UnaryOperator::Not,
                expr: Box::new(expression),
            },
        };
    }
    Ok(expression)
}

fn build_row_comparison(
    left: Vec<ParsedExpr>,
    operator: BinaryOperator,
    right: Vec<ParsedExpr>,
    position: Option<usize>,
) -> Result<ParsedExpr> {
    if left.len() != right.len() {
        return Err(
            DbError::new(SYNTAX_ERROR, "unequal number of entries in row expressions")
                .with_position_opt(position),
        );
    }
    let mut comparisons = left.into_iter().zip(right).map(|(left, right)| ParsedExpr {
        position,
        kind: ParsedExprKind::Binary {
            left: Box::new(left),
            op: BinaryOperator::Eq,
            right: Box::new(right),
        },
    });
    let mut expression = comparisons
        .next()
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "row value must not be empty"))?;
    for comparison in comparisons {
        expression = ParsedExpr {
            position,
            kind: ParsedExprKind::Binary {
                left: Box::new(expression),
                op: BinaryOperator::And,
                right: Box::new(comparison),
            },
        };
    }
    if operator == BinaryOperator::NotEq {
        expression = ParsedExpr {
            position,
            kind: ParsedExprKind::Unary {
                op: UnaryOperator::Not,
                expr: Box::new(expression),
            },
        };
    }
    Ok(expression)
}

fn convert_window_function(
    function: Function,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedExprKind> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported_at("this window function option is not supported yet", position);
    }
    let FunctionArguments::List(arguments) = function.args else {
        return unsupported_at("window function arguments must use parentheses", position);
    };
    if !arguments.clauses.is_empty() {
        return unsupported_at(
            "ordered window function arguments are not supported yet",
            position,
        );
    }
    if matches!(
        arguments.duplicate_treatment,
        Some(DuplicateTreatment::Distinct)
    ) {
        return unsupported_at("DISTINCT is not implemented for window functions", position);
    }
    let function_name = function.name.to_string().to_ascii_lowercase();
    let mut count_star = false;
    let (window_function, expected_arguments) = match function_name.as_str() {
        "row_number" => (WindowFunction::RowNumber, 0..=0),
        "rank" => (WindowFunction::Rank, 0..=0),
        "dense_rank" => (WindowFunction::DenseRank, 0..=0),
        "lag" => (WindowFunction::Lag, 1..=3),
        "lead" => (WindowFunction::Lead, 1..=3),
        "first_value" => (WindowFunction::FirstValue, 1..=1),
        "last_value" => (WindowFunction::LastValue, 1..=1),
        "nth_value" => (WindowFunction::NthValue, 2..=2),
        "count" => (WindowFunction::Aggregate(AggregateFunction::Count), 1..=1),
        "sum" => (WindowFunction::Aggregate(AggregateFunction::Sum), 1..=1),
        "avg" => (WindowFunction::Aggregate(AggregateFunction::Avg), 1..=1),
        "min" => (WindowFunction::Aggregate(AggregateFunction::Min), 1..=1),
        "max" => (WindowFunction::Aggregate(AggregateFunction::Max), 1..=1),
        _ => return unsupported_at("this window function is not supported yet", position),
    };
    let converted_arguments = match arguments.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
            if window_function == WindowFunction::Aggregate(AggregateFunction::Count) =>
        {
            count_star = true;
            Vec::new()
        }
        values => values
            .iter()
            .map(|argument| match argument {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(argument)) => {
                    convert_expr(argument.clone(), sql)
                }
                _ => unsupported_at("window function requires expression arguments", position),
            })
            .collect::<Result<Vec<_>>>()?,
    };
    if !count_star && !expected_arguments.contains(&converted_arguments.len()) {
        return Err(DbError::new(
            SYNTAX_ERROR,
            format!("invalid argument count for window function {function_name}"),
        )
        .with_position_opt(position));
    }
    let filter = function
        .filter
        .map(|filter| convert_expr(*filter, sql).map(Box::new))
        .transpose()?;
    if filter.is_some() && !matches!(window_function, WindowFunction::Aggregate(_)) {
        return Err(DbError::new(
            "42809",
            "FILTER is specified, but the window function is not an aggregate",
        )
        .with_position_opt(position));
    }
    let call = ParsedWindowCall {
        function: window_function,
        arguments: converted_arguments,
        count_star,
        filter,
    };
    let over = function
        .over
        .ok_or_else(|| DbError::internal("window function lost its OVER clause"))?;
    match over {
        WindowType::WindowSpec(spec) => Ok(ParsedExprKind::Window {
            call: Box::new(call),
            spec: Box::new(convert_window_spec(spec, sql, position)?),
        }),
        WindowType::NamedWindow(name) => Ok(ParsedExprKind::NamedWindow {
            call: Box::new(call),
            name: convert_ident(name, sql),
        }),
    }
}

fn convert_window_spec(
    spec: SqlWindowSpec,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedWindowSpec> {
    let window_name = spec.window_name.map(|name| convert_ident(name, sql));
    let partition_by = spec
        .partition_by
        .into_iter()
        .map(|expr| convert_expr(expr, sql))
        .collect::<Result<Vec<_>>>()?;
    let order_by = spec
        .order_by
        .into_iter()
        .map(|order| {
            if order.with_fill.is_some() {
                return unsupported_at("window ORDER BY WITH FILL is not supported", position);
            }
            Ok(ParsedOrder {
                expr: convert_expr(order.expr, sql)?,
                ascending: order.options.asc.unwrap_or(true),
                nulls_first: order.options.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let frame = spec
        .window_frame
        .map(|frame| convert_window_frame(frame, sql, position))
        .transpose()?;
    Ok(ParsedWindowSpec {
        window_name,
        partition_by,
        order_by,
        frame,
    })
}

fn convert_window_frame(
    frame: SqlWindowFrame,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedWindowFrame> {
    let units = match frame.units {
        SqlWindowFrameUnits::Rows => WindowFrameUnits::Rows,
        SqlWindowFrameUnits::Range => WindowFrameUnits::Range,
        SqlWindowFrameUnits::Groups => {
            return unsupported_at("GROUPS window frames are not supported yet", position);
        }
    };
    let start_bound = convert_window_frame_bound(frame.start_bound, sql)?;
    let end_bound = frame
        .end_bound
        .map(|bound| convert_window_frame_bound(bound, sql))
        .transpose()?
        .unwrap_or(ParsedWindowFrameBound::CurrentRow);
    validate_window_frame_order(&start_bound, &end_bound, position)?;
    Ok(ParsedWindowFrame {
        units,
        start_bound,
        end_bound,
    })
}

fn convert_window_frame_bound(
    bound: SqlWindowFrameBound,
    sql: &str,
) -> Result<ParsedWindowFrameBound> {
    Ok(match bound {
        SqlWindowFrameBound::CurrentRow => ParsedWindowFrameBound::CurrentRow,
        SqlWindowFrameBound::Preceding(None) => ParsedWindowFrameBound::UnboundedPreceding,
        SqlWindowFrameBound::Preceding(Some(offset)) => {
            ParsedWindowFrameBound::Preceding(Box::new(convert_expr(*offset, sql)?))
        }
        SqlWindowFrameBound::Following(None) => ParsedWindowFrameBound::UnboundedFollowing,
        SqlWindowFrameBound::Following(Some(offset)) => {
            ParsedWindowFrameBound::Following(Box::new(convert_expr(*offset, sql)?))
        }
    })
}

fn validate_window_frame_order(
    start: &ParsedWindowFrameBound,
    end: &ParsedWindowFrameBound,
    position: Option<usize>,
) -> Result<()> {
    if matches!(start, ParsedWindowFrameBound::UnboundedFollowing) {
        return Err(
            DbError::new("42P20", "frame start cannot be UNBOUNDED FOLLOWING")
                .with_position_opt(position),
        );
    }
    if matches!(end, ParsedWindowFrameBound::UnboundedPreceding) {
        return Err(
            DbError::new("42P20", "frame end cannot be UNBOUNDED PRECEDING")
                .with_position_opt(position),
        );
    }
    let rank = |bound: &ParsedWindowFrameBound| match bound {
        ParsedWindowFrameBound::UnboundedPreceding => 0_u8,
        ParsedWindowFrameBound::Preceding(_) => 1,
        ParsedWindowFrameBound::CurrentRow => 2,
        ParsedWindowFrameBound::Following(_) => 3,
        ParsedWindowFrameBound::UnboundedFollowing => 4,
    };
    if rank(start) > rank(end) {
        return Err(DbError::new(
            "42P20",
            "frame starting from following row cannot end before it",
        )
        .with_position_opt(position));
    }
    Ok(())
}

fn convert_named_windows(
    definitions: Vec<sqlparser::ast::NamedWindowDefinition>,
    sql: &str,
) -> Result<BTreeMap<Identifier, ParsedWindowSpec>> {
    let mut windows: BTreeMap<Identifier, ParsedWindowSpec> = BTreeMap::new();
    for sqlparser::ast::NamedWindowDefinition(name, definition) in definitions {
        let name = convert_ident(name, sql);
        if windows.contains_key(&name.name) {
            return Err(DbError::new(
                "42712",
                format!("window {} is specified more than once", name.name),
            )
            .with_position_opt(name.position));
        }
        let spec = match definition {
            NamedWindowExpr::NamedWindow(base) => {
                let base = convert_ident(base, sql);
                windows.get(&base.name).cloned().ok_or_else(|| {
                    DbError::new("42704", format!("window {} does not exist", base.name))
                        .with_position_opt(base.position)
                })?
            }
            NamedWindowExpr::WindowSpec(mut spec) => {
                let inherited = spec
                    .window_name
                    .take()
                    .map(|base| {
                        let base = convert_ident(base, sql);
                        windows.get(&base.name).cloned().ok_or_else(|| {
                            DbError::new("42704", format!("window {} does not exist", base.name))
                                .with_position_opt(base.position)
                        })
                    })
                    .transpose()?;
                let has_partition = !spec.partition_by.is_empty();
                let has_order = !spec.order_by.is_empty();
                let has_frame = spec.window_frame.is_some();
                let mut converted = convert_window_spec(spec, sql, name.position)?;
                if let Some(base) = inherited {
                    if has_partition {
                        return Err(DbError::new(
                            "42P20",
                            "cannot override PARTITION BY clause of named window",
                        )
                        .with_position_opt(name.position));
                    }
                    if has_order && !base.order_by.is_empty() {
                        return Err(DbError::new(
                            "42P20",
                            "cannot override ORDER BY clause of named window",
                        )
                        .with_position_opt(name.position));
                    }
                    if base.frame.is_some() {
                        return Err(DbError::new(
                            "42P20",
                            "cannot copy a window that has a frame clause",
                        )
                        .with_position_opt(name.position));
                    }
                    converted.partition_by = base.partition_by;
                    if !has_order {
                        converted.order_by = base.order_by;
                    }
                    if !has_frame {
                        converted.frame = None;
                    }
                }
                converted
            }
        };
        windows.insert(name.name, spec);
    }
    Ok(windows)
}

fn resolve_named_window_expr(
    expression: &mut ParsedExpr,
    windows: &BTreeMap<Identifier, ParsedWindowSpec>,
) -> Result<()> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        if let ParsedExprKind::NamedWindow { call, name } = &expression.kind {
            let spec = windows.get(&name.name).cloned().ok_or_else(|| {
                DbError::new("42704", format!("window {} does not exist", name.name))
                    .with_position_opt(name.position)
            })?;
            expression.kind = ParsedExprKind::Window {
                call: call.clone(),
                spec: Box::new(spec),
            };
            continue;
        }
        match &mut expression.kind {
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                pending.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => pending.extend(elements.iter_mut().rev()),
            ParsedExprKind::Function { arguments, .. } => {
                pending.extend(arguments.iter_mut().rev());
            }
            ParsedExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                pending.extend(list.iter_mut().rev());
                pending.push(expr);
            }
            ParsedExprKind::InSubquery { expr, .. }
            | ParsedExprKind::QuantifiedSubquery { left: expr, .. } => pending.push(expr),
            ParsedExprKind::RowSubquery { left, .. } => pending.extend(left.iter_mut().rev()),
            ParsedExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            ParsedExprKind::Window { call, spec } => {
                resolve_window_spec_inheritance(spec, windows, expression.position)?;
                if let Some(filter) = &mut call.filter {
                    pending.push(filter);
                }
                pending.extend(call.arguments.iter_mut().rev());
                if let Some(frame) = &mut spec.frame {
                    match &mut frame.start_bound {
                        ParsedWindowFrameBound::Preceding(offset)
                        | ParsedWindowFrameBound::Following(offset) => pending.push(offset),
                        ParsedWindowFrameBound::UnboundedPreceding
                        | ParsedWindowFrameBound::CurrentRow
                        | ParsedWindowFrameBound::UnboundedFollowing => {}
                    }
                    match &mut frame.end_bound {
                        ParsedWindowFrameBound::Preceding(offset)
                        | ParsedWindowFrameBound::Following(offset) => pending.push(offset),
                        ParsedWindowFrameBound::UnboundedPreceding
                        | ParsedWindowFrameBound::CurrentRow
                        | ParsedWindowFrameBound::UnboundedFollowing => {}
                    }
                }
                pending.extend(spec.order_by.iter_mut().map(|order| &mut order.expr));
                pending.extend(&mut spec.partition_by);
            }
            ParsedExprKind::NamedWindow { .. } => unreachable!("handled above"),
            ParsedExprKind::Column(_)
            | ParsedExprKind::Literal(_)
            | ParsedExprKind::Parameter(_)
            | ParsedExprKind::ResolvedParameter { .. }
            | ParsedExprKind::ScalarSubquery(_)
            | ParsedExprKind::Exists { .. }
            | ParsedExprKind::ApplyValue { .. }
            | ParsedExprKind::WindowValue { .. } => {}
        }
    }
    Ok(())
}

fn resolve_window_spec_inheritance(
    spec: &mut ParsedWindowSpec,
    windows: &BTreeMap<Identifier, ParsedWindowSpec>,
    position: Option<usize>,
) -> Result<()> {
    let Some(base_name) = spec.window_name.take() else {
        return Ok(());
    };
    let base = windows.get(&base_name.name).cloned().ok_or_else(|| {
        DbError::new("42704", format!("window {} does not exist", base_name.name))
            .with_position_opt(base_name.position)
    })?;
    if !spec.partition_by.is_empty() {
        return Err(DbError::new(
            "42P20",
            "cannot override PARTITION BY clause of named window",
        )
        .with_position_opt(position));
    }
    if !spec.order_by.is_empty() && !base.order_by.is_empty() {
        return Err(
            DbError::new("42P20", "cannot override ORDER BY clause of named window")
                .with_position_opt(position),
        );
    }
    if base.frame.is_some() {
        return Err(
            DbError::new("42P20", "cannot copy a window that has a frame clause")
                .with_position_opt(position),
        );
    }
    spec.partition_by = base.partition_by;
    if spec.order_by.is_empty() {
        spec.order_by = base.order_by;
    }
    Ok(())
}

fn convert_binary_operator(
    operator: SqlBinaryOperator,
    position: Option<usize>,
) -> Result<BinaryOperator> {
    match operator {
        SqlBinaryOperator::Eq => Ok(BinaryOperator::Eq),
        SqlBinaryOperator::Plus => Ok(BinaryOperator::Add),
        SqlBinaryOperator::Minus => Ok(BinaryOperator::Subtract),
        SqlBinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
        SqlBinaryOperator::Divide => Ok(BinaryOperator::Divide),
        SqlBinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
        SqlBinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
        SqlBinaryOperator::Lt => Ok(BinaryOperator::Lt),
        SqlBinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
        SqlBinaryOperator::Gt => Ok(BinaryOperator::Gt),
        SqlBinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
        SqlBinaryOperator::And => Ok(BinaryOperator::And),
        SqlBinaryOperator::Or => Ok(BinaryOperator::Or),
        _ => unsupported_at("this binary operator is not supported yet", position),
    }
}

fn convert_comparison_operator(
    operator: SqlBinaryOperator,
    position: Option<usize>,
) -> Result<BinaryOperator> {
    let operator = convert_binary_operator(operator, position)?;
    if matches!(
        operator,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    ) {
        Ok(operator)
    } else {
        unsupported_at(
            "quantified subqueries require a comparison operator",
            position,
        )
    }
}
