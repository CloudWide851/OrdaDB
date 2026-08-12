
fn bind_scalar_function(
    function: ScalarFunction,
    arguments: Vec<ParsedExpr>,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
    parameter_types: &BTreeMap<usize, ScalarType>,
    position: Option<usize>,
) -> Result<BoundExpr> {
    let inferred = infer_scalar_function_type(
        function,
        &arguments,
        |argument| infer_expr_type(argument, table, parameter_types),
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
            bind_expr_with_parameter_types(argument, table, expected, parameter_types)
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

fn bind_binary(
    left: ParsedExpr,
    op: BinaryOperator,
    right: ParsedExpr,
    table: Option<&TableDefinition>,
    position: Option<usize>,
    expected: Option<&ScalarType>,
    parameter_types: &BTreeMap<usize, ScalarType>,
) -> Result<BoundExpr> {
    if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
        let left = bind_expr_with_parameter_types(
            left,
            table,
            Some(&ScalarType::Boolean),
            parameter_types,
        )?;
        let right = bind_expr_with_parameter_types(
            right,
            table,
            Some(&ScalarType::Boolean),
            parameter_types,
        )?;
        if left.data_type != ScalarType::Boolean || right.data_type != ScalarType::Boolean {
            return Err(DbError::new(
                DATATYPE_MISMATCH,
                "boolean operator operands must be boolean",
            )
            .with_position_opt(position));
        }
        return Ok(BoundExpr {
            kind: BoundExprKind::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: ScalarType::Boolean,
            nullable: true,
        });
    }

    let left_type = infer_expr_type(&left, table, parameter_types)?;
    let right_type = infer_expr_type(&right, table, parameter_types)?;
    let mut operand_type = match (left_type, right_type) {
        (Some(left_type), Some(right_type)) => {
            common_type_with_literal(&left_type, &right_type, Some(&left), &right).ok_or_else(
                || {
                    DbError::new(
                        DATATYPE_MISMATCH,
                        format!("operator cannot match {left_type:?} with {right_type:?}"),
                    )
                    .with_position_opt(position)
                },
            )?
        }
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
    let left = bind_expr_with_parameter_types(left, table, Some(&operand_type), parameter_types)?;
    let right = bind_expr_with_parameter_types(right, table, Some(&operand_type), parameter_types)?;
    let nullable = left.nullable || right.nullable;
    Ok(BoundExpr {
        kind: BoundExprKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        data_type: if is_arithmetic_operator(op) {
            operand_type
        } else {
            ScalarType::Boolean
        },
        nullable,
    })
}

fn infer_expr_type(
    expr: &ParsedExpr,
    table: Option<&TableDefinition>,
    parameter_types: &BTreeMap<usize, ScalarType>,
) -> Result<Option<ScalarType>> {
    match &expr.kind {
        ParsedExprKind::Column(column) => {
            let table = table.ok_or_else(|| {
                DbError::new(UNDEFINED_COLUMN, "column reference is not valid here")
                    .with_position_opt(expr.position)
            })?;
            Ok(Some(
                table.columns()[resolve_column(column, table)?]
                    .data_type
                    .clone(),
            ))
        }
        ParsedExprKind::Literal(value) => Ok(value.scalar_type()),
        ParsedExprKind::Parameter(index) => Ok(parameter_types.get(index).cloned()),
        ParsedExprKind::ResolvedParameter { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::Unary { op, expr: inner } => match op {
            UnaryOperator::Not => Ok(Some(ScalarType::Boolean)),
            UnaryOperator::Negate => infer_expr_type(inner, table, parameter_types),
        },
        ParsedExprKind::Cast { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::Array { elements, .. } => {
            let mut element_type = None;
            for element in elements {
                let Some(candidate) = infer_expr_type(element, table, parameter_types)? else {
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
            |argument| infer_expr_type(argument, table, parameter_types),
            expr.position,
        ),
        ParsedExprKind::Binary { left, op, right } => {
            if is_arithmetic_operator(*op) {
                let left = infer_expr_type(left, table, parameter_types)?;
                let right = infer_expr_type(right, table, parameter_types)?;
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
        ParsedExprKind::Window { .. } => Err(DbError::new(
            "42P20",
            "window functions are not allowed in this statement",
        )
        .with_position_opt(expr.position)),
        ParsedExprKind::NamedWindow { .. } => Err(DbError::new(
            "42704",
            "named window reference was not resolved",
        )
        .with_position_opt(expr.position)),
        ParsedExprKind::Aggregate { .. } => {
            unsupported_at("aggregate is not valid in this statement", expr.position)
        }
    }
}

fn bind_literal(
    value: Value,
    expected: Option<&ScalarType>,
    position: Option<usize>,
) -> Result<BoundExpr> {
    let data_type = match expected {
        Some(expected) => {
            if !expected.accepts(&value) {
                if let (ScalarType::Enum { .. }, Value::Text(label)) = (expected, &value) {
                    return Err(DbError::new(
                        "22P02",
                        format!("invalid input value for enum: {label}"),
                    )
                    .with_position_opt(position));
                }
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    format!("value cannot be assigned to {expected:?}"),
                )
                .with_position_opt(position));
            }
            expected.clone()
        }
        None => value.scalar_type().unwrap_or(ScalarType::Text),
    };
    Ok(BoundExpr {
        nullable: value.is_null(),
        kind: BoundExprKind::Literal(value),
        data_type,
    })
}

fn resolve_table<'a>(name: &ParsedObjectName, catalog: &'a Catalog) -> Result<&'a TableDefinition> {
    let (schema, table, position) = split_table_name(name)?;
    if catalog.schema(&schema).is_none() {
        return Err(
            DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                .with_position_opt(position),
        );
    }
    catalog.table(&schema, &table).ok_or_else(|| {
        DbError::new(
            UNDEFINED_TABLE,
            format!("relation {schema}.{table} does not exist"),
        )
        .with_position_opt(position)
    })
}

fn resolve_trigger_target(name: &ParsedObjectName, catalog: &Catalog) -> Result<TriggerTarget> {
    let (schema_name, relation_name, position) = split_table_name(name)?;
    let schema = catalog.schema(&schema_name).ok_or_else(|| {
        DbError::new(
            UNDEFINED_SCHEMA,
            format!("schema {schema_name} does not exist"),
        )
        .with_position_opt(position)
    })?;
    if let Some(table) = schema.table(&relation_name) {
        return Ok(TriggerTarget::Table(table.id));
    }
    if let Some(view) = schema.view(&relation_name) {
        return Ok(TriggerTarget::View(view.id));
    }
    Err(DbError::new(
        UNDEFINED_TABLE,
        format!("relation {schema_name}.{relation_name} does not exist"),
    )
    .with_position_opt(position))
}

fn trigger_target_name(target: TriggerTarget, catalog: &Catalog) -> Result<&Identifier> {
    match target {
        TriggerTarget::Table(table_id) => catalog
            .table_by_id(table_id)
            .map(|table| &table.name)
            .ok_or_else(|| DbError::internal("bound trigger table disappeared")),
        TriggerTarget::View(view_id) => catalog
            .view_by_id(view_id)
            .map(|view| &view.name)
            .ok_or_else(|| DbError::internal("bound trigger view disappeared")),
    }
}

fn split_table_name(name: &ParsedObjectName) -> Result<(Identifier, Identifier, Option<usize>)> {
    match name.parts.as_slice() {
        [table] => Ok((
            Identifier::unquoted("public"),
            table.name.clone(),
            table.position,
        )),
        [schema, table] => Ok((
            schema.name.clone(),
            table.name.clone(),
            table.position.or(schema.position),
        )),
        _ => unsupported_at(
            "database-qualified names are not supported yet",
            name.parts.first().and_then(|part| part.position),
        ),
    }
}

fn resolve_column(name: &ParsedObjectName, table: &TableDefinition) -> Result<usize> {
    let (column, position) = match name.parts.as_slice() {
        [column] => (&column.name, column.position),
        [qualifier, column] if qualifier.name == table.name => (&column.name, column.position),
        [qualifier, _] => {
            return Err(DbError::new(
                UNDEFINED_TABLE,
                format!("invalid reference to table {}", qualifier.name),
            )
            .with_position_opt(qualifier.position));
        }
        _ => {
            return unsupported_at(
                "column references may contain at most a table qualifier",
                name.parts.first().and_then(|part| part.position),
            );
        }
    };
    table.column_index(column).ok_or_else(|| {
        DbError::new(UNDEFINED_COLUMN, format!("column {column} does not exist"))
            .with_position_opt(position)
    })
}

fn projection_name(expr: &ParsedExpr) -> String {
    if let ParsedExprKind::Column(column) = &expr.kind
        && let Some(column) = column.parts.last()
    {
        return column.name.as_str().to_owned();
    }
    if let ParsedExprKind::Function { function, .. } = &expr.kind {
        return match function {
            ScalarFunction::Version => "version",
            ScalarFunction::CurrentDatabase => "current_database",
            ScalarFunction::CurrentUser => "current_user",
            ScalarFunction::SessionUser => "session_user",
            _ => "?column?",
        }
        .to_owned();
    }
    "?column?".to_owned()
}

fn infer_scalar_function_type<F>(
    function: ScalarFunction,
    arguments: &[ParsedExpr],
    mut infer: F,
    position: Option<usize>,
) -> Result<Option<ScalarType>>
where
    F: FnMut(&ParsedExpr) -> Result<Option<ScalarType>>,
{
    match function {
        ScalarFunction::Version
        | ScalarFunction::CurrentDatabase
        | ScalarFunction::CurrentUser
        | ScalarFunction::SessionUser
        | ScalarFunction::CurrentSetting
        | ScalarFunction::Lower
        | ScalarFunction::Upper
        | ScalarFunction::Concat
        | ScalarFunction::Substring
        | ScalarFunction::Btrim
        | ScalarFunction::Ltrim
        | ScalarFunction::Rtrim
        | ScalarFunction::Replace
        | ScalarFunction::JsonbTypeof => Ok(Some(ScalarType::Text)),
        ScalarFunction::CharacterLength
        | ScalarFunction::OctetLength
        | ScalarFunction::ArrayLength
        | ScalarFunction::Cardinality
        | ScalarFunction::Strpos => Ok(Some(ScalarType::Int32)),
        ScalarFunction::Abs => infer(&arguments[0]),
        ScalarFunction::Coalesce
        | ScalarFunction::NullIf
        | ScalarFunction::Greatest
        | ScalarFunction::Least => {
            let mut common = None;
            for argument in arguments {
                let Some(candidate) = infer(argument)? else {
                    continue;
                };
                common = Some(match common {
                    Some(current) => common_type(&current, &candidate).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "function argument types {current:?} and {candidate:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(position)
                    })?,
                    None => candidate,
                });
            }
            Ok(common)
        }
    }
}

fn scalar_function_argument_type(
    function: ScalarFunction,
    index: usize,
    common: Option<&ScalarType>,
) -> Option<&ScalarType> {
    match function {
        ScalarFunction::Version
        | ScalarFunction::CurrentDatabase
        | ScalarFunction::CurrentUser
        | ScalarFunction::SessionUser => None,
        ScalarFunction::CurrentSetting if index == 0 => Some(&ScalarType::Text),
        ScalarFunction::CurrentSetting => Some(&ScalarType::Boolean),
        ScalarFunction::Lower
        | ScalarFunction::Upper
        | ScalarFunction::Btrim
        | ScalarFunction::Ltrim
        | ScalarFunction::Rtrim
        | ScalarFunction::Replace
        | ScalarFunction::Strpos => Some(&ScalarType::Text),
        ScalarFunction::Substring if index == 0 => Some(&ScalarType::Text),
        ScalarFunction::Substring => Some(&ScalarType::Int32),
        ScalarFunction::JsonbTypeof => Some(&ScalarType::Jsonb),
        ScalarFunction::ArrayLength if index == 1 => Some(&ScalarType::Int32),
        ScalarFunction::Coalesce
        | ScalarFunction::NullIf
        | ScalarFunction::Greatest
        | ScalarFunction::Least => common,
        ScalarFunction::CharacterLength
        | ScalarFunction::OctetLength
        | ScalarFunction::Abs
        | ScalarFunction::Concat
        | ScalarFunction::ArrayLength
        | ScalarFunction::Cardinality => None,
    }
}

fn validate_bound_scalar_function(
    function: ScalarFunction,
    arguments: &[BoundExpr],
    position: Option<usize>,
) -> Result<(ScalarType, bool)> {
    let invalid = |message: String| DbError::new("42883", message).with_position_opt(position);
    match function {
        ScalarFunction::Version
        | ScalarFunction::CurrentDatabase
        | ScalarFunction::CurrentUser
        | ScalarFunction::SessionUser => Ok((ScalarType::Text, false)),
        ScalarFunction::CurrentSetting => Ok((ScalarType::Text, true)),
        ScalarFunction::Lower | ScalarFunction::Upper => {
            if !is_textual(&arguments[0].data_type) {
                return Err(invalid(format!(
                    "function {function:?} requires a textual argument"
                )));
            }
            Ok((ScalarType::Text, arguments[0].nullable))
        }
        ScalarFunction::CharacterLength | ScalarFunction::OctetLength => {
            if !is_textual(&arguments[0].data_type) && arguments[0].data_type != ScalarType::Binary
            {
                return Err(invalid(format!(
                    "function {function:?} requires text or bytea"
                )));
            }
            Ok((ScalarType::Int32, arguments[0].nullable))
        }
        ScalarFunction::Abs => {
            if !is_numeric(&arguments[0].data_type) {
                return Err(invalid("ABS requires a numeric argument".to_owned()));
            }
            Ok((arguments[0].data_type.clone(), arguments[0].nullable))
        }
        ScalarFunction::Coalesce => {
            let data_type = arguments
                .first()
                .map(|argument| argument.data_type.clone())
                .ok_or_else(|| invalid("COALESCE requires an argument".to_owned()))?;
            Ok((
                data_type,
                arguments.iter().all(|argument| argument.nullable),
            ))
        }
        ScalarFunction::NullIf => Ok((arguments[0].data_type.clone(), true)),
        ScalarFunction::Concat => Ok((ScalarType::Text, false)),
        ScalarFunction::Substring => Ok((
            ScalarType::Text,
            arguments.iter().any(|argument| argument.nullable),
        )),
        ScalarFunction::Btrim | ScalarFunction::Ltrim | ScalarFunction::Rtrim => {
            if arguments
                .iter()
                .any(|argument| !is_textual(&argument.data_type))
            {
                return Err(invalid(format!(
                    "function {function:?} requires textual arguments"
                )));
            }
            Ok((
                ScalarType::Text,
                arguments.iter().any(|argument| argument.nullable),
            ))
        }
        ScalarFunction::Replace | ScalarFunction::Strpos => {
            if arguments
                .iter()
                .any(|argument| !is_textual(&argument.data_type))
            {
                return Err(invalid(format!(
                    "function {function:?} requires textual arguments"
                )));
            }
            Ok((
                if function == ScalarFunction::Strpos {
                    ScalarType::Int32
                } else {
                    ScalarType::Text
                },
                arguments.iter().any(|argument| argument.nullable),
            ))
        }
        ScalarFunction::Greatest | ScalarFunction::Least => {
            let data_type = arguments
                .first()
                .map(|argument| argument.data_type.clone())
                .ok_or_else(|| invalid(format!("function {function:?} requires an argument")))?;
            if arguments
                .iter()
                .any(|argument| argument.data_type != data_type)
            {
                return Err(invalid(format!(
                    "function {function:?} arguments must have a common type"
                )));
            }
            Ok((
                data_type,
                arguments.iter().all(|argument| argument.nullable),
            ))
        }
        ScalarFunction::JsonbTypeof => {
            if arguments[0].data_type != ScalarType::Jsonb {
                return Err(invalid("JSONB_TYPEOF requires a jsonb argument".to_owned()));
            }
            Ok((ScalarType::Text, arguments[0].nullable))
        }
        ScalarFunction::ArrayLength | ScalarFunction::Cardinality => {
            if !matches!(arguments[0].data_type, ScalarType::Array { .. }) {
                return Err(invalid(format!(
                    "function {function:?} requires an array argument"
                )));
            }
            Ok((ScalarType::Int32, true))
        }
    }
}

fn common_type(left: &ScalarType, right: &ScalarType) -> Option<ScalarType> {
    if left == right {
        return Some(left.clone());
    }
    if matches!(left, ScalarType::Oid)
        && matches!(
            right,
            ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64
        )
        || matches!(right, ScalarType::Oid)
            && matches!(
                left,
                ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64
            )
    {
        return Some(ScalarType::Oid);
    }
    if is_numeric(left) && is_numeric(right) {
        return Some(if numeric_rank(left) >= numeric_rank(right) {
            left.clone()
        } else {
            right.clone()
        });
    }
    if is_textual(left) && is_textual(right) {
        return Some(ScalarType::Text);
    }
    None
}

fn common_type_with_literal(
    left: &ScalarType,
    right: &ScalarType,
    left_expr: Option<&ParsedExpr>,
    right_expr: &ParsedExpr,
) -> Option<ScalarType> {
    common_type(left, right).or_else(|| match (left, right) {
        (ScalarType::Enum { .. }, ScalarType::Text) if is_unknown_text_literal(right_expr) => {
            Some(left.clone())
        }
        (ScalarType::Text, ScalarType::Enum { .. })
            if left_expr.is_some_and(is_unknown_text_literal) =>
        {
            Some(right.clone())
        }
        (ScalarType::Oid, ScalarType::Text) if is_unknown_text_literal(right_expr) => {
            Some(ScalarType::Oid)
        }
        (ScalarType::Text, ScalarType::Oid) if left_expr.is_some_and(is_unknown_text_literal) => {
            Some(ScalarType::Oid)
        }
        (
            ScalarType::Array {
                element: left_element,
            },
            ScalarType::Array {
                element: right_element,
            },
        ) if is_unknown_text_literal(right_expr)
            && matches!(left_element.as_ref(), ScalarType::Enum { .. })
            && matches!(right_element.as_ref(), ScalarType::Text) =>
        {
            Some(left.clone())
        }
        (
            ScalarType::Array {
                element: left_element,
            },
            ScalarType::Array {
                element: right_element,
            },
        ) if left_expr.is_some_and(is_unknown_text_literal)
            && matches!(left_element.as_ref(), ScalarType::Text)
            && matches!(right_element.as_ref(), ScalarType::Enum { .. }) =>
        {
            Some(right.clone())
        }
        _ => None,
    })
}

fn is_unknown_text_literal(expression: &ParsedExpr) -> bool {
    match &expression.kind {
        ParsedExprKind::Literal(Value::Text(_) | Value::Null) => true,
        ParsedExprKind::Array { elements, .. } => elements.iter().all(is_unknown_text_literal),
        _ => false,
    }
}

fn ensure_types_compatible(
    actual: &ScalarType,
    expected: &ScalarType,
    position: Option<usize>,
) -> Result<()> {
    if common_type(actual, expected).is_none() {
        return Err(DbError::new(
            DATATYPE_MISMATCH,
            format!("expected {expected:?}, found {actual:?}"),
        )
        .with_position_opt(position));
    }
    Ok(())
}

fn ensure_explicit_cast_supported(
    source: &ScalarType,
    target: &ScalarType,
    position: Option<usize>,
) -> Result<()> {
    let supported = source == target
        || (is_numeric(source) && is_numeric(target))
        || is_textual(target)
        || (is_textual(source)
            && matches!(
                target,
                ScalarType::Boolean
                    | ScalarType::Int16
                    | ScalarType::Int32
                    | ScalarType::Int64
                    | ScalarType::Oid
                    | ScalarType::Float32
                    | ScalarType::Float64
                    | ScalarType::Decimal { .. }
                    | ScalarType::Binary
                    | ScalarType::Date
                    | ScalarType::Time
                    | ScalarType::Timestamp { .. }
                    | ScalarType::Interval
                    | ScalarType::Json
                    | ScalarType::Jsonb
                    | ScalarType::Uuid
                    | ScalarType::Enum { .. }
            ))
        || matches!(
            (source, target),
            (
                ScalarType::Date,
                ScalarType::Timestamp {
                    with_timezone: false
                }
            ) | (
                ScalarType::Timestamp { .. },
                ScalarType::Date | ScalarType::Time
            ) | (ScalarType::Timestamp { .. }, ScalarType::Timestamp { .. })
                | (ScalarType::Json, ScalarType::Jsonb)
                | (ScalarType::Jsonb, ScalarType::Json)
                | (
                    ScalarType::Oid,
                    ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64
                )
                | (
                    ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64,
                    ScalarType::Oid
                )
        )
        || matches!(
            (source, target),
            (ScalarType::Array { .. }, ScalarType::Array { .. })
        );
    if supported {
        Ok(())
    } else {
        Err(DbError::new(
            "42846",
            format!("cannot cast type {source:?} to {target:?}"),
        )
        .with_position_opt(position))
    }
}

const fn is_arithmetic_operator(operator: BinaryOperator) -> bool {
    matches!(
        operator,
        BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
    )
}

fn is_numeric(data_type: &ScalarType) -> bool {
    matches!(
        data_type,
        ScalarType::Int16
            | ScalarType::Int32
            | ScalarType::Int64
            | ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Decimal { .. }
    )
}

fn numeric_rank(data_type: &ScalarType) -> u8 {
    match data_type {
        ScalarType::Int16 => 1,
        ScalarType::Int32 => 2,
        ScalarType::Int64 => 3,
        ScalarType::Decimal { .. } => 4,
        ScalarType::Float32 => 5,
        ScalarType::Float64 => 6,
        _ => 0,
    }
}

fn is_textual(data_type: &ScalarType) -> bool {
    matches!(
        data_type,
        ScalarType::Name
            | ScalarType::InternalChar
            | ScalarType::Char { .. }
            | ScalarType::Varchar { .. }
            | ScalarType::Text
    )
}

fn unsupported<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::new(FEATURE_NOT_SUPPORTED, message))
}

fn unsupported_at<T>(message: impl Into<String>, position: Option<usize>) -> Result<T> {
    Err(DbError::new(FEATURE_NOT_SUPPORTED, message).with_position_opt(position))
}
