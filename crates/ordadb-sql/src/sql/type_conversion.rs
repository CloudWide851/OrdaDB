
fn convert_sql_value(value: SqlValue, position: Option<usize>) -> Result<ParsedExprKind> {
    match value {
        SqlValue::Null => Ok(ParsedExprKind::Literal(Value::Null)),
        SqlValue::Boolean(value) => Ok(ParsedExprKind::Literal(Value::Boolean(value))),
        SqlValue::Number(value, _) => {
            let value = if value.contains(['.', 'e', 'E']) {
                if value.contains(['e', 'E']) {
                    Value::Float64(value.parse().map_err(|_| {
                        DbError::new(SYNTAX_ERROR, "invalid floating-point literal")
                            .with_position_opt(position)
                    })?)
                } else {
                    Value::Decimal(Decimal::from_str(&value).map_err(|_| {
                        DbError::new(SYNTAX_ERROR, "invalid decimal literal")
                            .with_position_opt(position)
                    })?)
                }
            } else if let Ok(value) = value.parse::<i32>() {
                Value::Int32(value)
            } else {
                Value::Int64(value.parse().map_err(|_| {
                    DbError::new(SYNTAX_ERROR, "integer literal is out of range")
                        .with_position_opt(position)
                })?)
            };
            Ok(ParsedExprKind::Literal(value))
        }
        SqlValue::SingleQuotedString(value)
        | SqlValue::EscapedStringLiteral(value)
        | SqlValue::UnicodeStringLiteral(value)
        | SqlValue::NationalStringLiteral(value) => Ok(ParsedExprKind::Literal(Value::Text(value))),
        SqlValue::Placeholder(parameter) => {
            let index = parameter
                .strip_prefix('$')
                .and_then(|index| index.parse::<usize>().ok())
                .filter(|index| *index > 0)
                .or_else(|| named_at_parameter_index(&parameter))
                .ok_or_else(|| {
                    DbError::new("42P02", format!("invalid parameter reference {parameter}"))
                        .with_position_opt(position)
                })?;
            Ok(ParsedExprKind::Parameter(index))
        }
        _ => unsupported_at("this literal form is not supported yet", position),
    }
}

fn named_at_parameter_index(value: &str) -> Option<usize> {
    value
        .get(..2)
        .filter(|prefix| prefix.eq_ignore_ascii_case("@p"))
        .and_then(|_| value.get(2..))
        .and_then(|index| index.parse::<usize>().ok())
        .filter(|index| *index > 0)
}

fn parse_temporal_literal(
    data_type: DataType,
    value: &str,
    position: Option<usize>,
) -> Result<Value> {
    let data_type_label = data_type.to_string();
    let invalid = || {
        DbError::new(
            "22007",
            format!("invalid {data_type_label} literal {value:?}"),
        )
        .with_position_opt(position)
    };
    match data_type {
        DataType::Date => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|_| invalid()),
        DataType::Time(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
                .map(Value::Time)
                .map_err(|_| invalid())
        }
        DataType::Timestamp(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
        | DataType::Datetime(_) => NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
            .map(Value::Timestamp)
            .map_err(|_| invalid()),
        DataType::Timestamp(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
            DateTime::parse_from_rfc3339(value)
                .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%:z"))
                .map(|value| Value::Timestamp(value.naive_utc()))
                .map_err(|_| invalid())
        }
        DataType::Interval {
            fields: None,
            precision: None,
        } => PgInterval::from_str(value)
            .map(Value::Interval)
            .map_err(|error| error.with_position_opt(position)),
        DataType::Interval { .. } => unsupported_at(
            "INTERVAL field and precision qualifiers are not supported yet",
            position,
        ),
        _ => unsupported_at("this typed literal is not supported yet", position),
    }
}

fn convert_data_type(data_type: DataType) -> Result<ScalarType> {
    convert_data_type_with_array_depth(data_type, 0)
}

fn convert_column_data_type(
    data_type: DataType,
    sql: &str,
) -> Result<(ScalarType, Option<ParsedObjectName>)> {
    convert_column_data_type_with_array_depth(data_type, sql, 0)
}

fn convert_column_data_type_with_array_depth(
    data_type: DataType,
    sql: &str,
    array_depth: usize,
) -> Result<(ScalarType, Option<ParsedObjectName>)> {
    match data_type {
        DataType::Custom(name, modifiers)
            if modifiers.is_empty()
                && !name.to_string().eq_ignore_ascii_case("uniqueidentifier")
                && !name.to_string().eq_ignore_ascii_case("vector") =>
        {
            if let Some(data_type) = postgres_catalog_scalar_type(&name, &modifiers) {
                return Ok((data_type, None));
            }
            Ok((ScalarType::Text, Some(convert_object_name(name, sql)?)))
        }
        DataType::Array(ArrayElemTypeDef::SquareBracket(element, _)) => {
            const MAX_ARRAY_DIMENSIONS: usize = 6;
            if array_depth >= MAX_ARRAY_DIMENSIONS {
                return Err(DbError::new(
                    "54000",
                    format!("array type exceeds the maximum of {MAX_ARRAY_DIMENSIONS} dimensions"),
                ));
            }
            let (element, declared_type) =
                convert_column_data_type_with_array_depth(*element, sql, array_depth + 1)?;
            Ok((
                match element {
                    ScalarType::Array { .. } => element,
                    element => ScalarType::Array {
                        element: Box::new(element),
                    },
                },
                declared_type,
            ))
        }
        data_type => Ok((
            convert_data_type_with_array_depth(data_type, array_depth)?,
            None,
        )),
    }
}

fn convert_data_type_with_array_depth(
    data_type: DataType,
    array_depth: usize,
) -> Result<ScalarType> {
    match data_type {
        DataType::Bool | DataType::Boolean => Ok(ScalarType::Boolean),
        DataType::Int2(_) | DataType::SmallInt(_) | DataType::TinyInt(_) => Ok(ScalarType::Int16),
        DataType::Int(_) | DataType::Int4(_) | DataType::Integer(_) | DataType::MediumInt(_) => {
            Ok(ScalarType::Int32)
        }
        DataType::Int8(_) | DataType::BigInt(_) => Ok(ScalarType::Int64),
        DataType::Float4 | DataType::Real => Ok(ScalarType::Float32),
        DataType::Float8 | DataType::Double(_) | DataType::DoublePrecision => {
            Ok(ScalarType::Float64)
        }
        DataType::Numeric(info) | DataType::Decimal(info) | DataType::Dec(info) => {
            let (precision, scale) = decimal_info(info)?;
            Ok(ScalarType::Decimal { precision, scale })
        }
        DataType::Character(length) | DataType::Char(length) => Ok(ScalarType::Char {
            length: character_length(length)?,
        }),
        DataType::CharacterVarying(length)
        | DataType::Varchar(length)
        | DataType::Nvarchar(length) => Ok(ScalarType::Varchar {
            length: character_length(length)?,
        }),
        DataType::Text => Ok(ScalarType::Text),
        DataType::Bytea
        | DataType::Binary(_)
        | DataType::Varbinary(_)
        | DataType::Blob(_)
        | DataType::TinyBlob
        | DataType::MediumBlob
        | DataType::LongBlob => Ok(ScalarType::Binary),
        DataType::Date => Ok(ScalarType::Date),
        DataType::Time(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            Ok(ScalarType::Time)
        }
        DataType::Timestamp(_, timezone) => Ok(ScalarType::Timestamp {
            with_timezone: matches!(timezone, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz),
        }),
        DataType::Datetime(_) => Ok(ScalarType::Timestamp {
            with_timezone: false,
        }),
        DataType::Interval {
            fields: None,
            precision: None,
        } => Ok(ScalarType::Interval),
        DataType::Interval { .. } => {
            unsupported("INTERVAL field and precision qualifiers are not supported yet")
        }
        DataType::Array(ArrayElemTypeDef::SquareBracket(element, _)) => {
            const MAX_ARRAY_DIMENSIONS: usize = 6;
            if array_depth >= MAX_ARRAY_DIMENSIONS {
                return Err(DbError::new(
                    "54000",
                    format!("array type exceeds the maximum of {MAX_ARRAY_DIMENSIONS} dimensions"),
                ));
            }
            let element = convert_data_type_with_array_depth(*element, array_depth + 1)?;
            Ok(match element {
                ScalarType::Array { .. } => element,
                element => ScalarType::Array {
                    element: Box::new(element),
                },
            })
        }
        DataType::Array(_) => unsupported("only PostgreSQL type[] array syntax is supported"),
        DataType::JSON => Ok(ScalarType::Json),
        DataType::JSONB => Ok(ScalarType::Jsonb),
        DataType::Uuid => Ok(ScalarType::Uuid),
        DataType::Custom(name, modifiers)
            if postgres_catalog_scalar_type(&name, &modifiers).is_some() =>
        {
            Ok(postgres_catalog_scalar_type(&name, &modifiers)
                .expect("guard established PostgreSQL catalog scalar type"))
        }
        DataType::Custom(name, modifiers) if name.to_string().eq_ignore_ascii_case("vector") => {
            let dimensions = match modifiers.as_slice() {
                [] => None,
                [dimension] => Some(dimension.parse::<usize>().map_err(|_| {
                    DbError::new(SYNTAX_ERROR, "VECTOR dimension must be a positive integer")
                })?),
                _ => return unsupported("VECTOR accepts at most one dimension"),
            };
            Ok(ScalarType::Vector { dimensions })
        }
        DataType::Custom(name, modifiers)
            if modifiers.is_empty()
                && name.to_string().eq_ignore_ascii_case("uniqueidentifier") =>
        {
            Ok(ScalarType::Uuid)
        }
        _ => unsupported("this SQL data type is not supported yet"),
    }
}

fn postgres_catalog_scalar_type(name: &ObjectName, modifiers: &[String]) -> Option<ScalarType> {
    if !modifiers.is_empty() {
        return None;
    }
    match name.to_string().to_ascii_lowercase().as_str() {
        "oid" | "pg_catalog.oid" => Some(ScalarType::Oid),
        "name" | "pg_catalog.name" => Some(ScalarType::Name),
        "\"char\"" | "pg_catalog.\"char\"" => Some(ScalarType::InternalChar),
        _ => None,
    }
}

fn decimal_info(info: ExactNumberInfo) -> Result<(Option<u8>, Option<u8>)> {
    match info {
        ExactNumberInfo::None => Ok((None, None)),
        ExactNumberInfo::Precision(precision) => Ok((
            Some(
                u8::try_from(precision)
                    .map_err(|_| DbError::new(SYNTAX_ERROR, "decimal precision is out of range"))?,
            ),
            None,
        )),
        ExactNumberInfo::PrecisionAndScale(precision, scale) => {
            if scale < 0 {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "negative decimal scale is not supported",
                ));
            }
            Ok((
                Some(u8::try_from(precision).map_err(|_| {
                    DbError::new(SYNTAX_ERROR, "decimal precision is out of range")
                })?),
                Some(
                    u8::try_from(scale)
                        .map_err(|_| DbError::new(SYNTAX_ERROR, "decimal scale is out of range"))?,
                ),
            ))
        }
    }
}

fn character_length(length: Option<CharacterLength>) -> Result<Option<u32>> {
    match length {
        None | Some(CharacterLength::Max) => Ok(None),
        Some(CharacterLength::IntegerLength { length, .. }) => {
            Ok(Some(u32::try_from(length).map_err(|_| {
                DbError::new(SYNTAX_ERROR, "character length is out of range")
            })?))
        }
    }
}

fn convert_index_method(method: Option<IndexType>) -> Result<IndexMethod> {
    match method {
        None | Some(IndexType::BTree) => Ok(IndexMethod::BTree),
        Some(IndexType::Custom(name))
            if name.quote_style.is_none() && name.value.eq_ignore_ascii_case("fulltext") =>
        {
            Ok(IndexMethod::FullText)
        }
        Some(IndexType::Custom(name))
            if name.quote_style.is_none() && name.value.eq_ignore_ascii_case("hnsw") =>
        {
            Ok(IndexMethod::Hnsw)
        }
        Some(method) => unsupported(format!("index method {method} is not supported")),
    }
}

fn convert_index_options(options: Vec<SqlExpr>, sql: &str) -> Result<Vec<ParsedIndexOption>> {
    options
        .into_iter()
        .map(|option| {
            let position = span_position(sql, option.span());
            let SqlExpr::BinaryOp { left, op, right } = option else {
                return unsupported_at("index options must use name = value", position);
            };
            if op != SqlBinaryOperator::Eq {
                return unsupported_at("index options must use name = value", position);
            }
            let SqlExpr::Identifier(name) = *left else {
                return unsupported_at("index option names must be identifiers", position);
            };
            let parsed_name = convert_ident(name, sql);
            if parsed_name.name.is_quoted() {
                return unsupported_at(
                    "quoted index option names are not supported",
                    parsed_name.position,
                );
            }
            let SqlExpr::Value(value) = *right else {
                return unsupported_at(
                    "index option values must be strings or non-negative integers",
                    position,
                );
            };
            let value = match value.value {
                SqlValue::SingleQuotedString(value)
                | SqlValue::EscapedStringLiteral(value)
                | SqlValue::UnicodeStringLiteral(value)
                | SqlValue::NationalStringLiteral(value) => ParsedIndexOptionValue::Text(value),
                SqlValue::Number(value, _) => {
                    ParsedIndexOptionValue::Integer(value.parse::<usize>().map_err(|_| {
                        DbError::new("22023", "index option integer is out of range")
                            .with_position_opt(position)
                    })?)
                }
                _ => {
                    return unsupported_at(
                        "index option values must be strings or non-negative integers",
                        position,
                    );
                }
            };
            Ok(ParsedIndexOption {
                name: parsed_name,
                value,
            })
        })
        .collect()
}

fn convert_index_column(
    column: &sqlparser::ast::IndexColumn,
    sql: &str,
) -> Result<ParsedIdentifier> {
    if column.operator_class.is_some()
        || column.column.options.asc.is_some()
        || column.column.options.nulls_first.is_some()
        || column.column.with_fill.is_some()
    {
        return unsupported("extended index columns are not supported yet");
    }
    let SqlExpr::Identifier(ident) = &column.column.expr else {
        return unsupported("constraint columns must be simple identifiers");
    };
    Ok(convert_ident(ident.clone(), sql))
}

fn convert_object_name(name: ObjectName, sql: &str) -> Result<ParsedObjectName> {
    let parts = name
        .0
        .into_iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => Ok(convert_ident(ident, sql)),
            ObjectNamePart::Function(_) => unsupported("dynamic object names are not supported"),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ParsedObjectName { parts })
}

fn convert_single_identifier(name: ObjectName, sql: &str) -> Result<ParsedIdentifier> {
    let object = convert_object_name(name, sql)?;
    let [identifier] = object.parts.as_slice() else {
        return unsupported("qualified column lists are not supported");
    };
    Ok(identifier.clone())
}

fn convert_ident(ident: Ident, sql: &str) -> ParsedIdentifier {
    ParsedIdentifier {
        name: Identifier::new(ident.value, ident.quote_style.is_some()),
        position: span_position(sql, ident.span),
    }
}

fn resolve_declared_data_type(
    catalog: &Catalog,
    parsed_data_type: &ScalarType,
    declared_type: &ParsedObjectName,
) -> Result<(ScalarType, TypeId)> {
    let (type_schema, type_name, type_position) = split_table_name(declared_type)?;
    let definition = catalog
        .user_defined_type(&type_schema, &type_name)
        .ok_or_else(|| {
            DbError::new(
                "42704",
                format!("type {type_schema}.{type_name} does not exist"),
            )
            .with_position_opt(type_position)
        })?;
    let logical_type = definition.logical_type();
    let data_type = if matches!(parsed_data_type, ScalarType::Array { .. }) {
        match logical_type {
            ScalarType::Array { .. } => logical_type,
            element => ScalarType::Array {
                element: Box::new(element),
            },
        }
    } else {
        logical_type
    };
    Ok((data_type, definition.id))
}

fn resolve_user_defined_type<'a>(
    name: &ParsedObjectName,
    catalog: &'a Catalog,
) -> Result<&'a TypeDefinition> {
    let (schema, name, position) = split_table_name(name)?;
    if catalog.schema(&schema).is_none() {
        return Err(
            DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                .with_position_opt(position),
        );
    }
    catalog.user_defined_type(&schema, &name).ok_or_else(|| {
        DbError::new("42704", format!("type {schema}.{name} does not exist"))
            .with_position_opt(position)
    })
}

fn bind_create_table(
    name: ParsedObjectName,
    columns: Vec<ParsedColumn>,
    constraints: Vec<ParsedTableConstraint>,
    if_not_exists: bool,
    catalog: &Catalog,
) -> Result<BoundStatement> {
    let (schema, table, position) = split_table_name(&name)?;
    if catalog.schema(&schema).is_none() {
        return Err(
            DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                .with_position_opt(position),
        );
    }
    if catalog.table(&schema, &table).is_some() {
        if if_not_exists {
            return Ok(BoundStatement::NoOp {
                tag: "CREATE TABLE".to_owned(),
            });
        }
        return Err(
            DbError::new("42P07", format!("table {schema}.{table} already exists"))
                .with_position_opt(position),
        );
    }
    let mut seen = BTreeSet::new();
    let columns = columns
        .into_iter()
        .map(|column| {
            if !seen.insert(column.name.name.clone()) {
                return Err(DbError::new(
                    "42701",
                    format!("column {} specified more than once", column.name.name),
                )
                .with_position_opt(column.name.position));
            }
            let (data_type, declared_type) = match column.declared_type {
                Some(type_name) => {
                    let (data_type, type_id) =
                        resolve_declared_data_type(catalog, &column.data_type, &type_name)?;
                    (data_type, Some(type_id))
                }
                None => (column.data_type, None),
            };
            let default = column
                .default
                .map(|default| {
                    bind_expr(default.expression, None, Some(&data_type))?;
                    Ok(CatalogExpression::new(default.sql))
                })
                .transpose()?;
            Ok(NewColumn {
                name: column.name.name,
                data_type,
                declared_type,
                nullable: column.nullable,
                primary_key: column.primary_key,
                unique: column.unique,
                default,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut seen_constraints = BTreeSet::new();
    let constraints = constraints
        .into_iter()
        .enumerate()
        .map(|(ordinal, constraint)| {
            let constraint = bind_table_constraint(constraint, &table, ordinal, &columns, catalog)?;
            if !seen_constraints.insert(constraint.name.clone()) {
                return Err(DbError::new(
                    "42710",
                    format!("constraint {} specified more than once", constraint.name),
                ));
            }
            Ok(constraint)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundStatement::CreateTable {
        schema,
        name: table,
        columns,
        constraints,
        if_not_exists,
    })
}

fn bind_table_constraint(
    constraint: ParsedTableConstraint,
    table_name: &Identifier,
    ordinal: usize,
    columns: &[NewColumn],
    catalog: &Catalog,
) -> Result<NewConstraint> {
    let local_names = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<BTreeSet<_>>();
    let validate_columns = |values: Vec<ParsedIdentifier>| -> Result<Vec<Identifier>> {
        let mut seen = BTreeSet::new();
        values
            .into_iter()
            .map(|column| {
                if !local_names.contains(&column.name) {
                    return Err(DbError::new(
                        UNDEFINED_COLUMN,
                        format!("column {} does not exist", column.name),
                    )
                    .with_position_opt(column.position));
                }
                if !seen.insert(column.name.clone()) {
                    return Err(DbError::new(
                        "42701",
                        format!("column {} specified more than once", column.name),
                    )
                    .with_position_opt(column.position));
                }
                Ok(column.name)
            })
            .collect()
    };
    let generated_name =
        |provided: Option<ParsedIdentifier>, columns: &[Identifier], suffix: &str| -> Identifier {
            provided.map(|value| value.name).unwrap_or_else(|| {
                let column_part = if columns.is_empty() {
                    ordinal.saturating_add(1).to_string()
                } else {
                    columns
                        .iter()
                        .map(Identifier::as_str)
                        .collect::<Vec<_>>()
                        .join("_")
                };
                Identifier::unquoted(format!(
                    "{}_{}_{}",
                    table_name.as_str(),
                    column_part,
                    suffix
                ))
            })
        };
    let (name, kind) = match constraint {
        ParsedTableConstraint::PrimaryKey { name, columns } => {
            let columns = validate_columns(columns)?;
            (
                generated_name(name, &columns, "pkey"),
                NewConstraintKind::PrimaryKey { columns },
            )
        }
        ParsedTableConstraint::Unique { name, columns } => {
            let columns = validate_columns(columns)?;
            (
                generated_name(name, &columns, "key"),
                NewConstraintKind::Unique { columns },
            )
        }
        ParsedTableConstraint::Check {
            name,
            expression,
            sql,
        } => {
            validate_check_expression(&expression, &local_names)?;
            (
                generated_name(name, &[], "check"),
                NewConstraintKind::Check {
                    expression: CatalogExpression::new(sql),
                },
            )
        }
        ParsedTableConstraint::ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        } => {
            let columns = validate_columns(columns)?;
            let referenced = resolve_table(&referenced_table, catalog)?;
            if referenced_columns.is_empty() {
                return Err(DbError::new(
                    "42830",
                    "foreign keys must name referenced columns",
                ));
            }
            let referenced_columns = referenced_columns
                .into_iter()
                .map(|column| {
                    referenced
                        .column(&column.name)
                        .map(|definition| definition.id)
                        .ok_or_else(|| {
                            DbError::new(
                                UNDEFINED_COLUMN,
                                format!("column {} does not exist", column.name),
                            )
                            .with_position_opt(column.position)
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            (
                generated_name(name, &columns, "fkey"),
                NewConstraintKind::ForeignKey {
                    columns,
                    referenced_table: referenced.id,
                    referenced_columns,
                    on_delete,
                    on_update,
                },
            )
        }
    };
    Ok(NewConstraint { name, kind })
}

fn validate_check_expression(
    expression: &ParsedExpr,
    columns: &BTreeSet<Identifier>,
) -> Result<()> {
    let mut stack = vec![expression];
    while let Some(expression) = stack.pop() {
        match &expression.kind {
            ParsedExprKind::Column(name) => {
                let Some(column) = name.parts.last() else {
                    return Err(DbError::new(SYNTAX_ERROR, "empty column reference"));
                };
                if !columns.contains(&column.name) {
                    return Err(DbError::new(
                        UNDEFINED_COLUMN,
                        format!("column {} does not exist", column.name),
                    )
                    .with_position_opt(column.position));
                }
            }
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                stack.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => stack.extend(elements),
            ParsedExprKind::Function { arguments, .. } => stack.extend(arguments),
            ParsedExprKind::Binary { left, right, .. } => {
                stack.push(right);
                stack.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                stack.extend(list);
                stack.push(expr);
            }
            ParsedExprKind::Aggregate { .. } => {
                return unsupported("aggregate functions are not allowed in CHECK constraints");
            }
            ParsedExprKind::Window { .. }
            | ParsedExprKind::NamedWindow { .. }
            | ParsedExprKind::WindowValue { .. } => {
                return Err(DbError::new(
                    "42P20",
                    "window functions are not allowed in CHECK constraints",
                ));
            }
            ParsedExprKind::ScalarSubquery(_)
            | ParsedExprKind::Exists { .. }
            | ParsedExprKind::InSubquery { .. }
            | ParsedExprKind::QuantifiedSubquery { .. }
            | ParsedExprKind::RowSubquery { .. } => {
                return unsupported("subqueries are not allowed in CHECK constraints");
            }
            ParsedExprKind::Literal(_)
            | ParsedExprKind::Parameter(_)
            | ParsedExprKind::ResolvedParameter { .. }
            | ParsedExprKind::ApplyValue { .. } => {}
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DmlTarget {
    Table(TableId),
    View(ViewId),
}

struct DmlRelation {
    target: DmlTarget,
    scope: TableDefinition,
}
