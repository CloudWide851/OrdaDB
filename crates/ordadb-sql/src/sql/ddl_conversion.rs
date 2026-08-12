
fn convert_table_constraint(
    constraint: TableConstraint,
    sql: &str,
) -> Result<ParsedTableConstraint> {
    match constraint {
        TableConstraint::PrimaryKey(constraint) => {
            if constraint.index_name.is_some()
                || constraint.index_type.is_some()
                || !constraint.index_options.is_empty()
                || constraint.characteristics.is_some()
            {
                return unsupported("extended primary-key constraints are not supported");
            }
            Ok(ParsedTableConstraint::PrimaryKey {
                name: constraint.name.map(|name| convert_ident(name, sql)),
                columns: constraint
                    .columns
                    .iter()
                    .map(|column| convert_index_column(column, sql))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        TableConstraint::Unique(constraint) => {
            if constraint.index_name.is_some()
                || constraint.index_type.is_some()
                || !constraint.index_options.is_empty()
                || constraint.characteristics.is_some()
            {
                return unsupported("extended unique constraints are not supported");
            }
            Ok(ParsedTableConstraint::Unique {
                name: constraint.name.map(|name| convert_ident(name, sql)),
                columns: constraint
                    .columns
                    .iter()
                    .map(|column| convert_index_column(column, sql))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        TableConstraint::Check(constraint) => {
            if constraint.enforced.is_some() {
                return unsupported("CHECK ENFORCED clauses are not supported");
            }
            Ok(ParsedTableConstraint::Check {
                name: constraint.name.map(|name| convert_ident(name, sql)),
                sql: constraint.expr.to_string(),
                expression: convert_expr(*constraint.expr, sql)?,
            })
        }
        TableConstraint::ForeignKey(constraint) => {
            if constraint.index_name.is_some()
                || constraint.match_kind.is_some()
                || constraint.characteristics.is_some()
            {
                return unsupported("extended foreign-key constraints are not supported");
            }
            Ok(ParsedTableConstraint::ForeignKey {
                name: constraint.name.map(|name| convert_ident(name, sql)),
                columns: constraint
                    .columns
                    .into_iter()
                    .map(|column| convert_ident(column, sql))
                    .collect(),
                referenced_table: convert_object_name(constraint.foreign_table, sql)?,
                referenced_columns: constraint
                    .referred_columns
                    .into_iter()
                    .map(|column| convert_ident(column, sql))
                    .collect(),
                on_delete: convert_referential_action(constraint.on_delete)?,
                on_update: convert_referential_action(constraint.on_update)?,
            })
        }
        _ => unsupported("this table constraint is not supported"),
    }
}

fn convert_referential_action(action: Option<SqlReferentialAction>) -> Result<ReferentialAction> {
    match action {
        None | Some(SqlReferentialAction::NoAction) => Ok(ReferentialAction::NoAction),
        Some(SqlReferentialAction::Restrict) => Ok(ReferentialAction::Restrict),
        Some(SqlReferentialAction::Cascade) => Ok(ReferentialAction::Cascade),
        Some(SqlReferentialAction::SetNull) => Ok(ReferentialAction::SetNull),
        Some(SqlReferentialAction::SetDefault) => Ok(ReferentialAction::SetDefault),
    }
}

fn convert_drop_behavior(behavior: Option<SqlDropBehavior>) -> DropBehavior {
    match behavior {
        Some(SqlDropBehavior::Cascade) => DropBehavior::Cascade,
        Some(SqlDropBehavior::Restrict) | None => DropBehavior::Restrict,
    }
}

fn convert_alter_table(table: AlterTable, sql: &str) -> Result<ParsedStatement> {
    if table.only
        || table.location.is_some()
        || table.on_cluster.is_some()
        || table.table_type.is_some()
    {
        return unsupported("this ALTER TABLE form is not supported");
    }
    if table.operations.is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "ALTER TABLE requires at least one operation",
        ));
    }
    let mut operations = Vec::new();
    for operation in table.operations {
        match operation {
            SqlAlterTableOperation::RenameTable { table_name } => {
                let RenameTableNameKind::To(name) = table_name else {
                    return unsupported("ALTER TABLE rename requires RENAME TO");
                };
                operations.push(ParsedAlterTableOperation::RenameTable {
                    new_name: convert_single_identifier(name, sql)?,
                });
            }
            SqlAlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => operations.push(ParsedAlterTableOperation::RenameColumn {
                old_name: convert_ident(old_column_name, sql),
                new_name: convert_ident(new_column_name, sql),
            }),
            SqlAlterTableOperation::AddColumn {
                if_not_exists,
                column_def,
                column_position,
                ..
            } => {
                if column_position.is_some() {
                    return unsupported("column positioning is not supported");
                }
                let (column, constraints) = convert_column_definition(column_def, sql)?;
                operations.push(ParsedAlterTableOperation::AddColumn {
                    column,
                    if_not_exists,
                });
                operations.extend(
                    constraints
                        .into_iter()
                        .map(|constraint| ParsedAlterTableOperation::AddConstraint { constraint }),
                );
            }
            SqlAlterTableOperation::DropColumn {
                column_names,
                if_exists,
                drop_behavior,
                ..
            } => operations.push(ParsedAlterTableOperation::DropColumns {
                columns: column_names
                    .into_iter()
                    .map(|name| convert_ident(name, sql))
                    .collect(),
                if_exists,
                behavior: convert_drop_behavior(drop_behavior),
            }),
            SqlAlterTableOperation::AlterColumn { column_name, op } => {
                let column = convert_ident(column_name, sql);
                operations.push(match op {
                    SqlAlterColumnOperation::SetNotNull => {
                        ParsedAlterTableOperation::SetNotNull { column }
                    }
                    SqlAlterColumnOperation::DropNotNull => {
                        ParsedAlterTableOperation::DropNotNull { column }
                    }
                    SqlAlterColumnOperation::SetDefault { value } => {
                        ParsedAlterTableOperation::SetDefault {
                            column,
                            default: ParsedDefault {
                                sql: value.to_string(),
                                expression: convert_expr(value, sql)?,
                            },
                        }
                    }
                    SqlAlterColumnOperation::DropDefault => {
                        ParsedAlterTableOperation::DropDefault { column }
                    }
                    SqlAlterColumnOperation::SetDataType {
                        data_type, using, ..
                    } => {
                        if using.is_some() {
                            return unsupported(
                                "ALTER COLUMN TYPE USING expressions are not supported",
                            );
                        }
                        let (data_type, declared_type) = convert_column_data_type(data_type, sql)?;
                        ParsedAlterTableOperation::SetDataType {
                            column,
                            data_type,
                            declared_type,
                        }
                    }
                    SqlAlterColumnOperation::AddGenerated { .. } => {
                        return unsupported("generated columns are not supported");
                    }
                });
            }
            SqlAlterTableOperation::AddConstraint {
                constraint,
                not_valid,
            } => {
                if not_valid {
                    return unsupported("NOT VALID constraints are not supported");
                }
                operations.push(ParsedAlterTableOperation::AddConstraint {
                    constraint: convert_table_constraint(constraint, sql)?,
                });
            }
            SqlAlterTableOperation::DropConstraint {
                if_exists,
                name,
                drop_behavior,
            } => operations.push(ParsedAlterTableOperation::DropConstraint {
                name: convert_ident(name, sql),
                if_exists,
                behavior: convert_drop_behavior(drop_behavior),
            }),
            SqlAlterTableOperation::EnableTrigger { name } => {
                operations.push(ParsedAlterTableOperation::SetTriggerEnabled {
                    name: convert_ident(name, sql),
                    enabled: true,
                });
            }
            SqlAlterTableOperation::DisableTrigger { name } => {
                operations.push(ParsedAlterTableOperation::SetTriggerEnabled {
                    name: convert_ident(name, sql),
                    enabled: false,
                });
            }
            _ => return unsupported("this ALTER TABLE operation is not supported"),
        }
    }
    Ok(ParsedStatement::AlterTable {
        name: convert_object_name(table.name, sql)?,
        if_exists: table.if_exists,
        operations,
    })
}

fn apply_sequence_options(
    sequence: &mut NewSequence,
    options: Vec<SequenceOptions>,
    sql: &str,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for option in options {
        let key = match &option {
            SequenceOptions::IncrementBy(..) => 0_u8,
            SequenceOptions::MinValue(..) => 1,
            SequenceOptions::MaxValue(..) => 2,
            SequenceOptions::StartWith(..) => 3,
            SequenceOptions::Cache(..) => 4,
            SequenceOptions::Cycle(..) => 5,
        };
        if !seen.insert(key) {
            return Err(DbError::new(
                SYNTAX_ERROR,
                "a sequence option was specified more than once",
            ));
        }
        match option {
            SequenceOptions::IncrementBy(value, _) => {
                sequence.increment = sequence_option_i64(value, sql)?;
            }
            SequenceOptions::MinValue(value) => {
                sequence.min_value = value
                    .map(|value| sequence_option_i64(value, sql))
                    .transpose()?;
            }
            SequenceOptions::MaxValue(value) => {
                sequence.max_value = value
                    .map(|value| sequence_option_i64(value, sql))
                    .transpose()?;
            }
            SequenceOptions::StartWith(value, _) => {
                sequence.start_value = Some(sequence_option_i64(value, sql)?);
            }
            SequenceOptions::Cache(value) => {
                if sequence_option_i64(value, sql)? != 1 {
                    return unsupported("sequence CACHE values other than 1 are not supported");
                }
            }
            SequenceOptions::Cycle(no_cycle) => sequence.cycle = !no_cycle,
        }
    }
    Ok(())
}

fn sequence_option_i64(expression: SqlExpr, sql: &str) -> Result<i64> {
    let expression = convert_expr(expression, sql)?;
    match expression.kind {
        ParsedExprKind::Literal(Value::Int16(value)) => Ok(i64::from(value)),
        ParsedExprKind::Literal(Value::Int32(value)) => Ok(i64::from(value)),
        ParsedExprKind::Literal(Value::Int64(value)) => Ok(value),
        ParsedExprKind::Unary {
            op: UnaryOperator::Negate,
            expr,
        } => match expr.kind {
            ParsedExprKind::Literal(Value::Int16(value)) => Ok(-i64::from(value)),
            ParsedExprKind::Literal(Value::Int32(value)) => Ok(-i64::from(value)),
            ParsedExprKind::Literal(Value::Int64(value)) => value
                .checked_neg()
                .ok_or_else(|| DbError::new("22003", "sequence option is out of range")),
            _ => Err(DbError::new(
                SYNTAX_ERROR,
                "sequence options must be integer constants",
            )),
        },
        _ => Err(DbError::new(
            SYNTAX_ERROR,
            "sequence options must be integer constants",
        )),
    }
}

fn split_owned_by(owner: ObjectName, sql: &str) -> Result<(ParsedObjectName, ParsedIdentifier)> {
    let mut parts = convert_object_name(owner, sql)?.parts;
    if parts.len() < 2 || parts.len() > 3 {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "OWNED BY requires table.column or schema.table.column",
        ));
    }
    let column = parts
        .pop()
        .ok_or_else(|| DbError::internal("OWNED BY column disappeared"))?;
    Ok((ParsedObjectName { parts }, column))
}

fn convert_create_view(view: CreateView, sql: &str) -> Result<ParsedStatement> {
    if view.or_alter
        || view.secure
        || view.name_before_not_exists
        || !matches!(view.options, CreateTableOptions::None)
        || !view.cluster_by.is_empty()
        || view.comment.is_some()
        || view.with_no_schema_binding
        || view.temporary
        || view.copy_grants
        || view.to.is_some()
        || view.params.is_some()
        || (view.or_replace && view.if_not_exists)
        || (view.materialized && view.or_replace)
    {
        return unsupported("this CREATE VIEW form is not supported");
    }
    let columns = view
        .columns
        .into_iter()
        .map(|column| {
            if column.data_type.is_some() || column.options.is_some() {
                return unsupported("typed or optioned view columns are not supported");
            }
            Ok(convert_ident(column.name, sql))
        })
        .collect::<Result<Vec<_>>>()?;
    let query_sql = view.query.to_string();
    let query = convert_select_query(*view.query, sql)?;
    Ok(ParsedStatement::CreateView {
        name: convert_object_name(view.name, sql)?,
        kind: if view.materialized {
            ViewKind::Materialized
        } else {
            ViewKind::Regular
        },
        query: Box::new(query),
        query_sql,
        columns,
        replace: view.or_replace,
        if_not_exists: view.if_not_exists,
        with_data: !has_keyword_sequence(sql, &["WITH", "NO", "DATA"]),
    })
}

fn convert_create_function(function: SqlCreateFunction, sql: &str) -> Result<ParsedStatement> {
    if function.or_alter
        || function.temporary
        || function.if_not_exists
        || function.behavior.is_some()
        || function.called_on_null.is_some()
        || function.parallel.is_some()
        || !function.set_params.is_empty()
        || function.using.is_some()
        || function.determinism_specifier.is_some()
        || function.options.is_some()
        || function.remote_connection.is_some()
    {
        return unsupported("this CREATE FUNCTION option is not supported");
    }
    if matches!(function.security, Some(FunctionSecurity::Definer)) {
        return unsupported("SECURITY DEFINER routines are not supported");
    }
    let language = function
        .language
        .map(|language| language.value)
        .unwrap_or_else(|| "plpgsql".to_owned());
    if !language.eq_ignore_ascii_case("plpgsql") {
        return unsupported("only LANGUAGE plpgsql routines are supported");
    }
    let arguments = function
        .args
        .unwrap_or_default()
        .into_iter()
        .map(|argument| {
            if argument.default_expr.is_some() {
                return unsupported("defaulted routine arguments are not supported yet");
            }
            let (data_type, declared_type) = convert_column_data_type(argument.data_type, sql)?;
            Ok(ParsedRoutineArgument {
                name: argument.name.map(|name| convert_ident(name, sql).name),
                data_type,
                declared_type,
                mode: convert_routine_argument_mode(argument.mode),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let has_output_arguments = arguments
        .iter()
        .any(|argument| argument.mode.produces_output());
    let (return_type, return_declared_type, returns_set) = match function.return_type {
        Some(FunctionReturnType::DataType(data_type)) if is_trigger_type(&data_type) => {
            (None, None, false)
        }
        Some(FunctionReturnType::DataType(data_type)) => {
            let (data_type, declared_type) = convert_column_data_type(data_type, sql)?;
            (Some(data_type), declared_type, false)
        }
        Some(FunctionReturnType::SetOf(data_type)) => {
            let (data_type, declared_type) = convert_column_data_type(data_type, sql)?;
            (Some(data_type), declared_type, true)
        }
        None if has_output_arguments => (None, None, false),
        None => return unsupported("CREATE FUNCTION requires a return type or OUT parameter"),
    };
    let body = match function.function_body {
        Some(CreateFunctionBody::AsBeforeOptions {
            body,
            link_symbol: None,
        })
        | Some(CreateFunctionBody::AsAfterOptions(body)) => routine_body_string(body)?,
        _ => {
            return unsupported("CREATE FUNCTION requires one quoted PL/pgSQL body after AS");
        }
    };
    Ok(ParsedStatement::CreateRoutine {
        name: convert_object_name(function.name, sql)?,
        kind: RoutineKind::Function,
        arguments,
        return_type,
        return_declared_type,
        returns_set,
        language: "plpgsql".to_owned(),
        body,
        replace: function.or_replace,
    })
}

fn convert_drop_routine(
    mut routines: Vec<sqlparser::ast::FunctionDesc>,
    kind: RoutineKind,
    if_exists: bool,
    behavior: Option<SqlDropBehavior>,
    sql: &str,
) -> Result<ParsedStatement> {
    if routines.len() != 1 {
        return unsupported("dropping multiple routines in one statement is not supported");
    }
    let routine = routines
        .pop()
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "DROP routine requires a name"))?;
    let argument_types = routine
        .args
        .map(|arguments| {
            arguments
                .into_iter()
                .map(|argument| {
                    if argument.default_expr.is_some() {
                        return unsupported("DROP routine signatures cannot contain defaults");
                    }
                    let (data_type, declared_type) =
                        convert_column_data_type(argument.data_type, sql)?;
                    Ok(ParsedRoutineArgument {
                        name: None,
                        data_type,
                        declared_type,
                        mode: convert_routine_argument_mode(argument.mode),
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let argument_types = argument_types.map(|arguments| {
        arguments
            .into_iter()
            .filter(|argument| argument.mode.accepts_input())
            .collect()
    });
    Ok(ParsedStatement::DropRoutine {
        name: convert_object_name(routine.name, sql)?,
        kind,
        argument_types,
        if_exists,
        behavior: convert_drop_behavior(behavior),
    })
}

fn convert_routine_argument_mode(mode: Option<ArgMode>) -> RoutineArgumentMode {
    match mode {
        None | Some(ArgMode::In) => RoutineArgumentMode::In,
        Some(ArgMode::Out) => RoutineArgumentMode::Out,
        Some(ArgMode::InOut) => RoutineArgumentMode::InOut,
        Some(ArgMode::Variadic) => RoutineArgumentMode::Variadic,
    }
}

fn convert_create_trigger(trigger: SqlCreateTrigger, sql: &str) -> Result<ParsedStatement> {
    if trigger.or_alter
        || trigger.temporary
        || trigger.or_replace
        || trigger.is_constraint
        || trigger.referenced_table_name.is_some()
        || !trigger.referencing.is_empty()
        || trigger.condition.is_some()
        || trigger.statements_as
        || trigger.statements.is_some()
        || trigger.characteristics.is_some()
        || !trigger.period_before_table
    {
        return unsupported("this CREATE TRIGGER option is not supported");
    }
    let level = match trigger.trigger_object {
        Some(TriggerObjectKind::ForEach(TriggerObject::Row))
        | Some(TriggerObjectKind::For(TriggerObject::Row)) => TriggerLevel::Row,
        Some(TriggerObjectKind::ForEach(TriggerObject::Statement))
        | Some(TriggerObjectKind::For(TriggerObject::Statement))
        | None => TriggerLevel::Statement,
    };
    let timing = match (trigger.period, level) {
        (Some(TriggerPeriod::Before), TriggerLevel::Row) => TriggerTiming::Before,
        (Some(TriggerPeriod::After), TriggerLevel::Row) => TriggerTiming::After,
        (Some(TriggerPeriod::InsteadOf), TriggerLevel::Row) => TriggerTiming::InsteadOf,
        (Some(TriggerPeriod::Before), TriggerLevel::Statement) => TriggerTiming::BeforeStatement,
        (Some(TriggerPeriod::After), TriggerLevel::Statement) => TriggerTiming::AfterStatement,
        (Some(TriggerPeriod::InsteadOf), TriggerLevel::Statement) => {
            return unsupported("INSTEAD OF triggers must use FOR EACH ROW");
        }
        _ => return unsupported("this trigger timing is not supported"),
    };
    let events = trigger
        .events
        .into_iter()
        .map(|event| match event {
            SqlTriggerEvent::Insert => Ok(CatalogTriggerEvent::Insert),
            SqlTriggerEvent::Update(columns) if columns.is_empty() => {
                Ok(CatalogTriggerEvent::Update)
            }
            SqlTriggerEvent::Delete => Ok(CatalogTriggerEvent::Delete),
            _ => unsupported("this trigger event is not supported"),
        })
        .collect::<Result<Vec<_>>>()?;
    let body = trigger
        .exec_body
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "trigger requires EXECUTE FUNCTION"))?;
    if body.exec_type != TriggerExecBodyType::Function
        || body
            .func_desc
            .args
            .as_ref()
            .is_some_and(|arguments| !arguments.is_empty())
    {
        return unsupported("trigger functions must be invoked without arguments");
    }
    let name = convert_object_name(trigger.name, sql)?;
    let [name] = name.parts.as_slice() else {
        return unsupported("trigger names cannot be schema qualified");
    };
    Ok(ParsedStatement::CreateTrigger {
        name: name.clone(),
        table: convert_object_name(trigger.table_name, sql)?,
        timing,
        level,
        events,
        routine: convert_object_name(body.func_desc.name, sql)?,
    })
}

fn is_trigger_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Trigger)
        || matches!(
            data_type,
            DataType::Custom(name, modifiers)
                if modifiers.is_empty() && name.to_string().eq_ignore_ascii_case("trigger")
        )
}

fn routine_body_string(body: SqlExpr) -> Result<String> {
    let SqlExpr::Value(value) = body else {
        return unsupported("routine body must be a quoted string");
    };
    match value.value {
        SqlValue::DollarQuotedString(value) => Ok(value.value),
        SqlValue::SingleQuotedString(value) => Ok(value),
        _ => unsupported("routine body must be a dollar-quoted or single-quoted string"),
    }
}

fn convert_routine_invocation(
    function: Function,
    sql: &str,
) -> Result<(ParsedObjectName, Vec<ParsedExpr>)> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("routine invocation options are not supported");
    }
    let FunctionArguments::List(arguments) = function.args else {
        return unsupported("routine invocation requires a parenthesized argument list");
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return unsupported("routine argument modifiers are not supported");
    }
    let arguments = arguments
        .args
        .into_iter()
        .map(|argument| match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => {
                convert_expr(expression, sql)
            }
            _ => unsupported("routine calls support positional expression arguments only"),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((convert_object_name(function.name, sql)?, arguments))
}
