
fn bind_routine_statement(statement: ParsedStatement, catalog: &Catalog) -> Result<BoundStatement> {
    match statement {
        ParsedStatement::CreateRoutine {
            name,
            kind,
            arguments,
            return_type,
            return_declared_type,
            returns_set,
            language,
            body,
            replace,
        } => {
            let (schema, name, position) = split_table_name(&name)?;
            if catalog.schema(&schema).is_none() {
                return Err(DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {schema} does not exist"),
                )
                .with_position_opt(position));
            }
            let arguments = arguments
                .into_iter()
                .map(|argument| {
                    let (data_type, declared_type) = match argument.declared_type {
                        Some(type_name) => {
                            let (data_type, type_id) = resolve_declared_data_type(
                                catalog,
                                &argument.data_type,
                                &type_name,
                            )?;
                            (data_type, Some(type_id))
                        }
                        None => (argument.data_type, None),
                    };
                    Ok(RoutineArgument {
                        name: argument.name,
                        data_type,
                        declared_type,
                        mode: argument.mode,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let (return_type, return_declared_type) = match (return_type, return_declared_type) {
                (Some(data_type), Some(type_name)) => {
                    let (data_type, type_id) =
                        resolve_declared_data_type(catalog, &data_type, &type_name)?;
                    (Some(data_type), Some(type_id))
                }
                (return_type, None) => (return_type, None),
                (None, Some(_)) => {
                    return Err(DbError::internal(
                        "routine return type name exists without a parsed data type",
                    ));
                }
            };
            Ok(BoundStatement::CreateRoutine {
                schema,
                name,
                kind,
                arguments,
                return_type,
                return_declared_type,
                returns_set,
                language,
                body,
                replace,
            })
        }
        ParsedStatement::DropRoutine {
            name,
            kind,
            argument_types,
            if_exists,
            behavior,
        } => {
            let (schema, name, position) = split_table_name(&name)?;
            let mut matches = Vec::new();
            for routine in catalog
                .routines_named(&schema, &name)
                .iter()
                .filter(|routine| routine.kind == kind)
            {
                let signature_matches = match argument_types.as_ref() {
                    None => true,
                    Some(argument_types) if routine.input_arity() == argument_types.len() => {
                        let mut matches = true;
                        for (argument, expected) in routine.input_arguments().zip(argument_types) {
                            let expected_declared_type = expected
                                .declared_type
                                .as_ref()
                                .map(|name| {
                                    resolve_user_defined_type(name, catalog).map(|ty| ty.id)
                                })
                                .transpose()?;
                            matches &= match expected_declared_type {
                                Some(type_id) => argument.declared_type == Some(type_id),
                                None => {
                                    argument.declared_type.is_none()
                                        && argument.data_type == expected.data_type
                                }
                            };
                        }
                        matches
                    }
                    Some(_) => false,
                };
                if signature_matches {
                    matches.push(routine);
                }
            }
            let object_kind = match kind {
                RoutineKind::Function => "function",
                RoutineKind::Procedure => "procedure",
            };
            let tag = match kind {
                RoutineKind::Function => "DROP FUNCTION",
                RoutineKind::Procedure => "DROP PROCEDURE",
            };
            match matches.as_slice() {
                [routine] => Ok(BoundStatement::DropRoutine {
                    routine_id: routine.id,
                    behavior,
                }),
                [] if if_exists => Ok(BoundStatement::NoOp {
                    tag: tag.to_owned(),
                }),
                [] => Err(DbError::new(
                    "42883",
                    format!("{object_kind} {schema}.{name} does not exist"),
                )
                .with_position_opt(position)),
                _ => Err(DbError::new(
                    "42725",
                    format!("{object_kind} {schema}.{name} is ambiguous"),
                )
                .with_position_opt(position)
                .with_hint("specify the routine argument types")),
            }
        }
        ParsedStatement::Call { name, arguments } => {
            let (schema, name, position) = split_table_name(&name)?;
            let schema_definition = catalog.schema(&schema).ok_or_else(|| {
                DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                    .with_position_opt(position)
            })?;
            let candidates = schema_definition
                .routines_named(&name)
                .iter()
                .filter(|routine| {
                    routine.kind == RoutineKind::Procedure
                        && routine.input_arity() == arguments.len()
                })
                .collect::<Vec<_>>();
            let mut matches = Vec::new();
            for routine in candidates {
                let input_arguments = routine.input_arguments().collect::<Vec<_>>();
                if let Some((bound, exact_declared_matches)) =
                    bind_routine_candidate(&arguments, &input_arguments, catalog)?
                {
                    matches.push((routine, bound, exact_declared_matches));
                }
            }
            retain_best_routine_matches(&mut matches, |candidate| candidate.2);
            match matches.as_slice() {
                [(routine, arguments, _)] => Ok(BoundStatement::Call {
                    routine_id: routine.id,
                    arguments: arguments.clone(),
                    schema: routine_output_schema(routine),
                }),
                [] => Err(DbError::new(
                    "42883",
                    format!("procedure {schema}.{name} with matching arguments does not exist"),
                )
                .with_position_opt(position)),
                _ => Err(DbError::new(
                    "42725",
                    format!("procedure call {schema}.{name} is ambiguous"),
                )
                .with_position_opt(position)),
            }
        }
        ParsedStatement::RoutineSelect {
            name,
            arguments,
            alias,
        } => {
            let (schema, name, position) = split_table_name(&name)?;
            let schema_definition = catalog.schema(&schema).ok_or_else(|| {
                DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                    .with_position_opt(position)
            })?;
            let candidates = schema_definition
                .routines_named(&name)
                .iter()
                .filter(|routine| {
                    routine.kind == RoutineKind::Function
                        && (routine.return_type.is_some()
                            || routine.output_arguments().next().is_some())
                        && routine.input_arity() == arguments.len()
                })
                .collect::<Vec<_>>();
            let mut matches = Vec::new();
            for routine in candidates {
                let input_arguments = routine.input_arguments().collect::<Vec<_>>();
                if let Some((bound, exact_declared_matches)) =
                    bind_routine_candidate(&arguments, &input_arguments, catalog)?
                {
                    matches.push((routine, bound, exact_declared_matches));
                }
            }
            retain_best_routine_matches(&mut matches, |candidate| candidate.2);
            match matches.as_slice() {
                [(routine, arguments, _)] => {
                    let output_arguments = routine.output_arguments().collect::<Vec<_>>();
                    if output_arguments.len() > 1 {
                        return Err(DbError::new(
                            DATATYPE_MISMATCH,
                            "a function with multiple OUT parameters cannot be used as a scalar expression",
                        ));
                    }
                    let return_type = routine
                        .return_type
                        .clone()
                        .or_else(|| {
                            output_arguments
                                .first()
                                .map(|argument| argument.data_type.clone())
                        })
                        .ok_or_else(|| {
                            DbError::internal("selected function lost its output type")
                        })?;
                    Ok(BoundStatement::RoutineSelect {
                        routine_id: routine.id,
                        arguments: arguments.clone(),
                        schema: Schema::new(vec![Field::new(
                            alias
                                .as_ref()
                                .map_or(name.as_str(), |alias| alias.name.as_str()),
                            return_type,
                            true,
                        )]),
                        returns_set: routine.returns_set,
                    })
                }
                [] => Err(DbError::new(
                    "42883",
                    format!("function {schema}.{name} with matching arguments does not exist"),
                )
                .with_position_opt(position)),
                _ => Err(DbError::new(
                    "42725",
                    format!("function call {schema}.{name} is ambiguous"),
                )
                .with_position_opt(position)),
            }
        }
        _ => Err(DbError::internal(
            "non-routine statement reached the routine binder",
        )),
    }
}
