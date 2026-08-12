impl ParameterTypeSolver {

    fn collect_scalar_function(
        &mut self,
        function: ScalarFunction,
        arguments: &[ParsedExpr],
        inputs: &[InputColumn],
        expected: Option<&ScalarType>,
        catalog: &Catalog,
        depth: usize,
    ) -> Result<Option<ScalarType>> {
        match function {
            ScalarFunction::Version
            | ScalarFunction::CurrentDatabase
            | ScalarFunction::CurrentUser
            | ScalarFunction::SessionUser
            | ScalarFunction::CurrentSetting => Ok(Some(ScalarType::Text)),
            ScalarFunction::Lower
            | ScalarFunction::Upper
            | ScalarFunction::Btrim
            | ScalarFunction::Ltrim
            | ScalarFunction::Rtrim
            | ScalarFunction::Replace
            | ScalarFunction::Strpos => {
                for argument in arguments {
                    self.collect_expr(
                        argument,
                        inputs,
                        Some(&ScalarType::Text),
                        catalog,
                        depth + 1,
                    )?;
                }
                Ok(Some(if function == ScalarFunction::Strpos {
                    ScalarType::Int32
                } else {
                    ScalarType::Text
                }))
            }
            ScalarFunction::CharacterLength | ScalarFunction::OctetLength => {
                let data_type =
                    self.collect_expr(&arguments[0], inputs, None, catalog, depth + 1)?;
                if data_type.is_none() {
                    self.collect_expr(
                        &arguments[0],
                        inputs,
                        Some(&ScalarType::Text),
                        catalog,
                        depth + 1,
                    )?;
                }
                Ok(Some(ScalarType::Int32))
            }
            ScalarFunction::Abs => {
                let data_type = self.collect_expr(
                    &arguments[0],
                    inputs,
                    expected.filter(|expected| is_numeric(expected)),
                    catalog,
                    depth + 1,
                )?;
                Ok(data_type)
            }
            ScalarFunction::Coalesce
            | ScalarFunction::NullIf
            | ScalarFunction::Greatest
            | ScalarFunction::Least => {
                let mut common = expected.cloned();
                for argument in arguments {
                    let candidate =
                        self.collect_expr(argument, inputs, None, catalog, depth + 1)?;
                    common = match (common, candidate) {
                        (Some(left), Some(right)) => common_type(&left, &right),
                        (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                        (None, None) => None,
                    };
                }
                if let Some(common) = &common {
                    for argument in arguments {
                        self.collect_expr(argument, inputs, Some(common), catalog, depth + 1)?;
                    }
                }
                Ok(common)
            }
            ScalarFunction::Concat => {
                for argument in arguments {
                    let data_type =
                        self.collect_expr(argument, inputs, None, catalog, depth + 1)?;
                    if data_type.is_none() {
                        self.collect_expr(
                            argument,
                            inputs,
                            Some(&ScalarType::Text),
                            catalog,
                            depth + 1,
                        )?;
                    }
                }
                Ok(Some(ScalarType::Text))
            }
            ScalarFunction::Substring => {
                self.collect_expr(
                    &arguments[0],
                    inputs,
                    Some(&ScalarType::Text),
                    catalog,
                    depth + 1,
                )?;
                for argument in &arguments[1..] {
                    self.collect_expr(
                        argument,
                        inputs,
                        Some(&ScalarType::Int32),
                        catalog,
                        depth + 1,
                    )?;
                }
                Ok(Some(ScalarType::Text))
            }
            ScalarFunction::JsonbTypeof => {
                self.collect_expr(
                    &arguments[0],
                    inputs,
                    Some(&ScalarType::Jsonb),
                    catalog,
                    depth + 1,
                )?;
                Ok(Some(ScalarType::Text))
            }
            ScalarFunction::ArrayLength => {
                self.collect_expr(&arguments[0], inputs, None, catalog, depth + 1)?;
                self.collect_expr(
                    &arguments[1],
                    inputs,
                    Some(&ScalarType::Int32),
                    catalog,
                    depth + 1,
                )?;
                Ok(Some(ScalarType::Int32))
            }
            ScalarFunction::Cardinality => {
                self.collect_expr(&arguments[0], inputs, None, catalog, depth + 1)?;
                Ok(Some(ScalarType::Int32))
            }
        }
    }

    fn collect_window(
        &mut self,
        call: &ParsedWindowCall,
        spec: &ParsedWindowSpec,
        inputs: &[InputColumn],
        catalog: &Catalog,
        depth: usize,
    ) -> Result<Option<ScalarType>> {
        for expression in &spec.partition_by {
            self.collect_expr(expression, inputs, None, catalog, depth + 1)?;
        }
        for order in &spec.order_by {
            self.collect_expr(&order.expr, inputs, None, catalog, depth + 1)?;
        }
        if let Some(frame) = &spec.frame {
            let range_type = if frame.units == WindowFrameUnits::Range {
                spec.order_by
                    .first()
                    .map(|order| self.collect_expr(&order.expr, inputs, None, catalog, depth + 1))
                    .transpose()?
                    .flatten()
            } else {
                Some(ScalarType::Int64)
            };
            for bound in [&frame.start_bound, &frame.end_bound] {
                if let ParsedWindowFrameBound::Preceding(expression)
                | ParsedWindowFrameBound::Following(expression) = bound
                {
                    self.collect_expr(expression, inputs, range_type.as_ref(), catalog, depth + 1)?;
                }
            }
        }
        if let Some(filter) = &call.filter {
            self.collect_expr(
                filter,
                inputs,
                Some(&ScalarType::Boolean),
                catalog,
                depth + 1,
            )?;
        }
        match call.function {
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                Ok(Some(ScalarType::Int64))
            }
            WindowFunction::FirstValue
            | WindowFunction::LastValue
            | WindowFunction::Lag
            | WindowFunction::Lead
            | WindowFunction::NthValue => {
                let value_type = call
                    .arguments
                    .first()
                    .map(|argument| self.collect_expr(argument, inputs, None, catalog, depth + 1))
                    .transpose()?
                    .flatten();
                if matches!(call.function, WindowFunction::Lag | WindowFunction::Lead)
                    && let Some(offset) = call.arguments.get(1)
                {
                    self.collect_expr(
                        offset,
                        inputs,
                        Some(&ScalarType::Int64),
                        catalog,
                        depth + 1,
                    )?;
                }
                if call.function == WindowFunction::NthValue
                    && let Some(offset) = call.arguments.get(1)
                {
                    self.collect_expr(
                        offset,
                        inputs,
                        Some(&ScalarType::Int64),
                        catalog,
                        depth + 1,
                    )?;
                }
                if matches!(call.function, WindowFunction::Lag | WindowFunction::Lead)
                    && let Some(default) = call.arguments.get(2)
                {
                    let default_type = self.collect_expr(
                        default,
                        inputs,
                        value_type.as_ref(),
                        catalog,
                        depth + 1,
                    )?;
                    let reconciled = match (value_type, default_type) {
                        (Some(left), Some(right)) => common_type(&left, &right),
                        (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                        (None, None) => None,
                    };
                    if let Some(reconciled) = &reconciled {
                        if let Some(value) = call.arguments.first() {
                            self.collect_expr(value, inputs, Some(reconciled), catalog, depth + 1)?;
                        }
                        self.collect_expr(default, inputs, Some(reconciled), catalog, depth + 1)?;
                    }
                    return Ok(reconciled);
                }
                Ok(value_type)
            }
            WindowFunction::Aggregate(function) => {
                let argument_type = call
                    .arguments
                    .first()
                    .map(|argument| self.collect_expr(argument, inputs, None, catalog, depth + 1))
                    .transpose()?
                    .flatten();
                Ok(parameter_aggregate_type(function, argument_type))
            }
        }
    }

    fn collect_on_conflict(
        &mut self,
        on_conflict: &ParsedOnConflict,
        table: &TableDefinition,
        catalog: &Catalog,
        depth: usize,
    ) -> Result<()> {
        let ParsedConflictAction::DoUpdate {
            assignments,
            filter,
        } = &on_conflict.action
        else {
            return Ok(());
        };
        let width = table.columns().len();
        let mut inputs = parameter_table_inputs(table, table.name.clone(), 0, false);
        inputs.extend(parameter_table_inputs(
            table,
            Identifier::unquoted("excluded"),
            width,
            false,
        ));
        for (column, expression) in assignments {
            if let Some(index) = table.column_index(&column.name) {
                self.collect_expr(
                    expression,
                    &inputs,
                    Some(&table.columns()[index].data_type),
                    catalog,
                    depth,
                )?;
            }
        }
        if let Some(filter) = filter {
            self.collect_expr(filter, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
        }
        Ok(())
    }

    fn collect_merge(
        &mut self,
        merge: &ParsedMerge,
        catalog: &Catalog,
        expected_output: Option<&[Option<ScalarType>]>,
        depth: usize,
    ) -> Result<Vec<Option<ScalarType>>> {
        let target = resolve_table(&merge.target.name, catalog)?;
        let source = resolve_table(&merge.source.name, catalog)?;
        let target_binding = merge
            .target
            .alias
            .as_ref()
            .map_or_else(|| target.name.clone(), |alias| alias.name.clone());
        let source_binding = merge
            .source
            .alias
            .as_ref()
            .map_or_else(|| source.name.clone(), |alias| alias.name.clone());
        let mut inputs = parameter_table_inputs(target, target_binding, 0, false);
        let source_offset = inputs.len();
        inputs.extend(parameter_table_inputs(
            source,
            source_binding,
            source_offset,
            false,
        ));
        self.collect_expr(
            &merge.on,
            &inputs,
            Some(&ScalarType::Boolean),
            catalog,
            depth,
        )?;
        for clause in &merge.clauses {
            if let Some(predicate) = &clause.predicate {
                self.collect_expr(
                    predicate,
                    &inputs,
                    Some(&ScalarType::Boolean),
                    catalog,
                    depth,
                )?;
            }
            match &clause.action {
                ParsedMergeAction::Update { assignments } => {
                    for (column, expression) in assignments {
                        if let Some(index) = target.column_index(&column.name) {
                            self.collect_expr(
                                expression,
                                &inputs,
                                Some(&target.columns()[index].data_type),
                                catalog,
                                depth,
                            )?;
                        }
                    }
                }
                ParsedMergeAction::Insert { columns, values } => {
                    let column_indexes = parameter_target_columns(columns, target)?;
                    for (expression, index) in values.iter().zip(column_indexes) {
                        self.collect_expr(
                            expression,
                            &inputs,
                            Some(&target.columns()[index].data_type),
                            catalog,
                            depth,
                        )?;
                    }
                }
                ParsedMergeAction::Delete | ParsedMergeAction::DoNothing => {}
            }
        }
        let target_inputs = parameter_table_inputs(target, target.name.clone(), 0, false);
        self.collect_projection(
            &merge.returning,
            &target_inputs,
            expected_output,
            catalog,
            depth,
        )
    }

    fn collect_routine_arguments(
        &mut self,
        name: &ParsedObjectName,
        arguments: &[ParsedExpr],
        catalog: &Catalog,
        depth: usize,
    ) -> Result<()> {
        let (schema, name, _) = split_table_name(name)?;
        let Some(schema) = catalog.schema(&schema) else {
            return Ok(());
        };
        let candidates = schema
            .routines_named(&name)
            .iter()
            .filter(|routine| routine.arguments.len() == arguments.len())
            .collect::<Vec<_>>();
        for (index, expression) in arguments.iter().enumerate() {
            let types = candidates
                .iter()
                .map(|routine| routine.arguments[index].data_type.clone())
                .collect::<Vec<_>>();
            let expected = types
                .first()
                .filter(|first| types.iter().all(|data_type| data_type == *first))
                .cloned();
            self.collect_expr(expression, &[], expected.as_ref(), catalog, depth)?;
        }
        Ok(())
    }

    fn collect_with_statement(
        &mut self,
        recursive: bool,
        ctes: &[ParsedCte],
        body: &ParsedStatement,
        catalog: &Catalog,
        expected_output: Option<&[Option<ScalarType>]>,
        depth: usize,
    ) -> Result<Vec<Option<ScalarType>>> {
        let mut transient_catalog = catalog.clone();
        let temporary_schema = Identifier::unquoted(format!("__ordadb_param_cte_{depth}"));
        if transient_catalog.schema(&temporary_schema).is_none() {
            transient_catalog.create_schema(temporary_schema.clone())?;
        }
        let mut replacements = BTreeMap::new();
        let mut names = BTreeSet::new();
        for cte in ctes {
            if !names.insert(cte.name.name.clone()) {
                return Err(DbError::new(
                    "42712",
                    format!("WITH query name {} specified more than once", cte.name.name),
                )
                .with_position_opt(cte.name.position));
            }
            let query = (*cte.query).clone();
            let self_recursive =
                recursive && parsed_query_references_table(&query, &cte.name.name, 0)?;
            let (mut seed, recursive_term) = if self_recursive {
                match query {
                    ParsedStatement::SetOperation {
                        left,
                        operator: QuerySetOperator::Union,
                        right,
                        ..
                    } => (*left, Some(*right)),
                    _ => return Ok(Vec::new()),
                }
            } else {
                (query, None)
            };
            rewrite_cte_references(&mut seed, &replacements, 0)?;
            let seed_types =
                self.collect_statement(&seed, &transient_catalog, &[], None, depth + 1)?;
            let Some(mut output) =
                self.try_statement_schema(&seed, &transient_catalog, &[], depth + 1)
            else {
                return Ok(seed_types);
            };
            apply_cte_column_aliases(&cte.name, &cte.columns, &mut output)?;
            create_cte_relation(
                &mut transient_catalog,
                &temporary_schema,
                &cte.name,
                &output,
            )?;
            replacements.insert(
                cte.name.name.clone(),
                cte_replacement_name(&temporary_schema, &cte.name),
            );
            if let Some(mut recursive_term) = recursive_term {
                rewrite_cte_references(&mut recursive_term, &replacements, 0)?;
                let expected = output
                    .fields
                    .iter()
                    .map(|field| Some(field.data_type.clone()))
                    .collect::<Vec<_>>();
                self.collect_statement(
                    &recursive_term,
                    &transient_catalog,
                    &[],
                    Some(&expected),
                    depth + 1,
                )?;
            }
        }
        let mut body = body.clone();
        rewrite_cte_references(&mut body, &replacements, 0)?;
        self.collect_statement(&body, &transient_catalog, &[], expected_output, depth + 1)
    }

    fn try_statement_schema(
        &self,
        statement: &ParsedStatement,
        catalog: &Catalog,
        outer_inputs: &[InputColumn],
        depth: usize,
    ) -> Option<Schema> {
        let mut statement = statement.clone();
        resolve_statement_types(&mut statement, &self.types, None, depth, None).ok()?;
        let statement = if outer_inputs.is_empty() {
            bind_with_view_depth(statement, catalog, depth).ok()?
        } else {
            bind_apply_query(statement, catalog, depth, outer_inputs).ok()?
        };
        bound_query_schema(&statement).ok()
    }
}

fn parameter_table_inputs(
    table: &TableDefinition,
    binding: Identifier,
    offset: usize,
    nullable: bool,
) -> Vec<InputColumn> {
    table
        .columns()
        .iter()
        .enumerate()
        .map(|(column_offset, column)| InputColumn {
            binding: binding.clone(),
            name: column.name.clone(),
            index: offset + column_offset,
            data_type: column.data_type.clone(),
            nullable: nullable || column.nullable,
            outer_depth: 0,
        })
        .collect()
}

fn parameter_relation_inputs(
    name: &ParsedObjectName,
    binding: Option<Identifier>,
    catalog: &Catalog,
    offset: usize,
    nullable: bool,
) -> Result<Vec<InputColumn>> {
    let (schema, relation, _) = split_table_name(name)?;
    let binding = binding.unwrap_or_else(|| relation.clone());
    if let Some(view) = catalog.view(&schema, &relation) {
        return Ok(view
            .output
            .fields
            .iter()
            .enumerate()
            .map(|(column_offset, field)| InputColumn {
                binding: binding.clone(),
                name: Identifier::unquoted(&field.name),
                index: offset + column_offset,
                data_type: field.data_type.clone(),
                nullable: nullable || field.nullable,
                outer_depth: 0,
            })
            .collect());
    }
    let table = resolve_table(name, catalog)?;
    Ok(parameter_table_inputs(table, binding, offset, nullable))
}

fn parameter_target_columns(
    columns: &[ParsedIdentifier],
    table: &TableDefinition,
) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Ok((0..table.columns().len()).collect());
    }
    columns
        .iter()
        .map(|column| {
            table.column_index(&column.name).ok_or_else(|| {
                DbError::new(
                    UNDEFINED_COLUMN,
                    format!("column {} does not exist", column.name),
                )
                .with_position_opt(column.position)
            })
        })
        .collect()
}

fn parameter_aggregate_type(
    function: AggregateFunction,
    argument_type: Option<ScalarType>,
) -> Option<ScalarType> {
    match function {
        AggregateFunction::Count => Some(ScalarType::Int64),
        AggregateFunction::Avg => argument_type.map(|_| ScalarType::Float64),
        AggregateFunction::Sum => argument_type.map(|data_type| match data_type {
            ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64 => ScalarType::Int64,
            ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
            other => other,
        }),
        AggregateFunction::Min | AggregateFunction::Max => argument_type,
    }
}

fn parsed_parameter_index(expression: &ParsedExpr) -> Option<usize> {
    match expression.kind {
        ParsedExprKind::Parameter(index) | ParsedExprKind::ResolvedParameter { index, .. } => {
            Some(index)
        }
        _ => None,
    }
}
