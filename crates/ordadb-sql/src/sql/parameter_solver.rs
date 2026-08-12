impl ParameterTypeSolver {
    fn solve(
        statement: &ParsedStatement,
        catalog: &Catalog,
    ) -> Result<BTreeMap<usize, ScalarType>> {
        let mut solver = Self::default();
        for _ in 0..MAX_PARAMETER_SOLVER_PASSES {
            solver.changed = false;
            solver.collect_statement(statement, catalog, &[], None, 0)?;
            if !solver.changed {
                return Ok(solver.types);
            }
        }
        Err(DbError::new(
            "54001",
            "parameter type inference exceeded its fixed-point pass limit",
        ))
    }

    fn constrain(
        &mut self,
        index: usize,
        data_type: &ScalarType,
        position: Option<usize>,
    ) -> Result<()> {
        if let Some(existing) = self.types.get(&index) {
            if existing != data_type {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    format!("inconsistent types deduced for parameter ${index}"),
                )
                .with_detail(format!(
                    "parameter ${index} was constrained as both {existing:?} and {data_type:?}"
                ))
                .with_position_opt(position));
            }
            return Ok(());
        }
        self.types.insert(index, data_type.clone());
        self.changed = true;
        Ok(())
    }

    fn collect_statement(
        &mut self,
        statement: &ParsedStatement,
        catalog: &Catalog,
        outer_inputs: &[InputColumn],
        expected_output: Option<&[Option<ScalarType>]>,
        depth: usize,
    ) -> Result<Vec<Option<ScalarType>>> {
        if depth >= MAX_PARAMETER_SOLVER_DEPTH {
            return Err(DbError::new(
                "54001",
                "parameter type inference exceeded its statement depth limit",
            ));
        }
        match statement {
            ParsedStatement::Select {
                table,
                projection,
                filter,
                order_by,
                offset,
                limit,
            } => {
                let local_inputs = parameter_relation_inputs(table, None, catalog, 0, false)?;
                let inputs = inputs_with_outer(&local_inputs, outer_inputs)?;
                if let Some(filter) = filter {
                    self.collect_expr(filter, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
                }
                for order in order_by {
                    self.collect_order_expr(&order.expr, &inputs, catalog, depth)?;
                }
                if let Some(offset) = offset {
                    self.collect_expr(offset, &inputs, Some(&ScalarType::Int64), catalog, depth)?;
                }
                if let Some(limit) = limit {
                    self.collect_expr(limit, &inputs, Some(&ScalarType::Int64), catalog, depth)?;
                }
                self.collect_projection(projection, &inputs, expected_output, catalog, depth)
            }
            ParsedStatement::AdvancedSelect {
                table,
                joins,
                projection,
                filter,
                group_by,
                having,
                order_by,
                offset,
                limit,
                ..
            } => {
                let binding = table.alias.as_ref().map(|alias| alias.name.clone());
                let mut local_inputs =
                    parameter_relation_inputs(&table.name, binding, catalog, 0, false)?;
                for join in joins {
                    match &join.source {
                        ParsedJoinSource::Table(table) => {
                            let binding = table.alias.as_ref().map(|alias| alias.name.clone());
                            let offset = local_inputs.len();
                            local_inputs.extend(parameter_relation_inputs(
                                &table.name,
                                binding,
                                catalog,
                                offset,
                                join.kind == JoinKind::Left,
                            )?);
                        }
                        ParsedJoinSource::Derived {
                            lateral,
                            query,
                            alias,
                            columns,
                        } => {
                            let visible = if *lateral {
                                inputs_with_outer(&local_inputs, outer_inputs)?
                            } else {
                                Vec::new()
                            };
                            self.collect_statement(query, catalog, &visible, None, depth + 1)?;
                            if let Some(schema) =
                                self.try_statement_schema(query, catalog, &visible, depth + 1)
                            {
                                let offset = local_inputs.len();
                                for (index, field) in schema.fields.iter().enumerate() {
                                    let name = columns.get(index).map_or_else(
                                        || Identifier::unquoted(&field.name),
                                        |name| name.name.clone(),
                                    );
                                    local_inputs.push(InputColumn {
                                        binding: alias.name.clone(),
                                        name,
                                        index: offset + index,
                                        data_type: field.data_type.clone(),
                                        nullable: join.kind == JoinKind::Left || field.nullable,
                                        outer_depth: 0,
                                    });
                                }
                            }
                        }
                    }
                    let inputs = inputs_with_outer(&local_inputs, outer_inputs)?;
                    self.collect_expr(
                        &join.on,
                        &inputs,
                        Some(&ScalarType::Boolean),
                        catalog,
                        depth,
                    )?;
                }
                let inputs = inputs_with_outer(&local_inputs, outer_inputs)?;
                if let Some(filter) = filter {
                    self.collect_expr(filter, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
                }
                for expression in group_by {
                    self.collect_expr(expression, &inputs, None, catalog, depth)?;
                }
                if let Some(having) = having {
                    self.collect_expr(having, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
                }
                for order in order_by {
                    self.collect_order_expr(&order.expr, &inputs, catalog, depth)?;
                }
                if let Some(offset) = offset {
                    self.collect_expr(offset, &inputs, Some(&ScalarType::Int64), catalog, depth)?;
                }
                if let Some(limit) = limit {
                    self.collect_expr(limit, &inputs, Some(&ScalarType::Int64), catalog, depth)?;
                }
                self.collect_projection(projection, &local_inputs, expected_output, catalog, depth)
            }
            ParsedStatement::SetOperation {
                left,
                right,
                order_by,
                offset,
                limit,
                ..
            } => {
                let mut left_output = self.collect_statement(
                    left,
                    catalog,
                    outer_inputs,
                    expected_output,
                    depth + 1,
                )?;
                let mut right_output = self.collect_statement(
                    right,
                    catalog,
                    outer_inputs,
                    expected_output,
                    depth + 1,
                )?;
                if left_output.len() == right_output.len() {
                    let reconciled = left_output
                        .iter()
                        .zip(&right_output)
                        .map(|(left, right)| match (left, right) {
                            (Some(left), Some(right)) => common_type(left, right),
                            (Some(data_type), None) | (None, Some(data_type)) => {
                                Some(data_type.clone())
                            }
                            (None, None) => None,
                        })
                        .collect::<Vec<_>>();
                    left_output = self.collect_statement(
                        left,
                        catalog,
                        outer_inputs,
                        Some(&reconciled),
                        depth + 1,
                    )?;
                    right_output = self.collect_statement(
                        right,
                        catalog,
                        outer_inputs,
                        Some(&reconciled),
                        depth + 1,
                    )?;
                }
                let _ = order_by;
                if let Some(offset) = offset {
                    self.collect_expr(offset, &[], Some(&ScalarType::Int64), catalog, depth)?;
                }
                if let Some(limit) = limit {
                    self.collect_expr(limit, &[], Some(&ScalarType::Int64), catalog, depth)?;
                }
                Ok(left_output
                    .into_iter()
                    .zip(right_output)
                    .map(|(left, right)| match (left, right) {
                        (Some(left), Some(right)) => common_type(&left, &right),
                        (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                        (None, None) => None,
                    })
                    .collect())
            }
            ParsedStatement::With {
                recursive,
                ctes,
                body,
            } => {
                self.collect_with_statement(*recursive, ctes, body, catalog, expected_output, depth)
            }
            ParsedStatement::Insert {
                table,
                columns,
                rows,
                on_conflict,
                returning,
            } => {
                let relation = resolve_dml_relation(table, CatalogTriggerEvent::Insert, catalog)?;
                let table = &relation.scope;
                let column_indexes = parameter_target_columns(columns, table)?;
                for row in rows {
                    for (expression, index) in row.iter().zip(&column_indexes) {
                        self.collect_expr(
                            expression,
                            &[],
                            Some(&table.columns()[*index].data_type),
                            catalog,
                            depth,
                        )?;
                    }
                }
                if let Some(on_conflict) = on_conflict {
                    if matches!(relation.target, DmlTarget::View(_)) {
                        return unsupported("ON CONFLICT is not supported for view DML");
                    }
                    self.collect_on_conflict(on_conflict, table, catalog, depth)?;
                }
                let inputs = parameter_table_inputs(table, table.name.clone(), 0, false);
                self.collect_projection(returning, &inputs, expected_output, catalog, depth)
            }
            ParsedStatement::Update {
                table,
                assignments,
                filter,
                returning,
            } => {
                let relation = resolve_dml_relation(table, CatalogTriggerEvent::Update, catalog)?;
                let table = &relation.scope;
                let inputs = parameter_table_inputs(table, table.name.clone(), 0, false);
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
                self.collect_projection(returning, &inputs, expected_output, catalog, depth)
            }
            ParsedStatement::Delete {
                table,
                filter,
                returning,
            } => {
                let relation = resolve_dml_relation(table, CatalogTriggerEvent::Delete, catalog)?;
                let table = &relation.scope;
                let inputs = parameter_table_inputs(table, table.name.clone(), 0, false);
                if let Some(filter) = filter {
                    self.collect_expr(filter, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
                }
                self.collect_projection(returning, &inputs, expected_output, catalog, depth)
            }
            ParsedStatement::Merge(merge) => {
                self.collect_merge(merge, catalog, expected_output, depth)
            }
            ParsedStatement::Explain { statement }
            | ParsedStatement::CreateView {
                query: statement, ..
            } => {
                self.collect_statement(statement, catalog, outer_inputs, expected_output, depth + 1)
            }
            ParsedStatement::Call {
                name, arguments, ..
            }
            | ParsedStatement::RoutineSelect {
                name, arguments, ..
            } => {
                self.collect_routine_arguments(name, arguments, catalog, depth)?;
                Ok(Vec::new())
            }
            ParsedStatement::ScalarSelect { projection } => {
                self.collect_projection(projection, &[], expected_output, catalog, depth)
            }
            ParsedStatement::SequenceValue { operation, .. } => {
                if let ParsedSequenceOperation::SetValue { value, .. } = operation {
                    self.collect_expr(value, &[], Some(&ScalarType::Int64), catalog, depth)?;
                }
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    fn collect_projection(
        &mut self,
        projection: &[ParsedProjection],
        inputs: &[InputColumn],
        expected_output: Option<&[Option<ScalarType>]>,
        catalog: &Catalog,
        depth: usize,
    ) -> Result<Vec<Option<ScalarType>>> {
        let mut output = Vec::new();
        for item in projection {
            match item {
                ParsedProjection::Wildcard => {
                    output.extend(
                        inputs
                            .iter()
                            .filter(|input| input.outer_depth == 0)
                            .map(|input| Some(input.data_type.clone())),
                    );
                }
                ParsedProjection::Expression { expr, .. } => {
                    let expected = expected_output
                        .and_then(|expected| expected.get(output.len()))
                        .and_then(Option::as_ref);
                    output.push(self.collect_expr(expr, inputs, expected, catalog, depth)?);
                }
            }
        }
        Ok(output)
    }

    fn collect_order_expr(
        &mut self,
        expression: &ParsedExpr,
        inputs: &[InputColumn],
        catalog: &Catalog,
        depth: usize,
    ) -> Result<()> {
        match self.collect_expr(expression, inputs, None, catalog, depth) {
            Ok(_) => Ok(()),
            Err(error) if error.sql_state == UNDEFINED_COLUMN => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn collect_expr(
        &mut self,
        expression: &ParsedExpr,
        inputs: &[InputColumn],
        expected: Option<&ScalarType>,
        catalog: &Catalog,
        depth: usize,
    ) -> Result<Option<ScalarType>> {
        if depth >= MAX_PARAMETER_SOLVER_DEPTH {
            return Err(DbError::new(
                "54001",
                "parameter type inference exceeded its expression depth limit",
            ));
        }
        match &expression.kind {
            ParsedExprKind::Column(name) => {
                Ok(Some(resolve_input_column(name, inputs)?.data_type.clone()))
            }
            ParsedExprKind::Literal(value) => Ok(value.scalar_type().or_else(|| expected.cloned())),
            ParsedExprKind::Parameter(index) => {
                if let Some(expected) = expected {
                    self.constrain(*index, expected, expression.position)?;
                }
                Ok(self.types.get(index).cloned())
            }
            ParsedExprKind::ResolvedParameter { index, data_type } => {
                self.constrain(*index, data_type, expression.position)?;
                if let Some(expected) = expected {
                    self.constrain(*index, expected, expression.position)?;
                }
                Ok(Some(data_type.clone()))
            }
            ParsedExprKind::ApplyValue { data_type, .. } => Ok(Some(data_type.clone())),
            ParsedExprKind::Unary { op, expr } => match op {
                UnaryOperator::Not => {
                    self.collect_expr(
                        expr,
                        inputs,
                        Some(&ScalarType::Boolean),
                        catalog,
                        depth + 1,
                    )?;
                    Ok(Some(ScalarType::Boolean))
                }
                UnaryOperator::Negate => {
                    self.collect_expr(expr, inputs, expected, catalog, depth + 1)
                }
            },
            ParsedExprKind::Cast {
                expr, data_type, ..
            } => {
                if let Some(index) = parsed_parameter_index(expr) {
                    self.constrain(index, data_type, expr.position)?;
                    self.collect_expr(expr, inputs, Some(data_type), catalog, depth + 1)?;
                } else {
                    self.collect_expr(expr, inputs, None, catalog, depth + 1)?;
                }
                Ok(Some(data_type.clone()))
            }
            ParsedExprKind::Array { elements, .. } => {
                let expected_element = match expected {
                    Some(ScalarType::Array { element }) => Some(element.as_ref()),
                    Some(_) => None,
                    None => None,
                };
                let mut element_type = expected_element.cloned();
                for element in elements {
                    let candidate =
                        self.collect_expr(element, inputs, expected_element, catalog, depth + 1)?;
                    element_type = match (element_type, candidate) {
                        (Some(left), Some(right)) => common_type(&left, &right),
                        (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                        (None, None) => None,
                    };
                }
                Ok(element_type.map(|element| ScalarType::Array {
                    element: Box::new(element),
                }))
            }
            ParsedExprKind::Function {
                function,
                arguments,
            } => {
                self.collect_scalar_function(*function, arguments, inputs, expected, catalog, depth)
            }
            ParsedExprKind::Binary { left, op, right } => {
                if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    self.collect_expr(
                        left,
                        inputs,
                        Some(&ScalarType::Boolean),
                        catalog,
                        depth + 1,
                    )?;
                    self.collect_expr(
                        right,
                        inputs,
                        Some(&ScalarType::Boolean),
                        catalog,
                        depth + 1,
                    )?;
                    return Ok(Some(ScalarType::Boolean));
                }
                let left_type = self.collect_expr(left, inputs, None, catalog, depth + 1)?;
                let right_type = self.collect_expr(right, inputs, None, catalog, depth + 1)?;
                if let (Some(index), Some(data_type)) =
                    (parsed_parameter_index(left), right_type.as_ref())
                {
                    self.constrain(index, data_type, left.position)?;
                }
                if let (Some(index), Some(data_type)) =
                    (parsed_parameter_index(right), left_type.as_ref())
                {
                    self.constrain(index, data_type, right.position)?;
                }
                let operand_type = match (left_type, right_type) {
                    (Some(left), Some(right)) => common_type(&left, &right),
                    (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                    (None, None) if is_arithmetic_operator(*op) => expected.cloned(),
                    (None, None) => None,
                };
                if let Some(operand_type) = &operand_type {
                    self.collect_expr(left, inputs, Some(operand_type), catalog, depth + 1)?;
                    self.collect_expr(right, inputs, Some(operand_type), catalog, depth + 1)?;
                }
                Ok(if is_arithmetic_operator(*op) {
                    operand_type
                } else {
                    Some(ScalarType::Boolean)
                })
            }
            ParsedExprKind::InList { expr, list, .. } => {
                let mut operand_type = self.collect_expr(expr, inputs, None, catalog, depth + 1)?;
                for candidate in list {
                    let candidate_type =
                        self.collect_expr(candidate, inputs, None, catalog, depth + 1)?;
                    operand_type = match (operand_type, candidate_type) {
                        (Some(left), Some(right)) => common_type(&left, &right),
                        (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                        (None, None) => None,
                    };
                }
                if let Some(operand_type) = &operand_type {
                    self.collect_expr(expr, inputs, Some(operand_type), catalog, depth + 1)?;
                    for candidate in list {
                        self.collect_expr(
                            candidate,
                            inputs,
                            Some(operand_type),
                            catalog,
                            depth + 1,
                        )?;
                    }
                }
                Ok(Some(ScalarType::Boolean))
            }
            ParsedExprKind::ScalarSubquery(subquery) => {
                let expected_output = expected.cloned().map(|data_type| vec![Some(data_type)]);
                let output = self.collect_statement(
                    subquery,
                    catalog,
                    inputs,
                    expected_output.as_deref(),
                    depth + 1,
                )?;
                Ok(output.first().cloned().flatten())
            }
            ParsedExprKind::Exists { subquery, .. } => {
                self.collect_statement(subquery, catalog, inputs, None, depth + 1)?;
                Ok(Some(ScalarType::Boolean))
            }
            ParsedExprKind::InSubquery { expr, subquery, .. }
            | ParsedExprKind::QuantifiedSubquery {
                left: expr,
                subquery,
                ..
            } => {
                let left_type = self.collect_expr(expr, inputs, None, catalog, depth + 1)?;
                let expected_output = left_type.clone().map(|data_type| vec![Some(data_type)]);
                let output = self.collect_statement(
                    subquery,
                    catalog,
                    inputs,
                    expected_output.as_deref(),
                    depth + 1,
                )?;
                let operand_type = match (left_type, output.first().cloned().flatten()) {
                    (Some(left), Some(right)) => common_type(&left, &right),
                    (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                    (None, None) => None,
                };
                if let Some(operand_type) = &operand_type {
                    self.collect_expr(expr, inputs, Some(operand_type), catalog, depth + 1)?;
                    let expected = [Some(operand_type.clone())];
                    self.collect_statement(subquery, catalog, inputs, Some(&expected), depth + 1)?;
                }
                Ok(Some(ScalarType::Boolean))
            }
            ParsedExprKind::RowSubquery { left, subquery, .. } => {
                let mut left_types = Vec::with_capacity(left.len());
                for expression in left {
                    left_types.push(self.collect_expr(
                        expression,
                        inputs,
                        None,
                        catalog,
                        depth + 1,
                    )?);
                }
                let output = self.collect_statement(
                    subquery,
                    catalog,
                    inputs,
                    Some(&left_types),
                    depth + 1,
                )?;
                for (expression, data_type) in left.iter().zip(output) {
                    if let Some(data_type) = data_type {
                        self.collect_expr(
                            expression,
                            inputs,
                            Some(&data_type),
                            catalog,
                            depth + 1,
                        )?;
                    }
                }
                Ok(Some(ScalarType::Boolean))
            }
            ParsedExprKind::Aggregate {
                function,
                argument,
                filter,
                ..
            } => {
                if let Some(filter) = filter {
                    self.collect_expr(
                        filter,
                        inputs,
                        Some(&ScalarType::Boolean),
                        catalog,
                        depth + 1,
                    )?;
                }
                let argument_type = argument
                    .as_deref()
                    .map(|argument| self.collect_expr(argument, inputs, None, catalog, depth + 1))
                    .transpose()?
                    .flatten();
                Ok(parameter_aggregate_type(*function, argument_type))
            }
            ParsedExprKind::Window { call, spec } => {
                self.collect_window(call, spec, inputs, catalog, depth)
            }
            ParsedExprKind::NamedWindow { call, .. } => self.collect_window(
                call,
                &ParsedWindowSpec {
                    window_name: None,
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    frame: None,
                },
                inputs,
                catalog,
                depth,
            ),
            ParsedExprKind::WindowValue { .. } => Ok(None),
        }
    }
}
