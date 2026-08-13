
fn evaluate_row_comparison(
    left: &[Value],
    op: BinaryOperator,
    right: &[Value],
    operand_types: &[ScalarType],
) -> Result<Value> {
    if left.len() != right.len() || left.len() != operand_types.len() {
        return Err(DbError::internal(
            "row comparison width does not match its bound schema",
        ));
    }
    if !matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq) {
        return Err(DbError::internal(
            "ordered row comparison reached the equality-only executor",
        ));
    }
    let mut saw_null = false;
    for ((left, right), operand_type) in left.iter().zip(right).zip(operand_types) {
        let left = super::coerce_value(left.clone(), operand_type)?;
        let right = super::coerce_value(right.clone(), operand_type)?;
        match evaluate_binary(left, BinaryOperator::Eq, right)? {
            Value::Boolean(false) => {
                return Ok(Value::Boolean(op == BinaryOperator::NotEq));
            }
            Value::Boolean(true) => {}
            Value::Null => saw_null = true,
            _ => {
                return Err(DbError::internal(
                    "row equality produced a non-boolean value",
                ));
            }
        }
    }
    if saw_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Boolean(op == BinaryOperator::Eq))
    }
}

impl AdvancedExecutionCursor {
    pub fn new(
        plan: AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
    ) -> Result<AdvancedExecutionCursor> {
        Self::with_options_and_cancellation(plan, context, ExecutionOptions::default(), None)
    }

    pub fn new_with_cancellation(
        plan: AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Result<AdvancedExecutionCursor> {
        Self::with_options_and_cancellation(
            plan,
            context,
            ExecutionOptions::default(),
            cancellation,
        )
    }

    pub fn with_options(
        plan: AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
        options: ExecutionOptions,
    ) -> Result<AdvancedExecutionCursor> {
        Self::with_options_and_cancellation(plan, context, options, None)
    }

    pub fn with_options_and_cancellation(
        plan: AdvancedExecutionPlan,
        context: &ExecutionContext<'_>,
        options: ExecutionOptions,
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Result<AdvancedExecutionCursor> {
        options.validate()?;
        let plan_depth = plan
            .joins
            .len()
            .saturating_add(plan.applies.len())
            .saturating_add(plan.windows.len())
            .saturating_add(1);
        if plan_depth > options.max_plan_depth {
            return Err(program_limit_error(format!(
                "advanced plan exceeds the depth limit of {}",
                options.max_plan_depth
            )));
        }
        let source = JoinedSource::new(&plan, context, &options)?;
        let filter = plan
            .filter
            .as_ref()
            .map(|expr| {
                ExpressionProgram::compile_with_limit(expr, false, options.max_expression_depth)
            })
            .transpose()?;
        let projection = if plan.aggregate {
            Vec::new()
        } else {
            plan.projection
                .iter()
                .map(|projection| {
                    ExpressionProgram::compile_with_limit(
                        &projection.expr,
                        false,
                        options.max_expression_depth,
                    )
                })
                .collect::<Result<Vec<_>>>()?
        };
        let group_programs = plan
            .aggregate
            .then(|| GroupPrograms::compile(&plan, options.max_expression_depth))
            .transpose()?;
        let aggregate_window_projection = aggregate_window_projection(&plan)?;
        let windows = plan
            .windows
            .iter()
            .map(|window| WindowProgram::compile(window, options.max_expression_depth))
            .collect::<Result<Vec<_>>>()?;
        let limit = plan
            .limit
            .as_ref()
            .map(|limit| {
                ExpressionProgram::compile_with_limit(limit, false, options.max_expression_depth)?
                    .evaluate(&[], context.params)
                    .and_then(limit_from_value)
            })
            .transpose()?
            .flatten();
        let offset_remaining = plan
            .offset
            .as_ref()
            .map(|offset| {
                ExpressionProgram::compile_with_limit(offset, false, options.max_expression_depth)?
                    .evaluate(&[], context.params)
                    .and_then(offset_from_value)
            })
            .transpose()?
            .unwrap_or(0);
        let memory = QueryMemoryContext::new(options.soft_memory_bytes, options.hard_memory_bytes)?;
        let mut nested_memory_peak = 0_usize;
        let applies = plan
            .applies
            .into_iter()
            .map(|apply| {
                let (apply, peak) = ApplyRuntime::new(apply, context, &options, &memory)?;
                nested_memory_peak = nested_memory_peak.max(peak);
                Ok(apply)
            })
            .collect::<Result<Vec<_>>>()?;
        let expression_stack = ExpressionStack::new(&memory)?;
        let apply_row_reservation = memory.try_reserve(0)?;
        let distinct_reservation = memory.try_reserve(0)?;
        Ok(Self {
            source,
            schema: plan.schema,
            filter,
            projection,
            group_programs,
            order_by: plan.order_by,
            offset_remaining,
            limit,
            emitted: 0,
            params: context.params.to_vec(),
            memory,
            pool: BatchPool::new(options.batch_rows),
            spill: SpillManager::new(options.spill_root.clone()),
            expression_stack,
            applies,
            windows,
            aggregate_window_projection,
            cancellation,
            apply_row_reservation,
            nested_memory_peak,
            output: None,
            in_flight: None,
            distinct: plan.distinct,
            distinct_rows: HashSet::new(),
            distinct_reservation,
            aggregate: plan.aggregate,
            options,
            exhausted: false,
        })
    }

    #[must_use]
    pub const fn memory(&self) -> &QueryMemoryContext {
        &self.memory
    }

    #[must_use]
    pub fn memory_peak_bytes(&self) -> usize {
        self.memory.peak_bytes().max(self.nested_memory_peak)
    }

    pub fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.check_cancelled()?;
        self.in_flight = None;
        if self.exhausted {
            return Ok(None);
        }
        if self.aggregate && self.output.is_none() {
            self.initialize_aggregate()?;
        }
        if self.aggregate_window_projection.is_some() && !self.windows.is_empty() {
            self.initialize_aggregate_windowed_source()?;
        } else if !self.aggregate && !self.windows.is_empty() && self.output.is_none() {
            self.initialize_windowed_source()?;
        } else if !self.aggregate && !self.order_by.is_empty() && self.output.is_none() {
            self.initialize_sorted_source()?;
        }

        let mut rows = self.pool.take();
        let mut reservation = self.memory.try_reserve(0)?;
        while rows.len() < self.options.batch_rows {
            if self.limit.is_some_and(|limit| self.emitted >= limit) {
                break;
            }
            let Some(row) = self.next_output_row()? else {
                break;
            };
            if self.offset_remaining > 0 {
                self.offset_remaining -= 1;
                continue;
            }
            let bytes = estimated_row_bytes(&row);
            reservation.grow(bytes)?;
            rows.push(row);
            self.emitted = self.emitted.saturating_add(1);
        }
        if rows.is_empty() {
            self.exhausted = true;
            self.output = None;
            self.pool.recycle(rows);
            return Ok(None);
        }
        self.in_flight = Some(reservation);
        Ok(Some(Batch {
            schema: self.schema.clone(),
            rows,
        }))
    }

    fn next_output_row(&mut self) -> Result<Option<Row>> {
        loop {
            let row = if let Some(output) = &mut self.output {
                let Some(row) = output.next_row(&self.memory)? else {
                    return Ok(None);
                };
                if self.aggregate {
                    row
                } else {
                    self.project_row(row)?
                }
            } else {
                let Some(row) = self.next_filtered_source_row()? else {
                    return Ok(None);
                };
                self.project_row(row)?
            };
            if self.keep_distinct_row(&row)? {
                return Ok(Some(row));
            }
        }
    }

    fn keep_distinct_row(&mut self, row: &Row) -> Result<bool> {
        if !self.distinct {
            return Ok(true);
        }
        let key = DistinctRowKey(row.values.iter().map(distinct_value_key).collect());
        if self.distinct_rows.contains(&key) {
            return Ok(false);
        }
        self.distinct_reservation
            .grow(estimated_distinct_row_key_bytes(&key))?;
        self.distinct_rows.insert(key);
        Ok(true)
    }

    fn matches_filter(&mut self, row: &Row) -> Result<bool> {
        let Some(filter) = &self.filter else {
            return Ok(true);
        };
        match filter.evaluate_reusing(&row.values, &self.params, &mut self.expression_stack)? {
            Value::Boolean(matches) => Ok(matches),
            Value::Null => Ok(false),
            _ => Err(DbError::new("42804", "predicate must evaluate to boolean")),
        }
    }

    fn project_row(&mut self, row: Row) -> Result<Row> {
        self.projection
            .iter()
            .map(|program| {
                program.evaluate_reusing(&row.values, &self.params, &mut self.expression_stack)
            })
            .collect::<Result<Vec<_>>>()
            .map(Row::new)
    }

    fn next_filtered_source_row(&mut self) -> Result<Option<Row>> {
        loop {
            self.check_cancelled()?;
            let row = self.source.next_row(
                &self.params,
                &mut self.memory,
                &mut self.spill,
                &mut self.expression_stack,
            )?;
            self.nested_memory_peak = self
                .nested_memory_peak
                .max(self.source.nested_memory_peak());
            let Some(mut row) = row else {
                return Ok(None);
            };
            self.apply_row_reservation.resize(0)?;
            for apply in &mut self.applies {
                let (value, nested_peak) =
                    apply.evaluate(&row.values, &self.params, &mut self.expression_stack)?;
                self.nested_memory_peak = self.nested_memory_peak.max(nested_peak);
                self.apply_row_reservation
                    .grow(estimated_value_bytes(&value))?;
                row.values.push(value);
            }
            if self.matches_filter(&row)? {
                return Ok(Some(row));
            }
        }
    }

    fn initialize_sorted_source(&mut self) -> Result<()> {
        let mut builder = RowsOutputBuilder::new(
            &self.order_by,
            &self.memory,
            self.options.max_expression_depth,
        )?;
        while let Some(row) = self.next_filtered_source_row()? {
            builder.push(
                row,
                &self.params,
                &mut self.expression_stack,
                &self.memory,
                &mut self.spill,
            )?;
        }
        self.output = Some(builder.finish(&self.memory, &mut self.spill)?);
        Ok(())
    }

    fn initialize_windowed_source(&mut self) -> Result<()> {
        let windows = std::mem::take(&mut self.windows);
        let mut builder = WindowRowStoreBuilder::new(&self.memory)?;
        while let Some(row) = self.next_filtered_source_row()? {
            self.check_cancelled()?;
            builder.push(row, &self.memory, &mut self.spill)?;
        }
        let mut rows = builder.finish(&self.memory)?;
        for window in &windows {
            rows = window.apply(
                rows,
                &self.params,
                &mut self.expression_stack,
                &self.memory,
                &mut self.spill,
                self.cancellation.as_deref(),
            )?;
        }
        self.install_window_output(rows)
    }

    fn initialize_aggregate_windowed_source(&mut self) -> Result<()> {
        let output = self
            .output
            .take()
            .ok_or_else(|| DbError::internal("grouped window input is unavailable"))?;
        let mut rows = output.into_window_store(&self.memory, &mut self.spill)?;
        let windows = std::mem::take(&mut self.windows);
        for window in &windows {
            rows = window.apply(
                rows,
                &self.params,
                &mut self.expression_stack,
                &self.memory,
                &mut self.spill,
                self.cancellation.as_deref(),
            )?;
        }
        let projection = self
            .aggregate_window_projection
            .take()
            .ok_or_else(|| DbError::internal("grouped window projection is unavailable"))?;
        let mut projected = WindowRowStoreBuilder::new(&self.memory)?;
        let row_count = rows.len();
        for index in 0..row_count {
            self.check_cancelled()?;
            let ReservedRow {
                mut row,
                mut reservation,
            } = rows.read(index, &self.memory)?;
            let mut values = std::mem::take(&mut row.values)
                .into_iter()
                .map(Some)
                .collect::<Vec<_>>();
            row.values = projection
                .iter()
                .map(|index| {
                    values
                        .get_mut(*index)
                        .and_then(Option::take)
                        .ok_or_else(|| {
                            DbError::internal(
                                "grouped window projection index is out of bounds or duplicated",
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            reservation.resize(estimated_row_bytes(&row))?;
            projected.push_transferred(row, &mut reservation, &self.memory, &mut self.spill)?;
        }
        let rows = projected.finish(&self.memory)?;
        self.install_window_output(rows)
    }

    fn install_window_output(&mut self, rows: WindowRowStore) -> Result<()> {
        if self.order_by.is_empty() {
            self.output = Some(match rows {
                WindowRowStore::Memory { rows, reservation } => RowsOutput::Memory {
                    rows,
                    offset: 0,
                    reservation: Some(reservation),
                },
                WindowRowStore::Spill(store) => RowsOutput::Indexed {
                    store,
                    offset: 0,
                    current_reservation: None,
                },
            });
            return Ok(());
        }
        let mut builder = RowsOutputBuilder::new(
            &self.order_by,
            &self.memory,
            self.options.max_expression_depth,
        )?;
        match rows {
            WindowRowStore::Memory {
                rows,
                mut reservation,
            } => {
                for row in rows {
                    builder.push_transferred(
                        row,
                        &self.params,
                        &mut self.expression_stack,
                        &self.memory,
                        &mut self.spill,
                        &mut reservation,
                    )?;
                }
                if reservation.bytes() != 0 {
                    return Err(DbError::internal(
                        "window row reservation was not fully transferred",
                    ));
                }
            }
            WindowRowStore::Spill(mut store) => {
                for index in 0..store.len {
                    self.check_cancelled()?;
                    let ReservedRow {
                        row,
                        mut reservation,
                    } = store.read(index, &self.memory)?;
                    builder.push_transferred(
                        row,
                        &self.params,
                        &mut self.expression_stack,
                        &self.memory,
                        &mut self.spill,
                        &mut reservation,
                    )?;
                }
            }
        }
        self.output = Some(builder.finish(&self.memory, &mut self.spill)?);
        Ok(())
    }

    fn check_cancelled(&self) -> Result<()> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(|cancellation| cancellation.load(AtomicOrdering::Acquire))
        {
            Err(DbError::new("57014", "query was cancelled"))
        } else {
            Ok(())
        }
    }

    fn initialize_aggregate(&mut self) -> Result<()> {
        let programs = self
            .group_programs
            .take()
            .ok_or_else(|| DbError::internal("aggregate programs are unavailable"))?;
        let mut groups = Vec::<GroupAccumulator>::new();
        let mut group_reservation = self.memory.try_reserve(0)?;
        let mut spill_paths = None;
        let unjoined_rows = if self.applies.is_empty()
            && self.filter.is_none()
            && programs.group_by.is_empty()
            && programs.aggregate_specs.iter().all(|spec| !spec.distinct)
        {
            self.source.take_unjoined_rows()
        } else {
            None
        };
        if let Some(rows) = unjoined_rows {
            if let Some((first, remaining)) = rows.split_first() {
                let mut group = GroupAccumulator::new(
                    Vec::new(),
                    first.clone(),
                    0,
                    &programs.aggregate_specs,
                    &self.params,
                    &mut self.expression_stack,
                )?;
                for row in remaining {
                    group.update(
                        &programs.aggregate_specs,
                        row,
                        &self.params,
                        &mut self.expression_stack,
                    )?;
                }
                group_reservation.grow(group.estimated_bytes())?;
                groups.push(group);
            } else {
                let group = GroupAccumulator::empty(&programs.aggregate_specs);
                group_reservation.grow(group.estimated_bytes())?;
                groups.push(group);
            }
        } else {
            let mut ordinal = 0_u64;
            while let Some(row) = self.next_filtered_source_row()? {
                let key = programs.group_key(&row, &self.params, &mut self.expression_stack)?;
                if let Some(group) = groups.iter_mut().find(|group| group.key == key) {
                    let before = group.estimated_bytes();
                    group.update(
                        &programs.aggregate_specs,
                        &row,
                        &self.params,
                        &mut self.expression_stack,
                    )?;
                    let after = group.estimated_bytes();
                    if after > before {
                        group_reservation.grow(after - before)?;
                    } else if before > after {
                        group_reservation
                            .resize(group_reservation.bytes().saturating_sub(before - after))?;
                    }
                } else {
                    let group = GroupAccumulator::new(
                        key,
                        row,
                        ordinal,
                        &programs.aggregate_specs,
                        &self.params,
                        &mut self.expression_stack,
                    )?;
                    let bytes = group.estimated_bytes();
                    if !groups.is_empty()
                        && self.memory.current_bytes().saturating_add(bytes)
                            > self.memory.soft_limit_bytes()
                    {
                        if spill_paths.is_none() {
                            spill_paths =
                                Some(self.spill.partition_paths("aggregate", HASH_PARTITIONS)?);
                        }
                        let paths = spill_paths.as_ref().ok_or_else(|| {
                            DbError::internal("aggregate spill paths disappeared")
                        })?;
                        self.spill
                            .write_group_partials(paths, &groups, &self.memory)?;
                        groups.clear();
                        group_reservation.resize(0)?;
                    }
                    group_reservation.grow(bytes)?;
                    groups.push(group);
                }
                if self.memory.current_bytes() > self.memory.soft_limit_bytes() {
                    if spill_paths.is_none() {
                        spill_paths =
                            Some(self.spill.partition_paths("aggregate", HASH_PARTITIONS)?);
                    }
                    let paths = spill_paths
                        .as_ref()
                        .ok_or_else(|| DbError::internal("aggregate spill paths disappeared"))?;
                    self.spill
                        .write_group_partials(paths, &groups, &self.memory)?;
                    groups.clear();
                    group_reservation.resize(0)?;
                }
                ordinal = ordinal.saturating_add(1);
            }
        }

        if programs.group_by.is_empty() && groups.is_empty() {
            let group = GroupAccumulator::empty(&programs.aggregate_specs);
            let bytes = group.estimated_bytes();
            group_reservation.grow(bytes)?;
            groups.push(group);
        }

        if let Some(paths) = &spill_paths
            && !groups.is_empty()
        {
            self.spill
                .write_group_partials(paths, &groups, &self.memory)?;
            groups.clear();
            group_reservation.resize(0)?;
        }

        let aggregate_order_by = if self.aggregate_window_projection.is_some() {
            &[][..]
        } else {
            self.order_by.as_slice()
        };
        let mut output = RowsOutputBuilder::new(
            aggregate_order_by,
            &self.memory,
            self.options.max_expression_depth,
        )?;
        if let Some(paths) = spill_paths {
            for path in paths {
                if !path.exists() {
                    continue;
                }
                let partition_groups = self.spill.read_and_merge_groups(
                    &path,
                    &self.memory,
                    &programs.aggregate_specs,
                )?;
                for group in partition_groups.values {
                    if let Some(row) =
                        programs.project_group(&group, &self.params, &mut self.expression_stack)?
                    {
                        output.push(
                            row,
                            &self.params,
                            &mut self.expression_stack,
                            &self.memory,
                            &mut self.spill,
                        )?;
                    }
                }
            }
        } else {
            for group in groups {
                if let Some(row) =
                    programs.project_group(&group, &self.params, &mut self.expression_stack)?
                {
                    output.push(
                        row,
                        &self.params,
                        &mut self.expression_stack,
                        &self.memory,
                        &mut self.spill,
                    )?;
                }
            }
        }
        drop(group_reservation);
        self.output = Some(output.finish(&self.memory, &mut self.spill)?);
        self.group_programs = Some(programs);
        Ok(())
    }
}

struct JoinedSource {
    base: Arc<Vec<Row>>,
    base_offset: usize,
    joins: Vec<JoinRuntime>,
    prefixes: Vec<Row>,
    frames: Vec<JoinFrame>,
    depth: usize,
    nested_memory_peak: usize,
}

enum FastJoinStep {
    Row(Row),
    Exhausted,
    Fallback,
}
