
impl ExecutionCursor {
    pub fn new(plan: &PlanNode, context: &ExecutionContext<'_>, schema: Schema) -> Result<Self> {
        Self::with_options(plan, context, schema, ExecutionOptions::default())
    }

    pub fn new_with_table_provider(
        plan: &PlanNode,
        context: &ExecutionContext<'_>,
        schema: Schema,
        provider: &dyn TableProvider,
    ) -> Result<Self> {
        Self::with_options_and_table_provider(
            plan,
            context,
            schema,
            ExecutionOptions::default(),
            Some(provider),
        )
    }

    pub fn with_options(
        plan: &PlanNode,
        context: &ExecutionContext<'_>,
        schema: Schema,
        options: ExecutionOptions,
    ) -> Result<Self> {
        Self::with_options_and_table_provider(plan, context, schema, options, None)
    }

    pub fn with_options_and_table_provider(
        plan: &PlanNode,
        context: &ExecutionContext<'_>,
        schema: Schema,
        options: ExecutionOptions,
        table_provider: Option<&dyn TableProvider>,
    ) -> Result<Self> {
        options.validate()?;
        let (source, arena, frames) = build_pipeline(plan, context, &options, table_provider)?;
        let sort_positions = frames
            .as_slice()
            .iter()
            .enumerate()
            .filter_map(|(position, id)| {
                arena
                    .get(*id)
                    .ok()
                    .is_some_and(|operator| matches!(operator, OperatorFrame::Sort(_)))
                    .then_some(position)
            })
            .collect::<Vec<_>>();
        if sort_positions.len() > 1 {
            return Err(program_limit_error(
                "a physical pipeline may contain at most one Sort frame",
            ));
        }
        let sort_position = sort_positions.first().copied();
        let memory = QueryMemoryContext::new(options.soft_memory_bytes, options.hard_memory_bytes)?;
        let expression_stack = ExpressionStack::new(&memory)?;
        Ok(Self {
            source,
            arena,
            frames,
            sort_position,
            sort_output: None,
            schema,
            params: context.params.to_vec(),
            memory,
            pool: BatchPool::new(options.batch_rows),
            spill: SpillManager::new(options.spill_root.clone()),
            expression_stack,
            in_flight: None,
            options,
            exhausted: false,
        })
    }

    #[must_use]
    pub const fn memory(&self) -> &QueryMemoryContext {
        &self.memory
    }

    pub fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.in_flight = None;
        if self.exhausted {
            return Ok(None);
        }
        if self.sort_position.is_some() && self.sort_output.is_none() {
            self.initialize_sort()?;
        }

        if let Some(sort_position) = self.sort_position {
            let mut output = self.pool.take();
            let mut reservation = self.memory.try_reserve(0)?;
            while output.len() < self.options.batch_rows {
                let Some(row) = self.next_sorted_row()? else {
                    break;
                };
                let row = apply_row_frames(
                    &mut self.arena,
                    &self.frames.as_slice()[sort_position + 1..],
                    &self.params,
                    &mut self.expression_stack,
                    Cow::Owned(row),
                )?;
                if let Some(row) = row {
                    reservation.grow(estimated_row_bytes(&row))?;
                    output.push(row);
                }
            }
            if output.is_empty() {
                self.exhausted = true;
                self.pool.recycle(output);
                return Ok(None);
            }
            self.in_flight = Some(reservation);
            return Ok(Some(Batch {
                schema: self.schema.clone(),
                rows: output,
            }));
        }

        loop {
            let Some(mut leased) = self
                .source
                .next_chunk(self.options.batch_rows, &self.memory)?
            else {
                self.exhausted = true;
                return Ok(None);
            };
            if !apply_chunk_frames(
                &mut self.arena,
                self.frames.as_slice(),
                &self.params,
                &mut self.expression_stack,
                &mut leased,
            )? {
                leased.recycle()?;
                continue;
            }
            if leased.chunk().is_empty() {
                leased.recycle()?;
                continue;
            }
            let output_reservation = self
                .memory
                .try_reserve(leased.chunk().estimated_selected_row_bytes()?)?;
            let rows = leased.take_rows()?;
            leased.recycle()?;
            if rows.is_empty() {
                continue;
            }
            self.in_flight = Some(output_reservation);
            return Ok(Some(Batch {
                schema: self.schema.clone(),
                rows,
            }));
        }
    }

    fn initialize_sort(&mut self) -> Result<()> {
        let sort_position = self
            .sort_position
            .ok_or_else(|| DbError::internal("Sort initialization has no Sort frame"))?;
        let sort_id = *self
            .frames
            .as_slice()
            .get(sort_position)
            .ok_or_else(|| DbError::internal("Sort frame position is invalid"))?;
        let OperatorFrame::Sort(order_by) = self.arena.get(sort_id)? else {
            return Err(DbError::internal("Sort frame index is invalid"));
        };
        let (mut order_by, sort_programs) =
            compile_sort_orders(order_by, self.options.max_expression_depth)?;
        let mut rows = Vec::new();
        let mut rows_reservation = self.memory.try_reserve(0)?;
        let mut run_paths = Vec::new();
        loop {
            let Some(mut input) = self
                .source
                .next_chunk(self.options.batch_rows, &self.memory)?
            else {
                break;
            };
            if !apply_chunk_frames(
                &mut self.arena,
                &self.frames.as_slice()[..sort_position],
                &self.params,
                &mut self.expression_stack,
                &mut input,
            )? {
                input.recycle()?;
                continue;
            }
            for logical_row in 0..input.chunk().len() {
                let row = input.chunk().row(logical_row)?;
                let Some(mut row) = apply_row_frames(
                    &mut self.arena,
                    &[],
                    &self.params,
                    &mut self.expression_stack,
                    Cow::Owned(row),
                )?
                else {
                    continue;
                };
                materialize_sort_keys(
                    &mut row,
                    &mut order_by,
                    &sort_programs,
                    &self.params,
                    &mut self.expression_stack,
                )?;
                let row_bytes = estimated_row_bytes(&row);
                if !rows.is_empty() && self.memory.would_cross_soft_limit(row_bytes) {
                    sort_rows(&mut rows, &order_by)?;
                    run_paths.push(self.spill.write_sorted_run(&rows, &self.memory)?);
                    rows.clear();
                    rows_reservation.resize(0)?;
                }
                rows_reservation.grow(row_bytes)?;
                rows.push(row);
            }
            input.recycle()?;
        }

        if run_paths.is_empty() {
            sort_rows(&mut rows, &order_by)?;
            self.sort_output = Some(SortOutput::Memory {
                rows: rows.into_iter(),
                _reservation: rows_reservation,
            });
        } else {
            if !rows.is_empty() {
                sort_rows(&mut rows, &order_by)?;
                run_paths.push(self.spill.write_sorted_run(&rows, &self.memory)?);
            }
            drop(rows_reservation);
            let run_paths = self
                .spill
                .compact_sorted_runs(run_paths, &order_by, &self.memory)?;
            self.sort_output = Some(SortOutput::Runs(SpillMergeCursor::open(
                &run_paths,
                &order_by,
                &self.memory,
            )?));
        }
        Ok(())
    }

    fn next_sorted_row(&mut self) -> Result<Option<Row>> {
        let Some(output) = &mut self.sort_output else {
            return Ok(None);
        };
        match output {
            SortOutput::Memory {
                rows,
                _reservation: _,
            } => Ok(rows.next()),
            SortOutput::Runs(merge) => {
                let sort_position = self
                    .sort_position
                    .ok_or_else(|| DbError::internal("Sort output has no frame"))?;
                let sort_id = *self
                    .frames
                    .as_slice()
                    .get(sort_position)
                    .ok_or_else(|| DbError::internal("Sort output position is invalid"))?;
                let OperatorFrame::Sort(order_by) = self.arena.get(sort_id)? else {
                    return Err(DbError::internal("Sort output frame is invalid"));
                };
                merge.pop_next(order_by, &self.memory)
            }
        }
    }
}

fn apply_chunk_frames(
    arena: &mut OperatorArena,
    frames: &[OperatorId],
    params: &[Value],
    expression_stack: &mut ExpressionStack,
    chunk: &mut LeasedDataChunk,
) -> Result<bool> {
    for id in frames {
        match arena.get_mut(*id)? {
            OperatorFrame::Filter(program) => {
                let direct = match &program.fast_path {
                    Some(FastExpression::ColumnLiteralBinary {
                        column,
                        column_type,
                        operator,
                        literal,
                        literal_type,
                        target,
                    }) if column_type == literal_type
                        && matches!(target, ScalarType::Boolean)
                        && !matches!(column_type, ScalarType::Enum { .. }) =>
                    {
                        chunk
                            .chunk_mut()
                            .retain_literal_comparison(*column, literal, *operator)
                    }
                    _ => None,
                };
                if let Some(result) = direct {
                    result?;
                } else {
                    chunk.chunk_mut().retain_selected(|chunk, physical_row| {
                        match program.evaluate_chunk_row(
                            chunk,
                            physical_row,
                            params,
                            expression_stack,
                        )? {
                            Value::Boolean(matches) => Ok(matches),
                            Value::Null => Ok(false),
                            _ => Err(DbError::new("42804", "predicate must evaluate to boolean")),
                        }
                    })?;
                }
                if chunk.chunk().is_empty() {
                    return Ok(false);
                }
            }
            OperatorFrame::Projection(programs) => {
                let direct = programs
                    .iter()
                    .map(ExpressionProgram::column_projection)
                    .collect::<Option<Vec<_>>>();
                let projected_in_place = if let Some(projections) = direct {
                    chunk.chunk_mut().project_columns_in_place(&projections)?
                } else {
                    false
                };
                if projected_in_place {
                    chunk.refresh_reservation()?;
                } else {
                    let rows = (0..chunk.chunk().len())
                        .map(|logical_row| {
                            let row = chunk.chunk().row(logical_row)?;
                            programs
                                .iter()
                                .map(|program| {
                                    program.evaluate_reusing(&row.values, params, expression_stack)
                                })
                                .collect::<Result<Vec<_>>>()
                                .map(Row::new)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    chunk.replace(DataChunk::from_rows(&rows)?)?;
                }
            }
            OperatorFrame::Limit { remaining } => {
                if *remaining == 0 {
                    return Ok(false);
                }
                let emitted = chunk.chunk().len().min(*remaining);
                chunk.chunk_mut().selection_mut().truncate(emitted);
                *remaining -= emitted;
                if emitted == 0 {
                    return Ok(false);
                }
            }
            OperatorFrame::Offset { remaining } => {
                let skipped = chunk.chunk().len().min(*remaining);
                chunk.chunk_mut().selection_mut().discard_prefix(skipped);
                *remaining -= skipped;
                if chunk.chunk().is_empty() {
                    return Ok(false);
                }
            }
            OperatorFrame::Sort(_) => {
                return Err(DbError::internal(
                    "Sort frame reached the streaming chunk evaluator",
                ));
            }
        }
    }
    Ok(!chunk.chunk().is_empty())
}

fn apply_row_frames(
    arena: &mut OperatorArena,
    frames: &[OperatorId],
    params: &[Value],
    expression_stack: &mut ExpressionStack,
    mut row: Cow<'_, Row>,
) -> Result<Option<Row>> {
    for id in frames {
        match arena.get_mut(*id)? {
            OperatorFrame::Filter(program) => {
                match program.evaluate_reusing(&row.values, params, expression_stack)? {
                    Value::Boolean(true) => {}
                    Value::Boolean(false) | Value::Null => return Ok(None),
                    _ => {
                        return Err(DbError::new("42804", "predicate must evaluate to boolean"));
                    }
                }
            }
            OperatorFrame::Projection(programs) => {
                let mut values = Vec::with_capacity(programs.len());
                for program in programs {
                    values.push(program.evaluate_reusing(&row.values, params, expression_stack)?);
                }
                row = Cow::Owned(Row::new(values));
            }
            OperatorFrame::Limit { remaining } => {
                if *remaining == 0 {
                    return Ok(None);
                }
                *remaining -= 1;
            }
            OperatorFrame::Offset { remaining } => {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Ok(None);
                }
            }
            OperatorFrame::Sort(_) => {
                return Err(DbError::internal(
                    "Sort frame reached the streaming row evaluator",
                ));
            }
        }
    }
    Ok(Some(row.into_owned()))
}

fn build_pipeline(
    plan: &PlanNode,
    context: &ExecutionContext<'_>,
    options: &ExecutionOptions,
    table_provider: Option<&dyn TableProvider>,
) -> Result<(SourceCursor, OperatorArena, FrameStack)> {
    let mut node = plan;
    let mut arena = OperatorArena::default();
    let mut frames = FrameStack::default();
    let source = loop {
        if frames.len() >= options.max_plan_depth {
            return Err(program_limit_error(format!(
                "physical plan exceeds the depth limit of {}",
                options.max_plan_depth
            )));
        }
        match &node.kind {
            PlanKind::Scan {
                table_id, access, ..
            } => {
                break build_source(
                    *table_id,
                    access,
                    context,
                    options.max_expression_depth,
                    table_provider,
                )?;
            }
            PlanKind::Filter { predicate, input } => {
                let id = arena.insert(OperatorFrame::Filter(Box::new(
                    ExpressionProgram::compile_with_limit(
                        predicate,
                        false,
                        options.max_expression_depth,
                    )?,
                )));
                frames.push(id);
                node = input;
            }
            PlanKind::Projection { expressions, input } => {
                let id = arena.insert(OperatorFrame::Projection(compile_projections(
                    expressions,
                    options.max_expression_depth,
                )?));
                frames.push(id);
                node = input;
            }
            PlanKind::Sort { order_by, input } => {
                let id = arena.insert(OperatorFrame::Sort(order_by.clone()));
                frames.push(id);
                node = input;
            }
            PlanKind::Offset { offset, input } => {
                let id = arena.insert(OperatorFrame::Offset {
                    remaining: evaluate_offset_program(
                        &ExpressionProgram::compile_with_limit(
                            offset,
                            false,
                            options.max_expression_depth,
                        )?,
                        context.params,
                    )?,
                });
                frames.push(id);
                node = input;
            }
            PlanKind::Limit { limit, input } => {
                let id = arena.insert(OperatorFrame::Limit {
                    remaining: evaluate_limit_program(
                        &ExpressionProgram::compile_with_limit(
                            limit,
                            false,
                            options.max_expression_depth,
                        )?,
                        context.params,
                    )?,
                });
                frames.push(id);
                node = input;
            }
        }
    };
    frames.reverse();
    Ok((source, arena, frames))
}

fn build_source(
    table_id: TableId,
    access: &AccessPath,
    context: &ExecutionContext<'_>,
    max_expression_depth: usize,
    table_provider: Option<&dyn TableProvider>,
) -> Result<SourceCursor> {
    match access {
        AccessPath::Empty => Ok(SourceCursor::Empty),
        AccessPath::Sequential => match table_provider {
            Some(provider) => provider.scan(table_id).map(SourceCursor::Sequential),
            None => SnapshotTableProvider::new(context.tables)
                .scan(table_id)
                .map(SourceCursor::Sequential),
        },
        AccessPath::Index {
            index_id,
            data_type,
            operator,
            value,
            ..
        } => {
            let program =
                ExpressionProgram::compile_with_limit(value, false, max_expression_depth)?;
            let value = coerce_value(program.evaluate(&[], context.params)?, data_type)?;
            if value.is_null() {
                return Ok(SourceCursor::Empty);
            }
            let rows = context
                .tables
                .get(&table_id)
                .cloned()
                .unwrap_or_else(|| Arc::new(Vec::new()));
            let key = IndexKey::from_typed_values(
                std::slice::from_ref(&value),
                std::slice::from_ref(data_type),
            )?;
            let tree = context
                .indexes
                .get(index_id)
                .ok_or_else(|| DbError::internal("planned index is unavailable"))?;
            let entries = match operator {
                BinaryOperator::Eq => tree.owned_get_iter(key),
                BinaryOperator::Lt => tree.owned_range_iter(Bound::Unbounded, Bound::Excluded(key)),
                BinaryOperator::LtEq => {
                    tree.owned_range_iter(Bound::Unbounded, Bound::Included(key))
                }
                BinaryOperator::Gt => tree.owned_range_iter(Bound::Excluded(key), Bound::Unbounded),
                BinaryOperator::GtEq => {
                    tree.owned_range_iter(Bound::Included(key), Bound::Unbounded)
                }
                _ => {
                    return Err(DbError::internal(
                        "optimizer selected an unsupported index operator",
                    ));
                }
            };
            Ok(SourceCursor::Index { rows, entries })
        }
    }
}

fn compile_projections(
    projections: &[BoundProjection],
    max_expression_depth: usize,
) -> Result<Vec<ExpressionProgram>> {
    projections
        .iter()
        .map(|projection| {
            ExpressionProgram::compile_with_limit(&projection.expr, false, max_expression_depth)
        })
        .collect()
}

fn compile_sort_orders(
    order_by: &[BoundOrder],
    max_expression_depth: usize,
) -> Result<(Vec<BoundOrder>, Vec<Option<ExpressionProgram>>)> {
    let mut effective = order_by.to_vec();
    let programs = effective
        .iter_mut()
        .map(|order| {
            order
                .expression
                .take()
                .map(|expression| {
                    order.column_index = usize::MAX;
                    ExpressionProgram::compile_with_limit(&expression, false, max_expression_depth)
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((effective, programs))
}

fn materialize_sort_keys(
    row: &mut Row,
    order_by: &mut [BoundOrder],
    programs: &[Option<ExpressionProgram>],
    params: &[Value],
    stack: &mut ExpressionStack,
) -> Result<()> {
    let base_width = row.values.len();
    let keys = programs
        .iter()
        .map(|program| {
            program
                .as_ref()
                .map(|program| program.evaluate_reusing(&row.values, params, stack))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    for (ordinal, (order, key)) in order_by.iter_mut().zip(keys).enumerate() {
        let Some(key) = key else {
            continue;
        };
        let expected_index = base_width.saturating_add(ordinal);
        if order.column_index == usize::MAX {
            order.column_index = expected_index;
        } else if order.column_index != expected_index {
            return Err(DbError::internal(
                "materialized sort-key layout changed between rows",
            ));
        }
        while row.values.len() < expected_index {
            row.values.push(Value::Null);
        }
        row.values.push(key);
    }
    Ok(())
}

fn sort_rows(rows: &mut [Row], order_by: &[BoundOrder]) -> Result<()> {
    let mut error = None;
    rows.sort_by(|left, right| {
        compare_rows(left, right, order_by).unwrap_or_else(|sort_error| {
            error = Some(sort_error);
            Ordering::Equal
        })
    });
    error.map_or(Ok(()), Err)
}

fn spill_io_error(error: std::io::Error) -> DbError {
    DbError::new("58030", "query spill I/O failed")
        .with_detail(error.to_string())
        .with_hint("Check free disk space and permissions for the configured spill directory.")
}

struct ReservedSpillWriter {
    writer: BufWriter<File>,
    _reservation: Reservation,
}

impl Write for ReservedSpillWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.writer.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

struct ReservedSpillReader {
    reader: BufReader<File>,
    _reservation: Reservation,
}

impl Read for ReservedSpillReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buffer)
    }
}

impl Seek for ReservedSpillReader {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.reader.seek(position)
    }
}

fn spill_io_buffer_bytes(memory: &MemoryGrant) -> usize {
    memory
        .hard_limit_bytes()
        .checked_div(MAX_CONCURRENT_SPILL_STREAMS.saturating_mul(4))
        .unwrap_or(0)
        .clamp(1, DEFAULT_SPILL_IO_BUFFER_BYTES)
}

fn reserve_spill_writer(file: File, memory: &MemoryGrant) -> Result<ReservedSpillWriter> {
    let capacity = spill_io_buffer_bytes(memory);
    let reservation = memory.try_reserve(capacity)?;
    Ok(ReservedSpillWriter {
        writer: BufWriter::with_capacity(capacity, file),
        _reservation: reservation,
    })
}

fn create_spill_writer(path: &Path, memory: &MemoryGrant) -> Result<ReservedSpillWriter> {
    let file = File::create(path).map_err(spill_io_error)?;
    let mut writer = reserve_spill_writer(file, memory)?;
    writer.write_all(&SPILL_MAGIC).map_err(spill_io_error)?;
    writer
        .write_all(&SPILL_VERSION.to_le_bytes())
        .map_err(spill_io_error)?;
    Ok(writer)
}

fn open_spill_reader(path: &Path, memory: &MemoryGrant) -> Result<ReservedSpillReader> {
    let file = File::open(path).map_err(spill_io_error)?;
    let capacity = spill_io_buffer_bytes(memory);
    let reservation = memory.try_reserve(capacity)?;
    let mut reader = ReservedSpillReader {
        reader: BufReader::with_capacity(capacity, file),
        _reservation: reservation,
    };
    let mut magic = [0_u8; SPILL_MAGIC.len()];
    reader
        .read_exact(&mut magic)
        .map_err(|error| spill_corruption("spill header is truncated", error))?;
    if magic != SPILL_MAGIC {
        return Err(DbError::new("XX001", "query spill magic is invalid"));
    }
    let mut version = [0_u8; 2];
    reader
        .read_exact(&mut version)
        .map_err(|error| spill_corruption("spill version is truncated", error))?;
    if u16::from_le_bytes(version) != SPILL_VERSION {
        return Err(DbError::new(
            "XX001",
            "query spill format version is unsupported",
        ));
    }
    Ok(reader)
}

struct SpillRecord<T> {
    value: T,
    _reservation: Reservation,
}

struct ReservedSpillBuffer {
    bytes: Vec<u8>,
    reservation: Reservation,
    failure: Option<DbError>,
}

impl ReservedSpillBuffer {
    fn new(memory: &MemoryGrant) -> Result<Self> {
        Ok(Self {
            bytes: Vec::new(),
            reservation: memory.try_reserve(0)?,
            failure: None,
        })
    }
}

impl Write for ReservedSpillBuffer {
    fn write(&mut self, source: &[u8]) -> std::io::Result<usize> {
        let required = self
            .bytes
            .len()
            .checked_add(source.len())
            .ok_or_else(|| std::io::Error::other("spill buffer length overflow"))?;
        if required > self.bytes.capacity() {
            let old_capacity = self.bytes.capacity();
            let requested = required - old_capacity;
            if let Err(error) = self.reservation.grow(requested) {
                self.failure = Some(error);
                return Err(std::io::Error::other(
                    "spill buffer exceeds query memory grant",
                ));
            }
            if let Err(error) = self
                .bytes
                .try_reserve_exact(required.saturating_sub(self.bytes.len()))
            {
                let _ = self.reservation.resize(old_capacity);
                let error = DbError::new("53200", "query memory limit exceeded")
                    .with_detail(format!("failed to allocate spill buffer: {error}"));
                self.failure = Some(error);
                return Err(std::io::Error::other("failed to allocate spill buffer"));
            }
            let actual_capacity = self.bytes.capacity();
            if actual_capacity > old_capacity.saturating_add(requested)
                && let Err(error) = self
                    .reservation
                    .grow(actual_capacity - old_capacity - requested)
            {
                self.failure = Some(error);
                return Err(std::io::Error::other(
                    "spill buffer exceeds query memory grant",
                ));
            }
        }
        self.bytes.extend_from_slice(source);
        Ok(source.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn write_spill_record<T: Serialize>(
    writer: &mut impl Write,
    value: &T,
    memory: &MemoryGrant,
) -> Result<usize> {
    let mut payload = ReservedSpillBuffer::new(memory)?;
    if let Err(error) = serde_json::to_writer(&mut payload, value) {
        if let Some(memory_error) = payload.failure.take() {
            return Err(memory_error);
        }
        return Err(
            DbError::new("58030", "query spill encoding failed").with_detail(error.to_string())
        );
    }
    let length = u32::try_from(payload.bytes.len())
        .map_err(|_| DbError::new("53200", "query spill record length is out of range"))?;
    writer
        .write_all(&length.to_le_bytes())
        .map_err(spill_io_error)?;
    writer.write_all(&payload.bytes).map_err(spill_io_error)?;
    Ok(std::mem::size_of::<u32>().saturating_add(payload.bytes.len()))
}
