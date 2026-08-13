
impl WindowProgram {
    fn compile(window: &BoundWindow, max_depth: usize) -> Result<Self> {
        let partition_by = window
            .partition_by
            .iter()
            .map(|expression| ExpressionProgram::compile_with_limit(expression, false, max_depth))
            .collect::<Result<Vec<_>>>()?;
        let arguments = window
            .arguments
            .iter()
            .map(|argument| ExpressionProgram::compile_with_limit(argument, false, max_depth))
            .collect::<Result<Vec<_>>>()?;
        let filter = window
            .filter
            .as_ref()
            .map(|filter| ExpressionProgram::compile_with_limit(filter, false, max_depth))
            .transpose()?;
        let order_programs = window
            .order_by
            .iter()
            .map(|order| {
                order
                    .expression
                    .as_ref()
                    .map(|expression| {
                        ExpressionProgram::compile_with_limit(expression, false, max_depth)
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        let frame = window
            .frame
            .as_ref()
            .map(|frame| WindowFrameProgram::compile(frame, max_depth))
            .transpose()?;
        Ok(Self {
            function: window.function,
            arguments,
            filter,
            partition_by,
            order_by: window.order_by.clone(),
            order_programs,
            frame,
        })
    }

    fn apply(
        &self,
        mut rows: WindowRowStore,
        params: &[Value],
        stack: &mut ExpressionStack,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
        cancellation: Option<&AtomicBool>,
    ) -> Result<WindowRowStore> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(rows);
        }
        let partition_count = self.partition_by.len();
        let order_count = self.order_by.len();
        let mut sort_orders = self
            .partition_by
            .iter()
            .enumerate()
            .map(|(column_index, program)| BoundOrder {
                column_index,
                expression: None,
                data_type: program.result_type().clone(),
                ascending: true,
                nulls_first: Some(true),
            })
            .collect::<Vec<_>>();
        sort_orders.extend(
            self.order_by
                .iter()
                .enumerate()
                .map(|(ordinal, order)| BoundOrder {
                    column_index: partition_count.saturating_add(ordinal),
                    expression: None,
                    data_type: order.data_type.clone(),
                    ascending: order.ascending,
                    nulls_first: order.nulls_first,
                }),
        );
        sort_orders.push(BoundOrder {
            column_index: partition_count.saturating_add(order_count),
            expression: None,
            data_type: ScalarType::Int64,
            ascending: true,
            nulls_first: Some(false),
        });

        let mut keyed_builder = RowsOutputBuilder::new(&sort_orders, memory, 1)?;
        for index in 0..row_count {
            ensure_window_not_cancelled(cancellation)?;
            let row = rows.read(index, memory)?;
            let mut values = self
                .partition_by
                .iter()
                .map(|program| program.evaluate_reusing(&row.row.values, params, stack))
                .collect::<Result<Vec<_>>>()?;
            for (order, program) in self.order_by.iter().zip(&self.order_programs) {
                let value = if let Some(program) = program {
                    program.evaluate_reusing(&row.row.values, params, stack)?
                } else {
                    row.row
                        .values
                        .get(order.column_index)
                        .cloned()
                        .ok_or_else(|| {
                            DbError::internal("window ORDER BY column index is out of bounds")
                        })?
                };
                values.push(value);
            }
            let index = i64::try_from(index)
                .map_err(|_| DbError::new("54001", "window row index is out of range"))?;
            values.push(Value::Int64(index));
            keyed_builder.push(Row::new(values), params, stack, memory, spill)?;
        }
        let mut keyed = keyed_builder
            .finish(memory, spill)?
            .into_window_store(memory, spill)?;

        let frame = self
            .frame
            .as_ref()
            .map(|frame| frame.evaluate(params, stack))
            .transpose()?
            .unwrap_or_else(|| EvaluatedWindowFrame::default_for_order(order_count));
        let mut state_reservation = memory.try_reserve(0)?;
        let mut results = WindowResultWriter::new(spill, row_count, memory)?;
        let mut partition_start = 0_usize;
        while partition_start < keyed.len() {
            ensure_window_not_cancelled(cancellation)?;
            let mut partition_end = partition_start.saturating_add(1);
            while partition_end < keyed.len() {
                let left = keyed.read(partition_start, memory)?;
                let right = keyed.read(partition_end, memory)?;
                if !window_key_slices_equal(
                    &left.row.values[..partition_count],
                    &right.row.values[..partition_count],
                )? {
                    break;
                }
                partition_end = partition_end.saturating_add(1);
            }

            if let WindowFunction::Aggregate(function) = self.function
                && self.apply_optimized_aggregate(
                    function,
                    &frame,
                    partition_start..partition_end,
                    partition_count,
                    order_count,
                    &mut keyed,
                    &mut rows,
                    params,
                    stack,
                    spill,
                    memory,
                    &mut state_reservation,
                    &mut results,
                    cancellation,
                )?
            {
                partition_start = partition_end;
                continue;
            }

            let mut peer_rank = 1_usize;
            let mut dense_rank = 1_usize;
            for index in partition_start..partition_end {
                ensure_window_not_cancelled(cancellation)?;
                let partition_position = index.saturating_sub(partition_start).saturating_add(1);
                if index > partition_start {
                    let previous = keyed.read(index - 1, memory)?;
                    let current = keyed.read(index, memory)?;
                    if !window_key_slices_equal(
                        &previous.row.values
                            [partition_count..partition_count.saturating_add(order_count)],
                        &current.row.values
                            [partition_count..partition_count.saturating_add(order_count)],
                    )? {
                        peer_rank = partition_position;
                        dense_rank = dense_rank.saturating_add(1);
                    }
                }
                state_reservation.resize(0)?;
                let result = self.evaluate_result(
                    index,
                    partition_start,
                    partition_end,
                    partition_position,
                    peer_rank,
                    dense_rank,
                    partition_count,
                    &mut keyed,
                    &mut rows,
                    params,
                    stack,
                    memory,
                    &frame,
                    &mut state_reservation,
                    cancellation,
                )?;
                append_window_result(index, result, &mut keyed, &mut results, spill, memory)?;
            }
            partition_start = partition_end;
        }

        let mut results = results.finish(memory)?;
        let mut next = WindowRowStoreBuilder::new(memory)?;
        for source_index in 0..row_count {
            ensure_window_not_cancelled(cancellation)?;
            let ReservedRow {
                mut row,
                mut reservation,
            } = rows.read(source_index, memory)?;
            let ReservedValue {
                value,
                reservation: _result_reservation,
            } = results.take(source_index, memory)?;
            reservation.grow(estimated_value_bytes(&value))?;
            row.values.push(value);
            next.push_transferred(row, &mut reservation, memory, spill)?;
        }
        next.finish(memory)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_optimized_aggregate(
        &self,
        function: AggregateFunction,
        frame: &EvaluatedWindowFrame,
        partition: std::ops::Range<usize>,
        partition_count: usize,
        order_count: usize,
        keyed: &mut WindowRowStore,
        rows: &mut WindowRowStore,
        params: &[Value],
        stack: &mut ExpressionStack,
        spill: &mut SpillManager,
        memory: &QueryMemoryContext,
        state_reservation: &mut Reservation,
        results: &mut WindowResultWriter,
        cancellation: Option<&AtomicBool>,
    ) -> Result<bool> {
        let Some(mode) = aggregate_window_mode(frame) else {
            return Ok(false);
        };
        state_reservation.resize(0)?;
        let spec = AggregateSpec {
            function,
            argument: self.arguments.first().cloned(),
            distinct: false,
            filter: self.filter.clone(),
            source: None,
            source_filter: None,
        };
        let mut state = AggregateState::new(&spec);
        match mode {
            AggregateWindowMode::WholePartition => {
                for target in partition.clone() {
                    update_window_aggregate(
                        &mut state,
                        &spec,
                        target,
                        keyed,
                        rows,
                        params,
                        stack,
                        memory,
                        state_reservation,
                        cancellation,
                    )?;
                }
                let result = state.value(&spec)?;
                for index in partition {
                    ensure_window_not_cancelled(cancellation)?;
                    append_window_result(index, result.clone(), keyed, results, spill, memory)?;
                }
            }
            AggregateWindowMode::RowsRunning => {
                for index in partition {
                    update_window_aggregate(
                        &mut state,
                        &spec,
                        index,
                        keyed,
                        rows,
                        params,
                        stack,
                        memory,
                        state_reservation,
                        cancellation,
                    )?;
                    append_window_result(
                        index,
                        state.value(&spec)?,
                        keyed,
                        results,
                        spill,
                        memory,
                    )?;
                }
            }
            AggregateWindowMode::RangeRunning => {
                let order_start = partition_count;
                let order_end = partition_count.saturating_add(order_count);
                let mut peer_start = partition.start;
                while peer_start < partition.end {
                    ensure_window_not_cancelled(cancellation)?;
                    let mut peer_end = peer_start.saturating_add(1);
                    while peer_end < partition.end {
                        let left = keyed.read(peer_start, memory)?;
                        let right = keyed.read(peer_end, memory)?;
                        if !window_key_slices_equal(
                            &left.row.values[order_start..order_end],
                            &right.row.values[order_start..order_end],
                        )? {
                            break;
                        }
                        peer_end = peer_end.saturating_add(1);
                    }
                    for target in peer_start..peer_end {
                        update_window_aggregate(
                            &mut state,
                            &spec,
                            target,
                            keyed,
                            rows,
                            params,
                            stack,
                            memory,
                            state_reservation,
                            cancellation,
                        )?;
                    }
                    let result = state.value(&spec)?;
                    for index in peer_start..peer_end {
                        append_window_result(index, result.clone(), keyed, results, spill, memory)?;
                    }
                    peer_start = peer_end;
                }
            }
        }
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_result(
        &self,
        index: usize,
        partition_start: usize,
        partition_end: usize,
        partition_position: usize,
        peer_rank: usize,
        dense_rank: usize,
        partition_count: usize,
        keyed: &mut WindowRowStore,
        rows: &mut WindowRowStore,
        params: &[Value],
        stack: &mut ExpressionStack,
        memory: &QueryMemoryContext,
        frame: &EvaluatedWindowFrame,
        state_reservation: &mut Reservation,
        cancellation: Option<&AtomicBool>,
    ) -> Result<Value> {
        match self.function {
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                let value = match self.function {
                    WindowFunction::RowNumber => partition_position,
                    WindowFunction::Rank => peer_rank,
                    WindowFunction::DenseRank => dense_rank,
                    _ => unreachable!("ranking match guarded above"),
                };
                i64::try_from(value)
                    .map(Value::Int64)
                    .map_err(|_| DbError::new("22003", "window rank is out of range"))
            }
            WindowFunction::Lag | WindowFunction::Lead => self.evaluate_offset_value(
                index,
                partition_start..partition_end,
                WindowRowStores {
                    keyed,
                    source: rows,
                },
                params,
                stack,
                memory,
            ),
            WindowFunction::FirstValue | WindowFunction::LastValue | WindowFunction::NthValue => {
                let Some((frame_start, frame_end)) = window_frame_range(
                    frame,
                    index,
                    partition_start..partition_end,
                    partition_count,
                    &self.order_by,
                    keyed,
                    memory,
                )?
                else {
                    return Ok(Value::Null);
                };
                let target = match self.function {
                    WindowFunction::FirstValue => Some(frame_start),
                    WindowFunction::LastValue => Some(frame_end),
                    WindowFunction::NthValue => {
                        let current = window_source_row(index, keyed, rows, memory)?;
                        let nth = self.arguments.get(1).ok_or_else(|| {
                            DbError::internal("NTH_VALUE ordinal program is unavailable")
                        })?;
                        let nth = positive_window_offset(
                            nth.evaluate_reusing(&current.row.values, params, stack)?,
                            "NTH_VALUE argument",
                            false,
                        )?;
                        frame_start
                            .checked_add(nth.saturating_sub(1))
                            .filter(|target| *target <= frame_end)
                    }
                    _ => unreachable!("value match guarded above"),
                };
                let Some(target) = target else {
                    return Ok(Value::Null);
                };
                self.arguments
                    .first()
                    .ok_or_else(|| DbError::internal("window value argument is unavailable"))?
                    .evaluate_reusing(
                        &window_source_row(target, keyed, rows, memory)?.row.values,
                        params,
                        stack,
                    )
            }
            WindowFunction::Aggregate(function) => {
                let spec = AggregateSpec {
                    function,
                    argument: self.arguments.first().cloned(),
                    distinct: false,
                    filter: self.filter.clone(),
                    source: None,
                    source_filter: None,
                };
                let mut state = AggregateState::new(&spec);
                if let Some((frame_start, frame_end)) = window_frame_range(
                    frame,
                    index,
                    partition_start..partition_end,
                    partition_count,
                    &self.order_by,
                    keyed,
                    memory,
                )? {
                    for target in frame_start..=frame_end {
                        ensure_window_not_cancelled(cancellation)?;
                        let source = window_source_row(target, keyed, rows, memory)?;
                        state.update(&spec, &source.row, params, stack)?;
                        state_reservation.resize(state.estimated_bytes())?;
                    }
                }
                state_reservation.resize(state.estimated_bytes())?;
                state.value(&spec)
            }
        }
    }

    fn evaluate_offset_value(
        &self,
        index: usize,
        partition: std::ops::Range<usize>,
        stores: WindowRowStores<'_>,
        params: &[Value],
        stack: &mut ExpressionStack,
        memory: &QueryMemoryContext,
    ) -> Result<Value> {
        let current = window_source_row(index, stores.keyed, stores.source, memory)?;
        let offset = self
            .arguments
            .get(1)
            .map(|offset| offset.evaluate_reusing(&current.row.values, params, stack))
            .transpose()?
            .map_or(Ok(Some(1_i128)), |offset| {
                signed_window_offset(offset, "window offset")
            })?;
        let Some(offset) = offset else {
            return Ok(Value::Null);
        };
        let current_index = i128::try_from(index)
            .map_err(|_| DbError::new("54001", "window row index is out of range"))?;
        let target = match self.function {
            WindowFunction::Lag => current_index.checked_sub(offset),
            WindowFunction::Lead => current_index.checked_add(offset),
            _ => unreachable!("offset match guarded by caller"),
        }
        .and_then(|target| usize::try_from(target).ok())
        .filter(|target| partition.contains(target));
        if let Some(target) = target {
            return self
                .arguments
                .first()
                .ok_or_else(|| DbError::internal("offset window value is unavailable"))?
                .evaluate_reusing(
                    &window_source_row(target, stores.keyed, stores.source, memory)?
                        .row
                        .values,
                    params,
                    stack,
                );
        }
        self.arguments
            .get(2)
            .map(|default| default.evaluate_reusing(&current.row.values, params, stack))
            .transpose()
            .map(|default| default.unwrap_or(Value::Null))
    }
}

impl WindowFrameProgram {
    fn compile(frame: &BoundWindowFrame, max_depth: usize) -> Result<Self> {
        Ok(Self {
            units: frame.units,
            start_bound: WindowFrameBoundProgram::compile(&frame.start_bound, max_depth)?,
            end_bound: WindowFrameBoundProgram::compile(&frame.end_bound, max_depth)?,
        })
    }

    fn evaluate(
        &self,
        params: &[Value],
        stack: &mut ExpressionStack,
    ) -> Result<EvaluatedWindowFrame> {
        let start = self.start_bound.evaluate(params, stack, self.units)?;
        let end = self.end_bound.evaluate(params, stack, self.units)?;
        match (&start, &end) {
            (
                EvaluatedWindowFrameBound::Preceding(start),
                EvaluatedWindowFrameBound::Preceding(end),
            ) if compare_values(start, end)? == Ordering::Less => Err(DbError::new(
                "42P20",
                "frame starting offset must not follow the ending offset",
            )),
            (
                EvaluatedWindowFrameBound::Following(start),
                EvaluatedWindowFrameBound::Following(end),
            ) if compare_values(start, end)? == Ordering::Greater => Err(DbError::new(
                "42P20",
                "frame starting offset must not follow the ending offset",
            )),
            _ => Ok(EvaluatedWindowFrame {
                units: self.units,
                start_bound: start,
                end_bound: end,
            }),
        }
    }
}

struct EvaluatedWindowFrame {
    units: WindowFrameUnits,
    start_bound: EvaluatedWindowFrameBound,
    end_bound: EvaluatedWindowFrameBound,
}

enum EvaluatedWindowFrameBound {
    UnboundedPreceding,
    Preceding(Value),
    CurrentRow,
    Following(Value),
    UnboundedFollowing,
}

impl EvaluatedWindowFrame {
    fn default_for_order(order_count: usize) -> Self {
        Self {
            units: WindowFrameUnits::Range,
            start_bound: EvaluatedWindowFrameBound::UnboundedPreceding,
            end_bound: if order_count == 0 {
                EvaluatedWindowFrameBound::UnboundedFollowing
            } else {
                EvaluatedWindowFrameBound::CurrentRow
            },
        }
    }
}

impl WindowFrameBoundProgram {
    fn compile(bound: &BoundWindowFrameBound, max_depth: usize) -> Result<Self> {
        Ok(match bound {
            BoundWindowFrameBound::UnboundedPreceding => Self::UnboundedPreceding,
            BoundWindowFrameBound::Preceding(offset) => Self::Preceding(
                ExpressionProgram::compile_with_limit(offset, false, max_depth)?,
            ),
            BoundWindowFrameBound::CurrentRow => Self::CurrentRow,
            BoundWindowFrameBound::Following(offset) => Self::Following(
                ExpressionProgram::compile_with_limit(offset, false, max_depth)?,
            ),
            BoundWindowFrameBound::UnboundedFollowing => Self::UnboundedFollowing,
        })
    }

    fn evaluate(
        &self,
        params: &[Value],
        stack: &mut ExpressionStack,
        units: WindowFrameUnits,
    ) -> Result<EvaluatedWindowFrameBound> {
        let program = match self {
            Self::Preceding(program) | Self::Following(program) => program,
            Self::UnboundedPreceding => {
                return Ok(EvaluatedWindowFrameBound::UnboundedPreceding);
            }
            Self::CurrentRow => return Ok(EvaluatedWindowFrameBound::CurrentRow),
            Self::UnboundedFollowing => {
                return Ok(EvaluatedWindowFrameBound::UnboundedFollowing);
            }
        };
        let value = program.evaluate_reusing(&[], params, stack)?;
        let valid = match &value {
            Value::Int16(value) => *value >= 0,
            Value::Int32(value) => *value >= 0,
            Value::Int64(value) => *value >= 0,
            Value::Float32(value) => value.is_finite() && *value >= 0.0,
            Value::Float64(value) => value.is_finite() && *value >= 0.0,
            Value::Decimal(value) => !value.is_sign_negative(),
            Value::Null => false,
            _ => false,
        };
        if !valid {
            let unit = match units {
                WindowFrameUnits::Rows => "ROWS",
                WindowFrameUnits::Range => "RANGE",
            };
            return Err(DbError::new(
                "22013",
                format!("{unit} frame offset must not be negative or null"),
            ));
        }
        Ok(match self {
            Self::Preceding(_) => EvaluatedWindowFrameBound::Preceding(value),
            Self::Following(_) => EvaluatedWindowFrameBound::Following(value),
            Self::UnboundedPreceding | Self::CurrentRow | Self::UnboundedFollowing => {
                unreachable!("non-offset frame bound returned above")
            }
        })
    }
}

fn window_source_index(key: &Row) -> Result<usize> {
    match key.values.last() {
        Some(Value::Int64(index)) => {
            usize::try_from(*index).map_err(|_| DbError::internal("window row index is invalid"))
        }
        _ => Err(DbError::internal("window row index is missing")),
    }
}

fn ensure_window_not_cancelled(cancellation: Option<&AtomicBool>) -> Result<()> {
    if cancellation.is_some_and(|cancellation| cancellation.load(AtomicOrdering::Acquire)) {
        Err(DbError::new("57014", "query was cancelled"))
    } else {
        Ok(())
    }
}

fn aggregate_window_mode(frame: &EvaluatedWindowFrame) -> Option<AggregateWindowMode> {
    match (&frame.start_bound, &frame.end_bound) {
        (
            EvaluatedWindowFrameBound::UnboundedPreceding,
            EvaluatedWindowFrameBound::UnboundedFollowing,
        ) => Some(AggregateWindowMode::WholePartition),
        (EvaluatedWindowFrameBound::UnboundedPreceding, EvaluatedWindowFrameBound::CurrentRow)
            if frame.units == WindowFrameUnits::Rows =>
        {
            Some(AggregateWindowMode::RowsRunning)
        }
        (EvaluatedWindowFrameBound::UnboundedPreceding, EvaluatedWindowFrameBound::CurrentRow)
            if frame.units == WindowFrameUnits::Range =>
        {
            Some(AggregateWindowMode::RangeRunning)
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn update_window_aggregate(
    state: &mut AggregateState,
    spec: &AggregateSpec,
    index: usize,
    keyed: &mut WindowRowStore,
    rows: &mut WindowRowStore,
    params: &[Value],
    stack: &mut ExpressionStack,
    memory: &QueryMemoryContext,
    state_reservation: &mut Reservation,
    cancellation: Option<&AtomicBool>,
) -> Result<()> {
    ensure_window_not_cancelled(cancellation)?;
    let source = window_source_row(index, keyed, rows, memory)?;
    state.update(spec, &source.row, params, stack)?;
    state_reservation.resize(state.estimated_bytes())
}

fn append_window_result(
    index: usize,
    result: Value,
    keyed: &mut WindowRowStore,
    results: &mut WindowResultWriter,
    spill: &mut SpillManager,
    memory: &QueryMemoryContext,
) -> Result<()> {
    let key = keyed.read(index, memory)?;
    let source_index = window_source_index(&key.row)?;
    results.push_at(source_index, result, spill, memory)
}

fn window_source_row(
    index: usize,
    keyed: &mut WindowRowStore,
    rows: &mut WindowRowStore,
    memory: &QueryMemoryContext,
) -> Result<ReservedRow> {
    let key = keyed.read(index, memory)?;
    let source_index = window_source_index(&key.row)?;
    rows.read(source_index, memory)
}

fn positive_window_offset(value: Value, label: &str, zero_allowed: bool) -> Result<usize> {
    let value = match value {
        Value::Int16(value) => i64::from(value),
        Value::Int32(value) => i64::from(value),
        Value::Int64(value) => value,
        Value::Null => {
            return Err(DbError::new("22013", format!("{label} must not be null")));
        }
        _ => return Err(DbError::new("42804", format!("{label} must be an integer"))),
    };
    if value < 0 || (!zero_allowed && value == 0) {
        return Err(DbError::new(
            "22013",
            format!(
                "{label} must be {}",
                if zero_allowed {
                    "nonnegative"
                } else {
                    "positive"
                }
            ),
        ));
    }
    usize::try_from(value).map_err(|_| DbError::new("22003", format!("{label} is out of range")))
}

fn signed_window_offset(value: Value, label: &str) -> Result<Option<i128>> {
    match value {
        Value::Int16(value) => Ok(Some(i128::from(value))),
        Value::Int32(value) => Ok(Some(i128::from(value))),
        Value::Int64(value) => Ok(Some(i128::from(value))),
        Value::Null => Ok(None),
        _ => Err(DbError::new("42804", format!("{label} must be an integer"))),
    }
}
