
impl CursorState {
    fn new(events: Box<dyn Iterator<Item = Result<QueryEvent>>>) -> Self {
        Self {
            events,
            current_rows: VecDeque::new(),
            store: CursorPageStore::Memory {
                rows: Vec::new(),
                bytes: 0,
            },
            fields: None,
            position: 0,
            exhausted: false,
        }
    }

    fn seek(
        &mut self,
        direction: EvaluatedCursorDirection,
        limits: ResourceLimits,
    ) -> Result<Option<RuntimeQueryRow>> {
        let target = match direction {
            EvaluatedCursorDirection::Next => self.position.saturating_add(1),
            EvaluatedCursorDirection::Prior => self.position.saturating_sub(1),
            EvaluatedCursorDirection::First => 1,
            EvaluatedCursorDirection::Last => {
                self.load_all(limits)?;
                self.cached_len_i64()?
            }
            EvaluatedCursorDirection::Absolute(position) if position < 0 => {
                self.load_all(limits)?;
                self.cached_len_i64()?
                    .saturating_add(1)
                    .saturating_add(position)
            }
            EvaluatedCursorDirection::Absolute(position) => position,
            EvaluatedCursorDirection::Relative(offset) => self.position.saturating_add(offset),
            EvaluatedCursorDirection::Forward(count) => self.position.saturating_add(count),
            EvaluatedCursorDirection::ForwardAll => {
                self.load_all(limits)?;
                self.position = self.cached_len_i64()?.saturating_add(1);
                return Ok(None);
            }
            EvaluatedCursorDirection::Backward(count) => self.position.saturating_sub(count),
            EvaluatedCursorDirection::BackwardAll => {
                self.position = 0;
                return Ok(None);
            }
        };
        if target <= 0 {
            self.position = 0;
            return Ok(None);
        }
        let target = usize::try_from(target)
            .map_err(|_| DbError::new("54000", "cursor position is not addressable"))?;
        if self.load_through(target, limits)? {
            self.position = i64::try_from(target)
                .map_err(|_| DbError::new("54000", "cursor position is not addressable"))?;
            Ok(self
                .store
                .get(target - 1, limits)?
                .map(|row| RuntimeQueryRow {
                    fields: self.fields.clone().unwrap_or_default(),
                    row,
                }))
        } else {
            self.position = self.cached_len_i64()?.saturating_add(1);
            Ok(None)
        }
    }

    fn load_through(&mut self, target: usize, limits: ResourceLimits) -> Result<bool> {
        while self.store.len() < target && !self.exhausted {
            self.pull_one(limits)?;
        }
        Ok(self.store.len() >= target)
    }

    fn load_all(&mut self, limits: ResourceLimits) -> Result<()> {
        while !self.exhausted {
            self.pull_one(limits)?;
        }
        Ok(())
    }

    fn pull_one(&mut self, limits: ResourceLimits) -> Result<()> {
        loop {
            if let Some(row) = self.current_rows.pop_front() {
                if self.store.len() >= limits.max_cursor_rows {
                    return Err(DbError::new("54000", "PL/pgSQL cursor row limit exceeded"));
                }
                self.store.push(row, limits)?;
                return Ok(());
            }
            let Some(event) = self.events.next() else {
                self.exhausted = true;
                return Ok(());
            };
            match event? {
                QueryEvent::Schema(schema) => {
                    self.fields = Some(schema.fields.into_iter().map(|field| field.name).collect());
                }
                QueryEvent::Batch(batch) => {
                    if self.fields.is_none() {
                        self.fields = Some(
                            batch
                                .schema
                                .fields
                                .iter()
                                .map(|field| field.name.clone())
                                .collect(),
                        );
                    }
                    self.current_rows = batch.rows.into();
                }
                QueryEvent::Progress(_) | QueryEvent::Notice(_) | QueryEvent::Complete(_) => {}
            }
        }
    }

    fn cached_len_i64(&self) -> Result<i64> {
        i64::try_from(self.store.len())
            .map_err(|_| DbError::new("54000", "cursor row count is not addressable"))
    }
}

impl CursorPageStore {
    fn len(&self) -> usize {
        match self {
            Self::Memory { rows, .. } => rows.len(),
            Self::Spilled(store) => store.offsets.len(),
        }
    }

    fn push(&mut self, row: ordadb_types::Row, limits: ResourceLimits) -> Result<()> {
        match self {
            Self::Memory { rows, bytes } => {
                let row_bytes = estimated_cursor_row_bytes(&row);
                if row_bytes > limits.max_cursor_bytes {
                    return cursor_memory_limit();
                }
                let next_bytes = bytes.checked_add(row_bytes).ok_or_else(|| {
                    DbError::new("53200", "PL/pgSQL cursor memory accounting overflowed")
                })?;
                let memory_window = limits.max_cursor_bytes / 2;
                if next_bytes <= memory_window {
                    *bytes = next_bytes;
                    rows.push(row);
                    return Ok(());
                }
                let spilled = CursorSpillStore::from_values(
                    rows.iter().cloned().chain(std::iter::once(row)),
                    limits,
                )?;
                *self = Self::Spilled(spilled);
                Ok(())
            }
            Self::Spilled(store) => store.push(row, limits),
        }
    }

    fn get(&mut self, index: usize, limits: ResourceLimits) -> Result<Option<ordadb_types::Row>> {
        match self {
            Self::Memory { rows, .. } => Ok(rows.get(index).cloned()),
            Self::Spilled(store) => store.get(index, limits),
        }
    }
}

impl CursorSpillStore {
    fn from_values(
        rows: impl IntoIterator<Item = ordadb_types::Row>,
        limits: ResourceLimits,
    ) -> Result<Self> {
        let mut store = Self {
            file: NamedTempFile::new().map_err(|error| {
                cursor_io_error("failed to create PL/pgSQL cursor spill file", error)
            })?,
            offsets: Vec::new(),
        };
        for row in rows {
            store.push(row, limits)?;
        }
        Ok(store)
    }

    fn push(&mut self, row: ordadb_types::Row, limits: ResourceLimits) -> Result<()> {
        let payload = serde_json::to_vec(&row).map_err(|error| {
            DbError::new("XX000", "failed to encode PL/pgSQL cursor spill row")
                .with_detail(error.to_string())
        })?;
        if payload.len() > limits.max_cursor_bytes {
            return cursor_memory_limit();
        }
        let length = u32::try_from(payload.len())
            .map_err(|_| DbError::new("54000", "cursor spill row is too large"))?;
        let retained_offsets = self
            .offsets
            .len()
            .checked_add(1)
            .and_then(|count| count.checked_mul(std::mem::size_of::<u64>()))
            .ok_or_else(|| DbError::new("53200", "PL/pgSQL cursor memory accounting overflowed"))?;
        if retained_offsets.saturating_add(payload.len()) > limits.max_cursor_bytes {
            return cursor_memory_limit();
        }
        self.offsets.try_reserve_exact(1).map_err(|error| {
            DbError::new("53200", "failed to reserve PL/pgSQL cursor spill index")
                .with_detail(error.to_string())
        })?;
        let file = self.file.as_file_mut();
        let offset = file
            .seek(SeekFrom::End(0))
            .map_err(|error| cursor_io_error("failed to seek PL/pgSQL cursor spill file", error))?;
        file.write_all(&length.to_le_bytes()).map_err(|error| {
            cursor_io_error("failed to write PL/pgSQL cursor spill length", error)
        })?;
        file.write_all(&payload)
            .map_err(|error| cursor_io_error("failed to write PL/pgSQL cursor spill row", error))?;
        self.offsets.push(offset);
        Ok(())
    }

    fn get(&mut self, index: usize, limits: ResourceLimits) -> Result<Option<ordadb_types::Row>> {
        let Some(offset) = self.offsets.get(index).copied() else {
            return Ok(None);
        };
        let file = self.file.as_file_mut();
        file.seek(SeekFrom::Start(offset))
            .map_err(|error| cursor_io_error("failed to seek PL/pgSQL cursor spill row", error))?;
        let mut encoded_length = [0_u8; 4];
        file.read_exact(&mut encoded_length).map_err(|error| {
            cursor_io_error("failed to read PL/pgSQL cursor spill length", error)
        })?;
        let length = usize::try_from(u32::from_le_bytes(encoded_length))
            .map_err(|_| DbError::new("54000", "cursor spill value length is not addressable"))?;
        if length > limits.max_cursor_bytes {
            return Err(DbError::new(
                "XX001",
                "PL/pgSQL cursor spill row exceeds its declared bound",
            ));
        }
        let mut payload = vec![0_u8; length];
        file.read_exact(&mut payload)
            .map_err(|error| cursor_io_error("failed to read PL/pgSQL cursor spill row", error))?;
        serde_json::from_slice(&payload).map(Some).map_err(|error| {
            DbError::new("XX001", "PL/pgSQL cursor spill row is corrupt")
                .with_detail(error.to_string())
        })
    }
}

fn cursor_memory_limit<T>() -> Result<T> {
    Err(DbError::new(
        "53200",
        "PL/pgSQL cursor retained-memory limit exceeded",
    ))
}

fn cursor_io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

fn estimated_cursor_row_bytes(row: &ordadb_types::Row) -> usize {
    std::mem::size_of::<ordadb_types::Row>()
        .saturating_add(
            row.values
                .capacity()
                .saturating_mul(std::mem::size_of::<Value>()),
        )
        .saturating_add(
            row.values
                .iter()
                .map(estimated_value_dynamic_bytes)
                .sum::<usize>(),
        )
}

fn estimated_cursor_value_bytes(value: &Value) -> usize {
    std::mem::size_of::<Value>().saturating_add(estimated_value_dynamic_bytes(value))
}

fn estimated_value_dynamic_bytes(value: &Value) -> usize {
    match value {
        Value::Text(value) => value.capacity(),
        Value::Binary(value) => value.capacity(),
        Value::Array(value) => value
            .dimensions()
            .len()
            .saturating_mul(std::mem::size_of::<ordadb_types::ArrayDimension>())
            .saturating_add(
                value
                    .values()
                    .iter()
                    .map(estimated_cursor_value_bytes)
                    .sum(),
            ),
        Value::Json(value) | Value::Jsonb(value) => estimated_json_bytes(value),
        Value::Vector(value) => value.capacity().saturating_mul(std::mem::size_of::<f32>()),
        Value::Null
        | Value::Boolean(_)
        | Value::Int16(_)
        | Value::Int32(_)
        | Value::Int64(_)
        | Value::Float32(_)
        | Value::Float64(_)
        | Value::Decimal(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::Timestamp(_)
        | Value::Interval(_)
        | Value::Uuid(_) => 0,
    }
}

fn estimated_json_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 0,
        serde_json::Value::Bool(_) => std::mem::size_of::<bool>(),
        serde_json::Value::Number(_) => std::mem::size_of::<serde_json::Number>(),
        serde_json::Value::String(value) => value.capacity(),
        serde_json::Value::Array(values) => values
            .capacity()
            .saturating_mul(std::mem::size_of::<serde_json::Value>())
            .saturating_add(values.iter().map(estimated_json_bytes).sum::<usize>()),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                std::mem::size_of::<(String, serde_json::Value)>()
                    .saturating_add(key.capacity())
                    .saturating_add(estimated_json_bytes(value))
                    .saturating_add(4 * std::mem::size_of::<usize>())
            })
            .sum(),
    }
}

fn estimated_value_vec_bytes(values: &Vec<Value>) -> usize {
    std::mem::size_of::<Vec<Value>>()
        .saturating_add(
            values
                .capacity()
                .saturating_mul(std::mem::size_of::<Value>()),
        )
        .saturating_add(
            values
                .iter()
                .map(estimated_value_dynamic_bytes)
                .sum::<usize>(),
        )
}

fn estimated_string_vec_bytes(values: &Vec<String>) -> usize {
    std::mem::size_of::<Vec<String>>()
        .saturating_add(
            values
                .capacity()
                .saturating_mul(std::mem::size_of::<String>()),
        )
        .saturating_add(values.iter().map(String::capacity).sum::<usize>())
}

fn estimated_optional_string_bytes(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, String::capacity)
}

fn estimated_cursor_direction_bytes(direction: &CursorDirection) -> usize {
    match direction {
        CursorDirection::Absolute(value) | CursorDirection::Relative(value) => value.capacity(),
        CursorDirection::Forward(value) | CursorDirection::Backward(value) => {
            estimated_optional_string_bytes(value)
        }
        CursorDirection::Next
        | CursorDirection::Prior
        | CursorDirection::First
        | CursorDirection::Last
        | CursorDirection::ForwardAll
        | CursorDirection::BackwardAll => 0,
    }
}

fn estimated_instruction_dynamic_bytes(instruction: &Instruction) -> usize {
    match instruction {
        Instruction::Assign { expression, .. }
        | Instruction::JumpIfFalse { expression, .. }
        | Instruction::ExecuteSql {
            sql: expression, ..
        }
        | Instruction::QueryForStart {
            sql: expression, ..
        }
        | Instruction::ForeachStart {
            array: expression, ..
        } => expression.capacity(),
        Instruction::AssignField {
            field, expression, ..
        } => field.capacity().saturating_add(expression.capacity()),
        Instruction::DynamicExecute { query, using, .. } => query
            .capacity()
            .saturating_add(estimated_string_vec_bytes(using)),
        Instruction::OpenCursor { query, .. } => match query {
            CursorQuery::Bound => 0,
            CursorQuery::Static(query) => query.capacity(),
            CursorQuery::Dynamic { query, using } => query
                .capacity()
                .saturating_add(estimated_string_vec_bytes(using)),
        },
        Instruction::FetchCursor { direction, .. } | Instruction::MoveCursor { direction, .. } => {
            estimated_cursor_direction_bytes(direction)
        }
        Instruction::Raise {
            message, sql_state, ..
        } => estimated_optional_string_bytes(message)
            .saturating_add(estimated_optional_string_bytes(sql_state)),
        Instruction::Assert { condition, message } => condition
            .capacity()
            .saturating_add(estimated_optional_string_bytes(message)),
        Instruction::IntegerForStart {
            lower, upper, step, ..
        } => lower
            .capacity()
            .saturating_add(upper.capacity())
            .saturating_add(step.capacity()),
        Instruction::Return { expression, .. } => estimated_optional_string_bytes(expression),
        Instruction::Jump { .. }
        | Instruction::CloseCursor { .. }
        | Instruction::QueryForNext { .. }
        | Instruction::IntegerForNext { .. }
        | Instruction::ForeachNext { .. }
        | Instruction::Checkpoint => 0,
    }
}

fn estimated_program_bytes(program: &Program) -> usize {
    let instructions = program
        .instructions
        .capacity()
        .saturating_mul(std::mem::size_of::<Instruction>())
        .saturating_add(
            program
                .instructions
                .iter()
                .map(estimated_instruction_dynamic_bytes)
                .sum(),
        );
    let locals = program
        .locals
        .capacity()
        .saturating_mul(std::mem::size_of::<LocalSlot>())
        .saturating_add(
            program
                .locals
                .iter()
                .map(|local| {
                    local.name.capacity()
                        + match &local.kind {
                            LocalKind::RowType(name) => name.capacity(),
                            LocalKind::Scalar | LocalKind::Record => 0,
                        }
                })
                .sum::<usize>(),
        );
    let cursors = program
        .cursor_declarations
        .capacity()
        .saturating_mul(std::mem::size_of::<CursorDeclaration>())
        .saturating_add(
            program
                .cursor_declarations
                .iter()
                .map(|cursor| {
                    cursor
                        .name
                        .capacity()
                        .saturating_add(estimated_optional_string_bytes(&cursor.bound_query))
                })
                .sum::<usize>(),
        );
    let handlers = program
        .exception_handlers
        .capacity()
        .saturating_mul(std::mem::size_of::<ExceptionHandler>())
        .saturating_add(
            program
                .exception_handlers
                .iter()
                .map(|handler| match &handler.matcher {
                    ExceptionMatcher::SqlState(value) => value.capacity(),
                    ExceptionMatcher::Others => 0,
                })
                .sum::<usize>(),
        );
    std::mem::size_of::<Program>()
        .saturating_add(instructions)
        .saturating_add(locals)
        .saturating_add(cursors)
        .saturating_add(handlers)
}

fn estimated_fields_bytes(fields: &Option<Vec<String>>) -> usize {
    fields.as_ref().map_or(0, estimated_string_vec_bytes)
}

fn estimated_row_deque_bytes(rows: &VecDeque<ordadb_types::Row>) -> usize {
    std::mem::size_of::<VecDeque<ordadb_types::Row>>()
        .saturating_add(
            rows.capacity()
                .saturating_mul(std::mem::size_of::<ordadb_types::Row>()),
        )
        .saturating_add(
            rows.iter()
                .map(|row| {
                    estimated_cursor_row_bytes(row)
                        .saturating_sub(std::mem::size_of::<ordadb_types::Row>())
                })
                .sum::<usize>(),
        )
}

fn estimated_value_deque_bytes(values: &VecDeque<Value>) -> usize {
    std::mem::size_of::<VecDeque<Value>>()
        .saturating_add(
            values
                .capacity()
                .saturating_mul(std::mem::size_of::<Value>()),
        )
        .saturating_add(
            values
                .iter()
                .map(estimated_value_dynamic_bytes)
                .sum::<usize>(),
        )
}

fn estimated_cursor_store_bytes(store: &CursorPageStore) -> usize {
    match store {
        CursorPageStore::Memory { rows, .. } => std::mem::size_of::<CursorPageStore>()
            .saturating_add(
                rows.capacity()
                    .saturating_mul(std::mem::size_of::<ordadb_types::Row>()),
            )
            .saturating_add(
                rows.iter()
                    .map(|row| {
                        estimated_cursor_row_bytes(row)
                            .saturating_sub(std::mem::size_of::<ordadb_types::Row>())
                    })
                    .sum::<usize>(),
            ),
        CursorPageStore::Spilled(store) => std::mem::size_of::<CursorPageStore>().saturating_add(
            store
                .offsets
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>()),
        ),
    }
}

fn estimated_error_bytes(error: &DbError) -> usize {
    std::mem::size_of::<DbError>()
        .saturating_add(error.sql_state.capacity())
        .saturating_add(error.message.capacity())
        .saturating_add(error.detail.as_ref().map_or(0, |value| value.len()))
        .saturating_add(error.hint.as_ref().map_or(0, |value| value.len()))
        .saturating_add(error.query_id.len())
}

#[allow(clippy::too_many_arguments)]
fn estimated_vm_runtime_bytes(
    program: &Program,
    locals: &Vec<Value>,
    records: &BTreeMap<usize, RuntimeRecord>,
    returned_rows: &Vec<Value>,
    query_loops: &BTreeMap<usize, QueryLoopState>,
    integer_loops: &BTreeMap<usize, IntegerLoopState>,
    foreach_loops: &BTreeMap<usize, ForeachLoopState>,
    cursors: &BTreeMap<usize, CursorState>,
    active_exception: Option<&DbError>,
    exception_regions: &Vec<(usize, usize)>,
    active_exception_regions: &Vec<(usize, usize)>,
    pending_request: Option<&VmSqlRequest>,
) -> Result<usize> {
    let record_bytes = records
        .values()
        .map(RuntimeRecord::estimated_bytes)
        .sum::<usize>()
        .saturating_add(
            records
                .len()
                .saturating_mul(std::mem::size_of::<(usize, RuntimeRecord)>()),
        );
    let query_loop_bytes = query_loops
        .values()
        .map(|state| {
            std::mem::size_of::<QueryLoopState>()
                .saturating_add(estimated_row_deque_bytes(&state.current_rows))
                .saturating_add(estimated_fields_bytes(&state.fields))
        })
        .sum::<usize>();
    let foreach_bytes = foreach_loops
        .values()
        .map(|state| {
            std::mem::size_of::<ForeachLoopState>()
                .saturating_add(estimated_value_deque_bytes(&state.values))
        })
        .sum::<usize>();
    let cursor_bytes = cursors
        .values()
        .map(|state| {
            std::mem::size_of::<CursorState>()
                .saturating_add(estimated_row_deque_bytes(&state.current_rows))
                .saturating_add(estimated_cursor_store_bytes(&state.store))
                .saturating_add(estimated_fields_bytes(&state.fields))
        })
        .sum::<usize>();
    let pending_bytes = pending_request.map_or(0, |request| {
        std::mem::size_of::<VmSqlRequest>()
            .saturating_add(request.sql.capacity())
            .saturating_add(estimated_value_vec_bytes(&request.parameters))
    });
    let total = std::mem::size_of::<VmState>()
        .saturating_add(estimated_program_bytes(program))
        .saturating_add(estimated_value_vec_bytes(locals))
        .saturating_add(record_bytes)
        .saturating_add(estimated_value_vec_bytes(returned_rows))
        .saturating_add(query_loop_bytes)
        .saturating_add(
            integer_loops
                .len()
                .saturating_mul(std::mem::size_of::<(usize, IntegerLoopState)>()),
        )
        .saturating_add(foreach_bytes)
        .saturating_add(cursor_bytes)
        .saturating_add(active_exception.map_or(0, estimated_error_bytes))
        .saturating_add(
            exception_regions
                .capacity()
                .saturating_mul(std::mem::size_of::<(usize, usize)>()),
        )
        .saturating_add(
            active_exception_regions
                .capacity()
                .saturating_mul(std::mem::size_of::<(usize, usize)>()),
        )
        .saturating_add(pending_bytes);
    if total == usize::MAX {
        return Err(DbError::new(
            "53200",
            "PL/pgSQL retained-memory accounting overflowed",
        ));
    }
    Ok(total)
}

fn estimated_vm_output_bytes(output: &VmOutput) -> Result<usize> {
    let total = std::mem::size_of::<VmOutput>()
        .saturating_add(
            output
                .return_value
                .as_ref()
                .map_or(0, estimated_value_dynamic_bytes),
        )
        .saturating_add(estimated_value_vec_bytes(&output.returned_rows))
        .saturating_add(estimated_value_vec_bytes(&output.final_locals))
        .saturating_add(estimated_value_vec_bytes(&output.output_parameters));
    if total == usize::MAX {
        return Err(DbError::new(
            "53200",
            "PL/pgSQL output memory accounting overflowed",
        ));
    }
    Ok(total)
}

fn attach_output_memory(mut output: VmOutput, reservation: VmMemoryReservation) -> VmOutput {
    output.retained_memory = Some(VmMemoryHold(Arc::new(reservation)));
    output
}

pub fn compile(source: &str) -> Result<Program> {
    compile_with_limits(source, ResourceLimits::default())
}

pub fn compile_with_limits(source: &str, limits: ResourceLimits) -> Result<Program> {
    compile_with_arguments_and_limits(source, &[], limits)
}

pub fn compile_with_arguments(source: &str, argument_names: &[String]) -> Result<Program> {
    compile_with_arguments_and_limits(source, argument_names, ResourceLimits::default())
}
