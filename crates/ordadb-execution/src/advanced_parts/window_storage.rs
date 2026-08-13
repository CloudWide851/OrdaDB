
impl WindowResultWriter {
    fn new(spill: &mut SpillManager, len: usize, memory: &QueryMemoryContext) -> Result<Self> {
        let requested = len
            .checked_mul(std::mem::size_of::<Option<Value>>())
            .ok_or_else(|| DbError::new("53200", "window result slots are out of range"))?;
        if memory.would_cross_soft_limit(requested) {
            return IndexedResultWriter::new(spill, len, memory).map(Self::Spill);
        }
        let mut reservation = memory.try_reserve(requested)?;
        let mut values = Vec::new();
        if let Err(error) = values.try_reserve_exact(len) {
            return Err(DbError::new("53200", "query memory limit exceeded")
                .with_detail(format!("failed to allocate window result slots: {error}")));
        }
        let actual = values
            .capacity()
            .checked_mul(std::mem::size_of::<Option<Value>>())
            .ok_or_else(|| DbError::new("53200", "window result slots are out of range"))?;
        reservation.resize(actual)?;
        values.resize_with(len, || None);
        Ok(Self::Memory {
            values,
            reservation,
        })
    }

    fn push_at(
        &mut self,
        source_index: usize,
        result: Value,
        spill: &mut SpillManager,
        memory: &QueryMemoryContext,
    ) -> Result<()> {
        let result_bytes = estimated_value_bytes(&result);
        let should_spill = match self {
            Self::Memory { values, .. } => {
                let slot = values.get(source_index).ok_or_else(|| {
                    DbError::internal("window result source index is out of bounds")
                })?;
                if slot.is_some() {
                    return Err(DbError::internal(
                        "window result was written more than once",
                    ));
                }
                memory.would_cross_soft_limit(result_bytes)
            }
            Self::Spill(_) => false,
        };
        if should_spill {
            let len = match self {
                Self::Memory { values, .. } => values.len(),
                Self::Spill(_) => unreachable!("spill transition checked above"),
            };
            let mut writer = IndexedResultWriter::new(spill, len, memory)?;
            if let Self::Memory { values, .. } = self {
                for (index, value) in values.iter_mut().enumerate() {
                    if let Some(value) = value.take() {
                        writer.push_at(index, value, memory)?;
                    }
                }
            }
            *self = Self::Spill(writer);
        }
        match self {
            Self::Memory {
                values,
                reservation,
            } => {
                reservation.grow(result_bytes)?;
                values[source_index] = Some(result);
                Ok(())
            }
            Self::Spill(writer) => writer.push_at(source_index, result, memory),
        }
    }

    fn finish(self, memory: &QueryMemoryContext) -> Result<WindowResults> {
        match self {
            Self::Memory {
                values,
                reservation,
            } => {
                if values.iter().any(Option::is_none) {
                    return Err(DbError::internal("window result index is incomplete"));
                }
                Ok(WindowResults::Memory {
                    values,
                    reservation,
                })
            }
            Self::Spill(writer) => writer.finish(memory).map(WindowResults::Spill),
        }
    }
}

enum WindowResults {
    Memory {
        values: Vec<Option<Value>>,
        reservation: Reservation,
    },
    Spill(IndexedRowStore),
}

impl WindowResults {
    fn take(&mut self, index: usize, memory: &QueryMemoryContext) -> Result<ReservedValue> {
        match self {
            Self::Memory {
                values,
                reservation,
            } => {
                let value = values
                    .get_mut(index)
                    .and_then(Option::take)
                    .ok_or_else(|| DbError::internal("window result is missing"))?;
                let bytes = estimated_value_bytes(&value);
                let mut value_reservation = memory.try_reserve(0)?;
                reservation.transfer_to(&mut value_reservation, bytes)?;
                Ok(ReservedValue {
                    value,
                    reservation: value_reservation,
                })
            }
            Self::Spill(store) => {
                let mut result = store.read(index, memory)?;
                if result.row.values.len() != 1 {
                    return Err(DbError::new(
                        "XX001",
                        "window result spill row has an invalid width",
                    ));
                }
                let value = result
                    .row
                    .values
                    .pop()
                    .ok_or_else(|| DbError::new("XX001", "window result spill row is empty"))?;
                Ok(ReservedValue {
                    value,
                    reservation: result.reservation,
                })
            }
        }
    }
}

struct IndexedRowStore {
    reader: ReservedSpillReader,
    index: File,
    len: usize,
}

impl IndexedRowStore {
    fn open(
        data_path: PathBuf,
        index_path: PathBuf,
        len: usize,
        memory: &QueryMemoryContext,
    ) -> Result<Self> {
        Ok(Self {
            reader: open_spill_reader(&data_path, memory)?,
            index: File::open(index_path).map_err(spill_io_error)?,
            len,
        })
    }

    fn read(&mut self, index: usize, memory: &QueryMemoryContext) -> Result<ReservedRow> {
        if index >= self.len {
            return Err(DbError::internal("window spill row index is out of bounds"));
        }
        let index_position = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_mul(WINDOW_SPILL_INDEX_BYTES))
            .ok_or_else(|| DbError::new("53200", "window spill index is out of range"))?;
        self.index
            .seek(SeekFrom::Start(index_position))
            .map_err(spill_io_error)?;
        let mut offset = [0_u8; std::mem::size_of::<u64>()];
        self.index.read_exact(&mut offset).map_err(|error| {
            DbError::new("XX001", "window spill index is truncated").with_detail(error.to_string())
        })?;
        let offset = u64::from_le_bytes(offset);
        if offset == 0 {
            return Err(DbError::new(
                "XX001",
                "window spill index contains an empty entry",
            ));
        }
        self.reader
            .seek(SeekFrom::Start(offset))
            .map_err(spill_io_error)?;
        let record = read_spill_record::<Row>(&mut self.reader, memory)?
            .ok_or_else(|| DbError::new("XX001", "window spill row is missing"))?;
        let reservation = memory.try_reserve(estimated_row_bytes(&record.value))?;
        Ok(ReservedRow {
            row: record.value,
            reservation,
        })
    }
}

enum WindowRowStore {
    Memory {
        rows: Vec<Row>,
        reservation: Reservation,
    },
    Spill(IndexedRowStore),
}

impl WindowRowStore {
    fn len(&self) -> usize {
        match self {
            Self::Memory { rows, .. } => rows.len(),
            Self::Spill(store) => store.len,
        }
    }

    fn read(&mut self, index: usize, memory: &QueryMemoryContext) -> Result<ReservedRow> {
        match self {
            Self::Memory { rows, .. } => {
                let row = rows
                    .get(index)
                    .ok_or_else(|| DbError::internal("window memory row index is out of bounds"))?;
                let reservation = memory.try_reserve(estimated_row_bytes(row))?;
                Ok(ReservedRow {
                    row: row.clone(),
                    reservation,
                })
            }
            Self::Spill(store) => store.read(index, memory),
        }
    }
}

struct WindowRowStoreBuilder {
    rows: Vec<Row>,
    reservation: Reservation,
    writer: Option<IndexedRowStoreWriter>,
}

impl WindowRowStoreBuilder {
    fn new(memory: &QueryMemoryContext) -> Result<Self> {
        Ok(Self {
            rows: Vec::new(),
            reservation: memory.try_reserve(0)?,
            writer: None,
        })
    }

    fn push(
        &mut self,
        row: Row,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<()> {
        let mut source_reservation = memory.try_reserve(estimated_row_bytes(&row))?;
        self.push_transferred(row, &mut source_reservation, memory, spill)?;
        if source_reservation.bytes() != 0 {
            return Err(DbError::internal(
                "window input reservation was not fully transferred",
            ));
        }
        Ok(())
    }

    fn push_transferred(
        &mut self,
        row: Row,
        source_reservation: &mut Reservation,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<()> {
        let bytes = estimated_row_bytes(&row);
        if source_reservation.bytes() < bytes {
            return Err(DbError::internal(
                "window source reservation is smaller than its row",
            ));
        }
        if self.writer.is_none() && memory.current_bytes() > memory.soft_limit_bytes() {
            let mut writer = IndexedRowStoreWriter::new(spill, memory)?;
            for existing in &self.rows {
                writer.push(existing, memory)?;
            }
            self.rows.clear();
            self.reservation.resize(0)?;
            self.writer = Some(writer);
        }
        if let Some(writer) = &mut self.writer {
            writer.push(&row, memory)?;
            source_reservation.resize(source_reservation.bytes().saturating_sub(bytes))?;
        } else {
            source_reservation.transfer_to(&mut self.reservation, bytes)?;
            self.rows.push(row);
        }
        Ok(())
    }

    fn finish(self, memory: &QueryMemoryContext) -> Result<WindowRowStore> {
        if let Some(writer) = self.writer {
            writer.finish(memory).map(WindowRowStore::Spill)
        } else {
            Ok(WindowRowStore::Memory {
                rows: self.rows,
                reservation: self.reservation,
            })
        }
    }
}

struct WindowProgram {
    function: WindowFunction,
    arguments: Vec<ExpressionProgram>,
    filter: Option<ExpressionProgram>,
    partition_by: Vec<ExpressionProgram>,
    order_by: Vec<BoundOrder>,
    order_programs: Vec<Option<ExpressionProgram>>,
    frame: Option<WindowFrameProgram>,
}

struct WindowRowStores<'a> {
    keyed: &'a mut WindowRowStore,
    source: &'a mut WindowRowStore,
}

struct WindowFrameProgram {
    units: WindowFrameUnits,
    start_bound: WindowFrameBoundProgram,
    end_bound: WindowFrameBoundProgram,
}

enum WindowFrameBoundProgram {
    UnboundedPreceding,
    Preceding(ExpressionProgram),
    CurrentRow,
    Following(ExpressionProgram),
    UnboundedFollowing,
}

#[derive(Clone, Copy)]
enum AggregateWindowMode {
    WholePartition,
    RowsRunning,
    RangeRunning,
}
