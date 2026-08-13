
impl SelectionVector {
    pub fn all(row_count: usize) -> Result<Self> {
        let indexes = (0..row_count)
            .map(|index| {
                u32::try_from(index).map_err(|_| {
                    DbError::new("54000", "data chunk exceeds selection-vector capacity")
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { indexes })
    }

    fn reset_all(&mut self, row_count: usize) -> Result<()> {
        let row_count = u32::try_from(row_count)
            .map_err(|_| DbError::new("54000", "data chunk exceeds selection-vector capacity"))?;
        self.indexes.clear();
        self.indexes.extend(0..row_count);
        Ok(())
    }

    pub fn from_indexes(indexes: Vec<u32>, physical_rows: usize) -> Result<Self> {
        if indexes
            .iter()
            .any(|index| usize::try_from(*index).map_or(true, |index| index >= physical_rows))
        {
            return Err(DbError::internal(
                "selection vector index is out of physical chunk bounds",
            ));
        }
        Ok(Self { indexes })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub fn retain(&mut self, mut predicate: impl FnMut(usize) -> Result<bool>) -> Result<()> {
        let mut retained = Vec::with_capacity(self.indexes.len());
        for index in self.indexes.iter().copied() {
            if predicate(index as usize)? {
                retained.push(index);
            }
        }
        self.indexes = retained;
        Ok(())
    }

    pub fn truncate(&mut self, len: usize) {
        self.indexes.truncate(len);
    }

    pub fn discard_prefix(&mut self, len: usize) {
        self.indexes.drain(..len.min(self.indexes.len()));
    }

    fn physical_index(&self, logical_index: usize) -> Result<usize> {
        self.indexes
            .get(logical_index)
            .copied()
            .map(|index| index as usize)
            .ok_or_else(|| DbError::internal("logical chunk row is out of bounds"))
    }
}

/// A columnar execution batch. Public row APIs materialize only at boundaries.
#[derive(Debug, Clone, PartialEq)]
pub struct DataChunk {
    columns: Vec<ColumnVector>,
    selection: SelectionVector,
    physical_rows: usize,
}

impl DataChunk {
    pub fn from_rows(rows: &[Row]) -> Result<Self> {
        let kinds = infer_kinds(rows)?;
        let mut columns = kinds
            .iter()
            .map(|kind| ColumnVector::with_kind(*kind, rows.len()))
            .collect::<Vec<_>>();
        append_rows(&mut columns, rows)?;
        Ok(Self {
            columns,
            selection: SelectionVector::all(rows.len())?,
            physical_rows: rows.len(),
        })
    }

    pub(crate) fn from_row_snapshot(rows: Arc<Vec<Row>>, start: usize, end: usize) -> Result<Self> {
        if start > end || end > rows.len() {
            return Err(DbError::internal("row-backed data chunk range is invalid"));
        }
        let kinds = infer_kinds(&rows[start..end])?;
        let columns = kinds
            .into_iter()
            .enumerate()
            .map(|(column, kind)| ColumnVector::RowBacked {
                kind,
                view: RowColumnView {
                    rows: Arc::clone(&rows),
                    start,
                    end,
                    column,
                },
            })
            .collect::<Vec<_>>();
        let physical_rows = end - start;
        Ok(Self {
            columns,
            selection: SelectionVector::all(physical_rows)?,
            physical_rows,
        })
    }

    pub fn from_columns(columns: Vec<ColumnVector>) -> Result<Self> {
        let physical_rows = columns.first().map_or(0, ColumnVector::len);
        if columns.iter().any(|column| column.len() != physical_rows) {
            return Err(DbError::internal(
                "data chunk columns have different physical lengths",
            ));
        }
        Ok(Self {
            columns,
            selection: SelectionVector::all(physical_rows)?,
            physical_rows,
        })
    }

    #[must_use]
    pub fn columns(&self) -> &[ColumnVector] {
        &self.columns
    }

    #[must_use]
    pub fn selection(&self) -> &SelectionVector {
        &self.selection
    }

    pub fn selection_mut(&mut self) -> &mut SelectionVector {
        &mut self.selection
    }

    #[must_use]
    pub fn physical_rows(&self) -> usize {
        self.physical_rows
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.selection.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.selection.is_empty()
    }

    pub fn row(&self, logical_index: usize) -> Result<Row> {
        let physical_index = self.selection.physical_index(logical_index)?;
        self.physical_row(physical_index)
    }

    pub fn physical_row(&self, physical_index: usize) -> Result<Row> {
        if physical_index >= self.physical_rows {
            return Err(DbError::internal("physical chunk row is out of bounds"));
        }
        self.columns
            .iter()
            .map(|column| column.value(physical_index))
            .collect::<Result<Vec<_>>>()
            .map(Row::new)
    }

    pub fn into_rows(mut self) -> Result<Vec<Row>> {
        self.take_rows()
    }

    pub(crate) fn take_rows(&mut self) -> Result<Vec<Row>> {
        let indexes = std::mem::take(&mut self.selection.indexes);
        let mut rows = Vec::with_capacity(indexes.len());
        if self.columns.is_empty() {
            rows.resize_with(indexes.len(), || Row::new(Vec::new()));
            self.physical_rows = 0;
            return Ok(rows);
        }
        if let Some((snapshot, start, end)) = identity_row_snapshot(&self.columns) {
            for physical_index in indexes {
                let physical_index = physical_index as usize;
                if physical_index >= end.saturating_sub(start) {
                    return Err(DbError::internal(
                        "row-backed selection index is out of bounds",
                    ));
                }
                rows.push(
                    snapshot
                        .get(start + physical_index)
                        .cloned()
                        .ok_or_else(|| {
                            DbError::internal("row-backed snapshot row is unavailable")
                        })?,
                );
            }
            self.physical_rows = 0;
            return Ok(rows);
        }
        if self.columns.len() == 1 {
            take_single_column_rows(&mut self.columns[0], indexes, &mut rows)?;
            self.physical_rows = 0;
            return Ok(rows);
        }
        for physical_index in indexes {
            let values = self
                .columns
                .iter_mut()
                .map(|column| column.take_value(physical_index as usize))
                .collect::<Result<Vec<_>>>()?;
            rows.push(Row::new(values));
        }
        self.physical_rows = 0;
        Ok(rows)
    }

    pub(crate) fn value(&self, column: usize, physical_row: usize) -> Result<Value> {
        self.columns
            .get(column)
            .ok_or_else(|| DbError::internal("column index is outside the data chunk"))?
            .value(physical_row)
    }

    pub(crate) fn compare_literal(
        &self,
        column: usize,
        physical_row: usize,
        literal: &Value,
        operator: BinaryOperator,
    ) -> Option<Result<Value>> {
        self.columns
            .get(column)?
            .compare_literal(physical_row, literal, operator)
    }

    pub(crate) fn retain_selected(
        &mut self,
        mut predicate: impl FnMut(&Self, usize) -> Result<bool>,
    ) -> Result<()> {
        let indexes = std::mem::take(&mut self.selection.indexes);
        let mut retained = Vec::with_capacity(indexes.len());
        for index in indexes {
            if predicate(self, index as usize)? {
                retained.push(index);
            }
        }
        self.selection.indexes = retained;
        Ok(())
    }

    pub(crate) fn retain_literal_comparison(
        &mut self,
        column: usize,
        literal: &Value,
        operator: BinaryOperator,
    ) -> Option<Result<()>> {
        let column = self.columns.get(column)?;
        column.retain_literal_comparison(&mut self.selection.indexes, literal, operator)
    }

    pub(crate) fn project_columns_in_place(
        &mut self,
        projections: &[(usize, ScalarType)],
    ) -> Result<bool> {
        if !self.can_project_columns(projections)? {
            return Ok(false);
        }
        if projections.len() == self.columns.len()
            && projections
                .iter()
                .enumerate()
                .all(|(position, (index, _))| position == *index)
        {
            return Ok(true);
        }
        let mut source = std::mem::take(&mut self.columns)
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        let mut columns = Vec::with_capacity(projections.len());
        for (index, _) in projections {
            columns.push(
                source
                    .get_mut(*index)
                    .and_then(Option::take)
                    .ok_or_else(|| DbError::internal("projection column disappeared"))?,
            );
        }
        self.columns = columns;
        Ok(true)
    }

    pub(crate) fn can_project_columns(&self, projections: &[(usize, ScalarType)]) -> Result<bool> {
        if projections
            .iter()
            .enumerate()
            .any(|(position, (index, _))| {
                projections[..position]
                    .iter()
                    .any(|(earlier, _)| earlier == index)
            })
        {
            return Ok(false);
        }
        for (index, target) in projections {
            let column = self
                .columns
                .get(*index)
                .ok_or_else(|| DbError::internal("projection column is outside the data chunk"))?;
            if !column.matches_type(target) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .columns
                .iter()
                .map(ColumnVector::estimated_bytes)
                .sum::<usize>()
            + self.selection.indexes.capacity() * std::mem::size_of::<u32>()
    }

    pub(crate) fn estimated_selected_row_bytes(&self) -> Result<usize> {
        let mut fixed_row_bytes = std::mem::size_of::<Row>();
        for column in &self.columns {
            if let Some(bytes) = column.fixed_value_bytes() {
                fixed_row_bytes = fixed_row_bytes.checked_add(bytes).ok_or_else(|| {
                    DbError::new("53200", "query memory limit exceeded")
                        .with_detail("selected row estimate overflow")
                })?;
            }
        }
        let mut total = fixed_row_bytes
            .checked_mul(self.selection.indexes.len())
            .ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("selected chunk estimate overflow")
            })?;
        for column in &self.columns {
            if column.fixed_value_bytes().is_some() {
                continue;
            }
            for physical_index in &self.selection.indexes {
                total = total
                    .checked_add(column.estimated_value_bytes(*physical_index as usize)?)
                    .ok_or_else(|| {
                        DbError::new("53200", "query memory limit exceeded")
                            .with_detail("selected chunk estimate overflow")
                    })?;
            }
        }
        Ok(total)
    }

    fn reset_from_rows(&mut self, rows: &[Row]) -> Result<bool> {
        let kinds = rows.first().map_or_else(
            || Ok(Vec::new()),
            |row| {
                row.values
                    .iter()
                    .map(value_kind)
                    .collect::<Result<Vec<_>>>()
            },
        )?;
        if kinds.len() != self.columns.len()
            || kinds
                .iter()
                .zip(&self.columns)
                .any(|(kind, column)| *kind != column.kind())
        {
            return Ok(false);
        }
        for column in &mut self.columns {
            column.clear();
        }
        append_rows(&mut self.columns, rows)?;
        self.selection.reset_all(rows.len())?;
        self.physical_rows = rows.len();
        Ok(true)
    }
}

fn identity_row_snapshot(columns: &[ColumnVector]) -> Option<(Arc<Vec<Row>>, usize, usize)> {
    let ColumnVector::RowBacked { view: first, .. } = columns.first()? else {
        return None;
    };
    if first.column != 0
        || first
            .rows
            .get(first.start)
            .is_some_and(|row| row.values.len() != columns.len())
        || columns
            .iter()
            .enumerate()
            .any(|(column, vector)| match vector {
                ColumnVector::RowBacked { view, .. } => {
                    view.column != column
                        || view.start != first.start
                        || view.end != first.end
                        || !Arc::ptr_eq(&view.rows, &first.rows)
                }
                _ => true,
            })
    {
        return None;
    }
    Some((Arc::clone(&first.rows), first.start, first.end))
}

fn take_single_column_rows(
    column: &mut ColumnVector,
    indexes: Vec<u32>,
    rows: &mut Vec<Row>,
) -> Result<()> {
    let missing = || DbError::internal("column vector index is out of bounds");
    macro_rules! take_values {
        ($values:expr, $constructor:path) => {
            for physical_index in indexes {
                let value = $values
                    .get_mut(physical_index as usize)
                    .ok_or_else(missing)?
                    .take()
                    .map_or(Value::Null, $constructor);
                rows.push(Row::new(vec![value]));
            }
        };
    }
    match column {
        ColumnVector::RowBacked { view, .. } => {
            for physical_index in indexes {
                rows.push(Row::new(vec![view.value(physical_index as usize)?.clone()]));
            }
        }
        ColumnVector::Null(len) => {
            for physical_index in indexes {
                if physical_index as usize >= *len {
                    return Err(missing());
                }
                rows.push(Row::new(vec![Value::Null]));
            }
        }
        ColumnVector::Boolean(values) => take_values!(values, Value::Boolean),
        ColumnVector::Int16(values) => take_values!(values, Value::Int16),
        ColumnVector::Int32(values) => take_values!(values, Value::Int32),
        ColumnVector::Int64(values) => take_values!(values, Value::Int64),
        ColumnVector::Float32(values) => take_values!(values, Value::Float32),
        ColumnVector::Float64(values) => take_values!(values, Value::Float64),
        ColumnVector::Decimal(values) => take_values!(values, Value::Decimal),
        ColumnVector::Text(values) => take_values!(values, Value::Text),
        ColumnVector::Binary(values) => take_values!(values, Value::Binary),
        ColumnVector::Date(values) => take_values!(values, Value::Date),
        ColumnVector::Time(values) => take_values!(values, Value::Time),
        ColumnVector::Timestamp(values) => take_values!(values, Value::Timestamp),
        ColumnVector::Interval(values) => take_values!(values, Value::Interval),
        ColumnVector::Array(values) => take_values!(values, Value::Array),
        ColumnVector::Json(values) => take_values!(values, Value::Json),
        ColumnVector::Jsonb(values) => take_values!(values, Value::Jsonb),
        ColumnVector::Uuid(values) => take_values!(values, Value::Uuid),
        ColumnVector::Vector(values) => take_values!(values, Value::Vector),
    }
    Ok(())
}

fn compare_scalar<T: PartialOrd + PartialEq>(
    value: &T,
    literal: &T,
    operator: BinaryOperator,
) -> Result<Value> {
    let compared = match scalar_predicate_checked(value, literal, operator) {
        Some(compared) => compared,
        None => {
            return Err(DbError::internal(
                "unsupported columnar comparison operator",
            ));
        }
    };
    Ok(Value::Boolean(compared))
}

fn scalar_predicate<T: PartialOrd + PartialEq>(
    value: &T,
    literal: &T,
    operator: BinaryOperator,
) -> bool {
    scalar_predicate_checked(value, literal, operator)
        .expect("columnar predicate operator was validated")
}

fn scalar_predicate_checked<T: PartialOrd + PartialEq>(
    value: &T,
    literal: &T,
    operator: BinaryOperator,
) -> Option<bool> {
    Some(match operator {
        BinaryOperator::Eq => value == literal,
        BinaryOperator::NotEq => value != literal,
        BinaryOperator::Lt => value < literal,
        BinaryOperator::LtEq => value <= literal,
        BinaryOperator::Gt => value > literal,
        BinaryOperator::GtEq => value >= literal,
        _ => {
            return None;
        }
    })
}

fn row_backed_literal_supported(kind: ColumnVectorKind, literal: &Value) -> bool {
    matches!(
        (kind, literal),
        (ColumnVectorKind::Boolean, Value::Boolean(_))
            | (ColumnVectorKind::Int16, Value::Int16(_))
            | (ColumnVectorKind::Int32, Value::Int32(_))
            | (ColumnVectorKind::Int64, Value::Int64(_))
            | (ColumnVectorKind::Decimal, Value::Decimal(_))
            | (ColumnVectorKind::Text, Value::Text(_))
            | (ColumnVectorKind::Date, Value::Date(_))
            | (ColumnVectorKind::Time, Value::Time(_))
            | (ColumnVectorKind::Timestamp, Value::Timestamp(_))
            | (ColumnVectorKind::Uuid, Value::Uuid(_))
            | (ColumnVectorKind::Null, _)
    )
}

fn value_predicate(value: &Value, literal: &Value, operator: BinaryOperator) -> bool {
    if value.is_null() || literal.is_null() {
        return false;
    }
    if matches!(operator, BinaryOperator::Eq | BinaryOperator::NotEq) {
        return if operator == BinaryOperator::Eq {
            value == literal
        } else {
            value != literal
        };
    }
    match (value, literal) {
        (Value::Boolean(value), Value::Boolean(literal)) => {
            scalar_predicate(value, literal, operator)
        }
        (Value::Int16(value), Value::Int16(literal)) => scalar_predicate(value, literal, operator),
        (Value::Int32(value), Value::Int32(literal)) => scalar_predicate(value, literal, operator),
        (Value::Int64(value), Value::Int64(literal)) => scalar_predicate(value, literal, operator),
        (Value::Decimal(value), Value::Decimal(literal)) => {
            scalar_predicate(value, literal, operator)
        }
        (Value::Text(value), Value::Text(literal)) => scalar_predicate(value, literal, operator),
        (Value::Date(value), Value::Date(literal)) => scalar_predicate(value, literal, operator),
        (Value::Time(value), Value::Time(literal)) => scalar_predicate(value, literal, operator),
        (Value::Timestamp(value), Value::Timestamp(literal)) => {
            scalar_predicate(value, literal, operator)
        }
        (Value::Uuid(value), Value::Uuid(literal)) => scalar_predicate(value, literal, operator),
        _ => false,
    }
}

fn compare_value_literal(
    value: Result<&Value>,
    literal: &Value,
    operator: BinaryOperator,
) -> Option<Result<Value>> {
    let value = match value {
        Ok(value) => value,
        Err(error) => return Some(Err(error)),
    };
    if value.is_null() || literal.is_null() {
        return Some(Ok(Value::Null));
    }
    if matches!(operator, BinaryOperator::Eq | BinaryOperator::NotEq) {
        return Some(Ok(Value::Boolean(if operator == BinaryOperator::Eq {
            value == literal
        } else {
            value != literal
        })));
    }
    match (value, literal) {
        (Value::Boolean(value), Value::Boolean(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Int16(value), Value::Int16(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Int32(value), Value::Int32(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Int64(value), Value::Int64(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Decimal(value), Value::Decimal(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Text(value), Value::Text(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Date(value), Value::Date(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Time(value), Value::Time(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Timestamp(value), Value::Timestamp(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        (Value::Uuid(value), Value::Uuid(literal)) => {
            Some(compare_scalar(value, literal, operator))
        }
        _ => None,
    }
}

/// Reuses compatible column buffers while retaining a strict bounded cache.
#[derive(Debug)]
pub struct ChunkPool {
    chunks: Vec<(DataChunk, Reservation)>,
    max_retained: usize,
    max_rows: usize,
}

impl ChunkPool {
    #[must_use]
    pub fn new(max_rows: usize, max_retained: usize) -> Self {
        Self {
            chunks: Vec::new(),
            max_retained,
            max_rows,
        }
    }

    pub fn materialize(
        &mut self,
        rows: &[Row],
        grant: &MemoryGrant,
    ) -> Result<(DataChunk, Reservation)> {
        if rows.len() > self.max_rows {
            return Err(DbError::new(
                "54000",
                "data chunk exceeds the configured row limit",
            ));
        }
        while let Some((mut chunk, mut reservation)) = self.chunks.pop() {
            if chunk.reset_from_rows(rows)? {
                reservation.resize(chunk.estimated_bytes())?;
                return Ok((chunk, reservation));
            }
            drop(reservation);
            drop(chunk);
        }
        let estimated = rows
            .iter()
            .map(estimated_row_bytes)
            .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
            .ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("chunk pool row estimate overflow")
            })?;
        let mut reservation = grant.try_reserve(estimated)?;
        let chunk = DataChunk::from_rows(rows)?;
        reservation.resize(chunk.estimated_bytes())?;
        Ok((chunk, reservation))
    }

    pub fn recycle(&mut self, mut chunk: DataChunk, reservation: Reservation) {
        if self.chunks.len() >= self.max_retained || chunk.physical_rows > self.max_rows {
            return;
        }
        for column in &mut chunk.columns {
            column.clear();
        }
        chunk.selection.indexes.clear();
        chunk.physical_rows = 0;
        self.chunks.push((chunk, reservation));
    }

    #[must_use]
    pub fn retained(&self) -> usize {
        self.chunks.len()
    }
}

fn infer_kinds(rows: &[Row]) -> Result<Vec<ColumnVectorKind>> {
    let width = rows.first().map_or(0, |row| row.values.len());
    if rows.iter().any(|row| row.values.len() != width) {
        return Err(DbError::internal("data chunk rows have different widths"));
    }
    (0..width)
        .map(|column_index| {
            rows.iter()
                .map(|row| &row.values[column_index])
                .find(|value| !value.is_null())
                .map_or(Ok(ColumnVectorKind::Null), value_kind)
        })
        .collect()
}

fn estimated_array_bytes(array: &PgArray) -> usize {
    std::mem::size_of::<PgArray>()
        .saturating_add(
            array
                .dimensions()
                .len()
                .saturating_mul(std::mem::size_of::<ordadb_types::ArrayDimension>()),
        )
        .saturating_add(
            array
                .values()
                .iter()
                .map(estimated_value_bytes)
                .sum::<usize>(),
        )
}

fn value_kind(value: &Value) -> Result<ColumnVectorKind> {
    Ok(match value {
        Value::Null => ColumnVectorKind::Null,
        Value::Boolean(_) => ColumnVectorKind::Boolean,
        Value::Int16(_) => ColumnVectorKind::Int16,
        Value::Int32(_) => ColumnVectorKind::Int32,
        Value::Int64(_) => ColumnVectorKind::Int64,
        Value::Float32(_) => ColumnVectorKind::Float32,
        Value::Float64(_) => ColumnVectorKind::Float64,
        Value::Decimal(_) => ColumnVectorKind::Decimal,
        Value::Text(_) => ColumnVectorKind::Text,
        Value::Binary(_) => ColumnVectorKind::Binary,
        Value::Date(_) => ColumnVectorKind::Date,
        Value::Time(_) => ColumnVectorKind::Time,
        Value::Timestamp(_) => ColumnVectorKind::Timestamp,
        Value::Interval(_) => ColumnVectorKind::Interval,
        Value::Array(_) => ColumnVectorKind::Array,
        Value::Json(_) => ColumnVectorKind::Json,
        Value::Jsonb(_) => ColumnVectorKind::Jsonb,
        Value::Uuid(_) => ColumnVectorKind::Uuid,
        Value::Vector(_) => ColumnVectorKind::Vector,
    })
}

fn append_rows(columns: &mut [ColumnVector], rows: &[Row]) -> Result<()> {
    for row in rows {
        for (column, value) in columns.iter_mut().zip(&row.values) {
            column.push(value)?;
        }
    }
    Ok(())
}
