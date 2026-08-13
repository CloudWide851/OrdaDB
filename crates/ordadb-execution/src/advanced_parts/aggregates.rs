
fn group_stack_slots(instructions: &[GroupInstruction]) -> Result<usize> {
    let mut depth = 0_usize;
    let mut maximum = 0_usize;
    for instruction in instructions {
        match instruction {
            GroupInstruction::LoadColumn(_)
            | GroupInstruction::LoadLiteral(_)
            | GroupInstruction::LoadParameter(_)
            | GroupInstruction::AggregateValue(_) => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| program_limit_error("group value stack depth overflowed"))?;
                maximum = maximum.max(depth);
            }
            GroupInstruction::Unary(_)
            | GroupInstruction::Cast(_)
            | GroupInstruction::Coerce(_) => {
                if depth == 0 {
                    return Err(DbError::internal(
                        "group expression compiler produced a stack underflow",
                    ));
                }
            }
            GroupInstruction::Binary { .. } => {
                if depth < 2 {
                    return Err(DbError::internal(
                        "group expression compiler produced a stack underflow",
                    ));
                }
                depth -= 1;
            }
            GroupInstruction::InList { count, .. } => {
                let required = count.saturating_add(1);
                if depth < required {
                    return Err(DbError::internal(
                        "group expression compiler produced an IN list stack underflow",
                    ));
                }
                depth -= *count;
            }
            GroupInstruction::MakeArray { count, .. } => {
                if *count == 0 {
                    depth = depth
                        .checked_add(1)
                        .ok_or_else(|| program_limit_error("group value stack depth overflowed"))?;
                    maximum = maximum.max(depth);
                } else {
                    if depth < *count {
                        return Err(DbError::internal(
                            "group expression compiler produced an array stack underflow",
                        ));
                    }
                    depth = depth - *count + 1;
                }
            }
            GroupInstruction::Function { count, .. } => {
                if *count == 0 || depth < *count {
                    return Err(DbError::internal(
                        "group expression compiler produced a function stack underflow",
                    ));
                }
                depth = depth - *count + 1;
            }
        }
    }
    if depth != 1 {
        return Err(DbError::internal(
            "group expression compiler did not produce one stack result",
        ));
    }
    Ok(maximum)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupAccumulator {
    key: Vec<Value>,
    representative: Row,
    aggregates: Vec<AggregateState>,
    first_ordinal: u64,
}

impl GroupAccumulator {
    fn new(
        key: Vec<Value>,
        representative: Row,
        first_ordinal: u64,
        specs: &[AggregateSpec],
        params: &[Value],
        stack: &mut ExpressionStack,
    ) -> Result<Self> {
        let mut group = Self {
            key,
            representative: representative.clone(),
            aggregates: specs.iter().map(AggregateState::new).collect(),
            first_ordinal,
        };
        group.update(specs, &representative, params, stack)?;
        Ok(group)
    }

    fn empty(specs: &[AggregateSpec]) -> Self {
        Self {
            key: Vec::new(),
            representative: Row::new(Vec::new()),
            aggregates: specs.iter().map(AggregateState::new).collect(),
            first_ordinal: 0,
        }
    }

    fn update(
        &mut self,
        specs: &[AggregateSpec],
        row: &Row,
        params: &[Value],
        stack: &mut ExpressionStack,
    ) -> Result<()> {
        for (state, spec) in self.aggregates.iter_mut().zip(specs) {
            state.update(spec, row, params, stack)?;
        }
        Ok(())
    }

    fn merge(&mut self, other: Self, specs: &[AggregateSpec]) -> Result<()> {
        if other.first_ordinal < self.first_ordinal {
            self.first_ordinal = other.first_ordinal;
            self.representative = other.representative.clone();
        }
        if self.aggregates.len() != specs.len() || other.aggregates.len() != specs.len() {
            return Err(DbError::internal("aggregate spill state width changed"));
        }
        for ((state, incoming), spec) in self.aggregates.iter_mut().zip(other.aggregates).zip(specs)
        {
            state.merge(incoming, spec)?;
        }
        Ok(())
    }

    fn estimated_bytes(&self) -> usize {
        estimated_row_bytes(&self.representative)
            .saturating_add(self.key.iter().map(estimated_value_bytes).sum::<usize>())
            .saturating_add(
                self.aggregates
                    .iter()
                    .map(AggregateState::estimated_bytes)
                    .sum::<usize>(),
            )
            .saturating_add(64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum AggregateState {
    Count(u64),
    Sum(Option<Value>),
    Avg { sum: f64, count: u64 },
    Min(Option<Value>),
    Max(Option<Value>),
    Distinct(#[serde(with = "distinct_values_serde")] BTreeMap<DistinctValueKey, Value>),
}

impl AggregateState {
    fn new(spec: &AggregateSpec) -> Self {
        if spec.distinct {
            return Self::Distinct(BTreeMap::new());
        }
        match spec.function {
            AggregateFunction::Count => Self::Count(0),
            AggregateFunction::Sum => Self::Sum(None),
            AggregateFunction::Avg => Self::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Min => Self::Min(None),
            AggregateFunction::Max => Self::Max(None),
        }
    }

    fn update(
        &mut self,
        spec: &AggregateSpec,
        row: &Row,
        params: &[Value],
        stack: &mut ExpressionStack,
    ) -> Result<()> {
        if let Some(filter) = &spec.filter {
            match filter.evaluate_reusing(&row.values, params, stack)? {
                Value::Boolean(true) => {}
                Value::Boolean(false) | Value::Null => return Ok(()),
                _ => {
                    return Err(DbError::new(
                        "42804",
                        "aggregate FILTER predicate must be boolean",
                    ));
                }
            }
        }
        let value = spec
            .argument
            .as_ref()
            .map(|argument| argument.evaluate_reusing(&row.values, params, stack))
            .transpose()?;
        if let Self::Distinct(values) = self {
            let value = value
                .ok_or_else(|| DbError::internal("DISTINCT aggregate argument is unavailable"))?;
            if value.is_null() {
                return Ok(());
            }
            values.entry(distinct_value_key(&value)).or_insert(value);
            return Ok(());
        }
        match self {
            Self::Count(count) => {
                if value.as_ref().is_none_or(|value| !value.is_null()) {
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| DbError::new("22003", "COUNT result is out of range"))?;
                }
            }
            Self::Sum(sum) => {
                if let Some(value) = value.filter(|value| !value.is_null()) {
                    *sum = Some(match sum.take() {
                        None => value,
                        Some(existing) => add_values(existing, value)?,
                    });
                }
            }
            Self::Avg { sum, count } => {
                if let Some(value) = value.filter(|value| !value.is_null()) {
                    *sum += numeric_value(&value)?;
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| DbError::new("22003", "AVG count is out of range"))?;
                }
            }
            Self::Min(selected) => select_value(
                selected,
                value,
                Ordering::Less,
                aggregate_argument_type(spec)?,
            )?,
            Self::Max(selected) => select_value(
                selected,
                value,
                Ordering::Greater,
                aggregate_argument_type(spec)?,
            )?,
            Self::Distinct(_) => unreachable!("DISTINCT aggregate handled before state update"),
        }
        Ok(())
    }

    fn merge(&mut self, incoming: Self, spec: &AggregateSpec) -> Result<()> {
        match (self, incoming) {
            (Self::Count(left), Self::Count(right)) => {
                *left = left
                    .checked_add(right)
                    .ok_or_else(|| DbError::new("22003", "COUNT result is out of range"))?;
            }
            (Self::Sum(left), Self::Sum(right)) => {
                if let Some(right) = right {
                    *left = Some(match left.take() {
                        None => right,
                        Some(existing) => add_values(existing, right)?,
                    });
                }
            }
            (
                Self::Avg {
                    sum: left_sum,
                    count: left_count,
                },
                Self::Avg {
                    sum: right_sum,
                    count: right_count,
                },
            ) => {
                *left_sum += right_sum;
                *left_count = left_count
                    .checked_add(right_count)
                    .ok_or_else(|| DbError::new("22003", "AVG count is out of range"))?;
            }
            (Self::Min(left), Self::Min(right)) => {
                select_value(left, right, Ordering::Less, aggregate_argument_type(spec)?)?;
            }
            (Self::Max(left), Self::Max(right)) => {
                select_value(
                    left,
                    right,
                    Ordering::Greater,
                    aggregate_argument_type(spec)?,
                )?;
            }
            (Self::Distinct(left), Self::Distinct(right)) => {
                for (key, value) in right {
                    left.entry(key).or_insert(value);
                }
            }
            _ => return Err(DbError::internal("aggregate spill state kind changed")),
        }
        Ok(())
    }

    fn value(&self, spec: &AggregateSpec) -> Result<Value> {
        match self {
            Self::Count(count) => i64::try_from(*count)
                .map(Value::Int64)
                .map_err(|_| DbError::new("22003", "COUNT result is out of range")),
            Self::Sum(value) | Self::Min(value) | Self::Max(value) => {
                Ok(value.clone().unwrap_or(Value::Null))
            }
            Self::Avg { sum: _, count } if *count == 0 => Ok(Value::Null),
            Self::Avg { sum, count } => Ok(Value::Float64(*sum / *count as f64)),
            Self::Distinct(values) => distinct_aggregate_value(spec, values),
        }
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Distinct(values) => values.iter().fold(64_usize, |total, (key, value)| {
                total
                    .saturating_add(std::mem::size_of::<DistinctValueKey>())
                    .saturating_add(distinct_key_dynamic_bytes(key))
                    .saturating_add(estimated_value_bytes(value))
                    .saturating_add(std::mem::size_of::<usize>().saturating_mul(3))
            }),
            _ => 64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
enum DistinctValueKey {
    Null,
    Boolean(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(u32),
    Float64(u64),
    Decimal(String),
    Text(String),
    Binary(Vec<u8>),
    Date(String),
    Time(String),
    Timestamp(String),
    Interval(i32, i32, i64),
    Array {
        element_type: String,
        dimensions: Vec<(u32, i32)>,
        values: Vec<DistinctValueKey>,
    },
    Json(String),
    Jsonb(String),
    Uuid([u8; 16]),
    Vector(Vec<u32>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DistinctRowKey(Vec<DistinctValueKey>);

mod distinct_values_serde {
    use super::{BTreeMap, DistinctValueKey, Value};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        values: &BTreeMap<DistinctValueKey, Value>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        values.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<DistinctValueKey, Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<(DistinctValueKey, Value)>::deserialize(deserializer)
            .map(|entries| entries.into_iter().collect())
    }
}

fn distinct_value_key(value: &Value) -> DistinctValueKey {
    match value {
        Value::Null => DistinctValueKey::Null,
        Value::Boolean(value) => DistinctValueKey::Boolean(*value),
        Value::Int16(value) => DistinctValueKey::Int16(*value),
        Value::Int32(value) => DistinctValueKey::Int32(*value),
        Value::Int64(value) => DistinctValueKey::Int64(*value),
        Value::Float32(value) => DistinctValueKey::Float32(canonical_f32_bits(*value)),
        Value::Float64(value) => DistinctValueKey::Float64(canonical_f64_bits(*value)),
        Value::Decimal(value) => DistinctValueKey::Decimal(value.normalize().to_string()),
        Value::Text(value) => DistinctValueKey::Text(value.clone()),
        Value::Binary(value) => DistinctValueKey::Binary(value.clone()),
        Value::Date(value) => DistinctValueKey::Date(value.to_string()),
        Value::Time(value) => DistinctValueKey::Time(value.to_string()),
        Value::Timestamp(value) => DistinctValueKey::Timestamp(value.to_string()),
        Value::Interval(value) => {
            DistinctValueKey::Interval(value.months, value.days, value.microseconds)
        }
        Value::Array(value) => DistinctValueKey::Array {
            element_type: format!("{:?}", value.element_type()),
            dimensions: value
                .dimensions()
                .iter()
                .map(|dimension| (dimension.length, dimension.lower_bound))
                .collect(),
            values: value.values().iter().map(distinct_value_key).collect(),
        },
        Value::Json(value) => DistinctValueKey::Json(value.to_string()),
        Value::Jsonb(value) => DistinctValueKey::Jsonb(value.to_string()),
        Value::Uuid(value) => DistinctValueKey::Uuid(*value.as_bytes()),
        Value::Vector(values) => DistinctValueKey::Vector(
            values
                .iter()
                .map(|value| canonical_f32_bits(*value))
                .collect(),
        ),
    }
}

fn canonical_f32_bits(value: f32) -> u32 {
    if value == 0.0 {
        0.0_f32.to_bits()
    } else if value.is_nan() {
        f32::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

fn distinct_key_dynamic_bytes(key: &DistinctValueKey) -> usize {
    match key {
        DistinctValueKey::Decimal(value)
        | DistinctValueKey::Text(value)
        | DistinctValueKey::Date(value)
        | DistinctValueKey::Time(value)
        | DistinctValueKey::Timestamp(value)
        | DistinctValueKey::Json(value)
        | DistinctValueKey::Jsonb(value) => value.len(),
        DistinctValueKey::Array {
            element_type,
            dimensions,
            values,
        } => element_type
            .len()
            .saturating_add(
                dimensions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u32, i32)>()),
            )
            .saturating_add(
                values
                    .iter()
                    .map(|value| {
                        std::mem::size_of::<DistinctValueKey>()
                            .saturating_add(distinct_key_dynamic_bytes(value))
                    })
                    .sum::<usize>(),
            ),
        DistinctValueKey::Binary(value) => value.len(),
        DistinctValueKey::Vector(value) => value.len().saturating_mul(std::mem::size_of::<u32>()),
        DistinctValueKey::Boolean(_)
        | DistinctValueKey::Null
        | DistinctValueKey::Int16(_)
        | DistinctValueKey::Int32(_)
        | DistinctValueKey::Int64(_)
        | DistinctValueKey::Float32(_)
        | DistinctValueKey::Float64(_)
        | DistinctValueKey::Interval(_, _, _)
        | DistinctValueKey::Uuid(_) => 0,
    }
}

fn estimated_distinct_row_key_bytes(key: &DistinctRowKey) -> usize {
    std::mem::size_of::<DistinctRowKey>()
        .saturating_add(
            key.0
                .len()
                .saturating_mul(std::mem::size_of::<DistinctValueKey>()),
        )
        .saturating_add(key.0.iter().map(distinct_key_dynamic_bytes).sum::<usize>())
        .saturating_add(std::mem::size_of::<usize>().saturating_mul(2))
}

fn distinct_aggregate_value(
    spec: &AggregateSpec,
    values: &BTreeMap<DistinctValueKey, Value>,
) -> Result<Value> {
    match spec.function {
        AggregateFunction::Count => i64::try_from(values.len())
            .map(Value::Int64)
            .map_err(|_| DbError::new("22003", "COUNT result is out of range")),
        AggregateFunction::Sum => {
            let mut sum = None;
            for value in values.values().cloned() {
                sum = Some(match sum {
                    None => value,
                    Some(existing) => add_values(existing, value)?,
                });
            }
            Ok(sum.unwrap_or(Value::Null))
        }
        AggregateFunction::Avg => {
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let mut sum = 0.0;
            for value in values.values() {
                sum += numeric_value(value)?;
            }
            Ok(Value::Float64(sum / values.len() as f64))
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let desired = if spec.function == AggregateFunction::Min {
                Ordering::Less
            } else {
                Ordering::Greater
            };
            let mut selected = None;
            for value in values.values().cloned() {
                select_value(
                    &mut selected,
                    Some(value),
                    desired,
                    aggregate_argument_type(spec)?,
                )?;
            }
            Ok(selected.unwrap_or(Value::Null))
        }
    }
}

fn aggregate_argument_type(spec: &AggregateSpec) -> Result<&ScalarType> {
    spec.argument
        .as_ref()
        .map(ExpressionProgram::result_type)
        .ok_or_else(|| DbError::internal("aggregate argument type is unavailable"))
}

fn select_value(
    selected: &mut Option<Value>,
    value: Option<Value>,
    desired: Ordering,
    data_type: &ScalarType,
) -> Result<()> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let replace = selected
        .as_ref()
        .map(|current| {
            super::compare_values_as(&value, current, data_type).map(|order| order == desired)
        })
        .transpose()?
        .unwrap_or(true);
    if replace {
        *selected = Some(value);
    }
    Ok(())
}

fn add_values(left: Value, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Int16(left), Value::Int16(right)) => i64::from(left)
            .checked_add(i64::from(right))
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        (Value::Int32(left), Value::Int32(right)) => i64::from(left)
            .checked_add(i64::from(right))
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        (Value::Int64(left), Value::Int64(right)) => left
            .checked_add(right)
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        (Value::Int64(left), Value::Int16(right)) => left
            .checked_add(i64::from(right))
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        (Value::Int64(left), Value::Int32(right)) => left
            .checked_add(i64::from(right))
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        (Value::Float32(left), Value::Float32(right)) => {
            Ok(Value::Float64(f64::from(left) + f64::from(right)))
        }
        (Value::Float64(left), Value::Float32(right)) => {
            Ok(Value::Float64(left + f64::from(right)))
        }
        (Value::Float64(left), Value::Float64(right)) => Ok(Value::Float64(left + right)),
        (Value::Decimal(left), Value::Decimal(right)) => left
            .checked_add(right)
            .map(Value::Decimal)
            .ok_or_else(|| DbError::new("22003", "SUM result is out of range")),
        _ => Err(DbError::new("42804", "SUM values have mixed types")),
    }
}

fn numeric_value(value: &Value) -> Result<f64> {
    match value {
        Value::Int16(value) => Ok(f64::from(*value)),
        Value::Int32(value) => Ok(f64::from(*value)),
        Value::Int64(value) => Ok(*value as f64),
        Value::Float32(value) => Ok(f64::from(*value)),
        Value::Float64(value) => Ok(*value),
        Value::Decimal(value) => value
            .to_string()
            .parse()
            .map_err(|_| DbError::new("22003", "decimal cannot be represented as FLOAT8")),
        _ => Err(DbError::new("42804", "numeric value required")),
    }
}

struct RowsOutputBuilder {
    order_by: Vec<BoundOrder>,
    sort_programs: Vec<Option<ExpressionProgram>>,
    rows: Vec<Row>,
    reservation: Reservation,
    run_paths: Vec<PathBuf>,
}

impl RowsOutputBuilder {
    fn new(
        order_by: &[BoundOrder],
        memory: &QueryMemoryContext,
        max_expression_depth: usize,
    ) -> Result<Self> {
        let (order_by, sort_programs) = super::compile_sort_orders(order_by, max_expression_depth)?;
        Ok(Self {
            order_by,
            sort_programs,
            rows: Vec::new(),
            reservation: memory.try_reserve(0)?,
            run_paths: Vec::new(),
        })
    }

    fn push(
        &mut self,
        mut row: Row,
        params: &[Value],
        stack: &mut ExpressionStack,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<()> {
        super::materialize_sort_keys(
            &mut row,
            &mut self.order_by,
            &self.sort_programs,
            params,
            stack,
        )?;
        let bytes = estimated_row_bytes(&row);
        if !self.rows.is_empty() && memory.would_cross_soft_limit(bytes) {
            sort_rows(&mut self.rows, &self.order_by)?;
            self.run_paths
                .push(spill.write_sorted_run(&self.rows, memory)?);
            self.rows.clear();
            self.reservation.resize(0)?;
        }
        self.reservation.grow(bytes)?;
        self.rows.push(row);
        Ok(())
    }

    fn push_transferred(
        &mut self,
        mut row: Row,
        params: &[Value],
        stack: &mut ExpressionStack,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
        source_reservation: &mut Reservation,
    ) -> Result<()> {
        let transferred_bytes = estimated_row_bytes(&row);
        super::materialize_sort_keys(
            &mut row,
            &mut self.order_by,
            &self.sort_programs,
            params,
            stack,
        )?;
        let bytes = estimated_row_bytes(&row);
        let additional = bytes.saturating_sub(transferred_bytes);
        if !self.rows.is_empty() && memory.would_cross_soft_limit(additional) {
            sort_rows(&mut self.rows, &self.order_by)?;
            self.run_paths
                .push(spill.write_sorted_run(&self.rows, memory)?);
            self.rows.clear();
            self.reservation.resize(0)?;
        }
        source_reservation.transfer_to(&mut self.reservation, transferred_bytes)?;
        self.reservation.grow(additional)?;
        self.rows.push(row);
        Ok(())
    }

    fn finish(
        mut self,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<RowsOutput> {
        if self.run_paths.is_empty() {
            sort_rows(&mut self.rows, &self.order_by)?;
            return Ok(RowsOutput::Memory {
                rows: self.rows,
                offset: 0,
                reservation: Some(self.reservation),
            });
        }
        if !self.rows.is_empty() {
            sort_rows(&mut self.rows, &self.order_by)?;
            self.run_paths
                .push(spill.write_sorted_run(&self.rows, memory)?);
            self.rows.clear();
            self.reservation.resize(0)?;
        }
        let run_paths = spill.compact_sorted_runs(self.run_paths, &self.order_by, memory)?;
        Ok(RowsOutput::Runs {
            merge: SpillMergeCursor::open(&run_paths, &self.order_by, memory)?,
            order_by: self.order_by,
        })
    }
}

enum RowsOutput {
    Memory {
        rows: Vec<Row>,
        offset: usize,
        reservation: Option<Reservation>,
    },
    Runs {
        merge: SpillMergeCursor,
        order_by: Vec<BoundOrder>,
    },
    Indexed {
        store: IndexedRowStore,
        offset: usize,
        current_reservation: Option<Reservation>,
    },
}

impl RowsOutput {
    fn into_window_store(
        self,
        memory: &QueryMemoryContext,
        spill: &mut SpillManager,
    ) -> Result<WindowRowStore> {
        match self {
            Self::Memory {
                rows,
                offset,
                reservation,
            } => {
                if offset != 0 {
                    return Err(DbError::internal(
                        "cannot materialize a partially consumed grouped window input",
                    ));
                }
                Ok(WindowRowStore::Memory {
                    rows,
                    reservation: reservation.ok_or_else(|| {
                        DbError::internal("grouped window input reservation is unavailable")
                    })?,
                })
            }
            Self::Runs {
                mut merge,
                order_by,
            } => {
                let mut rows = WindowRowStoreBuilder::new(memory)?;
                while let Some(row) = merge.pop_next(&order_by, memory)? {
                    rows.push(row, memory, spill)?;
                }
                rows.finish(memory)
            }
            Self::Indexed {
                store,
                offset,
                current_reservation,
            } => {
                if offset != 0 || current_reservation.is_some() {
                    return Err(DbError::internal(
                        "cannot reuse a partially consumed indexed window input",
                    ));
                }
                Ok(WindowRowStore::Spill(store))
            }
        }
    }

    fn next_row(&mut self, memory: &QueryMemoryContext) -> Result<Option<Row>> {
        match self {
            Self::Memory {
                rows,
                offset,
                reservation,
            } => {
                let row = rows.get(*offset).cloned();
                *offset = offset.saturating_add(1);
                if row.is_none() {
                    *reservation = None;
                }
                Ok(row)
            }
            Self::Runs { merge, order_by } => merge.pop_next(order_by, memory),
            Self::Indexed {
                store,
                offset,
                current_reservation,
            } => {
                if *offset >= store.len {
                    *current_reservation = None;
                    return Ok(None);
                }
                let ReservedRow { row, reservation } = store.read(*offset, memory)?;
                *offset = offset.saturating_add(1);
                *current_reservation = Some(reservation);
                Ok(Some(row))
            }
        }
    }
}

struct ReservedValues<T> {
    values: Vec<T>,
    reservation: Reservation,
}
