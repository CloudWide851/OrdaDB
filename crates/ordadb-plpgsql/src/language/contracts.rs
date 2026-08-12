use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ordadb_types::{DbError, DbNotice, DbNoticeSeverity, QueryEvent, Result, Value};
use tempfile::NamedTempFile;

pub const BYTECODE_VERSION: u16 = 3;

#[derive(Debug)]
struct VmMemoryState {
    hard_limit: usize,
    current: AtomicUsize,
    peak: AtomicUsize,
}

/// A cloneable retained-memory budget shared by every frame in one routine chain.
#[derive(Debug, Clone)]
pub struct VmMemoryGrant {
    state: Arc<VmMemoryState>,
}

impl VmMemoryGrant {
    pub fn new(hard_limit: usize) -> Result<Self> {
        if hard_limit == 0 {
            return Err(DbError::new(
                "22023",
                "PL/pgSQL retained-memory limit must be positive",
            ));
        }
        Ok(Self {
            state: Arc::new(VmMemoryState {
                hard_limit,
                current: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            }),
        })
    }

    #[must_use]
    pub fn current_bytes(&self) -> usize {
        self.state.current.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn peak_bytes(&self) -> usize {
        self.state.peak.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn hard_limit_bytes(&self) -> usize {
        self.state.hard_limit
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<VmMemoryReservation> {
        self.acquire(bytes)?;
        Ok(VmMemoryReservation {
            grant: self.clone(),
            bytes,
        })
    }

    fn acquire(&self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut current = self.state.current.load(Ordering::Relaxed);
        loop {
            let next = current.checked_add(bytes).ok_or_else(|| {
                DbError::new("53200", "PL/pgSQL retained-memory accounting overflowed")
            })?;
            if next > self.state.hard_limit {
                return Err(DbError::new(
                    "53200",
                    "PL/pgSQL retained-memory limit exceeded",
                )
                .with_detail(format!(
                    "requested {bytes} bytes with {current} of {} bytes already retained",
                    self.state.hard_limit
                ))
                .with_hint(
                    "Reduce routine input, cursor width, returned rows, or nested routine depth.",
                ));
            }
            match self.state.current.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.state.peak.fetch_max(next, Ordering::Relaxed);
                    return Ok(());
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut current = self.state.current.load(Ordering::Relaxed);
        loop {
            debug_assert!(
                bytes <= current,
                "PL/pgSQL reservation release exceeded usage"
            );
            let next = current.saturating_sub(bytes);
            match self.state.current.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

#[derive(Debug)]
pub struct VmMemoryReservation {
    grant: VmMemoryGrant,
    bytes: usize,
}

impl VmMemoryReservation {
    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn resize(&mut self, bytes: usize) -> Result<()> {
        match bytes.cmp(&self.bytes) {
            std::cmp::Ordering::Greater => {
                self.grant.acquire(bytes - self.bytes)?;
                self.bytes = bytes;
            }
            std::cmp::Ordering::Less => {
                self.grant.release(self.bytes - bytes);
                self.bytes = bytes;
            }
            std::cmp::Ordering::Equal => {}
        }
        Ok(())
    }
}

impl Drop for VmMemoryReservation {
    fn drop(&mut self) {
        self.grant.release(self.bytes);
        self.bytes = 0;
    }
}

#[derive(Debug, Clone)]
pub struct VmMemoryHold(Arc<VmMemoryReservation>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceLimits {
    pub max_source_bytes: usize,
    pub max_tokens: usize,
    pub max_nesting: usize,
    pub max_instructions: usize,
    pub max_steps: usize,
    pub max_returned_rows: usize,
    pub max_dynamic_sql_bytes: usize,
    pub max_open_cursors: usize,
    pub max_cursor_rows: usize,
    pub max_cursor_bytes: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_source_bytes: 256 * 1024,
            max_tokens: 65_536,
            max_nesting: 128,
            max_instructions: 65_536,
            max_steps: 1_000_000,
            max_returned_rows: 100_000,
            max_dynamic_sql_bytes: 1024 * 1024,
            max_open_cursors: 64,
            max_cursor_rows: 100_000,
            max_cursor_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSlot {
    pub name: String,
    pub constant: bool,
    pub kind: LocalKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalKind {
    Scalar,
    Record,
    RowType(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorDeclaration {
    pub name: String,
    pub bound_query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorQuery {
    Bound,
    Static(String),
    Dynamic { query: String, using: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorDirection {
    Next,
    Prior,
    First,
    Last,
    Absolute(String),
    Relative(String),
    Forward(Option<String>),
    ForwardAll,
    Backward(Option<String>),
    BackwardAll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Instruction {
    Assign {
        slot: usize,
        expression: String,
    },
    AssignField {
        slot: usize,
        field: String,
        expression: String,
    },
    JumpIfFalse {
        expression: String,
        target: usize,
    },
    Jump {
        target: usize,
    },
    ExecuteSql {
        sql: String,
        into: Option<usize>,
    },
    DynamicExecute {
        query: String,
        using: Vec<String>,
        into: Option<usize>,
        strict: bool,
    },
    OpenCursor {
        cursor: usize,
        query: CursorQuery,
    },
    FetchCursor {
        cursor: usize,
        direction: CursorDirection,
        into: usize,
    },
    MoveCursor {
        cursor: usize,
        direction: CursorDirection,
    },
    CloseCursor {
        cursor: usize,
    },
    Raise {
        level: RaiseLevel,
        message: Option<String>,
        sql_state: Option<String>,
    },
    Assert {
        condition: String,
        message: Option<String>,
    },
    QueryForStart {
        slot: usize,
        sql: String,
        end: usize,
    },
    QueryForNext {
        start: usize,
        body: usize,
    },
    IntegerForStart {
        slot: usize,
        lower: String,
        upper: String,
        step: String,
        reverse: bool,
        end: usize,
    },
    IntegerForNext {
        start: usize,
        body: usize,
    },
    ForeachStart {
        slot: usize,
        array: String,
        end: usize,
    },
    ForeachNext {
        start: usize,
        body: usize,
    },
    Return {
        expression: Option<String>,
        next: bool,
    },
    Checkpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaiseLevel {
    Info,
    Notice,
    Warning,
    Exception,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExceptionMatcher {
    SqlState(String),
    Others,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExceptionHandler {
    pub protected_start: usize,
    pub protected_end: usize,
    pub matcher: ExceptionMatcher,
    pub target: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    pub version: u16,
    pub instructions: Vec<Instruction>,
    pub locals: Vec<LocalSlot>,
    pub cursor_declarations: Vec<CursorDeclaration>,
    pub exception_handlers: Vec<ExceptionHandler>,
    pub sqlstate_slot: Option<usize>,
    pub sqlerrm_slot: Option<usize>,
}

pub trait PlpgsqlHost {
    fn execute_sql(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>>>>;

    fn evaluate_expression(&mut self, sql: &str, parameters: &[Value]) -> Result<Value>;

    fn assign_composite_field(&mut self, slot: usize, field: &str, value: Value) -> Result<()> {
        let _ = (slot, field, value);
        Err(DbError::new(
            "0A000",
            "composite assignment is not supported in this PL/pgSQL context",
        ))
    }

    fn begin_exception_block(&mut self) -> Result<()> {
        Ok(())
    }

    fn commit_exception_block(&mut self) -> Result<()> {
        Ok(())
    }

    fn rollback_exception_block(&mut self) -> Result<()> {
        Ok(())
    }

    fn emit_notice(&mut self, _notice: DbNotice) -> Result<()> {
        Ok(())
    }

    fn resolve_row_type(&mut self, relation: &str) -> Result<Vec<String>> {
        Err(DbError::new(
            "0A000",
            format!("%ROWTYPE resolution is not available for {relation}"),
        ))
    }

    fn check_cancelled(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct VmOutput {
    pub return_value: Option<Value>,
    pub returned_rows: Vec<Value>,
    pub return_parameter: Option<usize>,
    pub final_locals: Vec<Value>,
    pub output_parameters: Vec<Value>,
    retained_memory: Option<VmMemoryHold>,
}

impl PartialEq for VmOutput {
    fn eq(&self, other: &Self) -> bool {
        self.return_value == other.return_value
            && self.returned_rows == other.returned_rows
            && self.return_parameter == other.return_parameter
            && self.final_locals == other.final_locals
            && self.output_parameters == other.output_parameters
    }
}

impl VmOutput {
    pub fn refresh_retained_memory(&mut self) -> Result<()> {
        let bytes = estimated_vm_output_bytes(self)?;
        let Some(memory) = self.retained_memory.as_mut() else {
            return Ok(());
        };
        let reservation = Arc::get_mut(&mut memory.0).ok_or_else(|| {
            DbError::internal("PL/pgSQL output memory hold was shared before finalization")
        })?;
        reservation.resize(bytes)
    }

    #[must_use]
    pub fn take_memory_hold(&mut self) -> Option<VmMemoryHold> {
        self.retained_memory.take()
    }
}

pub type VmSqlStream = Box<dyn Iterator<Item = Result<QueryEvent>>>;

#[derive(Debug, Clone, PartialEq)]
pub struct VmSqlRequest {
    pub sql: String,
    pub parameters: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VmRunState {
    Sql(VmSqlRequest),
    Complete(VmOutput),
}

pub struct VmMachine {
    state: Option<VmState>,
    pending_sql: Option<PendingSql>,
}

enum ControlFrame {
    If {
        pending_false: Option<usize>,
        end_jumps: Vec<usize>,
    },
    Case {
        operand: Option<String>,
        pending_false: Option<usize>,
        end_jumps: Vec<usize>,
        branch_started: bool,
    },
    Loop {
        label: Option<String>,
        start: usize,
        false_jump: Option<usize>,
        exits: Vec<usize>,
        continues: Vec<usize>,
        query_start: Option<usize>,
        integer_start: Option<usize>,
        foreach_start: Option<usize>,
    },
}

struct ExceptionCompileFrame {
    label: Option<String>,
    protected_start: usize,
    control_depth: usize,
    outer_local_names: BTreeMap<String, usize>,
    outer_cursor_names: BTreeMap<String, usize>,
    protected_end: Option<usize>,
    skip_handlers: Option<usize>,
    end_jumps: Vec<usize>,
    handler_indexes: Vec<usize>,
    exits: Vec<usize>,
    in_handlers: bool,
}

impl ExceptionCompileFrame {
    fn new(
        label: Option<String>,
        protected_start: usize,
        control_depth: usize,
        outer_local_names: BTreeMap<String, usize>,
        outer_cursor_names: BTreeMap<String, usize>,
    ) -> Self {
        Self {
            label,
            protected_start,
            control_depth,
            outer_local_names,
            outer_cursor_names,
            protected_end: None,
            skip_handlers: None,
            end_jumps: Vec::new(),
            handler_indexes: Vec::new(),
            exits: Vec::new(),
            in_handlers: false,
        }
    }
}

struct QueryLoopState {
    slot: usize,
    end: usize,
    events: Box<dyn Iterator<Item = Result<QueryEvent>>>,
    current_rows: VecDeque<ordadb_types::Row>,
    fields: Option<Vec<String>>,
    rows_seen: usize,
}

struct CursorState {
    events: Box<dyn Iterator<Item = Result<QueryEvent>>>,
    current_rows: VecDeque<ordadb_types::Row>,
    store: CursorPageStore,
    fields: Option<Vec<String>>,
    position: i64,
    exhausted: bool,
}

struct RuntimeQueryRow {
    fields: Vec<String>,
    row: ordadb_types::Row,
}

enum CursorPageStore {
    Memory {
        rows: Vec<ordadb_types::Row>,
        bytes: usize,
    },
    Spilled(CursorSpillStore),
}

struct CursorSpillStore {
    file: NamedTempFile,
    offsets: Vec<u64>,
}

#[derive(Debug, Clone)]
struct RuntimeRecord {
    declared_fields: Option<Vec<String>>,
    values: Option<Vec<(String, Value)>>,
}

impl RuntimeRecord {
    const fn unassigned() -> Self {
        Self {
            declared_fields: None,
            values: None,
        }
    }

    fn row_type(fields: Vec<String>) -> Result<Self> {
        validate_record_fields(&fields)?;
        Ok(Self {
            values: Some(
                fields
                    .iter()
                    .cloned()
                    .map(|field| (field, Value::Null))
                    .collect(),
            ),
            declared_fields: Some(fields),
        })
    }

    fn assign_row(&mut self, fields: &[String], row: ordadb_types::Row) -> Result<()> {
        validate_record_fields(fields)?;
        if fields.len() != row.values.len() {
            return Err(DbError::internal(
                "PL/pgSQL record row width does not match its schema",
            ));
        }
        if let Some(declared) = &self.declared_fields
            && (declared.len() != fields.len()
                || declared
                    .iter()
                    .zip(fields)
                    .any(|(left, right)| !left.eq_ignore_ascii_case(right)))
        {
            return Err(DbError::new(
                "42804",
                "query row is not assignable to the declared %ROWTYPE variable",
            ));
        }
        self.values = Some(fields.iter().cloned().zip(row.values).collect());
        Ok(())
    }

    fn clear_after_no_row(&mut self) {
        self.values = self.declared_fields.as_ref().map(|fields| {
            fields
                .iter()
                .cloned()
                .map(|field| (field, Value::Null))
                .collect()
        });
    }

    fn is_assigned(&self) -> bool {
        self.values.is_some()
    }

    fn value(&self, field: &str) -> Result<Value> {
        let values = self.values.as_ref().ok_or_else(|| {
            DbError::new("55000", "record is not assigned yet")
                .with_hint("assign a query row before reading a record field")
        })?;
        values
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(field))
            .map(|(_, value)| value.clone())
            .ok_or_else(|| DbError::new("42703", format!("record has no field {field}")))
    }

    fn assign_field(&mut self, field: &str, value: Value) -> Result<()> {
        let values = self.values.as_mut().ok_or_else(|| {
            DbError::new("55000", "record is not assigned yet")
                .with_hint("assign a query row before writing a record field")
        })?;
        let target = values
            .iter_mut()
            .find(|(name, _)| name.eq_ignore_ascii_case(field))
            .ok_or_else(|| DbError::new("42703", format!("record has no field {field}")))?;
        target.1 = value;
        Ok(())
    }

    fn estimated_bytes(&self) -> usize {
        let declared = self
            .declared_fields
            .as_ref()
            .map_or(0, |fields| fields.iter().map(String::len).sum());
        let values = self.values.as_ref().map_or(0, |values| {
            values
                .iter()
                .map(|(field, value)| {
                    field
                        .len()
                        .saturating_add(estimated_cursor_value_bytes(value))
                })
                .sum()
        });
        std::mem::size_of::<Self>()
            .saturating_add(declared)
            .saturating_add(values)
    }
}

fn validate_record_fields(fields: &[String]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for field in fields {
        if field.is_empty() || field.len() > 63 || field.contains('\0') {
            return Err(DbError::new(
                "22023",
                "record field name is empty or exceeds its bound",
            ));
        }
        if !seen.insert(field.to_ascii_lowercase()) {
            return Err(DbError::new(
                "42701",
                format!("record field {field} appears more than once"),
            ));
        }
    }
    Ok(())
}

fn initialize_runtime_records(
    program: &Program,
    host: &mut impl PlpgsqlHost,
    retained_byte_limit: usize,
) -> Result<BTreeMap<usize, RuntimeRecord>> {
    let mut records = BTreeMap::new();
    for (slot, local) in program.locals.iter().enumerate() {
        let record = match &local.kind {
            LocalKind::Scalar => continue,
            LocalKind::Record => RuntimeRecord::unassigned(),
            LocalKind::RowType(relation) => {
                RuntimeRecord::row_type(host.resolve_row_type(relation)?)?
            }
        };
        records.insert(slot, record);
    }
    ensure_runtime_record_limit(&records, None, retained_byte_limit)?;
    Ok(records)
}

fn evaluate_runtime_expression(
    host: &mut impl PlpgsqlHost,
    expression: &str,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
) -> Result<Value> {
    if let Some((slot, field)) = record_field_reference(expression)
        && let Some(record) = records.get(&slot)
    {
        return record.value(field);
    }
    if let Some(slot) = positional_parameter_index(expression)
        && records.contains_key(&slot)
    {
        return Err(DbError::new(
            "42804",
            "a record value cannot be used as an ordinary scalar expression",
        ));
    }
    let (expression, parameters) = expand_runtime_record_fields(expression, locals, records)?;
    host.evaluate_expression(&expression, &parameters)
}

fn assign_runtime_row(
    slot: usize,
    value: Option<RuntimeQueryRow>,
    locals: &mut [Value],
    records: &mut BTreeMap<usize, RuntimeRecord>,
    retained_byte_limit: usize,
) -> Result<()> {
    if let Some(record) = records.get(&slot) {
        let mut candidate = record.clone();
        if let Some(value) = value {
            candidate.assign_row(&value.fields, value.row)?;
        } else {
            candidate.clear_after_no_row();
        }
        ensure_runtime_record_limit(records, Some((slot, &candidate)), retained_byte_limit)?;
        records.insert(slot, candidate);
        return Ok(());
    }
    locals[slot] = value
        .and_then(|value| value.row.values.into_iter().next())
        .unwrap_or(Value::Null);
    Ok(())
}

fn ensure_runtime_record_limit(
    records: &BTreeMap<usize, RuntimeRecord>,
    candidate: Option<(usize, &RuntimeRecord)>,
    retained_byte_limit: usize,
) -> Result<()> {
    let mut retained = 0usize;
    for (slot, record) in records {
        let record = candidate
            .filter(|(candidate_slot, _)| candidate_slot == slot)
            .map_or(record, |(_, record)| record);
        retained = retained
            .checked_add(record.estimated_bytes())
            .ok_or_else(|| DbError::new("53200", "PL/pgSQL record memory accounting overflowed"))?;
        if retained > retained_byte_limit {
            return Err(DbError::new(
                "53200",
                "PL/pgSQL record retained-memory limit exceeded",
            ));
        }
    }
    Ok(())
}

fn record_field_reference(expression: &str) -> Option<(usize, &str)> {
    let (parameter, field) = expression.trim().strip_prefix('$')?.split_once('.')?;
    if field.is_empty()
        || !field
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '_')
    {
        return None;
    }
    Some((parameter.parse::<usize>().ok()?.checked_sub(1)?, field))
}

fn expand_runtime_record_fields(
    sql: &str,
    parameters: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
) -> Result<(String, Vec<Value>)> {
    if records.is_empty() {
        return Ok((sql.to_owned(), parameters.to_vec()));
    }
    let characters = sql.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(sql.len());
    let mut expanded = parameters.to_vec();
    let mut quote = None;
    let mut index = 0usize;
    while index < characters.len() {
        let character = characters[index];
        if let Some(delimiter) = quote {
            output.push(character);
            if character == delimiter {
                if characters.get(index + 1) == Some(&delimiter) {
                    output.push(delimiter);
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character);
            output.push(character);
            index += 1;
            continue;
        }
        if character != '$' {
            output.push(character);
            index += 1;
            continue;
        }
        let digits_start = index + 1;
        let mut cursor = digits_start;
        while characters.get(cursor).is_some_and(char::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == digits_start || characters.get(cursor) != Some(&'.') {
            output.push(character);
            index += 1;
            continue;
        }
        let parameter = characters[digits_start..cursor]
            .iter()
            .collect::<String>()
            .parse::<usize>()
            .map_err(|_| DbError::new("42P02", "invalid record parameter"))?
            .checked_sub(1)
            .ok_or_else(|| DbError::new("42P02", "record parameters are one-based"))?;
        let field_start = cursor + 1;
        cursor = field_start;
        while characters
            .get(cursor)
            .is_some_and(|value| value.is_ascii_alphanumeric() || *value == '_')
        {
            cursor += 1;
        }
        let Some(record) = records.get(&parameter) else {
            output.extend(characters[index..cursor].iter());
            index = cursor;
            continue;
        };
        if cursor == field_start {
            return Err(DbError::new(
                "42601",
                "record access requires an unquoted field name",
            ));
        }
        let field = characters[field_start..cursor].iter().collect::<String>();
        expanded.push(record.value(&field)?);
        output.push('$');
        output.push_str(&expanded.len().to_string());
        index = cursor;
    }
    Ok((output, expanded))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvaluatedCursorDirection {
    Next,
    Prior,
    First,
    Last,
    Absolute(i64),
    Relative(i64),
    Forward(i64),
    ForwardAll,
    Backward(i64),
    BackwardAll,
}

struct IntegerLoopState {
    slot: usize,
    current: i64,
    bound: i64,
    step: i64,
    reverse: bool,
    end: usize,
}

impl IntegerLoopState {
    fn advance(&mut self) -> Result<Option<Value>> {
        let next = if self.reverse {
            self.current.checked_sub(self.step)
        } else {
            self.current.checked_add(self.step)
        }
        .ok_or_else(|| DbError::new("22003", "PL/pgSQL integer FOR value is out of range"))?;
        let within_bound = if self.reverse {
            next >= self.bound
        } else {
            next <= self.bound
        };
        if within_bound {
            self.current = next;
            Ok(Some(Value::Int64(next)))
        } else {
            Ok(None)
        }
    }
}

struct ForeachLoopState {
    slot: usize,
    values: VecDeque<Value>,
    end: usize,
}

impl QueryLoopState {
    fn next_row(&mut self, max_rows: usize) -> Result<Option<RuntimeQueryRow>> {
        loop {
            if let Some(row) = self.current_rows.pop_front() {
                self.rows_seen = self.rows_seen.checked_add(1).ok_or_else(|| {
                    DbError::new("54001", "PL/pgSQL query FOR row count overflowed")
                })?;
                if self.rows_seen > max_rows {
                    return limit_error("PL/pgSQL query FOR row limit exceeded");
                }
                return Ok(Some(RuntimeQueryRow {
                    fields: self.fields.clone().unwrap_or_default(),
                    row,
                }));
            }
            let Some(event) = self.events.next() else {
                return Ok(None);
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
}
