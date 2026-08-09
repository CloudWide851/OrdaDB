//! Bounded PL/pgSQL compiler and explicit-frame virtual machine.

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

fn compile_with_arguments_and_limits(
    source: &str,
    argument_names: &[String],
    limits: ResourceLimits,
) -> Result<Program> {
    if source.len() > limits.max_source_bytes {
        return limit_error("PL/pgSQL source exceeds the configured byte limit");
    }
    let lines = logical_lines(source)?;
    let token_count = lines
        .iter()
        .map(|line| line.split_whitespace().count())
        .sum::<usize>();
    if token_count > limits.max_tokens {
        return limit_error("PL/pgSQL source exceeds the configured token limit");
    }

    let mut locals = Vec::with_capacity(argument_names.len());
    let mut local_names = BTreeMap::new();
    let mut cursor_declarations = Vec::new();
    let mut cursor_names = BTreeMap::new();
    for name in argument_names {
        let key = name.to_ascii_lowercase();
        if local_names.insert(key, locals.len()).is_some() {
            return Err(DbError::new(
                "42710",
                format!("PL/pgSQL argument {name} is declared more than once"),
            ));
        }
        locals.push(LocalSlot {
            name: name.clone(),
            constant: false,
            kind: LocalKind::Scalar,
        });
    }
    let mut instructions = Vec::new();
    let mut controls = Vec::new();
    let mut declaring = false;
    let mut exception_handlers = Vec::<ExceptionHandler>::new();
    let mut blocks = Vec::<ExceptionCompileFrame>::new();
    let mut sqlstate_slot = None;
    let mut sqlerrm_slot = None;
    let mut pending_label = None::<String>;
    let mut pending_block_label = None::<String>;
    let mut pending_scope = None::<(BTreeMap<String, usize>, BTreeMap<String, usize>)>;
    let mut declaration_scope = DeclarationScope::default();

    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let uppercase = trimmed.to_ascii_uppercase();
        if let Some(label) = parse_label(trimmed)? {
            if declaring || pending_block_label.is_some() || pending_label.replace(label).is_some()
            {
                return syntax_error("a PL/pgSQL label must be followed by one block or loop");
            }
            continue;
        }
        let starts_loop = ((uppercase.starts_with("WHILE ")
            || uppercase.starts_with("FOREACH ")
            || uppercase.starts_with("FOR "))
            && uppercase.ends_with(" LOOP"))
            || uppercase == "LOOP";
        if pending_label.is_some() && !starts_loop && uppercase != "DECLARE" && uppercase != "BEGIN"
        {
            return syntax_error("a PL/pgSQL label must be followed by one block or loop");
        }
        if uppercase == "DECLARE" {
            if blocks.last().is_some_and(|block| block.in_handlers) {
                return syntax_error("DECLARE is not valid inside an exception handler");
            }
            if declaring || pending_scope.is_some() {
                return syntax_error("DECLARE must be followed by one BEGIN block");
            }
            if blocks
                .last()
                .is_some_and(|block| controls.len() != block.control_depth)
            {
                return syntax_error("DECLARE is not valid inside an open control structure");
            }
            pending_scope = Some((local_names.clone(), cursor_names.clone()));
            pending_block_label = pending_label.take();
            declaration_scope.declared_names.clear();
            declaration_scope.allow_shadowing = !blocks.is_empty();
            declaring = true;
            continue;
        }
        if uppercase == "BEGIN" {
            let (outer_local_names, outer_cursor_names) = pending_scope
                .take()
                .unwrap_or_else(|| (local_names.clone(), cursor_names.clone()));
            declaring = false;
            declaration_scope.declared_names.clear();
            ensure_nesting(&blocks, limits)?;
            blocks.push(ExceptionCompileFrame::new(
                pending_block_label.take().or_else(|| pending_label.take()),
                instructions.len(),
                controls.len(),
                outer_local_names,
                outer_cursor_names,
            ));
            continue;
        }
        if uppercase == "EXCEPTION" {
            let block = blocks
                .last_mut()
                .ok_or_else(|| DbError::new("42601", "EXCEPTION has no matching BEGIN"))?;
            if block.in_handlers {
                return syntax_error("an exception block can contain one EXCEPTION section");
            }
            if controls.len() != block.control_depth {
                return syntax_error("EXCEPTION cannot begin inside an open control structure");
            }
            declaring = false;
            block.in_handlers = true;
            sqlstate_slot = Some(ensure_diagnostic_local(
                "sqlstate",
                &mut locals,
                &mut local_names,
            )?);
            sqlerrm_slot = Some(ensure_diagnostic_local(
                "sqlerrm",
                &mut locals,
                &mut local_names,
            )?);
            block.protected_end = Some(instructions.len());
            let skip = instructions.len();
            instructions.push(Instruction::Jump { target: usize::MAX });
            block.skip_handlers = Some(skip);
            continue;
        }
        if blocks
            .last()
            .is_some_and(|block| block.in_handlers && controls.len() == block.control_depth)
            && uppercase.starts_with("WHEN ")
            && uppercase.ends_with(" THEN")
        {
            let block = blocks
                .last_mut()
                .ok_or_else(|| DbError::internal("exception block stack is empty"))?;
            let protected_end = block
                .protected_end
                .ok_or_else(|| DbError::internal("exception region lost its protected end"))?;
            if !block.handler_indexes.is_empty() {
                let jump = instructions.len();
                instructions.push(Instruction::Jump { target: usize::MAX });
                block.end_jumps.push(jump);
            }
            let matcher_text = trimmed[5..trimmed.len() - 5].trim();
            let matcher = parse_exception_matcher(matcher_text)?;
            if block
                .handler_indexes
                .iter()
                .any(|index| exception_handlers[*index].matcher == ExceptionMatcher::Others)
            {
                return syntax_error("WHEN OTHERS must be the final exception handler");
            }
            let handler_index = exception_handlers.len();
            exception_handlers.push(ExceptionHandler {
                protected_start: block.protected_start,
                protected_end,
                matcher,
                target: instructions.len(),
            });
            block.handler_indexes.push(handler_index);
            ensure_instruction_limit(&instructions, limits)?;
            continue;
        }
        if let Some(closing_label) = parse_block_end_label(trimmed)? {
            let block = blocks
                .pop()
                .ok_or_else(|| DbError::new("42601", "END has no matching BEGIN"))?;
            if controls.len() != block.control_depth {
                return syntax_error("END closes a block with an open control structure");
            }
            if closing_label.is_some() && closing_label != block.label {
                return syntax_error("END label does not match its opening block label");
            }
            if block.in_handlers {
                if block.handler_indexes.is_empty() {
                    return syntax_error("EXCEPTION requires at least one WHEN handler");
                }
                let end = instructions.len();
                patch_target(&mut instructions, block.skip_handlers, end)?;
                for jump in block.end_jumps {
                    patch_target(&mut instructions, Some(jump), end)?;
                }
            }
            let end = instructions.len();
            for jump in block.exits {
                patch_target(&mut instructions, Some(jump), end)?;
            }
            local_names = block.outer_local_names;
            cursor_names = block.outer_cursor_names;
            continue;
        }
        if declaring {
            compile_declaration(
                trimmed,
                &mut locals,
                &mut local_names,
                &mut cursor_declarations,
                &mut cursor_names,
                &mut instructions,
                &mut declaration_scope,
            )?;
            ensure_instruction_limit(&instructions, limits)?;
            continue;
        }

        if uppercase.starts_with("IF ") && uppercase.ends_with(" THEN") {
            ensure_nesting(&controls, limits)?;
            let expression = trimmed[3..trimmed.len() - 5].trim();
            let expression = rewrite_locals(expression, &local_names);
            let jump = instructions.len();
            instructions.push(Instruction::JumpIfFalse {
                expression,
                target: usize::MAX,
            });
            controls.push(ControlFrame::If {
                pending_false: Some(jump),
                end_jumps: Vec::new(),
            });
        } else if uppercase.starts_with("ELSIF ") && uppercase.ends_with(" THEN") {
            let current = instructions.len();
            let frame = controls
                .last_mut()
                .ok_or_else(|| DbError::new("42601", "ELSIF has no matching IF"))?;
            let ControlFrame::If {
                pending_false,
                end_jumps,
            } = frame
            else {
                return syntax_error("ELSIF is only valid inside IF");
            };
            let end_jump = current;
            instructions.push(Instruction::Jump { target: usize::MAX });
            end_jumps.push(end_jump);
            let next_branch = instructions.len();
            patch_target(&mut instructions, pending_false.take(), next_branch)?;
            let expression = trimmed[6..trimmed.len() - 5].trim();
            let false_jump = instructions.len();
            instructions.push(Instruction::JumpIfFalse {
                expression: rewrite_locals(expression, &local_names),
                target: usize::MAX,
            });
            *pending_false = Some(false_jump);
        } else if uppercase == "ELSE" {
            let current = instructions.len();
            let frame = controls
                .last_mut()
                .ok_or_else(|| DbError::new("42601", "ELSE has no matching control structure"))?;
            let (pending_false, end_jumps) = match frame {
                ControlFrame::If {
                    pending_false,
                    end_jumps,
                } => (pending_false, end_jumps),
                ControlFrame::Case {
                    pending_false,
                    end_jumps,
                    branch_started,
                    ..
                } if *branch_started => (pending_false, end_jumps),
                ControlFrame::Case { .. } => {
                    return syntax_error("CASE ELSE requires a preceding WHEN");
                }
                ControlFrame::Loop { .. } => {
                    return syntax_error("ELSE is only valid inside IF or CASE");
                }
            };
            let end_jump = current;
            instructions.push(Instruction::Jump { target: usize::MAX });
            end_jumps.push(end_jump);
            let else_target = instructions.len();
            patch_target(&mut instructions, pending_false.take(), else_target)?;
        } else if uppercase == "END IF" {
            let end = instructions.len();
            let frame = controls
                .pop()
                .ok_or_else(|| DbError::new("42601", "END IF has no matching IF"))?;
            let ControlFrame::If {
                pending_false,
                end_jumps,
            } = frame
            else {
                return syntax_error("END IF closes a loop");
            };
            patch_target(&mut instructions, pending_false, end)?;
            for jump in end_jumps {
                patch_target(&mut instructions, Some(jump), end)?;
            }
        } else if uppercase == "CASE" || uppercase.starts_with("CASE ") {
            ensure_nesting(&controls, limits)?;
            let operand =
                (uppercase != "CASE").then(|| rewrite_locals(trimmed[5..].trim(), &local_names));
            controls.push(ControlFrame::Case {
                operand,
                pending_false: None,
                end_jumps: Vec::new(),
                branch_started: false,
            });
        } else if uppercase.starts_with("WHEN ") && uppercase.ends_with(" THEN") {
            let frame = controls
                .last_mut()
                .ok_or_else(|| DbError::new("42601", "WHEN has no matching CASE"))?;
            let ControlFrame::Case {
                operand,
                pending_false,
                end_jumps,
                branch_started,
            } = frame
            else {
                return syntax_error("WHEN is only valid inside CASE");
            };
            if *branch_started {
                let end_jump = instructions.len();
                instructions.push(Instruction::Jump { target: usize::MAX });
                end_jumps.push(end_jump);
                let next_branch = instructions.len();
                patch_target(&mut instructions, pending_false.take(), next_branch)?;
            }
            let branch = trimmed[5..trimmed.len() - 5].trim();
            let branch = rewrite_locals(branch, &local_names);
            let expression = operand.as_ref().map_or(branch.clone(), |operand| {
                format!("({operand}) = ({branch})")
            });
            let false_jump = instructions.len();
            instructions.push(Instruction::JumpIfFalse {
                expression,
                target: usize::MAX,
            });
            *pending_false = Some(false_jump);
            *branch_started = true;
        } else if uppercase == "END CASE" {
            let end = instructions.len();
            let frame = controls
                .pop()
                .ok_or_else(|| DbError::new("42601", "END CASE has no matching CASE"))?;
            let ControlFrame::Case {
                operand: _,
                pending_false,
                end_jumps,
                branch_started,
            } = frame
            else {
                return syntax_error("END CASE closes a non-CASE control structure");
            };
            if !branch_started {
                return syntax_error("CASE requires at least one WHEN branch");
            }
            patch_target(&mut instructions, pending_false, end)?;
            for jump in end_jumps {
                patch_target(&mut instructions, Some(jump), end)?;
            }
        } else if uppercase.starts_with("WHILE ") && uppercase.ends_with(" LOOP") {
            ensure_nesting(&controls, limits)?;
            let start = instructions.len();
            let expression = trimmed[6..trimmed.len() - 5].trim();
            instructions.push(Instruction::JumpIfFalse {
                expression: rewrite_locals(expression, &local_names),
                target: usize::MAX,
            });
            controls.push(ControlFrame::Loop {
                label: pending_label.take(),
                start,
                false_jump: Some(start),
                exits: Vec::new(),
                continues: Vec::new(),
                query_start: None,
                integer_start: None,
                foreach_start: None,
            });
        } else if uppercase.starts_with("FOREACH ") && uppercase.ends_with(" LOOP") {
            ensure_nesting(&controls, limits)?;
            let rest = trimmed[8..trimmed.len() - 5].trim();
            let (target, array) = split_keyword(rest, "IN ARRAY");
            let array = array.ok_or_else(|| {
                DbError::new(
                    "42601",
                    "FOREACH requires IN ARRAY followed by an expression",
                )
            })?;
            if target
                .split_whitespace()
                .any(|part| part.eq_ignore_ascii_case("SLICE"))
            {
                return unsupported_feature("FOREACH SLICE is not supported");
            }
            if target.split_whitespace().count() != 1 || array.trim().is_empty() {
                return syntax_error("FOREACH requires one target and one array expression");
            }
            let slot = *local_names
                .get(&target.to_ascii_lowercase())
                .ok_or_else(|| {
                    DbError::new("42703", format!("FOREACH variable {target} does not exist"))
                })?;
            let start = instructions.len();
            instructions.push(Instruction::ForeachStart {
                slot,
                array: rewrite_locals(array.trim(), &local_names),
                end: usize::MAX,
            });
            controls.push(ControlFrame::Loop {
                label: pending_label.take(),
                start,
                false_jump: None,
                exits: Vec::new(),
                continues: Vec::new(),
                query_start: None,
                integer_start: None,
                foreach_start: Some(start),
            });
        } else if uppercase.starts_with("FOR ") && uppercase.ends_with(" LOOP") {
            ensure_nesting(&controls, limits)?;
            let rest = trimmed[4..trimmed.len() - 5].trim();
            let (target, source) = split_keyword(rest, "IN");
            let source = source.ok_or_else(|| {
                DbError::new("42601", "FOR requires IN followed by a range or query")
            })?;
            let slot = *local_names
                .get(&target.trim().to_ascii_lowercase())
                .ok_or_else(|| {
                    DbError::new(
                        "42703",
                        format!("FOR variable {} does not exist", target.trim()),
                    )
                })?;
            let start = instructions.len();
            let (reverse, source) = strip_leading_keyword(source.trim(), "REVERSE")
                .map_or((false, source.trim()), |source| (true, source));
            let query_source = source
                .get(.."SELECT".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SELECT"));
            if query_source {
                if reverse {
                    return syntax_error("REVERSE requires an integer FOR range");
                }
                instructions.push(Instruction::QueryForStart {
                    slot,
                    sql: rewrite_locals(source, &local_names),
                    end: usize::MAX,
                });
                controls.push(ControlFrame::Loop {
                    label: pending_label.take(),
                    start,
                    false_jump: None,
                    exits: Vec::new(),
                    continues: Vec::new(),
                    query_start: Some(start),
                    integer_start: None,
                    foreach_start: None,
                });
            } else {
                let (range, step) = split_keyword(source, "BY");
                let (lower, upper) = range.split_once("..").ok_or_else(|| {
                    DbError::new(
                        "0A000",
                        "only query SELECT and integer FOR loops are supported",
                    )
                })?;
                if lower.trim().is_empty() || upper.trim().is_empty() || upper.contains("..") {
                    return syntax_error("integer FOR requires lower .. upper bounds");
                }
                instructions.push(Instruction::IntegerForStart {
                    slot,
                    lower: rewrite_locals(lower.trim(), &local_names),
                    upper: rewrite_locals(upper.trim(), &local_names),
                    step: rewrite_locals(step.unwrap_or("1").trim(), &local_names),
                    reverse,
                    end: usize::MAX,
                });
                controls.push(ControlFrame::Loop {
                    label: pending_label.take(),
                    start,
                    false_jump: None,
                    exits: Vec::new(),
                    continues: Vec::new(),
                    query_start: None,
                    integer_start: Some(start),
                    foreach_start: None,
                });
            }
        } else if uppercase == "LOOP" {
            ensure_nesting(&controls, limits)?;
            controls.push(ControlFrame::Loop {
                label: pending_label.take(),
                start: instructions.len(),
                false_jump: None,
                exits: Vec::new(),
                continues: Vec::new(),
                query_start: None,
                integer_start: None,
                foreach_start: None,
            });
        } else if uppercase == "EXIT"
            || uppercase.starts_with("EXIT ")
            || uppercase == "CONTINUE"
            || uppercase.starts_with("CONTINUE ")
        {
            let is_exit = uppercase == "EXIT" || uppercase.starts_with("EXIT ");
            let keyword = if is_exit { "EXIT" } else { "CONTINUE" };
            let (label, condition) = parse_loop_control(trimmed, keyword)?;
            if let Some(condition) = condition {
                let condition = condition.trim();
                if condition.is_empty() {
                    return syntax_error(format!("{keyword} WHEN requires a condition"));
                }
                let skip = instructions.len();
                instructions.push(Instruction::JumpIfFalse {
                    expression: rewrite_locals(condition, &local_names),
                    target: skip + 2,
                });
            }
            let matching_loop = label.as_deref().is_some_and(|label| {
                controls.iter().rev().any(|frame| {
                    matches!(
                        frame,
                        ControlFrame::Loop {
                            label: Some(frame_label),
                            ..
                        } if frame_label == label
                    )
                })
            });
            if is_exit && label.is_some() && !matching_loop {
                let label = label.as_deref().unwrap_or_default();
                let block = blocks
                    .iter_mut()
                    .rev()
                    .find(|block| block.label.as_deref() == Some(label))
                    .ok_or_else(|| {
                        DbError::new("42601", format!("label {label} does not exist"))
                    })?;
                let jump = instructions.len();
                instructions.push(Instruction::Jump { target: usize::MAX });
                block.exits.push(jump);
                ensure_instruction_limit(&instructions, limits)?;
                continue;
            }
            let frame = loop_control_target_mut(&mut controls, label.as_deref())?;
            let ControlFrame::Loop {
                start,
                exits,
                continues,
                query_start,
                integer_start,
                foreach_start,
                ..
            } = frame
            else {
                return Err(DbError::internal("nearest loop is not a loop frame"));
            };
            let jump = instructions.len();
            if is_exit {
                instructions.push(Instruction::Jump { target: usize::MAX });
                exits.push(jump);
            } else {
                let deferred =
                    query_start.is_some() || integer_start.is_some() || foreach_start.is_some();
                instructions.push(Instruction::Jump {
                    target: if deferred { usize::MAX } else { *start },
                });
                if deferred {
                    continues.push(jump);
                }
            }
        } else if uppercase == "END LOOP" || uppercase.starts_with("END LOOP ") {
            let closing_label = trimmed
                .get("END LOOP".len()..)
                .map(str::trim)
                .filter(|label| !label.is_empty())
                .map(normalize_label)
                .transpose()?;
            let frame = controls
                .pop()
                .ok_or_else(|| DbError::new("42601", "END LOOP has no matching LOOP"))?;
            let ControlFrame::Loop {
                label,
                start,
                false_jump,
                exits,
                continues,
                query_start,
                integer_start,
                foreach_start,
            } = frame
            else {
                return syntax_error("END LOOP closes a non-loop control structure");
            };
            if closing_label.is_some() && closing_label != label {
                return syntax_error("END LOOP label does not match its opening loop label");
            }
            let continue_target = instructions.len();
            match (query_start, integer_start, foreach_start) {
                (Some(query_start), None, None) => {
                    instructions.push(Instruction::QueryForNext {
                        start: query_start,
                        body: query_start + 1,
                    });
                }
                (None, Some(integer_start), None) => {
                    instructions.push(Instruction::IntegerForNext {
                        start: integer_start,
                        body: integer_start + 1,
                    });
                }
                (None, None, Some(foreach_start)) => {
                    instructions.push(Instruction::ForeachNext {
                        start: foreach_start,
                        body: foreach_start + 1,
                    });
                }
                (None, None, None) => instructions.push(Instruction::Jump { target: start }),
                _ => {
                    return Err(DbError::internal(
                        "loop has multiple iterator advance states",
                    ));
                }
            }
            let end = instructions.len();
            patch_target(&mut instructions, false_jump, end)?;
            if query_start.is_some() {
                patch_query_for_end(&mut instructions, start, end)?;
            }
            if integer_start.is_some() {
                patch_integer_for_end(&mut instructions, start, end)?;
            }
            if foreach_start.is_some() {
                patch_foreach_end(&mut instructions, start, end)?;
            }
            for jump in exits {
                patch_target(&mut instructions, Some(jump), end)?;
            }
            for jump in continues {
                patch_target(&mut instructions, Some(jump), continue_target)?;
            }
        } else {
            if blocks
                .last()
                .is_some_and(|block| block.in_handlers && block.handler_indexes.is_empty())
            {
                return syntax_error("EXCEPTION requires WHEN before handler statements");
            }
            compile_statement(
                trimmed,
                &local_names,
                &cursor_names,
                &cursor_declarations,
                &mut instructions,
            )?;
        }
        ensure_instruction_limit(&instructions, limits)?;
    }

    if !controls.is_empty() {
        return syntax_error("PL/pgSQL block has an unclosed control structure");
    }
    if pending_label.is_some() || pending_block_label.is_some() {
        return syntax_error("a PL/pgSQL label must be followed by one block or loop");
    }
    if declaring || pending_scope.is_some() {
        return syntax_error("DECLARE must be followed by one BEGIN block");
    }
    if !blocks.is_empty() {
        return syntax_error("PL/pgSQL block has an unmatched BEGIN");
    }
    instructions.push(Instruction::Checkpoint);
    ensure_instruction_limit(&instructions, limits)?;
    Ok(Program {
        version: BYTECODE_VERSION,
        instructions,
        locals,
        cursor_declarations,
        exception_handlers,
        sqlstate_slot,
        sqlerrm_slot,
    })
}

fn ensure_diagnostic_local(
    name: &str,
    locals: &mut Vec<LocalSlot>,
    names: &mut BTreeMap<String, usize>,
) -> Result<usize> {
    if let Some(slot) = names.get(name) {
        return Ok(*slot);
    }
    let slot = locals.len();
    names.insert(name.to_owned(), slot);
    locals.push(LocalSlot {
        name: name.to_owned(),
        constant: false,
        kind: LocalKind::Scalar,
    });
    Ok(slot)
}

#[derive(Debug, Default)]
struct DeclarationScope {
    declared_names: BTreeSet<String>,
    allow_shadowing: bool,
}

fn compile_cursor_declaration(
    declaration: &str,
    locals: &mut Vec<LocalSlot>,
    names: &mut BTreeMap<String, usize>,
    cursor_declarations: &mut Vec<CursorDeclaration>,
    cursor_names: &mut BTreeMap<String, usize>,
    scope: &mut DeclarationScope,
) -> Result<bool> {
    let (cursor_head, bound_query) = split_keyword(declaration, "CURSOR FOR");
    let is_refcursor = declaration
        .split_whitespace()
        .any(|part| part.eq_ignore_ascii_case("REFCURSOR"));
    if bound_query.is_none() && !is_refcursor {
        return Ok(false);
    }
    if declaration.contains(":=") {
        return unsupported_feature("cursor declaration initializers are not supported");
    }
    let mut parts = cursor_head.split_whitespace();
    let name = parts
        .next()
        .ok_or_else(|| DbError::new("42601", "cursor declaration requires a name"))?;
    if name.contains(['(', ')']) {
        return unsupported_feature("cursor declaration arguments are not supported");
    }
    let modifiers = parts.collect::<Vec<_>>();
    if let Some(query) = bound_query {
        let modifiers_valid = modifiers.is_empty()
            || (modifiers.len() == 1 && modifiers[0].eq_ignore_ascii_case("SCROLL"))
            || (modifiers.len() == 2
                && modifiers[0].eq_ignore_ascii_case("NO")
                && modifiers[1].eq_ignore_ascii_case("SCROLL"));
        if !modifiers_valid {
            return syntax_error("bound cursor declaration accepts only SCROLL or NO SCROLL");
        }
        if query.trim().is_empty() {
            return syntax_error("bound cursor declaration requires a query");
        }
    } else if modifiers.len() != 1 || !modifiers[0].eq_ignore_ascii_case("REFCURSOR") {
        return syntax_error("unbound cursor declaration must use REFCURSOR");
    }
    let key = name.to_ascii_lowercase();
    if !scope.declared_names.insert(key.clone())
        || (!scope.allow_shadowing && (names.contains_key(&key) || cursor_names.contains_key(&key)))
    {
        return Err(DbError::new(
            "42710",
            format!("PL/pgSQL variable {name} is declared more than once"),
        ));
    }
    let slot = locals.len();
    names.insert(key.clone(), slot);
    locals.push(LocalSlot {
        name: name.to_owned(),
        constant: false,
        kind: LocalKind::Scalar,
    });
    let cursor = cursor_declarations.len();
    cursor_names.insert(key, cursor);
    cursor_declarations.push(CursorDeclaration {
        name: name.to_owned(),
        bound_query: bound_query.map(|query| rewrite_locals(query.trim(), names)),
    });
    Ok(true)
}

fn parse_open_cursor(
    statement: &str,
    locals: &BTreeMap<String, usize>,
    cursor_names: &BTreeMap<String, usize>,
    cursor_declarations: &[CursorDeclaration],
) -> Result<Instruction> {
    let rest = statement["OPEN".len()..].trim();
    let (name, tail) = rest
        .split_once(char::is_whitespace)
        .map_or((rest, ""), |(name, tail)| (name, tail.trim_start()));
    let cursor = lookup_cursor(name, cursor_names)?;
    let declaration = cursor_declarations
        .get(cursor)
        .ok_or_else(|| DbError::internal("cursor declaration index is invalid"))?;
    let query = if tail.is_empty() {
        if declaration.bound_query.is_none() {
            return syntax_error(format!("OPEN {name} requires FOR followed by a query"));
        }
        CursorQuery::Bound
    } else {
        let body = strip_leading_keyword(tail, "FOR")
            .ok_or_else(|| DbError::new("42601", "OPEN cursor syntax requires FOR"))?;
        if declaration.bound_query.is_some() {
            return syntax_error("bound cursor OPEN must not specify another query");
        }
        if let Some(dynamic) = strip_leading_keyword(body, "EXECUTE") {
            let (query, using) = split_keyword(dynamic, "USING");
            if query.trim().is_empty() {
                return syntax_error("OPEN FOR EXECUTE requires a query expression");
            }
            let using = using
                .map(|values| -> Result<Vec<String>> {
                    Ok(
                        split_top_level_expressions(values, "OPEN FOR EXECUTE USING")?
                            .into_iter()
                            .map(|value| rewrite_locals(value.trim(), locals))
                            .collect(),
                    )
                })
                .transpose()?
                .unwrap_or_default();
            CursorQuery::Dynamic {
                query: rewrite_locals(query.trim(), locals),
                using,
            }
        } else {
            if body.trim().is_empty() {
                return syntax_error("OPEN FOR requires a query");
            }
            CursorQuery::Static(rewrite_locals(body.trim(), locals))
        }
    };
    Ok(Instruction::OpenCursor { cursor, query })
}

fn parse_fetch_cursor(
    statement: &str,
    locals: &BTreeMap<String, usize>,
    cursor_names: &BTreeMap<String, usize>,
) -> Result<Instruction> {
    let rest = statement["FETCH".len()..].trim();
    let (cursor_clause, into) = split_keyword(rest, "INTO");
    let into = into.ok_or_else(|| DbError::new("42601", "FETCH requires INTO"))?;
    if into.split_whitespace().count() != 1 {
        return unsupported_feature("FETCH INTO multiple targets is not supported");
    }
    let target = *locals
        .get(&into.to_ascii_lowercase())
        .ok_or_else(|| DbError::new("42703", format!("FETCH target {into} does not exist")))?;
    let (cursor, direction) = parse_cursor_reference(cursor_clause, locals, cursor_names)?;
    Ok(Instruction::FetchCursor {
        cursor,
        direction,
        into: target,
    })
}

fn parse_move_cursor(
    statement: &str,
    locals: &BTreeMap<String, usize>,
    cursor_names: &BTreeMap<String, usize>,
) -> Result<Instruction> {
    let rest = statement["MOVE".len()..].trim();
    let (cursor, direction) = parse_cursor_reference(rest, locals, cursor_names)?;
    Ok(Instruction::MoveCursor { cursor, direction })
}

fn parse_close_cursor(
    statement: &str,
    cursor_names: &BTreeMap<String, usize>,
) -> Result<Instruction> {
    let name = statement["CLOSE".len()..].trim();
    if name.split_whitespace().count() != 1 {
        return syntax_error("CLOSE requires one cursor name");
    }
    Ok(Instruction::CloseCursor {
        cursor: lookup_cursor(name, cursor_names)?,
    })
}

fn parse_cursor_reference(
    value: &str,
    locals: &BTreeMap<String, usize>,
    cursor_names: &BTreeMap<String, usize>,
) -> Result<(usize, CursorDirection)> {
    let (direction, cursor_name) = split_keyword(value, "FROM");
    let (direction, cursor_name) = if let Some(cursor_name) = cursor_name {
        (direction, cursor_name)
    } else {
        let (direction, cursor_name) = split_keyword(value, "IN");
        cursor_name.map_or(("", direction), |cursor_name| (direction, cursor_name))
    };
    if cursor_name.split_whitespace().count() != 1 {
        return syntax_error("cursor reference requires one cursor name");
    }
    Ok((
        lookup_cursor(cursor_name, cursor_names)?,
        parse_cursor_direction(direction, locals)?,
    ))
}

fn parse_cursor_direction(
    value: &str,
    locals: &BTreeMap<String, usize>,
) -> Result<CursorDirection> {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("NEXT") {
        return Ok(CursorDirection::Next);
    }
    if value.eq_ignore_ascii_case("PRIOR") {
        return Ok(CursorDirection::Prior);
    }
    if value.eq_ignore_ascii_case("FIRST") {
        return Ok(CursorDirection::First);
    }
    if value.eq_ignore_ascii_case("LAST") {
        return Ok(CursorDirection::Last);
    }
    let (kind, amount) = value
        .split_once(char::is_whitespace)
        .map_or((value, ""), |(kind, amount)| (kind, amount.trim()));
    if kind.eq_ignore_ascii_case("ABSOLUTE") {
        return cursor_direction_expression(amount, locals, "ABSOLUTE")
            .map(CursorDirection::Absolute);
    }
    if kind.eq_ignore_ascii_case("RELATIVE") {
        return cursor_direction_expression(amount, locals, "RELATIVE")
            .map(CursorDirection::Relative);
    }
    if kind.eq_ignore_ascii_case("FORWARD") {
        if amount.eq_ignore_ascii_case("ALL") {
            return Ok(CursorDirection::ForwardAll);
        }
        return Ok(CursorDirection::Forward(
            (!amount.is_empty()).then(|| rewrite_locals(amount, locals)),
        ));
    }
    if kind.eq_ignore_ascii_case("BACKWARD") {
        if amount.eq_ignore_ascii_case("ALL") {
            return Ok(CursorDirection::BackwardAll);
        }
        return Ok(CursorDirection::Backward(
            (!amount.is_empty()).then(|| rewrite_locals(amount, locals)),
        ));
    }
    syntax_error(format!("unsupported cursor direction {value}"))
}

fn cursor_direction_expression(
    value: &str,
    locals: &BTreeMap<String, usize>,
    direction: &str,
) -> Result<String> {
    if value.is_empty() {
        return syntax_error(format!("{direction} requires a position"));
    }
    Ok(rewrite_locals(value, locals))
}

fn lookup_cursor(name: &str, cursor_names: &BTreeMap<String, usize>) -> Result<usize> {
    cursor_names
        .get(&name.to_ascii_lowercase())
        .copied()
        .ok_or_else(|| DbError::new("34000", format!("cursor {name} does not exist")))
}

fn compile_declaration(
    declaration: &str,
    locals: &mut Vec<LocalSlot>,
    names: &mut BTreeMap<String, usize>,
    cursor_declarations: &mut Vec<CursorDeclaration>,
    cursor_names: &mut BTreeMap<String, usize>,
    instructions: &mut Vec<Instruction>,
    scope: &mut DeclarationScope,
) -> Result<()> {
    if compile_cursor_declaration(
        declaration,
        locals,
        names,
        cursor_declarations,
        cursor_names,
        scope,
    )? {
        return Ok(());
    }
    let (head, initializer) = declaration
        .split_once(":=")
        .map_or((declaration, None), |(head, value)| {
            (head, Some(value.trim()))
        });
    let mut parts = head.split_whitespace();
    let name = parts
        .next()
        .ok_or_else(|| DbError::new("42601", "variable declaration requires a name"))?;
    let declaration_parts = parts.collect::<Vec<_>>();
    let constant = declaration_parts
        .iter()
        .any(|part| part.eq_ignore_ascii_case("CONSTANT"));
    let kind = declaration_parts
        .iter()
        .find_map(|part| {
            if part.eq_ignore_ascii_case("RECORD") {
                Some(LocalKind::Record)
            } else {
                part.to_ascii_uppercase()
                    .strip_suffix("%ROWTYPE")
                    .map(|_| LocalKind::RowType(part[..part.len() - "%ROWTYPE".len()].to_owned()))
            }
        })
        .unwrap_or(LocalKind::Scalar);
    let key = name.to_ascii_lowercase();
    if !scope.declared_names.insert(key.clone())
        || (!scope.allow_shadowing && (names.contains_key(&key) || cursor_names.contains_key(&key)))
    {
        return Err(DbError::new(
            "42710",
            format!("PL/pgSQL variable {name} is declared more than once"),
        ));
    }
    let slot = locals.len();
    names.insert(key, slot);
    locals.push(LocalSlot {
        name: name.to_owned(),
        constant,
        kind,
    });
    if let Some(initializer) = initializer {
        instructions.push(Instruction::Assign {
            slot,
            expression: rewrite_locals(initializer, names),
        });
    }
    Ok(())
}

fn compile_statement(
    statement: &str,
    locals: &BTreeMap<String, usize>,
    cursor_names: &BTreeMap<String, usize>,
    cursor_declarations: &[CursorDeclaration],
    instructions: &mut Vec<Instruction>,
) -> Result<()> {
    let uppercase = statement.to_ascii_uppercase();
    if uppercase.starts_with("OPEN ") {
        instructions.push(parse_open_cursor(
            statement,
            locals,
            cursor_names,
            cursor_declarations,
        )?);
    } else if uppercase.starts_with("FETCH ") {
        instructions.push(parse_fetch_cursor(statement, locals, cursor_names)?);
    } else if uppercase.starts_with("MOVE ") {
        instructions.push(parse_move_cursor(statement, locals, cursor_names)?);
    } else if uppercase.starts_with("CLOSE ") {
        instructions.push(parse_close_cursor(statement, cursor_names)?);
    } else if uppercase == "RAISE" || uppercase.starts_with("RAISE ") {
        instructions.push(parse_raise_instruction(statement, locals)?);
    } else if uppercase.starts_with("ASSERT ") {
        let body = statement[7..].trim();
        let (condition, message) = split_top_level_comma(body)?;
        if condition.trim().is_empty() {
            return syntax_error("ASSERT requires a condition");
        }
        instructions.push(Instruction::Assert {
            condition: rewrite_locals(condition.trim(), locals),
            message: message.map(|message| rewrite_locals(message.trim(), locals)),
        });
    } else if uppercase.starts_with("RETURN NEXT ") {
        instructions.push(Instruction::Return {
            expression: Some(rewrite_locals(statement[12..].trim(), locals)),
            next: true,
        });
    } else if uppercase == "RETURN" {
        instructions.push(Instruction::Return {
            expression: None,
            next: false,
        });
    } else if uppercase.starts_with("RETURN ") {
        instructions.push(Instruction::Return {
            expression: Some(rewrite_locals(statement[7..].trim(), locals)),
            next: false,
        });
    } else if uppercase.starts_with("PERFORM ") {
        instructions.push(Instruction::ExecuteSql {
            sql: format!("SELECT {}", rewrite_locals(statement[8..].trim(), locals)),
            into: None,
        });
    } else if uppercase.starts_with("EXECUTE ") {
        let rest = statement[8..].trim();
        let (head, using) = split_keyword(rest, "USING");
        let (query, into) = split_keyword(head, "INTO");
        if query.trim().is_empty() {
            return syntax_error("dynamic EXECUTE requires a query expression");
        }
        let (into, strict) = parse_dynamic_into(into, locals)?;
        let using = using
            .map(|values| -> Result<Vec<String>> {
                Ok(split_top_level_expressions(values, "EXECUTE USING")?
                    .into_iter()
                    .map(|value| rewrite_locals(value.trim(), locals))
                    .collect::<Vec<_>>())
            })
            .transpose()?
            .unwrap_or_default();
        instructions.push(Instruction::DynamicExecute {
            query: rewrite_locals(query.trim(), locals),
            using,
            into,
            strict,
        });
    } else if let Some((name, expression)) = statement.split_once(":=") {
        let target = name.trim();
        if let Some((record, field)) = target.split_once('.') {
            if field.is_empty() || field.contains('.') {
                return syntax_error("composite assignment requires one field name");
            }
            let slot = *locals
                .get(&record.trim().to_ascii_lowercase())
                .ok_or_else(|| {
                    DbError::new("42703", format!("variable {record} does not exist"))
                })?;
            instructions.push(Instruction::AssignField {
                slot,
                field: field.trim().to_owned(),
                expression: rewrite_locals(expression.trim(), locals),
            });
        } else {
            let key = target.to_ascii_lowercase();
            let slot = *locals
                .get(&key)
                .ok_or_else(|| DbError::new("42703", format!("variable {name} does not exist")))?;
            instructions.push(Instruction::Assign {
                slot,
                expression: rewrite_locals(expression.trim(), locals),
            });
        }
    } else {
        let (sql, into) = extract_select_into(statement, locals)?;
        instructions.push(Instruction::ExecuteSql {
            sql: rewrite_locals(&sql, locals),
            into,
        });
    }
    Ok(())
}

fn parse_dynamic_into(
    into: Option<&str>,
    locals: &BTreeMap<String, usize>,
) -> Result<(Option<usize>, bool)> {
    let Some(into) = into else {
        return Ok((None, false));
    };
    if into.contains(',') {
        return unsupported_feature("dynamic EXECUTE INTO multiple targets is not supported");
    }
    let mut parts = into.split_whitespace();
    let first = parts
        .next()
        .ok_or_else(|| DbError::new("42601", "dynamic EXECUTE INTO requires a target"))?;
    let (strict, target) = if first.eq_ignore_ascii_case("STRICT") {
        (
            true,
            parts.next().ok_or_else(|| {
                DbError::new("42601", "dynamic EXECUTE INTO STRICT requires a target")
            })?,
        )
    } else {
        (false, first)
    };
    if parts.next().is_some() {
        return syntax_error("dynamic EXECUTE INTO accepts one target variable");
    }
    let slot = locals
        .get(&target.to_ascii_lowercase())
        .copied()
        .ok_or_else(|| {
            DbError::new(
                "42703",
                format!("dynamic EXECUTE INTO variable {target} does not exist"),
            )
        })?;
    Ok((Some(slot), strict))
}

fn split_top_level_expressions<'a>(value: &'a str, context: &str) -> Result<Vec<&'a str>> {
    let mut expressions = Vec::new();
    let mut quote = None;
    let mut depth = 0_usize;
    let mut start = 0_usize;
    for (index, character) in value.char_indices() {
        match quote {
            Some(delimiter) if character == delimiter => quote = None,
            Some(_) => {}
            None if matches!(character, '\'' | '"') => quote = Some(character),
            None if character == '(' => depth = depth.saturating_add(1),
            None if character == ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    DbError::new(
                        "42601",
                        format!("{context} has an unmatched closing parenthesis"),
                    )
                })?;
            }
            None if character == ',' && depth == 0 => {
                let expression = value[start..index].trim();
                if expression.is_empty() {
                    return syntax_error(format!("{context} contains an empty expression"));
                }
                expressions.push(expression);
                start = index + 1;
            }
            None => {}
        }
    }
    if quote.is_some() || depth != 0 {
        return syntax_error(format!("{context} expressions are not balanced"));
    }
    let expression = value[start..].trim();
    if expression.is_empty() {
        return syntax_error(format!("{context} requires at least one expression"));
    }
    expressions.push(expression);
    Ok(expressions)
}

fn parse_raise_instruction(
    statement: &str,
    locals: &BTreeMap<String, usize>,
) -> Result<Instruction> {
    let rest = statement["RAISE".len()..].trim();
    if rest.is_empty() {
        return Ok(Instruction::Raise {
            level: RaiseLevel::Exception,
            message: None,
            sql_state: None,
        });
    }
    let (first, tail) = rest
        .split_once(char::is_whitespace)
        .map_or((rest, ""), |(first, tail)| (first, tail.trim_start()));
    let (level, body) = if first.eq_ignore_ascii_case("INFO") {
        (RaiseLevel::Info, tail)
    } else if first.eq_ignore_ascii_case("NOTICE") {
        (RaiseLevel::Notice, tail)
    } else if first.eq_ignore_ascii_case("WARNING") {
        (RaiseLevel::Warning, tail)
    } else if first.eq_ignore_ascii_case("EXCEPTION") {
        (RaiseLevel::Exception, tail)
    } else {
        (RaiseLevel::Exception, rest)
    };
    let (message, options) = split_keyword(body, "USING");
    if message.trim().is_empty() {
        return unsupported_feature(
            "RAISE USING MESSAGE without a message expression is not supported",
        );
    }
    let sql_state = options.map(parse_raise_options).transpose()?.flatten();
    Ok(Instruction::Raise {
        level,
        message: Some(rewrite_locals(message.trim(), locals)),
        sql_state,
    })
}

fn parse_raise_options(options: &str) -> Result<Option<String>> {
    let mut sql_state = None;
    for option in options.split(',') {
        let (name, value) = option
            .split_once('=')
            .ok_or_else(|| DbError::new("42601", "RAISE USING options require name = value"))?;
        if !name.trim().eq_ignore_ascii_case("ERRCODE") {
            return unsupported_feature(format!("RAISE USING {} is not supported", name.trim()));
        }
        if sql_state.is_some() {
            return syntax_error("RAISE specifies ERRCODE more than once");
        }
        let value = value
            .trim()
            .strip_prefix('\'')
            .and_then(|value| value.strip_suffix('\''))
            .ok_or_else(|| DbError::new("42601", "RAISE ERRCODE must be a string literal"))?;
        validate_sql_state(value)?;
        sql_state = Some(value.to_ascii_uppercase());
    }
    Ok(sql_state)
}

fn split_top_level_comma(value: &str) -> Result<(&str, Option<&str>)> {
    let mut quote = None;
    let mut depth = 0_usize;
    for (index, character) in value.char_indices() {
        match quote {
            Some(delimiter) if character == delimiter => quote = None,
            Some(_) => {}
            None if matches!(character, '\'' | '"') => quote = Some(character),
            None if character == '(' => depth = depth.saturating_add(1),
            None if character == ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    DbError::new("42601", "ASSERT has an unmatched closing parenthesis")
                })?;
            }
            None if character == ',' && depth == 0 => {
                return Ok((&value[..index], Some(&value[index + 1..])));
            }
            None => {}
        }
    }
    if quote.is_some() || depth != 0 {
        return syntax_error("ASSERT condition or message is not balanced");
    }
    Ok((value, None))
}

fn extract_select_into(
    statement: &str,
    locals: &BTreeMap<String, usize>,
) -> Result<(String, Option<usize>)> {
    if !statement
        .get(.."SELECT ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SELECT "))
    {
        return Ok((statement.to_owned(), None));
    }
    let uppercase = statement.to_ascii_uppercase();
    let Some(into_start) = uppercase.find(" INTO ") else {
        return Ok((statement.to_owned(), None));
    };
    let tail = &statement[into_start + 6..];
    let variable_end = tail.find(char::is_whitespace).unwrap_or(tail.len());
    let variable = tail[..variable_end].trim();
    let slot = *locals.get(&variable.to_ascii_lowercase()).ok_or_else(|| {
        DbError::new(
            "42703",
            format!("SELECT INTO variable {variable} does not exist"),
        )
    })?;
    let before = statement[..into_start].trim_end();
    let after = tail[variable_end..].trim_start();
    let sql = if after.is_empty() {
        before.to_owned()
    } else {
        format!("{before} {after}")
    };
    Ok((sql, Some(slot)))
}

struct VmState {
    program: Program,
    limits: ResourceLimits,
    locals: Vec<Value>,
    records: BTreeMap<usize, RuntimeRecord>,
    instruction_pointer: usize,
    steps: usize,
    returned_rows: Vec<Value>,
    query_loops: BTreeMap<usize, QueryLoopState>,
    integer_loops: BTreeMap<usize, IntegerLoopState>,
    foreach_loops: BTreeMap<usize, ForeachLoopState>,
    cursors: BTreeMap<usize, CursorState>,
    active_exception: Option<DbError>,
    exception_regions: Vec<(usize, usize)>,
    active_exception_regions: Vec<(usize, usize)>,
    memory_reservation: VmMemoryReservation,
}

enum PendingSql {
    Execute {
        into: Option<usize>,
    },
    Dynamic {
        into: Option<usize>,
        strict: bool,
    },
    OpenCursor {
        cursor: usize,
    },
    QueryForStart {
        start: usize,
        slot: usize,
        end: usize,
    },
}

pub fn execute(
    program: &Program,
    host: &mut impl PlpgsqlHost,
    arguments: &[Value],
) -> Result<VmOutput> {
    execute_with_limits(program, host, arguments, ResourceLimits::default())
}

pub fn execute_with_limits(
    program: &Program,
    host: &mut impl PlpgsqlHost,
    arguments: &[Value],
    limits: ResourceLimits,
) -> Result<VmOutput> {
    let memory = VmMemoryGrant::new(limits.max_cursor_bytes)?;
    execute_with_memory_grant(program, host, arguments, limits, memory)
}

pub fn execute_with_memory_grant(
    program: &Program,
    host: &mut impl PlpgsqlHost,
    arguments: &[Value],
    limits: ResourceLimits,
    memory: VmMemoryGrant,
) -> Result<VmOutput> {
    let mut machine = VmMachine::new_with_memory_grant(program, host, arguments, limits, memory)?;
    let mut response = None;
    loop {
        match machine.resume(host, response.take())? {
            VmRunState::Sql(request) => {
                response = Some(host.execute_sql(&request.sql, &request.parameters));
            }
            VmRunState::Complete(output) => return Ok(output),
        }
    }
}

impl VmMachine {
    pub fn new(
        program: &Program,
        host: &mut impl PlpgsqlHost,
        arguments: &[Value],
        limits: ResourceLimits,
    ) -> Result<Self> {
        let memory = VmMemoryGrant::new(limits.max_cursor_bytes)?;
        Self::new_with_memory_grant(program, host, arguments, limits, memory)
    }

    pub fn new_with_memory_grant(
        program: &Program,
        host: &mut impl PlpgsqlHost,
        arguments: &[Value],
        limits: ResourceLimits,
        memory: VmMemoryGrant,
    ) -> Result<Self> {
        if program.version != BYTECODE_VERSION {
            return Err(DbError::new(
                "0A000",
                format!("unsupported PL/pgSQL bytecode version {}", program.version),
            ));
        }
        let mut locals = vec![Value::Null; program.locals.len()];
        for (slot, value) in locals.iter_mut().zip(arguments) {
            *slot = value.clone();
        }
        let records = initialize_runtime_records(program, host, limits.max_cursor_bytes)?;
        let mut exception_regions = program
            .exception_handlers
            .iter()
            .map(|handler| (handler.protected_start, handler.protected_end))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        exception_regions
            .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| right.1.cmp(&left.1)));
        let memory_reservation = memory.try_reserve(0)?;
        let mut state = VmState {
            program: program.clone(),
            limits,
            locals,
            records,
            instruction_pointer: 0,
            steps: 0,
            returned_rows: Vec::new(),
            query_loops: BTreeMap::new(),
            integer_loops: BTreeMap::new(),
            foreach_loops: BTreeMap::new(),
            cursors: BTreeMap::new(),
            active_exception: None,
            exception_regions,
            active_exception_regions: Vec::new(),
            memory_reservation,
        };
        state.refresh_memory(None)?;
        Ok(Self {
            state: Some(state),
            pending_sql: None,
        })
    }

    pub fn resume(
        &mut self,
        host: &mut impl PlpgsqlHost,
        response: Option<Result<VmSqlStream>>,
    ) -> Result<VmRunState> {
        let mut state = self
            .state
            .take()
            .ok_or_else(|| DbError::new("55000", "PL/pgSQL VM is already complete"))?;
        match (self.pending_sql.take(), response) {
            (Some(pending), Some(response)) => {
                if let Err(error) = state.apply_sql_response(pending, response) {
                    state.handle_error(host, error)?;
                }
            }
            (Some(_), None) => {
                return Err(DbError::internal(
                    "PL/pgSQL VM resumed without its pending SQL response",
                ));
            }
            (None, Some(_)) => {
                return Err(DbError::internal(
                    "PL/pgSQL VM received an unexpected SQL response",
                ));
            }
            (None, None) => {}
        }
        if let Err(error) = state.refresh_memory(None) {
            state.handle_error(host, error)?;
            state.refresh_memory(None)?;
        }
        let VmState {
            program,
            limits,
            mut locals,
            mut records,
            mut instruction_pointer,
            mut steps,
            mut returned_rows,
            mut query_loops,
            mut integer_loops,
            mut foreach_loops,
            mut cursors,
            mut active_exception,
            exception_regions,
            mut active_exception_regions,
            mut memory_reservation,
        } = state;
        while instruction_pointer < program.instructions.len() {
            query_loops.retain(|_, state| instruction_pointer < state.end);
            integer_loops.retain(|_, state| instruction_pointer < state.end);
            foreach_loops.retain(|_, state| instruction_pointer < state.end);
            while active_exception_regions
                .last()
                .is_some_and(|(_, end)| instruction_pointer >= *end)
            {
                host.commit_exception_block()?;
                active_exception_regions.pop();
            }
            for region in exception_regions
                .iter()
                .copied()
                .filter(|(start, _)| *start == instruction_pointer)
            {
                if !active_exception_regions.contains(&region) {
                    host.begin_exception_block()?;
                    active_exception_regions.push(region);
                }
            }
            steps = steps.saturating_add(1);
            if steps > limits.max_steps {
                return limit_error("PL/pgSQL execution step limit exceeded");
            }
            host.check_cancelled()?;
            let mut yielded_sql = None::<(PendingSql, VmSqlRequest)>;
            let step = (|| -> Result<Option<VmOutput>> {
                match &program.instructions[instruction_pointer] {
                    Instruction::Assign { slot, expression } => {
                        let already_assigned = records
                            .get(slot)
                            .map_or_else(|| !locals[*slot].is_null(), RuntimeRecord::is_assigned);
                        if program
                            .locals
                            .get(*slot)
                            .is_some_and(|local| local.constant && already_assigned)
                        {
                            return Err(DbError::new(
                                "22005",
                                format!(
                                    "constant {} cannot be reassigned",
                                    program.locals[*slot].name
                                ),
                            ));
                        }
                        if records.contains_key(slot) {
                            let source = positional_parameter_index(expression)
                                .and_then(|source| records.get(&source))
                                .cloned()
                                .ok_or_else(|| {
                                    DbError::new(
                                        "42804",
                                        "a record variable can only be assigned another record row",
                                    )
                                })?;
                            ensure_runtime_record_limit(
                                &records,
                                Some((*slot, &source)),
                                limits.max_cursor_bytes,
                            )?;
                            records.insert(*slot, source);
                        } else {
                            locals[*slot] =
                                evaluate_runtime_expression(host, expression, &locals, &records)?;
                        }
                        instruction_pointer += 1;
                    }
                    Instruction::AssignField {
                        slot,
                        field,
                        expression,
                    } => {
                        let value =
                            evaluate_runtime_expression(host, expression, &locals, &records)?;
                        if let Some(record) = records.get(slot) {
                            let mut candidate = record.clone();
                            candidate.assign_field(field, value)?;
                            ensure_runtime_record_limit(
                                &records,
                                Some((*slot, &candidate)),
                                limits.max_cursor_bytes,
                            )?;
                            records.insert(*slot, candidate);
                        } else {
                            host.assign_composite_field(*slot, field, value)?;
                        }
                        instruction_pointer += 1;
                    }
                    Instruction::JumpIfFalse { expression, target } => {
                        let value =
                            evaluate_runtime_expression(host, expression, &locals, &records)?;
                        instruction_pointer = if value == Value::Boolean(true) {
                            instruction_pointer + 1
                        } else {
                            checked_target(*target, program.instructions.len())?
                        };
                    }
                    Instruction::Jump { target } => {
                        instruction_pointer = checked_target(*target, program.instructions.len())?;
                    }
                    Instruction::ExecuteSql { sql, into } => {
                        let (sql, parameters) =
                            expand_runtime_record_fields(sql, &locals, &records)?;
                        yielded_sql = Some((
                            PendingSql::Execute { into: *into },
                            VmSqlRequest { sql, parameters },
                        ));
                    }
                    Instruction::DynamicExecute {
                        query,
                        using,
                        into,
                        strict,
                    } => {
                        let query = evaluate_runtime_expression(host, query, &locals, &records)?;
                        let Value::Text(query) = query else {
                            return Err(DbError::new(
                                "42804",
                                "dynamic EXECUTE query must evaluate to text",
                            ));
                        };
                        if query.len() > limits.max_dynamic_sql_bytes {
                            return limit_error("dynamic SQL exceeds the configured byte limit");
                        }
                        let parameters = using
                            .iter()
                            .map(|expression| {
                                evaluate_runtime_expression(host, expression, &locals, &records)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        yielded_sql = Some((
                            PendingSql::Dynamic {
                                into: *into,
                                strict: *strict,
                            },
                            VmSqlRequest {
                                sql: query,
                                parameters,
                            },
                        ));
                    }
                    Instruction::OpenCursor { cursor, query } => {
                        if cursors.contains_key(cursor) {
                            let name = program
                                .cursor_declarations
                                .get(*cursor)
                                .map_or("<unknown>", |cursor| cursor.name.as_str());
                            return Err(DbError::new(
                                "42P03",
                                format!("cursor {name} is already open"),
                            ));
                        }
                        if cursors.len() >= limits.max_open_cursors {
                            return Err(DbError::new(
                                "54000",
                                "PL/pgSQL open-cursor limit exceeded",
                            ));
                        }
                        let (sql, parameters) = match query {
                            CursorQuery::Bound => {
                                let sql = program
                                    .cursor_declarations
                                    .get(*cursor)
                                    .and_then(|cursor| cursor.bound_query.clone())
                                    .ok_or_else(|| {
                                        DbError::internal(
                                            "bound cursor query is missing from bytecode",
                                        )
                                    })?;
                                expand_runtime_record_fields(&sql, &locals, &records)?
                            }
                            CursorQuery::Static(query) => {
                                expand_runtime_record_fields(query, &locals, &records)?
                            }
                            CursorQuery::Dynamic { query, using } => {
                                let value =
                                    evaluate_runtime_expression(host, query, &locals, &records)?;
                                let Value::Text(query) = value else {
                                    return Err(DbError::new(
                                        "42804",
                                        "OPEN FOR EXECUTE query must evaluate to text",
                                    ));
                                };
                                if query.len() > limits.max_dynamic_sql_bytes {
                                    return limit_error(
                                        "OPEN FOR EXECUTE query exceeds the configured byte limit",
                                    );
                                }
                                let parameters = using
                                    .iter()
                                    .map(|expression| {
                                        evaluate_runtime_expression(
                                            host, expression, &locals, &records,
                                        )
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                (query, parameters)
                            }
                        };
                        yielded_sql = Some((
                            PendingSql::OpenCursor { cursor: *cursor },
                            VmSqlRequest { sql, parameters },
                        ));
                    }
                    Instruction::FetchCursor {
                        cursor,
                        direction,
                        into,
                    } => {
                        let direction =
                            evaluate_cursor_direction(host, direction, &locals, &records)?;
                        let state = cursors.get_mut(cursor).ok_or_else(|| {
                            let name = program
                                .cursor_declarations
                                .get(*cursor)
                                .map_or("<unknown>", |cursor| cursor.name.as_str());
                            DbError::new("34000", format!("cursor {name} is not open"))
                        })?;
                        let row = state.seek(direction, limits)?;
                        assign_runtime_row(
                            *into,
                            row,
                            &mut locals,
                            &mut records,
                            limits.max_cursor_bytes,
                        )?;
                        instruction_pointer += 1;
                    }
                    Instruction::MoveCursor { cursor, direction } => {
                        let direction =
                            evaluate_cursor_direction(host, direction, &locals, &records)?;
                        let state = cursors.get_mut(cursor).ok_or_else(|| {
                            let name = program
                                .cursor_declarations
                                .get(*cursor)
                                .map_or("<unknown>", |cursor| cursor.name.as_str());
                            DbError::new("34000", format!("cursor {name} is not open"))
                        })?;
                        state.seek(direction, limits)?;
                        instruction_pointer += 1;
                    }
                    Instruction::CloseCursor { cursor } => {
                        if cursors.remove(cursor).is_none() {
                            let name = program
                                .cursor_declarations
                                .get(*cursor)
                                .map_or("<unknown>", |cursor| cursor.name.as_str());
                            return Err(DbError::new(
                                "34000",
                                format!("cursor {name} is not open"),
                            ));
                        }
                        instruction_pointer += 1;
                    }
                    Instruction::Raise {
                        level,
                        message,
                        sql_state,
                    } => {
                        let Some(message) = message else {
                            return Err(active_exception.clone().ok_or_else(|| {
                                DbError::new(
                                    "0Z002",
                                    "RAISE without parameters is outside an exception handler",
                                )
                            })?);
                        };
                        let message = evaluate_message(host, message, &locals, &records, "RAISE")?;
                        let default_state = match level {
                            RaiseLevel::Info | RaiseLevel::Notice => "00000",
                            RaiseLevel::Warning => "01000",
                            RaiseLevel::Exception => "P0001",
                        };
                        let sql_state = sql_state.as_deref().unwrap_or(default_state);
                        match level {
                            RaiseLevel::Exception => {
                                return Err(DbError::new(sql_state, message));
                            }
                            RaiseLevel::Info | RaiseLevel::Notice | RaiseLevel::Warning => {
                                let severity = match level {
                                    RaiseLevel::Info => DbNoticeSeverity::Info,
                                    RaiseLevel::Notice => DbNoticeSeverity::Notice,
                                    RaiseLevel::Warning => DbNoticeSeverity::Warning,
                                    RaiseLevel::Exception => {
                                        return Err(DbError::internal(
                                            "exception raise reached the notice path",
                                        ));
                                    }
                                };
                                host.emit_notice(DbNotice {
                                    severity,
                                    sql_state: sql_state.to_owned(),
                                    message,
                                    detail: None,
                                    hint: None,
                                    position: None,
                                    object_identity: None,
                                })?;
                                instruction_pointer += 1;
                            }
                        }
                    }
                    Instruction::Assert { condition, message } => {
                        let condition =
                            evaluate_runtime_expression(host, condition, &locals, &records)?;
                        if condition == Value::Boolean(true) {
                            instruction_pointer += 1;
                        } else if condition == Value::Boolean(false) || condition.is_null() {
                            let message = message
                                .as_deref()
                                .map(|message| {
                                    evaluate_message(host, message, &locals, &records, "ASSERT")
                                })
                                .transpose()?
                                .unwrap_or_else(|| "assertion failed".to_owned());
                            return Err(DbError::new("P0004", message));
                        } else {
                            return Err(DbError::new(
                                "42804",
                                "ASSERT condition must evaluate to boolean",
                            ));
                        }
                    }
                    Instruction::QueryForStart { slot, sql, end } => {
                        let (sql, parameters) =
                            expand_runtime_record_fields(sql, &locals, &records)?;
                        yielded_sql = Some((
                            PendingSql::QueryForStart {
                                start: instruction_pointer,
                                slot: *slot,
                                end: checked_target(*end, program.instructions.len())?,
                            },
                            VmSqlRequest { sql, parameters },
                        ));
                    }
                    Instruction::QueryForNext { start, body } => {
                        let Some(state) = query_loops.get_mut(start) else {
                            return Err(DbError::internal(
                                "PL/pgSQL query FOR iterator state is missing",
                            ));
                        };
                        let slot = state.slot;
                        if let Some(row) = state.next_row(limits.max_returned_rows)? {
                            assign_runtime_row(
                                slot,
                                Some(row),
                                &mut locals,
                                &mut records,
                                limits.max_cursor_bytes,
                            )?;
                            instruction_pointer =
                                checked_target(*body, program.instructions.len())?;
                        } else {
                            query_loops.remove(start);
                            instruction_pointer += 1;
                        }
                    }
                    Instruction::IntegerForStart {
                        slot,
                        lower,
                        upper,
                        step,
                        reverse,
                        end,
                    } => {
                        let lower = evaluate_integer_expression(
                            host,
                            lower,
                            &locals,
                            &records,
                            "lower bound",
                        )?;
                        let upper = evaluate_integer_expression(
                            host,
                            upper,
                            &locals,
                            &records,
                            "upper bound",
                        )?;
                        let step =
                            evaluate_integer_expression(host, step, &locals, &records, "BY value")?;
                        if step <= 0 {
                            return Err(DbError::new(
                                "22023",
                                "BY value of PL/pgSQL integer FOR loop must be greater than zero",
                            ));
                        }
                        let has_first = if *reverse {
                            lower >= upper
                        } else {
                            lower <= upper
                        };
                        if has_first {
                            locals[*slot] = Value::Int64(lower);
                            integer_loops.insert(
                                instruction_pointer,
                                IntegerLoopState {
                                    slot: *slot,
                                    current: lower,
                                    bound: upper,
                                    step,
                                    reverse: *reverse,
                                    end: checked_target(*end, program.instructions.len())?,
                                },
                            );
                            instruction_pointer += 1;
                        } else {
                            instruction_pointer = checked_target(*end, program.instructions.len())?;
                        }
                    }
                    Instruction::IntegerForNext { start, body } => {
                        let Some(state) = integer_loops.get_mut(start) else {
                            return Err(DbError::internal(
                                "PL/pgSQL integer FOR iterator state is missing",
                            ));
                        };
                        if let Some(value) = state.advance()? {
                            locals[state.slot] = value;
                            instruction_pointer =
                                checked_target(*body, program.instructions.len())?;
                        } else {
                            integer_loops.remove(start);
                            instruction_pointer += 1;
                        }
                    }
                    Instruction::ForeachStart { slot, array, end } => {
                        let array = evaluate_runtime_expression(host, array, &locals, &records)?;
                        let Value::Array(array) = array else {
                            if array.is_null() {
                                return Err(DbError::new(
                                    "22004",
                                    "FOREACH expression must not be NULL",
                                ));
                            }
                            return Err(DbError::new(
                                "42804",
                                "FOREACH expression must evaluate to an array",
                            ));
                        };
                        let mut values = VecDeque::from(array.into_values());
                        if let Some(value) = values.pop_front() {
                            locals[*slot] = value;
                            foreach_loops.insert(
                                instruction_pointer,
                                ForeachLoopState {
                                    slot: *slot,
                                    values,
                                    end: checked_target(*end, program.instructions.len())?,
                                },
                            );
                            instruction_pointer += 1;
                        } else {
                            instruction_pointer = checked_target(*end, program.instructions.len())?;
                        }
                    }
                    Instruction::ForeachNext { start, body } => {
                        let Some(state) = foreach_loops.get_mut(start) else {
                            return Err(DbError::internal(
                                "PL/pgSQL FOREACH iterator state is missing",
                            ));
                        };
                        if let Some(value) = state.values.pop_front() {
                            locals[state.slot] = value;
                            instruction_pointer =
                                checked_target(*body, program.instructions.len())?;
                        } else {
                            foreach_loops.remove(start);
                            instruction_pointer += 1;
                        }
                    }
                    Instruction::Return { expression, next } => {
                        let value = expression
                            .as_ref()
                            .map(|expression| {
                                evaluate_runtime_expression(host, expression, &locals, &records)
                            })
                            .transpose()?
                            .unwrap_or(Value::Null);
                        if *next {
                            returned_rows.push(value);
                            if returned_rows.len() > limits.max_returned_rows {
                                return limit_error("PL/pgSQL returned-row limit exceeded");
                            }
                            instruction_pointer += 1;
                        } else {
                            return Ok(Some(VmOutput {
                                return_value: Some(value),
                                returned_rows: std::mem::take(&mut returned_rows),
                                return_parameter: expression
                                    .as_deref()
                                    .and_then(positional_parameter_index),
                                final_locals: locals.clone(),
                                output_parameters: Vec::new(),
                                retained_memory: None,
                            }));
                        }
                    }
                    Instruction::Checkpoint => {
                        instruction_pointer += 1;
                    }
                }
                Ok(None)
            })();
            let step = step.and_then(|output| {
                let bytes = if let Some(output) = &output {
                    estimated_vm_output_bytes(output)?
                } else {
                    estimated_vm_runtime_bytes(
                        &program,
                        &locals,
                        &records,
                        &returned_rows,
                        &query_loops,
                        &integer_loops,
                        &foreach_loops,
                        &cursors,
                        active_exception.as_ref(),
                        &exception_regions,
                        &active_exception_regions,
                        yielded_sql.as_ref().map(|(_, request)| request),
                    )?
                };
                memory_reservation.resize(bytes)?;
                Ok(output)
            });
            match step {
                Ok(Some(output)) => {
                    while active_exception_regions.pop().is_some() {
                        host.commit_exception_block()?;
                    }
                    return Ok(VmRunState::Complete(attach_output_memory(
                        output,
                        memory_reservation,
                    )));
                }
                Ok(None) => {
                    if let Some((pending, request)) = yielded_sql {
                        self.state = Some(VmState {
                            program,
                            limits,
                            locals,
                            records,
                            instruction_pointer,
                            steps,
                            returned_rows,
                            query_loops,
                            integer_loops,
                            foreach_loops,
                            cursors,
                            active_exception,
                            exception_regions,
                            active_exception_regions,
                            memory_reservation,
                        });
                        self.pending_sql = Some(pending);
                        return Ok(VmRunState::Sql(request));
                    }
                }
                Err(error) => {
                    let handler = program
                        .exception_handlers
                        .iter()
                        .enumerate()
                        .filter(|(_, handler)| {
                            handler.protected_start <= instruction_pointer
                                && instruction_pointer < handler.protected_end
                                && match &handler.matcher {
                                    ExceptionMatcher::SqlState(state) => {
                                        state.eq_ignore_ascii_case(&error.sql_state)
                                    }
                                    ExceptionMatcher::Others => {
                                        !matches!(error.sql_state.as_str(), "57014" | "P0004")
                                    }
                                }
                        })
                        .max_by_key(|(index, handler)| {
                            (
                                handler.protected_start,
                                usize::MAX.saturating_sub(handler.protected_end),
                                usize::MAX.saturating_sub(*index),
                            )
                        })
                        .map(|(_, handler)| {
                            (
                                handler.protected_start,
                                handler.protected_end,
                                handler.target,
                            )
                        });
                    let Some((protected_start, protected_end, handler_target)) = handler else {
                        while active_exception_regions.pop().is_some() {
                            host.rollback_exception_block()?;
                        }
                        return Err(error);
                    };
                    let selected_region = (protected_start, protected_end);
                    let mut selected_rolled_back = false;
                    while let Some(region) = active_exception_regions.pop() {
                        host.rollback_exception_block()?;
                        if region == selected_region {
                            selected_rolled_back = true;
                            break;
                        }
                    }
                    if !selected_rolled_back {
                        return Err(DbError::internal(
                            "PL/pgSQL exception handler region was not active",
                        ));
                    }
                    query_loops.clear();
                    integer_loops.clear();
                    foreach_loops.clear();
                    cursors.clear();
                    if let Some(slot) = program.sqlstate_slot {
                        locals[slot] = Value::Text(error.sql_state.clone());
                    }
                    if let Some(slot) = program.sqlerrm_slot {
                        locals[slot] = Value::Text(error.message.clone());
                    }
                    active_exception = Some(error);
                    instruction_pointer =
                        checked_target(handler_target, program.instructions.len())?;
                    let bytes = estimated_vm_runtime_bytes(
                        &program,
                        &locals,
                        &records,
                        &returned_rows,
                        &query_loops,
                        &integer_loops,
                        &foreach_loops,
                        &cursors,
                        active_exception.as_ref(),
                        &exception_regions,
                        &active_exception_regions,
                        None,
                    )?;
                    memory_reservation.resize(bytes)?;
                }
            }
        }
        while active_exception_regions.pop().is_some() {
            host.commit_exception_block()?;
        }
        let output = VmOutput {
            return_value: None,
            returned_rows,
            return_parameter: None,
            final_locals: locals,
            output_parameters: Vec::new(),
            retained_memory: None,
        };
        memory_reservation.resize(estimated_vm_output_bytes(&output)?)?;
        Ok(VmRunState::Complete(attach_output_memory(
            output,
            memory_reservation,
        )))
    }

    pub fn ensure_transaction_boundary_ready(&self) -> Result<()> {
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| DbError::new("55000", "PL/pgSQL VM is already complete"))?;
        if !state.active_exception_regions.is_empty()
            || !state.query_loops.is_empty()
            || !state.cursors.is_empty()
        {
            return Err(DbError::new(
                "2D000",
                "cannot end a transaction while a PL/pgSQL subtransaction or cursor is active",
            )
            .with_hint(
                "close active cursors and leave exception blocks before transaction control",
            ));
        }
        Ok(())
    }
}

impl VmState {
    fn refresh_memory(&mut self, pending_request: Option<&VmSqlRequest>) -> Result<()> {
        let bytes = estimated_vm_runtime_bytes(
            &self.program,
            &self.locals,
            &self.records,
            &self.returned_rows,
            &self.query_loops,
            &self.integer_loops,
            &self.foreach_loops,
            &self.cursors,
            self.active_exception.as_ref(),
            &self.exception_regions,
            &self.active_exception_regions,
            pending_request,
        )?;
        self.memory_reservation.resize(bytes)
    }

    fn apply_sql_response(
        &mut self,
        pending: PendingSql,
        response: Result<VmSqlStream>,
    ) -> Result<()> {
        let events = response?;
        match pending {
            PendingSql::Execute { into } => {
                let (first, _) = collect_sql_result(events, false)?;
                if let Some(slot) = into {
                    assign_runtime_row(
                        slot,
                        first,
                        &mut self.locals,
                        &mut self.records,
                        self.limits.max_cursor_bytes,
                    )?;
                }
                self.instruction_pointer += 1;
            }
            PendingSql::Dynamic { into, strict } => {
                let (first, row_count) = collect_sql_result(events, strict)?;
                if let Some(slot) = into {
                    if strict && row_count == 0 {
                        return Err(DbError::new(
                            "P0002",
                            "dynamic EXECUTE INTO STRICT returned no rows",
                        ));
                    }
                    assign_runtime_row(
                        slot,
                        first,
                        &mut self.locals,
                        &mut self.records,
                        self.limits.max_cursor_bytes,
                    )?;
                }
                self.instruction_pointer += 1;
            }
            PendingSql::OpenCursor { cursor } => {
                self.cursors.insert(cursor, CursorState::new(events));
                self.instruction_pointer += 1;
            }
            PendingSql::QueryForStart { start, slot, end } => {
                let mut state = QueryLoopState {
                    slot,
                    end,
                    events,
                    current_rows: VecDeque::new(),
                    fields: None,
                    rows_seen: 0,
                };
                if let Some(row) = state.next_row(self.limits.max_returned_rows)? {
                    assign_runtime_row(
                        slot,
                        Some(row),
                        &mut self.locals,
                        &mut self.records,
                        self.limits.max_cursor_bytes,
                    )?;
                    self.query_loops.insert(start, state);
                    self.instruction_pointer += 1;
                } else {
                    self.instruction_pointer = end;
                }
            }
        }
        Ok(())
    }

    fn handle_error(&mut self, host: &mut impl PlpgsqlHost, error: DbError) -> Result<()> {
        let handler = self
            .program
            .exception_handlers
            .iter()
            .enumerate()
            .filter(|(_, handler)| {
                handler.protected_start <= self.instruction_pointer
                    && self.instruction_pointer < handler.protected_end
                    && match &handler.matcher {
                        ExceptionMatcher::SqlState(state) => {
                            state.eq_ignore_ascii_case(&error.sql_state)
                        }
                        ExceptionMatcher::Others => {
                            !matches!(error.sql_state.as_str(), "57014" | "P0004")
                        }
                    }
            })
            .max_by_key(|(index, handler)| {
                (
                    handler.protected_start,
                    usize::MAX.saturating_sub(handler.protected_end),
                    usize::MAX.saturating_sub(*index),
                )
            })
            .map(|(_, handler)| {
                (
                    handler.protected_start,
                    handler.protected_end,
                    handler.target,
                )
            });
        let Some((protected_start, protected_end, handler_target)) = handler else {
            while self.active_exception_regions.pop().is_some() {
                host.rollback_exception_block()?;
            }
            return Err(error);
        };
        let selected_region = (protected_start, protected_end);
        let mut selected_rolled_back = false;
        while let Some(region) = self.active_exception_regions.pop() {
            host.rollback_exception_block()?;
            if region == selected_region {
                selected_rolled_back = true;
                break;
            }
        }
        if !selected_rolled_back {
            return Err(DbError::internal(
                "PL/pgSQL exception handler region was not active",
            ));
        }
        self.query_loops.clear();
        self.integer_loops.clear();
        self.foreach_loops.clear();
        self.cursors.clear();
        if let Some(slot) = self.program.sqlstate_slot {
            self.locals[slot] = Value::Text(error.sql_state.clone());
        }
        if let Some(slot) = self.program.sqlerrm_slot {
            self.locals[slot] = Value::Text(error.message.clone());
        }
        self.active_exception = Some(error);
        self.instruction_pointer = checked_target(handler_target, self.program.instructions.len())?;
        Ok(())
    }
}

fn collect_sql_result(
    events: VmSqlStream,
    strict: bool,
) -> Result<(Option<RuntimeQueryRow>, usize)> {
    let mut fields = None::<Vec<String>>;
    let mut first = None::<RuntimeQueryRow>;
    let mut row_count = 0_usize;
    for event in events {
        match event? {
            QueryEvent::Schema(schema) => {
                fields = Some(schema.fields.into_iter().map(|field| field.name).collect());
            }
            QueryEvent::Batch(batch) => {
                let row_fields = fields.get_or_insert_with(|| {
                    batch
                        .schema
                        .fields
                        .iter()
                        .map(|field| field.name.clone())
                        .collect()
                });
                for row in batch.rows {
                    row_count = row_count.saturating_add(1);
                    if first.is_none() {
                        first = Some(RuntimeQueryRow {
                            fields: row_fields.clone(),
                            row,
                        });
                    }
                    if strict && row_count > 1 {
                        return Err(DbError::new(
                            "P0003",
                            "dynamic EXECUTE INTO STRICT returned more than one row",
                        ));
                    }
                }
            }
            QueryEvent::Progress(_) | QueryEvent::Notice(_) | QueryEvent::Complete(_) => {}
        }
    }
    Ok((first, row_count))
}

fn evaluate_integer_expression(
    host: &mut impl PlpgsqlHost,
    expression: &str,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
    context: &str,
) -> Result<i64> {
    match evaluate_runtime_expression(host, expression, locals, records)? {
        Value::Int16(value) => Ok(i64::from(value)),
        Value::Int32(value) => Ok(i64::from(value)),
        Value::Int64(value) => Ok(value),
        _ => Err(DbError::new(
            "42804",
            format!("PL/pgSQL integer FOR {context} must evaluate to an integer"),
        )),
    }
}

fn evaluate_cursor_direction(
    host: &mut impl PlpgsqlHost,
    direction: &CursorDirection,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
) -> Result<EvaluatedCursorDirection> {
    match direction {
        CursorDirection::Next => Ok(EvaluatedCursorDirection::Next),
        CursorDirection::Prior => Ok(EvaluatedCursorDirection::Prior),
        CursorDirection::First => Ok(EvaluatedCursorDirection::First),
        CursorDirection::Last => Ok(EvaluatedCursorDirection::Last),
        CursorDirection::Absolute(expression) => Ok(EvaluatedCursorDirection::Absolute(
            evaluate_cursor_integer_expression(host, expression, locals, records)?,
        )),
        CursorDirection::Relative(expression) => Ok(EvaluatedCursorDirection::Relative(
            evaluate_cursor_integer_expression(host, expression, locals, records)?,
        )),
        CursorDirection::Forward(expression) => Ok(EvaluatedCursorDirection::Forward(
            evaluate_cursor_count(host, expression.as_deref(), locals, records, "FORWARD")?,
        )),
        CursorDirection::ForwardAll => Ok(EvaluatedCursorDirection::ForwardAll),
        CursorDirection::Backward(expression) => Ok(EvaluatedCursorDirection::Backward(
            evaluate_cursor_count(host, expression.as_deref(), locals, records, "BACKWARD")?,
        )),
        CursorDirection::BackwardAll => Ok(EvaluatedCursorDirection::BackwardAll),
    }
}

fn evaluate_cursor_count(
    host: &mut impl PlpgsqlHost,
    expression: Option<&str>,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
    direction: &str,
) -> Result<i64> {
    let count = expression.map_or(Ok(1), |expression| {
        evaluate_cursor_integer_expression(host, expression, locals, records)
    })?;
    if count < 0 {
        return Err(DbError::new(
            "22023",
            format!("cursor {direction} count must not be negative"),
        ));
    }
    Ok(count)
}

fn evaluate_cursor_integer_expression(
    host: &mut impl PlpgsqlHost,
    expression: &str,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
) -> Result<i64> {
    match evaluate_runtime_expression(host, expression, locals, records)? {
        Value::Int16(value) => Ok(i64::from(value)),
        Value::Int32(value) => Ok(i64::from(value)),
        Value::Int64(value) => Ok(value),
        _ => Err(DbError::new(
            "42804",
            "cursor direction value must evaluate to an integer",
        )),
    }
}

fn evaluate_message(
    host: &mut impl PlpgsqlHost,
    expression: &str,
    locals: &[Value],
    records: &BTreeMap<usize, RuntimeRecord>,
    statement: &str,
) -> Result<String> {
    match evaluate_runtime_expression(host, expression, locals, records)? {
        Value::Text(message) => Ok(message),
        Value::Null => Ok(String::new()),
        _ => Err(DbError::new(
            "42804",
            format!("{statement} message must evaluate to text"),
        )),
    }
}

fn positional_parameter_index(expression: &str) -> Option<usize> {
    expression
        .trim()
        .strip_prefix('$')?
        .parse::<usize>()
        .ok()?
        .checked_sub(1)
}

fn logical_lines(source: &str) -> Result<Vec<String>> {
    let mut lines = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut parenthesis_depth = 0_usize;
    let mut characters = source.chars().peekable();
    while let Some(character) = characters.next() {
        match quote {
            Some(delimiter) if character == delimiter => {
                current.push(character);
                if characters.peek() == Some(&delimiter) {
                    current.push(characters.next().unwrap_or(delimiter));
                } else {
                    quote = None;
                }
            }
            Some(_) => current.push(character),
            None if matches!(character, '\'' | '"') => {
                quote = Some(character);
                current.push(character);
            }
            None if character == '(' => {
                parenthesis_depth = parenthesis_depth.saturating_add(1);
                current.push(character);
            }
            None if character == ')' => {
                parenthesis_depth = parenthesis_depth.checked_sub(1).ok_or_else(|| {
                    DbError::new(
                        "42601",
                        "PL/pgSQL source has an unmatched closing parenthesis",
                    )
                })?;
                current.push(character);
            }
            None if matches!(character, ';' | '\n' | '\r') && parenthesis_depth == 0 => {
                push_logical_segment(&mut lines, &current);
                current.clear();
            }
            None if matches!(character, '\n' | '\r') => current.push(' '),
            None => current.push(character),
        }
    }
    if quote.is_some() {
        return syntax_error("unterminated quoted string in PL/pgSQL source");
    }
    if parenthesis_depth != 0 {
        return syntax_error("PL/pgSQL source has an unmatched opening parenthesis");
    }
    push_logical_segment(&mut lines, &current);
    Ok(lines)
}

fn push_logical_segment(lines: &mut Vec<String>, segment: &str) {
    let trimmed = segment.trim();
    if trimmed.is_empty() {
        return;
    }
    for keyword in ["DECLARE", "BEGIN"] {
        if let Some(rest) = strip_leading_keyword(trimmed, keyword) {
            lines.push(keyword.to_owned());
            if !rest.is_empty() {
                lines.push(rest.to_owned());
            }
            return;
        }
    }
    lines.push(trimmed.to_owned());
}

fn strip_leading_keyword<'a>(value: &'a str, keyword: &str) -> Option<&'a str> {
    let prefix = value.get(..keyword.len())?;
    if !prefix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = value.get(keyword.len()..)?;
    rest.chars()
        .next()
        .is_some_and(char::is_whitespace)
        .then(|| rest.trim_start())
}

fn rewrite_locals(expression: &str, locals: &BTreeMap<String, usize>) -> String {
    let mut output = String::with_capacity(expression.len());
    let mut identifier = String::new();
    let mut quote = None;
    let flush = |output: &mut String, identifier: &mut String| {
        if identifier.is_empty() {
            return;
        }
        if let Some(slot) = locals.get(&identifier.to_ascii_lowercase()) {
            output.push('$');
            output.push_str(&(slot + 1).to_string());
        } else {
            output.push_str(identifier);
        }
        identifier.clear();
    };
    for character in expression.chars() {
        match quote {
            Some(delimiter) => {
                output.push(character);
                if character == delimiter {
                    quote = None;
                }
            }
            None if matches!(character, '\'' | '"') => {
                flush(&mut output, &mut identifier);
                quote = Some(character);
                output.push(character);
            }
            None if character.is_ascii_alphanumeric() || character == '_' => {
                identifier.push(character);
            }
            None => {
                flush(&mut output, &mut identifier);
                output.push(character);
            }
        }
    }
    flush(&mut output, &mut identifier);
    output
}

fn split_keyword<'a>(value: &'a str, keyword: &str) -> (&'a str, Option<&'a str>) {
    let bytes = value.as_bytes();
    let keyword = keyword.as_bytes();
    let mut quote = None;
    let mut depth = 0_usize;
    let mut index = 0_usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(delimiter) = quote {
            if byte == delimiter {
                if bytes.get(index + 1) == Some(&delimiter) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'\'' | b'"' => quote = Some(byte),
            b'(' => depth = depth.saturating_add(1),
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        let end = index.saturating_add(keyword.len());
        if depth == 0
            && index > 0
            && end < bytes.len()
            && bytes[index - 1].is_ascii_whitespace()
            && bytes[end].is_ascii_whitespace()
            && bytes[index..end].eq_ignore_ascii_case(keyword)
        {
            return (value[..index].trim_end(), Some(value[end..].trim_start()));
        }
        index += 1;
    }
    (value, None)
}

fn parse_exception_matcher(value: &str) -> Result<ExceptionMatcher> {
    if value.eq_ignore_ascii_case("OTHERS") {
        return Ok(ExceptionMatcher::Others);
    }
    if let Some(state) = value
        .get(8..)
        .filter(|_| {
            value
                .get(.."SQLSTATE".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SQLSTATE"))
        })
        .map(str::trim)
        .and_then(|value| value.strip_prefix('\''))
        .and_then(|value| value.strip_suffix('\''))
    {
        validate_sql_state(state)?;
        return Ok(ExceptionMatcher::SqlState(state.to_ascii_uppercase()));
    }
    let state = match value.trim().to_ascii_lowercase().as_str() {
        "unique_violation" => "23505",
        "division_by_zero" => "22012",
        "null_value_not_allowed" => "22004",
        "no_data_found" => "P0002",
        "too_many_rows" => "P0003",
        "assert_failure" => "P0004",
        "raise_exception" => "P0001",
        _ => {
            return Err(DbError::new(
                "42704",
                format!("unrecognized exception condition {value}"),
            ));
        }
    };
    Ok(ExceptionMatcher::SqlState(state.to_owned()))
}

fn validate_sql_state(state: &str) -> Result<()> {
    if state.len() != 5 || !state.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return syntax_error("SQLSTATE must contain five ASCII letters or digits");
    }
    if state.starts_with("00") {
        return syntax_error("SQLSTATE class 00 cannot be raised as an error");
    }
    Ok(())
}

fn parse_label(value: &str) -> Result<Option<String>> {
    if !value.starts_with("<<") && !value.ends_with(">>") {
        return Ok(None);
    }
    let label = value
        .strip_prefix("<<")
        .and_then(|value| value.strip_suffix(">>"))
        .ok_or_else(|| DbError::new("42601", "PL/pgSQL label is malformed"))?;
    normalize_label(label).map(Some)
}

fn normalize_label(label: &str) -> Result<String> {
    let label = label.trim();
    let mut characters = label.chars();
    if label.is_empty()
        || label.len() > 63
        || !characters
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        || !characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return syntax_error("PL/pgSQL label must be a bounded unquoted identifier");
    }
    Ok(label.to_ascii_lowercase())
}

fn parse_block_end_label(value: &str) -> Result<Option<Option<String>>> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("END") {
        return Ok(Some(None));
    }
    let Some(tail) = value
        .get("END".len()..)
        .filter(|_| {
            value
                .get(.."END".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("END"))
        })
        .map(str::trim)
        .filter(|tail| !tail.is_empty())
    else {
        return Ok(None);
    };
    if tail.split_whitespace().next().is_some_and(|word| {
        ["IF", "CASE", "LOOP"]
            .iter()
            .any(|keyword| word.eq_ignore_ascii_case(keyword))
    }) {
        return Ok(None);
    }
    normalize_label(tail).map(|label| Some(Some(label)))
}

fn parse_loop_control<'a>(
    statement: &'a str,
    keyword: &str,
) -> Result<(Option<String>, Option<&'a str>)> {
    let rest = statement
        .get(keyword.len()..)
        .ok_or_else(|| DbError::internal("loop-control keyword length is invalid"))?
        .trim();
    if rest.is_empty() {
        return Ok((None, None));
    }
    let mut parts = rest.splitn(2, char::is_whitespace);
    let first = parts.next().unwrap_or_default();
    let tail = parts.next().map(str::trim).unwrap_or_default();
    if first.eq_ignore_ascii_case("WHEN") {
        return Ok((None, Some(tail)));
    }
    let label = normalize_label(first)?;
    if tail.is_empty() {
        return Ok((Some(label), None));
    }
    let condition = tail
        .get("WHEN".len()..)
        .filter(|_| {
            tail.get(.."WHEN".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("WHEN"))
        })
        .map(str::trim)
        .ok_or_else(|| {
            DbError::new(
                "42601",
                format!("{keyword} label may only be followed by WHEN"),
            )
        })?;
    Ok((Some(label), Some(condition)))
}

fn loop_control_target_mut<'a>(
    controls: &'a mut [ControlFrame],
    label: Option<&str>,
) -> Result<&'a mut ControlFrame> {
    for frame in controls.iter_mut().rev() {
        if let ControlFrame::Loop {
            label: frame_label, ..
        } = frame
            && label.is_none_or(|label| frame_label.as_deref() == Some(label))
        {
            return Ok(frame);
        }
    }
    match label {
        Some(label) => syntax_error(format!("loop label {label} does not exist")),
        None => syntax_error("loop control statement is outside a loop"),
    }
}

fn patch_query_for_end(
    instructions: &mut [Instruction],
    instruction: usize,
    target: usize,
) -> Result<()> {
    match instructions.get_mut(instruction) {
        Some(Instruction::QueryForStart { end, .. }) => {
            *end = target;
            Ok(())
        }
        _ => Err(DbError::internal(
            "PL/pgSQL compiler query FOR patch target is invalid",
        )),
    }
}

fn patch_integer_for_end(
    instructions: &mut [Instruction],
    instruction: usize,
    target: usize,
) -> Result<()> {
    match instructions.get_mut(instruction) {
        Some(Instruction::IntegerForStart { end, .. }) => {
            *end = target;
            Ok(())
        }
        _ => Err(DbError::internal(
            "PL/pgSQL compiler integer FOR patch target is invalid",
        )),
    }
}

fn patch_foreach_end(
    instructions: &mut [Instruction],
    instruction: usize,
    target: usize,
) -> Result<()> {
    match instructions.get_mut(instruction) {
        Some(Instruction::ForeachStart { end, .. }) => {
            *end = target;
            Ok(())
        }
        _ => Err(DbError::internal(
            "PL/pgSQL compiler FOREACH patch target is invalid",
        )),
    }
}

fn patch_target(
    instructions: &mut [Instruction],
    instruction: Option<usize>,
    target: usize,
) -> Result<()> {
    let Some(instruction) = instruction else {
        return Ok(());
    };
    match instructions.get_mut(instruction) {
        Some(Instruction::JumpIfFalse {
            target: destination,
            ..
        })
        | Some(Instruction::Jump {
            target: destination,
        }) => {
            *destination = target;
            Ok(())
        }
        _ => Err(DbError::internal(
            "PL/pgSQL compiler patch target is not a jump",
        )),
    }
}

fn checked_target(target: usize, instruction_count: usize) -> Result<usize> {
    if target <= instruction_count {
        Ok(target)
    } else {
        Err(DbError::internal(
            "PL/pgSQL bytecode jump target is outside the program",
        ))
    }
}

fn ensure_nesting<T>(controls: &[T], limits: ResourceLimits) -> Result<()> {
    if controls.len() >= limits.max_nesting {
        limit_error("PL/pgSQL nesting exceeds the configured limit")
    } else {
        Ok(())
    }
}

fn ensure_instruction_limit(instructions: &[Instruction], limits: ResourceLimits) -> Result<()> {
    if instructions.len() > limits.max_instructions {
        limit_error("PL/pgSQL bytecode exceeds the configured instruction limit")
    } else {
        Ok(())
    }
}

fn syntax_error<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::new("42601", message))
}

fn unsupported_feature<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::new("0A000", message))
}

fn limit_error<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::new("54001", message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordadb_types::{Batch, Field, PgArray, Row, ScalarType, Schema};

    struct Host {
        cancelled: bool,
    }

    fn row_pair_schema(second_field: &str) -> Schema {
        Schema::new(vec![
            Field::new("id", ScalarType::Int64, false),
            Field::new(second_field, ScalarType::Text, false),
        ])
    }

    impl PlpgsqlHost for Host {
        fn execute_sql(
            &mut self,
            sql: &str,
            _parameters: &[Value],
        ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>>>> {
            let events = if sql == "FAIL" {
                vec![Err(DbError::new("23505", "test unique violation"))]
            } else if sql == "SELECT none" {
                Vec::new()
            } else if matches!(
                sql,
                "SELECT id, name FROM row_pair" | "SELECT id, label FROM wrong_pair"
            ) {
                let second_field = if sql.contains("wrong_pair") {
                    "label"
                } else {
                    "name"
                };
                let schema = row_pair_schema(second_field);
                vec![
                    Ok(QueryEvent::Schema(schema.clone())),
                    Ok(QueryEvent::Batch(Batch {
                        schema,
                        rows: vec![Row::new(vec![Value::Int64(7), Value::Text("seven".into())])],
                    })),
                ]
            } else if sql == "SELECT many" {
                vec![Ok(QueryEvent::Batch(Batch {
                    schema: Schema::empty(),
                    rows: vec![
                        Row::new(vec![Value::Int64(1)]),
                        Row::new(vec![Value::Int64(2)]),
                    ],
                }))]
            } else if sql.starts_with("SELECT") {
                vec![Ok(QueryEvent::Batch(Batch {
                    schema: Schema::empty(),
                    rows: vec![Row::new(vec![Value::Int64(9)])],
                }))]
            } else {
                Vec::new()
            };
            Ok(Box::new(events.into_iter()))
        }

        fn evaluate_expression(&mut self, sql: &str, parameters: &[Value]) -> Result<Value> {
            match sql.trim() {
                "TRUE" | "true" => Ok(Value::Boolean(true)),
                "FALSE" | "false" => Ok(Value::Boolean(false)),
                "$1" => Ok(parameters.first().cloned().unwrap_or(Value::Null)),
                "$2" => Ok(parameters.get(1).cloned().unwrap_or(Value::Null)),
                "($1) = (1)" => Ok(Value::Boolean(parameters.first() == Some(&Value::Int64(1)))),
                "($1) = (2)" => Ok(Value::Boolean(parameters.first() == Some(&Value::Int64(2)))),
                "$1 = 2" => Ok(Value::Boolean(parameters.first() == Some(&Value::Int64(2)))),
                "$1 = 1" => Ok(Value::Boolean(parameters.first() == Some(&Value::Int64(1)))),
                "$1 = 4" => Ok(Value::Boolean(parameters.first() == Some(&Value::Int64(4)))),
                "0" => Ok(Value::Int64(0)),
                "1" => Ok(Value::Int64(1)),
                "2" => Ok(Value::Int64(2)),
                "3" => Ok(Value::Int64(3)),
                "4" => Ok(Value::Int64(4)),
                "5" => Ok(Value::Int64(5)),
                other if other.parse::<i64>().is_ok() => {
                    Ok(Value::Int64(other.parse::<i64>().map_err(|_| {
                        DbError::new("22003", "integer literal is out of range")
                    })?))
                }
                "ARRAY_TEST" => Ok(Value::Array(PgArray::one_dimensional(
                    ScalarType::Int64,
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                )?)),
                "'SELECT 1'" => Ok(Value::Text("SELECT 1".into())),
                other if other.starts_with('\'') && other.ends_with('\'') && other.len() >= 2 => {
                    Ok(Value::Text(other[1..other.len() - 1].to_owned()))
                }
                other if other.starts_with('$') => {
                    let index = other[1..]
                        .parse::<usize>()
                        .ok()
                        .and_then(|index| index.checked_sub(1))
                        .ok_or_else(|| DbError::new("42P02", "invalid positional parameter"))?;
                    Ok(parameters.get(index).cloned().unwrap_or(Value::Null))
                }
                other => Err(DbError::new(
                    "0A000",
                    format!("test host cannot evaluate {other}"),
                )),
            }
        }

        fn resolve_row_type(&mut self, relation: &str) -> Result<Vec<String>> {
            if relation.eq_ignore_ascii_case("public.items") {
                Ok(vec!["id".into(), "name".into()])
            } else {
                Err(DbError::new(
                    "42P01",
                    format!("relation {relation} does not exist"),
                ))
            }
        }

        fn check_cancelled(&self) -> Result<()> {
            if self.cancelled {
                Err(DbError::new("57014", "query was cancelled"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn compiles_and_executes_explicit_control_flow() {
        let program = compile(
            "DECLARE
             answer BIGINT := 1;
             BEGIN
             IF true THEN
             answer := 2;
             ELSE
             answer := 1;
             END IF;
             RETURN answer;
             END;",
        )
        .expect("compile");
        let output = execute(&program, &mut Host { cancelled: false }, &[]).expect("execute");
        assert_eq!(output.return_value, Some(Value::Int64(2)));
    }

    #[test]
    fn resumable_vm_yields_sql_and_preserves_exception_state() {
        let program = compile(
            "BEGIN
             BEGIN
             FAIL;
             EXCEPTION
             WHEN SQLSTATE '23505' THEN
             RETURN 4;
             END;
             END;",
        )
        .expect("compile");
        let mut host = Host { cancelled: false };
        let mut machine =
            VmMachine::new(&program, &mut host, &[], ResourceLimits::default()).expect("create VM");
        let VmRunState::Sql(request) = machine.resume(&mut host, None).expect("yield SQL") else {
            panic!("expected SQL yield");
        };
        assert_eq!(request.sql, "FAIL");
        let response = host.execute_sql(&request.sql, &request.parameters);
        let VmRunState::Complete(output) = machine
            .resume(&mut host, Some(response))
            .expect("resume through exception handler")
        else {
            panic!("expected completion");
        };
        assert_eq!(output.return_value, Some(Value::Int64(4)));
        assert_eq!(
            machine
                .resume(&mut host, None)
                .expect_err("completed VM cannot resume")
                .sql_state,
            "55000"
        );
    }

    #[test]
    fn compiles_block_introducers_without_line_breaks() {
        let program =
            compile("DECLARE answer BIGINT := 1; BEGIN RETURN answer; END;").expect("compile");
        let output = execute(&program, &mut Host { cancelled: false }, &[]).expect("execute");
        assert_eq!(output.return_value, Some(Value::Int64(1)));
    }

    #[test]
    fn multiline_sql_inside_parentheses_is_one_instruction() {
        let program = compile_with_arguments(
            "BEGIN
             INSERT INTO audit VALUES (
               tg_op,
               tg_name
             );
             RETURN NULL;
             END;",
            &["tg_op".into(), "tg_name".into()],
        )
        .expect("compile multiline SQL");
        let sql = program
            .instructions
            .iter()
            .find_map(|instruction| match instruction {
                Instruction::ExecuteSql { sql, into: None } => Some(sql.as_str()),
                _ => None,
            })
            .expect("SQL instruction");
        assert!(sql.starts_with("INSERT INTO audit VALUES ("), "{sql}");
        assert!(sql.contains("$1"), "{sql}");
        assert!(sql.contains("$2"), "{sql}");
    }

    #[test]
    fn perform_compiles_expression_as_a_select_statement() {
        let program = compile_with_arguments(
            "BEGIN PERFORM pg_notify('core_events', event_payload); END;",
            &["event_payload".into()],
        )
        .expect("compile PERFORM");
        let sql = program
            .instructions
            .iter()
            .find_map(|instruction| match instruction {
                Instruction::ExecuteSql { sql, into: None } => Some(sql.as_str()),
                _ => None,
            })
            .expect("PERFORM SQL instruction");
        assert_eq!(sql, "SELECT pg_notify('core_events', $1)");
    }

    #[test]
    fn select_into_dynamic_using_and_limits_are_bounded() {
        let program = compile(
            "DECLARE
             value BIGINT;
             BEGIN
             SELECT 9 INTO value;
             EXECUTE 'SELECT 1' INTO STRICT value USING value;
             RETURN NEXT value;
             RETURN;
             END;",
        )
        .expect("compile");
        let output = execute(&program, &mut Host { cancelled: false }, &[]).expect("execute");
        assert_eq!(output.returned_rows, vec![Value::Int64(9)]);

        let no_rows = compile(
            "DECLARE value BIGINT;
             BEGIN
             EXECUTE 'SELECT none' INTO STRICT value;
             RETURN value;
             END;",
        )
        .expect("compile no-row strict execute");
        assert_eq!(
            execute(&no_rows, &mut Host { cancelled: false }, &[])
                .expect_err("strict no rows")
                .sql_state,
            "P0002"
        );

        let many_rows = compile(
            "DECLARE value BIGINT;
             BEGIN
             EXECUTE 'SELECT many' INTO STRICT value;
             RETURN value;
             END;",
        )
        .expect("compile many-row strict execute");
        assert_eq!(
            execute(&many_rows, &mut Host { cancelled: false }, &[])
                .expect_err("strict many rows")
                .sql_state,
            "P0003"
        );

        let limits = ResourceLimits {
            max_source_bytes: 4,
            ..ResourceLimits::default()
        };
        assert_eq!(
            compile_with_limits("BEGIN RETURN; END;", limits)
                .expect_err("source limit")
                .sql_state,
            "54001"
        );
        let _ = Schema::empty();
    }

    #[test]
    fn false_branches_case_query_for_and_exception_handlers_are_explicit() {
        let false_branch = compile(
            "BEGIN
             IF false THEN
             RETURN 1;
             ELSE
             RETURN 2;
             END IF;
             END;",
        )
        .expect("compile false branch");
        assert_eq!(
            execute(&false_branch, &mut Host { cancelled: false }, &[])
                .expect("execute false branch")
                .return_value,
            Some(Value::Int64(2))
        );

        let query_for = compile(
            "DECLARE
             item BIGINT;
             answer BIGINT := 3;
             BEGIN
             FOR item IN SELECT many LOOP
             CASE item
             WHEN 1 THEN
             answer := 1;
             WHEN 2 THEN
             answer := 2;
             ELSE
             answer := 3;
             END CASE;
             END LOOP;
             RETURN answer;
             END;",
        )
        .expect("compile query for");
        assert_eq!(
            execute(&query_for, &mut Host { cancelled: false }, &[])
                .expect("execute query for")
                .return_value,
            Some(Value::Int64(2))
        );

        let exception = compile(
            "BEGIN
             FAIL;
             EXCEPTION
             WHEN SQLSTATE '23505' THEN
             RETURN 2;
             WHEN OTHERS THEN
             RETURN 3;
             END;",
        )
        .expect("compile exception");
        assert_eq!(
            execute(&exception, &mut Host { cancelled: false }, &[])
                .expect("execute exception")
                .return_value,
            Some(Value::Int64(2))
        );
        assert_eq!(
            execute(&exception, &mut Host { cancelled: true }, &[])
                .expect_err("cancellation bypasses exception handlers")
                .sql_state,
            "57014"
        );
    }

    #[test]
    fn integer_for_reverse_by_and_conditional_loop_control_are_explicit() {
        let forward = compile(
            "DECLARE
             item BIGINT;
             answer BIGINT;
             BEGIN
             FOR item IN 1..5 BY 1 LOOP
             CONTINUE WHEN item = 2;
             EXIT WHEN item = 4;
             answer := item;
             END LOOP;
             RETURN answer;
             END;",
        )
        .expect("compile integer FOR");
        assert_eq!(
            execute(&forward, &mut Host { cancelled: false }, &[])
                .expect("execute integer FOR")
                .return_value,
            Some(Value::Int64(3))
        );

        let reverse = compile(
            "DECLARE
             item BIGINT;
             answer BIGINT;
             BEGIN
             FOR item IN REVERSE 3..1 BY 1 LOOP
             answer := item;
             END LOOP;
             RETURN answer;
             END;",
        )
        .expect("compile reverse integer FOR");
        assert_eq!(
            execute(&reverse, &mut Host { cancelled: false }, &[])
                .expect("execute reverse integer FOR")
                .return_value,
            Some(Value::Int64(1))
        );

        let invalid_step = compile(
            "DECLARE item BIGINT;
             BEGIN
             FOR item IN 1..3 BY 0 LOOP
             RETURN item;
             END LOOP;
             END;",
        )
        .expect("compile invalid step");
        assert_eq!(
            execute(&invalid_step, &mut Host { cancelled: false }, &[])
                .expect_err("zero step")
                .sql_state,
            "22023"
        );
    }

    #[test]
    fn labeled_nested_loops_patch_exit_and_continue_to_the_named_frame() {
        let program = compile(
            "DECLARE
             outer_value BIGINT;
             inner_value BIGINT;
             answer BIGINT := 0;
             BEGIN
             <<outer_loop>>
             FOR outer_value IN 1..3 LOOP
             <<inner_loop>>
             FOR inner_value IN 1..3 LOOP
             CONTINUE outer_loop WHEN outer_value = 1;
             EXIT outer_loop WHEN outer_value = 2;
             answer := 99;
             END LOOP inner_loop;
             END LOOP outer_loop;
             RETURN outer_value;
             END;",
        )
        .expect("compile labeled loops");
        assert_eq!(
            execute(&program, &mut Host { cancelled: false }, &[])
                .expect("execute labeled loops")
                .return_value,
            Some(Value::Int64(2))
        );

        assert_eq!(
            compile(
                "BEGIN
                 <<actual_loop>>
                 LOOP
                 EXIT;
                 END LOOP wrong_loop;
                 END;",
            )
            .expect_err("mismatched closing label")
            .sql_state,
            "42601"
        );
        assert_eq!(
            compile(
                "BEGIN
                 LOOP
                 EXIT missing_loop;
                 END LOOP;
                 END;",
            )
            .expect_err("missing loop label")
            .sql_state,
            "42601"
        );
    }

    #[test]
    fn labeled_blocks_patch_exit_and_validate_closing_labels() {
        let program = compile(
            "<<outer_block>>
             DECLARE
             answer BIGINT := 1;
             BEGIN
             <<inner_block>>
             BEGIN
             answer := 2;
             EXIT outer_block WHEN true;
             answer := 99;
             END inner_block;
             answer := 100;
             END outer_block;",
        )
        .expect("compile labeled blocks");
        let output =
            execute(&program, &mut Host { cancelled: false }, &[]).expect("execute labeled blocks");
        assert_eq!(output.final_locals.first(), Some(&Value::Int64(2)));

        assert_eq!(
            compile(
                "<<actual_block>>
                 BEGIN
                 END wrong_block;",
            )
            .expect_err("mismatched block label")
            .sql_state,
            "42601"
        );
        assert_eq!(
            compile(
                "<<actual_block>>
                 BEGIN
                 CONTINUE actual_block;
                 END actual_block;",
            )
            .expect_err("block label cannot be a continue target")
            .sql_state,
            "42601"
        );
    }

    #[test]
    fn nested_declare_blocks_restore_outer_variable_bindings() {
        let program = compile(
            "DECLARE
             scoped_value BIGINT := 1;
             BEGIN
             DECLARE
             scoped_value BIGINT := 2;
             BEGIN
             scoped_value := 3;
             RETURN NEXT scoped_value;
             END;
             RETURN scoped_value;
             END;",
        )
        .expect("compile nested declaration scope");
        let output = execute(&program, &mut Host { cancelled: false }, &[])
            .expect("execute nested declaration scope");
        assert_eq!(output.returned_rows, vec![Value::Int64(3)]);
        assert_eq!(output.return_value, Some(Value::Int64(1)));

        assert_eq!(
            compile(
                "BEGIN
                 DECLARE
                 duplicate_value BIGINT;
                 duplicate_value BIGINT;
                 BEGIN
                 RETURN;
                 END;
                 END;",
            )
            .expect_err("duplicate nested declaration")
            .sql_state,
            "42710"
        );
    }

    #[test]
    fn foreach_array_uses_owned_iterator_state_and_rejects_non_arrays() {
        let foreach = compile(
            "DECLARE
             item BIGINT;
             answer BIGINT;
             BEGIN
             FOREACH item IN ARRAY ARRAY_TEST LOOP
             answer := item;
             END LOOP;
             RETURN answer;
             END;",
        )
        .expect("compile FOREACH");
        assert_eq!(
            execute(&foreach, &mut Host { cancelled: false }, &[])
                .expect("execute FOREACH")
                .return_value,
            Some(Value::Int64(3))
        );

        let non_array = compile(
            "DECLARE item BIGINT;
             BEGIN
             FOREACH item IN ARRAY 1 LOOP
             RETURN item;
             END LOOP;
             END;",
        )
        .expect("compile non-array FOREACH");
        assert_eq!(
            execute(&non_array, &mut Host { cancelled: false }, &[])
                .expect_err("non-array FOREACH")
                .sql_state,
            "42804"
        );
    }

    #[test]
    fn raise_assert_and_handler_diagnostics_preserve_sqlstate() {
        let diagnostics = compile(
            "BEGIN
             RAISE EXCEPTION 'duplicate' USING ERRCODE = '23505';
             EXCEPTION
             WHEN unique_violation THEN
             RETURN sqlerrm;
             END;",
        )
        .expect("compile named exception handler");
        assert_eq!(
            execute(&diagnostics, &mut Host { cancelled: false }, &[])
                .expect("handle raised exception")
                .return_value,
            Some(Value::Text("duplicate".into()))
        );

        let sqlstate = compile(
            "BEGIN
             FAIL;
             EXCEPTION
             WHEN SQLSTATE '23505' THEN
             RETURN sqlstate;
             END;",
        )
        .expect("compile SQLSTATE diagnostic");
        assert_eq!(
            execute(&sqlstate, &mut Host { cancelled: false }, &[])
                .expect("read SQLSTATE")
                .return_value,
            Some(Value::Text("23505".into()))
        );

        let rethrow = compile(
            "BEGIN
             FAIL;
             EXCEPTION
             WHEN unique_violation THEN
             RAISE;
             END;",
        )
        .expect("compile rethrow");
        assert_eq!(
            execute(&rethrow, &mut Host { cancelled: false }, &[])
                .expect_err("rethrow active exception")
                .sql_state,
            "23505"
        );

        let assertion = compile(
            "BEGIN
             ASSERT false, 'invariant failed';
             EXCEPTION
             WHEN OTHERS THEN
             RETURN 1;
             END;",
        )
        .expect("compile assertion");
        let error = execute(&assertion, &mut Host { cancelled: false }, &[])
            .expect_err("OTHERS does not catch assertion failures");
        assert_eq!(error.sql_state, "P0004");
        assert_eq!(error.message, "invariant failed");
    }

    #[test]
    fn nested_exception_blocks_select_the_innermost_matching_handler() {
        let outer_fallback = compile(
            "BEGIN
             BEGIN
             RAISE EXCEPTION 'inner' USING ERRCODE = '23505';
             EXCEPTION
             WHEN division_by_zero THEN
             RETURN 1;
             END;
             EXCEPTION
             WHEN unique_violation THEN
             RETURN sqlstate;
             END;",
        )
        .expect("compile nested outer fallback");
        assert_eq!(
            execute(&outer_fallback, &mut Host { cancelled: false }, &[])
                .expect("outer handler")
                .return_value,
            Some(Value::Text("23505".into()))
        );

        let inner_match = compile(
            "DECLARE answer BIGINT;
             BEGIN
             BEGIN
             RAISE EXCEPTION 'inner' USING ERRCODE = '23505';
             EXCEPTION
             WHEN unique_violation THEN
             answer := 2;
             END;
             RETURN answer;
             END;",
        )
        .expect("compile nested inner match");
        assert_eq!(
            execute(&inner_match, &mut Host { cancelled: false }, &[])
                .expect("inner handler")
                .return_value,
            Some(Value::Int64(2))
        );
    }

    #[test]
    fn step_dynamic_sql_query_loop_and_returned_row_limits_fail_explicitly() {
        let loop_program = compile(
            "BEGIN
             LOOP
             CONTINUE;
             END LOOP;
             END;",
        )
        .expect("compile loop program");
        let step_error = execute_with_limits(
            &loop_program,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_steps: 4,
                ..ResourceLimits::default()
            },
        )
        .expect_err("step limit");
        assert_eq!(step_error.sql_state, "54001");

        let dynamic_program = compile(
            "BEGIN
             EXECUTE 'SELECT 1';
             END;",
        )
        .expect("compile dynamic SQL");
        let dynamic_error = execute_with_limits(
            &dynamic_program,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_dynamic_sql_bytes: 4,
                ..ResourceLimits::default()
            },
        )
        .expect_err("dynamic SQL byte limit");
        assert_eq!(dynamic_error.sql_state, "54001");

        let query_loop = compile(
            "DECLARE
             item BIGINT;
             BEGIN
             FOR item IN SELECT many LOOP
             CONTINUE;
             END LOOP;
             END;",
        )
        .expect("compile query loop");
        let query_loop_error = execute_with_limits(
            &query_loop,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_returned_rows: 1,
                ..ResourceLimits::default()
            },
        )
        .expect_err("query loop row limit");
        assert_eq!(query_loop_error.sql_state, "54001");

        let early_exit = compile(
            "DECLARE
             item BIGINT;
             BEGIN
             FOR item IN SELECT many LOOP
             EXIT;
             END LOOP;
             RETURN item;
             END;",
        )
        .expect("compile early-exit query loop");
        let early_exit_output = execute_with_limits(
            &early_exit,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_returned_rows: 1,
                ..ResourceLimits::default()
            },
        )
        .expect("early exit does not drain the query cursor");
        assert_eq!(early_exit_output.return_value, Some(Value::Int64(1)));

        let returns = compile(
            "BEGIN
             RETURN NEXT 1;
             RETURN NEXT 2;
             RETURN;
             END;",
        )
        .expect("compile returned rows");
        let return_error = execute_with_limits(
            &returns,
            &mut Host { cancelled: false },
            &[],
            ResourceLimits {
                max_returned_rows: 1,
                ..ResourceLimits::default()
            },
        )
        .expect_err("returned row limit");
        assert_eq!(return_error.sql_state, "54001");
    }

    #[test]
    fn declared_cursor_supports_every_direction_with_bounded_owned_rows() {
        let program = compile(
            "DECLARE
             values_cursor SCROLL CURSOR FOR SELECT many;
             item BIGINT;
             BEGIN
             OPEN values_cursor;
             FETCH NEXT FROM values_cursor INTO item;
             RETURN NEXT item;
             FETCH LAST FROM values_cursor INTO item;
             RETURN NEXT item;
             FETCH PRIOR FROM values_cursor INTO item;
             RETURN NEXT item;
             FETCH FIRST FROM values_cursor INTO item;
             RETURN NEXT item;
             FETCH ABSOLUTE 2 FROM values_cursor INTO item;
             RETURN NEXT item;
             FETCH RELATIVE -1 FROM values_cursor INTO item;
             RETURN NEXT item;
             MOVE FORWARD 1 FROM values_cursor;
             FETCH BACKWARD 1 FROM values_cursor INTO item;
             RETURN NEXT item;
             MOVE FORWARD ALL FROM values_cursor;
             FETCH PRIOR FROM values_cursor INTO item;
             RETURN NEXT item;
             CLOSE values_cursor;
             RETURN;
             END;",
        )
        .expect("compile directional cursor");

        let output = execute(&program, &mut Host { cancelled: false }, &[])
            .expect("execute directional cursor");
        assert_eq!(
            output.returned_rows,
            vec![1, 2, 1, 1, 2, 1, 1, 2]
                .into_iter()
                .map(Value::Int64)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn unbound_cursor_opens_static_and_dynamic_queries_and_enforces_limits() {
        let program = compile(
            "DECLARE
             values_cursor REFCURSOR;
             item BIGINT;
             query_text TEXT := 'SELECT many';
             BEGIN
             OPEN values_cursor FOR EXECUTE query_text USING 1;
             FETCH FIRST FROM values_cursor INTO item;
             RETURN NEXT item;
             CLOSE values_cursor;
             OPEN values_cursor FOR SELECT many;
             FETCH LAST FROM values_cursor INTO item;
             RETURN NEXT item;
             CLOSE values_cursor;
             RETURN;
             END;",
        )
        .expect("compile unbound cursor");
        let output =
            execute(&program, &mut Host { cancelled: false }, &[]).expect("execute unbound cursor");
        assert_eq!(output.returned_rows, vec![Value::Int64(1), Value::Int64(2)]);

        let too_many_open = compile(
            "DECLARE
             first_cursor CURSOR FOR SELECT many;
             second_cursor CURSOR FOR SELECT many;
             BEGIN
             OPEN first_cursor;
             OPEN second_cursor;
             END;",
        )
        .expect("compile open cursor limit");
        assert_eq!(
            execute_with_limits(
                &too_many_open,
                &mut Host { cancelled: false },
                &[],
                ResourceLimits {
                    max_open_cursors: 1,
                    ..ResourceLimits::default()
                },
            )
            .expect_err("open cursor limit")
            .sql_state,
            "54000"
        );

        let retained_memory = compile(
            "DECLARE
             values_cursor CURSOR FOR SELECT many;
             item BIGINT;
             BEGIN
             OPEN values_cursor;
             FETCH NEXT FROM values_cursor INTO item;
             END;",
        )
        .expect("compile cursor memory limit");
        assert_eq!(
            execute_with_limits(
                &retained_memory,
                &mut Host { cancelled: false },
                &[],
                ResourceLimits {
                    max_cursor_bytes: 1,
                    ..ResourceLimits::default()
                },
            )
            .expect_err("cursor retained-memory limit")
            .sql_state,
            "53200"
        );
    }

    #[test]
    fn directional_cursor_spills_and_removes_its_owned_page_store() {
        let limits = ResourceLimits {
            max_cursor_bytes: 256,
            ..ResourceLimits::default()
        };
        let mut store = CursorPageStore::Memory {
            rows: Vec::new(),
            bytes: 0,
        };
        let first = ordadb_types::Row::new(vec![Value::Text("a".repeat(64))]);
        let second = ordadb_types::Row::new(vec![Value::Text("b".repeat(64))]);
        store.push(first.clone(), limits).expect("first value");
        store.push(second.clone(), limits).expect("spill value");
        let spill_path = match &store {
            CursorPageStore::Spilled(spill) => spill.file.path().to_path_buf(),
            CursorPageStore::Memory { .. } => panic!("cursor did not spill"),
        };
        assert_eq!(store.get(0, limits).expect("first read"), Some(first));
        assert_eq!(store.get(1, limits).expect("second read"), Some(second));
        assert!(spill_path.exists());
        drop(store);
        assert!(!spill_path.exists());
    }

    #[test]
    fn record_and_rowtype_rows_flow_through_select_loops_and_cursors() {
        let record = compile(
            "DECLARE
             source RECORD;
             copied RECORD;
             BEGIN
             SELECT id, name INTO source FROM row_pair;
             source.name := 'updated';
             copied := source;
             RETURN copied.name;
             END;",
        )
        .expect("compile record assignment");
        assert_eq!(
            execute(&record, &mut Host { cancelled: false }, &[])
                .expect("execute record assignment")
                .return_value,
            Some(Value::Text("updated".into()))
        );

        let query_loop = compile(
            "DECLARE
             item RECORD;
             answer TEXT;
             BEGIN
             FOR item IN SELECT id, name FROM row_pair LOOP
             answer := item.name;
             END LOOP;
             RETURN answer;
             END;",
        )
        .expect("compile record query loop");
        assert_eq!(
            execute(&query_loop, &mut Host { cancelled: false }, &[])
                .expect("execute record query loop")
                .return_value,
            Some(Value::Text("seven".into()))
        );

        let rowtype_cursor = compile(
            "DECLARE
             values_cursor CURSOR FOR SELECT id, name FROM row_pair;
             item public.items%ROWTYPE;
             BEGIN
             OPEN values_cursor;
             FETCH NEXT FROM values_cursor INTO item;
             CLOSE values_cursor;
             RETURN item.name;
             END;",
        )
        .expect("compile rowtype cursor");
        assert_eq!(
            execute(&rowtype_cursor, &mut Host { cancelled: false }, &[])
                .expect("execute rowtype cursor")
                .return_value,
            Some(Value::Text("seven".into()))
        );

        let unassigned = compile(
            "DECLARE
             item RECORD;
             BEGIN
             RETURN item.id;
             END;",
        )
        .expect("compile unassigned record");
        assert_eq!(
            execute(&unassigned, &mut Host { cancelled: false }, &[])
                .expect_err("unassigned record field")
                .sql_state,
            "55000"
        );

        let mismatched = compile(
            "DECLARE
             item public.items%ROWTYPE;
             BEGIN
             SELECT id, label INTO item FROM wrong_pair;
             END;",
        )
        .expect("compile mismatched rowtype");
        assert_eq!(
            execute(&mismatched, &mut Host { cancelled: false }, &[])
                .expect_err("rowtype field mismatch")
                .sql_state,
            "42804"
        );

        assert_eq!(
            execute_with_limits(
                &record,
                &mut Host { cancelled: false },
                &[],
                ResourceLimits {
                    max_cursor_bytes: 1,
                    ..ResourceLimits::default()
                },
            )
            .expect_err("record retained-memory limit")
            .sql_state,
            "53200"
        );
    }

    #[test]
    fn nested_vm_frames_share_one_raii_retained_memory_grant() {
        let program = compile_with_arguments("BEGIN RETURN $1; END;", &["payload".to_owned()])
            .expect("compile routine");
        let limits = ResourceLimits {
            max_cursor_bytes: 256 * 1024,
            ..ResourceLimits::default()
        };
        let grant = VmMemoryGrant::new(limits.max_cursor_bytes).expect("memory grant");
        let payload = Value::Text("x".repeat(150 * 1024));
        let mut first_host = Host { cancelled: false };
        let first = VmMachine::new_with_memory_grant(
            &program,
            &mut first_host,
            std::slice::from_ref(&payload),
            limits,
            grant.clone(),
        )
        .expect("first frame");
        let first_bytes = grant.current_bytes();
        assert!(first_bytes > 150 * 1024);

        let mut second_host = Host { cancelled: false };
        let error = match VmMachine::new_with_memory_grant(
            &program,
            &mut second_host,
            std::slice::from_ref(&payload),
            limits,
            grant.clone(),
        ) {
            Ok(_) => panic!("nested frame must share the hard limit"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "53200");
        assert_eq!(grant.current_bytes(), first_bytes);
        drop(first);
        assert_eq!(grant.current_bytes(), 0);

        let replacement = VmMachine::new_with_memory_grant(
            &program,
            &mut second_host,
            &[payload],
            limits,
            grant.clone(),
        )
        .expect("released bytes are reusable");
        drop(replacement);
        assert_eq!(grant.current_bytes(), 0);
        assert!(grant.peak_bytes() <= grant.hard_limit_bytes());
    }

    #[test]
    fn completed_output_holds_its_raii_reservation_until_drop() {
        let program = compile_with_arguments("BEGIN RETURN $1; END;", &["payload".to_owned()])
            .expect("compile routine");
        let limits = ResourceLimits {
            max_cursor_bytes: 256 * 1024,
            ..ResourceLimits::default()
        };
        let grant = VmMemoryGrant::new(limits.max_cursor_bytes).expect("memory grant");
        let mut host = Host { cancelled: false };
        let mut machine = VmMachine::new_with_memory_grant(
            &program,
            &mut host,
            &[Value::Text("x".repeat(64 * 1024))],
            limits,
            grant.clone(),
        )
        .expect("create VM");
        let VmRunState::Complete(output) = machine.resume(&mut host, None).expect("complete VM")
        else {
            panic!("routine unexpectedly yielded SQL");
        };
        assert!(grant.current_bytes() > 64 * 1024);
        drop(machine);
        assert!(grant.current_bytes() > 64 * 1024);
        drop(output);
        assert_eq!(grant.current_bytes(), 0);
    }

    #[test]
    fn cancellation_and_runtime_memory_errors_release_the_shared_grant() {
        let program = compile_with_arguments(
            "BEGIN RETURN NEXT $1; RETURN NEXT $1; END;",
            &["payload".to_owned()],
        )
        .expect("compile routine");
        let limits = ResourceLimits {
            max_cursor_bytes: 192 * 1024,
            ..ResourceLimits::default()
        };
        let payload = Value::Text("x".repeat(100 * 1024));

        let cancelled_grant = VmMemoryGrant::new(limits.max_cursor_bytes).expect("memory grant");
        let mut cancelled_host = Host { cancelled: false };
        let mut cancelled = VmMachine::new_with_memory_grant(
            &program,
            &mut cancelled_host,
            std::slice::from_ref(&payload),
            limits,
            cancelled_grant.clone(),
        )
        .expect("create cancelled VM");
        cancelled_host.cancelled = true;
        assert_eq!(
            cancelled
                .resume(&mut cancelled_host, None)
                .expect_err("cancel VM")
                .sql_state,
            "57014"
        );
        assert_eq!(cancelled_grant.current_bytes(), 0);

        let limited_grant = VmMemoryGrant::new(limits.max_cursor_bytes).expect("memory grant");
        let mut limited_host = Host { cancelled: false };
        let mut limited = VmMachine::new_with_memory_grant(
            &program,
            &mut limited_host,
            &[payload],
            limits,
            limited_grant.clone(),
        )
        .expect("create limited VM");
        assert_eq!(
            limited
                .resume(&mut limited_host, None)
                .expect_err("runtime memory limit")
                .sql_state,
            "53200"
        );
        assert_eq!(limited_grant.current_bytes(), 0);
    }
}
