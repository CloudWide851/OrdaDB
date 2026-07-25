//! SQL execution and persistent snapshot publication for OrdaDB.
//!
//! This crate owns SQL semantics and candidate-state atomicity. Physical page
//! encoding belongs to `ordadb-storage`; WAL and crash recovery remain later
//! milestones.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use ordadb_catalog::{Catalog, TableDefinition};
use ordadb_sql::{
    BinaryOperator, BoundExpr, BoundExprKind, BoundOrder, BoundProjection, BoundStatement,
    UnaryOperator, bind, parse,
};
use ordadb_storage::{DatabaseStore, PersistentState};
use ordadb_types::{
    Batch, CommandComplete, DbError, QueryEvent, QueryProgress, Result, Row, ScalarType, Schema,
    TableId, Value,
};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
}

impl EngineConfig {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Engine {
    config: Arc<EngineConfig>,
    state: Arc<RwLock<DatabaseState>>,
    store: Arc<Mutex<DatabaseStore>>,
}

impl Engine {
    pub fn open(config: EngineConfig) -> Result<Self> {
        let store = DatabaseStore::open(&config.data_dir)?;
        let state = DatabaseState::from(store.committed_state().clone());
        Ok(Self {
            config: Arc::new(config),
            state: Arc::new(RwLock::new(state)),
            store: Arc::new(Mutex::new(store)),
        })
    }

    pub fn connect(&self) -> Result<Session> {
        Ok(Session {
            state: Arc::clone(&self.state),
            store: Arc::clone(&self.store),
        })
    }

    #[must_use]
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }
}

#[derive(Debug)]
pub struct Session {
    state: Arc<RwLock<DatabaseState>>,
    store: Arc<Mutex<DatabaseStore>>,
}

impl Session {
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let (mut candidate, events, dirty) = execute_candidate(&state, sql, params)?;
        if dirty {
            candidate.generation = state.generation.saturating_add(1);
            self.store
                .lock()
                .map_err(|_| internal_error("database store lock is poisoned"))?
                .commit(&PersistentState::from(&candidate))?;
            *state = candidate;
        }
        Ok(QueryStream::new(events))
    }

    pub fn begin(&mut self) -> Result<Transaction<'_>> {
        let state = self
            .state
            .read()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let base_generation = state.generation;
        let working = state.clone();
        drop(state);
        Ok(Transaction {
            state: &self.state,
            store: &self.store,
            working,
            base_generation,
            dirty: false,
        })
    }
}

#[derive(Debug)]
pub struct Transaction<'session> {
    state: &'session Arc<RwLock<DatabaseState>>,
    store: &'session Arc<Mutex<DatabaseStore>>,
    working: DatabaseState,
    base_generation: u64,
    dirty: bool,
}

impl Transaction<'_> {
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        let (candidate, events, dirty) = execute_candidate(&self.working, sql, params)?;
        if dirty {
            self.working = candidate;
            self.dirty = true;
        }
        Ok(QueryStream::new(events))
    }

    pub fn commit(mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        if state.generation != self.base_generation {
            return Err(DbError::new(
                "40001",
                "transaction snapshot conflicts with a committed change",
            )
            .with_hint("retry the transaction"));
        }
        self.working.generation = state.generation.saturating_add(1);
        self.store
            .lock()
            .map_err(|_| internal_error("database store lock is poisoned"))?
            .commit(&PersistentState::from(&self.working))?;
        *state = self.working;
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct QueryStream {
    events: std::vec::IntoIter<QueryEvent>,
}

impl QueryStream {
    fn new(events: Vec<QueryEvent>) -> Self {
        Self {
            events: events.into_iter(),
        }
    }
}

impl Iterator for QueryStream {
    type Item = QueryEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.events.next()
    }
}

#[derive(Debug, Clone, Default)]
struct DatabaseState {
    catalog: Catalog,
    rows: BTreeMap<TableId, Vec<Row>>,
    generation: u64,
}

impl From<PersistentState> for DatabaseState {
    fn from(state: PersistentState) -> Self {
        Self {
            catalog: state.catalog,
            rows: state.tables,
            generation: state.generation,
        }
    }
}

impl From<&DatabaseState> for PersistentState {
    fn from(state: &DatabaseState) -> Self {
        Self {
            generation: state.generation,
            catalog: state.catalog.clone(),
            tables: state.rows.clone(),
        }
    }
}

fn execute_candidate(
    state: &DatabaseState,
    sql: &str,
    params: &[Value],
) -> Result<(DatabaseState, Vec<QueryEvent>, bool)> {
    let parsed = parse(sql)?;
    let statement = bind(parsed, &state.catalog)?;
    let mut candidate = state.clone();
    let (events, dirty) = execute_bound(&mut candidate, statement, params)?;
    Ok((candidate, events, dirty))
}

fn execute_bound(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    match statement {
        BoundStatement::CreateSchema { name } => {
            state.catalog.create_schema(name)?;
            Ok((
                command_events(Schema::empty(), "CREATE SCHEMA", 0, None),
                true,
            ))
        }
        BoundStatement::CreateTable {
            schema,
            name,
            columns,
        } => {
            let table_id = state.catalog.create_table(&schema, name, columns)?;
            state.rows.insert(table_id, Vec::new());
            Ok((
                command_events(Schema::empty(), "CREATE TABLE", 0, None),
                true,
            ))
        }
        BoundStatement::Insert {
            table_id,
            column_indexes,
            rows,
        } => execute_insert(state, table_id, column_indexes, rows, params),
        BoundStatement::Select {
            table_id,
            schema,
            projection,
            filter,
            order_by,
            limit,
        } => execute_select(
            state, table_id, schema, projection, filter, order_by, limit, params,
        ),
        BoundStatement::Update {
            table_id,
            assignments,
            filter,
        } => execute_update(state, table_id, assignments, filter, params),
        BoundStatement::Delete { table_id, filter } => {
            execute_delete(state, table_id, filter, params)
        }
    }
}

fn execute_insert(
    state: &mut DatabaseState,
    table_id: TableId,
    column_indexes: Vec<usize>,
    expressions: Vec<Vec<BoundExpr>>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let table = table_definition(state, table_id)?.clone();
    let mut candidate_rows = state.rows.get(&table_id).cloned().unwrap_or_default();
    let inserted = expressions.len() as u64;
    for expressions in expressions {
        let mut values = vec![Value::Null; table.columns().len()];
        for (expression, column_index) in expressions.into_iter().zip(&column_indexes) {
            values[*column_index] = evaluate(&expression, &[], params)?;
        }
        candidate_rows.push(Row::new(values));
    }
    validate_rows(&table, &candidate_rows)?;
    state.rows.insert(table_id, candidate_rows);
    Ok((
        command_events(
            Schema::empty(),
            format!("INSERT 0 {inserted}"),
            inserted,
            None,
        ),
        true,
    ))
}

#[allow(clippy::too_many_arguments)]
fn execute_select(
    state: &DatabaseState,
    table_id: TableId,
    schema: Schema,
    projection: Vec<BoundProjection>,
    filter: Option<BoundExpr>,
    order_by: Vec<BoundOrder>,
    limit: Option<BoundExpr>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let mut source_rows = state.rows.get(&table_id).cloned().unwrap_or_default();
    if let Some(filter) = &filter {
        let mut filtered = Vec::with_capacity(source_rows.len());
        for row in source_rows {
            if predicate_matches(filter, &row, params)? {
                filtered.push(row);
            }
        }
        source_rows = filtered;
    }
    if !order_by.is_empty() {
        let mut sort_error = None;
        source_rows.sort_by(|left, right| {
            compare_rows(left, right, &order_by).unwrap_or_else(|error| {
                sort_error = Some(error);
                Ordering::Equal
            })
        });
        if let Some(error) = sort_error {
            return Err(error);
        }
    }
    let limit = limit
        .as_ref()
        .map(|limit| evaluate_limit(limit, params))
        .transpose()?;
    if let Some(limit) = limit {
        source_rows.truncate(limit);
    }

    let result_rows = source_rows
        .iter()
        .map(|row| {
            projection
                .iter()
                .map(|projection| evaluate(&projection.expr, &row.values, params))
                .collect::<Result<Vec<_>>>()
                .map(Row::new)
        })
        .collect::<Result<Vec<_>>>()?;
    let count = result_rows.len() as u64;
    let batch = Batch {
        schema: schema.clone(),
        rows: result_rows,
    };
    Ok((
        command_events(schema, format!("SELECT {count}"), count, Some(batch)),
        false,
    ))
}

fn execute_update(
    state: &mut DatabaseState,
    table_id: TableId,
    assignments: Vec<(usize, BoundExpr)>,
    filter: Option<BoundExpr>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let table = table_definition(state, table_id)?.clone();
    let mut candidate_rows = state.rows.get(&table_id).cloned().unwrap_or_default();
    let mut updated = 0u64;
    for row in &mut candidate_rows {
        if filter
            .as_ref()
            .map(|filter| predicate_matches(filter, row, params))
            .transpose()?
            .unwrap_or(true)
        {
            let original = row.values.clone();
            let mut replacements = Vec::with_capacity(assignments.len());
            for (column_index, expression) in &assignments {
                replacements.push((*column_index, evaluate(expression, &original, params)?));
            }
            for (column_index, value) in replacements {
                row.values[column_index] = value;
            }
            updated += 1;
        }
    }
    validate_rows(&table, &candidate_rows)?;
    state.rows.insert(table_id, candidate_rows);
    Ok((
        command_events(Schema::empty(), format!("UPDATE {updated}"), updated, None),
        true,
    ))
}

fn execute_delete(
    state: &mut DatabaseState,
    table_id: TableId,
    filter: Option<BoundExpr>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    table_definition(state, table_id)?;
    let rows = state.rows.entry(table_id).or_default();
    let original_len = rows.len();
    if let Some(filter) = &filter {
        let mut error = None;
        rows.retain(|row| match predicate_matches(filter, row, params) {
            Ok(matches) => !matches,
            Err(predicate_error) => {
                error = Some(predicate_error);
                true
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
    } else {
        rows.clear();
    }
    let deleted = (original_len - rows.len()) as u64;
    Ok((
        command_events(Schema::empty(), format!("DELETE {deleted}"), deleted, None),
        true,
    ))
}

fn evaluate(expr: &BoundExpr, row: &[Value], params: &[Value]) -> Result<Value> {
    let value = match &expr.kind {
        BoundExprKind::Column { index } => row
            .get(*index)
            .cloned()
            .ok_or_else(|| internal_error(format!("bound column index {index} is out of range")))?,
        BoundExprKind::Literal(value) => value.clone(),
        BoundExprKind::Parameter { index } => params.get(index - 1).cloned().ok_or_else(|| {
            DbError::new("42P02", format!("no value supplied for parameter ${index}"))
        })?,
        BoundExprKind::Unary { op, expr } => {
            let value = evaluate(expr, row, params)?;
            evaluate_unary(*op, value)?
        }
        BoundExprKind::Binary { left, op, right } => {
            let left = evaluate(left, row, params)?;
            let right = evaluate(right, row, params)?;
            evaluate_binary(left, *op, right)?
        }
    };
    coerce_value(value, &expr.data_type)
}

fn evaluate_unary(operator: UnaryOperator, value: Value) -> Result<Value> {
    match (operator, value) {
        (_, Value::Null) => Ok(Value::Null),
        (UnaryOperator::Not, Value::Boolean(value)) => Ok(Value::Boolean(!value)),
        (UnaryOperator::Negate, Value::Int16(value)) => value
            .checked_neg()
            .map(Value::Int16)
            .ok_or_else(|| DbError::new("22003", "numeric value out of range")),
        (UnaryOperator::Negate, Value::Int32(value)) => value
            .checked_neg()
            .map(Value::Int32)
            .ok_or_else(|| DbError::new("22003", "numeric value out of range")),
        (UnaryOperator::Negate, Value::Int64(value)) => value
            .checked_neg()
            .map(Value::Int64)
            .ok_or_else(|| DbError::new("22003", "numeric value out of range")),
        (UnaryOperator::Negate, Value::Float32(value)) => Ok(Value::Float32(-value)),
        (UnaryOperator::Negate, Value::Float64(value)) => Ok(Value::Float64(-value)),
        (UnaryOperator::Negate, Value::Decimal(value)) => Ok(Value::Decimal(-value)),
        _ => Err(DbError::new(
            "42804",
            "unary operator received an incompatible value",
        )),
    }
}

fn evaluate_binary(left: Value, operator: BinaryOperator, right: Value) -> Result<Value> {
    if matches!(operator, BinaryOperator::And | BinaryOperator::Or) {
        return evaluate_boolean_binary(left, operator, right);
    }
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    match operator {
        BinaryOperator::Eq => return Ok(Value::Boolean(left == right)),
        BinaryOperator::NotEq => return Ok(Value::Boolean(left != right)),
        _ => {}
    }
    let ordering = compare_values(&left, &right)?;
    let result = match operator {
        BinaryOperator::Lt => ordering == Ordering::Less,
        BinaryOperator::LtEq => ordering != Ordering::Greater,
        BinaryOperator::Gt => ordering == Ordering::Greater,
        BinaryOperator::GtEq => ordering != Ordering::Less,
        BinaryOperator::Eq | BinaryOperator::NotEq | BinaryOperator::And | BinaryOperator::Or => {
            unreachable!("handled above")
        }
    };
    Ok(Value::Boolean(result))
}

fn evaluate_boolean_binary(left: Value, operator: BinaryOperator, right: Value) -> Result<Value> {
    let left = boolean_or_null(left)?;
    let right = boolean_or_null(right)?;
    let value = match operator {
        BinaryOperator::And => match (left, right) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        BinaryOperator::Or => match (left, right) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        _ => unreachable!("only boolean operators are accepted"),
    };
    Ok(value.map_or(Value::Null, Value::Boolean))
}

fn boolean_or_null(value: Value) -> Result<Option<bool>> {
    match value {
        Value::Null => Ok(None),
        Value::Boolean(value) => Ok(Some(value)),
        _ => Err(DbError::new("42804", "boolean value required")),
    }
}

fn predicate_matches(expr: &BoundExpr, row: &Row, params: &[Value]) -> Result<bool> {
    match evaluate(expr, &row.values, params)? {
        Value::Boolean(value) => Ok(value),
        Value::Null => Ok(false),
        _ => Err(DbError::new("42804", "predicate must evaluate to boolean")),
    }
}

fn evaluate_limit(expr: &BoundExpr, params: &[Value]) -> Result<usize> {
    match evaluate(expr, &[], params)? {
        Value::Int64(value) if value >= 0 => {
            usize::try_from(value).map_err(|_| DbError::new("22003", "LIMIT value is out of range"))
        }
        Value::Null => Err(DbError::new("22004", "LIMIT cannot be null")),
        _ => Err(DbError::new(
            "2201W",
            "LIMIT must be a non-negative integer",
        )),
    }
}

fn coerce_value(value: Value, target: &ScalarType) -> Result<Value> {
    if value.is_null() {
        return Ok(value);
    }
    match (value, target) {
        (Value::Boolean(value), ScalarType::Boolean) => Ok(Value::Boolean(value)),
        (Value::Int16(value), ScalarType::Int16) => Ok(Value::Int16(value)),
        (Value::Int16(value), ScalarType::Int32) => Ok(Value::Int32(i32::from(value))),
        (Value::Int16(value), ScalarType::Int64) => Ok(Value::Int64(i64::from(value))),
        (Value::Int16(value), ScalarType::Float32) => Ok(Value::Float32(f32::from(value))),
        (Value::Int16(value), ScalarType::Float64) => Ok(Value::Float64(f64::from(value))),
        (Value::Int16(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Int32(value), ScalarType::Int32) => Ok(Value::Int32(value)),
        (Value::Int32(value), ScalarType::Int64) => Ok(Value::Int64(i64::from(value))),
        (Value::Int32(value), ScalarType::Float64) => Ok(Value::Float64(f64::from(value))),
        (Value::Int32(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Int64(value), ScalarType::Int64) => Ok(Value::Int64(value)),
        (Value::Int64(value), ScalarType::Float64) => Ok(Value::Float64(value as f64)),
        (Value::Int64(value), ScalarType::Decimal { .. }) => {
            Ok(Value::Decimal(Decimal::from(value)))
        }
        (Value::Float32(value), ScalarType::Float32) => Ok(Value::Float32(value)),
        (Value::Float64(value), ScalarType::Float64) => Ok(Value::Float64(value)),
        (Value::Decimal(value), ScalarType::Decimal { .. }) => Ok(Value::Decimal(value)),
        (
            Value::Text(value),
            ScalarType::Text | ScalarType::Char { .. } | ScalarType::Varchar { .. },
        ) => Ok(Value::Text(value)),
        (Value::Binary(value), ScalarType::Binary) => Ok(Value::Binary(value)),
        (Value::Date(value), ScalarType::Date) => Ok(Value::Date(value)),
        (Value::Time(value), ScalarType::Time) => Ok(Value::Time(value)),
        (
            Value::Timestamp(value),
            ScalarType::Timestamp {
                with_timezone: false,
            },
        ) => Ok(Value::Timestamp(value)),
        (Value::Json(value), ScalarType::Json) => Ok(Value::Json(value)),
        (Value::Jsonb(value), ScalarType::Jsonb) => Ok(Value::Jsonb(value)),
        (Value::Uuid(value), ScalarType::Uuid) => Ok(Value::Uuid(value)),
        (Value::Vector(value), ScalarType::Vector { dimensions })
            if dimensions.is_none_or(|dimensions| dimensions == value.len()) =>
        {
            Ok(Value::Vector(value))
        }
        (value, target) => Err(DbError::new(
            "42804",
            format!("value {value:?} cannot be assigned to {target:?}"),
        )),
    }
}

fn validate_rows(table: &TableDefinition, rows: &[Row]) -> Result<()> {
    for row in rows {
        if row.values.len() != table.columns().len() {
            return Err(internal_error("row width does not match table metadata"));
        }
        for (column, value) in table.columns().iter().zip(&row.values) {
            if !column.nullable && value.is_null() {
                return Err(DbError::new(
                    "23502",
                    format!(
                        "null value in column {} violates not-null constraint",
                        column.name
                    ),
                ));
            }
            coerce_value(value.clone(), &column.data_type)?;
        }
    }
    for (column_index, column) in table.columns().iter().enumerate() {
        if !column.unique {
            continue;
        }
        for left in 0..rows.len() {
            let left_value = &rows[left].values[column_index];
            if left_value.is_null() {
                continue;
            }
            for right_row in rows.iter().skip(left + 1) {
                if left_value == &right_row.values[column_index] {
                    return Err(DbError::new(
                        "23505",
                        format!(
                            "duplicate value violates unique constraint on {}",
                            column.name
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn compare_rows(left: &Row, right: &Row, order_by: &[BoundOrder]) -> Result<Ordering> {
    for order in order_by {
        let left_value = &left.values[order.column_index];
        let right_value = &right.values[order.column_index];
        let ordering = match (left_value.is_null(), right_value.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if order.nulls_first.unwrap_or(!order.ascending) {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if order.nulls_first.unwrap_or(!order.ascending) {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = compare_values(left_value, right_value)?;
                if order.ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            }
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_values(left: &Value, right: &Value) -> Result<Ordering> {
    let ordering = match (left, right) {
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Int16(left), Value::Int16(right)) => left.cmp(right),
        (Value::Int32(left), Value::Int32(right)) => left.cmp(right),
        (Value::Int64(left), Value::Int64(right)) => left.cmp(right),
        (Value::Float32(left), Value::Float32(right)) => left
            .partial_cmp(right)
            .ok_or_else(|| DbError::new("22003", "NaN values cannot be ordered"))?,
        (Value::Float64(left), Value::Float64(right)) => left
            .partial_cmp(right)
            .ok_or_else(|| DbError::new("22003", "NaN values cannot be ordered"))?,
        (Value::Decimal(left), Value::Decimal(right)) => left.cmp(right),
        (Value::Text(left), Value::Text(right)) => left.cmp(right),
        (Value::Binary(left), Value::Binary(right)) => left.cmp(right),
        (Value::Date(left), Value::Date(right)) => left.cmp(right),
        (Value::Time(left), Value::Time(right)) => left.cmp(right),
        (Value::Timestamp(left), Value::Timestamp(right)) => left.cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.cmp(right),
        _ if left == right => Ordering::Equal,
        _ => {
            return Err(DbError::new(
                "42883",
                "values do not support this comparison",
            ));
        }
    };
    Ok(ordering)
}

fn table_definition(state: &DatabaseState, table_id: TableId) -> Result<&TableDefinition> {
    state
        .catalog
        .table_by_id(table_id)
        .ok_or_else(|| internal_error(format!("bound table ID {table_id:?} does not exist")))
}

fn command_events(
    schema: Schema,
    tag: impl Into<String>,
    rows_affected: u64,
    batch: Option<Batch>,
) -> Vec<QueryEvent> {
    let mut events = vec![QueryEvent::Schema(schema)];
    if let Some(batch) = batch {
        events.push(QueryEvent::Batch(batch));
    }
    events.push(QueryEvent::Progress(QueryProgress {
        rows_processed: rows_affected,
    }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: tag.into(),
        rows_affected,
    }));
    events
}

fn internal_error(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message).with_hint("restart the session and retry")
}

#[must_use]
pub fn configured_data_dir(config: &EngineConfig) -> &Path {
    &config.data_dir
}

#[cfg(test)]
mod tests {
    use tempfile::{TempDir, tempdir};

    use super::*;

    fn engine() -> (TempDir, Engine) {
        let directory = tempdir().expect("tempdir");
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
        (directory, engine)
    }

    fn execute(session: &mut Session, sql: &str, params: &[Value]) -> Vec<QueryEvent> {
        session
            .execute(sql, params)
            .expect("execute statement")
            .collect()
    }

    fn rows(events: &[QueryEvent]) -> Vec<Row> {
        events
            .iter()
            .filter_map(|event| match event {
                QueryEvent::Batch(batch) => Some(batch.rows.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    fn create_documents(session: &mut Session) {
        execute(
            session,
            "CREATE TABLE documents (\
                id BIGINT PRIMARY KEY,\
                title TEXT NOT NULL,\
                score INTEGER\
            )",
            &[],
        );
    }

    #[test]
    fn executes_crud_with_parameters_ordering_and_limits() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        create_documents(&mut session);
        execute(
            &mut session,
            "INSERT INTO documents (id, title, score) VALUES \
             ($1, 'first', 10), ($2, 'second', 20), ($3, 'third', 30)",
            &[Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        );

        let events = execute(
            &mut session,
            "SELECT id, title FROM documents WHERE score >= $1 ORDER BY id DESC LIMIT 2",
            &[Value::Int32(15)],
        );
        assert_eq!(
            rows(&events),
            vec![
                Row::new(vec![Value::Int64(3), Value::Text("third".into())]),
                Row::new(vec![Value::Int64(2), Value::Text("second".into())]),
            ]
        );

        execute(
            &mut session,
            "UPDATE documents SET title = 'updated' WHERE id = $1",
            &[Value::Int64(2)],
        );
        execute(
            &mut session,
            "DELETE FROM documents WHERE id = $1",
            &[Value::Int64(1)],
        );
        let events = execute(
            &mut session,
            "SELECT id, title FROM documents ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&events),
            vec![
                Row::new(vec![Value::Int64(2), Value::Text("updated".into()),]),
                Row::new(vec![Value::Int64(3), Value::Text("third".into())]),
            ]
        );
    }

    #[test]
    fn compares_jsonb_parameters_by_equality_without_requiring_ordering() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE payloads (id BIGINT PRIMARY KEY, body JSONB NOT NULL)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO payloads VALUES (1, $1), (2, $2)",
            &[
                Value::Jsonb(serde_json::json!({"kind": "match"})),
                Value::Jsonb(serde_json::json!({"kind": "other"})),
            ],
        );

        let equal = execute(
            &mut session,
            "SELECT id FROM payloads WHERE body = $1 ORDER BY id",
            &[Value::Jsonb(serde_json::json!({"kind": "match"}))],
        );
        assert_eq!(rows(&equal), vec![Row::new(vec![Value::Int64(1)])]);

        let not_equal = execute(
            &mut session,
            "SELECT id FROM payloads WHERE body <> $1 ORDER BY id",
            &[Value::Jsonb(serde_json::json!({"kind": "match"}))],
        );
        assert_eq!(rows(&not_equal), vec![Row::new(vec![Value::Int64(2)])]);
    }

    #[test]
    fn enforces_not_null_primary_key_and_unique_constraints_atomically() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE NOT NULL)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO users VALUES (1, 'a@example.test')",
            &[],
        );

        let error = session
            .execute(
                "INSERT INTO users VALUES (2, 'b@example.test'), (1, 'c@example.test')",
                &[],
            )
            .expect_err("duplicate primary key");
        assert_eq!(error.sql_state, "23505");
        let events = execute(&mut session, "SELECT * FROM users", &[]);
        assert_eq!(rows(&events).len(), 1);

        let error = session
            .execute("INSERT INTO users VALUES (2, NULL)", &[])
            .expect_err("not null");
        assert_eq!(error.sql_state, "23502");
    }

    #[test]
    fn commits_rolls_back_and_detects_generation_conflicts() {
        let (_directory, engine) = engine();
        let mut first = engine.connect().expect("first");
        let mut second = engine.connect().expect("second");
        create_documents(&mut first);

        {
            let mut transaction = first.begin().expect("begin");
            transaction
                .execute("INSERT INTO documents VALUES (1, 'rolled back', 1)", &[])
                .expect("insert");
            transaction.rollback().expect("rollback");
        }
        assert!(rows(&execute(&mut first, "SELECT * FROM documents", &[])).is_empty());

        {
            let mut transaction = first.begin().expect("begin");
            transaction
                .execute("INSERT INTO documents VALUES (1, 'committed', 1)", &[])
                .expect("insert");
            transaction.commit().expect("commit");
        }
        assert_eq!(
            rows(&execute(&mut first, "SELECT * FROM documents", &[])).len(),
            1
        );

        let mut transaction = first.begin().expect("begin conflict");
        transaction
            .execute("INSERT INTO documents VALUES (2, 'stale', 2)", &[])
            .expect("transaction insert");
        execute(
            &mut second,
            "INSERT INTO documents VALUES (3, 'concurrent', 3)",
            &[],
        );
        let error = transaction.commit().expect_err("generation conflict");
        assert_eq!(error.sql_state, "40001");
    }

    #[test]
    fn emits_schema_then_work_then_exactly_one_completion() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        create_documents(&mut session);
        let events = execute(&mut session, "SELECT * FROM documents", &[]);

        assert!(matches!(events.first(), Some(QueryEvent::Schema(_))));
        assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, QueryEvent::Complete(_)))
                .count(),
            1
        );
        assert!(events[1..events.len() - 1].iter().all(|event| matches!(
            event,
            QueryEvent::Batch(_) | QueryEvent::Progress(_) | QueryEvent::Notice(_)
        )));
    }

    #[test]
    fn open_bootstraps_and_reopens_the_persistent_store() {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
            assert_eq!(configured_data_dir(engine.config()), directory.path());
        }
        assert!(directory.path().join("ordadb.data").is_file());
        assert!(Engine::open(EngineConfig::new(directory.path())).is_ok());
    }
}
