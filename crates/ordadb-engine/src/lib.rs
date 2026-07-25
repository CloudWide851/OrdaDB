//! SQL execution and persistent snapshot publication for OrdaDB.
//!
//! This crate owns SQL semantics and candidate-state atomicity. Physical page
//! encoding belongs to `ordadb-storage`; WAL and crash recovery remain later
//! milestones.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use ordadb_catalog::{Catalog, ColumnStatistics, TableDefinition, TableStatistics, indexable_type};
use ordadb_execution::{
    ExecutionContext, coerce_value as coerce_execution_value,
    compare_values as compare_execution_values, evaluate as evaluate_scalar, evaluate_group,
    execute as execute_plan, predicate_matches as execution_predicate_matches,
};
use ordadb_index::{BPlusTree, IndexEntry, IndexKey, RowId};
use ordadb_optimizer::{
    JoinStrategy, choose_join_strategy, explain as explain_plan, optimize_select,
};
use ordadb_sql::{
    BinaryOperator, BoundExpr, BoundExprKind, BoundJoin, BoundOrder, BoundProjection,
    BoundStatement, BoundTable, JoinKind, bind, parse,
};
use ordadb_storage::{DatabaseStore, PersistentState, encode_row};
use ordadb_types::{
    Batch, CommandComplete, DbError, Field, IndexId, QueryEvent, QueryProgress, Result, Row,
    ScalarType, Schema, TableId, Value,
};

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
        let state = DatabaseState::from_persistent(store.committed_state().clone())?;
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
    indexes: BTreeMap<IndexId, BPlusTree>,
    generation: u64,
}

struct SelectExecution {
    table_id: TableId,
    schema: Schema,
    projection: Vec<BoundProjection>,
    filter: Option<BoundExpr>,
    order_by: Vec<BoundOrder>,
    limit: Option<BoundExpr>,
}

struct AdvancedExecution {
    table: BoundTable,
    joins: Vec<BoundJoin>,
    schema: Schema,
    projection: Vec<BoundProjection>,
    filter: Option<BoundExpr>,
    group_by: Vec<BoundExpr>,
    having: Option<BoundExpr>,
    order_by: Vec<BoundOrder>,
    limit: Option<BoundExpr>,
    aggregate: bool,
}

impl DatabaseState {
    fn from_persistent(state: PersistentState) -> Result<Self> {
        let indexes = state
            .indexes
            .into_iter()
            .map(|(index_id, entries)| {
                let definition = state
                    .catalog
                    .index_by_id(index_id)
                    .ok_or_else(|| internal_error("persistent index is absent from the catalog"))?;
                BPlusTree::from_entries(definition.unique, entries).map(|tree| (index_id, tree))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            catalog: state.catalog,
            rows: state.tables,
            indexes,
            generation: state.generation,
        })
    }
}

impl From<&DatabaseState> for PersistentState {
    fn from(state: &DatabaseState) -> Self {
        Self {
            generation: state.generation,
            catalog: state.catalog.clone(),
            tables: state.rows.clone(),
            indexes: state
                .indexes
                .iter()
                .map(|(index_id, tree)| {
                    (
                        *index_id,
                        tree.entries().into_iter().cloned().collect::<Vec<_>>(),
                    )
                })
                .collect(),
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
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "CREATE TABLE", 0, None),
                true,
            ))
        }
        BoundStatement::CreateIndex { table_id, index } => {
            state.catalog.create_index(table_id, index)?;
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "CREATE INDEX", 0, None),
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
            state,
            SelectExecution {
                table_id,
                schema,
                projection,
                filter,
                order_by,
                limit,
            },
            params,
        ),
        BoundStatement::AdvancedSelect {
            table,
            joins,
            schema,
            projection,
            filter,
            group_by,
            having,
            order_by,
            limit,
            aggregate,
        } => execute_advanced_select(
            state,
            AdvancedExecution {
                table,
                joins,
                schema,
                projection,
                filter,
                group_by,
                having,
                order_by,
                limit,
                aggregate,
            },
            params,
        ),
        BoundStatement::Explain { statement } => execute_explain(state, *statement),
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
            values[*column_index] = evaluate_scalar(&expression, &[], params)?;
        }
        candidate_rows.push(Row::new(values));
    }
    validate_rows(&table, &candidate_rows)?;
    state.rows.insert(table_id, candidate_rows);
    rebuild_table_derived(state, table_id)?;
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

fn execute_select(
    state: &DatabaseState,
    execution: SelectExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let SelectExecution {
        table_id,
        schema,
        projection,
        filter,
        order_by,
        limit,
    } = execution;
    let plan = optimize_select(
        table_definition(state, table_id)?,
        projection,
        filter,
        order_by,
        limit,
    );
    let result_rows = execute_plan(
        &plan,
        &ExecutionContext {
            tables: &state.rows,
            indexes: &state.indexes,
            params,
        },
    )?;
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

fn execute_advanced_select(
    state: &DatabaseState,
    execution: AdvancedExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let AdvancedExecution {
        table,
        joins,
        schema,
        projection,
        filter,
        group_by,
        having,
        order_by,
        limit,
        aggregate,
    } = execution;
    let mut source_rows = state.rows.get(&table.table_id).cloned().unwrap_or_default();
    for join in &joins {
        source_rows = execute_join(state, source_rows, join, params)?;
    }
    if let Some(filter) = &filter {
        source_rows = source_rows
            .into_iter()
            .filter_map(
                |row| match execution_predicate_matches(filter, &row, params) {
                    Ok(true) => Some(Ok(row)),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                },
            )
            .collect::<Result<Vec<_>>>()?;
    }

    let result_rows = if aggregate {
        execute_aggregate_projection(
            source_rows,
            &projection,
            &group_by,
            having.as_ref(),
            &order_by,
            limit.as_ref(),
            params,
        )?
    } else {
        sort_rows(&mut source_rows, &order_by)?;
        if let Some(limit) = &limit {
            source_rows.truncate(evaluate_limit_expr(limit, params)?);
        }
        source_rows
            .iter()
            .map(|row| {
                projection
                    .iter()
                    .map(|projection| evaluate_scalar(&projection.expr, &row.values, params))
                    .collect::<Result<Vec<_>>>()
                    .map(Row::new)
            })
            .collect::<Result<Vec<_>>>()?
    };
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

fn execute_join(
    state: &DatabaseState,
    left_rows: Vec<Row>,
    join: &BoundJoin,
    params: &[Value],
) -> Result<Vec<Row>> {
    let right_rows = state
        .rows
        .get(&join.table.table_id)
        .map_or(&[][..], Vec::as_slice);
    let equi_columns = equi_join_columns(&join.on, join.table.offset);
    let strategy = choose_join_strategy(
        left_rows.len() as u64,
        right_rows.len() as u64,
        equi_columns.is_some(),
    )
    .strategy;
    match (strategy, equi_columns) {
        (JoinStrategy::Hash, Some((left_index, right_index))) => execute_hash_join(
            left_rows,
            right_rows,
            join,
            left_index,
            right_index - join.table.offset,
            params,
        ),
        _ => execute_nested_loop_join(left_rows, right_rows, join, params),
    }
}

fn execute_nested_loop_join(
    left_rows: Vec<Row>,
    right_rows: &[Row],
    join: &BoundJoin,
    params: &[Value],
) -> Result<Vec<Row>> {
    let mut output = Vec::new();
    for left in left_rows {
        let mut matched = false;
        for right in right_rows {
            let mut values = left.values.clone();
            values.extend(right.values.clone());
            let row = Row::new(values);
            if execution_predicate_matches(&join.on, &row, params)? {
                matched = true;
                output.push(row);
            }
        }
        if !matched && join.kind == JoinKind::Left {
            let mut values = left.values;
            values.extend(std::iter::repeat_n(Value::Null, join.table.width));
            output.push(Row::new(values));
        }
    }
    Ok(output)
}

fn execute_hash_join(
    left_rows: Vec<Row>,
    right_rows: &[Row],
    join: &BoundJoin,
    left_index: usize,
    right_index: usize,
    params: &[Value],
) -> Result<Vec<Row>> {
    let mut buckets = HashMap::<Vec<u8>, Vec<&Row>>::new();
    for right in right_rows {
        let Some(value) = right.values.get(right_index) else {
            return Err(internal_error("hash join right key is out of bounds"));
        };
        if value.is_null() {
            continue;
        }
        buckets
            .entry(encode_row(&Row::new(vec![value.clone()]))?)
            .or_default()
            .push(right);
    }
    let mut output = Vec::new();
    for left in left_rows {
        let Some(value) = left.values.get(left_index) else {
            return Err(internal_error("hash join left key is out of bounds"));
        };
        let candidates = if value.is_null() {
            None
        } else {
            buckets.get(&encode_row(&Row::new(vec![value.clone()]))?)
        };
        let mut matched = false;
        if let Some(candidates) = candidates {
            for right in candidates {
                let mut values = left.values.clone();
                values.extend(right.values.clone());
                let row = Row::new(values);
                if execution_predicate_matches(&join.on, &row, params)? {
                    matched = true;
                    output.push(row);
                }
            }
        }
        if !matched && join.kind == JoinKind::Left {
            let mut values = left.values;
            values.extend(std::iter::repeat_n(Value::Null, join.table.width));
            output.push(Row::new(values));
        }
    }
    Ok(output)
}

fn equi_join_columns(expr: &BoundExpr, right_offset: usize) -> Option<(usize, usize)> {
    let BoundExprKind::Binary {
        left,
        op: BinaryOperator::Eq,
        right,
    } = &expr.kind
    else {
        return None;
    };
    let (BoundExprKind::Column { index: left_index }, BoundExprKind::Column { index: right_index }) =
        (&left.kind, &right.kind)
    else {
        return None;
    };
    if *left_index < right_offset && *right_index >= right_offset {
        Some((*left_index, *right_index))
    } else if *right_index < right_offset && *left_index >= right_offset {
        Some((*right_index, *left_index))
    } else {
        None
    }
}

fn execute_aggregate_projection(
    rows: Vec<Row>,
    projection: &[BoundProjection],
    group_by: &[BoundExpr],
    having: Option<&BoundExpr>,
    order_by: &[BoundOrder],
    limit: Option<&BoundExpr>,
    params: &[Value],
) -> Result<Vec<Row>> {
    let mut groups = Vec::<(Vec<Value>, Vec<Row>)>::new();
    if group_by.is_empty() {
        groups.push((Vec::new(), rows));
    } else {
        for row in rows {
            let key = group_by
                .iter()
                .map(|expr| evaluate_scalar(expr, &row.values, params))
                .collect::<Result<Vec<_>>>()?;
            if let Some((_, group_rows)) = groups.iter_mut().find(|(existing, _)| existing == &key)
            {
                group_rows.push(row);
            } else {
                groups.push((key, vec![row]));
            }
        }
    }
    if let Some(having) = having {
        groups = groups
            .into_iter()
            .filter_map(|(key, rows)| {
                let result = {
                    let representative = rows.first().map_or(&[][..], |row| row.values.as_slice());
                    evaluate_group(having, &rows, representative, params)
                };
                match result {
                    Ok(Value::Boolean(true)) => Some(Ok((key, rows))),
                    Ok(Value::Boolean(false) | Value::Null) => None,
                    Ok(_) => Some(Err(DbError::new(
                        "42804",
                        "HAVING must evaluate to boolean",
                    ))),
                    Err(error) => Some(Err(error)),
                }
            })
            .collect::<Result<Vec<_>>>()?;
    }
    if !order_by.is_empty() {
        let mut error = None;
        groups.sort_by(|(_, left), (_, right)| {
            let left = left
                .first()
                .cloned()
                .unwrap_or_else(|| Row::new(Vec::new()));
            let right = right
                .first()
                .cloned()
                .unwrap_or_else(|| Row::new(Vec::new()));
            compare_ordered_rows(&left, &right, order_by).unwrap_or_else(|sort_error| {
                error = Some(sort_error);
                Ordering::Equal
            })
        });
        if let Some(error) = error {
            return Err(error);
        }
    }
    if let Some(limit) = limit {
        groups.truncate(evaluate_limit_expr(limit, params)?);
    }

    groups
        .into_iter()
        .map(|(_, rows)| {
            let representative = rows.first().map_or(&[][..], |row| row.values.as_slice());
            projection
                .iter()
                .map(|projection| evaluate_group(&projection.expr, &rows, representative, params))
                .collect::<Result<Vec<_>>>()
                .map(Row::new)
        })
        .collect()
}

fn sort_rows(rows: &mut [Row], order_by: &[BoundOrder]) -> Result<()> {
    if order_by.is_empty() {
        return Ok(());
    }
    let mut error = None;
    rows.sort_by(|left, right| {
        compare_ordered_rows(left, right, order_by).unwrap_or_else(|sort_error| {
            error = Some(sort_error);
            Ordering::Equal
        })
    });
    if let Some(error) = error {
        Err(error)
    } else {
        Ok(())
    }
}

fn compare_ordered_rows(left: &Row, right: &Row, order_by: &[BoundOrder]) -> Result<Ordering> {
    for order in order_by {
        let left_value = left
            .values
            .get(order.column_index)
            .ok_or_else(|| internal_error("ORDER BY column is out of bounds"))?;
        let right_value = right
            .values
            .get(order.column_index)
            .ok_or_else(|| internal_error("ORDER BY column is out of bounds"))?;
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
                let ordering = compare_execution_values(left_value, right_value)?;
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

fn evaluate_limit_expr(expr: &BoundExpr, params: &[Value]) -> Result<usize> {
    match evaluate_scalar(expr, &[], params)? {
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

fn execute_explain(
    state: &DatabaseState,
    statement: BoundStatement,
) -> Result<(Vec<QueryEvent>, bool)> {
    let lines = match statement {
        BoundStatement::Select {
            table_id,
            projection,
            filter,
            order_by,
            limit,
            ..
        } => explain_plan(&optimize_select(
            table_definition(state, table_id)?,
            projection,
            filter,
            order_by,
            limit,
        )),
        BoundStatement::AdvancedSelect {
            table,
            joins,
            filter,
            aggregate,
            ..
        } => explain_advanced(state, &table, &joins, filter.is_some(), aggregate)?,
        _ => {
            return Err(DbError::new(
                "0A000",
                "EXPLAIN supports SELECT statements only",
            ));
        }
    };
    let schema = Schema::new(vec![Field::new("QUERY PLAN", ScalarType::Text, false)]);
    let count = lines.len() as u64;
    let batch = Batch {
        schema: schema.clone(),
        rows: lines
            .into_iter()
            .map(|line| Row::new(vec![Value::Text(line)]))
            .collect(),
    };
    Ok((
        command_events(schema, format!("EXPLAIN {count}"), count, Some(batch)),
        false,
    ))
}

fn explain_advanced(
    state: &DatabaseState,
    table: &BoundTable,
    joins: &[BoundJoin],
    filtered: bool,
    aggregate: bool,
) -> Result<Vec<String>> {
    let base = table_definition(state, table.table_id)?;
    let mut estimated_rows = base.statistics().row_count;
    let mut lines = vec!["Projection  (cost=0.00 rows=1)".to_owned()];
    if aggregate {
        lines.push("  Aggregate  (cost=0.00 rows=1)".to_owned());
    }
    if filtered {
        lines.push(format!(
            "  Filter  (cost={:.2} rows={})",
            estimated_rows as f64 * 0.01,
            estimated_rows
        ));
    }
    for join in joins {
        let right = table_definition(state, join.table.table_id)?;
        let choice = choose_join_strategy(
            estimated_rows,
            right.statistics().row_count,
            equi_join_columns(&join.on, join.table.offset).is_some(),
        );
        let name = match choice.strategy {
            JoinStrategy::NestedLoop => "Nested Loop",
            JoinStrategy::Hash => "Hash Join",
        };
        let kind = if join.kind == JoinKind::Left {
            "Left"
        } else {
            "Inner"
        };
        lines.push(format!(
            "  {name} {kind}  (cost={:.2} rows={:.0})",
            choice.estimated_cost, choice.estimated_rows
        ));
        estimated_rows = choice.estimated_rows as u64;
    }
    lines.push(format!(
        "    Seq Scan on {}  (cost={:.2} rows={})",
        table.binding,
        estimated_rows as f64 * 0.01,
        base.statistics().row_count
    ));
    for join in joins {
        let right = table_definition(state, join.table.table_id)?;
        lines.push(format!(
            "    Seq Scan on {}  (cost={:.2} rows={})",
            join.table.binding,
            right.statistics().row_count as f64 * 0.01,
            right.statistics().row_count
        ));
    }
    Ok(lines)
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
            .map(|filter| execution_predicate_matches(filter, row, params))
            .transpose()?
            .unwrap_or(true)
        {
            let original = row.values.clone();
            let mut replacements = Vec::with_capacity(assignments.len());
            for (column_index, expression) in &assignments {
                replacements.push((
                    *column_index,
                    evaluate_scalar(expression, &original, params)?,
                ));
            }
            for (column_index, value) in replacements {
                row.values[column_index] = value;
            }
            updated += 1;
        }
    }
    validate_rows(&table, &candidate_rows)?;
    state.rows.insert(table_id, candidate_rows);
    rebuild_table_derived(state, table_id)?;
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
        rows.retain(
            |row| match execution_predicate_matches(filter, row, params) {
                Ok(matches) => !matches,
                Err(predicate_error) => {
                    error = Some(predicate_error);
                    true
                }
            },
        );
        if let Some(error) = error {
            return Err(error);
        }
    } else {
        rows.clear();
    }
    let deleted = (original_len - rows.len()) as u64;
    rebuild_table_derived(state, table_id)?;
    Ok((
        command_events(Schema::empty(), format!("DELETE {deleted}"), deleted, None),
        true,
    ))
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
            coerce_execution_value(value.clone(), &column.data_type)?;
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

fn rebuild_table_derived(state: &mut DatabaseState, table_id: TableId) -> Result<()> {
    let table = table_definition(state, table_id)?.clone();
    let rows = state.rows.get(&table_id).cloned().unwrap_or_default();
    let mut rebuilt = Vec::new();
    for definition in table.indexes() {
        let key_positions = definition
            .key_columns
            .iter()
            .map(|column_id| {
                table
                    .column_index_by_id(*column_id)
                    .ok_or_else(|| internal_error("index key column is absent from its table"))
            })
            .collect::<Result<Vec<_>>>()?;
        let include_positions = definition
            .include_columns
            .iter()
            .map(|column_id| {
                table
                    .column_index_by_id(*column_id)
                    .ok_or_else(|| internal_error("index include column is absent from its table"))
            })
            .collect::<Result<Vec<_>>>()?;
        let entries = rows
            .iter()
            .enumerate()
            .map(|(row_index, row)| {
                let row_id = u64::try_from(row_index)
                    .map(RowId::new)
                    .map_err(|_| DbError::new("54000", "table row count exceeds index limits"))?;
                let key_values = key_positions
                    .iter()
                    .map(|position| row.values[*position].clone())
                    .collect::<Vec<_>>();
                let included = include_positions
                    .iter()
                    .map(|position| row.values[*position].clone())
                    .collect();
                IndexEntry::new(&key_values, row_id, included)
            })
            .collect::<Result<Vec<_>>>()?;
        let tree = BPlusTree::from_entries(definition.unique, entries)?;
        rebuilt.push((definition.id, tree));
    }
    state
        .indexes
        .retain(|index_id, _| state.catalog.index_by_id(*index_id).is_some());
    for (index_id, tree) in rebuilt {
        state.indexes.insert(index_id, tree);
    }
    state
        .catalog
        .set_table_statistics(table_id, compute_statistics(&table, &rows)?)?;
    Ok(())
}

fn compute_statistics(table: &TableDefinition, rows: &[Row]) -> Result<TableStatistics> {
    let mut columns = BTreeMap::new();
    for (column_index, column) in table.columns().iter().enumerate() {
        let values = rows
            .iter()
            .filter_map(|row| row.values.get(column_index))
            .collect::<Vec<_>>();
        let null_count = values.iter().filter(|value| value.is_null()).count() as u64;
        let mut distinct = HashSet::new();
        for value in values.iter().filter(|value| !value.is_null()) {
            distinct.insert(encode_row(&Row::new(vec![(*value).clone()]))?);
        }
        let (min, max) = if indexable_type(&column.data_type) {
            let mut minimum: Option<(IndexKey, Value)> = None;
            let mut maximum: Option<(IndexKey, Value)> = None;
            for value in values.iter().filter(|value| !value.is_null()) {
                let key = IndexKey::from_values(&[(*value).clone()])?;
                if minimum.as_ref().is_none_or(|(minimum, _)| key < *minimum) {
                    minimum = Some((key.clone(), (*value).clone()));
                }
                if maximum.as_ref().is_none_or(|(maximum, _)| key > *maximum) {
                    maximum = Some((key, (*value).clone()));
                }
            }
            (
                minimum.map(|(_, value)| value),
                maximum.map(|(_, value)| value),
            )
        } else {
            (None, None)
        };
        columns.insert(
            column.id,
            ColumnStatistics {
                null_count,
                distinct_count: distinct.len() as u64,
                min,
                max,
            },
        );
    }
    Ok(TableStatistics {
        row_count: rows.len() as u64,
        columns,
    })
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

    #[test]
    fn executes_inner_left_join_grouped_aggregates_and_having() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT, amount BIGINT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO orders VALUES (10, 1, 5), (11, 1, 7)",
            &[],
        );

        let grouped = execute(
            &mut session,
            "SELECT c.id, COUNT(o.id) AS order_count, SUM(o.amount) AS total \
             FROM customers c LEFT JOIN orders o ON c.id = o.customer_id \
             GROUP BY c.id ORDER BY c.id",
            &[],
        );
        assert_eq!(
            rows(&grouped),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(2), Value::Int64(12)]),
                Row::new(vec![Value::Int64(2), Value::Int64(0), Value::Null]),
            ]
        );

        let having = execute(
            &mut session,
            "SELECT c.id, COUNT(o.id) AS order_count \
             FROM customers c INNER JOIN orders o ON c.id = o.customer_id \
             GROUP BY c.id HAVING COUNT(o.id) > 1",
            &[],
        );
        assert_eq!(
            rows(&having),
            vec![Row::new(vec![Value::Int64(1), Value::Int64(2)])]
        );

        let aggregate = execute(
            &mut session,
            "SELECT COUNT(*), AVG(amount), MIN(amount), MAX(amount) FROM orders",
            &[],
        );
        assert_eq!(
            rows(&aggregate),
            vec![Row::new(vec![
                Value::Int64(2),
                Value::Float64(6.0),
                Value::Int64(5),
                Value::Int64(7),
            ])]
        );
    }

    #[test]
    fn persists_covering_indexes_statistics_and_explains_real_access_paths() {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
            let mut session = engine.connect().expect("connect");
            execute(
                &mut session,
                "CREATE TABLE metrics (id BIGINT PRIMARY KEY, bucket BIGINT, score BIGINT, payload TEXT)",
                &[],
            );
            let values = (0..512)
                .map(|value| format!("({value}, {}, {value}, 'p{value}')", value % 8))
                .collect::<Vec<_>>()
                .join(", ");
            execute(
                &mut session,
                &format!("INSERT INTO metrics VALUES {values}"),
                &[],
            );
            let duplicate = session
                .execute(
                    "CREATE UNIQUE INDEX metrics_bucket_unique ON metrics (bucket)",
                    &[],
                )
                .expect_err("duplicate unique build");
            assert_eq!(duplicate.sql_state, "23505");
            execute(
                &mut session,
                "CREATE INDEX metrics_score_idx ON metrics (score) INCLUDE (payload)",
                &[],
            );
        }

        let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
        let mut session = engine.connect().expect("connect");
        let explain = execute(
            &mut session,
            "EXPLAIN SELECT payload FROM metrics WHERE score = 511",
            &[],
        );
        let plan = rows(&explain)
            .into_iter()
            .filter_map(|row| match row.values.as_slice() {
                [Value::Text(line)] => Some(line.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            plan.iter().any(|line| line.contains("Index Scan")),
            "{plan:?}"
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT payload FROM metrics WHERE score = 511",
                &[],
            )),
            vec![Row::new(vec![Value::Text("p511".into())])]
        );
    }
}
