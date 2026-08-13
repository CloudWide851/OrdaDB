
fn bound_statement_schema(statement: &BoundStatement) -> Schema {
    match statement {
        BoundStatement::Select { schema, .. }
        | BoundStatement::AdvancedSelect { schema, .. }
        | BoundStatement::SetOperation { schema, .. }
        | BoundStatement::With { schema, .. }
        | BoundStatement::ViewSelect { schema, .. }
        | BoundStatement::ScalarSelect { schema, .. }
        | BoundStatement::Call { schema, .. }
        | BoundStatement::RoutineSelect { schema, .. }
        | BoundStatement::PgNotify { schema, .. }
        | BoundStatement::SequenceValue { schema, .. } => schema.clone(),
        BoundStatement::Insert {
            returning: Some(returning),
            ..
        }
        | BoundStatement::ViewInsert {
            returning: Some(returning),
            ..
        }
        | BoundStatement::Update {
            returning: Some(returning),
            ..
        }
        | BoundStatement::ViewUpdate {
            returning: Some(returning),
            ..
        }
        | BoundStatement::Delete {
            returning: Some(returning),
            ..
        }
        | BoundStatement::ViewDelete {
            returning: Some(returning),
            ..
        }
        | BoundStatement::Merge(BoundMerge {
            returning: Some(returning),
            ..
        }) => returning.schema.clone(),
        BoundStatement::Explain { .. } => {
            Schema::new(vec![Field::new("QUERY PLAN", ScalarType::Text, false)])
        }
        BoundStatement::Begin { .. }
        | BoundStatement::Commit { .. }
        | BoundStatement::Rollback { .. }
        | BoundStatement::Savepoint { .. }
        | BoundStatement::RollbackTo { .. }
        | BoundStatement::ReleaseSavepoint { .. }
        | BoundStatement::Analyze { .. }
        | BoundStatement::Vacuum { .. }
        | BoundStatement::Reindex { .. }
        | BoundStatement::Listen { .. }
        | BoundStatement::Unlisten { .. }
        | BoundStatement::Notify { .. }
        | BoundStatement::Do { .. }
        | BoundStatement::DiscardAll
        | BoundStatement::DeallocateAll
        | BoundStatement::NoOp { .. }
        | BoundStatement::CreateSchema { .. }
        | BoundStatement::CreateEnumType { .. }
        | BoundStatement::CreateDomain { .. }
        | BoundStatement::AlterEnumAddValue { .. }
        | BoundStatement::AlterEnumRenameValue { .. }
        | BoundStatement::AlterDomain { .. }
        | BoundStatement::AlterSchemaRename { .. }
        | BoundStatement::DropObjects { .. }
        | BoundStatement::CreateTable { .. }
        | BoundStatement::AlterTable { .. }
        | BoundStatement::CreateIndex { .. }
        | BoundStatement::AlterIndexRename { .. }
        | BoundStatement::CreateSequence { .. }
        | BoundStatement::AlterSequenceRename { .. }
        | BoundStatement::AlterSequence { .. }
        | BoundStatement::CreateView { .. }
        | BoundStatement::AlterViewRename { .. }
        | BoundStatement::RefreshMaterializedView { .. }
        | BoundStatement::CreateRoutine { .. }
        | BoundStatement::DropRoutine { .. }
        | BoundStatement::CreateTrigger { .. }
        | BoundStatement::DropTrigger { .. }
        | BoundStatement::Insert { .. }
        | BoundStatement::ViewInsert { .. }
        | BoundStatement::Merge(BoundMerge {
            returning: None, ..
        })
        | BoundStatement::Update { .. }
        | BoundStatement::ViewUpdate { .. }
        | BoundStatement::Delete { .. }
        | BoundStatement::ViewDelete { .. } => Schema::empty(),
    }
}

fn bound_statement_parameter_types(statement: &BoundStatement) -> Result<Vec<ScalarType>> {
    let mut statements = vec![statement];
    let mut expressions = Vec::new();
    while let Some(statement) = statements.pop() {
        match statement {
            BoundStatement::CreateView { query, .. }
            | BoundStatement::RefreshMaterializedView { query, .. } => {
                statements.push(query);
            }
            BoundStatement::ViewSelect { source, .. }
            | BoundStatement::Explain { statement: source } => {
                statements.push(source);
            }
            BoundStatement::Call { arguments, .. }
            | BoundStatement::RoutineSelect { arguments, .. } => {
                expressions.extend(arguments);
            }
            BoundStatement::ScalarSelect { projection, .. } => {
                expressions.extend(projection.iter().map(|projection| &projection.expr));
            }
            BoundStatement::PgNotify {
                channel, payload, ..
            } => {
                expressions.push(channel);
                expressions.push(payload);
            }
            BoundStatement::SequenceValue { operation, .. } => {
                if let BoundSequenceOperation::SetValue { value, .. } = operation {
                    expressions.push(value);
                }
            }
            BoundStatement::Insert {
                rows,
                on_conflict,
                returning,
                ..
            } => {
                expressions.extend(rows.iter().flatten());
                if let Some(BoundOnConflict {
                    action:
                        BoundConflictAction::DoUpdate {
                            assignments,
                            filter,
                        },
                    ..
                }) = on_conflict
                {
                    expressions.extend(assignments.iter().map(|(_, expression)| expression));
                    expressions.extend(filter.iter());
                }
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::ViewInsert {
                source,
                rows,
                returning,
                ..
            } => {
                statements.push(source);
                expressions.extend(rows.iter().flatten());
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::Merge(merge) => {
                expressions.push(&merge.on);
                for clause in &merge.clauses {
                    expressions.extend(clause.predicate.iter());
                    match &clause.action {
                        BoundMergeAction::Update { assignments } => {
                            expressions.extend(assignments.iter().map(|(_, expression)| expression))
                        }
                        BoundMergeAction::Insert { values, .. } => expressions.extend(values),
                        BoundMergeAction::Delete | BoundMergeAction::DoNothing => {}
                    }
                }
                push_returning_expressions(&mut expressions, merge.returning.as_ref());
            }
            BoundStatement::With { ctes, body, .. } => {
                statements.push(body);
                for cte in ctes {
                    statements.push(&cte.seed);
                    if let Some(recursive) = &cte.recursive {
                        statements.push(recursive);
                    }
                }
            }
            BoundStatement::SetOperation {
                left,
                right,
                order_by,
                offset,
                limit,
                ..
            } => {
                statements.extend([left.as_ref(), right.as_ref()]);
                push_order_expressions(&mut expressions, order_by);
                expressions.extend(offset.iter());
                expressions.extend(limit.iter());
            }
            BoundStatement::Select {
                projection,
                filter,
                order_by,
                offset,
                limit,
                ..
            } => {
                push_projection_expressions(&mut expressions, projection);
                expressions.extend(filter.iter());
                push_order_expressions(&mut expressions, order_by);
                expressions.extend(offset.iter());
                expressions.extend(limit.iter());
            }
            BoundStatement::AdvancedSelect {
                joins,
                applies,
                windows,
                projection,
                filter,
                group_by,
                having,
                order_by,
                offset,
                limit,
                ..
            } => {
                for join in joins {
                    if let BoundJoinSource::Derived { query, .. } = &join.source {
                        statements.push(query);
                    }
                    expressions.push(&join.on);
                }
                for apply in applies {
                    statements.push(&apply.query);
                    match &apply.kind {
                        BoundApplyKind::In { left, .. }
                        | BoundApplyKind::Quantified { left, .. } => expressions.push(left),
                        BoundApplyKind::RowScalar { left, .. }
                        | BoundApplyKind::RowQuantified { left, .. } => expressions.extend(left),
                        BoundApplyKind::Scalar | BoundApplyKind::Exists { .. } => {}
                    }
                }
                for window in windows {
                    expressions.extend(&window.arguments);
                    expressions.extend(window.filter.iter());
                    expressions.extend(&window.partition_by);
                    push_order_expressions(&mut expressions, &window.order_by);
                    if let Some(frame) = &window.frame {
                        push_window_frame_bound(&mut expressions, &frame.start_bound);
                        push_window_frame_bound(&mut expressions, &frame.end_bound);
                    }
                }
                push_projection_expressions(&mut expressions, projection);
                expressions.extend(filter.iter());
                expressions.extend(group_by);
                expressions.extend(having.iter());
                push_order_expressions(&mut expressions, order_by);
                expressions.extend(offset.iter());
                expressions.extend(limit.iter().map(Box::as_ref));
            }
            BoundStatement::Update {
                assignments,
                filter,
                returning,
                ..
            } => {
                expressions.extend(assignments.iter().map(|(_, expression)| expression));
                expressions.extend(filter.iter());
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::ViewUpdate {
                source,
                assignments,
                filter,
                returning,
                ..
            } => {
                statements.push(source);
                expressions.extend(assignments.iter().map(|(_, expression)| expression));
                expressions.extend(filter.iter());
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::Delete {
                filter, returning, ..
            } => {
                expressions.extend(filter.iter());
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::ViewDelete {
                source,
                filter,
                returning,
                ..
            } => {
                statements.push(source);
                expressions.extend(filter.iter());
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::NoOp { .. }
            | BoundStatement::Begin { .. }
            | BoundStatement::Commit { .. }
            | BoundStatement::Rollback { .. }
            | BoundStatement::Savepoint { .. }
            | BoundStatement::RollbackTo { .. }
            | BoundStatement::ReleaseSavepoint { .. }
            | BoundStatement::Analyze { .. }
            | BoundStatement::Vacuum { .. }
            | BoundStatement::Reindex { .. }
            | BoundStatement::Listen { .. }
            | BoundStatement::Unlisten { .. }
            | BoundStatement::Notify { .. }
            | BoundStatement::Do { .. }
            | BoundStatement::DiscardAll
            | BoundStatement::DeallocateAll
            | BoundStatement::CreateSchema { .. }
            | BoundStatement::CreateEnumType { .. }
            | BoundStatement::CreateDomain { .. }
            | BoundStatement::AlterEnumAddValue { .. }
            | BoundStatement::AlterEnumRenameValue { .. }
            | BoundStatement::AlterDomain { .. }
            | BoundStatement::AlterSchemaRename { .. }
            | BoundStatement::DropObjects { .. }
            | BoundStatement::CreateTable { .. }
            | BoundStatement::AlterTable { .. }
            | BoundStatement::CreateIndex { .. }
            | BoundStatement::AlterIndexRename { .. }
            | BoundStatement::CreateSequence { .. }
            | BoundStatement::AlterSequenceRename { .. }
            | BoundStatement::AlterSequence { .. }
            | BoundStatement::AlterViewRename { .. }
            | BoundStatement::CreateRoutine { .. }
            | BoundStatement::DropRoutine { .. }
            | BoundStatement::CreateTrigger { .. }
            | BoundStatement::DropTrigger { .. } => {}
        }
    }

    let mut parameters = BTreeMap::new();
    while let Some(expression) = expressions.pop() {
        match &expression.kind {
            BoundExprKind::Parameter { index } => {
                if let Some(existing) = parameters.get(index) {
                    if existing != &expression.data_type {
                        return Err(DbError::new(
                            "42804",
                            format!("inconsistent types deduced for parameter ${index}"),
                        )
                        .with_detail(format!(
                            "parameter ${index} was inferred as both {existing:?} and {:?}",
                            expression.data_type
                        )));
                    }
                } else {
                    parameters.insert(*index, expression.data_type.clone());
                }
            }
            BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => {
                expressions.push(expr);
            }
            BoundExprKind::Array { elements, .. } => expressions.extend(elements),
            BoundExprKind::Function { arguments, .. } => expressions.extend(arguments),
            BoundExprKind::Binary { left, right, .. } => {
                expressions.extend([left.as_ref(), right.as_ref()]);
            }
            BoundExprKind::InList { expr, list, .. } => {
                expressions.push(expr);
                expressions.extend(list);
            }
            BoundExprKind::Aggregate {
                argument, filter, ..
            } => {
                expressions.extend(argument.iter().map(Box::as_ref));
                expressions.extend(filter.iter().map(Box::as_ref));
            }
            BoundExprKind::Column { .. }
            | BoundExprKind::Literal(_)
            | BoundExprKind::Correlation { .. }
            | BoundExprKind::ApplyValue { .. } => {}
        }
    }

    let Some(max_index) = parameters.keys().next_back().copied() else {
        return Ok(Vec::new());
    };
    (1..=max_index)
        .map(|index| {
            parameters.get(&index).cloned().ok_or_else(|| {
                DbError::new(
                    "42P18",
                    format!("could not determine data type of parameter ${index}"),
                )
            })
        })
        .collect()
}

fn push_projection_expressions<'a>(
    expressions: &mut Vec<&'a BoundExpr>,
    projection: &'a [BoundProjection],
) {
    expressions.extend(projection.iter().map(|projection| &projection.expr));
}

fn push_returning_expressions<'a>(
    expressions: &mut Vec<&'a BoundExpr>,
    returning: Option<&'a BoundReturning>,
) {
    if let Some(returning) = returning {
        push_projection_expressions(expressions, &returning.projection);
    }
}

fn push_order_expressions<'a>(expressions: &mut Vec<&'a BoundExpr>, order_by: &'a [BoundOrder]) {
    expressions.extend(
        order_by
            .iter()
            .filter_map(|order| order.expression.as_ref()),
    );
}

fn push_window_frame_bound<'a>(
    expressions: &mut Vec<&'a BoundExpr>,
    bound: &'a BoundWindowFrameBound,
) {
    if let BoundWindowFrameBound::Preceding(expression)
    | BoundWindowFrameBound::Following(expression) = bound
    {
        expressions.push(expression);
    }
}

#[derive(Debug)]
pub struct Transaction<'session> {
    state: &'session Arc<RwLock<DatabaseState>>,
    store: &'session Arc<Mutex<DatabaseStore>>,
    storage_access: &'session Arc<StorageAccessGate>,
    wal: &'session Arc<WalManager>,
    transaction_status: &'session Arc<TransactionStatusStore>,
    transactions: &'session Arc<TransactionManager>,
    locks: &'session Arc<LockManager>,
    writer: &'session Arc<WriterCoordinator>,
    commits_since_checkpoint: &'session Arc<AtomicU64>,
    sequence_currvals: &'session mut BTreeMap<SequenceId, i64>,
    dialect: SqlDialect,
    authorization: Option<SessionAuthorization>,
    runtime_metadata: SessionRuntimeMetadata,
    transaction: DurableTransaction,
    base: Option<DatabaseState>,
    working: Option<DatabaseState>,
    lock_guards: Vec<LockGuard>,
    lease: Option<WriterLease>,
    dml_only: bool,
    failed: bool,
}

impl Transaction<'_> {
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        if self.failed {
            return Err(failed_transaction_error());
        }
        self.transaction.begin_statement()?;
        if let Err(error) = refresh_read_committed_candidate(
            self.state,
            self.transaction_status.as_ref(),
            &self.transaction,
            &mut self.base,
            &mut self.working,
            self.dml_only,
        ) {
            self.working = None;
            self.lock_guards.clear();
            self.lease = None;
            self.failed = true;
            return Err(error);
        }
        match self.execute_inner(sql, params) {
            Ok(stream) => {
                self.transaction.finish_statement()?;
                Ok(stream)
            }
            Err(error) => {
                self.working = None;
                self.lock_guards.clear();
                self.lease = None;
                self.failed = true;
                Err(error)
            }
        }
    }

    pub fn commit(mut self) -> Result<()> {
        if self.failed {
            return Err(failed_transaction_error());
        }
        let Some(candidate) = self.working else {
            return self.transaction.commit_empty();
        };
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let candidate = if self.dml_only {
            let base = self
                .base
                .as_ref()
                .ok_or_else(|| internal_error("DML transaction is missing its base snapshot"))?;
            merge_dml_candidate(
                &state,
                base,
                &candidate,
                &self.transaction,
                self.transaction_status.as_ref(),
            )?
        } else {
            candidate
        };
        persist_candidate(
            &mut state,
            self.store,
            self.storage_access,
            self.wal,
            &mut self.transaction,
            candidate,
        )?;
        drop(state);
        self.lock_guards.clear();
        self.lease = None;
        record_commit_and_maybe_checkpoint(
            self.state,
            self.store,
            self.wal,
            self.transactions,
            self.commits_since_checkpoint,
        )
    }

    pub fn rollback(self) -> Result<()> {
        self.transaction.abort()
    }

    fn execute_inner(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        let mut snapshot = match &self.working {
            Some(working) => working.clone(),
            None => {
                let committed = committed_snapshot(self.state)?;
                let transaction_snapshot = self
                    .transaction
                    .snapshot()
                    .ok_or_else(|| no_active_transaction_error("take a statement snapshot"))?;
                project_database_visibility(
                    committed,
                    transaction_snapshot,
                    self.transaction.transaction_id(),
                    self.transaction_status.as_ref(),
                )?
            }
        };
        snapshot.sequence_currvals = self.sequence_currvals.clone();
        let statement = resolve_sequence_currval(
            bind_with_session(
                parse_with_dialect(sql, self.dialect)?,
                &snapshot.catalog,
                self.runtime_metadata.bind_values(),
            )?,
            self.sequence_currvals,
        )?;
        if matches!(
            &statement,
            BoundStatement::Begin { .. }
                | BoundStatement::Commit { .. }
                | BoundStatement::Rollback { .. }
                | BoundStatement::Savepoint { .. }
                | BoundStatement::RollbackTo { .. }
                | BoundStatement::ReleaseSavepoint { .. }
        ) {
            return Err(DbError::new(
                "25001",
                "SQL transaction control is not allowed inside Session::begin",
            )
            .with_hint("use Transaction::commit or Transaction::rollback"));
        }
        if matches!(&statement, BoundStatement::Vacuum { .. }) {
            return Err(DbError::new(
                "25001",
                "VACUUM cannot run inside a transaction block",
            ));
        }
        if let Some(stream) = prepare_read_stream(&snapshot, statement.clone(), params, None)? {
            return stream.collect::<Result<Vec<_>>>().map(QueryStream::new);
        }
        let sequence_id = sequence_mutation_id(&statement);
        let write_scope = statement_write_scope(&statement);
        let maintenance =
            maintenance_context(self.transactions.as_ref(), self.transaction_status.as_ref())?;
        let (candidate, events, dirty) = execute_bound_candidate(
            &snapshot,
            statement,
            params,
            self.authorization.as_ref(),
            Some(version_mutation_context(&self.transaction)?),
            maintenance,
        )?;
        if !dirty {
            return Ok(QueryStream::new(events));
        }
        match write_scope {
            StatementWriteScope::Dml => {
                let mut acquired = acquire_dml_locks(
                    self.locks,
                    &self.transaction,
                    &snapshot,
                    &candidate,
                    &self.lock_guards,
                    None,
                )?;
                self.lock_guards.append(&mut acquired);
                if self.base.is_none() {
                    self.base = Some(snapshot.clone());
                }
            }
            StatementWriteScope::Exclusive => {
                if self.working.is_some() && self.dml_only {
                    self.lock_guards.clear();
                    let base = self.base.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its base snapshot")
                    })?;
                    let working = self.working.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its working state")
                    })?;
                    let (upgraded_base, mut upgraded_working, lease, lock) =
                        upgrade_dml_candidate_to_exclusive(
                            DmlUpgradeAuthorities {
                                state: self.state,
                                statuses: self.transaction_status.as_ref(),
                                locks: self.locks,
                                writer: self.writer,
                            },
                            &self.transaction,
                            base,
                            working,
                            None,
                        )?;
                    upgraded_working.sequence_currvals = self.sequence_currvals.clone();
                    let (candidate, events, dirty) = execute_candidate(
                        &upgraded_working,
                        sql,
                        params,
                        StatementExecutionContext {
                            dialect: self.dialect,
                            runtime_metadata: &self.runtime_metadata,
                            authorization: self.authorization.as_ref(),
                        },
                        Some(version_mutation_context(&self.transaction)?),
                        maintenance,
                    )?;
                    if !dirty {
                        return Err(internal_error(
                            "exclusive statement unexpectedly produced a clean candidate",
                        ));
                    }
                    if let Some(sequence_id) = sequence_id {
                        self.sequence_currvals.insert(
                            sequence_id,
                            candidate_sequence_value(&candidate, sequence_id)?,
                        );
                    }
                    *self.sequence_currvals = candidate.sequence_currvals.clone();
                    self.base = Some(upgraded_base);
                    self.working = Some(candidate);
                    self.lease = Some(lease);
                    self.lock_guards.push(lock);
                    self.dml_only = false;
                    return Ok(QueryStream::new(events));
                }
                if self.lease.is_none() {
                    self.lease = Some(self.writer.try_acquire(self.transaction.transaction_id())?);
                    self.lock_guards.push(acquire_compatibility_write_lock(
                        self.locks,
                        &self.transaction,
                        None,
                    )?);
                }
                self.dml_only = false;
                if self.base.is_none() {
                    self.base = Some(snapshot.clone());
                }
            }
            StatementWriteScope::ReadOnly => {
                return Err(internal_error(
                    "read-only statement unexpectedly produced a dirty candidate",
                ));
            }
        }
        if self.working.is_some() || write_scope == StatementWriteScope::Dml {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            *self.sequence_currvals = candidate.sequence_currvals.clone();
            self.working = Some(candidate);
            return Ok(QueryStream::new(events));
        }

        let mut committed = committed_snapshot(self.state)?;
        committed.sequence_currvals = self.sequence_currvals.clone();
        let (candidate, events, dirty) = execute_candidate(
            &committed,
            sql,
            params,
            StatementExecutionContext {
                dialect: self.dialect,
                runtime_metadata: &self.runtime_metadata,
                authorization: self.authorization.as_ref(),
            },
            Some(version_mutation_context(&self.transaction)?),
            maintenance,
        )?;
        if dirty {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            *self.sequence_currvals = candidate.sequence_currvals.clone();
            self.working = Some(candidate);
        }
        Ok(QueryStream::new(events))
    }
}

#[derive(Debug)]
pub struct QueryStream {
    events: std::vec::IntoIter<QueryEvent>,
}

pub struct TryQueryStream {
    state: TryQueryStreamState,
    failed: bool,
    failure_flag: Option<Arc<AtomicBool>>,
    cancellation: Option<Arc<AtomicBool>>,
    execution_memory_peak_bytes: Option<usize>,
    _event_reservation: Option<Reservation>,
}

enum TryQueryStreamState {
    Events(std::vec::IntoIter<Result<QueryEvent>>),
    Select(Box<SelectStreamState>),
    Done,
}

struct SelectStreamState {
    schema: Schema,
    cursor: StreamBatchCursor,
    phase: SelectStreamPhase,
    rows_processed: u64,
    emitted_batch: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectStreamPhase {
    Schema,
    Batches,
    EmptyBatch,
    Progress,
    Complete,
    Done,
}

enum StreamBatchCursor {
    Simple(Box<ExecutionCursor>),
    Advanced(Box<AdvancedExecutionCursor>),
}

impl StreamBatchCursor {
    fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Self::Simple(cursor) => cursor.next_batch(),
            Self::Advanced(cursor) => cursor.next_batch(),
        }
    }

    fn memory_peak_bytes(&self) -> usize {
        match self {
            Self::Simple(cursor) => cursor.memory().peak_bytes(),
            Self::Advanced(cursor) => cursor.memory_peak_bytes(),
        }
    }
}
