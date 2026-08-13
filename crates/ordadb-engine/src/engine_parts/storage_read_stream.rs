
impl TableScan for StorageTableScanV2 {
    fn next_chunk(
        &mut self,
        max_rows: usize,
        grant: &MemoryGrant,
    ) -> Result<Option<LeasedDataChunk>> {
        if max_rows == 0 {
            self.lease = None;
            return Err(DbError::new(
                "22023",
                "table scan chunk size must be positive",
            ));
        }
        let expected_rows = self.rows.len();
        if self.offset >= expected_rows {
            self.lease = None;
            return Ok(None);
        }
        let end = self.offset.saturating_add(max_rows).min(expected_rows);
        match LeasedDataChunk::from_snapshot(Arc::clone(&self.rows), self.offset, end, grant) {
            Ok(chunk) => {
                self.offset = end;
                if self.offset == expected_rows {
                    self.lease = None;
                }
                Ok(Some(chunk))
            }
            Err(error) => {
                self.lease = None;
                Err(error)
            }
        }
    }
}

fn prepare_read_stream(
    state: &DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    table_provider: Option<&dyn TableProvider>,
) -> Result<Option<TryQueryStream>> {
    prepare_read_stream_with_options(
        state,
        statement,
        params,
        table_provider,
        &ExecutionOptions::default(),
    )
}

fn prepare_read_stream_with_options(
    state: &DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    table_provider: Option<&dyn TableProvider>,
    options: &ExecutionOptions,
) -> Result<Option<TryQueryStream>> {
    match statement {
        BoundStatement::Select {
            table_id,
            schema,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => {
            let (schema, cursor) = prepare_select_cursor(
                state,
                SelectExecution {
                    table_id,
                    schema,
                    projection,
                    filter,
                    order_by,
                    offset,
                    limit,
                },
                params,
                table_provider,
                options,
            )?;
            Ok(Some(TryQueryStream::select(
                schema,
                StreamBatchCursor::Simple(Box::new(cursor)),
                state.cancellation.clone(),
            )))
        }
        BoundStatement::AdvancedSelect {
            table,
            joins,
            applies,
            windows,
            schema,
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
            aggregate,
        } => {
            let (schema, cursor) = prepare_advanced_cursor(
                state,
                AdvancedExecution {
                    table,
                    joins,
                    applies,
                    windows,
                    schema,
                    projection,
                    distinct,
                    filter,
                    group_by,
                    having,
                    order_by,
                    offset,
                    limit: limit.map(|limit| *limit),
                    aggregate,
                },
                params,
                options,
            )?;
            Ok(Some(TryQueryStream::select(
                schema,
                StreamBatchCursor::Advanced(Box::new(cursor)),
                state.cancellation.clone(),
            )))
        }
        _ => Ok(None),
    }
}

#[derive(Debug, Clone, Copy)]
struct VersionMutationContext {
    transaction_id: TransactionId,
    command_id: u32,
}

#[derive(Clone, Copy)]
struct StatementExecutionContext<'a> {
    dialect: SqlDialect,
    runtime_metadata: &'a SessionRuntimeMetadata,
    authorization: Option<&'a SessionAuthorization>,
}

#[derive(Clone, Copy)]
struct MaintenanceContext<'a> {
    horizon: TransactionId,
    expired_snapshot: Option<TransactionId>,
    statuses: &'a TransactionStatusStore,
}

fn maintenance_context<'a>(
    transactions: &TransactionManager,
    statuses: &'a TransactionStatusStore,
) -> Result<MaintenanceContext<'a>> {
    Ok(MaintenanceContext {
        horizon: transactions.global_xmin()?,
        expired_snapshot: transactions.expired_snapshot()?,
        statuses,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementWriteScope {
    ReadOnly,
    Dml,
    Exclusive,
}

fn statement_write_scope(statement: &BoundStatement) -> StatementWriteScope {
    match statement {
        BoundStatement::Insert { .. }
        | BoundStatement::ViewInsert { .. }
        | BoundStatement::Merge(_)
        | BoundStatement::Update { .. }
        | BoundStatement::ViewUpdate { .. }
        | BoundStatement::Delete { .. }
        | BoundStatement::ViewDelete { .. } => StatementWriteScope::Dml,
        BoundStatement::Select { .. }
        | BoundStatement::AdvancedSelect { .. }
        | BoundStatement::SetOperation { .. }
        | BoundStatement::With { .. }
        | BoundStatement::ViewSelect { .. }
        | BoundStatement::ScalarSelect { .. }
        | BoundStatement::RoutineSelect { .. }
        | BoundStatement::Explain { .. }
        | BoundStatement::NoOp { .. } => StatementWriteScope::ReadOnly,
        _ => StatementWriteScope::Exclusive,
    }
}

fn reject_system_catalog_write(statement: &BoundStatement) -> Result<()> {
    let mut pending = vec![statement];
    while let Some(statement) = pending.pop() {
        let target = match statement {
            BoundStatement::Insert { table_id, .. }
            | BoundStatement::Update { table_id, .. }
            | BoundStatement::Delete { table_id, .. } => Some(*table_id),
            BoundStatement::Merge(merge) => Some(merge.target.table_id),
            BoundStatement::With { body, .. } => {
                pending.push(body);
                None
            }
            _ => None,
        };
        if target.is_some_and(Catalog::is_system_table) {
            return Err(
                DbError::new("42501", "system catalog relations are read-only")
                    .with_hint("query pg_catalog and information_schema with SELECT"),
            );
        }
    }
    Ok(())
}

fn statement_read_predicates(statement: &BoundStatement) -> Vec<PredicateLock> {
    statement_read_table_ids(statement)
        .into_iter()
        .map(|table_id| PredicateLock::Table {
            table_id: table_id.get(),
        })
        .collect()
}

fn statement_read_table_ids(statement: &BoundStatement) -> BTreeSet<TableId> {
    let mut table_ids = BTreeSet::new();
    let mut pending = vec![statement];
    while let Some(statement) = pending.pop() {
        match statement {
            BoundStatement::Select { table_id, .. } => {
                table_ids.insert(*table_id);
            }
            BoundStatement::AdvancedSelect {
                table,
                joins,
                applies,
                ..
            } => {
                table_ids.insert(table.table_id);
                for join in joins {
                    match &join.source {
                        BoundJoinSource::Table(table) => {
                            table_ids.insert(table.table_id);
                        }
                        BoundJoinSource::Derived { query, .. } => pending.push(query),
                    }
                }
                pending.extend(applies.iter().map(|apply| apply.query.as_ref()));
            }
            BoundStatement::SetOperation { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundStatement::With { ctes, body, .. } => {
                pending.push(body);
                for cte in ctes {
                    pending.push(&cte.seed);
                    if let Some(recursive) = &cte.recursive {
                        pending.push(recursive);
                    }
                }
            }
            BoundStatement::Merge(merge) => {
                table_ids.insert(merge.target.table_id);
                table_ids.insert(merge.source.table_id);
            }
            BoundStatement::ViewSelect { source, .. }
            | BoundStatement::ViewInsert { source, .. }
            | BoundStatement::ViewUpdate { source, .. }
            | BoundStatement::ViewDelete { source, .. }
            | BoundStatement::Explain { statement: source } => {
                pending.push(source);
            }
            _ => {}
        }
    }
    table_ids
}

fn changed_table_ids(before: &DatabaseState, after: &DatabaseState) -> BTreeSet<TableId> {
    before
        .versions
        .keys()
        .chain(after.versions.keys())
        .copied()
        .filter(|table_id| {
            before.versions.get(table_id) != after.versions.get(table_id)
                || before.visible_versions.get(table_id) != after.visible_versions.get(table_id)
        })
        .collect()
}

fn acquire_compatibility_write_lock(
    locks: &Arc<LockManager>,
    transaction: &DurableTransaction,
    cancelled: Option<&AtomicBool>,
) -> Result<LockGuard> {
    locks.acquire(
        transaction.transaction_id(),
        LockKey::Database,
        LockMode::Exclusive,
        None,
        cancelled,
    )
}

fn acquire_dml_locks(
    locks: &Arc<LockManager>,
    transaction: &DurableTransaction,
    before: &DatabaseState,
    after: &DatabaseState,
    existing: &[LockGuard],
    cancelled: Option<&AtomicBool>,
) -> Result<Vec<LockGuard>> {
    let mut keys = BTreeSet::from([LockKey::Database]);
    let transaction_id = transaction.transaction_id();
    let table_ids = before
        .versions
        .keys()
        .chain(after.versions.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for table_id in table_ids {
        let before_versions = before
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        let after_versions = after
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        let changed = before_versions != after_versions
            || before.visible_versions.get(&table_id) != after.visible_versions.get(&table_id);
        if !changed {
            continue;
        }
        keys.insert(LockKey::Table {
            table_id: table_id.get(),
        });
        for (before_version, after_version) in before_versions.iter().zip(after_versions) {
            if before_version.header.xmax == 0 && after_version.header.xmax == transaction_id.get()
            {
                keys.insert(LockKey::Row {
                    table_id: table_id.get(),
                    version_id: u64::from(before_version.version_id),
                });
            }
        }
        let before_visible = before
            .visible_versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice())
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let Some(table) = after.catalog.table_by_id(table_id) else {
            continue;
        };
        let after_rows = after
            .rows
            .get(&table_id)
            .map_or(&[][..], |rows| rows.as_slice());
        let after_visible = after
            .visible_versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        for (row, version_id) in after_rows.iter().zip(after_visible) {
            if before_visible.contains(version_id) {
                continue;
            }
            for index in table.indexes().filter(|index| index.unique) {
                let key_values = index
                    .key_columns
                    .iter()
                    .map(|column_id| {
                        table
                            .column_index_by_id(*column_id)
                            .and_then(|position| row.values.get(position))
                            .cloned()
                            .ok_or_else(|| {
                                internal_error("unique-index lock column is outside its row")
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if key_values.iter().any(Value::is_null) {
                    continue;
                }
                let encoded = serde_json::to_vec(&key_values)
                    .map_err(|error| internal_error(error.to_string()))?;
                let fingerprint: [u8; 32] = Sha256::digest(encoded).into();
                keys.insert(LockKey::IndexKey {
                    index_id: index.id.get(),
                    fingerprint,
                });
            }
        }
    }
    let existing = existing
        .iter()
        .map(|guard| guard.key().clone())
        .collect::<BTreeSet<_>>();
    let mut acquired = Vec::new();
    for key in keys {
        if existing.contains(&key) {
            continue;
        }
        let mode = if key == LockKey::Database || matches!(key, LockKey::Table { .. }) {
            LockMode::Shared
        } else {
            LockMode::Exclusive
        };
        acquired.push(locks.acquire(transaction_id, key, mode, None, cancelled)?);
    }
    Ok(acquired)
}

fn version_mutation_context(transaction: &DurableTransaction) -> Result<VersionMutationContext> {
    Ok(VersionMutationContext {
        transaction_id: transaction.transaction_id(),
        command_id: transaction
            .snapshot()
            .ok_or_else(|| DbError::new("25P01", "transaction is no longer active"))?
            .command_id,
    })
}

fn execute_bound_candidate(
    state: &DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    authorization: Option<&SessionAuthorization>,
    version_context: Option<VersionMutationContext>,
    maintenance: MaintenanceContext<'_>,
) -> Result<(DatabaseState, Vec<QueryEvent>, bool)> {
    let mut candidate = state.clone();
    candidate.triggers_fired = 0;
    candidate.routine_frames.clear();
    candidate.pending_notices.clear();
    candidate.pending_notifications = NotificationTransactionState::default();
    candidate.authorization = authorization.cloned();
    let reconciles_versions = !matches!(
        &statement,
        BoundStatement::Analyze { .. } | BoundStatement::Vacuum { .. }
    );
    let (mut events, dirty) = execute_root_bound(&mut candidate, statement, params, maintenance)?;
    insert_pending_notices(&mut events, mem::take(&mut candidate.pending_notices));
    if dirty
        && reconciles_versions
        && let Some(version_context) = version_context
    {
        reconcile_version_changes(state, &mut candidate, version_context)?;
    }
    candidate.cancellation = None;
    candidate.authorization = None;
    Ok((candidate, events, dirty))
}

fn execute_candidate(
    state: &DatabaseState,
    sql: &str,
    params: &[Value],
    context: StatementExecutionContext<'_>,
    version_context: Option<VersionMutationContext>,
    maintenance: MaintenanceContext<'_>,
) -> Result<(DatabaseState, Vec<QueryEvent>, bool)> {
    let parsed = parse_with_dialect(sql, context.dialect)?;
    let statement = bind_with_session(
        parsed,
        &state.catalog,
        context.runtime_metadata.bind_values(),
    )?;
    let mut candidate = state.clone();
    candidate.triggers_fired = 0;
    candidate.routine_frames.clear();
    candidate.pending_notices.clear();
    candidate.pending_notifications = NotificationTransactionState::default();
    candidate.authorization = context.authorization.cloned();
    let reconciles_versions = !matches!(
        &statement,
        BoundStatement::Analyze { .. } | BoundStatement::Vacuum { .. }
    );
    let (mut events, dirty) = execute_root_bound(&mut candidate, statement, params, maintenance)?;
    insert_pending_notices(&mut events, mem::take(&mut candidate.pending_notices));
    if dirty
        && reconciles_versions
        && let Some(version_context) = version_context
    {
        reconcile_version_changes(state, &mut candidate, version_context)?;
    }
    candidate.cancellation = None;
    candidate.authorization = None;
    Ok((candidate, events, dirty))
}

fn insert_pending_notices(events: &mut Vec<QueryEvent>, notices: Vec<DbNotice>) {
    if notices.is_empty() {
        return;
    }
    let position = usize::from(matches!(events.first(), Some(QueryEvent::Schema(_))));
    events.splice(
        position..position,
        notices.into_iter().map(QueryEvent::Notice),
    );
}

fn reconcile_version_changes(
    before: &DatabaseState,
    after: &mut DatabaseState,
    context: VersionMutationContext,
) -> Result<()> {
    let table_ids = before
        .rows
        .keys()
        .chain(after.rows.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for table_id in table_ids {
        let Some(after_rows) = after.rows.get(&table_id).map(|rows| (**rows).clone()) else {
            after.versions.remove(&table_id);
            after.visible_versions.remove(&table_id);
            continue;
        };
        let before_rows = before
            .rows
            .get(&table_id)
            .map(|rows| (**rows).clone())
            .unwrap_or_default();
        let before_ids = before
            .visible_versions
            .get(&table_id)
            .map(|versions| (**versions).clone())
            .unwrap_or_default();
        if before_rows.len() != before_ids.len() {
            return Err(internal_error(format!(
                "table {} visible row/version state is not aligned",
                table_id.get()
            )));
        }
        let mut versions = before
            .versions
            .get(&table_id)
            .map(|versions| (**versions).clone())
            .unwrap_or_default();
        let visible_ids = reconcile_table_version_changes(
            &before_rows,
            &before_ids,
            &after_rows,
            &mut versions,
            context,
        )?;
        after.versions.insert(table_id, Arc::new(versions));
        after
            .visible_versions
            .insert(table_id, Arc::new(visible_ids));
    }
    Ok(())
}

fn reconcile_table_version_changes(
    before_rows: &[Row],
    before_ids: &[u32],
    after_rows: &[Row],
    versions: &mut Vec<VersionedRow>,
    context: VersionMutationContext,
) -> Result<Vec<u32>> {
    if before_rows.len() == after_rows.len() {
        return before_rows
            .iter()
            .zip(before_ids)
            .zip(after_rows)
            .map(|((before, version_id), after)| {
                if before == after {
                    Ok(*version_id)
                } else {
                    update_version(versions, *version_id, after, context)
                }
            })
            .collect();
    }
    if is_subsequence(before_rows, after_rows) {
        let mut before_index = 0_usize;
        let mut visible = Vec::with_capacity(after_rows.len());
        for row in after_rows {
            if before_index < before_rows.len() && row == &before_rows[before_index] {
                visible.push(before_ids[before_index]);
                before_index += 1;
            } else {
                visible.push(append_version(versions, row, 0, context)?);
            }
        }
        return Ok(visible);
    }
    if is_subsequence(after_rows, before_rows) {
        let mut before_index = 0_usize;
        let mut visible = Vec::with_capacity(after_rows.len());
        for row in after_rows {
            while before_index < before_rows.len() && &before_rows[before_index] != row {
                delete_version(versions, before_ids[before_index], context)?;
                before_index += 1;
            }
            if before_index == before_rows.len() {
                return Err(internal_error(
                    "row subsequence changed while deriving version deletes",
                ));
            }
            visible.push(before_ids[before_index]);
            before_index += 1;
        }
        for version_id in &before_ids[before_index..] {
            delete_version(versions, *version_id, context)?;
        }
        return Ok(visible);
    }

    let shared = before_rows.len().min(after_rows.len());
    let mut visible = Vec::with_capacity(after_rows.len());
    for index in 0..shared {
        if before_rows[index] == after_rows[index] {
            visible.push(before_ids[index]);
        } else {
            visible.push(update_version(
                versions,
                before_ids[index],
                &after_rows[index],
                context,
            )?);
        }
    }
    for version_id in &before_ids[shared..] {
        delete_version(versions, *version_id, context)?;
    }
    for row in &after_rows[shared..] {
        visible.push(append_version(versions, row, 0, context)?);
    }
    Ok(visible)
}

fn is_subsequence(needle: &[Row], haystack: &[Row]) -> bool {
    let mut index = 0_usize;
    for row in haystack {
        if index < needle.len() && row == &needle[index] {
            index += 1;
        }
    }
    index == needle.len()
}

fn update_version(
    versions: &mut Vec<VersionedRow>,
    previous_version: u32,
    row: &Row,
    context: VersionMutationContext,
) -> Result<u32> {
    delete_version(versions, previous_version, context)?;
    append_version(versions, row, previous_version, context)
}

fn delete_version(
    versions: &mut [VersionedRow],
    version_id: u32,
    context: VersionMutationContext,
) -> Result<()> {
    let version = version_id
        .checked_sub(1)
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| versions.get_mut(index))
        .ok_or_else(|| internal_error("visible version ID is outside its table version state"))?;
    if version.header.xmax != 0 {
        return Err(DbError::new(
            "40001",
            "tuple version changed since the transaction snapshot",
        )
        .with_hint("retry the transaction with a fresh snapshot"));
    }
    version.header.xmax = context.transaction_id.get();
    version.header.command_id = context.command_id;
    Ok(())
}

fn append_version(
    versions: &mut Vec<VersionedRow>,
    row: &Row,
    previous_version: u32,
    context: VersionMutationContext,
) -> Result<u32> {
    let version_id = u32::try_from(versions.len())
        .ok()
        .and_then(|version_id| version_id.checked_add(1))
        .ok_or_else(|| DbError::new("54000", "table version ordinal space is exhausted"))?;
    if previous_version >= version_id {
        return Err(internal_error(
            "new tuple predecessor is not earlier than its version ordinal",
        ));
    }
    let mut header = TupleHeaderV2::frozen(row)?;
    header.xmin = context.transaction_id.get();
    header.command_id = context.command_id;
    header.previous_version = previous_version;
    versions.push(VersionedRow {
        version_id,
        header,
        row: row.clone(),
    });
    Ok(version_id)
}

fn merge_dml_candidate(
    latest: &DatabaseState,
    base: &DatabaseState,
    candidate: &DatabaseState,
    transaction: &DurableTransaction,
    statuses: &TransactionStatusStore,
) -> Result<DatabaseState> {
    let transaction_id = transaction.transaction_id();
    let mut merged = latest.clone();
    let table_ids = base
        .versions
        .keys()
        .chain(candidate.versions.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for table_id in table_ids {
        let base_versions = base
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        let candidate_versions = candidate
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        if base_versions == candidate_versions
            && base.visible_versions.get(&table_id) == candidate.visible_versions.get(&table_id)
        {
            continue;
        }
        if candidate_versions.len() < base_versions.len() {
            return Err(internal_error(
                "DML candidate removed authoritative tuple versions",
            ));
        }
        let mut latest_versions = merged
            .versions
            .get(&table_id)
            .map(|versions| (**versions).clone())
            .ok_or_else(|| {
                DbError::new(
                    "40001",
                    format!("table {} changed during the transaction", table_id.get()),
                )
            })?;
        if latest_versions.len() < base_versions.len() {
            return Err(internal_error(
                "latest tuple-version state is shorter than the transaction base",
            ));
        }
        for (index, (base_version, candidate_version)) in
            base_versions.iter().zip(candidate_versions).enumerate()
        {
            if base_version == candidate_version {
                continue;
            }
            if base_version.row != candidate_version.row
                || base_version.version_id != candidate_version.version_id
                || base_version.header.xmax != 0
                || candidate_version.header.xmax != transaction_id.get()
            {
                return Err(internal_error(
                    "DML candidate changed an existing tuple outside its deletion header",
                ));
            }
            let latest_version = latest_versions
                .get_mut(index)
                .ok_or_else(|| internal_error("latest tuple version disappeared during merge"))?;
            if latest_version != base_version {
                return Err(DbError::new(
                    "40001",
                    "tuple version changed since the transaction snapshot",
                )
                .with_hint("retry the transaction with a fresh snapshot"));
            }
            latest_version.header = candidate_version.header;
        }
        let mut remapped = BTreeMap::<u32, u32>::new();
        for candidate_version in &candidate_versions[base_versions.len()..] {
            if candidate_version.header.xmin != transaction_id.get() {
                return Err(internal_error(
                    "DML candidate appended a version owned by another transaction",
                ));
            }
            let version_id = u32::try_from(latest_versions.len())
                .ok()
                .and_then(|version_id| version_id.checked_add(1))
                .ok_or_else(|| DbError::new("54000", "table version ordinal space is exhausted"))?;
            let mut appended = candidate_version.clone();
            let previous = appended.header.previous_version;
            if previous > u32::try_from(base_versions.len()).unwrap_or(u32::MAX) {
                appended.header.previous_version = *remapped
                    .get(&previous)
                    .ok_or_else(|| internal_error("DML candidate predecessor was not remapped"))?;
            }
            appended.version_id = version_id;
            remapped.insert(candidate_version.version_id, version_id);
            latest_versions.push(appended);
        }
        merged.versions.insert(table_id, Arc::new(latest_versions));
    }
    project_current_database_visibility(merged, transaction_id, statuses)
}

fn refresh_read_committed_candidate(
    state: &Arc<RwLock<DatabaseState>>,
    statuses: &TransactionStatusStore,
    transaction: &DurableTransaction,
    base: &mut Option<DatabaseState>,
    working: &mut Option<DatabaseState>,
    dml_only: bool,
) -> Result<()> {
    if !dml_only
        || transaction.characteristics().is_none_or(|characteristics| {
            characteristics.isolation_level != IsolationLevel::ReadCommitted
        })
        || working.is_none()
    {
        return Ok(());
    }
    let previous_base = base
        .as_ref()
        .ok_or_else(|| internal_error("DML transaction is missing its base snapshot"))?;
    let previous_working = working
        .as_ref()
        .ok_or_else(|| internal_error("DML transaction is missing its working state"))?;
    let transaction_snapshot = transaction
        .snapshot()
        .ok_or_else(|| no_active_transaction_error("refresh a statement snapshot"))?;
    let refreshed_base = project_database_visibility(
        committed_snapshot(state)?,
        transaction_snapshot,
        transaction.transaction_id(),
        statuses,
    )?;
    let refreshed_working = merge_dml_candidate(
        &refreshed_base,
        previous_base,
        previous_working,
        transaction,
        statuses,
    )?;
    *base = Some(refreshed_base);
    *working = Some(refreshed_working);
    Ok(())
}

fn read_committed_statement_state(
    state: &Arc<RwLock<DatabaseState>>,
    statuses: &TransactionStatusStore,
    transaction: &mut DurableTransaction,
    base: Option<&DatabaseState>,
    working: Option<&DatabaseState>,
    cancellation: Option<Arc<AtomicBool>>,
) -> Result<DatabaseState> {
    let transaction_snapshot = transaction.begin_statement()?.clone();
    let mut latest = project_database_visibility(
        committed_snapshot(state)?,
        &transaction_snapshot,
        transaction.transaction_id(),
        statuses,
    )?;
    latest.cancellation = cancellation;
    match (base, working) {
        (Some(base), Some(working)) => {
            merge_dml_candidate(&latest, base, working, transaction, statuses)
        }
        (None, None) => Ok(latest),
        _ => Err(internal_error(
            "DML transaction has incomplete base/working state during conflict recheck",
        )),
    }
}
