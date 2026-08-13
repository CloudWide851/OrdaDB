
struct DmlUpgradeAuthorities<'a> {
    state: &'a Arc<RwLock<DatabaseState>>,
    statuses: &'a TransactionStatusStore,
    locks: &'a Arc<LockManager>,
    writer: &'a Arc<WriterCoordinator>,
}

fn upgrade_dml_candidate_to_exclusive(
    authorities: DmlUpgradeAuthorities<'_>,
    transaction: &DurableTransaction,
    base: &DatabaseState,
    working: &DatabaseState,
    cancellation: Option<Arc<AtomicBool>>,
) -> Result<(DatabaseState, DatabaseState, WriterLease, LockGuard)> {
    let lease = authorities
        .writer
        .try_acquire(transaction.transaction_id())?;
    let lock =
        acquire_compatibility_write_lock(authorities.locks, transaction, cancellation.as_deref())?;
    let mut latest = committed_snapshot(authorities.state)?;
    latest.cancellation = cancellation;
    let characteristics = transaction
        .characteristics()
        .ok_or_else(|| no_active_transaction_error("upgrade transaction locks"))?;
    let upgraded = if characteristics.isolation_level == IsolationLevel::ReadCommitted {
        merge_dml_candidate(&latest, base, working, transaction, authorities.statuses)?
    } else {
        if latest.generation != base.generation {
            return Err(DbError::new(
                "40001",
                "could not serialize DDL after concurrent database changes",
            )
            .with_hint("retry the transaction"));
        }
        let mut working = working.clone();
        working.cancellation = latest.cancellation.clone();
        working
    };
    Ok((latest, upgraded, lease, lock))
}

fn project_current_database_visibility(
    state: DatabaseState,
    current_transaction: TransactionId,
    statuses: &TransactionStatusStore,
) -> Result<DatabaseState> {
    let status = statuses.snapshot()?;
    let xmax = TransactionId::new(status.next_transaction_id)
        .ok_or_else(|| internal_error("transaction status high-water mark is zero"))?;
    let snapshot = TransactionSnapshot {
        xmin: xmax,
        xmax,
        in_progress: Arc::new(BTreeSet::new()),
        command_id: u32::MAX,
    };
    project_database_visibility(state, &snapshot, current_transaction, statuses)
}

fn persist_candidate(
    state: &mut DatabaseState,
    store: &Arc<Mutex<DatabaseStore>>,
    storage_access: &Arc<StorageAccessGate>,
    wal: &Arc<WalManager>,
    transaction: &mut DurableTransaction,
    mut candidate: DatabaseState,
) -> Result<()> {
    candidate.sequence_currvals.clear();
    candidate.generation = state.generation.checked_add(1).ok_or_else(|| {
        DbError::new("54000", "database generation space is exhausted")
            .with_hint("create a logical backup before retrying on a fresh database")
    })?;
    let persistent = PersistentState::from(&candidate);
    let _storage_write_lease = storage_access.acquire_write()?;
    let mut store = store
        .lock()
        .map_err(|_| internal_error("database store lock is poisoned"))?;
    let mut prepared = store.prepare_commit(&persistent)?;
    let transaction_id = transaction.transaction_id();
    let logged = wal.log_prepared(transaction_id, &mut prepared)?;
    transaction.mark_status_committed()?;
    store.apply_prepared_with_observer(&prepared, |point| {
        wal.check_fault(match point {
            ApplyPoint::BeforePageWrite(_) => FaultPoint::BeforeDataPageWrite,
            ApplyPoint::AfterPageWrite(_) => FaultPoint::AfterDataPageWrite,
            ApplyPoint::BeforeResize { .. } => FaultPoint::BeforeDataResize,
            ApplyPoint::AfterResize { .. } => FaultPoint::AfterDataResize,
            ApplyPoint::BeforeSync => FaultPoint::BeforeDataSync,
            ApplyPoint::AfterSync => FaultPoint::AfterDataSync,
        })
    })?;
    wal.commit(&logged)?;
    store.publish_prepared(prepared)?;
    transaction.finish_commit()?;
    *state = candidate;
    Ok(())
}

fn checkpoint_shared(
    state: &Arc<RwLock<DatabaseState>>,
    store: &Arc<Mutex<DatabaseStore>>,
    wal: &Arc<WalManager>,
    transactions: &Arc<TransactionManager>,
) -> Result<()> {
    let durable_data_generation = state
        .read()
        .map_err(|_| internal_error("engine state lock is poisoned"))?
        .generation;
    let data_file_page_count = store
        .lock()
        .map_err(|_| internal_error("database store lock is poisoned"))?
        .page_count()?;
    let mut active_transactions = BTreeMap::new();
    for transaction_id in transactions.active_transactions()? {
        if let Some(last_lsn) = wal.last_lsn(transaction_id)? {
            active_transactions.insert(transaction_id, last_lsn);
        }
    }
    wal.checkpoint(CheckpointState {
        active_transactions,
        dirty_pages: wal.dirty_pages()?,
        visibility_horizon: Some(transactions.global_xmin()?),
        durable_data_generation,
        durable_wal_lsn: wal.durable_lsn()?,
        data_file_page_count,
    })?;
    Ok(())
}

fn record_commit_and_maybe_checkpoint(
    state: &Arc<RwLock<DatabaseState>>,
    store: &Arc<Mutex<DatabaseStore>>,
    wal: &Arc<WalManager>,
    transactions: &Arc<TransactionManager>,
    commits_since_checkpoint: &AtomicU64,
) -> Result<()> {
    let count = commits_since_checkpoint
        .fetch_add(1, Ordering::AcqRel)
        .checked_add(1)
        .ok_or_else(|| DbError::new("54000", "automatic checkpoint commit counter overflowed"))?;
    if count < AUTOMATIC_CHECKPOINT_INTERVAL {
        return Ok(());
    }
    checkpoint_shared(state, store, wal, transactions)?;
    commits_since_checkpoint.store(0, Ordering::Release);
    Ok(())
}

fn execute_transaction_session_command(
    transaction: &mut ActiveSqlTransaction,
    statement: &BoundStatement,
    params: &[Value],
) -> Result<Option<TryQueryStream>> {
    if let BoundStatement::PgNotify {
        channel,
        payload,
        schema,
    } = statement
    {
        let (channel, payload) = evaluate_pg_notify(channel, payload, params)?;
        transaction.notification_state.notify(channel, payload);
        return Ok(Some(TryQueryStream::new(pg_notify_events(schema.clone()))));
    }
    let tag = match statement {
        BoundStatement::Listen { channel } => {
            transaction.notification_state.listen(channel.clone());
            "LISTEN"
        }
        BoundStatement::Unlisten { channel } => {
            transaction.notification_state.unlisten(channel.clone());
            "UNLISTEN"
        }
        BoundStatement::Notify { channel, payload } => {
            transaction
                .notification_state
                .notify(channel.clone(), payload.clone());
            "NOTIFY"
        }
        BoundStatement::DeallocateAll => "DEALLOCATE ALL",
        BoundStatement::DiscardAll => {
            return Err(DbError::new(
                "25001",
                "DISCARD ALL cannot run inside a transaction block",
            ));
        }
        _ => return Ok(None),
    };
    Ok(Some(TryQueryStream::new(transaction_events(tag))))
}

fn transaction_events(tag: &str) -> Vec<QueryEvent> {
    command_events(Schema::empty(), tag, 0, None)
}

fn evaluate_pg_notify(
    channel: &BoundExpr,
    payload: &BoundExpr,
    params: &[Value],
) -> Result<(Identifier, String)> {
    let Value::Text(channel) = evaluate_scalar(channel, &[], params)? else {
        return Err(DbError::new("22004", "pg_notify channel must not be NULL"));
    };
    let Value::Text(payload) = evaluate_scalar(payload, &[], params)? else {
        return Err(DbError::new("22004", "pg_notify payload must not be NULL"));
    };
    if channel.is_empty() || channel.len() > ordadb_types::MAX_POSTGRES_NAME_BYTES {
        return Err(DbError::new(
            "42622",
            "notification channel name is empty or too long",
        ));
    }
    if channel.contains('\0') || payload.contains('\0') {
        return Err(DbError::new(
            "22021",
            "notification channel and payload cannot contain NUL",
        ));
    }
    if payload.len() > 7_999 {
        return Err(DbError::new("22023", "NOTIFY payload is too long"));
    }
    let identifier = if channel
        .chars()
        .all(|character| !character.is_ascii_uppercase())
    {
        Identifier::unquoted(channel)
    } else {
        Identifier::quoted(channel)
    };
    Ok((identifier, payload))
}

fn pg_notify_events(schema: Schema) -> Vec<QueryEvent> {
    command_events(
        schema.clone(),
        "SELECT 1",
        1,
        Some(Batch {
            schema,
            rows: vec![Row::new(vec![Value::Null])],
        }),
    )
}

fn no_active_transaction_error(action: &str) -> DbError {
    DbError::new(
        "25P01",
        format!("cannot {action} because no transaction is active"),
    )
}

fn failed_transaction_error() -> DbError {
    DbError::new(
        "25P02",
        "the current transaction is aborted; commands are ignored until ROLLBACK",
    )
    .with_hint("issue ROLLBACK before starting new work")
}

fn execute_root_bound(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    maintenance: MaintenanceContext<'_>,
) -> Result<(Vec<QueryEvent>, bool)> {
    match statement {
        BoundStatement::Analyze { table_id } => {
            authorize_statement_ownership(
                &state.catalog,
                &BoundStatement::Analyze { table_id },
                state.authorization.as_ref(),
            )?;
            execute_analyze(state, table_id)
        }
        BoundStatement::Vacuum { table_id, analyze } => {
            authorize_statement_ownership(
                &state.catalog,
                &BoundStatement::Vacuum { table_id, analyze },
                state.authorization.as_ref(),
            )?;
            execute_vacuum(state, table_id, analyze, maintenance)
        }
        BoundStatement::Reindex { target } => {
            authorize_statement_ownership(
                &state.catalog,
                &BoundStatement::Reindex { target },
                state.authorization.as_ref(),
            )?;
            execute_reindex(state, target)
        }
        statement => execute_bound_with_ownership(state, statement, params),
    }
}

fn execute_bound_with_ownership(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let authorization = state.authorization.clone();
    authorize_statement_ownership(&state.catalog, &statement, authorization.as_ref())?;
    let previous_catalog = authorization
        .as_ref()
        .filter(|_| statement_may_create_catalog_objects(&statement))
        .map(|_| Arc::clone(&state.catalog));
    let (events, dirty) = execute_bound(state, statement, params)?;
    if dirty
        && let (Some(authorization), Some(previous_catalog)) =
            (authorization.as_ref(), previous_catalog.as_deref())
    {
        Arc::make_mut(&mut state.catalog)
            .assign_new_object_owners(previous_catalog, authorization.owner())?;
    }
    Ok((events, dirty))
}

fn authorize_statement_ownership(
    catalog: &Catalog,
    statement: &BoundStatement,
    authorization: Option<&SessionAuthorization>,
) -> Result<()> {
    let Some(authorization) =
        authorization.filter(|authorization| !authorization.bypasses_ownership())
    else {
        return Ok(());
    };
    let mut objects = Vec::new();
    let schema_object = |schema: &Identifier| {
        catalog
            .schema(schema)
            .map(|schema| CatalogObjectRef::Schema(schema.id))
    };
    match statement {
        BoundStatement::CreateEnumType { schema, .. }
        | BoundStatement::CreateDomain { schema, .. }
        | BoundStatement::CreateTable { schema, .. }
        | BoundStatement::CreateSequence { schema, .. } => {
            objects.extend(schema_object(schema));
        }
        BoundStatement::CreateView {
            schema, existing, ..
        } => match existing {
            Some(view_id) => objects.push(CatalogObjectRef::View(*view_id)),
            None => objects.extend(schema_object(schema)),
        },
        BoundStatement::CreateRoutine {
            schema,
            name,
            kind,
            arguments,
            replace,
            ..
        } => {
            if *replace
                && let Some(routine) = catalog.routine_by_signature(schema, name, *kind, arguments)
            {
                objects.push(CatalogObjectRef::Routine(routine.id));
            } else {
                objects.extend(schema_object(schema));
            }
        }
        BoundStatement::AlterEnumAddValue { type_id, .. }
        | BoundStatement::AlterEnumRenameValue { type_id, .. }
        | BoundStatement::AlterDomain { type_id, .. } => {
            objects.push(CatalogObjectRef::Type(*type_id));
        }
        BoundStatement::AlterSchemaRename { schema_id, .. } => {
            objects.push(CatalogObjectRef::Schema(*schema_id));
        }
        BoundStatement::Analyze {
            table_id: Some(table_id),
        }
        | BoundStatement::Vacuum {
            table_id: Some(table_id),
            ..
        } => objects.push(CatalogObjectRef::Table(*table_id)),
        BoundStatement::Reindex { target } => match target {
            BoundReindexTarget::Index(index_id) => {
                objects.push(CatalogObjectRef::Index(*index_id));
            }
            BoundReindexTarget::Table(table_id) => {
                objects.push(CatalogObjectRef::Table(*table_id));
            }
            BoundReindexTarget::Schema(schema_id) => {
                objects.push(CatalogObjectRef::Schema(*schema_id));
            }
            BoundReindexTarget::Database => {
                objects.extend(
                    catalog
                        .database()
                        .schemas()
                        .map(|schema| CatalogObjectRef::Schema(schema.id)),
                );
            }
        },
        BoundStatement::DropObjects {
            objects: dropped, ..
        } => objects.extend(dropped.iter().copied()),
        BoundStatement::AlterTable { table_id, .. }
        | BoundStatement::CreateIndex { table_id, .. } => {
            objects.push(CatalogObjectRef::Table(*table_id));
        }
        BoundStatement::CreateTrigger { target, .. } => objects.push(target.object_ref()),
        BoundStatement::AlterIndexRename { index_id, .. } => {
            objects.push(CatalogObjectRef::Index(*index_id));
        }
        BoundStatement::AlterSequenceRename { sequence_id, .. }
        | BoundStatement::AlterSequence { sequence_id, .. } => {
            objects.push(CatalogObjectRef::Sequence(*sequence_id));
        }
        BoundStatement::AlterViewRename { view_id, .. }
        | BoundStatement::RefreshMaterializedView { view_id, .. } => {
            objects.push(CatalogObjectRef::View(*view_id));
        }
        BoundStatement::DropRoutine { routine_id, .. } => {
            objects.push(CatalogObjectRef::Routine(*routine_id));
        }
        BoundStatement::DropTrigger { trigger_id, .. } => {
            objects.push(CatalogObjectRef::Trigger(*trigger_id));
        }
        _ => {}
    }
    for object in objects {
        let Some(owner) = catalog.owner_of(object) else {
            continue;
        };
        if owner != authorization.owner() {
            return Err(
                DbError::new("42501", "must be owner of catalog object").with_detail(format!(
                    "authenticated role {} does not own {object:?}",
                    authorization.owner().as_str()
                )),
            );
        }
    }
    Ok(())
}

fn statement_may_create_catalog_objects(statement: &BoundStatement) -> bool {
    matches!(
        statement,
        BoundStatement::CreateSchema { .. }
            | BoundStatement::CreateEnumType { .. }
            | BoundStatement::CreateDomain { .. }
            | BoundStatement::CreateTable { .. }
            | BoundStatement::AlterTable { .. }
            | BoundStatement::CreateIndex { .. }
            | BoundStatement::CreateSequence { .. }
            | BoundStatement::CreateView { .. }
            | BoundStatement::CreateRoutine { .. }
            | BoundStatement::CreateTrigger { .. }
    )
}

fn execute_analyze(
    state: &mut DatabaseState,
    table_id: Option<TableId>,
) -> Result<(Vec<QueryEvent>, bool)> {
    let table_ids = maintenance_table_ids(state, table_id)?;
    for table_id in &table_ids {
        rebuild_table_derived(state, *table_id)?;
    }
    Ok((
        command_events(
            Schema::empty(),
            "ANALYZE",
            u64::try_from(table_ids.len()).unwrap_or(u64::MAX),
            None,
        ),
        true,
    ))
}

fn execute_reindex(
    state: &mut DatabaseState,
    target: BoundReindexTarget,
) -> Result<(Vec<QueryEvent>, bool)> {
    let table_ids = match target {
        BoundReindexTarget::Index(index_id) => {
            ensure_statement_not_cancelled(state)?;
            rebuild_index_derived(state, index_id)?;
            Vec::new()
        }
        BoundReindexTarget::Table(table_id) => {
            table_definition(state, table_id)?;
            vec![table_id]
        }
        BoundReindexTarget::Schema(schema_id) => state
            .catalog
            .schema_by_id(schema_id)
            .ok_or_else(|| DbError::new("3F000", "schema does not exist"))?
            .tables()
            .map(|table| table.id)
            .collect(),
        BoundReindexTarget::Database => state
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .map(|table| table.id)
            .collect(),
    };
    for table_id in table_ids {
        ensure_statement_not_cancelled(state)?;
        rebuild_table_indexes(state, table_id)?;
    }
    Ok((command_events(Schema::empty(), "REINDEX", 0, None), true))
}

fn execute_vacuum(
    state: &mut DatabaseState,
    table_id: Option<TableId>,
    _analyze: bool,
    maintenance: MaintenanceContext<'_>,
) -> Result<(Vec<QueryEvent>, bool)> {
    if let Some(transaction_id) = maintenance.expired_snapshot {
        return Err(DbError::new(
            "55000",
            format!(
                "VACUUM cannot proceed while transaction {transaction_id} holds an expired snapshot"
            ),
        )
        .with_hint("commit or roll back the long-running transaction before retrying VACUUM"));
    }
    let table_ids = maintenance_table_ids(state, table_id)?;
    let mut reclaimed = 0_u64;
    for table_id in &table_ids {
        reclaimed = reclaimed
            .checked_add(vacuum_table_versions(state, *table_id, maintenance)?)
            .ok_or_else(|| DbError::new("54000", "VACUUM reclaimed-row count overflow"))?;
        rebuild_table_derived(state, *table_id)?;
    }
    Ok((
        command_events(Schema::empty(), "VACUUM", reclaimed, None),
        true,
    ))
}

fn maintenance_table_ids(state: &DatabaseState, table_id: Option<TableId>) -> Result<Vec<TableId>> {
    if let Some(table_id) = table_id {
        table_definition(state, table_id)?;
        return Ok(vec![table_id]);
    }
    Ok(state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .map(|table| table.id)
        .collect())
}

fn vacuum_table_versions(
    state: &mut DatabaseState,
    table_id: TableId,
    maintenance: MaintenanceContext<'_>,
) -> Result<u64> {
    let original = state
        .versions
        .get(&table_id)
        .map(|versions| (**versions).clone())
        .unwrap_or_default();
    let mut retained = Vec::with_capacity(original.len());
    let mut id_map = BTreeMap::new();
    for version in &original {
        if version_reclaimable(version, maintenance)? {
            continue;
        }
        let new_id = u32::try_from(retained.len())
            .ok()
            .and_then(|id| id.checked_add(1))
            .ok_or_else(|| DbError::new("54000", "table version ordinal space is exhausted"))?;
        let mut retained_version = version.clone();
        freeze_retained_version(&mut retained_version, maintenance)?;
        id_map.insert(version.version_id, new_id);
        retained.push((version.version_id, retained_version));
    }
    for (old_id, version) in &mut retained {
        let mut predecessor = version.header.previous_version;
        let mut traversed = 0_usize;
        while predecessor != 0 && !id_map.contains_key(&predecessor) {
            traversed = traversed
                .checked_add(1)
                .ok_or_else(|| DbError::new("54001", "tuple predecessor depth overflow"))?;
            if traversed > original.len() {
                return Err(DbError::new(
                    "XX001",
                    "tuple predecessor chain is cyclic during VACUUM",
                ));
            }
            predecessor = original
                .get(usize::try_from(predecessor.saturating_sub(1)).unwrap_or(usize::MAX))
                .ok_or_else(|| {
                    DbError::new(
                        "XX001",
                        "tuple predecessor points outside the version sequence",
                    )
                })?
                .header
                .previous_version;
        }
        version.version_id = *id_map
            .get(old_id)
            .ok_or_else(|| internal_error("retained tuple version was not remapped"))?;
        version.header.previous_version = if predecessor == 0 {
            0
        } else {
            *id_map
                .get(&predecessor)
                .ok_or_else(|| internal_error("retained tuple predecessor was not remapped"))?
        };
    }
    let visible = state
        .visible_versions
        .get(&table_id)
        .map_or(&[][..], |versions| versions.as_slice())
        .iter()
        .map(|old_id| {
            id_map.get(old_id).copied().ok_or_else(|| {
                internal_error("VACUUM attempted to reclaim a currently visible tuple")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let reclaimed = original.len().saturating_sub(retained.len());
    state.versions.insert(
        table_id,
        Arc::new(retained.into_iter().map(|(_, version)| version).collect()),
    );
    state.visible_versions.insert(table_id, Arc::new(visible));
    u64::try_from(reclaimed).map_err(|_| DbError::new("54000", "VACUUM count overflow"))
}

fn version_reclaimable(
    version: &VersionedRow,
    maintenance: MaintenanceContext<'_>,
) -> Result<bool> {
    let creator = TransactionId::new(version.header.xmin)
        .ok_or_else(|| DbError::new("XX001", "tuple creator transaction ID is zero"))?;
    let creator_outcome = if version.header.xmin == FROZEN_TRANSACTION_ID {
        TransactionOutcome::Committed
    } else {
        maintenance.statuses.transaction_outcome(creator)?
    };
    if creator < maintenance.horizon && creator_outcome == TransactionOutcome::Aborted {
        return Ok(true);
    }
    if creator_outcome != TransactionOutcome::Committed || version.header.xmax == 0 {
        return Ok(false);
    }
    let deleter = TransactionId::new(version.header.xmax)
        .ok_or_else(|| DbError::new("XX001", "tuple deleter transaction ID is zero"))?;
    Ok(deleter < maintenance.horizon
        && maintenance.statuses.transaction_outcome(deleter)? == TransactionOutcome::Committed)
}

fn freeze_retained_version(
    version: &mut VersionedRow,
    maintenance: MaintenanceContext<'_>,
) -> Result<()> {
    if version.header.xmin != FROZEN_TRANSACTION_ID {
        let creator = TransactionId::new(version.header.xmin)
            .ok_or_else(|| DbError::new("XX001", "tuple creator transaction ID is zero"))?;
        if creator < maintenance.horizon
            && maintenance.statuses.transaction_outcome(creator)? == TransactionOutcome::Committed
        {
            version.header.xmin = FROZEN_TRANSACTION_ID;
        }
    }
    let Some(deleter) = TransactionId::new(version.header.xmax) else {
        return Ok(());
    };
    if deleter < maintenance.horizon {
        match maintenance.statuses.transaction_outcome(deleter)? {
            TransactionOutcome::Aborted => version.header.xmax = 0,
            TransactionOutcome::Committed => {
                return Err(internal_error(
                    "VACUUM retained a tuple deleted before the safe horizon",
                ));
            }
            TransactionOutcome::InProgress => {}
        }
    }
    Ok(())
}
