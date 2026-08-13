impl Session {

    fn rollback_to_sql_savepoint(&mut self, name: &Identifier) -> Result<TryQueryStream> {
        let mut transaction =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => {
                    return Err(no_active_transaction_error("roll back to a savepoint"));
                }
                SqlTransactionState::Failed(characteristics) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(DbError::new(
                        "3B001",
                        format!("savepoint \"{name}\" does not exist"),
                    ));
                }
                SqlTransactionState::Active(transaction) => transaction,
            };
        let savepoint = match transaction.savepoints.rollback_to(name.as_str()) {
            Ok(savepoint) => savepoint,
            Err(error) => {
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(error);
            }
        };
        let saved = match transaction.savepoint_states.get(&savepoint.id).cloned() {
            Some(saved) => saved,
            None => {
                transaction.failed = true;
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(internal_error("savepoint state is missing"));
            }
        };
        let ssi_rollback = match (&transaction.ssi, &saved.ssi) {
            (Some(ssi), Some(savepoint)) => ssi.rollback_to(savepoint),
            (None, None) => Ok(()),
            _ => Err(internal_error("savepoint SSI state is inconsistent")),
        };
        if let Err(error) = ssi_rollback {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        transaction.base = saved.base;
        transaction.working = saved.working;
        transaction.locks.truncate(saved.lock_len);
        transaction.dml_only = saved.dml_only;
        transaction.notification_state = saved.notification_state;
        if transaction.working.is_none() {
            transaction.lease = None;
        }
        self.sequence_currvals = saved.sequence_currvals;
        let retained = transaction
            .savepoints
            .frames()
            .iter()
            .map(|frame| frame.id)
            .collect::<BTreeSet<_>>();
        transaction
            .savepoint_states
            .retain(|id, _| retained.contains(id));
        transaction.failed = false;
        transaction.stream_failed.store(false, Ordering::Release);
        if let Err(error) = transaction.transaction.finish_statement() {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        self.sql_transaction = SqlTransactionState::Active(transaction);
        Ok(TryQueryStream::new(transaction_events("ROLLBACK")))
    }

    fn release_sql_savepoint(&mut self, name: &Identifier) -> Result<TryQueryStream> {
        let mut transaction =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => {
                    return Err(no_active_transaction_error("release a savepoint"));
                }
                SqlTransactionState::Failed(characteristics) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(failed_transaction_error());
                }
                SqlTransactionState::Active(transaction) => transaction,
            };
        if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) {
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(failed_transaction_error());
        }
        if let Err(error) = transaction.savepoints.release(name.as_str()) {
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        let retained = transaction
            .savepoints
            .frames()
            .iter()
            .map(|frame| frame.id)
            .collect::<BTreeSet<_>>();
        transaction
            .savepoint_states
            .retain(|id, _| retained.contains(id));
        if let Err(error) = transaction.transaction.finish_statement() {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        self.sql_transaction = SqlTransactionState::Active(transaction);
        Ok(TryQueryStream::new(transaction_events("RELEASE")))
    }

    fn execute_auto_commit(
        &mut self,
        sql: &str,
        params: &[Value],
        snapshot: DatabaseState,
        statement: BoundStatement,
    ) -> Result<TryQueryStream> {
        let procedure = match &statement {
            BoundStatement::Call {
                routine_id,
                arguments,
                schema,
            } if snapshot
                .catalog
                .routine_by_id(*routine_id)
                .is_some_and(|routine| routine.kind == ordadb_catalog::RoutineKind::Procedure) =>
            {
                Some((*routine_id, arguments.clone(), schema.clone()))
            }
            _ => None,
        };
        if let Some((routine_id, arguments, schema)) = procedure {
            return self
                .execute_auto_commit_procedure(snapshot, routine_id, &arguments, schema, params);
        }
        if let Some(stream) = self.execute_auto_commit_session_command(&statement, params)? {
            return Ok(stream);
        }
        let table_provider = StorageTableProviderV2::new(
            Arc::clone(&self.store),
            Arc::clone(&self.storage_access),
            snapshot.generation,
            &snapshot.rows,
            snapshot.system_catalog.as_deref(),
        );
        if let Some(stream) = prepare_read_stream_with_options(
            &snapshot,
            statement.clone(),
            params,
            Some(&table_provider),
            &self.execution_options,
        )? {
            return Ok(stream);
        }
        let compacts_transaction_status =
            matches!(&statement, BoundStatement::Vacuum { table_id: None, .. });
        let sequence_id = sequence_mutation_id(&statement);
        let write_scope = statement_write_scope(&statement);
        let maintenance =
            maintenance_context(self.transactions.as_ref(), self.transaction_status.as_ref())?;
        let (mut preview, events, dirty) = execute_bound_candidate(
            &snapshot,
            statement,
            params,
            self.authorization.as_ref(),
            None,
            maintenance,
        )?;
        if !dirty {
            let pending = mem::take(&mut preview.pending_notifications);
            self.notifications
                .commit(self.notification_session_id, pending);
            return TryQueryStream::buffered(events);
        }

        let mut transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            TransactionCharacteristics::default(),
        )?;
        let mut lease = None;
        let mut write_locks = Vec::new();
        match write_scope {
            StatementWriteScope::Dml => {
                let (lock_candidate, _, lock_dirty) = execute_candidate(
                    &snapshot,
                    sql,
                    params,
                    StatementExecutionContext {
                        dialect: self.options.dialect,
                        runtime_metadata: &self.runtime_metadata,
                        authorization: self.authorization.as_ref(),
                    },
                    Some(version_mutation_context(&transaction)?),
                    maintenance,
                )?;
                if lock_dirty {
                    write_locks = acquire_dml_locks(
                        &self.locks,
                        &transaction,
                        &snapshot,
                        &lock_candidate,
                        &[],
                        snapshot.cancellation.as_deref(),
                    )?;
                }
            }
            StatementWriteScope::Exclusive => {
                lease = Some(self.writer.try_acquire(transaction.transaction_id())?);
                write_locks.push(acquire_compatibility_write_lock(
                    &self.locks,
                    &transaction,
                    snapshot.cancellation.as_deref(),
                )?);
            }
            StatementWriteScope::ReadOnly => {
                return Err(internal_error(
                    "read-only statement unexpectedly produced a dirty candidate",
                ));
            }
        }
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let mut committed = state.clone();
        committed.cancellation = snapshot.cancellation.clone();
        committed.sequence_currvals = self.sequence_currvals.clone();
        let (mut candidate, events, dirty) = execute_candidate(
            &committed,
            sql,
            params,
            StatementExecutionContext {
                dialect: self.options.dialect,
                runtime_metadata: &self.runtime_metadata,
                authorization: self.authorization.as_ref(),
            },
            Some(version_mutation_context(&transaction)?),
            maintenance,
        )?;
        let pending_notifications = mem::take(&mut candidate.pending_notifications);
        let runtime_sequence_currvals = candidate.sequence_currvals.clone();
        let stream = TryQueryStream::buffered(events)?;
        if dirty {
            let sequence_value = sequence_id
                .map(|sequence_id| candidate_sequence_value(&candidate, sequence_id))
                .transpose()?;
            persist_candidate(
                &mut state,
                &self.store,
                &self.storage_access,
                &self.wal,
                &mut transaction,
                candidate,
            )?;
            self.sequence_currvals = runtime_sequence_currvals;
            drop(state);
            drop(write_locks);
            drop(lease.take());
            record_commit_and_maybe_checkpoint(
                &self.state,
                &self.store,
                &self.wal,
                &self.transactions,
                &self.commits_since_checkpoint,
            )?;
            if compacts_transaction_status {
                self.transaction_status
                    .compact_before(maintenance.horizon)?;
                self.transactions.compact_before(maintenance.horizon)?;
            }
            if let Some((sequence_id, value)) = sequence_id.zip(sequence_value) {
                self.sequence_currvals.insert(sequence_id, value);
            }
        }
        self.notifications
            .commit(self.notification_session_id, pending_notifications);
        Ok(stream)
    }

    fn execute_auto_commit_procedure(
        &mut self,
        snapshot: DatabaseState,
        routine_id: ordadb_types::RoutineId,
        arguments: &[BoundExpr],
        schema: Schema,
        params: &[Value],
    ) -> Result<TryQueryStream> {
        let mut coordinator = ProcedureTransactionCoordinator::new(self, snapshot)?;
        let mut candidate = coordinator.base.clone();
        let execution = {
            let mut boundary = |boundary, candidate: &mut DatabaseState, dirty| {
                coordinator.boundary(boundary, candidate, dirty)
            };
            execute_routine_program_with_boundaries(
                &mut candidate,
                routine_id,
                arguments,
                params,
                Some(&mut boundary),
            )
        };
        let (output, dirty) = match execution {
            Ok(output) => output,
            Err(error) => {
                coordinator.abort();
                self.sequence_currvals = coordinator.runtime_sequence_currvals();
                return Err(error);
            }
        };
        if let Err(error) = coordinator.finish_final(&mut candidate, dirty) {
            coordinator.abort();
            self.sequence_currvals = coordinator.runtime_sequence_currvals();
            return Err(error);
        }
        self.sequence_currvals = coordinator.runtime_sequence_currvals();
        let row_count = u64::from(!schema.fields.is_empty());
        let batch = (!schema.fields.is_empty()).then(|| Batch {
            schema: schema.clone(),
            rows: vec![Row::new(output.output_parameters)],
        });
        let mut events = command_events(schema, "CALL", row_count, batch);
        insert_pending_notices(&mut events, mem::take(&mut coordinator.notices));
        TryQueryStream::buffered(events)
    }

    fn execute_in_sql_transaction(
        &mut self,
        transaction: &mut ActiveSqlTransaction,
        sql: &str,
        params: &[Value],
        snapshot: DatabaseState,
        statement: BoundStatement,
    ) -> Result<TryQueryStream> {
        if matches!(&statement, BoundStatement::Vacuum { .. }) {
            return Err(DbError::new(
                "25001",
                "VACUUM cannot run inside a transaction block",
            ));
        }
        if let Some(stream) = execute_transaction_session_command(transaction, &statement, params)?
        {
            return Ok(stream);
        }
        if let Some(ssi) = &transaction.ssi {
            for predicate in statement_read_predicates(&statement) {
                ssi.record_read(predicate)?;
            }
        }
        if let Some(stream) = prepare_read_stream_with_options(
            &snapshot,
            statement.clone(),
            params,
            None,
            &self.execution_options,
        )? {
            return Ok(stream);
        }
        let has_conflict_action = matches!(
            &statement,
            BoundStatement::Insert {
                on_conflict: Some(_),
                ..
            }
        );
        let read_committed =
            transaction
                .transaction
                .characteristics()
                .is_some_and(|characteristics| {
                    characteristics.isolation_level == IsolationLevel::ReadCommitted
                });
        let recheck_conflict_after_locks = has_conflict_action && read_committed;
        let sequence_id = sequence_mutation_id(&statement);
        let write_scope = statement_write_scope(&statement);
        let maintenance =
            maintenance_context(self.transactions.as_ref(), self.transaction_status.as_ref())?;
        let (mut candidate, mut events, dirty) = execute_bound_candidate(
            &snapshot,
            statement,
            params,
            self.authorization.as_ref(),
            Some(version_mutation_context(&transaction.transaction)?),
            maintenance,
        )?;
        if !dirty {
            transaction
                .notification_state
                .append(mem::take(&mut candidate.pending_notifications));
            return TryQueryStream::buffered(events);
        }
        if transaction
            .transaction
            .characteristics()
            .is_some_and(|characteristics| {
                characteristics.access_mode == TransactionAccessMode::ReadOnly
            })
        {
            return Err(DbError::new(
                "25006",
                "cannot execute a write in a read-only transaction",
            ));
        }
        let mut statement_base = snapshot.clone();
        match write_scope {
            StatementWriteScope::Dml => {
                let mut acquired = acquire_dml_locks(
                    &self.locks,
                    &transaction.transaction,
                    &snapshot,
                    &candidate,
                    &transaction.locks,
                    snapshot.cancellation.as_deref(),
                )?;
                transaction.locks.append(&mut acquired);
                if recheck_conflict_after_locks {
                    let mut completed = false;
                    for _ in 0..MAX_DML_LOCK_RECHECKS {
                        let mut recheck_base = read_committed_statement_state(
                            &self.state,
                            self.transaction_status.as_ref(),
                            &mut transaction.transaction,
                            transaction.base.as_ref(),
                            transaction.working.as_ref(),
                            snapshot.cancellation.clone(),
                        )?;
                        recheck_base.sequence_currvals = self.sequence_currvals.clone();
                        let (rechecked, rechecked_events, rechecked_dirty) = execute_candidate(
                            &recheck_base,
                            sql,
                            params,
                            StatementExecutionContext {
                                dialect: self.options.dialect,
                                runtime_metadata: &self.runtime_metadata,
                                authorization: self.authorization.as_ref(),
                            },
                            Some(version_mutation_context(&transaction.transaction)?),
                            maintenance,
                        )?;
                        if !rechecked_dirty {
                            return Err(internal_error(
                                "ON CONFLICT lock recheck produced a clean candidate",
                            ));
                        }
                        let mut additional = acquire_dml_locks(
                            &self.locks,
                            &transaction.transaction,
                            &recheck_base,
                            &rechecked,
                            &transaction.locks,
                            snapshot.cancellation.as_deref(),
                        )?;
                        if additional.is_empty() {
                            statement_base = recheck_base;
                            candidate = rechecked;
                            events = rechecked_events;
                            completed = true;
                            break;
                        }
                        transaction.locks.append(&mut additional);
                    }
                    if !completed {
                        return Err(DbError::new(
                            "54001",
                            "ON CONFLICT lock recheck exceeded its iteration limit",
                        )
                        .with_hint("Retry the transaction after concurrent writers finish."));
                    }
                } else if has_conflict_action {
                    let latest = committed_snapshot(&self.state)?;
                    let merge_base = transaction.base.as_ref().unwrap_or(&snapshot);
                    if let Err(error) = merge_dml_candidate(
                        &latest,
                        merge_base,
                        &candidate,
                        &transaction.transaction,
                        self.transaction_status.as_ref(),
                    ) {
                        if error.sql_state == "23505" {
                            return Err(DbError::new(
                                "40001",
                                "could not serialize access due to concurrent ON CONFLICT update",
                            )
                            .with_hint("Retry the transaction with a fresh snapshot."));
                        }
                        return Err(error);
                    }
                }
                if let Some(ssi) = &transaction.ssi {
                    for table_id in changed_table_ids(&statement_base, &candidate) {
                        ssi.record_write(PredicateLock::Table {
                            table_id: table_id.get(),
                        })?;
                    }
                }
                if transaction.base.is_none() {
                    transaction.base = Some(statement_base);
                }
            }
            StatementWriteScope::Exclusive => {
                if transaction.working.is_some() && transaction.dml_only {
                    transaction.locks.clear();
                    let base = transaction.base.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its base snapshot")
                    })?;
                    let working = transaction.working.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its working state")
                    })?;
                    let (upgraded_base, mut upgraded_working, lease, lock) =
                        upgrade_dml_candidate_to_exclusive(
                            DmlUpgradeAuthorities {
                                state: &self.state,
                                statuses: self.transaction_status.as_ref(),
                                locks: &self.locks,
                                writer: &self.writer,
                            },
                            &transaction.transaction,
                            base,
                            working,
                            snapshot.cancellation.clone(),
                        )?;
                    upgraded_working.sequence_currvals = self.sequence_currvals.clone();
                    let (mut candidate, events, dirty) = execute_candidate(
                        &upgraded_working,
                        sql,
                        params,
                        StatementExecutionContext {
                            dialect: self.options.dialect,
                            runtime_metadata: &self.runtime_metadata,
                            authorization: self.authorization.as_ref(),
                        },
                        Some(version_mutation_context(&transaction.transaction)?),
                        maintenance,
                    )?;
                    if !dirty {
                        return Err(internal_error(
                            "exclusive statement unexpectedly produced a clean candidate",
                        ));
                    }
                    let stream = TryQueryStream::buffered(events)?;
                    if let Some(sequence_id) = sequence_id {
                        self.sequence_currvals.insert(
                            sequence_id,
                            candidate_sequence_value(&candidate, sequence_id)?,
                        );
                    }
                    self.sequence_currvals = candidate.sequence_currvals.clone();
                    transaction
                        .notification_state
                        .append(mem::take(&mut candidate.pending_notifications));
                    transaction.base = Some(upgraded_base);
                    transaction.working = Some(candidate);
                    transaction.lease = Some(lease);
                    transaction.locks.push(lock);
                    transaction.dml_only = false;
                    return Ok(stream);
                }
                if transaction.lease.is_none() {
                    transaction.lease = Some(
                        self.writer
                            .try_acquire(transaction.transaction.transaction_id())?,
                    );
                    transaction.locks.push(acquire_compatibility_write_lock(
                        &self.locks,
                        &transaction.transaction,
                        snapshot.cancellation.as_deref(),
                    )?);
                }
                transaction.dml_only = false;
                if transaction.base.is_none() {
                    transaction.base = Some(snapshot.clone());
                }
            }
            StatementWriteScope::ReadOnly => {
                return Err(internal_error(
                    "read-only statement unexpectedly produced a dirty candidate",
                ));
            }
        }
        if transaction.working.is_some() || write_scope == StatementWriteScope::Dml {
            let stream = TryQueryStream::buffered(events)?;
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            self.sequence_currvals = candidate.sequence_currvals.clone();
            transaction
                .notification_state
                .append(mem::take(&mut candidate.pending_notifications));
            transaction.working = Some(candidate);
            return Ok(stream);
        }

        let mut committed = committed_snapshot(&self.state)?;
        committed.cancellation = snapshot.cancellation.clone();
        committed.sequence_currvals = self.sequence_currvals.clone();
        let (mut candidate, events, dirty) = execute_candidate(
            &committed,
            sql,
            params,
            StatementExecutionContext {
                dialect: self.options.dialect,
                runtime_metadata: &self.runtime_metadata,
                authorization: self.authorization.as_ref(),
            },
            Some(version_mutation_context(&transaction.transaction)?),
            maintenance,
        )?;
        let stream = TryQueryStream::buffered(events)?;
        transaction
            .notification_state
            .append(mem::take(&mut candidate.pending_notifications));
        if dirty {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            self.sequence_currvals = candidate.sequence_currvals.clone();
            transaction.working = Some(candidate);
        }
        Ok(stream)
    }

    fn execute_auto_commit_session_command(
        &mut self,
        statement: &BoundStatement,
        params: &[Value],
    ) -> Result<Option<TryQueryStream>> {
        let mut pending = NotificationTransactionState::default();
        if let BoundStatement::PgNotify {
            channel,
            payload,
            schema,
        } = statement
        {
            let (channel, payload) = evaluate_pg_notify(channel, payload, params)?;
            pending.notify(channel, payload);
            self.notifications
                .commit(self.notification_session_id, pending);
            return Ok(Some(TryQueryStream::new(pg_notify_events(schema.clone()))));
        }
        let tag = match statement {
            BoundStatement::Listen { channel } => {
                pending.listen(channel.clone());
                "LISTEN"
            }
            BoundStatement::Unlisten { channel } => {
                pending.unlisten(channel.clone());
                "UNLISTEN"
            }
            BoundStatement::Notify { channel, payload } => {
                pending.notify(channel.clone(), payload.clone());
                "NOTIFY"
            }
            BoundStatement::DiscardAll => {
                pending.unlisten(None);
                self.sequence_currvals.clear();
                "DISCARD ALL"
            }
            BoundStatement::DeallocateAll => "DEALLOCATE ALL",
            _ => return Ok(None),
        };
        self.notifications
            .commit(self.notification_session_id, pending);
        Ok(Some(TryQueryStream::new(transaction_events(tag))))
    }

    fn fail_sql_transaction(&mut self) {
        if let SqlTransactionState::Active(transaction) = &mut self.sql_transaction {
            transaction.failed = true;
        }
    }

    fn normalize_sql_transaction_failure(&mut self) {
        if let SqlTransactionState::Active(transaction) = &mut self.sql_transaction
            && transaction.stream_failed.load(Ordering::Acquire)
        {
            transaction.failed = true;
        }
    }
}

fn resolve_sequence_currval(
    statement: BoundStatement,
    currvals: &BTreeMap<SequenceId, i64>,
) -> Result<BoundStatement> {
    match statement {
        BoundStatement::SequenceValue {
            sequence_id,
            operation: BoundSequenceOperation::CurrentValue { .. },
            schema,
        } => Ok(BoundStatement::SequenceValue {
            sequence_id,
            operation: BoundSequenceOperation::CurrentValue {
                value: currvals.get(&sequence_id).copied(),
            },
            schema,
        }),
        statement => Ok(statement),
    }
}

fn sequence_mutation_id(statement: &BoundStatement) -> Option<SequenceId> {
    match statement {
        BoundStatement::SequenceValue {
            sequence_id,
            operation: BoundSequenceOperation::NextValue | BoundSequenceOperation::SetValue { .. },
            ..
        } => Some(*sequence_id),
        _ => None,
    }
}

fn candidate_sequence_value(state: &DatabaseState, sequence_id: SequenceId) -> Result<i64> {
    state
        .catalog
        .sequence_by_id(sequence_id)
        .map(|sequence| sequence.last_value)
        .ok_or_else(|| internal_error("mutated sequence disappeared from the candidate catalog"))
}
