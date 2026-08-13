impl Session {
    pub fn set_backend_process_id(&mut self, process_id: u32) -> Result<()> {
        self.notifications
            .set_process_id(self.notification_session_id, process_id)
    }

    pub fn drain_notifications(&mut self) -> Result<Vec<DatabaseNotification>> {
        self.notifications.drain(self.notification_session_id)
    }

    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        match self
            .execute_stream(sql, params)?
            .collect::<Result<Vec<_>>>()
        {
            Ok(events) => Ok(QueryStream::new(events)),
            Err(error) => {
                self.fail_sql_transaction();
                Err(error)
            }
        }
    }

    /// Bind a statement against the session's current catalog snapshot without
    /// executing it and return the row schema exposed to protocol clients.
    pub fn describe(&mut self, sql: &str) -> Result<Schema> {
        self.describe_statement(sql)
            .map(|description| description.schema)
    }

    /// Bind a statement without executing it and return both its result schema
    /// and the positional parameter types inferred by the binder.
    pub fn describe_statement(&mut self, sql: &str) -> Result<StatementDescription> {
        self.normalize_sql_transaction_failure();
        if self.transaction_status() == TransactionStatus::Failed {
            return Err(failed_transaction_error());
        }
        let snapshot = self.statement_snapshot()?;
        let described = parse_with_dialect(sql, self.options.dialect)
            .and_then(|statement| {
                bind_with_session(
                    statement,
                    &snapshot.catalog,
                    self.runtime_metadata.bind_values(),
                )
            })
            .and_then(|statement| {
                Ok(StatementDescription {
                    schema: bound_statement_schema(&statement),
                    parameter_types: bound_statement_parameter_types(&statement)?,
                })
            });
        if described.is_err() {
            self.fail_sql_transaction();
        }
        described
    }

    pub fn execute_stream(&mut self, sql: &str, params: &[Value]) -> Result<TryQueryStream> {
        self.execute_stream_controlled(sql, params, None)
    }

    pub fn execute_stream_with_cancellation(
        &mut self,
        sql: &str,
        params: &[Value],
        cancellation: Arc<AtomicBool>,
    ) -> Result<TryQueryStream> {
        self.execute_stream_controlled(sql, params, Some(cancellation))
    }

    pub fn set_runtime_metadata(&mut self, metadata: SessionRuntimeMetadata) {
        self.runtime_metadata = metadata;
    }

    pub fn refresh_system_catalog_metadata(
        &mut self,
        roles: Vec<CatalogRoleMetadata>,
        settings: Vec<CatalogSettingMetadata>,
        visibility: CatalogVisibility,
    ) -> Result<()> {
        let authorization = self.authorization.as_mut().ok_or_else(|| {
            DbError::new(
                "55000",
                "system catalog role metadata requires an authenticated session",
            )
        })?;
        authorization.replace_system_catalog_metadata(roles, settings)?;
        authorization.catalog_visibility = visibility;
        Ok(())
    }

    fn execute_stream_controlled(
        &mut self,
        sql: &str,
        params: &[Value],
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Result<TryQueryStream> {
        self.normalize_sql_transaction_failure();
        let transaction_was_failed = self.transaction_status() == TransactionStatus::Failed;
        let parsed = match parse_with_dialect(sql, self.options.dialect) {
            Ok(parsed) => parsed,
            Err(_) if transaction_was_failed => {
                return Err(failed_transaction_error());
            }
            Err(error) => {
                self.fail_sql_transaction();
                return Err(error);
            }
        };
        if transaction_was_failed {
            return match parsed {
                ParsedStatement::Rollback { chain } => self.rollback_sql_transaction(chain),
                ParsedStatement::RollbackTo { name } => self.rollback_to_sql_savepoint(&name.name),
                _ => Err(failed_transaction_error()),
            };
        }
        if !parsed_is_transaction_control(&parsed)
            && let Err(error) = self.begin_active_statement(cancellation.as_deref())
        {
            self.fail_sql_transaction();
            return Err(error);
        }
        let mut snapshot = self.statement_snapshot()?;
        snapshot.cancellation = cancellation;
        snapshot.sequence_currvals = self.sequence_currvals.clone();
        let statement = match bind_with_session(
            parsed,
            &snapshot.catalog,
            self.runtime_metadata.bind_values(),
        )
        .and_then(|statement| resolve_sequence_currval(statement, &self.sequence_currvals))
        {
            Ok(statement) => statement,
            Err(error) => {
                self.fail_sql_transaction();
                return Err(error);
            }
        };
        if let Err(error) = reject_system_catalog_write(&statement) {
            self.fail_sql_transaction();
            return Err(error);
        }
        if statement_write_scope(&statement) == StatementWriteScope::ReadOnly {
            let system_table_ids = statement_read_table_ids(&statement)
                .into_iter()
                .filter(|table_id| Catalog::is_system_table(*table_id))
                .collect::<BTreeSet<_>>();
            let system_catalog = match system_catalog::build_system_catalog_snapshot(
                &snapshot.catalog,
                self.authorization.as_ref(),
                &system_table_ids,
            ) {
                Ok(snapshot) => Arc::new(snapshot),
                Err(error) => {
                    self.fail_sql_transaction();
                    return Err(error);
                }
            };
            snapshot.rows.extend(
                system_catalog
                    .tables()
                    .iter()
                    .map(|(table_id, rows)| (*table_id, Arc::clone(rows))),
            );
            snapshot.system_catalog = Some(system_catalog);
        }

        match &statement {
            BoundStatement::Begin { characteristics } => {
                return self.begin_sql_transaction(*characteristics);
            }
            BoundStatement::Commit { chain } => return self.commit_sql_transaction(*chain),
            BoundStatement::Rollback { chain } => return self.rollback_sql_transaction(*chain),
            BoundStatement::Savepoint { name } => return self.create_sql_savepoint(name),
            BoundStatement::RollbackTo { name } => return self.rollback_to_sql_savepoint(name),
            BoundStatement::ReleaseSavepoint { name } => {
                return self.release_sql_savepoint(name);
            }
            _ => {}
        }
        match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
            SqlTransactionState::Idle => self.execute_auto_commit(sql, params, snapshot, statement),
            SqlTransactionState::Active(mut transaction) => {
                match self.execute_in_sql_transaction(
                    &mut transaction,
                    sql,
                    params,
                    snapshot,
                    statement,
                ) {
                    Ok(stream) => {
                        if let Err(error) = transaction.transaction.finish_statement() {
                            transaction.failed = true;
                            self.sql_transaction = SqlTransactionState::Active(transaction);
                            return Err(error);
                        }
                        let stream =
                            stream.with_failure_flag(Arc::clone(&transaction.stream_failed));
                        self.sql_transaction = SqlTransactionState::Active(transaction);
                        Ok(stream)
                    }
                    Err(error) => {
                        transaction.failed = true;
                        self.sql_transaction = SqlTransactionState::Active(transaction);
                        Err(error)
                    }
                }
            }
            SqlTransactionState::Failed(characteristics) => {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                Err(failed_transaction_error())
            }
        }
    }

    pub fn begin(&mut self) -> Result<Transaction<'_>> {
        if self.transaction_status() != TransactionStatus::Idle {
            return Err(DbError::new(
                "25001",
                "a SQL transaction is already active in this session",
            )
            .with_hint("commit or roll back the SQL transaction before using Session::begin"));
        }
        let transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            TransactionCharacteristics::default(),
        )?;
        Ok(Transaction {
            state: &self.state,
            store: &self.store,
            storage_access: &self.storage_access,
            wal: &self.wal,
            transaction_status: &self.transaction_status,
            transactions: &self.transactions,
            locks: &self.locks,
            writer: &self.writer,
            commits_since_checkpoint: &self.commits_since_checkpoint,
            sequence_currvals: &mut self.sequence_currvals,
            dialect: self.options.dialect,
            authorization: self.authorization.clone(),
            runtime_metadata: self.runtime_metadata.clone(),
            transaction,
            base: None,
            working: None,
            lock_guards: Vec::new(),
            lease: None,
            dml_only: true,
            failed: false,
        })
    }

    #[must_use]
    pub fn transaction_status(&self) -> TransactionStatus {
        match &self.sql_transaction {
            SqlTransactionState::Idle => TransactionStatus::Idle,
            SqlTransactionState::Active(transaction)
                if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) =>
            {
                TransactionStatus::Failed
            }
            SqlTransactionState::Active(_) => TransactionStatus::Active,
            SqlTransactionState::Failed(_) => TransactionStatus::Failed,
        }
    }

    /// Mark an explicit SQL transaction as failed when a protocol adapter
    /// executes a statement through a specialized path outside the normal
    /// bound-statement dispatcher.
    pub fn mark_transaction_failed(&mut self) {
        self.normalize_sql_transaction_failure();
        self.fail_sql_transaction();
    }

    pub fn search(&self, request: SearchRequest) -> Result<Vec<SearchResult>> {
        self.search_with_filter(request, None)
    }

    pub fn search_with_filter(
        &self,
        request: SearchRequest,
        filter: Option<&ScalarSearchFilter>,
    ) -> Result<Vec<SearchResult>> {
        let snapshot = self.statement_snapshot()?;
        match request {
            SearchRequest::Text(mut request) => {
                let table_id =
                    search_index_table(&snapshot, request.index_id, IndexMethod::FullText)?;
                if let Some(filter) = filter {
                    let allowed = evaluate_search_filter(&snapshot, table_id, filter)?;
                    request.allowed_rows = intersect_allowed_rows(request.allowed_rows, allowed);
                }
                snapshot
                    .searches
                    .text_search(&request)?
                    .into_iter()
                    .map(|hit| {
                        Ok(SearchResult {
                            row_id: hit.row_id,
                            row: search_result_row(&snapshot, table_id, hit.row_id)?,
                            text_score: Some(hit.score),
                            vector_score: None,
                            distance: None,
                            combined_score: None,
                        })
                    })
                    .collect()
            }
            SearchRequest::Vector(mut request) => {
                let table_id = search_index_table(&snapshot, request.index_id, IndexMethod::Hnsw)?;
                if let Some(filter) = filter {
                    let allowed = evaluate_search_filter(&snapshot, table_id, filter)?;
                    request.allowed_rows = intersect_allowed_rows(request.allowed_rows, allowed);
                }
                snapshot
                    .searches
                    .vector_search(&request)?
                    .into_iter()
                    .map(|hit| {
                        Ok(SearchResult {
                            row_id: hit.row_id,
                            row: search_result_row(&snapshot, table_id, hit.row_id)?,
                            text_score: None,
                            vector_score: Some(hit.score),
                            distance: Some(hit.distance),
                            combined_score: None,
                        })
                    })
                    .collect()
            }
            SearchRequest::Hybrid(mut request) => {
                let text_table =
                    search_index_table(&snapshot, request.text.index_id, IndexMethod::FullText)?;
                let vector_table =
                    search_index_table(&snapshot, request.vector.index_id, IndexMethod::Hnsw)?;
                if text_table != vector_table {
                    return Err(DbError::new(
                        "22023",
                        "hybrid search indexes must belong to the same table",
                    ));
                }
                if let Some(filter) = filter {
                    let allowed = evaluate_search_filter(&snapshot, text_table, filter)?;
                    request.text.allowed_rows =
                        intersect_allowed_rows(request.text.allowed_rows, Arc::clone(&allowed));
                    request.vector.allowed_rows =
                        intersect_allowed_rows(request.vector.allowed_rows, allowed);
                }
                snapshot
                    .searches
                    .hybrid_search(&request)?
                    .into_iter()
                    .map(|hit| {
                        Ok(SearchResult {
                            row_id: hit.row_id,
                            row: search_result_row(&snapshot, text_table, hit.row_id)?,
                            text_score: Some(hit.text_score),
                            vector_score: Some(hit.vector_score),
                            distance: None,
                            combined_score: Some(hit.combined_score),
                        })
                    })
                    .collect()
            }
        }
    }

    #[must_use]
    pub const fn options(&self) -> SessionOptions {
        self.options
    }

    pub fn set_query_memory_limit(&mut self, hard_memory_bytes: usize) -> Result<()> {
        if hard_memory_bytes == 0 || hard_memory_bytes > DEFAULT_HARD_MEMORY_BYTES {
            return Err(DbError::new(
                "22023",
                "query memory limit must be between 1 byte and the server default",
            ));
        }
        self.execution_options.hard_memory_bytes = hard_memory_bytes;
        self.execution_options.soft_memory_bytes = DEFAULT_SOFT_MEMORY_BYTES.min(hard_memory_bytes);
        Ok(())
    }

    fn statement_snapshot(&self) -> Result<DatabaseState> {
        if let SqlTransactionState::Active(transaction) = &self.sql_transaction {
            if let Some(working) = &transaction.working {
                return Ok(working.clone());
            }
            let committed = committed_snapshot(&self.state)?;
            let snapshot = transaction
                .transaction
                .snapshot()
                .ok_or_else(|| no_active_transaction_error("take a statement snapshot"))?;
            return project_database_visibility(
                committed,
                snapshot,
                transaction.transaction.transaction_id(),
                self.transaction_status.as_ref(),
            );
        }
        committed_snapshot(&self.state)
    }

    fn begin_active_statement(&mut self, cancellation: Option<&AtomicBool>) -> Result<()> {
        let state = Arc::clone(&self.state);
        let transaction_status = Arc::clone(&self.transaction_status);
        if let SqlTransactionState::Active(transaction) = &mut self.sql_transaction {
            let snapshot = match cancellation {
                Some(cancellation) => transaction
                    .transaction
                    .begin_statement_with_cancellation(cancellation)?,
                None => transaction.transaction.begin_statement()?,
            }
            .clone();
            if let Some(ssi) = &transaction.ssi {
                ssi.refresh_snapshot(&snapshot)?;
            }
            refresh_read_committed_candidate(
                &state,
                transaction_status.as_ref(),
                &transaction.transaction,
                &mut transaction.base,
                &mut transaction.working,
                transaction.dml_only,
            )?;
        }
        Ok(())
    }

    fn begin_sql_transaction(
        &mut self,
        characteristics: TransactionCharacteristics,
    ) -> Result<TryQueryStream> {
        match self.transaction_status() {
            TransactionStatus::Idle => {
                self.sql_transaction =
                    SqlTransactionState::Active(self.new_active_sql_transaction(characteristics)?);
                Ok(TryQueryStream::new(transaction_events("BEGIN")))
            }
            TransactionStatus::Active => {
                Err(DbError::new("25001", "a transaction is already active")
                    .with_hint("commit or roll back the current transaction first"))
            }
            TransactionStatus::Failed => Err(failed_transaction_error()),
        }
    }

    fn new_active_sql_transaction(
        &self,
        characteristics: TransactionCharacteristics,
    ) -> Result<Box<ActiveSqlTransaction>> {
        let transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            characteristics,
        )?;
        let ssi = SsiTransactionGuard::begin(Arc::clone(&self.ssi), &transaction)?;
        Ok(Box::new(ActiveSqlTransaction {
            transaction,
            base: None,
            working: None,
            locks: Vec::new(),
            lease: None,
            dml_only: true,
            ssi,
            savepoints: SavepointStack::new(),
            savepoint_states: BTreeMap::new(),
            failed: false,
            stream_failed: Arc::new(AtomicBool::new(false)),
            notification_state: NotificationTransactionState::default(),
        }))
    }

    fn commit_sql_transaction(&mut self, chain: TransactionChain) -> Result<TryQueryStream> {
        let transaction = match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
            SqlTransactionState::Idle => return Err(no_active_transaction_error("commit")),
            SqlTransactionState::Failed(characteristics) => {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                return Err(failed_transaction_error());
            }
            SqlTransactionState::Active(transaction)
                if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) =>
            {
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(failed_transaction_error());
            }
            SqlTransactionState::Active(transaction) => transaction,
        };
        let characteristics = transaction
            .transaction
            .characteristics()
            .ok_or_else(|| no_active_transaction_error("commit"))?;
        let ActiveSqlTransaction {
            transaction: mut durable,
            base,
            working,
            mut locks,
            lease,
            dml_only,
            mut ssi,
            notification_state,
            ..
        } = *transaction;
        if let Some(ssi) = &mut ssi
            && let Err(error) = self
                .transactions
                .global_xmin_excluding(durable.transaction_id())
                .and_then(|horizon| ssi.validate_commit(horizon))
        {
            self.sql_transaction = SqlTransactionState::Failed(characteristics);
            return Err(error);
        }
        if let Some(candidate) = working {
            let mut state = match self.state.write() {
                Ok(state) => state,
                Err(_) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(internal_error("engine state lock is poisoned"));
                }
            };
            let candidate = if dml_only {
                let base = base.as_ref().ok_or_else(|| {
                    internal_error("DML transaction is missing its base snapshot")
                })?;
                match merge_dml_candidate(
                    &state,
                    base,
                    &candidate,
                    &durable,
                    self.transaction_status.as_ref(),
                ) {
                    Ok(candidate) => candidate,
                    Err(error) => {
                        self.sql_transaction = SqlTransactionState::Failed(characteristics);
                        return Err(error);
                    }
                }
            } else {
                candidate
            };
            if let Err(error) = persist_candidate(
                &mut state,
                &self.store,
                &self.storage_access,
                &self.wal,
                &mut durable,
                candidate,
            ) {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                return Err(error);
            }
            if let Some(ssi) = &mut ssi {
                ssi.finish();
            }
            drop(state);
            locks.clear();
            drop(lease);
            record_commit_and_maybe_checkpoint(
                &self.state,
                &self.store,
                &self.wal,
                &self.transactions,
                &self.commits_since_checkpoint,
            )?;
        } else {
            if let Err(error) = durable.commit_empty() {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                return Err(error);
            }
            if let Some(ssi) = &mut ssi {
                ssi.finish();
            }
        }
        self.notifications
            .commit(self.notification_session_id, notification_state);
        self.start_chained_sql_transaction(chain, characteristics)?;
        Ok(TryQueryStream::new(transaction_events("COMMIT")))
    }

    fn rollback_sql_transaction(&mut self, chain: TransactionChain) -> Result<TryQueryStream> {
        let characteristics =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => return Err(no_active_transaction_error("roll back")),
                SqlTransactionState::Active(transaction) => {
                    let characteristics = transaction
                        .transaction
                        .characteristics()
                        .ok_or_else(|| no_active_transaction_error("roll back"))?;
                    transaction.transaction.abort()?;
                    characteristics
                }
                SqlTransactionState::Failed(characteristics) => characteristics,
            };
        self.start_chained_sql_transaction(chain, characteristics)?;
        Ok(TryQueryStream::new(transaction_events("ROLLBACK")))
    }

    fn start_chained_sql_transaction(
        &mut self,
        chain: TransactionChain,
        characteristics: TransactionCharacteristics,
    ) -> Result<()> {
        if chain == TransactionChain::Chain {
            self.sql_transaction =
                SqlTransactionState::Active(self.new_active_sql_transaction(characteristics)?);
        }
        Ok(())
    }

    fn create_sql_savepoint(&mut self, name: &Identifier) -> Result<TryQueryStream> {
        let mut transaction =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => {
                    return Err(no_active_transaction_error("create a savepoint"));
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
        let command_id = transaction
            .transaction
            .snapshot()
            .ok_or_else(|| no_active_transaction_error("create a savepoint"))?
            .command_id;
        let ssi = match transaction
            .ssi
            .as_ref()
            .map(SsiTransactionGuard::savepoint)
            .transpose()
        {
            Ok(ssi) => ssi,
            Err(error) => {
                transaction.failed = true;
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(error);
            }
        };
        let id =
            match transaction
                .savepoints
                .push(name.as_str(), command_id, 0, transaction.locks.len())
            {
                Ok(id) => id,
                Err(error) => {
                    self.sql_transaction = SqlTransactionState::Active(transaction);
                    return Err(error);
                }
            };
        transaction.savepoint_states.insert(
            id,
            SqlSavepointState {
                base: transaction.base.clone(),
                working: transaction.working.clone(),
                sequence_currvals: self.sequence_currvals.clone(),
                lock_len: transaction.locks.len(),
                dml_only: transaction.dml_only,
                ssi,
                notification_state: transaction.notification_state.clone(),
            },
        );
        if let Err(error) = transaction.transaction.finish_statement() {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        self.sql_transaction = SqlTransactionState::Active(transaction);
        Ok(TryQueryStream::new(transaction_events("SAVEPOINT")))
    }
}
