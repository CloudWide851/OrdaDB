
impl Connection {
    fn run(&mut self) -> Result<()> {
        loop {
            self.deliver_pending_notifications()?;
            let Some(message) = self.next_frontend_message()? else {
                return Ok(());
            };
            if self.extended_state == ExtendedQueryState::FailedUntilSync {
                match failed_message_action(&message) {
                    FailedMessageAction::Synchronize => {
                        self.extended_state = ExtendedQueryState::Ready;
                        write_ready(&mut self.stream, transaction_status(&self.session))?;
                    }
                    FailedMessageAction::Terminate => return Ok(()),
                    FailedMessageAction::Flush => self.flush()?,
                    FailedMessageAction::Ignore => {}
                }
                continue;
            }
            let extended = !matches!(
                message,
                FrontendMessage::Query(_) | FrontendMessage::Terminate | FrontendMessage::Flush
            );
            let result = self.handle_message(message);
            if let Err(error) = result {
                write_error(&mut self.stream, &error)?;
                if extended {
                    self.extended_state = ExtendedQueryState::FailedUntilSync;
                } else {
                    write_ready(&mut self.stream, transaction_status(&self.session))?;
                }
            }
        }
    }

    fn next_frontend_message(&mut self) -> Result<Option<FrontendMessage>> {
        loop {
            match self
                .frontend_reader
                .poll(&mut self.stream, self.config.max_frame_bytes)?
            {
                FrontendRead::Message(message) => return Ok(Some(message)),
                FrontendRead::Closed => return Ok(None),
                FrontendRead::Pending => self.deliver_pending_notifications()?,
            }
        }
    }

    fn deliver_pending_notifications(&mut self) -> Result<()> {
        let notifications = match self.session.drain_notifications() {
            Ok(notifications) => notifications,
            Err(error) => {
                write_error(&mut self.stream, &error)?;
                self.flush()?;
                return Err(error);
            }
        };
        if notifications.is_empty() {
            return Ok(());
        }
        for notification in notifications {
            write_notification(
                &mut self.stream,
                notification.sender_process_id,
                &notification.channel,
                &notification.payload,
            )?;
        }
        self.flush()
    }

    fn handle_message(&mut self, message: FrontendMessage) -> Result<()> {
        match message {
            FrontendMessage::Query(sql) => self.simple_query(&sql),
            FrontendMessage::Parse {
                name,
                sql,
                parameter_oids,
            } => self.parse(name, sql, parameter_oids),
            FrontendMessage::Bind {
                portal,
                statement,
                parameter_formats,
                parameters,
                result_formats,
            } => self.bind(
                portal,
                &statement,
                &parameter_formats,
                &parameters,
                result_formats,
            ),
            FrontendMessage::Describe { kind, name } => self.describe(kind, &name),
            FrontendMessage::Execute { portal, max_rows } => self.execute_portal(&portal, max_rows),
            FrontendMessage::Close { kind, name } => self.close(kind, &name),
            FrontendMessage::Sync => {
                write_ready(&mut self.stream, transaction_status(&self.session))
            }
            FrontendMessage::Flush => self.flush(),
            FrontendMessage::Terminate => Ok(()),
            FrontendMessage::Password(_) => Err(protocol(
                "PasswordMessage is only valid during authentication",
            )),
            FrontendMessage::CopyData(_)
            | FrontendMessage::CopyDone
            | FrontendMessage::CopyFail(_) => {
                Err(protocol("COPY message is not valid outside COPY IN"))
            }
        }
    }

    fn simple_query(&mut self, sql: &str) -> Result<()> {
        self.prepared.remove("");
        if let Some(portal) = self.portals.remove("") {
            retire_portal(&self.registry, portal, QueryOutcome::Cancelled)?;
        }
        let statements = split_statements(sql)?;
        if statements.is_empty() {
            write_empty_query(&mut self.stream)?;
            write_ready(&mut self.stream, transaction_status(&self.session))?;
            return self.flush();
        }
        for statement in statements {
            if let Some(copy) = parse_copy(&statement)? {
                self.execute_copy(copy)?;
            } else {
                self.execute_simple_statement(&statement)?;
            }
        }
        write_ready(&mut self.stream, transaction_status(&self.session))?;
        self.flush()
    }

    fn execute_simple_statement(&mut self, sql: &str) -> Result<()> {
        self.authorize(sql)?;
        self.registry.reset_cancellation(self.handle.process_id())?;
        let query_id = Uuid::new_v4().to_string();
        self.registry.begin_query(
            self.handle.process_id(),
            query_id.clone(),
            redacted_security_sql(sql),
        )?;
        let result = (|| {
            let protocol_reset = protocol_session_reset(sql)?;
            let stream = self.statement_stream(sql, &[])?;
            let mut schema = Schema::empty();
            for event in stream {
                self.check_cancelled()?;
                match event? {
                    QueryEvent::Schema(value) => {
                        schema = value;
                        if !schema.fields.is_empty() {
                            write_row_description(&mut self.stream, &schema, &[])?;
                        }
                    }
                    QueryEvent::Batch(batch) => {
                        for row in &batch.rows {
                            self.check_cancelled()?;
                            write_data_row(&mut self.stream, &schema, row, &[])?;
                        }
                    }
                    QueryEvent::Progress(progress) => {
                        self.registry
                            .update_query_rows(&query_id, progress.rows_processed)?;
                    }
                    QueryEvent::Notice(notice) => {
                        write_notice(&mut self.stream, &notice)?;
                    }
                    QueryEvent::Complete(complete) => {
                        if let Some(reset) = protocol_reset {
                            self.apply_protocol_session_reset(reset)?;
                        }
                        write_command_complete(&mut self.stream, &command_tag(&complete))?;
                    }
                }
            }
            Ok(())
        })();
        self.finish_registered_query(&query_id, &result)?;
        result
    }

    fn parse(&mut self, name: String, sql: String, parameter_oids: Vec<u32>) -> Result<()> {
        ensure_prepared_statement_slot(&self.prepared, &name, self.config.max_prepared_statements)?;
        if sql.len() > self.config.max_frame_bytes {
            return Err(protocol("prepared SQL exceeds frame limit"));
        }
        let description = self.statement_description(&sql)?;
        let parameter_oids = resolve_parameter_oids(&parameter_oids, &description.parameter_types)?;
        if name.is_empty() {
            self.retire_portals_for_statement("")?;
        }
        self.prepared.insert(
            name,
            PreparedStatement {
                sql,
                parameter_oids,
                parameter_types: description.parameter_types,
                schema: description.schema,
            },
        );
        write_parse_complete(&mut self.stream)
    }

    fn bind(
        &mut self,
        portal_name: String,
        statement_name: &str,
        parameter_formats: &[i16],
        parameters: &[Option<Vec<u8>>],
        result_formats: Vec<i16>,
    ) -> Result<()> {
        ensure_portal_slot(&self.portals, &portal_name, self.config.max_portals)?;
        let prepared = self
            .prepared
            .get(statement_name)
            .ok_or_else(|| DbError::new("26000", "prepared statement does not exist"))?
            .clone();
        let parameters = decode_parameters_as(
            &prepared.parameter_oids,
            &prepared.parameter_types,
            parameter_formats,
            parameters,
        )?;
        let replaced = self.portals.insert(
            portal_name,
            Portal {
                statement_name: statement_name.to_owned(),
                sql: prepared.sql,
                parameters,
                result_formats,
                stream: None,
                schema: Some(prepared.schema),
                pending_rows: VecDeque::new(),
                completed: false,
                query_id: None,
                rows_processed: 0,
            },
        );
        if let Some(portal) = replaced {
            retire_portal(&self.registry, portal, QueryOutcome::Cancelled)?;
        }
        write_bind_complete(&mut self.stream)
    }

    fn describe(&mut self, kind: u8, name: &str) -> Result<()> {
        match kind {
            b'S' => {
                let statement = self
                    .prepared
                    .get(name)
                    .ok_or_else(|| DbError::new("26000", "prepared statement does not exist"))?;
                write_parameter_description(&mut self.stream, &statement.parameter_oids)?;
                if statement.schema.fields.is_empty() {
                    write_no_data(&mut self.stream)
                } else {
                    write_row_description(&mut self.stream, &statement.schema, &[])
                }
            }
            b'P' => {
                let portal = self
                    .portals
                    .get(name)
                    .ok_or_else(|| DbError::new("34000", "portal does not exist"))?;
                match &portal.schema {
                    Some(schema) if !schema.fields.is_empty() => {
                        write_row_description(&mut self.stream, schema, &portal.result_formats)
                    }
                    _ => write_no_data(&mut self.stream),
                }
            }
            _ => Err(protocol("Describe kind must be S or P")),
        }
    }

    fn execute_portal(&mut self, name: &str, max_rows: u32) -> Result<()> {
        let mut portal = self
            .portals
            .remove(name)
            .ok_or_else(|| DbError::new("34000", "portal does not exist"))?;
        let protocol_reset = protocol_session_reset(&portal.sql)?;
        let result = self.execute_portal_inner(&mut portal, max_rows, protocol_reset);
        if !(portal.completed && protocol_reset.is_some()) {
            self.portals.insert(name.to_owned(), portal);
        }
        result
    }

    fn execute_portal_inner(
        &mut self,
        portal: &mut Portal,
        max_rows: u32,
        protocol_reset: Option<ProtocolSessionReset>,
    ) -> Result<()> {
        if portal.completed {
            return write_command_complete(&mut self.stream, "SELECT 0");
        }
        if portal.stream.is_none() {
            self.authorize(&portal.sql)?;
            self.registry.reset_cancellation(self.handle.process_id())?;
            let query_id = Uuid::new_v4().to_string();
            self.registry.begin_query(
                self.handle.process_id(),
                query_id.clone(),
                redacted_security_sql(&portal.sql),
            )?;
            portal.query_id = Some(query_id);
            portal.stream = Some(self.statement_stream(&portal.sql, &portal.parameters)?);
        }

        let unlimited = max_rows == 0;
        let mut emitted = 0_u32;
        loop {
            while let Some(row) = portal.pending_rows.pop_front() {
                self.check_cancelled()?;
                let schema = portal
                    .schema
                    .as_ref()
                    .ok_or_else(|| DbError::new("XX000", "portal row has no schema"))?;
                write_data_row(&mut self.stream, schema, &row, &portal.result_formats)?;
                emitted = emitted.saturating_add(1);
                if !unlimited && emitted >= max_rows {
                    write_portal_suspended(&mut self.stream)?;
                    return Ok(());
                }
            }

            let next = portal.stream.as_mut().and_then(|stream| stream.next());
            let Some(event) = next else {
                if let Some(query_id) = &portal.query_id {
                    self.registry.finish_query(query_id, QueryOutcome::Error)?;
                }
                return Err(DbError::new(
                    "XX000",
                    "portal stream ended without a completion event",
                )
                .with_hint("close the portal and retry the statement"));
            };
            match event {
                Ok(QueryEvent::Schema(schema)) => {
                    if let Some(described) = &portal.schema
                        && described != &schema
                    {
                        return Err(DbError::new(
                            "XX000",
                            "portal execution schema changed after Describe",
                        ));
                    }
                    portal.schema = Some(schema);
                }
                Ok(QueryEvent::Batch(batch)) => {
                    portal.pending_rows.extend(batch.rows);
                }
                Ok(QueryEvent::Progress(progress)) => {
                    portal.rows_processed = progress.rows_processed;
                    if let Some(query_id) = &portal.query_id {
                        self.registry
                            .update_query_rows(query_id, progress.rows_processed)?;
                    }
                }
                Ok(QueryEvent::Notice(notice)) => {
                    write_notice(&mut self.stream, &notice)?;
                }
                Ok(QueryEvent::Complete(complete)) => {
                    if let Some(query_id) = &portal.query_id {
                        self.registry
                            .finish_query(query_id, QueryOutcome::Complete)?;
                    }
                    portal.completed = true;
                    portal.stream = None;
                    if let Some(reset) = protocol_reset {
                        self.apply_protocol_session_reset(reset)?;
                    }
                    write_command_complete(&mut self.stream, &command_tag(&complete))?;
                    return Ok(());
                }
                Err(error) => {
                    if let Some(query_id) = &portal.query_id {
                        let outcome = if error.sql_state == "57014" {
                            QueryOutcome::Cancelled
                        } else {
                            QueryOutcome::Error
                        };
                        self.registry.finish_query(query_id, outcome)?;
                    }
                    return Err(error);
                }
            }
        }
    }

    fn close(&mut self, kind: u8, name: &str) -> Result<()> {
        match kind {
            b'S' => {
                self.prepared
                    .remove(name)
                    .ok_or_else(|| DbError::new("26000", "prepared statement does not exist"))?;
                self.retire_portals_for_statement(name)?;
            }
            b'P' => {
                let portal = self
                    .portals
                    .remove(name)
                    .ok_or_else(|| DbError::new("34000", "portal does not exist"))?;
                retire_portal(&self.registry, portal, QueryOutcome::Cancelled)?;
            }
            _ => return Err(protocol("Close kind must be S or P")),
        }
        write_close_complete(&mut self.stream)
    }

    fn retire_portals_for_statement(&mut self, statement_name: &str) -> Result<()> {
        let names = self
            .portals
            .iter()
            .filter_map(|(name, portal)| {
                (portal.statement_name == statement_name).then_some(name.clone())
            })
            .collect::<Vec<_>>();
        for name in names {
            if let Some(portal) = self.portals.remove(&name) {
                retire_portal(&self.registry, portal, QueryOutcome::Cancelled)?;
            }
        }
        Ok(())
    }

    fn apply_protocol_session_reset(&mut self, reset: ProtocolSessionReset) -> Result<()> {
        self.prepared.clear();
        let portals = std::mem::take(&mut self.portals);
        for (_, portal) in portals {
            retire_portal(&self.registry, portal, QueryOutcome::Cancelled)?;
        }
        if reset == ProtocolSessionReset::DiscardAll {
            self.settings.reset_all();
            self.refresh_runtime_metadata()?;
        }
        Ok(())
    }

    fn statement_stream(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>>>> {
        refresh_system_catalog_metadata(
            &mut self.session,
            &self.auth,
            &self.settings,
            &self.principal,
            &self.database,
        )?;
        if let Some(statement) = parse_security_statement(sql)? {
            if !parameters.is_empty() {
                return Err(DbError::new(
                    "0A000",
                    "security DDL does not support protocol parameters",
                ));
            }
            if !matches!(self.session.transaction_status(), TransactionStatus::Idle) {
                return Err(DbError::new(
                    "25001",
                    "security DDL must execute outside a transaction",
                ));
            }
            let tag = execute_security_statement(&self.auth, &mut self.principal, statement)?;
            let events = vec![
                QueryEvent::Schema(Schema::empty()),
                QueryEvent::Progress(QueryProgress { rows_processed: 0 }),
                QueryEvent::Complete(CommandComplete {
                    tag: tag.to_owned(),
                    rows_affected: 0,
                }),
            ];
            return Ok(Box::new(events.into_iter().map(Ok)));
        }
        if !matches!(self.session.transaction_status(), TransactionStatus::Failed)
            && let Some(events) = session_setting_events(sql, &mut self.settings)?
        {
            self.refresh_runtime_metadata()?;
            refresh_system_catalog_metadata(
                &mut self.session,
                &self.auth,
                &self.settings,
                &self.principal,
                &self.database,
            )?;
            return Ok(Box::new(events.into_iter().map(Ok)));
        }
        Ok(Box::new(self.session.execute_stream_with_cancellation(
            sql,
            parameters,
            self.handle.cancellation_flag(),
        )?))
    }

    fn statement_description(&mut self, sql: &str) -> Result<StatementDescription> {
        if matches!(self.session.transaction_status(), TransactionStatus::Failed) {
            return self.session.describe_statement(sql);
        }
        if parse_security_statement(sql)?.is_some() {
            return Ok(StatementDescription {
                schema: Schema::empty(),
                parameter_types: Vec::new(),
            });
        }
        if let Some(description) = session_setting_description(sql, &self.settings)? {
            return Ok(description);
        }
        self.session.describe_statement(sql)
    }

    fn refresh_runtime_metadata(&mut self) -> Result<()> {
        self.session.set_runtime_metadata(session_runtime_metadata(
            &self.settings,
            &self.database,
            &self.principal,
        )?);
        Ok(())
    }

    fn authorize(&self, sql: &str) -> Result<()> {
        let authorizer = Authorizer::from_store(&self.auth)?;
        if is_security_sql(sql) {
            authorizer.authorize(&self.principal, Action::Manage, &DbObject::Server)
        } else {
            authorizer.authorize_sql(&self.principal, &self.database, sql)
        }
    }

    fn check_cancelled(&self) -> Result<()> {
        if self
            .shutdown
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(DbError::new("57P01", "server is shutting down"));
        }
        if self.handle.is_cancelled() {
            return Err(DbError::new("57014", "query cancelled"));
        }
        Ok(())
    }

    fn finish_registered_query(&self, query_id: &str, result: &Result<()>) -> Result<()> {
        let outcome = match result {
            Ok(()) => QueryOutcome::Complete,
            Err(error) if error.sql_state == "57014" => QueryOutcome::Cancelled,
            Err(_) => QueryOutcome::Error,
        };
        self.registry.finish_query(query_id, outcome)
    }

    fn flush(&mut self) -> Result<()> {
        self.stream
            .flush()
            .map_err(|error| io_error("failed to flush PostgreSQL connection", error))
    }

    fn execute_copy(&mut self, copy: CopyCommand) -> Result<()> {
        match copy.direction {
            CopyDirection::ToStdout => self.copy_to_stdout(&copy),
            CopyDirection::FromStdin => self.copy_from_stdin(&copy),
        }
    }

    fn copy_to_stdout(&mut self, copy: &CopyCommand) -> Result<()> {
        let projection = if copy.columns.is_empty() {
            "*".to_owned()
        } else {
            copy.columns.join(", ")
        };
        let sql = format!("SELECT {projection} FROM {}", copy.table);
        self.authorize(&sql)?;
        let mut stream = self.session.execute_stream(&sql, &[])?;
        let schema = match stream.next() {
            Some(Ok(QueryEvent::Schema(schema))) if !schema.fields.is_empty() => schema,
            Some(Ok(_)) => {
                return Err(DbError::new(
                    "XX000",
                    "COPY source did not begin with a non-empty schema",
                ));
            }
            Some(Err(error)) => return Err(error),
            None => {
                return Err(DbError::new("XX000", "COPY source ended before its schema"));
            }
        };
        write_copy_response(&mut self.stream, b'H', schema.fields.len())?;
        if copy.options.header {
            let header = encode_copy_header(&schema, &copy.options)?;
            write_message(&mut self.stream, b'd', &header)?;
        }
        let handle = &self.handle;
        let shutdown = self.shutdown.as_ref();
        let rows = write_copy_stream(&mut self.stream, &schema, &copy.options, stream, || {
            if shutdown.is_some_and(CancellationToken::is_cancelled) {
                return Err(DbError::new("57P01", "server is shutting down"));
            }
            if handle.is_cancelled() {
                return Err(DbError::new("57014", "query cancelled"));
            }
            Ok(())
        })?;
        write_message(&mut self.stream, b'c', &[])?;
        write_command_complete(&mut self.stream, &format!("COPY {rows}"))
    }

    fn copy_from_stdin(&mut self, copy: &CopyCommand) -> Result<()> {
        self.authorize(&format!("COPY {} FROM STDIN", copy.table))?;
        let columns = copy_columns(&self.engine, &copy.table, &copy.columns)?;
        let owns_transaction = begin_copy_transaction(&mut self.session)?;
        write_copy_response(&mut self.stream, b'G', columns.len())?;
        self.flush()?;
        let mut bytes = Vec::new();
        let receive = loop {
            let Some(message) = self.next_frontend_message()? else {
                break Err(DbError::new("08006", "connection closed during COPY IN"));
            };
            match message {
                FrontendMessage::CopyData(chunk) => {
                    let next = bytes
                        .len()
                        .checked_add(chunk.len())
                        .ok_or_else(|| DbError::new("54000", "COPY input length overflowed"))?;
                    if next > self.config.max_copy_bytes {
                        break Err(DbError::new(
                            "54000",
                            "COPY input exceeds the configured limit",
                        ));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                FrontendMessage::CopyDone => break Ok(()),
                FrontendMessage::CopyFail(message) => {
                    break Err(DbError::new("57014", "COPY aborted by client").with_detail(message));
                }
                FrontendMessage::Flush => self.flush()?,
                _ => break Err(protocol("only COPY data/done/fail is valid during COPY IN")),
            }
        };
        if let Err(error) = receive {
            abort_copy_transaction(&mut self.session, owns_transaction);
            return Err(error);
        }
        let rows = match import_copy(
            &mut self.session,
            &copy.table,
            &columns,
            &copy.options,
            &bytes,
        ) {
            Ok(rows) => rows,
            Err(error) => {
                abort_copy_transaction(&mut self.session, owns_transaction);
                return Err(error);
            }
        };
        complete_copy_transaction(&mut self.session, owns_transaction)?;
        write_command_complete(&mut self.stream, &format!("COPY {rows}"))
    }
}

fn write_copy_stream<W, I, F>(
    writer: &mut W,
    schema: &Schema,
    options: &CopyOptions,
    stream: I,
    mut check_cancelled: F,
) -> Result<u64>
where
    W: Write,
    I: IntoIterator<Item = Result<QueryEvent>>,
    F: FnMut() -> Result<()>,
{
    let mut rows = 0_u64;
    let mut completed = false;
    for event in stream {
        check_cancelled()?;
        if completed {
            return Err(DbError::new(
                "XX000",
                "COPY source emitted an event after completion",
            ));
        }
        match event? {
            QueryEvent::Schema(_) => {
                return Err(DbError::new(
                    "XX000",
                    "COPY source emitted more than one schema",
                ));
            }
            QueryEvent::Batch(batch) => {
                for row in &batch.rows {
                    let encoded = encode_copy_row(schema, row, options)?;
                    write_message(writer, b'd', &encoded)?;
                    rows = rows.saturating_add(1);
                }
            }
            QueryEvent::Notice(notice) => write_notice(writer, &notice)?,
            QueryEvent::Progress(_) => {}
            QueryEvent::Complete(_) => completed = true,
        }
    }
    if !completed {
        return Err(DbError::new(
            "XX000",
            "COPY source ended without a completion event",
        ));
    }
    Ok(rows)
}

fn begin_copy_transaction(session: &mut Session) -> Result<bool> {
    match session.transaction_status() {
        TransactionStatus::Idle => {
            drain(session.execute_stream("BEGIN", &[])?)?;
            Ok(true)
        }
        TransactionStatus::Active => Ok(false),
        TransactionStatus::Failed => Err(DbError::new(
            "25P02",
            "the current transaction is aborted; commands are ignored until ROLLBACK",
        )),
    }
}

fn complete_copy_transaction(session: &mut Session, owns_transaction: bool) -> Result<()> {
    if owns_transaction {
        drain(session.execute_stream("COMMIT", &[])?)?;
    }
    Ok(())
}

fn abort_copy_transaction(session: &mut Session, owns_transaction: bool) {
    if owns_transaction {
        if let Ok(stream) = session.execute_stream("ROLLBACK", &[]) {
            let _ = drain(stream);
        }
    } else {
        session.mark_transaction_failed();
    }
}

fn drain(stream: impl Iterator<Item = Result<QueryEvent>>) -> Result<()> {
    for event in stream {
        event?;
    }
    Ok(())
}

fn transaction_status(session: &Session) -> u8 {
    match session.transaction_status() {
        TransactionStatus::Idle => b'I',
        TransactionStatus::Active => b'T',
        TransactionStatus::Failed => b'E',
    }
}
