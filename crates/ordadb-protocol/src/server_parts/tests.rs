
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn protocol_session_reset_classifier_is_exact() {
        assert_eq!(
            protocol_session_reset("DISCARD ALL").expect("discard"),
            Some(ProtocolSessionReset::DiscardAll)
        );
        assert_eq!(
            protocol_session_reset("deallocate all").expect("deallocate"),
            Some(ProtocolSessionReset::DeallocateAll)
        );
        assert_eq!(protocol_session_reset("SELECT 1").expect("select"), None);
        assert_eq!(
            protocol_session_reset("DEALLOCATE named")
                .expect_err("named deallocate remains unsupported")
                .sql_state,
            "0A000"
        );
    }

    #[test]
    fn prepared_parameter_oids_fill_unknowns_and_reject_conflicts() {
        assert_eq!(
            resolve_parameter_oids(&[], &[ScalarType::Int64, ScalarType::Text]).expect("infer all"),
            [crate::value::OID_INT8, crate::value::OID_TEXT]
        );
        assert_eq!(
            resolve_parameter_oids(
                &[0, crate::value::OID_TEXT],
                &[ScalarType::Int64, ScalarType::Text],
            )
            .expect("fill unknown"),
            [crate::value::OID_INT8, crate::value::OID_TEXT]
        );
        assert_eq!(
            resolve_parameter_oids(&[crate::value::OID_INT4], &[ScalarType::Int64])
                .expect("safe widening"),
            [crate::value::OID_INT4]
        );

        let enum_type = ScalarType::Enum {
            type_id: ordadb_types::TypeId::new(11),
            labels: vec!["draft".into(), "published".into()],
        };
        let enum_oid = type_oid(&enum_type);
        let described = resolve_parameter_oids(&[], std::slice::from_ref(&enum_type))
            .expect("describe enum parameter");
        assert_eq!(described, [enum_oid]);
        assert_eq!(
            decode_parameters_as(
                &described,
                std::slice::from_ref(&enum_type),
                &[1],
                &[Some(b"published".to_vec())],
            )
            .expect("execute described enum parameter"),
            [Value::Text("published".into())]
        );

        let mismatch = resolve_parameter_oids(&[crate::value::OID_TEXT], &[ScalarType::Int64])
            .expect_err("mismatched declaration");
        assert_eq!(mismatch.sql_state, "42804");

        let count = resolve_parameter_oids(
            &[crate::value::OID_INT8],
            &[ScalarType::Int64, ScalarType::Text],
        )
        .expect_err("mismatched count");
        assert_eq!(count.sql_state, "08P01");
    }

    #[test]
    fn extended_query_state_waits_for_sync_and_ignores_other_messages() {
        assert_eq!(
            failed_message_action(&FrontendMessage::Sync),
            FailedMessageAction::Synchronize
        );
        assert_eq!(
            failed_message_action(&FrontendMessage::Flush),
            FailedMessageAction::Flush
        );
        assert_eq!(
            failed_message_action(&FrontendMessage::Terminate),
            FailedMessageAction::Terminate
        );
        assert_eq!(
            failed_message_action(&FrontendMessage::Parse {
                name: "ignored".into(),
                sql: "SELECT 1".into(),
                parameter_oids: Vec::new(),
            }),
            FailedMessageAction::Ignore
        );
        assert_eq!(
            failed_message_action(&FrontendMessage::Query("SELECT 1".into())),
            FailedMessageAction::Ignore
        );
    }

    #[test]
    fn named_extended_objects_require_close_while_unnamed_objects_replace() {
        let statement = PreparedStatement {
            sql: "SELECT 1".into(),
            parameter_oids: Vec::new(),
            parameter_types: Vec::new(),
            schema: Schema::empty(),
        };
        let mut prepared = BTreeMap::new();
        prepared.insert(String::new(), statement.clone());
        ensure_prepared_statement_slot(&prepared, "", 1).expect("replace unnamed statement");
        assert_eq!(
            ensure_prepared_statement_slot(&prepared, "named", 1)
                .expect_err("statement limit")
                .sql_state,
            "54000"
        );
        prepared.insert("named".into(), statement);
        assert_eq!(
            ensure_prepared_statement_slot(&prepared, "named", 3)
                .expect_err("named statement requires close")
                .sql_state,
            "42P05"
        );

        let portal = || Portal {
            statement_name: String::new(),
            sql: "SELECT 1".into(),
            parameters: Vec::new(),
            result_formats: Vec::new(),
            stream: None,
            schema: Some(Schema::empty()),
            pending_rows: VecDeque::new(),
            completed: false,
            query_id: None,
            rows_processed: 0,
        };
        let mut portals = BTreeMap::new();
        portals.insert(String::new(), portal());
        ensure_portal_slot(&portals, "", 1).expect("replace unnamed portal");
        assert_eq!(
            ensure_portal_slot(&portals, "named", 1)
                .expect_err("portal limit")
                .sql_state,
            "54000"
        );
        portals.insert("named".into(), portal());
        assert_eq!(
            ensure_portal_slot(&portals, "named", 3)
                .expect_err("named portal requires close")
                .sql_state,
            "42P03"
        );
    }

    #[test]
    fn retiring_an_active_portal_finishes_its_registered_query() {
        let registry = SessionRegistry::default();
        let handle = registry
            .register_session("user".into(), "db".into(), None, "local".into(), 17)
            .expect("register session");
        registry
            .begin_query(
                handle.process_id(),
                "portal-query".into(),
                "SELECT 1".into(),
            )
            .expect("begin query");
        retire_portal(
            &registry,
            Portal {
                statement_name: String::new(),
                sql: "SELECT 1".into(),
                parameters: Vec::new(),
                result_formats: Vec::new(),
                stream: None,
                schema: Some(Schema::empty()),
                pending_rows: VecDeque::new(),
                completed: false,
                query_id: Some("portal-query".into()),
                rows_processed: 0,
            },
            QueryOutcome::Cancelled,
        )
        .expect("retire portal");
        assert_eq!(registry.active_query_count().expect("active count"), 0);
        assert!(
            registry
                .queries()
                .expect("query history")
                .iter()
                .any(|query| query.query_id == "portal-query"
                    && matches!(query.outcome, QueryOutcome::Cancelled))
        );
    }

    #[test]
    fn simple_query_splitter_respects_quotes_and_copy_is_explicit() {
        assert_eq!(
            split_statements("SELECT ';'; SELECT 1").expect("split"),
            vec!["SELECT ';'", "SELECT 1"]
        );
        assert_eq!(
            split_statements("SELECT 'it''s;still one'; SELECT \"a\"\";b\" FROM items")
                .expect("doubled quotes"),
            vec!["SELECT 'it''s;still one'", "SELECT \"a\"\";b\" FROM items"]
        );
        assert_eq!(
            split_statements(r"SELECT E'escaped\\'; SELECT 2").expect("even backslashes"),
            vec![r"SELECT E'escaped\\'", "SELECT 2"]
        );
        assert_eq!(
            split_statements(
                "CREATE PROCEDURE p() AS $body$
                 BEGIN
                 PERFORM ';';
                 END;
                 $body$ LANGUAGE plpgsql;
                 SELECT $1"
            )
            .expect("dollar quote"),
            vec![
                "CREATE PROCEDURE p() AS $body$
                 BEGIN
                 PERFORM ';';
                 END;
                 $body$ LANGUAGE plpgsql",
                "SELECT $1",
            ]
        );
        assert_eq!(
            split_statements("SELECT $$semi;colon$$; SELECT 2").expect("empty dollar tag"),
            vec!["SELECT $$semi;colon$$", "SELECT 2"]
        );
        assert_eq!(
            split_statements("SELECT $body$missing")
                .expect_err("unterminated dollar quote")
                .sql_state,
            "42601"
        );
        let copy = parse_copy("COPY public.items TO STDOUT")
            .expect("copy")
            .expect("command");
        assert!(matches!(copy.direction, CopyDirection::ToStdout));
        assert_eq!(copy.options, default_copy_options(CopyFormat::Text));
    }

    #[test]
    fn copy_grammar_supports_columns_and_typed_csv_options() {
        let copy = parse_copy(
            "COPY public.items (id, title) FROM STDIN \
             WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL 'NULL', QUOTE '\"')",
        )
        .expect("parse COPY")
        .expect("COPY command");
        assert_eq!(copy.table, "public.items");
        assert_eq!(copy.columns, ["id", "title"]);
        assert_eq!(copy.direction, CopyDirection::FromStdin);
        assert_eq!(copy.options.format, CopyFormat::Csv);
        assert_eq!(copy.options.delimiter, b';');
        assert_eq!(copy.options.null, "NULL");
        assert!(copy.options.header);
        assert_eq!(copy.options.quote, b'"');

        for sql in [
            "COPY items TO 'file.csv'",
            "COPY items FROM PROGRAM 'generate'",
            "COPY items TO STDOUT WITH (FORMAT binary)",
            "COPY BINARY items TO STDOUT",
        ] {
            assert_eq!(
                parse_copy(sql)
                    .expect_err("unsupported COPY form")
                    .sql_state,
                "0A000",
                "{sql}"
            );
        }
        assert_eq!(
            parse_copy("COPY items (id, ID) FROM STDIN")
                .expect_err("duplicate COPY column")
                .sql_state,
            "42701"
        );
        assert_eq!(
            parse_copy("COPY items TO STDOUT WITH (HEADER)")
                .expect_err("header requires CSV")
                .sql_state,
            "22023"
        );
    }

    #[test]
    fn copy_text_codec_escapes_delimiters_nulls_and_newlines() {
        let options = default_copy_options(CopyFormat::Text);
        let schema = Schema::new(vec![
            Field::new("first", ScalarType::Text, false),
            Field::new("second", ScalarType::Text, true),
        ]);
        let original = b"tab\tbackslash\\newline\n".to_vec();
        let encoded = encode_copy_row(
            &schema,
            &Row::new(vec![
                Value::Text(String::from_utf8(original.clone()).unwrap()),
                Value::Null,
            ]),
            &options,
        )
        .expect("encode COPY text");
        assert!(encoded.ends_with(b"\n"));
        let decoded =
            decode_text_record(&encoded[..encoded.len() - 1], &options).expect("decode COPY text");
        assert_eq!(decoded, vec![Some(original), None]);
    }

    #[test]
    fn copy_out_requires_exactly_one_terminal_completion_event() {
        let schema = Schema::new(vec![Field::new("value", ScalarType::Text, false)]);
        let options = default_copy_options(CopyFormat::Text);
        let batch = QueryEvent::Batch(Batch {
            schema: schema.clone(),
            rows: vec![Row::new(vec![Value::Text("row".into())])],
        });
        let complete = QueryEvent::Complete(CommandComplete {
            tag: "SELECT".into(),
            rows_affected: 1,
        });

        let mut encoded = Vec::new();
        assert_eq!(
            write_copy_stream(
                &mut encoded,
                &schema,
                &options,
                [Ok(batch.clone()), Ok(complete.clone())],
                || Ok(()),
            )
            .expect("complete COPY stream"),
            1
        );
        assert!(!encoded.is_empty());

        let missing = write_copy_stream(
            &mut Vec::new(),
            &schema,
            &options,
            [Ok(batch.clone())],
            || Ok(()),
        )
        .expect_err("missing completion");
        assert_eq!(missing.sql_state, "XX000");

        let duplicate = write_copy_stream(
            &mut Vec::new(),
            &schema,
            &options,
            [Ok(batch), Ok(complete.clone()), Ok(complete)],
            || Ok(()),
        )
        .expect_err("duplicate completion");
        assert_eq!(duplicate.sql_state, "XX000");
    }

    #[test]
    fn copy_in_uses_and_preserves_an_existing_transaction_boundary() {
        let directory = tempfile::tempdir().expect("tempdir");
        let engine =
            Engine::open(ordadb_engine::EngineConfig::new(directory.path())).expect("open engine");
        let mut session = engine.connect().expect("connect");
        drain(
            session
                .execute_stream("CREATE TABLE copy_tx (id BIGINT, label TEXT)", &[])
                .expect("create table"),
        )
        .expect("drain create table");
        let columns = copy_columns(&engine, "copy_tx", &[]).expect("COPY columns");
        let options = default_copy_options(CopyFormat::Text);

        drain(session.execute_stream("BEGIN", &[]).expect("begin")).expect("drain begin");
        let owns_transaction = begin_copy_transaction(&mut session).expect("reuse transaction");
        assert!(!owns_transaction);
        assert_eq!(
            import_copy(&mut session, "copy_tx", &columns, &options, b"1\touter\n",)
                .expect("import COPY row"),
            1
        );
        complete_copy_transaction(&mut session, owns_transaction).expect("finish COPY");
        assert_eq!(session.transaction_status(), TransactionStatus::Active);
        drain(session.execute_stream("ROLLBACK", &[]).expect("rollback")).expect("drain rollback");
        let rows = session
            .execute_stream("SELECT id FROM copy_tx", &[])
            .expect("select after rollback")
            .collect::<Result<Vec<_>>>()
            .expect("drain select");
        assert_eq!(
            rows.iter()
                .filter_map(|event| match event {
                    QueryEvent::Batch(batch) => Some(batch.rows.len()),
                    _ => None,
                })
                .sum::<usize>(),
            0
        );

        drain(session.execute_stream("BEGIN", &[]).expect("second begin"))
            .expect("drain second begin");
        let owns_transaction = begin_copy_transaction(&mut session).expect("reuse transaction");
        let error = import_copy(
            &mut session,
            "copy_tx",
            &columns,
            &options,
            b"2\tvalid\n3\ttoo\tmany\n",
        )
        .expect_err("malformed COPY input");
        assert_eq!(error.sql_state, "22P04");
        abort_copy_transaction(&mut session, owns_transaction);
        assert_eq!(session.transaction_status(), TransactionStatus::Failed);
        drain(
            session
                .execute_stream("ROLLBACK", &[])
                .expect("failed rollback"),
        )
        .expect("drain failed rollback");
    }

    #[test]
    fn copy_csv_codec_distinguishes_null_from_quoted_empty_text() {
        let options = default_copy_options(CopyFormat::Csv);
        let schema = Schema::new(vec![
            Field::new("nullable", ScalarType::Text, true),
            Field::new("empty", ScalarType::Text, false),
        ]);
        let encoded = encode_copy_row(
            &schema,
            &Row::new(vec![Value::Null, Value::Text(String::new())]),
            &options,
        )
        .expect("encode COPY CSV");
        assert_eq!(encoded, b",\"\"\n");
        let records = decode_csv_records(&encoded, &options).expect("decode COPY CSV");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0][0].value, b"");
        assert!(!records[0][0].quoted);
        assert_eq!(records[0][1].value, b"");
        assert!(records[0][1].quoted);

        let multiline = decode_csv_records(b"\"line 1\nline 2\",value\r\n", &options)
            .expect("decode multiline COPY CSV");
        assert_eq!(multiline[0][0].value, b"line 1\nline 2");
        assert!(multiline[0][0].quoted);
    }

    #[test]
    fn copy_import_honors_columns_csv_header_and_transaction_rollback() {
        let directory = tempfile::tempdir().expect("tempdir");
        let engine =
            Engine::open(ordadb_engine::EngineConfig::new(directory.path())).expect("open engine");
        let mut session = engine.connect().expect("connect session");
        drain(
            session
                .execute_stream(
                    "CREATE TABLE items (\
                     id BIGINT PRIMARY KEY, title TEXT NOT NULL, score INTEGER DEFAULT 7)",
                    &[],
                )
                .expect("create table"),
        )
        .expect("drain create table");
        let requested = vec!["id".to_owned(), "title".to_owned()];
        let columns = copy_columns(&engine, "items", &requested).expect("COPY columns");
        let mut csv = default_copy_options(CopyFormat::Csv);
        csv.header = true;
        csv.delimiter = b';';

        drain(session.execute_stream("BEGIN", &[]).expect("begin import")).expect("drain begin");
        assert_eq!(
            import_copy(
                &mut session,
                "items",
                &columns,
                &csv,
                b"id;title\n1;first\n2;second\n",
            )
            .expect("import CSV"),
            2
        );
        drain(
            session
                .execute_stream("COMMIT", &[])
                .expect("commit import"),
        )
        .expect("drain commit");

        drain(session.execute_stream("BEGIN", &[]).expect("begin failure"))
            .expect("drain begin failure");
        let error = import_copy(
            &mut session,
            "items",
            &columns,
            &default_copy_options(CopyFormat::Text),
            b"3\tthird\nnot-an-id\tbroken\n",
        )
        .expect_err("invalid COPY row");
        assert_eq!(error.sql_state, "22P02");
        drain(
            session
                .execute_stream("ROLLBACK", &[])
                .expect("rollback failed COPY"),
        )
        .expect("drain rollback");

        let rows = session
            .execute("SELECT id, title, score FROM items ORDER BY id", &[])
            .expect("query imported rows")
            .filter_map(|event| match event {
                QueryEvent::Batch(batch) => Some(batch.rows),
                _ => None,
            })
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                Row::new(vec![
                    Value::Int64(1),
                    Value::Text("first".into()),
                    Value::Int32(7),
                ]),
                Row::new(vec![
                    Value::Int64(2),
                    Value::Text("second".into()),
                    Value::Int32(7),
                ]),
            ]
        );
    }

    #[test]
    fn server_limits_reject_zero_or_tiny_values() {
        let mut config = PgServerConfig {
            max_frame_bytes: 4,
            ..PgServerConfig::default()
        };
        assert_eq!(config.validate().expect_err("frame").sql_state, "22023");
        config.max_frame_bytes = DEFAULT_MAX_FRAME_BYTES;
        config.max_portals = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn startup_encryption_negotiation_is_ordered_and_non_repeating() {
        let mut negotiation = StartupNegotiation::default();
        negotiation
            .record(EncryptionRequest::Gss)
            .expect("GSS preference probe");
        negotiation
            .record(EncryptionRequest::Ssl)
            .expect("TLS probe after GSS rejection");
        assert_eq!(
            negotiation
                .record(EncryptionRequest::Ssl)
                .expect_err("repeated SSLRequest")
                .sql_state,
            "08P01"
        );

        let mut out_of_order = StartupNegotiation::default();
        out_of_order
            .record(EncryptionRequest::Ssl)
            .expect("initial SSLRequest");
        assert_eq!(
            out_of_order
                .record(EncryptionRequest::Gss)
                .expect_err("GSS request after SSL")
                .sql_state,
            "08P01"
        );
    }

    #[test]
    fn session_compatibility_functions_remain_bounded_without_catalog_interception() {
        let directory = tempfile::tempdir().expect("tempdir");
        let engine =
            Engine::open(ordadb_engine::EngineConfig::new(directory.path())).expect("open engine");
        let mut session = engine.connect().expect("connect session");
        session.set_runtime_metadata(
            SessionRuntimeMetadata::postgres_compatible("18.0", "ordadb", "dba", "dba")
                .expect("runtime metadata"),
        );

        for (sql, field_name, expected) in [
            (
                "SELECT version()",
                "version",
                Value::Text("PostgreSQL 18.0 compatible OrdaDB on x86_64-pc-windows-msvc".into()),
            ),
            (
                "SELECT current_database()",
                "current_database",
                Value::Text("ordadb".into()),
            ),
            (
                "SELECT CURRENT_USER",
                "current_user",
                Value::Text("dba".into()),
            ),
            (
                "SELECT SESSION_USER",
                "session_user",
                Value::Text("dba".into()),
            ),
            (
                "SELECT current_setting('client_encoding')",
                "current_setting",
                Value::Text("UTF8".into()),
            ),
            ("SELECT 1", "?column?", Value::Int32(1)),
        ] {
            let events = session
                .execute(sql, &[])
                .unwrap_or_else(|error| panic!("{sql}: {error}"))
                .collect::<Vec<_>>();
            let QueryEvent::Schema(schema) = &events[0] else {
                panic!("{sql}: schema event");
            };
            assert_eq!(schema.fields[0].name, field_name);
            let value = events.iter().find_map(|event| match event {
                QueryEvent::Batch(batch) => batch.rows.first()?.values.first(),
                _ => None,
            });
            assert_eq!(value, Some(&expected), "{sql}");
            assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        }

        let settings_events = session
            .execute(
                "SELECT current_setting('client_encoding'), \
                 current_setting('standard_conforming_strings')",
                &[],
            )
            .expect("multi-setting query")
            .collect::<Vec<_>>();
        let values = settings_events.iter().find_map(|event| match event {
            QueryEvent::Batch(batch) => batch.rows.first().map(|row| row.values.clone()),
            _ => None,
        });
        assert_eq!(
            values,
            Some(vec![Value::Text("UTF8".into()), Value::Text("on".into())])
        );

        let catalog_events = session
            .execute("SELECT relname FROM pg_catalog.pg_class LIMIT 1", &[])
            .expect("system catalog query")
            .collect::<Vec<_>>();
        assert!(matches!(
            catalog_events.first(),
            Some(QueryEvent::Schema(_))
        ));
    }

    #[test]
    fn session_settings_describe_without_mutation_and_apply_on_execution() {
        let mut settings = PgSessionSettings::from_startup(
            "18.0 (OrdaDB test)".to_owned(),
            "dba",
            &BTreeMap::new(),
        )
        .expect("settings");
        let description =
            session_setting_description("SET application_name TO 'DataGrip'", &settings)
                .expect("describe")
                .expect("session statement");
        assert!(description.schema.fields.is_empty());
        assert_eq!(settings.get("application_name"), Some(""));

        let events = session_setting_events("SET application_name TO 'DataGrip'", &mut settings)
            .expect("execute")
            .expect("session statement");
        assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        assert_eq!(settings.get("application_name"), Some("DataGrip"));

        let events = session_setting_events("SHOW application_name", &mut settings)
            .expect("show")
            .expect("session statement");
        assert!(matches!(events.first(), Some(QueryEvent::Schema(_))));

        let description = session_setting_description(
            "SELECT set_config('application_name', 'DescribeOnly', false)",
            &settings,
        )
        .expect("describe set_config")
        .expect("set_config statement");
        assert_eq!(description.schema.fields[0].name, "set_config");
        assert_eq!(settings.get("application_name"), Some("DataGrip"));

        let events = session_setting_events(
            "SELECT set_config('application_name', 'pgjdbc', false)",
            &mut settings,
        )
        .expect("execute set_config")
        .expect("set_config statement");
        assert!(events.iter().any(|event| matches!(
            event,
            QueryEvent::Batch(batch)
                if batch.rows == [Row::new(vec![Value::Text("pgjdbc".into())])]
        )));
        let error = session_setting_events(
            "SELECT set_config('application_name', 'local', true)",
            &mut settings,
        )
        .expect_err("local set_config rejected");
        assert_eq!(error.sql_state, "0A000");
        assert_eq!(settings.get("application_name"), Some("pgjdbc"));

        let directory = tempfile::tempdir().expect("tempdir");
        let engine =
            Engine::open(ordadb_engine::EngineConfig::new(directory.path())).expect("open engine");
        let mut session = engine.connect().expect("connect session");
        let principal = Principal {
            user: "dba".into(),
            roles: BTreeSet::new(),
        };
        session.set_runtime_metadata(
            session_runtime_metadata(&settings, "ordadb", &principal)
                .expect("refreshed runtime metadata"),
        );
        let values = session
            .execute("SELECT current_setting('application_name')", &[])
            .expect("read changed setting")
            .find_map(|event| match event {
                QueryEvent::Batch(batch) => batch.rows.into_iter().next().map(|row| row.values),
                _ => None,
            });
        assert_eq!(values, Some(vec![Value::Text("pgjdbc".into())]));
    }

    #[test]
    fn pgwire_sessions_keep_the_default_postgresql_dialect() {
        let directory = tempfile::tempdir().expect("tempdir");
        let engine =
            Engine::open(ordadb_engine::EngineConfig::new(directory.path())).expect("open engine");
        let principal = Principal {
            user: "dba".into(),
            roles: BTreeSet::new(),
        };
        let mut session =
            connect_postgresql_session(&engine, &principal, false).expect("connect session");
        assert_eq!(
            session.options(),
            ordadb_engine::SessionOptions::default(),
            "PostgreSQL Wire must not negotiate a non-PostgreSQL dialect"
        );
        let events = session
            .execute("CREATE SCHEMA wire_owned", &[])
            .expect("create owned schema")
            .collect::<Vec<_>>();
        assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        let catalog = engine.catalog_snapshot().expect("catalog");
        let schema = catalog
            .schema(&Identifier::unquoted("wire_owned"))
            .expect("wire-owned schema");
        assert_eq!(
            catalog
                .owner_of(ordadb_catalog::CatalogObjectRef::Schema(schema.id))
                .map(ordadb_catalog::CatalogOwner::as_str),
            Some(principal.user.as_str())
        );
    }
}
