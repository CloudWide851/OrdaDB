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

fn catalog_setting(name: &str, setting: &str) -> CatalogSettingMetadata {
    CatalogSettingMetadata {
        name: name.to_owned(),
        setting: setting.to_owned(),
        unit: None,
        category: "OrdaDB test settings".to_owned(),
        short_description: format!("Test projection for {name}."),
        context: "user".to_owned(),
        value_type: "string".to_owned(),
        source: "session".to_owned(),
        minimum: None,
        maximum: None,
        enum_values: None,
        boot_value: setting.to_owned(),
        reset_value: setting.to_owned(),
    }
}

#[test]
fn system_catalog_queries_use_normal_relational_execution() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE catalog_widgets (id BIGINT PRIMARY KEY, label TEXT)",
        &[],
    );

    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT nspname FROM pg_catalog.pg_namespace \
             WHERE nspname <> $1 ORDER BY nspname LIMIT 2",
            &[Value::Text("information_schema".into())],
        )),
        vec![
            Row::new(vec![Value::Text("pg_catalog".into())]),
            Row::new(vec![Value::Text("public".into())]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT n.nspname, c.relname \
             FROM pg_catalog.pg_namespace AS n \
             JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid \
             WHERE c.relname = 'catalog_widgets'",
            &[],
        )),
        vec![Row::new(vec![
            Value::Text("public".into()),
            Value::Text("catalog_widgets".into()),
        ])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "WITH matching AS (\
                SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = $1\
             ) SELECT nspname FROM matching",
            &[Value::Text("public".into())],
        )),
        vec![Row::new(vec![Value::Text("public".into())])]
    );
}

#[test]
fn system_catalog_materializes_only_relations_referenced_by_the_statement() {
    let catalog = Catalog::default();
    let requested = BTreeSet::from([
        ordadb_catalog::PG_NAMESPACE_TABLE_ID,
        ordadb_catalog::PG_CLASS_TABLE_ID,
    ]);
    let snapshot = system_catalog::build_system_catalog_snapshot(&catalog, None, &requested)
        .expect("requested system rows");
    assert_eq!(
        snapshot.tables().keys().copied().collect::<BTreeSet<_>>(),
        requested
    );
    assert!(!snapshot.tables()[&ordadb_catalog::PG_NAMESPACE_TABLE_ID].is_empty());
    assert!(!snapshot.tables()[&ordadb_catalog::PG_CLASS_TABLE_ID].is_empty());
    let grant = MemoryGrant::new(64 * 1024, 1024 * 1024).expect("scan grant");
    let mut scan = snapshot
        .scan(ordadb_catalog::PG_NAMESPACE_TABLE_ID)
        .expect("virtual scan");
    let first = scan
        .next_chunk(1, &grant)
        .expect("first virtual chunk")
        .expect("virtual row");
    assert_eq!(first.chunk().len(), 1);
    assert!(grant.peak_bytes() > 0);
}

#[test]
fn system_catalog_supporting_relations_and_information_schema_are_relational() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE catalog_parent (
            tenant BIGINT,
            id BIGINT,
            code TEXT,
            CONSTRAINT catalog_parent_pk PRIMARY KEY (tenant, id),
            CONSTRAINT catalog_parent_code_unique UNIQUE (tenant, code),
            CONSTRAINT catalog_parent_code_check CHECK (code <> '')
        )",
        &[],
    );
    execute(
        &mut session,
        "CREATE TABLE catalog_child (
            tenant BIGINT,
            parent_id BIGINT,
            CONSTRAINT catalog_child_parent_fk FOREIGN KEY (tenant, parent_id)
                REFERENCES catalog_parent (tenant, id)
        )",
        &[],
    );
    execute(
        &mut session,
        "CREATE VIEW catalog_parent_view AS SELECT tenant, id, code FROM catalog_parent",
        &[],
    );
    execute(
        &mut session,
        "CREATE SEQUENCE catalog_sequence INCREMENT BY 2 START WITH 10",
        &[],
    );
    execute(
        &mut session,
        "CREATE FUNCTION catalog_echo(value BIGINT)
         RETURNS BIGINT
         LANGUAGE plpgsql
         AS $$
         BEGIN
         RETURN value;
         END;
         $$",
        &[],
    );

    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT a.amname, p.proname
             FROM pg_catalog.pg_am AS a
             JOIN pg_catalog.pg_proc AS p ON p.oid = a.amhandler
             ORDER BY a.amname",
            &[],
        )),
        vec![
            Row::new(vec![
                Value::Text("btree".into()),
                Value::Text("bthandler".into()),
            ]),
            Row::new(vec![
                Value::Text("heap".into()),
                Value::Text("heap_tableam_handler".into()),
            ]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT collname FROM pg_catalog.pg_collation ORDER BY oid",
            &[],
        )),
        vec![
            Row::new(vec![Value::Text("C".into())]),
            Row::new(vec![Value::Text("POSIX".into())]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT table_name, check_option, is_updatable
             FROM information_schema.views
             WHERE table_name = 'catalog_parent_view'",
            &[],
        )),
        vec![Row::new(vec![
            Value::Text("catalog_parent_view".into()),
            Value::Text("NONE".into()),
            Value::Text("NO".into()),
        ])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT sequence_name, data_type, numeric_precision, increment, cycle_option
             FROM information_schema.sequences
             WHERE sequence_name = 'catalog_sequence'",
            &[],
        )),
        vec![Row::new(vec![
            Value::Text("catalog_sequence".into()),
            Value::Text("bigint".into()),
            Value::Int32(64),
            Value::Text("2".into()),
            Value::Text("NO".into()),
        ])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT constraint_name, constraint_type
             FROM information_schema.table_constraints
             WHERE table_name = 'catalog_parent'
             ORDER BY constraint_name",
            &[],
        )),
        vec![
            Row::new(vec![
                Value::Text("catalog_parent_code_check".into()),
                Value::Text("CHECK".into()),
            ]),
            Row::new(vec![
                Value::Text("catalog_parent_code_unique".into()),
                Value::Text("UNIQUE".into()),
            ]),
            Row::new(vec![
                Value::Text("catalog_parent_pk".into()),
                Value::Text("PRIMARY KEY".into()),
            ]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT column_name, ordinal_position, position_in_unique_constraint
             FROM information_schema.key_column_usage
             WHERE constraint_name = 'catalog_child_parent_fk'
             ORDER BY ordinal_position",
            &[],
        )),
        vec![
            Row::new(vec![
                Value::Text("tenant".into()),
                Value::Int32(1),
                Value::Int32(1),
            ]),
            Row::new(vec![
                Value::Text("parent_id".into()),
                Value::Int32(2),
                Value::Int32(2),
            ]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT routine_name, routine_type, data_type, routine_definition
             FROM information_schema.routines
             WHERE routine_name = 'catalog_echo'",
            &[],
        )),
        vec![Row::new(vec![
            Value::Text("catalog_echo".into()),
            Value::Text("FUNCTION".into()),
            Value::Text("bigint".into()),
            Value::Null,
        ])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT p.ordinal_position, p.parameter_mode, p.parameter_name, p.data_type
             FROM information_schema.parameters AS p
             JOIN information_schema.routines AS r
               ON r.specific_name = p.specific_name
             WHERE r.routine_name = 'catalog_echo'
             ORDER BY p.ordinal_position",
            &[],
        )),
        vec![
            Row::new(vec![
                Value::Int32(0),
                Value::Null,
                Value::Null,
                Value::Text("bigint".into()),
            ]),
            Row::new(vec![
                Value::Int32(1),
                Value::Text("IN".into()),
                Value::Text("value".into()),
                Value::Text("bigint".into()),
            ]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT dependent.relname, referenced.relname, d.deptype
             FROM pg_catalog.pg_depend AS d
             JOIN pg_catalog.pg_class AS dependent ON dependent.oid = d.objid
             JOIN pg_catalog.pg_class AS referenced ON referenced.oid = d.refobjid
             WHERE dependent.relname = 'catalog_parent_view'",
            &[],
        )),
        vec![Row::new(vec![
            Value::Text("catalog_parent_view".into()),
            Value::Text("catalog_parent".into()),
            Value::Text("n".into()),
        ])]
    );
    assert!(
        rows(&execute(
            &mut session,
            "SELECT * FROM pg_catalog.pg_description",
            &[],
        ))
        .is_empty()
    );
    assert!(
        rows(&execute(
            &mut session,
            "SELECT * FROM pg_catalog.pg_inherits",
            &[],
        ))
        .is_empty()
    );
}

#[test]
fn system_catalog_oids_survive_engine_reopen() {
    let (directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE reopen_catalog_oid (id BIGINT PRIMARY KEY)",
        &[],
    );
    let before = rows(&execute(
        &mut session,
        "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'reopen_catalog_oid'",
        &[],
    ));
    assert_eq!(before.len(), 1);
    drop(session);
    drop(engine);

    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen engine");
    let mut session = reopened.connect().expect("reconnect");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'reopen_catalog_oid'",
            &[],
        )),
        before
    );
}

#[test]
fn system_catalog_visibility_roles_settings_and_writes_are_safe() {
    let (_directory, engine) = engine();
    let mut alice = engine
        .connect_authenticated(
            SessionAuthorization::new("alice", false)
                .expect("alice authorization")
                .with_system_catalog_metadata(
                    vec![
                        CatalogRoleMetadata {
                            postgres_oid: 20_001,
                            name: "alice".to_owned(),
                            can_login: true,
                            login_enabled: true,
                        },
                        CatalogRoleMetadata {
                            postgres_oid: 20_002,
                            name: "reporting".to_owned(),
                            can_login: false,
                            login_enabled: false,
                        },
                    ],
                    vec![catalog_setting("application_name", "catalog-test")],
                )
                .expect("system catalog metadata"),
        )
        .expect("alice session");
    execute(
        &mut alice,
        "CREATE TABLE alice_private (id BIGINT PRIMARY KEY)",
        &[],
    );
    let catalog = engine.catalog_snapshot().expect("catalog snapshot");
    let public_schema = catalog
        .schema(&Identifier::unquoted("public"))
        .expect("public schema");
    let public_schema_oid = i64::from(
        catalog
            .postgres_oid(ordadb_catalog::PostgresOidObject::Schema(public_schema.id))
            .expect("public schema OID")
            .get(),
    );

    let role_rows = rows(&execute(
        &mut alice,
        "SELECT rolname, oid, rolpassword FROM pg_catalog.pg_roles ORDER BY oid",
        &[],
    ));
    assert_eq!(
        role_rows,
        vec![
            Row::new(vec![
                Value::Text("alice".into()),
                Value::Int64(20_001),
                Value::Text("********".into()),
            ]),
            Row::new(vec![
                Value::Text("reporting".into()),
                Value::Int64(20_002),
                Value::Text("********".into()),
            ]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut alice,
            "SELECT setting FROM pg_catalog.pg_settings \
             WHERE name = 'application_name'",
            &[],
        )),
        vec![Row::new(vec![Value::Text("catalog-test".into())])]
    );
    let namespace_events = execute(
        &mut alice,
        "SELECT n.oid, n.nspname, r.rolname \
         FROM pg_catalog.pg_namespace AS n \
         LEFT JOIN pg_catalog.pg_roles AS r ON r.oid = n.nspowner \
         WHERE n.nspname = 'public'",
        &[],
    );
    let namespace_schema = namespace_events
        .iter()
        .find_map(|event| match event {
            QueryEvent::Schema(schema) => Some(schema),
            _ => None,
        })
        .expect("namespace schema");
    assert_eq!(namespace_schema.fields[0].data_type, ScalarType::Oid);
    assert_eq!(namespace_schema.fields[1].data_type, ScalarType::Name);
    assert_eq!(namespace_schema.fields[2].data_type, ScalarType::Name);
    assert_eq!(
        rows(&namespace_events),
        vec![Row::new(vec![
            Value::Int64(public_schema_oid),
            Value::Text("public".into()),
            Value::Null,
        ])]
    );

    let mut bob = engine
        .connect_authenticated(SessionAuthorization::new("bob", false).expect("bob auth"))
        .expect("bob session");
    assert!(
        rows(&execute(
            &mut bob,
            "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'alice_private'",
            &[],
        ))
        .is_empty()
    );
    let visibility = CatalogVisibility::from_scopes([CatalogVisibilityScope::Object {
        schema: "public".to_owned(),
        name: "alice_private".to_owned(),
    }])
    .expect("catalog visibility");
    let mut reporting = engine
        .connect_authenticated(
            SessionAuthorization::new("reporter", false)
                .expect("reporter auth")
                .with_catalog_visibility(visibility),
        )
        .expect("reporter session");
    assert_eq!(
        rows(&execute(
            &mut reporting,
            "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'alice_private'",
            &[],
        )),
        vec![Row::new(vec![Value::Text("alice_private".into())])]
    );
    let error = bob
        .execute("DELETE FROM pg_catalog.pg_namespace", &[])
        .expect_err("system DML must fail");
    assert_eq!(error.sql_state, "42501");
    let error = bob
        .execute("DROP TABLE pg_catalog.pg_namespace", &[])
        .expect_err("system DDL must fail");
    assert_eq!(error.sql_state, "42501");
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
fn enum_and_domain_ddl_validate_values_and_reopen() {
    let (directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')",
        &[],
    );
    execute(
        &mut session,
        "CREATE DOMAIN positive_int AS integer DEFAULT 1 NOT NULL \
         CONSTRAINT positive CHECK (VALUE > 0)",
        &[],
    );
    execute(
        &mut session,
        "CREATE TABLE feelings (current_mood mood NOT NULL, score positive_int)",
        &[],
    );
    execute(
        &mut session,
        "CREATE TABLE typed_arrays (id bigint, moods mood[], scores positive_int[])",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO feelings (current_mood) VALUES ('happy')",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO typed_arrays VALUES (1, ARRAY['sad', 'happy'], ARRAY[1, 2])",
        &[],
    );

    let error = match session.execute("INSERT INTO feelings VALUES ('angry', 1)", &[]) {
        Ok(_) => panic!("invalid enum value was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.sql_state, "22P02");
    let error = match session.execute("INSERT INTO feelings VALUES ('ok', 0)", &[]) {
        Ok(_) => panic!("invalid domain value was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.sql_state, "23514");
    let error = match session.execute("INSERT INTO feelings VALUES ('ok', NULL)", &[]) {
        Ok(_) => panic!("null domain value was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.sql_state, "23502");
    let error = match session.execute(
        "INSERT INTO typed_arrays VALUES (2, ARRAY['ok', 'angry'], ARRAY[1])",
        &[],
    ) {
        Ok(_) => panic!("invalid enum array value was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.sql_state, "22P02");
    let error = match session.execute(
        "INSERT INTO typed_arrays VALUES (2, ARRAY['ok'], ARRAY[1, 0])",
        &[],
    ) {
        Ok(_) => panic!("invalid domain array value was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.sql_state, "23514");
    let error = match session.execute(
        "INSERT INTO typed_arrays VALUES (2, ARRAY['ok'], ARRAY[1, NULL])",
        &[],
    ) {
        Ok(_) => panic!("null domain array element was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.sql_state, "23502");
    let error = match session.execute("DROP TYPE mood", &[]) {
        Ok(_) => panic!("dependent enum type was dropped"),
        Err(error) => error,
    };
    assert_eq!(error.sql_state, "2BP01");

    drop(session);
    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen engine");
    let mut session = reopened.connect().expect("reconnect");
    let events = execute(
        &mut session,
        "SELECT current_mood, score FROM feelings",
        &[],
    );
    assert_eq!(
        rows(&events),
        vec![Row::new(vec![Value::Text("happy".into()), Value::Int32(1)])]
    );
    execute(&mut session, "DROP TYPE mood CASCADE", &[]);
    let events = execute(&mut session, "SELECT score FROM feelings", &[]);
    assert_eq!(rows(&events), vec![Row::new(vec![Value::Int32(1)])]);
    let events = execute(&mut session, "SELECT id, scores FROM typed_arrays", &[]);
    assert_eq!(rows(&events).len(), 1);
    let error = session
        .execute("SELECT current_mood FROM feelings", &[])
        .expect_err("cascade removed enum column");
    assert_eq!(error.sql_state, "42703");
}

#[test]
fn describe_statement_infers_parameters_without_execution() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    create_documents(&mut session);

    let description = session
        .describe_statement(
            "SELECT id, title FROM documents \
             WHERE score >= $1 ORDER BY id OFFSET $2 LIMIT $3",
        )
        .expect("describe statement");
    assert_eq!(
        description.parameter_types,
        [ScalarType::Int32, ScalarType::Int64, ScalarType::Int64]
    );
    assert_eq!(description.schema.fields.len(), 2);

    let fixed_point = session
        .describe_statement("SELECT $1 AS repeated, id FROM documents WHERE id = $1")
        .expect("describe cross-occurrence parameter");
    assert_eq!(fixed_point.parameter_types, [ScalarType::Int64]);
    assert_eq!(fixed_point.schema.fields[0].data_type, ScalarType::Int64);
    let executed = execute(
        &mut session,
        "SELECT $1 AS repeated, id FROM documents WHERE id = $1",
        &[Value::Int64(1)],
    );
    assert!(matches!(
        executed.first(),
        Some(QueryEvent::Schema(schema))
            if schema.fields[0].data_type == ScalarType::Int64
    ));

    let conflict = session
        .describe_statement("SELECT id FROM documents WHERE id = $1 OR score = $1")
        .expect_err("conflicting parameter types");
    assert_eq!(conflict.sql_state, "42804");
}

#[test]
fn scalar_select_describe_and_execute_share_runtime_metadata() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    session.set_runtime_metadata(
        SessionRuntimeMetadata::new(
            "PostgreSQL 18 compatible OrdaDB test",
            "metadata_db",
            "alice",
            "bootstrap",
        )
        .expect("runtime metadata")
        .with_settings([
            ("client_encoding", "UTF8"),
            ("standard_conforming_strings", "on"),
        ])
        .expect("runtime settings"),
    );

    let description = session
        .describe_statement("SELECT current_database()")
        .expect("describe scalar select");
    assert!(description.parameter_types.is_empty());
    assert_eq!(description.schema.fields.len(), 1);
    assert_eq!(description.schema.fields[0].name, "current_database");
    assert_eq!(description.schema.fields[0].data_type, ScalarType::Text);
    assert!(!description.schema.fields[0].nullable);

    let settings_description = session
        .describe_statement(
            "SELECT current_setting('client_encoding'), \
             current_setting('standard_conforming_strings')",
        )
        .expect("describe settings");
    assert_eq!(settings_description.schema.fields.len(), 2);
    let settings_events = execute(
        &mut session,
        "SELECT current_setting('client_encoding'), \
         current_setting('standard_conforming_strings')",
        &[],
    );
    assert_eq!(
        rows(&settings_events),
        vec![Row::new(vec![
            Value::Text("UTF8".into()),
            Value::Text("on".into()),
        ])]
    );

    for (sql, expected) in [
        (
            "SELECT version()",
            Value::Text("PostgreSQL 18 compatible OrdaDB test".into()),
        ),
        (
            "SELECT current_database()",
            Value::Text("metadata_db".into()),
        ),
        ("SELECT CURRENT_USER", Value::Text("alice".into())),
        ("SELECT SESSION_USER", Value::Text("bootstrap".into())),
        ("SELECT 1", Value::Int32(1)),
    ] {
        let events = execute(&mut session, sql, &[]);
        assert_eq!(rows(&events), vec![Row::new(vec![expected])], "{sql}");
        assert!(matches!(
            events.last(),
            Some(QueryEvent::Complete(CommandComplete { tag, rows_affected: 1 }))
                if tag == "SELECT 1"
        ));
    }

    for invalid in [
        SessionRuntimeMetadata::new("", "db", "user", "user"),
        SessionRuntimeMetadata::new("version", "bad\0db", "user", "user"),
        SessionRuntimeMetadata::new(
            "version",
            "db",
            "x".repeat(MAX_SESSION_RUNTIME_TEXT_BYTES + 1),
            "user",
        ),
    ] {
        assert_eq!(invalid.expect_err("invalid metadata").sql_state, "22023");
    }
}

#[test]
fn describe_statement_rejects_parameter_index_gaps() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    create_documents(&mut session);

    let error = session
        .describe_statement("SELECT id FROM documents WHERE id = $2")
        .expect_err("missing parameter type");
    assert_eq!(error.sql_state, "42P18");
}

#[test]
fn executes_crud_with_parameters_ordering_and_limits() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    create_documents(&mut session);
    let events = execute(
        &mut session,
        "INSERT INTO documents (id, title, score) VALUES \
         ($1, 'first', 10), ($2, 'second', 20), ($3, 'third', 30) \
         RETURNING id, title",
        &[Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    );
    assert_eq!(rows(&events).len(), 3);
    assert!(matches!(
        events.last(),
        Some(QueryEvent::Complete(CommandComplete { tag, rows_affected: 3 }))
            if tag == "INSERT 0 3"
    ));

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

    let events = execute(
        &mut session,
        "SELECT id, title FROM documents ORDER BY id DESC OFFSET $1 LIMIT NULL",
        &[Value::Int64(1)],
    );
    assert_eq!(
        rows(&events),
        vec![
            Row::new(vec![Value::Int64(2), Value::Text("second".into())]),
            Row::new(vec![Value::Int64(1), Value::Text("first".into())]),
        ]
    );

    let events = execute(
        &mut session,
        "UPDATE documents SET title = 'updated' WHERE id = $1 RETURNING id, title AS name",
        &[Value::Int64(2)],
    );
    assert_eq!(
        rows(&events),
        vec![Row::new(vec![
            Value::Int64(2),
            Value::Text("updated".into()),
        ])]
    );
    let events = execute(
        &mut session,
        "DELETE FROM documents WHERE id = $1 RETURNING *",
        &[Value::Int64(1)],
    );
    assert_eq!(
        rows(&events),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("first".into()),
            Value::Int32(10),
        ])]
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
