
#[test]
fn parses_and_binds_remaining_core_session_and_maintenance_commands() {
    let catalog = catalog_with_documents();
    assert!(matches!(
        bind(
            parse("REINDEX TABLE public.documents").expect("parse reindex table"),
            &catalog,
        )
        .expect("bind reindex table"),
        BoundStatement::Reindex {
            target: BoundReindexTarget::Table(_)
        }
    ));
    assert_eq!(
        parse("REINDEX (VERBOSE true) TABLE public.documents")
            .expect_err("reindex parameters are explicit unsupported")
            .sql_state,
        FEATURE_NOT_SUPPORTED
    );
    assert_eq!(
        parse("REINDEX TABLE CONCURRENTLY public.documents")
            .expect_err("concurrent reindex is explicit unsupported")
            .sql_state,
        FEATURE_NOT_SUPPORTED
    );

    assert!(matches!(
        bind(parse("LISTEN events").expect("parse listen"), &catalog)
            .expect("bind listen"),
        BoundStatement::Listen { ref channel } if channel.as_str() == "events"
    ));
    assert!(matches!(
        bind(
            parse("NOTIFY events, 'ready'").expect("parse notify"),
            &catalog,
        )
        .expect("bind notify"),
        BoundStatement::Notify { ref channel, ref payload }
            if channel.as_str() == "events" && payload == "ready"
    ));
    assert!(matches!(
        bind(
            parse("SELECT pg_catalog.pg_notify('events', 'from-function')")
                .expect("parse pg_notify"),
            &catalog,
        )
        .expect("bind pg_notify"),
        BoundStatement::PgNotify { ref schema, .. }
            if schema.fields.len() == 1 && schema.fields[0].name == "pg_notify"
    ));
    assert!(matches!(
        bind(parse("UNLISTEN *").expect("parse unlisten"), &catalog).expect("bind unlisten"),
        BoundStatement::Unlisten { channel: None }
    ));
    assert!(matches!(
        bind(
            parse("DO LANGUAGE plpgsql $$ BEGIN NULL; END $$").expect("parse do"),
            &catalog,
        )
        .expect("bind do"),
        BoundStatement::Do { ref body } if body.contains("BEGIN")
    ));
    assert!(matches!(
        bind(parse("DISCARD ALL").expect("parse discard"), &catalog).expect("bind discard"),
        BoundStatement::DiscardAll
    ));
    assert!(matches!(
        bind(
            parse("DEALLOCATE PREPARE ALL").expect("parse deallocate"),
            &catalog,
        )
        .expect("bind deallocate"),
        BoundStatement::DeallocateAll
    ));
}

#[test]
fn parses_and_binds_alter_drop_and_if_exists_forms() {
    let catalog = catalog_with_documents();
    let alter = bind(
        parse(
            "ALTER TABLE documents \
             ADD COLUMN IF NOT EXISTS archived BOOLEAN DEFAULT FALSE, \
             ALTER COLUMN title SET DEFAULT 'untitled'",
        )
        .expect("parse alter table"),
        &catalog,
    )
    .expect("bind alter table");
    assert!(matches!(
        alter,
        BoundStatement::AlterTable {
            operations,
            ..
        } if operations.len() == 2
    ));

    let missing = bind(
        parse("DROP TABLE IF EXISTS missing").expect("parse drop"),
        &catalog,
    )
    .expect("bind missing drop");
    assert!(matches!(missing, BoundStatement::NoOp { .. }));

    let drop_table = bind(
        parse("DROP TABLE documents CASCADE").expect("parse drop table"),
        &catalog,
    )
    .expect("bind drop table");
    assert!(matches!(
        drop_table,
        BoundStatement::DropObjects {
            kind: DdlObjectKind::Table,
            behavior: DropBehavior::Cascade,
            ..
        }
    ));
}

#[test]
fn exposes_parser_error_positions() {
    let error = parse("SELECT *\nFROM documents WHERE = 1").expect_err("invalid SQL");
    assert_eq!(error.sql_state, SYNTAX_ERROR);
    assert!(error.position.is_some(), "{error:?}");
}

#[test]
fn requires_parameter_type_context() {
    let catalog = catalog_with_documents();
    let error = bind(parse("SELECT $1 FROM documents").expect("parse"), &catalog)
        .expect_err("unknown parameter type");
    assert_eq!(error.sql_state, INDETERMINATE_DATATYPE);
}

#[test]
fn solves_parameter_types_across_occurrences_and_query_boundaries() {
    let catalog = catalog_with_documents();

    let statement = bind(
        parse("SELECT $1 AS repeated, id FROM documents WHERE id = $1")
            .expect("parse cross-clause parameter"),
        &catalog,
    )
    .expect("bind cross-clause parameter");
    let BoundStatement::Select { projection, .. } = statement else {
        panic!("simple SELECT");
    };
    assert_eq!(projection[0].expr.data_type, ScalarType::Int64);

    let set = bind(
        parse("SELECT $1 AS value FROM documents UNION SELECT id FROM documents")
            .expect("parse set parameter"),
        &catalog,
    )
    .expect("bind set parameter");
    let BoundStatement::SetOperation { schema, .. } = set else {
        panic!("set operation");
    };
    assert_eq!(schema.fields[0].data_type, ScalarType::Int64);

    bind(
        parse(
            "WITH picked(value) AS (\
                 SELECT $1 FROM documents WHERE id = $1\
             ) SELECT value FROM picked",
        )
        .expect("parse CTE parameter"),
        &catalog,
    )
    .expect("bind CTE parameter");

    bind(
        parse(
            "SELECT $1, LAG(id, $2, $1) OVER (ORDER BY id) FROM documents \
             WHERE id IN (SELECT id FROM documents WHERE id = $1)",
        )
        .expect("parse window and Apply parameters"),
        &catalog,
    )
    .expect("bind window and Apply parameters");

    bind(
        parse(
            "SELECT outer_documents.id FROM documents outer_documents \
             WHERE EXISTS (\
                 SELECT middle_documents.id FROM documents middle_documents \
                 WHERE EXISTS (\
                     SELECT inner_documents.id FROM documents inner_documents \
                     WHERE inner_documents.id = middle_documents.id \
                       AND middle_documents.id = outer_documents.id\
                 )\
             )",
        )
        .expect("parse nested correlation"),
        &catalog,
    )
    .expect("parameter solver preserves nested correlation scopes");

    let insert = bind(
        parse(
            "INSERT INTO documents (id, title) VALUES ($1, $2) \
             RETURNING $1, $2",
        )
        .expect("parse DML parameters"),
        &catalog,
    )
    .expect("bind DML parameters");
    let BoundStatement::Insert {
        returning: Some(returning),
        ..
    } = insert
    else {
        panic!("INSERT RETURNING");
    };
    assert_eq!(returning.schema.fields[0].data_type, ScalarType::Int64);
    assert_eq!(returning.schema.fields[1].data_type, ScalarType::Text);

    let conflict = bind(
        parse("SELECT $1 FROM documents WHERE id = $1 OR title = $1")
            .expect("parse conflicting parameter"),
        &catalog,
    )
    .expect_err("conflicting parameter constraints");
    assert_eq!(conflict.sql_state, DATATYPE_MISMATCH);
}

#[test]
fn defaults_to_postgresql_and_parses_dialect_names() {
    assert_eq!(
        parse("SELECT id FROM documents").expect("default parse"),
        parse_with_dialect("SELECT id FROM documents", SqlDialect::PostgreSql)
            .expect("explicit parse")
    );
    for (source, expected) in [
        ("postgres", SqlDialect::PostgreSql),
        ("mysql", SqlDialect::MySql),
        ("sqlite3", SqlDialect::Sqlite),
        ("sql-server", SqlDialect::SqlServer),
    ] {
        assert_eq!(source.parse::<SqlDialect>().expect("dialect"), expected);
    }
}

#[test]
fn normalizes_question_mark_and_named_parameters_without_touching_literals() {
    let mysql = parse_with_dialect(
        "SELECT `id` FROM `documents` \
         WHERE `title` = '?' AND `id` >= ? AND `id` <> ? /* ? */ LIMIT 5",
        SqlDialect::MySql,
    )
    .expect("mysql");
    let ParsedStatement::Select {
        table,
        filter: Some(filter),
        limit: Some(limit),
        ..
    } = mysql
    else {
        panic!("mysql select");
    };
    assert_eq!(table.parts[0].name.as_str(), "documents");
    assert_eq!(parameter_indices(&filter), vec![1, 2]);
    assert!(matches!(
        limit.kind,
        ParsedExprKind::Literal(Value::Int32(5))
    ));

    let sql_server = parse_with_dialect(
        "SELECT TOP 7 [id] FROM [documents] WHERE [id] = @p1",
        SqlDialect::SqlServer,
    )
    .expect("sql server");
    let ParsedStatement::Select {
        filter: Some(filter),
        limit: Some(limit),
        ..
    } = sql_server
    else {
        panic!("sql server select");
    };
    assert_eq!(parameter_indices(&filter), vec![1], "{filter:?}");
    assert!(matches!(
        limit.kind,
        ParsedExprKind::Literal(Value::Int64(7))
    ));
}

#[test]
fn accepts_verified_dialect_type_aliases_and_temporal_literals() {
    let mysql = parse_with_dialect(
        "CREATE TABLE dialect_types (\
            tiny TINYINT,\
            medium MEDIUMINT,\
            payload BLOB,\
            created DATETIME\
        )",
        SqlDialect::MySql,
    )
    .expect("mysql types");
    let ParsedStatement::CreateTable { columns, .. } = mysql else {
        panic!("create table");
    };
    assert_eq!(columns[0].data_type, ScalarType::Int16);
    assert_eq!(columns[1].data_type, ScalarType::Int32);
    assert_eq!(columns[2].data_type, ScalarType::Binary);
    assert_eq!(
        columns[3].data_type,
        ScalarType::Timestamp {
            with_timezone: false
        }
    );

    let sql_server = parse_with_dialect(
        "CREATE TABLE [dialect_types] (\
            [token] UNIQUEIDENTIFIER,\
            [title] NVARCHAR(32)\
        )",
        SqlDialect::SqlServer,
    )
    .expect("sql server types");
    let ParsedStatement::CreateTable { columns, .. } = sql_server else {
        panic!("create table");
    };
    assert_eq!(columns[0].data_type, ScalarType::Uuid);
    assert_eq!(
        columns[1].data_type,
        ScalarType::Varchar { length: Some(32) }
    );

    let temporal = parse_with_dialect(
        "INSERT INTO events (created_on, created_at) VALUES (\
            DATE '2026-07-25',\
            TIMESTAMP '2026-07-25 09:30:00.125'\
        )",
        SqlDialect::PostgreSql,
    )
    .expect("temporal literals");
    let ParsedStatement::Insert { rows, .. } = temporal else {
        panic!("insert");
    };
    assert!(matches!(
        rows[0][0].kind,
        ParsedExprKind::Literal(Value::Date(_))
    ));
    assert!(matches!(
        rows[0][1].kind,
        ParsedExprKind::Literal(Value::Timestamp(_))
    ));
}

#[test]
fn parses_postgres_catalog_scalar_type_names_without_user_type_lookup() {
    let statement = parse(
        "CREATE TABLE catalog_scalar_types (object_id OID, object_name NAME, kind \"char\")",
    )
    .expect("PostgreSQL catalog scalar types");
    let ParsedStatement::CreateTable { columns, .. } = statement else {
        panic!("create table");
    };
    assert_eq!(columns[0].data_type, ScalarType::Oid);
    assert_eq!(columns[0].declared_type, None);
    assert_eq!(columns[1].data_type, ScalarType::Name);
    assert_eq!(columns[1].declared_type, None);
    assert_eq!(columns[2].data_type, ScalarType::InternalChar);
    assert_eq!(columns[2].declared_type, None);
}

#[test]
fn normalizes_sqlite_types_quotes_parameters_and_zero_offset() {
    let statement = parse_with_dialect(
        "CREATE TABLE \"sqlite_types\" (\
            \"id\" INTEGER,\
            \"payload\" BLOB,\
            \"created\" DATETIME\
        )",
        SqlDialect::Sqlite,
    )
    .expect("sqlite types");
    let ParsedStatement::CreateTable { name, columns, .. } = statement else {
        panic!("create table");
    };
    assert!(name.parts[0].name.is_quoted());
    assert_eq!(columns[0].data_type, ScalarType::Int32);
    assert_eq!(columns[1].data_type, ScalarType::Binary);
    assert_eq!(
        columns[2].data_type,
        ScalarType::Timestamp {
            with_timezone: false
        }
    );

    let statement = parse_with_dialect(
        "SELECT \"id\" FROM \"sqlite_types\" WHERE \"id\" = ? LIMIT 5 OFFSET 0",
        SqlDialect::Sqlite,
    )
    .expect("sqlite select");
    let ParsedStatement::Select {
        table,
        filter: Some(filter),
        limit: Some(limit),
        ..
    } = statement
    else {
        panic!("sqlite select");
    };
    assert!(table.parts[0].name.is_quoted());
    assert_eq!(parameter_indices(&filter), vec![1]);
    assert!(matches!(
        limit.kind,
        ParsedExprKind::Literal(Value::Int32(5))
    ));
}

#[test]
fn normalizes_supported_row_limit_and_offset_forms() {
    for (dialect, sql, expected_limit, expected_offset) in [
        (
            SqlDialect::PostgreSql,
            "SELECT id FROM documents OFFSET 0 ROWS FETCH FIRST 4 ROWS ONLY",
            4,
            0,
        ),
        (
            SqlDialect::MySql,
            "SELECT id FROM documents LIMIT 1, 5",
            5,
            1,
        ),
        (
            SqlDialect::Sqlite,
            "SELECT id FROM documents LIMIT 6 OFFSET 2",
            6,
            2,
        ),
        (
            SqlDialect::SqlServer,
            "SELECT [id] FROM [documents] ORDER BY [id] \
             OFFSET 3 ROWS FETCH NEXT 7 ROWS ONLY",
            7,
            3,
        ),
    ] {
        let statement = parse_with_dialect(sql, dialect)
            .unwrap_or_else(|error| panic!("{dialect}: {error:?}"));
        let ParsedStatement::Select {
            limit: Some(limit),
            offset: Some(offset),
            ..
        } = statement
        else {
            panic!("select with limit and offset");
        };
        assert!(
            matches!(
                limit.kind,
                ParsedExprKind::Literal(Value::Int32(value))
                    if value == expected_limit
            ) || matches!(
                limit.kind,
                ParsedExprKind::Literal(Value::Int64(value))
                    if value == i64::from(expected_limit)
            ),
            "{dialect}: {limit:?}"
        );
        assert!(
            matches!(
                offset.kind,
                ParsedExprKind::Literal(Value::Int32(value))
                    if value == expected_offset
            ) || matches!(
                offset.kind,
                ParsedExprKind::Literal(Value::Int64(value))
                    if value == i64::from(expected_offset)
            ),
            "{dialect}: {offset:?}"
        );
    }
}

#[test]
fn binds_postgres_offset_and_limit_parameters_as_bigint() {
    let catalog = catalog_with_documents();
    let bound = bind(
        parse("SELECT id FROM documents ORDER BY id OFFSET $1 LIMIT $2").expect("parse offset"),
        &catalog,
    )
    .expect("bind offset");
    let BoundStatement::Select {
        offset: Some(offset),
        limit: Some(limit),
        ..
    } = bound
    else {
        panic!("bound select with offset and limit");
    };
    assert_eq!(offset.data_type, ScalarType::Int64);
    assert_eq!(limit.data_type, ScalarType::Int64);
    assert!(matches!(offset.kind, BoundExprKind::Parameter { index: 1 }));
    assert!(matches!(limit.kind, BoundExprKind::Parameter { index: 2 }));
}

#[test]
fn parses_and_binds_postgres_set_operations() {
    let catalog = catalog_with_documents();
    let bound = bind(
        parse(
            "SELECT id AS item_id FROM documents WHERE id <= 2 \
             UNION ALL \
             SELECT id FROM documents WHERE id >= 2 \
             INTERSECT \
             SELECT id FROM documents \
             ORDER BY item_id DESC NULLS LAST OFFSET $1 LIMIT $2",
        )
        .expect("parse set operation"),
        &catalog,
    )
    .expect("bind set operation");
    let BoundStatement::SetOperation {
        operator: QuerySetOperator::Union,
        all: true,
        right,
        schema,
        order_by,
        offset: Some(offset),
        limit: Some(limit),
        ..
    } = bound
    else {
        panic!("bound outer set operation");
    };
    assert_eq!(schema.fields[0].name, "item_id");
    assert_eq!(order_by[0].column_index, 0);
    assert!(!order_by[0].ascending);
    assert_eq!(order_by[0].nulls_first, Some(false));
    assert!(matches!(offset.kind, BoundExprKind::Parameter { index: 1 }));
    assert!(matches!(limit.kind, BoundExprKind::Parameter { index: 2 }));
    assert!(matches!(
        *right,
        BoundStatement::SetOperation {
            operator: QuerySetOperator::Intersect,
            all: false,
            ..
        }
    ));

    let width_error = bind(
        parse(
            "SELECT id, title FROM documents \
             EXCEPT SELECT id FROM documents",
        )
        .expect("parse mismatched set width"),
        &catalog,
    )
    .expect_err("set width mismatch");
    assert_eq!(width_error.sql_state, SYNTAX_ERROR);

    let type_error = bind(
        parse("SELECT id FROM documents UNION SELECT title FROM documents")
            .expect("parse mismatched set types"),
        &catalog,
    )
    .expect_err("set type mismatch");
    assert_eq!(type_error.sql_state, DATATYPE_MISMATCH);
}

#[test]
fn parses_and_binds_ordered_non_recursive_ctes() {
    let catalog = catalog_with_documents();
    let bound = bind(
        parse(
            "WITH base(item, label) AS (
                 SELECT id, title FROM documents WHERE id >= 1
             ), filtered AS (
                 SELECT item AS id FROM base WHERE item <= 10
             )
             SELECT id FROM filtered ORDER BY id",
        )
        .expect("parse CTEs"),
        &catalog,
    )
    .expect("bind CTEs");
    let BoundStatement::With {
        ctes, body, schema, ..
    } = bound
    else {
        panic!("bound WITH");
    };
    assert_eq!(ctes.len(), 2);
    assert_eq!(schema.fields[0].name, "id");
    assert!(matches!(
        ctes[1].seed.as_ref(),
        BoundStatement::Select { table_id, .. } if *table_id == ctes[0].table_id
    ));
    assert!(matches!(
        body.as_ref(),
        BoundStatement::Select { table_id, .. } if *table_id == ctes[1].table_id
    ));

    let cte_apply = bind(
        parse(
            "WITH base(item) AS (
                 SELECT id FROM documents WHERE id <= 2
             )
             SELECT id FROM documents
             WHERE EXISTS (SELECT item FROM base WHERE item = 2)",
        )
        .expect("parse CTE Apply"),
        &catalog,
    )
    .expect("bind CTE Apply");
    let BoundStatement::With { ctes, body, .. } = cte_apply else {
        panic!("bound CTE Apply WITH");
    };
    let BoundStatement::AdvancedSelect { applies, .. } = body.as_ref() else {
        panic!("bound CTE Apply body");
    };
    assert!(matches!(
        applies[0].query.as_ref(),
        BoundStatement::AdvancedSelect { table, .. } if table.table_id == ctes[0].table_id
    ));

    let duplicate = bind(
        parse(
            "WITH repeated AS (SELECT id FROM documents),
                  repeated AS (SELECT id FROM documents)
             SELECT id FROM repeated",
        )
        .expect("parse duplicate CTE"),
        &catalog,
    )
    .expect_err("duplicate CTE name");
    assert_eq!(duplicate.sql_state, "42712");

    let recursive = bind(
        parse(
            "WITH RECURSIVE numbers(value) AS (
                 SELECT id FROM documents WHERE id = 1
                 UNION ALL
                 SELECT value + 1 FROM numbers WHERE value < 3
             ) SELECT value FROM numbers ORDER BY value",
        )
        .expect("parse recursive CTE"),
        &catalog,
    )
    .expect("bind recursive CTE");
    let BoundStatement::With { ctes, .. } = recursive else {
        panic!("bound recursive WITH");
    };
    assert_eq!(ctes.len(), 1);
    assert!(ctes[0].union_all);
    assert!(ctes[0].recursive.is_some());

    let invalid_recursive = bind(
        parse(
            "WITH RECURSIVE numbers(value) AS (
                 SELECT value FROM numbers
             ) SELECT value FROM numbers",
        )
        .expect("parse invalid recursive CTE"),
        &catalog,
    )
    .expect_err("recursive CTE without UNION");
    assert_eq!(invalid_recursive.sql_state, FEATURE_NOT_SUPPORTED);
}

#[test]
fn parses_and_binds_dml_returning_projections() {
    let catalog = catalog_with_documents();

    let insert = bind(
        parse("INSERT INTO documents VALUES (1, 'one') RETURNING id, title AS name")
            .expect("parse insert returning"),
        &catalog,
    )
    .expect("bind insert returning");
    let BoundStatement::Insert {
        returning: Some(returning),
        ..
    } = insert
    else {
        panic!("insert returning");
    };
    assert_eq!(returning.schema.fields[0].name, "id");
    assert_eq!(returning.schema.fields[1].name, "name");

    let update = bind(
        parse("UPDATE documents SET title = 'changed' RETURNING *")
            .expect("parse update returning"),
        &catalog,
    )
    .expect("bind update returning");
    let BoundStatement::Update {
        returning: Some(returning),
        ..
    } = update
    else {
        panic!("update returning");
    };
    assert_eq!(returning.schema.fields.len(), 2);

    let delete = bind(
        parse("DELETE FROM documents RETURNING id").expect("parse delete returning"),
        &catalog,
    )
    .expect("bind delete returning");
    let BoundStatement::Delete {
        returning: Some(returning),
        ..
    } = delete
    else {
        panic!("delete returning");
    };
    assert_eq!(returning.schema.fields.len(), 1);
    assert_eq!(returning.schema.fields[0].data_type, ScalarType::Int64);
}

#[test]
fn parses_and_binds_postgres_on_conflict_actions() {
    let mut catalog = catalog_with_documents();

    let do_nothing = bind(
        parse("INSERT INTO documents VALUES (1, 'one') ON CONFLICT DO NOTHING")
            .expect("parse conflict do nothing"),
        &catalog,
    )
    .expect("bind conflict do nothing");
    let BoundStatement::Insert {
        on_conflict:
            Some(BoundOnConflict {
                target_columns,
                action,
            }),
        ..
    } = do_nothing
    else {
        panic!("insert on conflict do nothing");
    };
    assert!(target_columns.is_none());
    assert!(matches!(action, BoundConflictAction::DoNothing));

    let do_update = bind(
        parse(
            "INSERT INTO documents VALUES (1, 'new') \
             ON CONFLICT (id) DO UPDATE SET title = excluded.title \
             WHERE documents.id = 1 RETURNING id, title",
        )
        .expect("parse conflict do update"),
        &catalog,
    )
    .expect("bind conflict do update");
    let BoundStatement::Insert {
        on_conflict:
            Some(BoundOnConflict {
                target_columns: Some(target_columns),
                action:
                    BoundConflictAction::DoUpdate {
                        assignments,
                        filter: Some(filter),
                    },
            }),
        returning: Some(returning),
        ..
    } = do_update
    else {
        panic!("insert on conflict do update");
    };
    assert_eq!(target_columns, vec![0]);
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].0, 1);
    assert!(matches!(
        assignments[0].1.kind,
        BoundExprKind::Column { index: 3 }
    ));
    assert_eq!(filter.data_type, ScalarType::Boolean);
    assert_eq!(returning.schema.fields.len(), 2);

    let table_id = catalog
        .table(
            &Identifier::unquoted("public"),
            &Identifier::unquoted("documents"),
        )
        .expect("documents table")
        .id;
    let constraint_name = Identifier::unquoted("documents_id_key");
    catalog
        .create_constraint(
            table_id,
            NewConstraint {
                name: constraint_name.clone(),
                kind: NewConstraintKind::Unique {
                    columns: vec![Identifier::unquoted("id")],
                },
            },
        )
        .expect("create unique constraint");
    let by_constraint = bind(
        parse(&format!(
            "INSERT INTO documents VALUES (1, 'one') \
             ON CONFLICT ON CONSTRAINT {} DO NOTHING",
            constraint_name.as_str()
        ))
        .expect("parse conflict constraint"),
        &catalog,
    )
    .expect("bind conflict constraint");
    let BoundStatement::Insert {
        on_conflict:
            Some(BoundOnConflict {
                target_columns: Some(target_columns),
                action: BoundConflictAction::DoNothing,
            }),
        ..
    } = by_constraint
    else {
        panic!("insert on conflict constraint");
    };
    assert_eq!(target_columns, vec![0]);
}

#[test]
fn parses_and_binds_ordered_postgres_merge_actions() {
    let mut catalog = catalog_with_documents();
    catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("updates"),
            vec![
                NewColumn {
                    name: Identifier::unquoted("id"),
                    data_type: ScalarType::Int64,
                    declared_type: None,
                    nullable: false,
                    primary_key: true,
                    unique: true,
                    default: None,
                },
                NewColumn {
                    name: Identifier::unquoted("title"),
                    data_type: ScalarType::Text,
                    declared_type: None,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    default: None,
                },
            ],
        )
        .expect("create merge source");

    let bound = bind(
        parse(
            "MERGE INTO documents AS d USING updates AS u ON d.id = u.id \
             WHEN MATCHED AND u.title <> 'skip' THEN UPDATE SET title = u.title \
             WHEN MATCHED THEN DELETE \
             WHEN NOT MATCHED BY TARGET THEN \
             INSERT (id, title) VALUES (u.id, u.title) \
             RETURNING id, title",
        )
        .expect("parse merge"),
        &catalog,
    )
    .expect("bind merge");
    let BoundStatement::Merge(BoundMerge {
        target,
        source,
        on,
        clauses,
        returning: Some(returning),
    }) = bound
    else {
        panic!("bound merge");
    };
    assert_eq!(target.binding.as_str(), "d");
    assert_eq!(target.offset, 0);
    assert_eq!(source.binding.as_str(), "u");
    assert_eq!(source.offset, 2);
    assert!(matches!(
        on.kind,
        BoundExprKind::Binary {
            left,
            right,
            ..
        } if matches!(left.kind, BoundExprKind::Column { index: 0 })
            && matches!(right.kind, BoundExprKind::Column { index: 2 })
    ));
    assert_eq!(clauses.len(), 3);
    assert!(matches!(
        &clauses[0],
        BoundMergeClause {
            kind: BoundMergeClauseKind::Matched,
            predicate: Some(_),
            action: BoundMergeAction::Update { assignments },
        } if assignments.len() == 1
            && assignments[0].0 == 1
            && matches!(assignments[0].1.kind, BoundExprKind::Column { index: 3 })
    ));
    assert!(matches!(clauses[1].action, BoundMergeAction::Delete));
    assert!(matches!(
        &clauses[2].action,
        BoundMergeAction::Insert {
            column_indexes,
            values,
        } if column_indexes == &[0, 1]
            && matches!(values[0].kind, BoundExprKind::Column { index: 2 })
            && matches!(values[1].kind, BoundExprKind::Column { index: 3 })
    ));
    assert_eq!(returning.schema.fields.len(), 2);

    let do_nothing = bind(
        parse(
            "MERGE INTO documents AS d USING updates AS u ON d.id = u.id \
             WHEN MATCHED THEN DO NOTHING \
             WHEN NOT MATCHED THEN DO NOTHING \
             WHEN NOT MATCHED BY SOURCE THEN DO NOTHING",
        )
        .expect("parse MERGE DO NOTHING"),
        &catalog,
    )
    .expect("bind MERGE DO NOTHING");
    let BoundStatement::Merge(BoundMerge {
        clauses,
        returning: None,
        ..
    }) = do_nothing
    else {
        panic!("bound MERGE DO NOTHING");
    };
    assert!(matches!(
        clauses.as_slice(),
        [
            BoundMergeClause {
                kind: BoundMergeClauseKind::Matched,
                action: BoundMergeAction::DoNothing,
                ..
            },
            BoundMergeClause {
                kind: BoundMergeClauseKind::NotMatchedByTarget,
                action: BoundMergeAction::DoNothing,
                ..
            },
            BoundMergeClause {
                kind: BoundMergeClauseKind::NotMatchedBySource,
                action: BoundMergeAction::DoNothing,
                ..
            }
        ]
    ));

    let audited = merge_clause_token_info(&significant_tokens(
        "MERGE INTO documents AS d USING updates AS u ON d.id = u.id \
         WHEN MATCHED AND CASE WHEN u.id = 1 THEN TRUE ELSE FALSE END \
             THEN DO NOTHING \
         WHEN NOT MATCHED THEN INSERT (id, title) VALUES (u.id, u.title)",
    ))
    .expect("audit MERGE clauses around CASE");
    assert_eq!(audited.len(), 2);
    assert!(audited[0].do_nothing.is_some());
    assert!(audited[1].do_nothing.is_none());
}
