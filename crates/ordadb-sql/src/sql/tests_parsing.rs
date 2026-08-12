use super::*;

fn catalog_with_documents() -> Catalog {
    let mut catalog = Catalog::default();
    catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("documents"),
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
        .expect("create documents");
    catalog
}

#[test]
fn parses_and_binds_supported_postgres_subset() {
    let catalog = catalog_with_documents();
    let statement = parse(
        "SELECT id, title AS name FROM documents \
         WHERE id >= $1 AND title <> 'archived' ORDER BY id DESC LIMIT 5",
    )
    .expect("parse select");
    let bound = bind(statement, &catalog).expect("bind select");

    let BoundStatement::Select {
        projection,
        filter,
        order_by,
        limit,
        ..
    } = bound
    else {
        panic!("expected select");
    };
    assert_eq!(projection.len(), 2);
    assert_eq!(projection[1].field.name, "name");
    assert!(filter.is_some());
    assert_eq!(order_by[0].column_index, 0);
    assert!(!order_by[0].ascending);
    assert!(limit.is_some());
}

#[test]
fn binds_scalar_selects_with_immutable_session_values() {
    let catalog = Catalog::default();
    let settings = BTreeMap::from([
        ("client_encoding".to_owned(), "UTF8".to_owned()),
        ("standard_conforming_strings".to_owned(), "on".to_owned()),
    ]);
    let session_values = SessionBindValues {
        version: "PostgreSQL 18 compatible OrdaDB test",
        current_database: "metadata_db",
        current_user: "alice",
        session_user: "bootstrap",
        settings: &settings,
    };
    let statement = bind_with_session(
        parse("SELECT version()").expect("parse version"),
        &catalog,
        session_values,
    )
    .expect("bind version");
    assert!(matches!(
        statement,
        BoundStatement::ScalarSelect {
            projection,
            ..
        } if matches!(projection.as_slice(), [BoundProjection {
                expr: BoundExpr {
                    kind: BoundExprKind::Literal(Value::Text(value)),
                    data_type: ScalarType::Text,
                    nullable: false,
                },
                field,
            }] if value == "PostgreSQL 18 compatible OrdaDB test" && field.name == "version")
    ));

    let settings_statement = bind_with_session(
        parse(
            "SELECT current_setting('client_encoding'), \
             current_setting('standard_conforming_strings')",
        )
        .expect("parse settings"),
        &catalog,
        session_values,
    )
    .expect("bind settings");
    let BoundStatement::ScalarSelect { projection, schema } = settings_statement else {
        panic!("expected scalar setting select");
    };
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(projection.len(), 2);
    assert!(matches!(
        projection[0].expr.kind,
        BoundExprKind::Literal(Value::Text(ref value)) if value == "UTF8"
    ));
    assert!(matches!(
        projection[1].expr.kind,
        BoundExprKind::Literal(Value::Text(ref value)) if value == "on"
    ));

    let missing_ok = bind_with_session(
        parse("SELECT current_setting('ordadb.missing', true)").expect("parse missing_ok"),
        &catalog,
        session_values,
    )
    .expect("bind missing_ok");
    assert!(matches!(
        missing_ok,
        BoundStatement::ScalarSelect { projection, .. }
            if matches!(projection.as_slice(), [BoundProjection {
                expr: BoundExpr {
                    kind: BoundExprKind::Literal(Value::Null),
                    ..
                },
                ..
            }])
    ));
    let missing = bind_with_session(
        parse("SELECT current_setting('ordadb.missing')").expect("parse missing"),
        &catalog,
        session_values,
    )
    .expect_err("unknown setting");
    assert_eq!(missing.sql_state, "42704");

    let literal = bind(parse("SELECT 1").expect("parse literal"), &catalog)
        .expect("bind literal scalar select");
    assert!(matches!(
        literal,
        BoundStatement::ScalarSelect {
            projection,
            ..
        } if matches!(projection.as_slice(), [BoundProjection {
                expr: BoundExpr {
                    kind: BoundExprKind::Literal(Value::Int32(1)),
                    ..
                },
                field,
            }] if field.name == "?column?")
    ));

    let missing = bind(
        parse("SELECT current_database()").expect("parse session function"),
        &catalog,
    )
    .expect_err("session function requires immutable session values");
    assert_eq!(missing.sql_state, "55000");
}

#[test]
fn parses_and_binds_interval_arrays_and_explicit_casts() {
    let catalog = catalog_with_documents();
    let statement = bind(
        parse(
            "SELECT ARRAY[[1, 2], [3, 4]]::BIGINT[] AS values, \
             INTERVAL '1 day 02:03:04.5' AS duration FROM documents",
        )
        .expect("parse typed expressions"),
        &catalog,
    )
    .expect("bind typed expressions");
    let projection = match statement {
        BoundStatement::Select { projection, .. }
        | BoundStatement::AdvancedSelect { projection, .. } => projection,
        other => panic!("unexpected statement: {other:?}"),
    };
    assert_eq!(
        projection[0].expr.data_type,
        ScalarType::Array {
            element: Box::new(ScalarType::Int64),
        }
    );
    let BoundExprKind::Cast { expr } = &projection[0].expr.kind else {
        panic!("array cast");
    };
    assert!(matches!(
        &expr.kind,
        BoundExprKind::Array { dimensions, elements }
            if dimensions == &[
                ArrayDimension::new(2, 1),
                ArrayDimension::new(2, 1),
            ] && elements.len() == 4
    ));
    assert_eq!(projection[1].expr.data_type, ScalarType::Interval);

    let ddl = bind(
        parse(
            "CREATE TABLE typed_values (ids BIGINT[], elapsed INTERVAL, observed TIMESTAMPTZ)",
        )
        .expect("parse typed DDL"),
        &Catalog::default(),
    )
    .expect("bind typed DDL");
    let BoundStatement::CreateTable { columns, .. } = ddl else {
        panic!("create table");
    };
    assert_eq!(
        columns[0].data_type,
        ScalarType::Array {
            element: Box::new(ScalarType::Int64),
        }
    );
    assert_eq!(columns[1].data_type, ScalarType::Interval);
    assert_eq!(
        columns[2].data_type,
        ScalarType::Timestamp {
            with_timezone: true,
        }
    );
}

#[test]
fn parses_and_binds_common_scalar_functions() {
    let statement = bind(
        parse(
            "SELECT LOWER(title), UPPER(title), LENGTH(title), OCTET_LENGTH(title), \
             ABS(id), COALESCE(title, 'fallback'), NULLIF(id, 0), \
             CONCAT(title, id), SUBSTRING(title FROM 1 FOR 2), \
             JSONB_TYPEOF('{\"a\":1}'::JSONB), ARRAY_LENGTH(ARRAY[[1,2],[3,4]], 2), \
             CARDINALITY(ARRAY[1,2,3]), BTRIM('xyhelloxy', 'xy'), \
             LTRIM('  hello'), RTRIM('hello  '), REPLACE(title, 'a', 'b'), \
             STRPOS('åbcå', 'c'), GREATEST(id, 0), LEAST(id, 0), \
             TRIM(BOTH 'xy' FROM 'xyhelloxy'), POSITION('c' IN 'åbcå') FROM documents",
        )
        .expect("parse scalar functions"),
        &catalog_with_documents(),
    )
    .expect("bind scalar functions");
    let projection = match statement {
        BoundStatement::Select { projection, .. }
        | BoundStatement::AdvancedSelect { projection, .. } => projection,
        other => panic!("unexpected statement: {other:?}"),
    };
    assert_eq!(projection.len(), 21);
    assert_eq!(projection[0].expr.data_type, ScalarType::Text);
    assert_eq!(projection[2].expr.data_type, ScalarType::Int32);
    assert_eq!(projection[4].expr.data_type, ScalarType::Int64);
    assert_eq!(projection[7].expr.data_type, ScalarType::Text);
    assert_eq!(projection[9].expr.data_type, ScalarType::Text);
    assert_eq!(projection[10].expr.data_type, ScalarType::Int32);
    assert_eq!(projection[12].expr.data_type, ScalarType::Text);
    assert_eq!(projection[16].expr.data_type, ScalarType::Int32);
    assert_eq!(projection[17].expr.data_type, ScalarType::Int64);
    assert_eq!(projection[18].expr.data_type, ScalarType::Int64);
    assert_eq!(projection[19].expr.data_type, ScalarType::Text);
    assert_eq!(projection[20].expr.data_type, ScalarType::Int32);

    let parameter = bind(
        parse("SELECT LOWER($1) FROM documents").expect("parse function parameter"),
        &catalog_with_documents(),
    )
    .expect("bind function parameter");
    let BoundStatement::Select { projection, .. } = parameter else {
        panic!("parameter select");
    };
    let BoundExprKind::Function { arguments, .. } = &projection[0].expr.kind else {
        panic!("lower call");
    };
    assert_eq!(arguments[0].data_type, ScalarType::Text);
}

#[test]
fn parses_and_binds_owned_transaction_control_variants() {
    let catalog = Catalog::default();
    let begin = ParsedStatement::Begin {
        characteristics: TransactionCharacteristics::default(),
    };
    let bound_begin = BoundStatement::Begin {
        characteristics: TransactionCharacteristics::default(),
    };
    for (sql, parsed, bound) in [
        ("BEGIN", begin.clone(), bound_begin.clone()),
        (
            "  bEgIn \n TrAnSaCtIoN ; ",
            begin.clone(),
            bound_begin.clone(),
        ),
        ("\n StArT \t TrAnSaCtIoN ;", begin, bound_begin),
        (
            "cOmMiT \n WoRk;",
            ParsedStatement::Commit {
                chain: TransactionChain::Default,
            },
            BoundStatement::Commit {
                chain: TransactionChain::Default,
            },
        ),
        (
            "\r\n RoLlBaCk \t TrAnSaCtIoN ;",
            ParsedStatement::Rollback {
                chain: TransactionChain::Default,
            },
            BoundStatement::Rollback {
                chain: TransactionChain::Default,
            },
        ),
    ] {
        let actual = parse(sql).expect("parse transaction control");
        assert_eq!(actual, parsed, "{sql}");
        assert_eq!(
            bind(actual, &catalog).expect("bind transaction control"),
            bound,
            "{sql}"
        );
    }
}

#[test]
fn parses_transaction_modes_chaining_and_savepoints() {
    assert_eq!(
        parse("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE").expect("begin"),
        ParsedStatement::Begin {
            characteristics: TransactionCharacteristics {
                isolation_level: IsolationLevel::Serializable,
                access_mode: TransactionAccessMode::ReadOnly,
                deferrable: true,
            },
        }
    );
    assert_eq!(
        parse("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED, READ WRITE NOT DEFERRABLE")
            .expect("start"),
        ParsedStatement::Begin {
            characteristics: TransactionCharacteristics::default(),
        }
    );
    assert_eq!(
        parse("COMMIT AND CHAIN").expect("commit chain"),
        ParsedStatement::Commit {
            chain: TransactionChain::Chain,
        }
    );
    assert_eq!(
        parse("ROLLBACK WORK AND NO CHAIN").expect("rollback no chain"),
        ParsedStatement::Rollback {
            chain: TransactionChain::NoChain,
        }
    );
    assert_eq!(
        parse("SAVEPOINT before_update").expect("savepoint"),
        ParsedStatement::Savepoint {
            name: ParsedIdentifier {
                name: Identifier::unquoted("before_update"),
                position: Some(11),
            },
        }
    );
    assert!(matches!(
        parse("ROLLBACK TO SAVEPOINT before_update").expect("rollback to"),
        ParsedStatement::RollbackTo { name }
            if name.name == Identifier::unquoted("before_update")
    ));
    assert!(matches!(
        parse("RELEASE SAVEPOINT before_update").expect("release"),
        ParsedStatement::ReleaseSavepoint { name }
            if name.name == Identifier::unquoted("before_update")
    ));

    assert_eq!(
        parse("BEGIN READ WRITE DEFERRABLE")
            .expect_err("invalid deferrable")
            .sql_state,
        "25001"
    );
    assert_eq!(
        parse("BEGIN READ ONLY READ WRITE")
            .expect_err("duplicate access mode")
            .sql_state,
        SYNTAX_ERROR
    );
    assert_eq!(
        parse("BEGIN ISOLATION LEVEL SNAPSHOT")
            .expect_err("snapshot")
            .sql_state,
        FEATURE_NOT_SUPPORTED
    );
}

#[test]
fn parses_and_binds_transactional_maintenance() {
    let catalog = catalog_with_documents();
    let documents = catalog
        .table(
            &Identifier::unquoted("public"),
            &Identifier::unquoted("documents"),
        )
        .expect("documents")
        .id;
    assert_eq!(
        bind(parse("ANALYZE documents").expect("analyze"), &catalog).expect("bind analyze"),
        BoundStatement::Analyze {
            table_id: Some(documents),
        }
    );
    assert_eq!(
        bind(parse("VACUUM documents").expect("vacuum"), &catalog).expect("bind vacuum"),
        BoundStatement::Vacuum {
            table_id: Some(documents),
            analyze: false,
        }
    );
    assert_eq!(
        bind(
            parse("VACUUM ANALYZE documents").expect("vacuum analyze"),
            &catalog,
        )
        .expect("bind vacuum analyze"),
        BoundStatement::Vacuum {
            table_id: Some(documents),
            analyze: true,
        }
    );
    assert_eq!(
        parse("VACUUM FULL documents")
            .expect_err("vacuum full")
            .sql_state,
        FEATURE_NOT_SUPPORTED
    );
    assert_eq!(
        parse("ANALYZE documents (id)")
            .expect_err("column analyze")
            .sql_state,
        FEATURE_NOT_SUPPORTED
    );
}

#[test]
fn parses_create_table_constraints_and_normalizes_names() {
    let statement = parse(
        "CREATE TABLE Audit.Events (\
            id BIGINT PRIMARY KEY,\
            code VARCHAR(24) UNIQUE,\
            payload JSONB NOT NULL\
        )",
    )
    .expect("parse create table");
    let ParsedStatement::CreateTable { name, columns, .. } = statement else {
        panic!("expected create table");
    };
    assert_eq!(name.parts[0].name.as_str(), "audit");
    assert!(columns[0].primary_key);
    assert!(columns[1].unique);
    assert!(!columns[2].nullable);
}

#[test]
fn reports_unknown_objects_columns_and_type_mismatches() {
    let catalog = catalog_with_documents();

    let error = bind(parse("SELECT id FROM missing").expect("parse"), &catalog)
        .expect_err("unknown table");
    assert_eq!(error.sql_state, UNDEFINED_TABLE);

    let error = bind(
        parse("SELECT missing FROM documents").expect("parse"),
        &catalog,
    )
    .expect_err("unknown column");
    assert_eq!(error.sql_state, UNDEFINED_COLUMN);
    assert!(error.position.is_some());

    let error = bind(
        parse("INSERT INTO documents (id, title) VALUES ('bad', 'title')").expect("parse"),
        &catalog,
    )
    .expect_err("type mismatch");
    assert_eq!(error.sql_state, DATATYPE_MISMATCH);

    let error = bind(
        parse("INSERT INTO documents (id, title) VALUES (id, 'title')").expect("parse"),
        &catalog,
    )
    .expect_err("column is not visible in VALUES");
    assert_eq!(error.sql_state, UNDEFINED_COLUMN);
}

#[test]
fn rejects_unsupported_syntax_without_panicking() {
    let catalog = catalog_with_documents();
    for sql in [
        "CREATE TABLE inherited (id BIGINT) INHERITS (documents)",
        "CREATE INDEX unsupported_hash ON documents USING HASH (id)",
    ] {
        let error = parse(sql)
            .and_then(|statement| bind(statement, &catalog))
            .expect_err("unsupported syntax");
        assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED, "{sql}");
    }
}

#[test]
fn binds_indexes_joins_aggregates_and_explain() {
    let catalog = catalog_with_documents();
    let index = bind(
        parse("CREATE INDEX documents_title_idx ON documents (title) INCLUDE (id)")
            .expect("parse index"),
        &catalog,
    )
    .expect("bind index");
    assert!(matches!(index, BoundStatement::CreateIndex { .. }));

    let grouped = bind(
        parse(
            "SELECT d.id, COUNT(e.id) AS total \
             FROM documents d LEFT JOIN documents e ON d.id = e.id \
             GROUP BY d.id HAVING COUNT(e.id) > 0",
        )
        .expect("parse grouped join"),
        &catalog,
    )
    .expect("bind grouped join");
    let BoundStatement::AdvancedSelect {
        joins,
        aggregate,
        group_by,
        ..
    } = grouped
    else {
        panic!("advanced select");
    };
    assert_eq!(joins.len(), 1);
    assert!(aggregate);
    assert_eq!(group_by.len(), 1);

    let explain = bind(
        parse("EXPLAIN SELECT id FROM documents WHERE id = 1").expect("parse explain"),
        &catalog,
    )
    .expect("bind explain");
    assert!(matches!(explain, BoundStatement::Explain { .. }));
}

#[test]
fn binds_lateral_derived_tables_with_left_to_right_scope() {
    let catalog = catalog_with_documents();
    let statement = parse(
        "SELECT d.id, matched.renamed_title FROM documents d \
         INNER JOIN LATERAL ( \
             SELECT lookup.title FROM documents lookup WHERE lookup.id = d.id \
         ) AS matched(renamed_title) ON TRUE",
    )
    .expect("parse LATERAL derived table");
    let ParsedStatement::AdvancedSelect { joins, .. } = &statement else {
        panic!("parsed LATERAL advanced select");
    };
    assert!(matches!(
        joins[0].source,
        ParsedJoinSource::Derived { lateral: true, .. }
    ));

    let statement = bind(statement, &catalog).expect("bind LATERAL derived table");
    let BoundStatement::AdvancedSelect { joins, schema, .. } = statement else {
        panic!("bound LATERAL advanced select");
    };
    assert_eq!(schema.fields[1].name, "renamed_title");
    let BoundJoinSource::Derived {
        lateral,
        query,
        offset,
        width,
        ..
    } = &joins[0].source
    else {
        panic!("bound derived join source");
    };
    assert!(*lateral);
    assert_eq!((*offset, *width), (2, 1));
    let BoundStatement::AdvancedSelect {
        filter: Some(filter),
        ..
    } = query.as_ref()
    else {
        panic!("bound correlated derived query");
    };
    assert!(matches!(
        &filter.kind,
        BoundExprKind::Binary { right, .. }
            if matches!(right.kind, BoundExprKind::Correlation { depth: 1, index: 0 })
    ));

    let error = bind(
        parse(
            "SELECT d.id FROM documents d \
             INNER JOIN (SELECT lookup.id FROM documents lookup WHERE lookup.id = d.id) \
             AS matched ON TRUE",
        )
        .expect("parse non-LATERAL derived table"),
        &catalog,
    )
    .expect_err("non-LATERAL source cannot see its left input");
    assert_eq!(error.sql_state, UNDEFINED_COLUMN);

    let error = bind(
        parse(
            "SELECT d.id FROM documents d \
             INNER JOIN LATERAL (SELECT lookup.id FROM documents lookup) \
             AS matched(first, extra) ON TRUE",
        )
        .expect("parse excessive derived aliases"),
        &catalog,
    )
    .expect_err("derived alias count");
    assert_eq!(error.sql_state, SYNTAX_ERROR);
}

#[test]
fn binds_postgres_aggregate_filter_predicates() {
    let catalog = catalog_with_documents();
    let statement = bind(
        parse(
            "SELECT COUNT(*) FILTER (WHERE id > $1) AS selected, \
             SUM(id) FILTER (WHERE title = 'keep') AS total FROM documents",
        )
        .expect("parse aggregate FILTER"),
        &catalog,
    )
    .expect("bind aggregate FILTER");
    let BoundStatement::AdvancedSelect { projection, .. } = statement else {
        panic!("aggregate FILTER select");
    };
    assert!(matches!(
        projection[0].expr.kind,
        BoundExprKind::Aggregate {
            filter: Some(_),
            ..
        }
    ));
    assert!(matches!(
        projection[1].expr.kind,
        BoundExprKind::Aggregate {
            filter: Some(_),
            ..
        }
    ));

    let error = bind(
        parse("SELECT COUNT(*) FILTER (WHERE id) FROM documents")
            .expect("parse invalid aggregate FILTER"),
        &catalog,
    )
    .expect_err("non-boolean aggregate FILTER");
    assert_eq!(error.sql_state, DATATYPE_MISMATCH);
}

#[test]
fn binds_postgres_distinct_aggregate_inputs() {
    let catalog = catalog_with_documents();
    let statement = bind(
        parse(
            "SELECT COUNT(DISTINCT id), SUM(DISTINCT id) FILTER (WHERE id > 0), \
             AVG(ALL id) FROM documents",
        )
        .expect("parse DISTINCT aggregates"),
        &catalog,
    )
    .expect("bind DISTINCT aggregates");
    let BoundStatement::AdvancedSelect { projection, .. } = statement else {
        panic!("DISTINCT aggregate select");
    };
    assert!(matches!(
        projection[0].expr.kind,
        BoundExprKind::Aggregate { distinct: true, .. }
    ));
    assert!(matches!(
        projection[1].expr.kind,
        BoundExprKind::Aggregate {
            distinct: true,
            filter: Some(_),
            ..
        }
    ));
    assert!(matches!(
        projection[2].expr.kind,
        BoundExprKind::Aggregate {
            distinct: false,
            ..
        }
    ));

    let error = parse("SELECT COUNT(DISTINCT *) FROM documents")
        .expect_err("DISTINCT wildcard aggregate must fail");
    assert_eq!(error.sql_state, SYNTAX_ERROR);
}

#[test]
fn binds_inline_ranking_windows_after_apply_slots() {
    let catalog = catalog_with_documents();
    let statement = bind(
        parse(
            "SELECT id, \
             (SELECT lookup.id FROM documents lookup \
              WHERE lookup.id = documents.id LIMIT 1) AS copied, \
             ROW_NUMBER() OVER (PARTITION BY title ORDER BY id DESC) AS row_no, \
             RANK() OVER (PARTITION BY title ORDER BY id DESC) AS rank_no, \
             DENSE_RANK() OVER (PARTITION BY title ORDER BY id DESC) AS dense_no \
             FROM documents ORDER BY row_no, id",
        )
        .expect("parse ranking windows"),
        &catalog,
    )
    .expect("bind ranking windows");
    let BoundStatement::AdvancedSelect {
        applies,
        windows,
        projection,
        order_by,
        ..
    } = statement
    else {
        panic!("ranking window select");
    };
    assert_eq!(applies.len(), 1);
    assert_eq!(windows.len(), 3);
    assert!(matches!(windows[0].function, WindowFunction::RowNumber));
    assert!(matches!(windows[1].function, WindowFunction::Rank));
    assert!(matches!(windows[2].function, WindowFunction::DenseRank));
    assert_eq!(windows[0].partition_by.len(), 1);
    assert_eq!(windows[0].order_by.len(), 1);
    assert!(!windows[0].order_by[0].ascending);
    assert!(matches!(
        projection[1].expr.kind,
        BoundExprKind::ApplyValue { index: 2 }
    ));
    for (ordinal, projection) in projection.iter().skip(2).enumerate() {
        assert!(matches!(
            projection.expr.kind,
            BoundExprKind::ApplyValue { index } if index == 3 + ordinal
        ));
        assert_eq!(projection.field.data_type, ScalarType::Int64);
        assert!(!projection.field.nullable);
    }
    assert_eq!(order_by.len(), 2);
}

#[test]
fn ranking_windows_fail_closed_for_unimplemented_or_invalid_forms() {
    let catalog = catalog_with_documents();
    let named = bind(
        parse(
            "SELECT ROW_NUMBER() OVER ranked FROM documents \
         WINDOW ranked AS (ORDER BY id)",
        )
        .expect("parse named window"),
        &catalog,
    )
    .expect("bind named window");
    let BoundStatement::AdvancedSelect { windows, .. } = named else {
        panic!("named window select");
    };
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].order_by.len(), 1);

    let inherited = bind(
        parse(
            "SELECT RANK() OVER ranked FROM documents \
             WINDOW grouped AS (PARTITION BY title), \
                    ranked AS (grouped ORDER BY id)",
        )
        .expect("parse inherited window"),
        &catalog,
    )
    .expect("bind inherited window");
    let BoundStatement::AdvancedSelect { windows, .. } = inherited else {
        panic!("inherited window select");
    };
    assert_eq!(windows[0].partition_by.len(), 1);
    assert_eq!(windows[0].order_by.len(), 1);

    let missing = parse("SELECT RANK() OVER missing_window FROM documents")
        .expect_err("missing named window");
    assert_eq!(missing.sql_state, "42704");

    let duplicate = parse(
        "SELECT RANK() OVER duplicate_name FROM documents \
         WINDOW duplicate_name AS (ORDER BY id), duplicate_name AS (ORDER BY title)",
    )
    .expect_err("duplicate named window");
    assert_eq!(duplicate.sql_state, "42712");

    let framed = bind(
        parse("SELECT RANK() OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM documents")
            .expect("parse explicit frame"),
        &catalog,
    )
    .expect("bind explicit frame");
    let BoundStatement::AdvancedSelect { windows, .. } = framed else {
        panic!("framed window select");
    };
    assert!(matches!(
        windows[0].frame,
        Some(BoundWindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: BoundWindowFrameBound::UnboundedPreceding,
            end_bound: BoundWindowFrameBound::CurrentRow,
        })
    ));

    let inline_inherited = bind(
        parse(
            "SELECT RANK() OVER (grouped ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
             FROM documents WINDOW grouped AS (PARTITION BY title)",
        )
        .expect("parse inline inherited frame"),
        &catalog,
    )
    .expect("bind inline inherited frame");
    let BoundStatement::AdvancedSelect { windows, .. } = inline_inherited else {
        panic!("inline inherited window select");
    };
    assert_eq!(windows[0].partition_by.len(), 1);
    assert!(windows[0].frame.is_some());

    let invalid_order = parse(
        "SELECT RANK() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) \
         FROM documents",
    )
    .expect_err("frame end before start");
    assert_eq!(invalid_order.sql_state, "42P20");

    let range_without_order = bind(
        parse("SELECT RANK() OVER (RANGE 1 PRECEDING) FROM documents")
            .expect("parse RANGE offset"),
        &catalog,
    )
    .expect_err("RANGE offset without one ORDER BY");
    assert_eq!(range_without_order.sql_state, "42P20");

    let variable_offset = bind(
        parse("SELECT RANK() OVER (ORDER BY id ROWS id PRECEDING) FROM documents")
            .expect("parse variable ROWS offset"),
        &catalog,
    )
    .expect_err("frame variable");
    assert_eq!(variable_offset.sql_state, "42P20");

    let groups = parse("SELECT RANK() OVER (ORDER BY id GROUPS CURRENT ROW) FROM documents")
        .expect_err("GROUPS frame");
    assert_eq!(groups.sql_state, FEATURE_NOT_SUPPORTED);

    let in_where = bind(
        parse("SELECT id FROM documents WHERE ROW_NUMBER() OVER () = 1")
            .expect("parse window in WHERE"),
        &catalog,
    )
    .expect_err("window in WHERE");
    assert_eq!(in_where.sql_state, "42P20");

    let nested = bind(
        parse("SELECT SUM(ROW_NUMBER() OVER ()) FROM documents").expect("parse nested window"),
        &catalog,
    )
    .expect_err("nested window");
    assert_eq!(nested.sql_state, "42P20");
}
