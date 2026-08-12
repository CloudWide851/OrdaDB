
#[test]
fn binds_value_and_aggregate_window_types() {
    let catalog = catalog_with_documents();
    let statement = bind(
        parse(
            "SELECT
                LAG(id) OVER ordered,
                LEAD(id, 2, 0) OVER ordered,
                FIRST_VALUE(title) OVER ordered,
                LAST_VALUE(title) OVER (
                    ordered ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                NTH_VALUE(title, 2) OVER ordered,
                COUNT(*) OVER (PARTITION BY title),
                SUM(id) FILTER (WHERE id > 0) OVER (PARTITION BY title),
                AVG(id) OVER (PARTITION BY title)
             FROM documents WINDOW ordered AS (PARTITION BY title ORDER BY id)",
        )
        .expect("parse value and aggregate windows"),
        &catalog,
    )
    .expect("bind value and aggregate windows");
    let BoundStatement::AdvancedSelect {
        windows, schema, ..
    } = statement
    else {
        panic!("window select");
    };
    assert_eq!(windows.len(), 8);
    assert!(matches!(windows[0].function, WindowFunction::Lag));
    assert!(matches!(windows[1].function, WindowFunction::Lead));
    assert!(matches!(windows[2].function, WindowFunction::FirstValue));
    assert!(matches!(windows[3].function, WindowFunction::LastValue));
    assert!(matches!(windows[4].function, WindowFunction::NthValue));
    assert!(matches!(
        windows[5].function,
        WindowFunction::Aggregate(AggregateFunction::Count)
    ));
    assert!(windows[5].count_star);
    assert!(windows[6].filter.is_some());
    assert_eq!(schema.fields[0].data_type, ScalarType::Int64);
    assert!(schema.fields[0].nullable);
    assert_eq!(schema.fields[2].data_type, ScalarType::Text);
    assert_eq!(schema.fields[5].data_type, ScalarType::Int64);
    assert!(!schema.fields[5].nullable);
    assert_eq!(schema.fields[6].data_type, ScalarType::Int64);
    assert!(schema.fields[6].nullable);
    assert_eq!(schema.fields[7].data_type, ScalarType::Float64);

    let distinct = parse("SELECT COUNT(DISTINCT id) OVER () FROM documents")
        .expect_err("DISTINCT window aggregate");
    assert_eq!(distinct.sql_state, FEATURE_NOT_SUPPORTED);

    let non_aggregate_filter =
        parse("SELECT LAG(id) FILTER (WHERE id > 0) OVER (ORDER BY id) FROM documents")
            .expect_err("FILTER on non-aggregate window");
    assert_eq!(non_aggregate_filter.sql_state, "42809");
}

#[test]
fn binds_select_distinct_and_enforces_order_visibility() {
    let catalog = catalog_with_documents();
    let statement = bind(
        parse("SELECT DISTINCT title FROM documents ORDER BY title")
            .expect("parse SELECT DISTINCT"),
        &catalog,
    )
    .expect("bind SELECT DISTINCT");
    assert!(matches!(
        statement,
        BoundStatement::AdvancedSelect { distinct: true, .. }
    ));

    let all = bind(
        parse("SELECT ALL title FROM documents").expect("parse SELECT ALL"),
        &catalog,
    )
    .expect("bind SELECT ALL");
    assert!(matches!(all, BoundStatement::Select { .. }));

    let error = bind(
        parse("SELECT DISTINCT title FROM documents ORDER BY id")
            .expect("parse invalid DISTINCT order"),
        &catalog,
    )
    .expect_err("DISTINCT order expression outside projection");
    assert_eq!(error.sql_state, "42P10");

    let error = parse("SELECT DISTINCT ON (title) title FROM documents")
        .expect_err("DISTINCT ON remains explicit");
    assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED);

    let mut json_catalog = Catalog::default();
    json_catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("payloads"),
            vec![NewColumn::new(
                Identifier::unquoted("payload"),
                ScalarType::Json,
            )],
        )
        .expect("JSON table");
    for sql in [
        "SELECT DISTINCT payload FROM payloads",
        "SELECT COUNT(DISTINCT payload) FROM payloads",
    ] {
        let error = bind(parse(sql).expect("parse JSON DISTINCT"), &json_catalog)
            .expect_err("JSON DISTINCT equality");
        assert_eq!(error.sql_state, "42883");
    }
}

#[test]
fn binds_in_lists_with_shared_parameter_types() {
    let catalog = catalog_with_documents();
    let statement = bind(
        parse("SELECT id FROM documents WHERE id NOT IN ($1, 2, NULL)")
            .expect("parse NOT IN list"),
        &catalog,
    )
    .expect("bind NOT IN list");
    let BoundStatement::Select {
        filter:
            Some(BoundExpr {
                kind:
                    BoundExprKind::InList {
                        expr,
                        list,
                        negated,
                    },
                ..
            }),
        ..
    } = statement
    else {
        panic!("bound NOT IN filter");
    };
    assert!(negated);
    assert_eq!(expr.data_type, ScalarType::Int64);
    assert_eq!(list.len(), 3);
    assert!(matches!(
        list[0].kind,
        BoundExprKind::Parameter { index: 1 }
    ));
    assert_eq!(list[0].data_type, ScalarType::Int64);

    let error = bind(
        parse("SELECT id FROM documents WHERE id IN ('wrong')")
            .expect("parse incompatible IN list"),
        &catalog,
    )
    .expect_err("incompatible IN types");
    assert_eq!(error.sql_state, DATATYPE_MISMATCH);

    let error = bind(
        parse("SELECT id FROM documents WHERE $1 IN ($2)")
            .expect("parse indeterminate IN list"),
        &catalog,
    )
    .expect_err("indeterminate IN types");
    assert_eq!(error.sql_state, INDETERMINATE_DATATYPE);
}

#[test]
fn owns_and_binds_uncorrelated_subquery_apply_forms() {
    let mut catalog = catalog_with_documents();
    catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("apply_lookup"),
            vec![NewColumn::new(
                Identifier::unquoted("id"),
                ScalarType::Int64,
            )],
        )
        .expect("create Apply lookup");
    let scalar =
        parse("SELECT (SELECT id FROM documents LIMIT 1) AS selected_id FROM documents")
            .expect("parse scalar subquery");
    let ParsedStatement::AdvancedSelect { projection, .. } = &scalar else {
        panic!("scalar subquery select");
    };
    assert!(matches!(
        &projection[0],
        ParsedProjection::Expression {
            expr: ParsedExpr {
                kind: ParsedExprKind::ScalarSubquery(_),
                ..
            },
            ..
        }
    ));
    let scalar = bind(scalar, &catalog).expect("bind scalar Apply");
    let BoundStatement::AdvancedSelect {
        applies,
        projection,
        ..
    } = scalar
    else {
        panic!("bound scalar Apply select");
    };
    assert_eq!(applies.len(), 1);
    assert!(matches!(applies[0].kind, BoundApplyKind::Scalar));
    assert_eq!(
        bound_query_schema(&applies[0].query).expect("scalar Apply schema"),
        Schema::new(vec![Field::new("id", ScalarType::Int64, false)])
    );
    assert!(matches!(
        projection[0].expr,
        BoundExpr {
            kind: BoundExprKind::ApplyValue { index: 2 },
            data_type: ScalarType::Int64,
            nullable: true,
        }
    ));

    let cases = [
        (
            "SELECT id FROM documents WHERE EXISTS (SELECT id FROM documents)",
            SubqueryQuantifier::Any,
        ),
        (
            "SELECT id FROM documents WHERE id IN (SELECT id FROM documents)",
            SubqueryQuantifier::Any,
        ),
        (
            "SELECT id FROM documents WHERE id = ANY (SELECT id FROM documents)",
            SubqueryQuantifier::Any,
        ),
        (
            "SELECT id FROM documents WHERE id <> ALL (SELECT id FROM documents)",
            SubqueryQuantifier::All,
        ),
    ];
    for (index, (sql, quantifier)) in cases.into_iter().enumerate() {
        let statement = parse(sql).expect("parse subquery predicate");
        let ParsedStatement::AdvancedSelect {
            filter: Some(filter),
            ..
        } = &statement
        else {
            panic!("subquery predicate select");
        };
        match (index, &filter.kind) {
            (0, ParsedExprKind::Exists { negated: false, .. }) => {}
            (1, ParsedExprKind::InSubquery { negated: false, .. }) => {}
            (
                2 | 3,
                ParsedExprKind::QuantifiedSubquery {
                    quantifier: actual, ..
                },
            ) if actual == &quantifier => {}
            _ => panic!("unexpected owned subquery form: {filter:?}"),
        }
        let statement = bind(statement, &catalog).expect("bind uncorrelated Apply");
        let BoundStatement::AdvancedSelect {
            applies,
            filter: Some(filter),
            ..
        } = statement
        else {
            panic!("bound subquery predicate select");
        };
        assert_eq!(applies.len(), 1);
        assert!(matches!(
            filter.kind,
            BoundExprKind::ApplyValue { index: 2 }
        ));
        match (index, &applies[0].kind) {
            (0, BoundApplyKind::Exists { negated: false }) => {
                assert!(!filter.nullable);
            }
            (
                1,
                BoundApplyKind::In {
                    left,
                    negated: false,
                },
            ) => {
                assert_eq!(left.data_type, ScalarType::Int64);
                assert!(filter.nullable);
            }
            (
                2 | 3,
                BoundApplyKind::Quantified {
                    quantifier: actual, ..
                },
            ) if actual == &quantifier => {
                assert!(filter.nullable);
            }
            _ => panic!("unexpected bound Apply form: {:?}", applies[0].kind),
        }
    }

    let parameterized = bind(
        parse("SELECT id FROM documents WHERE $1 IN (SELECT id FROM documents)")
            .expect("parse parameterized Apply"),
        &catalog,
    )
    .expect("bind parameterized Apply");
    let BoundStatement::AdvancedSelect { applies, .. } = parameterized else {
        panic!("parameterized Apply select");
    };
    assert!(matches!(
        applies[0].kind,
        BoundApplyKind::In {
            left: BoundExpr {
                kind: BoundExprKind::Parameter { index: 1 },
                data_type: ScalarType::Int64,
                ..
            },
            ..
        }
    ));

    bind(
        parse("SELECT id FROM documents WHERE EXISTS (SELECT id, title FROM documents)")
            .expect("parse multi-column EXISTS"),
        &catalog,
    )
    .expect("EXISTS may project multiple columns");

    let error = bind(
        parse("SELECT (SELECT id, title FROM documents) FROM documents")
            .expect("parse multi-column scalar subquery"),
        &catalog,
    )
    .expect_err("scalar subquery must return one column");
    assert_eq!(error.sql_state, SYNTAX_ERROR);

    let dependencies = bind(
        parse(
            "SELECT id FROM documents \
             WHERE EXISTS (SELECT id FROM apply_lookup)",
        )
        .expect("parse Apply dependencies"),
        &catalog,
    )
    .expect("bind Apply dependencies");
    assert_eq!(bound_statement_references(&dependencies).len(), 2);

    let correlated = bind(
        parse(
            "SELECT outer_docs.id FROM documents outer_docs
             WHERE EXISTS (
                 SELECT inner_docs.id FROM documents inner_docs
                 WHERE inner_docs.id = outer_docs.id
             )",
        )
        .expect("parse correlated Apply"),
        &catalog,
    )
    .expect("bind correlated Apply");
    let BoundStatement::AdvancedSelect { applies, .. } = correlated else {
        panic!("bound correlated Apply select");
    };
    let BoundStatement::AdvancedSelect {
        filter: Some(filter),
        ..
    } = applies[0].query.as_ref()
    else {
        panic!("bound correlated Apply inner query");
    };
    assert!(matches!(
        filter.kind,
        BoundExprKind::Binary {
            right: ref correlation,
            ..
        } if matches!(
            correlation.kind,
            BoundExprKind::Correlation { depth: 1, index: 0 }
        )
    ));

    let nested = bind(
        parse(
            "SELECT outer_docs.id FROM documents outer_docs
             WHERE EXISTS (
                 SELECT middle_docs.id FROM documents middle_docs
                 WHERE EXISTS (
                     SELECT inner_docs.id FROM documents inner_docs
                     WHERE inner_docs.id = middle_docs.id
                       AND middle_docs.id = outer_docs.id
                 )
             )",
        )
        .expect("parse nested correlated Apply"),
        &catalog,
    )
    .expect("bind nested correlated Apply");
    assert!(matches!(nested, BoundStatement::AdvancedSelect { .. }));
}

#[test]
fn owns_and_binds_row_comparisons_and_row_apply_forms() {
    fn select_filter(statement: &ParsedStatement) -> &ParsedExpr {
        match statement {
            ParsedStatement::Select {
                filter: Some(filter),
                ..
            }
            | ParsedStatement::AdvancedSelect {
                filter: Some(filter),
                ..
            } => filter,
            _ => panic!("statement does not contain a SELECT filter"),
        }
    }

    let catalog = catalog_with_documents();

    let direct = parse("SELECT id FROM documents WHERE (id, title) = (1, 'first')")
        .expect("parse row equality");
    let filter = select_filter(&direct);
    assert!(matches!(
        filter.kind,
        ParsedExprKind::Binary {
            op: BinaryOperator::And,
            ..
        }
    ));
    bind(direct, &catalog).expect("bind row equality");

    let not_equal = parse("SELECT id FROM documents WHERE (id, title) <> (1, 'first')")
        .expect("parse row inequality");
    let filter = select_filter(&not_equal);
    assert!(matches!(
        filter.kind,
        ParsedExprKind::Unary {
            op: UnaryOperator::Not,
            ..
        }
    ));
    bind(not_equal, &catalog).expect("bind row inequality");

    let listed =
        parse("SELECT id FROM documents WHERE (id, title) IN ((1, 'first'), (2, NULL))")
            .expect("parse row IN list");
    let filter = select_filter(&listed);
    assert!(matches!(
        filter.kind,
        ParsedExprKind::Binary {
            op: BinaryOperator::Or,
            ..
        }
    ));
    bind(listed, &catalog).expect("bind row IN list");

    let cases = [
        (
            "SELECT id FROM documents WHERE (id, title) = (SELECT id, title FROM documents LIMIT 1)",
            None,
            false,
        ),
        (
            "SELECT id FROM documents WHERE (id, title) IN (SELECT id, title FROM documents)",
            Some(SubqueryQuantifier::Any),
            false,
        ),
        (
            "SELECT id FROM documents WHERE (id, title) NOT IN (SELECT id, title FROM documents)",
            Some(SubqueryQuantifier::Any),
            true,
        ),
        (
            "SELECT id FROM documents WHERE (id, title) = ANY (SELECT id, title FROM documents)",
            Some(SubqueryQuantifier::Any),
            false,
        ),
        (
            "SELECT id FROM documents WHERE (id, title) <> ALL (SELECT id, title FROM documents)",
            Some(SubqueryQuantifier::All),
            false,
        ),
    ];
    for (sql, quantifier, negated) in cases {
        let statement = parse(sql).expect("parse row subquery");
        let ParsedStatement::AdvancedSelect {
            filter: Some(filter),
            ..
        } = &statement
        else {
            panic!("row subquery select");
        };
        assert!(matches!(
            filter.kind,
            ParsedExprKind::RowSubquery {
                quantifier: actual,
                negated: actual_negated,
                ..
            } if actual == quantifier && actual_negated == negated
        ));

        let statement = bind(statement, &catalog).expect("bind row subquery");
        let BoundStatement::AdvancedSelect {
            applies,
            filter: Some(filter),
            ..
        } = statement
        else {
            panic!("bound row subquery select");
        };
        assert_eq!(applies.len(), 1);
        assert!(matches!(filter.kind, BoundExprKind::ApplyValue { .. }));
        match (&applies[0].kind, quantifier) {
            (
                BoundApplyKind::RowScalar {
                    left,
                    op: BinaryOperator::Eq,
                    operand_types,
                },
                None,
            ) => {
                assert_eq!(left.len(), 2);
                assert_eq!(operand_types, &[ScalarType::Int64, ScalarType::Text]);
            }
            (
                BoundApplyKind::RowQuantified {
                    left,
                    quantifier: actual,
                    negated: actual_negated,
                    operand_types,
                    ..
                },
                Some(expected),
            ) => {
                assert_eq!(left.len(), 2);
                assert_eq!(*actual, expected);
                assert_eq!(*actual_negated, negated);
                assert_eq!(operand_types, &[ScalarType::Int64, ScalarType::Text]);
            }
            _ => panic!("unexpected bound row Apply form: {:?}", applies[0].kind),
        }
    }

    let direct_width = parse("SELECT id FROM documents WHERE (id, title) = (1, 'first', 3)")
        .expect_err("direct row width mismatch");
    assert_eq!(direct_width.sql_state, SYNTAX_ERROR);

    let subquery_width = bind(
        parse("SELECT id FROM documents WHERE (id, title) IN (SELECT id FROM documents)")
            .expect("parse subquery row width mismatch"),
        &catalog,
    )
    .expect_err("subquery row width mismatch");
    assert_eq!(subquery_width.sql_state, SYNTAX_ERROR);

    let ordered = parse("SELECT id FROM documents WHERE (id, title) < (1, 'first')")
        .expect_err("ordered row comparison remains explicit");
    assert_eq!(ordered.sql_state, FEATURE_NOT_SUPPORTED);

    let mixed_list = parse("SELECT id FROM documents WHERE (id, title) IN ((1, 'first'), 2)")
        .expect_err("row IN list requires row entries");
    assert_eq!(mixed_list.sql_state, SYNTAX_ERROR);
}

#[test]
fn binds_full_text_and_hnsw_index_methods_with_bounded_options() {
    let catalog = catalog_with_documents();
    let full_text = bind(
        parse(
            "CREATE INDEX documents_fts ON documents USING fulltext (title) \
             WITH (analyzer = 'whitespace')",
        )
        .expect("parse full-text index"),
        &catalog,
    )
    .expect("bind full-text index");
    let BoundStatement::CreateIndex { index, .. } = full_text else {
        panic!("full-text CREATE INDEX");
    };
    assert_eq!(index.method, IndexMethod::FullText);
    assert_eq!(
        index.options,
        IndexOptions::FullText {
            analyzer: FullTextAnalyzer::Whitespace
        }
    );

    let mut vector_catalog = Catalog::default();
    vector_catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("embeddings"),
            vec![NewColumn::new(
                Identifier::unquoted("value"),
                ScalarType::Vector {
                    dimensions: Some(3),
                },
            )],
        )
        .expect("vector table");
    let hnsw = bind(
        parse(
            "CREATE INDEX embeddings_hnsw ON embeddings USING hnsw (value) \
             WITH (metric = 'l2', m = 8, ef_construction = 32, ef_search = 24)",
        )
        .expect("parse HNSW index"),
        &vector_catalog,
    )
    .expect("bind HNSW index");
    let BoundStatement::CreateIndex { index, .. } = hnsw else {
        panic!("HNSW CREATE INDEX");
    };
    assert_eq!(index.method, IndexMethod::Hnsw);
    assert_eq!(
        index.options,
        IndexOptions::Hnsw {
            metric: VectorDistanceMetric::L2,
            dimensions: 3,
            m: 8,
            ef_construction: 32,
            ef_search: 24,
        }
    );

    let wrong_type = bind(
        parse("CREATE INDEX documents_hnsw ON documents USING hnsw (title)")
            .expect("parse wrong HNSW"),
        &catalog,
    )
    .expect_err("HNSW requires VECTOR");
    assert_eq!(wrong_type.sql_state, DATATYPE_MISMATCH);
    let unsupported_option = bind(
        parse(
            "CREATE INDEX documents_bad_fts ON documents USING fulltext (title) \
             WITH (language = 'english')",
        )
        .expect("parse unsupported option"),
        &catalog,
    )
    .expect_err("unsupported option");
    assert_eq!(unsupported_option.sql_state, FEATURE_NOT_SUPPORTED);
}

#[test]
fn parses_and_binds_postgres_ddl_defaults_constraints_sequences_and_views() {
    let catalog = catalog_with_documents();
    let create = bind(
        parse(
            "CREATE TABLE IF NOT EXISTS child_items (\
                id BIGINT DEFAULT 1,\
                document_id BIGINT,\
                CONSTRAINT child_items_pkey PRIMARY KEY (id, document_id),\
                CONSTRAINT child_items_document_fk FOREIGN KEY (document_id) \
                    REFERENCES documents(id) ON DELETE CASCADE ON UPDATE RESTRICT,\
                CONSTRAINT child_items_id_check CHECK (id > 0)\
            )",
        )
        .expect("parse table ddl"),
        &catalog,
    )
    .expect("bind table ddl");
    let BoundStatement::CreateTable {
        columns,
        constraints,
        if_not_exists,
        ..
    } = create
    else {
        panic!("create table");
    };
    assert!(if_not_exists);
    assert_eq!(
        columns[0].default.as_ref().map(|value| value.sql.as_str()),
        Some("1")
    );
    assert_eq!(constraints.len(), 3);
    assert!(matches!(
        constraints[0].kind,
        NewConstraintKind::PrimaryKey { ref columns } if columns.len() == 2
    ));
    assert!(matches!(
        constraints[1].kind,
        NewConstraintKind::ForeignKey {
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::Restrict,
            ..
        }
    ));

    let sequence = bind(
        parse(
            "CREATE SEQUENCE IF NOT EXISTS public.child_items_seq \
             AS BIGINT INCREMENT BY 2 START WITH 5 NO CYCLE",
        )
        .expect("parse sequence"),
        &catalog,
    )
    .expect("bind sequence");
    assert!(matches!(
        sequence,
        BoundStatement::CreateSequence {
            sequence: NewSequence {
                increment: 2,
                start_value: Some(5),
                cycle: false,
                ..
            },
            if_not_exists: true,
            ..
        }
    ));

    let view = bind(
        parse("CREATE VIEW docs_view (doc_id, doc_title) AS SELECT id, title FROM documents")
            .expect("parse view"),
        &catalog,
    )
    .expect("bind view");
    let BoundStatement::CreateView {
        output, references, ..
    } = view
    else {
        panic!("create view");
    };
    assert_eq!(output.fields[0].name, "doc_id");
    assert_eq!(references.len(), 1);
}

#[test]
fn parses_procedure_options_across_arbitrary_whitespace() {
    let procedure = parse(
        "CREATE PROCEDURE public.refresh_items(value public.mood, count BIGINT)
         LANGUAGE
         plpgsql
         AS $body$
         BEGIN
         RETURN;
         END;
         $body$",
    )
    .expect("parse procedure");
    assert!(matches!(
        procedure,
        ParsedStatement::CreateRoutine {
            kind: RoutineKind::Procedure,
            arguments,
            body,
            ..
        } if arguments.len() == 2
            && arguments[0].declared_type.is_some()
            && arguments[1].declared_type.is_none()
            && body.contains("RETURN")
    ));
}

#[test]
fn parses_routine_argument_modes_and_trigger_activation() {
    let procedure = parse(
        "CREATE PROCEDURE public.mode_probe(\
         IN input_value BIGINT, OUT output_value TEXT, \
         INOUT counter INTEGER, VARIADIC rest BIGINT[]) \
         LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$",
    )
    .expect("parse procedure modes");
    let ParsedStatement::CreateRoutine { arguments, .. } = procedure else {
        panic!("expected procedure");
    };
    assert_eq!(
        arguments
            .iter()
            .map(|argument| argument.mode)
            .collect::<Vec<_>>(),
        vec![
            RoutineArgumentMode::In,
            RoutineArgumentMode::Out,
            RoutineArgumentMode::InOut,
            RoutineArgumentMode::Variadic,
        ]
    );

    let function = parse(
        "CREATE FUNCTION public.output_probe(IN value BIGINT, OUT doubled BIGINT) \
         LANGUAGE plpgsql AS $$ BEGIN doubled := value * 2; RETURN; END $$",
    )
    .expect("parse function OUT mode");
    assert!(matches!(
        function,
        ParsedStatement::CreateRoutine {
            return_type: None,
            ref arguments,
            ..
        } if arguments[1].mode == RoutineArgumentMode::Out
    ));

    let statement_trigger = parse(
        "CREATE TRIGGER documents_audit AFTER UPDATE ON documents \
         FOR EACH STATEMENT EXECUTE FUNCTION public.audit_documents()",
    )
    .expect("parse statement trigger");
    assert!(matches!(
        statement_trigger,
        ParsedStatement::CreateTrigger {
            timing: TriggerTiming::AfterStatement,
            level: TriggerLevel::Statement,
            ..
        }
    ));

    let instead_of = parse(
        "CREATE TRIGGER documents_view_insert INSTEAD OF INSERT ON documents_view \
         FOR EACH ROW EXECUTE FUNCTION public.insert_documents_view()",
    )
    .expect("parse INSTEAD OF trigger");
    assert!(matches!(
        instead_of,
        ParsedStatement::CreateTrigger {
            timing: TriggerTiming::InsteadOf,
            level: TriggerLevel::Row,
            ..
        }
    ));
}

#[test]
fn binds_regular_view_instead_of_trigger_targets_and_view_dml() {
    let mut catalog = catalog_with_documents();
    let documents = catalog
        .table(
            &Identifier::unquoted("public"),
            &Identifier::unquoted("documents"),
        )
        .expect("documents")
        .id;
    let view_id = catalog
        .create_view(
            &Identifier::unquoted("public"),
            ordadb_catalog::NewView {
                name: Identifier::unquoted("document_view"),
                kind: ViewKind::Regular,
                query: "SELECT id, title FROM documents".into(),
                output: Schema::new(vec![
                    Field::new("id", ScalarType::Int64, false),
                    Field::new("title", ScalarType::Text, false),
                ]),
                materialized_table_id: None,
                populated: true,
                references: vec![CatalogObjectRef::Table(documents)],
            },
        )
        .expect("view");
    let routine_id = catalog
        .create_or_replace_routine(
            &Identifier::unquoted("public"),
            ordadb_catalog::NewRoutine {
                name: Identifier::unquoted("document_view_insert"),
                kind: RoutineKind::Function,
                arguments: Vec::new(),
                return_type: None,
                return_declared_type: None,
                returns_set: false,
                language: "plpgsql".into(),
                body: "BEGIN RETURN NEW; END".into(),
                replace: false,
                references: Vec::new(),
            },
        )
        .expect("routine");
    let unavailable = bind(
        parse("INSERT INTO document_view VALUES (1, 'one')").expect("parse view insert"),
        &catalog,
    )
    .expect_err("view DML requires a trigger");
    assert_eq!(unavailable.sql_state, "55000");

    let create = bind(
        parse(
            "CREATE TRIGGER document_view_insert_trigger INSTEAD OF INSERT ON document_view \
             FOR EACH ROW EXECUTE FUNCTION document_view_insert()",
        )
        .expect("parse view trigger"),
        &catalog,
    )
    .expect("bind view trigger");
    assert!(matches!(
        create,
        BoundStatement::CreateTrigger {
            target: TriggerTarget::View(id),
            ..
        } if id == view_id
    ));
    catalog
        .create_trigger_on_target_with_level(
            TriggerTarget::View(view_id),
            Identifier::unquoted("document_view_insert_trigger"),
            TriggerTiming::InsteadOf,
            TriggerLevel::Row,
            BTreeSet::from([CatalogTriggerEvent::Insert]),
            routine_id,
        )
        .expect("catalog view trigger");
    assert!(matches!(
        bind(
            parse("INSERT INTO document_view VALUES ($1, $2) RETURNING *")
                .expect("parse parameterized view insert"),
            &catalog,
        )
        .expect("bind parameterized view insert"),
        BoundStatement::ViewInsert { view_id: id, .. } if id == view_id
    ));
}
