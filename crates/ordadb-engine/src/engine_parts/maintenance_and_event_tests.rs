
#[test]
fn failed_vacuum_and_analyze_reopen_without_publishing_candidates() {
    let directory = tempdir().expect("tempdir");
    {
        let engine =
            Engine::open(EngineConfig::new(directory.path())).expect("baseline engine");
        let mut session = engine.connect().expect("baseline session");
        create_documents(&mut session);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'v1', 1)",
            &[],
        );
        execute(
            &mut session,
            "UPDATE documents SET title = 'v2' WHERE id = 1",
            &[],
        );
    }

    let vacuum_fault = ordadb_transaction::DeterministicFaultInjector::new();
    let vacuum_injector: Arc<dyn FaultInjector> = vacuum_fault.clone();
    let engine =
        Engine::open_with_fault_injector(EngineConfig::new(directory.path()), vacuum_injector)
            .expect("vacuum engine");
    let baseline_generation = engine
        .status_snapshot()
        .expect("baseline status")
        .generation;
    let mut session = engine.connect().expect("vacuum session");
    vacuum_fault
        .arm(FaultPoint::AfterDataSync, 1)
        .expect("arm vacuum fault");
    assert_eq!(
        session
            .execute("VACUUM documents", &[])
            .expect_err("vacuum fault")
            .sql_state,
        "58030"
    );
    drop(session);
    drop(engine);

    let recovered = Engine::open(EngineConfig::new(directory.path())).expect("recover vacuum");
    assert_eq!(
        recovered
            .status_snapshot()
            .expect("recovered status")
            .generation,
        baseline_generation
    );
    let table_id = recovered
        .catalog_snapshot()
        .expect("catalog")
        .table(
            &Identifier::unquoted("public"),
            &Identifier::unquoted("documents"),
        )
        .expect("documents")
        .id;
    assert_eq!(
        recovered
            .state
            .read()
            .expect("recovered state")
            .versions
            .get(&table_id)
            .expect("version chain")
            .len(),
        2
    );
    drop(recovered);

    let analyze_fault = ordadb_transaction::DeterministicFaultInjector::new();
    let analyze_injector: Arc<dyn FaultInjector> = analyze_fault.clone();
    let engine =
        Engine::open_with_fault_injector(EngineConfig::new(directory.path()), analyze_injector)
            .expect("analyze engine");
    let baseline_generation = engine
        .status_snapshot()
        .expect("baseline status")
        .generation;
    let mut session = engine.connect().expect("analyze session");
    analyze_fault
        .arm(FaultPoint::AfterDataSync, 1)
        .expect("arm analyze fault");
    assert_eq!(
        session
            .execute("ANALYZE documents", &[])
            .expect_err("analyze fault")
            .sql_state,
        "58030"
    );
    drop(session);
    drop(engine);

    let recovered = Engine::open(EngineConfig::new(directory.path())).expect("recover analyze");
    assert_eq!(
        recovered
            .status_snapshot()
            .expect("recovered status")
            .generation,
        baseline_generation
    );
    let mut session = recovered.connect().expect("final session");
    execute(&mut session, "VACUUM ANALYZE documents", &[]);
    assert_eq!(
        recovered
            .state
            .read()
            .expect("vacuumed state")
            .versions
            .get(&table_id)
            .expect("compacted version chain")
            .len(),
        1
    );
}

#[test]
fn repeated_savepoint_names_use_the_nearest_frame_and_release_it() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("session");
    create_documents(&mut session);
    execute(&mut session, "BEGIN", &[]);
    execute(
        &mut session,
        "INSERT INTO documents VALUES (1, 'base', 1)",
        &[],
    );
    execute(&mut session, "SAVEPOINT repeated", &[]);
    execute(
        &mut session,
        "INSERT INTO documents VALUES (2, 'middle', 2)",
        &[],
    );
    execute(&mut session, "SAVEPOINT repeated", &[]);
    execute(
        &mut session,
        "INSERT INTO documents VALUES (3, 'latest', 3)",
        &[],
    );

    execute(&mut session, "ROLLBACK TO repeated", &[]);
    execute(&mut session, "RELEASE SAVEPOINT repeated", &[]);
    execute(&mut session, "ROLLBACK TO repeated", &[]);
    execute(&mut session, "COMMIT", &[]);
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id FROM documents ORDER BY id",
            &[],
        )),
        vec![Row::new(vec![Value::Int64(1)])]
    );
}

#[test]
fn read_only_and_chained_transactions_preserve_characteristics() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("session");
    create_documents(&mut session);
    execute(
        &mut session,
        "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE",
        &[],
    );
    let first_id = match &session.sql_transaction {
        SqlTransactionState::Active(transaction) => {
            assert_eq!(
                transaction.transaction.characteristics(),
                Some(TransactionCharacteristics {
                    isolation_level: ordadb_transaction::IsolationLevel::Serializable,
                    access_mode: TransactionAccessMode::ReadOnly,
                    deferrable: true,
                })
            );
            transaction.transaction.transaction_id()
        }
        _ => panic!("expected active transaction"),
    };
    assert_eq!(
        session
            .execute("INSERT INTO documents VALUES (1, 'blocked', 1)", &[])
            .expect_err("read only")
            .sql_state,
        "25006"
    );
    execute(&mut session, "ROLLBACK AND CHAIN", &[]);
    let second_id = match &session.sql_transaction {
        SqlTransactionState::Active(transaction) => {
            assert_eq!(
                transaction.transaction.characteristics(),
                Some(TransactionCharacteristics {
                    isolation_level: ordadb_transaction::IsolationLevel::Serializable,
                    access_mode: TransactionAccessMode::ReadOnly,
                    deferrable: true,
                })
            );
            transaction.transaction.transaction_id()
        }
        _ => panic!("expected chained transaction"),
    };
    assert!(second_id > first_id);
    execute(&mut session, "ROLLBACK AND NO CHAIN", &[]);
    assert_eq!(session.transaction_status(), TransactionStatus::Idle);
}

#[test]
fn deferrable_safe_snapshot_wait_cancels_through_the_session_boundary() {
    let (_directory, engine) = engine();
    let mut writer = engine.connect().expect("writer");
    let mut reader = engine.connect().expect("reader");
    create_documents(&mut writer);
    execute(&mut writer, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
    execute(
        &mut reader,
        "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE",
        &[],
    );
    let cancellation = Arc::new(AtomicBool::new(false));
    let worker_cancellation = Arc::clone(&cancellation);
    let (send, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = reader
            .execute_stream_with_cancellation(
                "SELECT id FROM documents",
                &[],
                worker_cancellation,
            )
            .map(|_| ());
        send.send((result, reader.transaction_status()))
            .expect("send cancellation result");
    });
    std::thread::sleep(Duration::from_millis(20));
    cancellation.store(true, Ordering::Release);

    let (result, status) = receive
        .recv_timeout(Duration::from_secs(1))
        .expect("cancelled statement result");
    assert_eq!(
        result.expect_err("safe snapshot cancellation").sql_state,
        "57014"
    );
    assert_eq!(status, TransactionStatus::Failed);
    execute(&mut writer, "ROLLBACK", &[]);
    worker.join().expect("reader worker");
}

#[test]
fn emits_schema_then_work_then_exactly_one_completion() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    create_documents(&mut session);
    let events = execute(&mut session, "SELECT * FROM documents", &[]);

    assert!(matches!(events.first(), Some(QueryEvent::Schema(_))));
    assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, QueryEvent::Complete(_)))
            .count(),
        1
    );
    assert!(events[1..events.len() - 1].iter().all(|event| matches!(
        event,
        QueryEvent::Batch(_) | QueryEvent::Progress(_) | QueryEvent::Notice(_)
    )));
}

#[test]
fn open_bootstraps_and_reopens_the_persistent_store() {
    let directory = tempdir().expect("tempdir");
    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
        assert_eq!(configured_data_dir(engine.config()), directory.path());
    }
    assert!(directory.path().join("ordadb.data").is_file());
    assert!(Engine::open(EngineConfig::new(directory.path())).is_ok());
}

#[test]
fn cluster_transaction_floor_seeds_the_first_durable_transaction() {
    let directory = tempdir().expect("tempdir");
    let engine = Engine::open(EngineConfig::for_cluster(
        directory.path().join("database"),
        directory.path(),
        42,
    ))
    .expect("open cluster database");
    let mut session = engine.connect().expect("session");
    execute(
        &mut session,
        "CREATE TABLE migration_floor (id BIGINT)",
        &[],
    );
    drop(session);
    drop(engine);

    assert_eq!(
        ordadb_transaction::inspect_wal_read_only(directory.path().join("database"))
            .expect("inspect WAL")
            .max_transaction_id
            .expect("transaction ID")
            .get(),
        42
    );
}

#[test]
fn executes_inner_left_join_grouped_aggregates_and_having() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
        &[],
    );
    execute(
        &mut session,
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT, amount BIGINT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO orders VALUES (10, 1, 5), (11, 1, 7)",
        &[],
    );

    let grouped = execute(
        &mut session,
        "SELECT c.id, COUNT(o.id) AS order_count, SUM(o.amount) AS total \
         FROM customers c LEFT JOIN orders o ON c.id = o.customer_id \
         GROUP BY c.id ORDER BY c.id",
        &[],
    );
    assert_eq!(
        rows(&grouped),
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(2), Value::Int64(12)]),
            Row::new(vec![Value::Int64(2), Value::Int64(0), Value::Null]),
        ]
    );

    let filtered_aggregates = execute(
        &mut session,
        "SELECT c.id, COUNT(*) FILTER (WHERE o.amount > 5) AS large_orders, \
         SUM(o.amount) FILTER (WHERE o.amount > 5) AS large_total \
         FROM customers c LEFT JOIN orders o ON c.id = o.customer_id \
         GROUP BY c.id ORDER BY c.id",
        &[],
    );
    assert_eq!(
        rows(&filtered_aggregates),
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(1), Value::Int64(7)]),
            Row::new(vec![Value::Int64(2), Value::Int64(0), Value::Null]),
        ]
    );

    let having = execute(
        &mut session,
        "SELECT c.id, COUNT(o.id) AS order_count \
         FROM customers c INNER JOIN orders o ON c.id = o.customer_id \
         GROUP BY c.id HAVING COUNT(o.id) > 1",
        &[],
    );
    assert_eq!(
        rows(&having),
        vec![Row::new(vec![Value::Int64(1), Value::Int64(2)])]
    );

    let aggregate = execute(
        &mut session,
        "SELECT COUNT(*), AVG(amount), MIN(amount), MAX(amount) FROM orders",
        &[],
    );
    assert_eq!(
        rows(&aggregate),
        vec![Row::new(vec![
            Value::Int64(2),
            Value::Float64(6.0),
            Value::Int64(5),
            Value::Int64(7),
        ])]
    );

    execute(
        &mut session,
        "CREATE TABLE aggregate_values (id BIGINT PRIMARY KEY, amount BIGINT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO aggregate_values VALUES (1, 5), (2, 5), (3, 7), (4, NULL)",
        &[],
    );
    let distinct = execute(
        &mut session,
        "SELECT COUNT(DISTINCT amount), SUM(DISTINCT amount), \
         AVG(DISTINCT amount), MIN(DISTINCT amount), MAX(DISTINCT amount), \
         COUNT(DISTINCT amount) FILTER (WHERE id < 3) FROM aggregate_values",
        &[],
    );
    assert_eq!(
        rows(&distinct),
        vec![Row::new(vec![
            Value::Int64(2),
            Value::Int64(12),
            Value::Float64(6.0),
            Value::Int64(5),
            Value::Int64(7),
            Value::Int64(1),
        ])]
    );

    let empty = execute(
        &mut session,
        "SELECT COUNT(DISTINCT amount), SUM(DISTINCT amount), \
         AVG(amount), MIN(amount), MAX(amount) \
         FROM aggregate_values WHERE id < 0",
        &[],
    );
    assert_eq!(
        rows(&empty),
        vec![Row::new(vec![
            Value::Int64(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ])]
    );
}

#[test]
fn executes_select_distinct_before_offset_and_limit() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE distinct_values (id BIGINT PRIMARY KEY, bucket BIGINT, label TEXT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO distinct_values VALUES \
         (1, 1, 'b'), (2, 1, 'b'), (3, 2, 'a'), (4, 2, 'a'), (5, 2, NULL)",
        &[],
    );

    let distinct = execute(
        &mut session,
        "SELECT DISTINCT bucket, label FROM distinct_values ORDER BY label, bucket",
        &[],
    );
    assert_eq!(
        rows(&distinct),
        vec![
            Row::new(vec![Value::Int64(2), Value::Text("a".to_owned())]),
            Row::new(vec![Value::Int64(1), Value::Text("b".to_owned())]),
            Row::new(vec![Value::Int64(2), Value::Null]),
        ]
    );

    let paged = execute(
        &mut session,
        "SELECT DISTINCT label FROM distinct_values ORDER BY label OFFSET 1 LIMIT 1",
        &[],
    );
    assert_eq!(
        rows(&paged),
        vec![Row::new(vec![Value::Text("b".to_owned())])]
    );

    let in_rows = execute(
        &mut session,
        "SELECT id FROM distinct_values WHERE bucket IN ($1, 99, NULL) ORDER BY id",
        &[Value::Int64(1)],
    );
    assert_eq!(
        rows(&in_rows),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );
    let not_in_with_null = execute(
        &mut session,
        "SELECT id FROM distinct_values WHERE bucket NOT IN (1, NULL) ORDER BY id",
        &[],
    );
    assert!(rows(&not_in_with_null).is_empty());
    let projected_in = execute(
        &mut session,
        "SELECT id, label IN ('a', NULL) FROM distinct_values ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&projected_in),
        vec![
            Row::new(vec![Value::Int64(1), Value::Null]),
            Row::new(vec![Value::Int64(2), Value::Null]),
            Row::new(vec![Value::Int64(3), Value::Boolean(true)]),
            Row::new(vec![Value::Int64(4), Value::Boolean(true)]),
            Row::new(vec![Value::Int64(5), Value::Null]),
        ]
    );

    execute(
        &mut session,
        "INSERT INTO distinct_values VALUES (6, 3, 'c'), (7, 3, 'c')",
        &[],
    );
    let grouped = execute(
        &mut session,
        "SELECT DISTINCT COUNT(*) AS count FROM distinct_values \
         GROUP BY bucket ORDER BY count",
        &[],
    );
    assert_eq!(
        rows(&grouped),
        vec![
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(3)]),
        ]
    );
    let grouped_in = execute(
        &mut session,
        "SELECT bucket, COUNT(*) IN (2) FROM distinct_values \
         GROUP BY bucket ORDER BY bucket",
        &[],
    );
    assert_eq!(
        rows(&grouped_in),
        vec![
            Row::new(vec![Value::Int64(1), Value::Boolean(true)]),
            Row::new(vec![Value::Int64(2), Value::Boolean(false)]),
            Row::new(vec![Value::Int64(3), Value::Boolean(true)]),
        ]
    );
}

#[test]
fn executes_uncorrelated_apply_with_postgres_cardinality_and_null_semantics() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE apply_values (id BIGINT PRIMARY KEY, value BIGINT, marker TEXT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO apply_values VALUES (1, 10, 'a'), (2, 20, 'b'), (3, NULL, 'c')",
        &[],
    );
    execute(
        &mut session,
        "CREATE TABLE apply_lookup (id BIGINT PRIMARY KEY, value BIGINT, marker TEXT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO apply_lookup VALUES (1, 10, 'x'), (2, 20, 'y'), (3, NULL, 'z')",
        &[],
    );

    let scalar = execute(
        &mut session,
        "SELECT id, (SELECT value FROM apply_lookup WHERE id = 1) \
         FROM apply_values WHERE id IN (1, 3) ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&scalar),
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(10)]),
            Row::new(vec![Value::Int64(3), Value::Int64(10)]),
        ]
    );
    let empty_scalar = execute(
        &mut session,
        "SELECT id, (SELECT value FROM apply_lookup WHERE id = 99) \
         FROM apply_values WHERE id = 1",
        &[],
    );
    assert_eq!(
        rows(&empty_scalar),
        vec![Row::new(vec![Value::Int64(1), Value::Null])]
    );

    let exists = execute(
        &mut session,
        "SELECT id, EXISTS (SELECT id, marker FROM apply_lookup WHERE id = 2), \
         NOT EXISTS (SELECT id FROM apply_lookup WHERE id = 99) \
         FROM apply_values WHERE id = 1",
        &[],
    );
    assert_eq!(
        rows(&exists),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Boolean(true),
            Value::Boolean(true),
        ])]
    );

    let membership = execute(
        &mut session,
        "SELECT id, value IN (SELECT value FROM apply_lookup WHERE id IN (1, 3)), \
         value NOT IN (SELECT value FROM apply_lookup WHERE id IN (1, 3)) \
         FROM apply_values ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&membership),
        vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Boolean(true),
                Value::Boolean(false),
            ]),
            Row::new(vec![Value::Int64(2), Value::Null, Value::Null]),
            Row::new(vec![Value::Int64(3), Value::Null, Value::Null]),
        ]
    );

    let quantified = execute(
        &mut session,
        "SELECT id, value = ANY (SELECT value FROM apply_lookup WHERE id IN (1, 3)), \
         value = ALL (SELECT value FROM apply_lookup WHERE id IN (1, 3)), \
         value = ANY (SELECT value FROM apply_lookup WHERE id = 99), \
         value = ALL (SELECT value FROM apply_lookup WHERE id = 99) \
         FROM apply_values ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&quantified),
        vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Boolean(true),
                Value::Null,
                Value::Boolean(false),
                Value::Boolean(true),
            ]),
            Row::new(vec![
                Value::Int64(2),
                Value::Null,
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Boolean(true),
            ]),
            Row::new(vec![
                Value::Int64(3),
                Value::Null,
                Value::Null,
                Value::Boolean(false),
                Value::Boolean(true),
            ]),
        ]
    );

    let parameterized = execute(
        &mut session,
        "SELECT id, $1 IN (SELECT value FROM apply_lookup WHERE id = 2) \
         FROM apply_values WHERE id = 1",
        &[Value::Int64(20)],
    );
    assert_eq!(
        rows(&parameterized),
        vec![Row::new(vec![Value::Int64(1), Value::Boolean(true),])]
    );

    let cte_apply = execute(
        &mut session,
        "WITH lookup(value) AS (
             SELECT value FROM apply_lookup WHERE id = 2
         )
         SELECT id FROM apply_values
         WHERE value IN (SELECT value FROM lookup) ORDER BY id",
        &[],
    );
    assert_eq!(rows(&cte_apply), vec![Row::new(vec![Value::Int64(2)])]);

    let catalog = engine.catalog_snapshot().expect("catalog snapshot");
    let predicate_statement = bind(
        parse(
            "SELECT id FROM apply_values \
             WHERE EXISTS (SELECT id FROM apply_lookup)",
        )
        .expect("parse Apply predicate locks"),
        &catalog,
    )
    .expect("bind Apply predicate locks");
    assert_eq!(statement_read_predicates(&predicate_statement).len(), 2);

    let explain = execute(
        &mut session,
        "EXPLAIN SELECT id FROM apply_values \
         WHERE EXISTS (SELECT id FROM apply_lookup)",
        &[],
    );
    assert!(rows(&explain).iter().any(|row| {
        matches!(row.values.as_slice(), [Value::Text(line)] if line.contains("Exists Apply"))
    }));

    let error = session
        .execute(
            "SELECT (SELECT value FROM apply_lookup) FROM apply_values",
            &[],
        )
        .expect_err("scalar subquery returning multiple rows");
    assert_eq!(error.sql_state, "21000");
}

#[test]
fn executes_row_comparisons_and_row_subqueries_with_three_value_logic() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE row_values (id BIGINT PRIMARY KEY, key_value BIGINT, item_value BIGINT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO row_values VALUES \
         (1, 10, 100), (2, 10, 200), (3, 20, NULL), (4, 30, 300)",
        &[],
    );
    execute(
        &mut session,
        "CREATE TABLE row_lookup (id BIGINT PRIMARY KEY, key_value BIGINT, item_value BIGINT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO row_lookup VALUES (1, 10, 100), (2, 10, NULL), (3, 20, NULL)",
        &[],
    );

    let membership = execute(
        &mut session,
        "SELECT id,
                (key_value, item_value) IN (
                    SELECT key_value, item_value FROM row_lookup
                ) AS included,
                (key_value, item_value) NOT IN (
                    SELECT key_value, item_value FROM row_lookup
                ) AS excluded
         FROM row_values ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&membership),
        vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Boolean(true),
                Value::Boolean(false),
            ]),
            Row::new(vec![Value::Int64(2), Value::Null, Value::Null]),
            Row::new(vec![Value::Int64(3), Value::Null, Value::Null]),
            Row::new(vec![
                Value::Int64(4),
                Value::Boolean(false),
                Value::Boolean(true),
            ]),
        ]
    );

    let scalar = execute(
        &mut session,
        "SELECT id,
                (key_value, item_value) = (
                    SELECT key_value, item_value FROM row_lookup WHERE id = 1
                ) AS same_row,
                (key_value, item_value) = (
                    SELECT key_value, item_value FROM row_lookup WHERE id = 99
                ) AS empty_row
         FROM row_values ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&scalar),
        vec![
            Row::new(vec![Value::Int64(1), Value::Boolean(true), Value::Null]),
            Row::new(vec![Value::Int64(2), Value::Boolean(false), Value::Null,]),
            Row::new(vec![Value::Int64(3), Value::Boolean(false), Value::Null,]),
            Row::new(vec![Value::Int64(4), Value::Boolean(false), Value::Null,]),
        ]
    );

    let direct = execute(
        &mut session,
        "SELECT id,
                (key_value, item_value) = (10, 100) AS exact_row,
                (key_value, item_value) IN ((10, 100), (20, NULL)) AS listed_row
         FROM row_values ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&direct),
        vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Boolean(true),
                Value::Boolean(true),
            ]),
            Row::new(vec![
                Value::Int64(2),
                Value::Boolean(false),
                Value::Boolean(false),
            ]),
            Row::new(vec![Value::Int64(3), Value::Boolean(false), Value::Null,]),
            Row::new(vec![
                Value::Int64(4),
                Value::Boolean(false),
                Value::Boolean(false),
            ]),
        ]
    );

    let correlated = execute(
        &mut session,
        "SELECT outer_values.id,
                (outer_values.key_value, outer_values.item_value) = (
                    SELECT inner_values.key_value, inner_values.item_value
                    FROM row_lookup inner_values
                    WHERE inner_values.id = outer_values.id
                ) AS same_row
         FROM row_values outer_values ORDER BY outer_values.id",
        &[],
    );
    assert_eq!(
        rows(&correlated),
        vec![
            Row::new(vec![Value::Int64(1), Value::Boolean(true)]),
            Row::new(vec![Value::Int64(2), Value::Null]),
            Row::new(vec![Value::Int64(3), Value::Null]),
            Row::new(vec![Value::Int64(4), Value::Null]),
        ]
    );

    let parameterized = execute(
        &mut session,
        "SELECT ($1, $2) IN (
             SELECT key_value, item_value FROM row_lookup
         ) FROM row_values WHERE id = 1",
        &[Value::Int64(10), Value::Int64(100)],
    );
    assert_eq!(
        rows(&parameterized),
        vec![Row::new(vec![Value::Boolean(true)])]
    );

    let empty_quantifiers = execute(
        &mut session,
        "SELECT (key_value, item_value) = ANY (
                     SELECT key_value, item_value FROM row_lookup WHERE id = 99
                 ),
                (key_value, item_value) = ALL (
                     SELECT key_value, item_value FROM row_lookup WHERE id = 99
                 )
         FROM row_values WHERE id = 1",
        &[],
    );
    assert_eq!(
        rows(&empty_quantifiers),
        vec![Row::new(vec![Value::Boolean(false), Value::Boolean(true),])]
    );

    let null_witness = execute(
        &mut session,
        "SELECT (key_value, item_value) = ANY (
                     SELECT key_value, item_value FROM row_lookup
                 ),
                (key_value, item_value) <> ALL (
                     SELECT key_value, item_value FROM row_lookup
                 )
         FROM row_values WHERE id = 2",
        &[],
    );
    assert_eq!(
        rows(&null_witness),
        vec![Row::new(vec![Value::Null, Value::Null])]
    );

    let cardinality = session
        .execute(
            "SELECT (key_value, item_value) = (
                 SELECT key_value, item_value FROM row_lookup WHERE key_value = 10
             ) FROM row_values WHERE id = 1",
            &[],
        )
        .expect_err("row scalar subquery cardinality");
    assert_eq!(cardinality.sql_state, "21000");

    execute(
        &mut session,
        "CREATE TABLE row_narrow (
             id BIGINT PRIMARY KEY,
             key_value INTEGER,
             item_value SMALLINT
         )",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO row_narrow VALUES (1, $1, $2)",
        &[Value::Int32(10), Value::Int16(100)],
    );
    let promoted = execute(
        &mut session,
        "SELECT (key_value, item_value) = (
                     SELECT key_value, item_value FROM row_narrow WHERE id = 1
                 ),
                (key_value, item_value) IN (
                     SELECT key_value, item_value FROM row_narrow
                 )
         FROM row_values WHERE id = 1",
        &[],
    );
    assert_eq!(
        rows(&promoted),
        vec![Row::new(vec![Value::Boolean(true), Value::Boolean(true),])]
    );

    let width = session
        .execute(
            "SELECT id FROM row_values WHERE (key_value, item_value) IN (
                 SELECT key_value FROM row_lookup
             )",
            &[],
        )
        .expect_err("row width mismatch");
    assert_eq!(width.sql_state, "42601");
}
