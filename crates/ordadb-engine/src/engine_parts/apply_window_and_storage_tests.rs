
#[test]
fn executes_correlated_apply_with_parameter_frames_and_per_row_results() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE correlated_values (id BIGINT PRIMARY KEY, value BIGINT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO correlated_values VALUES (1, 10), (2, 20), (3, NULL), (4, 40)",
        &[],
    );
    execute(
        &mut session,
        "CREATE TABLE correlated_lookup (id BIGINT PRIMARY KEY, value BIGINT, marker TEXT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO correlated_lookup VALUES (1, 10, 'x'), (2, 20, 'y'), (4, NULL, 'z')",
        &[],
    );

    let scalar = execute(
        &mut session,
        "SELECT outer_values.id, (
             SELECT inner_values.marker FROM correlated_lookup inner_values
             WHERE inner_values.id = outer_values.id
         )
         FROM correlated_values outer_values ORDER BY outer_values.id",
        &[],
    );
    assert_eq!(
        rows(&scalar),
        vec![
            Row::new(vec![Value::Int64(1), Value::Text("x".to_owned())]),
            Row::new(vec![Value::Int64(2), Value::Text("y".to_owned())]),
            Row::new(vec![Value::Int64(3), Value::Null]),
            Row::new(vec![Value::Int64(4), Value::Text("z".to_owned())]),
        ]
    );

    let exists = execute(
        &mut session,
        "SELECT outer_values.id FROM correlated_values outer_values
         WHERE EXISTS (
             SELECT inner_values.id FROM correlated_lookup inner_values
             WHERE inner_values.id = outer_values.id
         ) ORDER BY outer_values.id",
        &[],
    );
    assert_eq!(
        rows(&exists),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(4)]),
        ]
    );

    let membership = execute(
        &mut session,
        "SELECT outer_values.id, outer_values.value IN (
             SELECT inner_values.value FROM correlated_lookup inner_values
             WHERE inner_values.id <= outer_values.id
         ) FROM correlated_values outer_values ORDER BY outer_values.id",
        &[],
    );
    assert_eq!(
        rows(&membership),
        vec![
            Row::new(vec![Value::Int64(1), Value::Boolean(true)]),
            Row::new(vec![Value::Int64(2), Value::Boolean(true)]),
            Row::new(vec![Value::Int64(3), Value::Null]),
            Row::new(vec![Value::Int64(4), Value::Null]),
        ]
    );

    let quantified = execute(
        &mut session,
        "SELECT outer_values.id,
                outer_values.value = ANY (
                    SELECT inner_values.value FROM correlated_lookup inner_values
                    WHERE inner_values.id <= outer_values.id
                ),
                outer_values.value = ALL (
                    SELECT inner_values.value FROM correlated_lookup inner_values
                    WHERE inner_values.id <= outer_values.id
                )
         FROM correlated_values outer_values ORDER BY outer_values.id",
        &[],
    );
    assert_eq!(
        rows(&quantified),
        vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Boolean(true),
                Value::Boolean(true),
            ]),
            Row::new(vec![
                Value::Int64(2),
                Value::Boolean(true),
                Value::Boolean(false),
            ]),
            Row::new(vec![Value::Int64(3), Value::Null, Value::Null]),
            Row::new(vec![Value::Int64(4), Value::Null, Value::Boolean(false)]),
        ]
    );

    let shadowed = execute(
        &mut session,
        "SELECT outer_values.id FROM correlated_values outer_values
         WHERE EXISTS (
             SELECT id FROM correlated_lookup
             WHERE id = id AND outer_values.id = 3
         )",
        &[],
    );
    assert_eq!(rows(&shadowed), vec![Row::new(vec![Value::Int64(3)])]);

    let nested = execute(
        &mut session,
        "SELECT outer_values.id FROM correlated_values outer_values
         WHERE EXISTS (
             SELECT middle_values.id FROM correlated_lookup middle_values
             WHERE EXISTS (
                 SELECT inner_values.id FROM correlated_lookup inner_values
                 WHERE inner_values.id = middle_values.id
                   AND middle_values.id = outer_values.id
             )
         ) ORDER BY outer_values.id",
        &[],
    );
    assert_eq!(
        rows(&nested),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(4)]),
        ]
    );

    let parameterized = execute(
        &mut session,
        "SELECT outer_values.id FROM correlated_values outer_values
         WHERE EXISTS (
             SELECT inner_values.id FROM correlated_lookup inner_values
             WHERE inner_values.id = outer_values.id AND inner_values.marker = $1
         )",
        &[Value::Text("y".to_owned())],
    );
    assert_eq!(rows(&parameterized), vec![Row::new(vec![Value::Int64(2)])]);

    let error = session
        .execute(
            "SELECT (
                 SELECT inner_values.value FROM correlated_lookup inner_values
                 WHERE inner_values.id <= outer_values.id
             ) FROM correlated_values outer_values WHERE outer_values.id = 2",
            &[],
        )
        .expect_err("correlated scalar returning multiple rows");
    assert_eq!(error.sql_state, "21000");

    let error = session
        .execute(
            "SELECT outer_values.id FROM correlated_values outer_values
             WHERE EXISTS (
                 SELECT inner_values.id FROM correlated_lookup inner_values
                 WHERE inner_values.id = missing_outer.id
             )",
            &[],
        )
        .expect_err("unknown outer alias");
    assert_eq!(error.sql_state, "42703");
}

#[test]
fn executes_streaming_lateral_joins_with_left_null_extension() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE lateral_values (id BIGINT PRIMARY KEY, ceiling BIGINT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO lateral_values VALUES (1, 1), (2, 2), (3, 0)",
        &[],
    );
    execute(
        &mut session,
        "CREATE TABLE lateral_lookup (id BIGINT PRIMARY KEY, marker TEXT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO lateral_lookup VALUES (1, 'a'), (2, 'b')",
        &[],
    );

    let inner = execute(
        &mut session,
        "SELECT outer_values.id, matched.renamed_marker
         FROM lateral_values outer_values
         INNER JOIN LATERAL (
             SELECT lookup.marker FROM lateral_lookup lookup
             WHERE lookup.id <= outer_values.ceiling
         ) AS matched(renamed_marker) ON TRUE
         ORDER BY outer_values.id, matched.renamed_marker",
        &[],
    );
    assert_eq!(
        rows(&inner),
        vec![
            Row::new(vec![Value::Int64(1), Value::Text("a".to_owned())]),
            Row::new(vec![Value::Int64(2), Value::Text("a".to_owned())]),
            Row::new(vec![Value::Int64(2), Value::Text("b".to_owned())]),
        ]
    );

    let left = execute(
        &mut session,
        "SELECT outer_values.id, matched.marker
         FROM lateral_values outer_values
         LEFT JOIN LATERAL (
             SELECT lookup.marker FROM lateral_lookup lookup
             WHERE lookup.id = outer_values.id
         ) AS matched ON TRUE
         ORDER BY outer_values.id",
        &[],
    );
    assert_eq!(
        rows(&left),
        vec![
            Row::new(vec![Value::Int64(1), Value::Text("a".to_owned())]),
            Row::new(vec![Value::Int64(2), Value::Text("b".to_owned())]),
            Row::new(vec![Value::Int64(3), Value::Null]),
        ]
    );

    let parameterized = execute(
        &mut session,
        "SELECT outer_values.id
         FROM lateral_values outer_values
         INNER JOIN LATERAL (
             SELECT lookup.marker FROM lateral_lookup lookup
             WHERE lookup.id = outer_values.id AND lookup.marker = $1
         ) AS matched ON TRUE",
        &[Value::Text("b".to_owned())],
    );
    assert_eq!(rows(&parameterized), vec![Row::new(vec![Value::Int64(2)])]);

    let multiple_left_inputs = execute(
        &mut session,
        "SELECT outer_values.id, derived.marker
         FROM lateral_values outer_values
         INNER JOIN lateral_lookup first_match ON first_match.id = outer_values.id
         INNER JOIN LATERAL (
             SELECT second_match.marker FROM lateral_lookup second_match
             WHERE second_match.id = first_match.id
               AND second_match.id = outer_values.id
         ) AS derived ON TRUE
         ORDER BY outer_values.id",
        &[],
    );
    assert_eq!(
        rows(&multiple_left_inputs),
        vec![
            Row::new(vec![Value::Int64(1), Value::Text("a".to_owned())]),
            Row::new(vec![Value::Int64(2), Value::Text("b".to_owned())]),
        ]
    );

    let catalog = engine.catalog_snapshot().expect("catalog snapshot");
    let statement = bind(
        parse(
            "SELECT outer_values.id FROM lateral_values outer_values
             INNER JOIN LATERAL (
                 SELECT lookup.id FROM lateral_lookup lookup
                 WHERE lookup.id = outer_values.id
             ) AS matched ON TRUE",
        )
        .expect("parse LATERAL predicate locks"),
        &catalog,
    )
    .expect("bind LATERAL predicate locks");
    assert_eq!(statement_read_predicates(&statement).len(), 2);

    let explain = execute(
        &mut session,
        "EXPLAIN SELECT outer_values.id FROM lateral_values outer_values
         INNER JOIN LATERAL (
             SELECT lookup.id FROM lateral_lookup lookup
             WHERE lookup.id = outer_values.id
         ) AS matched ON TRUE",
        &[],
    );
    assert!(rows(&explain).iter().any(|row| {
        matches!(row.values.as_slice(), [Value::Text(line)] if line.contains("Lateral Subquery Scan"))
    }));

    let cancellation = Arc::new(AtomicBool::new(false));
    let mut stream = session
        .execute_stream_with_cancellation(
            "SELECT outer_values.id, matched.marker
             FROM lateral_values outer_values
             INNER JOIN LATERAL (
                 SELECT lookup.marker FROM lateral_lookup lookup
                 WHERE lookup.id <= outer_values.id
             ) AS matched ON TRUE",
            &[],
            Arc::clone(&cancellation),
        )
        .expect("LATERAL cancellable stream");
    assert!(matches!(
        stream.next().expect("schema").expect("schema event"),
        QueryEvent::Schema(_)
    ));
    cancellation.store(true, Ordering::Release);
    assert_eq!(
        stream
            .next()
            .expect("cancellation error")
            .expect_err("cancelled LATERAL stream")
            .sql_state,
        "57014"
    );
    assert!(stream.next().is_none());

    let error = session
        .execute(
            "SELECT outer_values.id FROM lateral_values outer_values
             INNER JOIN (
                 SELECT lookup.id FROM lateral_lookup lookup
                 WHERE lookup.id = outer_values.id
             ) AS matched ON TRUE",
            &[],
        )
        .expect_err("non-LATERAL derived table cannot see the left input");
    assert_eq!(error.sql_state, "42703");
}

#[test]
fn executes_partitioned_ranking_windows_with_apply_and_outer_order() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE window_values (id BIGINT PRIMARY KEY, group_name TEXT, score BIGINT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO window_values VALUES \
         (1, 'a', 20), (2, 'a', 10), (3, 'a', 20), (4, 'b', 30)",
        &[],
    );

    let ranked = execute(
        &mut session,
        "SELECT id, \
                (SELECT lookup.score FROM window_values lookup \
                 WHERE lookup.id = window_values.id) AS copied_score, \
                ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY score DESC) AS row_no, \
                RANK() OVER (PARTITION BY group_name ORDER BY score DESC) AS rank_no, \
                DENSE_RANK() OVER (PARTITION BY group_name ORDER BY score DESC) AS dense_no \
         FROM window_values ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&ranked),
        vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Int64(20),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
            ]),
            Row::new(vec![
                Value::Int64(2),
                Value::Int64(10),
                Value::Int64(3),
                Value::Int64(3),
                Value::Int64(2),
            ]),
            Row::new(vec![
                Value::Int64(3),
                Value::Int64(20),
                Value::Int64(2),
                Value::Int64(1),
                Value::Int64(1),
            ]),
            Row::new(vec![
                Value::Int64(4),
                Value::Int64(30),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
            ]),
        ]
    );

    let named = execute(
        &mut session,
        "SELECT id, RANK() OVER ranked AS rank_no FROM window_values \
         WINDOW ranked AS (PARTITION BY group_name ORDER BY score DESC) \
         ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&named),
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(1)]),
            Row::new(vec![Value::Int64(2), Value::Int64(3)]),
            Row::new(vec![Value::Int64(3), Value::Int64(1)]),
            Row::new(vec![Value::Int64(4), Value::Int64(1)]),
        ]
    );

    let framed = execute(
        &mut session,
        "SELECT id, RANK() OVER (
             PARTITION BY group_name ORDER BY score DESC
             ROWS BETWEEN $1 PRECEDING AND $2 FOLLOWING
         ) AS rank_no FROM window_values ORDER BY id",
        &[Value::Int64(1), Value::Int64(1)],
    );
    assert_eq!(rows(&framed), rows(&named));

    let negative_frame = session
        .execute(
            "SELECT RANK() OVER (ORDER BY score ROWS $1 PRECEDING) FROM window_values",
            &[Value::Int64(-1)],
        )
        .expect_err("negative frame offset");
    assert_eq!(negative_frame.sql_state, "22013");

    let reversed_frame = session
        .execute(
            "SELECT RANK() OVER (
                 ORDER BY score ROWS BETWEEN $1 PRECEDING AND $2 PRECEDING
             ) FROM window_values",
            &[Value::Int64(1), Value::Int64(2)],
        )
        .expect_err("frame start after end");
    assert_eq!(reversed_frame.sql_state, "42P20");

    let values = execute(
        &mut session,
        "SELECT id,
                LAG(score) OVER grouped AS lag_score,
                LEAD(score, 1, -1) OVER grouped AS lead_score,
                FIRST_VALUE(score) OVER grouped AS first_score,
                LAST_VALUE(score) OVER (
                    grouped ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS last_score,
                NTH_VALUE(score, 2) OVER (
                    grouped ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS second_score,
                COUNT(*) OVER (PARTITION BY group_name) AS group_count,
                SUM(score) OVER (
                    grouped ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_score,
                AVG(score) OVER (PARTITION BY group_name) AS average_score
         FROM window_values
         WINDOW grouped AS (PARTITION BY group_name ORDER BY id)
         ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&values),
        vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Null,
                Value::Int64(10),
                Value::Int64(20),
                Value::Int64(20),
                Value::Int64(10),
                Value::Int64(3),
                Value::Int64(20),
                Value::Float64(50.0 / 3.0),
            ]),
            Row::new(vec![
                Value::Int64(2),
                Value::Int64(20),
                Value::Int64(20),
                Value::Int64(20),
                Value::Int64(10),
                Value::Int64(10),
                Value::Int64(3),
                Value::Int64(30),
                Value::Float64(50.0 / 3.0),
            ]),
            Row::new(vec![
                Value::Int64(3),
                Value::Int64(10),
                Value::Int64(-1),
                Value::Int64(20),
                Value::Int64(20),
                Value::Int64(10),
                Value::Int64(3),
                Value::Int64(50),
                Value::Float64(50.0 / 3.0),
            ]),
            Row::new(vec![
                Value::Int64(4),
                Value::Null,
                Value::Int64(-1),
                Value::Int64(30),
                Value::Int64(30),
                Value::Null,
                Value::Int64(1),
                Value::Int64(30),
                Value::Float64(30.0),
            ]),
        ]
    );

    let range = execute(
        &mut session,
        "SELECT id, SUM(score) OVER (
             ORDER BY score RANGE BETWEEN 5 PRECEDING AND CURRENT ROW
         ) AS nearby_score FROM window_values ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&range),
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(40)]),
            Row::new(vec![Value::Int64(2), Value::Int64(10)]),
            Row::new(vec![Value::Int64(3), Value::Int64(40)]),
            Row::new(vec![Value::Int64(4), Value::Int64(30)]),
        ]
    );

    let sliding_rows = execute(
        &mut session,
        "SELECT id, SUM(score) OVER (
             PARTITION BY group_name ORDER BY id
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
         ) AS sliding_score FROM window_values ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&sliding_rows),
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(20)]),
            Row::new(vec![Value::Int64(2), Value::Int64(30)]),
            Row::new(vec![Value::Int64(3), Value::Int64(30)]),
            Row::new(vec![Value::Int64(4), Value::Int64(30)]),
        ]
    );

    let default_range = execute(
        &mut session,
        "SELECT id, SUM(score) OVER (ORDER BY score) AS running_peers \
         FROM window_values ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&default_range),
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(50)]),
            Row::new(vec![Value::Int64(2), Value::Int64(10)]),
            Row::new(vec![Value::Int64(3), Value::Int64(50)]),
            Row::new(vec![Value::Int64(4), Value::Int64(80)]),
        ]
    );

    let signed_offsets = execute(
        &mut session,
        "SELECT id,
                LAG(score, -1) OVER grouped AS next_score,
                LEAD(score, NULL, 999) OVER grouped AS null_offset
         FROM window_values
         WINDOW grouped AS (PARTITION BY group_name ORDER BY id)
         ORDER BY id",
        &[],
    );
    assert_eq!(
        rows(&signed_offsets),
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(10), Value::Null]),
            Row::new(vec![Value::Int64(2), Value::Int64(20), Value::Null]),
            Row::new(vec![Value::Int64(3), Value::Null, Value::Null]),
            Row::new(vec![Value::Int64(4), Value::Null, Value::Null]),
        ]
    );

    let grouped_windows = execute(
        &mut session,
        "SELECT group_name,
                SUM(score) AS total_score,
                RANK() OVER (ORDER BY SUM(score) DESC) AS total_rank,
                SUM(SUM(score)) OVER (
                    ORDER BY group_name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_groups
         FROM window_values
         GROUP BY group_name
         ORDER BY group_name",
        &[],
    );
    assert_eq!(
        rows(&grouped_windows),
        vec![
            Row::new(vec![
                Value::Text("a".to_owned()),
                Value::Int64(50),
                Value::Int64(1),
                Value::Int64(50),
            ]),
            Row::new(vec![
                Value::Text("b".to_owned()),
                Value::Int64(30),
                Value::Int64(2),
                Value::Int64(80),
            ]),
        ]
    );

    let ordered = execute(
        &mut session,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS row_no \
         FROM window_values ORDER BY row_no DESC",
        &[],
    );
    assert_eq!(
        rows(&ordered),
        vec![
            Row::new(vec![Value::Int64(2), Value::Int64(4)]),
            Row::new(vec![Value::Int64(3), Value::Int64(3)]),
            Row::new(vec![Value::Int64(1), Value::Int64(2)]),
            Row::new(vec![Value::Int64(4), Value::Int64(1)]),
        ]
    );

    let explain = execute(
        &mut session,
        "EXPLAIN SELECT ROW_NUMBER() OVER (ORDER BY score) FROM window_values",
        &[],
    );
    assert!(rows(&explain).iter().any(|row| {
        matches!(row.values.as_slice(), [Value::Text(line)] if line.contains("WindowAgg"))
    }));

    let mut stream = session
        .execute_stream(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY score) FROM window_values",
            &[],
        )
        .expect("window stream");
    for event in stream.by_ref() {
        event.expect("window stream event");
    }
    assert!(
        stream
            .execution_memory_peak_bytes()
            .is_some_and(|peak| peak > 0)
    );
}

#[test]
fn persists_covering_indexes_statistics_and_explains_real_access_paths() {
    let directory = tempdir().expect("tempdir");
    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE metrics (id BIGINT PRIMARY KEY, bucket BIGINT, score BIGINT, payload TEXT)",
            &[],
        );
        let values = (0..512)
            .map(|value| format!("({value}, {}, {value}, 'p{value}')", value % 8))
            .collect::<Vec<_>>()
            .join(", ");
        execute(
            &mut session,
            &format!("INSERT INTO metrics VALUES {values}"),
            &[],
        );
        let duplicate = session
            .execute(
                "CREATE UNIQUE INDEX metrics_bucket_unique ON metrics (bucket)",
                &[],
            )
            .expect_err("duplicate unique build");
        assert_eq!(duplicate.sql_state, "23505");
        execute(
            &mut session,
            "CREATE INDEX metrics_score_idx ON metrics (score) INCLUDE (payload)",
            &[],
        );
    }

    let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut session = engine.connect().expect("connect");
    let explain = execute(
        &mut session,
        "EXPLAIN SELECT payload FROM metrics WHERE score = 511",
        &[],
    );
    let plan = rows(&explain)
        .into_iter()
        .filter_map(|row| match row.values.as_slice() {
            [Value::Text(line)] => Some(line.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        plan.iter().any(|line| line.contains("Index Scan")),
        "{plan:?}"
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT payload FROM metrics WHERE score = 511",
            &[],
        )),
        vec![Row::new(vec![Value::Text("p511".into())])]
    );
}

#[test]
fn fallible_stream_preserves_event_order_and_legacy_adapter() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE stream_items (id BIGINT PRIMARY KEY)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO stream_items VALUES (1), (2), (3)",
        &[],
    );

    let events = session
        .execute_stream("SELECT id FROM stream_items ORDER BY id", &[])
        .expect("stream")
        .collect::<Result<Vec<_>>>()
        .expect("fallible events");
    assert!(matches!(events.first(), Some(QueryEvent::Schema(_))));
    assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, QueryEvent::Complete(_)))
            .count(),
        1
    );
    assert_eq!(rows(&events).len(), 3);
    assert_eq!(
        session
            .execute("SELECT id FROM stream_items", &[])
            .expect("legacy")
            .filter(|event| matches!(event, QueryEvent::Complete(_)))
            .count(),
        1
    );
}

#[test]
fn storage_backed_stream_holds_one_generation_until_exhaustion() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE lazy_items (id BIGINT PRIMARY KEY)",
        &[],
    );
    execute(&mut session, "INSERT INTO lazy_items VALUES (1), (2)", &[]);

    let snapshot = session
        .execute_stream("SELECT id FROM lazy_items ORDER BY id", &[])
        .expect("lazy stream");
    assert_eq!(
        engine
            .storage_access
            .active_readers()
            .expect("active readers"),
        1
    );

    let writer_engine = engine.clone();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
    let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel(0);
    let writer = std::thread::spawn(move || {
        let mut writer_session = writer_engine.connect().expect("writer connect");
        started_tx.send(()).expect("signal writer start");
        let result = writer_session
            .execute("INSERT INTO lazy_items VALUES (3)", &[])
            .map(|_| ());
        finished_tx.send(result).expect("send writer result");
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("writer started");
    let waiting_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while engine
        .storage_access
        .waiting_writers()
        .expect("waiting writers")
        == 0
    {
        assert!(
            std::time::Instant::now() < waiting_deadline,
            "writer did not reach the storage gate"
        );
        std::thread::yield_now();
    }
    assert!(matches!(
        finished_rx.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));

    let events = snapshot
        .collect::<Result<Vec<_>>>()
        .expect("snapshot events");
    assert_eq!(
        rows(&events),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );
    assert_eq!(
        engine
            .storage_access
            .active_readers()
            .expect("active readers"),
        0
    );
    finished_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("writer finished after stream exhaustion")
        .expect("writer commit");
    writer.join().expect("writer thread");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id FROM lazy_items ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(3)]),
        ]
    );
}

#[test]
fn storage_access_gate_prefers_a_waiting_writer_over_new_readers() {
    let gate = Arc::new(StorageAccessGate::default());
    let first_reader = gate.acquire_read().expect("first reader");
    let writer_gate = Arc::clone(&gate);
    let (writer_acquired_tx, writer_acquired_rx) = std::sync::mpsc::sync_channel(0);
    let (release_writer_tx, release_writer_rx) = std::sync::mpsc::sync_channel(0);
    let writer = std::thread::spawn(move || {
        let lease = writer_gate.acquire_write().expect("writer lease");
        writer_acquired_tx.send(()).expect("writer acquired");
        release_writer_rx.recv().expect("release writer");
        drop(lease);
    });

    let waiting_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while gate.waiting_writers().expect("waiting writers") == 0 {
        assert!(
            std::time::Instant::now() < waiting_deadline,
            "writer did not reach the storage gate"
        );
        std::thread::yield_now();
    }

    let second_reader_gate = Arc::clone(&gate);
    let (reader_acquired_tx, reader_acquired_rx) = std::sync::mpsc::sync_channel(0);
    let second_reader = std::thread::spawn(move || {
        let lease = second_reader_gate.acquire_read().expect("second reader");
        reader_acquired_tx.send(()).expect("reader acquired");
        drop(lease);
    });

    drop(first_reader);
    writer_acquired_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("writer wins after readers drain");
    assert!(matches!(
        reader_acquired_rx.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    release_writer_tx.send(()).expect("release writer");
    reader_acquired_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("reader proceeds after writer");
    writer.join().expect("writer thread");
    second_reader.join().expect("reader thread");
}
