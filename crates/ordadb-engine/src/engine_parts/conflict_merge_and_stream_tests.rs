
#[test]
fn executes_on_conflict_atomically_with_returning_and_cardinality_checks() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE conflict_items (
            id BIGINT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            label TEXT NOT NULL
        )",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO conflict_items VALUES (1, 'one@example.test', 'original')",
        &[],
    );

    let skipped = execute(
        &mut session,
        "INSERT INTO conflict_items VALUES (1, 'ignored@example.test', 'ignored') \
         ON CONFLICT DO NOTHING RETURNING id",
        &[],
    );
    assert!(rows(&skipped).is_empty());
    assert!(matches!(
        skipped.last(),
        Some(QueryEvent::Complete(CommandComplete {
            tag,
            rows_affected: 0
        })) if tag == "INSERT 0 0"
    ));

    let updated = execute(
        &mut session,
        "INSERT INTO conflict_items VALUES (1, 'updated@example.test', 'updated') \
         ON CONFLICT (id) DO UPDATE \
         SET email = excluded.email, label = excluded.label \
         WHERE conflict_items.label <> excluded.label \
         RETURNING id, email, label",
        &[],
    );
    assert_eq!(
        rows(&updated),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("updated@example.test".into()),
            Value::Text("updated".into()),
        ])]
    );
    assert!(matches!(
        updated.last(),
        Some(QueryEvent::Complete(CommandComplete {
            tag,
            rows_affected: 1
        })) if tag == "INSERT 0 1"
    ));

    let filtered = execute(
        &mut session,
        "INSERT INTO conflict_items VALUES (1, 'different@example.test', 'updated') \
         ON CONFLICT (id) DO UPDATE SET email = excluded.email \
         WHERE conflict_items.label <> excluded.label RETURNING id",
        &[],
    );
    assert!(rows(&filtered).is_empty());
    assert!(matches!(
        filtered.last(),
        Some(QueryEvent::Complete(CommandComplete {
            tag,
            rows_affected: 0
        })) if tag == "INSERT 0 0"
    ));

    let non_arbiter = session
        .execute(
            "INSERT INTO conflict_items VALUES (2, 'updated@example.test', 'duplicate') \
             ON CONFLICT (id) DO NOTHING",
            &[],
        )
        .expect_err("non-arbiter unique conflict");
    assert_eq!(non_arbiter.sql_state, "23505");

    let cardinality = session
        .execute(
            "INSERT INTO conflict_items VALUES \
             (1, 'updated@example.test', 'first'), \
             (1, 'updated@example.test', 'second') \
             ON CONFLICT (id) DO UPDATE SET label = excluded.label",
            &[],
        )
        .expect_err("same target row affected twice");
    assert_eq!(cardinality.sql_state, "21000");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id, email, label FROM conflict_items",
            &[],
        )),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("updated@example.test".into()),
            Value::Text("updated".into()),
        ])]
    );

    execute(&mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
    execute(&mut session, "SAVEPOINT before_upsert", &[]);
    execute(
        &mut session,
        "INSERT INTO conflict_items VALUES (1, 'updated@example.test', 'temporary') \
         ON CONFLICT (id) DO UPDATE SET label = excluded.label",
        &[],
    );
    execute(&mut session, "ROLLBACK TO SAVEPOINT before_upsert", &[]);
    execute(&mut session, "COMMIT", &[]);
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT label FROM conflict_items WHERE id = 1",
            &[],
        )),
        vec![Row::new(vec![Value::Text("updated".into())])]
    );
}

#[test]
fn recursive_cte_row_limit_accepts_the_exact_boundary() {
    ensure_recursive_cte_row_limit(MAX_RECURSIVE_CTE_ROWS).expect("exact row limit");
    let error = ensure_recursive_cte_row_limit(MAX_RECURSIVE_CTE_ROWS + 1)
        .expect_err("row above recursive CTE limit");
    assert_eq!(error.sql_state, "54000");
}

#[test]
fn executes_postgres_set_operations_with_duplicates_nulls_and_limits() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(&mut session, "CREATE TABLE set_left (value BIGINT)", &[]);
    execute(&mut session, "CREATE TABLE set_right (value INTEGER)", &[]);
    execute(
        &mut session,
        "INSERT INTO set_left VALUES (1), (1), (2), (3), (NULL)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO set_right VALUES (1), (2), (2), (4), (NULL)",
        &[],
    );

    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT value AS item FROM set_left
             UNION SELECT value FROM set_right
             ORDER BY item NULLS FIRST",
            &[],
        )),
        vec![
            Row::new(vec![Value::Null]),
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(3)]),
            Row::new(vec![Value::Int64(4)]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT value AS item FROM set_left
             UNION ALL SELECT value FROM set_right
             ORDER BY item NULLS FIRST OFFSET $1 LIMIT $2",
            &[Value::Int64(2), Value::Int64(4)],
        )),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT value FROM set_left
             INTERSECT SELECT value FROM set_right
             ORDER BY value NULLS FIRST",
            &[],
        )),
        vec![
            Row::new(vec![Value::Null]),
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT value FROM set_left
             EXCEPT ALL SELECT value FROM set_right
             ORDER BY value",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(3)]),
        ]
    );

    assert_eq!(
        rows(&execute(
            &mut session,
            "WITH combined(item) AS (
                 SELECT value FROM set_left
                 UNION ALL SELECT value FROM set_right
             ), filtered AS (
                 SELECT item FROM combined WHERE item >= 2
             )
             SELECT item FROM filtered ORDER BY item OFFSET 1 LIMIT 3",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(3)]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "WITH RECURSIVE numbers(value) AS (
                 SELECT value FROM set_left WHERE value = 3
                 UNION ALL
                 SELECT value - 1 FROM numbers WHERE value > 1
             )
             SELECT value FROM numbers ORDER BY value",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(3)]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "WITH RECURSIVE stable(value) AS (
                 SELECT value FROM set_left WHERE value = 3
                 UNION
                 SELECT value FROM stable
             )
             SELECT value FROM stable",
            &[],
        )),
        vec![Row::new(vec![Value::Int64(3)])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT value + 2 * 3 FROM set_left WHERE value = 1 LIMIT 1",
            &[],
        )),
        vec![Row::new(vec![Value::Int64(7)])]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT value * 2 AS doubled FROM set_left
             WHERE value <= 3 ORDER BY doubled DESC",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(6)]),
            Row::new(vec![Value::Int64(4)]),
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT value FROM set_left
             WHERE value <= 3 ORDER BY value + 1 DESC, 1 ASC",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(3)]),
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(1)]),
        ]
    );
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT value, COUNT(*) AS total FROM set_left
             GROUP BY value ORDER BY total DESC, 1 ASC",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(2)]),
            Row::new(vec![Value::Int64(2), Value::Int64(1)]),
            Row::new(vec![Value::Int64(3), Value::Int64(1)]),
            Row::new(vec![Value::Null, Value::Int64(1)]),
        ]
    );
    let division = session
        .execute("SELECT value / 0 FROM set_left LIMIT 1", &[])
        .expect_err("division by zero");
    assert_eq!(division.sql_state, "22012");
    execute(
        &mut session,
        "INSERT INTO set_left VALUES (9223372036854775807)",
        &[],
    );
    let overflow = session
        .execute(
            "SELECT value + 1 FROM set_left WHERE value = 9223372036854775807",
            &[],
        )
        .expect_err("integer overflow");
    assert_eq!(overflow.sql_state, "22003");
}

#[test]
fn executes_merge_as_one_atomic_ordered_candidate() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE merge_target (
            id BIGINT PRIMARY KEY,
            value TEXT UNIQUE NOT NULL
        )",
        &[],
    );
    execute(
        &mut session,
        "CREATE TABLE merge_source (
            id BIGINT NOT NULL,
            value TEXT NOT NULL,
            action TEXT NOT NULL
        )",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO merge_target VALUES
            (1, 'old-one'), (2, 'old-two'), (4, 'stable')",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO merge_source VALUES
            (1, 'new-one', 'update'),
            (2, 'ignored', 'delete'),
            (3, 'new-three', 'insert'),
            (4, 'ignored', 'skip')",
        &[],
    );

    let events = execute(
        &mut session,
        "MERGE INTO merge_target AS target
         USING merge_source AS source ON target.id = source.id
         WHEN MATCHED AND source.action = 'delete' THEN DELETE
         WHEN MATCHED AND source.action = 'update' THEN
             UPDATE SET value = source.value
         WHEN NOT MATCHED THEN INSERT (id, value)
             VALUES (source.id, source.value)
         RETURNING id, value",
        &[],
    );
    assert_eq!(
        rows(&events),
        vec![
            Row::new(vec![Value::Int64(1), Value::Text("new-one".into())]),
            Row::new(vec![Value::Int64(2), Value::Text("old-two".into())]),
            Row::new(vec![Value::Int64(3), Value::Text("new-three".into())]),
        ]
    );
    assert!(matches!(
        events.last(),
        Some(QueryEvent::Complete(CommandComplete {
            tag,
            rows_affected: 3
        })) if tag == "MERGE 3"
    ));
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id, value FROM merge_target ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1), Value::Text("new-one".into())]),
            Row::new(vec![Value::Int64(3), Value::Text("new-three".into())]),
            Row::new(vec![Value::Int64(4), Value::Text("stable".into())]),
        ]
    );

    let do_nothing = execute(
        &mut session,
        "MERGE INTO merge_target AS target
         USING merge_source AS source ON target.id = source.id
         WHEN MATCHED THEN DO NOTHING
         WHEN NOT MATCHED THEN DO NOTHING
         RETURNING id, value",
        &[],
    );
    assert!(rows(&do_nothing).is_empty());
    assert!(matches!(
        do_nothing.last(),
        Some(QueryEvent::Complete(CommandComplete {
            tag,
            rows_affected: 0
        })) if tag == "MERGE 0"
    ));
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id, value FROM merge_target ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1), Value::Text("new-one".into())]),
            Row::new(vec![Value::Int64(3), Value::Text("new-three".into())]),
            Row::new(vec![Value::Int64(4), Value::Text("stable".into())]),
        ]
    );

    execute(&mut session, "DELETE FROM merge_source", &[]);
    execute(
        &mut session,
        "INSERT INTO merge_source VALUES
            (1, 'first', 'update'), (1, 'second', 'update')",
        &[],
    );
    let cardinality = session
        .execute(
            "MERGE INTO merge_target AS target
             USING merge_source AS source ON target.id = source.id
             WHEN MATCHED THEN UPDATE SET value = source.value",
            &[],
        )
        .expect_err("same target affected twice");
    assert_eq!(cardinality.sql_state, "21000");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT value FROM merge_target WHERE id = 1",
            &[],
        )),
        vec![Row::new(vec![Value::Text("new-one".into())])]
    );

    execute(&mut session, "DELETE FROM merge_source", &[]);
    execute(
        &mut session,
        "INSERT INTO merge_source VALUES
            (1, 'temporary', 'update'), (5, 'stable', 'insert')",
        &[],
    );
    let uniqueness = session
        .execute(
            "MERGE INTO merge_target AS target
             USING merge_source AS source ON target.id = source.id
             WHEN MATCHED THEN UPDATE SET value = source.value
             WHEN NOT MATCHED THEN INSERT (id, value)
                 VALUES (source.id, source.value)",
            &[],
        )
        .expect_err("atomic unique failure");
    assert_eq!(uniqueness.sql_state, "23505");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id, value FROM merge_target ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1), Value::Text("new-one".into())]),
            Row::new(vec![Value::Int64(3), Value::Text("new-three".into())]),
            Row::new(vec![Value::Int64(4), Value::Text("stable".into())]),
        ]
    );

    execute(&mut session, "DELETE FROM merge_source", &[]);
    execute(
        &mut session,
        "INSERT INTO merge_source VALUES (4, 'savepoint', 'update')",
        &[],
    );
    execute(&mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
    execute(&mut session, "SAVEPOINT before_merge", &[]);
    execute(
        &mut session,
        "MERGE INTO merge_target AS target
         USING merge_source AS source ON target.id = source.id
         WHEN MATCHED THEN UPDATE SET value = source.value",
        &[],
    );
    execute(&mut session, "ROLLBACK TO SAVEPOINT before_merge", &[]);
    execute(&mut session, "COMMIT", &[]);
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT value FROM merge_target WHERE id = 4",
            &[],
        )),
        vec![Row::new(vec![Value::Text("stable".into())])]
    );

    execute(&mut session, "DELETE FROM merge_source", &[]);
    execute(
        &mut session,
        "INSERT INTO merge_source VALUES (1, 'ignored', 'skip')",
        &[],
    );
    let by_source = execute(
        &mut session,
        "MERGE INTO merge_target AS target
         USING merge_source AS source ON target.id = source.id
         WHEN MATCHED THEN DO NOTHING
         WHEN NOT MATCHED BY SOURCE AND target.id = 3 THEN DELETE
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 'orphan'
         RETURNING id, value",
        &[],
    );
    let returned = rows(&by_source);
    assert_eq!(returned.len(), 2);
    assert!(returned.contains(&Row::new(vec![
        Value::Int64(3),
        Value::Text("new-three".into()),
    ])));
    assert!(returned.contains(&Row::new(vec![
        Value::Int64(4),
        Value::Text("orphan".into()),
    ])));
    assert!(matches!(
        by_source.last(),
        Some(QueryEvent::Complete(CommandComplete {
            tag,
            rows_affected: 2
        })) if tag == "MERGE 2"
    ));
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id, value FROM merge_target ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1), Value::Text("new-one".into())]),
            Row::new(vec![Value::Int64(4), Value::Text("orphan".into())]),
        ]
    );
}

#[test]
fn read_committed_on_conflict_rechecks_after_unique_key_wait() {
    let (_directory, engine) = engine();
    engine
        .set_default_lock_timeout(Duration::from_secs(2))
        .expect("configure lock timeout");
    let mut first = engine.connect().expect("first session");
    let mut second = engine.connect().expect("second session");
    execute(
        &mut first,
        "CREATE TABLE concurrent_upserts (id BIGINT PRIMARY KEY, label TEXT NOT NULL)",
        &[],
    );
    execute(&mut first, "BEGIN", &[]);
    execute(&mut second, "BEGIN", &[]);
    execute(
        &mut first,
        "INSERT INTO concurrent_upserts VALUES (1, 'first')",
        &[],
    );

    let worker = std::thread::spawn(move || -> Result<Vec<QueryEvent>> {
        let events = second
            .execute(
                "INSERT INTO concurrent_upserts VALUES (1, 'second') \
                 ON CONFLICT (id) DO UPDATE SET label = excluded.label \
                 RETURNING label",
                &[],
            )?
            .collect::<Vec<_>>();
        second.execute("COMMIT", &[])?.for_each(drop);
        Ok(events)
    });
    let mut waiting_observed = false;
    for _ in 0..100 {
        if !engine.lock_snapshot().expect("lock snapshot").1.is_empty() {
            waiting_observed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(waiting_observed, "UPSERT did not wait for the unique key");
    execute(&mut first, "COMMIT", &[]);

    let events = worker
        .join()
        .expect("UPSERT worker")
        .expect("UPSERT result");
    assert_eq!(
        rows(&events),
        vec![Row::new(vec![Value::Text("second".into())])]
    );
    assert_eq!(
        rows(&execute(
            &mut first,
            "SELECT label FROM concurrent_upserts WHERE id = 1",
            &[],
        )),
        vec![Row::new(vec![Value::Text("second".into())])]
    );
}

#[test]
fn repeatable_read_on_conflict_reports_serialization_after_unique_key_wait() {
    let (_directory, engine) = engine();
    engine
        .set_default_lock_timeout(Duration::from_secs(2))
        .expect("configure lock timeout");
    let mut first = engine.connect().expect("first session");
    let mut second = engine.connect().expect("second session");
    execute(
        &mut first,
        "CREATE TABLE repeatable_upserts (id BIGINT PRIMARY KEY, label TEXT NOT NULL)",
        &[],
    );
    execute(&mut first, "BEGIN", &[]);
    execute(&mut second, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
    execute(
        &mut first,
        "INSERT INTO repeatable_upserts VALUES (1, 'first')",
        &[],
    );

    let worker = std::thread::spawn(move || -> Result<String> {
        let error = second
            .execute(
                "INSERT INTO repeatable_upserts VALUES (1, 'second') \
                 ON CONFLICT (id) DO UPDATE SET label = excluded.label",
                &[],
            )
            .expect_err("stale Repeatable Read UPSERT");
        second.execute("ROLLBACK", &[])?.for_each(drop);
        Ok(error.sql_state.to_string())
    });
    let mut waiting_observed = false;
    for _ in 0..100 {
        if !engine.lock_snapshot().expect("lock snapshot").1.is_empty() {
            waiting_observed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(waiting_observed, "UPSERT did not wait for the unique key");
    execute(&mut first, "COMMIT", &[]);

    assert_eq!(
        worker
            .join()
            .expect("UPSERT worker")
            .expect("UPSERT result"),
        "40001"
    );
    assert_eq!(
        rows(&execute(
            &mut first,
            "SELECT label FROM repeatable_upserts WHERE id = 1",
            &[],
        )),
        vec![Row::new(vec![Value::Text("first".into())])]
    );
}

#[test]
fn returning_stream_batches_rows_and_retains_its_memory_peak() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE returning_items (id BIGINT PRIMARY KEY)",
        &[],
    );
    let values = (0..=DEFAULT_BATCH_ROWS)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut stream = session
        .execute_stream(
            &format!("INSERT INTO returning_items VALUES {values} RETURNING id"),
            &[],
        )
        .expect("returning stream");
    assert!(
        stream
            .execution_memory_peak_bytes()
            .is_some_and(|peak| peak > 0)
    );

    let events = stream
        .by_ref()
        .collect::<Result<Vec<_>>>()
        .expect("stream events");
    let batch_lengths = events
        .iter()
        .filter_map(|event| match event {
            QueryEvent::Batch(batch) => Some(batch.rows.len()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(batch_lengths, [DEFAULT_BATCH_ROWS, 1]);
    assert_eq!(rows(&events).len(), DEFAULT_BATCH_ROWS + 1);
    assert!(
        stream
            .execution_memory_peak_bytes()
            .is_some_and(|peak| peak > 0)
    );
    assert!(matches!(
        events.last(),
        Some(QueryEvent::Complete(CommandComplete {
            tag,
            rows_affected
        })) if tag == "INSERT 0 1025" && *rows_affected == 1_025
    ));
}

#[test]
fn committed_versions_reopen_with_stable_predecessors_and_visible_storage_scan() {
    let directory = tempdir().expect("tempdir");
    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
        let mut session = engine.connect().expect("connect");
        create_documents(&mut session);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'original', 10), (2, 'deleted', 20)",
            &[],
        );
        execute(
            &mut session,
            "UPDATE documents SET title = 'updated' WHERE id = 1",
            &[],
        );
        execute(&mut session, "DELETE FROM documents WHERE id = 2", &[]);

        let state = engine.state.read().expect("state");
        let table_id = state
            .catalog
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        let versions = state.versions.get(&table_id).expect("versions");
        let visible = state
            .visible_versions
            .get(&table_id)
            .expect("visible versions");
        assert_eq!(versions.len(), 3);
        assert_eq!(visible.as_slice(), &[3]);
        assert_eq!(versions[0].version_id, 1);
        assert_eq!(versions[0].header.previous_version, 0);
        assert_ne!(versions[0].header.xmax, 0);
        assert_eq!(versions[1].version_id, 2);
        assert_eq!(versions[1].header.previous_version, 0);
        assert_ne!(versions[1].header.xmax, 0);
        assert_eq!(versions[2].version_id, 3);
        assert_eq!(versions[2].header.previous_version, 1);
        assert_eq!(versions[2].header.xmin, versions[0].header.xmax);
        assert_eq!(versions[2].header.xmax, 0);
        assert_eq!(
            engine
                .transaction_status
                .transaction_outcome(
                    TransactionId::new(versions[2].header.xmin).expect("creator transaction")
                )
                .expect("creator outcome"),
            ordadb_transaction::TransactionOutcome::Committed
        );
    }

    let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut session = engine.connect().expect("connect");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id, title FROM documents ORDER BY id",
            &[],
        )),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("updated".into())
        ])]
    );
    let state = engine.state.read().expect("state");
    let table_id = state
        .catalog
        .table(
            &Identifier::unquoted("public"),
            &Identifier::unquoted("documents"),
        )
        .expect("documents")
        .id;
    assert_eq!(state.versions.get(&table_id).expect("versions").len(), 3);
    assert_eq!(
        state
            .visible_versions
            .get(&table_id)
            .expect("visible versions")
            .as_slice(),
        &[3]
    );
}

#[test]
fn aborted_update_keeps_the_original_version_visible_after_reopen() {
    let directory = tempdir().expect("tempdir");
    {
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
        let mut session = engine.connect().expect("connect");
        create_documents(&mut session);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'original', 10)",
            &[],
        );
        let mut transaction = session.begin().expect("begin");
        transaction
            .execute("UPDATE documents SET title = 'aborted' WHERE id = 1", &[])
            .expect("update");
        transaction.rollback().expect("rollback");
    }

    let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut session = engine.connect().expect("connect");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id, title FROM documents",
            &[],
        )),
        vec![Row::new(vec![
            Value::Int64(1),
            Value::Text("original".into())
        ])]
    );
    let state = engine.state.read().expect("state");
    let table_id = state
        .catalog
        .table(
            &Identifier::unquoted("public"),
            &Identifier::unquoted("documents"),
        )
        .expect("documents")
        .id;
    let versions = state.versions.get(&table_id).expect("versions");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].header.xmax, 0);
    assert_eq!(
        state
            .visible_versions
            .get(&table_id)
            .expect("visible versions")
            .as_slice(),
        &[1]
    );
}

#[test]
fn compares_jsonb_parameters_by_equality_without_requiring_ordering() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE payloads (id BIGINT PRIMARY KEY, body JSONB NOT NULL)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO payloads VALUES (1, $1), (2, $2)",
        &[
            Value::Jsonb(serde_json::json!({"kind": "match"})),
            Value::Jsonb(serde_json::json!({"kind": "other"})),
        ],
    );

    let equal = execute(
        &mut session,
        "SELECT id FROM payloads WHERE body = $1 ORDER BY id",
        &[Value::Jsonb(serde_json::json!({"kind": "match"}))],
    );
    assert_eq!(rows(&equal), vec![Row::new(vec![Value::Int64(1)])]);

    let not_equal = execute(
        &mut session,
        "SELECT id FROM payloads WHERE body <> $1 ORDER BY id",
        &[Value::Jsonb(serde_json::json!({"kind": "match"}))],
    );
    assert_eq!(rows(&not_equal), vec![Row::new(vec![Value::Int64(2)])]);
}

#[test]
fn enforces_not_null_primary_key_and_unique_constraints_atomically() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE NOT NULL)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO users VALUES (1, 'a@example.test')",
        &[],
    );

    let error = session
        .execute(
            "INSERT INTO users VALUES (2, 'b@example.test'), (1, 'c@example.test')",
            &[],
        )
        .expect_err("duplicate primary key");
    assert_eq!(error.sql_state, "23505");
    let events = execute(&mut session, "SELECT * FROM users", &[]);
    assert_eq!(rows(&events).len(), 1);

    let error = session
        .execute("INSERT INTO users VALUES (2, NULL)", &[])
        .expect_err("not null");
    assert_eq!(error.sql_state, "23502");
}
