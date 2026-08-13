
#[test]
fn commits_rolls_back_and_allows_disjoint_writers() {
    let (_directory, engine) = engine();
    let mut first = engine.connect().expect("first");
    let mut second = engine.connect().expect("second");
    create_documents(&mut first);

    {
        let mut transaction = first.begin().expect("begin");
        transaction
            .execute("INSERT INTO documents VALUES (1, 'rolled back', 1)", &[])
            .expect("insert");
        transaction.rollback().expect("rollback");
    }
    assert!(rows(&execute(&mut first, "SELECT * FROM documents", &[])).is_empty());

    {
        let mut transaction = first.begin().expect("begin");
        transaction
            .execute("INSERT INTO documents VALUES (1, 'committed', 1)", &[])
            .expect("insert");
        transaction.commit().expect("commit");
    }
    assert_eq!(
        rows(&execute(&mut first, "SELECT * FROM documents", &[])).len(),
        1
    );

    let mut transaction = first.begin().expect("begin writer");
    transaction
        .execute("INSERT INTO documents VALUES (2, 'rolled back', 2)", &[])
        .expect("transaction insert");
    execute(
        &mut second,
        "INSERT INTO documents VALUES (3, 'concurrent', 3)",
        &[],
    );
    transaction.rollback().expect("rollback writer");
    assert_eq!(
        rows(&execute(
            &mut second,
            "SELECT id FROM documents ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(3)]),
        ]
    );
}

#[test]
fn dml_locks_are_scoped_and_released_on_transaction_rollback() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("session");
    create_documents(&mut session);
    let mut transaction = session.begin().expect("begin");
    transaction
        .execute("INSERT INTO documents VALUES (1, 'locked', 1)", &[])
        .expect("insert");

    let (granted, waiting) = engine.locks.snapshot().expect("lock snapshot");
    assert!(waiting.is_empty());
    assert!(
        granted
            .iter()
            .any(|lock| { lock.key == LockKey::Database && lock.mode == LockMode::Shared })
    );
    assert!(granted.iter().any(|lock| {
        matches!(lock.key, LockKey::Table { .. }) && lock.mode == LockMode::Shared
    }));
    assert!(granted.iter().any(|lock| {
        matches!(lock.key, LockKey::IndexKey { .. }) && lock.mode == LockMode::Exclusive
    }));

    transaction.rollback().expect("rollback");
    let (granted, waiting) = engine.locks.snapshot().expect("released snapshot");
    assert!(granted.is_empty());
    assert!(waiting.is_empty());
}

#[test]
fn concurrent_disjoint_writers_merge_without_lost_updates() {
    let (_directory, engine) = engine();
    let mut first = engine.connect().expect("first");
    let mut second = engine.connect().expect("second");
    create_documents(&mut first);

    let mut first_transaction = first.begin().expect("first begin");
    let mut second_transaction = second.begin().expect("second begin");
    first_transaction
        .execute("INSERT INTO documents VALUES (1, 'first', 1)", &[])
        .expect("first insert");
    second_transaction
        .execute("INSERT INTO documents VALUES (2, 'second', 2)", &[])
        .expect("second insert");
    first_transaction.commit().expect("first commit");
    second_transaction.commit().expect("second commit");

    assert_eq!(
        rows(&execute(
            &mut first,
            "SELECT id FROM documents ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );
}

#[test]
fn row_and_unique_conflicts_timeout_then_report_committed_duplicates() {
    let (_directory, engine) = engine();
    engine
        .set_default_lock_timeout(Duration::from_millis(20))
        .expect("configure lock timeout");
    let mut first = engine.connect().expect("first");
    let mut second = engine.connect().expect("second");
    create_documents(&mut first);
    execute(
        &mut first,
        "INSERT INTO documents VALUES (1, 'base', 1)",
        &[],
    );

    execute(&mut first, "BEGIN", &[]);
    execute(&mut second, "BEGIN", &[]);
    execute(
        &mut first,
        "UPDATE documents SET title = 'first' WHERE id = 1",
        &[],
    );
    assert_eq!(
        second
            .execute("UPDATE documents SET title = 'second' WHERE id = 1", &[],)
            .expect_err("row lock timeout")
            .sql_state,
        "55P03"
    );
    execute(&mut second, "ROLLBACK", &[]);
    execute(&mut first, "COMMIT", &[]);

    execute(&mut first, "BEGIN", &[]);
    execute(&mut second, "BEGIN", &[]);
    execute(
        &mut first,
        "INSERT INTO documents VALUES (2, 'first unique', 2)",
        &[],
    );
    assert_eq!(
        second
            .execute("INSERT INTO documents VALUES (2, 'second unique', 3)", &[],)
            .expect_err("unique key lock timeout")
            .sql_state,
        "55P03"
    );
    execute(&mut second, "ROLLBACK", &[]);
    execute(&mut first, "COMMIT", &[]);
    assert_eq!(
        second
            .execute(
                "INSERT INTO documents VALUES (2, 'committed duplicate', 4)",
                &[],
            )
            .expect_err("committed duplicate")
            .sql_state,
        "23505"
    );
}

#[test]
fn engine_deadlock_aborts_the_youngest_transaction_and_releases_waiters() {
    let (_directory, engine) = engine();
    engine
        .set_default_lock_timeout(Duration::from_secs(1))
        .expect("configure lock timeout");
    let mut first = engine.connect().expect("first");
    let mut second = engine.connect().expect("second");
    create_documents(&mut first);
    execute(
        &mut first,
        "INSERT INTO documents VALUES (1, 'one', 1), (2, 'two', 2)",
        &[],
    );
    execute(&mut first, "BEGIN", &[]);
    execute(&mut second, "BEGIN", &[]);
    execute(
        &mut first,
        "UPDATE documents SET title = 'first' WHERE id = 1",
        &[],
    );
    execute(
        &mut second,
        "UPDATE documents SET title = 'second' WHERE id = 2",
        &[],
    );

    let (send, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = first
            .execute("UPDATE documents SET title = 'first' WHERE id = 2", &[])
            .map(|_| ());
        if result.is_ok() {
            execute(&mut first, "COMMIT", &[]);
        }
        send.send(result).expect("send first result");
    });
    let mut waiting_observed = false;
    for _ in 0..100 {
        if !engine.lock_snapshot().expect("lock snapshot").1.is_empty() {
            waiting_observed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        waiting_observed,
        "first transaction did not enter lock wait"
    );
    assert_eq!(
        second
            .execute("UPDATE documents SET title = 'second' WHERE id = 1", &[],)
            .expect_err("deadlock victim")
            .sql_state,
        "40P01"
    );
    execute(&mut second, "ROLLBACK", &[]);
    receive
        .recv_timeout(Duration::from_secs(1))
        .expect("first result")
        .expect("surviving transaction");
    worker.join().expect("first transaction join");
    assert!(
        engine
            .lock_snapshot()
            .expect("released lock snapshot")
            .0
            .is_empty()
    );
}

#[test]
fn read_committed_refreshes_and_repeatable_read_retains_visibility() {
    let (_directory, engine) = engine();
    let mut reader = engine.connect().expect("reader");
    let mut writer = engine.connect().expect("writer");
    create_documents(&mut reader);
    execute(
        &mut writer,
        "INSERT INTO documents VALUES (1, 'v1', 1)",
        &[],
    );

    execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
    assert_eq!(
        rows(&execute(
            &mut reader,
            "SELECT title FROM documents WHERE id = 1",
            &[],
        )),
        vec![Row::new(vec![Value::Text("v1".to_owned())])]
    );
    execute(
        &mut writer,
        "UPDATE documents SET title = 'v2' WHERE id = 1",
        &[],
    );
    assert_eq!(
        rows(&execute(
            &mut reader,
            "SELECT title FROM documents WHERE id = 1",
            &[],
        )),
        vec![Row::new(vec![Value::Text("v1".to_owned())])]
    );
    execute(&mut reader, "COMMIT", &[]);

    execute(&mut reader, "BEGIN ISOLATION LEVEL READ COMMITTED", &[]);
    assert_eq!(
        rows(&execute(
            &mut reader,
            "SELECT title FROM documents WHERE id = 1",
            &[],
        )),
        vec![Row::new(vec![Value::Text("v2".to_owned())])]
    );
    execute(
        &mut writer,
        "UPDATE documents SET title = 'v3' WHERE id = 1",
        &[],
    );
    assert_eq!(
        rows(&execute(
            &mut reader,
            "SELECT title FROM documents WHERE id = 1",
            &[],
        )),
        vec![Row::new(vec![Value::Text("v3".to_owned())])]
    );
    execute(&mut reader, "COMMIT", &[]);
}

#[test]
fn read_committed_rebases_private_writes_over_disjoint_commits() {
    let (_directory, engine) = engine();
    let mut reader = engine.connect().expect("reader");
    let mut writer = engine.connect().expect("writer");
    create_documents(&mut reader);

    execute(&mut reader, "BEGIN ISOLATION LEVEL READ COMMITTED", &[]);
    execute(
        &mut reader,
        "INSERT INTO documents VALUES (1, 'private', 1)",
        &[],
    );
    execute(
        &mut writer,
        "INSERT INTO documents VALUES (2, 'concurrent', 2)",
        &[],
    );
    assert_eq!(
        rows(&execute(
            &mut reader,
            "SELECT id FROM documents ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );
    execute(
        &mut reader,
        "INSERT INTO documents VALUES (3, 'after-refresh', 3)",
        &[],
    );
    execute(&mut reader, "COMMIT", &[]);

    assert_eq!(
        rows(&execute(
            &mut writer,
            "SELECT id FROM documents ORDER BY id",
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
fn programmatic_read_committed_rebases_private_writes() {
    let (_directory, engine) = engine();
    let mut reader = engine.connect().expect("reader");
    let mut writer = engine.connect().expect("writer");
    create_documents(&mut reader);

    let mut transaction = reader.begin().expect("begin");
    transaction
        .execute("INSERT INTO documents VALUES (1, 'private', 1)", &[])
        .expect("private insert");
    execute(
        &mut writer,
        "INSERT INTO documents VALUES (2, 'concurrent', 2)",
        &[],
    );
    let selected = transaction
        .execute("SELECT id FROM documents ORDER BY id", &[])
        .expect("refreshed select")
        .collect::<Vec<_>>();
    assert_eq!(
        rows(&selected),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );
    transaction.commit().expect("commit");
}

#[test]
fn sql_transaction_upgrades_staged_dml_before_ddl() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("session");
    let mut writer = engine.connect().expect("writer");
    create_documents(&mut session);

    execute(&mut session, "BEGIN ISOLATION LEVEL READ COMMITTED", &[]);
    execute(
        &mut session,
        "INSERT INTO documents VALUES (1, 'private', 1)",
        &[],
    );
    execute(
        &mut writer,
        "INSERT INTO documents VALUES (2, 'concurrent', 2)",
        &[],
    );
    execute(
        &mut session,
        "CREATE INDEX documents_score_idx ON documents (score)",
        &[],
    );
    execute(&mut session, "COMMIT", &[]);

    assert_eq!(
        rows(&execute(
            &mut writer,
            "SELECT id FROM documents ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );
    execute(&mut writer, "DROP INDEX documents_score_idx", &[]);
}

#[test]
fn programmatic_transaction_upgrades_staged_dml_before_ddl() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("session");
    create_documents(&mut session);

    let mut transaction = session.begin().expect("begin");
    transaction
        .execute("INSERT INTO documents VALUES (1, 'private', 1)", &[])
        .expect("insert");
    transaction
        .execute("CREATE INDEX documents_score_idx ON documents (score)", &[])
        .expect("create index after DML");
    transaction.commit().expect("commit");

    execute(&mut session, "DROP INDEX documents_score_idx", &[]);
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
fn sql_savepoint_restores_candidate_and_recovers_failed_transaction() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("session");
    create_documents(&mut session);
    execute(&mut session, "BEGIN", &[]);
    execute(
        &mut session,
        "INSERT INTO documents VALUES (1, 'before', 1)",
        &[],
    );
    execute(&mut session, "SAVEPOINT keep_before", &[]);
    execute(
        &mut session,
        "INSERT INTO documents VALUES (2, 'after', 2)",
        &[],
    );
    let duplicate = session
        .execute("INSERT INTO documents VALUES (1, 'duplicate', 3)", &[])
        .expect_err("duplicate");
    assert_eq!(duplicate.sql_state, "23505");
    assert_eq!(session.transaction_status(), TransactionStatus::Failed);
    assert_eq!(
        session
            .execute("SELECT id FROM documents", &[])
            .expect_err("failed transaction")
            .sql_state,
        "25P02"
    );

    execute(&mut session, "ROLLBACK TO SAVEPOINT keep_before", &[]);
    assert_eq!(session.transaction_status(), TransactionStatus::Active);
    execute(
        &mut session,
        "INSERT INTO documents VALUES (3, 'recovered', 3)",
        &[],
    );
    execute(&mut session, "COMMIT", &[]);
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT id FROM documents ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(3)]),
        ]
    );
}

#[test]
fn sql_savepoint_rollback_restores_ssi_predicates() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("session");
    create_documents(&mut session);
    execute(&mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
    execute(&mut session, "SAVEPOINT before_read", &[]);
    execute(&mut session, "SELECT id FROM documents", &[]);

    let before = engine.ssi.snapshot().expect("SSI before rollback");
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].read_predicates, 1);
    execute(&mut session, "ROLLBACK TO SAVEPOINT before_read", &[]);

    let after = engine.ssi.snapshot().expect("SSI after rollback");
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].read_predicates, 0);
    execute(&mut session, "ROLLBACK", &[]);
}

#[test]
fn serializable_ssi_rejects_write_skew() {
    let (_directory, engine) = engine();
    let mut first = engine.connect().expect("first session");
    let mut second = engine.connect().expect("second session");
    execute(
        &mut first,
        "CREATE TABLE doctors (id INT PRIMARY KEY, on_call BOOLEAN NOT NULL)",
        &[],
    );
    execute(
        &mut first,
        "INSERT INTO doctors VALUES (1, TRUE), (2, TRUE)",
        &[],
    );
    execute(&mut first, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
    execute(&mut second, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
    assert_eq!(
        rows(&execute(
            &mut first,
            "SELECT id FROM doctors WHERE on_call = TRUE ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int32(1)]),
            Row::new(vec![Value::Int32(2)]),
        ]
    );
    execute(
        &mut second,
        "SELECT id FROM doctors WHERE on_call = TRUE ORDER BY id",
        &[],
    );
    execute(
        &mut first,
        "UPDATE doctors SET on_call = FALSE WHERE id = 1",
        &[],
    );
    execute(&mut first, "COMMIT", &[]);
    execute(
        &mut second,
        "UPDATE doctors SET on_call = FALSE WHERE id = 2",
        &[],
    );
    let error = second.execute("COMMIT", &[]).expect_err("write skew");
    assert_eq!(error.sql_state, "40001");

    let mut verifier = engine.connect().expect("verifier");
    assert_eq!(
        rows(&execute(
            &mut verifier,
            "SELECT id FROM doctors WHERE on_call = TRUE ORDER BY id",
            &[],
        )),
        vec![Row::new(vec![Value::Int32(2)])]
    );
}

#[test]
fn isolation_snapshots_prevent_dirty_reads_and_repeatable_read_phantoms() {
    let (_directory, engine) = engine();
    let mut writer = engine.connect().expect("writer");
    let mut reader = engine.connect().expect("reader");
    create_documents(&mut writer);

    execute(&mut writer, "BEGIN", &[]);
    execute(
        &mut writer,
        "INSERT INTO documents VALUES (1, 'uncommitted', 1)",
        &[],
    );
    assert!(rows(&execute(&mut reader, "SELECT id FROM documents", &[])).is_empty());
    execute(&mut writer, "COMMIT", &[]);

    execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
    assert_eq!(
        rows(&execute(
            &mut reader,
            "SELECT id FROM documents ORDER BY id",
            &[],
        )),
        vec![Row::new(vec![Value::Int64(1)])]
    );
    execute(
        &mut writer,
        "INSERT INTO documents VALUES (2, 'phantom', 2)",
        &[],
    );
    assert_eq!(
        rows(&execute(
            &mut reader,
            "SELECT id FROM documents ORDER BY id",
            &[],
        )),
        vec![Row::new(vec![Value::Int64(1)])]
    );
    execute(&mut reader, "COMMIT", &[]);
    assert_eq!(
        rows(&execute(
            &mut reader,
            "SELECT id FROM documents ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]
    );
}

#[test]
fn repeatable_read_rejects_stale_update_and_delete_targets() {
    let (_directory, engine) = engine();
    let mut stale = engine.connect().expect("stale session");
    let mut concurrent = engine.connect().expect("concurrent session");
    create_documents(&mut stale);
    execute(
        &mut stale,
        "INSERT INTO documents VALUES (1, 'first', 1), (2, 'second', 2)",
        &[],
    );

    execute(&mut stale, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
    execute(&mut stale, "SELECT title FROM documents WHERE id = 1", &[]);
    execute(
        &mut concurrent,
        "UPDATE documents SET title = 'concurrent' WHERE id = 1",
        &[],
    );
    assert_eq!(
        stale
            .execute("UPDATE documents SET title = 'stale' WHERE id = 1", &[],)
            .expect_err("stale update conflict")
            .sql_state,
        "40001"
    );
    execute(&mut stale, "ROLLBACK", &[]);

    execute(&mut stale, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
    execute(&mut stale, "SELECT title FROM documents WHERE id = 2", &[]);
    execute(
        &mut concurrent,
        "UPDATE documents SET title = 'changed' WHERE id = 2",
        &[],
    );
    assert_eq!(
        stale
            .execute("DELETE FROM documents WHERE id = 2", &[])
            .expect_err("stale delete conflict")
            .sql_state,
        "40001"
    );
    execute(&mut stale, "ROLLBACK", &[]);
}

#[test]
fn repeatable_read_writers_merge_unrelated_row_changes() {
    let (_directory, engine) = engine();
    let mut first = engine.connect().expect("first session");
    let mut second = engine.connect().expect("second session");
    create_documents(&mut first);
    execute(
        &mut first,
        "INSERT INTO documents VALUES (1, 'first', 1), (2, 'second', 2)",
        &[],
    );
    execute(&mut first, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
    execute(&mut second, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
    execute(
        &mut first,
        "UPDATE documents SET title = 'first-committed' WHERE id = 1",
        &[],
    );
    execute(
        &mut second,
        "UPDATE documents SET title = 'second-committed' WHERE id = 2",
        &[],
    );
    execute(&mut first, "COMMIT", &[]);
    execute(&mut second, "COMMIT", &[]);

    let mut verifier = engine.connect().expect("verifier");
    assert_eq!(
        rows(&execute(
            &mut verifier,
            "SELECT title FROM documents ORDER BY id",
            &[],
        )),
        vec![
            Row::new(vec![Value::Text("first-committed".to_owned())]),
            Row::new(vec![Value::Text("second-committed".to_owned())]),
        ]
    );
}

#[test]
fn vacuum_protects_active_snapshots_then_reclaims_and_analyzes() {
    let (_directory, engine) = engine();
    let mut reader = engine.connect().expect("reader");
    let mut writer = engine.connect().expect("writer");
    create_documents(&mut writer);
    execute(
        &mut writer,
        "INSERT INTO documents VALUES (1, 'v1', 1)",
        &[],
    );
    execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
    execute(&mut reader, "SELECT title FROM documents WHERE id = 1", &[]);
    execute(
        &mut writer,
        "UPDATE documents SET title = 'v2' WHERE id = 1",
        &[],
    );
    let table_id = {
        let state = engine.state.read().expect("state");
        state
            .catalog
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id
    };
    execute(&mut writer, "VACUUM documents", &[]);
    assert_eq!(
        engine
            .state
            .read()
            .expect("protected state")
            .versions
            .get(&table_id)
            .expect("versions")
            .len(),
        2
    );

    execute(&mut reader, "ROLLBACK", &[]);
    execute(&mut writer, "VACUUM ANALYZE documents", &[]);
    let state = engine.state.read().expect("vacuumed state");
    let versions = state.versions.get(&table_id).expect("versions");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version_id, 1);
    assert_eq!(versions[0].header.previous_version, 0);
    assert_eq!(
        state
            .catalog
            .table_by_id(table_id)
            .expect("documents")
            .statistics()
            .row_count,
        1
    );
    drop(state);
    assert_eq!(
        rows(&execute(
            &mut writer,
            "SELECT title FROM documents WHERE id = 1",
            &[],
        )),
        vec![Row::new(vec![Value::Text("v2".to_owned())])]
    );
}

#[test]
fn vacuum_rejects_an_expired_protected_snapshot() {
    let (_directory, engine) = engine();
    engine
        .set_maximum_snapshot_age(Duration::from_millis(1))
        .expect("configure snapshot age");
    let mut reader = engine.connect().expect("reader");
    let mut writer = engine.connect().expect("writer");
    create_documents(&mut writer);
    execute(
        &mut writer,
        "INSERT INTO documents VALUES (1, 'visible', 1)",
        &[],
    );
    execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
    execute(&mut reader, "SELECT id FROM documents", &[]);
    std::thread::sleep(Duration::from_millis(5));
    let error = writer
        .execute("VACUUM documents", &[])
        .expect_err("expired snapshot");
    assert_eq!(error.sql_state, "55000");
    assert!(error.message.contains("expired snapshot"));
    execute(&mut reader, "ROLLBACK", &[]);
    execute(&mut writer, "VACUUM documents", &[]);
}

#[test]
fn full_vacuum_freezes_live_versions_and_compacts_transaction_status() {
    let (directory, engine) = engine();
    let mut session = engine.connect().expect("session");
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
    let before = engine
        .transaction_status
        .snapshot()
        .expect("status before vacuum");
    execute(&mut session, "VACUUM", &[]);
    let after = engine
        .transaction_status
        .snapshot()
        .expect("status after vacuum");
    assert!(after.retained_transaction_floor > before.retained_transaction_floor);
    assert!(after.statuses.len() < before.statuses.len());
    let table_id = engine
        .catalog_snapshot()
        .expect("catalog")
        .table(
            &Identifier::unquoted("public"),
            &Identifier::unquoted("documents"),
        )
        .expect("documents")
        .id;
    assert!(
        engine
            .state
            .read()
            .expect("state")
            .versions
            .get(&table_id)
            .expect("versions")
            .iter()
            .all(|version| version.header.xmin == FROZEN_TRANSACTION_ID)
    );
    drop(session);
    drop(engine);

    let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
    let mut session = reopened.connect().expect("reopened session");
    assert_eq!(
        rows(&execute(
            &mut session,
            "SELECT title FROM documents WHERE id = 1",
            &[],
        )),
        vec![Row::new(vec![Value::Text("v2".to_owned())])]
    );
}

#[test]
fn vacuum_is_rejected_inside_sql_and_programmatic_transactions() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("session");
    create_documents(&mut session);
    execute(&mut session, "BEGIN", &[]);
    assert_eq!(
        session
            .execute("VACUUM", &[])
            .expect_err("SQL transaction VACUUM")
            .sql_state,
        "25001"
    );
    execute(&mut session, "ROLLBACK", &[]);

    let mut transaction = session.begin().expect("programmatic transaction");
    assert_eq!(
        transaction
            .execute("VACUUM", &[])
            .expect_err("programmatic transaction VACUUM")
            .sql_state,
        "25001"
    );
    transaction.rollback().expect("rollback");
}
