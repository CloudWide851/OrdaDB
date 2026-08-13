
#[test]
fn storage_scan_open_error_and_stream_drop_release_the_read_lease() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE lease_items (id BIGINT PRIMARY KEY)",
        &[],
    );
    execute(&mut session, "INSERT INTO lease_items VALUES (1)", &[]);

    let generation = session.state.read().expect("state").generation;
    let snapshot = session.state.read().expect("state").clone();
    let provider = StorageTableProviderV2::new(
        Arc::clone(&engine.store),
        Arc::clone(&engine.storage_access),
        generation,
        &snapshot.rows,
        snapshot.system_catalog.as_deref(),
    );
    let error = match provider.scan(TableId::new(u64::MAX)) {
        Ok(_) => panic!("unknown table scan must fail"),
        Err(error) => error,
    };
    assert_eq!(error.sql_state, "42P01");
    assert_eq!(
        engine
            .storage_access
            .active_readers()
            .expect("active readers"),
        0
    );

    let stream = session
        .execute_stream("SELECT id FROM lease_items", &[])
        .expect("stream");
    assert_eq!(
        engine
            .storage_access
            .active_readers()
            .expect("active readers"),
        1
    );
    drop(stream);
    assert_eq!(
        engine
            .storage_access
            .active_readers()
            .expect("active readers"),
        0
    );
}

#[test]
fn storage_scan_rejects_resident_row_count_mismatch_and_releases_lease() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE resident_items (id BIGINT PRIMARY KEY)",
        &[],
    );
    execute(&mut session, "INSERT INTO resident_items VALUES (1)", &[]);

    let snapshot = session.state.read().expect("state").clone();
    let table_id = *snapshot.rows.keys().next().expect("resident table");
    let mismatched_rows = BTreeMap::from([(table_id, Arc::new(Vec::<Row>::new()))]);
    let provider = StorageTableProviderV2::new(
        Arc::clone(&engine.store),
        Arc::clone(&engine.storage_access),
        snapshot.generation,
        &mismatched_rows,
        None,
    );

    let error = match provider.scan(table_id) {
        Ok(_) => panic!("row-count mismatch must fail"),
        Err(error) => error,
    };
    assert_eq!(error.sql_state, "XX001");
    assert_eq!(
        engine
            .storage_access
            .active_readers()
            .expect("active readers"),
        0
    );
}

#[test]
fn cancelled_storage_stream_releases_resources_without_terminal_success_events() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE cancelled_items (id BIGINT PRIMARY KEY)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO cancelled_items VALUES (1), (2), (3)",
        &[],
    );

    let cancellation = Arc::new(AtomicBool::new(false));
    let mut stream = session
        .execute_stream_with_cancellation(
            "SELECT id FROM cancelled_items",
            &[],
            Arc::clone(&cancellation),
        )
        .expect("stream");
    assert!(matches!(
        stream.next().expect("schema").expect("schema event"),
        QueryEvent::Schema(_)
    ));
    assert_eq!(
        engine
            .storage_access
            .active_readers()
            .expect("active readers"),
        1
    );

    cancellation.store(true, Ordering::Release);
    assert_eq!(
        stream
            .next()
            .expect("cancellation error")
            .expect_err("cancelled")
            .sql_state,
        "57014"
    );
    assert!(stream.next().is_none());
    assert_eq!(
        engine
            .storage_access
            .active_readers()
            .expect("active readers"),
        0
    );
}

#[test]
fn fallible_stream_retains_query_accounted_peak_after_completion() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE memory_items (id BIGINT, payload TEXT)",
        &[],
    );
    execute(
        &mut session,
        "INSERT INTO memory_items VALUES (1, 'alpha'), (2, 'beta')",
        &[],
    );

    let mut stream = session
        .execute_stream("SELECT id, payload FROM memory_items", &[])
        .expect("stream");
    assert_eq!(stream.execution_memory_peak_bytes(), Some(0));
    while stream.next().is_some() {}
    assert!(
        stream
            .execution_memory_peak_bytes()
            .is_some_and(|peak| peak > 0 && peak <= 256 * 1024 * 1024)
    );
}

#[test]
fn read_snapshots_share_arcs_and_writes_copy_only_the_affected_table() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE cow_a (id BIGINT PRIMARY KEY)",
        &[],
    );
    execute(
        &mut session,
        "CREATE TABLE cow_b (id BIGINT PRIMARY KEY)",
        &[],
    );
    execute(&mut session, "INSERT INTO cow_a VALUES (1)", &[]);
    execute(&mut session, "INSERT INTO cow_b VALUES (2)", &[]);

    let (a_before, b_before, catalog_before) = {
        let state = session.state.read().expect("state");
        (
            state.rows.get(&TableId::new(1)).expect("a").clone(),
            state.rows.get(&TableId::new(2)).expect("b").clone(),
            state.catalog.clone(),
        )
    };
    execute(&mut session, "SELECT id FROM cow_a", &[]);
    {
        let state = session.state.read().expect("state");
        assert!(Arc::ptr_eq(
            &a_before,
            state.rows.get(&TableId::new(1)).expect("a")
        ));
        assert!(Arc::ptr_eq(
            &b_before,
            state.rows.get(&TableId::new(2)).expect("b")
        ));
        assert!(Arc::ptr_eq(&catalog_before, &state.catalog));
    }

    execute(&mut session, "UPDATE cow_a SET id = 3", &[]);
    let state = session.state.read().expect("state");
    assert!(!Arc::ptr_eq(
        &a_before,
        state.rows.get(&TableId::new(1)).expect("a")
    ));
    assert!(Arc::ptr_eq(
        &b_before,
        state.rows.get(&TableId::new(2)).expect("b")
    ));
}

#[test]
fn a_lazy_stream_error_marks_its_sql_transaction_failed() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    execute(
        &mut session,
        "CREATE TABLE stream_failures (id BIGINT PRIMARY KEY)",
        &[],
    );
    execute(&mut session, "BEGIN", &[]);
    let failure_flag = match &session.sql_transaction {
        SqlTransactionState::Active(transaction) => Arc::clone(&transaction.stream_failed),
        _ => panic!("expected active SQL transaction"),
    };
    let mut stream = TryQueryStream {
        state: TryQueryStreamState::Events(
            vec![Err(DbError::new("53200", "query memory limit exceeded"))].into_iter(),
        ),
        failed: false,
        failure_flag: Some(failure_flag),
        cancellation: None,
        execution_memory_peak_bytes: None,
        _event_reservation: None,
    };

    assert_eq!(
        stream
            .next()
            .expect("stream error")
            .expect_err("error")
            .sql_state,
        "53200"
    );
    assert_eq!(session.transaction_status(), TransactionStatus::Failed);
    assert_eq!(
        session
            .execute("SELECT * FROM stream_failures", &[])
            .expect_err("failed transaction")
            .sql_state,
        "25P02"
    );
    execute(&mut session, "ROLLBACK", &[]);
    assert_eq!(session.transaction_status(), TransactionStatus::Idle);
}

#[test]
fn durable_commits_trigger_the_conservative_automatic_checkpoint() {
    let (_directory, engine) = engine();
    let mut session = engine.connect().expect("connect");
    engine
        .commits_since_checkpoint
        .store(AUTOMATIC_CHECKPOINT_INTERVAL - 1, Ordering::Release);
    execute(
        &mut session,
        "CREATE TABLE checkpoint_rows (id BIGINT PRIMARY KEY)",
        &[],
    );

    let records = engine.wal.scan().expect("scan WAL").records;
    assert!(
        records
            .iter()
            .any(|record| { record.kind() == ordadb_transaction::RecordKind::CheckpointEnd })
    );
    assert!(records.iter().any(|record| {
        matches!(
            record.payload(),
            ordadb_transaction::WalPayload::CheckpointBegin(checkpoint)
                if checkpoint.visibility_horizon.is_some()
        )
    }));
    assert_eq!(
        engine.writer.active_transaction().expect("writer state"),
        None
    );
    assert_eq!(engine.commits_since_checkpoint.load(Ordering::Acquire), 0);
}
