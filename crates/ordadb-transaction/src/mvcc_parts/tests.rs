
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use ordadb_storage::{FROZEN_TRANSACTION_ID, TupleHeaderV2};
    use tempfile::tempdir;

    use super::*;

    #[derive(Debug, Default)]
    struct Statuses(BTreeMap<TransactionId, TransactionOutcome>);

    impl TransactionStatusProvider for Statuses {
        fn transaction_outcome(&self, transaction_id: TransactionId) -> Result<TransactionOutcome> {
            self.0
                .get(&transaction_id)
                .copied()
                .ok_or_else(|| DbError::new("XX001", "missing test transaction status"))
        }
    }

    #[test]
    fn read_committed_refreshes_while_repeatable_read_retains_snapshot() {
        let manager = TransactionManager::new();
        let mut read_committed = manager
            .begin(TransactionCharacteristics::default())
            .expect("read committed");
        let mut repeatable = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::RepeatableRead,
                ..TransactionCharacteristics::default()
            })
            .expect("repeatable read");
        let writer = manager
            .begin(TransactionCharacteristics::default())
            .expect("writer");
        let writer_id = writer.transaction_id();
        assert!(
            read_committed
                .begin_statement()
                .expect("first snapshot")
                .in_progress
                .contains(&writer_id)
        );
        let repeatable_xmax = repeatable.snapshot().xmax;
        writer.commit().expect("writer commit");
        assert!(
            !read_committed
                .begin_statement()
                .expect("refreshed snapshot")
                .in_progress
                .contains(&writer_id)
        );
        assert_eq!(
            repeatable
                .begin_statement()
                .expect("retained snapshot")
                .xmax,
            repeatable_xmax
        );
    }

    #[test]
    fn dropped_transaction_is_aborted_and_removed_from_horizon() {
        let manager = TransactionManager::new();
        let transaction = manager
            .begin(TransactionCharacteristics::default())
            .expect("transaction");
        let transaction_id = transaction.transaction_id();
        drop(transaction);
        assert_eq!(
            manager
                .transaction_outcome(transaction_id)
                .expect("terminal status"),
            TransactionOutcome::Aborted
        );
        assert!(
            !manager
                .active_transactions()
                .expect("active set")
                .contains(&transaction_id)
        );
    }

    #[test]
    fn repeatable_snapshot_age_is_bounded_and_configurable() {
        let manager = TransactionManager::new();
        assert_eq!(
            manager
                .set_maximum_snapshot_age(Duration::ZERO)
                .expect_err("zero age")
                .sql_state,
            "22023"
        );
        manager
            .set_maximum_snapshot_age(Duration::from_millis(1))
            .expect("configure maximum age");
        let transaction = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::RepeatableRead,
                ..TransactionCharacteristics::default()
            })
            .expect("repeatable transaction");
        let transaction_id = transaction.transaction_id();
        thread::sleep(Duration::from_millis(5));
        assert_eq!(
            manager.expired_snapshot().expect("expired snapshot"),
            Some(transaction_id)
        );
        transaction.abort().expect("abort");
        assert_eq!(manager.expired_snapshot().expect("empty horizon"), None);
    }

    #[test]
    fn deferrable_requires_serializable_read_only() {
        let error = TransactionCharacteristics {
            deferrable: true,
            ..TransactionCharacteristics::default()
        }
        .validate()
        .expect_err("invalid deferrable");
        assert_eq!(error.sql_state, "25001");
        TransactionCharacteristics {
            isolation_level: IsolationLevel::Serializable,
            access_mode: TransactionAccessMode::ReadOnly,
            deferrable: true,
        }
        .validate()
        .expect("valid deferrable");
    }

    #[test]
    fn deferrable_reader_waits_for_a_safe_serializable_snapshot() {
        let manager = TransactionManager::new();
        let writer = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::Serializable,
                ..TransactionCharacteristics::default()
            })
            .expect("serializable writer");
        let writer_id = writer.transaction_id();
        let mut reader = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::Serializable,
                access_mode: TransactionAccessMode::ReadOnly,
                deferrable: true,
            })
            .expect("deferrable reader");
        let (send, receive) = mpsc::channel();
        let worker = thread::spawn(move || {
            let result = reader.begin_statement().cloned();
            let _ = reader.abort();
            send.send(result).expect("send snapshot");
        });
        thread::sleep(Duration::from_millis(20));
        assert!(matches!(receive.try_recv(), Err(mpsc::TryRecvError::Empty)));
        writer.commit().expect("writer commit");
        let snapshot = receive
            .recv_timeout(Duration::from_secs(1))
            .expect("safe snapshot result")
            .expect("safe snapshot");
        assert!(!snapshot.in_progress.contains(&writer_id));
        worker.join().expect("reader join");
    }

    #[test]
    fn deferrable_safe_snapshot_wait_is_cancellable() {
        let manager = TransactionManager::new();
        let writer = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::Serializable,
                ..TransactionCharacteristics::default()
            })
            .expect("serializable writer");
        let mut reader = manager
            .begin(TransactionCharacteristics {
                isolation_level: IsolationLevel::Serializable,
                access_mode: TransactionAccessMode::ReadOnly,
                deferrable: true,
            })
            .expect("deferrable reader");
        let cancellation = Arc::new(AtomicBool::new(false));
        let worker_cancellation = Arc::clone(&cancellation);
        let (send, receive) = mpsc::channel();
        let worker = thread::spawn(move || {
            let result = reader
                .begin_statement_with_cancellation(worker_cancellation.as_ref())
                .map(|_| ());
            let _ = reader.abort();
            send.send(result).expect("send cancellation result");
        });
        thread::sleep(Duration::from_millis(20));
        cancellation.store(true, Ordering::Release);

        let error = receive
            .recv_timeout(Duration::from_secs(1))
            .expect("cancellation result")
            .expect_err("safe snapshot wait cancelled");
        assert_eq!(error.sql_state, "57014");
        writer.abort().expect("writer abort");
        worker.join().expect("reader join");
    }

    #[test]
    fn tuple_visibility_respects_snapshot_and_terminal_status() {
        let current = TransactionId::new(20).expect("current ID");
        let creator = TransactionId::new(10).expect("creator ID");
        let deleter = TransactionId::new(12).expect("deleter ID");
        let mut statuses = Statuses::default();
        statuses.0.insert(creator, TransactionOutcome::Committed);
        statuses.0.insert(deleter, TransactionOutcome::Aborted);
        let snapshot = TransactionSnapshot {
            xmin: creator,
            xmax: current,
            in_progress: Arc::new(BTreeSet::new()),
            command_id: 3,
        };
        let header = TupleHeaderV2 {
            flags: 0,
            column_count: 1,
            xmin: creator.get(),
            xmax: deleter.get(),
            command_id: 1,
            previous_version: 0,
        };
        assert!(tuple_visible(header, &snapshot, current, &statuses).expect("visible"));
        statuses.0.insert(deleter, TransactionOutcome::Committed);
        assert!(!tuple_visible(header, &snapshot, current, &statuses).expect("deleted"));
        let frozen = TupleHeaderV2 {
            xmin: FROZEN_TRANSACTION_ID,
            xmax: 0,
            ..header
        };
        assert!(tuple_visible(frozen, &snapshot, current, &statuses).expect("frozen"));
    }

    #[test]
    fn savepoint_names_use_nearest_scope_and_release_descendants() {
        let mut stack = SavepointStack::new();
        let first = stack.push("same", 1, 2, 3).expect("first savepoint");
        stack.push("nested", 2, 4, 5).expect("nested savepoint");
        let nearest = stack.push("same", 3, 6, 7).expect("nearest savepoint");
        assert_ne!(first, nearest);
        assert_eq!(stack.rollback_to("same").expect("rollback").id, nearest);
        assert_eq!(stack.frames().len(), 3);
        assert_eq!(stack.release("nested").expect("release").name, "nested");
        assert_eq!(stack.frames().len(), 1);
        assert_eq!(
            stack
                .rollback_to("missing")
                .expect_err("unknown savepoint")
                .sql_state,
            "3B001"
        );
    }

    #[test]
    fn durable_transaction_commit_and_drop_keep_status_and_wal_aligned() {
        let directory = tempdir().expect("tempdir");
        let wal = WalManager::open(directory.path()).expect("wal");
        let status = Arc::new(TransactionStatusStore::open(directory.path(), 9).expect("status"));
        let snapshot = status.snapshot().expect("status snapshot");
        let manager = TransactionManager::from_status_snapshot(
            snapshot.next_transaction_id,
            snapshot.statuses,
        )
        .expect("manager");

        let committed = DurableTransaction::begin(
            &manager,
            Arc::clone(&status),
            Arc::clone(&wal),
            TransactionCharacteristics::default(),
        )
        .expect("committed transaction");
        let committed_id = committed.transaction_id();
        committed.commit_empty().expect("empty commit");
        assert_eq!(
            status
                .transaction_outcome(committed_id)
                .expect("committed status"),
            TransactionOutcome::Committed
        );
        assert_eq!(
            wal.transaction_outcomes()
                .expect("wal outcomes")
                .get(&committed_id),
            Some(&TransactionOutcome::Committed)
        );

        let aborted = DurableTransaction::begin(
            &manager,
            Arc::clone(&status),
            Arc::clone(&wal),
            TransactionCharacteristics::default(),
        )
        .expect("aborted transaction");
        let aborted_id = aborted.transaction_id();
        drop(aborted);
        assert_eq!(
            status
                .transaction_outcome(aborted_id)
                .expect("aborted status"),
            TransactionOutcome::Aborted
        );
        assert_eq!(
            wal.transaction_outcomes()
                .expect("wal outcomes")
                .get(&aborted_id),
            Some(&TransactionOutcome::Aborted)
        );
    }
}
