
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use ordadb_catalog::NewColumn;
    use ordadb_storage::{
        DatabaseStore, DurabilityBarrier, PageId, PersistentState, RecoveryDataFile, RecoveryPlan,
    };
    use ordadb_types::{Identifier, Row, ScalarType, Value};
    use tempfile::tempdir;

    use super::analyze;
    use crate::{
        CheckpointBegin, CheckpointState, DeterministicFaultInjector, FaultInjector, FaultPoint,
        Lsn, RecordKind, TransactionId, WalManager, WalPayload, WalRecord,
    };

    fn lsn(value: u64) -> Lsn {
        Lsn::new(value).expect("non-zero LSN")
    }

    fn transaction_id(value: u64) -> TransactionId {
        TransactionId::new(value).expect("non-zero transaction ID")
    }

    fn open_store(path: &std::path::Path, wal: &Arc<WalManager>) -> DatabaseStore {
        let barrier: Arc<dyn DurabilityBarrier> = wal.clone();
        DatabaseStore::open_with_barrier(path, barrier).expect("open store")
    }

    fn prepared_generation(
        store: &DatabaseStore,
        generation: u64,
    ) -> ordadb_storage::PreparedCommit {
        let candidate = PersistentState {
            generation,
            ..PersistentState::default()
        };
        store.prepare_commit(&candidate).expect("prepare commit")
    }

    fn large_row_state(row_count: usize, generation: u64) -> PersistentState {
        let mut state = PersistentState {
            generation,
            ..PersistentState::default()
        };
        let table_id = state
            .catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("large_rows"),
                vec![NewColumn::new(
                    Identifier::unquoted("payload"),
                    ScalarType::Text,
                )],
            )
            .expect("create large-row table");
        state.tables.insert(
            table_id,
            (0..row_count)
                .map(|index| Row::new(vec![Value::Text(format!("{index}:{}", "x".repeat(6_000)))]))
                .collect(),
        );
        state
    }

    #[test]
    fn analysis_identifies_winners_and_losers() {
        let winner = transaction_id(1);
        let loser = transaction_id(2);
        let records = vec![
            WalRecord::new(lsn(1), Some(winner), None, WalPayload::Begin).expect("winner begin"),
            WalRecord::new(lsn(2), Some(winner), Some(lsn(1)), WalPayload::Commit)
                .expect("winner commit"),
            WalRecord::new(lsn(3), Some(loser), None, WalPayload::Begin).expect("loser begin"),
        ];
        let analysis = analyze(&records).expect("analysis");
        let (winners, losers) = super::transaction_outcomes(&analysis.transactions);
        assert_eq!(winners, [winner].into_iter().collect());
        assert_eq!(losers, [loser].into_iter().collect());
    }

    #[test]
    fn incomplete_checkpoint_is_ignored() {
        let begin = WalRecord::new(
            lsn(1),
            None,
            None,
            WalPayload::CheckpointBegin(CheckpointBegin {
                active_transactions: BTreeMap::new(),
                dirty_pages: BTreeMap::new(),
                visibility_horizon: None,
            }),
        )
        .expect("checkpoint begin");
        let analysis = analyze(&[begin]).expect("analysis");
        assert!(analysis.checkpoint.is_none());
    }

    #[test]
    fn last_complete_checkpoint_wins_over_a_later_incomplete_begin() {
        let directory = tempdir().expect("temp directory");
        let faults = DeterministicFaultInjector::new();
        let injector: Arc<dyn FaultInjector> = faults.clone();
        let wal =
            WalManager::open_with_fault_injector(directory.path(), injector).expect("open WAL");
        faults
            .arm(FaultPoint::BeforeCheckpointEndAppend, 1)
            .expect("arm checkpoint failure");
        let error = wal
            .checkpoint(CheckpointState {
                durable_data_generation: 6,
                data_file_page_count: 1,
                ..CheckpointState::default()
            })
            .expect_err("checkpoint End is interrupted");
        assert_eq!(error.sql_state, "58030");
        let interrupted = wal.scan().expect("scan interrupted checkpoint");
        assert!(
            analyze(&interrupted.records)
                .expect("analyze interrupted checkpoint")
                .checkpoint
                .is_none()
        );

        let complete_end = wal
            .checkpoint(CheckpointState {
                durable_data_generation: 7,
                data_file_page_count: 1,
                ..CheckpointState::default()
            })
            .expect("complete checkpoint");
        let incomplete_begin = wal
            .append(
                None,
                None,
                WalPayload::CheckpointBegin(CheckpointBegin::default()),
            )
            .expect("incomplete checkpoint begin");
        wal.flush_lsn(incomplete_begin)
            .expect("flush incomplete checkpoint begin");

        let scan = wal.scan().expect("scan WAL");
        let analysis = analyze(&scan.records).expect("analysis");
        let checkpoint = analysis.checkpoint.expect("complete checkpoint");
        assert_eq!(checkpoint.end_lsn, complete_end);
        assert!(checkpoint.begin_lsn < incomplete_begin);
    }

    #[test]
    fn durable_winner_is_recovered() {
        let directory = tempdir().expect("temp directory");
        let faults = DeterministicFaultInjector::new();
        let injector: Arc<dyn FaultInjector> = faults.clone();
        let wal =
            WalManager::open_with_fault_injector(directory.path(), injector).expect("open WAL");
        let mut store = open_store(directory.path(), &wal);
        let mut prepared = prepared_generation(&store, 1);
        let transaction = wal
            .log_prepared(transaction_id(1), &mut prepared)
            .expect("log prepared");
        let original_page = prepared.page_deltas()[0]
            .before
            .clone()
            .expect("metadata before image");
        let page_lsn = *transaction
            .page_update_lsns()
            .values()
            .next()
            .expect("metadata update");
        assert!(
            wal.durable_lsn()
                .expect("durable LSN")
                .is_some_and(|durable| durable >= page_lsn)
        );
        store.apply_prepared(&prepared).expect("apply data");
        wal.commit(&transaction).expect("commit WAL");
        store
            .publish_prepared(prepared)
            .expect("publish prepared metadata");
        drop(store);
        let mut stale_data =
            RecoveryDataFile::open(directory.path(), RecoveryPlan::new(1, [PageId::new(0)]))
                .expect("open stale-page fixture");
        stale_data
            .apply_page(&original_page)
            .expect("restore stale page");
        stale_data.finish().expect("finish stale-page fixture");

        faults
            .arm(FaultPoint::BeforeDataPageWrite, 1)
            .expect("arm recovery page-write failure");
        let error = wal
            .recover(directory.path())
            .expect_err("recovery page write is interrupted");
        assert_eq!(error.sql_state, "58030");
        let report = wal.recover(directory.path()).expect("recover winner");
        assert_eq!(report.winners, [transaction_id(1)].into_iter().collect());
        assert!(report.redone_page_records > 0);
        let reopened = open_store(directory.path(), &wal);
        assert_eq!(reopened.committed_state().generation, 1);
    }

    #[test]
    fn loser_is_undone_and_aborted() {
        let directory = tempdir().expect("temp directory");
        let wal = WalManager::open(directory.path()).expect("open WAL");
        let mut store = open_store(directory.path(), &wal);
        let mut prepared = prepared_generation(&store, 1);
        wal.log_prepared(transaction_id(1), &mut prepared)
            .expect("log prepared");
        store
            .apply_prepared(&prepared)
            .expect("apply uncommitted data");
        drop(store);

        let report = wal.recover(directory.path()).expect("recover loser");
        assert_eq!(report.losers, [transaction_id(1)].into_iter().collect());
        assert!(report.compensation_records > 0);
        let reopened = open_store(directory.path(), &wal);
        assert_eq!(reopened.committed_state().generation, 0);
    }

    #[test]
    fn flushed_compensation_is_redone_before_loser_undo_resumes() {
        let directory = tempdir().expect("temp directory");
        let faults = DeterministicFaultInjector::new();
        let injector: Arc<dyn FaultInjector> = faults.clone();
        let wal =
            WalManager::open_with_fault_injector(directory.path(), injector).expect("open WAL");
        let mut store = open_store(directory.path(), &wal);
        let mut prepared = prepared_generation(&store, 1);
        wal.log_prepared(transaction_id(1), &mut prepared)
            .expect("log prepared");
        store
            .apply_prepared(&prepared)
            .expect("apply uncommitted data");
        drop(store);

        faults
            .arm(FaultPoint::AfterCompensationFlush, 1)
            .expect("arm post-CLR failure");
        let error = wal
            .recover(directory.path())
            .expect_err("recovery stops after durable CLR");
        assert_eq!(error.sql_state, "58030");
        let report = wal
            .recover(directory.path())
            .expect("second recovery redoes CLR");
        assert_eq!(report.losers, [transaction_id(1)].into_iter().collect());
        let records = wal.scan().expect("final WAL scan").records;
        assert_eq!(
            records
                .iter()
                .filter(|record| record.kind() == RecordKind::Compensation)
                .count(),
            1
        );
        assert!(
            records
                .iter()
                .any(|record| record.kind() == RecordKind::Abort)
        );
        let reopened = open_store(directory.path(), &wal);
        assert_eq!(reopened.committed_state().generation, 0);

        drop(reopened);
        let second = wal
            .recover(directory.path())
            .expect("repeated recovery is idempotent");
        assert!(second.losers.is_empty());
        assert_eq!(second.compensation_records, 0);
    }

    #[test]
    fn durable_commit_response_failure_recovers_as_a_winner() {
        let directory = tempdir().expect("temp directory");
        let faults = DeterministicFaultInjector::new();
        let injector: Arc<dyn FaultInjector> = faults.clone();
        let wal =
            WalManager::open_with_fault_injector(directory.path(), injector).expect("open WAL");
        let mut store = open_store(directory.path(), &wal);
        let mut prepared = prepared_generation(&store, 1);
        let transaction = wal
            .log_prepared(transaction_id(1), &mut prepared)
            .expect("log prepared");
        store.apply_prepared(&prepared).expect("apply data");
        faults
            .arm(FaultPoint::AfterCommitFlush, 1)
            .expect("arm ambiguous Commit response");
        let error = wal
            .commit(&transaction)
            .expect_err("response fails after durable Commit");
        assert_eq!(error.sql_state, "58030");
        drop(store);

        let report = wal
            .recover(directory.path())
            .expect("recover durable ambiguous Commit");
        assert_eq!(report.winners, [transaction_id(1)].into_iter().collect());
        let reopened = open_store(directory.path(), &wal);
        assert_eq!(reopened.committed_state().generation, 1);
    }

    #[test]
    fn loser_growth_and_shrink_restore_original_page_counts() {
        let growth_directory = tempdir().expect("growth temp directory");
        let growth_wal = WalManager::open(growth_directory.path()).expect("open growth WAL");
        let mut growth_store = open_store(growth_directory.path(), &growth_wal);
        growth_store
            .commit(&large_row_state(1, 0))
            .expect("install one-row baseline");
        let growth_candidate = large_row_state(2, 1);
        let mut growth_prepared = growth_store
            .prepare_commit(&growth_candidate)
            .expect("prepare growth");
        growth_wal
            .log_prepared(transaction_id(1), &mut growth_prepared)
            .expect("log growth");
        growth_store
            .apply_prepared(&growth_prepared)
            .expect("apply uncommitted growth");
        drop(growth_store);
        let growth_report = growth_wal
            .recover(growth_directory.path())
            .expect("undo loser growth");
        assert_eq!(growth_report.final_page_count, 2);
        let growth_reopened = open_store(growth_directory.path(), &growth_wal);
        assert_eq!(growth_reopened.committed_state().generation, 0);
        assert_eq!(growth_reopened.page_count().expect("growth page count"), 2);

        let shrink_directory = tempdir().expect("shrink temp directory");
        let shrink_wal = WalManager::open(shrink_directory.path()).expect("open shrink WAL");
        let mut shrink_store = open_store(shrink_directory.path(), &shrink_wal);
        shrink_store
            .commit(&large_row_state(2, 0))
            .expect("install two-row baseline");
        let shrink_candidate = large_row_state(1, 1);
        let mut shrink_prepared = shrink_store
            .prepare_commit(&shrink_candidate)
            .expect("prepare shrink");
        shrink_wal
            .log_prepared(transaction_id(1), &mut shrink_prepared)
            .expect("log shrink");
        shrink_store
            .apply_prepared(&shrink_prepared)
            .expect("apply uncommitted shrink");
        drop(shrink_store);

        let shrink_report = shrink_wal
            .recover(shrink_directory.path())
            .expect("undo loser shrink");
        assert_eq!(shrink_report.final_page_count, 3);
        let shrink_reopened = open_store(shrink_directory.path(), &shrink_wal);
        assert_eq!(shrink_reopened.committed_state().generation, 0);
        assert_eq!(shrink_reopened.page_count().expect("shrink page count"), 3);
    }
}
