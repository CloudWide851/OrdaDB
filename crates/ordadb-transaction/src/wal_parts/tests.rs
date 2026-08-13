
#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::Arc;

    use ordadb_storage::DurabilityBarrier;
    use tempfile::tempdir;

    use super::{WalManager, inspect_wal_read_only};
    use crate::{
        DeterministicFaultInjector, FaultInjector, FaultPoint, Lsn, TransactionId, WalPayload,
        WalRecord,
    };

    fn transaction_id(value: u64) -> TransactionId {
        TransactionId::new(value).expect("non-zero transaction ID")
    }

    #[test]
    fn read_only_inspection_treats_a_missing_wal_as_empty() {
        let directory = tempdir().expect("temp directory");

        let inspection = inspect_wal_read_only(directory.path()).expect("inspect missing WAL");

        assert_eq!(inspection.file_bytes, 0);
        assert_eq!(inspection.record_count, 0);
        assert_eq!(inspection.max_transaction_id, None);
        assert!(
            directory
                .path()
                .read_dir()
                .expect("read directory")
                .next()
                .is_none()
        );
    }

    #[test]
    fn read_only_inspection_reports_exact_records_and_transaction_high_water_mark() {
        let directory = tempdir().expect("temp directory");
        let wal = WalManager::open(directory.path()).expect("open WAL");
        wal.append(Some(transaction_id(4)), None, WalPayload::Begin)
            .expect("append first Begin");
        wal.append(Some(transaction_id(9)), None, WalPayload::Begin)
            .expect("append second Begin");
        let expected_bytes = wal.path().metadata().expect("inspect WAL").len();
        drop(wal);

        let inspection = inspect_wal_read_only(directory.path()).expect("inspect WAL");

        assert_eq!(inspection.file_bytes, expected_bytes);
        assert_eq!(inspection.record_count, 2);
        assert_eq!(inspection.max_transaction_id, Some(transaction_id(9)));
    }

    #[test]
    fn read_only_inspection_rejects_an_incomplete_tail_without_repairing_it() {
        let directory = tempdir().expect("temp directory");
        let wal = WalManager::open(directory.path()).expect("open WAL");
        wal.append(Some(transaction_id(1)), None, WalPayload::Begin)
            .expect("append Begin");
        let path = wal.path().to_path_buf();
        drop(wal);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open tail")
            .write_all(b"ORDA")
            .expect("append partial header");
        let before = std::fs::read(&path).expect("read WAL before inspection");

        let error = inspect_wal_read_only(directory.path()).expect_err("incomplete tail refused");

        assert_eq!(error.sql_state, "XX001");
        assert_eq!(
            std::fs::read(path).expect("read WAL after inspection"),
            before
        );
    }

    #[test]
    fn scanner_repairs_only_an_incomplete_final_record() {
        let directory = tempdir().expect("temp directory");
        let wal = WalManager::open(directory.path()).expect("open WAL");
        wal.append(Some(transaction_id(1)), None, WalPayload::Begin)
            .expect("append Begin");
        let path = wal.path().to_path_buf();
        drop(wal);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open tail")
            .write_all(b"ORDA")
            .expect("append partial header");

        let reopened = WalManager::open(directory.path()).expect("repair tail");
        assert_eq!(reopened.startup_truncated_bytes(), 4);
        assert_eq!(reopened.scan().expect("scan").records.len(), 1);
    }

    #[test]
    fn complete_checksum_corruption_is_not_repaired() {
        let directory = tempdir().expect("temp directory");
        let wal = WalManager::open(directory.path()).expect("open WAL");
        wal.append(Some(transaction_id(1)), None, WalPayload::Begin)
            .expect("append Begin");
        let path = wal.path().to_path_buf();
        drop(wal);
        let mut bytes = std::fs::read(&path).expect("read WAL");
        let last = bytes.len() - 1;
        bytes[last] ^= 0x80;
        std::fs::write(path, bytes).expect("corrupt WAL");

        let error = WalManager::open(directory.path()).expect_err("corruption refused");
        assert_eq!(error.sql_state, "XX001");
    }

    #[test]
    fn durability_barrier_rejects_unappended_lsn() {
        let directory = tempdir().expect("temp directory");
        let wal = WalManager::open(directory.path()).expect("open WAL");
        let barrier: Arc<dyn DurabilityBarrier> = wal;
        let error = barrier
            .flush_through(1)
            .expect_err("unappended LSN cannot flush");
        assert_eq!(error.sql_state, "55000");
    }

    #[test]
    fn append_and_flush_advances_durable_lsn() {
        let directory = tempdir().expect("temp directory");
        let wal = WalManager::open(directory.path()).expect("open WAL");
        let lsn = wal
            .append(Some(transaction_id(1)), None, WalPayload::Begin)
            .expect("append Begin");
        assert_eq!(wal.durable_lsn().expect("durable state"), None);
        wal.flush_lsn(lsn).expect("flush Begin");
        assert_eq!(wal.durable_lsn().expect("durable state"), Some(lsn));
    }

    #[test]
    fn wal_flush_failpoint_is_deterministic_and_retryable() {
        let directory = tempdir().expect("temp directory");
        let faults = DeterministicFaultInjector::new();
        let injector: Arc<dyn FaultInjector> = faults.clone();
        let wal =
            WalManager::open_with_fault_injector(directory.path(), injector).expect("open WAL");
        let lsn = wal
            .append(Some(transaction_id(1)), None, WalPayload::Begin)
            .expect("append Begin");
        faults
            .arm(FaultPoint::BeforeWalFlush, 1)
            .expect("arm WAL flush failure");
        let error = wal.flush_lsn(lsn).expect_err("flush is interrupted");
        assert_eq!(error.sql_state, "58030");
        assert_eq!(wal.durable_lsn().expect("durable state"), None);
        wal.flush_lsn(lsn).expect("retry flush");
        assert_eq!(wal.durable_lsn().expect("durable state"), Some(lsn));
    }

    #[test]
    fn complete_record_with_impossible_previous_lsn_is_rejected() {
        let directory = tempdir().expect("temp directory");
        let path = directory.path().join("ordadb.wal");
        let transaction_id = transaction_id(1);
        let begin = WalRecord::new(
            Lsn::new(1).expect("LSN"),
            Some(transaction_id),
            None,
            WalPayload::Begin,
        )
        .expect("Begin")
        .encode()
        .expect("encode Begin");
        let commit = WalRecord::new(
            Lsn::new(3).expect("LSN"),
            Some(transaction_id),
            Lsn::new(2),
            WalPayload::Commit,
        )
        .expect("locally valid Commit")
        .encode()
        .expect("encode Commit");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .expect("create WAL");
        file.write_all(&begin).expect("write Begin");
        file.write_all(&commit).expect("write Commit");
        file.sync_all().expect("sync corrupt chain");
        drop(file);

        let error =
            WalManager::open(directory.path()).expect_err("invalid transaction chain refused");
        assert_eq!(error.sql_state, "XX001");
    }
}
