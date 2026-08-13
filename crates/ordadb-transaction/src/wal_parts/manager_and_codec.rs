use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use ordadb_storage::{DurabilityBarrier, PageId, PreparedCommit, SlottedPage};
use ordadb_types::{DbError, Result};

use crate::record::{CheckpointBegin, CheckpointEnd, parse_header};
use crate::{
    FaultInjector, FaultPoint, Lsn, NoFaultInjector, RecordKind, ScanResult, TransactionId,
    TransactionOutcome, WAL_HEADER_LEN, WalPayload, WalRecord, corruption, io_error,
};

pub const WAL_FILE_NAME: &str = "ordadb.wal";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalInspection {
    pub file_bytes: u64,
    pub record_count: usize,
    pub max_transaction_id: Option<TransactionId>,
}

pub fn inspect_wal_read_only(data_dir: impl AsRef<Path>) -> Result<WalInspection> {
    let path = data_dir.as_ref().join(WAL_FILE_NAME);
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WalInspection::default());
        }
        Err(error) => return Err(io_error("failed to open the WAL read-only", error)),
    };
    let file_bytes = file
        .metadata()
        .map_err(|error| io_error("failed to inspect the WAL read-only", error))?
        .len();
    let scan = scan_file(&mut file, false)?;
    let max_transaction_id = scan
        .records
        .iter()
        .filter_map(WalRecord::transaction_id)
        .max();
    Ok(WalInspection {
        file_bytes,
        record_count: scan.records.len(),
        max_transaction_id,
    })
}

#[derive(Debug, Clone, Default)]
pub struct CheckpointState {
    pub active_transactions: BTreeMap<TransactionId, Lsn>,
    pub dirty_pages: BTreeMap<PageId, Lsn>,
    pub visibility_horizon: Option<TransactionId>,
    pub durable_data_generation: u64,
    pub durable_wal_lsn: Option<Lsn>,
    pub data_file_page_count: u64,
}

#[derive(Debug, Clone)]
pub struct LoggedTransaction {
    transaction_id: TransactionId,
    begin_lsn: Lsn,
    last_lsn: Lsn,
    page_update_lsns: BTreeMap<PageId, Lsn>,
}

impl LoggedTransaction {
    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[must_use]
    pub const fn begin_lsn(&self) -> Lsn {
        self.begin_lsn
    }

    #[must_use]
    pub const fn last_lsn(&self) -> Lsn {
        self.last_lsn
    }

    #[must_use]
    pub const fn page_update_lsns(&self) -> &BTreeMap<PageId, Lsn> {
        &self.page_update_lsns
    }
}

pub struct WalManager {
    path: PathBuf,
    state: Mutex<WalState>,
    recovery_lock: Mutex<()>,
    fault_injector: Arc<dyn FaultInjector>,
    startup_truncated_bytes: u64,
}

impl std::fmt::Debug for WalManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WalManager")
            .field("path", &self.path)
            .field("startup_truncated_bytes", &self.startup_truncated_bytes)
            .finish_non_exhaustive()
    }
}

struct WalState {
    file: File,
    next_lsn: Lsn,
    durable_lsn: Option<Lsn>,
    begin_by_transaction: BTreeMap<TransactionId, Lsn>,
    last_by_transaction: BTreeMap<TransactionId, Lsn>,
    terminal_transactions: BTreeSet<TransactionId>,
    transaction_outcomes: BTreeMap<TransactionId, TransactionOutcome>,
    max_transaction_id: Option<TransactionId>,
    dirty_pages: BTreeMap<PageId, Lsn>,
    transaction_dirty_pages: BTreeMap<TransactionId, BTreeSet<PageId>>,
}

impl WalManager {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::open_with_fault_injector(data_dir, Arc::new(NoFaultInjector))
    }

    pub fn open_with_fault_injector(
        data_dir: impl AsRef<Path>,
        fault_injector: Arc<dyn FaultInjector>,
    ) -> Result<Arc<Self>> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir)
            .map_err(|error| io_error("failed to create WAL data directory", error))?;
        let path = data_dir.join(WAL_FILE_NAME);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| io_error("failed to open WAL file", error))?;
        let scan = scan_file(&mut file, true)?;
        let last_lsn = scan.records.last().map(WalRecord::lsn);
        let next_lsn = match last_lsn {
            Some(last) => last.checked_next()?,
            None => Lsn::new(1).ok_or_else(|| DbError::internal("LSN one is invalid"))?,
        };
        let mut state = WalState {
            file,
            next_lsn,
            durable_lsn: last_lsn,
            begin_by_transaction: BTreeMap::new(),
            last_by_transaction: BTreeMap::new(),
            terminal_transactions: BTreeSet::new(),
            transaction_outcomes: BTreeMap::new(),
            max_transaction_id: None,
            dirty_pages: BTreeMap::new(),
            transaction_dirty_pages: BTreeMap::new(),
        };
        for record in &scan.records {
            apply_record_state(&mut state, record, true)?;
        }
        Ok(Arc::new(Self {
            path,
            state: Mutex::new(state),
            recovery_lock: Mutex::new(()),
            fault_injector,
            startup_truncated_bytes: scan.truncated_bytes,
        }))
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub const fn startup_truncated_bytes(&self) -> u64 {
        self.startup_truncated_bytes
    }

    pub fn scan(&self) -> Result<ScanResult> {
        let mut state = self.lock_state()?;
        scan_file(&mut state.file, false)
    }

    pub fn append(
        &self,
        transaction_id: Option<TransactionId>,
        previous_lsn: Option<Lsn>,
        payload: WalPayload,
    ) -> Result<Lsn> {
        let mut state = self.lock_state()?;
        append_locked(&mut state, transaction_id, previous_lsn, payload)
    }

    pub fn begin_transaction(&self, transaction_id: TransactionId) -> Result<LoggedTransaction> {
        let begin_lsn = self.append(Some(transaction_id), None, WalPayload::Begin)?;
        self.flush_lsn(begin_lsn)?;
        Ok(LoggedTransaction {
            transaction_id,
            begin_lsn,
            last_lsn: begin_lsn,
            page_update_lsns: BTreeMap::new(),
        })
    }

    pub fn log_prepared(
        &self,
        transaction_id: TransactionId,
        prepared: &mut PreparedCommit,
    ) -> Result<LoggedTransaction> {
        let deltas = prepared.page_deltas().to_vec();
        validate_sorted_deltas(&deltas)?;

        let (begin_lsn, mut previous_lsn) = match self.transaction_chain(transaction_id)? {
            Some((begin_lsn, last_lsn)) if begin_lsn == last_lsn => (begin_lsn, last_lsn),
            Some(_) => {
                return Err(DbError::new(
                    "25000",
                    format!("transaction {transaction_id} already has prepared WAL changes"),
                ));
            }
            None => {
                let transaction = self.begin_transaction(transaction_id)?;
                (transaction.begin_lsn, transaction.last_lsn)
            }
        };
        let mut page_update_lsns = BTreeMap::new();
        for delta in deltas {
            let lsn = self.append_prepared_page(
                transaction_id,
                previous_lsn,
                delta.page_id,
                delta.before,
                delta.after,
                prepared,
            )?;
            page_update_lsns.insert(delta.page_id, lsn);
            previous_lsn = lsn;
        }
        if prepared.before_page_count() != prepared.after_page_count() {
            previous_lsn = self.append(
                Some(transaction_id),
                Some(previous_lsn),
                WalPayload::Resize {
                    before_page_count: prepared.before_page_count(),
                    after_page_count: prepared.after_page_count(),
                },
            )?;
        }
        self.flush_lsn(previous_lsn)?;
        Ok(LoggedTransaction {
            transaction_id,
            begin_lsn,
            last_lsn: previous_lsn,
            page_update_lsns,
        })
    }

    pub fn commit(&self, transaction: &LoggedTransaction) -> Result<Lsn> {
        self.commit_transaction_at(transaction.transaction_id, transaction.last_lsn)
    }

    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<Lsn> {
        let (_, last_lsn) = self.transaction_chain(transaction_id)?.ok_or_else(|| {
            DbError::new(
                "25P01",
                format!("transaction {transaction_id} has no WAL Begin record"),
            )
        })?;
        self.commit_transaction_at(transaction_id, last_lsn)
    }

    fn commit_transaction_at(
        &self,
        transaction_id: TransactionId,
        expected_last_lsn: Lsn,
    ) -> Result<Lsn> {
        let current = self.last_lsn(transaction_id)?;
        if current != Some(expected_last_lsn) {
            return Err(corruption(format!(
                "transaction {} WAL chain changed before Commit",
                transaction_id
            )));
        }
        let commit_lsn = self.append(
            Some(transaction_id),
            Some(expected_last_lsn),
            WalPayload::Commit,
        )?;
        self.check_fault(FaultPoint::BeforeCommitFlush)?;
        self.flush_lsn(commit_lsn)?;
        self.check_fault(FaultPoint::AfterCommitFlush)?;
        self.mark_transaction_clean(transaction_id)?;
        Ok(commit_lsn)
    }

    pub fn abort(&self, transaction_id: TransactionId) -> Result<Lsn> {
        let previous_lsn = match self.last_lsn(transaction_id)? {
            Some(previous_lsn) => previous_lsn,
            None => self.append(Some(transaction_id), None, WalPayload::Begin)?,
        };
        let abort_lsn = self.append(Some(transaction_id), Some(previous_lsn), WalPayload::Abort)?;
        self.flush_lsn(abort_lsn)?;
        self.mark_transaction_clean(transaction_id)?;
        Ok(abort_lsn)
    }

    pub fn checkpoint(&self, checkpoint: CheckpointState) -> Result<Lsn> {
        let (begin_lsn, sampled_durable_lsn) = {
            let mut state = self.lock_state()?;
            let mut dirty_pages = state.dirty_pages.clone();
            for (page_id, rec_lsn) in checkpoint.dirty_pages {
                dirty_pages
                    .entry(page_id)
                    .and_modify(|current| *current = (*current).min(rec_lsn))
                    .or_insert(rec_lsn);
            }
            let begin_lsn = append_locked(
                &mut state,
                None,
                None,
                WalPayload::CheckpointBegin(CheckpointBegin {
                    active_transactions: checkpoint.active_transactions,
                    dirty_pages,
                    visibility_horizon: checkpoint.visibility_horizon,
                }),
            )?;
            self.check_fault(FaultPoint::BeforeWalFlush)?;
            sync_locked(&mut state, begin_lsn)?;
            self.check_fault(FaultPoint::AfterWalFlush)?;
            self.check_fault(FaultPoint::AfterCheckpointBeginFlush)?;
            let sampled = checkpoint
                .durable_wal_lsn
                .unwrap_or(begin_lsn)
                .min(state.durable_lsn.unwrap_or(begin_lsn));
            (begin_lsn, sampled)
        };

        self.check_fault(FaultPoint::BeforeCheckpointEndAppend)?;
        let end_lsn = self.append(
            None,
            None,
            WalPayload::CheckpointEnd(CheckpointEnd {
                begin_lsn,
                durable_data_generation: checkpoint.durable_data_generation,
                durable_wal_lsn: sampled_durable_lsn,
                data_file_page_count: checkpoint.data_file_page_count,
            }),
        )?;
        self.flush_lsn(end_lsn)?;
        self.check_fault(FaultPoint::AfterCheckpointEndFlush)?;
        Ok(end_lsn)
    }

    pub fn durable_lsn(&self) -> Result<Option<Lsn>> {
        Ok(self.lock_state()?.durable_lsn)
    }

    pub fn last_lsn(&self, transaction_id: TransactionId) -> Result<Option<Lsn>> {
        Ok(self
            .lock_state()?
            .last_by_transaction
            .get(&transaction_id)
            .copied())
    }

    pub fn last_transaction_id(&self) -> Result<Option<TransactionId>> {
        Ok(self.lock_state()?.max_transaction_id)
    }

    pub fn transaction_outcomes(&self) -> Result<BTreeMap<TransactionId, TransactionOutcome>> {
        Ok(self.lock_state()?.transaction_outcomes.clone())
    }

    pub fn dirty_pages(&self) -> Result<BTreeMap<PageId, Lsn>> {
        Ok(self.lock_state()?.dirty_pages.clone())
    }

    pub fn flush_lsn(&self, lsn: Lsn) -> Result<()> {
        self.check_fault(FaultPoint::BeforeWalFlush)?;
        let mut state = self.lock_state()?;
        sync_locked(&mut state, lsn)?;
        drop(state);
        self.check_fault(FaultPoint::AfterWalFlush)
    }

    pub fn check_fault(&self, point: FaultPoint) -> Result<()> {
        self.fault_injector.check(point)
    }

    pub(crate) fn append_compensation(
        &self,
        transaction_id: TransactionId,
        page_id: PageId,
        mut undo: Option<SlottedPage>,
        resulting_page_count: u64,
        undo_next_lsn: Option<Lsn>,
    ) -> Result<(Lsn, Option<SlottedPage>)> {
        let mut state = self.lock_state()?;
        let previous_lsn = state
            .last_by_transaction
            .get(&transaction_id)
            .copied()
            .ok_or_else(|| corruption("cannot append Compensation for unknown transaction"))?;
        let lsn = state.next_lsn;
        if let Some(page) = undo.as_mut() {
            page.set_lsn(lsn.get());
        }
        let appended = append_locked(
            &mut state,
            Some(transaction_id),
            Some(previous_lsn),
            WalPayload::Compensation {
                page_id,
                undo: undo.clone().map(Box::new),
                resulting_page_count,
                undo_next_lsn,
            },
        )?;
        debug_assert_eq!(appended, lsn);
        self.check_fault(FaultPoint::BeforeWalFlush)?;
        sync_locked(&mut state, lsn)?;
        self.check_fault(FaultPoint::AfterWalFlush)?;
        self.check_fault(FaultPoint::AfterCompensationFlush)?;
        Ok((lsn, undo))
    }

    pub(crate) fn lock_recovery(&self) -> Result<MutexGuard<'_, ()>> {
        self.recovery_lock.lock().map_err(|_| {
            DbError::internal("WAL recovery lock is poisoned")
                .with_hint("restart the process before retrying recovery")
        })
    }

    fn append_prepared_page(
        &self,
        transaction_id: TransactionId,
        previous_lsn: Lsn,
        page_id: PageId,
        before: Option<SlottedPage>,
        mut after: Option<SlottedPage>,
        prepared: &mut PreparedCommit,
    ) -> Result<Lsn> {
        let mut state = self.lock_state()?;
        let lsn = state.next_lsn;
        if let Some(page) = after.as_mut() {
            page.set_lsn(lsn.get());
            prepared.mark_after_lsn(page_id, lsn.get())?;
        }
        append_locked(
            &mut state,
            Some(transaction_id),
            Some(previous_lsn),
            WalPayload::PageUpdate {
                page_id,
                before: before.map(Box::new),
                after: after.map(Box::new),
            },
        )
    }

    fn mark_transaction_clean(&self, transaction_id: TransactionId) -> Result<()> {
        let mut state = self.lock_state()?;
        if let Some(page_ids) = state.transaction_dirty_pages.remove(&transaction_id) {
            for page_id in page_ids {
                state.dirty_pages.remove(&page_id);
            }
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, WalState>> {
        self.state.lock().map_err(|_| {
            DbError::internal("WAL manager lock is poisoned")
                .with_hint("restart the process before retrying durable work")
        })
    }

    fn transaction_chain(&self, transaction_id: TransactionId) -> Result<Option<(Lsn, Lsn)>> {
        let state = self.lock_state()?;
        match (
            state.begin_by_transaction.get(&transaction_id).copied(),
            state.last_by_transaction.get(&transaction_id).copied(),
        ) {
            (Some(begin_lsn), Some(last_lsn))
                if !state.terminal_transactions.contains(&transaction_id) =>
            {
                Ok(Some((begin_lsn, last_lsn)))
            }
            (Some(_), Some(_)) => Err(DbError::new(
                "25000",
                format!("transaction {transaction_id} is already terminal"),
            )),
            (None, None) => Ok(None),
            _ => Err(corruption(format!(
                "transaction {transaction_id} has an incomplete in-memory WAL chain"
            ))),
        }
    }
}

impl DurabilityBarrier for WalManager {
    fn flush_through(&self, page_lsn: u64) -> Result<()> {
        if page_lsn == 0 {
            return Ok(());
        }
        let lsn = Lsn::try_from(page_lsn)?;
        self.flush_lsn(lsn)
    }
}

fn append_locked(
    state: &mut WalState,
    transaction_id: Option<TransactionId>,
    previous_lsn: Option<Lsn>,
    payload: WalPayload,
) -> Result<Lsn> {
    validate_append_chain(state, transaction_id, previous_lsn, &payload)?;
    let lsn = state.next_lsn;
    let next_lsn = lsn.checked_next()?;
    let record = WalRecord::new(lsn, transaction_id, previous_lsn, payload)?;
    let bytes = record.encode()?;
    let original_len = state
        .file
        .seek(SeekFrom::End(0))
        .map_err(|error| io_error("failed to seek to the WAL append position", error))?;
    if let Err(error) = state.file.write_all(&bytes) {
        let _ = state.file.set_len(original_len);
        let _ = state.file.seek(SeekFrom::End(0));
        return Err(io_error("failed to append a WAL record", error));
    }
    state.next_lsn = next_lsn;
    apply_record_state(state, &record, false)?;
    Ok(lsn)
}

fn sync_locked(state: &mut WalState, requested_lsn: Lsn) -> Result<()> {
    let appended_lsn = state
        .next_lsn
        .get()
        .checked_sub(1)
        .and_then(Lsn::new)
        .ok_or_else(|| {
            DbError::new(
                "55000",
                format!("cannot flush WAL through unappended LSN {requested_lsn}"),
            )
        })?;
    if requested_lsn > appended_lsn {
        return Err(DbError::new(
            "55000",
            format!(
                "cannot flush WAL through LSN {requested_lsn}; last appended LSN is {appended_lsn}"
            ),
        ));
    }
    if state
        .durable_lsn
        .is_some_and(|durable| durable >= requested_lsn)
    {
        return Ok(());
    }
    state
        .file
        .sync_all()
        .map_err(|error| io_error("failed to synchronize the WAL file", error))?;
    state.durable_lsn = Some(appended_lsn);
    Ok(())
}

fn scan_file(file: &mut File, repair_tail: bool) -> Result<ScanResult> {
    let file_len = file
        .metadata()
        .map_err(|error| io_error("failed to inspect the WAL file", error))?
        .len();
    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error("failed to seek to the WAL origin", error))?;
    let mut offset = 0_u64;
    let mut records = Vec::new();
    while offset < file_len {
        let remaining = file_len - offset;
        if remaining < WAL_HEADER_LEN as u64 {
            return finish_incomplete_tail(file, repair_tail, file_len, offset, records);
        }
        let mut header_bytes = [0_u8; WAL_HEADER_LEN];
        file.read_exact(&mut header_bytes)
            .map_err(|error| io_error("failed to read a WAL record header", error))?;
        let header = parse_header(&header_bytes)?;
        let total_len = u64::try_from(header.total_len)
            .map_err(|_| corruption("WAL record length exceeds u64"))?;
        if remaining < total_len {
            return finish_incomplete_tail(file, repair_tail, file_len, offset, records);
        }
        let mut bytes = vec![0_u8; header.total_len];
        bytes[..WAL_HEADER_LEN].copy_from_slice(&header_bytes);
        file.read_exact(&mut bytes[WAL_HEADER_LEN..])
            .map_err(|error| io_error("failed to read a complete WAL record", error))?;
        records.push(WalRecord::from_bytes(&bytes)?);
        offset = offset
            .checked_add(total_len)
            .ok_or_else(|| corruption("WAL file offset overflow"))?;
    }
    validate_record_sequence(&records)?;
    Ok(ScanResult {
        records,
        valid_bytes: offset,
        truncated_bytes: 0,
    })
}

fn finish_incomplete_tail(
    file: &mut File,
    repair_tail: bool,
    file_len: u64,
    valid_bytes: u64,
    records: Vec<WalRecord>,
) -> Result<ScanResult> {
    validate_record_sequence(&records)?;
    if !repair_tail {
        return Err(corruption(format!(
            "WAL contains an incomplete final record at byte {valid_bytes}"
        )));
    }
    file.set_len(valid_bytes)
        .map_err(|error| io_error("failed to truncate an incomplete WAL tail", error))?;
    file.sync_all()
        .map_err(|error| io_error("failed to synchronize repaired WAL length", error))?;
    file.seek(SeekFrom::End(0))
        .map_err(|error| io_error("failed to seek after WAL tail repair", error))?;
    Ok(ScanResult {
        records,
        valid_bytes,
        truncated_bytes: file_len - valid_bytes,
    })
}

fn validate_record_sequence(records: &[WalRecord]) -> Result<()> {
    let mut previous_lsn = None;
    let mut last_by_transaction = BTreeMap::new();
    let mut terminal_transactions = BTreeSet::new();
    let mut max_transaction_id = None;
    for record in records {
        if previous_lsn.is_some_and(|previous| record.lsn() <= previous) {
            return Err(corruption("WAL LSNs are not strictly increasing"));
        }
        previous_lsn = Some(record.lsn());
        let Some(transaction_id) = record.transaction_id() else {
            continue;
        };
        match record.kind() {
            RecordKind::Begin => {
                if last_by_transaction.contains_key(&transaction_id) {
                    return Err(corruption(format!(
                        "transaction {transaction_id} has more than one Begin record"
                    )));
                }
                if max_transaction_id.is_some_and(|maximum| transaction_id <= maximum) {
                    return Err(corruption(
                        "WAL transaction IDs are not strictly increasing",
                    ));
                }
                max_transaction_id = Some(transaction_id);
            }
            _ => {
                if terminal_transactions.contains(&transaction_id) {
                    return Err(corruption(format!(
                        "transaction {transaction_id} has records after its terminal record"
                    )));
                }
                if record.previous_lsn() != last_by_transaction.get(&transaction_id).copied() {
                    return Err(corruption(format!(
                        "transaction {transaction_id} previous-LSN chain is invalid"
                    )));
                }
            }
        }
        last_by_transaction.insert(transaction_id, record.lsn());
        if matches!(record.kind(), RecordKind::Commit | RecordKind::Abort) {
            terminal_transactions.insert(transaction_id);
        }
    }
    Ok(())
}

fn validate_append_chain(
    state: &WalState,
    transaction_id: Option<TransactionId>,
    previous_lsn: Option<Lsn>,
    payload: &WalPayload,
) -> Result<()> {
    let Some(transaction_id) = transaction_id else {
        return Ok(());
    };
    if matches!(payload, WalPayload::Begin) {
        if state.last_by_transaction.contains_key(&transaction_id) {
            return Err(corruption(format!(
                "transaction {transaction_id} already has a WAL chain"
            )));
        }
        if state
            .max_transaction_id
            .is_some_and(|maximum| transaction_id <= maximum)
        {
            return Err(corruption(format!(
                "transaction {transaction_id} is not greater than the last WAL transaction ID"
            )));
        }
        return Ok(());
    }
    if state.terminal_transactions.contains(&transaction_id) {
        return Err(corruption(format!(
            "transaction {transaction_id} is already terminal"
        )));
    }
    if state.last_by_transaction.get(&transaction_id).copied() != previous_lsn {
        return Err(corruption(format!(
            "transaction {transaction_id} previous-LSN chain changed"
        )));
    }
    Ok(())
}

fn apply_record_state(
    state: &mut WalState,
    record: &WalRecord,
    clear_terminal: bool,
) -> Result<()> {
    let Some(transaction_id) = record.transaction_id() else {
        return Ok(());
    };
    if matches!(record.kind(), RecordKind::Begin) {
        state
            .begin_by_transaction
            .insert(transaction_id, record.lsn());
        state.max_transaction_id = Some(
            state
                .max_transaction_id
                .map_or(transaction_id, |current| current.max(transaction_id)),
        );
        state
            .transaction_outcomes
            .insert(transaction_id, TransactionOutcome::InProgress);
    }
    state
        .last_by_transaction
        .insert(transaction_id, record.lsn());
    match record.payload() {
        WalPayload::PageUpdate { page_id, .. } | WalPayload::Compensation { page_id, .. } => {
            state.dirty_pages.entry(*page_id).or_insert(record.lsn());
            state
                .transaction_dirty_pages
                .entry(transaction_id)
                .or_default()
                .insert(*page_id);
        }
        WalPayload::Commit | WalPayload::Abort => {
            state.terminal_transactions.insert(transaction_id);
            state.transaction_outcomes.insert(
                transaction_id,
                if matches!(record.payload(), WalPayload::Commit) {
                    TransactionOutcome::Committed
                } else {
                    TransactionOutcome::Aborted
                },
            );
            if clear_terminal
                && let Some(page_ids) = state.transaction_dirty_pages.remove(&transaction_id)
            {
                for page_id in page_ids {
                    state.dirty_pages.remove(&page_id);
                }
            }
        }
        WalPayload::Begin
        | WalPayload::Resize { .. }
        | WalPayload::CheckpointBegin(_)
        | WalPayload::CheckpointEnd(_) => {}
    }
    Ok(())
}

fn validate_sorted_deltas(deltas: &[ordadb_storage::PageDelta]) -> Result<()> {
    let mut previous = None;
    for delta in deltas {
        if previous.is_some_and(|page_id| delta.page_id <= page_id) {
            return Err(corruption(
                "prepared page deltas must be sorted by unique page ID",
            ));
        }
        previous = Some(delta.page_id);
    }
    Ok(())
}
