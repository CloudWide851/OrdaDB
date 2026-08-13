use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::path::Path;

use ordadb_storage::{DatabaseStore, PageId, RecoveryDataFile, RecoveryPlan, SlottedPage};
use ordadb_types::{DbError, Result};

use crate::record::CheckpointBegin;
use crate::{
    CheckpointState, Lsn, RecordKind, TransactionId, WalManager, WalPayload, WalRecord, corruption,
};

#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub scanned_records: usize,
    pub truncated_tail_bytes: u64,
    pub analysis_start_lsn: Option<Lsn>,
    pub redo_start_lsn: Option<Lsn>,
    pub checkpoint_begin_lsn: Option<Lsn>,
    pub checkpoint_end_lsn: Option<Lsn>,
    pub recovery_checkpoint_lsn: Option<Lsn>,
    pub winners: BTreeSet<TransactionId>,
    pub losers: BTreeSet<TransactionId>,
    pub dirty_pages: BTreeMap<PageId, Lsn>,
    pub redone_page_records: usize,
    pub redone_resize_records: usize,
    pub undone_records: usize,
    pub compensation_records: usize,
    pub final_page_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionStatus {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy)]
struct TransactionAnalysis {
    last_lsn: Lsn,
    status: TransactionStatus,
}

#[derive(Debug)]
struct CompleteCheckpoint {
    begin_lsn: Lsn,
    end_lsn: Lsn,
    begin: CheckpointBegin,
}

#[derive(Debug)]
struct Analysis {
    records_by_lsn: BTreeMap<Lsn, WalRecord>,
    transactions: BTreeMap<TransactionId, TransactionAnalysis>,
    dirty_pages: BTreeMap<PageId, Lsn>,
    checkpoint: Option<CompleteCheckpoint>,
    redo_start_lsn: Option<Lsn>,
}

impl WalManager {
    pub fn recover(&self, data_dir: &Path) -> Result<RecoveryReport> {
        let _recovery = self.lock_recovery()?;
        let scan = self.scan()?;
        let mut report = RecoveryReport {
            scanned_records: scan.records.len(),
            truncated_tail_bytes: self.startup_truncated_bytes(),
            ..RecoveryReport::default()
        };
        if scan.records.is_empty() {
            return Ok(report);
        }

        let analysis = analyze(&scan.records)?;
        let (winners, losers) = transaction_outcomes(&analysis.transactions);
        report.winners = winners;
        report.losers = losers.clone();
        report.dirty_pages = analysis.dirty_pages.clone();
        report.redo_start_lsn = analysis.redo_start_lsn;
        if let Some(checkpoint) = &analysis.checkpoint {
            report.analysis_start_lsn = Some(checkpoint.begin_lsn);
            report.checkpoint_begin_lsn = Some(checkpoint.begin_lsn);
            report.checkpoint_end_lsn = Some(checkpoint.end_lsn);
        } else {
            report.analysis_start_lsn = scan.records.first().map(WalRecord::lsn);
        }

        let file_state = RecoveryDataFile::inspect(data_dir)?;
        let final_page_count = final_page_count(
            file_state.logical_page_count(),
            &scan.records,
            &analysis.transactions,
        )?;
        let recovery_pages = recovery_page_ids(&scan.records);
        let plan = RecoveryPlan::new(final_page_count, recovery_pages);
        let mut data_file = RecoveryDataFile::open(data_dir, plan)?;
        let mut current_page_count = file_state.logical_page_count();

        if let Some(redo_start_lsn) = analysis.redo_start_lsn {
            for record in scan
                .records
                .iter()
                .filter(|record| record.lsn() >= redo_start_lsn)
            {
                redo_record(
                    self,
                    &mut data_file,
                    record,
                    &mut current_page_count,
                    &mut report,
                )?;
            }
        }

        undo_losers(
            self,
            &mut data_file,
            &analysis,
            &losers,
            &mut current_page_count,
            &mut report,
        )?;
        if current_page_count != final_page_count {
            self.check_fault(crate::FaultPoint::BeforeDataResize)?;
            data_file.resize_pages(final_page_count)?;
            self.check_fault(crate::FaultPoint::AfterDataResize)?;
        }
        self.check_fault(crate::FaultPoint::BeforeDataSync)?;
        data_file.sync_all()?;
        self.check_fault(crate::FaultPoint::AfterDataSync)?;
        data_file.finish()?;
        let durable_data_generation = DatabaseStore::open_read_only(data_dir)?
            .committed_state()
            .generation;

        let checkpoint_lsn = self.checkpoint(CheckpointState {
            active_transactions: BTreeMap::new(),
            dirty_pages: BTreeMap::new(),
            visibility_horizon: None,
            durable_data_generation,
            durable_wal_lsn: self.durable_lsn()?,
            data_file_page_count: final_page_count,
        })?;
        report.recovery_checkpoint_lsn = Some(checkpoint_lsn);
        report.final_page_count = final_page_count;
        Ok(report)
    }
}

fn analyze(records: &[WalRecord]) -> Result<Analysis> {
    let checkpoint = find_last_complete_checkpoint(records)?;
    let mut records_by_lsn = BTreeMap::new();
    let mut transactions = BTreeMap::new();
    for record in records {
        if records_by_lsn
            .insert(record.lsn(), record.clone())
            .is_some()
        {
            return Err(corruption(format!(
                "WAL contains duplicate LSN {}",
                record.lsn()
            )));
        }
        let Some(transaction_id) = record.transaction_id() else {
            continue;
        };
        match record.kind() {
            RecordKind::Begin => {
                transactions.insert(
                    transaction_id,
                    TransactionAnalysis {
                        last_lsn: record.lsn(),
                        status: TransactionStatus::Active,
                    },
                );
            }
            RecordKind::Commit => {
                update_transaction(
                    &mut transactions,
                    transaction_id,
                    record.lsn(),
                    TransactionStatus::Committed,
                )?;
            }
            RecordKind::Abort => {
                update_transaction(
                    &mut transactions,
                    transaction_id,
                    record.lsn(),
                    TransactionStatus::Aborted,
                )?;
            }
            RecordKind::PageUpdate | RecordKind::Resize | RecordKind::Compensation => {
                update_transaction(
                    &mut transactions,
                    transaction_id,
                    record.lsn(),
                    TransactionStatus::Active,
                )?;
            }
            RecordKind::CheckpointBegin | RecordKind::CheckpointEnd => {
                return Err(corruption(
                    "checkpoint record unexpectedly has a transaction ID",
                ));
            }
        }
    }
    validate_checkpoint_references(checkpoint.as_ref(), &records_by_lsn)?;

    let mut dirty_pages = checkpoint
        .as_ref()
        .map_or_else(BTreeMap::new, |checkpoint| {
            checkpoint.begin.dirty_pages.clone()
        });
    let analysis_start = checkpoint.as_ref().map(|checkpoint| checkpoint.begin_lsn);
    for record in records
        .iter()
        .filter(|record| analysis_start.is_none_or(|analysis_start| record.lsn() > analysis_start))
    {
        match record.payload() {
            WalPayload::PageUpdate { page_id, .. } | WalPayload::Compensation { page_id, .. } => {
                dirty_pages.entry(*page_id).or_insert(record.lsn());
            }
            WalPayload::Begin
            | WalPayload::Resize { .. }
            | WalPayload::Commit
            | WalPayload::Abort
            | WalPayload::CheckpointBegin(_)
            | WalPayload::CheckpointEnd(_) => {}
        }
    }
    let redo_start_lsn = dirty_pages.values().copied().min();
    Ok(Analysis {
        records_by_lsn,
        transactions,
        dirty_pages,
        checkpoint,
        redo_start_lsn,
    })
}

fn update_transaction(
    transactions: &mut BTreeMap<TransactionId, TransactionAnalysis>,
    transaction_id: TransactionId,
    lsn: Lsn,
    status: TransactionStatus,
) -> Result<()> {
    let transaction = transactions.get_mut(&transaction_id).ok_or_else(|| {
        corruption(format!(
            "transaction {transaction_id} has a WAL record without Begin"
        ))
    })?;
    transaction.last_lsn = lsn;
    transaction.status = status;
    Ok(())
}

fn find_last_complete_checkpoint(records: &[WalRecord]) -> Result<Option<CompleteCheckpoint>> {
    let mut begins = BTreeMap::new();
    let mut complete = None;
    for record in records {
        match record.payload() {
            WalPayload::CheckpointBegin(begin) => {
                begins.insert(record.lsn(), begin.clone());
            }
            WalPayload::CheckpointEnd(end) => {
                if let Some(begin) = begins.get(&end.begin_lsn) {
                    complete = Some(CompleteCheckpoint {
                        begin_lsn: end.begin_lsn,
                        end_lsn: record.lsn(),
                        begin: begin.clone(),
                    });
                }
            }
            WalPayload::Begin
            | WalPayload::PageUpdate { .. }
            | WalPayload::Resize { .. }
            | WalPayload::Commit
            | WalPayload::Abort
            | WalPayload::Compensation { .. } => {}
        }
    }
    Ok(complete)
}

fn validate_checkpoint_references(
    checkpoint: Option<&CompleteCheckpoint>,
    records: &BTreeMap<Lsn, WalRecord>,
) -> Result<()> {
    let Some(checkpoint) = checkpoint else {
        return Ok(());
    };
    for (transaction_id, last_lsn) in &checkpoint.begin.active_transactions {
        let record = records.get(last_lsn).ok_or_else(|| {
            corruption(format!(
                "checkpoint references missing transaction LSN {last_lsn}"
            ))
        })?;
        if record.transaction_id() != Some(*transaction_id) {
            return Err(corruption(
                "checkpoint active transaction references another transaction's LSN",
            ));
        }
    }
    for (page_id, rec_lsn) in &checkpoint.begin.dirty_pages {
        let record = records
            .get(rec_lsn)
            .ok_or_else(|| corruption(format!("checkpoint references missing recLSN {rec_lsn}")))?;
        let referenced_page_id = match record.payload() {
            WalPayload::PageUpdate { page_id, .. } | WalPayload::Compensation { page_id, .. } => {
                *page_id
            }
            _ => {
                return Err(corruption(
                    "checkpoint dirty-page recLSN is not a physical page record",
                ));
            }
        };
        if referenced_page_id != *page_id {
            return Err(corruption(
                "checkpoint dirty-page recLSN references a different page",
            ));
        }
    }
    Ok(())
}

fn transaction_outcomes(
    transactions: &BTreeMap<TransactionId, TransactionAnalysis>,
) -> (BTreeSet<TransactionId>, BTreeSet<TransactionId>) {
    let mut winners = BTreeSet::new();
    let mut losers = BTreeSet::new();
    for (transaction_id, transaction) in transactions {
        match transaction.status {
            TransactionStatus::Committed => {
                winners.insert(*transaction_id);
            }
            TransactionStatus::Active => {
                losers.insert(*transaction_id);
            }
            TransactionStatus::Aborted => {}
        }
    }
    (winners, losers)
}

fn final_page_count(
    inspected_page_count: u64,
    records: &[WalRecord],
    transactions: &BTreeMap<TransactionId, TransactionAnalysis>,
) -> Result<u64> {
    let mut final_page_count = inspected_page_count;
    for record in records {
        let WalPayload::Resize {
            before_page_count,
            after_page_count,
        } = record.payload()
        else {
            continue;
        };
        let transaction_id = record
            .transaction_id()
            .ok_or_else(|| corruption("Resize record is missing its transaction identity"))?;
        let transaction = transactions
            .get(&transaction_id)
            .ok_or_else(|| corruption("Resize record references an unknown transaction"))?;
        final_page_count = if transaction.status == TransactionStatus::Committed {
            *after_page_count
        } else {
            *before_page_count
        };
    }
    if final_page_count == 0 {
        return Err(corruption(
            "recovery resolved a zero-page database without a metadata page",
        ));
    }
    Ok(final_page_count)
}

fn recovery_page_ids(records: &[WalRecord]) -> BTreeSet<PageId> {
    records
        .iter()
        .filter_map(|record| match record.payload() {
            WalPayload::PageUpdate { page_id, .. } | WalPayload::Compensation { page_id, .. } => {
                Some(*page_id)
            }
            _ => None,
        })
        .collect()
}

fn redo_record(
    wal: &WalManager,
    data_file: &mut RecoveryDataFile,
    record: &WalRecord,
    current_page_count: &mut u64,
    report: &mut RecoveryReport,
) -> Result<()> {
    match record.payload() {
        WalPayload::PageUpdate { page_id, after, .. } => {
            if let Some(after) = after
                && page_needs_redo(data_file, *page_id, record.lsn())?
            {
                wal.check_fault(crate::FaultPoint::BeforeDataPageWrite)?;
                data_file.apply_page(after)?;
                wal.check_fault(crate::FaultPoint::AfterDataPageWrite)?;
                *current_page_count = (*current_page_count).max(next_page_count(*page_id)?);
                report.redone_page_records = report.redone_page_records.saturating_add(1);
            }
        }
        WalPayload::Resize {
            after_page_count, ..
        } => {
            wal.check_fault(crate::FaultPoint::BeforeDataResize)?;
            data_file.resize_pages(*after_page_count)?;
            wal.check_fault(crate::FaultPoint::AfterDataResize)?;
            *current_page_count = *after_page_count;
            report.redone_resize_records = report.redone_resize_records.saturating_add(1);
        }
        WalPayload::Compensation {
            page_id,
            undo,
            resulting_page_count,
            ..
        } => {
            apply_compensation(
                wal,
                data_file,
                record.lsn(),
                *page_id,
                undo.as_deref(),
                *resulting_page_count,
                current_page_count,
            )?;
            report.redone_page_records = report.redone_page_records.saturating_add(1);
        }
        WalPayload::Begin
        | WalPayload::Commit
        | WalPayload::Abort
        | WalPayload::CheckpointBegin(_)
        | WalPayload::CheckpointEnd(_) => {}
    }
    Ok(())
}

fn page_needs_redo(
    data_file: &mut RecoveryDataFile,
    page_id: PageId,
    record_lsn: Lsn,
) -> Result<bool> {
    Ok(data_file
        .read_page(page_id)?
        .is_none_or(|page| page.lsn() < record_lsn.get()))
}

fn apply_compensation(
    wal: &WalManager,
    data_file: &mut RecoveryDataFile,
    lsn: Lsn,
    page_id: PageId,
    undo: Option<&SlottedPage>,
    resulting_page_count: u64,
    current_page_count: &mut u64,
) -> Result<()> {
    wal.check_fault(crate::FaultPoint::BeforeCompensationApply)?;
    if resulting_page_count > *current_page_count {
        wal.check_fault(crate::FaultPoint::BeforeDataResize)?;
        data_file.resize_pages(resulting_page_count)?;
        wal.check_fault(crate::FaultPoint::AfterDataResize)?;
        *current_page_count = resulting_page_count;
    }
    if let Some(undo) = undo
        && page_needs_redo(data_file, page_id, lsn)?
    {
        wal.check_fault(crate::FaultPoint::BeforeDataPageWrite)?;
        data_file.apply_page(undo)?;
        wal.check_fault(crate::FaultPoint::AfterDataPageWrite)?;
        *current_page_count = (*current_page_count).max(next_page_count(page_id)?);
    }
    if resulting_page_count < *current_page_count {
        wal.check_fault(crate::FaultPoint::BeforeDataResize)?;
        data_file.resize_pages(resulting_page_count)?;
        wal.check_fault(crate::FaultPoint::AfterDataResize)?;
        *current_page_count = resulting_page_count;
    }
    Ok(())
}

fn undo_losers(
    wal: &WalManager,
    data_file: &mut RecoveryDataFile,
    analysis: &Analysis,
    losers: &BTreeSet<TransactionId>,
    current_page_count: &mut u64,
    report: &mut RecoveryReport,
) -> Result<()> {
    let mut queue = BinaryHeap::new();
    for transaction_id in losers {
        let last_lsn = analysis
            .transactions
            .get(transaction_id)
            .ok_or_else(|| corruption("loser transaction disappeared during undo"))?
            .last_lsn;
        queue.push((last_lsn, Reverse(*transaction_id)));
    }

    while let Some((lsn, Reverse(transaction_id))) = queue.pop() {
        let record = analysis
            .records_by_lsn
            .get(&lsn)
            .ok_or_else(|| corruption(format!("undo chain references missing LSN {lsn}")))?;
        let next_undo_lsn = match record.payload() {
            WalPayload::Begin => {
                wal.abort(transaction_id)?;
                None
            }
            WalPayload::PageUpdate {
                page_id, before, ..
            } => {
                let undo_next_lsn = record.previous_lsn();
                let (_, undo) = wal.append_compensation(
                    transaction_id,
                    *page_id,
                    before.as_deref().cloned(),
                    *current_page_count,
                    undo_next_lsn,
                )?;
                wal.check_fault(crate::FaultPoint::BeforeCompensationApply)?;
                if let Some(undo) = undo.as_ref() {
                    wal.check_fault(crate::FaultPoint::BeforeDataPageWrite)?;
                    data_file.apply_page(undo)?;
                    wal.check_fault(crate::FaultPoint::AfterDataPageWrite)?;
                    *current_page_count = (*current_page_count).max(next_page_count(*page_id)?);
                }
                report.undone_records = report.undone_records.saturating_add(1);
                report.compensation_records = report.compensation_records.saturating_add(1);
                undo_next_lsn
            }
            WalPayload::Resize {
                before_page_count, ..
            } => {
                let undo_next_lsn = record.previous_lsn();
                let marker_page = PageId::new(before_page_count.saturating_sub(1));
                wal.append_compensation(
                    transaction_id,
                    marker_page,
                    None,
                    *before_page_count,
                    undo_next_lsn,
                )?;
                wal.check_fault(crate::FaultPoint::BeforeCompensationApply)?;
                wal.check_fault(crate::FaultPoint::BeforeDataResize)?;
                data_file.resize_pages(*before_page_count)?;
                wal.check_fault(crate::FaultPoint::AfterDataResize)?;
                *current_page_count = *before_page_count;
                report.undone_records = report.undone_records.saturating_add(1);
                report.compensation_records = report.compensation_records.saturating_add(1);
                undo_next_lsn
            }
            WalPayload::Compensation { undo_next_lsn, .. } => *undo_next_lsn,
            WalPayload::Commit | WalPayload::Abort => {
                return Err(corruption(
                    "winner or aborted transaction entered the loser undo queue",
                ));
            }
            WalPayload::CheckpointBegin(_) | WalPayload::CheckpointEnd(_) => {
                return Err(corruption(
                    "transaction undo chain references a checkpoint record",
                ));
            }
        };
        if let Some(next_undo_lsn) = next_undo_lsn {
            queue.push((next_undo_lsn, Reverse(transaction_id)));
        }
    }
    Ok(())
}

fn next_page_count(page_id: PageId) -> Result<u64> {
    page_id
        .get()
        .checked_add(1)
        .ok_or_else(|| DbError::new("54000", "page ID space is exhausted during recovery"))
}
