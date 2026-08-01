use std::collections::BTreeMap;

use ordadb_storage::{PAGE_SIZE, PageId, SlottedPage};
use ordadb_types::{DbError, Result};

use crate::{Lsn, TransactionId, corruption, unsupported_version};

pub const WAL_MAGIC: [u8; 8] = *b"ORDAWAL1";
pub const WAL_FORMAT_VERSION: u16 = 1;
pub const WAL_HEADER_LEN: usize = 48;

const CHECKSUM_OFFSET: usize = 44;
const PAGE_UPDATE_PAYLOAD_LEN: usize = 8 + 1 + PAGE_SIZE + 1 + PAGE_SIZE;
pub const WAL_MAX_RECORD_LEN: usize = WAL_HEADER_LEN + PAGE_UPDATE_PAYLOAD_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    Begin = 1,
    PageUpdate = 2,
    Resize = 3,
    Commit = 4,
    Abort = 5,
    Compensation = 6,
    CheckpointBegin = 7,
    CheckpointEnd = 8,
}

impl TryFrom<u8> for RecordKind {
    type Error = DbError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Begin),
            2 => Ok(Self::PageUpdate),
            3 => Ok(Self::Resize),
            4 => Ok(Self::Commit),
            5 => Ok(Self::Abort),
            6 => Ok(Self::Compensation),
            7 => Ok(Self::CheckpointBegin),
            8 => Ok(Self::CheckpointEnd),
            _ => Err(corruption(format!("unknown WAL record kind {value}"))),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CheckpointBegin {
    pub active_transactions: BTreeMap<TransactionId, Lsn>,
    pub dirty_pages: BTreeMap<PageId, Lsn>,
    pub visibility_horizon: Option<TransactionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointEnd {
    pub begin_lsn: Lsn,
    pub durable_data_generation: u64,
    pub durable_wal_lsn: Lsn,
    pub data_file_page_count: u64,
}

#[derive(Debug, Clone)]
pub enum WalPayload {
    Begin,
    PageUpdate {
        page_id: PageId,
        before: Option<Box<SlottedPage>>,
        after: Option<Box<SlottedPage>>,
    },
    Resize {
        before_page_count: u64,
        after_page_count: u64,
    },
    Commit,
    Abort,
    Compensation {
        page_id: PageId,
        undo: Option<Box<SlottedPage>>,
        resulting_page_count: u64,
        undo_next_lsn: Option<Lsn>,
    },
    CheckpointBegin(CheckpointBegin),
    CheckpointEnd(CheckpointEnd),
}

impl WalPayload {
    #[must_use]
    pub const fn kind(&self) -> RecordKind {
        match self {
            Self::Begin => RecordKind::Begin,
            Self::PageUpdate { .. } => RecordKind::PageUpdate,
            Self::Resize { .. } => RecordKind::Resize,
            Self::Commit => RecordKind::Commit,
            Self::Abort => RecordKind::Abort,
            Self::Compensation { .. } => RecordKind::Compensation,
            Self::CheckpointBegin(_) => RecordKind::CheckpointBegin,
            Self::CheckpointEnd(_) => RecordKind::CheckpointEnd,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalRecord {
    lsn: Lsn,
    transaction_id: Option<TransactionId>,
    previous_lsn: Option<Lsn>,
    payload: WalPayload,
}

impl WalRecord {
    pub fn new(
        lsn: Lsn,
        transaction_id: Option<TransactionId>,
        previous_lsn: Option<Lsn>,
        payload: WalPayload,
    ) -> Result<Self> {
        let record = Self {
            lsn,
            transaction_id,
            previous_lsn,
            payload,
        };
        record.validate()?;
        Ok(record)
    }

    #[must_use]
    pub const fn lsn(&self) -> Lsn {
        self.lsn
    }

    #[must_use]
    pub const fn transaction_id(&self) -> Option<TransactionId> {
        self.transaction_id
    }

    #[must_use]
    pub const fn previous_lsn(&self) -> Option<Lsn> {
        self.previous_lsn
    }

    #[must_use]
    pub const fn kind(&self) -> RecordKind {
        self.payload.kind()
    }

    #[must_use]
    pub const fn payload(&self) -> &WalPayload {
        &self.payload
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let payload = encode_payload(self.lsn, &self.payload)?;
        let total_len = WAL_HEADER_LEN
            .checked_add(payload.len())
            .ok_or_else(|| corruption("WAL record length overflow"))?;
        if total_len > WAL_MAX_RECORD_LEN {
            return Err(corruption(format!(
                "WAL record length {total_len} exceeds maximum {WAL_MAX_RECORD_LEN}"
            )));
        }
        let total_len_u32 = u32::try_from(total_len)
            .map_err(|_| corruption("WAL record length does not fit its header"))?;
        let payload_len_u32 = u32::try_from(payload.len())
            .map_err(|_| corruption("WAL payload length does not fit its header"))?;

        let mut bytes = vec![0_u8; total_len];
        bytes[0..8].copy_from_slice(&WAL_MAGIC);
        write_u16(&mut bytes, 8, WAL_FORMAT_VERSION);
        bytes[10] = self.kind() as u8;
        bytes[11] = 0;
        write_u32(&mut bytes, 12, total_len_u32);
        write_u64(&mut bytes, 16, self.lsn.get());
        write_u64(
            &mut bytes,
            24,
            self.transaction_id.map_or(0, TransactionId::get),
        );
        write_u64(&mut bytes, 32, self.previous_lsn.map_or(0, Lsn::get));
        write_u32(&mut bytes, 40, payload_len_u32);
        bytes[WAL_HEADER_LEN..].copy_from_slice(&payload);
        let checksum = crc32c::crc32c(&bytes);
        write_u32(&mut bytes, CHECKSUM_OFFSET, checksum);
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let header = parse_header(bytes)?;
        if bytes.len() != header.total_len {
            return Err(corruption(format!(
                "complete WAL record has length {}, header declares {}",
                bytes.len(),
                header.total_len
            )));
        }
        let expected_checksum = header.checksum;
        let mut checksummed = bytes.to_vec();
        write_u32(&mut checksummed, CHECKSUM_OFFSET, 0);
        let actual_checksum = crc32c::crc32c(&checksummed);
        if actual_checksum != expected_checksum {
            return Err(corruption(format!(
                "WAL record {} checksum mismatch",
                header.lsn
            )));
        }
        let payload = decode_payload(
            header.kind,
            header.lsn,
            &bytes[WAL_HEADER_LEN..header.total_len],
        )?;
        Self::new(
            header.lsn,
            header.transaction_id,
            header.previous_lsn,
            payload,
        )
    }

    fn validate(&self) -> Result<()> {
        let system_record = matches!(
            self.payload,
            WalPayload::CheckpointBegin(_) | WalPayload::CheckpointEnd(_)
        );
        if system_record != self.transaction_id.is_none() {
            return Err(corruption(
                "transaction ID must be zero only for checkpoint records",
            ));
        }
        if system_record && self.previous_lsn.is_some() {
            return Err(corruption(
                "checkpoint records must not have a transaction previous LSN",
            ));
        }
        if matches!(self.payload, WalPayload::Begin) && self.previous_lsn.is_some() {
            return Err(corruption("Begin record must not have a previous LSN"));
        }
        if !system_record
            && !matches!(self.payload, WalPayload::Begin)
            && self.previous_lsn.is_none()
        {
            return Err(corruption(format!(
                "{:?} record must have a previous transaction LSN",
                self.kind()
            )));
        }
        if self
            .previous_lsn
            .is_some_and(|previous| previous >= self.lsn)
        {
            return Err(corruption(
                "transaction previous LSN must precede the current record",
            ));
        }

        match &self.payload {
            WalPayload::PageUpdate {
                page_id,
                before,
                after,
            } => {
                if before.is_none() && after.is_none() {
                    return Err(corruption(
                        "PageUpdate must contain a before or after image",
                    ));
                }
                validate_page_image(*page_id, before.as_deref(), None)?;
                validate_page_image(*page_id, after.as_deref(), Some(self.lsn))?;
            }
            WalPayload::Resize {
                before_page_count,
                after_page_count,
            } if before_page_count == after_page_count => {
                return Err(corruption(
                    "Resize before and after page counts must differ",
                ));
            }
            WalPayload::Compensation { page_id, undo, .. } => {
                validate_page_image(*page_id, undo.as_deref(), Some(self.lsn))?;
            }
            WalPayload::CheckpointBegin(begin) => {
                if begin
                    .active_transactions
                    .values()
                    .chain(begin.dirty_pages.values())
                    .any(|lsn| *lsn >= self.lsn)
                {
                    return Err(corruption(
                        "checkpoint state references an LSN at or after its Begin",
                    ));
                }
            }
            WalPayload::CheckpointEnd(end) => {
                if end.begin_lsn >= self.lsn {
                    return Err(corruption(
                        "CheckpointEnd begin LSN must precede the End record",
                    ));
                }
                if end.durable_wal_lsn > self.lsn {
                    return Err(corruption(
                        "CheckpointEnd durable WAL LSN is beyond the End record",
                    ));
                }
            }
            WalPayload::Begin
            | WalPayload::Resize { .. }
            | WalPayload::Commit
            | WalPayload::Abort => {}
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ScanResult {
    pub records: Vec<WalRecord>,
    pub valid_bytes: u64,
    pub truncated_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordHeader {
    pub kind: RecordKind,
    pub total_len: usize,
    pub lsn: Lsn,
    pub transaction_id: Option<TransactionId>,
    pub previous_lsn: Option<Lsn>,
    pub checksum: u32,
}

pub(crate) fn parse_header(bytes: &[u8]) -> Result<RecordHeader> {
    if bytes.len() < WAL_HEADER_LEN {
        return Err(corruption("WAL record header is incomplete"));
    }
    if bytes[0..8] != WAL_MAGIC {
        return Err(corruption("WAL record has an invalid magic value"));
    }
    let version = read_u16(bytes, 8);
    if version != WAL_FORMAT_VERSION {
        return Err(unsupported_version(version));
    }
    let kind = RecordKind::try_from(bytes[10])?;
    if bytes[11] != 0 {
        return Err(corruption("WAL v1 record flags must be zero"));
    }
    let total_len = usize::try_from(read_u32(bytes, 12))
        .map_err(|_| corruption("WAL record length exceeds this platform"))?;
    if !(WAL_HEADER_LEN..=WAL_MAX_RECORD_LEN).contains(&total_len) {
        return Err(corruption(format!(
            "WAL record length {total_len} is outside [{WAL_HEADER_LEN}, {WAL_MAX_RECORD_LEN}]"
        )));
    }
    let payload_len = usize::try_from(read_u32(bytes, 40))
        .map_err(|_| corruption("WAL payload length exceeds this platform"))?;
    if payload_len != total_len - WAL_HEADER_LEN {
        return Err(corruption(
            "WAL payload length does not match the total record length",
        ));
    }
    let lsn = Lsn::try_from(read_u64(bytes, 16))?;
    let transaction_id = optional_transaction_id(read_u64(bytes, 24))?;
    let previous_lsn = optional_lsn(read_u64(bytes, 32))?;
    Ok(RecordHeader {
        kind,
        total_len,
        lsn,
        transaction_id,
        previous_lsn,
        checksum: read_u32(bytes, CHECKSUM_OFFSET),
    })
}

fn encode_payload(lsn: Lsn, payload: &WalPayload) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    match payload {
        WalPayload::Begin | WalPayload::Commit | WalPayload::Abort => {}
        WalPayload::PageUpdate {
            page_id,
            before,
            after,
        } => {
            push_u64(&mut bytes, page_id.get());
            push_page(&mut bytes, before.as_deref());
            push_page(&mut bytes, after.as_deref());
        }
        WalPayload::Resize {
            before_page_count,
            after_page_count,
        } => {
            push_u64(&mut bytes, *before_page_count);
            push_u64(&mut bytes, *after_page_count);
        }
        WalPayload::Compensation {
            page_id,
            undo,
            resulting_page_count,
            undo_next_lsn,
        } => {
            push_u64(&mut bytes, page_id.get());
            push_page(&mut bytes, undo.as_deref());
            push_u64(&mut bytes, *resulting_page_count);
            push_u64(&mut bytes, undo_next_lsn.map_or(0, Lsn::get));
        }
        WalPayload::CheckpointBegin(begin) => {
            push_count(&mut bytes, begin.active_transactions.len())?;
            for (transaction_id, last_lsn) in &begin.active_transactions {
                push_u64(&mut bytes, transaction_id.get());
                push_u64(&mut bytes, last_lsn.get());
            }
            push_count(&mut bytes, begin.dirty_pages.len())?;
            for (page_id, rec_lsn) in &begin.dirty_pages {
                push_u64(&mut bytes, page_id.get());
                push_u64(&mut bytes, rec_lsn.get());
            }
            if let Some(visibility_horizon) = begin.visibility_horizon {
                push_u64(&mut bytes, visibility_horizon.get());
            }
        }
        WalPayload::CheckpointEnd(end) => {
            push_u64(&mut bytes, end.begin_lsn.get());
            push_u64(&mut bytes, end.durable_data_generation);
            push_u64(&mut bytes, end.durable_wal_lsn.get());
            push_u64(&mut bytes, end.data_file_page_count);
        }
    }
    if bytes.len() > WAL_MAX_RECORD_LEN - WAL_HEADER_LEN {
        return Err(corruption(format!(
            "WAL {:?} payload exceeds the v1 record bound",
            payload.kind()
        )));
    }
    let _ = lsn;
    Ok(bytes)
}

fn decode_payload(kind: RecordKind, lsn: Lsn, bytes: &[u8]) -> Result<WalPayload> {
    let mut decoder = Decoder::new(bytes);
    let payload = match kind {
        RecordKind::Begin => WalPayload::Begin,
        RecordKind::PageUpdate => {
            let page_id = PageId::new(decoder.read_u64()?);
            let before = decoder.read_page(page_id)?.map(Box::new);
            let after = decoder.read_page(page_id)?.map(Box::new);
            WalPayload::PageUpdate {
                page_id,
                before,
                after,
            }
        }
        RecordKind::Resize => WalPayload::Resize {
            before_page_count: decoder.read_u64()?,
            after_page_count: decoder.read_u64()?,
        },
        RecordKind::Commit => WalPayload::Commit,
        RecordKind::Abort => WalPayload::Abort,
        RecordKind::Compensation => {
            let page_id = PageId::new(decoder.read_u64()?);
            WalPayload::Compensation {
                page_id,
                undo: decoder.read_page(page_id)?.map(Box::new),
                resulting_page_count: decoder.read_u64()?,
                undo_next_lsn: optional_lsn(decoder.read_u64()?)?,
            }
        }
        RecordKind::CheckpointBegin => {
            let active_count = decoder.read_u32()?;
            let mut active_transactions = BTreeMap::new();
            for _ in 0..active_count {
                let transaction_id = TransactionId::try_from(decoder.read_u64()?)?;
                let last_lsn = Lsn::try_from(decoder.read_u64()?)?;
                if active_transactions
                    .insert(transaction_id, last_lsn)
                    .is_some()
                {
                    return Err(corruption(
                        "CheckpointBegin contains a duplicate transaction ID",
                    ));
                }
            }
            let dirty_count = decoder.read_u32()?;
            let mut dirty_pages = BTreeMap::new();
            for _ in 0..dirty_count {
                let page_id = PageId::new(decoder.read_u64()?);
                let rec_lsn = Lsn::try_from(decoder.read_u64()?)?;
                if dirty_pages.insert(page_id, rec_lsn).is_some() {
                    return Err(corruption(
                        "CheckpointBegin contains a duplicate dirty page",
                    ));
                }
            }
            let visibility_horizon = if decoder.remaining() == 0 {
                None
            } else {
                optional_transaction_id(decoder.read_u64()?)?
            };
            WalPayload::CheckpointBegin(CheckpointBegin {
                active_transactions,
                dirty_pages,
                visibility_horizon,
            })
        }
        RecordKind::CheckpointEnd => WalPayload::CheckpointEnd(CheckpointEnd {
            begin_lsn: Lsn::try_from(decoder.read_u64()?)?,
            durable_data_generation: decoder.read_u64()?,
            durable_wal_lsn: Lsn::try_from(decoder.read_u64()?)?,
            data_file_page_count: decoder.read_u64()?,
        }),
    };
    decoder.finish()?;
    let _ = lsn;
    Ok(payload)
}

fn validate_page_image(
    page_id: PageId,
    page: Option<&SlottedPage>,
    required_lsn: Option<Lsn>,
) -> Result<()> {
    let Some(page) = page else {
        return Ok(());
    };
    page.validate()?;
    if page.page_id() != page_id {
        return Err(corruption(format!(
            "WAL page image identity {} does not match payload page {}",
            page.page_id().get(),
            page_id.get()
        )));
    }
    if let Some(required_lsn) = required_lsn
        && page.lsn() != required_lsn.get()
    {
        return Err(corruption(format!(
            "WAL page {} image LSN {} does not match record LSN {required_lsn}",
            page_id.get(),
            page.lsn()
        )));
    }
    Ok(())
}

fn optional_transaction_id(value: u64) -> Result<Option<TransactionId>> {
    if value == 0 {
        Ok(None)
    } else {
        TransactionId::try_from(value).map(Some)
    }
}

fn optional_lsn(value: u64) -> Result<Option<Lsn>> {
    if value == 0 {
        Ok(None)
    } else {
        Lsn::try_from(value).map(Some)
    }
}

fn push_page(bytes: &mut Vec<u8>, page: Option<&SlottedPage>) {
    match page {
        Some(page) => {
            bytes.push(1);
            bytes.extend_from_slice(&page.sealed_bytes());
        }
        None => bytes.push(0),
    }
}

fn push_count(bytes: &mut Vec<u8>, count: usize) -> Result<()> {
    let count =
        u32::try_from(count).map_err(|_| corruption("WAL checkpoint entry count exceeds u32"))?;
    push_u32(bytes, count);
    Ok(())
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let bytes = self.take(1)?;
        Ok(bytes[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_page(&mut self, page_id: PageId) -> Result<Option<SlottedPage>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let bytes = self.take(PAGE_SIZE)?;
                SlottedPage::from_bytes(bytes, page_id).map(Some)
            }
            flag => Err(corruption(format!(
                "WAL page-image presence flag {flag} is invalid"
            ))),
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| corruption("WAL payload offset overflow"))?;
        if end > self.bytes.len() {
            return Err(corruption("WAL record payload is truncated"));
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    const fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(corruption("WAL payload contains trailing bytes"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use ordadb_storage::{PageId, PageType, SlottedPage};

    use super::{CheckpointBegin, WAL_HEADER_LEN, WalPayload, WalRecord};
    use crate::{Lsn, TransactionId};

    fn lsn(value: u64) -> Lsn {
        Lsn::new(value).expect("non-zero LSN")
    }

    fn transaction_id(value: u64) -> TransactionId {
        TransactionId::new(value).expect("non-zero transaction ID")
    }

    #[test]
    fn page_update_round_trips_and_binds_image_identity_and_lsn() {
        let page_id = PageId::new(7);
        let before = SlottedPage::new(page_id, PageType::Heap);
        let mut after = before.clone();
        after.set_lsn(2);
        let record = WalRecord::new(
            lsn(2),
            Some(transaction_id(4)),
            Some(lsn(1)),
            WalPayload::PageUpdate {
                page_id,
                before: Some(Box::new(before)),
                after: Some(Box::new(after)),
            },
        )
        .expect("valid update");
        let bytes = record.encode().expect("encoded record");
        let decoded = WalRecord::from_bytes(&bytes).expect("decoded record");
        let WalPayload::PageUpdate { after, .. } = decoded.payload() else {
            panic!("expected page update");
        };
        assert_eq!(after.as_ref().expect("after image").lsn(), 2);
    }

    #[test]
    fn checksum_corruption_is_rejected() {
        let record = WalRecord::new(lsn(1), Some(transaction_id(1)), None, WalPayload::Begin)
            .expect("valid begin");
        let mut bytes = record.encode().expect("encoded record");
        bytes[WAL_HEADER_LEN - 1] ^= 0x55;
        let error = WalRecord::from_bytes(&bytes).expect_err("checksum corruption");
        assert_eq!(error.sql_state, "XX001");
    }

    #[test]
    fn unsupported_version_is_distinct_from_corruption() {
        let record = WalRecord::new(lsn(1), Some(transaction_id(1)), None, WalPayload::Begin)
            .expect("valid begin");
        let mut bytes = record.encode().expect("encoded record");
        bytes[8..10].copy_from_slice(&2_u16.to_le_bytes());
        let error = WalRecord::from_bytes(&bytes).expect_err("unsupported version");
        assert_eq!(error.sql_state, "0A000");
    }

    #[test]
    fn checkpoint_visibility_horizon_round_trips_and_legacy_payload_remains_readable() {
        for expected_horizon in [None, Some(transaction_id(9))] {
            let record = WalRecord::new(
                lsn(10),
                None,
                None,
                WalPayload::CheckpointBegin(CheckpointBegin {
                    active_transactions: BTreeMap::new(),
                    dirty_pages: BTreeMap::new(),
                    visibility_horizon: expected_horizon,
                }),
            )
            .expect("checkpoint begin");
            let bytes = record.encode().expect("encode checkpoint");
            let decoded = WalRecord::from_bytes(&bytes).expect("decode checkpoint");
            let WalPayload::CheckpointBegin(begin) = decoded.payload() else {
                panic!("expected checkpoint begin");
            };
            assert_eq!(begin.visibility_horizon, expected_horizon);
        }
    }
}
