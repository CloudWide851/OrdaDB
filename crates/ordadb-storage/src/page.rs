use std::collections::BTreeSet;

use crc32c::crc32c;
use serde::{Deserialize, Serialize};

use crate::{corruption, unsupported_version};
use ordadb_types::{DbError, Result};

pub const PAGE_SIZE: usize = 8192;
pub const FILE_FORMAT_VERSION: u16 = 1;
pub const SLOT_SIZE: usize = 6;

const PAGE_MAGIC: [u8; 4] = *b"ORDA";
const HEADER_SIZE: usize = 40;
const CHECKSUM_OFFSET: usize = 24;
const LOWER_OFFSET: usize = 28;
const UPPER_OFFSET: usize = 30;
const SLOT_COUNT_OFFSET: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PageId(u64);

impl PageId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    Metadata = 1,
    Heap = 2,
}

impl TryFrom<u8> for PageType {
    type Error = DbError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Metadata),
            2 => Ok(Self::Heap),
            _ => Err(corruption(format!("unknown page type {value}"))),
        }
    }
}

#[derive(Clone)]
pub struct SlottedPage {
    bytes: [u8; PAGE_SIZE],
}

impl std::fmt::Debug for SlottedPage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SlottedPage")
            .field("page_id", &self.page_id())
            .field("page_type", &self.page_type())
            .field("lsn", &self.lsn())
            .field("slot_count", &self.slot_count())
            .finish()
    }
}

impl SlottedPage {
    #[must_use]
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let mut bytes = [0_u8; PAGE_SIZE];
        bytes[0..4].copy_from_slice(&PAGE_MAGIC);
        write_u16(&mut bytes, 4, FILE_FORMAT_VERSION);
        bytes[6] = page_type as u8;
        bytes[7] = 0;
        write_u64(&mut bytes, 8, page_id.get());
        write_u64(&mut bytes, 16, 0);
        write_u16(&mut bytes, LOWER_OFFSET, HEADER_SIZE as u16);
        write_u16(&mut bytes, UPPER_OFFSET, PAGE_SIZE as u16);
        write_u16(&mut bytes, SLOT_COUNT_OFFSET, 0);
        seal(&mut bytes);
        Self { bytes }
    }

    pub fn from_bytes(bytes: &[u8], expected_page_id: PageId) -> Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(corruption(format!(
                "page {} has length {}, expected {PAGE_SIZE}",
                expected_page_id.get(),
                bytes.len()
            )));
        }
        let mut owned = [0_u8; PAGE_SIZE];
        owned.copy_from_slice(bytes);
        validate(&owned, expected_page_id)?;
        Ok(Self { bytes: owned })
    }

    #[must_use]
    pub fn page_id(&self) -> PageId {
        PageId::new(read_u64(&self.bytes, 8))
    }

    pub fn page_type(&self) -> Result<PageType> {
        PageType::try_from(self.bytes[6])
    }

    #[must_use]
    pub fn lsn(&self) -> u64 {
        read_u64(&self.bytes, 16)
    }

    #[must_use]
    pub fn flags(&self) -> u8 {
        self.bytes[7]
    }

    #[must_use]
    pub fn slot_count(&self) -> u16 {
        read_u16(&self.bytes, SLOT_COUNT_OFFSET)
    }

    #[must_use]
    pub fn free_space(&self) -> usize {
        let lower = usize::from(read_u16(&self.bytes, LOWER_OFFSET));
        let upper = usize::from(read_u16(&self.bytes, UPPER_OFFSET));
        upper.saturating_sub(lower)
    }

    #[must_use]
    pub fn can_fit(&self, record_len: usize) -> bool {
        record_len
            .checked_add(SLOT_SIZE)
            .is_some_and(|required| required <= self.free_space())
            && record_len <= usize::from(u16::MAX)
    }

    pub fn insert(&mut self, record: &[u8]) -> Result<Option<u16>> {
        if !self.can_fit(record.len()) {
            return Ok(None);
        }

        let slot_id = self.slot_count();
        let lower = usize::from(read_u16(&self.bytes, LOWER_OFFSET));
        let upper = usize::from(read_u16(&self.bytes, UPPER_OFFSET));
        let new_upper = upper
            .checked_sub(record.len())
            .ok_or_else(|| corruption("page free-space bounds underflow"))?;
        let record_len = u16::try_from(record.len())
            .map_err(|_| corruption("record length exceeds the page format limit"))?;

        self.bytes[new_upper..upper].copy_from_slice(record);
        write_u16(&mut self.bytes, lower, new_upper as u16);
        write_u16(&mut self.bytes, lower + 2, record_len);
        write_u16(&mut self.bytes, lower + 4, 0);
        write_u16(&mut self.bytes, LOWER_OFFSET, (lower + SLOT_SIZE) as u16);
        write_u16(&mut self.bytes, UPPER_OFFSET, new_upper as u16);
        write_u16(&mut self.bytes, SLOT_COUNT_OFFSET, slot_id + 1);
        seal(&mut self.bytes);
        Ok(Some(slot_id))
    }

    pub fn record(&self, slot_id: u16) -> Result<&[u8]> {
        let slot = self.slot(slot_id)?;
        Ok(&self.bytes[slot.offset..slot.offset + slot.length])
    }

    pub fn records(&self) -> Result<Vec<Vec<u8>>> {
        (0..self.slot_count())
            .map(|slot_id| self.record(slot_id).map(<[u8]>::to_vec))
            .collect()
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        write_u64(&mut self.bytes, 16, lsn);
        seal(&mut self.bytes);
    }

    #[must_use]
    pub fn sealed_bytes(&self) -> [u8; PAGE_SIZE] {
        self.bytes
    }

    pub fn validate(&self) -> Result<()> {
        validate(&self.bytes, self.page_id())
    }

    fn slot(&self, slot_id: u16) -> Result<Slot> {
        if slot_id >= self.slot_count() {
            return Err(corruption(format!(
                "slot {slot_id} is outside page {} slot directory",
                self.page_id().get()
            )));
        }
        let start = HEADER_SIZE + usize::from(slot_id) * SLOT_SIZE;
        Ok(Slot {
            offset: usize::from(read_u16(&self.bytes, start)),
            length: usize::from(read_u16(&self.bytes, start + 2)),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct Slot {
    offset: usize,
    length: usize,
}

fn validate(bytes: &[u8; PAGE_SIZE], expected_page_id: PageId) -> Result<()> {
    if bytes[0..4] != PAGE_MAGIC {
        return Err(corruption(format!(
            "page {} has an invalid magic value",
            expected_page_id.get()
        )));
    }
    let version = read_u16(bytes, 4);
    if version != FILE_FORMAT_VERSION {
        return Err(unsupported_version(version));
    }
    PageType::try_from(bytes[6])?;
    let actual_page_id = PageId::new(read_u64(bytes, 8));
    if actual_page_id != expected_page_id {
        return Err(corruption(format!(
            "page identity mismatch: expected {}, found {}",
            expected_page_id.get(),
            actual_page_id.get()
        )));
    }

    let lower = usize::from(read_u16(bytes, LOWER_OFFSET));
    let upper = usize::from(read_u16(bytes, UPPER_OFFSET));
    let slot_count = usize::from(read_u16(bytes, SLOT_COUNT_OFFSET));
    let expected_lower = HEADER_SIZE
        .checked_add(
            slot_count
                .checked_mul(SLOT_SIZE)
                .ok_or_else(|| corruption("slot directory size overflow"))?,
        )
        .ok_or_else(|| corruption("slot directory bound overflow"))?;
    if lower != expected_lower || lower > upper || upper > PAGE_SIZE {
        return Err(corruption(format!(
            "page {} has invalid free-space bounds lower={lower}, upper={upper}, slots={slot_count}",
            expected_page_id.get()
        )));
    }

    let mut occupied = BTreeSet::new();
    for slot_id in 0..slot_count {
        let start = HEADER_SIZE + slot_id * SLOT_SIZE;
        let offset = usize::from(read_u16(bytes, start));
        let length = usize::from(read_u16(bytes, start + 2));
        let flags = read_u16(bytes, start + 4);
        let end = offset
            .checked_add(length)
            .ok_or_else(|| corruption("slot record bound overflow"))?;
        if flags != 0 || offset < upper || end > PAGE_SIZE {
            return Err(corruption(format!(
                "page {} slot {slot_id} has invalid bounds or flags",
                expected_page_id.get()
            )));
        }
        for byte_index in offset..end {
            if !occupied.insert(byte_index) {
                return Err(corruption(format!(
                    "page {} contains overlapping slot records",
                    expected_page_id.get()
                )));
            }
        }
    }

    let stored_checksum = read_u32(bytes, CHECKSUM_OFFSET);
    let mut checksum_input = *bytes;
    checksum_input[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
    let computed_checksum = crc32c(&checksum_input);
    if stored_checksum != computed_checksum {
        return Err(corruption(format!(
            "page {} checksum mismatch",
            expected_page_id.get()
        )));
    }
    Ok(())
}

fn seal(bytes: &mut [u8; PAGE_SIZE]) {
    bytes[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
    let checksum = crc32c(bytes);
    write_u32(bytes, CHECKSUM_OFFSET, checksum);
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

fn write_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_exact_record_round_trip() {
        let page_id = PageId::new(7);
        let mut page = SlottedPage::new(page_id, PageType::Heap);
        assert_eq!(page.slot_count(), 0);
        assert!(page.validate().is_ok());

        let payload = vec![0x5a; page.free_space() - SLOT_SIZE];
        assert_eq!(page.insert(&payload).expect("insert"), Some(0));
        assert_eq!(page.free_space(), 0);
        assert_eq!(page.record(0).expect("record"), payload);
        assert_eq!(page.insert(&[1]).expect("full"), None);

        let decoded =
            SlottedPage::from_bytes(&page.sealed_bytes()[..], page_id).expect("decode page");
        assert_eq!(decoded.record(0).expect("decoded record"), payload);
    }

    #[test]
    fn slots_preserve_insertion_order() {
        let mut page = SlottedPage::new(PageId::new(1), PageType::Heap);
        page.insert(b"first").expect("insert");
        page.insert(b"second").expect("insert");
        assert_eq!(
            page.records().expect("records"),
            vec![b"first".to_vec(), b"second".to_vec()]
        );
    }

    #[test]
    fn rejects_checksum_bounds_identity_and_version_corruption() {
        let page = SlottedPage::new(PageId::new(2), PageType::Metadata);

        let mut checksum = page.sealed_bytes();
        checksum[100] ^= 0xff;
        assert_eq!(
            SlottedPage::from_bytes(&checksum[..], PageId::new(2))
                .expect_err("checksum")
                .sql_state,
            "XX001"
        );

        let mut bounds = page.sealed_bytes();
        write_u16(&mut bounds, LOWER_OFFSET, 2);
        assert_eq!(
            SlottedPage::from_bytes(&bounds[..], PageId::new(2))
                .expect_err("bounds")
                .sql_state,
            "XX001"
        );

        assert_eq!(
            SlottedPage::from_bytes(&page.sealed_bytes()[..], PageId::new(3))
                .expect_err("identity")
                .sql_state,
            "XX001"
        );

        let mut version = page.sealed_bytes();
        write_u16(&mut version, 4, FILE_FORMAT_VERSION + 1);
        assert_eq!(
            SlottedPage::from_bytes(&version[..], PageId::new(2))
                .expect_err("version")
                .sql_state,
            "0A000"
        );
    }

    #[test]
    fn rejects_wrong_page_length_and_overlapping_slots() {
        assert_eq!(
            SlottedPage::from_bytes(&[0; PAGE_SIZE - 1], PageId::new(0))
                .expect_err("length")
                .sql_state,
            "XX001"
        );

        let mut page = SlottedPage::new(PageId::new(4), PageType::Heap);
        page.insert(b"one").expect("insert one");
        page.insert(b"two").expect("insert two");
        let mut bytes = page.sealed_bytes();
        let first_offset = read_u16(&bytes, HEADER_SIZE);
        write_u16(&mut bytes, HEADER_SIZE + SLOT_SIZE, first_offset);
        seal(&mut bytes);
        assert_eq!(
            SlottedPage::from_bytes(&bytes[..], PageId::new(4))
                .expect_err("overlap")
                .sql_state,
            "XX001"
        );
    }
}
