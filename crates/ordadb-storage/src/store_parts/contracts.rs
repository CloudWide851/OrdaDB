use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ordadb_catalog::{Catalog, IndexDefinition, IndexMethod, TableDefinition};
use ordadb_index::{IndexEntry, IndexKey, RowId};
use ordadb_types::{DbError, IndexId, Result, Row, TableId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BufferPool, DiskManager, DurabilityBarrier, MAX_RECORD_BYTES, NoWalBarrier, PAGE_SIZE, PageId,
    PageType, SlottedPage, TUPLE_FORMAT_V2, TupleHeaderV2, corruption, decode_row, decode_row_v2,
    encode_row, encode_row_v2, io_error,
};

pub const DATABASE_FILE_NAME: &str = "ordadb.data";
const MANIFEST_MAGIC: &str = "ORDADB";
pub const DATABASE_FORMAT_V1: u16 = 1;
pub const DATABASE_FORMAT_V2: u16 = 2;
const MANIFEST_ENVELOPE_V2: u16 = 1;
const INDEX_REBUILD_CONTRACT_V2: u16 = 1;
const MAX_MANIFEST_BYTES_V2: usize = 64 * 1024 * 1024;
const MAX_MANIFEST_LAYOUT_PASSES: usize = 16;
const DEFAULT_BUFFER_CAPACITY: usize = 64;
const INDEX_RECORD_VERSION_V1: u16 = 1;
const INDEX_RECORD_VERSION: u16 = 2;
const MAX_MANIFEST_PAGE_REFERENCES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DataFormat {
    V1,
    #[default]
    V2,
}

impl DataFormat {
    #[must_use]
    pub const fn version(self) -> u16 {
        match self {
            Self::V1 => DATABASE_FORMAT_V1,
            Self::V2 => DATABASE_FORMAT_V2,
        }
    }

    fn from_version(version: u16) -> Result<Self> {
        match version {
            DATABASE_FORMAT_V1 => Ok(Self::V1),
            DATABASE_FORMAT_V2 => Ok(Self::V2),
            _ => Err(unsupported_database_version(version)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IndexRebuildModeV2 {
    FromAuthoritativeHeap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IndexRebuildContractV2 {
    pub contract_version: u16,
    pub mode: IndexRebuildModeV2,
}

impl Default for IndexRebuildContractV2 {
    fn default() -> Self {
        Self {
            contract_version: INDEX_REBUILD_CONTRACT_V2,
            mode: IndexRebuildModeV2::FromAuthoritativeHeap,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PersistentState {
    pub generation: u64,
    pub catalog: Catalog,
    pub tables: BTreeMap<TableId, Vec<Row>>,
    pub versions: BTreeMap<TableId, Vec<VersionedRow>>,
    pub indexes: BTreeMap<IndexId, Vec<IndexEntry>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VersionedRow {
    pub version_id: u32,
    pub header: TupleHeaderV2,
    pub row: Row,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableManifest {
    pub table_id: TableId,
    #[serde(with = "page_directory")]
    pub heap_pages: Vec<PageId>,
}

/// Resumable, generation-bound cursor over one authoritative v2 heap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageTableCursorV2 {
    table_id: TableId,
    generation: u64,
    heap_pages: Vec<PageId>,
    expected_rows: u64,
    expected_visible_rows: u64,
    emitted_rows: u64,
    page_index: usize,
    slot_index: u16,
    exhausted: bool,
}

impl StorageTableCursorV2 {
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn emitted_rows(&self) -> u64 {
        self.emitted_rows
    }

    #[must_use]
    pub const fn expected_rows(&self) -> u64 {
        self.expected_rows
    }

    #[must_use]
    pub const fn expected_visible_rows(&self) -> u64 {
        self.expected_visible_rows
    }

    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexManifest {
    pub index_id: IndexId,
    #[serde(with = "page_directory")]
    pub index_pages: Vec<PageId>,
}

mod page_directory {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{MAX_MANIFEST_PAGE_REFERENCES, PageId};

    #[derive(Serialize, Deserialize)]
    struct PageRange {
        first: PageId,
        count: u64,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum Representation {
        Legacy(Vec<PageId>),
        Compact { ranges: Vec<PageRange> },
    }

    pub fn serialize<S>(pages: &[PageId], serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ranges = Vec::<PageRange>::new();
        for page in pages {
            if let Some(previous) = ranges.last_mut() {
                let next = previous
                    .first
                    .get()
                    .checked_add(previous.count)
                    .ok_or_else(|| serde::ser::Error::custom("page directory range overflowed"))?;
                if next == page.get() {
                    previous.count = previous.count.checked_add(1).ok_or_else(|| {
                        serde::ser::Error::custom("page directory range count overflowed")
                    })?;
                    continue;
                }
            }
            ranges.push(PageRange {
                first: *page,
                count: 1,
            });
        }
        Representation::Compact { ranges }.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Vec<PageId>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Representation::deserialize(deserializer)? {
            Representation::Legacy(pages) => {
                validate_count(pages.len() as u64)?;
                Ok(pages)
            }
            Representation::Compact { ranges } => expand_ranges(ranges),
        }
    }

    fn expand_ranges<E>(ranges: Vec<PageRange>) -> std::result::Result<Vec<PageId>, E>
    where
        E: serde::de::Error,
    {
        let mut total = 0_u64;
        let mut previous_end = None;
        for range in &ranges {
            if range.count == 0 {
                return Err(E::custom("page directory ranges must be non-empty"));
            }
            let end = range
                .first
                .get()
                .checked_add(range.count)
                .ok_or_else(|| E::custom("page directory range overflowed"))?;
            if previous_end.is_some_and(|previous| range.first.get() < previous) {
                return Err(E::custom(
                    "page directory ranges must be ordered and non-overlapping",
                ));
            }
            previous_end = Some(end);
            total = total
                .checked_add(range.count)
                .ok_or_else(|| E::custom("page directory count overflowed"))?;
        }
        validate_count(total)?;
        let capacity = usize::try_from(total)
            .map_err(|_| E::custom("page directory does not fit this platform"))?;
        let mut pages = Vec::with_capacity(capacity);
        for range in ranges {
            let end = range
                .first
                .get()
                .checked_add(range.count)
                .ok_or_else(|| E::custom("page directory range overflowed"))?;
            pages.extend((range.first.get()..end).map(PageId::new));
        }
        Ok(pages)
    }

    fn validate_count<E>(count: u64) -> std::result::Result<(), E>
    where
        E: serde::de::Error,
    {
        if count > MAX_MANIFEST_PAGE_REFERENCES {
            return Err(E::custom(format!(
                "page directory contains {count} references, exceeding {MAX_MANIFEST_PAGE_REFERENCES}"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestV1 {
    magic: String,
    format_version: u16,
    generation: u64,
    catalog: Catalog,
    tables: Vec<TableManifest>,
    #[serde(default)]
    indexes: Vec<IndexManifest>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestEnvelopeV2 {
    magic: String,
    format_version: u16,
    envelope_version: u16,
    manifest_bytes: u64,
    manifest_sha256: String,
    #[serde(with = "page_directory")]
    metadata_pages: Vec<PageId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestV2 {
    magic: String,
    format_version: u16,
    tuple_format_version: u16,
    generation: u64,
    catalog: Catalog,
    tables: Vec<TableManifest>,
    index_rebuild: IndexRebuildContractV2,
}

#[derive(Debug)]
pub struct DatabaseStore {
    pool: BufferPool,
    data_format: DataFormat,
    read_only: bool,
    committed_state: PersistentState,
    committed_tables: Vec<TableManifest>,
    committed_indexes: Vec<IndexManifest>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StorageInspection {
    pub data_dir: PathBuf,
    pub data_format: DataFormat,
    pub generation: u64,
    pub page_count: u64,
    pub file_bytes: u64,
    pub catalog: Catalog,
    pub table_rows: BTreeMap<TableId, u64>,
    pub durable_index_count: usize,
    pub persistent_state: PersistentState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageEstimate {
    pub data_format: DataFormat,
    pub page_count: u64,
    pub file_bytes: u64,
    pub metadata_pages: u64,
    pub heap_pages: u64,
    pub durable_index_pages: u64,
}

#[derive(Debug, Clone)]
pub struct PageDelta {
    pub page_id: PageId,
    pub before: Option<SlottedPage>,
    pub after: Option<SlottedPage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyPoint {
    BeforePageWrite(PageId),
    AfterPageWrite(PageId),
    BeforeResize {
        before_page_count: u64,
        after_page_count: u64,
    },
    AfterResize {
        before_page_count: u64,
        after_page_count: u64,
    },
    BeforeSync,
    AfterSync,
}

#[derive(Debug, Clone)]
pub struct PreparedCommit {
    candidate: PersistentState,
    table_manifests: Vec<TableManifest>,
    index_manifests: Vec<IndexManifest>,
    page_deltas: Vec<PageDelta>,
    before_page_count: u64,
    after_page_count: u64,
}

impl PreparedCommit {
    #[must_use]
    pub fn page_deltas(&self) -> &[PageDelta] {
        &self.page_deltas
    }

    pub fn mark_after_lsn(&mut self, page_id: PageId, lsn: u64) -> Result<()> {
        if lsn == 0 {
            return Err(DbError::new(
                "22023",
                "a WAL-backed page update requires a non-zero LSN",
            ));
        }
        let delta_index = self
            .page_deltas
            .binary_search_by_key(&page_id, |delta| delta.page_id)
            .map_err(|_| {
                DbError::new(
                    "22023",
                    format!(
                        "page {} is not changed by this prepared commit",
                        page_id.get()
                    ),
                )
            })?;
        let after = self.page_deltas[delta_index]
            .after
            .as_mut()
            .ok_or_else(|| {
                DbError::new(
                    "22023",
                    format!(
                        "page {} is removed by this prepared commit and has no after image",
                        page_id.get()
                    ),
                )
            })?;
        after.set_lsn(lsn);
        Ok(())
    }

    #[must_use]
    pub const fn before_page_count(&self) -> u64 {
        self.before_page_count
    }

    #[must_use]
    pub const fn after_page_count(&self) -> u64 {
        self.after_page_count
    }
}
