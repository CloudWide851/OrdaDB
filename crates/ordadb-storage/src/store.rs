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
const INDEX_RECORD_VERSION: u16 = 1;
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
    pub indexes: BTreeMap<IndexId, Vec<IndexEntry>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableManifest {
    pub table_id: TableId,
    #[serde(with = "page_directory")]
    pub heap_pages: Vec<PageId>,
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

impl DatabaseStore {
    pub fn estimate_state(
        state: &PersistentState,
        data_format: DataFormat,
    ) -> Result<StorageEstimate> {
        let (pages, _, _) = build_snapshot(state, data_format)?;
        let mut metadata_pages = 0_u64;
        let mut heap_pages = 0_u64;
        let mut durable_index_pages = 0_u64;
        for page in &pages {
            match page.page_type()? {
                PageType::Metadata => metadata_pages += 1,
                PageType::Heap => heap_pages += 1,
                PageType::Index => durable_index_pages += 1,
            }
        }
        let page_count = u64::try_from(pages.len())
            .map_err(|_| corruption("prepared database page count exceeds the format limit"))?;
        let file_bytes = page_count
            .checked_mul(PAGE_SIZE as u64)
            .ok_or_else(|| corruption("prepared database byte length overflowed"))?;
        Ok(StorageEstimate {
            data_format,
            page_count,
            file_bytes,
            metadata_pages,
            heap_pages,
            durable_index_pages,
        })
    }

    pub fn detect_format_read_only(data_dir: impl AsRef<Path>) -> Result<Option<DataFormat>> {
        let path = data_dir.as_ref().join(DATABASE_FILE_NAME);
        let metadata = match std::fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(io_error("failed to inspect database file format", error));
            }
        };
        if metadata.len() == 0 {
            return Ok(None);
        }
        if metadata.len() < PAGE_SIZE as u64 {
            return Err(corruption(
                "database file is shorter than its page-zero format header",
            ));
        }
        let mut bytes = [0_u8; PAGE_SIZE];
        File::open(&path)
            .and_then(|mut file| file.read_exact(&mut bytes))
            .map_err(|error| io_error("failed to read database page zero", error))?;
        let page = SlottedPage::from_bytes(&bytes, PageId::new(0))?;
        detect_manifest_format(&page).map(Some)
    }

    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_barrier(data_dir, Arc::new(NoWalBarrier))
    }

    pub fn open_with_format(data_dir: impl AsRef<Path>, data_format: DataFormat) -> Result<Self> {
        Self::open_with_barrier_and_format(data_dir, Arc::new(NoWalBarrier), data_format)
    }

    pub fn open_with_barrier(
        data_dir: impl AsRef<Path>,
        barrier: Arc<dyn DurabilityBarrier>,
    ) -> Result<Self> {
        Self::open_with_barrier_and_format(data_dir, barrier, DataFormat::V1)
    }

    pub fn open_with_barrier_and_format(
        data_dir: impl AsRef<Path>,
        barrier: Arc<dyn DurabilityBarrier>,
        data_format: DataFormat,
    ) -> Result<Self> {
        Self::open_internal(
            data_dir,
            DEFAULT_BUFFER_CAPACITY,
            barrier,
            Some(data_format),
            false,
        )
    }

    pub fn open_with_capacity(data_dir: impl AsRef<Path>, capacity: usize) -> Result<Self> {
        Self::open_internal(
            data_dir,
            capacity,
            Arc::new(NoWalBarrier),
            Some(DataFormat::V1),
            false,
        )
    }

    pub fn open_read_only(data_dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_internal(
            data_dir,
            DEFAULT_BUFFER_CAPACITY,
            Arc::new(NoWalBarrier),
            None,
            true,
        )
    }

    pub fn inspect_read_only(data_dir: impl AsRef<Path>) -> Result<StorageInspection> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let store = Self::open_read_only(&data_dir)?;
        let page_count = store.page_count()?;
        let file_bytes = page_count
            .checked_mul(PAGE_SIZE as u64)
            .ok_or_else(|| corruption("database file byte length overflowed"))?;
        let table_rows = store
            .committed_state
            .tables
            .iter()
            .map(|(table_id, rows)| {
                u64::try_from(rows.len())
                    .map(|count| (*table_id, count))
                    .map_err(|_| corruption("table row count exceeds the storage format limit"))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(StorageInspection {
            data_dir,
            data_format: store.data_format,
            generation: store.committed_state.generation,
            page_count,
            file_bytes,
            catalog: store.committed_state.catalog.clone(),
            table_rows,
            durable_index_count: store.committed_indexes.len(),
            persistent_state: store.committed_state.clone(),
        })
    }

    fn open_internal(
        data_dir: impl AsRef<Path>,
        capacity: usize,
        barrier: Arc<dyn DurabilityBarrier>,
        expected_format: Option<DataFormat>,
        read_only: bool,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        if !read_only {
            std::fs::create_dir_all(data_dir)
                .map_err(|error| io_error("failed to create database data directory", error))?;
        } else if !data_dir.is_dir() {
            return Err(DbError::new(
                "58030",
                "database data directory does not exist for read-only inspection",
            ));
        }
        let database_path = data_dir.join(DATABASE_FILE_NAME);
        let disk = if read_only {
            DiskManager::open_read_only(database_path)?
        } else {
            DiskManager::open(database_path)?
        };
        let pool = BufferPool::new(disk, capacity, barrier)?;
        if pool.page_count()? == 0 {
            if read_only {
                return Err(corruption(
                    "read-only database file does not contain a metadata page",
                ));
            }
            let data_format = expected_format.unwrap_or_default();
            let mut store = Self {
                pool,
                data_format,
                read_only,
                committed_state: PersistentState::default(),
                committed_tables: Vec::new(),
                committed_indexes: Vec::new(),
            };
            let bootstrap = PersistentState::default();
            store.commit(&bootstrap)?;
            return Ok(store);
        }

        let loaded = load_state(&pool)?;
        if expected_format.is_some_and(|expected| expected != loaded.data_format) {
            return Err(DbError::new(
                "0A000",
                format!(
                    "database format {} cannot be opened as format {}",
                    loaded.data_format.version(),
                    expected_format.map(DataFormat::version).unwrap_or_default()
                ),
            )
            .with_hint("use read-only inspection or run an explicit supported migration"));
        }
        Ok(Self {
            pool,
            data_format: loaded.data_format,
            read_only,
            committed_state: loaded.state,
            committed_tables: loaded.tables,
            committed_indexes: loaded.indexes,
        })
    }

    #[must_use]
    pub const fn data_format(&self) -> DataFormat {
        self.data_format
    }

    #[must_use]
    pub const fn is_read_only(&self) -> bool {
        self.read_only
    }

    #[must_use]
    pub const fn committed_state(&self) -> &PersistentState {
        &self.committed_state
    }

    #[must_use]
    pub fn table_manifests(&self) -> &[TableManifest] {
        &self.committed_tables
    }

    #[must_use]
    pub fn index_manifests(&self) -> &[IndexManifest] {
        &self.committed_indexes
    }

    pub fn commit(&mut self, candidate: &PersistentState) -> Result<()> {
        self.ensure_writable()?;
        let prepared = self.prepare_commit(candidate)?;
        self.apply_prepared(&prepared)?;
        self.publish_prepared(prepared)
    }

    pub fn prepare_commit(&self, candidate: &PersistentState) -> Result<PreparedCommit> {
        self.ensure_writable()?;
        let (after_pages, table_manifests, index_manifests) =
            build_snapshot(candidate, self.data_format)?;
        let before_page_count = self.pool.page_count()?;
        let after_page_count = u64::try_from(after_pages.len())
            .map_err(|_| corruption("prepared snapshot page count exceeds the format limit"))?;
        let compared_page_count = before_page_count.max(after_page_count);
        let mut page_deltas = Vec::new();

        for page_index in 0..compared_page_count {
            let page_id = PageId::new(page_index);
            let before = if page_index < before_page_count {
                Some(self.pool.fetch(page_id)?.snapshot()?)
            } else {
                None
            };
            let mut after = if page_index < after_page_count {
                let page_index = usize::try_from(page_index)
                    .map_err(|_| corruption("prepared page index exceeds the platform limit"))?;
                after_pages.get(page_index).cloned()
            } else {
                None
            };

            if let (Some(before), Some(after)) = (&before, &mut after) {
                after.set_lsn(before.lsn());
                if before.sealed_bytes() == after.sealed_bytes() {
                    continue;
                }
            }
            page_deltas.push(PageDelta {
                page_id,
                before,
                after,
            });
        }

        Ok(PreparedCommit {
            candidate: candidate.clone(),
            table_manifests,
            index_manifests,
            page_deltas,
            before_page_count,
            after_page_count,
        })
    }

    pub fn apply_prepared(&mut self, prepared: &PreparedCommit) -> Result<()> {
        self.apply_prepared_with_observer(prepared, |_| Ok(()))
    }

    pub fn apply_prepared_with_observer(
        &mut self,
        prepared: &PreparedCommit,
        mut observe: impl FnMut(ApplyPoint) -> Result<()>,
    ) -> Result<()> {
        self.ensure_writable()?;
        validate_prepared(&self.pool, prepared)?;
        let affected_pages = prepared
            .page_deltas
            .iter()
            .map(|delta| delta.page_id)
            .collect::<Vec<_>>();
        self.pool.invalidate_pages(&affected_pages)?;

        for delta in &prepared.page_deltas {
            if let Some(after) = &delta.after {
                observe(ApplyPoint::BeforePageWrite(delta.page_id))?;
                self.pool.install(after.clone(), true)?;
                self.pool.flush_page(delta.page_id)?;
                observe(ApplyPoint::AfterPageWrite(delta.page_id))?;
            }
        }
        if prepared.before_page_count != prepared.after_page_count {
            let resize = ApplyPoint::BeforeResize {
                before_page_count: prepared.before_page_count,
                after_page_count: prepared.after_page_count,
            };
            observe(resize)?;
            self.pool.resize_pages(prepared.after_page_count)?;
            observe(ApplyPoint::AfterResize {
                before_page_count: prepared.before_page_count,
                after_page_count: prepared.after_page_count,
            })?;
        }
        observe(ApplyPoint::BeforeSync)?;
        self.pool.sync_all()?;
        observe(ApplyPoint::AfterSync)?;
        Ok(())
    }

    pub fn publish_prepared(&mut self, prepared: PreparedCommit) -> Result<()> {
        validate_applied(&self.pool, &prepared)?;
        self.committed_state = prepared.candidate;
        self.committed_tables = prepared.table_manifests;
        self.committed_indexes = prepared.index_manifests;
        Ok(())
    }

    pub fn page_count(&self) -> Result<u64> {
        self.pool.page_count()
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            return Err(DbError::new("25006", "database store is read-only")
                .with_hint("migrate the v1 database into a writable v2 cluster"));
        }
        Ok(())
    }
}

fn validate_prepared(pool: &BufferPool, prepared: &PreparedCommit) -> Result<()> {
    let actual_page_count = pool.page_count()?;
    if actual_page_count != prepared.before_page_count {
        return Err(DbError::new(
            "55000",
            "prepared commit no longer matches the database file length",
        )
        .with_detail(format!(
            "prepared against {} pages, found {actual_page_count}",
            prepared.before_page_count
        ))
        .with_hint("discard the prepared commit and prepare the candidate again"));
    }

    let compared_page_count = prepared.before_page_count.max(prepared.after_page_count);
    let mut previous_page_id = None;
    for delta in &prepared.page_deltas {
        if delta.page_id.get() >= compared_page_count
            || previous_page_id.is_some_and(|previous| previous >= delta.page_id)
        {
            return Err(corruption(
                "prepared commit page deltas are not a sorted unique page sequence",
            ));
        }
        let expects_before = delta.page_id.get() < prepared.before_page_count;
        let expects_after = delta.page_id.get() < prepared.after_page_count;
        if delta.before.is_some() != expects_before
            || delta.after.is_some() != expects_after
            || delta
                .before
                .as_ref()
                .is_some_and(|page| page.page_id() != delta.page_id)
            || delta
                .after
                .as_ref()
                .is_some_and(|page| page.page_id() != delta.page_id)
        {
            return Err(corruption(
                "prepared commit contains an invalid page delta identity",
            ));
        }
        match &delta.before {
            Some(before) => {
                before.validate()?;
                let current = pool.fetch(delta.page_id)?.snapshot()?;
                if current.sealed_bytes() != before.sealed_bytes() {
                    return Err(DbError::new(
                        "55000",
                        format!(
                            "prepared before image for page {} no longer matches storage",
                            delta.page_id.get()
                        ),
                    )
                    .with_hint("run recovery or prepare the candidate again"));
                }
            }
            None if delta.page_id.get() < actual_page_count => {
                return Err(corruption(
                    "prepared commit omits an existing page before image",
                ));
            }
            None => {}
        }
        if let Some(after) = &delta.after {
            after.validate()?;
        }
        previous_page_id = Some(delta.page_id);
    }
    Ok(())
}

fn validate_applied(pool: &BufferPool, prepared: &PreparedCommit) -> Result<()> {
    let actual_page_count = pool.page_count()?;
    if actual_page_count != prepared.after_page_count {
        return Err(DbError::new(
            "55000",
            "applied commit no longer matches the prepared database file length",
        )
        .with_detail(format!(
            "prepared {} pages, found {actual_page_count}",
            prepared.after_page_count
        ))
        .with_hint("run recovery before publishing database state"));
    }
    for delta in &prepared.page_deltas {
        if let Some(after) = &delta.after {
            let current = pool.fetch(delta.page_id)?.snapshot()?;
            if current.sealed_bytes() != after.sealed_bytes() {
                return Err(DbError::new(
                    "55000",
                    format!(
                        "applied page {} no longer matches its prepared after image",
                        delta.page_id.get()
                    ),
                )
                .with_hint("run recovery before publishing database state"));
            }
        }
    }
    Ok(())
}

fn build_snapshot(
    state: &PersistentState,
    data_format: DataFormat,
) -> Result<(Vec<SlottedPage>, Vec<TableManifest>, Vec<IndexManifest>)> {
    match data_format {
        DataFormat::V1 => build_snapshot_v1(state),
        DataFormat::V2 => build_snapshot_v2(state),
    }
}

fn build_snapshot_v2(
    state: &PersistentState,
) -> Result<(Vec<SlottedPage>, Vec<TableManifest>, Vec<IndexManifest>)> {
    let catalog_table_ids = validate_table_directory(state)?;
    validate_derived_indexes(state)?;
    let mut metadata_page_count = 2_u64;

    for _ in 0..MAX_MANIFEST_LAYOUT_PASSES {
        let (heap_pages, table_manifests) =
            build_v2_heap_pages(state, &catalog_table_ids, metadata_page_count)?;
        let manifest = ManifestV2 {
            magic: MANIFEST_MAGIC.to_owned(),
            format_version: DATABASE_FORMAT_V2,
            tuple_format_version: TUPLE_FORMAT_V2,
            generation: state.generation,
            catalog: state.catalog.clone(),
            tables: table_manifests.clone(),
            index_rebuild: IndexRebuildContractV2::default(),
        };
        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|error| corruption(format!("failed to encode v2 manifest: {error}")))?;
        if manifest_bytes.is_empty() || manifest_bytes.len() > MAX_MANIFEST_BYTES_V2 {
            return Err(DbError::new(
                "54000",
                format!(
                    "database v2 manifest is {} bytes; limit is {MAX_MANIFEST_BYTES_V2}",
                    manifest_bytes.len()
                ),
            )
            .with_hint("reduce catalog objects or restore into separate databases"));
        }
        let chunk_count = manifest_bytes.len().div_ceil(MAX_RECORD_BYTES);
        let required_metadata_pages = u64::try_from(chunk_count)
            .map_err(|_| corruption("v2 manifest page count exceeds the format limit"))?
            .checked_add(1)
            .ok_or_else(|| corruption("v2 metadata page count overflowed"))?;
        if required_metadata_pages != metadata_page_count {
            metadata_page_count = required_metadata_pages;
            continue;
        }

        let metadata_pages = (1..metadata_page_count)
            .map(PageId::new)
            .collect::<Vec<_>>();
        let envelope = ManifestEnvelopeV2 {
            magic: MANIFEST_MAGIC.to_owned(),
            format_version: DATABASE_FORMAT_V2,
            envelope_version: MANIFEST_ENVELOPE_V2,
            manifest_bytes: u64::try_from(manifest_bytes.len())
                .map_err(|_| corruption("v2 manifest byte length exceeds the format limit"))?,
            manifest_sha256: sha256_hex(&manifest_bytes),
            metadata_pages: metadata_pages.clone(),
        };
        let envelope_bytes = serde_json::to_vec(&envelope).map_err(|error| {
            corruption(format!("failed to encode v2 manifest envelope: {error}"))
        })?;
        let mut page_zero = SlottedPage::new(PageId::new(0), PageType::Metadata);
        if page_zero.insert(&envelope_bytes)?.is_none() {
            return Err(DbError::new(
                "54000",
                "database v2 manifest envelope is too large for page zero",
            ));
        }

        let mut pages = Vec::with_capacity(
            usize::try_from(metadata_page_count)
                .unwrap_or(usize::MAX)
                .saturating_add(heap_pages.len()),
        );
        pages.push(page_zero);
        for (page_id, chunk) in metadata_pages
            .into_iter()
            .zip(manifest_bytes.chunks(MAX_RECORD_BYTES))
        {
            let mut page = SlottedPage::new(page_id, PageType::Metadata);
            if page.insert(chunk)?.is_none() {
                return Err(corruption(
                    "v2 manifest chunk does not fit its metadata page",
                ));
            }
            pages.push(page);
        }
        pages.extend(heap_pages);
        return Ok((pages, table_manifests, Vec::new()));
    }

    Err(corruption(
        "database v2 manifest page layout did not converge",
    ))
}

fn validate_table_directory(state: &PersistentState) -> Result<BTreeSet<TableId>> {
    let catalog_table_ids = state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .map(|table| table.id)
        .collect::<BTreeSet<_>>();
    for table_id in state.tables.keys() {
        if !catalog_table_ids.contains(table_id) {
            return Err(corruption(format!(
                "row state references unknown table {}",
                table_id.get()
            )));
        }
    }
    Ok(catalog_table_ids)
}

fn build_v2_heap_pages(
    state: &PersistentState,
    catalog_table_ids: &BTreeSet<TableId>,
    first_heap_page: u64,
) -> Result<(Vec<SlottedPage>, Vec<TableManifest>)> {
    let mut next_page_id = first_heap_page;
    let mut heap_pages = Vec::new();
    let mut table_manifests = Vec::with_capacity(catalog_table_ids.len());
    for table_id in catalog_table_ids {
        let rows = state.tables.get(table_id).map_or(&[][..], Vec::as_slice);
        let mut page_ids = Vec::new();
        let mut current_page: Option<SlottedPage> = None;
        for row in rows {
            let encoded = encode_row_v2(row, TupleHeaderV2::frozen(row)?)?;
            if current_page
                .as_ref()
                .is_some_and(|page| !page.can_fit(encoded.len()))
                && let Some(page) = current_page.take()
            {
                heap_pages.push(page);
            }
            if current_page.is_none() {
                let page_id = PageId::new(next_page_id);
                next_page_id = next_page_id
                    .checked_add(1)
                    .ok_or_else(|| corruption("v2 heap page ID overflow"))?;
                page_ids.push(page_id);
                current_page = Some(SlottedPage::new(page_id, PageType::Heap));
            }
            if current_page
                .as_mut()
                .ok_or_else(|| DbError::internal("v2 heap page was not initialized"))?
                .insert(&encoded)?
                .is_none()
            {
                return Err(DbError::new(
                    "54000",
                    format!(
                        "v2 row for table {} is too large for an 8 KiB heap page",
                        table_id.get()
                    ),
                )
                .with_hint("reduce variable-width values before retrying"));
            }
        }
        if let Some(page) = current_page {
            heap_pages.push(page);
        }
        table_manifests.push(TableManifest {
            table_id: *table_id,
            heap_pages: page_ids,
        });
    }
    Ok((heap_pages, table_manifests))
}

fn validate_derived_indexes(state: &PersistentState) -> Result<()> {
    let catalog_index_ids = state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .flat_map(|table| table.indexes())
        .filter(|index| index.method == IndexMethod::BTree)
        .map(|index| index.id)
        .collect::<BTreeSet<_>>();
    if state.indexes.keys().copied().collect::<BTreeSet<_>>() != catalog_index_ids {
        return Err(corruption(
            "index state does not match the catalog index directory",
        ));
    }
    for index_id in catalog_index_ids {
        let definition = state
            .catalog
            .index_by_id(index_id)
            .ok_or_else(|| corruption("catalog index disappeared during v2 snapshot build"))?;
        let rows = state
            .tables
            .get(&definition.table_id)
            .ok_or_else(|| corruption("v2 index owner table has no row state"))?;
        let owner = state
            .catalog
            .table_by_id(definition.table_id)
            .ok_or_else(|| corruption("v2 index owner table is absent from the catalog"))?;
        let entries = state
            .indexes
            .get(&index_id)
            .ok_or_else(|| corruption("v2 catalog index has no derived entry state"))?;
        validate_index_entries(definition, owner, rows, entries)?;
    }
    Ok(())
}

fn build_snapshot_v1(
    state: &PersistentState,
) -> Result<(Vec<SlottedPage>, Vec<TableManifest>, Vec<IndexManifest>)> {
    let catalog_table_ids: BTreeSet<_> = state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .map(|table| table.id)
        .collect();
    for table_id in state.tables.keys() {
        if !catalog_table_ids.contains(table_id) {
            return Err(corruption(format!(
                "row state references unknown table {}",
                table_id.get()
            )));
        }
    }

    let mut next_page_id = 1_u64;
    let mut heap_pages = Vec::new();
    let mut table_manifests = Vec::with_capacity(catalog_table_ids.len());
    for table_id in catalog_table_ids {
        let rows = state.tables.get(&table_id).map_or(&[][..], Vec::as_slice);
        let mut page_ids = Vec::new();
        let mut current_page: Option<SlottedPage> = None;

        for row in rows {
            let encoded = encode_row(row)?;
            if current_page
                .as_ref()
                .is_some_and(|page| !page.can_fit(encoded.len()))
                && let Some(page) = current_page.take()
            {
                heap_pages.push(page);
            }
            if current_page.is_none() {
                let page_id = PageId::new(next_page_id);
                next_page_id = next_page_id
                    .checked_add(1)
                    .ok_or_else(|| corruption("heap page ID overflow"))?;
                page_ids.push(page_id);
                current_page = Some(SlottedPage::new(page_id, PageType::Heap));
            }
            let inserted = current_page
                .as_mut()
                .ok_or_else(|| DbError::internal("heap page was not initialized"))?
                .insert(&encoded)?;
            if inserted.is_none() {
                return Err(DbError::new(
                    "54000",
                    format!(
                        "row for table {} is too large for an 8 KiB heap page",
                        table_id.get()
                    ),
                )
                .with_hint("reduce variable-width values before retrying"));
            }
        }
        if let Some(page) = current_page {
            heap_pages.push(page);
        }
        table_manifests.push(TableManifest {
            table_id,
            heap_pages: page_ids,
        });
    }

    let catalog_index_ids = state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .flat_map(|table| table.indexes())
        .filter(|index| index.method == IndexMethod::BTree)
        .map(|index| index.id)
        .collect::<BTreeSet<_>>();
    if state.indexes.keys().copied().collect::<BTreeSet<_>>() != catalog_index_ids {
        return Err(corruption(
            "index state does not match the catalog index directory",
        ));
    }

    let mut index_pages = Vec::new();
    let mut index_manifests = Vec::with_capacity(catalog_index_ids.len());
    for index_id in catalog_index_ids {
        let definition = state
            .catalog
            .index_by_id(index_id)
            .ok_or_else(|| corruption("catalog index disappeared during snapshot build"))?;
        let rows = state
            .tables
            .get(&definition.table_id)
            .ok_or_else(|| corruption("index owner table has no row state"))?;
        let owner = state
            .catalog
            .table_by_id(definition.table_id)
            .ok_or_else(|| corruption("index owner table is absent from the catalog"))?;
        let entries = state
            .indexes
            .get(&index_id)
            .ok_or_else(|| corruption("catalog index has no entry state"))?;
        validate_index_entries(definition, owner, rows, entries)?;

        let mut page_ids = Vec::new();
        let mut current_page: Option<SlottedPage> = None;
        for entry in entries {
            let encoded = encode_index_entry(entry)?;
            if current_page
                .as_ref()
                .is_some_and(|page| !page.can_fit(encoded.len()))
                && let Some(page) = current_page.take()
            {
                index_pages.push(page);
            }
            if current_page.is_none() {
                let page_id = PageId::new(next_page_id);
                next_page_id = next_page_id
                    .checked_add(1)
                    .ok_or_else(|| corruption("index page ID overflow"))?;
                page_ids.push(page_id);
                current_page = Some(SlottedPage::new(page_id, PageType::Index));
            }
            if current_page
                .as_mut()
                .ok_or_else(|| DbError::internal("index page was not initialized"))?
                .insert(&encoded)?
                .is_none()
            {
                return Err(DbError::new(
                    "54000",
                    format!(
                        "entry for index {} is too large for an 8 KiB index page",
                        index_id.get()
                    ),
                ));
            }
        }
        if let Some(page) = current_page {
            index_pages.push(page);
        }
        index_manifests.push(IndexManifest {
            index_id,
            index_pages: page_ids,
        });
    }

    let manifest = ManifestV1 {
        magic: MANIFEST_MAGIC.to_owned(),
        format_version: DATABASE_FORMAT_V1,
        generation: state.generation,
        catalog: state.catalog.clone(),
        tables: table_manifests.clone(),
        indexes: index_manifests.clone(),
    };
    let manifest_bytes = serde_json::to_vec(&manifest)
        .map_err(|error| corruption(format!("failed to encode catalog manifest: {error}")))?;
    let mut metadata_page = SlottedPage::new(PageId::new(0), PageType::Metadata);
    if metadata_page.insert(&manifest_bytes)?.is_none() {
        return Err(DbError::new(
            "54000",
            "catalog manifest is too large for the v1 metadata page",
        )
        .with_hint("reduce catalog objects until multi-page metadata is supported"));
    }

    let mut pages = Vec::with_capacity(1 + heap_pages.len() + index_pages.len());
    pages.push(metadata_page);
    pages.extend(heap_pages);
    pages.extend(index_pages);
    Ok((pages, table_manifests, index_manifests))
}

struct LoadedState {
    data_format: DataFormat,
    state: PersistentState,
    tables: Vec<TableManifest>,
    indexes: Vec<IndexManifest>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestVersionHeader {
    magic: String,
    #[serde(alias = "format_version")]
    format_version: u16,
}

fn load_state(pool: &BufferPool) -> Result<LoadedState> {
    let page_count = pool.page_count()?;
    if page_count == 0 {
        return Err(corruption("database file does not contain a metadata page"));
    }
    let metadata = pool.fetch(PageId::new(0))?.snapshot()?;
    if metadata.page_type()? != PageType::Metadata {
        return Err(corruption("page zero is not a metadata page"));
    }
    if metadata.slot_count() != 1 {
        return Err(corruption(format!(
            "metadata page contains {} records, expected exactly one manifest",
            metadata.slot_count()
        )));
    }
    match detect_manifest_format(&metadata)? {
        DataFormat::V1 => {
            let (state, tables, indexes) = load_state_v1(pool)?;
            Ok(LoadedState {
                data_format: DataFormat::V1,
                state,
                tables,
                indexes,
            })
        }
        DataFormat::V2 => load_state_v2(pool),
    }
}

fn detect_manifest_format(metadata: &SlottedPage) -> Result<DataFormat> {
    if metadata.page_type()? != PageType::Metadata || metadata.slot_count() != 1 {
        return Err(corruption("page zero is not a single-record metadata page"));
    }
    let header: ManifestVersionHeader = serde_json::from_slice(metadata.record(0)?)
        .map_err(|error| corruption(format!("database manifest header is malformed: {error}")))?;
    if header.magic != MANIFEST_MAGIC {
        return Err(corruption("database manifest has an invalid magic value"));
    }
    DataFormat::from_version(header.format_version)
}

fn load_state_v2(pool: &BufferPool) -> Result<LoadedState> {
    let page_count = pool.page_count()?;
    let page_zero = pool.fetch(PageId::new(0))?.snapshot()?;
    let envelope: ManifestEnvelopeV2 = serde_json::from_slice(page_zero.record(0)?)
        .map_err(|error| corruption(format!("v2 manifest envelope is malformed: {error}")))?;
    if envelope.magic != MANIFEST_MAGIC
        || envelope.format_version != DATABASE_FORMAT_V2
        || envelope.envelope_version != MANIFEST_ENVELOPE_V2
    {
        return Err(DbError::new(
            "0A000",
            "database v2 manifest envelope version is not supported",
        )
        .with_hint("restore from a compatible logical backup"));
    }
    let manifest_length = usize::try_from(envelope.manifest_bytes)
        .map_err(|_| corruption("v2 manifest byte length exceeds this platform"))?;
    if manifest_length == 0 || manifest_length > MAX_MANIFEST_BYTES_V2 {
        return Err(corruption(format!(
            "v2 manifest byte length {} is outside 1..={MAX_MANIFEST_BYTES_V2}",
            envelope.manifest_bytes
        )));
    }
    let expected_chunks = manifest_length.div_ceil(MAX_RECORD_BYTES);
    if envelope.metadata_pages.len() != expected_chunks {
        return Err(corruption(format!(
            "v2 manifest declares {} metadata pages for {expected_chunks} chunks",
            envelope.metadata_pages.len()
        )));
    }
    for (index, page_id) in envelope.metadata_pages.iter().enumerate() {
        let expected = u64::try_from(index)
            .map_err(|_| corruption("v2 metadata page index exceeds the format limit"))?
            .checked_add(1)
            .ok_or_else(|| corruption("v2 metadata page ID overflowed"))?;
        if page_id.get() != expected || page_id.get() >= page_count {
            return Err(corruption(
                "v2 metadata pages must be a contiguous in-file sequence after page zero",
            ));
        }
    }

    let mut manifest_bytes = Vec::with_capacity(manifest_length);
    let mut referenced_pages = BTreeSet::from([PageId::new(0)]);
    for (index, page_id) in envelope.metadata_pages.iter().enumerate() {
        if !referenced_pages.insert(*page_id) {
            return Err(corruption("v2 metadata page is referenced more than once"));
        }
        let page = pool.fetch(*page_id)?.snapshot()?;
        if page.page_type()? != PageType::Metadata || page.slot_count() != 1 {
            return Err(corruption(format!(
                "v2 manifest page {} is not a single-record metadata page",
                page_id.get()
            )));
        }
        let chunk = page.record(0)?;
        let expected_length = if index + 1 == envelope.metadata_pages.len() {
            manifest_length
                .checked_sub(index.saturating_mul(MAX_RECORD_BYTES))
                .ok_or_else(|| corruption("v2 manifest chunk length underflowed"))?
        } else {
            MAX_RECORD_BYTES
        };
        if chunk.len() != expected_length {
            return Err(corruption(format!(
                "v2 manifest page {} contains {} bytes; expected {expected_length}",
                page_id.get(),
                chunk.len()
            )));
        }
        manifest_bytes.extend_from_slice(chunk);
    }
    if manifest_bytes.len() != manifest_length
        || sha256_hex(&manifest_bytes) != envelope.manifest_sha256
    {
        return Err(corruption(
            "v2 manifest length or SHA-256 does not match its envelope",
        ));
    }
    let manifest: ManifestV2 = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| corruption(format!("database v2 manifest is malformed: {error}")))?;
    if manifest.magic != MANIFEST_MAGIC
        || manifest.format_version != DATABASE_FORMAT_V2
        || manifest.tuple_format_version != TUPLE_FORMAT_V2
    {
        return Err(DbError::new(
            "0A000",
            "database v2 manifest or tuple version is not supported",
        )
        .with_hint("restore from a compatible logical backup"));
    }
    if manifest.index_rebuild != IndexRebuildContractV2::default() {
        return Err(DbError::new(
            "0A000",
            "database v2 index rebuild contract is not supported",
        )
        .with_hint("use a compatible OrdaDB build to rebuild derived indexes"));
    }

    let catalog_table_ids = manifest
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .map(|table| table.id)
        .collect::<BTreeSet<_>>();
    let manifest_table_ids = manifest
        .tables
        .iter()
        .map(|table| table.table_id)
        .collect::<BTreeSet<_>>();
    if manifest_table_ids.len() != manifest.tables.len() || manifest_table_ids != catalog_table_ids
    {
        return Err(corruption(
            "database v2 table directory does not match its catalog",
        ));
    }

    let mut tables = BTreeMap::new();
    for table in &manifest.tables {
        let mut rows = Vec::new();
        for page_id in &table.heap_pages {
            if page_id.get() == 0
                || page_id.get() >= page_count
                || !referenced_pages.insert(*page_id)
            {
                return Err(corruption(format!(
                    "v2 heap page {} is outside the file or referenced more than once",
                    page_id.get()
                )));
            }
            let page = pool.fetch(*page_id)?.snapshot()?;
            if page.page_type()? != PageType::Heap {
                return Err(corruption(format!(
                    "v2 table {} references non-heap page {}",
                    table.table_id.get(),
                    page_id.get()
                )));
            }
            for record in page.records()? {
                let (header, row) = decode_row_v2(&record)?;
                if usize::from(header.column_count) != row.values.len() {
                    return Err(corruption(
                        "v2 tuple header column count does not match the decoded row",
                    ));
                }
                rows.push(row);
            }
        }
        tables.insert(table.table_id, rows);
    }
    if u64::try_from(referenced_pages.len()).ok() != Some(page_count)
        || (0..page_count).any(|page_id| !referenced_pages.contains(&PageId::new(page_id)))
    {
        return Err(corruption(
            "database v2 file contains missing or unreferenced pages",
        ));
    }

    Ok(LoadedState {
        data_format: DataFormat::V2,
        state: PersistentState {
            generation: manifest.generation,
            catalog: manifest.catalog,
            tables,
            indexes: BTreeMap::new(),
        },
        tables: manifest.tables,
        indexes: Vec::new(),
    })
}

fn load_state_v1(
    pool: &BufferPool,
) -> Result<(PersistentState, Vec<TableManifest>, Vec<IndexManifest>)> {
    let page_count = pool.page_count()?;
    if page_count == 0 {
        return Err(corruption("database file does not contain a metadata page"));
    }
    let metadata = pool.fetch(PageId::new(0))?.snapshot()?;
    if metadata.page_type()? != PageType::Metadata {
        return Err(corruption("page zero is not a metadata page"));
    }
    if metadata.slot_count() != 1 {
        return Err(corruption(format!(
            "metadata page contains {} records, expected exactly one manifest",
            metadata.slot_count()
        )));
    }
    let manifest: ManifestV1 = serde_json::from_slice(metadata.record(0)?)
        .map_err(|error| corruption(format!("catalog manifest is malformed: {error}")))?;
    if manifest.magic != MANIFEST_MAGIC {
        return Err(corruption("catalog manifest has an invalid magic value"));
    }
    if manifest.format_version != DATABASE_FORMAT_V1 {
        return Err(unsupported_database_version(manifest.format_version));
    }

    let catalog_table_ids: BTreeSet<_> = manifest
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .map(|table| table.id)
        .collect();
    let manifest_table_ids: BTreeSet<_> =
        manifest.tables.iter().map(|table| table.table_id).collect();
    if manifest_table_ids.len() != manifest.tables.len() {
        return Err(corruption(
            "catalog manifest contains duplicate table entries",
        ));
    }
    if manifest_table_ids != catalog_table_ids {
        return Err(corruption(
            "catalog manifest table directory does not match the catalog",
        ));
    }

    let mut referenced_pages = BTreeSet::from([PageId::new(0)]);
    let mut tables = BTreeMap::new();
    for table in &manifest.tables {
        let mut rows = Vec::new();
        for page_id in &table.heap_pages {
            if page_id.get() == 0 || !referenced_pages.insert(*page_id) {
                return Err(corruption(format!(
                    "heap page {} is referenced more than once or aliases metadata",
                    page_id.get()
                )));
            }
            let page = pool.fetch(*page_id)?.snapshot()?;
            if page.page_type()? != PageType::Heap {
                return Err(corruption(format!(
                    "table {} references non-heap page {}",
                    table.table_id.get(),
                    page_id.get()
                )));
            }
            for record in page.records()? {
                rows.push(decode_row(&record)?);
            }
        }
        tables.insert(table.table_id, rows);
    }

    let catalog_index_ids = manifest
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .flat_map(|table| table.indexes())
        .filter(|index| index.method == IndexMethod::BTree)
        .map(|index| index.id)
        .collect::<BTreeSet<_>>();
    let manifest_index_ids = manifest
        .indexes
        .iter()
        .map(|index| index.index_id)
        .collect::<BTreeSet<_>>();
    if manifest_index_ids.len() != manifest.indexes.len() {
        return Err(corruption(
            "catalog manifest contains duplicate index entries",
        ));
    }
    if manifest_index_ids != catalog_index_ids {
        return Err(corruption(
            "catalog manifest index directory does not match the catalog",
        ));
    }

    let mut indexes = BTreeMap::new();
    for index in &manifest.indexes {
        let definition = manifest
            .catalog
            .index_by_id(index.index_id)
            .ok_or_else(|| corruption("manifest references an unknown index"))?;
        let owner_rows = tables
            .get(&definition.table_id)
            .ok_or_else(|| corruption("index owner table has no heap state"))?;
        let owner = manifest
            .catalog
            .table_by_id(definition.table_id)
            .ok_or_else(|| corruption("index owner table is absent from the catalog"))?;
        let mut entries = Vec::new();
        for page_id in &index.index_pages {
            if page_id.get() == 0 || !referenced_pages.insert(*page_id) {
                return Err(corruption(format!(
                    "index page {} is referenced more than once or aliases metadata",
                    page_id.get()
                )));
            }
            let page = pool.fetch(*page_id)?.snapshot()?;
            if page.page_type()? != PageType::Index {
                return Err(corruption(format!(
                    "index {} references non-index page {}",
                    index.index_id.get(),
                    page_id.get()
                )));
            }
            for record in page.records()? {
                entries.push(decode_index_entry(&record)?);
            }
        }
        validate_index_entries(definition, owner, owner_rows, &entries)?;
        indexes.insert(index.index_id, entries);
    }

    if u64::try_from(referenced_pages.len()).ok() != Some(page_count)
        || (0..page_count).any(|page_id| !referenced_pages.contains(&PageId::new(page_id)))
    {
        return Err(corruption(
            "database file contains missing or unreferenced pages",
        ));
    }

    Ok((
        PersistentState {
            generation: manifest.generation,
            catalog: manifest.catalog,
            tables,
            indexes,
        },
        manifest.tables,
        manifest.indexes,
    ))
}

fn validate_index_entries(
    definition: &IndexDefinition,
    owner: &TableDefinition,
    rows: &[Row],
    entries: &[IndexEntry],
) -> Result<()> {
    if entries.len() != rows.len() {
        return Err(corruption(format!(
            "index {} contains {} entries for {} heap rows",
            definition.id.get(),
            entries.len(),
            rows.len()
        )));
    }
    let mut referenced_rows = BTreeSet::new();
    let mut previous: Option<&IndexEntry> = None;
    for entry in entries {
        let row_index = usize::try_from(entry.row_id.get())
            .map_err(|_| corruption("index row reference exceeds the platform limit"))?;
        let row = rows.get(row_index).ok_or_else(|| {
            corruption(format!(
                "index {} row reference {} is outside its heap",
                definition.id.get(),
                entry.row_id.get()
            ))
        })?;
        if !referenced_rows.insert(entry.row_id) {
            return Err(corruption(format!(
                "index {} references row {} more than once",
                definition.id.get(),
                entry.row_id.get()
            )));
        }
        let key_values = definition
            .key_columns
            .iter()
            .map(|column_id| {
                owner
                    .column_index_by_id(*column_id)
                    .and_then(|position| row.values.get(position))
                    .cloned()
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                corruption(format!(
                    "index {} key shape does not match its heap row",
                    definition.id.get()
                ))
            })?;
        let expected_key = IndexKey::from_values(&key_values)?;
        if entry.key != expected_key {
            return Err(corruption(format!(
                "index {} key does not match heap row {}",
                definition.id.get(),
                entry.row_id.get()
            )));
        }
        let expected_included = definition
            .include_columns
            .iter()
            .map(|column_id| {
                owner
                    .column_index_by_id(*column_id)
                    .and_then(|position| row.values.get(position))
                    .cloned()
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                corruption(format!(
                    "index {} covering shape does not match its heap row",
                    definition.id.get()
                ))
            })?;
        if entry.included != expected_included {
            return Err(corruption(format!(
                "index {} covering payload does not match heap row {}",
                definition.id.get(),
                entry.row_id.get()
            )));
        }
        if let Some(previous) = previous {
            let ordering = previous
                .key
                .cmp(&entry.key)
                .then_with(|| previous.row_id.cmp(&entry.row_id));
            if ordering.is_gt() {
                return Err(corruption(format!(
                    "index {} entries are not ordered",
                    definition.id.get()
                )));
            }
            if definition.unique && !previous.key.contains_null() && previous.key == entry.key {
                return Err(corruption(format!(
                    "unique index {} contains a duplicate key",
                    definition.id.get()
                )));
            }
        }
        previous = Some(entry);
    }
    Ok(())
}

fn encode_index_entry(entry: &IndexEntry) -> Result<Vec<u8>> {
    let key = encode_row(&Row::new(entry.key.values()))?;
    let included = encode_row(&Row::new(entry.included.clone()))?;
    let key_len = u32::try_from(key.len())
        .map_err(|_| DbError::new("54000", "encoded index key is too large"))?;
    let included_len = u32::try_from(included.len())
        .map_err(|_| DbError::new("54000", "encoded covering payload is too large"))?;
    let mut bytes = Vec::with_capacity(18 + key.len() + included.len());
    bytes.extend_from_slice(&INDEX_RECORD_VERSION.to_le_bytes());
    bytes.extend_from_slice(&entry.row_id.get().to_le_bytes());
    bytes.extend_from_slice(&key_len.to_le_bytes());
    bytes.extend_from_slice(&key);
    bytes.extend_from_slice(&included_len.to_le_bytes());
    bytes.extend_from_slice(&included);
    Ok(bytes)
}

fn decode_index_entry(bytes: &[u8]) -> Result<IndexEntry> {
    let mut offset = 0;
    let version = read_u16(bytes, &mut offset)?;
    if version != INDEX_RECORD_VERSION {
        return Err(corruption(format!(
            "unsupported index record version {version}"
        )));
    }
    let row_id = RowId::new(read_u64(bytes, &mut offset)?);
    let key_len = usize::try_from(read_u32(bytes, &mut offset)?)
        .map_err(|_| corruption("index key length exceeds the platform limit"))?;
    let key_end = offset
        .checked_add(key_len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| corruption("index key record is truncated"))?;
    let key_values = decode_row(&bytes[offset..key_end])?.values;
    offset = key_end;
    let included_len = usize::try_from(read_u32(bytes, &mut offset)?)
        .map_err(|_| corruption("covering payload length exceeds the platform limit"))?;
    let included_end = offset
        .checked_add(included_len)
        .filter(|end| *end == bytes.len())
        .ok_or_else(|| corruption("index covering record is truncated or has trailing data"))?;
    let included = decode_row(&bytes[offset..included_end])?.values;
    IndexEntry::new(&key_values, row_id, included)
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> Result<u16> {
    let value = read_exact::<2>(bytes, offset)?;
    Ok(u16::from_le_bytes(value))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let value = read_exact::<4>(bytes, offset)?;
    Ok(u32::from_le_bytes(value))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let value = read_exact::<8>(bytes, offset)?;
    Ok(u64::from_le_bytes(value))
}

fn read_exact<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    let end = offset
        .checked_add(N)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| corruption("index record is truncated"))?;
    let mut value = [0_u8; N];
    value.copy_from_slice(&bytes[*offset..end]);
    *offset = end;
    Ok(value)
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn unsupported_database_version(version: u16) -> DbError {
    DbError::new(
        "0A000",
        format!("database format version {version} is not supported"),
    )
    .with_detail(format!(
        "this OrdaDB build supports database formats {DATABASE_FORMAT_V1} and {DATABASE_FORMAT_V2}"
    ))
    .with_hint("open v1 read-only for inspection or run an explicit supported migration")
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};

    use ordadb_catalog::NewColumn;
    use ordadb_types::{Identifier, ScalarType, Value};
    use tempfile::tempdir;

    use super::*;
    use crate::PAGE_SIZE;

    #[derive(Debug)]
    struct LimitedBarrier {
        durable_lsn: u64,
    }

    impl DurabilityBarrier for LimitedBarrier {
        fn flush_through(&self, page_lsn: u64) -> Result<()> {
            if page_lsn > self.durable_lsn {
                return Err(io_error(
                    "injected WAL durability failure",
                    std::io::Error::other("LSN is not durable"),
                ));
            }
            Ok(())
        }
    }

    fn populated_state() -> PersistentState {
        let mut catalog = Catalog::default();
        catalog
            .create_schema(Identifier::unquoted("app"))
            .expect("schema");
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("app"),
                Identifier::unquoted("events"),
                vec![
                    NewColumn {
                        name: Identifier::unquoted("id"),
                        data_type: ScalarType::Int64,
                        nullable: false,
                        primary_key: true,
                        unique: true,
                        default: None,
                    },
                    NewColumn::new(Identifier::unquoted("payload"), ScalarType::Text),
                ],
            )
            .expect("table");
        let rows = (0..80)
            .map(|id| {
                Row::new(vec![
                    Value::Int64(id),
                    Value::Text(format!("{id:04}-{}", "x".repeat(400))),
                ])
            })
            .collect::<Vec<_>>();
        let index_id = catalog
            .table_by_id(table_id)
            .expect("table")
            .indexes()
            .next()
            .expect("primary index")
            .id;
        let entries = rows
            .iter()
            .enumerate()
            .map(|(row_id, row)| {
                IndexEntry::new(&row.values[0..1], RowId::new(row_id as u64), Vec::new())
                    .expect("index entry")
            })
            .collect();
        PersistentState {
            generation: 9,
            catalog,
            tables: BTreeMap::from([(table_id, rows)]),
            indexes: BTreeMap::from([(index_id, entries)]),
        }
    }

    fn large_catalog_state(table_count: usize) -> PersistentState {
        let mut catalog = Catalog::default();
        let mut tables = BTreeMap::new();
        let mut indexes = BTreeMap::new();
        for table_number in 0..table_count {
            let table_id = catalog
                .create_table(
                    &Identifier::unquoted("public"),
                    Identifier::unquoted(format!("table_{table_number:04}")),
                    vec![NewColumn {
                        name: Identifier::unquoted("id"),
                        data_type: ScalarType::Int64,
                        nullable: false,
                        primary_key: true,
                        unique: true,
                        default: None,
                    }],
                )
                .expect("table");
            let index_id = catalog
                .table_by_id(table_id)
                .expect("table")
                .indexes()
                .next()
                .expect("primary index")
                .id;
            tables.insert(table_id, Vec::new());
            indexes.insert(index_id, Vec::new());
        }
        PersistentState {
            generation: 17,
            catalog,
            tables,
            indexes,
        }
    }

    #[test]
    fn persists_and_reopens_catalog_generation_and_multi_page_rows() {
        let directory = tempdir().expect("tempdir");
        let state = populated_state();
        let (table_manifests, index_manifests) = {
            let mut store = DatabaseStore::open_with_capacity(directory.path(), 2).expect("open");
            store.commit(&state).expect("commit");
            assert!(
                store.table_manifests()[0].heap_pages.len() > 1,
                "fixture must span heap pages"
            );
            assert!(
                !store.index_manifests()[0].index_pages.is_empty(),
                "primary index must own persisted index pages"
            );
            (
                store.table_manifests().to_vec(),
                store.index_manifests().to_vec(),
            )
        };

        let reopened =
            DatabaseStore::open_with_capacity(directory.path(), 2).expect("reopen database");
        assert_eq!(reopened.committed_state(), &state);
        assert_eq!(reopened.table_manifests(), table_manifests);
        assert_eq!(reopened.index_manifests(), index_manifests);
    }

    #[test]
    fn v2_uses_multi_page_manifests_versioned_tuples_and_read_only_inspection() {
        assert_eq!(
            serde_json::to_string(&DataFormat::V2).expect("format"),
            r#""v2""#
        );
        let directory = tempdir().expect("tempdir");
        let state = large_catalog_state(300);
        let estimate = DatabaseStore::estimate_state(&state, DataFormat::V2).expect("v2 estimate");
        assert!(estimate.metadata_pages > 2);
        assert_eq!(estimate.durable_index_pages, 0);
        {
            let mut store =
                DatabaseStore::open_with_format(directory.path(), DataFormat::V2).expect("open v2");
            store.commit(&state).expect("commit v2");
            assert_eq!(store.data_format(), DataFormat::V2);
            assert!(store.index_manifests().is_empty());
            assert!(
                store
                    .table_manifests()
                    .iter()
                    .all(|table| table.heap_pages.is_empty())
            );
            assert!(store.page_count().expect("pages") > 2);
            assert_eq!(store.page_count().expect("pages"), estimate.page_count);
        }

        let before = std::fs::read(directory.path().join(DATABASE_FILE_NAME)).expect("before");
        let inspection = DatabaseStore::inspect_read_only(directory.path()).expect("inspect");
        assert_eq!(inspection.data_format, DataFormat::V2);
        assert_eq!(inspection.generation, state.generation);
        assert_eq!(inspection.catalog, state.catalog);
        assert_eq!(inspection.table_rows.len(), 300);
        assert_eq!(inspection.durable_index_count, 0);
        assert!(inspection.persistent_state.indexes.is_empty());

        let mut read_only = DatabaseStore::open_read_only(directory.path()).expect("read only");
        assert_eq!(
            read_only
                .commit(&inspection.persistent_state)
                .expect_err("read-only commit")
                .sql_state,
            "25006"
        );
        drop(read_only);
        assert_eq!(
            std::fs::read(directory.path().join(DATABASE_FILE_NAME)).expect("after"),
            before
        );

        let reopened =
            DatabaseStore::open_with_format(directory.path(), DataFormat::V2).expect("reopen v2");
        assert_eq!(reopened.committed_state(), &inspection.persistent_state);
        assert!(reopened.index_manifests().is_empty());
    }

    #[test]
    fn v2_manifest_page_corruption_is_rejected_without_mutation() {
        let directory = tempdir().expect("tempdir");
        let state = large_catalog_state(300);
        let path = directory.path().join(DATABASE_FILE_NAME);
        {
            let mut store =
                DatabaseStore::open_with_format(directory.path(), DataFormat::V2).expect("open v2");
            store.commit(&state).expect("commit v2");
            assert!(
                DatabaseStore::estimate_state(&state, DataFormat::V2)
                    .expect("estimate")
                    .metadata_pages
                    > 2
            );
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open database");
        file.seek(SeekFrom::Start(PAGE_SIZE as u64 + 100))
            .expect("seek manifest continuation");
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).expect("read manifest byte");
        byte[0] ^= 0x80;
        file.seek(SeekFrom::Current(-1)).expect("rewind");
        file.write_all(&byte).expect("corrupt manifest page");
        file.sync_all().expect("sync corruption");
        drop(file);
        let before = std::fs::read(&path).expect("read before inspection");

        assert_eq!(
            DatabaseStore::inspect_read_only(directory.path())
                .expect_err("manifest corruption refused")
                .sql_state,
            "XX001"
        );
        assert_eq!(std::fs::read(path).expect("read after inspection"), before);
    }

    #[test]
    fn unsupported_v2_manifest_envelope_version_is_rejected_without_mutation() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join(DATABASE_FILE_NAME);
        drop(
            DatabaseStore::open_with_format(directory.path(), DataFormat::V2)
                .expect("bootstrap v2"),
        );
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open database");
        let mut page_bytes = [0_u8; PAGE_SIZE];
        file.read_exact(&mut page_bytes).expect("read page zero");
        let page = SlottedPage::from_bytes(&page_bytes, PageId::new(0)).expect("page zero");
        let mut envelope: ManifestEnvelopeV2 =
            serde_json::from_slice(page.record(0).expect("envelope")).expect("decode envelope");
        envelope.envelope_version += 1;
        let encoded = serde_json::to_vec(&envelope).expect("encode envelope");
        let mut replacement = SlottedPage::new(PageId::new(0), PageType::Metadata);
        replacement
            .insert(&encoded)
            .expect("insert envelope")
            .expect("envelope fits");
        file.seek(SeekFrom::Start(0)).expect("rewind");
        file.write_all(&replacement.sealed_bytes())
            .expect("write envelope");
        file.sync_all().expect("sync envelope");
        drop(file);
        let before = std::fs::read(&path).expect("read before inspection");

        assert_eq!(
            DatabaseStore::inspect_read_only(directory.path())
                .expect_err("unsupported envelope refused")
                .sql_state,
            "0A000"
        );
        assert_eq!(std::fs::read(path).expect("read after inspection"), before);
    }

    #[test]
    fn read_only_inspection_detects_legacy_v1_without_mutation() {
        let directory = tempdir().expect("tempdir");
        let state = populated_state();
        {
            let mut store = DatabaseStore::open(directory.path()).expect("open v1");
            store.commit(&state).expect("commit v1");
        }
        let path = directory.path().join(DATABASE_FILE_NAME);
        let before = std::fs::read(&path).expect("before");
        let inspection = DatabaseStore::inspect_read_only(directory.path()).expect("inspect");
        assert_eq!(inspection.data_format, DataFormat::V1);
        assert_eq!(inspection.persistent_state, state);
        assert!(!inspection.persistent_state.indexes.is_empty());
        assert_eq!(std::fs::read(path).expect("after"), before);
    }

    #[test]
    fn page_directories_use_compact_ranges_and_accept_legacy_lists() {
        let manifest = TableManifest {
            table_id: TableId::new(7),
            heap_pages: (1..=10_000).map(PageId::new).collect(),
        };
        let encoded = serde_json::to_string(&manifest).expect("compact manifest");
        assert!(encoded.contains("\"ranges\""));
        assert!(encoded.len() < 128, "{encoded}");
        assert_eq!(
            serde_json::from_str::<TableManifest>(&encoded).expect("compact round trip"),
            manifest
        );

        let legacy = r#"{"table_id":7,"heap_pages":[1,2,3]}"#;
        assert_eq!(
            serde_json::from_str::<TableManifest>(legacy)
                .expect("legacy page directory")
                .heap_pages,
            vec![PageId::new(1), PageId::new(2), PageId::new(3)]
        );
    }

    #[test]
    fn page_directories_reject_empty_overlapping_and_oversized_ranges() {
        let empty = r#"{"table_id":7,"heap_pages":{"ranges":[{"first":1,"count":0}]}}"#;
        assert!(serde_json::from_str::<TableManifest>(empty).is_err());
        let overlapping = r#"{"table_id":7,"heap_pages":{"ranges":[{"first":1,"count":3},{"first":2,"count":1}]}}"#;
        assert!(serde_json::from_str::<TableManifest>(overlapping).is_err());
        let oversized = format!(
            r#"{{"table_id":7,"heap_pages":{{"ranges":[{{"first":1,"count":{}}}]}}}}"#,
            MAX_MANIFEST_PAGE_REFERENCES + 1
        );
        assert!(serde_json::from_str::<TableManifest>(&oversized).is_err());
    }

    #[test]
    fn rejects_checksum_corruption_without_mutating_the_file() {
        let directory = tempdir().expect("tempdir");
        let state = populated_state();
        let path = directory.path().join(DATABASE_FILE_NAME);
        {
            let mut store = DatabaseStore::open(directory.path()).expect("open");
            store.commit(&state).expect("commit");
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("file");
        file.seek(SeekFrom::Start(PAGE_SIZE as u64 + 100))
            .expect("seek");
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).expect("read");
        byte[0] ^= 0xff;
        file.seek(SeekFrom::Current(-1)).expect("rewind");
        file.write_all(&byte).expect("corrupt");
        file.sync_all().expect("sync");
        let before = std::fs::read(&path).expect("before");

        assert_eq!(
            DatabaseStore::open(directory.path())
                .expect_err("corruption")
                .sql_state,
            "XX001"
        );
        assert_eq!(std::fs::read(path).expect("after"), before);
    }

    #[test]
    fn rejects_corrupt_index_pages_without_repairing_the_file() {
        let directory = tempdir().expect("tempdir");
        let state = populated_state();
        let path = directory.path().join(DATABASE_FILE_NAME);
        let index_page = {
            let mut store = DatabaseStore::open(directory.path()).expect("open");
            store.commit(&state).expect("commit");
            store.index_manifests()[0].index_pages[0]
        };
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("file");
        file.seek(SeekFrom::Start(index_page.get() * PAGE_SIZE as u64 + 100))
            .expect("seek index");
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).expect("read");
        byte[0] ^= 0xff;
        file.seek(SeekFrom::Current(-1)).expect("rewind");
        file.write_all(&byte).expect("corrupt");
        file.sync_all().expect("sync");
        let before = std::fs::read(&path).expect("before");

        assert_eq!(
            DatabaseStore::open(directory.path())
                .expect_err("index corruption")
                .sql_state,
            "XX001"
        );
        assert_eq!(std::fs::read(path).expect("after"), before);
    }

    #[test]
    fn rejects_unsupported_page_version_without_mutating_the_file() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join(DATABASE_FILE_NAME);
        drop(DatabaseStore::open(directory.path()).expect("bootstrap"));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("file");
        file.seek(SeekFrom::Start(4)).expect("seek");
        file.write_all(&(crate::FILE_FORMAT_VERSION + 1).to_le_bytes())
            .expect("version");
        file.sync_all().expect("sync");
        let before = std::fs::read(&path).expect("before");

        assert_eq!(
            DatabaseStore::open(directory.path())
                .expect_err("unsupported")
                .sql_state,
            "0A000"
        );
        assert_eq!(std::fs::read(path).expect("after"), before);
    }

    #[test]
    fn row_too_large_does_not_replace_the_committed_snapshot() {
        let directory = tempdir().expect("tempdir");
        let mut store = DatabaseStore::open(directory.path()).expect("open");
        let baseline = store.committed_state().clone();
        let mut candidate = populated_state();
        let rows = candidate.tables.values_mut().next().expect("table");
        rows.push(Row::new(vec![
            Value::Int64(999),
            Value::Text("x".repeat(PAGE_SIZE)),
        ]));

        assert_eq!(
            store.commit(&candidate).expect_err("too large").sql_state,
            "54000"
        );
        assert_eq!(store.committed_state(), &baseline);
    }

    #[test]
    fn prepared_commit_deltas_are_sorted_for_unchanged_growth_and_shrink() {
        let directory = tempdir().expect("tempdir");
        let mut store = DatabaseStore::open(directory.path()).expect("open");
        let unchanged = store
            .prepare_commit(store.committed_state())
            .expect("prepare unchanged");
        assert_eq!(unchanged.before_page_count(), 1);
        assert_eq!(unchanged.after_page_count(), 1);
        assert!(unchanged.page_deltas().is_empty());

        let populated = populated_state();
        let growth = store.prepare_commit(&populated).expect("prepare growth");
        assert!(growth.after_page_count() > growth.before_page_count());
        assert!(
            growth
                .page_deltas()
                .windows(2)
                .all(|pair| pair[0].page_id < pair[1].page_id)
        );
        assert!(
            growth
                .page_deltas()
                .iter()
                .any(|delta| delta.before.is_none() && delta.after.is_some())
        );

        store.commit(&populated).expect("commit growth");
        let baseline_page_count = store.page_count().expect("page count");
        let shrink = store
            .prepare_commit(&PersistentState::default())
            .expect("prepare shrink");
        assert_eq!(shrink.before_page_count(), baseline_page_count);
        assert_eq!(shrink.after_page_count(), 1);
        assert!(
            shrink
                .page_deltas()
                .windows(2)
                .all(|pair| pair[0].page_id < pair[1].page_id)
        );
        assert!(
            shrink
                .page_deltas()
                .iter()
                .any(|delta| delta.before.is_some() && delta.after.is_none())
        );
    }

    #[test]
    fn prepared_after_images_receive_their_page_update_lsns() {
        let directory = tempdir().expect("tempdir");
        let store = DatabaseStore::open(directory.path()).expect("open");
        let mut prepared = store
            .prepare_commit(&populated_state())
            .expect("prepare growth");
        let page_id = prepared
            .page_deltas()
            .iter()
            .find(|delta| delta.after.is_some())
            .expect("after image")
            .page_id;

        prepared.mark_after_lsn(page_id, 42).expect("mark LSN");
        assert_eq!(
            prepared
                .page_deltas()
                .iter()
                .find(|delta| delta.page_id == page_id)
                .and_then(|delta| delta.after.as_ref())
                .expect("marked after image")
                .lsn(),
            42
        );
        assert_eq!(
            prepared
                .mark_after_lsn(page_id, 0)
                .expect_err("zero LSN")
                .sql_state,
            "22023"
        );
    }

    #[test]
    fn apply_failure_and_pre_commit_success_do_not_publish_metadata() {
        let directory = tempdir().expect("tempdir");
        let barrier = Arc::new(LimitedBarrier { durable_lsn: 1 });
        let mut store = DatabaseStore::open_with_barrier(directory.path(), barrier).expect("open");
        let baseline = store.committed_state().clone();
        let baseline_tables = store.table_manifests().to_vec();
        let candidate = populated_state();
        let mut prepared = store.prepare_commit(&candidate).expect("prepare");
        let after_page_ids = prepared
            .page_deltas()
            .iter()
            .filter(|delta| delta.after.is_some())
            .map(|delta| delta.page_id)
            .collect::<Vec<_>>();
        assert!(after_page_ids.len() > 1);
        for (index, page_id) in after_page_ids.into_iter().enumerate() {
            prepared
                .mark_after_lsn(page_id, index as u64 + 1)
                .expect("mark");
        }

        assert_eq!(
            store
                .apply_prepared(&prepared)
                .expect_err("barrier failure")
                .sql_state,
            "58030"
        );
        assert_eq!(store.committed_state(), &baseline);
        assert_eq!(store.table_manifests(), baseline_tables);

        let clean_directory = tempdir().expect("clean tempdir");
        let mut clean_store = DatabaseStore::open(clean_directory.path()).expect("open clean");
        let clean_baseline = clean_store.committed_state().clone();
        let clean_prepared = clean_store
            .prepare_commit(&candidate)
            .expect("prepare clean");
        clean_store
            .apply_prepared(&clean_prepared)
            .expect("apply clean");
        assert_eq!(clean_store.committed_state(), &clean_baseline);
        clean_store
            .publish_prepared(clean_prepared)
            .expect("publish clean");
        assert_eq!(clean_store.committed_state(), &candidate);
    }

    #[test]
    fn prepared_apply_reports_ordered_page_resize_and_sync_boundaries() {
        let directory = tempdir().expect("tempdir");
        let mut store = DatabaseStore::open(directory.path()).expect("open");
        let prepared = store
            .prepare_commit(&populated_state())
            .expect("prepare growth");
        let mut points = Vec::new();

        store
            .apply_prepared_with_observer(&prepared, |point| {
                points.push(point);
                Ok(())
            })
            .expect("apply with observer");

        assert!(matches!(
            points.first(),
            Some(ApplyPoint::BeforePageWrite(page_id)) if *page_id == PageId::new(0)
        ));
        assert!(points.windows(2).any(|pair| {
            matches!(
                pair,
                [
                    ApplyPoint::BeforeResize { .. },
                    ApplyPoint::AfterResize { .. }
                ]
            )
        }));
        assert!(matches!(
            points.as_slice(),
            [.., ApplyPoint::BeforeSync, ApplyPoint::AfterSync]
        ));
        assert_eq!(
            points
                .iter()
                .filter(|point| matches!(point, ApplyPoint::BeforePageWrite(_)))
                .count(),
            points
                .iter()
                .filter(|point| matches!(point, ApplyPoint::AfterPageWrite(_)))
                .count()
        );
    }
}
