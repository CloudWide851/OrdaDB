use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use ordadb_catalog::{Catalog, IndexDefinition, TableDefinition};
use ordadb_index::{IndexEntry, IndexKey, RowId};
use ordadb_types::{DbError, IndexId, Result, Row, TableId};
use serde::{Deserialize, Serialize};

use crate::{
    BufferPool, DiskManager, FILE_FORMAT_VERSION, NoWalBarrier, PageId, PageType, SlottedPage,
    corruption, decode_row, encode_row, io_error, unsupported_version,
};

const DATABASE_FILE_NAME: &str = "ordadb.data";
const MANIFEST_MAGIC: &str = "ORDADB";
const DEFAULT_BUFFER_CAPACITY: usize = 64;
const INDEX_RECORD_VERSION: u16 = 1;

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
    pub heap_pages: Vec<PageId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexManifest {
    pub index_id: IndexId,
    pub index_pages: Vec<PageId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    magic: String,
    format_version: u16,
    generation: u64,
    catalog: Catalog,
    tables: Vec<TableManifest>,
    #[serde(default)]
    indexes: Vec<IndexManifest>,
}

#[derive(Debug)]
pub struct DatabaseStore {
    pool: BufferPool,
    committed_state: PersistentState,
    committed_tables: Vec<TableManifest>,
    committed_indexes: Vec<IndexManifest>,
}

impl DatabaseStore {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_capacity(data_dir, DEFAULT_BUFFER_CAPACITY)
    }

    pub fn open_with_capacity(data_dir: impl AsRef<Path>, capacity: usize) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir)
            .map_err(|error| io_error("failed to create database data directory", error))?;
        let disk = DiskManager::open(data_dir.join(DATABASE_FILE_NAME))?;
        let pool = BufferPool::new(disk, capacity, Arc::new(NoWalBarrier))?;
        if pool.page_count()? == 0 {
            let mut store = Self {
                pool,
                committed_state: PersistentState::default(),
                committed_tables: Vec::new(),
                committed_indexes: Vec::new(),
            };
            let bootstrap = PersistentState::default();
            store.commit(&bootstrap)?;
            return Ok(store);
        }

        let (committed_state, committed_tables, committed_indexes) = load_state(&pool)?;
        Ok(Self {
            pool,
            committed_state,
            committed_tables,
            committed_indexes,
        })
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
        let (pages, table_manifests, index_manifests) = build_snapshot(candidate)?;
        self.pool.reset_storage()?;
        for page in pages {
            self.pool.install(page, true)?;
        }
        self.pool.flush_all()?;
        self.pool.sync_all()?;
        self.committed_state = candidate.clone();
        self.committed_tables = table_manifests;
        self.committed_indexes = index_manifests;
        Ok(())
    }
}

fn build_snapshot(
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

    let manifest = Manifest {
        magic: MANIFEST_MAGIC.to_owned(),
        format_version: FILE_FORMAT_VERSION,
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

fn load_state(
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
    let manifest: Manifest = serde_json::from_slice(metadata.record(0)?)
        .map_err(|error| corruption(format!("catalog manifest is malformed: {error}")))?;
    if manifest.magic != MANIFEST_MAGIC {
        return Err(corruption("catalog manifest has an invalid magic value"));
    }
    if manifest.format_version != FILE_FORMAT_VERSION {
        return Err(unsupported_version(manifest.format_version));
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

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};

    use ordadb_catalog::NewColumn;
    use ordadb_types::{Identifier, ScalarType, Value};
    use tempfile::tempdir;

    use super::*;
    use crate::PAGE_SIZE;

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
        file.write_all(&(FILE_FORMAT_VERSION + 1).to_le_bytes())
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
}
