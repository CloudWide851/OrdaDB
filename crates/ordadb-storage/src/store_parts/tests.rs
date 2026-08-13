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
                    declared_type: None,
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
        versions: BTreeMap::new(),
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
                    declared_type: None,
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
        versions: BTreeMap::new(),
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
fn v2_table_cursor_streams_bounded_rows_across_pages_and_stays_exhausted() {
    let directory = tempdir().expect("tempdir");
    let state = populated_state();
    let table_id = *state.tables.keys().next().expect("table id");
    let expected = state.tables[&table_id].clone();
    let mut store =
        DatabaseStore::open_with_format(directory.path(), DataFormat::V2).expect("open v2");
    store.commit(&state).expect("commit v2");
    assert!(
        store.table_manifests()[0].heap_pages.len() > 1,
        "fixture must span v2 heap pages"
    );

    let mut cursor = store
        .open_table_cursor_v2(table_id, state.generation)
        .expect("cursor");
    assert_eq!(cursor.table_id(), table_id);
    assert_eq!(cursor.generation(), state.generation);
    assert_eq!(cursor.expected_rows(), expected.len() as u64);
    assert_eq!(
        store
            .read_table_cursor_v2(&mut cursor, 0)
            .expect_err("zero chunk rejected")
            .sql_state,
        "22023"
    );

    let mut actual = Vec::new();
    loop {
        let rows = store
            .read_table_cursor_v2(&mut cursor, 7)
            .expect("cursor chunk");
        assert!(rows.len() <= 7);
        if rows.is_empty() {
            break;
        }
        actual.extend(rows);
    }
    assert_eq!(actual, expected);
    assert_eq!(cursor.emitted_rows(), cursor.expected_rows());
    assert!(cursor.is_exhausted());
    assert!(
        store
            .read_table_cursor_v2(&mut cursor, 7)
            .expect("post exhaustion")
            .is_empty()
    );
}

#[test]
fn v2_version_cursor_preserves_headers_and_stable_predecessor_ordinals() {
    let directory = tempdir().expect("tempdir");
    let mut state = PersistentState {
        generation: 1,
        ..PersistentState::default()
    };
    let table_id = state
        .catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("versions"),
            vec![NewColumn::new(
                Identifier::unquoted("value"),
                ScalarType::Int64,
            )],
        )
        .expect("table");
    let original = Row::new(vec![Value::Int64(1)]);
    let updated = Row::new(vec![Value::Int64(2)]);
    state.tables.insert(table_id, vec![updated.clone()]);
    state.versions.insert(
        table_id,
        vec![
            VersionedRow {
                version_id: 1,
                header: TupleHeaderV2 {
                    flags: 0,
                    column_count: 1,
                    xmin: 10,
                    xmax: 11,
                    command_id: 0,
                    previous_version: 0,
                },
                row: original.clone(),
            },
            VersionedRow {
                version_id: 2,
                header: TupleHeaderV2 {
                    flags: 0,
                    column_count: 1,
                    xmin: 11,
                    xmax: 0,
                    command_id: 0,
                    previous_version: 1,
                },
                row: updated.clone(),
            },
        ],
    );

    {
        let mut store =
            DatabaseStore::open_with_format(directory.path(), DataFormat::V2).expect("open");
        store.commit(&state).expect("commit");
    }
    let store =
        DatabaseStore::open_with_format(directory.path(), DataFormat::V2).expect("reopen");
    assert_eq!(
        store.committed_state().tables.get(&table_id),
        Some(&vec![updated])
    );
    let mut cursor = store
        .open_table_cursor_v2(table_id, state.generation)
        .expect("cursor");
    let versions = store
        .read_versioned_table_cursor_v2(&mut cursor, 8)
        .expect("versions");
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version_id, 1);
    assert_eq!(versions[0].row, original);
    assert_eq!(versions[1].version_id, 2);
    assert_eq!(versions[1].header.previous_version, 1);
    assert!(cursor.is_exhausted());
}

#[test]
fn v2_table_cursor_rejects_stale_generation_and_inconsistent_directory_state() {
    let directory = tempdir().expect("tempdir");
    let state = populated_state();
    let table_id = *state.tables.keys().next().expect("table id");
    let mut store =
        DatabaseStore::open_with_format(directory.path(), DataFormat::V2).expect("open v2");
    store.commit(&state).expect("commit v2");

    let mut stale = store
        .open_table_cursor_v2(table_id, state.generation)
        .expect("stale candidate");
    let mut next = state.clone();
    next.generation += 1;
    store.commit(&next).expect("next generation");
    assert_eq!(
        store
            .read_table_cursor_v2(&mut stale, 1)
            .expect_err("stale generation")
            .sql_state,
        "55000"
    );
    assert_eq!(
        store
            .open_table_cursor_v2(TableId::new(u64::MAX), next.generation)
            .expect_err("unknown table")
            .sql_state,
        "42P01"
    );

    let mut wrong_page = store
        .open_table_cursor_v2(table_id, next.generation)
        .expect("wrong-page cursor");
    wrong_page.heap_pages[0] = PageId::new(0);
    assert_eq!(
        store
            .read_table_cursor_v2(&mut wrong_page, 1)
            .expect_err("metadata page is not heap")
            .sql_state,
        "XX001"
    );

    let mut wrong_count = store
        .open_table_cursor_v2(table_id, next.generation)
        .expect("wrong-count cursor");
    wrong_count.expected_rows += 1;
    while !wrong_count.is_exhausted() {
        match store.read_table_cursor_v2(&mut wrong_count, 64) {
            Ok(_) => {}
            Err(error) => {
                assert_eq!(error.sql_state, "XX001");
                return;
            }
        }
    }
    panic!("row-count mismatch must fail before exhaustion");
}

#[test]
fn v2_table_cursor_is_not_a_legacy_format_fallback() {
    let directory = tempdir().expect("tempdir");
    let state = populated_state();
    let table_id = *state.tables.keys().next().expect("table id");
    let mut store =
        DatabaseStore::open_with_format(directory.path(), DataFormat::V1).expect("open v1");
    store.commit(&state).expect("commit v1");
    assert_eq!(
        store
            .open_table_cursor_v2(table_id, state.generation)
            .expect_err("v1 cursor rejected")
            .sql_state,
        "0A000"
    );
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
