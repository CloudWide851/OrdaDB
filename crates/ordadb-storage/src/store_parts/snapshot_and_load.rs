
fn build_v2_heap_pages(
    state: &PersistentState,
    catalog_table_ids: &BTreeSet<TableId>,
    first_heap_page: u64,
) -> Result<(Vec<SlottedPage>, Vec<TableManifest>)> {
    let mut next_page_id = first_heap_page;
    let mut heap_pages = Vec::new();
    let mut table_manifests = Vec::with_capacity(catalog_table_ids.len());
    for table_id in catalog_table_ids {
        let mut page_ids = Vec::new();
        let mut current_page: Option<SlottedPage> = None;
        {
            let mut append_version = |header: TupleHeaderV2, row: &Row| -> Result<()> {
                let encoded = encode_row_v2(row, header)?;
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
                Ok(())
            };
            if let Some(versions) = state.versions.get(table_id) {
                for (index, version) in versions.iter().enumerate() {
                    let expected_version = u32::try_from(index)
                        .ok()
                        .and_then(|index| index.checked_add(1))
                        .ok_or_else(|| {
                            DbError::new(
                                "54000",
                                format!(
                                    "v2 table {} exceeds the u32 version ordinal limit",
                                    table_id.get()
                                ),
                            )
                        })?;
                    if version.version_id != expected_version
                        || version.header.previous_version >= version.version_id
                    {
                        return Err(corruption(format!(
                            "v2 table {} has an invalid version chain at ordinal {}",
                            table_id.get(),
                            version.version_id
                        )));
                    }
                    append_version(version.header, &version.row)?;
                }
            } else {
                let rows = state.tables.get(table_id).map_or(&[][..], Vec::as_slice);
                for row in rows {
                    append_version(TupleHeaderV2::frozen(row)?, row)?;
                }
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
    let mut versions = BTreeMap::new();
    for table in &manifest.tables {
        let mut rows = Vec::new();
        let mut table_versions = Vec::new();
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
                let version_id = u32::try_from(table_versions.len())
                    .ok()
                    .and_then(|version_id| version_id.checked_add(1))
                    .ok_or_else(|| {
                        corruption("v2 table version ordinal exceeds the u32 format limit")
                    })?;
                if header.previous_version >= version_id {
                    return Err(corruption(format!(
                        "v2 table {} tuple {} has a non-backward predecessor",
                        table.table_id.get(),
                        version_id
                    )));
                }
                if header.xmax == 0 {
                    rows.push(row.clone());
                }
                table_versions.push(VersionedRow {
                    version_id,
                    header,
                    row,
                });
            }
        }
        tables.insert(table.table_id, rows);
        versions.insert(table.table_id, table_versions);
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
            versions,
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
            versions: BTreeMap::new(),
            indexes,
        },
        manifest.tables,
        manifest.indexes,
    ))
}
