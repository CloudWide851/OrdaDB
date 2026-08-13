
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

    pub fn open_table_cursor_v2(
        &self,
        table_id: TableId,
        expected_generation: u64,
    ) -> Result<StorageTableCursorV2> {
        if self.data_format != DataFormat::V2 {
            return Err(DbError::new(
                "0A000",
                "persistent table cursors require database format v2",
            )
            .with_hint("migrate the v1 database before using storage-backed scans"));
        }
        if self.committed_state.generation != expected_generation {
            return Err(stale_scan_generation(
                expected_generation,
                self.committed_state.generation,
            ));
        }
        let manifest = self
            .committed_tables
            .iter()
            .find(|manifest| manifest.table_id == table_id)
            .ok_or_else(|| {
                DbError::new(
                    "42P01",
                    format!(
                        "table {} does not exist in the v2 heap directory",
                        table_id.get()
                    ),
                )
            })?;
        let expected_rows = self
            .committed_state
            .versions
            .get(&table_id)
            .map(Vec::len)
            .or_else(|| self.committed_state.tables.get(&table_id).map(Vec::len))
            .ok_or_else(|| corruption("v2 table directory has no matching row state"))?;
        let expected_rows = u64::try_from(expected_rows)
            .map_err(|_| corruption("v2 table row count exceeds the storage format limit"))?;
        let expected_visible_rows = self
            .committed_state
            .tables
            .get(&table_id)
            .map(Vec::len)
            .ok_or_else(|| corruption("v2 table directory has no visible row state"))
            .and_then(|rows| {
                u64::try_from(rows).map_err(|_| {
                    corruption("v2 visible row count exceeds the storage format limit")
                })
            })?;
        let page_count = self.pool.page_count()?;
        let mut previous = None;
        for page_id in &manifest.heap_pages {
            if page_id.get() == 0
                || page_id.get() >= page_count
                || previous.is_some_and(|previous| previous >= *page_id)
            {
                return Err(corruption(
                    "v2 table cursor page directory is not a sorted in-file heap sequence",
                ));
            }
            previous = Some(*page_id);
        }
        if expected_rows == 0 && !manifest.heap_pages.is_empty() {
            return Err(corruption(
                "empty v2 table references one or more heap pages",
            ));
        }
        if expected_rows > 0 && manifest.heap_pages.is_empty() {
            return Err(corruption("non-empty v2 table has no heap pages"));
        }
        Ok(StorageTableCursorV2 {
            table_id,
            generation: expected_generation,
            heap_pages: manifest.heap_pages.clone(),
            expected_rows,
            expected_visible_rows,
            emitted_rows: 0,
            page_index: 0,
            slot_index: 0,
            exhausted: expected_rows == 0,
        })
    }

    pub fn read_table_cursor_v2(
        &self,
        cursor: &mut StorageTableCursorV2,
        max_rows: usize,
    ) -> Result<Vec<Row>> {
        self.read_versioned_table_cursor_v2(cursor, max_rows)
            .map(|versions| versions.into_iter().map(|version| version.row).collect())
    }

    pub fn read_versioned_table_cursor_v2(
        &self,
        cursor: &mut StorageTableCursorV2,
        max_rows: usize,
    ) -> Result<Vec<VersionedRow>> {
        if max_rows == 0 {
            return Err(DbError::new(
                "22023",
                "persistent table scan chunk size must be positive",
            ));
        }
        if cursor.exhausted {
            return Ok(Vec::new());
        }
        if self.data_format != DataFormat::V2 {
            return Err(DbError::new(
                "0A000",
                "persistent table cursor cannot read a non-v2 database",
            ));
        }
        if self.committed_state.generation != cursor.generation {
            return Err(stale_scan_generation(
                cursor.generation,
                self.committed_state.generation,
            ));
        }
        if !self
            .committed_tables
            .iter()
            .any(|manifest| manifest.table_id == cursor.table_id)
        {
            return Err(corruption(
                "v2 table cursor owner disappeared from the heap directory",
            ));
        }

        let mut versions = Vec::with_capacity(max_rows.min(1024));
        while versions.len() < max_rows && cursor.page_index < cursor.heap_pages.len() {
            let page_id = cursor.heap_pages[cursor.page_index];
            let page = self.pool.fetch(page_id)?.snapshot()?;
            if page.page_type()? != PageType::Heap {
                return Err(corruption(format!(
                    "v2 table {} cursor references non-heap page {}",
                    cursor.table_id.get(),
                    page_id.get()
                )));
            }
            let slot_count = page.slot_count();
            if cursor.slot_index > slot_count {
                return Err(corruption(
                    "v2 table cursor slot position exceeds its heap page",
                ));
            }
            while versions.len() < max_rows && cursor.slot_index < slot_count {
                let record = page.record(cursor.slot_index)?;
                let (header, row) = decode_row_v2(record)?;
                if usize::from(header.column_count) != row.values.len() {
                    return Err(corruption(
                        "v2 tuple header column count does not match the decoded row",
                    ));
                }
                cursor.slot_index = cursor
                    .slot_index
                    .checked_add(1)
                    .ok_or_else(|| corruption("v2 table cursor slot position overflowed"))?;
                let version_id = cursor
                    .emitted_rows
                    .checked_add(1)
                    .and_then(|version_id| u32::try_from(version_id).ok())
                    .ok_or_else(|| {
                        corruption("v2 table version ordinal exceeds the u32 format limit")
                    })?;
                if header.previous_version >= version_id {
                    return Err(corruption(
                        "v2 tuple predecessor is not earlier than its version ordinal",
                    ));
                }
                cursor.emitted_rows = cursor
                    .emitted_rows
                    .checked_add(1)
                    .ok_or_else(|| corruption("v2 table cursor emitted-row count overflowed"))?;
                if cursor.emitted_rows > cursor.expected_rows {
                    return Err(corruption(
                        "v2 table cursor decoded more rows than the manifest state declares",
                    ));
                }
                versions.push(VersionedRow {
                    version_id,
                    header,
                    row,
                });
            }
            if cursor.slot_index == slot_count {
                cursor.page_index = cursor
                    .page_index
                    .checked_add(1)
                    .ok_or_else(|| corruption("v2 table cursor page position overflowed"))?;
                cursor.slot_index = 0;
            }
        }

        if cursor.page_index == cursor.heap_pages.len() {
            if cursor.emitted_rows != cursor.expected_rows {
                return Err(corruption(format!(
                    "v2 table cursor decoded {} rows; expected {}",
                    cursor.emitted_rows, cursor.expected_rows
                )));
            }
            cursor.exhausted = true;
        }
        Ok(versions)
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

fn stale_scan_generation(expected: u64, actual: u64) -> DbError {
    DbError::new(
        "55000",
        "persistent table cursor no longer matches the committed generation",
    )
    .with_detail(format!(
        "cursor generation {expected}, committed generation {actual}"
    ))
    .with_hint("restart the statement against a fresh committed snapshot")
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
    for table_id in state.versions.keys() {
        if !catalog_table_ids.contains(table_id) {
            return Err(corruption(format!(
                "version state references unknown table {}",
                table_id.get()
            )));
        }
    }
    Ok(catalog_table_ids)
}
