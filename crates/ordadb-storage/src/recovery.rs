use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use ordadb_types::Result;

use crate::store::DATABASE_FILE_NAME;
use crate::{PAGE_SIZE, PageId, SlottedPage, corruption, io_error};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryFileState {
    pub complete_page_count: u64,
    pub has_partial_tail: bool,
}

impl RecoveryFileState {
    #[must_use]
    pub fn logical_page_count(self) -> u64 {
        self.complete_page_count + u64::from(self.has_partial_tail)
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    target_page_count: u64,
    page_updates: BTreeSet<PageId>,
}

impl RecoveryPlan {
    #[must_use]
    pub fn new(target_page_count: u64, page_updates: impl IntoIterator<Item = PageId>) -> Self {
        Self {
            target_page_count,
            page_updates: page_updates.into_iter().collect(),
        }
    }

    #[must_use]
    pub const fn target_page_count(&self) -> u64 {
        self.target_page_count
    }
}

#[derive(Debug)]
pub struct RecoveryDataFile {
    path: PathBuf,
    file: File,
    plan: RecoveryPlan,
}

impl RecoveryDataFile {
    pub fn inspect(data_dir: impl AsRef<Path>) -> Result<RecoveryFileState> {
        let path = data_dir.as_ref().join(DATABASE_FILE_NAME);
        match std::fs::metadata(&path) {
            Ok(metadata) => Ok(file_state(metadata.len())),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(file_state(0)),
            Err(error) => Err(io_error(
                "failed to inspect database file for recovery",
                error,
            )),
        }
    }

    pub fn open(data_dir: impl AsRef<Path>, plan: RecoveryPlan) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir)
            .map_err(|error| io_error("failed to create database recovery directory", error))?;
        let path = data_dir.join(DATABASE_FILE_NAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| io_error("failed to open database file for recovery", error))?;
        let state = inspect_file(&file)?;
        if state.has_partial_tail {
            let partial_page_id = PageId::new(state.complete_page_count);
            let tail_will_be_removed = plan.target_page_count <= state.complete_page_count;
            if !tail_will_be_removed && !plan.page_updates.contains(&partial_page_id) {
                return Err(corruption(format!(
                    "database file has a trailing partial page {} without a recovery update",
                    partial_page_id.get()
                )));
            }
        }
        target_length(plan.target_page_count)?;
        Ok(Self { path, file, plan })
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Option<SlottedPage>> {
        let state = inspect_file(&self.file)?;
        if page_id.get() >= state.complete_page_count {
            if page_id.get() == state.complete_page_count
                && state.has_partial_tail
                && self.plan.page_updates.contains(&page_id)
            {
                return Ok(None);
            }
            if page_id.get() >= state.logical_page_count()
                && self.plan.page_updates.contains(&page_id)
            {
                return Ok(None);
            }
            return Err(corruption(format!(
                "recovery page {} is outside the database file recovery plan",
                page_id.get()
            )));
        }

        let mut bytes = [0_u8; PAGE_SIZE];
        self.file
            .seek(SeekFrom::Start(page_offset(page_id)?))
            .map_err(|error| io_error("failed to seek to database page during recovery", error))?;
        self.file
            .read_exact(&mut bytes)
            .map_err(|error| io_error("failed to read database page during recovery", error))?;
        match SlottedPage::from_bytes(&bytes, page_id) {
            Ok(page) => Ok(Some(page)),
            Err(_) if self.plan.page_updates.contains(&page_id) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn apply_page(&mut self, page: &SlottedPage) -> Result<()> {
        page.validate()?;
        let page_id = page.page_id();
        if !self.plan.page_updates.contains(&page_id) {
            return Err(corruption(format!(
                "page {} is not enumerated by the recovery plan",
                page_id.get()
            )));
        }
        let state = inspect_file(&self.file)?;
        if page_id.get() > state.complete_page_count {
            return Err(corruption(format!(
                "cannot apply sparse recovery page {} after {} complete pages",
                page_id.get(),
                state.complete_page_count
            )));
        }
        self.file
            .seek(SeekFrom::Start(page_offset(page_id)?))
            .map_err(|error| io_error("failed to seek to database page during recovery", error))?;
        self.file
            .write_all(&page.sealed_bytes())
            .map_err(|error| io_error("failed to apply database page during recovery", error))
    }

    pub fn resize_pages(&mut self, page_count: u64) -> Result<()> {
        self.file
            .set_len(target_length(page_count)?)
            .map_err(|error| io_error("failed to resize database file during recovery", error))
    }

    pub fn sync_all(&self) -> Result<()> {
        self.file
            .sync_all()
            .map_err(|error| io_error("failed to synchronize database recovery", error))
    }

    pub fn finish(mut self) -> Result<()> {
        let state = inspect_file(&self.file)?;
        if state.has_partial_tail || state.complete_page_count != self.plan.target_page_count {
            return Err(corruption(format!(
                "recovery finished with {} complete pages{}; expected {} pages",
                state.complete_page_count,
                if state.has_partial_tail {
                    " and a partial tail"
                } else {
                    ""
                },
                self.plan.target_page_count
            )));
        }
        for page_index in 0..state.complete_page_count {
            let page_id = PageId::new(page_index);
            let mut bytes = [0_u8; PAGE_SIZE];
            self.file
                .seek(SeekFrom::Start(page_offset(page_id)?))
                .map_err(|error| {
                    io_error("failed to seek while validating recovered database", error)
                })?;
            self.file.read_exact(&mut bytes).map_err(|error| {
                io_error("failed to read while validating recovered database", error)
            })?;
            SlottedPage::from_bytes(&bytes, page_id)?;
        }
        self.sync_all()
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn inspect_file(file: &File) -> Result<RecoveryFileState> {
    let length = file
        .metadata()
        .map_err(|error| io_error("failed to inspect database file during recovery", error))?
        .len();
    Ok(file_state(length))
}

fn file_state(length: u64) -> RecoveryFileState {
    RecoveryFileState {
        complete_page_count: length / PAGE_SIZE as u64,
        has_partial_tail: length % PAGE_SIZE as u64 != 0,
    }
}

fn target_length(page_count: u64) -> Result<u64> {
    page_count
        .checked_mul(PAGE_SIZE as u64)
        .ok_or_else(|| corruption("database recovery file length overflow"))
}

fn page_offset(page_id: PageId) -> Result<u64> {
    target_length(page_id.get())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PageType;
    use tempfile::tempdir;

    #[test]
    fn raw_recovery_overwrites_grows_shrinks_and_validates() {
        let directory = tempdir().expect("tempdir");
        let page_zero = SlottedPage::new(PageId::new(0), PageType::Metadata);
        let page_one = SlottedPage::new(PageId::new(1), PageType::Heap);
        let plan = RecoveryPlan::new(2, [PageId::new(0), PageId::new(1)]);
        let mut data = RecoveryDataFile::open(directory.path(), plan).expect("open recovery");

        assert!(
            data.read_page(PageId::new(0))
                .expect("missing page")
                .is_none()
        );
        data.apply_page(&page_zero).expect("page zero");
        data.apply_page(&page_one).expect("page one");
        assert_eq!(
            data.read_page(PageId::new(1))
                .expect("read page")
                .expect("present page")
                .page_id(),
            PageId::new(1)
        );
        data.resize_pages(1).expect("shrink");
        data.resize_pages(2).expect("grow");
        assert!(
            data.read_page(PageId::new(1))
                .expect("planned zero page")
                .is_none()
        );
        data.apply_page(&page_one).expect("restore page one");
        data.finish().expect("finish");
    }

    #[test]
    fn partial_tail_requires_and_obeys_explicit_recovery_plan() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join(DATABASE_FILE_NAME);
        std::fs::write(&path, [0_u8; 17]).expect("partial tail");
        let state = RecoveryDataFile::inspect(directory.path()).expect("inspect");
        assert_eq!(
            state,
            RecoveryFileState {
                complete_page_count: 0,
                has_partial_tail: true,
            }
        );
        assert_eq!(state.logical_page_count(), 1);

        assert_eq!(
            RecoveryDataFile::open(directory.path(), RecoveryPlan::new(1, []))
                .expect_err("unplanned partial tail")
                .sql_state,
            "XX001"
        );

        let mut recovery =
            RecoveryDataFile::open(directory.path(), RecoveryPlan::new(1, [PageId::new(0)]))
                .expect("planned partial tail");
        assert!(
            recovery
                .read_page(PageId::new(0))
                .expect("read partial")
                .is_none()
        );
        recovery
            .apply_page(&SlottedPage::new(PageId::new(0), PageType::Metadata))
            .expect("repair");
        recovery.finish().expect("finish repaired file");
    }

    #[test]
    fn recovery_plan_rejects_unplanned_or_sparse_writes() {
        let directory = tempdir().expect("tempdir");
        let mut recovery = RecoveryDataFile::open(
            directory.path(),
            RecoveryPlan::new(1, [PageId::new(0), PageId::new(2)]),
        )
        .expect("open");
        assert_eq!(
            recovery
                .apply_page(&SlottedPage::new(PageId::new(1), PageType::Heap))
                .expect_err("unplanned")
                .sql_state,
            "XX001"
        );
        assert_eq!(
            recovery
                .apply_page(&SlottedPage::new(PageId::new(2), PageType::Heap))
                .expect_err("sparse")
                .sql_state,
            "XX001"
        );
    }
}
