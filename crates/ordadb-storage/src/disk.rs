use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use ordadb_types::Result;

use crate::{PAGE_SIZE, PageId, SlottedPage, corruption, io_error};

#[derive(Debug)]
pub struct DiskManager {
    path: PathBuf,
    file: File,
}

impl DiskManager {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| io_error("failed to open database file", error))?;
        let manager = Self { path, file };
        manager.page_count()?;
        Ok(manager)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn page_count(&self) -> Result<u64> {
        let length = self
            .file
            .metadata()
            .map_err(|error| io_error("failed to inspect database file", error))?
            .len();
        if length % PAGE_SIZE as u64 != 0 {
            return Err(corruption(format!(
                "database file has a trailing partial page: {length} bytes"
            )));
        }
        Ok(length / PAGE_SIZE as u64)
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<SlottedPage> {
        let page_count = self.page_count()?;
        if page_id.get() >= page_count {
            return Err(corruption(format!(
                "page {} is outside database file with {page_count} pages",
                page_id.get()
            )));
        }
        let offset = page_offset(page_id)?;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|error| io_error("failed to seek to database page", error))?;
        let mut bytes = Box::new([0_u8; PAGE_SIZE]);
        self.file
            .read_exact(&mut bytes[..])
            .map_err(|error| io_error("failed to read complete database page", error))?;
        SlottedPage::from_bytes(&bytes[..], page_id)
    }

    pub fn write_page(&mut self, page: &SlottedPage) -> Result<()> {
        page.validate()?;
        let page_count = self.page_count()?;
        if page.page_id().get() > page_count {
            return Err(corruption(format!(
                "cannot write page {} after database file page {page_count}",
                page.page_id().get()
            )));
        }
        let offset = page_offset(page.page_id())?;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|error| io_error("failed to seek to database page", error))?;
        self.file
            .write_all(&page.sealed_bytes()[..])
            .map_err(|error| io_error("failed to write complete database page", error))
    }

    pub fn truncate_pages(&mut self, page_count: u64) -> Result<()> {
        let length = page_count
            .checked_mul(PAGE_SIZE as u64)
            .ok_or_else(|| corruption("database file length overflow"))?;
        self.file
            .set_len(length)
            .map_err(|error| io_error("failed to resize database file", error))
    }

    pub fn sync_all(&self) -> Result<()> {
        self.file
            .sync_all()
            .map_err(|error| io_error("failed to synchronize database file", error))
    }
}

fn page_offset(page_id: PageId) -> Result<u64> {
    page_id
        .get()
        .checked_mul(PAGE_SIZE as u64)
        .ok_or_else(|| corruption(format!("page {} offset overflow", page_id.get())))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::tempdir;

    use super::*;
    use crate::PageType;

    #[test]
    fn writes_reads_truncates_and_syncs_exact_pages() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("ordadb.data");
        let mut disk = DiskManager::open(&path).expect("open");
        assert_eq!(disk.page_count().expect("count"), 0);

        let mut page = SlottedPage::new(PageId::new(0), PageType::Metadata);
        page.insert(b"manifest").expect("insert");
        disk.write_page(&page).expect("write");
        disk.sync_all().expect("sync");
        assert_eq!(disk.page_count().expect("count"), 1);
        assert_eq!(
            disk.read_page(PageId::new(0))
                .expect("read")
                .record(0)
                .expect("record"),
            b"manifest"
        );

        disk.truncate_pages(0).expect("truncate");
        assert_eq!(disk.page_count().expect("count"), 0);
    }

    #[test]
    fn rejects_sparse_page_writes() {
        let directory = tempdir().expect("tempdir");
        let mut disk = DiskManager::open(directory.path().join("ordadb.data")).expect("open disk");
        assert_eq!(
            disk.write_page(&SlottedPage::new(PageId::new(1), PageType::Heap))
                .expect_err("sparse page")
                .sql_state,
            "XX001"
        );
        assert_eq!(disk.page_count().expect("count"), 0);
    }

    #[test]
    fn rejects_trailing_file_fragments() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("ordadb.data");
        std::fs::File::create(&path)
            .expect("create")
            .write_all(b"partial")
            .expect("write");
        assert_eq!(
            DiskManager::open(&path).expect_err("partial").sql_state,
            "XX001"
        );
    }
}
