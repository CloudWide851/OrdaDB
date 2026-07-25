use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use ordadb_types::{DbError, Result};

use crate::{DiskManager, PageId, SlottedPage};

pub trait DurabilityBarrier: Send + Sync + 'static {
    fn flush_through(&self, page_lsn: u64) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoWalBarrier;

impl DurabilityBarrier for NoWalBarrier {
    fn flush_through(&self, page_lsn: u64) -> Result<()> {
        if page_lsn != 0 {
            return Err(DbError::new(
                "55000",
                format!("cannot flush page LSN {page_lsn} before WAL is available"),
            )
            .with_hint("keep page LSN at zero until the WAL milestone installs a barrier"));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<Mutex<BufferPoolInner>>,
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.lock() {
            Ok(inner) => formatter
                .debug_struct("BufferPool")
                .field("capacity", &inner.capacity)
                .field("frames", &inner.frames.len())
                .finish(),
            Err(_) => formatter
                .debug_struct("BufferPool")
                .field("state", &"poisoned")
                .finish(),
        }
    }
}

struct BufferPoolInner {
    disk: DiskManager,
    capacity: usize,
    k: usize,
    clock: u64,
    frames: BTreeMap<PageId, Frame>,
    barrier: Arc<dyn DurabilityBarrier>,
}

#[derive(Debug)]
struct Frame {
    page: SlottedPage,
    pin_count: usize,
    dirty: bool,
    history: VecDeque<u64>,
}

pub struct PageGuard {
    pool: Arc<Mutex<BufferPoolInner>>,
    page_id: PageId,
}

impl std::fmt::Debug for PageGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PageGuard")
            .field("page_id", &self.page_id)
            .finish_non_exhaustive()
    }
}

impl BufferPool {
    pub fn new(
        disk: DiskManager,
        capacity: usize,
        barrier: Arc<dyn DurabilityBarrier>,
    ) -> Result<Self> {
        if capacity == 0 {
            return Err(DbError::new(
                "22023",
                "buffer pool capacity must be greater than zero",
            ));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(BufferPoolInner {
                disk,
                capacity,
                k: 2,
                clock: 0,
                frames: BTreeMap::new(),
                barrier,
            })),
        })
    }

    pub fn fetch(&self, page_id: PageId) -> Result<PageGuard> {
        let mut inner = self.lock()?;
        inner.clock = inner.clock.saturating_add(1);
        let access = inner.clock;
        let k = inner.k;
        if let Some(frame) = inner.frames.get_mut(&page_id) {
            frame.pin_count = frame.pin_count.saturating_add(1);
            record_access(frame, access, k);
        } else {
            ensure_capacity(&mut inner)?;
            let page = inner.disk.read_page(page_id)?;
            let mut history = VecDeque::new();
            history.push_back(access);
            inner.frames.insert(
                page_id,
                Frame {
                    page,
                    pin_count: 1,
                    dirty: false,
                    history,
                },
            );
        }
        drop(inner);
        Ok(PageGuard {
            pool: Arc::clone(&self.inner),
            page_id,
        })
    }

    pub fn install(&self, page: SlottedPage, dirty: bool) -> Result<()> {
        page.validate()?;
        let page_id = page.page_id();
        let mut inner = self.lock()?;
        if inner.frames.contains_key(&page_id) {
            return Err(DbError::new(
                "55000",
                format!(
                    "page {} is already resident; replace it through its page guard",
                    page_id.get()
                ),
            )
            .with_hint("fetch the page and use PageGuard::replace"));
        }
        ensure_capacity(&mut inner)?;
        inner.clock = inner.clock.saturating_add(1);
        let access = inner.clock;
        let mut history = VecDeque::new();
        history.push_back(access);
        inner.frames.insert(
            page_id,
            Frame {
                page,
                pin_count: 0,
                dirty,
                history,
            },
        );
        Ok(())
    }

    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let mut inner = self.lock()?;
        flush_page_locked(&mut inner, page_id)
    }

    pub fn flush_all(&self) -> Result<()> {
        let mut inner = self.lock()?;
        let page_ids: Vec<_> = inner.frames.keys().copied().collect();
        for page_id in page_ids {
            flush_page_locked(&mut inner, page_id)?;
        }
        Ok(())
    }

    pub fn reset_storage(&self) -> Result<()> {
        let mut inner = self.lock()?;
        if inner.frames.values().any(|frame| frame.pin_count > 0) {
            return Err(DbError::new(
                "55000",
                "cannot reset storage while pages are pinned",
            ));
        }
        inner.frames.clear();
        inner.clock = 0;
        inner.disk.truncate_pages(0)
    }

    pub(crate) fn invalidate_pages(&self, page_ids: &[PageId]) -> Result<()> {
        let mut inner = self.lock()?;
        if let Some(page_id) = page_ids.iter().find(|page_id| {
            inner
                .frames
                .get(page_id)
                .is_some_and(|frame| frame.pin_count > 0)
        }) {
            return Err(DbError::new(
                "55000",
                format!(
                    "cannot invalidate pinned page {} during prepared commit",
                    page_id.get()
                ),
            )
            .with_hint("release page guards before applying a prepared commit"));
        }
        for page_id in page_ids {
            inner.frames.remove(page_id);
        }
        Ok(())
    }

    pub(crate) fn resize_pages(&self, page_count: u64) -> Result<()> {
        let mut inner = self.lock()?;
        if let Some(page_id) = inner
            .frames
            .iter()
            .find(|(page_id, frame)| page_id.get() >= page_count && frame.pin_count > 0)
            .map(|(page_id, _)| *page_id)
        {
            return Err(DbError::new(
                "55000",
                format!(
                    "cannot remove pinned page {} while resizing database storage",
                    page_id.get()
                ),
            )
            .with_hint("release page guards before resizing database storage"));
        }
        inner.frames.retain(|page_id, _| page_id.get() < page_count);
        inner.disk.truncate_pages(page_count)
    }

    pub fn sync_all(&self) -> Result<()> {
        self.lock()?.disk.sync_all()
    }

    pub fn page_count(&self) -> Result<u64> {
        self.lock()?.disk.page_count()
    }

    #[cfg(test)]
    fn is_cached(&self, page_id: PageId) -> bool {
        self.inner
            .lock()
            .is_ok_and(|inner| inner.frames.contains_key(&page_id))
    }

    fn lock(&self) -> Result<MutexGuard<'_, BufferPoolInner>> {
        self.inner.lock().map_err(|_| {
            DbError::internal("buffer pool lock is poisoned")
                .with_hint("restart the process before retrying storage access")
        })
    }
}

impl PageGuard {
    #[must_use]
    pub const fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn snapshot(&self) -> Result<SlottedPage> {
        let inner = self.lock()?;
        inner
            .frames
            .get(&self.page_id)
            .map(|frame| frame.page.clone())
            .ok_or_else(|| DbError::internal("pinned page disappeared from the buffer pool"))
    }

    pub fn replace(&self, page: SlottedPage) -> Result<()> {
        if page.page_id() != self.page_id {
            return Err(DbError::new(
                "22023",
                "replacement page identity does not match the pinned page",
            ));
        }
        page.validate()?;
        let mut inner = self.lock()?;
        let frame = inner
            .frames
            .get_mut(&self.page_id)
            .ok_or_else(|| DbError::internal("pinned page disappeared from the buffer pool"))?;
        frame.page = page;
        frame.dirty = true;
        Ok(())
    }

    pub fn insert_record(&self, record: &[u8]) -> Result<Option<u16>> {
        let mut inner = self.lock()?;
        let frame = inner
            .frames
            .get_mut(&self.page_id)
            .ok_or_else(|| DbError::internal("pinned page disappeared from the buffer pool"))?;
        let slot = frame.page.insert(record)?;
        if slot.is_some() {
            frame.dirty = true;
        }
        Ok(slot)
    }

    pub fn mark_dirty(&self) -> Result<()> {
        let mut inner = self.lock()?;
        let frame = inner
            .frames
            .get_mut(&self.page_id)
            .ok_or_else(|| DbError::internal("pinned page disappeared from the buffer pool"))?;
        frame.dirty = true;
        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, BufferPoolInner>> {
        self.pool.lock().map_err(|_| {
            DbError::internal("buffer pool lock is poisoned")
                .with_hint("restart the process before retrying storage access")
        })
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.pool.lock()
            && let Some(frame) = inner.frames.get_mut(&self.page_id)
        {
            frame.pin_count = frame.pin_count.saturating_sub(1);
        }
    }
}

fn ensure_capacity(inner: &mut BufferPoolInner) -> Result<()> {
    if inner.frames.len() < inner.capacity {
        return Ok(());
    }
    let victim = choose_victim(inner).ok_or_else(|| {
        DbError::new(
            "53000",
            "buffer pool has no evictable frame because every frame is pinned",
        )
        .with_hint("release page guards or increase the buffer pool capacity")
    })?;
    let frame = inner
        .frames
        .remove(&victim)
        .ok_or_else(|| DbError::internal("selected buffer victim disappeared"))?;
    if let Err(error) = flush_frame(inner, &frame) {
        inner.frames.insert(victim, frame);
        return Err(error);
    }
    Ok(())
}

fn choose_victim(inner: &BufferPoolInner) -> Option<PageId> {
    inner
        .frames
        .iter()
        .filter(|(_, frame)| frame.pin_count == 0)
        .min_by_key(|(page_id, frame)| {
            if frame.history.len() < inner.k {
                (0_u8, frame.history.front().copied().unwrap_or(0), **page_id)
            } else {
                let index = frame.history.len() - inner.k;
                (
                    1_u8,
                    frame.history.get(index).copied().unwrap_or(0),
                    **page_id,
                )
            }
        })
        .map(|(page_id, _)| *page_id)
}

fn flush_page_locked(inner: &mut BufferPoolInner, page_id: PageId) -> Result<()> {
    let Some(frame) = inner.frames.get(&page_id) else {
        return Err(DbError::new(
            "XX001",
            format!("page {} is not resident in the buffer pool", page_id.get()),
        ));
    };
    if !frame.dirty {
        return Ok(());
    }
    let page = frame.page.clone();
    inner.barrier.flush_through(page.lsn())?;
    inner.disk.write_page(&page)?;
    if let Some(frame) = inner.frames.get_mut(&page_id) {
        frame.dirty = false;
    }
    Ok(())
}

fn flush_frame(inner: &mut BufferPoolInner, frame: &Frame) -> Result<()> {
    if frame.dirty {
        inner.barrier.flush_through(frame.page.lsn())?;
        inner.disk.write_page(&frame.page)?;
    }
    Ok(())
}

fn record_access(frame: &mut Frame, access: u64, k: usize) {
    frame.history.push_back(access);
    while frame.history.len() > k {
        frame.history.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::PageType;

    fn pool_with_pages(capacity: usize, page_count: u64) -> (tempfile::TempDir, BufferPool) {
        let directory = tempdir().expect("tempdir");
        let mut disk = DiskManager::open(directory.path().join("buffer.data")).expect("disk");
        for page_id in 0..page_count {
            disk.write_page(&SlottedPage::new(PageId::new(page_id), PageType::Heap))
                .expect("write page");
        }
        disk.sync_all().expect("sync");
        let pool = BufferPool::new(disk, capacity, Arc::new(NoWalBarrier)).expect("buffer pool");
        (directory, pool)
    }

    #[test]
    fn pinned_frames_are_never_evicted() {
        let (_directory, pool) = pool_with_pages(1, 2);
        let guard = pool.fetch(PageId::new(0)).expect("pin");
        assert_eq!(
            pool.fetch(PageId::new(1))
                .expect_err("all frames pinned")
                .sql_state,
            "53000"
        );
        drop(guard);
        assert!(pool.fetch(PageId::new(1)).is_ok());
    }

    #[test]
    fn dirty_eviction_flushes_and_reload_validates_checksum() {
        let (directory, pool) = pool_with_pages(1, 2);
        {
            let guard = pool.fetch(PageId::new(0)).expect("fetch");
            guard.insert_record(b"persisted").expect("insert");
        }
        drop(pool.fetch(PageId::new(1)).expect("evict"));

        let mut disk = DiskManager::open(directory.path().join("buffer.data")).expect("reopen");
        assert_eq!(
            disk.read_page(PageId::new(0))
                .expect("read")
                .record(0)
                .expect("record"),
            b"persisted"
        );
    }

    #[test]
    fn lru_k_prefers_frames_with_less_than_k_history() {
        let (_directory, pool) = pool_with_pages(2, 3);
        drop(pool.fetch(PageId::new(0)).expect("page zero once"));
        drop(pool.fetch(PageId::new(1)).expect("page one once"));
        drop(pool.fetch(PageId::new(1)).expect("page one twice"));
        drop(pool.fetch(PageId::new(2)).expect("evict colder page"));

        assert!(!pool.is_cached(PageId::new(0)));
        assert!(pool.is_cached(PageId::new(1)));
        assert!(pool.is_cached(PageId::new(2)));
    }

    #[test]
    fn non_zero_lsn_flush_requires_a_wal_barrier() {
        let (_directory, pool) = pool_with_pages(1, 0);
        let mut page = SlottedPage::new(PageId::new(0), PageType::Heap);
        page.set_lsn(42);
        pool.install(page, true).expect("install");
        assert_eq!(
            pool.flush_all().expect_err("WAL barrier").sql_state,
            "55000"
        );
    }

    #[test]
    fn install_cannot_silently_replace_a_resident_frame() {
        let (_directory, pool) = pool_with_pages(2, 0);
        pool.install(SlottedPage::new(PageId::new(0), PageType::Heap), true)
            .expect("initial install");
        assert_eq!(
            pool.install(SlottedPage::new(PageId::new(0), PageType::Metadata), false)
                .expect_err("resident replacement")
                .sql_state,
            "55000"
        );
    }
}
