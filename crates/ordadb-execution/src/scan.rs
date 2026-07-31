use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use ordadb_types::{DbError, Result, Row, TableId};

use crate::{ChunkPool, DataChunk, MemoryGrant, Reservation, estimated_row_bytes};

/// A memory-accounted columnar chunk returned by a table scan.
#[derive(Debug)]
pub struct LeasedDataChunk {
    chunk: Option<DataChunk>,
    reservation: Option<Reservation>,
    pool: Option<Arc<Mutex<ChunkPool>>>,
}

impl LeasedDataChunk {
    pub fn from_rows(rows: &[Row], grant: &MemoryGrant) -> Result<Self> {
        let estimated = rows
            .iter()
            .map(estimated_row_bytes)
            .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
            .ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("table scan chunk estimate overflow")
            })?;
        let mut reservation = grant.try_reserve(estimated)?;
        let chunk = DataChunk::from_rows(rows)?;
        reservation.resize(chunk.estimated_bytes())?;
        Ok(Self {
            chunk: Some(chunk),
            reservation: Some(reservation),
            pool: None,
        })
    }

    pub fn from_snapshot(
        rows: Arc<Vec<Row>>,
        start: usize,
        end: usize,
        grant: &MemoryGrant,
    ) -> Result<Self> {
        let row_count = end
            .checked_sub(start)
            .ok_or_else(|| DbError::internal("row-backed table scan range is reversed"))?;
        let selection_bytes = row_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("table scan selection estimate overflow")
            })?;
        let mut reservation = grant.try_reserve(selection_bytes)?;
        let chunk = DataChunk::from_row_snapshot(rows, start, end)?;
        reservation.resize(chunk.estimated_bytes())?;
        Ok(Self {
            chunk: Some(chunk),
            reservation: Some(reservation),
            pool: None,
        })
    }

    fn from_pool(rows: &[Row], grant: &MemoryGrant, pool: Arc<Mutex<ChunkPool>>) -> Result<Self> {
        let (chunk, reservation) = pool
            .lock()
            .map_err(|_| DbError::internal("columnar chunk pool lock is poisoned"))?
            .materialize(rows, grant)?;
        Ok(Self {
            chunk: Some(chunk),
            reservation: Some(reservation),
            pool: Some(pool),
        })
    }

    #[must_use]
    pub fn chunk(&self) -> &DataChunk {
        self.chunk
            .as_ref()
            .expect("leased data chunk is unavailable")
    }

    pub fn chunk_mut(&mut self) -> &mut DataChunk {
        self.chunk
            .as_mut()
            .expect("leased data chunk is unavailable")
    }

    pub fn replace(&mut self, chunk: DataChunk) -> Result<()> {
        self.reservation
            .as_mut()
            .ok_or_else(|| DbError::internal("data chunk reservation is unavailable"))?
            .resize(chunk.estimated_bytes())?;
        self.chunk = Some(chunk);
        Ok(())
    }

    pub fn refresh_reservation(&mut self) -> Result<()> {
        let bytes = self.chunk().estimated_bytes();
        self.reservation
            .as_mut()
            .ok_or_else(|| DbError::internal("data chunk reservation is unavailable"))?
            .resize(bytes)
    }

    pub fn take_rows(&mut self) -> Result<Vec<Row>> {
        self.chunk_mut().take_rows()
    }

    pub fn into_parts(mut self) -> (DataChunk, Reservation) {
        self.pool = None;
        (
            self.chunk.take().expect("leased data chunk is unavailable"),
            self.reservation
                .take()
                .expect("data chunk reservation is unavailable"),
        )
    }

    pub fn recycle(mut self) -> Result<()> {
        self.recycle_chunk()
    }

    fn recycle_chunk(&mut self) -> Result<()> {
        let (Some(pool), Some(chunk), Some(reservation)) = (
            self.pool.as_ref().map(Arc::clone),
            self.chunk.take(),
            self.reservation.take(),
        ) else {
            return Ok(());
        };
        pool.lock()
            .map_err(|_| DbError::internal("columnar chunk pool lock is poisoned"))?
            .recycle(chunk, reservation);
        Ok(())
    }
}

impl Drop for LeasedDataChunk {
    fn drop(&mut self) {
        let _ = self.recycle_chunk();
    }
}

/// Pulls one bounded columnar chunk at a time from a table snapshot.
pub trait TableScan: Send {
    fn next_chunk(
        &mut self,
        max_rows: usize,
        grant: &MemoryGrant,
    ) -> Result<Option<LeasedDataChunk>>;
}

/// Resolves stable table snapshots without exposing their storage container.
pub trait TableProvider {
    fn scan(&self, table_id: TableId) -> Result<Box<dyn TableScan>>;
}

pub struct SnapshotTableProvider<'a> {
    tables: &'a BTreeMap<TableId, Arc<Vec<Row>>>,
}

impl<'a> SnapshotTableProvider<'a> {
    #[must_use]
    pub const fn new(tables: &'a BTreeMap<TableId, Arc<Vec<Row>>>) -> Self {
        Self { tables }
    }
}

impl TableProvider for SnapshotTableProvider<'_> {
    fn scan(&self, table_id: TableId) -> Result<Box<dyn TableScan>> {
        let rows = self
            .tables
            .get(&table_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()));
        Ok(Box::new(SnapshotTableScan {
            rows,
            offset: 0,
            pool: None,
        }))
    }
}

struct SnapshotTableScan {
    rows: Arc<Vec<Row>>,
    offset: usize,
    pool: Option<Arc<Mutex<ChunkPool>>>,
}

impl TableScan for SnapshotTableScan {
    fn next_chunk(
        &mut self,
        max_rows: usize,
        grant: &MemoryGrant,
    ) -> Result<Option<LeasedDataChunk>> {
        if max_rows == 0 {
            return Err(DbError::new(
                "22023",
                "table scan chunk size must be positive",
            ));
        }
        if self.offset >= self.rows.len() {
            return Ok(None);
        }
        let end = self.offset.saturating_add(max_rows).min(self.rows.len());
        let pool = self
            .pool
            .get_or_insert_with(|| Arc::new(Mutex::new(ChunkPool::new(max_rows, 2))))
            .clone();
        let chunk = LeasedDataChunk::from_pool(&self.rows[self.offset..end], grant, pool)?;
        self.offset = end;
        Ok(Some(chunk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordadb_types::Value;

    #[test]
    fn snapshot_scan_is_bounded_and_releases_each_chunk_grant() {
        let table_id = TableId::new(7);
        let rows = Arc::new(
            (0..5)
                .map(|value| Row::new(vec![Value::Int64(value)]))
                .collect::<Vec<_>>(),
        );
        let tables = BTreeMap::from([(table_id, rows)]);
        let provider = SnapshotTableProvider::new(&tables);
        let mut scan = provider.scan(table_id).expect("scan");
        let grant = MemoryGrant::new(1024, 4096).expect("grant");

        let first = scan.next_chunk(2, &grant).expect("first").expect("chunk");
        assert_eq!(first.chunk().len(), 2);
        assert!(grant.current_bytes() > 0);
        drop(first);
        assert!(
            grant.current_bytes() > 0,
            "the bounded pool retains its accounted reusable chunk"
        );

        assert_eq!(
            scan.next_chunk(2, &grant)
                .expect("second")
                .expect("chunk")
                .chunk()
                .len(),
            2
        );
        assert_eq!(
            scan.next_chunk(2, &grant)
                .expect("third")
                .expect("chunk")
                .chunk()
                .len(),
            1
        );
        assert!(scan.next_chunk(2, &grant).expect("done").is_none());
        drop(scan);
        assert_eq!(grant.current_bytes(), 0);
    }

    #[test]
    fn poisoned_pool_releases_chunk_reservations_on_explicit_recycle_and_drop() {
        fn poison(pool: &Arc<Mutex<ChunkPool>>) {
            let pool = Arc::clone(pool);
            let poisoned = std::panic::catch_unwind(move || {
                let _guard = pool.lock().expect("pool lock before poison");
                panic!("poison chunk pool");
            });
            assert!(poisoned.is_err());
        }

        let rows = Arc::new(vec![Row::new(vec![Value::Int64(1)])]);

        let mut scan = SnapshotTableScan {
            rows: Arc::clone(&rows),
            offset: 0,
            pool: None,
        };
        let grant = MemoryGrant::new(1024, 4096).expect("grant");
        let leased = scan.next_chunk(1, &grant).expect("chunk").expect("leased");
        let pool = scan
            .pool
            .clone()
            .expect("snapshot scan pool is initialized");
        poison(&pool);
        let error = leased.recycle().expect_err("poisoned recycle");
        assert_eq!(error.sql_state, "XX000");
        assert_eq!(grant.current_bytes(), 0);

        let mut scan = SnapshotTableScan {
            rows,
            offset: 0,
            pool: None,
        };
        let grant = MemoryGrant::new(1024, 4096).expect("grant");
        let leased = scan.next_chunk(1, &grant).expect("chunk").expect("leased");
        let pool = scan
            .pool
            .clone()
            .expect("snapshot scan pool is initialized");
        poison(&pool);
        drop(leased);
        assert_eq!(grant.current_bytes(), 0);
    }
}
