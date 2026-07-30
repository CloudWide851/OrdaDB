use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use ordadb_types::{DbError, Result};

use crate::TransactionId;

#[derive(Debug)]
pub struct WriterCoordinator {
    next_transaction_id: AtomicU64,
    owner: Mutex<Option<TransactionId>>,
}

impl WriterCoordinator {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_transaction_id: AtomicU64::new(1),
            owner: Mutex::new(None),
        })
    }

    pub fn from_last_transaction_id(
        last_transaction_id: Option<TransactionId>,
    ) -> Result<Arc<Self>> {
        let next_transaction_id = match last_transaction_id {
            Some(last) => last
                .get()
                .checked_add(1)
                .ok_or_else(|| DbError::new("54000", "transaction ID space is exhausted"))?,
            None => 1,
        };
        Self::from_next_transaction_id(next_transaction_id)
    }

    pub fn from_next_transaction_id(next_transaction_id: u64) -> Result<Arc<Self>> {
        if TransactionId::new(next_transaction_id).is_none() {
            return Err(DbError::new(
                "22023",
                "next transaction ID must be non-zero",
            ));
        }
        Ok(Arc::new(Self {
            next_transaction_id: AtomicU64::new(next_transaction_id),
            owner: Mutex::new(None),
        }))
    }

    pub fn next_transaction_id(&self) -> Result<TransactionId> {
        let value = self
            .next_transaction_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| DbError::new("54000", "transaction ID space is exhausted"))?;
        TransactionId::new(value)
            .ok_or_else(|| DbError::internal("writer coordinator generated transaction ID zero"))
    }

    pub fn try_acquire(self: &Arc<Self>, transaction_id: TransactionId) -> Result<WriterLease> {
        let mut owner = self.lock_owner()?;
        if let Some(active) = *owner {
            return Err(DbError::new(
                "55P03",
                format!("transaction {active} already owns the database writer"),
            )
            .with_hint("retry after the active writer commits or rolls back"));
        }
        *owner = Some(transaction_id);
        drop(owner);
        Ok(WriterLease {
            coordinator: Some(Arc::clone(self)),
            transaction_id,
        })
    }

    pub fn active_transaction(&self) -> Result<Option<TransactionId>> {
        Ok(*self.lock_owner()?)
    }

    fn lock_owner(&self) -> Result<MutexGuard<'_, Option<TransactionId>>> {
        self.owner.lock().map_err(|_| {
            DbError::internal("writer coordinator lock is poisoned")
                .with_hint("restart the process before retrying transaction work")
        })
    }

    fn release(&self, transaction_id: TransactionId) {
        if let Ok(mut owner) = self.owner.lock()
            && *owner == Some(transaction_id)
        {
            *owner = None;
        }
    }
}

#[derive(Debug)]
pub struct WriterLease {
    coordinator: Option<Arc<WriterCoordinator>>,
    transaction_id: TransactionId,
}

impl WriterLease {
    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[must_use]
    pub const fn is_released(&self) -> bool {
        self.coordinator.is_none()
    }

    pub fn release(&mut self) {
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.release(self.transaction_id);
        }
    }
}

impl Drop for WriterLease {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::WriterCoordinator;
    use crate::TransactionId;

    #[test]
    fn lease_is_exclusive_and_drop_releases_it() {
        let coordinator = WriterCoordinator::new();
        let first_id = coordinator.next_transaction_id().expect("first ID");
        let second_id = coordinator.next_transaction_id().expect("second ID");
        let lease = coordinator.try_acquire(first_id).expect("first lease");
        let busy = coordinator
            .try_acquire(second_id)
            .expect_err("second writer must be busy");
        assert_eq!(busy.sql_state, "55P03");
        drop(lease);
        let second = coordinator
            .try_acquire(second_id)
            .expect("drop releases writer");
        assert_eq!(second.transaction_id(), second_id);
    }

    #[test]
    fn explicit_release_is_idempotent() {
        let coordinator = WriterCoordinator::new();
        let transaction_id = coordinator.next_transaction_id().expect("ID");
        let mut lease = coordinator
            .try_acquire(transaction_id)
            .expect("writer lease");
        lease.release();
        lease.release();
        assert!(lease.is_released());
        assert_eq!(coordinator.active_transaction().expect("owner state"), None);
    }

    #[test]
    fn coordinator_resumes_after_the_last_durable_transaction_id() {
        let last = TransactionId::new(41).expect("non-zero transaction ID");
        let coordinator =
            WriterCoordinator::from_last_transaction_id(Some(last)).expect("seed coordinator");
        assert_eq!(
            coordinator
                .next_transaction_id()
                .expect("resumed transaction ID")
                .get(),
            42
        );
    }

    #[test]
    fn coordinator_can_start_at_a_migration_transaction_floor() {
        let coordinator =
            WriterCoordinator::from_next_transaction_id(42).expect("seed coordinator");
        assert_eq!(
            coordinator
                .next_transaction_id()
                .expect("migration transaction ID")
                .get(),
            42
        );
        assert_eq!(
            WriterCoordinator::from_next_transaction_id(0)
                .expect_err("zero floor refused")
                .sql_state,
            "22023"
        );
    }
}
