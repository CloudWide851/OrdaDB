use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ordadb_types::{DbError, Result};

#[derive(Debug)]
struct MemoryState {
    soft_limit: usize,
    hard_limit: usize,
    current: AtomicUsize,
    peak: AtomicUsize,
}

/// A cloneable query-level memory budget shared by every operator.
///
/// Allocations become visible to the grant only through [`Reservation`]
/// ownership. Dropping a reservation always returns its bytes, including
/// error, cancellation, and cursor-destruction paths.
#[derive(Debug, Clone)]
pub struct MemoryGrant {
    state: Arc<MemoryState>,
}

impl MemoryGrant {
    pub fn new(soft_limit: usize, hard_limit: usize) -> Result<Self> {
        if soft_limit == 0 || hard_limit == 0 || soft_limit > hard_limit {
            return Err(
                DbError::new("22023", "query memory grant limits are invalid").with_hint(
                    "Use positive limits with the soft limit no larger than the hard limit.",
                ),
            );
        }
        Ok(Self {
            state: Arc::new(MemoryState {
                soft_limit,
                hard_limit,
                current: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            }),
        })
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<Reservation> {
        self.acquire(bytes)?;
        Ok(Reservation {
            grant: self.clone(),
            bytes,
        })
    }

    #[must_use]
    pub fn current_bytes(&self) -> usize {
        self.state.current.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn peak_bytes(&self) -> usize {
        self.state.peak.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn soft_limit_bytes(&self) -> usize {
        self.state.soft_limit
    }

    #[must_use]
    pub fn hard_limit_bytes(&self) -> usize {
        self.state.hard_limit
    }

    #[must_use]
    pub fn would_cross_soft_limit(&self, additional: usize) -> bool {
        self.current_bytes().saturating_add(additional) > self.state.soft_limit
    }

    fn acquire(&self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut current = self.state.current.load(Ordering::Relaxed);
        loop {
            let next = current.checked_add(bytes).ok_or_else(|| {
                DbError::new("53200", "query memory limit exceeded")
                    .with_detail("memory accounting overflow")
            })?;
            if next > self.state.hard_limit {
                return Err(DbError::new("53200", "query memory limit exceeded")
                    .with_detail(format!(
                        "requested {bytes} bytes with {current} of {} bytes already in use",
                        self.state.hard_limit
                    ))
                    .with_hint(
                        "Reduce result width, add a LIMIT, or raise the query memory grant.",
                    ));
            }
            match self.state.current.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.state.peak.fetch_max(next, Ordering::Relaxed);
                    return Ok(());
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn release_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut current = self.state.current.load(Ordering::Relaxed);
        loop {
            debug_assert!(
                bytes <= current,
                "reservation release exceeded current query memory"
            );
            let next = current.saturating_sub(bytes);
            match self.state.current.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

/// Owned bytes within a [`MemoryGrant`].
pub struct Reservation {
    grant: MemoryGrant,
    bytes: usize,
}

impl Reservation {
    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn grow(&mut self, additional: usize) -> Result<()> {
        self.grant.acquire(additional)?;
        self.bytes = self.bytes.checked_add(additional).ok_or_else(|| {
            DbError::new("53200", "query memory limit exceeded")
                .with_detail("reservation size overflow")
        })?;
        Ok(())
    }

    pub fn resize(&mut self, bytes: usize) -> Result<()> {
        match bytes.cmp(&self.bytes) {
            std::cmp::Ordering::Greater => self.grow(bytes - self.bytes),
            std::cmp::Ordering::Less => {
                self.grant.release_bytes(self.bytes - bytes);
                self.bytes = bytes;
                Ok(())
            }
            std::cmp::Ordering::Equal => Ok(()),
        }
    }

    pub(crate) fn transfer_to(&mut self, target: &mut Self, bytes: usize) -> Result<()> {
        if !Arc::ptr_eq(&self.grant.state, &target.grant.state) {
            return Err(DbError::internal(
                "memory reservation transfer crossed query grants",
            ));
        }
        if bytes > self.bytes {
            return Err(DbError::internal(
                "memory reservation transfer exceeded its source",
            ));
        }
        let target_bytes = target
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| DbError::new("53200", "query memory limit exceeded"))?;
        self.bytes -= bytes;
        target.bytes = target_bytes;
        Ok(())
    }
}

impl fmt::Debug for Reservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Reservation")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.grant.release_bytes(self.bytes);
        self.bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_release_on_drop_and_retain_peak() {
        let grant = MemoryGrant::new(8, 16).expect("grant");
        {
            let mut reservation = grant.try_reserve(6).expect("reserve");
            reservation.grow(4).expect("grow");
            assert_eq!(reservation.bytes(), 10);
            assert_eq!(grant.current_bytes(), 10);
            assert_eq!(grant.peak_bytes(), 10);
        }
        assert_eq!(grant.current_bytes(), 0);
        assert_eq!(grant.peak_bytes(), 10);
    }

    #[test]
    fn hard_limit_failure_never_publishes_bytes() {
        let grant = MemoryGrant::new(4, 8).expect("grant");
        let reservation = grant.try_reserve(6).expect("reserve");
        let error = grant.try_reserve(3).expect_err("hard limit");
        assert_eq!(error.sql_state, "53200");
        assert_eq!(grant.current_bytes(), 6);
        drop(reservation);
        assert_eq!(grant.current_bytes(), 0);
    }

    #[test]
    fn shared_grants_account_all_operator_reservations() {
        let grant = MemoryGrant::new(4, 16).expect("grant");
        let cloned = grant.clone();
        let first = grant.try_reserve(3).expect("first");
        let second = cloned.try_reserve(5).expect("second");
        assert!(grant.would_cross_soft_limit(1));
        assert_eq!(grant.current_bytes(), 8);
        drop((first, second));
        assert_eq!(cloned.current_bytes(), 0);
    }
}
