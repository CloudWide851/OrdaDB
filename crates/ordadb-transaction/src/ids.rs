use std::fmt;
use std::num::NonZeroU64;

use ordadb_types::{DbError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(NonZeroU64);

impl Lsn {
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub(crate) fn checked_next(self) -> Result<Self> {
        self.get()
            .checked_add(1)
            .and_then(Self::new)
            .ok_or_else(|| DbError::new("54000", "WAL LSN space is exhausted"))
    }
}

impl TryFrom<u64> for Lsn {
    type Error = DbError;

    fn try_from(value: u64) -> Result<Self> {
        Self::new(value).ok_or_else(|| DbError::new("XX001", "WAL LSN must be non-zero"))
    }
}

impl From<Lsn> for u64 {
    fn from(value: Lsn) -> Self {
        value.get()
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionId(NonZeroU64);

impl TransactionId {
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for TransactionId {
    type Error = DbError;

    fn try_from(value: u64) -> Result<Self> {
        Self::new(value).ok_or_else(|| DbError::new("XX001", "transaction ID must be non-zero"))
    }
}

impl From<TransactionId> for u64 {
    fn from(value: TransactionId) -> Self {
        value.get()
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::{Lsn, TransactionId};

    #[test]
    fn identifiers_are_non_zero_and_ordered() {
        assert!(Lsn::new(0).is_none());
        assert!(TransactionId::new(0).is_none());
        assert!(Lsn::new(1) < Lsn::new(2));
        assert!(TransactionId::new(1) < TransactionId::new(2));
    }
}
