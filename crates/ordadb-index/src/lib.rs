//! Ordered secondary-index primitives for OrdaDB.
//!
//! The tree deliberately owns no page or transaction types. Storage persists
//! sorted [`IndexEntry`] values, and the transaction layer will later decide
//! when those entries become visible.

mod index;

pub use index::*;
