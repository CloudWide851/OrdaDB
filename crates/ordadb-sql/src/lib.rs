//! Multi-dialect parsing and catalog-aware binding for OrdaDB.
//!
//! The public syntax tree in this crate is owned by OrdaDB. `sqlparser` is an
//! implementation detail. Every accepted source dialect is normalized into
//! OrdaDB's PostgreSQL-compatible semantics before binding.

mod sql;

pub use sql::*;
