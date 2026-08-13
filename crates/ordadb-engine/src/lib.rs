//! SQL execution, transaction coordination, and durable publication for OrdaDB.
//!
//! This crate owns SQL semantics and candidate-state atomicity. Physical page
//! encoding belongs to `ordadb-storage`; WAL and crash recovery belong to
//! `ordadb-transaction`.

mod system_catalog;

include!("engine_parts/contracts_and_engine.rs");
include!("engine_parts/engine_and_session_state.rs");
include!("engine_parts/session_lifecycle.rs");
include!("engine_parts/session_execution.rs");
include!("engine_parts/statement_schema_and_transaction.rs");
include!("engine_parts/query_streams_and_routines.rs");
include!("engine_parts/storage_read_stream.rs");
include!("engine_parts/persistence_and_transaction_commands.rs");
include!("engine_parts/bound_execution_and_routines.rs");
include!("engine_parts/routine_vm_and_triggers.rs");
include!("engine_parts/drop_and_alter.rs");
include!("engine_parts/plpgsql_host_and_merge.rs");
include!("engine_parts/dml_and_select.rs");
include!("engine_parts/set_queries_and_advanced_select.rs");
include!("engine_parts/explain_update_and_views.rs");
include!("engine_parts/validation_and_derived_state.rs");
