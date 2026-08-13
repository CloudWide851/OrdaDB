//! Physical relational operators and typed scalar evaluation for OrdaDB.

mod advanced;
mod columnar;
mod memory;
mod scan;

include!("execution_parts/contracts_memory_and_spill.rs");
include!("execution_parts/cursor_and_pipeline.rs");
include!("execution_parts/spill_codec_and_expressions.rs");
include!("execution_parts/expression_evaluation.rs");
include!("execution_parts/casts_and_comparisons.rs");
include!("execution_parts/tests.rs");
