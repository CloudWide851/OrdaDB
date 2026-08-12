//! Syntax contracts, dialect parsing, type inference, and catalog binding.

include!("contracts.rs");
include!("effects.rs");
include!("extended_parser.rs");
include!("sequence_parser.rs");
include!("parameter_solver.rs");
include!("parameter_constraints.rs");
include!("type_resolution.rs");
include!("statement_binder.rs");
include!("routine_binder.rs");
include!("statement_conversion.rs");
include!("ddl_conversion.rs");
include!("query_conversion.rs");
include!("expression_conversion.rs");
include!("window_conversion.rs");
include!("type_conversion.rs");
include!("dml_binding.rs");
include!("ddl_binding.rs");
include!("cte_binding.rs");
include!("apply_binding.rs");
include!("select_binding.rs");
include!("expression_binding.rs");
include!("relation_binding.rs");
include!("scalar_binding.rs");
include!("parser_support.rs");
