mod credentials;

include!("dbms_parts/contracts_and_runtime_state.rs");
include!("dbms_parts/connection_probe_and_catalog.rs");
include!("dbms_parts/execution_transactions_and_admin.rs");
include!("dbms_parts/commands_validation_and_admin_http.rs");
include!("dbms_parts/event_projection_and_platform.rs");
include!("dbms_parts/tests.rs");
