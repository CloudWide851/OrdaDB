//! Authentication, authorization, operational registries, and management API.

mod api;
mod auth;
mod operations;
mod rbac;
mod registry;

pub use api::{AdminState, ApiEnvelope, api_router};
pub use auth::{
    AUTH_FORMAT_VERSION, AuthStore, POSTGRES_ROLE_OID_FIRST_USER, Principal, SafeRoleMetadata,
    SafeRoleMetadataSnapshot, ScramVerifier, TokenResponse, TokenStore,
};
pub use operations::{
    OperationKind, OperationManager, OperationRecord, OperationState, StartOperation,
};
pub use rbac::{Action, Authorizer, DbObject, Grant, Role};
pub use registry::{
    CancellationHandle, QueryInfo, QueryOutcome, ServerEvent, SessionInfo, SessionRegistry,
};
