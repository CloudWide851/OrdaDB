//! Authentication, authorization, operational registries, and management API.

mod api;
mod auth;
mod rbac;
mod registry;

pub use api::{AdminState, ApiEnvelope, api_router};
pub use auth::{
    AUTH_FORMAT_VERSION, AuthStore, Principal, ScramVerifier, TokenResponse, TokenStore,
};
pub use rbac::{Action, Authorizer, DbObject, Grant, Role};
pub use registry::{
    CancellationHandle, QueryInfo, QueryOutcome, ServerEvent, SessionInfo, SessionRegistry,
};
