#[cfg(windows)]
mod host;
mod manager;
mod manifest;
mod protocol;

#[cfg(windows)]
pub use host::ConnectorHost;
pub use manager::{
    DownloadReceipt, HttpsRegistryTransport, OperationKind, OperationStarted, PluginCatalogItem,
    PluginCatalogSnapshot, PluginLifecycle, PluginManager, PluginManagerOptions, PluginProgress,
    PluginProgressPhase, RegistryAvailability, RegistryStatus, RegistryTransport,
};
pub use manifest::{
    ConnectorArchitecture, ConnectorDialect, ConnectorPermission, ManifestPolicy, PluginManifestV1,
    RegistryCatalogV1, decode_public_key, manifest_signing_payload, validate_manifest,
};
#[cfg(windows)]
pub use ordadb_windows::{CredentialVault, StoredCredential};
pub use protocol::{
    CatalogEntry, ConnectorRequestV1, ConnectorResponseV1, CredentialPayload,
    MAX_CONNECTOR_FRAME_BYTES, ProtocolReady, read_connector_frame, validate_protocol_ready,
    write_connector_frame,
};

use ordadb_types::DbError;

pub const CONNECTOR_MANIFEST_VERSION: u32 = 1;
pub const MIN_CONNECTOR_API_VERSION: u32 = 1;
pub const CONNECTOR_API_VERSION: u32 = 2;

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::new("08P01", message)
}

fn security_error(message: impl Into<String>) -> DbError {
    DbError::new("28000", message)
}

fn io_error(context: impl Into<String>, error: impl std::fmt::Display) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

fn network_error(context: impl Into<String>, error: impl std::fmt::Display) -> DbError {
    DbError::new("08006", context).with_detail(error.to_string())
}
