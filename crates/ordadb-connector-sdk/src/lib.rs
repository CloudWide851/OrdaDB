mod driver;
mod protocol;
#[cfg(windows)]
mod runtime;

pub use driver::{ConnectorDriver, ConnectorEventSink, ConnectorSession};
pub use protocol::{
    CONNECTOR_PROTOCOL_V2, ConnectorBatchV2, ConnectorCapabilitiesV2, ConnectorCatalogColumnV2,
    ConnectorCatalogObjectKindV2, ConnectorCatalogObjectV2, ConnectorColumnV2,
    ConnectorCredentialV2, ConnectorEndpointV2, ConnectorErrorV2, ConnectorIsolationLevelV2,
    ConnectorLogicalTypeV2, ConnectorNoticeV2, ConnectorParameterV2, ConnectorQueryEventV2,
    ConnectorRequestV2, ConnectorResponseV2, ConnectorTlsModeV2, ConnectorTransactionStateV2,
    ConnectorTypeV2, ConnectorValueV2, MAX_CONNECTOR_FRAME_BYTES, MAX_CONNECTOR_TEXT_BYTES,
    ProtocolHelloV2, ProtocolReadyV2, read_connector_frame, validate_capabilities,
    validate_endpoint, validate_protocol_ready, write_connector_frame,
};
#[cfg(windows)]
pub use runtime::{connector_pipe_argument, run_named_pipe_helper};
