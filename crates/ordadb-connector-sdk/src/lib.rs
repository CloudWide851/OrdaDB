mod driver;
mod driver_v3;
mod protocol;
mod protocol_v3;
#[cfg(windows)]
mod runtime;
#[cfg(windows)]
mod runtime_v3;

pub use driver::{ConnectorDriver, ConnectorEventSink, ConnectorSession};
pub use driver_v3::{ConnectorDriverV3, ConnectorEventSinkV3, ConnectorSessionV3};
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
pub use protocol_v3::{
    CONNECTOR_PROTOCOL_V3, ConnectorCapabilitiesV3, ConnectorCatalogNodeKindV3,
    ConnectorCatalogNodeV3, ConnectorCatalogPageV3, ConnectorCommandInputModeV3,
    ConnectorCommandLanguageV3, ConnectorCommandV3, ConnectorErrorCategoryV3, ConnectorErrorV3,
    ConnectorKeyValueV3, ConnectorKindV3, ConnectorRequestV3, ConnectorResponseV3,
    ConnectorResultBatchV3, ConnectorResultEventV3, ConnectorResultStreamValidatorV3,
    MAX_CONNECTOR_CATALOG_PAGE_NODES, MAX_CONNECTOR_COMMAND_ARGUMENTS, MAX_CONNECTOR_CURSOR_BYTES,
    MAX_CONNECTOR_JSON_DEPTH, MAX_CONNECTOR_LANGUAGES, ProtocolHelloV3, ProtocolReadyV3,
    read_connector_frame_v3, validate_capabilities_v3, validate_catalog_page_v3,
    validate_catalog_request_v3, validate_command_v3, validate_error_v3,
    validate_protocol_ready_v3, write_connector_frame_v3,
};
#[cfg(windows)]
pub use runtime::{connector_pipe_argument, run_named_pipe_helper};
#[cfg(windows)]
pub use runtime_v3::run_named_pipe_helper_v3;
