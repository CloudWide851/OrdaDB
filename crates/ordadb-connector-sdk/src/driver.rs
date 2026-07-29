use std::{future::Future, pin::Pin};

use async_trait::async_trait;
use ordadb_types::Result;
use tokio_util::sync::CancellationToken;

use crate::{
    ConnectorCapabilitiesV2, ConnectorCatalogObjectV2, ConnectorCredentialV2, ConnectorEndpointV2,
    ConnectorIsolationLevelV2, ConnectorParameterV2, ConnectorQueryEventV2, ConnectorTlsModeV2,
};

pub type ConnectorEventFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

pub trait ConnectorEventSink: Send {
    fn send(&mut self, event: ConnectorQueryEventV2) -> ConnectorEventFuture<'_>;
}

#[async_trait]
pub trait ConnectorSession: Send {
    fn capabilities(&self) -> &ConnectorCapabilitiesV2;

    async fn catalog(&mut self) -> Result<Vec<ConnectorCatalogObjectV2>>;

    async fn execute(
        &mut self,
        request_id: &str,
        sql: &str,
        params: &[ConnectorParameterV2],
        batch_size: u32,
        cancellation: &CancellationToken,
        sink: &mut dyn ConnectorEventSink,
    ) -> Result<()>;

    async fn cancel(&mut self, request_id: &str) -> Result<()>;

    async fn begin(&mut self, isolation: Option<ConnectorIsolationLevelV2>) -> Result<()>;

    async fn commit(&mut self) -> Result<()>;

    async fn rollback(&mut self) -> Result<()>;
}

#[async_trait]
pub trait ConnectorDriver: Send + Sync + 'static {
    fn capabilities(&self) -> ConnectorCapabilitiesV2;

    async fn connect(
        &self,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<Box<dyn ConnectorSession>>;
}
