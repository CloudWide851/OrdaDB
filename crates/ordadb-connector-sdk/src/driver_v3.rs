use std::{future::Future, pin::Pin};

use async_trait::async_trait;
use ordadb_types::Result;
use tokio_util::sync::CancellationToken;

use crate::{
    ConnectorCapabilitiesV3, ConnectorCatalogPageV3, ConnectorCommandV3, ConnectorCredentialV2,
    ConnectorEndpointV2, ConnectorIsolationLevelV2, ConnectorResultEventV3, ConnectorTlsModeV2,
};

pub type ConnectorEventFutureV3<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

pub trait ConnectorEventSinkV3: Send {
    fn send(&mut self, event: ConnectorResultEventV3) -> ConnectorEventFutureV3<'_>;
}

#[async_trait]
pub trait ConnectorSessionV3: Send {
    fn capabilities(&self) -> &ConnectorCapabilitiesV3;

    async fn catalog_page(
        &mut self,
        parent_id: Option<&str>,
        page_size: u32,
        cursor: Option<&str>,
    ) -> Result<ConnectorCatalogPageV3>;

    async fn execute(
        &mut self,
        request_id: &str,
        command: &ConnectorCommandV3,
        batch_size: u32,
        cancellation: &CancellationToken,
        sink: &mut dyn ConnectorEventSinkV3,
    ) -> Result<()>;

    async fn cancel(&mut self, request_id: &str) -> Result<()>;

    async fn begin(&mut self, isolation: Option<ConnectorIsolationLevelV2>) -> Result<()>;

    async fn commit(&mut self) -> Result<()>;

    async fn rollback(&mut self) -> Result<()>;
}

#[async_trait]
pub trait ConnectorDriverV3: Send + Sync + 'static {
    fn capabilities(&self) -> ConnectorCapabilitiesV3;

    async fn connect(
        &self,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<Box<dyn ConnectorSessionV3>>;
}
