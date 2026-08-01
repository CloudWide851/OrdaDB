//! PostgreSQL v3 wire codec, authenticated server connection, and minimal client.

mod client;
mod codec;
mod scram;
mod security;
mod server;
mod value;

pub use client::{
    ClientConfig, CopyOutResult, PgCancelToken, PgClient, PgQueryEvent, PgTransactionStatus,
    QueryResult, QuerySummary,
};
pub use server::{
    PgConnectionContext, PgServerConfig, TlsPaths, load_tls_config, serve_tcp_connection,
    serve_tcp_connection_with_shutdown,
};
