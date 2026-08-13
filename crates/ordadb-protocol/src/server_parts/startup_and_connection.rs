use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rand::RngCore;
use rand::rngs::OsRng;
use rustls::pki_types::pem::{Error as PemError, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig as RustlsServerConfig, ServerConnection, StreamOwned};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use ordadb_admin::{
    Action, AuthStore, Authorizer, CancellationHandle, DbObject, Principal, QueryOutcome,
    SessionRegistry,
};
use ordadb_catalog::ColumnDefinition;
use ordadb_engine::{
    CatalogRoleMetadata, CatalogVisibility, CatalogVisibilityScope, Engine, Session,
    SessionAuthorization, SessionRuntimeMetadata, StatementDescription, TransactionStatus,
};
use ordadb_sql::{ParsedStatement, parse};
use ordadb_types::{
    Batch, CommandComplete, DbError, Field, Identifier, QueryEvent, QueryProgress, Result, Row,
    ScalarType, Schema, Value,
};

use crate::codec::{
    DEFAULT_MAX_FRAME_BYTES, FrontendMessage, FrontendMessageReader, FrontendRead, StartupPacket,
    io_error, protocol, read_startup, write_backend_key, write_bind_complete, write_close_complete,
    write_command_complete, write_empty_query, write_error, write_message, write_no_data,
    write_notice, write_notification, write_parameter_description, write_parameter_status,
    write_parse_complete, write_portal_suspended, write_ready,
};
use crate::scram::authenticate;
use crate::security::{
    execute_security_statement, is_security_sql, parse_security_statement, redacted_security_sql,
};
use crate::settings::{PgSessionSettings, PgSettingStatement, parse_setting_statement};
use crate::value::{
    decode_parameters_as, encode_text, type_oid, write_data_row, write_row_description,
};

const DEFAULT_MAX_PREPARED: usize = 1024;
const DEFAULT_MAX_PORTALS: usize = 1024;
const DEFAULT_MAX_COPY_BYTES: usize = 64 * 1024 * 1024;
const COPY_INSERT_BATCH_ROWS: usize = 128;
const COPY_INSERT_BATCH_PARAMETERS: usize = 4_096;
const SOCKET_TIMEOUT: Duration = Duration::from_secs(300);
const SOCKET_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsPaths {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PgServerConfig {
    pub max_frame_bytes: usize,
    pub max_prepared_statements: usize,
    pub max_portals: usize,
    pub max_copy_bytes: usize,
    pub server_version: String,
}

impl Default for PgServerConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_prepared_statements: DEFAULT_MAX_PREPARED,
            max_portals: DEFAULT_MAX_PORTALS,
            max_copy_bytes: DEFAULT_MAX_COPY_BYTES,
            server_version: format!("18.0 (OrdaDB {})", env!("CARGO_PKG_VERSION")),
        }
    }
}

impl PgServerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.max_frame_bytes < 8 {
            return Err(invalid("max_frame_bytes must be at least 8"));
        }
        if self.max_prepared_statements == 0 || self.max_portals == 0 {
            return Err(invalid(
                "prepared statement and portal limits must be greater than zero",
            ));
        }
        if self.max_copy_bytes == 0 {
            return Err(invalid("max_copy_bytes must be greater than zero"));
        }
        if self.server_version.is_empty() || self.server_version.as_bytes().contains(&0) {
            return Err(invalid(
                "server_version must be non-empty without NUL bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct PgConnectionContext {
    engine: Arc<Engine>,
    auth: Arc<AuthStore>,
    registry: Arc<SessionRegistry>,
    config: PgServerConfig,
    tls: Option<Arc<RustlsServerConfig>>,
}

impl PgConnectionContext {
    #[must_use]
    pub fn new(
        engine: Arc<Engine>,
        auth: Arc<AuthStore>,
        registry: Arc<SessionRegistry>,
        config: PgServerConfig,
        tls: Option<Arc<RustlsServerConfig>>,
    ) -> Self {
        Self {
            engine,
            auth,
            registry,
            config,
            tls,
        }
    }
}

pub fn load_tls_config(paths: &TlsPaths) -> Result<Arc<RustlsServerConfig>> {
    let certificate = File::open(&paths.certificate)
        .map_err(|error| io_error("failed to open TLS certificate", error))?;
    let certificates = CertificateDer::pem_reader_iter(certificate)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            invalid("failed to parse TLS certificate").with_detail(error.to_string())
        })?;
    if certificates.is_empty() {
        return Err(invalid("TLS certificate file contains no certificates"));
    }
    let private_key = File::open(&paths.private_key)
        .map_err(|error| io_error("failed to open TLS private key", error))?;
    let private_key = match PrivateKeyDer::from_pem_reader(private_key) {
        Ok(private_key) => private_key,
        Err(PemError::NoItemsFound) => {
            return Err(invalid("TLS private key file contains no private key"));
        }
        Err(error) => {
            return Err(invalid("failed to parse TLS private key").with_detail(error.to_string()));
        }
    };
    RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map(Arc::new)
        .map_err(|error| invalid(format!("TLS certificate/private key mismatch: {error}")))
}

pub fn serve_tcp_connection(
    stream: TcpStream,
    peer: String,
    context: PgConnectionContext,
) -> Result<()> {
    serve_tcp_connection_inner(stream, peer, context, None)
}

pub fn serve_tcp_connection_with_shutdown(
    stream: TcpStream,
    peer: String,
    context: PgConnectionContext,
    shutdown: CancellationToken,
) -> Result<()> {
    serve_tcp_connection_inner(stream, peer, context, Some(shutdown))
}

fn serve_tcp_connection_inner(
    stream: TcpStream,
    peer: String,
    context: PgConnectionContext,
    shutdown: Option<CancellationToken>,
) -> Result<()> {
    let PgConnectionContext {
        engine,
        auth,
        registry,
        config,
        tls,
    } = context;
    config.validate()?;
    stream
        .set_nodelay(true)
        .map_err(|error| io_error("failed to configure PostgreSQL socket", error))?;
    let socket_timeout = if shutdown.is_some() {
        SOCKET_POLL_INTERVAL
    } else {
        SOCKET_TIMEOUT
    };
    stream
        .set_read_timeout(Some(socket_timeout))
        .map_err(|error| io_error("failed to configure PostgreSQL read timeout", error))?;
    stream
        .set_write_timeout(Some(socket_timeout))
        .map_err(|error| io_error("failed to configure PostgreSQL write timeout", error))?;

    let stream = InterruptibleTcpStream {
        stream,
        shutdown,
        frontend_polling: false,
    };
    let connection_shutdown = stream.shutdown.clone();
    let (mut stream, startup) = negotiate_startup(stream, tls, config.max_frame_bytes)?;
    let parameters = match startup {
        StartupPacket::Startup(parameters) => parameters,
        StartupPacket::CancelRequest {
            process_id,
            secret_key,
        } => {
            registry.cancel(process_id, secret_key)?;
            return Ok(());
        }
        StartupPacket::SslRequest => {
            return Err(protocol(
                "startup negotiation did not yield a startup packet",
            ));
        }
        StartupPacket::GssEncRequest => {
            return Err(protocol(
                "GSS encryption negotiation did not yield a startup packet",
            ));
        }
    };
    let parameters: BTreeMap<String, String> = parameters.into_iter().collect();
    let user = parameters
        .get("user")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| DbError::new("28000", "startup parameter `user` is required"))?;
    let database = parameters
        .get("database")
        .filter(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| "ordadb".to_owned());
    let principal = match authenticate(&mut stream, user, &auth, config.max_frame_bytes) {
        Ok(principal) => principal,
        Err(error) => {
            let _ = write_error(&mut stream, &error);
            let _ = stream.flush();
            return Err(error);
        }
    };
    let settings = PgSessionSettings::from_startup(
        config.server_version.clone(),
        &principal.user,
        &parameters,
    )?;
    let authorizer = Authorizer::from_store(&auth)?;
    authorizer.authorize_sql(&principal, &database, "CONNECT")?;
    let bypass_ownership = match authorizer.authorize(&principal, Action::Manage, &DbObject::Server)
    {
        Ok(()) => true,
        Err(error) if error.sql_state == "42501" => false,
        Err(error) => return Err(error),
    };

    let mut secret = [0_u8; 4];
    OsRng.fill_bytes(&mut secret);
    let secret_key = u32::from_ne_bytes(secret);
    let handle = registry.register_session(
        principal.user.clone(),
        database.clone(),
        parameters.get("application_name").cloned(),
        peer,
        secret_key,
    )?;
    let _guard = SessionGuard::new(Arc::clone(&registry), handle.process_id());
    let mut session = connect_postgresql_session(&engine, &principal, bypass_ownership)?;
    if let Some(query_memory_bytes) = parameters.get("ordadb_query_memory_bytes") {
        let query_memory_bytes = query_memory_bytes.parse::<usize>().map_err(|_| {
            DbError::new(
                "22023",
                "ordadb_query_memory_bytes must be a positive integer",
            )
        })?;
        session.set_query_memory_limit(query_memory_bytes)?;
    }
    session.set_backend_process_id(handle.process_id())?;
    session.set_runtime_metadata(session_runtime_metadata(&settings, &database, &principal)?);
    refresh_system_catalog_metadata(&mut session, &auth, &settings, &principal, &database)?;
    write_startup_responses(&mut stream, &settings, &handle)?;
    stream.enable_frontend_polling()?;
    let mut connection = Connection {
        stream,
        session,
        engine,
        auth,
        registry,
        principal,
        database,
        handle,
        config,
        settings,
        prepared: BTreeMap::new(),
        portals: BTreeMap::new(),
        frontend_reader: FrontendMessageReader::default(),
        extended_state: ExtendedQueryState::Ready,
        shutdown: connection_shutdown,
    };
    connection.run()
}

fn negotiate_startup(
    mut stream: InterruptibleTcpStream,
    tls: Option<Arc<RustlsServerConfig>>,
    max_frame_bytes: usize,
) -> Result<(ConnectionStream, StartupPacket)> {
    let mut negotiation = StartupNegotiation::default();
    loop {
        match read_startup(&mut stream, max_frame_bytes)? {
            StartupPacket::SslRequest => {
                negotiation.record(EncryptionRequest::Ssl)?;
                match tls {
                    Some(config) => {
                        stream
                            .write_all(b"S")
                            .map_err(|error| io_error("failed to accept TLS negotiation", error))?;
                        stream
                            .flush()
                            .map_err(|error| io_error("failed to flush TLS negotiation", error))?;
                        let connection = ServerConnection::new(config).map_err(|error| {
                            invalid(format!("failed to create TLS session: {error}"))
                        })?;
                        let mut tls_stream =
                            ConnectionStream::Tls(Box::new(StreamOwned::new(connection, stream)));
                        let startup = read_startup(&mut tls_stream, max_frame_bytes)?;
                        if matches!(
                            startup,
                            StartupPacket::SslRequest | StartupPacket::GssEncRequest
                        ) {
                            return Err(protocol(
                                "nested encryption negotiation request is invalid",
                            ));
                        }
                        if let StartupPacket::CancelRequest {
                            process_id: _,
                            secret_key: _,
                        } = startup
                        {
                            return Err(protocol("TLS cancel request must use a fresh connection"));
                        }
                        return Ok((tls_stream, startup));
                    }
                    None => {
                        stream
                            .write_all(b"N")
                            .map_err(|error| io_error("failed to reject TLS negotiation", error))?;
                        stream
                            .flush()
                            .map_err(|error| io_error("failed to flush TLS rejection", error))?;
                    }
                }
            }
            StartupPacket::GssEncRequest => {
                negotiation.record(EncryptionRequest::Gss)?;
                stream
                    .write_all(b"N")
                    .map_err(|error| io_error("failed to reject GSS encryption", error))?;
                stream
                    .flush()
                    .map_err(|error| io_error("failed to flush GSS encryption rejection", error))?;
            }
            StartupPacket::CancelRequest {
                process_id,
                secret_key,
            } => {
                return Ok((
                    ConnectionStream::Plain(stream),
                    StartupPacket::CancelRequest {
                        process_id,
                        secret_key,
                    },
                ));
            }
            startup @ StartupPacket::Startup(_) => {
                return Ok((ConnectionStream::Plain(stream), startup));
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EncryptionRequest {
    Gss,
    Ssl,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct StartupNegotiation {
    saw_gss: bool,
    saw_ssl: bool,
}

impl StartupNegotiation {
    fn record(&mut self, request: EncryptionRequest) -> Result<()> {
        match request {
            EncryptionRequest::Gss if self.saw_gss || self.saw_ssl => {
                Err(protocol("repeated or out-of-order GSS encryption request"))
            }
            EncryptionRequest::Gss => {
                self.saw_gss = true;
                Ok(())
            }
            EncryptionRequest::Ssl if self.saw_ssl => {
                Err(protocol("repeated SSLRequest is invalid"))
            }
            EncryptionRequest::Ssl => {
                self.saw_ssl = true;
                Ok(())
            }
        }
    }
}

enum ConnectionStream {
    Plain(InterruptibleTcpStream),
    Tls(Box<StreamOwned<ServerConnection, InterruptibleTcpStream>>),
}

impl ConnectionStream {
    fn enable_frontend_polling(&mut self) -> Result<()> {
        match self {
            Self::Plain(stream) => stream.enable_frontend_polling(),
            Self::Tls(stream) => stream.sock.enable_frontend_polling(),
        }
    }
}

struct InterruptibleTcpStream {
    stream: TcpStream,
    shutdown: Option<CancellationToken>,
    frontend_polling: bool,
}

impl InterruptibleTcpStream {
    fn should_retry_read(&self, error: &std::io::Error) -> bool {
        !self.frontend_polling
            && self.shutdown.is_some()
            && matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock)
            && !self
                .shutdown
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
    }

    fn enable_frontend_polling(&mut self) -> Result<()> {
        self.stream
            .set_read_timeout(Some(SOCKET_POLL_INTERVAL))
            .map_err(|error| io_error("failed to configure notification polling", error))?;
        self.frontend_polling = true;
        Ok(())
    }
}

impl Read for InterruptibleTcpStream {
    fn read(&mut self, target: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.stream.read(target) {
                Err(error) if self.should_retry_read(&error) => {}
                Err(error)
                    if self.is_shutdown()
                        && matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) =>
                {
                    return Ok(0);
                }
                result => return result,
            }
        }
    }
}

impl Write for InterruptibleTcpStream {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        loop {
            match self.stream.write(bytes) {
                Err(error)
                    if self.shutdown.is_some()
                        && matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock)
                        && !self.is_shutdown() => {}
                Err(error)
                    if self.is_shutdown()
                        && matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) =>
                {
                    return Err(std::io::Error::new(
                        ErrorKind::BrokenPipe,
                        "server is shutting down",
                    ));
                }
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

impl Read for ConnectionStream {
    fn read(&mut self, target: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(target),
            Self::Tls(stream) => stream.read(target),
        }
    }
}

impl Write for ConnectionStream {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(bytes),
            Self::Tls(stream) => stream.write(bytes),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

fn write_startup_responses<W: Write>(
    writer: &mut W,
    settings: &PgSessionSettings,
    handle: &CancellationHandle,
) -> Result<()> {
    for (name, value) in settings.parameter_statuses() {
        write_parameter_status(writer, name, value)?;
    }
    write_backend_key(writer, handle.process_id(), handle.secret_key())?;
    write_ready(writer, b'I')?;
    writer
        .flush()
        .map_err(|error| io_error("failed to flush startup responses", error))
}

struct SessionGuard {
    registry: Arc<SessionRegistry>,
    process_id: u32,
}

impl SessionGuard {
    fn new(registry: Arc<SessionRegistry>, process_id: u32) -> Self {
        Self {
            registry,
            process_id,
        }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        let _ = self.registry.unregister_session(self.process_id);
    }
}

#[derive(Clone)]
struct PreparedStatement {
    sql: String,
    parameter_oids: Vec<u32>,
    parameter_types: Vec<ScalarType>,
    schema: Schema,
}

fn resolve_parameter_oids(
    declared_oids: &[u32],
    inferred_types: &[ScalarType],
) -> Result<Vec<u32>> {
    let inferred_oids = inferred_types.iter().map(type_oid).collect::<Vec<_>>();
    if declared_oids.is_empty() {
        return Ok(inferred_oids);
    }
    if declared_oids.len() != inferred_oids.len() {
        return Err(protocol(format!(
            "declared parameter count {} does not match inferred count {}",
            declared_oids.len(),
            inferred_oids.len()
        )));
    }
    declared_oids
        .iter()
        .copied()
        .zip(inferred_oids)
        .enumerate()
        .map(|(index, (declared, inferred))| match declared {
            0 => Ok(inferred),
            value if parameter_oid_can_coerce(value, inferred) => Ok(value),
            value => Err(DbError::new(
                "42804",
                format!(
                    "parameter ${} has type OID {value}, but the statement requires OID {inferred}",
                    index + 1
                ),
            )),
        })
        .collect()
}

fn parameter_oid_can_coerce(source: u32, target: u32) -> bool {
    use crate::value::{
        OID_BPCHAR, OID_FLOAT4, OID_FLOAT8, OID_INT2, OID_INT4, OID_INT8, OID_NUMERIC, OID_TEXT,
        OID_VARCHAR,
    };

    source == target
        || matches!(
            (source, target),
            (
                OID_INT2,
                OID_INT4 | OID_INT8 | OID_FLOAT4 | OID_FLOAT8 | OID_NUMERIC
            ) | (OID_INT4, OID_INT8 | OID_FLOAT8 | OID_NUMERIC)
                | (OID_INT8, OID_FLOAT8 | OID_NUMERIC)
        )
        || matches!(source, OID_TEXT | OID_BPCHAR | OID_VARCHAR)
            && matches!(target, OID_TEXT | OID_BPCHAR | OID_VARCHAR)
}

struct Portal {
    statement_name: String,
    sql: String,
    parameters: Vec<Value>,
    result_formats: Vec<i16>,
    stream: Option<Box<dyn Iterator<Item = Result<QueryEvent>>>>,
    schema: Option<Schema>,
    pending_rows: VecDeque<Row>,
    completed: bool,
    query_id: Option<String>,
    rows_processed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtendedQueryState {
    Ready,
    FailedUntilSync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtocolSessionReset {
    DeallocateAll,
    DiscardAll,
}

fn protocol_session_reset(sql: &str) -> Result<Option<ProtocolSessionReset>> {
    let first_word = sql
        .trim_start()
        .split_once(char::is_whitespace)
        .map_or_else(|| sql.trim(), |(word, _)| word);
    if !first_word.eq_ignore_ascii_case("DISCARD") && !first_word.eq_ignore_ascii_case("DEALLOCATE")
    {
        return Ok(None);
    }
    match parse(sql)? {
        ParsedStatement::DiscardAll => Ok(Some(ProtocolSessionReset::DiscardAll)),
        ParsedStatement::DeallocateAll => Ok(Some(ProtocolSessionReset::DeallocateAll)),
        _ => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailedMessageAction {
    Synchronize,
    Terminate,
    Flush,
    Ignore,
}

fn failed_message_action(message: &FrontendMessage) -> FailedMessageAction {
    match message {
        FrontendMessage::Sync => FailedMessageAction::Synchronize,
        FrontendMessage::Terminate => FailedMessageAction::Terminate,
        FrontendMessage::Flush => FailedMessageAction::Flush,
        _ => FailedMessageAction::Ignore,
    }
}

fn ensure_prepared_statement_slot(
    prepared: &BTreeMap<String, PreparedStatement>,
    name: &str,
    limit: usize,
) -> Result<()> {
    if !name.is_empty() && prepared.contains_key(name) {
        return Err(DbError::new(
            "42P05",
            format!("prepared statement {name} already exists"),
        ));
    }
    if !prepared.contains_key(name) && prepared.len() >= limit {
        return Err(DbError::new(
            "54000",
            "prepared statement count exceeds the configured limit",
        ));
    }
    Ok(())
}

fn ensure_portal_slot(portals: &BTreeMap<String, Portal>, name: &str, limit: usize) -> Result<()> {
    if !name.is_empty() && portals.contains_key(name) {
        return Err(DbError::new(
            "42P03",
            format!("portal {name} already exists"),
        ));
    }
    if !portals.contains_key(name) && portals.len() >= limit {
        return Err(DbError::new(
            "54000",
            "portal count exceeds the configured limit",
        ));
    }
    Ok(())
}

fn retire_portal(registry: &SessionRegistry, portal: Portal, outcome: QueryOutcome) -> Result<()> {
    if !portal.completed
        && let Some(query_id) = portal.query_id
    {
        registry.finish_query(&query_id, outcome)?;
    }
    Ok(())
}

struct Connection {
    stream: ConnectionStream,
    session: Session,
    engine: Arc<Engine>,
    auth: Arc<AuthStore>,
    registry: Arc<SessionRegistry>,
    principal: ordadb_admin::Principal,
    database: String,
    handle: CancellationHandle,
    config: PgServerConfig,
    settings: PgSessionSettings,
    prepared: BTreeMap<String, PreparedStatement>,
    portals: BTreeMap<String, Portal>,
    frontend_reader: FrontendMessageReader,
    extended_state: ExtendedQueryState,
    shutdown: Option<CancellationToken>,
}
