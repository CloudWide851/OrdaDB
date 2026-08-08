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
use ordadb_types::{
    Batch, CommandComplete, DbError, Field, Identifier, QueryEvent, QueryProgress, Result, Row,
    ScalarType, Schema, Value,
};

use crate::codec::{
    DEFAULT_MAX_FRAME_BYTES, FrontendMessage, StartupPacket, io_error, protocol, read_frontend,
    read_startup, write_backend_key, write_bind_complete, write_close_complete,
    write_command_complete, write_empty_query, write_error, write_message, write_no_data,
    write_notice, write_parameter_description, write_parameter_status, write_parse_complete,
    write_portal_suspended, write_ready,
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

    let stream = InterruptibleTcpStream { stream, shutdown };
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
    write_startup_responses(&mut stream, &settings, &handle)?;

    let mut session = connect_postgresql_session(&engine, &principal, bypass_ownership)?;
    session.set_runtime_metadata(session_runtime_metadata(&settings, &database, &principal)?);
    refresh_system_catalog_metadata(&mut session, &auth, &settings, &principal, &database)?;
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

struct InterruptibleTcpStream {
    stream: TcpStream,
    shutdown: Option<CancellationToken>,
}

impl InterruptibleTcpStream {
    fn should_retry(&self, error: &std::io::Error) -> bool {
        self.shutdown.is_some()
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
}

impl Read for InterruptibleTcpStream {
    fn read(&mut self, target: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.stream.read(target) {
                Err(error) if self.should_retry(&error) => {}
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
                Err(error) if self.should_retry(&error) => {}
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
    extended_state: ExtendedQueryState,
    shutdown: Option<CancellationToken>,
}

impl Connection {
    fn run(&mut self) -> Result<()> {
        loop {
            let Some(message) = read_frontend(&mut self.stream, self.config.max_frame_bytes)?
            else {
                return Ok(());
            };
            if self.extended_state == ExtendedQueryState::FailedUntilSync {
                match failed_message_action(&message) {
                    FailedMessageAction::Synchronize => {
                        self.extended_state = ExtendedQueryState::Ready;
                        write_ready(&mut self.stream, transaction_status(&self.session))?;
                    }
                    FailedMessageAction::Terminate => return Ok(()),
                    FailedMessageAction::Flush => self.flush()?,
                    FailedMessageAction::Ignore => {}
                }
                continue;
            }
            let extended = !matches!(
                message,
                FrontendMessage::Query(_) | FrontendMessage::Terminate | FrontendMessage::Flush
            );
            let result = self.handle_message(message);
            if let Err(error) = result {
                write_error(&mut self.stream, &error)?;
                if extended {
                    self.extended_state = ExtendedQueryState::FailedUntilSync;
                } else {
                    write_ready(&mut self.stream, transaction_status(&self.session))?;
                }
            }
        }
    }

    fn handle_message(&mut self, message: FrontendMessage) -> Result<()> {
        match message {
            FrontendMessage::Query(sql) => self.simple_query(&sql),
            FrontendMessage::Parse {
                name,
                sql,
                parameter_oids,
            } => self.parse(name, sql, parameter_oids),
            FrontendMessage::Bind {
                portal,
                statement,
                parameter_formats,
                parameters,
                result_formats,
            } => self.bind(
                portal,
                &statement,
                &parameter_formats,
                &parameters,
                result_formats,
            ),
            FrontendMessage::Describe { kind, name } => self.describe(kind, &name),
            FrontendMessage::Execute { portal, max_rows } => self.execute_portal(&portal, max_rows),
            FrontendMessage::Close { kind, name } => self.close(kind, &name),
            FrontendMessage::Sync => {
                write_ready(&mut self.stream, transaction_status(&self.session))
            }
            FrontendMessage::Flush => self.flush(),
            FrontendMessage::Terminate => Ok(()),
            FrontendMessage::Password(_) => Err(protocol(
                "PasswordMessage is only valid during authentication",
            )),
            FrontendMessage::CopyData(_)
            | FrontendMessage::CopyDone
            | FrontendMessage::CopyFail(_) => {
                Err(protocol("COPY message is not valid outside COPY IN"))
            }
        }
    }

    fn simple_query(&mut self, sql: &str) -> Result<()> {
        self.prepared.remove("");
        if let Some(portal) = self.portals.remove("") {
            retire_portal(&self.registry, portal, QueryOutcome::Cancelled)?;
        }
        let statements = split_statements(sql)?;
        if statements.is_empty() {
            write_empty_query(&mut self.stream)?;
            write_ready(&mut self.stream, transaction_status(&self.session))?;
            return self.flush();
        }
        for statement in statements {
            if let Some(copy) = parse_copy(&statement)? {
                self.execute_copy(copy)?;
            } else {
                self.execute_simple_statement(&statement)?;
            }
        }
        write_ready(&mut self.stream, transaction_status(&self.session))?;
        self.flush()
    }

    fn execute_simple_statement(&mut self, sql: &str) -> Result<()> {
        self.authorize(sql)?;
        self.registry.reset_cancellation(self.handle.process_id())?;
        let query_id = Uuid::new_v4().to_string();
        self.registry.begin_query(
            self.handle.process_id(),
            query_id.clone(),
            redacted_security_sql(sql),
        )?;
        let result = (|| {
            let stream = self.statement_stream(sql, &[])?;
            let mut schema = Schema::empty();
            for event in stream {
                self.check_cancelled()?;
                match event? {
                    QueryEvent::Schema(value) => {
                        schema = value;
                        if !schema.fields.is_empty() {
                            write_row_description(&mut self.stream, &schema, &[])?;
                        }
                    }
                    QueryEvent::Batch(batch) => {
                        for row in &batch.rows {
                            self.check_cancelled()?;
                            write_data_row(&mut self.stream, &schema, row, &[])?;
                        }
                    }
                    QueryEvent::Progress(progress) => {
                        self.registry
                            .update_query_rows(&query_id, progress.rows_processed)?;
                    }
                    QueryEvent::Notice(notice) => {
                        write_notice(&mut self.stream, &notice)?;
                    }
                    QueryEvent::Complete(complete) => {
                        write_command_complete(&mut self.stream, &command_tag(&complete))?;
                    }
                }
            }
            Ok(())
        })();
        self.finish_registered_query(&query_id, &result)?;
        result
    }

    fn parse(&mut self, name: String, sql: String, parameter_oids: Vec<u32>) -> Result<()> {
        ensure_prepared_statement_slot(&self.prepared, &name, self.config.max_prepared_statements)?;
        if sql.len() > self.config.max_frame_bytes {
            return Err(protocol("prepared SQL exceeds frame limit"));
        }
        let description = self.statement_description(&sql)?;
        let parameter_oids = resolve_parameter_oids(&parameter_oids, &description.parameter_types)?;
        if name.is_empty() {
            self.retire_portals_for_statement("")?;
        }
        self.prepared.insert(
            name,
            PreparedStatement {
                sql,
                parameter_oids,
                parameter_types: description.parameter_types,
                schema: description.schema,
            },
        );
        write_parse_complete(&mut self.stream)
    }

    fn bind(
        &mut self,
        portal_name: String,
        statement_name: &str,
        parameter_formats: &[i16],
        parameters: &[Option<Vec<u8>>],
        result_formats: Vec<i16>,
    ) -> Result<()> {
        ensure_portal_slot(&self.portals, &portal_name, self.config.max_portals)?;
        let prepared = self
            .prepared
            .get(statement_name)
            .ok_or_else(|| DbError::new("26000", "prepared statement does not exist"))?
            .clone();
        let parameters = decode_parameters_as(
            &prepared.parameter_oids,
            &prepared.parameter_types,
            parameter_formats,
            parameters,
        )?;
        let replaced = self.portals.insert(
            portal_name,
            Portal {
                statement_name: statement_name.to_owned(),
                sql: prepared.sql,
                parameters,
                result_formats,
                stream: None,
                schema: Some(prepared.schema),
                pending_rows: VecDeque::new(),
                completed: false,
                query_id: None,
                rows_processed: 0,
            },
        );
        if let Some(portal) = replaced {
            retire_portal(&self.registry, portal, QueryOutcome::Cancelled)?;
        }
        write_bind_complete(&mut self.stream)
    }

    fn describe(&mut self, kind: u8, name: &str) -> Result<()> {
        match kind {
            b'S' => {
                let statement = self
                    .prepared
                    .get(name)
                    .ok_or_else(|| DbError::new("26000", "prepared statement does not exist"))?;
                write_parameter_description(&mut self.stream, &statement.parameter_oids)?;
                if statement.schema.fields.is_empty() {
                    write_no_data(&mut self.stream)
                } else {
                    write_row_description(&mut self.stream, &statement.schema, &[])
                }
            }
            b'P' => {
                let portal = self
                    .portals
                    .get(name)
                    .ok_or_else(|| DbError::new("34000", "portal does not exist"))?;
                match &portal.schema {
                    Some(schema) if !schema.fields.is_empty() => {
                        write_row_description(&mut self.stream, schema, &portal.result_formats)
                    }
                    _ => write_no_data(&mut self.stream),
                }
            }
            _ => Err(protocol("Describe kind must be S or P")),
        }
    }

    fn execute_portal(&mut self, name: &str, max_rows: u32) -> Result<()> {
        let mut portal = self
            .portals
            .remove(name)
            .ok_or_else(|| DbError::new("34000", "portal does not exist"))?;
        let result = self.execute_portal_inner(&mut portal, max_rows);
        self.portals.insert(name.to_owned(), portal);
        result
    }

    fn execute_portal_inner(&mut self, portal: &mut Portal, max_rows: u32) -> Result<()> {
        if portal.completed {
            return write_command_complete(&mut self.stream, "SELECT 0");
        }
        if portal.stream.is_none() {
            self.authorize(&portal.sql)?;
            self.registry.reset_cancellation(self.handle.process_id())?;
            let query_id = Uuid::new_v4().to_string();
            self.registry.begin_query(
                self.handle.process_id(),
                query_id.clone(),
                redacted_security_sql(&portal.sql),
            )?;
            portal.query_id = Some(query_id);
            portal.stream = Some(self.statement_stream(&portal.sql, &portal.parameters)?);
        }

        let unlimited = max_rows == 0;
        let mut emitted = 0_u32;
        loop {
            while let Some(row) = portal.pending_rows.pop_front() {
                self.check_cancelled()?;
                let schema = portal
                    .schema
                    .as_ref()
                    .ok_or_else(|| DbError::new("XX000", "portal row has no schema"))?;
                write_data_row(&mut self.stream, schema, &row, &portal.result_formats)?;
                emitted = emitted.saturating_add(1);
                if !unlimited && emitted >= max_rows {
                    write_portal_suspended(&mut self.stream)?;
                    return Ok(());
                }
            }

            let next = portal.stream.as_mut().and_then(|stream| stream.next());
            let Some(event) = next else {
                if let Some(query_id) = &portal.query_id {
                    self.registry.finish_query(query_id, QueryOutcome::Error)?;
                }
                return Err(DbError::new(
                    "XX000",
                    "portal stream ended without a completion event",
                )
                .with_hint("close the portal and retry the statement"));
            };
            match event {
                Ok(QueryEvent::Schema(schema)) => {
                    if let Some(described) = &portal.schema
                        && described != &schema
                    {
                        return Err(DbError::new(
                            "XX000",
                            "portal execution schema changed after Describe",
                        ));
                    }
                    portal.schema = Some(schema);
                }
                Ok(QueryEvent::Batch(batch)) => {
                    portal.pending_rows.extend(batch.rows);
                }
                Ok(QueryEvent::Progress(progress)) => {
                    portal.rows_processed = progress.rows_processed;
                    if let Some(query_id) = &portal.query_id {
                        self.registry
                            .update_query_rows(query_id, progress.rows_processed)?;
                    }
                }
                Ok(QueryEvent::Notice(notice)) => {
                    write_notice(&mut self.stream, &notice)?;
                }
                Ok(QueryEvent::Complete(complete)) => {
                    write_command_complete(&mut self.stream, &command_tag(&complete))?;
                    if let Some(query_id) = &portal.query_id {
                        self.registry
                            .finish_query(query_id, QueryOutcome::Complete)?;
                    }
                    portal.completed = true;
                    portal.stream = None;
                    return Ok(());
                }
                Err(error) => {
                    if let Some(query_id) = &portal.query_id {
                        let outcome = if error.sql_state == "57014" {
                            QueryOutcome::Cancelled
                        } else {
                            QueryOutcome::Error
                        };
                        self.registry.finish_query(query_id, outcome)?;
                    }
                    return Err(error);
                }
            }
        }
    }

    fn close(&mut self, kind: u8, name: &str) -> Result<()> {
        match kind {
            b'S' => {
                self.prepared
                    .remove(name)
                    .ok_or_else(|| DbError::new("26000", "prepared statement does not exist"))?;
                self.retire_portals_for_statement(name)?;
            }
            b'P' => {
                let portal = self
                    .portals
                    .remove(name)
                    .ok_or_else(|| DbError::new("34000", "portal does not exist"))?;
                retire_portal(&self.registry, portal, QueryOutcome::Cancelled)?;
            }
            _ => return Err(protocol("Close kind must be S or P")),
        }
        write_close_complete(&mut self.stream)
    }

    fn retire_portals_for_statement(&mut self, statement_name: &str) -> Result<()> {
        let names = self
            .portals
            .iter()
            .filter_map(|(name, portal)| {
                (portal.statement_name == statement_name).then_some(name.clone())
            })
            .collect::<Vec<_>>();
        for name in names {
            if let Some(portal) = self.portals.remove(&name) {
                retire_portal(&self.registry, portal, QueryOutcome::Cancelled)?;
            }
        }
        Ok(())
    }

    fn statement_stream(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>>>> {
        refresh_system_catalog_metadata(
            &mut self.session,
            &self.auth,
            &self.settings,
            &self.principal,
            &self.database,
        )?;
        if let Some(statement) = parse_security_statement(sql)? {
            if !parameters.is_empty() {
                return Err(DbError::new(
                    "0A000",
                    "security DDL does not support protocol parameters",
                ));
            }
            if !matches!(self.session.transaction_status(), TransactionStatus::Idle) {
                return Err(DbError::new(
                    "25001",
                    "security DDL must execute outside a transaction",
                ));
            }
            let tag = execute_security_statement(&self.auth, &mut self.principal, statement)?;
            let events = vec![
                QueryEvent::Schema(Schema::empty()),
                QueryEvent::Progress(QueryProgress { rows_processed: 0 }),
                QueryEvent::Complete(CommandComplete {
                    tag: tag.to_owned(),
                    rows_affected: 0,
                }),
            ];
            return Ok(Box::new(events.into_iter().map(Ok)));
        }
        if !matches!(self.session.transaction_status(), TransactionStatus::Failed)
            && let Some(events) = session_setting_events(sql, &mut self.settings)?
        {
            self.refresh_runtime_metadata()?;
            refresh_system_catalog_metadata(
                &mut self.session,
                &self.auth,
                &self.settings,
                &self.principal,
                &self.database,
            )?;
            return Ok(Box::new(events.into_iter().map(Ok)));
        }
        Ok(Box::new(self.session.execute_stream_with_cancellation(
            sql,
            parameters,
            self.handle.cancellation_flag(),
        )?))
    }

    fn statement_description(&mut self, sql: &str) -> Result<StatementDescription> {
        if matches!(self.session.transaction_status(), TransactionStatus::Failed) {
            return self.session.describe_statement(sql);
        }
        if parse_security_statement(sql)?.is_some() {
            return Ok(StatementDescription {
                schema: Schema::empty(),
                parameter_types: Vec::new(),
            });
        }
        if let Some(description) = session_setting_description(sql, &self.settings)? {
            return Ok(description);
        }
        self.session.describe_statement(sql)
    }

    fn refresh_runtime_metadata(&mut self) -> Result<()> {
        self.session.set_runtime_metadata(session_runtime_metadata(
            &self.settings,
            &self.database,
            &self.principal,
        )?);
        Ok(())
    }

    fn authorize(&self, sql: &str) -> Result<()> {
        let authorizer = Authorizer::from_store(&self.auth)?;
        if is_security_sql(sql) {
            authorizer.authorize(&self.principal, Action::Manage, &DbObject::Server)
        } else {
            authorizer.authorize_sql(&self.principal, &self.database, sql)
        }
    }

    fn check_cancelled(&self) -> Result<()> {
        if self
            .shutdown
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(DbError::new("57P01", "server is shutting down"));
        }
        if self.handle.is_cancelled() {
            return Err(DbError::new("57014", "query cancelled"));
        }
        Ok(())
    }

    fn finish_registered_query(&self, query_id: &str, result: &Result<()>) -> Result<()> {
        let outcome = match result {
            Ok(()) => QueryOutcome::Complete,
            Err(error) if error.sql_state == "57014" => QueryOutcome::Cancelled,
            Err(_) => QueryOutcome::Error,
        };
        self.registry.finish_query(query_id, outcome)
    }

    fn flush(&mut self) -> Result<()> {
        self.stream
            .flush()
            .map_err(|error| io_error("failed to flush PostgreSQL connection", error))
    }

    fn execute_copy(&mut self, copy: CopyCommand) -> Result<()> {
        match copy.direction {
            CopyDirection::ToStdout => self.copy_to_stdout(&copy),
            CopyDirection::FromStdin => self.copy_from_stdin(&copy),
        }
    }

    fn copy_to_stdout(&mut self, copy: &CopyCommand) -> Result<()> {
        let projection = if copy.columns.is_empty() {
            "*".to_owned()
        } else {
            copy.columns.join(", ")
        };
        let sql = format!("SELECT {projection} FROM {}", copy.table);
        self.authorize(&sql)?;
        let mut stream = self.session.execute_stream(&sql, &[])?;
        let schema = match stream.next() {
            Some(Ok(QueryEvent::Schema(schema))) if !schema.fields.is_empty() => schema,
            Some(Ok(_)) => {
                return Err(DbError::new(
                    "XX000",
                    "COPY source did not begin with a non-empty schema",
                ));
            }
            Some(Err(error)) => return Err(error),
            None => {
                return Err(DbError::new("XX000", "COPY source ended before its schema"));
            }
        };
        write_copy_response(&mut self.stream, b'H', schema.fields.len())?;
        if copy.options.header {
            let header = encode_copy_header(&schema, &copy.options)?;
            write_message(&mut self.stream, b'd', &header)?;
        }
        let handle = &self.handle;
        let shutdown = self.shutdown.as_ref();
        let rows = write_copy_stream(&mut self.stream, &schema, &copy.options, stream, || {
            if shutdown.is_some_and(CancellationToken::is_cancelled) {
                return Err(DbError::new("57P01", "server is shutting down"));
            }
            if handle.is_cancelled() {
                return Err(DbError::new("57014", "query cancelled"));
            }
            Ok(())
        })?;
        write_message(&mut self.stream, b'c', &[])?;
        write_command_complete(&mut self.stream, &format!("COPY {rows}"))
    }

    fn copy_from_stdin(&mut self, copy: &CopyCommand) -> Result<()> {
        self.authorize(&format!("COPY {} FROM STDIN", copy.table))?;
        let columns = copy_columns(&self.engine, &copy.table, &copy.columns)?;
        let insert = insert_statement(&copy.table, &columns);
        let owns_transaction = begin_copy_transaction(&mut self.session)?;
        write_copy_response(&mut self.stream, b'G', columns.len())?;
        self.flush()?;
        let mut bytes = Vec::new();
        let receive = loop {
            let Some(message) = read_frontend(&mut self.stream, self.config.max_frame_bytes)?
            else {
                break Err(DbError::new("08006", "connection closed during COPY IN"));
            };
            match message {
                FrontendMessage::CopyData(chunk) => {
                    let next = bytes
                        .len()
                        .checked_add(chunk.len())
                        .ok_or_else(|| DbError::new("54000", "COPY input length overflowed"))?;
                    if next > self.config.max_copy_bytes {
                        break Err(DbError::new(
                            "54000",
                            "COPY input exceeds the configured limit",
                        ));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                FrontendMessage::CopyDone => break Ok(()),
                FrontendMessage::CopyFail(message) => {
                    break Err(DbError::new("57014", "COPY aborted by client").with_detail(message));
                }
                FrontendMessage::Flush => self.flush()?,
                _ => break Err(protocol("only COPY data/done/fail is valid during COPY IN")),
            }
        };
        if let Err(error) = receive {
            abort_copy_transaction(&mut self.session, owns_transaction);
            return Err(error);
        }
        let rows = match import_copy(&mut self.session, &insert, &columns, &copy.options, &bytes) {
            Ok(rows) => rows,
            Err(error) => {
                abort_copy_transaction(&mut self.session, owns_transaction);
                return Err(error);
            }
        };
        complete_copy_transaction(&mut self.session, owns_transaction)?;
        write_command_complete(&mut self.stream, &format!("COPY {rows}"))
    }
}

fn write_copy_stream<W, I, F>(
    writer: &mut W,
    schema: &Schema,
    options: &CopyOptions,
    stream: I,
    mut check_cancelled: F,
) -> Result<u64>
where
    W: Write,
    I: IntoIterator<Item = Result<QueryEvent>>,
    F: FnMut() -> Result<()>,
{
    let mut rows = 0_u64;
    let mut completed = false;
    for event in stream {
        check_cancelled()?;
        if completed {
            return Err(DbError::new(
                "XX000",
                "COPY source emitted an event after completion",
            ));
        }
        match event? {
            QueryEvent::Schema(_) => {
                return Err(DbError::new(
                    "XX000",
                    "COPY source emitted more than one schema",
                ));
            }
            QueryEvent::Batch(batch) => {
                for row in &batch.rows {
                    let encoded = encode_copy_row(schema, row, options)?;
                    write_message(writer, b'd', &encoded)?;
                    rows = rows.saturating_add(1);
                }
            }
            QueryEvent::Notice(notice) => write_notice(writer, &notice)?,
            QueryEvent::Progress(_) => {}
            QueryEvent::Complete(_) => completed = true,
        }
    }
    if !completed {
        return Err(DbError::new(
            "XX000",
            "COPY source ended without a completion event",
        ));
    }
    Ok(rows)
}

fn begin_copy_transaction(session: &mut Session) -> Result<bool> {
    match session.transaction_status() {
        TransactionStatus::Idle => {
            drain(session.execute_stream("BEGIN", &[])?)?;
            Ok(true)
        }
        TransactionStatus::Active => Ok(false),
        TransactionStatus::Failed => Err(DbError::new(
            "25P02",
            "the current transaction is aborted; commands are ignored until ROLLBACK",
        )),
    }
}

fn complete_copy_transaction(session: &mut Session, owns_transaction: bool) -> Result<()> {
    if owns_transaction {
        drain(session.execute_stream("COMMIT", &[])?)?;
    }
    Ok(())
}

fn abort_copy_transaction(session: &mut Session, owns_transaction: bool) {
    if owns_transaction {
        if let Ok(stream) = session.execute_stream("ROLLBACK", &[]) {
            let _ = drain(stream);
        }
    } else {
        session.mark_transaction_failed();
    }
}

fn drain(stream: impl Iterator<Item = Result<QueryEvent>>) -> Result<()> {
    for event in stream {
        event?;
    }
    Ok(())
}

fn transaction_status(session: &Session) -> u8 {
    match session.transaction_status() {
        TransactionStatus::Idle => b'I',
        TransactionStatus::Active => b'T',
        TransactionStatus::Failed => b'E',
    }
}

fn session_setting_events(
    sql: &str,
    settings: &mut PgSessionSettings,
) -> Result<Option<Vec<QueryEvent>>> {
    let Some(statement) = parse_setting_statement(sql)? else {
        return Ok(None);
    };
    match statement {
        PgSettingStatement::Show { name } => {
            let value = settings.get(&name).ok_or_else(|| {
                DbError::new(
                    "42704",
                    format!("unrecognized configuration parameter {name}"),
                )
            })?;
            let schema = Schema::new(vec![Field::new(&name, ScalarType::Text, false)]);
            Ok(Some(result_events(
                schema,
                vec![Row::new(vec![Value::Text(value.to_owned())])],
                "SHOW",
            )))
        }
        PgSettingStatement::Set { name, value } => {
            settings.set(&name, &value)?;
            Ok(Some(command_events("SET", 0)))
        }
        PgSettingStatement::SetConfig {
            name,
            value,
            is_local,
            result_name,
        } => {
            if is_local {
                return Err(DbError::new(
                    "0A000",
                    "transaction-local set_config settings are not supported yet",
                ));
            }
            settings.set(&name, &value)?;
            let value = settings.get(&name).ok_or_else(|| {
                DbError::internal("set_config updated a setting that cannot be read back")
            })?;
            let schema = Schema::new(vec![Field::new(result_name, ScalarType::Text, false)]);
            Ok(Some(result_events(
                schema,
                vec![Row::new(vec![Value::Text(value.to_owned())])],
                "SELECT 1",
            )))
        }
        PgSettingStatement::Reset { name } => {
            settings.reset(&name)?;
            Ok(Some(command_events("RESET", 0)))
        }
        PgSettingStatement::ResetAll => {
            settings.reset_all();
            Ok(Some(command_events("RESET", 0)))
        }
    }
}

fn session_setting_description(
    sql: &str,
    settings: &PgSessionSettings,
) -> Result<Option<StatementDescription>> {
    let Some(statement) = parse_setting_statement(sql)? else {
        return Ok(None);
    };
    let schema = match statement {
        PgSettingStatement::Show { name } => {
            if settings.get(&name).is_none() {
                return Err(DbError::new(
                    "42704",
                    format!("unrecognized configuration parameter {name}"),
                ));
            }
            Schema::new(vec![Field::new(name, ScalarType::Text, false)])
        }
        PgSettingStatement::Set { .. }
        | PgSettingStatement::Reset { .. }
        | PgSettingStatement::ResetAll => Schema::empty(),
        PgSettingStatement::SetConfig { result_name, .. } => {
            Schema::new(vec![Field::new(result_name, ScalarType::Text, false)])
        }
    };
    Ok(Some(StatementDescription {
        schema,
        parameter_types: Vec::new(),
    }))
}

fn connect_postgresql_session(
    engine: &Engine,
    principal: &Principal,
    bypass_ownership: bool,
) -> Result<Session> {
    engine.connect_authenticated(SessionAuthorization::new(
        principal.user.clone(),
        bypass_ownership,
    )?)
}

fn session_runtime_metadata(
    settings: &PgSessionSettings,
    database: &str,
    principal: &Principal,
) -> Result<SessionRuntimeMetadata> {
    let server_version = settings
        .get("server_version")
        .ok_or_else(|| DbError::internal("PostgreSQL session has no server_version setting"))?;
    SessionRuntimeMetadata::postgres_compatible(
        server_version,
        database,
        principal.user.as_str(),
        principal.user.as_str(),
    )?
    .with_settings(settings.runtime_values())
}

fn refresh_system_catalog_metadata(
    session: &mut Session,
    auth: &AuthStore,
    settings: &PgSessionSettings,
    principal: &Principal,
    database: &str,
) -> Result<()> {
    let roles = auth
        .safe_role_metadata_snapshot()?
        .roles
        .into_iter()
        .map(|role| CatalogRoleMetadata {
            postgres_oid: role.postgres_oid,
            name: role.name,
            can_login: role.can_login,
            login_enabled: role.login_enabled,
        })
        .collect();
    let authorizer = Authorizer::from_store(auth)?;
    let visibility = CatalogVisibility::from_scopes(
        authorizer
            .discovery_objects(principal)?
            .into_iter()
            .filter_map(|object| catalog_visibility_scope(object, database)),
    )?;
    session.refresh_system_catalog_metadata(roles, settings.system_catalog_metadata(), visibility)
}

fn catalog_visibility_scope(object: DbObject, database: &str) -> Option<CatalogVisibilityScope> {
    match object {
        DbObject::Server => Some(CatalogVisibilityScope::All),
        DbObject::Database(name) => name
            .eq_ignore_ascii_case(database)
            .then_some(CatalogVisibilityScope::All),
        DbObject::Schema(name) => {
            let parts = name.split('.').collect::<Vec<_>>();
            match parts.as_slice() {
                [schema] => Some(CatalogVisibilityScope::Schema {
                    schema: (*schema).to_owned(),
                }),
                [scope_database, schema] if scope_database.eq_ignore_ascii_case(database) => {
                    Some(CatalogVisibilityScope::Schema {
                        schema: (*schema).to_owned(),
                    })
                }
                _ => None,
            }
        }
        DbObject::Table(name) | DbObject::Sequence(name) | DbObject::Function(name) => {
            let parts = name.split('.').collect::<Vec<_>>();
            match parts.as_slice() {
                [name] => Some(CatalogVisibilityScope::Object {
                    schema: "public".to_owned(),
                    name: (*name).to_owned(),
                }),
                [schema, name] => Some(CatalogVisibilityScope::Object {
                    schema: (*schema).to_owned(),
                    name: (*name).to_owned(),
                }),
                [scope_database, schema, name] if scope_database.eq_ignore_ascii_case(database) => {
                    Some(CatalogVisibilityScope::Object {
                        schema: (*schema).to_owned(),
                        name: (*name).to_owned(),
                    })
                }
                _ => None,
            }
        }
    }
}

fn command_tag(complete: &CommandComplete) -> String {
    let upper = complete.tag.to_ascii_uppercase();
    if upper == "INSERT" {
        format!("INSERT 0 {}", complete.rows_affected)
    } else if matches!(upper.as_str(), "SELECT" | "UPDATE" | "DELETE") {
        format!("{upper} {}", complete.rows_affected)
    } else {
        complete.tag.clone()
    }
}

fn result_events(schema: Schema, rows: Vec<Row>, tag: &str) -> Vec<QueryEvent> {
    let rows_processed = u64::try_from(rows.len()).unwrap_or(u64::MAX);
    let mut events = vec![QueryEvent::Schema(schema.clone())];
    if !rows.is_empty() {
        events.push(QueryEvent::Batch(Batch { schema, rows }));
    }
    events.push(QueryEvent::Progress(QueryProgress { rows_processed }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: tag.into(),
        rows_affected: rows_processed,
    }));
    events
}

fn command_events(tag: &str, rows_affected: u64) -> Vec<QueryEvent> {
    vec![
        QueryEvent::Schema(Schema::empty()),
        QueryEvent::Progress(QueryProgress {
            rows_processed: rows_affected,
        }),
        QueryEvent::Complete(CommandComplete {
            tag: tag.into(),
            rows_affected,
        }),
    ]
}

fn split_statements(sql: &str) -> Result<Vec<String>> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let characters: Vec<char> = sql.chars().collect();
    let mut single_quote = false;
    let mut double_quote = false;
    let mut dollar_quote: Option<Vec<char>> = None;
    let mut index = 0;
    while index < characters.len() {
        if let Some(delimiter) = dollar_quote.as_ref() {
            if characters[index..].starts_with(delimiter) {
                current.extend(delimiter.iter().copied());
                index += delimiter.len();
                dollar_quote = None;
            } else {
                current.push(characters[index]);
                index += 1;
            }
            continue;
        }
        let character = characters[index];
        match character {
            '\'' if !double_quote => {
                current.push(character);
                if single_quote && characters.get(index + 1) == Some(&'\'') {
                    current.push('\'');
                    index += 2;
                    continue;
                }
                let preceding_backslashes = current
                    .chars()
                    .rev()
                    .skip(1)
                    .take_while(|value| *value == '\\')
                    .count();
                if preceding_backslashes % 2 == 0 {
                    single_quote = !single_quote;
                }
                index += 1;
                continue;
            }
            '"' if !single_quote => {
                current.push(character);
                if double_quote && characters.get(index + 1) == Some(&'"') {
                    current.push('"');
                    index += 2;
                    continue;
                }
                double_quote = !double_quote;
                index += 1;
                continue;
            }
            '$' if !single_quote && !double_quote => {
                if let Some(delimiter) = dollar_quote_delimiter(&characters, index) {
                    current.extend(delimiter.iter().copied());
                    index += delimiter.len();
                    dollar_quote = Some(delimiter);
                    continue;
                }
            }
            ';' if !single_quote && !double_quote => {
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_owned());
                }
                current.clear();
                index += 1;
                continue;
            }
            _ => {}
        }
        current.push(character);
        index += 1;
    }
    if single_quote || double_quote {
        return Err(DbError::new("42601", "unterminated SQL quote"));
    }
    if dollar_quote.is_some() {
        return Err(DbError::new(
            "42601",
            "unterminated dollar-quoted SQL string",
        ));
    }
    if !current.trim().is_empty() {
        statements.push(current.trim().to_owned());
    }
    Ok(statements)
}

fn dollar_quote_delimiter(characters: &[char], start: usize) -> Option<Vec<char>> {
    if characters.get(start) != Some(&'$') {
        return None;
    }
    let mut end = start + 1;
    while let Some(character) = characters.get(end) {
        if *character == '$' {
            let tag = &characters[start + 1..end];
            if tag
                .first()
                .is_some_and(|value| !(value.is_ascii_alphabetic() || *value == '_'))
                || !tag
                    .iter()
                    .all(|value| value.is_ascii_alphanumeric() || *value == '_')
            {
                return None;
            }
            return Some(characters[start..=end].to_vec());
        }
        if !(character.is_ascii_alphanumeric() || *character == '_') {
            return None;
        }
        end += 1;
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyDirection {
    ToStdout,
    FromStdin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyFormat {
    Text,
    Csv,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyOptions {
    format: CopyFormat,
    delimiter: u8,
    null: String,
    header: bool,
    quote: u8,
    escape: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyCommand {
    table: String,
    columns: Vec<String>,
    direction: CopyDirection,
    options: CopyOptions,
}

fn parse_copy(sql: &str) -> Result<Option<CopyCommand>> {
    let trimmed = sql.trim_start();
    let first_word_end = trimmed
        .bytes()
        .position(|byte| !byte.is_ascii_alphabetic())
        .unwrap_or(trimmed.len());
    if !trimmed[..first_word_end].eq_ignore_ascii_case("COPY") {
        return Ok(None);
    }
    CopyParser::new(lex_copy(trimmed)?).parse().map(Some)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CopyToken {
    Word(String),
    String(String),
    LeftParen,
    RightParen,
    Comma,
    Equals,
}

fn lex_copy(sql: &str) -> Result<Vec<CopyToken>> {
    const MAX_COPY_TOKENS: usize = 256;
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() {
            index += 1;
            continue;
        }
        let token = match bytes[index] {
            b'(' => {
                index += 1;
                CopyToken::LeftParen
            }
            b')' => {
                index += 1;
                CopyToken::RightParen
            }
            b',' => {
                index += 1;
                CopyToken::Comma
            }
            b'=' => {
                index += 1;
                CopyToken::Equals
            }
            b'\'' => {
                index += 1;
                let mut value = String::new();
                loop {
                    let Some(&byte) = bytes.get(index) else {
                        return Err(DbError::new("42601", "unterminated COPY string literal"));
                    };
                    if byte == b'\'' {
                        if bytes.get(index + 1) == Some(&b'\'') {
                            value.push('\'');
                            index += 2;
                            continue;
                        }
                        index += 1;
                        break;
                    }
                    let rest = std::str::from_utf8(&bytes[index..])
                        .map_err(|_| DbError::new("22021", "COPY command is not valid UTF-8"))?;
                    let character = rest
                        .chars()
                        .next()
                        .ok_or_else(|| DbError::new("22021", "COPY command is not valid UTF-8"))?;
                    value.push(character);
                    index += character.len_utf8();
                }
                CopyToken::String(value)
            }
            b'"' => {
                return Err(copy_unsupported(
                    "quoted COPY table and column names are not supported",
                ));
            }
            byte if byte.is_ascii_alphanumeric() || byte == b'_' => {
                let start = index;
                index += 1;
                while bytes.get(index).is_some_and(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b'+')
                }) {
                    index += 1;
                }
                CopyToken::Word(sql[start..index].to_owned())
            }
            _ => {
                return Err(DbError::new(
                    "42601",
                    "COPY command contains an unsupported token",
                ));
            }
        };
        tokens.push(token);
        if tokens.len() > MAX_COPY_TOKENS {
            return Err(DbError::new("54000", "COPY command has too many tokens"));
        }
    }
    Ok(tokens)
}

struct CopyParser {
    tokens: Vec<CopyToken>,
    index: usize,
}

impl CopyParser {
    const fn new(tokens: Vec<CopyToken>) -> Self {
        Self { tokens, index: 0 }
    }

    fn parse(mut self) -> Result<CopyCommand> {
        self.expect_keyword("COPY")?;
        if self.consume_keyword("BINARY") {
            return Err(copy_unsupported("COPY BINARY is not supported"));
        }
        if self.peek_left_paren() {
            return Err(copy_unsupported("COPY query sources are not supported"));
        }
        let table = self.take_word("COPY requires a table name")?;
        validate_copy_identifier_path(&table, "table")?;
        let columns = self.parse_columns()?;
        let direction = if self.consume_keyword("TO") {
            self.expect_target("STDOUT", CopyDirection::ToStdout)?
        } else if self.consume_keyword("FROM") {
            self.expect_target("STDIN", CopyDirection::FromStdin)?
        } else {
            return Err(copy_unsupported("COPY requires TO STDOUT or FROM STDIN"));
        };
        let options = self.parse_options()?;
        if self.index != self.tokens.len() {
            return Err(DbError::new("42601", "COPY command has trailing tokens"));
        }
        Ok(CopyCommand {
            table,
            columns,
            direction,
            options,
        })
    }

    fn parse_columns(&mut self) -> Result<Vec<String>> {
        if !self.consume_left_paren() {
            return Ok(Vec::new());
        }
        let mut columns = Vec::new();
        loop {
            let column = self.take_word("COPY column list requires a column name")?;
            validate_copy_identifier_path(&column, "column")?;
            if column.contains('.') {
                return Err(copy_unsupported("COPY column names cannot be qualified"));
            }
            if columns
                .iter()
                .any(|existing: &String| existing.eq_ignore_ascii_case(&column))
            {
                return Err(DbError::new(
                    "42701",
                    format!("COPY column {column} is specified more than once"),
                ));
            }
            columns.push(column);
            if self.consume_right_paren() {
                return Ok(columns);
            }
            if !self.consume_comma() {
                return Err(DbError::new("42601", "COPY column list requires a comma"));
            }
        }
    }

    fn expect_target(&mut self, expected: &str, direction: CopyDirection) -> Result<CopyDirection> {
        if self.consume_keyword(expected) {
            return Ok(direction);
        }
        if self.consume_keyword("PROGRAM") {
            return Err(copy_unsupported("COPY PROGRAM is not supported"));
        }
        if matches!(self.tokens.get(self.index), Some(CopyToken::String(_))) {
            return Err(copy_unsupported("server-side COPY files are not supported"));
        }
        Err(copy_unsupported(format!("COPY requires {expected}")))
    }

    fn parse_options(&mut self) -> Result<CopyOptions> {
        let _ = self.consume_keyword("WITH");
        if self.index == self.tokens.len() {
            return Ok(default_copy_options(CopyFormat::Text));
        }
        let parenthesized = self.consume_left_paren();
        let mut format = None;
        let mut delimiter = None;
        let mut null = None;
        let mut header = None;
        let mut quote = None;
        let mut escape = None;
        let mut seen = std::collections::BTreeSet::new();
        loop {
            if parenthesized && self.consume_right_paren() {
                break;
            }
            if self.index == self.tokens.len() {
                if parenthesized {
                    return Err(DbError::new("42601", "unterminated COPY option list"));
                }
                break;
            }
            let name = self
                .take_word("COPY option name is required")?
                .to_ascii_lowercase();
            if !seen.insert(name.clone()) {
                return Err(DbError::new(
                    "42601",
                    format!("COPY option {name} is specified more than once"),
                ));
            }
            self.consume_equals();
            match name.as_str() {
                "format" => format = Some(self.parse_format()?),
                "text" => format = Some(CopyFormat::Text),
                "csv" => format = Some(CopyFormat::Csv),
                "delimiter" => delimiter = Some(self.take_single_byte("DELIMITER")?),
                "null" => null = Some(self.take_string("NULL")?),
                "header" => header = Some(self.take_optional_boolean()?.unwrap_or(true)),
                "quote" => quote = Some(self.take_single_byte("QUOTE")?),
                "escape" => escape = Some(self.take_single_byte("ESCAPE")?),
                "encoding" => {
                    let value = self.take_value("ENCODING")?;
                    if !matches!(value.to_ascii_lowercase().as_str(), "utf8" | "utf-8") {
                        return Err(copy_unsupported("COPY supports only UTF8 encoding"));
                    }
                }
                "binary" => {
                    return Err(copy_unsupported("COPY FORMAT BINARY is not supported"));
                }
                _ => {
                    return Err(copy_unsupported(format!(
                        "COPY option {name} is not supported"
                    )));
                }
            }
            if parenthesized {
                if self.consume_comma() {
                    continue;
                }
                if self.consume_right_paren() {
                    break;
                }
                return Err(DbError::new(
                    "42601",
                    "COPY options require a comma or closing parenthesis",
                ));
            }
            self.consume_comma();
        }

        let format = format.unwrap_or(CopyFormat::Text);
        let mut options = default_copy_options(format);
        if let Some(value) = delimiter {
            options.delimiter = value;
        }
        if let Some(value) = null {
            options.null = value;
        }
        if let Some(value) = header {
            options.header = value;
        }
        if let Some(value) = quote {
            options.quote = value;
        }
        if let Some(value) = escape {
            options.escape = value;
        }
        if format == CopyFormat::Text && (options.header || quote.is_some() || escape.is_some()) {
            return Err(DbError::new(
                "22023",
                "COPY HEADER, QUOTE and ESCAPE require FORMAT CSV",
            ));
        }
        if matches!(options.delimiter, 0 | b'\r' | b'\n' | b'\\') {
            return Err(DbError::new("22023", "COPY delimiter is not valid"));
        }
        if format == CopyFormat::Csv && options.delimiter == options.quote {
            return Err(DbError::new(
                "22023",
                "COPY delimiter and quote must be different",
            ));
        }
        if format == CopyFormat::Csv
            && (matches!(options.quote, 0 | b'\r' | b'\n')
                || matches!(options.escape, 0 | b'\r' | b'\n'))
        {
            return Err(DbError::new("22023", "COPY quote or escape is not valid"));
        }
        if options.null.contains(['\r', '\n'])
            || options.null.as_bytes().contains(&options.delimiter)
            || format == CopyFormat::Csv && options.null.as_bytes().contains(&options.quote)
        {
            return Err(DbError::new(
                "22023",
                "COPY NULL marker conflicts with the selected format",
            ));
        }
        Ok(options)
    }

    fn parse_format(&mut self) -> Result<CopyFormat> {
        let value = self.take_word("COPY FORMAT requires TEXT or CSV")?;
        match value.to_ascii_lowercase().as_str() {
            "text" => Ok(CopyFormat::Text),
            "csv" => Ok(CopyFormat::Csv),
            "binary" => Err(copy_unsupported("COPY FORMAT BINARY is not supported")),
            _ => Err(DbError::new("22023", "COPY FORMAT must be TEXT or CSV")),
        }
    }

    fn take_single_byte(&mut self, option: &str) -> Result<u8> {
        let value = self.take_string(option)?;
        let [byte] = value.as_bytes() else {
            return Err(DbError::new(
                "22023",
                format!("COPY {option} must be exactly one single-byte character"),
            ));
        };
        Ok(*byte)
    }

    fn take_string(&mut self, option: &str) -> Result<String> {
        let Some(CopyToken::String(value)) = self.tokens.get(self.index) else {
            return Err(DbError::new(
                "42601",
                format!("COPY {option} requires a string literal"),
            ));
        };
        self.index += 1;
        Ok(value.clone())
    }

    fn take_value(&mut self, option: &str) -> Result<String> {
        match self.tokens.get(self.index) {
            Some(CopyToken::Word(value)) | Some(CopyToken::String(value)) => {
                self.index += 1;
                Ok(value.clone())
            }
            _ => Err(DbError::new(
                "42601",
                format!("COPY {option} requires a value"),
            )),
        }
    }

    fn take_optional_boolean(&mut self) -> Result<Option<bool>> {
        let Some(CopyToken::Word(value)) = self.tokens.get(self.index) else {
            return Ok(None);
        };
        let value = value.to_ascii_lowercase();
        let result = match value.as_str() {
            "true" | "on" | "1" => true,
            "false" | "off" | "0" => false,
            _ => return Ok(None),
        };
        self.index += 1;
        Ok(Some(result))
    }

    fn take_word(&mut self, message: &str) -> Result<String> {
        let Some(CopyToken::Word(value)) = self.tokens.get(self.index) else {
            return Err(DbError::new("42601", message));
        };
        self.index += 1;
        Ok(value.clone())
    }

    fn expect_keyword(&mut self, expected: &str) -> Result<()> {
        if self.consume_keyword(expected) {
            Ok(())
        } else {
            Err(DbError::new(
                "42601",
                format!("COPY expected keyword {expected}"),
            ))
        }
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        let Some(CopyToken::Word(value)) = self.tokens.get(self.index) else {
            return false;
        };
        if value.eq_ignore_ascii_case(expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn peek_left_paren(&self) -> bool {
        matches!(self.tokens.get(self.index), Some(CopyToken::LeftParen))
    }

    fn consume_left_paren(&mut self) -> bool {
        consume_copy_token(&self.tokens, &mut self.index, &CopyToken::LeftParen)
    }

    fn consume_right_paren(&mut self) -> bool {
        consume_copy_token(&self.tokens, &mut self.index, &CopyToken::RightParen)
    }

    fn consume_comma(&mut self) -> bool {
        consume_copy_token(&self.tokens, &mut self.index, &CopyToken::Comma)
    }

    fn consume_equals(&mut self) -> bool {
        consume_copy_token(&self.tokens, &mut self.index, &CopyToken::Equals)
    }
}

fn consume_copy_token(tokens: &[CopyToken], index: &mut usize, expected: &CopyToken) -> bool {
    if tokens.get(*index) == Some(expected) {
        *index += 1;
        true
    } else {
        false
    }
}

fn default_copy_options(format: CopyFormat) -> CopyOptions {
    CopyOptions {
        format,
        delimiter: match format {
            CopyFormat::Text => b'\t',
            CopyFormat::Csv => b',',
        },
        null: match format {
            CopyFormat::Text => "\\N".to_owned(),
            CopyFormat::Csv => String::new(),
        },
        header: false,
        quote: b'"',
        escape: b'"',
    }
}

fn validate_copy_identifier_path(value: &str, label: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        });
    if valid {
        Ok(())
    } else {
        Err(copy_unsupported(format!(
            "COPY {label} must be an unquoted identifier"
        )))
    }
}

fn copy_unsupported(message: impl Into<String>) -> DbError {
    DbError::new("0A000", message)
}

fn write_copy_response<W: Write>(writer: &mut W, tag: u8, columns: usize) -> Result<()> {
    let columns = i16::try_from(columns).map_err(|_| protocol("COPY column count exceeds i16"))?;
    let mut payload = vec![0];
    payload.extend_from_slice(&columns.to_be_bytes());
    for _ in 0..columns {
        payload.extend_from_slice(&0_i16.to_be_bytes());
    }
    write_message(writer, tag, &payload)
}

fn encode_copy_header(schema: &Schema, options: &CopyOptions) -> Result<Vec<u8>> {
    if options.format != CopyFormat::Csv {
        return Err(DbError::internal(
            "COPY text header passed option validation",
        ));
    }
    encode_csv_record(
        schema
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>(),
        options,
    )
}

fn encode_copy_row(schema: &Schema, row: &Row, options: &CopyOptions) -> Result<Vec<u8>> {
    if schema.fields.len() != row.values.len() {
        return Err(DbError::new(
            "XX000",
            "COPY row width does not match schema",
        ));
    }
    match options.format {
        CopyFormat::Text => encode_text_copy_row(row, options),
        CopyFormat::Csv => encode_csv_copy_row(row, options),
    }
}

fn encode_csv_copy_row(row: &Row, options: &CopyOptions) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    for (index, value) in row.values.iter().enumerate() {
        if index > 0 {
            encoded.push(options.delimiter);
        }
        if matches!(value, Value::Null) {
            append_csv_field(&mut encoded, options.null.as_bytes(), true, options);
        } else {
            append_csv_field(&mut encoded, &encode_text(value)?, false, options);
        }
    }
    encoded.push(b'\n');
    Ok(encoded)
}

fn encode_csv_record(fields: Vec<String>, options: &CopyOptions) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            encoded.push(options.delimiter);
        }
        append_csv_field(&mut encoded, field.as_bytes(), false, options);
    }
    encoded.push(b'\n');
    Ok(encoded)
}

fn append_csv_field(encoded: &mut Vec<u8>, field: &[u8], null: bool, options: &CopyOptions) {
    if null {
        encoded.extend_from_slice(field);
        return;
    }
    let quote = field == options.null.as_bytes()
        || field.iter().any(|byte| {
            matches!(*byte, b'\r' | b'\n') || *byte == options.delimiter || *byte == options.quote
        });
    if !quote {
        encoded.extend_from_slice(field);
        return;
    }
    encoded.push(options.quote);
    for &byte in field {
        if byte == options.quote {
            encoded.push(options.escape);
        }
        encoded.push(byte);
    }
    encoded.push(options.quote);
}

fn encode_text_copy_row(row: &Row, options: &CopyOptions) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    for (index, value) in row.values.iter().enumerate() {
        if index > 0 {
            encoded.push(options.delimiter);
        }
        if matches!(value, Value::Null) {
            encoded.extend_from_slice(options.null.as_bytes());
            continue;
        }
        for byte in encode_text(value)? {
            match byte {
                b'\\' => encoded.extend_from_slice(b"\\\\"),
                b'\n' => encoded.extend_from_slice(b"\\n"),
                b'\r' => encoded.extend_from_slice(b"\\r"),
                b'\t' => encoded.extend_from_slice(b"\\t"),
                b'\x08' => encoded.extend_from_slice(b"\\b"),
                b'\x0c' => encoded.extend_from_slice(b"\\f"),
                b'\x0b' => encoded.extend_from_slice(b"\\v"),
                value if value == options.delimiter => {
                    encoded.push(b'\\');
                    encoded.push(value);
                }
                value => encoded.push(value),
            }
        }
    }
    encoded.push(b'\n');
    Ok(encoded)
}

fn copy_columns(
    engine: &Engine,
    table: &str,
    requested: &[String],
) -> Result<Vec<ColumnDefinition>> {
    let (schema, table) = table
        .split_once('.')
        .map_or(("public", table), |(schema, table)| (schema, table));
    let catalog = engine.catalog_snapshot()?;
    let table = catalog
        .table(&Identifier::unquoted(schema), &Identifier::unquoted(table))
        .ok_or_else(|| DbError::new("42P01", "COPY table does not exist"))?;
    if requested.is_empty() {
        return Ok(table.columns().to_vec());
    }
    requested
        .iter()
        .map(|name| {
            table
                .column(&Identifier::unquoted(name))
                .cloned()
                .ok_or_else(|| DbError::new("42703", format!("COPY column {name} does not exist")))
        })
        .collect()
}

fn insert_statement(table: &str, columns: &[ColumnDefinition]) -> String {
    let names = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let parameters = (1..=columns.len())
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} ({names}) VALUES ({parameters})")
}

fn import_copy(
    session: &mut Session,
    insert: &str,
    columns: &[ColumnDefinition],
    options: &CopyOptions,
    bytes: &[u8],
) -> Result<u64> {
    match options.format {
        CopyFormat::Text => import_text(session, insert, columns, options, bytes),
        CopyFormat::Csv => import_csv(session, insert, columns, options, bytes),
    }
}

fn import_csv(
    session: &mut Session,
    insert: &str,
    columns: &[ColumnDefinition],
    options: &CopyOptions,
    bytes: &[u8],
) -> Result<u64> {
    let records = decode_csv_records(bytes, options)?;
    let mut records = records.into_iter();
    if options.header {
        let header = records
            .next()
            .ok_or_else(|| DbError::new("22P04", "COPY CSV header is missing"))?;
        let expected = columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let actual = header
            .iter()
            .map(|field| std::str::from_utf8(&field.value))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                DbError::new("22021", "COPY CSV header is not valid UTF-8")
                    .with_detail(error.to_string())
            })?;
        if actual != expected {
            return Err(DbError::new(
                "22P04",
                "COPY CSV header does not match the target columns",
            ));
        }
    }
    let mut rows = 0_u64;
    for record in records {
        let raw = record
            .into_iter()
            .map(|field| {
                if !field.quoted && field.value == options.null.as_bytes() {
                    None
                } else {
                    Some(field.value)
                }
            })
            .collect::<Vec<_>>();
        insert_copy_row(session, insert, columns, raw)?;
        rows = checked_copy_row_count(rows)?;
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedCsvField {
    value: Vec<u8>,
    quoted: bool,
}

fn decode_csv_records(bytes: &[u8], options: &CopyOptions) -> Result<Vec<Vec<DecodedCsvField>>> {
    let mut records = Vec::new();
    let mut record = Vec::new();
    let mut field = Vec::new();
    let mut quoted = false;
    let mut in_quotes = false;
    let mut after_quote = false;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if in_quotes {
            if byte == options.quote {
                if options.escape == options.quote && bytes.get(index + 1) == Some(&options.quote) {
                    field.push(options.quote);
                    index += 2;
                } else {
                    in_quotes = false;
                    after_quote = true;
                    index += 1;
                }
            } else if options.escape != options.quote && byte == options.escape {
                let Some(&escaped) = bytes.get(index + 1) else {
                    return Err(DbError::new("22P04", "COPY CSV ends with an escape byte"));
                };
                if escaped == options.quote || escaped == options.escape {
                    field.push(escaped);
                    index += 2;
                } else {
                    field.push(byte);
                    index += 1;
                }
            } else {
                field.push(byte);
                index += 1;
            }
            continue;
        }

        if after_quote {
            if byte == options.delimiter {
                push_csv_field(&mut record, &mut field, &mut quoted);
                after_quote = false;
                index += 1;
                continue;
            }
            if matches!(byte, b'\r' | b'\n') {
                push_csv_field(&mut record, &mut field, &mut quoted);
                records.push(std::mem::take(&mut record));
                after_quote = false;
                index = skip_csv_record_end(bytes, index);
                continue;
            }
            return Err(DbError::new(
                "22P04",
                "COPY CSV has data after a closing quote",
            ));
        }

        if field.is_empty() && byte == options.quote {
            quoted = true;
            in_quotes = true;
            index += 1;
        } else if byte == options.delimiter {
            push_csv_field(&mut record, &mut field, &mut quoted);
            index += 1;
        } else if matches!(byte, b'\r' | b'\n') {
            push_csv_field(&mut record, &mut field, &mut quoted);
            records.push(std::mem::take(&mut record));
            index = skip_csv_record_end(bytes, index);
        } else if byte == options.quote {
            return Err(DbError::new(
                "22P04",
                "COPY CSV quote appears inside an unquoted field",
            ));
        } else {
            field.push(byte);
            index += 1;
        }
    }
    if in_quotes {
        return Err(DbError::new(
            "22P04",
            "COPY CSV has an unterminated quoted field",
        ));
    }
    if after_quote || !field.is_empty() || quoted || !record.is_empty() {
        push_csv_field(&mut record, &mut field, &mut quoted);
        records.push(record);
    }
    Ok(records)
}

fn push_csv_field(record: &mut Vec<DecodedCsvField>, field: &mut Vec<u8>, quoted: &mut bool) {
    record.push(DecodedCsvField {
        value: std::mem::take(field),
        quoted: *quoted,
    });
    *quoted = false;
}

fn skip_csv_record_end(bytes: &[u8], index: usize) -> usize {
    if bytes[index] == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
        index + 2
    } else {
        index + 1
    }
}

fn import_text(
    session: &mut Session,
    insert: &str,
    columns: &[ColumnDefinition],
    options: &CopyOptions,
    bytes: &[u8],
) -> Result<u64> {
    let mut rows = 0_u64;
    let mut start = 0;
    for end in (0..=bytes.len()).filter(|index| *index == bytes.len() || bytes[*index] == b'\n') {
        if end == bytes.len() && start == end {
            break;
        }
        let mut record = &bytes[start..end];
        if record.ends_with(b"\r") {
            record = &record[..record.len() - 1];
        }
        let raw = decode_text_record(record, options)?;
        insert_copy_row(session, insert, columns, raw)?;
        rows = checked_copy_row_count(rows)?;
        start = end.saturating_add(1);
    }
    Ok(rows)
}

fn decode_text_record(record: &[u8], options: &CopyOptions) -> Result<Vec<Option<Vec<u8>>>> {
    let mut fields = Vec::new();
    let mut field = Vec::new();
    let mut escaped = false;
    for &byte in record {
        if escaped {
            field.push(byte);
            escaped = false;
        } else if byte == b'\\' {
            field.push(byte);
            escaped = true;
        } else if byte == options.delimiter {
            fields.push(decode_text_field(&field, options)?);
            field.clear();
        } else {
            field.push(byte);
        }
    }
    if escaped {
        return Err(DbError::new("22P04", "COPY text row ends with a backslash"));
    }
    fields.push(decode_text_field(&field, options)?);
    Ok(fields)
}

fn decode_text_field(field: &[u8], options: &CopyOptions) -> Result<Option<Vec<u8>>> {
    if field == options.null.as_bytes() {
        return Ok(None);
    }
    let mut decoded = Vec::with_capacity(field.len());
    let mut index = 0;
    while index < field.len() {
        if field[index] != b'\\' {
            decoded.push(field[index]);
            index += 1;
            continue;
        }
        let Some(&escaped) = field.get(index + 1) else {
            return Err(DbError::new(
                "22P04",
                "COPY text field ends with a backslash",
            ));
        };
        decoded.push(match escaped {
            b'b' => b'\x08',
            b'f' => b'\x0c',
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'v' => b'\x0b',
            value => value,
        });
        index += 2;
    }
    Ok(Some(decoded))
}

fn insert_copy_row(
    session: &mut Session,
    insert: &str,
    columns: &[ColumnDefinition],
    raw: Vec<Option<Vec<u8>>>,
) -> Result<()> {
    if raw.len() != columns.len() {
        return Err(DbError::new(
            "22P04",
            format!(
                "COPY row has {} fields but target has {} columns",
                raw.len(),
                columns.len()
            ),
        ));
    }
    let data_types = columns
        .iter()
        .map(|column| column.data_type.clone())
        .collect::<Vec<_>>();
    let oids = data_types.iter().map(type_oid).collect::<Vec<_>>();
    let values = decode_parameters_as(&oids, &data_types, &[], &raw)?;
    drain(session.execute_stream(insert, &values)?)
}

fn checked_copy_row_count(rows: u64) -> Result<u64> {
    rows.checked_add(1)
        .ok_or_else(|| DbError::new("54000", "COPY row count overflowed"))
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn prepared_parameter_oids_fill_unknowns_and_reject_conflicts() {
        assert_eq!(
            resolve_parameter_oids(&[], &[ScalarType::Int64, ScalarType::Text]).expect("infer all"),
            [crate::value::OID_INT8, crate::value::OID_TEXT]
        );
        assert_eq!(
            resolve_parameter_oids(
                &[0, crate::value::OID_TEXT],
                &[ScalarType::Int64, ScalarType::Text],
            )
            .expect("fill unknown"),
            [crate::value::OID_INT8, crate::value::OID_TEXT]
        );
        assert_eq!(
            resolve_parameter_oids(&[crate::value::OID_INT4], &[ScalarType::Int64])
                .expect("safe widening"),
            [crate::value::OID_INT4]
        );

        let enum_type = ScalarType::Enum {
            type_id: ordadb_types::TypeId::new(11),
            labels: vec!["draft".into(), "published".into()],
        };
        let enum_oid = type_oid(&enum_type);
        let described = resolve_parameter_oids(&[], std::slice::from_ref(&enum_type))
            .expect("describe enum parameter");
        assert_eq!(described, [enum_oid]);
        assert_eq!(
            decode_parameters_as(
                &described,
                std::slice::from_ref(&enum_type),
                &[1],
                &[Some(b"published".to_vec())],
            )
            .expect("execute described enum parameter"),
            [Value::Text("published".into())]
        );

        let mismatch = resolve_parameter_oids(&[crate::value::OID_TEXT], &[ScalarType::Int64])
            .expect_err("mismatched declaration");
        assert_eq!(mismatch.sql_state, "42804");

        let count = resolve_parameter_oids(
            &[crate::value::OID_INT8],
            &[ScalarType::Int64, ScalarType::Text],
        )
        .expect_err("mismatched count");
        assert_eq!(count.sql_state, "08P01");
    }

    #[test]
    fn extended_query_state_waits_for_sync_and_ignores_other_messages() {
        assert_eq!(
            failed_message_action(&FrontendMessage::Sync),
            FailedMessageAction::Synchronize
        );
        assert_eq!(
            failed_message_action(&FrontendMessage::Flush),
            FailedMessageAction::Flush
        );
        assert_eq!(
            failed_message_action(&FrontendMessage::Terminate),
            FailedMessageAction::Terminate
        );
        assert_eq!(
            failed_message_action(&FrontendMessage::Parse {
                name: "ignored".into(),
                sql: "SELECT 1".into(),
                parameter_oids: Vec::new(),
            }),
            FailedMessageAction::Ignore
        );
        assert_eq!(
            failed_message_action(&FrontendMessage::Query("SELECT 1".into())),
            FailedMessageAction::Ignore
        );
    }

    #[test]
    fn named_extended_objects_require_close_while_unnamed_objects_replace() {
        let statement = PreparedStatement {
            sql: "SELECT 1".into(),
            parameter_oids: Vec::new(),
            parameter_types: Vec::new(),
            schema: Schema::empty(),
        };
        let mut prepared = BTreeMap::new();
        prepared.insert(String::new(), statement.clone());
        ensure_prepared_statement_slot(&prepared, "", 1).expect("replace unnamed statement");
        assert_eq!(
            ensure_prepared_statement_slot(&prepared, "named", 1)
                .expect_err("statement limit")
                .sql_state,
            "54000"
        );
        prepared.insert("named".into(), statement);
        assert_eq!(
            ensure_prepared_statement_slot(&prepared, "named", 3)
                .expect_err("named statement requires close")
                .sql_state,
            "42P05"
        );

        let portal = || Portal {
            statement_name: String::new(),
            sql: "SELECT 1".into(),
            parameters: Vec::new(),
            result_formats: Vec::new(),
            stream: None,
            schema: Some(Schema::empty()),
            pending_rows: VecDeque::new(),
            completed: false,
            query_id: None,
            rows_processed: 0,
        };
        let mut portals = BTreeMap::new();
        portals.insert(String::new(), portal());
        ensure_portal_slot(&portals, "", 1).expect("replace unnamed portal");
        assert_eq!(
            ensure_portal_slot(&portals, "named", 1)
                .expect_err("portal limit")
                .sql_state,
            "54000"
        );
        portals.insert("named".into(), portal());
        assert_eq!(
            ensure_portal_slot(&portals, "named", 3)
                .expect_err("named portal requires close")
                .sql_state,
            "42P03"
        );
    }

    #[test]
    fn retiring_an_active_portal_finishes_its_registered_query() {
        let registry = SessionRegistry::default();
        let handle = registry
            .register_session("user".into(), "db".into(), None, "local".into(), 17)
            .expect("register session");
        registry
            .begin_query(
                handle.process_id(),
                "portal-query".into(),
                "SELECT 1".into(),
            )
            .expect("begin query");
        retire_portal(
            &registry,
            Portal {
                statement_name: String::new(),
                sql: "SELECT 1".into(),
                parameters: Vec::new(),
                result_formats: Vec::new(),
                stream: None,
                schema: Some(Schema::empty()),
                pending_rows: VecDeque::new(),
                completed: false,
                query_id: Some("portal-query".into()),
                rows_processed: 0,
            },
            QueryOutcome::Cancelled,
        )
        .expect("retire portal");
        assert_eq!(registry.active_query_count().expect("active count"), 0);
        assert!(
            registry
                .queries()
                .expect("query history")
                .iter()
                .any(|query| query.query_id == "portal-query"
                    && matches!(query.outcome, QueryOutcome::Cancelled))
        );
    }

    #[test]
    fn simple_query_splitter_respects_quotes_and_copy_is_explicit() {
        assert_eq!(
            split_statements("SELECT ';'; SELECT 1").expect("split"),
            vec!["SELECT ';'", "SELECT 1"]
        );
        assert_eq!(
            split_statements("SELECT 'it''s;still one'; SELECT \"a\"\";b\" FROM items")
                .expect("doubled quotes"),
            vec!["SELECT 'it''s;still one'", "SELECT \"a\"\";b\" FROM items"]
        );
        assert_eq!(
            split_statements(r"SELECT E'escaped\\'; SELECT 2").expect("even backslashes"),
            vec![r"SELECT E'escaped\\'", "SELECT 2"]
        );
        assert_eq!(
            split_statements(
                "CREATE PROCEDURE p() AS $body$
                 BEGIN
                 PERFORM ';';
                 END;
                 $body$ LANGUAGE plpgsql;
                 SELECT $1"
            )
            .expect("dollar quote"),
            vec![
                "CREATE PROCEDURE p() AS $body$
                 BEGIN
                 PERFORM ';';
                 END;
                 $body$ LANGUAGE plpgsql",
                "SELECT $1",
            ]
        );
        assert_eq!(
            split_statements("SELECT $$semi;colon$$; SELECT 2").expect("empty dollar tag"),
            vec!["SELECT $$semi;colon$$", "SELECT 2"]
        );
        assert_eq!(
            split_statements("SELECT $body$missing")
                .expect_err("unterminated dollar quote")
                .sql_state,
            "42601"
        );
        let copy = parse_copy("COPY public.items TO STDOUT")
            .expect("copy")
            .expect("command");
        assert!(matches!(copy.direction, CopyDirection::ToStdout));
        assert_eq!(copy.options, default_copy_options(CopyFormat::Text));
    }

    #[test]
    fn copy_grammar_supports_columns_and_typed_csv_options() {
        let copy = parse_copy(
            "COPY public.items (id, title) FROM STDIN \
             WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL 'NULL', QUOTE '\"')",
        )
        .expect("parse COPY")
        .expect("COPY command");
        assert_eq!(copy.table, "public.items");
        assert_eq!(copy.columns, ["id", "title"]);
        assert_eq!(copy.direction, CopyDirection::FromStdin);
        assert_eq!(copy.options.format, CopyFormat::Csv);
        assert_eq!(copy.options.delimiter, b';');
        assert_eq!(copy.options.null, "NULL");
        assert!(copy.options.header);
        assert_eq!(copy.options.quote, b'"');

        for sql in [
            "COPY items TO 'file.csv'",
            "COPY items FROM PROGRAM 'generate'",
            "COPY items TO STDOUT WITH (FORMAT binary)",
            "COPY BINARY items TO STDOUT",
        ] {
            assert_eq!(
                parse_copy(sql)
                    .expect_err("unsupported COPY form")
                    .sql_state,
                "0A000",
                "{sql}"
            );
        }
        assert_eq!(
            parse_copy("COPY items (id, ID) FROM STDIN")
                .expect_err("duplicate COPY column")
                .sql_state,
            "42701"
        );
        assert_eq!(
            parse_copy("COPY items TO STDOUT WITH (HEADER)")
                .expect_err("header requires CSV")
                .sql_state,
            "22023"
        );
    }

    #[test]
    fn copy_text_codec_escapes_delimiters_nulls_and_newlines() {
        let options = default_copy_options(CopyFormat::Text);
        let schema = Schema::new(vec![
            Field::new("first", ScalarType::Text, false),
            Field::new("second", ScalarType::Text, true),
        ]);
        let original = b"tab\tbackslash\\newline\n".to_vec();
        let encoded = encode_copy_row(
            &schema,
            &Row::new(vec![
                Value::Text(String::from_utf8(original.clone()).unwrap()),
                Value::Null,
            ]),
            &options,
        )
        .expect("encode COPY text");
        assert!(encoded.ends_with(b"\n"));
        let decoded =
            decode_text_record(&encoded[..encoded.len() - 1], &options).expect("decode COPY text");
        assert_eq!(decoded, vec![Some(original), None]);
    }

    #[test]
    fn copy_out_requires_exactly_one_terminal_completion_event() {
        let schema = Schema::new(vec![Field::new("value", ScalarType::Text, false)]);
        let options = default_copy_options(CopyFormat::Text);
        let batch = QueryEvent::Batch(Batch {
            schema: schema.clone(),
            rows: vec![Row::new(vec![Value::Text("row".into())])],
        });
        let complete = QueryEvent::Complete(CommandComplete {
            tag: "SELECT".into(),
            rows_affected: 1,
        });

        let mut encoded = Vec::new();
        assert_eq!(
            write_copy_stream(
                &mut encoded,
                &schema,
                &options,
                [Ok(batch.clone()), Ok(complete.clone())],
                || Ok(()),
            )
            .expect("complete COPY stream"),
            1
        );
        assert!(!encoded.is_empty());

        let missing = write_copy_stream(
            &mut Vec::new(),
            &schema,
            &options,
            [Ok(batch.clone())],
            || Ok(()),
        )
        .expect_err("missing completion");
        assert_eq!(missing.sql_state, "XX000");

        let duplicate = write_copy_stream(
            &mut Vec::new(),
            &schema,
            &options,
            [Ok(batch), Ok(complete.clone()), Ok(complete)],
            || Ok(()),
        )
        .expect_err("duplicate completion");
        assert_eq!(duplicate.sql_state, "XX000");
    }

    #[test]
    fn copy_in_uses_and_preserves_an_existing_transaction_boundary() {
        let directory = tempfile::tempdir().expect("tempdir");
        let engine =
            Engine::open(ordadb_engine::EngineConfig::new(directory.path())).expect("open engine");
        let mut session = engine.connect().expect("connect");
        drain(
            session
                .execute_stream("CREATE TABLE copy_tx (id BIGINT, label TEXT)", &[])
                .expect("create table"),
        )
        .expect("drain create table");
        let columns = copy_columns(&engine, "copy_tx", &[]).expect("COPY columns");
        let insert = insert_statement("copy_tx", &columns);
        let options = default_copy_options(CopyFormat::Text);

        drain(session.execute_stream("BEGIN", &[]).expect("begin")).expect("drain begin");
        let owns_transaction = begin_copy_transaction(&mut session).expect("reuse transaction");
        assert!(!owns_transaction);
        assert_eq!(
            import_copy(&mut session, &insert, &columns, &options, b"1\touter\n",)
                .expect("import COPY row"),
            1
        );
        complete_copy_transaction(&mut session, owns_transaction).expect("finish COPY");
        assert_eq!(session.transaction_status(), TransactionStatus::Active);
        drain(session.execute_stream("ROLLBACK", &[]).expect("rollback")).expect("drain rollback");
        let rows = session
            .execute_stream("SELECT id FROM copy_tx", &[])
            .expect("select after rollback")
            .collect::<Result<Vec<_>>>()
            .expect("drain select");
        assert_eq!(
            rows.iter()
                .filter_map(|event| match event {
                    QueryEvent::Batch(batch) => Some(batch.rows.len()),
                    _ => None,
                })
                .sum::<usize>(),
            0
        );

        drain(session.execute_stream("BEGIN", &[]).expect("second begin"))
            .expect("drain second begin");
        let owns_transaction = begin_copy_transaction(&mut session).expect("reuse transaction");
        let error = import_copy(
            &mut session,
            &insert,
            &columns,
            &options,
            b"2\tvalid\n3\ttoo\tmany\n",
        )
        .expect_err("malformed COPY input");
        assert_eq!(error.sql_state, "22P04");
        abort_copy_transaction(&mut session, owns_transaction);
        assert_eq!(session.transaction_status(), TransactionStatus::Failed);
        drain(
            session
                .execute_stream("ROLLBACK", &[])
                .expect("failed rollback"),
        )
        .expect("drain failed rollback");
    }

    #[test]
    fn copy_csv_codec_distinguishes_null_from_quoted_empty_text() {
        let options = default_copy_options(CopyFormat::Csv);
        let schema = Schema::new(vec![
            Field::new("nullable", ScalarType::Text, true),
            Field::new("empty", ScalarType::Text, false),
        ]);
        let encoded = encode_copy_row(
            &schema,
            &Row::new(vec![Value::Null, Value::Text(String::new())]),
            &options,
        )
        .expect("encode COPY CSV");
        assert_eq!(encoded, b",\"\"\n");
        let records = decode_csv_records(&encoded, &options).expect("decode COPY CSV");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0][0].value, b"");
        assert!(!records[0][0].quoted);
        assert_eq!(records[0][1].value, b"");
        assert!(records[0][1].quoted);

        let multiline = decode_csv_records(b"\"line 1\nline 2\",value\r\n", &options)
            .expect("decode multiline COPY CSV");
        assert_eq!(multiline[0][0].value, b"line 1\nline 2");
        assert!(multiline[0][0].quoted);
    }

    #[test]
    fn copy_import_honors_columns_csv_header_and_transaction_rollback() {
        let directory = tempfile::tempdir().expect("tempdir");
        let engine =
            Engine::open(ordadb_engine::EngineConfig::new(directory.path())).expect("open engine");
        let mut session = engine.connect().expect("connect session");
        drain(
            session
                .execute_stream(
                    "CREATE TABLE items (\
                     id BIGINT PRIMARY KEY, title TEXT NOT NULL, score INTEGER DEFAULT 7)",
                    &[],
                )
                .expect("create table"),
        )
        .expect("drain create table");
        let requested = vec!["id".to_owned(), "title".to_owned()];
        let columns = copy_columns(&engine, "items", &requested).expect("COPY columns");
        let insert = insert_statement("items", &columns);
        let mut csv = default_copy_options(CopyFormat::Csv);
        csv.header = true;
        csv.delimiter = b';';

        drain(session.execute_stream("BEGIN", &[]).expect("begin import")).expect("drain begin");
        assert_eq!(
            import_copy(
                &mut session,
                &insert,
                &columns,
                &csv,
                b"id;title\n1;first\n2;second\n",
            )
            .expect("import CSV"),
            2
        );
        drain(
            session
                .execute_stream("COMMIT", &[])
                .expect("commit import"),
        )
        .expect("drain commit");

        drain(session.execute_stream("BEGIN", &[]).expect("begin failure"))
            .expect("drain begin failure");
        let error = import_copy(
            &mut session,
            &insert,
            &columns,
            &default_copy_options(CopyFormat::Text),
            b"3\tthird\nnot-an-id\tbroken\n",
        )
        .expect_err("invalid COPY row");
        assert_eq!(error.sql_state, "22P02");
        drain(
            session
                .execute_stream("ROLLBACK", &[])
                .expect("rollback failed COPY"),
        )
        .expect("drain rollback");

        let rows = session
            .execute("SELECT id, title, score FROM items ORDER BY id", &[])
            .expect("query imported rows")
            .filter_map(|event| match event {
                QueryEvent::Batch(batch) => Some(batch.rows),
                _ => None,
            })
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                Row::new(vec![
                    Value::Int64(1),
                    Value::Text("first".into()),
                    Value::Int32(7),
                ]),
                Row::new(vec![
                    Value::Int64(2),
                    Value::Text("second".into()),
                    Value::Int32(7),
                ]),
            ]
        );
    }

    #[test]
    fn server_limits_reject_zero_or_tiny_values() {
        let mut config = PgServerConfig {
            max_frame_bytes: 4,
            ..PgServerConfig::default()
        };
        assert_eq!(config.validate().expect_err("frame").sql_state, "22023");
        config.max_frame_bytes = DEFAULT_MAX_FRAME_BYTES;
        config.max_portals = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn startup_encryption_negotiation_is_ordered_and_non_repeating() {
        let mut negotiation = StartupNegotiation::default();
        negotiation
            .record(EncryptionRequest::Gss)
            .expect("GSS preference probe");
        negotiation
            .record(EncryptionRequest::Ssl)
            .expect("TLS probe after GSS rejection");
        assert_eq!(
            negotiation
                .record(EncryptionRequest::Ssl)
                .expect_err("repeated SSLRequest")
                .sql_state,
            "08P01"
        );

        let mut out_of_order = StartupNegotiation::default();
        out_of_order
            .record(EncryptionRequest::Ssl)
            .expect("initial SSLRequest");
        assert_eq!(
            out_of_order
                .record(EncryptionRequest::Gss)
                .expect_err("GSS request after SSL")
                .sql_state,
            "08P01"
        );
    }

    #[test]
    fn session_compatibility_functions_remain_bounded_without_catalog_interception() {
        let directory = tempfile::tempdir().expect("tempdir");
        let engine =
            Engine::open(ordadb_engine::EngineConfig::new(directory.path())).expect("open engine");
        let mut session = engine.connect().expect("connect session");
        session.set_runtime_metadata(
            SessionRuntimeMetadata::postgres_compatible("18.0", "ordadb", "dba", "dba")
                .expect("runtime metadata"),
        );

        for (sql, field_name, expected) in [
            (
                "SELECT version()",
                "version",
                Value::Text("PostgreSQL 18.0 compatible OrdaDB on x86_64-pc-windows-msvc".into()),
            ),
            (
                "SELECT current_database()",
                "current_database",
                Value::Text("ordadb".into()),
            ),
            (
                "SELECT CURRENT_USER",
                "current_user",
                Value::Text("dba".into()),
            ),
            (
                "SELECT SESSION_USER",
                "session_user",
                Value::Text("dba".into()),
            ),
            (
                "SELECT current_setting('client_encoding')",
                "current_setting",
                Value::Text("UTF8".into()),
            ),
            ("SELECT 1", "?column?", Value::Int32(1)),
        ] {
            let events = session
                .execute(sql, &[])
                .unwrap_or_else(|error| panic!("{sql}: {error}"))
                .collect::<Vec<_>>();
            let QueryEvent::Schema(schema) = &events[0] else {
                panic!("{sql}: schema event");
            };
            assert_eq!(schema.fields[0].name, field_name);
            let value = events.iter().find_map(|event| match event {
                QueryEvent::Batch(batch) => batch.rows.first()?.values.first(),
                _ => None,
            });
            assert_eq!(value, Some(&expected), "{sql}");
            assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        }

        let settings_events = session
            .execute(
                "SELECT current_setting('client_encoding'), \
                 current_setting('standard_conforming_strings')",
                &[],
            )
            .expect("multi-setting query")
            .collect::<Vec<_>>();
        let values = settings_events.iter().find_map(|event| match event {
            QueryEvent::Batch(batch) => batch.rows.first().map(|row| row.values.clone()),
            _ => None,
        });
        assert_eq!(
            values,
            Some(vec![Value::Text("UTF8".into()), Value::Text("on".into())])
        );

        let catalog_events = session
            .execute("SELECT relname FROM pg_catalog.pg_class LIMIT 1", &[])
            .expect("system catalog query")
            .collect::<Vec<_>>();
        assert!(matches!(
            catalog_events.first(),
            Some(QueryEvent::Schema(_))
        ));
    }

    #[test]
    fn session_settings_describe_without_mutation_and_apply_on_execution() {
        let mut settings = PgSessionSettings::from_startup(
            "18.0 (OrdaDB test)".to_owned(),
            "dba",
            &BTreeMap::new(),
        )
        .expect("settings");
        let description =
            session_setting_description("SET application_name TO 'DataGrip'", &settings)
                .expect("describe")
                .expect("session statement");
        assert!(description.schema.fields.is_empty());
        assert_eq!(settings.get("application_name"), Some(""));

        let events = session_setting_events("SET application_name TO 'DataGrip'", &mut settings)
            .expect("execute")
            .expect("session statement");
        assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        assert_eq!(settings.get("application_name"), Some("DataGrip"));

        let events = session_setting_events("SHOW application_name", &mut settings)
            .expect("show")
            .expect("session statement");
        assert!(matches!(events.first(), Some(QueryEvent::Schema(_))));

        let description = session_setting_description(
            "SELECT set_config('application_name', 'DescribeOnly', false)",
            &settings,
        )
        .expect("describe set_config")
        .expect("set_config statement");
        assert_eq!(description.schema.fields[0].name, "set_config");
        assert_eq!(settings.get("application_name"), Some("DataGrip"));

        let events = session_setting_events(
            "SELECT set_config('application_name', 'pgjdbc', false)",
            &mut settings,
        )
        .expect("execute set_config")
        .expect("set_config statement");
        assert!(events.iter().any(|event| matches!(
            event,
            QueryEvent::Batch(batch)
                if batch.rows == [Row::new(vec![Value::Text("pgjdbc".into())])]
        )));
        let error = session_setting_events(
            "SELECT set_config('application_name', 'local', true)",
            &mut settings,
        )
        .expect_err("local set_config rejected");
        assert_eq!(error.sql_state, "0A000");
        assert_eq!(settings.get("application_name"), Some("pgjdbc"));

        let directory = tempfile::tempdir().expect("tempdir");
        let engine =
            Engine::open(ordadb_engine::EngineConfig::new(directory.path())).expect("open engine");
        let mut session = engine.connect().expect("connect session");
        let principal = Principal {
            user: "dba".into(),
            roles: BTreeSet::new(),
        };
        session.set_runtime_metadata(
            session_runtime_metadata(&settings, "ordadb", &principal)
                .expect("refreshed runtime metadata"),
        );
        let values = session
            .execute("SELECT current_setting('application_name')", &[])
            .expect("read changed setting")
            .find_map(|event| match event {
                QueryEvent::Batch(batch) => batch.rows.into_iter().next().map(|row| row.values),
                _ => None,
            });
        assert_eq!(values, Some(vec![Value::Text("pgjdbc".into())]));
    }

    #[test]
    fn pgwire_sessions_keep_the_default_postgresql_dialect() {
        let directory = tempfile::tempdir().expect("tempdir");
        let engine =
            Engine::open(ordadb_engine::EngineConfig::new(directory.path())).expect("open engine");
        let principal = Principal {
            user: "dba".into(),
            roles: BTreeSet::new(),
        };
        let mut session =
            connect_postgresql_session(&engine, &principal, false).expect("connect session");
        assert_eq!(
            session.options(),
            ordadb_engine::SessionOptions::default(),
            "PostgreSQL Wire must not negotiate a non-PostgreSQL dialect"
        );
        let events = session
            .execute("CREATE SCHEMA wire_owned", &[])
            .expect("create owned schema")
            .collect::<Vec<_>>();
        assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        let catalog = engine.catalog_snapshot().expect("catalog");
        let schema = catalog
            .schema(&Identifier::unquoted("wire_owned"))
            .expect("wire-owned schema");
        assert_eq!(
            catalog
                .owner_of(ordadb_catalog::CatalogObjectRef::Schema(schema.id))
                .map(ordadb_catalog::CatalogOwner::as_str),
            Some(principal.user.as_str())
        );
    }
}
