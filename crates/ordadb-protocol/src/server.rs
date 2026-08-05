use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use csv::{ReaderBuilder, WriterBuilder};
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
use ordadb_catalog::{
    Catalog, ColumnDefinition, ConstraintKind, RoutineKind, UserDefinedTypeKind, ViewKind,
};
use ordadb_engine::{
    Engine, Session, SessionAuthorization, StatementDescription, TransactionStatus,
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
use crate::value::{
    decode_parameters_as, encode_text, type_oid, user_defined_array_oid, user_defined_type_oid,
    write_data_row, write_row_description,
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
            server_version: format!("16.0 (OrdaDB {})", env!("CARGO_PKG_VERSION")),
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
    write_startup_responses(&mut stream, &config, &principal.user, &handle)?;

    let session = connect_postgresql_session(&engine, &principal, bypass_ownership)?;
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
        prepared: BTreeMap::new(),
        portals: BTreeMap::new(),
        extended_failed: false,
        shutdown: connection_shutdown,
    };
    connection.run()
}

fn negotiate_startup(
    mut stream: InterruptibleTcpStream,
    tls: Option<Arc<RustlsServerConfig>>,
    max_frame_bytes: usize,
) -> Result<(ConnectionStream, StartupPacket)> {
    loop {
        match read_startup(&mut stream, max_frame_bytes)? {
            StartupPacket::SslRequest => match tls {
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
                    if matches!(startup, StartupPacket::SslRequest) {
                        return Err(protocol("nested SSLRequest is invalid"));
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
            },
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
    config: &PgServerConfig,
    user: &str,
    handle: &CancellationHandle,
) -> Result<()> {
    for (name, value) in [
        ("server_version", config.server_version.as_str()),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, YMD"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("session_authorization", user),
    ] {
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
    prepared: BTreeMap<String, PreparedStatement>,
    portals: BTreeMap<String, Portal>,
    extended_failed: bool,
    shutdown: Option<CancellationToken>,
}

impl Connection {
    fn run(&mut self) -> Result<()> {
        loop {
            let Some(message) = read_frontend(&mut self.stream, self.config.max_frame_bytes)?
            else {
                return Ok(());
            };
            if self.extended_failed {
                match message {
                    FrontendMessage::Sync => {
                        self.extended_failed = false;
                        write_ready(&mut self.stream, transaction_status(&self.session))?;
                    }
                    FrontendMessage::Terminate => return Ok(()),
                    FrontendMessage::Flush => self.flush()?,
                    _ => {}
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
                    self.extended_failed = true;
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
                        write_notice(&mut self.stream, &notice.sql_state, &notice.message)?;
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
        if !self.prepared.contains_key(&name)
            && self.prepared.len() >= self.config.max_prepared_statements
        {
            return Err(DbError::new(
                "54000",
                "prepared statement count exceeds the configured limit",
            ));
        }
        if sql.len() > self.config.max_frame_bytes {
            return Err(protocol("prepared SQL exceeds frame limit"));
        }
        let description = self.statement_description(&sql)?;
        let parameter_oids = resolve_parameter_oids(&parameter_oids, &description.parameter_types)?;
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
        if !self.portals.contains_key(&portal_name) && self.portals.len() >= self.config.max_portals
        {
            return Err(DbError::new(
                "54000",
                "portal count exceeds the configured limit",
            ));
        }
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
        self.portals.insert(
            portal_name,
            Portal {
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
                    write_notice(&mut self.stream, &notice.sql_state, &notice.message)?;
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
                self.prepared.remove(name);
            }
            b'P' => {
                self.portals.remove(name);
            }
            _ => return Err(protocol("Close kind must be S or P")),
        }
        write_close_complete(&mut self.stream)
    }

    fn statement_stream(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>>>> {
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
        let catalog = self.engine.catalog_snapshot()?;
        if !matches!(self.session.transaction_status(), TransactionStatus::Failed)
            && let Some(events) = virtual_query(sql, &self.principal.user, &self.database, &catalog)
        {
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
        let catalog = self.engine.catalog_snapshot()?;
        if let Some(events) = virtual_query(sql, &self.principal.user, &self.database, &catalog) {
            let schema = events
                .into_iter()
                .find_map(|event| match event {
                    QueryEvent::Schema(schema) => Some(schema),
                    _ => None,
                })
                .ok_or_else(|| DbError::new("XX000", "virtual query has no schema event"))?;
            return Ok(StatementDescription {
                schema,
                parameter_types: Vec::new(),
            });
        }
        self.session.describe_statement(sql)
    }

    fn authorize(&self, sql: &str) -> Result<()> {
        let authorizer = Authorizer::from_store(&self.auth)?;
        if is_security_sql(sql) {
            authorizer.authorize(&self.principal, Action::Manage, &DbObject::Server)
        } else if catalog_query_source(sql).is_some() {
            // PostgreSQL exposes system catalogs to authenticated sessions.
            // The projection below contains metadata only and deliberately
            // excludes routine bodies and authentication material.
            Ok(())
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
            CopyDirection::ToStdout => self.copy_to_stdout(&copy.table),
            CopyDirection::FromStdin => self.copy_from_stdin(&copy.table),
        }
    }

    fn copy_to_stdout(&mut self, table: &str) -> Result<()> {
        let sql = format!("SELECT * FROM {table}");
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
        let mut rows = 0_u64;
        write_copy_response(&mut self.stream, b'H', schema.fields.len())?;
        for event in stream {
            self.check_cancelled()?;
            match event? {
                QueryEvent::Schema(_) => {
                    return Err(DbError::new(
                        "XX000",
                        "COPY source emitted more than one schema",
                    ));
                }
                QueryEvent::Batch(batch) => {
                    for row in &batch.rows {
                        let csv = encode_csv_row(&schema, row)?;
                        write_message(&mut self.stream, b'd', &csv)?;
                        rows = rows.saturating_add(1);
                    }
                }
                QueryEvent::Notice(notice) => {
                    write_notice(&mut self.stream, &notice.sql_state, &notice.message)?;
                }
                QueryEvent::Progress(_) | QueryEvent::Complete(_) => {}
            }
        }
        write_message(&mut self.stream, b'c', &[])?;
        write_command_complete(&mut self.stream, &format!("COPY {rows}"))
    }

    fn copy_from_stdin(&mut self, table: &str) -> Result<()> {
        self.authorize(&format!("COPY {table} FROM STDIN"))?;
        let columns = copy_columns(&self.engine, table)?;
        let insert = insert_statement(table, columns.len());
        drain(self.session.execute_stream("BEGIN", &[])?)?;
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
            self.rollback_copy();
            return Err(error);
        }
        let rows = match import_csv(&mut self.session, &insert, &columns, &bytes) {
            Ok(rows) => rows,
            Err(error) => {
                self.rollback_copy();
                return Err(error);
            }
        };
        drain(self.session.execute_stream("COMMIT", &[])?)?;
        write_command_complete(&mut self.stream, &format!("COPY {rows}"))
    }

    fn rollback_copy(&mut self) {
        if let Ok(stream) = self.session.execute_stream("ROLLBACK", &[]) {
            let _ = drain(stream);
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogQuerySource {
    Namespace,
    Class,
    Type,
    Enum,
    Constraint,
    Procedure,
    Trigger,
    InformationSchemaTables,
}

fn catalog_query_source(sql: &str) -> Option<CatalogQuerySource> {
    let normalized = sql
        .split_ascii_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace('"', "")
        .to_ascii_uppercase();
    if !normalized.trim_start().starts_with("SELECT ") {
        return None;
    }
    [
        ("PG_CATALOG.PG_NAMESPACE", CatalogQuerySource::Namespace),
        ("PG_CATALOG.PG_CLASS", CatalogQuerySource::Class),
        ("PG_CATALOG.PG_TYPE", CatalogQuerySource::Type),
        ("PG_CATALOG.PG_ENUM", CatalogQuerySource::Enum),
        ("PG_CATALOG.PG_CONSTRAINT", CatalogQuerySource::Constraint),
        ("PG_CATALOG.PG_PROC", CatalogQuerySource::Procedure),
        ("PG_CATALOG.PG_TRIGGER", CatalogQuerySource::Trigger),
        (
            "INFORMATION_SCHEMA.TABLES",
            CatalogQuerySource::InformationSchemaTables,
        ),
    ]
    .into_iter()
    .find_map(|(relation, source)| {
        normalized
            .contains(&format!("FROM {relation}"))
            .then_some(source)
    })
}

fn virtual_query(
    sql: &str,
    user: &str,
    database: &str,
    catalog: &Catalog,
) -> Option<Vec<QueryEvent>> {
    let normalized = sql
        .split_ascii_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(';')
        .to_ascii_uppercase();
    let text_result = |name: &str, value: String, tag: &str| {
        let schema = Schema::new(vec![Field::new(name, ScalarType::Text, false)]);
        result_events(schema, vec![Row::new(vec![Value::Text(value)])], tag)
    };
    if normalized.starts_with("SET ") || normalized.starts_with("RESET ") {
        return Some(command_events("SET", 0));
    }
    if normalized == "SHOW SERVER_VERSION" {
        return Some(text_result(
            "server_version",
            format!("16.0 (OrdaDB {})", env!("CARGO_PKG_VERSION")),
            "SHOW",
        ));
    }
    if normalized == "SHOW TRANSACTION_ISOLATION"
        || normalized == "SHOW DEFAULT_TRANSACTION_ISOLATION"
    {
        return Some(text_result(
            "transaction_isolation",
            "read committed".into(),
            "SHOW",
        ));
    }
    if let Some(source) = catalog_query_source(sql) {
        return Some(catalog_query_events(source, catalog, database));
    }
    if normalized.contains("VERSION()") {
        return Some(text_result(
            "version",
            format!(
                "PostgreSQL 16 compatible OrdaDB {} on x86_64-pc-windows-msvc",
                env!("CARGO_PKG_VERSION")
            ),
            "SELECT",
        ));
    }
    if normalized.contains("CURRENT_DATABASE()") {
        return Some(text_result(
            "current_database",
            database.to_owned(),
            "SELECT",
        ));
    }
    if normalized == "SELECT CURRENT_USER" || normalized == "SELECT SESSION_USER" {
        return Some(text_result("current_user", user.to_owned(), "SELECT"));
    }
    if normalized == "SELECT 1" {
        let schema = Schema::new(vec![Field::new("?column?", ScalarType::Int32, false)]);
        return Some(result_events(
            schema,
            vec![Row::new(vec![Value::Int32(1)])],
            "SELECT",
        ));
    }
    None
}

const fn pg_type_category(data_type: &ScalarType) -> &'static str {
    match data_type {
        ScalarType::Boolean => "B",
        ScalarType::Int16
        | ScalarType::Int32
        | ScalarType::Int64
        | ScalarType::Float32
        | ScalarType::Float64
        | ScalarType::Decimal { .. } => "N",
        ScalarType::Char { .. } | ScalarType::Varchar { .. } | ScalarType::Text => "S",
        ScalarType::Date
        | ScalarType::Time
        | ScalarType::Timestamp { .. }
        | ScalarType::Interval => "D",
        ScalarType::Array { .. } => "A",
        ScalarType::Enum { .. } => "E",
        ScalarType::Binary
        | ScalarType::Json
        | ScalarType::Jsonb
        | ScalarType::Uuid
        | ScalarType::Vector { .. } => "U",
    }
}

fn catalog_query_events(
    source: CatalogQuerySource,
    catalog: &Catalog,
    connection_database: &str,
) -> Vec<QueryEvent> {
    const SCHEMA_OID_BASE: i64 = 10_000_000;
    const TABLE_OID_BASE: i64 = 20_000_000;
    const VIEW_OID_BASE: i64 = 30_000_000;
    const ROUTINE_OID_BASE: i64 = 40_000_000;
    const TRIGGER_OID_BASE: i64 = 50_000_000;
    const SEQUENCE_OID_BASE: i64 = 60_000_000;
    const INDEX_OID_BASE: i64 = 70_000_000;

    let oid = |base: i64, id: u64| base.saturating_add(i64::try_from(id).unwrap_or(i64::MAX));
    let catalog_database = catalog.database();
    let (schema, rows) = match source {
        CatalogQuerySource::Namespace => (
            Schema::new(vec![
                Field::new("oid", ScalarType::Int64, false),
                Field::new("nspname", ScalarType::Text, false),
            ]),
            catalog_database
                .schemas()
                .map(|schema| {
                    Row::new(vec![
                        Value::Int64(oid(SCHEMA_OID_BASE, schema.id.get())),
                        Value::Text(schema.name.as_str().to_owned()),
                    ])
                })
                .collect(),
        ),
        CatalogQuerySource::Class => {
            let mut rows = Vec::new();
            for schema in catalog_database.schemas() {
                let namespace_oid = oid(SCHEMA_OID_BASE, schema.id.get());
                let materialized_tables = schema
                    .views()
                    .filter_map(|view| view.materialized_table_id)
                    .collect::<BTreeSet<_>>();
                for table in schema
                    .tables()
                    .filter(|table| !materialized_tables.contains(&table.id))
                {
                    rows.push(Row::new(vec![
                        Value::Int64(oid(TABLE_OID_BASE, table.id.get())),
                        Value::Text(table.name.as_str().to_owned()),
                        Value::Int64(namespace_oid),
                        Value::Text("r".into()),
                        Value::Text("p".into()),
                        Value::Int32(i32::try_from(table.columns().len()).unwrap_or(i32::MAX)),
                        Value::Boolean(table.indexes().next().is_some()),
                    ]));
                    rows.extend(table.indexes().map(|index| {
                        Row::new(vec![
                            Value::Int64(oid(INDEX_OID_BASE, index.id.get())),
                            Value::Text(index.name.as_str().to_owned()),
                            Value::Int64(namespace_oid),
                            Value::Text("i".into()),
                            Value::Text("p".into()),
                            Value::Int32(0),
                            Value::Boolean(false),
                        ])
                    }));
                }
                rows.extend(schema.sequences().map(|sequence| {
                    Row::new(vec![
                        Value::Int64(oid(SEQUENCE_OID_BASE, sequence.id.get())),
                        Value::Text(sequence.name.as_str().to_owned()),
                        Value::Int64(namespace_oid),
                        Value::Text("S".into()),
                        Value::Text("p".into()),
                        Value::Int32(0),
                        Value::Boolean(false),
                    ])
                }));
                rows.extend(schema.views().map(|view| {
                    Row::new(vec![
                        Value::Int64(oid(VIEW_OID_BASE, view.id.get())),
                        Value::Text(view.name.as_str().to_owned()),
                        Value::Int64(namespace_oid),
                        Value::Text(
                            match view.kind {
                                ViewKind::Regular => "v",
                                ViewKind::Materialized => "m",
                            }
                            .into(),
                        ),
                        Value::Text("p".into()),
                        Value::Int32(i32::try_from(view.output.fields.len()).unwrap_or(i32::MAX)),
                        Value::Boolean(view.materialized_table_id.is_some_and(|table_id| {
                            catalog
                                .table_by_id(table_id)
                                .is_some_and(|table| table.indexes().next().is_some())
                        })),
                    ])
                }));
            }
            (
                Schema::new(vec![
                    Field::new("oid", ScalarType::Int64, false),
                    Field::new("relname", ScalarType::Text, false),
                    Field::new("relnamespace", ScalarType::Int64, false),
                    Field::new("relkind", ScalarType::Text, false),
                    Field::new("relpersistence", ScalarType::Text, false),
                    Field::new("relnatts", ScalarType::Int32, false),
                    Field::new("relhasindex", ScalarType::Boolean, false),
                ]),
                rows,
            )
        }
        CatalogQuerySource::Type => {
            let mut rows = Vec::new();
            for schema in catalog_database.schemas() {
                let namespace_oid = oid(SCHEMA_OID_BASE, schema.id.get());
                for definition in schema.types() {
                    let scalar_oid = i64::from(user_defined_type_oid(definition.id));
                    let array_oid = i64::from(user_defined_array_oid(definition.id));
                    let (kind, category, base_oid, not_null, default) = match &definition.definition
                    {
                        UserDefinedTypeKind::Enum { .. } => ("e", "E", 0_i64, false, Value::Null),
                        UserDefinedTypeKind::Domain {
                            base_type,
                            base_declared_type,
                            not_null,
                            default,
                            ..
                        } => (
                            "d",
                            pg_type_category(base_type),
                            i64::from(
                                base_declared_type
                                    .map_or_else(|| type_oid(base_type), user_defined_type_oid),
                            ),
                            *not_null,
                            default.as_ref().map_or(Value::Null, |expression| {
                                Value::Text(expression.sql.clone())
                            }),
                        ),
                    };
                    rows.push(Row::new(vec![
                        Value::Int64(scalar_oid),
                        Value::Text(definition.name.as_str().to_owned()),
                        Value::Int64(namespace_oid),
                        Value::Text(kind.into()),
                        Value::Text(category.into()),
                        Value::Boolean(true),
                        Value::Int64(0),
                        Value::Int64(array_oid),
                        Value::Int64(base_oid),
                        Value::Boolean(not_null),
                        default,
                    ]));
                    rows.push(Row::new(vec![
                        Value::Int64(array_oid),
                        Value::Text(format!("_{}", definition.name.as_str())),
                        Value::Int64(namespace_oid),
                        Value::Text("b".into()),
                        Value::Text("A".into()),
                        Value::Boolean(true),
                        Value::Int64(scalar_oid),
                        Value::Int64(0),
                        Value::Int64(0),
                        Value::Boolean(false),
                        Value::Null,
                    ]));
                }
            }
            (
                Schema::new(vec![
                    Field::new("oid", ScalarType::Int64, false),
                    Field::new("typname", ScalarType::Text, false),
                    Field::new("typnamespace", ScalarType::Int64, false),
                    Field::new("typtype", ScalarType::Text, false),
                    Field::new("typcategory", ScalarType::Text, false),
                    Field::new("typisdefined", ScalarType::Boolean, false),
                    Field::new("typelem", ScalarType::Int64, false),
                    Field::new("typarray", ScalarType::Int64, false),
                    Field::new("typbasetype", ScalarType::Int64, false),
                    Field::new("typnotnull", ScalarType::Boolean, false),
                    Field::new("typdefault", ScalarType::Text, true),
                ]),
                rows,
            )
        }
        CatalogQuerySource::Enum => {
            const ENUM_LABEL_OID_BASE: i64 = 1_000_000_000;
            let rows = catalog_database
                .schemas()
                .flat_map(|schema| {
                    schema.types().flat_map(|definition| {
                        let UserDefinedTypeKind::Enum { labels } = &definition.definition else {
                            return Vec::new().into_iter();
                        };
                        let type_oid = i64::from(user_defined_type_oid(definition.id));
                        let type_offset = i64::try_from(definition.id.get())
                            .unwrap_or(i64::MAX)
                            .saturating_mul(1_000_000);
                        labels
                            .iter()
                            .enumerate()
                            .map(|(ordinal, label)| {
                                Row::new(vec![
                                    Value::Int64(
                                        ENUM_LABEL_OID_BASE
                                            .saturating_add(type_offset)
                                            .saturating_add(
                                                i64::try_from(ordinal).unwrap_or(i64::MAX),
                                            ),
                                    ),
                                    Value::Int64(type_oid),
                                    Value::Float64(
                                        u32::try_from(ordinal)
                                            .map_or(f64::from(u32::MAX), |value| {
                                                f64::from(value.saturating_add(1))
                                            }),
                                    ),
                                    Value::Text(label.clone()),
                                ])
                            })
                            .collect::<Vec<_>>()
                            .into_iter()
                    })
                })
                .collect();
            (
                Schema::new(vec![
                    Field::new("oid", ScalarType::Int64, false),
                    Field::new("enumtypid", ScalarType::Int64, false),
                    Field::new("enumsortorder", ScalarType::Float64, false),
                    Field::new("enumlabel", ScalarType::Text, false),
                ]),
                rows,
            )
        }
        CatalogQuerySource::Constraint => {
            const CONSTRAINT_OID_BASE: i64 = 1_100_000_000;
            const LEGACY_DOMAIN_CONSTRAINT_OID_BASE: i64 = 1_200_000_000;
            let mut rows = Vec::new();
            for schema in catalog_database.schemas() {
                let namespace_oid = oid(SCHEMA_OID_BASE, schema.id.get());
                for table in schema.tables() {
                    for constraint in table.constraints() {
                        let (constraint_type, expression) = match &constraint.kind {
                            ConstraintKind::PrimaryKey { .. } => ("p", Value::Null),
                            ConstraintKind::Unique { .. } => ("u", Value::Null),
                            ConstraintKind::ForeignKey { .. } => ("f", Value::Null),
                            ConstraintKind::Check { expression } => {
                                ("c", Value::Text(expression.sql.clone()))
                            }
                        };
                        rows.push(Row::new(vec![
                            Value::Int64(oid(CONSTRAINT_OID_BASE, constraint.id.get())),
                            Value::Text(constraint.name.as_str().to_owned()),
                            Value::Text(constraint_type.into()),
                            Value::Int64(namespace_oid),
                            Value::Int64(oid(TABLE_OID_BASE, table.id.get())),
                            Value::Int64(0),
                            Value::Boolean(true),
                            expression,
                        ]));
                    }
                }
                for definition in schema.types() {
                    let UserDefinedTypeKind::Domain { checks, .. } = &definition.definition else {
                        continue;
                    };
                    for (ordinal, constraint) in checks.iter().enumerate() {
                        let generated_name = if ordinal == 0 {
                            format!("{}_check", definition.name.as_str())
                        } else {
                            format!("{}_check{}", definition.name.as_str(), ordinal + 1)
                        };
                        let constraint_oid = constraint.id.map_or_else(
                            || {
                                LEGACY_DOMAIN_CONSTRAINT_OID_BASE
                                    .saturating_add(
                                        i64::try_from(definition.id.get())
                                            .unwrap_or(i64::MAX)
                                            .saturating_mul(1_000_000),
                                    )
                                    .saturating_add(i64::try_from(ordinal).unwrap_or(i64::MAX))
                            },
                            |id| oid(CONSTRAINT_OID_BASE, id.get()),
                        );
                        rows.push(Row::new(vec![
                            Value::Int64(constraint_oid),
                            Value::Text(
                                constraint
                                    .name
                                    .as_ref()
                                    .map_or(generated_name, |name| name.as_str().to_owned()),
                            ),
                            Value::Text("c".into()),
                            Value::Int64(namespace_oid),
                            Value::Int64(0),
                            Value::Int64(i64::from(user_defined_type_oid(definition.id))),
                            Value::Boolean(true),
                            Value::Text(constraint.expression.sql.clone()),
                        ]));
                    }
                }
            }
            (
                Schema::new(vec![
                    Field::new("oid", ScalarType::Int64, false),
                    Field::new("conname", ScalarType::Text, false),
                    Field::new("contype", ScalarType::Text, false),
                    Field::new("connamespace", ScalarType::Int64, false),
                    Field::new("conrelid", ScalarType::Int64, false),
                    Field::new("contypid", ScalarType::Int64, false),
                    Field::new("convalidated", ScalarType::Boolean, false),
                    Field::new("conbin", ScalarType::Text, true),
                ]),
                rows,
            )
        }
        CatalogQuerySource::Procedure => {
            let rows = catalog_database
                .schemas()
                .flat_map(|schema| {
                    let namespace_oid = oid(SCHEMA_OID_BASE, schema.id.get());
                    schema.routines().map(move |routine| {
                        let return_oid = routine.return_type.as_ref().map_or(2278, |return_type| {
                            routine
                                .return_declared_type
                                .map_or_else(|| type_oid(return_type), user_defined_type_oid)
                        });
                        let argument_oids = routine
                            .arguments
                            .iter()
                            .map(|argument| {
                                argument
                                    .declared_type
                                    .map_or_else(
                                        || type_oid(&argument.data_type),
                                        user_defined_type_oid,
                                    )
                                    .to_string()
                            })
                            .collect::<Vec<_>>()
                            .join(" ");
                        Row::new(vec![
                            Value::Int64(oid(ROUTINE_OID_BASE, routine.id.get())),
                            Value::Text(routine.name.as_str().to_owned()),
                            Value::Int64(namespace_oid),
                            Value::Text(
                                match routine.kind {
                                    RoutineKind::Function => "f",
                                    RoutineKind::Procedure => "p",
                                }
                                .into(),
                            ),
                            Value::Int64(i64::from(return_oid)),
                            Value::Boolean(routine.returns_set),
                            Value::Text(argument_oids),
                            Value::Text(routine.language.clone()),
                        ])
                    })
                })
                .collect();
            (
                Schema::new(vec![
                    Field::new("oid", ScalarType::Int64, false),
                    Field::new("proname", ScalarType::Text, false),
                    Field::new("pronamespace", ScalarType::Int64, false),
                    Field::new("prokind", ScalarType::Text, false),
                    Field::new("prorettype", ScalarType::Int64, false),
                    Field::new("proretset", ScalarType::Boolean, false),
                    Field::new("proargtypes", ScalarType::Text, false),
                    Field::new("prolang", ScalarType::Text, false),
                ]),
                rows,
            )
        }
        CatalogQuerySource::Trigger => {
            let rows = catalog_database
                .schemas()
                .flat_map(|schema| {
                    let materialized_tables = schema
                        .views()
                        .filter_map(|view| view.materialized_table_id)
                        .collect::<BTreeSet<_>>();
                    schema
                        .tables()
                        .filter(move |table| !materialized_tables.contains(&table.id))
                        .flat_map(|table| {
                            table.triggers().map(|trigger| {
                                Row::new(vec![
                                    Value::Int64(oid(TRIGGER_OID_BASE, trigger.id.get())),
                                    Value::Text(trigger.name.as_str().to_owned()),
                                    Value::Int64(oid(TABLE_OID_BASE, table.id.get())),
                                    Value::Text(if trigger.enabled { "O" } else { "D" }.into()),
                                    Value::Boolean(false),
                                    Value::Int64(oid(ROUTINE_OID_BASE, trigger.routine_id.get())),
                                ])
                            })
                        })
                })
                .collect();
            (
                Schema::new(vec![
                    Field::new("oid", ScalarType::Int64, false),
                    Field::new("tgname", ScalarType::Text, false),
                    Field::new("tgrelid", ScalarType::Int64, false),
                    Field::new("tgenabled", ScalarType::Text, false),
                    Field::new("tgisinternal", ScalarType::Boolean, false),
                    Field::new("tgfoid", ScalarType::Int64, false),
                ]),
                rows,
            )
        }
        CatalogQuerySource::InformationSchemaTables => {
            let mut rows = Vec::new();
            for schema in catalog_database.schemas() {
                let materialized_tables = schema
                    .views()
                    .filter_map(|view| view.materialized_table_id)
                    .collect::<BTreeSet<_>>();
                rows.extend(
                    schema
                        .tables()
                        .filter(|table| !materialized_tables.contains(&table.id))
                        .map(|table| {
                            Row::new(vec![
                                Value::Text(connection_database.to_owned()),
                                Value::Text(schema.name.as_str().to_owned()),
                                Value::Text(table.name.as_str().to_owned()),
                                Value::Text("BASE TABLE".into()),
                                Value::Text("YES".into()),
                            ])
                        }),
                );
                rows.extend(schema.views().map(|view| {
                    Row::new(vec![
                        Value::Text(connection_database.to_owned()),
                        Value::Text(schema.name.as_str().to_owned()),
                        Value::Text(view.name.as_str().to_owned()),
                        Value::Text(
                            match view.kind {
                                ViewKind::Regular => "VIEW",
                                ViewKind::Materialized => "MATERIALIZED VIEW",
                            }
                            .into(),
                        ),
                        Value::Text("NO".into()),
                    ])
                }));
            }
            (
                Schema::new(vec![
                    Field::new("table_catalog", ScalarType::Text, false),
                    Field::new("table_schema", ScalarType::Text, false),
                    Field::new("table_name", ScalarType::Text, false),
                    Field::new("table_type", ScalarType::Text, false),
                    Field::new("is_insertable_into", ScalarType::Text, false),
                ]),
                rows,
            )
        }
    };
    result_events(schema, rows, "SELECT")
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

enum CopyDirection {
    ToStdout,
    FromStdin,
}

struct CopyCommand {
    table: String,
    direction: CopyDirection,
}

fn parse_copy(sql: &str) -> Result<Option<CopyCommand>> {
    let parts: Vec<&str> = sql.split_ascii_whitespace().collect();
    if !parts
        .first()
        .is_some_and(|part| part.eq_ignore_ascii_case("COPY"))
    {
        return Ok(None);
    }
    if parts.len() != 4 {
        return Err(DbError::new(
            "0A000",
            "only COPY <table> TO STDOUT and COPY <table> FROM STDIN are supported",
        ));
    }
    let table = parts[1];
    if !table
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.'))
    {
        return Err(DbError::new(
            "0A000",
            "COPY currently supports only unquoted schema/table names",
        ));
    }
    let direction =
        if parts[2].eq_ignore_ascii_case("TO") && parts[3].eq_ignore_ascii_case("STDOUT") {
            CopyDirection::ToStdout
        } else if parts[2].eq_ignore_ascii_case("FROM") && parts[3].eq_ignore_ascii_case("STDIN") {
            CopyDirection::FromStdin
        } else {
            return Err(DbError::new(
                "0A000",
                "only COPY TO STDOUT or COPY FROM STDIN is supported",
            ));
        };
    Ok(Some(CopyCommand {
        table: table.to_owned(),
        direction,
    }))
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

fn encode_csv_row(schema: &Schema, row: &Row) -> Result<Vec<u8>> {
    if schema.fields.len() != row.values.len() {
        return Err(DbError::new(
            "XX000",
            "COPY row width does not match schema",
        ));
    }
    let fields: Result<Vec<String>> = row
        .values
        .iter()
        .map(|value| match value {
            Value::Null => Ok("\\N".to_owned()),
            value => String::from_utf8(encode_text(value)?)
                .map_err(|_| DbError::new("XX000", "COPY text encoding is not UTF-8")),
        })
        .collect();
    let mut writer = WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    writer.write_record(fields?).map_err(|error| {
        DbError::new("XX000", "failed to encode COPY CSV").with_detail(error.to_string())
    })?;
    writer.into_inner().map_err(|error| {
        DbError::new("XX000", "failed to flush COPY CSV").with_detail(error.to_string())
    })
}

fn copy_columns(engine: &Engine, table: &str) -> Result<Vec<ColumnDefinition>> {
    let (schema, table) = table
        .split_once('.')
        .map_or(("public", table), |(schema, table)| (schema, table));
    let catalog = engine.catalog_snapshot()?;
    let table = catalog
        .table(&Identifier::unquoted(schema), &Identifier::unquoted(table))
        .ok_or_else(|| DbError::new("42P01", "COPY table does not exist"))?;
    Ok(table.columns().to_vec())
}

fn insert_statement(table: &str, columns: usize) -> String {
    let parameters = (1..=columns)
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} VALUES ({parameters})")
}

fn import_csv(
    session: &mut Session,
    insert: &str,
    columns: &[ColumnDefinition],
    bytes: &[u8],
) -> Result<u64> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_reader(bytes);
    let mut rows = 0_u64;
    let data_types = columns
        .iter()
        .map(|column| column.data_type.clone())
        .collect::<Vec<_>>();
    let oids = data_types.iter().map(type_oid).collect::<Vec<_>>();
    for record in reader.records() {
        let record = record.map_err(|error| {
            DbError::new("22P04", "COPY CSV is invalid").with_detail(error.to_string())
        })?;
        if record.len() != columns.len() {
            return Err(DbError::new(
                "22P04",
                format!(
                    "COPY row has {} fields but table has {} columns",
                    record.len(),
                    columns.len()
                ),
            ));
        }
        let raw: Vec<Option<Vec<u8>>> = record
            .iter()
            .map(|value| {
                if value == "\\N" {
                    None
                } else {
                    Some(value.as_bytes().to_vec())
                }
            })
            .collect();
        let values = decode_parameters_as(&oids, &data_types, &[], &raw)?;
        drain(session.execute_stream(insert, &values)?)?;
        rows = rows
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "COPY row count overflowed"))?;
    }
    Ok(rows)
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

#[cfg(test)]
mod tests {
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
    fn user_defined_type_catalog_projection_preserves_declared_identity() {
        use ordadb_catalog::{
            CatalogExpression, CatalogObjectRef, DomainConstraint, NewRoutine, RoutineArgument,
        };
        use ordadb_types::{Identifier, TypeId};

        let mut catalog = Catalog::default();
        let mood_id = catalog
            .create_enum_type(
                &Identifier::unquoted("public"),
                Identifier::unquoted("mood"),
                vec!["sad".into(), "ok".into(), "happy".into()],
            )
            .expect("enum");
        let positive_id = catalog
            .create_domain(
                &Identifier::unquoted("public"),
                Identifier::unquoted("positive_int"),
                ScalarType::Int32,
                true,
                Some(CatalogExpression::new("1")),
                vec![DomainConstraint {
                    id: None,
                    name: Some(Identifier::unquoted("positive")),
                    expression: CatalogExpression::new("VALUE > 0"),
                }],
            )
            .expect("positive domain");
        let nonnegative_id = catalog
            .create_domain(
                &Identifier::unquoted("public"),
                Identifier::unquoted("nonnegative_int"),
                ScalarType::Int32,
                false,
                None,
                Vec::new(),
            )
            .expect("nonnegative domain");
        let routine = |type_id: TypeId| NewRoutine {
            name: Identifier::unquoted("choose_value"),
            kind: RoutineKind::Function,
            arguments: vec![RoutineArgument {
                name: Some(Identifier::unquoted("value")),
                data_type: ScalarType::Int32,
                declared_type: Some(type_id),
            }],
            return_type: Some(ScalarType::Int32),
            return_declared_type: Some(type_id),
            returns_set: false,
            language: "plpgsql".into(),
            body: "BEGIN RETURN value; END".into(),
            replace: false,
            references: vec![CatalogObjectRef::Type(type_id)],
        };
        catalog
            .create_or_replace_routine(&Identifier::unquoted("public"), routine(positive_id))
            .expect("positive overload");
        catalog
            .create_or_replace_routine(&Identifier::unquoted("public"), routine(nonnegative_id))
            .expect("nonnegative overload");

        assert_eq!(
            catalog_query_source("SELECT * FROM pg_catalog.pg_type"),
            Some(CatalogQuerySource::Type)
        );
        assert_eq!(
            catalog_query_source("SELECT * FROM pg_catalog.pg_enum"),
            Some(CatalogQuerySource::Enum)
        );
        assert_eq!(
            catalog_query_source("SELECT * FROM pg_catalog.pg_constraint"),
            Some(CatalogQuerySource::Constraint)
        );

        let rows = |source| {
            catalog_query_events(source, &catalog, "ordadb")
                .into_iter()
                .find_map(|event| match event {
                    QueryEvent::Batch(batch) => Some(batch.rows),
                    _ => None,
                })
                .unwrap_or_default()
        };
        let type_rows = rows(CatalogQuerySource::Type);
        for (name, type_id, kind) in [
            ("mood", mood_id, "e"),
            ("positive_int", positive_id, "d"),
            ("nonnegative_int", nonnegative_id, "d"),
        ] {
            assert!(type_rows.iter().any(|row| {
                row.values[0] == Value::Int64(i64::from(user_defined_type_oid(type_id)))
                    && row.values[1] == Value::Text(name.into())
                    && row.values[3] == Value::Text(kind.into())
                    && row.values[7] == Value::Int64(i64::from(user_defined_array_oid(type_id)))
            }));
        }
        let enum_rows = rows(CatalogQuerySource::Enum);
        assert_eq!(
            enum_rows
                .iter()
                .map(|row| row.values[3].clone())
                .collect::<Vec<_>>(),
            vec![
                Value::Text("sad".into()),
                Value::Text("ok".into()),
                Value::Text("happy".into()),
            ]
        );
        let procedure_rows = rows(CatalogQuerySource::Procedure);
        let argument_oids = procedure_rows
            .iter()
            .filter(|row| row.values[1] == Value::Text("choose_value".into()))
            .filter_map(|row| match &row.values[6] {
                Value::Text(value) => Some(value.clone()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            argument_oids,
            BTreeSet::from([
                user_defined_type_oid(positive_id).to_string(),
                user_defined_type_oid(nonnegative_id).to_string(),
            ])
        );
        let constraint_rows = rows(CatalogQuerySource::Constraint);
        let constraint_oid = constraint_rows
            .iter()
            .find(|row| row.values[1] == Value::Text("positive".into()))
            .map(|row| row.values[0].clone())
            .expect("domain constraint projection");

        let reopened: Catalog =
            serde_json::from_slice(&serde_json::to_vec(&catalog).expect("serialize catalog"))
                .expect("reopen catalog");
        let reopened_constraint_oid =
            catalog_query_events(CatalogQuerySource::Constraint, &reopened, "ordadb")
                .into_iter()
                .find_map(|event| match event {
                    QueryEvent::Batch(batch) => batch
                        .rows
                        .into_iter()
                        .find(|row| row.values[1] == Value::Text("positive".into()))
                        .map(|row| row.values[0].clone()),
                    _ => None,
                })
                .expect("reopened domain constraint projection");
        assert_eq!(reopened_constraint_oid, constraint_oid);
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
        assert!(parse_copy("COPY items TO 'file.csv'").is_err());
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
    fn synthetic_compatibility_queries_have_real_rows() {
        let events = virtual_query("SELECT version()", "dba", "ordadb", &Catalog::default())
            .expect("virtual");
        assert!(matches!(events.first(), Some(QueryEvent::Schema(_))));
        assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
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
