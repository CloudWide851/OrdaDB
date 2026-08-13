use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use rand::rngs::OsRng;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use ordadb_types::{DbError, DbNotice, DbNoticeSeverity, DbObjectIdentity, Result};

use crate::codec::{CANCEL_REQUEST_CODE, PROTOCOL_VERSION_3, io_error, protocol};

const CLIENT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const CLIENT_MAX_PENDING_NOTICES: usize = 1_024;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_CLIENT_BATCH_ROWS: usize = 1_024;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub address: SocketAddr,
    pub user: String,
    pub database: String,
    pub password: Zeroizing<String>,
    pub application_name: String,
    pub query_memory_bytes: Option<usize>,
    pub timeout: Option<Duration>,
}

pub struct PgClient {
    stream: TcpStream,
    address: SocketAddr,
    process_id: u32,
    secret_key: u32,
    transaction_status: PgTransactionStatus,
    pending_notices: Vec<DbNotice>,
}

#[derive(Clone)]
pub struct PgCancelToken {
    address: SocketAddr,
    process_id: u32,
    secret_key: u32,
}

impl std::fmt::Debug for PgCancelToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgCancelToken")
            .field("address", &self.address)
            .field("process_id", &self.process_id)
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Debug for PgClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgClient")
            .field("address", &self.address)
            .field("process_id", &self.process_id)
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tags: Vec<String>,
    pub notices: Vec<DbNotice>,
    pub notifications: Vec<PgNotification>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PgTransactionStatus {
    #[default]
    Idle,
    InTransaction,
    FailedTransaction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgQueryEvent {
    Schema(Vec<String>),
    Batch(Vec<Vec<Option<String>>>),
    Notice(DbNotice),
    Complete(String),
    Notification(PgNotification),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgNotification {
    pub sender_process_id: u32,
    pub channel: String,
    pub payload: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuerySummary {
    pub columns: Vec<String>,
    pub command_tags: Vec<String>,
    pub row_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CopyOutResult {
    pub columns: usize,
    pub data: Vec<u8>,
    pub command_tag: String,
}

impl PgClient {
    pub fn connect(config: ClientConfig) -> Result<Self> {
        let timeout = config.timeout.unwrap_or(CLIENT_TIMEOUT);
        if timeout.is_zero() || timeout > CLIENT_TIMEOUT {
            return Err(DbError::new(
                "22023",
                "PostgreSQL client timeout must be between 1 millisecond and 60 seconds",
            ));
        }
        let mut stream = TcpStream::connect_timeout(&config.address, timeout)
            .map_err(|error| io_error("failed to connect to OrdaDB", error))?;
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|error| io_error("failed to set client read timeout", error))?;
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|error| io_error("failed to set client write timeout", error))?;
        write_startup(&mut stream, &config)?;

        let mut process_id = 0;
        let mut secret_key = 0;
        let mut pending_notices = Vec::new();
        let mut scram = ClientScram::new(&config.user, config.password.as_bytes());
        let transaction_status = loop {
            let message = read_backend(&mut stream)?;
            match message.tag {
                b'R' => scram.handle_authentication(&mut stream, &message.payload)?,
                b'K' => {
                    if message.payload.len() != 8 {
                        return Err(protocol("BackendKeyData length is invalid"));
                    }
                    process_id =
                        u32::from_be_bytes(message.payload[..4].try_into().expect("checked"));
                    secret_key =
                        u32::from_be_bytes(message.payload[4..].try_into().expect("checked"));
                }
                b'E' => return Err(decode_error(&message.payload)),
                b'Z' => break decode_ready_status(&message.payload)?,
                b'N' => {
                    push_pending_notice(&mut pending_notices, decode_notice(&message.payload)?)?
                }
                b'S' => {}
                other => {
                    return Err(protocol(format!(
                        "unexpected backend message 0x{other:02x} during startup"
                    )));
                }
            }
        };
        if process_id == 0 {
            return Err(protocol("server did not send BackendKeyData"));
        }
        Ok(Self {
            stream,
            address: config.address,
            process_id,
            secret_key,
            transaction_status,
            pending_notices,
        })
    }

    #[must_use]
    pub const fn transaction_status(&self) -> PgTransactionStatus {
        self.transaction_status
    }

    pub fn take_notices(&mut self) -> Vec<DbNotice> {
        std::mem::take(&mut self.pending_notices)
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let mut result = QueryResult::default();
        self.query_batches(sql, DEFAULT_CLIENT_BATCH_ROWS, |event| {
            collect_query_event(&mut result, event);
            Ok(())
        })?;
        Ok(result)
    }

    pub fn query_batches(
        &mut self,
        sql: &str,
        batch_rows: usize,
        mut on_event: impl FnMut(PgQueryEvent) -> Result<()>,
    ) -> Result<QuerySummary> {
        validate_batch_rows(batch_rows)?;
        let mut payload = Vec::with_capacity(sql.len() + 1);
        push_cstring(&mut payload, sql)?;
        write_frontend(&mut self.stream, b'Q', &payload)?;
        self.stream
            .flush()
            .map_err(|error| io_error("failed to flush query", error))?;
        let mut state = QueryStreamState::default();
        let suspended = self.read_query_cycle_batches(&mut state, batch_rows, &mut on_event)?;
        if suspended {
            return Err(protocol("Simple Query unexpectedly suspended a portal"));
        }
        Ok(state.into_summary())
    }

    pub fn query_prepared(
        &mut self,
        sql: &str,
        parameter_oids: &[u32],
        parameters: &[Option<Vec<u8>>],
        fetch_rows: u32,
    ) -> Result<QueryResult> {
        let mut result = QueryResult::default();
        self.query_prepared_batches(sql, parameter_oids, parameters, fetch_rows, |event| {
            collect_query_event(&mut result, event);
            Ok(())
        })?;
        Ok(result)
    }

    pub fn read_notification(&mut self) -> Result<PgNotification> {
        loop {
            let message = read_backend(&mut self.stream)?;
            match message.tag {
                b'A' => return decode_notification(&message.payload),
                b'E' => return Err(decode_error(&message.payload)),
                b'N' => {
                    let notice = decode_notice(&message.payload)?;
                    push_pending_notice(&mut self.pending_notices, notice)?;
                }
                other => {
                    return Err(protocol(format!(
                        "unexpected backend message 0x{other:02x} while waiting for a notification"
                    )));
                }
            }
        }
    }

    pub fn query_prepared_batches(
        &mut self,
        sql: &str,
        parameter_oids: &[u32],
        parameters: &[Option<Vec<u8>>],
        fetch_rows: u32,
        mut on_event: impl FnMut(PgQueryEvent) -> Result<()>,
    ) -> Result<QuerySummary> {
        if !parameter_oids.is_empty() && parameter_oids.len() != parameters.len() {
            return Err(protocol(
                "parameter OID count does not match parameter value count",
            ));
        }
        let parameter_count =
            u16::try_from(parameters.len()).map_err(|_| protocol("parameter count exceeds u16"))?;
        let oid_count = u16::try_from(parameter_oids.len())
            .map_err(|_| protocol("parameter OID count exceeds u16"))?;

        let mut parse = Vec::new();
        push_cstring(&mut parse, "")?;
        push_cstring(&mut parse, sql)?;
        parse.extend_from_slice(&oid_count.to_be_bytes());
        for oid in parameter_oids {
            parse.extend_from_slice(&oid.to_be_bytes());
        }
        write_frontend(&mut self.stream, b'P', &parse)?;

        let mut bind = Vec::new();
        push_cstring(&mut bind, "")?;
        push_cstring(&mut bind, "")?;
        bind.extend_from_slice(&0_u16.to_be_bytes());
        bind.extend_from_slice(&parameter_count.to_be_bytes());
        for parameter in parameters {
            match parameter {
                Some(bytes) => {
                    let length = i32::try_from(bytes.len())
                        .map_err(|_| protocol("parameter value exceeds i32"))?;
                    bind.extend_from_slice(&length.to_be_bytes());
                    bind.extend_from_slice(bytes);
                }
                None => bind.extend_from_slice(&(-1_i32).to_be_bytes()),
            }
        }
        bind.extend_from_slice(&0_u16.to_be_bytes());
        write_frontend(&mut self.stream, b'B', &bind)?;

        write_frontend(&mut self.stream, b'D', &[b'P', 0])?;
        self.write_execute(fetch_rows)?;
        write_frontend(&mut self.stream, b'S', &[])?;

        let batch_rows = usize::try_from(fetch_rows)
            .ok()
            .filter(|rows| *rows > 0)
            .unwrap_or(DEFAULT_CLIENT_BATCH_ROWS);
        let mut state = QueryStreamState::default();
        while self.read_query_cycle_batches(&mut state, batch_rows, &mut on_event)? {
            self.write_execute(fetch_rows)?;
            write_frontend(&mut self.stream, b'S', &[])?;
        }
        Ok(state.into_summary())
    }

    pub fn copy_to_stdout(&mut self, table: &str) -> Result<CopyOutResult> {
        validate_copy_table(table)?;
        self.write_query(&format!("COPY {table} TO STDOUT WITH (FORMAT csv)"))?;
        let mut result = CopyOutResult::default();
        let mut pending_error = None;
        loop {
            let message = read_backend(&mut self.stream)?;
            match message.tag {
                b'H' => result.columns = decode_copy_response(&message.payload)?,
                b'd' => result.data.extend_from_slice(&message.payload),
                b'A' => {
                    let _ = decode_notification(&message.payload)?;
                }
                b'N' => {
                    let notice = decode_notice(&message.payload)?;
                    push_pending_notice(&mut self.pending_notices, notice)?;
                }
                b'c' | b'I' => {}
                b'C' => {
                    result.command_tag = decode_only_cstring(&message.payload, "CommandComplete")?;
                }
                b'E' => pending_error = Some(decode_error(&message.payload)),
                b'Z' => {
                    self.transaction_status = decode_ready_status(&message.payload)?;
                    return pending_error.map_or(Ok(result), Err);
                }
                other => {
                    return Err(protocol(format!(
                        "unexpected backend message 0x{other:02x} during COPY OUT"
                    )));
                }
            }
        }
    }

    pub fn copy_from_stdin(&mut self, table: &str, data: &[u8]) -> Result<String> {
        validate_copy_table(table)?;
        self.write_query(&format!("COPY {table} FROM STDIN WITH (FORMAT csv)"))?;
        let mut pending_error = None;
        let mut copy_started = false;
        let mut command_tag = String::new();
        loop {
            let message = read_backend(&mut self.stream)?;
            match message.tag {
                b'G' if !copy_started => {
                    let _ = decode_copy_response(&message.payload)?;
                    copy_started = true;
                    for chunk in data.chunks(64 * 1024) {
                        write_frontend(&mut self.stream, b'd', chunk)?;
                    }
                    write_frontend(&mut self.stream, b'c', &[])?;
                }
                b'C' => {
                    command_tag = decode_only_cstring(&message.payload, "CommandComplete")?;
                }
                b'E' => pending_error = Some(decode_error(&message.payload)),
                b'A' => {
                    let _ = decode_notification(&message.payload)?;
                }
                b'N' => {
                    let notice = decode_notice(&message.payload)?;
                    push_pending_notice(&mut self.pending_notices, notice)?;
                }
                b'I' => {}
                b'Z' => {
                    self.transaction_status = decode_ready_status(&message.payload)?;
                    if let Some(error) = pending_error {
                        return Err(error);
                    }
                    if !copy_started {
                        return Err(protocol("COPY IN ended before CopyInResponse"));
                    }
                    return Ok(command_tag);
                }
                other => {
                    return Err(protocol(format!(
                        "unexpected backend message 0x{other:02x} during COPY IN"
                    )));
                }
            }
        }
    }

    #[must_use]
    pub const fn cancellation_token(&self) -> PgCancelToken {
        PgCancelToken {
            address: self.address,
            process_id: self.process_id,
            secret_key: self.secret_key,
        }
    }

    pub fn cancel(&self) -> Result<()> {
        self.cancellation_token().cancel()
    }

    fn write_query(&mut self, sql: &str) -> Result<()> {
        let mut payload = Vec::with_capacity(sql.len() + 1);
        push_cstring(&mut payload, sql)?;
        write_frontend(&mut self.stream, b'Q', &payload)
    }

    fn write_execute(&mut self, max_rows: u32) -> Result<()> {
        let mut execute = vec![0];
        execute.extend_from_slice(&max_rows.to_be_bytes());
        write_frontend(&mut self.stream, b'E', &execute)
    }

    fn read_query_cycle_batches(
        &mut self,
        state: &mut QueryStreamState,
        batch_rows: usize,
        on_event: &mut impl FnMut(PgQueryEvent) -> Result<()>,
    ) -> Result<bool> {
        let mut pending_error = None;
        let mut callback_error = None;
        let mut rows = Vec::with_capacity(batch_rows);
        let mut suspended = false;
        loop {
            let message = read_backend(&mut self.stream)?;
            match message.tag {
                b'T' => {
                    state.columns = decode_row_description(&message.payload)?;
                    deliver_query_event(
                        on_event,
                        &mut callback_error,
                        PgQueryEvent::Schema(state.columns.clone()),
                    );
                }
                b'D' => {
                    rows.push(decode_data_row(&message.payload, state.columns.len())?);
                    state.row_count = state.row_count.saturating_add(1);
                    if rows.len() == batch_rows {
                        deliver_query_event(
                            on_event,
                            &mut callback_error,
                            PgQueryEvent::Batch(std::mem::take(&mut rows)),
                        );
                        rows = Vec::with_capacity(batch_rows);
                    }
                }
                b'C' => {
                    flush_query_rows(on_event, &mut callback_error, &mut rows);
                    let tag = decode_only_cstring(&message.payload, "CommandComplete")?;
                    state.command_tags.push(tag.clone());
                    deliver_query_event(on_event, &mut callback_error, PgQueryEvent::Complete(tag));
                }
                b'E' => pending_error = Some(decode_error(&message.payload)),
                b'A' => {
                    let notification = decode_notification(&message.payload)?;
                    deliver_query_event(
                        on_event,
                        &mut callback_error,
                        PgQueryEvent::Notification(notification),
                    );
                }
                b'N' => {
                    flush_query_rows(on_event, &mut callback_error, &mut rows);
                    let notice = decode_notice(&message.payload)?;
                    deliver_query_event(
                        on_event,
                        &mut callback_error,
                        PgQueryEvent::Notice(notice),
                    );
                }
                b's' => suspended = true,
                b'Z' => {
                    self.transaction_status = decode_ready_status(&message.payload)?;
                    flush_query_rows(on_event, &mut callback_error, &mut rows);
                    if let Some(error) = pending_error {
                        return Err(error);
                    }
                    return callback_error.map_or(Ok(suspended), Err);
                }
                b'1' | b'2' | b'3' | b'I' | b'n' => {}
                b'H' | b'G' | b'd' | b'c' => {
                    return Err(DbError::new(
                        "0A000",
                        "use the dedicated COPY client methods for COPY streams",
                    ));
                }
                other => {
                    return Err(protocol(format!(
                        "unexpected backend message 0x{other:02x} during query"
                    )));
                }
            }
        }
    }
}

fn decode_ready_status(payload: &[u8]) -> Result<PgTransactionStatus> {
    match payload {
        [b'I'] => Ok(PgTransactionStatus::Idle),
        [b'T'] => Ok(PgTransactionStatus::InTransaction),
        [b'E'] => Ok(PgTransactionStatus::FailedTransaction),
        [_] => Err(protocol("ReadyForQuery transaction status is invalid")),
        _ => Err(protocol("ReadyForQuery payload length is invalid")),
    }
}

#[derive(Default)]
struct QueryStreamState {
    columns: Vec<String>,
    command_tags: Vec<String>,
    row_count: u64,
}

impl QueryStreamState {
    fn into_summary(self) -> QuerySummary {
        QuerySummary {
            columns: self.columns,
            command_tags: self.command_tags,
            row_count: self.row_count,
        }
    }
}

fn validate_batch_rows(batch_rows: usize) -> Result<()> {
    if batch_rows == 0 {
        return Err(DbError::new(
            "22023",
            "PostgreSQL client batch size must be positive",
        ));
    }
    Ok(())
}

fn collect_query_event(result: &mut QueryResult, event: PgQueryEvent) {
    match event {
        PgQueryEvent::Schema(columns) => result.columns = columns,
        PgQueryEvent::Batch(rows) => result.rows.extend(rows),
        PgQueryEvent::Notice(notice) => result.notices.push(notice),
        PgQueryEvent::Complete(tag) => result.command_tags.push(tag),
        PgQueryEvent::Notification(notification) => result.notifications.push(notification),
    }
}

fn deliver_query_event(
    on_event: &mut impl FnMut(PgQueryEvent) -> Result<()>,
    callback_error: &mut Option<DbError>,
    event: PgQueryEvent,
) {
    if callback_error.is_none()
        && let Err(error) = on_event(event)
    {
        *callback_error = Some(error);
    }
}

fn flush_query_rows(
    on_event: &mut impl FnMut(PgQueryEvent) -> Result<()>,
    callback_error: &mut Option<DbError>,
    rows: &mut Vec<Vec<Option<String>>>,
) {
    if !rows.is_empty() {
        deliver_query_event(
            on_event,
            callback_error,
            PgQueryEvent::Batch(std::mem::take(rows)),
        );
    }
}

impl PgCancelToken {
    pub fn cancel(&self) -> Result<()> {
        let mut stream = TcpStream::connect_timeout(&self.address, CLIENT_TIMEOUT)
            .map_err(|error| io_error("failed to open cancellation connection", error))?;
        let mut packet = Vec::with_capacity(16);
        packet.extend_from_slice(&16_u32.to_be_bytes());
        packet.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        packet.extend_from_slice(&self.process_id.to_be_bytes());
        packet.extend_from_slice(&self.secret_key.to_be_bytes());
        stream
            .write_all(&packet)
            .map_err(|error| io_error("failed to send cancellation request", error))
    }
}

fn validate_copy_table(table: &str) -> Result<()> {
    if table.is_empty()
        || !table
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.'))
    {
        return Err(DbError::new(
            "22023",
            "COPY table must be an unquoted schema/table name",
        ));
    }
    Ok(())
}

fn decode_copy_response(payload: &[u8]) -> Result<usize> {
    if payload.len() < 3 {
        return Err(protocol("COPY response is truncated"));
    }
    if !matches!(payload[0], 0 | 1) {
        return Err(protocol("COPY overall format must be text or binary"));
    }
    let columns = usize::from(u16::from_be_bytes(
        payload[1..3].try_into().expect("checked COPY count"),
    ));
    let expected = 3_usize
        .checked_add(
            columns
                .checked_mul(2)
                .ok_or_else(|| protocol("COPY column count overflowed"))?,
        )
        .ok_or_else(|| protocol("COPY response length overflowed"))?;
    if payload.len() != expected {
        return Err(protocol("COPY response column formats are truncated"));
    }
    if payload[3..].chunks_exact(2).any(|format| {
        !matches!(
            i16::from_be_bytes(format.try_into().expect("two bytes")),
            0 | 1
        )
    }) {
        return Err(protocol("COPY column format must be text or binary"));
    }
    Ok(columns)
}

fn write_startup(stream: &mut TcpStream, config: &ClientConfig) -> Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
    for (name, value) in [
        ("user", config.user.as_str()),
        ("database", config.database.as_str()),
        ("application_name", config.application_name.as_str()),
        ("client_encoding", "UTF8"),
    ] {
        push_cstring(&mut payload, name)?;
        push_cstring(&mut payload, value)?;
    }
    if let Some(query_memory_bytes) = config.query_memory_bytes {
        push_cstring(&mut payload, "ordadb_query_memory_bytes")?;
        push_cstring(&mut payload, &query_memory_bytes.to_string())?;
    }
    payload.push(0);
    let length = u32::try_from(payload.len() + 4)
        .map_err(|_| protocol("startup packet length overflowed"))?;
    stream
        .write_all(&length.to_be_bytes())
        .and_then(|()| stream.write_all(&payload))
        .and_then(|()| stream.flush())
        .map_err(|error| io_error("failed to send startup packet", error))
}

struct ClientScram<'a> {
    password: &'a [u8],
    client_nonce: String,
    client_first_bare: String,
    expected_server_signature: Option<[u8; 32]>,
}

impl<'a> ClientScram<'a> {
    fn new(username: &'a str, password: &'a [u8]) -> Self {
        let mut nonce = [0_u8; 18];
        OsRng.fill_bytes(&mut nonce);
        let client_nonce = URL_SAFE_NO_PAD.encode(nonce);
        let escaped = username.replace('=', "=3D").replace(',', "=2C");
        Self {
            password,
            client_first_bare: format!("n={escaped},r={client_nonce}"),
            client_nonce,
            expected_server_signature: None,
        }
    }

    fn handle_authentication(&mut self, stream: &mut TcpStream, payload: &[u8]) -> Result<()> {
        if payload.len() < 4 {
            return Err(protocol("Authentication message is truncated"));
        }
        let code = u32::from_be_bytes(payload[..4].try_into().expect("checked"));
        let data = &payload[4..];
        match code {
            0 => Ok(()),
            10 => {
                if !data
                    .split(|byte| *byte == 0)
                    .any(|mechanism| mechanism == b"SCRAM-SHA-256")
                {
                    return Err(DbError::new("0A000", "server does not offer SCRAM-SHA-256"));
                }
                let client_first = format!("n,,{}", self.client_first_bare);
                let mut initial = Vec::new();
                push_cstring(&mut initial, "SCRAM-SHA-256")?;
                initial.extend_from_slice(
                    &i32::try_from(client_first.len())
                        .map_err(|_| protocol("SCRAM initial response is too long"))?
                        .to_be_bytes(),
                );
                initial.extend_from_slice(client_first.as_bytes());
                write_frontend(stream, b'p', &initial)
            }
            11 => self.continue_scram(stream, data),
            12 => {
                let value = std::str::from_utf8(data)
                    .map_err(|_| protocol("SCRAM server-final is not UTF-8"))?;
                let signature = value
                    .strip_prefix("v=")
                    .ok_or_else(|| protocol("SCRAM server-final has no verifier"))?;
                let signature = STANDARD
                    .decode(signature)
                    .map_err(|_| protocol("SCRAM server verifier is not base64"))?;
                let expected = self
                    .expected_server_signature
                    .ok_or_else(|| protocol("SCRAM server-final arrived before continuation"))?;
                if signature.len() != expected.len()
                    || !bool::from(signature.as_slice().ct_eq(&expected))
                {
                    return Err(DbError::new("28000", "SCRAM server proof is invalid"));
                }
                Ok(())
            }
            other => Err(DbError::new(
                "0A000",
                format!("authentication method {other} is unsupported"),
            )),
        }
    }

    fn continue_scram(&mut self, stream: &mut TcpStream, data: &[u8]) -> Result<()> {
        let server_first =
            std::str::from_utf8(data).map_err(|_| protocol("SCRAM server-first is not UTF-8"))?;
        let attributes = attributes(server_first)?;
        let nonce = attributes
            .get(&'r')
            .copied()
            .ok_or_else(|| protocol("SCRAM server nonce is missing"))?;
        if !nonce.starts_with(&self.client_nonce) {
            return Err(protocol("SCRAM server nonce does not extend client nonce"));
        }
        let salt = STANDARD
            .decode(
                attributes
                    .get(&'s')
                    .copied()
                    .ok_or_else(|| protocol("SCRAM salt is missing"))?,
            )
            .map_err(|_| protocol("SCRAM salt is not base64"))?;
        let iterations: u32 = attributes
            .get(&'i')
            .copied()
            .ok_or_else(|| protocol("SCRAM iteration count is missing"))?
            .parse()
            .map_err(|_| protocol("SCRAM iteration count is invalid"))?;
        if !(4096..=1_000_000).contains(&iterations) {
            return Err(protocol("SCRAM iteration count is outside safe bounds"));
        }

        let client_final_without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!(
            "{},{server_first},{client_final_without_proof}",
            self.client_first_bare
        );
        let mut salted_password = Zeroizing::new([0_u8; 32]);
        pbkdf2_hmac::<Sha256>(self.password, &salt, iterations, salted_password.as_mut());
        let client_key = hmac_sha256(&salted_password[..], b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature)
            .map(|(key, signature)| key ^ signature)
            .collect();
        let server_key = hmac_sha256(&salted_password[..], b"Server Key");
        self.expected_server_signature = Some(hmac_sha256(&server_key, auth_message.as_bytes()));
        let final_message = format!("{client_final_without_proof},p={}", STANDARD.encode(proof));
        write_frontend(stream, b'p', final_message.as_bytes())
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of every size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn attributes(value: &str) -> Result<std::collections::BTreeMap<char, &str>> {
    let mut attributes = std::collections::BTreeMap::new();
    for part in value.split(',') {
        let bytes = part.as_bytes();
        if bytes.len() < 3 || bytes[1] != b'=' {
            return Err(protocol("SCRAM attribute is malformed"));
        }
        let key = char::from(bytes[0]);
        if attributes.insert(key, &part[2..]).is_some() {
            return Err(protocol("SCRAM attribute is duplicated"));
        }
    }
    Ok(attributes)
}
