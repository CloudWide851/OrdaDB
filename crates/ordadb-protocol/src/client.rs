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

use ordadb_types::{DbError, Result};

use crate::codec::{CANCEL_REQUEST_CODE, PROTOCOL_VERSION_3, io_error, protocol};

const CLIENT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub address: SocketAddr,
    pub user: String,
    pub database: String,
    pub password: Zeroizing<String>,
    pub application_name: String,
}

pub struct PgClient {
    stream: TcpStream,
    address: SocketAddr,
    process_id: u32,
    secret_key: u32,
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CopyOutResult {
    pub columns: usize,
    pub data: Vec<u8>,
    pub command_tag: String,
}

impl PgClient {
    pub fn connect(config: ClientConfig) -> Result<Self> {
        let mut stream = TcpStream::connect_timeout(&config.address, CLIENT_TIMEOUT)
            .map_err(|error| io_error("failed to connect to OrdaDB", error))?;
        stream
            .set_read_timeout(Some(CLIENT_TIMEOUT))
            .map_err(|error| io_error("failed to set client read timeout", error))?;
        stream
            .set_write_timeout(Some(CLIENT_TIMEOUT))
            .map_err(|error| io_error("failed to set client write timeout", error))?;
        write_startup(&mut stream, &config)?;

        let mut process_id = 0;
        let mut secret_key = 0;
        let mut scram = ClientScram::new(&config.user, config.password.as_bytes());
        loop {
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
                b'Z' => break,
                b'S' | b'N' => {}
                other => {
                    return Err(protocol(format!(
                        "unexpected backend message 0x{other:02x} during startup"
                    )));
                }
            }
        }
        if process_id == 0 {
            return Err(protocol("server did not send BackendKeyData"));
        }
        Ok(Self {
            stream,
            address: config.address,
            process_id,
            secret_key,
        })
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let mut payload = Vec::with_capacity(sql.len() + 1);
        push_cstring(&mut payload, sql)?;
        write_frontend(&mut self.stream, b'Q', &payload)?;
        self.stream
            .flush()
            .map_err(|error| io_error("failed to flush query", error))?;
        let mut result = QueryResult::default();
        let suspended = self.read_query_cycle(&mut result)?;
        if suspended {
            return Err(protocol("Simple Query unexpectedly suspended a portal"));
        }
        Ok(result)
    }

    pub fn query_prepared(
        &mut self,
        sql: &str,
        parameter_oids: &[u32],
        parameters: &[Option<Vec<u8>>],
        fetch_rows: u32,
    ) -> Result<QueryResult> {
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

        let mut result = QueryResult::default();
        while self.read_query_cycle(&mut result)? {
            self.write_execute(fetch_rows)?;
            write_frontend(&mut self.stream, b'S', &[])?;
        }
        Ok(result)
    }

    pub fn copy_to_stdout(&mut self, table: &str) -> Result<CopyOutResult> {
        validate_copy_table(table)?;
        self.write_query(&format!("COPY {table} TO STDOUT"))?;
        let mut result = CopyOutResult::default();
        let mut pending_error = None;
        loop {
            let message = read_backend(&mut self.stream)?;
            match message.tag {
                b'H' => result.columns = decode_copy_response(&message.payload)?,
                b'd' => result.data.extend_from_slice(&message.payload),
                b'c' | b'N' | b'I' => {}
                b'C' => {
                    result.command_tag = decode_only_cstring(&message.payload, "CommandComplete")?;
                }
                b'E' => pending_error = Some(decode_error(&message.payload)),
                b'Z' => return pending_error.map_or(Ok(result), Err),
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
        self.write_query(&format!("COPY {table} FROM STDIN"))?;
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
                b'N' | b'I' => {}
                b'Z' => {
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

    fn read_query_cycle(&mut self, result: &mut QueryResult) -> Result<bool> {
        let mut pending_error = None;
        let mut suspended = false;
        loop {
            let message = read_backend(&mut self.stream)?;
            match message.tag {
                b'T' => result.columns = decode_row_description(&message.payload)?,
                b'D' => result
                    .rows
                    .push(decode_data_row(&message.payload, result.columns.len())?),
                b'C' => result
                    .command_tags
                    .push(decode_only_cstring(&message.payload, "CommandComplete")?),
                b'E' => pending_error = Some(decode_error(&message.payload)),
                b's' => suspended = true,
                b'Z' => return pending_error.map_or(Ok(suspended), Err),
                b'1' | b'2' | b'3' | b'N' | b'I' | b'n' => {}
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

struct BackendMessage {
    tag: u8,
    payload: Vec<u8>,
}

fn read_backend(stream: &mut TcpStream) -> Result<BackendMessage> {
    let mut tag = [0_u8; 1];
    stream
        .read_exact(&mut tag)
        .map_err(|error| io_error("failed to read backend tag", error))?;
    let mut length = [0_u8; 4];
    stream
        .read_exact(&mut length)
        .map_err(|error| io_error("failed to read backend length", error))?;
    let length = u32::from_be_bytes(length);
    let length =
        usize::try_from(length).map_err(|_| protocol("backend length cannot fit in memory"))?;
    if !(4..=CLIENT_MAX_FRAME_BYTES).contains(&length) {
        return Err(protocol(
            "backend frame length is outside configured bounds",
        ));
    }
    let mut payload = vec![0_u8; length - 4];
    stream
        .read_exact(&mut payload)
        .map_err(|error| io_error("failed to read backend payload", error))?;
    Ok(BackendMessage {
        tag: tag[0],
        payload,
    })
}

fn write_frontend(stream: &mut TcpStream, tag: u8, payload: &[u8]) -> Result<()> {
    let length = u32::try_from(payload.len() + 4)
        .map_err(|_| protocol("frontend frame length overflowed"))?;
    stream
        .write_all(&[tag])
        .and_then(|()| stream.write_all(&length.to_be_bytes()))
        .and_then(|()| stream.write_all(payload))
        .and_then(|()| stream.flush())
        .map_err(|error| io_error("failed to send frontend message", error))
}

fn decode_row_description(payload: &[u8]) -> Result<Vec<String>> {
    let mut cursor = SliceCursor::new(payload);
    let count = usize::from(cursor.u16()?);
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        columns.push(cursor.cstring()?);
        cursor.skip(18)?;
    }
    cursor.finish()?;
    Ok(columns)
}

fn decode_data_row(payload: &[u8], expected: usize) -> Result<Vec<Option<String>>> {
    let mut cursor = SliceCursor::new(payload);
    let count = usize::from(cursor.u16()?);
    if count != expected {
        return Err(protocol("DataRow width does not match RowDescription"));
    }
    let mut row = Vec::with_capacity(count);
    for _ in 0..count {
        let length = cursor.i32()?;
        if length == -1 {
            row.push(None);
        } else {
            let length =
                usize::try_from(length).map_err(|_| protocol("DataRow length is below -1"))?;
            let value = std::str::from_utf8(cursor.bytes(length)?)
                .map_err(|_| DbError::new("22021", "DataRow text is not UTF-8"))?
                .to_owned();
            row.push(Some(value));
        }
    }
    cursor.finish()?;
    Ok(row)
}

fn decode_error(payload: &[u8]) -> DbError {
    let mut fields = std::collections::BTreeMap::new();
    let mut offset = 0;
    while offset < payload.len() && payload[offset] != 0 {
        let tag = payload[offset];
        offset += 1;
        let Some(end) = payload[offset..].iter().position(|byte| *byte == 0) else {
            return protocol("ErrorResponse field is not terminated");
        };
        let value = String::from_utf8_lossy(&payload[offset..offset + end]).into_owned();
        fields.insert(tag, value);
        offset += end + 1;
    }
    let mut error = DbError::new(
        fields.get(&b'C').cloned().unwrap_or_else(|| "XX000".into()),
        fields
            .get(&b'M')
            .cloned()
            .unwrap_or_else(|| "unknown server error".into()),
    );
    if let Some(detail) = fields.get(&b'D') {
        error = error.with_detail(detail.clone());
    }
    if let Some(hint) = fields.get(&b'H') {
        error = error.with_hint(hint.clone());
    }
    error
}

fn decode_only_cstring(payload: &[u8], context: &str) -> Result<String> {
    if payload.last() != Some(&0) || payload[..payload.len().saturating_sub(1)].contains(&0) {
        return Err(protocol(format!("{context} is not one C string")));
    }
    std::str::from_utf8(&payload[..payload.len() - 1])
        .map(str::to_owned)
        .map_err(|_| protocol(format!("{context} is not UTF-8")))
}

fn push_cstring(target: &mut Vec<u8>, value: &str) -> Result<()> {
    if value.as_bytes().contains(&0) {
        return Err(protocol("client string contains NUL"));
    }
    target.extend_from_slice(value.as_bytes());
    target.push(0);
    Ok(())
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SliceCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| protocol("backend message is truncated"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn skip(&mut self, length: usize) -> Result<()> {
        self.bytes(length).map(|_| ())
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.bytes(2)?.try_into().expect("checked"),
        ))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.bytes(4)?.try_into().expect("checked"),
        ))
    }

    fn cstring(&mut self) -> Result<String> {
        let remaining = &self.bytes[self.offset..];
        let end = remaining
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| protocol("backend string is not terminated"))?;
        let value = std::str::from_utf8(&remaining[..end])
            .map_err(|_| protocol("backend string is not UTF-8"))?
            .to_owned();
        self.offset += end + 1;
        Ok(value)
    }

    fn finish(&self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(protocol("backend message contains trailing bytes"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_response_preserves_sqlstate_message_and_detail() {
        let payload = b"SERROR\0C42P01\0Mmissing table\0Dpublic.items\0\0";
        let error = decode_error(payload);
        assert_eq!(error.sql_state, "42P01");
        assert_eq!(error.message, "missing table");
        assert_eq!(error.detail.as_deref(), Some("public.items"));
    }

    #[test]
    fn data_row_decoder_checks_width_and_utf8() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1_u16.to_be_bytes());
        payload.extend_from_slice(&2_i32.to_be_bytes());
        payload.extend_from_slice(b"42");
        assert_eq!(
            decode_data_row(&payload, 1).expect("row"),
            vec![Some("42".into())]
        );
        assert!(decode_data_row(&payload, 2).is_err());
    }
}
