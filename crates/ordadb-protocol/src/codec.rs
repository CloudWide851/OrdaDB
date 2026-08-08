use std::{
    collections::BTreeSet,
    io::{Read, Write},
};

use ordadb_types::{DbError, DbNotice, Result};

pub const PROTOCOL_VERSION_3: u32 = 196_608;
pub const SSL_REQUEST_CODE: u32 = 80_877_103;
pub const CANCEL_REQUEST_CODE: u32 = 80_877_102;
pub const GSSENC_REQUEST_CODE: u32 = 80_877_104;
pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_NAME_BYTES: usize = 1024;
pub const DEFAULT_MAX_PARAMETERS: usize = 32_767;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupPacket {
    Startup(Vec<(String, String)>),
    SslRequest,
    GssEncRequest,
    CancelRequest { process_id: u32, secret_key: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendMessage {
    Query(String),
    Parse {
        name: String,
        sql: String,
        parameter_oids: Vec<u32>,
    },
    Bind {
        portal: String,
        statement: String,
        parameter_formats: Vec<i16>,
        parameters: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    Describe {
        kind: u8,
        name: String,
    },
    Execute {
        portal: String,
        max_rows: u32,
    },
    Close {
        kind: u8,
        name: String,
    },
    Sync,
    Flush,
    Terminate,
    Password(Vec<u8>),
    CopyData(Vec<u8>),
    CopyDone,
    CopyFail(String),
}

pub fn read_startup<R: Read>(reader: &mut R, max_frame_bytes: usize) -> Result<StartupPacket> {
    let length = read_u32(reader, "startup packet length")?;
    let length = checked_frame_length(length, max_frame_bytes)?;
    if length < 8 {
        return Err(protocol("startup packet is shorter than 8 bytes"));
    }
    let mut payload = vec![0_u8; length - 4];
    read_exact(reader, &mut payload, "startup packet")?;
    let mut cursor = Cursor::new(&payload);
    let code = cursor.u32("startup protocol code")?;
    match code {
        SSL_REQUEST_CODE => {
            cursor.finish("SSL request")?;
            Ok(StartupPacket::SslRequest)
        }
        GSSENC_REQUEST_CODE => {
            cursor.finish("GSS encryption request")?;
            Ok(StartupPacket::GssEncRequest)
        }
        CANCEL_REQUEST_CODE => {
            let process_id = cursor.u32("cancel process ID")?;
            let secret_key = cursor.u32("cancel secret key")?;
            cursor.finish("cancel request")?;
            Ok(StartupPacket::CancelRequest {
                process_id,
                secret_key,
            })
        }
        PROTOCOL_VERSION_3 => {
            let mut parameters = Vec::new();
            let mut names = BTreeSet::new();
            let mut terminated = false;
            while !cursor.remaining().is_empty() {
                if cursor.remaining()[0] == 0 {
                    cursor.byte("startup terminator")?;
                    cursor.finish("startup packet")?;
                    terminated = true;
                    break;
                }
                if parameters.len() >= 64 {
                    return Err(protocol("startup parameter count exceeds 64"));
                }
                let key = cursor.cstring("startup parameter name", DEFAULT_MAX_NAME_BYTES)?;
                let value = cursor.cstring("startup parameter value", DEFAULT_MAX_NAME_BYTES)?;
                if !names.insert(key.clone()) {
                    return Err(protocol(format!(
                        "startup parameter {key} is specified more than once"
                    )));
                }
                parameters.push((key, value));
            }
            if !terminated {
                return Err(protocol("startup packet has no terminating NUL byte"));
            }
            Ok(StartupPacket::Startup(parameters))
        }
        unsupported => Err(DbError::new(
            "0A000",
            format!("PostgreSQL protocol version {unsupported} is unsupported"),
        )),
    }
}

pub fn read_frontend<R: Read>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<Option<FrontendMessage>> {
    let mut tag = [0_u8; 1];
    match reader.read(&mut tag) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("one-byte read returned more than one byte"),
        Err(error) => return Err(io_error("failed to read frontend message tag", error)),
    }
    let length = read_u32(reader, "frontend message length")?;
    let length = checked_frame_length(length, max_frame_bytes)?;
    let mut payload = vec![0_u8; length - 4];
    read_exact(reader, &mut payload, "frontend message payload")?;
    decode_frontend(tag[0], payload).map(Some)
}

fn decode_frontend(tag: u8, payload: Vec<u8>) -> Result<FrontendMessage> {
    let mut cursor = Cursor::new(&payload);
    let message = match tag {
        b'Q' => FrontendMessage::Query(cursor.only_cstring("query")?),
        b'P' => {
            let name = cursor.cstring("prepared statement name", DEFAULT_MAX_NAME_BYTES)?;
            let sql = cursor.cstring("prepared statement SQL", DEFAULT_MAX_FRAME_BYTES)?;
            let count = cursor.count("parameter type count")?;
            let mut parameter_oids = Vec::with_capacity(count);
            for _ in 0..count {
                parameter_oids.push(cursor.u32("parameter type OID")?);
            }
            cursor.finish("Parse message")?;
            FrontendMessage::Parse {
                name,
                sql,
                parameter_oids,
            }
        }
        b'B' => {
            let portal = cursor.cstring("portal name", DEFAULT_MAX_NAME_BYTES)?;
            let statement = cursor.cstring("prepared statement name", DEFAULT_MAX_NAME_BYTES)?;
            let format_count = cursor.count("parameter format count")?;
            let mut parameter_formats = Vec::with_capacity(format_count);
            for _ in 0..format_count {
                parameter_formats.push(cursor.i16("parameter format")?);
            }
            let parameter_count = cursor.count("parameter count")?;
            let mut parameters = Vec::with_capacity(parameter_count);
            for _ in 0..parameter_count {
                let length = cursor.i32("parameter length")?;
                if length == -1 {
                    parameters.push(None);
                    continue;
                }
                let length = usize::try_from(length)
                    .map_err(|_| protocol("parameter length is below -1"))?;
                parameters.push(Some(cursor.bytes(length, "parameter value")?.to_vec()));
            }
            let result_count = cursor.count("result format count")?;
            let mut result_formats = Vec::with_capacity(result_count);
            for _ in 0..result_count {
                result_formats.push(cursor.i16("result format")?);
            }
            cursor.finish("Bind message")?;
            FrontendMessage::Bind {
                portal,
                statement,
                parameter_formats,
                parameters,
                result_formats,
            }
        }
        b'D' => {
            let kind = cursor.byte("Describe kind")?;
            let name = cursor.cstring("Describe name", DEFAULT_MAX_NAME_BYTES)?;
            cursor.finish("Describe message")?;
            FrontendMessage::Describe { kind, name }
        }
        b'E' => {
            let portal = cursor.cstring("portal name", DEFAULT_MAX_NAME_BYTES)?;
            let max_rows = cursor.u32("Execute max rows")?;
            cursor.finish("Execute message")?;
            FrontendMessage::Execute { portal, max_rows }
        }
        b'C' => {
            let kind = cursor.byte("Close kind")?;
            let name = cursor.cstring("Close name", DEFAULT_MAX_NAME_BYTES)?;
            cursor.finish("Close message")?;
            FrontendMessage::Close { kind, name }
        }
        b'S' => {
            cursor.finish("Sync message")?;
            FrontendMessage::Sync
        }
        b'H' => {
            cursor.finish("Flush message")?;
            FrontendMessage::Flush
        }
        b'X' => {
            cursor.finish("Terminate message")?;
            FrontendMessage::Terminate
        }
        b'p' => FrontendMessage::Password(payload),
        b'd' => FrontendMessage::CopyData(payload),
        b'c' => {
            cursor.finish("CopyDone message")?;
            FrontendMessage::CopyDone
        }
        b'f' => FrontendMessage::CopyFail(cursor.only_cstring("COPY failure")?),
        other => {
            return Err(protocol(format!(
                "frontend message type 0x{other:02x} is unsupported"
            )));
        }
    };
    Ok(message)
}

pub fn write_message<W: Write>(writer: &mut W, tag: u8, payload: &[u8]) -> Result<()> {
    let length = payload
        .len()
        .checked_add(4)
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| protocol("backend message length overflowed"))?;
    write_all(writer, &[tag], "backend message tag")?;
    write_all(writer, &length.to_be_bytes(), "backend message length")?;
    write_all(writer, payload, "backend message payload")
}

pub fn write_authentication<W: Write>(writer: &mut W, code: u32, data: &[u8]) -> Result<()> {
    let mut payload = Vec::with_capacity(4 + data.len());
    payload.extend_from_slice(&code.to_be_bytes());
    payload.extend_from_slice(data);
    write_message(writer, b'R', &payload)
}

pub fn write_parameter_status<W: Write>(writer: &mut W, name: &str, value: &str) -> Result<()> {
    let mut payload = Vec::new();
    push_cstring(&mut payload, name)?;
    push_cstring(&mut payload, value)?;
    write_message(writer, b'S', &payload)
}

pub fn write_backend_key<W: Write>(writer: &mut W, process_id: u32, secret_key: u32) -> Result<()> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&process_id.to_be_bytes());
    payload.extend_from_slice(&secret_key.to_be_bytes());
    write_message(writer, b'K', &payload)
}

pub fn write_ready<W: Write>(writer: &mut W, status: u8) -> Result<()> {
    write_message(writer, b'Z', &[status])
}

pub fn write_error<W: Write>(writer: &mut W, error: &DbError) -> Result<()> {
    let mut payload = Vec::new();
    push_error_field(&mut payload, b'S', "ERROR")?;
    push_error_field(&mut payload, b'V', "ERROR")?;
    push_error_field(&mut payload, b'C', &error.sql_state)?;
    push_error_field(&mut payload, b'M', &error.message)?;
    if let Some(detail) = &error.detail {
        push_error_field(&mut payload, b'D', detail)?;
    }
    if let Some(hint) = &error.hint {
        push_error_field(&mut payload, b'H', hint)?;
    }
    if let Some(position) = error.position {
        push_error_field(&mut payload, b'P', &position.to_string())?;
    }
    let identity = error.object_identity.as_deref();
    push_optional_error_field(
        &mut payload,
        b's',
        identity.and_then(|identity| identity.schema_name.as_deref()),
    )?;
    push_optional_error_field(
        &mut payload,
        b't',
        identity.and_then(|identity| identity.table_name.as_deref()),
    )?;
    push_optional_error_field(
        &mut payload,
        b'c',
        identity.and_then(|identity| identity.column_name.as_deref()),
    )?;
    push_optional_error_field(
        &mut payload,
        b'd',
        identity.and_then(|identity| identity.data_type_name.as_deref()),
    )?;
    push_optional_error_field(
        &mut payload,
        b'n',
        identity.and_then(|identity| identity.constraint_name.as_deref()),
    )?;
    push_error_field(
        &mut payload,
        b'W',
        &format!("OrdaDB query_id={}", error.query_id),
    )?;
    payload.push(0);
    write_message(writer, b'E', &payload)
}

pub fn write_notice<W: Write>(writer: &mut W, notice: &DbNotice) -> Result<()> {
    let mut payload = Vec::new();
    push_error_field(&mut payload, b'S', "NOTICE")?;
    push_error_field(&mut payload, b'V', "NOTICE")?;
    push_error_field(&mut payload, b'C', &notice.sql_state)?;
    push_error_field(&mut payload, b'M', &notice.message)?;
    push_optional_error_field(&mut payload, b'D', notice.detail.as_deref())?;
    push_optional_error_field(&mut payload, b'H', notice.hint.as_deref())?;
    if let Some(position) = notice.position {
        push_error_field(&mut payload, b'P', &position.to_string())?;
    }
    let identity = notice.object_identity.as_deref();
    push_optional_error_field(
        &mut payload,
        b's',
        identity.and_then(|identity| identity.schema_name.as_deref()),
    )?;
    push_optional_error_field(
        &mut payload,
        b't',
        identity.and_then(|identity| identity.table_name.as_deref()),
    )?;
    push_optional_error_field(
        &mut payload,
        b'c',
        identity.and_then(|identity| identity.column_name.as_deref()),
    )?;
    push_optional_error_field(
        &mut payload,
        b'd',
        identity.and_then(|identity| identity.data_type_name.as_deref()),
    )?;
    push_optional_error_field(
        &mut payload,
        b'n',
        identity.and_then(|identity| identity.constraint_name.as_deref()),
    )?;
    payload.push(0);
    write_message(writer, b'N', &payload)
}

pub fn write_command_complete<W: Write>(writer: &mut W, tag: &str) -> Result<()> {
    let mut payload = Vec::new();
    push_cstring(&mut payload, tag)?;
    write_message(writer, b'C', &payload)
}

pub fn write_empty_query<W: Write>(writer: &mut W) -> Result<()> {
    write_message(writer, b'I', &[])
}

pub fn write_parse_complete<W: Write>(writer: &mut W) -> Result<()> {
    write_message(writer, b'1', &[])
}

pub fn write_bind_complete<W: Write>(writer: &mut W) -> Result<()> {
    write_message(writer, b'2', &[])
}

pub fn write_close_complete<W: Write>(writer: &mut W) -> Result<()> {
    write_message(writer, b'3', &[])
}

pub fn write_no_data<W: Write>(writer: &mut W) -> Result<()> {
    write_message(writer, b'n', &[])
}

pub fn write_portal_suspended<W: Write>(writer: &mut W) -> Result<()> {
    write_message(writer, b's', &[])
}

pub fn write_parameter_description<W: Write>(writer: &mut W, oids: &[u32]) -> Result<()> {
    let count = u16::try_from(oids.len()).map_err(|_| protocol("parameter count exceeds u16"))?;
    let mut payload = Vec::with_capacity(2 + oids.len() * 4);
    payload.extend_from_slice(&count.to_be_bytes());
    for oid in oids {
        payload.extend_from_slice(&oid.to_be_bytes());
    }
    write_message(writer, b't', &payload)
}

pub fn push_cstring(target: &mut Vec<u8>, value: &str) -> Result<()> {
    if value.as_bytes().contains(&0) {
        return Err(protocol("string contains an embedded NUL byte"));
    }
    target.extend_from_slice(value.as_bytes());
    target.push(0);
    Ok(())
}

fn push_error_field(target: &mut Vec<u8>, tag: u8, value: &str) -> Result<()> {
    target.push(tag);
    push_cstring(target, value)
}

fn push_optional_error_field(target: &mut Vec<u8>, tag: u8, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        push_error_field(target, tag, value)?;
    }
    Ok(())
}

fn checked_frame_length(length: u32, max_frame_bytes: usize) -> Result<usize> {
    let length =
        usize::try_from(length).map_err(|_| protocol("message length cannot fit in memory"))?;
    if length < 4 || length > max_frame_bytes {
        return Err(protocol(format!(
            "message length {length} is outside 4..={max_frame_bytes}"
        )));
    }
    Ok(length)
}

fn read_u32<R: Read>(reader: &mut R, context: &str) -> Result<u32> {
    let mut bytes = [0_u8; 4];
    read_exact(reader, &mut bytes, context)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_exact<R: Read>(reader: &mut R, target: &mut [u8], context: &str) -> Result<()> {
    reader
        .read_exact(target)
        .map_err(|error| io_error(format!("failed to read {context}"), error))
}

fn write_all<W: Write>(writer: &mut W, bytes: &[u8], context: &str) -> Result<()> {
    writer
        .write_all(bytes)
        .map_err(|error| io_error(format!("failed to write {context}"), error))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.offset..]
    }

    fn bytes(&mut self, length: usize, context: &str) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| protocol(format!("{context} is truncated")))?;
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn byte(&mut self, context: &str) -> Result<u8> {
        Ok(self.bytes(1, context)?[0])
    }

    fn i16(&mut self, context: &str) -> Result<i16> {
        Ok(i16::from_be_bytes(
            self.bytes(2, context)?
                .try_into()
                .expect("checked two-byte slice"),
        ))
    }

    fn i32(&mut self, context: &str) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.bytes(4, context)?
                .try_into()
                .expect("checked four-byte slice"),
        ))
    }

    fn u32(&mut self, context: &str) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.bytes(4, context)?
                .try_into()
                .expect("checked four-byte slice"),
        ))
    }

    fn count(&mut self, context: &str) -> Result<usize> {
        let count = usize::from(u16::from_be_bytes(
            self.bytes(2, context)?
                .try_into()
                .expect("checked two-byte slice"),
        ));
        if count > DEFAULT_MAX_PARAMETERS {
            return Err(protocol(format!(
                "{context} {count} exceeds {DEFAULT_MAX_PARAMETERS}"
            )));
        }
        Ok(count)
    }

    fn cstring(&mut self, context: &str, max_bytes: usize) -> Result<String> {
        let remaining = self.remaining();
        let end = remaining
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| protocol(format!("{context} has no NUL terminator")))?;
        if end > max_bytes {
            return Err(protocol(format!("{context} exceeds {max_bytes} bytes")));
        }
        let value = std::str::from_utf8(&remaining[..end])
            .map_err(|_| protocol(format!("{context} is not valid UTF-8")))?
            .to_owned();
        self.offset += end + 1;
        Ok(value)
    }

    fn only_cstring(mut self, context: &str) -> Result<String> {
        let value = self.cstring(context, DEFAULT_MAX_FRAME_BYTES)?;
        self.finish(context)?;
        Ok(value)
    }

    fn finish(&self, context: &str) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(protocol(format!("{context} contains trailing bytes")));
        }
        Ok(())
    }
}

pub fn protocol(message: impl Into<String>) -> DbError {
    DbError::new("08P01", message).with_hint("close the connection and retry with protocol v3")
}

pub fn io_error(context: impl Into<String>, error: std::io::Error) -> DbError {
    DbError::new("08006", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontend(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![tag];
        bytes.extend_from_slice(
            &u32::try_from(payload.len() + 4)
                .expect("length")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(payload);
        bytes
    }

    fn startup(payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(
            &u32::try_from(payload.len() + 4)
                .expect("length")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn startup_and_query_frames_are_bounded_and_exact() {
        let mut startup_payload = Vec::new();
        startup_payload.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
        startup_payload.extend_from_slice(b"user\0dba\0\0");
        let mut startup = Vec::new();
        startup.extend_from_slice(
            &u32::try_from(startup_payload.len() + 4)
                .expect("length")
                .to_be_bytes(),
        );
        startup.extend_from_slice(&startup_payload);
        assert_eq!(
            read_startup(&mut startup.as_slice(), DEFAULT_MAX_FRAME_BYTES).expect("startup"),
            StartupPacket::Startup(vec![("user".into(), "dba".into())])
        );

        let query = frontend(b'Q', b"SELECT 1\0");
        assert_eq!(
            read_frontend(&mut query.as_slice(), DEFAULT_MAX_FRAME_BYTES).expect("query"),
            Some(FrontendMessage::Query("SELECT 1".into()))
        );
    }

    #[test]
    fn malformed_lengths_and_trailing_bytes_are_rejected() {
        let mut short = [0_u8, 0, 0, 3].as_slice();
        assert_eq!(
            read_startup(&mut short, DEFAULT_MAX_FRAME_BYTES)
                .expect_err("short")
                .sql_state,
            "08P01"
        );
        let sync = frontend(b'S', &[1]);
        assert_eq!(
            read_frontend(&mut sync.as_slice(), DEFAULT_MAX_FRAME_BYTES)
                .expect_err("trailing")
                .sql_state,
            "08P01"
        );
    }

    #[test]
    fn startup_requires_termination_unique_names_and_bounded_frames() {
        let mut missing_terminator = PROTOCOL_VERSION_3.to_be_bytes().to_vec();
        missing_terminator.extend_from_slice(b"user\0dba\0");
        assert_eq!(
            read_startup(
                &mut startup(&missing_terminator).as_slice(),
                DEFAULT_MAX_FRAME_BYTES,
            )
            .expect_err("missing startup terminator")
            .sql_state,
            "08P01"
        );

        let mut duplicate = PROTOCOL_VERSION_3.to_be_bytes().to_vec();
        duplicate.extend_from_slice(b"user\0dba\0user\0other\0\0");
        assert_eq!(
            read_startup(&mut startup(&duplicate).as_slice(), DEFAULT_MAX_FRAME_BYTES)
                .expect_err("duplicate startup parameter")
                .sql_state,
            "08P01"
        );

        let oversized_length = u32::try_from(DEFAULT_MAX_FRAME_BYTES + 1)
            .expect("bounded test length")
            .to_be_bytes();
        assert_eq!(
            read_startup(&mut oversized_length.as_slice(), DEFAULT_MAX_FRAME_BYTES,)
                .expect_err("oversized startup frame")
                .sql_state,
            "08P01"
        );
    }

    #[test]
    fn encryption_requests_are_exact_frames() {
        assert_eq!(
            read_startup(
                &mut startup(&GSSENC_REQUEST_CODE.to_be_bytes()).as_slice(),
                DEFAULT_MAX_FRAME_BYTES,
            )
            .expect("GSS encryption request"),
            StartupPacket::GssEncRequest
        );
        let mut ssl_with_trailing = SSL_REQUEST_CODE.to_be_bytes().to_vec();
        ssl_with_trailing.push(0);
        assert_eq!(
            read_startup(
                &mut startup(&ssl_with_trailing).as_slice(),
                DEFAULT_MAX_FRAME_BYTES,
            )
            .expect_err("SSLRequest trailing byte")
            .sql_state,
            "08P01"
        );
    }

    #[test]
    fn error_and_notice_responses_preserve_structured_fields() {
        let error = DbError::new("23505", "duplicate key")
            .with_detail("Key (id)=(1) already exists")
            .with_hint("Choose another id")
            .with_position(17)
            .with_schema_name("public")
            .with_table_name("items")
            .with_column_name("id")
            .with_data_type_name("int8")
            .with_constraint_name("items_pkey");
        let mut encoded = Vec::new();
        write_error(&mut encoded, &error).expect("encode error");
        assert_eq!(encoded[0], b'E');
        let payload = &encoded[5..];
        for field in [
            b"C23505\0".as_slice(),
            b"DKey (id)=(1) already exists\0".as_slice(),
            b"HChoose another id\0".as_slice(),
            b"P17\0".as_slice(),
            b"spublic\0".as_slice(),
            b"titems\0".as_slice(),
            b"cid\0".as_slice(),
            b"dint8\0".as_slice(),
            b"nitems_pkey\0".as_slice(),
        ] {
            assert!(payload.windows(field.len()).any(|window| window == field));
        }
        assert_eq!(payload.last(), Some(&0));

        let notice = DbNotice {
            sql_state: "00000".into(),
            message: "maintenance complete".into(),
            detail: Some("one table".into()),
            hint: None,
            position: Some(3),
            object_identity: Some(Box::new(ordadb_types::DbObjectIdentity {
                schema_name: Some("public".into()),
                table_name: Some("items".into()),
                column_name: None,
                data_type_name: None,
                constraint_name: None,
            })),
        };
        encoded.clear();
        write_notice(&mut encoded, &notice).expect("encode notice");
        assert_eq!(encoded[0], b'N');
        assert!(
            encoded[5..]
                .windows(b"VNOTICE\0".len())
                .any(|window| window == b"VNOTICE\0")
        );
        assert_eq!(encoded.last(), Some(&0));
    }
}
