
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

fn push_pending_notice(notices: &mut Vec<DbNotice>, notice: DbNotice) -> Result<()> {
    if notices.len() >= CLIENT_MAX_PENDING_NOTICES {
        return Err(DbError::new(
            "54000",
            "PostgreSQL client pending-notice limit exceeded",
        ));
    }
    notices.push(notice);
    Ok(())
}

fn decode_notification(payload: &[u8]) -> Result<PgNotification> {
    let mut cursor = SliceCursor::new(payload);
    let sender_process_id = cursor.u32()?;
    let channel = cursor.cstring()?;
    let payload = cursor.cstring()?;
    cursor.finish()?;
    Ok(PgNotification {
        sender_process_id,
        channel,
        payload,
    })
}

fn decode_error(payload: &[u8]) -> DbError {
    let fields = match decode_response_fields(payload, "ErrorResponse") {
        Ok(fields) => fields,
        Err(error) => return error,
    };
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
    if let Some(position) = fields
        .get(&b'P')
        .and_then(|position| position.parse::<usize>().ok())
    {
        error = error.with_position(position);
    }
    if let Some(name) = fields.get(&b's') {
        error = error.with_schema_name(name.clone());
    }
    if let Some(name) = fields.get(&b't') {
        error = error.with_table_name(name.clone());
    }
    if let Some(name) = fields.get(&b'c') {
        error = error.with_column_name(name.clone());
    }
    if let Some(name) = fields.get(&b'd') {
        error = error.with_data_type_name(name.clone());
    }
    if let Some(name) = fields.get(&b'n') {
        error = error.with_constraint_name(name.clone());
    }
    error
}

fn decode_notice(payload: &[u8]) -> Result<DbNotice> {
    let fields = decode_response_fields(payload, "NoticeResponse")?;
    let severity = match fields
        .get(&b'V')
        .or_else(|| fields.get(&b'S'))
        .map(String::as_str)
    {
        Some("INFO") => DbNoticeSeverity::Info,
        Some("NOTICE") | None => DbNoticeSeverity::Notice,
        Some("WARNING") => DbNoticeSeverity::Warning,
        Some(severity) => {
            return Err(protocol(format!(
                "NoticeResponse severity {severity} is not supported"
            )));
        }
    };
    let position = fields
        .get(&b'P')
        .map(|position| {
            position
                .parse::<usize>()
                .map_err(|_| protocol("NoticeResponse position is invalid"))
        })
        .transpose()?;
    let identity = DbObjectIdentity {
        schema_name: fields.get(&b's').cloned().map(String::into_boxed_str),
        table_name: fields.get(&b't').cloned().map(String::into_boxed_str),
        column_name: fields.get(&b'c').cloned().map(String::into_boxed_str),
        data_type_name: fields.get(&b'd').cloned().map(String::into_boxed_str),
        constraint_name: fields.get(&b'n').cloned().map(String::into_boxed_str),
    };
    let object_identity = (identity.schema_name.is_some()
        || identity.table_name.is_some()
        || identity.column_name.is_some()
        || identity.data_type_name.is_some()
        || identity.constraint_name.is_some())
    .then(|| Box::new(identity));
    Ok(DbNotice {
        severity,
        sql_state: fields.get(&b'C').cloned().unwrap_or_else(|| "00000".into()),
        message: fields
            .get(&b'M')
            .cloned()
            .unwrap_or_else(|| "server notice".into()),
        detail: fields.get(&b'D').cloned().map(String::into_boxed_str),
        hint: fields.get(&b'H').cloned().map(String::into_boxed_str),
        position,
        object_identity,
    })
}

fn decode_response_fields(
    payload: &[u8],
    context: &str,
) -> Result<std::collections::BTreeMap<u8, String>> {
    if payload.last() != Some(&0) {
        return Err(protocol(format!("{context} is not terminated")));
    }
    let mut fields = std::collections::BTreeMap::new();
    let mut offset = 0;
    while payload.get(offset) != Some(&0) {
        let tag = payload[offset];
        offset += 1;
        let Some(end) = payload[offset..].iter().position(|byte| *byte == 0) else {
            return Err(protocol(format!("{context} field is not terminated")));
        };
        let value = std::str::from_utf8(&payload[offset..offset + end])
            .map_err(|_| protocol(format!("{context} field is not UTF-8")))?
            .to_owned();
        fields.insert(tag, value);
        offset += end + 1;
    }
    if offset + 1 != payload.len() {
        return Err(protocol(format!("{context} contains trailing bytes")));
    }
    Ok(fields)
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

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
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
        let payload = b"SERROR\0C42P01\0Mmissing table\0Dpublic.items\0Hcheck the name\0P9\0spublic\0titems\0cid\0dint8\0nitems_pkey\0\0";
        let error = decode_error(payload);
        assert_eq!(error.sql_state, "42P01");
        assert_eq!(error.message, "missing table");
        assert_eq!(error.detail.as_deref(), Some("public.items"));
        assert_eq!(error.hint.as_deref(), Some("check the name"));
        assert_eq!(error.position, Some(9));
        let identity = error.object_identity.as_deref().expect("object identity");
        assert_eq!(identity.schema_name.as_deref(), Some("public"));
        assert_eq!(identity.table_name.as_deref(), Some("items"));
        assert_eq!(identity.column_name.as_deref(), Some("id"));
        assert_eq!(identity.data_type_name.as_deref(), Some("int8"));
        assert_eq!(identity.constraint_name.as_deref(), Some("items_pkey"));
    }

    #[test]
    fn notice_response_preserves_typed_severity_and_structured_fields() {
        let payload = b"SWARNING\0VWARNING\0C01000\0Mcareful\0Dcheck the statement\0Hreview the plan\0P7\0spublic\0titems\0\0";
        let notice = decode_notice(payload).expect("notice");
        assert_eq!(notice.severity, DbNoticeSeverity::Warning);
        assert_eq!(notice.sql_state, "01000");
        assert_eq!(notice.message, "careful");
        assert_eq!(notice.detail.as_deref(), Some("check the statement"));
        assert_eq!(notice.hint.as_deref(), Some("review the plan"));
        assert_eq!(notice.position, Some(7));
        let identity = notice.object_identity.as_deref().expect("object identity");
        assert_eq!(identity.schema_name.as_deref(), Some("public"));
        assert_eq!(identity.table_name.as_deref(), Some("items"));

        let mut result = QueryResult::default();
        collect_query_event(&mut result, PgQueryEvent::Notice(notice.clone()));
        assert_eq!(result.notices, vec![notice]);
        assert_eq!(
            decode_notice(b"VDEBUG\0C00000\0Mdebug\0\0")
                .expect_err("unsupported severity")
                .sql_state,
            "08P01"
        );
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

    #[test]
    fn ready_for_query_status_is_strict_and_typed() {
        assert_eq!(
            decode_ready_status(b"I").expect("idle"),
            PgTransactionStatus::Idle
        );
        assert_eq!(
            decode_ready_status(b"T").expect("active"),
            PgTransactionStatus::InTransaction
        );
        assert_eq!(
            decode_ready_status(b"E").expect("failed"),
            PgTransactionStatus::FailedTransaction
        );
        assert_eq!(
            decode_ready_status(b"X")
                .expect_err("unknown status")
                .sql_state,
            "08P01"
        );
        assert!(decode_ready_status(b"").is_err());
        assert!(decode_ready_status(b"II").is_err());
    }

    #[test]
    fn notification_response_is_typed_and_exact() {
        let mut payload = 42_u32.to_be_bytes().to_vec();
        payload.extend_from_slice(b"events\0ready\0");
        assert_eq!(
            decode_notification(&payload).expect("notification"),
            PgNotification {
                sender_process_id: 42,
                channel: "events".into(),
                payload: "ready".into(),
            }
        );
        payload.push(0);
        assert_eq!(
            decode_notification(&payload)
                .expect_err("trailing byte")
                .sql_state,
            "08P01"
        );
    }
}
