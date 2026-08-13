
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

fn insert_statement(table: &str, columns: &[ColumnDefinition], rows: usize) -> Result<String> {
    if columns.is_empty() || rows == 0 {
        return Err(DbError::new(
            "XX000",
            "COPY insert batches require at least one row and column",
        ));
    }
    let names = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let parameter_count = rows
        .checked_mul(columns.len())
        .ok_or_else(|| DbError::new("54000", "COPY parameter count overflowed"))?;
    let parameters = (0..rows)
        .map(|row| {
            let first = row * columns.len() + 1;
            let values = (first..first + columns.len())
                .map(|index| format!("${index}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({values})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    debug_assert_eq!(parameter_count, rows * columns.len());
    Ok(format!("INSERT INTO {table} ({names}) VALUES {parameters}"))
}

fn import_copy(
    session: &mut Session,
    table: &str,
    columns: &[ColumnDefinition],
    options: &CopyOptions,
    bytes: &[u8],
) -> Result<u64> {
    let plan = CopyInsertPlan::new(table, columns)?;
    match options.format {
        CopyFormat::Text => import_text(session, &plan, options, bytes),
        CopyFormat::Csv => import_csv(session, &plan, options, bytes),
    }
}

struct CopyInsertPlan<'a> {
    table: &'a str,
    columns: &'a [ColumnDefinition],
    data_types: Vec<ScalarType>,
    oids: Vec<u32>,
    batch_rows: usize,
}

impl<'a> CopyInsertPlan<'a> {
    fn new(table: &'a str, columns: &'a [ColumnDefinition]) -> Result<Self> {
        if columns.is_empty() {
            return Err(DbError::new("XX000", "COPY target has no columns"));
        }
        let data_types = columns
            .iter()
            .map(|column| column.data_type.clone())
            .collect::<Vec<_>>();
        let oids = data_types.iter().map(type_oid).collect::<Vec<_>>();
        let batch_rows =
            (COPY_INSERT_BATCH_PARAMETERS / columns.len()).clamp(1, COPY_INSERT_BATCH_ROWS);
        Ok(Self {
            table,
            columns,
            data_types,
            oids,
            batch_rows,
        })
    }

    fn insert_rows(&self, session: &mut Session, raw_rows: &[Vec<Option<Vec<u8>>>]) -> Result<()> {
        if raw_rows.is_empty() {
            return Ok(());
        }
        let parameter_count = raw_rows
            .len()
            .checked_mul(self.columns.len())
            .ok_or_else(|| DbError::new("54000", "COPY parameter count overflowed"))?;
        let mut values = Vec::with_capacity(parameter_count);
        for raw in raw_rows {
            if raw.len() != self.columns.len() {
                return Err(DbError::new(
                    "22P04",
                    format!(
                        "COPY row has {} fields but target has {} columns",
                        raw.len(),
                        self.columns.len()
                    ),
                ));
            }
            values.extend(decode_parameters_as(
                &self.oids,
                &self.data_types,
                &[],
                raw,
            )?);
        }
        let insert = insert_statement(self.table, self.columns, raw_rows.len())?;
        drain(session.execute_stream(&insert, &values)?)
    }
}

fn import_csv(
    session: &mut Session,
    plan: &CopyInsertPlan<'_>,
    options: &CopyOptions,
    bytes: &[u8],
) -> Result<u64> {
    let records = decode_csv_records(bytes, options)?;
    let mut records = records.into_iter();
    if options.header {
        let header = records
            .next()
            .ok_or_else(|| DbError::new("22P04", "COPY CSV header is missing"))?;
        let expected = plan
            .columns
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
    let mut batch = Vec::with_capacity(plan.batch_rows);
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
        batch.push(raw);
        rows = checked_copy_row_count(rows)?;
        if batch.len() == plan.batch_rows {
            plan.insert_rows(session, &batch)?;
            batch.clear();
        }
    }
    plan.insert_rows(session, &batch)?;
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
    plan: &CopyInsertPlan<'_>,
    options: &CopyOptions,
    bytes: &[u8],
) -> Result<u64> {
    let mut rows = 0_u64;
    let mut batch = Vec::with_capacity(plan.batch_rows);
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
        batch.push(raw);
        rows = checked_copy_row_count(rows)?;
        if batch.len() == plan.batch_rows {
            plan.insert_rows(session, &batch)?;
            batch.clear();
        }
        start = end.saturating_add(1);
    }
    plan.insert_rows(session, &batch)?;
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

fn checked_copy_row_count(rows: u64) -> Result<u64> {
    rows.checked_add(1)
        .ok_or_else(|| DbError::new("54000", "COPY row count overflowed"))
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}
