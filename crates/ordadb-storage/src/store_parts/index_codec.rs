
fn validate_index_entries(
    definition: &IndexDefinition,
    owner: &TableDefinition,
    rows: &[Row],
    entries: &[IndexEntry],
) -> Result<()> {
    if entries.len() != rows.len() {
        return Err(corruption(format!(
            "index {} contains {} entries for {} heap rows",
            definition.id.get(),
            entries.len(),
            rows.len()
        )));
    }
    let mut referenced_rows = BTreeSet::new();
    let mut previous: Option<&IndexEntry> = None;
    for entry in entries {
        let row_index = usize::try_from(entry.row_id.get())
            .map_err(|_| corruption("index row reference exceeds the platform limit"))?;
        let row = rows.get(row_index).ok_or_else(|| {
            corruption(format!(
                "index {} row reference {} is outside its heap",
                definition.id.get(),
                entry.row_id.get()
            ))
        })?;
        if !referenced_rows.insert(entry.row_id) {
            return Err(corruption(format!(
                "index {} references row {} more than once",
                definition.id.get(),
                entry.row_id.get()
            )));
        }
        let key_values = definition
            .key_columns
            .iter()
            .map(|column_id| {
                owner
                    .column_index_by_id(*column_id)
                    .and_then(|position| row.values.get(position))
                    .cloned()
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                corruption(format!(
                    "index {} key shape does not match its heap row",
                    definition.id.get()
                ))
            })?;
        let key_types = definition
            .key_columns
            .iter()
            .map(|column_id| {
                owner
                    .column_index_by_id(*column_id)
                    .and_then(|position| owner.columns().get(position))
                    .map(|column| column.data_type.clone())
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                corruption(format!(
                    "index {} key types do not match its table definition",
                    definition.id.get()
                ))
            })?;
        let expected_key = IndexKey::from_typed_values(&key_values, &key_types)?;
        if entry.key != expected_key {
            return Err(corruption(format!(
                "index {} key does not match heap row {}",
                definition.id.get(),
                entry.row_id.get()
            )));
        }
        let expected_included = definition
            .include_columns
            .iter()
            .map(|column_id| {
                owner
                    .column_index_by_id(*column_id)
                    .and_then(|position| row.values.get(position))
                    .cloned()
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                corruption(format!(
                    "index {} covering shape does not match its heap row",
                    definition.id.get()
                ))
            })?;
        if entry.included != expected_included {
            return Err(corruption(format!(
                "index {} covering payload does not match heap row {}",
                definition.id.get(),
                entry.row_id.get()
            )));
        }
        if let Some(previous) = previous {
            let ordering = previous
                .key
                .cmp(&entry.key)
                .then_with(|| previous.row_id.cmp(&entry.row_id));
            if ordering.is_gt() {
                return Err(corruption(format!(
                    "index {} entries are not ordered",
                    definition.id.get()
                )));
            }
            if definition.unique && !previous.key.contains_null() && previous.key == entry.key {
                return Err(corruption(format!(
                    "unique index {} contains a duplicate key",
                    definition.id.get()
                )));
            }
        }
        previous = Some(entry);
    }
    Ok(())
}

fn encode_index_entry(entry: &IndexEntry) -> Result<Vec<u8>> {
    let key = serde_json::to_vec(&entry.key)
        .map_err(|error| corruption(format!("index key encoding failed: {error}")))?;
    let included = encode_row(&Row::new(entry.included.clone()))?;
    let key_len = u32::try_from(key.len())
        .map_err(|_| DbError::new("54000", "encoded index key is too large"))?;
    let included_len = u32::try_from(included.len())
        .map_err(|_| DbError::new("54000", "encoded covering payload is too large"))?;
    let mut bytes = Vec::with_capacity(18 + key.len() + included.len());
    bytes.extend_from_slice(&INDEX_RECORD_VERSION.to_le_bytes());
    bytes.extend_from_slice(&entry.row_id.get().to_le_bytes());
    bytes.extend_from_slice(&key_len.to_le_bytes());
    bytes.extend_from_slice(&key);
    bytes.extend_from_slice(&included_len.to_le_bytes());
    bytes.extend_from_slice(&included);
    Ok(bytes)
}

fn decode_index_entry(bytes: &[u8]) -> Result<IndexEntry> {
    let mut offset = 0;
    let version = read_u16(bytes, &mut offset)?;
    if !matches!(version, INDEX_RECORD_VERSION_V1 | INDEX_RECORD_VERSION) {
        return Err(corruption(format!(
            "unsupported index record version {version}"
        )));
    }
    let row_id = RowId::new(read_u64(bytes, &mut offset)?);
    let key_len = usize::try_from(read_u32(bytes, &mut offset)?)
        .map_err(|_| corruption("index key length exceeds the platform limit"))?;
    let key_end = offset
        .checked_add(key_len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| corruption("index key record is truncated"))?;
    let key = if version == INDEX_RECORD_VERSION_V1 {
        IndexKey::from_values(&decode_row(&bytes[offset..key_end])?.values)?
    } else {
        serde_json::from_slice(&bytes[offset..key_end])
            .map_err(|error| corruption(format!("index key is malformed: {error}")))?
    };
    offset = key_end;
    let included_len = usize::try_from(read_u32(bytes, &mut offset)?)
        .map_err(|_| corruption("covering payload length exceeds the platform limit"))?;
    let included_end = offset
        .checked_add(included_len)
        .filter(|end| *end == bytes.len())
        .ok_or_else(|| corruption("index covering record is truncated or has trailing data"))?;
    let included = decode_row(&bytes[offset..included_end])?.values;
    IndexEntry::from_key(key, row_id, included)
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> Result<u16> {
    let value = read_exact::<2>(bytes, offset)?;
    Ok(u16::from_le_bytes(value))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let value = read_exact::<4>(bytes, offset)?;
    Ok(u32::from_le_bytes(value))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let value = read_exact::<8>(bytes, offset)?;
    Ok(u64::from_le_bytes(value))
}

fn read_exact<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    let end = offset
        .checked_add(N)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| corruption("index record is truncated"))?;
    let mut value = [0_u8; N];
    value.copy_from_slice(&bytes[*offset..end]);
    *offset = end;
    Ok(value)
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn unsupported_database_version(version: u16) -> DbError {
    DbError::new(
        "0A000",
        format!("database format version {version} is not supported"),
    )
    .with_detail(format!(
        "this OrdaDB build supports database formats {DATABASE_FORMAT_V1} and {DATABASE_FORMAT_V2}"
    ))
    .with_hint("open v1 read-only for inspection or run an explicit supported migration")
}

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
