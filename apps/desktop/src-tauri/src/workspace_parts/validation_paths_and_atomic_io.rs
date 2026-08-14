
fn validate_profile_v2(profile: &ConnectionProfileV2) -> Result<(), DbError> {
    if profile.format_version != 2 {
        return Err(unsupported_version(
            "connection profile",
            profile.format_version,
        ));
    }
    validate_id(&profile.profile_id, "connection profile ID")?;
    validate_id(&profile.credential_id, "credential ID")?;
    validate_text(&profile.label, 1, 128, "connection profile label")?;
    validate_text(&profile.connector_id, 1, 128, "connector ID")?;
    validate_text(&profile.dialect, 1, 32, "SQL dialect")?;
    validate_text(&profile.endpoint, 1, 2_048, "database endpoint")?;
    if let Some(admin_endpoint) = &profile.admin_endpoint {
        validate_text(admin_endpoint, 1, 2_048, "administration endpoint")?;
    }
    if let Some(database) = &profile.database {
        validate_text(database, 1, 256, "database name")?;
    }
    validate_choice(
        &profile.tls_mode,
        &["disable", "prefer", "require", "verifyCa", "verifyFull"],
        "TLS mode",
    )?;
    if profile.data_source_kind != data_source_kind(&profile.connector_id) {
        return Err(invalid(
            "data source kind does not match the connector identity",
        ));
    }
    match profile.data_source_kind {
        DataSourceKind::OrdadbNative => {
            if profile.connector_id != NATIVE_CONNECTOR_ID
                || profile.dialect != "postgresql"
                || profile.admin_endpoint.is_none()
                || profile.tls_mode != "disable"
            {
                return Err(invalid("native OrdaDB profile fields are inconsistent"));
            }
        }
        DataSourceKind::Postgresql => {
            if profile.connector_id != "postgresql"
                || profile.dialect != "postgresql"
                || profile.admin_endpoint.is_some()
            {
                return Err(invalid(
                    "external PostgreSQL profile fields are inconsistent",
                ));
            }
        }
        DataSourceKind::Mysql => {
            if profile.connector_id != "mysql" || profile.dialect != "mysql" {
                return Err(invalid("MySQL profile fields are inconsistent"));
            }
        }
        DataSourceKind::Sqlite => {
            if profile.connector_id != "sqlite" || profile.dialect != "sqlite" {
                return Err(invalid("SQLite profile fields are inconsistent"));
            }
        }
        DataSourceKind::SqlServer => {
            if profile.connector_id != "sql-server" || profile.dialect != "sqlServer" {
                return Err(invalid("SQL Server profile fields are inconsistent"));
            }
        }
        DataSourceKind::Mongodb
        | DataSourceKind::Redis
        | DataSourceKind::Mariadb
        | DataSourceKind::Clickhouse
        | DataSourceKind::Oracle => {
            return Err(invalid(
                "connection profile v2 does not support this data source kind",
            ));
        }
    }
    Ok(())
}

fn validate_profiles_v3(document: &ConnectionProfilesV3) -> Result<(), DbError> {
    if document.format_version != PROFILES_VERSION {
        return Err(unsupported_version(
            "connection profiles",
            document.format_version,
        ));
    }
    if document.profiles.len() > MAX_CONNECTION_PROFILES {
        return Err(resource("connection profile limit exceeded"));
    }
    let mut ids = BTreeSet::new();
    for profile in &document.profiles {
        validate_profile_v3(profile)?;
        if !ids.insert(profile.profile_id.as_str()) {
            return Err(invalid("connection profile IDs must be unique"));
        }
    }
    Ok(())
}

fn validate_profile_v3(profile: &ConnectionProfileV3) -> Result<(), DbError> {
    if profile.format_version != PROFILES_VERSION {
        return Err(unsupported_version(
            "connection profile",
            profile.format_version,
        ));
    }
    validate_id(&profile.profile_id, "connection profile ID")?;
    validate_id(&profile.credential_id, "credential ID")?;
    validate_text(&profile.label, 1, 128, "connection profile label")?;
    validate_text(&profile.connector_id, 1, 128, "connector ID")?;
    validate_text(
        &profile.command_language,
        1,
        64,
        "connector command language",
    )?;
    validate_text(&profile.endpoint, 1, 2_048, "database endpoint")?;
    if !matches!(
        profile.connector_id.as_str(),
        NATIVE_CONNECTOR_ID
            | "postgresql"
            | "mysql"
            | "sqlite"
            | "sql-server"
            | "mongodb"
            | "redis"
            | "mariadb"
            | "clickhouse"
            | "oracle"
    ) {
        return Err(invalid("connection profile has an unknown connector ID"));
    }
    if let Some(dialect) = &profile.dialect {
        validate_text(dialect, 1, 32, "SQL dialect")?;
    }
    if let Some(admin_endpoint) = &profile.admin_endpoint {
        validate_text(admin_endpoint, 1, 2_048, "administration endpoint")?;
    }
    if let Some(database) = &profile.database {
        validate_text(database, 1, 256, "database name")?;
    }
    validate_choice(
        &profile.tls_mode,
        &["disable", "prefer", "require", "verifyCa", "verifyFull"],
        "TLS mode",
    )?;
    if profile.data_source_kind != data_source_kind(&profile.connector_id)
        || profile.connector_kind != connector_kind(&profile.connector_id)
        || profile.command_language != command_language(&profile.connector_id)
    {
        return Err(invalid(
            "connection profile metadata does not match the connector identity",
        ));
    }
    let expected_dialect = connector_dialect(&profile.connector_id);
    if profile.dialect.as_deref() != expected_dialect {
        return Err(invalid(
            "connection profile SQL dialect does not match the connector identity",
        ));
    }
    if profile.connector_id == NATIVE_CONNECTOR_ID
        && (profile.admin_endpoint.is_none() || profile.tls_mode != "disable")
    {
        return Err(invalid("native OrdaDB profile fields are inconsistent"));
    }
    if profile.connector_id != NATIVE_CONNECTOR_ID && profile.admin_endpoint.is_some() {
        return Err(invalid(
            "external connector profiles do not accept an administration endpoint",
        ));
    }
    Ok(())
}

fn migrate_connector_id(connector_id: &str) -> &str {
    match connector_id {
        "ordadb-postgresql" => NATIVE_CONNECTOR_ID,
        "ordadb-mysql" => "mysql",
        "ordadb-sqlite" => "sqlite",
        "ordadb-sql-server" => "sql-server",
        current => current,
    }
}

fn data_source_kind(connector_id: &str) -> DataSourceKind {
    match migrate_connector_id(connector_id) {
        NATIVE_CONNECTOR_ID => DataSourceKind::OrdadbNative,
        "postgresql" => DataSourceKind::Postgresql,
        "mysql" => DataSourceKind::Mysql,
        "sqlite" => DataSourceKind::Sqlite,
        "sql-server" => DataSourceKind::SqlServer,
        "mongodb" => DataSourceKind::Mongodb,
        "redis" => DataSourceKind::Redis,
        "mariadb" => DataSourceKind::Mariadb,
        "clickhouse" => DataSourceKind::Clickhouse,
        "oracle" => DataSourceKind::Oracle,
        _ => DataSourceKind::Postgresql,
    }
}

fn connector_kind(connector_id: &str) -> ConnectorKind {
    match connector_id {
        "mongodb" => ConnectorKind::Document,
        "redis" => ConnectorKind::KeyValue,
        _ => ConnectorKind::Sql,
    }
}

fn command_language(connector_id: &str) -> &'static str {
    match connector_id {
        NATIVE_CONNECTOR_ID | "postgresql" => "postgresql-sql",
        "mysql" => "mysql-sql",
        "sqlite" => "sqlite-sql",
        "sql-server" => "sql-server-sql",
        "mongodb" => "mongodb-json",
        "redis" => "redis-resp3",
        "mariadb" => "mariadb-sql",
        "clickhouse" => "clickhouse-sql",
        "oracle" => "oracle-sql",
        _ => "unknown",
    }
}

fn connector_dialect(connector_id: &str) -> Option<&'static str> {
    match connector_id {
        NATIVE_CONNECTOR_ID | "postgresql" => Some("postgresql"),
        "mysql" => Some("mysql"),
        "sqlite" => Some("sqlite"),
        "sql-server" => Some("sqlServer"),
        "mariadb" => Some("mariadb"),
        "clickhouse" => Some("clickhouse"),
        "oracle" => Some("oracle"),
        "mongodb" | "redis" => None,
        _ => None,
    }
}

fn validate_choice(value: &str, allowed: &[&str], field: &str) -> Result<(), DbError> {
    if allowed.contains(&value) {
        Ok(())
    } else {
        Err(invalid(format!("{field} is not supported")))
    }
}

fn canonical_workspace_root(value: &str) -> Result<PathBuf, DbError> {
    validate_text(value, 1, 32_768, "workspace root")?;
    let root = fs::canonicalize(value)
        .map_err(|error| io_error("failed to open SQL workspace root", error))?;
    if !root.is_dir() {
        return Err(invalid("SQL workspace root must be a directory"));
    }
    Ok(root)
}

fn canonical_external_sql_file(value: &str) -> Result<PathBuf, DbError> {
    validate_absolute_path_text(value, "external SQL file")?;
    let path = fs::canonicalize(value)
        .map_err(|error| io_error("failed to open external SQL file", error))?;
    if !path.is_file() || !is_sql_path(&path) {
        return Err(invalid("external document must be an existing .sql file"));
    }
    Ok(path)
}

fn normalize_save_destination(selected_path: &Path) -> Result<PathBuf, DbError> {
    if !selected_path.is_absolute() {
        return Err(invalid("Save As destination must be an absolute path"));
    }
    let mut destination = selected_path.to_path_buf();
    if destination.extension().is_none() {
        destination.set_extension("sql");
    }
    if !is_sql_path(&destination) {
        return Err(invalid("Save As destination must use the .sql extension"));
    }
    let file_name = destination
        .file_name()
        .ok_or_else(|| invalid("Save As destination has no file name"))?
        .to_string_lossy()
        .into_owned();
    validate_file_name(&file_name)?;
    if destination.exists() {
        return canonical_external_sql_file(&destination.display().to_string());
    }
    let parent = destination
        .parent()
        .ok_or_else(|| invalid("Save As destination has no parent directory"))?;
    let parent = fs::canonicalize(parent)
        .map_err(|error| io_error("failed to resolve Save As directory", error))?;
    if !parent.is_dir() {
        return Err(invalid("Save As parent must be a directory"));
    }
    Ok(parent.join(file_name))
}

fn validate_absolute_path_text(value: &str, context: &str) -> Result<(), DbError> {
    validate_text(value, 1, 32_768, context)?;
    if !Path::new(value).is_absolute() {
        return Err(invalid(format!("{context} must be an absolute path")));
    }
    Ok(())
}

fn resolve_workspace_entry(root: &Path, relative: &str) -> Result<PathBuf, DbError> {
    validate_relative_path(relative)?;
    let normalized = relative.replace('/', "\\");
    let joined = root.join(&normalized);
    reject_reparse_components(root, Path::new(&normalized))?;
    let canonical = fs::canonicalize(&joined)
        .map_err(|error| io_error("failed to resolve workspace entry", error))?;
    if !canonical.starts_with(root) {
        return Err(invalid("workspace entry escapes the selected root"));
    }
    Ok(canonical)
}

fn reject_reparse_components(root: &Path, relative: &Path) -> Result<(), DbError> {
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(invalid("workspace path must be relative and normalized"));
        };
        current.push(component);
        let metadata = fs::symlink_metadata(&current)
            .map_err(|error| io_error("failed to inspect workspace entry", error))?;
        if is_reparse_point(&metadata) {
            return Err(invalid(
                "workspace paths cannot pass through symbolic links or reparse points",
            ));
        }
    }
    Ok(())
}

fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

fn relative_display_path(root: &Path, path: &Path) -> Result<String, DbError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| invalid("workspace entry escapes the selected root"))?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

fn validate_relative_path(value: &str) -> Result<(), DbError> {
    if value.is_empty() || value.len() > 32_768 || value.contains('\0') {
        return Err(invalid("workspace path is empty or too long"));
    }
    let path = Path::new(value);
    let mut depth = 0_usize;
    for component in path.components() {
        match component {
            Component::Normal(_) => depth += 1,
            _ => return Err(invalid("workspace path must be relative and normalized")),
        }
    }
    if depth == 0 || depth > MAX_DIRECTORY_DEPTH {
        return Err(invalid("workspace path exceeds the supported depth"));
    }
    Ok(())
}

fn validate_relative_sql_path(value: &str) -> Result<(), DbError> {
    validate_relative_path(value)?;
    if !value.to_ascii_lowercase().ends_with(".sql") {
        return Err(invalid("workspace documents must use the .sql extension"));
    }
    Ok(())
}

fn validate_file_name(value: &str) -> Result<(), DbError> {
    validate_entry_name(value)?;
    if !value.to_ascii_lowercase().ends_with(".sql") {
        return Err(invalid("SQL file name must end with .sql"));
    }
    Ok(())
}

fn validate_entry_name(value: &str) -> Result<(), DbError> {
    if value.is_empty()
        || value.len() > 255
        || value.ends_with(['.', ' '])
        || value.contains(['\0', '/', '\\', ':', '*', '?', '"', '<', '>', '|'])
        || matches!(value, "." | "..")
        || value.chars().any(char::is_control)
    {
        return Err(invalid("workspace entry name is not a valid Windows name"));
    }
    Ok(())
}

fn is_sql_path(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("sql"))
}

fn read_workspace_document(root: &Path, path: &Path) -> Result<SqlDocument, DbError> {
    if !path.is_file() || !is_sql_path(path) {
        return Err(invalid("workspace document must be an existing .sql file"));
    }
    let relative = relative_display_path(root, path)?;
    read_sql_document_at(
        path,
        DocumentLocator::Workspace {
            root_path: root.display().to_string(),
            path: relative.clone(),
        },
        relative,
    )
}

fn read_external_document(path: &Path) -> Result<SqlDocument, DbError> {
    let absolute = path.display().to_string();
    read_sql_document_at(
        path,
        DocumentLocator::External {
            path: absolute.clone(),
        },
        absolute,
    )
}

fn read_sql_document_at(
    path: &Path,
    locator: DocumentLocator,
    display_path: String,
) -> Result<SqlDocument, DbError> {
    if !path.is_file() || !is_sql_path(path) {
        return Err(invalid("SQL document must be an existing .sql file"));
    }
    let (bytes, revision) = read_sql_file_snapshot(path)?;
    let content = String::from_utf8(bytes)
        .map_err(|error| invalid("SQL file must be valid UTF-8").with_detail(error.to_string()))?;
    Ok(SqlDocument {
        locator,
        path: display_path,
        name: path
            .file_name()
            .ok_or_else(|| invalid("SQL file has no name"))?
            .to_string_lossy()
            .into_owned(),
        content,
        revision,
    })
}

fn file_revision(path: &Path) -> Result<FileRevision, DbError> {
    read_sql_file_snapshot(path).map(|(_, revision)| revision)
}

fn read_sql_file_snapshot(path: &Path) -> Result<(Vec<u8>, FileRevision), DbError> {
    let mut file = File::open(path).map_err(|error| io_error("failed to open SQL file", error))?;
    let before = file
        .metadata()
        .map_err(|error| io_error("failed to inspect SQL file", error))?;
    if before.len() > MAX_SQL_FILE_BYTES {
        return Err(resource("SQL file exceeds the 4 MiB limit"));
    }
    let capacity = usize::try_from(before.len())
        .map_err(|_| resource("SQL file size does not fit this process"))?;
    let mut bytes = Vec::with_capacity(capacity);
    Read::take(&mut file, MAX_SQL_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io_error("failed to read SQL file", error))?;
    if bytes.len() as u64 > MAX_SQL_FILE_BYTES {
        return Err(resource("SQL file exceeds the 4 MiB limit"));
    }
    let after = file
        .metadata()
        .map_err(|error| io_error("failed to inspect SQL file after reading", error))?;
    if before.len() != after.len()
        || before.modified().ok() != after.modified().ok()
        || after.len() != bytes.len() as u64
    {
        return Err(
            DbError::new("40001", "SQL file changed while OrdaDB was reading it")
                .with_hint("retry after the external editor finishes writing the file"),
        );
    }
    let modified_at_ms = after
        .modified()
        .map_err(|error| io_error("failed to read SQL file timestamp", error))?
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("SQL file timestamp is before the Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| resource("SQL file timestamp overflowed"))?;
    let sha256 = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok((
        bytes,
        FileRevision {
            size_bytes: after.len(),
            modified_at_ms,
            sha256,
        },
    ))
}

fn read_json_or_default<T>(path: &Path) -> Result<T, DbError>
where
    T: DeserializeOwned + Default,
{
    if !path.exists() {
        return Ok(T::default());
    }
    let metadata =
        fs::metadata(path).map_err(|error| io_error("failed to inspect console state", error))?;
    if metadata.len() > MAX_DRAFT_BYTES as u64 {
        return Err(resource("console state file exceeds 16 MiB"));
    }
    let file = File::open(path).map_err(|error| io_error("failed to open console state", error))?;
    serde_json::from_reader(file)
        .map_err(|error| invalid("console state JSON is invalid").with_detail(error.to_string()))
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), DbError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
        DbError::internal("failed to encode console state").with_detail(error.to_string())
    })?;
    if bytes.len() > MAX_DRAFT_BYTES {
        return Err(resource("console state file exceeds 16 MiB"));
    }
    write_bytes_atomic(path, &bytes)
}

fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<(), DbError> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("atomic state destination has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error("failed to create state destination", error))?;
    let extension = format!("tmp-{}", Uuid::new_v4());
    let temporary = path.with_extension(extension);
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| io_error("failed to create atomic state file", error))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| io_error("failed to synchronize atomic state file", error))?;
        move_file_replace(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn move_file_replace(source: &Path, destination: &Path) -> Result<(), DbError> {
    let source_wide = wide(source);
    let destination_wide = wide(destination);
    // SAFETY: both buffers are live NUL-terminated UTF-16 paths and flags
    // request a same-volume, write-through replacement.
    let moved = unsafe {
        MoveFileExW(
            source_wide.as_ptr(),
            destination_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(io_error(
            "failed to publish atomic state file",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

fn wide(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .to_string_lossy()
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect()
}

fn validate_id(value: &str, context: &str) -> Result<(), DbError> {
    if value.is_empty()
        || value.len() > 128
        || value.starts_with('.')
        || value.ends_with('.')
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(invalid(format!(
            "{context} must use 1-128 ASCII letters, digits, dots, hyphens, or underscores"
        )));
    }
    Ok(())
}

fn validate_text(
    value: &str,
    minimum: usize,
    maximum: usize,
    context: &str,
) -> Result<(), DbError> {
    if !(minimum..=maximum).contains(&value.len())
        || value.contains('\0')
        || value.chars().any(char::is_control)
    {
        return Err(invalid(format!(
            "{context} must contain {minimum}-{maximum} printable UTF-8 bytes"
        )));
    }
    Ok(())
}

fn unsupported_version(context: &str, version: u32) -> DbError {
    DbError::new(
        "0A000",
        format!("{context} format version {version} is not supported"),
    )
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn resource(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}
