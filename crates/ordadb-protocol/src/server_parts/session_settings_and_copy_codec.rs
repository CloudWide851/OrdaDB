
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
