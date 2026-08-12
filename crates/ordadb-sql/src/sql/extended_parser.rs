
fn create_domain_not_null_tokens(tokens: &[Token]) -> Option<(usize, usize)> {
    if !tokens
        .first()
        .is_some_and(|token| is_unquoted_word(token, "CREATE"))
        || !tokens
            .get(1)
            .is_some_and(|token| is_unquoted_word(token, "DOMAIN"))
    {
        return None;
    }
    let mut parenthesis_depth = 0_usize;
    for index in 2..tokens.len().saturating_sub(1) {
        match &tokens[index] {
            Token::LParen => parenthesis_depth = parenthesis_depth.saturating_add(1),
            Token::RParen => parenthesis_depth = parenthesis_depth.saturating_sub(1),
            token
                if parenthesis_depth == 0
                    && is_unquoted_word(token, "NOT")
                    && !tokens
                        .get(index.wrapping_sub(1))
                        .is_some_and(|token| is_unquoted_word(token, "IS"))
                    && is_unquoted_word(&tokens[index + 1], "NULL") =>
            {
                return Some((index, index + 1));
            }
            _ => {}
        }
    }
    None
}

fn create_domain_is_not_null(sql: &str) -> bool {
    create_domain_not_null_tokens(&significant_tokens(sql)).is_some()
}

#[derive(Debug, Clone, Copy)]
struct MergeClauseTokenInfo {
    not_matched_by_target: bool,
    do_nothing: Option<(usize, usize)>,
}

fn rewrite_postgres_merge_do_nothing(
    sql: &str,
) -> std::result::Result<Option<Vec<TokenWithSpan>>, ParserError> {
    let dialect = PostgreSqlDialect {};
    let mut tokens = Tokenizer::new(&dialect, sql).tokenize_with_location()?;
    let significant_indices = tokens
        .iter()
        .enumerate()
        .filter_map(|(index, token)| {
            (!matches!(token.token, Token::Whitespace(_))).then_some(index)
        })
        .collect::<Vec<_>>();
    let significant = significant_indices
        .iter()
        .map(|index| tokens[*index].token.clone())
        .collect::<Vec<_>>();
    let Some(clauses) = merge_clause_token_info(&significant) else {
        return Ok(None);
    };
    let replacements = clauses
        .into_iter()
        .filter_map(|clause| {
            clause.do_nothing.map(|(do_index, nothing_index)| {
                (clause.not_matched_by_target, do_index, nothing_index)
            })
        })
        .collect::<Vec<_>>();
    if replacements.is_empty() {
        return Ok(None);
    }
    for (not_matched_by_target, do_index, nothing_index) in replacements {
        let do_index = significant_indices[do_index];
        let nothing_index = significant_indices[nothing_index];
        if not_matched_by_target {
            tokens[do_index].token = Token::make_keyword("INSERT");
            tokens[nothing_index].token = Token::make_keyword("ROW");
        } else {
            tokens[do_index].token = Token::make_keyword("DELETE");
            tokens[nothing_index].token = Token::Whitespace(Whitespace::Space);
        }
    }
    Ok(Some(tokens))
}

fn merge_clause_token_info(tokens: &[Token]) -> Option<Vec<MergeClauseTokenInfo>> {
    if !tokens
        .first()
        .is_some_and(|token| is_unquoted_word(token, "MERGE"))
    {
        return None;
    }
    let mut starts = Vec::new();
    let mut parenthesis_depth = 0_usize;
    let mut case_depth = 0_usize;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Token::LParen => parenthesis_depth = parenthesis_depth.saturating_add(1),
            Token::RParen => parenthesis_depth = parenthesis_depth.saturating_sub(1),
            _ if parenthesis_depth == 0 && is_unquoted_word(token, "CASE") => {
                case_depth = case_depth.saturating_add(1);
            }
            _ if parenthesis_depth == 0 && is_unquoted_word(token, "END") && case_depth > 0 => {
                case_depth -= 1;
            }
            _ if parenthesis_depth == 0
                && case_depth == 0
                && is_merge_clause_start(tokens, index) =>
            {
                starts.push(index);
            }
            _ => {}
        }
    }
    let mut clauses = Vec::with_capacity(starts.len());
    for (ordinal, start) in starts.iter().copied().enumerate() {
        let end = starts.get(ordinal + 1).copied().unwrap_or(tokens.len());
        let not_matched = tokens
            .get(start.saturating_add(1))
            .is_some_and(|token| is_unquoted_word(token, "NOT"));
        let by_source = tokens
            .get(start.saturating_add(3))
            .is_some_and(|token| is_unquoted_word(token, "BY"))
            && tokens
                .get(start.saturating_add(4))
                .is_some_and(|token| is_unquoted_word(token, "SOURCE"));
        let do_nothing = find_merge_do_nothing(tokens, start, end);
        clauses.push(MergeClauseTokenInfo {
            not_matched_by_target: not_matched && !by_source,
            do_nothing,
        });
    }
    Some(clauses)
}

fn is_merge_clause_start(tokens: &[Token], index: usize) -> bool {
    is_unquoted_word(&tokens[index], "WHEN")
        && (tokens
            .get(index.saturating_add(1))
            .is_some_and(|token| is_unquoted_word(token, "MATCHED"))
            || (tokens
                .get(index.saturating_add(1))
                .is_some_and(|token| is_unquoted_word(token, "NOT"))
                && tokens
                    .get(index.saturating_add(2))
                    .is_some_and(|token| is_unquoted_word(token, "MATCHED"))))
}

fn find_merge_do_nothing(tokens: &[Token], start: usize, end: usize) -> Option<(usize, usize)> {
    let mut parenthesis_depth = 0_usize;
    let mut case_depth = 0_usize;
    for index in start..end.saturating_sub(2) {
        let token = &tokens[index];
        match token {
            Token::LParen => parenthesis_depth = parenthesis_depth.saturating_add(1),
            Token::RParen => parenthesis_depth = parenthesis_depth.saturating_sub(1),
            _ if parenthesis_depth == 0 && is_unquoted_word(token, "CASE") => {
                case_depth = case_depth.saturating_add(1);
            }
            _ if parenthesis_depth == 0 && is_unquoted_word(token, "END") && case_depth > 0 => {
                case_depth -= 1;
            }
            _ if parenthesis_depth == 0
                && case_depth == 0
                && is_unquoted_word(token, "THEN")
                && is_unquoted_word(&tokens[index + 1], "DO")
                && is_unquoted_word(&tokens[index + 2], "NOTHING") =>
            {
                return Some((index + 1, index + 2));
            }
            _ => {}
        }
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParameterStyle {
    QuestionMark,
    NamedAtP,
}

fn parse_tokenized_source(
    sql: &str,
    dialect: &dyn Dialect,
    parameter_style: ParameterStyle,
) -> std::result::Result<Vec<SqlStatement>, ParserError> {
    let mut parameter_index = 1usize;
    let mut tokens = Vec::<TokenWithSpan>::new();
    Tokenizer::new(dialect, sql).tokenize_with_location_into_buf_with_mapper(
        &mut tokens,
        |mut token| {
            if parameter_style == ParameterStyle::QuestionMark
                && matches!(&token.token, Token::Placeholder(value) if value == "?")
            {
                token.token = Token::Placeholder(format!("${parameter_index}"));
                parameter_index = parameter_index.saturating_add(1);
            }
            token
        },
    )?;
    Parser::new(dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
}

fn dialect_error(mut error: DbError, dialect: SqlDialect) -> DbError {
    if dialect != SqlDialect::PostgreSql && error.sql_state == FEATURE_NOT_SUPPORTED {
        error.message = format!(
            "{} feature is not supported: {}",
            dialect.label(),
            error.message
        );
        if error.hint.is_none() {
            error.hint =
                Some("Rewrite the statement using the OrdaDB/PostgreSQL-compatible subset.".into());
        }
    }
    error
}

fn parse_create_procedure(sql: &str) -> Result<Option<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let uppercase = trimmed.to_ascii_uppercase();
    let (header_prefix, replace) = if uppercase.starts_with("CREATE OR REPLACE PROCEDURE ") {
        ("CREATE OR REPLACE PROCEDURE ", true)
    } else if uppercase.starts_with("CREATE PROCEDURE ") {
        ("CREATE PROCEDURE ", false)
    } else {
        return Ok(None);
    };
    let after_prefix = &trimmed[header_prefix.len()..];
    let open = after_prefix
        .find('(')
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "CREATE PROCEDURE requires arguments"))?;
    let close = after_prefix[open + 1..]
        .find(')')
        .map(|position| position + open + 1)
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "unterminated procedure argument list"))?;
    let name = parse_simple_object_name(after_prefix[..open].trim())?;
    let arguments = parse_procedure_arguments(&after_prefix[open + 1..close])?;
    let tail = after_prefix[close + 1..].trim();
    let (as_position, body_position) = keyword_span(tail, "AS")
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "CREATE PROCEDURE requires AS body"))?;
    let options = tail[..as_position].trim();
    if !options.is_empty()
        && !matches_keyword_sequence(options, &["LANGUAGE", "plpgsql"])
        && !matches_keyword_sequence(options, &["LANGUAGE", "plpgsql", "SECURITY", "INVOKER"])
    {
        return unsupported("only LANGUAGE plpgsql SECURITY INVOKER procedures are supported");
    }
    let body_start = tail[body_position..].trim();
    let body = parse_dollar_quoted_body(body_start)?;
    Ok(Some(ParsedStatement::CreateRoutine {
        name,
        kind: RoutineKind::Procedure,
        arguments,
        return_type: None,
        return_declared_type: None,
        returns_set: false,
        language: "plpgsql".to_owned(),
        body,
        replace,
    }))
}

fn parse_postgres_session_or_maintenance(sql: &str) -> Result<Option<ParsedStatement>> {
    const MAX_NOTIFICATION_PAYLOAD_BYTES: usize = 7_999;
    const MAX_DO_BODY_BYTES: usize = 1024 * 1024;

    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let tokens = significant_tokens(trimmed);
    let Some(first) = tokens.first() else {
        return Ok(None);
    };

    if is_unquoted_word(first, "REINDEX") {
        if tokens.iter().any(|token| matches!(token, Token::LParen)) {
            return unsupported("REINDEX parameter clauses are not supported");
        }
        if tokens
            .iter()
            .any(|token| is_unquoted_word(token, "CONCURRENTLY"))
        {
            return unsupported("REINDEX CONCURRENTLY is not supported");
        }
        let mut cursor = 1;
        if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "SYSTEM"))
        {
            return unsupported("REINDEX SYSTEM is not supported");
        }
        let target = if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "INDEX"))
        {
            cursor += 1;
            ParsedReindexTarget::Index(parse_token_object_name(&tokens, &mut cursor, 2)?)
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "TABLE"))
        {
            cursor += 1;
            ParsedReindexTarget::Table(parse_token_object_name(&tokens, &mut cursor, 2)?)
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "SCHEMA"))
        {
            cursor += 1;
            ParsedReindexTarget::Schema(parse_token_identifier(&tokens, &mut cursor)?)
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "DATABASE"))
        {
            cursor += 1;
            ParsedReindexTarget::Database(parse_token_identifier(&tokens, &mut cursor)?)
        } else {
            return unsupported("REINDEX requires INDEX, TABLE, SCHEMA, or DATABASE");
        };
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::Reindex { target }));
    }

    if is_unquoted_word(first, "LISTEN") {
        let mut cursor = 1;
        let channel = parse_token_identifier(&tokens, &mut cursor)?;
        validate_notification_channel(&channel.name)?;
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::Listen { channel }));
    }

    if is_unquoted_word(first, "UNLISTEN") {
        let mut cursor = 1;
        let channel = if tokens.get(cursor) == Some(&Token::Mul) {
            cursor += 1;
            None
        } else {
            let channel = parse_token_identifier(&tokens, &mut cursor)?;
            validate_notification_channel(&channel.name)?;
            Some(channel)
        };
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::Unlisten { channel }));
    }

    if is_unquoted_word(first, "NOTIFY") {
        let mut cursor = 1;
        let channel = parse_token_identifier(&tokens, &mut cursor)?;
        validate_notification_channel(&channel.name)?;
        let payload = if tokens.get(cursor) == Some(&Token::Comma) {
            cursor += 1;
            let Token::SingleQuotedString(payload) = tokens
                .get(cursor)
                .ok_or_else(|| DbError::new(SYNTAX_ERROR, "NOTIFY payload expected"))?
            else {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "NOTIFY payload must be a string literal",
                ));
            };
            cursor += 1;
            payload.clone()
        } else {
            String::new()
        };
        if payload.contains('\0') {
            return Err(DbError::new("22021", "NOTIFY payload cannot contain NUL"));
        }
        if payload.len() > MAX_NOTIFICATION_PAYLOAD_BYTES {
            return Err(DbError::new("22023", "NOTIFY payload is too long"));
        }
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::Notify { channel, payload }));
    }

    if is_unquoted_word(first, "DO") {
        let mut cursor = 1;
        if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "LANGUAGE"))
        {
            cursor += 1;
            let language = parse_token_identifier(&tokens, &mut cursor)?;
            if !language.name.as_str().eq_ignore_ascii_case("plpgsql") {
                return unsupported("only LANGUAGE plpgsql DO blocks are supported");
            }
        }
        let body = match tokens.get(cursor) {
            Some(Token::DollarQuotedString(body)) => body.value.clone(),
            Some(Token::SingleQuotedString(body)) => body.clone(),
            _ => return Err(DbError::new(SYNTAX_ERROR, "DO requires a quoted body")),
        };
        cursor += 1;
        if body.len() > MAX_DO_BODY_BYTES {
            return Err(DbError::new("54000", "DO body exceeds the source limit"));
        }
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::Do { body }));
    }

    Ok(None)
}

fn validate_notification_channel(channel: &Identifier) -> Result<()> {
    if channel.as_str().is_empty() || channel.as_str().len() > ordadb_types::MAX_POSTGRES_NAME_BYTES
    {
        return Err(DbError::new(
            "42622",
            "notification channel name is empty or too long",
        ));
    }
    if channel.as_str().contains('\0') {
        return Err(DbError::new(
            "22021",
            "notification channel cannot contain NUL",
        ));
    }
    Ok(())
}

fn keyword_span(value: &str, keyword: &str) -> Option<(usize, usize)> {
    let mut start = None;
    for (position, character) in value.char_indices() {
        if character.is_whitespace() {
            if let Some(token_start) = start.take()
                && value[token_start..position].eq_ignore_ascii_case(keyword)
            {
                return Some((token_start, position));
            }
        } else if start.is_none() {
            start = Some(position);
        }
    }
    let token_start = start?;
    value[token_start..]
        .eq_ignore_ascii_case(keyword)
        .then_some((token_start, value.len()))
}

fn matches_keyword_sequence(value: &str, expected: &[&str]) -> bool {
    let mut actual = value.split_whitespace();
    expected.iter().all(|keyword| {
        actual
            .next()
            .is_some_and(|value| value.eq_ignore_ascii_case(keyword))
    }) && actual.next().is_none()
}

fn parse_alter_domain(sql: &str) -> Result<Option<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let tokens = significant_tokens(trimmed);
    if tokens.len() < 3
        || !tokens
            .first()
            .is_some_and(|token| is_unquoted_word(token, "ALTER"))
        || !tokens
            .get(1)
            .is_some_and(|token| is_unquoted_word(token, "DOMAIN"))
    {
        return Ok(None);
    }
    let mut cursor = 2usize;
    let name = parse_token_object_name(&tokens, &mut cursor, 2)?;
    let operation = if consume_keyword_sequence(&tokens, &mut cursor, &["SET", "DEFAULT"]) {
        if cursor == tokens.len() {
            return Err(DbError::new(
                SYNTAX_ERROR,
                "ALTER DOMAIN SET DEFAULT requires an expression",
            ));
        }
        let default = parsed_default_from_tokens(&tokens[cursor..])?;
        cursor = tokens.len();
        ParsedAlterDomainOperation::SetDefault(default)
    } else if consume_keyword_sequence(&tokens, &mut cursor, &["DROP", "DEFAULT"]) {
        ParsedAlterDomainOperation::DropDefault
    } else if consume_keyword_sequence(&tokens, &mut cursor, &["SET", "NOT", "NULL"]) {
        ParsedAlterDomainOperation::SetNotNull
    } else if consume_keyword_sequence(&tokens, &mut cursor, &["DROP", "NOT", "NULL"]) {
        ParsedAlterDomainOperation::DropNotNull
    } else if consume_keyword(&tokens, &mut cursor, "ADD") {
        let constraint = parse_domain_constraint_tokens(&tokens, &mut cursor)?;
        if consume_keyword_sequence(&tokens, &mut cursor, &["NOT", "VALID"]) {
            return unsupported("ALTER DOMAIN ADD CONSTRAINT NOT VALID is not supported yet");
        }
        ParsedAlterDomainOperation::AddConstraint(constraint)
    } else if consume_keyword_sequence(&tokens, &mut cursor, &["DROP", "CONSTRAINT"]) {
        let if_exists = consume_keyword_sequence(&tokens, &mut cursor, &["IF", "EXISTS"]);
        let name = parse_token_identifier(&tokens, &mut cursor)?;
        if consume_keyword(&tokens, &mut cursor, "CASCADE") {
            return unsupported("ALTER DOMAIN DROP CONSTRAINT CASCADE is not supported yet");
        }
        let _ = consume_keyword(&tokens, &mut cursor, "RESTRICT");
        ParsedAlterDomainOperation::DropConstraint { name, if_exists }
    } else {
        return unsupported("this ALTER DOMAIN operation is not supported yet");
    };
    ensure_token_end(&tokens, cursor)?;
    Ok(Some(ParsedStatement::AlterDomain { name, operation }))
}

fn parse_domain_constraint_tokens(
    tokens: &[Token],
    cursor: &mut usize,
) -> Result<DomainConstraint> {
    let name = if consume_keyword(tokens, cursor, "CONSTRAINT") {
        Some(parse_token_identifier(tokens, cursor)?.name)
    } else {
        None
    };
    expect_keyword(tokens, cursor, "CHECK")?;
    if tokens.get(*cursor) != Some(&Token::LParen) {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "domain CHECK constraint requires parentheses",
        ));
    }
    *cursor += 1;
    let expression_start = *cursor;
    let mut depth = 1usize;
    while *cursor < tokens.len() {
        match tokens[*cursor] {
            Token::LParen => depth = depth.saturating_add(1),
            Token::RParen => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
        *cursor += 1;
    }
    if depth != 0 {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "unterminated domain CHECK constraint",
        ));
    }
    if expression_start == *cursor {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "domain CHECK constraint requires an expression",
        ));
    }
    let expression = tokens_sql(&tokens[expression_start..*cursor]);
    parse_parsed_expression(&expression)?;
    *cursor += 1;
    Ok(DomainConstraint {
        id: None,
        name,
        expression: CatalogExpression::new(expression),
    })
}

fn parsed_default_from_tokens(tokens: &[Token]) -> Result<ParsedDefault> {
    let sql = tokens_sql(tokens);
    Ok(ParsedDefault {
        expression: parse_parsed_expression(&sql)?,
        sql,
    })
}

fn parse_parsed_expression(sql: &str) -> Result<ParsedExpr> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|error| DbError::new(SYNTAX_ERROR, error.to_string()))?;
    let expression = parser
        .parse_expr()
        .map_err(|error| DbError::new(SYNTAX_ERROR, error.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "domain expression contains trailing SQL",
        ));
    }
    convert_expr(expression, sql)
}

fn tokens_sql(tokens: &[Token]) -> String {
    tokens
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_alter_view(sql: &str) -> Result<Option<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let tokens = significant_tokens(trimmed);
    if tokens.len() < 3
        || !tokens
            .first()
            .is_some_and(|token| is_unquoted_word(token, "ALTER"))
    {
        return Ok(None);
    }
    let mut cursor = 1usize;
    let kind = if tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "MATERIALIZED"))
    {
        cursor += 1;
        if !tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "VIEW"))
        {
            return Ok(None);
        }
        cursor += 1;
        ViewKind::Materialized
    } else if tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "VIEW"))
    {
        cursor += 1;
        ViewKind::Regular
    } else {
        return Ok(None);
    };
    let if_exists = if tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "IF"))
        && tokens
            .get(cursor + 1)
            .is_some_and(|token| is_unquoted_word(token, "EXISTS"))
    {
        cursor += 2;
        true
    } else {
        false
    };
    let name = parse_token_object_name(&tokens, &mut cursor, 2)?;
    if !tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "RENAME"))
    {
        return unsupported("only ALTER VIEW ... RENAME TO is supported");
    }
    cursor += 1;
    expect_keyword(&tokens, &mut cursor, "TO")?;
    let new_name = parse_token_identifier(&tokens, &mut cursor)?;
    ensure_token_end(&tokens, cursor)?;
    Ok(Some(ParsedStatement::AlterViewRename {
        name,
        kind,
        if_exists,
        new_name,
    }))
}

fn parse_procedure_arguments(value: &str) -> Result<Vec<ParsedRoutineArgument>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|argument| {
            let mut parts = argument.split_whitespace().collect::<Vec<_>>();
            if parts
                .iter()
                .any(|part| part.eq_ignore_ascii_case("DEFAULT") || part.contains('='))
            {
                return unsupported("defaulted procedure arguments are not supported yet");
            }
            let mode = parts
                .first()
                .and_then(|part| parse_routine_argument_mode(part))
                .unwrap_or_default();
            if parse_routine_argument_mode(parts.first().copied().unwrap_or_default()).is_some() {
                parts.remove(0);
            }
            let (name, data_type) = match parts.as_slice() {
                [data_type] => (None, *data_type),
                [first, second]
                    if first.eq_ignore_ascii_case("DOUBLE")
                        && second.eq_ignore_ascii_case("PRECISION") =>
                {
                    (None, "DOUBLE PRECISION")
                }
                [name, data_type] => (Some(Identifier::unquoted(*name)), *data_type),
                [name, first, second] if first.eq_ignore_ascii_case("DOUBLE") => (
                    Some(Identifier::unquoted(*name)),
                    if second.eq_ignore_ascii_case("PRECISION") {
                        "DOUBLE PRECISION"
                    } else {
                        return unsupported("unsupported procedure argument type");
                    },
                ),
                _ => return unsupported("unsupported procedure argument declaration"),
            };
            let (data_type, declared_type) = parse_procedure_data_type(data_type)?;
            Ok(ParsedRoutineArgument {
                name,
                data_type,
                declared_type,
                mode,
            })
        })
        .collect()
}

fn parse_routine_argument_mode(value: &str) -> Option<RoutineArgumentMode> {
    if value.eq_ignore_ascii_case("IN") {
        Some(RoutineArgumentMode::In)
    } else if value.eq_ignore_ascii_case("OUT") {
        Some(RoutineArgumentMode::Out)
    } else if value.eq_ignore_ascii_case("INOUT") {
        Some(RoutineArgumentMode::InOut)
    } else if value.eq_ignore_ascii_case("VARIADIC") {
        Some(RoutineArgumentMode::Variadic)
    } else {
        None
    }
}

fn parse_procedure_data_type(value: &str) -> Result<(ScalarType, Option<ParsedObjectName>)> {
    let (value, is_array) = value
        .strip_suffix("[]")
        .map_or((value, false), |value| (value, true));
    let built_in = match value.to_ascii_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => Some(ScalarType::Boolean),
        "SMALLINT" | "INT2" => Some(ScalarType::Int16),
        "INT" | "INTEGER" | "INT4" => Some(ScalarType::Int32),
        "BIGINT" | "INT8" => Some(ScalarType::Int64),
        "REAL" | "FLOAT4" => Some(ScalarType::Float32),
        "DOUBLE PRECISION" | "FLOAT8" => Some(ScalarType::Float64),
        "TEXT" => Some(ScalarType::Text),
        "UUID" => Some(ScalarType::Uuid),
        "JSON" => Some(ScalarType::Json),
        "JSONB" => Some(ScalarType::Jsonb),
        _ => None,
    };
    let (data_type, declared_type) = if let Some(data_type) = built_in {
        (data_type, None)
    } else {
        let parts = value
            .split('.')
            .map(|part| {
                let (name, quoted) =
                    if part.starts_with('"') && part.ends_with('"') && part.len() >= 2 {
                        (&part[1..part.len() - 1], true)
                    } else {
                        (part, false)
                    };
                let valid = !name.is_empty()
                    && (quoted
                        || (name
                            .bytes()
                            .next()
                            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
                            && name.bytes().all(|byte| {
                                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
                            })));
                if !valid {
                    return Err(DbError::new(
                        SYNTAX_ERROR,
                        format!("invalid procedure argument type name {value}"),
                    ));
                }
                Ok(ParsedIdentifier {
                    name: Identifier::new(name, quoted),
                    position: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if !(1..=2).contains(&parts.len()) {
            return unsupported("procedure argument type names support at most one schema");
        }
        (ScalarType::Text, Some(ParsedObjectName { parts }))
    };
    Ok((
        if is_array {
            ScalarType::Array {
                element: Box::new(data_type),
            }
        } else {
            data_type
        },
        declared_type,
    ))
}

fn parse_dollar_quoted_body(value: &str) -> Result<String> {
    if !value.starts_with('$') {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "procedure body must be dollar quoted",
        ));
    }
    let delimiter_end = value[1..]
        .find('$')
        .map(|position| position + 1)
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "invalid dollar-quote delimiter"))?;
    let delimiter = &value[..=delimiter_end];
    if !delimiter[1..delimiter.len() - 1]
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(DbError::new(SYNTAX_ERROR, "invalid dollar-quote delimiter"));
    }
    let body_start = delimiter.len();
    let body_end = value[body_start..]
        .rfind(delimiter)
        .map(|position| position + body_start)
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "unterminated dollar-quoted body"))?;
    if !value[body_end + delimiter.len()..].trim().is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "unexpected SQL after procedure body",
        ));
    }
    Ok(value[body_start..body_end].to_owned())
}
