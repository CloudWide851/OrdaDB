
fn parse_transaction_begin(sql: &str) -> Result<Option<ParsedStatement>> {
    let mut tokens = significant_tokens(sql);
    if matches!(tokens.last(), Some(Token::SemiColon)) {
        tokens.pop();
    }
    let mut index = 0_usize;
    if consume_keyword(&tokens, &mut index, "BEGIN") {
        if token_is_keyword(&tokens, index, "WORK")
            || token_is_keyword(&tokens, index, "TRANSACTION")
        {
            index += 1;
        }
    } else if consume_keyword(&tokens, &mut index, "START") {
        if !consume_keyword(&tokens, &mut index, "TRANSACTION") {
            return Ok(None);
        }
    } else {
        return Ok(None);
    }

    let mut characteristics = TransactionCharacteristics::default();
    let mut isolation_seen = false;
    let mut access_seen = false;
    let mut deferrable_seen = false;
    while index < tokens.len() {
        if matches!(tokens.get(index), Some(Token::Comma)) {
            return Err(transaction_syntax_error(
                "transaction mode is missing before comma",
            ));
        }
        if consume_keyword(&tokens, &mut index, "ISOLATION") {
            if isolation_seen {
                return Err(transaction_syntax_error(
                    "transaction isolation level was specified more than once",
                ));
            }
            isolation_seen = true;
            expect_keyword(&tokens, &mut index, "LEVEL")?;
            characteristics.isolation_level =
                if consume_keyword_sequence(&tokens, &mut index, &["READ", "UNCOMMITTED"])
                    || consume_keyword_sequence(&tokens, &mut index, &["READ", "COMMITTED"])
                {
                    IsolationLevel::ReadCommitted
                } else if consume_keyword_sequence(&tokens, &mut index, &["REPEATABLE", "READ"]) {
                    IsolationLevel::RepeatableRead
                } else if consume_keyword(&tokens, &mut index, "SERIALIZABLE") {
                    IsolationLevel::Serializable
                } else if consume_keyword(&tokens, &mut index, "SNAPSHOT") {
                    return unsupported("SNAPSHOT isolation is not supported");
                } else {
                    return Err(transaction_syntax_error(
                        "expected READ COMMITTED, REPEATABLE READ, or SERIALIZABLE",
                    ));
                };
        } else if consume_keyword_sequence(&tokens, &mut index, &["READ", "ONLY"]) {
            if access_seen {
                return Err(transaction_syntax_error(
                    "transaction access mode was specified more than once",
                ));
            }
            access_seen = true;
            characteristics.access_mode = TransactionAccessMode::ReadOnly;
        } else if consume_keyword_sequence(&tokens, &mut index, &["READ", "WRITE"]) {
            if access_seen {
                return Err(transaction_syntax_error(
                    "transaction access mode was specified more than once",
                ));
            }
            access_seen = true;
            characteristics.access_mode = TransactionAccessMode::ReadWrite;
        } else if consume_keyword(&tokens, &mut index, "DEFERRABLE") {
            if deferrable_seen {
                return Err(transaction_syntax_error(
                    "transaction deferrability was specified more than once",
                ));
            }
            deferrable_seen = true;
            characteristics.deferrable = true;
        } else if consume_keyword_sequence(&tokens, &mut index, &["NOT", "DEFERRABLE"]) {
            if deferrable_seen {
                return Err(transaction_syntax_error(
                    "transaction deferrability was specified more than once",
                ));
            }
            deferrable_seen = true;
            characteristics.deferrable = false;
        } else {
            return Err(transaction_syntax_error(
                "unrecognized transaction mode or trailing token",
            ));
        }
        if matches!(tokens.get(index), Some(Token::Comma)) {
            index += 1;
            if index == tokens.len() || matches!(tokens.get(index), Some(Token::Comma)) {
                return Err(transaction_syntax_error(
                    "transaction mode is missing after comma",
                ));
            }
        }
    }
    Ok(Some(ParsedStatement::Begin {
        characteristics: characteristics.validate()?,
    }))
}

fn convert_transaction_modes(
    modes: Vec<TransactionMode>,
    deferrable: bool,
) -> Result<TransactionCharacteristics> {
    let mut characteristics = TransactionCharacteristics {
        deferrable,
        ..TransactionCharacteristics::default()
    };
    let mut isolation_seen = false;
    let mut access_seen = false;
    for mode in modes {
        match mode {
            TransactionMode::IsolationLevel(level) => {
                if isolation_seen {
                    return Err(transaction_syntax_error(
                        "transaction isolation level was specified more than once",
                    ));
                }
                isolation_seen = true;
                characteristics.isolation_level = match level {
                    SqlTransactionIsolationLevel::ReadUncommitted
                    | SqlTransactionIsolationLevel::ReadCommitted => IsolationLevel::ReadCommitted,
                    SqlTransactionIsolationLevel::RepeatableRead => IsolationLevel::RepeatableRead,
                    SqlTransactionIsolationLevel::Serializable => IsolationLevel::Serializable,
                    SqlTransactionIsolationLevel::Snapshot => {
                        return unsupported("SNAPSHOT isolation is not supported");
                    }
                };
            }
            TransactionMode::AccessMode(mode) => {
                if access_seen {
                    return Err(transaction_syntax_error(
                        "transaction access mode was specified more than once",
                    ));
                }
                access_seen = true;
                characteristics.access_mode = match mode {
                    SqlTransactionAccessMode::ReadOnly => TransactionAccessMode::ReadOnly,
                    SqlTransactionAccessMode::ReadWrite => TransactionAccessMode::ReadWrite,
                };
            }
        }
    }
    characteristics.validate()
}

fn convert_transaction_chain(chain: bool, sql: &str) -> TransactionChain {
    if chain {
        TransactionChain::Chain
    } else if has_keyword_sequence(sql, &["AND", "NO", "CHAIN"]) {
        TransactionChain::NoChain
    } else {
        TransactionChain::Default
    }
}

fn consume_keyword(tokens: &[Token], index: &mut usize, keyword: &str) -> bool {
    if token_is_keyword(tokens, *index, keyword) {
        *index += 1;
        true
    } else {
        false
    }
}

fn consume_keyword_sequence(tokens: &[Token], index: &mut usize, keywords: &[&str]) -> bool {
    if keywords
        .iter()
        .enumerate()
        .all(|(offset, keyword)| token_is_keyword(tokens, *index + offset, keyword))
    {
        *index += keywords.len();
        true
    } else {
        false
    }
}

fn token_is_keyword(tokens: &[Token], index: usize, keyword: &str) -> bool {
    tokens
        .get(index)
        .is_some_and(|token| is_unquoted_word(token, keyword))
}

fn transaction_syntax_error(message: impl Into<String>) -> DbError {
    DbError::new(SYNTAX_ERROR, message)
}

fn has_keyword_sequence(sql: &str, sequence: &[&str]) -> bool {
    significant_tokens(sql)
        .windows(sequence.len())
        .any(|window| {
            window
                .iter()
                .zip(sequence)
                .all(|(token, keyword)| is_unquoted_word(token, keyword))
        })
}

fn parse_vacuum_analyze(sql: &str) -> Result<Option<ParsedStatement>> {
    let tokens = significant_tokens(sql);
    if tokens.len() < 2
        || !is_unquoted_word(&tokens[0], "VACUUM")
        || !is_unquoted_word(&tokens[1], "ANALYZE")
    {
        return Ok(None);
    }
    let mut normalized = String::from("VACUUM");
    for token in &tokens[2..] {
        normalized.push(' ');
        normalized.push_str(&token.to_string());
    }
    let mut statements = parse_source_statements(&normalized, SqlDialect::PostgreSql)
        .map_err(|error| DbError::new(SYNTAX_ERROR, error.to_string()))?;
    if statements.len() != 1 {
        return Err(DbError::new(
            FEATURE_NOT_SUPPORTED,
            "exactly one SQL statement must be executed at a time",
        ));
    }
    let parsed = convert_statement(
        statements
            .pop()
            .ok_or_else(|| DbError::new(SYNTAX_ERROR, "VACUUM statement is empty"))?,
        &normalized,
    )?;
    let ParsedStatement::Vacuum { table, .. } = parsed else {
        return Err(DbError::internal(
            "VACUUM ANALYZE normalization produced a non-VACUUM statement",
        ));
    };
    Ok(Some(ParsedStatement::Vacuum {
        table,
        analyze: true,
    }))
}

fn significant_tokens(sql: &str) -> Vec<Token> {
    let dialect = PostgreSqlDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Vec::new();
    };
    tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect()
}

fn is_unquoted_word(token: &Token, expected: &str) -> bool {
    matches!(
        token,
        Token::Word(word)
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected)
    )
}

fn span_position(sql: &str, span: Span) -> Option<usize> {
    location_position(sql, span.start)
}

fn location_position(sql: &str, location: Location) -> Option<usize> {
    if location.line == 0 || location.column == 0 {
        return None;
    }
    let target_line = usize::try_from(location.line).ok()?;
    let target_column = usize::try_from(location.column).ok()?;
    let mut position = 1usize;
    for (line_index, line) in sql.split_inclusive('\n').enumerate() {
        if line_index + 1 == target_line {
            let column_offset = line.chars().take(target_column.saturating_sub(1)).count();
            return Some(position + column_offset);
        }
        position += line.chars().count();
    }
    None
}

fn parser_error_position(sql: &str, message: &str) -> Option<usize> {
    let marker = " at Line: ";
    let start = message.rfind(marker)? + marker.len();
    let (line, column) = message[start..].split_once(", Column: ")?;
    location_position(sql, Location::new(line.parse().ok()?, column.parse().ok()?))
}

trait DbErrorPosition {
    fn with_position_opt(self, position: Option<usize>) -> Self;
}

impl DbErrorPosition for DbError {
    fn with_position_opt(mut self, position: Option<usize>) -> Self {
        self.position = position;
        self
    }
}

#[must_use]
pub fn compare_scalar_types(left: &ScalarType, right: &ScalarType) -> Ordering {
    numeric_rank(left).cmp(&numeric_rank(right))
}

#[cfg(test)]
mod tests {
    include!("tests_parsing.rs");
    include!("tests_binding.rs");
    include!("tests_postgres.rs");
    include!("tests_dialects.rs");
}
