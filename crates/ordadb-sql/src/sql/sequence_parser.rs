
fn parse_alter_sequence(sql: &str) -> Result<Option<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let tokens = significant_tokens(trimmed);
    if tokens.len() < 3
        || !tokens
            .first()
            .is_some_and(|token| is_unquoted_word(token, "ALTER"))
        || !tokens
            .get(1)
            .is_some_and(|token| is_unquoted_word(token, "SEQUENCE"))
    {
        return Ok(None);
    }
    let mut cursor = 2usize;
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
    if tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "RENAME"))
    {
        cursor += 1;
        expect_keyword(&tokens, &mut cursor, "TO")?;
        let new_name = parse_token_identifier(&tokens, &mut cursor)?;
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::AlterSequenceRename {
            name,
            if_exists,
            new_name,
        }));
    }

    let mut options = ParsedAlterSequence::default();
    let mut seen = BTreeSet::new();
    while cursor < tokens.len() {
        if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "INCREMENT"))
        {
            unique_sequence_option(&mut seen, "INCREMENT")?;
            cursor += 1;
            if tokens
                .get(cursor)
                .is_some_and(|token| is_unquoted_word(token, "BY"))
            {
                cursor += 1;
            }
            options.increment = Some(parse_signed_i64(&tokens, &mut cursor)?);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "MINVALUE"))
        {
            unique_sequence_option(&mut seen, "MINVALUE")?;
            cursor += 1;
            options.min_value = Some(parse_signed_i64(&tokens, &mut cursor)?);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "MAXVALUE"))
        {
            unique_sequence_option(&mut seen, "MAXVALUE")?;
            cursor += 1;
            options.max_value = Some(parse_signed_i64(&tokens, &mut cursor)?);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "RESTART"))
        {
            unique_sequence_option(&mut seen, "RESTART")?;
            cursor += 1;
            if tokens
                .get(cursor)
                .is_some_and(|token| is_unquoted_word(token, "WITH"))
            {
                cursor += 1;
            }
            if cursor >= tokens.len() || alter_sequence_option_starts(&tokens[cursor]) {
                return unsupported("ALTER SEQUENCE RESTART requires an explicit value");
            }
            options.restart = Some(parse_signed_i64(&tokens, &mut cursor)?);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "CYCLE"))
        {
            unique_sequence_option(&mut seen, "CYCLE")?;
            cursor += 1;
            options.cycle = Some(true);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "NO"))
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| is_unquoted_word(token, "CYCLE"))
        {
            unique_sequence_option(&mut seen, "CYCLE")?;
            cursor += 2;
            options.cycle = Some(false);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "OWNED"))
        {
            unique_sequence_option(&mut seen, "OWNED")?;
            cursor += 1;
            expect_keyword(&tokens, &mut cursor, "BY")?;
            if tokens
                .get(cursor)
                .is_some_and(|token| is_unquoted_word(token, "NONE"))
            {
                cursor += 1;
                options.owner = Some(None);
            } else {
                let mut owner = parse_token_object_name(&tokens, &mut cursor, 3)?;
                if owner.parts.len() < 2 {
                    return Err(DbError::new(
                        SYNTAX_ERROR,
                        "OWNED BY requires table.column or schema.table.column",
                    ));
                }
                let column = owner
                    .parts
                    .pop()
                    .ok_or_else(|| DbError::internal("validated sequence owner lost its column"))?;
                options.owner = Some(Some((owner, column)));
            }
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "NO"))
        {
            return unsupported("NO MINVALUE and NO MAXVALUE are not supported yet");
        } else {
            return unsupported(format!(
                "this ALTER SEQUENCE option is not supported: {}",
                tokens[cursor]
            ));
        }
    }
    if seen.is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "ALTER SEQUENCE requires an option or RENAME TO",
        ));
    }
    Ok(Some(ParsedStatement::AlterSequence {
        name,
        if_exists,
        options,
    }))
}

fn parse_token_object_name(
    tokens: &[Token],
    cursor: &mut usize,
    max_parts: usize,
) -> Result<ParsedObjectName> {
    let mut parts = vec![parse_token_identifier(tokens, cursor)?];
    while parts.len() < max_parts && tokens.get(*cursor) == Some(&Token::Period) {
        *cursor += 1;
        parts.push(parse_token_identifier(tokens, cursor)?);
    }
    Ok(ParsedObjectName { parts })
}

fn parse_token_identifier(tokens: &[Token], cursor: &mut usize) -> Result<ParsedIdentifier> {
    let Token::Word(word) = tokens
        .get(*cursor)
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "identifier expected"))?
    else {
        return Err(DbError::new(SYNTAX_ERROR, "identifier expected"));
    };
    *cursor += 1;
    Ok(ParsedIdentifier {
        name: Identifier::new(word.value.clone(), word.quote_style.is_some()),
        position: None,
    })
}

fn parse_signed_i64(tokens: &[Token], cursor: &mut usize) -> Result<i64> {
    let negative = tokens.get(*cursor) == Some(&Token::Minus);
    if negative {
        *cursor += 1;
    }
    let Token::Number(value, _) = tokens
        .get(*cursor)
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "integer value expected"))?
    else {
        return Err(DbError::new(SYNTAX_ERROR, "integer value expected"));
    };
    *cursor += 1;
    let value = value
        .parse::<i64>()
        .map_err(|_| DbError::new("22003", "sequence option is outside BIGINT range"))?;
    if negative {
        value
            .checked_neg()
            .ok_or_else(|| DbError::new("22003", "sequence option is outside BIGINT range"))
    } else {
        Ok(value)
    }
}

fn expect_keyword(tokens: &[Token], cursor: &mut usize, keyword: &str) -> Result<()> {
    if tokens
        .get(*cursor)
        .is_some_and(|token| is_unquoted_word(token, keyword))
    {
        *cursor += 1;
        Ok(())
    } else {
        Err(DbError::new(
            SYNTAX_ERROR,
            format!("expected keyword {keyword}"),
        ))
    }
}

fn ensure_token_end(tokens: &[Token], cursor: usize) -> Result<()> {
    if cursor == tokens.len() {
        Ok(())
    } else {
        Err(DbError::new(
            SYNTAX_ERROR,
            format!("unexpected token {}", tokens[cursor]),
        ))
    }
}

fn unique_sequence_option(seen: &mut BTreeSet<&'static str>, option: &'static str) -> Result<()> {
    if seen.insert(option) {
        Ok(())
    } else {
        Err(DbError::new(
            SYNTAX_ERROR,
            format!("ALTER SEQUENCE option {option} is specified more than once"),
        ))
    }
}

fn alter_sequence_option_starts(token: &Token) -> bool {
    [
        "INCREMENT",
        "MINVALUE",
        "MAXVALUE",
        "RESTART",
        "CYCLE",
        "NO",
        "OWNED",
    ]
    .iter()
    .any(|keyword| is_unquoted_word(token, keyword))
}

fn parse_refresh_materialized_view(sql: &str) -> Result<Option<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    const PREFIX: &str = "REFRESH MATERIALIZED VIEW ";
    if !trimmed
        .get(..PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
    {
        return Ok(None);
    }
    let mut target = trimmed[PREFIX.len()..].trim();
    if target
        .get(.."CONCURRENTLY ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CONCURRENTLY "))
    {
        return unsupported("REFRESH MATERIALIZED VIEW CONCURRENTLY is not supported");
    }
    let uppercase = target.to_ascii_uppercase();
    let with_data = if uppercase.ends_with(" WITH NO DATA") {
        target = target[..target.len() - " WITH NO DATA".len()].trim_end();
        false
    } else if uppercase.ends_with(" WITH DATA") {
        target = target[..target.len() - " WITH DATA".len()].trim_end();
        true
    } else {
        true
    };
    Ok(Some(ParsedStatement::RefreshMaterializedView {
        name: parse_simple_object_name(target)?,
        with_data,
    }))
}

fn parse_simple_object_name(value: &str) -> Result<ParsedObjectName> {
    if value.is_empty() {
        return Err(DbError::new(SYNTAX_ERROR, "object name is empty"));
    }
    let parts = value
        .split('.')
        .map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return Err(DbError::new(SYNTAX_ERROR, "object name has an empty part"));
            }
            let name = if part.starts_with('"') && part.ends_with('"') && part.len() >= 2 {
                Identifier::quoted(part[1..part.len() - 1].replace("\"\"", "\""))
            } else if part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
            {
                Identifier::unquoted(part)
            } else {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    format!("invalid object-name part {part}"),
                ));
            };
            Ok(ParsedIdentifier {
                name,
                position: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if parts.len() > 2 {
        return unsupported("database-qualified object names are not supported");
    }
    Ok(ParsedObjectName { parts })
}

fn materialized_view_parser_sql(sql: &str) -> Cow<'_, str> {
    let trimmed = sql.trim();
    let without_semicolon = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    let uppercase = without_semicolon.to_ascii_uppercase();
    if !uppercase.starts_with("CREATE MATERIALIZED VIEW ") {
        return Cow::Borrowed(sql);
    }
    for suffix in [" WITH NO DATA", " WITH DATA"] {
        if uppercase.ends_with(suffix) {
            let prefix_len = without_semicolon.len().saturating_sub(suffix.len());
            return Cow::Owned(without_semicolon[..prefix_len].trim_end().to_owned());
        }
    }
    Cow::Borrowed(sql)
}

/// Parse and bind a persisted catalog expression without leaking sqlparser
/// nodes across the SQL boundary.
pub fn bind_catalog_expression(
    expression: &CatalogExpression,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
) -> Result<BoundExpr> {
    bind_catalog_expression_with_parameter_types_and_catalog(
        expression,
        table,
        expected,
        &BTreeMap::new(),
        None,
    )
}

/// Bind a persisted catalog expression with access to user-defined types.
///
/// The original catalog-expression entry point remains available for callers
/// that cannot contain named casts. Durable expressions owned by a catalog
/// should use this overload so enum/domain casts are resolved from the same
/// immutable catalog snapshot that owns the expression.
pub fn bind_catalog_expression_with_catalog(
    expression: &CatalogExpression,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
    catalog: &Catalog,
) -> Result<BoundExpr> {
    bind_catalog_expression_with_parameter_types_and_catalog(
        expression,
        table,
        expected,
        &BTreeMap::new(),
        Some(catalog),
    )
}

/// Bind a persisted expression with caller-owned positional parameter types.
///
/// This is used by procedural contexts where a parameter represents a typed
/// transient record field rather than an untyped client placeholder.
pub fn bind_catalog_expression_with_parameter_types(
    expression: &CatalogExpression,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
    parameter_types: &BTreeMap<usize, ScalarType>,
) -> Result<BoundExpr> {
    bind_catalog_expression_with_parameter_types_and_catalog(
        expression,
        table,
        expected,
        parameter_types,
        None,
    )
}

/// Bind a persisted expression with positional parameter types and an
/// optional catalog used to resolve named enum/domain casts.
pub fn bind_catalog_expression_with_parameter_types_and_catalog(
    expression: &CatalogExpression,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
) -> Result<BoundExpr> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(&expression.sql)
        .map_err(|error| DbError::new(SYNTAX_ERROR, error.to_string()))?;
    let parsed = parser
        .parse_expr()
        .map_err(|error| DbError::new(SYNTAX_ERROR, error.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "catalog expression contains trailing SQL",
        ));
    }
    let mut expression = convert_expr(parsed, &expression.sql)?;
    resolve_expr_types(&mut expression, parameter_types, catalog, 0, None)?;
    bind_expr_with_parameter_types(expression, table, expected, parameter_types)
}

/// Bind an OrdaDB-owned parsed statement against an immutable catalog view.
pub fn bind(statement: ParsedStatement, catalog: &Catalog) -> Result<BoundStatement> {
    bind_internal(statement, catalog, None)
}

/// Bind a parsed statement with the values owned by the current database
/// session. Session-dependent scalar functions are materialized before type
/// solving so Describe and Execute observe one immutable statement value.
pub fn bind_with_session(
    statement: ParsedStatement,
    catalog: &Catalog,
    session: SessionBindValues<'_>,
) -> Result<BoundStatement> {
    bind_internal(statement, catalog, Some(session))
}

fn bind_internal(
    mut statement: ParsedStatement,
    catalog: &Catalog,
    session: Option<SessionBindValues<'_>>,
) -> Result<BoundStatement> {
    resolve_statement_types(&mut statement, &BTreeMap::new(), Some(catalog), 0, session)?;
    let parameter_types = ParameterTypeSolver::solve(&statement, catalog)?;
    resolve_statement_types(&mut statement, &parameter_types, None, 0, session)?;
    bind_with_view_depth(statement, catalog, 0)
}

const MAX_PARAMETER_SOLVER_PASSES: usize = 128;
const MAX_PARAMETER_SOLVER_DEPTH: usize = 64;

#[derive(Default)]
struct ParameterTypeSolver {
    types: BTreeMap<usize, ScalarType>,
    changed: bool,
}
