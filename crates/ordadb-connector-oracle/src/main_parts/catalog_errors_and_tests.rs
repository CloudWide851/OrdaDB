
fn catalog_page(
    connection: &Connection,
    parent_id: Option<&str>,
    page_size: u32,
    cursor: Option<&str>,
) -> Result<ConnectorCatalogPageV3> {
    let offset = parse_catalog_cursor(cursor)?;
    let limit = page_size.saturating_add(1);
    let parent = parse_catalog_parent(parent_id)?;
    let rows = match &parent {
        CatalogParent::Root => catalog_rows(
            connection,
            "SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY",
            vec![
                OracleBind::Unsigned(offset),
                OracleBind::Unsigned(u64::from(limit)),
            ],
            limit,
        )?,
        CatalogParent::Schema(schema) => catalog_rows(
            connection,
            "SELECT DISTINCT OWNER, OBJECT_NAME, OBJECT_TYPE FROM ALL_OBJECTS WHERE OWNER = :1 AND OBJECT_TYPE IN ('TABLE','VIEW','MATERIALIZED VIEW','SEQUENCE','FUNCTION','PROCEDURE') ORDER BY OBJECT_NAME, OBJECT_TYPE OFFSET :2 ROWS FETCH NEXT :3 ROWS ONLY",
            vec![
                OracleBind::Text(schema.clone()),
                OracleBind::Unsigned(offset),
                OracleBind::Unsigned(u64::from(limit)),
            ],
            limit,
        )?,
        CatalogParent::Object { schema, name } => catalog_rows(
            connection,
            "SELECT ITEM_KIND, OWNER_NAME, TABLE_NAME, ITEM_NAME, TYPE_NAME, NULLABLE_VALUE, LENGTH_VALUE FROM (SELECT 'COLUMN' ITEM_KIND, OWNER OWNER_NAME, TABLE_NAME, COLUMN_NAME ITEM_NAME, DATA_TYPE TYPE_NAME, NULLABLE NULLABLE_VALUE, DATA_LENGTH LENGTH_VALUE FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2 UNION ALL SELECT 'INDEX' ITEM_KIND, TABLE_OWNER OWNER_NAME, TABLE_NAME, INDEX_NAME ITEM_NAME, INDEX_TYPE TYPE_NAME, 'N' NULLABLE_VALUE, 0 LENGTH_VALUE FROM ALL_INDEXES WHERE TABLE_OWNER = :3 AND TABLE_NAME = :4) ORDER BY ITEM_KIND, ITEM_NAME OFFSET :5 ROWS FETCH NEXT :6 ROWS ONLY",
            vec![
                OracleBind::Text(schema.clone()),
                OracleBind::Text(name.clone()),
                OracleBind::Text(schema.clone()),
                OracleBind::Text(name.clone()),
                OracleBind::Unsigned(offset),
                OracleBind::Unsigned(u64::from(limit)),
            ],
            limit,
        )?,
    };
    let mut nodes = catalog_nodes(&parent, rows)?;
    let has_more = nodes.len() > usize::try_from(page_size).unwrap_or(512);
    if has_more {
        nodes.pop();
    }
    Ok(ConnectorCatalogPageV3 {
        nodes,
        next_cursor: has_more.then(|| offset.saturating_add(u64::from(page_size)).to_string()),
    })
}

fn catalog_rows(
    connection: &Connection,
    sql: &str,
    parameters: Vec<OracleBind>,
    maximum_rows: u32,
) -> Result<Vec<Vec<String>>> {
    let parameters = parameters
        .iter()
        .map(OracleBind::as_to_sql)
        .collect::<Vec<_>>();
    let mut statement = connection
        .statement(sql)
        .fetch_array_size(maximum_rows.max(1))
        .build()
        .map_err(map_oracle_error)?;
    let rows = statement.query(&parameters).map_err(map_oracle_error)?;
    let mut output = Vec::new();
    for row in rows.take(usize::try_from(maximum_rows).unwrap_or(513)) {
        let row = row.map_err(map_oracle_error)?;
        let mut values = Vec::with_capacity(row.column_info().len());
        for index in 0..row.column_info().len() {
            values.push(bounded_string(&row, index)?);
        }
        output.push(values);
    }
    Ok(output)
}

fn catalog_nodes(
    parent: &CatalogParent,
    rows: Vec<Vec<String>>,
) -> Result<Vec<ConnectorCatalogNodeV3>> {
    rows.into_iter()
        .map(|row| match parent {
            CatalogParent::Root => {
                let schema = required_catalog_value(&row, 0, "schema name")?;
                Ok(ConnectorCatalogNodeV3 {
                    id: schema_id(schema),
                    parent_id: None,
                    kind: ConnectorCatalogNodeKindV3::Schema,
                    name: schema.to_owned(),
                    namespace: Some(schema.to_owned()),
                    has_children: true,
                    columns: Vec::new(),
                    attributes: BTreeMap::new(),
                })
            }
            CatalogParent::Schema(schema) => {
                let owner = required_catalog_value(&row, 0, "object owner")?;
                if owner != schema {
                    return Err(protocol_error(
                        "Oracle Catalog object escaped its parent schema",
                    ));
                }
                let name = required_catalog_value(&row, 1, "object name")?;
                let object_type = required_catalog_value(&row, 2, "object type")?;
                let (kind, has_children) = match object_type {
                    "TABLE" => (ConnectorCatalogNodeKindV3::Table, true),
                    "VIEW" => (ConnectorCatalogNodeKindV3::View, true),
                    "MATERIALIZED VIEW" => (ConnectorCatalogNodeKindV3::MaterializedView, true),
                    "SEQUENCE" => (ConnectorCatalogNodeKindV3::Sequence, false),
                    "FUNCTION" => (ConnectorCatalogNodeKindV3::Function, false),
                    "PROCEDURE" => (ConnectorCatalogNodeKindV3::Procedure, false),
                    _ => (ConnectorCatalogNodeKindV3::Other, false),
                };
                Ok(ConnectorCatalogNodeV3 {
                    id: object_id(schema, name),
                    parent_id: Some(schema_id(schema)),
                    kind,
                    name: name.to_owned(),
                    namespace: Some(schema.clone()),
                    has_children,
                    columns: Vec::new(),
                    attributes: BTreeMap::from([("oracleObjectType".into(), object_type.into())]),
                })
            }
            CatalogParent::Object { schema, name } => {
                let item_kind = required_catalog_value(&row, 0, "item kind")?;
                let owner = required_catalog_value(&row, 1, "item owner")?;
                let table = required_catalog_value(&row, 2, "item table")?;
                if owner != schema || table != name {
                    return Err(protocol_error(
                        "Oracle Catalog item escaped its parent object",
                    ));
                }
                let item_name = required_catalog_value(&row, 3, "item name")?;
                let type_name = required_catalog_value(&row, 4, "item type")?;
                let nullable = required_catalog_value(&row, 5, "item nullability")?;
                let length = required_catalog_value(&row, 6, "item length")?;
                let kind = if item_kind == "COLUMN" {
                    ConnectorCatalogNodeKindV3::Column
                } else if item_kind == "INDEX" {
                    ConnectorCatalogNodeKindV3::Index
                } else {
                    ConnectorCatalogNodeKindV3::Other
                };
                Ok(ConnectorCatalogNodeV3 {
                    id: catalog_item_id(schema, name, item_kind, item_name),
                    parent_id: Some(object_id(schema, name)),
                    kind,
                    name: item_name.to_owned(),
                    namespace: Some(format!("{schema}.{name}")),
                    has_children: false,
                    columns: Vec::new(),
                    attributes: BTreeMap::from([
                        ("type".into(), type_name.into()),
                        ("nullable".into(), (nullable == "Y").to_string()),
                        ("length".into(), length.into()),
                    ]),
                })
            }
        })
        .collect()
}

fn required_catalog_value<'a>(row: &'a [String], index: usize, name: &str) -> Result<&'a str> {
    row.get(index)
        .filter(|value| !value.is_empty() && value.len() <= MAX_IDENTIFIER_BYTES)
        .map(String::as_str)
        .ok_or_else(|| protocol_error(format!("Oracle Catalog {name} is invalid")))
}

fn parse_catalog_cursor(cursor: Option<&str>) -> Result<u64> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    if cursor.is_empty() || cursor.len() > 20 || !cursor.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid("Oracle Catalog cursor is invalid"));
    }
    cursor
        .parse()
        .map_err(|_| invalid("Oracle Catalog cursor is invalid"))
}

fn parse_catalog_parent(parent_id: Option<&str>) -> Result<CatalogParent> {
    let Some(parent_id) = parent_id else {
        return Ok(CatalogParent::Root);
    };
    if let Some(schema) = parent_id.strip_prefix("oracle:schema:") {
        return Ok(CatalogParent::Schema(decode_id_component(schema)?));
    }
    if let Some(object) = parent_id.strip_prefix("oracle:object:") {
        let mut components = object.split(':');
        let schema = components
            .next()
            .ok_or_else(|| invalid("Oracle Catalog object ID is invalid"))?;
        let name = components
            .next()
            .ok_or_else(|| invalid("Oracle Catalog object ID is invalid"))?;
        if components.next().is_some() {
            return Err(invalid("Oracle Catalog object ID is invalid"));
        }
        return Ok(CatalogParent::Object {
            schema: decode_id_component(schema)?,
            name: decode_id_component(name)?,
        });
    }
    Err(invalid("Oracle Catalog parent ID is invalid"))
}

fn schema_id(schema: &str) -> String {
    format!("oracle:schema:{}", encode_id_component(schema))
}

fn object_id(schema: &str, name: &str) -> String {
    format!(
        "oracle:object:{}:{}",
        encode_id_component(schema),
        encode_id_component(name)
    )
}

fn catalog_item_id(schema: &str, object: &str, kind: &str, name: &str) -> String {
    format!(
        "oracle:item:{}:{}:{}:{}",
        encode_id_component(schema),
        encode_id_component(object),
        encode_id_component(kind),
        encode_id_component(name)
    )
}

fn encode_id_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn decode_id_component(value: &str) -> Result<String> {
    if value.is_empty() || !value.len().is_multiple_of(2) || value.len() > 1_024 {
        return Err(invalid("Oracle Catalog ID component is invalid"));
    }
    let bytes = value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair)
                .map_err(|_| invalid("Oracle Catalog ID component is invalid"))?;
            u8::from_str_radix(pair, 16)
                .map_err(|_| invalid("Oracle Catalog ID component is invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    String::from_utf8(bytes).map_err(|_| invalid("Oracle Catalog ID component is invalid"))
}

fn begin_transaction(
    connection: &Connection,
    isolation: Option<ConnectorIsolationLevelV2>,
) -> Result<()> {
    let sql = match isolation {
        None | Some(ConnectorIsolationLevelV2::ReadCommitted) => {
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
        }
        Some(ConnectorIsolationLevelV2::Serializable) => {
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
        }
        Some(ConnectorIsolationLevelV2::ReadUncommitted) => {
            return Err(DbError::unsupported("Oracle READ UNCOMMITTED isolation"));
        }
        Some(ConnectorIsolationLevelV2::RepeatableRead) => {
            return Err(DbError::unsupported("Oracle REPEATABLE READ isolation"));
        }
    };
    connection
        .execute(sql, &[])
        .map(|_| ())
        .map_err(map_oracle_error)
}

fn capabilities() -> ConnectorCapabilitiesV3 {
    ConnectorCapabilitiesV3 {
        kind: ConnectorKindV3::Sql,
        command_languages: vec![ConnectorCommandLanguageV3 {
            id: LANGUAGE_ID.into(),
            display_name: "Oracle SQL".into(),
            input_modes: vec![ConnectorCommandInputModeV3::Text],
        }],
        catalog: true,
        cancellation: true,
        transactions: true,
        savepoints: true,
        batch_query: true,
        maximum_batch_rows: MAX_BATCH_ROWS,
        maximum_catalog_page_size: MAX_CATALOG_PAGE_SIZE,
        tls_modes: vec![ConnectorTlsModeV2::Disable, ConnectorTlsModeV2::Require],
    }
}

fn map_oracle_error(error: OracleError) -> DbError {
    let oracle_code = error.oci_code();
    let dpi_code = error.dpi_code();
    if dpi_code == Some(1047) {
        return DbError::new("58000", "Oracle Instant Client could not be loaded")
            .with_detail("ODPI-C reported DPI-1047")
            .with_hint(
                "Install a compatible Windows x64 Oracle Instant Client and add its directory to PATH.",
            );
    }
    let sql_state = oracle_code.map_or_else(
        || match error.kind() {
            OracleErrorKind::InvalidArgument
            | OracleErrorKind::InvalidBindIndex
            | OracleErrorKind::InvalidBindName
            | OracleErrorKind::InvalidColumnIndex
            | OracleErrorKind::InvalidColumnName
            | OracleErrorKind::InvalidTypeConversion
            | OracleErrorKind::ParseError
            | OracleErrorKind::OutOfRange => "22023",
            OracleErrorKind::NoDataFound => "02000",
            OracleErrorKind::DpiError | OracleErrorKind::OciError => "58000",
            _ => "XX000",
        },
        oracle_sql_state,
    );
    let message = oracle_code.map_or("Oracle driver operation failed", oracle_message);
    let mut mapped = DbError::new(sql_state, message);
    if let Some(database_error) = error.db_error() {
        let code = if oracle_code.is_some() { "ORA" } else { "DPI" };
        mapped = mapped.with_detail(format!(
            "{code} error {} at offset {}",
            database_error.code(),
            database_error.offset()
        ));
        if database_error.offset() > 0 {
            mapped = mapped
                .with_position(usize::try_from(database_error.offset()).unwrap_or(usize::MAX));
        }
    } else if let Some(dpi_code) = dpi_code {
        mapped = mapped.with_detail(format!("ODPI-C error DPI-{dpi_code:04}"));
    }
    mapped
}

fn oracle_sql_state(code: i32) -> &'static str {
    match code {
        1 => "23505",
        54 => "55P03",
        60 => "40P01",
        904 => "42703",
        942 => "42P01",
        1013 => "57014",
        1017 => "28P01",
        1031 => "42501",
        2291 | 2292 => "23503",
        12154 | 12514 | 12541 => "08001",
        12528 | 12537 | 12545 | 12560 | 12571 => "08006",
        _ => "58000",
    }
}

fn oracle_message(code: i32) -> &'static str {
    match code {
        1 => "Oracle unique constraint was violated",
        54 => "Oracle resource is busy",
        60 => "Oracle transaction was deadlocked",
        904 => "Oracle column does not exist",
        942 => "Oracle table or view does not exist",
        1013 => "Oracle query was cancelled",
        1017 => "Oracle authentication failed",
        1031 => "Oracle permission was denied",
        2291 | 2292 => "Oracle foreign key constraint was violated",
        12154 | 12514 | 12541 => "Oracle endpoint could not be resolved",
        12528 | 12537 | 12545 | 12560 | 12571 => "Oracle connection failed",
        _ => "Oracle database operation failed",
    }
}

fn worker_closed() -> DbError {
    DbError::new("08006", "Oracle connector worker is not running")
}

fn cancelled() -> DbError {
    DbError::new("57014", "Oracle query was cancelled")
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn limit_error(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::new("08P01", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_types_and_catalog_ids_are_stable() {
        let capabilities = capabilities();
        assert_eq!(capabilities.kind, ConnectorKindV3::Sql);
        assert_eq!(capabilities.command_languages[0].id, LANGUAGE_ID);
        assert!(capabilities.transactions);
        assert!(capabilities.cancellation);
        assert_eq!(capabilities.maximum_batch_rows, MAX_BATCH_ROWS);

        let number = connector_type(&OracleType::Number(18, 4));
        assert_eq!(number.logical_type, ConnectorLogicalTypeV2::Decimal);
        assert_eq!(number.precision, Some(18));
        assert_eq!(number.scale, Some(4));
        let timestamp = connector_type(&OracleType::TimestampTZ(6));
        assert_eq!(
            timestamp.logical_type,
            ConnectorLogicalTypeV2::TimestampWithTimeZone
        );

        let name = "数据:ORDERS";
        let encoded = encode_id_component(name);
        assert_eq!(decode_id_component(&encoded).expect("decode"), name);
        assert_eq!(
            object_id("APP", "ORDERS"),
            "oracle:object:415050:4f5244455253"
        );
    }

    #[test]
    fn endpoint_and_parameter_validation_are_bounded_and_secret_safe() {
        let secret = "oracle-secret-that-must-not-leak";
        let result = connection_options(
            ConnectorEndpointV2::Network {
                host: "bad(host".into(),
                port: 1521,
                database: Some("ORCLPDB1".into()),
                instance: None,
                options: BTreeMap::new(),
            },
            ConnectorTlsModeV2::Disable,
            Some(ConnectorCredentialV2::new(Some("system".into()), secret)),
        );
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("descriptor injection must fail"),
        };
        assert_eq!(error.sql_state, "22023");
        assert!(!format!("{error:?}").contains(secret));

        let mut total = 0;
        let result = oracle_bind(
            &ConnectorParameterV2 {
                data_type: None,
                value: ConnectorValueV2::Array(vec![]),
            },
            &mut total,
        );
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("array parameter must fail explicitly"),
        };
        assert_eq!(error.sql_state, "0A000");
    }

    #[test]
    fn catalog_parent_and_error_mappings_are_deterministic() {
        let parent =
            parse_catalog_parent(Some(&object_id("APP", "ORDERS"))).expect("parse object parent");
        assert!(matches!(
            parent,
            CatalogParent::Object { schema, name } if schema == "APP" && name == "ORDERS"
        ));
        assert_eq!(oracle_sql_state(1), "23505");
        assert_eq!(oracle_sql_state(1017), "28P01");
        assert_eq!(oracle_sql_state(60), "40P01");
        assert_eq!(oracle_sql_state(1013), "57014");
        assert_eq!(oracle_sql_state(12541), "08001");
    }

    #[test]
    fn optional_real_oracle_matrix_fails_closed_when_required() {
        let required = std::env::var("ORDADB_REQUIRE_REAL_CONNECTOR_TESTS")
            .ok()
            .as_deref()
            == Some("1");
        let names = [
            "ORDADB_TEST_ORACLE_HOST",
            "ORDADB_TEST_ORACLE_SERVICE",
            "ORDADB_TEST_ORACLE_USERNAME",
            "ORDADB_TEST_ORACLE_PASSWORD",
        ];
        let configured = names
            .iter()
            .all(|name| std::env::var_os(name).is_some_and(|value| !value.is_empty()));
        assert!(
            !required || configured,
            "required Oracle connector matrix inputs are missing"
        );
        if !configured {
            return;
        }
        ordadb_windows::discover_amd64_oracle_client(None)
            .expect("required Oracle Instant Client is unavailable");
        let host = std::env::var("ORDADB_TEST_ORACLE_HOST").expect("host");
        let service = std::env::var("ORDADB_TEST_ORACLE_SERVICE").expect("service");
        let username = std::env::var("ORDADB_TEST_ORACLE_USERNAME").expect("username");
        let password =
            Zeroizing::new(std::env::var("ORDADB_TEST_ORACLE_PASSWORD").expect("password"));
        let port = std::env::var("ORDADB_TEST_ORACLE_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(1521);
        let connection = Connection::connect(
            username,
            password.as_str(),
            format!("{host}:{port}/{service}"),
        )
        .expect("connect real Oracle matrix");
        let value: String = connection
            .query_row_as("SELECT 'ordadb-oracle' FROM DUAL", &[])
            .expect("query real Oracle matrix");
        assert_eq!(value, "ordadb-oracle");
        connection.close().expect("close real Oracle matrix");
    }
}
