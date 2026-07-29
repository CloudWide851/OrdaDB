use std::collections::BTreeMap;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use mysql_async::{
    Column, Conn, Error as MySqlError, Opts, OptsBuilder, Params, Row, SslOpts,
    Value as MySqlValue, prelude::Queryable,
};
use ordadb_connector_sdk::{
    ConnectorBatchV2, ConnectorCapabilitiesV2, ConnectorCatalogColumnV2,
    ConnectorCatalogObjectKindV2, ConnectorCatalogObjectV2, ConnectorColumnV2,
    ConnectorCredentialV2, ConnectorDriver, ConnectorEndpointV2, ConnectorEventSink,
    ConnectorIsolationLevelV2, ConnectorLogicalTypeV2, ConnectorParameterV2, ConnectorQueryEventV2,
    ConnectorSession, ConnectorTlsModeV2, ConnectorTypeV2, ConnectorValueV2,
    connector_pipe_argument, run_named_pipe_helper,
};
use ordadb_types::{DbError, Result};
use serde_json::Value as JsonValue;
use tokio_util::sync::CancellationToken;

const PLUGIN_ID: &str = "mysql";

#[derive(Debug, Default)]
struct MySqlDriver;

struct MySqlConnectOptions {
    host: String,
    port: u16,
    database: Option<String>,
    username: String,
    secret: zeroize::Zeroizing<String>,
    tls_mode: ConnectorTlsModeV2,
}

struct MySqlSession {
    connection: Conn,
    connection_id: u64,
    options: MySqlConnectOptions,
    capabilities: ConnectorCapabilitiesV2,
}

#[derive(Debug)]
struct TableMetadata {
    catalog: String,
    name: String,
    kind: ConnectorCatalogObjectKindV2,
    columns: Vec<ConnectorCatalogColumnV2>,
}

#[tokio::main]
async fn main() {
    let result = async {
        let pipe = connector_pipe_argument()?;
        run_named_pipe_helper(&pipe, PLUGIN_ID, env!("CARGO_PKG_VERSION"), MySqlDriver).await
    }
    .await;
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[async_trait]
impl ConnectorDriver for MySqlDriver {
    fn capabilities(&self) -> ConnectorCapabilitiesV2 {
        capabilities()
    }

    async fn connect(
        &self,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<Box<dyn ConnectorSession>> {
        let ConnectorEndpointV2::Network {
            host,
            port,
            database,
            instance,
            options,
        } = endpoint
        else {
            return Err(invalid("MySQL requires a network endpoint"));
        };
        if instance.is_some() {
            return Err(invalid("MySQL endpoints do not accept an instance name"));
        }
        if !options.is_empty() {
            return Err(DbError::unsupported("MySQL connector endpoint options"));
        }
        let credential =
            credential.ok_or_else(|| DbError::new("28000", "MySQL credentials are required"))?;
        let username = credential
            .username
            .filter(|username| !username.trim().is_empty())
            .ok_or_else(|| DbError::new("28000", "MySQL username is required"))?;
        let options = MySqlConnectOptions {
            host,
            port,
            database,
            username,
            secret: credential.secret,
            tls_mode,
        };
        let (connection, connection_id) = connect_mysql(&options).await?;
        Ok(Box::new(MySqlSession {
            connection,
            connection_id,
            options,
            capabilities: capabilities(),
        }))
    }
}

#[async_trait]
impl ConnectorSession for MySqlSession {
    fn capabilities(&self) -> &ConnectorCapabilitiesV2 {
        &self.capabilities
    }

    async fn catalog(&mut self) -> Result<Vec<ConnectorCatalogObjectV2>> {
        let rows = self
            .connection
            .exec::<Row, _, _>(
                "SELECT c.TABLE_SCHEMA,
                        c.TABLE_NAME,
                        t.TABLE_TYPE,
                        c.COLUMN_NAME,
                        c.ORDINAL_POSITION,
                        c.IS_NULLABLE,
                        c.DATA_TYPE,
                        c.COLUMN_TYPE,
                        c.COLUMN_DEFAULT
                 FROM information_schema.COLUMNS AS c
                 JOIN information_schema.TABLES AS t
                   ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
                  AND t.TABLE_NAME = c.TABLE_NAME
                 WHERE c.TABLE_SCHEMA NOT IN
                       ('information_schema', 'mysql', 'performance_schema', 'sys')
                   AND (? IS NULL OR c.TABLE_SCHEMA = ?)
                 ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION",
                (self.options.database.clone(), self.options.database.clone()),
            )
            .await
            .map_err(mysql_error)?;
        catalog_from_rows(rows)
    }

    async fn execute(
        &mut self,
        _request_id: &str,
        sql: &str,
        params: &[ConnectorParameterV2],
        batch_size: u32,
        cancellation: &CancellationToken,
        sink: &mut dyn ConnectorEventSink,
    ) -> Result<()> {
        if batch_size == 0 || batch_size > self.capabilities.maximum_batch_rows {
            return Err(invalid(
                "MySQL connector batch size is outside its capability",
            ));
        }
        let params = Params::Positional(
            params
                .iter()
                .map(mysql_parameter)
                .collect::<Result<Vec<_>>>()?,
        );
        let mut result = self
            .connection
            .exec_iter(sql, params)
            .await
            .map_err(mysql_error)?;
        let columns = result
            .columns_ref()
            .iter()
            .map(mysql_column)
            .collect::<Vec<_>>();
        sink.send(ConnectorQueryEventV2::Schema { columns }).await?;

        let batch_size = usize::try_from(batch_size).unwrap_or(1024);
        let mut batch = Vec::with_capacity(batch_size);
        let mut processed = 0_u64;
        loop {
            let row = tokio::select! {
                next = result.next() => next.map_err(mysql_error)?,
                () = cancellation.cancelled() => {
                    let _ = kill_mysql_query(&self.options, self.connection_id).await;
                    let _ = result.drop_result().await;
                    return Err(DbError::new("57014", "MySQL query was cancelled"));
                }
            };
            let Some(row) = row else {
                break;
            };
            batch.push(mysql_row(&row)?);
            processed = processed.saturating_add(1);
            if batch.len() == batch_size {
                sink.send(ConnectorQueryEventV2::Batch {
                    batch: ConnectorBatchV2 {
                        rows: std::mem::take(&mut batch),
                    },
                })
                .await?;
                sink.send(ConnectorQueryEventV2::Progress {
                    rows_processed: processed,
                })
                .await?;
            }
        }
        let affected_rows = if processed == 0 {
            result.affected_rows()
        } else {
            processed
        };
        if !batch.is_empty() {
            sink.send(ConnectorQueryEventV2::Batch {
                batch: ConnectorBatchV2 { rows: batch },
            })
            .await?;
        }
        sink.send(ConnectorQueryEventV2::Progress {
            rows_processed: affected_rows,
        })
        .await?;
        sink.send(ConnectorQueryEventV2::Complete {
            command_tag: command_tag(sql),
            affected_rows: Some(affected_rows),
        })
        .await
    }

    async fn cancel(&mut self, _request_id: &str) -> Result<()> {
        kill_mysql_query(&self.options, self.connection_id).await
    }

    async fn begin(&mut self, isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
        if let Some(isolation) = isolation {
            let level = match isolation {
                ConnectorIsolationLevelV2::ReadUncommitted => "READ UNCOMMITTED",
                ConnectorIsolationLevelV2::ReadCommitted => "READ COMMITTED",
                ConnectorIsolationLevelV2::RepeatableRead => "REPEATABLE READ",
                ConnectorIsolationLevelV2::Serializable => "SERIALIZABLE",
            };
            self.connection
                .query_drop(format!("SET TRANSACTION ISOLATION LEVEL {level}"))
                .await
                .map_err(mysql_error)?;
        }
        self.connection
            .query_drop("START TRANSACTION")
            .await
            .map_err(mysql_error)
    }

    async fn commit(&mut self) -> Result<()> {
        self.connection
            .query_drop("COMMIT")
            .await
            .map_err(mysql_error)
    }

    async fn rollback(&mut self) -> Result<()> {
        self.connection
            .query_drop("ROLLBACK")
            .await
            .map_err(mysql_error)
    }
}

fn capabilities() -> ConnectorCapabilitiesV2 {
    ConnectorCapabilitiesV2 {
        catalog: true,
        cancellation: true,
        transactions: true,
        savepoints: true,
        batch_query: true,
        maximum_batch_rows: 1024,
        tls_modes: vec![
            ConnectorTlsModeV2::Disable,
            ConnectorTlsModeV2::Require,
            ConnectorTlsModeV2::VerifyCa,
            ConnectorTlsModeV2::VerifyFull,
        ],
    }
}

async fn connect_mysql(options: &MySqlConnectOptions) -> Result<(Conn, u64)> {
    let mut connection = Conn::new(mysql_options(options))
        .await
        .map_err(mysql_error)?;
    let connection_id = connection
        .query_first::<u64, _>("SELECT CONNECTION_ID()")
        .await
        .map_err(mysql_error)?
        .ok_or_else(|| DbError::new("08006", "MySQL did not return a connection ID"))?;
    Ok((connection, connection_id))
}

fn mysql_options(options: &MySqlConnectOptions) -> Opts {
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(&options.host)
        .tcp_port(options.port)
        .user(Some(&options.username))
        .pass(Some(options.secret.as_str()))
        .db_name(options.database.as_ref())
        .prefer_socket(false);
    let ssl = match options.tls_mode {
        ConnectorTlsModeV2::Disable => None,
        ConnectorTlsModeV2::Require => Some(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ),
        ConnectorTlsModeV2::VerifyCa => {
            Some(SslOpts::default().with_danger_skip_domain_validation(true))
        }
        ConnectorTlsModeV2::VerifyFull => Some(SslOpts::default()),
        ConnectorTlsModeV2::Prefer => None,
    };
    builder = builder.ssl_opts(ssl);
    builder.into()
}

async fn kill_mysql_query(options: &MySqlConnectOptions, connection_id: u64) -> Result<()> {
    let mut control = Conn::new(mysql_options(options))
        .await
        .map_err(mysql_error)?;
    let result = control
        .query_drop(format!("KILL QUERY {connection_id}"))
        .await
        .map_err(mysql_error);
    let _ = control.disconnect().await;
    result
}

fn catalog_from_rows(rows: Vec<Row>) -> Result<Vec<ConnectorCatalogObjectV2>> {
    let mut tables = BTreeMap::<(String, String), TableMetadata>::new();
    for row in rows {
        let catalog = mysql_row_text(&row, 0, "TABLE_SCHEMA")?;
        let table = mysql_row_text(&row, 1, "TABLE_NAME")?;
        let table_type = mysql_row_text(&row, 2, "TABLE_TYPE")?;
        let column = mysql_row_text(&row, 3, "COLUMN_NAME")?;
        let ordinal = mysql_row_u32(&row, 4, "ORDINAL_POSITION")?;
        let nullable = mysql_row_text(&row, 5, "IS_NULLABLE")? == "YES";
        let data_type = mysql_row_text(&row, 6, "DATA_TYPE")?;
        let column_type = mysql_row_text(&row, 7, "COLUMN_TYPE")?;
        let default_expression = mysql_row_optional_text(&row, 8)?;
        let metadata = tables
            .entry((catalog.clone(), table.clone()))
            .or_insert_with(|| TableMetadata {
                catalog: catalog.clone(),
                name: table.clone(),
                kind: if table_type == "VIEW" {
                    ConnectorCatalogObjectKindV2::View
                } else {
                    ConnectorCatalogObjectKindV2::Table
                },
                columns: Vec::new(),
            });
        metadata.columns.push(ConnectorCatalogColumnV2 {
            name: column,
            ordinal,
            data_type: mysql_named_type(&data_type, &column_type),
            nullable,
            default_expression,
        });
    }

    let mut objects = Vec::new();
    let mut catalogs = BTreeMap::<String, ()>::new();
    for table in tables.into_values() {
        if catalogs.insert(table.catalog.clone(), ()).is_none() {
            objects.push(ConnectorCatalogObjectV2 {
                id: format!("mysql:database:{}", table.catalog),
                kind: ConnectorCatalogObjectKindV2::Database,
                catalog: Some(table.catalog.clone()),
                schema: None,
                name: table.catalog.clone(),
                parent_id: None,
                comment: None,
                columns: Vec::new(),
                attributes: BTreeMap::new(),
            });
            objects.push(ConnectorCatalogObjectV2 {
                id: format!("mysql:schema:{}", table.catalog),
                kind: ConnectorCatalogObjectKindV2::Schema,
                catalog: Some(table.catalog.clone()),
                schema: Some(table.catalog.clone()),
                name: table.catalog.clone(),
                parent_id: Some(format!("mysql:database:{}", table.catalog)),
                comment: None,
                columns: Vec::new(),
                attributes: BTreeMap::new(),
            });
        }
        objects.push(ConnectorCatalogObjectV2 {
            id: format!("mysql:{:?}:{}:{}", table.kind, table.catalog, table.name)
                .to_ascii_lowercase(),
            kind: table.kind,
            catalog: Some(table.catalog.clone()),
            schema: Some(table.catalog.clone()),
            name: table.name,
            parent_id: Some(format!("mysql:schema:{}", table.catalog)),
            comment: None,
            columns: table.columns,
            attributes: BTreeMap::new(),
        });
    }
    Ok(objects)
}

fn mysql_row(row: &Row) -> Result<Vec<ConnectorValueV2>> {
    row.columns_ref()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let value = row.as_ref(index).ok_or_else(|| {
                DbError::internal("MySQL row did not contain the advertised column")
            })?;
            mysql_value(value, &mysql_type(column))
        })
        .collect()
}

fn mysql_parameter(parameter: &ConnectorParameterV2) -> Result<MySqlValue> {
    match &parameter.value {
        ConnectorValueV2::Null => Ok(MySqlValue::NULL),
        ConnectorValueV2::Boolean(value) => Ok(MySqlValue::Int(i64::from(*value))),
        ConnectorValueV2::SignedInteger(value) => Ok(MySqlValue::Int(*value)),
        ConnectorValueV2::UnsignedInteger(value) => Ok(MySqlValue::UInt(*value)),
        ConnectorValueV2::FloatingPoint(value) => Ok(MySqlValue::Double(*value)),
        ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Text(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::Uuid(value) => Ok(MySqlValue::Bytes(value.as_bytes().to_vec())),
        ConnectorValueV2::Binary(value) => BASE64
            .decode(value)
            .map(MySqlValue::Bytes)
            .map_err(|_| invalid("MySQL binary parameter is not valid base64")),
        ConnectorValueV2::Json(value) => {
            serde_json::to_vec(value)
                .map(MySqlValue::Bytes)
                .map_err(|error| {
                    DbError::internal("failed to encode MySQL JSON parameter")
                        .with_detail(error.to_string())
                })
        }
        ConnectorValueV2::Array(_) => Err(DbError::unsupported("MySQL array parameters")),
    }
}

fn mysql_value(value: &MySqlValue, data_type: &ConnectorTypeV2) -> Result<ConnectorValueV2> {
    match value {
        MySqlValue::NULL => Ok(ConnectorValueV2::Null),
        MySqlValue::Int(value) => {
            if data_type.logical_type == ConnectorLogicalTypeV2::Boolean {
                Ok(ConnectorValueV2::Boolean(*value != 0))
            } else {
                Ok(ConnectorValueV2::SignedInteger(*value))
            }
        }
        MySqlValue::UInt(value) => Ok(ConnectorValueV2::UnsignedInteger(*value)),
        MySqlValue::Float(value) => Ok(ConnectorValueV2::FloatingPoint(f64::from(*value))),
        MySqlValue::Double(value) => Ok(ConnectorValueV2::FloatingPoint(*value)),
        MySqlValue::Bytes(value) => mysql_bytes(value, data_type),
        MySqlValue::Date(year, month, day, hour, minute, second, micros) => {
            let value = format!(
                "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}"
            );
            if data_type.logical_type == ConnectorLogicalTypeV2::Date {
                Ok(ConnectorValueV2::Date(value[..10].to_owned()))
            } else {
                Ok(ConnectorValueV2::Timestamp(value))
            }
        }
        MySqlValue::Time(negative, days, hour, minute, second, micros) => {
            let total_hours = days.saturating_mul(24).saturating_add(u32::from(*hour));
            Ok(ConnectorValueV2::Time(format!(
                "{}{total_hours:02}:{minute:02}:{second:02}.{micros:06}",
                if *negative { "-" } else { "" }
            )))
        }
    }
}

fn mysql_bytes(value: &[u8], data_type: &ConnectorTypeV2) -> Result<ConnectorValueV2> {
    if data_type.logical_type == ConnectorLogicalTypeV2::Binary {
        return Ok(ConnectorValueV2::Binary(BASE64.encode(value)));
    }
    let text = std::str::from_utf8(value)
        .map_err(|_| DbError::new("22021", "MySQL returned non-UTF-8 text"))?
        .to_owned();
    match data_type.logical_type {
        ConnectorLogicalTypeV2::Boolean => Ok(ConnectorValueV2::Boolean(
            text != "0" && !text.eq_ignore_ascii_case("false"),
        )),
        ConnectorLogicalTypeV2::Decimal => Ok(ConnectorValueV2::Decimal(text)),
        ConnectorLogicalTypeV2::Date => Ok(ConnectorValueV2::Date(text)),
        ConnectorLogicalTypeV2::Time => Ok(ConnectorValueV2::Time(text)),
        ConnectorLogicalTypeV2::Timestamp => Ok(ConnectorValueV2::Timestamp(text)),
        ConnectorLogicalTypeV2::Json => serde_json::from_str::<JsonValue>(&text)
            .map(ConnectorValueV2::Json)
            .map_err(|error| {
                DbError::new("22032", "MySQL returned invalid JSON").with_detail(error.to_string())
            }),
        _ => Ok(ConnectorValueV2::Text(text)),
    }
}

fn mysql_column(column: &Column) -> ConnectorColumnV2 {
    ConnectorColumnV2 {
        name: column.name_str().into_owned(),
        data_type: mysql_type(column),
        nullable: !format!("{:?}", column.flags()).contains("NOT_NULL_FLAG"),
    }
}

fn mysql_type(column: &Column) -> ConnectorTypeV2 {
    let vendor_name = format!("{:?}", column.column_type())
        .trim_start_matches("MYSQL_TYPE_")
        .to_owned();
    let logical_type = mysql_logical_type(&vendor_name, column.column_length());
    ConnectorTypeV2 {
        vendor_name,
        logical_type,
        element_type: None,
        precision: None,
        scale: (column.decimals() <= 0x51).then(|| u32::from(column.decimals())),
        length: Some(u64::from(column.column_length())),
    }
}

fn mysql_named_type(data_type: &str, column_type: &str) -> ConnectorTypeV2 {
    ConnectorTypeV2 {
        vendor_name: column_type.to_owned(),
        logical_type: mysql_named_logical_type(data_type),
        element_type: None,
        precision: None,
        scale: None,
        length: None,
    }
}

fn mysql_logical_type(vendor_name: &str, length: u32) -> ConnectorLogicalTypeV2 {
    match vendor_name {
        "NULL" => ConnectorLogicalTypeV2::Null,
        "TINY" if length == 1 => ConnectorLogicalTypeV2::Boolean,
        "TINY" | "SHORT" | "LONG" | "INT24" | "LONGLONG" | "YEAR" => {
            ConnectorLogicalTypeV2::SignedInteger
        }
        "FLOAT" | "DOUBLE" => ConnectorLogicalTypeV2::FloatingPoint,
        "DECIMAL" | "NEWDECIMAL" => ConnectorLogicalTypeV2::Decimal,
        "BIT" if length == 1 => ConnectorLogicalTypeV2::Boolean,
        "BIT" | "TINY_BLOB" | "MEDIUM_BLOB" | "LONG_BLOB" | "BLOB" | "GEOMETRY" => {
            ConnectorLogicalTypeV2::Binary
        }
        "DATE" | "NEWDATE" => ConnectorLogicalTypeV2::Date,
        "TIME" | "TIME2" => ConnectorLogicalTypeV2::Time,
        "TIMESTAMP" | "TIMESTAMP2" | "DATETIME" | "DATETIME2" => ConnectorLogicalTypeV2::Timestamp,
        "JSON" => ConnectorLogicalTypeV2::Json,
        _ => ConnectorLogicalTypeV2::Text,
    }
}

fn mysql_named_logical_type(data_type: &str) -> ConnectorLogicalTypeV2 {
    match data_type.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => ConnectorLogicalTypeV2::Boolean,
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
            ConnectorLogicalTypeV2::SignedInteger
        }
        "float" | "double" | "real" => ConnectorLogicalTypeV2::FloatingPoint,
        "decimal" | "numeric" => ConnectorLogicalTypeV2::Decimal,
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "geometry" => {
            ConnectorLogicalTypeV2::Binary
        }
        "date" => ConnectorLogicalTypeV2::Date,
        "time" => ConnectorLogicalTypeV2::Time,
        "datetime" | "timestamp" => ConnectorLogicalTypeV2::Timestamp,
        "json" => ConnectorLogicalTypeV2::Json,
        _ => ConnectorLogicalTypeV2::Text,
    }
}

fn mysql_row_text(row: &Row, index: usize, name: &str) -> Result<String> {
    match row.as_ref(index) {
        Some(MySqlValue::Bytes(value)) => String::from_utf8(value.clone())
            .map_err(|_| DbError::new("22021", format!("MySQL Catalog field {name} is not UTF-8"))),
        Some(value) => Err(DbError::new(
            "08P01",
            format!("MySQL Catalog field {name} had unexpected value {value:?}"),
        )),
        None => Err(DbError::new(
            "08P01",
            format!("MySQL Catalog field {name} is missing"),
        )),
    }
}

fn mysql_row_optional_text(row: &Row, index: usize) -> Result<Option<String>> {
    match row.as_ref(index) {
        Some(MySqlValue::NULL) => Ok(None),
        Some(MySqlValue::Bytes(value)) => String::from_utf8(value.clone())
            .map(Some)
            .map_err(|_| DbError::new("22021", "MySQL Catalog default is not UTF-8")),
        Some(_) => Err(DbError::new(
            "08P01",
            "MySQL Catalog default had an unexpected value",
        )),
        None => Err(DbError::new("08P01", "MySQL Catalog default is missing")),
    }
}

fn mysql_row_u32(row: &Row, index: usize, name: &str) -> Result<u32> {
    let value = match row.as_ref(index) {
        Some(MySqlValue::Int(value)) => u64::try_from(*value).ok(),
        Some(MySqlValue::UInt(value)) => Some(*value),
        Some(MySqlValue::Bytes(value)) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse().ok()),
        _ => None,
    };
    value
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            DbError::new(
                "08P01",
                format!("MySQL Catalog field {name} is not a valid u32"),
            )
        })
}

fn command_tag(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .unwrap_or("MYSQL")
        .to_ascii_uppercase()
}

fn mysql_error(error: MySqlError) -> DbError {
    match error {
        MySqlError::Server(server) => {
            let sql_state = if server.state.len() == 5 {
                server.state
            } else {
                "HY000".into()
            };
            DbError::new(sql_state, "MySQL connector operation failed")
                .with_detail(format!("{} (vendor code {})", server.message, server.code))
        }
        MySqlError::Io(error) => {
            DbError::new("08006", "MySQL connection failed").with_detail(error.to_string())
        }
        MySqlError::Driver(error) => {
            DbError::new("08006", "MySQL driver operation failed").with_detail(error.to_string())
        }
        error => {
            DbError::new("58000", "MySQL connector operation failed").with_detail(error.to_string())
        }
    }
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<ConnectorQueryEventV2>,
    }

    impl ConnectorEventSink for RecordingSink {
        fn send(
            &mut self,
            event: ConnectorQueryEventV2,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async move {
                self.events.push(event);
                Ok(())
            })
        }
    }

    #[test]
    fn capabilities_and_type_mapping_are_stable() {
        let capabilities = capabilities();
        assert!(capabilities.catalog);
        assert!(capabilities.cancellation);
        assert_eq!(capabilities.maximum_batch_rows, 1024);
        assert_eq!(
            mysql_named_logical_type("json"),
            ConnectorLogicalTypeV2::Json
        );
        assert_eq!(
            mysql_named_logical_type("varbinary"),
            ConnectorLogicalTypeV2::Binary
        );
    }

    #[test]
    fn parameters_preserve_unsigned_and_binary_values() {
        assert_eq!(
            mysql_parameter(&ConnectorParameterV2 {
                data_type: None,
                value: ConnectorValueV2::UnsignedInteger(u64::MAX),
            })
            .expect("unsigned"),
            MySqlValue::UInt(u64::MAX)
        );
        assert_eq!(
            mysql_parameter(&ConnectorParameterV2 {
                data_type: None,
                value: ConnectorValueV2::Binary(BASE64.encode([0_u8, 255])),
            })
            .expect("binary"),
            MySqlValue::Bytes(vec![0, 255])
        );
    }

    #[tokio::test]
    async fn real_mysql_matrix_when_configured() {
        let Some(host) = std::env::var("ORDADB_TEST_MYSQL_HOST").ok() else {
            assert!(
                std::env::var("ORDADB_REQUIRE_REAL_CONNECTOR_TESTS").as_deref() != Ok("1"),
                "ORDADB_TEST_MYSQL_HOST is required for the real connector matrix"
            );
            return;
        };
        let username = std::env::var("ORDADB_TEST_MYSQL_USER")
            .expect("ORDADB_TEST_MYSQL_USER must accompany the host");
        let password = std::env::var("ORDADB_TEST_MYSQL_PASSWORD")
            .expect("ORDADB_TEST_MYSQL_PASSWORD must accompany the host");
        let mut session = MySqlDriver
            .connect(
                ConnectorEndpointV2::Network {
                    host,
                    port: env_port("ORDADB_TEST_MYSQL_PORT", 3306),
                    database: std::env::var("ORDADB_TEST_MYSQL_DATABASE").ok(),
                    instance: None,
                    options: BTreeMap::new(),
                },
                env_tls_mode("ORDADB_TEST_MYSQL_TLS"),
                Some(ConnectorCredentialV2::new(Some(username), password)),
            )
            .await
            .expect("connect MySQL");

        let mut version_sink = RecordingSink::default();
        session
            .execute(
                "mysql-version",
                "SELECT VERSION()",
                &[],
                1,
                &CancellationToken::new(),
                &mut version_sink,
            )
            .await
            .expect("MySQL version");
        assert!(
            first_text(&version_sink.events).starts_with("8.4."),
            "the real connector matrix requires MySQL 8.4 LTS"
        );
        session.catalog().await.expect("MySQL Catalog");
        session
            .begin(Some(ConnectorIsolationLevelV2::ReadCommitted))
            .await
            .expect("begin");
        let mut sink = RecordingSink::default();
        session
            .execute(
                "mysql-types",
                "SELECT TRUE,
                        CAST(42 AS SIGNED),
                        CAST(42 AS UNSIGNED),
                        CAST(1.25 AS DECIMAL(10,2)),
                        CAST('text' AS CHAR),
                        X'00FF',
                        DATE '2026-01-02',
                        TIMESTAMP '2026-01-02 03:04:05',
                        JSON_OBJECT('ok', TRUE)",
                &[],
                64,
                &CancellationToken::new(),
                &mut sink,
            )
            .await
            .expect("typed query");
        session.rollback().await.expect("rollback");
        assert!(
            sink.events
                .iter()
                .any(|event| matches!(event, ConnectorQueryEventV2::Complete { .. }))
        );

        let mut stream_sink = RecordingSink::default();
        session
            .execute(
                "mysql-large",
                "WITH RECURSIVE seq(value) AS (
                    SELECT 1
                    UNION ALL
                    SELECT value + 1 FROM seq WHERE value < 512
                 )
                 SELECT value FROM seq",
                &[],
                64,
                &CancellationToken::new(),
                &mut stream_sink,
            )
            .await
            .expect("large stream");
        assert_eq!(streamed_rows(&stream_sink.events), 512);

        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let mut cancel_sink = RecordingSink::default();
        let error = session
            .execute(
                "mysql-cancel",
                "SELECT SLEEP(30)",
                &[],
                64,
                &cancellation,
                &mut cancel_sink,
            )
            .await
            .expect_err("cancelled MySQL query");
        assert_eq!(error.sql_state, "57014");
    }

    fn env_port(name: &str, default: u16) -> u16 {
        std::env::var(name)
            .ok()
            .map(|value| value.parse().expect("valid connector test port"))
            .unwrap_or(default)
    }

    fn env_tls_mode(name: &str) -> ConnectorTlsModeV2 {
        match std::env::var(name)
            .unwrap_or_else(|_| "verifyFull".into())
            .as_str()
        {
            "disable" => ConnectorTlsModeV2::Disable,
            "require" => ConnectorTlsModeV2::Require,
            "verifyCa" => ConnectorTlsModeV2::VerifyCa,
            "verifyFull" => ConnectorTlsModeV2::VerifyFull,
            value => panic!("unsupported connector test TLS mode {value}"),
        }
    }

    fn streamed_rows(events: &[ConnectorQueryEventV2]) -> usize {
        events
            .iter()
            .filter_map(|event| match event {
                ConnectorQueryEventV2::Batch { batch } => Some(batch.rows.len()),
                _ => None,
            })
            .sum()
    }

    fn first_text(events: &[ConnectorQueryEventV2]) -> &str {
        events
            .iter()
            .find_map(|event| match event {
                ConnectorQueryEventV2::Batch { batch } => batch.rows.first(),
                _ => None,
            })
            .and_then(|row| row.first())
            .and_then(|value| match value {
                ConnectorValueV2::Text(value) => Some(value.as_str()),
                _ => None,
            })
            .expect("text result")
    }
}
