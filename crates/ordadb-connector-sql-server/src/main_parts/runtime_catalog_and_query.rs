use std::{
    collections::BTreeMap,
    os::windows::io::{AsRawSocket, RawSocket},
    str::FromStr,
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use futures_util::TryStreamExt;
use ordadb_connector_sdk::{
    ConnectorBatchV2, ConnectorCapabilitiesV2, ConnectorCatalogColumnV2,
    ConnectorCatalogObjectKindV2, ConnectorCatalogObjectV2, ConnectorColumnV2,
    ConnectorCredentialV2, ConnectorDriver, ConnectorEndpointV2, ConnectorEventSink,
    ConnectorIsolationLevelV2, ConnectorLogicalTypeV2, ConnectorParameterV2, ConnectorQueryEventV2,
    ConnectorSession, ConnectorTlsModeV2, ConnectorTypeV2, ConnectorValueV2,
    connector_pipe_argument, run_named_pipe_helper,
};
use ordadb_types::{DbError, Result};
use rust_decimal::Decimal;
use tiberius::{
    AuthMethod, Client, ColumnType, Config, EncryptionLevel, QueryItem, Row, SqlBrowser, ToSql,
    Uuid, xml::XmlData,
};
use tokio::net::TcpStream;
use tokio_util::{
    compat::{Compat, TokioAsyncWriteCompatExt},
    sync::CancellationToken,
};
use windows_sys::Win32::Networking::WinSock::{SD_BOTH, SOCKET_ERROR, shutdown};
use zeroize::Zeroizing;

const PLUGIN_ID: &str = "sql-server";

type TdsClient = Client<Compat<TcpStream>>;
type TdsParameter = Box<dyn ToSql>;

#[derive(Debug, Default)]
struct SqlServerDriver;

struct SqlServerConnectOptions {
    host: String,
    port: u16,
    database: Option<String>,
    instance: Option<String>,
    username: String,
    secret: Zeroizing<String>,
    tls_mode: ConnectorTlsModeV2,
}

struct SqlServerSession {
    client: Option<TdsClient>,
    session_id: i32,
    raw_socket: RawSocket,
    options: SqlServerConnectOptions,
    capabilities: ConnectorCapabilitiesV2,
}

#[derive(Debug)]
struct TableMetadata {
    catalog: String,
    schema: String,
    name: String,
    kind: ConnectorCatalogObjectKindV2,
    columns: Vec<ConnectorCatalogColumnV2>,
}

enum QueryStep {
    Item(Option<QueryItem>),
    Cancelled,
}

#[tokio::main]
async fn main() {
    let result = async {
        let pipe = connector_pipe_argument()?;
        run_named_pipe_helper(&pipe, PLUGIN_ID, env!("CARGO_PKG_VERSION"), SqlServerDriver).await
    }
    .await;
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[async_trait]
impl ConnectorDriver for SqlServerDriver {
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
            return Err(invalid("SQL Server requires a network endpoint"));
        };
        if !options.is_empty() {
            return Err(DbError::unsupported(
                "SQL Server connector endpoint options",
            ));
        }
        if tls_mode == ConnectorTlsModeV2::VerifyCa {
            return Err(DbError::unsupported(
                "SQL Server TLS verification without hostname verification",
            ));
        }
        let credential = credential
            .ok_or_else(|| DbError::new("28000", "SQL Server credentials are required"))?;
        let username = credential
            .username
            .filter(|username| !username.trim().is_empty())
            .ok_or_else(|| DbError::new("28000", "SQL Server username is required"))?;
        let options = SqlServerConnectOptions {
            host,
            port,
            database,
            instance,
            username,
            secret: credential.secret,
            tls_mode,
        };
        let (client, session_id, raw_socket) = connect_sql_server(&options).await?;
        Ok(Box::new(SqlServerSession {
            client: Some(client),
            session_id,
            raw_socket,
            options,
            capabilities: capabilities(),
        }))
    }
}

#[async_trait]
impl ConnectorSession for SqlServerSession {
    fn capabilities(&self) -> &ConnectorCapabilitiesV2 {
        &self.capabilities
    }

    async fn catalog(&mut self) -> Result<Vec<ConnectorCatalogObjectV2>> {
        let rows = self
            .client_mut()?
            .query(
                "SELECT DB_NAME(),
                        s.name,
                        o.name,
                        o.type,
                        c.name,
                        c.column_id,
                        c.is_nullable,
                        ty.name,
                        c.max_length,
                        c.precision,
                        c.scale,
                        dc.definition
                 FROM sys.objects AS o
                 JOIN sys.schemas AS s ON s.schema_id = o.schema_id
                 JOIN sys.columns AS c ON c.object_id = o.object_id
                 JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
                 LEFT JOIN sys.default_constraints AS dc
                   ON dc.parent_object_id = c.object_id
                  AND dc.parent_column_id = c.column_id
                 WHERE o.type IN ('U', 'V')
                   AND o.is_ms_shipped = 0
                 ORDER BY s.name, o.name, c.column_id",
                &[],
            )
            .await
            .map_err(tds_error)?
            .into_first_result()
            .await
            .map_err(tds_error)?;
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
                "SQL Server connector batch size is outside its capability",
            ));
        }
        let parameters = params
            .iter()
            .map(sql_server_parameter)
            .collect::<Result<Vec<_>>>()?;
        let parameter_refs = parameters
            .iter()
            .map(|parameter| parameter.as_ref() as &dyn ToSql)
            .collect::<Vec<_>>();

        if !returns_rows(sql) {
            let outcome = {
                let client = self.client_mut()?;
                tokio::select! {
                    result = client.execute(sql, &parameter_refs) => Some(result),
                    () = cancellation.cancelled() => None,
                }
            };
            let Some(result) = outcome else {
                self.reset_connection().await?;
                return Err(cancelled());
            };
            let affected_rows = result.map_err(tds_error)?.total();
            sink.send(ConnectorQueryEventV2::Schema {
                columns: Vec::new(),
            })
            .await?;
            sink.send(ConnectorQueryEventV2::Progress {
                rows_processed: affected_rows,
            })
            .await?;
            return sink
                .send(ConnectorQueryEventV2::Complete {
                    command_tag: command_tag(sql),
                    affected_rows: Some(affected_rows),
                })
                .await;
        }

        let raw_socket = self.raw_socket;
        let (query_start, cancelled_start) = {
            let client = self.client_mut()?;
            let query = client.query(sql, &parameter_refs);
            tokio::pin!(query);
            tokio::select! {
                result = &mut query => (result, false),
                () = cancellation.cancelled() => {
                    let _ = shutdown_socket(raw_socket);
                    (query.await, true)
                },
            }
        };
        if cancelled_start {
            drop(query_start);
            self.reset_connection().await?;
            return Err(cancelled());
        }
        let mut stream = query_start.map_err(tds_error)?;
        let columns = stream
            .columns()
            .await
            .map_err(tds_error)?
            .unwrap_or_default()
            .iter()
            .map(|column| ConnectorColumnV2 {
                name: column.name().to_owned(),
                data_type: sql_server_type(column.column_type()),
                nullable: true,
            })
            .collect::<Vec<_>>();
        sink.send(ConnectorQueryEventV2::Schema { columns }).await?;

        let batch_size = usize::try_from(batch_size).unwrap_or(1024);
        let mut batch = Vec::with_capacity(batch_size);
        let mut processed = 0_u64;
        let mut cancelled_query = false;
        loop {
            let step = tokio::select! {
                next = stream.try_next() => QueryStep::Item(next.map_err(tds_error)?),
                () = cancellation.cancelled() => QueryStep::Cancelled,
            };
            let item = match step {
                QueryStep::Item(item) => item,
                QueryStep::Cancelled => {
                    cancelled_query = true;
                    break;
                }
            };
            let Some(item) = item else {
                break;
            };
            match item {
                QueryItem::Metadata(metadata) if metadata.result_index() > 0 => {
                    return Err(DbError::unsupported(
                        "multiple SQL Server result sets in one connector request",
                    ));
                }
                QueryItem::Metadata(_) => {}
                QueryItem::Row(row) => {
                    batch.push(sql_server_row(&row)?);
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
            }
        }
        drop(stream);
        if cancelled_query {
            self.reset_connection().await?;
            return Err(cancelled());
        }
        if !batch.is_empty() {
            sink.send(ConnectorQueryEventV2::Batch {
                batch: ConnectorBatchV2 { rows: batch },
            })
            .await?;
        }
        sink.send(ConnectorQueryEventV2::Progress {
            rows_processed: processed,
        })
        .await?;
        sink.send(ConnectorQueryEventV2::Complete {
            command_tag: command_tag(sql),
            affected_rows: Some(processed),
        })
        .await
    }

    async fn cancel(&mut self, _request_id: &str) -> Result<()> {
        self.reset_connection().await
    }

    async fn begin(&mut self, isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
        if let Some(isolation) = isolation {
            let level = match isolation {
                ConnectorIsolationLevelV2::ReadUncommitted => "READ UNCOMMITTED",
                ConnectorIsolationLevelV2::ReadCommitted => "READ COMMITTED",
                ConnectorIsolationLevelV2::RepeatableRead => "REPEATABLE READ",
                ConnectorIsolationLevelV2::Serializable => "SERIALIZABLE",
            };
            self.execute_control(format!("SET TRANSACTION ISOLATION LEVEL {level}"))
                .await?;
        }
        self.execute_control("BEGIN TRANSACTION").await
    }

    async fn commit(&mut self) -> Result<()> {
        self.execute_control("COMMIT TRANSACTION").await
    }

    async fn rollback(&mut self) -> Result<()> {
        self.execute_control("ROLLBACK TRANSACTION").await
    }
}

impl SqlServerSession {
    fn client_mut(&mut self) -> Result<&mut TdsClient> {
        self.client
            .as_mut()
            .ok_or_else(|| DbError::new("08006", "SQL Server connection is not available"))
    }

    async fn reset_connection(&mut self) -> Result<()> {
        let _ = shutdown_socket(self.raw_socket);
        self.client.take();
        let (client, session_id, raw_socket) = connect_sql_server(&self.options).await?;
        self.client = Some(client);
        self.session_id = session_id;
        self.raw_socket = raw_socket;
        Ok(())
    }

    async fn execute_control(&mut self, sql: impl Into<String>) -> Result<()> {
        self.client_mut()?
            .execute(sql.into(), &[])
            .await
            .map_err(tds_error)?;
        Ok(())
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
            ConnectorTlsModeV2::Prefer,
            ConnectorTlsModeV2::Require,
            ConnectorTlsModeV2::VerifyFull,
        ],
    }
}

async fn connect_sql_server(
    options: &SqlServerConnectOptions,
) -> Result<(TdsClient, i32, RawSocket)> {
    let mut config = Config::new();
    config.host(&options.host);
    config.port(options.port);
    if let Some(database) = &options.database {
        config.database(database);
    }
    if let Some(instance) = &options.instance {
        config.instance_name(instance);
    }
    config.application_name("OrdaDB Connector");
    config.authentication(AuthMethod::sql_server(
        &options.username,
        options.secret.as_str(),
    ));
    match options.tls_mode {
        ConnectorTlsModeV2::Disable => config.encryption(EncryptionLevel::NotSupported),
        ConnectorTlsModeV2::Prefer => config.encryption(EncryptionLevel::On),
        ConnectorTlsModeV2::Require => {
            config.encryption(EncryptionLevel::Required);
            config.trust_cert();
        }
        ConnectorTlsModeV2::VerifyFull => config.encryption(EncryptionLevel::Required),
        ConnectorTlsModeV2::VerifyCa => {
            return Err(DbError::unsupported(
                "SQL Server TLS verification without hostname verification",
            ));
        }
    }
    let tcp = if options.instance.is_some() {
        TcpStream::connect_named(&config).await.map_err(tds_error)?
    } else {
        TcpStream::connect(config.get_addr())
            .await
            .map_err(|error| {
                DbError::new("08001", "failed to connect to SQL Server")
                    .with_detail(error.to_string())
            })?
    };
    tcp.set_nodelay(true).map_err(|error| {
        DbError::new("08006", "failed to configure SQL Server socket")
            .with_detail(error.to_string())
    })?;
    let raw_socket = tcp.as_raw_socket();
    let mut client = Client::connect(config, tcp.compat_write())
        .await
        .map_err(tds_error)?;
    let row = client
        .query("SELECT @@SPID", &[])
        .await
        .map_err(tds_error)?
        .into_row()
        .await
        .map_err(tds_error)?
        .ok_or_else(|| DbError::new("08006", "SQL Server did not return a session ID"))?;
    let session_id = row
        .try_get::<i32, _>(0)
        .map_err(tds_error)?
        .ok_or_else(|| DbError::new("08006", "SQL Server returned a null session ID"))?;
    Ok((client, session_id, raw_socket))
}

fn shutdown_socket(raw_socket: RawSocket) -> Result<()> {
    let socket = usize::try_from(raw_socket)
        .map_err(|_| DbError::new("08006", "SQL Server socket handle is invalid"))?;
    // SAFETY: the socket handle is captured from the live Tokio TcpStream owned
    // by this session. `shutdown` does not take ownership, and reset replaces
    // the handle before another query can use the session.
    let result = unsafe { shutdown(socket, SD_BOTH) };
    if result == SOCKET_ERROR {
        return Err(DbError::new("08006", "failed to cancel SQL Server socket")
            .with_detail(std::io::Error::last_os_error().to_string()));
    }
    Ok(())
}

fn catalog_from_rows(rows: Vec<Row>) -> Result<Vec<ConnectorCatalogObjectV2>> {
    let mut tables = BTreeMap::<(String, String), TableMetadata>::new();
    for row in rows {
        let catalog = required_text(&row, 0, "database")?;
        let schema = required_text(&row, 1, "schema")?;
        let table = required_text(&row, 2, "object")?;
        let object_type = required_text(&row, 3, "object type")?;
        let column = required_text(&row, 4, "column")?;
        let ordinal = row
            .try_get::<i32, _>(5)
            .map_err(tds_error)?
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| DbError::new("08P01", "SQL Server Catalog ordinal is invalid"))?;
        let nullable = row
            .try_get::<bool, _>(6)
            .map_err(tds_error)?
            .ok_or_else(|| DbError::new("08P01", "SQL Server Catalog nullable flag is null"))?;
        let data_type = required_text(&row, 7, "data type")?;
        let max_length = row
            .try_get::<i16, _>(8)
            .map_err(tds_error)?
            .ok_or_else(|| DbError::new("08P01", "SQL Server Catalog length is null"))?;
        let precision = row
            .try_get::<u8, _>(9)
            .map_err(tds_error)?
            .ok_or_else(|| DbError::new("08P01", "SQL Server Catalog precision is null"))?;
        let scale = row
            .try_get::<u8, _>(10)
            .map_err(tds_error)?
            .ok_or_else(|| DbError::new("08P01", "SQL Server Catalog scale is null"))?;
        let default_expression = row
            .try_get::<&str, _>(11)
            .map_err(tds_error)?
            .map(str::to_owned);
        let metadata = tables
            .entry((schema.clone(), table.clone()))
            .or_insert_with(|| TableMetadata {
                catalog: catalog.clone(),
                schema: schema.clone(),
                name: table.clone(),
                kind: if object_type.trim() == "V" {
                    ConnectorCatalogObjectKindV2::View
                } else {
                    ConnectorCatalogObjectKindV2::Table
                },
                columns: Vec::new(),
            });
        metadata.columns.push(ConnectorCatalogColumnV2 {
            name: column,
            ordinal,
            data_type: sql_server_named_type(&data_type, max_length, precision, scale),
            nullable,
            default_expression,
        });
    }

    let mut objects = Vec::new();
    let mut catalogs = BTreeMap::<String, ()>::new();
    let mut schemas = BTreeMap::<(String, String), ()>::new();
    for table in tables.into_values() {
        if catalogs.insert(table.catalog.clone(), ()).is_none() {
            objects.push(ConnectorCatalogObjectV2 {
                id: format!("sql-server:database:{}", table.catalog),
                kind: ConnectorCatalogObjectKindV2::Database,
                catalog: Some(table.catalog.clone()),
                schema: None,
                name: table.catalog.clone(),
                parent_id: None,
                comment: None,
                columns: Vec::new(),
                attributes: BTreeMap::new(),
            });
        }
        if schemas
            .insert((table.catalog.clone(), table.schema.clone()), ())
            .is_none()
        {
            objects.push(ConnectorCatalogObjectV2 {
                id: format!("sql-server:schema:{}:{}", table.catalog, table.schema),
                kind: ConnectorCatalogObjectKindV2::Schema,
                catalog: Some(table.catalog.clone()),
                schema: Some(table.schema.clone()),
                name: table.schema.clone(),
                parent_id: Some(format!("sql-server:database:{}", table.catalog)),
                comment: None,
                columns: Vec::new(),
                attributes: BTreeMap::new(),
            });
        }
        objects.push(ConnectorCatalogObjectV2 {
            id: format!(
                "sql-server:{:?}:{}:{}:{}",
                table.kind, table.catalog, table.schema, table.name
            )
            .to_ascii_lowercase(),
            kind: table.kind,
            catalog: Some(table.catalog.clone()),
            schema: Some(table.schema.clone()),
            name: table.name,
            parent_id: Some(format!(
                "sql-server:schema:{}:{}",
                table.catalog, table.schema
            )),
            comment: None,
            columns: table.columns,
            attributes: BTreeMap::new(),
        });
    }
    Ok(objects)
}

fn sql_server_row(row: &Row) -> Result<Vec<ConnectorValueV2>> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(index, column)| sql_server_value(row, index, column.column_type()))
        .collect()
}

fn sql_server_value(row: &Row, index: usize, column_type: ColumnType) -> Result<ConnectorValueV2> {
    macro_rules! optional {
        ($type:ty, $variant:path) => {
            row.try_get::<$type, _>(index)
                .map_err(tds_error)?
                .map_or(Ok(ConnectorValueV2::Null), |value| {
                    Ok($variant(value.into()))
                })
        };
    }
    match column_type {
        ColumnType::Null => Ok(ConnectorValueV2::Null),
        ColumnType::Bit | ColumnType::Bitn => optional!(bool, ConnectorValueV2::Boolean),
        ColumnType::Int1 => optional!(u8, ConnectorValueV2::UnsignedInteger),
        ColumnType::Int2 => optional!(i16, ConnectorValueV2::SignedInteger),
        ColumnType::Int4 => optional!(i32, ConnectorValueV2::SignedInteger),
        ColumnType::Int8 => optional!(i64, ConnectorValueV2::SignedInteger),
        ColumnType::Float4 => optional!(f32, ConnectorValueV2::FloatingPoint),
        ColumnType::Float8 => optional!(f64, ConnectorValueV2::FloatingPoint),
        ColumnType::Money | ColumnType::Money4 => row
            .try_get::<f64, _>(index)
            .map_err(tds_error)?
            .map_or(Ok(ConnectorValueV2::Null), |value| {
                Ok(ConnectorValueV2::Decimal(format!("{value:.4}")))
            }),
        ColumnType::Decimaln | ColumnType::Numericn => row
            .try_get::<Decimal, _>(index)
            .map_err(tds_error)?
            .map_or(Ok(ConnectorValueV2::Null), |value| {
                Ok(ConnectorValueV2::Decimal(value.to_string()))
            }),
        ColumnType::Guid => row
            .try_get::<Uuid, _>(index)
            .map_err(tds_error)?
            .map_or(Ok(ConnectorValueV2::Null), |value| {
                Ok(ConnectorValueV2::Uuid(value.to_string()))
            }),
        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => row
            .try_get::<&[u8], _>(index)
            .map_err(tds_error)?
            .map_or(Ok(ConnectorValueV2::Null), |value| {
                Ok(ConnectorValueV2::Binary(BASE64.encode(value)))
            }),
        ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::Text
        | ColumnType::NText => row
            .try_get::<&str, _>(index)
            .map_err(tds_error)?
            .map_or(Ok(ConnectorValueV2::Null), |value| {
                Ok(ConnectorValueV2::Text(value.to_owned()))
            }),
        ColumnType::Xml => row
            .try_get::<&XmlData, _>(index)
            .map_err(tds_error)?
            .map_or(Ok(ConnectorValueV2::Null), |value| {
                Ok(ConnectorValueV2::Text(value.to_string()))
            }),
        ColumnType::Daten => row
            .try_get::<NaiveDate, _>(index)
            .map_err(tds_error)?
            .map_or(Ok(ConnectorValueV2::Null), |value| {
                Ok(ConnectorValueV2::Date(value.to_string()))
            }),
        ColumnType::Timen => row
            .try_get::<NaiveTime, _>(index)
            .map_err(tds_error)?
            .map_or(Ok(ConnectorValueV2::Null), |value| {
                Ok(ConnectorValueV2::Time(value.to_string()))
            }),
        ColumnType::Datetime4
        | ColumnType::Datetime
        | ColumnType::Datetimen
        | ColumnType::Datetime2 => row
            .try_get::<NaiveDateTime, _>(index)
            .map_err(tds_error)?
            .map_or(Ok(ConnectorValueV2::Null), |value| {
                Ok(ConnectorValueV2::Timestamp(value.to_string()))
            }),
        ColumnType::DatetimeOffsetn => row
            .try_get::<DateTime<FixedOffset>, _>(index)
            .map_err(tds_error)?
            .map_or(Ok(ConnectorValueV2::Null), |value| {
                Ok(ConnectorValueV2::TimestampWithTimeZone(value.to_rfc3339()))
            }),
        ColumnType::Intn | ColumnType::Floatn | ColumnType::Udt | ColumnType::SSVariant => Err(
            DbError::unsupported(format!("SQL Server result type {column_type:?}")),
        ),
    }
}

fn sql_server_parameter(parameter: &ConnectorParameterV2) -> Result<TdsParameter> {
    match &parameter.value {
        ConnectorValueV2::Null => Ok(Box::new(Option::<String>::None)),
        ConnectorValueV2::Boolean(value) => Ok(Box::new(*value)),
        ConnectorValueV2::SignedInteger(value) => Ok(Box::new(*value)),
        ConnectorValueV2::UnsignedInteger(value) => i64::try_from(*value)
            .map(|value| Box::new(value) as TdsParameter)
            .map_err(|_| invalid("SQL Server unsigned parameter exceeds bigint")),
        ConnectorValueV2::FloatingPoint(value) => Ok(Box::new(*value)),
        ConnectorValueV2::Decimal(value) => Decimal::from_str(value)
            .map(|value| Box::new(value) as TdsParameter)
            .map_err(|_| invalid("SQL Server decimal parameter is invalid")),
        ConnectorValueV2::Text(value) | ConnectorValueV2::Interval(value) => {
            Ok(Box::new(value.clone()))
        }
        ConnectorValueV2::Binary(value) => BASE64
            .decode(value)
            .map(|value| Box::new(value) as TdsParameter)
            .map_err(|_| invalid("SQL Server binary parameter is not valid base64")),
        ConnectorValueV2::Date(value) => NaiveDate::from_str(value)
            .map(|value| Box::new(value) as TdsParameter)
            .map_err(|_| invalid("SQL Server date parameter is invalid")),
        ConnectorValueV2::Time(value) => NaiveTime::from_str(value)
            .map(|value| Box::new(value) as TdsParameter)
            .map_err(|_| invalid("SQL Server time parameter is invalid")),
        ConnectorValueV2::Timestamp(value) => {
            parse_naive_timestamp(value).map(|value| Box::new(value) as TdsParameter)
        }
        ConnectorValueV2::TimestampWithTimeZone(value) => DateTime::parse_from_rfc3339(value)
            .map(|value| Box::new(value) as TdsParameter)
            .map_err(|_| invalid("SQL Server timestamp with time zone parameter is invalid")),
        ConnectorValueV2::Uuid(value) => Uuid::parse_str(value)
            .map(|value| Box::new(value) as TdsParameter)
            .map_err(|_| invalid("SQL Server UUID parameter is invalid")),
        ConnectorValueV2::Json(value) => serde_json::to_string(value)
            .map(|value| Box::new(value) as TdsParameter)
            .map_err(|error| {
                DbError::internal("failed to encode SQL Server JSON parameter")
                    .with_detail(error.to_string())
            }),
        ConnectorValueV2::Array(_) => Err(DbError::unsupported("SQL Server array parameters")),
    }
}

fn parse_naive_timestamp(value: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .map_err(|_| invalid("SQL Server timestamp parameter is invalid"))
}
