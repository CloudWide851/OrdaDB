use std::{collections::BTreeMap, str::FromStr};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures_util::TryStreamExt as _;
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
use tokio_postgres::{
    Client, Config, NoTls, Row,
    config::SslMode,
    error::ErrorPosition,
    types::{ToSql, Type},
};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const PLUGIN_ID: &str = "postgresql";

#[derive(Debug, Default)]
struct PostgresDriver;

struct PostgresSession {
    client: Client,
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

type PgParameter = Box<dyn ToSql + Sync + Send>;

#[tokio::main]
async fn main() {
    let result = async {
        let pipe = connector_pipe_argument()?;
        run_named_pipe_helper(&pipe, PLUGIN_ID, env!("CARGO_PKG_VERSION"), PostgresDriver).await
    }
    .await;
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[async_trait]
impl ConnectorDriver for PostgresDriver {
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
            return Err(invalid("PostgreSQL requires a network endpoint"));
        };
        if instance.is_some() {
            return Err(invalid("PostgreSQL does not support named instances"));
        }
        let credential = credential
            .ok_or_else(|| DbError::new("28P01", "PostgreSQL credentials are required"))?;
        let username = credential
            .username
            .as_deref()
            .ok_or_else(|| DbError::new("28P01", "PostgreSQL username is required"))?;
        let mut config = Config::new();
        config
            .host(&host)
            .port(port)
            .user(username)
            .password(credential.secret.as_str());
        if let Some(database) = database {
            config.dbname(&database);
        }
        for (name, value) in options {
            match name.as_str() {
                "applicationName" => {
                    config.application_name(&value);
                }
                _ => return Err(invalid(format!("unsupported PostgreSQL option {name}"))),
            }
        }

        let client = match tls_mode {
            ConnectorTlsModeV2::Disable => {
                config.ssl_mode(SslMode::Disable);
                let (client, connection) = config.connect(NoTls).await.map_err(postgres_error)?;
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                client
            }
            ConnectorTlsModeV2::Prefer
            | ConnectorTlsModeV2::Require
            | ConnectorTlsModeV2::VerifyFull => {
                let _ = rustls::crypto::ring::default_provider().install_default();
                config.ssl_mode(if tls_mode == ConnectorTlsModeV2::Prefer {
                    SslMode::Prefer
                } else {
                    SslMode::Require
                });
                let (tls, _) = MakeRustlsConnect::with_native_certs().map_err(|errors| {
                    DbError::new(
                        "08001",
                        "Windows TLS trust store has no usable certificates",
                    )
                    .with_detail(
                        errors
                            .into_iter()
                            .map(|error| error.to_string())
                            .collect::<Vec<_>>()
                            .join("; "),
                    )
                })?;
                let (client, connection) = config.connect(tls).await.map_err(postgres_error)?;
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                client
            }
            ConnectorTlsModeV2::VerifyCa => {
                return Err(DbError::unsupported(
                    "PostgreSQL TLS verification without hostname verification",
                ));
            }
        };
        Ok(Box::new(PostgresSession {
            client,
            capabilities: capabilities(),
        }))
    }
}

#[async_trait]
impl ConnectorSession for PostgresSession {
    fn capabilities(&self) -> &ConnectorCapabilitiesV2 {
        &self.capabilities
    }

    async fn catalog(&mut self) -> Result<Vec<ConnectorCatalogObjectV2>> {
        let rows = self
            .client
            .query(
                "SELECT current_database(),
                        c.table_schema,
                        c.table_name,
                        t.table_type,
                        c.column_name,
                        c.ordinal_position,
                        c.is_nullable,
                        c.data_type,
                        c.udt_name,
                        c.column_default
                 FROM information_schema.columns AS c
                 JOIN information_schema.tables AS t
                   ON t.table_schema = c.table_schema
                  AND t.table_name = c.table_name
                 WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
                 ORDER BY c.table_schema, c.table_name, c.ordinal_position",
                &[],
            )
            .await
            .map_err(postgres_error)?;
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
                "PostgreSQL connector batch size is outside its capability",
            ));
        }
        let statement = self.client.prepare(sql).await.map_err(postgres_error)?;
        let parameters = params
            .iter()
            .map(postgres_parameter)
            .collect::<Result<Vec<_>>>()?;
        let parameter_refs = parameters
            .iter()
            .map(|parameter| parameter.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let columns = statement
            .columns()
            .iter()
            .map(|column| ConnectorColumnV2 {
                name: column.name().into(),
                data_type: postgres_type(column.type_()),
                nullable: true,
            })
            .collect::<Vec<_>>();
        sink.send(ConnectorQueryEventV2::Schema { columns }).await?;

        if statement.columns().is_empty() {
            let affected = tokio::select! {
                result = self.client.execute(&statement, &parameter_refs) => {
                    result.map_err(postgres_error)?
                }
                () = cancellation.cancelled() => {
                    cancel_postgres(&self.client).await;
                    return Err(DbError::new("57014", "PostgreSQL query was cancelled"));
                }
            };
            sink.send(ConnectorQueryEventV2::Complete {
                command_tag: command_tag(sql),
                affected_rows: Some(affected),
            })
            .await?;
            return Ok(());
        }

        let stream = self
            .client
            .query_raw(&statement, parameter_refs)
            .await
            .map_err(postgres_error)?;
        tokio::pin!(stream);
        let batch_size = usize::try_from(batch_size).unwrap_or(1024);
        let mut batch = Vec::with_capacity(batch_size);
        let mut processed = 0_u64;
        loop {
            let row = tokio::select! {
                result = stream.try_next() => result.map_err(postgres_error)?,
                () = cancellation.cancelled() => {
                    cancel_postgres(&self.client).await;
                    return Err(DbError::new("57014", "PostgreSQL query was cancelled"));
                }
            };
            let Some(row) = row else {
                break;
            };
            batch.push(postgres_row(&row)?);
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
        cancel_postgres(&self.client).await;
        Ok(())
    }

    async fn begin(&mut self, isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
        let sql = match isolation {
            None => "BEGIN",
            Some(ConnectorIsolationLevelV2::ReadUncommitted) => {
                "BEGIN ISOLATION LEVEL READ UNCOMMITTED"
            }
            Some(ConnectorIsolationLevelV2::ReadCommitted) => {
                "BEGIN ISOLATION LEVEL READ COMMITTED"
            }
            Some(ConnectorIsolationLevelV2::RepeatableRead) => {
                "BEGIN ISOLATION LEVEL REPEATABLE READ"
            }
            Some(ConnectorIsolationLevelV2::Serializable) => "BEGIN ISOLATION LEVEL SERIALIZABLE",
        };
        self.client.batch_execute(sql).await.map_err(postgres_error)
    }

    async fn commit(&mut self) -> Result<()> {
        self.client
            .batch_execute("COMMIT")
            .await
            .map_err(postgres_error)
    }

    async fn rollback(&mut self) -> Result<()> {
        self.client
            .batch_execute("ROLLBACK")
            .await
            .map_err(postgres_error)
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

async fn cancel_postgres(client: &Client) {
    let _ = client.cancel_token().cancel_query(NoTls).await;
}

fn catalog_from_rows(rows: Vec<Row>) -> Result<Vec<ConnectorCatalogObjectV2>> {
    let mut tables = BTreeMap::<(String, String), TableMetadata>::new();
    for row in rows {
        let catalog = row.try_get::<_, String>(0).map_err(postgres_error)?;
        let schema = row.try_get::<_, String>(1).map_err(postgres_error)?;
        let table = row.try_get::<_, String>(2).map_err(postgres_error)?;
        let table_type = row.try_get::<_, String>(3).map_err(postgres_error)?;
        let column = row.try_get::<_, String>(4).map_err(postgres_error)?;
        let ordinal = row.try_get::<_, i32>(5).map_err(postgres_error)?;
        let nullable = row.try_get::<_, String>(6).map_err(postgres_error)?;
        let data_type = row.try_get::<_, String>(7).map_err(postgres_error)?;
        let udt_name = row.try_get::<_, String>(8).map_err(postgres_error)?;
        let default_expression = row
            .try_get::<_, Option<String>>(9)
            .map_err(postgres_error)?;
        let metadata = tables
            .entry((schema.clone(), table.clone()))
            .or_insert_with(|| TableMetadata {
                catalog: catalog.clone(),
                schema: schema.clone(),
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
            ordinal: u32::try_from(ordinal).unwrap_or(u32::MAX),
            data_type: postgres_named_type(&data_type, &udt_name),
            nullable: nullable == "YES",
            default_expression,
        });
    }
    let mut objects = Vec::new();
    let mut catalogs = BTreeMap::<String, ()>::new();
    let mut schemas = BTreeMap::<(String, String), ()>::new();
    for table in tables.into_values() {
        if catalogs.insert(table.catalog.clone(), ()).is_none() {
            objects.push(ConnectorCatalogObjectV2 {
                id: format!("postgresql:database:{}", table.catalog),
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
                id: format!("postgresql:schema:{}:{}", table.catalog, table.schema),
                kind: ConnectorCatalogObjectKindV2::Schema,
                catalog: Some(table.catalog.clone()),
                schema: Some(table.schema.clone()),
                name: table.schema.clone(),
                parent_id: Some(format!("postgresql:database:{}", table.catalog)),
                comment: None,
                columns: Vec::new(),
                attributes: BTreeMap::new(),
            });
        }
        objects.push(ConnectorCatalogObjectV2 {
            id: format!(
                "postgresql:{:?}:{}:{}",
                table.kind, table.schema, table.name
            )
            .to_ascii_lowercase(),
            kind: table.kind,
            catalog: Some(table.catalog.clone()),
            schema: Some(table.schema.clone()),
            name: table.name,
            parent_id: Some(format!(
                "postgresql:schema:{}:{}",
                table.catalog, table.schema
            )),
            comment: None,
            columns: table.columns,
            attributes: BTreeMap::new(),
        });
    }
    Ok(objects)
}

fn postgres_row(row: &Row) -> Result<Vec<ConnectorValueV2>> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(index, column)| postgres_value(row, index, column.type_()))
        .collect()
}

fn postgres_value(row: &Row, index: usize, data_type: &Type) -> Result<ConnectorValueV2> {
    let value = match *data_type {
        Type::BOOL => pg_optional(row, index, ConnectorValueV2::Boolean)?,
        Type::INT2 => pg_optional(row, index, |value: i16| {
            ConnectorValueV2::SignedInteger(i64::from(value))
        })?,
        Type::INT4 => pg_optional(row, index, |value: i32| {
            ConnectorValueV2::SignedInteger(i64::from(value))
        })?,
        Type::INT8 => pg_optional(row, index, ConnectorValueV2::SignedInteger)?,
        Type::FLOAT4 => pg_optional(row, index, |value: f32| {
            ConnectorValueV2::FloatingPoint(f64::from(value))
        })?,
        Type::FLOAT8 => pg_optional(row, index, ConnectorValueV2::FloatingPoint)?,
        Type::NUMERIC => pg_optional(row, index, |value: Decimal| {
            ConnectorValueV2::Decimal(value.to_string())
        })?,
        Type::BYTEA => pg_optional(row, index, |value: Vec<u8>| {
            ConnectorValueV2::Binary(BASE64.encode(value))
        })?,
        Type::DATE => pg_optional(row, index, |value: NaiveDate| {
            ConnectorValueV2::Date(value.to_string())
        })?,
        Type::TIME => pg_optional(row, index, |value: NaiveTime| {
            ConnectorValueV2::Time(value.to_string())
        })?,
        Type::TIMESTAMP => pg_optional(row, index, |value: NaiveDateTime| {
            ConnectorValueV2::Timestamp(value.to_string())
        })?,
        Type::TIMESTAMPTZ => pg_optional(row, index, |value: DateTime<Utc>| {
            ConnectorValueV2::TimestampWithTimeZone(value.to_rfc3339())
        })?,
        Type::UUID => pg_optional(row, index, |value: Uuid| {
            ConnectorValueV2::Uuid(value.to_string())
        })?,
        Type::JSON | Type::JSONB => pg_optional(row, index, ConnectorValueV2::Json)?,
        _ => pg_optional(row, index, ConnectorValueV2::Text)?,
    };
    Ok(value)
}

fn pg_optional<T>(
    row: &Row,
    index: usize,
    map: impl FnOnce(T) -> ConnectorValueV2,
) -> Result<ConnectorValueV2>
where
    T: tokio_postgres::types::FromSqlOwned,
{
    match row.try_get::<_, Option<T>>(index).map_err(postgres_error)? {
        Some(value) => Ok(map(value)),
        None => Ok(ConnectorValueV2::Null),
    }
}

fn postgres_parameter(parameter: &ConnectorParameterV2) -> Result<PgParameter> {
    match &parameter.value {
        ConnectorValueV2::Null => null_parameter(parameter.data_type.as_ref()),
        ConnectorValueV2::Boolean(value) => Ok(Box::new(*value)),
        ConnectorValueV2::SignedInteger(value) => Ok(Box::new(*value)),
        ConnectorValueV2::UnsignedInteger(value) => {
            Ok(Box::new(i64::try_from(*value).map_err(|_| {
                DbError::new("22003", "PostgreSQL parameter exceeds int64")
            })?))
        }
        ConnectorValueV2::FloatingPoint(value) => Ok(Box::new(*value)),
        ConnectorValueV2::Decimal(value) => Decimal::from_str(value)
            .map(|value| Box::new(value) as PgParameter)
            .map_err(|error| {
                invalid("PostgreSQL decimal parameter is invalid").with_detail(error.to_string())
            }),
        ConnectorValueV2::Text(value) | ConnectorValueV2::Interval(value) => {
            Ok(Box::new(value.clone()))
        }
        ConnectorValueV2::Binary(value) => BASE64
            .decode(value)
            .map(|value| Box::new(value) as PgParameter)
            .map_err(|_| invalid("PostgreSQL binary parameter is not valid base64")),
        ConnectorValueV2::Date(value) => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(|value| Box::new(value) as PgParameter)
            .map_err(|error| {
                invalid("PostgreSQL date parameter is invalid").with_detail(error.to_string())
            }),
        ConnectorValueV2::Time(value) => NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
            .map(|value| Box::new(value) as PgParameter)
            .map_err(|error| {
                invalid("PostgreSQL time parameter is invalid").with_detail(error.to_string())
            }),
        ConnectorValueV2::Timestamp(value) => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
                .map(|value| Box::new(value) as PgParameter)
                .map_err(|error| {
                    invalid("PostgreSQL timestamp parameter is invalid")
                        .with_detail(error.to_string())
                })
        }
        ConnectorValueV2::TimestampWithTimeZone(value) => DateTime::parse_from_rfc3339(value)
            .map(|value| Box::new(value.with_timezone(&Utc)) as PgParameter)
            .map_err(|error| {
                invalid("PostgreSQL timestamptz parameter is invalid")
                    .with_detail(error.to_string())
            }),
        ConnectorValueV2::Uuid(value) => Uuid::parse_str(value)
            .map(|value| Box::new(value) as PgParameter)
            .map_err(|error| {
                invalid("PostgreSQL UUID parameter is invalid").with_detail(error.to_string())
            }),
        ConnectorValueV2::Json(value) => Ok(Box::new(value.clone())),
        ConnectorValueV2::Array(_) => Err(DbError::unsupported(
            "untyped array parameters for PostgreSQL connectors",
        )),
    }
}

fn null_parameter(data_type: Option<&ConnectorTypeV2>) -> Result<PgParameter> {
    let parameter: PgParameter = match data_type.map(|value| value.logical_type) {
        Some(ConnectorLogicalTypeV2::Boolean) => Box::new(None::<bool>),
        Some(ConnectorLogicalTypeV2::SignedInteger) => Box::new(None::<i64>),
        Some(ConnectorLogicalTypeV2::FloatingPoint) => Box::new(None::<f64>),
        Some(ConnectorLogicalTypeV2::Decimal) => Box::new(None::<Decimal>),
        Some(ConnectorLogicalTypeV2::Binary) => Box::new(None::<Vec<u8>>),
        Some(ConnectorLogicalTypeV2::Date) => Box::new(None::<NaiveDate>),
        Some(ConnectorLogicalTypeV2::Time) => Box::new(None::<NaiveTime>),
        Some(ConnectorLogicalTypeV2::Timestamp) => Box::new(None::<NaiveDateTime>),
        Some(ConnectorLogicalTypeV2::TimestampWithTimeZone) => Box::new(None::<DateTime<Utc>>),
        Some(ConnectorLogicalTypeV2::Uuid) => Box::new(None::<Uuid>),
        Some(ConnectorLogicalTypeV2::Json) => Box::new(None::<serde_json::Value>),
        Some(ConnectorLogicalTypeV2::Array) => {
            return Err(DbError::unsupported(
                "untyped NULL array parameters for PostgreSQL connectors",
            ));
        }
        Some(
            ConnectorLogicalTypeV2::Null
            | ConnectorLogicalTypeV2::UnsignedInteger
            | ConnectorLogicalTypeV2::Text
            | ConnectorLogicalTypeV2::Interval
            | ConnectorLogicalTypeV2::Other,
        )
        | None => Box::new(None::<String>),
    };
    Ok(parameter)
}

fn postgres_type(data_type: &Type) -> ConnectorTypeV2 {
    let logical_type = match *data_type {
        Type::BOOL => ConnectorLogicalTypeV2::Boolean,
        Type::INT2 | Type::INT4 | Type::INT8 => ConnectorLogicalTypeV2::SignedInteger,
        Type::FLOAT4 | Type::FLOAT8 => ConnectorLogicalTypeV2::FloatingPoint,
        Type::NUMERIC => ConnectorLogicalTypeV2::Decimal,
        Type::BYTEA => ConnectorLogicalTypeV2::Binary,
        Type::DATE => ConnectorLogicalTypeV2::Date,
        Type::TIME => ConnectorLogicalTypeV2::Time,
        Type::TIMESTAMP => ConnectorLogicalTypeV2::Timestamp,
        Type::TIMESTAMPTZ => ConnectorLogicalTypeV2::TimestampWithTimeZone,
        Type::UUID => ConnectorLogicalTypeV2::Uuid,
        Type::JSON | Type::JSONB => ConnectorLogicalTypeV2::Json,
        _ if matches!(data_type.kind(), tokio_postgres::types::Kind::Array(_)) => {
            ConnectorLogicalTypeV2::Array
        }
        _ => ConnectorLogicalTypeV2::Text,
    };
    ConnectorTypeV2 {
        vendor_name: data_type.name().into(),
        logical_type,
        element_type: match data_type.kind() {
            tokio_postgres::types::Kind::Array(element) => Some(Box::new(postgres_type(element))),
            _ => None,
        },
        precision: None,
        scale: None,
        length: None,
    }
}

fn postgres_named_type(data_type: &str, udt_name: &str) -> ConnectorTypeV2 {
    let logical_type = match data_type {
        "boolean" => ConnectorLogicalTypeV2::Boolean,
        "smallint" | "integer" | "bigint" => ConnectorLogicalTypeV2::SignedInteger,
        "real" | "double precision" => ConnectorLogicalTypeV2::FloatingPoint,
        "numeric" | "decimal" => ConnectorLogicalTypeV2::Decimal,
        "bytea" => ConnectorLogicalTypeV2::Binary,
        "date" => ConnectorLogicalTypeV2::Date,
        "time without time zone" | "time with time zone" => ConnectorLogicalTypeV2::Time,
        "timestamp without time zone" => ConnectorLogicalTypeV2::Timestamp,
        "timestamp with time zone" => ConnectorLogicalTypeV2::TimestampWithTimeZone,
        "uuid" => ConnectorLogicalTypeV2::Uuid,
        "json" | "jsonb" => ConnectorLogicalTypeV2::Json,
        "ARRAY" => ConnectorLogicalTypeV2::Array,
        _ => ConnectorLogicalTypeV2::Text,
    };
    ConnectorTypeV2 {
        vendor_name: udt_name.into(),
        logical_type,
        element_type: None,
        precision: None,
        scale: None,
        length: None,
    }
}

fn postgres_error(error: tokio_postgres::Error) -> DbError {
    let Some(database) = error.as_db_error() else {
        return DbError::new("08006", "PostgreSQL connector operation failed")
            .with_detail(error.to_string());
    };
    let mut converted = DbError::new(database.code().code(), database.message());
    if let Some(detail) = database.detail() {
        converted = converted.with_detail(detail);
    }
    if let Some(hint) = database.hint() {
        converted = converted.with_hint(hint);
    }
    if let Some(ErrorPosition::Original(position)) = database.position()
        && let Ok(position) = usize::try_from(*position)
    {
        converted = converted.with_position(position);
    }
    converted
}

fn command_tag(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .unwrap_or("POSTGRESQL")
        .to_ascii_uppercase()
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
    fn postgres_type_mapping_covers_core_wire_types() {
        assert_eq!(
            postgres_type(&Type::INT8).logical_type,
            ConnectorLogicalTypeV2::SignedInteger
        );
        assert_eq!(
            postgres_type(&Type::TIMESTAMPTZ).logical_type,
            ConnectorLogicalTypeV2::TimestampWithTimeZone
        );
        assert_eq!(
            postgres_type(&Type::JSONB).logical_type,
            ConnectorLogicalTypeV2::Json
        );
    }

    #[test]
    fn postgres_catalog_projection_groups_columns() {
        let data_type = postgres_named_type("integer", "int4");
        assert_eq!(
            data_type.logical_type,
            ConnectorLogicalTypeV2::SignedInteger
        );
        assert_eq!(data_type.vendor_name, "int4");
    }

    #[tokio::test]
    async fn real_postgresql_matrix_when_configured() {
        let Some(host) = std::env::var("ORDADB_TEST_POSTGRESQL_HOST").ok() else {
            assert!(
                std::env::var("ORDADB_REQUIRE_REAL_CONNECTOR_TESTS").as_deref() != Ok("1"),
                "ORDADB_TEST_POSTGRESQL_HOST is required for the real connector matrix"
            );
            return;
        };
        let port = env_port("ORDADB_TEST_POSTGRESQL_PORT", 5432);
        let database = std::env::var("ORDADB_TEST_POSTGRESQL_DATABASE").ok();
        let username = std::env::var("ORDADB_TEST_POSTGRESQL_USER")
            .expect("ORDADB_TEST_POSTGRESQL_USER must accompany the host");
        let password = std::env::var("ORDADB_TEST_POSTGRESQL_PASSWORD")
            .expect("ORDADB_TEST_POSTGRESQL_PASSWORD must accompany the host");
        let tls_mode = env_tls_mode("ORDADB_TEST_POSTGRESQL_TLS");
        let mut session = PostgresDriver
            .connect(
                ConnectorEndpointV2::Network {
                    host,
                    port,
                    database,
                    instance: None,
                    options: BTreeMap::from([(
                        "applicationName".into(),
                        "ordadb-real-connector-test".into(),
                    )]),
                },
                tls_mode,
                Some(ConnectorCredentialV2::new(Some(username), password)),
            )
            .await
            .expect("connect PostgreSQL");

        let mut version_sink = RecordingSink::default();
        session
            .execute(
                "postgresql-version",
                "SELECT current_setting('server_version')",
                &[],
                1,
                &CancellationToken::new(),
                &mut version_sink,
            )
            .await
            .expect("PostgreSQL version");
        assert!(
            first_text(&version_sink.events).starts_with("18."),
            "the real connector matrix requires PostgreSQL 18"
        );
        session.catalog().await.expect("PostgreSQL Catalog");
        session
            .begin(Some(ConnectorIsolationLevelV2::ReadCommitted))
            .await
            .expect("begin");
        let mut sink = RecordingSink::default();
        session
            .execute(
                "postgresql-types",
                "SELECT TRUE,
                        42::bigint,
                        1.25::numeric,
                        'text'::text,
                        decode('00ff', 'hex')::bytea,
                        DATE '2026-01-02',
                        TIMESTAMP '2026-01-02 03:04:05',
                        TIMESTAMPTZ '2026-01-02 03:04:05+00',
                        '00000000-0000-0000-0000-000000000001'::uuid,
                        '{\"ok\":true}'::jsonb",
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
                "postgresql-large",
                "SELECT value FROM generate_series(1, 4096) AS value",
                &[],
                128,
                &CancellationToken::new(),
                &mut stream_sink,
            )
            .await
            .expect("large stream");
        let rows = streamed_rows(&stream_sink.events);
        assert_eq!(rows, 4096);

        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let error = session
            .execute(
                "postgresql-cancel",
                "SELECT pg_sleep(30)",
                &[],
                64,
                &cancellation,
                &mut RecordingSink::default(),
            )
            .await
            .expect_err("cancelled PostgreSQL query");
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
            "prefer" => ConnectorTlsModeV2::Prefer,
            "require" => ConnectorTlsModeV2::Require,
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
