use std::collections::BTreeMap;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use mysql_async::{
    Column, Conn, Error as MySqlError, Opts, OptsBuilder, Params, Row, SslOpts,
    Value as MySqlValue, prelude::Queryable,
};
use ordadb_connector_sdk::{
    ConnectorBatchV2, ConnectorCapabilitiesV2, ConnectorCapabilitiesV3, ConnectorCatalogColumnV2,
    ConnectorCatalogNodeKindV3, ConnectorCatalogNodeV3, ConnectorCatalogObjectKindV2,
    ConnectorCatalogObjectV2, ConnectorCatalogPageV3, ConnectorColumnV2,
    ConnectorCommandInputModeV3, ConnectorCommandLanguageV3, ConnectorCommandV3,
    ConnectorCredentialV2, ConnectorDriver, ConnectorDriverV3, ConnectorEndpointV2,
    ConnectorEventSink, ConnectorEventSinkV3, ConnectorIsolationLevelV2, ConnectorKindV3,
    ConnectorLogicalTypeV2, ConnectorParameterV2, ConnectorQueryEventV2, ConnectorResultBatchV3,
    ConnectorResultEventV3, ConnectorSession, ConnectorSessionV3, ConnectorTlsModeV2,
    ConnectorTypeV2, ConnectorValueV2, run_named_pipe_helper, run_named_pipe_helper_v3,
};
use ordadb_types::{DbError, Result};
use serde_json::Value as JsonValue;
use tokio_util::sync::CancellationToken;

const MYSQL_PLUGIN_ID: &str = "mysql";
const MARIADB_PLUGIN_ID: &str = "mariadb";
const MARIADB_LANGUAGE_ID: &str = "mariadb-sql";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MySqlFamily {
    MySql,
    MariaDb,
}

impl MySqlFamily {
    const fn id(self) -> &'static str {
        match self {
            Self::MySql => MYSQL_PLUGIN_ID,
            Self::MariaDb => MARIADB_PLUGIN_ID,
        }
    }

    const fn display_name(self) -> &'static str {
        match self {
            Self::MySql => "MySQL",
            Self::MariaDb => "MariaDB",
        }
    }
}

#[derive(Debug, Default)]
struct MySqlDriver;

struct MySqlConnectOptions {
    family: MySqlFamily,
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

pub async fn run_mysql_helper(pipe: &std::ffi::OsStr) -> Result<()> {
    run_named_pipe_helper(
        pipe,
        MYSQL_PLUGIN_ID,
        env!("CARGO_PKG_VERSION"),
        MySqlDriver,
    )
    .await
}

pub async fn run_mariadb_helper(pipe: &std::ffi::OsStr) -> Result<()> {
    run_named_pipe_helper_v3(
        pipe,
        MARIADB_PLUGIN_ID,
        env!("CARGO_PKG_VERSION"),
        MariaDbDriver,
    )
    .await
}

#[derive(Debug, Default)]
struct MariaDbDriver;

struct MariaDbSession {
    connection: Conn,
    connection_id: u64,
    options: MySqlConnectOptions,
    capabilities: ConnectorCapabilitiesV3,
}

#[async_trait]
impl ConnectorDriver for MySqlDriver {
    fn capabilities(&self) -> ConnectorCapabilitiesV2 {
        mysql_capabilities_v2()
    }

    async fn connect(
        &self,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<Box<dyn ConnectorSession>> {
        let options = connection_options(MySqlFamily::MySql, endpoint, tls_mode, credential)?;
        let (connection, connection_id) = connect_family(&options).await?;
        Ok(Box::new(MySqlSession {
            connection,
            connection_id,
            options,
            capabilities: mysql_capabilities_v2(),
        }))
    }
}

#[async_trait]
impl ConnectorDriverV3 for MariaDbDriver {
    fn capabilities(&self) -> ConnectorCapabilitiesV3 {
        mariadb_capabilities_v3()
    }

    async fn connect(
        &self,
        endpoint: ConnectorEndpointV2,
        tls_mode: ConnectorTlsModeV2,
        credential: Option<ConnectorCredentialV2>,
    ) -> Result<Box<dyn ConnectorSessionV3>> {
        let options = connection_options(MySqlFamily::MariaDb, endpoint, tls_mode, credential)?;
        let (mut connection, connection_id) = connect_family(&options).await?;
        let version = connection
            .query_first::<String, _>("SELECT VERSION()")
            .await
            .map_err(mariadb_error)?
            .ok_or_else(|| DbError::new("08006", "MariaDB did not return a server version"))?;
        if !is_mariadb_version(&version) {
            let _ = connection.disconnect().await;
            return Err(DbError::unsupported("MariaDB connector target server")
                .with_hint("Select the MySQL data source for MySQL servers."));
        }
        Ok(Box::new(MariaDbSession {
            connection,
            connection_id,
            options,
            capabilities: mariadb_capabilities_v3(),
        }))
    }
}

#[async_trait]
impl ConnectorSession for MySqlSession {
    fn capabilities(&self) -> &ConnectorCapabilitiesV2 {
        &self.capabilities
    }

    async fn catalog(&mut self) -> Result<Vec<ConnectorCatalogObjectV2>> {
        load_catalog(
            &mut self.connection,
            self.options.database.clone(),
            MySqlFamily::MySql,
        )
        .await
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
                    let _ = kill_family_query(&self.options, self.connection_id).await;
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
        kill_family_query(&self.options, self.connection_id).await
    }

    async fn begin(&mut self, isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
        begin_transaction(&mut self.connection, isolation, MySqlFamily::MySql).await
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

#[async_trait]
impl ConnectorSessionV3 for MariaDbSession {
    fn capabilities(&self) -> &ConnectorCapabilitiesV3 {
        &self.capabilities
    }

    async fn catalog_page(
        &mut self,
        parent_id: Option<&str>,
        page_size: u32,
        cursor: Option<&str>,
    ) -> Result<ConnectorCatalogPageV3> {
        if page_size == 0 || page_size > self.capabilities.maximum_catalog_page_size {
            return Err(invalid(
                "MariaDB Catalog page size is outside its capability",
            ));
        }
        let objects = load_catalog(
            &mut self.connection,
            self.options.database.clone(),
            MySqlFamily::MariaDb,
        )
        .await?;
        let nodes = objects
            .into_iter()
            .filter(|object| object.parent_id.as_deref() == parent_id)
            .map(catalog_node_v3)
            .collect::<Vec<_>>();
        paginate_catalog(nodes, page_size, cursor)
    }

    async fn execute(
        &mut self,
        _request_id: &str,
        command: &ConnectorCommandV3,
        batch_size: u32,
        cancellation: &CancellationToken,
        sink: &mut dyn ConnectorEventSinkV3,
    ) -> Result<()> {
        if batch_size == 0 || batch_size > self.capabilities.maximum_batch_rows {
            return Err(invalid(
                "MariaDB connector batch size is outside its capability",
            ));
        }
        let ConnectorCommandV3::Text {
            language_id,
            text,
            params,
        } = command
        else {
            return Err(DbError::unsupported("MariaDB non-SQL command input"));
        };
        if language_id != MARIADB_LANGUAGE_ID {
            return Err(DbError::unsupported(format!(
                "MariaDB command language {language_id}",
            )));
        }
        let params = Params::Positional(
            params
                .iter()
                .map(mysql_parameter)
                .collect::<Result<Vec<_>>>()?,
        );
        let mut result = self
            .connection
            .exec_iter(text, params)
            .await
            .map_err(mariadb_error)?;
        let columns = result
            .columns_ref()
            .iter()
            .map(mysql_column)
            .collect::<Vec<_>>();
        sink.send(ConnectorResultEventV3::Schema { columns })
            .await?;

        let batch_size = usize::try_from(batch_size).unwrap_or(1_024);
        let mut rows = Vec::with_capacity(batch_size);
        let mut processed = 0_u64;
        loop {
            let row = tokio::select! {
                next = result.next() => next.map_err(mariadb_error)?,
                () = cancellation.cancelled() => {
                    let _ = kill_family_query(&self.options, self.connection_id).await;
                    let _ = result.drop_result().await;
                    return Err(cancelled(MySqlFamily::MariaDb));
                }
            };
            let Some(row) = row else {
                break;
            };
            rows.push(mysql_row(&row)?);
            processed = processed.saturating_add(1);
            if rows.len() == batch_size {
                sink.send(ConnectorResultEventV3::Batch {
                    batch: ConnectorResultBatchV3::Rows {
                        rows: std::mem::take(&mut rows),
                    },
                })
                .await?;
                sink.send(ConnectorResultEventV3::Progress {
                    items_processed: processed,
                })
                .await?;
            }
        }
        let affected_items = if processed == 0 {
            result.affected_rows()
        } else {
            processed
        };
        if !rows.is_empty() {
            sink.send(ConnectorResultEventV3::Batch {
                batch: ConnectorResultBatchV3::Rows { rows },
            })
            .await?;
        }
        sink.send(ConnectorResultEventV3::Progress {
            items_processed: affected_items,
        })
        .await?;
        sink.send(ConnectorResultEventV3::Complete {
            command_tag: command_tag(text),
            affected_items: Some(affected_items),
        })
        .await
    }

    async fn cancel(&mut self, _request_id: &str) -> Result<()> {
        kill_family_query(&self.options, self.connection_id).await
    }

    async fn begin(&mut self, isolation: Option<ConnectorIsolationLevelV2>) -> Result<()> {
        begin_transaction(&mut self.connection, isolation, MySqlFamily::MariaDb).await
    }

    async fn commit(&mut self) -> Result<()> {
        self.connection
            .query_drop("COMMIT")
            .await
            .map_err(mariadb_error)
    }

    async fn rollback(&mut self) -> Result<()> {
        self.connection
            .query_drop("ROLLBACK")
            .await
            .map_err(mariadb_error)
    }
}

fn mysql_capabilities_v2() -> ConnectorCapabilitiesV2 {
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

fn connection_options(
    family: MySqlFamily,
    endpoint: ConnectorEndpointV2,
    tls_mode: ConnectorTlsModeV2,
    credential: Option<ConnectorCredentialV2>,
) -> Result<MySqlConnectOptions> {
    let ConnectorEndpointV2::Network {
        host,
        port,
        database,
        instance,
        options,
    } = endpoint
    else {
        return Err(invalid(format!(
            "{} requires a network endpoint",
            family.display_name()
        )));
    };
    if instance.is_some() {
        return Err(invalid(format!(
            "{} endpoints do not accept an instance name",
            family.display_name()
        )));
    }
    if !options.is_empty() {
        return Err(DbError::unsupported(format!(
            "{} connector endpoint options",
            family.display_name()
        )));
    }
    if tls_mode == ConnectorTlsModeV2::Prefer {
        return Err(
            DbError::unsupported(format!("{} opportunistic TLS", family.display_name()))
                .with_hint("Select disable or an enforced TLS mode."),
        );
    }
    let credential = credential.ok_or_else(|| {
        DbError::new(
            "28000",
            format!("{} credentials are required", family.display_name()),
        )
    })?;
    let username = credential
        .username
        .filter(|username| !username.trim().is_empty())
        .ok_or_else(|| {
            DbError::new(
                "28000",
                format!("{} username is required", family.display_name()),
            )
        })?;
    Ok(MySqlConnectOptions {
        family,
        host,
        port,
        database,
        username,
        secret: credential.secret,
        tls_mode,
    })
}

async fn connect_family(options: &MySqlConnectOptions) -> Result<(Conn, u64)> {
    let mut connection = Conn::new(mysql_options(options))
        .await
        .map_err(|error| family_error(error, options.family))?;
    let connection_id = connection
        .query_first::<u64, _>("SELECT CONNECTION_ID()")
        .await
        .map_err(|error| family_error(error, options.family))?
        .ok_or_else(|| {
            DbError::new(
                "08006",
                format!(
                    "{} did not return a connection ID",
                    options.family.display_name()
                ),
            )
        })?;
    Ok((connection, connection_id))
}

fn mariadb_capabilities_v3() -> ConnectorCapabilitiesV3 {
    ConnectorCapabilitiesV3 {
        kind: ConnectorKindV3::Sql,
        command_languages: vec![ConnectorCommandLanguageV3 {
            id: MARIADB_LANGUAGE_ID.into(),
            display_name: "MariaDB SQL".into(),
            input_modes: vec![ConnectorCommandInputModeV3::Text],
        }],
        catalog: true,
        cancellation: true,
        transactions: true,
        savepoints: true,
        batch_query: true,
        maximum_batch_rows: 1_024,
        maximum_catalog_page_size: 512,
        tls_modes: vec![
            ConnectorTlsModeV2::Disable,
            ConnectorTlsModeV2::Require,
            ConnectorTlsModeV2::VerifyCa,
            ConnectorTlsModeV2::VerifyFull,
        ],
    }
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

async fn kill_family_query(options: &MySqlConnectOptions, connection_id: u64) -> Result<()> {
    let mut control = Conn::new(mysql_options(options))
        .await
        .map_err(|error| family_error(error, options.family))?;
    let result = control
        .query_drop(format!("KILL QUERY {connection_id}"))
        .await
        .map_err(|error| family_error(error, options.family));
    let _ = control.disconnect().await;
    result
}

async fn begin_transaction(
    connection: &mut Conn,
    isolation: Option<ConnectorIsolationLevelV2>,
    family: MySqlFamily,
) -> Result<()> {
    if let Some(isolation) = isolation {
        let level = match isolation {
            ConnectorIsolationLevelV2::ReadUncommitted => "READ UNCOMMITTED",
            ConnectorIsolationLevelV2::ReadCommitted => "READ COMMITTED",
            ConnectorIsolationLevelV2::RepeatableRead => "REPEATABLE READ",
            ConnectorIsolationLevelV2::Serializable => "SERIALIZABLE",
        };
        connection
            .query_drop(format!("SET TRANSACTION ISOLATION LEVEL {level}"))
            .await
            .map_err(|error| family_error(error, family))?;
    }
    connection
        .query_drop("START TRANSACTION")
        .await
        .map_err(|error| family_error(error, family))
}

async fn load_catalog(
    connection: &mut Conn,
    database: Option<String>,
    family: MySqlFamily,
) -> Result<Vec<ConnectorCatalogObjectV2>> {
    let rows = connection
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
            (database.clone(), database),
        )
        .await
        .map_err(|error| family_error(error, family))?;
    catalog_from_rows(rows, family)
}

fn catalog_from_rows(rows: Vec<Row>, family: MySqlFamily) -> Result<Vec<ConnectorCatalogObjectV2>> {
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
                id: format!("{}:database:{}", family.id(), table.catalog),
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
                id: format!("{}:schema:{}", family.id(), table.catalog),
                kind: ConnectorCatalogObjectKindV2::Schema,
                catalog: Some(table.catalog.clone()),
                schema: Some(table.catalog.clone()),
                name: table.catalog.clone(),
                parent_id: Some(format!("{}:database:{}", family.id(), table.catalog)),
                comment: None,
                columns: Vec::new(),
                attributes: BTreeMap::new(),
            });
        }
        objects.push(ConnectorCatalogObjectV2 {
            id: format!(
                "{}:{:?}:{}:{}",
                family.id(),
                table.kind,
                table.catalog,
                table.name
            )
            .to_ascii_lowercase(),
            kind: table.kind,
            catalog: Some(table.catalog.clone()),
            schema: Some(table.catalog.clone()),
            name: table.name,
            parent_id: Some(format!("{}:schema:{}", family.id(), table.catalog)),
            comment: None,
            columns: table.columns,
            attributes: BTreeMap::new(),
        });
    }
    Ok(objects)
}

fn catalog_node_v3(object: ConnectorCatalogObjectV2) -> ConnectorCatalogNodeV3 {
    let kind = match object.kind {
        ConnectorCatalogObjectKindV2::Database => ConnectorCatalogNodeKindV3::Database,
        ConnectorCatalogObjectKindV2::Schema => ConnectorCatalogNodeKindV3::Schema,
        ConnectorCatalogObjectKindV2::Table => ConnectorCatalogNodeKindV3::Table,
        ConnectorCatalogObjectKindV2::View => ConnectorCatalogNodeKindV3::View,
        ConnectorCatalogObjectKindV2::MaterializedView => {
            ConnectorCatalogNodeKindV3::MaterializedView
        }
        ConnectorCatalogObjectKindV2::Column => ConnectorCatalogNodeKindV3::Column,
        ConnectorCatalogObjectKindV2::Index => ConnectorCatalogNodeKindV3::Index,
        ConnectorCatalogObjectKindV2::Constraint => ConnectorCatalogNodeKindV3::Constraint,
        ConnectorCatalogObjectKindV2::Sequence => ConnectorCatalogNodeKindV3::Sequence,
        ConnectorCatalogObjectKindV2::Function => ConnectorCatalogNodeKindV3::Function,
        ConnectorCatalogObjectKindV2::Procedure => ConnectorCatalogNodeKindV3::Procedure,
    };
    let has_children = matches!(
        object.kind,
        ConnectorCatalogObjectKindV2::Database | ConnectorCatalogObjectKindV2::Schema
    );
    ConnectorCatalogNodeV3 {
        id: object.id,
        parent_id: object.parent_id,
        kind,
        name: object.name,
        namespace: object.schema.or(object.catalog),
        has_children,
        columns: object
            .columns
            .into_iter()
            .map(|column| ConnectorColumnV2 {
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
            })
            .collect(),
        attributes: object.attributes,
    }
}

fn paginate_catalog(
    nodes: Vec<ConnectorCatalogNodeV3>,
    page_size: u32,
    cursor: Option<&str>,
) -> Result<ConnectorCatalogPageV3> {
    let offset = cursor
        .map(|value| {
            if value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid("invalid MariaDB Catalog cursor"));
            }
            value
                .parse::<usize>()
                .map_err(|_| invalid("invalid MariaDB Catalog cursor"))
        })
        .transpose()?
        .unwrap_or_default();
    if offset > nodes.len() {
        return Err(invalid("MariaDB Catalog cursor is outside the result set"));
    }
    let page_size = usize::try_from(page_size).unwrap_or(512);
    let total = nodes.len();
    let end = offset.saturating_add(page_size).min(total);
    Ok(ConnectorCatalogPageV3 {
        nodes: nodes.into_iter().skip(offset).take(page_size).collect(),
        next_cursor: (end < total).then(|| end.to_string()),
    })
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
