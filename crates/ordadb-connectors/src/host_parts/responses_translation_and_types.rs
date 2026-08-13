
fn prepare_v3_request(
    request: &ConnectorRequestV3,
    pending: &BTreeMap<String, PendingV3Request>,
) -> Result<Option<(String, PendingV3Request)>> {
    let prepared = match request {
        ConnectorRequestV3::Catalog {
            request_id,
            page_size,
            ..
        } => Some((
            request_id.clone(),
            PendingV3Request::Catalog {
                maximum_nodes: *page_size,
            },
        )),
        ConnectorRequestV3::Execute {
            request_id,
            batch_size,
            ..
        } => Some((
            request_id.clone(),
            PendingV3Request::Execute {
                maximum_batch_rows: *batch_size,
            },
        )),
        ConnectorRequestV3::Begin { request_id, .. }
        | ConnectorRequestV3::Commit { request_id, .. }
        | ConnectorRequestV3::Rollback { request_id, .. } => {
            Some((request_id.clone(), PendingV3Request::Transaction))
        }
        ConnectorRequestV3::Cancel { request_id } => {
            if !matches!(
                pending.get(request_id),
                Some(PendingV3Request::Execute { .. })
            ) {
                return Err(DbError::new(
                    "42704",
                    "connector execution request does not exist",
                ));
            }
            None
        }
        ConnectorRequestV3::Hello { .. }
        | ConnectorRequestV3::Connect { .. }
        | ConnectorRequestV3::Disconnect { .. }
        | ConnectorRequestV3::Shutdown => None,
    };
    if let Some((request_id, _)) = &prepared {
        if pending.contains_key(request_id) {
            return Err(DbError::new("42P04", "connector request already exists"));
        }
        if pending.len() >= MAX_HOST_ACTIVE_V3_REQUESTS {
            return Err(DbError::new(
                "54000",
                "connector has too many active v3 requests",
            ));
        }
    }
    Ok(prepared)
}

fn validate_v3_response(
    response: &ConnectorResponseV3,
    capabilities: &ConnectorCapabilitiesV3,
    pending: &mut BTreeMap<String, PendingV3Request>,
    validators: &mut BTreeMap<String, ConnectorResultStreamValidatorV3>,
) -> Result<()> {
    match response {
        ConnectorResponseV3::Ready { .. } => Err(handshake_response_error()),
        ConnectorResponseV3::Connected {
            capabilities: actual,
            ..
        } => validate_capability_subset_v3(capabilities, actual),
        ConnectorResponseV3::CatalogPage { request_id, page } => {
            let Some(PendingV3Request::Catalog { maximum_nodes }) =
                pending.get(request_id).copied()
            else {
                return Err(unexpected_v3_request_id(request_id, "Catalog"));
            };
            validate_catalog_page_v3(page, maximum_nodes)?;
            pending.remove(request_id);
            Ok(())
        }
        ConnectorResponseV3::ResultEvent { request_id, event } => {
            let Some(PendingV3Request::Execute { maximum_batch_rows }) =
                pending.get(request_id).copied()
            else {
                return Err(unexpected_v3_request_id(request_id, "result"));
            };
            let terminal = matches!(event, ConnectorResultEventV3::Complete { .. });
            validators
                .entry(request_id.clone())
                .or_insert_with(|| {
                    ConnectorResultStreamValidatorV3::new(capabilities.kind, maximum_batch_rows)
                })
                .validate(event)?;
            if terminal {
                validators.remove(request_id);
                pending.remove(request_id);
            }
            Ok(())
        }
        ConnectorResponseV3::Cancelled { request_id } => {
            if !matches!(
                pending.get(request_id),
                Some(PendingV3Request::Execute { .. })
            ) {
                return Err(unexpected_v3_request_id(request_id, "cancellation"));
            }
            validators.remove(request_id);
            pending.remove(request_id);
            Ok(())
        }
        ConnectorResponseV3::Transaction { request_id, .. } => {
            if pending.get(request_id) != Some(&PendingV3Request::Transaction) {
                return Err(unexpected_v3_request_id(request_id, "transaction"));
            }
            pending.remove(request_id);
            Ok(())
        }
        ConnectorResponseV3::Error { request_id, error } => {
            validate_error_v3(error, capabilities.kind)?;
            if let Some(request_id) = request_id {
                if !pending.contains_key(request_id) {
                    return Err(unexpected_v3_request_id(request_id, "error"));
                }
                validators.remove(request_id);
                pending.remove(request_id);
            }
            Ok(())
        }
        ConnectorResponseV3::Disconnected { .. } | ConnectorResponseV3::Shutdown => Ok(()),
    }
}

fn unexpected_v3_request_id(request_id: &str, response_kind: &str) -> DbError {
    DbError::new(
        "08P01",
        format!("connector returned {response_kind} for unknown v3 request {request_id}"),
    )
}

fn validate_v3_id(value: &str, context: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value.chars().any(|character| {
            !character.is_ascii_alphanumeric() && !matches!(character, '-' | '_' | '.')
        })
    {
        return Err(DbError::new(
            "22023",
            format!("connector {context} is invalid"),
        ));
    }
    Ok(())
}

fn translate_response_v2(
    response: ConnectorResponseV2,
    query_schemas: &mut BTreeMap<String, Schema>,
) -> Result<ConnectorResponseV1> {
    match response {
        ConnectorResponseV2::Ready { .. } => Err(handshake_response_error()),
        ConnectorResponseV2::Connected { connection_id, .. } => {
            Ok(ConnectorResponseV1::Connected { connection_id })
        }
        ConnectorResponseV2::Disconnected { connection_id } => Ok(ConnectorResponseV1::Error {
            request_id: None,
            error: DbError::new(
                "08003",
                format!("connector connection {connection_id} was closed"),
            ),
        }),
        ConnectorResponseV2::Catalog {
            request_id,
            objects,
        } => Ok(ConnectorResponseV1::Catalog {
            request_id,
            entries: objects
                .into_iter()
                .map(|object| CatalogEntry {
                    kind: catalog_kind(object.kind).into(),
                    schema: object.schema.unwrap_or_default(),
                    name: object.name,
                })
                .collect(),
        }),
        ConnectorResponseV2::QueryEvent { request_id, event } => {
            let event = query_event_v1(&request_id, event, query_schemas)?;
            Ok(ConnectorResponseV1::QueryEvent { request_id, event })
        }
        ConnectorResponseV2::Cancelled { request_id } => Ok(ConnectorResponseV1::Error {
            request_id: Some(request_id),
            error: DbError::new("57014", "connector query was cancelled"),
        }),
        ConnectorResponseV2::Transaction { request_id, state } => {
            Ok(ConnectorResponseV1::QueryEvent {
                request_id,
                event: QueryEvent::Complete(CommandComplete {
                    tag: format!("{state:?}").to_ascii_uppercase(),
                    rows_affected: 0,
                }),
            })
        }
        ConnectorResponseV2::Error { request_id, error } => Ok(ConnectorResponseV1::Error {
            request_id,
            error: error.into_db_error(),
        }),
        ConnectorResponseV2::Shutdown => Ok(ConnectorResponseV1::Shutdown),
    }
}

fn translate_response_v3(
    response: ConnectorResponseV3,
    query_schemas: &mut BTreeMap<String, Schema>,
) -> Result<ConnectorResponseV1> {
    match response {
        ConnectorResponseV3::Ready { .. } => Err(handshake_response_error()),
        ConnectorResponseV3::Connected { connection_id, .. } => {
            Ok(ConnectorResponseV1::Connected { connection_id })
        }
        ConnectorResponseV3::Disconnected { connection_id } => Ok(ConnectorResponseV1::Error {
            request_id: None,
            error: DbError::new(
                "08003",
                format!("connector connection {connection_id} was closed"),
            ),
        }),
        ConnectorResponseV3::CatalogPage { request_id, page } => Ok(ConnectorResponseV1::Catalog {
            request_id,
            entries: page
                .nodes
                .into_iter()
                .map(|node| CatalogEntry {
                    kind: catalog_kind_v3(node.kind).into(),
                    schema: node.namespace.unwrap_or_default(),
                    name: node.name,
                })
                .collect(),
        }),
        ConnectorResponseV3::ResultEvent { request_id, event } => {
            let event = query_event_v3(&request_id, event, query_schemas)?;
            Ok(ConnectorResponseV1::QueryEvent { request_id, event })
        }
        ConnectorResponseV3::Cancelled { request_id } => Ok(ConnectorResponseV1::Error {
            request_id: Some(request_id),
            error: DbError::new("57014", "connector query was cancelled"),
        }),
        ConnectorResponseV3::Transaction { request_id, state } => {
            Ok(ConnectorResponseV1::QueryEvent {
                request_id,
                event: QueryEvent::Complete(CommandComplete {
                    tag: format!("{state:?}").to_ascii_uppercase(),
                    rows_affected: 0,
                }),
            })
        }
        ConnectorResponseV3::Error { request_id, error } => Ok(ConnectorResponseV1::Error {
            request_id,
            error: error.into_db_error(),
        }),
        ConnectorResponseV3::Shutdown => Ok(ConnectorResponseV1::Shutdown),
    }
}

fn query_event_v3(
    request_id: &str,
    event: ConnectorResultEventV3,
    query_schemas: &mut BTreeMap<String, Schema>,
) -> Result<QueryEvent> {
    match event {
        ConnectorResultEventV3::Schema { columns } => {
            let schema = Schema::new(
                columns
                    .into_iter()
                    .map(|column| {
                        Field::new(column.name, scalar_type(&column.data_type), column.nullable)
                    })
                    .collect(),
            );
            query_schemas.insert(request_id.to_owned(), schema.clone());
            Ok(QueryEvent::Schema(schema))
        }
        ConnectorResultEventV3::Batch {
            batch: ConnectorResultBatchV3::Rows { rows },
        } => {
            let schema = query_schemas.get(request_id).cloned().ok_or_else(|| {
                DbError::new(
                    "08P01",
                    "connector sent a row batch before the query schema",
                )
            })?;
            let rows = rows
                .into_iter()
                .map(|values| {
                    values
                        .into_iter()
                        .map(value_v1)
                        .collect::<Result<Vec<_>>>()
                        .map(Row::new)
                })
                .collect::<Result<Vec<_>>>()?;
            if rows
                .iter()
                .any(|row| row.values.len() != schema.fields.len())
            {
                return Err(DbError::new(
                    "08P01",
                    "connector row width does not match the query schema",
                ));
            }
            Ok(QueryEvent::Batch(Batch { schema, rows }))
        }
        ConnectorResultEventV3::Batch {
            batch:
                ConnectorResultBatchV3::Documents { .. } | ConnectorResultBatchV3::KeyValues { .. },
        } => Err(DbError::unsupported(
            "document or key/value results through the legacy SQL event adapter",
        )),
        ConnectorResultEventV3::Progress { items_processed } => {
            Ok(QueryEvent::Progress(QueryProgress {
                rows_processed: items_processed,
            }))
        }
        ConnectorResultEventV3::Notice { notice } => Ok(QueryEvent::Notice(DbNotice {
            severity: ordadb_types::DbNoticeSeverity::Notice,
            sql_state: notice.code.unwrap_or_else(|| "00000".into()),
            message: notice.message,
            detail: None,
            hint: None,
            position: None,
            object_identity: None,
        })),
        ConnectorResultEventV3::Complete {
            command_tag,
            affected_items,
        } => {
            query_schemas.remove(request_id);
            Ok(QueryEvent::Complete(CommandComplete {
                tag: command_tag,
                rows_affected: affected_items.unwrap_or(0),
            }))
        }
    }
}

fn query_event_v1(
    request_id: &str,
    event: ConnectorQueryEventV2,
    query_schemas: &mut BTreeMap<String, Schema>,
) -> Result<QueryEvent> {
    match event {
        ConnectorQueryEventV2::Schema { columns } => {
            let schema = Schema::new(
                columns
                    .into_iter()
                    .map(|column| {
                        Field::new(column.name, scalar_type(&column.data_type), column.nullable)
                    })
                    .collect(),
            );
            query_schemas.insert(request_id.to_owned(), schema.clone());
            Ok(QueryEvent::Schema(schema))
        }
        ConnectorQueryEventV2::Batch { batch } => {
            let schema = query_schemas.get(request_id).cloned().ok_or_else(|| {
                DbError::new(
                    "08P01",
                    "connector sent a row batch before the query schema",
                )
            })?;
            let rows = batch
                .rows
                .into_iter()
                .map(|values| {
                    values
                        .into_iter()
                        .map(value_v1)
                        .collect::<Result<Vec<_>>>()
                        .map(Row::new)
                })
                .collect::<Result<Vec<_>>>()?;
            if rows
                .iter()
                .any(|row| row.values.len() != schema.fields.len())
            {
                return Err(DbError::new(
                    "08P01",
                    "connector row width does not match the query schema",
                ));
            }
            Ok(QueryEvent::Batch(Batch { schema, rows }))
        }
        ConnectorQueryEventV2::Progress { rows_processed } => {
            Ok(QueryEvent::Progress(QueryProgress { rows_processed }))
        }
        ConnectorQueryEventV2::Notice { notice } => Ok(QueryEvent::Notice(DbNotice {
            severity: ordadb_types::DbNoticeSeverity::Notice,
            sql_state: notice.code.unwrap_or_else(|| "00000".into()),
            message: notice.message,
            detail: None,
            hint: None,
            position: None,
            object_identity: None,
        })),
        ConnectorQueryEventV2::Complete {
            command_tag,
            affected_rows,
        } => {
            query_schemas.remove(request_id);
            Ok(QueryEvent::Complete(CommandComplete {
                tag: command_tag,
                rows_affected: affected_rows.unwrap_or(0),
            }))
        }
    }
}

fn structured_endpoint(
    plugin_id: &str,
    endpoint: &str,
    database: Option<String>,
) -> Result<ConnectorEndpointV2> {
    if matches!(plugin_id, "sqlite" | "ordadb-sqlite") {
        return Ok(ConnectorEndpointV2::File {
            path: endpoint.to_owned(),
            read_only: false,
            create: true,
            options: BTreeMap::new(),
        });
    }
    let default_port = match plugin_id {
        "mongodb" => 27017,
        "redis" => 6379,
        "mysql" | "ordadb-mysql" | "mariadb" => 3306,
        "clickhouse" => 8123,
        "oracle" => 1521,
        "sql-server" | "ordadb-sql-server" => 1433,
        _ => 5432,
    };
    let (host_and_instance, port) = split_host_port(endpoint, default_port)?;
    let (host, instance) = host_and_instance
        .split_once('\\')
        .map_or((host_and_instance.as_str(), None), |(host, instance)| {
            (host, Some(instance.to_owned()))
        });
    if host.trim().is_empty() {
        return Err(DbError::new("22023", "connector endpoint host is empty"));
    }
    Ok(ConnectorEndpointV2::Network {
        host: host.to_owned(),
        port,
        database,
        instance,
        options: BTreeMap::new(),
    })
}

fn split_host_port(endpoint: &str, default_port: u16) -> Result<(String, u16)> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() || endpoint.chars().any(char::is_control) {
        return Err(DbError::new("22023", "connector endpoint is invalid"));
    }
    if let Some(rest) = endpoint.strip_prefix('[') {
        let (host, suffix) = rest
            .split_once(']')
            .ok_or_else(|| DbError::new("22023", "connector IPv6 endpoint is invalid"))?;
        let port = suffix
            .strip_prefix(':')
            .map(str::parse)
            .transpose()
            .map_err(|_| DbError::new("22023", "connector endpoint port is invalid"))?
            .unwrap_or(default_port);
        return Ok((host.to_owned(), port));
    }
    if let Some((host, port)) = endpoint.rsplit_once(':')
        && !host.contains(':')
        && let Ok(port) = port.parse::<u16>()
    {
        if port == 0 {
            return Err(DbError::new(
                "22023",
                "connector endpoint port must be positive",
            ));
        }
        return Ok((host.to_owned(), port));
    }
    if endpoint.contains(':') {
        return Err(DbError::new(
            "22023",
            "IPv6 connector endpoints must use brackets",
        ));
    }
    Ok((endpoint.to_owned(), default_port))
}

fn default_tls_mode(plugin_id: &str) -> ConnectorTlsModeV2 {
    if matches!(plugin_id, "sqlite" | "ordadb-sqlite") {
        ConnectorTlsModeV2::Disable
    } else {
        ConnectorTlsModeV2::Prefer
    }
}

fn parameter_v2(value: &Value) -> ConnectorParameterV2 {
    ConnectorParameterV2 {
        data_type: value.scalar_type().map(|scalar| connector_type(&scalar)),
        value: value_v2(value),
    }
}

fn value_v2(value: &Value) -> ConnectorValueV2 {
    match value {
        Value::Null => ConnectorValueV2::Null,
        Value::Boolean(value) => ConnectorValueV2::Boolean(*value),
        Value::Int16(value) => ConnectorValueV2::SignedInteger(i64::from(*value)),
        Value::Int32(value) => ConnectorValueV2::SignedInteger(i64::from(*value)),
        Value::Int64(value) => ConnectorValueV2::SignedInteger(*value),
        Value::Float32(value) => ConnectorValueV2::FloatingPoint(f64::from(*value)),
        Value::Float64(value) => ConnectorValueV2::FloatingPoint(*value),
        Value::Decimal(value) => ConnectorValueV2::Decimal(value.to_string()),
        Value::Text(value) => ConnectorValueV2::Text(value.clone()),
        Value::Binary(value) => ConnectorValueV2::Binary(BASE64.encode(value)),
        Value::Date(value) => ConnectorValueV2::Date(value.to_string()),
        Value::Time(value) => ConnectorValueV2::Time(value.to_string()),
        Value::Timestamp(value) => ConnectorValueV2::Timestamp(value.to_string()),
        Value::Interval(value) => ConnectorValueV2::Interval(value.to_string()),
        Value::Array(array) => {
            ConnectorValueV2::Array(array.values().iter().map(value_v2).collect())
        }
        Value::Json(value) | Value::Jsonb(value) => ConnectorValueV2::Json(value.clone()),
        Value::Uuid(value) => ConnectorValueV2::Uuid(value.to_string()),
        Value::Vector(values) => ConnectorValueV2::Array(
            values
                .iter()
                .map(|value| ConnectorValueV2::FloatingPoint(f64::from(*value)))
                .collect(),
        ),
    }
}

fn value_v1(value: ConnectorValueV2) -> Result<Value> {
    match value {
        ConnectorValueV2::Null => Ok(Value::Null),
        ConnectorValueV2::Boolean(value) => Ok(Value::Boolean(value)),
        ConnectorValueV2::SignedInteger(value) => Ok(Value::Int64(value)),
        ConnectorValueV2::UnsignedInteger(value) => i64::try_from(value)
            .map(Value::Int64)
            .map_err(|_| DbError::new("22003", "connector unsigned integer exceeds int64")),
        ConnectorValueV2::FloatingPoint(value) => Ok(Value::Float64(value)),
        ConnectorValueV2::Decimal(value) => {
            Decimal::from_str(&value)
                .map(Value::Decimal)
                .map_err(|error| {
                    DbError::new("22P02", "connector decimal value is invalid")
                        .with_detail(error.to_string())
                })
        }
        ConnectorValueV2::Text(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::TimestampWithTimeZone(value) => Ok(Value::Text(value)),
        ConnectorValueV2::Binary(value) => BASE64
            .decode(value)
            .map(Value::Binary)
            .map_err(|_| DbError::new("22P02", "connector binary value is not valid base64")),
        ConnectorValueV2::Date(value) => NaiveDate::parse_from_str(&value, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|error| temporal_error("date", error)),
        ConnectorValueV2::Time(value) => NaiveTime::parse_from_str(&value, "%H:%M:%S%.f")
            .map(Value::Time)
            .map_err(|error| temporal_error("time", error)),
        ConnectorValueV2::Timestamp(value) => parse_timestamp(&value).map(Value::Timestamp),
        ConnectorValueV2::Uuid(value) => Uuid::parse_str(&value)
            .map(Value::Uuid)
            .map_err(|error| temporal_error("UUID", error)),
        ConnectorValueV2::Json(value) => Ok(Value::Jsonb(value)),
        ConnectorValueV2::Array(values) => Ok(Value::Jsonb(serde_json::Value::Array(
            values
                .into_iter()
                .map(connector_value_json)
                .collect::<Result<Vec<_>>>()?,
        ))),
    }
}

fn connector_value_json(value: ConnectorValueV2) -> Result<serde_json::Value> {
    match value {
        ConnectorValueV2::Null => Ok(serde_json::Value::Null),
        ConnectorValueV2::Boolean(value) => Ok(value.into()),
        ConnectorValueV2::SignedInteger(value) => Ok(value.into()),
        ConnectorValueV2::UnsignedInteger(value) => Ok(value.into()),
        ConnectorValueV2::FloatingPoint(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| DbError::new("22003", "connector floating-point value is not finite")),
        ConnectorValueV2::Json(value) => Ok(value),
        ConnectorValueV2::Array(values) => Ok(serde_json::Value::Array(
            values
                .into_iter()
                .map(connector_value_json)
                .collect::<Result<Vec<_>>>()?,
        )),
        ConnectorValueV2::Binary(value) => Ok(value.into()),
        ConnectorValueV2::Decimal(value)
        | ConnectorValueV2::Text(value)
        | ConnectorValueV2::Date(value)
        | ConnectorValueV2::Time(value)
        | ConnectorValueV2::Timestamp(value)
        | ConnectorValueV2::TimestampWithTimeZone(value)
        | ConnectorValueV2::Interval(value)
        | ConnectorValueV2::Uuid(value) => Ok(value.into()),
    }
}

fn parse_timestamp(value: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.naive_utc()))
        .map_err(|error| temporal_error("timestamp", error))
}

fn temporal_error(name: &str, error: impl std::fmt::Display) -> DbError {
    DbError::new("22007", format!("connector {name} value is invalid"))
        .with_detail(error.to_string())
}

fn connector_type(data_type: &ScalarType) -> ConnectorTypeV2 {
    let element_type = match data_type {
        ScalarType::Array { element } => Some(Box::new(connector_type(element))),
        _ => None,
    };
    let (vendor_name, logical_type, precision, scale, length) = match data_type {
        ScalarType::Boolean => ("boolean", ConnectorLogicalTypeV2::Boolean, None, None, None),
        ScalarType::Int16 => (
            "smallint",
            ConnectorLogicalTypeV2::SignedInteger,
            None,
            None,
            None,
        ),
        ScalarType::Int32 => (
            "integer",
            ConnectorLogicalTypeV2::SignedInteger,
            None,
            None,
            None,
        ),
        ScalarType::Int64 => (
            "bigint",
            ConnectorLogicalTypeV2::SignedInteger,
            None,
            None,
            None,
        ),
        ScalarType::Oid => (
            "oid",
            ConnectorLogicalTypeV2::UnsignedInteger,
            None,
            None,
            None,
        ),
        ScalarType::Name => ("name", ConnectorLogicalTypeV2::Text, None, None, Some(63)),
        ScalarType::InternalChar => ("char", ConnectorLogicalTypeV2::Text, None, None, Some(1)),
        ScalarType::Float32 => (
            "real",
            ConnectorLogicalTypeV2::FloatingPoint,
            None,
            None,
            None,
        ),
        ScalarType::Float64 => (
            "double precision",
            ConnectorLogicalTypeV2::FloatingPoint,
            None,
            None,
            None,
        ),
        ScalarType::Decimal { precision, scale } => (
            "numeric",
            ConnectorLogicalTypeV2::Decimal,
            precision.map(u32::from),
            scale.map(u32::from),
            None,
        ),
        ScalarType::Char { length } => (
            "char",
            ConnectorLogicalTypeV2::Text,
            None,
            None,
            length.map(u64::from),
        ),
        ScalarType::Varchar { length } => (
            "varchar",
            ConnectorLogicalTypeV2::Text,
            None,
            None,
            length.map(u64::from),
        ),
        ScalarType::Enum { .. } => ("enum", ConnectorLogicalTypeV2::Text, None, None, None),
        ScalarType::Text => ("text", ConnectorLogicalTypeV2::Text, None, None, None),
        ScalarType::Binary => ("bytea", ConnectorLogicalTypeV2::Binary, None, None, None),
        ScalarType::Date => ("date", ConnectorLogicalTypeV2::Date, None, None, None),
        ScalarType::Time => ("time", ConnectorLogicalTypeV2::Time, None, None, None),
        ScalarType::Interval => (
            "interval",
            ConnectorLogicalTypeV2::Interval,
            None,
            None,
            None,
        ),
        ScalarType::Timestamp {
            with_timezone: true,
        } => (
            "timestamptz",
            ConnectorLogicalTypeV2::TimestampWithTimeZone,
            None,
            None,
            None,
        ),
        ScalarType::Timestamp {
            with_timezone: false,
        } => (
            "timestamp",
            ConnectorLogicalTypeV2::Timestamp,
            None,
            None,
            None,
        ),
        ScalarType::Json => ("json", ConnectorLogicalTypeV2::Json, None, None, None),
        ScalarType::Jsonb => ("jsonb", ConnectorLogicalTypeV2::Json, None, None, None),
        ScalarType::Uuid => ("uuid", ConnectorLogicalTypeV2::Uuid, None, None, None),
        ScalarType::Array { .. } => ("array", ConnectorLogicalTypeV2::Array, None, None, None),
        ScalarType::Vector { dimensions } => (
            "vector",
            ConnectorLogicalTypeV2::Array,
            None,
            None,
            dimensions.and_then(|value| u64::try_from(value).ok()),
        ),
    };
    ConnectorTypeV2 {
        vendor_name: vendor_name.into(),
        logical_type,
        element_type,
        precision,
        scale,
        length,
    }
}

fn scalar_type(data_type: &ConnectorTypeV2) -> ScalarType {
    match data_type.logical_type {
        ConnectorLogicalTypeV2::Boolean => ScalarType::Boolean,
        ConnectorLogicalTypeV2::SignedInteger | ConnectorLogicalTypeV2::UnsignedInteger => {
            ScalarType::Int64
        }
        ConnectorLogicalTypeV2::FloatingPoint => ScalarType::Float64,
        ConnectorLogicalTypeV2::Decimal => ScalarType::Decimal {
            precision: data_type
                .precision
                .and_then(|precision| u8::try_from(precision).ok()),
            scale: data_type.scale.and_then(|scale| u8::try_from(scale).ok()),
        },
        ConnectorLogicalTypeV2::Binary => ScalarType::Binary,
        ConnectorLogicalTypeV2::Date => ScalarType::Date,
        ConnectorLogicalTypeV2::Time => ScalarType::Time,
        ConnectorLogicalTypeV2::Timestamp => ScalarType::Timestamp {
            with_timezone: false,
        },
        ConnectorLogicalTypeV2::TimestampWithTimeZone => ScalarType::Timestamp {
            with_timezone: true,
        },
        ConnectorLogicalTypeV2::Uuid => ScalarType::Uuid,
        ConnectorLogicalTypeV2::Json | ConnectorLogicalTypeV2::Array => ScalarType::Jsonb,
        ConnectorLogicalTypeV2::Null
        | ConnectorLogicalTypeV2::Text
        | ConnectorLogicalTypeV2::Interval
        | ConnectorLogicalTypeV2::Other => ScalarType::Text,
    }
}
