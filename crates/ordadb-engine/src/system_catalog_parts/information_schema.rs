
fn push_information_schema_table(
    rows: &mut SystemRelationRows,
    database: &str,
    schema: &str,
    table: &str,
    kind: &str,
    insertable: &str,
) -> ordadb_types::Result<()> {
    push_row(
        rows,
        Row::new(vec![
            Value::Text(database.to_owned()),
            Value::Text(schema.to_owned()),
            Value::Text(table.to_owned()),
            Value::Text(kind.to_owned()),
            Value::Text(insertable.to_owned()),
        ]),
    )
}

fn build_information_schema_columns(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    let database = catalog.database().name.as_str();
    for schema in catalog.database().schemas() {
        let materialized_tables = schema
            .views()
            .filter_map(|view| view.materialized_table_id)
            .collect::<BTreeSet<_>>();
        for table in schema
            .tables()
            .filter(|table| !materialized_tables.contains(&table.id))
        {
            if !object_visible(catalog, CatalogObjectRef::Table(table.id), authorization) {
                continue;
            }
            for (ordinal, column) in table.columns().iter().enumerate() {
                push_information_schema_column(
                    rows,
                    InformationSchemaColumn {
                        catalog: database,
                        schema: schema.name.as_str(),
                        table: table.name.as_str(),
                        column: column.name.as_str(),
                        ordinal,
                        default: column.default.as_ref().map(|value| value.sql.as_str()),
                        nullable: column.nullable,
                        data_type: &column.data_type,
                    },
                )?;
            }
        }
        for view in schema.views() {
            if !object_visible(catalog, CatalogObjectRef::View(view.id), authorization) {
                continue;
            }
            for (ordinal, field) in view.output.fields.iter().enumerate() {
                push_information_schema_column(
                    rows,
                    InformationSchemaColumn {
                        catalog: database,
                        schema: schema.name.as_str(),
                        table: view.name.as_str(),
                        column: &field.name,
                        ordinal,
                        default: None,
                        nullable: field.nullable,
                        data_type: &field.data_type,
                    },
                )?;
            }
        }
    }
    Ok(())
}

struct InformationSchemaColumn<'a> {
    catalog: &'a str,
    schema: &'a str,
    table: &'a str,
    column: &'a str,
    ordinal: usize,
    default: Option<&'a str>,
    nullable: bool,
    data_type: &'a ScalarType,
}

fn push_information_schema_column(
    rows: &mut SystemRelationRows,
    column: InformationSchemaColumn<'_>,
) -> ordadb_types::Result<()> {
    let ordinal = column
        .ordinal
        .checked_add(1)
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| DbError::new("54000", "column ordinal exceeds i32"))?;
    let (character_length, numeric_precision, numeric_scale, datetime_precision) =
        information_schema_type_metadata(column.data_type);
    let udt_name = scalar_type_name(column.data_type);
    push_row(
        rows,
        Row::new(vec![
            Value::Text(column.catalog.to_owned()),
            Value::Text(column.schema.to_owned()),
            Value::Text(column.table.to_owned()),
            Value::Text(column.column.to_owned()),
            Value::Int32(ordinal),
            column
                .default
                .map_or(Value::Null, |value| Value::Text(value.to_owned())),
            Value::Text(if column.nullable { "YES" } else { "NO" }.to_owned()),
            Value::Text(information_schema_type_name(column.data_type).to_owned()),
            character_length.map_or(Value::Null, Value::Int64),
            numeric_precision.map_or(Value::Null, Value::Int32),
            numeric_scale.map_or(Value::Null, Value::Int32),
            datetime_precision.map_or(Value::Null, Value::Int32),
            Value::Text(column.catalog.to_owned()),
            Value::Text(
                if matches!(
                    column.data_type,
                    ScalarType::Enum { .. } | ScalarType::Array { .. }
                ) {
                    column.schema.to_owned()
                } else {
                    "pg_catalog".to_owned()
                },
            ),
            Value::Text(udt_name.to_owned()),
        ]),
    )
}

fn build_information_schema_views(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    let database = catalog.database().name.as_str();
    for schema in catalog.database().schemas() {
        for view in schema.views().filter(|view| view.kind == ViewKind::Regular) {
            let object = CatalogObjectRef::View(view.id);
            if !object_visible(catalog, object, authorization) {
                continue;
            }
            let definition = if definition_visible(catalog, object, authorization) {
                Value::Text(view.query.clone())
            } else {
                Value::Null
            };
            push_row(
                rows,
                Row::new(vec![
                    Value::Text(database.to_owned()),
                    Value::Text(schema.name.as_str().to_owned()),
                    Value::Text(view.name.as_str().to_owned()),
                    definition,
                    Value::Text("NONE".to_owned()),
                    Value::Text("NO".to_owned()),
                    Value::Text("NO".to_owned()),
                ]),
            )?;
        }
    }
    Ok(())
}

fn definition_visible(
    catalog: &Catalog,
    object: CatalogObjectRef,
    authorization: Option<&SessionAuthorization>,
) -> bool {
    authorization.is_none_or(|authorization| {
        authorization.bypasses_ownership()
            || catalog
                .owner_of(object)
                .is_some_and(|owner| owner == authorization.owner())
    })
}

fn build_information_schema_sequences(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    let database = catalog.database().name.as_str();
    for schema in catalog.database().schemas() {
        for sequence in schema.sequences() {
            if !object_visible(
                catalog,
                CatalogObjectRef::Sequence(sequence.id),
                authorization,
            ) {
                continue;
            }
            let (_, numeric_precision, numeric_scale, _) =
                information_schema_type_metadata(&sequence.data_type);
            push_row(
                rows,
                Row::new(vec![
                    Value::Text(database.to_owned()),
                    Value::Text(schema.name.as_str().to_owned()),
                    Value::Text(sequence.name.as_str().to_owned()),
                    Value::Text(information_schema_type_name(&sequence.data_type).to_owned()),
                    numeric_precision.map_or(Value::Null, Value::Int32),
                    numeric_scale.map_or(Value::Null, Value::Int32),
                    Value::Text(sequence.start_value.to_string()),
                    Value::Text(sequence.min_value.to_string()),
                    Value::Text(sequence.max_value.to_string()),
                    Value::Text(sequence.increment.to_string()),
                    Value::Text(if sequence.cycle { "YES" } else { "NO" }.to_owned()),
                ]),
            )?;
        }
    }
    Ok(())
}

fn build_information_schema_table_constraints(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    let database = catalog.database().name.as_str();
    for schema in catalog.database().schemas() {
        for table in schema.tables() {
            if !object_visible(catalog, CatalogObjectRef::Table(table.id), authorization) {
                continue;
            }
            for constraint in table.constraints() {
                push_row(
                    rows,
                    Row::new(vec![
                        Value::Text(database.to_owned()),
                        Value::Text(schema.name.as_str().to_owned()),
                        Value::Text(constraint.name.as_str().to_owned()),
                        Value::Text(database.to_owned()),
                        Value::Text(schema.name.as_str().to_owned()),
                        Value::Text(table.name.as_str().to_owned()),
                        Value::Text(
                            information_schema_constraint_type(&constraint.kind).to_owned(),
                        ),
                        Value::Text("NO".to_owned()),
                        Value::Text("NO".to_owned()),
                        Value::Text("YES".to_owned()),
                    ]),
                )?;
            }
        }
    }
    Ok(())
}

const fn information_schema_constraint_type(kind: &ConstraintKind) -> &'static str {
    match kind {
        ConstraintKind::PrimaryKey { .. } => "PRIMARY KEY",
        ConstraintKind::Unique { .. } => "UNIQUE",
        ConstraintKind::Check { .. } => "CHECK",
        ConstraintKind::ForeignKey { .. } => "FOREIGN KEY",
    }
}

fn build_information_schema_key_column_usage(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    let database = catalog.database().name.as_str();
    for schema in catalog.database().schemas() {
        for table in schema.tables() {
            if !object_visible(catalog, CatalogObjectRef::Table(table.id), authorization) {
                continue;
            }
            for constraint in table.constraints() {
                let (columns, has_unique_position) = match &constraint.kind {
                    ConstraintKind::PrimaryKey { columns } | ConstraintKind::Unique { columns } => {
                        (columns.as_slice(), false)
                    }
                    ConstraintKind::ForeignKey { columns, .. } => (columns.as_slice(), true),
                    ConstraintKind::Check { .. } => continue,
                };
                for (ordinal, column_id) in columns.iter().enumerate() {
                    let column = table
                        .columns()
                        .iter()
                        .find(|column| column.id == *column_id)
                        .ok_or_else(|| {
                            DbError::new(
                                "XX001",
                                "catalog constraint references a missing table column",
                            )
                        })?;
                    let ordinal = ordinal
                        .checked_add(1)
                        .and_then(|value| i32::try_from(value).ok())
                        .ok_or_else(|| {
                            DbError::new("54000", "constraint column ordinal exceeds i32")
                        })?;
                    push_row(
                        rows,
                        Row::new(vec![
                            Value::Text(database.to_owned()),
                            Value::Text(schema.name.as_str().to_owned()),
                            Value::Text(constraint.name.as_str().to_owned()),
                            Value::Text(database.to_owned()),
                            Value::Text(schema.name.as_str().to_owned()),
                            Value::Text(table.name.as_str().to_owned()),
                            Value::Text(column.name.as_str().to_owned()),
                            Value::Int32(ordinal),
                            if has_unique_position {
                                Value::Int32(ordinal)
                            } else {
                                Value::Null
                            },
                        ]),
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn build_information_schema_routines(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    let database = catalog.database().name.as_str();
    for schema in catalog.database().schemas() {
        for routine in schema.routines() {
            if !object_visible(
                catalog,
                CatalogObjectRef::Routine(routine.id),
                authorization,
            ) {
                continue;
            }
            let specific_name = routine_specific_name(catalog, routine)?;
            push_row(
                rows,
                Row::new(vec![
                    Value::Text(database.to_owned()),
                    Value::Text(schema.name.as_str().to_owned()),
                    Value::Text(specific_name),
                    Value::Text(database.to_owned()),
                    Value::Text(schema.name.as_str().to_owned()),
                    Value::Text(routine.name.as_str().to_owned()),
                    Value::Text(
                        match routine.kind {
                            RoutineKind::Function => "FUNCTION",
                            RoutineKind::Procedure => "PROCEDURE",
                        }
                        .to_owned(),
                    ),
                    routine
                        .return_type
                        .as_ref()
                        .or_else(|| {
                            let mut outputs = routine.output_arguments();
                            let first = outputs.next()?;
                            outputs.next().is_none().then_some(&first.data_type)
                        })
                        .map_or(Value::Null, |data_type| {
                            Value::Text(information_schema_type_name(data_type).to_owned())
                        }),
                    Value::Null,
                    Value::Text(routine.language.to_ascii_uppercase()),
                ]),
            )?;
        }
    }
    Ok(())
}

fn routine_specific_name(
    catalog: &Catalog,
    routine: &ordadb_catalog::RoutineDefinition,
) -> ordadb_types::Result<String> {
    object_oid(catalog, PostgresOidObject::Routine(routine.id))
        .map(|oid| format!("{}_{}", routine.name.as_str(), oid))
}

fn build_information_schema_parameters(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    let database = catalog.database().name.as_str();
    for schema in catalog.database().schemas() {
        for routine in schema.routines() {
            if !object_visible(
                catalog,
                CatalogObjectRef::Routine(routine.id),
                authorization,
            ) {
                continue;
            }
            let specific_name = routine_specific_name(catalog, routine)?;
            if let Some(return_type) = routine.return_type.as_ref() {
                push_information_schema_parameter(
                    catalog,
                    rows,
                    InformationSchemaParameter {
                        database,
                        schema: schema.name.as_str(),
                        specific_name: &specific_name,
                        ordinal: 0,
                        mode: None,
                        name: None,
                        data_type: return_type,
                        declared_type: routine.return_declared_type,
                    },
                )?;
            }
            for (ordinal, argument) in routine.arguments.iter().enumerate() {
                let ordinal = ordinal
                    .checked_add(1)
                    .and_then(|value| i32::try_from(value).ok())
                    .ok_or_else(|| {
                        DbError::new("54000", "routine parameter ordinal exceeds i32")
                    })?;
                push_information_schema_parameter(
                    catalog,
                    rows,
                    InformationSchemaParameter {
                        database,
                        schema: schema.name.as_str(),
                        specific_name: &specific_name,
                        ordinal,
                        mode: Some(match argument.mode {
                            RoutineArgumentMode::In | RoutineArgumentMode::Variadic => "IN",
                            RoutineArgumentMode::Out => "OUT",
                            RoutineArgumentMode::InOut => "INOUT",
                        }),
                        name: argument.name.as_ref().map(|name| name.as_str()),
                        data_type: &argument.data_type,
                        declared_type: argument.declared_type,
                    },
                )?;
            }
        }
    }
    Ok(())
}

struct InformationSchemaParameter<'a> {
    database: &'a str,
    schema: &'a str,
    specific_name: &'a str,
    ordinal: i32,
    mode: Option<&'a str>,
    name: Option<&'a str>,
    data_type: &'a ScalarType,
    declared_type: Option<TypeId>,
}

fn push_information_schema_parameter(
    catalog: &Catalog,
    rows: &mut SystemRelationRows,
    parameter: InformationSchemaParameter<'_>,
) -> ordadb_types::Result<()> {
    let (udt_schema, udt_name) = information_schema_udt(
        catalog,
        parameter.schema,
        parameter.data_type,
        parameter.declared_type,
    )?;
    push_row(
        rows,
        Row::new(vec![
            Value::Text(parameter.database.to_owned()),
            Value::Text(parameter.schema.to_owned()),
            Value::Text(parameter.specific_name.to_owned()),
            Value::Int32(parameter.ordinal),
            parameter
                .mode
                .map_or(Value::Null, |mode| Value::Text(mode.to_owned())),
            parameter
                .name
                .map_or(Value::Null, |name| Value::Text(name.to_owned())),
            Value::Text(information_schema_type_name(parameter.data_type).to_owned()),
            Value::Text(parameter.database.to_owned()),
            Value::Text(udt_schema),
            Value::Text(udt_name),
        ]),
    )
}

fn information_schema_udt(
    catalog: &Catalog,
    object_schema: &str,
    data_type: &ScalarType,
    declared_type: Option<TypeId>,
) -> ordadb_types::Result<(String, String)> {
    let user_type_id = declared_type.or_else(|| match data_type {
        ScalarType::Enum { type_id, .. } => Some(*type_id),
        ScalarType::Array { element } => match element.as_ref() {
            ScalarType::Enum { type_id, .. } => Some(*type_id),
            _ => None,
        },
        _ => None,
    });
    if let Some(type_id) = user_type_id {
        let definition = catalog.type_by_id(type_id).ok_or_else(|| {
            DbError::new("XX001", "information schema references a missing user type")
        })?;
        let schema = catalog.schema_by_id(definition.schema_id).ok_or_else(|| {
            DbError::new(
                "XX001",
                "information schema references a missing type schema",
            )
        })?;
        let prefix = if matches!(data_type, ScalarType::Array { .. }) {
            "_"
        } else {
            ""
        };
        return Ok((
            schema.name.as_str().to_owned(),
            format!("{prefix}{}", definition.name.as_str()),
        ));
    }
    let name = match data_type {
        ScalarType::Array { element } => format!("_{}", scalar_type_name(element)),
        _ => scalar_type_name(data_type).to_owned(),
    };
    let schema = if matches!(data_type, ScalarType::Enum { .. }) {
        object_schema
    } else {
        "pg_catalog"
    };
    Ok((schema.to_owned(), name))
}

fn scalar_type_oid(
    catalog: &Catalog,
    data_type: &ScalarType,
    declared_type: Option<TypeId>,
) -> ordadb_types::Result<i64> {
    if let Some(type_id) = declared_type {
        return object_oid(catalog, PostgresOidObject::Type(type_id));
    }
    let oid = match data_type {
        ScalarType::Boolean => 16,
        ScalarType::Binary => 17,
        ScalarType::InternalChar => 18,
        ScalarType::Name => 19,
        ScalarType::Int64 => 20,
        ScalarType::Int16 => 21,
        ScalarType::Int32 => 23,
        ScalarType::Text => 25,
        ScalarType::Oid => 26,
        ScalarType::Json => 114,
        ScalarType::Float32 => 700,
        ScalarType::Float64 => 701,
        ScalarType::Char { .. } => 1_042,
        ScalarType::Varchar { .. } => 1_043,
        ScalarType::Date => 1_082,
        ScalarType::Time => 1_083,
        ScalarType::Timestamp {
            with_timezone: false,
        } => 1_114,
        ScalarType::Timestamp {
            with_timezone: true,
        } => 1_184,
        ScalarType::Interval => 1_186,
        ScalarType::Decimal { .. } => 1_700,
        ScalarType::Uuid => 2_950,
        ScalarType::Jsonb => 3_802,
        ScalarType::Vector { .. } => 16_383,
        ScalarType::Enum { type_id, .. } => {
            return object_oid(catalog, PostgresOidObject::Type(*type_id));
        }
        ScalarType::Array { element } => array_type_oid(element)?,
    };
    Ok(i64::from(oid))
}

fn array_type_oid(element: &ScalarType) -> ordadb_types::Result<u32> {
    let oid = match element {
        ScalarType::Boolean => 1_000,
        ScalarType::Binary => 1_001,
        ScalarType::InternalChar => 1_002,
        ScalarType::Name => 1_003,
        ScalarType::Int16 => 1_005,
        ScalarType::Int32 => 1_007,
        ScalarType::Text => 1_009,
        ScalarType::Oid => 1_028,
        ScalarType::Char { .. } => 1_014,
        ScalarType::Varchar { .. } => 1_015,
        ScalarType::Int64 => 1_016,
        ScalarType::Float32 => 1_021,
        ScalarType::Float64 => 1_022,
        ScalarType::Date => 1_182,
        ScalarType::Time => 1_183,
        ScalarType::Timestamp {
            with_timezone: false,
        } => 1_115,
        ScalarType::Timestamp {
            with_timezone: true,
        } => 1_185,
        ScalarType::Interval => 1_187,
        ScalarType::Decimal { .. } => 1_231,
        ScalarType::Json => 199,
        ScalarType::Uuid => 2_951,
        ScalarType::Jsonb => 3_807,
        ScalarType::Enum { .. } => 0,
        ScalarType::Array { .. } => {
            return Err(DbError::new(
                "0A000",
                "nested PostgreSQL array element types are not supported",
            ));
        }
        ScalarType::Vector { .. } => 0,
    };
    Ok(oid)
}

const fn scalar_type_category(data_type: &ScalarType) -> &'static str {
    match data_type {
        ScalarType::Boolean => "B",
        ScalarType::Int16
        | ScalarType::Int32
        | ScalarType::Int64
        | ScalarType::Oid
        | ScalarType::Float32
        | ScalarType::Float64
        | ScalarType::Decimal { .. } => "N",
        ScalarType::Name
        | ScalarType::InternalChar
        | ScalarType::Char { .. }
        | ScalarType::Varchar { .. }
        | ScalarType::Text => "S",
        ScalarType::Date
        | ScalarType::Time
        | ScalarType::Timestamp { .. }
        | ScalarType::Interval => "D",
        ScalarType::Array { .. } => "A",
        ScalarType::Enum { .. } => "E",
        ScalarType::Binary
        | ScalarType::Json
        | ScalarType::Jsonb
        | ScalarType::Uuid
        | ScalarType::Vector { .. } => "U",
    }
}

const fn information_schema_type_name(data_type: &ScalarType) -> &'static str {
    match data_type {
        ScalarType::Boolean => "boolean",
        ScalarType::Int16 => "smallint",
        ScalarType::Int32 => "integer",
        ScalarType::Int64 => "bigint",
        ScalarType::Oid => "oid",
        ScalarType::Name => "name",
        ScalarType::InternalChar => "\"char\"",
        ScalarType::Float32 => "real",
        ScalarType::Float64 => "double precision",
        ScalarType::Decimal { .. } => "numeric",
        ScalarType::Char { .. } => "character",
        ScalarType::Varchar { .. } => "character varying",
        ScalarType::Text => "text",
        ScalarType::Enum { .. } | ScalarType::Vector { .. } => "USER-DEFINED",
        ScalarType::Binary => "bytea",
        ScalarType::Date => "date",
        ScalarType::Time => "time without time zone",
        ScalarType::Timestamp {
            with_timezone: false,
        } => "timestamp without time zone",
        ScalarType::Timestamp {
            with_timezone: true,
        } => "timestamp with time zone",
        ScalarType::Interval => "interval",
        ScalarType::Array { .. } => "ARRAY",
        ScalarType::Json => "json",
        ScalarType::Jsonb => "jsonb",
        ScalarType::Uuid => "uuid",
    }
}

const fn scalar_type_name(data_type: &ScalarType) -> &'static str {
    match data_type {
        ScalarType::Boolean => "bool",
        ScalarType::Int16 => "int2",
        ScalarType::Int32 => "int4",
        ScalarType::Int64 => "int8",
        ScalarType::Oid => "oid",
        ScalarType::Name => "name",
        ScalarType::InternalChar => "char",
        ScalarType::Float32 => "float4",
        ScalarType::Float64 => "float8",
        ScalarType::Decimal { .. } => "numeric",
        ScalarType::Char { .. } => "bpchar",
        ScalarType::Varchar { .. } => "varchar",
        ScalarType::Text => "text",
        ScalarType::Enum { .. } => "enum",
        ScalarType::Binary => "bytea",
        ScalarType::Date => "date",
        ScalarType::Time => "time",
        ScalarType::Timestamp {
            with_timezone: false,
        } => "timestamp",
        ScalarType::Timestamp {
            with_timezone: true,
        } => "timestamptz",
        ScalarType::Interval => "interval",
        ScalarType::Array { .. } => "array",
        ScalarType::Json => "json",
        ScalarType::Jsonb => "jsonb",
        ScalarType::Uuid => "uuid",
        ScalarType::Vector { .. } => "vector",
    }
}

fn information_schema_type_metadata(
    data_type: &ScalarType,
) -> (Option<i64>, Option<i32>, Option<i32>, Option<i32>) {
    match data_type {
        ScalarType::Char { length } | ScalarType::Varchar { length } => {
            (length.map(i64::from), None, None, None)
        }
        ScalarType::Decimal { precision, scale } => {
            (None, precision.map(i32::from), scale.map(i32::from), None)
        }
        ScalarType::Int16 => (None, Some(16), Some(0), None),
        ScalarType::Int32 => (None, Some(32), Some(0), None),
        ScalarType::Int64 => (None, Some(64), Some(0), None),
        ScalarType::Float32 => (None, Some(24), None, None),
        ScalarType::Float64 => (None, Some(53), None, None),
        ScalarType::Time | ScalarType::Timestamp { .. } | ScalarType::Interval => {
            (None, None, None, Some(6))
        }
        _ => (None, None, None, None),
    }
}
