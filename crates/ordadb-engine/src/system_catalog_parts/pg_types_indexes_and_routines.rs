
fn build_pg_type(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for builtin in BUILTIN_TYPES {
        push_type_row(
            rows,
            i64::from(builtin.oid),
            builtin.name,
            PG_CATALOG_NAMESPACE_OID,
            "b",
            builtin.category,
            0,
            i64::from(builtin.array_oid),
            0,
            false,
            Value::Null,
        )?;
        if builtin.array_oid != 0 {
            push_type_row(
                rows,
                i64::from(builtin.array_oid),
                &format!("_{}", builtin.name),
                PG_CATALOG_NAMESPACE_OID,
                "b",
                "A",
                i64::from(builtin.oid),
                0,
                0,
                false,
                Value::Null,
            )?;
        }
    }
    for schema in catalog.database().schemas() {
        let namespace_oid = schema_oid(catalog, schema.id)?;
        for definition in schema.types() {
            if !object_visible(
                catalog,
                CatalogObjectRef::Type(definition.id),
                authorization,
            ) {
                continue;
            }
            let oid = object_oid(catalog, PostgresOidObject::Type(definition.id))?;
            let (kind, category, base_oid, not_null, default) = match &definition.definition {
                UserDefinedTypeKind::Enum { .. } => ("e", "E", 0, false, Value::Null),
                UserDefinedTypeKind::Domain {
                    base_type,
                    base_declared_type,
                    not_null,
                    default,
                    ..
                } => (
                    "d",
                    scalar_type_category(base_type),
                    scalar_type_oid(catalog, base_type, *base_declared_type)?,
                    *not_null,
                    default.as_ref().map_or(Value::Null, |expression| {
                        Value::Text(expression.sql.clone())
                    }),
                ),
            };
            push_type_row(
                rows,
                oid,
                definition.name.as_str(),
                namespace_oid,
                kind,
                category,
                0,
                0,
                base_oid,
                not_null,
                default,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_type_row(
    rows: &mut SystemRelationRows,
    oid: i64,
    name: &str,
    namespace_oid: i64,
    kind: &str,
    category: &str,
    element_oid: i64,
    array_oid: i64,
    base_oid: i64,
    not_null: bool,
    default: Value,
) -> ordadb_types::Result<()> {
    push_row(
        rows,
        Row::new(vec![
            Value::Int64(oid),
            Value::Text(name.to_owned()),
            Value::Int64(namespace_oid),
            Value::Text(kind.to_owned()),
            Value::Text(category.to_owned()),
            Value::Boolean(true),
            Value::Int64(element_oid),
            Value::Int64(array_oid),
            Value::Int64(base_oid),
            Value::Boolean(not_null),
            default,
        ]),
    )
}

fn build_pg_enum(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for schema in catalog.database().schemas() {
        for definition in schema.types() {
            if !object_visible(
                catalog,
                CatalogObjectRef::Type(definition.id),
                authorization,
            ) {
                continue;
            }
            let UserDefinedTypeKind::Enum { labels } = &definition.definition else {
                continue;
            };
            let type_oid = object_oid(catalog, PostgresOidObject::Type(definition.id))?;
            for (ordinal, label) in labels.iter().enumerate() {
                let ordinal_u64 = u64::try_from(ordinal)
                    .map_err(|_| DbError::new("54000", "enum label ordinal exceeds u64"))?;
                let label_oid = 1_000_000_000_u64
                    .checked_add(definition.id.get().saturating_mul(1_024))
                    .and_then(|value| value.checked_add(ordinal_u64))
                    .filter(|value| *value <= u64::from(u32::MAX))
                    .ok_or_else(|| DbError::new("54000", "enum label OID space is exhausted"))?;
                let sort_order = u16::try_from(ordinal)
                    .ok()
                    .and_then(|ordinal| ordinal.checked_add(1))
                    .map(f32::from)
                    .ok_or_else(|| DbError::new("54000", "enum label ordinal exceeds float4"))?;
                push_row(
                    rows,
                    Row::new(vec![
                        Value::Int64(
                            i64::try_from(label_oid).map_err(|_| {
                                DbError::new("54000", "enum label OID exceeds int64")
                            })?,
                        ),
                        Value::Int64(type_oid),
                        Value::Float32(sort_order),
                        Value::Text(label.clone()),
                    ]),
                )?;
            }
        }
    }
    Ok(())
}

fn build_pg_index(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for schema in catalog.database().schemas() {
        for table in schema.tables() {
            if !object_visible(catalog, CatalogObjectRef::Table(table.id), authorization) {
                continue;
            }
            let table_oid = object_oid(catalog, PostgresOidObject::Table(table.id))?;
            for index in table.indexes() {
                let key = index
                    .key_columns
                    .iter()
                    .map(|column_id| {
                        table
                            .columns()
                            .iter()
                            .position(|column| column.id == *column_id)
                            .and_then(|position| position.checked_add(1))
                            .ok_or_else(|| {
                                DbError::internal("index references a missing table column")
                            })
                            .map(|position| position.to_string())
                    })
                    .collect::<ordadb_types::Result<Vec<_>>>()?
                    .join(" ");
                push_row(
                    rows,
                    Row::new(vec![
                        Value::Int64(object_oid(catalog, PostgresOidObject::Index(index.id))?),
                        Value::Int64(table_oid),
                        Value::Boolean(index.unique),
                        Value::Boolean(index.primary),
                        Value::Text(key),
                    ]),
                )?;
            }
        }
    }
    Ok(())
}

fn build_pg_constraint(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for schema in catalog.database().schemas() {
        let namespace_oid = schema_oid(catalog, schema.id)?;
        for table in schema.tables() {
            if !object_visible(catalog, CatalogObjectRef::Table(table.id), authorization) {
                continue;
            }
            let table_oid = object_oid(catalog, PostgresOidObject::Table(table.id))?;
            for constraint in table.constraints() {
                let (kind, expression) = match &constraint.kind {
                    ConstraintKind::PrimaryKey { .. } => ("p", Value::Null),
                    ConstraintKind::Unique { .. } => ("u", Value::Null),
                    ConstraintKind::ForeignKey { .. } => ("f", Value::Null),
                    ConstraintKind::Check { expression } => {
                        ("c", Value::Text(expression.sql.clone()))
                    }
                };
                push_row(
                    rows,
                    Row::new(vec![
                        Value::Int64(object_oid(
                            catalog,
                            PostgresOidObject::Constraint(constraint.id),
                        )?),
                        Value::Text(constraint.name.as_str().to_owned()),
                        Value::Text(kind.to_owned()),
                        Value::Int64(namespace_oid),
                        Value::Int64(table_oid),
                        Value::Int64(0),
                        Value::Boolean(true),
                        expression,
                    ]),
                )?;
            }
        }
        for definition in schema.types() {
            if !object_visible(
                catalog,
                CatalogObjectRef::Type(definition.id),
                authorization,
            ) {
                continue;
            }
            let UserDefinedTypeKind::Domain { checks, .. } = &definition.definition else {
                continue;
            };
            let type_oid = object_oid(catalog, PostgresOidObject::Type(definition.id))?;
            for (ordinal, constraint) in checks.iter().enumerate() {
                let oid = match constraint.id {
                    Some(id) => object_oid(catalog, PostgresOidObject::Constraint(id))?,
                    None => legacy_domain_constraint_oid(definition.id, ordinal)?,
                };
                let name = constraint.name.as_ref().map_or_else(
                    || {
                        if ordinal == 0 {
                            format!("{}_check", definition.name.as_str())
                        } else {
                            format!("{}_check{}", definition.name.as_str(), ordinal + 1)
                        }
                    },
                    |name| name.as_str().to_owned(),
                );
                push_row(
                    rows,
                    Row::new(vec![
                        Value::Int64(oid),
                        Value::Text(name),
                        Value::Text("c".to_owned()),
                        Value::Int64(namespace_oid),
                        Value::Int64(0),
                        Value::Int64(type_oid),
                        Value::Boolean(true),
                        Value::Text(constraint.expression.sql.clone()),
                    ]),
                )?;
            }
        }
    }
    Ok(())
}

fn legacy_domain_constraint_oid(type_id: TypeId, ordinal: usize) -> ordadb_types::Result<i64> {
    let ordinal = u64::try_from(ordinal)
        .map_err(|_| DbError::new("54000", "domain constraint ordinal exceeds u64"))?;
    1_200_000_000_u64
        .checked_add(type_id.get().saturating_mul(1_024))
        .and_then(|value| value.checked_add(ordinal))
        .filter(|value| *value <= u64::from(u32::MAX))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| DbError::new("54000", "domain constraint OID space is exhausted"))
}

fn build_pg_proc(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for (oid, name, return_type_oid) in [
        (3_i64, "heap_tableam_handler", 269_i64),
        (330_i64, "bthandler", 325_i64),
    ] {
        push_row(
            rows,
            Row::new(vec![
                Value::Int64(oid),
                Value::Text(name.to_owned()),
                Value::Int64(PG_CATALOG_NAMESPACE_OID),
                Value::Text("f".to_owned()),
                Value::Int64(return_type_oid),
                Value::Boolean(false),
                Value::Text(String::new()),
                Value::Text("internal".to_owned()),
                Value::Null,
                Value::Null,
                Value::Null,
            ]),
        )?;
    }
    for schema in catalog.database().schemas() {
        let namespace_oid = schema_oid(catalog, schema.id)?;
        for routine in schema.routines() {
            if !object_visible(
                catalog,
                CatalogObjectRef::Routine(routine.id),
                authorization,
            ) {
                continue;
            }
            let output_arguments = routine.output_arguments().collect::<Vec<_>>();
            let return_oid = match output_arguments.as_slice() {
                [] => routine
                    .return_type
                    .as_ref()
                    .map_or(Ok(2_278_i64), |data_type| {
                        scalar_type_oid(catalog, data_type, routine.return_declared_type)
                    })?,
                [argument] => {
                    scalar_type_oid(catalog, &argument.data_type, argument.declared_type)?
                }
                _ => 2_249,
            };
            let argument_oids = routine
                .input_arguments()
                .map(|argument| {
                    scalar_type_oid(catalog, &argument.data_type, argument.declared_type)
                        .map(|oid| oid.to_string())
                })
                .collect::<ordadb_types::Result<Vec<_>>>()?
                .join(" ");
            let all_argument_oids = output_arguments.first().map(|_| {
                routine
                    .arguments
                    .iter()
                    .map(|argument| {
                        scalar_type_oid(catalog, &argument.data_type, argument.declared_type)
                            .map(|oid| oid.to_string())
                    })
                    .collect::<ordadb_types::Result<Vec<_>>>()
                    .map(|oids| oids.join(" "))
            });
            let argument_modes = output_arguments.first().map(|_| {
                routine
                    .arguments
                    .iter()
                    .map(|argument| match argument.mode {
                        RoutineArgumentMode::In => "i",
                        RoutineArgumentMode::Out => "o",
                        RoutineArgumentMode::InOut => "b",
                        RoutineArgumentMode::Variadic => "v",
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            });
            let argument_names = routine
                .arguments
                .iter()
                .any(|argument| argument.name.is_some())
                .then(|| {
                    routine
                        .arguments
                        .iter()
                        .map(|argument| argument.name.as_ref().map_or("", |name| name.as_str()))
                        .collect::<Vec<_>>()
                        .join(" ")
                });
            push_row(
                rows,
                Row::new(vec![
                    Value::Int64(object_oid(catalog, PostgresOidObject::Routine(routine.id))?),
                    Value::Text(routine.name.as_str().to_owned()),
                    Value::Int64(namespace_oid),
                    Value::Text(
                        match routine.kind {
                            RoutineKind::Function => "f",
                            RoutineKind::Procedure => "p",
                        }
                        .to_owned(),
                    ),
                    Value::Int64(return_oid),
                    Value::Boolean(routine.returns_set),
                    Value::Text(argument_oids),
                    Value::Text(routine.language.clone()),
                    all_argument_oids
                        .transpose()?
                        .map_or(Value::Null, Value::Text),
                    argument_modes.map_or(Value::Null, Value::Text),
                    argument_names.map_or(Value::Null, Value::Text),
                ]),
            )?;
        }
    }
    Ok(())
}

fn build_pg_trigger(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for schema in catalog.database().schemas() {
        for table in schema.tables() {
            if !object_visible(catalog, CatalogObjectRef::Table(table.id), authorization) {
                continue;
            }
            push_pg_trigger_rows(
                catalog,
                object_oid(catalog, PostgresOidObject::Table(table.id))?,
                table.triggers(),
                rows,
            )?;
        }
        for view in schema.views() {
            if !object_visible(catalog, CatalogObjectRef::View(view.id), authorization) {
                continue;
            }
            push_pg_trigger_rows(
                catalog,
                object_oid(catalog, PostgresOidObject::View(view.id))?,
                view.triggers(),
                rows,
            )?;
        }
    }
    Ok(())
}

fn push_pg_trigger_rows<'a>(
    catalog: &Catalog,
    relation_oid: i64,
    triggers: impl Iterator<Item = &'a TriggerDefinition>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for trigger in triggers {
        push_row(
            rows,
            Row::new(vec![
                Value::Int64(object_oid(catalog, PostgresOidObject::Trigger(trigger.id))?),
                Value::Text(trigger.name.as_str().to_owned()),
                Value::Int64(relation_oid),
                Value::Text(if trigger.enabled { "O" } else { "D" }.to_owned()),
                Value::Boolean(false),
                Value::Int64(object_oid(
                    catalog,
                    PostgresOidObject::Routine(trigger.routine_id),
                )?),
                Value::Int16(pg_trigger_type(
                    trigger.timing,
                    trigger.level,
                    &trigger.events,
                )),
            ]),
        )?;
    }
    Ok(())
}

fn pg_trigger_type(
    timing: TriggerTiming,
    level: TriggerLevel,
    events: &BTreeSet<TriggerEvent>,
) -> i16 {
    let mut value = 0_i16;
    if level == TriggerLevel::Row {
        value |= 1;
    }
    if matches!(
        timing,
        TriggerTiming::Before | TriggerTiming::BeforeStatement
    ) {
        value |= 2;
    }
    if timing == TriggerTiming::InsteadOf {
        value |= 64;
    }
    for event in events {
        value |= match event {
            TriggerEvent::Insert => 4,
            TriggerEvent::Delete => 8,
            TriggerEvent::Update => 16,
        };
    }
    value
}

fn build_pg_roles(
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    let Some(authorization) = authorization else {
        return Ok(());
    };
    for role in authorization.catalog_roles() {
        push_row(
            rows,
            Row::new(vec![
                Value::Text(role.name.clone()),
                Value::Boolean(false),
                Value::Boolean(true),
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Boolean(role.can_login && role.login_enabled),
                Value::Boolean(false),
                Value::Int32(-1),
                Value::Text("********".to_owned()),
                Value::Null,
                Value::Boolean(false),
                Value::Null,
                Value::Int64(i64::from(role.postgres_oid)),
            ]),
        )?;
    }
    Ok(())
}

fn build_pg_user(
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    let Some(authorization) = authorization else {
        return Ok(());
    };
    for role in authorization
        .catalog_roles()
        .iter()
        .filter(|role| role.can_login)
    {
        push_row(
            rows,
            Row::new(vec![
                Value::Text(role.name.clone()),
                Value::Int64(i64::from(role.postgres_oid)),
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Text("********".to_owned()),
                Value::Null,
                Value::Null,
            ]),
        )?;
    }
    Ok(())
}

fn build_pg_settings(
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    let Some(authorization) = authorization else {
        return Ok(());
    };
    for setting in authorization.catalog_settings() {
        push_row(
            rows,
            Row::new(vec![
                Value::Text(setting.name.clone()),
                Value::Text(setting.setting.clone()),
                setting.unit.clone().map_or(Value::Null, Value::Text),
                Value::Text(setting.category.clone()),
                Value::Text(setting.short_description.clone()),
                Value::Text(setting.context.clone()),
                Value::Text(setting.value_type.clone()),
                Value::Text(setting.source.clone()),
                setting.minimum.clone().map_or(Value::Null, Value::Text),
                setting.maximum.clone().map_or(Value::Null, Value::Text),
                setting.enum_values.clone().map_or(Value::Null, Value::Text),
                Value::Text(setting.boot_value.clone()),
                Value::Text(setting.reset_value.clone()),
            ]),
        )?;
    }
    Ok(())
}

fn build_pg_am(rows: &mut SystemRelationRows) -> ordadb_types::Result<()> {
    for (oid, name, handler_oid, kind) in [
        (2_i64, "heap", 3_i64, "t"),
        (403_i64, "btree", 330_i64, "i"),
    ] {
        push_row(
            rows,
            Row::new(vec![
                Value::Int64(oid),
                Value::Text(name.to_owned()),
                Value::Int64(handler_oid),
                Value::Text(kind.to_owned()),
            ]),
        )?;
    }
    Ok(())
}

fn build_pg_collation(rows: &mut SystemRelationRows) -> ordadb_types::Result<()> {
    for (oid, name) in [(950_i64, "C"), (951_i64, "POSIX")] {
        push_row(
            rows,
            Row::new(vec![
                Value::Int64(oid),
                Value::Text(name.to_owned()),
                Value::Int64(PG_CATALOG_NAMESPACE_OID),
                Value::Int64(PG_BOOTSTRAP_ROLE_OID),
                Value::Text("c".to_owned()),
                Value::Boolean(true),
                Value::Int32(-1),
                Value::Text(name.to_owned()),
                Value::Text(name.to_owned()),
            ]),
        )?;
    }
    Ok(())
}

fn build_pg_depend(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for dependent in catalog.object_refs() {
        if !object_visible(catalog, dependent, authorization) {
            continue;
        }
        let dependent_address = pg_catalog_object_address(catalog, dependent)?;
        for referenced in catalog.dependencies().references(dependent) {
            if !object_visible(catalog, referenced, authorization) {
                continue;
            }
            let referenced_address = pg_catalog_object_address(catalog, referenced)?;
            push_row(
                rows,
                Row::new(vec![
                    Value::Int64(dependent_address.class_oid),
                    Value::Int64(dependent_address.object_oid),
                    Value::Int32(dependent_address.sub_id),
                    Value::Int64(referenced_address.class_oid),
                    Value::Int64(referenced_address.object_oid),
                    Value::Int32(referenced_address.sub_id),
                    Value::Text("n".to_owned()),
                ]),
            )?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct PgCatalogObjectAddress {
    class_oid: i64,
    object_oid: i64,
    sub_id: i32,
}

fn pg_catalog_object_address(
    catalog: &Catalog,
    object: CatalogObjectRef,
) -> ordadb_types::Result<PgCatalogObjectAddress> {
    let (class_oid, object_oid, sub_id) = match object {
        CatalogObjectRef::Schema(schema_id) => (
            PG_NAMESPACE_RELATION_OID,
            object_oid(catalog, PostgresOidObject::Schema(schema_id))?,
            0,
        ),
        CatalogObjectRef::Table(table_id) => (
            PG_CLASS_RELATION_OID,
            object_oid(catalog, PostgresOidObject::Table(table_id))?,
            0,
        ),
        CatalogObjectRef::Column(table_id, column_id) => {
            let table = catalog.table_by_id(table_id).ok_or_else(|| {
                DbError::new(
                    "XX001",
                    "catalog dependency references a missing column relation",
                )
            })?;
            let sub_id = table
                .columns()
                .iter()
                .position(|column| column.id == column_id)
                .and_then(|position| position.checked_add(1))
                .and_then(|position| i32::try_from(position).ok())
                .ok_or_else(|| {
                    DbError::new(
                        "XX001",
                        "catalog dependency references a missing or oversized column",
                    )
                })?;
            (
                PG_CLASS_RELATION_OID,
                object_oid(catalog, PostgresOidObject::Table(table_id))?,
                sub_id,
            )
        }
        CatalogObjectRef::Index(index_id) => (
            PG_CLASS_RELATION_OID,
            object_oid(catalog, PostgresOidObject::Index(index_id))?,
            0,
        ),
        CatalogObjectRef::Constraint(constraint_id) => (
            PG_CONSTRAINT_RELATION_OID,
            object_oid(catalog, PostgresOidObject::Constraint(constraint_id))?,
            0,
        ),
        CatalogObjectRef::Sequence(sequence_id) => (
            PG_CLASS_RELATION_OID,
            object_oid(catalog, PostgresOidObject::Sequence(sequence_id))?,
            0,
        ),
        CatalogObjectRef::View(view_id) => (
            PG_CLASS_RELATION_OID,
            object_oid(catalog, PostgresOidObject::View(view_id))?,
            0,
        ),
        CatalogObjectRef::Routine(routine_id) => (
            PG_PROC_RELATION_OID,
            object_oid(catalog, PostgresOidObject::Routine(routine_id))?,
            0,
        ),
        CatalogObjectRef::Trigger(trigger_id) => (
            PG_TRIGGER_RELATION_OID,
            object_oid(catalog, PostgresOidObject::Trigger(trigger_id))?,
            0,
        ),
        CatalogObjectRef::Type(type_id) => (
            PG_TYPE_RELATION_OID,
            object_oid(catalog, PostgresOidObject::Type(type_id))?,
            0,
        ),
    };
    Ok(PgCatalogObjectAddress {
        class_oid,
        object_oid,
        sub_id,
    })
}

fn build_information_schema_schemata(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for (name, owner) in [("pg_catalog", "ordadb"), ("information_schema", "ordadb")] {
        push_row(
            rows,
            Row::new(vec![
                Value::Text(catalog.database().name.as_str().to_owned()),
                Value::Text(name.to_owned()),
                Value::Text(owner.to_owned()),
            ]),
        )?;
    }
    for schema in catalog.database().schemas() {
        if !visible_schema(catalog, schema, authorization) {
            continue;
        }
        let owner = catalog
            .owner_of(CatalogObjectRef::Schema(schema.id))
            .map_or("ordadb", ordadb_catalog::CatalogOwner::as_str);
        push_row(
            rows,
            Row::new(vec![
                Value::Text(catalog.database().name.as_str().to_owned()),
                Value::Text(schema.name.as_str().to_owned()),
                Value::Text(owner.to_owned()),
            ]),
        )?;
    }
    Ok(())
}

fn build_information_schema_tables(
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
            if object_visible(catalog, CatalogObjectRef::Table(table.id), authorization) {
                push_information_schema_table(
                    rows,
                    database,
                    schema.name.as_str(),
                    table.name.as_str(),
                    "BASE TABLE",
                    "YES",
                )?;
            }
        }
        for view in schema.views() {
            if object_visible(catalog, CatalogObjectRef::View(view.id), authorization) {
                push_information_schema_table(
                    rows,
                    database,
                    schema.name.as_str(),
                    view.name.as_str(),
                    match view.kind {
                        ViewKind::Regular => "VIEW",
                        ViewKind::Materialized => "MATERIALIZED VIEW",
                    },
                    "NO",
                )?;
            }
        }
    }
    Ok(())
}
