use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ordadb_catalog::{
    Catalog, CatalogObjectRef, ConstraintKind, INFORMATION_SCHEMA_COLUMNS_TABLE_ID,
    INFORMATION_SCHEMA_KEY_COLUMN_USAGE_TABLE_ID, INFORMATION_SCHEMA_PARAMETERS_TABLE_ID,
    INFORMATION_SCHEMA_ROUTINES_TABLE_ID, INFORMATION_SCHEMA_SCHEMATA_TABLE_ID,
    INFORMATION_SCHEMA_SEQUENCES_TABLE_ID, INFORMATION_SCHEMA_TABLE_CONSTRAINTS_TABLE_ID,
    INFORMATION_SCHEMA_TABLES_TABLE_ID, INFORMATION_SCHEMA_VIEWS_TABLE_ID, PG_AM_TABLE_ID,
    PG_ATTRIBUTE_TABLE_ID, PG_CLASS_TABLE_ID, PG_COLLATION_TABLE_ID, PG_CONSTRAINT_TABLE_ID,
    PG_DATABASE_TABLE_ID, PG_DEPEND_TABLE_ID, PG_ENUM_TABLE_ID, PG_INDEX_TABLE_ID,
    PG_NAMESPACE_TABLE_ID, PG_PROC_TABLE_ID, PG_ROLES_TABLE_ID, PG_SETTINGS_TABLE_ID,
    PG_TRIGGER_TABLE_ID, PG_TYPE_TABLE_ID, PG_USER_TABLE_ID, PostgresOidObject,
    RoutineArgumentMode, RoutineKind, SystemRelationDescriptor, TriggerDefinition, TriggerEvent,
    TriggerLevel, TriggerTarget, TriggerTiming, UserDefinedTypeKind, ViewKind, system_relations,
};
use ordadb_execution::{SnapshotTableProvider, TableProvider, TableScan, estimated_row_bytes};
use ordadb_types::{DbError, Row, ScalarType, TableId, TypeId, Value};

use super::SessionAuthorization;

const MAX_SYSTEM_RELATION_ROWS: usize = 1_048_576;
const MAX_SYSTEM_CATALOG_MATERIALIZED_BYTES: usize = 64 * 1024 * 1024;
const PG_CATALOG_NAMESPACE_OID: i64 = 11;
const INFORMATION_SCHEMA_NAMESPACE_OID: i64 = 12_099;
const PG_BOOTSTRAP_ROLE_OID: i64 = 10;
const PG_CLASS_RELATION_OID: i64 = 1_259;
const PG_TYPE_RELATION_OID: i64 = 1_247;
const PG_PROC_RELATION_OID: i64 = 1_255;
const PG_NAMESPACE_RELATION_OID: i64 = 2_615;
const PG_CONSTRAINT_RELATION_OID: i64 = 2_606;
const PG_TRIGGER_RELATION_OID: i64 = 2_620;

#[derive(Debug)]
pub(crate) struct SystemCatalogSnapshot {
    tables: BTreeMap<TableId, Arc<Vec<Row>>>,
}

impl SystemCatalogSnapshot {
    pub(crate) fn tables(&self) -> &BTreeMap<TableId, Arc<Vec<Row>>> {
        &self.tables
    }
}

impl TableProvider for SystemCatalogSnapshot {
    fn scan(&self, table_id: TableId) -> ordadb_types::Result<Box<dyn TableScan>> {
        SnapshotTableProvider::new(&self.tables).scan(table_id)
    }
}

#[derive(Debug)]
struct SystemRelationRows {
    rows: Vec<Row>,
    materialized_bytes: usize,
    byte_limit: usize,
}

impl SystemRelationRows {
    const fn new(byte_limit: usize) -> Self {
        Self {
            rows: Vec::new(),
            materialized_bytes: 0,
            byte_limit,
        }
    }
}

pub(crate) fn build_system_catalog_snapshot(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    requested: &BTreeSet<TableId>,
) -> ordadb_types::Result<SystemCatalogSnapshot> {
    let relation_byte_limit = MAX_SYSTEM_CATALOG_MATERIALIZED_BYTES
        .checked_div(requested.len().max(1))
        .unwrap_or(MAX_SYSTEM_CATALOG_MATERIALIZED_BYTES);
    let mut rows = system_relations()
        .iter()
        .filter(|relation| requested.contains(&relation.table_id))
        .map(|relation| {
            (
                relation.table_id,
                SystemRelationRows::new(relation_byte_limit),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if let Some(relation_rows) = rows.get_mut(&PG_DATABASE_TABLE_ID) {
        build_pg_database(catalog, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_NAMESPACE_TABLE_ID) {
        build_pg_namespace(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_CLASS_TABLE_ID) {
        build_pg_class(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_ATTRIBUTE_TABLE_ID) {
        build_pg_attribute(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_TYPE_TABLE_ID) {
        build_pg_type(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_ENUM_TABLE_ID) {
        build_pg_enum(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_INDEX_TABLE_ID) {
        build_pg_index(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_CONSTRAINT_TABLE_ID) {
        build_pg_constraint(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_PROC_TABLE_ID) {
        build_pg_proc(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_TRIGGER_TABLE_ID) {
        build_pg_trigger(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_ROLES_TABLE_ID) {
        build_pg_roles(authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_USER_TABLE_ID) {
        build_pg_user(authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_SETTINGS_TABLE_ID) {
        build_pg_settings(authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_AM_TABLE_ID) {
        build_pg_am(relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_COLLATION_TABLE_ID) {
        build_pg_collation(relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&PG_DEPEND_TABLE_ID) {
        build_pg_depend(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&INFORMATION_SCHEMA_SCHEMATA_TABLE_ID) {
        build_information_schema_schemata(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&INFORMATION_SCHEMA_TABLES_TABLE_ID) {
        build_information_schema_tables(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&INFORMATION_SCHEMA_COLUMNS_TABLE_ID) {
        build_information_schema_columns(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&INFORMATION_SCHEMA_VIEWS_TABLE_ID) {
        build_information_schema_views(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&INFORMATION_SCHEMA_SEQUENCES_TABLE_ID) {
        build_information_schema_sequences(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&INFORMATION_SCHEMA_TABLE_CONSTRAINTS_TABLE_ID) {
        build_information_schema_table_constraints(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&INFORMATION_SCHEMA_KEY_COLUMN_USAGE_TABLE_ID) {
        build_information_schema_key_column_usage(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&INFORMATION_SCHEMA_ROUTINES_TABLE_ID) {
        build_information_schema_routines(catalog, authorization, relation_rows)?;
    }
    if let Some(relation_rows) = rows.get_mut(&INFORMATION_SCHEMA_PARAMETERS_TABLE_ID) {
        build_information_schema_parameters(catalog, authorization, relation_rows)?;
    }

    for relation in system_relations()
        .iter()
        .filter(|relation| requested.contains(&relation.table_id))
    {
        validate_relation_rows(
            relation,
            rows.get(&relation.table_id)
                .map(|rows| rows.rows.as_slice())
                .unwrap_or_default(),
        )?;
    }
    Ok(SystemCatalogSnapshot {
        tables: rows
            .into_iter()
            .map(|(table_id, rows)| (table_id, Arc::new(rows.rows)))
            .collect(),
    })
}

fn push_row(rows: &mut SystemRelationRows, row: Row) -> ordadb_types::Result<()> {
    if rows.rows.len() >= MAX_SYSTEM_RELATION_ROWS {
        return Err(DbError::new(
            "54000",
            "system relation exceeds the bounded row limit",
        ));
    }
    let next_bytes = rows
        .materialized_bytes
        .checked_add(estimated_row_bytes(&row))
        .ok_or_else(|| DbError::new("54000", "system relation byte count overflowed"))?;
    if next_bytes > rows.byte_limit {
        return Err(DbError::new(
            "54000",
            "system catalog snapshot exceeds the bounded materialization limit",
        ));
    }
    rows.rows.push(row);
    rows.materialized_bytes = next_bytes;
    Ok(())
}

fn validate_relation_rows(
    relation: &SystemRelationDescriptor,
    rows: &[Row],
) -> ordadb_types::Result<()> {
    for row in rows {
        if row.values.len() != relation.columns.len() {
            return Err(DbError::internal(format!(
                "system relation {}.{} produced a row with the wrong width",
                relation.schema, relation.name
            )));
        }
        for (value, column) in row.values.iter().zip(relation.columns) {
            if matches!(value, Value::Null) {
                if !column.nullable {
                    return Err(DbError::internal(format!(
                        "system relation {}.{} produced NULL for {}",
                        relation.schema, relation.name, column.name
                    )));
                }
            } else if !column.data_type.accepts(value) {
                return Err(DbError::internal(format!(
                    "system relation {}.{} produced the wrong type for {}",
                    relation.schema, relation.name, column.name
                )));
            }
        }
    }
    Ok(())
}

fn object_visible(
    catalog: &Catalog,
    object: CatalogObjectRef,
    authorization: Option<&SessionAuthorization>,
) -> bool {
    let Some(authorization) = authorization else {
        return true;
    };
    if authorization.bypasses_ownership() {
        return true;
    }
    catalog
        .owner_of(object)
        .is_none_or(|owner| owner == authorization.owner())
        || object_visible_by_privilege(catalog, object, authorization)
}

fn object_visible_by_privilege(
    catalog: &Catalog,
    object: CatalogObjectRef,
    authorization: &SessionAuthorization,
) -> bool {
    for schema in catalog.database().schemas() {
        let schema_name = schema.name.as_str();
        match object {
            CatalogObjectRef::Schema(schema_id) if schema.id == schema_id => {
                return authorization.can_discover(schema_name, None);
            }
            CatalogObjectRef::Table(table_id) | CatalogObjectRef::Column(table_id, _) => {
                if let Some(table) = schema.tables().find(|table| table.id == table_id) {
                    return authorization.can_discover(schema_name, Some(table.name.as_str()));
                }
            }
            CatalogObjectRef::Index(index_id) => {
                if let Some(table) = schema
                    .tables()
                    .find(|table| table.indexes().any(|index| index.id == index_id))
                {
                    return authorization.can_discover(schema_name, Some(table.name.as_str()));
                }
            }
            CatalogObjectRef::Constraint(constraint_id) => {
                if let Some(table) = schema.tables().find(|table| {
                    table
                        .constraints()
                        .any(|constraint| constraint.id == constraint_id)
                }) {
                    return authorization.can_discover(schema_name, Some(table.name.as_str()));
                }
            }
            CatalogObjectRef::Sequence(sequence_id) => {
                if let Some(sequence) = schema
                    .sequences()
                    .find(|sequence| sequence.id == sequence_id)
                {
                    return authorization.can_discover(schema_name, Some(sequence.name.as_str()));
                }
            }
            CatalogObjectRef::View(view_id) => {
                if let Some(view) = schema.views().find(|view| view.id == view_id) {
                    return authorization.can_discover(schema_name, Some(view.name.as_str()));
                }
            }
            CatalogObjectRef::Routine(routine_id) => {
                if let Some(routine) = schema.routines().find(|routine| routine.id == routine_id) {
                    return authorization.can_discover(schema_name, Some(routine.name.as_str()));
                }
            }
            CatalogObjectRef::Trigger(trigger_id) => {
                if let Some(trigger) = catalog.trigger_by_id(trigger_id) {
                    match trigger.target {
                        TriggerTarget::Table(table_id) => {
                            if let Some(table) = schema.tables().find(|table| table.id == table_id)
                            {
                                return authorization
                                    .can_discover(schema_name, Some(table.name.as_str()));
                            }
                        }
                        TriggerTarget::View(view_id) => {
                            if let Some(view) = schema.views().find(|view| view.id == view_id) {
                                return authorization
                                    .can_discover(schema_name, Some(view.name.as_str()));
                            }
                        }
                    }
                }
            }
            CatalogObjectRef::Type(type_id) => {
                if schema.types().any(|data_type| data_type.id == type_id) {
                    return authorization.can_discover(schema_name, None);
                }
            }
            _ => {}
        }
    }
    false
}

fn visible_schema(
    catalog: &Catalog,
    schema: &ordadb_catalog::SchemaDefinition,
    authorization: Option<&SessionAuthorization>,
) -> bool {
    object_visible(catalog, CatalogObjectRef::Schema(schema.id), authorization)
        || schema
            .tables()
            .any(|table| object_visible(catalog, CatalogObjectRef::Table(table.id), authorization))
        || schema
            .views()
            .any(|view| object_visible(catalog, CatalogObjectRef::View(view.id), authorization))
        || schema.sequences().any(|sequence| {
            object_visible(
                catalog,
                CatalogObjectRef::Sequence(sequence.id),
                authorization,
            )
        })
        || schema.routines().any(|routine| {
            object_visible(
                catalog,
                CatalogObjectRef::Routine(routine.id),
                authorization,
            )
        })
        || schema.types().any(|data_type| {
            object_visible(catalog, CatalogObjectRef::Type(data_type.id), authorization)
        })
}

fn schema_oid(catalog: &Catalog, schema_id: ordadb_types::SchemaId) -> ordadb_types::Result<i64> {
    catalog
        .postgres_oid(PostgresOidObject::Schema(schema_id))
        .map(|oid| i64::from(oid.get()))
}

fn object_oid(catalog: &Catalog, object: PostgresOidObject) -> ordadb_types::Result<i64> {
    catalog.postgres_oid(object).map(|oid| i64::from(oid.get()))
}

fn build_pg_database(catalog: &Catalog, rows: &mut SystemRelationRows) -> ordadb_types::Result<()> {
    push_row(
        rows,
        Row::new(vec![
            Value::Int64(object_oid(
                catalog,
                PostgresOidObject::Database(catalog.database().id),
            )?),
            Value::Text(catalog.database().name.as_str().to_owned()),
            Value::Int64(PG_BOOTSTRAP_ROLE_OID),
            Value::Int32(6),
            Value::Text("C".to_owned()),
            Value::Text("C".to_owned()),
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Int32(-1),
        ]),
    )
}

fn build_pg_namespace(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    push_row(
        rows,
        Row::new(vec![
            Value::Int64(PG_CATALOG_NAMESPACE_OID),
            Value::Text("pg_catalog".to_owned()),
            Value::Int64(PG_BOOTSTRAP_ROLE_OID),
        ]),
    )?;
    push_row(
        rows,
        Row::new(vec![
            Value::Int64(INFORMATION_SCHEMA_NAMESPACE_OID),
            Value::Text("information_schema".to_owned()),
            Value::Int64(PG_BOOTSTRAP_ROLE_OID),
        ]),
    )?;
    for schema in catalog.database().schemas() {
        if visible_schema(catalog, schema, authorization) {
            push_row(
                rows,
                Row::new(vec![
                    Value::Int64(schema_oid(catalog, schema.id)?),
                    Value::Text(schema.name.as_str().to_owned()),
                    Value::Int64(catalog_owner_oid(
                        catalog,
                        CatalogObjectRef::Schema(schema.id),
                        authorization,
                    )),
                ]),
            )?;
        }
    }
    Ok(())
}

fn catalog_owner_oid(
    catalog: &Catalog,
    object: CatalogObjectRef,
    authorization: Option<&SessionAuthorization>,
) -> i64 {
    catalog
        .owner_of(object)
        .and_then(|owner| {
            authorization.and_then(|authorization| {
                authorization
                    .catalog_roles()
                    .iter()
                    .find(|role| role.name == owner.as_str())
            })
        })
        .map_or(PG_BOOTSTRAP_ROLE_OID, |role| i64::from(role.postgres_oid))
}

fn build_pg_class(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for relation in system_relations() {
        push_row(
            rows,
            Row::new(vec![
                Value::Int64(i64::from(relation.oid.get())),
                Value::Text(relation.name.to_owned()),
                Value::Int64(i64::from(relation.schema_oid.get())),
                Value::Text(system_relation_kind(relation).to_owned()),
                Value::Text("p".to_owned()),
                Value::Int32(relation_column_count(relation)?),
                Value::Boolean(false),
            ]),
        )?;
    }
    for schema in catalog.database().schemas() {
        let namespace_oid = schema_oid(catalog, schema.id)?;
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
            push_row(
                rows,
                Row::new(vec![
                    Value::Int64(object_oid(catalog, PostgresOidObject::Table(table.id))?),
                    Value::Text(table.name.as_str().to_owned()),
                    Value::Int64(namespace_oid),
                    Value::Text("r".to_owned()),
                    Value::Text("p".to_owned()),
                    Value::Int32(count_i32(table.columns().len(), "table columns")?),
                    Value::Boolean(table.indexes().next().is_some()),
                ]),
            )?;
            for index in table.indexes() {
                push_row(
                    rows,
                    Row::new(vec![
                        Value::Int64(object_oid(catalog, PostgresOidObject::Index(index.id))?),
                        Value::Text(index.name.as_str().to_owned()),
                        Value::Int64(namespace_oid),
                        Value::Text("i".to_owned()),
                        Value::Text("p".to_owned()),
                        Value::Int32(0),
                        Value::Boolean(false),
                    ]),
                )?;
            }
        }
        for sequence in schema.sequences() {
            if !object_visible(
                catalog,
                CatalogObjectRef::Sequence(sequence.id),
                authorization,
            ) {
                continue;
            }
            push_row(
                rows,
                Row::new(vec![
                    Value::Int64(object_oid(
                        catalog,
                        PostgresOidObject::Sequence(sequence.id),
                    )?),
                    Value::Text(sequence.name.as_str().to_owned()),
                    Value::Int64(namespace_oid),
                    Value::Text("S".to_owned()),
                    Value::Text("p".to_owned()),
                    Value::Int32(0),
                    Value::Boolean(false),
                ]),
            )?;
        }
        for view in schema.views() {
            if !object_visible(catalog, CatalogObjectRef::View(view.id), authorization) {
                continue;
            }
            push_row(
                rows,
                Row::new(vec![
                    Value::Int64(object_oid(catalog, PostgresOidObject::View(view.id))?),
                    Value::Text(view.name.as_str().to_owned()),
                    Value::Int64(namespace_oid),
                    Value::Text(
                        match view.kind {
                            ViewKind::Regular => "v",
                            ViewKind::Materialized => "m",
                        }
                        .to_owned(),
                    ),
                    Value::Text("p".to_owned()),
                    Value::Int32(count_i32(view.output.fields.len(), "view columns")?),
                    Value::Boolean(view.materialized_table_id.is_some_and(|table_id| {
                        catalog
                            .table_by_id(table_id)
                            .is_some_and(|table| table.indexes().next().is_some())
                    })),
                ]),
            )?;
        }
    }
    Ok(())
}

fn system_relation_kind(relation: &SystemRelationDescriptor) -> &'static str {
    if relation.schema == "information_schema"
        || matches!(relation.name, "pg_roles" | "pg_user" | "pg_settings")
    {
        "v"
    } else {
        "r"
    }
}

fn relation_column_count(relation: &SystemRelationDescriptor) -> ordadb_types::Result<i32> {
    count_i32(relation.columns.len(), "system relation columns")
}

fn count_i32(value: usize, context: &str) -> ordadb_types::Result<i32> {
    i32::try_from(value).map_err(|_| DbError::new("54000", format!("{context} exceed i32")))
}

fn build_pg_attribute(
    catalog: &Catalog,
    authorization: Option<&SessionAuthorization>,
    rows: &mut SystemRelationRows,
) -> ordadb_types::Result<()> {
    for relation in system_relations() {
        for (ordinal, column) in relation.columns.iter().enumerate() {
            push_attribute_row(
                rows,
                i64::from(relation.oid.get()),
                column.name,
                scalar_type_oid(catalog, &column.data_type, None)?,
                ordinal,
                !column.nullable,
                false,
            )?;
        }
    }
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
            let relation_oid = object_oid(catalog, PostgresOidObject::Table(table.id))?;
            for (ordinal, column) in table.columns().iter().enumerate() {
                push_attribute_row(
                    rows,
                    relation_oid,
                    column.name.as_str(),
                    scalar_type_oid(catalog, &column.data_type, column.declared_type)?,
                    ordinal,
                    !column.nullable,
                    column.default.is_some(),
                )?;
            }
        }
        for view in schema.views() {
            if !object_visible(catalog, CatalogObjectRef::View(view.id), authorization) {
                continue;
            }
            let relation_oid = object_oid(catalog, PostgresOidObject::View(view.id))?;
            for (ordinal, field) in view.output.fields.iter().enumerate() {
                push_attribute_row(
                    rows,
                    relation_oid,
                    &field.name,
                    scalar_type_oid(catalog, &field.data_type, None)?,
                    ordinal,
                    !field.nullable,
                    false,
                )?;
            }
        }
    }
    Ok(())
}

fn push_attribute_row(
    rows: &mut SystemRelationRows,
    relation_oid: i64,
    name: &str,
    type_oid: i64,
    zero_based_ordinal: usize,
    not_null: bool,
    has_default: bool,
) -> ordadb_types::Result<()> {
    let attnum = zero_based_ordinal
        .checked_add(1)
        .and_then(|ordinal| i16::try_from(ordinal).ok())
        .ok_or_else(|| DbError::new("54000", "relation column ordinal exceeds int16"))?;
    push_row(
        rows,
        Row::new(vec![
            Value::Int64(relation_oid),
            Value::Text(name.to_owned()),
            Value::Int64(type_oid),
            Value::Int16(attnum),
            Value::Boolean(not_null),
            Value::Boolean(has_default),
            Value::Boolean(false),
        ]),
    )
}

#[derive(Clone, Copy)]
struct BuiltinType {
    oid: u32,
    name: &'static str,
    category: &'static str,
    array_oid: u32,
}

const BUILTIN_TYPES: &[BuiltinType] = &[
    BuiltinType {
        oid: 16,
        name: "bool",
        category: "B",
        array_oid: 1_000,
    },
    BuiltinType {
        oid: 17,
        name: "bytea",
        category: "U",
        array_oid: 1_001,
    },
    BuiltinType {
        oid: 20,
        name: "int8",
        category: "N",
        array_oid: 1_016,
    },
    BuiltinType {
        oid: 21,
        name: "int2",
        category: "N",
        array_oid: 1_005,
    },
    BuiltinType {
        oid: 23,
        name: "int4",
        category: "N",
        array_oid: 1_007,
    },
    BuiltinType {
        oid: 25,
        name: "text",
        category: "S",
        array_oid: 1_009,
    },
    BuiltinType {
        oid: 114,
        name: "json",
        category: "U",
        array_oid: 199,
    },
    BuiltinType {
        oid: 269,
        name: "table_am_handler",
        category: "P",
        array_oid: 0,
    },
    BuiltinType {
        oid: 325,
        name: "index_am_handler",
        category: "P",
        array_oid: 0,
    },
    BuiltinType {
        oid: 700,
        name: "float4",
        category: "N",
        array_oid: 1_021,
    },
    BuiltinType {
        oid: 701,
        name: "float8",
        category: "N",
        array_oid: 1_022,
    },
    BuiltinType {
        oid: 1_042,
        name: "bpchar",
        category: "S",
        array_oid: 1_014,
    },
    BuiltinType {
        oid: 1_043,
        name: "varchar",
        category: "S",
        array_oid: 1_015,
    },
    BuiltinType {
        oid: 1_082,
        name: "date",
        category: "D",
        array_oid: 1_182,
    },
    BuiltinType {
        oid: 1_083,
        name: "time",
        category: "D",
        array_oid: 1_183,
    },
    BuiltinType {
        oid: 1_114,
        name: "timestamp",
        category: "D",
        array_oid: 1_115,
    },
    BuiltinType {
        oid: 1_184,
        name: "timestamptz",
        category: "D",
        array_oid: 1_185,
    },
    BuiltinType {
        oid: 1_186,
        name: "interval",
        category: "T",
        array_oid: 1_187,
    },
    BuiltinType {
        oid: 1_700,
        name: "numeric",
        category: "N",
        array_oid: 1_231,
    },
    BuiltinType {
        oid: 2_950,
        name: "uuid",
        category: "U",
        array_oid: 2_951,
    },
    BuiltinType {
        oid: 3_802,
        name: "jsonb",
        category: "U",
        array_oid: 3_807,
    },
    BuiltinType {
        oid: 16_383,
        name: "vector",
        category: "U",
        array_oid: 0,
    },
];
