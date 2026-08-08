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
    PG_TRIGGER_TABLE_ID, PG_TYPE_TABLE_ID, PG_USER_TABLE_ID, PostgresOidObject, RoutineKind,
    SystemRelationDescriptor, UserDefinedTypeKind, ViewKind, system_relations,
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
                if let Some(table) = schema
                    .tables()
                    .find(|table| table.triggers().any(|trigger| trigger.id == trigger_id))
                {
                    return authorization.can_discover(schema_name, Some(table.name.as_str()));
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
            let return_oid = routine
                .return_type
                .as_ref()
                .map_or(Ok(2_278_i64), |data_type| {
                    scalar_type_oid(catalog, data_type, routine.return_declared_type)
                })?;
            let argument_oids = routine
                .arguments
                .iter()
                .map(|argument| {
                    scalar_type_oid(catalog, &argument.data_type, argument.declared_type)
                        .map(|oid| oid.to_string())
                })
                .collect::<ordadb_types::Result<Vec<_>>>()?
                .join(" ");
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
            let table_oid = object_oid(catalog, PostgresOidObject::Table(table.id))?;
            for trigger in table.triggers() {
                push_row(
                    rows,
                    Row::new(vec![
                        Value::Int64(object_oid(catalog, PostgresOidObject::Trigger(trigger.id))?),
                        Value::Text(trigger.name.as_str().to_owned()),
                        Value::Int64(table_oid),
                        Value::Text(if trigger.enabled { "O" } else { "D" }.to_owned()),
                        Value::Boolean(false),
                        Value::Int64(object_oid(
                            catalog,
                            PostgresOidObject::Routine(trigger.routine_id),
                        )?),
                    ]),
                )?;
            }
        }
    }
    Ok(())
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
                        mode: Some("IN"),
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
