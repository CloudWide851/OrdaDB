use std::collections::BTreeMap;
use std::sync::OnceLock;

use ordadb_types::{ColumnId, DatabaseId, Identifier, ScalarType, SchemaId, TableId};

use super::{ColumnDefinition, PostgresOid, SchemaDefinition, TableDefinition, TableStatistics};

pub const PG_CATALOG_SCHEMA_ID: SchemaId = SchemaId::new(u64::MAX);
pub const INFORMATION_SCHEMA_SCHEMA_ID: SchemaId = SchemaId::new(u64::MAX - 1);
pub const PG_CATALOG_SCHEMA_OID: PostgresOid = PostgresOid(11);
pub const INFORMATION_SCHEMA_SCHEMA_OID: PostgresOid = PostgresOid(12_099);

pub(crate) const FIRST_RESERVED_TABLE_ID: u64 = u64::MAX - 1_024;
pub(crate) const FIRST_RESERVED_COLUMN_ID: u64 = u64::MAX - 65_536;

const fn table_id(ordinal: u64) -> TableId {
    TableId::new(FIRST_RESERVED_TABLE_ID + ordinal)
}

const fn column_id(relation_ordinal: u64, column_ordinal: u64) -> ColumnId {
    ColumnId::new(FIRST_RESERVED_COLUMN_ID + relation_ordinal * 128 + column_ordinal)
}

pub const PG_DATABASE_TABLE_ID: TableId = table_id(1);
pub const PG_NAMESPACE_TABLE_ID: TableId = table_id(2);
pub const PG_CLASS_TABLE_ID: TableId = table_id(3);
pub const PG_ATTRIBUTE_TABLE_ID: TableId = table_id(4);
pub const PG_TYPE_TABLE_ID: TableId = table_id(5);
pub const PG_ENUM_TABLE_ID: TableId = table_id(6);
pub const PG_INDEX_TABLE_ID: TableId = table_id(7);
pub const PG_CONSTRAINT_TABLE_ID: TableId = table_id(8);
pub const PG_PROC_TABLE_ID: TableId = table_id(9);
pub const PG_TRIGGER_TABLE_ID: TableId = table_id(10);
pub const PG_ROLES_TABLE_ID: TableId = table_id(11);
pub const PG_USER_TABLE_ID: TableId = table_id(12);
pub const PG_SETTINGS_TABLE_ID: TableId = table_id(13);
pub const INFORMATION_SCHEMA_SCHEMATA_TABLE_ID: TableId = table_id(14);
pub const INFORMATION_SCHEMA_TABLES_TABLE_ID: TableId = table_id(15);
pub const INFORMATION_SCHEMA_COLUMNS_TABLE_ID: TableId = table_id(16);
pub const PG_AM_TABLE_ID: TableId = table_id(17);
pub const PG_COLLATION_TABLE_ID: TableId = table_id(18);
pub const PG_DESCRIPTION_TABLE_ID: TableId = table_id(19);
pub const PG_DEPEND_TABLE_ID: TableId = table_id(20);
pub const PG_INHERITS_TABLE_ID: TableId = table_id(21);
pub const INFORMATION_SCHEMA_VIEWS_TABLE_ID: TableId = table_id(22);
pub const INFORMATION_SCHEMA_SEQUENCES_TABLE_ID: TableId = table_id(23);
pub const INFORMATION_SCHEMA_TABLE_CONSTRAINTS_TABLE_ID: TableId = table_id(24);
pub const INFORMATION_SCHEMA_KEY_COLUMN_USAGE_TABLE_ID: TableId = table_id(25);
pub const INFORMATION_SCHEMA_ROUTINES_TABLE_ID: TableId = table_id(26);
pub const INFORMATION_SCHEMA_PARAMETERS_TABLE_ID: TableId = table_id(27);

#[derive(Debug, PartialEq, Eq)]
pub struct SystemColumnDescriptor {
    pub id: ColumnId,
    pub name: &'static str,
    pub data_type: ScalarType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemRelationDescriptor {
    pub schema_id: SchemaId,
    pub schema_oid: PostgresOid,
    pub schema: &'static str,
    pub table_id: TableId,
    pub oid: PostgresOid,
    pub name: &'static str,
    pub columns: &'static [SystemColumnDescriptor],
}

macro_rules! column {
    ($relation:literal, $ordinal:literal, $name:literal, $data_type:expr, $nullable:literal) => {
        SystemColumnDescriptor {
            id: column_id($relation, $ordinal),
            name: $name,
            data_type: $data_type,
            nullable: $nullable,
        }
    };
}

static SYSTEM_RELATIONS: &[SystemRelationDescriptor] = &[
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_DATABASE_TABLE_ID,
        oid: PostgresOid(1_262),
        name: "pg_database",
        columns: &[
            column!(1, 1, "oid", ScalarType::Oid, false),
            column!(1, 2, "datname", ScalarType::Name, false),
            column!(1, 3, "datdba", ScalarType::Oid, false),
            column!(1, 4, "encoding", ScalarType::Int32, false),
            column!(1, 5, "datcollate", ScalarType::Text, false),
            column!(1, 6, "datctype", ScalarType::Text, false),
            column!(1, 7, "datistemplate", ScalarType::Boolean, false),
            column!(1, 8, "datallowconn", ScalarType::Boolean, false),
            column!(1, 9, "datconnlimit", ScalarType::Int32, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_NAMESPACE_TABLE_ID,
        oid: PostgresOid(2_615),
        name: "pg_namespace",
        columns: &[
            column!(2, 1, "oid", ScalarType::Oid, false),
            column!(2, 2, "nspname", ScalarType::Name, false),
            column!(2, 3, "nspowner", ScalarType::Oid, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_CLASS_TABLE_ID,
        oid: PostgresOid(1_259),
        name: "pg_class",
        columns: &[
            column!(3, 1, "oid", ScalarType::Oid, false),
            column!(3, 2, "relname", ScalarType::Name, false),
            column!(3, 3, "relnamespace", ScalarType::Oid, false),
            column!(3, 4, "relkind", ScalarType::InternalChar, false),
            column!(3, 5, "relpersistence", ScalarType::Text, false),
            column!(3, 6, "relnatts", ScalarType::Int32, false),
            column!(3, 7, "relhasindex", ScalarType::Boolean, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_ATTRIBUTE_TABLE_ID,
        oid: PostgresOid(1_249),
        name: "pg_attribute",
        columns: &[
            column!(4, 1, "attrelid", ScalarType::Oid, false),
            column!(4, 2, "attname", ScalarType::Name, false),
            column!(4, 3, "atttypid", ScalarType::Oid, false),
            column!(4, 4, "attnum", ScalarType::Int16, false),
            column!(4, 5, "attnotnull", ScalarType::Boolean, false),
            column!(4, 6, "atthasdef", ScalarType::Boolean, false),
            column!(4, 7, "attisdropped", ScalarType::Boolean, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_TYPE_TABLE_ID,
        oid: PostgresOid(1_247),
        name: "pg_type",
        columns: &[
            column!(5, 1, "oid", ScalarType::Oid, false),
            column!(5, 2, "typname", ScalarType::Name, false),
            column!(5, 3, "typnamespace", ScalarType::Oid, false),
            column!(5, 4, "typtype", ScalarType::InternalChar, false),
            column!(5, 5, "typcategory", ScalarType::InternalChar, false),
            column!(5, 6, "typisdefined", ScalarType::Boolean, false),
            column!(5, 7, "typelem", ScalarType::Oid, false),
            column!(5, 8, "typarray", ScalarType::Oid, false),
            column!(5, 9, "typbasetype", ScalarType::Oid, false),
            column!(5, 10, "typnotnull", ScalarType::Boolean, false),
            column!(5, 11, "typdefault", ScalarType::Text, true),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_ENUM_TABLE_ID,
        oid: PostgresOid(3_501),
        name: "pg_enum",
        columns: &[
            column!(6, 1, "oid", ScalarType::Oid, false),
            column!(6, 2, "enumtypid", ScalarType::Oid, false),
            column!(6, 3, "enumsortorder", ScalarType::Float64, false),
            column!(6, 4, "enumlabel", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_INDEX_TABLE_ID,
        oid: PostgresOid(2_610),
        name: "pg_index",
        columns: &[
            column!(7, 1, "indexrelid", ScalarType::Oid, false),
            column!(7, 2, "indrelid", ScalarType::Oid, false),
            column!(7, 3, "indisunique", ScalarType::Boolean, false),
            column!(7, 4, "indisprimary", ScalarType::Boolean, false),
            column!(7, 5, "indkey", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_CONSTRAINT_TABLE_ID,
        oid: PostgresOid(2_606),
        name: "pg_constraint",
        columns: &[
            column!(8, 1, "oid", ScalarType::Oid, false),
            column!(8, 2, "conname", ScalarType::Name, false),
            column!(8, 3, "contype", ScalarType::InternalChar, false),
            column!(8, 4, "connamespace", ScalarType::Oid, false),
            column!(8, 5, "conrelid", ScalarType::Oid, false),
            column!(8, 6, "contypid", ScalarType::Oid, false),
            column!(8, 7, "convalidated", ScalarType::Boolean, false),
            column!(8, 8, "conbin", ScalarType::Text, true),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_PROC_TABLE_ID,
        oid: PostgresOid(1_255),
        name: "pg_proc",
        columns: &[
            column!(9, 1, "oid", ScalarType::Oid, false),
            column!(9, 2, "proname", ScalarType::Name, false),
            column!(9, 3, "pronamespace", ScalarType::Oid, false),
            column!(9, 4, "prokind", ScalarType::InternalChar, false),
            column!(9, 5, "prorettype", ScalarType::Oid, false),
            column!(9, 6, "proretset", ScalarType::Boolean, false),
            column!(9, 7, "proargtypes", ScalarType::Text, false),
            column!(9, 8, "prolang", ScalarType::Text, false),
            column!(9, 9, "proallargtypes", ScalarType::Text, true),
            column!(9, 10, "proargmodes", ScalarType::Text, true),
            column!(9, 11, "proargnames", ScalarType::Text, true),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_TRIGGER_TABLE_ID,
        oid: PostgresOid(2_620),
        name: "pg_trigger",
        columns: &[
            column!(10, 1, "oid", ScalarType::Oid, false),
            column!(10, 2, "tgname", ScalarType::Name, false),
            column!(10, 3, "tgrelid", ScalarType::Oid, false),
            column!(10, 4, "tgenabled", ScalarType::InternalChar, false),
            column!(10, 5, "tgisinternal", ScalarType::Boolean, false),
            column!(10, 6, "tgfoid", ScalarType::Oid, false),
            column!(10, 7, "tgtype", ScalarType::Int16, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_ROLES_TABLE_ID,
        oid: PostgresOid(12_000),
        name: "pg_roles",
        columns: &[
            column!(11, 1, "rolname", ScalarType::Name, false),
            column!(11, 2, "rolsuper", ScalarType::Boolean, false),
            column!(11, 3, "rolinherit", ScalarType::Boolean, false),
            column!(11, 4, "rolcreaterole", ScalarType::Boolean, false),
            column!(11, 5, "rolcreatedb", ScalarType::Boolean, false),
            column!(11, 6, "rolcanlogin", ScalarType::Boolean, false),
            column!(11, 7, "rolreplication", ScalarType::Boolean, false),
            column!(11, 8, "rolconnlimit", ScalarType::Int32, false),
            column!(11, 9, "rolpassword", ScalarType::Text, true),
            column!(
                11,
                10,
                "rolvaliduntil",
                ScalarType::Timestamp {
                    with_timezone: true
                },
                true
            ),
            column!(11, 11, "rolbypassrls", ScalarType::Boolean, false),
            column!(11, 12, "rolconfig", ScalarType::Text, true),
            column!(11, 13, "oid", ScalarType::Oid, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_USER_TABLE_ID,
        oid: PostgresOid(12_001),
        name: "pg_user",
        columns: &[
            column!(12, 1, "usename", ScalarType::Name, false),
            column!(12, 2, "usesysid", ScalarType::Oid, false),
            column!(12, 3, "usecreatedb", ScalarType::Boolean, false),
            column!(12, 4, "usesuper", ScalarType::Boolean, false),
            column!(12, 5, "userepl", ScalarType::Boolean, false),
            column!(12, 6, "usebypassrls", ScalarType::Boolean, false),
            column!(12, 7, "passwd", ScalarType::Text, true),
            column!(
                12,
                8,
                "valuntil",
                ScalarType::Timestamp {
                    with_timezone: true
                },
                true
            ),
            column!(12, 9, "useconfig", ScalarType::Text, true),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_SETTINGS_TABLE_ID,
        oid: PostgresOid(12_002),
        name: "pg_settings",
        columns: &[
            column!(13, 1, "name", ScalarType::Text, false),
            column!(13, 2, "setting", ScalarType::Text, false),
            column!(13, 3, "unit", ScalarType::Text, true),
            column!(13, 4, "category", ScalarType::Text, false),
            column!(13, 5, "short_desc", ScalarType::Text, false),
            column!(13, 6, "context", ScalarType::Text, false),
            column!(13, 7, "vartype", ScalarType::Text, false),
            column!(13, 8, "source", ScalarType::Text, false),
            column!(13, 9, "min_val", ScalarType::Text, true),
            column!(13, 10, "max_val", ScalarType::Text, true),
            column!(13, 11, "enumvals", ScalarType::Text, true),
            column!(13, 12, "boot_val", ScalarType::Text, false),
            column!(13, 13, "reset_val", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: INFORMATION_SCHEMA_SCHEMA_ID,
        schema_oid: INFORMATION_SCHEMA_SCHEMA_OID,
        schema: "information_schema",
        table_id: INFORMATION_SCHEMA_SCHEMATA_TABLE_ID,
        oid: PostgresOid(12_100),
        name: "schemata",
        columns: &[
            column!(14, 1, "catalog_name", ScalarType::Text, false),
            column!(14, 2, "schema_name", ScalarType::Text, false),
            column!(14, 3, "schema_owner", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: INFORMATION_SCHEMA_SCHEMA_ID,
        schema_oid: INFORMATION_SCHEMA_SCHEMA_OID,
        schema: "information_schema",
        table_id: INFORMATION_SCHEMA_TABLES_TABLE_ID,
        oid: PostgresOid(12_101),
        name: "tables",
        columns: &[
            column!(15, 1, "table_catalog", ScalarType::Text, false),
            column!(15, 2, "table_schema", ScalarType::Text, false),
            column!(15, 3, "table_name", ScalarType::Text, false),
            column!(15, 4, "table_type", ScalarType::Text, false),
            column!(15, 5, "is_insertable_into", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: INFORMATION_SCHEMA_SCHEMA_ID,
        schema_oid: INFORMATION_SCHEMA_SCHEMA_OID,
        schema: "information_schema",
        table_id: INFORMATION_SCHEMA_COLUMNS_TABLE_ID,
        oid: PostgresOid(12_102),
        name: "columns",
        columns: &[
            column!(16, 1, "table_catalog", ScalarType::Text, false),
            column!(16, 2, "table_schema", ScalarType::Text, false),
            column!(16, 3, "table_name", ScalarType::Text, false),
            column!(16, 4, "column_name", ScalarType::Text, false),
            column!(16, 5, "ordinal_position", ScalarType::Int32, false),
            column!(16, 6, "column_default", ScalarType::Text, true),
            column!(16, 7, "is_nullable", ScalarType::Text, false),
            column!(16, 8, "data_type", ScalarType::Text, false),
            column!(16, 9, "character_maximum_length", ScalarType::Int64, true),
            column!(16, 10, "numeric_precision", ScalarType::Int32, true),
            column!(16, 11, "numeric_scale", ScalarType::Int32, true),
            column!(16, 12, "datetime_precision", ScalarType::Int32, true),
            column!(16, 13, "udt_catalog", ScalarType::Text, false),
            column!(16, 14, "udt_schema", ScalarType::Text, false),
            column!(16, 15, "udt_name", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_AM_TABLE_ID,
        oid: PostgresOid(2_601),
        name: "pg_am",
        columns: &[
            column!(17, 1, "oid", ScalarType::Oid, false),
            column!(17, 2, "amname", ScalarType::Name, false),
            column!(17, 3, "amhandler", ScalarType::Oid, false),
            column!(17, 4, "amtype", ScalarType::InternalChar, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_COLLATION_TABLE_ID,
        oid: PostgresOid(3_456),
        name: "pg_collation",
        columns: &[
            column!(18, 1, "oid", ScalarType::Oid, false),
            column!(18, 2, "collname", ScalarType::Name, false),
            column!(18, 3, "collnamespace", ScalarType::Oid, false),
            column!(18, 4, "collowner", ScalarType::Oid, false),
            column!(18, 5, "collprovider", ScalarType::InternalChar, false),
            column!(18, 6, "collisdeterministic", ScalarType::Boolean, false),
            column!(18, 7, "collencoding", ScalarType::Int32, false),
            column!(18, 8, "collcollate", ScalarType::Text, true),
            column!(18, 9, "collctype", ScalarType::Text, true),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_DESCRIPTION_TABLE_ID,
        oid: PostgresOid(2_609),
        name: "pg_description",
        columns: &[
            column!(19, 1, "objoid", ScalarType::Oid, false),
            column!(19, 2, "classoid", ScalarType::Oid, false),
            column!(19, 3, "objsubid", ScalarType::Int32, false),
            column!(19, 4, "description", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_DEPEND_TABLE_ID,
        oid: PostgresOid(2_608),
        name: "pg_depend",
        columns: &[
            column!(20, 1, "classid", ScalarType::Oid, false),
            column!(20, 2, "objid", ScalarType::Oid, false),
            column!(20, 3, "objsubid", ScalarType::Int32, false),
            column!(20, 4, "refclassid", ScalarType::Oid, false),
            column!(20, 5, "refobjid", ScalarType::Oid, false),
            column!(20, 6, "refobjsubid", ScalarType::Int32, false),
            column!(20, 7, "deptype", ScalarType::InternalChar, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: PG_CATALOG_SCHEMA_ID,
        schema_oid: PG_CATALOG_SCHEMA_OID,
        schema: "pg_catalog",
        table_id: PG_INHERITS_TABLE_ID,
        oid: PostgresOid(2_611),
        name: "pg_inherits",
        columns: &[
            column!(21, 1, "inhrelid", ScalarType::Oid, false),
            column!(21, 2, "inhparent", ScalarType::Oid, false),
            column!(21, 3, "inhseqno", ScalarType::Int32, false),
            column!(21, 4, "inhdetachpending", ScalarType::Boolean, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: INFORMATION_SCHEMA_SCHEMA_ID,
        schema_oid: INFORMATION_SCHEMA_SCHEMA_OID,
        schema: "information_schema",
        table_id: INFORMATION_SCHEMA_VIEWS_TABLE_ID,
        oid: PostgresOid(12_103),
        name: "views",
        columns: &[
            column!(22, 1, "table_catalog", ScalarType::Text, false),
            column!(22, 2, "table_schema", ScalarType::Text, false),
            column!(22, 3, "table_name", ScalarType::Text, false),
            column!(22, 4, "view_definition", ScalarType::Text, true),
            column!(22, 5, "check_option", ScalarType::Text, false),
            column!(22, 6, "is_updatable", ScalarType::Text, false),
            column!(22, 7, "is_insertable_into", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: INFORMATION_SCHEMA_SCHEMA_ID,
        schema_oid: INFORMATION_SCHEMA_SCHEMA_OID,
        schema: "information_schema",
        table_id: INFORMATION_SCHEMA_SEQUENCES_TABLE_ID,
        oid: PostgresOid(12_104),
        name: "sequences",
        columns: &[
            column!(23, 1, "sequence_catalog", ScalarType::Text, false),
            column!(23, 2, "sequence_schema", ScalarType::Text, false),
            column!(23, 3, "sequence_name", ScalarType::Text, false),
            column!(23, 4, "data_type", ScalarType::Text, false),
            column!(23, 5, "numeric_precision", ScalarType::Int32, true),
            column!(23, 6, "numeric_scale", ScalarType::Int32, true),
            column!(23, 7, "start_value", ScalarType::Text, false),
            column!(23, 8, "minimum_value", ScalarType::Text, false),
            column!(23, 9, "maximum_value", ScalarType::Text, false),
            column!(23, 10, "increment", ScalarType::Text, false),
            column!(23, 11, "cycle_option", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: INFORMATION_SCHEMA_SCHEMA_ID,
        schema_oid: INFORMATION_SCHEMA_SCHEMA_OID,
        schema: "information_schema",
        table_id: INFORMATION_SCHEMA_TABLE_CONSTRAINTS_TABLE_ID,
        oid: PostgresOid(12_105),
        name: "table_constraints",
        columns: &[
            column!(24, 1, "constraint_catalog", ScalarType::Text, false),
            column!(24, 2, "constraint_schema", ScalarType::Text, false),
            column!(24, 3, "constraint_name", ScalarType::Text, false),
            column!(24, 4, "table_catalog", ScalarType::Text, false),
            column!(24, 5, "table_schema", ScalarType::Text, false),
            column!(24, 6, "table_name", ScalarType::Text, false),
            column!(24, 7, "constraint_type", ScalarType::Text, false),
            column!(24, 8, "is_deferrable", ScalarType::Text, false),
            column!(24, 9, "initially_deferred", ScalarType::Text, false),
            column!(24, 10, "enforced", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: INFORMATION_SCHEMA_SCHEMA_ID,
        schema_oid: INFORMATION_SCHEMA_SCHEMA_OID,
        schema: "information_schema",
        table_id: INFORMATION_SCHEMA_KEY_COLUMN_USAGE_TABLE_ID,
        oid: PostgresOid(12_106),
        name: "key_column_usage",
        columns: &[
            column!(25, 1, "constraint_catalog", ScalarType::Text, false),
            column!(25, 2, "constraint_schema", ScalarType::Text, false),
            column!(25, 3, "constraint_name", ScalarType::Text, false),
            column!(25, 4, "table_catalog", ScalarType::Text, false),
            column!(25, 5, "table_schema", ScalarType::Text, false),
            column!(25, 6, "table_name", ScalarType::Text, false),
            column!(25, 7, "column_name", ScalarType::Text, false),
            column!(25, 8, "ordinal_position", ScalarType::Int32, false),
            column!(
                25,
                9,
                "position_in_unique_constraint",
                ScalarType::Int32,
                true
            ),
        ],
    },
    SystemRelationDescriptor {
        schema_id: INFORMATION_SCHEMA_SCHEMA_ID,
        schema_oid: INFORMATION_SCHEMA_SCHEMA_OID,
        schema: "information_schema",
        table_id: INFORMATION_SCHEMA_ROUTINES_TABLE_ID,
        oid: PostgresOid(12_107),
        name: "routines",
        columns: &[
            column!(26, 1, "specific_catalog", ScalarType::Text, false),
            column!(26, 2, "specific_schema", ScalarType::Text, false),
            column!(26, 3, "specific_name", ScalarType::Text, false),
            column!(26, 4, "routine_catalog", ScalarType::Text, false),
            column!(26, 5, "routine_schema", ScalarType::Text, false),
            column!(26, 6, "routine_name", ScalarType::Text, false),
            column!(26, 7, "routine_type", ScalarType::Text, false),
            column!(26, 8, "data_type", ScalarType::Text, true),
            column!(26, 9, "routine_definition", ScalarType::Text, true),
            column!(26, 10, "external_language", ScalarType::Text, false),
        ],
    },
    SystemRelationDescriptor {
        schema_id: INFORMATION_SCHEMA_SCHEMA_ID,
        schema_oid: INFORMATION_SCHEMA_SCHEMA_OID,
        schema: "information_schema",
        table_id: INFORMATION_SCHEMA_PARAMETERS_TABLE_ID,
        oid: PostgresOid(12_108),
        name: "parameters",
        columns: &[
            column!(27, 1, "specific_catalog", ScalarType::Text, false),
            column!(27, 2, "specific_schema", ScalarType::Text, false),
            column!(27, 3, "specific_name", ScalarType::Text, false),
            column!(27, 4, "ordinal_position", ScalarType::Int32, false),
            column!(27, 5, "parameter_mode", ScalarType::Text, true),
            column!(27, 6, "parameter_name", ScalarType::Text, true),
            column!(27, 7, "data_type", ScalarType::Text, false),
            column!(27, 8, "udt_catalog", ScalarType::Text, false),
            column!(27, 9, "udt_schema", ScalarType::Text, false),
            column!(27, 10, "udt_name", ScalarType::Text, false),
        ],
    },
];

#[must_use]
pub const fn system_relations() -> &'static [SystemRelationDescriptor] {
    SYSTEM_RELATIONS
}

#[must_use]
pub fn system_relation(table_id: TableId) -> Option<&'static SystemRelationDescriptor> {
    SYSTEM_RELATIONS
        .iter()
        .find(|relation| relation.table_id == table_id)
}

#[must_use]
pub fn system_relation_by_name(
    schema: &Identifier,
    name: &Identifier,
) -> Option<&'static SystemRelationDescriptor> {
    SYSTEM_RELATIONS
        .iter()
        .find(|relation| relation.schema == schema.as_str() && relation.name == name.as_str())
}

#[must_use]
pub(crate) fn is_system_schema_name(name: &Identifier) -> bool {
    matches!(name.as_str(), "pg_catalog" | "information_schema")
}

#[must_use]
pub(crate) fn is_system_schema_id(schema_id: SchemaId) -> bool {
    matches!(
        schema_id,
        PG_CATALOG_SCHEMA_ID | INFORMATION_SCHEMA_SCHEMA_ID
    )
}

fn build_schema(schema_id: SchemaId, name: &'static str) -> SchemaDefinition {
    let tables = SYSTEM_RELATIONS
        .iter()
        .filter(|relation| relation.schema_id == schema_id)
        .map(|relation| {
            let table_name = Identifier::unquoted(relation.name);
            let columns = relation
                .columns
                .iter()
                .map(|column| ColumnDefinition {
                    id: column.id,
                    name: Identifier::unquoted(column.name),
                    data_type: column.data_type.clone(),
                    declared_type: None,
                    nullable: column.nullable,
                    primary_key: false,
                    unique: false,
                    default: None,
                })
                .collect();
            (
                table_name.clone(),
                TableDefinition {
                    id: relation.table_id,
                    schema_id,
                    name: table_name,
                    columns,
                    indexes: BTreeMap::new(),
                    constraints: BTreeMap::new(),
                    triggers: BTreeMap::new(),
                    statistics: TableStatistics::default(),
                },
            )
        })
        .collect();
    SchemaDefinition {
        id: schema_id,
        database_id: DatabaseId::new(1),
        name: Identifier::unquoted(name),
        tables,
        sequences: BTreeMap::new(),
        views: BTreeMap::new(),
        routines: BTreeMap::new(),
        types: BTreeMap::new(),
    }
}

fn schemas() -> &'static [SchemaDefinition; 2] {
    static SCHEMAS: OnceLock<[SchemaDefinition; 2]> = OnceLock::new();
    SCHEMAS.get_or_init(|| {
        [
            build_schema(PG_CATALOG_SCHEMA_ID, "pg_catalog"),
            build_schema(INFORMATION_SCHEMA_SCHEMA_ID, "information_schema"),
        ]
    })
}

#[must_use]
pub(crate) fn system_schema(name: &Identifier) -> Option<&'static SchemaDefinition> {
    schemas()
        .iter()
        .find(|schema| schema.name.as_str() == name.as_str())
}

#[must_use]
pub(crate) fn system_schema_by_id(schema_id: SchemaId) -> Option<&'static SchemaDefinition> {
    schemas().iter().find(|schema| schema.id == schema_id)
}

#[must_use]
pub(crate) fn system_table(table_id: TableId) -> Option<&'static TableDefinition> {
    schemas()
        .iter()
        .flat_map(SchemaDefinition::tables)
        .find(|table| table.id == table_id)
}
