use std::collections::BTreeMap;

use ordadb_types::{
    ColumnId, DatabaseId, DbError, Identifier, Result, ScalarType, SchemaId, TableId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewColumn {
    pub name: Identifier,
    pub data_type: ScalarType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
}

impl NewColumn {
    #[must_use]
    pub fn new(name: Identifier, data_type: ScalarType) -> Self {
        Self {
            name,
            data_type,
            nullable: true,
            primary_key: false,
            unique: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub id: ColumnId,
    pub name: Identifier,
    pub data_type: ScalarType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDefinition {
    pub id: TableId,
    pub schema_id: SchemaId,
    pub name: Identifier,
    columns: Vec<ColumnDefinition>,
}

impl TableDefinition {
    #[must_use]
    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }

    #[must_use]
    pub fn column(&self, name: &Identifier) -> Option<&ColumnDefinition> {
        self.columns.iter().find(|column| &column.name == name)
    }

    #[must_use]
    pub fn column_index(&self, name: &Identifier) -> Option<usize> {
        self.columns.iter().position(|column| &column.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub id: SchemaId,
    pub database_id: DatabaseId,
    pub name: Identifier,
    tables: BTreeMap<Identifier, TableDefinition>,
}

impl SchemaDefinition {
    pub fn tables(&self) -> impl Iterator<Item = &TableDefinition> {
        self.tables.values()
    }

    #[must_use]
    pub fn table(&self, name: &Identifier) -> Option<&TableDefinition> {
        self.tables.get(name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseDefinition {
    pub id: DatabaseId,
    pub name: Identifier,
    schemas: BTreeMap<Identifier, SchemaDefinition>,
}

impl DatabaseDefinition {
    pub fn schemas(&self) -> impl Iterator<Item = &SchemaDefinition> {
        self.schemas.values()
    }

    #[must_use]
    pub fn schema(&self, name: &Identifier) -> Option<&SchemaDefinition> {
        self.schemas.get(name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    database: DatabaseDefinition,
    next_schema_id: u64,
    next_table_id: u64,
    next_column_id: u64,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::bootstrap("ordadb")
    }
}

impl Catalog {
    #[must_use]
    pub fn bootstrap(database_name: impl Into<String>) -> Self {
        let database_id = DatabaseId::new(1);
        let public_schema = SchemaDefinition {
            id: SchemaId::new(1),
            database_id,
            name: Identifier::unquoted("public"),
            tables: BTreeMap::new(),
        };
        let mut schemas = BTreeMap::new();
        schemas.insert(public_schema.name.clone(), public_schema);

        Self {
            database: DatabaseDefinition {
                id: database_id,
                name: Identifier::unquoted(database_name),
                schemas,
            },
            next_schema_id: 2,
            next_table_id: 1,
            next_column_id: 1,
        }
    }

    #[must_use]
    pub const fn database(&self) -> &DatabaseDefinition {
        &self.database
    }

    #[must_use]
    pub fn schema(&self, name: &Identifier) -> Option<&SchemaDefinition> {
        self.database.schema(name)
    }

    pub fn create_schema(&mut self, name: Identifier) -> Result<SchemaId> {
        if self.database.schemas.contains_key(&name) {
            return Err(DbError::new(
                "42P06",
                format!("schema {name} already exists"),
            ));
        }

        let id = SchemaId::new(self.next_schema_id);
        self.next_schema_id += 1;
        self.database.schemas.insert(
            name.clone(),
            SchemaDefinition {
                id,
                database_id: self.database.id,
                name,
                tables: BTreeMap::new(),
            },
        );
        Ok(id)
    }

    pub fn create_table(
        &mut self,
        schema_name: &Identifier,
        table_name: Identifier,
        columns: Vec<NewColumn>,
    ) -> Result<TableId> {
        if columns.is_empty() {
            return Err(DbError::new(
                "42601",
                "a table must contain at least one column",
            ));
        }

        let schema =
            self.database.schemas.get_mut(schema_name).ok_or_else(|| {
                DbError::new("3F000", format!("schema {schema_name} does not exist"))
            })?;

        if schema.tables.contains_key(&table_name) {
            return Err(DbError::new(
                "42P07",
                format!("table {schema_name}.{table_name} already exists"),
            ));
        }

        let mut seen_columns = BTreeMap::<Identifier, ()>::new();
        let mut definitions = Vec::with_capacity(columns.len());
        for column in columns {
            if seen_columns.insert(column.name.clone(), ()).is_some() {
                return Err(DbError::new(
                    "42701",
                    format!("column {} specified more than once", column.name),
                ));
            }

            let id = ColumnId::new(self.next_column_id);
            self.next_column_id += 1;
            definitions.push(ColumnDefinition {
                id,
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable && !column.primary_key,
                primary_key: column.primary_key,
                unique: column.unique || column.primary_key,
            });
        }

        let table_id = TableId::new(self.next_table_id);
        self.next_table_id += 1;
        schema.tables.insert(
            table_name.clone(),
            TableDefinition {
                id: table_id,
                schema_id: schema.id,
                name: table_name,
                columns: definitions,
            },
        );
        Ok(table_id)
    }

    #[must_use]
    pub fn table(
        &self,
        schema_name: &Identifier,
        table_name: &Identifier,
    ) -> Option<&TableDefinition> {
        self.schema(schema_name)?.table(table_name)
    }

    #[must_use]
    pub fn table_by_id(&self, table_id: TableId) -> Option<&TableDefinition> {
        self.database
            .schemas()
            .flat_map(SchemaDefinition::tables)
            .find(|table| table.id == table_id)
    }
}

#[cfg(test)]
mod tests {
    use ordadb_types::{Identifier, ScalarType, SchemaId, TableId};

    use super::{Catalog, NewColumn};

    #[test]
    fn bootstraps_public_schema_with_deterministic_ids() {
        let catalog = Catalog::default();
        assert_eq!(catalog.database().id.get(), 1);
        assert_eq!(
            catalog
                .schema(&Identifier::unquoted("PUBLIC"))
                .expect("public schema")
                .id,
            SchemaId::new(1)
        );
    }

    #[test]
    fn creates_and_resolves_normalized_schema_and_table_names() {
        let mut catalog = Catalog::default();
        assert_eq!(
            catalog
                .create_schema(Identifier::unquoted("Analytics"))
                .expect("create schema"),
            SchemaId::new(2)
        );
        assert_eq!(
            catalog
                .create_table(
                    &Identifier::unquoted("ANALYTICS"),
                    Identifier::unquoted("Events"),
                    vec![NewColumn::new(
                        Identifier::unquoted("ID"),
                        ScalarType::Int64,
                    )],
                )
                .expect("create table"),
            TableId::new(1)
        );
        assert!(
            catalog
                .table(
                    &Identifier::unquoted("analytics"),
                    &Identifier::unquoted("events")
                )
                .is_some()
        );
    }

    #[test]
    fn rejects_duplicate_objects_and_columns() {
        let mut catalog = Catalog::default();
        let duplicate_schema = catalog
            .create_schema(Identifier::unquoted("PUBLIC"))
            .expect_err("duplicate schema");
        assert_eq!(duplicate_schema.sql_state, "42P06");

        let duplicate_column = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("items"),
                vec![
                    NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                    NewColumn::new(Identifier::unquoted("ID"), ScalarType::Int64),
                ],
            )
            .expect_err("duplicate column");
        assert_eq!(duplicate_column.sql_state, "42701");
    }

    #[test]
    fn primary_keys_are_not_nullable_and_are_unique() {
        let mut catalog = Catalog::default();
        let mut id = NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64);
        id.primary_key = true;
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("documents"),
                vec![id],
            )
            .expect("create table");

        let column = &catalog
            .table_by_id(table_id)
            .expect("table by id")
            .columns()[0];
        assert!(!column.nullable);
        assert!(column.unique);
    }
}
