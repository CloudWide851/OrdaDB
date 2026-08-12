impl Catalog {
    pub fn drop_schema(
        &mut self,
        schema_id: SchemaId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        self.ensure_writable_schema_id(schema_id)?;
        let schema = self
            .schema_by_id(schema_id)
            .ok_or_else(|| DbError::new("3F000", "schema does not exist"))?;
        let is_empty = schema.tables.is_empty()
            && schema.sequences.is_empty()
            && schema.views.is_empty()
            && schema.routines.is_empty()
            && schema.types.is_empty();
        if behavior == DropBehavior::Restrict && !is_empty {
            return Err(
                DbError::new("2BP01", "cannot drop schema because it contains objects")
                    .with_hint("Use DROP SCHEMA ... CASCADE to remove contained objects."),
            );
        }

        let mut roots = schema
            .tables()
            .map(|table| CatalogObjectRef::Table(table.id))
            .chain(
                schema
                    .sequences()
                    .map(|sequence| CatalogObjectRef::Sequence(sequence.id)),
            )
            .chain(schema.views().map(|view| CatalogObjectRef::View(view.id)))
            .chain(
                schema
                    .routines()
                    .map(|routine| CatalogObjectRef::Routine(routine.id)),
            )
            .chain(
                schema
                    .types()
                    .map(|definition| CatalogObjectRef::Type(definition.id)),
            )
            .collect::<Vec<_>>();
        roots.sort();
        let mut removed = Vec::new();
        for root in roots {
            for object in self.drop_catalog_object(root, DropBehavior::Cascade)? {
                if !removed.contains(&object) {
                    removed.push(object);
                }
            }
        }
        self.remove_catalog_object(CatalogObjectRef::Schema(schema_id))?;
        self.validate_postgres_oid_registry()?;
        removed.push(CatalogObjectRef::Schema(schema_id));
        Ok(removed)
    }

    pub fn create_table(
        &mut self,
        schema_name: &Identifier,
        table_name: Identifier,
        columns: Vec<NewColumn>,
    ) -> Result<TableId> {
        ensure_writable_schema_name(schema_name)?;
        if columns.is_empty() {
            return Err(DbError::new(
                "42601",
                "a table must contain at least one column",
            ));
        }
        if let Some(type_id) = columns
            .iter()
            .filter_map(|column| column.declared_type)
            .find(|type_id| self.type_by_id(*type_id).is_none())
        {
            return Err(DbError::new(
                "42704",
                format!("declared type {} does not exist", type_id.get()),
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
                declared_type: column.declared_type,
                nullable: column.nullable && !column.primary_key,
                primary_key: column.primary_key,
                unique: column.unique || column.primary_key,
                default: column.default,
            });
        }

        let table_id = TableId::new(self.next_table_id);
        self.next_table_id += 1;
        let mut indexes = BTreeMap::new();
        for column in &definitions {
            if column.unique {
                let suffix = if column.primary_key { "pkey" } else { "key" };
                let name = Identifier::unquoted(format!(
                    "{}_{}_{}",
                    table_name.as_str(),
                    column.name.as_str(),
                    suffix
                ));
                let id = IndexId::new(self.next_index_id);
                self.next_index_id += 1;
                indexes.insert(
                    name.clone(),
                    IndexDefinition {
                        id,
                        table_id,
                        name,
                        key_columns: vec![column.id],
                        include_columns: Vec::new(),
                        unique: true,
                        primary: column.primary_key,
                        method: IndexMethod::BTree,
                        options: IndexOptions::BTree,
                    },
                );
            }
        }
        let mut oid_registry = self.postgres_oid_registry.clone();
        oid_registry.allocate(PostgresOidObject::Table(table_id))?;
        for column in &definitions {
            oid_registry.allocate(PostgresOidObject::Column(table_id, column.id))?;
        }
        for index in indexes.values() {
            oid_registry.allocate(PostgresOidObject::Index(index.id))?;
        }
        let declared_types = definitions
            .iter()
            .filter_map(|column| column.declared_type.map(|type_id| (column.id, type_id)))
            .collect::<Vec<_>>();
        schema.tables.insert(
            table_name.clone(),
            TableDefinition {
                id: table_id,
                schema_id: schema.id,
                name: table_name,
                columns: definitions,
                indexes,
                constraints: BTreeMap::new(),
                triggers: BTreeMap::new(),
                statistics: TableStatistics::default(),
            },
        );
        for (column_id, type_id) in declared_types {
            self.dependencies.add(
                CatalogObjectRef::Column(table_id, column_id),
                CatalogObjectRef::Type(type_id),
            )?;
        }
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(table_id)
    }

    #[must_use]
    pub fn table(
        &self,
        schema_name: &Identifier,
        table_name: &Identifier,
    ) -> Option<&TableDefinition> {
        system_relation_by_name(schema_name, table_name)
            .and_then(|relation| system::system_table(relation.table_id))
            .or_else(|| self.database.schema(schema_name)?.table(table_name))
    }

    #[must_use]
    pub fn table_by_id(&self, table_id: TableId) -> Option<&TableDefinition> {
        system::system_table(table_id).or_else(|| {
            self.database
                .schemas()
                .flat_map(SchemaDefinition::tables)
                .find(|table| table.id == table_id)
        })
    }

    pub fn rename_table(&mut self, table_id: TableId, new_name: Identifier) -> Result<()> {
        self.ensure_writable_table_id(table_id)?;
        let (schema_id, old_name) = self
            .table_by_id(table_id)
            .map(|table| (table.schema_id, table.name.clone()))
            .ok_or_else(|| DbError::new("42P01", "table does not exist"))?;
        let schema = self.schema_by_id(schema_id).ok_or_else(|| {
            DbError::internal("table owner schema disappeared during table rename")
        })?;
        if schema.relation_name_exists(&new_name) {
            return Err(DbError::new(
                "42P07",
                format!("relation {new_name} already exists"),
            ));
        }
        let schema = self.schema_by_id_mut(schema_id)?;
        let mut table = schema
            .tables
            .remove(&old_name)
            .ok_or_else(|| DbError::internal("table namespace changed during rename"))?;
        table.name = new_name.clone();
        schema.tables.insert(new_name, table);
        Ok(())
    }

    pub fn drop_table(
        &mut self,
        table_id: TableId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        self.ensure_writable_table_id(table_id)?;
        if self.table_by_id(table_id).is_none() {
            return Err(DbError::new("42P01", "table does not exist"));
        }
        let root = CatalogObjectRef::Table(table_id);
        if behavior == DropBehavior::Restrict {
            let external = self
                .dependencies
                .dependents(root)
                .filter(|object| !self.object_is_owned_by_table(*object, table_id))
                .collect::<Vec<_>>();
            if !external.is_empty() {
                return Err(DbError::new(
                    "2BP01",
                    "cannot drop table because other objects depend on it",
                )
                .with_detail(format!("dependents: {external:?}"))
                .with_hint("Use DROP TABLE ... CASCADE to remove dependent objects."));
            }
        }
        self.drop_catalog_object(root, DropBehavior::Cascade)
    }

    pub fn rename_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        new_name: Identifier,
    ) -> Result<()> {
        self.ensure_writable_table_id(table_id)?;
        let table = self.table_by_id_mut(table_id)?;
        if table.column(&new_name).is_some() {
            return Err(DbError::new(
                "42701",
                format!("column {new_name} already exists"),
            ));
        }
        let index = table
            .column_index_by_id(column_id)
            .ok_or_else(|| DbError::new("42703", "column does not exist"))?;
        table.columns[index].name = new_name;
        Ok(())
    }

    pub fn add_column(&mut self, table_id: TableId, column: NewColumn) -> Result<ColumnId> {
        self.ensure_writable_table_id(table_id)?;
        if let Some(type_id) = column.declared_type
            && self.type_by_id(type_id).is_none()
        {
            return Err(DbError::new(
                "42704",
                format!("declared type {} does not exist", type_id.get()),
            ));
        }
        if self
            .table_by_id(table_id)
            .ok_or_else(|| DbError::new("42P01", "table does not exist"))?
            .column(&column.name)
            .is_some()
        {
            return Err(DbError::new(
                "42701",
                format!("column {} already exists", column.name),
            ));
        }
        let column_id = ColumnId::new(self.next_column_id);
        let oid_registry =
            self.postgres_oid_candidate([PostgresOidObject::Column(table_id, column_id)])?;
        let next_column_id = self
            .next_column_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog column ID space is exhausted"))?;
        let declared_type = column.declared_type;
        let mut dependencies = self.dependencies.clone();
        if let Some(type_id) = declared_type {
            dependencies.add(
                CatalogObjectRef::Column(table_id, column_id),
                CatalogObjectRef::Type(type_id),
            )?;
        }
        self.table_by_id_mut(table_id)?
            .columns
            .push(ColumnDefinition {
                id: column_id,
                name: column.name,
                data_type: column.data_type,
                declared_type: column.declared_type,
                nullable: column.nullable && !column.primary_key,
                primary_key: column.primary_key,
                unique: column.unique || column.primary_key,
                default: column.default,
            });
        self.next_column_id = next_column_id;
        self.dependencies = dependencies;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(column_id)
    }

    pub fn alter_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        data_type: Option<ScalarType>,
        nullable: Option<bool>,
        default: Option<Option<CatalogExpression>>,
        declared_type: Option<Option<TypeId>>,
    ) -> Result<()> {
        self.ensure_writable_table_id(table_id)?;
        let mut dependencies = self.dependencies.clone();
        if let Some(declared_type) = declared_type {
            let object = CatalogObjectRef::Column(table_id, column_id);
            dependencies.remove_references(object);
            if let Some(type_id) = declared_type {
                if self.type_by_id(type_id).is_none() {
                    return Err(DbError::new("42704", "declared column type does not exist"));
                }
                dependencies.add(object, CatalogObjectRef::Type(type_id))?;
            }
        }
        let table = self.table_by_id_mut(table_id)?;
        let index = table
            .column_index_by_id(column_id)
            .ok_or_else(|| DbError::new("42703", "column does not exist"))?;
        let column = &mut table.columns[index];
        if let Some(data_type) = data_type {
            column.data_type = data_type;
        }
        if let Some(nullable) = nullable {
            if nullable && column.primary_key {
                return Err(DbError::new(
                    "42P16",
                    "primary-key columns cannot be nullable",
                ));
            }
            column.nullable = nullable;
        }
        if let Some(default) = default {
            column.default = default;
        }
        if let Some(declared_type) = declared_type {
            column.declared_type = declared_type;
        }
        self.dependencies = dependencies;
        Ok(())
    }

    pub fn drop_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        self.ensure_writable_table_id(table_id)?;
        let table = self
            .table_by_id(table_id)
            .ok_or_else(|| DbError::new("42P01", "table does not exist"))?;
        if table.column_index_by_id(column_id).is_none() {
            return Err(DbError::new("42703", "column does not exist"));
        }
        if table.columns.len() == 1 {
            return Err(DbError::new(
                "42601",
                "cannot drop the only column of a table",
            ));
        }
        let root = CatalogObjectRef::Column(table_id, column_id);
        self.drop_catalog_object(root, behavior)
    }

    #[must_use]
    pub fn index_by_id(&self, index_id: IndexId) -> Option<&IndexDefinition> {
        self.database
            .schemas()
            .flat_map(SchemaDefinition::tables)
            .flat_map(TableDefinition::indexes)
            .find(|index| index.id == index_id)
    }

    pub fn create_index(&mut self, table_id: TableId, new_index: NewIndex) -> Result<IndexId> {
        self.ensure_writable_table_id(table_id)?;
        if new_index.key_columns.is_empty() {
            return Err(DbError::new(
                "42601",
                "an index must contain at least one key column",
            ));
        }
        if new_index.method != new_index.options.method() {
            return Err(DbError::new(
                "22023",
                "index method and options do not describe the same index kind",
            ));
        }
        if self
            .database
            .schemas()
            .flat_map(SchemaDefinition::tables)
            .any(|table| table.index(&new_index.name).is_some())
        {
            return Err(DbError::new(
                "42P07",
                format!("relation {} already exists", new_index.name),
            ));
        }

        let table = self
            .table_by_id(table_id)
            .ok_or_else(|| DbError::new("42P01", "index owner table does not exist"))?;
        let mut seen = BTreeMap::<ColumnId, ()>::new();
        let key_columns = new_index
            .key_columns
            .iter()
            .map(|name| {
                let column = table.column(name).ok_or_else(|| {
                    DbError::new("42703", format!("column {name} does not exist"))
                })?;
                if seen.insert(column.id, ()).is_some() {
                    return Err(DbError::new(
                        "42701",
                        format!("column {name} specified more than once"),
                    ));
                }
                Ok(column.id)
            })
            .collect::<Result<Vec<_>>>()?;
        let include_columns = new_index
            .include_columns
            .iter()
            .map(|name| {
                let column = table.column(name).ok_or_else(|| {
                    DbError::new("42703", format!("column {name} does not exist"))
                })?;
                if seen.insert(column.id, ()).is_some() {
                    return Err(DbError::new(
                        "42701",
                        format!("column {name} specified more than once"),
                    ));
                }
                Ok(column.id)
            })
            .collect::<Result<Vec<_>>>()?;

        match (&new_index.method, &new_index.options) {
            (IndexMethod::BTree, IndexOptions::BTree) => {
                for name in &new_index.key_columns {
                    let column = table
                        .column(name)
                        .ok_or_else(|| DbError::internal("validated B+Tree column disappeared"))?;
                    if !indexable_type(&column.data_type) {
                        return Err(DbError::new(
                            "42804",
                            format!("column {name} has no B+Tree ordering"),
                        ));
                    }
                }
            }
            (IndexMethod::FullText, IndexOptions::FullText { .. }) => {
                if new_index.unique || !new_index.include_columns.is_empty() {
                    return Err(DbError::new(
                        "0A000",
                        "full-text indexes do not support UNIQUE or INCLUDE",
                    ));
                }
                for name in &new_index.key_columns {
                    let column = table.column(name).ok_or_else(|| {
                        DbError::internal("validated full-text column disappeared")
                    })?;
                    if !text_search_type(&column.data_type) {
                        return Err(DbError::new(
                            "42804",
                            format!("full-text index column {name} must be character or text"),
                        ));
                    }
                }
            }
            (
                IndexMethod::Hnsw,
                IndexOptions::Hnsw {
                    dimensions,
                    m,
                    ef_construction,
                    ef_search,
                    ..
                },
            ) => {
                if new_index.unique
                    || !new_index.include_columns.is_empty()
                    || new_index.key_columns.len() != 1
                {
                    return Err(DbError::new(
                        "0A000",
                        "HNSW indexes require one VECTOR column and do not support UNIQUE or INCLUDE",
                    ));
                }
                if !(2..=64).contains(m)
                    || *ef_construction < *m
                    || *ef_construction > 4_096
                    || !(1..=4_096).contains(ef_search)
                {
                    return Err(DbError::new(
                        "22023",
                        "HNSW options require m 2..64, ef_construction m..4096, and ef_search 1..4096",
                    ));
                }
                let name = new_index
                    .key_columns
                    .first()
                    .ok_or_else(|| DbError::internal("validated HNSW key disappeared"))?;
                let column = table
                    .column(name)
                    .ok_or_else(|| DbError::internal("validated HNSW column disappeared"))?;
                match column.data_type {
                    ScalarType::Vector {
                        dimensions: Some(column_dimensions),
                    } if column_dimensions == *dimensions && *dimensions > 0 => {}
                    ScalarType::Vector { dimensions: None } => {
                        return Err(DbError::new(
                            "42804",
                            format!("HNSW index column {name} requires a fixed VECTOR dimension"),
                        ));
                    }
                    ScalarType::Vector {
                        dimensions: Some(column_dimensions),
                    } => {
                        return Err(DbError::new(
                            "22023",
                            format!(
                                "HNSW dimensions {dimensions} do not match column dimension {column_dimensions}"
                            ),
                        ));
                    }
                    _ => {
                        return Err(DbError::new(
                            "42804",
                            format!("HNSW index column {name} must be VECTOR"),
                        ));
                    }
                }
            }
            _ => {
                return Err(DbError::new(
                    "22023",
                    "index method and options do not describe the same index kind",
                ));
            }
        }

        let id = IndexId::new(self.next_index_id);
        let oid_registry = self.postgres_oid_candidate([PostgresOidObject::Index(id)])?;
        let next_index_id = self
            .next_index_id
            .checked_add(1)
            .ok_or_else(|| DbError::new("54000", "catalog index ID space is exhausted"))?;
        let definition = IndexDefinition {
            id,
            table_id,
            name: new_index.name.clone(),
            key_columns,
            include_columns,
            unique: new_index.unique,
            primary: false,
            method: new_index.method,
            options: new_index.options,
        };
        self.table_by_id_mut(table_id)?
            .indexes
            .insert(new_index.name, definition);
        self.next_index_id = next_index_id;
        self.publish_postgres_oid_candidate(oid_registry)?;
        Ok(id)
    }

    pub fn rename_index(&mut self, index_id: IndexId, new_name: Identifier) -> Result<()> {
        let (table_id, old_name, schema_id) = self
            .index_by_id(index_id)
            .and_then(|index| {
                self.table_by_id(index.table_id)
                    .map(|table| (index.table_id, index.name.clone(), table.schema_id))
            })
            .ok_or_else(|| DbError::new("42704", "index does not exist"))?;
        if self
            .schema_by_id(schema_id)
            .is_some_and(|schema| schema.relation_name_exists(&new_name))
        {
            return Err(DbError::new(
                "42P07",
                format!("relation {new_name} already exists"),
            ));
        }
        let table = self.table_by_id_mut(table_id)?;
        let mut index = table
            .indexes
            .remove(&old_name)
            .ok_or_else(|| DbError::internal("index namespace changed during rename"))?;
        index.name = new_name.clone();
        table.indexes.insert(new_name, index);
        Ok(())
    }

    pub fn drop_index(
        &mut self,
        index_id: IndexId,
        behavior: DropBehavior,
    ) -> Result<Vec<CatalogObjectRef>> {
        if self.index_by_id(index_id).is_none() {
            return Err(DbError::new("42704", "index does not exist"));
        }
        self.drop_catalog_object(CatalogObjectRef::Index(index_id), behavior)
    }
}
