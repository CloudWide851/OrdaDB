
fn bind_index_options(
    method: IndexMethod,
    table: &TableDefinition,
    key_columns: &[ParsedIdentifier],
    include_columns: &[ParsedIdentifier],
    unique: bool,
    options: Vec<ParsedIndexOption>,
) -> Result<IndexOptions> {
    let mut options = collect_index_options(options)?;
    let bound = match method {
        IndexMethod::BTree => {
            reject_remaining_index_options(&options, "B-Tree")?;
            for column in key_columns {
                let definition = table
                    .column(&column.name)
                    .ok_or_else(|| DbError::internal("validated index column disappeared"))?;
                if !indexable_type(&definition.data_type) {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        format!("column {} has no B+Tree ordering", column.name),
                    )
                    .with_position_opt(column.position));
                }
            }
            IndexOptions::BTree
        }
        IndexMethod::FullText => {
            if unique || !include_columns.is_empty() {
                return unsupported("full-text indexes do not support UNIQUE or INCLUDE");
            }
            for column in key_columns {
                let definition = table
                    .column(&column.name)
                    .ok_or_else(|| DbError::internal("validated index column disappeared"))?;
                if !text_search_type(&definition.data_type) {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        format!(
                            "full-text index column {} must be character or text",
                            column.name
                        ),
                    )
                    .with_position_opt(column.position));
                }
            }
            let analyzer = match take_text_index_option(&mut options, "analyzer")? {
                None => FullTextAnalyzer::Standard,
                Some((value, _)) if value.eq_ignore_ascii_case("standard") => {
                    FullTextAnalyzer::Standard
                }
                Some((value, _)) if value.eq_ignore_ascii_case("whitespace") => {
                    FullTextAnalyzer::Whitespace
                }
                Some((value, position)) => {
                    return Err(DbError::new(
                        "22023",
                        format!("unsupported full-text analyzer {value}"),
                    )
                    .with_position_opt(position));
                }
            };
            reject_remaining_index_options(&options, "full-text")?;
            IndexOptions::FullText { analyzer }
        }
        IndexMethod::Hnsw => {
            if unique || !include_columns.is_empty() || key_columns.len() != 1 {
                return unsupported(
                    "HNSW indexes require one VECTOR column and do not support UNIQUE or INCLUDE",
                );
            }
            let column = key_columns
                .first()
                .ok_or_else(|| DbError::internal("validated HNSW column disappeared"))?;
            let definition = table
                .column(&column.name)
                .ok_or_else(|| DbError::internal("validated HNSW column disappeared"))?;
            let dimensions = match definition.data_type {
                ScalarType::Vector {
                    dimensions: Some(dimensions),
                } if dimensions > 0 => dimensions,
                ScalarType::Vector { dimensions: None } => {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        format!(
                            "HNSW index column {} requires a fixed VECTOR dimension",
                            column.name
                        ),
                    )
                    .with_position_opt(column.position));
                }
                _ => {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        format!("HNSW index column {} must be VECTOR", column.name),
                    )
                    .with_position_opt(column.position));
                }
            };
            let metric = match take_text_index_option(&mut options, "metric")? {
                None => VectorDistanceMetric::Cosine,
                Some((value, _)) if value.eq_ignore_ascii_case("cosine") => {
                    VectorDistanceMetric::Cosine
                }
                Some((value, _))
                    if value.eq_ignore_ascii_case("l2")
                        || value.eq_ignore_ascii_case("euclidean") =>
                {
                    VectorDistanceMetric::L2
                }
                Some((value, _)) if value.eq_ignore_ascii_case("dot") => VectorDistanceMetric::Dot,
                Some((value, position)) => {
                    return Err(DbError::new(
                        "22023",
                        format!("unsupported HNSW distance metric {value}"),
                    )
                    .with_position_opt(position));
                }
            };
            let m = take_integer_index_option(&mut options, "m")?.unwrap_or(16);
            let ef_construction =
                take_integer_index_option(&mut options, "ef_construction")?.unwrap_or(64);
            let ef_search = take_integer_index_option(&mut options, "ef_search")?.unwrap_or(40);
            reject_remaining_index_options(&options, "HNSW")?;
            if !(2..=64).contains(&m)
                || ef_construction < m
                || ef_construction > 4_096
                || !(1..=4_096).contains(&ef_search)
            {
                return Err(DbError::new(
                    "22023",
                    "HNSW options require m 2..64, ef_construction m..4096, and ef_search 1..4096",
                ));
            }
            IndexOptions::Hnsw {
                metric,
                dimensions,
                m,
                ef_construction,
                ef_search,
            }
        }
    };
    Ok(bound)
}

fn collect_index_options(
    options: Vec<ParsedIndexOption>,
) -> Result<BTreeMap<String, ParsedIndexOption>> {
    let mut collected = BTreeMap::new();
    for option in options {
        let name = option.name.name.as_str().to_owned();
        if collected.insert(name.clone(), option).is_some() {
            return Err(DbError::new(
                "42701",
                format!("index option {name} specified more than once"),
            ));
        }
    }
    Ok(collected)
}

fn take_text_index_option(
    options: &mut BTreeMap<String, ParsedIndexOption>,
    name: &str,
) -> Result<Option<(String, Option<usize>)>> {
    let Some(option) = options.remove(name) else {
        return Ok(None);
    };
    let position = option.name.position;
    match option.value {
        ParsedIndexOptionValue::Text(value) => Ok(Some((value, position))),
        ParsedIndexOptionValue::Integer(_) => Err(DbError::new(
            "22023",
            format!("index option {name} requires a string value"),
        )
        .with_position_opt(position)),
    }
}

fn take_integer_index_option(
    options: &mut BTreeMap<String, ParsedIndexOption>,
    name: &str,
) -> Result<Option<usize>> {
    let Some(option) = options.remove(name) else {
        return Ok(None);
    };
    match option.value {
        ParsedIndexOptionValue::Integer(value) => Ok(Some(value)),
        ParsedIndexOptionValue::Text(_) => Err(DbError::new(
            "22023",
            format!("index option {name} requires a non-negative integer"),
        )
        .with_position_opt(option.name.position)),
    }
}

fn reject_remaining_index_options(
    options: &BTreeMap<String, ParsedIndexOption>,
    method: &str,
) -> Result<()> {
    let Some((name, option)) = options.first_key_value() else {
        return Ok(());
    };
    unsupported_at(
        format!("{method} index option {name} is not supported"),
        option.name.position,
    )
}

fn resolve_index_relation<'a>(
    name: &ParsedObjectName,
    catalog: &'a Catalog,
) -> Result<&'a TableDefinition> {
    let (schema, relation, position) = split_table_name(name)?;
    if catalog.schema(&schema).is_none() {
        return Err(
            DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                .with_position_opt(position),
        );
    }
    if let Some(table) = catalog.table(&schema, &relation) {
        return Ok(table);
    }
    if let Some(view) = catalog.view(&schema, &relation) {
        if view.kind != ViewKind::Materialized {
            return Err(DbError::new(
                "42809",
                format!("cannot create an index on regular view {schema}.{relation}"),
            )
            .with_position_opt(position));
        }
        let table_id = view
            .materialized_table_id
            .ok_or_else(|| DbError::internal("materialized view is missing its backing table"))?;
        return catalog.table_by_id(table_id).ok_or_else(|| {
            DbError::internal("materialized view backing table is absent from the catalog")
        });
    }
    Err(DbError::new(
        UNDEFINED_TABLE,
        format!("relation {schema}.{relation} does not exist"),
    )
    .with_position_opt(position))
}

fn bind_drop_objects(
    kind: DdlObjectKind,
    names: Vec<ParsedObjectName>,
    if_exists: bool,
    behavior: DropBehavior,
    catalog: &Catalog,
) -> Result<BoundStatement> {
    let mut objects = Vec::with_capacity(names.len());
    for name in names {
        let found = match kind {
            DdlObjectKind::Schema => {
                let [name] = name.parts.as_slice() else {
                    return unsupported("qualified schema names are not supported");
                };
                catalog
                    .schema(&name.name)
                    .map(|schema| CatalogObjectRef::Schema(schema.id))
                    .ok_or_else(|| {
                        DbError::new(
                            UNDEFINED_SCHEMA,
                            format!("schema {} does not exist", name.name),
                        )
                        .with_position_opt(name.position)
                    })
            }
            DdlObjectKind::Table => {
                resolve_table(&name, catalog).map(|table| CatalogObjectRef::Table(table.id))
            }
            DdlObjectKind::Index => {
                let (schema, index, position) = split_table_name(&name)?;
                catalog
                    .index(&schema, &index)
                    .map(|index| CatalogObjectRef::Index(index.id))
                    .ok_or_else(|| {
                        DbError::new("42704", format!("index {schema}.{index} does not exist"))
                            .with_position_opt(position)
                    })
            }
            DdlObjectKind::Sequence => {
                let (schema, sequence, position) = split_table_name(&name)?;
                catalog
                    .sequence(&schema, &sequence)
                    .map(|sequence| CatalogObjectRef::Sequence(sequence.id))
                    .ok_or_else(|| {
                        DbError::new(
                            "42P01",
                            format!("sequence {schema}.{sequence} does not exist"),
                        )
                        .with_position_opt(position)
                    })
            }
            DdlObjectKind::View | DdlObjectKind::MaterializedView => {
                let (schema, view, position) = split_table_name(&name)?;
                catalog
                    .view(&schema, &view)
                    .filter(|view| {
                        (kind == DdlObjectKind::MaterializedView)
                            == (view.kind == ViewKind::Materialized)
                    })
                    .map(|view| CatalogObjectRef::View(view.id))
                    .ok_or_else(|| {
                        DbError::new("42P01", format!("view {schema}.{view} does not exist"))
                            .with_position_opt(position)
                    })
            }
            DdlObjectKind::Type => {
                let (schema, type_name, position) = split_table_name(&name)?;
                catalog
                    .user_defined_type(&schema, &type_name)
                    .map(|definition| CatalogObjectRef::Type(definition.id))
                    .ok_or_else(|| {
                        DbError::new("42704", format!("type {schema}.{type_name} does not exist"))
                            .with_position_opt(position)
                    })
            }
        };
        match found {
            Ok(object) => objects.push(object),
            Err(_) if if_exists => {}
            Err(error) => return Err(error),
        }
    }
    if objects.is_empty() {
        return Ok(BoundStatement::NoOp {
            tag: format!("DROP {}", ddl_object_label(kind)),
        });
    }
    Ok(BoundStatement::DropObjects {
        kind,
        objects,
        behavior,
    })
}

fn bind_alter_table(
    name: ParsedObjectName,
    if_exists: bool,
    operations: Vec<ParsedAlterTableOperation>,
    catalog: &Catalog,
) -> Result<BoundStatement> {
    let table = match resolve_table(&name, catalog) {
        Ok(table) => table.clone(),
        Err(_) if if_exists => {
            return Ok(BoundStatement::NoOp {
                tag: "ALTER TABLE".to_owned(),
            });
        }
        Err(error) => return Err(error),
    };
    let mut virtual_columns = table
        .columns()
        .iter()
        .map(|column| NewColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            declared_type: column.declared_type,
            nullable: column.nullable,
            primary_key: column.primary_key,
            unique: column.unique,
            default: column.default.clone(),
        })
        .collect::<Vec<_>>();
    let mut bound = Vec::with_capacity(operations.len());
    for (ordinal, operation) in operations.into_iter().enumerate() {
        match operation {
            ParsedAlterTableOperation::RenameTable { new_name } => {
                if catalog
                    .schema_by_id(table.schema_id)
                    .is_some_and(|schema| schema.relation_name_exists(&new_name.name))
                {
                    return Err(DbError::new(
                        "42P07",
                        format!("relation {} already exists", new_name.name),
                    )
                    .with_position_opt(new_name.position));
                }
                bound.push(BoundAlterTableOperation::RenameTable {
                    new_name: new_name.name,
                });
            }
            ParsedAlterTableOperation::RenameColumn { old_name, new_name } => {
                let column = table.column(&old_name.name).ok_or_else(|| {
                    DbError::new(
                        UNDEFINED_COLUMN,
                        format!("column {} does not exist", old_name.name),
                    )
                    .with_position_opt(old_name.position)
                })?;
                if table.column(&new_name.name).is_some() {
                    return Err(DbError::new(
                        "42701",
                        format!("column {} already exists", new_name.name),
                    )
                    .with_position_opt(new_name.position));
                }
                bound.push(BoundAlterTableOperation::RenameColumn {
                    column_id: column.id,
                    new_name: new_name.name,
                });
            }
            ParsedAlterTableOperation::AddColumn {
                column,
                if_not_exists,
            } => {
                if virtual_columns
                    .iter()
                    .any(|candidate| candidate.name == column.name.name)
                {
                    if if_not_exists {
                        continue;
                    }
                    return Err(DbError::new(
                        "42701",
                        format!("column {} already exists", column.name.name),
                    )
                    .with_position_opt(column.name.position));
                }
                let (data_type, declared_type) = match column.declared_type {
                    Some(type_name) => {
                        let (data_type, type_id) =
                            resolve_declared_data_type(catalog, &column.data_type, &type_name)?;
                        (data_type, Some(type_id))
                    }
                    None => (column.data_type, None),
                };
                let default = column
                    .default
                    .map(|default| {
                        bind_expr(default.expression, None, Some(&data_type))?;
                        Ok(CatalogExpression::new(default.sql))
                    })
                    .transpose()?;
                let column = NewColumn {
                    name: column.name.name,
                    data_type,
                    declared_type,
                    nullable: column.nullable,
                    primary_key: column.primary_key,
                    unique: column.unique,
                    default,
                };
                virtual_columns.push(column.clone());
                bound.push(BoundAlterTableOperation::AddColumn {
                    column,
                    if_not_exists,
                });
            }
            ParsedAlterTableOperation::DropColumns {
                columns,
                if_exists,
                behavior,
            } => {
                let mut column_ids = Vec::new();
                for column in columns {
                    match table.column(&column.name) {
                        Some(definition) => column_ids.push(definition.id),
                        None if if_exists => {}
                        None => {
                            return Err(DbError::new(
                                UNDEFINED_COLUMN,
                                format!("column {} does not exist", column.name),
                            )
                            .with_position_opt(column.position));
                        }
                    }
                }
                bound.push(BoundAlterTableOperation::DropColumns {
                    column_ids,
                    if_exists,
                    behavior,
                });
            }
            ParsedAlterTableOperation::SetNotNull { column } => {
                bound.push(BoundAlterTableOperation::SetNotNull {
                    column_id: resolve_column_id(&table, column)?,
                });
            }
            ParsedAlterTableOperation::DropNotNull { column } => {
                bound.push(BoundAlterTableOperation::DropNotNull {
                    column_id: resolve_column_id(&table, column)?,
                });
            }
            ParsedAlterTableOperation::SetDefault { column, default } => {
                let definition = table.column(&column.name).ok_or_else(|| {
                    DbError::new(
                        UNDEFINED_COLUMN,
                        format!("column {} does not exist", column.name),
                    )
                    .with_position_opt(column.position)
                })?;
                bind_expr(default.expression, None, Some(&definition.data_type))?;
                bound.push(BoundAlterTableOperation::SetDefault {
                    column_id: definition.id,
                    default: CatalogExpression::new(default.sql),
                });
            }
            ParsedAlterTableOperation::DropDefault { column } => {
                bound.push(BoundAlterTableOperation::DropDefault {
                    column_id: resolve_column_id(&table, column)?,
                });
            }
            ParsedAlterTableOperation::SetDataType {
                column,
                data_type,
                declared_type,
            } => {
                let (data_type, declared_type) = match declared_type {
                    Some(type_name) => {
                        let (data_type, type_id) =
                            resolve_declared_data_type(catalog, &data_type, &type_name)?;
                        (data_type, Some(type_id))
                    }
                    None => (data_type, None),
                };
                bound.push(BoundAlterTableOperation::SetDataType {
                    column_id: resolve_column_id(&table, column)?,
                    data_type,
                    declared_type,
                });
            }
            ParsedAlterTableOperation::AddConstraint { constraint } => {
                bound.push(BoundAlterTableOperation::AddConstraint {
                    constraint: bind_table_constraint(
                        constraint,
                        &table.name,
                        table.constraints().count().saturating_add(ordinal),
                        &virtual_columns,
                        catalog,
                    )?,
                });
            }
            ParsedAlterTableOperation::DropConstraint {
                name,
                if_exists,
                behavior,
            } => {
                let constraint = table.constraint(&name.name).map(|value| value.id);
                if constraint.is_none() && !if_exists {
                    return Err(DbError::new(
                        "42704",
                        format!("constraint {} does not exist", name.name),
                    )
                    .with_position_opt(name.position));
                }
                bound.push(BoundAlterTableOperation::DropConstraint {
                    constraint_id: constraint,
                    if_exists,
                    behavior,
                });
            }
            ParsedAlterTableOperation::SetTriggerEnabled { name, enabled } => {
                let trigger = table.trigger(&name.name).map(|value| value.id);
                if trigger.is_none() {
                    return Err(DbError::new(
                        "42704",
                        format!("trigger {} does not exist", name.name),
                    )
                    .with_position_opt(name.position));
                }
                bound.push(BoundAlterTableOperation::SetTriggerEnabled {
                    trigger_id: trigger,
                    name: name.name,
                    enabled,
                });
            }
        }
    }
    Ok(BoundStatement::AlterTable {
        table_id: table.id,
        operations: bound,
    })
}

fn resolve_column_id(table: &TableDefinition, column: ParsedIdentifier) -> Result<ColumnId> {
    table
        .column(&column.name)
        .map(|column| column.id)
        .ok_or_else(|| {
            DbError::new(
                UNDEFINED_COLUMN,
                format!("column {} does not exist", column.name),
            )
            .with_position_opt(column.position)
        })
}

struct CreateViewBindingInput {
    name: ParsedObjectName,
    kind: ViewKind,
    query: ParsedStatement,
    query_sql: String,
    columns: Vec<ParsedIdentifier>,
    replace: bool,
    if_not_exists: bool,
    with_data: bool,
}

fn bind_create_view(
    input: CreateViewBindingInput,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let CreateViewBindingInput {
        name,
        kind,
        query,
        query_sql,
        columns,
        replace,
        if_not_exists,
        with_data,
    } = input;
    let (schema, name, position) = split_table_name(&name)?;
    if catalog.schema(&schema).is_none() {
        return Err(
            DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                .with_position_opt(position),
        );
    }
    let existing = catalog.view(&schema, &name);
    if existing.is_some() && !replace {
        if if_not_exists {
            return Ok(BoundStatement::NoOp {
                tag: format!("CREATE {}", view_label(kind)),
            });
        }
        return Err(
            DbError::new("42P07", format!("relation {schema}.{name} already exists"))
                .with_position_opt(position),
        );
    }
    let query = bind_with_view_depth(query, catalog, view_depth)?;
    let mut output = bound_query_schema(&query)?;
    if !columns.is_empty() {
        if columns.len() != output.fields.len() {
            return Err(DbError::new(
                "42601",
                "view column list does not match query output",
            ));
        }
        for (field, name) in output.fields.iter_mut().zip(columns) {
            field.name = name.name.as_str().to_owned();
        }
    }
    let references = bound_statement_references(&query);
    Ok(BoundStatement::CreateView {
        schema,
        name,
        kind,
        query: Box::new(query),
        query_sql,
        output,
        references,
        replace,
        if_not_exists,
        with_data,
        existing: existing.map(|view| view.id),
    })
}

fn bound_query_schema(statement: &BoundStatement) -> Result<Schema> {
    match statement {
        BoundStatement::Select { schema, .. }
        | BoundStatement::AdvancedSelect { schema, .. }
        | BoundStatement::SetOperation { schema, .. }
        | BoundStatement::With { schema, .. }
        | BoundStatement::ViewSelect { schema, .. }
        | BoundStatement::ScalarSelect { schema, .. }
        | BoundStatement::RoutineSelect { schema, .. }
        | BoundStatement::SequenceValue { schema, .. } => Ok(schema.clone()),
        _ => unsupported("views require a SELECT query"),
    }
}

fn bound_statement_references(statement: &BoundStatement) -> Vec<CatalogObjectRef> {
    let mut references = Vec::new();
    let mut pending = vec![statement];
    while let Some(statement) = pending.pop() {
        match statement {
            BoundStatement::Select { table_id, .. } => {
                references.push(CatalogObjectRef::Table(*table_id));
            }
            BoundStatement::AdvancedSelect {
                table,
                joins,
                applies,
                ..
            } => {
                references.push(CatalogObjectRef::Table(table.table_id));
                for join in joins {
                    match &join.source {
                        BoundJoinSource::Table(table) => {
                            references.push(CatalogObjectRef::Table(table.table_id));
                        }
                        BoundJoinSource::Derived { query, .. } => pending.push(query),
                    }
                }
                pending.extend(applies.iter().map(|apply| apply.query.as_ref()));
            }
            BoundStatement::SetOperation { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundStatement::With { ctes, body, .. } => {
                pending.push(body);
                for cte in ctes {
                    pending.push(&cte.seed);
                    if let Some(recursive) = &cte.recursive {
                        pending.push(recursive);
                    }
                }
            }
            BoundStatement::ViewSelect { view_id, .. } => {
                references.push(CatalogObjectRef::View(*view_id));
            }
            BoundStatement::ScalarSelect { .. } => {}
            BoundStatement::RoutineSelect { routine_id, .. } => {
                references.push(CatalogObjectRef::Routine(*routine_id));
            }
            _ => {}
        }
    }
    references.sort();
    references.dedup();
    references
}

fn ddl_object_label(kind: DdlObjectKind) -> &'static str {
    match kind {
        DdlObjectKind::Schema => "SCHEMA",
        DdlObjectKind::Table => "TABLE",
        DdlObjectKind::Index => "INDEX",
        DdlObjectKind::Sequence => "SEQUENCE",
        DdlObjectKind::View => "VIEW",
        DdlObjectKind::MaterializedView => "MATERIALIZED VIEW",
        DdlObjectKind::Type => "TYPE",
    }
}

fn view_label(kind: ViewKind) -> &'static str {
    match kind {
        ViewKind::Regular => "VIEW",
        ViewKind::Materialized => "MATERIALIZED VIEW",
    }
}

#[derive(Debug, Clone)]
struct InputColumn {
    binding: Identifier,
    name: Identifier,
    index: usize,
    data_type: ScalarType,
    nullable: bool,
    outer_depth: usize,
}

struct AdvancedSelectInput {
    table: ParsedTable,
    joins: Vec<ParsedJoin>,
    projection: Vec<ParsedProjection>,
    distinct: bool,
    filter: Option<ParsedExpr>,
    group_by: Vec<ParsedExpr>,
    having: Option<ParsedExpr>,
    order_by: Vec<ParsedOrder>,
    offset: Option<ParsedExpr>,
    limit: Option<ParsedExpr>,
}

struct SelectInput {
    table_name: ParsedObjectName,
    projection: Vec<ParsedProjection>,
    filter: Option<ParsedExpr>,
    order_by: Vec<ParsedOrder>,
    offset: Option<ParsedExpr>,
    limit: Option<ParsedExpr>,
}
