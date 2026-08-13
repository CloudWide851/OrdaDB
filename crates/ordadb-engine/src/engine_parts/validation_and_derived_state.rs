
fn validate_rows(catalog: &Catalog, table: &TableDefinition, rows: &[Row]) -> Result<()> {
    for row in rows {
        if row.values.len() != table.columns().len() {
            return Err(internal_error("row width does not match table metadata"));
        }
        for (column, value) in table.columns().iter().zip(&row.values) {
            if !column.nullable && value.is_null() {
                return Err(DbError::new(
                    "23502",
                    format!(
                        "null value in column {} violates not-null constraint",
                        column.name
                    ),
                ));
            }
            coerce_execution_value(value.clone(), &column.data_type)?;
        }
    }
    for (column_index, column) in table.columns().iter().enumerate() {
        let Some(type_id) = column.declared_type else {
            continue;
        };
        let definition = catalog.type_by_id(type_id).ok_or_else(|| {
            DbError::new(
                "XX001",
                format!("column {} references a missing declared type", column.name),
            )
        })?;
        match &definition.definition {
            UserDefinedTypeKind::Enum { labels } => {
                for row in rows {
                    validate_enum_value(&row.values[column_index], labels, &definition.name)?;
                }
            }
            UserDefinedTypeKind::Domain {
                base_type,
                not_null,
                checks,
                ..
            } => {
                let scope = TableDefinition::expression_scope(
                    Identifier::unquoted("value"),
                    base_type.clone(),
                );
                let checks = checks
                    .iter()
                    .map(|constraint| {
                        Ok((
                            constraint.name.as_ref(),
                            bind_catalog_expression_with_catalog(
                                &constraint.expression,
                                Some(&scope),
                                Some(&ScalarType::Boolean),
                                catalog,
                            )?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                for row in rows {
                    let value = &row.values[column_index];
                    let domain_values = match value {
                        Value::Array(array) => array.values().iter().collect::<Vec<_>>(),
                        Value::Null if matches!(column.data_type, ScalarType::Array { .. }) => {
                            Vec::new()
                        }
                        value => vec![value],
                    };
                    for value in domain_values {
                        if *not_null && value.is_null() {
                            return Err(DbError::new(
                                "23502",
                                format!("domain {} does not allow null values", definition.name),
                            ));
                        }
                        for (constraint_name, check) in &checks {
                            if evaluate_scalar(check, std::slice::from_ref(value), &[])?
                                == Value::Boolean(false)
                            {
                                let label = constraint_name.map_or_else(
                                    || format!("domain {}", definition.name),
                                    |name| format!("constraint {name}"),
                                );
                                return Err(DbError::new(
                                    "23514",
                                    format!(
                                        "value for domain {} violates {label}",
                                        definition.name
                                    ),
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    for (column_index, column) in table.columns().iter().enumerate() {
        if !column.unique {
            continue;
        }
        for left in 0..rows.len() {
            let left_value = &rows[left].values[column_index];
            if left_value.is_null() {
                continue;
            }
            for right_row in rows.iter().skip(left + 1) {
                if left_value == &right_row.values[column_index] {
                    return Err(DbError::new(
                        "23505",
                        format!(
                            "duplicate value violates unique constraint on {}",
                            column.name
                        ),
                    ));
                }
            }
        }
    }
    for constraint in table.constraints() {
        match &constraint.kind {
            ConstraintKind::PrimaryKey { columns } | ConstraintKind::Unique { columns } => {
                let positions = columns
                    .iter()
                    .map(|column_id| {
                        table.column_index_by_id(*column_id).ok_or_else(|| {
                            internal_error("constraint column is absent from its table")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                for (left, left_row) in rows.iter().enumerate() {
                    let left_key = positions
                        .iter()
                        .map(|position| &left_row.values[*position])
                        .collect::<Vec<_>>();
                    if matches!(constraint.kind, ConstraintKind::PrimaryKey { .. })
                        && left_key.iter().any(|value| value.is_null())
                    {
                        return Err(DbError::new(
                            "23502",
                            format!(
                                "null value violates primary-key constraint {}",
                                constraint.name
                            ),
                        ));
                    }
                    if left_key.iter().any(|value| value.is_null()) {
                        continue;
                    }
                    for right_row in rows.iter().skip(left + 1) {
                        if positions.iter().all(|position| {
                            left_row.values[*position] == right_row.values[*position]
                        }) {
                            return Err(DbError::new(
                                "23505",
                                format!(
                                    "duplicate value violates unique constraint {}",
                                    constraint.name
                                ),
                            ));
                        }
                    }
                }
            }
            ConstraintKind::Check { expression } => {
                let bound = bind_catalog_expression_with_catalog(
                    expression,
                    Some(table),
                    Some(&ScalarType::Boolean),
                    catalog,
                )?;
                for row in rows {
                    if evaluate_scalar(&bound, &row.values, &[])? == Value::Boolean(false) {
                        return Err(DbError::new(
                            "23514",
                            format!("row violates check constraint {}", constraint.name),
                        ));
                    }
                }
            }
            ConstraintKind::ForeignKey { .. } => {}
        }
    }
    Ok(())
}

fn validate_enum_value(value: &Value, labels: &[String], type_name: &Identifier) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        Value::Text(value) if labels.iter().any(|label| label == value) => Ok(()),
        Value::Array(array) => {
            for value in array.values() {
                validate_enum_value(value, labels, type_name)?;
            }
            Ok(())
        }
        Value::Text(value) => Err(DbError::new(
            "22P02",
            format!("invalid input value for enum {type_name}: {value:?}"),
        )),
        _ => Err(DbError::new(
            "42804",
            format!("value is not assignable to enum {type_name}"),
        )),
    }
}

fn validate_database_rows(state: &DatabaseState) -> Result<()> {
    for schema in state.catalog.database().schemas() {
        for table in schema.tables() {
            let rows = state
                .rows
                .get(&table.id)
                .map_or(&[][..], |rows| rows.as_slice());
            validate_rows(&state.catalog, table, rows)?;
            for constraint in table.constraints() {
                let ConstraintKind::ForeignKey {
                    columns,
                    referenced_table,
                    referenced_columns,
                    ..
                } = &constraint.kind
                else {
                    continue;
                };
                let local_positions = columns
                    .iter()
                    .map(|column_id| {
                        table.column_index_by_id(*column_id).ok_or_else(|| {
                            internal_error("foreign-key column is absent from its table")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let referenced = table_definition(state, *referenced_table)?;
                let referenced_positions = referenced_columns
                    .iter()
                    .map(|column_id| {
                        referenced.column_index_by_id(*column_id).ok_or_else(|| {
                            internal_error("foreign-key referenced column is absent from its table")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let referenced_rows = state
                    .rows
                    .get(referenced_table)
                    .map_or(&[][..], |rows| rows.as_slice());
                for row in rows {
                    if local_positions
                        .iter()
                        .any(|position| row.values[*position].is_null())
                    {
                        continue;
                    }
                    if !referenced_rows.iter().any(|candidate| {
                        local_positions
                            .iter()
                            .zip(&referenced_positions)
                            .all(|(local, remote)| row.values[*local] == candidate.values[*remote])
                    }) {
                        return Err(DbError::new(
                            "23503",
                            format!(
                                "insert or update violates foreign-key constraint {}",
                                constraint.name
                            ),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn rebuild_btree_index(state: &mut DatabaseState, index_id: IndexId) -> Result<()> {
    let definition = state
        .catalog
        .index_by_id(index_id)
        .cloned()
        .ok_or_else(|| DbError::new("42704", "index does not exist"))?;
    if definition.method != IndexMethod::BTree {
        return Err(internal_error(
            "non-B-tree index reached the B-tree rebuild path",
        ));
    }
    let table = table_definition(state, definition.table_id)?.clone();
    let rows = state
        .rows
        .get(&definition.table_id)
        .cloned()
        .unwrap_or_default();
    let key_positions = definition
        .key_columns
        .iter()
        .map(|column_id| {
            table
                .column_index_by_id(*column_id)
                .ok_or_else(|| internal_error("index key column is absent from its table"))
        })
        .collect::<Result<Vec<_>>>()?;
    let key_types = key_positions
        .iter()
        .map(|position| table.columns()[*position].data_type.clone())
        .collect::<Vec<_>>();
    let include_positions = definition
        .include_columns
        .iter()
        .map(|column_id| {
            table
                .column_index_by_id(*column_id)
                .ok_or_else(|| internal_error("index include column is absent from its table"))
        })
        .collect::<Result<Vec<_>>>()?;
    let entries = rows
        .iter()
        .enumerate()
        .map(|(row_index, row)| {
            let row_id = u64::try_from(row_index)
                .map(RowId::new)
                .map_err(|_| DbError::new("54000", "table row count exceeds index limits"))?;
            let key_values = key_positions
                .iter()
                .map(|position| row.values[*position].clone())
                .collect::<Vec<_>>();
            let included = include_positions
                .iter()
                .map(|position| row.values[*position].clone())
                .collect();
            IndexEntry::new_typed(&key_values, &key_types, row_id, included)
        })
        .collect::<Result<Vec<_>>>()?;
    let tree = BPlusTree::from_entries(definition.unique, entries)?;
    state.indexes.insert(index_id, Arc::new(tree));
    Ok(())
}

fn rebuild_index_derived(state: &mut DatabaseState, index_id: IndexId) -> Result<()> {
    let definition = state
        .catalog
        .index_by_id(index_id)
        .cloned()
        .ok_or_else(|| DbError::new("42704", "index does not exist"))?;
    match definition.method {
        IndexMethod::BTree => rebuild_btree_index(state, index_id),
        IndexMethod::FullText | IndexMethod::Hnsw => {
            rebuild_search_catalog_for_table(state, definition.table_id)
        }
    }
}

fn rebuild_table_indexes(state: &mut DatabaseState, table_id: TableId) -> Result<()> {
    let table = table_definition(state, table_id)?.clone();
    let index_methods = table
        .indexes()
        .map(|definition| (definition.id, definition.method))
        .collect::<Vec<_>>();
    let catalog = Arc::clone(&state.catalog);
    state.indexes.retain(|index_id, _| {
        catalog
            .index_by_id(*index_id)
            .is_some_and(|definition| definition.method == IndexMethod::BTree)
    });
    for (index_id, method) in &index_methods {
        if *method == IndexMethod::BTree {
            rebuild_btree_index(state, *index_id)?;
        }
    }
    rebuild_search_catalog_for_table(state, table_id)?;
    Ok(())
}

fn rebuild_table_derived(state: &mut DatabaseState, table_id: TableId) -> Result<()> {
    rebuild_table_indexes(state, table_id)?;
    let table = table_definition(state, table_id)?.clone();
    let rows = state.rows.get(&table_id).cloned().unwrap_or_default();
    Arc::make_mut(&mut state.catalog)
        .set_table_statistics(table_id, compute_statistics(&table, &rows)?)?;
    Ok(())
}

fn rebuild_search_catalog_for_table(state: &mut DatabaseState, table_id: TableId) -> Result<()> {
    let searches = state
        .searches
        .rebuild_table(&state.catalog, &state.rows, table_id)?;
    state.searches = Arc::new(searches);
    Ok(())
}

fn reconcile_search_catalog(state: &mut DatabaseState) -> Result<()> {
    let searches = state.searches.reconcile(&state.catalog, &state.rows)?;
    state.searches = Arc::new(searches);
    Ok(())
}

fn compute_statistics(table: &TableDefinition, rows: &[Row]) -> Result<TableStatistics> {
    let mut columns = BTreeMap::new();
    for (column_index, column) in table.columns().iter().enumerate() {
        let values = rows
            .iter()
            .filter_map(|row| row.values.get(column_index))
            .collect::<Vec<_>>();
        let null_count = values.iter().filter(|value| value.is_null()).count() as u64;
        let mut distinct = HashSet::new();
        for value in values.iter().filter(|value| !value.is_null()) {
            distinct.insert(encode_row(&Row::new(vec![(*value).clone()]))?);
        }
        let (min, max) = if indexable_type(&column.data_type) {
            let mut minimum: Option<(IndexKey, Value)> = None;
            let mut maximum: Option<(IndexKey, Value)> = None;
            for value in values.iter().filter(|value| !value.is_null()) {
                let typed_value = (*value).clone();
                let key = IndexKey::from_typed_values(
                    std::slice::from_ref(&typed_value),
                    std::slice::from_ref(&column.data_type),
                )?;
                if minimum.as_ref().is_none_or(|(minimum, _)| key < *minimum) {
                    minimum = Some((key.clone(), (*value).clone()));
                }
                if maximum.as_ref().is_none_or(|(maximum, _)| key > *maximum) {
                    maximum = Some((key, (*value).clone()));
                }
            }
            (
                minimum.map(|(_, value)| value),
                maximum.map(|(_, value)| value),
            )
        } else {
            (None, None)
        };
        columns.insert(
            column.id,
            ColumnStatistics {
                null_count,
                distinct_count: distinct.len() as u64,
                min,
                max,
            },
        );
    }
    Ok(TableStatistics {
        row_count: rows.len() as u64,
        columns,
    })
}

fn table_definition(state: &DatabaseState, table_id: TableId) -> Result<&TableDefinition> {
    state
        .catalog
        .table_by_id(table_id)
        .ok_or_else(|| internal_error(format!("bound table ID {table_id:?} does not exist")))
}

fn search_index_table(
    state: &DatabaseState,
    index_id: IndexId,
    expected_method: IndexMethod,
) -> Result<TableId> {
    let definition = state
        .catalog
        .index_by_id(index_id)
        .ok_or_else(|| DbError::new("42704", format!("index {} does not exist", index_id.get())))?;
    if definition.method != expected_method {
        return Err(DbError::new(
            "42809",
            format!(
                "index {} uses {:?}, expected {expected_method:?}",
                definition.name, definition.method
            ),
        ));
    }
    Ok(definition.table_id)
}

fn search_result_row(state: &DatabaseState, table_id: TableId, row_id: SearchRowId) -> Result<Row> {
    let row_index = usize::try_from(row_id.get())
        .map_err(|_| internal_error("search Row ID exceeds the platform limit"))?;
    state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.get(row_index))
        .cloned()
        .ok_or_else(|| internal_error("search index returned a Row ID outside its table snapshot"))
}

fn evaluate_search_filter(
    state: &DatabaseState,
    table_id: TableId,
    filter: &ScalarSearchFilter,
) -> Result<AllowedRows> {
    let table = table_definition(state, table_id)?;
    let expression = bind_catalog_expression_with_catalog(
        &CatalogExpression::new(&filter.expression),
        Some(table),
        Some(&ScalarType::Boolean),
        &state.catalog,
    )?;
    let rows = state
        .rows
        .get(&table_id)
        .map_or(&[][..], |rows| rows.as_slice());
    let mut allowed = BTreeSet::new();
    for (row_index, row) in rows.iter().enumerate() {
        if execution_predicate_matches(&expression, row, &filter.parameters)? {
            allowed.insert(
                u64::try_from(row_index)
                    .map(SearchRowId::new)
                    .map_err(|_| DbError::new("54000", "table row count exceeds search limits"))?,
            );
        }
    }
    Ok(Arc::new(allowed))
}

fn intersect_allowed_rows(
    current: Option<AllowedRows>,
    filter: AllowedRows,
) -> Option<AllowedRows> {
    match current {
        Some(current) => Some(Arc::new(current.intersection(&filter).copied().collect())),
        None => Some(filter),
    }
}

fn command_events(
    schema: Schema,
    tag: impl Into<String>,
    rows_affected: u64,
    batch: Option<Batch>,
) -> Vec<QueryEvent> {
    let mut events = vec![QueryEvent::Schema(schema)];
    if let Some(batch) = batch {
        events.push(QueryEvent::Batch(batch));
    }
    events.push(QueryEvent::Progress(QueryProgress {
        rows_processed: rows_affected,
    }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: tag.into(),
        rows_affected,
    }));
    events
}

fn internal_error(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message).with_hint("restart the session and retry")
}

#[must_use]
pub fn configured_data_dir(config: &EngineConfig) -> &Path {
    &config.data_dir
}

#[cfg(test)]
mod tests {
    include!("system_catalog_and_reopen_tests.rs");
    include!("conflict_merge_and_stream_tests.rs");
    include!("transaction_and_lock_tests.rs");
    include!("maintenance_and_event_tests.rs");
    include!("apply_window_and_storage_tests.rs");
    include!("storage_resource_and_checkpoint_tests.rs");
}
