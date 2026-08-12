
fn convert_statement(statement: SqlStatement, sql: &str) -> Result<ParsedStatement> {
    match statement {
        SqlStatement::StartTransaction {
            modes,
            begin,
            transaction,
            modifier,
            statements,
            exception,
            has_end_keyword,
        } => {
            let supported_keyword = if begin {
                matches!(
                    transaction,
                    None | Some(BeginTransactionKind::Transaction)
                        | Some(BeginTransactionKind::Work)
                )
            } else {
                matches!(transaction, Some(BeginTransactionKind::Transaction))
            };
            if !supported_keyword
                || modifier.is_some()
                || !statements.is_empty()
                || exception.is_some()
                || has_end_keyword
            {
                return unsupported("transaction modes and options are not supported yet");
            }
            Ok(ParsedStatement::Begin {
                characteristics: convert_transaction_modes(modes, false)?,
            })
        }
        SqlStatement::Commit {
            chain,
            end,
            modifier,
        } => {
            if end || modifier.is_some() || has_keyword_sequence(sql, &["COMMIT", "TRAN"]) {
                return unsupported("COMMIT options are not supported yet");
            }
            Ok(ParsedStatement::Commit {
                chain: convert_transaction_chain(chain, sql),
            })
        }
        SqlStatement::Rollback { chain, savepoint } => {
            if has_keyword_sequence(sql, &["ROLLBACK", "TRAN"]) {
                return unsupported("ROLLBACK TRAN is not supported");
            }
            if let Some(name) = savepoint {
                if chain || has_keyword_sequence(sql, &["AND", "NO", "CHAIN"]) {
                    return unsupported("ROLLBACK TO SAVEPOINT cannot use AND CHAIN");
                }
                return Ok(ParsedStatement::RollbackTo {
                    name: convert_ident(name, sql),
                });
            }
            Ok(ParsedStatement::Rollback {
                chain: convert_transaction_chain(chain, sql),
            })
        }
        SqlStatement::Savepoint { name } => Ok(ParsedStatement::Savepoint {
            name: convert_ident(name, sql),
        }),
        SqlStatement::ReleaseSavepoint { name } => Ok(ParsedStatement::ReleaseSavepoint {
            name: convert_ident(name, sql),
        }),
        SqlStatement::Analyze(analyze) => {
            if analyze.partitions.is_some()
                || analyze.for_columns
                || !analyze.columns.is_empty()
                || analyze.cache_metadata
                || analyze.noscan
                || analyze.compute_statistics
                || analyze.has_table_keyword
            {
                return unsupported("this ANALYZE form is not supported yet");
            }
            Ok(ParsedStatement::Analyze {
                table: analyze
                    .table_name
                    .map(|table| convert_object_name(table, sql))
                    .transpose()?,
            })
        }
        SqlStatement::Vacuum(vacuum) => {
            if vacuum.full
                || vacuum.sort_only
                || vacuum.delete_only
                || vacuum.reindex
                || vacuum.recluster
                || vacuum.threshold.is_some()
                || vacuum.boost
            {
                return unsupported("this VACUUM form is not supported yet");
            }
            Ok(ParsedStatement::Vacuum {
                table: vacuum
                    .table_name
                    .map(|table| convert_object_name(table, sql))
                    .transpose()?,
                analyze: false,
            })
        }
        SqlStatement::CreateSchema {
            schema_name,
            if_not_exists,
            with,
            options,
            default_collate_spec,
            clone,
        } => {
            if with.is_some()
                || options.is_some()
                || default_collate_spec.is_some()
                || clone.is_some()
            {
                return unsupported("CREATE SCHEMA options are not supported yet");
            }
            let SchemaName::Simple(name) = schema_name else {
                return unsupported("CREATE SCHEMA AUTHORIZATION is not supported yet");
            };
            let object = convert_object_name(name, sql)?;
            let [name] = object.parts.as_slice() else {
                return unsupported("qualified schema names are not supported");
            };
            Ok(ParsedStatement::CreateSchema {
                name: name.clone(),
                if_not_exists,
            })
        }
        SqlStatement::CreateType {
            name,
            representation,
        } => {
            let Some(UserDefinedTypeRepresentation::Enum { labels }) = representation else {
                return unsupported("only CREATE TYPE ... AS ENUM is supported");
            };
            Ok(ParsedStatement::CreateEnumType {
                name: convert_object_name(name, sql)?,
                labels: labels.into_iter().map(|label| label.value).collect(),
            })
        }
        SqlStatement::AlterType(alter) => {
            let name = convert_object_name(alter.name, sql)?;
            match alter.operation {
                AlterTypeOperation::Rename(_) => {
                    unsupported("ALTER TYPE RENAME TO is not supported yet")
                }
                AlterTypeOperation::AddValue(operation) => {
                    let position = operation.position.map(|position| match position {
                        AlterTypeAddValuePosition::Before(label) => {
                            EnumValuePosition::Before(label.value)
                        }
                        AlterTypeAddValuePosition::After(label) => {
                            EnumValuePosition::After(label.value)
                        }
                    });
                    Ok(ParsedStatement::AlterEnumAddValue {
                        name,
                        label: operation.value.value,
                        position,
                        if_not_exists: operation.if_not_exists,
                    })
                }
                AlterTypeOperation::RenameValue(operation) => {
                    Ok(ParsedStatement::AlterEnumRenameValue {
                        name,
                        old_label: operation.from.value,
                        new_label: operation.to.value,
                    })
                }
            }
        }
        SqlStatement::CreateDomain(domain) => {
            if domain.collation.is_some() {
                return unsupported("CREATE DOMAIN COLLATE is not supported yet");
            }
            let (base_type, base_declared_type) = convert_column_data_type(domain.data_type, sql)?;
            let default = domain
                .default
                .map(|expression| {
                    Ok(ParsedDefault {
                        sql: expression.to_string(),
                        expression: convert_expr(expression, sql)?,
                    })
                })
                .transpose()?;
            let checks = domain
                .constraints
                .into_iter()
                .map(|constraint| {
                    let TableConstraint::Check(check) = constraint else {
                        return unsupported(
                            "CREATE DOMAIN supports only CHECK constraints in this build",
                        );
                    };
                    if check.enforced.is_some() {
                        return unsupported("domain CHECK ENFORCED clauses are not supported");
                    }
                    Ok(DomainConstraint {
                        id: None,
                        name: check.name.map(|name| convert_ident(name, sql).name),
                        expression: CatalogExpression::new(check.expr.to_string()),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ParsedStatement::CreateDomain {
                name: convert_object_name(domain.name, sql)?,
                base_type,
                base_declared_type,
                not_null: create_domain_is_not_null(sql),
                default,
                checks,
            })
        }
        SqlStatement::AlterSchema(alter) => {
            if alter.operations.len() != 1 {
                return unsupported("ALTER SCHEMA supports one operation at a time");
            }
            let object = convert_object_name(alter.name, sql)?;
            let [name] = object.parts.as_slice() else {
                return unsupported("qualified schema names are not supported");
            };
            let SqlAlterSchemaOperation::Rename { name: new_name } =
                alter.operations.into_iter().next().ok_or_else(|| {
                    DbError::new(SYNTAX_ERROR, "ALTER SCHEMA requires an operation")
                })?
            else {
                return unsupported("only ALTER SCHEMA ... RENAME TO is supported");
            };
            let new_name = convert_single_identifier(new_name, sql)?;
            Ok(ParsedStatement::AlterSchemaRename {
                name: name.clone(),
                new_name,
                if_exists: alter.if_exists,
            })
        }
        SqlStatement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
            table,
        } => {
            if purge || temporary || table.is_some() || (cascade && restrict) {
                return unsupported("this DROP form is not supported");
            }
            let kind = match object_type {
                ObjectType::Schema => DdlObjectKind::Schema,
                ObjectType::Table => DdlObjectKind::Table,
                ObjectType::Index => DdlObjectKind::Index,
                ObjectType::Sequence => DdlObjectKind::Sequence,
                ObjectType::View => DdlObjectKind::View,
                ObjectType::MaterializedView => DdlObjectKind::MaterializedView,
                ObjectType::Type => DdlObjectKind::Type,
                _ => return unsupported("this DROP object type is not supported"),
            };
            if names.is_empty() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "DROP requires at least one object name",
                ));
            }
            Ok(ParsedStatement::DropObjects {
                kind,
                names: names
                    .into_iter()
                    .map(|name| convert_object_name(name, sql))
                    .collect::<Result<Vec<_>>>()?,
                if_exists,
                behavior: if cascade {
                    DropBehavior::Cascade
                } else {
                    DropBehavior::Restrict
                },
            })
        }
        SqlStatement::CreateTable(table) => convert_create_table(table, sql),
        SqlStatement::AlterTable(alter) => convert_alter_table(alter, sql),
        SqlStatement::CreateIndex(index) => {
            if index.concurrently
                || index.nulls_distinct.is_some()
                || index.predicate.is_some()
                || !index.index_options.is_empty()
                || !index.alter_options.is_empty()
            {
                return unsupported("this CREATE INDEX form is not supported yet");
            }
            let method = convert_index_method(index.using)?;
            let options = convert_index_options(index.with, sql)?;
            let name = index
                .name
                .ok_or_else(|| DbError::new(SYNTAX_ERROR, "CREATE INDEX requires a name"))?;
            let name = convert_single_identifier(name, sql)?;
            let key_columns = index
                .columns
                .iter()
                .map(|column| convert_index_column(column, sql))
                .collect::<Result<Vec<_>>>()?;
            let include_columns = index
                .include
                .into_iter()
                .map(|column| convert_ident(column, sql))
                .collect();
            Ok(ParsedStatement::CreateIndex(ParsedCreateIndex {
                name,
                table: convert_object_name(index.table_name, sql)?,
                key_columns,
                include_columns,
                unique: index.unique,
                method,
                options,
                if_not_exists: index.if_not_exists,
            }))
        }
        SqlStatement::AlterIndex { name, operation } => {
            let AlterIndexOperation::RenameIndex { index_name } = operation;
            Ok(ParsedStatement::AlterIndexRename {
                name: convert_object_name(name, sql)?,
                new_name: convert_single_identifier(index_name, sql)?,
            })
        }
        SqlStatement::CreateSequence {
            temporary,
            if_not_exists,
            name,
            data_type,
            sequence_options,
            owned_by,
        } => {
            if temporary {
                return unsupported("temporary sequences are not supported");
            }
            let mut sequence = NewSequence::new(Identifier::unquoted("pending"));
            if let Some(data_type) = data_type {
                sequence.data_type = convert_data_type(data_type)?;
            }
            apply_sequence_options(&mut sequence, sequence_options, sql)?;
            let owner = owned_by
                .map(|owner| split_owned_by(owner, sql))
                .transpose()?;
            Ok(ParsedStatement::CreateSequence {
                name: convert_object_name(name, sql)?,
                sequence,
                if_not_exists,
                owner,
            })
        }
        SqlStatement::CreateView(view) => convert_create_view(view, sql),
        SqlStatement::CreateFunction(function) => convert_create_function(function, sql),
        SqlStatement::DropFunction(function) => convert_drop_routine(
            function.func_desc,
            RoutineKind::Function,
            function.if_exists,
            function.drop_behavior,
            sql,
        ),
        SqlStatement::DropProcedure {
            if_exists,
            proc_desc,
            drop_behavior,
        } => convert_drop_routine(
            proc_desc,
            RoutineKind::Procedure,
            if_exists,
            drop_behavior,
            sql,
        ),
        SqlStatement::CreateTrigger(trigger) => convert_create_trigger(trigger, sql),
        SqlStatement::DropTrigger(trigger) => {
            let trigger_name = convert_object_name(trigger.trigger_name, sql)?;
            let [trigger_name] = trigger_name.parts.as_slice() else {
                return unsupported("qualified trigger names are not supported");
            };
            let table = trigger
                .table_name
                .ok_or_else(|| DbError::new(SYNTAX_ERROR, "DROP TRIGGER requires ON table"))?;
            let behavior = match trigger.option {
                None | Some(SqlReferentialAction::Restrict) => DropBehavior::Restrict,
                Some(SqlReferentialAction::Cascade) => DropBehavior::Cascade,
                Some(_) => {
                    return unsupported("DROP TRIGGER supports only CASCADE or RESTRICT behavior");
                }
            };
            Ok(ParsedStatement::DropTrigger {
                name: trigger_name.clone(),
                table: convert_object_name(table, sql)?,
                if_exists: trigger.if_exists,
                behavior,
            })
        }
        SqlStatement::Call(function) => {
            let (name, arguments) = convert_routine_invocation(function, sql)?;
            Ok(ParsedStatement::Call { name, arguments })
        }
        SqlStatement::Insert(insert) => {
            if !insert.optimizer_hints.is_empty()
                || insert.or.is_some()
                || insert.ignore
                || insert.table_alias.is_some()
                || insert.overwrite
                || !insert.assignments.is_empty()
                || insert.partitioned.is_some()
                || !insert.after_columns.is_empty()
                || insert.output.is_some()
                || insert.replace_into
                || insert.priority.is_some()
                || insert.insert_alias.is_some()
                || insert.settings.is_some()
                || insert.format_clause.is_some()
                || insert.multi_table_insert_type.is_some()
                || !insert.multi_table_into_clauses.is_empty()
                || !insert.multi_table_when_clauses.is_empty()
                || insert.multi_table_else_clause.is_some()
            {
                return unsupported("this INSERT form is not supported yet");
            }
            let on_conflict = convert_on_conflict(insert.on, sql)?;
            let returning = convert_projection_items(insert.returning.unwrap_or_default(), sql)?;
            let TableObject::TableName(table) = insert.table else {
                return unsupported("INSERT targets must be named tables");
            };
            let source = insert
                .source
                .ok_or_else(|| DbError::new(SYNTAX_ERROR, "INSERT requires VALUES"))?;
            let rows = convert_values_query(*source, sql)?;
            let columns = insert
                .columns
                .into_iter()
                .map(|name| convert_single_identifier(name, sql))
                .collect::<Result<Vec<_>>>()?;
            Ok(ParsedStatement::Insert {
                table: convert_object_name(table, sql)?,
                columns,
                rows,
                on_conflict,
                returning,
            })
        }
        SqlStatement::Merge(merge) => convert_merge(merge, sql),
        SqlStatement::Query(query) => convert_select_query(*query, sql),
        SqlStatement::Update(update) => {
            if !update.optimizer_hints.is_empty()
                || update.from.is_some()
                || update.output.is_some()
                || update.or.is_some()
                || !update.order_by.is_empty()
                || update.limit.is_some()
            {
                return unsupported("this UPDATE form is not supported yet");
            }
            let returning = convert_projection_items(update.returning.unwrap_or_default(), sql)?;
            let table = convert_table_with_joins(update.table, sql)?;
            let assignments = convert_assignments(update.assignments, sql)?;
            Ok(ParsedStatement::Update {
                table,
                assignments,
                filter: update
                    .selection
                    .map(|expr| convert_expr(expr, sql))
                    .transpose()?,
                returning,
            })
        }
        SqlStatement::Delete(delete) => {
            if !delete.optimizer_hints.is_empty()
                || !delete.tables.is_empty()
                || delete.using.is_some()
                || delete.output.is_some()
                || !delete.order_by.is_empty()
                || delete.limit.is_some()
            {
                return unsupported("this DELETE form is not supported yet");
            }
            let returning = convert_projection_items(delete.returning.unwrap_or_default(), sql)?;
            let FromTable::WithFromKeyword(mut tables) = delete.from else {
                return unsupported("DELETE requires a named table after FROM");
            };
            if tables.len() != 1 {
                return unsupported("DELETE supports exactly one table");
            }
            Ok(ParsedStatement::Delete {
                table: convert_table_with_joins(tables.remove(0), sql)?,
                filter: delete
                    .selection
                    .map(|expr| convert_expr(expr, sql))
                    .transpose()?,
                returning,
            })
        }
        SqlStatement::Discard {
            object_type: DiscardObject::ALL,
        } => Ok(ParsedStatement::DiscardAll),
        SqlStatement::Discard { .. } => unsupported("only DISCARD ALL is supported"),
        SqlStatement::Deallocate { name, .. }
            if name.quote_style.is_none() && name.value.eq_ignore_ascii_case("ALL") =>
        {
            Ok(ParsedStatement::DeallocateAll)
        }
        SqlStatement::Deallocate { .. } => {
            unsupported("only DEALLOCATE ALL is supported at the SQL boundary")
        }
        SqlStatement::Explain {
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
            ..
        } => {
            if analyze || verbose || query_plan || estimate || format.is_some() || options.is_some()
            {
                return unsupported("EXPLAIN options and EXPLAIN ANALYZE are not supported yet");
            }
            Ok(ParsedStatement::Explain {
                statement: Box::new(convert_statement(*statement, sql)?),
            })
        }
        _ => unsupported("SQL statement is not supported in this milestone"),
    }
}

fn convert_create_table(table: CreateTable, sql: &str) -> Result<ParsedStatement> {
    if table.or_replace
        || table.temporary
        || table.external
        || table.dynamic
        || table.global.is_some()
        || table.transient
        || table.volatile
        || table.iceberg
        || table.snapshot
        || !matches!(
            table.hive_distribution,
            sqlparser::ast::HiveDistributionStyle::NONE
        )
        || table.hive_formats.is_some()
        || !matches!(table.table_options, CreateTableOptions::None)
        || table.file_format.is_some()
        || table.location.is_some()
        || table.query.is_some()
        || table.without_rowid
        || table.like.is_some()
        || table.clone.is_some()
        || table.version.is_some()
        || table.comment.is_some()
        || table.on_commit.is_some()
        || table.on_cluster.is_some()
        || table.primary_key.is_some()
        || table.order_by.is_some()
        || table.partition_by.is_some()
        || table.cluster_by.is_some()
        || table.clustered_by.is_some()
        || table.inherits.is_some()
        || table.partition_of.is_some()
        || table.for_values.is_some()
        || table.strict
        || table.copy_grants
        || table.enable_schema_evolution.is_some()
        || table.change_tracking.is_some()
        || table.data_retention_time_in_days.is_some()
        || table.max_data_extension_time_in_days.is_some()
        || table.default_ddl_collation.is_some()
        || table.with_aggregation_policy.is_some()
        || table.with_row_access_policy.is_some()
        || table.with_storage_lifecycle_policy.is_some()
        || table.with_tags.is_some()
        || table.external_volume.is_some()
        || table.base_location.is_some()
        || table.catalog.is_some()
        || table.catalog_sync.is_some()
        || table.storage_serialization_policy.is_some()
        || table.target_lag.is_some()
        || table.warehouse.is_some()
        || table.refresh_mode.is_some()
        || table.initialize.is_some()
        || table.require_user
        || table.diststyle.is_some()
        || table.distkey.is_some()
        || table.sortkey.is_some()
        || table.backup.is_some()
    {
        return unsupported("this CREATE TABLE form is not supported yet");
    }

    let mut columns = Vec::with_capacity(table.columns.len());
    let mut constraints = Vec::new();
    for column in table.columns {
        let (column, mut column_constraints) = convert_column_definition(column, sql)?;
        columns.push(column);
        constraints.append(&mut column_constraints);
    }
    for constraint in table.constraints {
        constraints.push(convert_table_constraint(constraint, sql)?);
    }

    Ok(ParsedStatement::CreateTable {
        name: convert_object_name(table.name, sql)?,
        columns,
        constraints,
        if_not_exists: table.if_not_exists,
    })
}

fn convert_column_definition(
    column: ColumnDef,
    sql: &str,
) -> Result<(ParsedColumn, Vec<ParsedTableConstraint>)> {
    let name = convert_ident(column.name, sql);
    let (data_type, declared_type) = convert_column_data_type(column.data_type, sql)?;
    let mut parsed = ParsedColumn {
        name: name.clone(),
        data_type,
        declared_type,
        nullable: true,
        primary_key: false,
        unique: false,
        default: None,
    };
    let mut constraints = Vec::new();
    for option in column.options {
        let constraint_name = option.name.map(|name| convert_ident(name, sql));
        match option.option {
            ColumnOption::Null => parsed.nullable = true,
            ColumnOption::NotNull => parsed.nullable = false,
            ColumnOption::Default(expression) => {
                if parsed.default.is_some() {
                    return Err(DbError::new(
                        SYNTAX_ERROR,
                        format!("column {} has more than one default", parsed.name.name),
                    )
                    .with_position_opt(parsed.name.position));
                }
                parsed.default = Some(ParsedDefault {
                    sql: expression.to_string(),
                    expression: convert_expr(expression, sql)?,
                });
            }
            ColumnOption::PrimaryKey(constraint) => {
                if constraint.characteristics.is_some()
                    || constraint.index_name.is_some()
                    || constraint.index_type.is_some()
                    || !constraint.index_options.is_empty()
                {
                    return unsupported("extended primary-key constraints are not supported");
                }
                parsed.nullable = false;
                if constraint_name.is_some() {
                    constraints.push(ParsedTableConstraint::PrimaryKey {
                        name: constraint_name,
                        columns: vec![name.clone()],
                    });
                } else {
                    parsed.primary_key = true;
                    parsed.unique = true;
                }
            }
            ColumnOption::Unique(constraint) => {
                if constraint.characteristics.is_some()
                    || constraint.index_name.is_some()
                    || constraint.index_type.is_some()
                    || !constraint.index_options.is_empty()
                {
                    return unsupported("extended unique constraints are not supported");
                }
                if constraint_name.is_some() {
                    constraints.push(ParsedTableConstraint::Unique {
                        name: constraint_name,
                        columns: vec![name.clone()],
                    });
                } else {
                    parsed.unique = true;
                }
            }
            ColumnOption::Check(constraint) => {
                if constraint.enforced.is_some() {
                    return unsupported("CHECK ENFORCED clauses are not supported");
                }
                constraints.push(ParsedTableConstraint::Check {
                    name: constraint_name
                        .or_else(|| constraint.name.map(|name| convert_ident(name, sql))),
                    sql: constraint.expr.to_string(),
                    expression: convert_expr(*constraint.expr, sql)?,
                });
            }
            ColumnOption::ForeignKey(constraint) => {
                if constraint.index_name.is_some()
                    || constraint.match_kind.is_some()
                    || constraint.characteristics.is_some()
                {
                    return unsupported("extended foreign-key constraints are not supported");
                }
                constraints.push(ParsedTableConstraint::ForeignKey {
                    name: constraint_name
                        .or_else(|| constraint.name.map(|name| convert_ident(name, sql))),
                    columns: vec![name.clone()],
                    referenced_table: convert_object_name(constraint.foreign_table, sql)?,
                    referenced_columns: constraint
                        .referred_columns
                        .into_iter()
                        .map(|column| convert_ident(column, sql))
                        .collect(),
                    on_delete: convert_referential_action(constraint.on_delete)?,
                    on_update: convert_referential_action(constraint.on_update)?,
                });
            }
            _ => return unsupported("this column constraint is not supported"),
        }
    }
    Ok((parsed, constraints))
}
