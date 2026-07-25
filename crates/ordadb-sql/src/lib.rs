//! PostgreSQL-dialect parsing and catalog-aware binding for OrdaDB.
//!
//! The public syntax tree in this crate is owned by OrdaDB. `sqlparser` is an
//! implementation detail so parser upgrades cannot leak into the engine,
//! storage, or protocol crates.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::str::FromStr;

use ordadb_catalog::{Catalog, NewColumn, TableDefinition};
use ordadb_types::{DbError, Field, Identifier, Result, ScalarType, Schema, TableId, Value};
use rust_decimal::Decimal;
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator as SqlBinaryOperator, CharacterLength, ColumnOption,
    CreateTable, DataType, ExactNumberInfo, Expr as SqlExpr, FromTable, GroupByExpr, Ident,
    LimitClause, ObjectName, ObjectNamePart, OrderByKind, Query, SchemaName, Select, SelectItem,
    SetExpr, Spanned, Statement as SqlStatement, TableConstraint, TableFactor, TableObject,
    TableWithJoins, TimezoneInfo, UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Span};

const FEATURE_NOT_SUPPORTED: &str = "0A000";
const SYNTAX_ERROR: &str = "42601";
const UNDEFINED_SCHEMA: &str = "3F000";
const UNDEFINED_TABLE: &str = "42P01";
const UNDEFINED_COLUMN: &str = "42703";
const DATATYPE_MISMATCH: &str = "42804";
const INDETERMINATE_DATATYPE: &str = "42P18";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedIdentifier {
    pub name: Identifier,
    pub position: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedObjectName {
    pub parts: Vec<ParsedIdentifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedExpr {
    pub kind: ParsedExprKind,
    pub position: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedExprKind {
    Column(ParsedObjectName),
    Literal(Value),
    Parameter(usize),
    Unary {
        op: UnaryOperator,
        expr: Box<ParsedExpr>,
    },
    Binary {
        left: Box<ParsedExpr>,
        op: BinaryOperator,
        right: Box<ParsedExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Negate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedProjection {
    Wildcard,
    Expression {
        expr: ParsedExpr,
        alias: Option<ParsedIdentifier>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedOrder {
    pub expr: ParsedExpr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedColumn {
    pub name: ParsedIdentifier,
    pub data_type: ScalarType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedStatement {
    CreateSchema {
        name: ParsedIdentifier,
    },
    CreateTable {
        name: ParsedObjectName,
        columns: Vec<ParsedColumn>,
    },
    Insert {
        table: ParsedObjectName,
        columns: Vec<ParsedIdentifier>,
        rows: Vec<Vec<ParsedExpr>>,
    },
    Select {
        table: ParsedObjectName,
        projection: Vec<ParsedProjection>,
        filter: Option<ParsedExpr>,
        order_by: Vec<ParsedOrder>,
        limit: Option<ParsedExpr>,
    },
    Update {
        table: ParsedObjectName,
        assignments: Vec<(ParsedIdentifier, ParsedExpr)>,
        filter: Option<ParsedExpr>,
    },
    Delete {
        table: ParsedObjectName,
        filter: Option<ParsedExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundExpr {
    pub kind: BoundExprKind,
    pub data_type: ScalarType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExprKind {
    Column {
        index: usize,
    },
    Literal(Value),
    Parameter {
        index: usize,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<BoundExpr>,
    },
    Binary {
        left: Box<BoundExpr>,
        op: BinaryOperator,
        right: Box<BoundExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundProjection {
    pub expr: BoundExpr,
    pub field: Field,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundOrder {
    pub column_index: usize,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundStatement {
    CreateSchema {
        name: Identifier,
    },
    CreateTable {
        schema: Identifier,
        name: Identifier,
        columns: Vec<NewColumn>,
    },
    Insert {
        table_id: TableId,
        column_indexes: Vec<usize>,
        rows: Vec<Vec<BoundExpr>>,
    },
    Select {
        table_id: TableId,
        schema: Schema,
        projection: Vec<BoundProjection>,
        filter: Option<BoundExpr>,
        order_by: Vec<BoundOrder>,
        limit: Option<BoundExpr>,
    },
    Update {
        table_id: TableId,
        assignments: Vec<(usize, BoundExpr)>,
        filter: Option<BoundExpr>,
    },
    Delete {
        table_id: TableId,
        filter: Option<BoundExpr>,
    },
}

/// Parse exactly one statement using PostgreSQL dialect rules.
pub fn parse(sql: &str) -> Result<ParsedStatement> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|error| {
        let message = error.to_string();
        let position = parser_error_position(sql, &message);
        let mut error = DbError::new(SYNTAX_ERROR, message);
        error.position = position;
        error
    })?;

    if statements.len() != 1 {
        return Err(DbError::new(
            FEATURE_NOT_SUPPORTED,
            "exactly one SQL statement must be executed at a time",
        ));
    }

    convert_statement(
        statements
            .pop()
            .ok_or_else(|| DbError::new(SYNTAX_ERROR, "SQL statement is empty"))?,
        sql,
    )
}

/// Bind an OrdaDB-owned parsed statement against an immutable catalog view.
pub fn bind(statement: ParsedStatement, catalog: &Catalog) -> Result<BoundStatement> {
    match statement {
        ParsedStatement::CreateSchema { name } => {
            if catalog.schema(&name.name).is_some() {
                return Err(
                    DbError::new("42P06", format!("schema {} already exists", name.name))
                        .with_position_opt(name.position),
                );
            }
            Ok(BoundStatement::CreateSchema { name: name.name })
        }
        ParsedStatement::CreateTable { name, columns } => bind_create_table(name, columns, catalog),
        ParsedStatement::Insert {
            table,
            columns,
            rows,
        } => bind_insert(table, columns, rows, catalog),
        ParsedStatement::Select {
            table,
            projection,
            filter,
            order_by,
            limit,
        } => bind_select(table, projection, filter, order_by, limit, catalog),
        ParsedStatement::Update {
            table,
            assignments,
            filter,
        } => bind_update(table, assignments, filter, catalog),
        ParsedStatement::Delete { table, filter } => bind_delete(table, filter, catalog),
    }
}

fn convert_statement(statement: SqlStatement, sql: &str) -> Result<ParsedStatement> {
    match statement {
        SqlStatement::CreateSchema {
            schema_name,
            if_not_exists,
            with,
            options,
            default_collate_spec,
            clone,
        } => {
            if if_not_exists
                || with.is_some()
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
            Ok(ParsedStatement::CreateSchema { name: name.clone() })
        }
        SqlStatement::CreateTable(table) => convert_create_table(table, sql),
        SqlStatement::Insert(insert) => {
            if !insert.optimizer_hints.is_empty()
                || insert.or.is_some()
                || insert.ignore
                || insert.table_alias.is_some()
                || insert.overwrite
                || !insert.assignments.is_empty()
                || insert.partitioned.is_some()
                || !insert.after_columns.is_empty()
                || insert.on.is_some()
                || insert.returning.is_some()
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
            })
        }
        SqlStatement::Query(query) => convert_select_query(*query, sql),
        SqlStatement::Update(update) => {
            if !update.optimizer_hints.is_empty()
                || update.from.is_some()
                || update.returning.is_some()
                || update.output.is_some()
                || update.or.is_some()
                || !update.order_by.is_empty()
                || update.limit.is_some()
            {
                return unsupported("this UPDATE form is not supported yet");
            }
            let table = convert_table_with_joins(update.table, sql)?;
            let assignments = update
                .assignments
                .into_iter()
                .map(|assignment| {
                    let AssignmentTarget::ColumnName(name) = assignment.target else {
                        return unsupported("tuple assignments are not supported yet");
                    };
                    Ok((
                        convert_single_identifier(name, sql)?,
                        convert_expr(assignment.value, sql)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ParsedStatement::Update {
                table,
                assignments,
                filter: update
                    .selection
                    .map(|expr| convert_expr(expr, sql))
                    .transpose()?,
            })
        }
        SqlStatement::Delete(delete) => {
            if !delete.optimizer_hints.is_empty()
                || !delete.tables.is_empty()
                || delete.using.is_some()
                || delete.returning.is_some()
                || delete.output.is_some()
                || !delete.order_by.is_empty()
                || delete.limit.is_some()
            {
                return unsupported("this DELETE form is not supported yet");
            }
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
        || table.if_not_exists
        || table.transient
        || table.volatile
        || table.iceberg
        || table.snapshot
        || table.query.is_some()
        || table.without_rowid
        || table.like.is_some()
        || table.clone.is_some()
        || table.version.is_some()
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
    {
        return unsupported("this CREATE TABLE form is not supported yet");
    }

    let mut columns = table
        .columns
        .into_iter()
        .map(|column| {
            let mut parsed = ParsedColumn {
                name: convert_ident(column.name, sql),
                data_type: convert_data_type(column.data_type)?,
                nullable: true,
                primary_key: false,
                unique: false,
            };
            for option in column.options {
                if option.name.is_some() {
                    return unsupported("named column constraints are not supported yet");
                }
                match option.option {
                    ColumnOption::Null => parsed.nullable = true,
                    ColumnOption::NotNull => parsed.nullable = false,
                    ColumnOption::PrimaryKey(constraint) => {
                        if constraint.characteristics.is_some() {
                            return unsupported(
                                "deferred primary-key constraints are not supported yet",
                            );
                        }
                        parsed.primary_key = true;
                        parsed.unique = true;
                        parsed.nullable = false;
                    }
                    ColumnOption::Unique(constraint) => {
                        if constraint.characteristics.is_some() {
                            return unsupported(
                                "deferred unique constraints are not supported yet",
                            );
                        }
                        parsed.unique = true;
                    }
                    _ => return unsupported("this column constraint is not supported yet"),
                }
            }
            Ok(parsed)
        })
        .collect::<Result<Vec<_>>>()?;

    for constraint in table.constraints {
        let (column_name, primary_key) = match constraint {
            TableConstraint::PrimaryKey(constraint) => {
                if constraint.columns.len() != 1
                    || constraint.index_type.is_some()
                    || !constraint.index_options.is_empty()
                    || constraint.characteristics.is_some()
                {
                    return unsupported("composite or extended primary keys are not supported yet");
                }
                (convert_index_column(&constraint.columns[0], sql)?, true)
            }
            TableConstraint::Unique(constraint) => {
                if constraint.columns.len() != 1
                    || constraint.index_type.is_some()
                    || !constraint.index_options.is_empty()
                    || constraint.characteristics.is_some()
                {
                    return unsupported("composite or extended unique keys are not supported yet");
                }
                (convert_index_column(&constraint.columns[0], sql)?, false)
            }
            _ => return unsupported("this table constraint is not supported yet"),
        };
        let column = columns
            .iter_mut()
            .find(|column| column.name.name == column_name.name)
            .ok_or_else(|| {
                DbError::new(
                    UNDEFINED_COLUMN,
                    format!("column {} does not exist", column_name.name),
                )
                .with_position_opt(column_name.position)
            })?;
        column.unique = true;
        if primary_key {
            column.primary_key = true;
            column.nullable = false;
        }
    }

    Ok(ParsedStatement::CreateTable {
        name: convert_object_name(table.name, sql)?,
        columns,
    })
}

fn convert_select_query(query: Query, sql: &str) -> Result<ParsedStatement> {
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return unsupported("this SELECT query form is not supported yet");
    }
    let SetExpr::Select(select) = *query.body else {
        return unsupported(
            "set operations, subqueries, and VALUES queries are not supported here",
        );
    };

    let order_by = match query.order_by {
        None => Vec::new(),
        Some(order) => {
            if order.interpolate.is_some() {
                return unsupported("ORDER BY INTERPOLATE is not supported");
            }
            let OrderByKind::Expressions(expressions) = order.kind else {
                return unsupported("ORDER BY ALL is not supported");
            };
            expressions
                .into_iter()
                .map(|order| {
                    if order.with_fill.is_some() {
                        return unsupported("ORDER BY WITH FILL is not supported");
                    }
                    Ok(ParsedOrder {
                        expr: convert_expr(order.expr, sql)?,
                        ascending: order.options.asc.unwrap_or(true),
                        nulls_first: order.options.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?
        }
    };

    let limit = match query.limit_clause {
        None => None,
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) if offset.is_none() && limit_by.is_empty() => {
            limit.map(|expr| convert_expr(expr, sql)).transpose()?
        }
        Some(_) => {
            return unsupported("OFFSET and dialect-specific LIMIT forms are not supported yet");
        }
    };

    convert_select(*select, order_by, limit, sql)
}

fn convert_select(
    select: Select,
    order_by: Vec<ParsedOrder>,
    limit: Option<ParsedExpr>,
    sql: &str,
) -> Result<ParsedStatement> {
    let group_by_is_empty = matches!(&select.group_by, GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty());
    if !select.optimizer_hints.is_empty()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !group_by_is_empty
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return unsupported(
            "joins, aggregates, DISTINCT, and extended SELECT clauses are not supported yet",
        );
    }
    if select.from.len() != 1 {
        return unsupported("SELECT supports exactly one table");
    }

    let projection = select
        .projection
        .into_iter()
        .map(|item| match item {
            SelectItem::Wildcard(_) => Ok(ParsedProjection::Wildcard),
            SelectItem::UnnamedExpr(expr) => Ok(ParsedProjection::Expression {
                expr: convert_expr(expr, sql)?,
                alias: None,
            }),
            SelectItem::ExprWithAlias { expr, alias } => Ok(ParsedProjection::Expression {
                expr: convert_expr(expr, sql)?,
                alias: Some(convert_ident(alias, sql)),
            }),
            _ => unsupported("qualified wildcards and multiple aliases are not supported yet"),
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ParsedStatement::Select {
        table: convert_table_with_joins(
            select
                .from
                .into_iter()
                .next()
                .ok_or_else(|| DbError::new(SYNTAX_ERROR, "SELECT requires a table"))?,
            sql,
        )?,
        projection,
        filter: select
            .selection
            .map(|expr| convert_expr(expr, sql))
            .transpose()?,
        order_by,
        limit,
    })
}

fn convert_values_query(query: Query, sql: &str) -> Result<Vec<Vec<ParsedExpr>>> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return unsupported("INSERT ... VALUES cannot contain query clauses");
    }
    let SetExpr::Values(values) = *query.body else {
        return unsupported("INSERT ... SELECT is not supported yet");
    };
    if values.explicit_row || values.value_keyword {
        return unsupported("dialect-specific VALUES forms are not supported");
    }
    values
        .rows
        .into_iter()
        .map(|row| {
            row.content
                .into_iter()
                .map(|expr| convert_expr(expr, sql))
                .collect()
        })
        .collect()
}

fn convert_table_with_joins(table: TableWithJoins, sql: &str) -> Result<ParsedObjectName> {
    if !table.joins.is_empty() {
        return unsupported("joins are not supported yet");
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = table.relation
    else {
        return unsupported("derived tables and table functions are not supported yet");
    };
    if alias.is_some()
        || args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return unsupported("table aliases and table modifiers are not supported yet");
    }
    convert_object_name(name, sql)
}

fn convert_expr(expr: SqlExpr, sql: &str) -> Result<ParsedExpr> {
    let position = span_position(sql, expr.span());
    let kind = match expr {
        SqlExpr::Identifier(ident) => ParsedExprKind::Column(ParsedObjectName {
            parts: vec![convert_ident(ident, sql)],
        }),
        SqlExpr::CompoundIdentifier(parts) => ParsedExprKind::Column(ParsedObjectName {
            parts: parts
                .into_iter()
                .map(|ident| convert_ident(ident, sql))
                .collect(),
        }),
        SqlExpr::Nested(expr) => return convert_expr(*expr, sql),
        SqlExpr::Value(value) => convert_sql_value(value.value, position)?,
        SqlExpr::UnaryOp { op, expr } => {
            let op = match op {
                SqlUnaryOperator::Not => UnaryOperator::Not,
                SqlUnaryOperator::Minus => UnaryOperator::Negate,
                SqlUnaryOperator::Plus => return convert_expr(*expr, sql),
                _ => return unsupported_at("this unary operator is not supported yet", position),
            };
            ParsedExprKind::Unary {
                op,
                expr: Box::new(convert_expr(*expr, sql)?),
            }
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let op = match op {
                SqlBinaryOperator::Eq => BinaryOperator::Eq,
                SqlBinaryOperator::NotEq => BinaryOperator::NotEq,
                SqlBinaryOperator::Lt => BinaryOperator::Lt,
                SqlBinaryOperator::LtEq => BinaryOperator::LtEq,
                SqlBinaryOperator::Gt => BinaryOperator::Gt,
                SqlBinaryOperator::GtEq => BinaryOperator::GtEq,
                SqlBinaryOperator::And => BinaryOperator::And,
                SqlBinaryOperator::Or => BinaryOperator::Or,
                _ => return unsupported_at("this binary operator is not supported yet", position),
            };
            ParsedExprKind::Binary {
                left: Box::new(convert_expr(*left, sql)?),
                op,
                right: Box::new(convert_expr(*right, sql)?),
            }
        }
        _ => return unsupported_at("this SQL expression is not supported yet", position),
    };
    Ok(ParsedExpr { kind, position })
}

fn convert_sql_value(value: SqlValue, position: Option<usize>) -> Result<ParsedExprKind> {
    match value {
        SqlValue::Null => Ok(ParsedExprKind::Literal(Value::Null)),
        SqlValue::Boolean(value) => Ok(ParsedExprKind::Literal(Value::Boolean(value))),
        SqlValue::Number(value, _) => {
            let value = if value.contains(['.', 'e', 'E']) {
                if value.contains(['e', 'E']) {
                    Value::Float64(value.parse().map_err(|_| {
                        DbError::new(SYNTAX_ERROR, "invalid floating-point literal")
                            .with_position_opt(position)
                    })?)
                } else {
                    Value::Decimal(Decimal::from_str(&value).map_err(|_| {
                        DbError::new(SYNTAX_ERROR, "invalid decimal literal")
                            .with_position_opt(position)
                    })?)
                }
            } else if let Ok(value) = value.parse::<i32>() {
                Value::Int32(value)
            } else {
                Value::Int64(value.parse().map_err(|_| {
                    DbError::new(SYNTAX_ERROR, "integer literal is out of range")
                        .with_position_opt(position)
                })?)
            };
            Ok(ParsedExprKind::Literal(value))
        }
        SqlValue::SingleQuotedString(value)
        | SqlValue::EscapedStringLiteral(value)
        | SqlValue::UnicodeStringLiteral(value)
        | SqlValue::NationalStringLiteral(value) => Ok(ParsedExprKind::Literal(Value::Text(value))),
        SqlValue::Placeholder(parameter) => {
            let index = parameter
                .strip_prefix('$')
                .and_then(|index| index.parse::<usize>().ok())
                .filter(|index| *index > 0)
                .ok_or_else(|| {
                    DbError::new("42P02", format!("invalid parameter reference {parameter}"))
                        .with_position_opt(position)
                })?;
            Ok(ParsedExprKind::Parameter(index))
        }
        _ => unsupported_at("this literal form is not supported yet", position),
    }
}

fn convert_data_type(data_type: DataType) -> Result<ScalarType> {
    match data_type {
        DataType::Bool | DataType::Boolean => Ok(ScalarType::Boolean),
        DataType::Int2(_) | DataType::SmallInt(_) => Ok(ScalarType::Int16),
        DataType::Int(_) | DataType::Int4(_) | DataType::Integer(_) => Ok(ScalarType::Int32),
        DataType::Int8(_) | DataType::BigInt(_) => Ok(ScalarType::Int64),
        DataType::Float4 | DataType::Real => Ok(ScalarType::Float32),
        DataType::Float8 | DataType::Double(_) | DataType::DoublePrecision => {
            Ok(ScalarType::Float64)
        }
        DataType::Numeric(info) | DataType::Decimal(info) | DataType::Dec(info) => {
            let (precision, scale) = decimal_info(info)?;
            Ok(ScalarType::Decimal { precision, scale })
        }
        DataType::Character(length) | DataType::Char(length) => Ok(ScalarType::Char {
            length: character_length(length)?,
        }),
        DataType::CharacterVarying(length) | DataType::Varchar(length) => Ok(ScalarType::Varchar {
            length: character_length(length)?,
        }),
        DataType::Text => Ok(ScalarType::Text),
        DataType::Bytea | DataType::Binary(_) => Ok(ScalarType::Binary),
        DataType::Date => Ok(ScalarType::Date),
        DataType::Time(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            Ok(ScalarType::Time)
        }
        DataType::Timestamp(_, timezone) => Ok(ScalarType::Timestamp {
            with_timezone: matches!(timezone, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz),
        }),
        DataType::JSON => Ok(ScalarType::Json),
        DataType::JSONB => Ok(ScalarType::Jsonb),
        DataType::Uuid => Ok(ScalarType::Uuid),
        DataType::Custom(name, modifiers) if name.to_string().eq_ignore_ascii_case("vector") => {
            let dimensions = match modifiers.as_slice() {
                [] => None,
                [dimension] => Some(dimension.parse::<usize>().map_err(|_| {
                    DbError::new(SYNTAX_ERROR, "VECTOR dimension must be a positive integer")
                })?),
                _ => return unsupported("VECTOR accepts at most one dimension"),
            };
            Ok(ScalarType::Vector { dimensions })
        }
        _ => unsupported("this SQL data type is not supported yet"),
    }
}

fn decimal_info(info: ExactNumberInfo) -> Result<(Option<u8>, Option<u8>)> {
    match info {
        ExactNumberInfo::None => Ok((None, None)),
        ExactNumberInfo::Precision(precision) => Ok((
            Some(
                u8::try_from(precision)
                    .map_err(|_| DbError::new(SYNTAX_ERROR, "decimal precision is out of range"))?,
            ),
            None,
        )),
        ExactNumberInfo::PrecisionAndScale(precision, scale) => {
            if scale < 0 {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "negative decimal scale is not supported",
                ));
            }
            Ok((
                Some(u8::try_from(precision).map_err(|_| {
                    DbError::new(SYNTAX_ERROR, "decimal precision is out of range")
                })?),
                Some(
                    u8::try_from(scale)
                        .map_err(|_| DbError::new(SYNTAX_ERROR, "decimal scale is out of range"))?,
                ),
            ))
        }
    }
}

fn character_length(length: Option<CharacterLength>) -> Result<Option<u32>> {
    match length {
        None | Some(CharacterLength::Max) => Ok(None),
        Some(CharacterLength::IntegerLength { length, .. }) => {
            Ok(Some(u32::try_from(length).map_err(|_| {
                DbError::new(SYNTAX_ERROR, "character length is out of range")
            })?))
        }
    }
}

fn convert_index_column(
    column: &sqlparser::ast::IndexColumn,
    sql: &str,
) -> Result<ParsedIdentifier> {
    if column.operator_class.is_some()
        || column.column.options.asc.is_some()
        || column.column.options.nulls_first.is_some()
        || column.column.with_fill.is_some()
    {
        return unsupported("extended index columns are not supported yet");
    }
    let SqlExpr::Identifier(ident) = &column.column.expr else {
        return unsupported("constraint columns must be simple identifiers");
    };
    Ok(convert_ident(ident.clone(), sql))
}

fn convert_object_name(name: ObjectName, sql: &str) -> Result<ParsedObjectName> {
    let parts = name
        .0
        .into_iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => Ok(convert_ident(ident, sql)),
            ObjectNamePart::Function(_) => unsupported("dynamic object names are not supported"),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ParsedObjectName { parts })
}

fn convert_single_identifier(name: ObjectName, sql: &str) -> Result<ParsedIdentifier> {
    let object = convert_object_name(name, sql)?;
    let [identifier] = object.parts.as_slice() else {
        return unsupported("qualified column lists are not supported");
    };
    Ok(identifier.clone())
}

fn convert_ident(ident: Ident, sql: &str) -> ParsedIdentifier {
    ParsedIdentifier {
        name: Identifier::new(ident.value, ident.quote_style.is_some()),
        position: span_position(sql, ident.span),
    }
}

fn bind_create_table(
    name: ParsedObjectName,
    columns: Vec<ParsedColumn>,
    catalog: &Catalog,
) -> Result<BoundStatement> {
    let (schema, table, position) = split_table_name(&name)?;
    if catalog.schema(&schema).is_none() {
        return Err(
            DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                .with_position_opt(position),
        );
    }
    if catalog.table(&schema, &table).is_some() {
        return Err(
            DbError::new("42P07", format!("table {schema}.{table} already exists"))
                .with_position_opt(position),
        );
    }
    let mut seen = BTreeSet::new();
    let columns = columns
        .into_iter()
        .map(|column| {
            if !seen.insert(column.name.name.clone()) {
                return Err(DbError::new(
                    "42701",
                    format!("column {} specified more than once", column.name.name),
                )
                .with_position_opt(column.name.position));
            }
            Ok(NewColumn {
                name: column.name.name,
                data_type: column.data_type,
                nullable: column.nullable,
                primary_key: column.primary_key,
                unique: column.unique,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundStatement::CreateTable {
        schema,
        name: table,
        columns,
    })
}

fn bind_insert(
    table_name: ParsedObjectName,
    columns: Vec<ParsedIdentifier>,
    rows: Vec<Vec<ParsedExpr>>,
    catalog: &Catalog,
) -> Result<BoundStatement> {
    let table = resolve_table(&table_name, catalog)?.clone();
    let column_indexes = if columns.is_empty() {
        (0..table.columns().len()).collect::<Vec<_>>()
    } else {
        let mut seen = BTreeSet::new();
        columns
            .into_iter()
            .map(|column| {
                let index = table.column_index(&column.name).ok_or_else(|| {
                    DbError::new(
                        UNDEFINED_COLUMN,
                        format!("column {} does not exist", column.name),
                    )
                    .with_position_opt(column.position)
                })?;
                if !seen.insert(index) {
                    return Err(DbError::new(
                        "42701",
                        format!("column {} specified more than once", column.name),
                    )
                    .with_position_opt(column.position));
                }
                Ok(index)
            })
            .collect::<Result<Vec<_>>>()?
    };
    if rows.is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "INSERT requires at least one row",
        ));
    }
    let rows = rows
        .into_iter()
        .map(|row| {
            if row.len() != column_indexes.len() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "INSERT has more target columns than expressions",
                ));
            }
            row.into_iter()
                .zip(&column_indexes)
                .map(|(expr, index)| {
                    bind_expr(expr, None, Some(&table.columns()[*index].data_type))
                })
                .collect()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundStatement::Insert {
        table_id: table.id,
        column_indexes,
        rows,
    })
}

fn bind_select(
    table_name: ParsedObjectName,
    projection: Vec<ParsedProjection>,
    filter: Option<ParsedExpr>,
    order_by: Vec<ParsedOrder>,
    limit: Option<ParsedExpr>,
    catalog: &Catalog,
) -> Result<BoundStatement> {
    let table = resolve_table(&table_name, catalog)?.clone();
    let mut bound_projection = Vec::new();
    for item in projection {
        match item {
            ParsedProjection::Wildcard => {
                for (index, column) in table.columns().iter().enumerate() {
                    bound_projection.push(BoundProjection {
                        expr: BoundExpr {
                            kind: BoundExprKind::Column { index },
                            data_type: column.data_type.clone(),
                            nullable: column.nullable,
                        },
                        field: Field::new(
                            column.name.as_str(),
                            column.data_type.clone(),
                            column.nullable,
                        ),
                    });
                }
            }
            ParsedProjection::Expression { expr, alias } => {
                let default_name = projection_name(&expr);
                let bound = bind_expr(expr, Some(&table), None)?;
                bound_projection.push(BoundProjection {
                    field: Field::new(
                        alias
                            .as_ref()
                            .map_or(default_name.as_str(), |alias| alias.name.as_str()),
                        bound.data_type.clone(),
                        bound.nullable,
                    ),
                    expr: bound,
                });
            }
        }
    }
    if bound_projection.is_empty() {
        return Err(DbError::new(SYNTAX_ERROR, "SELECT projection is empty"));
    }

    let filter = filter
        .map(|expr| bind_boolean_expr(expr, &table))
        .transpose()?;
    let order_by = order_by
        .into_iter()
        .map(|order| {
            let ParsedExprKind::Column(column) = order.expr.kind else {
                return unsupported_at(
                    "ORDER BY supports source columns only",
                    order.expr.position,
                );
            };
            Ok(BoundOrder {
                column_index: resolve_column(&column, &table)?,
                ascending: order.ascending,
                nulls_first: order.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let limit = limit
        .map(|expr| bind_expr(expr, Some(&table), Some(&ScalarType::Int64)))
        .transpose()?;
    let schema = Schema::new(
        bound_projection
            .iter()
            .map(|projection| projection.field.clone())
            .collect(),
    );
    Ok(BoundStatement::Select {
        table_id: table.id,
        schema,
        projection: bound_projection,
        filter,
        order_by,
        limit,
    })
}

fn bind_update(
    table_name: ParsedObjectName,
    assignments: Vec<(ParsedIdentifier, ParsedExpr)>,
    filter: Option<ParsedExpr>,
    catalog: &Catalog,
) -> Result<BoundStatement> {
    let table = resolve_table(&table_name, catalog)?.clone();
    let mut seen = BTreeSet::new();
    let assignments = assignments
        .into_iter()
        .map(|(column, expr)| {
            let index = table.column_index(&column.name).ok_or_else(|| {
                DbError::new(
                    UNDEFINED_COLUMN,
                    format!("column {} does not exist", column.name),
                )
                .with_position_opt(column.position)
            })?;
            if !seen.insert(index) {
                return Err(DbError::new(
                    "42701",
                    format!("column {} assigned more than once", column.name),
                )
                .with_position_opt(column.position));
            }
            Ok((
                index,
                bind_expr(expr, Some(&table), Some(&table.columns()[index].data_type))?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundStatement::Update {
        table_id: table.id,
        assignments,
        filter: filter
            .map(|expr| bind_boolean_expr(expr, &table))
            .transpose()?,
    })
}

fn bind_delete(
    table_name: ParsedObjectName,
    filter: Option<ParsedExpr>,
    catalog: &Catalog,
) -> Result<BoundStatement> {
    let table = resolve_table(&table_name, catalog)?.clone();
    Ok(BoundStatement::Delete {
        table_id: table.id,
        filter: filter
            .map(|expr| bind_boolean_expr(expr, &table))
            .transpose()?,
    })
}

fn bind_boolean_expr(expr: ParsedExpr, table: &TableDefinition) -> Result<BoundExpr> {
    let position = expr.position;
    let bound = bind_expr(expr, Some(table), Some(&ScalarType::Boolean))?;
    if bound.data_type != ScalarType::Boolean {
        return Err(DbError::new(DATATYPE_MISMATCH, "predicate must be boolean")
            .with_position_opt(position));
    }
    Ok(bound)
}

fn bind_expr(
    expr: ParsedExpr,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
) -> Result<BoundExpr> {
    let position = expr.position;
    match expr.kind {
        ParsedExprKind::Column(column) => {
            let table = table.ok_or_else(|| {
                DbError::new(UNDEFINED_COLUMN, "column reference is not valid here")
                    .with_position_opt(position)
            })?;
            let index = resolve_column(&column, table)?;
            let column = &table.columns()[index];
            if let Some(expected) = expected {
                ensure_types_compatible(&column.data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::Column { index },
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
        }
        ParsedExprKind::Literal(value) => bind_literal(value, expected, position),
        ParsedExprKind::Parameter(index) => {
            let data_type = expected.cloned().ok_or_else(|| {
                DbError::new(
                    INDETERMINATE_DATATYPE,
                    format!("could not determine data type of parameter ${index}"),
                )
                .with_position_opt(position)
            })?;
            Ok(BoundExpr {
                kind: BoundExprKind::Parameter { index },
                data_type,
                nullable: true,
            })
        }
        ParsedExprKind::Unary { op, expr } => match op {
            UnaryOperator::Not => {
                let expr = bind_expr(*expr, table, Some(&ScalarType::Boolean))?;
                if expr.data_type != ScalarType::Boolean {
                    return Err(
                        DbError::new(DATATYPE_MISMATCH, "NOT operand must be boolean")
                            .with_position_opt(position),
                    );
                }
                Ok(BoundExpr {
                    kind: BoundExprKind::Unary {
                        op,
                        expr: Box::new(expr),
                    },
                    data_type: ScalarType::Boolean,
                    nullable: true,
                })
            }
            UnaryOperator::Negate => {
                let expr = bind_expr(*expr, table, expected)?;
                if !is_numeric(&expr.data_type) {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        "unary minus requires a numeric operand",
                    )
                    .with_position_opt(position));
                }
                let data_type = expr.data_type.clone();
                Ok(BoundExpr {
                    kind: BoundExprKind::Unary {
                        op,
                        expr: Box::new(expr),
                    },
                    data_type,
                    nullable: false,
                })
            }
        },
        ParsedExprKind::Binary { left, op, right } => {
            bind_binary(*left, op, *right, table, position)
        }
    }
}

fn bind_binary(
    left: ParsedExpr,
    op: BinaryOperator,
    right: ParsedExpr,
    table: Option<&TableDefinition>,
    position: Option<usize>,
) -> Result<BoundExpr> {
    if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
        let left = bind_expr(left, table, Some(&ScalarType::Boolean))?;
        let right = bind_expr(right, table, Some(&ScalarType::Boolean))?;
        if left.data_type != ScalarType::Boolean || right.data_type != ScalarType::Boolean {
            return Err(DbError::new(
                DATATYPE_MISMATCH,
                "boolean operator operands must be boolean",
            )
            .with_position_opt(position));
        }
        return Ok(BoundExpr {
            kind: BoundExprKind::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: ScalarType::Boolean,
            nullable: true,
        });
    }

    let left_type = infer_expr_type(&left, table)?;
    let right_type = infer_expr_type(&right, table)?;
    let comparison_type = match (left_type, right_type) {
        (Some(left), Some(right)) => common_type(&left, &right).ok_or_else(|| {
            DbError::new(
                DATATYPE_MISMATCH,
                format!("cannot compare {left:?} with {right:?}"),
            )
            .with_position_opt(position)
        })?,
        (Some(data_type), None) | (None, Some(data_type)) => data_type,
        (None, None) => {
            return Err(DbError::new(
                INDETERMINATE_DATATYPE,
                "could not determine comparison operand types",
            )
            .with_position_opt(position));
        }
    };
    let left = bind_expr(left, table, Some(&comparison_type))?;
    let right = bind_expr(right, table, Some(&comparison_type))?;
    let nullable = left.nullable || right.nullable;
    Ok(BoundExpr {
        kind: BoundExprKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        data_type: ScalarType::Boolean,
        nullable,
    })
}

fn infer_expr_type(
    expr: &ParsedExpr,
    table: Option<&TableDefinition>,
) -> Result<Option<ScalarType>> {
    match &expr.kind {
        ParsedExprKind::Column(column) => {
            let table = table.ok_or_else(|| {
                DbError::new(UNDEFINED_COLUMN, "column reference is not valid here")
                    .with_position_opt(expr.position)
            })?;
            Ok(Some(
                table.columns()[resolve_column(column, table)?]
                    .data_type
                    .clone(),
            ))
        }
        ParsedExprKind::Literal(value) => Ok(value.scalar_type()),
        ParsedExprKind::Parameter(_) => Ok(None),
        ParsedExprKind::Unary { op, expr: inner } => match op {
            UnaryOperator::Not => Ok(Some(ScalarType::Boolean)),
            UnaryOperator::Negate => infer_expr_type(inner, table),
        },
        ParsedExprKind::Binary { .. } => Ok(Some(ScalarType::Boolean)),
    }
}

fn bind_literal(
    value: Value,
    expected: Option<&ScalarType>,
    position: Option<usize>,
) -> Result<BoundExpr> {
    let data_type = match expected {
        Some(expected) => {
            if !expected.accepts(&value) {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    format!("value cannot be assigned to {expected:?}"),
                )
                .with_position_opt(position));
            }
            expected.clone()
        }
        None => value.scalar_type().unwrap_or(ScalarType::Text),
    };
    Ok(BoundExpr {
        nullable: value.is_null(),
        kind: BoundExprKind::Literal(value),
        data_type,
    })
}

fn resolve_table<'a>(name: &ParsedObjectName, catalog: &'a Catalog) -> Result<&'a TableDefinition> {
    let (schema, table, position) = split_table_name(name)?;
    if catalog.schema(&schema).is_none() {
        return Err(
            DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                .with_position_opt(position),
        );
    }
    catalog.table(&schema, &table).ok_or_else(|| {
        DbError::new(
            UNDEFINED_TABLE,
            format!("relation {schema}.{table} does not exist"),
        )
        .with_position_opt(position)
    })
}

fn split_table_name(name: &ParsedObjectName) -> Result<(Identifier, Identifier, Option<usize>)> {
    match name.parts.as_slice() {
        [table] => Ok((
            Identifier::unquoted("public"),
            table.name.clone(),
            table.position,
        )),
        [schema, table] => Ok((
            schema.name.clone(),
            table.name.clone(),
            table.position.or(schema.position),
        )),
        _ => unsupported_at(
            "database-qualified names are not supported yet",
            name.parts.first().and_then(|part| part.position),
        ),
    }
}

fn resolve_column(name: &ParsedObjectName, table: &TableDefinition) -> Result<usize> {
    let (column, position) = match name.parts.as_slice() {
        [column] => (&column.name, column.position),
        [qualifier, column] if qualifier.name == table.name => (&column.name, column.position),
        [qualifier, _] => {
            return Err(DbError::new(
                UNDEFINED_TABLE,
                format!("invalid reference to table {}", qualifier.name),
            )
            .with_position_opt(qualifier.position));
        }
        _ => {
            return unsupported_at(
                "column references may contain at most a table qualifier",
                name.parts.first().and_then(|part| part.position),
            );
        }
    };
    table.column_index(column).ok_or_else(|| {
        DbError::new(UNDEFINED_COLUMN, format!("column {column} does not exist"))
            .with_position_opt(position)
    })
}

fn projection_name(expr: &ParsedExpr) -> String {
    if let ParsedExprKind::Column(column) = &expr.kind
        && let Some(column) = column.parts.last()
    {
        return column.name.as_str().to_owned();
    }
    "?column?".to_owned()
}

fn common_type(left: &ScalarType, right: &ScalarType) -> Option<ScalarType> {
    if left == right {
        return Some(left.clone());
    }
    if is_numeric(left) && is_numeric(right) {
        return Some(if numeric_rank(left) >= numeric_rank(right) {
            left.clone()
        } else {
            right.clone()
        });
    }
    if is_textual(left) && is_textual(right) {
        return Some(ScalarType::Text);
    }
    None
}

fn ensure_types_compatible(
    actual: &ScalarType,
    expected: &ScalarType,
    position: Option<usize>,
) -> Result<()> {
    if common_type(actual, expected).is_none() {
        return Err(DbError::new(
            DATATYPE_MISMATCH,
            format!("expected {expected:?}, found {actual:?}"),
        )
        .with_position_opt(position));
    }
    Ok(())
}

fn is_numeric(data_type: &ScalarType) -> bool {
    matches!(
        data_type,
        ScalarType::Int16
            | ScalarType::Int32
            | ScalarType::Int64
            | ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Decimal { .. }
    )
}

fn numeric_rank(data_type: &ScalarType) -> u8 {
    match data_type {
        ScalarType::Int16 => 1,
        ScalarType::Int32 => 2,
        ScalarType::Int64 => 3,
        ScalarType::Decimal { .. } => 4,
        ScalarType::Float32 => 5,
        ScalarType::Float64 => 6,
        _ => 0,
    }
}

fn is_textual(data_type: &ScalarType) -> bool {
    matches!(
        data_type,
        ScalarType::Char { .. } | ScalarType::Varchar { .. } | ScalarType::Text
    )
}

fn unsupported<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::new(FEATURE_NOT_SUPPORTED, message))
}

fn unsupported_at<T>(message: impl Into<String>, position: Option<usize>) -> Result<T> {
    Err(DbError::new(FEATURE_NOT_SUPPORTED, message).with_position_opt(position))
}

fn span_position(sql: &str, span: Span) -> Option<usize> {
    location_position(sql, span.start)
}

fn location_position(sql: &str, location: Location) -> Option<usize> {
    if location.line == 0 || location.column == 0 {
        return None;
    }
    let target_line = usize::try_from(location.line).ok()?;
    let target_column = usize::try_from(location.column).ok()?;
    let mut position = 1usize;
    for (line_index, line) in sql.split_inclusive('\n').enumerate() {
        if line_index + 1 == target_line {
            let column_offset = line.chars().take(target_column.saturating_sub(1)).count();
            return Some(position + column_offset);
        }
        position += line.chars().count();
    }
    None
}

fn parser_error_position(sql: &str, message: &str) -> Option<usize> {
    let marker = " at Line: ";
    let start = message.rfind(marker)? + marker.len();
    let (line, column) = message[start..].split_once(", Column: ")?;
    location_position(sql, Location::new(line.parse().ok()?, column.parse().ok()?))
}

trait DbErrorPosition {
    fn with_position_opt(self, position: Option<usize>) -> Self;
}

impl DbErrorPosition for DbError {
    fn with_position_opt(mut self, position: Option<usize>) -> Self {
        self.position = position;
        self
    }
}

#[must_use]
pub fn compare_scalar_types(left: &ScalarType, right: &ScalarType) -> Ordering {
    numeric_rank(left).cmp(&numeric_rank(right))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog_with_documents() -> Catalog {
        let mut catalog = Catalog::default();
        catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("documents"),
                vec![
                    NewColumn {
                        name: Identifier::unquoted("id"),
                        data_type: ScalarType::Int64,
                        nullable: false,
                        primary_key: true,
                        unique: true,
                    },
                    NewColumn {
                        name: Identifier::unquoted("title"),
                        data_type: ScalarType::Text,
                        nullable: false,
                        primary_key: false,
                        unique: false,
                    },
                ],
            )
            .expect("create documents");
        catalog
    }

    #[test]
    fn parses_and_binds_supported_postgres_subset() {
        let catalog = catalog_with_documents();
        let statement = parse(
            "SELECT id, title AS name FROM documents \
             WHERE id >= $1 AND title <> 'archived' ORDER BY id DESC LIMIT 5",
        )
        .expect("parse select");
        let bound = bind(statement, &catalog).expect("bind select");

        let BoundStatement::Select {
            projection,
            filter,
            order_by,
            limit,
            ..
        } = bound
        else {
            panic!("expected select");
        };
        assert_eq!(projection.len(), 2);
        assert_eq!(projection[1].field.name, "name");
        assert!(filter.is_some());
        assert_eq!(order_by[0].column_index, 0);
        assert!(!order_by[0].ascending);
        assert!(limit.is_some());
    }

    #[test]
    fn parses_create_table_constraints_and_normalizes_names() {
        let statement = parse(
            "CREATE TABLE Audit.Events (\
                id BIGINT PRIMARY KEY,\
                code VARCHAR(24) UNIQUE,\
                payload JSONB NOT NULL\
            )",
        )
        .expect("parse create table");
        let ParsedStatement::CreateTable { name, columns } = statement else {
            panic!("expected create table");
        };
        assert_eq!(name.parts[0].name.as_str(), "audit");
        assert!(columns[0].primary_key);
        assert!(columns[1].unique);
        assert!(!columns[2].nullable);
    }

    #[test]
    fn reports_unknown_objects_columns_and_type_mismatches() {
        let catalog = catalog_with_documents();

        let error = bind(parse("SELECT id FROM missing").expect("parse"), &catalog)
            .expect_err("unknown table");
        assert_eq!(error.sql_state, UNDEFINED_TABLE);

        let error = bind(
            parse("SELECT missing FROM documents").expect("parse"),
            &catalog,
        )
        .expect_err("unknown column");
        assert_eq!(error.sql_state, UNDEFINED_COLUMN);
        assert!(error.position.is_some());

        let error = bind(
            parse("INSERT INTO documents (id, title) VALUES ('bad', 'title')").expect("parse"),
            &catalog,
        )
        .expect_err("type mismatch");
        assert_eq!(error.sql_state, DATATYPE_MISMATCH);

        let error = bind(
            parse("INSERT INTO documents (id, title) VALUES (id, 'title')").expect("parse"),
            &catalog,
        )
        .expect_err("column is not visible in VALUES");
        assert_eq!(error.sql_state, UNDEFINED_COLUMN);
    }

    #[test]
    fn rejects_unsupported_syntax_without_panicking() {
        let catalog = catalog_with_documents();
        for sql in [
            "SELECT * FROM documents d JOIN documents e ON d.id = e.id",
            "WITH d AS (SELECT * FROM documents) SELECT * FROM d",
            "SELECT COUNT(*) FROM documents",
            "CREATE VIEW docs AS SELECT * FROM documents",
            "CREATE TABLE composite (a BIGINT, b BIGINT, PRIMARY KEY (a, b))",
        ] {
            let error = parse(sql)
                .and_then(|statement| bind(statement, &catalog))
                .expect_err("unsupported syntax");
            assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED, "{sql}");
        }
    }

    #[test]
    fn exposes_parser_error_positions() {
        let error = parse("SELECT *\nFROM documents WHERE = 1").expect_err("invalid SQL");
        assert_eq!(error.sql_state, SYNTAX_ERROR);
        assert!(error.position.is_some(), "{error:?}");
    }

    #[test]
    fn requires_parameter_type_context() {
        let catalog = catalog_with_documents();
        let error = bind(parse("SELECT $1 FROM documents").expect("parse"), &catalog)
            .expect_err("unknown parameter type");
        assert_eq!(error.sql_state, INDETERMINATE_DATATYPE);
    }
}
