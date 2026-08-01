//! Multi-dialect parsing and catalog-aware binding for OrdaDB.
//!
//! The public syntax tree in this crate is owned by OrdaDB. `sqlparser` is an
//! implementation detail. Every accepted source dialect is normalized into
//! OrdaDB's PostgreSQL-compatible semantics before binding.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use ordadb_catalog::{
    Catalog, CatalogExpression, CatalogObjectRef, DropBehavior, FullTextAnalyzer, IndexMethod,
    IndexOptions, NewColumn, NewConstraint, NewConstraintKind, NewIndex, NewSequence,
    ReferentialAction, RoutineArgument, RoutineKind, TableDefinition,
    TriggerEvent as CatalogTriggerEvent, TriggerTiming, VectorDistanceMetric, ViewDefinition,
    ViewKind, indexable_type, text_search_type,
};
use ordadb_transaction::{IsolationLevel, TransactionAccessMode, TransactionCharacteristics};
use ordadb_types::{
    ColumnId, ConstraintId, DbError, Field, Identifier, IndexId, Result, RoutineId, ScalarType,
    Schema, SchemaId, SequenceId, TableId, TriggerId, Value, ViewId,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    AlterColumnOperation as SqlAlterColumnOperation, AlterIndexOperation,
    AlterSchemaOperation as SqlAlterSchemaOperation, AlterTable,
    AlterTableOperation as SqlAlterTableOperation, ArgMode, AssignmentTarget, BeginTransactionKind,
    BinaryOperator as SqlBinaryOperator, CharacterLength, ColumnDef, ColumnOption,
    CreateFunction as SqlCreateFunction, CreateFunctionBody, CreateTable, CreateTableOptions,
    CreateTrigger as SqlCreateTrigger, CreateView, DataType, DropBehavior as SqlDropBehavior,
    ExactNumberInfo, Expr as SqlExpr, FromTable, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, FunctionReturnType, FunctionSecurity, GroupByExpr, Ident, IndexType,
    JoinConstraint, JoinOperator, LimitClause, ObjectName, ObjectNamePart, ObjectType, OrderByKind,
    Query, ReferentialAction as SqlReferentialAction, RenameTableNameKind, SchemaName, Select,
    SelectItem, SequenceOptions, SetExpr, Spanned, Statement as SqlStatement, TableAlias,
    TableConstraint, TableFactor, TableObject, TableWithJoins, TimezoneInfo, TopQuantity,
    TransactionAccessMode as SqlTransactionAccessMode,
    TransactionIsolationLevel as SqlTransactionIsolationLevel, TransactionMode,
    TriggerEvent as SqlTriggerEvent, TriggerExecBodyType, TriggerObject, TriggerObjectKind,
    TriggerPeriod, UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::{Dialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Span, Token, TokenWithSpan, Tokenizer};

const FEATURE_NOT_SUPPORTED: &str = "0A000";
const SYNTAX_ERROR: &str = "42601";
const UNDEFINED_SCHEMA: &str = "3F000";
const UNDEFINED_TABLE: &str = "42P01";
const UNDEFINED_COLUMN: &str = "42703";
const DATATYPE_MISMATCH: &str = "42804";
const INDETERMINATE_DATATYPE: &str = "42P18";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlDialect {
    #[default]
    #[serde(rename = "postgresql")]
    PostgreSql,
    #[serde(rename = "mysql")]
    MySql,
    #[serde(rename = "sqlite")]
    Sqlite,
    #[serde(rename = "sqlServer")]
    SqlServer,
}

impl SqlDialect {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::PostgreSql => "PostgreSQL",
            Self::MySql => "MySQL",
            Self::Sqlite => "SQLite",
            Self::SqlServer => "SQL Server",
        }
    }
}

impl fmt::Display for SqlDialect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

impl FromStr for SqlDialect {
    type Err = DbError;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "postgres" | "postgresql" => Ok(Self::PostgreSql),
            "mysql" => Ok(Self::MySql),
            "sqlite" | "sqlite3" => Ok(Self::Sqlite),
            "mssql" | "sqlserver" | "sql-server" => Ok(Self::SqlServer),
            _ => Err(
                DbError::new("22023", format!("unknown SQL dialect {value}"))
                    .with_hint("Use postgresql, mysql, sqlite, or sqlserver."),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedIdentifier {
    pub name: Identifier,
    pub position: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedObjectName {
    pub parts: Vec<ParsedIdentifier>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedIndexOption {
    pub name: ParsedIdentifier,
    pub value: ParsedIndexOptionValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedIndexOptionValue {
    Text(String),
    Integer(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCreateIndex {
    pub name: ParsedIdentifier,
    pub table: ParsedObjectName,
    pub key_columns: Vec<ParsedIdentifier>,
    pub include_columns: Vec<ParsedIdentifier>,
    pub unique: bool,
    pub method: IndexMethod,
    pub options: Vec<ParsedIndexOption>,
    pub if_not_exists: bool,
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
    Aggregate {
        function: AggregateFunction,
        argument: Option<Box<ParsedExpr>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
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
    pub default: Option<ParsedDefault>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedDefault {
    pub expression: ParsedExpr,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedTableConstraint {
    PrimaryKey {
        name: Option<ParsedIdentifier>,
        columns: Vec<ParsedIdentifier>,
    },
    Unique {
        name: Option<ParsedIdentifier>,
        columns: Vec<ParsedIdentifier>,
    },
    Check {
        name: Option<ParsedIdentifier>,
        expression: ParsedExpr,
        sql: String,
    },
    ForeignKey {
        name: Option<ParsedIdentifier>,
        columns: Vec<ParsedIdentifier>,
        referenced_table: ParsedObjectName,
        referenced_columns: Vec<ParsedIdentifier>,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlObjectKind {
    Schema,
    Table,
    Index,
    Sequence,
    View,
    MaterializedView,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedAlterTableOperation {
    RenameTable {
        new_name: ParsedIdentifier,
    },
    RenameColumn {
        old_name: ParsedIdentifier,
        new_name: ParsedIdentifier,
    },
    AddColumn {
        column: ParsedColumn,
        if_not_exists: bool,
    },
    DropColumns {
        columns: Vec<ParsedIdentifier>,
        if_exists: bool,
        behavior: DropBehavior,
    },
    SetNotNull {
        column: ParsedIdentifier,
    },
    DropNotNull {
        column: ParsedIdentifier,
    },
    SetDefault {
        column: ParsedIdentifier,
        default: ParsedDefault,
    },
    DropDefault {
        column: ParsedIdentifier,
    },
    SetDataType {
        column: ParsedIdentifier,
        data_type: ScalarType,
    },
    AddConstraint {
        constraint: ParsedTableConstraint,
    },
    DropConstraint {
        name: ParsedIdentifier,
        if_exists: bool,
        behavior: DropBehavior,
    },
    SetTriggerEnabled {
        name: ParsedIdentifier,
        enabled: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundAlterTableOperation {
    RenameTable {
        new_name: Identifier,
    },
    RenameColumn {
        column_id: ColumnId,
        new_name: Identifier,
    },
    AddColumn {
        column: NewColumn,
        if_not_exists: bool,
    },
    DropColumns {
        column_ids: Vec<ColumnId>,
        if_exists: bool,
        behavior: DropBehavior,
    },
    SetNotNull {
        column_id: ColumnId,
    },
    DropNotNull {
        column_id: ColumnId,
    },
    SetDefault {
        column_id: ColumnId,
        default: CatalogExpression,
    },
    DropDefault {
        column_id: ColumnId,
    },
    SetDataType {
        column_id: ColumnId,
        data_type: ScalarType,
    },
    AddConstraint {
        constraint: NewConstraint,
    },
    DropConstraint {
        constraint_id: Option<ConstraintId>,
        if_exists: bool,
        behavior: DropBehavior,
    },
    SetTriggerEnabled {
        trigger_id: Option<TriggerId>,
        name: Identifier,
        enabled: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransactionChain {
    #[default]
    Default,
    Chain,
    NoChain,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedTable {
    pub name: ParsedObjectName,
    pub alias: Option<ParsedIdentifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedJoin {
    pub table: ParsedTable,
    pub kind: JoinKind,
    pub on: ParsedExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedStatement {
    Begin {
        characteristics: TransactionCharacteristics,
    },
    Commit {
        chain: TransactionChain,
    },
    Rollback {
        chain: TransactionChain,
    },
    Savepoint {
        name: ParsedIdentifier,
    },
    RollbackTo {
        name: ParsedIdentifier,
    },
    ReleaseSavepoint {
        name: ParsedIdentifier,
    },
    Analyze {
        table: Option<ParsedObjectName>,
    },
    Vacuum {
        table: Option<ParsedObjectName>,
        analyze: bool,
    },
    CreateSchema {
        name: ParsedIdentifier,
        if_not_exists: bool,
    },
    AlterSchemaRename {
        name: ParsedIdentifier,
        new_name: ParsedIdentifier,
        if_exists: bool,
    },
    DropObjects {
        kind: DdlObjectKind,
        names: Vec<ParsedObjectName>,
        if_exists: bool,
        behavior: DropBehavior,
    },
    CreateTable {
        name: ParsedObjectName,
        columns: Vec<ParsedColumn>,
        constraints: Vec<ParsedTableConstraint>,
        if_not_exists: bool,
    },
    AlterTable {
        name: ParsedObjectName,
        if_exists: bool,
        operations: Vec<ParsedAlterTableOperation>,
    },
    CreateIndex(ParsedCreateIndex),
    AlterIndexRename {
        name: ParsedObjectName,
        new_name: ParsedIdentifier,
    },
    CreateSequence {
        name: ParsedObjectName,
        sequence: NewSequence,
        if_not_exists: bool,
        owner: Option<(ParsedObjectName, ParsedIdentifier)>,
    },
    AlterSequenceRename {
        name: ParsedObjectName,
        if_exists: bool,
        new_name: ParsedIdentifier,
    },
    AlterSequence {
        name: ParsedObjectName,
        if_exists: bool,
        options: ParsedAlterSequence,
    },
    CreateView {
        name: ParsedObjectName,
        kind: ViewKind,
        query: Box<ParsedStatement>,
        query_sql: String,
        columns: Vec<ParsedIdentifier>,
        replace: bool,
        if_not_exists: bool,
        with_data: bool,
    },
    AlterViewRename {
        name: ParsedObjectName,
        kind: ViewKind,
        if_exists: bool,
        new_name: ParsedIdentifier,
    },
    RefreshMaterializedView {
        name: ParsedObjectName,
        with_data: bool,
    },
    CreateRoutine {
        name: ParsedObjectName,
        kind: RoutineKind,
        arguments: Vec<RoutineArgument>,
        return_type: Option<ScalarType>,
        returns_set: bool,
        language: String,
        body: String,
        replace: bool,
    },
    DropRoutine {
        name: ParsedObjectName,
        kind: RoutineKind,
        argument_types: Option<Vec<ScalarType>>,
        if_exists: bool,
        behavior: DropBehavior,
    },
    Call {
        name: ParsedObjectName,
        arguments: Vec<ParsedExpr>,
    },
    RoutineSelect {
        name: ParsedObjectName,
        arguments: Vec<ParsedExpr>,
        alias: Option<ParsedIdentifier>,
    },
    SequenceValue {
        name: ParsedObjectName,
        operation: ParsedSequenceOperation,
        alias: Option<ParsedIdentifier>,
    },
    CreateTrigger {
        name: ParsedIdentifier,
        table: ParsedObjectName,
        timing: TriggerTiming,
        events: Vec<CatalogTriggerEvent>,
        routine: ParsedObjectName,
    },
    DropTrigger {
        name: ParsedIdentifier,
        table: ParsedObjectName,
        if_exists: bool,
        behavior: DropBehavior,
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
    AdvancedSelect {
        table: ParsedTable,
        joins: Vec<ParsedJoin>,
        projection: Vec<ParsedProjection>,
        filter: Option<ParsedExpr>,
        group_by: Vec<ParsedExpr>,
        having: Option<ParsedExpr>,
        order_by: Vec<ParsedOrder>,
        limit: Option<ParsedExpr>,
    },
    Explain {
        statement: Box<ParsedStatement>,
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
pub enum ParsedSequenceOperation {
    NextValue,
    CurrentValue,
    SetValue { value: ParsedExpr, is_called: bool },
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ParsedAlterSequence {
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub restart: Option<i64>,
    pub cycle: Option<bool>,
    pub owner: Option<Option<(ParsedObjectName, ParsedIdentifier)>>,
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
    Aggregate {
        function: AggregateFunction,
        argument: Option<Box<BoundExpr>>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundTable {
    pub table_id: TableId,
    pub binding: Identifier,
    pub offset: usize,
    pub width: usize,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundJoin {
    pub table: BoundTable,
    pub kind: JoinKind,
    pub on: BoundExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundStatement {
    NoOp {
        tag: String,
    },
    Begin {
        characteristics: TransactionCharacteristics,
    },
    Commit {
        chain: TransactionChain,
    },
    Rollback {
        chain: TransactionChain,
    },
    Savepoint {
        name: Identifier,
    },
    RollbackTo {
        name: Identifier,
    },
    ReleaseSavepoint {
        name: Identifier,
    },
    Analyze {
        table_id: Option<TableId>,
    },
    Vacuum {
        table_id: Option<TableId>,
        analyze: bool,
    },
    CreateSchema {
        name: Identifier,
        if_not_exists: bool,
    },
    AlterSchemaRename {
        schema_id: SchemaId,
        new_name: Identifier,
    },
    DropObjects {
        kind: DdlObjectKind,
        objects: Vec<CatalogObjectRef>,
        behavior: DropBehavior,
    },
    CreateTable {
        schema: Identifier,
        name: Identifier,
        columns: Vec<NewColumn>,
        constraints: Vec<NewConstraint>,
        if_not_exists: bool,
    },
    AlterTable {
        table_id: TableId,
        operations: Vec<BoundAlterTableOperation>,
    },
    CreateIndex {
        table_id: TableId,
        index: NewIndex,
        if_not_exists: bool,
    },
    AlterIndexRename {
        index_id: IndexId,
        new_name: Identifier,
    },
    CreateSequence {
        schema: Identifier,
        sequence: NewSequence,
        if_not_exists: bool,
    },
    AlterSequenceRename {
        sequence_id: SequenceId,
        new_name: Identifier,
    },
    AlterSequence {
        sequence_id: SequenceId,
        increment: Option<i64>,
        min_value: Option<i64>,
        max_value: Option<i64>,
        restart: Option<i64>,
        cycle: Option<bool>,
        owner: Option<Option<(TableId, ordadb_types::ColumnId)>>,
    },
    CreateView {
        schema: Identifier,
        name: Identifier,
        kind: ViewKind,
        query: Box<BoundStatement>,
        query_sql: String,
        output: Schema,
        references: Vec<CatalogObjectRef>,
        replace: bool,
        if_not_exists: bool,
        with_data: bool,
        existing: Option<ViewId>,
    },
    AlterViewRename {
        view_id: ViewId,
        new_name: Identifier,
    },
    RefreshMaterializedView {
        view_id: ViewId,
        table_id: TableId,
        query: Box<BoundStatement>,
        with_data: bool,
    },
    CreateRoutine {
        schema: Identifier,
        name: Identifier,
        kind: RoutineKind,
        arguments: Vec<RoutineArgument>,
        return_type: Option<ScalarType>,
        returns_set: bool,
        language: String,
        body: String,
        replace: bool,
    },
    DropRoutine {
        routine_id: RoutineId,
        behavior: DropBehavior,
    },
    Call {
        routine_id: RoutineId,
        arguments: Vec<BoundExpr>,
    },
    RoutineSelect {
        routine_id: RoutineId,
        arguments: Vec<BoundExpr>,
        schema: Schema,
        returns_set: bool,
    },
    SequenceValue {
        sequence_id: SequenceId,
        operation: BoundSequenceOperation,
        schema: Schema,
    },
    CreateTrigger {
        table_id: TableId,
        name: Identifier,
        timing: TriggerTiming,
        events: Vec<CatalogTriggerEvent>,
        routine_id: RoutineId,
    },
    DropTrigger {
        trigger_id: TriggerId,
        behavior: DropBehavior,
    },
    ViewSelect {
        view_id: ViewId,
        source: Box<BoundStatement>,
        schema: Schema,
        projection: Vec<usize>,
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
    AdvancedSelect {
        table: BoundTable,
        joins: Vec<BoundJoin>,
        schema: Schema,
        projection: Vec<BoundProjection>,
        filter: Option<BoundExpr>,
        group_by: Vec<BoundExpr>,
        having: Option<BoundExpr>,
        order_by: Vec<BoundOrder>,
        limit: Option<BoundExpr>,
        aggregate: bool,
    },
    Explain {
        statement: Box<BoundStatement>,
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

#[derive(Debug, Clone, PartialEq)]
pub enum BoundSequenceOperation {
    NextValue,
    CurrentValue { value: Option<i64> },
    SetValue { value: BoundExpr, is_called: bool },
}

/// Parse exactly one statement using PostgreSQL dialect rules.
pub fn parse(sql: &str) -> Result<ParsedStatement> {
    parse_with_dialect(sql, SqlDialect::PostgreSql)
}

/// Parse exactly one statement using the selected source dialect and lower it
/// into OrdaDB's PostgreSQL-compatible syntax tree.
pub fn parse_with_dialect(sql: &str, dialect: SqlDialect) -> Result<ParsedStatement> {
    if dialect == SqlDialect::PostgreSql {
        if let Some(statement) = parse_vacuum_analyze(sql)? {
            return Ok(statement);
        }
        if let Some(statement) = parse_transaction_begin(sql)? {
            return Ok(statement);
        }
        if let Some(statement) = parse_create_procedure(sql)? {
            return Ok(statement);
        }
        if let Some(statement) = parse_alter_view(sql)? {
            return Ok(statement);
        }
        if let Some(statement) = parse_alter_sequence(sql)? {
            return Ok(statement);
        }
        if let Some(statement) = parse_refresh_materialized_view(sql)? {
            return Ok(statement);
        }
    }
    let mut statements = match parse_source_statements(sql, dialect) {
        Ok(statements) => statements,
        Err(error) => {
            let message = error.to_string();
            let position = parser_error_position(sql, &message);
            let mut error = DbError::new(SYNTAX_ERROR, message);
            error.position = position;
            return Err(error);
        }
    };

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
    .map_err(|error| dialect_error(error, dialect))
}

fn parse_source_statements(
    sql: &str,
    dialect: SqlDialect,
) -> std::result::Result<Vec<SqlStatement>, ParserError> {
    match dialect {
        SqlDialect::PostgreSql => {
            let parser_sql = materialized_view_parser_sql(sql);
            Parser::parse_sql(&PostgreSqlDialect {}, &parser_sql)
        }
        SqlDialect::MySql => {
            parse_tokenized_source(sql, &MySqlDialect {}, ParameterStyle::QuestionMark)
        }
        SqlDialect::Sqlite => {
            parse_tokenized_source(sql, &SQLiteDialect {}, ParameterStyle::QuestionMark)
        }
        SqlDialect::SqlServer => {
            parse_tokenized_source(sql, &MsSqlDialect {}, ParameterStyle::NamedAtP)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParameterStyle {
    QuestionMark,
    NamedAtP,
}

fn parse_tokenized_source(
    sql: &str,
    dialect: &dyn Dialect,
    parameter_style: ParameterStyle,
) -> std::result::Result<Vec<SqlStatement>, ParserError> {
    let mut parameter_index = 1usize;
    let mut tokens = Vec::<TokenWithSpan>::new();
    Tokenizer::new(dialect, sql).tokenize_with_location_into_buf_with_mapper(
        &mut tokens,
        |mut token| {
            if parameter_style == ParameterStyle::QuestionMark
                && matches!(&token.token, Token::Placeholder(value) if value == "?")
            {
                token.token = Token::Placeholder(format!("${parameter_index}"));
                parameter_index = parameter_index.saturating_add(1);
            }
            token
        },
    )?;
    Parser::new(dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
}

fn dialect_error(mut error: DbError, dialect: SqlDialect) -> DbError {
    if dialect != SqlDialect::PostgreSql && error.sql_state == FEATURE_NOT_SUPPORTED {
        error.message = format!(
            "{} feature is not supported: {}",
            dialect.label(),
            error.message
        );
        if error.hint.is_none() {
            error.hint =
                Some("Rewrite the statement using the OrdaDB/PostgreSQL-compatible subset.".into());
        }
    }
    error
}

fn parse_create_procedure(sql: &str) -> Result<Option<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let uppercase = trimmed.to_ascii_uppercase();
    let (header_prefix, replace) = if uppercase.starts_with("CREATE OR REPLACE PROCEDURE ") {
        ("CREATE OR REPLACE PROCEDURE ", true)
    } else if uppercase.starts_with("CREATE PROCEDURE ") {
        ("CREATE PROCEDURE ", false)
    } else {
        return Ok(None);
    };
    let after_prefix = &trimmed[header_prefix.len()..];
    let open = after_prefix
        .find('(')
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "CREATE PROCEDURE requires arguments"))?;
    let close = after_prefix[open + 1..]
        .find(')')
        .map(|position| position + open + 1)
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "unterminated procedure argument list"))?;
    let name = parse_simple_object_name(after_prefix[..open].trim())?;
    let arguments = parse_procedure_arguments(&after_prefix[open + 1..close])?;
    let tail = after_prefix[close + 1..].trim();
    let (as_position, body_position) = keyword_span(tail, "AS")
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "CREATE PROCEDURE requires AS body"))?;
    let options = tail[..as_position].trim();
    if !options.is_empty()
        && !matches_keyword_sequence(options, &["LANGUAGE", "plpgsql"])
        && !matches_keyword_sequence(options, &["LANGUAGE", "plpgsql", "SECURITY", "INVOKER"])
    {
        return unsupported("only LANGUAGE plpgsql SECURITY INVOKER procedures are supported");
    }
    let body_start = tail[body_position..].trim();
    let body = parse_dollar_quoted_body(body_start)?;
    Ok(Some(ParsedStatement::CreateRoutine {
        name,
        kind: RoutineKind::Procedure,
        arguments,
        return_type: None,
        returns_set: false,
        language: "plpgsql".to_owned(),
        body,
        replace,
    }))
}

fn keyword_span(value: &str, keyword: &str) -> Option<(usize, usize)> {
    let mut start = None;
    for (position, character) in value.char_indices() {
        if character.is_whitespace() {
            if let Some(token_start) = start.take()
                && value[token_start..position].eq_ignore_ascii_case(keyword)
            {
                return Some((token_start, position));
            }
        } else if start.is_none() {
            start = Some(position);
        }
    }
    let token_start = start?;
    value[token_start..]
        .eq_ignore_ascii_case(keyword)
        .then_some((token_start, value.len()))
}

fn matches_keyword_sequence(value: &str, expected: &[&str]) -> bool {
    let mut actual = value.split_whitespace();
    expected.iter().all(|keyword| {
        actual
            .next()
            .is_some_and(|value| value.eq_ignore_ascii_case(keyword))
    }) && actual.next().is_none()
}

fn parse_alter_view(sql: &str) -> Result<Option<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let tokens = significant_tokens(trimmed);
    if tokens.len() < 3
        || !tokens
            .first()
            .is_some_and(|token| is_unquoted_word(token, "ALTER"))
    {
        return Ok(None);
    }
    let mut cursor = 1usize;
    let kind = if tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "MATERIALIZED"))
    {
        cursor += 1;
        if !tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "VIEW"))
        {
            return Ok(None);
        }
        cursor += 1;
        ViewKind::Materialized
    } else if tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "VIEW"))
    {
        cursor += 1;
        ViewKind::Regular
    } else {
        return Ok(None);
    };
    let if_exists = if tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "IF"))
        && tokens
            .get(cursor + 1)
            .is_some_and(|token| is_unquoted_word(token, "EXISTS"))
    {
        cursor += 2;
        true
    } else {
        false
    };
    let name = parse_token_object_name(&tokens, &mut cursor, 2)?;
    if !tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "RENAME"))
    {
        return unsupported("only ALTER VIEW ... RENAME TO is supported");
    }
    cursor += 1;
    expect_keyword(&tokens, &mut cursor, "TO")?;
    let new_name = parse_token_identifier(&tokens, &mut cursor)?;
    ensure_token_end(&tokens, cursor)?;
    Ok(Some(ParsedStatement::AlterViewRename {
        name,
        kind,
        if_exists,
        new_name,
    }))
}

fn parse_procedure_arguments(value: &str) -> Result<Vec<RoutineArgument>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|argument| {
            let parts = argument.split_whitespace().collect::<Vec<_>>();
            let (name, data_type) = match parts.as_slice() {
                [data_type] => (None, *data_type),
                [name, data_type] => (Some(Identifier::unquoted(*name)), *data_type),
                [name, first, second] if first.eq_ignore_ascii_case("DOUBLE") => (
                    Some(Identifier::unquoted(*name)),
                    if second.eq_ignore_ascii_case("PRECISION") {
                        "DOUBLE PRECISION"
                    } else {
                        return unsupported("unsupported procedure argument type");
                    },
                ),
                _ => return unsupported("unsupported procedure argument declaration"),
            };
            Ok(RoutineArgument {
                name,
                data_type: parse_simple_scalar_type(data_type)?,
            })
        })
        .collect()
}

fn parse_simple_scalar_type(value: &str) -> Result<ScalarType> {
    match value.to_ascii_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => Ok(ScalarType::Boolean),
        "SMALLINT" | "INT2" => Ok(ScalarType::Int16),
        "INT" | "INTEGER" | "INT4" => Ok(ScalarType::Int32),
        "BIGINT" | "INT8" => Ok(ScalarType::Int64),
        "REAL" | "FLOAT4" => Ok(ScalarType::Float32),
        "DOUBLE PRECISION" | "FLOAT8" => Ok(ScalarType::Float64),
        "TEXT" => Ok(ScalarType::Text),
        "UUID" => Ok(ScalarType::Uuid),
        "JSON" => Ok(ScalarType::Json),
        "JSONB" => Ok(ScalarType::Jsonb),
        _ => unsupported(format!("procedure argument type {value} is not supported")),
    }
}

fn parse_dollar_quoted_body(value: &str) -> Result<String> {
    if !value.starts_with('$') {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "procedure body must be dollar quoted",
        ));
    }
    let delimiter_end = value[1..]
        .find('$')
        .map(|position| position + 1)
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "invalid dollar-quote delimiter"))?;
    let delimiter = &value[..=delimiter_end];
    if !delimiter[1..delimiter.len() - 1]
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(DbError::new(SYNTAX_ERROR, "invalid dollar-quote delimiter"));
    }
    let body_start = delimiter.len();
    let body_end = value[body_start..]
        .rfind(delimiter)
        .map(|position| position + body_start)
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "unterminated dollar-quoted body"))?;
    if !value[body_end + delimiter.len()..].trim().is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "unexpected SQL after procedure body",
        ));
    }
    Ok(value[body_start..body_end].to_owned())
}

fn parse_alter_sequence(sql: &str) -> Result<Option<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let tokens = significant_tokens(trimmed);
    if tokens.len() < 3
        || !tokens
            .first()
            .is_some_and(|token| is_unquoted_word(token, "ALTER"))
        || !tokens
            .get(1)
            .is_some_and(|token| is_unquoted_word(token, "SEQUENCE"))
    {
        return Ok(None);
    }
    let mut cursor = 2usize;
    let if_exists = if tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "IF"))
        && tokens
            .get(cursor + 1)
            .is_some_and(|token| is_unquoted_word(token, "EXISTS"))
    {
        cursor += 2;
        true
    } else {
        false
    };
    let name = parse_token_object_name(&tokens, &mut cursor, 2)?;
    if tokens
        .get(cursor)
        .is_some_and(|token| is_unquoted_word(token, "RENAME"))
    {
        cursor += 1;
        expect_keyword(&tokens, &mut cursor, "TO")?;
        let new_name = parse_token_identifier(&tokens, &mut cursor)?;
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::AlterSequenceRename {
            name,
            if_exists,
            new_name,
        }));
    }

    let mut options = ParsedAlterSequence::default();
    let mut seen = BTreeSet::new();
    while cursor < tokens.len() {
        if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "INCREMENT"))
        {
            unique_sequence_option(&mut seen, "INCREMENT")?;
            cursor += 1;
            if tokens
                .get(cursor)
                .is_some_and(|token| is_unquoted_word(token, "BY"))
            {
                cursor += 1;
            }
            options.increment = Some(parse_signed_i64(&tokens, &mut cursor)?);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "MINVALUE"))
        {
            unique_sequence_option(&mut seen, "MINVALUE")?;
            cursor += 1;
            options.min_value = Some(parse_signed_i64(&tokens, &mut cursor)?);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "MAXVALUE"))
        {
            unique_sequence_option(&mut seen, "MAXVALUE")?;
            cursor += 1;
            options.max_value = Some(parse_signed_i64(&tokens, &mut cursor)?);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "RESTART"))
        {
            unique_sequence_option(&mut seen, "RESTART")?;
            cursor += 1;
            if tokens
                .get(cursor)
                .is_some_and(|token| is_unquoted_word(token, "WITH"))
            {
                cursor += 1;
            }
            if cursor >= tokens.len() || alter_sequence_option_starts(&tokens[cursor]) {
                return unsupported("ALTER SEQUENCE RESTART requires an explicit value");
            }
            options.restart = Some(parse_signed_i64(&tokens, &mut cursor)?);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "CYCLE"))
        {
            unique_sequence_option(&mut seen, "CYCLE")?;
            cursor += 1;
            options.cycle = Some(true);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "NO"))
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| is_unquoted_word(token, "CYCLE"))
        {
            unique_sequence_option(&mut seen, "CYCLE")?;
            cursor += 2;
            options.cycle = Some(false);
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "OWNED"))
        {
            unique_sequence_option(&mut seen, "OWNED")?;
            cursor += 1;
            expect_keyword(&tokens, &mut cursor, "BY")?;
            if tokens
                .get(cursor)
                .is_some_and(|token| is_unquoted_word(token, "NONE"))
            {
                cursor += 1;
                options.owner = Some(None);
            } else {
                let mut owner = parse_token_object_name(&tokens, &mut cursor, 3)?;
                if owner.parts.len() < 2 {
                    return Err(DbError::new(
                        SYNTAX_ERROR,
                        "OWNED BY requires table.column or schema.table.column",
                    ));
                }
                let column = owner
                    .parts
                    .pop()
                    .ok_or_else(|| DbError::internal("validated sequence owner lost its column"))?;
                options.owner = Some(Some((owner, column)));
            }
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "NO"))
        {
            return unsupported("NO MINVALUE and NO MAXVALUE are not supported yet");
        } else {
            return unsupported(format!(
                "this ALTER SEQUENCE option is not supported: {}",
                tokens[cursor]
            ));
        }
    }
    if seen.is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "ALTER SEQUENCE requires an option or RENAME TO",
        ));
    }
    Ok(Some(ParsedStatement::AlterSequence {
        name,
        if_exists,
        options,
    }))
}

fn parse_token_object_name(
    tokens: &[Token],
    cursor: &mut usize,
    max_parts: usize,
) -> Result<ParsedObjectName> {
    let mut parts = vec![parse_token_identifier(tokens, cursor)?];
    while parts.len() < max_parts && tokens.get(*cursor) == Some(&Token::Period) {
        *cursor += 1;
        parts.push(parse_token_identifier(tokens, cursor)?);
    }
    Ok(ParsedObjectName { parts })
}

fn parse_token_identifier(tokens: &[Token], cursor: &mut usize) -> Result<ParsedIdentifier> {
    let Token::Word(word) = tokens
        .get(*cursor)
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "identifier expected"))?
    else {
        return Err(DbError::new(SYNTAX_ERROR, "identifier expected"));
    };
    *cursor += 1;
    Ok(ParsedIdentifier {
        name: Identifier::new(word.value.clone(), word.quote_style.is_some()),
        position: None,
    })
}

fn parse_signed_i64(tokens: &[Token], cursor: &mut usize) -> Result<i64> {
    let negative = tokens.get(*cursor) == Some(&Token::Minus);
    if negative {
        *cursor += 1;
    }
    let Token::Number(value, _) = tokens
        .get(*cursor)
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "integer value expected"))?
    else {
        return Err(DbError::new(SYNTAX_ERROR, "integer value expected"));
    };
    *cursor += 1;
    let value = value
        .parse::<i64>()
        .map_err(|_| DbError::new("22003", "sequence option is outside BIGINT range"))?;
    if negative {
        value
            .checked_neg()
            .ok_or_else(|| DbError::new("22003", "sequence option is outside BIGINT range"))
    } else {
        Ok(value)
    }
}

fn expect_keyword(tokens: &[Token], cursor: &mut usize, keyword: &str) -> Result<()> {
    if tokens
        .get(*cursor)
        .is_some_and(|token| is_unquoted_word(token, keyword))
    {
        *cursor += 1;
        Ok(())
    } else {
        Err(DbError::new(
            SYNTAX_ERROR,
            format!("expected keyword {keyword}"),
        ))
    }
}

fn ensure_token_end(tokens: &[Token], cursor: usize) -> Result<()> {
    if cursor == tokens.len() {
        Ok(())
    } else {
        Err(DbError::new(
            SYNTAX_ERROR,
            format!("unexpected token {}", tokens[cursor]),
        ))
    }
}

fn unique_sequence_option(seen: &mut BTreeSet<&'static str>, option: &'static str) -> Result<()> {
    if seen.insert(option) {
        Ok(())
    } else {
        Err(DbError::new(
            SYNTAX_ERROR,
            format!("ALTER SEQUENCE option {option} is specified more than once"),
        ))
    }
}

fn alter_sequence_option_starts(token: &Token) -> bool {
    [
        "INCREMENT",
        "MINVALUE",
        "MAXVALUE",
        "RESTART",
        "CYCLE",
        "NO",
        "OWNED",
    ]
    .iter()
    .any(|keyword| is_unquoted_word(token, keyword))
}

fn parse_refresh_materialized_view(sql: &str) -> Result<Option<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    const PREFIX: &str = "REFRESH MATERIALIZED VIEW ";
    if !trimmed
        .get(..PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
    {
        return Ok(None);
    }
    let mut target = trimmed[PREFIX.len()..].trim();
    if target
        .get(.."CONCURRENTLY ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CONCURRENTLY "))
    {
        return unsupported("REFRESH MATERIALIZED VIEW CONCURRENTLY is not supported");
    }
    let uppercase = target.to_ascii_uppercase();
    let with_data = if uppercase.ends_with(" WITH NO DATA") {
        target = target[..target.len() - " WITH NO DATA".len()].trim_end();
        false
    } else if uppercase.ends_with(" WITH DATA") {
        target = target[..target.len() - " WITH DATA".len()].trim_end();
        true
    } else {
        true
    };
    Ok(Some(ParsedStatement::RefreshMaterializedView {
        name: parse_simple_object_name(target)?,
        with_data,
    }))
}

fn parse_simple_object_name(value: &str) -> Result<ParsedObjectName> {
    if value.is_empty() {
        return Err(DbError::new(SYNTAX_ERROR, "object name is empty"));
    }
    let parts = value
        .split('.')
        .map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return Err(DbError::new(SYNTAX_ERROR, "object name has an empty part"));
            }
            let name = if part.starts_with('"') && part.ends_with('"') && part.len() >= 2 {
                Identifier::quoted(part[1..part.len() - 1].replace("\"\"", "\""))
            } else if part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
            {
                Identifier::unquoted(part)
            } else {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    format!("invalid object-name part {part}"),
                ));
            };
            Ok(ParsedIdentifier {
                name,
                position: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if parts.len() > 2 {
        return unsupported("database-qualified object names are not supported");
    }
    Ok(ParsedObjectName { parts })
}

fn materialized_view_parser_sql(sql: &str) -> Cow<'_, str> {
    let trimmed = sql.trim();
    let without_semicolon = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    let uppercase = without_semicolon.to_ascii_uppercase();
    if !uppercase.starts_with("CREATE MATERIALIZED VIEW ") {
        return Cow::Borrowed(sql);
    }
    for suffix in [" WITH NO DATA", " WITH DATA"] {
        if uppercase.ends_with(suffix) {
            let prefix_len = without_semicolon.len().saturating_sub(suffix.len());
            return Cow::Owned(without_semicolon[..prefix_len].trim_end().to_owned());
        }
    }
    Cow::Borrowed(sql)
}

/// Parse and bind a persisted catalog expression without leaking sqlparser
/// nodes across the SQL boundary.
pub fn bind_catalog_expression(
    expression: &CatalogExpression,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
) -> Result<BoundExpr> {
    bind_catalog_expression_with_parameter_types(expression, table, expected, &BTreeMap::new())
}

/// Bind a persisted expression with caller-owned positional parameter types.
///
/// This is used by procedural contexts where a parameter represents a typed
/// transient record field rather than an untyped client placeholder.
pub fn bind_catalog_expression_with_parameter_types(
    expression: &CatalogExpression,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
    parameter_types: &BTreeMap<usize, ScalarType>,
) -> Result<BoundExpr> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(&expression.sql)
        .map_err(|error| DbError::new(SYNTAX_ERROR, error.to_string()))?;
    let parsed = parser
        .parse_expr()
        .map_err(|error| DbError::new(SYNTAX_ERROR, error.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "catalog expression contains trailing SQL",
        ));
    }
    bind_expr_with_parameter_types(
        convert_expr(parsed, &expression.sql)?,
        table,
        expected,
        parameter_types,
    )
}

/// Bind an OrdaDB-owned parsed statement against an immutable catalog view.
pub fn bind(statement: ParsedStatement, catalog: &Catalog) -> Result<BoundStatement> {
    bind_with_view_depth(statement, catalog, 0)
}

fn bind_with_view_depth(
    statement: ParsedStatement,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    if view_depth > 64 {
        return Err(DbError::new(
            "54001",
            "view expansion exceeds the maximum depth of 64",
        ));
    }
    match statement {
        ParsedStatement::Begin { characteristics } => Ok(BoundStatement::Begin { characteristics }),
        ParsedStatement::Commit { chain } => Ok(BoundStatement::Commit { chain }),
        ParsedStatement::Rollback { chain } => Ok(BoundStatement::Rollback { chain }),
        ParsedStatement::Savepoint { name } => Ok(BoundStatement::Savepoint { name: name.name }),
        ParsedStatement::RollbackTo { name } => Ok(BoundStatement::RollbackTo { name: name.name }),
        ParsedStatement::ReleaseSavepoint { name } => {
            Ok(BoundStatement::ReleaseSavepoint { name: name.name })
        }
        ParsedStatement::Analyze { table } => Ok(BoundStatement::Analyze {
            table_id: table
                .as_ref()
                .map(|table| resolve_table(table, catalog).map(|table| table.id))
                .transpose()?,
        }),
        ParsedStatement::Vacuum { table, analyze } => Ok(BoundStatement::Vacuum {
            table_id: table
                .as_ref()
                .map(|table| resolve_table(table, catalog).map(|table| table.id))
                .transpose()?,
            analyze,
        }),
        ParsedStatement::CreateSchema {
            name,
            if_not_exists,
        } => {
            if catalog.schema(&name.name).is_some() && !if_not_exists {
                return Err(
                    DbError::new("42P06", format!("schema {} already exists", name.name))
                        .with_position_opt(name.position),
                );
            }
            Ok(BoundStatement::CreateSchema {
                name: name.name,
                if_not_exists,
            })
        }
        ParsedStatement::AlterSchemaRename {
            name,
            new_name,
            if_exists,
        } => {
            let Some(schema) = catalog.schema(&name.name) else {
                if if_exists {
                    return Ok(BoundStatement::NoOp {
                        tag: "ALTER SCHEMA".to_owned(),
                    });
                }
                return Err(DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {} does not exist", name.name),
                )
                .with_position_opt(name.position));
            };
            if catalog.schema(&new_name.name).is_some() {
                return Err(DbError::new(
                    "42P06",
                    format!("schema {} already exists", new_name.name),
                )
                .with_position_opt(new_name.position));
            }
            Ok(BoundStatement::AlterSchemaRename {
                schema_id: schema.id,
                new_name: new_name.name,
            })
        }
        ParsedStatement::DropObjects {
            kind,
            names,
            if_exists,
            behavior,
        } => bind_drop_objects(kind, names, if_exists, behavior, catalog),
        ParsedStatement::CreateTable {
            name,
            columns,
            constraints,
            if_not_exists,
        } => bind_create_table(name, columns, constraints, if_not_exists, catalog),
        ParsedStatement::AlterTable {
            name,
            if_exists,
            operations,
        } => bind_alter_table(name, if_exists, operations, catalog),
        ParsedStatement::CreateIndex(index) => bind_create_index(index, catalog),
        ParsedStatement::AlterIndexRename { name, new_name } => {
            let (schema, index, position) = split_table_name(&name)?;
            let index = catalog.index(&schema, &index).ok_or_else(|| {
                DbError::new("42704", format!("index {schema}.{index} does not exist"))
                    .with_position_opt(position)
            })?;
            Ok(BoundStatement::AlterIndexRename {
                index_id: index.id,
                new_name: new_name.name,
            })
        }
        ParsedStatement::CreateSequence {
            name,
            mut sequence,
            if_not_exists,
            owner,
        } => {
            let (schema, sequence_name, position) = split_table_name(&name)?;
            if catalog.schema(&schema).is_none() {
                return Err(DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {schema} does not exist"),
                )
                .with_position_opt(position));
            }
            if catalog.sequence(&schema, &sequence_name).is_some() && !if_not_exists {
                return Err(DbError::new(
                    "42P07",
                    format!("relation {schema}.{sequence_name} already exists"),
                )
                .with_position_opt(position));
            }
            sequence.name = sequence_name;
            sequence.owner = owner
                .map(|(table, column)| {
                    let table = resolve_table(&table, catalog)?;
                    let column = table.column(&column.name).ok_or_else(|| {
                        DbError::new(
                            UNDEFINED_COLUMN,
                            format!("column {} does not exist", column.name),
                        )
                        .with_position_opt(column.position)
                    })?;
                    Ok((table.id, column.id))
                })
                .transpose()?;
            Ok(BoundStatement::CreateSequence {
                schema,
                sequence,
                if_not_exists,
            })
        }
        ParsedStatement::AlterSequenceRename {
            name,
            if_exists,
            new_name,
        } => {
            let (schema_name, sequence_name, position) = split_table_name(&name)?;
            let Some(sequence) = catalog.sequence(&schema_name, &sequence_name) else {
                if if_exists {
                    return Ok(BoundStatement::NoOp {
                        tag: "ALTER SEQUENCE".to_owned(),
                    });
                }
                return Err(DbError::new(
                    "42P01",
                    format!("sequence {schema_name}.{sequence_name} does not exist"),
                )
                .with_position_opt(position));
            };
            Ok(BoundStatement::AlterSequenceRename {
                sequence_id: sequence.id,
                new_name: new_name.name,
            })
        }
        ParsedStatement::AlterSequence {
            name,
            if_exists,
            options,
        } => {
            let (schema_name, sequence_name, position) = split_table_name(&name)?;
            let Some(sequence) = catalog.sequence(&schema_name, &sequence_name) else {
                if if_exists {
                    return Ok(BoundStatement::NoOp {
                        tag: "ALTER SEQUENCE".to_owned(),
                    });
                }
                return Err(DbError::new(
                    "42P01",
                    format!("sequence {schema_name}.{sequence_name} does not exist"),
                )
                .with_position_opt(position));
            };
            let owner = options
                .owner
                .map(|owner| {
                    owner
                        .map(|(table, column)| {
                            let table = resolve_table(&table, catalog)?;
                            let column = table.column(&column.name).ok_or_else(|| {
                                DbError::new(
                                    UNDEFINED_COLUMN,
                                    format!("column {} does not exist", column.name),
                                )
                                .with_position_opt(column.position)
                            })?;
                            Ok((table.id, column.id))
                        })
                        .transpose()
                })
                .transpose()?;
            Ok(BoundStatement::AlterSequence {
                sequence_id: sequence.id,
                increment: options.increment,
                min_value: options.min_value,
                max_value: options.max_value,
                restart: options.restart,
                cycle: options.cycle,
                owner,
            })
        }
        ParsedStatement::CreateView {
            name,
            kind,
            query,
            query_sql,
            columns,
            replace,
            if_not_exists,
            with_data,
        } => bind_create_view(
            CreateViewBindingInput {
                name,
                kind,
                query: *query,
                query_sql,
                columns,
                replace,
                if_not_exists,
                with_data,
            },
            catalog,
            view_depth,
        ),
        ParsedStatement::AlterViewRename {
            name,
            kind,
            if_exists,
            new_name,
        } => {
            let (schema, name, position) = split_table_name(&name)?;
            let Some(view) = catalog.view(&schema, &name) else {
                if if_exists {
                    let tag = match kind {
                        ViewKind::Regular => "ALTER VIEW",
                        ViewKind::Materialized => "ALTER MATERIALIZED VIEW",
                    };
                    return Ok(BoundStatement::NoOp {
                        tag: tag.to_owned(),
                    });
                }
                return Err(DbError::new(
                    UNDEFINED_TABLE,
                    format!("view {schema}.{name} does not exist"),
                )
                .with_position_opt(position));
            };
            if view.kind != kind {
                let expected = match kind {
                    ViewKind::Regular => "view",
                    ViewKind::Materialized => "materialized view",
                };
                return Err(DbError::new(
                    "42809",
                    format!("{schema}.{name} is not a {expected}"),
                ));
            }
            Ok(BoundStatement::AlterViewRename {
                view_id: view.id,
                new_name: new_name.name,
            })
        }
        ParsedStatement::RefreshMaterializedView { name, with_data } => {
            let (schema, name, position) = split_table_name(&name)?;
            let view = catalog.view(&schema, &name).ok_or_else(|| {
                DbError::new(
                    UNDEFINED_TABLE,
                    format!("materialized view {schema}.{name} does not exist"),
                )
                .with_position_opt(position)
            })?;
            if view.kind != ViewKind::Materialized {
                return Err(DbError::new(
                    "42809",
                    format!("{schema}.{name} is not a materialized view"),
                ));
            }
            let table_id = view.materialized_table_id.ok_or_else(|| {
                DbError::internal("materialized view is missing its backing table")
            })?;
            let query =
                bind_with_view_depth(parse(&view.query)?, catalog, view_depth.saturating_add(1))?;
            Ok(BoundStatement::RefreshMaterializedView {
                view_id: view.id,
                table_id,
                query: Box::new(query),
                with_data,
            })
        }
        ParsedStatement::CreateRoutine {
            name,
            kind,
            arguments,
            return_type,
            returns_set,
            language,
            body,
            replace,
        } => {
            let (schema, name, position) = split_table_name(&name)?;
            if catalog.schema(&schema).is_none() {
                return Err(DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {schema} does not exist"),
                )
                .with_position_opt(position));
            }
            Ok(BoundStatement::CreateRoutine {
                schema,
                name,
                kind,
                arguments,
                return_type,
                returns_set,
                language,
                body,
                replace,
            })
        }
        ParsedStatement::DropRoutine {
            name,
            kind,
            argument_types,
            if_exists,
            behavior,
        } => {
            let (schema, name, position) = split_table_name(&name)?;
            let matches = catalog
                .routines_named(&schema, &name)
                .iter()
                .filter(|routine| {
                    routine.kind == kind
                        && argument_types.as_ref().is_none_or(|argument_types| {
                            routine.arguments.len() == argument_types.len()
                                && routine
                                    .arguments
                                    .iter()
                                    .zip(argument_types)
                                    .all(|(argument, expected)| argument.data_type == *expected)
                        })
                })
                .collect::<Vec<_>>();
            let object_kind = match kind {
                RoutineKind::Function => "function",
                RoutineKind::Procedure => "procedure",
            };
            let tag = match kind {
                RoutineKind::Function => "DROP FUNCTION",
                RoutineKind::Procedure => "DROP PROCEDURE",
            };
            match matches.as_slice() {
                [routine] => Ok(BoundStatement::DropRoutine {
                    routine_id: routine.id,
                    behavior,
                }),
                [] if if_exists => Ok(BoundStatement::NoOp {
                    tag: tag.to_owned(),
                }),
                [] => Err(DbError::new(
                    "42883",
                    format!("{object_kind} {schema}.{name} does not exist"),
                )
                .with_position_opt(position)),
                _ => Err(DbError::new(
                    "42725",
                    format!("{object_kind} {schema}.{name} is ambiguous"),
                )
                .with_position_opt(position)
                .with_hint("specify the routine argument types")),
            }
        }
        ParsedStatement::Call { name, arguments } => {
            let (schema, name, position) = split_table_name(&name)?;
            let schema_definition = catalog.schema(&schema).ok_or_else(|| {
                DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                    .with_position_opt(position)
            })?;
            let candidates = schema_definition
                .routines_named(&name)
                .iter()
                .filter(|routine| {
                    routine.kind == RoutineKind::Procedure
                        && routine.arguments.len() == arguments.len()
                })
                .collect::<Vec<_>>();
            let mut matches = Vec::new();
            for routine in candidates {
                let bound = arguments
                    .iter()
                    .cloned()
                    .zip(&routine.arguments)
                    .map(|(argument, expected)| {
                        bind_expr(argument, None, Some(&expected.data_type))
                    })
                    .collect::<Result<Vec<_>>>();
                if let Ok(bound) = bound {
                    matches.push((routine.id, bound));
                }
            }
            match matches.as_slice() {
                [(routine_id, arguments)] => Ok(BoundStatement::Call {
                    routine_id: *routine_id,
                    arguments: arguments.clone(),
                }),
                [] => Err(DbError::new(
                    "42883",
                    format!("procedure {schema}.{name} with matching arguments does not exist"),
                )
                .with_position_opt(position)),
                _ => Err(DbError::new(
                    "42725",
                    format!("procedure call {schema}.{name} is ambiguous"),
                )
                .with_position_opt(position)),
            }
        }
        ParsedStatement::RoutineSelect {
            name,
            arguments,
            alias,
        } => {
            let (schema, name, position) = split_table_name(&name)?;
            let schema_definition = catalog.schema(&schema).ok_or_else(|| {
                DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                    .with_position_opt(position)
            })?;
            let candidates = schema_definition
                .routines_named(&name)
                .iter()
                .filter(|routine| {
                    routine.kind == RoutineKind::Function
                        && routine.return_type.is_some()
                        && routine.arguments.len() == arguments.len()
                })
                .collect::<Vec<_>>();
            let mut matches = Vec::new();
            for routine in candidates {
                let bound = arguments
                    .iter()
                    .cloned()
                    .zip(&routine.arguments)
                    .map(|(argument, expected)| {
                        bind_expr(argument, None, Some(&expected.data_type))
                    })
                    .collect::<Result<Vec<_>>>();
                if let Ok(bound) = bound {
                    matches.push((routine, bound));
                }
            }
            match matches.as_slice() {
                [(routine, arguments)] => {
                    let return_type = routine.return_type.clone().ok_or_else(|| {
                        DbError::internal("selected function lost its return type")
                    })?;
                    Ok(BoundStatement::RoutineSelect {
                        routine_id: routine.id,
                        arguments: arguments.clone(),
                        schema: Schema::new(vec![Field::new(
                            alias
                                .as_ref()
                                .map_or(name.as_str(), |alias| alias.name.as_str()),
                            return_type,
                            true,
                        )]),
                        returns_set: routine.returns_set,
                    })
                }
                [] => Err(DbError::new(
                    "42883",
                    format!("function {schema}.{name} with matching arguments does not exist"),
                )
                .with_position_opt(position)),
                _ => Err(DbError::new(
                    "42725",
                    format!("function call {schema}.{name} is ambiguous"),
                )
                .with_position_opt(position)),
            }
        }
        ParsedStatement::SequenceValue {
            name,
            operation,
            alias,
        } => {
            let (schema_name, sequence_name, position) = split_table_name(&name)?;
            let sequence = catalog
                .sequence(&schema_name, &sequence_name)
                .ok_or_else(|| {
                    DbError::new(
                        "42P01",
                        format!("sequence {schema_name}.{sequence_name} does not exist"),
                    )
                    .with_position_opt(position)
                })?;
            let operation = match operation {
                ParsedSequenceOperation::NextValue => BoundSequenceOperation::NextValue,
                ParsedSequenceOperation::CurrentValue => {
                    BoundSequenceOperation::CurrentValue { value: None }
                }
                ParsedSequenceOperation::SetValue { value, is_called } => {
                    BoundSequenceOperation::SetValue {
                        value: bind_expr(value, None, Some(&ScalarType::Int64))?,
                        is_called,
                    }
                }
            };
            let field_name = alias.as_ref().map_or_else(
                || match &operation {
                    BoundSequenceOperation::NextValue => "nextval",
                    BoundSequenceOperation::CurrentValue { .. } => "currval",
                    BoundSequenceOperation::SetValue { .. } => "setval",
                },
                |alias| alias.name.as_str(),
            );
            Ok(BoundStatement::SequenceValue {
                sequence_id: sequence.id,
                operation,
                schema: Schema::new(vec![Field::new(field_name, ScalarType::Int64, false)]),
            })
        }
        ParsedStatement::CreateTrigger {
            name,
            table,
            timing,
            events,
            routine,
        } => {
            let table = resolve_table(&table, catalog)?;
            let (routine_schema, routine_name, routine_position) = split_table_name(&routine)?;
            let schema = catalog.schema(&routine_schema).ok_or_else(|| {
                DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {routine_schema} does not exist"),
                )
                .with_position_opt(routine_position)
            })?;
            let matches = schema
                .routines_named(&routine_name)
                .iter()
                .filter(|routine| {
                    routine.kind == RoutineKind::Function
                        && routine.arguments.is_empty()
                        && routine.return_type.is_none()
                })
                .collect::<Vec<_>>();
            let routine_id = match matches.as_slice() {
                [routine] => routine.id,
                [] => {
                    return Err(DbError::new(
                        "42883",
                        format!(
                            "trigger function {routine_schema}.{routine_name}() does not exist"
                        ),
                    )
                    .with_position_opt(routine_position));
                }
                _ => {
                    return Err(DbError::new(
                        "42725",
                        format!("trigger function {routine_schema}.{routine_name}() is ambiguous"),
                    )
                    .with_position_opt(routine_position));
                }
            };
            Ok(BoundStatement::CreateTrigger {
                table_id: table.id,
                name: name.name,
                timing,
                events,
                routine_id,
            })
        }
        ParsedStatement::DropTrigger {
            name,
            table,
            if_exists,
            behavior,
        } => {
            let table = resolve_table(&table, catalog)?;
            let Some(trigger) = table.trigger(&name.name) else {
                if if_exists {
                    return Ok(BoundStatement::NoOp {
                        tag: "DROP TRIGGER".to_owned(),
                    });
                }
                return Err(DbError::new(
                    "42704",
                    format!(
                        "trigger {} for relation {} does not exist",
                        name.name, table.name
                    ),
                )
                .with_position_opt(name.position));
            };
            Ok(BoundStatement::DropTrigger {
                trigger_id: trigger.id,
                behavior,
            })
        }
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
        } => bind_select(
            table, projection, filter, order_by, limit, catalog, view_depth,
        ),
        ParsedStatement::AdvancedSelect {
            table,
            joins,
            projection,
            filter,
            group_by,
            having,
            order_by,
            limit,
        } => bind_advanced_select(
            AdvancedSelectInput {
                table,
                joins,
                projection,
                filter,
                group_by,
                having,
                order_by,
                limit,
            },
            catalog,
        ),
        ParsedStatement::Explain { statement } => {
            let statement = bind_with_view_depth(*statement, catalog, view_depth)?;
            if !matches!(
                statement,
                BoundStatement::Select { .. } | BoundStatement::AdvancedSelect { .. }
            ) {
                return unsupported("EXPLAIN supports SELECT statements only");
            }
            Ok(BoundStatement::Explain {
                statement: Box::new(statement),
            })
        }
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
    let mut parsed = ParsedColumn {
        name: name.clone(),
        data_type: convert_data_type(column.data_type)?,
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

fn convert_table_constraint(
    constraint: TableConstraint,
    sql: &str,
) -> Result<ParsedTableConstraint> {
    match constraint {
        TableConstraint::PrimaryKey(constraint) => {
            if constraint.index_name.is_some()
                || constraint.index_type.is_some()
                || !constraint.index_options.is_empty()
                || constraint.characteristics.is_some()
            {
                return unsupported("extended primary-key constraints are not supported");
            }
            Ok(ParsedTableConstraint::PrimaryKey {
                name: constraint.name.map(|name| convert_ident(name, sql)),
                columns: constraint
                    .columns
                    .iter()
                    .map(|column| convert_index_column(column, sql))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        TableConstraint::Unique(constraint) => {
            if constraint.index_name.is_some()
                || constraint.index_type.is_some()
                || !constraint.index_options.is_empty()
                || constraint.characteristics.is_some()
            {
                return unsupported("extended unique constraints are not supported");
            }
            Ok(ParsedTableConstraint::Unique {
                name: constraint.name.map(|name| convert_ident(name, sql)),
                columns: constraint
                    .columns
                    .iter()
                    .map(|column| convert_index_column(column, sql))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        TableConstraint::Check(constraint) => {
            if constraint.enforced.is_some() {
                return unsupported("CHECK ENFORCED clauses are not supported");
            }
            Ok(ParsedTableConstraint::Check {
                name: constraint.name.map(|name| convert_ident(name, sql)),
                sql: constraint.expr.to_string(),
                expression: convert_expr(*constraint.expr, sql)?,
            })
        }
        TableConstraint::ForeignKey(constraint) => {
            if constraint.index_name.is_some()
                || constraint.match_kind.is_some()
                || constraint.characteristics.is_some()
            {
                return unsupported("extended foreign-key constraints are not supported");
            }
            Ok(ParsedTableConstraint::ForeignKey {
                name: constraint.name.map(|name| convert_ident(name, sql)),
                columns: constraint
                    .columns
                    .into_iter()
                    .map(|column| convert_ident(column, sql))
                    .collect(),
                referenced_table: convert_object_name(constraint.foreign_table, sql)?,
                referenced_columns: constraint
                    .referred_columns
                    .into_iter()
                    .map(|column| convert_ident(column, sql))
                    .collect(),
                on_delete: convert_referential_action(constraint.on_delete)?,
                on_update: convert_referential_action(constraint.on_update)?,
            })
        }
        _ => unsupported("this table constraint is not supported"),
    }
}

fn convert_referential_action(action: Option<SqlReferentialAction>) -> Result<ReferentialAction> {
    match action {
        None | Some(SqlReferentialAction::NoAction) => Ok(ReferentialAction::NoAction),
        Some(SqlReferentialAction::Restrict) => Ok(ReferentialAction::Restrict),
        Some(SqlReferentialAction::Cascade) => Ok(ReferentialAction::Cascade),
        Some(SqlReferentialAction::SetNull) => Ok(ReferentialAction::SetNull),
        Some(SqlReferentialAction::SetDefault) => Ok(ReferentialAction::SetDefault),
    }
}

fn convert_drop_behavior(behavior: Option<SqlDropBehavior>) -> DropBehavior {
    match behavior {
        Some(SqlDropBehavior::Cascade) => DropBehavior::Cascade,
        Some(SqlDropBehavior::Restrict) | None => DropBehavior::Restrict,
    }
}

fn convert_alter_table(table: AlterTable, sql: &str) -> Result<ParsedStatement> {
    if table.only
        || table.location.is_some()
        || table.on_cluster.is_some()
        || table.table_type.is_some()
    {
        return unsupported("this ALTER TABLE form is not supported");
    }
    if table.operations.is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "ALTER TABLE requires at least one operation",
        ));
    }
    let mut operations = Vec::new();
    for operation in table.operations {
        match operation {
            SqlAlterTableOperation::RenameTable { table_name } => {
                let RenameTableNameKind::To(name) = table_name else {
                    return unsupported("ALTER TABLE rename requires RENAME TO");
                };
                operations.push(ParsedAlterTableOperation::RenameTable {
                    new_name: convert_single_identifier(name, sql)?,
                });
            }
            SqlAlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => operations.push(ParsedAlterTableOperation::RenameColumn {
                old_name: convert_ident(old_column_name, sql),
                new_name: convert_ident(new_column_name, sql),
            }),
            SqlAlterTableOperation::AddColumn {
                if_not_exists,
                column_def,
                column_position,
                ..
            } => {
                if column_position.is_some() {
                    return unsupported("column positioning is not supported");
                }
                let (column, constraints) = convert_column_definition(column_def, sql)?;
                operations.push(ParsedAlterTableOperation::AddColumn {
                    column,
                    if_not_exists,
                });
                operations.extend(
                    constraints
                        .into_iter()
                        .map(|constraint| ParsedAlterTableOperation::AddConstraint { constraint }),
                );
            }
            SqlAlterTableOperation::DropColumn {
                column_names,
                if_exists,
                drop_behavior,
                ..
            } => operations.push(ParsedAlterTableOperation::DropColumns {
                columns: column_names
                    .into_iter()
                    .map(|name| convert_ident(name, sql))
                    .collect(),
                if_exists,
                behavior: convert_drop_behavior(drop_behavior),
            }),
            SqlAlterTableOperation::AlterColumn { column_name, op } => {
                let column = convert_ident(column_name, sql);
                operations.push(match op {
                    SqlAlterColumnOperation::SetNotNull => {
                        ParsedAlterTableOperation::SetNotNull { column }
                    }
                    SqlAlterColumnOperation::DropNotNull => {
                        ParsedAlterTableOperation::DropNotNull { column }
                    }
                    SqlAlterColumnOperation::SetDefault { value } => {
                        ParsedAlterTableOperation::SetDefault {
                            column,
                            default: ParsedDefault {
                                sql: value.to_string(),
                                expression: convert_expr(value, sql)?,
                            },
                        }
                    }
                    SqlAlterColumnOperation::DropDefault => {
                        ParsedAlterTableOperation::DropDefault { column }
                    }
                    SqlAlterColumnOperation::SetDataType {
                        data_type, using, ..
                    } => {
                        if using.is_some() {
                            return unsupported(
                                "ALTER COLUMN TYPE USING expressions are not supported",
                            );
                        }
                        ParsedAlterTableOperation::SetDataType {
                            column,
                            data_type: convert_data_type(data_type)?,
                        }
                    }
                    SqlAlterColumnOperation::AddGenerated { .. } => {
                        return unsupported("generated columns are not supported");
                    }
                });
            }
            SqlAlterTableOperation::AddConstraint {
                constraint,
                not_valid,
            } => {
                if not_valid {
                    return unsupported("NOT VALID constraints are not supported");
                }
                operations.push(ParsedAlterTableOperation::AddConstraint {
                    constraint: convert_table_constraint(constraint, sql)?,
                });
            }
            SqlAlterTableOperation::DropConstraint {
                if_exists,
                name,
                drop_behavior,
            } => operations.push(ParsedAlterTableOperation::DropConstraint {
                name: convert_ident(name, sql),
                if_exists,
                behavior: convert_drop_behavior(drop_behavior),
            }),
            SqlAlterTableOperation::EnableTrigger { name } => {
                operations.push(ParsedAlterTableOperation::SetTriggerEnabled {
                    name: convert_ident(name, sql),
                    enabled: true,
                });
            }
            SqlAlterTableOperation::DisableTrigger { name } => {
                operations.push(ParsedAlterTableOperation::SetTriggerEnabled {
                    name: convert_ident(name, sql),
                    enabled: false,
                });
            }
            _ => return unsupported("this ALTER TABLE operation is not supported"),
        }
    }
    Ok(ParsedStatement::AlterTable {
        name: convert_object_name(table.name, sql)?,
        if_exists: table.if_exists,
        operations,
    })
}

fn apply_sequence_options(
    sequence: &mut NewSequence,
    options: Vec<SequenceOptions>,
    sql: &str,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for option in options {
        let key = match &option {
            SequenceOptions::IncrementBy(..) => 0_u8,
            SequenceOptions::MinValue(..) => 1,
            SequenceOptions::MaxValue(..) => 2,
            SequenceOptions::StartWith(..) => 3,
            SequenceOptions::Cache(..) => 4,
            SequenceOptions::Cycle(..) => 5,
        };
        if !seen.insert(key) {
            return Err(DbError::new(
                SYNTAX_ERROR,
                "a sequence option was specified more than once",
            ));
        }
        match option {
            SequenceOptions::IncrementBy(value, _) => {
                sequence.increment = sequence_option_i64(value, sql)?;
            }
            SequenceOptions::MinValue(value) => {
                sequence.min_value = value
                    .map(|value| sequence_option_i64(value, sql))
                    .transpose()?;
            }
            SequenceOptions::MaxValue(value) => {
                sequence.max_value = value
                    .map(|value| sequence_option_i64(value, sql))
                    .transpose()?;
            }
            SequenceOptions::StartWith(value, _) => {
                sequence.start_value = Some(sequence_option_i64(value, sql)?);
            }
            SequenceOptions::Cache(value) => {
                if sequence_option_i64(value, sql)? != 1 {
                    return unsupported("sequence CACHE values other than 1 are not supported");
                }
            }
            SequenceOptions::Cycle(no_cycle) => sequence.cycle = !no_cycle,
        }
    }
    Ok(())
}

fn sequence_option_i64(expression: SqlExpr, sql: &str) -> Result<i64> {
    let expression = convert_expr(expression, sql)?;
    match expression.kind {
        ParsedExprKind::Literal(Value::Int16(value)) => Ok(i64::from(value)),
        ParsedExprKind::Literal(Value::Int32(value)) => Ok(i64::from(value)),
        ParsedExprKind::Literal(Value::Int64(value)) => Ok(value),
        ParsedExprKind::Unary {
            op: UnaryOperator::Negate,
            expr,
        } => match expr.kind {
            ParsedExprKind::Literal(Value::Int16(value)) => Ok(-i64::from(value)),
            ParsedExprKind::Literal(Value::Int32(value)) => Ok(-i64::from(value)),
            ParsedExprKind::Literal(Value::Int64(value)) => value
                .checked_neg()
                .ok_or_else(|| DbError::new("22003", "sequence option is out of range")),
            _ => Err(DbError::new(
                SYNTAX_ERROR,
                "sequence options must be integer constants",
            )),
        },
        _ => Err(DbError::new(
            SYNTAX_ERROR,
            "sequence options must be integer constants",
        )),
    }
}

fn split_owned_by(owner: ObjectName, sql: &str) -> Result<(ParsedObjectName, ParsedIdentifier)> {
    let mut parts = convert_object_name(owner, sql)?.parts;
    if parts.len() < 2 || parts.len() > 3 {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "OWNED BY requires table.column or schema.table.column",
        ));
    }
    let column = parts
        .pop()
        .ok_or_else(|| DbError::internal("OWNED BY column disappeared"))?;
    Ok((ParsedObjectName { parts }, column))
}

fn convert_create_view(view: CreateView, sql: &str) -> Result<ParsedStatement> {
    if view.or_alter
        || view.secure
        || view.name_before_not_exists
        || !matches!(view.options, CreateTableOptions::None)
        || !view.cluster_by.is_empty()
        || view.comment.is_some()
        || view.with_no_schema_binding
        || view.temporary
        || view.copy_grants
        || view.to.is_some()
        || view.params.is_some()
        || (view.or_replace && view.if_not_exists)
        || (view.materialized && view.or_replace)
    {
        return unsupported("this CREATE VIEW form is not supported");
    }
    let columns = view
        .columns
        .into_iter()
        .map(|column| {
            if column.data_type.is_some() || column.options.is_some() {
                return unsupported("typed or optioned view columns are not supported");
            }
            Ok(convert_ident(column.name, sql))
        })
        .collect::<Result<Vec<_>>>()?;
    let query_sql = view.query.to_string();
    let query = convert_select_query(*view.query, sql)?;
    Ok(ParsedStatement::CreateView {
        name: convert_object_name(view.name, sql)?,
        kind: if view.materialized {
            ViewKind::Materialized
        } else {
            ViewKind::Regular
        },
        query: Box::new(query),
        query_sql,
        columns,
        replace: view.or_replace,
        if_not_exists: view.if_not_exists,
        with_data: !has_keyword_sequence(sql, &["WITH", "NO", "DATA"]),
    })
}

fn convert_create_function(function: SqlCreateFunction, sql: &str) -> Result<ParsedStatement> {
    if function.or_alter
        || function.temporary
        || function.if_not_exists
        || function.behavior.is_some()
        || function.called_on_null.is_some()
        || function.parallel.is_some()
        || !function.set_params.is_empty()
        || function.using.is_some()
        || function.determinism_specifier.is_some()
        || function.options.is_some()
        || function.remote_connection.is_some()
    {
        return unsupported("this CREATE FUNCTION option is not supported");
    }
    if matches!(function.security, Some(FunctionSecurity::Definer)) {
        return unsupported("SECURITY DEFINER routines are not supported");
    }
    let language = function
        .language
        .map(|language| language.value)
        .unwrap_or_else(|| "plpgsql".to_owned());
    if !language.eq_ignore_ascii_case("plpgsql") {
        return unsupported("only LANGUAGE plpgsql routines are supported");
    }
    let arguments = function
        .args
        .unwrap_or_default()
        .into_iter()
        .map(|argument| {
            if !matches!(argument.mode, None | Some(ArgMode::In)) || argument.default_expr.is_some()
            {
                return unsupported("only non-defaulted IN routine arguments are supported");
            }
            Ok(RoutineArgument {
                name: argument.name.map(|name| convert_ident(name, sql).name),
                data_type: convert_data_type(argument.data_type)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let (return_type, returns_set) = match function.return_type {
        Some(FunctionReturnType::DataType(data_type)) if is_trigger_type(&data_type) => {
            (None, false)
        }
        Some(FunctionReturnType::DataType(data_type)) => {
            (Some(convert_data_type(data_type)?), false)
        }
        Some(FunctionReturnType::SetOf(data_type)) => (Some(convert_data_type(data_type)?), true),
        None => return unsupported("CREATE FUNCTION requires a return type"),
    };
    let body = match function.function_body {
        Some(CreateFunctionBody::AsBeforeOptions {
            body,
            link_symbol: None,
        })
        | Some(CreateFunctionBody::AsAfterOptions(body)) => routine_body_string(body)?,
        _ => {
            return unsupported("CREATE FUNCTION requires one quoted PL/pgSQL body after AS");
        }
    };
    Ok(ParsedStatement::CreateRoutine {
        name: convert_object_name(function.name, sql)?,
        kind: RoutineKind::Function,
        arguments,
        return_type,
        returns_set,
        language: "plpgsql".to_owned(),
        body,
        replace: function.or_replace,
    })
}

fn convert_drop_routine(
    mut routines: Vec<sqlparser::ast::FunctionDesc>,
    kind: RoutineKind,
    if_exists: bool,
    behavior: Option<SqlDropBehavior>,
    sql: &str,
) -> Result<ParsedStatement> {
    if routines.len() != 1 {
        return unsupported("dropping multiple routines in one statement is not supported");
    }
    let routine = routines
        .pop()
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "DROP routine requires a name"))?;
    let argument_types = routine
        .args
        .map(|arguments| {
            arguments
                .into_iter()
                .map(|argument| {
                    if !matches!(argument.mode, None | Some(ArgMode::In))
                        || argument.default_expr.is_some()
                    {
                        return unsupported(
                            "DROP routine signatures support only non-defaulted IN arguments",
                        );
                    }
                    convert_data_type(argument.data_type)
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    Ok(ParsedStatement::DropRoutine {
        name: convert_object_name(routine.name, sql)?,
        kind,
        argument_types,
        if_exists,
        behavior: convert_drop_behavior(behavior),
    })
}

fn convert_create_trigger(trigger: SqlCreateTrigger, sql: &str) -> Result<ParsedStatement> {
    if trigger.or_alter
        || trigger.temporary
        || trigger.or_replace
        || trigger.is_constraint
        || trigger.referenced_table_name.is_some()
        || !trigger.referencing.is_empty()
        || trigger.condition.is_some()
        || trigger.statements_as
        || trigger.statements.is_some()
        || trigger.characteristics.is_some()
        || !trigger.period_before_table
    {
        return unsupported("this CREATE TRIGGER option is not supported");
    }
    if !matches!(
        trigger.trigger_object,
        Some(TriggerObjectKind::ForEach(TriggerObject::Row))
            | Some(TriggerObjectKind::For(TriggerObject::Row))
    ) {
        return unsupported("only FOR EACH ROW triggers are supported");
    }
    let timing = match trigger.period {
        Some(TriggerPeriod::Before) => TriggerTiming::Before,
        Some(TriggerPeriod::After) => TriggerTiming::After,
        _ => return unsupported("only BEFORE and AFTER triggers are supported"),
    };
    let events = trigger
        .events
        .into_iter()
        .map(|event| match event {
            SqlTriggerEvent::Insert => Ok(CatalogTriggerEvent::Insert),
            SqlTriggerEvent::Update(columns) if columns.is_empty() => {
                Ok(CatalogTriggerEvent::Update)
            }
            SqlTriggerEvent::Delete => Ok(CatalogTriggerEvent::Delete),
            _ => unsupported("this trigger event is not supported"),
        })
        .collect::<Result<Vec<_>>>()?;
    let body = trigger
        .exec_body
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "trigger requires EXECUTE FUNCTION"))?;
    if body.exec_type != TriggerExecBodyType::Function
        || body
            .func_desc
            .args
            .as_ref()
            .is_some_and(|arguments| !arguments.is_empty())
    {
        return unsupported("trigger functions must be invoked without arguments");
    }
    let name = convert_object_name(trigger.name, sql)?;
    let [name] = name.parts.as_slice() else {
        return unsupported("trigger names cannot be schema qualified");
    };
    Ok(ParsedStatement::CreateTrigger {
        name: name.clone(),
        table: convert_object_name(trigger.table_name, sql)?,
        timing,
        events,
        routine: convert_object_name(body.func_desc.name, sql)?,
    })
}

fn is_trigger_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Trigger)
        || matches!(
            data_type,
            DataType::Custom(name, modifiers)
                if modifiers.is_empty() && name.to_string().eq_ignore_ascii_case("trigger")
        )
}

fn routine_body_string(body: SqlExpr) -> Result<String> {
    let SqlExpr::Value(value) = body else {
        return unsupported("routine body must be a quoted string");
    };
    match value.value {
        SqlValue::DollarQuotedString(value) => Ok(value.value),
        SqlValue::SingleQuotedString(value) => Ok(value),
        _ => unsupported("routine body must be a dollar-quoted or single-quoted string"),
    }
}

fn convert_routine_invocation(
    function: Function,
    sql: &str,
) -> Result<(ParsedObjectName, Vec<ParsedExpr>)> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("routine invocation options are not supported");
    }
    let FunctionArguments::List(arguments) = function.args else {
        return unsupported("routine invocation requires a parenthesized argument list");
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return unsupported("routine argument modifiers are not supported");
    }
    let arguments = arguments
        .args
        .into_iter()
        .map(|argument| match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => {
                convert_expr(expression, sql)
            }
            _ => unsupported("routine calls support positional expression arguments only"),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((convert_object_name(function.name, sql)?, arguments))
}

fn convert_select_query(query: Query, sql: &str) -> Result<ParsedStatement> {
    if query.with.is_some()
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
    let mut select = *select;
    let top_limit = match select.top.take() {
        None => None,
        Some(top) if top.with_ties || top.percent => {
            return unsupported("TOP PERCENT and TOP WITH TIES are not supported");
        }
        Some(top) => match top.quantity {
            Some(TopQuantity::Expr(expression)) => Some(convert_expr(expression, sql)?),
            Some(TopQuantity::Constant(value)) => {
                let value = i64::try_from(value)
                    .map_err(|_| DbError::new("22003", "TOP value is out of range"))?;
                Some(ParsedExpr {
                    kind: ParsedExprKind::Literal(Value::Int64(value)),
                    position: None,
                })
            }
            None => return unsupported("TOP requires an explicit row count"),
        },
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

    let fetch_limit = match query.fetch {
        None => None,
        Some(fetch) if fetch.with_ties || fetch.percent => {
            return unsupported("FETCH PERCENT and FETCH WITH TIES are not supported");
        }
        Some(fetch) => Some(
            fetch
                .quantity
                .map(|expression| convert_expr(expression, sql))
                .transpose()?
                .unwrap_or(ParsedExpr {
                    kind: ParsedExprKind::Literal(Value::Int64(1)),
                    position: None,
                }),
        ),
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
        Some(LimitClause::LimitOffset {
            limit,
            offset: Some(offset),
            limit_by,
        }) if limit_by.is_empty() && sql_expr_is_integer_zero(&offset.value) => {
            limit.map(|expr| convert_expr(expr, sql)).transpose()?
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit })
            if sql_expr_is_integer_zero(&offset) =>
        {
            Some(convert_expr(limit, sql)?)
        }
        Some(_) => {
            return unsupported(
                "non-zero OFFSET and unrepresentable dialect-specific LIMIT forms are not supported",
            );
        }
    };
    let limit = match (top_limit, limit, fetch_limit) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) | (_, Some(_), Some(_)) => {
            return unsupported("a query may specify only one row-limit form");
        }
        (top, limit, fetch) => top.or(limit).or(fetch),
    };

    convert_select(select, order_by, limit, sql)
}

fn sql_expr_is_integer_zero(expression: &SqlExpr) -> bool {
    matches!(
        expression,
        SqlExpr::Value(value)
            if matches!(&value.value, SqlValue::Number(number, _) if number == "0")
    )
}

fn convert_select(
    select: Select,
    order_by: Vec<ParsedOrder>,
    limit: Option<ParsedExpr>,
    sql: &str,
) -> Result<ParsedStatement> {
    if !select.optimizer_hints.is_empty()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return unsupported("DISTINCT and extended SELECT clauses are not supported yet");
    }
    if select.from.is_empty() {
        return convert_routine_select(select, order_by, limit, sql);
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
    let filter = select
        .selection
        .map(|expr| convert_expr(expr, sql))
        .transpose()?;
    let group_by = match select.group_by {
        GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => expressions
            .into_iter()
            .map(|expr| convert_expr(expr, sql))
            .collect::<Result<Vec<_>>>()?,
        GroupByExpr::Expressions(_, _) => {
            return unsupported("GROUP BY modifiers are not supported yet");
        }
        GroupByExpr::All(_) => return unsupported("GROUP BY ALL is not supported yet"),
    };
    let having = select
        .having
        .map(|expr| convert_expr(expr, sql))
        .transpose()?;
    let from = select
        .from
        .into_iter()
        .next()
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "SELECT requires a table"))?;
    let advanced = !from.joins.is_empty()
        || !group_by.is_empty()
        || having.is_some()
        || projection.iter().any(projection_has_aggregate);
    if advanced {
        let (table, joins) = convert_select_from(from, sql)?;
        Ok(ParsedStatement::AdvancedSelect {
            table,
            joins,
            projection,
            filter,
            group_by,
            having,
            order_by,
            limit,
        })
    } else {
        Ok(ParsedStatement::Select {
            table: convert_table_with_joins(from, sql)?,
            projection,
            filter,
            order_by,
            limit,
        })
    }
}

fn convert_routine_select(
    select: Select,
    order_by: Vec<ParsedOrder>,
    limit: Option<ParsedExpr>,
    sql: &str,
) -> Result<ParsedStatement> {
    if select.selection.is_some()
        || select.having.is_some()
        || !order_by.is_empty()
        || limit.is_some()
        || !matches!(
            select.group_by,
            GroupByExpr::Expressions(ref expressions, ref modifiers)
                if expressions.is_empty() && modifiers.is_empty()
        )
    {
        return unsupported("scalar routine SELECT does not support query clauses");
    }
    let [projection] = select.projection.as_slice() else {
        return unsupported("scalar routine SELECT requires exactly one projection");
    };
    let (expression, alias) = match projection {
        SelectItem::UnnamedExpr(expression) => (expression.clone(), None),
        SelectItem::ExprWithAlias { expr, alias } => {
            (expr.clone(), Some(convert_ident(alias.clone(), sql)))
        }
        _ => return unsupported("scalar routine SELECT requires one routine call"),
    };
    let SqlExpr::Function(function) = expression else {
        return unsupported("SELECT without FROM supports routine calls only");
    };
    let (name, arguments) = convert_routine_invocation(function, sql)?;
    if let Some(operation_name) = sequence_operation_name(&name) {
        return convert_sequence_value_select(operation_name, arguments, alias);
    }
    Ok(ParsedStatement::RoutineSelect {
        name,
        arguments,
        alias,
    })
}

fn sequence_operation_name(name: &ParsedObjectName) -> Option<&str> {
    if name.parts.is_empty() || name.parts.len() > 2 {
        return None;
    }
    let operation = name.parts.last()?.name.as_str();
    operation
        .eq_ignore_ascii_case("nextval")
        .then_some("nextval")
        .or_else(|| {
            operation
                .eq_ignore_ascii_case("currval")
                .then_some("currval")
        })
        .or_else(|| operation.eq_ignore_ascii_case("setval").then_some("setval"))
}

fn convert_sequence_value_select(
    operation: &str,
    mut arguments: Vec<ParsedExpr>,
    alias: Option<ParsedIdentifier>,
) -> Result<ParsedStatement> {
    let expected = if operation == "setval" {
        "two or three"
    } else {
        "one"
    };
    let valid_count = if operation == "setval" {
        matches!(arguments.len(), 2 | 3)
    } else {
        arguments.len() == 1
    };
    if !valid_count {
        return Err(DbError::new(
            "42883",
            format!("{operation} requires {expected} positional argument(s)"),
        ));
    }
    let name_argument = arguments.remove(0);
    let name = parsed_sequence_regclass(&name_argument)?;
    let operation = match operation {
        "nextval" => ParsedSequenceOperation::NextValue,
        "currval" => ParsedSequenceOperation::CurrentValue,
        "setval" => {
            let value = arguments.remove(0);
            let is_called = if arguments.is_empty() {
                true
            } else {
                match arguments.remove(0).kind {
                    ParsedExprKind::Literal(Value::Boolean(value)) => value,
                    _ => {
                        return Err(DbError::new(
                            "42804",
                            "setval third argument must be a boolean literal",
                        ));
                    }
                }
            };
            ParsedSequenceOperation::SetValue { value, is_called }
        }
        _ => return Err(DbError::internal("unknown sequence operation")),
    };
    Ok(ParsedStatement::SequenceValue {
        name,
        operation,
        alias,
    })
}

fn parsed_sequence_regclass(argument: &ParsedExpr) -> Result<ParsedObjectName> {
    let ParsedExprKind::Literal(Value::Text(value)) = &argument.kind else {
        return Err(
            DbError::new("42804", "sequence name must be a text regclass literal")
                .with_position_opt(argument.position),
        );
    };
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.is_empty()
        || parts.len() > 2
        || parts
            .iter()
            .any(|part| part.is_empty() || !is_simple_identifier(part))
    {
        return Err(DbError::new(
            "42602",
            "sequence regclass must be an unquoted name or schema.name",
        )
        .with_position_opt(argument.position));
    }
    Ok(ParsedObjectName {
        parts: parts
            .into_iter()
            .map(|part| ParsedIdentifier {
                name: Identifier::unquoted(part),
                position: argument.position,
            })
            .collect(),
    })
}

fn is_simple_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn convert_select_from(table: TableWithJoins, sql: &str) -> Result<(ParsedTable, Vec<ParsedJoin>)> {
    let first = convert_select_table(table.relation, sql)?;
    let joins = table
        .joins
        .into_iter()
        .map(|join| {
            if join.global {
                return unsupported("GLOBAL joins are not supported");
            }
            let (kind, constraint) = match join.join_operator {
                JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                    (JoinKind::Inner, constraint)
                }
                JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                    (JoinKind::Left, constraint)
                }
                _ => return unsupported("only INNER and LEFT joins are supported"),
            };
            let JoinConstraint::On(on) = constraint else {
                return unsupported("joins require an ON predicate");
            };
            Ok(ParsedJoin {
                table: convert_select_table(join.relation, sql)?,
                kind,
                on: convert_expr(on, sql)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((first, joins))
}

fn convert_select_table(table: TableFactor, sql: &str) -> Result<ParsedTable> {
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
    } = table
    else {
        return unsupported("derived tables and table functions are not supported yet");
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return unsupported("table modifiers are not supported yet");
    }
    Ok(ParsedTable {
        name: convert_object_name(name, sql)?,
        alias: alias
            .map(|alias| convert_table_alias(alias, sql))
            .transpose()?,
    })
}

fn convert_table_alias(alias: TableAlias, sql: &str) -> Result<ParsedIdentifier> {
    if !alias.columns.is_empty() {
        return unsupported("column aliases on table bindings are not supported yet");
    }
    Ok(convert_ident(alias.name, sql))
}

fn projection_has_aggregate(projection: &ParsedProjection) -> bool {
    match projection {
        ParsedProjection::Wildcard => false,
        ParsedProjection::Expression { expr, .. } => expr_has_aggregate(expr),
    }
}

fn expr_has_aggregate(expr: &ParsedExpr) -> bool {
    match &expr.kind {
        ParsedExprKind::Aggregate { .. } => true,
        ParsedExprKind::Unary { expr, .. } => expr_has_aggregate(expr),
        ParsedExprKind::Binary { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        ParsedExprKind::Column(_) | ParsedExprKind::Literal(_) | ParsedExprKind::Parameter(_) => {
            false
        }
    }
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
        SqlExpr::Identifier(ident) => {
            if ident.quote_style.is_none()
                && let Some(index) = named_at_parameter_index(&ident.value)
            {
                ParsedExprKind::Parameter(index)
            } else {
                ParsedExprKind::Column(ParsedObjectName {
                    parts: vec![convert_ident(ident, sql)],
                })
            }
        }
        SqlExpr::CompoundIdentifier(parts) => ParsedExprKind::Column(ParsedObjectName {
            parts: parts
                .into_iter()
                .map(|ident| convert_ident(ident, sql))
                .collect(),
        }),
        SqlExpr::Nested(expr) => return convert_expr(*expr, sql),
        SqlExpr::Value(value) => convert_sql_value(value.value, position)?,
        SqlExpr::TypedString(typed) => {
            let value = typed.value.into_string().ok_or_else(|| {
                DbError::new(SYNTAX_ERROR, "typed literal requires a string value")
                    .with_position_opt(position)
            })?;
            ParsedExprKind::Literal(parse_temporal_literal(typed.data_type, &value, position)?)
        }
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
        SqlExpr::Function(function) => {
            if function.uses_odbc_syntax
                || !matches!(function.parameters, FunctionArguments::None)
                || function.filter.is_some()
                || function.null_treatment.is_some()
                || function.over.is_some()
                || !function.within_group.is_empty()
            {
                return unsupported_at(
                    "aggregate options and window functions are not supported yet",
                    position,
                );
            }
            let function_name = function.name.to_string().to_ascii_lowercase();
            let aggregate_function = match function_name.as_str() {
                "count" => AggregateFunction::Count,
                "sum" => AggregateFunction::Sum,
                "avg" => AggregateFunction::Avg,
                "min" => AggregateFunction::Min,
                "max" => AggregateFunction::Max,
                _ => return unsupported_at("this SQL function is not supported yet", position),
            };
            let FunctionArguments::List(arguments) = function.args else {
                return unsupported_at("aggregate arguments must use parentheses", position);
            };
            if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
                return unsupported_at(
                    "DISTINCT and ordered aggregate arguments are not supported yet",
                    position,
                );
            }
            let argument = match arguments.args.as_slice() {
                [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                    if aggregate_function == AggregateFunction::Count =>
                {
                    None
                }
                [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] => {
                    Some(Box::new(convert_expr(argument.clone(), sql)?))
                }
                _ => {
                    return unsupported_at(
                        "aggregate requires one expression, or COUNT(*)",
                        position,
                    );
                }
            };
            ParsedExprKind::Aggregate {
                function: aggregate_function,
                argument,
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
                .or_else(|| named_at_parameter_index(&parameter))
                .ok_or_else(|| {
                    DbError::new("42P02", format!("invalid parameter reference {parameter}"))
                        .with_position_opt(position)
                })?;
            Ok(ParsedExprKind::Parameter(index))
        }
        _ => unsupported_at("this literal form is not supported yet", position),
    }
}

fn named_at_parameter_index(value: &str) -> Option<usize> {
    value
        .get(..2)
        .filter(|prefix| prefix.eq_ignore_ascii_case("@p"))
        .and_then(|_| value.get(2..))
        .and_then(|index| index.parse::<usize>().ok())
        .filter(|index| *index > 0)
}

fn parse_temporal_literal(
    data_type: DataType,
    value: &str,
    position: Option<usize>,
) -> Result<Value> {
    let data_type_label = data_type.to_string();
    let invalid = || {
        DbError::new(
            "22007",
            format!("invalid {data_type_label} literal {value:?}"),
        )
        .with_position_opt(position)
    };
    match data_type {
        DataType::Date => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|_| invalid()),
        DataType::Time(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
                .map(Value::Time)
                .map_err(|_| invalid())
        }
        DataType::Timestamp(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
        | DataType::Datetime(_) => NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
            .map(Value::Timestamp)
            .map_err(|_| invalid()),
        _ => unsupported_at("this typed literal is not supported yet", position),
    }
}

fn convert_data_type(data_type: DataType) -> Result<ScalarType> {
    match data_type {
        DataType::Bool | DataType::Boolean => Ok(ScalarType::Boolean),
        DataType::Int2(_) | DataType::SmallInt(_) | DataType::TinyInt(_) => Ok(ScalarType::Int16),
        DataType::Int(_) | DataType::Int4(_) | DataType::Integer(_) | DataType::MediumInt(_) => {
            Ok(ScalarType::Int32)
        }
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
        DataType::CharacterVarying(length)
        | DataType::Varchar(length)
        | DataType::Nvarchar(length) => Ok(ScalarType::Varchar {
            length: character_length(length)?,
        }),
        DataType::Text => Ok(ScalarType::Text),
        DataType::Bytea
        | DataType::Binary(_)
        | DataType::Varbinary(_)
        | DataType::Blob(_)
        | DataType::TinyBlob
        | DataType::MediumBlob
        | DataType::LongBlob => Ok(ScalarType::Binary),
        DataType::Date => Ok(ScalarType::Date),
        DataType::Time(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            Ok(ScalarType::Time)
        }
        DataType::Timestamp(_, timezone) => Ok(ScalarType::Timestamp {
            with_timezone: matches!(timezone, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz),
        }),
        DataType::Datetime(_) => Ok(ScalarType::Timestamp {
            with_timezone: false,
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
        DataType::Custom(name, modifiers)
            if modifiers.is_empty()
                && name.to_string().eq_ignore_ascii_case("uniqueidentifier") =>
        {
            Ok(ScalarType::Uuid)
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

fn convert_index_method(method: Option<IndexType>) -> Result<IndexMethod> {
    match method {
        None | Some(IndexType::BTree) => Ok(IndexMethod::BTree),
        Some(IndexType::Custom(name))
            if name.quote_style.is_none() && name.value.eq_ignore_ascii_case("fulltext") =>
        {
            Ok(IndexMethod::FullText)
        }
        Some(IndexType::Custom(name))
            if name.quote_style.is_none() && name.value.eq_ignore_ascii_case("hnsw") =>
        {
            Ok(IndexMethod::Hnsw)
        }
        Some(method) => unsupported(format!("index method {method} is not supported")),
    }
}

fn convert_index_options(options: Vec<SqlExpr>, sql: &str) -> Result<Vec<ParsedIndexOption>> {
    options
        .into_iter()
        .map(|option| {
            let position = span_position(sql, option.span());
            let SqlExpr::BinaryOp { left, op, right } = option else {
                return unsupported_at("index options must use name = value", position);
            };
            if op != SqlBinaryOperator::Eq {
                return unsupported_at("index options must use name = value", position);
            }
            let SqlExpr::Identifier(name) = *left else {
                return unsupported_at("index option names must be identifiers", position);
            };
            let parsed_name = convert_ident(name, sql);
            if parsed_name.name.is_quoted() {
                return unsupported_at(
                    "quoted index option names are not supported",
                    parsed_name.position,
                );
            }
            let SqlExpr::Value(value) = *right else {
                return unsupported_at(
                    "index option values must be strings or non-negative integers",
                    position,
                );
            };
            let value = match value.value {
                SqlValue::SingleQuotedString(value)
                | SqlValue::EscapedStringLiteral(value)
                | SqlValue::UnicodeStringLiteral(value)
                | SqlValue::NationalStringLiteral(value) => ParsedIndexOptionValue::Text(value),
                SqlValue::Number(value, _) => {
                    ParsedIndexOptionValue::Integer(value.parse::<usize>().map_err(|_| {
                        DbError::new("22023", "index option integer is out of range")
                            .with_position_opt(position)
                    })?)
                }
                _ => {
                    return unsupported_at(
                        "index option values must be strings or non-negative integers",
                        position,
                    );
                }
            };
            Ok(ParsedIndexOption {
                name: parsed_name,
                value,
            })
        })
        .collect()
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
    constraints: Vec<ParsedTableConstraint>,
    if_not_exists: bool,
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
        if if_not_exists {
            return Ok(BoundStatement::NoOp {
                tag: "CREATE TABLE".to_owned(),
            });
        }
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
            let default = column
                .default
                .map(|default| {
                    bind_expr(default.expression, None, Some(&column.data_type))?;
                    Ok(CatalogExpression::new(default.sql))
                })
                .transpose()?;
            Ok(NewColumn {
                name: column.name.name,
                data_type: column.data_type,
                nullable: column.nullable,
                primary_key: column.primary_key,
                unique: column.unique,
                default,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut seen_constraints = BTreeSet::new();
    let constraints = constraints
        .into_iter()
        .enumerate()
        .map(|(ordinal, constraint)| {
            let constraint = bind_table_constraint(constraint, &table, ordinal, &columns, catalog)?;
            if !seen_constraints.insert(constraint.name.clone()) {
                return Err(DbError::new(
                    "42710",
                    format!("constraint {} specified more than once", constraint.name),
                ));
            }
            Ok(constraint)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundStatement::CreateTable {
        schema,
        name: table,
        columns,
        constraints,
        if_not_exists,
    })
}

fn bind_table_constraint(
    constraint: ParsedTableConstraint,
    table_name: &Identifier,
    ordinal: usize,
    columns: &[NewColumn],
    catalog: &Catalog,
) -> Result<NewConstraint> {
    let local_names = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<BTreeSet<_>>();
    let validate_columns = |values: Vec<ParsedIdentifier>| -> Result<Vec<Identifier>> {
        let mut seen = BTreeSet::new();
        values
            .into_iter()
            .map(|column| {
                if !local_names.contains(&column.name) {
                    return Err(DbError::new(
                        UNDEFINED_COLUMN,
                        format!("column {} does not exist", column.name),
                    )
                    .with_position_opt(column.position));
                }
                if !seen.insert(column.name.clone()) {
                    return Err(DbError::new(
                        "42701",
                        format!("column {} specified more than once", column.name),
                    )
                    .with_position_opt(column.position));
                }
                Ok(column.name)
            })
            .collect()
    };
    let generated_name =
        |provided: Option<ParsedIdentifier>, columns: &[Identifier], suffix: &str| -> Identifier {
            provided.map(|value| value.name).unwrap_or_else(|| {
                let column_part = if columns.is_empty() {
                    ordinal.saturating_add(1).to_string()
                } else {
                    columns
                        .iter()
                        .map(Identifier::as_str)
                        .collect::<Vec<_>>()
                        .join("_")
                };
                Identifier::unquoted(format!(
                    "{}_{}_{}",
                    table_name.as_str(),
                    column_part,
                    suffix
                ))
            })
        };
    let (name, kind) = match constraint {
        ParsedTableConstraint::PrimaryKey { name, columns } => {
            let columns = validate_columns(columns)?;
            (
                generated_name(name, &columns, "pkey"),
                NewConstraintKind::PrimaryKey { columns },
            )
        }
        ParsedTableConstraint::Unique { name, columns } => {
            let columns = validate_columns(columns)?;
            (
                generated_name(name, &columns, "key"),
                NewConstraintKind::Unique { columns },
            )
        }
        ParsedTableConstraint::Check {
            name,
            expression,
            sql,
        } => {
            validate_check_expression(&expression, &local_names)?;
            (
                generated_name(name, &[], "check"),
                NewConstraintKind::Check {
                    expression: CatalogExpression::new(sql),
                },
            )
        }
        ParsedTableConstraint::ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        } => {
            let columns = validate_columns(columns)?;
            let referenced = resolve_table(&referenced_table, catalog)?;
            if referenced_columns.is_empty() {
                return Err(DbError::new(
                    "42830",
                    "foreign keys must name referenced columns",
                ));
            }
            let referenced_columns = referenced_columns
                .into_iter()
                .map(|column| {
                    referenced
                        .column(&column.name)
                        .map(|definition| definition.id)
                        .ok_or_else(|| {
                            DbError::new(
                                UNDEFINED_COLUMN,
                                format!("column {} does not exist", column.name),
                            )
                            .with_position_opt(column.position)
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            (
                generated_name(name, &columns, "fkey"),
                NewConstraintKind::ForeignKey {
                    columns,
                    referenced_table: referenced.id,
                    referenced_columns,
                    on_delete,
                    on_update,
                },
            )
        }
    };
    Ok(NewConstraint { name, kind })
}

fn validate_check_expression(
    expression: &ParsedExpr,
    columns: &BTreeSet<Identifier>,
) -> Result<()> {
    let mut stack = vec![expression];
    while let Some(expression) = stack.pop() {
        match &expression.kind {
            ParsedExprKind::Column(name) => {
                let Some(column) = name.parts.last() else {
                    return Err(DbError::new(SYNTAX_ERROR, "empty column reference"));
                };
                if !columns.contains(&column.name) {
                    return Err(DbError::new(
                        UNDEFINED_COLUMN,
                        format!("column {} does not exist", column.name),
                    )
                    .with_position_opt(column.position));
                }
            }
            ParsedExprKind::Unary { expr, .. } => stack.push(expr),
            ParsedExprKind::Binary { left, right, .. } => {
                stack.push(right);
                stack.push(left);
            }
            ParsedExprKind::Aggregate { .. } => {
                return unsupported("aggregate functions are not allowed in CHECK constraints");
            }
            ParsedExprKind::Literal(_) | ParsedExprKind::Parameter(_) => {}
        }
    }
    Ok(())
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

fn bind_create_index(index: ParsedCreateIndex, catalog: &Catalog) -> Result<BoundStatement> {
    let ParsedCreateIndex {
        name,
        table: table_name,
        key_columns,
        include_columns,
        unique,
        method,
        options,
        if_not_exists,
    } = index;
    let table = resolve_index_relation(&table_name, catalog)?;
    let schema = catalog
        .schema_by_id(table.schema_id)
        .ok_or_else(|| DbError::internal("index owner schema disappeared"))?;
    if schema
        .tables()
        .any(|candidate| candidate.index(&name.name).is_some())
    {
        if if_not_exists {
            return Ok(BoundStatement::NoOp {
                tag: "CREATE INDEX".to_owned(),
            });
        }
        return Err(
            DbError::new("42P07", format!("relation {} already exists", name.name))
                .with_position_opt(name.position),
        );
    }
    if key_columns.is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "CREATE INDEX requires at least one key column",
        ));
    }
    let mut seen = BTreeSet::new();
    for column in key_columns.iter().chain(&include_columns) {
        let definition = table.column(&column.name).ok_or_else(|| {
            DbError::new(
                UNDEFINED_COLUMN,
                format!("column {} does not exist", column.name),
            )
            .with_position_opt(column.position)
        })?;
        if !seen.insert(definition.id) {
            return Err(DbError::new(
                "42701",
                format!("column {} specified more than once", column.name),
            )
            .with_position_opt(column.position));
        }
    }
    let options = bind_index_options(
        method,
        table,
        &key_columns,
        &include_columns,
        unique,
        options,
    )?;
    Ok(BoundStatement::CreateIndex {
        table_id: table.id,
        index: NewIndex {
            name: name.name,
            key_columns: key_columns.into_iter().map(|column| column.name).collect(),
            include_columns: include_columns
                .into_iter()
                .map(|column| column.name)
                .collect(),
            unique,
            method,
            options,
        },
        if_not_exists,
    })
}

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
                let default = column
                    .default
                    .map(|default| {
                        bind_expr(default.expression, None, Some(&column.data_type))?;
                        Ok(CatalogExpression::new(default.sql))
                    })
                    .transpose()?;
                let column = NewColumn {
                    name: column.name.name,
                    data_type: column.data_type,
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
            ParsedAlterTableOperation::SetDataType { column, data_type } => {
                bound.push(BoundAlterTableOperation::SetDataType {
                    column_id: resolve_column_id(&table, column)?,
                    data_type,
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
        | BoundStatement::ViewSelect { schema, .. }
        | BoundStatement::RoutineSelect { schema, .. } => Ok(schema.clone()),
        _ => unsupported("views require a SELECT query"),
    }
}

fn bound_statement_references(statement: &BoundStatement) -> Vec<CatalogObjectRef> {
    let mut references = match statement {
        BoundStatement::Select { table_id, .. } => vec![CatalogObjectRef::Table(*table_id)],
        BoundStatement::AdvancedSelect { table, joins, .. } => std::iter::once(table.table_id)
            .chain(joins.iter().map(|join| join.table.table_id))
            .map(CatalogObjectRef::Table)
            .collect(),
        BoundStatement::ViewSelect { view_id, .. } => {
            vec![CatalogObjectRef::View(*view_id)]
        }
        BoundStatement::RoutineSelect { routine_id, .. } => {
            vec![CatalogObjectRef::Routine(*routine_id)]
        }
        _ => Vec::new(),
    };
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
}

struct AdvancedSelectInput {
    table: ParsedTable,
    joins: Vec<ParsedJoin>,
    projection: Vec<ParsedProjection>,
    filter: Option<ParsedExpr>,
    group_by: Vec<ParsedExpr>,
    having: Option<ParsedExpr>,
    order_by: Vec<ParsedOrder>,
    limit: Option<ParsedExpr>,
}

fn bind_advanced_select(input: AdvancedSelectInput, catalog: &Catalog) -> Result<BoundStatement> {
    let AdvancedSelectInput {
        table,
        joins,
        projection,
        filter,
        group_by,
        having,
        order_by,
        limit,
    } = input;
    let mut inputs = Vec::new();
    let table = bind_input_table(table, false, catalog, &mut inputs)?;
    let mut bound_joins = Vec::new();
    for join in joins {
        let nullable = join.kind == JoinKind::Left;
        let table = bind_input_table(join.table, nullable, catalog, &mut inputs)?;
        let on = bind_multi_boolean(join.on, &inputs)?;
        if bound_expr_has_aggregate(&on) {
            return Err(DbError::new(
                "42803",
                "aggregate functions are not allowed in JOIN conditions",
            ));
        }
        bound_joins.push(BoundJoin {
            table,
            kind: join.kind,
            on,
        });
    }

    let mut bound_projection = Vec::new();
    for item in projection {
        match item {
            ParsedProjection::Wildcard => {
                for input in &inputs {
                    bound_projection.push(BoundProjection {
                        expr: BoundExpr {
                            kind: BoundExprKind::Column { index: input.index },
                            data_type: input.data_type.clone(),
                            nullable: input.nullable,
                        },
                        field: Field::new(
                            input.name.as_str(),
                            input.data_type.clone(),
                            input.nullable,
                        ),
                    });
                }
            }
            ParsedProjection::Expression { expr, alias } => {
                let default_name = projection_name(&expr);
                let bound = bind_expr_multi(expr, &inputs, None, true)?;
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
        .map(|expr| bind_multi_boolean(expr, &inputs))
        .transpose()?;
    if filter.as_ref().is_some_and(bound_expr_has_aggregate) {
        return Err(DbError::new(
            "42803",
            "aggregate functions are not allowed in WHERE",
        ));
    }
    let group_by = group_by
        .into_iter()
        .map(|expr| bind_expr_multi(expr, &inputs, None, false))
        .collect::<Result<Vec<_>>>()?;
    let having = having
        .map(|expr| bind_multi_boolean(expr, &inputs))
        .transpose()?;
    let aggregate = !group_by.is_empty()
        || bound_projection
            .iter()
            .any(|projection| bound_expr_has_aggregate(&projection.expr))
        || having.as_ref().is_some_and(bound_expr_has_aggregate);
    if aggregate {
        for projection in &bound_projection {
            validate_grouped_expr(&projection.expr, &group_by)?;
        }
        if let Some(having) = &having {
            validate_grouped_expr(having, &group_by)?;
        }
    } else if having.is_some() {
        return Err(DbError::new(
            "42803",
            "HAVING requires grouping or an aggregate",
        ));
    }

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
                column_index: resolve_input_column(&column, &inputs)?.index,
                ascending: order.ascending,
                nulls_first: order.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let limit = limit
        .map(|expr| bind_expr_multi(expr, &inputs, Some(&ScalarType::Int64), false))
        .transpose()?;
    let schema = Schema::new(
        bound_projection
            .iter()
            .map(|projection| projection.field.clone())
            .collect(),
    );
    Ok(BoundStatement::AdvancedSelect {
        table,
        joins: bound_joins,
        schema,
        projection: bound_projection,
        filter,
        group_by,
        having,
        order_by,
        limit,
        aggregate,
    })
}

fn bind_input_table(
    parsed: ParsedTable,
    nullable: bool,
    catalog: &Catalog,
    inputs: &mut Vec<InputColumn>,
) -> Result<BoundTable> {
    let table = resolve_table(&parsed.name, catalog)?;
    let binding = parsed
        .alias
        .as_ref()
        .map_or_else(|| table.name.clone(), |alias| alias.name.clone());
    if inputs.iter().any(|input| input.binding == binding) {
        return Err(DbError::new(
            "42712",
            format!("table name {binding} specified more than once"),
        ));
    }
    let offset = inputs.len();
    inputs.extend(
        table
            .columns()
            .iter()
            .enumerate()
            .map(|(column_offset, column)| InputColumn {
                binding: binding.clone(),
                name: column.name.clone(),
                index: offset + column_offset,
                data_type: column.data_type.clone(),
                nullable: nullable || column.nullable,
            }),
    );
    Ok(BoundTable {
        table_id: table.id,
        binding,
        offset,
        width: table.columns().len(),
        nullable,
    })
}

fn bind_multi_boolean(expr: ParsedExpr, inputs: &[InputColumn]) -> Result<BoundExpr> {
    let position = expr.position;
    let bound = bind_expr_multi(expr, inputs, Some(&ScalarType::Boolean), true)?;
    if bound.data_type != ScalarType::Boolean {
        return Err(DbError::new(DATATYPE_MISMATCH, "predicate must be boolean")
            .with_position_opt(position));
    }
    Ok(bound)
}

fn bind_expr_multi(
    expr: ParsedExpr,
    inputs: &[InputColumn],
    expected: Option<&ScalarType>,
    allow_aggregate: bool,
) -> Result<BoundExpr> {
    let position = expr.position;
    match expr.kind {
        ParsedExprKind::Column(name) => {
            let column = resolve_input_column(&name, inputs)?;
            if let Some(expected) = expected {
                ensure_types_compatible(&column.data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::Column {
                    index: column.index,
                },
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
        ParsedExprKind::Unary { op, expr } => {
            let expected_type = match op {
                UnaryOperator::Not => Some(&ScalarType::Boolean),
                UnaryOperator::Negate => expected,
            };
            let bound = bind_expr_multi(*expr, inputs, expected_type, allow_aggregate)?;
            match op {
                UnaryOperator::Not if bound.data_type != ScalarType::Boolean => Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "NOT operand must be boolean",
                )
                .with_position_opt(position)),
                UnaryOperator::Negate if !is_numeric(&bound.data_type) => Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "unary minus requires a numeric operand",
                )
                .with_position_opt(position)),
                _ => {
                    let data_type = bound.data_type.clone();
                    let nullable = bound.nullable;
                    Ok(BoundExpr {
                        kind: BoundExprKind::Unary {
                            op,
                            expr: Box::new(bound),
                        },
                        data_type,
                        nullable,
                    })
                }
            }
        }
        ParsedExprKind::Binary { left, op, right } => {
            bind_multi_binary(*left, op, *right, inputs, position, allow_aggregate)
        }
        ParsedExprKind::Aggregate { function, argument } => {
            if !allow_aggregate {
                return Err(DbError::new(
                    "42803",
                    "aggregate functions are not allowed in this clause",
                )
                .with_position_opt(position));
            }
            let argument = argument
                .map(|argument| bind_expr_multi(*argument, inputs, None, false))
                .transpose()?;
            let (data_type, nullable) = match (function, argument.as_ref()) {
                (AggregateFunction::Count, _) => (ScalarType::Int64, false),
                (AggregateFunction::Avg, Some(argument)) if is_numeric(&argument.data_type) => {
                    (ScalarType::Float64, true)
                }
                (AggregateFunction::Sum, Some(argument)) if is_numeric(&argument.data_type) => {
                    let data_type = match argument.data_type {
                        ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64 => {
                            ScalarType::Int64
                        }
                        ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
                        ScalarType::Decimal { .. } => argument.data_type.clone(),
                        _ => unreachable!("numeric guard"),
                    };
                    (data_type, true)
                }
                (AggregateFunction::Min | AggregateFunction::Max, Some(argument))
                    if indexable_type(&argument.data_type) =>
                {
                    (argument.data_type.clone(), true)
                }
                _ => {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        "aggregate argument has an incompatible type",
                    )
                    .with_position_opt(position));
                }
            };
            Ok(BoundExpr {
                kind: BoundExprKind::Aggregate {
                    function,
                    argument: argument.map(Box::new),
                },
                data_type,
                nullable,
            })
        }
    }
}

fn bind_multi_binary(
    left: ParsedExpr,
    op: BinaryOperator,
    right: ParsedExpr,
    inputs: &[InputColumn],
    position: Option<usize>,
    allow_aggregate: bool,
) -> Result<BoundExpr> {
    if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
        let left = bind_expr_multi(left, inputs, Some(&ScalarType::Boolean), allow_aggregate)?;
        let right = bind_expr_multi(right, inputs, Some(&ScalarType::Boolean), allow_aggregate)?;
        return Ok(BoundExpr {
            nullable: left.nullable || right.nullable,
            data_type: ScalarType::Boolean,
            kind: BoundExprKind::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
        });
    }
    let left_type = infer_multi_type(&left, inputs)?;
    let right_type = infer_multi_type(&right, inputs)?;
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
    let left = bind_expr_multi(left, inputs, Some(&comparison_type), allow_aggregate)?;
    let right = bind_expr_multi(right, inputs, Some(&comparison_type), allow_aggregate)?;
    Ok(BoundExpr {
        nullable: left.nullable || right.nullable,
        data_type: ScalarType::Boolean,
        kind: BoundExprKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
    })
}

fn infer_multi_type(expr: &ParsedExpr, inputs: &[InputColumn]) -> Result<Option<ScalarType>> {
    match &expr.kind {
        ParsedExprKind::Column(column) => Ok(Some(
            resolve_input_column(column, inputs)?.data_type.clone(),
        )),
        ParsedExprKind::Literal(value) => Ok(value.scalar_type()),
        ParsedExprKind::Parameter(_) => Ok(None),
        ParsedExprKind::Unary { op, expr } => match op {
            UnaryOperator::Not => Ok(Some(ScalarType::Boolean)),
            UnaryOperator::Negate => infer_multi_type(expr, inputs),
        },
        ParsedExprKind::Binary { .. } => Ok(Some(ScalarType::Boolean)),
        ParsedExprKind::Aggregate { function, argument } => match function {
            AggregateFunction::Count => Ok(Some(ScalarType::Int64)),
            AggregateFunction::Avg => Ok(Some(ScalarType::Float64)),
            AggregateFunction::Sum => {
                let data_type = argument
                    .as_ref()
                    .ok_or_else(|| {
                        DbError::new(DATATYPE_MISMATCH, "aggregate requires an argument")
                            .with_position_opt(expr.position)
                    })
                    .and_then(|argument| infer_multi_type(argument, inputs))?;
                Ok(data_type.map(|data_type| match data_type {
                    ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64 => ScalarType::Int64,
                    ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
                    other => other,
                }))
            }
            AggregateFunction::Min | AggregateFunction::Max => argument
                .as_ref()
                .ok_or_else(|| {
                    DbError::new(DATATYPE_MISMATCH, "aggregate requires an argument")
                        .with_position_opt(expr.position)
                })
                .and_then(|argument| infer_multi_type(argument, inputs)),
        },
    }
}

fn resolve_input_column<'a>(
    name: &ParsedObjectName,
    inputs: &'a [InputColumn],
) -> Result<&'a InputColumn> {
    let (qualifier, column, position) = match name.parts.as_slice() {
        [column] => (None, &column.name, column.position),
        [qualifier, column] => (Some(&qualifier.name), &column.name, column.position),
        _ => {
            return unsupported_at(
                "column references may contain at most a table qualifier",
                name.parts.first().and_then(|part| part.position),
            );
        }
    };
    let matches = inputs
        .iter()
        .filter(|input| {
            &input.name == column && qualifier.is_none_or(|qualifier| &input.binding == qualifier)
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok(*column),
        [] => Err(
            DbError::new(UNDEFINED_COLUMN, format!("column {column} does not exist"))
                .with_position_opt(position),
        ),
        _ => Err(
            DbError::new("42702", format!("column reference {column} is ambiguous"))
                .with_position_opt(position),
        ),
    }
}

fn bound_expr_has_aggregate(expr: &BoundExpr) -> bool {
    match &expr.kind {
        BoundExprKind::Aggregate { .. } => true,
        BoundExprKind::Unary { expr, .. } => bound_expr_has_aggregate(expr),
        BoundExprKind::Binary { left, right, .. } => {
            bound_expr_has_aggregate(left) || bound_expr_has_aggregate(right)
        }
        BoundExprKind::Column { .. }
        | BoundExprKind::Literal(_)
        | BoundExprKind::Parameter { .. } => false,
    }
}

fn validate_grouped_expr(expr: &BoundExpr, group_by: &[BoundExpr]) -> Result<()> {
    if group_by.iter().any(|group| group == expr) {
        return Ok(());
    }
    match &expr.kind {
        BoundExprKind::Aggregate { .. }
        | BoundExprKind::Literal(_)
        | BoundExprKind::Parameter { .. } => Ok(()),
        BoundExprKind::Column { .. } => Err(DbError::new(
            "42803",
            "column must appear in GROUP BY or be used in an aggregate function",
        )),
        BoundExprKind::Unary { expr, .. } => validate_grouped_expr(expr, group_by),
        BoundExprKind::Binary { left, right, .. } => {
            validate_grouped_expr(left, group_by)?;
            validate_grouped_expr(right, group_by)
        }
    }
}

fn bind_select(
    table_name: ParsedObjectName,
    projection: Vec<ParsedProjection>,
    filter: Option<ParsedExpr>,
    order_by: Vec<ParsedOrder>,
    limit: Option<ParsedExpr>,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let (schema_name, relation_name, _) = split_table_name(&table_name)?;
    if let Some(view) = catalog.view(&schema_name, &relation_name) {
        return bind_view_select(
            view, projection, filter, order_by, limit, catalog, view_depth,
        );
    }
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

fn bind_view_select(
    view: &ViewDefinition,
    projection: Vec<ParsedProjection>,
    filter: Option<ParsedExpr>,
    order_by: Vec<ParsedOrder>,
    limit: Option<ParsedExpr>,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    if filter.is_some() || !order_by.is_empty() || limit.is_some() {
        return unsupported(
            "WHERE, ORDER BY, and LIMIT on views are not supported in this milestone",
        );
    }
    let source = match view.kind {
        ViewKind::Regular => {
            bind_with_view_depth(parse(&view.query)?, catalog, view_depth.saturating_add(1))?
        }
        ViewKind::Materialized => {
            if !view.populated {
                return Err(DbError::new(
                    "55000",
                    format!("materialized view {} has not been populated", view.name),
                )
                .with_hint("run REFRESH MATERIALIZED VIEW before querying it"));
            }
            let table_id = view.materialized_table_id.ok_or_else(|| {
                DbError::internal("materialized view is missing its backing table")
            })?;
            let projection = view
                .output
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| BoundProjection {
                    expr: BoundExpr {
                        kind: BoundExprKind::Column { index },
                        data_type: field.data_type.clone(),
                        nullable: field.nullable,
                    },
                    field: field.clone(),
                })
                .collect();
            BoundStatement::Select {
                table_id,
                schema: view.output.clone(),
                projection,
                filter: None,
                order_by: Vec::new(),
                limit: None,
            }
        }
    };
    let source_schema = bound_query_schema(&source)?;
    if source_schema.fields.len() != view.output.fields.len() {
        return Err(DbError::new(
            "42P16",
            "stored view query output no longer matches its catalog definition",
        ));
    }

    let mut positions = Vec::new();
    let mut fields = Vec::new();
    for item in projection {
        match item {
            ParsedProjection::Wildcard => {
                positions.extend(0..view.output.fields.len());
                fields.extend(view.output.fields.iter().cloned());
            }
            ParsedProjection::Expression { expr, alias } => {
                let ParsedExprKind::Column(name) = expr.kind else {
                    return unsupported_at(
                        "view projection supports columns and wildcard only",
                        expr.position,
                    );
                };
                let column = name
                    .parts
                    .last()
                    .ok_or_else(|| DbError::new(SYNTAX_ERROR, "view column reference is empty"))?;
                let position = view
                    .output
                    .fields
                    .iter()
                    .position(|field| field.name == column.name.as_str())
                    .ok_or_else(|| {
                        DbError::new(
                            UNDEFINED_COLUMN,
                            format!("column {} does not exist", column.name),
                        )
                        .with_position_opt(column.position)
                    })?;
                let mut field = view.output.fields[position].clone();
                if let Some(alias) = alias {
                    field.name = alias.name.as_str().to_owned();
                }
                positions.push(position);
                fields.push(field);
            }
        }
    }
    if fields.is_empty() {
        return Err(DbError::new(SYNTAX_ERROR, "SELECT projection is empty"));
    }
    Ok(BoundStatement::ViewSelect {
        view_id: view.id,
        source: Box::new(source),
        schema: Schema::new(fields),
        projection: positions,
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
    bind_expr_with_parameter_types(expr, table, expected, &BTreeMap::new())
}

fn bind_expr_with_parameter_types(
    expr: ParsedExpr,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
    parameter_types: &BTreeMap<usize, ScalarType>,
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
            let declared = parameter_types.get(&index);
            if let (Some(declared), Some(expected)) = (declared, expected) {
                ensure_types_compatible(declared, expected, position)?;
            }
            let data_type = expected
                .cloned()
                .or_else(|| declared.cloned())
                .ok_or_else(|| {
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
                let expr = bind_expr_with_parameter_types(
                    *expr,
                    table,
                    Some(&ScalarType::Boolean),
                    parameter_types,
                )?;
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
                let expr = bind_expr_with_parameter_types(*expr, table, expected, parameter_types)?;
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
            bind_binary(*left, op, *right, table, position, parameter_types)
        }
        ParsedExprKind::Aggregate { .. } => {
            unsupported_at("aggregate is not valid in this statement", position)
        }
    }
}

fn bind_binary(
    left: ParsedExpr,
    op: BinaryOperator,
    right: ParsedExpr,
    table: Option<&TableDefinition>,
    position: Option<usize>,
    parameter_types: &BTreeMap<usize, ScalarType>,
) -> Result<BoundExpr> {
    if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
        let left = bind_expr_with_parameter_types(
            left,
            table,
            Some(&ScalarType::Boolean),
            parameter_types,
        )?;
        let right = bind_expr_with_parameter_types(
            right,
            table,
            Some(&ScalarType::Boolean),
            parameter_types,
        )?;
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

    let left_type = infer_expr_type(&left, table, parameter_types)?;
    let right_type = infer_expr_type(&right, table, parameter_types)?;
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
    let left =
        bind_expr_with_parameter_types(left, table, Some(&comparison_type), parameter_types)?;
    let right =
        bind_expr_with_parameter_types(right, table, Some(&comparison_type), parameter_types)?;
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
    parameter_types: &BTreeMap<usize, ScalarType>,
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
        ParsedExprKind::Parameter(index) => Ok(parameter_types.get(index).cloned()),
        ParsedExprKind::Unary { op, expr: inner } => match op {
            UnaryOperator::Not => Ok(Some(ScalarType::Boolean)),
            UnaryOperator::Negate => infer_expr_type(inner, table, parameter_types),
        },
        ParsedExprKind::Binary { .. } => Ok(Some(ScalarType::Boolean)),
        ParsedExprKind::Aggregate { .. } => {
            unsupported_at("aggregate is not valid in this statement", expr.position)
        }
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

fn parse_transaction_begin(sql: &str) -> Result<Option<ParsedStatement>> {
    let mut tokens = significant_tokens(sql);
    if matches!(tokens.last(), Some(Token::SemiColon)) {
        tokens.pop();
    }
    let mut index = 0_usize;
    if consume_keyword(&tokens, &mut index, "BEGIN") {
        if token_is_keyword(&tokens, index, "WORK")
            || token_is_keyword(&tokens, index, "TRANSACTION")
        {
            index += 1;
        }
    } else if consume_keyword(&tokens, &mut index, "START") {
        if !consume_keyword(&tokens, &mut index, "TRANSACTION") {
            return Ok(None);
        }
    } else {
        return Ok(None);
    }

    let mut characteristics = TransactionCharacteristics::default();
    let mut isolation_seen = false;
    let mut access_seen = false;
    let mut deferrable_seen = false;
    while index < tokens.len() {
        if matches!(tokens.get(index), Some(Token::Comma)) {
            return Err(transaction_syntax_error(
                "transaction mode is missing before comma",
            ));
        }
        if consume_keyword(&tokens, &mut index, "ISOLATION") {
            if isolation_seen {
                return Err(transaction_syntax_error(
                    "transaction isolation level was specified more than once",
                ));
            }
            isolation_seen = true;
            expect_keyword(&tokens, &mut index, "LEVEL")?;
            characteristics.isolation_level =
                if consume_keyword_sequence(&tokens, &mut index, &["READ", "UNCOMMITTED"])
                    || consume_keyword_sequence(&tokens, &mut index, &["READ", "COMMITTED"])
                {
                    IsolationLevel::ReadCommitted
                } else if consume_keyword_sequence(&tokens, &mut index, &["REPEATABLE", "READ"]) {
                    IsolationLevel::RepeatableRead
                } else if consume_keyword(&tokens, &mut index, "SERIALIZABLE") {
                    IsolationLevel::Serializable
                } else if consume_keyword(&tokens, &mut index, "SNAPSHOT") {
                    return unsupported("SNAPSHOT isolation is not supported");
                } else {
                    return Err(transaction_syntax_error(
                        "expected READ COMMITTED, REPEATABLE READ, or SERIALIZABLE",
                    ));
                };
        } else if consume_keyword_sequence(&tokens, &mut index, &["READ", "ONLY"]) {
            if access_seen {
                return Err(transaction_syntax_error(
                    "transaction access mode was specified more than once",
                ));
            }
            access_seen = true;
            characteristics.access_mode = TransactionAccessMode::ReadOnly;
        } else if consume_keyword_sequence(&tokens, &mut index, &["READ", "WRITE"]) {
            if access_seen {
                return Err(transaction_syntax_error(
                    "transaction access mode was specified more than once",
                ));
            }
            access_seen = true;
            characteristics.access_mode = TransactionAccessMode::ReadWrite;
        } else if consume_keyword(&tokens, &mut index, "DEFERRABLE") {
            if deferrable_seen {
                return Err(transaction_syntax_error(
                    "transaction deferrability was specified more than once",
                ));
            }
            deferrable_seen = true;
            characteristics.deferrable = true;
        } else if consume_keyword_sequence(&tokens, &mut index, &["NOT", "DEFERRABLE"]) {
            if deferrable_seen {
                return Err(transaction_syntax_error(
                    "transaction deferrability was specified more than once",
                ));
            }
            deferrable_seen = true;
            characteristics.deferrable = false;
        } else {
            return Err(transaction_syntax_error(
                "unrecognized transaction mode or trailing token",
            ));
        }
        if matches!(tokens.get(index), Some(Token::Comma)) {
            index += 1;
            if index == tokens.len() || matches!(tokens.get(index), Some(Token::Comma)) {
                return Err(transaction_syntax_error(
                    "transaction mode is missing after comma",
                ));
            }
        }
    }
    Ok(Some(ParsedStatement::Begin {
        characteristics: characteristics.validate()?,
    }))
}

fn convert_transaction_modes(
    modes: Vec<TransactionMode>,
    deferrable: bool,
) -> Result<TransactionCharacteristics> {
    let mut characteristics = TransactionCharacteristics {
        deferrable,
        ..TransactionCharacteristics::default()
    };
    let mut isolation_seen = false;
    let mut access_seen = false;
    for mode in modes {
        match mode {
            TransactionMode::IsolationLevel(level) => {
                if isolation_seen {
                    return Err(transaction_syntax_error(
                        "transaction isolation level was specified more than once",
                    ));
                }
                isolation_seen = true;
                characteristics.isolation_level = match level {
                    SqlTransactionIsolationLevel::ReadUncommitted
                    | SqlTransactionIsolationLevel::ReadCommitted => IsolationLevel::ReadCommitted,
                    SqlTransactionIsolationLevel::RepeatableRead => IsolationLevel::RepeatableRead,
                    SqlTransactionIsolationLevel::Serializable => IsolationLevel::Serializable,
                    SqlTransactionIsolationLevel::Snapshot => {
                        return unsupported("SNAPSHOT isolation is not supported");
                    }
                };
            }
            TransactionMode::AccessMode(mode) => {
                if access_seen {
                    return Err(transaction_syntax_error(
                        "transaction access mode was specified more than once",
                    ));
                }
                access_seen = true;
                characteristics.access_mode = match mode {
                    SqlTransactionAccessMode::ReadOnly => TransactionAccessMode::ReadOnly,
                    SqlTransactionAccessMode::ReadWrite => TransactionAccessMode::ReadWrite,
                };
            }
        }
    }
    characteristics.validate()
}

fn convert_transaction_chain(chain: bool, sql: &str) -> TransactionChain {
    if chain {
        TransactionChain::Chain
    } else if has_keyword_sequence(sql, &["AND", "NO", "CHAIN"]) {
        TransactionChain::NoChain
    } else {
        TransactionChain::Default
    }
}

fn consume_keyword(tokens: &[Token], index: &mut usize, keyword: &str) -> bool {
    if token_is_keyword(tokens, *index, keyword) {
        *index += 1;
        true
    } else {
        false
    }
}

fn consume_keyword_sequence(tokens: &[Token], index: &mut usize, keywords: &[&str]) -> bool {
    if keywords
        .iter()
        .enumerate()
        .all(|(offset, keyword)| token_is_keyword(tokens, *index + offset, keyword))
    {
        *index += keywords.len();
        true
    } else {
        false
    }
}

fn token_is_keyword(tokens: &[Token], index: usize, keyword: &str) -> bool {
    tokens
        .get(index)
        .is_some_and(|token| is_unquoted_word(token, keyword))
}

fn transaction_syntax_error(message: impl Into<String>) -> DbError {
    DbError::new(SYNTAX_ERROR, message)
}

fn has_keyword_sequence(sql: &str, sequence: &[&str]) -> bool {
    significant_tokens(sql)
        .windows(sequence.len())
        .any(|window| {
            window
                .iter()
                .zip(sequence)
                .all(|(token, keyword)| is_unquoted_word(token, keyword))
        })
}

fn parse_vacuum_analyze(sql: &str) -> Result<Option<ParsedStatement>> {
    let tokens = significant_tokens(sql);
    if tokens.len() < 2
        || !is_unquoted_word(&tokens[0], "VACUUM")
        || !is_unquoted_word(&tokens[1], "ANALYZE")
    {
        return Ok(None);
    }
    let mut normalized = String::from("VACUUM");
    for token in &tokens[2..] {
        normalized.push(' ');
        normalized.push_str(&token.to_string());
    }
    let mut statements = parse_source_statements(&normalized, SqlDialect::PostgreSql)
        .map_err(|error| DbError::new(SYNTAX_ERROR, error.to_string()))?;
    if statements.len() != 1 {
        return Err(DbError::new(
            FEATURE_NOT_SUPPORTED,
            "exactly one SQL statement must be executed at a time",
        ));
    }
    let parsed = convert_statement(
        statements
            .pop()
            .ok_or_else(|| DbError::new(SYNTAX_ERROR, "VACUUM statement is empty"))?,
        &normalized,
    )?;
    let ParsedStatement::Vacuum { table, .. } = parsed else {
        return Err(DbError::internal(
            "VACUUM ANALYZE normalization produced a non-VACUUM statement",
        ));
    };
    Ok(Some(ParsedStatement::Vacuum {
        table,
        analyze: true,
    }))
}

fn significant_tokens(sql: &str) -> Vec<Token> {
    let dialect = PostgreSqlDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Vec::new();
    };
    tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect()
}

fn is_unquoted_word(token: &Token, expected: &str) -> bool {
    matches!(
        token,
        Token::Word(word)
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected)
    )
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
                        default: None,
                    },
                    NewColumn {
                        name: Identifier::unquoted("title"),
                        data_type: ScalarType::Text,
                        nullable: false,
                        primary_key: false,
                        unique: false,
                        default: None,
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
    fn parses_and_binds_owned_transaction_control_variants() {
        let catalog = Catalog::default();
        let begin = ParsedStatement::Begin {
            characteristics: TransactionCharacteristics::default(),
        };
        let bound_begin = BoundStatement::Begin {
            characteristics: TransactionCharacteristics::default(),
        };
        for (sql, parsed, bound) in [
            ("BEGIN", begin.clone(), bound_begin.clone()),
            (
                "  bEgIn \n TrAnSaCtIoN ; ",
                begin.clone(),
                bound_begin.clone(),
            ),
            ("\n StArT \t TrAnSaCtIoN ;", begin, bound_begin),
            (
                "cOmMiT \n WoRk;",
                ParsedStatement::Commit {
                    chain: TransactionChain::Default,
                },
                BoundStatement::Commit {
                    chain: TransactionChain::Default,
                },
            ),
            (
                "\r\n RoLlBaCk \t TrAnSaCtIoN ;",
                ParsedStatement::Rollback {
                    chain: TransactionChain::Default,
                },
                BoundStatement::Rollback {
                    chain: TransactionChain::Default,
                },
            ),
        ] {
            let actual = parse(sql).expect("parse transaction control");
            assert_eq!(actual, parsed, "{sql}");
            assert_eq!(
                bind(actual, &catalog).expect("bind transaction control"),
                bound,
                "{sql}"
            );
        }
    }

    #[test]
    fn parses_transaction_modes_chaining_and_savepoints() {
        assert_eq!(
            parse("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE").expect("begin"),
            ParsedStatement::Begin {
                characteristics: TransactionCharacteristics {
                    isolation_level: IsolationLevel::Serializable,
                    access_mode: TransactionAccessMode::ReadOnly,
                    deferrable: true,
                },
            }
        );
        assert_eq!(
            parse("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED, READ WRITE NOT DEFERRABLE")
                .expect("start"),
            ParsedStatement::Begin {
                characteristics: TransactionCharacteristics::default(),
            }
        );
        assert_eq!(
            parse("COMMIT AND CHAIN").expect("commit chain"),
            ParsedStatement::Commit {
                chain: TransactionChain::Chain,
            }
        );
        assert_eq!(
            parse("ROLLBACK WORK AND NO CHAIN").expect("rollback no chain"),
            ParsedStatement::Rollback {
                chain: TransactionChain::NoChain,
            }
        );
        assert_eq!(
            parse("SAVEPOINT before_update").expect("savepoint"),
            ParsedStatement::Savepoint {
                name: ParsedIdentifier {
                    name: Identifier::unquoted("before_update"),
                    position: Some(11),
                },
            }
        );
        assert!(matches!(
            parse("ROLLBACK TO SAVEPOINT before_update").expect("rollback to"),
            ParsedStatement::RollbackTo { name }
                if name.name == Identifier::unquoted("before_update")
        ));
        assert!(matches!(
            parse("RELEASE SAVEPOINT before_update").expect("release"),
            ParsedStatement::ReleaseSavepoint { name }
                if name.name == Identifier::unquoted("before_update")
        ));

        assert_eq!(
            parse("BEGIN READ WRITE DEFERRABLE")
                .expect_err("invalid deferrable")
                .sql_state,
            "25001"
        );
        assert_eq!(
            parse("BEGIN READ ONLY READ WRITE")
                .expect_err("duplicate access mode")
                .sql_state,
            SYNTAX_ERROR
        );
        assert_eq!(
            parse("BEGIN ISOLATION LEVEL SNAPSHOT")
                .expect_err("snapshot")
                .sql_state,
            FEATURE_NOT_SUPPORTED
        );
    }

    #[test]
    fn parses_and_binds_transactional_maintenance() {
        let catalog = catalog_with_documents();
        let documents = catalog
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        assert_eq!(
            bind(parse("ANALYZE documents").expect("analyze"), &catalog).expect("bind analyze"),
            BoundStatement::Analyze {
                table_id: Some(documents),
            }
        );
        assert_eq!(
            bind(parse("VACUUM documents").expect("vacuum"), &catalog).expect("bind vacuum"),
            BoundStatement::Vacuum {
                table_id: Some(documents),
                analyze: false,
            }
        );
        assert_eq!(
            bind(
                parse("VACUUM ANALYZE documents").expect("vacuum analyze"),
                &catalog,
            )
            .expect("bind vacuum analyze"),
            BoundStatement::Vacuum {
                table_id: Some(documents),
                analyze: true,
            }
        );
        assert_eq!(
            parse("VACUUM FULL documents")
                .expect_err("vacuum full")
                .sql_state,
            FEATURE_NOT_SUPPORTED
        );
        assert_eq!(
            parse("ANALYZE documents (id)")
                .expect_err("column analyze")
                .sql_state,
            FEATURE_NOT_SUPPORTED
        );
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
        let ParsedStatement::CreateTable { name, columns, .. } = statement else {
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
            "WITH d AS (SELECT * FROM documents) SELECT * FROM d",
            "CREATE TABLE inherited (id BIGINT) INHERITS (documents)",
            "CREATE INDEX unsupported_hash ON documents USING HASH (id)",
        ] {
            let error = parse(sql)
                .and_then(|statement| bind(statement, &catalog))
                .expect_err("unsupported syntax");
            assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED, "{sql}");
        }
    }

    #[test]
    fn binds_indexes_joins_aggregates_and_explain() {
        let catalog = catalog_with_documents();
        let index = bind(
            parse("CREATE INDEX documents_title_idx ON documents (title) INCLUDE (id)")
                .expect("parse index"),
            &catalog,
        )
        .expect("bind index");
        assert!(matches!(index, BoundStatement::CreateIndex { .. }));

        let grouped = bind(
            parse(
                "SELECT d.id, COUNT(e.id) AS total \
                 FROM documents d LEFT JOIN documents e ON d.id = e.id \
                 GROUP BY d.id HAVING COUNT(e.id) > 0",
            )
            .expect("parse grouped join"),
            &catalog,
        )
        .expect("bind grouped join");
        let BoundStatement::AdvancedSelect {
            joins,
            aggregate,
            group_by,
            ..
        } = grouped
        else {
            panic!("advanced select");
        };
        assert_eq!(joins.len(), 1);
        assert!(aggregate);
        assert_eq!(group_by.len(), 1);

        let explain = bind(
            parse("EXPLAIN SELECT id FROM documents WHERE id = 1").expect("parse explain"),
            &catalog,
        )
        .expect("bind explain");
        assert!(matches!(explain, BoundStatement::Explain { .. }));
    }

    #[test]
    fn binds_full_text_and_hnsw_index_methods_with_bounded_options() {
        let catalog = catalog_with_documents();
        let full_text = bind(
            parse(
                "CREATE INDEX documents_fts ON documents USING fulltext (title) \
                 WITH (analyzer = 'whitespace')",
            )
            .expect("parse full-text index"),
            &catalog,
        )
        .expect("bind full-text index");
        let BoundStatement::CreateIndex { index, .. } = full_text else {
            panic!("full-text CREATE INDEX");
        };
        assert_eq!(index.method, IndexMethod::FullText);
        assert_eq!(
            index.options,
            IndexOptions::FullText {
                analyzer: FullTextAnalyzer::Whitespace
            }
        );

        let mut vector_catalog = Catalog::default();
        vector_catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("embeddings"),
                vec![NewColumn::new(
                    Identifier::unquoted("value"),
                    ScalarType::Vector {
                        dimensions: Some(3),
                    },
                )],
            )
            .expect("vector table");
        let hnsw = bind(
            parse(
                "CREATE INDEX embeddings_hnsw ON embeddings USING hnsw (value) \
                 WITH (metric = 'l2', m = 8, ef_construction = 32, ef_search = 24)",
            )
            .expect("parse HNSW index"),
            &vector_catalog,
        )
        .expect("bind HNSW index");
        let BoundStatement::CreateIndex { index, .. } = hnsw else {
            panic!("HNSW CREATE INDEX");
        };
        assert_eq!(index.method, IndexMethod::Hnsw);
        assert_eq!(
            index.options,
            IndexOptions::Hnsw {
                metric: VectorDistanceMetric::L2,
                dimensions: 3,
                m: 8,
                ef_construction: 32,
                ef_search: 24,
            }
        );

        let wrong_type = bind(
            parse("CREATE INDEX documents_hnsw ON documents USING hnsw (title)")
                .expect("parse wrong HNSW"),
            &catalog,
        )
        .expect_err("HNSW requires VECTOR");
        assert_eq!(wrong_type.sql_state, DATATYPE_MISMATCH);
        let unsupported_option = bind(
            parse(
                "CREATE INDEX documents_bad_fts ON documents USING fulltext (title) \
                 WITH (language = 'english')",
            )
            .expect("parse unsupported option"),
            &catalog,
        )
        .expect_err("unsupported option");
        assert_eq!(unsupported_option.sql_state, FEATURE_NOT_SUPPORTED);
    }

    #[test]
    fn parses_and_binds_postgres_ddl_defaults_constraints_sequences_and_views() {
        let catalog = catalog_with_documents();
        let create = bind(
            parse(
                "CREATE TABLE IF NOT EXISTS child_items (\
                    id BIGINT DEFAULT 1,\
                    document_id BIGINT,\
                    CONSTRAINT child_items_pkey PRIMARY KEY (id, document_id),\
                    CONSTRAINT child_items_document_fk FOREIGN KEY (document_id) \
                        REFERENCES documents(id) ON DELETE CASCADE ON UPDATE RESTRICT,\
                    CONSTRAINT child_items_id_check CHECK (id > 0)\
                )",
            )
            .expect("parse table ddl"),
            &catalog,
        )
        .expect("bind table ddl");
        let BoundStatement::CreateTable {
            columns,
            constraints,
            if_not_exists,
            ..
        } = create
        else {
            panic!("create table");
        };
        assert!(if_not_exists);
        assert_eq!(
            columns[0].default.as_ref().map(|value| value.sql.as_str()),
            Some("1")
        );
        assert_eq!(constraints.len(), 3);
        assert!(matches!(
            constraints[0].kind,
            NewConstraintKind::PrimaryKey { ref columns } if columns.len() == 2
        ));
        assert!(matches!(
            constraints[1].kind,
            NewConstraintKind::ForeignKey {
                on_delete: ReferentialAction::Cascade,
                on_update: ReferentialAction::Restrict,
                ..
            }
        ));

        let sequence = bind(
            parse(
                "CREATE SEQUENCE IF NOT EXISTS public.child_items_seq \
                 AS BIGINT INCREMENT BY 2 START WITH 5 NO CYCLE",
            )
            .expect("parse sequence"),
            &catalog,
        )
        .expect("bind sequence");
        assert!(matches!(
            sequence,
            BoundStatement::CreateSequence {
                sequence: NewSequence {
                    increment: 2,
                    start_value: Some(5),
                    cycle: false,
                    ..
                },
                if_not_exists: true,
                ..
            }
        ));

        let view = bind(
            parse("CREATE VIEW docs_view (doc_id, doc_title) AS SELECT id, title FROM documents")
                .expect("parse view"),
            &catalog,
        )
        .expect("bind view");
        let BoundStatement::CreateView {
            output, references, ..
        } = view
        else {
            panic!("create view");
        };
        assert_eq!(output.fields[0].name, "doc_id");
        assert_eq!(references.len(), 1);
    }

    #[test]
    fn parses_procedure_options_across_arbitrary_whitespace() {
        let procedure = parse(
            "CREATE PROCEDURE public.refresh_items(value BIGINT)
             LANGUAGE
             plpgsql
             AS $body$
             BEGIN
             RETURN;
             END;
             $body$",
        )
        .expect("parse procedure");
        assert!(matches!(
            procedure,
            ParsedStatement::CreateRoutine {
                kind: RoutineKind::Procedure,
                arguments,
                body,
                ..
            } if arguments.len() == 1 && body.contains("RETURN")
        ));
    }

    #[test]
    fn parses_and_binds_alter_drop_and_if_exists_forms() {
        let catalog = catalog_with_documents();
        let alter = bind(
            parse(
                "ALTER TABLE documents \
                 ADD COLUMN IF NOT EXISTS archived BOOLEAN DEFAULT FALSE, \
                 ALTER COLUMN title SET DEFAULT 'untitled'",
            )
            .expect("parse alter table"),
            &catalog,
        )
        .expect("bind alter table");
        assert!(matches!(
            alter,
            BoundStatement::AlterTable {
                operations,
                ..
            } if operations.len() == 2
        ));

        let missing = bind(
            parse("DROP TABLE IF EXISTS missing").expect("parse drop"),
            &catalog,
        )
        .expect("bind missing drop");
        assert!(matches!(missing, BoundStatement::NoOp { .. }));

        let drop_table = bind(
            parse("DROP TABLE documents CASCADE").expect("parse drop table"),
            &catalog,
        )
        .expect("bind drop table");
        assert!(matches!(
            drop_table,
            BoundStatement::DropObjects {
                kind: DdlObjectKind::Table,
                behavior: DropBehavior::Cascade,
                ..
            }
        ));
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

    #[test]
    fn defaults_to_postgresql_and_parses_dialect_names() {
        assert_eq!(
            parse("SELECT id FROM documents").expect("default parse"),
            parse_with_dialect("SELECT id FROM documents", SqlDialect::PostgreSql)
                .expect("explicit parse")
        );
        for (source, expected) in [
            ("postgres", SqlDialect::PostgreSql),
            ("mysql", SqlDialect::MySql),
            ("sqlite3", SqlDialect::Sqlite),
            ("sql-server", SqlDialect::SqlServer),
        ] {
            assert_eq!(source.parse::<SqlDialect>().expect("dialect"), expected);
        }
    }

    #[test]
    fn normalizes_question_mark_and_named_parameters_without_touching_literals() {
        let mysql = parse_with_dialect(
            "SELECT `id` FROM `documents` \
             WHERE `title` = '?' AND `id` >= ? AND `id` <> ? /* ? */ LIMIT 5",
            SqlDialect::MySql,
        )
        .expect("mysql");
        let ParsedStatement::Select {
            table,
            filter: Some(filter),
            limit: Some(limit),
            ..
        } = mysql
        else {
            panic!("mysql select");
        };
        assert_eq!(table.parts[0].name.as_str(), "documents");
        assert_eq!(parameter_indices(&filter), vec![1, 2]);
        assert!(matches!(
            limit.kind,
            ParsedExprKind::Literal(Value::Int32(5))
        ));

        let sql_server = parse_with_dialect(
            "SELECT TOP 7 [id] FROM [documents] WHERE [id] = @p1",
            SqlDialect::SqlServer,
        )
        .expect("sql server");
        let ParsedStatement::Select {
            filter: Some(filter),
            limit: Some(limit),
            ..
        } = sql_server
        else {
            panic!("sql server select");
        };
        assert_eq!(parameter_indices(&filter), vec![1], "{filter:?}");
        assert!(matches!(
            limit.kind,
            ParsedExprKind::Literal(Value::Int64(7))
        ));
    }

    #[test]
    fn accepts_verified_dialect_type_aliases_and_temporal_literals() {
        let mysql = parse_with_dialect(
            "CREATE TABLE dialect_types (\
                tiny TINYINT,\
                medium MEDIUMINT,\
                payload BLOB,\
                created DATETIME\
            )",
            SqlDialect::MySql,
        )
        .expect("mysql types");
        let ParsedStatement::CreateTable { columns, .. } = mysql else {
            panic!("create table");
        };
        assert_eq!(columns[0].data_type, ScalarType::Int16);
        assert_eq!(columns[1].data_type, ScalarType::Int32);
        assert_eq!(columns[2].data_type, ScalarType::Binary);
        assert_eq!(
            columns[3].data_type,
            ScalarType::Timestamp {
                with_timezone: false
            }
        );

        let sql_server = parse_with_dialect(
            "CREATE TABLE [dialect_types] (\
                [token] UNIQUEIDENTIFIER,\
                [title] NVARCHAR(32)\
            )",
            SqlDialect::SqlServer,
        )
        .expect("sql server types");
        let ParsedStatement::CreateTable { columns, .. } = sql_server else {
            panic!("create table");
        };
        assert_eq!(columns[0].data_type, ScalarType::Uuid);
        assert_eq!(
            columns[1].data_type,
            ScalarType::Varchar { length: Some(32) }
        );

        let temporal = parse_with_dialect(
            "INSERT INTO events (created_on, created_at) VALUES (\
                DATE '2026-07-25',\
                TIMESTAMP '2026-07-25 09:30:00.125'\
            )",
            SqlDialect::PostgreSql,
        )
        .expect("temporal literals");
        let ParsedStatement::Insert { rows, .. } = temporal else {
            panic!("insert");
        };
        assert!(matches!(
            rows[0][0].kind,
            ParsedExprKind::Literal(Value::Date(_))
        ));
        assert!(matches!(
            rows[0][1].kind,
            ParsedExprKind::Literal(Value::Timestamp(_))
        ));
    }

    #[test]
    fn normalizes_sqlite_types_quotes_parameters_and_zero_offset() {
        let statement = parse_with_dialect(
            "CREATE TABLE \"sqlite_types\" (\
                \"id\" INTEGER,\
                \"payload\" BLOB,\
                \"created\" DATETIME\
            )",
            SqlDialect::Sqlite,
        )
        .expect("sqlite types");
        let ParsedStatement::CreateTable { name, columns, .. } = statement else {
            panic!("create table");
        };
        assert!(name.parts[0].name.is_quoted());
        assert_eq!(columns[0].data_type, ScalarType::Int32);
        assert_eq!(columns[1].data_type, ScalarType::Binary);
        assert_eq!(
            columns[2].data_type,
            ScalarType::Timestamp {
                with_timezone: false
            }
        );

        let statement = parse_with_dialect(
            "SELECT \"id\" FROM \"sqlite_types\" WHERE \"id\" = ? LIMIT 5 OFFSET 0",
            SqlDialect::Sqlite,
        )
        .expect("sqlite select");
        let ParsedStatement::Select {
            table,
            filter: Some(filter),
            limit: Some(limit),
            ..
        } = statement
        else {
            panic!("sqlite select");
        };
        assert!(table.parts[0].name.is_quoted());
        assert_eq!(parameter_indices(&filter), vec![1]);
        assert!(matches!(
            limit.kind,
            ParsedExprKind::Literal(Value::Int32(5))
        ));
    }

    #[test]
    fn normalizes_verified_zero_offset_row_limit_forms() {
        for (dialect, sql, expected_limit) in [
            (
                SqlDialect::PostgreSql,
                "SELECT id FROM documents OFFSET 0 ROWS FETCH FIRST 4 ROWS ONLY",
                4,
            ),
            (SqlDialect::MySql, "SELECT id FROM documents LIMIT 0, 5", 5),
            (
                SqlDialect::Sqlite,
                "SELECT id FROM documents LIMIT 6 OFFSET 0",
                6,
            ),
            (
                SqlDialect::SqlServer,
                "SELECT [id] FROM [documents] ORDER BY [id] \
                 OFFSET 0 ROWS FETCH NEXT 7 ROWS ONLY",
                7,
            ),
        ] {
            let statement = parse_with_dialect(sql, dialect)
                .unwrap_or_else(|error| panic!("{dialect}: {error:?}"));
            let ParsedStatement::Select {
                limit: Some(limit), ..
            } = statement
            else {
                panic!("select with limit");
            };
            assert!(
                matches!(
                    limit.kind,
                    ParsedExprKind::Literal(Value::Int32(value))
                        if value == expected_limit
                ) || matches!(
                    limit.kind,
                    ParsedExprKind::Literal(Value::Int64(value))
                        if value == i64::from(expected_limit)
                ),
                "{dialect}: {limit:?}"
            );
        }
    }

    #[test]
    fn rejects_non_zero_offset_with_the_selected_dialect() {
        for (dialect, sql) in [
            (
                SqlDialect::PostgreSql,
                "SELECT id FROM documents LIMIT 5 OFFSET 1",
            ),
            (SqlDialect::MySql, "SELECT id FROM documents LIMIT 1, 5"),
            (
                SqlDialect::Sqlite,
                "SELECT id FROM documents LIMIT 5 OFFSET 1",
            ),
            (
                SqlDialect::SqlServer,
                "SELECT [id] FROM [documents] ORDER BY [id] \
                 OFFSET 1 ROWS FETCH NEXT 5 ROWS ONLY",
            ),
        ] {
            let error = parse_with_dialect(sql, dialect).expect_err("non-zero offset");
            assert_eq!(
                error.sql_state, FEATURE_NOT_SUPPORTED,
                "{dialect}: {error:?}"
            );
            if dialect != SqlDialect::PostgreSql {
                assert!(
                    error.message.contains(dialect.label()),
                    "{dialect}: {error:?}"
                );
            }
        }
    }

    #[test]
    fn reports_unsupported_vendor_features_with_the_selected_dialect() {
        let error = parse_with_dialect(
            "INSERT IGNORE INTO documents (id, title) VALUES (1, 'ignored')",
            SqlDialect::MySql,
        )
        .expect_err("insert ignore");
        assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED);
        assert!(error.message.contains("MySQL"), "{error:?}");
        assert!(error.hint.is_some());

        let error = parse_with_dialect(
            "SELECT TOP 10 PERCENT [id] FROM [documents]",
            SqlDialect::SqlServer,
        )
        .expect_err("top percent");
        assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED);
        assert!(error.message.contains("SQL Server"), "{error:?}");
    }

    fn parameter_indices(expression: &ParsedExpr) -> Vec<usize> {
        let mut parameters = Vec::new();
        let mut stack = vec![expression];
        while let Some(expression) = stack.pop() {
            match &expression.kind {
                ParsedExprKind::Parameter(index) => parameters.push(*index),
                ParsedExprKind::Unary { expr, .. } => stack.push(expr),
                ParsedExprKind::Binary { left, right, .. } => {
                    stack.push(right);
                    stack.push(left);
                }
                ParsedExprKind::Aggregate {
                    argument: Some(argument),
                    ..
                } => stack.push(argument),
                ParsedExprKind::Column(_)
                | ParsedExprKind::Literal(_)
                | ParsedExprKind::Aggregate { argument: None, .. } => {}
            }
        }
        parameters
    }
}
