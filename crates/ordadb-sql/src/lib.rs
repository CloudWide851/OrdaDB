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

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use ordadb_catalog::{
    Catalog, CatalogExpression, CatalogObjectRef, ConstraintKind, DomainConstraint, DropBehavior,
    EnumValuePosition, FullTextAnalyzer, IndexMethod, IndexOptions, NewColumn, NewConstraint,
    NewConstraintKind, NewIndex, NewSequence, ReferentialAction, RoutineArgument,
    RoutineArgumentMode, RoutineKind, TableDefinition, TriggerEvent as CatalogTriggerEvent,
    TriggerLevel, TriggerTarget, TriggerTiming, TypeDefinition, VectorDistanceMetric,
    ViewDefinition, ViewKind, indexable_type, text_search_type,
};
use ordadb_transaction::{IsolationLevel, TransactionAccessMode, TransactionCharacteristics};
use ordadb_types::{
    ArrayDimension, ColumnId, ConstraintId, DbError, Field, Identifier, IndexId, PgInterval,
    Result, RoutineId, ScalarType, Schema, SchemaId, SequenceId, TableId, TriggerId, TypeId, Value,
    ViewId,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    AlterColumnOperation as SqlAlterColumnOperation, AlterIndexOperation,
    AlterSchemaOperation as SqlAlterSchemaOperation, AlterTable,
    AlterTableOperation as SqlAlterTableOperation, AlterTypeAddValuePosition, AlterTypeOperation,
    ArgMode, Array as SqlArray, ArrayElemTypeDef, Assignment as SqlAssignment, AssignmentTarget,
    BeginTransactionKind, BinaryOperator as SqlBinaryOperator, CastKind, CharacterLength,
    ColumnDef, ColumnOption, ConflictTarget as SqlConflictTarget,
    CreateFunction as SqlCreateFunction, CreateFunctionBody, CreateTable, CreateTableOptions,
    CreateTrigger as SqlCreateTrigger, CreateView, DataType, DiscardObject,
    Distinct as SqlDistinct, DropBehavior as SqlDropBehavior, DuplicateTreatment, ExactNumberInfo,
    Expr as SqlExpr, FromTable, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    FunctionReturnType, FunctionSecurity, GroupByExpr, Ident, IndexType, JoinConstraint,
    JoinOperator, LimitClause, Merge as SqlMerge, MergeAction as SqlMergeAction,
    MergeClauseKind as SqlMergeClauseKind, MergeInsertKind as SqlMergeInsertKind, NamedWindowExpr,
    ObjectName, ObjectNamePart, ObjectType, OnConflictAction as SqlOnConflictAction,
    OnInsert as SqlOnInsert, OrderByKind, OutputClause as SqlOutputClause, Query,
    ReferentialAction as SqlReferentialAction, RenameTableNameKind, SchemaName, Select, SelectItem,
    SequenceOptions, SetExpr, SetOperator as SqlSetOperator, SetQuantifier as SqlSetQuantifier,
    Spanned, Statement as SqlStatement, TableAlias, TableConstraint, TableFactor, TableObject,
    TableWithJoins, TimezoneInfo, TopQuantity, TransactionAccessMode as SqlTransactionAccessMode,
    TransactionIsolationLevel as SqlTransactionIsolationLevel, TransactionMode,
    TriggerEvent as SqlTriggerEvent, TriggerExecBodyType, TriggerObject, TriggerObjectKind,
    TriggerPeriod, TrimWhereField, UnaryOperator as SqlUnaryOperator,
    UserDefinedTypeRepresentation, Value as SqlValue, WindowFrame as SqlWindowFrame,
    WindowFrameBound as SqlWindowFrameBound, WindowFrameUnits as SqlWindowFrameUnits,
    WindowSpec as SqlWindowSpec, WindowType, With as SqlWith,
};
use sqlparser::dialect::{
    Dialect, GenericDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect,
};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Span, Token, TokenWithSpan, Tokenizer, Whitespace};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryQuantifier {
    Any,
    All,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedExprKind {
    Column(ParsedObjectName),
    Literal(Value),
    Parameter(usize),
    ResolvedParameter {
        index: usize,
        data_type: ScalarType,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<ParsedExpr>,
    },
    Cast {
        expr: Box<ParsedExpr>,
        data_type: ScalarType,
        declared_type: Option<ParsedObjectName>,
    },
    Array {
        elements: Vec<ParsedExpr>,
        dimensions: Vec<ArrayDimension>,
    },
    Function {
        function: ScalarFunction,
        arguments: Vec<ParsedExpr>,
    },
    Binary {
        left: Box<ParsedExpr>,
        op: BinaryOperator,
        right: Box<ParsedExpr>,
    },
    InList {
        expr: Box<ParsedExpr>,
        list: Vec<ParsedExpr>,
        negated: bool,
    },
    ScalarSubquery(Box<ParsedStatement>),
    Exists {
        subquery: Box<ParsedStatement>,
        negated: bool,
    },
    InSubquery {
        expr: Box<ParsedExpr>,
        subquery: Box<ParsedStatement>,
        negated: bool,
    },
    QuantifiedSubquery {
        left: Box<ParsedExpr>,
        op: BinaryOperator,
        quantifier: SubqueryQuantifier,
        subquery: Box<ParsedStatement>,
    },
    RowSubquery {
        left: Vec<ParsedExpr>,
        op: BinaryOperator,
        quantifier: Option<SubqueryQuantifier>,
        negated: bool,
        subquery: Box<ParsedStatement>,
    },
    ApplyValue {
        index: usize,
        data_type: ScalarType,
        nullable: bool,
    },
    Aggregate {
        function: AggregateFunction,
        argument: Option<Box<ParsedExpr>>,
        distinct: bool,
        filter: Option<Box<ParsedExpr>>,
    },
    Window {
        call: Box<ParsedWindowCall>,
        spec: Box<ParsedWindowSpec>,
    },
    NamedWindow {
        call: Box<ParsedWindowCall>,
        name: ParsedIdentifier,
    },
    WindowValue {
        ordinal: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    Aggregate(AggregateFunction),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedWindowCall {
    pub function: WindowFunction,
    pub arguments: Vec<ParsedExpr>,
    pub count_star: bool,
    pub filter: Option<Box<ParsedExpr>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedWindowSpec {
    pub window_name: Option<ParsedIdentifier>,
    pub partition_by: Vec<ParsedExpr>,
    pub order_by: Vec<ParsedOrder>,
    pub frame: Option<ParsedWindowFrame>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedWindowFrameBound {
    UnboundedPreceding,
    Preceding(Box<ParsedExpr>),
    CurrentRow,
    Following(Box<ParsedExpr>),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedWindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: ParsedWindowFrameBound,
    pub end_bound: ParsedWindowFrameBound,
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
pub enum ScalarFunction {
    Version,
    CurrentDatabase,
    CurrentUser,
    SessionUser,
    CurrentSetting,
    Lower,
    Upper,
    CharacterLength,
    OctetLength,
    Abs,
    Coalesce,
    NullIf,
    Concat,
    Substring,
    Btrim,
    Ltrim,
    Rtrim,
    Replace,
    Strpos,
    Greatest,
    Least,
    JsonbTypeof,
    ArrayLength,
    Cardinality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionBindValues<'a> {
    pub version: &'a str,
    pub current_database: &'a str,
    pub current_user: &'a str,
    pub session_user: &'a str,
    pub settings: &'a BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Negate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuerySetOperator {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedCte {
    pub name: ParsedIdentifier,
    pub columns: Vec<ParsedIdentifier>,
    pub query: Box<ParsedStatement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedColumn {
    pub name: ParsedIdentifier,
    pub data_type: ScalarType,
    pub declared_type: Option<ParsedObjectName>,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<ParsedDefault>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedRoutineArgument {
    pub name: Option<Identifier>,
    pub data_type: ScalarType,
    pub declared_type: Option<ParsedObjectName>,
    pub mode: RoutineArgumentMode,
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
    Type,
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
        declared_type: Option<ParsedObjectName>,
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
pub enum ParsedAlterDomainOperation {
    SetDefault(ParsedDefault),
    DropDefault,
    SetNotNull,
    DropNotNull,
    AddConstraint(DomainConstraint),
    DropConstraint {
        name: ParsedIdentifier,
        if_exists: bool,
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
        declared_type: Option<TypeId>,
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

#[derive(Debug, Clone, PartialEq)]
pub enum BoundAlterDomainOperation {
    SetDefault(CatalogExpression),
    DropDefault,
    SetNotNull,
    DropNotNull,
    AddConstraint(DomainConstraint),
    DropConstraint { name: Identifier, if_exists: bool },
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
pub enum ParsedJoinSource {
    Table(ParsedTable),
    Derived {
        lateral: bool,
        query: Box<ParsedStatement>,
        alias: ParsedIdentifier,
        columns: Vec<ParsedIdentifier>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedJoin {
    pub source: ParsedJoinSource,
    pub kind: JoinKind,
    pub on: ParsedExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedOnConflict {
    pub target: Option<ParsedConflictTarget>,
    pub action: ParsedConflictAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedConflictTarget {
    Columns(Vec<ParsedIdentifier>),
    Constraint(ParsedObjectName),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedConflictAction {
    DoNothing,
    DoUpdate {
        assignments: Vec<(ParsedIdentifier, ParsedExpr)>,
        filter: Option<ParsedExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedMerge {
    pub target: ParsedTable,
    pub source: ParsedTable,
    pub on: ParsedExpr,
    pub clauses: Vec<ParsedMergeClause>,
    pub returning: Vec<ParsedProjection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedMergeClauseKind {
    Matched,
    NotMatchedByTarget,
    NotMatchedBySource,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedMergeClause {
    pub kind: ParsedMergeClauseKind,
    pub predicate: Option<ParsedExpr>,
    pub action: ParsedMergeAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedMergeAction {
    Update {
        assignments: Vec<(ParsedIdentifier, ParsedExpr)>,
    },
    Delete,
    Insert {
        columns: Vec<ParsedIdentifier>,
        values: Vec<ParsedExpr>,
    },
    DoNothing,
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
    Reindex {
        target: ParsedReindexTarget,
    },
    Listen {
        channel: ParsedIdentifier,
    },
    Unlisten {
        channel: Option<ParsedIdentifier>,
    },
    Notify {
        channel: ParsedIdentifier,
        payload: String,
    },
    Do {
        body: String,
    },
    DiscardAll,
    DeallocateAll,
    CreateSchema {
        name: ParsedIdentifier,
        if_not_exists: bool,
    },
    CreateEnumType {
        name: ParsedObjectName,
        labels: Vec<String>,
    },
    CreateDomain {
        name: ParsedObjectName,
        base_type: ScalarType,
        base_declared_type: Option<ParsedObjectName>,
        not_null: bool,
        default: Option<ParsedDefault>,
        checks: Vec<DomainConstraint>,
    },
    AlterEnumAddValue {
        name: ParsedObjectName,
        label: String,
        position: Option<EnumValuePosition>,
        if_not_exists: bool,
    },
    AlterEnumRenameValue {
        name: ParsedObjectName,
        old_label: String,
        new_label: String,
    },
    AlterDomain {
        name: ParsedObjectName,
        operation: ParsedAlterDomainOperation,
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
        arguments: Vec<ParsedRoutineArgument>,
        return_type: Option<ScalarType>,
        return_declared_type: Option<ParsedObjectName>,
        returns_set: bool,
        language: String,
        body: String,
        replace: bool,
    },
    DropRoutine {
        name: ParsedObjectName,
        kind: RoutineKind,
        argument_types: Option<Vec<ParsedRoutineArgument>>,
        if_exists: bool,
        behavior: DropBehavior,
    },
    Call {
        name: ParsedObjectName,
        arguments: Vec<ParsedExpr>,
    },
    ScalarSelect {
        projection: Vec<ParsedProjection>,
    },
    RoutineSelect {
        name: ParsedObjectName,
        arguments: Vec<ParsedExpr>,
        alias: Option<ParsedIdentifier>,
    },
    PgNotify {
        channel: ParsedExpr,
        payload: ParsedExpr,
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
        level: TriggerLevel,
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
        on_conflict: Option<ParsedOnConflict>,
        returning: Vec<ParsedProjection>,
    },
    Merge(ParsedMerge),
    With {
        recursive: bool,
        ctes: Vec<ParsedCte>,
        body: Box<ParsedStatement>,
    },
    SetOperation {
        left: Box<ParsedStatement>,
        operator: QuerySetOperator,
        all: bool,
        right: Box<ParsedStatement>,
        order_by: Vec<ParsedOrder>,
        offset: Option<ParsedExpr>,
        limit: Option<ParsedExpr>,
    },
    Select {
        table: ParsedObjectName,
        projection: Vec<ParsedProjection>,
        filter: Option<ParsedExpr>,
        order_by: Vec<ParsedOrder>,
        offset: Option<ParsedExpr>,
        limit: Option<ParsedExpr>,
    },
    AdvancedSelect {
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
    },
    Explain {
        statement: Box<ParsedStatement>,
    },
    Update {
        table: ParsedObjectName,
        assignments: Vec<(ParsedIdentifier, ParsedExpr)>,
        filter: Option<ParsedExpr>,
        returning: Vec<ParsedProjection>,
    },
    Delete {
        table: ParsedObjectName,
        filter: Option<ParsedExpr>,
        returning: Vec<ParsedProjection>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedReindexTarget {
    Index(ParsedObjectName),
    Table(ParsedObjectName),
    Schema(ParsedIdentifier),
    Database(ParsedIdentifier),
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
    Correlation {
        depth: usize,
        index: usize,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<BoundExpr>,
    },
    Cast {
        expr: Box<BoundExpr>,
    },
    Array {
        elements: Vec<BoundExpr>,
        dimensions: Vec<ArrayDimension>,
    },
    Function {
        function: ScalarFunction,
        arguments: Vec<BoundExpr>,
    },
    Binary {
        left: Box<BoundExpr>,
        op: BinaryOperator,
        right: Box<BoundExpr>,
    },
    InList {
        expr: Box<BoundExpr>,
        list: Vec<BoundExpr>,
        negated: bool,
    },
    ApplyValue {
        index: usize,
    },
    Aggregate {
        function: AggregateFunction,
        argument: Option<Box<BoundExpr>>,
        distinct: bool,
        filter: Option<Box<BoundExpr>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundApplyKind {
    Scalar,
    Exists {
        negated: bool,
    },
    In {
        left: BoundExpr,
        negated: bool,
    },
    Quantified {
        left: BoundExpr,
        op: BinaryOperator,
        quantifier: SubqueryQuantifier,
    },
    RowScalar {
        left: Vec<BoundExpr>,
        op: BinaryOperator,
        operand_types: Vec<ScalarType>,
    },
    RowQuantified {
        left: Vec<BoundExpr>,
        op: BinaryOperator,
        quantifier: SubqueryQuantifier,
        negated: bool,
        operand_types: Vec<ScalarType>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundApply {
    pub kind: BoundApplyKind,
    pub query: Box<BoundStatement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundProjection {
    pub expr: BoundExpr,
    pub field: Field,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundWindow {
    pub function: WindowFunction,
    pub value_index: usize,
    pub arguments: Vec<BoundExpr>,
    pub count_star: bool,
    pub filter: Option<BoundExpr>,
    pub partition_by: Vec<BoundExpr>,
    pub order_by: Vec<BoundOrder>,
    pub frame: Option<BoundWindowFrame>,
    pub data_type: ScalarType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundWindowFrameBound {
    UnboundedPreceding,
    Preceding(BoundExpr),
    CurrentRow,
    Following(BoundExpr),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundWindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: BoundWindowFrameBound,
    pub end_bound: BoundWindowFrameBound,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundReturning {
    pub schema: Schema,
    pub projection: Vec<BoundProjection>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundOnConflict {
    pub target_columns: Option<Vec<usize>>,
    pub action: BoundConflictAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundConflictAction {
    DoNothing,
    DoUpdate {
        assignments: Vec<(usize, BoundExpr)>,
        filter: Option<BoundExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundMerge {
    pub target: BoundTable,
    pub source: BoundTable,
    pub on: BoundExpr,
    pub clauses: Vec<BoundMergeClause>,
    pub returning: Option<BoundReturning>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundMergeClauseKind {
    Matched,
    NotMatchedByTarget,
    NotMatchedBySource,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundMergeClause {
    pub kind: BoundMergeClauseKind,
    pub predicate: Option<BoundExpr>,
    pub action: BoundMergeAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundMergeAction {
    Update {
        assignments: Vec<(usize, BoundExpr)>,
    },
    Delete,
    Insert {
        column_indexes: Vec<usize>,
        values: Vec<BoundExpr>,
    },
    DoNothing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundOrder {
    pub column_index: usize,
    pub expression: Option<BoundExpr>,
    pub data_type: ScalarType,
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
pub enum BoundJoinSource {
    Table(BoundTable),
    Derived {
        lateral: bool,
        query: Box<BoundStatement>,
        binding: Identifier,
        offset: usize,
        width: usize,
        nullable: bool,
    },
}

impl BoundJoinSource {
    #[must_use]
    pub const fn offset(&self) -> usize {
        match self {
            Self::Table(table) => table.offset,
            Self::Derived { offset, .. } => *offset,
        }
    }

    #[must_use]
    pub const fn width(&self) -> usize {
        match self {
            Self::Table(table) => table.width,
            Self::Derived { width, .. } => *width,
        }
    }

    #[must_use]
    pub fn binding(&self) -> &Identifier {
        match self {
            Self::Table(table) => &table.binding,
            Self::Derived { binding, .. } => binding,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundJoin {
    pub source: BoundJoinSource,
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
    Reindex {
        target: BoundReindexTarget,
    },
    Listen {
        channel: Identifier,
    },
    Unlisten {
        channel: Option<Identifier>,
    },
    Notify {
        channel: Identifier,
        payload: String,
    },
    Do {
        body: String,
    },
    DiscardAll,
    DeallocateAll,
    CreateSchema {
        name: Identifier,
        if_not_exists: bool,
    },
    CreateEnumType {
        schema: Identifier,
        name: Identifier,
        labels: Vec<String>,
    },
    CreateDomain {
        schema: Identifier,
        name: Identifier,
        base_type: ScalarType,
        base_declared_type: Option<TypeId>,
        not_null: bool,
        default: Option<CatalogExpression>,
        checks: Vec<DomainConstraint>,
    },
    AlterEnumAddValue {
        type_id: TypeId,
        label: String,
        position: Option<EnumValuePosition>,
        if_not_exists: bool,
    },
    AlterEnumRenameValue {
        type_id: TypeId,
        old_label: String,
        new_label: String,
    },
    AlterDomain {
        type_id: TypeId,
        operation: BoundAlterDomainOperation,
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
        return_declared_type: Option<TypeId>,
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
        schema: Schema,
    },
    ScalarSelect {
        projection: Vec<BoundProjection>,
        schema: Schema,
    },
    RoutineSelect {
        routine_id: RoutineId,
        arguments: Vec<BoundExpr>,
        schema: Schema,
        returns_set: bool,
    },
    PgNotify {
        channel: BoundExpr,
        payload: BoundExpr,
        schema: Schema,
    },
    SequenceValue {
        sequence_id: SequenceId,
        operation: BoundSequenceOperation,
        schema: Schema,
    },
    CreateTrigger {
        target: TriggerTarget,
        name: Identifier,
        timing: TriggerTiming,
        level: TriggerLevel,
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
        on_conflict: Option<BoundOnConflict>,
        returning: Option<BoundReturning>,
    },
    ViewInsert {
        view_id: ViewId,
        source: Box<BoundStatement>,
        column_indexes: Vec<usize>,
        rows: Vec<Vec<BoundExpr>>,
        returning: Option<BoundReturning>,
    },
    Merge(BoundMerge),
    With {
        ctes: Vec<BoundCte>,
        body: Box<BoundStatement>,
        catalog: Box<Catalog>,
        schema: Schema,
    },
    SetOperation {
        left: Box<BoundStatement>,
        operator: QuerySetOperator,
        all: bool,
        right: Box<BoundStatement>,
        schema: Schema,
        order_by: Vec<BoundOrder>,
        offset: Option<BoundExpr>,
        limit: Option<BoundExpr>,
    },
    Select {
        table_id: TableId,
        schema: Schema,
        projection: Vec<BoundProjection>,
        filter: Option<BoundExpr>,
        order_by: Vec<BoundOrder>,
        offset: Option<BoundExpr>,
        limit: Option<BoundExpr>,
    },
    AdvancedSelect {
        table: BoundTable,
        joins: Vec<BoundJoin>,
        applies: Vec<BoundApply>,
        windows: Vec<BoundWindow>,
        schema: Schema,
        projection: Vec<BoundProjection>,
        distinct: bool,
        filter: Option<BoundExpr>,
        group_by: Vec<BoundExpr>,
        having: Option<BoundExpr>,
        order_by: Vec<BoundOrder>,
        offset: Option<BoundExpr>,
        limit: Option<Box<BoundExpr>>,
        aggregate: bool,
    },
    Explain {
        statement: Box<BoundStatement>,
    },
    Update {
        table_id: TableId,
        assignments: Vec<(usize, BoundExpr)>,
        filter: Option<BoundExpr>,
        returning: Option<BoundReturning>,
    },
    ViewUpdate {
        view_id: ViewId,
        source: Box<BoundStatement>,
        assignments: Vec<(usize, BoundExpr)>,
        filter: Option<BoundExpr>,
        returning: Option<BoundReturning>,
    },
    Delete {
        table_id: TableId,
        filter: Option<BoundExpr>,
        returning: Option<BoundReturning>,
    },
    ViewDelete {
        view_id: ViewId,
        source: Box<BoundStatement>,
        filter: Option<BoundExpr>,
        returning: Option<BoundReturning>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundReindexTarget {
    Index(IndexId),
    Table(TableId),
    Schema(SchemaId),
    Database,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundCte {
    pub table_id: TableId,
    pub seed: Box<BoundStatement>,
    pub recursive: Option<Box<BoundStatement>>,
    pub union_all: bool,
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
        if let Some(statement) = parse_postgres_session_or_maintenance(sql)? {
            return Ok(statement);
        }
        if let Some(statement) = parse_vacuum_analyze(sql)? {
            return Ok(statement);
        }
        if let Some(statement) = parse_transaction_begin(sql)? {
            return Ok(statement);
        }
        if let Some(statement) = parse_create_procedure(sql)? {
            return Ok(statement);
        }
        if let Some(statement) = parse_alter_domain(sql)? {
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
            if let Some(tokens) = rewrite_postgres_merge_do_nothing(sql)? {
                return Parser::new(&GenericDialect {})
                    .with_tokens_with_locations(tokens)
                    .parse_statements();
            }
            if let Some(tokens) = rewrite_postgres_create_domain_not_null(sql)? {
                return Parser::new(&PostgreSqlDialect {})
                    .with_tokens_with_locations(tokens)
                    .parse_statements();
            }
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

fn rewrite_postgres_create_domain_not_null(
    sql: &str,
) -> std::result::Result<Option<Vec<TokenWithSpan>>, ParserError> {
    let dialect = PostgreSqlDialect {};
    let mut tokens = Tokenizer::new(&dialect, sql).tokenize_with_location()?;
    let significant_indices = tokens
        .iter()
        .enumerate()
        .filter_map(|(index, token)| {
            (!matches!(token.token, Token::Whitespace(_))).then_some(index)
        })
        .collect::<Vec<_>>();
    let significant = significant_indices
        .iter()
        .map(|index| tokens[*index].token.clone())
        .collect::<Vec<_>>();
    let Some((not_index, null_index)) = create_domain_not_null_tokens(&significant) else {
        return Ok(None);
    };
    tokens[significant_indices[not_index]].token = Token::Whitespace(Whitespace::Space);
    tokens[significant_indices[null_index]].token = Token::Whitespace(Whitespace::Space);
    Ok(Some(tokens))
}

fn create_domain_not_null_tokens(tokens: &[Token]) -> Option<(usize, usize)> {
    if !tokens
        .first()
        .is_some_and(|token| is_unquoted_word(token, "CREATE"))
        || !tokens
            .get(1)
            .is_some_and(|token| is_unquoted_word(token, "DOMAIN"))
    {
        return None;
    }
    let mut parenthesis_depth = 0_usize;
    for index in 2..tokens.len().saturating_sub(1) {
        match &tokens[index] {
            Token::LParen => parenthesis_depth = parenthesis_depth.saturating_add(1),
            Token::RParen => parenthesis_depth = parenthesis_depth.saturating_sub(1),
            token
                if parenthesis_depth == 0
                    && is_unquoted_word(token, "NOT")
                    && !tokens
                        .get(index.wrapping_sub(1))
                        .is_some_and(|token| is_unquoted_word(token, "IS"))
                    && is_unquoted_word(&tokens[index + 1], "NULL") =>
            {
                return Some((index, index + 1));
            }
            _ => {}
        }
    }
    None
}

fn create_domain_is_not_null(sql: &str) -> bool {
    create_domain_not_null_tokens(&significant_tokens(sql)).is_some()
}

#[derive(Debug, Clone, Copy)]
struct MergeClauseTokenInfo {
    not_matched_by_target: bool,
    do_nothing: Option<(usize, usize)>,
}

fn rewrite_postgres_merge_do_nothing(
    sql: &str,
) -> std::result::Result<Option<Vec<TokenWithSpan>>, ParserError> {
    let dialect = PostgreSqlDialect {};
    let mut tokens = Tokenizer::new(&dialect, sql).tokenize_with_location()?;
    let significant_indices = tokens
        .iter()
        .enumerate()
        .filter_map(|(index, token)| {
            (!matches!(token.token, Token::Whitespace(_))).then_some(index)
        })
        .collect::<Vec<_>>();
    let significant = significant_indices
        .iter()
        .map(|index| tokens[*index].token.clone())
        .collect::<Vec<_>>();
    let Some(clauses) = merge_clause_token_info(&significant) else {
        return Ok(None);
    };
    let replacements = clauses
        .into_iter()
        .filter_map(|clause| {
            clause.do_nothing.map(|(do_index, nothing_index)| {
                (clause.not_matched_by_target, do_index, nothing_index)
            })
        })
        .collect::<Vec<_>>();
    if replacements.is_empty() {
        return Ok(None);
    }
    for (not_matched_by_target, do_index, nothing_index) in replacements {
        let do_index = significant_indices[do_index];
        let nothing_index = significant_indices[nothing_index];
        if not_matched_by_target {
            tokens[do_index].token = Token::make_keyword("INSERT");
            tokens[nothing_index].token = Token::make_keyword("ROW");
        } else {
            tokens[do_index].token = Token::make_keyword("DELETE");
            tokens[nothing_index].token = Token::Whitespace(Whitespace::Space);
        }
    }
    Ok(Some(tokens))
}

fn merge_clause_token_info(tokens: &[Token]) -> Option<Vec<MergeClauseTokenInfo>> {
    if !tokens
        .first()
        .is_some_and(|token| is_unquoted_word(token, "MERGE"))
    {
        return None;
    }
    let mut starts = Vec::new();
    let mut parenthesis_depth = 0_usize;
    let mut case_depth = 0_usize;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Token::LParen => parenthesis_depth = parenthesis_depth.saturating_add(1),
            Token::RParen => parenthesis_depth = parenthesis_depth.saturating_sub(1),
            _ if parenthesis_depth == 0 && is_unquoted_word(token, "CASE") => {
                case_depth = case_depth.saturating_add(1);
            }
            _ if parenthesis_depth == 0 && is_unquoted_word(token, "END") && case_depth > 0 => {
                case_depth -= 1;
            }
            _ if parenthesis_depth == 0
                && case_depth == 0
                && is_merge_clause_start(tokens, index) =>
            {
                starts.push(index);
            }
            _ => {}
        }
    }
    let mut clauses = Vec::with_capacity(starts.len());
    for (ordinal, start) in starts.iter().copied().enumerate() {
        let end = starts.get(ordinal + 1).copied().unwrap_or(tokens.len());
        let not_matched = tokens
            .get(start.saturating_add(1))
            .is_some_and(|token| is_unquoted_word(token, "NOT"));
        let by_source = tokens
            .get(start.saturating_add(3))
            .is_some_and(|token| is_unquoted_word(token, "BY"))
            && tokens
                .get(start.saturating_add(4))
                .is_some_and(|token| is_unquoted_word(token, "SOURCE"));
        let do_nothing = find_merge_do_nothing(tokens, start, end);
        clauses.push(MergeClauseTokenInfo {
            not_matched_by_target: not_matched && !by_source,
            do_nothing,
        });
    }
    Some(clauses)
}

fn is_merge_clause_start(tokens: &[Token], index: usize) -> bool {
    is_unquoted_word(&tokens[index], "WHEN")
        && (tokens
            .get(index.saturating_add(1))
            .is_some_and(|token| is_unquoted_word(token, "MATCHED"))
            || (tokens
                .get(index.saturating_add(1))
                .is_some_and(|token| is_unquoted_word(token, "NOT"))
                && tokens
                    .get(index.saturating_add(2))
                    .is_some_and(|token| is_unquoted_word(token, "MATCHED"))))
}

fn find_merge_do_nothing(tokens: &[Token], start: usize, end: usize) -> Option<(usize, usize)> {
    let mut parenthesis_depth = 0_usize;
    let mut case_depth = 0_usize;
    for index in start..end.saturating_sub(2) {
        let token = &tokens[index];
        match token {
            Token::LParen => parenthesis_depth = parenthesis_depth.saturating_add(1),
            Token::RParen => parenthesis_depth = parenthesis_depth.saturating_sub(1),
            _ if parenthesis_depth == 0 && is_unquoted_word(token, "CASE") => {
                case_depth = case_depth.saturating_add(1);
            }
            _ if parenthesis_depth == 0 && is_unquoted_word(token, "END") && case_depth > 0 => {
                case_depth -= 1;
            }
            _ if parenthesis_depth == 0
                && case_depth == 0
                && is_unquoted_word(token, "THEN")
                && is_unquoted_word(&tokens[index + 1], "DO")
                && is_unquoted_word(&tokens[index + 2], "NOTHING") =>
            {
                return Some((index + 1, index + 2));
            }
            _ => {}
        }
    }
    None
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
        return_declared_type: None,
        returns_set: false,
        language: "plpgsql".to_owned(),
        body,
        replace,
    }))
}

fn parse_postgres_session_or_maintenance(sql: &str) -> Result<Option<ParsedStatement>> {
    const MAX_NOTIFICATION_PAYLOAD_BYTES: usize = 7_999;
    const MAX_DO_BODY_BYTES: usize = 1024 * 1024;

    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let tokens = significant_tokens(trimmed);
    let Some(first) = tokens.first() else {
        return Ok(None);
    };

    if is_unquoted_word(first, "REINDEX") {
        if tokens.iter().any(|token| matches!(token, Token::LParen)) {
            return unsupported("REINDEX parameter clauses are not supported");
        }
        if tokens
            .iter()
            .any(|token| is_unquoted_word(token, "CONCURRENTLY"))
        {
            return unsupported("REINDEX CONCURRENTLY is not supported");
        }
        let mut cursor = 1;
        if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "SYSTEM"))
        {
            return unsupported("REINDEX SYSTEM is not supported");
        }
        let target = if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "INDEX"))
        {
            cursor += 1;
            ParsedReindexTarget::Index(parse_token_object_name(&tokens, &mut cursor, 2)?)
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "TABLE"))
        {
            cursor += 1;
            ParsedReindexTarget::Table(parse_token_object_name(&tokens, &mut cursor, 2)?)
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "SCHEMA"))
        {
            cursor += 1;
            ParsedReindexTarget::Schema(parse_token_identifier(&tokens, &mut cursor)?)
        } else if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "DATABASE"))
        {
            cursor += 1;
            ParsedReindexTarget::Database(parse_token_identifier(&tokens, &mut cursor)?)
        } else {
            return unsupported("REINDEX requires INDEX, TABLE, SCHEMA, or DATABASE");
        };
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::Reindex { target }));
    }

    if is_unquoted_word(first, "LISTEN") {
        let mut cursor = 1;
        let channel = parse_token_identifier(&tokens, &mut cursor)?;
        validate_notification_channel(&channel.name)?;
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::Listen { channel }));
    }

    if is_unquoted_word(first, "UNLISTEN") {
        let mut cursor = 1;
        let channel = if tokens.get(cursor) == Some(&Token::Mul) {
            cursor += 1;
            None
        } else {
            let channel = parse_token_identifier(&tokens, &mut cursor)?;
            validate_notification_channel(&channel.name)?;
            Some(channel)
        };
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::Unlisten { channel }));
    }

    if is_unquoted_word(first, "NOTIFY") {
        let mut cursor = 1;
        let channel = parse_token_identifier(&tokens, &mut cursor)?;
        validate_notification_channel(&channel.name)?;
        let payload = if tokens.get(cursor) == Some(&Token::Comma) {
            cursor += 1;
            let Token::SingleQuotedString(payload) = tokens
                .get(cursor)
                .ok_or_else(|| DbError::new(SYNTAX_ERROR, "NOTIFY payload expected"))?
            else {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "NOTIFY payload must be a string literal",
                ));
            };
            cursor += 1;
            payload.clone()
        } else {
            String::new()
        };
        if payload.contains('\0') {
            return Err(DbError::new("22021", "NOTIFY payload cannot contain NUL"));
        }
        if payload.len() > MAX_NOTIFICATION_PAYLOAD_BYTES {
            return Err(DbError::new("22023", "NOTIFY payload is too long"));
        }
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::Notify { channel, payload }));
    }

    if is_unquoted_word(first, "DO") {
        let mut cursor = 1;
        if tokens
            .get(cursor)
            .is_some_and(|token| is_unquoted_word(token, "LANGUAGE"))
        {
            cursor += 1;
            let language = parse_token_identifier(&tokens, &mut cursor)?;
            if !language.name.as_str().eq_ignore_ascii_case("plpgsql") {
                return unsupported("only LANGUAGE plpgsql DO blocks are supported");
            }
        }
        let body = match tokens.get(cursor) {
            Some(Token::DollarQuotedString(body)) => body.value.clone(),
            Some(Token::SingleQuotedString(body)) => body.clone(),
            _ => return Err(DbError::new(SYNTAX_ERROR, "DO requires a quoted body")),
        };
        cursor += 1;
        if body.len() > MAX_DO_BODY_BYTES {
            return Err(DbError::new("54000", "DO body exceeds the source limit"));
        }
        ensure_token_end(&tokens, cursor)?;
        return Ok(Some(ParsedStatement::Do { body }));
    }

    Ok(None)
}

fn validate_notification_channel(channel: &Identifier) -> Result<()> {
    if channel.as_str().is_empty() || channel.as_str().len() > ordadb_types::MAX_POSTGRES_NAME_BYTES
    {
        return Err(DbError::new(
            "42622",
            "notification channel name is empty or too long",
        ));
    }
    if channel.as_str().contains('\0') {
        return Err(DbError::new(
            "22021",
            "notification channel cannot contain NUL",
        ));
    }
    Ok(())
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

fn parse_alter_domain(sql: &str) -> Result<Option<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let tokens = significant_tokens(trimmed);
    if tokens.len() < 3
        || !tokens
            .first()
            .is_some_and(|token| is_unquoted_word(token, "ALTER"))
        || !tokens
            .get(1)
            .is_some_and(|token| is_unquoted_word(token, "DOMAIN"))
    {
        return Ok(None);
    }
    let mut cursor = 2usize;
    let name = parse_token_object_name(&tokens, &mut cursor, 2)?;
    let operation = if consume_keyword_sequence(&tokens, &mut cursor, &["SET", "DEFAULT"]) {
        if cursor == tokens.len() {
            return Err(DbError::new(
                SYNTAX_ERROR,
                "ALTER DOMAIN SET DEFAULT requires an expression",
            ));
        }
        let default = parsed_default_from_tokens(&tokens[cursor..])?;
        cursor = tokens.len();
        ParsedAlterDomainOperation::SetDefault(default)
    } else if consume_keyword_sequence(&tokens, &mut cursor, &["DROP", "DEFAULT"]) {
        ParsedAlterDomainOperation::DropDefault
    } else if consume_keyword_sequence(&tokens, &mut cursor, &["SET", "NOT", "NULL"]) {
        ParsedAlterDomainOperation::SetNotNull
    } else if consume_keyword_sequence(&tokens, &mut cursor, &["DROP", "NOT", "NULL"]) {
        ParsedAlterDomainOperation::DropNotNull
    } else if consume_keyword(&tokens, &mut cursor, "ADD") {
        let constraint = parse_domain_constraint_tokens(&tokens, &mut cursor)?;
        if consume_keyword_sequence(&tokens, &mut cursor, &["NOT", "VALID"]) {
            return unsupported("ALTER DOMAIN ADD CONSTRAINT NOT VALID is not supported yet");
        }
        ParsedAlterDomainOperation::AddConstraint(constraint)
    } else if consume_keyword_sequence(&tokens, &mut cursor, &["DROP", "CONSTRAINT"]) {
        let if_exists = consume_keyword_sequence(&tokens, &mut cursor, &["IF", "EXISTS"]);
        let name = parse_token_identifier(&tokens, &mut cursor)?;
        if consume_keyword(&tokens, &mut cursor, "CASCADE") {
            return unsupported("ALTER DOMAIN DROP CONSTRAINT CASCADE is not supported yet");
        }
        let _ = consume_keyword(&tokens, &mut cursor, "RESTRICT");
        ParsedAlterDomainOperation::DropConstraint { name, if_exists }
    } else {
        return unsupported("this ALTER DOMAIN operation is not supported yet");
    };
    ensure_token_end(&tokens, cursor)?;
    Ok(Some(ParsedStatement::AlterDomain { name, operation }))
}

fn parse_domain_constraint_tokens(
    tokens: &[Token],
    cursor: &mut usize,
) -> Result<DomainConstraint> {
    let name = if consume_keyword(tokens, cursor, "CONSTRAINT") {
        Some(parse_token_identifier(tokens, cursor)?.name)
    } else {
        None
    };
    expect_keyword(tokens, cursor, "CHECK")?;
    if tokens.get(*cursor) != Some(&Token::LParen) {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "domain CHECK constraint requires parentheses",
        ));
    }
    *cursor += 1;
    let expression_start = *cursor;
    let mut depth = 1usize;
    while *cursor < tokens.len() {
        match tokens[*cursor] {
            Token::LParen => depth = depth.saturating_add(1),
            Token::RParen => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
        *cursor += 1;
    }
    if depth != 0 {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "unterminated domain CHECK constraint",
        ));
    }
    if expression_start == *cursor {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "domain CHECK constraint requires an expression",
        ));
    }
    let expression = tokens_sql(&tokens[expression_start..*cursor]);
    parse_parsed_expression(&expression)?;
    *cursor += 1;
    Ok(DomainConstraint {
        id: None,
        name,
        expression: CatalogExpression::new(expression),
    })
}

fn parsed_default_from_tokens(tokens: &[Token]) -> Result<ParsedDefault> {
    let sql = tokens_sql(tokens);
    Ok(ParsedDefault {
        expression: parse_parsed_expression(&sql)?,
        sql,
    })
}

fn parse_parsed_expression(sql: &str) -> Result<ParsedExpr> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|error| DbError::new(SYNTAX_ERROR, error.to_string()))?;
    let expression = parser
        .parse_expr()
        .map_err(|error| DbError::new(SYNTAX_ERROR, error.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "domain expression contains trailing SQL",
        ));
    }
    convert_expr(expression, sql)
}

fn tokens_sql(tokens: &[Token]) -> String {
    tokens
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ")
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

fn parse_procedure_arguments(value: &str) -> Result<Vec<ParsedRoutineArgument>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|argument| {
            let mut parts = argument.split_whitespace().collect::<Vec<_>>();
            if parts
                .iter()
                .any(|part| part.eq_ignore_ascii_case("DEFAULT") || part.contains('='))
            {
                return unsupported("defaulted procedure arguments are not supported yet");
            }
            let mode = parts
                .first()
                .and_then(|part| parse_routine_argument_mode(part))
                .unwrap_or_default();
            if parse_routine_argument_mode(parts.first().copied().unwrap_or_default()).is_some() {
                parts.remove(0);
            }
            let (name, data_type) = match parts.as_slice() {
                [data_type] => (None, *data_type),
                [first, second]
                    if first.eq_ignore_ascii_case("DOUBLE")
                        && second.eq_ignore_ascii_case("PRECISION") =>
                {
                    (None, "DOUBLE PRECISION")
                }
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
            let (data_type, declared_type) = parse_procedure_data_type(data_type)?;
            Ok(ParsedRoutineArgument {
                name,
                data_type,
                declared_type,
                mode,
            })
        })
        .collect()
}

fn parse_routine_argument_mode(value: &str) -> Option<RoutineArgumentMode> {
    if value.eq_ignore_ascii_case("IN") {
        Some(RoutineArgumentMode::In)
    } else if value.eq_ignore_ascii_case("OUT") {
        Some(RoutineArgumentMode::Out)
    } else if value.eq_ignore_ascii_case("INOUT") {
        Some(RoutineArgumentMode::InOut)
    } else if value.eq_ignore_ascii_case("VARIADIC") {
        Some(RoutineArgumentMode::Variadic)
    } else {
        None
    }
}

fn parse_procedure_data_type(value: &str) -> Result<(ScalarType, Option<ParsedObjectName>)> {
    let (value, is_array) = value
        .strip_suffix("[]")
        .map_or((value, false), |value| (value, true));
    let built_in = match value.to_ascii_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => Some(ScalarType::Boolean),
        "SMALLINT" | "INT2" => Some(ScalarType::Int16),
        "INT" | "INTEGER" | "INT4" => Some(ScalarType::Int32),
        "BIGINT" | "INT8" => Some(ScalarType::Int64),
        "REAL" | "FLOAT4" => Some(ScalarType::Float32),
        "DOUBLE PRECISION" | "FLOAT8" => Some(ScalarType::Float64),
        "TEXT" => Some(ScalarType::Text),
        "UUID" => Some(ScalarType::Uuid),
        "JSON" => Some(ScalarType::Json),
        "JSONB" => Some(ScalarType::Jsonb),
        _ => None,
    };
    let (data_type, declared_type) = if let Some(data_type) = built_in {
        (data_type, None)
    } else {
        let parts = value
            .split('.')
            .map(|part| {
                let (name, quoted) =
                    if part.starts_with('"') && part.ends_with('"') && part.len() >= 2 {
                        (&part[1..part.len() - 1], true)
                    } else {
                        (part, false)
                    };
                let valid = !name.is_empty()
                    && (quoted
                        || (name
                            .bytes()
                            .next()
                            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
                            && name.bytes().all(|byte| {
                                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
                            })));
                if !valid {
                    return Err(DbError::new(
                        SYNTAX_ERROR,
                        format!("invalid procedure argument type name {value}"),
                    ));
                }
                Ok(ParsedIdentifier {
                    name: Identifier::new(name, quoted),
                    position: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if !(1..=2).contains(&parts.len()) {
            return unsupported("procedure argument type names support at most one schema");
        }
        (ScalarType::Text, Some(ParsedObjectName { parts }))
    };
    Ok((
        if is_array {
            ScalarType::Array {
                element: Box::new(data_type),
            }
        } else {
            data_type
        },
        declared_type,
    ))
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
    bind_catalog_expression_with_parameter_types_and_catalog(
        expression,
        table,
        expected,
        &BTreeMap::new(),
        None,
    )
}

/// Bind a persisted catalog expression with access to user-defined types.
///
/// The original catalog-expression entry point remains available for callers
/// that cannot contain named casts. Durable expressions owned by a catalog
/// should use this overload so enum/domain casts are resolved from the same
/// immutable catalog snapshot that owns the expression.
pub fn bind_catalog_expression_with_catalog(
    expression: &CatalogExpression,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
    catalog: &Catalog,
) -> Result<BoundExpr> {
    bind_catalog_expression_with_parameter_types_and_catalog(
        expression,
        table,
        expected,
        &BTreeMap::new(),
        Some(catalog),
    )
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
    bind_catalog_expression_with_parameter_types_and_catalog(
        expression,
        table,
        expected,
        parameter_types,
        None,
    )
}

/// Bind a persisted expression with positional parameter types and an
/// optional catalog used to resolve named enum/domain casts.
pub fn bind_catalog_expression_with_parameter_types_and_catalog(
    expression: &CatalogExpression,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
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
    let mut expression = convert_expr(parsed, &expression.sql)?;
    resolve_expr_types(&mut expression, parameter_types, catalog, 0, None)?;
    bind_expr_with_parameter_types(expression, table, expected, parameter_types)
}

/// Bind an OrdaDB-owned parsed statement against an immutable catalog view.
pub fn bind(statement: ParsedStatement, catalog: &Catalog) -> Result<BoundStatement> {
    bind_internal(statement, catalog, None)
}

/// Bind a parsed statement with the values owned by the current database
/// session. Session-dependent scalar functions are materialized before type
/// solving so Describe and Execute observe one immutable statement value.
pub fn bind_with_session(
    statement: ParsedStatement,
    catalog: &Catalog,
    session: SessionBindValues<'_>,
) -> Result<BoundStatement> {
    bind_internal(statement, catalog, Some(session))
}

fn bind_internal(
    mut statement: ParsedStatement,
    catalog: &Catalog,
    session: Option<SessionBindValues<'_>>,
) -> Result<BoundStatement> {
    resolve_statement_types(&mut statement, &BTreeMap::new(), Some(catalog), 0, session)?;
    let parameter_types = ParameterTypeSolver::solve(&statement, catalog)?;
    resolve_statement_types(&mut statement, &parameter_types, None, 0, session)?;
    bind_with_view_depth(statement, catalog, 0)
}

const MAX_PARAMETER_SOLVER_PASSES: usize = 128;
const MAX_PARAMETER_SOLVER_DEPTH: usize = 64;

#[derive(Default)]
struct ParameterTypeSolver {
    types: BTreeMap<usize, ScalarType>,
    changed: bool,
}

impl ParameterTypeSolver {
    fn solve(
        statement: &ParsedStatement,
        catalog: &Catalog,
    ) -> Result<BTreeMap<usize, ScalarType>> {
        let mut solver = Self::default();
        for _ in 0..MAX_PARAMETER_SOLVER_PASSES {
            solver.changed = false;
            solver.collect_statement(statement, catalog, &[], None, 0)?;
            if !solver.changed {
                return Ok(solver.types);
            }
        }
        Err(DbError::new(
            "54001",
            "parameter type inference exceeded its fixed-point pass limit",
        ))
    }

    fn constrain(
        &mut self,
        index: usize,
        data_type: &ScalarType,
        position: Option<usize>,
    ) -> Result<()> {
        if let Some(existing) = self.types.get(&index) {
            if existing != data_type {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    format!("inconsistent types deduced for parameter ${index}"),
                )
                .with_detail(format!(
                    "parameter ${index} was constrained as both {existing:?} and {data_type:?}"
                ))
                .with_position_opt(position));
            }
            return Ok(());
        }
        self.types.insert(index, data_type.clone());
        self.changed = true;
        Ok(())
    }

    fn collect_statement(
        &mut self,
        statement: &ParsedStatement,
        catalog: &Catalog,
        outer_inputs: &[InputColumn],
        expected_output: Option<&[Option<ScalarType>]>,
        depth: usize,
    ) -> Result<Vec<Option<ScalarType>>> {
        if depth >= MAX_PARAMETER_SOLVER_DEPTH {
            return Err(DbError::new(
                "54001",
                "parameter type inference exceeded its statement depth limit",
            ));
        }
        match statement {
            ParsedStatement::Select {
                table,
                projection,
                filter,
                order_by,
                offset,
                limit,
            } => {
                let local_inputs = parameter_relation_inputs(table, None, catalog, 0, false)?;
                let inputs = inputs_with_outer(&local_inputs, outer_inputs)?;
                if let Some(filter) = filter {
                    self.collect_expr(filter, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
                }
                for order in order_by {
                    self.collect_order_expr(&order.expr, &inputs, catalog, depth)?;
                }
                if let Some(offset) = offset {
                    self.collect_expr(offset, &inputs, Some(&ScalarType::Int64), catalog, depth)?;
                }
                if let Some(limit) = limit {
                    self.collect_expr(limit, &inputs, Some(&ScalarType::Int64), catalog, depth)?;
                }
                self.collect_projection(projection, &inputs, expected_output, catalog, depth)
            }
            ParsedStatement::AdvancedSelect {
                table,
                joins,
                projection,
                filter,
                group_by,
                having,
                order_by,
                offset,
                limit,
                ..
            } => {
                let binding = table.alias.as_ref().map(|alias| alias.name.clone());
                let mut local_inputs =
                    parameter_relation_inputs(&table.name, binding, catalog, 0, false)?;
                for join in joins {
                    match &join.source {
                        ParsedJoinSource::Table(table) => {
                            let binding = table.alias.as_ref().map(|alias| alias.name.clone());
                            let offset = local_inputs.len();
                            local_inputs.extend(parameter_relation_inputs(
                                &table.name,
                                binding,
                                catalog,
                                offset,
                                join.kind == JoinKind::Left,
                            )?);
                        }
                        ParsedJoinSource::Derived {
                            lateral,
                            query,
                            alias,
                            columns,
                        } => {
                            let visible = if *lateral {
                                inputs_with_outer(&local_inputs, outer_inputs)?
                            } else {
                                Vec::new()
                            };
                            self.collect_statement(query, catalog, &visible, None, depth + 1)?;
                            if let Some(schema) =
                                self.try_statement_schema(query, catalog, &visible, depth + 1)
                            {
                                let offset = local_inputs.len();
                                for (index, field) in schema.fields.iter().enumerate() {
                                    let name = columns.get(index).map_or_else(
                                        || Identifier::unquoted(&field.name),
                                        |name| name.name.clone(),
                                    );
                                    local_inputs.push(InputColumn {
                                        binding: alias.name.clone(),
                                        name,
                                        index: offset + index,
                                        data_type: field.data_type.clone(),
                                        nullable: join.kind == JoinKind::Left || field.nullable,
                                        outer_depth: 0,
                                    });
                                }
                            }
                        }
                    }
                    let inputs = inputs_with_outer(&local_inputs, outer_inputs)?;
                    self.collect_expr(
                        &join.on,
                        &inputs,
                        Some(&ScalarType::Boolean),
                        catalog,
                        depth,
                    )?;
                }
                let inputs = inputs_with_outer(&local_inputs, outer_inputs)?;
                if let Some(filter) = filter {
                    self.collect_expr(filter, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
                }
                for expression in group_by {
                    self.collect_expr(expression, &inputs, None, catalog, depth)?;
                }
                if let Some(having) = having {
                    self.collect_expr(having, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
                }
                for order in order_by {
                    self.collect_order_expr(&order.expr, &inputs, catalog, depth)?;
                }
                if let Some(offset) = offset {
                    self.collect_expr(offset, &inputs, Some(&ScalarType::Int64), catalog, depth)?;
                }
                if let Some(limit) = limit {
                    self.collect_expr(limit, &inputs, Some(&ScalarType::Int64), catalog, depth)?;
                }
                self.collect_projection(projection, &local_inputs, expected_output, catalog, depth)
            }
            ParsedStatement::SetOperation {
                left,
                right,
                order_by,
                offset,
                limit,
                ..
            } => {
                let mut left_output = self.collect_statement(
                    left,
                    catalog,
                    outer_inputs,
                    expected_output,
                    depth + 1,
                )?;
                let mut right_output = self.collect_statement(
                    right,
                    catalog,
                    outer_inputs,
                    expected_output,
                    depth + 1,
                )?;
                if left_output.len() == right_output.len() {
                    let reconciled = left_output
                        .iter()
                        .zip(&right_output)
                        .map(|(left, right)| match (left, right) {
                            (Some(left), Some(right)) => common_type(left, right),
                            (Some(data_type), None) | (None, Some(data_type)) => {
                                Some(data_type.clone())
                            }
                            (None, None) => None,
                        })
                        .collect::<Vec<_>>();
                    left_output = self.collect_statement(
                        left,
                        catalog,
                        outer_inputs,
                        Some(&reconciled),
                        depth + 1,
                    )?;
                    right_output = self.collect_statement(
                        right,
                        catalog,
                        outer_inputs,
                        Some(&reconciled),
                        depth + 1,
                    )?;
                }
                let _ = order_by;
                if let Some(offset) = offset {
                    self.collect_expr(offset, &[], Some(&ScalarType::Int64), catalog, depth)?;
                }
                if let Some(limit) = limit {
                    self.collect_expr(limit, &[], Some(&ScalarType::Int64), catalog, depth)?;
                }
                Ok(left_output
                    .into_iter()
                    .zip(right_output)
                    .map(|(left, right)| match (left, right) {
                        (Some(left), Some(right)) => common_type(&left, &right),
                        (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                        (None, None) => None,
                    })
                    .collect())
            }
            ParsedStatement::With {
                recursive,
                ctes,
                body,
            } => {
                self.collect_with_statement(*recursive, ctes, body, catalog, expected_output, depth)
            }
            ParsedStatement::Insert {
                table,
                columns,
                rows,
                on_conflict,
                returning,
            } => {
                let relation = resolve_dml_relation(table, CatalogTriggerEvent::Insert, catalog)?;
                let table = &relation.scope;
                let column_indexes = parameter_target_columns(columns, table)?;
                for row in rows {
                    for (expression, index) in row.iter().zip(&column_indexes) {
                        self.collect_expr(
                            expression,
                            &[],
                            Some(&table.columns()[*index].data_type),
                            catalog,
                            depth,
                        )?;
                    }
                }
                if let Some(on_conflict) = on_conflict {
                    if matches!(relation.target, DmlTarget::View(_)) {
                        return unsupported("ON CONFLICT is not supported for view DML");
                    }
                    self.collect_on_conflict(on_conflict, table, catalog, depth)?;
                }
                let inputs = parameter_table_inputs(table, table.name.clone(), 0, false);
                self.collect_projection(returning, &inputs, expected_output, catalog, depth)
            }
            ParsedStatement::Update {
                table,
                assignments,
                filter,
                returning,
            } => {
                let relation = resolve_dml_relation(table, CatalogTriggerEvent::Update, catalog)?;
                let table = &relation.scope;
                let inputs = parameter_table_inputs(table, table.name.clone(), 0, false);
                for (column, expression) in assignments {
                    if let Some(index) = table.column_index(&column.name) {
                        self.collect_expr(
                            expression,
                            &inputs,
                            Some(&table.columns()[index].data_type),
                            catalog,
                            depth,
                        )?;
                    }
                }
                if let Some(filter) = filter {
                    self.collect_expr(filter, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
                }
                self.collect_projection(returning, &inputs, expected_output, catalog, depth)
            }
            ParsedStatement::Delete {
                table,
                filter,
                returning,
            } => {
                let relation = resolve_dml_relation(table, CatalogTriggerEvent::Delete, catalog)?;
                let table = &relation.scope;
                let inputs = parameter_table_inputs(table, table.name.clone(), 0, false);
                if let Some(filter) = filter {
                    self.collect_expr(filter, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
                }
                self.collect_projection(returning, &inputs, expected_output, catalog, depth)
            }
            ParsedStatement::Merge(merge) => {
                self.collect_merge(merge, catalog, expected_output, depth)
            }
            ParsedStatement::Explain { statement }
            | ParsedStatement::CreateView {
                query: statement, ..
            } => {
                self.collect_statement(statement, catalog, outer_inputs, expected_output, depth + 1)
            }
            ParsedStatement::Call {
                name, arguments, ..
            }
            | ParsedStatement::RoutineSelect {
                name, arguments, ..
            } => {
                self.collect_routine_arguments(name, arguments, catalog, depth)?;
                Ok(Vec::new())
            }
            ParsedStatement::ScalarSelect { projection } => {
                self.collect_projection(projection, &[], expected_output, catalog, depth)
            }
            ParsedStatement::SequenceValue { operation, .. } => {
                if let ParsedSequenceOperation::SetValue { value, .. } = operation {
                    self.collect_expr(value, &[], Some(&ScalarType::Int64), catalog, depth)?;
                }
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    fn collect_projection(
        &mut self,
        projection: &[ParsedProjection],
        inputs: &[InputColumn],
        expected_output: Option<&[Option<ScalarType>]>,
        catalog: &Catalog,
        depth: usize,
    ) -> Result<Vec<Option<ScalarType>>> {
        let mut output = Vec::new();
        for item in projection {
            match item {
                ParsedProjection::Wildcard => {
                    output.extend(
                        inputs
                            .iter()
                            .filter(|input| input.outer_depth == 0)
                            .map(|input| Some(input.data_type.clone())),
                    );
                }
                ParsedProjection::Expression { expr, .. } => {
                    let expected = expected_output
                        .and_then(|expected| expected.get(output.len()))
                        .and_then(Option::as_ref);
                    output.push(self.collect_expr(expr, inputs, expected, catalog, depth)?);
                }
            }
        }
        Ok(output)
    }

    fn collect_order_expr(
        &mut self,
        expression: &ParsedExpr,
        inputs: &[InputColumn],
        catalog: &Catalog,
        depth: usize,
    ) -> Result<()> {
        match self.collect_expr(expression, inputs, None, catalog, depth) {
            Ok(_) => Ok(()),
            Err(error) if error.sql_state == UNDEFINED_COLUMN => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn collect_expr(
        &mut self,
        expression: &ParsedExpr,
        inputs: &[InputColumn],
        expected: Option<&ScalarType>,
        catalog: &Catalog,
        depth: usize,
    ) -> Result<Option<ScalarType>> {
        if depth >= MAX_PARAMETER_SOLVER_DEPTH {
            return Err(DbError::new(
                "54001",
                "parameter type inference exceeded its expression depth limit",
            ));
        }
        match &expression.kind {
            ParsedExprKind::Column(name) => {
                Ok(Some(resolve_input_column(name, inputs)?.data_type.clone()))
            }
            ParsedExprKind::Literal(value) => Ok(value.scalar_type().or_else(|| expected.cloned())),
            ParsedExprKind::Parameter(index) => {
                if let Some(expected) = expected {
                    self.constrain(*index, expected, expression.position)?;
                }
                Ok(self.types.get(index).cloned())
            }
            ParsedExprKind::ResolvedParameter { index, data_type } => {
                self.constrain(*index, data_type, expression.position)?;
                if let Some(expected) = expected {
                    self.constrain(*index, expected, expression.position)?;
                }
                Ok(Some(data_type.clone()))
            }
            ParsedExprKind::ApplyValue { data_type, .. } => Ok(Some(data_type.clone())),
            ParsedExprKind::Unary { op, expr } => match op {
                UnaryOperator::Not => {
                    self.collect_expr(
                        expr,
                        inputs,
                        Some(&ScalarType::Boolean),
                        catalog,
                        depth + 1,
                    )?;
                    Ok(Some(ScalarType::Boolean))
                }
                UnaryOperator::Negate => {
                    self.collect_expr(expr, inputs, expected, catalog, depth + 1)
                }
            },
            ParsedExprKind::Cast {
                expr, data_type, ..
            } => {
                if let Some(index) = parsed_parameter_index(expr) {
                    self.constrain(index, data_type, expr.position)?;
                    self.collect_expr(expr, inputs, Some(data_type), catalog, depth + 1)?;
                } else {
                    self.collect_expr(expr, inputs, None, catalog, depth + 1)?;
                }
                Ok(Some(data_type.clone()))
            }
            ParsedExprKind::Array { elements, .. } => {
                let expected_element = match expected {
                    Some(ScalarType::Array { element }) => Some(element.as_ref()),
                    Some(_) => None,
                    None => None,
                };
                let mut element_type = expected_element.cloned();
                for element in elements {
                    let candidate =
                        self.collect_expr(element, inputs, expected_element, catalog, depth + 1)?;
                    element_type = match (element_type, candidate) {
                        (Some(left), Some(right)) => common_type(&left, &right),
                        (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                        (None, None) => None,
                    };
                }
                Ok(element_type.map(|element| ScalarType::Array {
                    element: Box::new(element),
                }))
            }
            ParsedExprKind::Function {
                function,
                arguments,
            } => {
                self.collect_scalar_function(*function, arguments, inputs, expected, catalog, depth)
            }
            ParsedExprKind::Binary { left, op, right } => {
                if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    self.collect_expr(
                        left,
                        inputs,
                        Some(&ScalarType::Boolean),
                        catalog,
                        depth + 1,
                    )?;
                    self.collect_expr(
                        right,
                        inputs,
                        Some(&ScalarType::Boolean),
                        catalog,
                        depth + 1,
                    )?;
                    return Ok(Some(ScalarType::Boolean));
                }
                let left_type = self.collect_expr(left, inputs, None, catalog, depth + 1)?;
                let right_type = self.collect_expr(right, inputs, None, catalog, depth + 1)?;
                if let (Some(index), Some(data_type)) =
                    (parsed_parameter_index(left), right_type.as_ref())
                {
                    self.constrain(index, data_type, left.position)?;
                }
                if let (Some(index), Some(data_type)) =
                    (parsed_parameter_index(right), left_type.as_ref())
                {
                    self.constrain(index, data_type, right.position)?;
                }
                let operand_type = match (left_type, right_type) {
                    (Some(left), Some(right)) => common_type(&left, &right),
                    (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                    (None, None) if is_arithmetic_operator(*op) => expected.cloned(),
                    (None, None) => None,
                };
                if let Some(operand_type) = &operand_type {
                    self.collect_expr(left, inputs, Some(operand_type), catalog, depth + 1)?;
                    self.collect_expr(right, inputs, Some(operand_type), catalog, depth + 1)?;
                }
                Ok(if is_arithmetic_operator(*op) {
                    operand_type
                } else {
                    Some(ScalarType::Boolean)
                })
            }
            ParsedExprKind::InList { expr, list, .. } => {
                let mut operand_type = self.collect_expr(expr, inputs, None, catalog, depth + 1)?;
                for candidate in list {
                    let candidate_type =
                        self.collect_expr(candidate, inputs, None, catalog, depth + 1)?;
                    operand_type = match (operand_type, candidate_type) {
                        (Some(left), Some(right)) => common_type(&left, &right),
                        (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                        (None, None) => None,
                    };
                }
                if let Some(operand_type) = &operand_type {
                    self.collect_expr(expr, inputs, Some(operand_type), catalog, depth + 1)?;
                    for candidate in list {
                        self.collect_expr(
                            candidate,
                            inputs,
                            Some(operand_type),
                            catalog,
                            depth + 1,
                        )?;
                    }
                }
                Ok(Some(ScalarType::Boolean))
            }
            ParsedExprKind::ScalarSubquery(subquery) => {
                let expected_output = expected.cloned().map(|data_type| vec![Some(data_type)]);
                let output = self.collect_statement(
                    subquery,
                    catalog,
                    inputs,
                    expected_output.as_deref(),
                    depth + 1,
                )?;
                Ok(output.first().cloned().flatten())
            }
            ParsedExprKind::Exists { subquery, .. } => {
                self.collect_statement(subquery, catalog, inputs, None, depth + 1)?;
                Ok(Some(ScalarType::Boolean))
            }
            ParsedExprKind::InSubquery { expr, subquery, .. }
            | ParsedExprKind::QuantifiedSubquery {
                left: expr,
                subquery,
                ..
            } => {
                let left_type = self.collect_expr(expr, inputs, None, catalog, depth + 1)?;
                let expected_output = left_type.clone().map(|data_type| vec![Some(data_type)]);
                let output = self.collect_statement(
                    subquery,
                    catalog,
                    inputs,
                    expected_output.as_deref(),
                    depth + 1,
                )?;
                let operand_type = match (left_type, output.first().cloned().flatten()) {
                    (Some(left), Some(right)) => common_type(&left, &right),
                    (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                    (None, None) => None,
                };
                if let Some(operand_type) = &operand_type {
                    self.collect_expr(expr, inputs, Some(operand_type), catalog, depth + 1)?;
                    let expected = [Some(operand_type.clone())];
                    self.collect_statement(subquery, catalog, inputs, Some(&expected), depth + 1)?;
                }
                Ok(Some(ScalarType::Boolean))
            }
            ParsedExprKind::RowSubquery { left, subquery, .. } => {
                let mut left_types = Vec::with_capacity(left.len());
                for expression in left {
                    left_types.push(self.collect_expr(
                        expression,
                        inputs,
                        None,
                        catalog,
                        depth + 1,
                    )?);
                }
                let output = self.collect_statement(
                    subquery,
                    catalog,
                    inputs,
                    Some(&left_types),
                    depth + 1,
                )?;
                for (expression, data_type) in left.iter().zip(output) {
                    if let Some(data_type) = data_type {
                        self.collect_expr(
                            expression,
                            inputs,
                            Some(&data_type),
                            catalog,
                            depth + 1,
                        )?;
                    }
                }
                Ok(Some(ScalarType::Boolean))
            }
            ParsedExprKind::Aggregate {
                function,
                argument,
                filter,
                ..
            } => {
                if let Some(filter) = filter {
                    self.collect_expr(
                        filter,
                        inputs,
                        Some(&ScalarType::Boolean),
                        catalog,
                        depth + 1,
                    )?;
                }
                let argument_type = argument
                    .as_deref()
                    .map(|argument| self.collect_expr(argument, inputs, None, catalog, depth + 1))
                    .transpose()?
                    .flatten();
                Ok(parameter_aggregate_type(*function, argument_type))
            }
            ParsedExprKind::Window { call, spec } => {
                self.collect_window(call, spec, inputs, catalog, depth)
            }
            ParsedExprKind::NamedWindow { call, .. } => self.collect_window(
                call,
                &ParsedWindowSpec {
                    window_name: None,
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    frame: None,
                },
                inputs,
                catalog,
                depth,
            ),
            ParsedExprKind::WindowValue { .. } => Ok(None),
        }
    }

    fn collect_scalar_function(
        &mut self,
        function: ScalarFunction,
        arguments: &[ParsedExpr],
        inputs: &[InputColumn],
        expected: Option<&ScalarType>,
        catalog: &Catalog,
        depth: usize,
    ) -> Result<Option<ScalarType>> {
        match function {
            ScalarFunction::Version
            | ScalarFunction::CurrentDatabase
            | ScalarFunction::CurrentUser
            | ScalarFunction::SessionUser
            | ScalarFunction::CurrentSetting => Ok(Some(ScalarType::Text)),
            ScalarFunction::Lower
            | ScalarFunction::Upper
            | ScalarFunction::Btrim
            | ScalarFunction::Ltrim
            | ScalarFunction::Rtrim
            | ScalarFunction::Replace
            | ScalarFunction::Strpos => {
                for argument in arguments {
                    self.collect_expr(
                        argument,
                        inputs,
                        Some(&ScalarType::Text),
                        catalog,
                        depth + 1,
                    )?;
                }
                Ok(Some(if function == ScalarFunction::Strpos {
                    ScalarType::Int32
                } else {
                    ScalarType::Text
                }))
            }
            ScalarFunction::CharacterLength | ScalarFunction::OctetLength => {
                let data_type =
                    self.collect_expr(&arguments[0], inputs, None, catalog, depth + 1)?;
                if data_type.is_none() {
                    self.collect_expr(
                        &arguments[0],
                        inputs,
                        Some(&ScalarType::Text),
                        catalog,
                        depth + 1,
                    )?;
                }
                Ok(Some(ScalarType::Int32))
            }
            ScalarFunction::Abs => {
                let data_type = self.collect_expr(
                    &arguments[0],
                    inputs,
                    expected.filter(|expected| is_numeric(expected)),
                    catalog,
                    depth + 1,
                )?;
                Ok(data_type)
            }
            ScalarFunction::Coalesce
            | ScalarFunction::NullIf
            | ScalarFunction::Greatest
            | ScalarFunction::Least => {
                let mut common = expected.cloned();
                for argument in arguments {
                    let candidate =
                        self.collect_expr(argument, inputs, None, catalog, depth + 1)?;
                    common = match (common, candidate) {
                        (Some(left), Some(right)) => common_type(&left, &right),
                        (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                        (None, None) => None,
                    };
                }
                if let Some(common) = &common {
                    for argument in arguments {
                        self.collect_expr(argument, inputs, Some(common), catalog, depth + 1)?;
                    }
                }
                Ok(common)
            }
            ScalarFunction::Concat => {
                for argument in arguments {
                    let data_type =
                        self.collect_expr(argument, inputs, None, catalog, depth + 1)?;
                    if data_type.is_none() {
                        self.collect_expr(
                            argument,
                            inputs,
                            Some(&ScalarType::Text),
                            catalog,
                            depth + 1,
                        )?;
                    }
                }
                Ok(Some(ScalarType::Text))
            }
            ScalarFunction::Substring => {
                self.collect_expr(
                    &arguments[0],
                    inputs,
                    Some(&ScalarType::Text),
                    catalog,
                    depth + 1,
                )?;
                for argument in &arguments[1..] {
                    self.collect_expr(
                        argument,
                        inputs,
                        Some(&ScalarType::Int32),
                        catalog,
                        depth + 1,
                    )?;
                }
                Ok(Some(ScalarType::Text))
            }
            ScalarFunction::JsonbTypeof => {
                self.collect_expr(
                    &arguments[0],
                    inputs,
                    Some(&ScalarType::Jsonb),
                    catalog,
                    depth + 1,
                )?;
                Ok(Some(ScalarType::Text))
            }
            ScalarFunction::ArrayLength => {
                self.collect_expr(&arguments[0], inputs, None, catalog, depth + 1)?;
                self.collect_expr(
                    &arguments[1],
                    inputs,
                    Some(&ScalarType::Int32),
                    catalog,
                    depth + 1,
                )?;
                Ok(Some(ScalarType::Int32))
            }
            ScalarFunction::Cardinality => {
                self.collect_expr(&arguments[0], inputs, None, catalog, depth + 1)?;
                Ok(Some(ScalarType::Int32))
            }
        }
    }

    fn collect_window(
        &mut self,
        call: &ParsedWindowCall,
        spec: &ParsedWindowSpec,
        inputs: &[InputColumn],
        catalog: &Catalog,
        depth: usize,
    ) -> Result<Option<ScalarType>> {
        for expression in &spec.partition_by {
            self.collect_expr(expression, inputs, None, catalog, depth + 1)?;
        }
        for order in &spec.order_by {
            self.collect_expr(&order.expr, inputs, None, catalog, depth + 1)?;
        }
        if let Some(frame) = &spec.frame {
            let range_type = if frame.units == WindowFrameUnits::Range {
                spec.order_by
                    .first()
                    .map(|order| self.collect_expr(&order.expr, inputs, None, catalog, depth + 1))
                    .transpose()?
                    .flatten()
            } else {
                Some(ScalarType::Int64)
            };
            for bound in [&frame.start_bound, &frame.end_bound] {
                if let ParsedWindowFrameBound::Preceding(expression)
                | ParsedWindowFrameBound::Following(expression) = bound
                {
                    self.collect_expr(expression, inputs, range_type.as_ref(), catalog, depth + 1)?;
                }
            }
        }
        if let Some(filter) = &call.filter {
            self.collect_expr(
                filter,
                inputs,
                Some(&ScalarType::Boolean),
                catalog,
                depth + 1,
            )?;
        }
        match call.function {
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                Ok(Some(ScalarType::Int64))
            }
            WindowFunction::FirstValue
            | WindowFunction::LastValue
            | WindowFunction::Lag
            | WindowFunction::Lead
            | WindowFunction::NthValue => {
                let value_type = call
                    .arguments
                    .first()
                    .map(|argument| self.collect_expr(argument, inputs, None, catalog, depth + 1))
                    .transpose()?
                    .flatten();
                if matches!(call.function, WindowFunction::Lag | WindowFunction::Lead)
                    && let Some(offset) = call.arguments.get(1)
                {
                    self.collect_expr(
                        offset,
                        inputs,
                        Some(&ScalarType::Int64),
                        catalog,
                        depth + 1,
                    )?;
                }
                if call.function == WindowFunction::NthValue
                    && let Some(offset) = call.arguments.get(1)
                {
                    self.collect_expr(
                        offset,
                        inputs,
                        Some(&ScalarType::Int64),
                        catalog,
                        depth + 1,
                    )?;
                }
                if matches!(call.function, WindowFunction::Lag | WindowFunction::Lead)
                    && let Some(default) = call.arguments.get(2)
                {
                    let default_type = self.collect_expr(
                        default,
                        inputs,
                        value_type.as_ref(),
                        catalog,
                        depth + 1,
                    )?;
                    let reconciled = match (value_type, default_type) {
                        (Some(left), Some(right)) => common_type(&left, &right),
                        (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                        (None, None) => None,
                    };
                    if let Some(reconciled) = &reconciled {
                        if let Some(value) = call.arguments.first() {
                            self.collect_expr(value, inputs, Some(reconciled), catalog, depth + 1)?;
                        }
                        self.collect_expr(default, inputs, Some(reconciled), catalog, depth + 1)?;
                    }
                    return Ok(reconciled);
                }
                Ok(value_type)
            }
            WindowFunction::Aggregate(function) => {
                let argument_type = call
                    .arguments
                    .first()
                    .map(|argument| self.collect_expr(argument, inputs, None, catalog, depth + 1))
                    .transpose()?
                    .flatten();
                Ok(parameter_aggregate_type(function, argument_type))
            }
        }
    }

    fn collect_on_conflict(
        &mut self,
        on_conflict: &ParsedOnConflict,
        table: &TableDefinition,
        catalog: &Catalog,
        depth: usize,
    ) -> Result<()> {
        let ParsedConflictAction::DoUpdate {
            assignments,
            filter,
        } = &on_conflict.action
        else {
            return Ok(());
        };
        let width = table.columns().len();
        let mut inputs = parameter_table_inputs(table, table.name.clone(), 0, false);
        inputs.extend(parameter_table_inputs(
            table,
            Identifier::unquoted("excluded"),
            width,
            false,
        ));
        for (column, expression) in assignments {
            if let Some(index) = table.column_index(&column.name) {
                self.collect_expr(
                    expression,
                    &inputs,
                    Some(&table.columns()[index].data_type),
                    catalog,
                    depth,
                )?;
            }
        }
        if let Some(filter) = filter {
            self.collect_expr(filter, &inputs, Some(&ScalarType::Boolean), catalog, depth)?;
        }
        Ok(())
    }

    fn collect_merge(
        &mut self,
        merge: &ParsedMerge,
        catalog: &Catalog,
        expected_output: Option<&[Option<ScalarType>]>,
        depth: usize,
    ) -> Result<Vec<Option<ScalarType>>> {
        let target = resolve_table(&merge.target.name, catalog)?;
        let source = resolve_table(&merge.source.name, catalog)?;
        let target_binding = merge
            .target
            .alias
            .as_ref()
            .map_or_else(|| target.name.clone(), |alias| alias.name.clone());
        let source_binding = merge
            .source
            .alias
            .as_ref()
            .map_or_else(|| source.name.clone(), |alias| alias.name.clone());
        let mut inputs = parameter_table_inputs(target, target_binding, 0, false);
        let source_offset = inputs.len();
        inputs.extend(parameter_table_inputs(
            source,
            source_binding,
            source_offset,
            false,
        ));
        self.collect_expr(
            &merge.on,
            &inputs,
            Some(&ScalarType::Boolean),
            catalog,
            depth,
        )?;
        for clause in &merge.clauses {
            if let Some(predicate) = &clause.predicate {
                self.collect_expr(
                    predicate,
                    &inputs,
                    Some(&ScalarType::Boolean),
                    catalog,
                    depth,
                )?;
            }
            match &clause.action {
                ParsedMergeAction::Update { assignments } => {
                    for (column, expression) in assignments {
                        if let Some(index) = target.column_index(&column.name) {
                            self.collect_expr(
                                expression,
                                &inputs,
                                Some(&target.columns()[index].data_type),
                                catalog,
                                depth,
                            )?;
                        }
                    }
                }
                ParsedMergeAction::Insert { columns, values } => {
                    let column_indexes = parameter_target_columns(columns, target)?;
                    for (expression, index) in values.iter().zip(column_indexes) {
                        self.collect_expr(
                            expression,
                            &inputs,
                            Some(&target.columns()[index].data_type),
                            catalog,
                            depth,
                        )?;
                    }
                }
                ParsedMergeAction::Delete | ParsedMergeAction::DoNothing => {}
            }
        }
        let target_inputs = parameter_table_inputs(target, target.name.clone(), 0, false);
        self.collect_projection(
            &merge.returning,
            &target_inputs,
            expected_output,
            catalog,
            depth,
        )
    }

    fn collect_routine_arguments(
        &mut self,
        name: &ParsedObjectName,
        arguments: &[ParsedExpr],
        catalog: &Catalog,
        depth: usize,
    ) -> Result<()> {
        let (schema, name, _) = split_table_name(name)?;
        let Some(schema) = catalog.schema(&schema) else {
            return Ok(());
        };
        let candidates = schema
            .routines_named(&name)
            .iter()
            .filter(|routine| routine.arguments.len() == arguments.len())
            .collect::<Vec<_>>();
        for (index, expression) in arguments.iter().enumerate() {
            let types = candidates
                .iter()
                .map(|routine| routine.arguments[index].data_type.clone())
                .collect::<Vec<_>>();
            let expected = types
                .first()
                .filter(|first| types.iter().all(|data_type| data_type == *first))
                .cloned();
            self.collect_expr(expression, &[], expected.as_ref(), catalog, depth)?;
        }
        Ok(())
    }

    fn collect_with_statement(
        &mut self,
        recursive: bool,
        ctes: &[ParsedCte],
        body: &ParsedStatement,
        catalog: &Catalog,
        expected_output: Option<&[Option<ScalarType>]>,
        depth: usize,
    ) -> Result<Vec<Option<ScalarType>>> {
        let mut transient_catalog = catalog.clone();
        let temporary_schema = Identifier::unquoted(format!("__ordadb_param_cte_{depth}"));
        if transient_catalog.schema(&temporary_schema).is_none() {
            transient_catalog.create_schema(temporary_schema.clone())?;
        }
        let mut replacements = BTreeMap::new();
        let mut names = BTreeSet::new();
        for cte in ctes {
            if !names.insert(cte.name.name.clone()) {
                return Err(DbError::new(
                    "42712",
                    format!("WITH query name {} specified more than once", cte.name.name),
                )
                .with_position_opt(cte.name.position));
            }
            let query = (*cte.query).clone();
            let self_recursive =
                recursive && parsed_query_references_table(&query, &cte.name.name, 0)?;
            let (mut seed, recursive_term) = if self_recursive {
                match query {
                    ParsedStatement::SetOperation {
                        left,
                        operator: QuerySetOperator::Union,
                        right,
                        ..
                    } => (*left, Some(*right)),
                    _ => return Ok(Vec::new()),
                }
            } else {
                (query, None)
            };
            rewrite_cte_references(&mut seed, &replacements, 0)?;
            let seed_types =
                self.collect_statement(&seed, &transient_catalog, &[], None, depth + 1)?;
            let Some(mut output) =
                self.try_statement_schema(&seed, &transient_catalog, &[], depth + 1)
            else {
                return Ok(seed_types);
            };
            apply_cte_column_aliases(&cte.name, &cte.columns, &mut output)?;
            create_cte_relation(
                &mut transient_catalog,
                &temporary_schema,
                &cte.name,
                &output,
            )?;
            replacements.insert(
                cte.name.name.clone(),
                cte_replacement_name(&temporary_schema, &cte.name),
            );
            if let Some(mut recursive_term) = recursive_term {
                rewrite_cte_references(&mut recursive_term, &replacements, 0)?;
                let expected = output
                    .fields
                    .iter()
                    .map(|field| Some(field.data_type.clone()))
                    .collect::<Vec<_>>();
                self.collect_statement(
                    &recursive_term,
                    &transient_catalog,
                    &[],
                    Some(&expected),
                    depth + 1,
                )?;
            }
        }
        let mut body = body.clone();
        rewrite_cte_references(&mut body, &replacements, 0)?;
        self.collect_statement(&body, &transient_catalog, &[], expected_output, depth + 1)
    }

    fn try_statement_schema(
        &self,
        statement: &ParsedStatement,
        catalog: &Catalog,
        outer_inputs: &[InputColumn],
        depth: usize,
    ) -> Option<Schema> {
        let mut statement = statement.clone();
        resolve_statement_types(&mut statement, &self.types, None, depth, None).ok()?;
        let statement = if outer_inputs.is_empty() {
            bind_with_view_depth(statement, catalog, depth).ok()?
        } else {
            bind_apply_query(statement, catalog, depth, outer_inputs).ok()?
        };
        bound_query_schema(&statement).ok()
    }
}

fn parameter_table_inputs(
    table: &TableDefinition,
    binding: Identifier,
    offset: usize,
    nullable: bool,
) -> Vec<InputColumn> {
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
            outer_depth: 0,
        })
        .collect()
}

fn parameter_relation_inputs(
    name: &ParsedObjectName,
    binding: Option<Identifier>,
    catalog: &Catalog,
    offset: usize,
    nullable: bool,
) -> Result<Vec<InputColumn>> {
    let (schema, relation, _) = split_table_name(name)?;
    let binding = binding.unwrap_or_else(|| relation.clone());
    if let Some(view) = catalog.view(&schema, &relation) {
        return Ok(view
            .output
            .fields
            .iter()
            .enumerate()
            .map(|(column_offset, field)| InputColumn {
                binding: binding.clone(),
                name: Identifier::unquoted(&field.name),
                index: offset + column_offset,
                data_type: field.data_type.clone(),
                nullable: nullable || field.nullable,
                outer_depth: 0,
            })
            .collect());
    }
    let table = resolve_table(name, catalog)?;
    Ok(parameter_table_inputs(table, binding, offset, nullable))
}

fn parameter_target_columns(
    columns: &[ParsedIdentifier],
    table: &TableDefinition,
) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Ok((0..table.columns().len()).collect());
    }
    columns
        .iter()
        .map(|column| {
            table.column_index(&column.name).ok_or_else(|| {
                DbError::new(
                    UNDEFINED_COLUMN,
                    format!("column {} does not exist", column.name),
                )
                .with_position_opt(column.position)
            })
        })
        .collect()
}

fn parameter_aggregate_type(
    function: AggregateFunction,
    argument_type: Option<ScalarType>,
) -> Option<ScalarType> {
    match function {
        AggregateFunction::Count => Some(ScalarType::Int64),
        AggregateFunction::Avg => argument_type.map(|_| ScalarType::Float64),
        AggregateFunction::Sum => argument_type.map(|data_type| match data_type {
            ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64 => ScalarType::Int64,
            ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
            other => other,
        }),
        AggregateFunction::Min | AggregateFunction::Max => argument_type,
    }
}

fn parsed_parameter_index(expression: &ParsedExpr) -> Option<usize> {
    match expression.kind {
        ParsedExprKind::Parameter(index) | ParsedExprKind::ResolvedParameter { index, .. } => {
            Some(index)
        }
        _ => None,
    }
}

fn resolve_statement_types(
    statement: &mut ParsedStatement,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    if depth >= MAX_PARAMETER_SOLVER_DEPTH {
        return Err(DbError::new(
            "54001",
            "type resolution exceeded its statement depth limit",
        ));
    }
    match statement {
        ParsedStatement::CreateTable {
            columns,
            constraints,
            ..
        } => {
            for column in columns {
                if let Some(default) = &mut column.default {
                    resolve_expr_types(
                        &mut default.expression,
                        parameter_types,
                        catalog,
                        depth + 1,
                        session,
                    )?;
                }
            }
            for constraint in constraints {
                resolve_constraint_types(constraint, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedStatement::AlterTable { operations, .. } => {
            for operation in operations {
                match operation {
                    ParsedAlterTableOperation::AddColumn { column, .. } => {
                        if let Some(default) = &mut column.default {
                            resolve_expr_types(
                                &mut default.expression,
                                parameter_types,
                                catalog,
                                depth + 1,
                                session,
                            )?;
                        }
                    }
                    ParsedAlterTableOperation::SetDefault { default, .. } => {
                        resolve_expr_types(
                            &mut default.expression,
                            parameter_types,
                            catalog,
                            depth + 1,
                            session,
                        )?;
                    }
                    ParsedAlterTableOperation::AddConstraint { constraint } => {
                        resolve_constraint_types(
                            constraint,
                            parameter_types,
                            catalog,
                            depth + 1,
                            session,
                        )?;
                    }
                    _ => {}
                }
            }
        }
        ParsedStatement::CreateDomain {
            default: Some(default),
            ..
        } => {
            resolve_expr_types(
                &mut default.expression,
                parameter_types,
                catalog,
                depth + 1,
                session,
            )?;
        }
        ParsedStatement::AlterDomain {
            operation: ParsedAlterDomainOperation::SetDefault(default),
            ..
        } => {
            resolve_expr_types(
                &mut default.expression,
                parameter_types,
                catalog,
                depth + 1,
                session,
            )?;
        }
        ParsedStatement::CreateView { query, .. }
        | ParsedStatement::Explain { statement: query } => {
            resolve_statement_types(query, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Call { arguments, .. }
        | ParsedStatement::RoutineSelect { arguments, .. } => {
            for argument in arguments {
                resolve_expr_types(argument, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedStatement::PgNotify {
            channel,
            payload,
            alias: _,
        } => {
            resolve_expr_types(channel, parameter_types, catalog, depth + 1, session)?;
            resolve_expr_types(payload, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::ScalarSelect { projection } => {
            resolve_projection_types(projection, parameter_types, catalog, depth + 1, session)?
        }
        ParsedStatement::SequenceValue {
            operation: ParsedSequenceOperation::SetValue { value, .. },
            ..
        } => {
            resolve_expr_types(value, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Insert {
            rows,
            on_conflict,
            returning,
            ..
        } => {
            for expression in rows.iter_mut().flatten() {
                resolve_expr_types(expression, parameter_types, catalog, depth + 1, session)?;
            }
            if let Some(on_conflict) = on_conflict
                && let ParsedConflictAction::DoUpdate {
                    assignments,
                    filter,
                } = &mut on_conflict.action
            {
                for (_, expression) in assignments {
                    resolve_expr_types(expression, parameter_types, catalog, depth + 1, session)?;
                }
                if let Some(filter) = filter {
                    resolve_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
                }
            }
            resolve_projection_types(returning, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Merge(merge) => {
            resolve_expr_types(&mut merge.on, parameter_types, catalog, depth + 1, session)?;
            for clause in &mut merge.clauses {
                if let Some(predicate) = &mut clause.predicate {
                    resolve_expr_types(predicate, parameter_types, catalog, depth + 1, session)?;
                }
                match &mut clause.action {
                    ParsedMergeAction::Update { assignments } => {
                        for (_, expression) in assignments {
                            resolve_expr_types(
                                expression,
                                parameter_types,
                                catalog,
                                depth + 1,
                                session,
                            )?;
                        }
                    }
                    ParsedMergeAction::Insert { values, .. } => {
                        for expression in values {
                            resolve_expr_types(
                                expression,
                                parameter_types,
                                catalog,
                                depth + 1,
                                session,
                            )?;
                        }
                    }
                    ParsedMergeAction::Delete | ParsedMergeAction::DoNothing => {}
                }
            }
            resolve_projection_types(
                &mut merge.returning,
                parameter_types,
                catalog,
                depth + 1,
                session,
            )?;
        }
        ParsedStatement::With { ctes, body, .. } => {
            for cte in ctes {
                resolve_statement_types(
                    &mut cte.query,
                    parameter_types,
                    catalog,
                    depth + 1,
                    session,
                )?;
            }
            resolve_statement_types(body, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::SetOperation {
            left,
            right,
            order_by,
            offset,
            limit,
            ..
        } => {
            resolve_statement_types(left, parameter_types, catalog, depth + 1, session)?;
            resolve_statement_types(right, parameter_types, catalog, depth + 1, session)?;
            resolve_orders_types(order_by, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(offset, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(limit, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Select {
            projection,
            filter,
            order_by,
            offset,
            limit,
            ..
        } => {
            resolve_projection_types(projection, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            resolve_orders_types(order_by, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(offset, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(limit, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::AdvancedSelect {
            joins,
            projection,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
            ..
        } => {
            for join in joins {
                if let ParsedJoinSource::Derived { query, .. } = &mut join.source {
                    resolve_statement_types(query, parameter_types, catalog, depth + 1, session)?;
                }
                resolve_expr_types(&mut join.on, parameter_types, catalog, depth + 1, session)?;
            }
            resolve_projection_types(projection, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            for expression in group_by {
                resolve_expr_types(expression, parameter_types, catalog, depth + 1, session)?;
            }
            resolve_optional_expr_types(having, parameter_types, catalog, depth + 1, session)?;
            resolve_orders_types(order_by, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(offset, parameter_types, catalog, depth + 1, session)?;
            resolve_optional_expr_types(limit, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Update {
            assignments,
            filter,
            returning,
            ..
        } => {
            for (_, expression) in assignments {
                resolve_expr_types(expression, parameter_types, catalog, depth + 1, session)?;
            }
            resolve_optional_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            resolve_projection_types(returning, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedStatement::Delete {
            filter, returning, ..
        } => {
            resolve_optional_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            resolve_projection_types(returning, parameter_types, catalog, depth + 1, session)?;
        }
        _ => {}
    }
    Ok(())
}

fn resolve_constraint_types(
    constraint: &mut ParsedTableConstraint,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    if let ParsedTableConstraint::Check { expression, .. } = constraint {
        resolve_expr_types(expression, parameter_types, catalog, depth, session)?;
    }
    Ok(())
}

fn resolve_projection_types(
    projection: &mut [ParsedProjection],
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    for item in projection {
        if let ParsedProjection::Expression { expr, alias } = item {
            if alias.is_none()
                && let Some(name) = session_function_name(expr)
            {
                *alias = Some(ParsedIdentifier {
                    name: Identifier::unquoted(name),
                    position: expr.position,
                });
            }
            resolve_expr_types(expr, parameter_types, catalog, depth, session)?;
        }
    }
    Ok(())
}

fn session_function_name(expression: &ParsedExpr) -> Option<&'static str> {
    let ParsedExprKind::Function { function, .. } = &expression.kind else {
        return None;
    };
    match function {
        ScalarFunction::Version => Some("version"),
        ScalarFunction::CurrentDatabase => Some("current_database"),
        ScalarFunction::CurrentUser => Some("current_user"),
        ScalarFunction::SessionUser => Some("session_user"),
        ScalarFunction::CurrentSetting => Some("current_setting"),
        _ => None,
    }
}

fn resolve_orders_types(
    order_by: &mut [ParsedOrder],
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    for order in order_by {
        resolve_expr_types(&mut order.expr, parameter_types, catalog, depth, session)?;
    }
    Ok(())
}

fn resolve_optional_expr_types(
    expression: &mut Option<ParsedExpr>,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    if let Some(expression) = expression {
        resolve_expr_types(expression, parameter_types, catalog, depth, session)?;
    }
    Ok(())
}

fn session_function_value(
    function: ScalarFunction,
    arguments: &[ParsedExpr],
    session: Option<SessionBindValues<'_>>,
    position: Option<usize>,
) -> Result<Option<Value>> {
    let Some(session) = session else {
        return match function {
            ScalarFunction::Version
            | ScalarFunction::CurrentDatabase
            | ScalarFunction::CurrentUser
            | ScalarFunction::SessionUser
            | ScalarFunction::CurrentSetting => Err(DbError::new(
                "55000",
                "session scalar function requires database session metadata",
            )
            .with_position_opt(position)),
            _ => Ok(None),
        };
    };
    let value = match function {
        ScalarFunction::Version => Value::Text(session.version.to_owned()),
        ScalarFunction::CurrentDatabase => Value::Text(session.current_database.to_owned()),
        ScalarFunction::CurrentUser => Value::Text(session.current_user.to_owned()),
        ScalarFunction::SessionUser => Value::Text(session.session_user.to_owned()),
        ScalarFunction::CurrentSetting => {
            let Some(ParsedExpr {
                kind: ParsedExprKind::Literal(Value::Text(name)),
                ..
            }) = arguments.first()
            else {
                return unsupported_at("current_setting requires a literal setting name", position);
            };
            let missing_ok = match arguments.get(1) {
                None => false,
                Some(ParsedExpr {
                    kind: ParsedExprKind::Literal(Value::Boolean(value)),
                    ..
                }) => *value,
                Some(_) => {
                    return unsupported_at(
                        "current_setting missing_ok must be a boolean literal",
                        position,
                    );
                }
            };
            let name = name.trim().to_ascii_lowercase();
            match session.settings.get(&name) {
                Some(value) => Value::Text(value.clone()),
                None if missing_ok => Value::Null,
                None => {
                    return Err(DbError::new(
                        "42704",
                        format!("unrecognized configuration parameter {name}"),
                    )
                    .with_position_opt(position));
                }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

fn resolve_expr_types(
    expression: &mut ParsedExpr,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    if depth >= MAX_PARAMETER_SOLVER_DEPTH {
        return Err(DbError::new(
            "54001",
            "type resolution exceeded its expression depth limit",
        ));
    }
    if let ParsedExprKind::Cast {
        data_type,
        declared_type: Some(type_name),
        ..
    } = &mut expression.kind
        && let Some(catalog) = catalog
    {
        let (resolved, _) = resolve_declared_data_type(catalog, data_type, type_name)?;
        *data_type = resolved;
    }
    if let ParsedExprKind::Parameter(index) = expression.kind
        && let Some(data_type) = parameter_types.get(&index)
    {
        expression.kind = ParsedExprKind::ResolvedParameter {
            index,
            data_type: data_type.clone(),
        };
        return Ok(());
    }
    let session_value = match &expression.kind {
        ParsedExprKind::Function {
            function,
            arguments,
        } => session_function_value(*function, arguments, session, expression.position)?,
        _ => None,
    };
    if let Some(value) = session_value {
        expression.kind = ParsedExprKind::Literal(value);
        return Ok(());
    }
    match &mut expression.kind {
        ParsedExprKind::Unary { expr, .. } => {
            resolve_expr_types(expr, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::Cast { expr, .. } => {
            resolve_expr_types(expr, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::Array { elements, .. } => {
            for element in elements {
                resolve_expr_types(element, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedExprKind::Function { arguments, .. } => {
            for argument in arguments {
                resolve_expr_types(argument, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedExprKind::Binary { left, right, .. } => {
            resolve_expr_types(left, parameter_types, catalog, depth + 1, session)?;
            resolve_expr_types(right, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::InList { expr, list, .. } => {
            resolve_expr_types(expr, parameter_types, catalog, depth + 1, session)?;
            for candidate in list {
                resolve_expr_types(candidate, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedExprKind::ScalarSubquery(subquery) | ParsedExprKind::Exists { subquery, .. } => {
            resolve_statement_types(subquery, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::InSubquery { expr, subquery, .. }
        | ParsedExprKind::QuantifiedSubquery {
            left: expr,
            subquery,
            ..
        } => {
            resolve_expr_types(expr, parameter_types, catalog, depth + 1, session)?;
            resolve_statement_types(subquery, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::RowSubquery { left, subquery, .. } => {
            for expression in left {
                resolve_expr_types(expression, parameter_types, catalog, depth + 1, session)?;
            }
            resolve_statement_types(subquery, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::Aggregate {
            argument, filter, ..
        } => {
            if let Some(argument) = argument {
                resolve_expr_types(argument, parameter_types, catalog, depth + 1, session)?;
            }
            if let Some(filter) = filter {
                resolve_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedExprKind::Window { call, spec } => {
            resolve_window_types(call, spec, parameter_types, catalog, depth + 1, session)?;
        }
        ParsedExprKind::NamedWindow { call, .. } => {
            for argument in &mut call.arguments {
                resolve_expr_types(argument, parameter_types, catalog, depth + 1, session)?;
            }
            if let Some(filter) = &mut call.filter {
                resolve_expr_types(filter, parameter_types, catalog, depth + 1, session)?;
            }
        }
        ParsedExprKind::Column(_)
        | ParsedExprKind::Literal(_)
        | ParsedExprKind::Parameter(_)
        | ParsedExprKind::ResolvedParameter { .. }
        | ParsedExprKind::ApplyValue { .. }
        | ParsedExprKind::WindowValue { .. } => {}
    }
    Ok(())
}

fn resolve_window_types(
    call: &mut ParsedWindowCall,
    spec: &mut ParsedWindowSpec,
    parameter_types: &BTreeMap<usize, ScalarType>,
    catalog: Option<&Catalog>,
    depth: usize,
    session: Option<SessionBindValues<'_>>,
) -> Result<()> {
    for argument in &mut call.arguments {
        resolve_expr_types(argument, parameter_types, catalog, depth, session)?;
    }
    if let Some(filter) = &mut call.filter {
        resolve_expr_types(filter, parameter_types, catalog, depth, session)?;
    }
    for expression in &mut spec.partition_by {
        resolve_expr_types(expression, parameter_types, catalog, depth, session)?;
    }
    resolve_orders_types(&mut spec.order_by, parameter_types, catalog, depth, session)?;
    if let Some(frame) = &mut spec.frame {
        for bound in [&mut frame.start_bound, &mut frame.end_bound] {
            if let ParsedWindowFrameBound::Preceding(expression)
            | ParsedWindowFrameBound::Following(expression) = bound
            {
                resolve_expr_types(expression, parameter_types, catalog, depth, session)?;
            }
        }
    }
    Ok(())
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
        ParsedStatement::Reindex { target } => {
            let target = match target {
                ParsedReindexTarget::Index(name) => {
                    let (schema, name, position) = split_table_name(&name)?;
                    let index = catalog.index(&schema, &name).ok_or_else(|| {
                        DbError::new("42704", format!("index {schema}.{name} does not exist"))
                            .with_position_opt(position)
                    })?;
                    BoundReindexTarget::Index(index.id)
                }
                ParsedReindexTarget::Table(name) => {
                    BoundReindexTarget::Table(resolve_table(&name, catalog)?.id)
                }
                ParsedReindexTarget::Schema(name) => {
                    let schema = catalog.schema(&name.name).ok_or_else(|| {
                        DbError::new(
                            UNDEFINED_SCHEMA,
                            format!("schema {} does not exist", name.name),
                        )
                        .with_position_opt(name.position)
                    })?;
                    BoundReindexTarget::Schema(schema.id)
                }
                ParsedReindexTarget::Database(name) => {
                    if catalog.database().name != name.name {
                        return Err(DbError::new(
                            "3D000",
                            format!("database {} does not exist", name.name),
                        )
                        .with_position_opt(name.position));
                    }
                    BoundReindexTarget::Database
                }
            };
            Ok(BoundStatement::Reindex { target })
        }
        ParsedStatement::Listen { channel } => Ok(BoundStatement::Listen {
            channel: channel.name,
        }),
        ParsedStatement::Unlisten { channel } => Ok(BoundStatement::Unlisten {
            channel: channel.map(|channel| channel.name),
        }),
        ParsedStatement::Notify { channel, payload } => Ok(BoundStatement::Notify {
            channel: channel.name,
            payload,
        }),
        ParsedStatement::Do { body } => Ok(BoundStatement::Do { body }),
        ParsedStatement::DiscardAll => Ok(BoundStatement::DiscardAll),
        ParsedStatement::DeallocateAll => Ok(BoundStatement::DeallocateAll),
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
        ParsedStatement::CreateEnumType { name, labels } => {
            let (schema, name, position) = split_table_name(&name)?;
            if catalog.schema(&schema).is_none() {
                return Err(DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {schema} does not exist"),
                )
                .with_position_opt(position));
            }
            if catalog.user_defined_type(&schema, &name).is_some() {
                return Err(
                    DbError::new("42710", format!("type {schema}.{name} already exists"))
                        .with_position_opt(position),
                );
            }
            Ok(BoundStatement::CreateEnumType {
                schema,
                name,
                labels,
            })
        }
        ParsedStatement::CreateDomain {
            name,
            base_type,
            base_declared_type,
            not_null,
            default,
            checks,
        } => {
            let (schema, name, position) = split_table_name(&name)?;
            if catalog.schema(&schema).is_none() {
                return Err(DbError::new(
                    UNDEFINED_SCHEMA,
                    format!("schema {schema} does not exist"),
                )
                .with_position_opt(position));
            }
            if catalog.user_defined_type(&schema, &name).is_some() {
                return Err(
                    DbError::new("42710", format!("type {schema}.{name} already exists"))
                        .with_position_opt(position),
                );
            }
            let (base_type, base_declared_type) = match base_declared_type {
                Some(type_name) => {
                    let definition = resolve_user_defined_type(&type_name, catalog)?;
                    if matches!(
                        definition.definition,
                        ordadb_catalog::UserDefinedTypeKind::Domain { .. }
                    ) {
                        return unsupported(
                            "domains whose base type is another domain are not supported yet",
                        );
                    }
                    let (base_type, type_id) =
                        resolve_declared_data_type(catalog, &base_type, &type_name)?;
                    (base_type, Some(type_id))
                }
                None => (base_type, None),
            };
            let default = default
                .map(|default| {
                    bind_expr(default.expression, None, Some(&base_type))?;
                    Ok(CatalogExpression::new(default.sql))
                })
                .transpose()?;
            let scope =
                TableDefinition::expression_scope(Identifier::unquoted("value"), base_type.clone());
            for constraint in &checks {
                bind_catalog_expression_with_catalog(
                    &constraint.expression,
                    Some(&scope),
                    Some(&ScalarType::Boolean),
                    catalog,
                )?;
            }
            Ok(BoundStatement::CreateDomain {
                schema,
                name,
                base_type,
                base_declared_type,
                not_null,
                default,
                checks,
            })
        }
        ParsedStatement::AlterEnumAddValue {
            name,
            label,
            position,
            if_not_exists,
        } => {
            let definition = resolve_user_defined_type(&name, catalog)?;
            if !matches!(
                definition.definition,
                ordadb_catalog::UserDefinedTypeKind::Enum { .. }
            ) {
                return Err(DbError::new(
                    "42809",
                    "ALTER TYPE ADD VALUE requires an enum type",
                ));
            }
            Ok(BoundStatement::AlterEnumAddValue {
                type_id: definition.id,
                label,
                position,
                if_not_exists,
            })
        }
        ParsedStatement::AlterEnumRenameValue {
            name,
            old_label,
            new_label,
        } => {
            let definition = resolve_user_defined_type(&name, catalog)?;
            if !matches!(
                definition.definition,
                ordadb_catalog::UserDefinedTypeKind::Enum { .. }
            ) {
                return Err(DbError::new(
                    "42809",
                    "ALTER TYPE RENAME VALUE requires an enum type",
                ));
            }
            Ok(BoundStatement::AlterEnumRenameValue {
                type_id: definition.id,
                old_label,
                new_label,
            })
        }
        ParsedStatement::AlterDomain { name, operation } => {
            let definition = resolve_user_defined_type(&name, catalog)?;
            let ordadb_catalog::UserDefinedTypeKind::Domain {
                base_type, checks, ..
            } = &definition.definition
            else {
                return Err(DbError::new("42809", "ALTER DOMAIN requires a domain type"));
            };
            let operation = match operation {
                ParsedAlterDomainOperation::SetDefault(default) => {
                    bind_expr(default.expression, None, Some(base_type))?;
                    BoundAlterDomainOperation::SetDefault(CatalogExpression::new(default.sql))
                }
                ParsedAlterDomainOperation::DropDefault => BoundAlterDomainOperation::DropDefault,
                ParsedAlterDomainOperation::SetNotNull => BoundAlterDomainOperation::SetNotNull,
                ParsedAlterDomainOperation::DropNotNull => BoundAlterDomainOperation::DropNotNull,
                ParsedAlterDomainOperation::AddConstraint(constraint) => {
                    let scope = TableDefinition::expression_scope(
                        Identifier::unquoted("value"),
                        base_type.clone(),
                    );
                    bind_catalog_expression_with_catalog(
                        &constraint.expression,
                        Some(&scope),
                        Some(&ScalarType::Boolean),
                        catalog,
                    )?;
                    BoundAlterDomainOperation::AddConstraint(constraint)
                }
                ParsedAlterDomainOperation::DropConstraint { name, if_exists } => {
                    if !if_exists
                        && !checks
                            .iter()
                            .any(|constraint| constraint.name.as_ref() == Some(&name.name))
                    {
                        return Err(DbError::new(
                            "42704",
                            format!("constraint {} does not exist", name.name),
                        )
                        .with_position_opt(name.position));
                    }
                    BoundAlterDomainOperation::DropConstraint {
                        name: name.name,
                        if_exists,
                    }
                }
            };
            Ok(BoundStatement::AlterDomain {
                type_id: definition.id,
                operation,
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
            return_declared_type,
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
            let arguments = arguments
                .into_iter()
                .map(|argument| {
                    let (data_type, declared_type) = match argument.declared_type {
                        Some(type_name) => {
                            let (data_type, type_id) = resolve_declared_data_type(
                                catalog,
                                &argument.data_type,
                                &type_name,
                            )?;
                            (data_type, Some(type_id))
                        }
                        None => (argument.data_type, None),
                    };
                    Ok(RoutineArgument {
                        name: argument.name,
                        data_type,
                        declared_type,
                        mode: argument.mode,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let (return_type, return_declared_type) = match (return_type, return_declared_type) {
                (Some(data_type), Some(type_name)) => {
                    let (data_type, type_id) =
                        resolve_declared_data_type(catalog, &data_type, &type_name)?;
                    (Some(data_type), Some(type_id))
                }
                (return_type, None) => (return_type, None),
                (None, Some(_)) => {
                    return Err(DbError::internal(
                        "routine return type name exists without a parsed data type",
                    ));
                }
            };
            Ok(BoundStatement::CreateRoutine {
                schema,
                name,
                kind,
                arguments,
                return_type,
                return_declared_type,
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
            let mut matches = Vec::new();
            for routine in catalog
                .routines_named(&schema, &name)
                .iter()
                .filter(|routine| routine.kind == kind)
            {
                let signature_matches = match argument_types.as_ref() {
                    None => true,
                    Some(argument_types) if routine.input_arity() == argument_types.len() => {
                        let mut matches = true;
                        for (argument, expected) in routine.input_arguments().zip(argument_types) {
                            let expected_declared_type = expected
                                .declared_type
                                .as_ref()
                                .map(|name| {
                                    resolve_user_defined_type(name, catalog).map(|ty| ty.id)
                                })
                                .transpose()?;
                            matches &= match expected_declared_type {
                                Some(type_id) => argument.declared_type == Some(type_id),
                                None => {
                                    argument.declared_type.is_none()
                                        && argument.data_type == expected.data_type
                                }
                            };
                        }
                        matches
                    }
                    Some(_) => false,
                };
                if signature_matches {
                    matches.push(routine);
                }
            }
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
                        && routine.input_arity() == arguments.len()
                })
                .collect::<Vec<_>>();
            let mut matches = Vec::new();
            for routine in candidates {
                let input_arguments = routine.input_arguments().collect::<Vec<_>>();
                if let Some((bound, exact_declared_matches)) =
                    bind_routine_candidate(&arguments, &input_arguments, catalog)?
                {
                    matches.push((routine, bound, exact_declared_matches));
                }
            }
            retain_best_routine_matches(&mut matches, |candidate| candidate.2);
            match matches.as_slice() {
                [(routine, arguments, _)] => Ok(BoundStatement::Call {
                    routine_id: routine.id,
                    arguments: arguments.clone(),
                    schema: routine_output_schema(routine),
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
                        && (routine.return_type.is_some()
                            || routine.output_arguments().next().is_some())
                        && routine.input_arity() == arguments.len()
                })
                .collect::<Vec<_>>();
            let mut matches = Vec::new();
            for routine in candidates {
                let input_arguments = routine.input_arguments().collect::<Vec<_>>();
                if let Some((bound, exact_declared_matches)) =
                    bind_routine_candidate(&arguments, &input_arguments, catalog)?
                {
                    matches.push((routine, bound, exact_declared_matches));
                }
            }
            retain_best_routine_matches(&mut matches, |candidate| candidate.2);
            match matches.as_slice() {
                [(routine, arguments, _)] => {
                    let output_arguments = routine.output_arguments().collect::<Vec<_>>();
                    if output_arguments.len() > 1 {
                        return Err(DbError::new(
                            DATATYPE_MISMATCH,
                            "a function with multiple OUT parameters cannot be used as a scalar expression",
                        ));
                    }
                    let return_type = routine
                        .return_type
                        .clone()
                        .or_else(|| {
                            output_arguments
                                .first()
                                .map(|argument| argument.data_type.clone())
                        })
                        .ok_or_else(|| {
                            DbError::internal("selected function lost its output type")
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
        ParsedStatement::PgNotify {
            channel,
            payload,
            alias,
        } => {
            let text = ScalarType::Text;
            Ok(BoundStatement::PgNotify {
                channel: bind_expr(channel, None, Some(&text))?,
                payload: bind_expr(payload, None, Some(&text))?,
                schema: Schema::new(vec![Field::new(
                    alias.map_or_else(|| "pg_notify".to_owned(), |alias| alias.name.to_string()),
                    ScalarType::Text,
                    true,
                )]),
            })
        }
        ParsedStatement::ScalarSelect { projection } => {
            let mut bound_projection = Vec::with_capacity(projection.len());
            let mut fields = Vec::with_capacity(projection.len());
            for projection in projection {
                let ParsedProjection::Expression { expr, alias } = projection else {
                    return unsupported("SELECT without FROM does not support wildcards");
                };
                let field_name = alias
                    .as_ref()
                    .map(|alias| alias.name.as_str().to_owned())
                    .unwrap_or_else(|| projection_name(&expr));
                let expr = bind_expr(expr, None, None)?;
                let field = Field::new(field_name, expr.data_type.clone(), expr.nullable);
                bound_projection.push(BoundProjection {
                    expr,
                    field: field.clone(),
                });
                fields.push(field);
            }
            Ok(BoundStatement::ScalarSelect {
                projection: bound_projection,
                schema: Schema::new(fields),
            })
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
            level,
            events,
            routine,
        } => {
            let target = resolve_trigger_target(&table, catalog)?;
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
                target,
                name: name.name,
                timing,
                level,
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
            let target = resolve_trigger_target(&table, catalog)?;
            let trigger = match target {
                TriggerTarget::Table(table_id) => catalog
                    .table_by_id(table_id)
                    .and_then(|table| table.trigger(&name.name)),
                TriggerTarget::View(view_id) => catalog
                    .view_by_id(view_id)
                    .and_then(|view| view.trigger(&name.name)),
            };
            let Some(trigger) = trigger else {
                if if_exists {
                    return Ok(BoundStatement::NoOp {
                        tag: "DROP TRIGGER".to_owned(),
                    });
                }
                return Err(DbError::new(
                    "42704",
                    format!(
                        "trigger {} for relation {} does not exist",
                        name.name,
                        trigger_target_name(target, catalog)?
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
            on_conflict,
            returning,
        } => bind_insert(
            table,
            columns,
            rows,
            on_conflict,
            returning,
            catalog,
            view_depth,
        ),
        ParsedStatement::Merge(merge) => bind_merge(merge, catalog),
        ParsedStatement::With {
            recursive,
            ctes,
            body,
        } => bind_with_clause(recursive, ctes, *body, catalog, view_depth),
        ParsedStatement::SetOperation {
            left,
            operator,
            all,
            right,
            order_by,
            offset,
            limit,
        } => bind_set_operation(
            *left, operator, all, *right, order_by, offset, limit, catalog, view_depth,
        ),
        ParsedStatement::Select {
            table,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => bind_select(
            SelectInput {
                table_name: table,
                projection,
                filter,
                order_by,
                offset,
                limit,
            },
            catalog,
            view_depth,
        ),
        ParsedStatement::AdvancedSelect {
            table,
            joins,
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
        } => bind_advanced_select(
            AdvancedSelectInput {
                table,
                joins,
                projection,
                distinct,
                filter,
                group_by,
                having,
                order_by,
                offset,
                limit,
            },
            catalog,
            view_depth,
            &[],
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
            returning,
        } => bind_update(table, assignments, filter, returning, catalog, view_depth),
        ParsedStatement::Delete {
            table,
            filter,
            returning,
        } => bind_delete(table, filter, returning, catalog, view_depth),
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
                        let (data_type, declared_type) = convert_column_data_type(data_type, sql)?;
                        ParsedAlterTableOperation::SetDataType {
                            column,
                            data_type,
                            declared_type,
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
            if argument.default_expr.is_some() {
                return unsupported("defaulted routine arguments are not supported yet");
            }
            let (data_type, declared_type) = convert_column_data_type(argument.data_type, sql)?;
            Ok(ParsedRoutineArgument {
                name: argument.name.map(|name| convert_ident(name, sql).name),
                data_type,
                declared_type,
                mode: convert_routine_argument_mode(argument.mode),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let has_output_arguments = arguments
        .iter()
        .any(|argument| argument.mode.produces_output());
    let (return_type, return_declared_type, returns_set) = match function.return_type {
        Some(FunctionReturnType::DataType(data_type)) if is_trigger_type(&data_type) => {
            (None, None, false)
        }
        Some(FunctionReturnType::DataType(data_type)) => {
            let (data_type, declared_type) = convert_column_data_type(data_type, sql)?;
            (Some(data_type), declared_type, false)
        }
        Some(FunctionReturnType::SetOf(data_type)) => {
            let (data_type, declared_type) = convert_column_data_type(data_type, sql)?;
            (Some(data_type), declared_type, true)
        }
        None if has_output_arguments => (None, None, false),
        None => return unsupported("CREATE FUNCTION requires a return type or OUT parameter"),
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
        return_declared_type,
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
                    if argument.default_expr.is_some() {
                        return unsupported("DROP routine signatures cannot contain defaults");
                    }
                    let (data_type, declared_type) =
                        convert_column_data_type(argument.data_type, sql)?;
                    Ok(ParsedRoutineArgument {
                        name: None,
                        data_type,
                        declared_type,
                        mode: convert_routine_argument_mode(argument.mode),
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let argument_types = argument_types.map(|arguments| {
        arguments
            .into_iter()
            .filter(|argument| argument.mode.accepts_input())
            .collect()
    });
    Ok(ParsedStatement::DropRoutine {
        name: convert_object_name(routine.name, sql)?,
        kind,
        argument_types,
        if_exists,
        behavior: convert_drop_behavior(behavior),
    })
}

fn convert_routine_argument_mode(mode: Option<ArgMode>) -> RoutineArgumentMode {
    match mode {
        None | Some(ArgMode::In) => RoutineArgumentMode::In,
        Some(ArgMode::Out) => RoutineArgumentMode::Out,
        Some(ArgMode::InOut) => RoutineArgumentMode::InOut,
        Some(ArgMode::Variadic) => RoutineArgumentMode::Variadic,
    }
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
    let level = match trigger.trigger_object {
        Some(TriggerObjectKind::ForEach(TriggerObject::Row))
        | Some(TriggerObjectKind::For(TriggerObject::Row)) => TriggerLevel::Row,
        Some(TriggerObjectKind::ForEach(TriggerObject::Statement))
        | Some(TriggerObjectKind::For(TriggerObject::Statement))
        | None => TriggerLevel::Statement,
    };
    let timing = match (trigger.period, level) {
        (Some(TriggerPeriod::Before), TriggerLevel::Row) => TriggerTiming::Before,
        (Some(TriggerPeriod::After), TriggerLevel::Row) => TriggerTiming::After,
        (Some(TriggerPeriod::InsteadOf), TriggerLevel::Row) => TriggerTiming::InsteadOf,
        (Some(TriggerPeriod::Before), TriggerLevel::Statement) => TriggerTiming::BeforeStatement,
        (Some(TriggerPeriod::After), TriggerLevel::Statement) => TriggerTiming::AfterStatement,
        (Some(TriggerPeriod::InsteadOf), TriggerLevel::Statement) => {
            return unsupported("INSTEAD OF triggers must use FOR EACH ROW");
        }
        _ => return unsupported("this trigger timing is not supported"),
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
        level,
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
    let with = query.with;
    if !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return unsupported("this SELECT query form is not supported yet");
    }
    let (body, top_limit) = match *query.body {
        SetExpr::Select(select) => {
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
            (SetExpr::Select(Box::new(select)), top_limit)
        }
        body => (body, None),
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
    let (limit, offset) = match query.limit_clause {
        None => (None, None),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) if limit_by.is_empty() => (
            limit.map(|expr| convert_expr(expr, sql)).transpose()?,
            offset
                .map(|offset| convert_expr(offset.value, sql))
                .transpose()?,
        ),
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => (
            Some(convert_expr(limit, sql)?),
            Some(convert_expr(offset, sql)?),
        ),
        Some(_) => {
            return unsupported("LIMIT BY and unrepresentable row-limit forms are not supported");
        }
    };
    let limit = match (top_limit, limit, fetch_limit) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) | (_, Some(_), Some(_)) => {
            return unsupported("a query may specify only one row-limit form");
        }
        (top, limit, fetch) => top.or(limit).or(fetch),
    };

    let statement = match body {
        SetExpr::Select(select) => convert_select(*select, order_by, offset, limit, sql),
        SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } => convert_set_operation(
            *left,
            op,
            set_quantifier,
            *right,
            order_by,
            offset,
            limit,
            sql,
            0,
        ),
        SetExpr::Query(query) if order_by.is_empty() && offset.is_none() && limit.is_none() => {
            convert_select_query(*query, sql)
        }
        SetExpr::Query(_) => unsupported(
            "outer ORDER BY, OFFSET, and LIMIT on a parenthesized query are not supported yet",
        ),
        SetExpr::Values(_) => unsupported("standalone VALUES queries are not supported yet"),
        SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Delete(_)
        | SetExpr::Merge(_)
        | SetExpr::Table(_) => unsupported("this query body is not supported in a set operation"),
    }?;
    if let Some(with) = with {
        convert_with_clause(with, statement, sql)
    } else {
        Ok(statement)
    }
}

fn convert_with_clause(with: SqlWith, body: ParsedStatement, sql: &str) -> Result<ParsedStatement> {
    let recursive = with.recursive;
    let ctes = with
        .cte_tables
        .into_iter()
        .map(|cte| {
            if cte.from.is_some() {
                return unsupported("CTE FROM modifiers are not supported");
            }
            Ok(ParsedCte {
                name: convert_ident(cte.alias.name, sql),
                columns: cte
                    .alias
                    .columns
                    .into_iter()
                    .map(|column| convert_ident(column.name, sql))
                    .collect(),
                query: Box::new(convert_select_query(*cte.query, sql)?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if ctes.is_empty() {
        return Err(DbError::new(SYNTAX_ERROR, "WITH requires at least one CTE"));
    }
    Ok(ParsedStatement::With {
        recursive,
        ctes,
        body: Box::new(body),
    })
}

#[allow(clippy::too_many_arguments)]
fn convert_set_operation(
    left: SetExpr,
    operator: SqlSetOperator,
    quantifier: SqlSetQuantifier,
    right: SetExpr,
    order_by: Vec<ParsedOrder>,
    offset: Option<ParsedExpr>,
    limit: Option<ParsedExpr>,
    sql: &str,
    depth: usize,
) -> Result<ParsedStatement> {
    if depth >= 64 {
        return Err(DbError::new(
            "54001",
            "set operation nesting exceeds the maximum depth of 64",
        ));
    }
    let operator = match operator {
        SqlSetOperator::Union => QuerySetOperator::Union,
        SqlSetOperator::Intersect => QuerySetOperator::Intersect,
        SqlSetOperator::Except => QuerySetOperator::Except,
        SqlSetOperator::Minus => return unsupported("MINUS set operations are not supported"),
    };
    let all = match quantifier {
        SqlSetQuantifier::All => true,
        SqlSetQuantifier::None | SqlSetQuantifier::Distinct => false,
        SqlSetQuantifier::ByName
        | SqlSetQuantifier::AllByName
        | SqlSetQuantifier::DistinctByName => {
            return unsupported("BY NAME set operations are not supported");
        }
    };
    Ok(ParsedStatement::SetOperation {
        left: Box::new(convert_set_operand(left, sql, depth + 1)?),
        operator,
        all,
        right: Box::new(convert_set_operand(right, sql, depth + 1)?),
        order_by,
        offset,
        limit,
    })
}

fn convert_set_operand(expr: SetExpr, sql: &str, depth: usize) -> Result<ParsedStatement> {
    match expr {
        SetExpr::Select(select) => convert_select(*select, Vec::new(), None, None, sql),
        SetExpr::Query(query) => convert_select_query(*query, sql),
        SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } => convert_set_operation(
            *left,
            op,
            set_quantifier,
            *right,
            Vec::new(),
            None,
            None,
            sql,
            depth,
        ),
        SetExpr::Values(_) => {
            unsupported("VALUES operands are not supported in set operations yet")
        }
        SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Delete(_)
        | SetExpr::Merge(_)
        | SetExpr::Table(_) => unsupported("this query body is not supported in a set operation"),
    }
}

fn convert_select(
    select: Select,
    mut order_by: Vec<ParsedOrder>,
    offset: Option<ParsedExpr>,
    limit: Option<ParsedExpr>,
    sql: &str,
) -> Result<ParsedStatement> {
    let distinct = match select.distinct.as_ref() {
        None | Some(SqlDistinct::All) => false,
        Some(SqlDistinct::Distinct) => true,
        Some(SqlDistinct::On(_)) => return unsupported("DISTINCT ON is not supported yet"),
    };
    if !select.optimizer_hints.is_empty()
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
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return unsupported("extended SELECT clauses are not supported yet");
    }
    if select.from.is_empty() {
        if distinct {
            return unsupported("SELECT DISTINCT without FROM is not supported yet");
        }
        if !select.named_window.is_empty() {
            return unsupported("named WINDOW clauses without FROM are not supported yet");
        }
        return convert_routine_select(select, order_by, offset, limit, sql);
    }
    let named_windows = convert_named_windows(select.named_window, sql)?;
    if select.from.len() != 1 {
        return unsupported("SELECT supports exactly one table");
    }

    let mut projection = convert_projection_items(select.projection, sql)?;
    let mut filter = select
        .selection
        .map(|expr| convert_expr(expr, sql))
        .transpose()?;
    let mut group_by = match select.group_by {
        GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => expressions
            .into_iter()
            .map(|expr| convert_expr(expr, sql))
            .collect::<Result<Vec<_>>>()?,
        GroupByExpr::Expressions(_, _) => {
            return unsupported("GROUP BY modifiers are not supported yet");
        }
        GroupByExpr::All(_) => return unsupported("GROUP BY ALL is not supported yet"),
    };
    let mut having = select
        .having
        .map(|expr| convert_expr(expr, sql))
        .transpose()?;
    for projection in &mut projection {
        if let ParsedProjection::Expression { expr, .. } = projection {
            resolve_named_window_expr(expr, &named_windows)?;
        }
    }
    if let Some(filter) = &mut filter {
        resolve_named_window_expr(filter, &named_windows)?;
    }
    for expression in &mut group_by {
        resolve_named_window_expr(expression, &named_windows)?;
    }
    if let Some(having) = &mut having {
        resolve_named_window_expr(having, &named_windows)?;
    }
    for order in &mut order_by {
        resolve_named_window_expr(&mut order.expr, &named_windows)?;
    }
    let from = select
        .from
        .into_iter()
        .next()
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "SELECT requires a table"))?;
    let advanced = distinct
        || matches!(&from.relation, TableFactor::Table { alias: Some(_), .. })
        || !from.joins.is_empty()
        || !group_by.is_empty()
        || having.is_some()
        || projection.iter().any(|projection| {
            projection_has_aggregate(projection)
                || projection_has_subquery(projection)
                || projection_has_window(projection)
        })
        || filter
            .as_ref()
            .is_some_and(|expr| expr_has_subquery(expr) || expr_has_window(expr))
        || order_by.iter().any(|order| expr_has_window(&order.expr));
    if advanced {
        let (table, joins) = convert_select_from(from, sql)?;
        Ok(ParsedStatement::AdvancedSelect {
            table,
            joins,
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
        })
    } else {
        Ok(ParsedStatement::Select {
            table: convert_table_with_joins(from, sql)?,
            projection,
            filter,
            order_by,
            offset,
            limit,
        })
    }
}

fn convert_on_conflict(on: Option<SqlOnInsert>, sql: &str) -> Result<Option<ParsedOnConflict>> {
    let Some(on) = on else {
        return Ok(None);
    };
    let SqlOnInsert::OnConflict(conflict) = on else {
        return unsupported("ON DUPLICATE KEY UPDATE is not PostgreSQL ON CONFLICT");
    };
    let target = conflict
        .conflict_target
        .map(|target| match target {
            SqlConflictTarget::Columns(columns) => columns
                .into_iter()
                .map(|column| convert_single_identifier(column.into(), sql))
                .collect::<Result<Vec<_>>>()
                .map(ParsedConflictTarget::Columns),
            SqlConflictTarget::OnConstraint(name) => {
                convert_object_name(name, sql).map(ParsedConflictTarget::Constraint)
            }
        })
        .transpose()?;
    let action = match conflict.action {
        SqlOnConflictAction::DoNothing => ParsedConflictAction::DoNothing,
        SqlOnConflictAction::DoUpdate(update) => ParsedConflictAction::DoUpdate {
            assignments: convert_assignments(update.assignments, sql)?,
            filter: update
                .selection
                .map(|expr| convert_expr(expr, sql))
                .transpose()?,
        },
    };
    Ok(Some(ParsedOnConflict { target, action }))
}

fn convert_merge(merge: SqlMerge, sql: &str) -> Result<ParsedStatement> {
    let SqlMerge {
        merge_token: _,
        optimizer_hints,
        into,
        table,
        source,
        on,
        clauses,
        output,
    } = merge;
    if !optimizer_hints.is_empty() {
        return unsupported("MERGE optimizer hints are not supported");
    }
    if !into {
        return Err(DbError::new(SYNTAX_ERROR, "MERGE requires INTO"));
    }
    if clauses.is_empty() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "MERGE requires at least one WHEN clause",
        ));
    }
    let returning = match output {
        None => Vec::new(),
        Some(SqlOutputClause::Returning { select_items, .. }) => {
            convert_projection_items(select_items, sql)?
        }
        Some(SqlOutputClause::Output { .. }) => {
            return unsupported("MERGE OUTPUT is not supported");
        }
    };
    let clause_tokens = merge_clause_token_info(&significant_tokens(sql)).ok_or_else(|| {
        DbError::internal("MERGE token audit could not identify the statement clauses")
    })?;
    if clause_tokens.len() != clauses.len() {
        return Err(DbError::internal(
            "MERGE token audit disagrees with the parsed clause count",
        ));
    }
    let clauses = clauses
        .into_iter()
        .enumerate()
        .map(|(clause_index, clause)| {
            let kind = match clause.clause_kind {
                SqlMergeClauseKind::Matched => ParsedMergeClauseKind::Matched,
                SqlMergeClauseKind::NotMatched | SqlMergeClauseKind::NotMatchedByTarget => {
                    ParsedMergeClauseKind::NotMatchedByTarget
                }
                SqlMergeClauseKind::NotMatchedBySource => ParsedMergeClauseKind::NotMatchedBySource,
            };
            let action = if clause_tokens[clause_index].do_nothing.is_some() {
                ParsedMergeAction::DoNothing
            } else {
                match clause.action {
                    SqlMergeAction::Update(update) => {
                        if kind == ParsedMergeClauseKind::NotMatchedByTarget {
                            return Err(DbError::new(
                                SYNTAX_ERROR,
                                "MERGE UPDATE requires WHEN MATCHED or WHEN NOT MATCHED BY SOURCE",
                            ));
                        }
                        if update.update_predicate.is_some() || update.delete_predicate.is_some() {
                            return unsupported("Oracle MERGE UPDATE predicates are not supported");
                        }
                        ParsedMergeAction::Update {
                            assignments: convert_assignments(update.assignments, sql)?,
                        }
                    }
                    SqlMergeAction::Delete { .. } => {
                        if kind == ParsedMergeClauseKind::NotMatchedByTarget {
                            return Err(DbError::new(
                                SYNTAX_ERROR,
                                "MERGE DELETE requires WHEN MATCHED or WHEN NOT MATCHED BY SOURCE",
                            ));
                        }
                        ParsedMergeAction::Delete
                    }
                    SqlMergeAction::Insert(mut insert) => {
                        if kind != ParsedMergeClauseKind::NotMatchedByTarget {
                            return Err(DbError::new(
                                SYNTAX_ERROR,
                                "MERGE INSERT requires WHEN NOT MATCHED",
                            ));
                        }
                        if insert.insert_predicate.is_some() {
                            return unsupported("Oracle MERGE INSERT predicates are not supported");
                        }
                        let columns = insert
                            .columns
                            .into_iter()
                            .map(|column| convert_single_identifier(column, sql))
                            .collect::<Result<Vec<_>>>()?;
                        let values = match insert.kind {
                            SqlMergeInsertKind::Values(ref mut values)
                                if !values.explicit_row
                                    && !values.value_keyword
                                    && values.rows.len() == 1 =>
                            {
                                values
                                    .rows
                                    .pop()
                                    .ok_or_else(|| {
                                        DbError::new(SYNTAX_ERROR, "MERGE INSERT VALUES is empty")
                                    })?
                                    .content
                                    .into_iter()
                                    .map(|expr| convert_expr(expr, sql))
                                    .collect::<Result<Vec<_>>>()?
                            }
                            SqlMergeInsertKind::Values(_) => {
                                return unsupported(
                                    "MERGE INSERT requires exactly one standard VALUES row",
                                );
                            }
                            SqlMergeInsertKind::Row => {
                                return unsupported("MERGE INSERT ROW is not supported");
                            }
                        };
                        ParsedMergeAction::Insert { columns, values }
                    }
                }
            };
            Ok(ParsedMergeClause {
                kind,
                predicate: clause
                    .predicate
                    .map(|predicate| convert_expr(predicate, sql))
                    .transpose()?,
                action,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ParsedStatement::Merge(ParsedMerge {
        target: convert_select_table(table, sql)?,
        source: convert_select_table(source, sql)?,
        on: convert_expr(*on, sql)?,
        clauses,
        returning,
    }))
}

fn convert_assignments(
    assignments: Vec<SqlAssignment>,
    sql: &str,
) -> Result<Vec<(ParsedIdentifier, ParsedExpr)>> {
    assignments
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
        .collect()
}

fn convert_projection_items(
    projection: Vec<SelectItem>,
    sql: &str,
) -> Result<Vec<ParsedProjection>> {
    projection
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
        .collect()
}

fn convert_routine_select(
    select: Select,
    order_by: Vec<ParsedOrder>,
    offset: Option<ParsedExpr>,
    limit: Option<ParsedExpr>,
    sql: &str,
) -> Result<ParsedStatement> {
    if select.selection.is_some()
        || select.having.is_some()
        || !order_by.is_empty()
        || offset.is_some()
        || limit.is_some()
        || !matches!(
            select.group_by,
            GroupByExpr::Expressions(ref expressions, ref modifiers)
                if expressions.is_empty() && modifiers.is_empty()
        )
    {
        return unsupported("scalar routine SELECT does not support query clauses");
    }
    if let [projection] = select.projection.as_slice() {
        let (expression, alias) = match projection {
            SelectItem::UnnamedExpr(expression) => (expression.clone(), None),
            SelectItem::ExprWithAlias { expr, alias } => {
                (expr.clone(), Some(convert_ident(alias.clone(), sql)))
            }
            _ => return unsupported("scalar SELECT does not support wildcards"),
        };
        if let SqlExpr::Function(function) = &expression {
            let function_name = function.name.to_string().to_ascii_lowercase();
            if matches!(function_name.as_str(), "pg_notify" | "pg_catalog.pg_notify") {
                let (_, mut arguments) = convert_routine_invocation(function.clone(), sql)?;
                if arguments.len() != 2 {
                    return Err(DbError::new(
                        "42883",
                        format!(
                            "function pg_notify does not accept {} arguments",
                            arguments.len()
                        ),
                    ));
                }
                let payload = arguments
                    .pop()
                    .ok_or_else(|| DbError::internal("pg_notify payload argument is missing"))?;
                let channel = arguments
                    .pop()
                    .ok_or_else(|| DbError::internal("pg_notify channel argument is missing"))?;
                return Ok(ParsedStatement::PgNotify {
                    channel,
                    payload,
                    alias,
                });
            }
            if scalar_function_from_name(&function_name).is_none() {
                let (name, arguments) = convert_routine_invocation(function.clone(), sql)?;
                if let Some(operation_name) = sequence_operation_name(&name) {
                    return convert_sequence_value_select(operation_name, arguments, alias);
                }
                return Ok(ParsedStatement::RoutineSelect {
                    name,
                    arguments,
                    alias,
                });
            }
        }
    }
    let projection = convert_projection_items(select.projection, sql)?;
    if projection
        .iter()
        .any(|item| matches!(item, ParsedProjection::Wildcard))
    {
        return unsupported("SELECT without FROM does not support wildcards");
    }
    Ok(ParsedStatement::ScalarSelect { projection })
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
                source: convert_join_source(join.relation, sql)?,
                kind,
                on: convert_expr(on, sql)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((first, joins))
}

fn convert_join_source(source: TableFactor, sql: &str) -> Result<ParsedJoinSource> {
    match source {
        source @ TableFactor::Table { .. } => {
            convert_select_table(source, sql).map(ParsedJoinSource::Table)
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if sample.is_some() {
                return unsupported("TABLESAMPLE on derived tables is not supported yet");
            }
            let alias = alias.ok_or_else(|| {
                DbError::new(
                    SYNTAX_ERROR,
                    "derived tables require an explicit relation alias",
                )
            })?;
            if alias.at.is_some() {
                return unsupported("AT aliases on derived tables are not supported");
            }
            let columns = alias
                .columns
                .into_iter()
                .map(|column| {
                    if column.data_type.is_some() {
                        return unsupported(
                            "typed column aliases on derived tables are not supported",
                        );
                    }
                    Ok(convert_ident(column.name, sql))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ParsedJoinSource::Derived {
                lateral,
                query: Box::new(convert_select_query(*subquery, sql)?),
                alias: convert_ident(alias.name, sql),
                columns,
            })
        }
        _ => unsupported("table functions and this join source are not supported yet"),
    }
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
    if !alias.columns.is_empty() || alias.at.is_some() {
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

fn projection_has_subquery(projection: &ParsedProjection) -> bool {
    match projection {
        ParsedProjection::Wildcard => false,
        ParsedProjection::Expression { expr, .. } => expr_has_subquery(expr),
    }
}

fn projection_has_window(projection: &ParsedProjection) -> bool {
    match projection {
        ParsedProjection::Wildcard => false,
        ParsedProjection::Expression { expr, .. } => expr_has_window(expr),
    }
}

fn expr_has_window(expr: &ParsedExpr) -> bool {
    match &expr.kind {
        ParsedExprKind::Window { .. }
        | ParsedExprKind::NamedWindow { .. }
        | ParsedExprKind::WindowValue { .. } => true,
        ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
            expr_has_window(expr)
        }
        ParsedExprKind::Array { elements, .. } => elements.iter().any(expr_has_window),
        ParsedExprKind::Function { arguments, .. } => arguments.iter().any(expr_has_window),
        ParsedExprKind::Binary { left, right, .. } => {
            expr_has_window(left) || expr_has_window(right)
        }
        ParsedExprKind::InList { expr, list, .. } => {
            expr_has_window(expr) || list.iter().any(expr_has_window)
        }
        ParsedExprKind::InSubquery { expr, .. } => expr_has_window(expr),
        ParsedExprKind::QuantifiedSubquery { left, .. } => expr_has_window(left),
        ParsedExprKind::RowSubquery { left, .. } => left.iter().any(expr_has_window),
        ParsedExprKind::Aggregate {
            argument, filter, ..
        } => {
            argument.as_deref().is_some_and(expr_has_window)
                || filter.as_deref().is_some_and(expr_has_window)
        }
        ParsedExprKind::ScalarSubquery(_)
        | ParsedExprKind::Exists { .. }
        | ParsedExprKind::Column(_)
        | ParsedExprKind::Literal(_)
        | ParsedExprKind::Parameter(_)
        | ParsedExprKind::ResolvedParameter { .. }
        | ParsedExprKind::ApplyValue { .. } => false,
    }
}

fn expr_has_subquery(expr: &ParsedExpr) -> bool {
    match &expr.kind {
        ParsedExprKind::ScalarSubquery(_)
        | ParsedExprKind::Exists { .. }
        | ParsedExprKind::InSubquery { .. }
        | ParsedExprKind::QuantifiedSubquery { .. }
        | ParsedExprKind::RowSubquery { .. } => true,
        ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
            expr_has_subquery(expr)
        }
        ParsedExprKind::Array { elements, .. } => elements.iter().any(expr_has_subquery),
        ParsedExprKind::Function { arguments, .. } => arguments.iter().any(expr_has_subquery),
        ParsedExprKind::Binary { left, right, .. } => {
            expr_has_subquery(left) || expr_has_subquery(right)
        }
        ParsedExprKind::InList { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        ParsedExprKind::Aggregate {
            argument, filter, ..
        } => {
            argument.as_deref().is_some_and(expr_has_subquery)
                || filter.as_deref().is_some_and(expr_has_subquery)
        }
        ParsedExprKind::Window { call, spec } => {
            call.arguments.iter().any(expr_has_subquery)
                || call.filter.as_deref().is_some_and(expr_has_subquery)
                || spec.partition_by.iter().any(expr_has_subquery)
                || spec
                    .order_by
                    .iter()
                    .any(|order| expr_has_subquery(&order.expr))
        }
        ParsedExprKind::NamedWindow { call, .. } => {
            call.arguments.iter().any(expr_has_subquery)
                || call.filter.as_deref().is_some_and(expr_has_subquery)
        }
        ParsedExprKind::Column(_)
        | ParsedExprKind::Literal(_)
        | ParsedExprKind::Parameter(_)
        | ParsedExprKind::ResolvedParameter { .. }
        | ParsedExprKind::ApplyValue { .. }
        | ParsedExprKind::WindowValue { .. } => false,
    }
}

fn expr_has_aggregate(expr: &ParsedExpr) -> bool {
    match &expr.kind {
        ParsedExprKind::Aggregate { .. } => true,
        ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
            expr_has_aggregate(expr)
        }
        ParsedExprKind::Array { elements, .. } => elements.iter().any(expr_has_aggregate),
        ParsedExprKind::Function { arguments, .. } => arguments.iter().any(expr_has_aggregate),
        ParsedExprKind::Binary { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        ParsedExprKind::InList { expr, list, .. } => {
            expr_has_aggregate(expr) || list.iter().any(expr_has_aggregate)
        }
        ParsedExprKind::InSubquery { expr, .. } => expr_has_aggregate(expr),
        ParsedExprKind::QuantifiedSubquery { left, .. } => expr_has_aggregate(left),
        ParsedExprKind::RowSubquery { left, .. } => left.iter().any(expr_has_aggregate),
        ParsedExprKind::ScalarSubquery(_) | ParsedExprKind::Exists { .. } => false,
        ParsedExprKind::Column(_)
        | ParsedExprKind::Literal(_)
        | ParsedExprKind::Parameter(_)
        | ParsedExprKind::ResolvedParameter { .. }
        | ParsedExprKind::ApplyValue { .. }
        | ParsedExprKind::WindowValue { .. } => false,
        ParsedExprKind::NamedWindow { call, .. } => {
            call.arguments.iter().any(expr_has_aggregate)
                || call.filter.as_deref().is_some_and(expr_has_aggregate)
        }
        ParsedExprKind::Window { call, spec } => {
            call.arguments.iter().any(expr_has_aggregate)
                || call.filter.as_deref().is_some_and(expr_has_aggregate)
                || spec.partition_by.iter().any(expr_has_aggregate)
                || spec
                    .order_by
                    .iter()
                    .any(|order| expr_has_aggregate(&order.expr))
        }
    }
}

fn bind_routine_candidate(
    arguments: &[ParsedExpr],
    expected: &[&RoutineArgument],
    catalog: &Catalog,
) -> Result<Option<(Vec<BoundExpr>, usize)>> {
    let mut bound = Vec::with_capacity(arguments.len());
    let mut exact_declared_matches = 0_usize;
    for (argument, expected) in arguments.iter().zip(expected) {
        let declared_type = match &argument.kind {
            ParsedExprKind::Cast {
                declared_type: Some(name),
                ..
            } => Some(resolve_user_defined_type(name, catalog)?.id),
            _ => None,
        };
        if declared_type.is_some() && declared_type == expected.declared_type {
            exact_declared_matches = exact_declared_matches.saturating_add(1);
        }
        let Ok(argument) = bind_expr(argument.clone(), None, Some(&expected.data_type)) else {
            return Ok(None);
        };
        bound.push(argument);
    }
    Ok(Some((bound, exact_declared_matches)))
}

fn routine_output_schema(routine: &ordadb_catalog::RoutineDefinition) -> Schema {
    Schema::new(
        routine
            .output_arguments()
            .enumerate()
            .map(|(index, argument)| {
                let name = argument.name.as_ref().map_or_else(
                    || format!("column{}", index + 1),
                    |name| name.as_str().to_owned(),
                );
                Field::new(name, argument.data_type.clone(), true)
            })
            .collect(),
    )
}

fn retain_best_routine_matches<T>(matches: &mut Vec<T>, score: impl Fn(&T) -> usize) {
    let Some(best) = matches.iter().map(&score).max() else {
        return;
    };
    matches.retain(|candidate| score(candidate) == best);
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
        SqlExpr::Interval(interval) => {
            if interval.leading_field.is_some()
                || interval.leading_precision.is_some()
                || interval.last_field.is_some()
                || interval.fractional_seconds_precision.is_some()
            {
                return unsupported_at(
                    "INTERVAL field and precision qualifiers are not supported yet",
                    position,
                );
            }
            let value = interval_literal_text(*interval.value, position)?;
            ParsedExprKind::Literal(Value::Interval(
                PgInterval::from_str(&value).map_err(|error| error.with_position_opt(position))?,
            ))
        }
        SqlExpr::Cast {
            kind,
            expr,
            data_type,
            array,
            format,
        } => {
            if !matches!(kind, CastKind::Cast | CastKind::DoubleColon) {
                return unsupported_at("TRY_CAST and SAFE_CAST are not supported", position);
            }
            if array || format.is_some() {
                return unsupported_at("this CAST option is not supported", position);
            }
            let (data_type, declared_type) = convert_column_data_type(data_type, sql)?;
            ParsedExprKind::Cast {
                expr: Box::new(convert_expr(*expr, sql)?),
                data_type,
                declared_type,
            }
        }
        SqlExpr::Array(array) => convert_array_expression(array, sql, position)?,
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            special: _,
            shorthand: _,
        } => {
            let from = substring_from.ok_or_else(|| {
                DbError::new(SYNTAX_ERROR, "SUBSTRING requires a start position")
                    .with_position_opt(position)
            })?;
            let mut arguments = vec![convert_expr(*expr, sql)?, convert_expr(*from, sql)?];
            if let Some(length) = substring_for {
                arguments.push(convert_expr(*length, sql)?);
            }
            ParsedExprKind::Function {
                function: ScalarFunction::Substring,
                arguments,
            }
        }
        SqlExpr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_characters.is_some() {
                return unsupported_at(
                    "comma-separated TRIM characters are not supported",
                    position,
                );
            }
            let function = match trim_where.unwrap_or(TrimWhereField::Both) {
                TrimWhereField::Both => ScalarFunction::Btrim,
                TrimWhereField::Leading => ScalarFunction::Ltrim,
                TrimWhereField::Trailing => ScalarFunction::Rtrim,
            };
            let mut arguments = vec![convert_expr(*expr, sql)?];
            if let Some(trim_what) = trim_what {
                arguments.push(convert_expr(*trim_what, sql)?);
            }
            ParsedExprKind::Function {
                function,
                arguments,
            }
        }
        SqlExpr::Position { expr, r#in } => ParsedExprKind::Function {
            function: ScalarFunction::Strpos,
            arguments: vec![convert_expr(*r#in, sql)?, convert_expr(*expr, sql)?],
        },
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
            let op = convert_binary_operator(op, position)?;
            match (*left, *right) {
                (SqlExpr::Tuple(left), SqlExpr::Tuple(right)) => {
                    return convert_row_comparison(left, op, right, sql, position);
                }
                (SqlExpr::Tuple(left), SqlExpr::Subquery(subquery)) => {
                    ParsedExprKind::RowSubquery {
                        left: convert_row_items(left, sql, position)?,
                        op: row_comparison_operator(op, position)?,
                        quantifier: None,
                        negated: false,
                        subquery: Box::new(convert_select_query(*subquery, sql)?),
                    }
                }
                (SqlExpr::Subquery(subquery), SqlExpr::Tuple(right)) => {
                    ParsedExprKind::RowSubquery {
                        left: convert_row_items(right, sql, position)?,
                        op: row_comparison_operator(op, position)?,
                        quantifier: None,
                        negated: false,
                        subquery: Box::new(convert_select_query(*subquery, sql)?),
                    }
                }
                (left, right) => ParsedExprKind::Binary {
                    left: Box::new(convert_expr(left, sql)?),
                    op,
                    right: Box::new(convert_expr(right, sql)?),
                },
            }
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            if list.is_empty() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "IN list must contain at least one expression",
                )
                .with_position_opt(position));
            }
            match *expr {
                SqlExpr::Tuple(left) => {
                    return convert_row_in_list(left, list, negated, sql, position);
                }
                expr => ParsedExprKind::InList {
                    expr: Box::new(convert_expr(expr, sql)?),
                    list: list
                        .into_iter()
                        .map(|expr| convert_expr(expr, sql))
                        .collect::<Result<Vec<_>>>()?,
                    negated,
                },
            }
        }
        SqlExpr::Subquery(subquery) => {
            ParsedExprKind::ScalarSubquery(Box::new(convert_select_query(*subquery, sql)?))
        }
        SqlExpr::Exists { subquery, negated } => ParsedExprKind::Exists {
            subquery: Box::new(convert_select_query(*subquery, sql)?),
            negated,
        },
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => match *expr {
            SqlExpr::Tuple(left) => ParsedExprKind::RowSubquery {
                left: convert_row_items(left, sql, position)?,
                op: BinaryOperator::Eq,
                quantifier: Some(SubqueryQuantifier::Any),
                negated,
                subquery: Box::new(convert_select_query(*subquery, sql)?),
            },
            expr => ParsedExprKind::InSubquery {
                expr: Box::new(convert_expr(expr, sql)?),
                subquery: Box::new(convert_select_query(*subquery, sql)?),
                negated,
            },
        },
        SqlExpr::AnyOp {
            left,
            compare_op,
            right,
            is_some: _,
        } => {
            let SqlExpr::Subquery(subquery) = *right else {
                return unsupported_at(
                    "ANY over arrays or non-subquery expressions is not supported yet",
                    position,
                );
            };
            let op = convert_comparison_operator(compare_op, position)?;
            match *left {
                SqlExpr::Tuple(left) => ParsedExprKind::RowSubquery {
                    left: convert_row_items(left, sql, position)?,
                    op: row_comparison_operator(op, position)?,
                    quantifier: Some(SubqueryQuantifier::Any),
                    negated: false,
                    subquery: Box::new(convert_select_query(*subquery, sql)?),
                },
                left => ParsedExprKind::QuantifiedSubquery {
                    left: Box::new(convert_expr(left, sql)?),
                    op,
                    quantifier: SubqueryQuantifier::Any,
                    subquery: Box::new(convert_select_query(*subquery, sql)?),
                },
            }
        }
        SqlExpr::AllOp {
            left,
            compare_op,
            right,
        } => {
            let SqlExpr::Subquery(subquery) = *right else {
                return unsupported_at(
                    "ALL over arrays or non-subquery expressions is not supported yet",
                    position,
                );
            };
            let op = convert_comparison_operator(compare_op, position)?;
            match *left {
                SqlExpr::Tuple(left) => ParsedExprKind::RowSubquery {
                    left: convert_row_items(left, sql, position)?,
                    op: row_comparison_operator(op, position)?,
                    quantifier: Some(SubqueryQuantifier::All),
                    negated: false,
                    subquery: Box::new(convert_select_query(*subquery, sql)?),
                },
                left => ParsedExprKind::QuantifiedSubquery {
                    left: Box::new(convert_expr(left, sql)?),
                    op,
                    quantifier: SubqueryQuantifier::All,
                    subquery: Box::new(convert_select_query(*subquery, sql)?),
                },
            }
        }
        SqlExpr::Tuple(_) => {
            return unsupported_at(
                "row values are supported only in comparisons and IN predicates",
                position,
            );
        }
        SqlExpr::Function(function) => {
            if function.over.is_some() {
                convert_window_function(function, sql, position)?
            } else {
                if function.uses_odbc_syntax
                    || !matches!(function.parameters, FunctionArguments::None)
                    || function.null_treatment.is_some()
                    || !function.within_group.is_empty()
                {
                    return unsupported_at("aggregate options are not supported yet", position);
                }
                let filter = function
                    .filter
                    .map(|filter| convert_expr(*filter, sql).map(Box::new))
                    .transpose()?;
                let function_name = function.name.to_string().to_ascii_lowercase();
                if let Some(scalar_function) = scalar_function_from_name(&function_name) {
                    if filter.is_some() {
                        return unsupported_at(
                            "FILTER is supported only for aggregate functions",
                            position,
                        );
                    }
                    let arguments =
                        convert_scalar_function_arguments(function.args, sql, position)?;
                    validate_scalar_function_arity(scalar_function, arguments.len(), position)?;
                    ParsedExprKind::Function {
                        function: scalar_function,
                        arguments,
                    }
                } else {
                    let aggregate_function = match function_name.as_str() {
                        "count" => AggregateFunction::Count,
                        "sum" => AggregateFunction::Sum,
                        "avg" => AggregateFunction::Avg,
                        "min" => AggregateFunction::Min,
                        "max" => AggregateFunction::Max,
                        _ => {
                            return unsupported_at(
                                "this SQL function is not supported yet",
                                position,
                            );
                        }
                    };
                    let FunctionArguments::List(arguments) = function.args else {
                        return unsupported_at(
                            "aggregate arguments must use parentheses",
                            position,
                        );
                    };
                    if !arguments.clauses.is_empty() {
                        return unsupported_at(
                            "ordered aggregate arguments are not supported yet",
                            position,
                        );
                    }
                    let distinct = matches!(
                        arguments.duplicate_treatment,
                        Some(DuplicateTreatment::Distinct)
                    );
                    let argument = match arguments.args.as_slice() {
                        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                            if aggregate_function == AggregateFunction::Count && !distinct =>
                        {
                            None
                        }
                        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] => {
                            return Err(DbError::new(
                                SYNTAX_ERROR,
                                "DISTINCT aggregate requires an expression",
                            )
                            .with_position_opt(position));
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
                        distinct,
                        filter,
                    }
                }
            }
        }
        _ => return unsupported_at("this SQL expression is not supported yet", position),
    };
    Ok(ParsedExpr { kind, position })
}

fn scalar_function_from_name(name: &str) -> Option<ScalarFunction> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    match name {
        "version" => Some(ScalarFunction::Version),
        "current_database" | "current_catalog" => Some(ScalarFunction::CurrentDatabase),
        "current_user" | "user" => Some(ScalarFunction::CurrentUser),
        "session_user" => Some(ScalarFunction::SessionUser),
        "current_setting" => Some(ScalarFunction::CurrentSetting),
        "lower" => Some(ScalarFunction::Lower),
        "upper" => Some(ScalarFunction::Upper),
        "length" | "char_length" | "character_length" => Some(ScalarFunction::CharacterLength),
        "octet_length" => Some(ScalarFunction::OctetLength),
        "abs" => Some(ScalarFunction::Abs),
        "coalesce" => Some(ScalarFunction::Coalesce),
        "nullif" => Some(ScalarFunction::NullIf),
        "concat" => Some(ScalarFunction::Concat),
        "substring" | "substr" => Some(ScalarFunction::Substring),
        "btrim" | "trim" => Some(ScalarFunction::Btrim),
        "ltrim" => Some(ScalarFunction::Ltrim),
        "rtrim" => Some(ScalarFunction::Rtrim),
        "replace" => Some(ScalarFunction::Replace),
        "strpos" => Some(ScalarFunction::Strpos),
        "greatest" => Some(ScalarFunction::Greatest),
        "least" => Some(ScalarFunction::Least),
        "jsonb_typeof" => Some(ScalarFunction::JsonbTypeof),
        "array_length" => Some(ScalarFunction::ArrayLength),
        "cardinality" => Some(ScalarFunction::Cardinality),
        _ => None,
    }
}

fn convert_scalar_function_arguments(
    arguments: FunctionArguments,
    sql: &str,
    position: Option<usize>,
) -> Result<Vec<ParsedExpr>> {
    let arguments = match arguments {
        FunctionArguments::None => return Ok(Vec::new()),
        FunctionArguments::List(arguments) => arguments,
        _ => {
            return unsupported_at("scalar function arguments must use parentheses", position);
        }
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return unsupported_at(
            "this scalar function argument option is not supported",
            position,
        );
    }
    arguments
        .args
        .into_iter()
        .map(|argument| match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => {
                convert_expr(expression, sql)
            }
            _ => unsupported_at(
                "scalar functions require positional expression arguments",
                position,
            ),
        })
        .collect()
}

fn validate_scalar_function_arity(
    function: ScalarFunction,
    count: usize,
    position: Option<usize>,
) -> Result<()> {
    let valid = match function {
        ScalarFunction::Version
        | ScalarFunction::CurrentDatabase
        | ScalarFunction::CurrentUser
        | ScalarFunction::SessionUser => count == 0,
        ScalarFunction::CurrentSetting => matches!(count, 1 | 2),
        ScalarFunction::Lower
        | ScalarFunction::Upper
        | ScalarFunction::CharacterLength
        | ScalarFunction::OctetLength
        | ScalarFunction::Abs
        | ScalarFunction::JsonbTypeof
        | ScalarFunction::Cardinality => count == 1,
        ScalarFunction::NullIf | ScalarFunction::ArrayLength | ScalarFunction::Strpos => count == 2,
        ScalarFunction::Btrim | ScalarFunction::Ltrim | ScalarFunction::Rtrim => {
            matches!(count, 1 | 2)
        }
        ScalarFunction::Replace => count == 3,
        ScalarFunction::Substring => matches!(count, 2 | 3),
        ScalarFunction::Coalesce
        | ScalarFunction::Concat
        | ScalarFunction::Greatest
        | ScalarFunction::Least => count > 0,
    };
    if valid {
        Ok(())
    } else {
        Err(DbError::new(
            "42883",
            format!("function {function:?} does not accept {count} arguments"),
        )
        .with_position_opt(position))
    }
}

fn interval_literal_text(expression: SqlExpr, position: Option<usize>) -> Result<String> {
    let SqlExpr::Value(value) = expression else {
        return unsupported_at("INTERVAL requires a string literal", position);
    };
    match value.value {
        SqlValue::SingleQuotedString(value)
        | SqlValue::EscapedStringLiteral(value)
        | SqlValue::UnicodeStringLiteral(value)
        | SqlValue::NationalStringLiteral(value) => Ok(value),
        _ => unsupported_at("INTERVAL requires a string literal", position),
    }
}

fn convert_array_expression(
    array: SqlArray,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedExprKind> {
    if !array.named {
        return unsupported_at("array constructors must use ARRAY[...]", position);
    }
    let (elements, dimensions) = flatten_array_elements(array.elem, sql, position, 0)?;
    Ok(ParsedExprKind::Array {
        elements,
        dimensions,
    })
}

fn flatten_array_elements(
    expressions: Vec<SqlExpr>,
    sql: &str,
    position: Option<usize>,
    depth: usize,
) -> Result<(Vec<ParsedExpr>, Vec<ArrayDimension>)> {
    const MAX_ARRAY_DIMENSIONS: usize = 6;
    const MAX_ARRAY_ELEMENTS: usize = 1_000_000;
    if depth >= MAX_ARRAY_DIMENSIONS {
        return Err(DbError::new(
            "54000",
            format!("array exceeds the maximum of {MAX_ARRAY_DIMENSIONS} dimensions"),
        )
        .with_position_opt(position));
    }
    if expressions.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let nested = matches!(expressions.first(), Some(SqlExpr::Array(_)));
    if expressions
        .iter()
        .any(|expression| matches!(expression, SqlExpr::Array(_)) != nested)
    {
        return Err(DbError::new(
            "2202E",
            "multidimensional arrays must have matching dimensions",
        )
        .with_position_opt(position));
    }
    let length = u32::try_from(expressions.len()).map_err(|_| {
        DbError::new("54000", "array dimension is too large").with_position_opt(position)
    })?;
    if !nested {
        if expressions.len() > MAX_ARRAY_ELEMENTS {
            return Err(DbError::new(
                "54000",
                format!("array exceeds the maximum of {MAX_ARRAY_ELEMENTS} elements"),
            )
            .with_position_opt(position));
        }
        return Ok((
            expressions
                .into_iter()
                .map(|expression| convert_expr(expression, sql))
                .collect::<Result<Vec<_>>>()?,
            vec![ArrayDimension::new(length, 1)],
        ));
    }

    let mut flattened = Vec::new();
    let mut child_dimensions: Option<Vec<ArrayDimension>> = None;
    for expression in expressions {
        let SqlExpr::Array(child) = expression else {
            return Err(DbError::internal(
                "validated nested array lost its child array",
            ));
        };
        let (mut child_elements, dimensions) =
            flatten_array_elements(child.elem, sql, position, depth + 1)?;
        if child_dimensions
            .as_ref()
            .is_some_and(|expected| expected != &dimensions)
        {
            return Err(DbError::new(
                "2202E",
                "multidimensional arrays must have matching dimensions",
            )
            .with_position_opt(position));
        }
        child_dimensions.get_or_insert(dimensions);
        flattened.append(&mut child_elements);
        if flattened.len() > MAX_ARRAY_ELEMENTS {
            return Err(DbError::new(
                "54000",
                format!("array exceeds the maximum of {MAX_ARRAY_ELEMENTS} elements"),
            )
            .with_position_opt(position));
        }
    }
    let mut dimensions = vec![ArrayDimension::new(length, 1)];
    dimensions.extend(child_dimensions.unwrap_or_default());
    Ok((flattened, dimensions))
}

fn convert_row_items(
    expressions: Vec<SqlExpr>,
    sql: &str,
    position: Option<usize>,
) -> Result<Vec<ParsedExpr>> {
    if expressions.is_empty() {
        return Err(
            DbError::new(SYNTAX_ERROR, "row value must not be empty").with_position_opt(position)
        );
    }
    expressions
        .into_iter()
        .map(|expression| convert_expr(expression, sql))
        .collect()
}

fn row_comparison_operator(
    operator: BinaryOperator,
    position: Option<usize>,
) -> Result<BinaryOperator> {
    match operator {
        BinaryOperator::Eq | BinaryOperator::NotEq => Ok(operator),
        _ => unsupported_at("ordered row comparisons are not supported yet", position),
    }
}

fn convert_row_comparison(
    left: Vec<SqlExpr>,
    operator: BinaryOperator,
    right: Vec<SqlExpr>,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedExpr> {
    let operator = row_comparison_operator(operator, position)?;
    build_row_comparison(
        convert_row_items(left, sql, position)?,
        operator,
        convert_row_items(right, sql, position)?,
        position,
    )
}

fn convert_row_in_list(
    left: Vec<SqlExpr>,
    list: Vec<SqlExpr>,
    negated: bool,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedExpr> {
    let left = convert_row_items(left, sql, position)?;
    let mut comparisons = Vec::with_capacity(list.len());
    for candidate in list {
        let SqlExpr::Tuple(candidate) = candidate else {
            return Err(
                DbError::new(SYNTAX_ERROR, "row IN list entries must all be row values")
                    .with_position_opt(position),
            );
        };
        comparisons.push(build_row_comparison(
            left.clone(),
            BinaryOperator::Eq,
            convert_row_items(candidate, sql, position)?,
            position,
        )?);
    }
    let mut comparisons = comparisons.into_iter();
    let mut expression = comparisons
        .next()
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "row IN list must not be empty"))?;
    for candidate in comparisons {
        expression = ParsedExpr {
            position,
            kind: ParsedExprKind::Binary {
                left: Box::new(expression),
                op: BinaryOperator::Or,
                right: Box::new(candidate),
            },
        };
    }
    if negated {
        expression = ParsedExpr {
            position,
            kind: ParsedExprKind::Unary {
                op: UnaryOperator::Not,
                expr: Box::new(expression),
            },
        };
    }
    Ok(expression)
}

fn build_row_comparison(
    left: Vec<ParsedExpr>,
    operator: BinaryOperator,
    right: Vec<ParsedExpr>,
    position: Option<usize>,
) -> Result<ParsedExpr> {
    if left.len() != right.len() {
        return Err(
            DbError::new(SYNTAX_ERROR, "unequal number of entries in row expressions")
                .with_position_opt(position),
        );
    }
    let mut comparisons = left.into_iter().zip(right).map(|(left, right)| ParsedExpr {
        position,
        kind: ParsedExprKind::Binary {
            left: Box::new(left),
            op: BinaryOperator::Eq,
            right: Box::new(right),
        },
    });
    let mut expression = comparisons
        .next()
        .ok_or_else(|| DbError::new(SYNTAX_ERROR, "row value must not be empty"))?;
    for comparison in comparisons {
        expression = ParsedExpr {
            position,
            kind: ParsedExprKind::Binary {
                left: Box::new(expression),
                op: BinaryOperator::And,
                right: Box::new(comparison),
            },
        };
    }
    if operator == BinaryOperator::NotEq {
        expression = ParsedExpr {
            position,
            kind: ParsedExprKind::Unary {
                op: UnaryOperator::Not,
                expr: Box::new(expression),
            },
        };
    }
    Ok(expression)
}

fn convert_window_function(
    function: Function,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedExprKind> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported_at("this window function option is not supported yet", position);
    }
    let FunctionArguments::List(arguments) = function.args else {
        return unsupported_at("window function arguments must use parentheses", position);
    };
    if !arguments.clauses.is_empty() {
        return unsupported_at(
            "ordered window function arguments are not supported yet",
            position,
        );
    }
    if matches!(
        arguments.duplicate_treatment,
        Some(DuplicateTreatment::Distinct)
    ) {
        return unsupported_at("DISTINCT is not implemented for window functions", position);
    }
    let function_name = function.name.to_string().to_ascii_lowercase();
    let mut count_star = false;
    let (window_function, expected_arguments) = match function_name.as_str() {
        "row_number" => (WindowFunction::RowNumber, 0..=0),
        "rank" => (WindowFunction::Rank, 0..=0),
        "dense_rank" => (WindowFunction::DenseRank, 0..=0),
        "lag" => (WindowFunction::Lag, 1..=3),
        "lead" => (WindowFunction::Lead, 1..=3),
        "first_value" => (WindowFunction::FirstValue, 1..=1),
        "last_value" => (WindowFunction::LastValue, 1..=1),
        "nth_value" => (WindowFunction::NthValue, 2..=2),
        "count" => (WindowFunction::Aggregate(AggregateFunction::Count), 1..=1),
        "sum" => (WindowFunction::Aggregate(AggregateFunction::Sum), 1..=1),
        "avg" => (WindowFunction::Aggregate(AggregateFunction::Avg), 1..=1),
        "min" => (WindowFunction::Aggregate(AggregateFunction::Min), 1..=1),
        "max" => (WindowFunction::Aggregate(AggregateFunction::Max), 1..=1),
        _ => return unsupported_at("this window function is not supported yet", position),
    };
    let converted_arguments = match arguments.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
            if window_function == WindowFunction::Aggregate(AggregateFunction::Count) =>
        {
            count_star = true;
            Vec::new()
        }
        values => values
            .iter()
            .map(|argument| match argument {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(argument)) => {
                    convert_expr(argument.clone(), sql)
                }
                _ => unsupported_at("window function requires expression arguments", position),
            })
            .collect::<Result<Vec<_>>>()?,
    };
    if !count_star && !expected_arguments.contains(&converted_arguments.len()) {
        return Err(DbError::new(
            SYNTAX_ERROR,
            format!("invalid argument count for window function {function_name}"),
        )
        .with_position_opt(position));
    }
    let filter = function
        .filter
        .map(|filter| convert_expr(*filter, sql).map(Box::new))
        .transpose()?;
    if filter.is_some() && !matches!(window_function, WindowFunction::Aggregate(_)) {
        return Err(DbError::new(
            "42809",
            "FILTER is specified, but the window function is not an aggregate",
        )
        .with_position_opt(position));
    }
    let call = ParsedWindowCall {
        function: window_function,
        arguments: converted_arguments,
        count_star,
        filter,
    };
    let over = function
        .over
        .ok_or_else(|| DbError::internal("window function lost its OVER clause"))?;
    match over {
        WindowType::WindowSpec(spec) => Ok(ParsedExprKind::Window {
            call: Box::new(call),
            spec: Box::new(convert_window_spec(spec, sql, position)?),
        }),
        WindowType::NamedWindow(name) => Ok(ParsedExprKind::NamedWindow {
            call: Box::new(call),
            name: convert_ident(name, sql),
        }),
    }
}

fn convert_window_spec(
    spec: SqlWindowSpec,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedWindowSpec> {
    let window_name = spec.window_name.map(|name| convert_ident(name, sql));
    let partition_by = spec
        .partition_by
        .into_iter()
        .map(|expr| convert_expr(expr, sql))
        .collect::<Result<Vec<_>>>()?;
    let order_by = spec
        .order_by
        .into_iter()
        .map(|order| {
            if order.with_fill.is_some() {
                return unsupported_at("window ORDER BY WITH FILL is not supported", position);
            }
            Ok(ParsedOrder {
                expr: convert_expr(order.expr, sql)?,
                ascending: order.options.asc.unwrap_or(true),
                nulls_first: order.options.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let frame = spec
        .window_frame
        .map(|frame| convert_window_frame(frame, sql, position))
        .transpose()?;
    Ok(ParsedWindowSpec {
        window_name,
        partition_by,
        order_by,
        frame,
    })
}

fn convert_window_frame(
    frame: SqlWindowFrame,
    sql: &str,
    position: Option<usize>,
) -> Result<ParsedWindowFrame> {
    let units = match frame.units {
        SqlWindowFrameUnits::Rows => WindowFrameUnits::Rows,
        SqlWindowFrameUnits::Range => WindowFrameUnits::Range,
        SqlWindowFrameUnits::Groups => {
            return unsupported_at("GROUPS window frames are not supported yet", position);
        }
    };
    let start_bound = convert_window_frame_bound(frame.start_bound, sql)?;
    let end_bound = frame
        .end_bound
        .map(|bound| convert_window_frame_bound(bound, sql))
        .transpose()?
        .unwrap_or(ParsedWindowFrameBound::CurrentRow);
    validate_window_frame_order(&start_bound, &end_bound, position)?;
    Ok(ParsedWindowFrame {
        units,
        start_bound,
        end_bound,
    })
}

fn convert_window_frame_bound(
    bound: SqlWindowFrameBound,
    sql: &str,
) -> Result<ParsedWindowFrameBound> {
    Ok(match bound {
        SqlWindowFrameBound::CurrentRow => ParsedWindowFrameBound::CurrentRow,
        SqlWindowFrameBound::Preceding(None) => ParsedWindowFrameBound::UnboundedPreceding,
        SqlWindowFrameBound::Preceding(Some(offset)) => {
            ParsedWindowFrameBound::Preceding(Box::new(convert_expr(*offset, sql)?))
        }
        SqlWindowFrameBound::Following(None) => ParsedWindowFrameBound::UnboundedFollowing,
        SqlWindowFrameBound::Following(Some(offset)) => {
            ParsedWindowFrameBound::Following(Box::new(convert_expr(*offset, sql)?))
        }
    })
}

fn validate_window_frame_order(
    start: &ParsedWindowFrameBound,
    end: &ParsedWindowFrameBound,
    position: Option<usize>,
) -> Result<()> {
    if matches!(start, ParsedWindowFrameBound::UnboundedFollowing) {
        return Err(
            DbError::new("42P20", "frame start cannot be UNBOUNDED FOLLOWING")
                .with_position_opt(position),
        );
    }
    if matches!(end, ParsedWindowFrameBound::UnboundedPreceding) {
        return Err(
            DbError::new("42P20", "frame end cannot be UNBOUNDED PRECEDING")
                .with_position_opt(position),
        );
    }
    let rank = |bound: &ParsedWindowFrameBound| match bound {
        ParsedWindowFrameBound::UnboundedPreceding => 0_u8,
        ParsedWindowFrameBound::Preceding(_) => 1,
        ParsedWindowFrameBound::CurrentRow => 2,
        ParsedWindowFrameBound::Following(_) => 3,
        ParsedWindowFrameBound::UnboundedFollowing => 4,
    };
    if rank(start) > rank(end) {
        return Err(DbError::new(
            "42P20",
            "frame starting from following row cannot end before it",
        )
        .with_position_opt(position));
    }
    Ok(())
}

fn convert_named_windows(
    definitions: Vec<sqlparser::ast::NamedWindowDefinition>,
    sql: &str,
) -> Result<BTreeMap<Identifier, ParsedWindowSpec>> {
    let mut windows: BTreeMap<Identifier, ParsedWindowSpec> = BTreeMap::new();
    for sqlparser::ast::NamedWindowDefinition(name, definition) in definitions {
        let name = convert_ident(name, sql);
        if windows.contains_key(&name.name) {
            return Err(DbError::new(
                "42712",
                format!("window {} is specified more than once", name.name),
            )
            .with_position_opt(name.position));
        }
        let spec = match definition {
            NamedWindowExpr::NamedWindow(base) => {
                let base = convert_ident(base, sql);
                windows.get(&base.name).cloned().ok_or_else(|| {
                    DbError::new("42704", format!("window {} does not exist", base.name))
                        .with_position_opt(base.position)
                })?
            }
            NamedWindowExpr::WindowSpec(mut spec) => {
                let inherited = spec
                    .window_name
                    .take()
                    .map(|base| {
                        let base = convert_ident(base, sql);
                        windows.get(&base.name).cloned().ok_or_else(|| {
                            DbError::new("42704", format!("window {} does not exist", base.name))
                                .with_position_opt(base.position)
                        })
                    })
                    .transpose()?;
                let has_partition = !spec.partition_by.is_empty();
                let has_order = !spec.order_by.is_empty();
                let has_frame = spec.window_frame.is_some();
                let mut converted = convert_window_spec(spec, sql, name.position)?;
                if let Some(base) = inherited {
                    if has_partition {
                        return Err(DbError::new(
                            "42P20",
                            "cannot override PARTITION BY clause of named window",
                        )
                        .with_position_opt(name.position));
                    }
                    if has_order && !base.order_by.is_empty() {
                        return Err(DbError::new(
                            "42P20",
                            "cannot override ORDER BY clause of named window",
                        )
                        .with_position_opt(name.position));
                    }
                    if base.frame.is_some() {
                        return Err(DbError::new(
                            "42P20",
                            "cannot copy a window that has a frame clause",
                        )
                        .with_position_opt(name.position));
                    }
                    converted.partition_by = base.partition_by;
                    if !has_order {
                        converted.order_by = base.order_by;
                    }
                    if !has_frame {
                        converted.frame = None;
                    }
                }
                converted
            }
        };
        windows.insert(name.name, spec);
    }
    Ok(windows)
}

fn resolve_named_window_expr(
    expression: &mut ParsedExpr,
    windows: &BTreeMap<Identifier, ParsedWindowSpec>,
) -> Result<()> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        if let ParsedExprKind::NamedWindow { call, name } = &expression.kind {
            let spec = windows.get(&name.name).cloned().ok_or_else(|| {
                DbError::new("42704", format!("window {} does not exist", name.name))
                    .with_position_opt(name.position)
            })?;
            expression.kind = ParsedExprKind::Window {
                call: call.clone(),
                spec: Box::new(spec),
            };
            continue;
        }
        match &mut expression.kind {
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                pending.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => pending.extend(elements.iter_mut().rev()),
            ParsedExprKind::Function { arguments, .. } => {
                pending.extend(arguments.iter_mut().rev());
            }
            ParsedExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                pending.extend(list.iter_mut().rev());
                pending.push(expr);
            }
            ParsedExprKind::InSubquery { expr, .. }
            | ParsedExprKind::QuantifiedSubquery { left: expr, .. } => pending.push(expr),
            ParsedExprKind::RowSubquery { left, .. } => pending.extend(left.iter_mut().rev()),
            ParsedExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            ParsedExprKind::Window { call, spec } => {
                resolve_window_spec_inheritance(spec, windows, expression.position)?;
                if let Some(filter) = &mut call.filter {
                    pending.push(filter);
                }
                pending.extend(call.arguments.iter_mut().rev());
                if let Some(frame) = &mut spec.frame {
                    match &mut frame.start_bound {
                        ParsedWindowFrameBound::Preceding(offset)
                        | ParsedWindowFrameBound::Following(offset) => pending.push(offset),
                        ParsedWindowFrameBound::UnboundedPreceding
                        | ParsedWindowFrameBound::CurrentRow
                        | ParsedWindowFrameBound::UnboundedFollowing => {}
                    }
                    match &mut frame.end_bound {
                        ParsedWindowFrameBound::Preceding(offset)
                        | ParsedWindowFrameBound::Following(offset) => pending.push(offset),
                        ParsedWindowFrameBound::UnboundedPreceding
                        | ParsedWindowFrameBound::CurrentRow
                        | ParsedWindowFrameBound::UnboundedFollowing => {}
                    }
                }
                pending.extend(spec.order_by.iter_mut().map(|order| &mut order.expr));
                pending.extend(&mut spec.partition_by);
            }
            ParsedExprKind::NamedWindow { .. } => unreachable!("handled above"),
            ParsedExprKind::Column(_)
            | ParsedExprKind::Literal(_)
            | ParsedExprKind::Parameter(_)
            | ParsedExprKind::ResolvedParameter { .. }
            | ParsedExprKind::ScalarSubquery(_)
            | ParsedExprKind::Exists { .. }
            | ParsedExprKind::ApplyValue { .. }
            | ParsedExprKind::WindowValue { .. } => {}
        }
    }
    Ok(())
}

fn resolve_window_spec_inheritance(
    spec: &mut ParsedWindowSpec,
    windows: &BTreeMap<Identifier, ParsedWindowSpec>,
    position: Option<usize>,
) -> Result<()> {
    let Some(base_name) = spec.window_name.take() else {
        return Ok(());
    };
    let base = windows.get(&base_name.name).cloned().ok_or_else(|| {
        DbError::new("42704", format!("window {} does not exist", base_name.name))
            .with_position_opt(base_name.position)
    })?;
    if !spec.partition_by.is_empty() {
        return Err(DbError::new(
            "42P20",
            "cannot override PARTITION BY clause of named window",
        )
        .with_position_opt(position));
    }
    if !spec.order_by.is_empty() && !base.order_by.is_empty() {
        return Err(
            DbError::new("42P20", "cannot override ORDER BY clause of named window")
                .with_position_opt(position),
        );
    }
    if base.frame.is_some() {
        return Err(
            DbError::new("42P20", "cannot copy a window that has a frame clause")
                .with_position_opt(position),
        );
    }
    spec.partition_by = base.partition_by;
    if spec.order_by.is_empty() {
        spec.order_by = base.order_by;
    }
    Ok(())
}

fn convert_binary_operator(
    operator: SqlBinaryOperator,
    position: Option<usize>,
) -> Result<BinaryOperator> {
    match operator {
        SqlBinaryOperator::Eq => Ok(BinaryOperator::Eq),
        SqlBinaryOperator::Plus => Ok(BinaryOperator::Add),
        SqlBinaryOperator::Minus => Ok(BinaryOperator::Subtract),
        SqlBinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
        SqlBinaryOperator::Divide => Ok(BinaryOperator::Divide),
        SqlBinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
        SqlBinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
        SqlBinaryOperator::Lt => Ok(BinaryOperator::Lt),
        SqlBinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
        SqlBinaryOperator::Gt => Ok(BinaryOperator::Gt),
        SqlBinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
        SqlBinaryOperator::And => Ok(BinaryOperator::And),
        SqlBinaryOperator::Or => Ok(BinaryOperator::Or),
        _ => unsupported_at("this binary operator is not supported yet", position),
    }
}

fn convert_comparison_operator(
    operator: SqlBinaryOperator,
    position: Option<usize>,
) -> Result<BinaryOperator> {
    let operator = convert_binary_operator(operator, position)?;
    if matches!(
        operator,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    ) {
        Ok(operator)
    } else {
        unsupported_at(
            "quantified subqueries require a comparison operator",
            position,
        )
    }
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
        DataType::Timestamp(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
            DateTime::parse_from_rfc3339(value)
                .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%:z"))
                .map(|value| Value::Timestamp(value.naive_utc()))
                .map_err(|_| invalid())
        }
        DataType::Interval {
            fields: None,
            precision: None,
        } => PgInterval::from_str(value)
            .map(Value::Interval)
            .map_err(|error| error.with_position_opt(position)),
        DataType::Interval { .. } => unsupported_at(
            "INTERVAL field and precision qualifiers are not supported yet",
            position,
        ),
        _ => unsupported_at("this typed literal is not supported yet", position),
    }
}

fn convert_data_type(data_type: DataType) -> Result<ScalarType> {
    convert_data_type_with_array_depth(data_type, 0)
}

fn convert_column_data_type(
    data_type: DataType,
    sql: &str,
) -> Result<(ScalarType, Option<ParsedObjectName>)> {
    convert_column_data_type_with_array_depth(data_type, sql, 0)
}

fn convert_column_data_type_with_array_depth(
    data_type: DataType,
    sql: &str,
    array_depth: usize,
) -> Result<(ScalarType, Option<ParsedObjectName>)> {
    match data_type {
        DataType::Custom(name, modifiers)
            if modifiers.is_empty()
                && !name.to_string().eq_ignore_ascii_case("uniqueidentifier")
                && !name.to_string().eq_ignore_ascii_case("vector") =>
        {
            if let Some(data_type) = postgres_catalog_scalar_type(&name, &modifiers) {
                return Ok((data_type, None));
            }
            Ok((ScalarType::Text, Some(convert_object_name(name, sql)?)))
        }
        DataType::Array(ArrayElemTypeDef::SquareBracket(element, _)) => {
            const MAX_ARRAY_DIMENSIONS: usize = 6;
            if array_depth >= MAX_ARRAY_DIMENSIONS {
                return Err(DbError::new(
                    "54000",
                    format!("array type exceeds the maximum of {MAX_ARRAY_DIMENSIONS} dimensions"),
                ));
            }
            let (element, declared_type) =
                convert_column_data_type_with_array_depth(*element, sql, array_depth + 1)?;
            Ok((
                match element {
                    ScalarType::Array { .. } => element,
                    element => ScalarType::Array {
                        element: Box::new(element),
                    },
                },
                declared_type,
            ))
        }
        data_type => Ok((
            convert_data_type_with_array_depth(data_type, array_depth)?,
            None,
        )),
    }
}

fn convert_data_type_with_array_depth(
    data_type: DataType,
    array_depth: usize,
) -> Result<ScalarType> {
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
        DataType::Interval {
            fields: None,
            precision: None,
        } => Ok(ScalarType::Interval),
        DataType::Interval { .. } => {
            unsupported("INTERVAL field and precision qualifiers are not supported yet")
        }
        DataType::Array(ArrayElemTypeDef::SquareBracket(element, _)) => {
            const MAX_ARRAY_DIMENSIONS: usize = 6;
            if array_depth >= MAX_ARRAY_DIMENSIONS {
                return Err(DbError::new(
                    "54000",
                    format!("array type exceeds the maximum of {MAX_ARRAY_DIMENSIONS} dimensions"),
                ));
            }
            let element = convert_data_type_with_array_depth(*element, array_depth + 1)?;
            Ok(match element {
                ScalarType::Array { .. } => element,
                element => ScalarType::Array {
                    element: Box::new(element),
                },
            })
        }
        DataType::Array(_) => unsupported("only PostgreSQL type[] array syntax is supported"),
        DataType::JSON => Ok(ScalarType::Json),
        DataType::JSONB => Ok(ScalarType::Jsonb),
        DataType::Uuid => Ok(ScalarType::Uuid),
        DataType::Custom(name, modifiers)
            if postgres_catalog_scalar_type(&name, &modifiers).is_some() =>
        {
            Ok(postgres_catalog_scalar_type(&name, &modifiers)
                .expect("guard established PostgreSQL catalog scalar type"))
        }
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

fn postgres_catalog_scalar_type(name: &ObjectName, modifiers: &[String]) -> Option<ScalarType> {
    if !modifiers.is_empty() {
        return None;
    }
    match name.to_string().to_ascii_lowercase().as_str() {
        "oid" | "pg_catalog.oid" => Some(ScalarType::Oid),
        "name" | "pg_catalog.name" => Some(ScalarType::Name),
        "\"char\"" | "pg_catalog.\"char\"" => Some(ScalarType::InternalChar),
        _ => None,
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

fn resolve_declared_data_type(
    catalog: &Catalog,
    parsed_data_type: &ScalarType,
    declared_type: &ParsedObjectName,
) -> Result<(ScalarType, TypeId)> {
    let (type_schema, type_name, type_position) = split_table_name(declared_type)?;
    let definition = catalog
        .user_defined_type(&type_schema, &type_name)
        .ok_or_else(|| {
            DbError::new(
                "42704",
                format!("type {type_schema}.{type_name} does not exist"),
            )
            .with_position_opt(type_position)
        })?;
    let logical_type = definition.logical_type();
    let data_type = if matches!(parsed_data_type, ScalarType::Array { .. }) {
        match logical_type {
            ScalarType::Array { .. } => logical_type,
            element => ScalarType::Array {
                element: Box::new(element),
            },
        }
    } else {
        logical_type
    };
    Ok((data_type, definition.id))
}

fn resolve_user_defined_type<'a>(
    name: &ParsedObjectName,
    catalog: &'a Catalog,
) -> Result<&'a TypeDefinition> {
    let (schema, name, position) = split_table_name(name)?;
    if catalog.schema(&schema).is_none() {
        return Err(
            DbError::new(UNDEFINED_SCHEMA, format!("schema {schema} does not exist"))
                .with_position_opt(position),
        );
    }
    catalog.user_defined_type(&schema, &name).ok_or_else(|| {
        DbError::new("42704", format!("type {schema}.{name} does not exist"))
            .with_position_opt(position)
    })
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
            Ok(NewColumn {
                name: column.name.name,
                data_type,
                declared_type,
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
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                stack.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => stack.extend(elements),
            ParsedExprKind::Function { arguments, .. } => stack.extend(arguments),
            ParsedExprKind::Binary { left, right, .. } => {
                stack.push(right);
                stack.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                stack.extend(list);
                stack.push(expr);
            }
            ParsedExprKind::Aggregate { .. } => {
                return unsupported("aggregate functions are not allowed in CHECK constraints");
            }
            ParsedExprKind::Window { .. }
            | ParsedExprKind::NamedWindow { .. }
            | ParsedExprKind::WindowValue { .. } => {
                return Err(DbError::new(
                    "42P20",
                    "window functions are not allowed in CHECK constraints",
                ));
            }
            ParsedExprKind::ScalarSubquery(_)
            | ParsedExprKind::Exists { .. }
            | ParsedExprKind::InSubquery { .. }
            | ParsedExprKind::QuantifiedSubquery { .. }
            | ParsedExprKind::RowSubquery { .. } => {
                return unsupported("subqueries are not allowed in CHECK constraints");
            }
            ParsedExprKind::Literal(_)
            | ParsedExprKind::Parameter(_)
            | ParsedExprKind::ResolvedParameter { .. }
            | ParsedExprKind::ApplyValue { .. } => {}
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DmlTarget {
    Table(TableId),
    View(ViewId),
}

struct DmlRelation {
    target: DmlTarget,
    scope: TableDefinition,
}

fn resolve_dml_relation(
    name: &ParsedObjectName,
    event: CatalogTriggerEvent,
    catalog: &Catalog,
) -> Result<DmlRelation> {
    let (schema_name, relation_name, position) = split_table_name(name)?;
    let schema = catalog.schema(&schema_name).ok_or_else(|| {
        DbError::new(
            UNDEFINED_SCHEMA,
            format!("schema {schema_name} does not exist"),
        )
        .with_position_opt(position)
    })?;
    if let Some(table) = schema.table(&relation_name) {
        return Ok(DmlRelation {
            target: DmlTarget::Table(table.id),
            scope: table.clone(),
        });
    }
    let view = schema.view(&relation_name).ok_or_else(|| {
        DbError::new(
            UNDEFINED_TABLE,
            format!("relation {schema_name}.{relation_name} does not exist"),
        )
        .with_position_opt(position)
    })?;
    if view.kind != ViewKind::Regular {
        return Err(
            DbError::new("42809", "cannot modify a materialized view").with_position_opt(position)
        );
    }
    let has_instead_of_trigger = view.triggers().any(|trigger| {
        trigger.enabled
            && trigger.timing == TriggerTiming::InsteadOf
            && trigger.level == TriggerLevel::Row
            && trigger.events.contains(&event)
    });
    if !has_instead_of_trigger {
        return Err(DbError::new(
            "55000",
            format!("cannot modify view {schema_name}.{relation_name}"),
        )
        .with_detail(format!(
            "no enabled INSTEAD OF ROW trigger handles {event:?}"
        ))
        .with_hint("Create a matching INSTEAD OF trigger on the view.")
        .with_position_opt(position));
    }
    Ok(DmlRelation {
        target: DmlTarget::View(view.id),
        scope: TableDefinition::expression_scope_for_schema(view.name.clone(), &view.output)?,
    })
}

fn bind_insert(
    table_name: ParsedObjectName,
    columns: Vec<ParsedIdentifier>,
    rows: Vec<Vec<ParsedExpr>>,
    on_conflict: Option<ParsedOnConflict>,
    returning: Vec<ParsedProjection>,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let relation = resolve_dml_relation(&table_name, CatalogTriggerEvent::Insert, catalog)?;
    let table = relation.scope;
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
    if matches!(relation.target, DmlTarget::View(_)) && on_conflict.is_some() {
        return unsupported("ON CONFLICT is not supported for view DML");
    }
    let on_conflict = on_conflict
        .map(|on_conflict| bind_on_conflict(on_conflict, &table))
        .transpose()?;
    let returning = bind_returning(returning, &table)?;
    match relation.target {
        DmlTarget::Table(table_id) => Ok(BoundStatement::Insert {
            table_id,
            column_indexes,
            rows,
            on_conflict,
            returning,
        }),
        DmlTarget::View(view_id) => {
            let view = catalog
                .view_by_id(view_id)
                .ok_or_else(|| DbError::internal("bound view target disappeared"))?;
            Ok(BoundStatement::ViewInsert {
                view_id,
                source: Box::new(bind_view_source(view, catalog, view_depth)?),
                column_indexes,
                rows,
                returning,
            })
        }
    }
}

fn bind_merge(merge: ParsedMerge, catalog: &Catalog) -> Result<BoundStatement> {
    let ParsedMerge {
        target,
        source,
        on,
        clauses,
        returning,
    } = merge;
    let target_definition = resolve_table(&target.name, catalog)?.clone();
    let mut inputs = Vec::new();
    let target = bind_input_table(target, false, catalog, &mut inputs)?;
    let source = bind_input_table(source, false, catalog, &mut inputs)?;
    let on = bind_merge_boolean(on, &inputs)?;
    let clauses = clauses
        .into_iter()
        .map(|clause| {
            let kind = match clause.kind {
                ParsedMergeClauseKind::Matched => BoundMergeClauseKind::Matched,
                ParsedMergeClauseKind::NotMatchedByTarget => {
                    BoundMergeClauseKind::NotMatchedByTarget
                }
                ParsedMergeClauseKind::NotMatchedBySource => {
                    BoundMergeClauseKind::NotMatchedBySource
                }
            };
            let predicate = clause
                .predicate
                .map(|predicate| bind_merge_boolean(predicate, &inputs))
                .transpose()?;
            if kind == BoundMergeClauseKind::NotMatchedBySource
                && predicate.as_ref().is_some_and(|predicate| {
                    bound_expr_references_column_at_or_after(predicate, source.offset)
                })
            {
                return Err(invalid_merge_source_reference());
            }
            let action = match clause.action {
                ParsedMergeAction::Update { assignments } => {
                    if kind == BoundMergeClauseKind::NotMatchedByTarget {
                        return Err(DbError::new(
                            SYNTAX_ERROR,
                            "MERGE UPDATE requires WHEN MATCHED or WHEN NOT MATCHED BY SOURCE",
                        ));
                    }
                    let mut seen = BTreeSet::new();
                    let assignments = assignments
                        .into_iter()
                        .map(|(column, expr)| {
                            let index =
                                target_definition
                                    .column_index(&column.name)
                                    .ok_or_else(|| {
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
                                bind_expr_multi(
                                    expr,
                                    &inputs,
                                    Some(&target_definition.columns()[index].data_type),
                                    false,
                                )?,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if kind == BoundMergeClauseKind::NotMatchedBySource
                        && assignments.iter().any(|(_, expression)| {
                            bound_expr_references_column_at_or_after(expression, source.offset)
                        })
                    {
                        return Err(invalid_merge_source_reference());
                    }
                    BoundMergeAction::Update { assignments }
                }
                ParsedMergeAction::Delete => {
                    if kind == BoundMergeClauseKind::NotMatchedByTarget {
                        return Err(DbError::new(
                            SYNTAX_ERROR,
                            "MERGE DELETE requires WHEN MATCHED or WHEN NOT MATCHED BY SOURCE",
                        ));
                    }
                    BoundMergeAction::Delete
                }
                ParsedMergeAction::Insert { columns, values } => {
                    if kind != BoundMergeClauseKind::NotMatchedByTarget {
                        return Err(DbError::new(
                            SYNTAX_ERROR,
                            "MERGE INSERT requires WHEN NOT MATCHED",
                        ));
                    }
                    let column_indexes = if columns.is_empty() {
                        (0..target_definition.columns().len()).collect::<Vec<_>>()
                    } else {
                        let mut seen = BTreeSet::new();
                        columns
                            .into_iter()
                            .map(|column| {
                                let index = target_definition
                                    .column_index(&column.name)
                                    .ok_or_else(|| {
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
                    if values.len() != column_indexes.len() {
                        return Err(DbError::new(
                            SYNTAX_ERROR,
                            "MERGE INSERT has more target columns than expressions",
                        ));
                    }
                    let values = values
                        .into_iter()
                        .zip(&column_indexes)
                        .map(|(expr, index)| {
                            bind_expr_multi(
                                expr,
                                &inputs,
                                Some(&target_definition.columns()[*index].data_type),
                                false,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    BoundMergeAction::Insert {
                        column_indexes,
                        values,
                    }
                }
                ParsedMergeAction::DoNothing => BoundMergeAction::DoNothing,
            };
            Ok(BoundMergeClause {
                kind,
                predicate,
                action,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundStatement::Merge(BoundMerge {
        target,
        source,
        on,
        clauses,
        returning: bind_returning(returning, &target_definition)?,
    }))
}

fn bound_expr_references_column_at_or_after(expr: &BoundExpr, first_index: usize) -> bool {
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match &expr.kind {
            BoundExprKind::Column { index } if *index >= first_index => return true,
            BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => pending.push(expr),
            BoundExprKind::Array { elements, .. } => pending.extend(elements),
            BoundExprKind::Function { arguments, .. } => pending.extend(arguments),
            BoundExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundExprKind::InList { expr, list, .. } => {
                pending.extend(list);
                pending.push(expr);
            }
            BoundExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(argument) = argument {
                    pending.push(argument);
                }
                if let Some(filter) = filter {
                    pending.push(filter);
                }
            }
            BoundExprKind::Column { .. }
            | BoundExprKind::Literal(_)
            | BoundExprKind::Parameter { .. }
            | BoundExprKind::Correlation { .. }
            | BoundExprKind::ApplyValue { .. } => {}
        }
    }
    false
}

fn invalid_merge_source_reference() -> DbError {
    DbError::new(
        UNDEFINED_TABLE,
        "MERGE source columns are not available in WHEN NOT MATCHED BY SOURCE",
    )
    .with_hint("Reference only target columns in this MERGE branch.")
}

fn bind_merge_boolean(expr: ParsedExpr, inputs: &[InputColumn]) -> Result<BoundExpr> {
    let position = expr.position;
    let bound = bind_expr_multi(expr, inputs, Some(&ScalarType::Boolean), false)?;
    if bound.data_type != ScalarType::Boolean {
        return Err(DbError::new(DATATYPE_MISMATCH, "predicate must be boolean")
            .with_position_opt(position));
    }
    Ok(bound)
}

fn bind_on_conflict(
    on_conflict: ParsedOnConflict,
    table: &TableDefinition,
) -> Result<BoundOnConflict> {
    let target_columns = on_conflict
        .target
        .map(|target| bind_conflict_target(target, table))
        .transpose()?;
    let action =
        match on_conflict.action {
            ParsedConflictAction::DoNothing => BoundConflictAction::DoNothing,
            ParsedConflictAction::DoUpdate {
                assignments,
                filter,
            } => {
                if target_columns.is_none() {
                    return Err(DbError::new(
                        SYNTAX_ERROR,
                        "ON CONFLICT DO UPDATE requires a conflict target",
                    ));
                }
                let excluded = Identifier::unquoted("excluded");
                if table.name == excluded {
                    return Err(DbError::new(
                        "42712",
                        "table name excluded conflicts with the ON CONFLICT pseudo-relation",
                    )
                    .with_hint("Alias the target table when target aliases are supported."));
                }
                let width = table.columns().len();
                let mut inputs = table
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(index, column)| InputColumn {
                        binding: table.name.clone(),
                        name: column.name.clone(),
                        index,
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        outer_depth: 0,
                    })
                    .collect::<Vec<_>>();
                inputs.extend(table.columns().iter().enumerate().map(|(index, column)| {
                    InputColumn {
                        binding: excluded.clone(),
                        name: column.name.clone(),
                        index: width + index,
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        outer_depth: 0,
                    }
                }));
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
                                format!("column {} specified more than once", column.name),
                            )
                            .with_position_opt(column.position));
                        }
                        let expr = qualify_conflict_expr(expr, &table.name);
                        let bound = bind_expr_multi(
                            expr,
                            &inputs,
                            Some(&table.columns()[index].data_type),
                            false,
                        )?;
                        Ok((index, bound))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let filter = filter
                    .map(|expr| {
                        bind_multi_boolean(qualify_conflict_expr(expr, &table.name), &inputs)
                    })
                    .transpose()?;
                BoundConflictAction::DoUpdate {
                    assignments,
                    filter,
                }
            }
        };
    Ok(BoundOnConflict {
        target_columns,
        action,
    })
}

fn bind_conflict_target(
    target: ParsedConflictTarget,
    table: &TableDefinition,
) -> Result<Vec<usize>> {
    let column_ids = match target {
        ParsedConflictTarget::Columns(columns) => {
            if columns.is_empty() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "ON CONFLICT column target is empty",
                ));
            }
            let mut seen = BTreeSet::new();
            let column_ids = columns
                .into_iter()
                .map(|column| {
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
                    Ok(definition.id)
                })
                .collect::<Result<Vec<_>>>()?;
            let matches_unique_index = table.indexes().any(|index| {
                index.unique
                    && index.method == IndexMethod::BTree
                    && same_column_set(&index.key_columns, &column_ids)
            });
            if !matches_unique_index {
                return Err(DbError::new(
                    "42P10",
                    "there is no unique or exclusion constraint matching ON CONFLICT",
                ));
            }
            column_ids
        }
        ParsedConflictTarget::Constraint(name) => {
            let [name] = name.parts.as_slice() else {
                return unsupported_at(
                    "ON CONFLICT constraint names must be unqualified",
                    name.parts.first().and_then(|part| part.position),
                );
            };
            let constraint = table.constraint(&name.name).ok_or_else(|| {
                DbError::new(
                    "42704",
                    format!(
                        "constraint {} for table {} does not exist",
                        name.name, table.name
                    ),
                )
                .with_position_opt(name.position)
            })?;
            match &constraint.kind {
                ConstraintKind::PrimaryKey { columns } | ConstraintKind::Unique { columns } => {
                    columns.clone()
                }
                _ => {
                    return Err(DbError::new(
                        "42809",
                        format!("constraint {} is not unique", constraint.name),
                    )
                    .with_position_opt(name.position));
                }
            }
        }
    };
    column_ids
        .into_iter()
        .map(|column_id| {
            table.column_index_by_id(column_id).ok_or_else(|| {
                DbError::internal("ON CONFLICT target column is absent from its table")
            })
        })
        .collect()
}

fn same_column_set(left: &[ColumnId], right: &[ColumnId]) -> bool {
    left.len() == right.len() && left.iter().all(|column| right.contains(column))
}

fn qualify_conflict_expr(mut expr: ParsedExpr, target_binding: &Identifier) -> ParsedExpr {
    expr.kind = match expr.kind {
        ParsedExprKind::Column(mut name) if name.parts.len() == 1 => {
            let position = name.parts.first().and_then(|part| part.position);
            name.parts.insert(
                0,
                ParsedIdentifier {
                    name: target_binding.clone(),
                    position,
                },
            );
            ParsedExprKind::Column(name)
        }
        ParsedExprKind::Unary { op, expr } => ParsedExprKind::Unary {
            op,
            expr: Box::new(qualify_conflict_expr(*expr, target_binding)),
        },
        ParsedExprKind::Cast {
            expr,
            data_type,
            declared_type,
        } => ParsedExprKind::Cast {
            expr: Box::new(qualify_conflict_expr(*expr, target_binding)),
            data_type,
            declared_type,
        },
        ParsedExprKind::Array {
            elements,
            dimensions,
        } => ParsedExprKind::Array {
            elements: elements
                .into_iter()
                .map(|expr| qualify_conflict_expr(expr, target_binding))
                .collect(),
            dimensions,
        },
        ParsedExprKind::Function {
            function,
            arguments,
        } => ParsedExprKind::Function {
            function,
            arguments: arguments
                .into_iter()
                .map(|expr| qualify_conflict_expr(expr, target_binding))
                .collect(),
        },
        ParsedExprKind::Binary { left, op, right } => ParsedExprKind::Binary {
            left: Box::new(qualify_conflict_expr(*left, target_binding)),
            op,
            right: Box::new(qualify_conflict_expr(*right, target_binding)),
        },
        ParsedExprKind::InList {
            expr,
            list,
            negated,
        } => ParsedExprKind::InList {
            expr: Box::new(qualify_conflict_expr(*expr, target_binding)),
            list: list
                .into_iter()
                .map(|expr| qualify_conflict_expr(expr, target_binding))
                .collect(),
            negated,
        },
        ParsedExprKind::Aggregate {
            function,
            argument,
            distinct,
            filter,
        } => ParsedExprKind::Aggregate {
            function,
            argument: argument
                .map(|argument| Box::new(qualify_conflict_expr(*argument, target_binding))),
            distinct,
            filter: filter.map(|filter| Box::new(qualify_conflict_expr(*filter, target_binding))),
        },
        kind => kind,
    };
    expr
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

fn bind_with_clause(
    recursive: bool,
    ctes: Vec<ParsedCte>,
    mut body: ParsedStatement,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let mut transient_catalog = catalog.clone();
    let temporary_schema = (0_u64..)
        .map(|suffix| Identifier::unquoted(format!("__ordadb_cte_{view_depth}_{suffix}")))
        .find(|candidate| transient_catalog.schema(candidate).is_none())
        .ok_or_else(|| DbError::new("54000", "could not allocate a transient CTE namespace"))?;
    transient_catalog.create_schema(temporary_schema.clone())?;

    let mut names = BTreeSet::new();
    let mut replacements = BTreeMap::new();
    let mut bound_ctes = Vec::with_capacity(ctes.len());
    for cte in ctes {
        if !names.insert(cte.name.name.clone()) {
            return Err(DbError::new(
                "42712",
                format!("WITH query name {} specified more than once", cte.name.name),
            )
            .with_position_opt(cte.name.position));
        }
        let cte_name = cte.name.clone();
        let cte_columns = cte.columns.clone();
        let query = *cte.query;
        let self_recursive = recursive && parsed_query_references_table(&query, &cte_name.name, 0)?;
        let (mut seed, recursive_term, union_all) = if self_recursive {
            match query {
                ParsedStatement::SetOperation {
                    left,
                    operator: QuerySetOperator::Union,
                    all,
                    right,
                    order_by,
                    offset,
                    limit,
                } if order_by.is_empty() && offset.is_none() && limit.is_none() => {
                    (*left, Some(*right), all)
                }
                _ => {
                    return Err(DbError::new(
                        FEATURE_NOT_SUPPORTED,
                        "recursive CTEs require a top-level UNION or UNION ALL",
                    ));
                }
            }
        } else {
            (query, None, false)
        };
        rewrite_cte_references(&mut seed, &replacements, 0)?;
        if self_recursive && parsed_query_references_table(&seed, &cte_name.name, 0)? {
            return Err(DbError::new(
                "42P19",
                "recursive reference must not appear within the non-recursive term",
            ));
        }
        let seed = bind_with_view_depth(seed, &transient_catalog, view_depth + 1)?;
        let mut output = bound_query_schema(&seed)?;
        apply_cte_column_aliases(&cte_name, &cte_columns, &mut output)?;
        let table_id = create_cte_relation(
            &mut transient_catalog,
            &temporary_schema,
            &cte_name,
            &output,
        )?;
        replacements.insert(
            cte_name.name.clone(),
            cte_replacement_name(&temporary_schema, &cte_name),
        );
        let recursive = recursive_term
            .map(|mut recursive_term| {
                rewrite_cte_references(&mut recursive_term, &replacements, 0)?;
                let recursive_term =
                    bind_with_view_depth(recursive_term, &transient_catalog, view_depth + 1)?;
                let recursive_schema = bound_query_schema(&recursive_term)?;
                ensure_recursive_cte_schema(&output, &recursive_schema)?;
                Ok(Box::new(recursive_term))
            })
            .transpose()?;
        bound_ctes.push(BoundCte {
            table_id,
            seed: Box::new(seed),
            recursive,
            union_all,
        });
    }
    rewrite_cte_references(&mut body, &replacements, 0)?;
    let body = bind_with_view_depth(body, &transient_catalog, view_depth + 1)?;
    let schema = bound_query_schema(&body)?;
    Ok(BoundStatement::With {
        ctes: bound_ctes,
        body: Box::new(body),
        catalog: Box::new(transient_catalog),
        schema,
    })
}

fn apply_cte_column_aliases(
    name: &ParsedIdentifier,
    columns: &[ParsedIdentifier],
    output: &mut Schema,
) -> Result<()> {
    if columns.is_empty() {
        return Ok(());
    }
    if columns.len() != output.fields.len() {
        return Err(DbError::new(
            "42601",
            format!(
                "WITH query {} has {} columns available but {} columns specified",
                name.name,
                output.fields.len(),
                columns.len()
            ),
        ));
    }
    for (field, column) in output.fields.iter_mut().zip(columns) {
        field.name = column.name.as_str().to_owned();
    }
    Ok(())
}

fn create_cte_relation(
    catalog: &mut Catalog,
    schema: &Identifier,
    name: &ParsedIdentifier,
    output: &Schema,
) -> Result<TableId> {
    catalog.create_table(
        schema,
        name.name.clone(),
        output
            .fields
            .iter()
            .map(|field| NewColumn {
                name: Identifier::unquoted(field.name.clone()),
                data_type: field.data_type.clone(),
                declared_type: None,
                nullable: field.nullable,
                primary_key: false,
                unique: false,
                default: None,
            })
            .collect(),
    )
}

fn cte_replacement_name(schema: &Identifier, name: &ParsedIdentifier) -> ParsedObjectName {
    ParsedObjectName {
        parts: vec![
            ParsedIdentifier {
                name: schema.clone(),
                position: name.position,
            },
            name.clone(),
        ],
    }
}

fn ensure_recursive_cte_schema(seed: &Schema, recursive: &Schema) -> Result<()> {
    if seed.fields.len() != recursive.fields.len() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "recursive UNION queries must have the same number of columns",
        ));
    }
    for (seed, recursive) in seed.fields.iter().zip(&recursive.fields) {
        let Some(common) = common_type(&seed.data_type, &recursive.data_type) else {
            return Err(DbError::new(
                DATATYPE_MISMATCH,
                format!(
                    "recursive UNION types {:?} and {:?} cannot be matched",
                    seed.data_type, recursive.data_type
                ),
            ));
        };
        if common != seed.data_type {
            return Err(DbError::new(
                DATATYPE_MISMATCH,
                "recursive query column type must match the non-recursive term",
            ));
        }
    }
    Ok(())
}

fn parsed_query_references_table(
    statement: &ParsedStatement,
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    if depth >= 64 {
        return Err(DbError::new(
            "54001",
            "recursive CTE analysis exceeds the maximum depth of 64",
        ));
    }
    let references = match statement {
        ParsedStatement::Select {
            table: source,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => {
            cte_table_matches(source, table)
                || parsed_projections_reference_table(projection, table, depth)?
                || parsed_optional_expr_references_table(filter.as_ref(), table, depth)?
                || parsed_orders_reference_table(order_by, table, depth)?
                || parsed_optional_expr_references_table(offset.as_ref(), table, depth)?
                || parsed_optional_expr_references_table(limit.as_ref(), table, depth)?
        }
        ParsedStatement::AdvancedSelect {
            table: source,
            joins,
            projection,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
            ..
        } => {
            cte_table_matches(&source.name, table)
                || joins.iter().try_fold(false, |found, join| {
                    Ok(found
                        || match &join.source {
                            ParsedJoinSource::Table(source) => {
                                cte_table_matches(&source.name, table)
                            }
                            ParsedJoinSource::Derived { query, .. } => {
                                parsed_query_references_table(query, table, depth + 1)?
                            }
                        })
                })?
                || parsed_exprs_reference_table(joins.iter().map(|join| &join.on), table, depth)?
                || parsed_projections_reference_table(projection, table, depth)?
                || parsed_optional_expr_references_table(filter.as_ref(), table, depth)?
                || parsed_exprs_reference_table(group_by.iter(), table, depth)?
                || parsed_optional_expr_references_table(having.as_ref(), table, depth)?
                || parsed_orders_reference_table(order_by, table, depth)?
                || parsed_optional_expr_references_table(offset.as_ref(), table, depth)?
                || parsed_optional_expr_references_table(limit.as_ref(), table, depth)?
        }
        ParsedStatement::SetOperation {
            left,
            right,
            order_by,
            offset,
            limit,
            ..
        } => {
            parsed_query_references_table(left, table, depth + 1)?
                || parsed_query_references_table(right, table, depth + 1)?
                || parsed_orders_reference_table(order_by, table, depth)?
                || parsed_optional_expr_references_table(offset.as_ref(), table, depth)?
                || parsed_optional_expr_references_table(limit.as_ref(), table, depth)?
        }
        ParsedStatement::With { ctes, body, .. } => {
            if ctes.iter().any(|cte| &cte.name.name == table) {
                false
            } else {
                ctes.iter().try_fold(false, |found, cte| {
                    Ok(found || parsed_query_references_table(&cte.query, table, depth + 1)?)
                })? || parsed_query_references_table(body, table, depth + 1)?
            }
        }
        ParsedStatement::Explain { statement } => {
            parsed_query_references_table(statement, table, depth + 1)?
        }
        _ => false,
    };
    Ok(references)
}

fn parsed_projections_reference_table(
    projections: &[ParsedProjection],
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    parsed_exprs_reference_table(
        projections
            .iter()
            .filter_map(|projection| match projection {
                ParsedProjection::Wildcard => None,
                ParsedProjection::Expression { expr, .. } => Some(expr),
            }),
        table,
        depth,
    )
}

fn parsed_orders_reference_table(
    orders: &[ParsedOrder],
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    parsed_exprs_reference_table(orders.iter().map(|order| &order.expr), table, depth)
}

fn parsed_optional_expr_references_table(
    expression: Option<&ParsedExpr>,
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    match expression {
        Some(expression) => parsed_expr_references_table(expression, table, depth),
        None => Ok(false),
    }
}

fn parsed_exprs_reference_table<'a>(
    expressions: impl IntoIterator<Item = &'a ParsedExpr>,
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    for expression in expressions {
        if parsed_expr_references_table(expression, table, depth)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parsed_expr_references_table(
    expression: &ParsedExpr,
    table: &Identifier,
    depth: usize,
) -> Result<bool> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        match &expression.kind {
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                pending.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => pending.extend(elements),
            ParsedExprKind::Function { arguments, .. } => pending.extend(arguments),
            ParsedExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                pending.extend(list);
                pending.push(expr);
            }
            ParsedExprKind::ScalarSubquery(query)
            | ParsedExprKind::Exists {
                subquery: query, ..
            } => {
                if parsed_query_references_table(query, table, depth + 1)? {
                    return Ok(true);
                }
            }
            ParsedExprKind::InSubquery { expr, subquery, .. } => {
                pending.push(expr);
                if parsed_query_references_table(subquery, table, depth + 1)? {
                    return Ok(true);
                }
            }
            ParsedExprKind::QuantifiedSubquery { left, subquery, .. } => {
                pending.push(left);
                if parsed_query_references_table(subquery, table, depth + 1)? {
                    return Ok(true);
                }
            }
            ParsedExprKind::RowSubquery { left, subquery, .. } => {
                pending.extend(left);
                if parsed_query_references_table(subquery, table, depth + 1)? {
                    return Ok(true);
                }
            }
            ParsedExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            ParsedExprKind::Window { call, spec } => {
                if let Some(filter) = &call.filter {
                    pending.push(filter);
                }
                pending.extend(&call.arguments);
                pending.extend(spec.order_by.iter().map(|order| &order.expr));
                pending.extend(&spec.partition_by);
            }
            ParsedExprKind::NamedWindow { call, .. } => {
                if let Some(filter) = &call.filter {
                    pending.push(filter);
                }
                pending.extend(&call.arguments);
            }
            ParsedExprKind::Column(_)
            | ParsedExprKind::Literal(_)
            | ParsedExprKind::Parameter(_)
            | ParsedExprKind::ResolvedParameter { .. }
            | ParsedExprKind::ApplyValue { .. }
            | ParsedExprKind::WindowValue { .. } => {}
        }
    }
    Ok(false)
}

fn cte_table_matches(name: &ParsedObjectName, table: &Identifier) -> bool {
    matches!(name.parts.as_slice(), [name] if &name.name == table)
}

fn rewrite_cte_references(
    statement: &mut ParsedStatement,
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
    depth: usize,
) -> Result<()> {
    if depth >= 64 {
        return Err(DbError::new(
            "54001",
            "CTE scope nesting exceeds the maximum depth of 64",
        ));
    }
    match statement {
        ParsedStatement::Select {
            table,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => {
            rewrite_cte_table(table, replacements);
            rewrite_cte_projections(projection, replacements, depth)?;
            rewrite_cte_optional_expr(filter.as_mut(), replacements, depth)?;
            rewrite_cte_orders(order_by, replacements, depth)?;
            rewrite_cte_optional_expr(offset.as_mut(), replacements, depth)?;
            rewrite_cte_optional_expr(limit.as_mut(), replacements, depth)?;
        }
        ParsedStatement::AdvancedSelect {
            table,
            joins,
            projection,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
            ..
        } => {
            rewrite_cte_table(&mut table.name, replacements);
            for join in joins {
                match &mut join.source {
                    ParsedJoinSource::Table(table) => {
                        rewrite_cte_table(&mut table.name, replacements);
                    }
                    ParsedJoinSource::Derived { query, .. } => {
                        rewrite_cte_references(query, replacements, depth + 1)?;
                    }
                }
                rewrite_cte_expr(&mut join.on, replacements, depth)?;
            }
            rewrite_cte_projections(projection, replacements, depth)?;
            rewrite_cte_optional_expr(filter.as_mut(), replacements, depth)?;
            for expression in group_by {
                rewrite_cte_expr(expression, replacements, depth)?;
            }
            rewrite_cte_optional_expr(having.as_mut(), replacements, depth)?;
            rewrite_cte_orders(order_by, replacements, depth)?;
            rewrite_cte_optional_expr(offset.as_mut(), replacements, depth)?;
            rewrite_cte_optional_expr(limit.as_mut(), replacements, depth)?;
        }
        ParsedStatement::SetOperation {
            left,
            right,
            order_by,
            offset,
            limit,
            ..
        } => {
            rewrite_cte_references(left, replacements, depth + 1)?;
            rewrite_cte_references(right, replacements, depth + 1)?;
            rewrite_cte_orders(order_by, replacements, depth)?;
            rewrite_cte_optional_expr(offset.as_mut(), replacements, depth)?;
            rewrite_cte_optional_expr(limit.as_mut(), replacements, depth)?;
        }
        ParsedStatement::With { ctes, body, .. } => {
            let mut outer = replacements.clone();
            for cte in ctes.iter() {
                outer.remove(&cte.name.name);
            }
            for cte in ctes {
                rewrite_cte_references(&mut cte.query, &outer, depth + 1)?;
            }
            rewrite_cte_references(body, &outer, depth + 1)?;
        }
        ParsedStatement::Explain { statement } => {
            rewrite_cte_references(statement, replacements, depth + 1)?;
        }
        _ => {}
    }
    Ok(())
}

fn rewrite_cte_projections(
    projections: &mut [ParsedProjection],
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
    depth: usize,
) -> Result<()> {
    for projection in projections {
        if let ParsedProjection::Expression { expr, .. } = projection {
            rewrite_cte_expr(expr, replacements, depth)?;
        }
    }
    Ok(())
}

fn rewrite_cte_orders(
    orders: &mut [ParsedOrder],
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
    depth: usize,
) -> Result<()> {
    for order in orders {
        rewrite_cte_expr(&mut order.expr, replacements, depth)?;
    }
    Ok(())
}

fn rewrite_cte_optional_expr(
    expression: Option<&mut ParsedExpr>,
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
    depth: usize,
) -> Result<()> {
    if let Some(expression) = expression {
        rewrite_cte_expr(expression, replacements, depth)?;
    }
    Ok(())
}

fn rewrite_cte_expr(
    expression: &mut ParsedExpr,
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
    depth: usize,
) -> Result<()> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        match &mut expression.kind {
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                pending.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => pending.extend(elements),
            ParsedExprKind::Function { arguments, .. } => pending.extend(arguments),
            ParsedExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                pending.extend(list);
                pending.push(expr);
            }
            ParsedExprKind::ScalarSubquery(query)
            | ParsedExprKind::Exists {
                subquery: query, ..
            } => rewrite_cte_references(query, replacements, depth + 1)?,
            ParsedExprKind::InSubquery { expr, subquery, .. } => {
                pending.push(expr);
                rewrite_cte_references(subquery, replacements, depth + 1)?;
            }
            ParsedExprKind::QuantifiedSubquery { left, subquery, .. } => {
                pending.push(left);
                rewrite_cte_references(subquery, replacements, depth + 1)?;
            }
            ParsedExprKind::RowSubquery { left, subquery, .. } => {
                pending.extend(left);
                rewrite_cte_references(subquery, replacements, depth + 1)?;
            }
            ParsedExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            ParsedExprKind::Window { call, spec } => {
                if let Some(filter) = &mut call.filter {
                    pending.push(filter);
                }
                pending.extend(&mut call.arguments);
                pending.extend(spec.order_by.iter_mut().map(|order| &mut order.expr));
                pending.extend(&mut spec.partition_by);
            }
            ParsedExprKind::NamedWindow { call, .. } => {
                if let Some(filter) = &mut call.filter {
                    pending.push(filter);
                }
                pending.extend(&mut call.arguments);
            }
            ParsedExprKind::Column(_)
            | ParsedExprKind::Literal(_)
            | ParsedExprKind::Parameter(_)
            | ParsedExprKind::ResolvedParameter { .. }
            | ParsedExprKind::ApplyValue { .. }
            | ParsedExprKind::WindowValue { .. } => {}
        }
    }
    Ok(())
}

fn rewrite_cte_table(
    table: &mut ParsedObjectName,
    replacements: &BTreeMap<Identifier, ParsedObjectName>,
) {
    if let [name] = table.parts.as_slice()
        && let Some(replacement) = replacements.get(&name.name)
    {
        *table = replacement.clone();
    }
}

#[allow(clippy::too_many_arguments)]
fn bind_set_operation(
    left: ParsedStatement,
    operator: QuerySetOperator,
    all: bool,
    right: ParsedStatement,
    order_by: Vec<ParsedOrder>,
    offset: Option<ParsedExpr>,
    limit: Option<ParsedExpr>,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let left = bind_with_view_depth(left, catalog, view_depth + 1)?;
    let right = bind_with_view_depth(right, catalog, view_depth + 1)?;
    let left_schema = bound_query_schema(&left)?;
    let right_schema = bound_query_schema(&right)?;
    if left_schema.fields.len() != right_schema.fields.len() {
        return Err(DbError::new(
            SYNTAX_ERROR,
            "each set-operation query must have the same number of columns",
        ));
    }
    let schema = Schema::new(
        left_schema
            .fields
            .iter()
            .zip(&right_schema.fields)
            .map(|(left, right)| {
                let data_type =
                    common_type(&left.data_type, &right.data_type).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "set-operation types {:?} and {:?} cannot be matched",
                                left.data_type, right.data_type
                            ),
                        )
                    })?;
                Ok(Field::new(
                    left.name.clone(),
                    data_type,
                    left.nullable || right.nullable,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    );
    if !(operator == QuerySetOperator::Union && all)
        && schema
            .fields
            .iter()
            .any(|field| field.data_type == ScalarType::Json)
    {
        return Err(DbError::new(
            "42883",
            "could not identify an equality operator for type json",
        ));
    }
    let order_by = order_by
        .into_iter()
        .map(|order| bind_set_order(order, &schema))
        .collect::<Result<Vec<_>>>()?;
    let offset = offset
        .map(|expr| bind_expr(expr, None, Some(&ScalarType::Int64)))
        .transpose()?;
    let limit = limit
        .map(|expr| bind_expr(expr, None, Some(&ScalarType::Int64)))
        .transpose()?;
    Ok(BoundStatement::SetOperation {
        left: Box::new(left),
        operator,
        all,
        right: Box::new(right),
        schema,
        order_by,
        offset,
        limit,
    })
}

fn bind_set_order(order: ParsedOrder, schema: &Schema) -> Result<BoundOrder> {
    let column_index = match order.expr.kind {
        ParsedExprKind::Literal(Value::Int16(value)) if value > 0 => usize::from(value as u16) - 1,
        ParsedExprKind::Literal(Value::Int32(value)) if value > 0 => usize::try_from(value - 1)
            .map_err(|_| DbError::new("22003", "ORDER BY position is out of range"))?,
        ParsedExprKind::Literal(Value::Int64(value)) if value > 0 => usize::try_from(value - 1)
            .map_err(|_| DbError::new("22003", "ORDER BY position is out of range"))?,
        ParsedExprKind::Column(name) if name.parts.len() == 1 => {
            let column = &name.parts[0];
            let matches = schema
                .fields
                .iter()
                .enumerate()
                .filter(|(_, field)| field.name == column.name.as_str())
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [index] => *index,
                [] => {
                    return Err(DbError::new(
                        UNDEFINED_COLUMN,
                        format!("column {} does not exist", column.name),
                    )
                    .with_position_opt(column.position));
                }
                _ => {
                    return Err(DbError::new(
                        "42702",
                        format!("column reference {} is ambiguous", column.name),
                    )
                    .with_position_opt(column.position));
                }
            }
        }
        _ => {
            return unsupported_at(
                "ORDER BY on a set operation supports output columns or ordinals only",
                order.expr.position,
            );
        }
    };
    if column_index >= schema.fields.len() {
        return Err(DbError::new(
            "42P10",
            format!(
                "ORDER BY position {} is not in select list",
                column_index + 1
            ),
        )
        .with_position_opt(order.expr.position));
    }
    Ok(BoundOrder {
        column_index,
        expression: None,
        data_type: schema.fields[column_index].data_type.clone(),
        ascending: order.ascending,
        nulls_first: order.nulls_first,
    })
}

fn bind_simple_order(
    order: ParsedOrder,
    projection: &[BoundProjection],
    table: &TableDefinition,
) -> Result<BoundOrder> {
    let expression = match projected_order_position(&order.expr, projection)? {
        Some(position) => projection[position].expr.clone(),
        None => bind_expr(order.expr.clone(), Some(table), None)?,
    };
    bound_expression_order(order, expression)
}

fn bind_multi_order(
    order: ParsedOrder,
    projection: &[BoundProjection],
    inputs: &[InputColumn],
) -> Result<BoundOrder> {
    let expression = match projected_order_position(&order.expr, projection)? {
        Some(position) => projection[position].expr.clone(),
        None => bind_expr_multi(order.expr.clone(), inputs, None, false)?,
    };
    bound_expression_order(order, expression)
}

fn bind_distinct_order(
    order: ParsedOrder,
    projection: &[BoundProjection],
    inputs: &[InputColumn],
) -> Result<BoundOrder> {
    let expression = match projected_order_position(&order.expr, projection)? {
        Some(position) => projection[position].expr.clone(),
        None => {
            let expression = bind_expr_multi(order.expr.clone(), inputs, None, false)?;
            if !projection
                .iter()
                .any(|projected| projected.expr == expression)
            {
                return Err(DbError::new(
                    "42P10",
                    "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
                )
                .with_position_opt(order.expr.position));
            }
            expression
        }
    };
    bound_expression_order(order, expression)
}

fn bind_projected_order(
    order: ParsedOrder,
    projection: &[BoundProjection],
    inputs: &[InputColumn],
    group_by: &[BoundExpr],
) -> Result<BoundOrder> {
    let position = if let Some(position) = projected_order_position(&order.expr, projection)? {
        position
    } else {
        let expression = bind_expr_multi(order.expr.clone(), inputs, None, true)?;
        validate_grouped_expr(&expression, group_by)?;
        projection
            .iter()
            .position(|projected| projected.expr == expression)
            .ok_or_else(|| {
                DbError::new(
                    FEATURE_NOT_SUPPORTED,
                    "ORDER BY on grouped queries requires a selected grouped expression",
                )
                .with_position_opt(order.expr.position)
            })?
    };
    Ok(BoundOrder {
        column_index: position,
        expression: None,
        data_type: projection[position].expr.data_type.clone(),
        ascending: order.ascending,
        nulls_first: order.nulls_first,
    })
}

fn projected_order_position(
    expr: &ParsedExpr,
    projection: &[BoundProjection],
) -> Result<Option<usize>> {
    let ordinal = match &expr.kind {
        ParsedExprKind::Literal(Value::Int16(value)) => Some(i64::from(*value)),
        ParsedExprKind::Literal(Value::Int32(value)) => Some(i64::from(*value)),
        ParsedExprKind::Literal(Value::Int64(value)) => Some(*value),
        _ => None,
    };
    if let Some(ordinal) = ordinal {
        if ordinal <= 0 {
            return Err(
                DbError::new("42P10", "ORDER BY position must be greater than zero")
                    .with_position_opt(expr.position),
            );
        }
        let position = usize::try_from(ordinal - 1)
            .map_err(|_| DbError::new("22003", "ORDER BY position is out of range"))?;
        if position >= projection.len() {
            return Err(DbError::new(
                "42P10",
                format!("ORDER BY position {ordinal} is not in select list"),
            )
            .with_position_opt(expr.position));
        }
        return Ok(Some(position));
    }
    let ParsedExprKind::Column(name) = &expr.kind else {
        return Ok(None);
    };
    let [name] = name.parts.as_slice() else {
        return Ok(None);
    };
    let matches = projection
        .iter()
        .enumerate()
        .filter(|(_, projection)| projection.field.name == name.name.as_str())
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [position] => Ok(Some(*position)),
        _ => Err(
            DbError::new("42702", format!("ORDER BY {} is ambiguous", name.name))
                .with_position_opt(name.position),
        ),
    }
}

fn bound_expression_order(order: ParsedOrder, expression: BoundExpr) -> Result<BoundOrder> {
    if expression.data_type == ScalarType::Json {
        return Err(DbError::new(
            "42883",
            "could not identify an ordering operator for type json",
        )
        .with_position_opt(order.expr.position));
    }
    let data_type = expression.data_type.clone();
    let (column_index, expression) = match &expression.kind {
        BoundExprKind::Column { index } => (*index, None),
        _ => (usize::MAX, Some(expression)),
    };
    Ok(BoundOrder {
        column_index,
        expression,
        data_type,
        ascending: order.ascending,
        nulls_first: order.nulls_first,
    })
}

fn bind_apply_query(
    statement: ParsedStatement,
    catalog: &Catalog,
    view_depth: usize,
    outer_inputs: &[InputColumn],
) -> Result<BoundStatement> {
    match statement {
        ParsedStatement::Select {
            table,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => bind_advanced_select(
            AdvancedSelectInput {
                table: ParsedTable {
                    name: table,
                    alias: None,
                },
                joins: Vec::new(),
                projection,
                distinct: false,
                filter,
                group_by: Vec::new(),
                having: None,
                order_by,
                offset,
                limit,
            },
            catalog,
            view_depth,
            outer_inputs,
        ),
        ParsedStatement::AdvancedSelect {
            table,
            joins,
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
        } => bind_advanced_select(
            AdvancedSelectInput {
                table,
                joins,
                projection,
                distinct,
                filter,
                group_by,
                having,
                order_by,
                offset,
                limit,
            },
            catalog,
            view_depth,
            outer_inputs,
        ),
        statement => bind_with_view_depth(statement, catalog, view_depth),
    }
}

#[allow(clippy::too_many_arguments)]
fn lower_apply_expr(
    mut expr: ParsedExpr,
    catalog: &Catalog,
    inputs: &[InputColumn],
    apply_base: usize,
    applies: &mut Vec<BoundApply>,
    view_depth: usize,
) -> Result<ParsedExpr> {
    let position = expr.position;
    expr.kind = match expr.kind {
        ParsedExprKind::Unary { op, expr } => ParsedExprKind::Unary {
            op,
            expr: Box::new(lower_apply_expr(
                *expr, catalog, inputs, apply_base, applies, view_depth,
            )?),
        },
        ParsedExprKind::Cast {
            expr,
            data_type,
            declared_type,
        } => ParsedExprKind::Cast {
            expr: Box::new(lower_apply_expr(
                *expr, catalog, inputs, apply_base, applies, view_depth,
            )?),
            data_type,
            declared_type,
        },
        ParsedExprKind::Array {
            elements,
            dimensions,
        } => ParsedExprKind::Array {
            elements: elements
                .into_iter()
                .map(|expr| {
                    lower_apply_expr(expr, catalog, inputs, apply_base, applies, view_depth)
                })
                .collect::<Result<Vec<_>>>()?,
            dimensions,
        },
        ParsedExprKind::Function {
            function,
            arguments,
        } => ParsedExprKind::Function {
            function,
            arguments: arguments
                .into_iter()
                .map(|expr| {
                    lower_apply_expr(expr, catalog, inputs, apply_base, applies, view_depth)
                })
                .collect::<Result<Vec<_>>>()?,
        },
        ParsedExprKind::Binary { left, op, right } => ParsedExprKind::Binary {
            left: Box::new(lower_apply_expr(
                *left, catalog, inputs, apply_base, applies, view_depth,
            )?),
            op,
            right: Box::new(lower_apply_expr(
                *right, catalog, inputs, apply_base, applies, view_depth,
            )?),
        },
        ParsedExprKind::InList {
            expr,
            list,
            negated,
        } => ParsedExprKind::InList {
            expr: Box::new(lower_apply_expr(
                *expr, catalog, inputs, apply_base, applies, view_depth,
            )?),
            list: list
                .into_iter()
                .map(|expr| {
                    lower_apply_expr(expr, catalog, inputs, apply_base, applies, view_depth)
                })
                .collect::<Result<Vec<_>>>()?,
            negated,
        },
        ParsedExprKind::Aggregate {
            function,
            argument,
            distinct,
            filter,
        } => ParsedExprKind::Aggregate {
            function,
            argument: argument
                .map(|argument| {
                    lower_apply_expr(*argument, catalog, inputs, apply_base, applies, view_depth)
                        .map(Box::new)
                })
                .transpose()?,
            distinct,
            filter: filter
                .map(|filter| {
                    lower_apply_expr(*filter, catalog, inputs, apply_base, applies, view_depth)
                        .map(Box::new)
                })
                .transpose()?,
        },
        ParsedExprKind::ScalarSubquery(subquery) => {
            let query = bind_apply_query(*subquery, catalog, view_depth.saturating_add(1), inputs)?;
            let field = scalar_subquery_field(&query, position)?;
            let index = push_bound_apply(applies, apply_base, BoundApplyKind::Scalar, query)?;
            ParsedExprKind::ApplyValue {
                index,
                data_type: field.data_type,
                nullable: true,
            }
        }
        ParsedExprKind::Exists { subquery, negated } => {
            let query = bind_apply_query(*subquery, catalog, view_depth.saturating_add(1), inputs)?;
            let index = push_bound_apply(
                applies,
                apply_base,
                BoundApplyKind::Exists { negated },
                query,
            )?;
            ParsedExprKind::ApplyValue {
                index,
                data_type: ScalarType::Boolean,
                nullable: false,
            }
        }
        ParsedExprKind::InSubquery {
            expr: left,
            subquery,
            negated,
        } => {
            let left = lower_apply_expr(*left, catalog, inputs, apply_base, applies, view_depth)?;
            let query = bind_apply_query(*subquery, catalog, view_depth.saturating_add(1), inputs)?;
            let field = scalar_subquery_field(&query, position)?;
            let left_type = infer_multi_type(&left, inputs)?;
            let operand_type = left_type
                .as_ref()
                .map(|left_type| common_type(left_type, &field.data_type))
                .unwrap_or_else(|| Some(field.data_type.clone()))
                .ok_or_else(|| {
                    DbError::new(
                        DATATYPE_MISMATCH,
                        format!(
                            "IN types {:?} and {:?} cannot be matched",
                            left_type, field.data_type
                        ),
                    )
                    .with_position_opt(position)
                })?;
            if operand_type == ScalarType::Json {
                return Err(DbError::new(
                    "42883",
                    "could not identify an equality operator for type json",
                )
                .with_position_opt(position));
            }
            let left = bind_expr_multi(left, inputs, Some(&operand_type), false)?;
            let index = push_bound_apply(
                applies,
                apply_base,
                BoundApplyKind::In { left, negated },
                query,
            )?;
            ParsedExprKind::ApplyValue {
                index,
                data_type: ScalarType::Boolean,
                nullable: true,
            }
        }
        ParsedExprKind::QuantifiedSubquery {
            left,
            op,
            quantifier,
            subquery,
        } => {
            let left = lower_apply_expr(*left, catalog, inputs, apply_base, applies, view_depth)?;
            let query = bind_apply_query(*subquery, catalog, view_depth.saturating_add(1), inputs)?;
            let field = scalar_subquery_field(&query, position)?;
            let left_type = infer_multi_type(&left, inputs)?;
            let operand_type = left_type
                .as_ref()
                .map(|left_type| common_type(left_type, &field.data_type))
                .unwrap_or_else(|| Some(field.data_type.clone()))
                .ok_or_else(|| {
                    DbError::new(
                        DATATYPE_MISMATCH,
                        format!(
                            "quantified comparison types {:?} and {:?} cannot be matched",
                            left_type, field.data_type
                        ),
                    )
                    .with_position_opt(position)
                })?;
            if operand_type == ScalarType::Json {
                return Err(DbError::new(
                    "42883",
                    "could not identify a comparison operator for type json",
                )
                .with_position_opt(position));
            }
            let left = bind_expr_multi(left, inputs, Some(&operand_type), false)?;
            let index = push_bound_apply(
                applies,
                apply_base,
                BoundApplyKind::Quantified {
                    left,
                    op,
                    quantifier,
                },
                query,
            )?;
            ParsedExprKind::ApplyValue {
                index,
                data_type: ScalarType::Boolean,
                nullable: true,
            }
        }
        ParsedExprKind::RowSubquery {
            left,
            op,
            quantifier,
            negated,
            subquery,
        } => {
            let left = left
                .into_iter()
                .map(|expression| {
                    lower_apply_expr(expression, catalog, inputs, apply_base, applies, view_depth)
                })
                .collect::<Result<Vec<_>>>()?;
            let query = bind_apply_query(*subquery, catalog, view_depth.saturating_add(1), inputs)?;
            let schema = bound_query_schema(&query)?;
            if left.len() != schema.fields.len() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "unequal number of entries in row expressions",
                )
                .with_position_opt(position));
            }
            let mut bound_left = Vec::with_capacity(left.len());
            let mut operand_types = Vec::with_capacity(left.len());
            for (expression, field) in left.into_iter().zip(&schema.fields) {
                let left_type = infer_multi_type(&expression, inputs)?;
                let operand_type = left_type
                    .as_ref()
                    .map(|left_type| common_type(left_type, &field.data_type))
                    .unwrap_or_else(|| Some(field.data_type.clone()))
                    .ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "row comparison types {:?} and {:?} cannot be matched",
                                left_type, field.data_type
                            ),
                        )
                        .with_position_opt(position)
                    })?;
                if operand_type == ScalarType::Json {
                    return Err(DbError::new(
                        "42883",
                        "could not identify an equality operator for type json",
                    )
                    .with_position_opt(position));
                }
                bound_left.push(bind_expr_multi(
                    expression,
                    inputs,
                    Some(&operand_type),
                    false,
                )?);
                operand_types.push(operand_type);
            }
            let kind = match quantifier {
                Some(quantifier) => BoundApplyKind::RowQuantified {
                    left: bound_left,
                    op,
                    quantifier,
                    negated,
                    operand_types,
                },
                None if !negated => BoundApplyKind::RowScalar {
                    left: bound_left,
                    op,
                    operand_types,
                },
                None => {
                    return Err(DbError::internal(
                        "scalar row subquery retained a negated quantifier flag",
                    ));
                }
            };
            let index = push_bound_apply(applies, apply_base, kind, query)?;
            ParsedExprKind::ApplyValue {
                index,
                data_type: ScalarType::Boolean,
                nullable: true,
            }
        }
        kind => kind,
    };
    Ok(expr)
}

fn scalar_subquery_field(statement: &BoundStatement, position: Option<usize>) -> Result<Field> {
    let schema = bound_query_schema(statement)?;
    let [field] = schema.fields.as_slice() else {
        return Err(
            DbError::new(SYNTAX_ERROR, "subquery must return only one column")
                .with_position_opt(position),
        );
    };
    Ok(field.clone())
}

fn push_bound_apply(
    applies: &mut Vec<BoundApply>,
    apply_base: usize,
    kind: BoundApplyKind,
    query: BoundStatement,
) -> Result<usize> {
    let index = apply_base
        .checked_add(applies.len())
        .ok_or_else(|| DbError::new("54001", "Apply value index overflowed"))?;
    applies.push(BoundApply {
        kind,
        query: Box::new(query),
    });
    Ok(index)
}

struct BoundWindowCall {
    function: WindowFunction,
    arguments: Vec<BoundExpr>,
    count_star: bool,
    filter: Option<BoundExpr>,
    data_type: ScalarType,
    nullable: bool,
}

fn bind_window_call(
    call: ParsedWindowCall,
    inputs: &[InputColumn],
    position: Option<usize>,
) -> Result<BoundWindowCall> {
    if call.arguments.iter().any(expr_has_window)
        || call.filter.as_deref().is_some_and(expr_has_window)
    {
        return Err(
            DbError::new("42P20", "window function calls cannot be nested")
                .with_position_opt(position),
        );
    }
    if call.arguments.iter().any(expr_has_subquery)
        || call.filter.as_deref().is_some_and(expr_has_subquery)
    {
        return unsupported_at(
            "subquery expressions in window function arguments are not supported yet",
            position,
        );
    }
    if let WindowFunction::Aggregate(function) = call.function {
        let argument = match call.arguments.into_iter().collect::<Vec<_>>().as_slice() {
            [] if call.count_star && function == AggregateFunction::Count => None,
            [argument] if !call.count_star => {
                Some(bind_expr_multi(argument.clone(), inputs, None, true)?)
            }
            _ => {
                return Err(DbError::internal(
                    "aggregate window argument shape changed after parsing",
                ));
            }
        };
        let filter = call
            .filter
            .map(|filter| bind_expr_multi(*filter, inputs, Some(&ScalarType::Boolean), true))
            .transpose()?;
        let (data_type, nullable) = match (function, argument.as_ref()) {
            (AggregateFunction::Count, _) => (ScalarType::Int64, false),
            (AggregateFunction::Avg, Some(argument)) if is_numeric(&argument.data_type) => {
                (ScalarType::Float64, true)
            }
            (AggregateFunction::Sum, Some(argument)) if is_numeric(&argument.data_type) => {
                let data_type = match argument.data_type {
                    ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64 => ScalarType::Int64,
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
                    "aggregate window argument has an incompatible type",
                )
                .with_position_opt(position));
            }
        };
        return Ok(BoundWindowCall {
            function: call.function,
            arguments: argument.into_iter().collect(),
            count_star: call.count_star,
            filter,
            data_type,
            nullable,
        });
    }

    let mut arguments = call.arguments.into_iter();
    let first = match call.function {
        WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => None,
        WindowFunction::Lag
        | WindowFunction::Lead
        | WindowFunction::FirstValue
        | WindowFunction::LastValue
        | WindowFunction::NthValue => Some(bind_expr_multi(
            arguments
                .next()
                .ok_or_else(|| DbError::internal("value window function argument disappeared"))?,
            inputs,
            None,
            true,
        )?),
        WindowFunction::Aggregate(_) => unreachable!("aggregate handled above"),
    };
    let mut bound_arguments = first.iter().cloned().collect::<Vec<_>>();
    match call.function {
        WindowFunction::Lag | WindowFunction::Lead => {
            if let Some(offset) = arguments.next() {
                bound_arguments.push(bind_expr_multi(
                    offset,
                    inputs,
                    Some(&ScalarType::Int64),
                    false,
                )?);
            }
            if let Some(default) = arguments.next() {
                let data_type = &first
                    .as_ref()
                    .ok_or_else(|| DbError::internal("window value type disappeared"))?
                    .data_type;
                bound_arguments.push(bind_expr_multi(default, inputs, Some(data_type), false)?);
            }
        }
        WindowFunction::NthValue => {
            bound_arguments.push(bind_expr_multi(
                arguments
                    .next()
                    .ok_or_else(|| DbError::internal("NTH_VALUE offset disappeared"))?,
                inputs,
                Some(&ScalarType::Int64),
                false,
            )?);
        }
        WindowFunction::RowNumber
        | WindowFunction::Rank
        | WindowFunction::DenseRank
        | WindowFunction::FirstValue
        | WindowFunction::LastValue => {}
        WindowFunction::Aggregate(_) => unreachable!("aggregate handled above"),
    }
    if arguments.next().is_some() {
        return Err(DbError::internal(
            "window function retained an unexpected argument",
        ));
    }
    let (data_type, nullable) = first.map_or((ScalarType::Int64, false), |argument| {
        (argument.data_type, true)
    });
    Ok(BoundWindowCall {
        function: call.function,
        arguments: bound_arguments,
        count_star: false,
        filter: None,
        data_type,
        nullable,
    })
}

fn bind_window_frame(
    frame: ParsedWindowFrame,
    inputs: &[InputColumn],
    order_by: &[BoundOrder],
    position: Option<usize>,
) -> Result<BoundWindowFrame> {
    let offset_type = match frame.units {
        WindowFrameUnits::Rows => ScalarType::Int64,
        WindowFrameUnits::Range => {
            let has_offset = matches!(
                frame.start_bound,
                ParsedWindowFrameBound::Preceding(_) | ParsedWindowFrameBound::Following(_)
            ) || matches!(
                frame.end_bound,
                ParsedWindowFrameBound::Preceding(_) | ParsedWindowFrameBound::Following(_)
            );
            if !has_offset {
                ScalarType::Int64
            } else {
                let [order] = order_by else {
                    return Err(DbError::new(
                        "42P20",
                        "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column",
                    )
                    .with_position_opt(position));
                };
                let data_type = if let Some(expression) = &order.expression {
                    expression.data_type.clone()
                } else {
                    inputs
                        .get(order.column_index)
                        .map(|input| input.data_type.clone())
                        .ok_or_else(|| {
                            DbError::internal("window ORDER BY type index is out of bounds")
                        })?
                };
                if !is_numeric(&data_type) {
                    return Err(DbError::new(
                        "42883",
                        "RANGE offset is supported only for numeric ORDER BY expressions",
                    )
                    .with_position_opt(position));
                }
                data_type
            }
        }
    };
    Ok(BoundWindowFrame {
        units: frame.units,
        start_bound: bind_window_frame_bound(frame.start_bound, &offset_type, position)?,
        end_bound: bind_window_frame_bound(frame.end_bound, &offset_type, position)?,
    })
}

fn bind_window_frame_bound(
    bound: ParsedWindowFrameBound,
    offset_type: &ScalarType,
    position: Option<usize>,
) -> Result<BoundWindowFrameBound> {
    let bind_offset = |offset: ParsedExpr| {
        if expr_has_aggregate(&offset) || expr_has_subquery(&offset) || expr_has_window(&offset) {
            return Err(DbError::new(
                "42P20",
                "window frame offset cannot contain aggregate, window, or subquery expressions",
            )
            .with_position_opt(offset.position.or(position)));
        }
        bind_expr_multi(offset, &[], Some(offset_type), false).map_err(|error| {
            if matches!(error.sql_state.as_str(), "42703" | "42P01") {
                DbError::new("42P20", "window frame offset cannot contain variables")
                    .with_position_opt(error.position.or(position))
            } else {
                error
            }
        })
    };
    Ok(match bound {
        ParsedWindowFrameBound::UnboundedPreceding => BoundWindowFrameBound::UnboundedPreceding,
        ParsedWindowFrameBound::Preceding(offset) => {
            BoundWindowFrameBound::Preceding(bind_offset(*offset)?)
        }
        ParsedWindowFrameBound::CurrentRow => BoundWindowFrameBound::CurrentRow,
        ParsedWindowFrameBound::Following(offset) => {
            BoundWindowFrameBound::Following(bind_offset(*offset)?)
        }
        ParsedWindowFrameBound::UnboundedFollowing => BoundWindowFrameBound::UnboundedFollowing,
    })
}

fn lower_window_expr(
    mut expr: ParsedExpr,
    inputs: &[InputColumn],
    windows: &mut Vec<BoundWindow>,
) -> Result<ParsedExpr> {
    let position = expr.position;
    expr.kind = match expr.kind {
        ParsedExprKind::Unary { op, expr } => ParsedExprKind::Unary {
            op,
            expr: Box::new(lower_window_expr(*expr, inputs, windows)?),
        },
        ParsedExprKind::Cast {
            expr,
            data_type,
            declared_type,
        } => ParsedExprKind::Cast {
            expr: Box::new(lower_window_expr(*expr, inputs, windows)?),
            data_type,
            declared_type,
        },
        ParsedExprKind::Array {
            elements,
            dimensions,
        } => ParsedExprKind::Array {
            elements: elements
                .into_iter()
                .map(|element| lower_window_expr(element, inputs, windows))
                .collect::<Result<Vec<_>>>()?,
            dimensions,
        },
        ParsedExprKind::Function {
            function,
            arguments,
        } => ParsedExprKind::Function {
            function,
            arguments: arguments
                .into_iter()
                .map(|argument| lower_window_expr(argument, inputs, windows))
                .collect::<Result<Vec<_>>>()?,
        },
        ParsedExprKind::Binary { left, op, right } => ParsedExprKind::Binary {
            left: Box::new(lower_window_expr(*left, inputs, windows)?),
            op,
            right: Box::new(lower_window_expr(*right, inputs, windows)?),
        },
        ParsedExprKind::InList {
            expr,
            list,
            negated,
        } => ParsedExprKind::InList {
            expr: Box::new(lower_window_expr(*expr, inputs, windows)?),
            list: list
                .into_iter()
                .map(|candidate| lower_window_expr(candidate, inputs, windows))
                .collect::<Result<Vec<_>>>()?,
            negated,
        },
        ParsedExprKind::InSubquery {
            expr,
            subquery,
            negated,
        } => ParsedExprKind::InSubquery {
            expr: Box::new(lower_window_expr(*expr, inputs, windows)?),
            subquery,
            negated,
        },
        ParsedExprKind::QuantifiedSubquery {
            left,
            op,
            quantifier,
            subquery,
        } => ParsedExprKind::QuantifiedSubquery {
            left: Box::new(lower_window_expr(*left, inputs, windows)?),
            op,
            quantifier,
            subquery,
        },
        ParsedExprKind::RowSubquery {
            left,
            op,
            quantifier,
            negated,
            subquery,
        } => ParsedExprKind::RowSubquery {
            left: left
                .into_iter()
                .map(|expression| lower_window_expr(expression, inputs, windows))
                .collect::<Result<Vec<_>>>()?,
            op,
            quantifier,
            negated,
            subquery,
        },
        ParsedExprKind::Aggregate {
            argument,
            distinct,
            filter,
            function,
        } => {
            if argument.as_deref().is_some_and(expr_has_window)
                || filter.as_deref().is_some_and(expr_has_window)
            {
                return Err(
                    DbError::new("42P20", "window function calls cannot be nested")
                        .with_position_opt(position),
                );
            }
            ParsedExprKind::Aggregate {
                function,
                argument,
                distinct,
                filter,
            }
        }
        ParsedExprKind::Window { call, spec } => {
            let call = *call;
            let spec = *spec;
            if spec.window_name.is_some() {
                return Err(DbError::internal(
                    "window inheritance was not resolved before binding",
                ));
            }
            if spec.partition_by.iter().any(expr_has_window)
                || spec
                    .order_by
                    .iter()
                    .any(|order| expr_has_window(&order.expr))
            {
                return Err(
                    DbError::new("42P20", "window function calls cannot be nested")
                        .with_position_opt(position),
                );
            }
            if spec.partition_by.iter().any(expr_has_subquery)
                || spec
                    .order_by
                    .iter()
                    .any(|order| expr_has_subquery(&order.expr))
            {
                return unsupported_at(
                    "subquery expressions in window definitions are not supported yet",
                    position,
                );
            }
            let call = bind_window_call(call, inputs, position)?;
            let partition_by = spec
                .partition_by
                .into_iter()
                .map(|expr| bind_expr_multi(expr, inputs, None, true))
                .collect::<Result<Vec<_>>>()?;
            let order_by = spec
                .order_by
                .into_iter()
                .map(|order| {
                    let expression = bind_expr_multi(order.expr.clone(), inputs, None, true)?;
                    bound_expression_order(order, expression)
                })
                .collect::<Result<Vec<_>>>()?;
            let frame = spec
                .frame
                .map(|frame| bind_window_frame(frame, inputs, &order_by, position))
                .transpose()?;
            let ordinal = windows.len();
            windows.push(BoundWindow {
                function: call.function,
                value_index: usize::MAX,
                arguments: call.arguments,
                count_star: call.count_star,
                filter: call.filter,
                partition_by,
                order_by,
                frame,
                data_type: call.data_type,
                nullable: call.nullable,
            });
            ParsedExprKind::WindowValue { ordinal }
        }
        ParsedExprKind::NamedWindow { .. } => {
            return Err(DbError::internal("named window reference was not resolved"));
        }
        ParsedExprKind::WindowValue { .. } => {
            return Err(DbError::internal(
                "window expression was lowered more than once",
            ));
        }
        kind => kind,
    };
    Ok(expr)
}

fn finalize_window_values(
    expression: &mut ParsedExpr,
    window_base: usize,
    windows: &[BoundWindow],
) -> Result<()> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        if let ParsedExprKind::WindowValue { ordinal } = &expression.kind {
            let index = window_base
                .checked_add(*ordinal)
                .ok_or_else(|| DbError::new("54001", "window value index overflowed"))?;
            let window = windows.get(*ordinal).ok_or_else(|| {
                DbError::internal("window value ordinal is outside the bound window list")
            })?;
            expression.kind = ParsedExprKind::ApplyValue {
                index,
                data_type: window.data_type.clone(),
                nullable: window.nullable,
            };
            continue;
        }
        match &mut expression.kind {
            ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                pending.push(expr);
            }
            ParsedExprKind::Array { elements, .. } => pending.extend(elements.iter_mut().rev()),
            ParsedExprKind::Function { arguments, .. } => {
                pending.extend(arguments.iter_mut().rev());
            }
            ParsedExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            ParsedExprKind::InList { expr, list, .. } => {
                pending.extend(list.iter_mut().rev());
                pending.push(expr);
            }
            ParsedExprKind::InSubquery { expr, .. }
            | ParsedExprKind::QuantifiedSubquery { left: expr, .. } => pending.push(expr),
            ParsedExprKind::RowSubquery { left, .. } => pending.extend(left.iter_mut().rev()),
            ParsedExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            ParsedExprKind::Window { .. } => {
                return Err(DbError::internal("window expression was not lowered"));
            }
            ParsedExprKind::NamedWindow { .. } => {
                return Err(DbError::internal("named window reference was not resolved"));
            }
            ParsedExprKind::WindowValue { .. } => unreachable!("handled above"),
            ParsedExprKind::Column(_)
            | ParsedExprKind::Literal(_)
            | ParsedExprKind::Parameter(_)
            | ParsedExprKind::ResolvedParameter { .. }
            | ParsedExprKind::ScalarSubquery(_)
            | ParsedExprKind::Exists { .. }
            | ParsedExprKind::ApplyValue { .. } => {}
        }
    }
    Ok(())
}

fn bind_advanced_select(
    input: AdvancedSelectInput,
    catalog: &Catalog,
    view_depth: usize,
    outer_inputs: &[InputColumn],
) -> Result<BoundStatement> {
    let AdvancedSelectInput {
        table,
        joins,
        mut projection,
        distinct,
        mut filter,
        mut group_by,
        mut having,
        mut order_by,
        offset,
        limit,
    } = input;
    let mut local_inputs = Vec::new();
    let table = bind_input_table(table, false, catalog, &mut local_inputs)?;
    let mut bound_joins = Vec::new();
    for join in joins {
        if expr_has_window(&join.on) {
            return Err(DbError::new(
                "42P20",
                "window functions are not allowed in JOIN conditions",
            ));
        }
        let nullable = join.kind == JoinKind::Left;
        let source = bind_join_source(
            join.source,
            nullable,
            catalog,
            view_depth,
            outer_inputs,
            &mut local_inputs,
        )?;
        let inputs = inputs_with_outer(&local_inputs, outer_inputs)?;
        let on = bind_multi_boolean(join.on, &inputs)?;
        if bound_expr_has_aggregate(&on) {
            return Err(DbError::new(
                "42803",
                "aggregate functions are not allowed in JOIN conditions",
            ));
        }
        bound_joins.push(BoundJoin {
            source,
            kind: join.kind,
            on,
        });
    }
    let inputs = inputs_with_outer(&local_inputs, outer_inputs)?;
    let apply_base = local_inputs.len();
    let mut applies = Vec::new();
    let mut windows = Vec::new();

    if filter.as_ref().is_some_and(expr_has_window) {
        return Err(DbError::new(
            "42P20",
            "window functions are not allowed in WHERE",
        ));
    }
    if group_by.iter().any(expr_has_window) {
        return Err(DbError::new(
            "42P20",
            "window functions are not allowed in GROUP BY",
        ));
    }
    if having.as_ref().is_some_and(expr_has_window) {
        return Err(DbError::new(
            "42P20",
            "window functions are not allowed in HAVING",
        ));
    }
    if limit.as_ref().is_some_and(expr_has_window) || offset.as_ref().is_some_and(expr_has_window) {
        return Err(DbError::new(
            "42P20",
            "window functions are not allowed in LIMIT or OFFSET",
        ));
    }

    projection = projection
        .into_iter()
        .map(|projection| match projection {
            ParsedProjection::Wildcard => Ok(ParsedProjection::Wildcard),
            ParsedProjection::Expression { expr, alias } => Ok(ParsedProjection::Expression {
                expr: lower_window_expr(expr, &inputs, &mut windows)?,
                alias,
            }),
        })
        .collect::<Result<Vec<_>>>()?;
    order_by = order_by
        .into_iter()
        .map(|mut order| {
            order.expr = lower_window_expr(order.expr, &inputs, &mut windows)?;
            Ok(order)
        })
        .collect::<Result<Vec<_>>>()?;

    projection = projection
        .into_iter()
        .map(|projection| match projection {
            ParsedProjection::Wildcard => Ok(ParsedProjection::Wildcard),
            ParsedProjection::Expression { expr, alias } => Ok(ParsedProjection::Expression {
                expr: lower_apply_expr(
                    expr,
                    catalog,
                    &inputs,
                    apply_base,
                    &mut applies,
                    view_depth,
                )?,
                alias,
            }),
        })
        .collect::<Result<Vec<_>>>()?;
    filter = filter
        .map(|expr| lower_apply_expr(expr, catalog, &inputs, apply_base, &mut applies, view_depth))
        .transpose()?;
    group_by = group_by
        .into_iter()
        .map(|expr| lower_apply_expr(expr, catalog, &inputs, apply_base, &mut applies, view_depth))
        .collect::<Result<Vec<_>>>()?;
    having = having
        .map(|expr| lower_apply_expr(expr, catalog, &inputs, apply_base, &mut applies, view_depth))
        .transpose()?;
    order_by = order_by
        .into_iter()
        .map(|mut order| {
            order.expr = lower_apply_expr(
                order.expr,
                catalog,
                &inputs,
                apply_base,
                &mut applies,
                view_depth,
            )?;
            Ok(order)
        })
        .collect::<Result<Vec<_>>>()?;

    let window_base = apply_base
        .checked_add(applies.len())
        .ok_or_else(|| DbError::new("54001", "window value index overflowed"))?;
    for (ordinal, window) in windows.iter_mut().enumerate() {
        window.value_index = window_base
            .checked_add(ordinal)
            .ok_or_else(|| DbError::new("54001", "window value index overflowed"))?;
    }
    for projection in &mut projection {
        if let ParsedProjection::Expression { expr, .. } = projection {
            finalize_window_values(expr, window_base, &windows)?;
        }
    }
    for order in &mut order_by {
        finalize_window_values(&mut order.expr, window_base, &windows)?;
    }

    let mut bound_projection = Vec::new();
    for item in projection {
        match item {
            ParsedProjection::Wildcard => {
                for input in &local_inputs {
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
    if distinct
        && bound_projection
            .iter()
            .any(|projection| projection.expr.data_type == ScalarType::Json)
    {
        return Err(DbError::new(
            "42883",
            "could not identify an equality operator for type json",
        ));
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
        || having.as_ref().is_some_and(bound_expr_has_aggregate)
        || windows.iter().any(bound_window_input_has_aggregate);
    if aggregate {
        for projection in &bound_projection {
            if window_ordinal_for_expr(&projection.expr, &windows).is_some() {
                continue;
            }
            if bound_expr_has_window_slot(&projection.expr, &windows) {
                return unsupported(
                    "grouped window functions must be top-level SELECT expressions",
                );
            }
            validate_grouped_expr(&projection.expr, &group_by)?;
        }
        if let Some(having) = &having {
            validate_grouped_expr(having, &group_by)?;
        }
        remap_grouped_window_inputs(&mut windows, &bound_projection, &group_by)?;
    } else if having.is_some() {
        return Err(DbError::new(
            "42803",
            "HAVING requires grouping or an aggregate",
        ));
    }

    let order_by = order_by
        .into_iter()
        .map(|order| {
            if aggregate {
                bind_projected_order(order, &bound_projection, &inputs, &group_by)
            } else if distinct {
                bind_distinct_order(order, &bound_projection, &inputs)
            } else {
                bind_multi_order(order, &bound_projection, &inputs)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if limit.as_ref().is_some_and(expr_has_subquery)
        || offset.as_ref().is_some_and(expr_has_subquery)
    {
        return unsupported("subqueries in LIMIT or OFFSET are not supported yet");
    }
    let limit = limit
        .map(|expr| bind_expr_multi(expr, &inputs, Some(&ScalarType::Int64), false))
        .transpose()?;
    let offset = offset
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
        applies,
        windows,
        schema,
        projection: bound_projection,
        distinct,
        filter,
        group_by,
        having,
        order_by,
        offset,
        limit: limit.map(Box::new),
        aggregate,
    })
}

fn bind_join_source(
    source: ParsedJoinSource,
    nullable: bool,
    catalog: &Catalog,
    view_depth: usize,
    outer_inputs: &[InputColumn],
    local_inputs: &mut Vec<InputColumn>,
) -> Result<BoundJoinSource> {
    match source {
        ParsedJoinSource::Table(table) => {
            bind_input_table(table, nullable, catalog, local_inputs).map(BoundJoinSource::Table)
        }
        ParsedJoinSource::Derived {
            lateral,
            query,
            alias,
            columns,
        } => {
            let alias_position = alias.position;
            let binding = alias.name;
            if local_inputs.iter().any(|input| input.binding == binding) {
                return Err(DbError::new(
                    "42712",
                    format!("table name {binding} specified more than once"),
                )
                .with_position_opt(alias_position));
            }
            let visible_inputs = if lateral {
                inputs_with_outer(local_inputs, outer_inputs)?
            } else {
                Vec::new()
            };
            let nested_depth = view_depth.checked_add(1).ok_or_else(|| {
                DbError::new(
                    "54001",
                    "derived table nesting exceeds the implementation limit",
                )
            })?;
            let query = bind_apply_query(*query, catalog, nested_depth, &visible_inputs)?;
            let schema = bound_query_schema(&query)?;
            if columns.len() > schema.fields.len() {
                return Err(DbError::new(
                    SYNTAX_ERROR,
                    "derived table has more column aliases than output columns",
                )
                .with_position_opt(alias_position));
            }
            let offset = local_inputs.len();
            let width = schema.fields.len();
            local_inputs.extend(schema.fields.iter().enumerate().map(|(index, field)| {
                let name = columns.get(index).map_or_else(
                    || Identifier::unquoted(&field.name),
                    |alias| alias.name.clone(),
                );
                InputColumn {
                    binding: binding.clone(),
                    name,
                    index: offset + index,
                    data_type: field.data_type.clone(),
                    nullable: nullable || field.nullable,
                    outer_depth: 0,
                }
            }));
            Ok(BoundJoinSource::Derived {
                lateral,
                query: Box::new(query),
                binding,
                offset,
                width,
                nullable,
            })
        }
    }
}

fn inputs_with_outer(
    local_inputs: &[InputColumn],
    outer_inputs: &[InputColumn],
) -> Result<Vec<InputColumn>> {
    let mut inputs = Vec::with_capacity(local_inputs.len().saturating_add(outer_inputs.len()));
    inputs.extend_from_slice(local_inputs);
    for mut input in outer_inputs.iter().cloned() {
        input.outer_depth = input.outer_depth.checked_add(1).ok_or_else(|| {
            DbError::new(
                "54001",
                "correlation scope depth exceeds the implementation limit",
            )
        })?;
        inputs.push(input);
    }
    Ok(inputs)
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
                outer_depth: 0,
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
                kind: if column.outer_depth > 0 {
                    BoundExprKind::Correlation {
                        depth: column.outer_depth,
                        index: column.index,
                    }
                } else {
                    BoundExprKind::Column {
                        index: column.index,
                    }
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
        ParsedExprKind::ResolvedParameter { index, data_type } => {
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::Parameter { index },
                data_type,
                nullable: true,
            })
        }
        ParsedExprKind::ApplyValue {
            index,
            data_type,
            nullable,
        } => {
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::ApplyValue { index },
                data_type,
                nullable,
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
        ParsedExprKind::Cast {
            expr, data_type, ..
        } => {
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            let source_type = infer_multi_type(&expr, inputs)?;
            let bound = bind_expr_multi(
                *expr,
                inputs,
                source_type.is_none().then_some(&data_type),
                allow_aggregate,
            )?;
            ensure_explicit_cast_supported(&bound.data_type, &data_type, position)?;
            let nullable = bound.nullable;
            Ok(BoundExpr {
                kind: BoundExprKind::Cast {
                    expr: Box::new(bound),
                },
                data_type,
                nullable,
            })
        }
        ParsedExprKind::Array {
            elements,
            dimensions,
        } => {
            let expected_element = match expected {
                Some(ScalarType::Array { element }) => Some(element.as_ref().clone()),
                Some(expected) => {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        format!("array cannot be assigned to {expected:?}"),
                    )
                    .with_position_opt(position));
                }
                None => None,
            };
            let mut element_type = expected_element;
            for element in &elements {
                let Some(candidate) = infer_multi_type(element, inputs)? else {
                    continue;
                };
                element_type = Some(match element_type {
                    Some(current) => common_type(&current, &candidate).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "array element types {current:?} and {candidate:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(position)
                    })?,
                    None => candidate,
                });
            }
            let element_type = element_type.ok_or_else(|| {
                DbError::new(
                    INDETERMINATE_DATATYPE,
                    "cannot determine type of empty array",
                )
                .with_hint("Explicitly cast the array, for example ARRAY[]::integer[].")
                .with_position_opt(position)
            })?;
            if matches!(element_type, ScalarType::Array { .. }) {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "nested array values must use one flattened PostgreSQL array type",
                )
                .with_position_opt(position));
            }
            let elements = elements
                .into_iter()
                .map(|element| {
                    bind_expr_multi(element, inputs, Some(&element_type), allow_aggregate)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(BoundExpr {
                kind: BoundExprKind::Array {
                    elements,
                    dimensions,
                },
                data_type: ScalarType::Array {
                    element: Box::new(element_type),
                },
                nullable: false,
            })
        }
        ParsedExprKind::Function {
            function,
            arguments,
        } => bind_scalar_function_multi(
            function,
            arguments,
            inputs,
            expected,
            allow_aggregate,
            position,
        ),
        ParsedExprKind::Binary { left, op, right } => bind_multi_binary(
            *left,
            op,
            *right,
            inputs,
            position,
            expected,
            allow_aggregate,
        ),
        ParsedExprKind::InList {
            expr,
            list,
            negated,
        } => {
            if expected.is_some_and(|expected| expected != &ScalarType::Boolean) {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "IN predicate produces a boolean result",
                )
                .with_position_opt(position));
            }
            let mut operand_type = infer_multi_type(&expr, inputs)?;
            for candidate in &list {
                let Some(candidate_type) = infer_multi_type(candidate, inputs)? else {
                    continue;
                };
                operand_type = Some(match operand_type {
                    Some(current) => common_type(&current, &candidate_type).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "IN types {current:?} and {candidate_type:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(position)
                    })?,
                    None => candidate_type,
                });
            }
            let operand_type = operand_type.ok_or_else(|| {
                DbError::new(
                    INDETERMINATE_DATATYPE,
                    "could not determine data type of IN operands",
                )
                .with_position_opt(position)
            })?;
            if operand_type == ScalarType::Json {
                return Err(DbError::new(
                    "42883",
                    "could not identify an equality operator for type json",
                )
                .with_position_opt(position));
            }
            let expr = bind_expr_multi(*expr, inputs, Some(&operand_type), allow_aggregate)?;
            let list = list
                .into_iter()
                .map(|candidate| {
                    bind_expr_multi(candidate, inputs, Some(&operand_type), allow_aggregate)
                })
                .collect::<Result<Vec<_>>>()?;
            let nullable = expr.nullable || list.iter().any(|candidate| candidate.nullable);
            Ok(BoundExpr {
                kind: BoundExprKind::InList {
                    expr: Box::new(expr),
                    list,
                    negated,
                },
                data_type: ScalarType::Boolean,
                nullable,
            })
        }
        ParsedExprKind::ScalarSubquery(_) => unsupported_at(
            "scalar subquery Apply execution is not supported yet",
            position,
        ),
        ParsedExprKind::Exists { .. } => {
            unsupported_at("EXISTS Apply execution is not supported yet", position)
        }
        ParsedExprKind::InSubquery { .. } => {
            unsupported_at("IN subquery Apply execution is not supported yet", position)
        }
        ParsedExprKind::QuantifiedSubquery { .. } => unsupported_at(
            "ANY/ALL subquery Apply execution is not supported yet",
            position,
        ),
        ParsedExprKind::RowSubquery { .. } => unsupported_at(
            "row subquery Apply execution is not supported in this context",
            position,
        ),
        ParsedExprKind::Aggregate {
            function,
            argument,
            distinct,
            filter,
        } => {
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
            if distinct
                && argument
                    .as_ref()
                    .is_some_and(|argument| argument.data_type == ScalarType::Json)
            {
                return Err(DbError::new(
                    "42883",
                    "could not identify an equality operator for type json",
                )
                .with_position_opt(position));
            }
            let filter = filter
                .map(|filter| bind_expr_multi(*filter, inputs, Some(&ScalarType::Boolean), false))
                .transpose()?;
            if filter
                .as_ref()
                .is_some_and(|filter| filter.data_type != ScalarType::Boolean)
            {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "aggregate FILTER predicate must be boolean",
                )
                .with_position_opt(position));
            }
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
                    distinct,
                    filter: filter.map(Box::new),
                },
                data_type,
                nullable,
            })
        }
        ParsedExprKind::Window { .. }
        | ParsedExprKind::NamedWindow { .. }
        | ParsedExprKind::WindowValue { .. } => Err(DbError::internal(
            "window expression reached binding before lowering",
        )),
    }
}

fn bind_scalar_function_multi(
    function: ScalarFunction,
    arguments: Vec<ParsedExpr>,
    inputs: &[InputColumn],
    expected: Option<&ScalarType>,
    allow_aggregate: bool,
    position: Option<usize>,
) -> Result<BoundExpr> {
    let inferred = infer_scalar_function_type(
        function,
        &arguments,
        |argument| infer_multi_type(argument, inputs),
        position,
    )?;
    if let (Some(actual), Some(expected)) = (&inferred, expected) {
        ensure_types_compatible(actual, expected, position)?;
    }
    let common = matches!(
        function,
        ScalarFunction::Coalesce
            | ScalarFunction::NullIf
            | ScalarFunction::Greatest
            | ScalarFunction::Least
    )
    .then_some(inferred.as_ref())
    .flatten();
    let arguments = arguments
        .into_iter()
        .enumerate()
        .map(|(index, argument)| {
            let expected = scalar_function_argument_type(function, index, common);
            bind_expr_multi(argument, inputs, expected, allow_aggregate)
        })
        .collect::<Result<Vec<_>>>()?;
    let (data_type, nullable) = validate_bound_scalar_function(function, &arguments, position)?;
    Ok(BoundExpr {
        kind: BoundExprKind::Function {
            function,
            arguments,
        },
        data_type,
        nullable,
    })
}

fn bind_multi_binary(
    left: ParsedExpr,
    op: BinaryOperator,
    right: ParsedExpr,
    inputs: &[InputColumn],
    position: Option<usize>,
    expected: Option<&ScalarType>,
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
    let mut operand_type = match (left_type, right_type) {
        (Some(left), Some(right)) => common_type(&left, &right).ok_or_else(|| {
            DbError::new(
                DATATYPE_MISMATCH,
                format!("operator cannot match {left:?} with {right:?}"),
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
    if is_arithmetic_operator(op) && !is_numeric(&operand_type) {
        return Err(DbError::new(
            "42883",
            format!("arithmetic operator is not defined for {operand_type:?}"),
        )
        .with_position_opt(position));
    }
    if is_arithmetic_operator(op)
        && let Some(expected) = expected
    {
        ensure_types_compatible(&operand_type, expected, position)?;
        operand_type = expected.clone();
    }
    let left = bind_expr_multi(left, inputs, Some(&operand_type), allow_aggregate)?;
    let right = bind_expr_multi(right, inputs, Some(&operand_type), allow_aggregate)?;
    Ok(BoundExpr {
        nullable: left.nullable || right.nullable,
        data_type: if is_arithmetic_operator(op) {
            operand_type
        } else {
            ScalarType::Boolean
        },
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
        ParsedExprKind::ResolvedParameter { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::Unary { op, expr } => match op {
            UnaryOperator::Not => Ok(Some(ScalarType::Boolean)),
            UnaryOperator::Negate => infer_multi_type(expr, inputs),
        },
        ParsedExprKind::Cast { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::Array { elements, .. } => {
            let mut element_type = None;
            for element in elements {
                let Some(candidate) = infer_multi_type(element, inputs)? else {
                    continue;
                };
                element_type = Some(match element_type {
                    Some(current) => common_type(&current, &candidate).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "array element types {current:?} and {candidate:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(expr.position)
                    })?,
                    None => candidate,
                });
            }
            Ok(element_type.map(|element| ScalarType::Array {
                element: Box::new(element),
            }))
        }
        ParsedExprKind::Function {
            function,
            arguments,
        } => infer_scalar_function_type(
            *function,
            arguments,
            |argument| infer_multi_type(argument, inputs),
            expr.position,
        ),
        ParsedExprKind::Binary { left, op, right } => {
            if is_arithmetic_operator(*op) {
                let left = infer_multi_type(left, inputs)?;
                let right = infer_multi_type(right, inputs)?;
                Ok(match (left, right) {
                    (Some(left), Some(right)) => common_type(&left, &right),
                    (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                    (None, None) => None,
                })
            } else {
                Ok(Some(ScalarType::Boolean))
            }
        }
        ParsedExprKind::InList { .. }
        | ParsedExprKind::Exists { .. }
        | ParsedExprKind::InSubquery { .. }
        | ParsedExprKind::QuantifiedSubquery { .. }
        | ParsedExprKind::RowSubquery { .. } => Ok(Some(ScalarType::Boolean)),
        ParsedExprKind::ScalarSubquery(_) => Ok(None),
        ParsedExprKind::ApplyValue { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::WindowValue { .. } => Ok(Some(ScalarType::Int64)),
        ParsedExprKind::Window { .. } => Err(DbError::internal(
            "window expression reached type inference before lowering",
        )),
        ParsedExprKind::NamedWindow { .. } => {
            Err(DbError::internal("named window reference was not resolved"))
        }
        ParsedExprKind::Aggregate {
            function, argument, ..
        } => match function {
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
    let local = matches
        .iter()
        .copied()
        .filter(|input| input.outer_depth == 0)
        .collect::<Vec<_>>();
    let visible = if local.is_empty() { &matches } else { &local };
    match visible.as_slice() {
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
        BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => {
            bound_expr_has_aggregate(expr)
        }
        BoundExprKind::Array { elements, .. } => elements.iter().any(bound_expr_has_aggregate),
        BoundExprKind::Function { arguments, .. } => arguments.iter().any(bound_expr_has_aggregate),
        BoundExprKind::Binary { left, right, .. } => {
            bound_expr_has_aggregate(left) || bound_expr_has_aggregate(right)
        }
        BoundExprKind::InList { expr, list, .. } => {
            bound_expr_has_aggregate(expr) || list.iter().any(bound_expr_has_aggregate)
        }
        BoundExprKind::Column { .. }
        | BoundExprKind::Literal(_)
        | BoundExprKind::Parameter { .. }
        | BoundExprKind::Correlation { .. }
        | BoundExprKind::ApplyValue { .. } => false,
    }
}

fn bound_window_input_has_aggregate(window: &BoundWindow) -> bool {
    window.arguments.iter().any(bound_expr_has_aggregate)
        || window.filter.as_ref().is_some_and(bound_expr_has_aggregate)
        || window.partition_by.iter().any(bound_expr_has_aggregate)
        || window.order_by.iter().any(|order| {
            order
                .expression
                .as_ref()
                .is_some_and(bound_expr_has_aggregate)
        })
}

fn window_ordinal_for_expr(expr: &BoundExpr, windows: &[BoundWindow]) -> Option<usize> {
    let BoundExprKind::ApplyValue { index } = expr.kind else {
        return None;
    };
    windows
        .iter()
        .position(|window| window.value_index == index)
}

fn bound_expr_has_window_slot(expr: &BoundExpr, windows: &[BoundWindow]) -> bool {
    let mut pending = vec![expr];
    while let Some(expression) = pending.pop() {
        match &expression.kind {
            BoundExprKind::ApplyValue { index }
                if windows.iter().any(|window| window.value_index == *index) =>
            {
                return true;
            }
            BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => pending.push(expr),
            BoundExprKind::Array { elements, .. } => pending.extend(elements),
            BoundExprKind::Function { arguments, .. } => pending.extend(arguments),
            BoundExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundExprKind::InList { expr, list, .. } => {
                pending.extend(list);
                pending.push(expr);
            }
            BoundExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            BoundExprKind::Column { .. }
            | BoundExprKind::Literal(_)
            | BoundExprKind::Parameter { .. }
            | BoundExprKind::Correlation { .. }
            | BoundExprKind::ApplyValue { .. } => {}
        }
    }
    false
}

fn remap_grouped_window_inputs(
    windows: &mut [BoundWindow],
    projection: &[BoundProjection],
    group_by: &[BoundExpr],
) -> Result<()> {
    let base_projection = projection
        .iter()
        .filter(|projection| window_ordinal_for_expr(&projection.expr, windows).is_none())
        .collect::<Vec<_>>();
    for window in windows {
        for argument in &mut window.arguments {
            *argument = remap_grouped_window_expr(argument, &base_projection, group_by)?;
        }
        if let Some(filter) = &mut window.filter {
            *filter = remap_grouped_window_expr(filter, &base_projection, group_by)?;
        }
        for expression in &mut window.partition_by {
            *expression = remap_grouped_window_expr(expression, &base_projection, group_by)?;
        }
        for order in &mut window.order_by {
            let expression = if let Some(expression) = &order.expression {
                expression.clone()
            } else {
                base_projection
                    .iter()
                    .find_map(|projection| match projection.expr.kind {
                        BoundExprKind::Column { index } if index == order.column_index => {
                            Some(projection.expr.clone())
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        DbError::new(
                            FEATURE_NOT_SUPPORTED,
                            "grouped window ORDER BY expression must appear in the select list",
                        )
                    })?
            };
            let expression = remap_grouped_window_expr(&expression, &base_projection, group_by)?;
            if let BoundExprKind::Column { index } = expression.kind {
                order.column_index = index;
                order.expression = None;
            } else {
                order.column_index = usize::MAX;
                order.expression = Some(expression);
            }
        }
    }
    Ok(())
}

fn remap_grouped_window_expr(
    expression: &BoundExpr,
    base_projection: &[&BoundProjection],
    group_by: &[BoundExpr],
) -> Result<BoundExpr> {
    validate_grouped_expr(expression, group_by)?;
    if let Some((index, projected)) = base_projection
        .iter()
        .enumerate()
        .find(|(_, projected)| projected.expr == *expression)
    {
        return Ok(BoundExpr {
            kind: BoundExprKind::Column { index },
            data_type: projected.expr.data_type.clone(),
            nullable: projected.expr.nullable,
        });
    }
    if matches!(
        expression.kind,
        BoundExprKind::Literal(_)
            | BoundExprKind::Parameter { .. }
            | BoundExprKind::Correlation { .. }
    ) {
        return Ok(expression.clone());
    }
    Err(DbError::new(
        FEATURE_NOT_SUPPORTED,
        "grouped window input expression must appear in the select list",
    ))
}

fn validate_grouped_expr(expr: &BoundExpr, group_by: &[BoundExpr]) -> Result<()> {
    if group_by.iter().any(|group| group == expr) {
        return Ok(());
    }
    match &expr.kind {
        BoundExprKind::Aggregate { .. }
        | BoundExprKind::Literal(_)
        | BoundExprKind::Parameter { .. }
        | BoundExprKind::Correlation { .. }
        | BoundExprKind::ApplyValue { .. } => Ok(()),
        BoundExprKind::Column { .. } => Err(DbError::new(
            "42803",
            "column must appear in GROUP BY or be used in an aggregate function",
        )),
        BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => {
            validate_grouped_expr(expr, group_by)
        }
        BoundExprKind::Array { elements, .. } => {
            for element in elements {
                validate_grouped_expr(element, group_by)?;
            }
            Ok(())
        }
        BoundExprKind::Function { arguments, .. } => {
            for argument in arguments {
                validate_grouped_expr(argument, group_by)?;
            }
            Ok(())
        }
        BoundExprKind::Binary { left, right, .. } => {
            validate_grouped_expr(left, group_by)?;
            validate_grouped_expr(right, group_by)
        }
        BoundExprKind::InList { expr, list, .. } => {
            validate_grouped_expr(expr, group_by)?;
            for candidate in list {
                validate_grouped_expr(candidate, group_by)?;
            }
            Ok(())
        }
    }
}

fn bind_select(input: SelectInput, catalog: &Catalog, view_depth: usize) -> Result<BoundStatement> {
    let SelectInput {
        table_name,
        projection,
        filter,
        order_by,
        offset,
        limit,
    } = input;
    let (schema_name, relation_name, _) = split_table_name(&table_name)?;
    if let Some(view) = catalog.view(&schema_name, &relation_name) {
        return bind_view_select(
            view,
            SelectInput {
                table_name,
                projection,
                filter,
                order_by,
                offset,
                limit,
            },
            catalog,
            view_depth,
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
        .map(|order| bind_simple_order(order, &bound_projection, &table))
        .collect::<Result<Vec<_>>>()?;
    let limit = limit
        .map(|expr| bind_expr(expr, Some(&table), Some(&ScalarType::Int64)))
        .transpose()?;
    let offset = offset
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
        offset,
        limit,
    })
}

fn bind_view_select(
    view: &ViewDefinition,
    input: SelectInput,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let SelectInput {
        table_name: _,
        projection,
        filter,
        order_by,
        offset,
        limit,
    } = input;
    if filter.is_some() || !order_by.is_empty() || offset.is_some() || limit.is_some() {
        return unsupported(
            "WHERE, ORDER BY, OFFSET, and LIMIT on views are not supported in this milestone",
        );
    }
    let source = bind_view_source(view, catalog, view_depth)?;
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

fn bind_view_source(
    view: &ViewDefinition,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
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
                offset: None,
                limit: None,
            }
        }
    };
    let source_schema = bound_query_schema(&source)?;
    if source_schema.fields.len() != view.output.fields.len()
        || source_schema
            .fields
            .iter()
            .zip(&view.output.fields)
            .any(|(source, target)| source.data_type != target.data_type)
    {
        return Err(DbError::new(
            "42P16",
            "stored view query output no longer matches its catalog definition",
        ));
    }
    Ok(source)
}

fn bind_update(
    table_name: ParsedObjectName,
    assignments: Vec<(ParsedIdentifier, ParsedExpr)>,
    filter: Option<ParsedExpr>,
    returning: Vec<ParsedProjection>,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let relation = resolve_dml_relation(&table_name, CatalogTriggerEvent::Update, catalog)?;
    let table = relation.scope;
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
    let returning = bind_returning(returning, &table)?;
    let filter = filter
        .map(|expr| bind_boolean_expr(expr, &table))
        .transpose()?;
    match relation.target {
        DmlTarget::Table(table_id) => Ok(BoundStatement::Update {
            table_id,
            assignments,
            filter,
            returning,
        }),
        DmlTarget::View(view_id) => {
            let view = catalog
                .view_by_id(view_id)
                .ok_or_else(|| DbError::internal("bound view target disappeared"))?;
            Ok(BoundStatement::ViewUpdate {
                view_id,
                source: Box::new(bind_view_source(view, catalog, view_depth)?),
                assignments,
                filter,
                returning,
            })
        }
    }
}

fn bind_delete(
    table_name: ParsedObjectName,
    filter: Option<ParsedExpr>,
    returning: Vec<ParsedProjection>,
    catalog: &Catalog,
    view_depth: usize,
) -> Result<BoundStatement> {
    let relation = resolve_dml_relation(&table_name, CatalogTriggerEvent::Delete, catalog)?;
    let table = relation.scope;
    let returning = bind_returning(returning, &table)?;
    let filter = filter
        .map(|expr| bind_boolean_expr(expr, &table))
        .transpose()?;
    match relation.target {
        DmlTarget::Table(table_id) => Ok(BoundStatement::Delete {
            table_id,
            filter,
            returning,
        }),
        DmlTarget::View(view_id) => {
            let view = catalog
                .view_by_id(view_id)
                .ok_or_else(|| DbError::internal("bound view target disappeared"))?;
            Ok(BoundStatement::ViewDelete {
                view_id,
                source: Box::new(bind_view_source(view, catalog, view_depth)?),
                filter,
                returning,
            })
        }
    }
}

fn bind_returning(
    returning: Vec<ParsedProjection>,
    table: &TableDefinition,
) -> Result<Option<BoundReturning>> {
    if returning.is_empty() {
        return Ok(None);
    }
    let mut projection = Vec::new();
    for item in returning {
        match item {
            ParsedProjection::Wildcard => {
                for (index, column) in table.columns().iter().enumerate() {
                    projection.push(BoundProjection {
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
                let bound = bind_expr(expr, Some(table), None)?;
                if bound_expr_has_aggregate(&bound) {
                    return Err(DbError::new(
                        "42803",
                        "aggregate functions are not allowed in RETURNING",
                    ));
                }
                projection.push(BoundProjection {
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
    let schema = Schema::new(
        projection
            .iter()
            .map(|projection| projection.field.clone())
            .collect(),
    );
    Ok(Some(BoundReturning { schema, projection }))
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
        ParsedExprKind::ResolvedParameter { index, data_type } => {
            let declared = parameter_types.get(&index);
            if let Some(declared) = declared {
                ensure_types_compatible(&data_type, declared, position)?;
            }
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::Parameter { index },
                data_type,
                nullable: true,
            })
        }
        ParsedExprKind::ApplyValue {
            index,
            data_type,
            nullable,
        } => {
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            Ok(BoundExpr {
                kind: BoundExprKind::ApplyValue { index },
                data_type,
                nullable,
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
        ParsedExprKind::Cast {
            expr, data_type, ..
        } => {
            if let Some(expected) = expected {
                ensure_types_compatible(&data_type, expected, position)?;
            }
            let source_type = infer_expr_type(&expr, table, parameter_types)?;
            let bound = bind_expr_with_parameter_types(
                *expr,
                table,
                source_type.is_none().then_some(&data_type),
                parameter_types,
            )?;
            ensure_explicit_cast_supported(&bound.data_type, &data_type, position)?;
            let nullable = bound.nullable;
            Ok(BoundExpr {
                kind: BoundExprKind::Cast {
                    expr: Box::new(bound),
                },
                data_type,
                nullable,
            })
        }
        ParsedExprKind::Array {
            elements,
            dimensions,
        } => {
            let expected_element = match expected {
                Some(ScalarType::Array { element }) => Some(element.as_ref().clone()),
                Some(expected) => {
                    return Err(DbError::new(
                        DATATYPE_MISMATCH,
                        format!("array cannot be assigned to {expected:?}"),
                    )
                    .with_position_opt(position));
                }
                None => None,
            };
            let mut element_type = expected_element;
            for element in &elements {
                let Some(candidate) = infer_expr_type(element, table, parameter_types)? else {
                    continue;
                };
                element_type = Some(match element_type {
                    Some(current) => common_type_with_literal(&current, &candidate, None, element)
                        .ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "array element types {current:?} and {candidate:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(position)
                    })?,
                    None => candidate,
                });
            }
            let element_type = element_type.ok_or_else(|| {
                DbError::new(
                    INDETERMINATE_DATATYPE,
                    "cannot determine type of empty array",
                )
                .with_hint("Explicitly cast the array, for example ARRAY[]::integer[].")
                .with_position_opt(position)
            })?;
            if matches!(element_type, ScalarType::Array { .. }) {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "nested array values must use one flattened PostgreSQL array type",
                )
                .with_position_opt(position));
            }
            let elements = elements
                .into_iter()
                .map(|element| {
                    bind_expr_with_parameter_types(
                        element,
                        table,
                        Some(&element_type),
                        parameter_types,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(BoundExpr {
                kind: BoundExprKind::Array {
                    elements,
                    dimensions,
                },
                data_type: ScalarType::Array {
                    element: Box::new(element_type),
                },
                nullable: false,
            })
        }
        ParsedExprKind::Function {
            function,
            arguments,
        } => bind_scalar_function(
            function,
            arguments,
            table,
            expected,
            parameter_types,
            position,
        ),
        ParsedExprKind::Binary { left, op, right } => bind_binary(
            *left,
            op,
            *right,
            table,
            position,
            expected,
            parameter_types,
        ),
        ParsedExprKind::InList {
            expr,
            list,
            negated,
        } => {
            if expected.is_some_and(|expected| expected != &ScalarType::Boolean) {
                return Err(DbError::new(
                    DATATYPE_MISMATCH,
                    "IN predicate produces a boolean result",
                )
                .with_position_opt(position));
            }
            let mut operand_type = infer_expr_type(&expr, table, parameter_types)?;
            for candidate in &list {
                let Some(candidate_type) = infer_expr_type(candidate, table, parameter_types)?
                else {
                    continue;
                };
                operand_type = Some(match operand_type {
                    Some(current) => {
                        common_type_with_literal(&current, &candidate_type, Some(&expr), candidate)
                            .ok_or_else(|| {
                                DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "IN types {current:?} and {candidate_type:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(position)
                            })?
                    }
                    None => candidate_type,
                });
            }
            let operand_type = operand_type.ok_or_else(|| {
                DbError::new(
                    INDETERMINATE_DATATYPE,
                    "could not determine data type of IN operands",
                )
                .with_position_opt(position)
            })?;
            if operand_type == ScalarType::Json {
                return Err(DbError::new(
                    "42883",
                    "could not identify an equality operator for type json",
                )
                .with_position_opt(position));
            }
            let expr =
                bind_expr_with_parameter_types(*expr, table, Some(&operand_type), parameter_types)?;
            let list = list
                .into_iter()
                .map(|candidate| {
                    bind_expr_with_parameter_types(
                        candidate,
                        table,
                        Some(&operand_type),
                        parameter_types,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let nullable = expr.nullable || list.iter().any(|candidate| candidate.nullable);
            Ok(BoundExpr {
                kind: BoundExprKind::InList {
                    expr: Box::new(expr),
                    list,
                    negated,
                },
                data_type: ScalarType::Boolean,
                nullable,
            })
        }
        ParsedExprKind::ScalarSubquery(_) => unsupported_at(
            "scalar subquery Apply execution is not supported yet",
            position,
        ),
        ParsedExprKind::Exists { .. } => {
            unsupported_at("EXISTS Apply execution is not supported yet", position)
        }
        ParsedExprKind::InSubquery { .. } => {
            unsupported_at("IN subquery Apply execution is not supported yet", position)
        }
        ParsedExprKind::QuantifiedSubquery { .. } => unsupported_at(
            "ANY/ALL subquery Apply execution is not supported yet",
            position,
        ),
        ParsedExprKind::RowSubquery { .. } => unsupported_at(
            "row subquery Apply execution is not supported in this context",
            position,
        ),
        ParsedExprKind::Aggregate { .. } => {
            unsupported_at("aggregate is not valid in this statement", position)
        }
        ParsedExprKind::Window { .. }
        | ParsedExprKind::NamedWindow { .. }
        | ParsedExprKind::WindowValue { .. } => Err(DbError::new(
            "42P20",
            "window functions are not allowed in this statement",
        )
        .with_position_opt(position)),
    }
}

fn bind_scalar_function(
    function: ScalarFunction,
    arguments: Vec<ParsedExpr>,
    table: Option<&TableDefinition>,
    expected: Option<&ScalarType>,
    parameter_types: &BTreeMap<usize, ScalarType>,
    position: Option<usize>,
) -> Result<BoundExpr> {
    let inferred = infer_scalar_function_type(
        function,
        &arguments,
        |argument| infer_expr_type(argument, table, parameter_types),
        position,
    )?;
    if let (Some(actual), Some(expected)) = (&inferred, expected) {
        ensure_types_compatible(actual, expected, position)?;
    }
    let common = matches!(
        function,
        ScalarFunction::Coalesce
            | ScalarFunction::NullIf
            | ScalarFunction::Greatest
            | ScalarFunction::Least
    )
    .then_some(inferred.as_ref())
    .flatten();
    let arguments = arguments
        .into_iter()
        .enumerate()
        .map(|(index, argument)| {
            let expected = scalar_function_argument_type(function, index, common);
            bind_expr_with_parameter_types(argument, table, expected, parameter_types)
        })
        .collect::<Result<Vec<_>>>()?;
    let (data_type, nullable) = validate_bound_scalar_function(function, &arguments, position)?;
    Ok(BoundExpr {
        kind: BoundExprKind::Function {
            function,
            arguments,
        },
        data_type,
        nullable,
    })
}

fn bind_binary(
    left: ParsedExpr,
    op: BinaryOperator,
    right: ParsedExpr,
    table: Option<&TableDefinition>,
    position: Option<usize>,
    expected: Option<&ScalarType>,
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
    let mut operand_type = match (left_type, right_type) {
        (Some(left_type), Some(right_type)) => {
            common_type_with_literal(&left_type, &right_type, Some(&left), &right).ok_or_else(
                || {
                    DbError::new(
                        DATATYPE_MISMATCH,
                        format!("operator cannot match {left_type:?} with {right_type:?}"),
                    )
                    .with_position_opt(position)
                },
            )?
        }
        (Some(data_type), None) | (None, Some(data_type)) => data_type,
        (None, None) => {
            return Err(DbError::new(
                INDETERMINATE_DATATYPE,
                "could not determine comparison operand types",
            )
            .with_position_opt(position));
        }
    };
    if is_arithmetic_operator(op) && !is_numeric(&operand_type) {
        return Err(DbError::new(
            "42883",
            format!("arithmetic operator is not defined for {operand_type:?}"),
        )
        .with_position_opt(position));
    }
    if is_arithmetic_operator(op)
        && let Some(expected) = expected
    {
        ensure_types_compatible(&operand_type, expected, position)?;
        operand_type = expected.clone();
    }
    let left = bind_expr_with_parameter_types(left, table, Some(&operand_type), parameter_types)?;
    let right = bind_expr_with_parameter_types(right, table, Some(&operand_type), parameter_types)?;
    let nullable = left.nullable || right.nullable;
    Ok(BoundExpr {
        kind: BoundExprKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        data_type: if is_arithmetic_operator(op) {
            operand_type
        } else {
            ScalarType::Boolean
        },
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
        ParsedExprKind::ResolvedParameter { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::Unary { op, expr: inner } => match op {
            UnaryOperator::Not => Ok(Some(ScalarType::Boolean)),
            UnaryOperator::Negate => infer_expr_type(inner, table, parameter_types),
        },
        ParsedExprKind::Cast { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::Array { elements, .. } => {
            let mut element_type = None;
            for element in elements {
                let Some(candidate) = infer_expr_type(element, table, parameter_types)? else {
                    continue;
                };
                element_type = Some(match element_type {
                    Some(current) => common_type(&current, &candidate).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "array element types {current:?} and {candidate:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(expr.position)
                    })?,
                    None => candidate,
                });
            }
            Ok(element_type.map(|element| ScalarType::Array {
                element: Box::new(element),
            }))
        }
        ParsedExprKind::Function {
            function,
            arguments,
        } => infer_scalar_function_type(
            *function,
            arguments,
            |argument| infer_expr_type(argument, table, parameter_types),
            expr.position,
        ),
        ParsedExprKind::Binary { left, op, right } => {
            if is_arithmetic_operator(*op) {
                let left = infer_expr_type(left, table, parameter_types)?;
                let right = infer_expr_type(right, table, parameter_types)?;
                Ok(match (left, right) {
                    (Some(left), Some(right)) => common_type(&left, &right),
                    (Some(data_type), None) | (None, Some(data_type)) => Some(data_type),
                    (None, None) => None,
                })
            } else {
                Ok(Some(ScalarType::Boolean))
            }
        }
        ParsedExprKind::InList { .. }
        | ParsedExprKind::Exists { .. }
        | ParsedExprKind::InSubquery { .. }
        | ParsedExprKind::QuantifiedSubquery { .. }
        | ParsedExprKind::RowSubquery { .. } => Ok(Some(ScalarType::Boolean)),
        ParsedExprKind::ScalarSubquery(_) => Ok(None),
        ParsedExprKind::ApplyValue { data_type, .. } => Ok(Some(data_type.clone())),
        ParsedExprKind::WindowValue { .. } => Ok(Some(ScalarType::Int64)),
        ParsedExprKind::Window { .. } => Err(DbError::new(
            "42P20",
            "window functions are not allowed in this statement",
        )
        .with_position_opt(expr.position)),
        ParsedExprKind::NamedWindow { .. } => Err(DbError::new(
            "42704",
            "named window reference was not resolved",
        )
        .with_position_opt(expr.position)),
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
                if let (ScalarType::Enum { .. }, Value::Text(label)) = (expected, &value) {
                    return Err(DbError::new(
                        "22P02",
                        format!("invalid input value for enum: {label}"),
                    )
                    .with_position_opt(position));
                }
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

fn resolve_trigger_target(name: &ParsedObjectName, catalog: &Catalog) -> Result<TriggerTarget> {
    let (schema_name, relation_name, position) = split_table_name(name)?;
    let schema = catalog.schema(&schema_name).ok_or_else(|| {
        DbError::new(
            UNDEFINED_SCHEMA,
            format!("schema {schema_name} does not exist"),
        )
        .with_position_opt(position)
    })?;
    if let Some(table) = schema.table(&relation_name) {
        return Ok(TriggerTarget::Table(table.id));
    }
    if let Some(view) = schema.view(&relation_name) {
        return Ok(TriggerTarget::View(view.id));
    }
    Err(DbError::new(
        UNDEFINED_TABLE,
        format!("relation {schema_name}.{relation_name} does not exist"),
    )
    .with_position_opt(position))
}

fn trigger_target_name(target: TriggerTarget, catalog: &Catalog) -> Result<&Identifier> {
    match target {
        TriggerTarget::Table(table_id) => catalog
            .table_by_id(table_id)
            .map(|table| &table.name)
            .ok_or_else(|| DbError::internal("bound trigger table disappeared")),
        TriggerTarget::View(view_id) => catalog
            .view_by_id(view_id)
            .map(|view| &view.name)
            .ok_or_else(|| DbError::internal("bound trigger view disappeared")),
    }
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
    if let ParsedExprKind::Function { function, .. } = &expr.kind {
        return match function {
            ScalarFunction::Version => "version",
            ScalarFunction::CurrentDatabase => "current_database",
            ScalarFunction::CurrentUser => "current_user",
            ScalarFunction::SessionUser => "session_user",
            _ => "?column?",
        }
        .to_owned();
    }
    "?column?".to_owned()
}

fn infer_scalar_function_type<F>(
    function: ScalarFunction,
    arguments: &[ParsedExpr],
    mut infer: F,
    position: Option<usize>,
) -> Result<Option<ScalarType>>
where
    F: FnMut(&ParsedExpr) -> Result<Option<ScalarType>>,
{
    match function {
        ScalarFunction::Version
        | ScalarFunction::CurrentDatabase
        | ScalarFunction::CurrentUser
        | ScalarFunction::SessionUser
        | ScalarFunction::CurrentSetting
        | ScalarFunction::Lower
        | ScalarFunction::Upper
        | ScalarFunction::Concat
        | ScalarFunction::Substring
        | ScalarFunction::Btrim
        | ScalarFunction::Ltrim
        | ScalarFunction::Rtrim
        | ScalarFunction::Replace
        | ScalarFunction::JsonbTypeof => Ok(Some(ScalarType::Text)),
        ScalarFunction::CharacterLength
        | ScalarFunction::OctetLength
        | ScalarFunction::ArrayLength
        | ScalarFunction::Cardinality
        | ScalarFunction::Strpos => Ok(Some(ScalarType::Int32)),
        ScalarFunction::Abs => infer(&arguments[0]),
        ScalarFunction::Coalesce
        | ScalarFunction::NullIf
        | ScalarFunction::Greatest
        | ScalarFunction::Least => {
            let mut common = None;
            for argument in arguments {
                let Some(candidate) = infer(argument)? else {
                    continue;
                };
                common = Some(match common {
                    Some(current) => common_type(&current, &candidate).ok_or_else(|| {
                        DbError::new(
                            DATATYPE_MISMATCH,
                            format!(
                                "function argument types {current:?} and {candidate:?} cannot be matched"
                            ),
                        )
                        .with_position_opt(position)
                    })?,
                    None => candidate,
                });
            }
            Ok(common)
        }
    }
}

fn scalar_function_argument_type(
    function: ScalarFunction,
    index: usize,
    common: Option<&ScalarType>,
) -> Option<&ScalarType> {
    match function {
        ScalarFunction::Version
        | ScalarFunction::CurrentDatabase
        | ScalarFunction::CurrentUser
        | ScalarFunction::SessionUser => None,
        ScalarFunction::CurrentSetting if index == 0 => Some(&ScalarType::Text),
        ScalarFunction::CurrentSetting => Some(&ScalarType::Boolean),
        ScalarFunction::Lower
        | ScalarFunction::Upper
        | ScalarFunction::Btrim
        | ScalarFunction::Ltrim
        | ScalarFunction::Rtrim
        | ScalarFunction::Replace
        | ScalarFunction::Strpos => Some(&ScalarType::Text),
        ScalarFunction::Substring if index == 0 => Some(&ScalarType::Text),
        ScalarFunction::Substring => Some(&ScalarType::Int32),
        ScalarFunction::JsonbTypeof => Some(&ScalarType::Jsonb),
        ScalarFunction::ArrayLength if index == 1 => Some(&ScalarType::Int32),
        ScalarFunction::Coalesce
        | ScalarFunction::NullIf
        | ScalarFunction::Greatest
        | ScalarFunction::Least => common,
        ScalarFunction::CharacterLength
        | ScalarFunction::OctetLength
        | ScalarFunction::Abs
        | ScalarFunction::Concat
        | ScalarFunction::ArrayLength
        | ScalarFunction::Cardinality => None,
    }
}

fn validate_bound_scalar_function(
    function: ScalarFunction,
    arguments: &[BoundExpr],
    position: Option<usize>,
) -> Result<(ScalarType, bool)> {
    let invalid = |message: String| DbError::new("42883", message).with_position_opt(position);
    match function {
        ScalarFunction::Version
        | ScalarFunction::CurrentDatabase
        | ScalarFunction::CurrentUser
        | ScalarFunction::SessionUser => Ok((ScalarType::Text, false)),
        ScalarFunction::CurrentSetting => Ok((ScalarType::Text, true)),
        ScalarFunction::Lower | ScalarFunction::Upper => {
            if !is_textual(&arguments[0].data_type) {
                return Err(invalid(format!(
                    "function {function:?} requires a textual argument"
                )));
            }
            Ok((ScalarType::Text, arguments[0].nullable))
        }
        ScalarFunction::CharacterLength | ScalarFunction::OctetLength => {
            if !is_textual(&arguments[0].data_type) && arguments[0].data_type != ScalarType::Binary
            {
                return Err(invalid(format!(
                    "function {function:?} requires text or bytea"
                )));
            }
            Ok((ScalarType::Int32, arguments[0].nullable))
        }
        ScalarFunction::Abs => {
            if !is_numeric(&arguments[0].data_type) {
                return Err(invalid("ABS requires a numeric argument".to_owned()));
            }
            Ok((arguments[0].data_type.clone(), arguments[0].nullable))
        }
        ScalarFunction::Coalesce => {
            let data_type = arguments
                .first()
                .map(|argument| argument.data_type.clone())
                .ok_or_else(|| invalid("COALESCE requires an argument".to_owned()))?;
            Ok((
                data_type,
                arguments.iter().all(|argument| argument.nullable),
            ))
        }
        ScalarFunction::NullIf => Ok((arguments[0].data_type.clone(), true)),
        ScalarFunction::Concat => Ok((ScalarType::Text, false)),
        ScalarFunction::Substring => Ok((
            ScalarType::Text,
            arguments.iter().any(|argument| argument.nullable),
        )),
        ScalarFunction::Btrim | ScalarFunction::Ltrim | ScalarFunction::Rtrim => {
            if arguments
                .iter()
                .any(|argument| !is_textual(&argument.data_type))
            {
                return Err(invalid(format!(
                    "function {function:?} requires textual arguments"
                )));
            }
            Ok((
                ScalarType::Text,
                arguments.iter().any(|argument| argument.nullable),
            ))
        }
        ScalarFunction::Replace | ScalarFunction::Strpos => {
            if arguments
                .iter()
                .any(|argument| !is_textual(&argument.data_type))
            {
                return Err(invalid(format!(
                    "function {function:?} requires textual arguments"
                )));
            }
            Ok((
                if function == ScalarFunction::Strpos {
                    ScalarType::Int32
                } else {
                    ScalarType::Text
                },
                arguments.iter().any(|argument| argument.nullable),
            ))
        }
        ScalarFunction::Greatest | ScalarFunction::Least => {
            let data_type = arguments
                .first()
                .map(|argument| argument.data_type.clone())
                .ok_or_else(|| invalid(format!("function {function:?} requires an argument")))?;
            if arguments
                .iter()
                .any(|argument| argument.data_type != data_type)
            {
                return Err(invalid(format!(
                    "function {function:?} arguments must have a common type"
                )));
            }
            Ok((
                data_type,
                arguments.iter().all(|argument| argument.nullable),
            ))
        }
        ScalarFunction::JsonbTypeof => {
            if arguments[0].data_type != ScalarType::Jsonb {
                return Err(invalid("JSONB_TYPEOF requires a jsonb argument".to_owned()));
            }
            Ok((ScalarType::Text, arguments[0].nullable))
        }
        ScalarFunction::ArrayLength | ScalarFunction::Cardinality => {
            if !matches!(arguments[0].data_type, ScalarType::Array { .. }) {
                return Err(invalid(format!(
                    "function {function:?} requires an array argument"
                )));
            }
            Ok((ScalarType::Int32, true))
        }
    }
}

fn common_type(left: &ScalarType, right: &ScalarType) -> Option<ScalarType> {
    if left == right {
        return Some(left.clone());
    }
    if matches!(left, ScalarType::Oid)
        && matches!(
            right,
            ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64
        )
        || matches!(right, ScalarType::Oid)
            && matches!(
                left,
                ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64
            )
    {
        return Some(ScalarType::Oid);
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

fn common_type_with_literal(
    left: &ScalarType,
    right: &ScalarType,
    left_expr: Option<&ParsedExpr>,
    right_expr: &ParsedExpr,
) -> Option<ScalarType> {
    common_type(left, right).or_else(|| match (left, right) {
        (ScalarType::Enum { .. }, ScalarType::Text) if is_unknown_text_literal(right_expr) => {
            Some(left.clone())
        }
        (ScalarType::Text, ScalarType::Enum { .. })
            if left_expr.is_some_and(is_unknown_text_literal) =>
        {
            Some(right.clone())
        }
        (ScalarType::Oid, ScalarType::Text) if is_unknown_text_literal(right_expr) => {
            Some(ScalarType::Oid)
        }
        (ScalarType::Text, ScalarType::Oid) if left_expr.is_some_and(is_unknown_text_literal) => {
            Some(ScalarType::Oid)
        }
        (
            ScalarType::Array {
                element: left_element,
            },
            ScalarType::Array {
                element: right_element,
            },
        ) if is_unknown_text_literal(right_expr)
            && matches!(left_element.as_ref(), ScalarType::Enum { .. })
            && matches!(right_element.as_ref(), ScalarType::Text) =>
        {
            Some(left.clone())
        }
        (
            ScalarType::Array {
                element: left_element,
            },
            ScalarType::Array {
                element: right_element,
            },
        ) if left_expr.is_some_and(is_unknown_text_literal)
            && matches!(left_element.as_ref(), ScalarType::Text)
            && matches!(right_element.as_ref(), ScalarType::Enum { .. }) =>
        {
            Some(right.clone())
        }
        _ => None,
    })
}

fn is_unknown_text_literal(expression: &ParsedExpr) -> bool {
    match &expression.kind {
        ParsedExprKind::Literal(Value::Text(_) | Value::Null) => true,
        ParsedExprKind::Array { elements, .. } => elements.iter().all(is_unknown_text_literal),
        _ => false,
    }
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

fn ensure_explicit_cast_supported(
    source: &ScalarType,
    target: &ScalarType,
    position: Option<usize>,
) -> Result<()> {
    let supported = source == target
        || (is_numeric(source) && is_numeric(target))
        || is_textual(target)
        || (is_textual(source)
            && matches!(
                target,
                ScalarType::Boolean
                    | ScalarType::Int16
                    | ScalarType::Int32
                    | ScalarType::Int64
                    | ScalarType::Oid
                    | ScalarType::Float32
                    | ScalarType::Float64
                    | ScalarType::Decimal { .. }
                    | ScalarType::Binary
                    | ScalarType::Date
                    | ScalarType::Time
                    | ScalarType::Timestamp { .. }
                    | ScalarType::Interval
                    | ScalarType::Json
                    | ScalarType::Jsonb
                    | ScalarType::Uuid
                    | ScalarType::Enum { .. }
            ))
        || matches!(
            (source, target),
            (
                ScalarType::Date,
                ScalarType::Timestamp {
                    with_timezone: false
                }
            ) | (
                ScalarType::Timestamp { .. },
                ScalarType::Date | ScalarType::Time
            ) | (ScalarType::Timestamp { .. }, ScalarType::Timestamp { .. })
                | (ScalarType::Json, ScalarType::Jsonb)
                | (ScalarType::Jsonb, ScalarType::Json)
                | (
                    ScalarType::Oid,
                    ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64
                )
                | (
                    ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64,
                    ScalarType::Oid
                )
        )
        || matches!(
            (source, target),
            (ScalarType::Array { .. }, ScalarType::Array { .. })
        );
    if supported {
        Ok(())
    } else {
        Err(DbError::new(
            "42846",
            format!("cannot cast type {source:?} to {target:?}"),
        )
        .with_position_opt(position))
    }
}

const fn is_arithmetic_operator(operator: BinaryOperator) -> bool {
    matches!(
        operator,
        BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
    )
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
        ScalarType::Name
            | ScalarType::InternalChar
            | ScalarType::Char { .. }
            | ScalarType::Varchar { .. }
            | ScalarType::Text
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
                        declared_type: None,
                        nullable: false,
                        primary_key: true,
                        unique: true,
                        default: None,
                    },
                    NewColumn {
                        name: Identifier::unquoted("title"),
                        data_type: ScalarType::Text,
                        declared_type: None,
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
    fn binds_scalar_selects_with_immutable_session_values() {
        let catalog = Catalog::default();
        let settings = BTreeMap::from([
            ("client_encoding".to_owned(), "UTF8".to_owned()),
            ("standard_conforming_strings".to_owned(), "on".to_owned()),
        ]);
        let session_values = SessionBindValues {
            version: "PostgreSQL 18 compatible OrdaDB test",
            current_database: "metadata_db",
            current_user: "alice",
            session_user: "bootstrap",
            settings: &settings,
        };
        let statement = bind_with_session(
            parse("SELECT version()").expect("parse version"),
            &catalog,
            session_values,
        )
        .expect("bind version");
        assert!(matches!(
            statement,
            BoundStatement::ScalarSelect {
                projection,
                ..
            } if matches!(projection.as_slice(), [BoundProjection {
                    expr: BoundExpr {
                        kind: BoundExprKind::Literal(Value::Text(value)),
                        data_type: ScalarType::Text,
                        nullable: false,
                    },
                    field,
                }] if value == "PostgreSQL 18 compatible OrdaDB test" && field.name == "version")
        ));

        let settings_statement = bind_with_session(
            parse(
                "SELECT current_setting('client_encoding'), \
                 current_setting('standard_conforming_strings')",
            )
            .expect("parse settings"),
            &catalog,
            session_values,
        )
        .expect("bind settings");
        let BoundStatement::ScalarSelect { projection, schema } = settings_statement else {
            panic!("expected scalar setting select");
        };
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(projection.len(), 2);
        assert!(matches!(
            projection[0].expr.kind,
            BoundExprKind::Literal(Value::Text(ref value)) if value == "UTF8"
        ));
        assert!(matches!(
            projection[1].expr.kind,
            BoundExprKind::Literal(Value::Text(ref value)) if value == "on"
        ));

        let missing_ok = bind_with_session(
            parse("SELECT current_setting('ordadb.missing', true)").expect("parse missing_ok"),
            &catalog,
            session_values,
        )
        .expect("bind missing_ok");
        assert!(matches!(
            missing_ok,
            BoundStatement::ScalarSelect { projection, .. }
                if matches!(projection.as_slice(), [BoundProjection {
                    expr: BoundExpr {
                        kind: BoundExprKind::Literal(Value::Null),
                        ..
                    },
                    ..
                }])
        ));
        let missing = bind_with_session(
            parse("SELECT current_setting('ordadb.missing')").expect("parse missing"),
            &catalog,
            session_values,
        )
        .expect_err("unknown setting");
        assert_eq!(missing.sql_state, "42704");

        let literal = bind(parse("SELECT 1").expect("parse literal"), &catalog)
            .expect("bind literal scalar select");
        assert!(matches!(
            literal,
            BoundStatement::ScalarSelect {
                projection,
                ..
            } if matches!(projection.as_slice(), [BoundProjection {
                    expr: BoundExpr {
                        kind: BoundExprKind::Literal(Value::Int32(1)),
                        ..
                    },
                    field,
                }] if field.name == "?column?")
        ));

        let missing = bind(
            parse("SELECT current_database()").expect("parse session function"),
            &catalog,
        )
        .expect_err("session function requires immutable session values");
        assert_eq!(missing.sql_state, "55000");
    }

    #[test]
    fn parses_and_binds_interval_arrays_and_explicit_casts() {
        let catalog = catalog_with_documents();
        let statement = bind(
            parse(
                "SELECT ARRAY[[1, 2], [3, 4]]::BIGINT[] AS values, \
                 INTERVAL '1 day 02:03:04.5' AS duration FROM documents",
            )
            .expect("parse typed expressions"),
            &catalog,
        )
        .expect("bind typed expressions");
        let projection = match statement {
            BoundStatement::Select { projection, .. }
            | BoundStatement::AdvancedSelect { projection, .. } => projection,
            other => panic!("unexpected statement: {other:?}"),
        };
        assert_eq!(
            projection[0].expr.data_type,
            ScalarType::Array {
                element: Box::new(ScalarType::Int64),
            }
        );
        let BoundExprKind::Cast { expr } = &projection[0].expr.kind else {
            panic!("array cast");
        };
        assert!(matches!(
            &expr.kind,
            BoundExprKind::Array { dimensions, elements }
                if dimensions == &[
                    ArrayDimension::new(2, 1),
                    ArrayDimension::new(2, 1),
                ] && elements.len() == 4
        ));
        assert_eq!(projection[1].expr.data_type, ScalarType::Interval);

        let ddl = bind(
            parse(
                "CREATE TABLE typed_values (ids BIGINT[], elapsed INTERVAL, observed TIMESTAMPTZ)",
            )
            .expect("parse typed DDL"),
            &Catalog::default(),
        )
        .expect("bind typed DDL");
        let BoundStatement::CreateTable { columns, .. } = ddl else {
            panic!("create table");
        };
        assert_eq!(
            columns[0].data_type,
            ScalarType::Array {
                element: Box::new(ScalarType::Int64),
            }
        );
        assert_eq!(columns[1].data_type, ScalarType::Interval);
        assert_eq!(
            columns[2].data_type,
            ScalarType::Timestamp {
                with_timezone: true,
            }
        );
    }

    #[test]
    fn parses_and_binds_common_scalar_functions() {
        let statement = bind(
            parse(
                "SELECT LOWER(title), UPPER(title), LENGTH(title), OCTET_LENGTH(title), \
                 ABS(id), COALESCE(title, 'fallback'), NULLIF(id, 0), \
                 CONCAT(title, id), SUBSTRING(title FROM 1 FOR 2), \
                 JSONB_TYPEOF('{\"a\":1}'::JSONB), ARRAY_LENGTH(ARRAY[[1,2],[3,4]], 2), \
                 CARDINALITY(ARRAY[1,2,3]), BTRIM('xyhelloxy', 'xy'), \
                 LTRIM('  hello'), RTRIM('hello  '), REPLACE(title, 'a', 'b'), \
                 STRPOS('åbcå', 'c'), GREATEST(id, 0), LEAST(id, 0), \
                 TRIM(BOTH 'xy' FROM 'xyhelloxy'), POSITION('c' IN 'åbcå') FROM documents",
            )
            .expect("parse scalar functions"),
            &catalog_with_documents(),
        )
        .expect("bind scalar functions");
        let projection = match statement {
            BoundStatement::Select { projection, .. }
            | BoundStatement::AdvancedSelect { projection, .. } => projection,
            other => panic!("unexpected statement: {other:?}"),
        };
        assert_eq!(projection.len(), 21);
        assert_eq!(projection[0].expr.data_type, ScalarType::Text);
        assert_eq!(projection[2].expr.data_type, ScalarType::Int32);
        assert_eq!(projection[4].expr.data_type, ScalarType::Int64);
        assert_eq!(projection[7].expr.data_type, ScalarType::Text);
        assert_eq!(projection[9].expr.data_type, ScalarType::Text);
        assert_eq!(projection[10].expr.data_type, ScalarType::Int32);
        assert_eq!(projection[12].expr.data_type, ScalarType::Text);
        assert_eq!(projection[16].expr.data_type, ScalarType::Int32);
        assert_eq!(projection[17].expr.data_type, ScalarType::Int64);
        assert_eq!(projection[18].expr.data_type, ScalarType::Int64);
        assert_eq!(projection[19].expr.data_type, ScalarType::Text);
        assert_eq!(projection[20].expr.data_type, ScalarType::Int32);

        let parameter = bind(
            parse("SELECT LOWER($1) FROM documents").expect("parse function parameter"),
            &catalog_with_documents(),
        )
        .expect("bind function parameter");
        let BoundStatement::Select { projection, .. } = parameter else {
            panic!("parameter select");
        };
        let BoundExprKind::Function { arguments, .. } = &projection[0].expr.kind else {
            panic!("lower call");
        };
        assert_eq!(arguments[0].data_type, ScalarType::Text);
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
    fn binds_lateral_derived_tables_with_left_to_right_scope() {
        let catalog = catalog_with_documents();
        let statement = parse(
            "SELECT d.id, matched.renamed_title FROM documents d \
             INNER JOIN LATERAL ( \
                 SELECT lookup.title FROM documents lookup WHERE lookup.id = d.id \
             ) AS matched(renamed_title) ON TRUE",
        )
        .expect("parse LATERAL derived table");
        let ParsedStatement::AdvancedSelect { joins, .. } = &statement else {
            panic!("parsed LATERAL advanced select");
        };
        assert!(matches!(
            joins[0].source,
            ParsedJoinSource::Derived { lateral: true, .. }
        ));

        let statement = bind(statement, &catalog).expect("bind LATERAL derived table");
        let BoundStatement::AdvancedSelect { joins, schema, .. } = statement else {
            panic!("bound LATERAL advanced select");
        };
        assert_eq!(schema.fields[1].name, "renamed_title");
        let BoundJoinSource::Derived {
            lateral,
            query,
            offset,
            width,
            ..
        } = &joins[0].source
        else {
            panic!("bound derived join source");
        };
        assert!(*lateral);
        assert_eq!((*offset, *width), (2, 1));
        let BoundStatement::AdvancedSelect {
            filter: Some(filter),
            ..
        } = query.as_ref()
        else {
            panic!("bound correlated derived query");
        };
        assert!(matches!(
            &filter.kind,
            BoundExprKind::Binary { right, .. }
                if matches!(right.kind, BoundExprKind::Correlation { depth: 1, index: 0 })
        ));

        let error = bind(
            parse(
                "SELECT d.id FROM documents d \
                 INNER JOIN (SELECT lookup.id FROM documents lookup WHERE lookup.id = d.id) \
                 AS matched ON TRUE",
            )
            .expect("parse non-LATERAL derived table"),
            &catalog,
        )
        .expect_err("non-LATERAL source cannot see its left input");
        assert_eq!(error.sql_state, UNDEFINED_COLUMN);

        let error = bind(
            parse(
                "SELECT d.id FROM documents d \
                 INNER JOIN LATERAL (SELECT lookup.id FROM documents lookup) \
                 AS matched(first, extra) ON TRUE",
            )
            .expect("parse excessive derived aliases"),
            &catalog,
        )
        .expect_err("derived alias count");
        assert_eq!(error.sql_state, SYNTAX_ERROR);
    }

    #[test]
    fn binds_postgres_aggregate_filter_predicates() {
        let catalog = catalog_with_documents();
        let statement = bind(
            parse(
                "SELECT COUNT(*) FILTER (WHERE id > $1) AS selected, \
                 SUM(id) FILTER (WHERE title = 'keep') AS total FROM documents",
            )
            .expect("parse aggregate FILTER"),
            &catalog,
        )
        .expect("bind aggregate FILTER");
        let BoundStatement::AdvancedSelect { projection, .. } = statement else {
            panic!("aggregate FILTER select");
        };
        assert!(matches!(
            projection[0].expr.kind,
            BoundExprKind::Aggregate {
                filter: Some(_),
                ..
            }
        ));
        assert!(matches!(
            projection[1].expr.kind,
            BoundExprKind::Aggregate {
                filter: Some(_),
                ..
            }
        ));

        let error = bind(
            parse("SELECT COUNT(*) FILTER (WHERE id) FROM documents")
                .expect("parse invalid aggregate FILTER"),
            &catalog,
        )
        .expect_err("non-boolean aggregate FILTER");
        assert_eq!(error.sql_state, DATATYPE_MISMATCH);
    }

    #[test]
    fn binds_postgres_distinct_aggregate_inputs() {
        let catalog = catalog_with_documents();
        let statement = bind(
            parse(
                "SELECT COUNT(DISTINCT id), SUM(DISTINCT id) FILTER (WHERE id > 0), \
                 AVG(ALL id) FROM documents",
            )
            .expect("parse DISTINCT aggregates"),
            &catalog,
        )
        .expect("bind DISTINCT aggregates");
        let BoundStatement::AdvancedSelect { projection, .. } = statement else {
            panic!("DISTINCT aggregate select");
        };
        assert!(matches!(
            projection[0].expr.kind,
            BoundExprKind::Aggregate { distinct: true, .. }
        ));
        assert!(matches!(
            projection[1].expr.kind,
            BoundExprKind::Aggregate {
                distinct: true,
                filter: Some(_),
                ..
            }
        ));
        assert!(matches!(
            projection[2].expr.kind,
            BoundExprKind::Aggregate {
                distinct: false,
                ..
            }
        ));

        let error = parse("SELECT COUNT(DISTINCT *) FROM documents")
            .expect_err("DISTINCT wildcard aggregate must fail");
        assert_eq!(error.sql_state, SYNTAX_ERROR);
    }

    #[test]
    fn binds_inline_ranking_windows_after_apply_slots() {
        let catalog = catalog_with_documents();
        let statement = bind(
            parse(
                "SELECT id, \
                 (SELECT lookup.id FROM documents lookup \
                  WHERE lookup.id = documents.id LIMIT 1) AS copied, \
                 ROW_NUMBER() OVER (PARTITION BY title ORDER BY id DESC) AS row_no, \
                 RANK() OVER (PARTITION BY title ORDER BY id DESC) AS rank_no, \
                 DENSE_RANK() OVER (PARTITION BY title ORDER BY id DESC) AS dense_no \
                 FROM documents ORDER BY row_no, id",
            )
            .expect("parse ranking windows"),
            &catalog,
        )
        .expect("bind ranking windows");
        let BoundStatement::AdvancedSelect {
            applies,
            windows,
            projection,
            order_by,
            ..
        } = statement
        else {
            panic!("ranking window select");
        };
        assert_eq!(applies.len(), 1);
        assert_eq!(windows.len(), 3);
        assert!(matches!(windows[0].function, WindowFunction::RowNumber));
        assert!(matches!(windows[1].function, WindowFunction::Rank));
        assert!(matches!(windows[2].function, WindowFunction::DenseRank));
        assert_eq!(windows[0].partition_by.len(), 1);
        assert_eq!(windows[0].order_by.len(), 1);
        assert!(!windows[0].order_by[0].ascending);
        assert!(matches!(
            projection[1].expr.kind,
            BoundExprKind::ApplyValue { index: 2 }
        ));
        for (ordinal, projection) in projection.iter().skip(2).enumerate() {
            assert!(matches!(
                projection.expr.kind,
                BoundExprKind::ApplyValue { index } if index == 3 + ordinal
            ));
            assert_eq!(projection.field.data_type, ScalarType::Int64);
            assert!(!projection.field.nullable);
        }
        assert_eq!(order_by.len(), 2);
    }

    #[test]
    fn ranking_windows_fail_closed_for_unimplemented_or_invalid_forms() {
        let catalog = catalog_with_documents();
        let named = bind(
            parse(
                "SELECT ROW_NUMBER() OVER ranked FROM documents \
             WINDOW ranked AS (ORDER BY id)",
            )
            .expect("parse named window"),
            &catalog,
        )
        .expect("bind named window");
        let BoundStatement::AdvancedSelect { windows, .. } = named else {
            panic!("named window select");
        };
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].order_by.len(), 1);

        let inherited = bind(
            parse(
                "SELECT RANK() OVER ranked FROM documents \
                 WINDOW grouped AS (PARTITION BY title), \
                        ranked AS (grouped ORDER BY id)",
            )
            .expect("parse inherited window"),
            &catalog,
        )
        .expect("bind inherited window");
        let BoundStatement::AdvancedSelect { windows, .. } = inherited else {
            panic!("inherited window select");
        };
        assert_eq!(windows[0].partition_by.len(), 1);
        assert_eq!(windows[0].order_by.len(), 1);

        let missing = parse("SELECT RANK() OVER missing_window FROM documents")
            .expect_err("missing named window");
        assert_eq!(missing.sql_state, "42704");

        let duplicate = parse(
            "SELECT RANK() OVER duplicate_name FROM documents \
             WINDOW duplicate_name AS (ORDER BY id), duplicate_name AS (ORDER BY title)",
        )
        .expect_err("duplicate named window");
        assert_eq!(duplicate.sql_state, "42712");

        let framed = bind(
            parse("SELECT RANK() OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM documents")
                .expect("parse explicit frame"),
            &catalog,
        )
        .expect("bind explicit frame");
        let BoundStatement::AdvancedSelect { windows, .. } = framed else {
            panic!("framed window select");
        };
        assert!(matches!(
            windows[0].frame,
            Some(BoundWindowFrame {
                units: WindowFrameUnits::Rows,
                start_bound: BoundWindowFrameBound::UnboundedPreceding,
                end_bound: BoundWindowFrameBound::CurrentRow,
            })
        ));

        let inline_inherited = bind(
            parse(
                "SELECT RANK() OVER (grouped ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
                 FROM documents WINDOW grouped AS (PARTITION BY title)",
            )
            .expect("parse inline inherited frame"),
            &catalog,
        )
        .expect("bind inline inherited frame");
        let BoundStatement::AdvancedSelect { windows, .. } = inline_inherited else {
            panic!("inline inherited window select");
        };
        assert_eq!(windows[0].partition_by.len(), 1);
        assert!(windows[0].frame.is_some());

        let invalid_order = parse(
            "SELECT RANK() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) \
             FROM documents",
        )
        .expect_err("frame end before start");
        assert_eq!(invalid_order.sql_state, "42P20");

        let range_without_order = bind(
            parse("SELECT RANK() OVER (RANGE 1 PRECEDING) FROM documents")
                .expect("parse RANGE offset"),
            &catalog,
        )
        .expect_err("RANGE offset without one ORDER BY");
        assert_eq!(range_without_order.sql_state, "42P20");

        let variable_offset = bind(
            parse("SELECT RANK() OVER (ORDER BY id ROWS id PRECEDING) FROM documents")
                .expect("parse variable ROWS offset"),
            &catalog,
        )
        .expect_err("frame variable");
        assert_eq!(variable_offset.sql_state, "42P20");

        let groups = parse("SELECT RANK() OVER (ORDER BY id GROUPS CURRENT ROW) FROM documents")
            .expect_err("GROUPS frame");
        assert_eq!(groups.sql_state, FEATURE_NOT_SUPPORTED);

        let in_where = bind(
            parse("SELECT id FROM documents WHERE ROW_NUMBER() OVER () = 1")
                .expect("parse window in WHERE"),
            &catalog,
        )
        .expect_err("window in WHERE");
        assert_eq!(in_where.sql_state, "42P20");

        let nested = bind(
            parse("SELECT SUM(ROW_NUMBER() OVER ()) FROM documents").expect("parse nested window"),
            &catalog,
        )
        .expect_err("nested window");
        assert_eq!(nested.sql_state, "42P20");
    }

    #[test]
    fn binds_value_and_aggregate_window_types() {
        let catalog = catalog_with_documents();
        let statement = bind(
            parse(
                "SELECT
                    LAG(id) OVER ordered,
                    LEAD(id, 2, 0) OVER ordered,
                    FIRST_VALUE(title) OVER ordered,
                    LAST_VALUE(title) OVER (
                        ordered ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ),
                    NTH_VALUE(title, 2) OVER ordered,
                    COUNT(*) OVER (PARTITION BY title),
                    SUM(id) FILTER (WHERE id > 0) OVER (PARTITION BY title),
                    AVG(id) OVER (PARTITION BY title)
                 FROM documents WINDOW ordered AS (PARTITION BY title ORDER BY id)",
            )
            .expect("parse value and aggregate windows"),
            &catalog,
        )
        .expect("bind value and aggregate windows");
        let BoundStatement::AdvancedSelect {
            windows, schema, ..
        } = statement
        else {
            panic!("window select");
        };
        assert_eq!(windows.len(), 8);
        assert!(matches!(windows[0].function, WindowFunction::Lag));
        assert!(matches!(windows[1].function, WindowFunction::Lead));
        assert!(matches!(windows[2].function, WindowFunction::FirstValue));
        assert!(matches!(windows[3].function, WindowFunction::LastValue));
        assert!(matches!(windows[4].function, WindowFunction::NthValue));
        assert!(matches!(
            windows[5].function,
            WindowFunction::Aggregate(AggregateFunction::Count)
        ));
        assert!(windows[5].count_star);
        assert!(windows[6].filter.is_some());
        assert_eq!(schema.fields[0].data_type, ScalarType::Int64);
        assert!(schema.fields[0].nullable);
        assert_eq!(schema.fields[2].data_type, ScalarType::Text);
        assert_eq!(schema.fields[5].data_type, ScalarType::Int64);
        assert!(!schema.fields[5].nullable);
        assert_eq!(schema.fields[6].data_type, ScalarType::Int64);
        assert!(schema.fields[6].nullable);
        assert_eq!(schema.fields[7].data_type, ScalarType::Float64);

        let distinct = parse("SELECT COUNT(DISTINCT id) OVER () FROM documents")
            .expect_err("DISTINCT window aggregate");
        assert_eq!(distinct.sql_state, FEATURE_NOT_SUPPORTED);

        let non_aggregate_filter =
            parse("SELECT LAG(id) FILTER (WHERE id > 0) OVER (ORDER BY id) FROM documents")
                .expect_err("FILTER on non-aggregate window");
        assert_eq!(non_aggregate_filter.sql_state, "42809");
    }

    #[test]
    fn binds_select_distinct_and_enforces_order_visibility() {
        let catalog = catalog_with_documents();
        let statement = bind(
            parse("SELECT DISTINCT title FROM documents ORDER BY title")
                .expect("parse SELECT DISTINCT"),
            &catalog,
        )
        .expect("bind SELECT DISTINCT");
        assert!(matches!(
            statement,
            BoundStatement::AdvancedSelect { distinct: true, .. }
        ));

        let all = bind(
            parse("SELECT ALL title FROM documents").expect("parse SELECT ALL"),
            &catalog,
        )
        .expect("bind SELECT ALL");
        assert!(matches!(all, BoundStatement::Select { .. }));

        let error = bind(
            parse("SELECT DISTINCT title FROM documents ORDER BY id")
                .expect("parse invalid DISTINCT order"),
            &catalog,
        )
        .expect_err("DISTINCT order expression outside projection");
        assert_eq!(error.sql_state, "42P10");

        let error = parse("SELECT DISTINCT ON (title) title FROM documents")
            .expect_err("DISTINCT ON remains explicit");
        assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED);

        let mut json_catalog = Catalog::default();
        json_catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("payloads"),
                vec![NewColumn::new(
                    Identifier::unquoted("payload"),
                    ScalarType::Json,
                )],
            )
            .expect("JSON table");
        for sql in [
            "SELECT DISTINCT payload FROM payloads",
            "SELECT COUNT(DISTINCT payload) FROM payloads",
        ] {
            let error = bind(parse(sql).expect("parse JSON DISTINCT"), &json_catalog)
                .expect_err("JSON DISTINCT equality");
            assert_eq!(error.sql_state, "42883");
        }
    }

    #[test]
    fn binds_in_lists_with_shared_parameter_types() {
        let catalog = catalog_with_documents();
        let statement = bind(
            parse("SELECT id FROM documents WHERE id NOT IN ($1, 2, NULL)")
                .expect("parse NOT IN list"),
            &catalog,
        )
        .expect("bind NOT IN list");
        let BoundStatement::Select {
            filter:
                Some(BoundExpr {
                    kind:
                        BoundExprKind::InList {
                            expr,
                            list,
                            negated,
                        },
                    ..
                }),
            ..
        } = statement
        else {
            panic!("bound NOT IN filter");
        };
        assert!(negated);
        assert_eq!(expr.data_type, ScalarType::Int64);
        assert_eq!(list.len(), 3);
        assert!(matches!(
            list[0].kind,
            BoundExprKind::Parameter { index: 1 }
        ));
        assert_eq!(list[0].data_type, ScalarType::Int64);

        let error = bind(
            parse("SELECT id FROM documents WHERE id IN ('wrong')")
                .expect("parse incompatible IN list"),
            &catalog,
        )
        .expect_err("incompatible IN types");
        assert_eq!(error.sql_state, DATATYPE_MISMATCH);

        let error = bind(
            parse("SELECT id FROM documents WHERE $1 IN ($2)")
                .expect("parse indeterminate IN list"),
            &catalog,
        )
        .expect_err("indeterminate IN types");
        assert_eq!(error.sql_state, INDETERMINATE_DATATYPE);
    }

    #[test]
    fn owns_and_binds_uncorrelated_subquery_apply_forms() {
        let mut catalog = catalog_with_documents();
        catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("apply_lookup"),
                vec![NewColumn::new(
                    Identifier::unquoted("id"),
                    ScalarType::Int64,
                )],
            )
            .expect("create Apply lookup");
        let scalar =
            parse("SELECT (SELECT id FROM documents LIMIT 1) AS selected_id FROM documents")
                .expect("parse scalar subquery");
        let ParsedStatement::AdvancedSelect { projection, .. } = &scalar else {
            panic!("scalar subquery select");
        };
        assert!(matches!(
            &projection[0],
            ParsedProjection::Expression {
                expr: ParsedExpr {
                    kind: ParsedExprKind::ScalarSubquery(_),
                    ..
                },
                ..
            }
        ));
        let scalar = bind(scalar, &catalog).expect("bind scalar Apply");
        let BoundStatement::AdvancedSelect {
            applies,
            projection,
            ..
        } = scalar
        else {
            panic!("bound scalar Apply select");
        };
        assert_eq!(applies.len(), 1);
        assert!(matches!(applies[0].kind, BoundApplyKind::Scalar));
        assert_eq!(
            bound_query_schema(&applies[0].query).expect("scalar Apply schema"),
            Schema::new(vec![Field::new("id", ScalarType::Int64, false)])
        );
        assert!(matches!(
            projection[0].expr,
            BoundExpr {
                kind: BoundExprKind::ApplyValue { index: 2 },
                data_type: ScalarType::Int64,
                nullable: true,
            }
        ));

        let cases = [
            (
                "SELECT id FROM documents WHERE EXISTS (SELECT id FROM documents)",
                SubqueryQuantifier::Any,
            ),
            (
                "SELECT id FROM documents WHERE id IN (SELECT id FROM documents)",
                SubqueryQuantifier::Any,
            ),
            (
                "SELECT id FROM documents WHERE id = ANY (SELECT id FROM documents)",
                SubqueryQuantifier::Any,
            ),
            (
                "SELECT id FROM documents WHERE id <> ALL (SELECT id FROM documents)",
                SubqueryQuantifier::All,
            ),
        ];
        for (index, (sql, quantifier)) in cases.into_iter().enumerate() {
            let statement = parse(sql).expect("parse subquery predicate");
            let ParsedStatement::AdvancedSelect {
                filter: Some(filter),
                ..
            } = &statement
            else {
                panic!("subquery predicate select");
            };
            match (index, &filter.kind) {
                (0, ParsedExprKind::Exists { negated: false, .. }) => {}
                (1, ParsedExprKind::InSubquery { negated: false, .. }) => {}
                (
                    2 | 3,
                    ParsedExprKind::QuantifiedSubquery {
                        quantifier: actual, ..
                    },
                ) if actual == &quantifier => {}
                _ => panic!("unexpected owned subquery form: {filter:?}"),
            }
            let statement = bind(statement, &catalog).expect("bind uncorrelated Apply");
            let BoundStatement::AdvancedSelect {
                applies,
                filter: Some(filter),
                ..
            } = statement
            else {
                panic!("bound subquery predicate select");
            };
            assert_eq!(applies.len(), 1);
            assert!(matches!(
                filter.kind,
                BoundExprKind::ApplyValue { index: 2 }
            ));
            match (index, &applies[0].kind) {
                (0, BoundApplyKind::Exists { negated: false }) => {
                    assert!(!filter.nullable);
                }
                (
                    1,
                    BoundApplyKind::In {
                        left,
                        negated: false,
                    },
                ) => {
                    assert_eq!(left.data_type, ScalarType::Int64);
                    assert!(filter.nullable);
                }
                (
                    2 | 3,
                    BoundApplyKind::Quantified {
                        quantifier: actual, ..
                    },
                ) if actual == &quantifier => {
                    assert!(filter.nullable);
                }
                _ => panic!("unexpected bound Apply form: {:?}", applies[0].kind),
            }
        }

        let parameterized = bind(
            parse("SELECT id FROM documents WHERE $1 IN (SELECT id FROM documents)")
                .expect("parse parameterized Apply"),
            &catalog,
        )
        .expect("bind parameterized Apply");
        let BoundStatement::AdvancedSelect { applies, .. } = parameterized else {
            panic!("parameterized Apply select");
        };
        assert!(matches!(
            applies[0].kind,
            BoundApplyKind::In {
                left: BoundExpr {
                    kind: BoundExprKind::Parameter { index: 1 },
                    data_type: ScalarType::Int64,
                    ..
                },
                ..
            }
        ));

        bind(
            parse("SELECT id FROM documents WHERE EXISTS (SELECT id, title FROM documents)")
                .expect("parse multi-column EXISTS"),
            &catalog,
        )
        .expect("EXISTS may project multiple columns");

        let error = bind(
            parse("SELECT (SELECT id, title FROM documents) FROM documents")
                .expect("parse multi-column scalar subquery"),
            &catalog,
        )
        .expect_err("scalar subquery must return one column");
        assert_eq!(error.sql_state, SYNTAX_ERROR);

        let dependencies = bind(
            parse(
                "SELECT id FROM documents \
                 WHERE EXISTS (SELECT id FROM apply_lookup)",
            )
            .expect("parse Apply dependencies"),
            &catalog,
        )
        .expect("bind Apply dependencies");
        assert_eq!(bound_statement_references(&dependencies).len(), 2);

        let correlated = bind(
            parse(
                "SELECT outer_docs.id FROM documents outer_docs
                 WHERE EXISTS (
                     SELECT inner_docs.id FROM documents inner_docs
                     WHERE inner_docs.id = outer_docs.id
                 )",
            )
            .expect("parse correlated Apply"),
            &catalog,
        )
        .expect("bind correlated Apply");
        let BoundStatement::AdvancedSelect { applies, .. } = correlated else {
            panic!("bound correlated Apply select");
        };
        let BoundStatement::AdvancedSelect {
            filter: Some(filter),
            ..
        } = applies[0].query.as_ref()
        else {
            panic!("bound correlated Apply inner query");
        };
        assert!(matches!(
            filter.kind,
            BoundExprKind::Binary {
                right: ref correlation,
                ..
            } if matches!(
                correlation.kind,
                BoundExprKind::Correlation { depth: 1, index: 0 }
            )
        ));

        let nested = bind(
            parse(
                "SELECT outer_docs.id FROM documents outer_docs
                 WHERE EXISTS (
                     SELECT middle_docs.id FROM documents middle_docs
                     WHERE EXISTS (
                         SELECT inner_docs.id FROM documents inner_docs
                         WHERE inner_docs.id = middle_docs.id
                           AND middle_docs.id = outer_docs.id
                     )
                 )",
            )
            .expect("parse nested correlated Apply"),
            &catalog,
        )
        .expect("bind nested correlated Apply");
        assert!(matches!(nested, BoundStatement::AdvancedSelect { .. }));
    }

    #[test]
    fn owns_and_binds_row_comparisons_and_row_apply_forms() {
        fn select_filter(statement: &ParsedStatement) -> &ParsedExpr {
            match statement {
                ParsedStatement::Select {
                    filter: Some(filter),
                    ..
                }
                | ParsedStatement::AdvancedSelect {
                    filter: Some(filter),
                    ..
                } => filter,
                _ => panic!("statement does not contain a SELECT filter"),
            }
        }

        let catalog = catalog_with_documents();

        let direct = parse("SELECT id FROM documents WHERE (id, title) = (1, 'first')")
            .expect("parse row equality");
        let filter = select_filter(&direct);
        assert!(matches!(
            filter.kind,
            ParsedExprKind::Binary {
                op: BinaryOperator::And,
                ..
            }
        ));
        bind(direct, &catalog).expect("bind row equality");

        let not_equal = parse("SELECT id FROM documents WHERE (id, title) <> (1, 'first')")
            .expect("parse row inequality");
        let filter = select_filter(&not_equal);
        assert!(matches!(
            filter.kind,
            ParsedExprKind::Unary {
                op: UnaryOperator::Not,
                ..
            }
        ));
        bind(not_equal, &catalog).expect("bind row inequality");

        let listed =
            parse("SELECT id FROM documents WHERE (id, title) IN ((1, 'first'), (2, NULL))")
                .expect("parse row IN list");
        let filter = select_filter(&listed);
        assert!(matches!(
            filter.kind,
            ParsedExprKind::Binary {
                op: BinaryOperator::Or,
                ..
            }
        ));
        bind(listed, &catalog).expect("bind row IN list");

        let cases = [
            (
                "SELECT id FROM documents WHERE (id, title) = (SELECT id, title FROM documents LIMIT 1)",
                None,
                false,
            ),
            (
                "SELECT id FROM documents WHERE (id, title) IN (SELECT id, title FROM documents)",
                Some(SubqueryQuantifier::Any),
                false,
            ),
            (
                "SELECT id FROM documents WHERE (id, title) NOT IN (SELECT id, title FROM documents)",
                Some(SubqueryQuantifier::Any),
                true,
            ),
            (
                "SELECT id FROM documents WHERE (id, title) = ANY (SELECT id, title FROM documents)",
                Some(SubqueryQuantifier::Any),
                false,
            ),
            (
                "SELECT id FROM documents WHERE (id, title) <> ALL (SELECT id, title FROM documents)",
                Some(SubqueryQuantifier::All),
                false,
            ),
        ];
        for (sql, quantifier, negated) in cases {
            let statement = parse(sql).expect("parse row subquery");
            let ParsedStatement::AdvancedSelect {
                filter: Some(filter),
                ..
            } = &statement
            else {
                panic!("row subquery select");
            };
            assert!(matches!(
                filter.kind,
                ParsedExprKind::RowSubquery {
                    quantifier: actual,
                    negated: actual_negated,
                    ..
                } if actual == quantifier && actual_negated == negated
            ));

            let statement = bind(statement, &catalog).expect("bind row subquery");
            let BoundStatement::AdvancedSelect {
                applies,
                filter: Some(filter),
                ..
            } = statement
            else {
                panic!("bound row subquery select");
            };
            assert_eq!(applies.len(), 1);
            assert!(matches!(filter.kind, BoundExprKind::ApplyValue { .. }));
            match (&applies[0].kind, quantifier) {
                (
                    BoundApplyKind::RowScalar {
                        left,
                        op: BinaryOperator::Eq,
                        operand_types,
                    },
                    None,
                ) => {
                    assert_eq!(left.len(), 2);
                    assert_eq!(operand_types, &[ScalarType::Int64, ScalarType::Text]);
                }
                (
                    BoundApplyKind::RowQuantified {
                        left,
                        quantifier: actual,
                        negated: actual_negated,
                        operand_types,
                        ..
                    },
                    Some(expected),
                ) => {
                    assert_eq!(left.len(), 2);
                    assert_eq!(*actual, expected);
                    assert_eq!(*actual_negated, negated);
                    assert_eq!(operand_types, &[ScalarType::Int64, ScalarType::Text]);
                }
                _ => panic!("unexpected bound row Apply form: {:?}", applies[0].kind),
            }
        }

        let direct_width = parse("SELECT id FROM documents WHERE (id, title) = (1, 'first', 3)")
            .expect_err("direct row width mismatch");
        assert_eq!(direct_width.sql_state, SYNTAX_ERROR);

        let subquery_width = bind(
            parse("SELECT id FROM documents WHERE (id, title) IN (SELECT id FROM documents)")
                .expect("parse subquery row width mismatch"),
            &catalog,
        )
        .expect_err("subquery row width mismatch");
        assert_eq!(subquery_width.sql_state, SYNTAX_ERROR);

        let ordered = parse("SELECT id FROM documents WHERE (id, title) < (1, 'first')")
            .expect_err("ordered row comparison remains explicit");
        assert_eq!(ordered.sql_state, FEATURE_NOT_SUPPORTED);

        let mixed_list = parse("SELECT id FROM documents WHERE (id, title) IN ((1, 'first'), 2)")
            .expect_err("row IN list requires row entries");
        assert_eq!(mixed_list.sql_state, SYNTAX_ERROR);
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
            "CREATE PROCEDURE public.refresh_items(value public.mood, count BIGINT)
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
            } if arguments.len() == 2
                && arguments[0].declared_type.is_some()
                && arguments[1].declared_type.is_none()
                && body.contains("RETURN")
        ));
    }

    #[test]
    fn parses_routine_argument_modes_and_trigger_activation() {
        let procedure = parse(
            "CREATE PROCEDURE public.mode_probe(\
             IN input_value BIGINT, OUT output_value TEXT, \
             INOUT counter INTEGER, VARIADIC rest BIGINT[]) \
             LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$",
        )
        .expect("parse procedure modes");
        let ParsedStatement::CreateRoutine { arguments, .. } = procedure else {
            panic!("expected procedure");
        };
        assert_eq!(
            arguments
                .iter()
                .map(|argument| argument.mode)
                .collect::<Vec<_>>(),
            vec![
                RoutineArgumentMode::In,
                RoutineArgumentMode::Out,
                RoutineArgumentMode::InOut,
                RoutineArgumentMode::Variadic,
            ]
        );

        let function = parse(
            "CREATE FUNCTION public.output_probe(IN value BIGINT, OUT doubled BIGINT) \
             LANGUAGE plpgsql AS $$ BEGIN doubled := value * 2; RETURN; END $$",
        )
        .expect("parse function OUT mode");
        assert!(matches!(
            function,
            ParsedStatement::CreateRoutine {
                return_type: None,
                ref arguments,
                ..
            } if arguments[1].mode == RoutineArgumentMode::Out
        ));

        let statement_trigger = parse(
            "CREATE TRIGGER documents_audit AFTER UPDATE ON documents \
             FOR EACH STATEMENT EXECUTE FUNCTION public.audit_documents()",
        )
        .expect("parse statement trigger");
        assert!(matches!(
            statement_trigger,
            ParsedStatement::CreateTrigger {
                timing: TriggerTiming::AfterStatement,
                level: TriggerLevel::Statement,
                ..
            }
        ));

        let instead_of = parse(
            "CREATE TRIGGER documents_view_insert INSTEAD OF INSERT ON documents_view \
             FOR EACH ROW EXECUTE FUNCTION public.insert_documents_view()",
        )
        .expect("parse INSTEAD OF trigger");
        assert!(matches!(
            instead_of,
            ParsedStatement::CreateTrigger {
                timing: TriggerTiming::InsteadOf,
                level: TriggerLevel::Row,
                ..
            }
        ));
    }

    #[test]
    fn binds_regular_view_instead_of_trigger_targets_and_view_dml() {
        let mut catalog = catalog_with_documents();
        let documents = catalog
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        let view_id = catalog
            .create_view(
                &Identifier::unquoted("public"),
                ordadb_catalog::NewView {
                    name: Identifier::unquoted("document_view"),
                    kind: ViewKind::Regular,
                    query: "SELECT id, title FROM documents".into(),
                    output: Schema::new(vec![
                        Field::new("id", ScalarType::Int64, false),
                        Field::new("title", ScalarType::Text, false),
                    ]),
                    materialized_table_id: None,
                    populated: true,
                    references: vec![CatalogObjectRef::Table(documents)],
                },
            )
            .expect("view");
        let routine_id = catalog
            .create_or_replace_routine(
                &Identifier::unquoted("public"),
                ordadb_catalog::NewRoutine {
                    name: Identifier::unquoted("document_view_insert"),
                    kind: RoutineKind::Function,
                    arguments: Vec::new(),
                    return_type: None,
                    return_declared_type: None,
                    returns_set: false,
                    language: "plpgsql".into(),
                    body: "BEGIN RETURN NEW; END".into(),
                    replace: false,
                    references: Vec::new(),
                },
            )
            .expect("routine");
        let unavailable = bind(
            parse("INSERT INTO document_view VALUES (1, 'one')").expect("parse view insert"),
            &catalog,
        )
        .expect_err("view DML requires a trigger");
        assert_eq!(unavailable.sql_state, "55000");

        let create = bind(
            parse(
                "CREATE TRIGGER document_view_insert_trigger INSTEAD OF INSERT ON document_view \
                 FOR EACH ROW EXECUTE FUNCTION document_view_insert()",
            )
            .expect("parse view trigger"),
            &catalog,
        )
        .expect("bind view trigger");
        assert!(matches!(
            create,
            BoundStatement::CreateTrigger {
                target: TriggerTarget::View(id),
                ..
            } if id == view_id
        ));
        catalog
            .create_trigger_on_target_with_level(
                TriggerTarget::View(view_id),
                Identifier::unquoted("document_view_insert_trigger"),
                TriggerTiming::InsteadOf,
                TriggerLevel::Row,
                BTreeSet::from([CatalogTriggerEvent::Insert]),
                routine_id,
            )
            .expect("catalog view trigger");
        assert!(matches!(
            bind(
                parse("INSERT INTO document_view VALUES ($1, $2) RETURNING *")
                    .expect("parse parameterized view insert"),
                &catalog,
            )
            .expect("bind parameterized view insert"),
            BoundStatement::ViewInsert { view_id: id, .. } if id == view_id
        ));
    }

    #[test]
    fn parses_and_binds_remaining_core_session_and_maintenance_commands() {
        let catalog = catalog_with_documents();
        assert!(matches!(
            bind(
                parse("REINDEX TABLE public.documents").expect("parse reindex table"),
                &catalog,
            )
            .expect("bind reindex table"),
            BoundStatement::Reindex {
                target: BoundReindexTarget::Table(_)
            }
        ));
        assert_eq!(
            parse("REINDEX (VERBOSE true) TABLE public.documents")
                .expect_err("reindex parameters are explicit unsupported")
                .sql_state,
            FEATURE_NOT_SUPPORTED
        );
        assert_eq!(
            parse("REINDEX TABLE CONCURRENTLY public.documents")
                .expect_err("concurrent reindex is explicit unsupported")
                .sql_state,
            FEATURE_NOT_SUPPORTED
        );

        assert!(matches!(
            bind(parse("LISTEN events").expect("parse listen"), &catalog)
                .expect("bind listen"),
            BoundStatement::Listen { ref channel } if channel.as_str() == "events"
        ));
        assert!(matches!(
            bind(
                parse("NOTIFY events, 'ready'").expect("parse notify"),
                &catalog,
            )
            .expect("bind notify"),
            BoundStatement::Notify { ref channel, ref payload }
                if channel.as_str() == "events" && payload == "ready"
        ));
        assert!(matches!(
            bind(
                parse("SELECT pg_catalog.pg_notify('events', 'from-function')")
                    .expect("parse pg_notify"),
                &catalog,
            )
            .expect("bind pg_notify"),
            BoundStatement::PgNotify { ref schema, .. }
                if schema.fields.len() == 1 && schema.fields[0].name == "pg_notify"
        ));
        assert!(matches!(
            bind(parse("UNLISTEN *").expect("parse unlisten"), &catalog).expect("bind unlisten"),
            BoundStatement::Unlisten { channel: None }
        ));
        assert!(matches!(
            bind(
                parse("DO LANGUAGE plpgsql $$ BEGIN NULL; END $$").expect("parse do"),
                &catalog,
            )
            .expect("bind do"),
            BoundStatement::Do { ref body } if body.contains("BEGIN")
        ));
        assert!(matches!(
            bind(parse("DISCARD ALL").expect("parse discard"), &catalog).expect("bind discard"),
            BoundStatement::DiscardAll
        ));
        assert!(matches!(
            bind(
                parse("DEALLOCATE PREPARE ALL").expect("parse deallocate"),
                &catalog,
            )
            .expect("bind deallocate"),
            BoundStatement::DeallocateAll
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
    fn solves_parameter_types_across_occurrences_and_query_boundaries() {
        let catalog = catalog_with_documents();

        let statement = bind(
            parse("SELECT $1 AS repeated, id FROM documents WHERE id = $1")
                .expect("parse cross-clause parameter"),
            &catalog,
        )
        .expect("bind cross-clause parameter");
        let BoundStatement::Select { projection, .. } = statement else {
            panic!("simple SELECT");
        };
        assert_eq!(projection[0].expr.data_type, ScalarType::Int64);

        let set = bind(
            parse("SELECT $1 AS value FROM documents UNION SELECT id FROM documents")
                .expect("parse set parameter"),
            &catalog,
        )
        .expect("bind set parameter");
        let BoundStatement::SetOperation { schema, .. } = set else {
            panic!("set operation");
        };
        assert_eq!(schema.fields[0].data_type, ScalarType::Int64);

        bind(
            parse(
                "WITH picked(value) AS (\
                     SELECT $1 FROM documents WHERE id = $1\
                 ) SELECT value FROM picked",
            )
            .expect("parse CTE parameter"),
            &catalog,
        )
        .expect("bind CTE parameter");

        bind(
            parse(
                "SELECT $1, LAG(id, $2, $1) OVER (ORDER BY id) FROM documents \
                 WHERE id IN (SELECT id FROM documents WHERE id = $1)",
            )
            .expect("parse window and Apply parameters"),
            &catalog,
        )
        .expect("bind window and Apply parameters");

        bind(
            parse(
                "SELECT outer_documents.id FROM documents outer_documents \
                 WHERE EXISTS (\
                     SELECT middle_documents.id FROM documents middle_documents \
                     WHERE EXISTS (\
                         SELECT inner_documents.id FROM documents inner_documents \
                         WHERE inner_documents.id = middle_documents.id \
                           AND middle_documents.id = outer_documents.id\
                     )\
                 )",
            )
            .expect("parse nested correlation"),
            &catalog,
        )
        .expect("parameter solver preserves nested correlation scopes");

        let insert = bind(
            parse(
                "INSERT INTO documents (id, title) VALUES ($1, $2) \
                 RETURNING $1, $2",
            )
            .expect("parse DML parameters"),
            &catalog,
        )
        .expect("bind DML parameters");
        let BoundStatement::Insert {
            returning: Some(returning),
            ..
        } = insert
        else {
            panic!("INSERT RETURNING");
        };
        assert_eq!(returning.schema.fields[0].data_type, ScalarType::Int64);
        assert_eq!(returning.schema.fields[1].data_type, ScalarType::Text);

        let conflict = bind(
            parse("SELECT $1 FROM documents WHERE id = $1 OR title = $1")
                .expect("parse conflicting parameter"),
            &catalog,
        )
        .expect_err("conflicting parameter constraints");
        assert_eq!(conflict.sql_state, DATATYPE_MISMATCH);
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
    fn parses_postgres_catalog_scalar_type_names_without_user_type_lookup() {
        let statement = parse(
            "CREATE TABLE catalog_scalar_types (object_id OID, object_name NAME, kind \"char\")",
        )
        .expect("PostgreSQL catalog scalar types");
        let ParsedStatement::CreateTable { columns, .. } = statement else {
            panic!("create table");
        };
        assert_eq!(columns[0].data_type, ScalarType::Oid);
        assert_eq!(columns[0].declared_type, None);
        assert_eq!(columns[1].data_type, ScalarType::Name);
        assert_eq!(columns[1].declared_type, None);
        assert_eq!(columns[2].data_type, ScalarType::InternalChar);
        assert_eq!(columns[2].declared_type, None);
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
    fn normalizes_supported_row_limit_and_offset_forms() {
        for (dialect, sql, expected_limit, expected_offset) in [
            (
                SqlDialect::PostgreSql,
                "SELECT id FROM documents OFFSET 0 ROWS FETCH FIRST 4 ROWS ONLY",
                4,
                0,
            ),
            (
                SqlDialect::MySql,
                "SELECT id FROM documents LIMIT 1, 5",
                5,
                1,
            ),
            (
                SqlDialect::Sqlite,
                "SELECT id FROM documents LIMIT 6 OFFSET 2",
                6,
                2,
            ),
            (
                SqlDialect::SqlServer,
                "SELECT [id] FROM [documents] ORDER BY [id] \
                 OFFSET 3 ROWS FETCH NEXT 7 ROWS ONLY",
                7,
                3,
            ),
        ] {
            let statement = parse_with_dialect(sql, dialect)
                .unwrap_or_else(|error| panic!("{dialect}: {error:?}"));
            let ParsedStatement::Select {
                limit: Some(limit),
                offset: Some(offset),
                ..
            } = statement
            else {
                panic!("select with limit and offset");
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
            assert!(
                matches!(
                    offset.kind,
                    ParsedExprKind::Literal(Value::Int32(value))
                        if value == expected_offset
                ) || matches!(
                    offset.kind,
                    ParsedExprKind::Literal(Value::Int64(value))
                        if value == i64::from(expected_offset)
                ),
                "{dialect}: {offset:?}"
            );
        }
    }

    #[test]
    fn binds_postgres_offset_and_limit_parameters_as_bigint() {
        let catalog = catalog_with_documents();
        let bound = bind(
            parse("SELECT id FROM documents ORDER BY id OFFSET $1 LIMIT $2").expect("parse offset"),
            &catalog,
        )
        .expect("bind offset");
        let BoundStatement::Select {
            offset: Some(offset),
            limit: Some(limit),
            ..
        } = bound
        else {
            panic!("bound select with offset and limit");
        };
        assert_eq!(offset.data_type, ScalarType::Int64);
        assert_eq!(limit.data_type, ScalarType::Int64);
        assert!(matches!(offset.kind, BoundExprKind::Parameter { index: 1 }));
        assert!(matches!(limit.kind, BoundExprKind::Parameter { index: 2 }));
    }

    #[test]
    fn parses_and_binds_postgres_set_operations() {
        let catalog = catalog_with_documents();
        let bound = bind(
            parse(
                "SELECT id AS item_id FROM documents WHERE id <= 2 \
                 UNION ALL \
                 SELECT id FROM documents WHERE id >= 2 \
                 INTERSECT \
                 SELECT id FROM documents \
                 ORDER BY item_id DESC NULLS LAST OFFSET $1 LIMIT $2",
            )
            .expect("parse set operation"),
            &catalog,
        )
        .expect("bind set operation");
        let BoundStatement::SetOperation {
            operator: QuerySetOperator::Union,
            all: true,
            right,
            schema,
            order_by,
            offset: Some(offset),
            limit: Some(limit),
            ..
        } = bound
        else {
            panic!("bound outer set operation");
        };
        assert_eq!(schema.fields[0].name, "item_id");
        assert_eq!(order_by[0].column_index, 0);
        assert!(!order_by[0].ascending);
        assert_eq!(order_by[0].nulls_first, Some(false));
        assert!(matches!(offset.kind, BoundExprKind::Parameter { index: 1 }));
        assert!(matches!(limit.kind, BoundExprKind::Parameter { index: 2 }));
        assert!(matches!(
            *right,
            BoundStatement::SetOperation {
                operator: QuerySetOperator::Intersect,
                all: false,
                ..
            }
        ));

        let width_error = bind(
            parse(
                "SELECT id, title FROM documents \
                 EXCEPT SELECT id FROM documents",
            )
            .expect("parse mismatched set width"),
            &catalog,
        )
        .expect_err("set width mismatch");
        assert_eq!(width_error.sql_state, SYNTAX_ERROR);

        let type_error = bind(
            parse("SELECT id FROM documents UNION SELECT title FROM documents")
                .expect("parse mismatched set types"),
            &catalog,
        )
        .expect_err("set type mismatch");
        assert_eq!(type_error.sql_state, DATATYPE_MISMATCH);
    }

    #[test]
    fn parses_and_binds_ordered_non_recursive_ctes() {
        let catalog = catalog_with_documents();
        let bound = bind(
            parse(
                "WITH base(item, label) AS (
                     SELECT id, title FROM documents WHERE id >= 1
                 ), filtered AS (
                     SELECT item AS id FROM base WHERE item <= 10
                 )
                 SELECT id FROM filtered ORDER BY id",
            )
            .expect("parse CTEs"),
            &catalog,
        )
        .expect("bind CTEs");
        let BoundStatement::With {
            ctes, body, schema, ..
        } = bound
        else {
            panic!("bound WITH");
        };
        assert_eq!(ctes.len(), 2);
        assert_eq!(schema.fields[0].name, "id");
        assert!(matches!(
            ctes[1].seed.as_ref(),
            BoundStatement::Select { table_id, .. } if *table_id == ctes[0].table_id
        ));
        assert!(matches!(
            body.as_ref(),
            BoundStatement::Select { table_id, .. } if *table_id == ctes[1].table_id
        ));

        let cte_apply = bind(
            parse(
                "WITH base(item) AS (
                     SELECT id FROM documents WHERE id <= 2
                 )
                 SELECT id FROM documents
                 WHERE EXISTS (SELECT item FROM base WHERE item = 2)",
            )
            .expect("parse CTE Apply"),
            &catalog,
        )
        .expect("bind CTE Apply");
        let BoundStatement::With { ctes, body, .. } = cte_apply else {
            panic!("bound CTE Apply WITH");
        };
        let BoundStatement::AdvancedSelect { applies, .. } = body.as_ref() else {
            panic!("bound CTE Apply body");
        };
        assert!(matches!(
            applies[0].query.as_ref(),
            BoundStatement::AdvancedSelect { table, .. } if table.table_id == ctes[0].table_id
        ));

        let duplicate = bind(
            parse(
                "WITH repeated AS (SELECT id FROM documents),
                      repeated AS (SELECT id FROM documents)
                 SELECT id FROM repeated",
            )
            .expect("parse duplicate CTE"),
            &catalog,
        )
        .expect_err("duplicate CTE name");
        assert_eq!(duplicate.sql_state, "42712");

        let recursive = bind(
            parse(
                "WITH RECURSIVE numbers(value) AS (
                     SELECT id FROM documents WHERE id = 1
                     UNION ALL
                     SELECT value + 1 FROM numbers WHERE value < 3
                 ) SELECT value FROM numbers ORDER BY value",
            )
            .expect("parse recursive CTE"),
            &catalog,
        )
        .expect("bind recursive CTE");
        let BoundStatement::With { ctes, .. } = recursive else {
            panic!("bound recursive WITH");
        };
        assert_eq!(ctes.len(), 1);
        assert!(ctes[0].union_all);
        assert!(ctes[0].recursive.is_some());

        let invalid_recursive = bind(
            parse(
                "WITH RECURSIVE numbers(value) AS (
                     SELECT value FROM numbers
                 ) SELECT value FROM numbers",
            )
            .expect("parse invalid recursive CTE"),
            &catalog,
        )
        .expect_err("recursive CTE without UNION");
        assert_eq!(invalid_recursive.sql_state, FEATURE_NOT_SUPPORTED);
    }

    #[test]
    fn parses_and_binds_dml_returning_projections() {
        let catalog = catalog_with_documents();

        let insert = bind(
            parse("INSERT INTO documents VALUES (1, 'one') RETURNING id, title AS name")
                .expect("parse insert returning"),
            &catalog,
        )
        .expect("bind insert returning");
        let BoundStatement::Insert {
            returning: Some(returning),
            ..
        } = insert
        else {
            panic!("insert returning");
        };
        assert_eq!(returning.schema.fields[0].name, "id");
        assert_eq!(returning.schema.fields[1].name, "name");

        let update = bind(
            parse("UPDATE documents SET title = 'changed' RETURNING *")
                .expect("parse update returning"),
            &catalog,
        )
        .expect("bind update returning");
        let BoundStatement::Update {
            returning: Some(returning),
            ..
        } = update
        else {
            panic!("update returning");
        };
        assert_eq!(returning.schema.fields.len(), 2);

        let delete = bind(
            parse("DELETE FROM documents RETURNING id").expect("parse delete returning"),
            &catalog,
        )
        .expect("bind delete returning");
        let BoundStatement::Delete {
            returning: Some(returning),
            ..
        } = delete
        else {
            panic!("delete returning");
        };
        assert_eq!(returning.schema.fields.len(), 1);
        assert_eq!(returning.schema.fields[0].data_type, ScalarType::Int64);
    }

    #[test]
    fn parses_and_binds_postgres_on_conflict_actions() {
        let mut catalog = catalog_with_documents();

        let do_nothing = bind(
            parse("INSERT INTO documents VALUES (1, 'one') ON CONFLICT DO NOTHING")
                .expect("parse conflict do nothing"),
            &catalog,
        )
        .expect("bind conflict do nothing");
        let BoundStatement::Insert {
            on_conflict:
                Some(BoundOnConflict {
                    target_columns,
                    action,
                }),
            ..
        } = do_nothing
        else {
            panic!("insert on conflict do nothing");
        };
        assert!(target_columns.is_none());
        assert!(matches!(action, BoundConflictAction::DoNothing));

        let do_update = bind(
            parse(
                "INSERT INTO documents VALUES (1, 'new') \
                 ON CONFLICT (id) DO UPDATE SET title = excluded.title \
                 WHERE documents.id = 1 RETURNING id, title",
            )
            .expect("parse conflict do update"),
            &catalog,
        )
        .expect("bind conflict do update");
        let BoundStatement::Insert {
            on_conflict:
                Some(BoundOnConflict {
                    target_columns: Some(target_columns),
                    action:
                        BoundConflictAction::DoUpdate {
                            assignments,
                            filter: Some(filter),
                        },
                }),
            returning: Some(returning),
            ..
        } = do_update
        else {
            panic!("insert on conflict do update");
        };
        assert_eq!(target_columns, vec![0]);
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].0, 1);
        assert!(matches!(
            assignments[0].1.kind,
            BoundExprKind::Column { index: 3 }
        ));
        assert_eq!(filter.data_type, ScalarType::Boolean);
        assert_eq!(returning.schema.fields.len(), 2);

        let table_id = catalog
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents table")
            .id;
        let constraint_name = Identifier::unquoted("documents_id_key");
        catalog
            .create_constraint(
                table_id,
                NewConstraint {
                    name: constraint_name.clone(),
                    kind: NewConstraintKind::Unique {
                        columns: vec![Identifier::unquoted("id")],
                    },
                },
            )
            .expect("create unique constraint");
        let by_constraint = bind(
            parse(&format!(
                "INSERT INTO documents VALUES (1, 'one') \
                 ON CONFLICT ON CONSTRAINT {} DO NOTHING",
                constraint_name.as_str()
            ))
            .expect("parse conflict constraint"),
            &catalog,
        )
        .expect("bind conflict constraint");
        let BoundStatement::Insert {
            on_conflict:
                Some(BoundOnConflict {
                    target_columns: Some(target_columns),
                    action: BoundConflictAction::DoNothing,
                }),
            ..
        } = by_constraint
        else {
            panic!("insert on conflict constraint");
        };
        assert_eq!(target_columns, vec![0]);
    }

    #[test]
    fn parses_and_binds_ordered_postgres_merge_actions() {
        let mut catalog = catalog_with_documents();
        catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("updates"),
                vec![
                    NewColumn {
                        name: Identifier::unquoted("id"),
                        data_type: ScalarType::Int64,
                        declared_type: None,
                        nullable: false,
                        primary_key: true,
                        unique: true,
                        default: None,
                    },
                    NewColumn {
                        name: Identifier::unquoted("title"),
                        data_type: ScalarType::Text,
                        declared_type: None,
                        nullable: false,
                        primary_key: false,
                        unique: false,
                        default: None,
                    },
                ],
            )
            .expect("create merge source");

        let bound = bind(
            parse(
                "MERGE INTO documents AS d USING updates AS u ON d.id = u.id \
                 WHEN MATCHED AND u.title <> 'skip' THEN UPDATE SET title = u.title \
                 WHEN MATCHED THEN DELETE \
                 WHEN NOT MATCHED BY TARGET THEN \
                 INSERT (id, title) VALUES (u.id, u.title) \
                 RETURNING id, title",
            )
            .expect("parse merge"),
            &catalog,
        )
        .expect("bind merge");
        let BoundStatement::Merge(BoundMerge {
            target,
            source,
            on,
            clauses,
            returning: Some(returning),
        }) = bound
        else {
            panic!("bound merge");
        };
        assert_eq!(target.binding.as_str(), "d");
        assert_eq!(target.offset, 0);
        assert_eq!(source.binding.as_str(), "u");
        assert_eq!(source.offset, 2);
        assert!(matches!(
            on.kind,
            BoundExprKind::Binary {
                left,
                right,
                ..
            } if matches!(left.kind, BoundExprKind::Column { index: 0 })
                && matches!(right.kind, BoundExprKind::Column { index: 2 })
        ));
        assert_eq!(clauses.len(), 3);
        assert!(matches!(
            &clauses[0],
            BoundMergeClause {
                kind: BoundMergeClauseKind::Matched,
                predicate: Some(_),
                action: BoundMergeAction::Update { assignments },
            } if assignments.len() == 1
                && assignments[0].0 == 1
                && matches!(assignments[0].1.kind, BoundExprKind::Column { index: 3 })
        ));
        assert!(matches!(clauses[1].action, BoundMergeAction::Delete));
        assert!(matches!(
            &clauses[2].action,
            BoundMergeAction::Insert {
                column_indexes,
                values,
            } if column_indexes == &[0, 1]
                && matches!(values[0].kind, BoundExprKind::Column { index: 2 })
                && matches!(values[1].kind, BoundExprKind::Column { index: 3 })
        ));
        assert_eq!(returning.schema.fields.len(), 2);

        let do_nothing = bind(
            parse(
                "MERGE INTO documents AS d USING updates AS u ON d.id = u.id \
                 WHEN MATCHED THEN DO NOTHING \
                 WHEN NOT MATCHED THEN DO NOTHING \
                 WHEN NOT MATCHED BY SOURCE THEN DO NOTHING",
            )
            .expect("parse MERGE DO NOTHING"),
            &catalog,
        )
        .expect("bind MERGE DO NOTHING");
        let BoundStatement::Merge(BoundMerge {
            clauses,
            returning: None,
            ..
        }) = do_nothing
        else {
            panic!("bound MERGE DO NOTHING");
        };
        assert!(matches!(
            clauses.as_slice(),
            [
                BoundMergeClause {
                    kind: BoundMergeClauseKind::Matched,
                    action: BoundMergeAction::DoNothing,
                    ..
                },
                BoundMergeClause {
                    kind: BoundMergeClauseKind::NotMatchedByTarget,
                    action: BoundMergeAction::DoNothing,
                    ..
                },
                BoundMergeClause {
                    kind: BoundMergeClauseKind::NotMatchedBySource,
                    action: BoundMergeAction::DoNothing,
                    ..
                }
            ]
        ));

        let audited = merge_clause_token_info(&significant_tokens(
            "MERGE INTO documents AS d USING updates AS u ON d.id = u.id \
             WHEN MATCHED AND CASE WHEN u.id = 1 THEN TRUE ELSE FALSE END \
                 THEN DO NOTHING \
             WHEN NOT MATCHED THEN INSERT (id, title) VALUES (u.id, u.title)",
        ))
        .expect("audit MERGE clauses around CASE");
        assert_eq!(audited.len(), 2);
        assert!(audited[0].do_nothing.is_some());
        assert!(audited[1].do_nothing.is_none());
    }

    #[test]
    fn merge_rejects_unrepresented_upstream_fields() {
        let catalog = catalog_with_documents();
        let derived = parse(
            "MERGE INTO documents AS d \
             USING (SELECT id, title FROM documents) AS u ON d.id = u.id \
             WHEN MATCHED THEN DELETE",
        )
        .expect_err("derived MERGE source");
        assert_eq!(derived.sql_state, FEATURE_NOT_SUPPORTED);

        let by_source = bind(
            parse(
                "MERGE INTO documents AS d USING documents AS u ON d.id = u.id \
             WHEN NOT MATCHED BY SOURCE AND u.title = 'missing' THEN DELETE",
            )
            .expect("parse BY SOURCE"),
            &catalog,
        )
        .expect_err("BY SOURCE source reference");
        assert_eq!(by_source.sql_state, UNDEFINED_TABLE);

        let missing_into = parse(
            "MERGE documents AS d USING documents AS u ON d.id = u.id \
             WHEN MATCHED THEN DELETE",
        )
        .expect_err("missing INTO");
        assert_eq!(missing_into.sql_state, SYNTAX_ERROR);

        let error = bind(
            parse(
                "MERGE INTO documents AS d USING documents AS u ON d.id = u.id \
                 WHEN MATCHED THEN UPDATE SET missing = u.title",
            )
            .expect("parse missing target column"),
            &catalog,
        )
        .expect_err("missing target column");
        assert_eq!(error.sql_state, UNDEFINED_COLUMN);
    }

    #[test]
    fn rejects_invalid_or_vendor_conflict_actions() {
        let catalog = catalog_with_documents();

        let error = bind(
            parse("INSERT INTO documents VALUES (1, 'one') ON CONFLICT (title) DO NOTHING")
                .expect("parse non-unique conflict target"),
            &catalog,
        )
        .expect_err("non-unique target");
        assert_eq!(error.sql_state, "42P10");

        let error = bind(
            parse(
                "INSERT INTO documents VALUES (1, 'one') \
                 ON CONFLICT DO UPDATE SET title = excluded.title",
            )
            .expect("parse targetless conflict update"),
            &catalog,
        )
        .expect_err("targetless update");
        assert_eq!(error.sql_state, SYNTAX_ERROR);

        let error = parse_with_dialect(
            "INSERT INTO documents VALUES (1, 'one') \
             ON DUPLICATE KEY UPDATE title = 'changed'",
            SqlDialect::MySql,
        )
        .expect_err("vendor conflict action");
        assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED);
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

    #[test]
    fn parses_and_binds_enum_domain_and_named_column_types() {
        let enum_statement =
            parse("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')").expect("parse enum");
        assert!(matches!(
            enum_statement,
            ParsedStatement::CreateEnumType { ref labels, .. }
                if labels == &["sad", "ok", "happy"]
        ));

        let domain_statement = parse(
            "CREATE DOMAIN positive_int AS integer DEFAULT 1 NOT NULL \
             CONSTRAINT positive CHECK (VALUE > 0)",
        )
        .expect("parse domain");
        assert!(matches!(
            bind(domain_statement, &Catalog::default()).expect("bind domain"),
            BoundStatement::CreateDomain { not_null: true, ref checks, .. }
                if checks.len() == 1
                    && checks[0].name.as_ref().is_some_and(|name| name.as_str() == "positive")
        ));

        let mut catalog = catalog_with_documents();
        let type_id = catalog
            .create_enum_type(
                &Identifier::unquoted("public"),
                Identifier::unquoted("mood"),
                vec!["sad".into(), "ok".into(), "happy".into()],
            )
            .expect("catalog enum");
        let domain_id = catalog
            .create_domain(
                &Identifier::unquoted("public"),
                Identifier::unquoted("positive_int"),
                ScalarType::Int32,
                true,
                None,
                Vec::new(),
            )
            .expect("catalog domain");
        let add_value = bind(
            parse("ALTER TYPE mood ADD VALUE IF NOT EXISTS 'calm' BEFORE 'happy'")
                .expect("parse enum add value"),
            &catalog,
        )
        .expect("bind enum add value");
        assert!(matches!(
            add_value,
            BoundStatement::AlterEnumAddValue {
                type_id: altered_type_id,
                ref label,
                position: Some(EnumValuePosition::Before(ref neighbor)),
                if_not_exists: true,
            } if altered_type_id == type_id && label == "calm" && neighbor == "happy"
        ));
        assert!(matches!(
            bind(
                parse("ALTER TYPE mood RENAME VALUE 'ok' TO 'fine'")
                    .expect("parse enum rename value"),
                &catalog,
            )
            .expect("bind enum rename value"),
            BoundStatement::AlterEnumRenameValue {
                type_id: altered_type_id,
                ref old_label,
                ref new_label,
            } if altered_type_id == type_id && old_label == "ok" && new_label == "fine"
        ));
        assert!(matches!(
            bind(
                parse("ALTER DOMAIN positive_int SET DEFAULT 2")
                    .expect("parse domain default"),
                &catalog,
            )
            .expect("bind domain default"),
            BoundStatement::AlterDomain {
                type_id: altered_type_id,
                operation: BoundAlterDomainOperation::SetDefault(ref default),
            } if altered_type_id == domain_id && default.sql == "2"
        ));
        assert!(matches!(
            bind(
                parse(
                    "ALTER DOMAIN positive_int ADD CONSTRAINT below_limit CHECK (VALUE < 100)",
                )
                .expect("parse domain constraint"),
                &catalog,
            )
            .expect("bind domain constraint"),
            BoundStatement::AlterDomain {
                type_id: altered_type_id,
                operation: BoundAlterDomainOperation::AddConstraint(ref constraint),
            } if altered_type_id == domain_id
                && constraint.name.as_ref().is_some_and(|name| name.as_str() == "below_limit")
        ));
        assert_eq!(
            bind(
                parse("ALTER DOMAIN mood SET NOT NULL").expect("parse wrong domain kind"),
                &catalog,
            )
            .expect_err("enum is not a domain")
            .sql_state,
            "42809"
        );
        let bound = bind(
            parse("CREATE TABLE feelings (current_mood mood NOT NULL)").expect("parse table"),
            &catalog,
        )
        .expect("bind table");
        assert!(matches!(
            bound,
            BoundStatement::CreateTable { ref columns, .. }
                if columns[0].declared_type == Some(type_id)
                    && columns[0].data_type == ScalarType::Enum {
                        type_id,
                        labels: vec!["sad".into(), "ok".into(), "happy".into()],
                    }
        ));

        let cast = bind(
            parse(
                "SELECT $1::mood, 'sad'::mood, $2::positive_int, \
                 ARRAY['sad', 'happy']::mood[] FROM documents",
            )
            .expect("parse named casts"),
            &catalog,
        )
        .expect("bind named casts");
        let BoundStatement::Select { projection, .. } = cast else {
            panic!("expected named cast select");
        };
        let enum_type = ScalarType::Enum {
            type_id,
            labels: vec!["sad".into(), "ok".into(), "happy".into()],
        };
        assert_eq!(projection[0].expr.data_type, enum_type);
        assert_eq!(projection[1].expr.data_type, enum_type);
        assert_eq!(projection[2].expr.data_type, ScalarType::Int32);
        assert_eq!(
            projection[3].expr.data_type,
            ScalarType::Array {
                element: Box::new(enum_type),
            }
        );

        let function = bind(
            parse(
                "CREATE FUNCTION echo_mood(value mood) RETURNS mood \
                 LANGUAGE plpgsql AS $$ BEGIN RETURN value; END $$",
            )
            .expect("parse named type function"),
            &catalog,
        )
        .expect("bind named type function");
        assert!(matches!(
            function,
            BoundStatement::CreateRoutine {
                ref arguments,
                return_declared_type: Some(return_type_id),
                ..
            } if arguments[0].declared_type == Some(type_id) && return_type_id == type_id
        ));

        let alter = bind(
            parse("ALTER TABLE documents ALTER COLUMN title TYPE mood")
                .expect("parse named type alter"),
            &catalog,
        )
        .expect("bind named type alter");
        assert!(matches!(
            alter,
            BoundStatement::AlterTable { ref operations, .. }
                if matches!(
                    operations.as_slice(),
                    [BoundAlterTableOperation::SetDataType {
                        declared_type: Some(alter_type_id),
                        ..
                    }] if *alter_type_id == type_id
                )
        ));

        let error = bind(
            parse("SELECT 'value'::missing_type FROM documents").expect("parse missing named cast"),
            &catalog,
        )
        .expect_err("undefined named cast type");
        assert_eq!(error.sql_state, "42704");

        let drop = bind(parse("DROP TYPE mood").expect("parse drop"), &catalog).expect("bind drop");
        assert!(matches!(
            drop,
            BoundStatement::DropObjects {
                kind: DdlObjectKind::Type,
                ..
            }
        ));

        let error = bind(
            parse("CREATE TABLE missing_type (value unknown_named_type)")
                .expect("parse unknown type"),
            &catalog,
        )
        .expect_err("undefined named type");
        assert_eq!(error.sql_state, "42704");

        let error = bind(
            parse("CREATE DOMAIN bad_check AS integer CHECK (missing > 0)")
                .expect("parse invalid domain check"),
            &catalog,
        )
        .expect_err("invalid domain check");
        assert_eq!(error.sql_state, UNDEFINED_COLUMN);

        assert!(!create_domain_is_not_null(
            "CREATE DOMAIN nullable_flag AS boolean DEFAULT NULL IS NOT NULL"
        ));

        for sql in [
            "CREATE TYPE shell_type",
            "CREATE TYPE inventory_item AS (name text)",
        ] {
            let error = parse(sql).expect_err("unsupported type definition");
            assert_eq!(error.sql_state, FEATURE_NOT_SUPPORTED, "{sql}");
        }
    }

    #[test]
    fn binds_named_domain_bases_catalog_expressions_and_routine_identity() {
        use ordadb_catalog::{DomainBaseType, NewRoutine};

        let mut catalog = Catalog::default();
        let mood_id = catalog
            .create_enum_type(
                &Identifier::unquoted("public"),
                Identifier::unquoted("mood"),
                vec!["sad".into(), "ok".into(), "happy".into()],
            )
            .expect("create mood");
        let mood_type = catalog.type_by_id(mood_id).expect("mood").logical_type();
        let mood_domain = bind(
            parse(
                "CREATE DOMAIN cheerful_mood AS mood DEFAULT 'ok'::mood \
                 CHECK (VALUE <> 'sad'::mood)",
            )
            .expect("parse enum domain"),
            &catalog,
        )
        .expect("bind enum domain");
        assert!(matches!(
            mood_domain,
            BoundStatement::CreateDomain {
                base_declared_type: Some(type_id),
                ref base_type,
                ..
            } if type_id == mood_id && base_type == &mood_type
        ));
        let cheerful_id = catalog
            .create_domain_with_declared_type(
                &Identifier::unquoted("public"),
                Identifier::unquoted("cheerful_mood"),
                DomainBaseType::new(mood_type, Some(mood_id)),
                false,
                Some(CatalogExpression::new("'ok'::mood")),
                Vec::new(),
            )
            .expect("create enum domain");
        assert!(matches!(
            bind(
                parse("ALTER DOMAIN cheerful_mood SET DEFAULT 'happy'::mood")
                    .expect("parse named domain default"),
                &catalog,
            )
            .expect("bind named domain default"),
            BoundStatement::AlterDomain {
                type_id,
                operation: BoundAlterDomainOperation::SetDefault(ref default),
            } if type_id == cheerful_id && default.sql == "'happy' :: mood"
        ));
        assert_eq!(
            bind(
                parse("CREATE DOMAIN nested_mood AS cheerful_mood").expect("parse nested domain"),
                &catalog,
            )
            .expect_err("nested domain base is explicit unsupported")
            .sql_state,
            FEATURE_NOT_SUPPORTED
        );

        let positive_id = catalog
            .create_domain(
                &Identifier::unquoted("public"),
                Identifier::unquoted("positive_int"),
                ScalarType::Int32,
                false,
                None,
                Vec::new(),
            )
            .expect("positive domain");
        let nonnegative_id = catalog
            .create_domain(
                &Identifier::unquoted("public"),
                Identifier::unquoted("nonnegative_int"),
                ScalarType::Int32,
                false,
                None,
                Vec::new(),
            )
            .expect("nonnegative domain");
        let create_routine = |name: &str, type_id: TypeId| NewRoutine {
            name: Identifier::unquoted(name),
            kind: RoutineKind::Function,
            arguments: vec![RoutineArgument {
                name: Some(Identifier::unquoted("value")),
                data_type: ScalarType::Int32,
                declared_type: Some(type_id),
                mode: RoutineArgumentMode::In,
            }],
            return_type: Some(ScalarType::Int32),
            return_declared_type: None,
            returns_set: false,
            language: "plpgsql".into(),
            body: "BEGIN RETURN value; END".into(),
            replace: false,
            references: vec![CatalogObjectRef::Type(type_id)],
        };
        let positive_routine = catalog
            .create_or_replace_routine(
                &Identifier::unquoted("public"),
                create_routine("choose_value", positive_id),
            )
            .expect("positive overload");
        catalog
            .create_or_replace_routine(
                &Identifier::unquoted("public"),
                create_routine("choose_value", nonnegative_id),
            )
            .expect("nonnegative overload");

        assert!(matches!(
            bind(
                parse("SELECT choose_value(1::positive_int)")
                    .expect("parse exact overload"),
                &catalog,
            )
            .expect("bind exact overload"),
            BoundStatement::RoutineSelect { routine_id, .. }
                if routine_id == positive_routine
        ));
        assert_eq!(
            bind(
                parse("SELECT choose_value(1)").expect("parse ambiguous overload"),
                &catalog,
            )
            .expect_err("same-base domains remain ambiguous without an exact declared type")
            .sql_state,
            "42725"
        );
        assert!(matches!(
            bind(
                parse("DROP FUNCTION choose_value(positive_int)")
                    .expect("parse named drop signature"),
                &catalog,
            )
            .expect("bind named drop signature"),
            BoundStatement::DropRoutine { routine_id, .. }
                if routine_id == positive_routine
        ));
    }

    fn parameter_indices(expression: &ParsedExpr) -> Vec<usize> {
        let mut parameters = Vec::new();
        let mut stack = vec![expression];
        while let Some(expression) = stack.pop() {
            match &expression.kind {
                ParsedExprKind::Parameter(index)
                | ParsedExprKind::ResolvedParameter { index, .. } => parameters.push(*index),
                ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                    stack.push(expr);
                }
                ParsedExprKind::Array { elements, .. } => stack.extend(elements),
                ParsedExprKind::Function { arguments, .. } => stack.extend(arguments),
                ParsedExprKind::Binary { left, right, .. } => {
                    stack.push(right);
                    stack.push(left);
                }
                ParsedExprKind::InList { expr, list, .. } => {
                    stack.extend(list);
                    stack.push(expr);
                }
                ParsedExprKind::InSubquery { expr, .. } => stack.push(expr),
                ParsedExprKind::QuantifiedSubquery { left, .. } => stack.push(left),
                ParsedExprKind::RowSubquery { left, .. } => stack.extend(left),
                ParsedExprKind::ScalarSubquery(_) | ParsedExprKind::Exists { .. } => {}
                ParsedExprKind::Aggregate {
                    argument, filter, ..
                } => {
                    if let Some(filter) = filter {
                        stack.push(filter);
                    }
                    if let Some(argument) = argument {
                        stack.push(argument);
                    }
                }
                ParsedExprKind::Window { call, spec } => {
                    if let Some(filter) = &call.filter {
                        stack.push(filter);
                    }
                    stack.extend(&call.arguments);
                    stack.extend(spec.order_by.iter().map(|order| &order.expr));
                    stack.extend(&spec.partition_by);
                }
                ParsedExprKind::NamedWindow { call, .. } => {
                    if let Some(filter) = &call.filter {
                        stack.push(filter);
                    }
                    stack.extend(&call.arguments);
                }
                ParsedExprKind::Column(_)
                | ParsedExprKind::Literal(_)
                | ParsedExprKind::ApplyValue { .. }
                | ParsedExprKind::WindowValue { .. } => {}
            }
        }
        parameters
    }
}
