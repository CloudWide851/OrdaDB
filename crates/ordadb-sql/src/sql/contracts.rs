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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StatementEffect {
    ReadOnly,
    RequiresApproval,
}

const MAX_STATEMENT_EFFECT_DEPTH: usize = 64;
const MAX_STATEMENT_EFFECT_NODES: usize = 65_536;
