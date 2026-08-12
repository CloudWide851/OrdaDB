
#[must_use]
pub fn classify_statement_effect(statement: &ParsedStatement) -> StatementEffect {
    let mut pending = vec![(EffectNode::Statement(statement), 0_usize)];
    let mut visited = 0_usize;
    while let Some((node, depth)) = pending.pop() {
        visited = visited.saturating_add(1);
        if depth > MAX_STATEMENT_EFFECT_DEPTH || visited > MAX_STATEMENT_EFFECT_NODES {
            return StatementEffect::RequiresApproval;
        }
        let child_depth = depth.saturating_add(1);
        match node {
            EffectNode::Statement(statement) => match statement {
                ParsedStatement::ScalarSelect { projection } => {
                    push_effect_projections(&mut pending, projection, child_depth);
                }
                ParsedStatement::Select {
                    projection,
                    filter,
                    order_by,
                    offset,
                    limit,
                    ..
                } => {
                    push_effect_projections(&mut pending, projection, child_depth);
                    push_effect_optional_expr(&mut pending, filter.as_ref(), child_depth);
                    push_effect_orders(&mut pending, order_by, child_depth);
                    push_effect_optional_expr(&mut pending, offset.as_ref(), child_depth);
                    push_effect_optional_expr(&mut pending, limit.as_ref(), child_depth);
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
                        if let ParsedJoinSource::Derived { query, .. } = &join.source {
                            pending.push((EffectNode::Statement(query), child_depth));
                        }
                        pending.push((EffectNode::Expr(&join.on), child_depth));
                    }
                    push_effect_projections(&mut pending, projection, child_depth);
                    push_effect_optional_expr(&mut pending, filter.as_ref(), child_depth);
                    push_effect_exprs(&mut pending, group_by, child_depth);
                    push_effect_optional_expr(&mut pending, having.as_ref(), child_depth);
                    push_effect_orders(&mut pending, order_by, child_depth);
                    push_effect_optional_expr(&mut pending, offset.as_ref(), child_depth);
                    push_effect_optional_expr(&mut pending, limit.as_ref(), child_depth);
                }
                ParsedStatement::With { ctes, body, .. } => {
                    pending.push((EffectNode::Statement(body), child_depth));
                    pending.extend(
                        ctes.iter()
                            .map(|cte| (EffectNode::Statement(cte.query.as_ref()), child_depth)),
                    );
                }
                ParsedStatement::SetOperation {
                    left,
                    right,
                    order_by,
                    offset,
                    limit,
                    ..
                } => {
                    pending.push((EffectNode::Statement(left), child_depth));
                    pending.push((EffectNode::Statement(right), child_depth));
                    push_effect_orders(&mut pending, order_by, child_depth);
                    push_effect_optional_expr(&mut pending, offset.as_ref(), child_depth);
                    push_effect_optional_expr(&mut pending, limit.as_ref(), child_depth);
                }
                ParsedStatement::Explain { statement } => {
                    pending.push((EffectNode::Statement(statement), child_depth));
                }
                ParsedStatement::Begin { .. }
                | ParsedStatement::Commit { .. }
                | ParsedStatement::Rollback { .. }
                | ParsedStatement::Savepoint { .. }
                | ParsedStatement::RollbackTo { .. }
                | ParsedStatement::ReleaseSavepoint { .. }
                | ParsedStatement::Analyze { .. }
                | ParsedStatement::Vacuum { .. }
                | ParsedStatement::Reindex { .. }
                | ParsedStatement::Listen { .. }
                | ParsedStatement::Unlisten { .. }
                | ParsedStatement::Notify { .. }
                | ParsedStatement::Do { .. }
                | ParsedStatement::DiscardAll
                | ParsedStatement::DeallocateAll
                | ParsedStatement::CreateSchema { .. }
                | ParsedStatement::CreateEnumType { .. }
                | ParsedStatement::CreateDomain { .. }
                | ParsedStatement::AlterEnumAddValue { .. }
                | ParsedStatement::AlterEnumRenameValue { .. }
                | ParsedStatement::AlterDomain { .. }
                | ParsedStatement::AlterSchemaRename { .. }
                | ParsedStatement::DropObjects { .. }
                | ParsedStatement::CreateTable { .. }
                | ParsedStatement::AlterTable { .. }
                | ParsedStatement::CreateIndex(_)
                | ParsedStatement::AlterIndexRename { .. }
                | ParsedStatement::CreateSequence { .. }
                | ParsedStatement::AlterSequenceRename { .. }
                | ParsedStatement::AlterSequence { .. }
                | ParsedStatement::CreateView { .. }
                | ParsedStatement::AlterViewRename { .. }
                | ParsedStatement::RefreshMaterializedView { .. }
                | ParsedStatement::CreateRoutine { .. }
                | ParsedStatement::DropRoutine { .. }
                | ParsedStatement::Call { .. }
                | ParsedStatement::RoutineSelect { .. }
                | ParsedStatement::PgNotify { .. }
                | ParsedStatement::SequenceValue { .. }
                | ParsedStatement::CreateTrigger { .. }
                | ParsedStatement::DropTrigger { .. }
                | ParsedStatement::Insert { .. }
                | ParsedStatement::Merge(_)
                | ParsedStatement::Update { .. }
                | ParsedStatement::Delete { .. } => {
                    return StatementEffect::RequiresApproval;
                }
            },
            EffectNode::Expr(expr) => match &expr.kind {
                ParsedExprKind::Column(_)
                | ParsedExprKind::Literal(_)
                | ParsedExprKind::Parameter(_)
                | ParsedExprKind::ResolvedParameter { .. }
                | ParsedExprKind::ApplyValue { .. }
                | ParsedExprKind::WindowValue { .. } => {}
                ParsedExprKind::Unary { expr, .. } | ParsedExprKind::Cast { expr, .. } => {
                    pending.push((EffectNode::Expr(expr), child_depth));
                }
                ParsedExprKind::Array { elements, .. } => {
                    push_effect_exprs(&mut pending, elements, child_depth);
                }
                ParsedExprKind::Function { arguments, .. } => {
                    push_effect_exprs(&mut pending, arguments, child_depth);
                }
                ParsedExprKind::Binary { left, right, .. } => {
                    pending.push((EffectNode::Expr(left), child_depth));
                    pending.push((EffectNode::Expr(right), child_depth));
                }
                ParsedExprKind::InList { expr, list, .. } => {
                    pending.push((EffectNode::Expr(expr), child_depth));
                    push_effect_exprs(&mut pending, list, child_depth);
                }
                ParsedExprKind::ScalarSubquery(query)
                | ParsedExprKind::Exists {
                    subquery: query, ..
                } => pending.push((EffectNode::Statement(query), child_depth)),
                ParsedExprKind::InSubquery { expr, subquery, .. }
                | ParsedExprKind::QuantifiedSubquery {
                    left: expr,
                    subquery,
                    ..
                } => {
                    pending.push((EffectNode::Expr(expr), child_depth));
                    pending.push((EffectNode::Statement(subquery), child_depth));
                }
                ParsedExprKind::RowSubquery { left, subquery, .. } => {
                    push_effect_exprs(&mut pending, left, child_depth);
                    pending.push((EffectNode::Statement(subquery), child_depth));
                }
                ParsedExprKind::Aggregate {
                    argument, filter, ..
                } => {
                    push_effect_optional_expr(&mut pending, argument.as_deref(), child_depth);
                    push_effect_optional_expr(&mut pending, filter.as_deref(), child_depth);
                }
                ParsedExprKind::Window { call, spec } => {
                    push_effect_exprs(&mut pending, &call.arguments, child_depth);
                    push_effect_optional_expr(&mut pending, call.filter.as_deref(), child_depth);
                    push_effect_exprs(&mut pending, &spec.partition_by, child_depth);
                    push_effect_orders(&mut pending, &spec.order_by, child_depth);
                    if let Some(frame) = &spec.frame {
                        push_effect_window_bound(&mut pending, &frame.start_bound, child_depth);
                        push_effect_window_bound(&mut pending, &frame.end_bound, child_depth);
                    }
                }
                ParsedExprKind::NamedWindow { call, .. } => {
                    push_effect_exprs(&mut pending, &call.arguments, child_depth);
                    push_effect_optional_expr(&mut pending, call.filter.as_deref(), child_depth);
                }
            },
        }
    }
    StatementEffect::ReadOnly
}

enum EffectNode<'a> {
    Statement(&'a ParsedStatement),
    Expr(&'a ParsedExpr),
}

fn push_effect_projections<'a>(
    pending: &mut Vec<(EffectNode<'a>, usize)>,
    projections: &'a [ParsedProjection],
    depth: usize,
) {
    pending.extend(
        projections
            .iter()
            .filter_map(|projection| match projection {
                ParsedProjection::Wildcard => None,
                ParsedProjection::Expression { expr, .. } => Some((EffectNode::Expr(expr), depth)),
            }),
    );
}

fn push_effect_orders<'a>(
    pending: &mut Vec<(EffectNode<'a>, usize)>,
    orders: &'a [ParsedOrder],
    depth: usize,
) {
    pending.extend(
        orders
            .iter()
            .map(|order| (EffectNode::Expr(&order.expr), depth)),
    );
}

fn push_effect_exprs<'a>(
    pending: &mut Vec<(EffectNode<'a>, usize)>,
    expressions: &'a [ParsedExpr],
    depth: usize,
) {
    pending.extend(
        expressions
            .iter()
            .map(|expr| (EffectNode::Expr(expr), depth)),
    );
}

fn push_effect_optional_expr<'a>(
    pending: &mut Vec<(EffectNode<'a>, usize)>,
    expression: Option<&'a ParsedExpr>,
    depth: usize,
) {
    if let Some(expression) = expression {
        pending.push((EffectNode::Expr(expression), depth));
    }
}

fn push_effect_window_bound<'a>(
    pending: &mut Vec<(EffectNode<'a>, usize)>,
    bound: &'a ParsedWindowFrameBound,
    depth: usize,
) {
    match bound {
        ParsedWindowFrameBound::Preceding(expression)
        | ParsedWindowFrameBound::Following(expression) => {
            pending.push((EffectNode::Expr(expression), depth));
        }
        ParsedWindowFrameBound::UnboundedPreceding
        | ParsedWindowFrameBound::CurrentRow
        | ParsedWindowFrameBound::UnboundedFollowing => {}
    }
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
