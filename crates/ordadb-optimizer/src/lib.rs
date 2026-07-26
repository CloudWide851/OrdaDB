//! Deterministic rule and base-cost planning for OrdaDB.

use std::collections::BTreeSet;

use ordadb_catalog::{IndexMethod, TableDefinition};
use ordadb_sql::{BinaryOperator, BoundExpr, BoundExprKind, BoundOrder, BoundProjection};
use ordadb_types::{IndexId, TableId};

const ROWS_PER_PAGE: f64 = 64.0;
const SEQUENTIAL_PAGE_COST: f64 = 1.0;
const RANDOM_PAGE_COST: f64 = 4.0;
const CPU_TUPLE_COST: f64 = 0.01;
const CPU_INDEX_TUPLE_COST: f64 = 0.005;
const HEAP_FETCH_COST: f64 = 0.02;

#[derive(Debug, Clone, PartialEq)]
pub enum AccessPath {
    Empty,
    Sequential,
    Index {
        index_id: IndexId,
        column_index: usize,
        operator: BinaryOperator,
        value: BoundExpr,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlanKind {
    Scan {
        table_id: TableId,
        access: AccessPath,
        required_columns: Vec<usize>,
    },
    Filter {
        predicate: BoundExpr,
        input: Box<PlanNode>,
    },
    Projection {
        expressions: Vec<BoundProjection>,
        input: Box<PlanNode>,
    },
    Sort {
        order_by: Vec<BoundOrder>,
        input: Box<PlanNode>,
    },
    Limit {
        limit: BoundExpr,
        input: Box<PlanNode>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanNode {
    pub kind: PlanKind,
    pub estimated_rows: f64,
    pub estimated_cost: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    NestedLoop,
    Hash,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinChoice {
    pub strategy: JoinStrategy,
    pub estimated_rows: f64,
    pub estimated_cost: f64,
}

#[must_use]
pub fn choose_join_strategy(left_rows: u64, right_rows: u64, equi_join: bool) -> JoinChoice {
    let left = left_rows as f64;
    let right = right_rows as f64;
    let nested_cost = left.max(1.0) * right.max(1.0) * CPU_TUPLE_COST;
    let hash_cost = (left + right) * (CPU_TUPLE_COST + CPU_INDEX_TUPLE_COST);
    let strategy = if equi_join && hash_cost < nested_cost {
        JoinStrategy::Hash
    } else {
        JoinStrategy::NestedLoop
    };
    JoinChoice {
        strategy,
        estimated_rows: left.max(right).max(1.0),
        estimated_cost: match strategy {
            JoinStrategy::NestedLoop => nested_cost,
            JoinStrategy::Hash => hash_cost,
        },
    }
}

pub fn optimize_select(
    table: &TableDefinition,
    projection: Vec<BoundProjection>,
    filter: Option<BoundExpr>,
    order_by: Vec<BoundOrder>,
    limit: Option<BoundExpr>,
) -> PlanNode {
    let mut required_columns = BTreeSet::new();
    for projection in &projection {
        collect_columns(&projection.expr, &mut required_columns);
    }
    if let Some(filter) = &filter {
        collect_columns(filter, &mut required_columns);
    }
    required_columns.extend(order_by.iter().map(|order| order.column_index));
    let required_columns = required_columns.into_iter().collect::<Vec<_>>();
    let constant_filter = filter.as_ref().and_then(constant_truth);
    let filter = if constant_filter == Some(true) {
        None
    } else {
        filter
    };
    let row_count = table.statistics().row_count as f64;
    let sequential_cost = (row_count / ROWS_PER_PAGE).ceil().max(1.0) * SEQUENTIAL_PAGE_COST
        + row_count * CPU_TUPLE_COST;
    let candidate = filter
        .as_ref()
        .and_then(|predicate| index_candidate(table, predicate));
    let (access, estimated_rows, scan_cost) = if matches!(constant_filter, Some(false)) {
        (AccessPath::Empty, 0.0, 0.0)
    } else {
        candidate.map_or_else(
            || (AccessPath::Sequential, row_count, sequential_cost),
            |(access, selectivity)| {
                let estimated_rows = (row_count * selectivity).max(1.0).min(row_count.max(1.0));
                let height = if row_count <= 1.0 {
                    1.0
                } else {
                    row_count.log(32.0).ceil().max(1.0)
                };
                let index_cost = height * RANDOM_PAGE_COST
                    + estimated_rows * (CPU_INDEX_TUPLE_COST + HEAP_FETCH_COST);
                if index_cost < sequential_cost {
                    (access, estimated_rows, index_cost)
                } else {
                    (AccessPath::Sequential, row_count, sequential_cost)
                }
            },
        )
    };
    let mut plan = PlanNode {
        kind: PlanKind::Scan {
            table_id: table.id,
            access,
            required_columns,
        },
        estimated_rows,
        estimated_cost: scan_cost,
    };
    if let Some(predicate) = filter {
        plan = PlanNode {
            estimated_rows,
            estimated_cost: plan.estimated_cost + estimated_rows * CPU_TUPLE_COST,
            kind: PlanKind::Filter {
                predicate,
                input: Box::new(plan),
            },
        };
    }
    if !order_by.is_empty() {
        let sort_cost = if estimated_rows <= 1.0 {
            0.0
        } else {
            estimated_rows * estimated_rows.log2() * CPU_TUPLE_COST
        };
        plan = PlanNode {
            estimated_rows,
            estimated_cost: plan.estimated_cost + sort_cost,
            kind: PlanKind::Sort {
                order_by,
                input: Box::new(plan),
            },
        };
    }
    if let Some(limit) = limit {
        plan = PlanNode {
            estimated_rows,
            estimated_cost: plan.estimated_cost,
            kind: PlanKind::Limit {
                limit,
                input: Box::new(plan),
            },
        };
    }
    PlanNode {
        estimated_rows,
        estimated_cost: plan.estimated_cost + estimated_rows * CPU_TUPLE_COST,
        kind: PlanKind::Projection {
            expressions: projection,
            input: Box::new(plan),
        },
    }
}

fn index_candidate(table: &TableDefinition, predicate: &BoundExpr) -> Option<(AccessPath, f64)> {
    let BoundExprKind::Binary { left, op, right } = &predicate.kind else {
        return None;
    };
    if !matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    ) {
        return None;
    }
    let (column_index, operator, value) = match (&left.kind, &right.kind) {
        (
            BoundExprKind::Column { index },
            BoundExprKind::Literal(_) | BoundExprKind::Parameter { .. },
        ) => (*index, *op, (**right).clone()),
        (
            BoundExprKind::Literal(_) | BoundExprKind::Parameter { .. },
            BoundExprKind::Column { index },
        ) => (*index, reverse_operator(*op), (**left).clone()),
        _ => return None,
    };
    let column = table.columns().get(column_index)?;
    let index = table
        .indexes()
        .filter(|index| index.method == IndexMethod::BTree && index.key_columns.len() == 1)
        .find(|index| index.key_columns[0] == column.id)?;
    let stats = table.statistics().columns.get(&column.id);
    let selectivity = if operator == BinaryOperator::Eq {
        stats.map_or(0.1, |stats| 1.0 / (stats.distinct_count.max(1) as f64))
    } else {
        0.25
    };
    Some((
        AccessPath::Index {
            index_id: index.id,
            column_index,
            operator,
            value,
        },
        selectivity,
    ))
}

fn collect_columns(expr: &BoundExpr, columns: &mut BTreeSet<usize>) {
    match &expr.kind {
        BoundExprKind::Column { index } => {
            columns.insert(*index);
        }
        BoundExprKind::Unary { expr, .. } => collect_columns(expr, columns),
        BoundExprKind::Binary { left, right, .. } => {
            collect_columns(left, columns);
            collect_columns(right, columns);
        }
        BoundExprKind::Aggregate { argument, .. } => {
            if let Some(argument) = argument {
                collect_columns(argument, columns);
            }
        }
        BoundExprKind::Literal(_) | BoundExprKind::Parameter { .. } => {}
    }
}

fn constant_truth(expr: &BoundExpr) -> Option<bool> {
    match &expr.kind {
        BoundExprKind::Literal(ordadb_types::Value::Boolean(value)) => Some(*value),
        BoundExprKind::Literal(ordadb_types::Value::Null) => Some(false),
        BoundExprKind::Unary {
            op: ordadb_sql::UnaryOperator::Not,
            expr,
        } => constant_truth(expr).map(|value| !value),
        BoundExprKind::Binary { left, op, right } => {
            let left = constant_truth(left)?;
            let right = constant_truth(right)?;
            match op {
                BinaryOperator::And => Some(left && right),
                BinaryOperator::Or => Some(left || right),
                _ => None,
            }
        }
        _ => None,
    }
}

const fn reverse_operator(operator: BinaryOperator) -> BinaryOperator {
    match operator {
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        other => other,
    }
}

#[must_use]
pub fn explain(plan: &PlanNode) -> Vec<String> {
    let mut lines = Vec::new();
    explain_node(plan, 0, &mut lines);
    lines
}

fn explain_node(plan: &PlanNode, depth: usize, lines: &mut Vec<String>) {
    let operator = match &plan.kind {
        PlanKind::Scan {
            access: AccessPath::Empty,
            ..
        } => "Result (empty)".to_owned(),
        PlanKind::Scan {
            access: AccessPath::Sequential,
            ..
        } => "Seq Scan".to_owned(),
        PlanKind::Scan {
            access: AccessPath::Index { index_id, .. },
            ..
        } => format!("Index Scan using #{}", index_id.get()),
        PlanKind::Filter { .. } => "Filter".to_owned(),
        PlanKind::Projection { .. } => "Projection".to_owned(),
        PlanKind::Sort { .. } => "Sort".to_owned(),
        PlanKind::Limit { .. } => "Limit".to_owned(),
    };
    lines.push(format!(
        "{}{}  (cost={:.2} rows={:.0})",
        "  ".repeat(depth),
        operator,
        plan.estimated_cost,
        plan.estimated_rows
    ));
    match &plan.kind {
        PlanKind::Filter { input, .. }
        | PlanKind::Projection { input, .. }
        | PlanKind::Sort { input, .. }
        | PlanKind::Limit { input, .. } => explain_node(input, depth + 1, lines),
        PlanKind::Scan { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use ordadb_catalog::{Catalog, NewColumn};
    use ordadb_sql::{BoundStatement, bind, parse};
    use ordadb_types::{Identifier, ScalarType};

    use super::{AccessPath, PlanKind, explain, optimize_select};

    #[test]
    fn chooses_index_for_selective_large_table_and_seq_scan_for_small_table() {
        let mut catalog = Catalog::default();
        let mut id = NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64);
        id.primary_key = true;
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("items"),
                vec![id],
            )
            .expect("table");
        let table = catalog.table_by_id(table_id).expect("table").clone();
        let BoundStatement::Select {
            projection,
            filter,
            order_by,
            limit,
            ..
        } = bind(
            parse("SELECT id FROM items WHERE id = $1").expect("parse"),
            &catalog,
        )
        .expect("bind")
        else {
            panic!("select");
        };
        let small = optimize_select(
            &table,
            projection.clone(),
            filter.clone(),
            order_by.clone(),
            limit.clone(),
        );
        assert!(matches!(scan_access(&small), AccessPath::Sequential));

        let mut stats = table.statistics().clone();
        stats.row_count = 10_000;
        stats.columns.insert(
            table.columns()[0].id,
            ordadb_catalog::ColumnStatistics {
                distinct_count: 10_000,
                ..Default::default()
            },
        );
        catalog
            .set_table_statistics(table_id, stats)
            .expect("statistics");
        let large = optimize_select(
            catalog.table_by_id(table_id).expect("table"),
            projection,
            filter,
            order_by,
            limit,
        );
        assert!(matches!(scan_access(&large), AccessPath::Index { .. }));
        assert!(
            explain(&large)
                .iter()
                .any(|line| line.contains("Index Scan"))
        );
    }

    #[test]
    fn chooses_hash_only_when_an_equi_join_is_large_enough() {
        assert_eq!(
            super::choose_join_strategy(2, 2, true).strategy,
            super::JoinStrategy::NestedLoop
        );
        assert_eq!(
            super::choose_join_strategy(1_000, 1_000, true).strategy,
            super::JoinStrategy::Hash
        );
        assert_eq!(
            super::choose_join_strategy(1_000, 1_000, false).strategy,
            super::JoinStrategy::NestedLoop
        );
    }

    #[test]
    fn folds_constant_false_and_tracks_required_scan_columns() {
        let mut catalog = Catalog::default();
        let table_id = catalog
            .create_table(
                &Identifier::unquoted("public"),
                Identifier::unquoted("items"),
                vec![
                    NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                    NewColumn::new(Identifier::unquoted("payload"), ScalarType::Text),
                ],
            )
            .expect("table");
        let BoundStatement::Select {
            projection,
            filter,
            order_by,
            limit,
            ..
        } = bind(
            parse("SELECT payload FROM items WHERE FALSE").expect("parse"),
            &catalog,
        )
        .expect("bind")
        else {
            panic!("select");
        };
        let plan = optimize_select(
            catalog.table_by_id(table_id).expect("table"),
            projection,
            filter,
            order_by,
            limit,
        );
        assert!(matches!(scan_access(&plan), AccessPath::Empty));
        assert_eq!(scan_required_columns(&plan), &[1]);
    }

    fn scan_access(plan: &super::PlanNode) -> &AccessPath {
        match &plan.kind {
            PlanKind::Scan { access, .. } => access,
            PlanKind::Filter { input, .. }
            | PlanKind::Projection { input, .. }
            | PlanKind::Sort { input, .. }
            | PlanKind::Limit { input, .. } => scan_access(input),
        }
    }

    fn scan_required_columns(plan: &super::PlanNode) -> &[usize] {
        match &plan.kind {
            PlanKind::Scan {
                required_columns, ..
            } => required_columns,
            PlanKind::Filter { input, .. }
            | PlanKind::Projection { input, .. }
            | PlanKind::Sort { input, .. }
            | PlanKind::Limit { input, .. } => scan_required_columns(input),
        }
    }
}
