use std::collections::BTreeMap;

use ordadb_sql::{BoundExpr, BoundExprKind, BoundProjection, BoundTable, JoinKind};
use ordadb_types::{Field, Identifier, IndexId, ScalarType, TableId};
use tempfile::tempdir;

use super::*;

fn column(index: usize, data_type: ScalarType) -> BoundExpr {
    BoundExpr {
        kind: BoundExprKind::Column { index },
        data_type,
        nullable: false,
    }
}

fn projection(index: usize, name: &str) -> BoundProjection {
    BoundProjection {
        expr: column(index, ScalarType::Int64),
        field: Field::new(name, ScalarType::Int64, false),
    }
}

fn table(table_id: TableId, binding: &str, offset: usize) -> BoundTable {
    BoundTable {
        table_id,
        binding: Identifier::unquoted(binding),
        offset,
        width: 1,
        nullable: false,
    }
}

#[test]
fn nested_query_options_decrement_and_enforce_the_plan_depth_budget() {
    let memory = QueryMemoryContext::new(1024, 4096).expect("memory grant");
    let nested = nested_execution_options(
        &ExecutionOptions {
            max_plan_depth: 2,
            ..ExecutionOptions::default()
        },
        &memory,
    )
    .expect("one nested query level");
    assert_eq!(nested.max_plan_depth, 1);

    let error = nested_execution_options(&nested, &memory)
        .expect_err("nested query depth must be exhausted");
    assert_eq!(error.sql_state, "54001");
}

#[test]
fn ranking_windows_partition_rank_and_preserve_stable_source_order() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let rows = vec![
        Row::new(vec![Value::Int64(2), Value::Text("a".to_owned())]),
        Row::new(vec![Value::Int64(1), Value::Text("a".to_owned())]),
        Row::new(vec![Value::Int64(5), Value::Text("b".to_owned())]),
        Row::new(vec![Value::Int64(2), Value::Text("a".to_owned())]),
    ];
    let tables = BTreeMap::from([(table_id, Arc::new(rows))]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let window = |function| BoundWindow {
        function,
        value_index: 2,
        arguments: Vec::new(),
        count_star: false,
        filter: None,
        partition_by: vec![column(1, ScalarType::Text)],
        order_by: vec![BoundOrder {
            column_index: 0,
            expression: None,
            data_type: ScalarType::Int64,
            ascending: true,
            nulls_first: None,
        }],
        frame: None,
        data_type: ScalarType::Int64,
        nullable: false,
    };
    let plan = AdvancedExecutionPlan {
        table: BoundTable {
            table_id,
            binding: Identifier::unquoted("items"),
            offset: 0,
            width: 2,
            nullable: false,
        },
        joins: Vec::new(),
        applies: Vec::new(),
        windows: vec![
            window(WindowFunction::RowNumber),
            window(WindowFunction::Rank),
            window(WindowFunction::DenseRank),
        ],
        schema: Schema::new(vec![
            Field::new("id", ScalarType::Int64, false),
            Field::new("group", ScalarType::Text, false),
            Field::new("row_no", ScalarType::Int64, false),
            Field::new("rank_no", ScalarType::Int64, false),
            Field::new("dense_no", ScalarType::Int64, false),
        ]),
        projection: vec![
            projection(0, "id"),
            BoundProjection {
                expr: column(1, ScalarType::Text),
                field: Field::new("group", ScalarType::Text, false),
            },
            projection(2, "row_no"),
            projection(3, "rank_no"),
            projection(4, "dense_no"),
        ],
        distinct: false,
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: false,
    };
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan.clone(),
        &context,
        ExecutionOptions {
            batch_rows: 2,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("ranking cursor");
    let mut actual = Vec::new();
    while let Some(batch) = cursor.next_batch().expect("ranking batch") {
        actual.extend(batch.rows);
    }
    assert_eq!(
        actual,
        vec![
            Row::new(vec![
                Value::Int64(2),
                Value::Text("a".to_owned()),
                Value::Int64(2),
                Value::Int64(2),
                Value::Int64(2),
            ]),
            Row::new(vec![
                Value::Int64(1),
                Value::Text("a".to_owned()),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
            ]),
            Row::new(vec![
                Value::Int64(5),
                Value::Text("b".to_owned()),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
            ]),
            Row::new(vec![
                Value::Int64(2),
                Value::Text("a".to_owned()),
                Value::Int64(3),
                Value::Int64(2),
                Value::Int64(2),
            ]),
        ]
    );
    assert_eq!(cursor.memory().current_bytes(), 0);
    assert!(
        std::fs::read_dir(spill_root.path())
            .expect("in-memory window spill root")
            .next()
            .is_none()
    );

    let mut ordered = plan;
    ordered.order_by = vec![BoundOrder {
        column_index: 2,
        expression: None,
        data_type: ScalarType::Int64,
        ascending: false,
        nulls_first: None,
    }];
    let mut ordered_cursor =
        AdvancedExecutionCursor::new(ordered, &context).expect("ordered ranking cursor");
    let first = ordered_cursor
        .next_batch()
        .expect("ordered ranking batch")
        .expect("ordered rows");
    assert_eq!(first.rows[0].values[2], Value::Int64(3));
}

#[test]
fn ranking_windows_spill_one_large_partition_across_multiple_programs() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let rows = (0..128)
        .rev()
        .map(|value| Row::new(vec![Value::Int64(value)]))
        .collect::<Vec<_>>();
    let tables = BTreeMap::from([(table_id, Arc::new(rows))]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let window = |function, value_index| BoundWindow {
        function,
        value_index,
        arguments: Vec::new(),
        count_star: false,
        filter: None,
        partition_by: Vec::new(),
        order_by: vec![BoundOrder {
            column_index: 0,
            expression: None,
            data_type: ScalarType::Int64,
            ascending: true,
            nulls_first: None,
        }],
        frame: None,
        data_type: ScalarType::Int64,
        nullable: false,
    };
    let plan = AdvancedExecutionPlan {
        table: table(table_id, "items", 0),
        joins: Vec::new(),
        applies: Vec::new(),
        windows: vec![
            window(WindowFunction::RowNumber, 1),
            window(WindowFunction::Rank, 2),
        ],
        schema: Schema::new(vec![
            Field::new("id", ScalarType::Int64, false),
            Field::new("row_no", ScalarType::Int64, false),
            Field::new("rank_no", ScalarType::Int64, false),
        ]),
        projection: vec![
            projection(0, "id"),
            projection(1, "row_no"),
            projection(2, "rank_no"),
        ],
        distinct: false,
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: false,
    };
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan,
        &context,
        ExecutionOptions {
            batch_rows: 17,
            soft_memory_bytes: 512,
            hard_memory_bytes: 256 * 1024,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("spilling window cursor");
    let mut actual = Vec::new();
    while let Some(batch) = cursor.next_batch().expect("spilling window batch") {
        actual.extend(batch.rows);
    }
    assert_eq!(actual.len(), 128);
    for (ordinal, row) in actual.iter().enumerate() {
        let value = 127_i64.saturating_sub(i64::try_from(ordinal).expect("row ordinal"));
        assert_eq!(
            row.values,
            vec![
                Value::Int64(value),
                Value::Int64(value.saturating_add(1)),
                Value::Int64(value.saturating_add(1)),
            ]
        );
    }
    assert!(
        std::fs::read_dir(spill_root.path())
            .expect("spill root entries")
            .next()
            .is_some()
    );
    let memory = cursor.memory().clone();
    drop(cursor);
    assert_eq!(memory.current_bytes(), 0);
    assert!(
        std::fs::read_dir(spill_root.path())
            .expect("clean spill root")
            .next()
            .is_none()
    );
}

#[test]
fn spilling_window_cancellation_releases_grants_and_drop_cleans_files() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let rows = (0..50_000)
        .rev()
        .map(|value| Row::new(vec![Value::Int64(value)]))
        .collect::<Vec<_>>();
    let tables = BTreeMap::from([(table_id, Arc::new(rows))]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let plan = AdvancedExecutionPlan {
        table: table(table_id, "items", 0),
        joins: Vec::new(),
        applies: Vec::new(),
        windows: vec![BoundWindow {
            function: WindowFunction::RowNumber,
            value_index: 1,
            arguments: Vec::new(),
            count_star: false,
            filter: None,
            partition_by: Vec::new(),
            order_by: vec![BoundOrder {
                column_index: 0,
                expression: None,
                data_type: ScalarType::Int64,
                ascending: true,
                nulls_first: None,
            }],
            frame: None,
            data_type: ScalarType::Int64,
            nullable: false,
        }],
        schema: Schema::new(vec![Field::new("row_no", ScalarType::Int64, false)]),
        projection: vec![projection(1, "row_no")],
        distinct: false,
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: false,
    };
    let cancellation = Arc::new(AtomicBool::new(false));
    let watcher_cancellation = Arc::clone(&cancellation);
    let watcher_root = spill_root.path().to_path_buf();
    let watcher = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if std::fs::read_dir(&watcher_root)
                .ok()
                .is_some_and(|mut entries| entries.next().is_some())
            {
                watcher_cancellation.store(true, AtomicOrdering::Release);
                return true;
            }
            std::thread::yield_now();
        }
        false
    });
    let mut cursor = AdvancedExecutionCursor::with_options_and_cancellation(
        plan,
        &context,
        ExecutionOptions {
            soft_memory_bytes: 512,
            hard_memory_bytes: 16 * 1024 * 1024,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
        Some(cancellation),
    )
    .expect("cancellable spilling window cursor");
    let error = cursor
        .next_batch()
        .expect_err("spilling window must observe cancellation");
    assert!(watcher.join().expect("cancellation watcher"));
    assert_eq!(error.sql_state, "57014");
    assert_eq!(cursor.memory().current_bytes(), 0);
    drop(cursor);
    assert!(
        std::fs::read_dir(spill_root.path())
            .expect("clean spill root")
            .next()
            .is_none()
    );
}

#[test]
fn ranking_window_hard_limit_fails_and_releases_state() {
    let table_id = TableId::new(1);
    let rows = (0..16)
        .map(|value| Row::new(vec![Value::Int64(value)]))
        .collect::<Vec<_>>();
    let tables = BTreeMap::from([(table_id, Arc::new(rows))]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let plan = AdvancedExecutionPlan {
        table: table(table_id, "items", 0),
        joins: Vec::new(),
        applies: Vec::new(),
        windows: vec![BoundWindow {
            function: WindowFunction::RowNumber,
            value_index: 1,
            arguments: Vec::new(),
            count_star: false,
            filter: None,
            partition_by: Vec::new(),
            order_by: vec![BoundOrder {
                column_index: 0,
                expression: None,
                data_type: ScalarType::Int64,
                ascending: true,
                nulls_first: None,
            }],
            frame: None,
            data_type: ScalarType::Int64,
            nullable: false,
        }],
        schema: Schema::new(vec![Field::new("row_no", ScalarType::Int64, false)]),
        projection: vec![projection(1, "row_no")],
        distinct: false,
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: false,
    };
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan.clone(),
        &context,
        ExecutionOptions {
            soft_memory_bytes: 128,
            hard_memory_bytes: 256,
            ..ExecutionOptions::default()
        },
    )
    .expect("bounded ranking cursor");
    let error = cursor.next_batch().expect_err("window must hit hard limit");
    assert_eq!(error.sql_state, "53200");
    assert_eq!(cursor.memory().current_bytes(), 0);

    let cancellation = Arc::new(AtomicBool::new(true));
    let mut cancelled =
        AdvancedExecutionCursor::new_with_cancellation(plan, &context, Some(cancellation))
            .expect("cancellable ranking cursor");
    let error = cancelled
        .next_batch()
        .expect_err("cancelled window must stop before initialization");
    assert_eq!(error.sql_state, "57014");
    assert_eq!(cancelled.memory().current_bytes(), 0);
}

#[test]
fn hash_join_spills_and_cleans_its_query_directory() {
    let spill_root = tempdir().expect("spill root");
    let left_id = TableId::new(1);
    let right_id = TableId::new(2);
    let left = (0..128)
        .map(|value| Row::new(vec![Value::Int64(value)]))
        .collect::<Vec<_>>();
    let right = left.clone();
    let tables = BTreeMap::from([(left_id, Arc::new(left)), (right_id, Arc::new(right))]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let join = JoinExecutionPlan {
        source: JoinExecutionSource::Table(table(right_id, "right_items", 1)),
        kind: JoinKind::Inner,
        on: BoundExpr {
            kind: BoundExprKind::Binary {
                left: Box::new(column(0, ScalarType::Int64)),
                op: BinaryOperator::Eq,
                right: Box::new(column(1, ScalarType::Int64)),
            },
            data_type: ScalarType::Boolean,
            nullable: false,
        },
    };
    let plan = AdvancedExecutionPlan {
        distinct: false,
        table: table(left_id, "left_items", 0),
        joins: vec![join],
        applies: Vec::new(),
        windows: Vec::new(),
        schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
        projection: vec![projection(0, "id")],
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: false,
    };
    let options = ExecutionOptions {
        batch_rows: 17,
        soft_memory_bytes: 512,
        hard_memory_bytes: 1024 * 1024,
        spill_root: spill_root.path().to_path_buf(),
        ..ExecutionOptions::default()
    };
    let mut cursor =
        AdvancedExecutionCursor::with_options(plan, &context, options).expect("cursor");
    let mut count = 0;
    while let Some(batch) = cursor.next_batch().expect("batch") {
        count += batch.rows.len();
    }
    assert_eq!(count, 128);
    assert!(
        std::fs::read_dir(spill_root.path())
            .expect("spill root entries")
            .next()
            .is_some()
    );
    drop(cursor);
    assert!(
        std::fs::read_dir(spill_root.path())
            .expect("clean spill root")
            .next()
            .is_none()
    );
}

#[test]
fn lateral_parameter_frames_obey_the_outer_hard_memory_limit() {
    let spill_root = tempdir().expect("spill root");
    let left_id = TableId::new(1);
    let right_id = TableId::new(2);
    let payload = "x".repeat(8 * 1024);
    let tables = BTreeMap::from([
        (
            left_id,
            Arc::new(vec![Row::new(vec![Value::Text(payload.clone())])]),
        ),
        (
            right_id,
            Arc::new(vec![Row::new(vec![Value::Text(payload)])]),
        ),
    ]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let inner_column = column(0, ScalarType::Text);
    let inner = QueryExecutionPlan::Advanced(Box::new(AdvancedExecutionPlan {
        distinct: false,
        table: table(right_id, "right_items", 0),
        joins: Vec::new(),
        applies: Vec::new(),
        windows: Vec::new(),
        schema: Schema::new(vec![Field::new("payload", ScalarType::Text, false)]),
        projection: vec![BoundProjection {
            expr: inner_column.clone(),
            field: Field::new("payload", ScalarType::Text, false),
        }],
        filter: Some(BoundExpr {
            kind: BoundExprKind::Binary {
                left: Box::new(inner_column),
                op: BinaryOperator::Eq,
                right: Box::new(BoundExpr {
                    kind: BoundExprKind::Parameter { index: 1 },
                    data_type: ScalarType::Text,
                    nullable: true,
                }),
            },
            data_type: ScalarType::Boolean,
            nullable: true,
        }),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: false,
    }));
    let plan = AdvancedExecutionPlan {
        distinct: false,
        table: table(left_id, "left_items", 0),
        joins: vec![JoinExecutionPlan {
            source: JoinExecutionSource::Derived {
                query: Box::new(inner),
                correlation_indexes: vec![0],
                offset: 1,
                width: 1,
            },
            kind: JoinKind::Inner,
            on: BoundExpr {
                kind: BoundExprKind::Literal(Value::Boolean(true)),
                data_type: ScalarType::Boolean,
                nullable: false,
            },
        }],
        applies: Vec::new(),
        windows: Vec::new(),
        schema: Schema::new(vec![Field::new("payload", ScalarType::Text, false)]),
        projection: vec![BoundProjection {
            expr: column(0, ScalarType::Text),
            field: Field::new("payload", ScalarType::Text, false),
        }],
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: false,
    };
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan,
        &context,
        ExecutionOptions {
            soft_memory_bytes: 512,
            hard_memory_bytes: 2 * 1024,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("LATERAL cursor");
    let baseline = cursor.memory().current_bytes();
    let error = cursor.next_batch().expect_err("LATERAL parameter memory");
    assert_eq!(error.sql_state, "53200");
    assert_eq!(cursor.memory().current_bytes(), baseline);
    drop(cursor);
    assert!(
        std::fs::read_dir(spill_root.path())
            .expect("spill root entries")
            .next()
            .is_none()
    );
}

#[test]
fn correlated_row_apply_memory_errors_and_cancellation_release_state() {
    let outer_id = TableId::new(1);
    let inner_id = TableId::new(2);
    let tables = BTreeMap::from([
        (outer_id, Arc::new(vec![Row::new(vec![Value::Int64(1)])])),
        (
            inner_id,
            Arc::new(vec![Row::new(vec![
                Value::Int32(1),
                Value::Text("x".repeat(8 * 1024)),
            ])]),
        ),
    ]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let inner = QueryExecutionPlan::Advanced(Box::new(AdvancedExecutionPlan {
        distinct: false,
        table: BoundTable {
            table_id: inner_id,
            binding: Identifier::unquoted("inner_items"),
            offset: 0,
            width: 2,
            nullable: false,
        },
        joins: Vec::new(),
        applies: Vec::new(),
        windows: Vec::new(),
        schema: Schema::new(vec![
            Field::new("id", ScalarType::Int32, false),
            Field::new("payload", ScalarType::Text, false),
        ]),
        projection: vec![
            BoundProjection {
                expr: column(0, ScalarType::Int32),
                field: Field::new("id", ScalarType::Int32, false),
            },
            BoundProjection {
                expr: column(1, ScalarType::Text),
                field: Field::new("payload", ScalarType::Text, false),
            },
        ],
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: false,
    }));
    let plan = AdvancedExecutionPlan {
        distinct: false,
        table: table(outer_id, "outer_items", 0),
        joins: Vec::new(),
        applies: vec![ApplyExecutionPlan {
            kind: ApplyExecutionKind::RowQuantified {
                left: vec![
                    BoundExpr {
                        kind: BoundExprKind::Literal(Value::Int64(1)),
                        data_type: ScalarType::Int64,
                        nullable: false,
                    },
                    BoundExpr {
                        kind: BoundExprKind::Literal(Value::Text("x".to_owned())),
                        data_type: ScalarType::Text,
                        nullable: false,
                    },
                ],
                op: BinaryOperator::Eq,
                quantifier: SubqueryQuantifier::Any,
                negated: false,
                operand_types: vec![ScalarType::Int64, ScalarType::Text],
            },
            query: Box::new(inner),
            correlation_indexes: vec![0],
        }],
        windows: Vec::new(),
        schema: Schema::new(vec![Field::new("matched", ScalarType::Boolean, true)]),
        projection: vec![BoundProjection {
            expr: BoundExpr {
                kind: BoundExprKind::ApplyValue { index: 1 },
                data_type: ScalarType::Boolean,
                nullable: true,
            },
            field: Field::new("matched", ScalarType::Boolean, true),
        }],
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: false,
    };

    let mut bounded = AdvancedExecutionCursor::with_options(
        plan.clone(),
        &context,
        ExecutionOptions {
            soft_memory_bytes: 512,
            hard_memory_bytes: 2 * 1024,
            ..ExecutionOptions::default()
        },
    )
    .expect("bounded row Apply cursor");
    let baseline = bounded.memory().current_bytes();
    let error = bounded
        .next_batch()
        .expect_err("row Apply candidate memory");
    assert_eq!(error.sql_state, "53200");
    assert_eq!(bounded.memory().current_bytes(), baseline);

    let cancellation = Arc::new(AtomicBool::new(true));
    let mut cancelled =
        AdvancedExecutionCursor::new_with_cancellation(plan, &context, Some(cancellation))
            .expect("cancellable row Apply cursor");
    let baseline = cancelled.memory().current_bytes();
    let error = cancelled
        .next_batch()
        .expect_err("cancelled row Apply must stop");
    assert_eq!(error.sql_state, "57014");
    assert_eq!(cancelled.memory().current_bytes(), baseline);
}

#[test]
fn hash_aggregate_spills_partial_states_and_streams_batches() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let rows = (0..96)
        .map(|value| Row::new(vec![Value::Int64(value)]))
        .collect::<Vec<_>>();
    let tables = BTreeMap::from([(table_id, Arc::new(rows))]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let count = BoundExpr {
        kind: BoundExprKind::Aggregate {
            function: AggregateFunction::Count,
            argument: None,
            distinct: false,
            filter: None,
        },
        data_type: ScalarType::Int64,
        nullable: false,
    };
    let plan = AdvancedExecutionPlan {
        distinct: false,
        table: table(table_id, "items", 0),
        joins: Vec::new(),
        applies: Vec::new(),
        windows: Vec::new(),
        schema: Schema::new(vec![
            Field::new("id", ScalarType::Int64, false),
            Field::new("count", ScalarType::Int64, false),
        ]),
        projection: vec![
            projection(0, "id"),
            BoundProjection {
                expr: count,
                field: Field::new("count", ScalarType::Int64, false),
            },
        ],
        filter: None,
        group_by: vec![column(0, ScalarType::Int64)],
        having: None,
        order_by: vec![BoundOrder {
            column_index: 0,
            expression: None,
            data_type: ScalarType::Int64,
            ascending: true,
            nulls_first: None,
        }],
        offset: None,
        limit: None,
        aggregate: true,
    };
    let options = ExecutionOptions {
        batch_rows: 13,
        soft_memory_bytes: 768,
        hard_memory_bytes: 1024 * 1024,
        spill_root: spill_root.path().to_path_buf(),
        ..ExecutionOptions::default()
    };
    let mut cursor =
        AdvancedExecutionCursor::with_options(plan, &context, options).expect("cursor");
    let mut output = Vec::new();
    while let Some(batch) = cursor.next_batch().expect("batch") {
        assert!(batch.rows.len() <= 13);
        output.extend(batch.rows);
    }
    assert_eq!(output.len(), 96);
    assert_eq!(
        output.first(),
        Some(&Row::new(vec![Value::Int64(0), Value::Int64(1)]))
    );
    assert_eq!(
        output.last(),
        Some(&Row::new(vec![Value::Int64(95), Value::Int64(1)]))
    );
    assert!(
        std::fs::read_dir(spill_root.path())
            .expect("spill root entries")
            .next()
            .is_some()
    );
    drop(cursor);
    assert!(
        std::fs::read_dir(spill_root.path())
            .expect("clean spill root")
            .next()
            .is_none()
    );
}
