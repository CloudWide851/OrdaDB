
#[test]
fn grouped_window_consumes_spilled_rows_without_full_materialization() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let rows = (0..96)
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
        distinct: false,
        table: table(table_id, "items", 0),
        joins: Vec::new(),
        applies: Vec::new(),
        windows: vec![BoundWindow {
            function: WindowFunction::Rank,
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
        schema: Schema::new(vec![
            Field::new("id", ScalarType::Int64, false),
            Field::new("rank_no", ScalarType::Int64, false),
        ]),
        projection: vec![
            projection(0, "id"),
            BoundProjection {
                expr: BoundExpr {
                    kind: BoundExprKind::ApplyValue { index: 1 },
                    data_type: ScalarType::Int64,
                    nullable: false,
                },
                field: Field::new("rank_no", ScalarType::Int64, false),
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
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan,
        &context,
        ExecutionOptions {
            batch_rows: 11,
            soft_memory_bytes: 768,
            hard_memory_bytes: 1024 * 1024,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("grouped spilling window cursor");
    let mut actual = Vec::new();
    while let Some(batch) = cursor.next_batch().expect("grouped window batch") {
        actual.extend(batch.rows);
    }
    assert_eq!(actual.len(), 96);
    for (value, row) in actual.iter().enumerate() {
        let value = i64::try_from(value).expect("group value");
        assert_eq!(
            row.values,
            vec![Value::Int64(value), Value::Int64(value.saturating_add(1))]
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
fn distinct_aggregate_spill_merges_overlapping_partial_values() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let rows = (0..96)
        .map(|value| Row::new(vec![Value::Int64(value % 8)]))
        .collect::<Vec<_>>();
    let tables = BTreeMap::from([(table_id, Arc::new(rows))]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let aggregate = |function, name: &str, data_type: ScalarType, nullable| BoundProjection {
        expr: BoundExpr {
            kind: BoundExprKind::Aggregate {
                function,
                argument: Some(Box::new(column(0, ScalarType::Int64))),
                distinct: true,
                filter: None,
            },
            data_type: data_type.clone(),
            nullable,
        },
        field: Field::new(name, data_type, nullable),
    };
    let plan = AdvancedExecutionPlan {
        distinct: false,
        table: table(table_id, "items", 0),
        joins: Vec::new(),
        applies: Vec::new(),
        windows: Vec::new(),
        schema: Schema::new(vec![
            Field::new("count", ScalarType::Int64, false),
            Field::new("sum", ScalarType::Int64, true),
        ]),
        projection: vec![
            aggregate(AggregateFunction::Count, "count", ScalarType::Int64, false),
            aggregate(AggregateFunction::Sum, "sum", ScalarType::Int64, true),
        ],
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: true,
    };
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan,
        &context,
        ExecutionOptions {
            soft_memory_bytes: 768,
            hard_memory_bytes: 1024 * 1024,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("cursor");
    assert_eq!(
        cursor
            .next_batch()
            .expect("batch")
            .expect("DISTINCT aggregate row")
            .rows,
        vec![Row::new(vec![Value::Int64(8), Value::Int64(28)])]
    );
    assert!(cursor.next_batch().expect("end").is_none());
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
fn select_distinct_charges_only_new_keys_to_the_hard_limit() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let duplicate_rows = (0..64)
        .map(|value| Row::new(vec![Value::Int64(value % 2)]))
        .collect::<Vec<_>>();
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let plan = AdvancedExecutionPlan {
        distinct: true,
        table: table(table_id, "items", 0),
        joins: Vec::new(),
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
    let tables = BTreeMap::from([(table_id, Arc::new(duplicate_rows))]);
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan.clone(),
        &context,
        ExecutionOptions {
            batch_rows: 64,
            soft_memory_bytes: 1024,
            hard_memory_bytes: 2048,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("duplicate cursor");
    assert_eq!(
        cursor
            .next_batch()
            .expect("duplicate batch")
            .expect("duplicate rows")
            .rows,
        vec![
            Row::new(vec![Value::Int64(0)]),
            Row::new(vec![Value::Int64(1)]),
        ]
    );
    assert!(cursor.next_batch().expect("duplicate end").is_none());

    let unique_rows = (0..64)
        .map(|value| Row::new(vec![Value::Int64(value)]))
        .collect::<Vec<_>>();
    let tables = BTreeMap::from([(table_id, Arc::new(unique_rows))]);
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan,
        &context,
        ExecutionOptions {
            batch_rows: 64,
            soft_memory_bytes: 1024,
            hard_memory_bytes: 2048,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("unique cursor");
    let error = cursor
        .next_batch()
        .expect_err("unique DISTINCT keys must exhaust the hard grant");
    assert_eq!(error.sql_state, "53200");
}

#[test]
fn nested_left_join_streams_matches_and_null_extensions() {
    let spill_root = tempdir().expect("spill root");
    let left_id = TableId::new(1);
    let right_id = TableId::new(2);
    let tables = BTreeMap::from([
        (
            left_id,
            Arc::new(vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]),
        ),
        (right_id, Arc::new(vec![Row::new(vec![Value::Int64(1)])])),
    ]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let plan = AdvancedExecutionPlan {
        distinct: false,
        table: table(left_id, "left_items", 0),
        joins: vec![JoinExecutionPlan {
            source: JoinExecutionSource::Table(BoundTable {
                nullable: true,
                ..table(right_id, "right_items", 1)
            }),
            kind: JoinKind::Left,
            on: BoundExpr {
                kind: BoundExprKind::Binary {
                    left: Box::new(column(0, ScalarType::Int64)),
                    op: BinaryOperator::Eq,
                    right: Box::new(column(1, ScalarType::Int64)),
                },
                data_type: ScalarType::Boolean,
                nullable: false,
            },
        }],
        applies: Vec::new(),
        windows: Vec::new(),
        schema: Schema::new(vec![
            Field::new("left_id", ScalarType::Int64, false),
            Field::new("right_id", ScalarType::Int64, true),
        ]),
        projection: vec![
            projection(0, "left_id"),
            BoundProjection {
                expr: BoundExpr {
                    nullable: true,
                    ..column(1, ScalarType::Int64)
                },
                field: Field::new("right_id", ScalarType::Int64, true),
            },
        ],
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
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("cursor");
    let rows = cursor.next_batch().expect("batch").expect("rows").rows;
    assert_eq!(
        rows,
        vec![
            Row::new(vec![Value::Int64(1), Value::Int64(1)]),
            Row::new(vec![Value::Int64(2), Value::Null]),
        ]
    );
    assert!(cursor.next_batch().expect("end").is_none());
}

#[test]
fn non_aggregate_sort_spills_then_applies_offset_and_limit() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let rows = (0..200)
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
        distinct: false,
        table: table(table_id, "items", 0),
        joins: Vec::new(),
        applies: Vec::new(),
        windows: Vec::new(),
        schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
        projection: vec![projection(0, "id")],
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: vec![BoundOrder {
            column_index: 0,
            expression: None,
            data_type: ScalarType::Int64,
            ascending: true,
            nulls_first: None,
        }],
        offset: Some(BoundExpr {
            kind: BoundExprKind::Literal(Value::Int64(7)),
            data_type: ScalarType::Int64,
            nullable: false,
        }),
        limit: Some(BoundExpr {
            kind: BoundExprKind::Literal(Value::Int64(5)),
            data_type: ScalarType::Int64,
            nullable: false,
        }),
        aggregate: false,
    };
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan,
        &context,
        ExecutionOptions {
            batch_rows: 3,
            soft_memory_bytes: 256,
            hard_memory_bytes: 4096,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("cursor");
    let mut output = Vec::new();
    while let Some(batch) = cursor.next_batch().expect("batch") {
        output.extend(batch.rows);
    }
    assert_eq!(
        output,
        (7..12)
            .map(|value| Row::new(vec![Value::Int64(value)]))
            .collect::<Vec<_>>()
    );
}

#[test]
fn empty_global_aggregate_returns_count_zero_and_null_average() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let tables = BTreeMap::from([(table_id, Arc::new(Vec::new()))]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let plan = AdvancedExecutionPlan {
        distinct: false,
        table: table(table_id, "items", 0),
        joins: Vec::new(),
        applies: Vec::new(),
        windows: Vec::new(),
        schema: Schema::new(vec![
            Field::new("count", ScalarType::Int64, false),
            Field::new("average", ScalarType::Float64, true),
        ]),
        projection: vec![
            BoundProjection {
                expr: BoundExpr {
                    kind: BoundExprKind::Aggregate {
                        function: AggregateFunction::Count,
                        argument: None,
                        distinct: false,
                        filter: None,
                    },
                    data_type: ScalarType::Int64,
                    nullable: false,
                },
                field: Field::new("count", ScalarType::Int64, false),
            },
            BoundProjection {
                expr: BoundExpr {
                    kind: BoundExprKind::Aggregate {
                        function: AggregateFunction::Avg,
                        argument: Some(Box::new(column(0, ScalarType::Int64))),
                        distinct: false,
                        filter: None,
                    },
                    data_type: ScalarType::Float64,
                    nullable: true,
                },
                field: Field::new("average", ScalarType::Float64, true),
            },
        ],
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: true,
    };
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan,
        &context,
        ExecutionOptions {
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("cursor");
    assert_eq!(
        cursor
            .next_batch()
            .expect("batch")
            .expect("aggregate row")
            .rows,
        vec![Row::new(vec![Value::Int64(0), Value::Null])]
    );
    assert!(cursor.next_batch().expect("end").is_none());
}

#[test]
fn hash_join_uses_memory_lookup_when_the_grant_is_sufficient() {
    let spill_root = tempdir().expect("spill root");
    let left_id = TableId::new(1);
    let right_id = TableId::new(2);
    let rows = (0..128)
        .map(|value| Row::new(vec![Value::Int64(value)]))
        .collect::<Vec<_>>();
    let tables = BTreeMap::from([
        (left_id, Arc::new(rows.clone())),
        (right_id, Arc::new(rows)),
    ]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let plan = AdvancedExecutionPlan {
        distinct: false,
        table: table(left_id, "left_items", 0),
        joins: vec![JoinExecutionPlan {
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
        }],
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
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan,
        &context,
        ExecutionOptions {
            batch_rows: 31,
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("cursor");
    let mut count = 0;
    while let Some(batch) = cursor.next_batch().expect("batch") {
        count += batch.rows.len();
    }
    assert_eq!(count, 128);
    assert!(
        std::fs::read_dir(spill_root.path())
            .expect("spill root")
            .next()
            .is_none()
    );
}

#[test]
fn advanced_filter_uses_parameters_without_materializing_rejected_rows() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let tables = BTreeMap::from([(
        table_id,
        Arc::new(vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
            Row::new(vec![Value::Int64(3)]),
        ]),
    )]);
    let indexes = BTreeMap::<IndexId, Arc<ordadb_index::BPlusTree>>::new();
    let params = vec![Value::Int64(2)];
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &params,
    };
    let plan = AdvancedExecutionPlan {
        distinct: false,
        table: table(table_id, "items", 0),
        joins: Vec::new(),
        applies: Vec::new(),
        windows: Vec::new(),
        schema: Schema::new(vec![Field::new("id", ScalarType::Int64, false)]),
        projection: vec![projection(0, "id")],
        filter: Some(BoundExpr {
            kind: BoundExprKind::Binary {
                left: Box::new(column(0, ScalarType::Int64)),
                op: BinaryOperator::Eq,
                right: Box::new(BoundExpr {
                    kind: BoundExprKind::Parameter { index: 1 },
                    data_type: ScalarType::Int64,
                    nullable: false,
                }),
            },
            data_type: ScalarType::Boolean,
            nullable: false,
        }),
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
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("cursor");
    assert_eq!(
        cursor.next_batch().expect("batch").expect("row").rows,
        vec![Row::new(vec![Value::Int64(2)])]
    );
    assert!(cursor.next_batch().expect("end").is_none());
}

#[test]
fn aggregate_having_false_emits_no_rows() {
    let spill_root = tempdir().expect("spill root");
    let table_id = TableId::new(1);
    let tables = BTreeMap::from([(
        table_id,
        Arc::new(vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ]),
    )]);
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
        schema: Schema::new(vec![Field::new("count", ScalarType::Int64, false)]),
        projection: vec![BoundProjection {
            expr: count.clone(),
            field: Field::new("count", ScalarType::Int64, false),
        }],
        filter: None,
        group_by: Vec::new(),
        having: Some(BoundExpr {
            kind: BoundExprKind::Binary {
                left: Box::new(count),
                op: BinaryOperator::Gt,
                right: Box::new(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Int64(5)),
                    data_type: ScalarType::Int64,
                    nullable: false,
                }),
            },
            data_type: ScalarType::Boolean,
            nullable: false,
        }),
        order_by: Vec::new(),
        offset: None,
        limit: None,
        aggregate: true,
    };
    let mut cursor = AdvancedExecutionCursor::with_options(
        plan,
        &context,
        ExecutionOptions {
            spill_root: spill_root.path().to_path_buf(),
            ..ExecutionOptions::default()
        },
    )
    .expect("cursor");
    assert!(cursor.next_batch().expect("no rows").is_none());
}
