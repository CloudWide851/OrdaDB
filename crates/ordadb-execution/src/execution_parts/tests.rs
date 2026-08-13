
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use ordadb_catalog::{Catalog, NewColumn};
    use ordadb_optimizer::{AccessPath, PlanKind, PlanNode, optimize_select};
    use ordadb_sql::{
        BinaryOperator, BoundExpr, BoundExprKind, BoundOrder, BoundStatement, ScalarFunction,
        UnaryOperator, bind, parse,
    };
    use ordadb_types::{
        ArrayDimension, Identifier, Row, ScalarType, Schema, TableId, TypeId, Value,
    };
    use tempfile::TempDir;

    use super::{
        DEFAULT_MAX_EXPRESSION_DEPTH, DEFAULT_MAX_PLAN_DEPTH, ExecutionContext, ExecutionCursor,
        ExecutionOptions, ExpressionProgram, ExpressionStack, MAX_SPILL_MERGE_FAN_IN, MemoryGrant,
        SPILL_MAGIC, SPILL_VERSION, SpillManager, SpillMergeCursor, SpillRun, evaluate, execute,
    };

    type TestTables = BTreeMap<TableId, Arc<Vec<Row>>>;
    type TestIndexes = BTreeMap<ordadb_types::IndexId, Arc<ordadb_index::BPlusTree>>;
    type TestFixture = (PlanNode, Schema, TestTables, TestIndexes);

    #[test]
    fn evaluates_explicit_scalar_and_multidimensional_array_casts() {
        let scalar = BoundExpr {
            kind: BoundExprKind::Cast {
                expr: Box::new(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Text("42".to_owned())),
                    data_type: ScalarType::Text,
                    nullable: false,
                }),
            },
            data_type: ScalarType::Int64,
            nullable: false,
        };
        assert_eq!(
            evaluate(&scalar, &[], &[]).expect("cast scalar"),
            Value::Int64(42)
        );

        let source_type = ScalarType::Array {
            element: Box::new(ScalarType::Int32),
        };
        let array = BoundExpr {
            kind: BoundExprKind::Array {
                elements: [1, 2, 3, 4]
                    .into_iter()
                    .map(|value| BoundExpr {
                        kind: BoundExprKind::Literal(Value::Int32(value)),
                        data_type: ScalarType::Int32,
                        nullable: false,
                    })
                    .collect(),
                dimensions: vec![ArrayDimension::new(2, 1), ArrayDimension::new(2, 1)],
            },
            data_type: source_type,
            nullable: false,
        };
        let cast = BoundExpr {
            kind: BoundExprKind::Cast {
                expr: Box::new(array),
            },
            data_type: ScalarType::Array {
                element: Box::new(ScalarType::Int64),
            },
            nullable: false,
        };
        let Value::Array(value) = evaluate(&cast, &[], &[]).expect("cast array") else {
            panic!("array result");
        };
        assert_eq!(
            value.values(),
            [
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(3),
                Value::Int64(4),
            ]
        );
        assert_eq!(
            value.dimensions(),
            [ArrayDimension::new(2, 1), ArrayDimension::new(2, 1)]
        );

        let enum_type = ScalarType::Enum {
            type_id: TypeId::new(7),
            labels: vec!["sad".into(), "happy".into()],
        };
        let invalid_enum = BoundExpr {
            kind: BoundExprKind::Cast {
                expr: Box::new(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Text("angry".into())),
                    data_type: ScalarType::Text,
                    nullable: false,
                }),
            },
            data_type: enum_type.clone(),
            nullable: false,
        };
        assert_eq!(
            evaluate(&invalid_enum, &[], &[])
                .expect_err("invalid enum cast")
                .sql_state,
            "22P02"
        );

        let invalid_enum_array = BoundExpr {
            kind: BoundExprKind::Cast {
                expr: Box::new(BoundExpr {
                    kind: BoundExprKind::Array {
                        elements: vec![BoundExpr {
                            kind: BoundExprKind::Literal(Value::Text("angry".into())),
                            data_type: ScalarType::Text,
                            nullable: false,
                        }],
                        dimensions: vec![ArrayDimension::new(1, 1)],
                    },
                    data_type: ScalarType::Array {
                        element: Box::new(ScalarType::Text),
                    },
                    nullable: false,
                }),
            },
            data_type: ScalarType::Array {
                element: Box::new(enum_type),
            },
            nullable: false,
        };
        assert_eq!(
            evaluate(&invalid_enum_array, &[], &[])
                .expect_err("invalid enum array cast")
                .sql_state,
            "22P02"
        );
    }

    #[test]
    fn evaluates_common_scalar_functions_on_the_expression_stack() {
        let text = |value: &str| BoundExpr {
            kind: BoundExprKind::Literal(Value::Text(value.to_owned())),
            data_type: ScalarType::Text,
            nullable: false,
        };
        let integer = |value| BoundExpr {
            kind: BoundExprKind::Literal(Value::Int32(value)),
            data_type: ScalarType::Int32,
            nullable: false,
        };
        let lower = BoundExpr {
            kind: BoundExprKind::Function {
                function: ScalarFunction::Lower,
                arguments: vec![text("ÄBC")],
            },
            data_type: ScalarType::Text,
            nullable: false,
        };
        assert_eq!(
            evaluate(&lower, &[], &[]).expect("lower"),
            Value::Text("äbc".to_owned())
        );

        let substring = BoundExpr {
            kind: BoundExprKind::Function {
                function: ScalarFunction::Substring,
                arguments: vec![text("abcdef"), integer(2), integer(3)],
            },
            data_type: ScalarType::Text,
            nullable: false,
        };
        assert_eq!(
            evaluate(&substring, &[], &[]).expect("substring"),
            Value::Text("bcd".to_owned())
        );

        let coalesce = BoundExpr {
            kind: BoundExprKind::Function {
                function: ScalarFunction::Coalesce,
                arguments: vec![
                    BoundExpr {
                        kind: BoundExprKind::Literal(Value::Null),
                        data_type: ScalarType::Text,
                        nullable: true,
                    },
                    text("fallback"),
                ],
            },
            data_type: ScalarType::Text,
            nullable: false,
        };
        assert_eq!(
            evaluate(&coalesce, &[], &[]).expect("coalesce"),
            Value::Text("fallback".to_owned())
        );

        let scalar = |function, arguments, data_type| BoundExpr {
            kind: BoundExprKind::Function {
                function,
                arguments,
            },
            data_type,
            nullable: false,
        };
        assert_eq!(
            evaluate(
                &scalar(
                    ScalarFunction::Btrim,
                    vec![text("xyhelloxy"), text("xy")],
                    ScalarType::Text,
                ),
                &[],
                &[],
            )
            .expect("btrim"),
            Value::Text("hello".to_owned())
        );
        assert_eq!(
            evaluate(
                &scalar(
                    ScalarFunction::Replace,
                    vec![text("café"), text("fé"), text("ke")],
                    ScalarType::Text,
                ),
                &[],
                &[],
            )
            .expect("replace"),
            Value::Text("cake".to_owned())
        );
        assert_eq!(
            evaluate(
                &scalar(
                    ScalarFunction::Strpos,
                    vec![text("åbcå"), text("c")],
                    ScalarType::Int32,
                ),
                &[],
                &[],
            )
            .expect("strpos"),
            Value::Int32(3)
        );
        assert_eq!(
            evaluate(
                &scalar(
                    ScalarFunction::Greatest,
                    vec![
                        integer(3),
                        BoundExpr {
                            kind: BoundExprKind::Literal(Value::Null),
                            data_type: ScalarType::Int32,
                            nullable: true,
                        },
                        integer(8),
                    ],
                    ScalarType::Int32,
                ),
                &[],
                &[],
            )
            .expect("greatest"),
            Value::Int32(8)
        );
    }

    fn fixture(query: &str, rows: Vec<Row>) -> TestFixture {
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
            schema,
            projection,
            filter,
            order_by,
            offset,
            limit,
            ..
        } = bind(parse(query).expect("parse"), &catalog).expect("bind")
        else {
            panic!("simple SELECT");
        };
        let plan = optimize_select(
            catalog.table_by_id(table_id).expect("definition"),
            projection,
            filter,
            order_by,
            offset,
            limit,
        );
        (
            plan,
            schema,
            BTreeMap::from([(table_id, Arc::new(rows))]),
            BTreeMap::new(),
        )
    }

    fn numbered_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|value| {
                Row::new(vec![
                    Value::Int64(i64::try_from(value).expect("test value")),
                    Value::Text(format!("row-{value}")),
                ])
            })
            .collect()
    }

    #[test]
    fn cursor_emits_default_sized_ordered_batches() {
        let (plan, schema, tables, indexes) =
            fixture("SELECT id FROM items WHERE id >= 0", numbered_rows(2_500));
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let mut cursor = ExecutionCursor::new(&plan, &context, schema).expect("cursor");
        let mut sizes = Vec::new();
        let mut ids = Vec::new();
        while let Some(batch) = cursor.next_batch().expect("batch") {
            sizes.push(batch.rows.len());
            ids.extend(batch.rows.into_iter().map(|row| row.values[0].clone()));
        }
        assert_eq!(sizes, vec![1_024, 1_024, 452]);
        assert_eq!(ids.first(), Some(&Value::Int64(0)));
        assert_eq!(ids.last(), Some(&Value::Int64(2_499)));
        assert!(cursor.next_batch().expect("exhausted").is_none());
    }

    #[test]
    fn compatibility_execute_collects_the_cursor() {
        let (plan, _schema, tables, indexes) =
            fixture("SELECT id FROM items LIMIT 17", numbered_rows(100));
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let rows = execute(&plan, &context).expect("execute");
        assert_eq!(rows.len(), 17);
        assert_eq!(rows[16].values, vec![Value::Int64(16)]);
    }

    #[test]
    fn offset_streams_after_sort_and_limit_null_is_unbounded() {
        let (plan, _schema, tables, indexes) = fixture(
            "SELECT id FROM items ORDER BY id DESC OFFSET 10 LIMIT NULL",
            numbered_rows(100),
        );
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let rows = execute(&plan, &context).expect("execute offset");
        assert_eq!(rows.len(), 90);
        assert_eq!(rows[0].values, vec![Value::Int64(89)]);
        assert_eq!(rows[89].values, vec![Value::Int64(0)]);
    }

    #[test]
    fn negative_offset_fails_before_scanning() {
        let (plan, schema, tables, indexes) =
            fixture("SELECT id FROM items OFFSET -1", numbered_rows(3));
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let error = match ExecutionCursor::new(&plan, &context, schema) {
            Ok(_) => panic!("negative offset must fail"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "2201X");
    }

    #[test]
    fn sort_spills_and_drop_cleans_the_query_directory() {
        let temp = TempDir::new().expect("temp");
        let spill_root = temp.path().join("spill");
        let (plan, schema, tables, indexes) =
            fixture("SELECT id FROM items ORDER BY id DESC", numbered_rows(200));
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let options = ExecutionOptions {
            batch_rows: 32,
            soft_memory_bytes: 256,
            hard_memory_bytes: 16_384,
            spill_root: spill_root.clone(),
            ..ExecutionOptions::default()
        };
        let mut cursor =
            ExecutionCursor::with_options(&plan, &context, schema, options).expect("cursor");
        let first = cursor.next_batch().expect("batch").expect("first batch");
        assert_eq!(first.rows[0].values, vec![Value::Int64(199)]);
        assert_eq!(
            std::fs::read_dir(&spill_root).expect("spill root").count(),
            1
        );
        drop(cursor);
        assert_eq!(
            std::fs::read_dir(&spill_root)
                .expect("clean spill root")
                .count(),
            0
        );
    }

    #[test]
    fn spill_reader_rejects_truncated_versioned_records() {
        let temp = TempDir::new().expect("temp");
        let path = temp.path().join("truncated.spill");
        let mut bytes = Vec::from(SPILL_MAGIC);
        bytes.extend_from_slice(&SPILL_VERSION.to_le_bytes());
        bytes.extend_from_slice(&16_u32.to_le_bytes());
        bytes.extend_from_slice(b"short");
        std::fs::write(&path, bytes).expect("write truncated spill");

        let memory = super::MemoryGrant::new(1024, 1024).expect("memory grant");
        let error = match SpillRun::open(&path, &memory) {
            Ok(_) => panic!("truncated spill must fail"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "XX001");
    }

    #[test]
    fn spill_write_failure_cleans_the_partial_query_directory() {
        let temp = TempDir::new().expect("temp");
        let spill_root = temp.path().join("spill");
        std::fs::create_dir(&spill_root).expect("spill root");
        let query_dir = {
            let mut spill = SpillManager::new(spill_root);
            let memory = MemoryGrant::new(32, 64).expect("memory grant");
            let error = spill
                .write_sorted_run(&[Row::new(vec![Value::Text("x".repeat(1_024))])], &memory)
                .expect_err("oversized spill record");
            assert_eq!(error.sql_state, "53200");
            let query_dir = spill.query_dir.clone().expect("query directory");
            assert_eq!(
                std::fs::read_dir(&query_dir)
                    .expect("partial query directory")
                    .count(),
                1
            );
            query_dir
        };
        assert!(!query_dir.exists());
    }

    #[test]
    fn spill_heap_merge_compacts_multiple_levels_and_preserves_stable_ties() {
        let temp = TempDir::new().expect("temp");
        let spill_root = temp.path().join("spill");
        std::fs::create_dir(&spill_root).expect("spill root");
        let query_dir = {
            let memory = MemoryGrant::new(1024 * 1024, 8 * 1024 * 1024).expect("memory");
            let order_by = vec![BoundOrder {
                column_index: 0,
                expression: None,
                data_type: ScalarType::Int64,
                ascending: true,
                nulls_first: None,
            }];
            let mut spill = SpillManager::new(spill_root);
            let run_count = MAX_SPILL_MERGE_FAN_IN * MAX_SPILL_MERGE_FAN_IN + 1;
            let mut paths = Vec::new();
            for run in 0..run_count {
                paths.push(
                    spill
                        .write_sorted_run(
                            &[Row::new(vec![
                                Value::Int64(1),
                                Value::Int64(i64::try_from(run).expect("run index")),
                            ])],
                            &memory,
                        )
                        .expect("write run"),
                );
            }
            let paths = spill
                .compact_sorted_runs(paths, &order_by, &memory)
                .expect("compact runs");
            assert!(paths.len() <= MAX_SPILL_MERGE_FAN_IN);
            let mut merge =
                SpillMergeCursor::open(&paths, &order_by, &memory).expect("merge cursor");
            assert!(merge.run_count() <= MAX_SPILL_MERGE_FAN_IN);
            let mut actual = Vec::new();
            while let Some(row) = merge.pop_next(&order_by, &memory).expect("merge row") {
                actual.push(row.values[1].clone());
            }
            assert_eq!(
                actual,
                (0..run_count)
                    .map(|run| Value::Int64(i64::try_from(run).expect("run index")))
                    .collect::<Vec<_>>()
            );
            drop(merge);
            assert_eq!(memory.current_bytes(), 0);
            let query_dir = spill.query_dir.clone().expect("query directory");
            assert!(
                std::fs::read_dir(&query_dir)
                    .expect("query directory")
                    .count()
                    > run_count
            );
            query_dir
        };
        assert!(!query_dir.exists());
        assert_eq!(
            std::fs::read_dir(temp.path().join("spill"))
                .expect("clean spill root")
                .count(),
            0
        );
    }

    #[test]
    fn spill_heap_merge_propagates_compare_errors_and_hard_limits() {
        let temp = TempDir::new().expect("temp");
        let spill_root = temp.path().join("spill");
        std::fs::create_dir(&spill_root).expect("spill root");
        let memory = MemoryGrant::new(1024, 16 * 1024).expect("memory");
        let order_by = vec![BoundOrder {
            column_index: 0,
            expression: None,
            data_type: ScalarType::Json,
            ascending: true,
            nulls_first: None,
        }];
        let query_dir = {
            let mut spill = SpillManager::new(spill_root);
            let json_paths = vec![
                spill
                    .write_sorted_run(
                        &[Row::new(vec![Value::Json(serde_json::json!({"run": 1}))])],
                        &memory,
                    )
                    .expect("first JSON run"),
                spill
                    .write_sorted_run(
                        &[Row::new(vec![Value::Json(serde_json::json!({"run": 2}))])],
                        &memory,
                    )
                    .expect("second JSON run"),
            ];
            let error = match SpillMergeCursor::open(&json_paths, &order_by, &memory) {
                Ok(_) => panic!("JSON spill ordering must fail"),
                Err(error) => error,
            };
            assert_eq!(error.sql_state, "42883");
            assert_eq!(memory.current_bytes(), 0);

            let empty_paths = vec![
                spill.write_sorted_run(&[], &memory).expect("empty run 1"),
                spill.write_sorted_run(&[], &memory).expect("empty run 2"),
            ];
            let tiny = MemoryGrant::new(8, 8).expect("tiny memory");
            let error = match SpillMergeCursor::open(&empty_paths, &order_by, &tiny) {
                Ok(_) => panic!("heap reservation must respect the hard limit"),
                Err(error) => error,
            };
            assert_eq!(error.sql_state, "53200");
            assert_eq!(tiny.current_bytes(), 0);
            spill.query_dir.clone().expect("query directory")
        };
        assert!(!query_dir.exists());
    }

    #[test]
    fn hard_memory_limit_returns_out_of_memory_sqlstate() {
        let (plan, schema, tables, indexes) = fixture(
            "SELECT payload FROM items",
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("x".repeat(2_048)),
            ])],
        );
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let options = ExecutionOptions {
            batch_rows: 1,
            soft_memory_bytes: 128,
            hard_memory_bytes: 256,
            ..ExecutionOptions::default()
        };
        let mut cursor =
            ExecutionCursor::with_options(&plan, &context, schema, options).expect("cursor");
        let error = cursor.next_batch().expect_err("hard memory limit");
        assert_eq!(error.sql_state, "53200");
    }

    #[test]
    fn expression_stack_owns_capacity_and_variable_values_through_raii() {
        let expression = BoundExpr {
            kind: BoundExprKind::Binary {
                left: Box::new(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Text("x".repeat(512))),
                    data_type: ScalarType::Text,
                    nullable: false,
                }),
                op: BinaryOperator::Eq,
                right: Box::new(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Text("x".repeat(512))),
                    data_type: ScalarType::Text,
                    nullable: false,
                }),
            },
            data_type: ScalarType::Boolean,
            nullable: false,
        };
        let program = ExpressionProgram::compile(&expression).expect("compile");
        let memory = MemoryGrant::new(128, 256).expect("grant");
        let mut stack = ExpressionStack::new(&memory).expect("stack");
        let error = program
            .evaluate_reusing(&[], &[], &mut stack)
            .expect_err("variable-width stack exceeds grant");
        assert_eq!(error.sql_state, "53200");
        assert!(memory.current_bytes() > 0);
        drop(stack);
        assert_eq!(memory.current_bytes(), 0);
    }

    #[test]
    fn explicit_expression_and_plan_limits_return_program_limit() {
        let mut expression = BoundExpr {
            kind: BoundExprKind::Literal(Value::Boolean(true)),
            data_type: ScalarType::Boolean,
            nullable: false,
        };
        for _ in 0..8 {
            expression = BoundExpr {
                kind: BoundExprKind::Unary {
                    op: UnaryOperator::Not,
                    expr: Box::new(expression),
                },
                data_type: ScalarType::Boolean,
                nullable: false,
            };
        }
        let error = ExpressionProgram::compile_with_limit(&expression, false, 4)
            .expect_err("expression limit");
        assert_eq!(error.sql_state, "54001");

        let table_id = TableId::new(1);
        let mut plan = PlanNode {
            kind: PlanKind::Scan {
                table_id,
                access: AccessPath::Sequential,
                required_columns: vec![0],
            },
            estimated_rows: 0.0,
            estimated_cost: 0.0,
        };
        for _ in 0..8 {
            plan = PlanNode {
                kind: PlanKind::Limit {
                    limit: BoundExpr {
                        kind: BoundExprKind::Literal(Value::Int64(1)),
                        data_type: ScalarType::Int64,
                        nullable: false,
                    },
                    input: Box::new(plan),
                },
                estimated_rows: 0.0,
                estimated_cost: 0.0,
            };
        }
        let tables = BTreeMap::from([(table_id, Arc::new(Vec::new()))]);
        let indexes = BTreeMap::new();
        let context = ExecutionContext {
            tables: &tables,
            indexes: &indexes,
            params: &[],
        };
        let error = match ExecutionCursor::with_options(
            &plan,
            &context,
            Schema::empty(),
            ExecutionOptions {
                max_plan_depth: 4,
                ..ExecutionOptions::default()
            },
        ) {
            Ok(_) => panic!("plan limit"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "54001");
    }

    #[test]
    fn bounded_deep_expression_and_plan_execute_on_small_native_stack() {
        let mut expression = BoundExpr {
            kind: BoundExprKind::Literal(Value::Boolean(true)),
            data_type: ScalarType::Boolean,
            nullable: false,
        };
        for _ in 0..DEFAULT_MAX_EXPRESSION_DEPTH {
            expression = BoundExpr {
                kind: BoundExprKind::Unary {
                    op: UnaryOperator::Not,
                    expr: Box::new(expression),
                },
                data_type: ScalarType::Boolean,
                nullable: false,
            };
        }

        let table_id = TableId::new(1);
        let mut plan = PlanNode {
            kind: PlanKind::Scan {
                table_id,
                access: AccessPath::Sequential,
                required_columns: vec![0],
            },
            estimated_rows: 0.0,
            estimated_cost: 0.0,
        };
        for _ in 0..DEFAULT_MAX_PLAN_DEPTH - 1 {
            plan = PlanNode {
                kind: PlanKind::Limit {
                    limit: BoundExpr {
                        kind: BoundExprKind::Literal(Value::Int64(1)),
                        data_type: ScalarType::Int64,
                        nullable: false,
                    },
                    input: Box::new(plan),
                },
                estimated_rows: 0.0,
                estimated_cost: 0.0,
            };
        }
        let tables = BTreeMap::from([(table_id, Arc::new(Vec::new()))]);
        let indexes = BTreeMap::new();

        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .name("bounded-query-stack".to_owned())
                .stack_size(128 * 1_024)
                .spawn_scoped(scope, move || {
                    let program = ExpressionProgram::compile_with_limit(
                        &expression,
                        false,
                        DEFAULT_MAX_EXPRESSION_DEPTH,
                    )
                    .expect("deep expression compiles iteratively");
                    assert_eq!(
                        program.evaluate(&[], &[]).expect("deep expression"),
                        Value::Boolean(true)
                    );
                    let context = ExecutionContext {
                        tables: &tables,
                        indexes: &indexes,
                        params: &[],
                    };
                    let mut cursor = ExecutionCursor::with_options(
                        &plan,
                        &context,
                        Schema::empty(),
                        ExecutionOptions::default(),
                    )
                    .expect("deep plan builds iteratively");
                    assert!(cursor.next_batch().expect("deep plan executes").is_none());
                })
                .expect("spawn bounded-stack thread")
                .join()
                .expect("bounded-stack thread");
        });
    }
}
