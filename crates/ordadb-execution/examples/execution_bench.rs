use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ordadb_catalog::{Catalog, NewColumn};
use ordadb_execution::{ExecutionContext, ExecutionCursor};
use ordadb_optimizer::optimize_select;
use ordadb_sql::{BoundStatement, bind, parse};
use ordadb_types::{Identifier, Row, ScalarType, Schema, Value};

const DEFAULT_ROWS: usize = 1_000_000;
const ITERATIONS: usize = 5;

fn main() {
    let row_count = std::env::var("ORDADB_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_ROWS);
    let threshold = i64::try_from(row_count / 2).expect("benchmark row count fits i64");

    let mut catalog = Catalog::default();
    let table_id = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("items"),
            vec![
                NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                NewColumn::new(Identifier::unquoted("payload"), ScalarType::Int64),
            ],
        )
        .expect("create benchmark table");
    let query = format!("SELECT payload FROM items WHERE id >= {threshold}");
    let BoundStatement::Select {
        projection,
        filter,
        order_by,
        limit,
        ..
    } = bind(parse(&query).expect("parse benchmark"), &catalog).expect("bind benchmark")
    else {
        panic!("benchmark query must bind as a simple SELECT");
    };
    let table = catalog.table_by_id(table_id).expect("benchmark table");
    let plan = optimize_select(table, projection, filter, order_by, limit);

    let rows = (0..row_count)
        .map(|value| {
            let value = i64::try_from(value).expect("benchmark value fits i64");
            Row::new(vec![Value::Int64(value), Value::Int64(value * 2)])
        })
        .collect::<Vec<_>>();
    let mut tables = BTreeMap::new();
    tables.insert(table_id, Arc::new(rows));
    let indexes = BTreeMap::new();
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };

    let warmup = drain(&plan, &context);
    black_box(warmup);

    let mut durations = Vec::with_capacity(ITERATIONS);
    let mut output_rows = 0;
    let mut query_peak_bytes = 0;
    for _ in 0..ITERATIONS {
        let started = Instant::now();
        let (count, peak_bytes) = drain(&plan, &context);
        durations.push(started.elapsed());
        output_rows = count;
        query_peak_bytes = peak_bytes;
    }
    durations.sort_unstable();
    let median = durations[ITERATIONS / 2];
    let rows_per_second = throughput(row_count, median);
    println!(
        "rows={row_count} output_rows={output_rows} median_ms={:.3} rows_per_second={rows_per_second:.0} query_peak_mib={:.2}",
        median.as_secs_f64() * 1_000.0,
        query_peak_bytes as f64 / (1024.0 * 1024.0)
    );
}

fn drain(plan: &ordadb_optimizer::PlanNode, context: &ExecutionContext<'_>) -> (usize, usize) {
    let mut cursor =
        ExecutionCursor::new(plan, context, Schema::empty()).expect("create benchmark cursor");
    let mut output_rows = 0;
    while let Some(batch) = cursor.next_batch().expect("execute benchmark batch") {
        output_rows += batch.rows.len();
        black_box(batch);
    }
    (output_rows, cursor.memory().peak_bytes())
}

fn throughput(rows: usize, elapsed: Duration) -> f64 {
    rows as f64 / elapsed.as_secs_f64()
}
