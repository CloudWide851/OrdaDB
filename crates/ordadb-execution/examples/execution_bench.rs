use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::mem::{size_of, zeroed};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ordadb_catalog::{
    Catalog, ColumnStatistics, IndexMethod, IndexOptions, NewColumn, NewIndex, TableStatistics,
};
use ordadb_execution::{
    AdvancedExecutionCursor, AdvancedExecutionPlan, ExecutionContext, ExecutionCursor,
    ExecutionOptions, JoinExecutionPlan, JoinExecutionSource,
};
use ordadb_index::{BPlusTree, IndexEntry, RowId};
use ordadb_optimizer::{PlanNode, optimize_select};
use ordadb_sql::{BoundJoinSource, BoundStatement, bind, parse};
use ordadb_types::{Identifier, IndexId, Row, ScalarType, Schema, TableId, Value};
use serde::Serialize;
use windows_sys::Win32::System::ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
use windows_sys::Win32::System::Threading::GetCurrentProcess;

const DEFAULT_ROWS: usize = 1_000_000;
const ITERATIONS: usize = 5;
const DEFAULT_BATCH_ROWS: usize = 1_024;
const ADVANCED_MAX_ROWS: usize = 100_000;

struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);

fn record_allocation() {
    if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    }
}

// SAFETY: Every operation delegates to the process System allocator with the
// original layout/pointer. The counter does not influence allocation results.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller supplied the layout required by GlobalAlloc.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller supplied the layout required by GlobalAlloc.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The caller returns the pointer with its original layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller supplied the original layout and desired size.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkReport {
    recorded_at_unix_ms: u128,
    target: &'static str,
    profile: &'static str,
    input_rows: usize,
    advanced_input_rows: usize,
    bounded_rows: usize,
    batch_rows: usize,
    spill_soft_memory_bytes: usize,
    iterations: usize,
    scenarios: Vec<ScenarioResult>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ScenarioResult {
    name: &'static str,
    input_rows: usize,
    output_rows: usize,
    median_ms: f64,
    p95_ms: f64,
    rows_per_second: f64,
    median_allocations: u64,
    query_peak_bytes: usize,
    process_peak_rss_bytes: usize,
}

enum BenchmarkPlan {
    Simple { plan: Box<PlanNode>, schema: Schema },
    Advanced(Box<AdvancedExecutionPlan>),
}

fn main() {
    let row_count = environment_usize("ORDADB_BENCH_ROWS").unwrap_or(DEFAULT_ROWS);
    let batch_rows = environment_usize("ORDADB_BENCH_BATCH_ROWS").unwrap_or(DEFAULT_BATCH_ROWS);
    let advanced_rows = row_count.min(ADVANCED_MAX_ROWS);
    let bounded_rows = environment_usize("ORDADB_BENCH_BOUNDED_ROWS")
        .unwrap_or_else(|| row_count.min(10_000))
        .min(row_count);
    let spill_soft_memory_bytes =
        environment_usize("ORDADB_BENCH_SPILL_SOFT_BYTES").unwrap_or(1 << 20);
    let (catalog, item_table, lookup_table) = catalog(row_count);
    let tables = tables(item_table, lookup_table, row_count, advanced_rows);

    let threshold = i64::try_from(row_count / 2).expect("benchmark row count fits i64");
    let scan = bind_plan(
        &format!("SELECT payload FROM items WHERE id >= {threshold}"),
        &catalog,
    );
    let sort_limit = bind_plan(
        &format!("SELECT payload FROM items WHERE id < {bounded_rows} ORDER BY id DESC LIMIT 1000"),
        &catalog,
    );
    let join = bind_plan(
        "SELECT items.payload FROM items INNER JOIN lookup ON items.id = lookup.id",
        &catalog,
    );
    let aggregate = bind_plan("SELECT COUNT(id), SUM(payload) FROM items", &catalog);
    let spill_sort = bind_plan(
        &format!("SELECT payload FROM items WHERE id < {bounded_rows} ORDER BY id DESC"),
        &catalog,
    );
    let window_rank = bind_plan(
        &format!(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY payload DESC) AS row_no \
             FROM items WHERE id < {bounded_rows}"
        ),
        &catalog,
    );
    let mut index_catalog = catalog.clone();
    let item_index = add_item_index(&mut index_catalog, item_table);
    let point_value = row_count / 2;
    let index_point = bind_plan(
        &format!("SELECT payload FROM items WHERE id = {point_value}"),
        &index_catalog,
    );
    let range_start = row_count.saturating_sub(row_count.min(1_000));
    let index_range = bind_plan(
        &format!("SELECT payload FROM items WHERE id >= {range_start}"),
        &index_catalog,
    );

    let requested = std::env::var("ORDADB_BENCH_SCENARIO").ok();
    let indexes = if requested
        .as_deref()
        .is_none_or(|scenario| matches!(scenario, "indexPoint" | "indexRange"))
    {
        indexes(&tables, item_table, item_index)
    } else {
        BTreeMap::new()
    };
    let context = ExecutionContext {
        tables: &tables,
        indexes: &indexes,
        params: &[],
    };
    let mut scenarios = Vec::new();
    if includes_scenario(requested.as_deref(), "scanFilterProject") {
        scenarios.push(measure("scanFilterProject", row_count, || {
            drain(&scan, &context, options(batch_rows, 64 << 20, 256 << 20))
        }));
    }
    if includes_scenario(requested.as_deref(), "sortLimit") {
        scenarios.push(measure("sortLimit", row_count, || {
            drain(
                &sort_limit,
                &context,
                options(batch_rows, 64 << 20, 256 << 20),
            )
        }));
    }
    if includes_scenario(requested.as_deref(), "indexPoint") {
        scenarios.push(measure("indexPoint", row_count, || {
            drain(
                &index_point,
                &context,
                options(batch_rows, 64 << 20, 256 << 20),
            )
        }));
    }
    if includes_scenario(requested.as_deref(), "indexRange") {
        scenarios.push(measure("indexRange", row_count, || {
            drain(
                &index_range,
                &context,
                options(batch_rows, 64 << 20, 256 << 20),
            )
        }));
    }
    if includes_scenario(requested.as_deref(), "hashJoin") {
        scenarios.push(measure("hashJoin", row_count, || {
            drain(&join, &context, options(batch_rows, 64 << 20, 256 << 20))
        }));
    }
    if includes_scenario(requested.as_deref(), "hashAggregate") {
        scenarios.push(measure("hashAggregate", row_count, || {
            drain(
                &aggregate,
                &context,
                options(batch_rows, 64 << 20, 256 << 20),
            )
        }));
    }
    if includes_scenario(requested.as_deref(), "spillSort") {
        scenarios.push(measure("spillSort", row_count, || {
            drain(
                &spill_sort,
                &context,
                options(batch_rows, spill_soft_memory_bytes, 256 << 20),
            )
        }));
    }
    if includes_scenario(requested.as_deref(), "windowRank") {
        scenarios.push(measure("windowRank", row_count, || {
            drain(
                &window_rank,
                &context,
                options(batch_rows, spill_soft_memory_bytes, 256 << 20),
            )
        }));
    }

    let report = BenchmarkReport {
        recorded_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock follows Unix epoch")
            .as_millis(),
        target: "x86_64-pc-windows-msvc",
        profile: "release",
        input_rows: row_count,
        advanced_input_rows: advanced_rows,
        bounded_rows,
        batch_rows,
        spill_soft_memory_bytes,
        iterations: ITERATIONS,
        scenarios,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize benchmark report")
    );
}

fn catalog(row_count: usize) -> (Catalog, TableId, TableId) {
    let mut catalog = Catalog::default();
    let item_table = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("items"),
            vec![
                NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                NewColumn::new(Identifier::unquoted("payload"), ScalarType::Int64),
            ],
        )
        .expect("create items table");
    let lookup_table = catalog
        .create_table(
            &Identifier::unquoted("public"),
            Identifier::unquoted("lookup"),
            vec![
                NewColumn::new(Identifier::unquoted("id"), ScalarType::Int64),
                NewColumn::new(Identifier::unquoted("category"), ScalarType::Int64),
            ],
        )
        .expect("create lookup table");
    let item_id = catalog
        .table_by_id(item_table)
        .expect("items table")
        .columns()[0]
        .id;
    catalog
        .set_table_statistics(
            item_table,
            TableStatistics {
                row_count: u64::try_from(row_count).expect("benchmark row count fits u64"),
                columns: BTreeMap::from([(
                    item_id,
                    ColumnStatistics {
                        distinct_count: u64::try_from(row_count)
                            .expect("benchmark row count fits u64"),
                        ..ColumnStatistics::default()
                    },
                )]),
            },
        )
        .expect("set item statistics");
    (catalog, item_table, lookup_table)
}

fn add_item_index(catalog: &mut Catalog, item_table: TableId) -> IndexId {
    catalog
        .create_index(
            item_table,
            NewIndex {
                name: Identifier::unquoted("items_id_idx"),
                key_columns: vec![Identifier::unquoted("id")],
                include_columns: Vec::new(),
                unique: true,
                method: IndexMethod::BTree,
                options: IndexOptions::BTree,
            },
        )
        .expect("create items index")
}

fn tables(
    item_table: TableId,
    lookup_table: TableId,
    row_count: usize,
    advanced_rows: usize,
) -> BTreeMap<TableId, Arc<Vec<Row>>> {
    let items = (0..row_count)
        .map(|value| {
            let value = i64::try_from(value).expect("benchmark value fits i64");
            Row::new(vec![Value::Int64(value), Value::Int64(value * 2)])
        })
        .collect::<Vec<_>>();
    let lookup = (0..advanced_rows)
        .map(|value| {
            let value = i64::try_from(value).expect("benchmark value fits i64");
            Row::new(vec![Value::Int64(value), Value::Int64(value % 128)])
        })
        .collect::<Vec<_>>();
    BTreeMap::from([
        (item_table, Arc::new(items)),
        (lookup_table, Arc::new(lookup)),
    ])
}

fn indexes(
    tables: &BTreeMap<TableId, Arc<Vec<Row>>>,
    item_table: TableId,
    item_index: IndexId,
) -> BTreeMap<IndexId, Arc<BPlusTree>> {
    let rows = tables.get(&item_table).expect("items rows");
    let entries = rows.iter().enumerate().map(|(row_id, row)| {
        IndexEntry::new(
            &row.values[..1],
            RowId::new(u64::try_from(row_id).expect("benchmark row ID fits u64")),
            Vec::new(),
        )
        .expect("benchmark index entry")
    });
    let tree = BPlusTree::from_entries(true, entries).expect("build items index");
    BTreeMap::from([(item_index, Arc::new(tree))])
}

fn bind_plan(sql: &str, catalog: &Catalog) -> BenchmarkPlan {
    match bind(parse(sql).expect("parse benchmark"), catalog).expect("bind benchmark") {
        BoundStatement::Select {
            table_id,
            schema,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => BenchmarkPlan::Simple {
            plan: Box::new(optimize_select(
                catalog.table_by_id(table_id).expect("benchmark table"),
                projection,
                filter,
                order_by,
                offset,
                limit,
            )),
            schema,
        },
        BoundStatement::AdvancedSelect {
            table,
            joins,
            applies: _,
            windows,
            schema,
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
            aggregate,
        } => BenchmarkPlan::Advanced(Box::new(AdvancedExecutionPlan {
            table,
            joins: joins
                .into_iter()
                .map(|join| JoinExecutionPlan {
                    source: match join.source {
                        BoundJoinSource::Table(table) => JoinExecutionSource::Table(table),
                        BoundJoinSource::Derived { .. } => {
                            panic!("execution benchmark does not use derived joins")
                        }
                    },
                    kind: join.kind,
                    on: join.on,
                })
                .collect(),
            applies: Vec::new(),
            windows,
            schema,
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit: limit.map(|limit| *limit),
            aggregate,
        })),
        _ => panic!("benchmark query did not bind as SELECT"),
    }
}

fn options(
    batch_rows: usize,
    soft_memory_bytes: usize,
    hard_memory_bytes: usize,
) -> ExecutionOptions {
    ExecutionOptions {
        batch_rows,
        soft_memory_bytes,
        hard_memory_bytes,
        ..ExecutionOptions::default()
    }
}

fn drain(
    plan: &BenchmarkPlan,
    context: &ExecutionContext<'_>,
    options: ExecutionOptions,
) -> (usize, usize) {
    match plan {
        BenchmarkPlan::Simple { plan, schema } => {
            let mut cursor = ExecutionCursor::with_options(plan, context, schema.clone(), options)
                .expect("create simple benchmark cursor");
            let mut output_rows = 0;
            while let Some(batch) = cursor.next_batch().expect("execute simple benchmark batch") {
                output_rows += batch.rows.len();
                black_box(batch);
            }
            (output_rows, cursor.memory().peak_bytes())
        }
        BenchmarkPlan::Advanced(plan) => {
            let mut cursor =
                AdvancedExecutionCursor::with_options((**plan).clone(), context, options)
                    .expect("create advanced benchmark cursor");
            let mut output_rows = 0;
            while let Some(batch) = cursor
                .next_batch()
                .expect("execute advanced benchmark batch")
            {
                output_rows += batch.rows.len();
                black_box(batch);
            }
            (output_rows, cursor.memory().peak_bytes())
        }
    }
}

fn measure(
    name: &'static str,
    input_rows: usize,
    mut run: impl FnMut() -> (usize, usize),
) -> ScenarioResult {
    black_box(run());
    let mut samples = Vec::with_capacity(ITERATIONS);
    let mut allocations = Vec::with_capacity(ITERATIONS);
    let mut output_rows = 0;
    let mut query_peak_bytes = 0;
    let mut process_peak_rss_bytes = 0;
    for _ in 0..ITERATIONS {
        let started = Instant::now();
        let (count, peak_bytes) = run();
        samples.push(started.elapsed());
        output_rows = count;
        query_peak_bytes = query_peak_bytes.max(peak_bytes);
        process_peak_rss_bytes =
            process_peak_rss_bytes.max(process_peak_rss().expect("read process RSS"));
    }
    for _ in 0..ITERATIONS {
        ALLOCATIONS.store(0, Ordering::Relaxed);
        COUNT_ALLOCATIONS.store(true, Ordering::Relaxed);
        black_box(run());
        COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
        allocations.push(ALLOCATIONS.load(Ordering::Relaxed));
    }
    samples.sort_unstable();
    allocations.sort_unstable();
    let median = samples[ITERATIONS / 2];
    let p95_index = (ITERATIONS * 95).div_ceil(100).saturating_sub(1);
    let p95 = samples[p95_index.min(ITERATIONS - 1)];
    ScenarioResult {
        name,
        input_rows,
        output_rows,
        median_ms: milliseconds(median),
        p95_ms: milliseconds(p95),
        rows_per_second: input_rows as f64 / median.as_secs_f64(),
        median_allocations: allocations[ITERATIONS / 2],
        query_peak_bytes,
        process_peak_rss_bytes,
    }
}

fn process_peak_rss() -> Result<usize, String> {
    // SAFETY: PROCESS_MEMORY_COUNTERS is a plain C output structure. The
    // current-process pseudo handle is always valid for this read-only call.
    let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { zeroed() };
    counters.cb =
        u32::try_from(size_of::<PROCESS_MEMORY_COUNTERS>()).map_err(|error| error.to_string())?;
    // SAFETY: `counters` is writable for `cb` bytes for the duration of call.
    let succeeded =
        unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &raw mut counters, counters.cb) };
    if succeeded == 0 {
        return Err("GetProcessMemoryInfo failed".to_owned());
    }
    Ok(counters.PeakWorkingSetSize)
}

fn environment_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

fn includes_scenario(requested: Option<&str>, name: &str) -> bool {
    requested.is_none_or(|requested| requested == name)
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
