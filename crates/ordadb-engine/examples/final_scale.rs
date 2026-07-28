use std::collections::BTreeMap;
use std::env;
use std::fmt::{self, Display};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ordadb_catalog::{Catalog, NewColumn};
use ordadb_engine::{Engine, EngineConfig, LOGICAL_SNAPSHOT_VERSION, LogicalDatabaseSnapshot};
use ordadb_types::{Identifier, QueryEvent, Row, ScalarType, Value};
use serde::Serialize;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
const FULL_ROWS: u64 = 10_000_000;
const FULL_DATABASE_BYTES: u64 = 20 * GIB;
const FULL_CONNECTIONS: usize = 32;
const FULL_REQUIRED_FREE_DISK_BYTES: u64 = FULL_DATABASE_BYTES * 3;
const FULL_REQUIRED_FREE_MEMORY_BYTES: u64 = FULL_DATABASE_BYTES * 4;
const DEFAULT_QUERY_HARD_LIMIT_BYTES: usize = 256 * 1024 * 1024;
const MAX_ROW_PAYLOAD_BYTES: usize = 7 * 1024;

fn main() {
    let options = match Options::parse(env::args().skip(1)) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}");
            process::exit(2);
        }
    };
    let started_at_unix_ms = unix_millis();
    let started = Instant::now();
    let result = run(&options);
    let report = match result {
        Ok(success) => Report {
            schema_version: 1,
            profile: options.profile,
            started_at_unix_ms,
            elapsed_ms: elapsed_millis(started.elapsed()),
            target: options.target.clone(),
            observed: success.observed,
            checks: success.checks,
            timings_ms: success.timings_ms,
            preflight: options.preflight.clone(),
            passed: true,
            failure: None,
        },
        Err(error) => Report {
            schema_version: 1,
            profile: options.profile,
            started_at_unix_ms,
            elapsed_ms: elapsed_millis(started.elapsed()),
            target: options.target.clone(),
            observed: Observed::default(),
            checks: Vec::new(),
            timings_ms: BTreeMap::new(),
            preflight: options.preflight.clone(),
            passed: false,
            failure: Some(FailureEvidence {
                stage: error.stage,
                message: error.message,
            }),
        },
    };
    if let Err(error) = write_report(&options.output, &report) {
        eprintln!("{error}");
        process::exit(2);
    }
    println!("{}", options.output.display());
    if !report.passed {
        process::exit(1);
    }
}

fn run(options: &Options) -> HarnessResult<RunSuccess> {
    options.validate_preflight()?;
    ensure_empty_target(&options.data_dir)?;
    let mut timings_ms = BTreeMap::new();

    let build_started = Instant::now();
    let (catalog, scale_table_id, writer_table_id) = build_catalog()?;
    let row_capacity = usize::try_from(options.target.rows)
        .map_err(|_| HarnessError::new("fixture", "row count does not fit this platform"))?;
    let payload = "x".repeat(options.target.payload_bytes);
    let mut scale_rows = Vec::with_capacity(row_capacity);
    for row_id in 0..options.target.rows {
        let row_id = i64::try_from(row_id)
            .map_err(|_| HarnessError::new("fixture", "row ID exceeds BIGINT"))?;
        scale_rows.push(Row::new(vec![
            Value::Int64(row_id),
            Value::Text(payload.clone()),
        ]));
    }
    let snapshot = LogicalDatabaseSnapshot {
        format_version: LOGICAL_SNAPSHOT_VERSION,
        source_generation: 0,
        catalog: Arc::new(catalog),
        tables: BTreeMap::from([
            (scale_table_id, Arc::new(scale_rows)),
            (writer_table_id, Arc::new(Vec::new())),
        ]),
    };
    timings_ms.insert(
        "fixtureBuild".to_owned(),
        elapsed_millis(build_started.elapsed()),
    );

    let restore_started = Instant::now();
    let engine = stage(
        "restore",
        Engine::restore_logical_snapshot(EngineConfig::new(&options.data_dir), snapshot),
    )?;
    timings_ms.insert(
        "initialRestore".to_owned(),
        elapsed_millis(restore_started.elapsed()),
    );

    let reopen_started = Instant::now();
    drop(engine);
    let engine = Arc::new(stage(
        "reopen",
        Engine::open(EngineConfig::new(&options.data_dir)),
    )?);
    let status = stage("status", engine.status_snapshot())?;
    timings_ms.insert(
        "reopen".to_owned(),
        elapsed_millis(reopen_started.elapsed()),
    );

    let query_started = Instant::now();
    let stream = validate_stream(&engine, &options.target)?;
    timings_ms.insert(
        "streamValidation".to_owned(),
        elapsed_millis(query_started.elapsed()),
    );

    let concurrency_started = Instant::now();
    let concurrency = validate_connections(Arc::clone(&engine), options.target.connections)?;
    timings_ms.insert(
        "connectionGate".to_owned(),
        elapsed_millis(concurrency_started.elapsed()),
    );

    let final_status = stage("final status", engine.status_snapshot())?;
    let data_file_bytes = stage(
        "data file metadata",
        fs::metadata(options.data_dir.join("ordadb.data")),
    )?
    .len();
    let data_directory_bytes = directory_bytes(&options.data_dir)?;
    let expected_sum = expected_id_sum(options.target.rows)?;

    let checks = vec![
        check(
            "databaseBytes",
            data_file_bytes >= options.target.database_bytes,
            format!(
                "{data_file_bytes} bytes on disk; target {}",
                options.target.database_bytes
            ),
        )?,
        check(
            "rowCount",
            status.row_count == options.target.rows
                && final_status.row_count == options.target.rows
                && stream.rows == options.target.rows,
            format!(
                "status={} final={} streamed={}",
                status.row_count, final_status.row_count, stream.rows
            ),
        )?,
        check(
            "rowChecksum",
            stream.id_sum == expected_sum
                && stream.min_id == Some(0)
                && stream.max_id
                    == Some(
                        i64::try_from(options.target.rows.saturating_sub(1))
                            .map_err(|_| HarnessError::new("checksum", "maximum ID overflowed"))?,
                    ),
            format!(
                "sum={} expected={} min={:?} max={:?}",
                stream.id_sum, expected_sum, stream.min_id, stream.max_id
            ),
        )?,
        check(
            "queryMemory",
            stream.execution_memory_peak_bytes > 0
                && stream.execution_memory_peak_bytes <= DEFAULT_QUERY_HARD_LIMIT_BYTES,
            format!(
                "peak={} hardLimit={DEFAULT_QUERY_HARD_LIMIT_BYTES}",
                stream.execution_memory_peak_bytes
            ),
        )?,
        check(
            "connections",
            concurrency.connected == options.target.connections
                && concurrency.reader_successes + 2 == options.target.connections,
            format!(
                "connected={} readers={}",
                concurrency.connected, concurrency.reader_successes
            ),
        )?,
        check(
            "singleWriter",
            concurrency.accepted_writers == 1
                && concurrency.rejected_writers == 1
                && concurrency.rows_after_rollback == 0,
            format!(
                "accepted={} rejected={} visibleAfterRollback={}",
                concurrency.accepted_writers,
                concurrency.rejected_writers,
                concurrency.rows_after_rollback
            ),
        )?,
        check(
            "durableReopen",
            status.durable_lsn.is_some()
                && status.dirty_page_count == 0
                && final_status.dirty_page_count == 0,
            format!(
                "durableLsn={:?} dirtyBefore={} dirtyAfter={}",
                status.durable_lsn, status.dirty_page_count, final_status.dirty_page_count
            ),
        )?,
    ];

    Ok(RunSuccess {
        observed: Observed {
            data_file_bytes,
            data_directory_bytes,
            rows: stream.rows,
            id_sum: stream.id_sum,
            min_id: stream.min_id,
            max_id: stream.max_id,
            execution_memory_peak_bytes: stream.execution_memory_peak_bytes,
            connected: concurrency.connected,
            reader_successes: concurrency.reader_successes,
            accepted_writers: concurrency.accepted_writers,
            rejected_writers: concurrency.rejected_writers,
            rows_after_rollback: concurrency.rows_after_rollback,
            generation: final_status.generation,
            durable_lsn: final_status.durable_lsn,
        },
        checks,
        timings_ms,
    })
}

fn build_catalog() -> HarnessResult<(Catalog, ordadb_types::TableId, ordadb_types::TableId)> {
    let mut catalog = Catalog::default();
    let schema = Identifier::unquoted("public");
    let scale_table_id = stage(
        "catalog",
        catalog.create_table(
            &schema,
            Identifier::unquoted("scale_rows"),
            vec![
                required_column("id", ScalarType::Int64),
                required_column("payload", ScalarType::Text),
            ],
        ),
    )?;
    let writer_table_id = stage(
        "catalog",
        catalog.create_table(
            &schema,
            Identifier::unquoted("writer_probe"),
            vec![required_column("id", ScalarType::Int64)],
        ),
    )?;
    Ok((catalog, scale_table_id, writer_table_id))
}

fn required_column(name: &str, data_type: ScalarType) -> NewColumn {
    let mut column = NewColumn::new(Identifier::unquoted(name), data_type);
    column.nullable = false;
    column
}

fn validate_stream(engine: &Engine, target: &Target) -> HarnessResult<StreamObservation> {
    let mut session = stage("stream connection", engine.connect())?;
    let mut stream = stage(
        "stream query",
        session.execute_stream("SELECT id, payload FROM scale_rows", &[]),
    )?;
    let mut rows = 0_u64;
    let mut id_sum = 0_i128;
    let mut min_id = None;
    let mut max_id = None;
    let mut complete = false;
    for event in stream.by_ref() {
        match stage("stream event", event)? {
            QueryEvent::Batch(batch) => {
                for row in batch.rows {
                    let [Value::Int64(row_id), Value::Text(payload)] = row.values.as_slice() else {
                        return Err(HarnessError::new(
                            "stream validation",
                            "scale row did not contain BIGINT and TEXT",
                        ));
                    };
                    if payload.len() != target.payload_bytes {
                        return Err(HarnessError::new(
                            "stream validation",
                            format!(
                                "row {row_id} payload was {} bytes, expected {}",
                                payload.len(),
                                target.payload_bytes
                            ),
                        ));
                    }
                    rows = rows.checked_add(1).ok_or_else(|| {
                        HarnessError::new("stream validation", "row count overflowed")
                    })?;
                    id_sum = id_sum.checked_add(i128::from(*row_id)).ok_or_else(|| {
                        HarnessError::new("stream validation", "row checksum overflowed")
                    })?;
                    min_id = Some(min_id.map_or(*row_id, |current: i64| current.min(*row_id)));
                    max_id = Some(max_id.map_or(*row_id, |current: i64| current.max(*row_id)));
                }
            }
            QueryEvent::Complete(_) => complete = true,
            QueryEvent::Schema(_) | QueryEvent::Progress(_) | QueryEvent::Notice(_) => {}
        }
    }
    if !complete {
        return Err(HarnessError::new(
            "stream validation",
            "query stream ended without Complete",
        ));
    }
    let execution_memory_peak_bytes = stream.execution_memory_peak_bytes().ok_or_else(|| {
        HarnessError::new(
            "stream validation",
            "SELECT stream did not expose query memory evidence",
        )
    })?;
    Ok(StreamObservation {
        rows,
        id_sum,
        min_id,
        max_id,
        execution_memory_peak_bytes,
    })
}

fn validate_connections(
    engine: Arc<Engine>,
    connections: usize,
) -> HarnessResult<ConnectionObservation> {
    let mut writer_session = stage("writer connection", engine.connect())?;
    let mut writer = stage("writer transaction", writer_session.begin())?;
    let writer_events = stage(
        "writer transaction",
        writer.execute("INSERT INTO writer_probe VALUES (1)", &[]),
    )?
    .collect::<Vec<_>>();
    if !writer_events
        .iter()
        .any(|event| matches!(event, QueryEvent::Complete(_)))
    {
        return Err(HarnessError::new(
            "writer transaction",
            "accepted writer did not produce Complete",
        ));
    }

    let mut sessions = Vec::with_capacity(connections.saturating_sub(1));
    for _ in 1..connections {
        sessions.push(stage("concurrent connection", engine.connect())?);
    }
    let barrier = Arc::new(Barrier::new(connections));
    let mut handles = Vec::with_capacity(sessions.len());
    for (index, mut session) in sessions.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || -> Result<ThreadOutcome, String> {
            barrier.wait();
            if index == 0 {
                return match session.execute("INSERT INTO writer_probe VALUES (2)", &[]) {
                    Err(error) if error.sql_state == "55P03" => Ok(ThreadOutcome::WriterRejected),
                    Err(error) => Err(format!(
                        "competing writer returned {}, expected 55P03",
                        error.sql_state
                    )),
                    Ok(_) => Err("unexpected second writer success".to_owned()),
                };
            }
            let rows = count_query_rows(&mut session, "SELECT id FROM writer_probe")
                .map_err(|error| error.to_string())?;
            if rows != 0 {
                return Err(format!("reader observed {rows} uncommitted writer rows"));
            }
            Ok(ThreadOutcome::Reader)
        }));
    }
    barrier.wait();

    let mut reader_successes = 0_usize;
    let mut rejected_writers = 0_usize;
    for handle in handles {
        match handle.join() {
            Ok(Ok(ThreadOutcome::Reader)) => reader_successes += 1,
            Ok(Ok(ThreadOutcome::WriterRejected)) => rejected_writers += 1,
            Ok(Err(message)) => return Err(HarnessError::new("connection gate", message)),
            Err(_) => {
                return Err(HarnessError::new(
                    "connection gate",
                    "connection worker panicked",
                ));
            }
        }
    }
    stage("writer rollback", writer.rollback())?;
    let mut verification = stage("rollback verification", engine.connect())?;
    let rows_after_rollback = count_query_rows(&mut verification, "SELECT id FROM writer_probe")?;

    Ok(ConnectionObservation {
        connected: connections,
        reader_successes,
        accepted_writers: 1,
        rejected_writers,
        rows_after_rollback,
    })
}

fn count_query_rows(session: &mut ordadb_engine::Session, sql: &str) -> HarnessResult<u64> {
    let stream = stage("connection query", session.execute_stream(sql, &[]))?;
    let mut rows = 0_u64;
    let mut complete = false;
    for event in stream {
        match stage("connection query event", event)? {
            QueryEvent::Batch(batch) => {
                rows = rows
                    .checked_add(batch.rows.len() as u64)
                    .ok_or_else(|| HarnessError::new("connection query", "row count overflowed"))?;
            }
            QueryEvent::Complete(_) => complete = true,
            QueryEvent::Schema(_) | QueryEvent::Progress(_) | QueryEvent::Notice(_) => {}
        }
    }
    if !complete {
        return Err(HarnessError::new(
            "connection query",
            "query ended without Complete",
        ));
    }
    Ok(rows)
}

fn expected_id_sum(rows: u64) -> HarnessResult<i128> {
    let rows = i128::from(rows);
    rows.checked_mul(rows.saturating_sub(1))
        .and_then(|value| value.checked_div(2))
        .ok_or_else(|| HarnessError::new("checksum", "expected ID checksum overflowed"))
}

fn ensure_empty_target(path: &Path) -> HarnessResult<()> {
    if !path.exists() {
        return Ok(());
    }
    let mut entries = stage("preflight", fs::read_dir(path))?;
    if stage("preflight", entries.next().transpose())?.is_some() {
        return Err(HarnessError::new(
            "preflight",
            format!("data directory {} is not empty", path.display()),
        ));
    }
    Ok(())
}

fn directory_bytes(path: &Path) -> HarnessResult<u64> {
    let mut total = 0_u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for entry in stage("directory size", fs::read_dir(&directory))? {
            let entry = stage("directory size", entry)?;
            let metadata = stage("directory size", fs::symlink_metadata(entry.path()))?;
            if metadata.file_type().is_symlink() {
                return Err(HarnessError::new(
                    "directory size",
                    "database directory contains an unexpected symbolic link",
                ));
            }
            if metadata.is_dir() {
                stack.push(entry.path());
            } else if metadata.is_file() {
                total = total.checked_add(metadata.len()).ok_or_else(|| {
                    HarnessError::new("directory size", "database directory size overflowed")
                })?;
            }
        }
    }
    Ok(total)
}

fn check(name: &str, passed: bool, detail: String) -> HarnessResult<CheckEvidence> {
    if !passed {
        return Err(HarnessError::new(name, detail));
    }
    Ok(CheckEvidence {
        name: name.to_owned(),
        passed,
        detail,
    })
}

fn write_report(path: &Path, report: &Report) -> HarnessResult<()> {
    if path.exists() {
        return Err(HarnessError::new(
            "evidence",
            format!("refusing to overwrite existing evidence {}", path.display()),
        ));
    }
    if let Some(parent) = path.parent() {
        stage("evidence", fs::create_dir_all(parent))?;
    }
    let temporary = path.with_extension(format!("tmp-{}", process::id()));
    let mut file = stage(
        "evidence",
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary),
    )?;
    let encoded = serde_json::to_vec_pretty(report)
        .map_err(|error| HarnessError::new("evidence", error.to_string()))?;
    stage("evidence", file.write_all(&encoded))?;
    stage("evidence", file.write_all(b"\n"))?;
    stage("evidence", file.sync_all())?;
    drop(file);
    stage("evidence", fs::rename(&temporary, path))
}

fn stage<T, E>(stage_name: &'static str, result: Result<T, E>) -> HarnessResult<T>
where
    E: Display,
{
    result.map_err(|error| HarnessError::new(stage_name, error.to_string()))
}

fn elapsed_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(elapsed_millis)
        .unwrap_or_default()
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
enum Profile {
    Smoke,
    Full,
}

impl Profile {
    fn parse(value: &str) -> HarnessResult<Self> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "full" => Ok(Self::Full),
            _ => Err(HarnessError::new(
                "arguments",
                "profile must be smoke or full",
            )),
        }
    }
}

#[derive(Debug)]
struct Options {
    profile: Profile,
    target: Target,
    data_dir: PathBuf,
    output: PathBuf,
    preflight: Option<PreflightEvidence>,
}

impl Options {
    fn parse(arguments: impl Iterator<Item = String>) -> HarnessResult<Self> {
        let mut profile = Profile::Smoke;
        let mut rows = None;
        let mut database_bytes = None;
        let mut connections = None;
        let mut data_dir = None;
        let mut output = None;
        let mut available_disk_bytes = None;
        let mut available_physical_memory_bytes = None;
        let mut confirm_full_scale = false;
        let mut arguments = arguments.peekable();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--profile" => profile = Profile::parse(&next_value(&mut arguments, &argument)?)?,
                "--rows" => {
                    rows = Some(parse_number(
                        &next_value(&mut arguments, &argument)?,
                        "rows",
                    )?)
                }
                "--target-bytes" => {
                    database_bytes = Some(parse_number(
                        &next_value(&mut arguments, &argument)?,
                        "target bytes",
                    )?)
                }
                "--connections" => {
                    connections = Some(parse_number(
                        &next_value(&mut arguments, &argument)?,
                        "connections",
                    )?)
                }
                "--data-dir" => data_dir = Some(next_value(&mut arguments, &argument)?.into()),
                "--output" => output = Some(next_value(&mut arguments, &argument)?.into()),
                "--available-disk-bytes" => {
                    available_disk_bytes = Some(parse_number(
                        &next_value(&mut arguments, &argument)?,
                        "available disk bytes",
                    )?)
                }
                "--available-memory-bytes" => {
                    available_physical_memory_bytes = Some(parse_number(
                        &next_value(&mut arguments, &argument)?,
                        "available physical-memory bytes",
                    )?)
                }
                "--confirm-full-scale" => confirm_full_scale = true,
                _ => {
                    return Err(HarnessError::new(
                        "arguments",
                        format!("unknown argument {argument}"),
                    ));
                }
            }
        }

        let defaults = match profile {
            Profile::Smoke => (20_000_u64, 16 * MIB, 8_usize),
            Profile::Full => (FULL_ROWS, FULL_DATABASE_BYTES, FULL_CONNECTIONS),
        };
        let target = Target {
            rows: rows.unwrap_or(defaults.0),
            database_bytes: database_bytes.unwrap_or(defaults.1),
            connections: connections.unwrap_or(defaults.2),
            writers: 1,
            payload_bytes: 0,
        };
        if matches!(profile, Profile::Full)
            && (!confirm_full_scale
                || target.rows != FULL_ROWS
                || target.database_bytes != FULL_DATABASE_BYTES
                || target.connections != FULL_CONNECTIONS)
        {
            return Err(HarnessError::new(
                "arguments",
                "full profile requires --confirm-full-scale and the fixed 20 GiB / 10M row / 32 connection target",
            ));
        }
        if target.rows == 0 || target.database_bytes == 0 || target.connections < 2 {
            return Err(HarnessError::new(
                "arguments",
                "rows and target bytes must be non-zero and connections must be at least two",
            ));
        }
        let payload_bytes = usize::try_from(target.database_bytes.div_ceil(target.rows))
            .map_err(|_| HarnessError::new("arguments", "payload width overflowed"))?;
        if payload_bytes == 0 || payload_bytes > MAX_ROW_PAYLOAD_BYTES {
            return Err(HarnessError::new(
                "arguments",
                format!(
                    "computed payload width {payload_bytes} is outside 1..={MAX_ROW_PAYLOAD_BYTES}"
                ),
            ));
        }
        let target = Target {
            payload_bytes,
            ..target
        };
        let preflight = match (available_disk_bytes, available_physical_memory_bytes) {
            (Some(available_disk_bytes), Some(available_physical_memory_bytes)) => {
                Some(PreflightEvidence {
                    available_disk_bytes,
                    available_physical_memory_bytes,
                    required_disk_bytes: FULL_REQUIRED_FREE_DISK_BYTES,
                    required_physical_memory_bytes: FULL_REQUIRED_FREE_MEMORY_BYTES,
                })
            }
            (None, None) if matches!(profile, Profile::Smoke) => None,
            (None, None) => {
                return Err(HarnessError::new(
                    "arguments",
                    "full profile requires available disk and physical-memory evidence",
                ));
            }
            _ => {
                return Err(HarnessError::new(
                    "arguments",
                    "available disk and physical-memory bytes must be supplied together",
                ));
            }
        };
        let timestamp = unix_millis();
        Ok(Self {
            profile,
            target,
            data_dir: data_dir.unwrap_or_else(|| {
                PathBuf::from(format!("target/final-scale/runs/{timestamp}/data"))
            }),
            output: output.unwrap_or_else(|| {
                PathBuf::from(format!("target/final-scale/evidence/{timestamp}.json"))
            }),
            preflight,
        })
    }

    fn validate_preflight(&self) -> HarnessResult<()> {
        if !matches!(self.profile, Profile::Full) {
            return Ok(());
        }
        let preflight = self.preflight.as_ref().ok_or_else(|| {
            HarnessError::new(
                "resource preflight",
                "full profile has no resource evidence",
            )
        })?;
        if preflight.available_disk_bytes < preflight.required_disk_bytes {
            return Err(HarnessError::new(
                "resource preflight",
                format!(
                    "full scale requires at least {} free disk bytes; {} are available",
                    preflight.required_disk_bytes, preflight.available_disk_bytes
                ),
            ));
        }
        if preflight.available_physical_memory_bytes < preflight.required_physical_memory_bytes {
            return Err(HarnessError::new(
                "resource preflight",
                format!(
                    "full scale requires at least {} free physical-memory bytes for the current v1 snapshot path; {} are available",
                    preflight.required_physical_memory_bytes,
                    preflight.available_physical_memory_bytes
                ),
            ));
        }
        Ok(())
    }
}

fn next_value(arguments: &mut impl Iterator<Item = String>, option: &str) -> HarnessResult<String> {
    arguments
        .next()
        .ok_or_else(|| HarnessError::new("arguments", format!("{option} requires a value")))
}

fn parse_number<T>(value: &str, name: &str) -> HarnessResult<T>
where
    T: std::str::FromStr,
    T::Err: Display,
{
    value
        .parse()
        .map_err(|error| HarnessError::new("arguments", format!("invalid {name}: {error}")))
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct Target {
    database_bytes: u64,
    rows: u64,
    connections: usize,
    writers: usize,
    payload_bytes: usize,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Observed {
    data_file_bytes: u64,
    data_directory_bytes: u64,
    rows: u64,
    id_sum: i128,
    min_id: Option<i64>,
    max_id: Option<i64>,
    execution_memory_peak_bytes: usize,
    connected: usize,
    reader_successes: usize,
    accepted_writers: usize,
    rejected_writers: usize,
    rows_after_rollback: u64,
    generation: u64,
    durable_lsn: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckEvidence {
    name: String,
    passed: bool,
    detail: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FailureEvidence {
    stage: String,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PreflightEvidence {
    available_disk_bytes: u64,
    available_physical_memory_bytes: u64,
    required_disk_bytes: u64,
    required_physical_memory_bytes: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    schema_version: u16,
    profile: Profile,
    started_at_unix_ms: u64,
    elapsed_ms: u64,
    target: Target,
    observed: Observed,
    checks: Vec<CheckEvidence>,
    timings_ms: BTreeMap<String, u64>,
    preflight: Option<PreflightEvidence>,
    passed: bool,
    failure: Option<FailureEvidence>,
}

struct RunSuccess {
    observed: Observed,
    checks: Vec<CheckEvidence>,
    timings_ms: BTreeMap<String, u64>,
}

struct StreamObservation {
    rows: u64,
    id_sum: i128,
    min_id: Option<i64>,
    max_id: Option<i64>,
    execution_memory_peak_bytes: usize,
}

struct ConnectionObservation {
    connected: usize,
    reader_successes: usize,
    accepted_writers: usize,
    rejected_writers: usize,
    rows_after_rollback: u64,
}

enum ThreadOutcome {
    Reader,
    WriterRejected,
}

type HarnessResult<T> = Result<T, HarnessError>;

#[derive(Debug)]
struct HarnessError {
    stage: String,
    message: String,
}

impl HarnessError {
    fn new(stage: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            stage: stage.into(),
            message: message.into(),
        }
    }
}

impl Display for HarnessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.stage, self.message)
    }
}

impl std::error::Error for HarnessError {}
