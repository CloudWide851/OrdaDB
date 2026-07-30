use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use ordadb_engine::{Engine, EngineConfig, LOGICAL_SNAPSHOT_VERSION, LogicalDatabaseSnapshot};
use ordadb_types::{DbError, Result, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use uuid::Uuid;

const ARCHIVE_MAGIC: [u8; 8] = *b"ORDBAK01";
pub const ARCHIVE_HEADER_BYTES: u64 = 8 + 2 + 2 + 8 + 32;
const COPY_BUFFER_BYTES: usize = 1024 * 1024;
pub const ARCHIVE_FORMAT_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveLimits {
    pub max_archive_bytes: u64,
    pub max_tables: usize,
    pub max_rows: u64,
    pub max_value_bytes: usize,
    pub max_vector_dimensions: usize,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            max_archive_bytes: 64 * 1024 * 1024 * 1024,
            max_tables: 65_536,
            max_rows: 100_000_000,
            max_value_bytes: 64 * 1024 * 1024,
            max_vector_dimensions: 4_096,
        }
    }
}

impl ArchiveLimits {
    fn validate(self) -> Result<Self> {
        if self.max_archive_bytes == 0
            || self.max_tables == 0
            || self.max_rows == 0
            || self.max_value_bytes == 0
            || self.max_vector_dimensions == 0
        {
            return Err(invalid("logical archive limits must be non-zero"));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArchiveMetadata {
    archive_id: Uuid,
    created_at: DateTime<Utc>,
    producer_version: String,
    source_generation: u64,
    database_name: String,
    table_count: u64,
    row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogicalArchive {
    metadata: ArchiveMetadata,
    snapshot: LogicalDatabaseSnapshot,
}

impl LogicalArchive {
    #[must_use]
    pub const fn snapshot(&self) -> &LogicalDatabaseSnapshot {
        &self.snapshot
    }

    #[must_use]
    pub fn into_snapshot(self) -> LogicalDatabaseSnapshot {
        self.snapshot
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSummary {
    pub archive_id: Uuid,
    pub path: PathBuf,
    pub created_at: DateTime<Utc>,
    pub source_generation: u64,
    pub table_count: u64,
    pub row_count: u64,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreSummary {
    pub archive_id: Uuid,
    pub data_dir: PathBuf,
    pub source_generation: u64,
    pub restored_generation: u64,
    pub table_count: u64,
    pub row_count: u64,
}

pub fn write_archive(
    engine: &Engine,
    path: impl AsRef<Path>,
    limits: ArchiveLimits,
) -> Result<BackupSummary> {
    write_snapshot_archive(engine.logical_snapshot()?, path, limits)
}

pub fn write_snapshot_archive(
    snapshot: LogicalDatabaseSnapshot,
    path: impl AsRef<Path>,
    limits: ArchiveLimits,
) -> Result<BackupSummary> {
    let limits = limits.validate()?;
    let path = absolute_output_path(path.as_ref())?;
    if path.exists() {
        return Err(
            DbError::new("55000", "logical backup destination already exists")
                .with_hint("choose a new archive path; existing backups are never overwritten"),
        );
    }
    validate_snapshot(&snapshot, limits)?;
    let (table_count, row_count) = snapshot_counts(&snapshot)?;
    let created_at = DateTime::from_timestamp(Utc::now().timestamp(), 0)
        .ok_or_else(|| internal_time_error("current archive timestamp is out of range"))?;
    let archive_id = Uuid::new_v4();
    let database_name = snapshot.catalog.database().name.to_string();
    let archive = LogicalArchive {
        metadata: ArchiveMetadata {
            archive_id,
            created_at,
            producer_version: env!("CARGO_PKG_VERSION").to_owned(),
            source_generation: snapshot.source_generation,
            database_name,
            table_count,
            row_count,
        },
        snapshot,
    };

    let parent = path
        .parent()
        .ok_or_else(|| invalid("logical backup destination has no parent directory"))?;
    let mut payload = NamedTempFile::new_in(parent)
        .map_err(|error| io_error("failed to create logical backup payload", error))?;
    serde_json::to_writer(BufWriter::new(payload.as_file_mut()), &archive)
        .map_err(|error| archive_error("failed to encode logical backup payload", error))?;
    payload
        .as_file_mut()
        .sync_all()
        .map_err(|error| io_error("failed to synchronize logical backup payload", error))?;
    let payload_bytes = payload
        .as_file()
        .metadata()
        .map_err(|error| io_error("failed to inspect logical backup payload", error))?
        .len();
    if payload_bytes > limits.max_archive_bytes {
        return Err(resource_limit(format!(
            "logical backup payload is {payload_bytes} bytes; limit is {}",
            limits.max_archive_bytes
        )));
    }
    let digest = hash_file(payload.as_file_mut(), payload_bytes)?;

    let mut output = NamedTempFile::new_in(parent)
        .map_err(|error| io_error("failed to create logical backup archive", error))?;
    {
        let mut writer = BufWriter::new(output.as_file_mut());
        writer
            .write_all(&ARCHIVE_MAGIC)
            .and_then(|_| writer.write_all(&ARCHIVE_FORMAT_VERSION.to_le_bytes()))
            .and_then(|_| writer.write_all(&0_u16.to_le_bytes()))
            .and_then(|_| writer.write_all(&payload_bytes.to_le_bytes()))
            .and_then(|_| writer.write_all(&digest))
            .map_err(|error| io_error("failed to write logical backup header", error))?;
        payload
            .as_file_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|error| io_error("failed to rewind logical backup payload", error))?;
        std::io::copy(payload.as_file_mut(), &mut writer)
            .map_err(|error| io_error("failed to write logical backup payload", error))?;
        writer
            .flush()
            .map_err(|error| io_error("failed to flush logical backup archive", error))?;
    }
    output
        .as_file_mut()
        .sync_all()
        .map_err(|error| io_error("failed to synchronize logical backup archive", error))?;
    output
        .persist(&path)
        .map_err(|error| io_error("failed to publish logical backup archive", error.error))?;
    sync_parent(parent)?;

    Ok(BackupSummary {
        archive_id,
        path,
        created_at,
        source_generation: archive.metadata.source_generation,
        table_count,
        row_count,
        bytes: payload_bytes
            .checked_add(ARCHIVE_HEADER_BYTES)
            .ok_or_else(|| resource_limit("logical backup archive size overflowed"))?,
        sha256: hex_digest(&digest),
    })
}

pub fn estimate_snapshot_archive_bytes(
    snapshot: &LogicalDatabaseSnapshot,
    limits: ArchiveLimits,
) -> Result<u64> {
    let limits = limits.validate()?;
    validate_snapshot(snapshot, limits)?;
    let (table_count, row_count) = snapshot_counts(snapshot)?;
    let archive = LogicalArchive {
        metadata: ArchiveMetadata {
            archive_id: Uuid::nil(),
            created_at: DateTime::from_timestamp(0, 0)
                .ok_or_else(|| internal_time_error("Unix epoch is out of range"))?,
            producer_version: env!("CARGO_PKG_VERSION").to_owned(),
            source_generation: snapshot.source_generation,
            database_name: snapshot.catalog.database().name.to_string(),
            table_count,
            row_count,
        },
        snapshot: snapshot.clone(),
    };
    let payload_bytes = u64::try_from(
        serde_json::to_vec(&archive)
            .map_err(|error| archive_error("failed to estimate logical backup payload", error))?
            .len(),
    )
    .map_err(|_| resource_limit("logical backup estimate exceeds u64"))?;
    if payload_bytes > limits.max_archive_bytes {
        return Err(resource_limit(format!(
            "logical backup payload is {payload_bytes} bytes; limit is {}",
            limits.max_archive_bytes
        )));
    }
    payload_bytes
        .checked_add(ARCHIVE_HEADER_BYTES)
        .ok_or_else(|| resource_limit("logical backup archive size overflowed"))
}

pub fn read_archive(path: impl AsRef<Path>, limits: ArchiveLimits) -> Result<LogicalArchive> {
    let limits = limits.validate()?;
    let path = path.as_ref();
    let mut file =
        File::open(path).map_err(|error| io_error("failed to open logical backup", error))?;
    let file_bytes = file
        .metadata()
        .map_err(|error| io_error("failed to inspect logical backup", error))?
        .len();
    let maximum = limits
        .max_archive_bytes
        .checked_add(ARCHIVE_HEADER_BYTES)
        .ok_or_else(|| resource_limit("logical archive size limit overflowed"))?;
    if file_bytes > maximum {
        return Err(resource_limit(format!(
            "logical backup is {file_bytes} bytes; limit is {maximum}"
        )));
    }
    if file_bytes < ARCHIVE_HEADER_BYTES {
        return Err(corruption("logical backup header is truncated"));
    }

    let mut magic = [0_u8; 8];
    let mut version = [0_u8; 2];
    let mut flags = [0_u8; 2];
    let mut payload_length = [0_u8; 8];
    let mut expected_digest = [0_u8; 32];
    file.read_exact(&mut magic)
        .and_then(|_| file.read_exact(&mut version))
        .and_then(|_| file.read_exact(&mut flags))
        .and_then(|_| file.read_exact(&mut payload_length))
        .and_then(|_| file.read_exact(&mut expected_digest))
        .map_err(|error| io_error("failed to read logical backup header", error))?;
    if magic != ARCHIVE_MAGIC {
        return Err(corruption("logical backup magic is invalid"));
    }
    let version = u16::from_le_bytes(version);
    if version != ARCHIVE_FORMAT_VERSION {
        return Err(DbError::new(
            "0A000",
            format!("logical backup version {version} is not supported"),
        )
        .with_hint("use a compatible OrdaDB version or perform an explicit migration"));
    }
    if u16::from_le_bytes(flags) != 0 {
        return Err(DbError::new(
            "0A000",
            "logical backup uses unsupported format flags",
        ));
    }
    let payload_length = u64::from_le_bytes(payload_length);
    if payload_length > limits.max_archive_bytes
        || payload_length
            .checked_add(ARCHIVE_HEADER_BYTES)
            .is_none_or(|expected| expected != file_bytes)
    {
        return Err(corruption(
            "logical backup payload length does not match the file",
        ));
    }
    let actual_digest = hash_file(&mut file, payload_length)?;
    if actual_digest != expected_digest {
        return Err(corruption("logical backup checksum does not match"));
    }
    file.seek(SeekFrom::Start(ARCHIVE_HEADER_BYTES))
        .map_err(|error| io_error("failed to rewind logical backup payload", error))?;
    let reader = BufReader::new(file.take(payload_length));
    let archive: LogicalArchive = serde_json::from_reader(reader)
        .map_err(|error| archive_error("failed to decode logical backup payload", error))?;
    if archive.snapshot.format_version != LOGICAL_SNAPSHOT_VERSION
        || archive.metadata.source_generation != archive.snapshot.source_generation
    {
        return Err(corruption(
            "logical backup metadata does not match its database snapshot",
        ));
    }
    validate_snapshot(&archive.snapshot, limits)?;
    let (table_count, row_count) = snapshot_counts(&archive.snapshot)?;
    if table_count != archive.metadata.table_count || row_count != archive.metadata.row_count {
        return Err(corruption(
            "logical backup counts do not match its database snapshot",
        ));
    }
    if archive.metadata.database_name != archive.snapshot.catalog.database().name.to_string() {
        return Err(corruption(
            "logical backup database name does not match its catalog",
        ));
    }
    Ok(archive)
}

pub fn restore_archive_to_new(
    archive_path: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
    limits: ArchiveLimits,
) -> Result<RestoreSummary> {
    let data_dir = data_dir.as_ref();
    if data_dir.exists()
        && fs::read_dir(data_dir)
            .map_err(|error| io_error("failed to inspect restore target", error))?
            .next()
            .transpose()
            .map_err(|error| io_error("failed to inspect restore target", error))?
            .is_some()
    {
        return Err(DbError::new(
            "55000",
            "logical restore target must be absent or empty",
        ));
    }
    let archive = read_archive(archive_path, limits)?;
    let source_generation = archive.snapshot.source_generation;
    let archive_id = archive.metadata.archive_id;
    let table_count = archive.metadata.table_count;
    let row_count = archive.metadata.row_count;
    let engine = Engine::restore_logical_snapshot(EngineConfig::new(data_dir), archive.snapshot)?;
    let restored_generation = engine.status_snapshot()?.generation;
    drop(engine);
    Engine::open(EngineConfig::new(data_dir))?;
    Ok(RestoreSummary {
        archive_id,
        data_dir: data_dir.to_path_buf(),
        source_generation,
        restored_generation,
        table_count,
        row_count,
    })
}

pub fn restore_archive_into_engine(
    engine: &Engine,
    archive_path: impl AsRef<Path>,
    limits: ArchiveLimits,
) -> Result<RestoreSummary> {
    let archive = read_archive(archive_path, limits)?;
    let source_generation = archive.snapshot.source_generation;
    let archive_id = archive.metadata.archive_id;
    let table_count = archive.metadata.table_count;
    let row_count = archive.metadata.row_count;
    engine.replace_logical_snapshot(archive.snapshot)?;
    engine.checkpoint()?;
    Ok(RestoreSummary {
        archive_id,
        data_dir: engine.config().data_dir.clone(),
        source_generation,
        restored_generation: engine.status_snapshot()?.generation,
        table_count,
        row_count,
    })
}

pub fn restore_archive_atomic(
    archive_path: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
    limits: ArchiveLimits,
) -> Result<RestoreSummary> {
    let data_dir = data_dir.as_ref();
    let parent = data_dir
        .parent()
        .ok_or_else(|| invalid("logical restore target has no parent directory"))?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error("failed to create logical restore parent", error))?;
    let nonce = Uuid::new_v4();
    let candidate = parent.join(format!(".ordadb-restore-{nonce}"));
    let rollback = parent.join(format!(".ordadb-rollback-{nonce}"));
    fs::create_dir(&candidate)
        .map_err(|error| io_error("failed to create logical restore candidate", error))?;
    let candidate_result = restore_archive_to_new(archive_path, &candidate, limits);
    let mut summary = match candidate_result {
        Ok(summary) => summary,
        Err(error) => {
            let _ = fs::remove_dir_all(&candidate);
            return Err(error);
        }
    };

    let had_active = data_dir.exists();
    if had_active {
        fs::rename(data_dir, &rollback)
            .map_err(|error| io_error("failed to preserve active database for restore", error))?;
    }
    if let Err(error) = fs::rename(&candidate, data_dir) {
        if had_active {
            let _ = fs::rename(&rollback, data_dir);
        }
        let _ = fs::remove_dir_all(&candidate);
        return Err(io_error("failed to activate restored database", error));
    }
    match Engine::open(EngineConfig::new(data_dir)) {
        Ok(engine) => drop(engine),
        Err(error) => {
            let failed = parent.join(format!(".ordadb-failed-{nonce}"));
            let _ = fs::rename(data_dir, &failed);
            if had_active {
                fs::rename(&rollback, data_dir).map_err(|rollback_error| {
                    io_error(
                        "restored database failed to open and rollback also failed",
                        rollback_error,
                    )
                    .with_detail(error.to_string())
                })?;
            }
            let _ = fs::remove_dir_all(&failed);
            return Err(error);
        }
    }
    if had_active {
        fs::remove_dir_all(&rollback)
            .map_err(|error| io_error("failed to remove proven restore rollback", error))?;
    }
    sync_parent(parent)?;
    summary.data_dir = data_dir.to_path_buf();
    Ok(summary)
}

fn validate_snapshot(snapshot: &LogicalDatabaseSnapshot, limits: ArchiveLimits) -> Result<()> {
    if snapshot.format_version != LOGICAL_SNAPSHOT_VERSION {
        return Err(DbError::new(
            "0A000",
            format!(
                "logical snapshot version {} is not supported",
                snapshot.format_version
            ),
        ));
    }
    if snapshot.tables.len() > limits.max_tables {
        return Err(resource_limit(format!(
            "logical snapshot has {} tables; limit is {}",
            snapshot.tables.len(),
            limits.max_tables
        )));
    }
    let mut rows = 0_u64;
    for table_rows in snapshot.tables.values() {
        let table_row_count = u64::try_from(table_rows.len())
            .map_err(|_| resource_limit("logical snapshot row count does not fit in u64"))?;
        rows = rows
            .checked_add(table_row_count)
            .ok_or_else(|| resource_limit("logical snapshot row count overflowed"))?;
        if rows > limits.max_rows {
            return Err(resource_limit(format!(
                "logical snapshot has more than {} rows",
                limits.max_rows
            )));
        }
        for row in table_rows.iter() {
            for value in &row.values {
                validate_value(value, limits)?;
            }
        }
    }
    Ok(())
}

fn validate_value(value: &Value, limits: ArchiveLimits) -> Result<()> {
    let bytes = match value {
        Value::Text(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::Json(value) | Value::Jsonb(value) => serde_json::to_vec(value)
            .map_err(|error| archive_error("failed to size JSON value", error))?
            .len(),
        Value::Vector(value) => {
            if value.len() > limits.max_vector_dimensions {
                return Err(resource_limit(format!(
                    "vector has {} dimensions; limit is {}",
                    value.len(),
                    limits.max_vector_dimensions
                )));
            }
            value
                .len()
                .checked_mul(size_of::<f32>())
                .ok_or_else(|| resource_limit("vector byte size overflowed"))?
        }
        _ => 0,
    };
    if bytes > limits.max_value_bytes {
        return Err(resource_limit(format!(
            "logical value is {bytes} bytes; limit is {}",
            limits.max_value_bytes
        )));
    }
    Ok(())
}

fn snapshot_counts(snapshot: &LogicalDatabaseSnapshot) -> Result<(u64, u64)> {
    let table_count = u64::try_from(snapshot.tables.len())
        .map_err(|_| resource_limit("logical snapshot table count does not fit in u64"))?;
    let row_count = snapshot.tables.values().try_fold(0_u64, |total, rows| {
        let rows = u64::try_from(rows.len())
            .map_err(|_| resource_limit("logical snapshot row count does not fit in u64"))?;
        total
            .checked_add(rows)
            .ok_or_else(|| resource_limit("logical snapshot row count overflowed"))
    })?;
    Ok((table_count, row_count))
}

fn absolute_output_path(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(invalid("logical backup destination must not be empty"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| invalid("logical backup destination has no parent directory"))?;
    let parent = parent
        .canonicalize()
        .map_err(|error| io_error("failed to resolve logical backup parent", error))?;
    let name = path
        .file_name()
        .ok_or_else(|| invalid("logical backup destination has no file name"))?;
    Ok(parent.join(name))
}

fn hash_file(file: &mut File, length: u64) -> Result<[u8; 32]> {
    file.seek(SeekFrom::Start(
        file.metadata()
            .map_err(|error| io_error("failed to inspect logical backup stream", error))?
            .len()
            .saturating_sub(length),
    ))
    .map_err(|error| io_error("failed to seek logical backup stream", error))?;
    let mut reader = file.take(length);
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|error| io_error("failed to hash logical backup payload", error))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let digest = digest.finalize();
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);
    Ok(output)
}

fn hex_digest(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(windows)]
fn sync_parent(parent: &Path) -> Result<()> {
    // `FlushFileBuffers` rejects ordinary NT directory handles even when they
    // are opened with backup semantics. The archive file itself is synced
    // before the same-volume atomic rename; re-stat the directory so missing
    // or inaccessible parents are still reported at this boundary.
    fs::metadata(parent)
        .map(|_| ())
        .map_err(|error| io_error("failed to verify logical backup directory", error))
}

#[cfg(not(windows))]
fn sync_parent(parent: &Path) -> Result<()> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("failed to synchronize logical backup directory", error))
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn resource_limit(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

fn corruption(message: impl Into<String>) -> DbError {
    DbError::new("XX001", message)
        .with_hint("discard this archive and restore from a verified logical backup")
}

fn io_error(context: impl Into<String>, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

fn archive_error(context: impl Into<String>, error: serde_json::Error) -> DbError {
    DbError::new("XX001", context).with_detail(error.to_string())
}

fn internal_time_error(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordadb_types::Value;
    use tempfile::tempdir;

    fn seeded_engine(data_dir: &Path) -> Engine {
        let engine = Engine::open(EngineConfig::new(data_dir)).expect("engine");
        let mut session = engine.connect().expect("session");
        session
            .execute(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT NOT NULL)",
                &[],
            )
            .expect("create");
        session
            .execute(
                "INSERT INTO items (id, label) VALUES ($1, $2)",
                &[Value::Int64(1), Value::Text("alpha".into())],
            )
            .expect("insert");
        engine
    }

    #[test]
    fn archive_round_trip_rebuilds_a_queryable_database() {
        let directory = tempdir().expect("tempdir");
        let source = seeded_engine(&directory.path().join("source"));
        let archive_path = directory.path().join("backup.orda");
        let summary =
            write_archive(&source, &archive_path, ArchiveLimits::default()).expect("backup");
        assert_eq!(summary.table_count, 1);
        assert_eq!(summary.row_count, 1);
        drop(source);

        let restored_dir = directory.path().join("restored");
        let restored =
            restore_archive_to_new(&archive_path, &restored_dir, ArchiveLimits::default())
                .expect("restore");
        assert_eq!(restored.archive_id, summary.archive_id);
        let engine = Engine::open(EngineConfig::new(restored_dir)).expect("reopen");
        let mut session = engine.connect().expect("session");
        let events = session
            .execute("SELECT label FROM items WHERE id = $1", &[Value::Int64(1)])
            .expect("query")
            .collect::<Vec<_>>();
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ordadb_types::QueryEvent::Batch(batch)
                    if batch.rows == vec![ordadb_types::Row::new(vec![Value::Text("alpha".into())])]
            )
        }));
    }

    #[test]
    fn archive_rejects_checksum_truncation_version_and_limits() {
        let directory = tempdir().expect("tempdir");
        let source = seeded_engine(&directory.path().join("source"));
        let archive_path = directory.path().join("backup.orda");
        write_archive(&source, &archive_path, ArchiveLimits::default()).expect("backup");
        let original = fs::read(&archive_path).expect("archive bytes");

        let truncated = directory.path().join("truncated.orda");
        fs::write(&truncated, &original[..original.len() - 1]).expect("truncated");
        assert_eq!(
            read_archive(&truncated, ArchiveLimits::default())
                .expect_err("truncation")
                .sql_state,
            "XX001"
        );

        let corrupted = directory.path().join("corrupted.orda");
        let mut bytes = original.clone();
        *bytes.last_mut().expect("payload byte") ^= 0x55;
        fs::write(&corrupted, bytes).expect("corrupt");
        assert_eq!(
            read_archive(&corrupted, ArchiveLimits::default())
                .expect_err("checksum")
                .sql_state,
            "XX001"
        );

        let future = directory.path().join("future.orda");
        let mut bytes = original;
        bytes[8..10].copy_from_slice(&2_u16.to_le_bytes());
        fs::write(&future, bytes).expect("future");
        assert_eq!(
            read_archive(&future, ArchiveLimits::default())
                .expect_err("version")
                .sql_state,
            "0A000"
        );

        let tiny = ArchiveLimits {
            max_archive_bytes: 32,
            ..ArchiveLimits::default()
        };
        assert_eq!(
            read_archive(&archive_path, tiny)
                .expect_err("limit")
                .sql_state,
            "54000"
        );
    }

    #[test]
    fn atomic_restore_replaces_valid_data_and_rolls_forward_cleanly() {
        let directory = tempdir().expect("tempdir");
        let source = seeded_engine(&directory.path().join("source"));
        let archive_path = directory.path().join("backup.orda");
        write_archive(&source, &archive_path, ArchiveLimits::default()).expect("backup");
        drop(source);

        let active_dir = directory.path().join("active");
        let active = Engine::open(EngineConfig::new(&active_dir)).expect("active");
        drop(active);
        let summary = restore_archive_atomic(&archive_path, &active_dir, ArchiveLimits::default())
            .expect("atomic restore");
        assert_eq!(summary.row_count, 1);
        assert!(
            !directory
                .path()
                .read_dir()
                .expect("parent entries")
                .any(|entry| entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".ordadb-rollback-"))
        );
    }
}
