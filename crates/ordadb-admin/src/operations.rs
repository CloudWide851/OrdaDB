use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{DateTime, Utc};
use ordadb_backup::{
    ArchiveLimits, TableTransferRequest, TransferLimits, export_table, import_table,
    resolve_operation_path, restore_archive_into_engine, write_archive,
};
use ordadb_engine::Engine;
use ordadb_types::{DbError, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_OPERATION_HISTORY: usize = 128;
const MAX_ACTIVE_OPERATIONS: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Backup,
    Restore,
    Import,
    Export,
}

impl OperationKind {
    const fn destructive(self) -> bool {
        matches!(self, Self::Restore | Self::Import)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl OperationState {
    const fn active(self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationRecord {
    pub operation_id: Uuid,
    pub kind: OperationKind,
    pub state: OperationState,
    pub path: PathBuf,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
    pub error: Option<DbError>,
}

#[derive(Debug, Clone)]
pub enum StartOperation {
    Backup { path: PathBuf },
    Restore { path: PathBuf },
    Import { request: TableTransferRequest },
    Export { request: TableTransferRequest },
}

impl StartOperation {
    const fn kind(&self) -> OperationKind {
        match self {
            Self::Backup { .. } => OperationKind::Backup,
            Self::Restore { .. } => OperationKind::Restore,
            Self::Import { .. } => OperationKind::Import,
            Self::Export { .. } => OperationKind::Export,
        }
    }
}

#[derive(Debug, Default)]
struct OperationStateStore {
    records: BTreeMap<Uuid, OperationRecord>,
    order: VecDeque<Uuid>,
    cancellations: BTreeMap<Uuid, Arc<AtomicBool>>,
}

pub struct OperationManager {
    engine: Arc<Engine>,
    operations_root: PathBuf,
    state: Mutex<OperationStateStore>,
}

impl std::fmt::Debug for OperationManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OperationManager")
            .field("operations_root", &self.operations_root)
            .finish_non_exhaustive()
    }
}

impl OperationManager {
    #[must_use]
    pub fn new(engine: Arc<Engine>, operations_root: impl Into<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            engine,
            operations_root: operations_root.into(),
            state: Mutex::new(OperationStateStore::default()),
        })
    }

    #[must_use]
    pub fn operations_root(&self) -> &Path {
        &self.operations_root
    }

    pub fn start(self: &Arc<Self>, operation: StartOperation) -> Result<OperationRecord> {
        std::fs::create_dir_all(&self.operations_root).map_err(|error| {
            DbError::new("58030", "failed to create operations root").with_detail(error.to_string())
        })?;
        let kind = operation.kind();
        let (path, schema, table) = self.resolve_operation(&operation)?;
        let operation_id = Uuid::new_v4();
        let cancellation = Arc::new(AtomicBool::new(false));
        let record = OperationRecord {
            operation_id,
            kind,
            state: OperationState::Queued,
            path: display_path(&self.operations_root, &path),
            schema,
            table,
            started_at: None,
            finished_at: None,
            rows: None,
            bytes: None,
            error: None,
        };
        {
            let mut state = self.lock()?;
            let active = state
                .records
                .values()
                .filter(|record| record.state.active())
                .count();
            if active >= MAX_ACTIVE_OPERATIONS {
                return Err(DbError::new(
                    "53300",
                    "too many administration operations are active",
                ));
            }
            if kind.destructive()
                && state
                    .records
                    .values()
                    .any(|record| record.kind.destructive() && record.state.active())
            {
                return Err(DbError::new(
                    "55P03",
                    "another destructive administration operation is active",
                ));
            }
            prune_history(&mut state);
            state.order.push_back(operation_id);
            state.records.insert(operation_id, record.clone());
            state
                .cancellations
                .insert(operation_id, Arc::clone(&cancellation));
        }

        let manager = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            manager.mark_running(operation_id);
            let outcome = if cancellation.load(Ordering::Acquire) {
                Err(DbError::new(
                    "57014",
                    "administration operation was cancelled",
                ))
            } else {
                manager.execute(operation, &path, &cancellation)
            };
            manager.mark_finished(operation_id, outcome, &cancellation);
        });
        Ok(record)
    }

    pub fn list(&self) -> Result<Vec<OperationRecord>> {
        let state = self.lock()?;
        Ok(state
            .order
            .iter()
            .rev()
            .filter_map(|operation_id| state.records.get(operation_id).cloned())
            .collect())
    }

    pub fn backups(&self) -> Result<Vec<OperationRecord>> {
        Ok(self
            .list()?
            .into_iter()
            .filter(|record| record.kind == OperationKind::Backup)
            .collect())
    }

    pub fn get(&self, operation_id: Uuid) -> Result<OperationRecord> {
        self.lock()?
            .records
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| DbError::new("42704", "administration operation does not exist"))
    }

    pub fn cancel(&self, operation_id: Uuid) -> Result<OperationRecord> {
        let state = self.lock()?;
        let record = state
            .records
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| DbError::new("42704", "administration operation does not exist"))?;
        if record.state.active()
            && let Some(cancellation) = state.cancellations.get(&operation_id)
        {
            cancellation.store(true, Ordering::Release);
        }
        Ok(record)
    }

    fn resolve_operation(
        &self,
        operation: &StartOperation,
    ) -> Result<(PathBuf, Option<String>, Option<String>)> {
        match operation {
            StartOperation::Backup { path } => {
                resolve_operation_path(&self.operations_root, path, true)
                    .map(|path| (path, None, None))
            }
            StartOperation::Restore { path } => {
                resolve_operation_path(&self.operations_root, path, false)
                    .map(|path| (path, None, None))
            }
            StartOperation::Import { request } => {
                resolve_operation_path(&self.operations_root, &request.path, false).map(|path| {
                    (
                        path,
                        Some(request.schema.clone()),
                        Some(request.table.clone()),
                    )
                })
            }
            StartOperation::Export { request } => {
                resolve_operation_path(&self.operations_root, &request.path, true).map(|path| {
                    (
                        path,
                        Some(request.schema.clone()),
                        Some(request.table.clone()),
                    )
                })
            }
        }
    }

    fn execute(
        &self,
        operation: StartOperation,
        path: &Path,
        cancellation: &AtomicBool,
    ) -> Result<(u64, u64)> {
        match operation {
            StartOperation::Backup { .. } => {
                let summary = write_archive(&self.engine, path, ArchiveLimits::default())?;
                Ok((summary.row_count, summary.bytes))
            }
            StartOperation::Restore { .. } => {
                let summary =
                    restore_archive_into_engine(&self.engine, path, ArchiveLimits::default())?;
                Ok((summary.row_count, 0))
            }
            StartOperation::Import { mut request } => {
                request.path = path.to_path_buf();
                let summary = import_table(
                    &self.engine,
                    &self.operations_root,
                    &request,
                    TransferLimits::default(),
                    Some(cancellation),
                )?;
                Ok((summary.rows, summary.bytes))
            }
            StartOperation::Export { mut request } => {
                request.path = path.to_path_buf();
                let summary = export_table(
                    &self.engine,
                    &self.operations_root,
                    &request,
                    TransferLimits::default(),
                    Some(cancellation),
                )?;
                Ok((summary.rows, summary.bytes))
            }
        }
    }

    fn mark_running(&self, operation_id: Uuid) {
        if let Ok(mut state) = self.state.lock()
            && let Some(record) = state.records.get_mut(&operation_id)
        {
            record.state = OperationState::Running;
            record.started_at = Some(Utc::now());
        }
    }

    fn mark_finished(
        &self,
        operation_id: Uuid,
        outcome: Result<(u64, u64)>,
        cancellation: &AtomicBool,
    ) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(record) = state.records.get_mut(&operation_id) {
                match outcome {
                    Ok((rows, bytes)) => {
                        record.state = OperationState::Succeeded;
                        record.rows = Some(rows);
                        record.bytes = Some(bytes);
                    }
                    Err(error) => {
                        record.state =
                            if error.sql_state == "57014" || cancellation.load(Ordering::Acquire) {
                                OperationState::Cancelled
                            } else {
                                OperationState::Failed
                            };
                        record.error = Some(error);
                    }
                }
                record.finished_at = Some(Utc::now());
            }
            state.cancellations.remove(&operation_id);
            prune_history(&mut state);
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, OperationStateStore>> {
        self.state.lock().map_err(|_| {
            DbError::internal("administration operation state lock is poisoned")
                .with_hint("restart the service before retrying operation work")
        })
    }
}

fn display_path(root: &Path, path: &Path) -> PathBuf {
    let canonical_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    path.strip_prefix(canonical_root)
        .map(Path::to_path_buf)
        .unwrap_or_else(|_| PathBuf::from("<redacted>"))
}

fn prune_history(state: &mut OperationStateStore) {
    while state.records.len() >= MAX_OPERATION_HISTORY {
        let Some(operation_id) = state.order.front().copied() else {
            break;
        };
        let removable = state
            .records
            .get(&operation_id)
            .is_none_or(|record| !record.state.active());
        if !removable {
            break;
        }
        state.order.pop_front();
        state.records.remove(&operation_id);
        state.cancellations.remove(&operation_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordadb_engine::EngineConfig;
    use ordadb_types::Value;
    use tempfile::tempdir;
    use tokio::time::{Duration, sleep};

    async fn wait(manager: &OperationManager, operation_id: Uuid) -> OperationRecord {
        for _ in 0..100 {
            let record = manager.get(operation_id).expect("record");
            if !record.state.active() {
                return record;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!("operation did not finish");
    }

    #[tokio::test]
    async fn backup_restore_and_transfer_jobs_keep_bounded_sanitized_state() {
        let directory = tempdir().expect("tempdir");
        let engine = Arc::new(
            Engine::open(EngineConfig::new(directory.path().join("data"))).expect("engine"),
        );
        let mut session = engine.connect().expect("session");
        session
            .execute(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT)",
                &[],
            )
            .expect("create");
        session
            .execute(
                "INSERT INTO items (id, label) VALUES ($1, $2)",
                &[Value::Int64(1), Value::Text("alpha".into())],
            )
            .expect("insert");
        let manager =
            OperationManager::new(Arc::clone(&engine), directory.path().join("operations"));
        let backup = manager
            .start(StartOperation::Backup {
                path: "database.orda".into(),
            })
            .expect("start backup");
        let backup = wait(&manager, backup.operation_id).await;
        assert_eq!(backup.state, OperationState::Succeeded);
        assert_eq!(backup.path, PathBuf::from("database.orda"));
        assert_eq!(backup.rows, Some(1));

        session
            .execute("DELETE FROM items", &[])
            .expect("delete rows");
        let restore = manager
            .start(StartOperation::Restore {
                path: "database.orda".into(),
            })
            .expect("start restore");
        let restore = wait(&manager, restore.operation_id).await;
        assert_eq!(restore.state, OperationState::Succeeded);
        assert_eq!(engine.status_snapshot().expect("status").row_count, 1);
        assert_eq!(manager.backups().expect("backups").len(), 1);
    }
}
