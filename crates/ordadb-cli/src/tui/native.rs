use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use ordadb_ai::{AiToolLimits, MAX_QUERY_MEMORY_BYTES};
use ordadb_protocol::{ClientConfig, PgCancelToken, PgClient, PgQueryEvent};
use ordadb_sql::{StatementEffect, classify_statement_effect, parse};
use ordadb_types::{DbError, Result};
use ordadb_windows::CredentialVault;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::settings::NativeConnectionSettings;

const QUERY_BATCH_ROWS: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tags: Vec<String>,
    pub total_rows: usize,
    pub bytes_retained: usize,
    pub truncated: bool,
}

#[derive(Clone)]
pub struct NativeExecutor {
    settings: NativeConnectionSettings,
    credentials: CredentialVault,
}

impl NativeExecutor {
    pub fn new(settings: NativeConnectionSettings) -> Result<Self> {
        Ok(Self {
            settings,
            credentials: CredentialVault::new("OrdaDB/Console")?,
        })
    }

    pub fn connection_id(&self) -> String {
        format!(
            "ordadb-native://{}/{}?user={}",
            self.settings.address, self.settings.database, self.settings.user
        )
    }

    pub async fn execute(
        &self,
        sql: String,
        isolated_read: bool,
        limits: AiToolLimits,
        cancellation: CancellationToken,
    ) -> Result<NativeQueryResult> {
        if isolated_read {
            let statement = parse(&sql)?;
            if classify_statement_effect(&statement) != StatementEffect::ReadOnly {
                return Err(DbError::new(
                    "25006",
                    "automatic TUI execution requires a conservatively read-only statement",
                ));
            }
        }
        let address = self
            .settings
            .address
            .parse::<SocketAddr>()
            .map_err(|_| invalid("TUI native address must be an IP socket address"))?;
        let stored = self.credentials.load(&self.settings.credential_id)?;
        if !stored.username.eq_ignore_ascii_case(&self.settings.user) {
            return Err(DbError::new(
                "28000",
                "the saved native credential belongs to a different database user",
            )
            .with_hint("run /connect to replace the saved credential for this TUI profile"));
        }
        let config = ClientConfig {
            address,
            user: stored.username,
            database: self.settings.database.clone(),
            password: stored.password,
            application_name: "ordadb-tui".to_owned(),
            query_memory_bytes: Some(limits.query_memory_bytes.min(MAX_QUERY_MEMORY_BYTES)),
            timeout: Some(Duration::from_millis(limits.timeout_ms.min(30_000))),
        };
        let cancel_slot = Arc::new(Mutex::new(None::<PgCancelToken>));
        let worker_slot = Arc::clone(&cancel_slot);
        let worker_cancellation = cancellation.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            execute_blocking(
                config,
                sql,
                isolated_read,
                limits,
                worker_cancellation,
                worker_slot,
            )
        });

        tokio::select! {
            joined = &mut worker => join_result(joined),
            () = cancellation.cancelled() => {
                cancel_when_available(&cancel_slot, &worker).await;
                let _ = worker.await;
                Err(cancelled())
            }
        }
    }
}

fn execute_blocking(
    config: ClientConfig,
    sql: String,
    isolated_read: bool,
    limits: AiToolLimits,
    cancellation: CancellationToken,
    cancel_slot: Arc<Mutex<Option<PgCancelToken>>>,
) -> Result<NativeQueryResult> {
    let mut client = PgClient::connect(config)?;
    *mutex_lock(&cancel_slot, "TUI PostgreSQL cancellation token")? =
        Some(client.cancellation_token());
    if cancellation.is_cancelled() {
        return Err(cancelled());
    }
    if isolated_read {
        client.query("BEGIN TRANSACTION READ ONLY")?;
    }
    let mut result = NativeQueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tags: Vec::new(),
        total_rows: 0,
        bytes_retained: 0,
        truncated: false,
    };
    let query = client.query_batches(&sql, QUERY_BATCH_ROWS, |event| {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        match event {
            PgQueryEvent::Schema(columns) => {
                result.bytes_retained = columns.iter().map(String::len).sum();
                result.columns = columns;
            }
            PgQueryEvent::Batch(rows) => {
                for row in rows {
                    result.total_rows = result.total_rows.saturating_add(1);
                    let row_bytes = row
                        .iter()
                        .map(|value| value.as_ref().map_or(1, String::len).saturating_add(1))
                        .sum::<usize>();
                    if result.rows.len() < limits.max_rows
                        && result.bytes_retained.saturating_add(row_bytes)
                            <= limits.max_result_bytes
                    {
                        result.bytes_retained = result.bytes_retained.saturating_add(row_bytes);
                        result.rows.push(row);
                    } else {
                        result.truncated = true;
                    }
                }
            }
            PgQueryEvent::Complete(tag) => result.command_tags.push(tag),
            PgQueryEvent::Notice(_) | PgQueryEvent::Notification(_) => {}
        }
        Ok(())
    });
    let rollback = if isolated_read {
        client.query("ROLLBACK").map(|_| ())
    } else {
        Ok(())
    };
    query?;
    rollback?;
    result.truncated |= result.total_rows > result.rows.len();
    Ok(result)
}

async fn cancel_when_available(
    slot: &Arc<Mutex<Option<PgCancelToken>>>,
    worker: &tokio::task::JoinHandle<Result<NativeQueryResult>>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let token = slot.lock().ok().and_then(|guard| guard.clone());
        if let Some(token) = token {
            let _ = tokio::task::spawn_blocking(move || token.cancel()).await;
            return;
        }
        if worker.is_finished() || tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn join_result(
    joined: std::result::Result<Result<NativeQueryResult>, tokio::task::JoinError>,
) -> Result<NativeQueryResult> {
    joined.map_err(|error| {
        DbError::new("XX000", "TUI PostgreSQL worker failed").with_detail(error.to_string())
    })?
}

fn mutex_lock<'a, T>(mutex: &'a Mutex<T>, label: &str) -> Result<MutexGuard<'a, T>> {
    mutex
        .lock()
        .map_err(|_| DbError::new("XX000", format!("{label} is poisoned")))
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn cancelled() -> DbError {
    DbError::new("57014", "TUI database operation was cancelled")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_identity_contains_no_password_material() {
        let executor = NativeExecutor::new(NativeConnectionSettings {
            address: "127.0.0.1:54329".to_owned(),
            user: "dba".to_owned(),
            database: "ordadb".to_owned(),
            credential_id: "ordadb-local".to_owned(),
        })
        .expect("executor");
        let identity = executor.connection_id();
        assert_eq!(identity, "ordadb-native://127.0.0.1:54329/ordadb?user=dba");
        assert!(!identity.contains("password"));
    }
}
