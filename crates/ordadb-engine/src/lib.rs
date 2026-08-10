//! SQL execution, transaction coordination, and durable publication for OrdaDB.
//!
//! This crate owns SQL semantics and candidate-state atomicity. Physical page
//! encoding belongs to `ordadb-storage`; WAL and crash recovery belong to
//! `ordadb-transaction`.

mod system_catalog;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

use ordadb_catalog::{
    Catalog, CatalogExpression, CatalogObjectRef, CatalogOwner, ColumnDefinition, ColumnStatistics,
    ConstraintKind, DomainBaseType, DropBehavior, IndexMethod, NewColumn, NewRoutine, NewView,
    PostgresOidObject, ReferentialAction, RoutineDefinition, SequenceAlteration, TableDefinition,
    TableStatistics, TriggerDefinition, TriggerEvent, TriggerLevel, TriggerTarget, TriggerTiming,
    UserDefinedTypeKind, ViewKind, indexable_type,
};
use ordadb_execution::{
    AdvancedExecutionCursor, AdvancedExecutionPlan, ApplyExecutionKind, ApplyExecutionPlan,
    DEFAULT_BATCH_ROWS, DEFAULT_HARD_MEMORY_BYTES, DEFAULT_SOFT_MEMORY_BYTES, ExecutionContext,
    ExecutionCursor, ExecutionOptions, JoinExecutionPlan, JoinExecutionSource, LeasedDataChunk,
    MemoryGrant, QueryExecutionPlan, Reservation, TableProvider, TableScan,
    coerce_value as coerce_execution_value, compare_values as compare_execution_values,
    estimated_row_bytes, evaluate as evaluate_scalar,
    predicate_matches as execution_predicate_matches,
};
use ordadb_index::{BPlusTree, IndexEntry, IndexKey, RowId};
use ordadb_optimizer::{
    JoinStrategy, choose_join_strategy, explain as explain_plan, optimize_select,
};
use ordadb_plpgsql::{
    PlpgsqlHost, VmMachine, VmMemoryGrant, VmMemoryHold, VmMemoryReservation, VmOutput, VmRunState,
    VmSqlStream, compile_with_arguments as compile_plpgsql,
    execute_with_memory_grant as execute_plpgsql_with_memory,
};
pub use ordadb_search::{
    AllowedRows, HybridSearchRequest, SearchRowId, TextSearchRequest, VectorSearchRequest,
};
use ordadb_search::{SearchCatalog, SearchLimits};
use ordadb_sql::{
    BinaryOperator, BoundAlterDomainOperation, BoundAlterTableOperation, BoundApply,
    BoundApplyKind, BoundConflictAction, BoundCte, BoundExpr, BoundExprKind, BoundJoin,
    BoundJoinSource, BoundMerge, BoundMergeAction, BoundMergeClauseKind, BoundOnConflict,
    BoundOrder, BoundProjection, BoundReindexTarget, BoundReturning, BoundSequenceOperation,
    BoundStatement, BoundTable, BoundWindow, BoundWindowFrameBound, DdlObjectKind, JoinKind,
    ParsedStatement, QuerySetOperator, SessionBindValues, SqlDialect, SubqueryQuantifier,
    TransactionChain, bind, bind_catalog_expression_with_catalog,
    bind_catalog_expression_with_parameter_types_and_catalog, bind_with_session, parse,
    parse_with_dialect,
};
use ordadb_storage::{
    ApplyPoint, DataFormat, DatabaseStore, DurabilityBarrier, FROZEN_TRANSACTION_ID,
    PersistentState, StorageTableCursorV2, TupleHeaderV2, VersionedRow, encode_row,
};
use ordadb_transaction::{
    CheckpointState, DurableTransaction, FaultInjector, FaultPoint, IsolationLevel, LockGuard,
    LockKey, LockManager, LockManagerOptions, LockMode, LockSnapshot, LockWaitSnapshot,
    NoFaultInjector, PredicateLock, SavepointId, SavepointStack, SsiManager, SsiManagerOptions,
    SsiSavepoint, TransactionAccessMode, TransactionCharacteristics, TransactionId,
    TransactionManager, TransactionOutcome, TransactionSnapshot, TransactionStatusProvider,
    TransactionStatusStore, WalManager, WriterCoordinator, WriterLease, tuple_visible,
};
use ordadb_types::{
    Batch, CommandComplete, DbError, DbNotice, Field, Identifier, IndexId, PgArray, QueryEvent,
    QueryProgress, Result, Row, ScalarType, Schema, SequenceId, TableId, TypeId, Value, ViewId,
};

const MAX_RECURSIVE_CTE_ITERATIONS: usize = 10_000;
const MAX_RECURSIVE_CTE_ROWS: usize = 1_000_000;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const AUTOMATIC_CHECKPOINT_INTERVAL: u64 = 64;
const MAX_REFERENTIAL_ACTIONS: usize = 16_384;
const MAX_DML_LOCK_RECHECKS: usize = 32;
const MAX_SYSTEM_CATALOG_ROLES: usize = 10_000;
const MAX_SYSTEM_CATALOG_SETTINGS: usize = 256;
const MAX_SYSTEM_CATALOG_TEXT_BYTES: usize = 1_024;
const MAX_SESSION_RUNTIME_TEXT_BYTES: usize = 1_024;
const MAX_SESSION_RUNTIME_SETTINGS: usize = 256;
const MAX_ROUTINE_FRAMES: usize = 64;
// Trigger-issued DML still re-enters the statement executor synchronously. Keep
// this guard below the verified 128 KiB Windows release-stack boundary until
// trigger statement continuations are fully heap-resident alongside routine
// VM frames. Exceeding it is a structured implementation limit, never a stack
// overflow.
const MAX_TRIGGER_FRAMES: usize = 1;
const MAX_PLPGSQL_NOTICES: usize = 1_024;
const MAX_PLPGSQL_NOTICE_BYTES: usize = 64 * 1024;
pub const LOGICAL_SNAPSHOT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub cluster_root: PathBuf,
    pub minimum_next_transaction_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionOptions {
    pub dialect: SqlDialect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRuntimeMetadata {
    version: String,
    current_database: String,
    current_user: String,
    session_user: String,
    settings: BTreeMap<String, String>,
}

impl SessionRuntimeMetadata {
    pub fn postgres_compatible(
        server_version: &str,
        current_database: impl Into<String>,
        current_user: impl Into<String>,
        session_user: impl Into<String>,
    ) -> Result<Self> {
        let session_user = session_user.into();
        Self::new(
            format!("PostgreSQL {server_version} compatible OrdaDB on x86_64-pc-windows-msvc"),
            current_database,
            current_user,
            session_user.clone(),
        )
        .and_then(|metadata| {
            metadata.with_settings([
                ("server_version", server_version),
                ("server_encoding", "UTF8"),
                ("client_encoding", "UTF8"),
                ("datestyle", "ISO, YMD"),
                ("timezone", "UTC"),
                ("integer_datetimes", "on"),
                ("standard_conforming_strings", "on"),
                ("default_transaction_isolation", "read committed"),
                ("transaction_isolation", "read committed"),
                ("default_transaction_read_only", "off"),
                ("session_authorization", session_user.as_str()),
                ("application_name", ""),
                ("extra_float_digits", "1"),
            ])
        })
    }

    pub fn new(
        version: impl Into<String>,
        current_database: impl Into<String>,
        current_user: impl Into<String>,
        session_user: impl Into<String>,
    ) -> Result<Self> {
        let metadata = Self {
            version: version.into(),
            current_database: current_database.into(),
            current_user: current_user.into(),
            session_user: session_user.into(),
            settings: BTreeMap::new(),
        };
        for (value, field) in [
            (metadata.version.as_str(), "version"),
            (metadata.current_database.as_str(), "current database"),
            (metadata.current_user.as_str(), "current user"),
            (metadata.session_user.as_str(), "session user"),
        ] {
            if value.is_empty()
                || value.len() > MAX_SESSION_RUNTIME_TEXT_BYTES
                || value.as_bytes().contains(&0)
            {
                return Err(DbError::new(
                    "22023",
                    format!(
                        "{field} must contain between 1 and {MAX_SESSION_RUNTIME_TEXT_BYTES} bytes without NUL"
                    ),
                ));
            }
        }
        Ok(metadata)
    }

    pub fn with_settings<K, V>(mut self, settings: impl IntoIterator<Item = (K, V)>) -> Result<Self>
    where
        K: Into<String>,
        V: Into<String>,
    {
        let mut values = BTreeMap::new();
        for (name, value) in settings {
            if values.len() >= MAX_SESSION_RUNTIME_SETTINGS {
                return Err(DbError::new(
                    "54000",
                    "session setting snapshot exceeds the 256-entry limit",
                ));
            }
            let name = name.into().trim().to_ascii_lowercase();
            let value = value.into();
            if name.is_empty()
                || name.len() > MAX_SESSION_RUNTIME_TEXT_BYTES
                || name.as_bytes().contains(&0)
                || value.len() > MAX_SESSION_RUNTIME_TEXT_BYTES
                || value.as_bytes().contains(&0)
            {
                return Err(DbError::new(
                    "22023",
                    "session setting names must be non-empty and setting names and values must fit the 1024-byte limit without NUL",
                ));
            }
            if values.insert(name.clone(), value).is_some() {
                return Err(DbError::new(
                    "42710",
                    format!("session setting {name} is defined more than once"),
                ));
            }
        }
        self.settings = values;
        Ok(self)
    }

    fn bind_values(&self) -> SessionBindValues<'_> {
        SessionBindValues {
            version: &self.version,
            current_database: &self.current_database,
            current_user: &self.current_user,
            session_user: &self.session_user,
            settings: &self.settings,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionAuthorization {
    owner: CatalogOwner,
    bypass_ownership: bool,
    catalog_visibility: CatalogVisibility,
    catalog_roles: Vec<CatalogRoleMetadata>,
    catalog_settings: Vec<CatalogSettingMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogVisibilityScope {
    All,
    Schema { schema: String },
    Object { schema: String, name: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogVisibility {
    allow_all: bool,
    schemas: BTreeSet<String>,
    objects: BTreeMap<String, BTreeSet<String>>,
}

impl CatalogVisibility {
    pub fn from_scopes(scopes: impl IntoIterator<Item = CatalogVisibilityScope>) -> Result<Self> {
        let mut visibility = Self::default();
        for scope in scopes {
            match scope {
                CatalogVisibilityScope::All => visibility.allow_all = true,
                CatalogVisibilityScope::Schema { schema } => {
                    validate_system_catalog_text(&schema, "catalog visibility schema")?;
                    visibility.schemas.insert(schema);
                }
                CatalogVisibilityScope::Object { schema, name } => {
                    validate_system_catalog_text(&schema, "catalog visibility schema")?;
                    validate_system_catalog_text(&name, "catalog visibility object")?;
                    visibility.objects.entry(schema).or_default().insert(name);
                }
            }
        }
        Ok(visibility)
    }

    fn allows(&self, schema: &str, object: Option<&str>) -> bool {
        self.allow_all
            || self.schemas.contains(schema)
            || object.is_some_and(|object| {
                self.objects
                    .get(schema)
                    .is_some_and(|objects| objects.contains(object))
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRoleMetadata {
    pub postgres_oid: u32,
    pub name: String,
    pub can_login: bool,
    pub login_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSettingMetadata {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
    pub category: String,
    pub short_description: String,
    pub context: String,
    pub value_type: String,
    pub source: String,
    pub minimum: Option<String>,
    pub maximum: Option<String>,
    pub enum_values: Option<String>,
    pub boot_value: String,
    pub reset_value: String,
}

impl SessionAuthorization {
    pub fn new(owner: impl Into<String>, bypass_ownership: bool) -> Result<Self> {
        Ok(Self {
            owner: CatalogOwner::new(owner)?,
            bypass_ownership,
            catalog_visibility: CatalogVisibility::default(),
            catalog_roles: Vec::new(),
            catalog_settings: Vec::new(),
        })
    }

    #[must_use]
    pub fn with_catalog_visibility(mut self, visibility: CatalogVisibility) -> Self {
        self.catalog_visibility = visibility;
        self
    }

    pub fn with_system_catalog_metadata(
        mut self,
        roles: Vec<CatalogRoleMetadata>,
        settings: Vec<CatalogSettingMetadata>,
    ) -> Result<Self> {
        self.replace_system_catalog_metadata(roles, settings)?;
        Ok(self)
    }

    #[must_use]
    pub const fn owner(&self) -> &CatalogOwner {
        &self.owner
    }

    #[must_use]
    pub const fn bypasses_ownership(&self) -> bool {
        self.bypass_ownership
    }

    fn replace_system_catalog_metadata(
        &mut self,
        roles: Vec<CatalogRoleMetadata>,
        settings: Vec<CatalogSettingMetadata>,
    ) -> Result<()> {
        validate_system_catalog_metadata(&roles, &settings)?;
        self.catalog_roles = roles;
        self.catalog_settings = settings;
        Ok(())
    }

    pub(crate) fn catalog_roles(&self) -> &[CatalogRoleMetadata] {
        &self.catalog_roles
    }

    pub(crate) fn catalog_settings(&self) -> &[CatalogSettingMetadata] {
        &self.catalog_settings
    }

    pub(crate) fn can_discover(&self, schema: &str, object: Option<&str>) -> bool {
        self.catalog_visibility.allows(schema, object)
    }
}

fn validate_system_catalog_metadata(
    roles: &[CatalogRoleMetadata],
    settings: &[CatalogSettingMetadata],
) -> Result<()> {
    if roles.len() > MAX_SYSTEM_CATALOG_ROLES {
        return Err(DbError::new(
            "54000",
            "system catalog role snapshot exceeds the bounded role limit",
        ));
    }
    if settings.len() > MAX_SYSTEM_CATALOG_SETTINGS {
        return Err(DbError::new(
            "54000",
            "system catalog settings snapshot exceeds the bounded setting limit",
        ));
    }
    let mut role_oids = BTreeSet::new();
    let mut role_names = BTreeSet::new();
    for role in roles {
        validate_system_catalog_text(&role.name, "role name")?;
        if role.postgres_oid == 0
            || !role_oids.insert(role.postgres_oid)
            || !role_names.insert(role.name.as_str())
        {
            return Err(DbError::new(
                "XX001",
                "system catalog role snapshot contains a duplicate or invalid identity",
            ));
        }
    }
    let mut setting_names = BTreeSet::new();
    for setting in settings {
        for (value, field) in [
            (setting.name.as_str(), "setting name"),
            (setting.category.as_str(), "setting category"),
            (setting.short_description.as_str(), "setting description"),
            (setting.context.as_str(), "setting context"),
            (setting.value_type.as_str(), "setting type"),
            (setting.source.as_str(), "setting source"),
        ] {
            validate_system_catalog_text(value, field)?;
        }
        for (value, field) in [
            (setting.setting.as_str(), "setting value"),
            (setting.boot_value.as_str(), "setting boot value"),
            (setting.reset_value.as_str(), "setting reset value"),
        ] {
            validate_system_catalog_bounded_text(value, field)?;
        }
        for (value, field) in [
            (setting.unit.as_deref(), "setting unit"),
            (setting.minimum.as_deref(), "setting minimum"),
            (setting.maximum.as_deref(), "setting maximum"),
            (setting.enum_values.as_deref(), "setting enum values"),
        ] {
            if let Some(value) = value {
                validate_system_catalog_bounded_text(value, field)?;
            }
        }
        if !setting_names.insert(setting.name.to_ascii_lowercase()) {
            return Err(DbError::new(
                "XX001",
                "system catalog settings snapshot contains a duplicate name",
            ));
        }
    }
    Ok(())
}

fn validate_system_catalog_text(value: &str, field: &str) -> Result<()> {
    if value.is_empty() {
        return Err(DbError::new(
            "22023",
            format!("system catalog {field} is empty, oversized, or contains NUL"),
        ));
    }
    validate_system_catalog_bounded_text(value, field)
}

fn validate_system_catalog_bounded_text(value: &str, field: &str) -> Result<()> {
    if value.len() > MAX_SYSTEM_CATALOG_TEXT_BYTES || value.as_bytes().contains(&0) {
        return Err(DbError::new(
            "22023",
            format!("system catalog {field} is oversized or contains NUL"),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatementDescription {
    pub schema: Schema,
    pub parameter_types: Vec<ScalarType>,
}

#[derive(Debug, Clone)]
pub enum SearchRequest {
    Text(TextSearchRequest),
    Vector(VectorSearchRequest),
    Hybrid(HybridSearchRequest),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScalarSearchFilter {
    pub expression: String,
    pub parameters: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub row_id: SearchRowId,
    pub row: Row,
    pub text_score: Option<f32>,
    pub vector_score: Option<f32>,
    pub distance: Option<f32>,
    pub combined_score: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineStatusSnapshot {
    pub data_format: DataFormat,
    pub generation: u64,
    pub table_count: usize,
    pub row_count: u64,
    pub index_count: usize,
    pub durable_lsn: Option<u64>,
    pub dirty_page_count: usize,
    pub commits_since_checkpoint: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogicalDatabaseSnapshot {
    pub format_version: u16,
    pub source_generation: u64,
    pub catalog: Arc<Catalog>,
    pub tables: BTreeMap<TableId, Arc<Vec<Row>>>,
}

impl EngineConfig {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        Self {
            cluster_root: data_dir.clone(),
            data_dir,
            minimum_next_transaction_id: None,
        }
    }

    #[must_use]
    pub fn for_cluster(
        data_dir: impl Into<PathBuf>,
        cluster_root: impl Into<PathBuf>,
        minimum_next_transaction_id: u64,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            cluster_root: cluster_root.into(),
            minimum_next_transaction_id: Some(minimum_next_transaction_id),
        }
    }
}

#[derive(Debug, Default)]
struct StorageAccessGate {
    state: Mutex<StorageAccessState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct StorageAccessState {
    active_readers: usize,
    waiting_writers: usize,
    writer_active: bool,
}

impl StorageAccessGate {
    fn acquire_read(self: &Arc<Self>) -> Result<StorageReadLease> {
        let mut state = self.lock_state()?;
        while state.writer_active || state.waiting_writers > 0 {
            state = self
                .changed
                .wait(state)
                .map_err(|_| internal_error("storage access gate is poisoned"))?;
        }
        state.active_readers = state.active_readers.checked_add(1).ok_or_else(|| {
            DbError::new("54000", "storage reader lease count overflowed")
                .with_hint("restart the database service before retrying")
        })?;
        drop(state);
        Ok(StorageReadLease {
            gate: Arc::clone(self),
        })
    }

    fn acquire_write(self: &Arc<Self>) -> Result<StorageWriteLease> {
        let mut state = self.lock_state()?;
        state.waiting_writers = state.waiting_writers.checked_add(1).ok_or_else(|| {
            DbError::new("54000", "storage writer wait count overflowed")
                .with_hint("restart the database service before retrying")
        })?;
        while state.writer_active || state.active_readers > 0 {
            state = self
                .changed
                .wait(state)
                .map_err(|_| internal_error("storage access gate is poisoned"))?;
        }
        state.waiting_writers = state.waiting_writers.saturating_sub(1);
        state.writer_active = true;
        drop(state);
        Ok(StorageWriteLease {
            gate: Arc::clone(self),
        })
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, StorageAccessState>> {
        self.state
            .lock()
            .map_err(|_| internal_error("storage access gate is poisoned"))
    }

    #[cfg(test)]
    fn active_readers(&self) -> Result<usize> {
        Ok(self.lock_state()?.active_readers)
    }

    #[cfg(test)]
    fn waiting_writers(&self) -> Result<usize> {
        Ok(self.lock_state()?.waiting_writers)
    }
}

#[derive(Debug)]
struct StorageReadLease {
    gate: Arc<StorageAccessGate>,
}

impl Drop for StorageReadLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.active_readers = state.active_readers.saturating_sub(1);
            self.gate.changed.notify_all();
        }
    }
}

#[derive(Debug)]
struct StorageWriteLease {
    gate: Arc<StorageAccessGate>,
}

impl Drop for StorageWriteLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.writer_active = false;
            self.gate.changed.notify_all();
        }
    }
}

const MAX_NOTIFICATION_QUEUE_ENTRIES: usize = 1_024;
const MAX_NOTIFICATION_QUEUE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseNotification {
    pub sender_process_id: u32,
    pub channel: String,
    pub payload: String,
}

#[derive(Debug, Clone)]
enum NotificationListenerAction {
    Listen(Identifier),
    Unlisten(Identifier),
    UnlistenAll,
}

#[derive(Debug, Clone, Default)]
struct NotificationTransactionState {
    listener_actions: Vec<NotificationListenerAction>,
    notifications: Vec<(Identifier, String)>,
    coalesced: BTreeSet<(Identifier, String)>,
}

impl NotificationTransactionState {
    fn listen(&mut self, channel: Identifier) {
        self.listener_actions
            .push(NotificationListenerAction::Listen(channel));
    }

    fn unlisten(&mut self, channel: Option<Identifier>) {
        self.listener_actions.push(channel.map_or(
            NotificationListenerAction::UnlistenAll,
            NotificationListenerAction::Unlisten,
        ));
    }

    fn notify(&mut self, channel: Identifier, payload: String) {
        if self.coalesced.insert((channel.clone(), payload.clone())) {
            self.notifications.push((channel, payload));
        }
    }

    fn append(&mut self, pending: Self) {
        self.listener_actions.extend(pending.listener_actions);
        for (channel, payload) in pending.notifications {
            self.notify(channel, payload);
        }
    }
}

#[derive(Debug)]
struct NotificationSessionQueue {
    process_id: u32,
    channels: BTreeSet<Identifier>,
    queue: VecDeque<DatabaseNotification>,
    queued_bytes: usize,
    overflowed: bool,
}

#[derive(Debug, Default)]
struct NotificationBrokerState {
    next_session_id: u64,
    sessions: BTreeMap<u64, NotificationSessionQueue>,
}

#[derive(Debug, Default)]
struct NotificationBroker {
    state: Mutex<NotificationBrokerState>,
}

impl NotificationBroker {
    fn register(&self) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.next_session_id = state.next_session_id.saturating_add(1).max(1);
        let session_id = state.next_session_id;
        let process_id = u32::try_from(session_id).unwrap_or(u32::MAX).max(1);
        state.sessions.insert(
            session_id,
            NotificationSessionQueue {
                process_id,
                channels: BTreeSet::new(),
                queue: VecDeque::new(),
                queued_bytes: 0,
                overflowed: false,
            },
        );
        session_id
    }

    fn set_process_id(&self, session_id: u64, process_id: u32) -> Result<()> {
        if process_id == 0 {
            return Err(DbError::new("22023", "backend process ID must be non-zero"));
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let session = state
            .sessions
            .get_mut(&session_id)
            .ok_or_else(|| DbError::new("08003", "notification session is not registered"))?;
        session.process_id = process_id;
        Ok(())
    }

    fn unregister(&self, session_id: u64) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sessions
            .remove(&session_id);
    }

    fn commit(&self, session_id: u64, pending: NotificationTransactionState) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let sender_process_id = state
            .sessions
            .get(&session_id)
            .map_or(0, |session| session.process_id);
        if let Some(session) = state.sessions.get_mut(&session_id) {
            for action in pending.listener_actions {
                match action {
                    NotificationListenerAction::Listen(channel) => {
                        session.channels.insert(channel);
                    }
                    NotificationListenerAction::Unlisten(channel) => {
                        session.channels.remove(&channel);
                    }
                    NotificationListenerAction::UnlistenAll => session.channels.clear(),
                }
            }
        }
        for (channel, payload) in pending.notifications {
            let notification_bytes = channel
                .as_str()
                .len()
                .saturating_add(payload.len())
                .saturating_add(std::mem::size_of::<DatabaseNotification>());
            for session in state.sessions.values_mut() {
                if !session.channels.contains(&channel) || session.overflowed {
                    continue;
                }
                if session.queue.len() >= MAX_NOTIFICATION_QUEUE_ENTRIES
                    || session.queued_bytes.saturating_add(notification_bytes)
                        > MAX_NOTIFICATION_QUEUE_BYTES
                {
                    session.overflowed = true;
                    continue;
                }
                session.queue.push_back(DatabaseNotification {
                    sender_process_id,
                    channel: channel.as_str().to_owned(),
                    payload: payload.clone(),
                });
                session.queued_bytes = session.queued_bytes.saturating_add(notification_bytes);
            }
        }
    }

    fn drain(&self, session_id: u64) -> Result<Vec<DatabaseNotification>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let session = state
            .sessions
            .get_mut(&session_id)
            .ok_or_else(|| DbError::new("08003", "notification session is not registered"))?;
        if session.overflowed {
            session.queue.clear();
            session.queued_bytes = 0;
            return Err(DbError::new(
                "54000",
                "asynchronous notification queue limit exceeded",
            )
            .with_detail(format!(
                "the session queue is limited to {MAX_NOTIFICATION_QUEUE_ENTRIES} messages and {MAX_NOTIFICATION_QUEUE_BYTES} bytes"
            ))
            .with_hint("consume notifications promptly and reconnect this session"));
        }
        session.queued_bytes = 0;
        Ok(session.queue.drain(..).collect())
    }
}

#[derive(Debug, Clone)]
pub struct Engine {
    config: Arc<EngineConfig>,
    state: Arc<RwLock<DatabaseState>>,
    store: Arc<Mutex<DatabaseStore>>,
    storage_access: Arc<StorageAccessGate>,
    wal: Arc<WalManager>,
    transaction_status: Arc<TransactionStatusStore>,
    transactions: Arc<TransactionManager>,
    locks: Arc<LockManager>,
    ssi: Arc<SsiManager>,
    writer: Arc<WriterCoordinator>,
    commits_since_checkpoint: Arc<AtomicU64>,
    notifications: Arc<NotificationBroker>,
}

impl Engine {
    pub fn open(config: EngineConfig) -> Result<Self> {
        Self::open_with_fault_injector(config, Arc::new(NoFaultInjector))
    }

    #[doc(hidden)]
    pub fn open_with_fault_injector(
        config: EngineConfig,
        fault_injector: Arc<dyn FaultInjector>,
    ) -> Result<Self> {
        if DatabaseStore::detect_format_read_only(&config.data_dir)? == Some(DataFormat::V1) {
            return Err(DbError::new(
                "0A000",
                "legacy database format v1 cannot be opened for normal execution",
            )
            .with_hint("run the offline storage migration before starting the database service"));
        }
        let wal = WalManager::open_with_fault_injector(&config.data_dir, fault_injector)?;
        wal.recover(&config.data_dir)?;
        let wal_next_transaction_id = wal
            .last_transaction_id()?
            .map(|transaction_id| {
                transaction_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| DbError::new("54000", "transaction ID space is exhausted"))
            })
            .transpose()?
            .unwrap_or(1);
        let next_transaction_id = config
            .minimum_next_transaction_id
            .unwrap_or(1)
            .max(wal_next_transaction_id);
        let transaction_status = Arc::new(TransactionStatusStore::open(
            &config.data_dir,
            next_transaction_id,
        )?);
        transaction_status.reconcile_with_wal(&wal.transaction_outcomes()?)?;
        let transaction_status_snapshot = transaction_status.snapshot()?;
        let transactions = TransactionManager::from_status_snapshot(
            transaction_status_snapshot.next_transaction_id,
            transaction_status_snapshot.statuses,
        )?;
        let writer = WriterCoordinator::from_next_transaction_id(
            transaction_status_snapshot.next_transaction_id,
        )?;
        let locks = LockManager::new(LockManagerOptions::default())?;
        let ssi = SsiManager::new(SsiManagerOptions::default())?;
        let barrier: Arc<dyn DurabilityBarrier> = wal.clone();
        let store =
            DatabaseStore::open_with_barrier_and_format(&config.data_dir, barrier, DataFormat::V2)?;
        let state = DatabaseState::from_persistent(
            store.committed_state().clone(),
            store.data_format(),
            transaction_status.as_ref(),
            transaction_status_snapshot.next_transaction_id,
        )?;
        Ok(Self {
            config: Arc::new(config),
            state: Arc::new(RwLock::new(state)),
            store: Arc::new(Mutex::new(store)),
            storage_access: Arc::new(StorageAccessGate::default()),
            wal,
            transaction_status,
            transactions,
            locks,
            ssi,
            writer,
            commits_since_checkpoint: Arc::new(AtomicU64::new(0)),
            notifications: Arc::new(NotificationBroker::default()),
        })
    }

    pub fn connect(&self) -> Result<Session> {
        self.connect_with_options(SessionOptions::default())
    }

    pub fn connect_with_options(&self, options: SessionOptions) -> Result<Session> {
        self.connect_session(options, None)
    }

    pub fn connect_authenticated(&self, authorization: SessionAuthorization) -> Result<Session> {
        self.connect_authenticated_with_options(SessionOptions::default(), authorization)
    }

    pub fn connect_authenticated_with_options(
        &self,
        options: SessionOptions,
        authorization: SessionAuthorization,
    ) -> Result<Session> {
        self.connect_session(options, Some(authorization))
    }

    fn connect_session(
        &self,
        options: SessionOptions,
        authorization: Option<SessionAuthorization>,
    ) -> Result<Session> {
        let session_user = authorization
            .as_ref()
            .map_or("ordadb", |authorization| authorization.owner().as_str());
        let runtime_metadata = SessionRuntimeMetadata::postgres_compatible(
            "18",
            "ordadb",
            session_user,
            session_user,
        )?;
        let notification_session_id = self.notifications.register();
        Ok(Session {
            state: Arc::clone(&self.state),
            store: Arc::clone(&self.store),
            storage_access: Arc::clone(&self.storage_access),
            wal: Arc::clone(&self.wal),
            transaction_status: Arc::clone(&self.transaction_status),
            transactions: Arc::clone(&self.transactions),
            locks: Arc::clone(&self.locks),
            ssi: Arc::clone(&self.ssi),
            writer: Arc::clone(&self.writer),
            commits_since_checkpoint: Arc::clone(&self.commits_since_checkpoint),
            notifications: Arc::clone(&self.notifications),
            notification_session_id,
            sql_transaction: SqlTransactionState::Idle,
            sequence_currvals: BTreeMap::new(),
            options,
            execution_options: ExecutionOptions::default(),
            authorization,
            runtime_metadata,
        })
    }

    pub fn checkpoint(&self) -> Result<()> {
        checkpoint_shared(&self.state, &self.store, &self.wal, &self.transactions)?;
        self.commits_since_checkpoint.store(0, Ordering::Release);
        Ok(())
    }

    pub fn lock_snapshot(&self) -> Result<(Vec<LockSnapshot>, Vec<LockWaitSnapshot>)> {
        self.locks.snapshot()
    }

    pub fn set_maximum_snapshot_age(&self, maximum_age: Duration) -> Result<()> {
        self.transactions.set_maximum_snapshot_age(maximum_age)
    }

    pub fn set_default_lock_timeout(&self, timeout: Duration) -> Result<()> {
        self.locks.set_default_timeout(timeout)
    }

    #[must_use]
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn catalog_snapshot(&self) -> Result<Arc<Catalog>> {
        self.state
            .read()
            .map(|state| Arc::clone(&state.catalog))
            .map_err(|_| internal_error("engine state lock is poisoned"))
    }

    pub fn logical_snapshot(&self) -> Result<LogicalDatabaseSnapshot> {
        let state = self
            .state
            .read()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let tables = state
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .map(|table| {
                (
                    table.id,
                    state
                        .rows
                        .get(&table.id)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(Vec::new())),
                )
            })
            .collect();
        Ok(LogicalDatabaseSnapshot {
            format_version: LOGICAL_SNAPSHOT_VERSION,
            source_generation: state.generation,
            catalog: Arc::clone(&state.catalog),
            tables,
        })
    }

    pub fn replace_logical_snapshot(&self, snapshot: LogicalDatabaseSnapshot) -> Result<()> {
        let candidate = DatabaseState::from_logical_snapshot(snapshot)?;
        let mut transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            TransactionCharacteristics::default(),
        )?;
        let mut lease = self.writer.try_acquire(transaction.transaction_id())?;
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        persist_candidate(
            &mut state,
            &self.store,
            &self.storage_access,
            &self.wal,
            &mut transaction,
            candidate,
        )?;
        drop(state);
        lease.release();
        record_commit_and_maybe_checkpoint(
            &self.state,
            &self.store,
            &self.wal,
            &self.transactions,
            &self.commits_since_checkpoint,
        )
    }

    pub fn restore_logical_snapshot(
        config: EngineConfig,
        snapshot: LogicalDatabaseSnapshot,
    ) -> Result<Self> {
        if config.data_dir.exists() {
            let mut entries = std::fs::read_dir(&config.data_dir).map_err(|error| {
                DbError::new("58030", "failed to inspect logical restore target")
                    .with_detail(error.to_string())
            })?;
            if entries
                .next()
                .transpose()
                .map_err(|error| {
                    DbError::new("58030", "failed to inspect logical restore target")
                        .with_detail(error.to_string())
                })?
                .is_some()
            {
                return Err(DbError::new(
                    "55000",
                    "logical restore target must be absent or empty",
                )
                .with_hint("restore into a new candidate directory before replacing active data"));
            }
        }
        let engine = Self::open(config)?;
        engine.replace_logical_snapshot(snapshot)?;
        engine.checkpoint()?;
        Ok(engine)
    }

    pub fn status_snapshot(&self) -> Result<EngineStatusSnapshot> {
        let state = self
            .state
            .read()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let table_count = state
            .catalog
            .database()
            .schemas()
            .map(|schema| schema.tables().count())
            .sum();
        let row_count = state.rows.values().try_fold(0_u64, |total, rows| {
            let rows = u64::try_from(rows.len())
                .map_err(|_| internal_error("table row count does not fit in u64"))?;
            total
                .checked_add(rows)
                .ok_or_else(|| internal_error("database row count overflowed"))
        })?;
        let durable_lsn = self.wal.durable_lsn()?.map(|lsn| lsn.get());
        let dirty_page_count = self.wal.dirty_pages()?.len();
        let index_count = state
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .flat_map(|table| table.indexes())
            .count();
        Ok(EngineStatusSnapshot {
            data_format: DataFormat::V2,
            generation: state.generation,
            table_count,
            row_count,
            index_count,
            durable_lsn,
            dirty_page_count,
            commits_since_checkpoint: self.commits_since_checkpoint.load(Ordering::Acquire),
        })
    }
}

#[derive(Debug)]
pub struct Session {
    state: Arc<RwLock<DatabaseState>>,
    store: Arc<Mutex<DatabaseStore>>,
    storage_access: Arc<StorageAccessGate>,
    wal: Arc<WalManager>,
    transaction_status: Arc<TransactionStatusStore>,
    transactions: Arc<TransactionManager>,
    locks: Arc<LockManager>,
    ssi: Arc<SsiManager>,
    writer: Arc<WriterCoordinator>,
    commits_since_checkpoint: Arc<AtomicU64>,
    notifications: Arc<NotificationBroker>,
    notification_session_id: u64,
    sql_transaction: SqlTransactionState,
    sequence_currvals: BTreeMap<SequenceId, i64>,
    options: SessionOptions,
    execution_options: ExecutionOptions,
    authorization: Option<SessionAuthorization>,
    runtime_metadata: SessionRuntimeMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Idle,
    Active,
    Failed,
}

#[derive(Debug)]
enum SqlTransactionState {
    Idle,
    Active(Box<ActiveSqlTransaction>),
    Failed(TransactionCharacteristics),
}

#[derive(Debug)]
struct ActiveSqlTransaction {
    transaction: DurableTransaction,
    base: Option<DatabaseState>,
    working: Option<DatabaseState>,
    locks: Vec<LockGuard>,
    lease: Option<WriterLease>,
    dml_only: bool,
    ssi: Option<SsiTransactionGuard>,
    savepoints: SavepointStack,
    savepoint_states: BTreeMap<SavepointId, SqlSavepointState>,
    failed: bool,
    stream_failed: Arc<AtomicBool>,
    notification_state: NotificationTransactionState,
}

#[derive(Debug, Clone)]
struct SqlSavepointState {
    base: Option<DatabaseState>,
    working: Option<DatabaseState>,
    sequence_currvals: BTreeMap<SequenceId, i64>,
    lock_len: usize,
    dml_only: bool,
    ssi: Option<SsiSavepoint>,
    notification_state: NotificationTransactionState,
}

#[derive(Debug)]
struct SsiTransactionGuard {
    manager: Arc<SsiManager>,
    transaction_id: TransactionId,
    validated: bool,
    finished: bool,
}

impl SsiTransactionGuard {
    fn begin(manager: Arc<SsiManager>, transaction: &DurableTransaction) -> Result<Option<Self>> {
        let Some(characteristics) = transaction.characteristics() else {
            return Err(DbError::new("25P01", "transaction is no longer active"));
        };
        if characteristics.isolation_level != IsolationLevel::Serializable {
            return Ok(None);
        }
        let snapshot = transaction
            .snapshot()
            .ok_or_else(|| DbError::new("25P01", "transaction is no longer active"))?;
        manager.begin(
            transaction.transaction_id(),
            snapshot,
            characteristics.access_mode == TransactionAccessMode::ReadOnly,
        )?;
        Ok(Some(Self {
            manager,
            transaction_id: transaction.transaction_id(),
            validated: false,
            finished: false,
        }))
    }

    fn record_read(&self, predicate: PredicateLock) -> Result<()> {
        self.manager.record_read(self.transaction_id, predicate)
    }

    fn record_write(&self, predicate: PredicateLock) -> Result<()> {
        self.manager.record_write(self.transaction_id, predicate)
    }

    fn refresh_snapshot(&self, snapshot: &TransactionSnapshot) -> Result<()> {
        self.manager.refresh_snapshot(self.transaction_id, snapshot)
    }

    fn savepoint(&self) -> Result<SsiSavepoint> {
        self.manager.savepoint(self.transaction_id)
    }

    fn rollback_to(&self, savepoint: &SsiSavepoint) -> Result<()> {
        self.manager.rollback_to(self.transaction_id, savepoint)
    }

    fn validate_commit(&mut self, cleanup_horizon: TransactionId) -> Result<()> {
        if !self.validated {
            self.manager.commit(self.transaction_id)?;
            self.manager.cleanup_before(cleanup_horizon)?;
            self.validated = true;
        }
        Ok(())
    }

    fn finish(&mut self) {
        self.finished = true;
    }
}

impl Drop for SsiTransactionGuard {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.manager.abort(self.transaction_id);
        }
    }
}

struct ProcedureTransactionCoordinator {
    state: Arc<RwLock<DatabaseState>>,
    store: Arc<Mutex<DatabaseStore>>,
    storage_access: Arc<StorageAccessGate>,
    wal: Arc<WalManager>,
    transaction_status: Arc<TransactionStatusStore>,
    transactions: Arc<TransactionManager>,
    locks: Arc<LockManager>,
    writer: Arc<WriterCoordinator>,
    commits_since_checkpoint: Arc<AtomicU64>,
    notifications: Arc<NotificationBroker>,
    notification_session_id: u64,
    authorization: Option<SessionAuthorization>,
    cancellation: Option<Arc<AtomicBool>>,
    base: DatabaseState,
    transaction: Option<DurableTransaction>,
    locks_held: Vec<LockGuard>,
    lease: Option<WriterLease>,
    notices: Vec<DbNotice>,
    sequence_currvals: BTreeMap<SequenceId, i64>,
}

impl ProcedureTransactionCoordinator {
    fn new(session: &Session, base: DatabaseState) -> Result<Self> {
        let mut coordinator = Self {
            state: Arc::clone(&session.state),
            store: Arc::clone(&session.store),
            storage_access: Arc::clone(&session.storage_access),
            wal: Arc::clone(&session.wal),
            transaction_status: Arc::clone(&session.transaction_status),
            transactions: Arc::clone(&session.transactions),
            locks: Arc::clone(&session.locks),
            writer: Arc::clone(&session.writer),
            commits_since_checkpoint: Arc::clone(&session.commits_since_checkpoint),
            notifications: Arc::clone(&session.notifications),
            notification_session_id: session.notification_session_id,
            authorization: session.authorization.clone(),
            cancellation: base.cancellation.clone(),
            base,
            transaction: None,
            locks_held: Vec::new(),
            lease: None,
            notices: Vec::new(),
            sequence_currvals: session.sequence_currvals.clone(),
        };
        coordinator.start_segment(TransactionCharacteristics::default())?;
        let committed = committed_snapshot(&coordinator.state)?;
        coordinator.base = coordinator.prepare_candidate(committed);
        Ok(coordinator)
    }

    fn prepare_candidate(&self, mut candidate: DatabaseState) -> DatabaseState {
        candidate.triggers_fired = 0;
        candidate.routine_frames.clear();
        candidate.pending_notices.clear();
        candidate.pending_notifications = NotificationTransactionState::default();
        candidate.cancellation = self.cancellation.clone();
        candidate.authorization = self.authorization.clone();
        candidate.sequence_currvals = self.sequence_currvals.clone();
        candidate
    }

    fn start_segment(&mut self, characteristics: TransactionCharacteristics) -> Result<()> {
        let transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            characteristics,
        )?;
        let lease = self.writer.try_acquire(transaction.transaction_id())?;
        let lock = acquire_compatibility_write_lock(
            &self.locks,
            &transaction,
            self.cancellation.as_deref(),
        )?;
        self.transaction = Some(transaction);
        self.lease = Some(lease);
        self.locks_held.push(lock);
        Ok(())
    }

    fn boundary(
        &mut self,
        boundary: ProcedureBoundary,
        candidate: &mut DatabaseState,
        dirty: bool,
    ) -> Result<()> {
        let characteristics = self
            .transaction
            .as_ref()
            .and_then(DurableTransaction::characteristics)
            .ok_or_else(|| no_active_transaction_error("end a procedure transaction"))?;
        match boundary {
            ProcedureBoundary::Commit(_) => self.finish_segment(candidate, dirty, true)?,
            ProcedureBoundary::Rollback(_) => self.finish_segment(candidate, dirty, false)?,
        }
        let next_characteristics = match boundary {
            ProcedureBoundary::Commit(TransactionChain::Chain)
            | ProcedureBoundary::Rollback(TransactionChain::Chain) => characteristics,
            ProcedureBoundary::Commit(TransactionChain::NoChain)
            | ProcedureBoundary::Commit(TransactionChain::Default)
            | ProcedureBoundary::Rollback(TransactionChain::NoChain)
            | ProcedureBoundary::Rollback(TransactionChain::Default) => {
                TransactionCharacteristics::default()
            }
        };
        let runtime_frames = candidate.routine_frames.clone();
        let committed = committed_snapshot(&self.state)?;
        self.base = self.prepare_candidate(committed);
        *candidate = self.base.clone();
        candidate.routine_frames = runtime_frames;
        self.start_segment(next_characteristics)
    }

    fn finish_final(&mut self, candidate: &mut DatabaseState, dirty: bool) -> Result<()> {
        self.finish_segment(candidate, dirty, true)
    }

    fn abort(&mut self) {
        self.notices
            .append(&mut mem::take(&mut self.base.pending_notices));
        if let Some(transaction) = self.transaction.take() {
            let _ = transaction.abort();
        }
        self.locks_held.clear();
        self.lease.take();
    }

    fn runtime_sequence_currvals(&self) -> BTreeMap<SequenceId, i64> {
        self.sequence_currvals.clone()
    }

    fn finish_segment(
        &mut self,
        candidate: &mut DatabaseState,
        dirty: bool,
        commit: bool,
    ) -> Result<()> {
        self.notices
            .append(&mut mem::take(&mut candidate.pending_notices));
        let pending_notifications = mem::take(&mut candidate.pending_notifications);
        let mut transaction = self
            .transaction
            .take()
            .ok_or_else(|| no_active_transaction_error("end a procedure transaction"))?;
        if commit {
            if dirty {
                reconcile_version_changes(
                    &self.base,
                    candidate,
                    version_mutation_context(&transaction)?,
                )?;
                let mut durable_candidate = candidate.clone();
                durable_candidate.routine_frames.clear();
                durable_candidate.pending_notices.clear();
                durable_candidate.pending_notifications = NotificationTransactionState::default();
                durable_candidate.cancellation = None;
                durable_candidate.authorization = None;
                let mut state = self
                    .state
                    .write()
                    .map_err(|_| internal_error("engine state lock is poisoned"))?;
                persist_candidate(
                    &mut state,
                    &self.store,
                    &self.storage_access,
                    &self.wal,
                    &mut transaction,
                    durable_candidate,
                )?;
                drop(state);
            } else {
                transaction.commit_empty()?;
            }
            self.sequence_currvals = candidate.sequence_currvals.clone();
        } else {
            transaction.abort()?;
        }
        self.locks_held.clear();
        self.lease.take();
        if commit {
            self.notifications
                .commit(self.notification_session_id, pending_notifications);
            if dirty {
                record_commit_and_maybe_checkpoint(
                    &self.state,
                    &self.store,
                    &self.wal,
                    &self.transactions,
                    &self.commits_since_checkpoint,
                )?;
            }
        }
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.notifications.unregister(self.notification_session_id);
    }
}

impl Session {
    pub fn set_backend_process_id(&mut self, process_id: u32) -> Result<()> {
        self.notifications
            .set_process_id(self.notification_session_id, process_id)
    }

    pub fn drain_notifications(&mut self) -> Result<Vec<DatabaseNotification>> {
        self.notifications.drain(self.notification_session_id)
    }

    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        match self
            .execute_stream(sql, params)?
            .collect::<Result<Vec<_>>>()
        {
            Ok(events) => Ok(QueryStream::new(events)),
            Err(error) => {
                self.fail_sql_transaction();
                Err(error)
            }
        }
    }

    /// Bind a statement against the session's current catalog snapshot without
    /// executing it and return the row schema exposed to protocol clients.
    pub fn describe(&mut self, sql: &str) -> Result<Schema> {
        self.describe_statement(sql)
            .map(|description| description.schema)
    }

    /// Bind a statement without executing it and return both its result schema
    /// and the positional parameter types inferred by the binder.
    pub fn describe_statement(&mut self, sql: &str) -> Result<StatementDescription> {
        self.normalize_sql_transaction_failure();
        if self.transaction_status() == TransactionStatus::Failed {
            return Err(failed_transaction_error());
        }
        let snapshot = self.statement_snapshot()?;
        let described = parse_with_dialect(sql, self.options.dialect)
            .and_then(|statement| {
                bind_with_session(
                    statement,
                    &snapshot.catalog,
                    self.runtime_metadata.bind_values(),
                )
            })
            .and_then(|statement| {
                Ok(StatementDescription {
                    schema: bound_statement_schema(&statement),
                    parameter_types: bound_statement_parameter_types(&statement)?,
                })
            });
        if described.is_err() {
            self.fail_sql_transaction();
        }
        described
    }

    pub fn execute_stream(&mut self, sql: &str, params: &[Value]) -> Result<TryQueryStream> {
        self.execute_stream_controlled(sql, params, None)
    }

    pub fn execute_stream_with_cancellation(
        &mut self,
        sql: &str,
        params: &[Value],
        cancellation: Arc<AtomicBool>,
    ) -> Result<TryQueryStream> {
        self.execute_stream_controlled(sql, params, Some(cancellation))
    }

    pub fn set_runtime_metadata(&mut self, metadata: SessionRuntimeMetadata) {
        self.runtime_metadata = metadata;
    }

    pub fn refresh_system_catalog_metadata(
        &mut self,
        roles: Vec<CatalogRoleMetadata>,
        settings: Vec<CatalogSettingMetadata>,
        visibility: CatalogVisibility,
    ) -> Result<()> {
        let authorization = self.authorization.as_mut().ok_or_else(|| {
            DbError::new(
                "55000",
                "system catalog role metadata requires an authenticated session",
            )
        })?;
        authorization.replace_system_catalog_metadata(roles, settings)?;
        authorization.catalog_visibility = visibility;
        Ok(())
    }

    fn execute_stream_controlled(
        &mut self,
        sql: &str,
        params: &[Value],
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Result<TryQueryStream> {
        self.normalize_sql_transaction_failure();
        let transaction_was_failed = self.transaction_status() == TransactionStatus::Failed;
        let parsed = match parse_with_dialect(sql, self.options.dialect) {
            Ok(parsed) => parsed,
            Err(_) if transaction_was_failed => {
                return Err(failed_transaction_error());
            }
            Err(error) => {
                self.fail_sql_transaction();
                return Err(error);
            }
        };
        if transaction_was_failed {
            return match parsed {
                ParsedStatement::Rollback { chain } => self.rollback_sql_transaction(chain),
                ParsedStatement::RollbackTo { name } => self.rollback_to_sql_savepoint(&name.name),
                _ => Err(failed_transaction_error()),
            };
        }
        if !parsed_is_transaction_control(&parsed)
            && let Err(error) = self.begin_active_statement(cancellation.as_deref())
        {
            self.fail_sql_transaction();
            return Err(error);
        }
        let mut snapshot = self.statement_snapshot()?;
        snapshot.cancellation = cancellation;
        snapshot.sequence_currvals = self.sequence_currvals.clone();
        let statement = match bind_with_session(
            parsed,
            &snapshot.catalog,
            self.runtime_metadata.bind_values(),
        )
        .and_then(|statement| resolve_sequence_currval(statement, &self.sequence_currvals))
        {
            Ok(statement) => statement,
            Err(error) => {
                self.fail_sql_transaction();
                return Err(error);
            }
        };
        if let Err(error) = reject_system_catalog_write(&statement) {
            self.fail_sql_transaction();
            return Err(error);
        }
        if statement_write_scope(&statement) == StatementWriteScope::ReadOnly {
            let system_table_ids = statement_read_table_ids(&statement)
                .into_iter()
                .filter(|table_id| Catalog::is_system_table(*table_id))
                .collect::<BTreeSet<_>>();
            let system_catalog = match system_catalog::build_system_catalog_snapshot(
                &snapshot.catalog,
                self.authorization.as_ref(),
                &system_table_ids,
            ) {
                Ok(snapshot) => Arc::new(snapshot),
                Err(error) => {
                    self.fail_sql_transaction();
                    return Err(error);
                }
            };
            snapshot.rows.extend(
                system_catalog
                    .tables()
                    .iter()
                    .map(|(table_id, rows)| (*table_id, Arc::clone(rows))),
            );
            snapshot.system_catalog = Some(system_catalog);
        }

        match &statement {
            BoundStatement::Begin { characteristics } => {
                return self.begin_sql_transaction(*characteristics);
            }
            BoundStatement::Commit { chain } => return self.commit_sql_transaction(*chain),
            BoundStatement::Rollback { chain } => return self.rollback_sql_transaction(*chain),
            BoundStatement::Savepoint { name } => return self.create_sql_savepoint(name),
            BoundStatement::RollbackTo { name } => return self.rollback_to_sql_savepoint(name),
            BoundStatement::ReleaseSavepoint { name } => {
                return self.release_sql_savepoint(name);
            }
            _ => {}
        }
        match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
            SqlTransactionState::Idle => self.execute_auto_commit(sql, params, snapshot, statement),
            SqlTransactionState::Active(mut transaction) => {
                match self.execute_in_sql_transaction(
                    &mut transaction,
                    sql,
                    params,
                    snapshot,
                    statement,
                ) {
                    Ok(stream) => {
                        if let Err(error) = transaction.transaction.finish_statement() {
                            transaction.failed = true;
                            self.sql_transaction = SqlTransactionState::Active(transaction);
                            return Err(error);
                        }
                        let stream =
                            stream.with_failure_flag(Arc::clone(&transaction.stream_failed));
                        self.sql_transaction = SqlTransactionState::Active(transaction);
                        Ok(stream)
                    }
                    Err(error) => {
                        transaction.failed = true;
                        self.sql_transaction = SqlTransactionState::Active(transaction);
                        Err(error)
                    }
                }
            }
            SqlTransactionState::Failed(characteristics) => {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                Err(failed_transaction_error())
            }
        }
    }

    pub fn begin(&mut self) -> Result<Transaction<'_>> {
        if self.transaction_status() != TransactionStatus::Idle {
            return Err(DbError::new(
                "25001",
                "a SQL transaction is already active in this session",
            )
            .with_hint("commit or roll back the SQL transaction before using Session::begin"));
        }
        let transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            TransactionCharacteristics::default(),
        )?;
        Ok(Transaction {
            state: &self.state,
            store: &self.store,
            storage_access: &self.storage_access,
            wal: &self.wal,
            transaction_status: &self.transaction_status,
            transactions: &self.transactions,
            locks: &self.locks,
            writer: &self.writer,
            commits_since_checkpoint: &self.commits_since_checkpoint,
            sequence_currvals: &mut self.sequence_currvals,
            dialect: self.options.dialect,
            authorization: self.authorization.clone(),
            runtime_metadata: self.runtime_metadata.clone(),
            transaction,
            base: None,
            working: None,
            lock_guards: Vec::new(),
            lease: None,
            dml_only: true,
            failed: false,
        })
    }

    #[must_use]
    pub fn transaction_status(&self) -> TransactionStatus {
        match &self.sql_transaction {
            SqlTransactionState::Idle => TransactionStatus::Idle,
            SqlTransactionState::Active(transaction)
                if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) =>
            {
                TransactionStatus::Failed
            }
            SqlTransactionState::Active(_) => TransactionStatus::Active,
            SqlTransactionState::Failed(_) => TransactionStatus::Failed,
        }
    }

    /// Mark an explicit SQL transaction as failed when a protocol adapter
    /// executes a statement through a specialized path outside the normal
    /// bound-statement dispatcher.
    pub fn mark_transaction_failed(&mut self) {
        self.normalize_sql_transaction_failure();
        self.fail_sql_transaction();
    }

    pub fn search(&self, request: SearchRequest) -> Result<Vec<SearchResult>> {
        self.search_with_filter(request, None)
    }

    pub fn search_with_filter(
        &self,
        request: SearchRequest,
        filter: Option<&ScalarSearchFilter>,
    ) -> Result<Vec<SearchResult>> {
        let snapshot = self.statement_snapshot()?;
        match request {
            SearchRequest::Text(mut request) => {
                let table_id =
                    search_index_table(&snapshot, request.index_id, IndexMethod::FullText)?;
                if let Some(filter) = filter {
                    let allowed = evaluate_search_filter(&snapshot, table_id, filter)?;
                    request.allowed_rows = intersect_allowed_rows(request.allowed_rows, allowed);
                }
                snapshot
                    .searches
                    .text_search(&request)?
                    .into_iter()
                    .map(|hit| {
                        Ok(SearchResult {
                            row_id: hit.row_id,
                            row: search_result_row(&snapshot, table_id, hit.row_id)?,
                            text_score: Some(hit.score),
                            vector_score: None,
                            distance: None,
                            combined_score: None,
                        })
                    })
                    .collect()
            }
            SearchRequest::Vector(mut request) => {
                let table_id = search_index_table(&snapshot, request.index_id, IndexMethod::Hnsw)?;
                if let Some(filter) = filter {
                    let allowed = evaluate_search_filter(&snapshot, table_id, filter)?;
                    request.allowed_rows = intersect_allowed_rows(request.allowed_rows, allowed);
                }
                snapshot
                    .searches
                    .vector_search(&request)?
                    .into_iter()
                    .map(|hit| {
                        Ok(SearchResult {
                            row_id: hit.row_id,
                            row: search_result_row(&snapshot, table_id, hit.row_id)?,
                            text_score: None,
                            vector_score: Some(hit.score),
                            distance: Some(hit.distance),
                            combined_score: None,
                        })
                    })
                    .collect()
            }
            SearchRequest::Hybrid(mut request) => {
                let text_table =
                    search_index_table(&snapshot, request.text.index_id, IndexMethod::FullText)?;
                let vector_table =
                    search_index_table(&snapshot, request.vector.index_id, IndexMethod::Hnsw)?;
                if text_table != vector_table {
                    return Err(DbError::new(
                        "22023",
                        "hybrid search indexes must belong to the same table",
                    ));
                }
                if let Some(filter) = filter {
                    let allowed = evaluate_search_filter(&snapshot, text_table, filter)?;
                    request.text.allowed_rows =
                        intersect_allowed_rows(request.text.allowed_rows, Arc::clone(&allowed));
                    request.vector.allowed_rows =
                        intersect_allowed_rows(request.vector.allowed_rows, allowed);
                }
                snapshot
                    .searches
                    .hybrid_search(&request)?
                    .into_iter()
                    .map(|hit| {
                        Ok(SearchResult {
                            row_id: hit.row_id,
                            row: search_result_row(&snapshot, text_table, hit.row_id)?,
                            text_score: Some(hit.text_score),
                            vector_score: Some(hit.vector_score),
                            distance: None,
                            combined_score: Some(hit.combined_score),
                        })
                    })
                    .collect()
            }
        }
    }

    #[must_use]
    pub const fn options(&self) -> SessionOptions {
        self.options
    }

    pub fn set_query_memory_limit(&mut self, hard_memory_bytes: usize) -> Result<()> {
        if hard_memory_bytes == 0 || hard_memory_bytes > DEFAULT_HARD_MEMORY_BYTES {
            return Err(DbError::new(
                "22023",
                "query memory limit must be between 1 byte and the server default",
            ));
        }
        self.execution_options.hard_memory_bytes = hard_memory_bytes;
        self.execution_options.soft_memory_bytes = DEFAULT_SOFT_MEMORY_BYTES.min(hard_memory_bytes);
        Ok(())
    }

    fn statement_snapshot(&self) -> Result<DatabaseState> {
        if let SqlTransactionState::Active(transaction) = &self.sql_transaction {
            if let Some(working) = &transaction.working {
                return Ok(working.clone());
            }
            let committed = committed_snapshot(&self.state)?;
            let snapshot = transaction
                .transaction
                .snapshot()
                .ok_or_else(|| no_active_transaction_error("take a statement snapshot"))?;
            return project_database_visibility(
                committed,
                snapshot,
                transaction.transaction.transaction_id(),
                self.transaction_status.as_ref(),
            );
        }
        committed_snapshot(&self.state)
    }

    fn begin_active_statement(&mut self, cancellation: Option<&AtomicBool>) -> Result<()> {
        let state = Arc::clone(&self.state);
        let transaction_status = Arc::clone(&self.transaction_status);
        if let SqlTransactionState::Active(transaction) = &mut self.sql_transaction {
            let snapshot = match cancellation {
                Some(cancellation) => transaction
                    .transaction
                    .begin_statement_with_cancellation(cancellation)?,
                None => transaction.transaction.begin_statement()?,
            }
            .clone();
            if let Some(ssi) = &transaction.ssi {
                ssi.refresh_snapshot(&snapshot)?;
            }
            refresh_read_committed_candidate(
                &state,
                transaction_status.as_ref(),
                &transaction.transaction,
                &mut transaction.base,
                &mut transaction.working,
                transaction.dml_only,
            )?;
        }
        Ok(())
    }

    fn begin_sql_transaction(
        &mut self,
        characteristics: TransactionCharacteristics,
    ) -> Result<TryQueryStream> {
        match self.transaction_status() {
            TransactionStatus::Idle => {
                self.sql_transaction =
                    SqlTransactionState::Active(self.new_active_sql_transaction(characteristics)?);
                Ok(TryQueryStream::new(transaction_events("BEGIN")))
            }
            TransactionStatus::Active => {
                Err(DbError::new("25001", "a transaction is already active")
                    .with_hint("commit or roll back the current transaction first"))
            }
            TransactionStatus::Failed => Err(failed_transaction_error()),
        }
    }

    fn new_active_sql_transaction(
        &self,
        characteristics: TransactionCharacteristics,
    ) -> Result<Box<ActiveSqlTransaction>> {
        let transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            characteristics,
        )?;
        let ssi = SsiTransactionGuard::begin(Arc::clone(&self.ssi), &transaction)?;
        Ok(Box::new(ActiveSqlTransaction {
            transaction,
            base: None,
            working: None,
            locks: Vec::new(),
            lease: None,
            dml_only: true,
            ssi,
            savepoints: SavepointStack::new(),
            savepoint_states: BTreeMap::new(),
            failed: false,
            stream_failed: Arc::new(AtomicBool::new(false)),
            notification_state: NotificationTransactionState::default(),
        }))
    }

    fn commit_sql_transaction(&mut self, chain: TransactionChain) -> Result<TryQueryStream> {
        let transaction = match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
            SqlTransactionState::Idle => return Err(no_active_transaction_error("commit")),
            SqlTransactionState::Failed(characteristics) => {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                return Err(failed_transaction_error());
            }
            SqlTransactionState::Active(transaction)
                if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) =>
            {
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(failed_transaction_error());
            }
            SqlTransactionState::Active(transaction) => transaction,
        };
        let characteristics = transaction
            .transaction
            .characteristics()
            .ok_or_else(|| no_active_transaction_error("commit"))?;
        let ActiveSqlTransaction {
            transaction: mut durable,
            base,
            working,
            mut locks,
            lease,
            dml_only,
            mut ssi,
            notification_state,
            ..
        } = *transaction;
        if let Some(ssi) = &mut ssi
            && let Err(error) = self
                .transactions
                .global_xmin_excluding(durable.transaction_id())
                .and_then(|horizon| ssi.validate_commit(horizon))
        {
            self.sql_transaction = SqlTransactionState::Failed(characteristics);
            return Err(error);
        }
        if let Some(candidate) = working {
            let mut state = match self.state.write() {
                Ok(state) => state,
                Err(_) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(internal_error("engine state lock is poisoned"));
                }
            };
            let candidate = if dml_only {
                let base = base.as_ref().ok_or_else(|| {
                    internal_error("DML transaction is missing its base snapshot")
                })?;
                match merge_dml_candidate(
                    &state,
                    base,
                    &candidate,
                    &durable,
                    self.transaction_status.as_ref(),
                ) {
                    Ok(candidate) => candidate,
                    Err(error) => {
                        self.sql_transaction = SqlTransactionState::Failed(characteristics);
                        return Err(error);
                    }
                }
            } else {
                candidate
            };
            if let Err(error) = persist_candidate(
                &mut state,
                &self.store,
                &self.storage_access,
                &self.wal,
                &mut durable,
                candidate,
            ) {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                return Err(error);
            }
            if let Some(ssi) = &mut ssi {
                ssi.finish();
            }
            drop(state);
            locks.clear();
            drop(lease);
            record_commit_and_maybe_checkpoint(
                &self.state,
                &self.store,
                &self.wal,
                &self.transactions,
                &self.commits_since_checkpoint,
            )?;
        } else {
            if let Err(error) = durable.commit_empty() {
                self.sql_transaction = SqlTransactionState::Failed(characteristics);
                return Err(error);
            }
            if let Some(ssi) = &mut ssi {
                ssi.finish();
            }
        }
        self.notifications
            .commit(self.notification_session_id, notification_state);
        self.start_chained_sql_transaction(chain, characteristics)?;
        Ok(TryQueryStream::new(transaction_events("COMMIT")))
    }

    fn rollback_sql_transaction(&mut self, chain: TransactionChain) -> Result<TryQueryStream> {
        let characteristics =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => return Err(no_active_transaction_error("roll back")),
                SqlTransactionState::Active(transaction) => {
                    let characteristics = transaction
                        .transaction
                        .characteristics()
                        .ok_or_else(|| no_active_transaction_error("roll back"))?;
                    transaction.transaction.abort()?;
                    characteristics
                }
                SqlTransactionState::Failed(characteristics) => characteristics,
            };
        self.start_chained_sql_transaction(chain, characteristics)?;
        Ok(TryQueryStream::new(transaction_events("ROLLBACK")))
    }

    fn start_chained_sql_transaction(
        &mut self,
        chain: TransactionChain,
        characteristics: TransactionCharacteristics,
    ) -> Result<()> {
        if chain == TransactionChain::Chain {
            self.sql_transaction =
                SqlTransactionState::Active(self.new_active_sql_transaction(characteristics)?);
        }
        Ok(())
    }

    fn create_sql_savepoint(&mut self, name: &Identifier) -> Result<TryQueryStream> {
        let mut transaction =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => {
                    return Err(no_active_transaction_error("create a savepoint"));
                }
                SqlTransactionState::Failed(characteristics) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(failed_transaction_error());
                }
                SqlTransactionState::Active(transaction) => transaction,
            };
        if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) {
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(failed_transaction_error());
        }
        let command_id = transaction
            .transaction
            .snapshot()
            .ok_or_else(|| no_active_transaction_error("create a savepoint"))?
            .command_id;
        let ssi = match transaction
            .ssi
            .as_ref()
            .map(SsiTransactionGuard::savepoint)
            .transpose()
        {
            Ok(ssi) => ssi,
            Err(error) => {
                transaction.failed = true;
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(error);
            }
        };
        let id =
            match transaction
                .savepoints
                .push(name.as_str(), command_id, 0, transaction.locks.len())
            {
                Ok(id) => id,
                Err(error) => {
                    self.sql_transaction = SqlTransactionState::Active(transaction);
                    return Err(error);
                }
            };
        transaction.savepoint_states.insert(
            id,
            SqlSavepointState {
                base: transaction.base.clone(),
                working: transaction.working.clone(),
                sequence_currvals: self.sequence_currvals.clone(),
                lock_len: transaction.locks.len(),
                dml_only: transaction.dml_only,
                ssi,
                notification_state: transaction.notification_state.clone(),
            },
        );
        if let Err(error) = transaction.transaction.finish_statement() {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        self.sql_transaction = SqlTransactionState::Active(transaction);
        Ok(TryQueryStream::new(transaction_events("SAVEPOINT")))
    }

    fn rollback_to_sql_savepoint(&mut self, name: &Identifier) -> Result<TryQueryStream> {
        let mut transaction =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => {
                    return Err(no_active_transaction_error("roll back to a savepoint"));
                }
                SqlTransactionState::Failed(characteristics) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(DbError::new(
                        "3B001",
                        format!("savepoint \"{name}\" does not exist"),
                    ));
                }
                SqlTransactionState::Active(transaction) => transaction,
            };
        let savepoint = match transaction.savepoints.rollback_to(name.as_str()) {
            Ok(savepoint) => savepoint,
            Err(error) => {
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(error);
            }
        };
        let saved = match transaction.savepoint_states.get(&savepoint.id).cloned() {
            Some(saved) => saved,
            None => {
                transaction.failed = true;
                self.sql_transaction = SqlTransactionState::Active(transaction);
                return Err(internal_error("savepoint state is missing"));
            }
        };
        let ssi_rollback = match (&transaction.ssi, &saved.ssi) {
            (Some(ssi), Some(savepoint)) => ssi.rollback_to(savepoint),
            (None, None) => Ok(()),
            _ => Err(internal_error("savepoint SSI state is inconsistent")),
        };
        if let Err(error) = ssi_rollback {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        transaction.base = saved.base;
        transaction.working = saved.working;
        transaction.locks.truncate(saved.lock_len);
        transaction.dml_only = saved.dml_only;
        transaction.notification_state = saved.notification_state;
        if transaction.working.is_none() {
            transaction.lease = None;
        }
        self.sequence_currvals = saved.sequence_currvals;
        let retained = transaction
            .savepoints
            .frames()
            .iter()
            .map(|frame| frame.id)
            .collect::<BTreeSet<_>>();
        transaction
            .savepoint_states
            .retain(|id, _| retained.contains(id));
        transaction.failed = false;
        transaction.stream_failed.store(false, Ordering::Release);
        if let Err(error) = transaction.transaction.finish_statement() {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        self.sql_transaction = SqlTransactionState::Active(transaction);
        Ok(TryQueryStream::new(transaction_events("ROLLBACK")))
    }

    fn release_sql_savepoint(&mut self, name: &Identifier) -> Result<TryQueryStream> {
        let mut transaction =
            match mem::replace(&mut self.sql_transaction, SqlTransactionState::Idle) {
                SqlTransactionState::Idle => {
                    return Err(no_active_transaction_error("release a savepoint"));
                }
                SqlTransactionState::Failed(characteristics) => {
                    self.sql_transaction = SqlTransactionState::Failed(characteristics);
                    return Err(failed_transaction_error());
                }
                SqlTransactionState::Active(transaction) => transaction,
            };
        if transaction.failed || transaction.stream_failed.load(Ordering::Acquire) {
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(failed_transaction_error());
        }
        if let Err(error) = transaction.savepoints.release(name.as_str()) {
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        let retained = transaction
            .savepoints
            .frames()
            .iter()
            .map(|frame| frame.id)
            .collect::<BTreeSet<_>>();
        transaction
            .savepoint_states
            .retain(|id, _| retained.contains(id));
        if let Err(error) = transaction.transaction.finish_statement() {
            transaction.failed = true;
            self.sql_transaction = SqlTransactionState::Active(transaction);
            return Err(error);
        }
        self.sql_transaction = SqlTransactionState::Active(transaction);
        Ok(TryQueryStream::new(transaction_events("RELEASE")))
    }

    fn execute_auto_commit(
        &mut self,
        sql: &str,
        params: &[Value],
        snapshot: DatabaseState,
        statement: BoundStatement,
    ) -> Result<TryQueryStream> {
        let procedure = match &statement {
            BoundStatement::Call {
                routine_id,
                arguments,
                schema,
            } if snapshot
                .catalog
                .routine_by_id(*routine_id)
                .is_some_and(|routine| routine.kind == ordadb_catalog::RoutineKind::Procedure) =>
            {
                Some((*routine_id, arguments.clone(), schema.clone()))
            }
            _ => None,
        };
        if let Some((routine_id, arguments, schema)) = procedure {
            return self
                .execute_auto_commit_procedure(snapshot, routine_id, &arguments, schema, params);
        }
        if let Some(stream) = self.execute_auto_commit_session_command(&statement, params)? {
            return Ok(stream);
        }
        let table_provider = StorageTableProviderV2::new(
            Arc::clone(&self.store),
            Arc::clone(&self.storage_access),
            snapshot.generation,
            &snapshot.rows,
            snapshot.system_catalog.as_deref(),
        );
        if let Some(stream) = prepare_read_stream_with_options(
            &snapshot,
            statement.clone(),
            params,
            Some(&table_provider),
            &self.execution_options,
        )? {
            return Ok(stream);
        }
        let compacts_transaction_status =
            matches!(&statement, BoundStatement::Vacuum { table_id: None, .. });
        let sequence_id = sequence_mutation_id(&statement);
        let write_scope = statement_write_scope(&statement);
        let maintenance =
            maintenance_context(self.transactions.as_ref(), self.transaction_status.as_ref())?;
        let (mut preview, events, dirty) = execute_bound_candidate(
            &snapshot,
            statement,
            params,
            self.authorization.as_ref(),
            None,
            maintenance,
        )?;
        if !dirty {
            let pending = mem::take(&mut preview.pending_notifications);
            self.notifications
                .commit(self.notification_session_id, pending);
            return TryQueryStream::buffered(events);
        }

        let mut transaction = DurableTransaction::begin(
            &self.transactions,
            Arc::clone(&self.transaction_status),
            Arc::clone(&self.wal),
            TransactionCharacteristics::default(),
        )?;
        let mut lease = None;
        let mut write_locks = Vec::new();
        match write_scope {
            StatementWriteScope::Dml => {
                let (lock_candidate, _, lock_dirty) = execute_candidate(
                    &snapshot,
                    sql,
                    params,
                    StatementExecutionContext {
                        dialect: self.options.dialect,
                        runtime_metadata: &self.runtime_metadata,
                        authorization: self.authorization.as_ref(),
                    },
                    Some(version_mutation_context(&transaction)?),
                    maintenance,
                )?;
                if lock_dirty {
                    write_locks = acquire_dml_locks(
                        &self.locks,
                        &transaction,
                        &snapshot,
                        &lock_candidate,
                        &[],
                        snapshot.cancellation.as_deref(),
                    )?;
                }
            }
            StatementWriteScope::Exclusive => {
                lease = Some(self.writer.try_acquire(transaction.transaction_id())?);
                write_locks.push(acquire_compatibility_write_lock(
                    &self.locks,
                    &transaction,
                    snapshot.cancellation.as_deref(),
                )?);
            }
            StatementWriteScope::ReadOnly => {
                return Err(internal_error(
                    "read-only statement unexpectedly produced a dirty candidate",
                ));
            }
        }
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let mut committed = state.clone();
        committed.cancellation = snapshot.cancellation.clone();
        committed.sequence_currvals = self.sequence_currvals.clone();
        let (mut candidate, events, dirty) = execute_candidate(
            &committed,
            sql,
            params,
            StatementExecutionContext {
                dialect: self.options.dialect,
                runtime_metadata: &self.runtime_metadata,
                authorization: self.authorization.as_ref(),
            },
            Some(version_mutation_context(&transaction)?),
            maintenance,
        )?;
        let pending_notifications = mem::take(&mut candidate.pending_notifications);
        let runtime_sequence_currvals = candidate.sequence_currvals.clone();
        let stream = TryQueryStream::buffered(events)?;
        if dirty {
            let sequence_value = sequence_id
                .map(|sequence_id| candidate_sequence_value(&candidate, sequence_id))
                .transpose()?;
            persist_candidate(
                &mut state,
                &self.store,
                &self.storage_access,
                &self.wal,
                &mut transaction,
                candidate,
            )?;
            self.sequence_currvals = runtime_sequence_currvals;
            drop(state);
            drop(write_locks);
            drop(lease.take());
            record_commit_and_maybe_checkpoint(
                &self.state,
                &self.store,
                &self.wal,
                &self.transactions,
                &self.commits_since_checkpoint,
            )?;
            if compacts_transaction_status {
                self.transaction_status
                    .compact_before(maintenance.horizon)?;
                self.transactions.compact_before(maintenance.horizon)?;
            }
            if let Some((sequence_id, value)) = sequence_id.zip(sequence_value) {
                self.sequence_currvals.insert(sequence_id, value);
            }
        }
        self.notifications
            .commit(self.notification_session_id, pending_notifications);
        Ok(stream)
    }

    fn execute_auto_commit_procedure(
        &mut self,
        snapshot: DatabaseState,
        routine_id: ordadb_types::RoutineId,
        arguments: &[BoundExpr],
        schema: Schema,
        params: &[Value],
    ) -> Result<TryQueryStream> {
        let mut coordinator = ProcedureTransactionCoordinator::new(self, snapshot)?;
        let mut candidate = coordinator.base.clone();
        let execution = {
            let mut boundary = |boundary, candidate: &mut DatabaseState, dirty| {
                coordinator.boundary(boundary, candidate, dirty)
            };
            execute_routine_program_with_boundaries(
                &mut candidate,
                routine_id,
                arguments,
                params,
                Some(&mut boundary),
            )
        };
        let (output, dirty) = match execution {
            Ok(output) => output,
            Err(error) => {
                coordinator.abort();
                self.sequence_currvals = coordinator.runtime_sequence_currvals();
                return Err(error);
            }
        };
        if let Err(error) = coordinator.finish_final(&mut candidate, dirty) {
            coordinator.abort();
            self.sequence_currvals = coordinator.runtime_sequence_currvals();
            return Err(error);
        }
        self.sequence_currvals = coordinator.runtime_sequence_currvals();
        let row_count = u64::from(!schema.fields.is_empty());
        let batch = (!schema.fields.is_empty()).then(|| Batch {
            schema: schema.clone(),
            rows: vec![Row::new(output.output_parameters)],
        });
        let mut events = command_events(schema, "CALL", row_count, batch);
        insert_pending_notices(&mut events, mem::take(&mut coordinator.notices));
        TryQueryStream::buffered(events)
    }

    fn execute_in_sql_transaction(
        &mut self,
        transaction: &mut ActiveSqlTransaction,
        sql: &str,
        params: &[Value],
        snapshot: DatabaseState,
        statement: BoundStatement,
    ) -> Result<TryQueryStream> {
        if matches!(&statement, BoundStatement::Vacuum { .. }) {
            return Err(DbError::new(
                "25001",
                "VACUUM cannot run inside a transaction block",
            ));
        }
        if let Some(stream) = execute_transaction_session_command(transaction, &statement, params)?
        {
            return Ok(stream);
        }
        if let Some(ssi) = &transaction.ssi {
            for predicate in statement_read_predicates(&statement) {
                ssi.record_read(predicate)?;
            }
        }
        if let Some(stream) = prepare_read_stream_with_options(
            &snapshot,
            statement.clone(),
            params,
            None,
            &self.execution_options,
        )? {
            return Ok(stream);
        }
        let has_conflict_action = matches!(
            &statement,
            BoundStatement::Insert {
                on_conflict: Some(_),
                ..
            }
        );
        let read_committed =
            transaction
                .transaction
                .characteristics()
                .is_some_and(|characteristics| {
                    characteristics.isolation_level == IsolationLevel::ReadCommitted
                });
        let recheck_conflict_after_locks = has_conflict_action && read_committed;
        let sequence_id = sequence_mutation_id(&statement);
        let write_scope = statement_write_scope(&statement);
        let maintenance =
            maintenance_context(self.transactions.as_ref(), self.transaction_status.as_ref())?;
        let (mut candidate, mut events, dirty) = execute_bound_candidate(
            &snapshot,
            statement,
            params,
            self.authorization.as_ref(),
            Some(version_mutation_context(&transaction.transaction)?),
            maintenance,
        )?;
        if !dirty {
            transaction
                .notification_state
                .append(mem::take(&mut candidate.pending_notifications));
            return TryQueryStream::buffered(events);
        }
        if transaction
            .transaction
            .characteristics()
            .is_some_and(|characteristics| {
                characteristics.access_mode == TransactionAccessMode::ReadOnly
            })
        {
            return Err(DbError::new(
                "25006",
                "cannot execute a write in a read-only transaction",
            ));
        }
        let mut statement_base = snapshot.clone();
        match write_scope {
            StatementWriteScope::Dml => {
                let mut acquired = acquire_dml_locks(
                    &self.locks,
                    &transaction.transaction,
                    &snapshot,
                    &candidate,
                    &transaction.locks,
                    snapshot.cancellation.as_deref(),
                )?;
                transaction.locks.append(&mut acquired);
                if recheck_conflict_after_locks {
                    let mut completed = false;
                    for _ in 0..MAX_DML_LOCK_RECHECKS {
                        let mut recheck_base = read_committed_statement_state(
                            &self.state,
                            self.transaction_status.as_ref(),
                            &mut transaction.transaction,
                            transaction.base.as_ref(),
                            transaction.working.as_ref(),
                            snapshot.cancellation.clone(),
                        )?;
                        recheck_base.sequence_currvals = self.sequence_currvals.clone();
                        let (rechecked, rechecked_events, rechecked_dirty) = execute_candidate(
                            &recheck_base,
                            sql,
                            params,
                            StatementExecutionContext {
                                dialect: self.options.dialect,
                                runtime_metadata: &self.runtime_metadata,
                                authorization: self.authorization.as_ref(),
                            },
                            Some(version_mutation_context(&transaction.transaction)?),
                            maintenance,
                        )?;
                        if !rechecked_dirty {
                            return Err(internal_error(
                                "ON CONFLICT lock recheck produced a clean candidate",
                            ));
                        }
                        let mut additional = acquire_dml_locks(
                            &self.locks,
                            &transaction.transaction,
                            &recheck_base,
                            &rechecked,
                            &transaction.locks,
                            snapshot.cancellation.as_deref(),
                        )?;
                        if additional.is_empty() {
                            statement_base = recheck_base;
                            candidate = rechecked;
                            events = rechecked_events;
                            completed = true;
                            break;
                        }
                        transaction.locks.append(&mut additional);
                    }
                    if !completed {
                        return Err(DbError::new(
                            "54001",
                            "ON CONFLICT lock recheck exceeded its iteration limit",
                        )
                        .with_hint("Retry the transaction after concurrent writers finish."));
                    }
                } else if has_conflict_action {
                    let latest = committed_snapshot(&self.state)?;
                    let merge_base = transaction.base.as_ref().unwrap_or(&snapshot);
                    if let Err(error) = merge_dml_candidate(
                        &latest,
                        merge_base,
                        &candidate,
                        &transaction.transaction,
                        self.transaction_status.as_ref(),
                    ) {
                        if error.sql_state == "23505" {
                            return Err(DbError::new(
                                "40001",
                                "could not serialize access due to concurrent ON CONFLICT update",
                            )
                            .with_hint("Retry the transaction with a fresh snapshot."));
                        }
                        return Err(error);
                    }
                }
                if let Some(ssi) = &transaction.ssi {
                    for table_id in changed_table_ids(&statement_base, &candidate) {
                        ssi.record_write(PredicateLock::Table {
                            table_id: table_id.get(),
                        })?;
                    }
                }
                if transaction.base.is_none() {
                    transaction.base = Some(statement_base);
                }
            }
            StatementWriteScope::Exclusive => {
                if transaction.working.is_some() && transaction.dml_only {
                    transaction.locks.clear();
                    let base = transaction.base.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its base snapshot")
                    })?;
                    let working = transaction.working.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its working state")
                    })?;
                    let (upgraded_base, mut upgraded_working, lease, lock) =
                        upgrade_dml_candidate_to_exclusive(
                            DmlUpgradeAuthorities {
                                state: &self.state,
                                statuses: self.transaction_status.as_ref(),
                                locks: &self.locks,
                                writer: &self.writer,
                            },
                            &transaction.transaction,
                            base,
                            working,
                            snapshot.cancellation.clone(),
                        )?;
                    upgraded_working.sequence_currvals = self.sequence_currvals.clone();
                    let (mut candidate, events, dirty) = execute_candidate(
                        &upgraded_working,
                        sql,
                        params,
                        StatementExecutionContext {
                            dialect: self.options.dialect,
                            runtime_metadata: &self.runtime_metadata,
                            authorization: self.authorization.as_ref(),
                        },
                        Some(version_mutation_context(&transaction.transaction)?),
                        maintenance,
                    )?;
                    if !dirty {
                        return Err(internal_error(
                            "exclusive statement unexpectedly produced a clean candidate",
                        ));
                    }
                    let stream = TryQueryStream::buffered(events)?;
                    if let Some(sequence_id) = sequence_id {
                        self.sequence_currvals.insert(
                            sequence_id,
                            candidate_sequence_value(&candidate, sequence_id)?,
                        );
                    }
                    self.sequence_currvals = candidate.sequence_currvals.clone();
                    transaction
                        .notification_state
                        .append(mem::take(&mut candidate.pending_notifications));
                    transaction.base = Some(upgraded_base);
                    transaction.working = Some(candidate);
                    transaction.lease = Some(lease);
                    transaction.locks.push(lock);
                    transaction.dml_only = false;
                    return Ok(stream);
                }
                if transaction.lease.is_none() {
                    transaction.lease = Some(
                        self.writer
                            .try_acquire(transaction.transaction.transaction_id())?,
                    );
                    transaction.locks.push(acquire_compatibility_write_lock(
                        &self.locks,
                        &transaction.transaction,
                        snapshot.cancellation.as_deref(),
                    )?);
                }
                transaction.dml_only = false;
                if transaction.base.is_none() {
                    transaction.base = Some(snapshot.clone());
                }
            }
            StatementWriteScope::ReadOnly => {
                return Err(internal_error(
                    "read-only statement unexpectedly produced a dirty candidate",
                ));
            }
        }
        if transaction.working.is_some() || write_scope == StatementWriteScope::Dml {
            let stream = TryQueryStream::buffered(events)?;
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            self.sequence_currvals = candidate.sequence_currvals.clone();
            transaction
                .notification_state
                .append(mem::take(&mut candidate.pending_notifications));
            transaction.working = Some(candidate);
            return Ok(stream);
        }

        let mut committed = committed_snapshot(&self.state)?;
        committed.cancellation = snapshot.cancellation.clone();
        committed.sequence_currvals = self.sequence_currvals.clone();
        let (mut candidate, events, dirty) = execute_candidate(
            &committed,
            sql,
            params,
            StatementExecutionContext {
                dialect: self.options.dialect,
                runtime_metadata: &self.runtime_metadata,
                authorization: self.authorization.as_ref(),
            },
            Some(version_mutation_context(&transaction.transaction)?),
            maintenance,
        )?;
        let stream = TryQueryStream::buffered(events)?;
        transaction
            .notification_state
            .append(mem::take(&mut candidate.pending_notifications));
        if dirty {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            self.sequence_currvals = candidate.sequence_currvals.clone();
            transaction.working = Some(candidate);
        }
        Ok(stream)
    }

    fn execute_auto_commit_session_command(
        &mut self,
        statement: &BoundStatement,
        params: &[Value],
    ) -> Result<Option<TryQueryStream>> {
        let mut pending = NotificationTransactionState::default();
        if let BoundStatement::PgNotify {
            channel,
            payload,
            schema,
        } = statement
        {
            let (channel, payload) = evaluate_pg_notify(channel, payload, params)?;
            pending.notify(channel, payload);
            self.notifications
                .commit(self.notification_session_id, pending);
            return Ok(Some(TryQueryStream::new(pg_notify_events(schema.clone()))));
        }
        let tag = match statement {
            BoundStatement::Listen { channel } => {
                pending.listen(channel.clone());
                "LISTEN"
            }
            BoundStatement::Unlisten { channel } => {
                pending.unlisten(channel.clone());
                "UNLISTEN"
            }
            BoundStatement::Notify { channel, payload } => {
                pending.notify(channel.clone(), payload.clone());
                "NOTIFY"
            }
            BoundStatement::DiscardAll => {
                pending.unlisten(None);
                self.sequence_currvals.clear();
                "DISCARD ALL"
            }
            BoundStatement::DeallocateAll => "DEALLOCATE ALL",
            _ => return Ok(None),
        };
        self.notifications
            .commit(self.notification_session_id, pending);
        Ok(Some(TryQueryStream::new(transaction_events(tag))))
    }

    fn fail_sql_transaction(&mut self) {
        if let SqlTransactionState::Active(transaction) = &mut self.sql_transaction {
            transaction.failed = true;
        }
    }

    fn normalize_sql_transaction_failure(&mut self) {
        if let SqlTransactionState::Active(transaction) = &mut self.sql_transaction
            && transaction.stream_failed.load(Ordering::Acquire)
        {
            transaction.failed = true;
        }
    }
}

fn resolve_sequence_currval(
    statement: BoundStatement,
    currvals: &BTreeMap<SequenceId, i64>,
) -> Result<BoundStatement> {
    match statement {
        BoundStatement::SequenceValue {
            sequence_id,
            operation: BoundSequenceOperation::CurrentValue { .. },
            schema,
        } => Ok(BoundStatement::SequenceValue {
            sequence_id,
            operation: BoundSequenceOperation::CurrentValue {
                value: currvals.get(&sequence_id).copied(),
            },
            schema,
        }),
        statement => Ok(statement),
    }
}

fn sequence_mutation_id(statement: &BoundStatement) -> Option<SequenceId> {
    match statement {
        BoundStatement::SequenceValue {
            sequence_id,
            operation: BoundSequenceOperation::NextValue | BoundSequenceOperation::SetValue { .. },
            ..
        } => Some(*sequence_id),
        _ => None,
    }
}

fn candidate_sequence_value(state: &DatabaseState, sequence_id: SequenceId) -> Result<i64> {
    state
        .catalog
        .sequence_by_id(sequence_id)
        .map(|sequence| sequence.last_value)
        .ok_or_else(|| internal_error("mutated sequence disappeared from the candidate catalog"))
}

fn bound_statement_schema(statement: &BoundStatement) -> Schema {
    match statement {
        BoundStatement::Select { schema, .. }
        | BoundStatement::AdvancedSelect { schema, .. }
        | BoundStatement::SetOperation { schema, .. }
        | BoundStatement::With { schema, .. }
        | BoundStatement::ViewSelect { schema, .. }
        | BoundStatement::ScalarSelect { schema, .. }
        | BoundStatement::Call { schema, .. }
        | BoundStatement::RoutineSelect { schema, .. }
        | BoundStatement::PgNotify { schema, .. }
        | BoundStatement::SequenceValue { schema, .. } => schema.clone(),
        BoundStatement::Insert {
            returning: Some(returning),
            ..
        }
        | BoundStatement::ViewInsert {
            returning: Some(returning),
            ..
        }
        | BoundStatement::Update {
            returning: Some(returning),
            ..
        }
        | BoundStatement::ViewUpdate {
            returning: Some(returning),
            ..
        }
        | BoundStatement::Delete {
            returning: Some(returning),
            ..
        }
        | BoundStatement::ViewDelete {
            returning: Some(returning),
            ..
        }
        | BoundStatement::Merge(BoundMerge {
            returning: Some(returning),
            ..
        }) => returning.schema.clone(),
        BoundStatement::Explain { .. } => {
            Schema::new(vec![Field::new("QUERY PLAN", ScalarType::Text, false)])
        }
        BoundStatement::Begin { .. }
        | BoundStatement::Commit { .. }
        | BoundStatement::Rollback { .. }
        | BoundStatement::Savepoint { .. }
        | BoundStatement::RollbackTo { .. }
        | BoundStatement::ReleaseSavepoint { .. }
        | BoundStatement::Analyze { .. }
        | BoundStatement::Vacuum { .. }
        | BoundStatement::Reindex { .. }
        | BoundStatement::Listen { .. }
        | BoundStatement::Unlisten { .. }
        | BoundStatement::Notify { .. }
        | BoundStatement::Do { .. }
        | BoundStatement::DiscardAll
        | BoundStatement::DeallocateAll
        | BoundStatement::NoOp { .. }
        | BoundStatement::CreateSchema { .. }
        | BoundStatement::CreateEnumType { .. }
        | BoundStatement::CreateDomain { .. }
        | BoundStatement::AlterEnumAddValue { .. }
        | BoundStatement::AlterEnumRenameValue { .. }
        | BoundStatement::AlterDomain { .. }
        | BoundStatement::AlterSchemaRename { .. }
        | BoundStatement::DropObjects { .. }
        | BoundStatement::CreateTable { .. }
        | BoundStatement::AlterTable { .. }
        | BoundStatement::CreateIndex { .. }
        | BoundStatement::AlterIndexRename { .. }
        | BoundStatement::CreateSequence { .. }
        | BoundStatement::AlterSequenceRename { .. }
        | BoundStatement::AlterSequence { .. }
        | BoundStatement::CreateView { .. }
        | BoundStatement::AlterViewRename { .. }
        | BoundStatement::RefreshMaterializedView { .. }
        | BoundStatement::CreateRoutine { .. }
        | BoundStatement::DropRoutine { .. }
        | BoundStatement::CreateTrigger { .. }
        | BoundStatement::DropTrigger { .. }
        | BoundStatement::Insert { .. }
        | BoundStatement::ViewInsert { .. }
        | BoundStatement::Merge(BoundMerge {
            returning: None, ..
        })
        | BoundStatement::Update { .. }
        | BoundStatement::ViewUpdate { .. }
        | BoundStatement::Delete { .. }
        | BoundStatement::ViewDelete { .. } => Schema::empty(),
    }
}

fn bound_statement_parameter_types(statement: &BoundStatement) -> Result<Vec<ScalarType>> {
    let mut statements = vec![statement];
    let mut expressions = Vec::new();
    while let Some(statement) = statements.pop() {
        match statement {
            BoundStatement::CreateView { query, .. }
            | BoundStatement::RefreshMaterializedView { query, .. } => {
                statements.push(query);
            }
            BoundStatement::ViewSelect { source, .. }
            | BoundStatement::Explain { statement: source } => {
                statements.push(source);
            }
            BoundStatement::Call { arguments, .. }
            | BoundStatement::RoutineSelect { arguments, .. } => {
                expressions.extend(arguments);
            }
            BoundStatement::ScalarSelect { projection, .. } => {
                expressions.extend(projection.iter().map(|projection| &projection.expr));
            }
            BoundStatement::PgNotify {
                channel, payload, ..
            } => {
                expressions.push(channel);
                expressions.push(payload);
            }
            BoundStatement::SequenceValue { operation, .. } => {
                if let BoundSequenceOperation::SetValue { value, .. } = operation {
                    expressions.push(value);
                }
            }
            BoundStatement::Insert {
                rows,
                on_conflict,
                returning,
                ..
            } => {
                expressions.extend(rows.iter().flatten());
                if let Some(BoundOnConflict {
                    action:
                        BoundConflictAction::DoUpdate {
                            assignments,
                            filter,
                        },
                    ..
                }) = on_conflict
                {
                    expressions.extend(assignments.iter().map(|(_, expression)| expression));
                    expressions.extend(filter.iter());
                }
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::ViewInsert {
                source,
                rows,
                returning,
                ..
            } => {
                statements.push(source);
                expressions.extend(rows.iter().flatten());
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::Merge(merge) => {
                expressions.push(&merge.on);
                for clause in &merge.clauses {
                    expressions.extend(clause.predicate.iter());
                    match &clause.action {
                        BoundMergeAction::Update { assignments } => {
                            expressions.extend(assignments.iter().map(|(_, expression)| expression))
                        }
                        BoundMergeAction::Insert { values, .. } => expressions.extend(values),
                        BoundMergeAction::Delete | BoundMergeAction::DoNothing => {}
                    }
                }
                push_returning_expressions(&mut expressions, merge.returning.as_ref());
            }
            BoundStatement::With { ctes, body, .. } => {
                statements.push(body);
                for cte in ctes {
                    statements.push(&cte.seed);
                    if let Some(recursive) = &cte.recursive {
                        statements.push(recursive);
                    }
                }
            }
            BoundStatement::SetOperation {
                left,
                right,
                order_by,
                offset,
                limit,
                ..
            } => {
                statements.extend([left.as_ref(), right.as_ref()]);
                push_order_expressions(&mut expressions, order_by);
                expressions.extend(offset.iter());
                expressions.extend(limit.iter());
            }
            BoundStatement::Select {
                projection,
                filter,
                order_by,
                offset,
                limit,
                ..
            } => {
                push_projection_expressions(&mut expressions, projection);
                expressions.extend(filter.iter());
                push_order_expressions(&mut expressions, order_by);
                expressions.extend(offset.iter());
                expressions.extend(limit.iter());
            }
            BoundStatement::AdvancedSelect {
                joins,
                applies,
                windows,
                projection,
                filter,
                group_by,
                having,
                order_by,
                offset,
                limit,
                ..
            } => {
                for join in joins {
                    if let BoundJoinSource::Derived { query, .. } = &join.source {
                        statements.push(query);
                    }
                    expressions.push(&join.on);
                }
                for apply in applies {
                    statements.push(&apply.query);
                    match &apply.kind {
                        BoundApplyKind::In { left, .. }
                        | BoundApplyKind::Quantified { left, .. } => expressions.push(left),
                        BoundApplyKind::RowScalar { left, .. }
                        | BoundApplyKind::RowQuantified { left, .. } => expressions.extend(left),
                        BoundApplyKind::Scalar | BoundApplyKind::Exists { .. } => {}
                    }
                }
                for window in windows {
                    expressions.extend(&window.arguments);
                    expressions.extend(window.filter.iter());
                    expressions.extend(&window.partition_by);
                    push_order_expressions(&mut expressions, &window.order_by);
                    if let Some(frame) = &window.frame {
                        push_window_frame_bound(&mut expressions, &frame.start_bound);
                        push_window_frame_bound(&mut expressions, &frame.end_bound);
                    }
                }
                push_projection_expressions(&mut expressions, projection);
                expressions.extend(filter.iter());
                expressions.extend(group_by);
                expressions.extend(having.iter());
                push_order_expressions(&mut expressions, order_by);
                expressions.extend(offset.iter());
                expressions.extend(limit.iter().map(Box::as_ref));
            }
            BoundStatement::Update {
                assignments,
                filter,
                returning,
                ..
            } => {
                expressions.extend(assignments.iter().map(|(_, expression)| expression));
                expressions.extend(filter.iter());
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::ViewUpdate {
                source,
                assignments,
                filter,
                returning,
                ..
            } => {
                statements.push(source);
                expressions.extend(assignments.iter().map(|(_, expression)| expression));
                expressions.extend(filter.iter());
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::Delete {
                filter, returning, ..
            } => {
                expressions.extend(filter.iter());
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::ViewDelete {
                source,
                filter,
                returning,
                ..
            } => {
                statements.push(source);
                expressions.extend(filter.iter());
                push_returning_expressions(&mut expressions, returning.as_ref());
            }
            BoundStatement::NoOp { .. }
            | BoundStatement::Begin { .. }
            | BoundStatement::Commit { .. }
            | BoundStatement::Rollback { .. }
            | BoundStatement::Savepoint { .. }
            | BoundStatement::RollbackTo { .. }
            | BoundStatement::ReleaseSavepoint { .. }
            | BoundStatement::Analyze { .. }
            | BoundStatement::Vacuum { .. }
            | BoundStatement::Reindex { .. }
            | BoundStatement::Listen { .. }
            | BoundStatement::Unlisten { .. }
            | BoundStatement::Notify { .. }
            | BoundStatement::Do { .. }
            | BoundStatement::DiscardAll
            | BoundStatement::DeallocateAll
            | BoundStatement::CreateSchema { .. }
            | BoundStatement::CreateEnumType { .. }
            | BoundStatement::CreateDomain { .. }
            | BoundStatement::AlterEnumAddValue { .. }
            | BoundStatement::AlterEnumRenameValue { .. }
            | BoundStatement::AlterDomain { .. }
            | BoundStatement::AlterSchemaRename { .. }
            | BoundStatement::DropObjects { .. }
            | BoundStatement::CreateTable { .. }
            | BoundStatement::AlterTable { .. }
            | BoundStatement::CreateIndex { .. }
            | BoundStatement::AlterIndexRename { .. }
            | BoundStatement::CreateSequence { .. }
            | BoundStatement::AlterSequenceRename { .. }
            | BoundStatement::AlterSequence { .. }
            | BoundStatement::AlterViewRename { .. }
            | BoundStatement::CreateRoutine { .. }
            | BoundStatement::DropRoutine { .. }
            | BoundStatement::CreateTrigger { .. }
            | BoundStatement::DropTrigger { .. } => {}
        }
    }

    let mut parameters = BTreeMap::new();
    while let Some(expression) = expressions.pop() {
        match &expression.kind {
            BoundExprKind::Parameter { index } => {
                if let Some(existing) = parameters.get(index) {
                    if existing != &expression.data_type {
                        return Err(DbError::new(
                            "42804",
                            format!("inconsistent types deduced for parameter ${index}"),
                        )
                        .with_detail(format!(
                            "parameter ${index} was inferred as both {existing:?} and {:?}",
                            expression.data_type
                        )));
                    }
                } else {
                    parameters.insert(*index, expression.data_type.clone());
                }
            }
            BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => {
                expressions.push(expr);
            }
            BoundExprKind::Array { elements, .. } => expressions.extend(elements),
            BoundExprKind::Function { arguments, .. } => expressions.extend(arguments),
            BoundExprKind::Binary { left, right, .. } => {
                expressions.extend([left.as_ref(), right.as_ref()]);
            }
            BoundExprKind::InList { expr, list, .. } => {
                expressions.push(expr);
                expressions.extend(list);
            }
            BoundExprKind::Aggregate {
                argument, filter, ..
            } => {
                expressions.extend(argument.iter().map(Box::as_ref));
                expressions.extend(filter.iter().map(Box::as_ref));
            }
            BoundExprKind::Column { .. }
            | BoundExprKind::Literal(_)
            | BoundExprKind::Correlation { .. }
            | BoundExprKind::ApplyValue { .. } => {}
        }
    }

    let Some(max_index) = parameters.keys().next_back().copied() else {
        return Ok(Vec::new());
    };
    (1..=max_index)
        .map(|index| {
            parameters.get(&index).cloned().ok_or_else(|| {
                DbError::new(
                    "42P18",
                    format!("could not determine data type of parameter ${index}"),
                )
            })
        })
        .collect()
}

fn push_projection_expressions<'a>(
    expressions: &mut Vec<&'a BoundExpr>,
    projection: &'a [BoundProjection],
) {
    expressions.extend(projection.iter().map(|projection| &projection.expr));
}

fn push_returning_expressions<'a>(
    expressions: &mut Vec<&'a BoundExpr>,
    returning: Option<&'a BoundReturning>,
) {
    if let Some(returning) = returning {
        push_projection_expressions(expressions, &returning.projection);
    }
}

fn push_order_expressions<'a>(expressions: &mut Vec<&'a BoundExpr>, order_by: &'a [BoundOrder]) {
    expressions.extend(
        order_by
            .iter()
            .filter_map(|order| order.expression.as_ref()),
    );
}

fn push_window_frame_bound<'a>(
    expressions: &mut Vec<&'a BoundExpr>,
    bound: &'a BoundWindowFrameBound,
) {
    if let BoundWindowFrameBound::Preceding(expression)
    | BoundWindowFrameBound::Following(expression) = bound
    {
        expressions.push(expression);
    }
}

#[derive(Debug)]
pub struct Transaction<'session> {
    state: &'session Arc<RwLock<DatabaseState>>,
    store: &'session Arc<Mutex<DatabaseStore>>,
    storage_access: &'session Arc<StorageAccessGate>,
    wal: &'session Arc<WalManager>,
    transaction_status: &'session Arc<TransactionStatusStore>,
    transactions: &'session Arc<TransactionManager>,
    locks: &'session Arc<LockManager>,
    writer: &'session Arc<WriterCoordinator>,
    commits_since_checkpoint: &'session Arc<AtomicU64>,
    sequence_currvals: &'session mut BTreeMap<SequenceId, i64>,
    dialect: SqlDialect,
    authorization: Option<SessionAuthorization>,
    runtime_metadata: SessionRuntimeMetadata,
    transaction: DurableTransaction,
    base: Option<DatabaseState>,
    working: Option<DatabaseState>,
    lock_guards: Vec<LockGuard>,
    lease: Option<WriterLease>,
    dml_only: bool,
    failed: bool,
}

impl Transaction<'_> {
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        if self.failed {
            return Err(failed_transaction_error());
        }
        self.transaction.begin_statement()?;
        if let Err(error) = refresh_read_committed_candidate(
            self.state,
            self.transaction_status.as_ref(),
            &self.transaction,
            &mut self.base,
            &mut self.working,
            self.dml_only,
        ) {
            self.working = None;
            self.lock_guards.clear();
            self.lease = None;
            self.failed = true;
            return Err(error);
        }
        match self.execute_inner(sql, params) {
            Ok(stream) => {
                self.transaction.finish_statement()?;
                Ok(stream)
            }
            Err(error) => {
                self.working = None;
                self.lock_guards.clear();
                self.lease = None;
                self.failed = true;
                Err(error)
            }
        }
    }

    pub fn commit(mut self) -> Result<()> {
        if self.failed {
            return Err(failed_transaction_error());
        }
        let Some(candidate) = self.working else {
            return self.transaction.commit_empty();
        };
        let mut state = self
            .state
            .write()
            .map_err(|_| internal_error("engine state lock is poisoned"))?;
        let candidate = if self.dml_only {
            let base = self
                .base
                .as_ref()
                .ok_or_else(|| internal_error("DML transaction is missing its base snapshot"))?;
            merge_dml_candidate(
                &state,
                base,
                &candidate,
                &self.transaction,
                self.transaction_status.as_ref(),
            )?
        } else {
            candidate
        };
        persist_candidate(
            &mut state,
            self.store,
            self.storage_access,
            self.wal,
            &mut self.transaction,
            candidate,
        )?;
        drop(state);
        self.lock_guards.clear();
        self.lease = None;
        record_commit_and_maybe_checkpoint(
            self.state,
            self.store,
            self.wal,
            self.transactions,
            self.commits_since_checkpoint,
        )
    }

    pub fn rollback(self) -> Result<()> {
        self.transaction.abort()
    }

    fn execute_inner(&mut self, sql: &str, params: &[Value]) -> Result<QueryStream> {
        let mut snapshot = match &self.working {
            Some(working) => working.clone(),
            None => {
                let committed = committed_snapshot(self.state)?;
                let transaction_snapshot = self
                    .transaction
                    .snapshot()
                    .ok_or_else(|| no_active_transaction_error("take a statement snapshot"))?;
                project_database_visibility(
                    committed,
                    transaction_snapshot,
                    self.transaction.transaction_id(),
                    self.transaction_status.as_ref(),
                )?
            }
        };
        snapshot.sequence_currvals = self.sequence_currvals.clone();
        let statement = resolve_sequence_currval(
            bind_with_session(
                parse_with_dialect(sql, self.dialect)?,
                &snapshot.catalog,
                self.runtime_metadata.bind_values(),
            )?,
            self.sequence_currvals,
        )?;
        if matches!(
            &statement,
            BoundStatement::Begin { .. }
                | BoundStatement::Commit { .. }
                | BoundStatement::Rollback { .. }
                | BoundStatement::Savepoint { .. }
                | BoundStatement::RollbackTo { .. }
                | BoundStatement::ReleaseSavepoint { .. }
        ) {
            return Err(DbError::new(
                "25001",
                "SQL transaction control is not allowed inside Session::begin",
            )
            .with_hint("use Transaction::commit or Transaction::rollback"));
        }
        if matches!(&statement, BoundStatement::Vacuum { .. }) {
            return Err(DbError::new(
                "25001",
                "VACUUM cannot run inside a transaction block",
            ));
        }
        if let Some(stream) = prepare_read_stream(&snapshot, statement.clone(), params, None)? {
            return stream.collect::<Result<Vec<_>>>().map(QueryStream::new);
        }
        let sequence_id = sequence_mutation_id(&statement);
        let write_scope = statement_write_scope(&statement);
        let maintenance =
            maintenance_context(self.transactions.as_ref(), self.transaction_status.as_ref())?;
        let (candidate, events, dirty) = execute_bound_candidate(
            &snapshot,
            statement,
            params,
            self.authorization.as_ref(),
            Some(version_mutation_context(&self.transaction)?),
            maintenance,
        )?;
        if !dirty {
            return Ok(QueryStream::new(events));
        }
        match write_scope {
            StatementWriteScope::Dml => {
                let mut acquired = acquire_dml_locks(
                    self.locks,
                    &self.transaction,
                    &snapshot,
                    &candidate,
                    &self.lock_guards,
                    None,
                )?;
                self.lock_guards.append(&mut acquired);
                if self.base.is_none() {
                    self.base = Some(snapshot.clone());
                }
            }
            StatementWriteScope::Exclusive => {
                if self.working.is_some() && self.dml_only {
                    self.lock_guards.clear();
                    let base = self.base.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its base snapshot")
                    })?;
                    let working = self.working.as_ref().ok_or_else(|| {
                        internal_error("DML transaction is missing its working state")
                    })?;
                    let (upgraded_base, mut upgraded_working, lease, lock) =
                        upgrade_dml_candidate_to_exclusive(
                            DmlUpgradeAuthorities {
                                state: self.state,
                                statuses: self.transaction_status.as_ref(),
                                locks: self.locks,
                                writer: self.writer,
                            },
                            &self.transaction,
                            base,
                            working,
                            None,
                        )?;
                    upgraded_working.sequence_currvals = self.sequence_currvals.clone();
                    let (candidate, events, dirty) = execute_candidate(
                        &upgraded_working,
                        sql,
                        params,
                        StatementExecutionContext {
                            dialect: self.dialect,
                            runtime_metadata: &self.runtime_metadata,
                            authorization: self.authorization.as_ref(),
                        },
                        Some(version_mutation_context(&self.transaction)?),
                        maintenance,
                    )?;
                    if !dirty {
                        return Err(internal_error(
                            "exclusive statement unexpectedly produced a clean candidate",
                        ));
                    }
                    if let Some(sequence_id) = sequence_id {
                        self.sequence_currvals.insert(
                            sequence_id,
                            candidate_sequence_value(&candidate, sequence_id)?,
                        );
                    }
                    *self.sequence_currvals = candidate.sequence_currvals.clone();
                    self.base = Some(upgraded_base);
                    self.working = Some(candidate);
                    self.lease = Some(lease);
                    self.lock_guards.push(lock);
                    self.dml_only = false;
                    return Ok(QueryStream::new(events));
                }
                if self.lease.is_none() {
                    self.lease = Some(self.writer.try_acquire(self.transaction.transaction_id())?);
                    self.lock_guards.push(acquire_compatibility_write_lock(
                        self.locks,
                        &self.transaction,
                        None,
                    )?);
                }
                self.dml_only = false;
                if self.base.is_none() {
                    self.base = Some(snapshot.clone());
                }
            }
            StatementWriteScope::ReadOnly => {
                return Err(internal_error(
                    "read-only statement unexpectedly produced a dirty candidate",
                ));
            }
        }
        if self.working.is_some() || write_scope == StatementWriteScope::Dml {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            *self.sequence_currvals = candidate.sequence_currvals.clone();
            self.working = Some(candidate);
            return Ok(QueryStream::new(events));
        }

        let mut committed = committed_snapshot(self.state)?;
        committed.sequence_currvals = self.sequence_currvals.clone();
        let (candidate, events, dirty) = execute_candidate(
            &committed,
            sql,
            params,
            StatementExecutionContext {
                dialect: self.dialect,
                runtime_metadata: &self.runtime_metadata,
                authorization: self.authorization.as_ref(),
            },
            Some(version_mutation_context(&self.transaction)?),
            maintenance,
        )?;
        if dirty {
            if let Some(sequence_id) = sequence_id {
                self.sequence_currvals.insert(
                    sequence_id,
                    candidate_sequence_value(&candidate, sequence_id)?,
                );
            }
            *self.sequence_currvals = candidate.sequence_currvals.clone();
            self.working = Some(candidate);
        }
        Ok(QueryStream::new(events))
    }
}

#[derive(Debug)]
pub struct QueryStream {
    events: std::vec::IntoIter<QueryEvent>,
}

pub struct TryQueryStream {
    state: TryQueryStreamState,
    failed: bool,
    failure_flag: Option<Arc<AtomicBool>>,
    cancellation: Option<Arc<AtomicBool>>,
    execution_memory_peak_bytes: Option<usize>,
    _event_reservation: Option<Reservation>,
}

enum TryQueryStreamState {
    Events(std::vec::IntoIter<Result<QueryEvent>>),
    Select(Box<SelectStreamState>),
    Done,
}

struct SelectStreamState {
    schema: Schema,
    cursor: StreamBatchCursor,
    phase: SelectStreamPhase,
    rows_processed: u64,
    emitted_batch: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectStreamPhase {
    Schema,
    Batches,
    EmptyBatch,
    Progress,
    Complete,
    Done,
}

enum StreamBatchCursor {
    Simple(Box<ExecutionCursor>),
    Advanced(Box<AdvancedExecutionCursor>),
}

impl StreamBatchCursor {
    fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Self::Simple(cursor) => cursor.next_batch(),
            Self::Advanced(cursor) => cursor.next_batch(),
        }
    }

    fn memory_peak_bytes(&self) -> usize {
        match self {
            Self::Simple(cursor) => cursor.memory().peak_bytes(),
            Self::Advanced(cursor) => cursor.memory_peak_bytes(),
        }
    }
}

impl std::fmt::Debug for TryQueryStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TryQueryStream")
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl TryQueryStream {
    fn new(events: Vec<QueryEvent>) -> Self {
        Self {
            state: TryQueryStreamState::Events(
                events.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
            ),
            failed: false,
            failure_flag: None,
            cancellation: None,
            execution_memory_peak_bytes: None,
            _event_reservation: None,
        }
    }

    fn buffered(events: Vec<QueryEvent>) -> Result<Self> {
        let memory = MemoryGrant::new(DEFAULT_SOFT_MEMORY_BYTES, DEFAULT_HARD_MEMORY_BYTES)?;
        let bytes = events.iter().try_fold(0usize, |total, event| {
            let QueryEvent::Batch(batch) = event else {
                return Ok(total);
            };
            batch.rows.iter().try_fold(total, |total, row| {
                total.checked_add(estimated_row_bytes(row)).ok_or_else(|| {
                    DbError::new("53200", "query memory limit exceeded")
                        .with_detail("RETURNING row memory accounting overflow")
                })
            })
        })?;
        if bytes == 0 {
            return Ok(Self::new(events));
        }
        let reservation = memory.try_reserve(bytes)?;
        let peak = memory.peak_bytes();
        Ok(Self {
            state: TryQueryStreamState::Events(
                events.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
            ),
            failed: false,
            failure_flag: None,
            cancellation: None,
            execution_memory_peak_bytes: Some(peak),
            _event_reservation: Some(reservation),
        })
    }

    fn select(
        schema: Schema,
        cursor: StreamBatchCursor,
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Self {
        Self {
            state: TryQueryStreamState::Select(Box::new(SelectStreamState {
                schema,
                cursor,
                phase: SelectStreamPhase::Schema,
                rows_processed: 0,
                emitted_batch: false,
            })),
            failed: false,
            failure_flag: None,
            cancellation,
            execution_memory_peak_bytes: Some(0),
            _event_reservation: None,
        }
    }

    fn with_failure_flag(mut self, failure_flag: Arc<AtomicBool>) -> Self {
        self.failure_flag = Some(failure_flag);
        self
    }

    /// Returns the highest number of query-accounted bytes observed by this
    /// SELECT or row-returning DML stream. Other statements return `None`.
    #[must_use]
    pub const fn execution_memory_peak_bytes(&self) -> Option<usize> {
        self.execution_memory_peak_bytes
    }
}

impl Iterator for TryQueryStream {
    type Item = Result<QueryEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if self
            .cancellation
            .as_ref()
            .is_some_and(|cancellation| cancellation.load(Ordering::Acquire))
        {
            self.failed = true;
            if let Some(failure_flag) = &self.failure_flag {
                failure_flag.store(true, Ordering::Release);
            }
            self.state = TryQueryStreamState::Done;
            return Some(Err(DbError::new("57014", "query was cancelled")));
        }
        let event = match &mut self.state {
            TryQueryStreamState::Events(events) => events.next().transpose(),
            TryQueryStreamState::Select(stream) => stream.next_event(),
            TryQueryStreamState::Done => Ok(None),
        };
        if let TryQueryStreamState::Select(stream) = &self.state {
            self.execution_memory_peak_bytes = Some(stream.cursor.memory_peak_bytes());
        }
        match event {
            Ok(Some(event)) => Some(Ok(event)),
            Ok(None) => {
                self.state = TryQueryStreamState::Done;
                None
            }
            Err(error) => {
                self.failed = true;
                if let Some(failure_flag) = &self.failure_flag {
                    failure_flag.store(true, Ordering::Release);
                }
                self.state = TryQueryStreamState::Done;
                Some(Err(error))
            }
        }
    }
}

impl SelectStreamState {
    fn next_event(&mut self) -> Result<Option<QueryEvent>> {
        match self.phase {
            SelectStreamPhase::Schema => {
                self.phase = SelectStreamPhase::Batches;
                Ok(Some(QueryEvent::Schema(self.schema.clone())))
            }
            SelectStreamPhase::Batches => match self.cursor.next_batch()? {
                Some(batch) => {
                    self.rows_processed =
                        self.rows_processed.saturating_add(batch.rows.len() as u64);
                    self.emitted_batch = true;
                    Ok(Some(QueryEvent::Batch(batch)))
                }
                None if !self.emitted_batch => {
                    self.phase = SelectStreamPhase::EmptyBatch;
                    Ok(Some(QueryEvent::Batch(Batch {
                        schema: self.schema.clone(),
                        rows: Vec::new(),
                    })))
                }
                None => {
                    self.phase = SelectStreamPhase::Progress;
                    self.next_event()
                }
            },
            SelectStreamPhase::EmptyBatch => {
                self.phase = SelectStreamPhase::Progress;
                self.next_event()
            }
            SelectStreamPhase::Progress => {
                self.phase = SelectStreamPhase::Complete;
                Ok(Some(QueryEvent::Progress(QueryProgress {
                    rows_processed: self.rows_processed,
                })))
            }
            SelectStreamPhase::Complete => {
                self.phase = SelectStreamPhase::Done;
                Ok(Some(QueryEvent::Complete(CommandComplete {
                    tag: format!("SELECT {}", self.rows_processed),
                    rows_affected: self.rows_processed,
                })))
            }
            SelectStreamPhase::Done => Ok(None),
        }
    }
}

impl QueryStream {
    fn new(events: Vec<QueryEvent>) -> Self {
        Self {
            events: events.into_iter(),
        }
    }
}

impl Iterator for QueryStream {
    type Item = QueryEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.events.next()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoutineFrameId(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoutineFrameKind {
    Routine(ordadb_types::RoutineId),
    Trigger(ordadb_types::TriggerId),
}

#[derive(Debug, Clone)]
struct RoutineFrame {
    kind: RoutineFrameKind,
}

#[derive(Debug, Clone, Default)]
struct RoutineFrameStack {
    arena: Vec<Option<RoutineFrame>>,
    free: Vec<usize>,
    active: Vec<RoutineFrameId>,
}

impl RoutineFrameStack {
    fn push_routine(&mut self, routine_id: ordadb_types::RoutineId) -> Result<RoutineFrameId> {
        self.push(RoutineFrameKind::Routine(routine_id))
    }

    fn push_trigger(&mut self, trigger_id: ordadb_types::TriggerId) -> Result<RoutineFrameId> {
        self.push(RoutineFrameKind::Trigger(trigger_id))
    }

    fn push(&mut self, kind: RoutineFrameKind) -> Result<RoutineFrameId> {
        let (current, maximum, label) = match kind {
            RoutineFrameKind::Routine(_) => (
                self.active_kind_count(|kind| matches!(kind, RoutineFrameKind::Routine(_))),
                MAX_ROUTINE_FRAMES,
                "routine-call",
            ),
            RoutineFrameKind::Trigger(_) => (
                self.active_kind_count(|kind| matches!(kind, RoutineFrameKind::Trigger(_))),
                MAX_TRIGGER_FRAMES,
                "trigger",
            ),
        };
        if current >= maximum {
            return Err(DbError::new(
                "54001",
                format!("PL/pgSQL {label} depth exceeds the maximum of {maximum}"),
            ));
        }
        let frame = RoutineFrame { kind };
        let index = if let Some(index) = self.free.pop() {
            self.arena[index] = Some(frame);
            index
        } else {
            let index = self.arena.len();
            self.arena.push(Some(frame));
            index
        };
        let id = RoutineFrameId(index);
        self.active.push(id);
        Ok(id)
    }

    fn pop(&mut self, id: RoutineFrameId) -> Result<()> {
        if self.active.last().copied() != Some(id) {
            return Err(internal_error("PL/pgSQL routine frame stack is not LIFO"));
        }
        self.active.pop();
        let frame = self
            .arena
            .get_mut(id.0)
            .and_then(Option::take)
            .ok_or_else(|| internal_error("PL/pgSQL routine frame is missing"))?;
        let _ = frame.kind;
        self.free.push(id.0);
        Ok(())
    }

    fn active_kind_count(&self, matches: impl Fn(RoutineFrameKind) -> bool) -> usize {
        self.active
            .iter()
            .filter_map(|id| self.arena.get(id.0).and_then(Option::as_ref))
            .filter(|frame| matches(frame.kind))
            .count()
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

enum RoutineCompletion {
    Root,
    Call { schema: Schema },
    Select { schema: Schema, returns_set: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcedureBoundary {
    Commit(TransactionChain),
    Rollback(TransactionChain),
}

type ProcedureBoundaryHandler<'a> =
    dyn FnMut(ProcedureBoundary, &mut DatabaseState, bool) -> Result<()> + 'a;

struct RoutineVmFrame {
    id: RoutineFrameId,
    routine: RoutineDefinition,
    machine: VmMachine,
    response: Option<Result<VmSqlStream>>,
    completion: RoutineCompletion,
    exception_states: Vec<DatabaseState>,
    exception_triggers: Vec<Option<TriggerRowSavepoint>>,
    exception_charges: Vec<usize>,
    exception_memory: VmMemoryReservation,
}

struct RoutineCompletionStream {
    events: std::vec::IntoIter<QueryEvent>,
    _memory: Option<VmMemoryHold>,
}

impl Iterator for RoutineCompletionStream {
    type Item = Result<QueryEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.events.next().map(Ok)
    }
}

#[derive(Debug, Clone, Default)]
struct DatabaseState {
    catalog: Arc<Catalog>,
    rows: BTreeMap<TableId, Arc<Vec<Row>>>,
    system_catalog: Option<Arc<system_catalog::SystemCatalogSnapshot>>,
    versions: BTreeMap<TableId, Arc<Vec<VersionedRow>>>,
    visible_versions: BTreeMap<TableId, Arc<Vec<u32>>>,
    indexes: BTreeMap<IndexId, Arc<BPlusTree>>,
    searches: Arc<SearchCatalog>,
    generation: u64,
    triggers_fired: usize,
    routine_frames: RoutineFrameStack,
    pending_notices: Vec<DbNotice>,
    pending_notifications: NotificationTransactionState,
    cancellation: Option<Arc<AtomicBool>>,
    authorization: Option<SessionAuthorization>,
    sequence_currvals: BTreeMap<SequenceId, i64>,
}

struct SelectExecution {
    table_id: TableId,
    schema: Schema,
    projection: Vec<BoundProjection>,
    filter: Option<BoundExpr>,
    order_by: Vec<BoundOrder>,
    offset: Option<BoundExpr>,
    limit: Option<BoundExpr>,
}

struct SetExecution {
    left: Box<BoundStatement>,
    operator: QuerySetOperator,
    all: bool,
    right: Box<BoundStatement>,
    schema: Schema,
    order_by: Vec<BoundOrder>,
    offset: Option<BoundExpr>,
    limit: Option<BoundExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SetRowKey(Vec<SetValueKey>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SetValueKey {
    Null,
    Boolean(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(u32),
    Float64(u64),
    Decimal(String),
    Text(String),
    Binary(Vec<u8>),
    Date(String),
    Time(String),
    Timestamp(String),
    Interval(i32, i32, i64),
    Array(String),
    Jsonb(String),
    Uuid([u8; 16]),
    Vector(Vec<u32>),
}

struct CreateViewExecution {
    schema: Identifier,
    name: Identifier,
    kind: ViewKind,
    query: BoundStatement,
    query_sql: String,
    output: Schema,
    references: Vec<CatalogObjectRef>,
    replace: bool,
    if_not_exists: bool,
    with_data: bool,
    existing: Option<ViewId>,
}

enum ReferentialChange {
    Delete {
        table_id: TableId,
        old: Row,
    },
    Update {
        table_id: TableId,
        old: Row,
        new: Row,
    },
}

struct AdvancedExecution {
    table: BoundTable,
    joins: Vec<BoundJoin>,
    applies: Vec<BoundApply>,
    windows: Vec<BoundWindow>,
    schema: Schema,
    projection: Vec<BoundProjection>,
    distinct: bool,
    filter: Option<BoundExpr>,
    group_by: Vec<BoundExpr>,
    having: Option<BoundExpr>,
    order_by: Vec<BoundOrder>,
    offset: Option<BoundExpr>,
    limit: Option<BoundExpr>,
    aggregate: bool,
}

impl DatabaseState {
    fn from_persistent(
        state: PersistentState,
        data_format: DataFormat,
        statuses: &impl TransactionStatusProvider,
        next_transaction_id: u64,
    ) -> Result<Self> {
        let PersistentState {
            generation,
            catalog,
            tables,
            mut versions,
            indexes,
        } = state;
        if data_format == DataFormat::V2 {
            if !indexes.is_empty() {
                return Err(DbError::new(
                    "XX001",
                    "database v2 contains durable derived index state",
                ));
            }
            let catalog_table_ids = catalog
                .database()
                .schemas()
                .flat_map(|schema| schema.tables())
                .map(|table| table.id)
                .collect::<BTreeSet<_>>();
            if tables
                .keys()
                .chain(versions.keys())
                .any(|table_id| !catalog_table_ids.contains(table_id))
            {
                return Err(DbError::new(
                    "XX001",
                    "database v2 contains rows for a table absent from its catalog",
                ));
            }
            let snapshot_transaction = TransactionId::new(next_transaction_id)
                .ok_or_else(|| internal_error("transaction high-water mark is zero"))?;
            let visibility_snapshot = TransactionSnapshot {
                xmin: snapshot_transaction,
                xmax: snapshot_transaction,
                in_progress: Arc::new(BTreeSet::new()),
                command_id: u32::MAX,
            };
            let mut rows = BTreeMap::new();
            let mut version_rows = BTreeMap::new();
            let mut visible_versions = BTreeMap::new();
            for table_id in &catalog_table_ids {
                let table_versions = match versions.remove(table_id) {
                    Some(versions) => versions,
                    None => tables
                        .get(table_id)
                        .into_iter()
                        .flatten()
                        .enumerate()
                        .map(|(index, row)| {
                            let version_id = u32::try_from(index)
                                .ok()
                                .and_then(|index| index.checked_add(1))
                                .ok_or_else(|| {
                                    DbError::new(
                                        "54000",
                                        "table exceeds the u32 version ordinal limit",
                                    )
                                })?;
                            Ok(VersionedRow {
                                version_id,
                                header: TupleHeaderV2::frozen(row)?,
                                row: row.clone(),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                };
                let mut visible_rows = Vec::new();
                let mut visible_ids = Vec::new();
                for (index, version) in table_versions.iter().enumerate() {
                    let expected_version = u32::try_from(index)
                        .ok()
                        .and_then(|index| index.checked_add(1))
                        .ok_or_else(|| {
                            DbError::new("54000", "table exceeds the u32 version ordinal limit")
                        })?;
                    if version.version_id != expected_version
                        || version.header.previous_version >= version.version_id
                    {
                        return Err(DbError::new(
                            "XX001",
                            format!(
                                "table {} has an invalid version chain at ordinal {}",
                                table_id.get(),
                                version.version_id
                            ),
                        ));
                    }
                    if tuple_visible(
                        version.header,
                        &visibility_snapshot,
                        snapshot_transaction,
                        statuses,
                    )? {
                        visible_rows.push(version.row.clone());
                        visible_ids.push(version.version_id);
                    }
                }
                rows.insert(*table_id, Arc::new(visible_rows));
                version_rows.insert(*table_id, Arc::new(table_versions));
                visible_versions.insert(*table_id, Arc::new(visible_ids));
            }
            let mut state = Self {
                catalog: Arc::new(catalog),
                rows,
                system_catalog: None,
                versions: version_rows,
                visible_versions,
                indexes: BTreeMap::new(),
                searches: Arc::new(SearchCatalog::default()),
                generation,
                triggers_fired: 0,
                routine_frames: RoutineFrameStack::default(),
                pending_notices: Vec::new(),
                pending_notifications: NotificationTransactionState::default(),
                cancellation: None,
                authorization: None,
                sequence_currvals: BTreeMap::new(),
            };
            validate_database_rows(&state)?;
            for table_id in catalog_table_ids {
                rebuild_table_derived(&mut state, table_id)?;
            }
            return Ok(state);
        }
        let indexes = indexes
            .into_iter()
            .map(|(index_id, entries)| {
                let definition = catalog
                    .index_by_id(index_id)
                    .ok_or_else(|| internal_error("persistent index is absent from the catalog"))?;
                BPlusTree::from_entries(definition.unique, entries)
                    .map(|tree| (index_id, Arc::new(tree)))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let rows = tables
            .into_iter()
            .map(|(table_id, rows)| (table_id, Arc::new(rows)))
            .collect::<BTreeMap<_, _>>();
        let (versions, visible_versions) = frozen_version_state(&rows)?;
        let searches = SearchCatalog::build(&catalog, &rows, SearchLimits::default())?;
        Ok(Self {
            catalog: Arc::new(catalog),
            rows,
            system_catalog: None,
            versions,
            visible_versions,
            indexes,
            searches: Arc::new(searches),
            generation,
            triggers_fired: 0,
            routine_frames: RoutineFrameStack::default(),
            pending_notices: Vec::new(),
            pending_notifications: NotificationTransactionState::default(),
            cancellation: None,
            authorization: None,
            sequence_currvals: BTreeMap::new(),
        })
    }

    fn from_logical_snapshot(snapshot: LogicalDatabaseSnapshot) -> Result<Self> {
        if snapshot.format_version != LOGICAL_SNAPSHOT_VERSION {
            return Err(DbError::new(
                "0A000",
                format!(
                    "logical snapshot version {} is not supported",
                    snapshot.format_version
                ),
            )
            .with_hint("use a compatible OrdaDB version or perform an explicit migration"));
        }
        let catalog_table_ids = snapshot
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .map(|table| table.id)
            .collect::<BTreeSet<_>>();
        if snapshot
            .tables
            .keys()
            .any(|table_id| !catalog_table_ids.contains(table_id))
        {
            return Err(DbError::new(
                "XX001",
                "logical snapshot contains rows for a table absent from its catalog",
            ));
        }
        let rows = catalog_table_ids
            .iter()
            .map(|table_id| {
                (
                    *table_id,
                    snapshot
                        .tables
                        .get(table_id)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(Vec::new())),
                )
            })
            .collect();
        let mut state = Self {
            catalog: snapshot.catalog,
            rows,
            system_catalog: None,
            versions: BTreeMap::new(),
            visible_versions: BTreeMap::new(),
            indexes: BTreeMap::new(),
            searches: Arc::new(SearchCatalog::default()),
            generation: snapshot.source_generation,
            triggers_fired: 0,
            routine_frames: RoutineFrameStack::default(),
            pending_notices: Vec::new(),
            pending_notifications: NotificationTransactionState::default(),
            cancellation: None,
            authorization: None,
            sequence_currvals: BTreeMap::new(),
        };
        (state.versions, state.visible_versions) = frozen_version_state(&state.rows)?;
        validate_database_rows(&state)?;
        for table_id in catalog_table_ids {
            rebuild_table_derived(&mut state, table_id)?;
        }
        Ok(state)
    }
}

type VersionRowsByTable = BTreeMap<TableId, Arc<Vec<VersionedRow>>>;
type VisibleVersionsByTable = BTreeMap<TableId, Arc<Vec<u32>>>;

fn frozen_version_state(
    rows: &BTreeMap<TableId, Arc<Vec<Row>>>,
) -> Result<(VersionRowsByTable, VisibleVersionsByTable)> {
    let mut versions = BTreeMap::new();
    let mut visible_versions = BTreeMap::new();
    for (table_id, table_rows) in rows {
        let mut table_versions = Vec::with_capacity(table_rows.len());
        let mut visible_ids = Vec::with_capacity(table_rows.len());
        for (index, row) in table_rows.iter().enumerate() {
            let version_id = u32::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .ok_or_else(|| {
                    DbError::new("54000", "table exceeds the u32 version ordinal limit")
                })?;
            table_versions.push(VersionedRow {
                version_id,
                header: TupleHeaderV2::frozen(row)?,
                row: row.clone(),
            });
            visible_ids.push(version_id);
        }
        versions.insert(*table_id, Arc::new(table_versions));
        visible_versions.insert(*table_id, Arc::new(visible_ids));
    }
    Ok((versions, visible_versions))
}

impl From<&DatabaseState> for PersistentState {
    fn from(state: &DatabaseState) -> Self {
        Self {
            generation: state.generation,
            catalog: (*state.catalog).clone(),
            tables: state
                .rows
                .iter()
                .map(|(table_id, rows)| (*table_id, (**rows).clone()))
                .collect(),
            versions: state
                .versions
                .iter()
                .map(|(table_id, versions)| (*table_id, (**versions).clone()))
                .collect(),
            indexes: state
                .indexes
                .iter()
                .map(|(index_id, tree)| (*index_id, tree.iter().cloned().collect::<Vec<_>>()))
                .collect(),
        }
    }
}

fn committed_snapshot(state: &Arc<RwLock<DatabaseState>>) -> Result<DatabaseState> {
    state
        .read()
        .map(|state| state.clone())
        .map_err(|_| internal_error("engine state lock is poisoned"))
}

fn project_database_visibility(
    mut state: DatabaseState,
    snapshot: &TransactionSnapshot,
    current_transaction: TransactionId,
    statuses: &impl TransactionStatusProvider,
) -> Result<DatabaseState> {
    let table_ids = state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .map(|table| table.id)
        .collect::<Vec<_>>();
    state.rows.clear();
    state.visible_versions.clear();
    state.indexes.clear();
    state.searches = Arc::new(SearchCatalog::default());
    for table_id in &table_ids {
        let versions = state
            .versions
            .get(table_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()));
        let mut rows = Vec::new();
        let mut visible_versions = Vec::new();
        for version in versions.iter() {
            if tuple_visible(version.header, snapshot, current_transaction, statuses)? {
                rows.push(version.row.clone());
                visible_versions.push(version.version_id);
            }
        }
        state.rows.insert(*table_id, Arc::new(rows));
        state
            .visible_versions
            .insert(*table_id, Arc::new(visible_versions));
    }
    validate_database_rows(&state)?;
    for table_id in table_ids {
        rebuild_table_derived(&mut state, table_id)?;
    }
    Ok(state)
}

fn parsed_is_transaction_control(statement: &ParsedStatement) -> bool {
    matches!(
        statement,
        ParsedStatement::Begin { .. }
            | ParsedStatement::Commit { .. }
            | ParsedStatement::Rollback { .. }
            | ParsedStatement::Savepoint { .. }
            | ParsedStatement::RollbackTo { .. }
            | ParsedStatement::ReleaseSavepoint { .. }
    )
}

/// Bridges the execution scan contract to authoritative v2 heap pages.
pub struct StorageTableProviderV2<'a> {
    store: Arc<Mutex<DatabaseStore>>,
    storage_access: Arc<StorageAccessGate>,
    generation: u64,
    rows: &'a BTreeMap<TableId, Arc<Vec<Row>>>,
    system_catalog: Option<&'a system_catalog::SystemCatalogSnapshot>,
}

impl<'a> StorageTableProviderV2<'a> {
    fn new(
        store: Arc<Mutex<DatabaseStore>>,
        storage_access: Arc<StorageAccessGate>,
        generation: u64,
        rows: &'a BTreeMap<TableId, Arc<Vec<Row>>>,
        system_catalog: Option<&'a system_catalog::SystemCatalogSnapshot>,
    ) -> Self {
        Self {
            store,
            storage_access,
            generation,
            rows,
            system_catalog,
        }
    }
}

impl TableProvider for StorageTableProviderV2<'_> {
    fn scan(&self, table_id: TableId) -> Result<Box<dyn TableScan>> {
        if Catalog::is_system_table(table_id) {
            return self
                .system_catalog
                .ok_or_else(|| internal_error("system catalog snapshot is unavailable"))?
                .scan(table_id);
        }
        let lease = self.storage_access.acquire_read()?;
        let cursor = self
            .store
            .lock()
            .map_err(|_| internal_error("database store lock is poisoned"))?
            .open_table_cursor_v2(table_id, self.generation)?;
        let rows = self
            .rows
            .get(&table_id)
            .cloned()
            .ok_or_else(|| internal_error("v2 table is absent from the resident snapshot"))?;
        let resident_rows = u64::try_from(rows.len()).map_err(|_| {
            DbError::new("54000", "resident table row count exceeds storage limits")
        })?;
        if resident_rows != cursor.expected_visible_rows() {
            return Err(DbError::new(
                "XX001",
                "resident visible row count does not match v2 heap metadata",
            )
            .with_detail(format!(
                "table {} has {resident_rows} visible rows but the v2 heap declares {}",
                table_id.get(),
                cursor.expected_visible_rows()
            )));
        }
        Ok(Box::new(StorageTableScanV2 {
            _cursor: cursor,
            rows,
            offset: 0,
            lease: Some(lease),
        }))
    }
}

struct StorageTableScanV2 {
    _cursor: StorageTableCursorV2,
    rows: Arc<Vec<Row>>,
    offset: usize,
    lease: Option<StorageReadLease>,
}

impl TableScan for StorageTableScanV2 {
    fn next_chunk(
        &mut self,
        max_rows: usize,
        grant: &MemoryGrant,
    ) -> Result<Option<LeasedDataChunk>> {
        if max_rows == 0 {
            self.lease = None;
            return Err(DbError::new(
                "22023",
                "table scan chunk size must be positive",
            ));
        }
        let expected_rows = self.rows.len();
        if self.offset >= expected_rows {
            self.lease = None;
            return Ok(None);
        }
        let end = self.offset.saturating_add(max_rows).min(expected_rows);
        match LeasedDataChunk::from_snapshot(Arc::clone(&self.rows), self.offset, end, grant) {
            Ok(chunk) => {
                self.offset = end;
                if self.offset == expected_rows {
                    self.lease = None;
                }
                Ok(Some(chunk))
            }
            Err(error) => {
                self.lease = None;
                Err(error)
            }
        }
    }
}

fn prepare_read_stream(
    state: &DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    table_provider: Option<&dyn TableProvider>,
) -> Result<Option<TryQueryStream>> {
    prepare_read_stream_with_options(
        state,
        statement,
        params,
        table_provider,
        &ExecutionOptions::default(),
    )
}

fn prepare_read_stream_with_options(
    state: &DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    table_provider: Option<&dyn TableProvider>,
    options: &ExecutionOptions,
) -> Result<Option<TryQueryStream>> {
    match statement {
        BoundStatement::Select {
            table_id,
            schema,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => {
            let (schema, cursor) = prepare_select_cursor(
                state,
                SelectExecution {
                    table_id,
                    schema,
                    projection,
                    filter,
                    order_by,
                    offset,
                    limit,
                },
                params,
                table_provider,
                options,
            )?;
            Ok(Some(TryQueryStream::select(
                schema,
                StreamBatchCursor::Simple(Box::new(cursor)),
                state.cancellation.clone(),
            )))
        }
        BoundStatement::AdvancedSelect {
            table,
            joins,
            applies,
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
        } => {
            let (schema, cursor) = prepare_advanced_cursor(
                state,
                AdvancedExecution {
                    table,
                    joins,
                    applies,
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
                },
                params,
                options,
            )?;
            Ok(Some(TryQueryStream::select(
                schema,
                StreamBatchCursor::Advanced(Box::new(cursor)),
                state.cancellation.clone(),
            )))
        }
        _ => Ok(None),
    }
}

#[derive(Debug, Clone, Copy)]
struct VersionMutationContext {
    transaction_id: TransactionId,
    command_id: u32,
}

#[derive(Clone, Copy)]
struct StatementExecutionContext<'a> {
    dialect: SqlDialect,
    runtime_metadata: &'a SessionRuntimeMetadata,
    authorization: Option<&'a SessionAuthorization>,
}

#[derive(Clone, Copy)]
struct MaintenanceContext<'a> {
    horizon: TransactionId,
    expired_snapshot: Option<TransactionId>,
    statuses: &'a TransactionStatusStore,
}

fn maintenance_context<'a>(
    transactions: &TransactionManager,
    statuses: &'a TransactionStatusStore,
) -> Result<MaintenanceContext<'a>> {
    Ok(MaintenanceContext {
        horizon: transactions.global_xmin()?,
        expired_snapshot: transactions.expired_snapshot()?,
        statuses,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementWriteScope {
    ReadOnly,
    Dml,
    Exclusive,
}

fn statement_write_scope(statement: &BoundStatement) -> StatementWriteScope {
    match statement {
        BoundStatement::Insert { .. }
        | BoundStatement::ViewInsert { .. }
        | BoundStatement::Merge(_)
        | BoundStatement::Update { .. }
        | BoundStatement::ViewUpdate { .. }
        | BoundStatement::Delete { .. }
        | BoundStatement::ViewDelete { .. } => StatementWriteScope::Dml,
        BoundStatement::Select { .. }
        | BoundStatement::AdvancedSelect { .. }
        | BoundStatement::SetOperation { .. }
        | BoundStatement::With { .. }
        | BoundStatement::ViewSelect { .. }
        | BoundStatement::ScalarSelect { .. }
        | BoundStatement::RoutineSelect { .. }
        | BoundStatement::Explain { .. }
        | BoundStatement::NoOp { .. } => StatementWriteScope::ReadOnly,
        _ => StatementWriteScope::Exclusive,
    }
}

fn reject_system_catalog_write(statement: &BoundStatement) -> Result<()> {
    let mut pending = vec![statement];
    while let Some(statement) = pending.pop() {
        let target = match statement {
            BoundStatement::Insert { table_id, .. }
            | BoundStatement::Update { table_id, .. }
            | BoundStatement::Delete { table_id, .. } => Some(*table_id),
            BoundStatement::Merge(merge) => Some(merge.target.table_id),
            BoundStatement::With { body, .. } => {
                pending.push(body);
                None
            }
            _ => None,
        };
        if target.is_some_and(Catalog::is_system_table) {
            return Err(
                DbError::new("42501", "system catalog relations are read-only")
                    .with_hint("query pg_catalog and information_schema with SELECT"),
            );
        }
    }
    Ok(())
}

fn statement_read_predicates(statement: &BoundStatement) -> Vec<PredicateLock> {
    statement_read_table_ids(statement)
        .into_iter()
        .map(|table_id| PredicateLock::Table {
            table_id: table_id.get(),
        })
        .collect()
}

fn statement_read_table_ids(statement: &BoundStatement) -> BTreeSet<TableId> {
    let mut table_ids = BTreeSet::new();
    let mut pending = vec![statement];
    while let Some(statement) = pending.pop() {
        match statement {
            BoundStatement::Select { table_id, .. } => {
                table_ids.insert(*table_id);
            }
            BoundStatement::AdvancedSelect {
                table,
                joins,
                applies,
                ..
            } => {
                table_ids.insert(table.table_id);
                for join in joins {
                    match &join.source {
                        BoundJoinSource::Table(table) => {
                            table_ids.insert(table.table_id);
                        }
                        BoundJoinSource::Derived { query, .. } => pending.push(query),
                    }
                }
                pending.extend(applies.iter().map(|apply| apply.query.as_ref()));
            }
            BoundStatement::SetOperation { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundStatement::With { ctes, body, .. } => {
                pending.push(body);
                for cte in ctes {
                    pending.push(&cte.seed);
                    if let Some(recursive) = &cte.recursive {
                        pending.push(recursive);
                    }
                }
            }
            BoundStatement::Merge(merge) => {
                table_ids.insert(merge.target.table_id);
                table_ids.insert(merge.source.table_id);
            }
            BoundStatement::ViewSelect { source, .. }
            | BoundStatement::ViewInsert { source, .. }
            | BoundStatement::ViewUpdate { source, .. }
            | BoundStatement::ViewDelete { source, .. }
            | BoundStatement::Explain { statement: source } => {
                pending.push(source);
            }
            _ => {}
        }
    }
    table_ids
}

fn changed_table_ids(before: &DatabaseState, after: &DatabaseState) -> BTreeSet<TableId> {
    before
        .versions
        .keys()
        .chain(after.versions.keys())
        .copied()
        .filter(|table_id| {
            before.versions.get(table_id) != after.versions.get(table_id)
                || before.visible_versions.get(table_id) != after.visible_versions.get(table_id)
        })
        .collect()
}

fn acquire_compatibility_write_lock(
    locks: &Arc<LockManager>,
    transaction: &DurableTransaction,
    cancelled: Option<&AtomicBool>,
) -> Result<LockGuard> {
    locks.acquire(
        transaction.transaction_id(),
        LockKey::Database,
        LockMode::Exclusive,
        None,
        cancelled,
    )
}

fn acquire_dml_locks(
    locks: &Arc<LockManager>,
    transaction: &DurableTransaction,
    before: &DatabaseState,
    after: &DatabaseState,
    existing: &[LockGuard],
    cancelled: Option<&AtomicBool>,
) -> Result<Vec<LockGuard>> {
    let mut keys = BTreeSet::from([LockKey::Database]);
    let transaction_id = transaction.transaction_id();
    let table_ids = before
        .versions
        .keys()
        .chain(after.versions.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for table_id in table_ids {
        let before_versions = before
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        let after_versions = after
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        let changed = before_versions != after_versions
            || before.visible_versions.get(&table_id) != after.visible_versions.get(&table_id);
        if !changed {
            continue;
        }
        keys.insert(LockKey::Table {
            table_id: table_id.get(),
        });
        for (before_version, after_version) in before_versions.iter().zip(after_versions) {
            if before_version.header.xmax == 0 && after_version.header.xmax == transaction_id.get()
            {
                keys.insert(LockKey::Row {
                    table_id: table_id.get(),
                    version_id: u64::from(before_version.version_id),
                });
            }
        }
        let before_visible = before
            .visible_versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice())
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let Some(table) = after.catalog.table_by_id(table_id) else {
            continue;
        };
        let after_rows = after
            .rows
            .get(&table_id)
            .map_or(&[][..], |rows| rows.as_slice());
        let after_visible = after
            .visible_versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        for (row, version_id) in after_rows.iter().zip(after_visible) {
            if before_visible.contains(version_id) {
                continue;
            }
            for index in table.indexes().filter(|index| index.unique) {
                let key_values = index
                    .key_columns
                    .iter()
                    .map(|column_id| {
                        table
                            .column_index_by_id(*column_id)
                            .and_then(|position| row.values.get(position))
                            .cloned()
                            .ok_or_else(|| {
                                internal_error("unique-index lock column is outside its row")
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if key_values.iter().any(Value::is_null) {
                    continue;
                }
                let encoded = serde_json::to_vec(&key_values)
                    .map_err(|error| internal_error(error.to_string()))?;
                let fingerprint: [u8; 32] = Sha256::digest(encoded).into();
                keys.insert(LockKey::IndexKey {
                    index_id: index.id.get(),
                    fingerprint,
                });
            }
        }
    }
    let existing = existing
        .iter()
        .map(|guard| guard.key().clone())
        .collect::<BTreeSet<_>>();
    let mut acquired = Vec::new();
    for key in keys {
        if existing.contains(&key) {
            continue;
        }
        let mode = if key == LockKey::Database || matches!(key, LockKey::Table { .. }) {
            LockMode::Shared
        } else {
            LockMode::Exclusive
        };
        acquired.push(locks.acquire(transaction_id, key, mode, None, cancelled)?);
    }
    Ok(acquired)
}

fn version_mutation_context(transaction: &DurableTransaction) -> Result<VersionMutationContext> {
    Ok(VersionMutationContext {
        transaction_id: transaction.transaction_id(),
        command_id: transaction
            .snapshot()
            .ok_or_else(|| DbError::new("25P01", "transaction is no longer active"))?
            .command_id,
    })
}

fn execute_bound_candidate(
    state: &DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    authorization: Option<&SessionAuthorization>,
    version_context: Option<VersionMutationContext>,
    maintenance: MaintenanceContext<'_>,
) -> Result<(DatabaseState, Vec<QueryEvent>, bool)> {
    let mut candidate = state.clone();
    candidate.triggers_fired = 0;
    candidate.routine_frames.clear();
    candidate.pending_notices.clear();
    candidate.pending_notifications = NotificationTransactionState::default();
    candidate.authorization = authorization.cloned();
    let reconciles_versions = !matches!(
        &statement,
        BoundStatement::Analyze { .. } | BoundStatement::Vacuum { .. }
    );
    let (mut events, dirty) = execute_root_bound(&mut candidate, statement, params, maintenance)?;
    insert_pending_notices(&mut events, mem::take(&mut candidate.pending_notices));
    if dirty
        && reconciles_versions
        && let Some(version_context) = version_context
    {
        reconcile_version_changes(state, &mut candidate, version_context)?;
    }
    candidate.cancellation = None;
    candidate.authorization = None;
    Ok((candidate, events, dirty))
}

fn execute_candidate(
    state: &DatabaseState,
    sql: &str,
    params: &[Value],
    context: StatementExecutionContext<'_>,
    version_context: Option<VersionMutationContext>,
    maintenance: MaintenanceContext<'_>,
) -> Result<(DatabaseState, Vec<QueryEvent>, bool)> {
    let parsed = parse_with_dialect(sql, context.dialect)?;
    let statement = bind_with_session(
        parsed,
        &state.catalog,
        context.runtime_metadata.bind_values(),
    )?;
    let mut candidate = state.clone();
    candidate.triggers_fired = 0;
    candidate.routine_frames.clear();
    candidate.pending_notices.clear();
    candidate.pending_notifications = NotificationTransactionState::default();
    candidate.authorization = context.authorization.cloned();
    let reconciles_versions = !matches!(
        &statement,
        BoundStatement::Analyze { .. } | BoundStatement::Vacuum { .. }
    );
    let (mut events, dirty) = execute_root_bound(&mut candidate, statement, params, maintenance)?;
    insert_pending_notices(&mut events, mem::take(&mut candidate.pending_notices));
    if dirty
        && reconciles_versions
        && let Some(version_context) = version_context
    {
        reconcile_version_changes(state, &mut candidate, version_context)?;
    }
    candidate.cancellation = None;
    candidate.authorization = None;
    Ok((candidate, events, dirty))
}

fn insert_pending_notices(events: &mut Vec<QueryEvent>, notices: Vec<DbNotice>) {
    if notices.is_empty() {
        return;
    }
    let position = usize::from(matches!(events.first(), Some(QueryEvent::Schema(_))));
    events.splice(
        position..position,
        notices.into_iter().map(QueryEvent::Notice),
    );
}

fn reconcile_version_changes(
    before: &DatabaseState,
    after: &mut DatabaseState,
    context: VersionMutationContext,
) -> Result<()> {
    let table_ids = before
        .rows
        .keys()
        .chain(after.rows.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for table_id in table_ids {
        let Some(after_rows) = after.rows.get(&table_id).map(|rows| (**rows).clone()) else {
            after.versions.remove(&table_id);
            after.visible_versions.remove(&table_id);
            continue;
        };
        let before_rows = before
            .rows
            .get(&table_id)
            .map(|rows| (**rows).clone())
            .unwrap_or_default();
        let before_ids = before
            .visible_versions
            .get(&table_id)
            .map(|versions| (**versions).clone())
            .unwrap_or_default();
        if before_rows.len() != before_ids.len() {
            return Err(internal_error(format!(
                "table {} visible row/version state is not aligned",
                table_id.get()
            )));
        }
        let mut versions = before
            .versions
            .get(&table_id)
            .map(|versions| (**versions).clone())
            .unwrap_or_default();
        let visible_ids = reconcile_table_version_changes(
            &before_rows,
            &before_ids,
            &after_rows,
            &mut versions,
            context,
        )?;
        after.versions.insert(table_id, Arc::new(versions));
        after
            .visible_versions
            .insert(table_id, Arc::new(visible_ids));
    }
    Ok(())
}

fn reconcile_table_version_changes(
    before_rows: &[Row],
    before_ids: &[u32],
    after_rows: &[Row],
    versions: &mut Vec<VersionedRow>,
    context: VersionMutationContext,
) -> Result<Vec<u32>> {
    if before_rows.len() == after_rows.len() {
        return before_rows
            .iter()
            .zip(before_ids)
            .zip(after_rows)
            .map(|((before, version_id), after)| {
                if before == after {
                    Ok(*version_id)
                } else {
                    update_version(versions, *version_id, after, context)
                }
            })
            .collect();
    }
    if is_subsequence(before_rows, after_rows) {
        let mut before_index = 0_usize;
        let mut visible = Vec::with_capacity(after_rows.len());
        for row in after_rows {
            if before_index < before_rows.len() && row == &before_rows[before_index] {
                visible.push(before_ids[before_index]);
                before_index += 1;
            } else {
                visible.push(append_version(versions, row, 0, context)?);
            }
        }
        return Ok(visible);
    }
    if is_subsequence(after_rows, before_rows) {
        let mut before_index = 0_usize;
        let mut visible = Vec::with_capacity(after_rows.len());
        for row in after_rows {
            while before_index < before_rows.len() && &before_rows[before_index] != row {
                delete_version(versions, before_ids[before_index], context)?;
                before_index += 1;
            }
            if before_index == before_rows.len() {
                return Err(internal_error(
                    "row subsequence changed while deriving version deletes",
                ));
            }
            visible.push(before_ids[before_index]);
            before_index += 1;
        }
        for version_id in &before_ids[before_index..] {
            delete_version(versions, *version_id, context)?;
        }
        return Ok(visible);
    }

    let shared = before_rows.len().min(after_rows.len());
    let mut visible = Vec::with_capacity(after_rows.len());
    for index in 0..shared {
        if before_rows[index] == after_rows[index] {
            visible.push(before_ids[index]);
        } else {
            visible.push(update_version(
                versions,
                before_ids[index],
                &after_rows[index],
                context,
            )?);
        }
    }
    for version_id in &before_ids[shared..] {
        delete_version(versions, *version_id, context)?;
    }
    for row in &after_rows[shared..] {
        visible.push(append_version(versions, row, 0, context)?);
    }
    Ok(visible)
}

fn is_subsequence(needle: &[Row], haystack: &[Row]) -> bool {
    let mut index = 0_usize;
    for row in haystack {
        if index < needle.len() && row == &needle[index] {
            index += 1;
        }
    }
    index == needle.len()
}

fn update_version(
    versions: &mut Vec<VersionedRow>,
    previous_version: u32,
    row: &Row,
    context: VersionMutationContext,
) -> Result<u32> {
    delete_version(versions, previous_version, context)?;
    append_version(versions, row, previous_version, context)
}

fn delete_version(
    versions: &mut [VersionedRow],
    version_id: u32,
    context: VersionMutationContext,
) -> Result<()> {
    let version = version_id
        .checked_sub(1)
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| versions.get_mut(index))
        .ok_or_else(|| internal_error("visible version ID is outside its table version state"))?;
    if version.header.xmax != 0 {
        return Err(DbError::new(
            "40001",
            "tuple version changed since the transaction snapshot",
        )
        .with_hint("retry the transaction with a fresh snapshot"));
    }
    version.header.xmax = context.transaction_id.get();
    version.header.command_id = context.command_id;
    Ok(())
}

fn append_version(
    versions: &mut Vec<VersionedRow>,
    row: &Row,
    previous_version: u32,
    context: VersionMutationContext,
) -> Result<u32> {
    let version_id = u32::try_from(versions.len())
        .ok()
        .and_then(|version_id| version_id.checked_add(1))
        .ok_or_else(|| DbError::new("54000", "table version ordinal space is exhausted"))?;
    if previous_version >= version_id {
        return Err(internal_error(
            "new tuple predecessor is not earlier than its version ordinal",
        ));
    }
    let mut header = TupleHeaderV2::frozen(row)?;
    header.xmin = context.transaction_id.get();
    header.command_id = context.command_id;
    header.previous_version = previous_version;
    versions.push(VersionedRow {
        version_id,
        header,
        row: row.clone(),
    });
    Ok(version_id)
}

fn merge_dml_candidate(
    latest: &DatabaseState,
    base: &DatabaseState,
    candidate: &DatabaseState,
    transaction: &DurableTransaction,
    statuses: &TransactionStatusStore,
) -> Result<DatabaseState> {
    let transaction_id = transaction.transaction_id();
    let mut merged = latest.clone();
    let table_ids = base
        .versions
        .keys()
        .chain(candidate.versions.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for table_id in table_ids {
        let base_versions = base
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        let candidate_versions = candidate
            .versions
            .get(&table_id)
            .map_or(&[][..], |versions| versions.as_slice());
        if base_versions == candidate_versions
            && base.visible_versions.get(&table_id) == candidate.visible_versions.get(&table_id)
        {
            continue;
        }
        if candidate_versions.len() < base_versions.len() {
            return Err(internal_error(
                "DML candidate removed authoritative tuple versions",
            ));
        }
        let mut latest_versions = merged
            .versions
            .get(&table_id)
            .map(|versions| (**versions).clone())
            .ok_or_else(|| {
                DbError::new(
                    "40001",
                    format!("table {} changed during the transaction", table_id.get()),
                )
            })?;
        if latest_versions.len() < base_versions.len() {
            return Err(internal_error(
                "latest tuple-version state is shorter than the transaction base",
            ));
        }
        for (index, (base_version, candidate_version)) in
            base_versions.iter().zip(candidate_versions).enumerate()
        {
            if base_version == candidate_version {
                continue;
            }
            if base_version.row != candidate_version.row
                || base_version.version_id != candidate_version.version_id
                || base_version.header.xmax != 0
                || candidate_version.header.xmax != transaction_id.get()
            {
                return Err(internal_error(
                    "DML candidate changed an existing tuple outside its deletion header",
                ));
            }
            let latest_version = latest_versions
                .get_mut(index)
                .ok_or_else(|| internal_error("latest tuple version disappeared during merge"))?;
            if latest_version != base_version {
                return Err(DbError::new(
                    "40001",
                    "tuple version changed since the transaction snapshot",
                )
                .with_hint("retry the transaction with a fresh snapshot"));
            }
            latest_version.header = candidate_version.header;
        }
        let mut remapped = BTreeMap::<u32, u32>::new();
        for candidate_version in &candidate_versions[base_versions.len()..] {
            if candidate_version.header.xmin != transaction_id.get() {
                return Err(internal_error(
                    "DML candidate appended a version owned by another transaction",
                ));
            }
            let version_id = u32::try_from(latest_versions.len())
                .ok()
                .and_then(|version_id| version_id.checked_add(1))
                .ok_or_else(|| DbError::new("54000", "table version ordinal space is exhausted"))?;
            let mut appended = candidate_version.clone();
            let previous = appended.header.previous_version;
            if previous > u32::try_from(base_versions.len()).unwrap_or(u32::MAX) {
                appended.header.previous_version = *remapped
                    .get(&previous)
                    .ok_or_else(|| internal_error("DML candidate predecessor was not remapped"))?;
            }
            appended.version_id = version_id;
            remapped.insert(candidate_version.version_id, version_id);
            latest_versions.push(appended);
        }
        merged.versions.insert(table_id, Arc::new(latest_versions));
    }
    project_current_database_visibility(merged, transaction_id, statuses)
}

fn refresh_read_committed_candidate(
    state: &Arc<RwLock<DatabaseState>>,
    statuses: &TransactionStatusStore,
    transaction: &DurableTransaction,
    base: &mut Option<DatabaseState>,
    working: &mut Option<DatabaseState>,
    dml_only: bool,
) -> Result<()> {
    if !dml_only
        || transaction.characteristics().is_none_or(|characteristics| {
            characteristics.isolation_level != IsolationLevel::ReadCommitted
        })
        || working.is_none()
    {
        return Ok(());
    }
    let previous_base = base
        .as_ref()
        .ok_or_else(|| internal_error("DML transaction is missing its base snapshot"))?;
    let previous_working = working
        .as_ref()
        .ok_or_else(|| internal_error("DML transaction is missing its working state"))?;
    let transaction_snapshot = transaction
        .snapshot()
        .ok_or_else(|| no_active_transaction_error("refresh a statement snapshot"))?;
    let refreshed_base = project_database_visibility(
        committed_snapshot(state)?,
        transaction_snapshot,
        transaction.transaction_id(),
        statuses,
    )?;
    let refreshed_working = merge_dml_candidate(
        &refreshed_base,
        previous_base,
        previous_working,
        transaction,
        statuses,
    )?;
    *base = Some(refreshed_base);
    *working = Some(refreshed_working);
    Ok(())
}

fn read_committed_statement_state(
    state: &Arc<RwLock<DatabaseState>>,
    statuses: &TransactionStatusStore,
    transaction: &mut DurableTransaction,
    base: Option<&DatabaseState>,
    working: Option<&DatabaseState>,
    cancellation: Option<Arc<AtomicBool>>,
) -> Result<DatabaseState> {
    let transaction_snapshot = transaction.begin_statement()?.clone();
    let mut latest = project_database_visibility(
        committed_snapshot(state)?,
        &transaction_snapshot,
        transaction.transaction_id(),
        statuses,
    )?;
    latest.cancellation = cancellation;
    match (base, working) {
        (Some(base), Some(working)) => {
            merge_dml_candidate(&latest, base, working, transaction, statuses)
        }
        (None, None) => Ok(latest),
        _ => Err(internal_error(
            "DML transaction has incomplete base/working state during conflict recheck",
        )),
    }
}

struct DmlUpgradeAuthorities<'a> {
    state: &'a Arc<RwLock<DatabaseState>>,
    statuses: &'a TransactionStatusStore,
    locks: &'a Arc<LockManager>,
    writer: &'a Arc<WriterCoordinator>,
}

fn upgrade_dml_candidate_to_exclusive(
    authorities: DmlUpgradeAuthorities<'_>,
    transaction: &DurableTransaction,
    base: &DatabaseState,
    working: &DatabaseState,
    cancellation: Option<Arc<AtomicBool>>,
) -> Result<(DatabaseState, DatabaseState, WriterLease, LockGuard)> {
    let lease = authorities
        .writer
        .try_acquire(transaction.transaction_id())?;
    let lock =
        acquire_compatibility_write_lock(authorities.locks, transaction, cancellation.as_deref())?;
    let mut latest = committed_snapshot(authorities.state)?;
    latest.cancellation = cancellation;
    let characteristics = transaction
        .characteristics()
        .ok_or_else(|| no_active_transaction_error("upgrade transaction locks"))?;
    let upgraded = if characteristics.isolation_level == IsolationLevel::ReadCommitted {
        merge_dml_candidate(&latest, base, working, transaction, authorities.statuses)?
    } else {
        if latest.generation != base.generation {
            return Err(DbError::new(
                "40001",
                "could not serialize DDL after concurrent database changes",
            )
            .with_hint("retry the transaction"));
        }
        let mut working = working.clone();
        working.cancellation = latest.cancellation.clone();
        working
    };
    Ok((latest, upgraded, lease, lock))
}

fn project_current_database_visibility(
    state: DatabaseState,
    current_transaction: TransactionId,
    statuses: &TransactionStatusStore,
) -> Result<DatabaseState> {
    let status = statuses.snapshot()?;
    let xmax = TransactionId::new(status.next_transaction_id)
        .ok_or_else(|| internal_error("transaction status high-water mark is zero"))?;
    let snapshot = TransactionSnapshot {
        xmin: xmax,
        xmax,
        in_progress: Arc::new(BTreeSet::new()),
        command_id: u32::MAX,
    };
    project_database_visibility(state, &snapshot, current_transaction, statuses)
}

fn persist_candidate(
    state: &mut DatabaseState,
    store: &Arc<Mutex<DatabaseStore>>,
    storage_access: &Arc<StorageAccessGate>,
    wal: &Arc<WalManager>,
    transaction: &mut DurableTransaction,
    mut candidate: DatabaseState,
) -> Result<()> {
    candidate.sequence_currvals.clear();
    candidate.generation = state.generation.checked_add(1).ok_or_else(|| {
        DbError::new("54000", "database generation space is exhausted")
            .with_hint("create a logical backup before retrying on a fresh database")
    })?;
    let persistent = PersistentState::from(&candidate);
    let _storage_write_lease = storage_access.acquire_write()?;
    let mut store = store
        .lock()
        .map_err(|_| internal_error("database store lock is poisoned"))?;
    let mut prepared = store.prepare_commit(&persistent)?;
    let transaction_id = transaction.transaction_id();
    let logged = wal.log_prepared(transaction_id, &mut prepared)?;
    transaction.mark_status_committed()?;
    store.apply_prepared_with_observer(&prepared, |point| {
        wal.check_fault(match point {
            ApplyPoint::BeforePageWrite(_) => FaultPoint::BeforeDataPageWrite,
            ApplyPoint::AfterPageWrite(_) => FaultPoint::AfterDataPageWrite,
            ApplyPoint::BeforeResize { .. } => FaultPoint::BeforeDataResize,
            ApplyPoint::AfterResize { .. } => FaultPoint::AfterDataResize,
            ApplyPoint::BeforeSync => FaultPoint::BeforeDataSync,
            ApplyPoint::AfterSync => FaultPoint::AfterDataSync,
        })
    })?;
    wal.commit(&logged)?;
    store.publish_prepared(prepared)?;
    transaction.finish_commit()?;
    *state = candidate;
    Ok(())
}

fn checkpoint_shared(
    state: &Arc<RwLock<DatabaseState>>,
    store: &Arc<Mutex<DatabaseStore>>,
    wal: &Arc<WalManager>,
    transactions: &Arc<TransactionManager>,
) -> Result<()> {
    let durable_data_generation = state
        .read()
        .map_err(|_| internal_error("engine state lock is poisoned"))?
        .generation;
    let data_file_page_count = store
        .lock()
        .map_err(|_| internal_error("database store lock is poisoned"))?
        .page_count()?;
    let mut active_transactions = BTreeMap::new();
    for transaction_id in transactions.active_transactions()? {
        if let Some(last_lsn) = wal.last_lsn(transaction_id)? {
            active_transactions.insert(transaction_id, last_lsn);
        }
    }
    wal.checkpoint(CheckpointState {
        active_transactions,
        dirty_pages: wal.dirty_pages()?,
        visibility_horizon: Some(transactions.global_xmin()?),
        durable_data_generation,
        durable_wal_lsn: wal.durable_lsn()?,
        data_file_page_count,
    })?;
    Ok(())
}

fn record_commit_and_maybe_checkpoint(
    state: &Arc<RwLock<DatabaseState>>,
    store: &Arc<Mutex<DatabaseStore>>,
    wal: &Arc<WalManager>,
    transactions: &Arc<TransactionManager>,
    commits_since_checkpoint: &AtomicU64,
) -> Result<()> {
    let count = commits_since_checkpoint
        .fetch_add(1, Ordering::AcqRel)
        .checked_add(1)
        .ok_or_else(|| DbError::new("54000", "automatic checkpoint commit counter overflowed"))?;
    if count < AUTOMATIC_CHECKPOINT_INTERVAL {
        return Ok(());
    }
    checkpoint_shared(state, store, wal, transactions)?;
    commits_since_checkpoint.store(0, Ordering::Release);
    Ok(())
}

fn execute_transaction_session_command(
    transaction: &mut ActiveSqlTransaction,
    statement: &BoundStatement,
    params: &[Value],
) -> Result<Option<TryQueryStream>> {
    if let BoundStatement::PgNotify {
        channel,
        payload,
        schema,
    } = statement
    {
        let (channel, payload) = evaluate_pg_notify(channel, payload, params)?;
        transaction.notification_state.notify(channel, payload);
        return Ok(Some(TryQueryStream::new(pg_notify_events(schema.clone()))));
    }
    let tag = match statement {
        BoundStatement::Listen { channel } => {
            transaction.notification_state.listen(channel.clone());
            "LISTEN"
        }
        BoundStatement::Unlisten { channel } => {
            transaction.notification_state.unlisten(channel.clone());
            "UNLISTEN"
        }
        BoundStatement::Notify { channel, payload } => {
            transaction
                .notification_state
                .notify(channel.clone(), payload.clone());
            "NOTIFY"
        }
        BoundStatement::DeallocateAll => "DEALLOCATE ALL",
        BoundStatement::DiscardAll => {
            return Err(DbError::new(
                "25001",
                "DISCARD ALL cannot run inside a transaction block",
            ));
        }
        _ => return Ok(None),
    };
    Ok(Some(TryQueryStream::new(transaction_events(tag))))
}

fn transaction_events(tag: &str) -> Vec<QueryEvent> {
    command_events(Schema::empty(), tag, 0, None)
}

fn evaluate_pg_notify(
    channel: &BoundExpr,
    payload: &BoundExpr,
    params: &[Value],
) -> Result<(Identifier, String)> {
    let Value::Text(channel) = evaluate_scalar(channel, &[], params)? else {
        return Err(DbError::new("22004", "pg_notify channel must not be NULL"));
    };
    let Value::Text(payload) = evaluate_scalar(payload, &[], params)? else {
        return Err(DbError::new("22004", "pg_notify payload must not be NULL"));
    };
    if channel.is_empty() || channel.len() > ordadb_types::MAX_POSTGRES_NAME_BYTES {
        return Err(DbError::new(
            "42622",
            "notification channel name is empty or too long",
        ));
    }
    if channel.contains('\0') || payload.contains('\0') {
        return Err(DbError::new(
            "22021",
            "notification channel and payload cannot contain NUL",
        ));
    }
    if payload.len() > 7_999 {
        return Err(DbError::new("22023", "NOTIFY payload is too long"));
    }
    let identifier = if channel
        .chars()
        .all(|character| !character.is_ascii_uppercase())
    {
        Identifier::unquoted(channel)
    } else {
        Identifier::quoted(channel)
    };
    Ok((identifier, payload))
}

fn pg_notify_events(schema: Schema) -> Vec<QueryEvent> {
    command_events(
        schema.clone(),
        "SELECT 1",
        1,
        Some(Batch {
            schema,
            rows: vec![Row::new(vec![Value::Null])],
        }),
    )
}

fn no_active_transaction_error(action: &str) -> DbError {
    DbError::new(
        "25P01",
        format!("cannot {action} because no transaction is active"),
    )
}

fn failed_transaction_error() -> DbError {
    DbError::new(
        "25P02",
        "the current transaction is aborted; commands are ignored until ROLLBACK",
    )
    .with_hint("issue ROLLBACK before starting new work")
}

fn execute_root_bound(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
    maintenance: MaintenanceContext<'_>,
) -> Result<(Vec<QueryEvent>, bool)> {
    match statement {
        BoundStatement::Analyze { table_id } => {
            authorize_statement_ownership(
                &state.catalog,
                &BoundStatement::Analyze { table_id },
                state.authorization.as_ref(),
            )?;
            execute_analyze(state, table_id)
        }
        BoundStatement::Vacuum { table_id, analyze } => {
            authorize_statement_ownership(
                &state.catalog,
                &BoundStatement::Vacuum { table_id, analyze },
                state.authorization.as_ref(),
            )?;
            execute_vacuum(state, table_id, analyze, maintenance)
        }
        BoundStatement::Reindex { target } => {
            authorize_statement_ownership(
                &state.catalog,
                &BoundStatement::Reindex { target },
                state.authorization.as_ref(),
            )?;
            execute_reindex(state, target)
        }
        statement => execute_bound_with_ownership(state, statement, params),
    }
}

fn execute_bound_with_ownership(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let authorization = state.authorization.clone();
    authorize_statement_ownership(&state.catalog, &statement, authorization.as_ref())?;
    let previous_catalog = authorization
        .as_ref()
        .filter(|_| statement_may_create_catalog_objects(&statement))
        .map(|_| Arc::clone(&state.catalog));
    let (events, dirty) = execute_bound(state, statement, params)?;
    if dirty
        && let (Some(authorization), Some(previous_catalog)) =
            (authorization.as_ref(), previous_catalog.as_deref())
    {
        Arc::make_mut(&mut state.catalog)
            .assign_new_object_owners(previous_catalog, authorization.owner())?;
    }
    Ok((events, dirty))
}

fn authorize_statement_ownership(
    catalog: &Catalog,
    statement: &BoundStatement,
    authorization: Option<&SessionAuthorization>,
) -> Result<()> {
    let Some(authorization) =
        authorization.filter(|authorization| !authorization.bypasses_ownership())
    else {
        return Ok(());
    };
    let mut objects = Vec::new();
    let schema_object = |schema: &Identifier| {
        catalog
            .schema(schema)
            .map(|schema| CatalogObjectRef::Schema(schema.id))
    };
    match statement {
        BoundStatement::CreateEnumType { schema, .. }
        | BoundStatement::CreateDomain { schema, .. }
        | BoundStatement::CreateTable { schema, .. }
        | BoundStatement::CreateSequence { schema, .. } => {
            objects.extend(schema_object(schema));
        }
        BoundStatement::CreateView {
            schema, existing, ..
        } => match existing {
            Some(view_id) => objects.push(CatalogObjectRef::View(*view_id)),
            None => objects.extend(schema_object(schema)),
        },
        BoundStatement::CreateRoutine {
            schema,
            name,
            kind,
            arguments,
            replace,
            ..
        } => {
            if *replace
                && let Some(routine) = catalog.routine_by_signature(schema, name, *kind, arguments)
            {
                objects.push(CatalogObjectRef::Routine(routine.id));
            } else {
                objects.extend(schema_object(schema));
            }
        }
        BoundStatement::AlterEnumAddValue { type_id, .. }
        | BoundStatement::AlterEnumRenameValue { type_id, .. }
        | BoundStatement::AlterDomain { type_id, .. } => {
            objects.push(CatalogObjectRef::Type(*type_id));
        }
        BoundStatement::AlterSchemaRename { schema_id, .. } => {
            objects.push(CatalogObjectRef::Schema(*schema_id));
        }
        BoundStatement::Analyze {
            table_id: Some(table_id),
        }
        | BoundStatement::Vacuum {
            table_id: Some(table_id),
            ..
        } => objects.push(CatalogObjectRef::Table(*table_id)),
        BoundStatement::Reindex { target } => match target {
            BoundReindexTarget::Index(index_id) => {
                objects.push(CatalogObjectRef::Index(*index_id));
            }
            BoundReindexTarget::Table(table_id) => {
                objects.push(CatalogObjectRef::Table(*table_id));
            }
            BoundReindexTarget::Schema(schema_id) => {
                objects.push(CatalogObjectRef::Schema(*schema_id));
            }
            BoundReindexTarget::Database => {
                objects.extend(
                    catalog
                        .database()
                        .schemas()
                        .map(|schema| CatalogObjectRef::Schema(schema.id)),
                );
            }
        },
        BoundStatement::DropObjects {
            objects: dropped, ..
        } => objects.extend(dropped.iter().copied()),
        BoundStatement::AlterTable { table_id, .. }
        | BoundStatement::CreateIndex { table_id, .. } => {
            objects.push(CatalogObjectRef::Table(*table_id));
        }
        BoundStatement::CreateTrigger { target, .. } => objects.push(target.object_ref()),
        BoundStatement::AlterIndexRename { index_id, .. } => {
            objects.push(CatalogObjectRef::Index(*index_id));
        }
        BoundStatement::AlterSequenceRename { sequence_id, .. }
        | BoundStatement::AlterSequence { sequence_id, .. } => {
            objects.push(CatalogObjectRef::Sequence(*sequence_id));
        }
        BoundStatement::AlterViewRename { view_id, .. }
        | BoundStatement::RefreshMaterializedView { view_id, .. } => {
            objects.push(CatalogObjectRef::View(*view_id));
        }
        BoundStatement::DropRoutine { routine_id, .. } => {
            objects.push(CatalogObjectRef::Routine(*routine_id));
        }
        BoundStatement::DropTrigger { trigger_id, .. } => {
            objects.push(CatalogObjectRef::Trigger(*trigger_id));
        }
        _ => {}
    }
    for object in objects {
        let Some(owner) = catalog.owner_of(object) else {
            continue;
        };
        if owner != authorization.owner() {
            return Err(
                DbError::new("42501", "must be owner of catalog object").with_detail(format!(
                    "authenticated role {} does not own {object:?}",
                    authorization.owner().as_str()
                )),
            );
        }
    }
    Ok(())
}

fn statement_may_create_catalog_objects(statement: &BoundStatement) -> bool {
    matches!(
        statement,
        BoundStatement::CreateSchema { .. }
            | BoundStatement::CreateEnumType { .. }
            | BoundStatement::CreateDomain { .. }
            | BoundStatement::CreateTable { .. }
            | BoundStatement::AlterTable { .. }
            | BoundStatement::CreateIndex { .. }
            | BoundStatement::CreateSequence { .. }
            | BoundStatement::CreateView { .. }
            | BoundStatement::CreateRoutine { .. }
            | BoundStatement::CreateTrigger { .. }
    )
}

fn execute_analyze(
    state: &mut DatabaseState,
    table_id: Option<TableId>,
) -> Result<(Vec<QueryEvent>, bool)> {
    let table_ids = maintenance_table_ids(state, table_id)?;
    for table_id in &table_ids {
        rebuild_table_derived(state, *table_id)?;
    }
    Ok((
        command_events(
            Schema::empty(),
            "ANALYZE",
            u64::try_from(table_ids.len()).unwrap_or(u64::MAX),
            None,
        ),
        true,
    ))
}

fn execute_reindex(
    state: &mut DatabaseState,
    target: BoundReindexTarget,
) -> Result<(Vec<QueryEvent>, bool)> {
    let table_ids = match target {
        BoundReindexTarget::Index(index_id) => {
            ensure_statement_not_cancelled(state)?;
            rebuild_index_derived(state, index_id)?;
            Vec::new()
        }
        BoundReindexTarget::Table(table_id) => {
            table_definition(state, table_id)?;
            vec![table_id]
        }
        BoundReindexTarget::Schema(schema_id) => state
            .catalog
            .schema_by_id(schema_id)
            .ok_or_else(|| DbError::new("3F000", "schema does not exist"))?
            .tables()
            .map(|table| table.id)
            .collect(),
        BoundReindexTarget::Database => state
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .map(|table| table.id)
            .collect(),
    };
    for table_id in table_ids {
        ensure_statement_not_cancelled(state)?;
        rebuild_table_indexes(state, table_id)?;
    }
    Ok((command_events(Schema::empty(), "REINDEX", 0, None), true))
}

fn execute_vacuum(
    state: &mut DatabaseState,
    table_id: Option<TableId>,
    _analyze: bool,
    maintenance: MaintenanceContext<'_>,
) -> Result<(Vec<QueryEvent>, bool)> {
    if let Some(transaction_id) = maintenance.expired_snapshot {
        return Err(DbError::new(
            "55000",
            format!(
                "VACUUM cannot proceed while transaction {transaction_id} holds an expired snapshot"
            ),
        )
        .with_hint("commit or roll back the long-running transaction before retrying VACUUM"));
    }
    let table_ids = maintenance_table_ids(state, table_id)?;
    let mut reclaimed = 0_u64;
    for table_id in &table_ids {
        reclaimed = reclaimed
            .checked_add(vacuum_table_versions(state, *table_id, maintenance)?)
            .ok_or_else(|| DbError::new("54000", "VACUUM reclaimed-row count overflow"))?;
        rebuild_table_derived(state, *table_id)?;
    }
    Ok((
        command_events(Schema::empty(), "VACUUM", reclaimed, None),
        true,
    ))
}

fn maintenance_table_ids(state: &DatabaseState, table_id: Option<TableId>) -> Result<Vec<TableId>> {
    if let Some(table_id) = table_id {
        table_definition(state, table_id)?;
        return Ok(vec![table_id]);
    }
    Ok(state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .map(|table| table.id)
        .collect())
}

fn vacuum_table_versions(
    state: &mut DatabaseState,
    table_id: TableId,
    maintenance: MaintenanceContext<'_>,
) -> Result<u64> {
    let original = state
        .versions
        .get(&table_id)
        .map(|versions| (**versions).clone())
        .unwrap_or_default();
    let mut retained = Vec::with_capacity(original.len());
    let mut id_map = BTreeMap::new();
    for version in &original {
        if version_reclaimable(version, maintenance)? {
            continue;
        }
        let new_id = u32::try_from(retained.len())
            .ok()
            .and_then(|id| id.checked_add(1))
            .ok_or_else(|| DbError::new("54000", "table version ordinal space is exhausted"))?;
        let mut retained_version = version.clone();
        freeze_retained_version(&mut retained_version, maintenance)?;
        id_map.insert(version.version_id, new_id);
        retained.push((version.version_id, retained_version));
    }
    for (old_id, version) in &mut retained {
        let mut predecessor = version.header.previous_version;
        let mut traversed = 0_usize;
        while predecessor != 0 && !id_map.contains_key(&predecessor) {
            traversed = traversed
                .checked_add(1)
                .ok_or_else(|| DbError::new("54001", "tuple predecessor depth overflow"))?;
            if traversed > original.len() {
                return Err(DbError::new(
                    "XX001",
                    "tuple predecessor chain is cyclic during VACUUM",
                ));
            }
            predecessor = original
                .get(usize::try_from(predecessor.saturating_sub(1)).unwrap_or(usize::MAX))
                .ok_or_else(|| {
                    DbError::new(
                        "XX001",
                        "tuple predecessor points outside the version sequence",
                    )
                })?
                .header
                .previous_version;
        }
        version.version_id = *id_map
            .get(old_id)
            .ok_or_else(|| internal_error("retained tuple version was not remapped"))?;
        version.header.previous_version = if predecessor == 0 {
            0
        } else {
            *id_map
                .get(&predecessor)
                .ok_or_else(|| internal_error("retained tuple predecessor was not remapped"))?
        };
    }
    let visible = state
        .visible_versions
        .get(&table_id)
        .map_or(&[][..], |versions| versions.as_slice())
        .iter()
        .map(|old_id| {
            id_map.get(old_id).copied().ok_or_else(|| {
                internal_error("VACUUM attempted to reclaim a currently visible tuple")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let reclaimed = original.len().saturating_sub(retained.len());
    state.versions.insert(
        table_id,
        Arc::new(retained.into_iter().map(|(_, version)| version).collect()),
    );
    state.visible_versions.insert(table_id, Arc::new(visible));
    u64::try_from(reclaimed).map_err(|_| DbError::new("54000", "VACUUM count overflow"))
}

fn version_reclaimable(
    version: &VersionedRow,
    maintenance: MaintenanceContext<'_>,
) -> Result<bool> {
    let creator = TransactionId::new(version.header.xmin)
        .ok_or_else(|| DbError::new("XX001", "tuple creator transaction ID is zero"))?;
    let creator_outcome = if version.header.xmin == FROZEN_TRANSACTION_ID {
        TransactionOutcome::Committed
    } else {
        maintenance.statuses.transaction_outcome(creator)?
    };
    if creator < maintenance.horizon && creator_outcome == TransactionOutcome::Aborted {
        return Ok(true);
    }
    if creator_outcome != TransactionOutcome::Committed || version.header.xmax == 0 {
        return Ok(false);
    }
    let deleter = TransactionId::new(version.header.xmax)
        .ok_or_else(|| DbError::new("XX001", "tuple deleter transaction ID is zero"))?;
    Ok(deleter < maintenance.horizon
        && maintenance.statuses.transaction_outcome(deleter)? == TransactionOutcome::Committed)
}

fn freeze_retained_version(
    version: &mut VersionedRow,
    maintenance: MaintenanceContext<'_>,
) -> Result<()> {
    if version.header.xmin != FROZEN_TRANSACTION_ID {
        let creator = TransactionId::new(version.header.xmin)
            .ok_or_else(|| DbError::new("XX001", "tuple creator transaction ID is zero"))?;
        if creator < maintenance.horizon
            && maintenance.statuses.transaction_outcome(creator)? == TransactionOutcome::Committed
        {
            version.header.xmin = FROZEN_TRANSACTION_ID;
        }
    }
    let Some(deleter) = TransactionId::new(version.header.xmax) else {
        return Ok(());
    };
    if deleter < maintenance.horizon {
        match maintenance.statuses.transaction_outcome(deleter)? {
            TransactionOutcome::Aborted => version.header.xmax = 0,
            TransactionOutcome::Committed => {
                return Err(internal_error(
                    "VACUUM retained a tuple deleted before the safe horizon",
                ));
            }
            TransactionOutcome::InProgress => {}
        }
    }
    Ok(())
}

fn execute_bound(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    match statement {
        BoundStatement::NoOp { tag } => Ok((command_events(Schema::empty(), tag, 0, None), false)),
        BoundStatement::Do { body } => {
            let program = compile_plpgsql(&body, &[])?;
            let limits = ordadb_plpgsql::ResourceLimits::default();
            let memory = VmMemoryGrant::new(limits.max_cursor_bytes)?;
            let output = {
                let mut host = EnginePlpgsqlHost {
                    state,
                    trigger: None,
                    exception_states: Vec::new(),
                    exception_triggers: Vec::new(),
                    exception_charges: Vec::new(),
                    exception_memory: memory.try_reserve(0)?,
                    sql_dirty: false,
                };
                execute_plpgsql_with_memory(&program, &mut host, &[], limits, memory)?
            };
            if output.return_value.is_some()
                || !output.returned_rows.is_empty()
                || output.return_parameter.is_some()
            {
                return Err(DbError::new(
                    "42601",
                    "DO blocks cannot return a value or result row",
                ));
            }
            Ok((command_events(Schema::empty(), "DO", 0, None), true))
        }
        BoundStatement::CreateSchema {
            name,
            if_not_exists,
        } => {
            if state.catalog.schema(&name).is_some() && if_not_exists {
                return Ok((
                    command_events(Schema::empty(), "CREATE SCHEMA", 0, None),
                    false,
                ));
            }
            Arc::make_mut(&mut state.catalog).create_schema(name)?;
            Ok((
                command_events(Schema::empty(), "CREATE SCHEMA", 0, None),
                true,
            ))
        }
        BoundStatement::CreateEnumType {
            schema,
            name,
            labels,
        } => {
            Arc::make_mut(&mut state.catalog).create_enum_type(&schema, name, labels)?;
            Ok((
                command_events(Schema::empty(), "CREATE TYPE", 0, None),
                true,
            ))
        }
        BoundStatement::CreateDomain {
            schema,
            name,
            base_type,
            base_declared_type,
            not_null,
            default,
            checks,
        } => {
            Arc::make_mut(&mut state.catalog).create_domain_with_declared_type(
                &schema,
                name,
                DomainBaseType::new(base_type, base_declared_type),
                not_null,
                default,
                checks,
            )?;
            Ok((
                command_events(Schema::empty(), "CREATE DOMAIN", 0, None),
                true,
            ))
        }
        BoundStatement::AlterEnumAddValue {
            type_id,
            label,
            position,
            if_not_exists,
        } => {
            let changed = Arc::make_mut(&mut state.catalog).alter_enum_add_value(
                type_id,
                label,
                position,
                if_not_exists,
            )?;
            if changed {
                rewrite_enum_values(state, type_id, None)?;
            }
            Ok((
                command_events(Schema::empty(), "ALTER TYPE", 0, None),
                changed,
            ))
        }
        BoundStatement::AlterEnumRenameValue {
            type_id,
            old_label,
            new_label,
        } => {
            Arc::make_mut(&mut state.catalog).alter_enum_rename_value(
                type_id,
                &old_label,
                new_label.clone(),
            )?;
            rewrite_enum_values(state, type_id, Some((&old_label, &new_label)))?;
            Ok((command_events(Schema::empty(), "ALTER TYPE", 0, None), true))
        }
        BoundStatement::AlterDomain { type_id, operation } => {
            let changed = match operation {
                BoundAlterDomainOperation::SetDefault(default) => {
                    Arc::make_mut(&mut state.catalog)
                        .alter_domain_default(type_id, Some(default))?;
                    true
                }
                BoundAlterDomainOperation::DropDefault => {
                    Arc::make_mut(&mut state.catalog).alter_domain_default(type_id, None)?;
                    true
                }
                BoundAlterDomainOperation::SetNotNull => {
                    Arc::make_mut(&mut state.catalog).alter_domain_not_null(type_id, true)?;
                    true
                }
                BoundAlterDomainOperation::DropNotNull => {
                    Arc::make_mut(&mut state.catalog).alter_domain_not_null(type_id, false)?;
                    true
                }
                BoundAlterDomainOperation::AddConstraint(constraint) => {
                    Arc::make_mut(&mut state.catalog).add_domain_constraint(type_id, constraint)?;
                    true
                }
                BoundAlterDomainOperation::DropConstraint { name, if_exists } => {
                    Arc::make_mut(&mut state.catalog)
                        .drop_domain_constraint(type_id, &name, if_exists)?
                }
            };
            validate_database_rows(state)?;
            Ok((
                command_events(Schema::empty(), "ALTER DOMAIN", 0, None),
                changed,
            ))
        }
        BoundStatement::AlterSchemaRename {
            schema_id,
            new_name,
        } => {
            Arc::make_mut(&mut state.catalog).rename_schema(schema_id, new_name)?;
            Ok((
                command_events(Schema::empty(), "ALTER SCHEMA", 0, None),
                true,
            ))
        }
        BoundStatement::DropObjects {
            kind,
            objects,
            behavior,
        } => execute_drop_objects(state, kind, objects, behavior),
        BoundStatement::CreateTable {
            schema,
            name,
            columns,
            constraints,
            if_not_exists,
        } => {
            if state.catalog.table(&schema, &name).is_some() && if_not_exists {
                return Ok((
                    command_events(Schema::empty(), "CREATE TABLE", 0, None),
                    false,
                ));
            }
            let table_id =
                Arc::make_mut(&mut state.catalog).create_table(&schema, name, columns)?;
            for constraint in constraints {
                Arc::make_mut(&mut state.catalog).create_constraint(table_id, constraint)?;
            }
            state.rows.insert(table_id, Arc::new(Vec::new()));
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "CREATE TABLE", 0, None),
                true,
            ))
        }
        BoundStatement::AlterTable {
            table_id,
            operations,
        } => execute_alter_table(state, table_id, operations),
        BoundStatement::CreateIndex {
            table_id,
            index,
            if_not_exists,
        } => {
            if table_definition(state, table_id)?
                .index(&index.name)
                .is_some()
                && if_not_exists
            {
                return Ok((
                    command_events(Schema::empty(), "CREATE INDEX", 0, None),
                    false,
                ));
            }
            Arc::make_mut(&mut state.catalog).create_index(table_id, index)?;
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "CREATE INDEX", 0, None),
                true,
            ))
        }
        BoundStatement::AlterIndexRename { index_id, new_name } => {
            Arc::make_mut(&mut state.catalog).rename_index(index_id, new_name)?;
            Ok((
                command_events(Schema::empty(), "ALTER INDEX", 0, None),
                true,
            ))
        }
        BoundStatement::CreateSequence {
            schema,
            sequence,
            if_not_exists,
        } => {
            if state.catalog.sequence(&schema, &sequence.name).is_some() && if_not_exists {
                return Ok((
                    command_events(Schema::empty(), "CREATE SEQUENCE", 0, None),
                    false,
                ));
            }
            Arc::make_mut(&mut state.catalog).create_sequence(&schema, sequence)?;
            Ok((
                command_events(Schema::empty(), "CREATE SEQUENCE", 0, None),
                true,
            ))
        }
        BoundStatement::AlterSequenceRename {
            sequence_id,
            new_name,
        } => {
            Arc::make_mut(&mut state.catalog).rename_sequence(sequence_id, new_name)?;
            Ok((
                command_events(Schema::empty(), "ALTER SEQUENCE", 0, None),
                true,
            ))
        }
        BoundStatement::AlterSequence {
            sequence_id,
            increment,
            min_value,
            max_value,
            restart,
            cycle,
            owner,
        } => {
            Arc::make_mut(&mut state.catalog).alter_sequence(
                sequence_id,
                SequenceAlteration {
                    increment,
                    min_value,
                    max_value,
                    restart,
                    cycle,
                    owner,
                },
            )?;
            Ok((
                command_events(Schema::empty(), "ALTER SEQUENCE", 0, None),
                true,
            ))
        }
        BoundStatement::CreateView {
            schema,
            name,
            kind,
            query,
            query_sql,
            output,
            references,
            replace,
            if_not_exists,
            with_data,
            existing,
        } => execute_create_view(
            state,
            CreateViewExecution {
                schema,
                name,
                kind,
                query: *query,
                query_sql,
                output,
                references,
                replace,
                if_not_exists,
                with_data,
                existing,
            },
            params,
        ),
        BoundStatement::AlterViewRename { view_id, new_name } => {
            let kind = state
                .catalog
                .view_by_id(view_id)
                .map(|view| view.kind)
                .ok_or_else(|| internal_error("view disappeared before rename"))?;
            Arc::make_mut(&mut state.catalog).rename_view(view_id, new_name)?;
            let tag = match kind {
                ordadb_catalog::ViewKind::Regular => "ALTER VIEW",
                ordadb_catalog::ViewKind::Materialized => "ALTER MATERIALIZED VIEW",
            };
            Ok((command_events(Schema::empty(), tag, 0, None), true))
        }
        BoundStatement::RefreshMaterializedView {
            view_id,
            table_id,
            query,
            with_data,
        } => {
            let rows = if with_data {
                materialize_statement_rows(state, *query, params)?
            } else {
                Vec::new()
            };
            state.rows.insert(table_id, Arc::new(rows));
            Arc::make_mut(&mut state.catalog)
                .set_materialized_view_populated(view_id, with_data)?;
            rebuild_table_derived(state, table_id)?;
            Ok((
                command_events(Schema::empty(), "REFRESH MATERIALIZED VIEW", 0, None),
                true,
            ))
        }
        BoundStatement::CreateRoutine {
            schema,
            name,
            kind,
            arguments,
            return_type,
            return_declared_type,
            returns_set,
            language,
            body,
            replace,
        } => {
            let argument_names = routine_argument_names(&arguments);
            let compile_names = if kind == ordadb_catalog::RoutineKind::Function
                && return_type.is_none()
                && arguments.is_empty()
            {
                vec!["old".to_owned(), "new".to_owned()]
            } else {
                argument_names
            };
            compile_plpgsql(&body, &compile_names)?;
            let tag = match kind {
                ordadb_catalog::RoutineKind::Function => "CREATE FUNCTION",
                ordadb_catalog::RoutineKind::Procedure => "CREATE PROCEDURE",
            };
            let referenced_types = arguments
                .iter()
                .filter_map(|argument| argument.declared_type)
                .chain(return_declared_type)
                .collect::<BTreeSet<_>>();
            Arc::make_mut(&mut state.catalog).create_or_replace_routine(
                &schema,
                NewRoutine {
                    name,
                    kind,
                    arguments,
                    return_type,
                    return_declared_type,
                    returns_set,
                    language,
                    body,
                    replace,
                    references: referenced_types
                        .into_iter()
                        .map(CatalogObjectRef::Type)
                        .collect(),
                },
            )?;
            Ok((command_events(Schema::empty(), tag, 0, None), true))
        }
        BoundStatement::DropRoutine {
            routine_id,
            behavior,
        } => {
            let kind = state
                .catalog
                .routine_by_id(routine_id)
                .map(|routine| routine.kind)
                .ok_or_else(|| DbError::new("42883", "routine does not exist"))?;
            let removed = Arc::make_mut(&mut state.catalog).drop_routine(routine_id, behavior)?;
            cleanup_removed_state(state, &removed);
            let tag = match kind {
                ordadb_catalog::RoutineKind::Function => "DROP FUNCTION",
                ordadb_catalog::RoutineKind::Procedure => "DROP PROCEDURE",
            };
            Ok((command_events(Schema::empty(), tag, 0, None), true))
        }
        BoundStatement::Call {
            routine_id,
            arguments,
            schema,
        } => {
            let output = execute_routine_program(state, routine_id, &arguments, params)?;
            let row_count = u64::from(!schema.fields.is_empty());
            let batch = (!schema.fields.is_empty()).then(|| Batch {
                schema: schema.clone(),
                rows: vec![Row::new(output.output_parameters)],
            });
            Ok((command_events(schema, "CALL", row_count, batch), true))
        }
        BoundStatement::ScalarSelect { projection, schema } => {
            let values = projection
                .iter()
                .map(|projection| evaluate_scalar(&projection.expr, &[], params))
                .collect::<Result<Vec<_>>>()?;
            Ok((
                command_events(
                    schema.clone(),
                    "SELECT 1",
                    1,
                    Some(Batch {
                        schema,
                        rows: vec![Row::new(values)],
                    }),
                ),
                false,
            ))
        }
        BoundStatement::RoutineSelect {
            routine_id,
            arguments,
            schema,
            returns_set,
        } => {
            let output = execute_routine_program(state, routine_id, &arguments, params)?;
            let values = if returns_set {
                output.returned_rows
            } else {
                vec![output.return_value.unwrap_or(Value::Null)]
            };
            let row_count = values.len() as u64;
            Ok((
                vec![
                    QueryEvent::Schema(schema.clone()),
                    QueryEvent::Batch(Batch {
                        schema,
                        rows: values
                            .into_iter()
                            .map(|value| Row::new(vec![value]))
                            .collect(),
                    }),
                    QueryEvent::Progress(QueryProgress {
                        rows_processed: row_count,
                    }),
                    QueryEvent::Complete(CommandComplete {
                        tag: format!("SELECT {row_count}"),
                        rows_affected: row_count,
                    }),
                ],
                false,
            ))
        }
        BoundStatement::SequenceValue {
            sequence_id,
            operation,
            schema,
        } => {
            let (value, dirty) = match operation {
                BoundSequenceOperation::NextValue => (
                    Arc::make_mut(&mut state.catalog).next_sequence_value(sequence_id)?,
                    true,
                ),
                BoundSequenceOperation::CurrentValue { value } => (
                    value.ok_or_else(|| {
                        DbError::new(
                            "55000",
                            "currval of sequence is not yet defined in this session",
                        )
                    })?,
                    false,
                ),
                BoundSequenceOperation::SetValue { value, is_called } => {
                    let value = evaluate_scalar(&value, &[], params)?;
                    let Value::Int64(value) = value else {
                        return Err(internal_error(
                            "bound setval expression did not produce BIGINT",
                        ));
                    };
                    Arc::make_mut(&mut state.catalog).set_sequence_value(
                        sequence_id,
                        value,
                        is_called,
                    )?;
                    (value, true)
                }
            };
            if dirty {
                state.sequence_currvals.insert(sequence_id, value);
            }
            Ok((
                command_events(
                    schema.clone(),
                    "SELECT 1",
                    1,
                    Some(Batch {
                        schema,
                        rows: vec![Row::new(vec![Value::Int64(value)])],
                    }),
                ),
                dirty,
            ))
        }
        BoundStatement::CreateTrigger {
            target,
            name,
            timing,
            level,
            events,
            routine_id,
        } => {
            Arc::make_mut(&mut state.catalog).create_trigger_on_target_with_level(
                target,
                name,
                timing,
                level,
                events.into_iter().collect(),
                routine_id,
            )?;
            Ok((
                command_events(Schema::empty(), "CREATE TRIGGER", 0, None),
                true,
            ))
        }
        BoundStatement::DropTrigger {
            trigger_id,
            behavior,
        } => {
            let removed = Arc::make_mut(&mut state.catalog).drop_trigger(trigger_id, behavior)?;
            cleanup_removed_state(state, &removed);
            Ok((
                command_events(Schema::empty(), "DROP TRIGGER", 0, None),
                true,
            ))
        }
        BoundStatement::Insert {
            table_id,
            column_indexes,
            rows,
            on_conflict,
            returning,
        } => execute_insert(
            state,
            table_id,
            column_indexes,
            rows,
            on_conflict,
            returning,
            params,
        ),
        BoundStatement::ViewInsert {
            view_id,
            source,
            column_indexes,
            rows,
            returning,
        } => execute_view_insert(
            state,
            view_id,
            *source,
            column_indexes,
            rows,
            returning,
            params,
        ),
        BoundStatement::Merge(merge) => execute_merge(state, merge, params),
        BoundStatement::With {
            ctes,
            body,
            catalog,
            schema,
        } => execute_with_clause(state, ctes, *body, *catalog, schema, params),
        BoundStatement::SetOperation {
            left,
            operator,
            all,
            right,
            schema,
            order_by,
            offset,
            limit,
        } => execute_set_operation(
            state,
            SetExecution {
                left,
                operator,
                all,
                right,
                schema,
                order_by,
                offset,
                limit,
            },
            params,
        ),
        BoundStatement::Select {
            table_id,
            schema,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => execute_select(
            state,
            SelectExecution {
                table_id,
                schema,
                projection,
                filter,
                order_by,
                offset,
                limit,
            },
            params,
        ),
        BoundStatement::AdvancedSelect {
            table,
            joins,
            applies,
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
        } => execute_advanced_select(
            state,
            AdvancedExecution {
                table,
                joins,
                applies,
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
            },
            params,
        ),
        BoundStatement::ViewSelect {
            source,
            schema,
            projection,
            ..
        } => execute_view_select(state, *source, schema, projection, params),
        BoundStatement::Explain { statement } => execute_explain(state, *statement),
        BoundStatement::Update {
            table_id,
            assignments,
            filter,
            returning,
        } => execute_update(state, table_id, assignments, filter, returning, params),
        BoundStatement::ViewUpdate {
            view_id,
            source,
            assignments,
            filter,
            returning,
        } => execute_view_update(
            state,
            view_id,
            *source,
            assignments,
            filter,
            returning,
            params,
        ),
        BoundStatement::Delete {
            table_id,
            filter,
            returning,
        } => execute_delete(state, table_id, filter, returning, params),
        BoundStatement::ViewDelete {
            view_id,
            source,
            filter,
            returning,
        } => execute_view_delete(state, view_id, *source, filter, returning, params),
        BoundStatement::Analyze { .. }
        | BoundStatement::Vacuum { .. }
        | BoundStatement::Reindex { .. } => Err(internal_error(
            "maintenance statement was not routed through the root executor",
        )),
        BoundStatement::Listen { .. }
        | BoundStatement::Unlisten { .. }
        | BoundStatement::Notify { .. }
        | BoundStatement::PgNotify { .. }
        | BoundStatement::DiscardAll
        | BoundStatement::DeallocateAll => Err(internal_error(
            "session command was not routed through the session executor",
        )),
        BoundStatement::Begin { .. }
        | BoundStatement::Commit { .. }
        | BoundStatement::Rollback { .. }
        | BoundStatement::Savepoint { .. }
        | BoundStatement::RollbackTo { .. }
        | BoundStatement::ReleaseSavepoint { .. } => Err(DbError::new(
            "25000",
            "transaction control was not routed through the session",
        )
        .with_hint("execute transaction control through Session")),
    }
}

fn routine_argument_names(arguments: &[ordadb_catalog::RoutineArgument]) -> Vec<String> {
    arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            argument
                .name
                .as_ref()
                .map_or_else(|| format!("__arg{}", index + 1), |name| name.to_string())
        })
        .collect()
}

fn execute_routine_program(
    state: &mut DatabaseState,
    routine_id: ordadb_types::RoutineId,
    arguments: &[BoundExpr],
    params: &[Value],
) -> Result<ordadb_plpgsql::VmOutput> {
    execute_routine_program_with_boundaries(state, routine_id, arguments, params, None)
        .map(|(output, _)| output)
}

fn execute_routine_program_with_boundaries(
    state: &mut DatabaseState,
    routine_id: ordadb_types::RoutineId,
    arguments: &[BoundExpr],
    params: &[Value],
    mut boundary_handler: Option<&mut ProcedureBoundaryHandler<'_>>,
) -> Result<(ordadb_plpgsql::VmOutput, bool)> {
    let routine_limits = ordadb_plpgsql::ResourceLimits::default();
    let routine_memory = VmMemoryGrant::new(routine_limits.max_cursor_bytes)?;
    let root = prepare_routine_vm_frame(
        state,
        routine_id,
        arguments,
        params,
        RoutineCompletion::Root,
        &routine_memory,
    )?;
    let mut frames = vec![root];
    let mut segment_dirty = false;
    loop {
        let resumed = {
            let frame = frames
                .last_mut()
                .ok_or_else(|| internal_error("PL/pgSQL VM frame stack is empty"))?;
            let mut host = EnginePlpgsqlHost {
                state,
                trigger: None,
                exception_states: mem::take(&mut frame.exception_states),
                exception_triggers: mem::take(&mut frame.exception_triggers),
                exception_charges: mem::take(&mut frame.exception_charges),
                exception_memory: mem::replace(
                    &mut frame.exception_memory,
                    routine_memory.try_reserve(0)?,
                ),
                sql_dirty: false,
            };
            let resumed = frame.machine.resume(&mut host, frame.response.take());
            frame.exception_states = host.exception_states;
            frame.exception_triggers = host.exception_triggers;
            frame.exception_charges = host.exception_charges;
            frame.exception_memory = host.exception_memory;
            resumed
        };
        let resumed = match resumed {
            Ok(resumed) => resumed,
            Err(error) => {
                let failed = frames
                    .pop()
                    .ok_or_else(|| internal_error("PL/pgSQL failed frame is missing"))?;
                state.routine_frames.pop(failed.id)?;
                match failed.completion {
                    RoutineCompletion::Root => return Err(error),
                    RoutineCompletion::Call { .. } | RoutineCompletion::Select { .. } => {
                        frames
                            .last_mut()
                            .ok_or_else(|| internal_error("PL/pgSQL parent frame is missing"))?
                            .response = Some(Err(error));
                        continue;
                    }
                }
            }
        };
        match resumed {
            VmRunState::Sql(request) => {
                let statement =
                    match parse(&request.sql).and_then(|parsed| bind(parsed, &state.catalog)) {
                        Ok(statement) => statement,
                        Err(error) => {
                            frames
                                .last_mut()
                                .ok_or_else(|| internal_error("PL/pgSQL SQL frame is missing"))?
                                .response = Some(Err(error));
                            continue;
                        }
                    };
                let boundary = match &statement {
                    BoundStatement::Commit { chain } => Some(ProcedureBoundary::Commit(*chain)),
                    BoundStatement::Rollback { chain } => Some(ProcedureBoundary::Rollback(*chain)),
                    _ => None,
                };
                if let Some(boundary) = boundary {
                    let response = if frames.len() != 1
                        || !frames.first().is_some_and(|frame| {
                            frame.routine.kind == ordadb_catalog::RoutineKind::Procedure
                        }) {
                        Err(DbError::new(
                            "2D000",
                            "invalid transaction termination inside this PL/pgSQL invocation",
                        )
                        .with_hint(
                            "transaction termination is allowed only in an eligible top-level procedure CALL",
                        ))
                    } else if let Err(error) = frames
                        .last()
                        .ok_or_else(|| internal_error("PL/pgSQL SQL frame is missing"))?
                        .machine
                        .ensure_transaction_boundary_ready()
                    {
                        Err(error)
                    } else if let Some(handler) = boundary_handler.as_deref_mut() {
                        match handler(boundary, state, segment_dirty) {
                            Ok(()) => {
                                segment_dirty = false;
                                let tag = match boundary {
                                    ProcedureBoundary::Commit(_) => "COMMIT",
                                    ProcedureBoundary::Rollback(_) => "ROLLBACK",
                                };
                                Ok(Box::new(transaction_events(tag).into_iter().map(Ok))
                                    as VmSqlStream)
                            }
                            Err(error) => Err(error),
                        }
                    } else {
                        Err(DbError::new(
                            "2D000",
                            "invalid transaction termination inside this PL/pgSQL invocation",
                        )
                        .with_hint(
                            "transaction termination requires an eligible top-level procedure CALL in autocommit mode",
                        ))
                    };
                    frames
                        .last_mut()
                        .ok_or_else(|| internal_error("PL/pgSQL SQL frame is missing"))?
                        .response = Some(response);
                    continue;
                }
                let child = match statement {
                    BoundStatement::Call {
                        routine_id,
                        arguments,
                        schema,
                    } => Some(prepare_routine_vm_frame(
                        state,
                        routine_id,
                        &arguments,
                        &request.parameters,
                        RoutineCompletion::Call { schema },
                        &routine_memory,
                    )),
                    BoundStatement::RoutineSelect {
                        routine_id,
                        arguments,
                        schema,
                        returns_set,
                    } => Some(prepare_routine_vm_frame(
                        state,
                        routine_id,
                        &arguments,
                        &request.parameters,
                        RoutineCompletion::Select {
                            schema,
                            returns_set,
                        },
                        &routine_memory,
                    )),
                    _ => None,
                };
                if let Some(child) = child {
                    match child {
                        Ok(child) => frames.push(child),
                        Err(error) => {
                            frames
                                .last_mut()
                                .ok_or_else(|| internal_error("PL/pgSQL parent frame is missing"))?
                                .response = Some(Err(error));
                        }
                    }
                    continue;
                }
                let (response, dirty) = {
                    let mut host = EnginePlpgsqlHost {
                        state,
                        trigger: None,
                        exception_states: Vec::new(),
                        exception_triggers: Vec::new(),
                        exception_charges: Vec::new(),
                        exception_memory: routine_memory.try_reserve(0)?,
                        sql_dirty: false,
                    };
                    let response = host.execute_sql(&request.sql, &request.parameters);
                    (response, host.sql_dirty)
                };
                segment_dirty |= dirty;
                frames
                    .last_mut()
                    .ok_or_else(|| internal_error("PL/pgSQL SQL frame is missing"))?
                    .response = Some(response);
            }
            VmRunState::Complete(output) => {
                let completed = frames
                    .pop()
                    .ok_or_else(|| internal_error("PL/pgSQL completed frame is missing"))?;
                state.routine_frames.pop(completed.id)?;
                let output = match finish_routine_output(&completed.routine, output) {
                    Ok(output) => output,
                    Err(error) => match completed.completion {
                        RoutineCompletion::Root => return Err(error),
                        RoutineCompletion::Call { .. } | RoutineCompletion::Select { .. } => {
                            frames
                                .last_mut()
                                .ok_or_else(|| internal_error("PL/pgSQL parent frame is missing"))?
                                .response = Some(Err(error));
                            continue;
                        }
                    },
                };
                match completed.completion {
                    RoutineCompletion::Root => return Ok((output, segment_dirty)),
                    completion => {
                        let events = routine_completion_events(completion, output);
                        frames
                            .last_mut()
                            .ok_or_else(|| internal_error("PL/pgSQL parent frame is missing"))?
                            .response = Some(Ok(events));
                    }
                }
            }
        }
    }
}

fn prepare_routine_vm_frame(
    state: &mut DatabaseState,
    routine_id: ordadb_types::RoutineId,
    arguments: &[BoundExpr],
    params: &[Value],
    completion: RoutineCompletion,
    memory: &VmMemoryGrant,
) -> Result<RoutineVmFrame> {
    let routine = state
        .catalog
        .routine_by_id(routine_id)
        .cloned()
        .ok_or_else(|| DbError::new("42883", "routine does not exist"))?;
    let program = compile_plpgsql(&routine.body, &routine_argument_names(&routine.arguments))?;
    let mut input_arguments = arguments.iter();
    let values = routine
        .arguments
        .iter()
        .map(|argument| {
            if argument.mode.accepts_input() {
                input_arguments
                    .next()
                    .ok_or_else(|| internal_error("routine input argument is missing"))
                    .and_then(|argument| evaluate_scalar(argument, &[], params))
            } else {
                Ok(Value::Null)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if input_arguments.next().is_some() {
        return Err(internal_error("routine received too many input arguments"));
    }
    let machine = {
        let mut host = EnginePlpgsqlHost {
            state,
            trigger: None,
            exception_states: Vec::new(),
            exception_triggers: Vec::new(),
            exception_charges: Vec::new(),
            exception_memory: memory.try_reserve(0)?,
            sql_dirty: false,
        };
        VmMachine::new_with_memory_grant(
            &program,
            &mut host,
            &values,
            ordadb_plpgsql::ResourceLimits::default(),
            memory.clone(),
        )?
    };
    let id = state.routine_frames.push_routine(routine_id)?;
    Ok(RoutineVmFrame {
        id,
        routine,
        machine,
        response: None,
        completion,
        exception_states: Vec::new(),
        exception_triggers: Vec::new(),
        exception_charges: Vec::new(),
        exception_memory: memory.try_reserve(0)?,
    })
}

fn finish_routine_output(routine: &RoutineDefinition, mut output: VmOutput) -> Result<VmOutput> {
    output.output_parameters = routine
        .arguments
        .iter()
        .enumerate()
        .filter(|(_, argument)| argument.mode.produces_output())
        .map(|(index, _)| {
            output
                .final_locals
                .get(index)
                .cloned()
                .ok_or_else(|| internal_error("routine output parameter local is missing"))
        })
        .collect::<Result<Vec<_>>>()?;
    if routine.return_type.is_none()
        && let [value] = output.output_parameters.as_slice()
    {
        output.return_value = Some(value.clone());
    }
    if let Some(return_type) = &routine.return_type {
        output.return_value = output
            .return_value
            .map(|value| coerce_execution_value(value, return_type))
            .transpose()?;
        output.returned_rows = output
            .returned_rows
            .into_iter()
            .map(|value| coerce_execution_value(value, return_type))
            .collect::<Result<Vec<_>>>()?;
    }
    output.refresh_retained_memory()?;
    Ok(output)
}

fn routine_completion_events(completion: RoutineCompletion, mut output: VmOutput) -> VmSqlStream {
    let memory = output.take_memory_hold();
    let events = match completion {
        RoutineCompletion::Root => Vec::new(),
        RoutineCompletion::Call { schema } => {
            let row_count = u64::from(!schema.fields.is_empty());
            let batch = (!schema.fields.is_empty()).then(|| Batch {
                schema: schema.clone(),
                rows: vec![Row::new(output.output_parameters)],
            });
            command_events(schema, "CALL", row_count, batch)
        }
        RoutineCompletion::Select {
            schema,
            returns_set,
        } => {
            let values = if returns_set {
                output.returned_rows
            } else {
                vec![output.return_value.unwrap_or(Value::Null)]
            };
            let row_count = values.len() as u64;
            vec![
                QueryEvent::Schema(schema.clone()),
                QueryEvent::Batch(Batch {
                    schema,
                    rows: values
                        .into_iter()
                        .map(|value| Row::new(vec![value]))
                        .collect(),
                }),
                QueryEvent::Progress(QueryProgress {
                    rows_processed: row_count,
                }),
                QueryEvent::Complete(CommandComplete {
                    tag: format!("SELECT {row_count}"),
                    rows_affected: row_count,
                }),
            ]
        }
    };
    Box::new(RoutineCompletionStream {
        events: events.into_iter(),
        _memory: memory,
    })
}

#[derive(Debug, Clone)]
struct TriggerRowContext {
    table: TableDefinition,
    old: Option<Row>,
    new: Option<Row>,
}

#[derive(Debug, Clone)]
struct TriggerRowSavepoint {
    old: Option<Row>,
    new: Option<Row>,
}

impl TriggerRowContext {
    fn value_and_type(&self, slot: usize, field: &str) -> Result<(Value, ScalarType)> {
        let row = match slot {
            0 => self.old.as_ref(),
            1 => self.new.as_ref(),
            _ => {
                return Err(DbError::new(
                    "42P02",
                    format!("trigger record parameter ${} does not exist", slot + 1),
                ));
            }
        };
        let field = Identifier::unquoted(field);
        let (column_index, data_type) = self
            .table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == field)
            .map(|(index, column)| (index, column.data_type.clone()))
            .ok_or_else(|| {
                DbError::new(
                    "42703",
                    format!("trigger record field {field} does not exist"),
                )
            })?;
        let value = match row {
            Some(row) => row.values.get(column_index).cloned().ok_or_else(|| {
                internal_error("trigger record width does not match its table definition")
            })?,
            None => Value::Null,
        };
        Ok((value, data_type))
    }

    fn value(&self, slot: usize, field: &str) -> Result<Value> {
        self.value_and_type(slot, field).map(|(value, _)| value)
    }

    fn assign(&mut self, slot: usize, field: &str, value: Value) -> Result<()> {
        if slot != 1 {
            return Err(DbError::new("25006", "OLD is read-only in a row trigger"));
        }
        let field = Identifier::unquoted(field);
        let (column_index, data_type) = self
            .table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == field)
            .map(|(index, column)| (index, column.data_type.clone()))
            .ok_or_else(|| {
                DbError::new(
                    "42703",
                    format!("trigger record field {field} does not exist"),
                )
            })?;
        let row = self
            .new
            .as_mut()
            .ok_or_else(|| DbError::new("55000", "NEW is not available for this trigger event"))?;
        let target = row.values.get_mut(column_index).ok_or_else(|| {
            internal_error("trigger record width does not match its table definition")
        })?;
        *target = coerce_execution_value(value, &data_type)?;
        Ok(())
    }
}

enum RowTriggerOutcome {
    Proceed(Option<Row>),
    Suppress,
}

fn trigger_argument_names() -> Vec<String> {
    [
        "old",
        "new",
        "tg_op",
        "tg_when",
        "tg_level",
        "tg_name",
        "tg_relid",
        "tg_table_schema",
        "tg_table_name",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[derive(Debug, Clone)]
struct TriggerRelation {
    target: TriggerTarget,
    schema_id: ordadb_types::SchemaId,
    name: Identifier,
    row_scope: TableDefinition,
}

fn trigger_relation(state: &DatabaseState, target: TriggerTarget) -> Result<TriggerRelation> {
    match target {
        TriggerTarget::Table(table_id) => {
            let table = table_definition(state, table_id)?.clone();
            Ok(TriggerRelation {
                target,
                schema_id: table.schema_id,
                name: table.name.clone(),
                row_scope: table,
            })
        }
        TriggerTarget::View(view_id) => {
            let view = state
                .catalog
                .view_by_id(view_id)
                .cloned()
                .ok_or_else(|| internal_error("trigger view does not exist"))?;
            Ok(TriggerRelation {
                target,
                schema_id: view.schema_id,
                name: view.name.clone(),
                row_scope: TableDefinition::expression_scope_for_schema(
                    view.name.clone(),
                    &view.output,
                )?,
            })
        }
    }
}

fn trigger_argument_values(
    state: &DatabaseState,
    relation: &TriggerRelation,
    trigger: &TriggerDefinition,
    timing: TriggerTiming,
    level: TriggerLevel,
    event: TriggerEvent,
) -> Result<Vec<Value>> {
    let operation = match event {
        TriggerEvent::Insert => "INSERT",
        TriggerEvent::Update => "UPDATE",
        TriggerEvent::Delete => "DELETE",
    };
    let when = match timing {
        TriggerTiming::Before | TriggerTiming::BeforeStatement => "BEFORE",
        TriggerTiming::After | TriggerTiming::AfterStatement => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    };
    let level = match level {
        TriggerLevel::Row => "ROW",
        TriggerLevel::Statement => "STATEMENT",
    };
    let schema = state
        .catalog
        .schema_by_id(relation.schema_id)
        .ok_or_else(|| internal_error("trigger relation schema does not exist"))?;
    let relation_oid = state.catalog.postgres_oid(match relation.target {
        TriggerTarget::Table(table_id) => PostgresOidObject::Table(table_id),
        TriggerTarget::View(view_id) => PostgresOidObject::View(view_id),
    })?;
    Ok(vec![
        Value::Null,
        Value::Null,
        Value::Text(operation.to_owned()),
        Value::Text(when.to_owned()),
        Value::Text(level.to_owned()),
        Value::Text(trigger.name.as_str().to_owned()),
        Value::Int64(i64::from(relation_oid.get())),
        Value::Text(schema.name.as_str().to_owned()),
        Value::Text(relation.name.as_str().to_owned()),
    ])
}

struct TriggerInvocation<'a> {
    timing: TriggerTiming,
    level: TriggerLevel,
    event: TriggerEvent,
    old: Option<&'a Row>,
    new: Option<&'a Row>,
}

fn execute_trigger(
    state: &mut DatabaseState,
    relation: &TriggerRelation,
    trigger_definition: &TriggerDefinition,
    invocation: TriggerInvocation<'_>,
) -> Result<(VmOutput, TriggerRowContext)> {
    if state.triggers_fired >= 16_384 {
        return Err(DbError::new("54001", "fired-trigger limit exceeded"));
    }
    let routine = state
        .catalog
        .routine_by_id(trigger_definition.routine_id)
        .cloned()
        .ok_or_else(|| DbError::new("42883", "trigger routine does not exist"))?;
    let program = compile_plpgsql(&routine.body, &trigger_argument_names())?;
    let parameters = trigger_argument_values(
        state,
        relation,
        trigger_definition,
        invocation.timing,
        invocation.level,
        invocation.event,
    )?;
    let frame = state.routine_frames.push_trigger(trigger_definition.id)?;
    state.triggers_fired += 1;
    let mut trigger = TriggerRowContext {
        table: relation.row_scope.clone(),
        old: invocation.old.cloned(),
        new: invocation.new.cloned(),
    };
    let limits = ordadb_plpgsql::ResourceLimits::default();
    let memory = VmMemoryGrant::new(limits.max_cursor_bytes)?;
    let result = {
        let mut host = EnginePlpgsqlHost {
            state,
            trigger: Some(&mut trigger),
            exception_states: Vec::new(),
            exception_triggers: Vec::new(),
            exception_charges: Vec::new(),
            exception_memory: memory.try_reserve(0)?,
            sql_dirty: false,
        };
        execute_plpgsql_with_memory(&program, &mut host, &parameters, limits, memory)
    };
    state.routine_frames.pop(frame)?;
    result.map(|output| (output, trigger))
}

fn fire_statement_triggers(
    state: &mut DatabaseState,
    table_id: TableId,
    timing: TriggerTiming,
    event: TriggerEvent,
) -> Result<bool> {
    let table = table_definition(state, table_id)?.clone();
    let relation = trigger_relation(state, TriggerTarget::Table(table_id))?;
    let mut triggers = table
        .triggers()
        .filter(|trigger| {
            trigger.enabled
                && trigger.level == TriggerLevel::Statement
                && trigger.timing == timing
                && trigger.events.contains(&event)
        })
        .cloned()
        .collect::<Vec<_>>();
    triggers.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    let fired = !triggers.is_empty();
    for trigger in triggers {
        let _ = execute_trigger(
            state,
            &relation,
            &trigger,
            TriggerInvocation {
                timing,
                level: TriggerLevel::Statement,
                event,
                old: None,
                new: None,
            },
        )?;
    }
    Ok(fired)
}

fn fire_row_triggers_with_rows(
    state: &mut DatabaseState,
    table_id: TableId,
    timing: TriggerTiming,
    event: TriggerEvent,
    old: Option<&Row>,
    new: Option<&Row>,
) -> Result<RowTriggerOutcome> {
    fire_relation_row_triggers_with_rows(
        state,
        TriggerTarget::Table(table_id),
        timing,
        event,
        old,
        new,
    )
}

fn fire_view_row_triggers_with_rows(
    state: &mut DatabaseState,
    view_id: ViewId,
    event: TriggerEvent,
    old: Option<&Row>,
    new: Option<&Row>,
) -> Result<RowTriggerOutcome> {
    fire_relation_row_triggers_with_rows(
        state,
        TriggerTarget::View(view_id),
        TriggerTiming::InsteadOf,
        event,
        old,
        new,
    )
}

fn fire_relation_row_triggers_with_rows(
    state: &mut DatabaseState,
    target: TriggerTarget,
    timing: TriggerTiming,
    event: TriggerEvent,
    old: Option<&Row>,
    new: Option<&Row>,
) -> Result<RowTriggerOutcome> {
    let relation = trigger_relation(state, target)?;
    let mut current_old = old.cloned();
    let mut current_new = new.cloned();
    let triggers = match target {
        TriggerTarget::Table(table_id) => state
            .catalog
            .table_by_id(table_id)
            .map(|table| table.triggers().cloned().collect::<Vec<_>>())
            .ok_or_else(|| internal_error("trigger table does not exist"))?,
        TriggerTarget::View(view_id) => state
            .catalog
            .view_by_id(view_id)
            .map(|view| view.triggers().cloned().collect::<Vec<_>>())
            .ok_or_else(|| internal_error("trigger view does not exist"))?,
    };
    if triggers.iter().any(|trigger| trigger.target != target) {
        return Err(DbError::new(
            "XX001",
            "trigger is stored under a different target relation",
        ));
    }
    let mut triggers = triggers
        .into_iter()
        .filter(|trigger| {
            trigger.enabled
                && trigger.level == TriggerLevel::Row
                && trigger.timing == timing
                && trigger.events.contains(&event)
        })
        .collect::<Vec<_>>();
    triggers.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    for trigger in triggers {
        let (output, trigger) = execute_trigger(
            state,
            &relation,
            &trigger,
            TriggerInvocation {
                timing,
                level: TriggerLevel::Row,
                event,
                old: current_old.as_ref(),
                new: current_new.as_ref(),
            },
        )?;
        if timing == TriggerTiming::After {
            continue;
        }
        match output.return_parameter {
            Some(parameter @ 0..=1) => {
                let returned = if parameter == 0 {
                    trigger.old
                } else {
                    trigger.new
                };
                let Some(returned) = returned else {
                    return Ok(RowTriggerOutcome::Suppress);
                };
                if event == TriggerEvent::Delete {
                    current_old = Some(returned);
                } else {
                    current_new = Some(returned);
                }
            }
            Some(parameter) => {
                return Err(DbError::new(
                    "42P02",
                    format!(
                        "trigger function returned unknown record parameter ${}",
                        parameter + 1
                    ),
                ));
            }
            None if output.return_value.is_none()
                || output.return_value.as_ref().is_some_and(Value::is_null) =>
            {
                return Ok(RowTriggerOutcome::Suppress);
            }
            None => {
                return Err(DbError::new(
                    "42804",
                    "row trigger functions must return OLD, NEW, or NULL",
                ));
            }
        }
    }
    Ok(RowTriggerOutcome::Proceed(
        if event == TriggerEvent::Delete {
            current_old
        } else {
            current_new
        },
    ))
}

fn execute_view_select(
    state: &mut DatabaseState,
    source: BoundStatement,
    schema: Schema,
    projection: Vec<usize>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let (mut events, dirty) = execute_bound(state, source, params)?;
    if dirty {
        return Err(internal_error(
            "a stored view query attempted to mutate state",
        ));
    }
    for event in &mut events {
        match event {
            QueryEvent::Schema(event_schema) => *event_schema = schema.clone(),
            QueryEvent::Batch(batch) => {
                for row in &mut batch.rows {
                    row.values = projection
                        .iter()
                        .map(|position| {
                            row.values.get(*position).cloned().ok_or_else(|| {
                                internal_error("stored view projection is outside its row width")
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                }
            }
            QueryEvent::Progress(_) | QueryEvent::Notice(_) | QueryEvent::Complete(_) => {}
        }
    }
    Ok((events, false))
}

fn execute_drop_objects(
    state: &mut DatabaseState,
    kind: DdlObjectKind,
    objects: Vec<CatalogObjectRef>,
    behavior: DropBehavior,
) -> Result<(Vec<QueryEvent>, bool)> {
    if objects.is_empty() {
        return Ok((
            command_events(Schema::empty(), drop_command_tag(kind), 0, None),
            false,
        ));
    }
    let catalog_before = Arc::clone(&state.catalog);
    let mut removed = Vec::new();
    for object in objects {
        let dropped = drop_catalog_root(Arc::make_mut(&mut state.catalog), object, behavior)?;
        for object in dropped {
            if !removed.contains(&object) {
                removed.push(object);
            }
        }
    }

    let backing_tables = removed
        .iter()
        .filter_map(|object| match object {
            CatalogObjectRef::View(view_id) => catalog_before
                .view_by_id(*view_id)
                .and_then(|view| view.materialized_table_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    for table_id in backing_tables {
        if state.catalog.table_by_id(table_id).is_some() {
            for object in
                Arc::make_mut(&mut state.catalog).drop_table(table_id, DropBehavior::Cascade)?
            {
                if !removed.contains(&object) {
                    removed.push(object);
                }
            }
        }
    }
    cleanup_removed_columns(state, &catalog_before, &removed)?;
    cleanup_removed_state(state, &removed);
    reconcile_search_catalog(state)?;
    Ok((
        command_events(Schema::empty(), drop_command_tag(kind), 0, None),
        true,
    ))
}

fn drop_catalog_root(
    catalog: &mut Catalog,
    object: CatalogObjectRef,
    behavior: DropBehavior,
) -> Result<Vec<CatalogObjectRef>> {
    match object {
        CatalogObjectRef::Schema(id) if catalog.schema_by_id(id).is_some() => {
            catalog.drop_schema(id, behavior)
        }
        CatalogObjectRef::Table(id) if catalog.table_by_id(id).is_some() => {
            catalog.drop_table(id, behavior)
        }
        CatalogObjectRef::Index(id) if catalog.index_by_id(id).is_some() => {
            catalog.drop_index(id, behavior)
        }
        CatalogObjectRef::Sequence(id) if catalog.sequence_by_id(id).is_some() => {
            catalog.drop_sequence(id, behavior)
        }
        CatalogObjectRef::View(id) if catalog.view_by_id(id).is_some() => {
            catalog.drop_view(id, behavior)
        }
        CatalogObjectRef::Constraint(id) if catalog.constraint_by_id(id).is_some() => {
            catalog.drop_constraint(id, behavior)
        }
        CatalogObjectRef::Routine(id) if catalog.routine_by_id(id).is_some() => {
            catalog.drop_routine(id, behavior)
        }
        CatalogObjectRef::Trigger(id) if catalog.trigger_by_id(id).is_some() => {
            catalog.drop_trigger(id, behavior)
        }
        CatalogObjectRef::Type(id) if catalog.type_by_id(id).is_some() => {
            catalog.drop_type(id, behavior)
        }
        CatalogObjectRef::Column(_, _) => Err(internal_error(
            "column drops must be routed through ALTER TABLE",
        )),
        _ => Ok(Vec::new()),
    }
}

fn cleanup_removed_state(state: &mut DatabaseState, removed: &[CatalogObjectRef]) {
    for object in removed {
        match object {
            CatalogObjectRef::Table(table_id) => {
                state.rows.remove(table_id);
            }
            CatalogObjectRef::Index(index_id) => {
                state.indexes.remove(index_id);
            }
            _ => {}
        }
    }
    state
        .rows
        .retain(|table_id, _| state.catalog.table_by_id(*table_id).is_some());
    state
        .indexes
        .retain(|index_id, _| state.catalog.index_by_id(*index_id).is_some());
}

fn cleanup_removed_columns(
    state: &mut DatabaseState,
    catalog_before: &Catalog,
    removed: &[CatalogObjectRef],
) -> Result<()> {
    let mut positions_by_table = BTreeMap::<TableId, Vec<usize>>::new();
    for object in removed {
        let CatalogObjectRef::Column(table_id, column_id) = object else {
            continue;
        };
        if state.catalog.table_by_id(*table_id).is_none() {
            continue;
        }
        let position = catalog_before
            .table_by_id(*table_id)
            .and_then(|table| table.column_index_by_id(*column_id))
            .ok_or_else(|| internal_error("dropped column is absent from the prior catalog"))?;
        positions_by_table
            .entry(*table_id)
            .or_default()
            .push(position);
    }
    for (table_id, positions) in &mut positions_by_table {
        positions.sort_unstable_by(|left, right| right.cmp(left));
        positions.dedup();
        for row in Arc::make_mut(
            state
                .rows
                .entry(*table_id)
                .or_insert_with(|| Arc::new(Vec::new())),
        ) {
            for position in positions.iter().copied() {
                if position >= row.values.len() {
                    return Err(internal_error(
                        "dropped column position exceeds the stored row width",
                    ));
                }
                row.values.remove(position);
            }
        }
    }
    Ok(())
}

fn drop_command_tag(kind: DdlObjectKind) -> &'static str {
    match kind {
        DdlObjectKind::Schema => "DROP SCHEMA",
        DdlObjectKind::Table => "DROP TABLE",
        DdlObjectKind::Index => "DROP INDEX",
        DdlObjectKind::Sequence => "DROP SEQUENCE",
        DdlObjectKind::View => "DROP VIEW",
        DdlObjectKind::MaterializedView => "DROP MATERIALIZED VIEW",
        DdlObjectKind::Type => "DROP TYPE",
    }
}

fn rewrite_enum_values(
    state: &mut DatabaseState,
    type_id: TypeId,
    renamed_label: Option<(&str, &str)>,
) -> Result<()> {
    let affected = state
        .catalog
        .database()
        .schemas()
        .flat_map(|schema| schema.tables())
        .filter_map(|table| {
            let columns = table
                .columns()
                .iter()
                .enumerate()
                .filter(|(_, column)| {
                    column.declared_type.is_some_and(|declared_type| {
                        declared_type == type_id
                            || state
                                .catalog
                                .type_by_id(declared_type)
                                .is_some_and(|definition| {
                                    matches!(
                                        definition.definition,
                                        UserDefinedTypeKind::Domain {
                                            base_declared_type: Some(base_type_id),
                                            ..
                                        } if base_type_id == type_id
                                    )
                                })
                    })
                })
                .map(|(index, column)| (index, column.data_type.clone()))
                .collect::<Vec<_>>();
            (!columns.is_empty()).then_some((table.id, columns))
        })
        .collect::<Vec<_>>();
    for (table_id, columns) in affected {
        for row in Arc::make_mut(
            state
                .rows
                .entry(table_id)
                .or_insert_with(|| Arc::new(Vec::new())),
        ) {
            for (index, data_type) in &columns {
                let value = row.values.get_mut(*index).ok_or_else(|| {
                    internal_error("enum column position exceeds the stored row width")
                })?;
                rewrite_enum_column_value(value, data_type, renamed_label)?;
            }
        }
        rebuild_table_derived(state, table_id)?;
    }
    validate_database_rows(state)
}

fn rewrite_enum_column_value(
    value: &mut Value,
    data_type: &ScalarType,
    renamed_label: Option<(&str, &str)>,
) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        Value::Text(label) => {
            if let Some((old_label, new_label)) = renamed_label
                && label == old_label
            {
                *label = new_label.to_owned();
            }
            Ok(())
        }
        Value::Array(array) => {
            let ScalarType::Array { element } = data_type else {
                return Err(internal_error(
                    "enum array value is paired with a non-array declared type",
                ));
            };
            let mut values = array.values().to_vec();
            for value in &mut values {
                if let (Value::Text(label), Some((old_label, new_label))) = (value, renamed_label)
                    && label == old_label
                {
                    *label = new_label.to_owned();
                }
            }
            *array = PgArray::new((**element).clone(), array.dimensions().to_vec(), values)?;
            Ok(())
        }
        _ => Err(DbError::new(
            "42804",
            "stored value is not assignable to the altered enum type",
        )),
    }
}

fn execute_alter_table(
    state: &mut DatabaseState,
    table_id: TableId,
    operations: Vec<BoundAlterTableOperation>,
) -> Result<(Vec<QueryEvent>, bool)> {
    for operation in operations {
        match operation {
            BoundAlterTableOperation::RenameTable { new_name } => {
                Arc::make_mut(&mut state.catalog).rename_table(table_id, new_name)?;
            }
            BoundAlterTableOperation::RenameColumn {
                column_id,
                new_name,
            } => {
                Arc::make_mut(&mut state.catalog).rename_column(table_id, column_id, new_name)?;
            }
            BoundAlterTableOperation::AddColumn {
                column,
                if_not_exists,
            } => {
                if table_definition(state, table_id)?
                    .column(&column.name)
                    .is_some()
                    && if_not_exists
                {
                    continue;
                }
                let value = new_column_default_value(&state.catalog, &column)?;
                Arc::make_mut(&mut state.catalog).add_column(table_id, column)?;
                for row in Arc::make_mut(
                    state
                        .rows
                        .entry(table_id)
                        .or_insert_with(|| Arc::new(Vec::new())),
                ) {
                    row.values.push(value.clone());
                }
            }
            BoundAlterTableOperation::DropColumns {
                column_ids,
                if_exists: _,
                behavior,
            } => {
                let table = table_definition(state, table_id)?.clone();
                let mut positions = column_ids
                    .iter()
                    .filter_map(|column_id| table.column_index_by_id(*column_id))
                    .collect::<Vec<_>>();
                for column_id in column_ids {
                    if state
                        .catalog
                        .table_by_id(table_id)
                        .is_some_and(|table| table.column_index_by_id(column_id).is_some())
                    {
                        Arc::make_mut(&mut state.catalog)
                            .drop_column(table_id, column_id, behavior)?;
                    }
                }
                positions.sort_unstable_by(|left, right| right.cmp(left));
                for row in Arc::make_mut(
                    state
                        .rows
                        .entry(table_id)
                        .or_insert_with(|| Arc::new(Vec::new())),
                ) {
                    for position in &positions {
                        row.values.remove(*position);
                    }
                }
            }
            BoundAlterTableOperation::SetNotNull { column_id } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    Some(false),
                    None,
                    None,
                )?;
            }
            BoundAlterTableOperation::DropNotNull { column_id } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    Some(true),
                    None,
                    None,
                )?;
            }
            BoundAlterTableOperation::SetDefault { column_id, default } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    None,
                    Some(Some(default)),
                    None,
                )?;
            }
            BoundAlterTableOperation::DropDefault { column_id } => {
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    None,
                    None,
                    Some(None),
                    None,
                )?;
            }
            BoundAlterTableOperation::SetDataType {
                column_id,
                data_type,
                declared_type,
            } => {
                let position = table_definition(state, table_id)?
                    .column_index_by_id(column_id)
                    .ok_or_else(|| DbError::new("42703", "column does not exist"))?;
                for row in Arc::make_mut(
                    state
                        .rows
                        .entry(table_id)
                        .or_insert_with(|| Arc::new(Vec::new())),
                ) {
                    row.values[position] =
                        coerce_execution_value(row.values[position].clone(), &data_type)?;
                }
                Arc::make_mut(&mut state.catalog).alter_column(
                    table_id,
                    column_id,
                    Some(data_type),
                    None,
                    None,
                    Some(declared_type),
                )?;
            }
            BoundAlterTableOperation::AddConstraint { constraint } => {
                Arc::make_mut(&mut state.catalog).create_constraint(table_id, constraint)?;
            }
            BoundAlterTableOperation::DropConstraint {
                constraint_id,
                if_exists: _,
                behavior,
            } => {
                if let Some(constraint_id) = constraint_id {
                    let removed = Arc::make_mut(&mut state.catalog)
                        .drop_constraint(constraint_id, behavior)?;
                    cleanup_removed_state(state, &removed);
                }
            }
            BoundAlterTableOperation::SetTriggerEnabled {
                trigger_id,
                name,
                enabled,
            } => {
                let trigger_id = trigger_id.ok_or_else(|| {
                    DbError::new("42704", format!("trigger {name} does not exist"))
                })?;
                Arc::make_mut(&mut state.catalog).set_trigger_enabled(trigger_id, enabled)?;
            }
        }
    }
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    Ok((
        command_events(Schema::empty(), "ALTER TABLE", 0, None),
        true,
    ))
}

fn catalog_default_value(
    catalog: &Catalog,
    expression: Option<&CatalogExpression>,
    data_type: &ScalarType,
) -> Result<Value> {
    let Some(expression) = expression else {
        return Ok(Value::Null);
    };
    let bound = bind_catalog_expression_with_catalog(expression, None, Some(data_type), catalog)?;
    evaluate_scalar(&bound, &[], &[])
}

fn column_default_value(catalog: &Catalog, column: &ColumnDefinition) -> Result<Value> {
    declared_column_default_value(
        catalog,
        column.default.as_ref(),
        column.declared_type,
        &column.data_type,
    )
}

fn new_column_default_value(catalog: &Catalog, column: &NewColumn) -> Result<Value> {
    declared_column_default_value(
        catalog,
        column.default.as_ref(),
        column.declared_type,
        &column.data_type,
    )
}

fn declared_column_default_value(
    catalog: &Catalog,
    explicit_default: Option<&CatalogExpression>,
    declared_type: Option<TypeId>,
    data_type: &ScalarType,
) -> Result<Value> {
    let domain_default =
        if explicit_default.is_none() && !matches!(data_type, ScalarType::Array { .. }) {
            declared_type
                .and_then(|type_id| catalog.type_by_id(type_id))
                .and_then(|definition| match &definition.definition {
                    UserDefinedTypeKind::Domain { default, .. } => default.as_ref(),
                    UserDefinedTypeKind::Enum { .. } => None,
                })
        } else {
            None
        };
    catalog_default_value(catalog, explicit_default.or(domain_default), data_type)
}

fn execute_create_view(
    state: &mut DatabaseState,
    view: CreateViewExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let tag = match view.kind {
        ViewKind::Regular => "CREATE VIEW",
        ViewKind::Materialized => "CREATE MATERIALIZED VIEW",
    };
    if view.existing.is_some() && view.if_not_exists && !view.replace {
        return Ok((command_events(Schema::empty(), tag, 0, None), false));
    }
    let materialized_rows = if view.kind == ViewKind::Materialized && view.with_data {
        Some(materialize_statement_rows(
            state,
            view.query.clone(),
            params,
        )?)
    } else {
        None
    };

    if let Some(view_id) = view.existing {
        let current = state
            .catalog
            .view_by_id(view_id)
            .cloned()
            .ok_or_else(|| DbError::new("42P01", "view does not exist"))?;
        if current.kind != view.kind {
            return Err(DbError::new(
                "42809",
                "cannot replace a view with a different relation kind",
            ));
        }
        Arc::make_mut(&mut state.catalog).replace_view(
            view_id,
            view.query_sql,
            view.output,
            view.kind == ViewKind::Regular || view.with_data,
            view.references,
        )?;
        if let Some(table_id) = current.materialized_table_id {
            state
                .rows
                .insert(table_id, Arc::new(materialized_rows.unwrap_or_default()));
            rebuild_table_derived(state, table_id)?;
        }
        return Ok((command_events(Schema::empty(), tag, 0, None), true));
    }

    let materialized_table_id = if view.kind == ViewKind::Materialized {
        let backing_name = Identifier::unquoted(format!("__ordadb_mv_{}", view.name.as_str()));
        let columns = view
            .output
            .fields
            .iter()
            .map(|field| NewColumn {
                name: Identifier::unquoted(field.name.clone()),
                data_type: field.data_type.clone(),
                declared_type: None,
                nullable: field.nullable,
                primary_key: false,
                unique: false,
                default: None,
            })
            .collect();
        let table_id =
            Arc::make_mut(&mut state.catalog).create_table(&view.schema, backing_name, columns)?;
        state
            .rows
            .insert(table_id, Arc::new(materialized_rows.unwrap_or_default()));
        rebuild_table_derived(state, table_id)?;
        Some(table_id)
    } else {
        None
    };
    Arc::make_mut(&mut state.catalog).create_view(
        &view.schema,
        NewView {
            name: view.name,
            kind: view.kind,
            query: view.query_sql,
            output: view.output,
            materialized_table_id,
            populated: view.kind == ViewKind::Regular || view.with_data,
            references: view.references,
        },
    )?;
    Ok((command_events(Schema::empty(), tag, 0, None), true))
}

fn materialize_statement_rows(
    state: &mut DatabaseState,
    statement: BoundStatement,
    params: &[Value],
) -> Result<Vec<Row>> {
    let (events, dirty) = execute_bound(state, statement, params)?;
    if dirty {
        return Err(internal_error(
            "a materialized query attempted to mutate database state",
        ));
    }
    Ok(events
        .into_iter()
        .filter_map(|event| match event {
            QueryEvent::Batch(batch) => Some(batch.rows),
            _ => None,
        })
        .flatten()
        .collect())
}

struct EnginePlpgsqlHost<'a> {
    state: &'a mut DatabaseState,
    trigger: Option<&'a mut TriggerRowContext>,
    exception_states: Vec<DatabaseState>,
    exception_triggers: Vec<Option<TriggerRowSavepoint>>,
    exception_charges: Vec<usize>,
    exception_memory: VmMemoryReservation,
    sql_dirty: bool,
}

fn estimated_btree_clone_bytes<K, V>(len: usize) -> usize {
    len.saturating_mul(
        std::mem::size_of::<(K, V)>().saturating_add(4 * std::mem::size_of::<usize>()),
    )
}

fn estimated_database_state_snapshot_bytes(state: &DatabaseState) -> Result<usize> {
    let routine_frames = state
        .routine_frames
        .arena
        .capacity()
        .saturating_mul(std::mem::size_of::<Option<RoutineFrame>>())
        .saturating_add(
            state
                .routine_frames
                .free
                .capacity()
                .saturating_mul(std::mem::size_of::<usize>()),
        )
        .saturating_add(
            state
                .routine_frames
                .active
                .capacity()
                .saturating_mul(std::mem::size_of::<RoutineFrameId>()),
        );
    let notices = state
        .pending_notices
        .capacity()
        .saturating_mul(std::mem::size_of::<DbNotice>())
        .saturating_add(
            state
                .pending_notices
                .iter()
                .map(|notice| {
                    notice
                        .sql_state
                        .capacity()
                        .saturating_add(notice.message.capacity())
                        .saturating_add(notice.detail.as_ref().map_or(0, |value| value.len()))
                        .saturating_add(notice.hint.as_ref().map_or(0, |value| value.len()))
                })
                .sum::<usize>(),
        );
    let listener_actions = state
        .pending_notifications
        .listener_actions
        .capacity()
        .saturating_mul(std::mem::size_of::<NotificationListenerAction>())
        .saturating_add(
            state
                .pending_notifications
                .listener_actions
                .iter()
                .map(|action| match action {
                    NotificationListenerAction::Listen(channel)
                    | NotificationListenerAction::Unlisten(channel) => channel.as_str().len(),
                    NotificationListenerAction::UnlistenAll => 0,
                })
                .sum::<usize>(),
        );
    let notifications = state
        .pending_notifications
        .notifications
        .capacity()
        .saturating_mul(std::mem::size_of::<(Identifier, String)>())
        .saturating_add(
            state
                .pending_notifications
                .notifications
                .iter()
                .map(|(channel, payload)| channel.as_str().len().saturating_add(payload.capacity()))
                .sum::<usize>(),
        );
    let coalesced = state
        .pending_notifications
        .coalesced
        .iter()
        .map(|(channel, payload)| {
            std::mem::size_of::<(Identifier, String)>()
                .saturating_add(channel.as_str().len())
                .saturating_add(payload.capacity())
                .saturating_add(4 * std::mem::size_of::<usize>())
        })
        .sum::<usize>();
    let total = std::mem::size_of::<DatabaseState>()
        .saturating_add(estimated_btree_clone_bytes::<TableId, Arc<Vec<Row>>>(
            state.rows.len(),
        ))
        .saturating_add(
            estimated_btree_clone_bytes::<TableId, Arc<Vec<VersionedRow>>>(state.versions.len()),
        )
        .saturating_add(estimated_btree_clone_bytes::<TableId, Arc<Vec<u32>>>(
            state.visible_versions.len(),
        ))
        .saturating_add(estimated_btree_clone_bytes::<IndexId, Arc<BPlusTree>>(
            state.indexes.len(),
        ))
        .saturating_add(estimated_btree_clone_bytes::<SequenceId, i64>(
            state.sequence_currvals.len(),
        ))
        .saturating_add(routine_frames)
        .saturating_add(notices)
        .saturating_add(listener_actions)
        .saturating_add(notifications)
        .saturating_add(coalesced);
    if total == usize::MAX {
        return Err(DbError::new(
            "53200",
            "PL/pgSQL exception savepoint memory accounting overflowed",
        ));
    }
    Ok(total)
}

fn estimated_trigger_savepoint_bytes(trigger: Option<&TriggerRowContext>) -> usize {
    trigger.map_or(0, |trigger| {
        std::mem::size_of::<TriggerRowSavepoint>()
            .saturating_add(trigger.old.as_ref().map_or(0, estimated_row_bytes))
            .saturating_add(trigger.new.as_ref().map_or(0, estimated_row_bytes))
    })
}

impl PlpgsqlHost for EnginePlpgsqlHost<'_> {
    fn execute_sql(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> Result<Box<dyn Iterator<Item = Result<QueryEvent>>>> {
        let (sql, parameters, _) =
            expand_trigger_record_fields(sql, parameters, self.trigger.as_deref())?;
        let statement = resolve_sequence_currval(
            bind(parse(&sql)?, &self.state.catalog)?,
            &self.state.sequence_currvals,
        )?;
        if let BoundStatement::PgNotify {
            channel,
            payload,
            schema,
        } = &statement
        {
            let (channel, payload) = evaluate_pg_notify(channel, payload, &parameters)?;
            self.state.pending_notifications.notify(channel, payload);
            return Ok(Box::new(
                pg_notify_events(schema.clone()).into_iter().map(Ok),
            ));
        }
        if let BoundStatement::Notify { channel, payload } = &statement {
            self.state
                .pending_notifications
                .notify(channel.clone(), payload.clone());
            return Ok(Box::new(transaction_events("NOTIFY").into_iter().map(Ok)));
        }
        if matches!(
            statement,
            BoundStatement::Commit { .. } | BoundStatement::Rollback { .. }
        ) {
            return Err(DbError::new(
                "2D000",
                "invalid transaction termination inside this PL/pgSQL invocation",
            )
            .with_hint(
                "transaction termination requires an eligible top-level procedure CALL in autocommit mode",
            ));
        }
        if matches!(
            statement,
            BoundStatement::Begin { .. }
                | BoundStatement::Savepoint { .. }
                | BoundStatement::RollbackTo { .. }
                | BoundStatement::ReleaseSavepoint { .. }
        ) {
            return Err(DbError::new(
                "0A000",
                "this transaction control command is not allowed inside PL/pgSQL",
            ));
        }
        if let Some(stream) = prepare_read_stream(self.state, statement.clone(), &parameters, None)?
        {
            return Ok(Box::new(stream));
        }
        let (events, dirty) = execute_bound_with_ownership(self.state, statement, &parameters)?;
        self.sql_dirty |= dirty;
        Ok(Box::new(events.into_iter().map(Ok)))
    }

    fn evaluate_expression(&mut self, sql: &str, parameters: &[Value]) -> Result<Value> {
        if let Some(trigger) = self.trigger.as_deref()
            && let Some((slot, field)) = trigger_field_reference(sql)
        {
            return trigger.value(slot, field);
        }
        if let Some(index) = sql
            .trim()
            .strip_prefix('$')
            .and_then(|index| index.parse::<usize>().ok())
        {
            return parameters
                .get(index.saturating_sub(1))
                .cloned()
                .ok_or_else(|| DbError::new("42P02", format!("there is no parameter ${index}")));
        }
        let (sql, parameters, mut parameter_types) =
            expand_trigger_record_fields(sql, parameters, self.trigger.as_deref())?;
        for (index, value) in parameters.iter().enumerate() {
            if let Some(data_type) = scalar_type_of_value(value) {
                parameter_types.entry(index + 1).or_insert(data_type);
            }
        }
        let expression = CatalogExpression::new(sql);
        let bound = bind_catalog_expression_with_parameter_types_and_catalog(
            &expression,
            None,
            None,
            &parameter_types,
            Some(&self.state.catalog),
        )?;
        evaluate_scalar(&bound, &[], &parameters)
    }

    fn assign_composite_field(&mut self, slot: usize, field: &str, value: Value) -> Result<()> {
        self.trigger
            .as_deref_mut()
            .ok_or_else(|| {
                DbError::new(
                    "0A000",
                    "composite assignment is only available in row triggers",
                )
            })?
            .assign(slot, field, value)
    }

    fn resolve_row_type(&mut self, relation: &str) -> Result<Vec<String>> {
        let statement = bind(
            parse(&format!("SELECT * FROM {relation} LIMIT 0"))?,
            &self.state.catalog,
        )?;
        let schema = match statement {
            BoundStatement::Select { schema, .. }
            | BoundStatement::AdvancedSelect { schema, .. }
            | BoundStatement::ViewSelect { schema, .. } => schema,
            _ => {
                return Err(DbError::new(
                    "42809",
                    format!("relation {relation} does not expose a row type"),
                ));
            }
        };
        Ok(schema.fields.into_iter().map(|field| field.name).collect())
    }

    fn begin_exception_block(&mut self) -> Result<()> {
        if self.exception_states.len() >= 128 {
            return Err(DbError::new(
                "54001",
                "PL/pgSQL exception block depth exceeds the maximum of 128",
            ));
        }
        let charge = estimated_database_state_snapshot_bytes(self.state)?
            .saturating_add(estimated_trigger_savepoint_bytes(self.trigger.as_deref()))
            .saturating_add(std::mem::size_of::<usize>());
        let next = self
            .exception_memory
            .bytes()
            .checked_add(charge)
            .ok_or_else(|| {
                DbError::new(
                    "53200",
                    "PL/pgSQL exception savepoint memory accounting overflowed",
                )
            })?;
        self.exception_memory.resize(next)?;
        self.exception_states.push(self.state.clone());
        self.exception_triggers
            .push(self.trigger.as_deref().map(|trigger| TriggerRowSavepoint {
                old: trigger.old.clone(),
                new: trigger.new.clone(),
            }));
        self.exception_charges.push(charge);
        Ok(())
    }

    fn commit_exception_block(&mut self) -> Result<()> {
        self.exception_states
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL exception savepoint stack is empty"))?;
        self.exception_triggers
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL trigger savepoint stack is empty"))?;
        let charge = self
            .exception_charges
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL exception memory stack is empty"))?;
        self.exception_memory
            .resize(self.exception_memory.bytes().saturating_sub(charge))?;
        Ok(())
    }

    fn rollback_exception_block(&mut self) -> Result<()> {
        let pending_notices = mem::take(&mut self.state.pending_notices);
        let saved = self
            .exception_states
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL exception savepoint is not active"))?;
        *self.state = saved;
        self.state.pending_notices = pending_notices;
        let saved_trigger = self
            .exception_triggers
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL trigger savepoint stack is empty"))?;
        let charge = self
            .exception_charges
            .pop()
            .ok_or_else(|| internal_error("PL/pgSQL exception memory stack is empty"))?;
        self.exception_memory
            .resize(self.exception_memory.bytes().saturating_sub(charge))?;
        if let Some(trigger) = self.trigger.as_deref_mut()
            && let Some(saved) = saved_trigger
        {
            trigger.old = saved.old;
            trigger.new = saved.new;
        }
        Ok(())
    }

    fn emit_notice(&mut self, notice: DbNotice) -> Result<()> {
        if notice.message.len() > MAX_PLPGSQL_NOTICE_BYTES {
            return Err(DbError::new(
                "54000",
                "PL/pgSQL notice message exceeds the configured byte limit",
            ));
        }
        if self.state.pending_notices.len() >= MAX_PLPGSQL_NOTICES {
            return Err(DbError::new(
                "54001",
                "PL/pgSQL notice count exceeds the configured limit",
            ));
        }
        self.state.pending_notices.push(notice);
        Ok(())
    }

    fn check_cancelled(&self) -> Result<()> {
        if self
            .state
            .cancellation
            .as_ref()
            .is_some_and(|cancellation| cancellation.load(Ordering::Acquire))
        {
            Err(DbError::new("57014", "query was cancelled"))
        } else {
            Ok(())
        }
    }
}

fn scalar_type_of_value(value: &Value) -> Option<ScalarType> {
    value.scalar_type()
}

fn trigger_field_reference(expression: &str) -> Option<(usize, &str)> {
    let (parameter, field) = expression.trim().strip_prefix('$')?.split_once('.')?;
    if field.is_empty()
        || !field
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '_')
    {
        return None;
    }
    Some((parameter.parse::<usize>().ok()?.checked_sub(1)?, field))
}

fn expand_trigger_record_fields(
    sql: &str,
    parameters: &[Value],
    trigger: Option<&TriggerRowContext>,
) -> Result<(String, Vec<Value>, BTreeMap<usize, ScalarType>)> {
    let Some(trigger) = trigger else {
        return Ok((sql.to_owned(), parameters.to_vec(), BTreeMap::new()));
    };
    let characters = sql.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(sql.len());
    let mut expanded = parameters.to_vec();
    let mut parameter_types = BTreeMap::new();
    let mut quote = None;
    let mut index = 0usize;
    while index < characters.len() {
        let character = characters[index];
        if let Some(delimiter) = quote {
            output.push(character);
            if character == delimiter {
                if characters.get(index + 1) == Some(&delimiter) {
                    output.push(delimiter);
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character);
            output.push(character);
            index += 1;
            continue;
        }
        if character != '$' {
            output.push(character);
            index += 1;
            continue;
        }
        let digits_start = index + 1;
        let mut cursor = digits_start;
        while characters.get(cursor).is_some_and(char::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == digits_start || characters.get(cursor) != Some(&'.') {
            output.push(character);
            index += 1;
            continue;
        }
        let field_start = cursor + 1;
        cursor = field_start;
        while characters
            .get(cursor)
            .is_some_and(|value| value.is_ascii_alphanumeric() || *value == '_')
        {
            cursor += 1;
        }
        if cursor == field_start {
            return Err(DbError::new(
                "42601",
                "trigger record access requires an unquoted field name",
            ));
        }
        let parameter = characters[digits_start..field_start - 1]
            .iter()
            .collect::<String>()
            .parse::<usize>()
            .map_err(|_| DbError::new("42P02", "invalid trigger record parameter"))?
            .checked_sub(1)
            .ok_or_else(|| DbError::new("42P02", "trigger parameters are one-based"))?;
        let field = characters[field_start..cursor].iter().collect::<String>();
        let (value, data_type) = trigger.value_and_type(parameter, &field)?;
        expanded.push(value);
        parameter_types.insert(expanded.len(), data_type);
        output.push('$');
        output.push_str(&expanded.len().to_string());
        index = cursor;
    }
    Ok((output, expanded, parameter_types))
}

#[derive(Debug, Clone, Copy)]
struct PlannedMergeAction {
    clause_index: usize,
    target_position: Option<usize>,
    source_position: Option<usize>,
}

fn execute_merge(
    state: &mut DatabaseState,
    merge: BoundMerge,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let BoundMerge {
        target,
        source,
        on,
        clauses,
        returning,
    } = merge;
    if target.offset != 0 || source.offset != target.width {
        return Err(internal_error(
            "MERGE input column offsets are inconsistent",
        ));
    }
    let statement_events = clauses
        .iter()
        .filter_map(|clause| match clause.action {
            BoundMergeAction::Insert { .. } => Some(TriggerEvent::Insert),
            BoundMergeAction::Update { .. } => Some(TriggerEvent::Update),
            BoundMergeAction::Delete => Some(TriggerEvent::Delete),
            BoundMergeAction::DoNothing => None,
        })
        .collect::<BTreeSet<_>>();
    let mut statement_trigger_fired = false;
    for event in &statement_events {
        statement_trigger_fired |= fire_statement_triggers(
            state,
            target.table_id,
            TriggerTiming::BeforeStatement,
            *event,
        )?;
    }
    let target_definition = table_definition(state, target.table_id)?.clone();
    table_definition(state, source.table_id)?;
    let target_rows = state
        .rows
        .get(&target.table_id)
        .cloned()
        .unwrap_or_default();
    let source_rows = state
        .rows
        .get(&source.table_id)
        .cloned()
        .unwrap_or_default();
    let memory = MemoryGrant::new(DEFAULT_SOFT_MEMORY_BYTES, DEFAULT_HARD_MEMORY_BYTES)?;
    let mut plan_reservation = memory.try_reserve(0)?;
    let mut input_reservation = memory.try_reserve(0)?;
    let mut returning_reservation = memory.try_reserve(0)?;
    let mut planned = Vec::new();
    let mut affected_targets = BTreeSet::new();
    let mut matched_targets = BTreeSet::new();

    for (source_position, source_row) in source_rows.iter().enumerate() {
        ensure_statement_not_cancelled(state)?;
        let mut matched = false;
        for (target_position, target_row) in target_rows.iter().enumerate() {
            ensure_statement_not_cancelled(state)?;
            let input = merge_input_row(
                Some(target_row),
                target.width,
                source_row,
                &mut input_reservation,
            )?;
            if !execution_predicate_matches(&on, &input, params)? {
                continue;
            }
            matched = true;
            if matched_targets.insert(target_position) {
                plan_reservation.grow(mem::size_of::<usize>() * 4)?;
            }
            let Some(clause_index) = first_matching_merge_clause(
                &clauses,
                BoundMergeClauseKind::Matched,
                &input,
                params,
            )?
            else {
                continue;
            };
            if matches!(clauses[clause_index].action, BoundMergeAction::DoNothing) {
                continue;
            }
            if !matches!(
                clauses[clause_index].action,
                BoundMergeAction::Update { .. } | BoundMergeAction::Delete
            ) {
                return Err(internal_error(
                    "MERGE matched clause contains an invalid action",
                ));
            }
            if !affected_targets.insert(target_position) {
                return Err(
                    DbError::new("21000", "MERGE command cannot affect row a second time")
                        .with_hint(
                            "Ensure that no more than one source row matches each target row.",
                        ),
                );
            }
            plan_reservation
                .grow(mem::size_of::<PlannedMergeAction>() + mem::size_of::<usize>() * 4)?;
            planned.push(PlannedMergeAction {
                clause_index,
                target_position: Some(target_position),
                source_position: Some(source_position),
            });
        }
        if matched {
            continue;
        }
        let input = merge_input_row(None, target.width, source_row, &mut input_reservation)?;
        let Some(clause_index) = first_matching_merge_clause(
            &clauses,
            BoundMergeClauseKind::NotMatchedByTarget,
            &input,
            params,
        )?
        else {
            continue;
        };
        if matches!(clauses[clause_index].action, BoundMergeAction::DoNothing) {
            continue;
        }
        if !matches!(
            clauses[clause_index].action,
            BoundMergeAction::Insert { .. }
        ) {
            return Err(internal_error(
                "MERGE not-matched clause contains an invalid action",
            ));
        }
        plan_reservation.grow(mem::size_of::<PlannedMergeAction>())?;
        planned.push(PlannedMergeAction {
            clause_index,
            target_position: None,
            source_position: Some(source_position),
        });
    }

    let null_source = Row::new(vec![Value::Null; source.width]);
    input_reservation.grow(estimated_row_bytes(&null_source))?;
    for (target_position, target_row) in target_rows.iter().enumerate() {
        ensure_statement_not_cancelled(state)?;
        if matched_targets.contains(&target_position) {
            continue;
        }
        let input = merge_input_row(
            Some(target_row),
            target.width,
            &null_source,
            &mut input_reservation,
        )?;
        let Some(clause_index) = first_matching_merge_clause(
            &clauses,
            BoundMergeClauseKind::NotMatchedBySource,
            &input,
            params,
        )?
        else {
            continue;
        };
        if matches!(clauses[clause_index].action, BoundMergeAction::DoNothing) {
            continue;
        }
        if !matches!(
            clauses[clause_index].action,
            BoundMergeAction::Update { .. } | BoundMergeAction::Delete
        ) {
            return Err(internal_error(
                "MERGE not-matched-by-source clause contains an invalid action",
            ));
        }
        if !affected_targets.insert(target_position) {
            return Err(internal_error("MERGE planned a target row more than once"));
        }
        plan_reservation
            .grow(mem::size_of::<PlannedMergeAction>() + mem::size_of::<usize>() * 4)?;
        planned.push(PlannedMergeAction {
            clause_index,
            target_position: Some(target_position),
            source_position: None,
        });
    }

    let mut affected = 0_u64;
    let mut returned_rows = Vec::new();
    let mut deleted_targets = BTreeSet::new();
    for action in planned {
        ensure_statement_not_cancelled(state)?;
        let source_row = action
            .source_position
            .map_or(Ok(&null_source), |source_position| {
                source_rows.get(source_position).ok_or_else(|| {
                    internal_error("MERGE source row is outside its statement snapshot")
                })
            })?;
        let clause = clauses
            .get(action.clause_index)
            .ok_or_else(|| internal_error("MERGE clause index is out of bounds"))?;
        let changed_row = match (&clause.action, action.target_position) {
            (BoundMergeAction::Update { assignments }, Some(target_position)) => {
                let old_row = target_rows.get(target_position).ok_or_else(|| {
                    internal_error("MERGE target row is outside its statement snapshot")
                })?;
                let current_position =
                    current_merge_target_position(target_position, &deleted_targets)?;
                let input = merge_input_row(
                    Some(old_row),
                    target.width,
                    source_row,
                    &mut input_reservation,
                )?;
                execute_merge_update(
                    state,
                    target.table_id,
                    current_position,
                    old_row,
                    &input,
                    assignments,
                    params,
                )?
            }
            (BoundMergeAction::Delete, Some(target_position)) => {
                let old_row = target_rows.get(target_position).ok_or_else(|| {
                    internal_error("MERGE target row is outside its statement snapshot")
                })?;
                let current_position =
                    current_merge_target_position(target_position, &deleted_targets)?;
                let deleted =
                    execute_merge_delete(state, target.table_id, current_position, old_row)?;
                if deleted.is_some() {
                    plan_reservation.grow(mem::size_of::<usize>() * 4)?;
                    deleted_targets.insert(target_position);
                }
                deleted
            }
            (
                BoundMergeAction::Insert {
                    column_indexes,
                    values,
                },
                None,
            ) => {
                let input =
                    merge_input_row(None, target.width, source_row, &mut input_reservation)?;
                execute_merge_insert(
                    state,
                    &target_definition,
                    column_indexes,
                    values,
                    &input,
                    params,
                )?
            }
            (BoundMergeAction::DoNothing, _) => None,
            _ => {
                return Err(internal_error(
                    "MERGE action does not match its clause kind",
                ));
            }
        };
        let Some(changed_row) = changed_row else {
            continue;
        };
        if let Some(returning) = &returning {
            let returned = evaluate_returning(returning, &changed_row, params)?;
            returning_reservation.grow(estimated_row_bytes(&returned))?;
            returned_rows.push(returned);
        }
        affected = affected.saturating_add(1);
    }

    for event in &statement_events {
        statement_trigger_fired |= fire_statement_triggers(
            state,
            target.table_id,
            TriggerTiming::AfterStatement,
            *event,
        )?;
    }
    validate_database_rows(state)?;

    Ok((
        dml_command_events(
            returning.as_ref(),
            format!("MERGE {affected}"),
            affected,
            returned_rows,
        ),
        affected != 0 || statement_trigger_fired,
    ))
}

fn first_matching_merge_clause(
    clauses: &[ordadb_sql::BoundMergeClause],
    kind: BoundMergeClauseKind,
    input: &Row,
    params: &[Value],
) -> Result<Option<usize>> {
    for (index, clause) in clauses.iter().enumerate() {
        if clause.kind != kind {
            continue;
        }
        if clause
            .predicate
            .as_ref()
            .map(|predicate| execution_predicate_matches(predicate, input, params))
            .transpose()?
            .unwrap_or(true)
        {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn merge_input_row(
    target: Option<&Row>,
    target_width: usize,
    source: &Row,
    reservation: &mut Reservation,
) -> Result<Row> {
    let target_bytes = target.map_or_else(
        || target_width.saturating_mul(mem::size_of::<Value>()),
        estimated_row_bytes,
    );
    reservation.resize(
        target_bytes
            .saturating_add(estimated_row_bytes(source))
            .saturating_add(mem::size_of::<Row>()),
    )?;
    let mut values = Vec::with_capacity(target_width.saturating_add(source.values.len()));
    match target {
        Some(target) => values.extend(target.values.iter().cloned()),
        None => values.resize(target_width, Value::Null),
    }
    values.extend(source.values.iter().cloned());
    Ok(Row::new(values))
}

fn current_merge_target_position(
    original_position: usize,
    deleted_targets: &BTreeSet<usize>,
) -> Result<usize> {
    if deleted_targets.contains(&original_position) {
        return Err(internal_error("MERGE target row was already deleted"));
    }
    original_position
        .checked_sub(deleted_targets.range(..original_position).count())
        .ok_or_else(|| internal_error("MERGE target position underflowed"))
}

fn execute_merge_update(
    state: &mut DatabaseState,
    table_id: TableId,
    position: usize,
    old_row: &Row,
    input: &Row,
    assignments: &[(usize, BoundExpr)],
    params: &[Value],
) -> Result<Option<Row>> {
    let mut proposed = old_row.clone();
    for (column_index, expression) in assignments {
        proposed.values[*column_index] = evaluate_scalar(expression, &input.values, params)?;
    }
    let replacement = match fire_row_triggers_with_rows(
        state,
        table_id,
        TriggerTiming::Before,
        TriggerEvent::Update,
        Some(old_row),
        Some(&proposed),
    )? {
        RowTriggerOutcome::Proceed(Some(row)) => row,
        RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => return Ok(None),
    };
    let current = state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.get(position))
        .ok_or_else(|| DbError::new("55000", "MERGE target row disappeared before update"))?;
    if current != old_row {
        return Err(DbError::new(
            "55000",
            "BEFORE trigger changed the row targeted by MERGE UPDATE",
        )
        .with_hint("Return a replacement NEW row instead of updating the same row recursively."));
    }
    Arc::make_mut(
        state
            .rows
            .get_mut(&table_id)
            .ok_or_else(|| internal_error("MERGE target rows disappeared"))?,
    )[position] = replacement.clone();
    if replacement != *old_row {
        apply_referential_actions(
            state,
            vec![ReferentialChange::Update {
                table_id,
                old: old_row.clone(),
                new: replacement.clone(),
            }],
        )?;
    }
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    let _ = fire_row_triggers_with_rows(
        state,
        table_id,
        TriggerTiming::After,
        TriggerEvent::Update,
        Some(old_row),
        Some(&replacement),
    )?;
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    Ok(Some(replacement))
}

fn execute_merge_delete(
    state: &mut DatabaseState,
    table_id: TableId,
    position: usize,
    old_row: &Row,
) -> Result<Option<Row>> {
    if matches!(
        fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::Before,
            TriggerEvent::Delete,
            Some(old_row),
            None,
        )?,
        RowTriggerOutcome::Suppress
    ) {
        return Ok(None);
    }
    let current = state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.get(position))
        .ok_or_else(|| DbError::new("55000", "MERGE target row disappeared before delete"))?;
    if current != old_row {
        return Err(DbError::new(
            "55000",
            "BEFORE trigger changed the row targeted by MERGE DELETE",
        )
        .with_hint("Return OLD instead of deleting the same row recursively."));
    }
    Arc::make_mut(
        state
            .rows
            .get_mut(&table_id)
            .ok_or_else(|| internal_error("MERGE target rows disappeared"))?,
    )
    .remove(position);
    apply_referential_actions(
        state,
        vec![ReferentialChange::Delete {
            table_id,
            old: old_row.clone(),
        }],
    )?;
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    let _ = fire_row_triggers_with_rows(
        state,
        table_id,
        TriggerTiming::After,
        TriggerEvent::Delete,
        Some(old_row),
        None,
    )?;
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    Ok(Some(old_row.clone()))
}

fn execute_merge_insert(
    state: &mut DatabaseState,
    table: &TableDefinition,
    column_indexes: &[usize],
    expressions: &[BoundExpr],
    input: &Row,
    params: &[Value],
) -> Result<Option<Row>> {
    let mut values = table
        .columns()
        .iter()
        .map(|column| column_default_value(&state.catalog, column))
        .collect::<Result<Vec<_>>>()?;
    for (expression, column_index) in expressions.iter().zip(column_indexes) {
        values[*column_index] = evaluate_scalar(expression, &input.values, params)?;
    }
    let proposed = Row::new(values);
    let inserted = match fire_row_triggers_with_rows(
        state,
        table.id,
        TriggerTiming::Before,
        TriggerEvent::Insert,
        None,
        Some(&proposed),
    )? {
        RowTriggerOutcome::Proceed(Some(row)) => row,
        RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => return Ok(None),
    };
    validate_rows(&state.catalog, table, std::slice::from_ref(&inserted))?;
    Arc::make_mut(
        state
            .rows
            .entry(table.id)
            .or_insert_with(|| Arc::new(Vec::new())),
    )
    .push(inserted.clone());
    validate_database_rows(state)?;
    rebuild_table_derived(state, table.id)?;
    let _ = fire_row_triggers_with_rows(
        state,
        table.id,
        TriggerTiming::After,
        TriggerEvent::Insert,
        None,
        Some(&inserted),
    )?;
    validate_database_rows(state)?;
    rebuild_table_derived(state, table.id)?;
    Ok(Some(inserted))
}

fn ensure_statement_not_cancelled(state: &DatabaseState) -> Result<()> {
    if state
        .cancellation
        .as_ref()
        .is_some_and(|cancellation| cancellation.load(Ordering::Acquire))
    {
        Err(DbError::new("57014", "query was cancelled"))
    } else {
        Ok(())
    }
}

fn execute_insert(
    state: &mut DatabaseState,
    table_id: TableId,
    column_indexes: Vec<usize>,
    expressions: Vec<Vec<BoundExpr>>,
    on_conflict: Option<BoundOnConflict>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let table = table_definition(state, table_id)?.clone();
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::BeforeStatement,
        TriggerEvent::Insert,
    )?;
    let conflict_update = matches!(
        on_conflict.as_ref().map(|conflict| &conflict.action),
        Some(BoundConflictAction::DoUpdate { .. })
    );
    if conflict_update {
        fire_statement_triggers(
            state,
            table_id,
            TriggerTiming::BeforeStatement,
            TriggerEvent::Update,
        )?;
    }
    let mut affected = 0u64;
    let mut returned_rows = Vec::new();
    let mut command_affected_rows = BTreeSet::new();
    let conflict_memory = MemoryGrant::new(DEFAULT_SOFT_MEMORY_BYTES, DEFAULT_HARD_MEMORY_BYTES)?;
    let mut conflict_reservation = conflict_memory.try_reserve(0)?;
    for expressions in expressions {
        let mut values = table
            .columns()
            .iter()
            .map(|column| column_default_value(&state.catalog, column))
            .collect::<Result<Vec<_>>>()?;
        for (expression, column_index) in expressions.into_iter().zip(&column_indexes) {
            values[*column_index] = evaluate_scalar(&expression, &[], params)?;
        }
        let proposed = Row::new(values);
        let inserted_row = match fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::Before,
            TriggerEvent::Insert,
            None,
            Some(&proposed),
        )? {
            RowTriggerOutcome::Proceed(Some(row)) => row,
            RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
        };
        validate_rows(&state.catalog, &table, std::slice::from_ref(&inserted_row))?;
        if let Some(on_conflict) = &on_conflict
            && let Some(position) = conflicting_row_position(
                state,
                &table,
                &inserted_row,
                on_conflict.target_columns.as_deref(),
            )?
        {
            match &on_conflict.action {
                BoundConflictAction::DoNothing => continue,
                BoundConflictAction::DoUpdate {
                    assignments,
                    filter,
                } => {
                    if command_affected_rows.contains(&position) {
                        return Err(DbError::new(
                            "21000",
                            "ON CONFLICT DO UPDATE command cannot affect row a second time",
                        )
                        .with_hint(
                            "Ensure that no rows proposed for insertion within the same command have duplicate constrained values.",
                        ));
                    }
                    conflict_reservation.grow(std::mem::size_of::<usize>() * 4)?;
                    command_affected_rows.insert(position);
                    let replacement = execute_conflict_update(
                        state,
                        table_id,
                        position,
                        &inserted_row,
                        assignments,
                        filter.as_ref(),
                        params,
                    )?;
                    if let Some(replacement) = replacement {
                        if let Some(returning) = &returning {
                            returned_rows.push(evaluate_returning(
                                returning,
                                &replacement,
                                params,
                            )?);
                        }
                        affected = affected.saturating_add(1);
                    }
                    continue;
                }
            }
        }
        let inserted_position = state.rows.get(&table_id).map_or(0, |rows| rows.len());
        Arc::make_mut(
            state
                .rows
                .entry(table_id)
                .or_insert_with(|| Arc::new(Vec::new())),
        )
        .push(inserted_row.clone());
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        let _ = fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::After,
            TriggerEvent::Insert,
            None,
            Some(&inserted_row),
        )?;
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        if let Some(returning) = &returning {
            returned_rows.push(evaluate_returning(returning, &inserted_row, params)?);
        }
        if conflict_update {
            conflict_reservation.grow(std::mem::size_of::<usize>() * 4)?;
            command_affected_rows.insert(inserted_position);
        }
        affected = affected.saturating_add(1);
    }
    if conflict_update {
        fire_statement_triggers(
            state,
            table_id,
            TriggerTiming::AfterStatement,
            TriggerEvent::Update,
        )?;
    }
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::AfterStatement,
        TriggerEvent::Insert,
    )?;
    validate_database_rows(state)?;
    Ok((
        dml_command_events(
            returning.as_ref(),
            format!("INSERT 0 {affected}"),
            affected,
            returned_rows,
        ),
        true,
    ))
}

fn execute_view_insert(
    state: &mut DatabaseState,
    view_id: ViewId,
    _source: BoundStatement,
    column_indexes: Vec<usize>,
    expressions: Vec<Vec<BoundExpr>>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let view = state
        .catalog
        .view_by_id(view_id)
        .cloned()
        .ok_or_else(|| internal_error("view INSERT target disappeared"))?;
    if view.kind != ViewKind::Regular {
        return Err(DbError::new("42809", "cannot modify a materialized view"));
    }
    let mut affected = 0_u64;
    let mut returned_rows = Vec::new();
    for expressions in expressions {
        ensure_statement_not_cancelled(state)?;
        let mut values = vec![Value::Null; view.output.fields.len()];
        for (expression, column_index) in expressions.into_iter().zip(&column_indexes) {
            let target = values
                .get_mut(*column_index)
                .ok_or_else(|| internal_error("view INSERT column is out of bounds"))?;
            *target = evaluate_scalar(&expression, &[], params)?;
        }
        let proposed = Row::new(values);
        let returned = match fire_view_row_triggers_with_rows(
            state,
            view_id,
            TriggerEvent::Insert,
            None,
            Some(&proposed),
        )? {
            RowTriggerOutcome::Proceed(Some(row)) => row,
            RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
        };
        if let Some(returning) = &returning {
            returned_rows.push(evaluate_returning(returning, &returned, params)?);
        }
        affected = affected.saturating_add(1);
    }
    validate_database_rows(state)?;
    Ok((
        dml_command_events(
            returning.as_ref(),
            format!("INSERT 0 {affected}"),
            affected,
            returned_rows,
        ),
        true,
    ))
}

fn execute_view_source_rows(
    state: &mut DatabaseState,
    source: BoundStatement,
    expected: &Schema,
    params: &[Value],
) -> Result<(Vec<Row>, Vec<DbNotice>)> {
    let (events, dirty) = execute_bound(state, source, params)?;
    if dirty {
        return Err(internal_error(
            "a stored view query attempted to mutate state",
        ));
    }
    let mut rows = Vec::new();
    let mut notices = Vec::new();
    for event in events {
        match event {
            QueryEvent::Schema(schema) if schema != *expected => {
                return Err(DbError::new(
                    "42P16",
                    "stored view query output no longer matches its catalog definition",
                ));
            }
            QueryEvent::Schema(_) | QueryEvent::Progress(_) | QueryEvent::Complete(_) => {}
            QueryEvent::Batch(batch) => rows.extend(batch.rows),
            QueryEvent::Notice(notice) => notices.push(notice),
        }
    }
    Ok((rows, notices))
}

fn conflicting_row_position(
    state: &DatabaseState,
    table: &TableDefinition,
    candidate: &Row,
    target_columns: Option<&[usize]>,
) -> Result<Option<usize>> {
    let target_column_ids = target_columns
        .map(|target| {
            target
                .iter()
                .map(|position| {
                    table
                        .columns()
                        .get(*position)
                        .map(|column| column.id)
                        .ok_or_else(|| internal_error("conflict target column is out of bounds"))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    for definition in table.indexes().filter(|index| {
        index.unique
            && index.method == IndexMethod::BTree
            && target_column_ids.as_deref().is_none_or(|target| {
                target.len() == index.key_columns.len()
                    && target
                        .iter()
                        .all(|column_id| index.key_columns.contains(column_id))
            })
    }) {
        let positions = definition
            .key_columns
            .iter()
            .map(|column_id| {
                table
                    .column_index_by_id(*column_id)
                    .ok_or_else(|| internal_error("conflict index column is absent from its table"))
            })
            .collect::<Result<Vec<_>>>()?;
        let values = positions
            .iter()
            .map(|position| candidate.values[*position].clone())
            .collect::<Vec<_>>();
        if values.iter().any(Value::is_null) {
            continue;
        }
        let key_types = positions
            .iter()
            .map(|position| table.columns()[*position].data_type.clone())
            .collect::<Vec<_>>();
        let key = IndexKey::from_typed_values(&values, &key_types)?;
        let tree = state
            .indexes
            .get(&definition.id)
            .ok_or_else(|| internal_error("conflict arbiter index is absent from live state"))?;
        if let Some(entry) = tree.get_iter(&key).next() {
            let position = usize::try_from(entry.row_id.get())
                .map_err(|_| internal_error("conflict row ID does not fit this platform"))?;
            if state
                .rows
                .get(&table.id)
                .and_then(|rows| rows.get(position))
                .is_none()
            {
                return Err(internal_error("conflict index row ID is outside its table"));
            }
            return Ok(Some(position));
        }
    }
    Ok(None)
}

fn execute_conflict_update(
    state: &mut DatabaseState,
    table_id: TableId,
    conflict_position: usize,
    excluded: &Row,
    assignments: &[(usize, BoundExpr)],
    filter: Option<&BoundExpr>,
    params: &[Value],
) -> Result<Option<Row>> {
    let old_row = state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.get(conflict_position))
        .cloned()
        .ok_or_else(|| internal_error("conflict row disappeared before update"))?;
    let mut conflict_values = old_row.values.clone();
    conflict_values.extend(excluded.values.iter().cloned());
    let conflict_row = Row::new(conflict_values);
    if !filter
        .map(|filter| execution_predicate_matches(filter, &conflict_row, params))
        .transpose()?
        .unwrap_or(true)
    {
        return Ok(None);
    }

    let mut replacements = Vec::with_capacity(assignments.len());
    for (column_index, expression) in assignments {
        replacements.push((
            *column_index,
            evaluate_scalar(expression, &conflict_row.values, params)?,
        ));
    }
    let mut proposed = old_row.clone();
    for (column_index, value) in replacements {
        proposed.values[column_index] = value;
    }
    let replacement = match fire_row_triggers_with_rows(
        state,
        table_id,
        TriggerTiming::Before,
        TriggerEvent::Update,
        Some(&old_row),
        Some(&proposed),
    )? {
        RowTriggerOutcome::Proceed(Some(row)) => row,
        RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => return Ok(None),
    };
    let position = state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.iter().position(|row| row == &old_row))
        .ok_or_else(|| {
            DbError::new(
                "55000",
                "BEFORE trigger changed the row targeted by ON CONFLICT DO UPDATE",
            )
            .with_hint("Return a replacement NEW row instead of updating the same row recursively.")
        })?;
    Arc::make_mut(
        state
            .rows
            .get_mut(&table_id)
            .ok_or_else(|| internal_error("conflict target rows disappeared"))?,
    )[position] = replacement.clone();
    if replacement != old_row {
        apply_referential_actions(
            state,
            vec![ReferentialChange::Update {
                table_id,
                old: old_row.clone(),
                new: replacement.clone(),
            }],
        )?;
    }
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    let _ = fire_row_triggers_with_rows(
        state,
        table_id,
        TriggerTiming::After,
        TriggerEvent::Update,
        Some(&old_row),
        Some(&replacement),
    )?;
    validate_database_rows(state)?;
    rebuild_table_derived(state, table_id)?;
    Ok(Some(replacement))
}

fn execute_select(
    state: &DatabaseState,
    execution: SelectExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let (schema, mut cursor) =
        prepare_select_cursor(state, execution, params, None, &ExecutionOptions::default())?;
    let mut events = vec![QueryEvent::Schema(schema.clone())];
    let mut count = 0_u64;
    let mut emitted_batch = false;
    while let Some(batch) = cursor.next_batch()? {
        count = count.saturating_add(batch.rows.len() as u64);
        emitted_batch = true;
        events.push(QueryEvent::Batch(batch));
    }
    if !emitted_batch {
        events.push(QueryEvent::Batch(Batch {
            schema,
            rows: Vec::new(),
        }));
    }
    events.push(QueryEvent::Progress(QueryProgress {
        rows_processed: count,
    }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: format!("SELECT {count}"),
        rows_affected: count,
    }));
    Ok((events, false))
}

fn execute_with_clause(
    state: &DatabaseState,
    ctes: Vec<BoundCte>,
    body: BoundStatement,
    catalog: Catalog,
    schema: Schema,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let memory = MemoryGrant::new(DEFAULT_SOFT_MEMORY_BYTES, DEFAULT_HARD_MEMORY_BYTES)?;
    let mut reservation = memory.try_reserve(0)?;
    let mut local = state.clone();
    local.catalog = Arc::new(catalog);
    for cte in ctes {
        ensure_statement_not_cancelled(state)?;
        let cte_schema = cte_table_schema(&local, cte.table_id)?;
        let (events, changed) = execute_bound(&mut local, *cte.seed, params)?;
        if changed {
            return Err(internal_error(
                "CTE query unexpectedly changed database state",
            ));
        }
        let mut rows = coerce_set_rows(
            collect_set_operand_rows(events),
            &cte_schema,
            &mut reservation,
        )?;
        ensure_recursive_cte_row_limit(rows.len())?;
        if let Some(recursive) = cte.recursive {
            let mut seen = (!cte.union_all).then(HashSet::new);
            if let Some(seen) = &mut seen {
                for row in &rows {
                    seen.insert(accounted_set_row_key(row, &mut reservation)?);
                }
            }
            let mut working = rows.clone();
            for row in &working {
                reservation.grow(estimated_row_bytes(row))?;
            }
            let mut iteration = 0_usize;
            while !working.is_empty() {
                ensure_statement_not_cancelled(state)?;
                iteration = iteration.saturating_add(1);
                if iteration > MAX_RECURSIVE_CTE_ITERATIONS {
                    return Err(DbError::new(
                        "54001",
                        format!("recursive CTE exceeded {MAX_RECURSIVE_CTE_ITERATIONS} iterations"),
                    )
                    .with_hint("Add a terminating predicate to the recursive term."));
                }
                local.rows.insert(cte.table_id, Arc::new(working));
                let (events, changed) = execute_bound(&mut local, (*recursive).clone(), params)?;
                if changed {
                    return Err(internal_error(
                        "recursive CTE term unexpectedly changed database state",
                    ));
                }
                let candidates = coerce_set_rows(
                    collect_set_operand_rows(events),
                    &cte_schema,
                    &mut reservation,
                )?;
                let mut next = Vec::new();
                for row in candidates {
                    ensure_statement_not_cancelled(state)?;
                    if let Some(seen) = &mut seen {
                        let key = accounted_set_row_key(&row, &mut reservation)?;
                        if !seen.insert(key) {
                            continue;
                        }
                    }
                    ensure_recursive_cte_row_limit(
                        rows.len().saturating_add(next.len()).saturating_add(1),
                    )?;
                    next.push(row);
                }
                for row in &next {
                    reservation.grow(estimated_row_bytes(row))?;
                }
                working = next.clone();
                rows.extend(next);
            }
        }
        local.rows.insert(cte.table_id, Arc::new(rows));
    }
    if bound_statement_schema(&body) != schema {
        return Err(internal_error(
            "WITH body schema changed after binding its CTEs",
        ));
    }
    let (events, changed) = execute_bound(&mut local, body, params)?;
    if changed {
        return Err(internal_error(
            "WITH body unexpectedly changed database state",
        ));
    }
    Ok((events, false))
}

fn ensure_recursive_cte_row_limit(row_count: usize) -> Result<()> {
    if row_count > MAX_RECURSIVE_CTE_ROWS {
        return Err(DbError::new(
            "54000",
            format!("recursive CTE exceeded {MAX_RECURSIVE_CTE_ROWS} rows"),
        ));
    }
    Ok(())
}

fn cte_table_schema(state: &DatabaseState, table_id: TableId) -> Result<Schema> {
    Ok(Schema::new(
        table_definition(state, table_id)?
            .columns()
            .iter()
            .map(|column| {
                Field::new(
                    column.name.as_str(),
                    column.data_type.clone(),
                    column.nullable,
                )
            })
            .collect(),
    ))
}

fn execute_set_operation(
    state: &mut DatabaseState,
    execution: SetExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let SetExecution {
        left,
        operator,
        all,
        right,
        schema,
        order_by,
        offset,
        limit,
    } = execution;
    let memory = MemoryGrant::new(DEFAULT_SOFT_MEMORY_BYTES, DEFAULT_HARD_MEMORY_BYTES)?;
    let mut reservation = memory.try_reserve(0)?;
    let (left_events, left_changed) = execute_bound(state, *left, params)?;
    let (right_events, right_changed) = execute_bound(state, *right, params)?;
    if left_changed || right_changed {
        return Err(internal_error(
            "set-operation operand unexpectedly changed database state",
        ));
    }
    let left = coerce_set_rows(
        collect_set_operand_rows(left_events),
        &schema,
        &mut reservation,
    )?;
    let right = coerce_set_rows(
        collect_set_operand_rows(right_events),
        &schema,
        &mut reservation,
    )?;
    let mut rows = combine_set_rows(state, left, right, operator, all, &memory)?;
    if !order_by.is_empty() {
        sort_set_rows(&mut rows, &order_by)?;
    }
    let offset = evaluate_set_offset(offset.as_ref(), params)?;
    let limit = evaluate_set_limit(limit.as_ref(), params)?;
    let rows = rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    Ok((select_rows_events(schema, rows), false))
}

fn collect_set_operand_rows(events: Vec<QueryEvent>) -> Vec<Row> {
    let mut rows = Vec::new();
    for event in events {
        if let QueryEvent::Batch(mut batch) = event {
            rows.append(&mut batch.rows);
        }
    }
    rows
}

fn coerce_set_rows(
    rows: Vec<Row>,
    schema: &Schema,
    reservation: &mut Reservation,
) -> Result<Vec<Row>> {
    rows.into_iter()
        .map(|row| {
            if row.values.len() != schema.fields.len() {
                return Err(internal_error(
                    "set-operation row width does not match its bound schema",
                ));
            }
            let row = Row::new(
                row.values
                    .into_iter()
                    .zip(&schema.fields)
                    .map(|(value, field)| coerce_execution_value(value, &field.data_type))
                    .collect::<Result<Vec<_>>>()?,
            );
            reservation.grow(estimated_row_bytes(&row))?;
            Ok(row)
        })
        .collect()
}

fn combine_set_rows(
    state: &DatabaseState,
    left: Vec<Row>,
    right: Vec<Row>,
    operator: QuerySetOperator,
    all: bool,
    memory: &MemoryGrant,
) -> Result<Vec<Row>> {
    if operator == QuerySetOperator::Union && all {
        return Ok(left.into_iter().chain(right).collect());
    }
    let mut key_reservation = memory.try_reserve(0)?;
    let mut output = Vec::new();
    match operator {
        QuerySetOperator::Union => {
            let mut seen = HashSet::new();
            for row in left.into_iter().chain(right) {
                ensure_statement_not_cancelled(state)?;
                let key = accounted_set_row_key(&row, &mut key_reservation)?;
                if seen.insert(key) {
                    output.push(row);
                }
            }
        }
        QuerySetOperator::Intersect => {
            let mut right_counts = set_row_counts(state, right, &mut key_reservation)?;
            let mut emitted = (!all).then(HashSet::new);
            for row in left {
                ensure_statement_not_cancelled(state)?;
                let key = accounted_set_row_key(&row, &mut key_reservation)?;
                if emitted
                    .as_ref()
                    .is_some_and(|emitted| emitted.contains(&key))
                {
                    continue;
                }
                let Some(count) = right_counts.get_mut(&key) else {
                    continue;
                };
                if *count == 0 {
                    continue;
                }
                if all {
                    *count -= 1;
                } else if let Some(emitted) = &mut emitted {
                    emitted.insert(key);
                }
                output.push(row);
            }
        }
        QuerySetOperator::Except => {
            let mut right_counts = set_row_counts(state, right, &mut key_reservation)?;
            let mut emitted = (!all).then(HashSet::new);
            for row in left {
                ensure_statement_not_cancelled(state)?;
                let key = accounted_set_row_key(&row, &mut key_reservation)?;
                if emitted
                    .as_ref()
                    .is_some_and(|emitted| emitted.contains(&key))
                {
                    continue;
                }
                if let Some(count) = right_counts.get_mut(&key)
                    && *count > 0
                {
                    if all {
                        *count -= 1;
                    }
                    continue;
                }
                if let Some(emitted) = &mut emitted {
                    emitted.insert(key);
                }
                output.push(row);
            }
        }
    }
    Ok(output)
}

fn set_row_counts(
    state: &DatabaseState,
    rows: Vec<Row>,
    reservation: &mut Reservation,
) -> Result<HashMap<SetRowKey, usize>> {
    let mut counts = HashMap::<SetRowKey, usize>::new();
    for row in rows {
        ensure_statement_not_cancelled(state)?;
        let key = accounted_set_row_key(&row, reservation)?;
        let count = counts.entry(key).or_default();
        *count = count
            .checked_add(1)
            .ok_or_else(|| DbError::new("22003", "set-operation duplicate count overflowed"))?;
    }
    Ok(counts)
}

fn accounted_set_row_key(row: &Row, reservation: &mut Reservation) -> Result<SetRowKey> {
    let key = set_row_key(row)?;
    reservation.grow(estimated_set_key_bytes(&key).saturating_add(64))?;
    Ok(key)
}

fn set_row_key(row: &Row) -> Result<SetRowKey> {
    row.values
        .iter()
        .map(|value| match value {
            Value::Null => Ok(SetValueKey::Null),
            Value::Boolean(value) => Ok(SetValueKey::Boolean(*value)),
            Value::Int16(value) => Ok(SetValueKey::Int16(*value)),
            Value::Int32(value) => Ok(SetValueKey::Int32(*value)),
            Value::Int64(value) => Ok(SetValueKey::Int64(*value)),
            Value::Float32(value) => Ok(SetValueKey::Float32(canonical_float32(*value))),
            Value::Float64(value) => Ok(SetValueKey::Float64(canonical_float64(*value))),
            Value::Decimal(value) => Ok(SetValueKey::Decimal(value.normalize().to_string())),
            Value::Text(value) => Ok(SetValueKey::Text(value.clone())),
            Value::Binary(value) => Ok(SetValueKey::Binary(value.clone())),
            Value::Date(value) => Ok(SetValueKey::Date(value.to_string())),
            Value::Time(value) => Ok(SetValueKey::Time(value.to_string())),
            Value::Timestamp(value) => Ok(SetValueKey::Timestamp(value.to_string())),
            Value::Interval(value) => Ok(SetValueKey::Interval(
                value.months,
                value.days,
                value.microseconds,
            )),
            Value::Array(value) => serde_json::to_string(value)
                .map(SetValueKey::Array)
                .map_err(|error| internal_error(format!("failed to canonicalize array: {error}"))),
            Value::Json(_) => Err(DbError::new(
                "42883",
                "could not identify an equality operator for type json",
            )),
            Value::Jsonb(value) => serde_json::to_string(value)
                .map(SetValueKey::Jsonb)
                .map_err(|error| internal_error(format!("failed to canonicalize jsonb: {error}"))),
            Value::Uuid(value) => Ok(SetValueKey::Uuid(*value.as_bytes())),
            Value::Vector(values) => Ok(SetValueKey::Vector(
                values
                    .iter()
                    .map(|value| canonical_float32(*value))
                    .collect(),
            )),
        })
        .collect::<Result<Vec<_>>>()
        .map(SetRowKey)
}

fn canonical_float32(value: f32) -> u32 {
    if value == 0.0 {
        0.0_f32.to_bits()
    } else if value.is_nan() {
        f32::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

fn canonical_float64(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

fn estimated_set_key_bytes(key: &SetRowKey) -> usize {
    mem::size_of::<SetRowKey>()
        .saturating_add(key.0.iter().map(estimated_set_value_key_bytes).sum())
}

fn estimated_set_value_key_bytes(key: &SetValueKey) -> usize {
    mem::size_of::<SetValueKey>()
        + match key {
            SetValueKey::Decimal(value)
            | SetValueKey::Text(value)
            | SetValueKey::Date(value)
            | SetValueKey::Time(value)
            | SetValueKey::Timestamp(value)
            | SetValueKey::Array(value)
            | SetValueKey::Jsonb(value) => value.len(),
            SetValueKey::Binary(value) => value.len(),
            SetValueKey::Vector(values) => values.len().saturating_mul(mem::size_of::<u32>()),
            SetValueKey::Null
            | SetValueKey::Boolean(_)
            | SetValueKey::Int16(_)
            | SetValueKey::Int32(_)
            | SetValueKey::Int64(_)
            | SetValueKey::Float32(_)
            | SetValueKey::Float64(_)
            | SetValueKey::Interval(_, _, _)
            | SetValueKey::Uuid(_) => 0,
        }
}

fn sort_set_rows(rows: &mut [Row], order_by: &[BoundOrder]) -> Result<()> {
    let mut error = None;
    rows.sort_by(|left, right| {
        compare_set_rows(left, right, order_by).unwrap_or_else(|sort_error| {
            error = Some(sort_error);
            std::cmp::Ordering::Equal
        })
    });
    error.map_or(Ok(()), Err)
}

fn compare_set_rows(
    left: &Row,
    right: &Row,
    order_by: &[BoundOrder],
) -> Result<std::cmp::Ordering> {
    for order in order_by {
        let left = left
            .values
            .get(order.column_index)
            .ok_or_else(|| internal_error("set-operation sort column is out of bounds"))?;
        let right = right
            .values
            .get(order.column_index)
            .ok_or_else(|| internal_error("set-operation sort column is out of bounds"))?;
        let ordering = match (left.is_null(), right.is_null()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => {
                if order.nulls_first.unwrap_or(!order.ascending) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (false, true) => {
                if order.nulls_first.unwrap_or(!order.ascending) {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (false, false) => {
                let ordering = compare_execution_values(left, right)?;
                if order.ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            }
        };
        if ordering != std::cmp::Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(std::cmp::Ordering::Equal)
}

fn evaluate_set_offset(offset: Option<&BoundExpr>, params: &[Value]) -> Result<usize> {
    let Some(offset) = offset else {
        return Ok(0);
    };
    match evaluate_scalar(offset, &[], params)? {
        Value::Int64(value) if value >= 0 => usize::try_from(value)
            .map_err(|_| DbError::new("22003", "OFFSET value is out of range")),
        Value::Null => Ok(0),
        _ => Err(DbError::new(
            "2201X",
            "OFFSET must be a non-negative integer",
        )),
    }
}

fn evaluate_set_limit(limit: Option<&BoundExpr>, params: &[Value]) -> Result<usize> {
    let Some(limit) = limit else {
        return Ok(usize::MAX);
    };
    match evaluate_scalar(limit, &[], params)? {
        Value::Int64(value) if value >= 0 => {
            usize::try_from(value).map_err(|_| DbError::new("22003", "LIMIT value is out of range"))
        }
        Value::Null => Ok(usize::MAX),
        _ => Err(DbError::new(
            "2201W",
            "LIMIT must be a non-negative integer",
        )),
    }
}

fn select_rows_events(schema: Schema, rows: Vec<Row>) -> Vec<QueryEvent> {
    let count = rows.len() as u64;
    let mut events = vec![QueryEvent::Schema(schema.clone())];
    let mut batch_rows = Vec::with_capacity(DEFAULT_BATCH_ROWS.min(rows.len()));
    for row in rows {
        batch_rows.push(row);
        if batch_rows.len() == DEFAULT_BATCH_ROWS {
            events.push(QueryEvent::Batch(Batch {
                schema: schema.clone(),
                rows: mem::take(&mut batch_rows),
            }));
        }
    }
    if !batch_rows.is_empty() {
        events.push(QueryEvent::Batch(Batch {
            schema: schema.clone(),
            rows: batch_rows,
        }));
    } else if count == 0 {
        events.push(QueryEvent::Batch(Batch {
            schema,
            rows: Vec::new(),
        }));
    }
    events.push(QueryEvent::Progress(QueryProgress {
        rows_processed: count,
    }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: format!("SELECT {count}"),
        rows_affected: count,
    }));
    events
}

fn prepare_select_cursor(
    state: &DatabaseState,
    execution: SelectExecution,
    params: &[Value],
    table_provider: Option<&dyn TableProvider>,
    options: &ExecutionOptions,
) -> Result<(Schema, ExecutionCursor)> {
    let SelectExecution {
        table_id,
        schema,
        projection,
        filter,
        order_by,
        offset,
        limit,
    } = execution;
    let plan = optimize_select(
        table_definition(state, table_id)?,
        projection,
        filter,
        order_by,
        offset,
        limit,
    );
    let context = ExecutionContext {
        tables: &state.rows,
        indexes: &state.indexes,
        params,
    };
    let cursor = match table_provider {
        Some(table_provider) => ExecutionCursor::with_options_and_table_provider(
            &plan,
            &context,
            schema.clone(),
            options.clone(),
            Some(table_provider),
        )?,
        None => ExecutionCursor::with_options(&plan, &context, schema.clone(), options.clone())?,
    };
    Ok((schema, cursor))
}

fn execute_advanced_select(
    state: &DatabaseState,
    execution: AdvancedExecution,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let (schema, mut cursor) =
        prepare_advanced_cursor(state, execution, params, &ExecutionOptions::default())?;
    let mut events = vec![QueryEvent::Schema(schema.clone())];
    let mut count = 0_u64;
    let mut emitted_batch = false;
    while let Some(batch) = cursor.next_batch()? {
        count = count.saturating_add(batch.rows.len() as u64);
        emitted_batch = true;
        events.push(QueryEvent::Batch(batch));
    }
    if !emitted_batch {
        events.push(QueryEvent::Batch(Batch {
            schema,
            rows: Vec::new(),
        }));
    }
    events.push(QueryEvent::Progress(QueryProgress {
        rows_processed: count,
    }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: format!("SELECT {count}"),
        rows_affected: count,
    }));
    Ok((events, false))
}

fn prepare_advanced_cursor(
    state: &DatabaseState,
    execution: AdvancedExecution,
    params: &[Value],
    options: &ExecutionOptions,
) -> Result<(Schema, AdvancedExecutionCursor)> {
    let AdvancedExecution {
        table,
        joins,
        applies,
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
    } = execution;
    let context = ExecutionContext {
        tables: &state.rows,
        indexes: &state.indexes,
        params,
    };
    let applies = applies
        .into_iter()
        .map(|apply| build_apply_execution_plan(state, apply, params.len(), &[]))
        .collect::<Result<Vec<_>>>()?;
    let joins = joins
        .into_iter()
        .map(|join| build_join_execution_plan(state, join, params.len(), &[]))
        .collect::<Result<Vec<_>>>()?;
    let cursor = AdvancedExecutionCursor::with_options_and_cancellation(
        AdvancedExecutionPlan {
            table,
            joins,
            applies,
            windows,
            schema: schema.clone(),
            projection,
            distinct,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
            aggregate,
        },
        &context,
        options.clone(),
        state.cancellation.clone(),
    )?;
    Ok((schema, cursor))
}

fn build_join_execution_plan(
    state: &DatabaseState,
    join: BoundJoin,
    parameter_base: usize,
    ancestor_slots: &[BTreeMap<usize, usize>],
) -> Result<JoinExecutionPlan> {
    let BoundJoin { source, kind, on } = join;
    let source = match source {
        BoundJoinSource::Table(table) => JoinExecutionSource::Table(table),
        BoundJoinSource::Derived {
            mut query,
            offset,
            width,
            ..
        } => {
            let correlation =
                rewrite_statement_correlations(&mut query, parameter_base, ancestor_slots)?;
            let nested_parameter_base = parameter_base
                .checked_add(correlation.indexes.len())
                .ok_or_else(|| DbError::new("54001", "LATERAL parameter depth overflowed"))?;
            let mut nested_ancestors = Vec::with_capacity(ancestor_slots.len().saturating_add(1));
            nested_ancestors.push(correlation.parameter_slots);
            nested_ancestors.extend_from_slice(ancestor_slots);
            JoinExecutionSource::Derived {
                query: Box::new(build_query_execution_plan(
                    state,
                    *query,
                    nested_parameter_base,
                    &nested_ancestors,
                )?),
                correlation_indexes: correlation.indexes,
                offset,
                width,
            }
        }
    };
    Ok(JoinExecutionPlan { source, kind, on })
}

fn build_apply_execution_plan(
    state: &DatabaseState,
    apply: BoundApply,
    parameter_base: usize,
    ancestor_slots: &[BTreeMap<usize, usize>],
) -> Result<ApplyExecutionPlan> {
    let BoundApply { kind, mut query } = apply;
    let correlation = rewrite_statement_correlations(&mut query, parameter_base, ancestor_slots)?;
    let nested_parameter_base = parameter_base
        .checked_add(correlation.indexes.len())
        .ok_or_else(|| DbError::new("54001", "correlation parameter depth overflowed"))?;
    let mut nested_ancestors = Vec::with_capacity(ancestor_slots.len().saturating_add(1));
    nested_ancestors.push(correlation.parameter_slots);
    nested_ancestors.extend_from_slice(ancestor_slots);
    let kind = match kind {
        BoundApplyKind::Scalar => ApplyExecutionKind::Scalar,
        BoundApplyKind::Exists { negated } => ApplyExecutionKind::Exists { negated },
        BoundApplyKind::In { left, negated } => ApplyExecutionKind::In { left, negated },
        BoundApplyKind::Quantified {
            left,
            op,
            quantifier,
        } => ApplyExecutionKind::Quantified {
            left,
            op,
            quantifier,
        },
        BoundApplyKind::RowScalar {
            left,
            op,
            operand_types,
        } => ApplyExecutionKind::RowScalar {
            left,
            op,
            operand_types,
        },
        BoundApplyKind::RowQuantified {
            left,
            op,
            quantifier,
            negated,
            operand_types,
        } => ApplyExecutionKind::RowQuantified {
            left,
            op,
            quantifier,
            negated,
            operand_types,
        },
    };
    Ok(ApplyExecutionPlan {
        kind,
        query: Box::new(build_query_execution_plan(
            state,
            *query,
            nested_parameter_base,
            &nested_ancestors,
        )?),
        correlation_indexes: correlation.indexes,
    })
}

fn build_query_execution_plan(
    state: &DatabaseState,
    statement: BoundStatement,
    parameter_base: usize,
    ancestor_slots: &[BTreeMap<usize, usize>],
) -> Result<QueryExecutionPlan> {
    match statement {
        BoundStatement::Select {
            table_id,
            schema,
            projection,
            filter,
            order_by,
            offset,
            limit,
        } => Ok(QueryExecutionPlan::Simple {
            plan: Box::new(optimize_select(
                table_definition(state, table_id)?,
                projection,
                filter,
                order_by,
                offset,
                limit,
            )),
            schema,
        }),
        BoundStatement::AdvancedSelect {
            table,
            joins,
            applies,
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
        } => Ok(QueryExecutionPlan::Advanced(Box::new(
            AdvancedExecutionPlan {
                table,
                joins: joins
                    .into_iter()
                    .map(|join| {
                        build_join_execution_plan(state, join, parameter_base, ancestor_slots)
                    })
                    .collect::<Result<Vec<_>>>()?,
                applies: applies
                    .into_iter()
                    .map(|apply| {
                        build_apply_execution_plan(state, apply, parameter_base, ancestor_slots)
                    })
                    .collect::<Result<Vec<_>>>()?,
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
            },
        ))),
        _ => Err(DbError::new(
            "0A000",
            "Apply subqueries currently support SELECT query bodies only",
        )),
    }
}

struct CorrelationRewrite {
    indexes: Vec<usize>,
    parameter_slots: BTreeMap<usize, usize>,
}

fn collect_forwarded_correlation_indexes(statement: &BoundStatement) -> Result<BTreeSet<usize>> {
    let mut indexes = BTreeSet::new();
    let mut statements = vec![(statement, 0_usize)];
    while let Some((statement, query_depth)) = statements.pop() {
        let target_depth = query_depth.checked_add(1).ok_or_else(|| {
            DbError::new(
                "54001",
                "correlation scope depth exceeds the implementation limit",
            )
        })?;
        match statement {
            BoundStatement::Select {
                projection,
                filter,
                order_by,
                offset,
                limit,
                ..
            } => {
                for projection in projection {
                    collect_expr_correlations(&projection.expr, target_depth, &mut indexes);
                }
                if let Some(filter) = filter {
                    collect_expr_correlations(filter, target_depth, &mut indexes);
                }
                for order in order_by {
                    if let Some(expression) = &order.expression {
                        collect_expr_correlations(expression, target_depth, &mut indexes);
                    }
                }
                if let Some(offset) = offset {
                    collect_expr_correlations(offset, target_depth, &mut indexes);
                }
                if let Some(limit) = limit {
                    collect_expr_correlations(limit, target_depth, &mut indexes);
                }
            }
            BoundStatement::AdvancedSelect {
                joins,
                applies,
                windows,
                projection,
                filter,
                group_by,
                having,
                order_by,
                offset,
                limit,
                ..
            } => {
                for join in joins {
                    collect_expr_correlations(&join.on, target_depth, &mut indexes);
                    if let BoundJoinSource::Derived { query, .. } = &join.source {
                        statements.push((query, target_depth));
                    }
                }
                for apply in applies {
                    match &apply.kind {
                        BoundApplyKind::In { left, .. }
                        | BoundApplyKind::Quantified { left, .. } => {
                            collect_expr_correlations(left, target_depth, &mut indexes);
                        }
                        BoundApplyKind::RowScalar { left, .. }
                        | BoundApplyKind::RowQuantified { left, .. } => {
                            for expression in left {
                                collect_expr_correlations(expression, target_depth, &mut indexes);
                            }
                        }
                        BoundApplyKind::Scalar | BoundApplyKind::Exists { .. } => {}
                    }
                    statements.push((&apply.query, target_depth));
                }
                for window in windows {
                    for argument in &window.arguments {
                        collect_expr_correlations(argument, target_depth, &mut indexes);
                    }
                    if let Some(filter) = &window.filter {
                        collect_expr_correlations(filter, target_depth, &mut indexes);
                    }
                    for expression in &window.partition_by {
                        collect_expr_correlations(expression, target_depth, &mut indexes);
                    }
                    for order in &window.order_by {
                        if let Some(expression) = &order.expression {
                            collect_expr_correlations(expression, target_depth, &mut indexes);
                        }
                    }
                    if let Some(frame) = &window.frame {
                        for bound in [&frame.start_bound, &frame.end_bound] {
                            if let BoundWindowFrameBound::Preceding(expression)
                            | BoundWindowFrameBound::Following(expression) = bound
                            {
                                collect_expr_correlations(expression, target_depth, &mut indexes);
                            }
                        }
                    }
                }
                for projection in projection {
                    collect_expr_correlations(&projection.expr, target_depth, &mut indexes);
                }
                if let Some(filter) = filter {
                    collect_expr_correlations(filter, target_depth, &mut indexes);
                }
                for expression in group_by {
                    collect_expr_correlations(expression, target_depth, &mut indexes);
                }
                if let Some(having) = having {
                    collect_expr_correlations(having, target_depth, &mut indexes);
                }
                for order in order_by {
                    if let Some(expression) = &order.expression {
                        collect_expr_correlations(expression, target_depth, &mut indexes);
                    }
                }
                if let Some(offset) = offset {
                    collect_expr_correlations(offset, target_depth, &mut indexes);
                }
                if let Some(limit) = limit {
                    collect_expr_correlations(limit, target_depth, &mut indexes);
                }
            }
            _ => {}
        }
    }
    Ok(indexes)
}

fn collect_expr_correlations(
    expression: &BoundExpr,
    target_depth: usize,
    indexes: &mut BTreeSet<usize>,
) {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        match &expression.kind {
            BoundExprKind::Correlation { depth, index } => {
                if *depth == target_depth {
                    indexes.insert(*index);
                }
            }
            BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => pending.push(expr),
            BoundExprKind::Array { elements, .. } => pending.extend(elements.iter().rev()),
            BoundExprKind::Function { arguments, .. } => pending.extend(arguments.iter().rev()),
            BoundExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundExprKind::InList { expr, list, .. } => {
                pending.extend(list.iter().rev());
                pending.push(expr);
            }
            BoundExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            BoundExprKind::Column { .. }
            | BoundExprKind::Literal(_)
            | BoundExprKind::Parameter { .. }
            | BoundExprKind::ApplyValue { .. } => {}
        }
    }
}

fn rewrite_statement_correlations(
    statement: &mut BoundStatement,
    parameter_base: usize,
    ancestor_slots: &[BTreeMap<usize, usize>],
) -> Result<CorrelationRewrite> {
    let mut correlation_indexes = Vec::new();
    let mut slots = BTreeMap::new();
    for index in collect_forwarded_correlation_indexes(statement)? {
        correlation_parameter_slot(index, parameter_base, &mut slots, &mut correlation_indexes)?;
    }
    match statement {
        BoundStatement::Select {
            projection,
            filter,
            order_by,
            offset,
            limit,
            ..
        } => {
            for projection in projection {
                rewrite_expr_correlations(
                    &mut projection.expr,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            if let Some(filter) = filter {
                rewrite_expr_correlations(
                    filter,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            for order in order_by {
                if let Some(expression) = &mut order.expression {
                    rewrite_expr_correlations(
                        expression,
                        parameter_base,
                        &mut slots,
                        &mut correlation_indexes,
                        ancestor_slots,
                    )?;
                }
            }
            if let Some(offset) = offset {
                rewrite_expr_correlations(
                    offset,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            if let Some(limit) = limit {
                rewrite_expr_correlations(
                    limit,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
        }
        BoundStatement::AdvancedSelect {
            joins,
            applies,
            windows,
            projection,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
            ..
        } => {
            for join in joins {
                rewrite_expr_correlations(
                    &mut join.on,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            for apply in applies {
                match &mut apply.kind {
                    BoundApplyKind::In { left, .. } | BoundApplyKind::Quantified { left, .. } => {
                        rewrite_expr_correlations(
                            left,
                            parameter_base,
                            &mut slots,
                            &mut correlation_indexes,
                            ancestor_slots,
                        )?;
                    }
                    BoundApplyKind::RowScalar { left, .. }
                    | BoundApplyKind::RowQuantified { left, .. } => {
                        for expression in left {
                            rewrite_expr_correlations(
                                expression,
                                parameter_base,
                                &mut slots,
                                &mut correlation_indexes,
                                ancestor_slots,
                            )?;
                        }
                    }
                    BoundApplyKind::Scalar | BoundApplyKind::Exists { .. } => {}
                }
            }
            for window in windows {
                for argument in &mut window.arguments {
                    rewrite_expr_correlations(
                        argument,
                        parameter_base,
                        &mut slots,
                        &mut correlation_indexes,
                        ancestor_slots,
                    )?;
                }
                if let Some(filter) = &mut window.filter {
                    rewrite_expr_correlations(
                        filter,
                        parameter_base,
                        &mut slots,
                        &mut correlation_indexes,
                        ancestor_slots,
                    )?;
                }
                for expression in &mut window.partition_by {
                    rewrite_expr_correlations(
                        expression,
                        parameter_base,
                        &mut slots,
                        &mut correlation_indexes,
                        ancestor_slots,
                    )?;
                }
                for order in &mut window.order_by {
                    if let Some(expression) = &mut order.expression {
                        rewrite_expr_correlations(
                            expression,
                            parameter_base,
                            &mut slots,
                            &mut correlation_indexes,
                            ancestor_slots,
                        )?;
                    }
                }
                if let Some(frame) = &mut window.frame {
                    for bound in [&mut frame.start_bound, &mut frame.end_bound] {
                        if let BoundWindowFrameBound::Preceding(expression)
                        | BoundWindowFrameBound::Following(expression) = bound
                        {
                            rewrite_expr_correlations(
                                expression,
                                parameter_base,
                                &mut slots,
                                &mut correlation_indexes,
                                ancestor_slots,
                            )?;
                        }
                    }
                }
            }
            for projection in projection {
                rewrite_expr_correlations(
                    &mut projection.expr,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            if let Some(filter) = filter {
                rewrite_expr_correlations(
                    filter,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            for expression in group_by {
                rewrite_expr_correlations(
                    expression,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            if let Some(having) = having {
                rewrite_expr_correlations(
                    having,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            for order in order_by {
                if let Some(expression) = &mut order.expression {
                    rewrite_expr_correlations(
                        expression,
                        parameter_base,
                        &mut slots,
                        &mut correlation_indexes,
                        ancestor_slots,
                    )?;
                }
            }
            if let Some(offset) = offset {
                rewrite_expr_correlations(
                    offset,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
            if let Some(limit) = limit {
                rewrite_expr_correlations(
                    limit,
                    parameter_base,
                    &mut slots,
                    &mut correlation_indexes,
                    ancestor_slots,
                )?;
            }
        }
        _ => {}
    }
    Ok(CorrelationRewrite {
        indexes: correlation_indexes,
        parameter_slots: slots,
    })
}

fn rewrite_expr_correlations(
    expression: &mut BoundExpr,
    parameter_base: usize,
    slots: &mut BTreeMap<usize, usize>,
    correlation_indexes: &mut Vec<usize>,
    ancestor_slots: &[BTreeMap<usize, usize>],
) -> Result<()> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        if let BoundExprKind::Correlation { depth, index } = &expression.kind {
            let depth = *depth;
            let outer_index = *index;
            let parameter_index = if depth == 1 {
                correlation_parameter_slot(outer_index, parameter_base, slots, correlation_indexes)?
            } else if depth > 1 {
                ancestor_slots
                    .get(depth - 2)
                    .and_then(|slots| slots.get(&outer_index))
                    .copied()
                    .ok_or_else(|| {
                        DbError::internal(
                            "nested correlation parameter was not forwarded by its parent Apply",
                        )
                    })?
            } else {
                return Err(DbError::internal("correlation depth must be positive"));
            };
            expression.kind = BoundExprKind::Parameter {
                index: parameter_index,
            };
            continue;
        }
        match &mut expression.kind {
            BoundExprKind::Unary { expr, .. } | BoundExprKind::Cast { expr } => pending.push(expr),
            BoundExprKind::Array { elements, .. } => pending.extend(elements.iter_mut().rev()),
            BoundExprKind::Function { arguments, .. } => {
                pending.extend(arguments.iter_mut().rev());
            }
            BoundExprKind::Binary { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            BoundExprKind::InList { expr, list, .. } => {
                for candidate in list.iter_mut().rev() {
                    pending.push(candidate);
                }
                pending.push(expr);
            }
            BoundExprKind::Aggregate {
                argument, filter, ..
            } => {
                if let Some(filter) = filter {
                    pending.push(filter);
                }
                if let Some(argument) = argument {
                    pending.push(argument);
                }
            }
            BoundExprKind::Column { .. }
            | BoundExprKind::Literal(_)
            | BoundExprKind::Parameter { .. }
            | BoundExprKind::Correlation { .. }
            | BoundExprKind::ApplyValue { .. } => {}
        }
    }
    Ok(())
}

fn correlation_parameter_slot(
    outer_index: usize,
    parameter_base: usize,
    slots: &mut BTreeMap<usize, usize>,
    correlation_indexes: &mut Vec<usize>,
) -> Result<usize> {
    if let Some(parameter_index) = slots.get(&outer_index) {
        return Ok(*parameter_index);
    }
    let parameter_index = parameter_base
        .checked_add(correlation_indexes.len())
        .and_then(|index| index.checked_add(1))
        .ok_or_else(|| DbError::new("54001", "correlation parameter depth overflowed"))?;
    slots.insert(outer_index, parameter_index);
    correlation_indexes.push(outer_index);
    Ok(parameter_index)
}

fn equi_join_columns(expr: &BoundExpr, right_offset: usize) -> Option<(usize, usize)> {
    let BoundExprKind::Binary {
        left,
        op: BinaryOperator::Eq,
        right,
    } = &expr.kind
    else {
        return None;
    };
    let (BoundExprKind::Column { index: left_index }, BoundExprKind::Column { index: right_index }) =
        (&left.kind, &right.kind)
    else {
        return None;
    };
    if *left_index < right_offset && *right_index >= right_offset {
        Some((*left_index, *right_index))
    } else if *right_index < right_offset && *left_index >= right_offset {
        Some((*right_index, *left_index))
    } else {
        None
    }
}

fn execute_explain(
    state: &DatabaseState,
    statement: BoundStatement,
) -> Result<(Vec<QueryEvent>, bool)> {
    let lines = match statement {
        BoundStatement::Select {
            table_id,
            projection,
            filter,
            order_by,
            offset,
            limit,
            ..
        } => explain_plan(&optimize_select(
            table_definition(state, table_id)?,
            projection,
            filter,
            order_by,
            offset,
            limit,
        )),
        BoundStatement::AdvancedSelect {
            table,
            joins,
            applies,
            windows,
            filter,
            distinct,
            aggregate,
            ..
        } => explain_advanced(
            state,
            &table,
            &joins,
            &applies,
            AdvancedExplainFeatures {
                window_count: windows.len(),
                filtered: filter.is_some(),
                distinct,
                aggregate,
            },
        )?,
        _ => {
            return Err(DbError::new(
                "0A000",
                "EXPLAIN supports SELECT statements only",
            ));
        }
    };
    let schema = Schema::new(vec![Field::new("QUERY PLAN", ScalarType::Text, false)]);
    let count = lines.len() as u64;
    let batch = Batch {
        schema: schema.clone(),
        rows: lines
            .into_iter()
            .map(|line| Row::new(vec![Value::Text(line)]))
            .collect(),
    };
    Ok((
        command_events(schema, format!("EXPLAIN {count}"), count, Some(batch)),
        false,
    ))
}

struct AdvancedExplainFeatures {
    window_count: usize,
    filtered: bool,
    distinct: bool,
    aggregate: bool,
}

fn explain_advanced(
    state: &DatabaseState,
    table: &BoundTable,
    joins: &[BoundJoin],
    applies: &[BoundApply],
    features: AdvancedExplainFeatures,
) -> Result<Vec<String>> {
    let base = table_definition(state, table.table_id)?;
    let mut estimated_rows = base.statistics().row_count;
    let mut lines = vec!["Projection  (cost=0.00 rows=1)".to_owned()];
    if features.distinct {
        lines.push("  Unique  (cost=0.00 rows=1)".to_owned());
    }
    if features.aggregate {
        lines.push("  Aggregate  (cost=0.00 rows=1)".to_owned());
    }
    if features.window_count > 0 {
        lines.push(format!(
            "  WindowAgg  (cost=0.00 rows=1 windows={})",
            features.window_count
        ));
    }
    if features.filtered {
        lines.push(format!(
            "  Filter  (cost={:.2} rows={})",
            estimated_rows as f64 * 0.01,
            estimated_rows
        ));
    }
    for apply in applies {
        let kind = match &apply.kind {
            BoundApplyKind::Scalar => "Scalar Apply",
            BoundApplyKind::Exists { negated: false } => "Exists Apply",
            BoundApplyKind::Exists { negated: true } => "Not Exists Apply",
            BoundApplyKind::In { negated: false, .. } => "In Apply",
            BoundApplyKind::In { negated: true, .. } => "Not In Apply",
            BoundApplyKind::Quantified {
                quantifier: SubqueryQuantifier::Any,
                ..
            } => "Any Apply",
            BoundApplyKind::Quantified {
                quantifier: SubqueryQuantifier::All,
                ..
            } => "All Apply",
            BoundApplyKind::RowScalar { .. } => "Row Scalar Apply",
            BoundApplyKind::RowQuantified {
                quantifier: SubqueryQuantifier::Any,
                ..
            } => "Row Any Apply",
            BoundApplyKind::RowQuantified {
                quantifier: SubqueryQuantifier::All,
                ..
            } => "Row All Apply",
        };
        lines.push(format!("  {kind}  (cost=0.00 rows=1)"));
    }
    for join in joins {
        let (right_rows, equi) = match &join.source {
            BoundJoinSource::Table(table) => {
                let right = table_definition(state, table.table_id)?;
                (
                    right.statistics().row_count,
                    equi_join_columns(&join.on, table.offset).is_some(),
                )
            }
            BoundJoinSource::Derived { .. } => (1, false),
        };
        let choice = choose_join_strategy(estimated_rows, right_rows, equi);
        let name = match choice.strategy {
            JoinStrategy::NestedLoop => "Nested Loop",
            JoinStrategy::Hash => "Hash Join",
        };
        let kind = if join.kind == JoinKind::Left {
            "Left"
        } else {
            "Inner"
        };
        lines.push(format!(
            "  {name} {kind}  (cost={:.2} rows={:.0})",
            choice.estimated_cost, choice.estimated_rows
        ));
        estimated_rows = choice.estimated_rows as u64;
    }
    lines.push(format!(
        "    Seq Scan on {}  (cost={:.2} rows={})",
        table.binding,
        estimated_rows as f64 * 0.01,
        base.statistics().row_count
    ));
    for join in joins {
        match &join.source {
            BoundJoinSource::Table(table) => {
                let right = table_definition(state, table.table_id)?;
                lines.push(format!(
                    "    Seq Scan on {}  (cost={:.2} rows={})",
                    table.binding,
                    right.statistics().row_count as f64 * 0.01,
                    right.statistics().row_count
                ));
            }
            BoundJoinSource::Derived {
                lateral, binding, ..
            } => {
                let label = if *lateral {
                    "Lateral Subquery Scan"
                } else {
                    "Subquery Scan"
                };
                lines.push(format!("    {label} on {binding}  (cost=0.01 rows=1)"));
            }
        }
    }
    Ok(lines)
}

fn execute_update(
    state: &mut DatabaseState,
    table_id: TableId,
    assignments: Vec<(usize, BoundExpr)>,
    filter: Option<BoundExpr>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    table_definition(state, table_id)?;
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::BeforeStatement,
        TriggerEvent::Update,
    )?;
    let source_rows = state
        .rows
        .get(&table_id)
        .map(|rows| (**rows).clone())
        .unwrap_or_default();
    let mut updated = 0u64;
    let mut returned_rows = Vec::new();
    for old_row in source_rows {
        if filter
            .as_ref()
            .map(|filter| execution_predicate_matches(filter, &old_row, params))
            .transpose()?
            .unwrap_or(true)
        {
            let original = old_row.values.clone();
            let mut replacements = Vec::with_capacity(assignments.len());
            for (column_index, expression) in &assignments {
                replacements.push((
                    *column_index,
                    evaluate_scalar(expression, &original, params)?,
                ));
            }
            let mut proposed = old_row.clone();
            for (column_index, value) in replacements {
                proposed.values[column_index] = value;
            }
            let replacement = match fire_row_triggers_with_rows(
                state,
                table_id,
                TriggerTiming::Before,
                TriggerEvent::Update,
                Some(&old_row),
                Some(&proposed),
            )? {
                RowTriggerOutcome::Proceed(Some(row)) => row,
                RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
            };
            let position = state
                .rows
                .get(&table_id)
                .and_then(|rows| rows.iter().position(|row| row == &old_row))
                .ok_or_else(|| {
                    DbError::new(
                        "55000",
                        "BEFORE trigger changed the row targeted by the outer UPDATE",
                    )
                    .with_hint(
                        "Return a replacement NEW row instead of updating the same row recursively.",
                    )
                })?;
            Arc::make_mut(
                state
                    .rows
                    .get_mut(&table_id)
                    .ok_or_else(|| internal_error("updated table rows disappeared"))?,
            )[position] = replacement.clone();
            if replacement != old_row {
                apply_referential_actions(
                    state,
                    vec![ReferentialChange::Update {
                        table_id,
                        old: old_row.clone(),
                        new: replacement.clone(),
                    }],
                )?;
            }
            validate_database_rows(state)?;
            rebuild_table_derived(state, table_id)?;
            let _ = fire_row_triggers_with_rows(
                state,
                table_id,
                TriggerTiming::After,
                TriggerEvent::Update,
                Some(&old_row),
                Some(&replacement),
            )?;
            validate_database_rows(state)?;
            rebuild_table_derived(state, table_id)?;
            if let Some(returning) = &returning {
                returned_rows.push(evaluate_returning(returning, &replacement, params)?);
            }
            updated = updated.saturating_add(1);
        }
    }
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::AfterStatement,
        TriggerEvent::Update,
    )?;
    validate_database_rows(state)?;
    Ok((
        dml_command_events(
            returning.as_ref(),
            format!("UPDATE {updated}"),
            updated,
            returned_rows,
        ),
        true,
    ))
}

fn execute_view_update(
    state: &mut DatabaseState,
    view_id: ViewId,
    source: BoundStatement,
    assignments: Vec<(usize, BoundExpr)>,
    filter: Option<BoundExpr>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let view = state
        .catalog
        .view_by_id(view_id)
        .cloned()
        .ok_or_else(|| internal_error("view UPDATE target disappeared"))?;
    if view.kind != ViewKind::Regular {
        return Err(DbError::new("42809", "cannot modify a materialized view"));
    }
    let (source_rows, notices) = execute_view_source_rows(state, source, &view.output, params)?;
    let mut updated = 0_u64;
    let mut returned_rows = Vec::new();
    for old_row in source_rows {
        ensure_statement_not_cancelled(state)?;
        if !filter
            .as_ref()
            .map(|filter| execution_predicate_matches(filter, &old_row, params))
            .transpose()?
            .unwrap_or(true)
        {
            continue;
        }
        let mut proposed = old_row.clone();
        for (column_index, expression) in &assignments {
            let value = evaluate_scalar(expression, &old_row.values, params)?;
            let target = proposed
                .values
                .get_mut(*column_index)
                .ok_or_else(|| internal_error("view UPDATE column is out of bounds"))?;
            *target = value;
        }
        let returned = match fire_view_row_triggers_with_rows(
            state,
            view_id,
            TriggerEvent::Update,
            Some(&old_row),
            Some(&proposed),
        )? {
            RowTriggerOutcome::Proceed(Some(row)) => row,
            RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
        };
        if let Some(returning) = &returning {
            returned_rows.push(evaluate_returning(returning, &returned, params)?);
        }
        updated = updated.saturating_add(1);
    }
    validate_database_rows(state)?;
    let mut events = dml_command_events(
        returning.as_ref(),
        format!("UPDATE {updated}"),
        updated,
        returned_rows,
    );
    insert_pending_notices(&mut events, notices);
    Ok((events, true))
}

fn execute_delete(
    state: &mut DatabaseState,
    table_id: TableId,
    filter: Option<BoundExpr>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    table_definition(state, table_id)?;
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::BeforeStatement,
        TriggerEvent::Delete,
    )?;
    let source_rows = state
        .rows
        .get(&table_id)
        .map(|rows| (**rows).clone())
        .unwrap_or_default();
    let mut deleted = 0u64;
    let mut returned_rows = Vec::new();
    for old_row in source_rows {
        let matches = filter
            .as_ref()
            .map(|filter| execution_predicate_matches(filter, &old_row, params))
            .transpose()?
            .unwrap_or(true);
        if !matches {
            continue;
        }
        if matches!(
            fire_row_triggers_with_rows(
                state,
                table_id,
                TriggerTiming::Before,
                TriggerEvent::Delete,
                Some(&old_row),
                None,
            )?,
            RowTriggerOutcome::Suppress
        ) {
            continue;
        }
        let position = state
            .rows
            .get(&table_id)
            .and_then(|rows| rows.iter().position(|row| row == &old_row))
            .ok_or_else(|| {
                DbError::new(
                    "55000",
                    "BEFORE trigger changed the row targeted by the outer DELETE",
                )
                .with_hint("Return OLD instead of deleting the same row recursively.")
            })?;
        Arc::make_mut(
            state
                .rows
                .get_mut(&table_id)
                .ok_or_else(|| internal_error("deleted table rows disappeared"))?,
        )
        .remove(position);
        apply_referential_actions(
            state,
            vec![ReferentialChange::Delete {
                table_id,
                old: old_row.clone(),
            }],
        )?;
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        let _ = fire_row_triggers_with_rows(
            state,
            table_id,
            TriggerTiming::After,
            TriggerEvent::Delete,
            Some(&old_row),
            None,
        )?;
        validate_database_rows(state)?;
        rebuild_table_derived(state, table_id)?;
        if let Some(returning) = &returning {
            returned_rows.push(evaluate_returning(returning, &old_row, params)?);
        }
        deleted = deleted.saturating_add(1);
    }
    fire_statement_triggers(
        state,
        table_id,
        TriggerTiming::AfterStatement,
        TriggerEvent::Delete,
    )?;
    validate_database_rows(state)?;
    Ok((
        dml_command_events(
            returning.as_ref(),
            format!("DELETE {deleted}"),
            deleted,
            returned_rows,
        ),
        true,
    ))
}

fn execute_view_delete(
    state: &mut DatabaseState,
    view_id: ViewId,
    source: BoundStatement,
    filter: Option<BoundExpr>,
    returning: Option<BoundReturning>,
    params: &[Value],
) -> Result<(Vec<QueryEvent>, bool)> {
    let view = state
        .catalog
        .view_by_id(view_id)
        .cloned()
        .ok_or_else(|| internal_error("view DELETE target disappeared"))?;
    if view.kind != ViewKind::Regular {
        return Err(DbError::new("42809", "cannot modify a materialized view"));
    }
    let (source_rows, notices) = execute_view_source_rows(state, source, &view.output, params)?;
    let mut deleted = 0_u64;
    let mut returned_rows = Vec::new();
    for old_row in source_rows {
        ensure_statement_not_cancelled(state)?;
        if !filter
            .as_ref()
            .map(|filter| execution_predicate_matches(filter, &old_row, params))
            .transpose()?
            .unwrap_or(true)
        {
            continue;
        }
        let returned = match fire_view_row_triggers_with_rows(
            state,
            view_id,
            TriggerEvent::Delete,
            Some(&old_row),
            None,
        )? {
            RowTriggerOutcome::Proceed(Some(row)) => row,
            RowTriggerOutcome::Proceed(None) | RowTriggerOutcome::Suppress => continue,
        };
        if let Some(returning) = &returning {
            returned_rows.push(evaluate_returning(returning, &returned, params)?);
        }
        deleted = deleted.saturating_add(1);
    }
    validate_database_rows(state)?;
    let mut events = dml_command_events(
        returning.as_ref(),
        format!("DELETE {deleted}"),
        deleted,
        returned_rows,
    );
    insert_pending_notices(&mut events, notices);
    Ok((events, true))
}

fn evaluate_returning(returning: &BoundReturning, row: &Row, params: &[Value]) -> Result<Row> {
    returning
        .projection
        .iter()
        .map(|projection| evaluate_scalar(&projection.expr, &row.values, params))
        .collect::<Result<Vec<_>>>()
        .map(Row::new)
}

fn dml_command_events(
    returning: Option<&BoundReturning>,
    tag: impl Into<String>,
    rows_affected: u64,
    rows: Vec<Row>,
) -> Vec<QueryEvent> {
    match returning {
        Some(returning) => {
            let schema = returning.schema.clone();
            let mut events = vec![QueryEvent::Schema(schema.clone())];
            let mut batch_rows = Vec::with_capacity(DEFAULT_BATCH_ROWS.min(rows.len()));
            for row in rows {
                batch_rows.push(row);
                if batch_rows.len() == DEFAULT_BATCH_ROWS {
                    events.push(QueryEvent::Batch(Batch {
                        schema: schema.clone(),
                        rows: mem::take(&mut batch_rows),
                    }));
                }
            }
            if !batch_rows.is_empty() {
                events.push(QueryEvent::Batch(Batch {
                    schema: schema.clone(),
                    rows: batch_rows,
                }));
            }
            events.push(QueryEvent::Progress(QueryProgress {
                rows_processed: rows_affected,
            }));
            events.push(QueryEvent::Complete(CommandComplete {
                tag: tag.into(),
                rows_affected,
            }));
            events
        }
        None => command_events(Schema::empty(), tag, rows_affected, None),
    }
}

fn removed_rows(before: &[Row], after: &[Row]) -> Vec<Row> {
    let mut matched = vec![false; after.len()];
    let mut removed = Vec::new();
    for row in before {
        if let Some((index, _)) = after
            .iter()
            .enumerate()
            .find(|(index, candidate)| !matched[*index] && *candidate == row)
        {
            matched[index] = true;
        } else {
            removed.push(row.clone());
        }
    }
    removed
}

fn apply_referential_actions(
    state: &mut DatabaseState,
    changes: Vec<ReferentialChange>,
) -> Result<()> {
    let mut queue = VecDeque::from(changes);
    let mut applied = 0usize;
    while let Some(change) = queue.pop_front() {
        applied = applied.saturating_add(1);
        if applied > MAX_REFERENTIAL_ACTIONS {
            return Err(DbError::new(
                "54001",
                "referential action work exceeds the configured limit",
            ));
        }
        let (referenced_table_id, old_row, new_row) = match &change {
            ReferentialChange::Delete { table_id, old } => (*table_id, old, None),
            ReferentialChange::Update { table_id, old, new } => (*table_id, old, Some(new)),
        };
        let referenced_table = table_definition(state, referenced_table_id)?.clone();
        let referencing = state
            .catalog
            .database()
            .schemas()
            .flat_map(|schema| schema.tables())
            .flat_map(|table| {
                table.constraints().filter_map(|constraint| {
                    let ConstraintKind::ForeignKey {
                        columns,
                        referenced_table,
                        referenced_columns,
                        on_delete,
                        on_update,
                    } = &constraint.kind
                    else {
                        return None;
                    };
                    (*referenced_table == referenced_table_id).then(|| {
                        (
                            table.id,
                            constraint.name.clone(),
                            columns.clone(),
                            referenced_columns.clone(),
                            *on_delete,
                            *on_update,
                        )
                    })
                })
            })
            .collect::<Vec<_>>();

        for (
            child_table_id,
            constraint_name,
            local_columns,
            referenced_columns,
            on_delete,
            on_update,
        ) in referencing
        {
            let child_table = table_definition(state, child_table_id)?.clone();
            let parent_positions = referenced_columns
                .iter()
                .map(|column_id| {
                    referenced_table
                        .column_index_by_id(*column_id)
                        .ok_or_else(|| {
                            internal_error("foreign-key parent column is absent during action")
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let child_positions = local_columns
                .iter()
                .map(|column_id| {
                    child_table.column_index_by_id(*column_id).ok_or_else(|| {
                        internal_error("foreign-key child column is absent during action")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            if let Some(new_row) = new_row
                && parent_positions
                    .iter()
                    .all(|position| old_row.values[*position] == new_row.values[*position])
            {
                continue;
            }
            let action = if new_row.is_some() {
                on_update
            } else {
                on_delete
            };
            let child_rows = Arc::make_mut(
                state
                    .rows
                    .entry(child_table_id)
                    .or_insert_with(|| Arc::new(Vec::new())),
            );
            let matches_parent = |row: &Row| {
                child_positions
                    .iter()
                    .zip(&parent_positions)
                    .all(|(child, parent)| row.values[*child] == old_row.values[*parent])
            };
            if matches!(
                action,
                ReferentialAction::NoAction | ReferentialAction::Restrict
            ) && child_rows.iter().any(matches_parent)
            {
                return Err(DbError::new(
                    "23503",
                    format!("update or delete violates foreign-key constraint {constraint_name}"),
                ));
            }
            match action {
                ReferentialAction::NoAction | ReferentialAction::Restrict => {}
                ReferentialAction::Cascade if new_row.is_none() => {
                    let before = child_rows.clone();
                    child_rows.retain(|row| !matches_parent(row));
                    queue.extend(removed_rows(&before, child_rows).into_iter().map(|old| {
                        ReferentialChange::Delete {
                            table_id: child_table_id,
                            old,
                        }
                    }));
                }
                ReferentialAction::Cascade => {
                    let new_row = new_row.ok_or_else(|| {
                        internal_error("update cascade has no replacement parent row")
                    })?;
                    for child_row in child_rows.iter_mut().filter(|row| matches_parent(row)) {
                        let old = child_row.clone();
                        for (child, parent) in child_positions.iter().zip(&parent_positions) {
                            child_row.values[*child] = new_row.values[*parent].clone();
                        }
                        queue.push_back(ReferentialChange::Update {
                            table_id: child_table_id,
                            old,
                            new: child_row.clone(),
                        });
                    }
                }
                ReferentialAction::SetNull | ReferentialAction::SetDefault => {
                    for child_row in child_rows.iter_mut().filter(|row| matches_parent(row)) {
                        let old = child_row.clone();
                        for child in &child_positions {
                            child_row.values[*child] = if action == ReferentialAction::SetNull {
                                Value::Null
                            } else {
                                let column = &child_table.columns()[*child];
                                column_default_value(&state.catalog, column)?
                            };
                        }
                        queue.push_back(ReferentialChange::Update {
                            table_id: child_table_id,
                            old,
                            new: child_row.clone(),
                        });
                    }
                }
            }
            rebuild_table_derived(state, child_table_id)?;
        }
    }
    Ok(())
}

fn validate_rows(catalog: &Catalog, table: &TableDefinition, rows: &[Row]) -> Result<()> {
    for row in rows {
        if row.values.len() != table.columns().len() {
            return Err(internal_error("row width does not match table metadata"));
        }
        for (column, value) in table.columns().iter().zip(&row.values) {
            if !column.nullable && value.is_null() {
                return Err(DbError::new(
                    "23502",
                    format!(
                        "null value in column {} violates not-null constraint",
                        column.name
                    ),
                ));
            }
            coerce_execution_value(value.clone(), &column.data_type)?;
        }
    }
    for (column_index, column) in table.columns().iter().enumerate() {
        let Some(type_id) = column.declared_type else {
            continue;
        };
        let definition = catalog.type_by_id(type_id).ok_or_else(|| {
            DbError::new(
                "XX001",
                format!("column {} references a missing declared type", column.name),
            )
        })?;
        match &definition.definition {
            UserDefinedTypeKind::Enum { labels } => {
                for row in rows {
                    validate_enum_value(&row.values[column_index], labels, &definition.name)?;
                }
            }
            UserDefinedTypeKind::Domain {
                base_type,
                not_null,
                checks,
                ..
            } => {
                let scope = TableDefinition::expression_scope(
                    Identifier::unquoted("value"),
                    base_type.clone(),
                );
                let checks = checks
                    .iter()
                    .map(|constraint| {
                        Ok((
                            constraint.name.as_ref(),
                            bind_catalog_expression_with_catalog(
                                &constraint.expression,
                                Some(&scope),
                                Some(&ScalarType::Boolean),
                                catalog,
                            )?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                for row in rows {
                    let value = &row.values[column_index];
                    let domain_values = match value {
                        Value::Array(array) => array.values().iter().collect::<Vec<_>>(),
                        Value::Null if matches!(column.data_type, ScalarType::Array { .. }) => {
                            Vec::new()
                        }
                        value => vec![value],
                    };
                    for value in domain_values {
                        if *not_null && value.is_null() {
                            return Err(DbError::new(
                                "23502",
                                format!("domain {} does not allow null values", definition.name),
                            ));
                        }
                        for (constraint_name, check) in &checks {
                            if evaluate_scalar(check, std::slice::from_ref(value), &[])?
                                == Value::Boolean(false)
                            {
                                let label = constraint_name.map_or_else(
                                    || format!("domain {}", definition.name),
                                    |name| format!("constraint {name}"),
                                );
                                return Err(DbError::new(
                                    "23514",
                                    format!(
                                        "value for domain {} violates {label}",
                                        definition.name
                                    ),
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    for (column_index, column) in table.columns().iter().enumerate() {
        if !column.unique {
            continue;
        }
        for left in 0..rows.len() {
            let left_value = &rows[left].values[column_index];
            if left_value.is_null() {
                continue;
            }
            for right_row in rows.iter().skip(left + 1) {
                if left_value == &right_row.values[column_index] {
                    return Err(DbError::new(
                        "23505",
                        format!(
                            "duplicate value violates unique constraint on {}",
                            column.name
                        ),
                    ));
                }
            }
        }
    }
    for constraint in table.constraints() {
        match &constraint.kind {
            ConstraintKind::PrimaryKey { columns } | ConstraintKind::Unique { columns } => {
                let positions = columns
                    .iter()
                    .map(|column_id| {
                        table.column_index_by_id(*column_id).ok_or_else(|| {
                            internal_error("constraint column is absent from its table")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                for (left, left_row) in rows.iter().enumerate() {
                    let left_key = positions
                        .iter()
                        .map(|position| &left_row.values[*position])
                        .collect::<Vec<_>>();
                    if matches!(constraint.kind, ConstraintKind::PrimaryKey { .. })
                        && left_key.iter().any(|value| value.is_null())
                    {
                        return Err(DbError::new(
                            "23502",
                            format!(
                                "null value violates primary-key constraint {}",
                                constraint.name
                            ),
                        ));
                    }
                    if left_key.iter().any(|value| value.is_null()) {
                        continue;
                    }
                    for right_row in rows.iter().skip(left + 1) {
                        if positions.iter().all(|position| {
                            left_row.values[*position] == right_row.values[*position]
                        }) {
                            return Err(DbError::new(
                                "23505",
                                format!(
                                    "duplicate value violates unique constraint {}",
                                    constraint.name
                                ),
                            ));
                        }
                    }
                }
            }
            ConstraintKind::Check { expression } => {
                let bound = bind_catalog_expression_with_catalog(
                    expression,
                    Some(table),
                    Some(&ScalarType::Boolean),
                    catalog,
                )?;
                for row in rows {
                    if evaluate_scalar(&bound, &row.values, &[])? == Value::Boolean(false) {
                        return Err(DbError::new(
                            "23514",
                            format!("row violates check constraint {}", constraint.name),
                        ));
                    }
                }
            }
            ConstraintKind::ForeignKey { .. } => {}
        }
    }
    Ok(())
}

fn validate_enum_value(value: &Value, labels: &[String], type_name: &Identifier) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        Value::Text(value) if labels.iter().any(|label| label == value) => Ok(()),
        Value::Array(array) => {
            for value in array.values() {
                validate_enum_value(value, labels, type_name)?;
            }
            Ok(())
        }
        Value::Text(value) => Err(DbError::new(
            "22P02",
            format!("invalid input value for enum {type_name}: {value:?}"),
        )),
        _ => Err(DbError::new(
            "42804",
            format!("value is not assignable to enum {type_name}"),
        )),
    }
}

fn validate_database_rows(state: &DatabaseState) -> Result<()> {
    for schema in state.catalog.database().schemas() {
        for table in schema.tables() {
            let rows = state
                .rows
                .get(&table.id)
                .map_or(&[][..], |rows| rows.as_slice());
            validate_rows(&state.catalog, table, rows)?;
            for constraint in table.constraints() {
                let ConstraintKind::ForeignKey {
                    columns,
                    referenced_table,
                    referenced_columns,
                    ..
                } = &constraint.kind
                else {
                    continue;
                };
                let local_positions = columns
                    .iter()
                    .map(|column_id| {
                        table.column_index_by_id(*column_id).ok_or_else(|| {
                            internal_error("foreign-key column is absent from its table")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let referenced = table_definition(state, *referenced_table)?;
                let referenced_positions = referenced_columns
                    .iter()
                    .map(|column_id| {
                        referenced.column_index_by_id(*column_id).ok_or_else(|| {
                            internal_error("foreign-key referenced column is absent from its table")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let referenced_rows = state
                    .rows
                    .get(referenced_table)
                    .map_or(&[][..], |rows| rows.as_slice());
                for row in rows {
                    if local_positions
                        .iter()
                        .any(|position| row.values[*position].is_null())
                    {
                        continue;
                    }
                    if !referenced_rows.iter().any(|candidate| {
                        local_positions
                            .iter()
                            .zip(&referenced_positions)
                            .all(|(local, remote)| row.values[*local] == candidate.values[*remote])
                    }) {
                        return Err(DbError::new(
                            "23503",
                            format!(
                                "insert or update violates foreign-key constraint {}",
                                constraint.name
                            ),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn rebuild_btree_index(state: &mut DatabaseState, index_id: IndexId) -> Result<()> {
    let definition = state
        .catalog
        .index_by_id(index_id)
        .cloned()
        .ok_or_else(|| DbError::new("42704", "index does not exist"))?;
    if definition.method != IndexMethod::BTree {
        return Err(internal_error(
            "non-B-tree index reached the B-tree rebuild path",
        ));
    }
    let table = table_definition(state, definition.table_id)?.clone();
    let rows = state
        .rows
        .get(&definition.table_id)
        .cloned()
        .unwrap_or_default();
    let key_positions = definition
        .key_columns
        .iter()
        .map(|column_id| {
            table
                .column_index_by_id(*column_id)
                .ok_or_else(|| internal_error("index key column is absent from its table"))
        })
        .collect::<Result<Vec<_>>>()?;
    let key_types = key_positions
        .iter()
        .map(|position| table.columns()[*position].data_type.clone())
        .collect::<Vec<_>>();
    let include_positions = definition
        .include_columns
        .iter()
        .map(|column_id| {
            table
                .column_index_by_id(*column_id)
                .ok_or_else(|| internal_error("index include column is absent from its table"))
        })
        .collect::<Result<Vec<_>>>()?;
    let entries = rows
        .iter()
        .enumerate()
        .map(|(row_index, row)| {
            let row_id = u64::try_from(row_index)
                .map(RowId::new)
                .map_err(|_| DbError::new("54000", "table row count exceeds index limits"))?;
            let key_values = key_positions
                .iter()
                .map(|position| row.values[*position].clone())
                .collect::<Vec<_>>();
            let included = include_positions
                .iter()
                .map(|position| row.values[*position].clone())
                .collect();
            IndexEntry::new_typed(&key_values, &key_types, row_id, included)
        })
        .collect::<Result<Vec<_>>>()?;
    let tree = BPlusTree::from_entries(definition.unique, entries)?;
    state.indexes.insert(index_id, Arc::new(tree));
    Ok(())
}

fn rebuild_index_derived(state: &mut DatabaseState, index_id: IndexId) -> Result<()> {
    let definition = state
        .catalog
        .index_by_id(index_id)
        .cloned()
        .ok_or_else(|| DbError::new("42704", "index does not exist"))?;
    match definition.method {
        IndexMethod::BTree => rebuild_btree_index(state, index_id),
        IndexMethod::FullText | IndexMethod::Hnsw => {
            rebuild_search_catalog_for_table(state, definition.table_id)
        }
    }
}

fn rebuild_table_indexes(state: &mut DatabaseState, table_id: TableId) -> Result<()> {
    let table = table_definition(state, table_id)?.clone();
    let index_methods = table
        .indexes()
        .map(|definition| (definition.id, definition.method))
        .collect::<Vec<_>>();
    let catalog = Arc::clone(&state.catalog);
    state.indexes.retain(|index_id, _| {
        catalog
            .index_by_id(*index_id)
            .is_some_and(|definition| definition.method == IndexMethod::BTree)
    });
    for (index_id, method) in &index_methods {
        if *method == IndexMethod::BTree {
            rebuild_btree_index(state, *index_id)?;
        }
    }
    rebuild_search_catalog_for_table(state, table_id)?;
    Ok(())
}

fn rebuild_table_derived(state: &mut DatabaseState, table_id: TableId) -> Result<()> {
    rebuild_table_indexes(state, table_id)?;
    let table = table_definition(state, table_id)?.clone();
    let rows = state.rows.get(&table_id).cloned().unwrap_or_default();
    Arc::make_mut(&mut state.catalog)
        .set_table_statistics(table_id, compute_statistics(&table, &rows)?)?;
    Ok(())
}

fn rebuild_search_catalog_for_table(state: &mut DatabaseState, table_id: TableId) -> Result<()> {
    let searches = state
        .searches
        .rebuild_table(&state.catalog, &state.rows, table_id)?;
    state.searches = Arc::new(searches);
    Ok(())
}

fn reconcile_search_catalog(state: &mut DatabaseState) -> Result<()> {
    let searches = state.searches.reconcile(&state.catalog, &state.rows)?;
    state.searches = Arc::new(searches);
    Ok(())
}

fn compute_statistics(table: &TableDefinition, rows: &[Row]) -> Result<TableStatistics> {
    let mut columns = BTreeMap::new();
    for (column_index, column) in table.columns().iter().enumerate() {
        let values = rows
            .iter()
            .filter_map(|row| row.values.get(column_index))
            .collect::<Vec<_>>();
        let null_count = values.iter().filter(|value| value.is_null()).count() as u64;
        let mut distinct = HashSet::new();
        for value in values.iter().filter(|value| !value.is_null()) {
            distinct.insert(encode_row(&Row::new(vec![(*value).clone()]))?);
        }
        let (min, max) = if indexable_type(&column.data_type) {
            let mut minimum: Option<(IndexKey, Value)> = None;
            let mut maximum: Option<(IndexKey, Value)> = None;
            for value in values.iter().filter(|value| !value.is_null()) {
                let typed_value = (*value).clone();
                let key = IndexKey::from_typed_values(
                    std::slice::from_ref(&typed_value),
                    std::slice::from_ref(&column.data_type),
                )?;
                if minimum.as_ref().is_none_or(|(minimum, _)| key < *minimum) {
                    minimum = Some((key.clone(), (*value).clone()));
                }
                if maximum.as_ref().is_none_or(|(maximum, _)| key > *maximum) {
                    maximum = Some((key, (*value).clone()));
                }
            }
            (
                minimum.map(|(_, value)| value),
                maximum.map(|(_, value)| value),
            )
        } else {
            (None, None)
        };
        columns.insert(
            column.id,
            ColumnStatistics {
                null_count,
                distinct_count: distinct.len() as u64,
                min,
                max,
            },
        );
    }
    Ok(TableStatistics {
        row_count: rows.len() as u64,
        columns,
    })
}

fn table_definition(state: &DatabaseState, table_id: TableId) -> Result<&TableDefinition> {
    state
        .catalog
        .table_by_id(table_id)
        .ok_or_else(|| internal_error(format!("bound table ID {table_id:?} does not exist")))
}

fn search_index_table(
    state: &DatabaseState,
    index_id: IndexId,
    expected_method: IndexMethod,
) -> Result<TableId> {
    let definition = state
        .catalog
        .index_by_id(index_id)
        .ok_or_else(|| DbError::new("42704", format!("index {} does not exist", index_id.get())))?;
    if definition.method != expected_method {
        return Err(DbError::new(
            "42809",
            format!(
                "index {} uses {:?}, expected {expected_method:?}",
                definition.name, definition.method
            ),
        ));
    }
    Ok(definition.table_id)
}

fn search_result_row(state: &DatabaseState, table_id: TableId, row_id: SearchRowId) -> Result<Row> {
    let row_index = usize::try_from(row_id.get())
        .map_err(|_| internal_error("search Row ID exceeds the platform limit"))?;
    state
        .rows
        .get(&table_id)
        .and_then(|rows| rows.get(row_index))
        .cloned()
        .ok_or_else(|| internal_error("search index returned a Row ID outside its table snapshot"))
}

fn evaluate_search_filter(
    state: &DatabaseState,
    table_id: TableId,
    filter: &ScalarSearchFilter,
) -> Result<AllowedRows> {
    let table = table_definition(state, table_id)?;
    let expression = bind_catalog_expression_with_catalog(
        &CatalogExpression::new(&filter.expression),
        Some(table),
        Some(&ScalarType::Boolean),
        &state.catalog,
    )?;
    let rows = state
        .rows
        .get(&table_id)
        .map_or(&[][..], |rows| rows.as_slice());
    let mut allowed = BTreeSet::new();
    for (row_index, row) in rows.iter().enumerate() {
        if execution_predicate_matches(&expression, row, &filter.parameters)? {
            allowed.insert(
                u64::try_from(row_index)
                    .map(SearchRowId::new)
                    .map_err(|_| DbError::new("54000", "table row count exceeds search limits"))?,
            );
        }
    }
    Ok(Arc::new(allowed))
}

fn intersect_allowed_rows(
    current: Option<AllowedRows>,
    filter: AllowedRows,
) -> Option<AllowedRows> {
    match current {
        Some(current) => Some(Arc::new(current.intersection(&filter).copied().collect())),
        None => Some(filter),
    }
}

fn command_events(
    schema: Schema,
    tag: impl Into<String>,
    rows_affected: u64,
    batch: Option<Batch>,
) -> Vec<QueryEvent> {
    let mut events = vec![QueryEvent::Schema(schema)];
    if let Some(batch) = batch {
        events.push(QueryEvent::Batch(batch));
    }
    events.push(QueryEvent::Progress(QueryProgress {
        rows_processed: rows_affected,
    }));
    events.push(QueryEvent::Complete(CommandComplete {
        tag: tag.into(),
        rows_affected,
    }));
    events
}

fn internal_error(message: impl Into<String>) -> DbError {
    DbError::new("XX000", message).with_hint("restart the session and retry")
}

#[must_use]
pub fn configured_data_dir(config: &EngineConfig) -> &Path {
    &config.data_dir
}

#[cfg(test)]
mod tests {
    use tempfile::{TempDir, tempdir};

    use super::*;

    fn engine() -> (TempDir, Engine) {
        let directory = tempdir().expect("tempdir");
        let engine = Engine::open(EngineConfig::new(directory.path())).expect("open engine");
        (directory, engine)
    }

    fn execute(session: &mut Session, sql: &str, params: &[Value]) -> Vec<QueryEvent> {
        session
            .execute(sql, params)
            .expect("execute statement")
            .collect()
    }

    fn rows(events: &[QueryEvent]) -> Vec<Row> {
        events
            .iter()
            .filter_map(|event| match event {
                QueryEvent::Batch(batch) => Some(batch.rows.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    fn catalog_setting(name: &str, setting: &str) -> CatalogSettingMetadata {
        CatalogSettingMetadata {
            name: name.to_owned(),
            setting: setting.to_owned(),
            unit: None,
            category: "OrdaDB test settings".to_owned(),
            short_description: format!("Test projection for {name}."),
            context: "user".to_owned(),
            value_type: "string".to_owned(),
            source: "session".to_owned(),
            minimum: None,
            maximum: None,
            enum_values: None,
            boot_value: setting.to_owned(),
            reset_value: setting.to_owned(),
        }
    }

    #[test]
    fn system_catalog_queries_use_normal_relational_execution() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE catalog_widgets (id BIGINT PRIMARY KEY, label TEXT)",
            &[],
        );

        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT nspname FROM pg_catalog.pg_namespace \
                 WHERE nspname <> $1 ORDER BY nspname LIMIT 2",
                &[Value::Text("information_schema".into())],
            )),
            vec![
                Row::new(vec![Value::Text("pg_catalog".into())]),
                Row::new(vec![Value::Text("public".into())]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT n.nspname, c.relname \
                 FROM pg_catalog.pg_namespace AS n \
                 JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid \
                 WHERE c.relname = 'catalog_widgets'",
                &[],
            )),
            vec![Row::new(vec![
                Value::Text("public".into()),
                Value::Text("catalog_widgets".into()),
            ])]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "WITH matching AS (\
                    SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = $1\
                 ) SELECT nspname FROM matching",
                &[Value::Text("public".into())],
            )),
            vec![Row::new(vec![Value::Text("public".into())])]
        );
    }

    #[test]
    fn system_catalog_materializes_only_relations_referenced_by_the_statement() {
        let catalog = Catalog::default();
        let requested = BTreeSet::from([
            ordadb_catalog::PG_NAMESPACE_TABLE_ID,
            ordadb_catalog::PG_CLASS_TABLE_ID,
        ]);
        let snapshot = system_catalog::build_system_catalog_snapshot(&catalog, None, &requested)
            .expect("requested system rows");
        assert_eq!(
            snapshot.tables().keys().copied().collect::<BTreeSet<_>>(),
            requested
        );
        assert!(!snapshot.tables()[&ordadb_catalog::PG_NAMESPACE_TABLE_ID].is_empty());
        assert!(!snapshot.tables()[&ordadb_catalog::PG_CLASS_TABLE_ID].is_empty());
        let grant = MemoryGrant::new(64 * 1024, 1024 * 1024).expect("scan grant");
        let mut scan = snapshot
            .scan(ordadb_catalog::PG_NAMESPACE_TABLE_ID)
            .expect("virtual scan");
        let first = scan
            .next_chunk(1, &grant)
            .expect("first virtual chunk")
            .expect("virtual row");
        assert_eq!(first.chunk().len(), 1);
        assert!(grant.peak_bytes() > 0);
    }

    #[test]
    fn system_catalog_supporting_relations_and_information_schema_are_relational() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE catalog_parent (
                tenant BIGINT,
                id BIGINT,
                code TEXT,
                CONSTRAINT catalog_parent_pk PRIMARY KEY (tenant, id),
                CONSTRAINT catalog_parent_code_unique UNIQUE (tenant, code),
                CONSTRAINT catalog_parent_code_check CHECK (code <> '')
            )",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE catalog_child (
                tenant BIGINT,
                parent_id BIGINT,
                CONSTRAINT catalog_child_parent_fk FOREIGN KEY (tenant, parent_id)
                    REFERENCES catalog_parent (tenant, id)
            )",
            &[],
        );
        execute(
            &mut session,
            "CREATE VIEW catalog_parent_view AS SELECT tenant, id, code FROM catalog_parent",
            &[],
        );
        execute(
            &mut session,
            "CREATE SEQUENCE catalog_sequence INCREMENT BY 2 START WITH 10",
            &[],
        );
        execute(
            &mut session,
            "CREATE FUNCTION catalog_echo(value BIGINT)
             RETURNS BIGINT
             LANGUAGE plpgsql
             AS $$
             BEGIN
             RETURN value;
             END;
             $$",
            &[],
        );

        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT a.amname, p.proname
                 FROM pg_catalog.pg_am AS a
                 JOIN pg_catalog.pg_proc AS p ON p.oid = a.amhandler
                 ORDER BY a.amname",
                &[],
            )),
            vec![
                Row::new(vec![
                    Value::Text("btree".into()),
                    Value::Text("bthandler".into()),
                ]),
                Row::new(vec![
                    Value::Text("heap".into()),
                    Value::Text("heap_tableam_handler".into()),
                ]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT collname FROM pg_catalog.pg_collation ORDER BY oid",
                &[],
            )),
            vec![
                Row::new(vec![Value::Text("C".into())]),
                Row::new(vec![Value::Text("POSIX".into())]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT table_name, check_option, is_updatable
                 FROM information_schema.views
                 WHERE table_name = 'catalog_parent_view'",
                &[],
            )),
            vec![Row::new(vec![
                Value::Text("catalog_parent_view".into()),
                Value::Text("NONE".into()),
                Value::Text("NO".into()),
            ])]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT sequence_name, data_type, numeric_precision, increment, cycle_option
                 FROM information_schema.sequences
                 WHERE sequence_name = 'catalog_sequence'",
                &[],
            )),
            vec![Row::new(vec![
                Value::Text("catalog_sequence".into()),
                Value::Text("bigint".into()),
                Value::Int32(64),
                Value::Text("2".into()),
                Value::Text("NO".into()),
            ])]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT constraint_name, constraint_type
                 FROM information_schema.table_constraints
                 WHERE table_name = 'catalog_parent'
                 ORDER BY constraint_name",
                &[],
            )),
            vec![
                Row::new(vec![
                    Value::Text("catalog_parent_code_check".into()),
                    Value::Text("CHECK".into()),
                ]),
                Row::new(vec![
                    Value::Text("catalog_parent_code_unique".into()),
                    Value::Text("UNIQUE".into()),
                ]),
                Row::new(vec![
                    Value::Text("catalog_parent_pk".into()),
                    Value::Text("PRIMARY KEY".into()),
                ]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT column_name, ordinal_position, position_in_unique_constraint
                 FROM information_schema.key_column_usage
                 WHERE constraint_name = 'catalog_child_parent_fk'
                 ORDER BY ordinal_position",
                &[],
            )),
            vec![
                Row::new(vec![
                    Value::Text("tenant".into()),
                    Value::Int32(1),
                    Value::Int32(1),
                ]),
                Row::new(vec![
                    Value::Text("parent_id".into()),
                    Value::Int32(2),
                    Value::Int32(2),
                ]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT routine_name, routine_type, data_type, routine_definition
                 FROM information_schema.routines
                 WHERE routine_name = 'catalog_echo'",
                &[],
            )),
            vec![Row::new(vec![
                Value::Text("catalog_echo".into()),
                Value::Text("FUNCTION".into()),
                Value::Text("bigint".into()),
                Value::Null,
            ])]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT p.ordinal_position, p.parameter_mode, p.parameter_name, p.data_type
                 FROM information_schema.parameters AS p
                 JOIN information_schema.routines AS r
                   ON r.specific_name = p.specific_name
                 WHERE r.routine_name = 'catalog_echo'
                 ORDER BY p.ordinal_position",
                &[],
            )),
            vec![
                Row::new(vec![
                    Value::Int32(0),
                    Value::Null,
                    Value::Null,
                    Value::Text("bigint".into()),
                ]),
                Row::new(vec![
                    Value::Int32(1),
                    Value::Text("IN".into()),
                    Value::Text("value".into()),
                    Value::Text("bigint".into()),
                ]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT dependent.relname, referenced.relname, d.deptype
                 FROM pg_catalog.pg_depend AS d
                 JOIN pg_catalog.pg_class AS dependent ON dependent.oid = d.objid
                 JOIN pg_catalog.pg_class AS referenced ON referenced.oid = d.refobjid
                 WHERE dependent.relname = 'catalog_parent_view'",
                &[],
            )),
            vec![Row::new(vec![
                Value::Text("catalog_parent_view".into()),
                Value::Text("catalog_parent".into()),
                Value::Text("n".into()),
            ])]
        );
        assert!(
            rows(&execute(
                &mut session,
                "SELECT * FROM pg_catalog.pg_description",
                &[],
            ))
            .is_empty()
        );
        assert!(
            rows(&execute(
                &mut session,
                "SELECT * FROM pg_catalog.pg_inherits",
                &[],
            ))
            .is_empty()
        );
    }

    #[test]
    fn system_catalog_oids_survive_engine_reopen() {
        let (directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE reopen_catalog_oid (id BIGINT PRIMARY KEY)",
            &[],
        );
        let before = rows(&execute(
            &mut session,
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'reopen_catalog_oid'",
            &[],
        ));
        assert_eq!(before.len(), 1);
        drop(session);
        drop(engine);

        let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen engine");
        let mut session = reopened.connect().expect("reconnect");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'reopen_catalog_oid'",
                &[],
            )),
            before
        );
    }

    #[test]
    fn system_catalog_visibility_roles_settings_and_writes_are_safe() {
        let (_directory, engine) = engine();
        let mut alice = engine
            .connect_authenticated(
                SessionAuthorization::new("alice", false)
                    .expect("alice authorization")
                    .with_system_catalog_metadata(
                        vec![
                            CatalogRoleMetadata {
                                postgres_oid: 20_001,
                                name: "alice".to_owned(),
                                can_login: true,
                                login_enabled: true,
                            },
                            CatalogRoleMetadata {
                                postgres_oid: 20_002,
                                name: "reporting".to_owned(),
                                can_login: false,
                                login_enabled: false,
                            },
                        ],
                        vec![catalog_setting("application_name", "catalog-test")],
                    )
                    .expect("system catalog metadata"),
            )
            .expect("alice session");
        execute(
            &mut alice,
            "CREATE TABLE alice_private (id BIGINT PRIMARY KEY)",
            &[],
        );
        let catalog = engine.catalog_snapshot().expect("catalog snapshot");
        let public_schema = catalog
            .schema(&Identifier::unquoted("public"))
            .expect("public schema");
        let public_schema_oid = i64::from(
            catalog
                .postgres_oid(ordadb_catalog::PostgresOidObject::Schema(public_schema.id))
                .expect("public schema OID")
                .get(),
        );

        let role_rows = rows(&execute(
            &mut alice,
            "SELECT rolname, oid, rolpassword FROM pg_catalog.pg_roles ORDER BY oid",
            &[],
        ));
        assert_eq!(
            role_rows,
            vec![
                Row::new(vec![
                    Value::Text("alice".into()),
                    Value::Int64(20_001),
                    Value::Text("********".into()),
                ]),
                Row::new(vec![
                    Value::Text("reporting".into()),
                    Value::Int64(20_002),
                    Value::Text("********".into()),
                ]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut alice,
                "SELECT setting FROM pg_catalog.pg_settings \
                 WHERE name = 'application_name'",
                &[],
            )),
            vec![Row::new(vec![Value::Text("catalog-test".into())])]
        );
        let namespace_events = execute(
            &mut alice,
            "SELECT n.oid, n.nspname, r.rolname \
             FROM pg_catalog.pg_namespace AS n \
             LEFT JOIN pg_catalog.pg_roles AS r ON r.oid = n.nspowner \
             WHERE n.nspname = 'public'",
            &[],
        );
        let namespace_schema = namespace_events
            .iter()
            .find_map(|event| match event {
                QueryEvent::Schema(schema) => Some(schema),
                _ => None,
            })
            .expect("namespace schema");
        assert_eq!(namespace_schema.fields[0].data_type, ScalarType::Oid);
        assert_eq!(namespace_schema.fields[1].data_type, ScalarType::Name);
        assert_eq!(namespace_schema.fields[2].data_type, ScalarType::Name);
        assert_eq!(
            rows(&namespace_events),
            vec![Row::new(vec![
                Value::Int64(public_schema_oid),
                Value::Text("public".into()),
                Value::Null,
            ])]
        );

        let mut bob = engine
            .connect_authenticated(SessionAuthorization::new("bob", false).expect("bob auth"))
            .expect("bob session");
        assert!(
            rows(&execute(
                &mut bob,
                "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'alice_private'",
                &[],
            ))
            .is_empty()
        );
        let visibility = CatalogVisibility::from_scopes([CatalogVisibilityScope::Object {
            schema: "public".to_owned(),
            name: "alice_private".to_owned(),
        }])
        .expect("catalog visibility");
        let mut reporting = engine
            .connect_authenticated(
                SessionAuthorization::new("reporter", false)
                    .expect("reporter auth")
                    .with_catalog_visibility(visibility),
            )
            .expect("reporter session");
        assert_eq!(
            rows(&execute(
                &mut reporting,
                "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'alice_private'",
                &[],
            )),
            vec![Row::new(vec![Value::Text("alice_private".into())])]
        );
        let error = bob
            .execute("DELETE FROM pg_catalog.pg_namespace", &[])
            .expect_err("system DML must fail");
        assert_eq!(error.sql_state, "42501");
        let error = bob
            .execute("DROP TABLE pg_catalog.pg_namespace", &[])
            .expect_err("system DDL must fail");
        assert_eq!(error.sql_state, "42501");
    }

    fn create_documents(session: &mut Session) {
        execute(
            session,
            "CREATE TABLE documents (\
                id BIGINT PRIMARY KEY,\
                title TEXT NOT NULL,\
                score INTEGER\
            )",
            &[],
        );
    }

    #[test]
    fn enum_and_domain_ddl_validate_values_and_reopen() {
        let (directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')",
            &[],
        );
        execute(
            &mut session,
            "CREATE DOMAIN positive_int AS integer DEFAULT 1 NOT NULL \
             CONSTRAINT positive CHECK (VALUE > 0)",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE feelings (current_mood mood NOT NULL, score positive_int)",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE typed_arrays (id bigint, moods mood[], scores positive_int[])",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO feelings (current_mood) VALUES ('happy')",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO typed_arrays VALUES (1, ARRAY['sad', 'happy'], ARRAY[1, 2])",
            &[],
        );

        let error = match session.execute("INSERT INTO feelings VALUES ('angry', 1)", &[]) {
            Ok(_) => panic!("invalid enum value was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "22P02");
        let error = match session.execute("INSERT INTO feelings VALUES ('ok', 0)", &[]) {
            Ok(_) => panic!("invalid domain value was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "23514");
        let error = match session.execute("INSERT INTO feelings VALUES ('ok', NULL)", &[]) {
            Ok(_) => panic!("null domain value was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "23502");
        let error = match session.execute(
            "INSERT INTO typed_arrays VALUES (2, ARRAY['ok', 'angry'], ARRAY[1])",
            &[],
        ) {
            Ok(_) => panic!("invalid enum array value was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "22P02");
        let error = match session.execute(
            "INSERT INTO typed_arrays VALUES (2, ARRAY['ok'], ARRAY[1, 0])",
            &[],
        ) {
            Ok(_) => panic!("invalid domain array value was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "23514");
        let error = match session.execute(
            "INSERT INTO typed_arrays VALUES (2, ARRAY['ok'], ARRAY[1, NULL])",
            &[],
        ) {
            Ok(_) => panic!("null domain array element was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "23502");
        let error = match session.execute("DROP TYPE mood", &[]) {
            Ok(_) => panic!("dependent enum type was dropped"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "2BP01");

        drop(session);
        drop(engine);
        let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen engine");
        let mut session = reopened.connect().expect("reconnect");
        let events = execute(
            &mut session,
            "SELECT current_mood, score FROM feelings",
            &[],
        );
        assert_eq!(
            rows(&events),
            vec![Row::new(vec![Value::Text("happy".into()), Value::Int32(1)])]
        );
        execute(&mut session, "DROP TYPE mood CASCADE", &[]);
        let events = execute(&mut session, "SELECT score FROM feelings", &[]);
        assert_eq!(rows(&events), vec![Row::new(vec![Value::Int32(1)])]);
        let events = execute(&mut session, "SELECT id, scores FROM typed_arrays", &[]);
        assert_eq!(rows(&events).len(), 1);
        let error = session
            .execute("SELECT current_mood FROM feelings", &[])
            .expect_err("cascade removed enum column");
        assert_eq!(error.sql_state, "42703");
    }

    #[test]
    fn describe_statement_infers_parameters_without_execution() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        create_documents(&mut session);

        let description = session
            .describe_statement(
                "SELECT id, title FROM documents \
                 WHERE score >= $1 ORDER BY id OFFSET $2 LIMIT $3",
            )
            .expect("describe statement");
        assert_eq!(
            description.parameter_types,
            [ScalarType::Int32, ScalarType::Int64, ScalarType::Int64]
        );
        assert_eq!(description.schema.fields.len(), 2);

        let fixed_point = session
            .describe_statement("SELECT $1 AS repeated, id FROM documents WHERE id = $1")
            .expect("describe cross-occurrence parameter");
        assert_eq!(fixed_point.parameter_types, [ScalarType::Int64]);
        assert_eq!(fixed_point.schema.fields[0].data_type, ScalarType::Int64);
        let executed = execute(
            &mut session,
            "SELECT $1 AS repeated, id FROM documents WHERE id = $1",
            &[Value::Int64(1)],
        );
        assert!(matches!(
            executed.first(),
            Some(QueryEvent::Schema(schema))
                if schema.fields[0].data_type == ScalarType::Int64
        ));

        let conflict = session
            .describe_statement("SELECT id FROM documents WHERE id = $1 OR score = $1")
            .expect_err("conflicting parameter types");
        assert_eq!(conflict.sql_state, "42804");
    }

    #[test]
    fn scalar_select_describe_and_execute_share_runtime_metadata() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        session.set_runtime_metadata(
            SessionRuntimeMetadata::new(
                "PostgreSQL 18 compatible OrdaDB test",
                "metadata_db",
                "alice",
                "bootstrap",
            )
            .expect("runtime metadata")
            .with_settings([
                ("client_encoding", "UTF8"),
                ("standard_conforming_strings", "on"),
            ])
            .expect("runtime settings"),
        );

        let description = session
            .describe_statement("SELECT current_database()")
            .expect("describe scalar select");
        assert!(description.parameter_types.is_empty());
        assert_eq!(description.schema.fields.len(), 1);
        assert_eq!(description.schema.fields[0].name, "current_database");
        assert_eq!(description.schema.fields[0].data_type, ScalarType::Text);
        assert!(!description.schema.fields[0].nullable);

        let settings_description = session
            .describe_statement(
                "SELECT current_setting('client_encoding'), \
                 current_setting('standard_conforming_strings')",
            )
            .expect("describe settings");
        assert_eq!(settings_description.schema.fields.len(), 2);
        let settings_events = execute(
            &mut session,
            "SELECT current_setting('client_encoding'), \
             current_setting('standard_conforming_strings')",
            &[],
        );
        assert_eq!(
            rows(&settings_events),
            vec![Row::new(vec![
                Value::Text("UTF8".into()),
                Value::Text("on".into()),
            ])]
        );

        for (sql, expected) in [
            (
                "SELECT version()",
                Value::Text("PostgreSQL 18 compatible OrdaDB test".into()),
            ),
            (
                "SELECT current_database()",
                Value::Text("metadata_db".into()),
            ),
            ("SELECT CURRENT_USER", Value::Text("alice".into())),
            ("SELECT SESSION_USER", Value::Text("bootstrap".into())),
            ("SELECT 1", Value::Int32(1)),
        ] {
            let events = execute(&mut session, sql, &[]);
            assert_eq!(rows(&events), vec![Row::new(vec![expected])], "{sql}");
            assert!(matches!(
                events.last(),
                Some(QueryEvent::Complete(CommandComplete { tag, rows_affected: 1 }))
                    if tag == "SELECT 1"
            ));
        }

        for invalid in [
            SessionRuntimeMetadata::new("", "db", "user", "user"),
            SessionRuntimeMetadata::new("version", "bad\0db", "user", "user"),
            SessionRuntimeMetadata::new(
                "version",
                "db",
                "x".repeat(MAX_SESSION_RUNTIME_TEXT_BYTES + 1),
                "user",
            ),
        ] {
            assert_eq!(invalid.expect_err("invalid metadata").sql_state, "22023");
        }
    }

    #[test]
    fn describe_statement_rejects_parameter_index_gaps() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        create_documents(&mut session);

        let error = session
            .describe_statement("SELECT id FROM documents WHERE id = $2")
            .expect_err("missing parameter type");
        assert_eq!(error.sql_state, "42P18");
    }

    #[test]
    fn executes_crud_with_parameters_ordering_and_limits() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        create_documents(&mut session);
        let events = execute(
            &mut session,
            "INSERT INTO documents (id, title, score) VALUES \
             ($1, 'first', 10), ($2, 'second', 20), ($3, 'third', 30) \
             RETURNING id, title",
            &[Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        );
        assert_eq!(rows(&events).len(), 3);
        assert!(matches!(
            events.last(),
            Some(QueryEvent::Complete(CommandComplete { tag, rows_affected: 3 }))
                if tag == "INSERT 0 3"
        ));

        let events = execute(
            &mut session,
            "SELECT id, title FROM documents WHERE score >= $1 ORDER BY id DESC LIMIT 2",
            &[Value::Int32(15)],
        );
        assert_eq!(
            rows(&events),
            vec![
                Row::new(vec![Value::Int64(3), Value::Text("third".into())]),
                Row::new(vec![Value::Int64(2), Value::Text("second".into())]),
            ]
        );

        let events = execute(
            &mut session,
            "SELECT id, title FROM documents ORDER BY id DESC OFFSET $1 LIMIT NULL",
            &[Value::Int64(1)],
        );
        assert_eq!(
            rows(&events),
            vec![
                Row::new(vec![Value::Int64(2), Value::Text("second".into())]),
                Row::new(vec![Value::Int64(1), Value::Text("first".into())]),
            ]
        );

        let events = execute(
            &mut session,
            "UPDATE documents SET title = 'updated' WHERE id = $1 RETURNING id, title AS name",
            &[Value::Int64(2)],
        );
        assert_eq!(
            rows(&events),
            vec![Row::new(vec![
                Value::Int64(2),
                Value::Text("updated".into()),
            ])]
        );
        let events = execute(
            &mut session,
            "DELETE FROM documents WHERE id = $1 RETURNING *",
            &[Value::Int64(1)],
        );
        assert_eq!(
            rows(&events),
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("first".into()),
                Value::Int32(10),
            ])]
        );
        let events = execute(
            &mut session,
            "SELECT id, title FROM documents ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&events),
            vec![
                Row::new(vec![Value::Int64(2), Value::Text("updated".into()),]),
                Row::new(vec![Value::Int64(3), Value::Text("third".into())]),
            ]
        );
    }

    #[test]
    fn executes_on_conflict_atomically_with_returning_and_cardinality_checks() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE conflict_items (
                id BIGINT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                label TEXT NOT NULL
            )",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO conflict_items VALUES (1, 'one@example.test', 'original')",
            &[],
        );

        let skipped = execute(
            &mut session,
            "INSERT INTO conflict_items VALUES (1, 'ignored@example.test', 'ignored') \
             ON CONFLICT DO NOTHING RETURNING id",
            &[],
        );
        assert!(rows(&skipped).is_empty());
        assert!(matches!(
            skipped.last(),
            Some(QueryEvent::Complete(CommandComplete {
                tag,
                rows_affected: 0
            })) if tag == "INSERT 0 0"
        ));

        let updated = execute(
            &mut session,
            "INSERT INTO conflict_items VALUES (1, 'updated@example.test', 'updated') \
             ON CONFLICT (id) DO UPDATE \
             SET email = excluded.email, label = excluded.label \
             WHERE conflict_items.label <> excluded.label \
             RETURNING id, email, label",
            &[],
        );
        assert_eq!(
            rows(&updated),
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("updated@example.test".into()),
                Value::Text("updated".into()),
            ])]
        );
        assert!(matches!(
            updated.last(),
            Some(QueryEvent::Complete(CommandComplete {
                tag,
                rows_affected: 1
            })) if tag == "INSERT 0 1"
        ));

        let filtered = execute(
            &mut session,
            "INSERT INTO conflict_items VALUES (1, 'different@example.test', 'updated') \
             ON CONFLICT (id) DO UPDATE SET email = excluded.email \
             WHERE conflict_items.label <> excluded.label RETURNING id",
            &[],
        );
        assert!(rows(&filtered).is_empty());
        assert!(matches!(
            filtered.last(),
            Some(QueryEvent::Complete(CommandComplete {
                tag,
                rows_affected: 0
            })) if tag == "INSERT 0 0"
        ));

        let non_arbiter = session
            .execute(
                "INSERT INTO conflict_items VALUES (2, 'updated@example.test', 'duplicate') \
                 ON CONFLICT (id) DO NOTHING",
                &[],
            )
            .expect_err("non-arbiter unique conflict");
        assert_eq!(non_arbiter.sql_state, "23505");

        let cardinality = session
            .execute(
                "INSERT INTO conflict_items VALUES \
                 (1, 'updated@example.test', 'first'), \
                 (1, 'updated@example.test', 'second') \
                 ON CONFLICT (id) DO UPDATE SET label = excluded.label",
                &[],
            )
            .expect_err("same target row affected twice");
        assert_eq!(cardinality.sql_state, "21000");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id, email, label FROM conflict_items",
                &[],
            )),
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("updated@example.test".into()),
                Value::Text("updated".into()),
            ])]
        );

        execute(&mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
        execute(&mut session, "SAVEPOINT before_upsert", &[]);
        execute(
            &mut session,
            "INSERT INTO conflict_items VALUES (1, 'updated@example.test', 'temporary') \
             ON CONFLICT (id) DO UPDATE SET label = excluded.label",
            &[],
        );
        execute(&mut session, "ROLLBACK TO SAVEPOINT before_upsert", &[]);
        execute(&mut session, "COMMIT", &[]);
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT label FROM conflict_items WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("updated".into())])]
        );
    }

    #[test]
    fn recursive_cte_row_limit_accepts_the_exact_boundary() {
        ensure_recursive_cte_row_limit(MAX_RECURSIVE_CTE_ROWS).expect("exact row limit");
        let error = ensure_recursive_cte_row_limit(MAX_RECURSIVE_CTE_ROWS + 1)
            .expect_err("row above recursive CTE limit");
        assert_eq!(error.sql_state, "54000");
    }

    #[test]
    fn executes_postgres_set_operations_with_duplicates_nulls_and_limits() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(&mut session, "CREATE TABLE set_left (value BIGINT)", &[]);
        execute(&mut session, "CREATE TABLE set_right (value INTEGER)", &[]);
        execute(
            &mut session,
            "INSERT INTO set_left VALUES (1), (1), (2), (3), (NULL)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO set_right VALUES (1), (2), (2), (4), (NULL)",
            &[],
        );

        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT value AS item FROM set_left
                 UNION SELECT value FROM set_right
                 ORDER BY item NULLS FIRST",
                &[],
            )),
            vec![
                Row::new(vec![Value::Null]),
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)]),
                Row::new(vec![Value::Int64(4)]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT value AS item FROM set_left
                 UNION ALL SELECT value FROM set_right
                 ORDER BY item NULLS FIRST OFFSET $1 LIMIT $2",
                &[Value::Int64(2), Value::Int64(4)],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT value FROM set_left
                 INTERSECT SELECT value FROM set_right
                 ORDER BY value NULLS FIRST",
                &[],
            )),
            vec![
                Row::new(vec![Value::Null]),
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT value FROM set_left
                 EXCEPT ALL SELECT value FROM set_right
                 ORDER BY value",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );

        assert_eq!(
            rows(&execute(
                &mut session,
                "WITH combined(item) AS (
                     SELECT value FROM set_left
                     UNION ALL SELECT value FROM set_right
                 ), filtered AS (
                     SELECT item FROM combined WHERE item >= 2
                 )
                 SELECT item FROM filtered ORDER BY item OFFSET 1 LIMIT 3",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "WITH RECURSIVE numbers(value) AS (
                     SELECT value FROM set_left WHERE value = 3
                     UNION ALL
                     SELECT value - 1 FROM numbers WHERE value > 1
                 )
                 SELECT value FROM numbers ORDER BY value",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "WITH RECURSIVE stable(value) AS (
                     SELECT value FROM set_left WHERE value = 3
                     UNION
                     SELECT value FROM stable
                 )
                 SELECT value FROM stable",
                &[],
            )),
            vec![Row::new(vec![Value::Int64(3)])]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT value + 2 * 3 FROM set_left WHERE value = 1 LIMIT 1",
                &[],
            )),
            vec![Row::new(vec![Value::Int64(7)])]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT value * 2 AS doubled FROM set_left
                 WHERE value <= 3 ORDER BY doubled DESC",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(6)]),
                Row::new(vec![Value::Int64(4)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT value FROM set_left
                 WHERE value <= 3 ORDER BY value + 1 DESC, 1 ASC",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(3)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(1)]),
            ]
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT value, COUNT(*) AS total FROM set_left
                 GROUP BY value ORDER BY total DESC, 1 ASC",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(2)]),
                Row::new(vec![Value::Int64(2), Value::Int64(1)]),
                Row::new(vec![Value::Int64(3), Value::Int64(1)]),
                Row::new(vec![Value::Null, Value::Int64(1)]),
            ]
        );
        let division = session
            .execute("SELECT value / 0 FROM set_left LIMIT 1", &[])
            .expect_err("division by zero");
        assert_eq!(division.sql_state, "22012");
        execute(
            &mut session,
            "INSERT INTO set_left VALUES (9223372036854775807)",
            &[],
        );
        let overflow = session
            .execute(
                "SELECT value + 1 FROM set_left WHERE value = 9223372036854775807",
                &[],
            )
            .expect_err("integer overflow");
        assert_eq!(overflow.sql_state, "22003");
    }

    #[test]
    fn executes_merge_as_one_atomic_ordered_candidate() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE merge_target (
                id BIGINT PRIMARY KEY,
                value TEXT UNIQUE NOT NULL
            )",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE merge_source (
                id BIGINT NOT NULL,
                value TEXT NOT NULL,
                action TEXT NOT NULL
            )",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO merge_target VALUES
                (1, 'old-one'), (2, 'old-two'), (4, 'stable')",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO merge_source VALUES
                (1, 'new-one', 'update'),
                (2, 'ignored', 'delete'),
                (3, 'new-three', 'insert'),
                (4, 'ignored', 'skip')",
            &[],
        );

        let events = execute(
            &mut session,
            "MERGE INTO merge_target AS target
             USING merge_source AS source ON target.id = source.id
             WHEN MATCHED AND source.action = 'delete' THEN DELETE
             WHEN MATCHED AND source.action = 'update' THEN
                 UPDATE SET value = source.value
             WHEN NOT MATCHED THEN INSERT (id, value)
                 VALUES (source.id, source.value)
             RETURNING id, value",
            &[],
        );
        assert_eq!(
            rows(&events),
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("new-one".into())]),
                Row::new(vec![Value::Int64(2), Value::Text("old-two".into())]),
                Row::new(vec![Value::Int64(3), Value::Text("new-three".into())]),
            ]
        );
        assert!(matches!(
            events.last(),
            Some(QueryEvent::Complete(CommandComplete {
                tag,
                rows_affected: 3
            })) if tag == "MERGE 3"
        ));
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id, value FROM merge_target ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("new-one".into())]),
                Row::new(vec![Value::Int64(3), Value::Text("new-three".into())]),
                Row::new(vec![Value::Int64(4), Value::Text("stable".into())]),
            ]
        );

        let do_nothing = execute(
            &mut session,
            "MERGE INTO merge_target AS target
             USING merge_source AS source ON target.id = source.id
             WHEN MATCHED THEN DO NOTHING
             WHEN NOT MATCHED THEN DO NOTHING
             RETURNING id, value",
            &[],
        );
        assert!(rows(&do_nothing).is_empty());
        assert!(matches!(
            do_nothing.last(),
            Some(QueryEvent::Complete(CommandComplete {
                tag,
                rows_affected: 0
            })) if tag == "MERGE 0"
        ));
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id, value FROM merge_target ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("new-one".into())]),
                Row::new(vec![Value::Int64(3), Value::Text("new-three".into())]),
                Row::new(vec![Value::Int64(4), Value::Text("stable".into())]),
            ]
        );

        execute(&mut session, "DELETE FROM merge_source", &[]);
        execute(
            &mut session,
            "INSERT INTO merge_source VALUES
                (1, 'first', 'update'), (1, 'second', 'update')",
            &[],
        );
        let cardinality = session
            .execute(
                "MERGE INTO merge_target AS target
                 USING merge_source AS source ON target.id = source.id
                 WHEN MATCHED THEN UPDATE SET value = source.value",
                &[],
            )
            .expect_err("same target affected twice");
        assert_eq!(cardinality.sql_state, "21000");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT value FROM merge_target WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("new-one".into())])]
        );

        execute(&mut session, "DELETE FROM merge_source", &[]);
        execute(
            &mut session,
            "INSERT INTO merge_source VALUES
                (1, 'temporary', 'update'), (5, 'stable', 'insert')",
            &[],
        );
        let uniqueness = session
            .execute(
                "MERGE INTO merge_target AS target
                 USING merge_source AS source ON target.id = source.id
                 WHEN MATCHED THEN UPDATE SET value = source.value
                 WHEN NOT MATCHED THEN INSERT (id, value)
                     VALUES (source.id, source.value)",
                &[],
            )
            .expect_err("atomic unique failure");
        assert_eq!(uniqueness.sql_state, "23505");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id, value FROM merge_target ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("new-one".into())]),
                Row::new(vec![Value::Int64(3), Value::Text("new-three".into())]),
                Row::new(vec![Value::Int64(4), Value::Text("stable".into())]),
            ]
        );

        execute(&mut session, "DELETE FROM merge_source", &[]);
        execute(
            &mut session,
            "INSERT INTO merge_source VALUES (4, 'savepoint', 'update')",
            &[],
        );
        execute(&mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
        execute(&mut session, "SAVEPOINT before_merge", &[]);
        execute(
            &mut session,
            "MERGE INTO merge_target AS target
             USING merge_source AS source ON target.id = source.id
             WHEN MATCHED THEN UPDATE SET value = source.value",
            &[],
        );
        execute(&mut session, "ROLLBACK TO SAVEPOINT before_merge", &[]);
        execute(&mut session, "COMMIT", &[]);
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT value FROM merge_target WHERE id = 4",
                &[],
            )),
            vec![Row::new(vec![Value::Text("stable".into())])]
        );

        execute(&mut session, "DELETE FROM merge_source", &[]);
        execute(
            &mut session,
            "INSERT INTO merge_source VALUES (1, 'ignored', 'skip')",
            &[],
        );
        let by_source = execute(
            &mut session,
            "MERGE INTO merge_target AS target
             USING merge_source AS source ON target.id = source.id
             WHEN MATCHED THEN DO NOTHING
             WHEN NOT MATCHED BY SOURCE AND target.id = 3 THEN DELETE
             WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 'orphan'
             RETURNING id, value",
            &[],
        );
        let returned = rows(&by_source);
        assert_eq!(returned.len(), 2);
        assert!(returned.contains(&Row::new(vec![
            Value::Int64(3),
            Value::Text("new-three".into()),
        ])));
        assert!(returned.contains(&Row::new(vec![
            Value::Int64(4),
            Value::Text("orphan".into()),
        ])));
        assert!(matches!(
            by_source.last(),
            Some(QueryEvent::Complete(CommandComplete {
                tag,
                rows_affected: 2
            })) if tag == "MERGE 2"
        ));
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id, value FROM merge_target ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("new-one".into())]),
                Row::new(vec![Value::Int64(4), Value::Text("orphan".into())]),
            ]
        );
    }

    #[test]
    fn read_committed_on_conflict_rechecks_after_unique_key_wait() {
        let (_directory, engine) = engine();
        engine
            .set_default_lock_timeout(Duration::from_secs(2))
            .expect("configure lock timeout");
        let mut first = engine.connect().expect("first session");
        let mut second = engine.connect().expect("second session");
        execute(
            &mut first,
            "CREATE TABLE concurrent_upserts (id BIGINT PRIMARY KEY, label TEXT NOT NULL)",
            &[],
        );
        execute(&mut first, "BEGIN", &[]);
        execute(&mut second, "BEGIN", &[]);
        execute(
            &mut first,
            "INSERT INTO concurrent_upserts VALUES (1, 'first')",
            &[],
        );

        let worker = std::thread::spawn(move || -> Result<Vec<QueryEvent>> {
            let events = second
                .execute(
                    "INSERT INTO concurrent_upserts VALUES (1, 'second') \
                     ON CONFLICT (id) DO UPDATE SET label = excluded.label \
                     RETURNING label",
                    &[],
                )?
                .collect::<Vec<_>>();
            second.execute("COMMIT", &[])?.for_each(drop);
            Ok(events)
        });
        let mut waiting_observed = false;
        for _ in 0..100 {
            if !engine.lock_snapshot().expect("lock snapshot").1.is_empty() {
                waiting_observed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(waiting_observed, "UPSERT did not wait for the unique key");
        execute(&mut first, "COMMIT", &[]);

        let events = worker
            .join()
            .expect("UPSERT worker")
            .expect("UPSERT result");
        assert_eq!(
            rows(&events),
            vec![Row::new(vec![Value::Text("second".into())])]
        );
        assert_eq!(
            rows(&execute(
                &mut first,
                "SELECT label FROM concurrent_upserts WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("second".into())])]
        );
    }

    #[test]
    fn repeatable_read_on_conflict_reports_serialization_after_unique_key_wait() {
        let (_directory, engine) = engine();
        engine
            .set_default_lock_timeout(Duration::from_secs(2))
            .expect("configure lock timeout");
        let mut first = engine.connect().expect("first session");
        let mut second = engine.connect().expect("second session");
        execute(
            &mut first,
            "CREATE TABLE repeatable_upserts (id BIGINT PRIMARY KEY, label TEXT NOT NULL)",
            &[],
        );
        execute(&mut first, "BEGIN", &[]);
        execute(&mut second, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(
            &mut first,
            "INSERT INTO repeatable_upserts VALUES (1, 'first')",
            &[],
        );

        let worker = std::thread::spawn(move || -> Result<String> {
            let error = second
                .execute(
                    "INSERT INTO repeatable_upserts VALUES (1, 'second') \
                     ON CONFLICT (id) DO UPDATE SET label = excluded.label",
                    &[],
                )
                .expect_err("stale Repeatable Read UPSERT");
            second.execute("ROLLBACK", &[])?.for_each(drop);
            Ok(error.sql_state.to_string())
        });
        let mut waiting_observed = false;
        for _ in 0..100 {
            if !engine.lock_snapshot().expect("lock snapshot").1.is_empty() {
                waiting_observed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(waiting_observed, "UPSERT did not wait for the unique key");
        execute(&mut first, "COMMIT", &[]);

        assert_eq!(
            worker
                .join()
                .expect("UPSERT worker")
                .expect("UPSERT result"),
            "40001"
        );
        assert_eq!(
            rows(&execute(
                &mut first,
                "SELECT label FROM repeatable_upserts WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("first".into())])]
        );
    }

    #[test]
    fn returning_stream_batches_rows_and_retains_its_memory_peak() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE returning_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        let values = (0..=DEFAULT_BATCH_ROWS)
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut stream = session
            .execute_stream(
                &format!("INSERT INTO returning_items VALUES {values} RETURNING id"),
                &[],
            )
            .expect("returning stream");
        assert!(
            stream
                .execution_memory_peak_bytes()
                .is_some_and(|peak| peak > 0)
        );

        let events = stream
            .by_ref()
            .collect::<Result<Vec<_>>>()
            .expect("stream events");
        let batch_lengths = events
            .iter()
            .filter_map(|event| match event {
                QueryEvent::Batch(batch) => Some(batch.rows.len()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(batch_lengths, [DEFAULT_BATCH_ROWS, 1]);
        assert_eq!(rows(&events).len(), DEFAULT_BATCH_ROWS + 1);
        assert!(
            stream
                .execution_memory_peak_bytes()
                .is_some_and(|peak| peak > 0)
        );
        assert!(matches!(
            events.last(),
            Some(QueryEvent::Complete(CommandComplete {
                tag,
                rows_affected
            })) if tag == "INSERT 0 1025" && *rows_affected == 1_025
        ));
    }

    #[test]
    fn committed_versions_reopen_with_stable_predecessors_and_visible_storage_scan() {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
            let mut session = engine.connect().expect("connect");
            create_documents(&mut session);
            execute(
                &mut session,
                "INSERT INTO documents VALUES (1, 'original', 10), (2, 'deleted', 20)",
                &[],
            );
            execute(
                &mut session,
                "UPDATE documents SET title = 'updated' WHERE id = 1",
                &[],
            );
            execute(&mut session, "DELETE FROM documents WHERE id = 2", &[]);

            let state = engine.state.read().expect("state");
            let table_id = state
                .catalog
                .table(
                    &Identifier::unquoted("public"),
                    &Identifier::unquoted("documents"),
                )
                .expect("documents")
                .id;
            let versions = state.versions.get(&table_id).expect("versions");
            let visible = state
                .visible_versions
                .get(&table_id)
                .expect("visible versions");
            assert_eq!(versions.len(), 3);
            assert_eq!(visible.as_slice(), &[3]);
            assert_eq!(versions[0].version_id, 1);
            assert_eq!(versions[0].header.previous_version, 0);
            assert_ne!(versions[0].header.xmax, 0);
            assert_eq!(versions[1].version_id, 2);
            assert_eq!(versions[1].header.previous_version, 0);
            assert_ne!(versions[1].header.xmax, 0);
            assert_eq!(versions[2].version_id, 3);
            assert_eq!(versions[2].header.previous_version, 1);
            assert_eq!(versions[2].header.xmin, versions[0].header.xmax);
            assert_eq!(versions[2].header.xmax, 0);
            assert_eq!(
                engine
                    .transaction_status
                    .transaction_outcome(
                        TransactionId::new(versions[2].header.xmin).expect("creator transaction")
                    )
                    .expect("creator outcome"),
                ordadb_transaction::TransactionOutcome::Committed
            );
        }

        let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
        let mut session = engine.connect().expect("connect");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id, title FROM documents ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("updated".into())
            ])]
        );
        let state = engine.state.read().expect("state");
        let table_id = state
            .catalog
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        assert_eq!(state.versions.get(&table_id).expect("versions").len(), 3);
        assert_eq!(
            state
                .visible_versions
                .get(&table_id)
                .expect("visible versions")
                .as_slice(),
            &[3]
        );
    }

    #[test]
    fn aborted_update_keeps_the_original_version_visible_after_reopen() {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
            let mut session = engine.connect().expect("connect");
            create_documents(&mut session);
            execute(
                &mut session,
                "INSERT INTO documents VALUES (1, 'original', 10)",
                &[],
            );
            let mut transaction = session.begin().expect("begin");
            transaction
                .execute("UPDATE documents SET title = 'aborted' WHERE id = 1", &[])
                .expect("update");
            transaction.rollback().expect("rollback");
        }

        let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
        let mut session = engine.connect().expect("connect");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id, title FROM documents",
                &[],
            )),
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Text("original".into())
            ])]
        );
        let state = engine.state.read().expect("state");
        let table_id = state
            .catalog
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        let versions = state.versions.get(&table_id).expect("versions");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].header.xmax, 0);
        assert_eq!(
            state
                .visible_versions
                .get(&table_id)
                .expect("visible versions")
                .as_slice(),
            &[1]
        );
    }

    #[test]
    fn compares_jsonb_parameters_by_equality_without_requiring_ordering() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE payloads (id BIGINT PRIMARY KEY, body JSONB NOT NULL)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO payloads VALUES (1, $1), (2, $2)",
            &[
                Value::Jsonb(serde_json::json!({"kind": "match"})),
                Value::Jsonb(serde_json::json!({"kind": "other"})),
            ],
        );

        let equal = execute(
            &mut session,
            "SELECT id FROM payloads WHERE body = $1 ORDER BY id",
            &[Value::Jsonb(serde_json::json!({"kind": "match"}))],
        );
        assert_eq!(rows(&equal), vec![Row::new(vec![Value::Int64(1)])]);

        let not_equal = execute(
            &mut session,
            "SELECT id FROM payloads WHERE body <> $1 ORDER BY id",
            &[Value::Jsonb(serde_json::json!({"kind": "match"}))],
        );
        assert_eq!(rows(&not_equal), vec![Row::new(vec![Value::Int64(2)])]);
    }

    #[test]
    fn enforces_not_null_primary_key_and_unique_constraints_atomically() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE NOT NULL)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO users VALUES (1, 'a@example.test')",
            &[],
        );

        let error = session
            .execute(
                "INSERT INTO users VALUES (2, 'b@example.test'), (1, 'c@example.test')",
                &[],
            )
            .expect_err("duplicate primary key");
        assert_eq!(error.sql_state, "23505");
        let events = execute(&mut session, "SELECT * FROM users", &[]);
        assert_eq!(rows(&events).len(), 1);

        let error = session
            .execute("INSERT INTO users VALUES (2, NULL)", &[])
            .expect_err("not null");
        assert_eq!(error.sql_state, "23502");
    }

    #[test]
    fn commits_rolls_back_and_allows_disjoint_writers() {
        let (_directory, engine) = engine();
        let mut first = engine.connect().expect("first");
        let mut second = engine.connect().expect("second");
        create_documents(&mut first);

        {
            let mut transaction = first.begin().expect("begin");
            transaction
                .execute("INSERT INTO documents VALUES (1, 'rolled back', 1)", &[])
                .expect("insert");
            transaction.rollback().expect("rollback");
        }
        assert!(rows(&execute(&mut first, "SELECT * FROM documents", &[])).is_empty());

        {
            let mut transaction = first.begin().expect("begin");
            transaction
                .execute("INSERT INTO documents VALUES (1, 'committed', 1)", &[])
                .expect("insert");
            transaction.commit().expect("commit");
        }
        assert_eq!(
            rows(&execute(&mut first, "SELECT * FROM documents", &[])).len(),
            1
        );

        let mut transaction = first.begin().expect("begin writer");
        transaction
            .execute("INSERT INTO documents VALUES (2, 'rolled back', 2)", &[])
            .expect("transaction insert");
        execute(
            &mut second,
            "INSERT INTO documents VALUES (3, 'concurrent', 3)",
            &[],
        );
        transaction.rollback().expect("rollback writer");
        assert_eq!(
            rows(&execute(
                &mut second,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
    }

    #[test]
    fn dml_locks_are_scoped_and_released_on_transaction_rollback() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        let mut transaction = session.begin().expect("begin");
        transaction
            .execute("INSERT INTO documents VALUES (1, 'locked', 1)", &[])
            .expect("insert");

        let (granted, waiting) = engine.locks.snapshot().expect("lock snapshot");
        assert!(waiting.is_empty());
        assert!(
            granted
                .iter()
                .any(|lock| { lock.key == LockKey::Database && lock.mode == LockMode::Shared })
        );
        assert!(granted.iter().any(|lock| {
            matches!(lock.key, LockKey::Table { .. }) && lock.mode == LockMode::Shared
        }));
        assert!(granted.iter().any(|lock| {
            matches!(lock.key, LockKey::IndexKey { .. }) && lock.mode == LockMode::Exclusive
        }));

        transaction.rollback().expect("rollback");
        let (granted, waiting) = engine.locks.snapshot().expect("released snapshot");
        assert!(granted.is_empty());
        assert!(waiting.is_empty());
    }

    #[test]
    fn concurrent_disjoint_writers_merge_without_lost_updates() {
        let (_directory, engine) = engine();
        let mut first = engine.connect().expect("first");
        let mut second = engine.connect().expect("second");
        create_documents(&mut first);

        let mut first_transaction = first.begin().expect("first begin");
        let mut second_transaction = second.begin().expect("second begin");
        first_transaction
            .execute("INSERT INTO documents VALUES (1, 'first', 1)", &[])
            .expect("first insert");
        second_transaction
            .execute("INSERT INTO documents VALUES (2, 'second', 2)", &[])
            .expect("second insert");
        first_transaction.commit().expect("first commit");
        second_transaction.commit().expect("second commit");

        assert_eq!(
            rows(&execute(
                &mut first,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
    }

    #[test]
    fn row_and_unique_conflicts_timeout_then_report_committed_duplicates() {
        let (_directory, engine) = engine();
        engine
            .set_default_lock_timeout(Duration::from_millis(20))
            .expect("configure lock timeout");
        let mut first = engine.connect().expect("first");
        let mut second = engine.connect().expect("second");
        create_documents(&mut first);
        execute(
            &mut first,
            "INSERT INTO documents VALUES (1, 'base', 1)",
            &[],
        );

        execute(&mut first, "BEGIN", &[]);
        execute(&mut second, "BEGIN", &[]);
        execute(
            &mut first,
            "UPDATE documents SET title = 'first' WHERE id = 1",
            &[],
        );
        assert_eq!(
            second
                .execute("UPDATE documents SET title = 'second' WHERE id = 1", &[],)
                .expect_err("row lock timeout")
                .sql_state,
            "55P03"
        );
        execute(&mut second, "ROLLBACK", &[]);
        execute(&mut first, "COMMIT", &[]);

        execute(&mut first, "BEGIN", &[]);
        execute(&mut second, "BEGIN", &[]);
        execute(
            &mut first,
            "INSERT INTO documents VALUES (2, 'first unique', 2)",
            &[],
        );
        assert_eq!(
            second
                .execute("INSERT INTO documents VALUES (2, 'second unique', 3)", &[],)
                .expect_err("unique key lock timeout")
                .sql_state,
            "55P03"
        );
        execute(&mut second, "ROLLBACK", &[]);
        execute(&mut first, "COMMIT", &[]);
        assert_eq!(
            second
                .execute(
                    "INSERT INTO documents VALUES (2, 'committed duplicate', 4)",
                    &[],
                )
                .expect_err("committed duplicate")
                .sql_state,
            "23505"
        );
    }

    #[test]
    fn engine_deadlock_aborts_the_youngest_transaction_and_releases_waiters() {
        let (_directory, engine) = engine();
        engine
            .set_default_lock_timeout(Duration::from_secs(1))
            .expect("configure lock timeout");
        let mut first = engine.connect().expect("first");
        let mut second = engine.connect().expect("second");
        create_documents(&mut first);
        execute(
            &mut first,
            "INSERT INTO documents VALUES (1, 'one', 1), (2, 'two', 2)",
            &[],
        );
        execute(&mut first, "BEGIN", &[]);
        execute(&mut second, "BEGIN", &[]);
        execute(
            &mut first,
            "UPDATE documents SET title = 'first' WHERE id = 1",
            &[],
        );
        execute(
            &mut second,
            "UPDATE documents SET title = 'second' WHERE id = 2",
            &[],
        );

        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = first
                .execute("UPDATE documents SET title = 'first' WHERE id = 2", &[])
                .map(|_| ());
            if result.is_ok() {
                execute(&mut first, "COMMIT", &[]);
            }
            send.send(result).expect("send first result");
        });
        let mut waiting_observed = false;
        for _ in 0..100 {
            if !engine.lock_snapshot().expect("lock snapshot").1.is_empty() {
                waiting_observed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            waiting_observed,
            "first transaction did not enter lock wait"
        );
        assert_eq!(
            second
                .execute("UPDATE documents SET title = 'second' WHERE id = 1", &[],)
                .expect_err("deadlock victim")
                .sql_state,
            "40P01"
        );
        execute(&mut second, "ROLLBACK", &[]);
        receive
            .recv_timeout(Duration::from_secs(1))
            .expect("first result")
            .expect("surviving transaction");
        worker.join().expect("first transaction join");
        assert!(
            engine
                .lock_snapshot()
                .expect("released lock snapshot")
                .0
                .is_empty()
        );
    }

    #[test]
    fn read_committed_refreshes_and_repeatable_read_retains_visibility() {
        let (_directory, engine) = engine();
        let mut reader = engine.connect().expect("reader");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut reader);
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (1, 'v1', 1)",
            &[],
        );

        execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v1".to_owned())])]
        );
        execute(
            &mut writer,
            "UPDATE documents SET title = 'v2' WHERE id = 1",
            &[],
        );
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v1".to_owned())])]
        );
        execute(&mut reader, "COMMIT", &[]);

        execute(&mut reader, "BEGIN ISOLATION LEVEL READ COMMITTED", &[]);
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v2".to_owned())])]
        );
        execute(
            &mut writer,
            "UPDATE documents SET title = 'v3' WHERE id = 1",
            &[],
        );
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v3".to_owned())])]
        );
        execute(&mut reader, "COMMIT", &[]);
    }

    #[test]
    fn read_committed_rebases_private_writes_over_disjoint_commits() {
        let (_directory, engine) = engine();
        let mut reader = engine.connect().expect("reader");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut reader);

        execute(&mut reader, "BEGIN ISOLATION LEVEL READ COMMITTED", &[]);
        execute(
            &mut reader,
            "INSERT INTO documents VALUES (1, 'private', 1)",
            &[],
        );
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (2, 'concurrent', 2)",
            &[],
        );
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        execute(
            &mut reader,
            "INSERT INTO documents VALUES (3, 'after-refresh', 3)",
            &[],
        );
        execute(&mut reader, "COMMIT", &[]);

        assert_eq!(
            rows(&execute(
                &mut writer,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
    }

    #[test]
    fn programmatic_read_committed_rebases_private_writes() {
        let (_directory, engine) = engine();
        let mut reader = engine.connect().expect("reader");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut reader);

        let mut transaction = reader.begin().expect("begin");
        transaction
            .execute("INSERT INTO documents VALUES (1, 'private', 1)", &[])
            .expect("private insert");
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (2, 'concurrent', 2)",
            &[],
        );
        let selected = transaction
            .execute("SELECT id FROM documents ORDER BY id", &[])
            .expect("refreshed select")
            .collect::<Vec<_>>();
        assert_eq!(
            rows(&selected),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        transaction.commit().expect("commit");
    }

    #[test]
    fn sql_transaction_upgrades_staged_dml_before_ddl() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut session);

        execute(&mut session, "BEGIN ISOLATION LEVEL READ COMMITTED", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'private', 1)",
            &[],
        );
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (2, 'concurrent', 2)",
            &[],
        );
        execute(
            &mut session,
            "CREATE INDEX documents_score_idx ON documents (score)",
            &[],
        );
        execute(&mut session, "COMMIT", &[]);

        assert_eq!(
            rows(&execute(
                &mut writer,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        execute(&mut writer, "DROP INDEX documents_score_idx", &[]);
    }

    #[test]
    fn programmatic_transaction_upgrades_staged_dml_before_ddl() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);

        let mut transaction = session.begin().expect("begin");
        transaction
            .execute("INSERT INTO documents VALUES (1, 'private', 1)", &[])
            .expect("insert");
        transaction
            .execute("CREATE INDEX documents_score_idx ON documents (score)", &[])
            .expect("create index after DML");
        transaction.commit().expect("commit");

        execute(&mut session, "DROP INDEX documents_score_idx", &[]);
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![Value::Int64(1)])]
        );
    }

    #[test]
    fn sql_savepoint_restores_candidate_and_recovers_failed_transaction() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(&mut session, "BEGIN", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'before', 1)",
            &[],
        );
        execute(&mut session, "SAVEPOINT keep_before", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (2, 'after', 2)",
            &[],
        );
        let duplicate = session
            .execute("INSERT INTO documents VALUES (1, 'duplicate', 3)", &[])
            .expect_err("duplicate");
        assert_eq!(duplicate.sql_state, "23505");
        assert_eq!(session.transaction_status(), TransactionStatus::Failed);
        assert_eq!(
            session
                .execute("SELECT id FROM documents", &[])
                .expect_err("failed transaction")
                .sql_state,
            "25P02"
        );

        execute(&mut session, "ROLLBACK TO SAVEPOINT keep_before", &[]);
        assert_eq!(session.transaction_status(), TransactionStatus::Active);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (3, 'recovered', 3)",
            &[],
        );
        execute(&mut session, "COMMIT", &[]);
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
    }

    #[test]
    fn sql_savepoint_rollback_restores_ssi_predicates() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(&mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
        execute(&mut session, "SAVEPOINT before_read", &[]);
        execute(&mut session, "SELECT id FROM documents", &[]);

        let before = engine.ssi.snapshot().expect("SSI before rollback");
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].read_predicates, 1);
        execute(&mut session, "ROLLBACK TO SAVEPOINT before_read", &[]);

        let after = engine.ssi.snapshot().expect("SSI after rollback");
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].read_predicates, 0);
        execute(&mut session, "ROLLBACK", &[]);
    }

    #[test]
    fn serializable_ssi_rejects_write_skew() {
        let (_directory, engine) = engine();
        let mut first = engine.connect().expect("first session");
        let mut second = engine.connect().expect("second session");
        execute(
            &mut first,
            "CREATE TABLE doctors (id INT PRIMARY KEY, on_call BOOLEAN NOT NULL)",
            &[],
        );
        execute(
            &mut first,
            "INSERT INTO doctors VALUES (1, TRUE), (2, TRUE)",
            &[],
        );
        execute(&mut first, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
        execute(&mut second, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
        assert_eq!(
            rows(&execute(
                &mut first,
                "SELECT id FROM doctors WHERE on_call = TRUE ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int32(1)]),
                Row::new(vec![Value::Int32(2)]),
            ]
        );
        execute(
            &mut second,
            "SELECT id FROM doctors WHERE on_call = TRUE ORDER BY id",
            &[],
        );
        execute(
            &mut first,
            "UPDATE doctors SET on_call = FALSE WHERE id = 1",
            &[],
        );
        execute(&mut first, "COMMIT", &[]);
        execute(
            &mut second,
            "UPDATE doctors SET on_call = FALSE WHERE id = 2",
            &[],
        );
        let error = second.execute("COMMIT", &[]).expect_err("write skew");
        assert_eq!(error.sql_state, "40001");

        let mut verifier = engine.connect().expect("verifier");
        assert_eq!(
            rows(&execute(
                &mut verifier,
                "SELECT id FROM doctors WHERE on_call = TRUE ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![Value::Int32(2)])]
        );
    }

    #[test]
    fn isolation_snapshots_prevent_dirty_reads_and_repeatable_read_phantoms() {
        let (_directory, engine) = engine();
        let mut writer = engine.connect().expect("writer");
        let mut reader = engine.connect().expect("reader");
        create_documents(&mut writer);

        execute(&mut writer, "BEGIN", &[]);
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (1, 'uncommitted', 1)",
            &[],
        );
        assert!(rows(&execute(&mut reader, "SELECT id FROM documents", &[])).is_empty());
        execute(&mut writer, "COMMIT", &[]);

        execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![Value::Int64(1)])]
        );
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (2, 'phantom', 2)",
            &[],
        );
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![Value::Int64(1)])]
        );
        execute(&mut reader, "COMMIT", &[]);
        assert_eq!(
            rows(&execute(
                &mut reader,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
    }

    #[test]
    fn repeatable_read_rejects_stale_update_and_delete_targets() {
        let (_directory, engine) = engine();
        let mut stale = engine.connect().expect("stale session");
        let mut concurrent = engine.connect().expect("concurrent session");
        create_documents(&mut stale);
        execute(
            &mut stale,
            "INSERT INTO documents VALUES (1, 'first', 1), (2, 'second', 2)",
            &[],
        );

        execute(&mut stale, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(&mut stale, "SELECT title FROM documents WHERE id = 1", &[]);
        execute(
            &mut concurrent,
            "UPDATE documents SET title = 'concurrent' WHERE id = 1",
            &[],
        );
        assert_eq!(
            stale
                .execute("UPDATE documents SET title = 'stale' WHERE id = 1", &[],)
                .expect_err("stale update conflict")
                .sql_state,
            "40001"
        );
        execute(&mut stale, "ROLLBACK", &[]);

        execute(&mut stale, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(&mut stale, "SELECT title FROM documents WHERE id = 2", &[]);
        execute(
            &mut concurrent,
            "UPDATE documents SET title = 'changed' WHERE id = 2",
            &[],
        );
        assert_eq!(
            stale
                .execute("DELETE FROM documents WHERE id = 2", &[])
                .expect_err("stale delete conflict")
                .sql_state,
            "40001"
        );
        execute(&mut stale, "ROLLBACK", &[]);
    }

    #[test]
    fn repeatable_read_writers_merge_unrelated_row_changes() {
        let (_directory, engine) = engine();
        let mut first = engine.connect().expect("first session");
        let mut second = engine.connect().expect("second session");
        create_documents(&mut first);
        execute(
            &mut first,
            "INSERT INTO documents VALUES (1, 'first', 1), (2, 'second', 2)",
            &[],
        );
        execute(&mut first, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(&mut second, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(
            &mut first,
            "UPDATE documents SET title = 'first-committed' WHERE id = 1",
            &[],
        );
        execute(
            &mut second,
            "UPDATE documents SET title = 'second-committed' WHERE id = 2",
            &[],
        );
        execute(&mut first, "COMMIT", &[]);
        execute(&mut second, "COMMIT", &[]);

        let mut verifier = engine.connect().expect("verifier");
        assert_eq!(
            rows(&execute(
                &mut verifier,
                "SELECT title FROM documents ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Text("first-committed".to_owned())]),
                Row::new(vec![Value::Text("second-committed".to_owned())]),
            ]
        );
    }

    #[test]
    fn vacuum_protects_active_snapshots_then_reclaims_and_analyzes() {
        let (_directory, engine) = engine();
        let mut reader = engine.connect().expect("reader");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut writer);
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (1, 'v1', 1)",
            &[],
        );
        execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(&mut reader, "SELECT title FROM documents WHERE id = 1", &[]);
        execute(
            &mut writer,
            "UPDATE documents SET title = 'v2' WHERE id = 1",
            &[],
        );
        let table_id = {
            let state = engine.state.read().expect("state");
            state
                .catalog
                .table(
                    &Identifier::unquoted("public"),
                    &Identifier::unquoted("documents"),
                )
                .expect("documents")
                .id
        };
        execute(&mut writer, "VACUUM documents", &[]);
        assert_eq!(
            engine
                .state
                .read()
                .expect("protected state")
                .versions
                .get(&table_id)
                .expect("versions")
                .len(),
            2
        );

        execute(&mut reader, "ROLLBACK", &[]);
        execute(&mut writer, "VACUUM ANALYZE documents", &[]);
        let state = engine.state.read().expect("vacuumed state");
        let versions = state.versions.get(&table_id).expect("versions");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version_id, 1);
        assert_eq!(versions[0].header.previous_version, 0);
        assert_eq!(
            state
                .catalog
                .table_by_id(table_id)
                .expect("documents")
                .statistics()
                .row_count,
            1
        );
        drop(state);
        assert_eq!(
            rows(&execute(
                &mut writer,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v2".to_owned())])]
        );
    }

    #[test]
    fn vacuum_rejects_an_expired_protected_snapshot() {
        let (_directory, engine) = engine();
        engine
            .set_maximum_snapshot_age(Duration::from_millis(1))
            .expect("configure snapshot age");
        let mut reader = engine.connect().expect("reader");
        let mut writer = engine.connect().expect("writer");
        create_documents(&mut writer);
        execute(
            &mut writer,
            "INSERT INTO documents VALUES (1, 'visible', 1)",
            &[],
        );
        execute(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ", &[]);
        execute(&mut reader, "SELECT id FROM documents", &[]);
        std::thread::sleep(Duration::from_millis(5));
        let error = writer
            .execute("VACUUM documents", &[])
            .expect_err("expired snapshot");
        assert_eq!(error.sql_state, "55000");
        assert!(error.message.contains("expired snapshot"));
        execute(&mut reader, "ROLLBACK", &[]);
        execute(&mut writer, "VACUUM documents", &[]);
    }

    #[test]
    fn full_vacuum_freezes_live_versions_and_compacts_transaction_status() {
        let (directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'v1', 1)",
            &[],
        );
        execute(
            &mut session,
            "UPDATE documents SET title = 'v2' WHERE id = 1",
            &[],
        );
        let before = engine
            .transaction_status
            .snapshot()
            .expect("status before vacuum");
        execute(&mut session, "VACUUM", &[]);
        let after = engine
            .transaction_status
            .snapshot()
            .expect("status after vacuum");
        assert!(after.retained_transaction_floor > before.retained_transaction_floor);
        assert!(after.statuses.len() < before.statuses.len());
        let table_id = engine
            .catalog_snapshot()
            .expect("catalog")
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        assert!(
            engine
                .state
                .read()
                .expect("state")
                .versions
                .get(&table_id)
                .expect("versions")
                .iter()
                .all(|version| version.header.xmin == FROZEN_TRANSACTION_ID)
        );
        drop(session);
        drop(engine);

        let reopened = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
        let mut session = reopened.connect().expect("reopened session");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT title FROM documents WHERE id = 1",
                &[],
            )),
            vec![Row::new(vec![Value::Text("v2".to_owned())])]
        );
    }

    #[test]
    fn vacuum_is_rejected_inside_sql_and_programmatic_transactions() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(&mut session, "BEGIN", &[]);
        assert_eq!(
            session
                .execute("VACUUM", &[])
                .expect_err("SQL transaction VACUUM")
                .sql_state,
            "25001"
        );
        execute(&mut session, "ROLLBACK", &[]);

        let mut transaction = session.begin().expect("programmatic transaction");
        assert_eq!(
            transaction
                .execute("VACUUM", &[])
                .expect_err("programmatic transaction VACUUM")
                .sql_state,
            "25001"
        );
        transaction.rollback().expect("rollback");
    }

    #[test]
    fn failed_vacuum_and_analyze_reopen_without_publishing_candidates() {
        let directory = tempdir().expect("tempdir");
        {
            let engine =
                Engine::open(EngineConfig::new(directory.path())).expect("baseline engine");
            let mut session = engine.connect().expect("baseline session");
            create_documents(&mut session);
            execute(
                &mut session,
                "INSERT INTO documents VALUES (1, 'v1', 1)",
                &[],
            );
            execute(
                &mut session,
                "UPDATE documents SET title = 'v2' WHERE id = 1",
                &[],
            );
        }

        let vacuum_fault = ordadb_transaction::DeterministicFaultInjector::new();
        let vacuum_injector: Arc<dyn FaultInjector> = vacuum_fault.clone();
        let engine =
            Engine::open_with_fault_injector(EngineConfig::new(directory.path()), vacuum_injector)
                .expect("vacuum engine");
        let baseline_generation = engine
            .status_snapshot()
            .expect("baseline status")
            .generation;
        let mut session = engine.connect().expect("vacuum session");
        vacuum_fault
            .arm(FaultPoint::AfterDataSync, 1)
            .expect("arm vacuum fault");
        assert_eq!(
            session
                .execute("VACUUM documents", &[])
                .expect_err("vacuum fault")
                .sql_state,
            "58030"
        );
        drop(session);
        drop(engine);

        let recovered = Engine::open(EngineConfig::new(directory.path())).expect("recover vacuum");
        assert_eq!(
            recovered
                .status_snapshot()
                .expect("recovered status")
                .generation,
            baseline_generation
        );
        let table_id = recovered
            .catalog_snapshot()
            .expect("catalog")
            .table(
                &Identifier::unquoted("public"),
                &Identifier::unquoted("documents"),
            )
            .expect("documents")
            .id;
        assert_eq!(
            recovered
                .state
                .read()
                .expect("recovered state")
                .versions
                .get(&table_id)
                .expect("version chain")
                .len(),
            2
        );
        drop(recovered);

        let analyze_fault = ordadb_transaction::DeterministicFaultInjector::new();
        let analyze_injector: Arc<dyn FaultInjector> = analyze_fault.clone();
        let engine =
            Engine::open_with_fault_injector(EngineConfig::new(directory.path()), analyze_injector)
                .expect("analyze engine");
        let baseline_generation = engine
            .status_snapshot()
            .expect("baseline status")
            .generation;
        let mut session = engine.connect().expect("analyze session");
        analyze_fault
            .arm(FaultPoint::AfterDataSync, 1)
            .expect("arm analyze fault");
        assert_eq!(
            session
                .execute("ANALYZE documents", &[])
                .expect_err("analyze fault")
                .sql_state,
            "58030"
        );
        drop(session);
        drop(engine);

        let recovered = Engine::open(EngineConfig::new(directory.path())).expect("recover analyze");
        assert_eq!(
            recovered
                .status_snapshot()
                .expect("recovered status")
                .generation,
            baseline_generation
        );
        let mut session = recovered.connect().expect("final session");
        execute(&mut session, "VACUUM ANALYZE documents", &[]);
        assert_eq!(
            recovered
                .state
                .read()
                .expect("vacuumed state")
                .versions
                .get(&table_id)
                .expect("compacted version chain")
                .len(),
            1
        );
    }

    #[test]
    fn repeated_savepoint_names_use_the_nearest_frame_and_release_it() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(&mut session, "BEGIN", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (1, 'base', 1)",
            &[],
        );
        execute(&mut session, "SAVEPOINT repeated", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (2, 'middle', 2)",
            &[],
        );
        execute(&mut session, "SAVEPOINT repeated", &[]);
        execute(
            &mut session,
            "INSERT INTO documents VALUES (3, 'latest', 3)",
            &[],
        );

        execute(&mut session, "ROLLBACK TO repeated", &[]);
        execute(&mut session, "RELEASE SAVEPOINT repeated", &[]);
        execute(&mut session, "ROLLBACK TO repeated", &[]);
        execute(&mut session, "COMMIT", &[]);
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id FROM documents ORDER BY id",
                &[],
            )),
            vec![Row::new(vec![Value::Int64(1)])]
        );
    }

    #[test]
    fn read_only_and_chained_transactions_preserve_characteristics() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("session");
        create_documents(&mut session);
        execute(
            &mut session,
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE",
            &[],
        );
        let first_id = match &session.sql_transaction {
            SqlTransactionState::Active(transaction) => {
                assert_eq!(
                    transaction.transaction.characteristics(),
                    Some(TransactionCharacteristics {
                        isolation_level: ordadb_transaction::IsolationLevel::Serializable,
                        access_mode: TransactionAccessMode::ReadOnly,
                        deferrable: true,
                    })
                );
                transaction.transaction.transaction_id()
            }
            _ => panic!("expected active transaction"),
        };
        assert_eq!(
            session
                .execute("INSERT INTO documents VALUES (1, 'blocked', 1)", &[])
                .expect_err("read only")
                .sql_state,
            "25006"
        );
        execute(&mut session, "ROLLBACK AND CHAIN", &[]);
        let second_id = match &session.sql_transaction {
            SqlTransactionState::Active(transaction) => {
                assert_eq!(
                    transaction.transaction.characteristics(),
                    Some(TransactionCharacteristics {
                        isolation_level: ordadb_transaction::IsolationLevel::Serializable,
                        access_mode: TransactionAccessMode::ReadOnly,
                        deferrable: true,
                    })
                );
                transaction.transaction.transaction_id()
            }
            _ => panic!("expected chained transaction"),
        };
        assert!(second_id > first_id);
        execute(&mut session, "ROLLBACK AND NO CHAIN", &[]);
        assert_eq!(session.transaction_status(), TransactionStatus::Idle);
    }

    #[test]
    fn deferrable_safe_snapshot_wait_cancels_through_the_session_boundary() {
        let (_directory, engine) = engine();
        let mut writer = engine.connect().expect("writer");
        let mut reader = engine.connect().expect("reader");
        create_documents(&mut writer);
        execute(&mut writer, "BEGIN ISOLATION LEVEL SERIALIZABLE", &[]);
        execute(
            &mut reader,
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE",
            &[],
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let worker_cancellation = Arc::clone(&cancellation);
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = reader
                .execute_stream_with_cancellation(
                    "SELECT id FROM documents",
                    &[],
                    worker_cancellation,
                )
                .map(|_| ());
            send.send((result, reader.transaction_status()))
                .expect("send cancellation result");
        });
        std::thread::sleep(Duration::from_millis(20));
        cancellation.store(true, Ordering::Release);

        let (result, status) = receive
            .recv_timeout(Duration::from_secs(1))
            .expect("cancelled statement result");
        assert_eq!(
            result.expect_err("safe snapshot cancellation").sql_state,
            "57014"
        );
        assert_eq!(status, TransactionStatus::Failed);
        execute(&mut writer, "ROLLBACK", &[]);
        worker.join().expect("reader worker");
    }

    #[test]
    fn emits_schema_then_work_then_exactly_one_completion() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        create_documents(&mut session);
        let events = execute(&mut session, "SELECT * FROM documents", &[]);

        assert!(matches!(events.first(), Some(QueryEvent::Schema(_))));
        assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, QueryEvent::Complete(_)))
                .count(),
            1
        );
        assert!(events[1..events.len() - 1].iter().all(|event| matches!(
            event,
            QueryEvent::Batch(_) | QueryEvent::Progress(_) | QueryEvent::Notice(_)
        )));
    }

    #[test]
    fn open_bootstraps_and_reopens_the_persistent_store() {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
            assert_eq!(configured_data_dir(engine.config()), directory.path());
        }
        assert!(directory.path().join("ordadb.data").is_file());
        assert!(Engine::open(EngineConfig::new(directory.path())).is_ok());
    }

    #[test]
    fn cluster_transaction_floor_seeds_the_first_durable_transaction() {
        let directory = tempdir().expect("tempdir");
        let engine = Engine::open(EngineConfig::for_cluster(
            directory.path().join("database"),
            directory.path(),
            42,
        ))
        .expect("open cluster database");
        let mut session = engine.connect().expect("session");
        execute(
            &mut session,
            "CREATE TABLE migration_floor (id BIGINT)",
            &[],
        );
        drop(session);
        drop(engine);

        assert_eq!(
            ordadb_transaction::inspect_wal_read_only(directory.path().join("database"))
                .expect("inspect WAL")
                .max_transaction_id
                .expect("transaction ID")
                .get(),
            42
        );
    }

    #[test]
    fn executes_inner_left_join_grouped_aggregates_and_having() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT, amount BIGINT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO orders VALUES (10, 1, 5), (11, 1, 7)",
            &[],
        );

        let grouped = execute(
            &mut session,
            "SELECT c.id, COUNT(o.id) AS order_count, SUM(o.amount) AS total \
             FROM customers c LEFT JOIN orders o ON c.id = o.customer_id \
             GROUP BY c.id ORDER BY c.id",
            &[],
        );
        assert_eq!(
            rows(&grouped),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(2), Value::Int64(12)]),
                Row::new(vec![Value::Int64(2), Value::Int64(0), Value::Null]),
            ]
        );

        let filtered_aggregates = execute(
            &mut session,
            "SELECT c.id, COUNT(*) FILTER (WHERE o.amount > 5) AS large_orders, \
             SUM(o.amount) FILTER (WHERE o.amount > 5) AS large_total \
             FROM customers c LEFT JOIN orders o ON c.id = o.customer_id \
             GROUP BY c.id ORDER BY c.id",
            &[],
        );
        assert_eq!(
            rows(&filtered_aggregates),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(1), Value::Int64(7)]),
                Row::new(vec![Value::Int64(2), Value::Int64(0), Value::Null]),
            ]
        );

        let having = execute(
            &mut session,
            "SELECT c.id, COUNT(o.id) AS order_count \
             FROM customers c INNER JOIN orders o ON c.id = o.customer_id \
             GROUP BY c.id HAVING COUNT(o.id) > 1",
            &[],
        );
        assert_eq!(
            rows(&having),
            vec![Row::new(vec![Value::Int64(1), Value::Int64(2)])]
        );

        let aggregate = execute(
            &mut session,
            "SELECT COUNT(*), AVG(amount), MIN(amount), MAX(amount) FROM orders",
            &[],
        );
        assert_eq!(
            rows(&aggregate),
            vec![Row::new(vec![
                Value::Int64(2),
                Value::Float64(6.0),
                Value::Int64(5),
                Value::Int64(7),
            ])]
        );

        execute(
            &mut session,
            "CREATE TABLE aggregate_values (id BIGINT PRIMARY KEY, amount BIGINT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO aggregate_values VALUES (1, 5), (2, 5), (3, 7), (4, NULL)",
            &[],
        );
        let distinct = execute(
            &mut session,
            "SELECT COUNT(DISTINCT amount), SUM(DISTINCT amount), \
             AVG(DISTINCT amount), MIN(DISTINCT amount), MAX(DISTINCT amount), \
             COUNT(DISTINCT amount) FILTER (WHERE id < 3) FROM aggregate_values",
            &[],
        );
        assert_eq!(
            rows(&distinct),
            vec![Row::new(vec![
                Value::Int64(2),
                Value::Int64(12),
                Value::Float64(6.0),
                Value::Int64(5),
                Value::Int64(7),
                Value::Int64(1),
            ])]
        );

        let empty = execute(
            &mut session,
            "SELECT COUNT(DISTINCT amount), SUM(DISTINCT amount), \
             AVG(amount), MIN(amount), MAX(amount) \
             FROM aggregate_values WHERE id < 0",
            &[],
        );
        assert_eq!(
            rows(&empty),
            vec![Row::new(vec![
                Value::Int64(0),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ])]
        );
    }

    #[test]
    fn executes_select_distinct_before_offset_and_limit() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE distinct_values (id BIGINT PRIMARY KEY, bucket BIGINT, label TEXT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO distinct_values VALUES \
             (1, 1, 'b'), (2, 1, 'b'), (3, 2, 'a'), (4, 2, 'a'), (5, 2, NULL)",
            &[],
        );

        let distinct = execute(
            &mut session,
            "SELECT DISTINCT bucket, label FROM distinct_values ORDER BY label, bucket",
            &[],
        );
        assert_eq!(
            rows(&distinct),
            vec![
                Row::new(vec![Value::Int64(2), Value::Text("a".to_owned())]),
                Row::new(vec![Value::Int64(1), Value::Text("b".to_owned())]),
                Row::new(vec![Value::Int64(2), Value::Null]),
            ]
        );

        let paged = execute(
            &mut session,
            "SELECT DISTINCT label FROM distinct_values ORDER BY label OFFSET 1 LIMIT 1",
            &[],
        );
        assert_eq!(
            rows(&paged),
            vec![Row::new(vec![Value::Text("b".to_owned())])]
        );

        let in_rows = execute(
            &mut session,
            "SELECT id FROM distinct_values WHERE bucket IN ($1, 99, NULL) ORDER BY id",
            &[Value::Int64(1)],
        );
        assert_eq!(
            rows(&in_rows),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        let not_in_with_null = execute(
            &mut session,
            "SELECT id FROM distinct_values WHERE bucket NOT IN (1, NULL) ORDER BY id",
            &[],
        );
        assert!(rows(&not_in_with_null).is_empty());
        let projected_in = execute(
            &mut session,
            "SELECT id, label IN ('a', NULL) FROM distinct_values ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&projected_in),
            vec![
                Row::new(vec![Value::Int64(1), Value::Null]),
                Row::new(vec![Value::Int64(2), Value::Null]),
                Row::new(vec![Value::Int64(3), Value::Boolean(true)]),
                Row::new(vec![Value::Int64(4), Value::Boolean(true)]),
                Row::new(vec![Value::Int64(5), Value::Null]),
            ]
        );

        execute(
            &mut session,
            "INSERT INTO distinct_values VALUES (6, 3, 'c'), (7, 3, 'c')",
            &[],
        );
        let grouped = execute(
            &mut session,
            "SELECT DISTINCT COUNT(*) AS count FROM distinct_values \
             GROUP BY bucket ORDER BY count",
            &[],
        );
        assert_eq!(
            rows(&grouped),
            vec![
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
        let grouped_in = execute(
            &mut session,
            "SELECT bucket, COUNT(*) IN (2) FROM distinct_values \
             GROUP BY bucket ORDER BY bucket",
            &[],
        );
        assert_eq!(
            rows(&grouped_in),
            vec![
                Row::new(vec![Value::Int64(1), Value::Boolean(true)]),
                Row::new(vec![Value::Int64(2), Value::Boolean(false)]),
                Row::new(vec![Value::Int64(3), Value::Boolean(true)]),
            ]
        );
    }

    #[test]
    fn executes_uncorrelated_apply_with_postgres_cardinality_and_null_semantics() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE apply_values (id BIGINT PRIMARY KEY, value BIGINT, marker TEXT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO apply_values VALUES (1, 10, 'a'), (2, 20, 'b'), (3, NULL, 'c')",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE apply_lookup (id BIGINT PRIMARY KEY, value BIGINT, marker TEXT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO apply_lookup VALUES (1, 10, 'x'), (2, 20, 'y'), (3, NULL, 'z')",
            &[],
        );

        let scalar = execute(
            &mut session,
            "SELECT id, (SELECT value FROM apply_lookup WHERE id = 1) \
             FROM apply_values WHERE id IN (1, 3) ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&scalar),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(10)]),
                Row::new(vec![Value::Int64(3), Value::Int64(10)]),
            ]
        );
        let empty_scalar = execute(
            &mut session,
            "SELECT id, (SELECT value FROM apply_lookup WHERE id = 99) \
             FROM apply_values WHERE id = 1",
            &[],
        );
        assert_eq!(
            rows(&empty_scalar),
            vec![Row::new(vec![Value::Int64(1), Value::Null])]
        );

        let exists = execute(
            &mut session,
            "SELECT id, EXISTS (SELECT id, marker FROM apply_lookup WHERE id = 2), \
             NOT EXISTS (SELECT id FROM apply_lookup WHERE id = 99) \
             FROM apply_values WHERE id = 1",
            &[],
        );
        assert_eq!(
            rows(&exists),
            vec![Row::new(vec![
                Value::Int64(1),
                Value::Boolean(true),
                Value::Boolean(true),
            ])]
        );

        let membership = execute(
            &mut session,
            "SELECT id, value IN (SELECT value FROM apply_lookup WHERE id IN (1, 3)), \
             value NOT IN (SELECT value FROM apply_lookup WHERE id IN (1, 3)) \
             FROM apply_values ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&membership),
            vec![
                Row::new(vec![
                    Value::Int64(1),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ]),
                Row::new(vec![Value::Int64(2), Value::Null, Value::Null]),
                Row::new(vec![Value::Int64(3), Value::Null, Value::Null]),
            ]
        );

        let quantified = execute(
            &mut session,
            "SELECT id, value = ANY (SELECT value FROM apply_lookup WHERE id IN (1, 3)), \
             value = ALL (SELECT value FROM apply_lookup WHERE id IN (1, 3)), \
             value = ANY (SELECT value FROM apply_lookup WHERE id = 99), \
             value = ALL (SELECT value FROM apply_lookup WHERE id = 99) \
             FROM apply_values ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&quantified),
            vec![
                Row::new(vec![
                    Value::Int64(1),
                    Value::Boolean(true),
                    Value::Null,
                    Value::Boolean(false),
                    Value::Boolean(true),
                ]),
                Row::new(vec![
                    Value::Int64(2),
                    Value::Null,
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::Boolean(true),
                ]),
                Row::new(vec![
                    Value::Int64(3),
                    Value::Null,
                    Value::Null,
                    Value::Boolean(false),
                    Value::Boolean(true),
                ]),
            ]
        );

        let parameterized = execute(
            &mut session,
            "SELECT id, $1 IN (SELECT value FROM apply_lookup WHERE id = 2) \
             FROM apply_values WHERE id = 1",
            &[Value::Int64(20)],
        );
        assert_eq!(
            rows(&parameterized),
            vec![Row::new(vec![Value::Int64(1), Value::Boolean(true),])]
        );

        let cte_apply = execute(
            &mut session,
            "WITH lookup(value) AS (
                 SELECT value FROM apply_lookup WHERE id = 2
             )
             SELECT id FROM apply_values
             WHERE value IN (SELECT value FROM lookup) ORDER BY id",
            &[],
        );
        assert_eq!(rows(&cte_apply), vec![Row::new(vec![Value::Int64(2)])]);

        let catalog = engine.catalog_snapshot().expect("catalog snapshot");
        let predicate_statement = bind(
            parse(
                "SELECT id FROM apply_values \
                 WHERE EXISTS (SELECT id FROM apply_lookup)",
            )
            .expect("parse Apply predicate locks"),
            &catalog,
        )
        .expect("bind Apply predicate locks");
        assert_eq!(statement_read_predicates(&predicate_statement).len(), 2);

        let explain = execute(
            &mut session,
            "EXPLAIN SELECT id FROM apply_values \
             WHERE EXISTS (SELECT id FROM apply_lookup)",
            &[],
        );
        assert!(rows(&explain).iter().any(|row| {
            matches!(row.values.as_slice(), [Value::Text(line)] if line.contains("Exists Apply"))
        }));

        let error = session
            .execute(
                "SELECT (SELECT value FROM apply_lookup) FROM apply_values",
                &[],
            )
            .expect_err("scalar subquery returning multiple rows");
        assert_eq!(error.sql_state, "21000");
    }

    #[test]
    fn executes_row_comparisons_and_row_subqueries_with_three_value_logic() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE row_values (id BIGINT PRIMARY KEY, key_value BIGINT, item_value BIGINT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO row_values VALUES \
             (1, 10, 100), (2, 10, 200), (3, 20, NULL), (4, 30, 300)",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE row_lookup (id BIGINT PRIMARY KEY, key_value BIGINT, item_value BIGINT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO row_lookup VALUES (1, 10, 100), (2, 10, NULL), (3, 20, NULL)",
            &[],
        );

        let membership = execute(
            &mut session,
            "SELECT id,
                    (key_value, item_value) IN (
                        SELECT key_value, item_value FROM row_lookup
                    ) AS included,
                    (key_value, item_value) NOT IN (
                        SELECT key_value, item_value FROM row_lookup
                    ) AS excluded
             FROM row_values ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&membership),
            vec![
                Row::new(vec![
                    Value::Int64(1),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ]),
                Row::new(vec![Value::Int64(2), Value::Null, Value::Null]),
                Row::new(vec![Value::Int64(3), Value::Null, Value::Null]),
                Row::new(vec![
                    Value::Int64(4),
                    Value::Boolean(false),
                    Value::Boolean(true),
                ]),
            ]
        );

        let scalar = execute(
            &mut session,
            "SELECT id,
                    (key_value, item_value) = (
                        SELECT key_value, item_value FROM row_lookup WHERE id = 1
                    ) AS same_row,
                    (key_value, item_value) = (
                        SELECT key_value, item_value FROM row_lookup WHERE id = 99
                    ) AS empty_row
             FROM row_values ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&scalar),
            vec![
                Row::new(vec![Value::Int64(1), Value::Boolean(true), Value::Null]),
                Row::new(vec![Value::Int64(2), Value::Boolean(false), Value::Null,]),
                Row::new(vec![Value::Int64(3), Value::Boolean(false), Value::Null,]),
                Row::new(vec![Value::Int64(4), Value::Boolean(false), Value::Null,]),
            ]
        );

        let direct = execute(
            &mut session,
            "SELECT id,
                    (key_value, item_value) = (10, 100) AS exact_row,
                    (key_value, item_value) IN ((10, 100), (20, NULL)) AS listed_row
             FROM row_values ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&direct),
            vec![
                Row::new(vec![
                    Value::Int64(1),
                    Value::Boolean(true),
                    Value::Boolean(true),
                ]),
                Row::new(vec![
                    Value::Int64(2),
                    Value::Boolean(false),
                    Value::Boolean(false),
                ]),
                Row::new(vec![Value::Int64(3), Value::Boolean(false), Value::Null,]),
                Row::new(vec![
                    Value::Int64(4),
                    Value::Boolean(false),
                    Value::Boolean(false),
                ]),
            ]
        );

        let correlated = execute(
            &mut session,
            "SELECT outer_values.id,
                    (outer_values.key_value, outer_values.item_value) = (
                        SELECT inner_values.key_value, inner_values.item_value
                        FROM row_lookup inner_values
                        WHERE inner_values.id = outer_values.id
                    ) AS same_row
             FROM row_values outer_values ORDER BY outer_values.id",
            &[],
        );
        assert_eq!(
            rows(&correlated),
            vec![
                Row::new(vec![Value::Int64(1), Value::Boolean(true)]),
                Row::new(vec![Value::Int64(2), Value::Null]),
                Row::new(vec![Value::Int64(3), Value::Null]),
                Row::new(vec![Value::Int64(4), Value::Null]),
            ]
        );

        let parameterized = execute(
            &mut session,
            "SELECT ($1, $2) IN (
                 SELECT key_value, item_value FROM row_lookup
             ) FROM row_values WHERE id = 1",
            &[Value::Int64(10), Value::Int64(100)],
        );
        assert_eq!(
            rows(&parameterized),
            vec![Row::new(vec![Value::Boolean(true)])]
        );

        let empty_quantifiers = execute(
            &mut session,
            "SELECT (key_value, item_value) = ANY (
                         SELECT key_value, item_value FROM row_lookup WHERE id = 99
                     ),
                    (key_value, item_value) = ALL (
                         SELECT key_value, item_value FROM row_lookup WHERE id = 99
                     )
             FROM row_values WHERE id = 1",
            &[],
        );
        assert_eq!(
            rows(&empty_quantifiers),
            vec![Row::new(vec![Value::Boolean(false), Value::Boolean(true),])]
        );

        let null_witness = execute(
            &mut session,
            "SELECT (key_value, item_value) = ANY (
                         SELECT key_value, item_value FROM row_lookup
                     ),
                    (key_value, item_value) <> ALL (
                         SELECT key_value, item_value FROM row_lookup
                     )
             FROM row_values WHERE id = 2",
            &[],
        );
        assert_eq!(
            rows(&null_witness),
            vec![Row::new(vec![Value::Null, Value::Null])]
        );

        let cardinality = session
            .execute(
                "SELECT (key_value, item_value) = (
                     SELECT key_value, item_value FROM row_lookup WHERE key_value = 10
                 ) FROM row_values WHERE id = 1",
                &[],
            )
            .expect_err("row scalar subquery cardinality");
        assert_eq!(cardinality.sql_state, "21000");

        execute(
            &mut session,
            "CREATE TABLE row_narrow (
                 id BIGINT PRIMARY KEY,
                 key_value INTEGER,
                 item_value SMALLINT
             )",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO row_narrow VALUES (1, $1, $2)",
            &[Value::Int32(10), Value::Int16(100)],
        );
        let promoted = execute(
            &mut session,
            "SELECT (key_value, item_value) = (
                         SELECT key_value, item_value FROM row_narrow WHERE id = 1
                     ),
                    (key_value, item_value) IN (
                         SELECT key_value, item_value FROM row_narrow
                     )
             FROM row_values WHERE id = 1",
            &[],
        );
        assert_eq!(
            rows(&promoted),
            vec![Row::new(vec![Value::Boolean(true), Value::Boolean(true),])]
        );

        let width = session
            .execute(
                "SELECT id FROM row_values WHERE (key_value, item_value) IN (
                     SELECT key_value FROM row_lookup
                 )",
                &[],
            )
            .expect_err("row width mismatch");
        assert_eq!(width.sql_state, "42601");
    }

    #[test]
    fn executes_correlated_apply_with_parameter_frames_and_per_row_results() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE correlated_values (id BIGINT PRIMARY KEY, value BIGINT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO correlated_values VALUES (1, 10), (2, 20), (3, NULL), (4, 40)",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE correlated_lookup (id BIGINT PRIMARY KEY, value BIGINT, marker TEXT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO correlated_lookup VALUES (1, 10, 'x'), (2, 20, 'y'), (4, NULL, 'z')",
            &[],
        );

        let scalar = execute(
            &mut session,
            "SELECT outer_values.id, (
                 SELECT inner_values.marker FROM correlated_lookup inner_values
                 WHERE inner_values.id = outer_values.id
             )
             FROM correlated_values outer_values ORDER BY outer_values.id",
            &[],
        );
        assert_eq!(
            rows(&scalar),
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("x".to_owned())]),
                Row::new(vec![Value::Int64(2), Value::Text("y".to_owned())]),
                Row::new(vec![Value::Int64(3), Value::Null]),
                Row::new(vec![Value::Int64(4), Value::Text("z".to_owned())]),
            ]
        );

        let exists = execute(
            &mut session,
            "SELECT outer_values.id FROM correlated_values outer_values
             WHERE EXISTS (
                 SELECT inner_values.id FROM correlated_lookup inner_values
                 WHERE inner_values.id = outer_values.id
             ) ORDER BY outer_values.id",
            &[],
        );
        assert_eq!(
            rows(&exists),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(4)]),
            ]
        );

        let membership = execute(
            &mut session,
            "SELECT outer_values.id, outer_values.value IN (
                 SELECT inner_values.value FROM correlated_lookup inner_values
                 WHERE inner_values.id <= outer_values.id
             ) FROM correlated_values outer_values ORDER BY outer_values.id",
            &[],
        );
        assert_eq!(
            rows(&membership),
            vec![
                Row::new(vec![Value::Int64(1), Value::Boolean(true)]),
                Row::new(vec![Value::Int64(2), Value::Boolean(true)]),
                Row::new(vec![Value::Int64(3), Value::Null]),
                Row::new(vec![Value::Int64(4), Value::Null]),
            ]
        );

        let quantified = execute(
            &mut session,
            "SELECT outer_values.id,
                    outer_values.value = ANY (
                        SELECT inner_values.value FROM correlated_lookup inner_values
                        WHERE inner_values.id <= outer_values.id
                    ),
                    outer_values.value = ALL (
                        SELECT inner_values.value FROM correlated_lookup inner_values
                        WHERE inner_values.id <= outer_values.id
                    )
             FROM correlated_values outer_values ORDER BY outer_values.id",
            &[],
        );
        assert_eq!(
            rows(&quantified),
            vec![
                Row::new(vec![
                    Value::Int64(1),
                    Value::Boolean(true),
                    Value::Boolean(true),
                ]),
                Row::new(vec![
                    Value::Int64(2),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ]),
                Row::new(vec![Value::Int64(3), Value::Null, Value::Null]),
                Row::new(vec![Value::Int64(4), Value::Null, Value::Boolean(false)]),
            ]
        );

        let shadowed = execute(
            &mut session,
            "SELECT outer_values.id FROM correlated_values outer_values
             WHERE EXISTS (
                 SELECT id FROM correlated_lookup
                 WHERE id = id AND outer_values.id = 3
             )",
            &[],
        );
        assert_eq!(rows(&shadowed), vec![Row::new(vec![Value::Int64(3)])]);

        let nested = execute(
            &mut session,
            "SELECT outer_values.id FROM correlated_values outer_values
             WHERE EXISTS (
                 SELECT middle_values.id FROM correlated_lookup middle_values
                 WHERE EXISTS (
                     SELECT inner_values.id FROM correlated_lookup inner_values
                     WHERE inner_values.id = middle_values.id
                       AND middle_values.id = outer_values.id
                 )
             ) ORDER BY outer_values.id",
            &[],
        );
        assert_eq!(
            rows(&nested),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(4)]),
            ]
        );

        let parameterized = execute(
            &mut session,
            "SELECT outer_values.id FROM correlated_values outer_values
             WHERE EXISTS (
                 SELECT inner_values.id FROM correlated_lookup inner_values
                 WHERE inner_values.id = outer_values.id AND inner_values.marker = $1
             )",
            &[Value::Text("y".to_owned())],
        );
        assert_eq!(rows(&parameterized), vec![Row::new(vec![Value::Int64(2)])]);

        let error = session
            .execute(
                "SELECT (
                     SELECT inner_values.value FROM correlated_lookup inner_values
                     WHERE inner_values.id <= outer_values.id
                 ) FROM correlated_values outer_values WHERE outer_values.id = 2",
                &[],
            )
            .expect_err("correlated scalar returning multiple rows");
        assert_eq!(error.sql_state, "21000");

        let error = session
            .execute(
                "SELECT outer_values.id FROM correlated_values outer_values
                 WHERE EXISTS (
                     SELECT inner_values.id FROM correlated_lookup inner_values
                     WHERE inner_values.id = missing_outer.id
                 )",
                &[],
            )
            .expect_err("unknown outer alias");
        assert_eq!(error.sql_state, "42703");
    }

    #[test]
    fn executes_streaming_lateral_joins_with_left_null_extension() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE lateral_values (id BIGINT PRIMARY KEY, ceiling BIGINT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO lateral_values VALUES (1, 1), (2, 2), (3, 0)",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE lateral_lookup (id BIGINT PRIMARY KEY, marker TEXT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO lateral_lookup VALUES (1, 'a'), (2, 'b')",
            &[],
        );

        let inner = execute(
            &mut session,
            "SELECT outer_values.id, matched.renamed_marker
             FROM lateral_values outer_values
             INNER JOIN LATERAL (
                 SELECT lookup.marker FROM lateral_lookup lookup
                 WHERE lookup.id <= outer_values.ceiling
             ) AS matched(renamed_marker) ON TRUE
             ORDER BY outer_values.id, matched.renamed_marker",
            &[],
        );
        assert_eq!(
            rows(&inner),
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("a".to_owned())]),
                Row::new(vec![Value::Int64(2), Value::Text("a".to_owned())]),
                Row::new(vec![Value::Int64(2), Value::Text("b".to_owned())]),
            ]
        );

        let left = execute(
            &mut session,
            "SELECT outer_values.id, matched.marker
             FROM lateral_values outer_values
             LEFT JOIN LATERAL (
                 SELECT lookup.marker FROM lateral_lookup lookup
                 WHERE lookup.id = outer_values.id
             ) AS matched ON TRUE
             ORDER BY outer_values.id",
            &[],
        );
        assert_eq!(
            rows(&left),
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("a".to_owned())]),
                Row::new(vec![Value::Int64(2), Value::Text("b".to_owned())]),
                Row::new(vec![Value::Int64(3), Value::Null]),
            ]
        );

        let parameterized = execute(
            &mut session,
            "SELECT outer_values.id
             FROM lateral_values outer_values
             INNER JOIN LATERAL (
                 SELECT lookup.marker FROM lateral_lookup lookup
                 WHERE lookup.id = outer_values.id AND lookup.marker = $1
             ) AS matched ON TRUE",
            &[Value::Text("b".to_owned())],
        );
        assert_eq!(rows(&parameterized), vec![Row::new(vec![Value::Int64(2)])]);

        let multiple_left_inputs = execute(
            &mut session,
            "SELECT outer_values.id, derived.marker
             FROM lateral_values outer_values
             INNER JOIN lateral_lookup first_match ON first_match.id = outer_values.id
             INNER JOIN LATERAL (
                 SELECT second_match.marker FROM lateral_lookup second_match
                 WHERE second_match.id = first_match.id
                   AND second_match.id = outer_values.id
             ) AS derived ON TRUE
             ORDER BY outer_values.id",
            &[],
        );
        assert_eq!(
            rows(&multiple_left_inputs),
            vec![
                Row::new(vec![Value::Int64(1), Value::Text("a".to_owned())]),
                Row::new(vec![Value::Int64(2), Value::Text("b".to_owned())]),
            ]
        );

        let catalog = engine.catalog_snapshot().expect("catalog snapshot");
        let statement = bind(
            parse(
                "SELECT outer_values.id FROM lateral_values outer_values
                 INNER JOIN LATERAL (
                     SELECT lookup.id FROM lateral_lookup lookup
                     WHERE lookup.id = outer_values.id
                 ) AS matched ON TRUE",
            )
            .expect("parse LATERAL predicate locks"),
            &catalog,
        )
        .expect("bind LATERAL predicate locks");
        assert_eq!(statement_read_predicates(&statement).len(), 2);

        let explain = execute(
            &mut session,
            "EXPLAIN SELECT outer_values.id FROM lateral_values outer_values
             INNER JOIN LATERAL (
                 SELECT lookup.id FROM lateral_lookup lookup
                 WHERE lookup.id = outer_values.id
             ) AS matched ON TRUE",
            &[],
        );
        assert!(rows(&explain).iter().any(|row| {
            matches!(row.values.as_slice(), [Value::Text(line)] if line.contains("Lateral Subquery Scan"))
        }));

        let cancellation = Arc::new(AtomicBool::new(false));
        let mut stream = session
            .execute_stream_with_cancellation(
                "SELECT outer_values.id, matched.marker
                 FROM lateral_values outer_values
                 INNER JOIN LATERAL (
                     SELECT lookup.marker FROM lateral_lookup lookup
                     WHERE lookup.id <= outer_values.id
                 ) AS matched ON TRUE",
                &[],
                Arc::clone(&cancellation),
            )
            .expect("LATERAL cancellable stream");
        assert!(matches!(
            stream.next().expect("schema").expect("schema event"),
            QueryEvent::Schema(_)
        ));
        cancellation.store(true, Ordering::Release);
        assert_eq!(
            stream
                .next()
                .expect("cancellation error")
                .expect_err("cancelled LATERAL stream")
                .sql_state,
            "57014"
        );
        assert!(stream.next().is_none());

        let error = session
            .execute(
                "SELECT outer_values.id FROM lateral_values outer_values
                 INNER JOIN (
                     SELECT lookup.id FROM lateral_lookup lookup
                     WHERE lookup.id = outer_values.id
                 ) AS matched ON TRUE",
                &[],
            )
            .expect_err("non-LATERAL derived table cannot see the left input");
        assert_eq!(error.sql_state, "42703");
    }

    #[test]
    fn executes_partitioned_ranking_windows_with_apply_and_outer_order() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE window_values (id BIGINT PRIMARY KEY, group_name TEXT, score BIGINT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO window_values VALUES \
             (1, 'a', 20), (2, 'a', 10), (3, 'a', 20), (4, 'b', 30)",
            &[],
        );

        let ranked = execute(
            &mut session,
            "SELECT id, \
                    (SELECT lookup.score FROM window_values lookup \
                     WHERE lookup.id = window_values.id) AS copied_score, \
                    ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY score DESC) AS row_no, \
                    RANK() OVER (PARTITION BY group_name ORDER BY score DESC) AS rank_no, \
                    DENSE_RANK() OVER (PARTITION BY group_name ORDER BY score DESC) AS dense_no \
             FROM window_values ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&ranked),
            vec![
                Row::new(vec![
                    Value::Int64(1),
                    Value::Int64(20),
                    Value::Int64(1),
                    Value::Int64(1),
                    Value::Int64(1),
                ]),
                Row::new(vec![
                    Value::Int64(2),
                    Value::Int64(10),
                    Value::Int64(3),
                    Value::Int64(3),
                    Value::Int64(2),
                ]),
                Row::new(vec![
                    Value::Int64(3),
                    Value::Int64(20),
                    Value::Int64(2),
                    Value::Int64(1),
                    Value::Int64(1),
                ]),
                Row::new(vec![
                    Value::Int64(4),
                    Value::Int64(30),
                    Value::Int64(1),
                    Value::Int64(1),
                    Value::Int64(1),
                ]),
            ]
        );

        let named = execute(
            &mut session,
            "SELECT id, RANK() OVER ranked AS rank_no FROM window_values \
             WINDOW ranked AS (PARTITION BY group_name ORDER BY score DESC) \
             ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&named),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(1)]),
                Row::new(vec![Value::Int64(2), Value::Int64(3)]),
                Row::new(vec![Value::Int64(3), Value::Int64(1)]),
                Row::new(vec![Value::Int64(4), Value::Int64(1)]),
            ]
        );

        let framed = execute(
            &mut session,
            "SELECT id, RANK() OVER (
                 PARTITION BY group_name ORDER BY score DESC
                 ROWS BETWEEN $1 PRECEDING AND $2 FOLLOWING
             ) AS rank_no FROM window_values ORDER BY id",
            &[Value::Int64(1), Value::Int64(1)],
        );
        assert_eq!(rows(&framed), rows(&named));

        let negative_frame = session
            .execute(
                "SELECT RANK() OVER (ORDER BY score ROWS $1 PRECEDING) FROM window_values",
                &[Value::Int64(-1)],
            )
            .expect_err("negative frame offset");
        assert_eq!(negative_frame.sql_state, "22013");

        let reversed_frame = session
            .execute(
                "SELECT RANK() OVER (
                     ORDER BY score ROWS BETWEEN $1 PRECEDING AND $2 PRECEDING
                 ) FROM window_values",
                &[Value::Int64(1), Value::Int64(2)],
            )
            .expect_err("frame start after end");
        assert_eq!(reversed_frame.sql_state, "42P20");

        let values = execute(
            &mut session,
            "SELECT id,
                    LAG(score) OVER grouped AS lag_score,
                    LEAD(score, 1, -1) OVER grouped AS lead_score,
                    FIRST_VALUE(score) OVER grouped AS first_score,
                    LAST_VALUE(score) OVER (
                        grouped ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS last_score,
                    NTH_VALUE(score, 2) OVER (
                        grouped ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS second_score,
                    COUNT(*) OVER (PARTITION BY group_name) AS group_count,
                    SUM(score) OVER (
                        grouped ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS running_score,
                    AVG(score) OVER (PARTITION BY group_name) AS average_score
             FROM window_values
             WINDOW grouped AS (PARTITION BY group_name ORDER BY id)
             ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&values),
            vec![
                Row::new(vec![
                    Value::Int64(1),
                    Value::Null,
                    Value::Int64(10),
                    Value::Int64(20),
                    Value::Int64(20),
                    Value::Int64(10),
                    Value::Int64(3),
                    Value::Int64(20),
                    Value::Float64(50.0 / 3.0),
                ]),
                Row::new(vec![
                    Value::Int64(2),
                    Value::Int64(20),
                    Value::Int64(20),
                    Value::Int64(20),
                    Value::Int64(10),
                    Value::Int64(10),
                    Value::Int64(3),
                    Value::Int64(30),
                    Value::Float64(50.0 / 3.0),
                ]),
                Row::new(vec![
                    Value::Int64(3),
                    Value::Int64(10),
                    Value::Int64(-1),
                    Value::Int64(20),
                    Value::Int64(20),
                    Value::Int64(10),
                    Value::Int64(3),
                    Value::Int64(50),
                    Value::Float64(50.0 / 3.0),
                ]),
                Row::new(vec![
                    Value::Int64(4),
                    Value::Null,
                    Value::Int64(-1),
                    Value::Int64(30),
                    Value::Int64(30),
                    Value::Null,
                    Value::Int64(1),
                    Value::Int64(30),
                    Value::Float64(30.0),
                ]),
            ]
        );

        let range = execute(
            &mut session,
            "SELECT id, SUM(score) OVER (
                 ORDER BY score RANGE BETWEEN 5 PRECEDING AND CURRENT ROW
             ) AS nearby_score FROM window_values ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&range),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(40)]),
                Row::new(vec![Value::Int64(2), Value::Int64(10)]),
                Row::new(vec![Value::Int64(3), Value::Int64(40)]),
                Row::new(vec![Value::Int64(4), Value::Int64(30)]),
            ]
        );

        let sliding_rows = execute(
            &mut session,
            "SELECT id, SUM(score) OVER (
                 PARTITION BY group_name ORDER BY id
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
             ) AS sliding_score FROM window_values ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&sliding_rows),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(20)]),
                Row::new(vec![Value::Int64(2), Value::Int64(30)]),
                Row::new(vec![Value::Int64(3), Value::Int64(30)]),
                Row::new(vec![Value::Int64(4), Value::Int64(30)]),
            ]
        );

        let default_range = execute(
            &mut session,
            "SELECT id, SUM(score) OVER (ORDER BY score) AS running_peers \
             FROM window_values ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&default_range),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(50)]),
                Row::new(vec![Value::Int64(2), Value::Int64(10)]),
                Row::new(vec![Value::Int64(3), Value::Int64(50)]),
                Row::new(vec![Value::Int64(4), Value::Int64(80)]),
            ]
        );

        let signed_offsets = execute(
            &mut session,
            "SELECT id,
                    LAG(score, -1) OVER grouped AS next_score,
                    LEAD(score, NULL, 999) OVER grouped AS null_offset
             FROM window_values
             WINDOW grouped AS (PARTITION BY group_name ORDER BY id)
             ORDER BY id",
            &[],
        );
        assert_eq!(
            rows(&signed_offsets),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(10), Value::Null]),
                Row::new(vec![Value::Int64(2), Value::Int64(20), Value::Null]),
                Row::new(vec![Value::Int64(3), Value::Null, Value::Null]),
                Row::new(vec![Value::Int64(4), Value::Null, Value::Null]),
            ]
        );

        let grouped_windows = execute(
            &mut session,
            "SELECT group_name,
                    SUM(score) AS total_score,
                    RANK() OVER (ORDER BY SUM(score) DESC) AS total_rank,
                    SUM(SUM(score)) OVER (
                        ORDER BY group_name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS running_groups
             FROM window_values
             GROUP BY group_name
             ORDER BY group_name",
            &[],
        );
        assert_eq!(
            rows(&grouped_windows),
            vec![
                Row::new(vec![
                    Value::Text("a".to_owned()),
                    Value::Int64(50),
                    Value::Int64(1),
                    Value::Int64(50),
                ]),
                Row::new(vec![
                    Value::Text("b".to_owned()),
                    Value::Int64(30),
                    Value::Int64(2),
                    Value::Int64(80),
                ]),
            ]
        );

        let ordered = execute(
            &mut session,
            "SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS row_no \
             FROM window_values ORDER BY row_no DESC",
            &[],
        );
        assert_eq!(
            rows(&ordered),
            vec![
                Row::new(vec![Value::Int64(2), Value::Int64(4)]),
                Row::new(vec![Value::Int64(3), Value::Int64(3)]),
                Row::new(vec![Value::Int64(1), Value::Int64(2)]),
                Row::new(vec![Value::Int64(4), Value::Int64(1)]),
            ]
        );

        let explain = execute(
            &mut session,
            "EXPLAIN SELECT ROW_NUMBER() OVER (ORDER BY score) FROM window_values",
            &[],
        );
        assert!(rows(&explain).iter().any(|row| {
            matches!(row.values.as_slice(), [Value::Text(line)] if line.contains("WindowAgg"))
        }));

        let mut stream = session
            .execute_stream(
                "SELECT id, ROW_NUMBER() OVER (ORDER BY score) FROM window_values",
                &[],
            )
            .expect("window stream");
        for event in stream.by_ref() {
            event.expect("window stream event");
        }
        assert!(
            stream
                .execution_memory_peak_bytes()
                .is_some_and(|peak| peak > 0)
        );
    }

    #[test]
    fn persists_covering_indexes_statistics_and_explains_real_access_paths() {
        let directory = tempdir().expect("tempdir");
        {
            let engine = Engine::open(EngineConfig::new(directory.path())).expect("open");
            let mut session = engine.connect().expect("connect");
            execute(
                &mut session,
                "CREATE TABLE metrics (id BIGINT PRIMARY KEY, bucket BIGINT, score BIGINT, payload TEXT)",
                &[],
            );
            let values = (0..512)
                .map(|value| format!("({value}, {}, {value}, 'p{value}')", value % 8))
                .collect::<Vec<_>>()
                .join(", ");
            execute(
                &mut session,
                &format!("INSERT INTO metrics VALUES {values}"),
                &[],
            );
            let duplicate = session
                .execute(
                    "CREATE UNIQUE INDEX metrics_bucket_unique ON metrics (bucket)",
                    &[],
                )
                .expect_err("duplicate unique build");
            assert_eq!(duplicate.sql_state, "23505");
            execute(
                &mut session,
                "CREATE INDEX metrics_score_idx ON metrics (score) INCLUDE (payload)",
                &[],
            );
        }

        let engine = Engine::open(EngineConfig::new(directory.path())).expect("reopen");
        let mut session = engine.connect().expect("connect");
        let explain = execute(
            &mut session,
            "EXPLAIN SELECT payload FROM metrics WHERE score = 511",
            &[],
        );
        let plan = rows(&explain)
            .into_iter()
            .filter_map(|row| match row.values.as_slice() {
                [Value::Text(line)] => Some(line.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            plan.iter().any(|line| line.contains("Index Scan")),
            "{plan:?}"
        );
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT payload FROM metrics WHERE score = 511",
                &[],
            )),
            vec![Row::new(vec![Value::Text("p511".into())])]
        );
    }

    #[test]
    fn fallible_stream_preserves_event_order_and_legacy_adapter() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE stream_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO stream_items VALUES (1), (2), (3)",
            &[],
        );

        let events = session
            .execute_stream("SELECT id FROM stream_items ORDER BY id", &[])
            .expect("stream")
            .collect::<Result<Vec<_>>>()
            .expect("fallible events");
        assert!(matches!(events.first(), Some(QueryEvent::Schema(_))));
        assert!(matches!(events.last(), Some(QueryEvent::Complete(_))));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, QueryEvent::Complete(_)))
                .count(),
            1
        );
        assert_eq!(rows(&events).len(), 3);
        assert_eq!(
            session
                .execute("SELECT id FROM stream_items", &[])
                .expect("legacy")
                .filter(|event| matches!(event, QueryEvent::Complete(_)))
                .count(),
            1
        );
    }

    #[test]
    fn storage_backed_stream_holds_one_generation_until_exhaustion() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE lazy_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(&mut session, "INSERT INTO lazy_items VALUES (1), (2)", &[]);

        let snapshot = session
            .execute_stream("SELECT id FROM lazy_items ORDER BY id", &[])
            .expect("lazy stream");
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            1
        );

        let writer_engine = engine.clone();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel(0);
        let writer = std::thread::spawn(move || {
            let mut writer_session = writer_engine.connect().expect("writer connect");
            started_tx.send(()).expect("signal writer start");
            let result = writer_session
                .execute("INSERT INTO lazy_items VALUES (3)", &[])
                .map(|_| ());
            finished_tx.send(result).expect("send writer result");
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("writer started");
        let waiting_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while engine
            .storage_access
            .waiting_writers()
            .expect("waiting writers")
            == 0
        {
            assert!(
                std::time::Instant::now() < waiting_deadline,
                "writer did not reach the storage gate"
            );
            std::thread::yield_now();
        }
        assert!(matches!(
            finished_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));

        let events = snapshot
            .collect::<Result<Vec<_>>>()
            .expect("snapshot events");
        assert_eq!(
            rows(&events),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ]
        );
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            0
        );
        finished_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("writer finished after stream exhaustion")
            .expect("writer commit");
        writer.join().expect("writer thread");
        assert_eq!(
            rows(&execute(
                &mut session,
                "SELECT id FROM lazy_items ORDER BY id",
                &[],
            )),
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)]),
            ]
        );
    }

    #[test]
    fn storage_access_gate_prefers_a_waiting_writer_over_new_readers() {
        let gate = Arc::new(StorageAccessGate::default());
        let first_reader = gate.acquire_read().expect("first reader");
        let writer_gate = Arc::clone(&gate);
        let (writer_acquired_tx, writer_acquired_rx) = std::sync::mpsc::sync_channel(0);
        let (release_writer_tx, release_writer_rx) = std::sync::mpsc::sync_channel(0);
        let writer = std::thread::spawn(move || {
            let lease = writer_gate.acquire_write().expect("writer lease");
            writer_acquired_tx.send(()).expect("writer acquired");
            release_writer_rx.recv().expect("release writer");
            drop(lease);
        });

        let waiting_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while gate.waiting_writers().expect("waiting writers") == 0 {
            assert!(
                std::time::Instant::now() < waiting_deadline,
                "writer did not reach the storage gate"
            );
            std::thread::yield_now();
        }

        let second_reader_gate = Arc::clone(&gate);
        let (reader_acquired_tx, reader_acquired_rx) = std::sync::mpsc::sync_channel(0);
        let second_reader = std::thread::spawn(move || {
            let lease = second_reader_gate.acquire_read().expect("second reader");
            reader_acquired_tx.send(()).expect("reader acquired");
            drop(lease);
        });

        drop(first_reader);
        writer_acquired_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("writer wins after readers drain");
        assert!(matches!(
            reader_acquired_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        release_writer_tx.send(()).expect("release writer");
        reader_acquired_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("reader proceeds after writer");
        writer.join().expect("writer thread");
        second_reader.join().expect("reader thread");
    }

    #[test]
    fn storage_scan_open_error_and_stream_drop_release_the_read_lease() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE lease_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(&mut session, "INSERT INTO lease_items VALUES (1)", &[]);

        let generation = session.state.read().expect("state").generation;
        let snapshot = session.state.read().expect("state").clone();
        let provider = StorageTableProviderV2::new(
            Arc::clone(&engine.store),
            Arc::clone(&engine.storage_access),
            generation,
            &snapshot.rows,
            snapshot.system_catalog.as_deref(),
        );
        let error = match provider.scan(TableId::new(u64::MAX)) {
            Ok(_) => panic!("unknown table scan must fail"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "42P01");
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            0
        );

        let stream = session
            .execute_stream("SELECT id FROM lease_items", &[])
            .expect("stream");
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            1
        );
        drop(stream);
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            0
        );
    }

    #[test]
    fn storage_scan_rejects_resident_row_count_mismatch_and_releases_lease() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE resident_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(&mut session, "INSERT INTO resident_items VALUES (1)", &[]);

        let snapshot = session.state.read().expect("state").clone();
        let table_id = *snapshot.rows.keys().next().expect("resident table");
        let mismatched_rows = BTreeMap::from([(table_id, Arc::new(Vec::<Row>::new()))]);
        let provider = StorageTableProviderV2::new(
            Arc::clone(&engine.store),
            Arc::clone(&engine.storage_access),
            snapshot.generation,
            &mismatched_rows,
            None,
        );

        let error = match provider.scan(table_id) {
            Ok(_) => panic!("row-count mismatch must fail"),
            Err(error) => error,
        };
        assert_eq!(error.sql_state, "XX001");
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            0
        );
    }

    #[test]
    fn cancelled_storage_stream_releases_resources_without_terminal_success_events() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE cancelled_items (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO cancelled_items VALUES (1), (2), (3)",
            &[],
        );

        let cancellation = Arc::new(AtomicBool::new(false));
        let mut stream = session
            .execute_stream_with_cancellation(
                "SELECT id FROM cancelled_items",
                &[],
                Arc::clone(&cancellation),
            )
            .expect("stream");
        assert!(matches!(
            stream.next().expect("schema").expect("schema event"),
            QueryEvent::Schema(_)
        ));
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            1
        );

        cancellation.store(true, Ordering::Release);
        assert_eq!(
            stream
                .next()
                .expect("cancellation error")
                .expect_err("cancelled")
                .sql_state,
            "57014"
        );
        assert!(stream.next().is_none());
        assert_eq!(
            engine
                .storage_access
                .active_readers()
                .expect("active readers"),
            0
        );
    }

    #[test]
    fn fallible_stream_retains_query_accounted_peak_after_completion() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE memory_items (id BIGINT, payload TEXT)",
            &[],
        );
        execute(
            &mut session,
            "INSERT INTO memory_items VALUES (1, 'alpha'), (2, 'beta')",
            &[],
        );

        let mut stream = session
            .execute_stream("SELECT id, payload FROM memory_items", &[])
            .expect("stream");
        assert_eq!(stream.execution_memory_peak_bytes(), Some(0));
        while stream.next().is_some() {}
        assert!(
            stream
                .execution_memory_peak_bytes()
                .is_some_and(|peak| peak > 0 && peak <= 256 * 1024 * 1024)
        );
    }

    #[test]
    fn read_snapshots_share_arcs_and_writes_copy_only_the_affected_table() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE cow_a (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(
            &mut session,
            "CREATE TABLE cow_b (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(&mut session, "INSERT INTO cow_a VALUES (1)", &[]);
        execute(&mut session, "INSERT INTO cow_b VALUES (2)", &[]);

        let (a_before, b_before, catalog_before) = {
            let state = session.state.read().expect("state");
            (
                state.rows.get(&TableId::new(1)).expect("a").clone(),
                state.rows.get(&TableId::new(2)).expect("b").clone(),
                state.catalog.clone(),
            )
        };
        execute(&mut session, "SELECT id FROM cow_a", &[]);
        {
            let state = session.state.read().expect("state");
            assert!(Arc::ptr_eq(
                &a_before,
                state.rows.get(&TableId::new(1)).expect("a")
            ));
            assert!(Arc::ptr_eq(
                &b_before,
                state.rows.get(&TableId::new(2)).expect("b")
            ));
            assert!(Arc::ptr_eq(&catalog_before, &state.catalog));
        }

        execute(&mut session, "UPDATE cow_a SET id = 3", &[]);
        let state = session.state.read().expect("state");
        assert!(!Arc::ptr_eq(
            &a_before,
            state.rows.get(&TableId::new(1)).expect("a")
        ));
        assert!(Arc::ptr_eq(
            &b_before,
            state.rows.get(&TableId::new(2)).expect("b")
        ));
    }

    #[test]
    fn a_lazy_stream_error_marks_its_sql_transaction_failed() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        execute(
            &mut session,
            "CREATE TABLE stream_failures (id BIGINT PRIMARY KEY)",
            &[],
        );
        execute(&mut session, "BEGIN", &[]);
        let failure_flag = match &session.sql_transaction {
            SqlTransactionState::Active(transaction) => Arc::clone(&transaction.stream_failed),
            _ => panic!("expected active SQL transaction"),
        };
        let mut stream = TryQueryStream {
            state: TryQueryStreamState::Events(
                vec![Err(DbError::new("53200", "query memory limit exceeded"))].into_iter(),
            ),
            failed: false,
            failure_flag: Some(failure_flag),
            cancellation: None,
            execution_memory_peak_bytes: None,
            _event_reservation: None,
        };

        assert_eq!(
            stream
                .next()
                .expect("stream error")
                .expect_err("error")
                .sql_state,
            "53200"
        );
        assert_eq!(session.transaction_status(), TransactionStatus::Failed);
        assert_eq!(
            session
                .execute("SELECT * FROM stream_failures", &[])
                .expect_err("failed transaction")
                .sql_state,
            "25P02"
        );
        execute(&mut session, "ROLLBACK", &[]);
        assert_eq!(session.transaction_status(), TransactionStatus::Idle);
    }

    #[test]
    fn durable_commits_trigger_the_conservative_automatic_checkpoint() {
        let (_directory, engine) = engine();
        let mut session = engine.connect().expect("connect");
        engine
            .commits_since_checkpoint
            .store(AUTOMATIC_CHECKPOINT_INTERVAL - 1, Ordering::Release);
        execute(
            &mut session,
            "CREATE TABLE checkpoint_rows (id BIGINT PRIMARY KEY)",
            &[],
        );

        let records = engine.wal.scan().expect("scan WAL").records;
        assert!(
            records
                .iter()
                .any(|record| { record.kind() == ordadb_transaction::RecordKind::CheckpointEnd })
        );
        assert!(records.iter().any(|record| {
            matches!(
                record.payload(),
                ordadb_transaction::WalPayload::CheckpointBegin(checkpoint)
                    if checkpoint.visibility_horizon.is_some()
            )
        }));
        assert_eq!(
            engine.writer.active_transaction().expect("writer state"),
            None
        );
        assert_eq!(engine.commits_since_checkpoint.load(Ordering::Acquire), 0);
    }
}
