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
