use std::path::PathBuf;
use std::time::Duration;

use duroxide::providers::{
    DispatcherCapabilityFilter, ExecutionMetadata, KvEntry, OrchestrationItem, Provider,
    ProviderAdmin, ProviderError, ScheduledActivityIdentifier, SessionFetchConfig, TagFilter,
    WorkItem,
};
use duroxide::{Event, EventKind, SystemStats};
use libsql::{Value, params};

use crate::{LibsqlDatabaseConfig, LibsqlProviderInitError};

pub struct NativeLibsqlProvider {
    db: libsql::Database,
}

impl NativeLibsqlProvider {
    pub async fn new(config: LibsqlDatabaseConfig) -> Result<Self, LibsqlProviderInitError> {
        match config {
            LibsqlDatabaseConfig::InMemory => Self::new_local(":memory:").await,
            LibsqlDatabaseConfig::Local { path } => Self::new_local(path).await,
            LibsqlDatabaseConfig::Remote { url, auth_token } => {
                Self::new_remote(url, auth_token).await
            }
            LibsqlDatabaseConfig::RemoteReplica {
                local_path,
                remote_url,
                auth_token,
            } => Self::new_remote_replica(local_path, remote_url, auth_token).await,
        }
    }

    pub async fn new_local(path: impl Into<PathBuf>) -> Result<Self, LibsqlProviderInitError> {
        let path = path.into();
        if path.as_os_str() != ":memory:" {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent).map_err(|e| {
                    libsql::Error::ConnectionFailed(format!(
                        "failed to create database directory: {e}"
                    ))
                })?;
            }
        }

        let db = libsql::Builder::new_local(path).build().await?;
        let provider = Self { db };
        provider.create_schema().await?;
        Ok(provider)
    }

    pub async fn new_remote(
        url: String,
        auth_token: String,
    ) -> Result<Self, LibsqlProviderInitError> {
        let db = libsql::Builder::new_remote(url, auth_token).build().await?;
        let provider = Self { db };
        provider.create_schema().await?;
        Ok(provider)
    }

    pub async fn new_remote_replica(
        local_path: impl Into<PathBuf>,
        remote_url: String,
        auth_token: String,
    ) -> Result<Self, LibsqlProviderInitError> {
        let local_path = local_path.into();
        if let Some(parent) = local_path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(|e| {
                libsql::Error::ConnectionFailed(format!(
                    "failed to create replica database directory: {e}"
                ))
            })?;
        }

        let db = libsql::Builder::new_remote_replica(local_path, remote_url, auth_token)
            .build()
            .await?;
        let provider = Self { db };
        provider.create_schema().await?;
        Ok(provider)
    }

    pub fn database(&self) -> &libsql::Database {
        &self.db
    }

    pub async fn connect(&self) -> libsql::Result<libsql::Connection> {
        self.db.connect()
    }

    async fn create_schema(&self) -> libsql::Result<()> {
        let conn = self.db.connect()?;
        for statement in SCHEMA_STATEMENTS {
            conn.execute(statement, ()).await?;
        }
        Ok(())
    }

    fn libsql_to_provider_error(operation: &str, error: libsql::Error) -> ProviderError {
        let msg = error.to_string();
        if msg.contains("database is locked")
            || msg.contains("SQLITE_BUSY")
            || msg.contains("timeout")
            || msg.contains("connection")
            || msg.contains("Hrana")
        {
            return ProviderError::retryable(operation, msg);
        }

        if msg.contains("UNIQUE constraint")
            || msg.contains("PRIMARY KEY")
            || msg.contains("constraint")
        {
            return ProviderError::permanent(operation, msg);
        }

        ProviderError::retryable(operation, msg)
    }

    fn now_millis() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after UNIX epoch")
            .as_millis() as i64
    }

    fn timestamp_after(duration: Duration) -> i64 {
        Self::now_millis().saturating_add(duration.as_millis().min(i64::MAX as u128) as i64)
    }

    fn generate_lock_token() -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after UNIX epoch")
            .as_nanos();
        format!("lock_{now}_{}", std::process::id())
    }

    fn option_text(value: Option<&str>) -> Value {
        value
            .map(|v| Value::Text(v.to_string()))
            .unwrap_or(Value::Null)
    }

    fn option_i64(value: Option<i64>) -> Value {
        value.map(Value::Integer).unwrap_or(Value::Null)
    }

    fn event_type(event: &Event) -> &'static str {
        match &event.kind {
            EventKind::OrchestrationStarted { .. } => "OrchestrationStarted",
            EventKind::OrchestrationCompleted { .. } => "OrchestrationCompleted",
            EventKind::OrchestrationFailed { .. } => "OrchestrationFailed",
            EventKind::OrchestrationContinuedAsNew { .. } => "OrchestrationContinuedAsNew",
            EventKind::ActivityScheduled { .. } => "ActivityScheduled",
            EventKind::ActivityCompleted { .. } => "ActivityCompleted",
            EventKind::ActivityFailed { .. } => "ActivityFailed",
            EventKind::ActivityCancelRequested { .. } => "ActivityCancelRequested",
            EventKind::TimerCreated { .. } => "TimerCreated",
            EventKind::TimerFired { .. } => "TimerFired",
            EventKind::ExternalSubscribed { .. } => "ExternalSubscribed",
            EventKind::ExternalEvent { .. } => "ExternalEvent",
            EventKind::SubOrchestrationScheduled { .. } => "SubOrchestrationScheduled",
            EventKind::SubOrchestrationCompleted { .. } => "SubOrchestrationCompleted",
            EventKind::SubOrchestrationFailed { .. } => "SubOrchestrationFailed",
            EventKind::SubOrchestrationCancelRequested { .. } => "SubOrchestrationCancelRequested",
            EventKind::OrchestrationCancelRequested { .. } => "OrchestrationCancelRequested",
            EventKind::ExternalSubscribedCancelled { .. } => "ExternalSubscribedCancelled",
            EventKind::QueueSubscribed { .. } => "ExternalSubscribedPersistent",
            EventKind::QueueEventDelivered { .. } => "ExternalEventPersistent",
            EventKind::QueueSubscriptionCancelled { .. } => "ExternalSubscribedPersistentCancelled",
            EventKind::OrchestrationChained { .. } => "OrchestrationChained",
            EventKind::CustomStatusUpdated { .. } => "CustomStatusUpdated",
            EventKind::KeyValueSet { .. } => "KeyValueSet",
            EventKind::KeyValueCleared { .. } => "KeyValueCleared",
            EventKind::KeyValuesCleared => "KeyValuesCleared",
        }
    }

    fn orchestrator_instance(item: &WorkItem) -> Option<&str> {
        match item {
            WorkItem::StartOrchestration { instance, .. }
            | WorkItem::ActivityCompleted { instance, .. }
            | WorkItem::ActivityFailed { instance, .. }
            | WorkItem::TimerFired { instance, .. }
            | WorkItem::ExternalRaised { instance, .. }
            | WorkItem::QueueMessage { instance, .. }
            | WorkItem::CancelInstance { instance, .. }
            | WorkItem::ContinueAsNew { instance, .. } => Some(instance),
            WorkItem::SubOrchCompleted {
                parent_instance, ..
            }
            | WorkItem::SubOrchFailed {
                parent_instance, ..
            } => Some(parent_instance),
            WorkItem::ActivityExecute { .. } => None,
        }
    }

    fn orchestrator_visible_at(item: &WorkItem, delay: Option<Duration>) -> i64 {
        match item {
            WorkItem::TimerFired { fire_at_ms, .. } => *fire_at_ms as i64,
            _ => delay
                .map(Self::timestamp_after)
                .unwrap_or_else(Self::now_millis),
        }
    }

    fn tag_clause(filter: &TagFilter, start_param: usize) -> String {
        match filter {
            TagFilter::DefaultOnly => "q.tag IS NULL".to_string(),
            TagFilter::Tags(set) => {
                let placeholders = (0..set.len())
                    .map(|i| format!("?{}", start_param + i))
                    .collect::<Vec<_>>();
                format!("q.tag IN ({})", placeholders.join(", "))
            }
            TagFilter::DefaultAnd(set) => {
                let placeholders = (0..set.len())
                    .map(|i| format!("?{}", start_param + i))
                    .collect::<Vec<_>>();
                format!("(q.tag IS NULL OR q.tag IN ({}))", placeholders.join(", "))
            }
            TagFilter::Any => "1".to_string(),
            TagFilter::None => "0".to_string(),
        }
    }

    fn tag_values(filter: &TagFilter) -> Vec<Value> {
        match filter {
            TagFilter::Tags(set) | TagFilter::DefaultAnd(set) => {
                let mut values = set.iter().cloned().collect::<Vec<_>>();
                values.sort();
                values.into_iter().map(Value::Text).collect()
            }
            _ => Vec::new(),
        }
    }

    async fn append_history(
        conn: &libsql::Connection,
        instance: &str,
        execution_id: u64,
        events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        for event in &events {
            if event.event_id() == 0 {
                return Err(ProviderError::permanent(
                    "append_with_execution",
                    "event_id must be set by runtime",
                ));
            }

            let event_data = serde_json::to_string(event).map_err(|e| {
                ProviderError::permanent(
                    "append_with_execution",
                    format!("Event serialization error: {e}"),
                )
            })?;

            conn.execute(
                r#"
                INSERT INTO history (instance_id, execution_id, event_id, event_type, event_data)
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![
                    instance,
                    execution_id as i64,
                    event.event_id() as i64,
                    Self::event_type(event),
                    event_data
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("append_with_execution", e))?;
        }

        Ok(())
    }

    async fn read_execution_history(
        conn: &libsql::Connection,
        instance: &str,
        execution_id: u64,
        operation: &'static str,
    ) -> Result<Vec<Event>, ProviderError> {
        let mut rows = conn
            .query(
                r#"
                SELECT event_data
                FROM history
                WHERE instance_id = ?1 AND execution_id = ?2
                ORDER BY event_id
                "#,
                params![instance, execution_id as i64],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error(operation, e))?;

        let mut events = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error(operation, e))?
        {
            let event_data = row.get::<String>(0).map_err(|e| {
                ProviderError::permanent(operation, format!("Failed to get event_data: {e}"))
            })?;
            let event = serde_json::from_str(&event_data).map_err(|e| {
                ProviderError::permanent(operation, format!("Failed to deserialize event: {e}"))
            })?;
            events.push(event);
        }

        Ok(events)
    }

    async fn kv_snapshot(
        conn: &libsql::Connection,
        instance: &str,
        operation: &'static str,
    ) -> Result<std::collections::HashMap<String, KvEntry>, ProviderError> {
        let mut rows = conn
            .query(
                "SELECT key, value, last_updated_at_ms FROM kv_store WHERE instance_id = ?1",
                params![instance],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error(operation, e))?;

        let mut values = std::collections::HashMap::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error(operation, e))?
        {
            let key = row.get::<String>(0).map_err(|e| {
                ProviderError::permanent(operation, format!("Failed to get key: {e}"))
            })?;
            let value = row.get::<String>(1).map_err(|e| {
                ProviderError::permanent(operation, format!("Failed to get value: {e}"))
            })?;
            let last_updated_at_ms = row.get::<i64>(2).map_err(|e| {
                ProviderError::permanent(
                    operation,
                    format!("Failed to get last_updated_at_ms: {e}"),
                )
            })?;
            values.insert(
                key,
                KvEntry {
                    value,
                    last_updated_at_ms: last_updated_at_ms as u64,
                },
            );
        }

        Ok(values)
    }
}

const SCHEMA_STATEMENTS: &[&str] = &[
    r#"
    CREATE TABLE IF NOT EXISTS instances (
        instance_id TEXT PRIMARY KEY,
        orchestration_name TEXT NOT NULL,
        orchestration_version TEXT,
        current_execution_id INTEGER NOT NULL DEFAULT 1,
        custom_status TEXT,
        custom_status_version INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        parent_instance_id TEXT REFERENCES instances(instance_id)
    )
    "#,
    "CREATE INDEX IF NOT EXISTS idx_instances_parent ON instances(parent_instance_id)",
    r#"
    CREATE TABLE IF NOT EXISTS executions (
        instance_id TEXT NOT NULL,
        execution_id INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'Running',
        output TEXT,
        duroxide_version_major INTEGER,
        duroxide_version_minor INTEGER,
        duroxide_version_patch INTEGER,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        PRIMARY KEY (instance_id, execution_id)
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS history (
        instance_id TEXT NOT NULL,
        execution_id INTEGER NOT NULL,
        event_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        event_data TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (instance_id, execution_id, event_id)
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS orchestrator_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instance_id TEXT NOT NULL,
        work_item TEXT NOT NULL,
        visible_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        lock_token TEXT,
        locked_until TIMESTAMP,
        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS worker_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        work_item TEXT NOT NULL,
        visible_at INTEGER NOT NULL DEFAULT 0,
        lock_token TEXT,
        locked_until TIMESTAMP,
        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        instance_id TEXT,
        execution_id TEXT,
        activity_id INTEGER,
        session_id TEXT,
        tag TEXT
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS instance_locks (
        instance_id TEXT PRIMARY KEY,
        lock_token TEXT NOT NULL,
        locked_until INTEGER NOT NULL,
        locked_at INTEGER NOT NULL
    )
    "#,
    "CREATE INDEX IF NOT EXISTS idx_orch_visible ON orchestrator_queue(visible_at, lock_token)",
    "CREATE INDEX IF NOT EXISTS idx_orch_instance ON orchestrator_queue(instance_id)",
    "CREATE INDEX IF NOT EXISTS idx_orch_lock ON orchestrator_queue(lock_token)",
    "CREATE INDEX IF NOT EXISTS idx_worker_available ON worker_queue(lock_token, id)",
    "CREATE INDEX IF NOT EXISTS idx_worker_identity ON worker_queue(instance_id, execution_id, activity_id)",
    "CREATE INDEX IF NOT EXISTS idx_worker_queue_session ON worker_queue(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_worker_queue_tag ON worker_queue(tag)",
    r#"
    CREATE TABLE IF NOT EXISTS sessions (
        session_id TEXT PRIMARY KEY,
        worker_id TEXT NOT NULL,
        locked_until INTEGER NOT NULL,
        last_activity_at INTEGER NOT NULL
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS kv_store (
        instance_id TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        last_updated_at_ms INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (instance_id, key)
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS kv_delta (
        instance_id TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT,
        last_updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (instance_id, key)
    )
    "#,
];

#[async_trait::async_trait]
impl Provider for NativeLibsqlProvider {
    fn name(&self) -> &str {
        "libsql-native"
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        _poll_timeout: Duration,
        filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
        let tx = conn
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
        let now_ms = Self::now_millis();

        let candidate = if let Some(cap_filter) = filter {
            let Some(range) = cap_filter.supported_duroxide_versions.first() else {
                tx.commit()
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
                return Ok(None);
            };
            let min_packed = range.min.major as i64 * 1_000_000
                + range.min.minor as i64 * 1_000
                + range.min.patch as i64;
            let max_packed = range.max.major as i64 * 1_000_000
                + range.max.minor as i64 * 1_000
                + range.max.patch as i64;
            let mut rows = tx
                .query(
                    r#"
                    SELECT q.instance_id
                    FROM orchestrator_queue q
                    LEFT JOIN instance_locks il ON q.instance_id = il.instance_id
                    LEFT JOIN instances i ON q.instance_id = i.instance_id
                    LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
                    WHERE q.visible_at <= ?1
                      AND (il.instance_id IS NULL OR il.locked_until <= ?1)
                      AND (
                        e.duroxide_version_major IS NULL
                        OR (e.duroxide_version_major * 1000000 + e.duroxide_version_minor * 1000 + e.duroxide_version_patch) BETWEEN ?2 AND ?3
                      )
                    ORDER BY q.id
                    LIMIT 1
                    "#,
                    params![now_ms, min_packed, max_packed],
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
            let candidate = rows
                .next()
                .await
                .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?
                .map(|row| row.get::<String>(0))
                .transpose()
                .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
            drop(rows);
            candidate
        } else {
            let mut rows = tx
                .query(
                    r#"
                    SELECT q.instance_id
                    FROM orchestrator_queue q
                    LEFT JOIN instance_locks il ON q.instance_id = il.instance_id
                    WHERE q.visible_at <= ?1
                      AND (il.instance_id IS NULL OR il.locked_until <= ?1)
                    ORDER BY q.id
                    LIMIT 1
                    "#,
                    params![now_ms],
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
            let candidate = rows
                .next()
                .await
                .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?
                .map(|row| row.get::<String>(0))
                .transpose()
                .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
            drop(rows);
            candidate
        };

        let Some(instance_id) = candidate else {
            tx.commit()
                .await
                .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
            return Ok(None);
        };

        let lock_token = Self::generate_lock_token();
        let locked_until = Self::timestamp_after(lock_timeout);
        let changed = tx
            .execute(
                r#"
                INSERT INTO instance_locks (instance_id, lock_token, locked_until, locked_at)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(instance_id) DO UPDATE
                SET lock_token = ?2, locked_until = ?3, locked_at = ?4
                WHERE locked_until <= ?4
                "#,
                params![
                    instance_id.as_str(),
                    lock_token.as_str(),
                    locked_until,
                    now_ms
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
        if changed == 0 {
            tx.rollback().await.ok();
            return Ok(None);
        }

        tx.execute(
            r#"
            UPDATE orchestrator_queue
            SET lock_token = ?1, attempt_count = attempt_count + 1
            WHERE instance_id = ?2 AND visible_at <= ?3
            "#,
            params![lock_token.as_str(), instance_id.as_str(), now_ms],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;

        let mut message_rows = tx
            .query(
                r#"
                SELECT work_item, attempt_count
                FROM orchestrator_queue
                WHERE lock_token = ?1
                ORDER BY id
                "#,
                params![lock_token.as_str()],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
        let mut messages = Vec::new();
        let mut max_attempt_count = 0u32;
        while let Some(row) = message_rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?
        {
            let work_item_str = row.get::<String>(0).map_err(|e| {
                ProviderError::permanent(
                    "fetch_orchestration_item",
                    format!("Failed to get work_item: {e}"),
                )
            })?;
            let attempt_count = row.get::<i64>(1).map_err(|e| {
                ProviderError::permanent(
                    "fetch_orchestration_item",
                    format!("Failed to get attempt_count: {e}"),
                )
            })?;
            max_attempt_count = max_attempt_count.max(attempt_count as u32);
            if let Ok(item) = serde_json::from_str(&work_item_str) {
                messages.push(item);
            }
        }
        drop(message_rows);

        if messages.is_empty() {
            tx.execute(
                "DELETE FROM instance_locks WHERE instance_id = ?1 AND lock_token = ?2",
                params![instance_id.as_str(), lock_token.as_str()],
            )
            .await
            .ok();
            tx.rollback().await.ok();
            return Ok(None);
        }

        let mut info_rows = tx
            .query(
                r#"
                SELECT orchestration_name, orchestration_version, current_execution_id
                FROM instances
                WHERE instance_id = ?1
                "#,
                params![instance_id.as_str()],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
        let info = info_rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
        drop(info_rows);

        let (orchestration_name, orchestration_version, execution_id, history) =
            if let Some(info) = info {
                let name = info.get::<String>(0).map_err(|e| {
                    ProviderError::permanent(
                        "fetch_orchestration_item",
                        format!("Failed to get orchestration_name: {e}"),
                    )
                })?;
                let version = match info.get_value(1).map_err(|e| {
                    ProviderError::permanent(
                        "fetch_orchestration_item",
                        format!("Failed to get orchestration_version: {e}"),
                    )
                })? {
                    Value::Text(v) => v,
                    Value::Null => "unknown".to_string(),
                    _ => "unknown".to_string(),
                };
                let exec_id = info.get::<i64>(2).map_err(|e| {
                    ProviderError::permanent(
                        "fetch_orchestration_item",
                        format!("Failed to get current_execution_id: {e}"),
                    )
                })?;
                match Self::read_execution_history(
                    &tx,
                    &instance_id,
                    exec_id as u64,
                    "fetch_orchestration_item",
                )
                .await
                {
                    Ok(history) => (name, version, exec_id as u64, history),
                    Err(error) => {
                        let error_msg = format!("Failed to deserialize history: {error}");
                        tx.commit().await.map_err(|e| {
                            Self::libsql_to_provider_error("fetch_orchestration_item", e)
                        })?;
                        return Ok(Some((
                            OrchestrationItem {
                                instance: instance_id,
                                orchestration_name: name,
                                execution_id: exec_id as u64,
                                version,
                                history: Vec::new(),
                                messages,
                                history_error: Some(error_msg),
                                kv_snapshot: std::collections::HashMap::new(),
                            },
                            lock_token,
                            max_attempt_count,
                        )));
                    }
                }
            } else {
                let mut exec_rows = tx
                    .query(
                        "SELECT MAX(execution_id) FROM history WHERE instance_id = ?1",
                        params![instance_id.as_str()],
                    )
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
                let history_execution_id = exec_rows
                    .next()
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?
                    .and_then(|row| match row.get_value(0).ok()? {
                        Value::Integer(value) => Some(value as u64),
                        _ => None,
                    });
                drop(exec_rows);

                let history = if let Some(history_execution_id) = history_execution_id {
                    Self::read_execution_history(
                        &tx,
                        &instance_id,
                        history_execution_id,
                        "fetch_orchestration_item",
                    )
                    .await
                    .unwrap_or_default()
                } else {
                    Vec::new()
                };

                if let Some((name, version)) = history.iter().find_map(|event| {
                    if let EventKind::OrchestrationStarted { name, version, .. } = &event.kind {
                        Some((name.clone(), version.clone()))
                    } else {
                        None
                    }
                }) {
                    (name, version, 1, history)
                } else if let Some(start_item) = messages.iter().find(|item| {
                    matches!(
                        item,
                        WorkItem::StartOrchestration { .. } | WorkItem::ContinueAsNew { .. }
                    )
                }) {
                    let (orchestration, version) = match start_item {
                        WorkItem::StartOrchestration {
                            orchestration,
                            version,
                            ..
                        }
                        | WorkItem::ContinueAsNew {
                            orchestration,
                            version,
                            ..
                        } => (
                            orchestration.clone(),
                            version.clone().unwrap_or_else(|| "unknown".to_string()),
                        ),
                        _ => unreachable!(),
                    };
                    (orchestration, version, 1, Vec::new())
                } else if messages
                    .iter()
                    .all(|item| matches!(item, WorkItem::QueueMessage { .. }))
                {
                    tx.execute(
                        "DELETE FROM orchestrator_queue WHERE lock_token = ?1",
                        params![lock_token.as_str()],
                    )
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
                    tx.execute(
                        "DELETE FROM instance_locks WHERE lock_token = ?1",
                        params![lock_token.as_str()],
                    )
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;
                    tx.commit().await.map_err(|e| {
                        Self::libsql_to_provider_error("fetch_orchestration_item", e)
                    })?;
                    return Ok(None);
                } else {
                    tx.rollback().await.ok();
                    return Ok(None);
                }
            };

        let kv_snapshot = Self::kv_snapshot(&tx, &instance_id, "fetch_orchestration_item").await?;
        tx.commit()
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_orchestration_item", e))?;

        Ok(Some((
            OrchestrationItem {
                instance: instance_id,
                orchestration_name,
                execution_id,
                version: orchestration_version,
                history,
                messages,
                history_error: None,
                kv_snapshot,
            },
            lock_token,
            max_attempt_count,
        )))
    }

    async fn ack_orchestration_item(
        &self,
        lock_token: &str,
        execution_id: u64,
        history_delta: Vec<Event>,
        worker_items: Vec<WorkItem>,
        orchestrator_items: Vec<WorkItem>,
        metadata: ExecutionMetadata,
        cancelled_activities: Vec<ScheduledActivityIdentifier>,
    ) -> Result<(), ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        let tx = conn
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;

        let mut rows = tx
            .query(
                "SELECT instance_id FROM instance_locks WHERE lock_token = ?1",
                params![lock_token],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        let instance_id = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?
            .map(|row| row.get::<String>(0))
            .transpose()
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?
            .ok_or_else(|| {
                ProviderError::permanent("ack_orchestration_item", "Invalid lock token")
            })?;
        drop(rows);

        tx.execute(
            "DELETE FROM orchestrator_queue WHERE lock_token = ?1",
            params![lock_token],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;

        if let (Some(name), Some(version)) = (
            &metadata.orchestration_name,
            &metadata.orchestration_version,
        ) {
            tx.execute(
                r#"
                INSERT OR IGNORE INTO instances
                (instance_id, orchestration_name, orchestration_version, current_execution_id, parent_instance_id)
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![
                    instance_id.as_str(),
                    name.as_str(),
                    version.as_str(),
                    execution_id as i64,
                    Self::option_text(metadata.parent_instance_id.as_deref())
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;

            tx.execute(
                r#"
                UPDATE instances
                SET orchestration_name = ?1, orchestration_version = ?2
                WHERE instance_id = ?3
                "#,
                params![name.as_str(), version.as_str(), instance_id.as_str()],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        }

        tx.execute(
            r#"
            INSERT OR IGNORE INTO executions (instance_id, execution_id, status)
            VALUES (?1, ?2, 'Running')
            "#,
            params![instance_id.as_str(), execution_id as i64],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;

        if let Some(pinned) = &metadata.pinned_duroxide_version {
            tx.execute(
                r#"
                UPDATE executions
                SET duroxide_version_major = ?1, duroxide_version_minor = ?2, duroxide_version_patch = ?3
                WHERE instance_id = ?4 AND execution_id = ?5
                "#,
                params![
                    pinned.major as i64,
                    pinned.minor as i64,
                    pinned.patch as i64,
                    instance_id.as_str(),
                    execution_id as i64
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        }

        tx.execute(
            r#"
            UPDATE instances
            SET current_execution_id = MAX(current_execution_id, ?1)
            WHERE instance_id = ?2
            "#,
            params![execution_id as i64, instance_id.as_str()],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;

        if !history_delta.is_empty() {
            Self::append_history(&tx, &instance_id, execution_id, history_delta.clone()).await?;
        }

        if let Some(status) = &metadata.status {
            tx.execute(
                r#"
                UPDATE executions
                SET status = ?1, output = ?2, completed_at = ?3
                WHERE instance_id = ?4 AND execution_id = ?5
                "#,
                params![
                    status.as_str(),
                    Self::option_text(metadata.output.as_deref()),
                    Self::now_millis(),
                    instance_id.as_str(),
                    execution_id as i64
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        }

        match history_delta
            .iter()
            .rev()
            .find_map(|event| match &event.kind {
                EventKind::CustomStatusUpdated { status } => Some(status.clone()),
                _ => None,
            }) {
            Some(Some(custom_status)) => {
                tx.execute(
                    r#"
                    UPDATE instances
                    SET custom_status = ?1, custom_status_version = custom_status_version + 1
                    WHERE instance_id = ?2
                    "#,
                    params![custom_status, instance_id.as_str()],
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
            }
            Some(None) => {
                tx.execute(
                    r#"
                    UPDATE instances
                    SET custom_status = NULL, custom_status_version = custom_status_version + 1
                    WHERE instance_id = ?1
                    "#,
                    params![instance_id.as_str()],
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
            }
            None => {}
        }

        for event in &history_delta {
            match &event.kind {
                EventKind::KeyValueSet {
                    key,
                    value,
                    last_updated_at_ms,
                } => {
                    tx.execute(
                        r#"
                        INSERT INTO kv_delta (instance_id, key, value, last_updated_at_ms)
                        VALUES (?1, ?2, ?3, ?4)
                        ON CONFLICT(instance_id, key)
                        DO UPDATE SET value = excluded.value, last_updated_at_ms = excluded.last_updated_at_ms
                        "#,
                        params![
                            instance_id.as_str(),
                            key.as_str(),
                            value.as_str(),
                            *last_updated_at_ms as i64
                        ],
                    )
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
                }
                EventKind::KeyValueCleared { key } => {
                    tx.execute(
                        r#"
                        INSERT INTO kv_delta (instance_id, key, value, last_updated_at_ms)
                        VALUES (?1, ?2, NULL, ?3)
                        ON CONFLICT(instance_id, key)
                        DO UPDATE SET value = NULL, last_updated_at_ms = excluded.last_updated_at_ms
                        "#,
                        params![instance_id.as_str(), key.as_str(), Self::now_millis()],
                    )
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
                }
                EventKind::KeyValuesCleared => {
                    let clear_ts = Self::now_millis();
                    tx.execute(
                        "UPDATE kv_delta SET value = NULL, last_updated_at_ms = ?1 WHERE instance_id = ?2",
                        params![clear_ts, instance_id.as_str()],
                    )
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
                    tx.execute(
                        r#"
                        INSERT OR IGNORE INTO kv_delta (instance_id, key, value, last_updated_at_ms)
                        SELECT instance_id, key, NULL, ?1 FROM kv_store WHERE instance_id = ?2
                        "#,
                        params![clear_ts, instance_id.as_str()],
                    )
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
                }
                _ => {}
            }
        }

        let is_terminal = metadata
            .status
            .as_deref()
            .is_some_and(|status| matches!(status, "Completed" | "ContinuedAsNew" | "Failed"));
        if is_terminal {
            tx.execute(
                r#"
                INSERT INTO kv_store (instance_id, key, value, last_updated_at_ms)
                SELECT instance_id, key, value, last_updated_at_ms
                FROM kv_delta
                WHERE instance_id = ?1 AND value IS NOT NULL
                ON CONFLICT(instance_id, key)
                DO UPDATE SET value = excluded.value, last_updated_at_ms = excluded.last_updated_at_ms
                "#,
                params![instance_id.as_str()],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
            tx.execute(
                r#"
                DELETE FROM kv_store
                WHERE instance_id = ?1
                AND key IN (SELECT key FROM kv_delta WHERE instance_id = ?2 AND value IS NULL)
                "#,
                params![instance_id.as_str(), instance_id.as_str()],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
            tx.execute(
                "DELETE FROM kv_delta WHERE instance_id = ?1",
                params![instance_id.as_str()],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        }

        let now_ms = Self::now_millis();
        for item in worker_items {
            let (activity_instance, activity_execution_id, activity_id, session_id, tag) =
                match &item {
                    WorkItem::ActivityExecute {
                        instance,
                        execution_id,
                        id,
                        session_id,
                        tag,
                        ..
                    } => (
                        Some(instance.as_str()),
                        Some(*execution_id as i64),
                        Some(*id as i64),
                        session_id.as_deref(),
                        tag.as_deref(),
                    ),
                    _ => (None, None, None, None, None),
                };
            let work_item = serde_json::to_string(&item).map_err(|e| {
                ProviderError::permanent(
                    "ack_orchestration_item",
                    format!("Serialization error: {e}"),
                )
            })?;
            tx.execute(
                r#"
                INSERT INTO worker_queue (work_item, visible_at, instance_id, execution_id, activity_id, session_id, tag)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    work_item,
                    now_ms,
                    Self::option_text(activity_instance),
                    Self::option_i64(activity_execution_id),
                    Self::option_i64(activity_id),
                    Self::option_text(session_id),
                    Self::option_text(tag)
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        }

        if !cancelled_activities.is_empty() {
            let placeholders = cancelled_activities
                .iter()
                .enumerate()
                .map(|(idx, _)| format!("(?{}, ?{}, ?{})", idx * 3 + 1, idx * 3 + 2, idx * 3 + 3))
                .collect::<Vec<_>>();
            let sql = format!(
                "DELETE FROM worker_queue WHERE (instance_id, execution_id, activity_id) IN (VALUES {})",
                placeholders.join(", ")
            );
            let mut values = Vec::with_capacity(cancelled_activities.len() * 3);
            for activity in &cancelled_activities {
                values.push(Value::Text(activity.instance.clone()));
                values.push(Value::Integer(activity.execution_id as i64));
                values.push(Value::Integer(activity.activity_id as i64));
            }
            tx.execute(&sql, values)
                .await
                .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        }

        for item in orchestrator_items {
            let Some(instance) = Self::orchestrator_instance(&item) else {
                continue;
            };
            let work_item = serde_json::to_string(&item).map_err(|e| {
                ProviderError::permanent(
                    "ack_orchestration_item",
                    format!("Serialization error: {e}"),
                )
            })?;
            tx.execute(
                "INSERT INTO orchestrator_queue (instance_id, work_item, visible_at) VALUES (?1, ?2, ?3)",
                params![
                    instance,
                    work_item,
                    Self::orchestrator_visible_at(&item, None)
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        }

        let now_ms = Self::now_millis();
        let mut rows = tx
            .query(
                r#"
                SELECT COUNT(*) FROM instance_locks
                WHERE instance_id = ?1 AND lock_token = ?2 AND locked_until > ?3
                "#,
                params![instance_id.as_str(), lock_token, now_ms],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        let lock_valid = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?
            .map(|row| row.get::<i64>(0))
            .transpose()
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?
            .unwrap_or(0);
        drop(rows);

        if lock_valid == 0 {
            tx.rollback().await.ok();
            return Err(ProviderError::permanent(
                "ack_orchestration_item",
                "Instance lock expired",
            ));
        }

        tx.execute(
            "DELETE FROM instance_locks WHERE instance_id = ?1 AND lock_token = ?2",
            params![instance_id.as_str(), lock_token],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;

        tx.commit()
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_orchestration_item", e))?;
        Ok(())
    }

    async fn abandon_orchestration_item(
        &self,
        lock_token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("abandon_orchestration_item", e))?;
        let tx = conn
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|e| Self::libsql_to_provider_error("abandon_orchestration_item", e))?;

        let mut rows = tx
            .query(
                "SELECT instance_id FROM instance_locks WHERE lock_token = ?1",
                params![lock_token],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("abandon_orchestration_item", e))?;
        let instance = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("abandon_orchestration_item", e))?
            .map(|row| row.get::<String>(0))
            .transpose()
            .map_err(|e| Self::libsql_to_provider_error("abandon_orchestration_item", e))?;
        drop(rows);

        if instance.is_none() {
            tx.rollback().await.ok();
            return Err(ProviderError::permanent(
                "abandon_orchestration_item",
                "Invalid lock token",
            ));
        }

        let visible_at = delay
            .map(Self::timestamp_after)
            .unwrap_or_else(Self::now_millis);
        tx.execute(
            r#"
            UPDATE orchestrator_queue
            SET lock_token = NULL,
                locked_until = NULL,
                visible_at = ?1,
                attempt_count = CASE WHEN ?2 AND attempt_count > 0 THEN attempt_count - 1 ELSE attempt_count END
            WHERE lock_token = ?3
            "#,
            params![visible_at, if ignore_attempt { 1 } else { 0 }, lock_token],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("abandon_orchestration_item", e))?;
        tx.execute(
            "DELETE FROM instance_locks WHERE lock_token = ?1",
            params![lock_token],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("abandon_orchestration_item", e))?;

        tx.commit()
            .await
            .map_err(|e| Self::libsql_to_provider_error("abandon_orchestration_item", e))?;
        Ok(())
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("read", e))?;
        let mut rows = conn
            .query(
                "SELECT COALESCE(MAX(execution_id), 1) FROM executions WHERE instance_id = ?1",
                params![instance],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("read", e))?;
        let execution_id = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("read", e))?
            .map(|row| row.get::<i64>(0))
            .transpose()
            .map_err(|e| Self::libsql_to_provider_error("read", e))?
            .unwrap_or(1);

        Self::read_execution_history(&conn, instance, execution_id as u64, "read").await
    }

    async fn read_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("read_with_execution", e))?;
        Self::read_execution_history(&conn, instance, execution_id, "read_with_execution").await
    }

    async fn append_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
        new_events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("append_with_execution", e))?;
        let tx = conn
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|e| Self::libsql_to_provider_error("append_with_execution", e))?;

        Self::append_history(&tx, instance, execution_id, new_events).await?;

        tx.commit()
            .await
            .map_err(|e| Self::libsql_to_provider_error("append_with_execution", e))?;
        Ok(())
    }

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        let (activity_instance, activity_execution_id, activity_id, session_id, tag) = match &item {
            WorkItem::ActivityExecute {
                instance,
                execution_id,
                id,
                session_id,
                tag,
                ..
            } => (
                Some(instance.as_str()),
                Some(*execution_id as i64),
                Some(*id as i64),
                session_id.as_deref(),
                tag.as_deref(),
            ),
            _ => (None, None, None, None, None),
        };
        let work_item = serde_json::to_string(&item).map_err(|e| {
            ProviderError::permanent("enqueue_for_worker", format!("Serialization error: {e}"))
        })?;
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("enqueue_for_worker", e))?;
        conn.execute(
            r#"
            INSERT INTO worker_queue (work_item, visible_at, instance_id, execution_id, activity_id, session_id, tag)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                work_item,
                Self::now_millis(),
                Self::option_text(activity_instance),
                Self::option_i64(activity_execution_id),
                Self::option_i64(activity_id),
                Self::option_text(session_id),
                Self::option_text(tag)
            ],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("enqueue_for_worker", e))?;
        Ok(())
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        _poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        if matches!(tag_filter, TagFilter::None) {
            return Ok(None);
        }

        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("fetch_work_item", e))?;
        let tx = conn
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_work_item", e))?;
        let now_ms = Self::now_millis();
        let lock_token = Self::generate_lock_token();
        let locked_until = Self::timestamp_after(lock_timeout);

        let tag_start_param = if session.is_some() { 3 } else { 2 };
        let tag_clause = Self::tag_clause(tag_filter, tag_start_param);
        let tag_values = Self::tag_values(tag_filter);

        let mut params_vec = vec![Value::Integer(now_ms)];
        let sql = if let Some(config) = session {
            params_vec.push(Value::Text(config.owner_id.clone()));
            params_vec.extend(tag_values);
            format!(
                r#"
                SELECT q.id, q.work_item, q.attempt_count, q.session_id
                FROM worker_queue q
                LEFT JOIN sessions s ON s.session_id = q.session_id AND s.locked_until > ?1
                WHERE q.visible_at <= ?1
                  AND (q.lock_token IS NULL OR q.locked_until <= ?1)
                  AND (
                    q.session_id IS NULL
                    OR s.worker_id = ?2
                    OR s.session_id IS NULL
                  )
                  AND ({tag_clause})
                ORDER BY q.id
                LIMIT 1
                "#
            )
        } else {
            params_vec.extend(tag_values);
            format!(
                r#"
                SELECT q.id, q.work_item, q.attempt_count, q.session_id
                FROM worker_queue q
                WHERE q.visible_at <= ?1
                  AND (q.lock_token IS NULL OR q.locked_until <= ?1)
                  AND q.session_id IS NULL
                  AND ({tag_clause})
                ORDER BY q.id
                LIMIT 1
                "#
            )
        };

        let mut rows = tx
            .query(&sql, params_vec)
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_work_item", e))?;
        let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_work_item", e))?
        else {
            tx.commit()
                .await
                .map_err(|e| Self::libsql_to_provider_error("fetch_work_item", e))?;
            return Ok(None);
        };

        let id = row.get::<i64>(0).map_err(|e| {
            ProviderError::permanent("fetch_work_item", format!("Failed to get id: {e}"))
        })?;
        let work_item_str = row.get::<String>(1).map_err(|e| {
            ProviderError::permanent("fetch_work_item", format!("Failed to get work_item: {e}"))
        })?;
        let current_attempt_count = row.get::<i64>(2).map_err(|e| {
            ProviderError::permanent(
                "fetch_work_item",
                format!("Failed to get attempt_count: {e}"),
            )
        })?;
        let session_id = match row.get_value(3).map_err(|e| {
            ProviderError::permanent("fetch_work_item", format!("Failed to get session_id: {e}"))
        })? {
            Value::Text(s) => Some(s),
            Value::Null => None,
            _ => None,
        };
        drop(rows);

        tx.execute(
            r#"
            UPDATE worker_queue
            SET lock_token = ?1, locked_until = ?2, attempt_count = attempt_count + 1
            WHERE id = ?3
            "#,
            params![lock_token.as_str(), locked_until, id],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("fetch_work_item", e))?;

        if let (Some(sid), Some(config)) = (&session_id, session) {
            let session_locked_until = Self::timestamp_after(config.lock_timeout);
            tx.execute(
                r#"
                INSERT INTO sessions (session_id, worker_id, locked_until, last_activity_at)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(session_id) DO UPDATE SET
                    worker_id = excluded.worker_id,
                    locked_until = excluded.locked_until,
                    last_activity_at = excluded.last_activity_at
                WHERE sessions.locked_until <= ?4 OR sessions.worker_id = ?2
                "#,
                params![
                    sid.as_str(),
                    config.owner_id.as_str(),
                    session_locked_until,
                    now_ms
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_work_item", e))?;
        }

        tx.commit()
            .await
            .map_err(|e| Self::libsql_to_provider_error("fetch_work_item", e))?;

        let work_item = serde_json::from_str(&work_item_str).map_err(|e| {
            ProviderError::permanent("fetch_work_item", format!("Deserialization error: {e}"))
        })?;
        Ok(Some((
            work_item,
            lock_token,
            (current_attempt_count + 1) as u32,
        )))
    }

    async fn ack_work_item(
        &self,
        token: &str,
        completion: Option<WorkItem>,
    ) -> Result<(), ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("ack_work_item", e))?;
        let tx = conn
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_work_item", e))?;
        let now_ms = Self::now_millis();

        let changed = tx
            .execute(
                "DELETE FROM worker_queue WHERE lock_token = ?1 AND locked_until > ?2",
                params![token, now_ms],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_work_item", e))?;
        if changed == 0 {
            tx.rollback().await.ok();
            return Err(ProviderError::permanent(
                "ack_work_item",
                "Work item not found, lock expired, or already acknowledged",
            ));
        }

        if let Some(completion) = completion {
            let instance = Self::orchestrator_instance(&completion).ok_or_else(|| {
                ProviderError::permanent(
                    "ack_work_item",
                    "completion is not an orchestrator work item",
                )
            })?;
            let work_item = serde_json::to_string(&completion).map_err(|e| {
                ProviderError::permanent("ack_work_item", format!("Serialization error: {e}"))
            })?;
            tx.execute(
                "INSERT INTO orchestrator_queue (instance_id, work_item, visible_at) VALUES (?1, ?2, ?3)",
                params![instance, work_item, Self::orchestrator_visible_at(&completion, None)],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_work_item", e))?;
        }

        tx.commit()
            .await
            .map_err(|e| Self::libsql_to_provider_error("ack_work_item", e))?;
        Ok(())
    }

    async fn renew_work_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("renew_work_item_lock", e))?;
        let now_ms = Self::now_millis();
        let changed = conn
            .execute(
                "UPDATE worker_queue SET locked_until = ?1 WHERE lock_token = ?2 AND locked_until > ?3",
                params![Self::timestamp_after(extend_for), token, now_ms],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("renew_work_item_lock", e))?;
        if changed == 0 {
            return Err(ProviderError::permanent(
                "renew_work_item_lock",
                "Lock token invalid, expired, or entry removed",
            ));
        }
        Ok(())
    }

    async fn renew_session_lock(
        &self,
        owner_ids: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        if owner_ids.is_empty() {
            return Ok(0);
        }

        let now_ms = Self::now_millis();
        let locked_until = Self::timestamp_after(extend_for);
        let idle_cutoff =
            now_ms.saturating_sub(idle_timeout.as_millis().min(i64::MAX as u128) as i64);
        let placeholders = (0..owner_ids.len())
            .map(|i| format!("?{}", i + 3))
            .collect::<Vec<_>>();
        let sql = format!(
            "UPDATE sessions SET locked_until = ?1 \
             WHERE worker_id IN ({}) \
             AND locked_until > ?2 \
             AND last_activity_at > ?{}",
            placeholders.join(", "),
            owner_ids.len() + 3,
        );
        let mut values = vec![Value::Integer(locked_until), Value::Integer(now_ms)];
        values.extend(owner_ids.iter().map(|id| Value::Text((*id).to_string())));
        values.push(Value::Integer(idle_cutoff));

        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("renew_session_lock", e))?;
        let changed = conn
            .execute(&sql, values)
            .await
            .map_err(|e| Self::libsql_to_provider_error("renew_session_lock", e))?;
        Ok(changed as usize)
    }

    async fn cleanup_orphaned_sessions(
        &self,
        _idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("cleanup_orphaned_sessions", e))?;
        let changed = conn
            .execute(
                r#"
                DELETE FROM sessions
                WHERE locked_until < ?1
                  AND NOT EXISTS (
                      SELECT 1 FROM worker_queue WHERE worker_queue.session_id = sessions.session_id
                  )
                "#,
                params![Self::now_millis()],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("cleanup_orphaned_sessions", e))?;
        Ok(changed as usize)
    }

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("abandon_work_item", e))?;
        let visible_at = delay
            .map(Self::timestamp_after)
            .unwrap_or_else(Self::now_millis);
        let changed = conn
            .execute(
                r#"
                UPDATE worker_queue
                SET lock_token = NULL,
                    locked_until = NULL,
                    visible_at = ?1,
                    attempt_count = CASE WHEN ?2 AND attempt_count > 0 THEN attempt_count - 1 ELSE attempt_count END
                WHERE lock_token = ?3
                "#,
                params![visible_at, if ignore_attempt { 1 } else { 0 }, token],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("abandon_work_item", e))?;
        if changed == 0 {
            return Err(ProviderError::permanent(
                "abandon_work_item",
                "Invalid lock token",
            ));
        }
        Ok(())
    }

    async fn renew_orchestration_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("renew_orchestration_item_lock", e))?;
        let now_ms = Self::now_millis();
        let changed = conn
            .execute(
                "UPDATE instance_locks SET locked_until = ?1 WHERE lock_token = ?2 AND locked_until > ?3",
                params![Self::timestamp_after(extend_for), token, now_ms],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("renew_orchestration_item_lock", e))?;
        if changed == 0 {
            return Err(ProviderError::permanent(
                "renew_orchestration_item_lock",
                "Lock token invalid or expired",
            ));
        }
        Ok(())
    }

    async fn enqueue_for_orchestrator(
        &self,
        item: WorkItem,
        delay: Option<Duration>,
    ) -> Result<(), ProviderError> {
        let instance = Self::orchestrator_instance(&item).ok_or_else(|| {
            ProviderError::permanent("enqueue_for_orchestrator", "Invalid work item type")
        })?;
        let work_item = serde_json::to_string(&item).map_err(|e| {
            ProviderError::permanent(
                "enqueue_for_orchestrator",
                format!("Serialization error: {e}"),
            )
        })?;
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("enqueue_for_orchestrator", e))?;
        conn.execute(
            "INSERT INTO orchestrator_queue (instance_id, work_item, visible_at) VALUES (?1, ?2, ?3)",
            params![instance, work_item, Self::orchestrator_visible_at(&item, delay)],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("enqueue_for_orchestrator", e))?;
        Ok(())
    }

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        None
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("get_custom_status", e))?;
        let mut rows = conn
            .query(
                r#"
                SELECT custom_status, custom_status_version
                FROM instances
                WHERE instance_id = ?1 AND custom_status_version > ?2
                "#,
                params![instance, last_seen_version as i64],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_custom_status", e))?;
        let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_custom_status", e))?
        else {
            return Ok(None);
        };
        let status = match row.get_value(0).map_err(|e| {
            ProviderError::permanent(
                "get_custom_status",
                format!("Failed to get custom_status: {e}"),
            )
        })? {
            Value::Text(value) => Some(value),
            Value::Null => None,
            _ => None,
        };
        let version = row.get::<i64>(1).map_err(|e| {
            ProviderError::permanent(
                "get_custom_status",
                format!("Failed to get custom_status_version: {e}"),
            )
        })?;
        Ok(Some((status, version as u64)))
    }

    async fn get_kv_value(
        &self,
        instance: &str,
        key: &str,
    ) -> Result<Option<String>, ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("get_kv_value", e))?;

        let mut delta_rows = conn
            .query(
                "SELECT value FROM kv_delta WHERE instance_id = ?1 AND key = ?2",
                params![instance, key],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_kv_value", e))?;
        if let Some(row) = delta_rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_kv_value", e))?
        {
            return match row.get_value(0).map_err(|e| {
                ProviderError::permanent("get_kv_value", format!("Failed to get delta value: {e}"))
            })? {
                Value::Text(value) => Ok(Some(value)),
                Value::Null => Ok(None),
                _ => Ok(None),
            };
        }
        drop(delta_rows);

        let mut store_rows = conn
            .query(
                "SELECT value FROM kv_store WHERE instance_id = ?1 AND key = ?2",
                params![instance, key],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_kv_value", e))?;
        let value = store_rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_kv_value", e))?
            .map(|row| row.get::<String>(0))
            .transpose()
            .map_err(|e| Self::libsql_to_provider_error("get_kv_value", e))?;
        Ok(value)
    }

    async fn get_kv_all_values(
        &self,
        instance: &str,
    ) -> Result<std::collections::HashMap<String, String>, ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("get_kv_all_values", e))?;
        let mut map = std::collections::HashMap::new();

        let mut store_rows = conn
            .query(
                "SELECT key, value FROM kv_store WHERE instance_id = ?1",
                params![instance],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_kv_all_values", e))?;
        while let Some(row) = store_rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_kv_all_values", e))?
        {
            let key = row
                .get::<String>(0)
                .map_err(|e| Self::libsql_to_provider_error("get_kv_all_values", e))?;
            let value = row
                .get::<String>(1)
                .map_err(|e| Self::libsql_to_provider_error("get_kv_all_values", e))?;
            map.insert(key, value);
        }
        drop(store_rows);

        let mut delta_rows = conn
            .query(
                "SELECT key, value FROM kv_delta WHERE instance_id = ?1",
                params![instance],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_kv_all_values", e))?;
        while let Some(row) = delta_rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_kv_all_values", e))?
        {
            let key = row
                .get::<String>(0)
                .map_err(|e| Self::libsql_to_provider_error("get_kv_all_values", e))?;
            match row
                .get_value(1)
                .map_err(|e| Self::libsql_to_provider_error("get_kv_all_values", e))?
            {
                Value::Text(value) => {
                    map.insert(key, value);
                }
                Value::Null => {
                    map.remove(&key);
                }
                _ => {}
            }
        }

        Ok(map)
    }

    async fn get_instance_stats(
        &self,
        instance: &str,
    ) -> Result<Option<SystemStats>, ProviderError> {
        let conn = self
            .db
            .connect()
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;

        let mut rows = conn
            .query(
                "SELECT current_execution_id FROM instances WHERE instance_id = ?1",
                params![instance],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;
        let execution_id = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?
            .map(|row| row.get::<i64>(0))
            .transpose()
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;
        drop(rows);
        let Some(execution_id) = execution_id else {
            return Ok(None);
        };

        let mut rows = conn
            .query(
                r#"
                SELECT COUNT(*) as cnt, COALESCE(SUM(LENGTH(event_data)), 0) as size_bytes
                FROM history WHERE instance_id = ?1 AND execution_id = ?2
                "#,
                params![instance, execution_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;
        let row = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?
            .ok_or_else(|| {
                ProviderError::permanent("get_instance_stats", "Missing history stats row")
            })?;
        let history_event_count = row
            .get::<i64>(0)
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;
        let history_size_bytes = row
            .get::<i64>(1)
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;
        drop(rows);

        let mut rows = conn
            .query(
                r#"
                SELECT COUNT(*) as cnt, COALESCE(SUM(LENGTH(value)), 0) as size_bytes
                FROM (
                    SELECT COALESCE(d.key, s.key) AS key,
                           CASE WHEN d.key IS NOT NULL THEN d.value ELSE s.value END AS value
                    FROM kv_store s
                    LEFT JOIN kv_delta d ON s.instance_id = d.instance_id AND s.key = d.key
                    WHERE s.instance_id = ?1
                    UNION
                    SELECT d.key, d.value
                    FROM kv_delta d
                    LEFT JOIN kv_store s ON d.instance_id = s.instance_id AND d.key = s.key
                    WHERE d.instance_id = ?2 AND s.key IS NULL
                ) merged WHERE value IS NOT NULL
                "#,
                params![instance, instance],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;
        let row = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?
            .ok_or_else(|| {
                ProviderError::permanent("get_instance_stats", "Missing KV stats row")
            })?;
        let kv_user_key_count = row
            .get::<i64>(0)
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;
        let kv_total_value_bytes = row
            .get::<i64>(1)
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;
        drop(rows);

        let mut rows = conn
            .query(
                r#"
                SELECT event_data FROM history
                WHERE instance_id = ?1 AND execution_id = ?2 AND event_id = 1
                "#,
                params![instance, execution_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;
        let queue_pending_count = match rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?
        {
            Some(row) => {
                let event_data = row
                    .get::<String>(0)
                    .map_err(|e| Self::libsql_to_provider_error("get_instance_stats", e))?;
                let event: Event = serde_json::from_str(&event_data).map_err(|e| {
                    ProviderError::permanent(
                        "get_instance_stats",
                        format!("Failed to deserialize OrchestrationStarted event: {e}"),
                    )
                })?;
                match event.kind {
                    EventKind::OrchestrationStarted {
                        carry_forward_events: Some(events),
                        ..
                    } => events.len() as u64,
                    _ => 0,
                }
            }
            None => 0,
        };

        Ok(Some(SystemStats {
            history_event_count: history_event_count as u64,
            history_size_bytes: history_size_bytes as u64,
            queue_pending_count,
            kv_user_key_count: kv_user_key_count as u64,
            kv_total_value_bytes: kv_total_value_bytes as u64,
        }))
    }
}
