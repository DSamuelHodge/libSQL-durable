//! PVM Phase 2 — Introspection language.
//!
//! Stable operator projections over kernel state so a host can answer
//! `ps` / `next` / `why_blocked` / `trace` / `queues` / `health` without
//! reading raw JSON event dumps by hand.
//!
//! See `docs/INTROSPECTION.md` and `docs/PVM.md` Phase 2.

use duroxide::providers::ProviderError;
use libsql::{params, Value};

use crate::native::{NativeLibsqlProvider, SCHEMA_VERSION};
use crate::world::{self, WORLD_FORMAT_VERSION};

/// Default attempt_count at/above which work is considered poisoned for health.
pub const DEFAULT_POISON_ATTEMPT_THRESHOLD: i64 = 5;

/// One process row for `ps`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRow {
    pub instance_id: String,
    pub orchestration_name: String,
    pub execution_id: u64,
    pub status: String,
    pub parent_instance_id: Option<String>,
    pub lock_token: Option<String>,
    pub locked_until_ms: Option<i64>,
    pub lock_held: bool,
}

/// Pending work visible to the scheduler (`next`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextWorkItem {
    pub queue: WorkQueueKind,
    pub id: i64,
    pub instance_id: Option<String>,
    pub visible_at: String,
    pub attempt_count: i64,
    pub locked: bool,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkQueueKind {
    Orchestrator,
    Worker,
}

/// Why a process is not making progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockReason {
    /// Instance lock held by a dispatcher.
    Locked {
        lock_token: String,
        locked_until_ms: i64,
        expired: bool,
    },
    /// Orchestrator work exists but is not yet visible (timer / delay).
    Delayed {
        queue_id: i64,
        visible_at: String,
        attempt_count: i64,
        summary: String,
    },
    /// Worker activity locked / in flight for this instance.
    WorkerInFlight {
        queue_id: i64,
        attempt_count: i64,
        summary: String,
    },
    /// Running, no lock, no pending delayed work — likely waiting on external event or empty.
    IdleOrWaitingExternal {
        last_event_type: Option<String>,
        last_event_id: Option<i64>,
    },
    /// Process is terminal.
    Terminal {
        status: String,
    },
    /// Instance not found.
    NotFound,
    /// Could not classify; includes free-form detail.
    Other {
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhyBlocked {
    pub instance_id: String,
    pub status: Option<String>,
    pub reason: BlockReason,
    pub notes: Vec<String>,
}

/// One journal projection row for `trace`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEvent {
    pub execution_id: u64,
    pub event_id: u64,
    pub event_type: String,
    /// Truncated event payload for operator display.
    pub data_preview: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueueSnapshot {
    pub orchestrator_unlocked: u64,
    pub orchestrator_locked: u64,
    pub orchestrator_oldest_visible_at: Option<String>,
    pub orchestrator_max_attempt: i64,
    pub worker_unlocked: u64,
    pub worker_locked: u64,
    pub worker_oldest_visible_at: Option<String>,
    pub worker_max_attempt: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldHealth {
    pub world_id: Option<String>,
    pub schema_version: Option<i64>,
    pub host_schema_version: i64,
    pub world_format_version: Option<i64>,
    pub host_world_format_version: i64,
    pub fence_ok: bool,
    pub total_instances: u64,
    pub running_instances: u64,
    pub terminal_instances: u64,
    pub active_locks: u64,
    pub expired_locks: u64,
    pub poison_orchestrator_items: u64,
    pub poison_worker_items: u64,
    pub queues: QueueSnapshot,
    pub notes: Vec<String>,
}

impl NativeLibsqlProvider {
    /// `ps` — non-terminal processes and lock holders.
    pub async fn introspect_ps(&self) -> Result<Vec<ProcessRow>, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_ps", e))?;
        let now = Self::now_millis();
        let mut rows = conn
            .query(
                r#"
                SELECT
                    i.instance_id,
                    i.orchestration_name,
                    i.current_execution_id,
                    COALESCE(e.status, 'Unknown'),
                    i.parent_instance_id,
                    il.lock_token,
                    il.locked_until
                FROM instances i
                LEFT JOIN executions e
                  ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
                LEFT JOIN instance_locks il ON i.instance_id = il.instance_id
                WHERE e.status IS NULL
                   OR e.status NOT IN ('Completed', 'Failed', 'ContinuedAsNew')
                ORDER BY i.instance_id
                "#,
                (),
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_ps", e))?;

        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_ps", e))?
        {
            let locked_until = match row.get_value(6) {
                Ok(Value::Integer(v)) => Some(v),
                Ok(Value::Null) => None,
                Ok(other) => Some(Self::coerce_timestamp(other) as i64),
                Err(_) => None,
            };
            let lock_token = Self::optional_text(row.get_value(5).unwrap_or(Value::Null));
            let lock_held = lock_token.is_some()
                && locked_until.map(|u| u > now).unwrap_or(false);
            out.push(ProcessRow {
                instance_id: row
                    .get::<String>(0)
                    .map_err(|e| Self::libsql_to_provider_error("introspect_ps", e))?,
                orchestration_name: row
                    .get::<String>(1)
                    .map_err(|e| Self::libsql_to_provider_error("introspect_ps", e))?,
                execution_id: row
                    .get::<i64>(2)
                    .map_err(|e| Self::libsql_to_provider_error("introspect_ps", e))?
                    as u64,
                status: row
                    .get::<String>(3)
                    .map_err(|e| Self::libsql_to_provider_error("introspect_ps", e))?,
                parent_instance_id: Self::optional_text(row.get_value(4).unwrap_or(Value::Null)),
                lock_token,
                locked_until_ms: locked_until,
                lock_held,
            });
        }
        Ok(out)
    }

    /// `next` — next unlocked, visible work items (both queues).
    pub async fn introspect_next(&self, limit: u32) -> Result<Vec<NextWorkItem>, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_next", e))?;
        let now = Self::now_millis();
        let limit = limit.clamp(1, 500) as i64;
        let mut out = Vec::new();

        // Orchestrator queue: visible_at may be integer millis or timestamp text.
        let mut rows = conn
            .query(
                r#"
                SELECT id, instance_id, CAST(visible_at AS TEXT), attempt_count,
                       lock_token IS NOT NULL, substr(work_item, 1, 180)
                FROM orchestrator_queue
                WHERE (lock_token IS NULL)
                  AND (
                    (typeof(visible_at) = 'integer' AND visible_at <= ?1)
                    OR (typeof(visible_at) != 'integer' AND visible_at <= datetime(?1 / 1000, 'unixepoch'))
                    OR visible_at <= ?1
                  )
                ORDER BY visible_at ASC, id ASC
                LIMIT ?2
                "#,
                params![now, limit],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_next", e))?;
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_next", e))?
        {
            out.push(Self::row_to_next(row, WorkQueueKind::Orchestrator)?);
        }
        drop(rows);

        let mut rows = conn
            .query(
                r#"
                SELECT id, instance_id, CAST(visible_at AS TEXT), attempt_count,
                       lock_token IS NOT NULL, substr(work_item, 1, 180)
                FROM worker_queue
                WHERE lock_token IS NULL AND visible_at <= ?1
                ORDER BY visible_at ASC, id ASC
                LIMIT ?2
                "#,
                params![now, limit],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_next", e))?;
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_next", e))?
        {
            out.push(Self::row_to_next(row, WorkQueueKind::Worker)?);
        }

        // Keep overall limit roughly balanced: sort by visible_at string/int poorly —
        // already limited per queue; trim total.
        if out.len() > limit as usize {
            out.truncate(limit as usize);
        }
        Ok(out)
    }

    fn row_to_next(
        row: libsql::Row,
        queue: WorkQueueKind,
    ) -> Result<NextWorkItem, ProviderError> {
        let locked = match row.get_value(4).unwrap_or(Value::Integer(0)) {
            Value::Integer(v) => v != 0,
            _ => false,
        };
        Ok(NextWorkItem {
            queue,
            id: row
                .get::<i64>(0)
                .map_err(|e| Self::libsql_to_provider_error("introspect_next", e))?,
            instance_id: Self::optional_text(row.get_value(1).unwrap_or(Value::Null)),
            visible_at: row
                .get::<String>(2)
                .unwrap_or_else(|_| String::from("0")),
            attempt_count: row.get::<i64>(3).unwrap_or(0),
            locked,
            summary: row.get::<String>(5).unwrap_or_default(),
        })
    }

    /// `why_blocked` — classify why an instance is not progressing.
    pub async fn introspect_why_blocked(
        &self,
        instance_id: &str,
    ) -> Result<WhyBlocked, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_why_blocked", e))?;
        let now = Self::now_millis();
        let mut notes = Vec::new();

        let mut rows = conn
            .query(
                r#"
                SELECT e.status, il.lock_token, il.locked_until
                FROM instances i
                LEFT JOIN executions e
                  ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
                LEFT JOIN instance_locks il ON i.instance_id = il.instance_id
                WHERE i.instance_id = ?1
                "#,
                params![instance_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_why_blocked", e))?;

        let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_why_blocked", e))?
        else {
            return Ok(WhyBlocked {
                instance_id: instance_id.to_string(),
                status: None,
                reason: BlockReason::NotFound,
                notes: vec!["instance not in instances table".into()],
            });
        };

        let status = Self::optional_text(row.get_value(0).unwrap_or(Value::Null));
        if let Some(ref s) = status {
            if matches!(s.as_str(), "Completed" | "Failed" | "ContinuedAsNew") {
                return Ok(WhyBlocked {
                    instance_id: instance_id.to_string(),
                    status: status.clone(),
                    reason: BlockReason::Terminal { status: s.clone() },
                    notes: vec!["process is terminal; no further scheduling expected".into()],
                });
            }
        }

        let lock_token = Self::optional_text(row.get_value(1).unwrap_or(Value::Null));
        let locked_until = match row.get_value(2) {
            Ok(Value::Integer(v)) => Some(v),
            Ok(Value::Null) => None,
            Ok(other) => Some(Self::coerce_timestamp(other) as i64),
            Err(_) => None,
        };
        drop(rows);

        if let (Some(token), Some(until)) = (lock_token.clone(), locked_until) {
            let expired = until <= now;
            if !expired {
                return Ok(WhyBlocked {
                    instance_id: instance_id.to_string(),
                    status,
                    reason: BlockReason::Locked {
                        lock_token: token,
                        locked_until_ms: until,
                        expired: false,
                    },
                    notes: vec!["instance lock held; another dispatcher owns this process".into()],
                });
            }
            notes.push(format!(
                "lock token present but expired (until={until}, now={now})"
            ));
            let _ = token;
        }

        // Future orchestrator work (timers / delays).
        let mut rows = conn
            .query(
                r#"
                SELECT id, CAST(visible_at AS TEXT), attempt_count, substr(work_item, 1, 180)
                FROM orchestrator_queue
                WHERE instance_id = ?1
                  AND lock_token IS NULL
                  AND (
                    (typeof(visible_at) = 'integer' AND visible_at > ?2)
                    OR (typeof(visible_at) != 'integer' AND visible_at > datetime(?2 / 1000, 'unixepoch'))
                    OR visible_at > ?2
                  )
                ORDER BY visible_at ASC, id ASC
                LIMIT 1
                "#,
                params![instance_id, now],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_why_blocked", e))?;
        if let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_why_blocked", e))?
        {
            return Ok(WhyBlocked {
                instance_id: instance_id.to_string(),
                status,
                reason: BlockReason::Delayed {
                    queue_id: row.get::<i64>(0).unwrap_or(0),
                    visible_at: row.get::<String>(1).unwrap_or_default(),
                    attempt_count: row.get::<i64>(2).unwrap_or(0),
                    summary: row.get::<String>(3).unwrap_or_default(),
                },
                notes,
            });
        }
        drop(rows);

        // Locked worker items for this instance.
        let mut rows = conn
            .query(
                r#"
                SELECT id, attempt_count, substr(work_item, 1, 180)
                FROM worker_queue
                WHERE instance_id = ?1 AND lock_token IS NOT NULL
                ORDER BY id ASC
                LIMIT 1
                "#,
                params![instance_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_why_blocked", e))?;
        if let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_why_blocked", e))?
        {
            return Ok(WhyBlocked {
                instance_id: instance_id.to_string(),
                status,
                reason: BlockReason::WorkerInFlight {
                    queue_id: row.get::<i64>(0).unwrap_or(0),
                    attempt_count: row.get::<i64>(1).unwrap_or(0),
                    summary: row.get::<String>(2).unwrap_or_default(),
                },
                notes: {
                    notes.push("worker item locked; syscall may be in flight".into());
                    notes
                },
            });
        }
        drop(rows);

        // Last journal event for idle/wait classification.
        let mut rows = conn
            .query(
                r#"
                SELECT event_id, event_type FROM history
                WHERE instance_id = ?1
                ORDER BY execution_id DESC, event_id DESC
                LIMIT 1
                "#,
                params![instance_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_why_blocked", e))?;
        let (last_event_id, last_event_type) = if let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_why_blocked", e))?
        {
            (
                Some(row.get::<i64>(0).unwrap_or(0)),
                Some(row.get::<String>(1).unwrap_or_default()),
            )
        } else {
            (None, None)
        };

        if let Some(ref t) = last_event_type {
            if t.contains("External") || t.contains("Subscribed") || t.contains("Wait") {
                notes.push("last event suggests waiting on external input".into());
            }
        }

        Ok(WhyBlocked {
            instance_id: instance_id.to_string(),
            status,
            reason: BlockReason::IdleOrWaitingExternal {
                last_event_type,
                last_event_id,
            },
            notes,
        })
    }

    /// `trace` — ordered journal projection for an instance (optional execution filter).
    pub async fn introspect_trace(
        &self,
        instance_id: &str,
        execution_id: Option<u64>,
        limit: u32,
    ) -> Result<Vec<TraceEvent>, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_trace", e))?;
        let limit = limit.clamp(1, 10_000) as i64;

        let mut out = Vec::new();
        if let Some(exec) = execution_id {
            let mut rows = conn
                .query(
                    r#"
                    SELECT execution_id, event_id, event_type, substr(event_data, 1, 400)
                    FROM history
                    WHERE instance_id = ?1 AND execution_id = ?2
                    ORDER BY event_id ASC
                    LIMIT ?3
                    "#,
                    params![instance_id, exec as i64, limit],
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("introspect_trace", e))?;
            while let Some(row) = rows
                .next()
                .await
                .map_err(|e| Self::libsql_to_provider_error("introspect_trace", e))?
            {
                out.push(Self::row_to_trace(row)?);
            }
        } else {
            let mut rows = conn
                .query(
                    r#"
                    SELECT execution_id, event_id, event_type, substr(event_data, 1, 400)
                    FROM history
                    WHERE instance_id = ?1
                    ORDER BY execution_id ASC, event_id ASC
                    LIMIT ?2
                    "#,
                    params![instance_id, limit],
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("introspect_trace", e))?;
            while let Some(row) = rows
                .next()
                .await
                .map_err(|e| Self::libsql_to_provider_error("introspect_trace", e))?
            {
                out.push(Self::row_to_trace(row)?);
            }
        }
        Ok(out)
    }

    fn row_to_trace(row: libsql::Row) -> Result<TraceEvent, ProviderError> {
        Ok(TraceEvent {
            execution_id: row.get::<i64>(0).unwrap_or(0) as u64,
            event_id: row.get::<i64>(1).unwrap_or(0) as u64,
            event_type: row.get::<String>(2).unwrap_or_default(),
            data_preview: row.get::<String>(3).unwrap_or_default(),
        })
    }

    /// `queues` — depths, oldest visible, max attempt counts.
    pub async fn introspect_queues(&self) -> Result<QueueSnapshot, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_queues", e))?;
        let mut q = QueueSnapshot::default();

        q.orchestrator_unlocked = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM orchestrator_queue WHERE lock_token IS NULL",
            (),
            "introspect_queues",
        )
        .await? as u64;
        q.orchestrator_locked = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM orchestrator_queue WHERE lock_token IS NOT NULL",
            (),
            "introspect_queues",
        )
        .await? as u64;
        q.worker_unlocked = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM worker_queue WHERE lock_token IS NULL",
            (),
            "introspect_queues",
        )
        .await? as u64;
        q.worker_locked = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM worker_queue WHERE lock_token IS NOT NULL",
            (),
            "introspect_queues",
        )
        .await? as u64;

        q.orchestrator_max_attempt = Self::query_count(
            &conn,
            "SELECT COALESCE(MAX(attempt_count), 0) FROM orchestrator_queue",
            (),
            "introspect_queues",
        )
        .await?;
        q.worker_max_attempt = Self::query_count(
            &conn,
            "SELECT COALESCE(MAX(attempt_count), 0) FROM worker_queue",
            (),
            "introspect_queues",
        )
        .await?;

        let mut rows = conn
            .query(
                "SELECT CAST(MIN(visible_at) AS TEXT) FROM orchestrator_queue WHERE lock_token IS NULL",
                (),
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_queues", e))?;
        if let Some(row) = rows.next().await.map_err(|e| Self::libsql_to_provider_error("introspect_queues", e))? {
            q.orchestrator_oldest_visible_at =
                Self::optional_text(row.get_value(0).unwrap_or(Value::Null));
        }
        drop(rows);

        let mut rows = conn
            .query(
                "SELECT CAST(MIN(visible_at) AS TEXT) FROM worker_queue WHERE lock_token IS NULL",
                (),
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_queues", e))?;
        if let Some(row) = rows.next().await.map_err(|e| Self::libsql_to_provider_error("introspect_queues", e))? {
            q.worker_oldest_visible_at =
                Self::optional_text(row.get_value(0).unwrap_or(Value::Null));
        }

        Ok(q)
    }

    /// `health` — schema fence, counts, poison, queues.
    pub async fn introspect_health(
        &self,
        poison_attempt_threshold: Option<i64>,
    ) -> Result<WorldHealth, ProviderError> {
        let threshold = poison_attempt_threshold.unwrap_or(DEFAULT_POISON_ATTEMPT_THRESHOLD);
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("introspect_health", e))?;
        let now = Self::now_millis();
        let mut notes = Vec::new();

        let manifest = world::read_world_manifest(&conn)
            .await
            .map_err(|e| ProviderError::permanent("introspect_health", e.to_string()))?;
        let schema_version = self
            .schema_version()
            .await
            .map_err(|e| ProviderError::permanent("introspect_health", e.to_string()))?;

        let mut fence_ok = true;
        if let Some(v) = schema_version {
            if world::check_schema_fence(v).is_err() {
                fence_ok = false;
                notes.push(format!("schema fence failed for version {v}"));
            }
        } else {
            notes.push("schema_meta missing".into());
            fence_ok = false;
        }
        if let Some(ref m) = manifest {
            if world::check_format_fence(m.world_format_version).is_err() {
                fence_ok = false;
                notes.push("world format fence failed".into());
            }
        } else {
            notes.push("world_manifest missing".into());
        }

        let total_instances = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM instances",
            (),
            "introspect_health",
        )
        .await? as u64;
        let running_instances = Self::query_count(
            &conn,
            r#"
            SELECT COUNT(*) FROM instances i
            JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
            WHERE e.status = 'Running'
            "#,
            (),
            "introspect_health",
        )
        .await? as u64;
        let terminal_instances = Self::query_count(
            &conn,
            r#"
            SELECT COUNT(*) FROM instances i
            JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
            WHERE e.status IN ('Completed', 'Failed', 'ContinuedAsNew')
            "#,
            (),
            "introspect_health",
        )
        .await? as u64;

        let active_locks = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM instance_locks WHERE locked_until > ?1",
            params![now],
            "introspect_health",
        )
        .await? as u64;
        let expired_locks = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM instance_locks WHERE locked_until <= ?1",
            params![now],
            "introspect_health",
        )
        .await? as u64;

        let poison_orchestrator_items = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM orchestrator_queue WHERE attempt_count >= ?1",
            params![threshold],
            "introspect_health",
        )
        .await? as u64;
        let poison_worker_items = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM worker_queue WHERE attempt_count >= ?1",
            params![threshold],
            "introspect_health",
        )
        .await? as u64;

        if poison_orchestrator_items + poison_worker_items > 0 {
            notes.push(format!(
                "poison items present (threshold attempt_count>={threshold})"
            ));
        }
        if expired_locks > 0 {
            notes.push(format!("{expired_locks} expired locks still present"));
        }
        if fence_ok {
            notes.push("fence ok".into());
        }

        let queues = self.introspect_queues().await?;

        Ok(WorldHealth {
            world_id: manifest.as_ref().map(|m| m.world_id.clone()),
            schema_version,
            host_schema_version: SCHEMA_VERSION,
            world_format_version: manifest.as_ref().map(|m| m.world_format_version),
            host_world_format_version: WORLD_FORMAT_VERSION,
            fence_ok,
            total_instances,
            running_instances,
            terminal_instances,
            active_locks,
            expired_locks,
            poison_orchestrator_items,
            poison_worker_items,
            queues,
            notes,
        })
    }
}
