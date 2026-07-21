//! PVM Phase 3 — Healing policies.
//!
//! Closed-loop recovery for common failure classes. Every mutating heal action
//! is recorded in `healing_audit` so repairs are inspectable.
//!
//! See `docs/HEALING.md` and `docs/PVM.md` Phase 3.

use duroxide::providers::{ProviderAdmin, ProviderError, PruneOptions, PruneResult};
use libsql::params;

use crate::introspect::DEFAULT_POISON_ATTEMPT_THRESHOLD;
use crate::native::NativeLibsqlProvider;

/// Default max history events before a process is considered for compact heal.
pub const DEFAULT_RUNAWAY_HISTORY_EVENTS: i64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HealOptions {
    /// attempt_count >= this → quarantine (default 5).
    pub poison_attempt_threshold: Option<i64>,
    /// When compacting, keep last N executions (default Some(2)).
    pub compact_keep_last: Option<u32>,
    /// Only compact instances with more than this many history events.
    pub runaway_history_events: Option<i64>,
    /// Max instances to compact in one heal_compact_histories call.
    pub compact_instance_limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealActionResult {
    pub action: String,
    pub rows_affected: u64,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HealReport {
    pub actions: Vec<HealActionResult>,
    pub notes: Vec<String>,
}

impl HealReport {
    pub fn total_rows_affected(&self) -> u64 {
        self.actions.iter().map(|a| a.rows_affected).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealingAuditRow {
    pub id: i64,
    pub action: String,
    pub detail: String,
    pub rows_affected: u64,
    pub created_at_ms: i64,
}

impl NativeLibsqlProvider {
    /// Ensure healing audit/quarantine tables exist (idempotent).
    pub async fn ensure_healing_schema(&self) -> Result<(), ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("ensure_healing_schema", e))?;
        for sql in [
            r#"
            CREATE TABLE IF NOT EXISTS healing_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                detail TEXT NOT NULL,
                rows_affected INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS healing_quarantine (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                original_id INTEGER,
                instance_id TEXT,
                attempt_count INTEGER NOT NULL,
                work_item TEXT NOT NULL,
                reason TEXT NOT NULL,
                quarantined_at_ms INTEGER NOT NULL
            )
            "#,
            "CREATE INDEX IF NOT EXISTS idx_healing_audit_created ON healing_audit(created_at_ms)",
            "CREATE INDEX IF NOT EXISTS idx_healing_quarantine_instance ON healing_quarantine(instance_id)",
        ] {
            conn.execute(sql, ())
                .await
                .map_err(|e| Self::libsql_to_provider_error("ensure_healing_schema", e))?;
        }
        Ok(())
    }

    async fn audit(
        &self,
        action: &str,
        rows_affected: u64,
        detail: &str,
    ) -> Result<(), ProviderError> {
        self.ensure_healing_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("healing_audit", e))?;
        conn.execute(
            r#"
            INSERT INTO healing_audit (action, detail, rows_affected, created_at_ms)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![action, detail, rows_affected as i64, Self::now_millis()],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("healing_audit", e))?;
        Ok(())
    }

    /// Reclaim expired instance locks and unlock expired queue locks.
    pub async fn heal_reclaim_expired_locks(&self) -> Result<HealActionResult, ProviderError> {
        self.ensure_healing_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_reclaim_expired_locks", e))?;
        let now = Self::now_millis();

        let deleted_instance_locks = conn
            .execute(
                "DELETE FROM instance_locks WHERE locked_until <= ?1",
                params![now],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_reclaim_expired_locks", e))?;

        // Unlock expired worker/orch locks so work can be redelivered.
        let unlocked_orch = conn
            .execute(
                r#"
                UPDATE orchestrator_queue
                SET lock_token = NULL, locked_until = NULL
                WHERE lock_token IS NOT NULL
                  AND locked_until IS NOT NULL
                  AND (
                    (typeof(locked_until) = 'integer' AND locked_until <= ?1)
                    OR locked_until <= ?1
                  )
                "#,
                params![now],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_reclaim_expired_locks", e))?;

        let unlocked_worker = conn
            .execute(
                r#"
                UPDATE worker_queue
                SET lock_token = NULL, locked_until = NULL
                WHERE lock_token IS NOT NULL
                  AND locked_until IS NOT NULL
                  AND (
                    (typeof(locked_until) = 'integer' AND locked_until <= ?1)
                    OR locked_until <= ?1
                  )
                "#,
                params![now],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_reclaim_expired_locks", e))?;

        let rows = deleted_instance_locks + unlocked_orch + unlocked_worker;
        let detail = format!(
            "instance_locks_deleted={deleted_instance_locks} orch_unlocked={unlocked_orch} worker_unlocked={unlocked_worker}"
        );
        self.audit("reclaim_expired_locks", rows, &detail).await?;
        Ok(HealActionResult {
            action: "reclaim_expired_locks".into(),
            rows_affected: rows,
            detail,
        })
    }

    /// Move poison queue items (high attempt_count) into `healing_quarantine` and delete from queues.
    pub async fn heal_quarantine_poison(
        &self,
        attempt_threshold: Option<i64>,
    ) -> Result<HealActionResult, ProviderError> {
        self.ensure_healing_schema().await?;
        let threshold = attempt_threshold.unwrap_or(DEFAULT_POISON_ATTEMPT_THRESHOLD);
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_quarantine_poison", e))?;
        let now = Self::now_millis();
        let mut moved = 0u64;

        // Orchestrator poison
        let mut rows = conn
            .query(
                r#"
                SELECT id, instance_id, attempt_count, work_item
                FROM orchestrator_queue
                WHERE attempt_count >= ?1
                "#,
                params![threshold],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_quarantine_poison", e))?;
        let mut orch_ids = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_quarantine_poison", e))?
        {
            let id = row.get::<i64>(0).unwrap_or(0);
            let instance_id = row.get::<String>(1).unwrap_or_default();
            let attempts = row.get::<i64>(2).unwrap_or(0);
            let work_item = row.get::<String>(3).unwrap_or_default();
            conn.execute(
                r#"
                INSERT INTO healing_quarantine
                  (queue, original_id, instance_id, attempt_count, work_item, reason, quarantined_at_ms)
                VALUES ('orchestrator', ?1, ?2, ?3, ?4, ?5, ?6)
                "#,
                params![
                    id,
                    instance_id,
                    attempts,
                    work_item,
                    format!("attempt_count>={threshold}"),
                    now
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_quarantine_poison", e))?;
            orch_ids.push(id);
            moved += 1;
        }
        drop(rows);
        for id in orch_ids {
            conn.execute(
                "DELETE FROM orchestrator_queue WHERE id = ?1",
                params![id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_quarantine_poison", e))?;
        }

        // Worker poison
        let mut rows = conn
            .query(
                r#"
                SELECT id, instance_id, attempt_count, work_item
                FROM worker_queue
                WHERE attempt_count >= ?1
                "#,
                params![threshold],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_quarantine_poison", e))?;
        let mut worker_ids = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_quarantine_poison", e))?
        {
            let id = row.get::<i64>(0).unwrap_or(0);
            let instance_id = match row.get_value(1) {
                Ok(libsql::Value::Text(s)) => s,
                _ => String::new(),
            };
            let attempts = row.get::<i64>(2).unwrap_or(0);
            let work_item = row.get::<String>(3).unwrap_or_default();
            conn.execute(
                r#"
                INSERT INTO healing_quarantine
                  (queue, original_id, instance_id, attempt_count, work_item, reason, quarantined_at_ms)
                VALUES ('worker', ?1, ?2, ?3, ?4, ?5, ?6)
                "#,
                params![
                    id,
                    instance_id,
                    attempts,
                    work_item,
                    format!("attempt_count>={threshold}"),
                    now
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_quarantine_poison", e))?;
            worker_ids.push(id);
            moved += 1;
        }
        drop(rows);
        for id in worker_ids {
            conn.execute("DELETE FROM worker_queue WHERE id = ?1", params![id])
                .await
                .map_err(|e| Self::libsql_to_provider_error("heal_quarantine_poison", e))?;
        }

        let detail = format!("threshold={threshold} quarantined={moved}");
        self.audit("quarantine_poison", moved, &detail).await?;
        Ok(HealActionResult {
            action: "quarantine_poison".into(),
            rows_affected: moved,
            detail,
        })
    }

    /// Drop orphan queue rows whose instance no longer exists (fence after delete).
    pub async fn heal_fence_orphan_queue_items(&self) -> Result<HealActionResult, ProviderError> {
        self.ensure_healing_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_fence_orphan_queue_items", e))?;

        let orch = conn
            .execute(
                r#"
                DELETE FROM orchestrator_queue
                WHERE instance_id IS NOT NULL
                  AND instance_id NOT IN (SELECT instance_id FROM instances)
                "#,
                (),
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_fence_orphan_queue_items", e))?;

        let worker = conn
            .execute(
                r#"
                DELETE FROM worker_queue
                WHERE instance_id IS NOT NULL
                  AND instance_id NOT IN (SELECT instance_id FROM instances)
                "#,
                (),
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_fence_orphan_queue_items", e))?;

        // Orphan locks without instances
        let locks = conn
            .execute(
                r#"
                DELETE FROM instance_locks
                WHERE instance_id NOT IN (SELECT instance_id FROM instances)
                "#,
                (),
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_fence_orphan_queue_items", e))?;

        let rows = orch + worker + locks;
        let detail =
            format!("orphan_orch={orch} orphan_worker={worker} orphan_locks={locks}");
        self.audit("fence_orphan_queue_items", rows, &detail)
            .await?;
        Ok(HealActionResult {
            action: "fence_orphan_queue_items".into(),
            rows_affected: rows,
            detail,
        })
    }

    /// Compact runaway histories: prune old executions on instances with huge event counts.
    pub async fn heal_compact_histories(
        &self,
        options: &HealOptions,
    ) -> Result<HealActionResult, ProviderError> {
        self.ensure_healing_schema().await?;
        let runaway = options
            .runaway_history_events
            .unwrap_or(DEFAULT_RUNAWAY_HISTORY_EVENTS);
        let keep_last = options.compact_keep_last.or(Some(2));
        let limit = options.compact_instance_limit.unwrap_or(50) as i64;

        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_compact_histories", e))?;

        let mut rows = conn
            .query(
                r#"
                SELECT instance_id, COUNT(*) AS cnt
                FROM history
                GROUP BY instance_id
                HAVING COUNT(*) > ?1
                ORDER BY cnt DESC
                LIMIT ?2
                "#,
                params![runaway, limit],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_compact_histories", e))?;

        let mut instances = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("heal_compact_histories", e))?
        {
            instances.push(row.get::<String>(0).unwrap_or_default());
        }
        drop(rows);

        let mut total_execs = 0u64;
        let mut total_events = 0u64;
        let mut processed = 0u64;
        for instance_id in &instances {
            let result: PruneResult = ProviderAdmin::prune_executions(
                self,
                instance_id,
                PruneOptions {
                    keep_last,
                    completed_before: None,
                },
            )
            .await?;
            total_execs += result.executions_deleted;
            total_events += result.events_deleted;
            processed += result.instances_processed;
        }

        let rows = total_execs + total_events;
        let detail = format!(
            "instances_scanned={} processed={processed} executions_deleted={total_execs} events_deleted={total_events} runaway_threshold={runaway} keep_last={keep_last:?}",
            instances.len()
        );
        self.audit("compact_histories", rows, &detail).await?;
        Ok(HealActionResult {
            action: "compact_histories".into(),
            rows_affected: rows,
            detail,
        })
    }

    /// Run the standard healing suite (reclaim → quarantine → fence orphans → optional compact).
    pub async fn heal(&self, options: HealOptions) -> Result<HealReport, ProviderError> {
        // Incompatible schema is refused at open/migrate (Phase 1 fence).
        let mut report = HealReport::default();
        report
            .notes
            .push("schema fence enforced at world open/migrate".into());

        let r = self.heal_reclaim_expired_locks().await?;
        report.actions.push(r);

        let r = self
            .heal_quarantine_poison(options.poison_attempt_threshold)
            .await?;
        report.actions.push(r);

        let r = self.heal_fence_orphan_queue_items().await?;
        report.actions.push(r);

        // Compact only if there is runaway pressure (cheap pre-check via health).
        let health = self.introspect_health(options.poison_attempt_threshold).await?;
        if health.queues.orchestrator_max_attempt >= 0 {
            // always allow compact pass; it no-ops when no runaway instances
            let r = self.heal_compact_histories(&options).await?;
            if r.rows_affected > 0 || r.detail.contains("instances_scanned=0") {
                report.actions.push(r);
            }
        }

        report.notes.push(format!(
            "heal complete; total_rows_affected={}",
            report.total_rows_affected()
        ));
        Ok(report)
    }

    /// Read recent healing audit rows (newest first).
    pub async fn healing_audit_log(
        &self,
        limit: u32,
    ) -> Result<Vec<HealingAuditRow>, ProviderError> {
        self.ensure_healing_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("healing_audit_log", e))?;
        let limit = limit.clamp(1, 1000) as i64;
        let mut rows = conn
            .query(
                r#"
                SELECT id, action, detail, rows_affected, created_at_ms
                FROM healing_audit
                ORDER BY id DESC
                LIMIT ?1
                "#,
                params![limit],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("healing_audit_log", e))?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("healing_audit_log", e))?
        {
            out.push(HealingAuditRow {
                id: row.get::<i64>(0).unwrap_or(0),
                action: row.get::<String>(1).unwrap_or_default(),
                detail: row.get::<String>(2).unwrap_or_default(),
                rows_affected: row.get::<i64>(3).unwrap_or(0) as u64,
                created_at_ms: row.get::<i64>(4).unwrap_or(0),
            });
        }
        Ok(out)
    }

    /// Count items currently in quarantine.
    pub async fn healing_quarantine_count(&self) -> Result<u64, ProviderError> {
        self.ensure_healing_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("healing_quarantine_count", e))?;
        let n = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM healing_quarantine",
            (),
            "healing_quarantine_count",
        )
        .await?;
        Ok(n as u64)
    }
}
