//! PVM Phase 6 — Adaptive runtime policy.
//!
//! Policy parameters are stored in the world and can be updated from observed
//! health/queue behavior. Changes are audited. Control-flow determinism is
//! unchanged — only host scheduling knobs move.

use duroxide::providers::ProviderError;
use libsql::params;

use crate::native::{NativeLibsqlProvider, ProviderTuning};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePolicy {
    pub lock_timeout_ms: i64,
    pub max_transient_retries: i64,
    pub retry_base_delay_ms: i64,
    pub poison_threshold: i64,
    pub compact_keep_last: i64,
    pub updated_at_ms: i64,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyAuditRow {
    pub id: i64,
    pub source: String,
    pub detail: String,
    pub created_at_ms: i64,
}

impl Default for RuntimePolicy {
    fn default() -> Self {
        let t = ProviderTuning::default();
        Self {
            lock_timeout_ms: 30_000,
            max_transient_retries: t.max_transient_retries as i64,
            retry_base_delay_ms: t.retry_base_delay_ms as i64,
            poison_threshold: 5,
            compact_keep_last: 2,
            updated_at_ms: 0,
            source: "default".into(),
        }
    }
}

impl RuntimePolicy {
    pub fn to_tuning(&self) -> ProviderTuning {
        ProviderTuning {
            busy_timeout_ms: self.lock_timeout_ms.max(0) as u64,
            max_transient_retries: self.max_transient_retries.max(0) as u32,
            retry_base_delay_ms: self.retry_base_delay_ms.max(0) as u64,
        }
    }
}

impl NativeLibsqlProvider {
    pub async fn ensure_policy_schema(&self) -> Result<(), ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("ensure_policy_schema", e))?;
        for sql in [
            r#"
            CREATE TABLE IF NOT EXISTS runtime_policy (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                lock_timeout_ms INTEGER NOT NULL,
                max_transient_retries INTEGER NOT NULL,
                retry_base_delay_ms INTEGER NOT NULL,
                poison_threshold INTEGER NOT NULL,
                compact_keep_last INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                source TEXT NOT NULL
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS policy_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                detail TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            )
            "#,
        ] {
            conn.execute(sql, ())
                .await
                .map_err(|e| Self::libsql_to_provider_error("ensure_policy_schema", e))?;
        }
        Ok(())
    }

    pub async fn get_runtime_policy(&self) -> Result<RuntimePolicy, ProviderError> {
        self.ensure_policy_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_runtime_policy", e))?;
        let mut rows = conn
            .query(
                r#"
                SELECT lock_timeout_ms, max_transient_retries, retry_base_delay_ms,
                       poison_threshold, compact_keep_last, updated_at_ms, source
                FROM runtime_policy WHERE id = 1
                "#,
                (),
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_runtime_policy", e))?;
        if let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_runtime_policy", e))?
        {
            return Ok(RuntimePolicy {
                lock_timeout_ms: row.get::<i64>(0).unwrap_or(30_000),
                max_transient_retries: row.get::<i64>(1).unwrap_or(2),
                retry_base_delay_ms: row.get::<i64>(2).unwrap_or(15),
                poison_threshold: row.get::<i64>(3).unwrap_or(5),
                compact_keep_last: row.get::<i64>(4).unwrap_or(2),
                updated_at_ms: row.get::<i64>(5).unwrap_or(0),
                source: row.get::<String>(6).unwrap_or_else(|_| "default".into()),
            });
        }
        let def = RuntimePolicy::default();
        self.set_runtime_policy(&def, "default").await?;
        Ok(def)
    }

    pub async fn set_runtime_policy(
        &self,
        policy: &RuntimePolicy,
        source: &str,
    ) -> Result<(), ProviderError> {
        self.ensure_policy_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("set_runtime_policy", e))?;
        let now = Self::now_millis();
        conn.execute(
            r#"
            INSERT INTO runtime_policy (
                id, lock_timeout_ms, max_transient_retries, retry_base_delay_ms,
                poison_threshold, compact_keep_last, updated_at_ms, source
            ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(id) DO UPDATE SET
                lock_timeout_ms = excluded.lock_timeout_ms,
                max_transient_retries = excluded.max_transient_retries,
                retry_base_delay_ms = excluded.retry_base_delay_ms,
                poison_threshold = excluded.poison_threshold,
                compact_keep_last = excluded.compact_keep_last,
                updated_at_ms = excluded.updated_at_ms,
                source = excluded.source
            "#,
            params![
                policy.lock_timeout_ms,
                policy.max_transient_retries,
                policy.retry_base_delay_ms,
                policy.poison_threshold,
                policy.compact_keep_last,
                now,
                source
            ],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("set_runtime_policy", e))?;

        let detail = format!(
            "lock_timeout_ms={} retries={} delay_ms={} poison={} keep_last={}",
            policy.lock_timeout_ms,
            policy.max_transient_retries,
            policy.retry_base_delay_ms,
            policy.poison_threshold,
            policy.compact_keep_last
        );
        conn.execute(
            "INSERT INTO policy_audit (source, detail, created_at_ms) VALUES (?1, ?2, ?3)",
            params![source, detail, now],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("set_runtime_policy", e))?;
        Ok(())
    }

    /// Adjust policy from observed health/queue pressure (bounded heuristics).
    pub async fn adapt_policy_from_health(&self) -> Result<RuntimePolicy, ProviderError> {
        let health = self.introspect_health(None).await?;
        let mut policy = self.get_runtime_policy().await?;
        let mut changed = false;

        // More pressure → longer busy timeout / more retries (capped).
        if health.queues.orchestrator_locked + health.queues.worker_locked > 0
            || health.expired_locks > 0
        {
            let next = (policy.lock_timeout_ms + 5_000).min(120_000);
            if next != policy.lock_timeout_ms {
                policy.lock_timeout_ms = next;
                changed = true;
            }
            let next_r = (policy.max_transient_retries + 1).min(8);
            if next_r != policy.max_transient_retries {
                policy.max_transient_retries = next_r;
                changed = true;
            }
        }

        if health.poison_orchestrator_items + health.poison_worker_items > 0 {
            // Slightly stricter poison threshold so quarantine triggers earlier next time? 
            // Actually if poison present, lower threshold to catch earlier - min 3.
            let next = (policy.poison_threshold - 1).max(3);
            if next != policy.poison_threshold {
                policy.poison_threshold = next;
                changed = true;
            }
        }

        // Large history pressure → prefer keeping fewer executions when compacting.
        if health.total_instances > 0 && health.queues.orchestrator_max_attempt >= 3 {
            if policy.compact_keep_last > 1 {
                policy.compact_keep_last = 1;
                changed = true;
            }
        }

        if changed {
            policy.source = "adaptive".into();
            self.set_runtime_policy(&policy, "adaptive").await?;
        }
        Ok(policy)
    }

    pub async fn policy_audit_log(&self, limit: u32) -> Result<Vec<PolicyAuditRow>, ProviderError> {
        self.ensure_policy_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("policy_audit_log", e))?;
        let limit = limit.clamp(1, 1000) as i64;
        let mut rows = conn
            .query(
                r#"
                SELECT id, source, detail, created_at_ms
                FROM policy_audit ORDER BY id DESC LIMIT ?1
                "#,
                params![limit],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("policy_audit_log", e))?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("policy_audit_log", e))?
        {
            out.push(PolicyAuditRow {
                id: row.get::<i64>(0).unwrap_or(0),
                source: row.get::<String>(1).unwrap_or_default(),
                detail: row.get::<String>(2).unwrap_or_default(),
                created_at_ms: row.get::<i64>(3).unwrap_or(0),
            });
        }
        Ok(out)
    }
}
