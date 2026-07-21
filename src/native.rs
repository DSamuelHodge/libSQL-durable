use std::path::PathBuf;
use std::time::Duration;

use duroxide::providers::{
    DeleteInstanceResult, DispatcherCapabilityFilter, ExecutionInfo, ExecutionMetadata,
    InstanceFilter, InstanceInfo, KvEntry, OrchestrationItem, Provider, ProviderAdmin,
    ProviderError, PruneOptions, PruneResult, QueueDepths, ScheduledActivityIdentifier,
    SessionFetchConfig, SystemMetrics, TagFilter, WorkItem,
};
use duroxide::{Event, EventKind, SystemStats};
use libsql::{Value, params};

use crate::{
    LibsqlDatabaseConfig, LibsqlDatabaseMode, LibsqlEngineOptions, LibsqlProviderInitError,
};

/// Connection/retry tuning for local files and remote `sqld` endpoints.
///
/// Defaults are conservative for embedded local use. Remote constructors and
/// `ProviderTuning::from_env()` raise busy-timeout and transient retries to
/// absorb HTTP/Hrana lock timing under concurrent load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderTuning {
    /// SQLite `PRAGMA busy_timeout` applied on every new connection (ms).
    pub busy_timeout_ms: u64,
    /// How many times to retry clearly transient provider errors.
    pub max_transient_retries: u32,
    /// Base backoff delay for transient retries (doubles each attempt).
    pub retry_base_delay_ms: u64,
}

impl Default for ProviderTuning {
    fn default() -> Self {
        Self {
            busy_timeout_ms: 2_000,
            max_transient_retries: 2,
            retry_base_delay_ms: 15,
        }
    }
}

impl ProviderTuning {
    /// Defaults tuned for self-hosted remote / multi-node `sqld`.
    pub fn remote_defaults() -> Self {
        Self {
            busy_timeout_ms: 5_000,
            max_transient_retries: 4,
            retry_base_delay_ms: 25,
        }
    }

    /// Build tuning from environment, preferring remote defaults when
    /// `LIBSQL_REMOTE_URL` is set.
    ///
    /// | Variable | Meaning |
    /// |---|---|
    /// | `LIBSQL_BUSY_TIMEOUT_MS` | `PRAGMA busy_timeout` |
    /// | `LIBSQL_TRANSIENT_RETRIES` | max transient retries |
    /// | `LIBSQL_RETRY_BASE_DELAY_MS` | base backoff delay |
    pub fn from_env() -> Self {
        let mut tuning = if std::env::var_os("LIBSQL_REMOTE_URL").is_some() {
            Self::remote_defaults()
        } else {
            Self::default()
        };
        if let Ok(v) = std::env::var("LIBSQL_BUSY_TIMEOUT_MS") {
            if let Ok(ms) = v.parse() {
                tuning.busy_timeout_ms = ms;
            }
        }
        if let Ok(v) = std::env::var("LIBSQL_TRANSIENT_RETRIES") {
            if let Ok(n) = v.parse() {
                tuning.max_transient_retries = n;
            }
        }
        if let Ok(v) = std::env::var("LIBSQL_RETRY_BASE_DELAY_MS") {
            if let Ok(ms) = v.parse() {
                tuning.retry_base_delay_ms = ms;
            }
        }
        tuning
    }
}

pub struct NativeLibsqlProvider {
    db: libsql::Database,
    tuning: ProviderTuning,
    engine_options: LibsqlEngineOptions,
}

impl NativeLibsqlProvider {
    pub async fn new(config: LibsqlDatabaseConfig) -> Result<Self, LibsqlProviderInitError> {
        let LibsqlDatabaseConfig { mode, options } = config;
        let tuning = match &mode {
            LibsqlDatabaseMode::InMemory | LibsqlDatabaseMode::Local { .. } => {
                ProviderTuning::from_env()
            }
            LibsqlDatabaseMode::Remote { .. }
            | LibsqlDatabaseMode::RemoteReplica { .. }
            | LibsqlDatabaseMode::OfflineSynced { .. } => {
                // Prefer remote defaults, then allow env overrides.
                let mut tuning = ProviderTuning::remote_defaults();
                let from_env = ProviderTuning::from_env();
                // from_env already prefers remote defaults when LIBSQL_REMOTE_URL is set.
                if std::env::var_os("LIBSQL_REMOTE_URL").is_some() {
                    tuning = from_env;
                } else {
                    // Keep remote defaults but honor explicit env knobs if present.
                    if std::env::var_os("LIBSQL_BUSY_TIMEOUT_MS").is_some()
                        || std::env::var_os("LIBSQL_TRANSIENT_RETRIES").is_some()
                        || std::env::var_os("LIBSQL_RETRY_BASE_DELAY_MS").is_some()
                    {
                        tuning = from_env;
                    }
                }
                tuning
            }
        };

        match mode {
            LibsqlDatabaseMode::InMemory => {
                Self::open_local(":memory:", options, tuning).await
            }
            LibsqlDatabaseMode::Local { path } => Self::open_local(path, options, tuning).await,
            LibsqlDatabaseMode::Remote { url, auth_token } => {
                Self::open_remote(url, auth_token, options, tuning).await
            }
            LibsqlDatabaseMode::RemoteReplica {
                local_path,
                remote_url,
                auth_token,
            } => Self::open_remote_replica(local_path, remote_url, auth_token, options, tuning).await,
            LibsqlDatabaseMode::OfflineSynced {
                local_path,
                remote_url,
                auth_token,
            } => Self::open_offline_synced(local_path, remote_url, auth_token, options, tuning).await,
        }
    }

    pub async fn new_local(path: impl Into<PathBuf>) -> Result<Self, LibsqlProviderInitError> {
        Self::open_local(
            path,
            LibsqlEngineOptions::default(),
            ProviderTuning::default(),
        )
        .await
    }

    pub async fn new_remote(
        url: String,
        auth_token: String,
    ) -> Result<Self, LibsqlProviderInitError> {
        Self::open_remote(
            url,
            auth_token,
            LibsqlEngineOptions::default(),
            ProviderTuning::remote_defaults(),
        )
        .await
    }

    pub async fn new_remote_replica(
        local_path: impl Into<PathBuf>,
        remote_url: String,
        auth_token: String,
    ) -> Result<Self, LibsqlProviderInitError> {
        Self::open_remote_replica(
            local_path,
            remote_url,
            auth_token,
            LibsqlEngineOptions::default(),
            ProviderTuning::remote_defaults(),
        )
        .await
    }

    pub async fn new_offline_synced(
        local_path: impl Into<PathBuf>,
        remote_url: String,
        auth_token: String,
    ) -> Result<Self, LibsqlProviderInitError> {
        Self::open_offline_synced(
            local_path,
            remote_url,
            auth_token,
            LibsqlEngineOptions::default(),
            ProviderTuning::remote_defaults(),
        )
        .await
    }

    async fn open_local(
        path: impl Into<PathBuf>,
        options: LibsqlEngineOptions,
        tuning: ProviderTuning,
    ) -> Result<Self, LibsqlProviderInitError> {
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

        let mut builder = libsql::Builder::new_local(path);
        if let Some(key) = &options.local_encryption_key {
            builder = builder.encryption_config(Self::local_encryption_config(key)?);
        }
        let db = builder.build().await?;
        let provider = Self {
            db,
            tuning,
            engine_options: options,
        };
        provider.create_schema().await?;
        Ok(provider)
    }

    async fn open_remote(
        url: String,
        auth_token: String,
        options: LibsqlEngineOptions,
        tuning: ProviderTuning,
    ) -> Result<Self, LibsqlProviderInitError> {
        let mut builder = libsql::Builder::new_remote(url, auth_token);
        if let Some(namespace) = &options.namespace {
            builder = builder.namespace(namespace.clone());
        }
        if let Some(key) = &options.remote_encryption_key_b64 {
            builder = builder.remote_encryption(Self::remote_encryption_context(key));
        }
        let db = builder.build().await?;
        let provider = Self {
            db,
            tuning,
            engine_options: options,
        };
        provider.create_schema().await?;
        Ok(provider)
    }

    async fn open_remote_replica(
        local_path: impl Into<PathBuf>,
        remote_url: String,
        auth_token: String,
        options: LibsqlEngineOptions,
        tuning: ProviderTuning,
    ) -> Result<Self, LibsqlProviderInitError> {
        let local_path = local_path.into();
        if let Some(parent) = local_path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(|e| {
                libsql::Error::ConnectionFailed(format!(
                    "failed to create replica database directory: {e}"
                ))
            })?;
        }

        // Embedded replicas need a clean file or a previously synced DB. Sync
        // from the primary before applying DDL so we don't invent a divergent
        // local schema ahead of the first replication handshake.
        let mut builder = libsql::Builder::new_remote_replica(local_path, remote_url, auth_token)
            .read_your_writes(options.read_your_writes);
        if let Some(namespace) = &options.namespace {
            builder = builder.namespace(namespace.clone());
        }
        if let Some(interval) = options.sync_interval {
            builder = builder.sync_interval(interval);
        }
        if let Some(key) = &options.local_encryption_key {
            builder = builder.encryption_config(Self::local_encryption_config(key)?);
        }
        if let Some(key) = &options.remote_encryption_key_b64 {
            builder = builder.remote_encryption(Self::remote_encryption_context(key));
        }
        let db = builder.build().await?;
        let provider = Self {
            db,
            tuning,
            engine_options: options,
        };
        provider.sync().await?;
        provider.create_schema().await?;
        provider.sync().await?;
        Ok(provider)
    }

    async fn open_offline_synced(
        local_path: impl Into<PathBuf>,
        remote_url: String,
        auth_token: String,
        options: LibsqlEngineOptions,
        tuning: ProviderTuning,
    ) -> Result<Self, LibsqlProviderInitError> {
        let local_path = local_path.into();
        if let Some(parent) = local_path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(|e| {
                libsql::Error::ConnectionFailed(format!(
                    "failed to create offline-sync database directory: {e}"
                ))
            })?;
        }

        let mut builder = libsql::Builder::new_synced_database(local_path, remote_url, auth_token)
            .read_your_writes(options.read_your_writes)
            .remote_writes(options.remote_writes);
        if let Some(interval) = options.sync_interval {
            builder = builder.sync_interval(interval);
        }
        if let Some(key) = &options.remote_encryption_key_b64 {
            builder = builder.remote_encryption(Self::remote_encryption_context(key));
        }
        let db = builder.build().await?;
        let provider = Self {
            db,
            tuning,
            engine_options: options,
        };
        // Initial pull/bootstrap then ensure durable schema exists.
        let _ = provider.sync().await;
        provider.create_schema().await?;
        let _ = provider.sync().await;
        Ok(provider)
    }

    fn local_encryption_config(
        key: &[u8],
    ) -> Result<libsql::EncryptionConfig, LibsqlProviderInitError> {
        if key.is_empty() {
            return Err(LibsqlProviderInitError::InvalidConfig(
                "local encryption key must not be empty".into(),
            ));
        }
        Ok(libsql::EncryptionConfig::new(
            libsql::Cipher::Aes256Cbc,
            bytes::Bytes::copy_from_slice(key),
        ))
    }

    fn remote_encryption_context(key_b64: &str) -> libsql::EncryptionContext {
        libsql::EncryptionContext {
            key: libsql::EncryptionKey::Base64Encoded(key_b64.to_string()),
        }
    }

    pub fn database(&self) -> &libsql::Database {
        &self.db
    }

    pub fn tuning(&self) -> &ProviderTuning {
        &self.tuning
    }

    pub fn engine_options(&self) -> &LibsqlEngineOptions {
        &self.engine_options
    }

    /// Escape hatch: run arbitrary SQL (vectors, WASM UDFs, app tables, etc.).
    pub async fn execute_sql(&self, sql: &str) -> Result<u64, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("execute_sql", e))?;
        conn.execute(sql, ())
            .await
            .map_err(|e| Self::libsql_to_provider_error("execute_sql", e))
    }

    /// Escape hatch: query arbitrary SQL as text rows.
    pub async fn query_sql(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("query_sql", e))?;
        let mut rows = conn
            .query(sql, ())
            .await
            .map_err(|e| Self::libsql_to_provider_error("query_sql", e))?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("query_sql", e))?
        {
            let mut cells = Vec::new();
            let cols = row.column_count().max(0) as i32;
            for idx in 0..cols {
                match row.get_value(idx) {
                    Ok(Value::Null) => cells.push(None),
                    Ok(Value::Text(s)) => cells.push(Some(s)),
                    Ok(Value::Integer(i)) => cells.push(Some(i.to_string())),
                    Ok(Value::Real(f)) => cells.push(Some(f.to_string())),
                    Ok(Value::Blob(b)) => cells.push(Some(format!("\\x{}", Self::hex_encode(&b)))),
                    Err(e) => {
                        return Err(ProviderError::permanent(
                            "query_sql",
                            format!("Failed to read column {idx}: {e}"),
                        ));
                    }
                }
            }
            out.push(cells);
        }
        Ok(out)
    }

    /// Returns true if native vector SQL functions appear available.
    pub async fn engine_supports_vector(&self) -> Result<bool, ProviderError> {
        // Probe a few known libSQL/Turso vector entry points; any success counts.
        let probes = [
            "SELECT vector32('[1.0, 0.0]')",
            "SELECT vector('[1.0, 0.0]')",
            "SELECT vector_distance_cos(vector32('[1,0]'), vector32('[1,0]'))",
        ];
        for sql in probes {
            if self.query_sql(sql).await.is_ok() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Load a native SQLite/libSQL extension dylib (local engines).
    pub async fn load_extension(
        &self,
        dylib_path: impl AsRef<std::path::Path>,
        entry_point: Option<&str>,
    ) -> Result<(), ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("load_extension", e))?;
        conn.load_extension_enable()
            .map_err(|e| Self::libsql_to_provider_error("load_extension", e))?;
        let result = conn.load_extension(dylib_path.as_ref(), entry_point);
        let _ = conn.load_extension_disable();
        result.map_err(|e| Self::libsql_to_provider_error("load_extension", e))
    }

    /// Open a connection with remote-aware busy timeout applied when supported.
    ///
    /// `pub(crate)` so introspection and world helpers share one connect path.
    ///
    /// Local SQLite applies the pragma via `query` (PRAGMA returns a row).
    /// Self-hosted `sqld` over Hrana may reject it with "unsupported statement";
    /// that is ignored so remote mode still works.
    pub async fn connect(&self) -> libsql::Result<libsql::Connection> {
        let conn = self.db.connect()?;
        if self.tuning.busy_timeout_ms > 0 {
            // Use query: `PRAGMA busy_timeout = N` returns a row on local SQLite,
            // and libsql's execute() errors with ExecuteReturnedRows.
            match conn
                .query(
                    &format!("PRAGMA busy_timeout = {}", self.tuning.busy_timeout_ms),
                    (),
                )
                .await
            {
                Ok(mut rows) => {
                    // Drain optional result row.
                    let _ = rows.next().await;
                }
                Err(err) => {
                    let msg = err.to_string();
                    if !(msg.contains("unsupported statement")
                        || msg.contains("unsupported")
                        || msg.contains("not authorized")
                        || msg.contains("ExecuteReturnedRows"))
                    {
                        return Err(err);
                    }
                }
            }
        }
        Ok(conn)
    }

    /// Retry a closure while the provider error is marked retryable.
    pub async fn with_transient_retry<T, F, Fut>(
        &self,
        operation: &'static str,
        mut f: F,
    ) -> Result<T, ProviderError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, ProviderError>>,
    {
        let mut attempt = 0u32;
        loop {
            match f().await {
                Ok(value) => return Ok(value),
                Err(err)
                    if err.is_retryable() && attempt < self.tuning.max_transient_retries =>
                {
                    attempt += 1;
                    let shift = (attempt - 1).min(4);
                    let delay_ms = self
                        .tuning
                        .retry_base_delay_ms
                        .saturating_mul(1u64 << shift);
                    tracing::debug!(
                        operation,
                        attempt,
                        delay_ms,
                        error = %err,
                        "retrying transient libsql provider error"
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Err(err) => {
                    // Preserve original operation label when present.
                    let _ = operation;
                    return Err(err);
                }
            }
        }
    }

    /// Pull/push replication with the configured primary (no-op semantics for
    /// pure local DBs that do not support sync — those return `Ok(None)`).
    pub async fn sync(&self) -> Result<Option<u64>, LibsqlProviderInitError> {
        match self.db.sync().await {
            Ok(replicated) => Ok(replicated.frame_no()),
            Err(err) => {
                let msg = err.to_string();
                // Local / pure-remote databases are not embedded replicas.
                if msg.contains("not supported")
                    || msg.contains("does not support")
                    || msg.contains("no replicator")
                    || msg.contains("sync is not")
                {
                    return Ok(None);
                }
                Err(err.into())
            }
        }
    }

    /// Current replication index when available (embedded replica only).
    pub async fn replication_index(&self) -> Result<Option<u64>, LibsqlProviderInitError> {
        match self.db.replication_index().await {
            Ok(index) => Ok(index),
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("not supported")
                    || msg.contains("does not support")
                    || msg.contains("no replicator")
                {
                    return Ok(None);
                }
                Err(err.into())
            }
        }
    }

    /// Apply durable-provider schema migrations idempotently.
    ///
    /// Safe to call on local files and self-hosted remote `sqld` endpoints.
    /// Creates/updates tables, records [`SCHEMA_VERSION`], and ensures the
    /// PVM [`crate::WorldManifest`] (version fence + world_id).
    pub async fn migrate(&self) -> Result<(), LibsqlProviderInitError> {
        self.create_schema().await?;
        Ok(())
    }

    /// Return the applied schema version, if the meta table is present.
    pub async fn schema_version(&self) -> Result<Option<i64>, LibsqlProviderInitError> {
        let conn = self.connect().await?;
        let mut rows = conn
            .query(
                "SELECT version FROM schema_meta WHERE id = 1",
                (),
            )
            .await?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<i64>(0)?)),
            None => Ok(None),
        }
    }

    /// Read the PVM world manifest (identity + version fence metadata).
    pub async fn world_manifest(
        &self,
    ) -> Result<Option<crate::WorldManifest>, LibsqlProviderInitError> {
        let conn = self.connect().await?;
        crate::world::read_world_manifest(&conn).await
    }

    /// Open-world checklist report (manifest + fence status).
    pub async fn world_open_report(&self) -> Result<crate::WorldOpenReport, LibsqlProviderInitError> {
        let manifest = self
            .world_manifest()
            .await?
            .ok_or_else(|| {
                LibsqlProviderInitError::InvalidConfig(
                    "world_manifest missing; call migrate()/open first".into(),
                )
            })?;
        let mut notes = Vec::new();
        let schema_ok = crate::world::check_schema_fence(manifest.schema_version).is_ok();
        let format_ok = crate::world::check_format_fence(manifest.world_format_version).is_ok();
        if !schema_ok {
            notes.push(format!(
                "schema_version {} incompatible with host {}",
                manifest.schema_version, SCHEMA_VERSION
            ));
        }
        if !format_ok {
            notes.push(format!(
                "world_format_version {} incompatible with host {}",
                manifest.world_format_version,
                crate::WORLD_FORMAT_VERSION
            ));
        }
        if schema_ok && format_ok {
            notes.push("world fence ok; host may schedule work".into());
        }
        Ok(crate::WorldOpenReport {
            provider_name: manifest.provider_name.clone(),
            provider_version: manifest.provider_version.clone(),
            manifest,
            schema_ok,
            format_ok,
            notes,
        })
    }

    /// Checkpoint WAL into the main db file (best-effort on local engines).
    ///
    /// Call before [`crate::copy_world_package`] when the world was recently written.
    pub async fn checkpoint_wal(&self) -> Result<(), LibsqlProviderInitError> {
        let conn = self.connect().await?;
        // Ignore "unsupported" on pure remote Hrana if pragma is rejected.
        match conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await {
            Ok(mut rows) => {
                let _ = rows.next().await;
                Ok(())
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("unsupported") || msg.contains("ExecuteReturnedRows") {
                    // Some engines return rows via execute path; try query already done.
                    Ok(())
                } else {
                    // Still attempt execute form for engines that accept it.
                    match conn.execute("PRAGMA wal_checkpoint(TRUNCATE)", ()).await {
                        Ok(_) => Ok(()),
                        Err(e2) => {
                            let m2 = e2.to_string();
                            if m2.contains("unsupported") || m2.contains("ExecuteReturnedRows") {
                                Ok(())
                            } else {
                                Err(e2.into())
                            }
                        }
                    }
                }
            }
        }
    }

    /// Checkpoint (best-effort) then copy this local world to `dst_db`.
    ///
    /// Only valid for file-backed worlds. Remote-only topologies should use
    /// server-side backup/replication instead.
    pub async fn package_copy_to(
        &self,
        src_db: impl AsRef<std::path::Path>,
        dst_db: impl AsRef<std::path::Path>,
    ) -> Result<crate::WorldPackagePaths, LibsqlProviderInitError> {
        let _ = self.checkpoint_wal().await;
        crate::copy_world_package(src_db, dst_db)
    }

    async fn create_schema(&self) -> Result<(), LibsqlProviderInitError> {
        let conn = self.connect().await?;

        // Fence before destructive meta updates if an existing world is newer.
        if let Ok(Some(found)) = crate::world::read_world_manifest(&conn).await {
            crate::world::check_schema_fence(found.schema_version)?;
            crate::world::check_format_fence(found.world_format_version)?;
        }

        for statement in SCHEMA_STATEMENTS {
            conn.execute(statement, ()).await?;
        }
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS schema_meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL,
                applied_at_ms INTEGER NOT NULL
            )
            "#,
            (),
        )
        .await?;

        // If schema_meta already has a future version, refuse.
        let mut meta_rows = conn
            .query("SELECT version FROM schema_meta WHERE id = 1", ())
            .await?;
        if let Some(row) = meta_rows.next().await? {
            let found = row.get::<i64>(0)?;
            crate::world::check_schema_fence(found)?;
        }
        drop(meta_rows);

        let now_ms = Self::now_millis();
        conn.execute(
            r#"
            INSERT INTO schema_meta (id, version, applied_at_ms)
            VALUES (1, ?1, ?2)
            ON CONFLICT(id) DO UPDATE SET
                version = excluded.version,
                applied_at_ms = excluded.applied_at_ms
            "#,
            params![SCHEMA_VERSION, now_ms],
        )
        .await?;

        let _manifest = crate::world::ensure_world_manifest(
            &conn,
            "libsql-native",
            env!("CARGO_PKG_VERSION"),
        )
        .await?;
        Ok(())
    }

    pub(crate) fn libsql_to_provider_error(operation: &str, error: libsql::Error) -> ProviderError {
        let msg = error.to_string();
        let lower = msg.to_ascii_lowercase();
        // Classify permanent failures first: remote Hrana wrappers often embed the
        // underlying SQLite constraint text inside a transport error string.
        if msg.contains("UNIQUE constraint")
            || msg.contains("PRIMARY KEY")
            || msg.contains("FOREIGN KEY constraint")
            || msg.contains("NOT NULL constraint")
            || msg.contains("CHECK constraint")
            || msg.contains("constraint failed")
            || lower.contains("invalid lock token")
            || lower.contains("no such table")
            || lower.contains("syntax error")
        {
            return ProviderError::permanent(operation, msg);
        }

        // Transient: locks, network, and Hrana transport flakiness common on
        // multi-node / HTTP remotes under concurrent load.
        if msg.contains("database is locked")
            || msg.contains("SQLITE_BUSY")
            || msg.contains("SQLITE_LOCKED")
            || lower.contains("timeout")
            || lower.contains("timed out")
            || lower.contains("connection")
            || lower.contains("broken pipe")
            || lower.contains("connection reset")
            || lower.contains("connection refused")
            || lower.contains("temporarily unavailable")
            || lower.contains("try again")
            || lower.contains("stream closed")
            || lower.contains("stream error")
            || msg.contains("Hrana")
            || lower.contains("http2")
            || lower.contains("hyper::error")
            || lower.contains("status code: 5")
            || lower.contains("status: 5")
        {
            return ProviderError::retryable(operation, msg);
        }

        ProviderError::retryable(operation, msg)
    }

    /// Delete runtime rows while keeping schema (test/admin helper for shared remote DBs).
    pub async fn clear_runtime_data(&self) -> Result<(), ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("clear_runtime_data", e))?;
        // Order matters for parent_instance_id FK on instances.
        for statement in [
            "DELETE FROM history",
            "DELETE FROM executions",
            "DELETE FROM orchestrator_queue",
            "DELETE FROM worker_queue",
            "DELETE FROM instance_locks",
            "DELETE FROM kv_store",
            "DELETE FROM kv_delta",
            "DELETE FROM sessions",
            "DELETE FROM instances",
        ] {
            conn.execute(statement, ())
                .await
                .map_err(|e| Self::libsql_to_provider_error("clear_runtime_data", e))?;
        }
        Ok(())
    }

    pub(crate) fn now_millis() -> i64 {
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

    fn hex_encode(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xf) as usize] as char);
        }
        out
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

    fn placeholders(count: usize, start: usize) -> String {
        (0..count)
            .map(|i| format!("?{}", start + i))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn id_values(ids: &[String]) -> Vec<Value> {
        ids.iter().cloned().map(Value::Text).collect()
    }

    pub(crate) fn coerce_timestamp(value: Value) -> u64 {
        match value {
            Value::Integer(v) => v.max(0) as u64,
            Value::Real(v) => {
                if v.is_finite() && v > 0.0 {
                    v as u64
                } else {
                    0
                }
            }
            Value::Text(s) => s.parse::<i64>().ok().map(|v| v.max(0) as u64).unwrap_or(0),
            _ => 0,
        }
    }

    pub(crate) fn optional_text(value: Value) -> Option<String> {
        match value {
            Value::Text(s) => Some(s),
            Value::Null => None,
            _ => None,
        }
    }

    pub(crate) async fn query_count(
        conn: &libsql::Connection,
        sql: &str,
        params: impl libsql::params::IntoParams,
        operation: &'static str,
    ) -> Result<i64, ProviderError> {
        let mut rows = conn
            .query(sql, params)
            .await
            .map_err(|e| Self::libsql_to_provider_error(operation, e))?;
        let row = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error(operation, e))?
            .ok_or_else(|| {
                ProviderError::permanent(operation, "Expected count row but got none")
            })?;
        row.get::<i64>(0)
            .map_err(|e| Self::libsql_to_provider_error(operation, e))
    }

    async fn query_count_tx(
        tx: &libsql::Transaction,
        sql: &str,
        params: impl libsql::params::IntoParams,
        operation: &'static str,
    ) -> Result<i64, ProviderError> {
        let mut rows = tx
            .query(sql, params)
            .await
            .map_err(|e| Self::libsql_to_provider_error(operation, e))?;
        let row = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error(operation, e))?
            .ok_or_else(|| {
                ProviderError::permanent(operation, "Expected count row but got none")
            })?;
        row.get::<i64>(0)
            .map_err(|e| Self::libsql_to_provider_error(operation, e))
    }

    async fn collect_text_column(
        conn: &libsql::Connection,
        sql: &str,
        params: impl libsql::params::IntoParams,
        operation: &'static str,
    ) -> Result<Vec<String>, ProviderError> {
        let mut rows = conn
            .query(sql, params)
            .await
            .map_err(|e| Self::libsql_to_provider_error(operation, e))?;
        let mut values = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error(operation, e))?
        {
            values.push(
                row.get::<String>(0)
                    .map_err(|e| Self::libsql_to_provider_error(operation, e))?,
            );
        }
        Ok(values)
    }
}

/// Schema revision applied by [`NativeLibsqlProvider::migrate`].
pub use crate::world::SCHEMA_VERSION;

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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
        Some(self as &dyn ProviderAdmin)
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        let conn = self
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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
            .connect()
            .await
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

const DEFAULT_BULK_OPERATION_LIMIT: u32 = 1000;

#[async_trait::async_trait]
impl ProviderAdmin for NativeLibsqlProvider {
    async fn list_instances(&self) -> Result<Vec<String>, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_instances", e))?;
        Self::collect_text_column(
            &conn,
            "SELECT instance_id FROM instances ORDER BY created_at DESC",
            (),
            "list_instances",
        )
        .await
    }

    async fn list_instances_by_status(&self, status: &str) -> Result<Vec<String>, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_instances_by_status", e))?;
        Self::collect_text_column(
            &conn,
            r#"
            SELECT i.instance_id
            FROM instances i
            JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
            WHERE e.status = ?1
            ORDER BY i.created_at DESC
            "#,
            params![status],
            "list_instances_by_status",
        )
        .await
    }

    async fn list_executions(&self, instance: &str) -> Result<Vec<u64>, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_executions", e))?;
        let mut rows = conn
            .query(
                "SELECT execution_id FROM executions WHERE instance_id = ?1 ORDER BY execution_id",
                params![instance],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_executions", e))?;
        let mut executions = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_executions", e))?
        {
            let execution_id = row
                .get::<i64>(0)
                .map_err(|e| Self::libsql_to_provider_error("list_executions", e))?;
            executions.push(execution_id as u64);
        }
        Ok(executions)
    }

    async fn read_history_with_execution_id(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("read_history_with_execution_id", e))?;
        Self::read_execution_history(
            &conn,
            instance,
            execution_id,
            "read_history_with_execution_id",
        )
        .await
    }

    async fn read_history(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        let execution_id = self.latest_execution_id(instance).await?;
        self.read_history_with_execution_id(instance, execution_id)
            .await
    }

    async fn latest_execution_id(&self, instance: &str) -> Result<u64, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("latest_execution_id", e))?;
        let mut rows = conn
            .query(
                "SELECT COALESCE(MAX(execution_id), 1) FROM executions WHERE instance_id = ?1",
                params![instance],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("latest_execution_id", e))?;
        match rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("latest_execution_id", e))?
        {
            Some(row) => {
                let max_id = row
                    .get::<i64>(0)
                    .map_err(|e| Self::libsql_to_provider_error("latest_execution_id", e))?;
                Ok(max_id as u64)
            }
            None => Ok(1),
        }
    }

    async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_info", e))?;
        let mut rows = conn
            .query(
                r#"
                SELECT
                    i.instance_id,
                    i.orchestration_name,
                    i.orchestration_version,
                    i.current_execution_id,
                    i.created_at,
                    i.updated_at,
                    i.parent_instance_id,
                    e.status,
                    e.output
                FROM instances i
                LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
                WHERE i.instance_id = ?1
                "#,
                params![instance],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_info", e))?;
        let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_instance_info", e))?
        else {
            return Err(ProviderError::permanent(
                "get_instance_info",
                format!("Instance {instance} not found"),
            ));
        };

        let instance_id = row
            .get::<String>(0)
            .map_err(|e| Self::libsql_to_provider_error("get_instance_info", e))?;
        let orchestration_name = row
            .get::<String>(1)
            .map_err(|e| Self::libsql_to_provider_error("get_instance_info", e))?;
        let orchestration_version = match row.get_value(2).map_err(|e| {
            ProviderError::permanent(
                "get_instance_info",
                format!("Failed to get orchestration_version: {e}"),
            )
        })? {
            Value::Text(v) => v,
            _ => "unknown".to_string(),
        };
        let current_execution_id = row
            .get::<i64>(3)
            .map_err(|e| Self::libsql_to_provider_error("get_instance_info", e))?
            as u64;
        let created_at = Self::coerce_timestamp(row.get_value(4).map_err(|e| {
            ProviderError::permanent("get_instance_info", format!("Failed to get created_at: {e}"))
        })?);
        let updated_at = Self::coerce_timestamp(row.get_value(5).map_err(|e| {
            ProviderError::permanent("get_instance_info", format!("Failed to get updated_at: {e}"))
        })?);
        let parent_instance_id = Self::optional_text(row.get_value(6).map_err(|e| {
            ProviderError::permanent(
                "get_instance_info",
                format!("Failed to get parent_instance_id: {e}"),
            )
        })?);
        let status = match row.get_value(7).map_err(|e| {
            ProviderError::permanent("get_instance_info", format!("Failed to get status: {e}"))
        })? {
            Value::Text(v) => v,
            _ => "Unknown".to_string(),
        };
        let output = Self::optional_text(row.get_value(8).map_err(|e| {
            ProviderError::permanent("get_instance_info", format!("Failed to get output: {e}"))
        })?);

        Ok(InstanceInfo {
            instance_id,
            orchestration_name,
            orchestration_version,
            current_execution_id,
            status,
            output,
            created_at,
            updated_at,
            parent_instance_id,
        })
    }

    async fn get_execution_info(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<ExecutionInfo, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_execution_info", e))?;
        let mut rows = conn
            .query(
                r#"
                SELECT
                    e.execution_id,
                    e.status,
                    e.output,
                    e.started_at,
                    e.completed_at,
                    COUNT(h.event_id) as event_count
                FROM executions e
                LEFT JOIN history h ON e.instance_id = h.instance_id AND e.execution_id = h.execution_id
                WHERE e.instance_id = ?1 AND e.execution_id = ?2
                GROUP BY e.execution_id, e.status, e.output, e.started_at, e.completed_at
                "#,
                params![instance, execution_id as i64],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_execution_info", e))?;
        let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_execution_info", e))?
        else {
            return Err(ProviderError::permanent(
                "get_execution_info",
                format!("Execution {execution_id} not found for instance {instance}"),
            ));
        };

        let execution_id = row
            .get::<i64>(0)
            .map_err(|e| Self::libsql_to_provider_error("get_execution_info", e))?
            as u64;
        let status = row
            .get::<String>(1)
            .map_err(|e| Self::libsql_to_provider_error("get_execution_info", e))?;
        let output = Self::optional_text(row.get_value(2).map_err(|e| {
            ProviderError::permanent("get_execution_info", format!("Failed to get output: {e}"))
        })?);
        let started_at = Self::coerce_timestamp(row.get_value(3).map_err(|e| {
            ProviderError::permanent(
                "get_execution_info",
                format!("Failed to get started_at: {e}"),
            )
        })?);
        let completed_at = match row.get_value(4).map_err(|e| {
            ProviderError::permanent(
                "get_execution_info",
                format!("Failed to get completed_at: {e}"),
            )
        })? {
            Value::Null => None,
            value => Some(Self::coerce_timestamp(value)),
        };
        let event_count = row
            .get::<i64>(5)
            .map_err(|e| Self::libsql_to_provider_error("get_execution_info", e))?
            as usize;

        Ok(ExecutionInfo {
            execution_id,
            status,
            output,
            started_at,
            completed_at,
            event_count,
        })
    }

    async fn get_system_metrics(&self) -> Result<SystemMetrics, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_system_metrics", e))?;
        let mut rows = conn
            .query(
                r#"
                SELECT
                    COUNT(*) as total_instances,
                    COALESCE(SUM(CASE WHEN e.status = 'Running' THEN 1 ELSE 0 END), 0) as running_instances,
                    COALESCE(SUM(CASE WHEN e.status = 'Completed' THEN 1 ELSE 0 END), 0) as completed_instances,
                    COALESCE(SUM(CASE WHEN e.status = 'Failed' THEN 1 ELSE 0 END), 0) as failed_instances
                FROM instances i
                LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
                "#,
                (),
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_system_metrics", e))?;
        let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_system_metrics", e))?
        else {
            return Ok(SystemMetrics::default());
        };

        let total_instances = row
            .get::<i64>(0)
            .map_err(|e| Self::libsql_to_provider_error("get_system_metrics", e))?
            as u64;
        let running_instances = row
            .get::<i64>(1)
            .map_err(|e| Self::libsql_to_provider_error("get_system_metrics", e))?
            as u64;
        let completed_instances = row
            .get::<i64>(2)
            .map_err(|e| Self::libsql_to_provider_error("get_system_metrics", e))?
            as u64;
        let failed_instances = row
            .get::<i64>(3)
            .map_err(|e| Self::libsql_to_provider_error("get_system_metrics", e))?
            as u64;
        drop(rows);

        let total_executions =
            Self::query_count(&conn, "SELECT COUNT(*) FROM executions", (), "get_system_metrics")
                .await? as u64;
        let total_events =
            Self::query_count(&conn, "SELECT COUNT(*) FROM history", (), "get_system_metrics")
                .await? as u64;

        Ok(SystemMetrics {
            total_instances,
            total_executions,
            running_instances,
            completed_instances,
            failed_instances,
            total_events,
        })
    }

    async fn get_queue_depths(&self) -> Result<QueueDepths, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_queue_depths", e))?;
        let orchestrator_queue = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM orchestrator_queue WHERE lock_token IS NULL",
            (),
            "get_queue_depths",
        )
        .await? as usize;
        let worker_queue = Self::query_count(
            &conn,
            "SELECT COUNT(*) FROM worker_queue WHERE lock_token IS NULL",
            (),
            "get_queue_depths",
        )
        .await? as usize;

        Ok(QueueDepths {
            orchestrator_queue,
            worker_queue,
            timer_queue: 0,
        })
    }

    async fn list_children(&self, instance_id: &str) -> Result<Vec<String>, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_children", e))?;
        Self::collect_text_column(
            &conn,
            "SELECT instance_id FROM instances WHERE parent_instance_id = ?1",
            params![instance_id],
            "list_children",
        )
        .await
    }

    async fn get_parent_id(&self, instance_id: &str) -> Result<Option<String>, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_parent_id", e))?;
        let mut rows = conn
            .query(
                "SELECT parent_instance_id FROM instances WHERE instance_id = ?1",
                params![instance_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_parent_id", e))?;
        match rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_parent_id", e))?
        {
            Some(row) => Ok(Self::optional_text(row.get_value(0).map_err(|e| {
                ProviderError::permanent(
                    "get_parent_id",
                    format!("Failed to get parent_instance_id: {e}"),
                )
            })?)),
            None => Err(ProviderError::permanent(
                "get_parent_id",
                format!("Instance {instance_id} not found"),
            )),
        }
    }

    async fn delete_instances_atomic(
        &self,
        ids: &[String],
        force: bool,
    ) -> Result<DeleteInstanceResult, ProviderError> {
        if ids.is_empty() {
            return Ok(DeleteInstanceResult::default());
        }

        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?;
        let placeholders = Self::placeholders(ids.len(), 1);
        let id_params = Self::id_values(ids);

        if !force {
            let check_sql = format!(
                r#"
                SELECT i.instance_id, e.status
                FROM instances i
                LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
                WHERE i.instance_id IN ({placeholders})
                "#
            );
            let mut rows = conn
                .query(&check_sql, id_params.clone())
                .await
                .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?;
            while let Some(row) = rows
                .next()
                .await
                .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?
            {
                let status = Self::optional_text(row.get_value(1).map_err(|e| {
                    ProviderError::permanent(
                        "delete_instances_atomic",
                        format!("Failed to get status: {e}"),
                    )
                })?);
                if status.as_deref() == Some("Running") {
                    let instance_id = row
                        .get::<String>(0)
                        .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?;
                    return Err(ProviderError::permanent(
                        "delete_instances_atomic",
                        format!(
                            "Instance {instance_id} is still running. Use force=true to delete anyway, or cancel first."
                        ),
                    ));
                }
            }
        }

        let tx = conn
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?;

        let child_placeholders = Self::placeholders(ids.len(), ids.len() + 1);
        let orphan_sql = format!(
            r#"
            SELECT instance_id, parent_instance_id FROM instances
            WHERE parent_instance_id IN ({placeholders})
              AND instance_id NOT IN ({child_placeholders})
            LIMIT 1
            "#
        );
        let mut orphan_params = id_params.clone();
        orphan_params.extend(Self::id_values(ids));
        let mut orphan_rows = tx
            .query(&orphan_sql, orphan_params)
            .await
            .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?;
        if let Some(orphan_row) = orphan_rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?
        {
            let orphan_id = orphan_row
                .get::<String>(0)
                .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?;
            let parent_id = orphan_row
                .get::<String>(1)
                .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?;
            return Err(ProviderError::permanent(
                "delete_instances_atomic",
                format!(
                    "Cannot delete: instance {parent_id} has child {orphan_id} that was created after tree traversal. \
                     Re-fetch the tree and retry."
                ),
            ));
        }
        drop(orphan_rows);

        let mut result = DeleteInstanceResult::default();
        result.events_deleted = Self::query_count_tx(
            &tx,
            &format!("SELECT COUNT(*) FROM history WHERE instance_id IN ({placeholders})"),
            id_params.clone(),
            "delete_instances_atomic",
        )
        .await? as u64;
        result.executions_deleted = Self::query_count_tx(
            &tx,
            &format!("SELECT COUNT(*) FROM executions WHERE instance_id IN ({placeholders})"),
            id_params.clone(),
            "delete_instances_atomic",
        )
        .await? as u64;
        let orch_q_count = Self::query_count_tx(
            &tx,
            &format!(
                "SELECT COUNT(*) FROM orchestrator_queue WHERE instance_id IN ({placeholders})"
            ),
            id_params.clone(),
            "delete_instances_atomic",
        )
        .await?;
        let worker_q_count = Self::query_count_tx(
            &tx,
            &format!("SELECT COUNT(*) FROM worker_queue WHERE instance_id IN ({placeholders})"),
            id_params.clone(),
            "delete_instances_atomic",
        )
        .await?;
        result.queue_messages_deleted = (orch_q_count + worker_q_count) as u64;

        for table in [
            "history",
            "executions",
            "orchestrator_queue",
            "worker_queue",
            "instance_locks",
            "kv_store",
            "kv_delta",
            "instances",
        ] {
            let sql = format!("DELETE FROM {table} WHERE instance_id IN ({placeholders})");
            tx.execute(&sql, id_params.clone())
                .await
                .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?;
        }

        tx.commit()
            .await
            .map_err(|e| Self::libsql_to_provider_error("delete_instances_atomic", e))?;

        result.instances_deleted = ids.len() as u64;
        Ok(result)
    }

    async fn delete_instance_bulk(
        &self,
        filter: InstanceFilter,
    ) -> Result<DeleteInstanceResult, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("delete_instance_bulk", e))?;

        let mut sql = String::from(
            r#"
            SELECT i.instance_id
            FROM instances i
            LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
            WHERE i.parent_instance_id IS NULL
              AND e.status IN ('Completed', 'Failed', 'ContinuedAsNew')
            "#,
        );
        let mut values = Vec::new();
        let mut next_param = 1usize;

        if let Some(ref ids) = filter.instance_ids {
            if ids.is_empty() {
                return Ok(DeleteInstanceResult::default());
            }
            let placeholders = Self::placeholders(ids.len(), next_param);
            sql.push_str(&format!(" AND i.instance_id IN ({placeholders})"));
            values.extend(Self::id_values(ids));
            next_param += ids.len();
        }

        if let Some(completed_before) = filter.completed_before {
            sql.push_str(&format!(" AND e.completed_at < ?{next_param}"));
            values.push(Value::Integer(completed_before as i64));
        }

        let limit = filter.limit.unwrap_or(DEFAULT_BULK_OPERATION_LIMIT);
        sql.push_str(&format!(" LIMIT {limit}"));

        let instance_ids =
            Self::collect_text_column(&conn, &sql, values, "delete_instance_bulk").await?;
        if instance_ids.is_empty() {
            return Ok(DeleteInstanceResult::default());
        }

        let mut result = DeleteInstanceResult::default();
        for instance_id in &instance_ids {
            let tree = self.get_instance_tree(instance_id).await?;
            let tree_size = tree.all_ids.len() as u64;
            let delete_result = self.delete_instances_atomic(&tree.all_ids, true).await?;
            result.executions_deleted += delete_result.executions_deleted;
            result.events_deleted += delete_result.events_deleted;
            result.queue_messages_deleted += delete_result.queue_messages_deleted;
            result.instances_deleted += tree_size;
        }

        Ok(result)
    }

    async fn prune_executions(
        &self,
        instance_id: &str,
        options: PruneOptions,
    ) -> Result<PruneResult, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?;

        let mut current_rows = conn
            .query(
                "SELECT current_execution_id FROM instances WHERE instance_id = ?1",
                params![instance_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?;
        let current_execution_id = match current_rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?
        {
            Some(row) => row
                .get::<i64>(0)
                .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?,
            None => {
                return Err(ProviderError::permanent(
                    "prune_executions",
                    format!("Instance {instance_id} not found"),
                ));
            }
        };
        drop(current_rows);

        let mut conditions = vec![
            "instance_id = ?1".to_string(),
            "execution_id != ?2".to_string(),
            "status != 'Running'".to_string(),
        ];
        let mut values = vec![
            Value::Text(instance_id.to_string()),
            Value::Integer(current_execution_id),
        ];
        let mut next_param = 3usize;

        if let Some(keep_last) = options.keep_last {
            conditions.push(format!(
                "execution_id NOT IN (SELECT execution_id FROM executions WHERE instance_id = ?{next_param} ORDER BY execution_id DESC LIMIT {keep_last})"
            ));
            values.push(Value::Text(instance_id.to_string()));
            next_param += 1;
        }

        if let Some(completed_before) = options.completed_before {
            conditions.push(format!("completed_at < ?{next_param}"));
            values.push(Value::Integer(completed_before as i64));
        }

        let sql = format!(
            "SELECT execution_id FROM executions WHERE {}",
            conditions.join(" AND ")
        );
        let mut rows = conn
            .query(&sql, values)
            .await
            .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?;
        let mut execution_ids = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?
        {
            execution_ids.push(
                row.get::<i64>(0)
                    .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?,
            );
        }
        drop(rows);

        if execution_ids.is_empty() {
            return Ok(PruneResult {
                instances_processed: 1,
                ..Default::default()
            });
        }

        let tx = conn
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?;

        let mut result = PruneResult {
            instances_processed: 1,
            ..Default::default()
        };

        for exec_id in &execution_ids {
            let history_count = Self::query_count_tx(
                &tx,
                "SELECT COUNT(*) FROM history WHERE instance_id = ?1 AND execution_id = ?2",
                params![instance_id, *exec_id],
                "prune_executions",
            )
            .await?;
            tx.execute(
                "DELETE FROM history WHERE instance_id = ?1 AND execution_id = ?2",
                params![instance_id, *exec_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?;
            tx.execute(
                "DELETE FROM executions WHERE instance_id = ?1 AND execution_id = ?2",
                params![instance_id, *exec_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?;
            result.executions_deleted += 1;
            result.events_deleted += history_count as u64;
        }

        tx.commit()
            .await
            .map_err(|e| Self::libsql_to_provider_error("prune_executions", e))?;

        Ok(result)
    }

    async fn prune_executions_bulk(
        &self,
        filter: InstanceFilter,
        options: PruneOptions,
    ) -> Result<PruneResult, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("prune_executions_bulk", e))?;

        let mut sql = String::from(
            r#"
            SELECT i.instance_id
            FROM instances i
            LEFT JOIN executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
            WHERE 1=1
            "#,
        );
        let mut values = Vec::new();
        let mut next_param = 1usize;

        if let Some(ref ids) = filter.instance_ids {
            if ids.is_empty() {
                return Ok(PruneResult::default());
            }
            let placeholders = Self::placeholders(ids.len(), next_param);
            sql.push_str(&format!(" AND i.instance_id IN ({placeholders})"));
            values.extend(Self::id_values(ids));
            next_param += ids.len();
        }

        if let Some(completed_before) = filter.completed_before {
            sql.push_str(&format!(" AND e.completed_at < ?{next_param}"));
            values.push(Value::Integer(completed_before as i64));
        }

        let limit = filter.limit.unwrap_or(1000);
        sql.push_str(&format!(" LIMIT {limit}"));

        let instance_ids =
            Self::collect_text_column(&conn, &sql, values, "prune_executions_bulk").await?;

        let mut result = PruneResult::default();
        for instance_id in &instance_ids {
            let single_result = self.prune_executions(instance_id, options.clone()).await?;
            result.instances_processed += single_result.instances_processed;
            result.executions_deleted += single_result.executions_deleted;
            result.events_deleted += single_result.events_deleted;
        }

        Ok(result)
    }
}
