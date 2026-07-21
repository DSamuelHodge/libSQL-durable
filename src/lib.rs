use std::path::PathBuf;
use std::time::Duration;

use duroxide::providers::{
    DispatcherCapabilityFilter, ExecutionMetadata, OrchestrationItem, Provider, ProviderAdmin,
    ProviderError, ScheduledActivityIdentifier, SessionFetchConfig, TagFilter, WorkItem,
};
use duroxide::{Event, SystemStats};

#[cfg(all(feature = "compat-sqlite", feature = "native-libsql"))]
compile_error!(
    "features `compat-sqlite` and `native-libsql` cannot be enabled together because SQLx's bundled SQLite and libsql-ffi both export SQLite C symbols"
);

#[cfg(feature = "compat-sqlite")]
mod compat;
mod config;
#[cfg(feature = "native-libsql")]
mod definitions;
#[cfg(feature = "native-libsql")]
mod fork;
#[cfg(feature = "native-libsql")]
mod heal;
#[cfg(feature = "native-libsql")]
mod interpreter;
#[cfg(feature = "native-libsql")]
mod introspect;
#[cfg(feature = "native-libsql")]
mod mesh;
#[cfg(feature = "native-libsql")]
mod native;
#[cfg(feature = "native-libsql")]
mod policy;
#[cfg(feature = "native-libsql")]
mod world;

pub use config::{LibsqlDatabaseConfig, LibsqlDatabaseMode, LibsqlEngineOptions};
pub use duroxide;

#[cfg(feature = "compat-sqlite")]
pub use compat::CompatSqliteProvider;
#[cfg(feature = "native-libsql")]
pub use definitions::{
    PVM_DEF_V1, ProcessDefinition, ProcessDefinitionPin, validate_definition_body,
};
#[cfg(feature = "native-libsql")]
pub use fork::{ForkOptions, ForkResult, discard_world_package, fork_world_files};
#[cfg(feature = "native-libsql")]
pub use heal::{
    DEFAULT_RUNAWAY_HISTORY_EVENTS, HealActionResult, HealOptions, HealReport, HealingAuditRow,
};
#[cfg(feature = "native-libsql")]
pub use interpreter::{INTERPRETED_ORCH_NAME, interpreted_orchestrations, wrap_interpret_input};
#[cfg(feature = "native-libsql")]
pub use introspect::{
    BlockReason, DEFAULT_POISON_ATTEMPT_THRESHOLD, NextWorkItem, ProcessRow, QueueSnapshot,
    TraceEvent, WhyBlocked, WorkQueueKind, WorldHealth,
};
#[cfg(feature = "native-libsql")]
pub use mesh::{MeshStatus, WorldPeer, WorldRef};
#[cfg(feature = "native-libsql")]
pub use native::{NativeLibsqlProvider, ProviderTuning};
#[cfg(feature = "native-libsql")]
pub use policy::{PolicyAuditRow, RuntimePolicy};
#[cfg(feature = "native-libsql")]
pub use world::{
    MIN_COMPATIBLE_SCHEMA_VERSION, SCHEMA_VERSION, WORLD_FORMAT_VERSION, WorldManifest,
    WorldOpenReport, WorldPackagePaths, copy_world_package, open_world_checklist,
};

#[derive(Debug, thiserror::Error)]
pub enum LibsqlProviderInitError {
    #[cfg(feature = "compat-sqlite")]
    #[error("SQLite compatibility provider initialization failed: {0}")]
    Sqlite(#[from] sqlx::Error),
    #[cfg(feature = "native-libsql")]
    #[error("libSQL initialization failed: {0}")]
    Libsql(#[from] libsql::Error),
    #[cfg(feature = "compat-sqlite")]
    #[error("remote/offline libSQL backends require the native libSQL transaction port")]
    RemoteNativePortRequired,
    #[error(
        "local encryption was requested but the crate was built without the `encryption` feature"
    )]
    EncryptionFeatureDisabled,
    #[error("no provider backend feature is enabled")]
    NoBackendFeatureEnabled,
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    /// World package failed the version fence (schema/format too new or too old).
    #[error("world incompatible with this host: {0}")]
    WorldIncompatible(String),
}

pub enum LibsqlProvider {
    #[cfg(feature = "compat-sqlite")]
    Compat(CompatSqliteProvider),
    #[cfg(feature = "native-libsql")]
    Native(NativeLibsqlProvider),
}

impl LibsqlProvider {
    pub async fn new(config: LibsqlDatabaseConfig) -> Result<Self, LibsqlProviderInitError> {
        #[cfg(feature = "compat-sqlite")]
        {
            return Ok(Self::Compat(CompatSqliteProvider::new(config).await?));
        }

        #[cfg(feature = "native-libsql")]
        {
            return Ok(Self::Native(NativeLibsqlProvider::new(config).await?));
        }

        #[allow(unreachable_code)]
        Err(LibsqlProviderInitError::NoBackendFeatureEnabled)
    }

    pub async fn from_env() -> Result<Self, LibsqlProviderInitError> {
        Self::new(LibsqlDatabaseConfig::from_env()).await
    }

    pub async fn new_in_memory() -> Result<Self, LibsqlProviderInitError> {
        Self::new(LibsqlDatabaseConfig::in_memory()).await
    }

    pub async fn new_local(path: impl Into<PathBuf>) -> Result<Self, LibsqlProviderInitError> {
        Self::new(LibsqlDatabaseConfig::local(path)).await
    }

    /// Connect to a self-hosted remote `sqld`/`libsql-server` and apply schema.
    #[cfg(feature = "native-libsql")]
    pub async fn new_remote(
        url: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> Result<Self, LibsqlProviderInitError> {
        Self::new(LibsqlDatabaseConfig::remote(url, auth_token)).await
    }

    /// Open an embedded remote replica that syncs from a self-hosted primary.
    #[cfg(feature = "native-libsql")]
    pub async fn new_remote_replica(
        local_path: impl Into<PathBuf>,
        remote_url: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> Result<Self, LibsqlProviderInitError> {
        Self::new(LibsqlDatabaseConfig::remote_replica(
            local_path, remote_url, auth_token,
        ))
        .await
    }

    /// Offline-capable local DB that periodically syncs with a remote primary.
    #[cfg(feature = "native-libsql")]
    pub async fn new_offline_synced(
        local_path: impl Into<PathBuf>,
        remote_url: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> Result<Self, LibsqlProviderInitError> {
        Self::new(LibsqlDatabaseConfig::offline_synced(
            local_path, remote_url, auth_token,
        ))
        .await
    }

    /// Re-apply schema migrations (idempotent).
    #[cfg(feature = "native-libsql")]
    pub async fn migrate(&self) -> Result<(), LibsqlProviderInitError> {
        match self {
            Self::Native(provider) => provider.migrate().await,
        }
    }

    /// Sync an embedded replica / offline-synced DB with its primary.
    #[cfg(feature = "native-libsql")]
    pub async fn sync(&self) -> Result<Option<u64>, LibsqlProviderInitError> {
        match self {
            Self::Native(provider) => provider.sync().await,
        }
    }

    /// Current replication index when available.
    #[cfg(feature = "native-libsql")]
    pub async fn replication_index(&self) -> Result<Option<u64>, LibsqlProviderInitError> {
        match self {
            Self::Native(provider) => provider.replication_index().await,
        }
    }

    /// Applied schema version from `schema_meta`, if present.
    #[cfg(feature = "native-libsql")]
    pub async fn schema_version(&self) -> Result<Option<i64>, LibsqlProviderInitError> {
        match self {
            Self::Native(provider) => provider.schema_version().await,
        }
    }

    /// PVM world identity and version fence metadata.
    #[cfg(feature = "native-libsql")]
    pub async fn world_manifest(&self) -> Result<Option<WorldManifest>, LibsqlProviderInitError> {
        match self {
            Self::Native(provider) => provider.world_manifest().await,
        }
    }

    /// Open-world checklist report after migrate.
    #[cfg(feature = "native-libsql")]
    pub async fn world_open_report(&self) -> Result<WorldOpenReport, LibsqlProviderInitError> {
        match self {
            Self::Native(provider) => provider.world_open_report().await,
        }
    }

    /// Best-effort WAL checkpoint (local engines).
    #[cfg(feature = "native-libsql")]
    pub async fn checkpoint_wal(&self) -> Result<(), LibsqlProviderInitError> {
        match self {
            Self::Native(provider) => provider.checkpoint_wal().await,
        }
    }

    /// Checkpoint then copy a local world package to `dst_db`.
    #[cfg(feature = "native-libsql")]
    pub async fn package_copy_to(
        &self,
        src_db: impl AsRef<std::path::Path>,
        dst_db: impl AsRef<std::path::Path>,
    ) -> Result<WorldPackagePaths, LibsqlProviderInitError> {
        match self {
            Self::Native(provider) => provider.package_copy_to(src_db, dst_db).await,
        }
    }

    /// PVM `ps` — non-terminal processes and lock holders.
    #[cfg(feature = "native-libsql")]
    pub async fn ps(&self) -> Result<Vec<ProcessRow>, ProviderError> {
        match self {
            Self::Native(provider) => provider.introspect_ps().await,
        }
    }

    /// PVM `next` — next unlocked visible work items.
    #[cfg(feature = "native-libsql")]
    pub async fn next_work(&self, limit: u32) -> Result<Vec<NextWorkItem>, ProviderError> {
        match self {
            Self::Native(provider) => provider.introspect_next(limit).await,
        }
    }

    /// PVM `why_blocked` — classify why an instance is not progressing.
    #[cfg(feature = "native-libsql")]
    pub async fn why_blocked(&self, instance_id: &str) -> Result<WhyBlocked, ProviderError> {
        match self {
            Self::Native(provider) => provider.introspect_why_blocked(instance_id).await,
        }
    }

    /// PVM `trace` — ordered journal projection.
    #[cfg(feature = "native-libsql")]
    pub async fn trace(
        &self,
        instance_id: &str,
        execution_id: Option<u64>,
        limit: u32,
    ) -> Result<Vec<TraceEvent>, ProviderError> {
        match self {
            Self::Native(provider) => {
                provider
                    .introspect_trace(instance_id, execution_id, limit)
                    .await
            }
        }
    }

    /// PVM `queues` — depths and pressure signals.
    #[cfg(feature = "native-libsql")]
    pub async fn queues(&self) -> Result<QueueSnapshot, ProviderError> {
        match self {
            Self::Native(provider) => provider.introspect_queues().await,
        }
    }

    /// PVM `health` — fence, counts, poison, queues.
    #[cfg(feature = "native-libsql")]
    pub async fn health(
        &self,
        poison_attempt_threshold: Option<i64>,
    ) -> Result<WorldHealth, ProviderError> {
        match self {
            Self::Native(provider) => provider.introspect_health(poison_attempt_threshold).await,
        }
    }

    /// Run standard healing suite (reclaim, quarantine, fence orphans, compact).
    #[cfg(feature = "native-libsql")]
    pub async fn heal(&self, options: HealOptions) -> Result<HealReport, ProviderError> {
        match self {
            Self::Native(provider) => provider.heal(options).await,
        }
    }

    /// Reclaim expired instance and queue locks.
    #[cfg(feature = "native-libsql")]
    pub async fn heal_reclaim_expired_locks(&self) -> Result<HealActionResult, ProviderError> {
        match self {
            Self::Native(provider) => provider.heal_reclaim_expired_locks().await,
        }
    }

    /// Quarantine poison queue items above attempt threshold.
    #[cfg(feature = "native-libsql")]
    pub async fn heal_quarantine_poison(
        &self,
        attempt_threshold: Option<i64>,
    ) -> Result<HealActionResult, ProviderError> {
        match self {
            Self::Native(provider) => provider.heal_quarantine_poison(attempt_threshold).await,
        }
    }

    /// Delete queue/lock rows for missing instances (post force-delete fence).
    #[cfg(feature = "native-libsql")]
    pub async fn heal_fence_orphan_queue_items(&self) -> Result<HealActionResult, ProviderError> {
        match self {
            Self::Native(provider) => provider.heal_fence_orphan_queue_items().await,
        }
    }

    /// Compact runaway histories via prune policy.
    #[cfg(feature = "native-libsql")]
    pub async fn heal_compact_histories(
        &self,
        options: &HealOptions,
    ) -> Result<HealActionResult, ProviderError> {
        match self {
            Self::Native(provider) => provider.heal_compact_histories(options).await,
        }
    }

    /// Recent healing audit log (newest first).
    #[cfg(feature = "native-libsql")]
    pub async fn healing_audit_log(
        &self,
        limit: u32,
    ) -> Result<Vec<HealingAuditRow>, ProviderError> {
        match self {
            Self::Native(provider) => provider.healing_audit_log(limit).await,
        }
    }

    /// Count quarantined work items.
    #[cfg(feature = "native-libsql")]
    pub async fn healing_quarantine_count(&self) -> Result<u64, ProviderError> {
        match self {
            Self::Native(provider) => provider.healing_quarantine_count().await,
        }
    }

    // --- Phase 4: process definitions as data ---

    /// Insert a process definition. Same `(name, version)` is immutable.
    #[cfg(feature = "native-libsql")]
    pub async fn put_process_definition(
        &self,
        name: &str,
        version: &str,
        body_json: &str,
    ) -> Result<(), ProviderError> {
        match self {
            Self::Native(provider) => {
                provider
                    .put_process_definition(name, version, body_json)
                    .await
            }
        }
    }

    /// Insert or force-replace a process definition.
    #[cfg(feature = "native-libsql")]
    pub async fn put_process_definition_ex(
        &self,
        name: &str,
        version: &str,
        body_json: &str,
        force: bool,
    ) -> Result<(), ProviderError> {
        match self {
            Self::Native(provider) => {
                provider
                    .put_process_definition_ex(name, version, body_json, force)
                    .await
            }
        }
    }

    /// Fetch a process definition by name and version.
    #[cfg(feature = "native-libsql")]
    pub async fn get_process_definition(
        &self,
        name: &str,
        version: &str,
    ) -> Result<Option<ProcessDefinition>, ProviderError> {
        match self {
            Self::Native(provider) => provider.get_process_definition(name, version).await,
        }
    }

    /// List all stored process definitions.
    #[cfg(feature = "native-libsql")]
    pub async fn list_process_definitions(&self) -> Result<Vec<ProcessDefinition>, ProviderError> {
        match self {
            Self::Native(provider) => provider.list_process_definitions().await,
        }
    }

    /// List definitions for one name (all versions).
    #[cfg(feature = "native-libsql")]
    pub async fn list_process_definitions_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<ProcessDefinition>, ProviderError> {
        match self {
            Self::Native(provider) => provider.list_process_definitions_by_name(name).await,
        }
    }

    /// Delete a definition version.
    #[cfg(feature = "native-libsql")]
    pub async fn delete_process_definition(
        &self,
        name: &str,
        version: &str,
    ) -> Result<bool, ProviderError> {
        match self {
            Self::Native(provider) => provider.delete_process_definition(name, version).await,
        }
    }

    /// Pin an instance to a definition version.
    #[cfg(feature = "native-libsql")]
    pub async fn pin_process_definition(
        &self,
        instance_id: &str,
        definition_name: &str,
        definition_version: &str,
    ) -> Result<(), ProviderError> {
        match self {
            Self::Native(provider) => {
                provider
                    .pin_process_definition(instance_id, definition_name, definition_version)
                    .await
            }
        }
    }

    /// Get the definition pin for an instance, if any.
    #[cfg(feature = "native-libsql")]
    pub async fn get_process_definition_pin(
        &self,
        instance_id: &str,
    ) -> Result<Option<ProcessDefinitionPin>, ProviderError> {
        match self {
            Self::Native(provider) => provider.get_process_definition_pin(instance_id).await,
        }
    }

    /// Resolve pin → full definition for an instance.
    #[cfg(feature = "native-libsql")]
    pub async fn resolve_definition_for_instance(
        &self,
        instance_id: &str,
    ) -> Result<Option<ProcessDefinition>, ProviderError> {
        match self {
            Self::Native(provider) => provider.resolve_definition_for_instance(instance_id).await,
        }
    }

    // --- Phase 5: fork + time travel ---

    /// Fork this file-backed world to `dst_db` (world-grain subprocess).
    #[cfg(feature = "native-libsql")]
    pub async fn fork_world_to(
        &self,
        src_db: impl AsRef<std::path::Path>,
        dst_db: impl AsRef<std::path::Path>,
        options: ForkOptions,
    ) -> Result<ForkResult, LibsqlProviderInitError> {
        match self {
            Self::Native(provider) => provider.fork_world_to(src_db, dst_db, options).await,
        }
    }

    /// Fork and open the child as a live provider.
    #[cfg(feature = "native-libsql")]
    pub async fn fork_and_open(
        &self,
        src_db: impl AsRef<std::path::Path>,
        dst_db: impl AsRef<std::path::Path>,
        options: ForkOptions,
    ) -> Result<(ForkResult, LibsqlProvider), LibsqlProviderInitError> {
        match self {
            Self::Native(provider) => provider.fork_and_open(src_db, dst_db, options).await,
        }
    }

    /// Delete history events with event_id greater than `after_event_id`.
    #[cfg(feature = "native-libsql")]
    pub async fn time_travel_truncate(
        &self,
        after_event_id: u64,
        only_instance: Option<&str>,
    ) -> Result<u64, ProviderError> {
        match self {
            Self::Native(provider) => {
                provider
                    .time_travel_truncate(after_event_id, only_instance)
                    .await
            }
        }
    }

    /// Keep one instance's rows; delete others (explore sandbox).
    #[cfg(feature = "native-libsql")]
    pub async fn retain_instance_only(&self, instance_id: &str) -> Result<(), ProviderError> {
        match self {
            Self::Native(provider) => provider.retain_instance_only(instance_id).await,
        }
    }

    /// Clear queues and locks for a clean explore fork.
    #[cfg(feature = "native-libsql")]
    pub async fn clear_scheduler_state(&self) -> Result<(), ProviderError> {
        match self {
            Self::Native(provider) => provider.clear_scheduler_state().await,
        }
    }

    // --- Phase 6: adaptive runtime policy ---

    /// Current runtime policy (defaults persisted on first read).
    #[cfg(feature = "native-libsql")]
    pub async fn get_runtime_policy(&self) -> Result<RuntimePolicy, ProviderError> {
        match self {
            Self::Native(provider) => provider.get_runtime_policy().await,
        }
    }

    /// Set runtime policy and append an audit row.
    #[cfg(feature = "native-libsql")]
    pub async fn set_runtime_policy(
        &self,
        policy: &RuntimePolicy,
        source: &str,
    ) -> Result<(), ProviderError> {
        match self {
            Self::Native(provider) => provider.set_runtime_policy(policy, source).await,
        }
    }

    /// Adjust policy from observed health/queue pressure (bounded heuristics).
    #[cfg(feature = "native-libsql")]
    pub async fn adapt_policy_from_health(&self) -> Result<RuntimePolicy, ProviderError> {
        match self {
            Self::Native(provider) => provider.adapt_policy_from_health().await,
        }
    }

    /// Recent policy audit log (newest first).
    #[cfg(feature = "native-libsql")]
    pub async fn policy_audit_log(&self, limit: u32) -> Result<Vec<PolicyAuditRow>, ProviderError> {
        match self {
            Self::Native(provider) => provider.policy_audit_log(limit).await,
        }
    }

    // --- Phase 7: world mesh ---

    /// Register or update a mesh peer.
    #[cfg(feature = "native-libsql")]
    pub async fn register_mesh_peer(
        &self,
        peer_world_id: &str,
        endpoint: &str,
        role: &str,
        meta_json: Option<&str>,
    ) -> Result<(), ProviderError> {
        match self {
            Self::Native(provider) => {
                provider
                    .register_mesh_peer(peer_world_id, endpoint, role, meta_json)
                    .await
            }
        }
    }

    /// List registered mesh peers.
    #[cfg(feature = "native-libsql")]
    pub async fn list_mesh_peers(&self) -> Result<Vec<WorldPeer>, ProviderError> {
        match self {
            Self::Native(provider) => provider.list_mesh_peers().await,
        }
    }

    /// Add an explicit cross-world reference.
    #[cfg(feature = "native-libsql")]
    pub async fn add_world_ref(
        &self,
        local_instance_id: &str,
        remote_world_id: &str,
        remote_instance_id: &str,
        note: Option<&str>,
    ) -> Result<(), ProviderError> {
        match self {
            Self::Native(provider) => {
                provider
                    .add_world_ref(local_instance_id, remote_world_id, remote_instance_id, note)
                    .await
            }
        }
    }

    /// List cross-world refs (optionally filtered by local instance).
    #[cfg(feature = "native-libsql")]
    pub async fn list_world_refs(
        &self,
        local_instance_id: Option<&str>,
    ) -> Result<Vec<WorldRef>, ProviderError> {
        match self {
            Self::Native(provider) => provider.list_world_refs(local_instance_id).await,
        }
    }

    /// Mesh summary: local world id, peer count, ref count, peers.
    #[cfg(feature = "native-libsql")]
    pub async fn mesh_status(&self) -> Result<MeshStatus, ProviderError> {
        match self {
            Self::Native(provider) => provider.mesh_status().await,
        }
    }

    /// Clear orchestration runtime rows (keeps schema and world_manifest).
    #[cfg(feature = "native-libsql")]
    pub async fn clear_runtime_data(&self) -> Result<(), ProviderError> {
        match self {
            Self::Native(provider) => provider.clear_runtime_data().await,
        }
    }

    /// Execute arbitrary SQL against the underlying libSQL engine.
    ///
    /// This is the escape hatch for engine features the Duroxide provider does
    /// not model (native vectors, WASM UDFs, custom tables, extensions).
    #[cfg(feature = "native-libsql")]
    pub async fn execute_sql(&self, sql: &str) -> Result<u64, ProviderError> {
        match self {
            Self::Native(provider) => provider.execute_sql(sql).await,
        }
    }

    /// Query arbitrary SQL; returns rows as text cells (NULL → None).
    #[cfg(feature = "native-libsql")]
    pub async fn query_sql(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>, ProviderError> {
        match self {
            Self::Native(provider) => provider.query_sql(sql).await,
        }
    }

    /// Probe whether the connected engine supports native vector functions.
    #[cfg(feature = "native-libsql")]
    pub async fn engine_supports_vector(&self) -> Result<bool, ProviderError> {
        match self {
            Self::Native(provider) => provider.engine_supports_vector().await,
        }
    }

    /// Load a SQLite/libSQL extension (local engines; may be unsupported remote).
    #[cfg(feature = "native-libsql")]
    pub async fn load_extension(
        &self,
        dylib_path: impl AsRef<std::path::Path>,
        entry_point: Option<&str>,
    ) -> Result<(), ProviderError> {
        match self {
            Self::Native(provider) => provider.load_extension(dylib_path, entry_point).await,
        }
    }

    #[cfg(feature = "compat-sqlite")]
    pub fn compat(&self) -> Option<&CompatSqliteProvider> {
        match self {
            Self::Compat(provider) => Some(provider),
        }
    }

    #[cfg(feature = "native-libsql")]
    pub fn native(&self) -> Option<&NativeLibsqlProvider> {
        match self {
            Self::Native(provider) => Some(provider),
        }
    }

    /// Current connection/retry tuning (native backend only).
    #[cfg(feature = "native-libsql")]
    pub fn tuning(&self) -> Option<&ProviderTuning> {
        match self {
            Self::Native(provider) => Some(provider.tuning()),
        }
    }

    /// Engine options used to open this provider (native only).
    #[cfg(feature = "native-libsql")]
    pub fn engine_options(&self) -> Option<&LibsqlEngineOptions> {
        match self {
            Self::Native(provider) => Some(provider.engine_options()),
        }
    }
}

#[async_trait::async_trait]
impl Provider for LibsqlProvider {
    fn name(&self) -> &str {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.name(),
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.name(),
        }
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => {
                provider
                    .fetch_orchestration_item(lock_timeout, poll_timeout, filter)
                    .await
            }
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => {
                provider
                    .fetch_orchestration_item(lock_timeout, poll_timeout, filter)
                    .await
            }
        }
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
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => {
                provider
                    .ack_orchestration_item(
                        lock_token,
                        execution_id,
                        history_delta,
                        worker_items,
                        orchestrator_items,
                        metadata,
                        cancelled_activities,
                    )
                    .await
            }
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => {
                provider
                    .ack_orchestration_item(
                        lock_token,
                        execution_id,
                        history_delta,
                        worker_items,
                        orchestrator_items,
                        metadata,
                        cancelled_activities,
                    )
                    .await
            }
        }
    }

    async fn abandon_orchestration_item(
        &self,
        lock_token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => {
                provider
                    .abandon_orchestration_item(lock_token, delay, ignore_attempt)
                    .await
            }
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => {
                provider
                    .abandon_orchestration_item(lock_token, delay, ignore_attempt)
                    .await
            }
        }
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.read(instance).await,
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.read(instance).await,
        }
    }

    async fn read_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.read_with_execution(instance, execution_id).await,
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.read_with_execution(instance, execution_id).await,
        }
    }

    async fn append_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
        new_events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => {
                provider
                    .append_with_execution(instance, execution_id, new_events)
                    .await
            }
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => {
                provider
                    .append_with_execution(instance, execution_id, new_events)
                    .await
            }
        }
    }

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.enqueue_for_worker(item).await,
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.enqueue_for_worker(item).await,
        }
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => {
                provider
                    .fetch_work_item(lock_timeout, poll_timeout, session, tag_filter)
                    .await
            }
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => {
                provider
                    .fetch_work_item(lock_timeout, poll_timeout, session, tag_filter)
                    .await
            }
        }
    }

    async fn ack_work_item(
        &self,
        token: &str,
        completion: Option<WorkItem>,
    ) -> Result<(), ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.ack_work_item(token, completion).await,
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.ack_work_item(token, completion).await,
        }
    }

    async fn renew_work_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.renew_work_item_lock(token, extend_for).await,
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.renew_work_item_lock(token, extend_for).await,
        }
    }

    async fn renew_session_lock(
        &self,
        owner_ids: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => {
                provider
                    .renew_session_lock(owner_ids, extend_for, idle_timeout)
                    .await
            }
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => {
                provider
                    .renew_session_lock(owner_ids, extend_for, idle_timeout)
                    .await
            }
        }
    }

    async fn cleanup_orphaned_sessions(
        &self,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.cleanup_orphaned_sessions(idle_timeout).await,
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.cleanup_orphaned_sessions(idle_timeout).await,
        }
    }

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => {
                provider
                    .abandon_work_item(token, delay, ignore_attempt)
                    .await
            }
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => {
                provider
                    .abandon_work_item(token, delay, ignore_attempt)
                    .await
            }
        }
    }

    async fn renew_orchestration_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => {
                provider
                    .renew_orchestration_item_lock(token, extend_for)
                    .await
            }
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => {
                provider
                    .renew_orchestration_item_lock(token, extend_for)
                    .await
            }
        }
    }

    async fn enqueue_for_orchestrator(
        &self,
        item: WorkItem,
        delay: Option<Duration>,
    ) -> Result<(), ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.enqueue_for_orchestrator(item, delay).await,
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.enqueue_for_orchestrator(item, delay).await,
        }
    }

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.as_management_capability(),
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.as_management_capability(),
        }
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => {
                provider
                    .get_custom_status(instance, last_seen_version)
                    .await
            }
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => {
                provider
                    .get_custom_status(instance, last_seen_version)
                    .await
            }
        }
    }

    async fn get_kv_value(
        &self,
        instance: &str,
        key: &str,
    ) -> Result<Option<String>, ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.get_kv_value(instance, key).await,
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.get_kv_value(instance, key).await,
        }
    }

    async fn get_kv_all_values(
        &self,
        instance: &str,
    ) -> Result<std::collections::HashMap<String, String>, ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.get_kv_all_values(instance).await,
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.get_kv_all_values(instance).await,
        }
    }

    async fn get_instance_stats(
        &self,
        instance: &str,
    ) -> Result<Option<SystemStats>, ProviderError> {
        match self {
            #[cfg(feature = "compat-sqlite")]
            Self::Compat(provider) => provider.get_instance_stats(instance).await,
            #[cfg(feature = "native-libsql")]
            Self::Native(provider) => provider.get_instance_stats(instance).await,
        }
    }
}
