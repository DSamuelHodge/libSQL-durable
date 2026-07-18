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
#[cfg(feature = "native-libsql")]
mod native;

pub use duroxide;

#[cfg(feature = "compat-sqlite")]
pub use compat::CompatSqliteProvider;
#[cfg(feature = "native-libsql")]
pub use native::NativeLibsqlProvider;

#[derive(Debug, thiserror::Error)]
pub enum LibsqlProviderInitError {
    #[cfg(feature = "compat-sqlite")]
    #[error("SQLite compatibility provider initialization failed: {0}")]
    Sqlite(#[from] sqlx::Error),
    #[cfg(feature = "native-libsql")]
    #[error("libSQL initialization failed: {0}")]
    Libsql(#[from] libsql::Error),
    #[cfg(feature = "compat-sqlite")]
    #[error("remote libSQL backends require the native libSQL transaction port")]
    RemoteNativePortRequired,
    #[error("no provider backend feature is enabled")]
    NoBackendFeatureEnabled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LibsqlDatabaseConfig {
    InMemory,
    Local {
        path: PathBuf,
    },
    Remote {
        url: String,
        auth_token: String,
    },
    RemoteReplica {
        local_path: PathBuf,
        remote_url: String,
        auth_token: String,
    },
}

impl LibsqlDatabaseConfig {
    pub fn from_env() -> Self {
        if let Ok(remote_url) = std::env::var("LIBSQL_REMOTE_URL") {
            let auth_token = std::env::var("LIBSQL_AUTH_TOKEN").unwrap_or_default();
            if let Ok(local_path) = std::env::var("LIBSQL_REPLICA_PATH") {
                return Self::RemoteReplica {
                    local_path: PathBuf::from(local_path),
                    remote_url,
                    auth_token,
                };
            }

            return Self::Remote {
                url: remote_url,
                auth_token,
            };
        }

        let url = std::env::var("LIBSQL_DATABASE_URL")
            .unwrap_or_else(|_| "file:./stress-libsql.db".to_string());
        Self::from_local_url(&url)
    }

    pub fn from_local_url(url: &str) -> Self {
        if url == ":memory:" || url.contains(":memory:") || url.contains("mode=memory") {
            return Self::InMemory;
        }

        let path = url
            .strip_prefix("file:")
            .or_else(|| url.strip_prefix("sqlite:"))
            .unwrap_or(url);
        Self::Local {
            path: PathBuf::from(path),
        }
    }
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
        Self::new(LibsqlDatabaseConfig::InMemory).await
    }

    pub async fn new_local(path: impl Into<PathBuf>) -> Result<Self, LibsqlProviderInitError> {
        Self::new(LibsqlDatabaseConfig::Local { path: path.into() }).await
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
