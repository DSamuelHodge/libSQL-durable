use std::path::{Path, PathBuf};
use std::time::Duration;

use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::{
    DispatcherCapabilityFilter, ExecutionMetadata, OrchestrationItem, Provider, ProviderAdmin,
    ProviderError, ScheduledActivityIdentifier, SessionFetchConfig, TagFilter, WorkItem,
};
use duroxide::{Event, SystemStats};

use crate::{LibsqlDatabaseConfig, LibsqlDatabaseMode, LibsqlProviderInitError};

pub struct CompatSqliteProvider {
    inner: SqliteProvider,
}

impl CompatSqliteProvider {
    pub async fn new(config: LibsqlDatabaseConfig) -> Result<Self, LibsqlProviderInitError> {
        match config.mode {
            LibsqlDatabaseMode::InMemory => Self::new_in_memory().await,
            LibsqlDatabaseMode::Local { path } => Self::new_local(path).await,
            LibsqlDatabaseMode::Remote { .. }
            | LibsqlDatabaseMode::RemoteReplica { .. }
            | LibsqlDatabaseMode::OfflineSynced { .. } => {
                Err(LibsqlProviderInitError::RemoteNativePortRequired)
            }
        }
    }

    pub async fn new_in_memory() -> Result<Self, LibsqlProviderInitError> {
        Ok(Self {
            inner: SqliteProvider::new_in_memory().await?,
        })
    }

    pub async fn new_local(path: impl Into<PathBuf>) -> Result<Self, LibsqlProviderInitError> {
        let path = path.into();
        create_file_if_needed(&path)?;

        let sqlite_url = format!("sqlite:{}", path.display());
        Ok(Self {
            inner: SqliteProvider::new(&sqlite_url, None).await?,
        })
    }

    pub fn inner(&self) -> &SqliteProvider {
        &self.inner
    }

    pub fn sqlite_pool(&self) -> &sqlx::SqlitePool {
        self.inner.get_pool()
    }
}

fn create_file_if_needed(path: &Path) -> Result<(), sqlx::Error> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|e| {
            sqlx::Error::Protocol(format!("failed to create database directory: {e}"))
        })?;
    }

    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| sqlx::Error::Protocol(format!("failed to create database file: {e}")))?;

    Ok(())
}

#[async_trait::async_trait]
impl Provider for CompatSqliteProvider {
    fn name(&self) -> &str {
        "libsql-compat-sqlite"
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
        self.inner
            .fetch_orchestration_item(lock_timeout, poll_timeout, filter)
            .await
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
        self.inner
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

    async fn abandon_orchestration_item(
        &self,
        lock_token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        self.inner
            .abandon_orchestration_item(lock_token, delay, ignore_attempt)
            .await
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        self.inner.read(instance).await
    }

    async fn read_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        self.inner.read_with_execution(instance, execution_id).await
    }

    async fn append_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
        new_events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        self.inner
            .append_with_execution(instance, execution_id, new_events)
            .await
    }

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        self.inner.enqueue_for_worker(item).await
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        self.inner
            .fetch_work_item(lock_timeout, poll_timeout, session, tag_filter)
            .await
    }

    async fn ack_work_item(
        &self,
        token: &str,
        completion: Option<WorkItem>,
    ) -> Result<(), ProviderError> {
        self.inner.ack_work_item(token, completion).await
    }

    async fn renew_work_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        self.inner.renew_work_item_lock(token, extend_for).await
    }

    async fn renew_session_lock(
        &self,
        owner_ids: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        self.inner
            .renew_session_lock(owner_ids, extend_for, idle_timeout)
            .await
    }

    async fn cleanup_orphaned_sessions(
        &self,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        self.inner.cleanup_orphaned_sessions(idle_timeout).await
    }

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        self.inner
            .abandon_work_item(token, delay, ignore_attempt)
            .await
    }

    async fn renew_orchestration_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        self.inner
            .renew_orchestration_item_lock(token, extend_for)
            .await
    }

    async fn enqueue_for_orchestrator(
        &self,
        item: WorkItem,
        delay: Option<Duration>,
    ) -> Result<(), ProviderError> {
        self.inner.enqueue_for_orchestrator(item, delay).await
    }

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        self.inner.as_management_capability()
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        self.inner
            .get_custom_status(instance, last_seen_version)
            .await
    }

    async fn get_kv_value(
        &self,
        instance: &str,
        key: &str,
    ) -> Result<Option<String>, ProviderError> {
        self.inner.get_kv_value(instance, key).await
    }

    async fn get_kv_all_values(
        &self,
        instance: &str,
    ) -> Result<std::collections::HashMap<String, String>, ProviderError> {
        self.inner.get_kv_all_values(instance).await
    }

    async fn get_instance_stats(
        &self,
        instance: &str,
    ) -> Result<Option<SystemStats>, ProviderError> {
        self.inner.get_instance_stats(instance).await
    }
}
