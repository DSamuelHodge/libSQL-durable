#![cfg(feature = "native-libsql")]

//! Self-hosted remote `sqld` validation suite.
//!
//! These tests are **opt-in**. They run only when `LIBSQL_REMOTE_URL` is set
//! and the endpoint is reachable. Without a local `sqld`, the suite is skipped
//! so everyday `cargo test` stays green on machines without Docker/containers.
//!
//! Start a local server (binary preferred on older Macs):
//!
//! ```sh
//! ./scripts/install-sqld.sh
//! ./scripts/start-sqld.sh
//! ./scripts/run-remote-tests.sh
//! ```

use std::sync::Arc;
use std::time::Duration;

use duroxide::provider_validations::bulk_deletion::test_delete_instance_bulk_safety_and_limits;
use duroxide::provider_validations::prune::test_prune_safety;
use duroxide::provider_validations::{
    ProviderFactory, test_get_execution_info, test_get_instance_info, test_get_queue_depths,
    test_get_system_metrics, test_list_executions, test_list_instances,
    test_list_instances_by_status, test_multi_operation_atomic_ack,
    test_worker_peek_lock_semantics,
};
use duroxide::providers::{Provider, TagFilter, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use libsql_durable::{LibsqlDatabaseConfig, LibsqlProvider, SCHEMA_VERSION};

fn remote_url() -> Option<String> {
    std::env::var("LIBSQL_REMOTE_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn auth_token() -> String {
    std::env::var("LIBSQL_AUTH_TOKEN").unwrap_or_default()
}

fn unique_suffix() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}

async fn remote_endpoint_reachable(url: &str) -> bool {
    match LibsqlProvider::new_remote(url.to_string(), auth_token()).await {
        Ok(_) => true,
        Err(err) => {
            eprintln!("remote endpoint not reachable ({url}): {err}");
            false
        }
    }
}

async fn require_remote_provider() -> Option<LibsqlProvider> {
    let url = remote_url()?;
    if !remote_endpoint_reachable(&url).await {
        eprintln!("skipping remote tests: LIBSQL_REMOTE_URL={url} is set but not reachable");
        return None;
    }
    let provider = LibsqlProvider::new(LibsqlDatabaseConfig::remote(url, auth_token()))
        .await
        .expect("remote provider init failed after reachability check");
    Some(provider)
}

struct RemoteLibsqlTestFactory {
    url: String,
    auth_token: String,
}

#[async_trait::async_trait]
impl ProviderFactory for RemoteLibsqlTestFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        // Shared remote DB: wipe runtime rows so Duroxide fixed instance IDs
        // (e.g. "instance-A") do not collide with prior runs or sibling tests.
        let provider = LibsqlProvider::new(LibsqlDatabaseConfig::remote(
            self.url.clone(),
            self.auth_token.clone(),
        ))
        .await
        .expect("failed to create remote validation provider");
        provider
            .clear_runtime_data()
            .await
            .expect("failed to clear remote runtime data for isolation");
        Arc::new(provider)
    }

    fn lock_timeout(&self) -> Duration {
        Duration::from_secs(2)
    }
}

fn remote_factory() -> Option<RemoteLibsqlTestFactory> {
    let url = remote_url()?;
    Some(RemoteLibsqlTestFactory {
        url,
        auth_token: auth_token(),
    })
}

macro_rules! skip_if_no_remote {
    () => {
        if remote_url().is_none() {
            eprintln!("skipping remote test: set LIBSQL_REMOTE_URL to enable");
            return;
        }
    };
}

#[tokio::test]
async fn remote_bootstrap_and_schema_version() {
    skip_if_no_remote!();
    let Some(provider) = require_remote_provider().await else {
        return;
    };

    provider
        .migrate()
        .await
        .expect("remote migrate should be idempotent");

    let version = provider
        .schema_version()
        .await
        .expect("schema_version query failed");
    assert_eq!(
        version,
        Some(SCHEMA_VERSION),
        "remote schema_meta should record SCHEMA_VERSION"
    );

    // Second migrate must not fail.
    provider
        .migrate()
        .await
        .expect("second remote migrate should succeed");
    let version_again = provider
        .schema_version()
        .await
        .expect("schema_version re-query failed");
    assert_eq!(version_again, Some(SCHEMA_VERSION));
}

#[tokio::test]
async fn remote_core_provider_validations() {
    skip_if_no_remote!();
    let Some(factory) = remote_factory() else {
        return;
    };
    if !remote_endpoint_reachable(&factory.url).await {
        return;
    }

    test_multi_operation_atomic_ack(&factory).await;
    test_worker_peek_lock_semantics(&factory).await;
}

#[tokio::test]
async fn remote_management_validations() {
    skip_if_no_remote!();
    let Some(factory) = remote_factory() else {
        return;
    };
    if !remote_endpoint_reachable(&factory.url).await {
        return;
    }

    test_list_instances(&factory).await;
    test_list_instances_by_status(&factory).await;
    test_list_executions(&factory).await;
    test_get_instance_info(&factory).await;
    test_get_execution_info(&factory).await;
    test_get_system_metrics(&factory).await;
    test_get_queue_depths(&factory).await;
}

#[tokio::test]
async fn remote_admin_prune_and_bulk_delete() {
    skip_if_no_remote!();
    let Some(factory) = remote_factory() else {
        return;
    };
    if !remote_endpoint_reachable(&factory.url).await {
        return;
    }

    test_prune_safety(&factory).await;
    test_delete_instance_bulk_safety_and_limits(&factory).await;
}

#[tokio::test]
async fn remote_append_read_and_queues_smoke() {
    skip_if_no_remote!();
    let Some(provider) = require_remote_provider().await else {
        return;
    };

    let suffix = unique_suffix();
    let instance = format!("remote-history-{suffix}");

    provider
        .append_with_execution(
            &instance,
            INITIAL_EXECUTION_ID,
            vec![Event::with_event_id(
                INITIAL_EVENT_ID,
                instance.clone(),
                INITIAL_EXECUTION_ID,
                None,
                EventKind::OrchestrationStarted {
                    name: "RemoteOrch".to_string(),
                    version: "1.0.0".to_string(),
                    input: "{}".to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
        )
        .await
        .expect("remote append failed");

    let history = provider.read(&instance).await.expect("remote read failed");
    assert_eq!(history.len(), 1);

    let worker_item = WorkItem::ActivityExecute {
        instance: format!("remote-worker-{suffix}"),
        execution_id: INITIAL_EXECUTION_ID,
        id: 42,
        name: "DoRemote".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    provider
        .enqueue_for_worker(worker_item.clone())
        .await
        .expect("remote enqueue worker failed");

    let (fetched, token, attempts) = provider
        .fetch_work_item(
            Duration::from_secs(30),
            Duration::ZERO,
            None,
            &TagFilter::default_only(),
        )
        .await
        .expect("remote fetch worker failed")
        .expect("expected remote worker item");
    assert_eq!(fetched, worker_item);
    assert_eq!(attempts, 1);
    provider
        .ack_work_item(&token, None)
        .await
        .expect("remote ack worker failed");

    // Management capability must be exposed over remote too.
    let mgmt = provider
        .as_management_capability()
        .expect("remote provider must expose ProviderAdmin");
    let listed = mgmt.list_instances().await.expect("list_instances");
    // Instance may not appear until metadata ack; list should still succeed.
    let _ = listed;
}
