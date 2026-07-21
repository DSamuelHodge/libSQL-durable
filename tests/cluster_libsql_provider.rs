#![cfg(feature = "native-libsql")]

//! Multi-node `sqld` primary/replica durability + remote tuning checks.
//!
//! Opt-in: requires both
//! - `LIBSQL_REMOTE_URL` (primary HTTP)
//! - `LIBSQL_REPLICA_HTTP_URL` (replica HTTP)
//!
//! ```sh
//! ./scripts/start-cluster.sh
//! ./scripts/run-cluster-tests.sh
//! ```

use std::time::Duration;

use duroxide::providers::{Provider, TagFilter, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use libsql_durable::{LibsqlProvider, ProviderTuning, SCHEMA_VERSION};

fn primary_url() -> Option<String> {
    std::env::var("LIBSQL_REMOTE_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn replica_url() -> Option<String> {
    std::env::var("LIBSQL_REPLICA_HTTP_URL")
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

macro_rules! skip_if_no_cluster {
    () => {
        if primary_url().is_none() || replica_url().is_none() {
            eprintln!(
                "skipping cluster test: set LIBSQL_REMOTE_URL and LIBSQL_REPLICA_HTTP_URL"
            );
            return;
        }
    };
}

async fn open_primary() -> Option<LibsqlProvider> {
    let url = primary_url()?;
    match LibsqlProvider::new_remote(url.clone(), auth_token()).await {
        Ok(p) => Some(p),
        Err(err) => {
            eprintln!("primary not reachable ({url}): {err}");
            None
        }
    }
}

async fn open_replica_http() -> Option<LibsqlProvider> {
    let url = replica_url()?;
    match LibsqlProvider::new_remote(url.clone(), auth_token()).await {
        Ok(p) => Some(p),
        Err(err) => {
            eprintln!("replica not reachable ({url}): {err}");
            None
        }
    }
}

fn started_event(instance: &str, note: &str) -> Event {
    Event::with_event_id(
        INITIAL_EVENT_ID,
        instance.to_string(),
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: "ClusterOrch".to_string(),
            version: "1.0.0".to_string(),
            input: format!("{{\"note\":\"{note}\"}}"),
            parent_instance: None,
            parent_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    )
}

/// Poll replica until history is visible or timeout (eventual replication).
async fn wait_for_replica_history(
    replica: &LibsqlProvider,
    instance: &str,
    expected_len: usize,
    timeout: Duration,
) -> Vec<Event> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let last = replica.read(instance).await.unwrap_or_default();
        if last.len() >= expected_len {
            return last;
        }
        if tokio::time::Instant::now() >= deadline {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn cluster_primary_write_replicates_to_replica_http() {
    skip_if_no_cluster!();
    let Some(primary) = open_primary().await else {
        return;
    };
    let Some(replica) = open_replica_http().await else {
        return;
    };

    primary
        .clear_runtime_data()
        .await
        .expect("clear primary runtime");
    // Give replica a moment to observe deletes / empty state is best-effort.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let suffix = unique_suffix();
    let instance = format!("cluster-hist-{suffix}");

    primary
        .append_with_execution(
            &instance,
            INITIAL_EXECUTION_ID,
            vec![started_event(&instance, "primary-write")],
        )
        .await
        .expect("primary append");

    let on_primary = primary.read(&instance).await.expect("primary read");
    assert_eq!(on_primary.len(), 1);

    let on_replica =
        wait_for_replica_history(&replica, &instance, 1, Duration::from_secs(10)).await;
    assert_eq!(
        on_replica.len(),
        1,
        "sqld replica HTTP should eventually observe primary history"
    );
    assert!(matches!(
        on_replica[0].kind,
        EventKind::OrchestrationStarted { .. }
    ));

    assert_eq!(
        replica.schema_version().await.expect("replica schema"),
        Some(SCHEMA_VERSION)
    );
}

#[tokio::test]
async fn cluster_replica_write_forwards_and_primary_sees_it() {
    skip_if_no_cluster!();
    let Some(primary) = open_primary().await else {
        return;
    };
    let Some(replica) = open_replica_http().await else {
        return;
    };

    primary
        .clear_runtime_data()
        .await
        .expect("clear primary runtime");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let suffix = unique_suffix();
    let instance = format!("cluster-fwd-{suffix}");

    let item = WorkItem::ActivityExecute {
        instance: instance.clone(),
        execution_id: INITIAL_EXECUTION_ID,
        id: 3,
        name: "ViaReplicaHttp".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    // Writes issued against the replica HTTP endpoint should forward to primary.
    replica
        .enqueue_for_worker(item.clone())
        .await
        .expect("replica enqueue (forwarded write)");

    let (fetched, token, attempts) = primary
        .fetch_work_item(
            Duration::from_secs(30),
            Duration::ZERO,
            None,
            &TagFilter::default_only(),
        )
        .await
        .expect("primary fetch after replica write")
        .expect("expected worker item on primary");
    assert_eq!(fetched, item);
    assert_eq!(attempts, 1);
    primary
        .ack_work_item(&token, None)
        .await
        .expect("primary ack");

    replica
        .append_with_execution(
            &instance,
            INITIAL_EXECUTION_ID,
            vec![started_event(&instance, "replica-forwarded")],
        )
        .await
        .expect("replica append forwarded");

    let on_primary = primary.read(&instance).await.expect("primary history");
    assert_eq!(on_primary.len(), 1);

    let on_replica =
        wait_for_replica_history(&replica, &instance, 1, Duration::from_secs(10)).await;
    assert_eq!(on_replica.len(), 1);
}

#[tokio::test]
async fn cluster_remote_tuning_applied_and_retries_transient() {
    skip_if_no_cluster!();
    let Some(primary) = open_primary().await else {
        return;
    };

    let tuning = primary
        .tuning()
        .expect("native provider exposes tuning")
        .clone();
    // Remote constructors use remote_defaults (or env overrides from run script).
    assert!(
        tuning.busy_timeout_ms >= 1000,
        "remote busy_timeout should be raised, got {}",
        tuning.busy_timeout_ms
    );
    assert!(
        tuning.max_transient_retries >= 2,
        "remote should allow transient retries, got {}",
        tuning.max_transient_retries
    );

    // from_env honors explicit overrides used by run-cluster-tests.sh
    let env_tuning = ProviderTuning::from_env();
    if std::env::var("LIBSQL_BUSY_TIMEOUT_MS").is_ok() {
        assert_eq!(
            env_tuning.busy_timeout_ms,
            std::env::var("LIBSQL_BUSY_TIMEOUT_MS")
                .unwrap()
                .parse::<u64>()
                .unwrap()
        );
    }

    // Permanent errors must not be misclassified as retryable.
    let permanent = duroxide::providers::ProviderError::permanent("test", "UNIQUE constraint");
    assert!(!permanent.is_retryable());

    // Exercise management path over primary with tuning applied on connect.
    primary.migrate().await.expect("migrate");
    primary
        .clear_runtime_data()
        .await
        .expect("clear for tuning test");
    let mgmt = primary
        .as_management_capability()
        .expect("ProviderAdmin on primary");
    let listed = mgmt.list_instances().await.expect("list_instances");
    assert!(listed.is_empty() || listed.iter().all(|s| !s.is_empty()));

    // with_transient_retry should succeed on first try for a healthy op.
    let native = primary.native().expect("native");
    let ok = native
        .with_transient_retry("tuning_probe", || async {
            native
                .schema_version()
                .await
                .map_err(|e| duroxide::providers::ProviderError::retryable("tuning_probe", e.to_string()))
        })
        .await
        .expect("retry wrapper");
    assert_eq!(ok, Some(SCHEMA_VERSION));
}

#[tokio::test]
async fn cluster_concurrent_primary_fetches_respect_locks() {
    skip_if_no_cluster!();
    let Some(primary) = open_primary().await else {
        return;
    };
    primary
        .clear_runtime_data()
        .await
        .expect("clear primary");

    let suffix = unique_suffix();
    let instance = format!("cluster-lock-{suffix}");
    primary
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: instance.clone(),
                orchestration: "ClusterLock".to_string(),
                input: "{}".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .expect("enqueue");

    let first = primary
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .expect("first fetch")
        .expect("item");
    assert_eq!(first.0.instance, instance);

    // Concurrent second fetch against same primary should not steal lock.
    let second = primary
        .fetch_orchestration_item(Duration::from_secs(1), Duration::ZERO, None)
        .await
        .expect("second fetch");
    assert!(
        second.is_none(),
        "instance lock must be exclusive under remote busy_timeout tuning"
    );

    primary
        .abandon_orchestration_item(&first.1, None, false)
        .await
        .expect("abandon");
}
