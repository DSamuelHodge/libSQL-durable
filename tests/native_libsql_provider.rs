#![cfg(feature = "native-libsql")]

use std::sync::Arc;
use std::time::Duration;

use duroxide::provider_validations::bulk_deletion::test_delete_instance_bulk_safety_and_limits;
use duroxide::provider_validations::capability_filtering::test_fetch_with_compatible_filter_returns_item;
use duroxide::provider_validations::prune::test_prune_safety;
use duroxide::provider_validations::sessions::test_session_affinity_same_worker;
use duroxide::provider_validations::tag_filtering::test_tag_round_trip_preservation;
use duroxide::provider_validations::{
    ProviderFactory, test_abandon_releases_lock_immediately, test_abandon_work_item_releases_lock,
    test_ack_work_item_none_deletes_without_enqueue,
    test_cancelled_activities_deleted_from_worker_queue,
    test_continue_as_new_creates_new_execution, test_exclusive_instance_lock,
    test_execution_history_persistence, test_execution_id_sequencing, test_execution_isolation,
    test_get_execution_info, test_get_instance_info, test_get_instance_stats_history,
    test_get_instance_stats_kv, test_get_instance_stats_nonexistent, test_get_queue_depths,
    test_get_system_metrics, test_instance_creation_via_metadata, test_latest_execution_detection,
    test_list_executions, test_list_instances, test_list_instances_by_status,
    test_lock_expires_after_timeout, test_lock_renewal_on_ack, test_lost_lock_token_handling,
    test_multi_operation_atomic_ack, test_no_instance_creation_on_enqueue,
    test_null_version_handling, test_sub_orchestration_instance_creation,
    test_timer_delayed_visibility, test_worker_ack_atomicity,
    test_worker_item_immediate_visibility, test_worker_lock_renewal_success,
    test_worker_peek_lock_semantics,
};
use duroxide::providers::{Provider, TagFilter, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use libsql::params;
use libsql_durable::{LibsqlDatabaseConfig, LibsqlProvider};
use tempfile::TempDir;

async fn native_provider() -> (TempDir, LibsqlProvider) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let path = dir.path().join("native.db");
    let provider = LibsqlProvider::new(LibsqlDatabaseConfig::Local { path })
        .await
        .expect("failed to create native provider");
    (dir, provider)
}

struct NativeLibsqlTestFactory;

#[async_trait::async_trait]
impl ProviderFactory for NativeLibsqlTestFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        let path = std::env::temp_dir().join(format!(
            "libsql-durable-native-validation-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after UNIX epoch")
                .as_nanos()
        ));
        let provider = LibsqlProvider::new(LibsqlDatabaseConfig::Local { path })
            .await
            .expect("failed to create native validation provider");
        Arc::new(provider)
    }

    fn lock_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }
}

fn started_event(instance: &str) -> Event {
    Event::with_event_id(
        INITIAL_EVENT_ID,
        instance.to_string(),
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: "TestOrch".to_string(),
            version: "1.0.0".to_string(),
            input: "{}".to_string(),
            parent_instance: None,
            parent_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    )
}

#[tokio::test]
async fn native_multi_operation_atomic_ack_validation() {
    let factory = NativeLibsqlTestFactory;
    test_multi_operation_atomic_ack(&factory).await;
}

#[tokio::test]
async fn native_provider_core_validations() {
    let factory = NativeLibsqlTestFactory;

    test_instance_creation_via_metadata(&factory).await;
    test_exclusive_instance_lock(&factory).await;
    test_multi_operation_atomic_ack(&factory).await;
    test_worker_peek_lock_semantics(&factory).await;
    test_timer_delayed_visibility(&factory).await;
    test_list_instances(&factory).await;
}

#[tokio::test]
async fn native_provider_management_validations() {
    let factory = NativeLibsqlTestFactory;

    test_list_instances_by_status(&factory).await;
    test_list_executions(&factory).await;
    test_get_instance_info(&factory).await;
    test_get_execution_info(&factory).await;
    test_get_system_metrics(&factory).await;
    test_get_queue_depths(&factory).await;
}

#[tokio::test]
async fn native_provider_extended_contract_validations() {
    let factory = NativeLibsqlTestFactory;

    test_fetch_with_compatible_filter_returns_item(&factory).await;
    test_tag_round_trip_preservation(&factory).await;
    test_session_affinity_same_worker(&factory).await;
    test_prune_safety(&factory).await;
    test_delete_instance_bulk_safety_and_limits(&factory).await;
}

#[tokio::test]
async fn native_provider_additional_contract_validations() {
    let factory = NativeLibsqlTestFactory;

    test_no_instance_creation_on_enqueue(&factory).await;
    test_null_version_handling(&factory).await;
    test_sub_orchestration_instance_creation(&factory).await;
    test_worker_ack_atomicity(&factory).await;
    test_lost_lock_token_handling(&factory).await;
    test_worker_item_immediate_visibility(&factory).await;
    test_lock_expires_after_timeout(&factory).await;
    test_abandon_releases_lock_immediately(&factory).await;
    test_abandon_work_item_releases_lock(&factory).await;
    test_lock_renewal_on_ack(&factory).await;
    test_worker_lock_renewal_success(&factory).await;
    test_execution_isolation(&factory).await;
    test_latest_execution_detection(&factory).await;
    test_execution_id_sequencing(&factory).await;
    test_continue_as_new_creates_new_execution(&factory).await;
    test_execution_history_persistence(&factory).await;
    test_ack_work_item_none_deletes_without_enqueue(&factory).await;
    test_cancelled_activities_deleted_from_worker_queue(&factory).await;
    test_get_instance_stats_nonexistent(&factory).await;
    test_get_instance_stats_history(&factory).await;
    test_get_instance_stats_kv(&factory).await;
}

#[tokio::test]
async fn native_append_and_read_history() {
    let (_dir, provider) = native_provider().await;
    provider
        .append_with_execution(
            "native-history",
            INITIAL_EXECUTION_ID,
            vec![started_event("native-history")],
        )
        .await
        .expect("append failed");

    let history = provider.read("native-history").await.expect("read failed");
    assert_eq!(history.len(), 1);
    assert!(matches!(
        history[0].kind,
        EventKind::OrchestrationStarted { .. }
    ));
}

#[tokio::test]
async fn native_worker_queue_peek_lock_ack() {
    let (_dir, provider) = native_provider().await;
    let item = WorkItem::ActivityExecute {
        instance: "native-worker".to_string(),
        execution_id: INITIAL_EXECUTION_ID,
        id: 7,
        name: "DoThing".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    provider
        .enqueue_for_worker(item.clone())
        .await
        .expect("enqueue worker failed");

    let (fetched, token, attempts) = provider
        .fetch_work_item(
            Duration::from_secs(30),
            Duration::ZERO,
            None,
            &TagFilter::default_only(),
        )
        .await
        .expect("fetch worker failed")
        .expect("expected worker item");
    assert_eq!(fetched, item);
    assert_eq!(attempts, 1);

    provider
        .ack_work_item(&token, None)
        .await
        .expect("ack worker failed");

    let next = provider
        .fetch_work_item(
            Duration::from_secs(30),
            Duration::ZERO,
            None,
            &TagFilter::default_only(),
        )
        .await
        .expect("second fetch failed");
    assert!(next.is_none());
}

#[tokio::test]
async fn native_orchestrator_enqueue_rejects_worker_item() {
    let (_dir, provider) = native_provider().await;
    let item = WorkItem::ActivityExecute {
        instance: "native-worker".to_string(),
        execution_id: INITIAL_EXECUTION_ID,
        id: 1,
        name: "DoThing".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };

    let err = provider
        .enqueue_for_orchestrator(item, None)
        .await
        .expect_err("worker item should not enqueue to orchestrator queue");
    assert!(!err.is_retryable());
}

#[tokio::test]
async fn native_orchestrator_fetch_renew_abandon_refetch() {
    let (_dir, provider) = native_provider().await;
    let start = WorkItem::StartOrchestration {
        instance: "native-orch".to_string(),
        orchestration: "TestOrch".to_string(),
        input: "{}".to_string(),
        version: Some("1.0.0".to_string()),
        parent_instance: None,
        parent_id: None,
        execution_id: INITIAL_EXECUTION_ID,
    };

    provider
        .enqueue_for_orchestrator(start.clone(), None)
        .await
        .expect("enqueue orchestrator failed");

    let (item, token, attempts) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .expect("fetch orchestration failed")
        .expect("expected orchestration item");
    assert_eq!(item.instance, "native-orch");
    assert_eq!(item.orchestration_name, "TestOrch");
    assert_eq!(item.execution_id, INITIAL_EXECUTION_ID);
    assert_eq!(item.history.len(), 0);
    assert_eq!(item.messages, vec![start]);
    assert_eq!(attempts, 1);

    provider
        .renew_orchestration_item_lock(&token, Duration::from_secs(30))
        .await
        .expect("renew orchestration lock failed");
    provider
        .abandon_orchestration_item(&token, None, false)
        .await
        .expect("abandon orchestration failed");

    let (_item, _token, attempts) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .expect("refetch orchestration failed")
        .expect("expected orchestration item after abandon");
    assert_eq!(attempts, 2);
}

#[tokio::test]
async fn native_custom_status_kv_and_session_helpers() {
    let (_dir, provider) = native_provider().await;
    let conn = provider
        .native()
        .expect("expected native provider")
        .connect()
        .await
        .expect("connect failed");
    conn.execute(
        r#"
        INSERT INTO instances
            (instance_id, orchestration_name, orchestration_version, custom_status, custom_status_version)
        VALUES (?1, ?2, ?3, ?4, ?5)
        "#,
        params!["native-state", "TestOrch", "1.0.0", "{\"step\":1}", 2],
    )
    .await
    .expect("insert instance failed");
    conn.execute(
        "INSERT INTO kv_store (instance_id, key, value, last_updated_at_ms) VALUES (?1, ?2, ?3, ?4)",
        params!["native-state", "keep", "store-value", 1],
    )
    .await
    .expect("insert kv_store failed");
    conn.execute(
        "INSERT INTO kv_delta (instance_id, key, value, last_updated_at_ms) VALUES (?1, ?2, ?3, ?4)",
        params!["native-state", "keep", "delta-value", 2],
    )
    .await
    .expect("insert kv_delta failed");
    conn.execute(
        "INSERT INTO kv_delta (instance_id, key, value, last_updated_at_ms) VALUES (?1, ?2, NULL, ?3)",
        params!["native-state", "deleted", 3],
    )
    .await
    .expect("insert kv tombstone failed");

    assert_eq!(
        provider
            .get_custom_status("native-state", 1)
            .await
            .expect("custom status failed"),
        Some((Some("{\"step\":1}".to_string()), 2))
    );
    assert_eq!(
        provider
            .get_kv_value("native-state", "keep")
            .await
            .expect("kv value failed"),
        Some("delta-value".to_string())
    );
    let all = provider
        .get_kv_all_values("native-state")
        .await
        .expect("kv all failed");
    assert_eq!(all.get("keep"), Some(&"delta-value".to_string()));
    assert!(!all.contains_key("deleted"));

    let now = 9_999_999_999_999_i64;
    conn.execute(
        "INSERT INTO sessions (session_id, worker_id, locked_until, last_activity_at) VALUES (?1, ?2, ?3, ?4)",
        params!["session-live", "worker-a", now, now],
    )
    .await
    .expect("insert session failed");
    let renewed = provider
        .renew_session_lock(
            &["worker-a"],
            Duration::from_secs(30),
            Duration::from_secs(30),
        )
        .await
        .expect("renew session failed");
    assert_eq!(renewed, 1);

    conn.execute(
        "INSERT INTO sessions (session_id, worker_id, locked_until, last_activity_at) VALUES (?1, ?2, ?3, ?4)",
        params!["session-orphan", "worker-b", 1, 1],
    )
    .await
    .expect("insert orphan session failed");
    let cleaned = provider
        .cleanup_orphaned_sessions(Duration::from_secs(30))
        .await
        .expect("cleanup failed");
    assert_eq!(cleaned, 1);
}

#[tokio::test]
async fn native_schema_version_bootstrap_is_idempotent() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let path = dir.path().join("schema-version.db");

    let first = LibsqlProvider::new(LibsqlDatabaseConfig::Local { path: path.clone() })
        .await
        .expect("first provider creation failed");
    drop(first);

    let second = LibsqlProvider::new(LibsqlDatabaseConfig::Local { path })
        .await
        .expect("second provider creation failed");
    let conn = second
        .native()
        .expect("expected native provider")
        .connect()
        .await
        .expect("connect failed");
    let mut rows = conn
        .query(
            "SELECT version, description FROM libsql_durable_schema_versions ORDER BY version",
            (),
        )
        .await
        .expect("schema version query failed");
    let row = rows
        .next()
        .await
        .expect("schema version row read failed")
        .expect("expected schema version row");
    assert_eq!(row.get::<i64>(0).expect("version"), 1);
    assert_eq!(
        row.get::<String>(1).expect("description"),
        "initial libsql-durable native schema"
    );
    assert!(
        rows.next()
            .await
            .expect("second schema version row read failed")
            .is_none()
    );
}

#[tokio::test]
async fn native_remote_validation_subset_when_configured() {
    let Ok(remote_url) = std::env::var("LIBSQL_REMOTE_URL") else {
        eprintln!("skipping remote native validation; LIBSQL_REMOTE_URL is not set");
        return;
    };
    let auth_token = std::env::var("LIBSQL_AUTH_TOKEN").unwrap_or_default();
    let provider = LibsqlProvider::new(LibsqlDatabaseConfig::Remote {
        url: remote_url,
        auth_token,
    })
    .await
    .expect("failed to create remote native provider");
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after UNIX epoch")
        .as_nanos();
    let instance = format!("remote-validation-{suffix}");

    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: instance.clone(),
                orchestration: "RemoteValidation".to_string(),
                input: "{}".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .expect("remote enqueue failed");
    let (_item, lock_token, _attempts) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
        .await
        .expect("remote fetch failed")
        .expect("expected remote orchestration item");
    provider
        .ack_orchestration_item(
            &lock_token,
            INITIAL_EXECUTION_ID,
            vec![started_event(&instance)],
            vec![],
            vec![],
            duroxide::providers::ExecutionMetadata {
                orchestration_name: Some("RemoteValidation".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .expect("remote ack failed");
    assert_eq!(
        provider
            .read(&instance)
            .await
            .expect("remote read failed")
            .len(),
        1
    );

    let worker = WorkItem::ActivityExecute {
        instance,
        execution_id: INITIAL_EXECUTION_ID,
        id: 1,
        name: "RemoteWork".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    provider
        .enqueue_for_worker(worker.clone())
        .await
        .expect("remote worker enqueue failed");
    let (fetched, token, _) = provider
        .fetch_work_item(
            Duration::from_secs(30),
            Duration::ZERO,
            None,
            &TagFilter::default_only(),
        )
        .await
        .expect("remote worker fetch failed")
        .expect("expected remote worker item");
    assert_eq!(fetched, worker);
    provider
        .ack_work_item(&token, None)
        .await
        .expect("remote worker ack failed");
}
