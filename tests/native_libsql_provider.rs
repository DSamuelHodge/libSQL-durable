#![cfg(feature = "native-libsql")]

use std::sync::Arc;
use std::time::Duration;

use duroxide::provider_validations::bulk_deletion::test_delete_instance_bulk_safety_and_limits;
use duroxide::provider_validations::prune::test_prune_safety;
use duroxide::provider_validations::{
    ProviderFactory, test_get_execution_info, test_get_instance_info, test_get_queue_depths,
    test_get_system_metrics, test_list_executions, test_list_instances,
    test_list_instances_by_status, test_multi_operation_atomic_ack,
};
use duroxide::providers::{Provider, TagFilter, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use libsql::params;
use libsql_durable::{LibsqlDatabaseConfig, LibsqlProvider};
use tempfile::TempDir;

async fn native_provider() -> (TempDir, LibsqlProvider) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let path = dir.path().join("native.db");
    let provider = LibsqlProvider::new(LibsqlDatabaseConfig::local(path))
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
        let provider = LibsqlProvider::new(LibsqlDatabaseConfig::local(path))
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
async fn native_provider_management_validations() {
    let factory = NativeLibsqlTestFactory;

    test_list_instances(&factory).await;
    test_list_instances_by_status(&factory).await;
    test_list_executions(&factory).await;
    test_get_instance_info(&factory).await;
    test_get_execution_info(&factory).await;
    test_get_system_metrics(&factory).await;
    test_get_queue_depths(&factory).await;
}

#[tokio::test]
async fn native_provider_admin_prune_and_bulk_delete() {
    let factory = NativeLibsqlTestFactory;

    test_prune_safety(&factory).await;
    test_delete_instance_bulk_safety_and_limits(&factory).await;
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
