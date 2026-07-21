#![cfg(feature = "native-libsql")]

use std::time::Duration;

use duroxide::providers::{Provider, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use libsql_durable::{BlockReason, LibsqlProvider, WorkQueueKind};

fn started(instance: &str) -> Event {
    Event::with_event_id(
        INITIAL_EVENT_ID,
        instance.to_string(),
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: "IntroOrch".to_string(),
            version: "1.0.0".to_string(),
            input: "{}".to_string(),
            parent_instance: None,
            parent_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    )
}

async fn seed_running_instance(provider: &LibsqlProvider, instance: &str) {
    // Create instance via orchestration start + ack with Running metadata.
    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: instance.to_string(),
                orchestration: "IntroOrch".to_string(),
                input: "{}".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                execution_id: INITIAL_EXECUTION_ID,
            },
            None,
        )
        .await
        .unwrap();

    let (_item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("orch item");

    provider
        .ack_orchestration_item(
            &token,
            INITIAL_EXECUTION_ID,
            vec![started(instance)],
            vec![],
            vec![],
            duroxide::providers::ExecutionMetadata {
                status: Some("Running".to_string()),
                orchestration_name: Some("IntroOrch".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn health_and_queues_on_empty_world() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("h.db"))
        .await
        .unwrap();
    let h = p.health(None).await.unwrap();
    assert!(h.fence_ok, "notes={:?}", h.notes);
    assert!(h.world_id.is_some());
    assert_eq!(h.total_instances, 0);
    let q = p.queues().await.unwrap();
    assert_eq!(q.orchestrator_unlocked, 0);
    assert_eq!(q.worker_unlocked, 0);
}

#[tokio::test]
async fn ps_lists_running_and_why_blocked_lock() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("ps.db"))
        .await
        .unwrap();

    seed_running_instance(&p, "run-1").await;

    // Hold a lock by fetching again after enqueueing another wake? After ack, lock released.
    // Re-enqueue and fetch to hold lock.
    p.enqueue_for_orchestrator(
        WorkItem::StartOrchestration {
            instance: "run-1".to_string(),
            orchestration: "IntroOrch".to_string(),
            input: "{}".to_string(),
            version: Some("1.0.0".to_string()),
            parent_instance: None,
            parent_id: None,
            execution_id: INITIAL_EXECUTION_ID,
        },
        None,
    )
    .await
    .unwrap();
    let (_item, _token, _) = p
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("locked item");

    let procs = p.ps().await.unwrap();
    assert!(
        procs
            .iter()
            .any(|r| r.instance_id == "run-1" && r.lock_held),
        "expected run-1 locked in ps: {procs:?}"
    );

    let blocked = p.why_blocked("run-1").await.unwrap();
    assert!(
        matches!(blocked.reason, BlockReason::Locked { expired: false, .. }),
        "reason={:?}",
        blocked.reason
    );
}

#[tokio::test]
async fn next_sees_worker_items_and_trace_history() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("n.db"))
        .await
        .unwrap();

    seed_running_instance(&p, "hist-1").await;

    p.enqueue_for_worker(WorkItem::ActivityExecute {
        instance: "hist-1".to_string(),
        execution_id: INITIAL_EXECUTION_ID,
        id: 1,
        name: "Tool".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    })
    .await
    .unwrap();

    let next = p.next_work(10).await.unwrap();
    assert!(
        next.iter().any(|w| w.queue == WorkQueueKind::Worker),
        "next={next:?}"
    );

    let tr = p.trace("hist-1", None, 50).await.unwrap();
    assert!(
        tr.iter().any(|e| e.event_type == "OrchestrationStarted"),
        "trace={tr:?}"
    );
}

#[tokio::test]
async fn why_blocked_not_found_and_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("t.db"))
        .await
        .unwrap();

    let missing = p.why_blocked("nope").await.unwrap();
    assert!(matches!(missing.reason, BlockReason::NotFound));

    // Terminal path via direct SQL-ish: append + complete via ack
    p.enqueue_for_orchestrator(
        WorkItem::StartOrchestration {
            instance: "done-1".to_string(),
            orchestration: "IntroOrch".to_string(),
            input: "{}".to_string(),
            version: Some("1.0.0".to_string()),
            parent_instance: None,
            parent_id: None,
            execution_id: INITIAL_EXECUTION_ID,
        },
        None,
    )
    .await
    .unwrap();
    let (_item, token, _) = p
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    p.ack_orchestration_item(
        &token,
        INITIAL_EXECUTION_ID,
        vec![started("done-1")],
        vec![],
        vec![],
        duroxide::providers::ExecutionMetadata {
            status: Some("Completed".to_string()),
            output: Some("ok".to_string()),
            orchestration_name: Some("IntroOrch".to_string()),
            orchestration_version: Some("1.0.0".to_string()),
            ..Default::default()
        },
        vec![],
    )
    .await
    .unwrap();

    let blocked = p.why_blocked("done-1").await.unwrap();
    assert!(
        matches!(blocked.reason, BlockReason::Terminal { .. }),
        "{:?}",
        blocked.reason
    );

    // Terminal instances should not appear in ps.
    let procs = p.ps().await.unwrap();
    assert!(!procs.iter().any(|r| r.instance_id == "done-1"));
}

#[tokio::test]
async fn delayed_work_classified() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("d.db"))
        .await
        .unwrap();
    seed_running_instance(&p, "delay-1").await;

    // Timer far in the future.
    p.enqueue_for_orchestrator(
        WorkItem::TimerFired {
            instance: "delay-1".to_string(),
            execution_id: INITIAL_EXECUTION_ID,
            id: 9,
            fire_at_ms: (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64)
                + 3_600_000,
        },
        None,
    )
    .await
    .unwrap();

    let blocked = p.why_blocked("delay-1").await.unwrap();
    assert!(
        matches!(blocked.reason, BlockReason::Delayed { .. }),
        "expected Delayed, got {:?}",
        blocked.reason
    );
}
