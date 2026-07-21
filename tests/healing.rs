#![cfg(feature = "native-libsql")]

use std::time::Duration;

use duroxide::providers::{Provider, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use libsql::params;
use libsql_durable::{HealOptions, LibsqlProvider};

fn started(instance: &str) -> Event {
    Event::with_event_id(
        INITIAL_EVENT_ID,
        instance.to_string(),
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: "HealOrch".to_string(),
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
async fn reclaim_expired_instance_locks() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("r.db"))
        .await
        .unwrap();

    // Seed a running instance then inject an expired lock via SQL.
    p.enqueue_for_orchestrator(
        WorkItem::StartOrchestration {
            instance: "h1".into(),
            orchestration: "HealOrch".into(),
            input: "{}".into(),
            version: Some("1.0.0".into()),
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
        vec![started("h1")],
        vec![],
        vec![],
        duroxide::providers::ExecutionMetadata {
            status: Some("Running".into()),
            orchestration_name: Some("HealOrch".into()),
            orchestration_version: Some("1.0.0".into()),
            ..Default::default()
        },
        vec![],
    )
    .await
    .unwrap();

    let native = p.native().unwrap();
    let conn = native.connect().await.unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO instance_locks (instance_id, lock_token, locked_until, locked_at) VALUES (?1, ?2, ?3, ?4)",
        params!["h1", "expired_tok", 1_i64, 1_i64],
    )
    .await
    .unwrap();

    let result = p.heal_reclaim_expired_locks().await.unwrap();
    assert!(
        result.rows_affected >= 1,
        "detail={}",
        result.detail
    );

    let health = p.health(None).await.unwrap();
    assert_eq!(health.expired_locks, 0);

    let audit = p.healing_audit_log(10).await.unwrap();
    assert!(audit.iter().any(|a| a.action == "reclaim_expired_locks"));
}

#[tokio::test]
async fn quarantine_poison_worker_items() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("q.db"))
        .await
        .unwrap();

    p.enqueue_for_worker(WorkItem::ActivityExecute {
        instance: "w1".into(),
        execution_id: INITIAL_EXECUTION_ID,
        id: 1,
        name: "BadTool".into(),
        input: "{}".into(),
        session_id: None,
        tag: None,
    })
    .await
    .unwrap();

    let native = p.native().unwrap();
    let conn = native.connect().await.unwrap();
    conn.execute(
        "UPDATE worker_queue SET attempt_count = 9 WHERE instance_id = ?1",
        params!["w1"],
    )
    .await
    .unwrap();

    let result = p.heal_quarantine_poison(Some(5)).await.unwrap();
    assert_eq!(result.rows_affected, 1);
    assert_eq!(p.healing_quarantine_count().await.unwrap(), 1);

    let next = p.next_work(10).await.unwrap();
    assert!(
        !next.iter().any(|w| w.instance_id.as_deref() == Some("w1")),
        "poison item should not remain in worker queue"
    );
}

#[tokio::test]
async fn fence_orphan_queue_after_instance_gone() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("o.db"))
        .await
        .unwrap();

    p.enqueue_for_orchestrator(
        WorkItem::StartOrchestration {
            instance: "ghost".into(),
            orchestration: "HealOrch".into(),
            input: "{}".into(),
            version: Some("1.0.0".into()),
            parent_instance: None,
            parent_id: None,
            execution_id: INITIAL_EXECUTION_ID,
        },
        None,
    )
    .await
    .unwrap();

    // Remove instance row without cleaning queues (simulate force-delete race).
    let native = p.native().unwrap();
    let conn = native.connect().await.unwrap();
    // No instance row yet until ack — insert then delete to create orphan message.
    conn.execute(
        "INSERT INTO instances (instance_id, orchestration_name, orchestration_version, current_execution_id) VALUES ('ghost', 'HealOrch', '1.0.0', 1)",
        (),
    )
    .await
    .unwrap();
    conn.execute("DELETE FROM instances WHERE instance_id = 'ghost'", ())
        .await
        .unwrap();

    let result = p.heal_fence_orphan_queue_items().await.unwrap();
    assert!(result.rows_affected >= 1, "detail={}", result.detail);

    let q = p.queues().await.unwrap();
    assert_eq!(q.orchestrator_unlocked + q.orchestrator_locked, 0);
}

#[tokio::test]
async fn heal_suite_runs_and_audits() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("all.db"))
        .await
        .unwrap();

    let report = p
        .heal(HealOptions {
            poison_attempt_threshold: Some(5),
            compact_keep_last: Some(1),
            runaway_history_events: Some(1000),
            compact_instance_limit: Some(10),
        })
        .await
        .unwrap();

    assert!(
        report.actions.len() >= 3,
        "expected reclaim/quarantine/fence at minimum: {:?}",
        report.actions
    );
    let audit = p.healing_audit_log(20).await.unwrap();
    assert!(!audit.is_empty());
}
