#![cfg(feature = "native-libsql")]

use duroxide::providers::Provider;
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use libsql_durable::{ForkOptions, LibsqlProvider, RuntimePolicy};

fn started(instance: &str, name: &str) -> Event {
    Event::with_event_id(
        INITIAL_EVENT_ID,
        instance.to_string(),
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            input: "{}".to_string(),
            parent_instance: None,
            parent_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    )
}

// --- Phase 4: definitions ---

#[tokio::test]
async fn put_get_list_process_definitions() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("def.db"))
        .await
        .unwrap();

    p.put_process_definition(
        "AgentLoop",
        "1.0.0",
        r#"{"steps":["think","act"],"meta":{"kind":"agent"}}"#,
    )
    .await
    .unwrap();
    p.put_process_definition("AgentLoop", "1.1.0", r#"{"steps":["think","act","reflect"]}"#)
        .await
        .unwrap();

    let d = p
        .get_process_definition("AgentLoop", "1.0.0")
        .await
        .unwrap()
        .expect("def");
    assert_eq!(d.name, "AgentLoop");
    assert_eq!(d.version, "1.0.0");
    assert!(d.body_json.contains("think"));

    let all = p.list_process_definitions().await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn pin_requires_existing_definition() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("pin.db"))
        .await
        .unwrap();

    let err = p
        .pin_process_definition("i1", "Missing", "1.0.0")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found"));

    p.put_process_definition("Ok", "1.0.0", r#"{"n":1}"#)
        .await
        .unwrap();
    p.pin_process_definition("i1", "Ok", "1.0.0")
        .await
        .unwrap();
    let pin = p
        .get_process_definition_pin("i1")
        .await
        .unwrap()
        .expect("pin");
    assert_eq!(pin.definition_name, "Ok");
    assert_eq!(pin.definition_version, "1.0.0");
}

#[tokio::test]
async fn put_definition_rejects_invalid_json() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("bad.db"))
        .await
        .unwrap();
    let err = p
        .put_process_definition("X", "1", "not-json")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not valid JSON"));
}

// --- Phase 5: fork + time travel ---

#[tokio::test]
async fn fork_stamps_lineage_and_new_world_id() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("parent.db");
    let dst = dir.path().join("child.db");

    let parent = LibsqlProvider::new_local(&src).await.unwrap();
    parent
        .append_with_execution("a", INITIAL_EXECUTION_ID, vec![started("a", "A")])
        .await
        .unwrap();
    let parent_id = parent.world_manifest().await.unwrap().unwrap().world_id;

    let result = parent
        .fork_world_to(
            &src,
            &dst,
            ForkOptions {
                note: Some("explore".into()),
                clear_scheduler_state: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(result.parent_world_id, parent_id);
    assert_ne!(result.child_world_id, parent_id);
    assert_eq!(result.note.as_deref(), Some("explore"));

    drop(parent);
    let child = LibsqlProvider::new_local(&dst).await.unwrap();
    let m = child.world_manifest().await.unwrap().unwrap();
    assert_eq!(m.world_id, result.child_world_id);
    assert_eq!(m.parent_world_id.as_deref(), Some(parent_id.as_str()));
    assert_eq!(m.fork_note.as_deref(), Some("explore"));
    // History copied
    assert_eq!(child.read("a").await.unwrap().len(), 1);
}

#[tokio::test]
async fn time_travel_truncate_and_retain_instance() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("tt.db"))
        .await
        .unwrap();

    p.append_with_execution("keep", INITIAL_EXECUTION_ID, vec![started("keep", "K")])
        .await
        .unwrap();
    p.append_with_execution("drop", INITIAL_EXECUTION_ID, vec![started("drop", "D")])
        .await
        .unwrap();

    // Truncate nothing useful with huge cut; then retain only keep.
    let n = p.time_travel_truncate(999_999, None).await.unwrap();
    assert_eq!(n, 0);

    p.retain_instance_only("keep").await.unwrap();
    assert_eq!(p.read("keep").await.unwrap().len(), 1);
    assert!(p.read("drop").await.unwrap().is_empty());
}

#[tokio::test]
async fn fork_with_truncate_after_event() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db");
    let dst = dir.path().join("fork.db");

    let parent = LibsqlProvider::new_local(&src).await.unwrap();
    // Two events: event_id 1 and 2
    parent
        .append_with_execution(
            "x",
            INITIAL_EXECUTION_ID,
            vec![
                started("x", "X"),
                Event::with_event_id(
                    2,
                    "x".to_string(),
                    INITIAL_EXECUTION_ID,
                    None,
                    EventKind::OrchestrationCompleted {
                        output: "done".to_string(),
                    },
                ),
            ],
        )
        .await
        .unwrap();
    assert_eq!(parent.read("x").await.unwrap().len(), 2);

    parent
        .fork_world_to(
            &src,
            &dst,
            ForkOptions {
                truncate_after_event_id: Some(1),
                clear_scheduler_state: true,
                note: Some("rewind".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    drop(parent);
    let child = LibsqlProvider::new_local(&dst).await.unwrap();
    assert_eq!(child.read("x").await.unwrap().len(), 1);
}

// --- Phase 6: adaptive policy ---

#[tokio::test]
async fn runtime_policy_get_set_audit() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("pol.db"))
        .await
        .unwrap();

    let mut policy = p.get_runtime_policy().await.unwrap();
    assert_eq!(policy.source, "default");
    policy.lock_timeout_ms = 45_000;
    policy.max_transient_retries = 4;
    p.set_runtime_policy(&policy, "operator").await.unwrap();

    let got = p.get_runtime_policy().await.unwrap();
    assert_eq!(got.lock_timeout_ms, 45_000);
    assert_eq!(got.max_transient_retries, 4);
    assert_eq!(got.source, "operator");

    let audit = p.policy_audit_log(10).await.unwrap();
    assert!(!audit.is_empty());
    assert!(audit.iter().any(|r| r.source == "operator"));
}

#[tokio::test]
async fn adapt_policy_from_health_is_idempotent_without_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("adapt.db"))
        .await
        .unwrap();
    let before = p.get_runtime_policy().await.unwrap();
    let after = p.adapt_policy_from_health().await.unwrap();
    // Healthy empty world: no pressure → no adaptive change required.
    assert_eq!(after.lock_timeout_ms, before.lock_timeout_ms);
    assert_eq!(after.poison_threshold, before.poison_threshold);
}

#[tokio::test]
async fn runtime_policy_to_tuning_maps_fields() {
    let policy = RuntimePolicy {
        lock_timeout_ms: 12_000,
        max_transient_retries: 3,
        retry_base_delay_ms: 25,
        poison_threshold: 4,
        compact_keep_last: 1,
        updated_at_ms: 0,
        source: "test".into(),
    };
    let t = policy.to_tuning();
    assert_eq!(t.busy_timeout_ms, 12_000);
    assert_eq!(t.max_transient_retries, 3);
    assert_eq!(t.retry_base_delay_ms, 25);
}

// --- Phase 7: world mesh ---

#[tokio::test]
async fn mesh_peers_and_refs() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("mesh.db"))
        .await
        .unwrap();

    p.register_mesh_peer(
        "world-peer-a",
        "libsql://edge-a.example/db",
        "replica",
        Some(r#"{"region":"us"}"#),
    )
    .await
    .unwrap();
    p.register_mesh_peer("world-peer-b", "file:./b.db", "primary", None)
        .await
        .unwrap();

    let peers = p.list_mesh_peers().await.unwrap();
    assert_eq!(peers.len(), 2);
    assert_eq!(peers[0].peer_world_id, "world-peer-a");

    p.add_world_ref("local-inst", "world-peer-a", "remote-inst", Some("handoff"))
        .await
        .unwrap();
    let refs = p.list_world_refs(Some("local-inst")).await.unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].remote_instance_id, "remote-inst");
    assert_eq!(refs[0].note, "handoff");

    let status = p.mesh_status().await.unwrap();
    assert!(status.local_world_id.is_some());
    assert_eq!(status.peer_count, 2);
    assert_eq!(status.ref_count, 1);
}

#[tokio::test]
async fn schema_version_is_horizon_v2() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("v2.db"))
        .await
        .unwrap();
    assert_eq!(
        p.schema_version().await.unwrap(),
        Some(libsql_durable::SCHEMA_VERSION)
    );
    assert_eq!(libsql_durable::SCHEMA_VERSION, 2);
    assert_eq!(libsql_durable::WORLD_FORMAT_VERSION, 2);

    // Horizon tables exist after migrate.
    let defs = p.list_process_definitions().await.unwrap();
    assert!(defs.is_empty());
    let peers = p.list_mesh_peers().await.unwrap();
    assert!(peers.is_empty());
    let _ = p.get_runtime_policy().await.unwrap();
}
