//! Extended coverage for collapse finish line (defs, interpreter, fork explore).
#![cfg(feature = "native-libsql")]

use std::sync::Arc;
use std::time::Duration;

use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{ActivityContext, Client, OrchestrationStatus};
use libsql_durable::{
    ForkOptions, INTERPRETED_ORCH_NAME, LibsqlProvider, PromoteOptions, SelectivePromoteOptions,
    discard_world_package, interpreted_orchestrations, promote_world_package,
    selective_promote_instances, validate_definition_body, wrap_interpret_input,
};

fn activities() -> ActivityRegistry {
    ActivityRegistry::builder()
        .register("echo", |_ctx: ActivityContext, input: String| async move {
            Ok(input)
        })
        .register("upper", |_ctx: ActivityContext, input: String| async move {
            Ok(input.to_ascii_uppercase())
        })
        .build()
}

#[test]
fn validate_rejects_missing_entry_step() {
    let body = r#"{
      "schema":"pvm.def.v1",
      "entry":"missing",
      "steps":[{"id":"main","op":"return","value":"x"}]
    }"#;
    assert!(
        validate_definition_body(body)
            .unwrap_err()
            .contains("entry")
    );
}

#[test]
fn validate_rejects_duplicate_step_ids() {
    let body = r#"{
      "schema":"pvm.def.v1",
      "entry":"a",
      "steps":[
        {"id":"a","op":"return","value":"1"},
        {"id":"a","op":"return","value":"2"}
      ]
    }"#;
    assert!(
        validate_definition_body(body)
            .unwrap_err()
            .contains("duplicate")
    );
}

#[test]
fn validate_allows_opaque_json_without_schema() {
    validate_definition_body(r#"{"steps":["host-defined"]}"#).unwrap();
    validate_definition_body(r#"[1,2,3]"#).unwrap();
}

#[tokio::test]
async fn definition_immutable_unless_force() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("d.db"))
        .await
        .unwrap();
    let body = r#"{"n":1}"#;
    p.put_process_definition("X", "1.0.0", body).await.unwrap();
    let err = p
        .put_process_definition("X", "1.0.0", r#"{"n":2}"#)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("immutable") || err.to_string().contains("already exists"),
        "{err}"
    );
    p.put_process_definition_ex("X", "1.0.0", r#"{"n":2}"#, true)
        .await
        .unwrap();
    let got = p
        .get_process_definition("X", "1.0.0")
        .await
        .unwrap()
        .unwrap();
    assert!(got.body_json.contains("2"));

    assert!(p.delete_process_definition("X", "1.0.0").await.unwrap());
    assert!(
        p.get_process_definition("X", "1.0.0")
            .await
            .unwrap()
            .is_none()
    );
    assert!(!p.delete_process_definition("X", "1.0.0").await.unwrap());
}

#[tokio::test]
async fn list_by_name_and_resolve_pin() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("l.db"))
        .await
        .unwrap();
    p.put_process_definition("A", "1.0.0", r#"{}"#)
        .await
        .unwrap();
    p.put_process_definition("A", "2.0.0", r#"{}"#)
        .await
        .unwrap();
    p.put_process_definition("B", "1.0.0", r#"{}"#)
        .await
        .unwrap();
    let a = p.list_process_definitions_by_name("A").await.unwrap();
    assert_eq!(a.len(), 2);
    p.pin_process_definition("i1", "A", "2.0.0").await.unwrap();
    let d = p
        .resolve_definition_for_instance("i1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.version, "2.0.0");
    assert!(
        p.resolve_definition_for_instance("nope")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn multi_step_interpreter_with_kv_and_chain() {
    let def = r#"{
      "schema": "pvm.def.v1",
      "entry": "s1",
      "steps": [
        { "id": "s1", "op": "activity", "name": "echo", "input": "$input", "out": "a", "next": "s2" },
        { "id": "s2", "op": "set_kv", "key": "last", "value": "$a", "next": "s3" },
        { "id": "s3", "op": "activity", "name": "upper", "input": "$a", "out": "b", "next": "done" },
        { "id": "done", "op": "return", "value": "$b" }
      ]
    }"#;
    validate_definition_body(def).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(
        LibsqlProvider::new_local(dir.path().join("m.db"))
            .await
            .unwrap(),
    );
    provider
        .put_process_definition("Multi", "1.0.0", def)
        .await
        .unwrap();

    let store: Arc<dyn duroxide::providers::Provider> = provider.clone();
    let rt = runtime::Runtime::start_with_store(
        store.clone(),
        activities(),
        interpreted_orchestrations(),
    )
    .await;
    let client = Client::new(store);
    let payload = wrap_interpret_input(def, "hi").unwrap();
    client
        .start_orchestration("m1", INTERPRETED_ORCH_NAME, payload)
        .await
        .unwrap();
    match client
        .wait_for_orchestration("m1", Duration::from_secs(20))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "HI"),
        other => panic!("{other:?}"),
    }
    let kv = client.get_kv_value("m1", "last").await.unwrap();
    assert_eq!(kv.as_deref(), Some("hi"));
    rt.shutdown(Some(5_000)).await;
}

#[tokio::test]
async fn fork_explore_and_discard_preserves_parent() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("parent.db");
    let dst = dir.path().join("child.db");

    let parent = LibsqlProvider::new_local(&src).await.unwrap();
    let parent_id = parent.world_manifest().await.unwrap().unwrap().world_id;
    parent
        .put_process_definition("Keep", "1.0.0", r#"{"ok":true}"#)
        .await
        .unwrap();

    let opts = ForkOptions::explore().with_note("quality-test");
    let (result, child) = parent.fork_and_open(&src, &dst, opts).await.unwrap();
    assert_eq!(result.parent_world_id, parent_id);
    assert_ne!(result.child_world_id, parent_id);
    let cm = child.world_manifest().await.unwrap().unwrap();
    assert_eq!(cm.parent_world_id.as_deref(), Some(parent_id.as_str()));
    assert_eq!(cm.fork_note.as_deref(), Some("quality-test"));
    // Definitions copied with package
    assert!(
        child
            .get_process_definition("Keep", "1.0.0")
            .await
            .unwrap()
            .is_some()
    );

    drop(child);
    discard_world_package(&dst).unwrap();
    assert!(!dst.exists());
    // Parent still healthy
    let p2 = LibsqlProvider::new_local(&src).await.unwrap();
    assert_eq!(
        p2.world_manifest().await.unwrap().unwrap().world_id,
        parent_id
    );
    assert!(
        p2.get_process_definition("Keep", "1.0.0")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn discard_missing_world_errors() {
    let dir = tempfile::tempdir().unwrap();
    let err = discard_world_package(dir.path().join("nope.db")).unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("discard"));
}

#[tokio::test]
async fn explore_instance_preset_sets_keep() {
    let o = ForkOptions::explore_instance("only-me");
    assert_eq!(o.keep_instance.as_deref(), Some("only-me"));
    assert!(o.clear_scheduler_state);
    let t = ForkOptions::time_travel(7);
    assert_eq!(t.truncate_after_event_id, Some(7));
    assert!(t.clear_scheduler_state);
}

#[tokio::test]
async fn invalid_pvm_def_rejected_on_put() {
    let dir = tempfile::tempdir().unwrap();
    let p = LibsqlProvider::new_local(dir.path().join("bad.db"))
        .await
        .unwrap();
    let bad = r#"{"schema":"pvm.def.v1","entry":"main","steps":[{"id":"main","op":"nope"}]}"#;
    let err = p.put_process_definition("Bad", "1", bad).await.unwrap_err();
    assert!(err.to_string().contains("unknown op") || err.to_string().contains("nope"));
}

#[tokio::test]
async fn promote_requires_confirm() {
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().join("p.db");
    let child = dir.path().join("c.db");
    let p = LibsqlProvider::new_local(&parent).await.unwrap();
    p.fork_world_to(&parent, &child, ForkOptions::explore())
        .await
        .unwrap();
    let err = promote_world_package(&parent, &child, PromoteOptions::default())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("confirm"),
        "expected confirm refusal, got: {err}"
    );
}

#[tokio::test]
async fn promote_refuses_same_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("one.db");
    let _ = LibsqlProvider::new_local(&path).await.unwrap();
    let err = promote_world_package(&path, &path, PromoteOptions::confirmed())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("must differ") || err.to_string().contains("differ"));
}

#[tokio::test]
async fn promote_with_lineage_backs_up_and_replaces() {
    let dir = tempfile::tempdir().unwrap();
    let parent_path = dir.path().join("parent.db");
    let child_path = dir.path().join("child.db");

    let parent = LibsqlProvider::new_local(&parent_path).await.unwrap();
    let parent_id = parent.world_manifest().await.unwrap().unwrap().world_id;
    parent
        .put_process_definition("OnlyParent", "1.0.0", r#"{"side":"parent"}"#)
        .await
        .unwrap();

    let (fork, child) = parent
        .fork_and_open(&parent_path, &child_path, ForkOptions::explore())
        .await
        .unwrap();
    assert_eq!(fork.parent_world_id, parent_id);
    let child_id = fork.child_world_id.clone();
    child
        .put_process_definition("OnlyChild", "1.0.0", r#"{"side":"child"}"#)
        .await
        .unwrap();
    drop(child);
    drop(parent);

    let result = promote_world_package(
        &parent_path,
        &child_path,
        PromoteOptions::confirmed()
            .with_discard_child(true)
            .with_note("quality promote"),
    )
    .await
    .unwrap();

    assert!(result.backup_path.exists());
    assert!(result.discarded_child);
    assert!(!child_path.exists());
    assert_eq!(result.previous_parent_world_id, parent_id);
    assert_eq!(result.promoted_world_id, child_id);

    let promoted = LibsqlProvider::new_local(&parent_path).await.unwrap();
    assert!(
        promoted
            .get_process_definition("OnlyChild", "1.0.0")
            .await
            .unwrap()
            .is_some()
    );
    let m = promoted.world_manifest().await.unwrap().unwrap();
    assert_eq!(m.world_id, child_id);
    assert_eq!(m.parent_world_id.as_deref(), Some(parent_id.as_str()));

    let bak = LibsqlProvider::new_local(&result.backup_path)
        .await
        .unwrap();
    assert_eq!(
        bak.world_manifest().await.unwrap().unwrap().world_id,
        parent_id
    );
    assert!(
        bak.get_process_definition("OnlyParent", "1.0.0")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        bak.get_process_definition("OnlyChild", "1.0.0")
            .await
            .unwrap()
            .is_none()
    );

    let rows = promoted
        .query_sql(
            "SELECT previous_world_id, promoted_from_child_id, note FROM world_promote_audit",
        )
        .await
        .unwrap();
    assert!(!rows.is_empty());
    assert_eq!(rows[0][0].as_deref(), Some(parent_id.as_str()));
    assert_eq!(rows[0][1].as_deref(), Some(child_id.as_str()));
}

#[tokio::test]
async fn promote_lineage_mismatch_refused() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.db");
    let b = dir.path().join("b.db");
    let _ = LibsqlProvider::new_local(&a).await.unwrap();
    let _ = LibsqlProvider::new_local(&b).await.unwrap();
    let err = promote_world_package(&a, &b, PromoteOptions::confirmed())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("lineage") || err.to_string().contains("parent_world_id"),
        "{err}"
    );
}

#[tokio::test]
async fn selective_promote_imports_instance_into_live_parent() {
    use duroxide::providers::Provider;
    use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};

    let dir = tempfile::tempdir().unwrap();
    let parent_path = dir.path().join("parent.db");
    let child_path = dir.path().join("child.db");

    let parent = LibsqlProvider::new_local(&parent_path).await.unwrap();
    let parent_id = parent.world_manifest().await.unwrap().unwrap().world_id;
    // Parent has its own instance
    parent
        .append_with_execution(
            "keep-me",
            INITIAL_EXECUTION_ID,
            vec![Event::with_event_id(
                INITIAL_EVENT_ID,
                "keep-me".to_string(),
                INITIAL_EXECUTION_ID,
                None,
                EventKind::OrchestrationStarted {
                    name: "Keep".to_string(),
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
        .unwrap();

    let (fork, child) = parent
        .fork_and_open(&parent_path, &child_path, ForkOptions::explore())
        .await
        .unwrap();
    assert_eq!(fork.parent_world_id, parent_id);

    // Child explores a new instance
    child
        .append_with_execution(
            "explore-1",
            INITIAL_EXECUTION_ID,
            vec![Event::with_event_id(
                INITIAL_EVENT_ID,
                "explore-1".to_string(),
                INITIAL_EXECUTION_ID,
                None,
                EventKind::OrchestrationStarted {
                    name: "Explore".to_string(),
                    version: "1.0.0".to_string(),
                    input: r#"{"try":1}"#.to_string(),
                    parent_instance: None,
                    parent_id: None,
                    carry_forward_events: None,
                    initial_custom_status: None,
                },
            )],
        )
        .await
        .unwrap();
    // Also mutate a forked copy of keep-me on child
    child
        .append_with_execution(
            "keep-me",
            INITIAL_EXECUTION_ID,
            vec![Event::with_event_id(
                2,
                "keep-me".to_string(),
                INITIAL_EXECUTION_ID,
                None,
                EventKind::OrchestrationCompleted {
                    output: "child-version".to_string(),
                },
            )],
        )
        .await
        .unwrap();

    // Parent still has only 1 event on keep-me before selective promote
    assert_eq!(parent.read("keep-me").await.unwrap().len(), 1);
    assert!(parent.read("explore-1").await.unwrap().is_empty());

    let result = selective_promote_instances(
        &parent,
        &child,
        SelectivePromoteOptions::confirmed(["keep-me", "explore-1"]).with_note("import explore"),
    )
    .await
    .unwrap();
    assert_eq!(result.parent_world_id, parent_id);
    assert_eq!(result.imported_instances.len(), 2);
    assert!(result.history_events_imported >= 2);

    // Parent world_id unchanged; instances imported
    assert_eq!(
        parent.world_manifest().await.unwrap().unwrap().world_id,
        parent_id
    );
    assert_eq!(parent.read("keep-me").await.unwrap().len(), 2);
    assert_eq!(parent.read("explore-1").await.unwrap().len(), 1);
}

#[tokio::test]
async fn cow_copy_produces_readable_world() {
    use libsql_durable::copy_world_package;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("s.db");
    let dst = dir.path().join("d.db");
    let p = LibsqlProvider::new_local(&src).await.unwrap();
    let id = p.world_manifest().await.unwrap().unwrap().world_id;
    p.checkpoint_wal().await.unwrap();
    drop(p);
    copy_world_package(&src, &dst).unwrap();
    let c = LibsqlProvider::new_local(&dst).await.unwrap();
    assert_eq!(c.world_manifest().await.unwrap().unwrap().world_id, id);
}
