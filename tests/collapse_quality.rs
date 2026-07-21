//! Extended coverage for collapse finish line (defs, interpreter, fork explore).
#![cfg(feature = "native-libsql")]

use std::sync::Arc;
use std::time::Duration;

use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{ActivityContext, Client, OrchestrationStatus};
use libsql_durable::{
    ForkOptions, INTERPRETED_ORCH_NAME, LibsqlProvider, discard_world_package,
    interpreted_orchestrations, validate_definition_body, wrap_interpret_input,
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
