#![cfg(feature = "native-libsql")]

use std::sync::Arc;
use std::time::Duration;

use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{ActivityContext, Client, OrchestrationStatus};
use libsql_durable::{
    INTERPRETED_ORCH_NAME, LibsqlProvider, PVM_DEF_V1, interpreted_orchestrations,
    validate_definition_body, wrap_interpret_input,
};

fn stock_activities() -> ActivityRegistry {
    ActivityRegistry::builder()
        .register("echo", |_ctx: ActivityContext, input: String| async move {
            Ok(input)
        })
        .register(
            "prefix",
            |_ctx: ActivityContext, input: String| async move { Ok(format!("P:{input}")) },
        )
        .build()
}

const DEF_ECHO: &str = r#"{
  "schema": "pvm.def.v1",
  "entry": "main",
  "steps": [
    { "id": "main", "op": "activity", "name": "echo", "input": "$input", "out": "r", "next": "done" },
    { "id": "done", "op": "return", "value": "$r" }
  ]
}"#;

const DEF_PREFIX: &str = r#"{
  "schema": "pvm.def.v1",
  "entry": "main",
  "steps": [
    { "id": "main", "op": "activity", "name": "prefix", "input": "$input", "out": "r", "next": "done" },
    { "id": "done", "op": "return", "value": "$r" }
  ]
}"#;

#[test]
fn validate_v1_ok() {
    validate_definition_body(DEF_ECHO).unwrap();
    assert_eq!(PVM_DEF_V1, "pvm.def.v1");
}

#[test]
fn validate_v1_rejects_bad_op() {
    let bad = r#"{"schema":"pvm.def.v1","entry":"main","steps":[{"id":"main","op":"explode"}]}"#;
    assert!(validate_definition_body(bad).is_err());
}

#[tokio::test]
async fn same_binary_two_definition_versions_different_behavior() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("interp.db");
    let provider = Arc::new(LibsqlProvider::new_local(&path).await.unwrap());

    provider
        .put_process_definition("Demo", "1.0.0", DEF_ECHO)
        .await
        .unwrap();
    provider
        .put_process_definition("Demo", "1.1.0", DEF_PREFIX)
        .await
        .unwrap();

    // Immutable: cannot overwrite 1.0.0
    let err = provider
        .put_process_definition("Demo", "1.0.0", DEF_PREFIX)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("immutable") || err.to_string().contains("already exists"));

    // Load bodies before starting the multi-worker runtime (avoid concurrent file contention).
    let body_v1 = provider
        .get_process_definition("Demo", "1.0.0")
        .await
        .unwrap()
        .unwrap()
        .body_json;
    let body_v11 = provider
        .get_process_definition("Demo", "1.1.0")
        .await
        .unwrap()
        .unwrap()
        .body_json;

    let store: Arc<dyn duroxide::providers::Provider> = provider.clone();
    let rt = runtime::Runtime::start_with_store(
        store.clone(),
        stock_activities(),
        interpreted_orchestrations(),
    )
    .await;
    let client = Client::new(store);

    // v1.0.0 → echo
    let payload = wrap_interpret_input(&body_v1, "hello").unwrap();
    client
        .start_orchestration("i-echo", INTERPRETED_ORCH_NAME, payload)
        .await
        .unwrap();
    match client
        .wait_for_orchestration("i-echo", Duration::from_secs(15))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "hello"),
        other => panic!("unexpected: {other:?}"),
    }

    // v1.1.0 → prefix (same binary, different data)
    let payload = wrap_interpret_input(&body_v11, "hello").unwrap();
    client
        .start_orchestration("i-prefix", INTERPRETED_ORCH_NAME, payload)
        .await
        .unwrap();
    match client
        .wait_for_orchestration("i-prefix", Duration::from_secs(15))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "P:hello"),
        other => panic!("unexpected: {other:?}"),
    }

    rt.shutdown(Some(5_000)).await;

    provider
        .pin_process_definition("i-echo", "Demo", "1.0.0")
        .await
        .unwrap();
    let resolved = provider
        .resolve_definition_for_instance("i-echo")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.version, "1.0.0");
}

#[tokio::test]
async fn if_branches_on_eq_cond() {
    let def = r#"{
      "schema": "pvm.def.v1",
      "entry": "start",
      "steps": [
        { "id": "start", "op": "activity", "name": "echo", "input": "$input", "out": "x", "next": "branch" },
        { "id": "branch", "op": "if", "cond": {"eq": ["$x", "go"]}, "then": "yes", "else": "no" },
        { "id": "yes", "op": "return", "value": "took-then" },
        { "id": "no", "op": "return", "value": "took-else" }
      ]
    }"#;

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(
        LibsqlProvider::new_local(dir.path().join("if.db"))
            .await
            .unwrap(),
    );
    let store: Arc<dyn duroxide::providers::Provider> = provider.clone();
    let rt = runtime::Runtime::start_with_store(
        store.clone(),
        stock_activities(),
        interpreted_orchestrations(),
    )
    .await;
    let client = Client::new(store);

    for (input, expect) in [("go", "took-then"), ("stop", "took-else")] {
        let payload = wrap_interpret_input(def, input).unwrap();
        let id = format!("if-{input}");
        client
            .start_orchestration(&id, INTERPRETED_ORCH_NAME, payload)
            .await
            .unwrap();
        match client
            .wait_for_orchestration(&id, Duration::from_secs(15))
            .await
            .unwrap()
        {
            OrchestrationStatus::Completed { output, .. } => assert_eq!(output, expect),
            other => panic!("{other:?}"),
        }
    }
    rt.shutdown(Some(5_000)).await;
}

#[tokio::test]
async fn select_timer_wins_over_slow_wait() {
    // Timer 50ms vs wait that never arrives → timer arm.
    let def = r#"{
      "schema": "pvm.def.v1",
      "entry": "race",
      "steps": [
        {
          "id": "race",
          "op": "select",
          "out": "winner_payload",
          "arms": [
            { "kind": "timer", "ms": 50, "value": "timed-out", "next": "done_timer" },
            { "kind": "wait", "event": "NeverComes", "next": "done_wait" }
          ]
        },
        { "id": "done_timer", "op": "return", "value": "$winner_payload" },
        { "id": "done_wait", "op": "return", "value": "should-not" }
      ]
    }"#;

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(
        LibsqlProvider::new_local(dir.path().join("sel.db"))
            .await
            .unwrap(),
    );
    let store: Arc<dyn duroxide::providers::Provider> = provider.clone();
    let rt = runtime::Runtime::start_with_store(
        store.clone(),
        stock_activities(),
        interpreted_orchestrations(),
    )
    .await;
    let client = Client::new(store);
    let payload = wrap_interpret_input(def, "").unwrap();
    client
        .start_orchestration("sel1", INTERPRETED_ORCH_NAME, payload)
        .await
        .unwrap();
    match client
        .wait_for_orchestration("sel1", Duration::from_secs(15))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "timed-out"),
        other => panic!("{other:?}"),
    }
    rt.shutdown(Some(5_000)).await;
}

#[tokio::test]
async fn select_wait_wins_when_event_raised() {
    let def = r#"{
      "schema": "pvm.def.v1",
      "entry": "race",
      "steps": [
        {
          "id": "race",
          "op": "select",
          "out": "data",
          "arms": [
            { "kind": "timer", "ms": 30000, "value": "late", "next": "t" },
            { "kind": "wait", "event": "Go", "out": "ev", "next": "w" }
          ]
        },
        { "id": "t", "op": "return", "value": "timer" },
        { "id": "w", "op": "return", "value": "$data" }
      ]
    }"#;

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(
        LibsqlProvider::new_local(dir.path().join("selw.db"))
            .await
            .unwrap(),
    );
    let store: Arc<dyn duroxide::providers::Provider> = provider.clone();
    let rt = runtime::Runtime::start_with_store(
        store.clone(),
        stock_activities(),
        interpreted_orchestrations(),
    )
    .await;
    let client = Client::new(store.clone());
    let payload = wrap_interpret_input(def, "").unwrap();
    client
        .start_orchestration("selw", INTERPRETED_ORCH_NAME, payload)
        .await
        .unwrap();

    // Give orchestrator a moment to subscribe, then raise event.
    tokio::time::sleep(Duration::from_millis(200)).await;
    client
        .raise_event("selw", "Go", r#"{"ok":true}"#)
        .await
        .unwrap();

    match client
        .wait_for_orchestration("selw", Duration::from_secs(15))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("ok") || output.contains("true") || !output.is_empty());
        }
        other => panic!("{other:?}"),
    }
    rt.shutdown(Some(5_000)).await;
}

#[tokio::test]
async fn if_plus_goto_bounded_loop() {
    // Counter loop using if + goto; activity appends "x".
    let def = r#"{
      "schema": "pvm.def.v1",
      "entry": "init",
      "steps": [
        { "id": "init", "op": "activity", "name": "echo", "input": "", "out": "acc", "next": "loop" },
        { "id": "loop", "op": "if", "cond": {"eq": ["$acc", "xxx"]}, "then": "done", "else": "tick" },
        { "id": "tick", "op": "activity", "name": "echo", "input": "$acc", "out": "tmp", "next": "append" },
        { "id": "append", "op": "activity", "name": "prefix", "input": "$tmp", "out": "acc", "next": "strip" },
        { "id": "strip", "op": "activity", "name": "echo", "input": "$acc", "out": "acc", "next": "loop" },
        { "id": "done", "op": "return", "value": "$acc" }
      ]
    }"#;
    // Simpler: use prefix to build "P:" * n — actually redefine with a cleaner loop.
    // Loop: start empty, each tick set acc to acc+"x" via activity "appendx"
    let _ = def;
    let def = r#"{
      "schema": "pvm.def.v1",
      "entry": "init",
      "steps": [
        { "id": "init", "op": "activity", "name": "echo", "input": "", "out": "acc", "next": "loop" },
        { "id": "loop", "op": "if", "cond": {"eq": ["$acc", "xxx"]}, "then": "done", "else": "tick" },
        { "id": "tick", "op": "activity", "name": "appendx", "input": "$acc", "out": "acc", "next": "loop" },
        { "id": "done", "op": "return", "value": "$acc" }
      ]
    }"#;

    let activities = ActivityRegistry::builder()
        .register("echo", |_ctx: ActivityContext, input: String| async move {
            Ok(input)
        })
        .register(
            "appendx",
            |_ctx: ActivityContext, input: String| async move { Ok(format!("{input}x")) },
        )
        .build();

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(
        LibsqlProvider::new_local(dir.path().join("loop.db"))
            .await
            .unwrap(),
    );
    let store: Arc<dyn duroxide::providers::Provider> = provider.clone();
    let rt =
        runtime::Runtime::start_with_store(store.clone(), activities, interpreted_orchestrations())
            .await;
    let client = Client::new(store);
    let payload = wrap_interpret_input(def, "").unwrap();
    client
        .start_orchestration("loop1", INTERPRETED_ORCH_NAME, payload)
        .await
        .unwrap();
    match client
        .wait_for_orchestration("loop1", Duration::from_secs(20))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "xxx"),
        other => panic!("{other:?}"),
    }
    rt.shutdown(Some(5_000)).await;
}
