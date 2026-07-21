#![cfg(feature = "native-libsql")]

use std::sync::Arc;
use std::time::Duration;

use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{ActivityContext, Client, OrchestrationStatus};
use libsql_durable::{
    interpreted_orchestrations, validate_definition_body, wrap_interpret_input, LibsqlProvider,
    INTERPRETED_ORCH_NAME, PVM_DEF_V1,
};

fn stock_activities() -> ActivityRegistry {
    ActivityRegistry::builder()
        .register("echo", |_ctx: ActivityContext, input: String| async move {
            Ok(input)
        })
        .register("prefix", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("P:{input}"))
        })
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

    let store: Arc<dyn duroxide::providers::Provider> = provider.clone();
    let rt = runtime::Runtime::start_with_store(
        store.clone(),
        stock_activities(),
        interpreted_orchestrations(),
    )
    .await;
    let client = Client::new(store);

    // v1.0.0 → echo
    let body = provider
        .get_process_definition("Demo", "1.0.0")
        .await
        .unwrap()
        .unwrap()
        .body_json;
    let payload = wrap_interpret_input(&body, "hello").unwrap();
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
    let body = provider
        .get_process_definition("Demo", "1.1.0")
        .await
        .unwrap()
        .unwrap()
        .body_json;
    let payload = wrap_interpret_input(&body, "hello").unwrap();
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

    rt.shutdown(Some(5_000)).await;
}
