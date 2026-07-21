#![cfg(feature = "native-libsql")]

use duroxide::providers::Provider;
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use libsql_durable::{
    LibsqlProvider, SCHEMA_VERSION, WORLD_FORMAT_VERSION, copy_world_package, open_world_checklist,
};

fn started(instance: &str) -> Event {
    Event::with_event_id(
        INITIAL_EVENT_ID,
        instance.to_string(),
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: "WorldTest".to_string(),
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
async fn new_world_gets_manifest_and_fence_ok() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("world.db");
    let provider = LibsqlProvider::new_local(&path).await.expect("open");

    let manifest = provider
        .world_manifest()
        .await
        .expect("manifest query")
        .expect("manifest row");
    assert!(manifest.world_id.starts_with("world-"));
    assert_eq!(manifest.schema_version, SCHEMA_VERSION);
    assert_eq!(manifest.world_format_version, WORLD_FORMAT_VERSION);
    assert_eq!(manifest.provider_name, "libsql-native");

    let report = provider.world_open_report().await.expect("report");
    assert!(report.is_ok(), "notes={:?}", report.notes);
    assert!(!open_world_checklist().is_empty());
}

#[tokio::test]
async fn reopen_preserves_world_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("world.db");

    let id1 = {
        let p = LibsqlProvider::new_local(&path).await.unwrap();
        p.world_manifest().await.unwrap().unwrap().world_id
    };
    let id2 = {
        let p = LibsqlProvider::new_local(&path).await.unwrap();
        p.world_manifest().await.unwrap().unwrap().world_id
    };
    assert_eq!(id1, id2);
}

#[tokio::test]
async fn copy_package_and_resume_preserves_world_and_history() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db");
    let dst = dir.path().join("dst.db");

    let provider = LibsqlProvider::new_local(&src).await.unwrap();
    provider
        .append_with_execution("p1", INITIAL_EXECUTION_ID, vec![started("p1")])
        .await
        .unwrap();
    let world_id = provider.world_manifest().await.unwrap().unwrap().world_id;

    provider.checkpoint_wal().await.unwrap();
    let paths = provider.package_copy_to(&src, &dst).await.unwrap();
    assert_eq!(paths.db, dst);
    assert!(dst.exists());

    // Source host dropped; open the copy as a new host.
    drop(provider);
    let resumed = LibsqlProvider::new_local(&dst).await.unwrap();
    let m = resumed.world_manifest().await.unwrap().unwrap();
    assert_eq!(m.world_id, world_id);
    assert_eq!(resumed.read("p1").await.unwrap().len(), 1);

    let report = resumed.world_open_report().await.unwrap();
    assert!(report.is_ok());
}

#[tokio::test]
async fn filesystem_copy_world_package_works_when_quiesced() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("a.db");
    let dst = dir.path().join("b.db");

    {
        let p = LibsqlProvider::new_local(&src).await.unwrap();
        p.append_with_execution("x", INITIAL_EXECUTION_ID, vec![started("x")])
            .await
            .unwrap();
        p.checkpoint_wal().await.unwrap();
    }

    copy_world_package(&src, &dst).unwrap();
    let p = LibsqlProvider::new_local(&dst).await.unwrap();
    assert_eq!(p.read("x").await.unwrap().len(), 1);
    assert!(p.world_manifest().await.unwrap().is_some());
}

#[tokio::test]
async fn clear_runtime_data_keeps_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.db");
    let p = LibsqlProvider::new_local(&path).await.unwrap();
    let id = p.world_manifest().await.unwrap().unwrap().world_id;
    p.append_with_execution("z", INITIAL_EXECUTION_ID, vec![started("z")])
        .await
        .unwrap();
    p.clear_runtime_data().await.unwrap();
    assert!(p.read("z").await.unwrap().is_empty());
    assert_eq!(p.world_manifest().await.unwrap().unwrap().world_id, id);
}
