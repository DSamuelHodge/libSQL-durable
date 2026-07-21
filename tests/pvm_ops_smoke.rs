//! Smoke: ops used by the `pvm` binary work against a temp world.
#![cfg(feature = "native-libsql")]

use libsql_durable::{HealOptions, LibsqlProvider, SCHEMA_VERSION};

#[tokio::test]
async fn status_health_ps_heal_on_empty_world() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("smoke.db");
    let p = LibsqlProvider::new_local(&path).await.unwrap();

    let report = p.world_open_report().await.unwrap();
    assert!(report.is_ok());
    assert_eq!(report.manifest.schema_version, SCHEMA_VERSION);

    let h = p.health(None).await.unwrap();
    assert!(h.fence_ok);
    assert_eq!(h.total_instances, 0);

    let ps = p.ps().await.unwrap();
    assert!(ps.is_empty());

    let next = p.next_work(5).await.unwrap();
    assert!(next.is_empty());

    let q = p.queues().await.unwrap();
    assert_eq!(q.orchestrator_unlocked + q.orchestrator_locked, 0);

    let heal = p.heal(HealOptions::default()).await.unwrap();
    assert!(!heal.actions.is_empty());
}

#[tokio::test]
async fn reopen_preserves_world_for_host_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("restart.db");
    let id = {
        let p = LibsqlProvider::new_local(&path).await.unwrap();
        p.world_manifest().await.unwrap().unwrap().world_id
    };
    let p2 = LibsqlProvider::new_local(&path).await.unwrap();
    assert_eq!(p2.world_manifest().await.unwrap().unwrap().world_id, id);
}
