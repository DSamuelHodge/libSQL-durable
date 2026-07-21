#![cfg(feature = "native-libsql")]

//! Wiring tests for libSQL engine capabilities exposed by the provider:
//! encryption, SQL escape hatch, vector probe, offline/replica options.

use std::time::Duration;

use duroxide::providers::Provider;
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use libsql_durable::{
    LibsqlDatabaseConfig, LibsqlEngineOptions, LibsqlProvider, SCHEMA_VERSION,
};

fn started(instance: &str) -> Event {
    Event::with_event_id(
        INITIAL_EVENT_ID,
        instance.to_string(),
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: "EngineOrch".to_string(),
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
async fn engine_sql_escape_hatch_and_schema_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("engine.db");
    let provider = LibsqlProvider::new(LibsqlDatabaseConfig::local(&path))
        .await
        .expect("local provider");

    assert_eq!(
        provider.schema_version().await.unwrap(),
        Some(SCHEMA_VERSION)
    );

    provider
        .execute_sql("CREATE TABLE IF NOT EXISTS app_notes (id INTEGER PRIMARY KEY, body TEXT)")
        .await
        .expect("create app table");
    provider
        .execute_sql("INSERT INTO app_notes (body) VALUES ('hello-libsql')")
        .await
        .expect("insert");
    let rows = provider
        .query_sql("SELECT body FROM app_notes")
        .await
        .expect("query");
    assert_eq!(rows[0][0].as_deref(), Some("hello-libsql"));
}

#[tokio::test]
async fn local_encryption_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("encrypted.db");
    let key = b"0123456789abcdef0123456789abcdef".to_vec(); // 32 bytes

    let options = LibsqlEngineOptions::default().with_local_encryption_key(key.clone());
    let provider = LibsqlProvider::new(LibsqlDatabaseConfig::local(&path).with_options(options))
        .await
        .expect("encrypted open");

    provider
        .append_with_execution("enc-1", INITIAL_EXECUTION_ID, vec![started("enc-1")])
        .await
        .expect("append on encrypted db");
    assert_eq!(provider.read("enc-1").await.unwrap().len(), 1);
    drop(provider);

    // Reopen with same key succeeds.
    let options = LibsqlEngineOptions::default().with_local_encryption_key(key);
    let provider = LibsqlProvider::new(LibsqlDatabaseConfig::local(&path).with_options(options))
        .await
        .expect("reopen encrypted");
    assert_eq!(provider.read("enc-1").await.unwrap().len(), 1);

    // Wrong key should fail to open or read meaningfully.
    let bad = LibsqlEngineOptions::default()
        .with_local_encryption_key(b"ffffffffffffffffffffffffffffffff".to_vec());
    let bad_open = LibsqlProvider::new(LibsqlDatabaseConfig::local(&path).with_options(bad)).await;
    // Either open fails or subsequent read/migrate fails depending on libsql version.
    if let Ok(bad_provider) = bad_open {
        let read = bad_provider.read("enc-1").await;
        assert!(
            read.is_err() || read.unwrap().is_empty(),
            "wrong encryption key must not yield original history"
        );
    }
}

#[tokio::test]
async fn engine_options_are_retained_on_provider() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("opts.db");
    let options = LibsqlEngineOptions::default()
        .with_namespace("tenant-demo")
        .with_sync_interval(Duration::from_secs(3))
        .with_read_your_writes(true)
        .with_remote_writes(false);

    let provider = LibsqlProvider::new(LibsqlDatabaseConfig::local(&path).with_options(options.clone()))
        .await
        .expect("open");
    let retained = provider.engine_options().expect("native options");
    assert_eq!(retained.namespace.as_deref(), Some("tenant-demo"));
    assert_eq!(retained.sync_interval, Some(Duration::from_secs(3)));
    assert!(retained.read_your_writes);
    assert!(!retained.remote_writes);
}

#[tokio::test]
async fn vector_support_probe_does_not_panic() {
    let provider = LibsqlProvider::new_in_memory().await.expect("memory");
    // Presence depends on libsql build; either answer is fine — API must work.
    let supported = provider
        .engine_supports_vector()
        .await
        .expect("vector probe");
    eprintln!("engine_supports_vector = {supported}");
}

#[tokio::test]
async fn config_helpers_cover_all_modes() {
    let offline = LibsqlDatabaseConfig::offline_synced("./off.db", "http://127.0.0.1:18080", "")
        .with_options(
            LibsqlEngineOptions::default()
                .with_sync_interval(Duration::from_secs(1))
                .with_remote_writes(true),
        );
    assert!(matches!(
        offline.mode,
        libsql_durable::LibsqlDatabaseMode::OfflineSynced { .. }
    ));

    let replica = LibsqlDatabaseConfig::remote_replica("./r.db", "http://127.0.0.1:18080", "")
        .with_options(LibsqlEngineOptions::default().with_namespace("ns-1"));
    assert!(matches!(
        replica.mode,
        libsql_durable::LibsqlDatabaseMode::RemoteReplica { .. }
    ));
}
