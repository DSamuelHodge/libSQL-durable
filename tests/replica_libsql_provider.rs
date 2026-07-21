#![cfg(feature = "native-libsql")]

//! Embedded remote-replica durability checks against a self-hosted primary.
//!
//! Opt-in: requires `LIBSQL_REMOTE_URL` (primary `sqld`). When unset, tests skip.
//!
//! ```sh
//! ./scripts/start-sqld.sh
//! ./scripts/run-replica-tests.sh
//! ```

use std::path::PathBuf;
use std::time::Duration;

use duroxide::providers::{Provider, TagFilter, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use libsql_durable::{LibsqlDatabaseConfig, LibsqlProvider, SCHEMA_VERSION};
use tempfile::TempDir;

fn remote_url() -> Option<String> {
    std::env::var("LIBSQL_REMOTE_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn auth_token() -> String {
    std::env::var("LIBSQL_AUTH_TOKEN").unwrap_or_default()
}

fn unique_suffix() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}

macro_rules! skip_if_no_remote {
    () => {
        if remote_url().is_none() {
            eprintln!("skipping replica test: set LIBSQL_REMOTE_URL to enable");
            return;
        }
    };
}

async fn primary_provider() -> Option<LibsqlProvider> {
    let url = remote_url()?;
    match LibsqlProvider::new_remote(url.clone(), auth_token()).await {
        Ok(provider) => {
            provider
                .clear_runtime_data()
                .await
                .expect("clear primary runtime data");
            Some(provider)
        }
        Err(err) => {
            eprintln!("primary not reachable ({url}): {err}");
            None
        }
    }
}

async fn open_replica(tmp: &TempDir, name: &str) -> LibsqlProvider {
    let url = remote_url().expect("LIBSQL_REMOTE_URL");
    let path = tmp.path().join(name);
    // Embedded replicas need a clean path (or a previously synced DB file).
    if path.exists() {
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
        // libsql also writes meta next to the db
        let _ = std::fs::remove_file(format!("{}-info", path.display()));
    }
    LibsqlProvider::new_remote_replica(path, url, auth_token())
        .await
        .expect("failed to open embedded remote replica")
}

fn started_event(instance: &str) -> Event {
    Event::with_event_id(
        INITIAL_EVENT_ID,
        instance.to_string(),
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: "ReplicaOrch".to_string(),
            version: "1.0.0".to_string(),
            input: "{\"via\":\"primary\"}".to_string(),
            parent_instance: None,
            parent_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    )
}

#[tokio::test]
async fn replica_syncs_history_written_on_primary() {
    skip_if_no_remote!();
    let Some(primary) = primary_provider().await else {
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let suffix = unique_suffix();
    let instance = format!("replica-hist-{suffix}");

    primary
        .append_with_execution(&instance, INITIAL_EXECUTION_ID, vec![started_event(&instance)])
        .await
        .expect("primary append failed");

    let history_primary = primary.read(&instance).await.expect("primary read");
    assert_eq!(history_primary.len(), 1);

    let replica = open_replica(&tmp, "replica-hist.db").await;
    replica.sync().await.expect("replica sync after primary write");

    let history_replica = replica
        .read(&instance)
        .await
        .expect("replica read after sync");
    assert_eq!(
        history_replica.len(),
        1,
        "replica should observe primary history after sync"
    );
    assert!(matches!(
        history_replica[0].kind,
        EventKind::OrchestrationStarted { .. }
    ));

    let version = replica
        .schema_version()
        .await
        .expect("replica schema_version");
    assert_eq!(version, Some(SCHEMA_VERSION));
}

#[tokio::test]
async fn replica_write_delegates_to_primary_and_resyncs() {
    skip_if_no_remote!();
    let Some(primary) = primary_provider().await else {
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let suffix = unique_suffix();
    let instance = format!("replica-write-{suffix}");

    // Ensure primary schema exists before replica handshake.
    primary.migrate().await.expect("primary migrate");

    let replica = open_replica(&tmp, "replica-write.db").await;

    let worker_item = WorkItem::ActivityExecute {
        instance: instance.clone(),
        execution_id: INITIAL_EXECUTION_ID,
        id: 9,
        name: "FromReplica".to_string(),
        input: "{}".to_string(),
        session_id: None,
        tag: None,
    };
    replica
        .enqueue_for_worker(worker_item.clone())
        .await
        .expect("replica enqueue should forward write to primary");

    // Primary should see the worker item without needing a local replica file.
    let (fetched, token, attempts) = primary
        .fetch_work_item(
            Duration::from_secs(30),
            Duration::ZERO,
            None,
            &TagFilter::default_only(),
        )
        .await
        .expect("primary fetch after replica write")
        .expect("expected worker item on primary");
    assert_eq!(fetched, worker_item);
    assert_eq!(attempts, 1);
    primary
        .ack_work_item(&token, None)
        .await
        .expect("primary ack");

    // History written via replica should also land on primary.
    replica
        .append_with_execution(&instance, INITIAL_EXECUTION_ID, vec![started_event(&instance)])
        .await
        .expect("replica append");
    replica.sync().await.expect("replica sync after append");

    let on_primary = primary
        .read(&instance)
        .await
        .expect("primary read after replica append");
    assert_eq!(on_primary.len(), 1);

    // Fresh replica file should catch up from primary.
    let replica2 = open_replica(&tmp, "replica-write-2.db").await;
    replica2.sync().await.expect("second replica sync");
    let on_replica2 = replica2
        .read(&instance)
        .await
        .expect("second replica read");
    assert_eq!(on_replica2.len(), 1);
}

#[tokio::test]
async fn replica_survives_reopen_after_sync() {
    skip_if_no_remote!();
    let Some(primary) = primary_provider().await else {
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let suffix = unique_suffix();
    let instance = format!("replica-reopen-{suffix}");
    let replica_path: PathBuf = tmp.path().join("replica-reopen.db");

    primary
        .append_with_execution(&instance, INITIAL_EXECUTION_ID, vec![started_event(&instance)])
        .await
        .expect("primary append");

    // First open + sync materializes local replica state.
    {
        let replica = LibsqlProvider::new(LibsqlDatabaseConfig::remote_replica(
            replica_path.clone(),
            remote_url().unwrap(),
            auth_token(),
        ))
        .await
        .expect("first replica open");
        replica.sync().await.expect("first sync");
        assert_eq!(replica.read(&instance).await.unwrap().len(), 1);
    }

    // Primary advances further.
    let instance2 = format!("replica-reopen-b-{suffix}");
    primary
        .append_with_execution(
            &instance2,
            INITIAL_EXECUTION_ID,
            vec![started_event(&instance2)],
        )
        .await
        .expect("primary second append");

    // Reopen the same replica path (previously synced) and catch up.
    let replica = LibsqlProvider::new(LibsqlDatabaseConfig::remote_replica(
        replica_path,
        remote_url().unwrap(),
        auth_token(),
    ))
    .await
    .expect("reopen replica");
    replica.sync().await.expect("resync");

    assert_eq!(replica.read(&instance).await.unwrap().len(), 1);
    assert_eq!(
        replica.read(&instance2).await.unwrap().len(),
        1,
        "reopened replica should pull newer primary frames"
    );

    let _ = replica.replication_index().await.expect("replication_index");
}
