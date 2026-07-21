//! Fork-first explore: speculative work runs on a child world, then discard.
//!
//! ```sh
//! cargo run --example explore_fork --no-default-features --features native-libsql
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use duroxide::runtime;
use duroxide::runtime::registry::{ActivityRegistry, OrchestrationRegistry};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationStatus};
use libsql_durable::{ForkOptions, LibsqlProvider, discard_world_package};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let parent_path = dir.path().join("parent.db");
    let child_path = dir.path().join("child.db");

    let parent = Arc::new(LibsqlProvider::new_local(&parent_path).await?);
    let store: Arc<dyn duroxide::providers::Provider> = parent.clone();
    let activities = ActivityRegistry::builder()
        .register("echo", |_ctx: ActivityContext, input: String| async move {
            Ok(input)
        })
        .build();
    let orch = OrchestrationRegistry::builder()
        .register(
            "Echo",
            |ctx: OrchestrationContext, input: String| async move {
                ctx.schedule_activity("echo", input).await
            },
        )
        .build();
    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orch).await;
    let client = Client::new(store);

    client
        .start_orchestration("main", "Echo", "parent-ok")
        .await?;
    match client
        .wait_for_orchestration("main", Duration::from_secs(10))
        .await?
    {
        OrchestrationStatus::Completed { output, .. } => {
            println!("parent completed: {output}");
        }
        other => println!("parent status: {other:?}"),
    }
    rt.shutdown(Some(3_000)).await;

    // World-grain subprocess (not a side chat)
    let (fork, child) = parent
        .fork_and_open(&parent_path, &child_path, ForkOptions::explore())
        .await?;
    println!(
        "forked explore: parent={} child={}",
        fork.parent_world_id, fork.child_world_id
    );

    let child_store: Arc<dyn duroxide::providers::Provider> = Arc::new(child);
    let activities = ActivityRegistry::builder()
        .register("echo", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("explore:{input}"))
        })
        .build();
    let orch = OrchestrationRegistry::builder()
        .register(
            "Echo",
            |ctx: OrchestrationContext, input: String| async move {
                ctx.schedule_activity("echo", input).await
            },
        )
        .build();
    let child_rt = runtime::Runtime::start_with_store(child_store.clone(), activities, orch).await;
    let child_client = Client::new(child_store);
    child_client
        .start_orchestration("speculative", "Echo", "try-this")
        .await?;
    match child_client
        .wait_for_orchestration("speculative", Duration::from_secs(10))
        .await?
    {
        OrchestrationStatus::Completed { output, .. } => {
            println!("child explore completed: {output}");
        }
        other => println!("child status: {other:?}"),
    }
    child_rt.shutdown(Some(3_000)).await;

    // Discard speculative world (parent untouched)
    drop(parent);
    discard_world_package(&child_path)?;
    println!(
        "discarded child package; parent still at {}",
        parent_path.display()
    );
    assert!(parent_path.exists());
    assert!(!child_path.exists());
    Ok(())
}
