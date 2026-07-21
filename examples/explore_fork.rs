//! Fork-first explore: speculative work on a **child world**, then discard **or** promote.
//!
//! ```sh
//! # Default: discard child (parent untouched)
//! cargo run --example explore_fork --no-default-features --features native-libsql
//!
//! # Promote child over parent (parent backed up, then replaced)
//! PVM_EXPLORE_MODE=promote cargo run --example explore_fork --no-default-features --features native-libsql
//! ```
//!
//! This is world-grain subprocess explore — not a side chat.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use duroxide::runtime;
use duroxide::runtime::registry::{ActivityRegistry, OrchestrationRegistry};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationStatus};
use libsql_durable::{
    ForkOptions, LibsqlProvider, PromoteOptions, discard_world_package, promote_world_package,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::var("PVM_EXPLORE_MODE").unwrap_or_else(|_| "discard".into());
    let mode = mode.to_ascii_lowercase();
    if mode != "discard" && mode != "promote" {
        return Err(format!("PVM_EXPLORE_MODE must be discard|promote, got {mode}").into());
    }

    let dir = tempfile::tempdir()?;
    let parent_path = dir.path().join("parent.db");
    let child_path = dir.path().join("child.db");

    // ── Parent: baseline work ─────────────────────────────────────────────
    let parent = Arc::new(LibsqlProvider::new_local(&parent_path).await?);
    let parent_id = parent
        .world_manifest()
        .await?
        .expect("manifest")
        .world_id
        .clone();
    run_echo(parent.clone(), "main", "parent-ok", |s| s).await?;
    println!("parent world_id={parent_id}");

    // ── World-grain subprocess (not a side chat) ──────────────────────────
    let (fork, child) = parent
        .fork_and_open(&parent_path, &child_path, ForkOptions::explore())
        .await?;
    println!(
        "forked explore: parent={} child={}",
        fork.parent_world_id, fork.child_world_id
    );
    let child_id = fork.child_world_id.clone();
    let child = Arc::new(child);
    run_echo(child.clone(), "speculative", "try-this", |s| {
        format!("explore:{s}")
    })
    .await?;
    drop(child);
    drop(parent);

    // ── Resolve: discard or promote ───────────────────────────────────────
    match mode.as_str() {
        "discard" => {
            discard_world_package(&child_path)?;
            println!(
                "discarded child; parent still at {} (id={parent_id})",
                parent_path.display()
            );
            assert!(parent_path.exists());
            assert!(!child_path.exists());
            let reopened = LibsqlProvider::new_local(&parent_path).await?;
            assert_eq!(
                reopened.world_manifest().await?.unwrap().world_id,
                parent_id
            );
        }
        "promote" => {
            let result = promote_world_package(
                &parent_path,
                &child_path,
                PromoteOptions::confirmed()
                    .with_discard_child(true)
                    .with_note("explore_fork promote demo"),
            )
            .await?;
            println!(
                "promoted child → parent path\n  backup={}\n  previous_parent={}\n  now_world_id={}",
                result.backup_path.display(),
                result.previous_parent_world_id,
                result.promoted_world_id
            );
            assert_eq!(result.previous_parent_world_id, parent_id);
            assert_eq!(result.promoted_world_id, child_id);
            assert!(result.backup_path.exists());
            assert!(!child_path.exists());
            let promoted = LibsqlProvider::new_local(&parent_path).await?;
            assert_eq!(promoted.world_manifest().await?.unwrap().world_id, child_id);
            let bak = LibsqlProvider::new_local(&result.backup_path).await?;
            assert_eq!(bak.world_manifest().await?.unwrap().world_id, parent_id);
        }
        _ => unreachable!(),
    }

    Ok(())
}

async fn run_echo<F>(
    provider: Arc<LibsqlProvider>,
    instance: &str,
    input: &str,
    map: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn(String) -> String + Send + Sync + 'static + Clone,
{
    let store: Arc<dyn duroxide::providers::Provider> = provider;
    let map2 = map.clone();
    let activities = ActivityRegistry::builder()
        .register("echo", move |_ctx: ActivityContext, input: String| {
            let map2 = map2.clone();
            async move { Ok(map2(input)) }
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
        .start_orchestration(instance, "Echo", input.to_string())
        .await?;
    match client
        .wait_for_orchestration(instance, Duration::from_secs(15))
        .await?
    {
        OrchestrationStatus::Completed { output, .. } => {
            println!("instance {instance} completed: {output}");
        }
        other => println!("instance {instance} status: {other:?}"),
    }
    rt.shutdown(Some(3_000)).await;
    Ok(())
}
