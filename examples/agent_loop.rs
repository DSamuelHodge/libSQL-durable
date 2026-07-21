//! Recipe 10 — Agent = durable orchestration on libsql-durable.
//!
//! One process, one libSQL file, one Duroxide runtime:
//! - Agent plan steps live in orchestration history
//! - Tools are activities (side effects / model-ish work)
//! - Scratch state uses orchestration KV + custom status
//! - Long-term memories live in an app table in the *same* DB
//! - Human-in-the-loop uses schedule_wait + raise_event
//!
//! Run:
//! ```sh
//! cargo run --example agent_loop --no-default-features --features native-libsql
//! ```
//!
//! Optional:
//! ```sh
//! AGENT_DB=./data/agent.db cargo run --example agent_loop --no-default-features --features native-libsql
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use duroxide::providers::Provider;
use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{
    ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus,
};
use libsql_durable::LibsqlProvider;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db_path = std::env::var("AGENT_DB").unwrap_or_else(|_| {
        let dir = std::env::temp_dir().join("libsql-durable-agent-example");
        let _ = std::fs::create_dir_all(&dir);
        dir.join("agent.db").display().to_string()
    });
    println!("agent durable store: {db_path}");

    // ── 1. One file: control plane + app memory ───────────────────────────
    let provider = Arc::new(LibsqlProvider::new_local(&db_path).await?);
    bootstrap_memory_table(&provider).await?;

    let store: Arc<dyn Provider> = provider.clone();

    // ── 2. Tools as activities (non-deterministic / I/O lives here) ───────
    let memory_db = provider.clone();
    let activities = ActivityRegistry::builder()
        .register("Plan", |_ctx: ActivityContext, goal: String| async move {
            // Stand-in for an LLM planner. Recorded as an activity completion event.
            let plan = json!({
                "goal": goal,
                "steps": ["research", "draft", "ask_human_if_needed", "remember"]
            });
            Ok(plan.to_string())
        })
        .register(
            "Research",
            |_ctx: ActivityContext, topic: String| async move {
                // Stand-in for web/search tool.
                Ok(format!(
                    "Research notes on '{topic}': libSQL is a SQLite fork with replicas, \
                 remote sqld, encryption, and optional vectors."
                ))
            },
        )
        .register("Draft", |_ctx: ActivityContext, notes: String| async move {
            Ok(format!(
                "Draft answer based on notes ({} chars): Durable agents should treat \
                 model calls and tools as activities, not orchestrator-side nondeterminism.",
                notes.len()
            ))
        })
        .register("Remember", move |_ctx: ActivityContext, payload: String| {
            let memory_db = memory_db.clone();
            async move {
                // Same libSQL file as orchestration history.
                let text = payload.replace('\'', "''");
                memory_db
                    .execute_sql(&format!(
                        "INSERT INTO agent_memories (text, source) VALUES ('{text}', 'Remember')"
                    ))
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(format!("stored memory ({} bytes)", payload.len()))
            }
        })
        .build();

    // ── 3. AgentLoop orchestration (deterministic control plane) ──────────
    let agent_loop = |ctx: OrchestrationContext, goal: String| async move {
        ctx.trace_info(format!("AgentLoop start: {goal}"));
        ctx.set_custom_status(r#"{"phase":"planning"}"#);
        ctx.set_kv_value("goal", &goal);

        let plan_json = ctx.schedule_activity("Plan", goal.clone()).await?;
        ctx.set_kv_value("plan", &plan_json);
        ctx.set_custom_status(r#"{"phase":"research"}"#);

        let notes = ctx.schedule_activity("Research", goal.clone()).await?;
        ctx.set_kv_value("research", &notes);
        ctx.set_custom_status(r#"{"phase":"drafting"}"#);

        let draft = ctx.schedule_activity("Draft", notes.clone()).await?;
        ctx.set_kv_value("draft", &draft);

        // Human-in-the-loop: wait for approval or time out and continue.
        ctx.set_custom_status(r#"{"phase":"awaiting_human","event":"HumanApproval"}"#);
        let timeout = async {
            ctx.schedule_timer(Duration::from_secs(2)).await;
            "timeout".to_string()
        };
        let human = async {
            let data = ctx.schedule_wait("HumanApproval").await;
            format!("approved:{data}")
        };
        let (_idx, decision) = ctx.select2(timeout, human).await.into_tuple();
        ctx.set_kv_value("human_decision", &decision);
        ctx.set_custom_status(format!(
            r#"{{"phase":"remembering","decision":"{decision}"}}"#
        ));

        let memory_blob = format!("goal={goal}; decision={decision}; draft={draft}");
        let remembered = ctx.schedule_activity("Remember", memory_blob).await?;

        ctx.set_custom_status(r#"{"phase":"done"}"#);
        Ok(format!(
            "agent complete; human={decision}; remember={remembered}; draft_len={}",
            draft.len()
        ))
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("AgentLoop", agent_loop)
        .build();

    // ── 4. Runtime + client on the same provider ──────────────────────────
    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    let instance_id = format!(
        "agent-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );

    client
        .start_orchestration(
            &instance_id,
            "AgentLoop",
            "Explain why durable execution fits AI agents",
        )
        .await?;
    println!("started instance: {instance_id}");

    // Simulate a human approving after the agent reaches waiting state.
    let client_bg = Client::new(store.clone());
    let instance_bg = instance_id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        println!("raising HumanApproval event…");
        let _ = client_bg
            .raise_event(
                &instance_bg,
                "HumanApproval",
                r#"{"ok":true,"note":"looks good"}"#,
            )
            .await;
    });

    match client
        .wait_for_orchestration(&instance_id, Duration::from_secs(30))
        .await
        .map_err(|e| format!("wait error: {e:?}"))?
    {
        OrchestrationStatus::Completed { output, .. } => {
            println!("✅ AgentLoop completed");
            println!("output: {output}");
        }
        OrchestrationStatus::Failed { details, .. } => {
            println!("❌ AgentLoop failed: {}", details.display_message());
        }
        other => {
            println!("⏳ unexpected status: {other:?}");
        }
    }

    // ── 5. Prove same-file collapse: history + memories ───────────────────
    if let Some(admin) = provider.as_management_capability()
        && let Ok(info) = admin.get_instance_info(&instance_id).await
    {
        println!(
            "instance status={} execution={}",
            info.status, info.current_execution_id
        );
    }
    if let Ok(goal) = client.get_kv_value(&instance_id, "goal").await {
        println!("kv.goal = {goal:?}");
    }
    if let Ok(rows) = provider
        .query_sql(
            "SELECT id, substr(text, 1, 80), source FROM agent_memories ORDER BY id DESC LIMIT 3",
        )
        .await
    {
        println!("agent_memories (same DB file):");
        for row in rows {
            println!("  {row:?}");
        }
    }

    rt.shutdown(None).await;
    println!("shutdown complete; db left at {db_path}");
    Ok(())
}

async fn bootstrap_memory_table(
    provider: &LibsqlProvider,
) -> Result<(), Box<dyn std::error::Error>> {
    provider
        .execute_sql(
            r#"
            CREATE TABLE IF NOT EXISTS agent_memories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                source TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
            "#,
        )
        .await?;
    Ok(())
}
