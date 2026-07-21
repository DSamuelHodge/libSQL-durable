//! Real agent on libsql-durable + Duroxide, calling an OpenAI-compatible NVIDIA endpoint.
//!
//! Model calls are **activities** (side effects). Orchestration stays deterministic.
//! Everything (history, KV, status, memories) lands in one libSQL file.
//!
//! ```sh
//! export NVIDIA_API_KEY=...          # required
//! export NVIDIA_BASE_URL=https://integrate.api.nvidia.com/v1
//! export NVIDIA_MODEL=z-ai/glm-5.2
//! export AGENT_GOAL='Explain durable execution for AI agents in 3 bullets'
//!
//! cargo run --example agent_nvidia --no-default-features --features native-libsql
//! ```
//!
//! Never commit API keys. Prefer env vars / a secret manager.

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
use serde_json::{Value, json};

#[derive(Clone)]
struct NvidiaClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
}

impl NvidiaClient {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let api_key = std::env::var("NVIDIA_API_KEY")
            .map_err(|_| "NVIDIA_API_KEY is required (export it; do not hardcode keys)")?;
        if api_key.trim().is_empty() {
            return Err("NVIDIA_API_KEY is empty".into());
        }
        let base_url = std::env::var("NVIDIA_BASE_URL")
            .unwrap_or_else(|_| "https://integrate.api.nvidia.com/v1".to_string())
            .trim_end_matches('/')
            .to_string();
        let model = std::env::var("NVIDIA_MODEL").unwrap_or_else(|_| "z-ai/glm-5.2".to_string());
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self {
            http,
            base_url,
            api_key,
            model,
        })
    }

    async fn chat(
        &self,
        system: &str,
        user: &str,
        temperature: f32,
        max_tokens: u32,
    ) -> Result<String, String> {
        let url = format!("{}/chat/completions", self.base_url);
        let body = json!({
            "model": self.model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user}
            ]
        });

        let res = self
            .http
            .post(&url)
            .bearer_auth(&self.api_key)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("NVIDIA request failed: {e}"))?;

        let status = res.status();
        let text = res
            .text()
            .await
            .map_err(|e| format!("read body failed: {e}"))?;
        if !status.is_success() {
            return Err(format!("NVIDIA HTTP {status}: {text}"));
        }

        let v: Value =
            serde_json::from_str(&text).map_err(|e| format!("invalid JSON response: {e}"))?;
        let content = v
            .pointer("/choices/0/message/content")
            .and_then(|c| c.as_str())
            .ok_or_else(|| format!("missing choices[0].message.content in: {text}"))?;
        Ok(content.trim().to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let llm = Arc::new(NvidiaClient::from_env()?);
    let goal = std::env::var("AGENT_GOAL").unwrap_or_else(|_| {
        "In 3 short bullets, explain why durable execution (event history + activity queues) is a good foundation for AI agent harnesses. Be concrete.".to_string()
    });

    let db_path = std::env::var("AGENT_DB").unwrap_or_else(|_| {
        let dir = std::env::temp_dir().join("libsql-durable-agent-nvidia");
        let _ = std::fs::create_dir_all(&dir);
        dir.join("agent.db").display().to_string()
    });

    println!("model     = {}", llm.model);
    println!("base_url  = {}", llm.base_url);
    println!("store     = {db_path}");
    println!("goal      = {goal}");

    let provider = Arc::new(LibsqlProvider::new_local(&db_path).await?);
    bootstrap_memory_table(&provider).await?;
    let store: Arc<dyn Provider> = provider.clone();

    // ── Tools / model calls as activities ────────────────────────────────
    let plan_llm = llm.clone();
    let research_llm = llm.clone();
    let draft_llm = llm.clone();
    let memory_db = provider.clone();

    let activities = ActivityRegistry::builder()
        .register("Plan", move |_ctx: ActivityContext, goal: String| {
            let plan_llm = plan_llm.clone();
            async move {
                plan_llm
                    .chat(
                        "You are a planning module for a durable agent. \
                         Return ONLY a JSON object with keys goal (string) and steps (array of short strings). \
                         No markdown fences.",
                        &format!("Create a minimal plan for this goal:\n{goal}"),
                        0.2,
                        512,
                    )
                    .await
            }
        })
        .register(
            "Research",
            move |_ctx: ActivityContext, topic: String| {
                let research_llm = research_llm.clone();
                async move {
                    research_llm
                        .chat(
                            "You are a research assistant. Produce concise, factual notes (not a final essay). \
                             Prefer concrete technical points about durable execution, workflows, queues, and agents.",
                            &format!("Research notes for:\n{topic}"),
                            0.3,
                            800,
                        )
                        .await
                }
            },
        )
        .register("Draft", move |_ctx: ActivityContext, packet: String| {
            let draft_llm = draft_llm.clone();
            async move {
                // packet is JSON: {goal, plan, research}
                draft_llm
                    .chat(
                        "You are the final writer for a durable AI agent. \
                         Write a clear answer for the end user. Use the provided plan and research. \
                         Keep it tight and useful.",
                        &format!("Compose the final answer from this packet:\n{packet}"),
                        0.4,
                        1024,
                    )
                    .await
            }
        })
        .register(
            "Remember",
            move |_ctx: ActivityContext, payload: String| {
                let memory_db = memory_db.clone();
                async move {
                    let text = payload.replace('\'', "''");
                    memory_db
                        .execute_sql(&format!(
                            "INSERT INTO agent_memories (text, source) VALUES ('{text}', 'Remember')"
                        ))
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(format!("stored memory ({} bytes)", payload.len()))
                }
            },
        )
        .build();

    // ── Deterministic agent control plane ────────────────────────────────
    let agent_loop = |ctx: OrchestrationContext, goal: String| async move {
        ctx.trace_info(format!("NVIDIA AgentLoop start: {goal}"));
        ctx.set_custom_status(r#"{"phase":"planning","provider":"nvidia"}"#);
        ctx.set_kv_value("goal", &goal);

        let plan = ctx.schedule_activity("Plan", goal.clone()).await?;
        ctx.set_kv_value("plan", &plan);
        ctx.set_custom_status(r#"{"phase":"research"}"#);

        let research = ctx.schedule_activity("Research", goal.clone()).await?;
        ctx.set_kv_value("research", &research);
        ctx.set_custom_status(r#"{"phase":"drafting"}"#);

        let packet = json!({
            "goal": goal,
            "plan": plan,
            "research": research
        })
        .to_string();
        let draft = ctx.schedule_activity("Draft", packet).await?;
        ctx.set_kv_value("draft", &draft);
        ctx.set_custom_status(r#"{"phase":"remembering"}"#);

        let memory_blob = format!("goal={goal}\nplan={plan}\ndraft={draft}");
        let remembered = ctx.schedule_activity("Remember", memory_blob).await?;
        ctx.set_kv_value("remembered", &remembered);

        ctx.set_custom_status(r#"{"phase":"done"}"#);
        Ok(draft)
    };

    let orchestrations = OrchestrationRegistry::builder()
        .register("AgentLoop", agent_loop)
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store.clone());

    let instance_id = format!(
        "nvidia-agent-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );

    client
        .start_orchestration(&instance_id, "AgentLoop", goal)
        .await?;
    println!("started instance: {instance_id}");

    match client
        .wait_for_orchestration(&instance_id, Duration::from_secs(300))
        .await
        .map_err(|e| format!("wait error: {e:?}"))?
    {
        OrchestrationStatus::Completed { output, .. } => {
            println!(
                "\n========== AGENT FINAL ANSWER ==========\n{output}\n========================================\n"
            );
        }
        OrchestrationStatus::Failed { details, .. } => {
            eprintln!("agent failed: {}", details.display_message());
            rt.shutdown(None).await;
            return Err(details.display_message().into());
        }
        other => {
            eprintln!("unexpected status: {other:?}");
            rt.shutdown(None).await;
            return Err("orchestration did not complete".into());
        }
    }

    if let Ok(Some(plan)) = client.get_kv_value(&instance_id, "plan").await {
        println!("kv.plan (truncated) = {}", truncate(&plan, 240));
    }
    if let Ok(rows) = provider
        .query_sql(
            "SELECT id, substr(text, 1, 100), source FROM agent_memories ORDER BY id DESC LIMIT 3",
        )
        .await
    {
        println!("agent_memories:");
        for row in rows {
            println!("  {row:?}");
        }
    }

    rt.shutdown(None).await;
    println!("durable store left at {db_path}");
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let t: String = s.chars().take(max).collect();
        format!("{t}…")
    }
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
