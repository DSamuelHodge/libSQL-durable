//! `pvm` — one-binary host for a libsql-durable world.
//!
//! Open a world file (or remote topology), inspect/heal it, or run the
//! Duroxide runtime with a thin stock syscall pack until interrupted.
//!
//! ```sh
//! cargo run --bin pvm --no-default-features --features native-libsql -- health --world ./w.db
//! cargo run --bin pvm --no-default-features --features native-libsql -- run --world ./w.db
//! cargo run --bin pvm --no-default-features --features native-libsql -- start --world ./w.db "hello"
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use duroxide::runtime;
use duroxide::runtime::registry::{ActivityRegistry, OrchestrationRegistry};
use duroxide::{ActivityContext, Client, OrchestrationContext};
use libsql_durable::{
    HealOptions, LibsqlDatabaseConfig, LibsqlProvider, SCHEMA_VERSION, WORLD_FORMAT_VERSION,
};

#[derive(Debug, Parser)]
#[command(
    name = "pvm",
    about = "PVM host: open a libsql-durable world and run or inspect it",
    long_about = "One binary = disposable host CPU for a durable world file.\n\
                  Kernel state lives in the world; this process is replaceable."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Parser)]
struct WorldArgs {
    /// Local world database path (file-backed libSQL).
    #[arg(long, global = true, env = "PVM_WORLD")]
    world: Option<PathBuf>,

    /// Remote sqld / libSQL URL (alternative to --world).
    #[arg(long, global = true, env = "LIBSQL_REMOTE_URL")]
    remote: Option<String>,

    /// Auth token for remote / replica modes.
    #[arg(long, global = true, env = "LIBSQL_AUTH_TOKEN", default_value = "")]
    auth: String,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Open the world and run the Duroxide runtime until Ctrl-C.
    Run {
        #[command(flatten)]
        world: WorldArgs,
        /// Run a full heal suite once before starting dispatchers.
        #[arg(long)]
        heal_on_start: bool,
    },
    /// Print world health snapshot.
    Health {
        #[command(flatten)]
        world: WorldArgs,
    },
    /// List non-terminal processes (`ps`).
    Ps {
        #[command(flatten)]
        world: WorldArgs,
    },
    /// Show next visible work items.
    Next {
        #[command(flatten)]
        world: WorldArgs,
        #[arg(long, default_value_t = 10)]
        limit: u32,
    },
    /// Queue depth / pressure snapshot.
    Queues {
        #[command(flatten)]
        world: WorldArgs,
    },
    /// Classify why an instance is not progressing.
    Why {
        #[command(flatten)]
        world: WorldArgs,
        instance: String,
    },
    /// Ordered journal projection for an instance.
    Trace {
        #[command(flatten)]
        world: WorldArgs,
        instance: String,
        #[arg(long)]
        execution: Option<u64>,
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
    /// Run healing suite (reclaim, quarantine, fence, compact).
    Heal {
        #[command(flatten)]
        world: WorldArgs,
    },
    /// Show world manifest + host schema fence.
    Status {
        #[command(flatten)]
        world: WorldArgs,
    },
    /// Start stock process `Echo` (spins a short-lived host for this command).
    Start {
        #[command(flatten)]
        world: WorldArgs,
        /// Instance id (default: auto).
        #[arg(long)]
        instance: Option<String>,
        /// Input string passed to Echo.
        input: String,
        /// Wait for completion (seconds). 0 = fire-and-forget then exit after start.
        #[arg(long, default_value_t = 30)]
        wait_secs: u64,
    },
}

#[tokio::main]
async fn main() {
    init_tracing();
    let cli = Cli::parse();
    if let Err(e) = dispatch(cli).await {
        eprintln!("pvm error: {e}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .try_init();
}

async fn dispatch(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Commands::Run {
            world,
            heal_on_start,
        } => cmd_run(world, heal_on_start).await,
        Commands::Health { world } => {
            let p = open_provider(&world).await?;
            let h = p.health(None).await?;
            println!(
                "fence_ok={} instances={} running={} poison_orch={} poison_worker={} expired_locks={}",
                h.fence_ok,
                h.total_instances,
                h.running_instances,
                h.poison_orchestrator_items,
                h.poison_worker_items,
                h.expired_locks
            );
            println!(
                "queues: orch_unlocked={} orch_locked={} worker_unlocked={} worker_locked={}",
                h.queues.orchestrator_unlocked,
                h.queues.orchestrator_locked,
                h.queues.worker_unlocked,
                h.queues.worker_locked
            );
            for n in &h.notes {
                println!("  note: {n}");
            }
            Ok(())
        }
        Commands::Ps { world } => {
            let p = open_provider(&world).await?;
            let rows = p.ps().await?;
            if rows.is_empty() {
                println!("(no non-terminal processes)");
            }
            for r in rows {
                println!(
                    "{}\tname={}\tstatus={}\texec={}\tlock_held={}",
                    r.instance_id, r.orchestration_name, r.status, r.execution_id, r.lock_held
                );
            }
            Ok(())
        }
        Commands::Next { world, limit } => {
            let p = open_provider(&world).await?;
            let items = p.next_work(limit).await?;
            if items.is_empty() {
                println!("(no visible work)");
            }
            for it in items {
                println!(
                    "{:?}\tid={}\tinstance={:?}\tattempt={}\t{}",
                    it.queue, it.id, it.instance_id, it.attempt_count, it.summary
                );
            }
            Ok(())
        }
        Commands::Queues { world } => {
            let p = open_provider(&world).await?;
            let q = p.queues().await?;
            println!("{q:#?}");
            Ok(())
        }
        Commands::Why { world, instance } => {
            let p = open_provider(&world).await?;
            let w = p.why_blocked(&instance).await?;
            println!("{w:#?}");
            Ok(())
        }
        Commands::Trace {
            world,
            instance,
            execution,
            limit,
        } => {
            let p = open_provider(&world).await?;
            let events = p.trace(&instance, execution, limit).await?;
            for e in events {
                println!(
                    "event_id={} exec={} type={} preview={}",
                    e.event_id, e.execution_id, e.event_type, e.data_preview
                );
            }
            Ok(())
        }
        Commands::Heal { world } => {
            let p = open_provider(&world).await?;
            let report = p.heal(HealOptions::default()).await?;
            println!(
                "heal complete: actions={} rows={}",
                report.actions.len(),
                report.total_rows_affected()
            );
            for a in report.actions {
                println!("  {} rows={} {}", a.action, a.rows_affected, a.detail);
            }
            for n in report.notes {
                println!("  note: {n}");
            }
            Ok(())
        }
        Commands::Status { world } => {
            let p = open_provider(&world).await?;
            let report = p.world_open_report().await?;
            let m = &report.manifest;
            println!("world_id={}", m.world_id);
            println!(
                "schema_version={} (host {})  format_version={} (host {})",
                m.schema_version, SCHEMA_VERSION, m.world_format_version, WORLD_FORMAT_VERSION
            );
            println!(
                "provider={} {}  parent={:?}  fork_note={:?}",
                m.provider_name, m.provider_version, m.parent_world_id, m.fork_note
            );
            println!(
                "schema_ok={} format_ok={}",
                report.schema_ok, report.format_ok
            );
            for n in &report.notes {
                println!("  note: {n}");
            }
            Ok(())
        }
        Commands::Start {
            world,
            instance,
            input,
            wait_secs,
        } => cmd_start(world, instance, input, wait_secs).await,
    }
}

async fn open_provider(args: &WorldArgs) -> Result<LibsqlProvider, Box<dyn std::error::Error>> {
    let config = resolve_config(args)?;
    Ok(LibsqlProvider::new(config).await?)
}

fn resolve_config(args: &WorldArgs) -> Result<LibsqlDatabaseConfig, Box<dyn std::error::Error>> {
    if let Some(ref path) = args.world {
        return Ok(LibsqlDatabaseConfig::local(path));
    }
    if let Some(ref url) = args.remote {
        return Ok(LibsqlDatabaseConfig::remote(url, args.auth.clone()));
    }
    if std::env::var("LIBSQL_REMOTE_URL").is_ok() || std::env::var("LIBSQL_DATABASE_URL").is_ok() {
        return Ok(LibsqlDatabaseConfig::from_env());
    }
    Err(
        "provide --world PATH, --remote URL, or set LIBSQL_DATABASE_URL / LIBSQL_REMOTE_URL"
            .into(),
    )
}

/// Stock syscalls (activities). Thin, explicit, host-only — never in the kernel crate.
fn stock_activities() -> ActivityRegistry {
    ActivityRegistry::builder()
        .register("echo", |ctx: ActivityContext, input: String| async move {
            let _ = ctx;
            Ok(input)
        })
        .register(
            "sleep_ms",
            |_ctx: ActivityContext, input: String| async move {
                let ms: u64 = input.trim().parse().unwrap_or(0);
                if ms > 0 {
                    tokio::time::sleep(Duration::from_millis(ms.min(60_000))).await;
                }
                Ok(format!("slept_{ms}"))
            },
        )
        .build()
}

/// Stock process: schedule `echo` with input and return the result.
fn stock_orchestrations() -> OrchestrationRegistry {
    let echo_orch = |ctx: OrchestrationContext, input: String| async move {
        let out = ctx.schedule_activity("echo", input).await?;
        Ok(out)
    };
    OrchestrationRegistry::builder()
        .register("Echo", echo_orch)
        .build()
}

async fn cmd_run(
    world: WorldArgs,
    heal_on_start: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(open_provider(&world).await?);
    if let Some(m) = provider.world_manifest().await? {
        tracing::info!(
            world_id = %m.world_id,
            schema = m.schema_version,
            "opened world"
        );
    }

    if heal_on_start {
        let report = provider.heal(HealOptions::default()).await?;
        tracing::info!(
            actions = report.actions.len(),
            rows = report.total_rows_affected(),
            "heal_on_start complete"
        );
    }

    let store: Arc<dyn duroxide::providers::Provider> = provider.clone();
    let activities = stock_activities();
    let orchestrations = stock_orchestrations();
    let rt = runtime::Runtime::start_with_store(store, activities, orchestrations).await;

    println!(
        "pvm run: world ready (schema={SCHEMA_VERSION}); stock processes: Echo; syscalls: echo, sleep_ms"
    );
    println!("press Ctrl-C to shutdown");

    tokio::signal::ctrl_c().await?;
    println!("shutting down…");
    rt.shutdown(Some(10_000)).await;
    println!("shutdown complete");
    Ok(())
}

async fn cmd_start(
    world: WorldArgs,
    instance: Option<String>,
    input: String,
    wait_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(open_provider(&world).await?);
    let store: Arc<dyn duroxide::providers::Provider> = provider.clone();
    let activities = stock_activities();
    let orchestrations = stock_orchestrations();
    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store);

    let instance_id = instance.unwrap_or_else(|| {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        format!("echo-{ms}")
    });

    client
        .start_orchestration(&instance_id, "Echo", input)
        .await
        .map_err(|e| format!("start: {e:?}"))?;
    println!("started instance={instance_id} process=Echo");

    if wait_secs > 0 {
        match client
            .wait_for_orchestration(&instance_id, Duration::from_secs(wait_secs))
            .await
            .map_err(|e| format!("wait: {e:?}"))?
        {
            duroxide::OrchestrationStatus::Completed { output, .. } => {
                println!("completed output={output}");
            }
            duroxide::OrchestrationStatus::Failed { details, .. } => {
                println!("failed: {}", details.display_message());
            }
            other => println!("status={other:?}"),
        }
    }

    rt.shutdown(Some(5_000)).await;
    Ok(())
}
