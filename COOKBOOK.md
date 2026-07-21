# libsql-durable Use-Case Cookbook

Practical patterns for pairing **Duroxide** (durable execution) with **native libSQL**
(local / remote / replica / offline / encrypted).

For the deeper architecture thesis—single-file / replicable **process virtual machine**,
kernel primitives, invariants, and roadmap—see [`docs/PVM.md`](./docs/PVM.md).

All snippets assume:

```toml
[dependencies]
libsql-durable = { path = ".", features = ["native-libsql"], default-features = false }
duroxide = "0.1.29"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

---

## Quick decision matrix

| If you need… | Use this mode | Start with |
|---|---|---|
| Single process, simplest ops | **Local file** | Recipe 1 |
| Multiple app workers / shared state | **Remote `sqld`** | Recipe 2 |
| Fast local reads, central writes | **Embedded replica** | Recipe 3 |
| Offline laptop/agent, sync later | **Offline synced** | Recipe 4 |
| Sensitive durable state | **Local encryption** | Recipe 5 |
| Multi-tenant isolation | **Namespace or per-tenant DB** | Recipe 6 |
| Workflows + embeddings in one store | **SQL escape hatch + vectors** | Recipe 7 |
| Full agent harness on one runtime/DB | **AgentLoop orchestration** | Recipe 10 |
| One binary host + day-2 ops | **`pvm` CLI** | Recipe 11 |
| Process logic as data (not only binary) | **`pvm.def.v1` interpreter** | Recipe 12 |
| Speculative subagent / explore | **World fork → discard or promote** | Recipe 13 |

---

## Recipe 1 — Single-node durable workflows (local file)

**When:** CLI tools, prototypes, single-server apps, CI.

**Topology:** one process → one `file:./durable.db`.

```rust
use libsql_durable::LibsqlProvider;
use duroxide::providers::Provider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = LibsqlProvider::new_local("./data/durable.db").await?;
    println!("ready: {} v{}", provider.name(), provider.version());

    // Pass `provider` into your Duroxide runtime / client.
    Ok(())
}
```

**Env equivalent:**

```sh
export LIBSQL_DATABASE_URL=file:./data/durable.db
# or simply rely on from_env defaults
```

**Ops notes:**
- Back up the file (and `-wal`/`-shm` if present) with the process stopped or via SQLite backup APIs.
- Great default until you need multi-process workers.

---

## Recipe 2 — Multi-worker backend on self-hosted `sqld`

**When:** API servers, background workers, horizontal scale-out of activities.

**Topology:** N app processes → one primary `sqld` (HTTP).

```sh
# one-time
./scripts/install-sqld.sh
./scripts/start-sqld.sh   # http://127.0.0.1:18080 by default
```

```rust
use libsql_durable::{LibsqlDatabaseConfig, LibsqlProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = LibsqlProvider::new(
        LibsqlDatabaseConfig::remote("http://127.0.0.1:18080", /* auth */ ""),
    )
    .await?;

    // All workers share the same remote store: queues + instance locks coordinate them.
    Ok(())
}
```

**Env:**

```sh
export LIBSQL_REMOTE_URL=http://127.0.0.1:18080
export LIBSQL_AUTH_TOKEN=   # set if your sqld requires JWT
export LIBSQL_BUSY_TIMEOUT_MS=5000
export LIBSQL_TRANSIENT_RETRIES=4
```

**Ops notes:**
- Run one primary; scale app/worker processes freely.
- Validate with `./scripts/run-remote-tests.sh` and `./scripts/run-remote-stress.sh`.
- Optional durability: `SQLD_ENABLE_BOTTOMLESS_REPLICATION=1` for S3-style continuous backup.

**Tag-routed workers (pattern):**
- Schedule activities with tags (`gpu`, `email`, …).
- Run specialized workers with matching `TagFilter` in Duroxide runtime options.
- Provider already stores/filters tags on the worker queue.

---

## Recipe 3 — Edge / local reads with embedded replica

**When:** desktop apps, edge nodes, status UIs that need low-latency history/KV reads.

**Topology:** local replica file ← sync ← remote primary `sqld`.

```rust
use std::time::Duration;
use libsql_durable::{LibsqlDatabaseConfig, LibsqlEngineOptions, LibsqlProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = LibsqlEngineOptions::default()
        .with_sync_interval(Duration::from_secs(2))
        .with_read_your_writes(true);

    let replica = LibsqlProvider::new(
        LibsqlDatabaseConfig::remote_replica(
            "./data/replica.db",
            "http://127.0.0.1:18080",
            "",
        )
        .with_options(options),
    )
    .await?;

    // Optional explicit pull:
    replica.sync().await?;

    // Writes are forwarded to primary; reads can be local after sync.
    Ok(())
}
```

**Env:**

```sh
export LIBSQL_REMOTE_URL=http://127.0.0.1:18080
export LIBSQL_REPLICA_PATH=./data/replica.db
export LIBSQL_SYNC_INTERVAL_MS=2000
export LIBSQL_READ_YOUR_WRITES=true
```

**Ops notes:**
- First open needs a clean replica path or a previously synced file.
- Validate with `./scripts/run-replica-tests.sh`.
- For multi-node *server* replicas (not embedded), use `./scripts/start-cluster.sh`.

---

## Recipe 4 — Offline agent / laptop that syncs later

**When:** agents, field tools, laptops that work disconnected.

**Topology:** local offline-synced DB ↔ remote primary when online.

```rust
use std::time::Duration;
use libsql_durable::{LibsqlDatabaseConfig, LibsqlEngineOptions, LibsqlProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = LibsqlEngineOptions::default()
        .with_remote_writes(true)
        .with_read_your_writes(true)
        .with_sync_interval(Duration::from_secs(5));

    let provider = LibsqlProvider::new(
        LibsqlDatabaseConfig::offline_synced(
            "./data/offline.db",
            "http://127.0.0.1:18080",
            "",
        )
        .with_options(options),
    )
    .await?;

    // Work offline against local file; background/interval sync pushes & pulls.
    let _ = provider.sync().await;
    Ok(())
}
```

**Env:**

```sh
export LIBSQL_REMOTE_URL=http://127.0.0.1:18080
export LIBSQL_REPLICA_PATH=./data/offline.db
export LIBSQL_OFFLINE_SYNC=1
export LIBSQL_REMOTE_WRITES=true
export LIBSQL_SYNC_INTERVAL_MS=5000
```

**Ops notes:**
- Expect conflict/latency semantics of offline sync; design activities to be idempotent.
- Call `sync()` after critical local commits if you need a hard push before exit.

---

## Recipe 5 — Encrypted durable state (compliance / secrets)

**When:** workflow input/output may contain PII, tokens, or regulated data.

**Topology:** local (or replica) file with AES-256-CBC encryption at rest.

```rust
use libsql_durable::{LibsqlDatabaseConfig, LibsqlEngineOptions, LibsqlProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 32-byte key material (example only — load from a secret manager).
    let key = b"0123456789abcdef0123456789abcdef".to_vec();

    let options = LibsqlEngineOptions::default().with_local_encryption_key(key);

    let provider = LibsqlProvider::new(
        LibsqlDatabaseConfig::local("./data/secure.db").with_options(options),
    )
    .await?;

    Ok(())
}
```

**Env:**

```sh
export LIBSQL_DATABASE_URL=file:./data/secure.db
export LIBSQL_ENCRYPTION_KEY='0123456789abcdef0123456789abcdef'
# if the key is base64 instead of raw bytes:
# export LIBSQL_ENCRYPTION_KEY_BASE64=1
```

**Ops notes:**
- Losing the key loses the database.
- Prefer KMS/secret store injection over committing keys.
- Remote servers can additionally use `LIBSQL_REMOTE_ENCRYPTION_KEY` (base64 header).

---

## Recipe 6 — Multi-tenant SaaS isolation

**When:** one control plane, many tenants.

### Option A — Namespace header (shared `sqld`)

```rust
use libsql_durable::{LibsqlDatabaseConfig, LibsqlEngineOptions, LibsqlProvider};

async fn provider_for_tenant(tenant: &str) -> Result<LibsqlProvider, Box<dyn std::error::Error>> {
    let options = LibsqlEngineOptions::default().with_namespace(tenant);
    Ok(LibsqlProvider::new(
        LibsqlDatabaseConfig::remote("http://127.0.0.1:18080", "")
            .with_options(options),
    )
    .await?)
}
```

```sh
export LIBSQL_REMOTE_URL=http://127.0.0.1:18080
export LIBSQL_NAMESPACE=tenant-acme
```

### Option B — Database-per-tenant (stronger isolation)

```rust
use libsql_durable::LibsqlProvider;

async fn provider_for_tenant(tenant: &str) -> Result<LibsqlProvider, Box<dyn std::error::Error>> {
    let path = format!("./data/tenants/{tenant}.db");
    Ok(LibsqlProvider::new_local(path).await?)
}
```

**Ops notes:**
- Namespace mode needs a server/topology that honors `x-namespace`.
- DB-per-tenant is simpler for backup/restore and noisy-neighbor control.
- Combine with Recipe 5 for per-tenant encryption keys.

---

## Recipe 7 — AI agent runtime + vectors in one store

**When:** agents with durable plans, tool calls as activities, embeddings beside workflow state.

```rust
use libsql_durable::LibsqlProvider;
use duroxide::providers::Provider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = LibsqlProvider::new_local("./data/agent.db").await?;

    // 1) Duroxide owns durable steps/retries/history via `provider`.
    // 2) App tables + vectors live in the same engine via SQL escape hatch:

    provider
        .execute_sql(
            r#"
            CREATE TABLE IF NOT EXISTS memories (
              id INTEGER PRIMARY KEY,
              text TEXT NOT NULL,
              embedding F32_BLOB(3)
            )
            "#,
        )
        .await
        .ok(); // vector column types vary by libsql build; fall back if unsupported

    if provider.engine_supports_vector().await? {
        println!("native vectors available — use vector_* SQL for RAG");
    } else {
        println!("vectors not in this build — store embeddings as blob/json instead");
    }

    // Custom status / KV on instances are ideal for "waiting on human" agent UX.
    Ok(())
}
```

**Pattern:**
| Concern | Store in |
|---|---|
| Plan steps, retries, timers | Duroxide history + queues |
| “Currently waiting on…” | `custom_status` |
| Small scratch state | orchestration KV |
| Documents / embeddings | app tables via `execute_sql` |

---

## Recipe 8 — Day-2 ops (inspect, prune, delete)

**When:** dashboards, retention jobs, stuck-instance cleanup.

```rust
use libsql_durable::LibsqlProvider;
use duroxide::providers::{InstanceFilter, Provider, ProviderAdmin, PruneOptions};

async fn nightly_retention(provider: &LibsqlProvider) -> Result<(), Box<dyn std::error::Error>> {
    let admin = provider
        .as_management_capability()
        .ok_or("ProviderAdmin missing")?;

    let completed = admin.list_instances_by_status("Completed").await?;
    println!("{} completed instances", completed.len());

    // Prune old ContinueAsNew executions on a long-lived instance.
    let _ = admin
        .prune_executions(
            "long-running-workflow",
            PruneOptions {
                keep_last: Some(2),
                ..Default::default()
            },
        )
        .await;

    // Bulk-delete terminal roots (never running).
    let _ = admin
        .delete_instance_bulk(InstanceFilter {
            limit: Some(100),
            ..Default::default()
        })
        .await;

    let metrics = admin.get_system_metrics().await?;
    println!(
        "instances={} running={} events={}",
        metrics.total_instances, metrics.running_instances, metrics.total_events
    );
    Ok(())
}
```

---

## Recipe 9 — Multi-node primary/replica `sqld` (server HA-ish)

**When:** read scaling at the HTTP layer, not only embedded client replicas.

```sh
./scripts/start-cluster.sh
# primary  http://127.0.0.1:18080  gRPC :15001
# replica  http://127.0.0.1:18081

export LIBSQL_REMOTE_URL=http://127.0.0.1:18080          # writers
export LIBSQL_REPLICA_HTTP_URL=http://127.0.0.1:18081    # readers / forwarded writes
./scripts/run-cluster-tests.sh
```

```rust
// Writers prefer primary:
let write = LibsqlProvider::new_remote("http://127.0.0.1:18080", "").await?;
// Readers can use replica HTTP (eventual consistency):
let read = LibsqlProvider::new_remote("http://127.0.0.1:18081", "").await?;
```

---

## Recipe 10 — Agent = durable orchestration (harness collapses here)

**When:** You want the agent **runner, retries, tool queue, run history, memory, and
human-in-the-loop** on one Duroxide runtime and one libSQL file (or `sqld`).

**Idea:**

```text
AgentInstance  = Duroxide orchestration ("AgentLoop")
Tools / LLM    = activities (side effects live here)
Scratch state  = orchestration KV + custom_status
Long memory    = app table in the same libSQL DB (execute_sql)
Human gate     = schedule_wait + Client::raise_event
Harness        = Runtime + LibsqlProvider
```

### Runnable examples

**Stub tools (no API key):**

```sh
cargo run --example agent_loop --no-default-features --features native-libsql
```

**Real NVIDIA OpenAI-compatible model:**

```sh
export NVIDIA_API_KEY=...   # never commit this
export NVIDIA_BASE_URL=https://integrate.api.nvidia.com/v1
export NVIDIA_MODEL=z-ai/glm-5.2
export AGENT_GOAL='In 3 bullets, why durable execution fits AI agents'

cargo run --example agent_nvidia --no-default-features --features native-libsql
```

Sources:
- [`examples/agent_loop.rs`](./examples/agent_loop.rs) — stub tools
- [`examples/agent_nvidia.rs`](./examples/agent_nvidia.rs) — real LLM activities via NVIDIA integrate API

### What the example does

1. Opens **one** `LibsqlProvider` local file.
2. Creates `agent_memories` in that same file (SQL escape hatch).
3. Registers tools as activities: `Plan`, `Research`, `Draft`, `Remember`.
4. Runs `AgentLoop` orchestration:
   - sets `custom_status` phases (`planning` → `research` → … → `done`)
   - stores `goal` / `plan` / `draft` in orchestration KV
   - races human approval vs timer (`HumanApproval` event)
   - writes a memory row via `Remember` into the same DB
5. Prints instance info, KV, and `agent_memories` rows after completion.

### Core shape (abbreviated)

```rust
use std::sync::Arc;
use std::time::Duration;
use duroxide::providers::Provider;
use duroxide::runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{
    ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus,
};
use libsql_durable::LibsqlProvider;

let provider = Arc::new(LibsqlProvider::new_local("./data/agent.db").await?);
provider.execute_sql(
    "CREATE TABLE IF NOT EXISTS agent_memories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        text TEXT NOT NULL,
        source TEXT NOT NULL
    )",
).await?;

let store: Arc<dyn Provider> = provider.clone();

let memory_db = provider.clone();
let activities = ActivityRegistry::builder()
    .register("Plan", |_ctx: ActivityContext, goal: String| async move {
        Ok(format!(r#"{{"goal":"{goal}","steps":["research","draft"]}}"#))
    })
    .register("Research", |_ctx: ActivityContext, topic: String| async move {
        Ok(format!("notes about {topic}"))
    })
    .register("Draft", |_ctx: ActivityContext, notes: String| async move {
        Ok(format!("draft from: {notes}"))
    })
    .register("Remember", move |_ctx: ActivityContext, payload: String| {
        let memory_db = memory_db.clone();
        async move {
            let text = payload.replace('\'', "''");
            memory_db
                .execute_sql(&format!(
                    "INSERT INTO agent_memories (text, source) VALUES ('{text}', 'Remember')"
                ))
                .await
                .map_err(|e| e.to_string())?;
            Ok("stored".into())
        }
    })
    .build();

let agent_loop = |ctx: OrchestrationContext, goal: String| async move {
    ctx.set_custom_status(r#"{"phase":"planning"}"#);
    ctx.set_kv_value("goal", &goal);

    let plan = ctx.schedule_activity("Plan", goal.clone()).await?;
    ctx.set_kv_value("plan", &plan);

    let notes = ctx.schedule_activity("Research", goal.clone()).await?;
    let draft = ctx.schedule_activity("Draft", notes).await?;

    // Human-in-the-loop (or timeout and continue)
    ctx.set_custom_status(r#"{"phase":"awaiting_human"}"#);
    let timeout = async {
        ctx.schedule_timer(Duration::from_secs(2)).await;
        "timeout".into()
    };
    let human = async {
        format!("approved:{}", ctx.schedule_wait("HumanApproval").await)
    };
    let (_i, decision) = ctx.select2(timeout, human).await.into_tuple();

    ctx.schedule_activity("Remember", format!("{goal} | {decision} | {draft}"))
        .await?;
    Ok(format!("done decision={decision}"))
};

let orchestrations = OrchestrationRegistry::builder()
    .register("AgentLoop", agent_loop)
    .build();

let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
let client = Client::new(store.clone());
client
    .start_orchestration("agent-1", "AgentLoop", "Explain durable agents")
    .await?;

// Elsewhere (UI / another task):
// client.raise_event("agent-1", "HumanApproval", r#"{"ok":true}"#).await?;

client
    .wait_for_orchestration("agent-1", Duration::from_secs(30))
    .await?;
rt.shutdown(None).await;
```

### Mapping: agent stack → this crate

| Usual agent harness piece | Here |
|---|---|
| Runner / step loop | `AgentLoop` orchestration |
| Tool dispatcher | Activity registry + worker queue |
| Retries / crashes | Durable history + locks + activity retry policies |
| Chat/run log | Orchestration event history |
| “What phase am I in?” | `set_custom_status` |
| Scratchpad | `set_kv_value` / `get_kv_value` |
| Vector/memory store | Same libSQL file via `execute_sql` |
| Human approval | `schedule_wait` + `raise_event` |
| Multi-worker tools | Recipe 2 (`sqld`) + activity tags |
| Offline agent laptop | Recipe 4 (`offline_synced`) |

### Rules of thumb for agents

1. **Orchestrator stays deterministic** — no raw LLM HTTP inside the orchestration body; wrap model/tool I/O as activities.
2. **Big payloads out of history** — store large tool dumps in `agent_memories` (or object storage) and keep short refs in KV/events.
3. **Idempotent tools** — activities may re-run after crash; design tools accordingly.
4. **Same provider everywhere** — Runtime and Client must share the same `Arc<dyn Provider>` (same file or same `sqld`).

### Scale path without rewriting the agent

| Stage | Change only config |
|---|---|
| Laptop prototype | `LibsqlProvider::new_local("./agent.db")` |
| Team shared store | `LibsqlDatabaseConfig::remote(...)` |
| Edge read latency | `remote_replica` + `sync_interval` |
| Encrypted at rest | `with_local_encryption_key(...)` |

---

## Minimal Duroxide wiring sketch

Provider creation is only half the story. Conceptually:

```text
LibsqlProvider  ──►  Duroxide Runtime (orchestrators + workers)
                 │
                 ├── history / queues / locks  (provider contract)
                 ├── custom status + KV
                 └── ProviderAdmin (optional client/management)
```

Downstream projects typically:
1. Build `LibsqlProvider` (one of the recipes above).
2. Register orchestrations + activities with Duroxide.
3. Start runtime with that provider as the history store.
4. Use Client APIs for start/wait/status when `ProviderAdmin` is present.

Exact runtime registration APIs live in Duroxide docs; this crate supplies the durable store.

---

## Validation cheat sheet

| Goal | Command |
|---|---|
| Local native contract | `cargo test --no-default-features --features native-libsql --test native_libsql_provider` |
| Engine options / encryption | `cargo test --no-default-features --features native-libsql --test engine_libsql_features` |
| Remote `sqld` | `./scripts/run-remote-tests.sh` |
| Remote stress | `./scripts/run-remote-stress.sh` |
| Embedded replica | `./scripts/run-replica-tests.sh` |
| Multi-node cluster | `./scripts/run-cluster-tests.sh` |

---

## Recipe 11 — One binary host (`pvm`)

**When:** You want a disposable host CPU for a world file without writing app glue.

**Docs:** [`docs/RUNTIME.md`](./docs/RUNTIME.md) · [`docs/SYSCALLS.md`](./docs/SYSCALLS.md)

```sh
# Inspect
cargo run --bin pvm --no-default-features --features native-libsql -- \
  status --world ./world.db
cargo run --bin pvm --no-default-features --features native-libsql -- \
  health --world ./world.db

# Stock Echo process (short-lived host)
cargo run --bin pvm --no-default-features --features native-libsql -- \
  start --world ./world.db "hello"

# Long-lived host (Ctrl-C to stop)
cargo run --bin pvm --no-default-features --features native-libsql -- \
  run --world ./world.db --heal-on-start
```

**Stock surface:** process `Echo`; syscalls `echo`, `sleep_ms`.  
**Kernel stays thin:** tool/LLM handlers belong in *your* binary, not the crate.

---

## Recipe 12 — Process definitions as data (`pvm.def.v1`)

**When:** You want to change control flow by writing versioned JSON in the world,
not only by shipping a new host binary. Activities (syscalls) stay host-supplied.

**Docs:** [`docs/DEFINITIONS.md`](./docs/DEFINITIONS.md)

```json
{
  "schema": "pvm.def.v1",
  "entry": "main",
  "steps": [
    { "id": "main", "op": "activity", "name": "echo", "input": "$input", "out": "r", "next": "done" },
    { "id": "done", "op": "return", "value": "$r" }
  ]
}
```

```sh
# Store definition (versions are immutable unless --force)
cargo run --bin pvm --no-default-features --features native-libsql -- \
  def-put --world ./world.db Demo 1.0.0 ./demo.json

# Run interpreter (same binary, data-driven control flow)
cargo run --bin pvm --no-default-features --features native-libsql -- \
  interpret --world ./world.db Demo@1.0.0 "hello"
```

**Rust:**

```rust
use libsql_durable::{
    interpreted_orchestrations, wrap_interpret_input, INTERPRETED_ORCH_NAME,
};

// Register interpreted_orchestrations() + your ActivityRegistry on Runtime.
// payload = wrap_interpret_input(&body_json, "input")?;
// client.start_orchestration(id, INTERPRETED_ORCH_NAME, payload).await?;
```

**Exit test:** two definition versions, **same host binary**, different behavior.

---

## Recipe 13 — Subagent explore = world fork (discard or promote)

**When:** Speculative work must not poison the primary world.  
**Idea:** subagent explore is a **world-grain subprocess**, not a side chat.

**Docs:** [`docs/FORK.md`](./docs/FORK.md)

```text
parent.db  ──fork explore──►  child.db
                               │
                    alternate syscalls
                               │
              ┌────────────────┴────────────────┐
              ▼                                 ▼
     discard_world_package              promote_world_package
     (parent untouched)                 (confirm + backup + replace)
```

### Runnable demo

```sh
# Discard path (default)
cargo run --example explore_fork --no-default-features --features native-libsql

# Promote path (parent backed up, then replaced by child)
PVM_EXPLORE_MODE=promote cargo run --example explore_fork --no-default-features --features native-libsql
```

### Library sketch

```rust
use libsql_durable::{
    discard_world_package, promote_world_package, ForkOptions, PromoteOptions,
};

// Explore
let (fork, child) = parent
    .fork_and_open(&parent_path, &child_path, ForkOptions::explore())
    .await?;

// ... run alternate work on `child` ...

// A) discard
discard_world_package(&child_path)?;

// B) promote (explicit)
// promote_world_package(
//     &parent_path,
//     &child_path,
//     PromoteOptions::confirmed().with_discard_child(true),
// ).await?;
```

### CLI promote

```sh
cargo run --bin pvm --no-default-features --features native-libsql -- \
  promote --parent ./parent.db --child ./child.db --confirm --discard-child
```

**Guards:** `--confirm` required; child must declare `parent_world_id` of parent;  
parent package copied to `*.promote-bak-<ts>` before replace; `world_promote_audit` on success.

**Not in v1:** selective merge of events/KV into a live parent (policy B).

---

## Anti-patterns

| Avoid | Prefer |
|---|---|
| Sharing one local file DB across many machines over NFS | Remote `sqld` |
| Putting huge blobs only in event history | App tables / object storage + small references in history |
| Non-idempotent activities with aggressive retries | Idempotent keys / dedupe |
| Forgetting encryption key backup | KMS + documented recovery |
| Enabling `compat-sqlite` and `native-libsql` together | One feature only (C symbol clash) |
| Side-chat “subagents” outside the journal | Child process **or** forked world (Recipe 13) |
| Shipping process graphs only in binary forever | Definitions + interpreter (Recipe 12) when portable |

---

## Where to go next

1. Pick **one** recipe (usually 1 → 11, or **10** if you are building agents).
2. Run `examples/agent_loop` (Recipe 10) or `examples/explore_fork` (Recipe 13).
3. Use `pvm` for day-2 ops (Recipe 11) and definitions (Recipe 12).
4. Add admin retention (Recipe 8) once you have real volume.
5. Layer encryption / replica / offline only when the deploy topology needs it.

Runnable examples / host in this repo:

| Example / bin | Command |
|---|---|
| Agent loop (Recipe 10) | `cargo run --example agent_loop --no-default-features --features native-libsql` |
| Explore fork discard/promote (Recipe 13) | `cargo run --example explore_fork --no-default-features --features native-libsql` |
| `pvm` host (Recipe 11) | `cargo run --bin pvm --no-default-features --features native-libsql -- --help` |
