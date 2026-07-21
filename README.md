# PVM — World kernel (`libsql-durable`)

**Product:** Process Virtual Machine — durable **Worlds** as computers.  
**Crate name:** `libsql-durable` (implementation). **Not** a libSQL/Duroxide promo.  
**Status:** team / pre-public — brand launch not ready.  
**Version:** 0.2.0 · [`CHANGELOG.md`](./CHANGELOG.md)

> Agent frameworks and harness stacks were converging on a process VM.  
> **PVM is that machine.** **World Mesh (multi-verse)** is next.

| Read first | |
|---|---|
| **Agents (coding agents)** | **[AGENTS.md](./AGENTS.md)** — collapse harness → World / Mesh |
| Vision | [docs/vision.md](./docs/vision.md) |
| Features & benefits | [docs/features.md](./docs/features.md) |
| Get started (team) | [docs/get-started.md](./docs/get-started.md) |

**Docs site (Blume → GitHub Pages):**  
https://dsamuelhodge.github.io/libSQL-durable/

This repository implements the **World substrate (kernel)** using libSQL as the
ACID medium and Duroxide as the replay engine. Those are **engines**, not the brand.
```sh
npm install
npm run docs:dev       # local hot reload + Orama search
npm run docs:build     # dist/ (same artifact Pages deploys)
npm run docs:preview
npm run docs:doctor
```

Deploy: push to `main` (paths under `docs/`, `blume.config.ts`, …) runs
[`.github/workflows/docs.yml`](./.github/workflows/docs.yml). One-time:
**Settings → Pages → Source: GitHub Actions**.
**Build target — Process Virtual Machine (PVM):** see [`docs/PVM.md`](./docs/PVM.md).  
Durable process kernel (world + journal + syscalls), not only a storage adapter.  
**PVM kernel (0–7) + collapse finish line complete** (`SCHEMA_VERSION` = 2):  
[World](./docs/WORLD_PACKAGE.md) · [Introspection](./docs/INTROSPECTION.md) · [Healing](./docs/HEALING.md) · [Definitions](./docs/DEFINITIONS.md) · [Fork / promote](./docs/FORK.md) · [Policy](./docs/POLICY.md) · [Mesh](./docs/MESH.md) · [Runtime](./docs/RUNTIME.md) · [PVM](./docs/PVM.md) · [Collapse plan](./docs/COLLAPSE_PR_PLAN.md)

**One-binary host (`pvm`)** — open a world, inspect/heal, run stock or interpreted processes:

```sh
cargo run --bin pvm --no-default-features --features native-libsql -- health --world ./world.db
cargo run --bin pvm --no-default-features --features native-libsql -- start --world ./world.db "hello"
cargo run --bin pvm --no-default-features --features native-libsql -- run --world ./world.db
```

**Demos:**

```sh
# Agent = process on one world file
cargo run --example agent_loop --no-default-features --features native-libsql

# Subagent explore = world fork (discard default; promote with PVM_EXPLORE_MODE=promote)
cargo run --example explore_fork --no-default-features --features native-libsql
```

## Prerequisites

- Rust stable.
- Optional local `sqld` for remote validation (binary preferred; Docker optional).

The provider has two mutually exclusive backend features:

- `compat-sqlite` (default): SQLx-backed bridge that delegates Duroxide runtime
  operations to Duroxide's upstream SQLite provider.
- `native-libsql`: real `libsql` SDK construction and schema setup for embedded
  local files, self-hosted remote `sqld`/`libsql-server`, and optional
  remote-replica topology. The local native provider now covers Duroxide's core
  runtime contract, including orchestration ack as one explicit transaction.

These features cannot be enabled together because `libsql-ffi` and SQLx's
bundled SQLite both export SQLite C symbols in one binary.

The code is split as:

- `src/compat.rs`: current green SQLx-backed bridge.
- `src/native.rs`: libSQL SDK backend, schema, runtime queues, history, locks,
  orchestration ack, custom status, KV state, sessions, and stats.
- `src/lib.rs`: public config/API wrapper.

## Local Validation

```sh
cargo test --test libsql_provider_validations
```

## Local Stress Smoke

```sh
LIBSQL_DATABASE_URL=file:./stress-libsql.db \
  cargo test --test libsql_provider_stress -- --nocapture
```

The stress tests use Duroxide's reusable provider stress harness and require a
100% success rate.

## Local Embedded Usage

Use embedded local libSQL with the default local database URL:

```sh
LIBSQL_DATABASE_URL=file:./durable.db
```

In Rust:

```rust
use duroxide::providers::Provider;
use libsql_durable::LibsqlProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = LibsqlProvider::from_env().await?;
    println!("provider ready: {}", provider.name());
    Ok(())
}
```

Downstream projects can depend on this crate from git:

```toml
[dependencies]
libsql-durable = { git = "<REPO_URL>", features = ["native-libsql"], default-features = false }
duroxide = "0.1.29"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Self-Hosted sqld (no Docker required)

Preferred on older Macs: install a prebuilt `sqld` binary and run it locally.

```sh
./scripts/install-sqld.sh          # downloads tools/bin/sqld for this OS/arch
./scripts/start-sqld.sh            # listens on 127.0.0.1:18080 by default
./scripts/run-remote-tests.sh      # sets LIBSQL_REMOTE_URL and runs remote suite
./scripts/stop-sqld.sh
```

`start-sqld.sh` defaults to port **18080** because 8080 is often already taken.
Override with `SQLD_HTTP_LISTEN_ADDR=127.0.0.1:8080` if you prefer.

Remote tests are **opt-in**: without `LIBSQL_REMOTE_URL` they skip cleanly so
everyday local `cargo test` stays green. When enabled they:

1. Connect with the native provider (`LibsqlProvider::new_remote`)
2. Apply idempotent schema migrations (`migrate` / `schema_meta.version`)
3. Clear runtime rows between factory creates (shared DB isolation)
4. Run bootstrap, core, management, prune/bulk-delete validations serially

Point native mode at the server manually:

```sh
export LIBSQL_REMOTE_URL=http://127.0.0.1:18080
export LIBSQL_AUTH_TOKEN=
cargo test --no-default-features --features native-libsql \
  --test remote_libsql_provider -- --nocapture --test-threads=1
```

### Docker alternative (optional)

```sh
docker run --name libsql-durable-sqld \
  -p 18080:8080 \
  -v "$(pwd)/sqld-data:/var/lib/sqld" \
  -e SQLD_NODE=primary \
  ghcr.io/tursodatabase/libsql-server:latest
```

### Embedded remote-replica mode

Client-side embedded replica (local file + sync from primary). This is the
crate's primary/replica durability path — not multi-node `sqld` clustering.

```sh
./scripts/start-sqld.sh
./scripts/run-replica-tests.sh
```

Or manually:

```sh
export LIBSQL_REMOTE_URL=http://127.0.0.1:18080
export LIBSQL_REPLICA_PATH=./replica.db
export LIBSQL_AUTH_TOKEN=
cargo test --no-default-features --features native-libsql \
  --test replica_libsql_provider -- --nocapture --test-threads=1
```

In Rust:

```rust
let primary = LibsqlProvider::new_remote("http://127.0.0.1:18080", "").await?;
let replica = LibsqlProvider::new_remote_replica("./replica.db", "http://127.0.0.1:18080", "").await?;
// Writes on replica are delegated to the primary; pull frames with:
replica.sync().await?;
```

## Native Build Check

```sh
cargo check --no-default-features --features native-libsql
cargo test --no-default-features --features native-libsql --test native_libsql_provider
cargo test --no-default-features --features native-libsql --test libsql_provider_stress -- --nocapture
# remote suite (requires local sqld; see scripts above)
./scripts/run-remote-tests.sh
# light remote stress smoke (failed == 0)
./scripts/run-remote-stress.sh
# embedded replica durability against the same primary
./scripts/run-replica-tests.sh
# multi-node primary/replica gRPC cluster + remote tuning
./scripts/run-cluster-tests.sh
cargo tree --no-default-features --features native-libsql --edges normal -i sqlx
```


The `cargo tree` command should report that `sqlx` does not match any packages,
confirming the native normal dependency graph does not include SQLx.
Dev-dependencies still use Duroxide's SQLite-backed provider validation/stress
tooling.

## Environment

- `DUROXIDE_PROVIDER=libsql` is the intended selector for downstream stress
  runners.
- `LIBSQL_DATABASE_URL=file:./stress-libsql.db` selects a local libSQL/SQLite
  file.
- `LIBSQL_REMOTE_URL=http://127.0.0.1:18080` selects a self-hosted remote
  `sqld`/`libsql-server` endpoint when building with
  `--no-default-features --features native-libsql`.
- `LIBSQL_AUTH_TOKEN=...` supplies an auth token when the self-hosted server
  requires one. It may be omitted for unauthenticated local `sqld`.
- `LIBSQL_REPLICA_PATH=./replica.db` switches the remote path to libSQL's
  remote-replica mode, using the URL above as the primary.
- `SQLD_HTTP_LISTEN_ADDR` / `SQLD_DB_PATH` configure `scripts/start-sqld.sh`.

Never commit real auth tokens, local database files, `tmp/`, `tools/bin/`,
`sqld-data`, or generated build output.

## Native libSQL Port Status

Port `duroxide::providers::sqlite::SqliteProvider` query-by-query from `sqlx`
to `libsql::{Connection, Transaction}` while preserving the tests added here.
The local native provider has coverage for:

- `read`
- `read_with_execution`
- `append_with_execution`
- `ack_orchestration_item`
- `enqueue_for_orchestrator`
- `enqueue_for_worker`
- `fetch_orchestration_item`
- `fetch_work_item`
- `ack_work_item`
- `renew_work_item_lock`
- `renew_orchestration_item_lock`
- `abandon_work_item`
- `abandon_orchestration_item`
- `renew_session_lock`
- `cleanup_orphaned_sessions`
- `get_custom_status`
- `get_kv_value`
- `get_kv_all_values`
- `get_instance_stats`
- `ProviderAdmin` management surface (`list_instances`, status/execution
  inspection, metrics, queue depths, hierarchy primitives, atomic delete,
  bulk delete, prune)
- Remote bootstrap/migrations (`migrate`, `schema_meta` / `SCHEMA_VERSION`)
- Remote-gated validation suite (`tests/remote_libsql_provider.rs`)
- Embedded remote replica (`new_remote_replica`, `sync`, reopen catch-up)
- Multi-node `sqld` primary/replica gRPC cluster scripts + durability tests
- Remote tuning (`ProviderTuning`, busy timeout, transient retry classification)
- Full libSQL engine option wiring (`LibsqlEngineOptions`): namespace, local
  encryption, remote encryption key, sync interval, read-your-writes, remote
  writes, offline-synced mode
- SQL escape hatch (`execute_sql` / `query_sql`) for vectors, WASM UDFs, app
  tables, and extensions (`load_extension`)
- `sqld` bottomless + extensions process knobs in start scripts

The native local acceptance gate currently passes:

- `test_multi_operation_atomic_ack`
- management validations (`list_instances`, status/execution info, metrics,
  queue depths)
- prune safety and bulk-delete safety/limits
- local parallel orchestration stress smoke with `failed == 0`
- local large-payload stress smoke with `failed == 0`
- native normal dependency graph excludes SQLx

The self-hosted remote acceptance gate (local binary `sqld`, no cloud) passes:

- remote bootstrap + idempotent `migrate` / `schema_version`
- remote multi-operation atomic ack + worker peek-lock
- remote management validations
- remote prune safety + bulk-delete safety/limits
- remote append/read/queue smoke
- remote parallel orchestration stress smoke with `failed == 0`
- remote large-payload stress smoke with `failed == 0`

Embedded remote-replica durability (primary `sqld` + local replica file) passes:

- primary write → replica `sync` → history visible
- replica write delegated to primary → visible on primary and second replica
- reopen previously synced replica path and catch up new primary frames

Remote stress defaults (override via env):

| Variable | Default | Meaning |
|---|---|---|
| `REMOTE_STRESS_MAX_CONCURRENT` | `2` | Parallel orch concurrency |
| `REMOTE_STRESS_DURATION_SECS` | `2` | Parallel stress duration |
| `REMOTE_STRESS_WAIT_TIMEOUT_SECS` | `120` | Per-run completion budget |

```sh
./scripts/run-remote-stress.sh
# or after validations:
RUN_REMOTE_STRESS=1 ./scripts/run-remote-tests.sh
```

### Multi-node primary/replica (gRPC)

Distinct from embedded client replicas: two `sqld` processes, primary exposes
gRPC, replica pulls frames and forwards writes.

```sh
./scripts/start-cluster.sh
./scripts/run-cluster-tests.sh
./scripts/stop-cluster.sh
```

Defaults:

| Role | HTTP | gRPC |
|---|---|---|
| Primary | `127.0.0.1:18080` | `127.0.0.1:15001` |
| Replica | `127.0.0.1:18081` | (connects to primary gRPC) |

### Remote tuning

`ProviderTuning` raises busy-timeout / transient retries for remote backends.
`PRAGMA busy_timeout` is applied when the engine supports it (local SQLite);
unsupported remote Hrana responses are ignored. Transient Hrana/network/lock
errors are classified as retryable; constraint failures stay permanent.

| Variable | Default (remote) | Meaning |
|---|---|---|
| `LIBSQL_BUSY_TIMEOUT_MS` | `5000` | local busy wait when supported |
| `LIBSQL_TRANSIENT_RETRIES` | `4` | max retries for retryable errors |
| `LIBSQL_RETRY_BASE_DELAY_MS` | `25` | exponential backoff base |

```rust
use libsql_durable::{LibsqlProvider, ProviderTuning};

let tuning = ProviderTuning::remote_defaults();
// or ProviderTuning::from_env()
let primary = LibsqlProvider::new_remote("http://127.0.0.1:18080", "").await?;
primary.tuning(); // Some(&ProviderTuning { ... })
```

### libSQL engine features (wired)

| Capability | How to use |
|---|---|
| **Namespace** | `LibsqlEngineOptions::with_namespace` / `LIBSQL_NAMESPACE` |
| **Local encryption** | `with_local_encryption_key` / `LIBSQL_ENCRYPTION_KEY` (AES-256-CBC) |
| **Remote encryption key** | `with_remote_encryption_key_b64` / `LIBSQL_REMOTE_ENCRYPTION_KEY` |
| **Background sync** | `with_sync_interval` / `LIBSQL_SYNC_INTERVAL_MS` (replica/offline) |
| **Read-your-writes** | `with_read_your_writes` / `LIBSQL_READ_YOUR_WRITES` |
| **Offline synced DB** | `LibsqlDatabaseConfig::offline_synced` / `LIBSQL_OFFLINE_SYNC=1` |
| **Remote writes (offline)** | `with_remote_writes` / `LIBSQL_REMOTE_WRITES` |
| **Arbitrary SQL** | `provider.execute_sql` / `provider.query_sql` |
| **Native vectors** | via SQL escape hatch; `engine_supports_vector()` probe |
| **Extensions** | `provider.load_extension(...)`; server: `SQLD_EXTENSIONS_PATH` |
| **Bottomless (S3 backup)** | `SQLD_ENABLE_BOTTOMLESS_REPLICATION=1` on start scripts |

```rust
use std::time::Duration;
use libsql_durable::{LibsqlDatabaseConfig, LibsqlEngineOptions, LibsqlProvider};

let options = LibsqlEngineOptions::default()
    .with_namespace("tenant-a")
    .with_local_encryption_key(b"0123456789abcdef0123456789abcdef")
    .with_sync_interval(Duration::from_secs(2));

let provider = LibsqlProvider::new(
    LibsqlDatabaseConfig::local("./durable.db").with_options(options),
).await?;

// App-level vector / custom SQL beside orchestration tables:
provider.execute_sql("CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT)").await?;
let supports_vector = provider.engine_supports_vector().await?;
```

Remote-ready acceptance for this crate is complete for local binary `sqld`
(single-node remote, stress, embedded replica, multi-node cluster, tuning,
and libSQL engine option wiring).

