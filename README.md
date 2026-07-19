# libSQL Durable Provider

This crate exposes a reusable native local + self-hosted libSQL durable
execution provider for [Duroxide](https://docs.rs/duroxide/latest/duroxide/).
It is intended for projects that want Duroxide orchestration state stored in
embedded libSQL files or self-hosted `sqld`/`libsql-server`.

It does not require managed Turso Cloud. Remote mode targets self-hosted libSQL
servers; managed hosting can be layered on by downstream projects if they choose,
but it is not an acceptance gate for this crate.

## Prerequisites

- Rust stable.
- Docker, optional, for running a local self-hosted `sqld` during remote
  validation.

The provider has two mutually exclusive backend features:

- `compat-sqlite` (default): SQLx-backed bridge that delegates Duroxide runtime
  operations to Duroxide's upstream SQLite provider.
- `native-libsql`: real `libsql` SDK construction and schema setup for embedded
  local files, self-hosted remote `sqld`/`libsql-server`, and optional
  remote-replica topology. The local native provider now covers Duroxide's core
  runtime and management contracts, including orchestration ack as one explicit
  transaction.

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
cargo test --no-default-features --features native-libsql --test native_libsql_provider
```

## Local Stress Smoke

```sh
LIBSQL_DATABASE_URL=file:./stress-libsql.db \
  cargo test --test libsql_provider_stress -- --nocapture
```

The stress tests use Duroxide's reusable provider stress harness and require a
100% success rate.

Native mode can be stress-tested without SQLx in the normal dependency graph:

```sh
cargo test --no-default-features --features native-libsql --test libsql_provider_stress -- --nocapture
```

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

## Self-Hosted sqld

Start a local self-hosted libSQL server:

```sh
docker run --name libsql-durable-sqld \
  -p 8080:8080 \
  -v "$(pwd)/sqld-data:/var/lib/sqld" \
  -e SQLD_NODE=primary \
  ghcr.io/tursodatabase/libsql-server:latest
```

Point native mode at that server:

```sh
LIBSQL_REMOTE_URL=http://127.0.0.1:8080
LIBSQL_AUTH_TOKEN=
```

For remote-replica mode:

```sh
LIBSQL_REMOTE_URL=http://127.0.0.1:8080
LIBSQL_REPLICA_PATH=./replica.db
LIBSQL_AUTH_TOKEN=
```

Remote validation is optional and env-gated, so ordinary `cargo test` runs do
not require Docker or a running `sqld`. Before claiming a self-hosted remote or
replica configuration is production-ready, run the remote validation subset and
remote stress smoke against the target server:

```sh
LIBSQL_REMOTE_URL=http://127.0.0.1:8080 LIBSQL_AUTH_TOKEN= \
  cargo test --no-default-features --features native-libsql \
  --test native_libsql_provider native_remote_validation_subset_when_configured -- --nocapture

LIBSQL_REMOTE_URL=http://127.0.0.1:8080 LIBSQL_AUTH_TOKEN= \
  cargo test --no-default-features --features native-libsql \
  --test libsql_provider_stress libsql_remote_parallel_orchestrations_stress_smoke_when_configured -- --nocapture
```

## Native Build Check

```sh
cargo check --no-default-features --features native-libsql
cargo test --no-default-features --features native-libsql --test native_libsql_provider
cargo test --no-default-features --features native-libsql --test libsql_provider_stress -- --nocapture
cargo tree --no-default-features --features native-libsql --edges normal -i sqlx
```

The `cargo tree` command should report that `sqlx` does not match any packages,
confirming the native normal dependency graph does not include SQLx. Cargo exits
with a non-zero status for that "no matching package" result.
Dev-dependencies still use Duroxide's SQLite-backed provider validation/stress
tooling.

## Environment

- `DUROXIDE_PROVIDER=libsql` is the intended selector for downstream stress
  runners.
- `LIBSQL_DATABASE_URL=file:./stress-libsql.db` selects a local libSQL/SQLite
  file.
- `LIBSQL_REMOTE_URL=http://127.0.0.1:8080` selects a self-hosted remote
  `sqld`/`libsql-server` endpoint when building with
  `--no-default-features --features native-libsql`.
- `LIBSQL_AUTH_TOKEN=...` supplies an auth token when the self-hosted server
  requires one. It may be omitted for unauthenticated local `sqld`.
- `LIBSQL_REPLICA_PATH=./replica.db` switches the remote path to libSQL's
  remote-replica mode, using the URL above as the primary.

Never commit real auth tokens, local database files, `sqld-data`, or generated
build output.

## Native libSQL Port Status

The native backend is schema version `1`. Startup creates
`libsql_durable_schema_versions`, applies the current schema idempotently,
records version `1`, and rejects databases stamped with a newer unsupported
version.

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
- `as_management_capability`
- `list_instances`
- `list_instances_by_status`
- `list_executions`
- `read_history_with_execution_id`
- `read_history`
- `latest_execution_id`
- `get_instance_info`
- `get_execution_info`
- `get_system_metrics`
- `get_queue_depths`
- `list_children`
- `get_parent_id`
- `delete_instances_atomic`
- `delete_instance_bulk`
- `prune_executions`
- `prune_executions_bulk`

The native local acceptance gate currently passes:

- `test_multi_operation_atomic_ack`
- native core, management, extended, and additional Duroxide provider
  validations
- native schema version idempotency validation
- local parallel orchestration stress smoke with `failed == 0`
- local large-payload stress smoke with `failed == 0`
- native normal dependency graph excludes SQLx

Remaining work before calling the crate self-hosted remote-ready:

- Run the env-gated provider validation subset against self-hosted
  `sqld`/`libsql-server` using `LIBSQL_REMOTE_URL`.
- Run the env-gated stress smoke against self-hosted `sqld` with `failed == 0`.
- Optional primary/replica `sqld` durability checks.
- Remote tuning only if local native remains green and self-hosted remote runs
  show transient network or lock-timing behavior.

Intentionally out of scope for this provider hardening wave: runtime facade,
workflow DSL, agent event model, vector memory, WASM tools, and Turso-native
copy-on-write branching.
