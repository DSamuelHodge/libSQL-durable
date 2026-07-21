# PVM Runtime Binary (`pvm`)

One binary = disposable host CPU for a durable world file.

```text
pvm (Tokio + Duroxide + stock syscalls)
        │
        ▼ Provider
world.db  (libsql-durable kernel state)
```

The **world** is the computer. This process is replaceable.

## Build / run

```sh
cargo build --bin pvm --no-default-features --features native-libsql

# Inspect
cargo run --bin pvm --no-default-features --features native-libsql -- \
  status --world ./world.db
cargo run --bin pvm --no-default-features --features native-libsql -- \
  health --world ./world.db

# Long-lived host (Ctrl-C to stop)
cargo run --bin pvm --no-default-features --features native-libsql -- \
  run --world ./world.db

# One-shot stock process (starts a short-lived host)
cargo run --bin pvm --no-default-features --features native-libsql -- \
  start --world ./world.db "hello from pvm"
```

Env alternatives: `PVM_WORLD`, `LIBSQL_DATABASE_URL`, `LIBSQL_REMOTE_URL` + `LIBSQL_AUTH_TOKEN`.

## Commands

| Command | Role |
|---|---|
| `run` | Open world, optional `--heal-on-start`, dispatch until Ctrl-C |
| `start <input>` | Run stock `Echo` process (wait by default) |
| `status` | Manifest + schema/format fence |
| `health` | Fence, counts, poison, queues |
| `ps` | Non-terminal processes |
| `next` | Next visible work items |
| `queues` | Queue snapshot |
| `why <instance>` | Why blocked |
| `trace <instance>` | Journal projection |
| `heal` | Full healing suite |

## Stock host (v1)

| Kind | Names |
|---|---|
| Processes | `Echo` |
| Syscalls (activities) | `echo`, `sleep_ms` |

Syscall implementations live **only in this binary**, not in the kernel crate. See [`SYSCALLS.md`](./SYSCALLS.md).

## Collapse plan

Finishing the one-file gut test: [`COLLAPSE_PR_PLAN.md`](./COLLAPSE_PR_PLAN.md).
