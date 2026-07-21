# Syscalls (Activities)

**Rule:** non-determinism enters only through activities. The world journals
*schedule + completion*; the host binary supplies *handlers*.

## Boundary

```text
Host binary                         World (libsql-durable)
─────────────                       ─────────────────────
ActivityRegistry name → fn          worker_queue + history
secrets, HTTP, LLM, tools           no tool code
OrchestrationRegistry / interpreter control-flow decisions (replayed)
```

| Layer | May contain |
|---|---|
| **Kernel crate** | Queue rows, locks, history events, optional *catalog metadata* (names/tags) |
| **Host binary** | Closures / crates that perform I/O |

Missing activity name at the worker → **permanent** failure for that work item.
That is not a signal to grow the kernel.

## Stock pack (`pvm` binary)

| Name | Input | Behavior |
|---|---|---|
| `echo` | string | Returns input unchanged |
| `sleep_ms` | milliseconds as string | Sleeps (capped 60s), returns `slept_N` |

These exist so a world can run end-to-end without an app-specific binary.
Product tools (HTTP, models, shell) belong in **your** host binary or a future
plugin pack — not in `libsql-durable`.

## Determinism

- Orchestrator code must not call network/time APIs except via activities/timers.
- Activity results re-enter via completion events and are replayed from history.
- Do not branch control flow on wall-clock or random in orchestrator paths.

## Optional future: catalog metadata

A world-side `syscall_catalog` (name, capability tags, notes) may document which
syscalls a world expects. That is **metadata only** — never executable code.
