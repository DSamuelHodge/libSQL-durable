# World Fork and Time Travel (PVM Phase 5)

Fork = **world-grain subprocess**: copy the durable computer, optionally trim
journals past a point, clear scheduler state for clean explore, stamp lineage.

This is the durable form of “spawn a subagent to try something”: not a side chat,
a **forked computer**.

## API

```rust
use libsql_durable::{discard_world_package, ForkOptions, LibsqlProvider};

// Explore preset: clear scheduler, note "explore"
let (result, child) = provider
    .fork_and_open(&src_path, &dst_path, ForkOptions::explore())
    .await?;

// Or time-travel / single-instance:
// ForkOptions::time_travel(42)
// ForkOptions::explore_instance("inst-1")

// Discard child package when done exploring
discard_world_package(&dst_path)?;
```

| Helper | Effect |
|---|---|
| `ForkOptions::explore()` | Clear scheduler + note |
| `ForkOptions::explore_instance(id)` | Explore + retain one instance |
| `ForkOptions::time_travel(n)` | Truncate history after event `n` |
| `fork_and_open` | Fork + return live child provider |
| `discard_world_package` | Delete db+wal+shm |
| `time_travel_truncate` / `retain_instance_only` / `clear_scheduler_state` | Lower-level |

**KV after time-travel:** history is cut; KV is left as-is in v1 (document before resume-sensitive explore).

## Lineage

After fork, the child world has:

- a **new** `world_id`
- `parent_world_id` = parent’s id
- `fork_note` = operator/system note

Parent world is never mutated by fork.

## Operator loop

```text
checkpoint → fork_world_to(options) → open child host
  → explore with alternate syscalls
  → promote (manual merge policy) or discard child file
```

## Exit criteria (Phase 5)

Create a branch world from a historical point and run alternate syscalls safely;
discard or promote without corrupting the parent world.
