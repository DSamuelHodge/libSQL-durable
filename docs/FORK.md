# World Fork and Time Travel (PVM Phase 5)

Fork = **world-grain subprocess**: copy the durable computer, optionally trim
journals past a point, clear scheduler state for clean explore, stamp lineage.

This is the durable form of “spawn a subagent to try something”: not a side chat,
a **forked computer**.

## API

```rust
use libsql_durable::{ForkOptions, LibsqlProvider};

let result = provider
    .fork_world_to(
        &src_path,
        &dst_path,
        ForkOptions {
            note: Some("explore alternate path".into()),
            truncate_after_event_id: Some(42), // keep events ≤ 42
            keep_instance: None,               // or Some("only-this-instance")
            clear_scheduler_state: true,       // drop queues/locks/sessions
        },
    )
    .await?;

// result.parent_world_id, result.child_world_id, result.child_path
```

Lower-level helpers (also on `LibsqlProvider`):

| Method | Effect |
|---|---|
| `time_travel_truncate(after, only?)` | `DELETE` history with `event_id > after` |
| `retain_instance_only(id)` | Drop other instances' rows (explore sandbox) |
| `clear_scheduler_state()` | Empty queues, locks, sessions |
| `fork_world_files(src, dst)` | Filesystem copy only (no live provider) |

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
