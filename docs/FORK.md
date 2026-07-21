---
title: "Fork, explore, promote"
description: "World-grain subprocess, discard, promote A/B."
---

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

// Or promote child over parent (explicit confirm; parent backed up first)
use libsql_durable::{promote_world_package, PromoteOptions};
let promoted = promote_world_package(
    &src_path,
    &dst_path,
    PromoteOptions::confirmed()
        .with_discard_child(true)
        .with_note("resolve explore"),
).await?;
// promoted.backup_path holds the pre-promote parent package
```

| Helper | Effect |
|---|---|
| `ForkOptions::explore()` | Clear scheduler + note |
| `ForkOptions::explore_instance(id)` | Explore + retain one instance |
| `ForkOptions::time_travel(n)` | Truncate history after event `n` |
| `fork_and_open` | Fork + return live child provider |
| `discard_world_package` | Delete db+wal+shm |
| `promote_world_package` | Replace parent file with child (see below) |
| `time_travel_truncate` / `retain_instance_only` / `clear_scheduler_state` | Lower-level |

**KV after time-travel:** history is cut; KV is left as-is in v1 (document before resume-sensitive explore).

## Lineage

After fork, the child world has:

- a **new** `world_id`
- `parent_world_id` = parent’s id
- `fork_note` = operator/system note

Parent world is never mutated by fork.

## Promote (v1 — file replace)

Policy **A**: the child package **replaces** the parent package on disk.

| Guard | Behavior |
|---|---|
| `confirm: true` | Required; silent promote refused |
| Lineage | Child `parent_world_id` must equal parent `world_id` (unless `without_lineage_check`) |
| Backup | Parent package copied to `parent.db-promote-bak-<ts>` (or `backup_dir`) |
| Audit | `world_promote_audit` row on the promoted world |
| Optional | `discard_child` deletes the child package after success |

```sh
cargo run --bin pvm --no-default-features --features native-libsql -- \
  promote --parent ./parent.db --child ./child.db --confirm --discard-child
```

## Selective promote (policy B)

Import **selected instances** from a forked child into a **live parent** without
replacing the parent file. Parent `world_id` is preserved.

```rust
use libsql_durable::{selective_promote_instances, SelectivePromoteOptions};

let result = selective_promote_instances(
    &parent,
    &child,
    SelectivePromoteOptions::confirmed(["explore-1", "keep-me"])
        .with_note("merge explore outcomes"),
).await?;
// result.history_events_imported, result.parent_world_id unchanged
```

For each instance: parent history/executions/kv/pins/queues for that id are
replaced with the child's rows. Other parent instances are untouched.

| | Policy A (`promote_world_package`) | Policy B (`selective_promote_instances`) |
|---|---|---|
| Unit | whole world file | listed instance ids |
| Parent identity | becomes child file | preserved |
| Backup | automatic file backup | caller may package_copy first |

## Operator loop

```text
checkpoint → fork explore → open child host
  → alternate syscalls
  → discard_world_package  OR  promote_world_package(confirm)
```

## Exit criteria (Phase 5)

Create a branch world from a historical point and run alternate syscalls safely;
discard or promote without corrupting the recoverable parent (backup retained).
