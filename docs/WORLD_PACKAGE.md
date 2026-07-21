# World Package (PVM Phase 1)

A **world package** is the portable unit of durable compute: one libSQL database
(and optional SQLite WAL/SHM sidecars) that holds kernel state and app tables.

## Layout (local file)

| Path | Role |
|---|---|
| `world.db` | Primary database file |
| `world.db-wal` | Write-ahead log (may be absent after checkpoint) |
| `world.db-shm` | Shared-memory index for WAL (may be absent) |

**Copy:** `copy_world_package` prefers CoW/reflink when available (macOS `cp -c`,
Linux `cp --reflink=auto`), otherwise full byte copy.

Remote topologies (`sqld`, replica, offline-synced) are the same **logical** world
with a different medium; packaging copy helpers apply to **file-backed** worlds.

## Manifest (`world_manifest`)

Single-row table (`id = 1`) written on migrate/open:

| Column | Meaning |
|---|---|
| `world_id` | Stable identity for this world |
| `schema_version` | Kernel schema revision (`SCHEMA_VERSION`) |
| `world_format_version` | Packaging format revision (`WORLD_FORMAT_VERSION`) |
| `provider_name` | e.g. `libsql-native` |
| `provider_version` | Host crate semver |
| `created_at_ms` / `updated_at_ms` | Timestamps |
| `runtime_semver_min` / `max` | Informational host range |

Hard fence uses **schema/format integers**, not semver parsing.

## Version fence

| Condition | Host behavior |
|---|---|
| `schema_version` > host `SCHEMA_VERSION` | **Refuse** — upgrade host |
| `schema_version` < host, ≥ min | **Migrate** additive DDL, then stamp host version |
| `world_format_version` > host | **Refuse** — upgrade host |

## Open-world checklist

```text
1. Confirm no live writer is mid-transaction (stop host or checkpoint WAL).
2. Open with a host binary that can migrate (schema ≤ host) and is not older than the world (schema ≰ host max).
3. Read world_manifest: world_id, schema_version, world_format_version.
4. Refuse if world schema/format is newer than this host.
5. Run migrate() to apply additive DDL and refresh manifest timestamps.
6. Verify provider name is libsql-native (or compatible) before scheduling work.
7. For copy/resume: copy db (+ wal/shm if present) then open destination only.
```

In code: `open_world_checklist()` and `provider.world_open_report().await?`.

## Safe copy / resume

```rust
use libsql_durable::{copy_world_package, LibsqlProvider};

// Prefer checkpoint while the source host still has the DB open:
let src = LibsqlProvider::new_local("./world.db").await?;
src.checkpoint_wal().await?;
// Or package_copy_to while open:
// src.package_copy_to("./world.db", "./world-copy.db").await?;

// If the host is fully stopped, filesystem copy is enough:
copy_world_package("./world.db", "./world-copy.db")?;

let resumed = LibsqlProvider::new_local("./world-copy.db").await?;
let m = resumed.world_manifest().await?.expect("manifest");
assert!(!m.world_id.is_empty());
```

### Rules

- **Do not** copy a live multi-writer world without checkpointing.
- After a clean `wal_checkpoint(TRUNCATE)`, often only `world.db` is required.
- If `-wal` exists, copy it **with** the db (consistent pair).
- Resume on another machine requires a **compatible host binary** (same or newer schema support).

## API surface

| API | Purpose |
|---|---|
| `provider.world_manifest()` | Read identity + versions |
| `provider.world_open_report()` | Fence checklist snapshot |
| `provider.checkpoint_wal()` | Best-effort TRUNCATE checkpoint |
| `provider.package_copy_to(src, dst)` | Checkpoint + copy package |
| `copy_world_package(src, dst)` | Filesystem package copy |
| `open_world_checklist()` | Static operator checklist |

## Exit criteria (Phase 1)

Copy a world file to another path/machine and resume with a compatible host
**without manual schema surgery** — `migrate()` + manifest fence handle it.
