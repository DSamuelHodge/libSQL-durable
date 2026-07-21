---
title: "World mesh"
description: "Peers and explicit cross-world references."
---

# World Mesh (PVM Phase 7)

Many worlds with explicit peers and cross-world references. **No hidden shared
memory**: links are data rows operators and hosts can inspect.

## Tables

| Table | Purpose |
|---|---|
| `world_mesh_peers` | Known peer worlds (`peer_world_id`, endpoint, role, meta) |
| `world_refs` | Explicit local instance → remote world/instance links |

## API

```rust
provider
    .register_mesh_peer(
        "world-edge-1",
        "libsql://edge.example/db",
        "replica",
        Some(r#"{"region":"us-east"}"#),
    )
    .await?;

provider
    .add_world_ref("local-inst", "world-edge-1", "remote-inst", Some("handoff"))
    .await?;

let peers = provider.list_mesh_peers().await?;
let refs = provider.list_world_refs(Some("local-inst")).await?;
let status = provider.mesh_status().await?;
// status.local_world_id, peer_count, ref_count, peers
```

## Rules

- Peers and refs are **metadata only** — no automatic cross-world transactions.
- Isolation remains world-grain; movement/sync uses existing topologies
  (remote, replica, offline-synced) or operator-driven package copy/fork.
- Capability boundaries (encryption keys, endpoints) stay outside the mesh
  tables; store hints in `meta_json` if useful.

## Exit criteria (Phase 7)

Operate a fleet of worlds with clear isolation and explicit cross-world
references — inspectable, no implicit shared memory.
