---
title: Features & benefits
description: What the World kernel can do — benefits first, not buried in architecture text.
---

These are the capabilities of the **PVM World kernel**.  
Implementation lives in this repo; the product language is World / process / syscall / mesh.

## At a glance

| You need… | You get… | How (team entry) |
|---|---|---|
| Crash-safe long-running work | Journal as truth; host can die | Open a world; run processes |
| One place for state | World file (or remote topology) holds schedule + memory + history | Local / remote / replica open paths |
| Tools without polluting control flow | Syscalls (activities) only | Host registers handlers; kernel queues results |
| Debug stuck work without spelunking | `ps`, `why`, `trace`, `queues`, `health` | `pvm` ops or provider APIs |
| Recover common failures | Heal suite + audit | `pvm heal` / `heal()` |
| Change logic without only shipping binaries | Definitions as data (`pvm.def.v1`) | `if`, `select`, activity graphs |
| Speculative “try this” safely | Fork world → explore | `ForkOptions::explore` |
| Resolve exploration | Discard child **or** promote (file / selective) | `discard` / `promote` / selective import |
| Runtime knobs that leave replay safe | Adaptive policy + audit | Policy tables; host scheduling only |
| Many worlds, explicit links | Mesh peers + world refs | Mesh tables (multi-verse foundation) |
| Day-2 ops for the team | One host binary | `pvm` CLI |

---

## Capability map (benefits → primitives)

### 1. The World is the computer

**Benefit:** Ship and resume a *machine*, not a service topology.  
**Primitive:** World package + manifest + version fence.  
**Team use:** Create/open a world; copy or fork it; resume with a host.

### 2. Disposable host CPU

**Benefit:** Crash, deploy, replace runners without lying about state.  
**Primitive:** Host process (e.g. `pvm`) opens the world; correctness is in the journal.  
**Team use:** `pvm run` / `pvm start` / your binary.

### 3. Processes, not “agent sessions”

**Benefit:** Long-running control flow with identity, hierarchy, and history.  
**Primitive:** Instances + executions + ordered journal.  
**Team use:** Start process; wait; inspect with `ps` / `trace`.

### 4. Syscalls, not hidden I/O

**Benefit:** Model/tool I/O is explicit, replay-safe at the boundary.  
**Primitive:** Worker queue + activity complete path.  
**Team use:** Register activities; never do network in pure orchestrator code.

### 5. Memory in the same world

**Benefit:** No separate “memory microservice” for process facts.  
**Primitive:** KV + app tables via SQL escape hatch.  
**Team use:** KV APIs; `execute_sql` / `query_sql` for domain tables.

### 6. Operator language

**Benefit:** Ask “what’s stuck?” without raw event archaeology.  
**Primitive:** Introspection projections.  
**Team use:** `pvm health|ps|next|why|trace|queues`.

### 7. Closed-loop healing

**Benefit:** Expired locks, poison work, orphans, runaway history — recover with audit.  
**Primitive:** Heal policies + `healing_audit`.  
**Team use:** `pvm heal` / `heal(HealOptions)`.

### 8. Programs as data

**Benefit:** Version process graphs; change control flow without only shipping new host code.  
**Primitive:** `process_definitions` + pins + interpreter (`if`, `select`, …).  
**Team use:** `pvm def-put` / `pvm interpret`.

### 9. World-grain explore

**Benefit:** Speculative work that cannot silently poison the primary computer.  
**Primitive:** Fork + lineage + clear scheduler + time-travel cut.  
**Team use:** `fork_and_open(…, ForkOptions::explore())`.

### 10. Resolve: discard or promote

**Benefit:** Explicit outcomes for exploration.  
**Primitive:**  
- **A** full file promote (backup + replace)  
- **B** selective instance import into live parent  
**Team use:** `discard_world_package` / `promote_world_package` / `selective_promote_instances`.

### 11. Adaptive policy (kernel seeds)

**Benefit:** Scheduling knobs move from observed pressure; control-flow determinism stays.  
**Primitive:** `runtime_policy` + audit + health-driven adapt.  
**Team use:** `get/set_runtime_policy`, `adapt_policy_from_health`.

### 12. Mesh foundation (multi-verse seed)

**Benefit:** Name peers and cross-world refs as data — no hidden shared memory.  
**Primitive:** `world_mesh_peers`, `world_refs`, `mesh_status`.  
**Team use:** Register peers/refs; inspect mesh status.  
**Horizon:** Adaptive dynamical system across many worlds (placement, capability, movement).

---

## Conventional “how do I…?” (team)

| How do I… | Answer |
|---|---|
| Run something durable locally | Open a world file; start a process via host |
| See if the machine is healthy | `pvm health --world ./w.db` |
| Debug a stuck process | `pvm why <id>` then `pvm trace <id>` |
| Try a risky path | Fork explore → work on child → discard or promote |
| Change process steps without rebuild | Put a new definition version; interpret it |
| Import only one explored instance | Selective promote (policy B) |
| Scale beyond one writer | More worlds (mesh), not one mega-file |

---

## Status of product surface

| Surface | Status |
|---|---|
| Kernel capabilities above | **Implemented** in-repo |
| Team docs (this site) | **Being shaped** for PVM language |
| Public brand / launch site | **Not ready** — do not market as finished consumer product |
| Multi-verse adaptive dynamics | **Next** — mesh data is the seed |

Features are no longer only “in the code.” This page is the **capability catalog**. Reference docs below go deep; product sentences start here.
