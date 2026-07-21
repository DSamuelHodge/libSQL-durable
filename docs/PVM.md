# Process Virtual Machine (PVM) Spec

**Status:** architectural thesis + **active build target**  
**Scope:** what we are finishing — the PVM — not only what the crate is today  
**Audience:** maintainers and systems designers

### Build mission

> **Finish building the PVM.**  
> `libsql-durable` is not “done” when it is a good Duroxide provider.  
> It is done when it is a credible **durable kernel** for a log-structured process virtual machine:  
> one world (file or replicated topology), disposable host CPU, journal as truth, activities as syscalls,  
> and processes/subprocesses/forks as the only concurrency model that matters.

**PVM v1 (ship definition):** Phases **0–3** complete — durable kernel world, world packaging, introspection language, healing policies.  
**PVM horizon:** Phases **4–7** — definitions as data, fork/time-travel, adaptive policy, world mesh.

Agents, harnesses, and multi-agent products are **not** separate build tracks. They collapse into processes, syscalls, and world forks on this machine.

This document defines a **log-structured, single-file (or replicable) process virtual machine** built from:

| Piece | Role |
|---|---|
| **libSQL** | ACID world medium (file, remote `sqld`, replica, offline sync) |
| **Duroxide** | Deterministic replay engine + activity syscall boundary |
| **libsql-durable** | Durable kernel state: history, queues, locks, timers, KV, sessions, admin |

It is deliberately **not** an agent framework spec. Product names like “agent,” “harness,” or “workflow product” are optional renames of PVM processes and syscalls.

---

## 1. Thesis

> A **PVM world** is an ACID libSQL database whose contents fully describe ongoing computation: journal, scheduler state, memory, locks, timers, and metrics.  
> A **host** (Tokio process) is disposable CPU.  
> **Activities** are the only non-determinism ports (syscalls).  
> **History** is the only truth for control-flow resume.

**Unit of compute:** the world file (or its replicated topology), not the OS process.

**Unit of execution:** a process instance (Duroxide orchestration instance), resumed by replaying its journal.

**Unit of deployment:** cold copy of a world, warm handoff via primary/replica, or edge-local embedded replica.

This inverts the usual durable-execution architecture:

```text
Common:  distributed cluster of services  →  hosts workflows
PVM:     one durable world                →  can be hosted by 1 process or N
```

### 1.1 Seismic implication: agents and harnesses collapse into the PVM

The industry is building **layered agent stacks**:

```text
Typical agentic stack (diverging complexity)
  agent framework
    └── harness / runner
          └── memory service
          └── tool runtime
          └── subagent spawner
          └── orchestrator (optional)
          └── database / queues / logs (several)
```

The PVM claim is the opposite convergence:

```text
PVM convergence
  world (libSQL) + replay kernel (Duroxide) + durable kernel state (libsql-durable)
       ≡ process machine
  “agent”     ≡ process
  “harness”   ≡ host CPU running the replay kernel
  “tool/LLM”  ≡ syscall (activity)
  “subagent”  ≡ child process  OR  forked world (see below)
  “memory”    ≡ journal + KV + app tables in the same world
```

There is **no separate agent runtime to invent** once you accept that:

1. A long-running, crash-safe control loop **is** a PVM process.  
2. Model and tool I/O **are** syscalls (must cross the activity boundary).  
3. Shared relational state **is** the same world medium processes already use.  
4. Parallelism **is** multiple processes in one world (or many worlds).  
5. Exploration **is** process hierarchy and/or **world fork (CoW)**, not a new abstraction.

That is a seismic shift from systems that keep adding layers.  
**We are early because we treat the convergence as the design, not an accident.**

| Industry rename | PVM primitive | Same machine? |
|---|---|---|
| Agent | Process (instance) | Yes |
| Harness / runner | Host + replay engine | Yes |
| Multi-agent parallel | Concurrent processes in one world | Yes |
| Subagent | Child process (sub-orchestration) | Yes |
| Subagent “sandbox explore” | **Forked world (CoW / branch)** as a subprocess of exploration | Yes — different isolation grain |
| Tool / LLM call | Syscall (activity) | Yes |
| Agent memory | Memory (KV / tables / optional vectors) | Yes |
| Trace / observability | Journal + admin introspection | Yes |

**Parallel agents are not a special mode.**  
They are processes scheduled against the same queues and locks.

**Subagents are not a special runtime.**  
They are either:

| Isolation need | Mechanism |
|---|---|
| Structured hierarchy, shared world state | Child process (sub-orchestration) in the **same** world |
| Speculative explore / resolve / discard without poisoning primary | **World fork** (copy-on-write / branch / file clone) — a subprocess of the *world*, not only of the process tree |
| OS crash domain / sandbox | Optional host OS subprocess running the same PVM against same or forked world |

So: **db fork (CoW) and subagents are the same idea at two grains** —

- **process-grain subprocess** = child instance in-world  
- **world-grain subprocess** = forked durable computer for explore/solve/merge-or-discard  

Current agent frameworks reimplement weak versions of both (threads, child chats, scratchpads) **outside** a single ACID journaled machine. The PVM makes them first-class and durable.

---

## 2. What this is / is not

### 2.1 Is

- A **process virtual machine** whose disk, scheduler, memory, and audit log share one ACID medium
- A **portable compute capsule** when the medium is a local file (with correct open/close/WAL discipline)
- A **replicable compute world** when the medium is libSQL remote/replica/offline topology
- A **kernel substrate** for higher products (APIs, UIs, model-driven loops, multi-tenant SaaS)

### 2.2 Is not

| Not this | Why |
|---|---|
| General-purpose OS | No MMU, no kernel networking stack, no device drivers |
| Message bus / Kafka replacement | Queues exist for runtime work, not arbitrary pub/sub product surface |
| Infinite single-file write scale | SQLite single-writer limits remain; scale out by sharding **worlds** or scaling **readers** via replicas |
| Agent platform | Model/tool policy, sandboxes, and product UX are above the kernel |
| Automatic safety for arbitrary syscalls | Activities still need host-level sandboxing when dangerous |

---

## 3. Layer model

```text
┌──────────────────────────────────────────────────────────┐
│ Host (optional, replaceable)                             │
│  OS process / container / edge binary                    │
│  Tokio = ephemeral CPU                                   │
└────────────────────────────┬─────────────────────────────┘
                             │ open world + run replay
┌────────────────────────────▼─────────────────────────────┐
│ PVM                                                      │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Replay engine (Duroxide)                           │  │
│  │  deterministic control flow · select/join/timers   │  │
│  │  activity syscalls · sub-process (sub-orch)        │  │
│  └────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Durable kernel state (libsql-durable provider)     │  │
│  │  journal · queues · locks · timers · KV · sessions │  │
│  │  metrics · admin introspection · schema version    │  │
│  └────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────┐  │
│  │ World medium (libSQL)                              │  │
│  │  local file | remote sqld | embedded replica       │  │
│  │  offline-synced | multi-node primary/replica       │  │
│  │  optional encryption / namespace / bottomless      │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

**Invariant:** if the world medium is intact and schema-compatible, any capable host can resume computation.

---

## 4. Kernel primitives

These are the only first-class concepts. Everything else is composition or policy.

### 4.1 World

The durable computer.

- **Identity:** path / remote URL / namespace / encryption capability
- **Contents:** schema + all kernel tables + optional app tables
- **Open modes:** local, remote, remote-replica, offline-synced (see crate config)
- **Version fence:** `schema_meta.version` / `SCHEMA_VERSION` must be compatible with the host binary

### 4.2 Process

A durable control-flow identity.

- **Maps to:** Duroxide orchestration **instance** (and its **executions** under ContinueAsNew)
- **State machine:** derived by replaying the process journal, not by mutable “current stack” alone
- **Lifecycle:** start → running → completed | failed | continued-as-new | cancelled

### 4.3 Journal (history)

Append-oriented truth for a process.

- **Maps to:** event history tables / `read` / `append_with_execution` / ack history deltas
- **Properties:**
  - ordered by `(instance, execution_id, event_id)`
  - sufficient to re-derive orchestration decisions on replay
  - audit surface for “what happened”

**Rule:** if it affects control flow, it must be in the journal (or recoverable equivalently via activity completion events).

### 4.4 Scheduler state (queues)

What may run next.

| Queue | Purpose |
|---|---|
| Orchestrator queue | wake process control-flow (messages, timers, completions) |
| Worker queue | execute syscalls (activities) |

**Properties:**

- peek-lock semantics (visibility + lock token + attempts)
- delayed visibility (`visible_at`) for timers and deferred work
- exclusive instance locks so one dispatcher owns a process at a time

### 4.5 Syscall (activity)

The only intentional non-determinism port.

- **Maps to:** worker items + activity complete/fail messages back to the process
- **Examples:** HTTP, model inference, shell, filesystem, GPU, DB calls outside the world
- **Contract:**
  - orchestrator schedules syscall
  - worker performs it
  - result is journaled via completion path
  - orchestrator may re-execute; must not re-issue external effects unless activity is idempotent or deduped

**Rule:** host code inside pure orchestration paths must be deterministic w.r.t. journaled inputs.

### 4.6 Memory

Process-associated durable data that is not the control-flow journal.

| Kind | Maps to | Use |
|---|---|---|
| Scratch / small facts | orchestration KV (`kv_store` / `kv_delta`) | flags, cursors, short summaries |
| Live phase signal | custom status | UI / wait-state without full history scan |
| App domain data | ordinary SQL tables in the same world | documents, artifacts, vectors, indexes |

**Rule:** large blobs should not bloat the journal; store in app tables/object storage and journal references.

### 4.7 Session / locality

Affinity of syscall workers to logical sessions.

- **Maps to:** provider session tables + fetch filters
- **Purpose:** sticky execution for caches, connections, or hardware locality
- **Not:** a multi-agent social graph API — coordination is processes + shared world state

### 4.8 Introspection (admin)

Read/repair interfaces over kernel state.

- **Maps to:** `ProviderAdmin` (list/filter instances, executions, metrics, queue depths, hierarchy, delete, prune)
- **Purpose:** `ps`-like and operator actions without ad-hoc SQL (SQL remains available as escape hatch)

### 4.9 Host CPU

Ephemeral.

- Tokio runtime, dispatcher loops, in-memory registries of process definitions (today)
- May crash at any time; correctness depends on world durability, not host longevity

---

## 5. Invariants

These are the laws of the PVM. Violating them means you no longer have this machine.

### I1 — Journal primacy
Control-flow truth is the journal (plus equivalently journaled activity results). Host RAM is a cache of replay.

### I2 — Single-writer process control
At most one orchestrator dispatcher holds a process lock at a time (except after lock expiry recovery).

### I3 — Atomic turn commit
A successful process turn (ack) commits history delta, outbound syscalls, and scheduler updates together, or not at all.

### I4 — Syscall quarantine
Non-determinism enters only through activities (or external events raised into the world). Orchestrator-side I/O is a kernel violation.

### I5 — Idempotent recovery
After crash, replaying the journal and redelivering unlocked work must converge to a safe state. Activities must be designed for at-least-once execution.

### I6 — World completeness
Everything required to resume (except host code/binaries and secrets held outside) lives in the world medium or is version-fenced against it.

### I7 — Schema fence
Hosts must not silently run against incompatible kernel schema versions.

### I8 — One ACID world for kernel state
Kernel queues, locks, history, and KV for a world share one transactional medium. Dual-writing kernel state to Kafka-plus-SQL is out of model.

### I9 — Host replaceability
Any compatible host may open the world. Correctness must not depend on sticky host identity (sessions are optional affinity, not correctness).

### I10 — Explicit failure classes
Provider errors distinguish retryable (lock/busy/network) vs permanent (constraint/invalid token) so the host can implement healing without data corruption.

---

## 6. Operational semantics (sketch)

### 6.1 Process start
1. Client enqueues start message for instance.
2. Orchestrator dispatcher locks instance, loads history (empty), runs process definition.
3. Process schedules syscalls / timers / waits; ack commits history + outbound work.

### 6.2 Syscall execution
1. Worker peeks locks an activity.
2. Worker runs host function (HTTP, model, tool, …).
3. Worker acks with completion/failure message to orchestrator queue.
4. Process later wakes, journals the result via normal turn commit.

### 6.3 Timer
1. Process schedules timer as delayed-visibility orchestrator work item.
2. When `visible_at` passes, dispatcher can deliver `TimerFired` semantics.
3. No host sleep in orchestrator code paths.

### 6.4 External event
1. Client raises event into the world.
2. Waiting process is woken through orchestrator queue/history path.
3. Event payload becomes journaled input to subsequent decisions.

### 6.5 Crash
1. Host dies mid-turn: uncommitted turn vanishes; locks expire or remain until timeout.
2. New host opens world, reclaims expired locks, redelivers work, replays journals.
3. In-flight activities may re-execute → activity idempotency required.

### 6.6 Continue-as-new
1. Process identity persists as instance; new `execution_id` begins.
2. Historical executions remain inspectable; pruning is explicit admin policy.

---

## 7. World topologies

All topologies preserve the same process/syscall model.

| Topology | Medium | Primary use |
|---|---|---|
| **Local world** | `file:./world.db` | single host, simplest capsule |
| **Remote world** | `sqld` HTTP/Hrana | multi-host shared kernel state |
| **Embedded replica** | local file + sync from primary | edge reads, local latency |
| **Offline-synced** | local file + push/pull | disconnected compute continuum |
| **Multi-node sqld** | primary gRPC + replica HTTP | read scale / write-forward at server layer |
| **Encrypted world** | local AES (and/or remote key header) | capability = possession of key+file |

**Placement principle:** local-first; network is optimization and sharing, not the definition of the machine.

---

## 8. Naming map (optional renames)

Use kernel names in specs and code. Product names are projections **of the same machine**, not extra layers.

| Product / colloquial | Kernel name |
|---|---|
| Agent | Process (instance) |
| Harness / runner | Host + replay engine |
| Tool / LLM call | Syscall (activity) |
| Memory | Memory (KV and/or app tables) |
| Subagent | Child process (sub-orchestration) **or** forked world (CoW explore) |
| Multi-agent system | Many processes (± many worlds) |
| Workflow engine | PVM |
| Database | World medium |

If a design needs a new noun that does not reduce to a kernel primitive, it is probably accidental complexity.

---

## 9. Capability roadmap (phase changes)

Phases are **unlocks**, not a feature dump. Each phase changes what the system *is*.

### Phase 0 — Durable kernel world (largely current crate)

**Unlock:** correct durable execution substrate on libSQL.

- Provider contract: history, queues, locks, timers, KV, sessions, stats
- Atomic ack
- `ProviderAdmin`
- Schema migrate + version
- Local / remote / replica / offline open paths
- Remote stress + cluster validation scripts
- Engine options: namespace, encryption, SQL escape hatch

**Exit criteria:** world opens, processes run, crash resume works, remote/replica validated.

### Phase 1 — World packaging

**Unlock:** a world is a defined resume package.

- Documented package layout (db + WAL rules + runtime version fence)
- Explicit “open world” checklist (schema version, provider name/version)
- Safe copy/resume guidance (checkpoint, no live multi-writer copy)
- Optional manifest table: runtime semver range, created_at, world_id

**Status: implemented in-tree** — `src/world.rs`, [`WORLD_PACKAGE.md`](./WORLD_PACKAGE.md), `tests/world_package.rs`.

**Exit criteria:** copy world file to another machine and resume with a compatible host binary without manual schema surgery.

### Phase 2 — Introspection language

**Unlock:** the machine answers operator questions stably.

Stable projections (API and/or SQL views):

| Query | Meaning |
|---|---|
| `ps` | non-terminal processes + lock holders |
| `next` | next visible work items by queue |
| `why_blocked(instance)` | lock, wait, timer, or dependency explanation from recent journal |
| `trace(instance[, execution])` | ordered event projection |
| `queues` | depths + oldest visible ages |
| `health` | schema version, poison counts, lag signals |

**Status: implemented in-tree** — `src/introspect.rs`, [`INTROSPECTION.md`](./INTROSPECTION.md), `tests/introspection.rs`.

**Exit criteria:** an operator can debug stuck work without reading Rust or raw JSON events by hand.

### Phase 3 — Healing policies

**Unlock:** closed-loop recovery with journaled decisions.

Policies (examples):

- reclaim expired locks
- quarantine poison items beyond attempt threshold
- compact/prune runaway histories under admin policy
- fence acks after force-delete
- refuse incompatible schema

**Rule:** healing actions that change world state should themselves be auditable (table or events).

**Status: implemented in-tree** — `src/heal.rs`, [`HEALING.md`](./HEALING.md), `tests/healing.rs`.

**Exit criteria:** common failure classes recover without human SQL.

### Phase 4 — Process definitions as data

**Unlock:** the world contains programs, not only program state.

- Process graphs / versions stored in tables
- Host becomes interpreter/executor against stored definitions
- Pin `definition_version` on each execution
- Enable migration of worlds across hosts that share the interpreter contract

**Exit criteria:** change process logic by writing data (with versioning), not only by shipping a new binary—while preserving replay safety.

### Phase 5 — Fork and time travel

**Unlock:** worlds are experimentally branchable — **world-grain subprocesses**.

- Export journal prefix to event N into a new world file
- Fork for what-if execution without destroying primary
- Explore / resolve / discard as first-class ops (merge policy optional and explicit)
- Optional content-addressed activity memoization

This is the durable form of “spawn a subagent to try something”: not a side chat, a **forked computer**.

**Exit criteria:** create a branch world from a historical point and run alternate syscalls safely; discard or promote results without corrupting the parent world.

### Phase 6 — Adaptive policy

**Unlock:** runtime parameters adjust from observed behavior.

Inputs: lock hold times, queue lag, retryable error rates, history growth.  
Outputs: lease durations, concurrency caps, retry windows, compaction triggers, placement hints.

**Rule:** policy changes are versioned and auditable; adaptation must not break determinism of process control flow (only host scheduling parameters).

### Phase 7 — World mesh

**Unlock:** many worlds with placement and capability boundaries.

- shard by tenant/world_id
- capability keys per world (encryption)
- partial sync / selective replica
- cross-world references by explicit protocol (not hidden shared memory)

**Exit criteria:** operate a fleet of worlds with clear isolation and movement semantics.

---

## 10. Security model (kernel-level)

| Concern | Kernel stance |
|---|---|
| Confidentiality of world | Optional encryption at rest; key is a capability |
| Integrity of journal | ACID medium; optional future hash-chaining |
| Confused deputy syscalls | Not solved by kernel alone—host sandbox on activities |
| Multi-tenant isolation | Namespace and/or world-per-tenant; do not rely on app convention alone |
| Replay attacks on external systems | Activity idempotency keys / dedupe at syscall boundary |
| Admin power | Delete/prune are privileged; treat as root |

**Non-goal:** making arbitrary shell/HTTP activities “safe” by storing them in SQL.

---

## 11. Performance model

| Resource | Constraint | Scaling strategy |
|---|---|---|
| Writes | single-writer per world (SQLite heritage) | more worlds; shorter transactions; careful indexes |
| Reads | good local; improve with replicas | embedded replica / sqld replica |
| Process density | high (many small instances per world) | natural fit |
| Journal growth | unbounded without policy | prune, continue-as-new, externalize blobs |
| Host CPU | horizontal for workers | more worker processes sharing remote world |

Design for **many small durable processes**, not one mega-process writing continuously at database peak throughput.

---

## 12. Mapping to this repository (today)

| PVM concept | Code / surface |
|---|---|
| World open | `LibsqlDatabaseConfig` / `LibsqlEngineOptions` / `LibsqlProvider::new*` |
| Kernel state | `src/native.rs` schema + Provider impl |
| Journal | history tables + read/append/ack paths |
| Scheduler | orchestrator_queue / worker_queue |
| Syscalls | worker queue + activity completion path (Duroxide) |
| Memory | KV APIs + `execute_sql` / `query_sql` |
| Introspection | `ProviderAdmin` + `ps` / `next` / `why_blocked` / `trace` / `queues` / `health` |
| Certification | `tests/*`, `scripts/run-*-tests.sh` |
| Practical patterns | [`COOKBOOK.md`](../COOKBOOK.md) |
| Runnable process demos | `examples/agent_loop.rs`, `examples/agent_nvidia.rs` (syscalls = activities) |

**Honest position:** **PVM v1 (Phases 0–3) implemented in-tree.** Horizon is Phases 4–7.

| Target | Scope | Status |
|---|---|---|
| **PVM v1** | Phases 0–3 | **Complete** |
| **PVM horizon** | Phases 4–7 | Next ambitions (definitions-as-data, fork, adaptive, mesh) |

**Phase 1:** [`WORLD_PACKAGE.md`](./WORLD_PACKAGE.md) · **Phase 2:** [`INTROSPECTION.md`](./INTROSPECTION.md) · **Phase 3:** [`HEALING.md`](./HEALING.md)

---

## 13. Non-goals (explicit)

1. Replace Linux/Windows process isolation.  
2. Bundle a model provider or tool marketplace.  
3. Hide the syscall boundary behind “autonomous agent” APIs in-kernel.  
4. Guarantee multi-primary write conflict magic on one SQLite world.  
5. Store multi-gigabyte artifacts in the journal.  
6. Provide a full distributed OS scheduler across heterogeneous hardware.

---

## 14. Design tests (for future changes)

Before accepting a feature into `libsql-durable`, ask:

1. **Does it strengthen a kernel primitive or only a product rename?**  
2. **Can a host crash immediately after the operation without lying about world state?**  
3. **Is non-determinism confined to activities/external events?**  
4. **Does it preserve single-world ACID for kernel state?**  
5. **Can the world still open under local-only topology?**  
6. **Is failure retryable vs permanent classified correctly?**  
7. **Does it create a second source of truth?** If yes, reject or redesign.

If a proposal needs “agent memory policy,” “tool sandbox,” or “prompt graph DSL,” it likely belongs **above** this crate unless it is truly process-definition-as-data (Phase 4) with a replay-safe interpreter contract.

---

## 15. One-sentence summary

**`libsql-durable` is the durable kernel-state layer of a log-structured process virtual machine whose entire world can live in one libSQL file—and whose CPU can be thrown away at any time.**

**Corollary:** agent frameworks are converging on this machine whether they know it or not; processes, syscalls, shared state, and forked exploration are not features to layer on later — they **are** the PVM.

---

## 16. Related docs

| Doc | Role |
|---|---|
| [`README.md`](../README.md) | crate usage, features, validation commands |
| [`COOKBOOK.md`](../COOKBOOK.md) | practical topologies and recipes |
| Duroxide docs | replay model, registries, client APIs |

---

*This spec is a compass. Implementation phases land only when invariants and exit criteria are met—not when metaphors are attractive.*
