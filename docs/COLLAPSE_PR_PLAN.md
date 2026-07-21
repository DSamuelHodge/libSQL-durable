---
title: "Collapse PR plan"
description: "Finish-line PR DAG for one-file PVM."
---

# PR Plan: Finish the Collapse (libsql-durable)

**Repo:** `/Users/derrickhodge/projects/libsql-durable`  
**North star:** *Open one file → the whole machine is there.*  
**Unit of compute:** PVM **world** (libSQL package).  
**Unit of CPU:** one host binary (Tokio + Duroxide), disposable.  
**Not a pivot:** this finishes the one-file collapse; it does not reintroduce a multi-app harness stack.

---

## Gut test (exit criteria for the whole program)

| # | Move | Done when |
|---|---|---|
| **1** | One binary = world runtime | `pvm run --world ./w.db` opens world, dispatches forever; `pvm health\|ps\|heal` work without app code |
| **2** | Definitions-as-data + interpreter | Change `body_json` version in the world → different control flow **without rebuilding the binary** |
| **3** | Fork as default explore | Speculative work = `fork explore` → child world → discard/promote; cookbook/example uses it |
| **4** | Thin explicit syscalls | Activity handlers live only in the binary; kernel stores schedule + results + optional *catalog metadata* only |

**Hard invariants (do not violate in any PR):**

- I1 Journal primacy for control flow  
- I4 Syscall quarantine (effects only via activities)  
- I6/I9 World completeness + host replaceability  
- Kernel stays a **provider + world APIs**, not an agent framework  
- `native-libsql` only for the binary; no dual-feature builds  

---

## Current baseline (already shipped)

Phases **0–7 kernel substrate** on `main` (`SCHEMA_VERSION` / `WORLD_FORMAT_VERSION` = 2):

- Durable kernel: history, queues, locks, KV, sessions, admin  
- World package, introspect, heal, definitions **storage**, fork **copy**, policy, mesh  
- Examples: `agent_loop`, `agent_nvidia` (register Rust closures → `Runtime::start_with_store`)  
- **No `[[bin]]`**, no interpreter, no explore presets, no promote/discard API  

Honest gap: Phases 4–5 are **plumbing**. Collapse items 2–3 need **interpreter + explore UX**.

---

## Architecture target

```text
┌─────────────────────────────────────────────────────────────┐
│  pvm binary (one host)                                      │
│  · clap CLI · stock + optional activities (syscalls)        │
│  · pvm.def.v1 interpreter · Duroxide Runtime                │
│  · ops: health/ps/heal/def/fork                             │
└──────────────────────────┬──────────────────────────────────┘
                           │ Provider
┌──────────────────────────▼──────────────────────────────────┐
│  world.db (one file = the computer)                         │
│  journal · queues · locks · KV · app tables                 │
│  process_definitions + pins · policy · mesh · audit         │
└─────────────────────────────────────────────────────────────┘
```

**Crate vs binary split**

| Stays in **crate** (libsql-durable) | Lives in **binary** (`pvm`) |
|---|---|
| Provider, schema, world fence | Activity handlers (HTTP/LLM/echo/SQL helpers) |
| Introspect, heal, policy rows, mesh data | When to heal / long-run worker loop |
| Definition storage + validate IR | Default stock processes (optional) |
| Fork/package/discard helpers | CLI glue + explore cookbook path |
| No model keys, no tool marketplace | Secrets, plugins later |

---

## PR DAG (dependency order)

```text
PR1  Runtime binary skeleton + ops CLI
 │
 ├─► PR2  Syscall catalog (metadata) + stock activity pack in binary
 │
 ├─► PR3  Definition hygiene (immutable versions, delete, hash)
 │         │
 │         ▼
 │        PR4  pvm.def.v1 schema + validate on put
 │         │
 │         ▼
 │        PR5  Minimal interpreter + pin wire-up + test (data changes behavior)
 │
 └─► PR6  Fork explore presets + consistent time-travel + discard
           │
           ▼
          PR7  Explore example + cookbook (fork-first subagent path)
           │
           ▼
          PR8  Promote v1 (explicit, audited, minimal)  [optional follow-on]
```

Parallelism after PR1: **PR2 ∥ PR3**, then **PR4→PR5** and **PR6→PR7** can run in parallel once PR1 lands. PR8 depends on PR6.

---

## PR specifications

### PR1 — `pvm` binary: open world + run + ops

**Goal:** Ship the disposable host as a first-class artifact. Gut test item **1** (partial: ops + daemon; stock orch can be minimal).

**Scope**

- `Cargo.toml`: `[[bin]] name = "pvm"`, `required-features = ["native-libsql"]`; deps `clap`, `tracing-subscriber` (bin or dev as appropriate)
- `src/bin/pvm/` (or `src/bin/pvm.rs` + modules):
  - `pvm run --world PATH | --remote URL --auth … | env LIBSQL_*`
  - Long-running: open provider → empty or stock registry → `Runtime::start_with_store` → wait on shutdown signal
  - Ops (provider-only, no Runtime required): `health`, `ps`, `next`, `queues`, `why`, `trace`, `heal`
- Reuse `LibsqlDatabaseConfig::from_env` + flags
- README + short `docs/RUNTIME.md`: “one binary host”
- Smoke test: temp world → `health` / `ps` via library path used by CLI

**Out of scope:** interpreter, fork CLI, LLM activities  

**Acceptance**

- [ ] `cargo run --bin pvm --no-default-features --features native-libsql -- health --world …` works  
- [ ] `pvm run` stays up until SIGINT; world survives restart  
- [ ] No kernel bloat (CLI is thin wrappers)

**Files:** `Cargo.toml`, `src/bin/pvm/**`, `README.md`, `docs/RUNTIME.md`, optional `tests/pvm_ops_smoke.rs`

---

### PR2 — Thin syscall discipline + stock activity pack

**Goal:** Gut test item **4**. Make the boundary explicit and ship a usable default pack **in the binary only**.

**Scope**

- Document syscall rules in `docs/SYSCALLS.md`:
  - name → host handler only  
  - journal holds schedule + completion, never tool code  
  - optional world **catalog** (names, tags, idempotency class) — metadata, not `fn`
- Optional table `syscall_catalog` (or fields inside definition body): `name`, `capability_tags`, `notes` — CRUD on provider if useful for operators
- Binary stock activities (feature or always-on for bin): e.g. `echo`, `sleep_ms`, `sql_query` (via provider `query_sql`), `http_get` (allowlist later)
- `pvm run --activities stock` (default)

**Out of scope:** WASM plugins, model providers in the crate  

**Acceptance**

- [ ] Kernel has zero HTTP/LLM dependencies added for “tools”  
- [ ] Stock pack enough to run a tiny interpreted or hardcoded process end-to-end  
- [ ] Docs state: missing activity name = permanent worker failure, not kernel extension

**Files:** `docs/SYSCALLS.md`, optional `src/syscall_catalog.rs`, `src/bin/pvm/activities.rs`

---

### PR3 — Definition hygiene (migration-safe data)

**Goal:** Make stored programs version-safe before interpreting them.

**Scope**

- Immutable `(name, version)` on put: reject overwrite unless `force: true`
- Optional `body_sha256` column (additive schema; bump `SCHEMA_VERSION` → 3 if needed, or store hash only in docs/validation without column)
- `delete_process_definition(name, version)`
- `list_process_definitions_by_name(name)`
- Tests for immutability + delete

**Acceptance**

- [ ] Same version cannot silently change body  
- [ ] Pins remain valid only if definition exists  

**Files:** `src/definitions.rs`, `src/lib.rs` facade, `tests/horizon.rs` or `tests/definitions.rs`, `docs/DEFINITIONS.md`

---

### PR4 — `pvm.def.v1` IR + validation

**Goal:** Shared interpreter contract (portable across hosts that ship the same interpreter version).

**Scope**

- Document IR in `docs/DEFINITIONS.md` (and/or `docs/def-v1.md`):

```json
{
  "schema": "pvm.def.v1",
  "entry": "main",
  "activities": ["echo"],
  "steps": [
    { "id": "main", "op": "activity", "name": "echo", "input": "$input", "out": "r" },
    { "id": "done", "op": "return", "value": "$r" }
  ]
}
```

- Ops subset (v1): `activity`, `timer`, `wait`, `select`, `set_kv`, `set_status`, `return`, optional `child` later  
- `validate_definition_body(json) -> Result<()>`  
- On `put_process_definition`: if `schema == pvm.def.v1`, validate structure; plain JSON without schema still allowed (escape hatch) or warn  

**Acceptance**

- [ ] Invalid IR rejected on put when schema declared  
- [ ] Docs map each op → Duroxide journaled API (determinism rules explicit)

**Files:** `src/definitions.rs` (validate), docs, tests

---

### PR5 — Minimal interpreter + pin wire-up

**Goal:** Gut test item **2**. Process logic is data.

**Scope**

- Interpreter module (crate or bin — prefer **crate** `src/interpreter.rs` behind `native-libsql` so any host can reuse; binary just enables it):
  - Load pin → definition → validate `pvm.def.v1`  
  - Single generic orchestration (e.g. `"pvm.interpret"`) **or** bind name from definition at start  
  - Step driver: only `schedule_activity`, timers, waits, KV/status, return — no host-side branching on raw wall-clock  
- Helpers: `resolve_definition_for_instance`, pin-on-start  
- `pvm def put|get|list|pin` CLI  
- `pvm start --definition Name@version` (pins + starts)  
- **Critical test:** world with def v1.0.0 vs v1.0.1 different steps; **same binary** → different behavior  

**Out of scope:** full pg_durable DSL, loops/CAN polish, WASM  

**Acceptance**

- [ ] Exit criteria Phase 4 (real): change logic by writing data  
- [ ] Activities still host-supplied (stock pack from PR2)  
- [ ] Replay-safe: no non-determinism outside activities  

**Files:** `src/interpreter.rs`, `src/definitions.rs`, `src/bin/pvm/**`, `tests/interpreter.rs`, docs

---

### PR6 — Fork explore presets + consistent cut + discard

**Goal:** Kernel ergonomics for gut test item **3**.

**Scope**

- `ForkOptions::explore()` → `clear_scheduler_state: true`, default note `"explore"`, optional `keep_instance`  
- `ForkOptions::time_travel(after_event_id)`  
- `fork_and_open(src, dst, opts) -> (ForkResult, LibsqlProvider)`  
- **Consistent time-travel:** after history truncate, document + implement minimal alignment (executions / instance status policy; KV leave-as-is vs document) so child is resumable  
- `retain_instance_only` also clears `process_definition_pins` for other instances  
- `discard_world_package(path)` — delete db+wal+shm; refuse if same as open parent path  
- Optional fork audit row (healing-style or `world_fork_audit`)  
- CLI: `pvm fork explore --src … --dst … [--instance …]`

**Acceptance**

- [ ] One-call explore fork opens a clean child host  
- [ ] Truncated child can be opened and inspected without nonsense state  
- [ ] Discard removes package safely  

**Files:** `src/fork.rs`, `src/world.rs` if needed, `src/lib.rs`, `src/bin/pvm/**`, `tests/horizon.rs` / `tests/fork_explore.rs`, `docs/FORK.md`

---

### PR7 — Fork-first explore example + cookbook

**Goal:** Make world-grain explore the **default story** for “subagent,” not a side chat.

**Scope**

- Example `examples/explore_fork.rs` (or extend agent_loop):
  1. Run process on parent  
  2. Fork explore at a point  
  3. Alternate syscall path on child  
  4. Discard child (or leave for inspect)  
- COOKBOOK recipe: “Subagent = fork world”  
- PVM.md: mark collapse items 2–3 with honest status after PR5+PR7  

**Acceptance**

- [ ] New contributor can copy one recipe and see fork-as-explore  
- [ ] No new product noun (“harness”, “agent runtime”) introduced  

**Files:** `examples/explore_fork.rs`, `COOKBOOK.md`, `docs/PVM.md`, `Cargo.toml` example entry

---

### PR8 — Promote v1 (explicit, minimal) — **implemented**

**Goal:** Close explore/resolve loop without silent merge magic.

**Shipped (policy A):** `promote_world_package` + `PromoteOptions` / `PromoteResult`

- `confirm: true` required  
- lineage check: child `parent_world_id` == parent `world_id`  
- parent package backed up before replace  
- `world_promote_audit` on promoted world  
- optional `discard_child`  
- CLI: `pvm promote --parent … --child … --confirm`  
- tests in `tests/collapse_quality.rs`  

**Not in v1 (policy B):** selective event/KV merge into a live parent.

**Acceptance**

- [x] Promote is explicit and auditable  
- [x] Cannot promote without acknowledging parent overwrite risk  

---

## Schema version policy

| Change | Action |
|---|---|
| Additive tables/columns (catalog, audit, sha256) | Bump `SCHEMA_VERSION` (e.g. 2 → 3); keep `MIN_COMPATIBLE` if migrate is additive |
| IR is data, not schema | No bump for `pvm.def.v1` alone |
| Fence | Refuse future worlds; migrate old → new on open |

---

## Testing strategy

| Layer | Tests |
|---|---|
| Unit | IR validate, fork presets, immutability |
| Integration | interpreter two-version same binary; explore fork + discard |
| CLI smoke | health/ps/run against temp world |
| Regression | existing `horizon`, `healing`, `introspection`, `world_package`, `native_libsql_provider` stay green |

Always: `cargo test --no-default-features --features native-libsql …`

---

## Docs deliverables (by PR)

| Doc | PR |
|---|---|
| `docs/RUNTIME.md` | PR1 |
| `docs/SYSCALLS.md` | PR2 |
| `docs/DEFINITIONS.md` (IR + interpreter) | PR4–PR5 |
| `docs/FORK.md` (explore/discard/promote) | PR6–PR8 |
| `docs/PVM.md` status honesty | PR5, PR7 |
| README / COOKBOOK | PR1, PR7 |

---

## Explicit non-goals (this program)

- Embedding Duroxide as a **sqld extension** / in-server BGW (pg_durable packaging) — separate product if ever  
- True page-level CoW (reflink optional later)  
- Full SQL DSL for processes  
- Multi-primary write on one world  
- Agent marketplace / model routing inside the kernel  
- Auto mesh networking  

---

## Suggested ship waves

| Wave | PRs | User-visible outcome |
|---|---|---|
| **Wave 1 — Host is real** | PR1, PR2 | One binary opens a world and runs stock syscalls; ops CLI |
| **Wave 2 — Program is data** | PR3, PR4, PR5 | Edit definition in DB → behavior changes |
| **Wave 3 — Explore is a world** | PR6, PR7 | Fork-first subagent story |
| **Wave 4 — Resolve** | PR8 | Explicit promote |

---

## Risk register

| Risk | Mitigation |
|---|---|
| Interpreter non-determinism | Only journaled ops; review each IR op against Duroxide rules |
| Facade bloat on `LibsqlProvider` | Prefer modules + thin CLI; avoid god-object growth without need |
| Time-travel inconsistent child | PR6 must fix or hard-document KV/status policy before marketing “resume at N” |
| Stock activities pull heavy deps | Keep HTTP optional; echo/sql first |
| Schema bump breaks old worlds | Additive migrate only; fence remains |

---

## Status (shipped)

| Wave | PRs | Status |
|---|---|---|
| Wave 1 Host | PR1–PR2 | **Shipped** (`pvm` + stock syscalls) |
| Wave 2 Program is data | PR3–PR5 | **Shipped** (immutable defs + `pvm.def.v1` + interpreter) |
| Wave 3 Explore is a world | PR6–PR7 | **Shipped** (explore presets + example) |
| Wave 4 Resolve | PR8 | **Shipped** (`promote_world_package`) |
| Quality gate | — | **Shipped** (clippy `-D warnings` + `collapse_quality`) |
| Operability | Cookbook 11–13 | **Shipped** (docs + dual-mode `explore_fork`) |

---

## One-line summary

**Finish the collapse by shipping a `pvm` host binary, a `pvm.def.v1` interpreter bound to world-stored definitions, fork-first explore with discard/promote, and a strict host-only syscall pack — while the world file remains the durable computer.**  
**Done on `main`.**