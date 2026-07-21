# AGENTS.md — Build on the World, not another harness

**Read this first.** This repo is a **Process Virtual Machine (PVM)** kernel: durable **Worlds** as computers.  
It is **not** an agent framework, harness product, or libSQL/Duroxide promo. Those were **engines we borrowed**.

| Status | Team / pre-public · crate `libsql-durable` 0.2.0 · product language: **PVM · World · Mesh** |
| Docs | https://dsamuelhodge.github.io/libSQL-durable/ · source `docs/` |

---

## 1. Instant mental model (30 seconds)

```text
WHERE MOST AGENTS ARE NOW          WHERE THIS REPO IS
─────────────────────────          ──────────────────
Agent framework                    →  Process (journaled instance)
Harness / runner                   →  Host (disposable CPU: pvm or your binary)
Memory service                     →  Same World (KV + app tables)
Tool / LLM runtime                 →  Syscall (activity)
Subagent / sandbox chat            →  Child process OR forked World
Multi-agent “platform”             →  Mesh of Worlds (multi-verse) — next
Logs for debugging                 →  Journal is truth
```

**Collapse rule:** if you are about to add a harness layer, stop. Express it as process, syscall, world memory, fork, or mesh data instead.

**One sentence:** Open a World → the machine is there. Host can die. Journal is truth. Fork to explore. Mesh is next.

---

## 2. Immediate start (copy-paste)

```sh
# Build host
cargo build --bin pvm --no-default-features --features native-libsql

# Open / inspect a world
cargo run --bin pvm --no-default-features --features native-libsql -- \
  status --world ./tmp/dev.world.db
cargo run --bin pvm --no-default-features --features native-libsql -- \
  health --world ./tmp/dev.world.db

# Run a durable process (stock Echo)
cargo run --bin pvm --no-default-features --features native-libsql -- \
  start --world ./tmp/dev.world.db "hello world"

# Explore without poisoning primary (discard)
cargo run --example explore_fork --no-default-features --features native-libsql

# Explore then promote child over parent
PVM_EXPLORE_MODE=promote cargo run --example explore_fork --no-default-features --features native-libsql

# Tests (native kernel)
cargo test --no-default-features --features native-libsql --lib --tests

# Docs site (Blume)
npm install && npm run docs:dev
```

**Feature flag:** always `--no-default-features --features native-libsql` for PVM work.  
Never enable `compat-sqlite` and `native-libsql` together (SQLite symbol clash).

---

## 3. How to build *on* this (collapse the harness)

When implementing agent-like behavior, map work to kernel primitives:

| You want… | Do this |
|---|---|
| Long-running agent loop | A **process** (orchestration instance) with journaled steps |
| Call model / tool / HTTP | A **syscall** (activity) — never orchestrator-side I/O |
| Scratch / flags / cursors | **KV** or small app tables in the **same world** |
| Human approval | `wait` / `select` (timer vs event) in process or `pvm.def.v1` |
| Change plan without rebuild | New **definition version** (`pvm.def.v1`: activity, if, select, …) |
| Speculative try / sandbox | **`fork_and_open(..., ForkOptions::explore())`** then discard or promote |
| Keep only good explore outcomes | **Selective promote** (`selective_promote_instances`) or file promote |
| Multi-world / multi-tenant | Separate worlds + **mesh** peers/refs (explicit links, no hidden shared memory) |
| Debug stuck work | `pvm why` / `trace` / `health` / `heal` — not ad-hoc print debugging only |

### Minimal host pattern (Rust)

```rust
// 1) Open world (substrate)
let provider = LibsqlProvider::new_local("./world.db").await?;

// 2) Syscalls = host handlers only
let activities = ActivityRegistry::builder()
    .register("MyTool", |ctx, input| async move { /* side effect */ Ok(out) })
    .build();

// 3) Processes = registry OR pvm.interpret + body in world
let orch = /* OrchestrationRegistry or interpreted_orchestrations() */;

// 4) Disposable CPU
let rt = Runtime::start_with_store(provider.clone(), activities, orch).await;
```

**Invariant:** control-flow truth is the journal. Host RAM is a cache.

---

## 4. Destination: self-improving World Mesh

Do **not** invent a new “self-improving agent platform.” Build toward **mesh dynamics** on Worlds:

| Layer | Now (implemented seed) | Direction |
|---|---|---|
| One World | Full kernel: journal, queues, heal, defs, fork, policy | Stable computer |
| Explore | Fork + discard / promote A/B | Speculative dynamics |
| Policy | Adaptive knobs + audit | Scheduling adapts; replay stays deterministic |
| Mesh | Peers + world_refs + status | Explicit multi-verse graph |
| Self-improving mesh | — | Worlds that fork, evaluate, promote, adapt policy, and link — **as data + kernel ops**, not a meta-harness |

**Self-improving** here means: observed health/outcomes feed **audited policy** and **world lineage** (fork/promote), not unconstrained self-modifying prompt soup outside the journal.

When adding features, ask: *does this strengthen World / process / syscall / fork / mesh?*  
If it only adds a product rename for “agent,” reject or collapse it.

---

## 5. Hard rules (do not violate)

1. **Journal primacy** — control flow must be recoverable from history (+ activity results).  
2. **Syscall quarantine** — non-determinism only via activities / external events.  
3. **Host replaceability** — no correctness depending on sticky host identity.  
4. **One ACID world for kernel state** — no dual-writing queues to Kafka-as-truth.  
5. **Product language** — lead with PVM/World/Mesh; libSQL/Duroxide are implementation.  
6. **Features must be usable** — if you add capability, update `docs/features.md` + this file or CLI help.  
7. **Tests** — `cargo test --no-default-features --features native-libsql …`; clippy clean for native.  
8. **No force-push** to `main`; no secrets in tree; don’t commit `tmp/`, `target/`, world DB files.

---

## 6. Repo map (where to edit)

| Path | Role |
|---|---|
| `src/native.rs` | Core provider / schema / queues / history |
| `src/world.rs` | Package, fence, CoW copy |
| `src/introspect.rs` / `heal.rs` | Operator language / recovery |
| `src/definitions.rs` / `interpreter.rs` | Programs as data |
| `src/fork.rs` | Fork, discard, promote A/B |
| `src/policy.rs` / `mesh.rs` | Adaptive knobs / multi-world seed |
| `src/bin/pvm.rs` | Host CLI |
| `docs/features.md` | Capability catalog (benefits) |
| `docs/vision.md` | Departure + multi-verse |
| `docs/get-started.md` | Human team onboarding |
| `blume.config.ts` | Docs site (not the kernel) |

---

## 7. Common tasks → entry points

| Task | Start at |
|---|---|
| New kernel API | `src/` + facade in `src/lib.rs` + test under `tests/` |
| New process graph | `pvm.def.v1` body + `tests/interpreter.rs` |
| Explore/promote behavior | `src/fork.rs` + `tests/collapse_quality.rs` |
| CLI surface | `src/bin/pvm.rs` + `docs/RUNTIME.md` |
| Docs / product language | `docs/features.md`, `docs/vision.md`, Blume build |
| Mesh / multi-verse design | `docs/MESH.md`, `docs/vision.md`, `src/mesh.rs` |

---

## 8. What success looks like for an agent session

- [ ] Used **World / process / syscall / fork** language, not harness jargon  
- [ ] Did not add a parallel “agent runtime” beside the kernel  
- [ ] Runnable path works (`pvm` or example) or a focused failing test first  
- [ ] Native tests green for touched surface  
- [ ] User-visible capability reflected in `docs/features.md` if behavior is new  

---

## 9. Deep dives (only if needed)

| Doc | When |
|---|---|
| [docs/vision.md](./docs/vision.md) | Why we left stacks |
| [docs/features.md](./docs/features.md) | Full benefit → primitive map |
| [docs/get-started.md](./docs/get-started.md) | Human hour-one |
| [docs/PVM.md](./docs/PVM.md) | Full architecture / invariants |
| [docs/FORK.md](./docs/FORK.md) | Explore / promote A & B |
| [docs/DEFINITIONS.md](./docs/DEFINITIONS.md) | `pvm.def.v1` IR |
| [docs/MESH.md](./docs/MESH.md) | Multi-world seed |

---

**Bottom line for agents:**  
You are not here to build a better harness. You are here to **strengthen the World kernel** and **bridge harness users into PVM** — until self-improving **World Mesh** is the natural multi-verse, not another stack.
