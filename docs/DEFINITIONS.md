---
title: "Definitions as data"
description: "process_definitions, pins, and pvm.def.v1 IR."
---

# Process Definitions as Data (PVM Phase 4)

Process graphs live in the world (tables), not only in host Rust registries.
The host supplies **syscalls** (activities); definitions supply **control flow**
when using the `pvm.def.v1` interpreter.

## Tables

| Table | Purpose |
|---|---|
| `process_definitions` | `(name, version)` → `body_json` (immutable unless force) |
| `process_definition_pins` | `instance_id` → definition name/version |

## API

```rust
// Insert (immutable version). Use put_process_definition_ex(..., force) to replace.
provider
    .put_process_definition("Demo", "1.0.0", body_v1)
    .await?;

// Pin + resolve
provider.pin_process_definition("inst-1", "Demo", "1.0.0").await?;
let def = provider.resolve_definition_for_instance("inst-1").await?;
```

## `pvm.def.v1` IR

```json
{
  "schema": "pvm.def.v1",
  "entry": "main",
  "steps": [
    { "id": "main", "op": "activity", "name": "echo", "input": "$input", "out": "r", "next": "done" },
    { "id": "done", "op": "return", "value": "$r" }
  ]
}
```

| Op | Fields | Maps to |
|---|---|---|
| `activity` | `name`, `input`, `out?`, `next` | `schedule_activity` |
| `timer` | `ms`, `next` | `schedule_timer` |
| `wait` | `event`, `out?`, `next` | `schedule_wait` |
| `set_kv` | `key`, `value`, `next` | `set_kv_value` |
| `set_status` | `value`, `next` | `set_custom_status` |
| `goto` | `target` | jump to step id |
| `if` | `cond`, `then`, `else` | branch on journaled vars only |
| `select` | `arms` (exactly 2), `out?` | `select2` race (timer / wait / activity) |
| `return` | `value` | complete process |

`$input` and `$var` substitute from process input / prior `out` bindings.

### `if` conditions

```json
{ "id": "b", "op": "if", "cond": "$flag", "then": "yes", "else": "no" }
{ "id": "b", "op": "if", "cond": {"eq": ["$x", "ok"]}, "then": "yes", "else": "no" }
{ "id": "b", "op": "if", "cond": {"neq": ["$x", ""]}, "then": "yes", "else": "no" }
{ "id": "b", "op": "if", "cond": {"truthy": "$x"}, "then": "yes", "else": "no" }
```

Truthy string: non-empty and not `0` / `false` / `null` / `no` (case-insensitive).

**Determinism:** branches only on values already in the var map (from input or prior journaled steps). No wall-clock or host I/O in `cond`.

### `select` (exactly two arms)

```json
{
  "id": "race",
  "op": "select",
  "out": "payload",
  "arms": [
    { "kind": "timer", "ms": 5000, "value": "timeout", "next": "on_timeout" },
    { "kind": "wait", "event": "HumanApproval", "out": "ev", "next": "on_human" }
  ]
}
```

Arm `kind`: `timer` | `wait` | `activity`. Winner sets step `out` and optional arm `out`;  
`$select_winner` is `"0"` or `"1"`. Loser work is cancelled by Duroxide select semantics.

**Loops:** use `if` + `goto` (or body that returns to a loop step) with a counter/var;  
the interpreter caps total steps at 10_000 per execution.

Bodies with `"schema":"pvm.def.v1"` are **structurally validated** on put.
Opaque JSON without that schema remains allowed for host-defined formats.

## Interpreter

```rust
use libsql_durable::{interpreted_orchestrations, wrap_interpret_input, INTERPRETED_ORCH_NAME};

let orch = interpreted_orchestrations(); // registers "pvm.interpret"
let payload = wrap_interpret_input(&body_json, "hello")?;
client.start_orchestration(&id, INTERPRETED_ORCH_NAME, payload).await?;
```

**Exit criteria:** change steps in a new definition version → different behavior
**without rebuilding the host binary** (activities still provided by the host).

## Rules

- Versions are **immutable** by default (migration-safe pins).
- Pin requires an existing definition.
- Syscall *implementations* stay host-side; definitions only *name* them.
