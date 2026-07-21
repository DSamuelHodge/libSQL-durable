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
| `return` | `value` | complete process |

`$input` and `$var` substitute from process input / prior `out` bindings.

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
