# Process Definitions as Data (PVM Phase 4)

Process graphs live in the world (tables), not only in host Rust registries.
The host still supplies syscall implementations (activities); definitions name
which process/version an instance is pinned to and store a portable JSON body
for inspection, migration, and future interpreters.

## Tables

| Table | Purpose |
|---|---|
| `process_definitions` | `(name, version)` → `body_json` |
| `process_definition_pins` | `instance_id` → definition name/version |

## API

```rust
// Store / update a definition (body must be valid JSON).
provider
    .put_process_definition("AgentLoop", "1.0.0", r#"{"steps":["think","act"]}"#)
    .await?;

let def = provider
    .get_process_definition("AgentLoop", "1.0.0")
    .await?;

// Pin an instance so operators know which program version owns it.
provider
    .pin_process_definition("inst-1", "AgentLoop", "1.0.0")
    .await?;

let pin = provider.get_process_definition_pin("inst-1").await?;
let all = provider.list_process_definitions().await?;
```

## Rules

- **JSON validated on write** — garbage bodies are rejected.
- **Pin requires existing definition** — prevents dangling version pins.
- **Host still owns execution** — today the host binary supplies the interpreter;
  stored definitions are portable data for migration, audit, and future in-world
  interpreters. Replay safety remains the Duroxide journal contract.

## Exit criteria (Phase 4)

Change process logic by writing versioned data (with pins), not only by shipping
a new binary—while preserving journal primacy for control-flow resume.
