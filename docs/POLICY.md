# Adaptive Runtime Policy (PVM Phase 6)

Policy parameters are stored in the world and can be updated from observed
health/queue behavior. Changes are audited. **Control-flow determinism is
unchanged** — only host scheduling knobs move.

## Parameters

| Field | Role |
|---|---|
| `lock_timeout_ms` | Busy / lock lease budget signal |
| `max_transient_retries` | Provider retry budget |
| `retry_base_delay_ms` | Backoff base |
| `poison_threshold` | Heal quarantine attempt threshold |
| `compact_keep_last` | History compact keep-last default |
| `source` | Who last wrote (`default`, `operator`, `adaptive`, …) |

## API

```rust
let mut policy = provider.get_runtime_policy().await?;
policy.lock_timeout_ms = 60_000;
provider.set_runtime_policy(&policy, "operator").await?;

// Bounded heuristics from health/queues:
let adapted = provider.adapt_policy_from_health().await?;

for row in provider.policy_audit_log(20).await? {
    println!("{}: {}", row.source, row.detail);
}
```

`RuntimePolicy::to_tuning()` maps into `ProviderTuning` for host connection
retry settings.

## Adaptation heuristics (v1)

| Signal | Adjustment (capped) |
|---|---|
| Locked queues / expired locks | ↑ `lock_timeout_ms` (max 120s), ↑ retries (max 8) |
| Poison items present | ↓ `poison_threshold` (min 3) |
| High orchestrator attempt pressure | `compact_keep_last` → 1 |

## Tables

- `runtime_policy` — singleton row (`id = 1`)
- `policy_audit` — append-only change log

## Exit criteria (Phase 6)

Runtime parameters adjust from observed behavior; every change is auditable;
adaptation never rewrites process journals or breaks deterministic replay.
