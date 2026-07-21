# Healing Policies (PVM Phase 3)

Closed-loop recovery for common failure classes. Every mutating heal action is
written to `healing_audit`.

## Policies

| Action | API | Effect |
|---|---|---|
| **Reclaim expired locks** | `heal_reclaim_expired_locks()` | Delete expired `instance_locks`; unlock expired queue locks |
| **Quarantine poison** | `heal_quarantine_poison(threshold)` | Move high-`attempt_count` items to `healing_quarantine` |
| **Fence orphans** | `heal_fence_orphan_queue_items()` | Drop queue/lock rows for missing instances |
| **Compact histories** | `heal_compact_histories(&opts)` | Prune old executions on runaway instances |
| **Full suite** | `heal(HealOptions)` | Runs reclaim → quarantine → fence → compact |
| **Schema refuse** | (open/migrate) | Phase 1 fence refuses future schema/format |

## Audit

```rust
let report = provider.heal(HealOptions::default()).await?;
for a in &report.actions {
    println!("{} rows={} {}", a.action, a.rows_affected, a.detail);
}
for row in provider.healing_audit_log(20).await? {
    println!("{}: {} ({})", row.action, row.detail, row.rows_affected);
}
```

Tables:

- `healing_audit` — action log  
- `healing_quarantine` — removed poison work items (inspect / manual replay later)

## Defaults

| Option | Default |
|---|---|
| Poison attempt threshold | 5 |
| Runaway history events | 10_000 |
| Compact keep_last | 2 |
| Compact instance limit | 50 |

## Operator loop

```text
health() → if expired_locks/poison/orphans → heal() → health() again
```

## Exit criteria (Phase 3)

Common failure classes (expired locks, poison messages, orphan queues, runaway
history) recover via API without hand-written SQL. Schema incompatibility is
refused at world open.
