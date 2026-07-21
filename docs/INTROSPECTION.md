---
title: "Introspection"
description: "ps, next, why_blocked, trace, queues, health."
---

# Introspection Language (PVM Phase 2)

Stable operator projections over kernel state. Goal: debug stuck work without
reading raw event JSON by hand.

## Commands

| Name | API | Meaning |
|---|---|---|
| **ps** | `provider.ps().await?` | Non-terminal processes + lock holders |
| **next** | `provider.next_work(limit).await?` | Next unlocked, visible work (orch + worker) |
| **why_blocked** | `provider.why_blocked(id).await?` | Classify why an instance is stuck |
| **trace** | `provider.trace(id, exec, limit).await?` | Ordered journal projection |
| **queues** | `provider.queues().await?` | Depths, oldest visible, max attempts |
| **health** | `provider.health(poison_threshold).await?` | Fence, counts, poison, queues |

## Examples

```rust
use libsql_durable::{BlockReason, LibsqlProvider};

let p = LibsqlProvider::new_local("./world.db").await?;

let health = p.health(None).await?;
assert!(health.fence_ok);

for proc in p.ps().await? {
    if proc.lock_held {
        println!("{} locked until {:?}", proc.instance_id, proc.locked_until_ms);
    }
    let why = p.why_blocked(&proc.instance_id).await?;
    println!("{} -> {:?}", proc.instance_id, why.reason);
}

for work in p.next_work(20).await? {
    println!("{:?} id={} attempts={}", work.queue, work.id, work.attempt_count);
}

for ev in p.trace("my-instance", None, 100).await? {
    println!("e{}:{} {}", ev.execution_id, ev.event_id, ev.event_type);
}
```

## `why_blocked` reasons

| `BlockReason` | Meaning |
|---|---|
| `Locked` | Instance lock held (or expired but still present) |
| `Delayed` | Future-visible orchestrator work (timer/delay) |
| `WorkerInFlight` | Worker queue item locked for this instance |
| `IdleOrWaitingExternal` | Running, no lock/delay; may wait on external event |
| `Terminal` | Completed / Failed / ContinuedAsNew |
| `NotFound` | No instance row |
| `Other` | Unclassified |

## Health signals

- Schema / world format fence vs this host
- Instance counts (total / running / terminal)
- Active vs expired locks
- Poison queue items (`attempt_count >= threshold`, default 5)
- Queue snapshot (unlocked/locked depths, max attempts)

## Exit criteria (Phase 2)

An operator can answer: what is running, what runs next, why stuck, what happened,
and whether the world is healthy — via these APIs alone.
