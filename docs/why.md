---
title: Why anyone should care
description: Target audiences, use cases, and why libsql-durable is not another agent framework.
---

## Target audiences

### 1. Builders of long-running AI agents (highest pull today)

People shipping agents that must survive restarts, tool failures, and multi-step plans — without reinventing queues, journals, and “harness” glue.

**They care because:** agent = process, tools = syscalls, memory = same world file — simpler than framework + runner + memory service + workflow engine + DB.

### 2. Backend / platform engineers (durable execution)

Teams looking at Temporal, Durable Functions, Restate, Inngest — who want **embeddable** Rust + **local-first** or self-hosted libSQL.

**They care because:** same durable-execution model (journal + activities), with a **portable world file** and optional remote/replica topologies.

### 3. Edge / offline / single-tenant products

Desktop agents, on-prem tools, one-customer-one-box, field laptops, air-gapped environments.

**They care because:** unit of deployment can be **one file** plus a disposable host binary.

### 4. Multi-agent / speculative workflows

Anyone spawning “try this path / sandbox / subagent” who today forks chats and loses auditability.

**They care because:** **fork explore → discard | promote** is a first-class durable computer operation.

### Not the primary audience (yet)

- Pure SQL/DBA shops who want everything inside Postgres → pg_durable is a better fit.
- Teams that need multi-primary global write conflict magic on one SQLite world.
- Products that only want a chat UI, not a durable kernel.

## Use cases

| Use case | Why this stack |
|---|---|
| Crash-safe agent loops | Plan → tool → wait human → remember in one ACID world |
| Human-in-the-loop | `wait` / `select` as journaled control flow |
| Speculative exploration | Fork world, alternate syscalls, discard or promote |
| Portable runs | Copy/fork world file; resume with compatible host |
| Day-2 ops | `pvm health/ps/trace/heal` without ad-hoc SQL |
| Process logic as data | Versioned `pvm.def.v1` graphs |
| Local → remote growth | File → `sqld` → replica / offline sync |

## Why care (the so what)

Industry agent stacks keep **growing layers**. libsql-durable **collapses** them:

| Colloquial | Kernel primitive |
|---|---|
| Agent | Process (instance + journal) |
| Harness | Disposable host (`pvm` / Tokio + Duroxide) |
| Tool / LLM | Syscall (activity) |
| Memory | KV + app tables in the same world |
| Subagent sandbox | Forked world (or child process) |
| Ops dashboard | Introspect + heal |

You are not competing as “another agent SDK.” You are competing as **the durable kernel agents converge toward** — journal as truth, host as disposable CPU, fork as explore.

## Honest limits

- Not a managed multi-tenant SaaS by itself.
- Not a full SQL workflow DSL (interpreter is intentional and limited).
- Not infinite write scale on one SQLite writer — scale by **more worlds**.
- Syscalls still need host sandbox discipline.
