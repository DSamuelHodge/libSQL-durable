---
title: Vision
description: PVM is the departure from agent frameworks and harness stacks. World Mesh (multi-verse) is next.
---

## The name of the thing

| Layer | What it is | What it is not |
|---|---|---|
| **PVM** | Process Virtual Machine — the *kind* of computer | An agent framework |
| **World** | The durable substrate — one computer image (file or topology) | “Just a database” |
| **Kernel** | Journal, schedule, memory, ops, fork, policy | A harness product |
| **Host** | Disposable CPU that opens a world | The source of truth |
| **Mesh / multi-verse** | Many worlds with explicit links and adaptive dynamics | Hidden shared memory between agents |

**libSQL** and **Duroxide** are borrowed engines (medium + replay). They are not the product story.  
We built on them to ship a **World substrate (kernel)** for durable processes.

## The departure

### What stacks were doing

```text
Agent framework
  └── Harness / runner
        └── Memory service
        └── Tool runtime
        └── Subagent spawner
        └── Orchestrator (optional)
        └── Queues / logs / DBs (several)
```

Each layer reimplements durability, identity, and recovery. Naming multiplies (“agent”, “session”, “thread”, “run”) while the machine stays vague.

### What we claim

Those stacks were **converging on a process virtual machine** whether they said so or not:

| Stack word | PVM primitive |
|---|---|
| Agent | **Process** (instance + journal) |
| Harness | **Host** (disposable CPU) |
| Tool / LLM | **Syscall** (activity) |
| Memory | **World memory** (KV + tables in the same computer) |
| Subagent / explore | **Child process** or **forked world** |
| Observability | **Journal + health / heal** |

**PVM is the destination of that convergence — stated as the design, not discovered as an accident.**

We are not “another agent stack.” We are the **kernel those stacks keep reinventing**.

## What is ready vs what is next

### Ready for the development team (now)

- One **World** as the unit of compute  
- **Kernel** capabilities: durability, schedule, introspect, heal, definitions, fork, promote A/B, adaptive policy seeds, mesh *data*  
- **Host** binary (`pvm`) and recipes so the team can *use* the machine  
- Docs that teach the product language first  

### Not public-facing product marketing yet

- Final brand system (name lock, visual identity, launch site)  
- Polished onboarding for strangers with zero context  
- Guarantees about multi-verse dynamics at fleet scale  

### What’s next: World Mesh (multi-verse)

A **mesh of worlds** is not “multi-tenant folders.” It is an **adaptive dynamical system**:

- Many worlds (computers), each with identity and lineage  
- Explicit peers and cross-world refs (no hidden shared memory)  
- Policy that adapts from observed health and pressure  
- Placement, capability, and movement as first-class ops  

**PVM** says: one world is a real computer.  
**Mesh / multi-verse** says: many computers form a living system — not a bigger harness.

## How to talk about this internally

| Prefer | Avoid |
|---|---|
| “Open a world / fork a world” | “Spin up the agent harness” |
| “Process + syscall” | “Agent + tool plugin” |
| “Journal is truth” | “We log for debugging” |
| “Host is disposable” | “The service is the brain” |
| “Mesh of worlds” | “Multi-agent orchestration platform” |
| “Kernel substrate” | “libSQL durable provider for Duroxide” (implementation detail) |

Implementation detail is fine in reference docs. **Product sentences start with World and PVM.**
