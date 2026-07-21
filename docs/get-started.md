---
title: Get started (team)
description: Conventional first hour with the PVM World kernel — product language first.
---

This is for **our development team**. Public onboarding will be redesigned later.  
Goal: use the **World** as a computer in under an hour, without drowning in provider APIs.

## 0. Mental model (2 minutes)

```text
World  = durable computer (file or topology)
Host   = disposable CPU (pvm binary or your process)
Process = long-running control flow (journaled)
Syscall = only intentional non-determinism (tools / models)
```

You are not starting an “agent harness.” You are **opening a world** and **running processes**.

## 1. Install host tooling

```sh
# From repo root — Rust host
cargo build --bin pvm --no-default-features --features native-libsql

# Optional: docs site
npm install && npm run docs:dev
```

## 2. Open a world and check health

```sh
cargo run --bin pvm --no-default-features --features native-libsql -- \
  status --world ./tmp/dev.world.db

cargo run --bin pvm --no-default-features --features native-libsql -- \
  health --world ./tmp/dev.world.db
```

**Benefit:** You always know fence, instance counts, and queue pressure.

## 3. Run a process (stock)

```sh
cargo run --bin pvm --no-default-features --features native-libsql -- \
  start --world ./tmp/dev.world.db "hello from the world"
```

**Benefit:** End-to-end durable turn without writing app glue.

## 4. Inspect

```sh
cargo run --bin pvm --no-default-features --features native-libsql -- \
  ps --world ./tmp/dev.world.db

cargo run --bin pvm --no-default-features --features native-libsql -- \
  trace --world ./tmp/dev.world.db <instance-id>
```

## 5. Process logic as data

Write a `pvm.def.v1` JSON file (see [Definitions](/DEFINITIONS)), then:

```sh
cargo run --bin pvm --no-default-features --features native-libsql -- \
  def-put --world ./tmp/dev.world.db Demo 1.0.0 ./demo.json

cargo run --bin pvm --no-default-features --features native-libsql -- \
  interpret --world ./tmp/dev.world.db Demo@1.0.0 "input"
```

**Benefit:** Change control flow by versioning data; syscalls stay on the host.

## 6. Explore without poisoning primary

```sh
# Demo: discard path
cargo run --example explore_fork --no-default-features --features native-libsql

# Demo: promote path
PVM_EXPLORE_MODE=promote cargo run --example explore_fork --no-default-features --features native-libsql
```

**Benefit:** Speculative work is a **forked computer**, with discard or promote.

## 7. Next reading (in product order)

1. [Features & benefits](/features) — what the kernel can do  
2. [Vision](/vision) — departure from stacks; multi-verse next  
3. [Runtime](/RUNTIME) — full `pvm` surface  
4. [Fork](/FORK) — explore / promote A & B  
5. [PVM](/PVM) — deep architecture (for maintainers)  

Implementation crate name `libsql-durable` is how Cargo finds the kernel.  
**What you are building on is the World substrate — not “a libSQL demo.”**
