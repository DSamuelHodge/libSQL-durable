---
title: "Changelog"
description: "Release history for libsql-durable."
---

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-07-21

PVM collapse finish line + richer definitions IR + promote (A/B) + CoW-aware fork copy.

### Added

- **`pvm` host binary** — open a world, ops (`health`/`ps`/`trace`/`heal`/…), stock `Echo`, `def-put`/`interpret`/`promote`
- **Process definitions** — immutable versions, pins, resolve helpers, `pvm.def.v1` validation
- **Interpreter** — `pvm.interpret` with ops: `activity`, `timer`, `wait`, `set_kv`, `set_status`, `goto`, `return`, **`if`**, **`select`**
- **Fork explore** — `ForkOptions::explore` / `time_travel` / `explore_instance`, `fork_and_open`, `discard_world_package`
- **Promote policy A** — `promote_world_package` (file replace, confirm + lineage + backup + audit)
- **Promote policy B** — `selective_promote_instances` (import instance history/KV/pins into live parent)
- **CoW package copy** — `copy_world_package` tries reflink/`cp -c` / `cp --reflink=auto`, falls back to full copy
- **Docs** — RUNTIME, SYSCALLS, DEFINITIONS, FORK, COLLAPSE_PR_PLAN; COOKBOOK recipes 11–13
- **Tests** — `collapse_quality`, `interpreter`, expanded horizon coverage

### Changed

- `SCHEMA_VERSION` / `WORLD_FORMAT_VERSION` remain **2** (additive tables created on demand)
- Definition put is **immutable** by default (`put_process_definition_ex(..., force)`)

### Notes

- Features `compat-sqlite` and `native-libsql` remain mutually exclusive
- Remote/replica/cluster tests skip when infrastructure is not running

## [0.1.0] — prior

Initial native libSQL Duroxide provider, world packaging (Phase 1), introspection (2), healing (3), and horizon substrate (4–7).
