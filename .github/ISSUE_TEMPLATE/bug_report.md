---
name: Bug report
about: Something is wrong with myelin behavior or docs
title: ''
labels: bug
assignees: ''
---

## Environment

- myelin version / commit:
- OS:
- PostgreSQL version:
- `wal_level` and replication setup (brief):
- NATS / JetStream (if used): version and stream config summary

## What happened

## What you expected

## How to reproduce

1.
2.

## Logs / metrics (redact secrets)

Paste relevant `tracing` lines or Prometheus samples. Remove passwords and connection strings.

## Notes

Optional: if this is about semantics (LSN, at-least-once, oversized payload), point to the relevant `stream.rs` / `publish.rs` behavior or [`TESTING.md`](https://github.com/YubinghanBai/myelin/blob/main/TESTING.md).
