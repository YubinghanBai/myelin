# Testing

## Local E2E script

`scripts/e2e_local.sh` builds myelin, uses `docker compose` for Postgres (and optionally NATS), and walks scripted phases. Open the script header for phase descriptions and environment toggles.

### Common invocations

| Command | What it checks |
|---------|----------------|
| `./scripts/e2e_local.sh` | Postgres path without JetStream-focused phases. |
| `USE_NATS=1 ./scripts/e2e_local.sh` | JetStream path, **Phase 3** + **Phase 6** (`E2E_BULK_ROWS`, default `2000`). |
| `USE_NATS=1 E2E_PHASE3=0 E2E_PHASE6=0 E2E_SIGTERM=1 ./scripts/e2e_local.sh` | Short smoke + **SIGTERM** → exit `0` and `graceful shutdown` in logs. |
| `USE_NATS=1 E2E_NATS_FAULT=1 E2E_SIGTERM=0 ./scripts/e2e_local.sh` | After Phase 2/3/6 (defaults), NATS is stopped → myelin exits non-zero after retries (`E2E_SIGTERM=0` so that step is not masked). |

**Phase 3** (`USE_NATS=1`, skippable with `E2E_PHASE3=0`): oversized **`stall`** path, replay with higher `MYELIN_MAX_PAYLOAD_BYTES`, **`dead_letter`** / DLQ behavior.

**Phase 6** (skippable with `E2E_PHASE6=0`): bulk INSERT → kill connector → restart → each bulk `correlation_id` appears in logs (duplicates allowed).

### CI

[`.github/workflows/e2e.yml`](./.github/workflows/e2e.yml) runs the short recipe on pushes/PRs to `main`/`master`: `USE_NATS=1`, Phase 3/6 off, `E2E_SIGTERM=1`. No NATS-fault step in CI.

[`.github/workflows/ci.yml`](./.github/workflows/ci.yml) runs `fmt`, `clippy`, `test`, and `cargo build --benches`.

### Pinning dependency images

Compose defaults: `postgres:16-alpine`, `nats:2-alpine`. For repeatable runs, pin patch tags after `docker pull` and record digests if you care about bitwise-identical images ([postgres](https://hub.docker.com/_/postgres), [nats](https://hub.docker.com/_/nats)).

### Reference machine snapshot (full local E2E — not a throughput benchmark)

**Rust 1.94.0.** One timed run of the **entire** script (`USE_NATS=1`, `E2E_BULK_ROWS=2000`, `E2E_SIGTERM=0`, `E2E_NATS_FAULT=0`): wall clock includes compose, setup, optional cold build, and Phases 2+3+6 — not an isolated replication hot-path measurement.

- **When / where:** 2026-03-30 · Darwin 25.4 arm64 · Docker 28.5.2 (OrbStack, client + engine)
- **Command:**

  ```bash
  /usr/bin/time -p env USE_NATS=1 E2E_BULK_ROWS=2000 E2E_SIGTERM=0 E2E_NATS_FAULT=0 ./scripts/e2e_local.sh
  ```

- **`time -p` (seconds):** real 90.96 · user 12.99 · sys 3.85
- **Image digests at pull time:**

  ```
  postgres@sha256:20edbde7749f822887a1a022ad526fde0a47d6b2be9a8364433605cf65099416
  nats@sha256:1cfc36e2e5e638243d8c722f72c954cd0ec4b15ee82fadbc718ce12e2b3c1652
  ```

Update this block when you change machine, Docker, or images on purpose.

### Optional micro-benchmark

Envelope JSON serialization only (local Criterion run; numbers are not tracked in the README):

```bash
cargo bench --bench envelope_json
```

### Spot-check note

`USE_NATS=1 E2E_PHASE3=0 E2E_PHASE6=0 E2E_SIGTERM=1 E2E_NATS_FAULT=1 ./scripts/e2e_local.sh`: graceful SIGTERM path OK; forced NATS loss → non-zero exit after retries; script restarts NATS.
