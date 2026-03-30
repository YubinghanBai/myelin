## Summary

What does this PR change and why?

## Type

- [ ] Bug fix
- [ ] Feature / behavior change
- [ ] Docs / tooling only

## Testing

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo test`
- [ ] CI **E2E** workflow (slim JetStream + SIGTERM) green — or note why skipped
- [ ] Local E2E full (`USE_NATS=1 ./scripts/e2e_local.sh` with Phase 3/6) — if you touch replication / publish / e2e script

## Semantics / replication

If this PR touches LSN feedback, JetStream publish order, or oversized payload handling:

- [ ] Behavior matches or updates `PLAN.md` / `src/pg/stream.rs` / `src/pg/publish.rs` comments.
- [ ] No silent data loss on default `stall` policy unless explicitly documented.

## Checklist

- [ ] No secrets or internal URLs committed
- [ ] README / `SECURITY.md` updated if behavior or threat model changes
