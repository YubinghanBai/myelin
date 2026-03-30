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
- [ ] Local E2E (`./scripts/e2e_local.sh` and/or `USE_NATS=1 ...`) — if applicable

## Semantics / replication

If this PR touches LSN feedback, JetStream publish order, or oversized payload handling:

- [ ] Behavior matches or updates `PLAN.md` / `src/pg/stream.rs` / `src/pg/publish.rs` comments.
- [ ] No silent data loss on default `stall` policy unless explicitly documented.

## Checklist

- [ ] No secrets or internal URLs committed
- [ ] README / `SECURITY.md` updated if behavior or threat model changes
