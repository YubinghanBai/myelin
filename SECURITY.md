# Security

## Supported versions

Security fixes are applied to the latest **minor** release on the default branch. There are no long-term support lines yet.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security-sensitive reports.

1. Prefer GitHub [**private vulnerability reporting**](https://github.com/YubinghanBai/myelin/security/advisories/new) on [**YubinghanBai/myelin**](https://github.com/YubinghanBai/myelin) (enable *Settings → Security → Private vulnerability reporting* on the repo if it is off).
2. If that is unavailable, contact the maintainer via GitHub **@YubinghanBai** (no secrets in messages).
3. Include: affected version or commit, reproduction steps, and impact assessment if known.

We aim to acknowledge within a few business days and coordinate disclosure after a fix is available.

## Scope

In scope for security review:

- This repository (`myelin` binary and library): logical replication handling, JetStream publish path, configuration parsing, and the optional Prometheus scrape endpoint when `MYELIN_METRICS_ADDR` is set.
- Out of scope: PostgreSQL or NATS server security (follow upstream advisories); misuse of replication credentials or publication scope in your deployment.

## Hardening notes

- Run with **least privilege**: dedicated replication user; publication limited to required tables.
- The metrics HTTP listener is **plaintext HTTP** by intention (scrape endpoint). Bind to loopback (`127.0.0.1`) or protect with network policy; do not expose untrusted interfaces without TLS termination in front.
