---
name: release-postgres-mcp
description: Release postgres-mcp through immutable annotated tags and verified dynamic-libpq native archives.
---

# Release postgres-mcp

This is an authorized-release runbook. Never push, create, move, or publish a tag/release without explicit authorization.

## Contract

The workspace version is authoritative and product crates use `version.workspace = true`. The tag is exactly `v{version}`. Publish exactly four executable-only archives and `SHA256SUMS`: `postgres-mcp-{x86_64-unknown-linux-gnu,aarch64-unknown-linux-gnu,aarch64-apple-darwin,x86_64-apple-darwin}.tar.gz`.

Archives are dynamically linked, not standalone. Linux uses Ubuntu 24.04 GNU/glibc. macOS requires Homebrew `libpq@17`; binaries are unsigned/unnotarized and older-platform compatibility is not promised. libpq is discovered only through pkg-config; static, bundled, and pg_config selectors are unsupported.

## First release and history

This repository may begin with no commits or tags. Record that root-history/empty-HEAD decision. Use two commits: implementation/version/lockfile plus provisional notes, then inspect the resulting history and make a separate final notes-only commit. Test the final SHA, push `main`, wait for all four native branch jobs, then create and push one immutable annotated tag.

## Checks

```sh
mise run ci
cargo run --locked -p xtask -- release-check v0.1.0
cargo run --locked -p xtask -- release-notes v0.1.0 release-notes.md
mise run workflow-check
```

Before `xtask publish`, a read-only origin preflight must report exactly `https://github.com/tilmon-engineering/pg-mcp.git` (an SSH or other origin is a blocker; never rewrite it automatically). Draft releases may be resumed only when tag, target SHA, notes, prerelease state, exact five assets, and all digests match. Published releases and mismatches require a new version.

Never expose `GH_TOKEN`, DSNs, or credentials. Public verification downloads anonymously over HTTPS and verifies checksums and archive membership before execution.
