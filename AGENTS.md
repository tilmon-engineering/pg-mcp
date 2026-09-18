# PostgreSQL MCP

Implement the libpq-backed design in DESIGN.md. Keep PGconn exclusively on its owning OS worker; PQtransactionStatus is authoritative, not SQLSTATE-derived application state. Unsafe code belongs only in the private libpq adapter.

Never log DSNs, credentials, SQL, parameter values, rows, or raw backend diagnostics. stdout is exclusively MCP protocol. PostgreSQL roles and schema definitions are trusted; native read-only transactions are not a side-effect sandbox. No automatic SQL retry.

Use Rust 1.96.0 through mise. Required checks:
- cargo fmt --all -- --check
- cargo clippy --workspace --all-targets -- -D warnings
- cargo test --workspace --locked
- cargo build --workspace --locked
- PG_MCP_LIVE=1 cargo test --workspace --locked -- --ignored

Live tests must create disposable local containers; never use operator database URLs. Report ignored tests as not run. Keep Cargo.lock checked in. Dynamic system libpq >=17, discoverable through pkg-config, is required; do not silently fall back to pg_config, static or bundled linking.

Release contract: supported native targets are `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `aarch64-apple-darwin`, and `x86_64-apple-darwin`. Archives are dynamic and not standalone; macOS requires Homebrew `libpq@17`, and macOS artifacts are unsigned/unnotarized. Verify `SHA256SUMS` and archive membership. Use `mise run ci`, `mise run release-check v0.1.0`, `mise run release-build`, `mise run native-smoke`, and `mise run workflow-check`. Before publication, require the exact HTTPS origin `https://github.com/tilmon-engineering/pg-mcp.git`; report SSH/wrong origins as blockers without rewriting them. Use the immutable annotated-tag/two-commit sequence and never publish before branch CI.
