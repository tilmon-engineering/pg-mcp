# Execution evidence

Work in progress; this is not an acceptance/success report.

## Bootstrap observations

- Before source creation, built-in discovery including hidden/ignored paths found only `0b86n2-smell/` session artifacts. No product source or Cargo files existed. Thus source workspace was empty, but the directory was not literally empty.
- Initial `git status --porcelain` failed: not a Git repository.
- Initialized local Git with `git init -b main` and added origin.
- `git branch --show-current`: `main`.
- `git remote get-url origin`: `git@github.com:tilmon-engineering/pg-mcp.git`.
- Immediately after bootstrap `git status --porcelain`: `?? .gitignore`, `?? mise.toml`.
- Executor did not fetch, push, create an empty commit, or change Git identity. These observations are not proof of all historical network actions.

## Native/tool observations

- `mise --version`: 2026.7.16 linux-x64.
- `rustc --version`: rustc 1.96.0 (ac68faa20 2026-05-25), Homebrew.
- `mise trust && mise install`: successful; tools already installed.
- Default `pkg-config --modversion libpq` failed because the package was not in the default search path.
- `brew list --versions libpq postgresql@17 postgresql@18`: libpq 18.6 and postgresql@17 17.11 installed.
- `PKG_CONFIG_PATH=/home/linuxbrew/.linuxbrew/opt/libpq/lib/pkgconfig pkg-config --modversion --cflags --libs libpq`: version 18.6.
- `podman info --format '{{.Host.OCIRuntime.Name}}'`: `crun`.
- Initial image inspection failed: postgres:17-bookworm and postgres:12 not cached. Both subsequent Podman pulls succeeded.
- Resolved `docker.io/library/postgres:17-bookworm`: `sha256:7bade6d532592ca8ce7ee32def7399dad2607c4ea5583839fc4352a095a11ea6`.
- Resolved `docker.io/library/postgres:12`: `sha256:4bf4eb8e5932534db5fb9d3d91a212a91406aecf1fa626a60df4a9e2781d73ae`.
- Separate pkg-config link probe emitted `-L/home/linuxbrew/.linuxbrew/opt/libpq/lib -lpq`; includes point to the same libpq prefix plus its native dependencies.

## Verification

- `cargo generate-lockfile`: succeeded; lockfile records rmcp 1.7.0, explicit rmcp-macros 1.7.0, pq-sys 0.7.5, and live-test support dependencies.
- `cargo fetch --locked`: succeeded.
- The library is dynamically linked. Running binaries/tests on this workstation requires `LD_LIBRARY_PATH=/home/linuxbrew/.linuxbrew/opt/libpq/lib` in addition to the pkg-config build path. Without it, the loader exits 127 before Rust main with `libpq.so.5` unavailable; this is the documented runtime-loader limitation, not an application startup result.
- Required default verification passed with `PKG_CONFIG_PATH=/home/linuxbrew/.linuxbrew/opt/libpq/lib/pkgconfig` and the loader path above:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace --locked` (all default tests passed; 10 explicitly live database tests correctly reported ignored)
  - `cargo build --workspace --locked`
- Explicit live verification passed serially to avoid contention among disposable containers/proxy fault fixtures:
  - `PG_MCP_LIVE=1 RUST_TEST_THREADS=1 cargo test --workspace --locked -- --ignored --test-threads=1`
  - 10/10 PostgreSQL live tests passed, covering PostgreSQL 17 and 12 fixtures, schema features, transaction status/SQLSTATE, parameters/result limits/DML, COPY disposal, same-target concurrency/cancellation, proxy disconnect uncertainty, commit uncertainty, and fixture cleanup. Two non-ignored proxy unit tests were filtered by `--ignored`, as expected.
- Actual Linux SIGINT subprocess verification passed in the default shutdown matrix. The binary uses an explicit Tokio runtime and calls `shutdown_background()` only after the unified rmcp/core cleanup future completes; this bounds process termination when Tokio 1.53's uncancellable stdin helper remains blocked after the host keeps stdin open. This is an OS-runtime limitation documented by Tokio; it does not claim the helper thread was joined.
- Post-remediation verification reran successfully with the same native environment after typed MCP failure propagation, exact result/schema budgeting, cancellation drain handling, final-status envelope publication, and shutdown tracking changes:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace --locked` (39 core unit tests, documentation contract, and proxy units passed; 10 live database tests reported ignored as designed)
  - `cargo build --workspace --locked`
  - `PG_MCP_LIVE=1 RUST_TEST_THREADS=1 cargo test --workspace --locked -- --ignored --test-threads=1` (10/10 live tests passed in the final rerun in 115.92s; two non-ignored proxy units were filtered as expected)
- This final rerun specifically validated that shutdown cancels pending opens, waits only until the aggregate deadline for their reservation cleanup, returns a frozen uncertainty report for stuck work, and waits for a verified close worker to finish without attempting to enqueue to its closed channel. It also validated envelopes are refreshed from the worker's final native status observation before reply publication.

The evidence above is tool-observed, not a claim about unexecuted deployment environments.

## Local development setup

- Added `compose.yaml` for a disposable PostgreSQL 17 service named `postgres-mcp-local`, bound only to `127.0.0.1:55432`, with a named data volume and readiness healthcheck.
- Added `config.local.toml`, `.env.example`, and mise tasks: `local-up`, `local-down`, `local-status`, `local-logs`, `local-mcp`, and `local-test`.
- Created the local ignored `.env` from the documented fixture credentials; its value is intentionally not recorded here.
- `podman-compose config` passed, `mise tasks` listed all local tasks, `.env` is git-ignored, and mise confirmed `PG_MCP_DSN` is set without exposing it.
- Started the session-local fixture and verified `/var/run/postgresql:5432: accepting connections`; a local query reported PostgreSQL 17.11.
- The named `local-postgres` service is intentionally left running for interactive testing. Stop it with `mise run local-down`.
