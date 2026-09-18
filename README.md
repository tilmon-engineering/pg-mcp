# PostgreSQL MCP

Rust stdio MCP server with explicit connection/transaction lifecycles and authoritative libpq transaction status. The nine tools are `open_database`, `list_handles`, `get_schema`, `open_read`, `open_write`, `query`, `commit`, `rollback`, and `close_database`. See [DESIGN.md](DESIGN.md) for protocol and cleanup rules.

## Build requirements

- Linux, Rust 1.96.0 (`mise install`).
- **System libpq >=17 development and runtime libraries**, including the nonblocking cancellation API, and pkg-config. PostgreSQL servers **>=12** are supported independently of the client version.
- Dynamic libpq's TLS and other native dependencies must be installed with that library. Preserve their distribution packaging and loader configuration.

Use `PKG_CONFIG_PATH`, `PKG_CONFIG_LIBDIR`, and `PKG_CONFIG_SYSROOT_DIR` (including platform-supported target forms) to select an installation. Discovery must succeed with `pkg-config --atleast-version=17 libpq`. No pg_config/vcpkg fallback is supported. Unset `PG_CONFIG`, generic/target-specific `PQ_LIB_DIR`, `PQ_LIB_STATIC`, `LIBPQ_STATIC`, and `PKG_CONFIG_ALL_STATIC`. pq-sys gives target-specific overrides precedence; allowing them could link a different library from the one this project validates. Bundled/static builds and contradictory pkg-config metadata are unsupported.

The deployed loader must resolve the compatible dynamic library. An older library can fail at process load before Rust startup; a friendly application error/exit 2 cannot be promised in that case. Startup also checks PQlibVersion.

```sh
mise trust
mise install
cargo build --workspace --locked
```

## Local PostgreSQL fixture

The repository includes a disposable PostgreSQL 17 fixture in `compose.yaml`. It
binds only to `127.0.0.1:55432`, uses the non-secret development credentials
shown in `.env.example`, and stores data in the named volume
`postgres-mcp-local-data`. Copy the example environment file before using
`mise` tasks:

```sh
cp .env.example .env
# Set this when libpq is installed outside the default pkg-config path.
export PKG_CONFIG_PATH=/path/to/libpq/lib/pkgconfig
mise run local-up
mise run local-status
mise run local-mcp
```

`local-mcp` starts the server with `config.local.toml`; use an absolute config
path as required by the server. Stop the fixture with `mise run local-down`.
Use `mise run local-logs` to follow PostgreSQL logs. To launch the server through
the Polytoken harness, start Polytoken from this repository (or pass
`--working-dir` here); `.polytoken/config.toml` registers `postgres` as a
repo-local stdio MCP server and passes the mise-loaded `PG_MCP_DSN` into the
child:

```sh
polytoken --working-dir "$PWD" --config-dir "$PWD" new
```

The harness entry runs `run-local-mcp.sh`, which uses the local config and
connects to the Compose PostgreSQL fixture at `127.0.0.1:55432`. The local
profile supplies this DSN through the config command; older flat configurations
may still use `PG_MCP_DSN`. Use
`mise run local-logs` to follow PostgreSQL logs. `mise run local-test` starts
the fixture and runs the existing opt-in live acceptance suite; those tests
create their own isolated disposable PostgreSQL containers and do not use the
Compose database. The named volume can be removed manually when a clean local
database is wanted.

## Configuration and launch

Copy `config.example.toml` to an absolute operator-owned path. Configuration rejects unknown fields and out-of-range values. Put shared limits in `[defaults]` and define named `[profiles.<name>]` entries with a `dsn` command. The command runs as `bash -c` at `open_database` time, inherits the server environment, and its stdout is passed to libpq; this supports `$VAR` and `$(...)` secrets-manager integrations without storing credentials in TOML. Never log or commit credentials. Each profile DSN must name one explicit nonempty database; service indirection and nested connection strings in dbname are not accepted.

Use libpq's native `sslmode`, certificate and authentication options appropriate to your deployment (for remote TLS, ordinarily `sslmode=verify-full` plus trusted roots). This server does not weaken TLS defaults or replace libpq authentication. No tool accepts a DSN or arbitrary connection options.

```sh
postgres-mcp --config /absolute/path/config.toml
postgres-mcp --help
postgres-mcp --version
```

stdout is exclusively MCP protocol. Errors and notices never print raw backend diagnostics, SQL, parameters, credentials or result rows. Exit 0 means normal EOF/SIGINT and verified cleanup; exit 1 means serve failure or cleanup uncertainty; exit 2 means startup/config failure (subject to loader limitations above).

## Workflow

1. `open_database({"profile":"production"})` → retain returned handle.
2. `get_schema({"handle":"..."})` → inspect role-visible metadata.
3. `open_read({"handle":"..."})` or `open_write({"handle":"..."})`.
4. `query({"handle":"...","sql":"SELECT $1::text","parameters":[{"type_oid":25,"value":"hello"}]})`.
5. `commit` or `rollback`, then `close_database`. After ending a user transaction, inspect schema again before opening another.

`query` uses PostgreSQL's single-statement extended protocol, not an application SQL parser. Normal tables with defaults, identities, foreign keys, triggers and RLS are supported. The role and schema are operator trusted. Native PostgreSQL read-only mode is **not a general side-effect sandbox**. COPY streaming is unsupported and disposes the connection with an unknown outcome.

## Values and envelopes

Parameters are text-only `{ "type_oid": unsigned_integer_or_null, "value": string_or_null }`. OID null/0 requests server inference; null value means SQL NULL. PostgreSQL validates numeric, UUID, JSON, bytea and temporal text syntax. Embedded NUL is rejected. Explicit OIDs permit otherwise ambiguous standalone parameters.

Query results contain `columns: [{name,type_oid,format:"text"}]`, `rows: [[string|null]]`, `command_tag`, and nullable `affected_rows` decimal text. Exact UTF8 server text is preserved rather than converted to JSON numbers or canonical timestamps. Session formatting can differ.

Envelope version 1 contains required `handle_state`, `next_moves` and exactly one `result` or `error`. Error fields are `code`, static `message`, nullable `sqlstate`, and nullable `transaction_outcome`. A five-character SQLSTATE is diagnostic only. `transaction_status` comes from PQtransactionStatus; `transaction_open` is null for unknown/active/busy, never false-as-unknown. Structured and text content contain the same JSON object; text is canonical compact JSON with recursively byte-sorted object keys. Suggested query arguments are partial templates, not replayable commands.

## Limits and uncertainty

Config caps bound handles, queue admission, SQL, parameter count/aggregate UTF8 bytes, retained rows/columns/cells and **complete canonical result-subobject JSON bytes**, including escaped text and metadata. Schema budgets are aggregate over the whole response. Overflow returns only an error, not truncated success. These are application retention limits, not a bound on libpq's internal row/message allocation or rmcp/serde's incoming-frame allocation.

Limit and cancellation errors do not imply DML rollback. Roll back if you do not want the transaction's effects. Cancellation dispatch success is not proof execution stopped; the worker drains and observes status or disposes broken/unknown. Commit transport loss cannot establish whether the server committed. Reconcile with the operator; **never automatically replay a commit**.

Normal socket waits and cleanup are deadline-controlled. DNS/platform C calls can exceed those bounds. Core cannot safely kill an OS thread; the binary exits nonzero on aggregate shutdown uncertainty rather than claiming the thread joined.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked
PG_MCP_LIVE=1 cargo test --workspace --locked -- --ignored
```

Schema observation is preserved across verified row-only commits and rollbacks. A verified commit after a successfully observed direct DDL requires `get_schema` again. Command-tag tracking intentionally cannot detect catalog changes hidden behind functions, triggers, rules, procedures, dynamic SQL, or SELECT-tagged `CREATE TABLE AS`/`SELECT INTO`; this is an accepted limitation.

## Native releases

The release page at `https://github.com/tilmon-engineering/pg-mcp/releases` publishes exactly four executable-only archives named for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `aarch64-apple-darwin`, and `x86_64-apple-darwin`, plus `SHA256SUMS`. Verify the checksum and archive membership before extraction. Archives are dynamically linked, not standalone: macOS requires Homebrew `libpq@17`, and macOS artifacts are unsigned/unnotarized. Linux uses the Ubuntu 24.04 GNU/glibc baseline; older platform compatibility is not promised.

Run `mise run ci`, `mise run release-check v0.1.0`, `mise run release-build`, `mise run native-smoke`, and `mise run workflow-check` for local validation. Before `xtask publish`, the read-only origin preflight must report exactly `https://github.com/tilmon-engineering/pg-mcp.git`; an SSH or other origin is a blocker and is never rewritten automatically. Release tags are immutable annotated `v{workspace version}` tags, created only after branch CI passes.

Live tests must create disposable Docker/Podman containers on dynamic loopback ports with generated fixture credentials; no operator database URLs. They are ignored by default: ignored is **not tested**, not compatibility evidence. Explicit opt-in must fail when infrastructure is unavailable. Main fixture: `postgres:17-bookworm`; oldest-server fixture: `postgres:12` (isolated EOL compatibility testing only). Record resolved image digests with verification. Test-only proxy/fault harnesses do not create production fault configuration.
