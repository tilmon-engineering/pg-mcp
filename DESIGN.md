# PostgreSQL MCP design (envelope version 1)

## Authority and trust

The TOML configuration contains shared `[defaults]` limits and named `[profiles.<name>]` entries. Each profile's `dsn` is evaluated by `bash -c` at `open_database` time, allowing inherited environment variables and subshells for external secret managers; stdout is the only DSN input and is never logged. The selected profile's DSN must name one explicit literal database. Service indirection and nested dbname expansion are rejected. Native libpq implements authentication and TLS without weaker replacement defaults. Setup enforces UTF8 and verifies the actual database and PostgreSQL server >=12. Client libpq >=17 is a separate requirement.

One OS worker exclusively owns each PGconn. All unsafe FFI stays in the private adapter. Connections, results and cancellation objects have RAII disposal. Nonlogging notice callbacks are installed before connect polling. Never publish raw backend diagnostics, DSNs, SQL or parameters.

The configured role and schema are operator trusted. There is no SQL keyword allowlist, exhaustive sandbox, eligibility exclusion, automatic savepoint, SQL rewriting or automatic retry. PostgreSQL native read-only transactions retain PostgreSQL-defined function and side-effect semantics.

## Nine tools

| Tool | Input | Contract |
|---|---|---|
| open_database | profile | Evaluate the named profile DSN and allocate an idle, unobserved handle. |
| list_handles | none | Sorted cached worker observations, not a concurrent PGconn inspection. |
| get_schema | handle | Idle-only bounded role-visible metadata; arms schema observation only after complete successful snapshot finalization. |
| open_read | handle | Schema observed and IDLE; BEGIN READ ONLY at server default isolation. |
| open_write | handle | Schema observed and IDLE; BEGIN READ WRITE at server default isolation. |
| query | handle, sql, parameters=[] | Schema observed and INTRANS; one extended-protocol statement, text parameters and results. |
| commit | handle | INTRANS only; successful COMMIT command tag plus final IDLE is the success oracle. |
| rollback | handle | INTRANS/INERROR rollback; verified IDLE is an idempotent no-op. |
| close_database | handle | FIFO worker decision; active transaction refused, idle/broken disposed locally. |

Internal schema retrieval uses `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`. It exposes schemas, supported relations, visible columns and constraints, not function/trigger/policy bodies or comments. Metadata expressions are untrusted data. Normal defaults, identity, foreign keys, triggers and RLS do not exclude a table. Shared row/cell/column/JSON caps apply across the entire response. No schema freshness/fingerprint promise exists. Every user transaction-ending IDLE path clears schema observation; successful internal schema finalization is the exception.

## Status and guidance

Schema observation is preserved across verified row-only commits and rollbacks. A verified commit after successfully observed direct catalog DDL invalidates the observation and requires `get_schema` before another transaction. This uses PostgreSQL result command tags, so indirect catalog changes through functions, triggers, rules, procedures, dynamic SQL, and SELECT-tagged `CREATE TABLE AS`/`SELECT INTO` are accepted blind spots; command-tag tracking is not a universal freshness guarantee.

`PQstatus` and `PQtransactionStatus` are authoritative after drain. SQLSTATE is diagnostic, never a transaction-state inference. Every `HandleState` carries `handle`, `database`, `connection_status`, `transaction_status`, `transaction_open`, `busy`, and `schema_observed`. `transaction_open` is false for IDLE, true for INTRANS/INERROR and null for ACTIVE/UNKNOWN (also null while busy). A closed snapshot is unknown, not a fabricated post-PQfinish libpq observation.

Move priority: stopping/unresolved/list/closed → none; bad/unknown → local close with outcome warning; busy/ACTIVE → none; INERROR → rollback; active-close refusal → commit/rollback; INTRANS → query/commit/rollback; idle unobserved → get_schema/close_database; idle observed → open_read/open_write/get_schema/close_database. Query moves are partial argument templates, never automatic replay instructions.

An envelope has exactly `envelope_version`, `handle_state`, `next_moves` and one of `result`/`error`. Nullable keys are retained. MCP isError reflects errors. Structured content equals the JSON object in text content. Canonical text recursively orders object keys by UTF8 bytes, preserves array order and uses compact serde_json UTF8 encoding.

Parameters are `{ "type_oid": null, "value": null }` or text values with optional unsigned OIDs. Null/0 OID requests inference; null value is SQL NULL. Embedded NUL is rejected. PostgreSQL converts its own text syntax. Results have `columns` (name/type_oid/format=text), `rows` (strings/null), `command_tag` and nullable decimal-text `affected_rows`. No numeric, temporal or unknown-type conversion is guessed.

## Execution and cleanup

Bounded channels and oneshot replies bridge async callers to workers. Registry admission, pending-open reservations and Running→Stopping→Stopped transitions share a short mutex; no I/O occurs under it. Workers publish final observations before dequeuing the next command even if a caller drops its reply. Close is an ordinary FIFO command.

Connect and asynchronous UTF8 setup share one deadline. Query dispatch uses PQsendQueryParams and requires PQsetSingleRowMode=1. Drive flush (including inbound traffic), consume, busy and results until NULL. COPY streaming is unsupported: dispose and return COPY_UNSUPPORTED with unknown outcome. Never try to ordinary-drain COPY indefinitely.

Application retention budgets include all canonical result JSON keys, metadata, escaping and framing. Overflow discards retained results and drains/cancels or disposes; it does not undo DML. No partial result is returned. libpq may allocate a row/message before checks: these are not native-memory bounds. Incoming rmcp/serde frames may be allocated before application validation.

Before send and at polling boundaries observe shutdown, client cancellation, then deadline in that priority. Unsent cancellation executes no SQL. Sent cancellation uses PQcancelCreate/Start/Poll/Socket/Finish on the owner thread, refreshing the socket after each poll. Cancellation dispatch is not completion: drain the original connection and observe status. Grace expiry disposes to broken/unknown. Final NULL plus status observation is the completion point; later cancellation does not rewrite it. Dispatched commit/rollback are not client-cancellable. Lost commit confirmation is COMMIT_OUTCOME_UNKNOWN and must never be retried automatically.

Shutdown rejects admission first, signals all registered workers including pending opens, rejects unstarted commands, and attempts rollback only on usable INTRANS/INERROR before disposal. One aggregate deadline bounds waiting. A frozen sorted verified/uncertain report is reused by repeated shutdown calls. Late completion cannot rewrite it. EOF, SIGINT and serve errors use one cleanup owner. Exit codes: normal EOF/SIGINT plus verified cleanup 0; serve failure or uncertain cleanup 1; startup/config failure 2. stdout remains protocol-only.

Socket polling uses at most 25ms slices. DNS or platform C calls may exceed socket deadlines; Rust cannot safely kill a stuck OS thread. The binary must terminate nonzero on deadline uncertainty, not claim that such a worker joined.

## Native distribution and verification

Initial distribution is Linux plus dynamically linked system libpq >=17 discovered exclusively through pkg-config. The build verifies cancellation symbols and refuses bundled/static and PQ_LIB_DIR/PG_CONFIG alternate selection. Runtime PQlibVersion is defense in depth; an incompatible dynamic loader may fail before main, so graceful exit 2 is not guaranteed for broken packaging.

Default unit tests require no database. Live tests are explicitly ignored and use disposable local PostgreSQL 17 and 12 fixtures.

## Native release contract

Release archives target only `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `aarch64-apple-darwin`, and `x86_64-apple-darwin`. They contain only the `postgres-mcp` executable and are dynamically linked rather than standalone. Runtime libpq `>=17` is discovered through pkg-config; macOS consumers must install Homebrew `libpq@17`. Archives are checksummed with `SHA256SUMS`; macOS builds are unsigned and unnotarized. Local release validation is `mise run ci`, `mise run release-check v0.1.0`, and `mise run workflow-check`. Publication requires the exact HTTPS origin `https://github.com/tilmon-engineering/pg-mcp.git`, an immutable annotated tag, and successful branch CI. PostgreSQL 12 is isolated compatibility evidence, not deployment advice for an EOL release. Test-only fake backends and transparent proxy barriers exercise lifecycle races and uncertainty without production fault switches.
