//! Ignored, explicit-opt-in acceptance tests against disposable PostgreSQL.
//!
//! These tests intentionally drive the public [`Core`] API rather than a
//! separate SQL client. The fixture is created per test and is torn down by its
//! `Drop` guard. Run with:
//!
//! ```text
//! PG_MCP_LIVE=1 cargo test -p postgres-mcp-core --test live -- --ignored
//! ```
//!
//! An ignored test is not evidence of compatibility. If explicitly enabled,
//! missing Podman/image/runtime prerequisites fail the test with a clear panic.

#[path = "support/postgres.rs"]
mod postgres;

#[path = "../../postgres-mcp/tests/support/proxy.rs"]
mod proxy;

use postgres::PostgresFixture;
use postgres_mcp_core::config::{Config, ConfigFile};
use postgres_mcp_core::protocol::{Envelope, Parameter};
use postgres_mcp_core::worker::{Core, CoreFailure};
use proxy::{Direction, TransparentProxy};
use serde_json::Value;
use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn require_live() {
    assert_eq!(
        std::env::var("PG_MCP_LIVE").as_deref(),
        Ok("1"),
        "live test explicitly requires PG_MCP_LIVE=1; rerun with PG_MCP_LIVE=1"
    );
}

fn fixture(version: u16) -> PostgresFixture {
    require_live();
    PostgresFixture::start(version).unwrap_or_else(|error| {
        panic!("PostgreSQL {version} disposable fixture unavailable: {error}")
    })
}

fn config() -> Config {
    Config {
        request_timeout_seconds: 30,
        connection_timeout_seconds: 30,
        cancel_drain_grace_seconds: 5,
        shutdown_total_seconds: 10,
        ..Config::default()
    }
}

fn core(fixture: &PostgresFixture) -> Core {
    Core::new(config(), fixture.dsn()).unwrap_or_else(|_| {
        panic!("Core could not initialize against the disposable PostgreSQL fixture")
    })
}

fn profile_core(fixture: &PostgresFixture) -> Core {
    let source = format!(
        "[profiles.local]\ndsn=\"printf '%s' '{}'\"\n",
        fixture.dsn()
    );
    let config = ConfigFile::from_toml(&source).expect("profile configuration");
    Core::new_with_profiles(config).expect("Core with profile configuration")
}

async fn call<T>(future: T) -> Value
where
    T: Future<Output = Result<Envelope, CoreFailure>>,
{
    let envelope = future.await.unwrap_or_else(|error| {
        panic!("Core operation failed before returning an envelope: {error:?}")
    });
    serde_json::to_value(envelope).expect("Core envelope is JSON serializable")
}

fn handle(value: &Value) -> String {
    value["result"]["handle"]
        .as_str()
        .or_else(|| value["handle_state"]["handle"].as_str())
        .expect("successful open response carries a handle")
        .to_owned()
}

fn state(value: &Value) -> &Value {
    &value["handle_state"]
}

fn assert_status(value: &Value, status: &str, open: Option<bool>, observed: Option<bool>) {
    assert_eq!(state(value)["transaction_status"], status);
    assert_eq!(state(value)["transaction_open"].as_bool(), open);
    if let Some(observed) = observed {
        assert_eq!(state(value)["schema_observed"], observed);
    }
}

fn error_code(value: &Value) -> &str {
    value["error"]["code"]
        .as_str()
        .unwrap_or_else(|| panic!("expected error envelope, got {value}"))
}

async fn open_and_schema(core: &Core, database: &str) -> String {
    let opened = call(core.open_database(database, CancellationToken::new())).await;
    assert_status(&opened, "idle", Some(false), Some(false));
    let handle = handle(&opened);
    let schema = call(core.get_schema(&handle, CancellationToken::new())).await;
    assert_status(&schema, "idle", Some(false), Some(true));
    assert!(schema["result"]["schemas"].is_array());
    handle
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 17"]
async fn profile_name_is_independent_from_dsn_database() {
    let fixture = fixture(17);
    let core = profile_core(&fixture);
    let opened = call(core.open_database("local", CancellationToken::new())).await;
    assert_status(&opened, "idle", Some(false), Some(false));
    assert_eq!(opened["handle_state"]["database"], fixture.database);

    let handle = handle(&opened);
    let schema = call(core.get_schema(&handle, CancellationToken::new())).await;
    assert_status(&schema, "idle", Some(false), Some(true));
    assert_eq!(schema["handle_state"]["database"], fixture.database);
    assert!(schema["result"]["schemas"].is_array());

    close(&core, &handle).await;
    assert!(core.shutdown().await.verified());
}

async fn close(core: &Core, handle: &str) {
    let closed = call(core.close_database(handle, CancellationToken::new())).await;
    assert_eq!(closed["result"]["disposed"], true);
    assert_eq!(closed["result"]["cleanup_verified"], true);
    assert_eq!(closed["handle_state"]["connection_status"], "closed");
    assert!(closed["next_moves"].as_array().expect("moves").is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 17"]
async fn live_transaction_status_lifecycle() {
    let fixture = fixture(17);
    let core = core(&fixture);
    let handle = open_and_schema(&core, &fixture.database).await;

    let read = call(core.open_read(&handle, CancellationToken::new())).await;
    assert_eq!(read["result"]["opened"], "read");
    assert_status(&read, "in_transaction", Some(true), Some(true));

    let read_only_error = call(core.query(
        &handle,
        "CREATE TABLE live_read_only_rejected(id integer)".to_owned(),
        Vec::new(),
        CancellationToken::new(),
    ))
    .await;
    assert_eq!(error_code(&read_only_error), "DATABASE_ERROR");
    assert_status(&read_only_error, "in_error", Some(true), Some(true));
    assert_eq!(read_only_error["error"]["sqlstate"], "25006");

    let rolled_back = call(core.rollback(&handle, CancellationToken::new())).await;
    assert_eq!(rolled_back["result"]["rolled_back"], true);
    assert_status(&rolled_back, "idle", Some(false), Some(true));

    let write_after_rollback = call(core.open_write(&handle, CancellationToken::new())).await;
    assert_eq!(write_after_rollback["result"]["opened"], "write");
    let rollback_again = call(core.rollback(&handle, CancellationToken::new())).await;
    assert_eq!(rollback_again["result"]["rolled_back"], true);

    let write = call(core.open_write(&handle, CancellationToken::new())).await;
    assert_eq!(write["result"]["opened"], "write");
    let created = call(core.query(
        &handle,
        "CREATE TABLE lifecycle_probe(id integer PRIMARY KEY, note text)".to_owned(),
        Vec::new(),
        CancellationToken::new(),
    ))
    .await;
    assert_eq!(created["result"]["command_tag"], "CREATE TABLE");
    assert_status(&created, "in_transaction", Some(true), Some(true));

    let committed = call(core.commit(&handle, CancellationToken::new())).await;
    assert_eq!(committed["result"]["committed"], true);
    assert_status(&committed, "idle", Some(false), Some(false));
    let schema = call(core.get_schema(&handle, CancellationToken::new())).await;
    assert_status(&schema, "idle", Some(false), Some(true));
    close(&core, &handle).await;
    assert!(core.shutdown().await.verified());
}

async fn schema_features(version: u16) {
    let fixture = fixture(version);
    fixture
        .exec_sql(
            "CREATE TABLE public.parent_features (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, code text NOT NULL UNIQUE); CREATE TABLE public.child_features (id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, parent_id integer NOT NULL REFERENCES public.parent_features(id), note text DEFAULT 'normal', CONSTRAINT child_note CHECK (length(note) > 0)); CREATE VIEW public.child_features_view AS SELECT id, parent_id FROM public.child_features; ALTER TABLE public.child_features ENABLE ROW LEVEL SECURITY; CREATE FUNCTION public.touch_child() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; CREATE TRIGGER child_features_touch BEFORE INSERT ON public.child_features FOR EACH ROW EXECUTE FUNCTION public.touch_child();",
        )
        .expect("feature setup SQL");
    let core = core(&fixture);
    let handle = open_and_schema(&core, &fixture.database).await;
    let schema = call(core.get_schema(&handle, CancellationToken::new())).await;
    let relations = schema["result"]["schemas"]
        .as_array()
        .and_then(|schemas| schemas.iter().find(|schema| schema["name"] == "public"))
        .and_then(|schema| schema["relations"].as_array())
        .expect("public schema relation metadata");
    for name in ["parent_features", "child_features", "child_features_view"] {
        assert!(
            relations.iter().any(|relation| relation["name"] == name),
            "schema omitted {name}"
        );
    }
    let child = relations
        .iter()
        .find(|relation| relation["name"] == "child_features")
        .expect("child relation");
    assert!(
        child["columns"]
            .as_array()
            .unwrap()
            .iter()
            .any(|column| column["name"] == "note" && column["default_expression"].is_string())
    );
    assert!(
        child["constraints"]
            .as_array()
            .unwrap()
            .iter()
            .any(|constraint| constraint["kind"] == "foreign_key")
    );
    assert!(
        child["constraints"]
            .as_array()
            .unwrap()
            .iter()
            .any(|constraint| constraint["kind"] == "check")
    );
    close(&core, &handle).await;
    assert!(core.shutdown().await.verified());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 17"]
async fn live_schema_normal_features_postgres_17() {
    schema_features(17).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 12"]
async fn live_schema_normal_features_postgres_12() {
    schema_features(12).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 17"]
async fn live_parameters_results_limits_and_dml() {
    let fixture = fixture(17);
    let mut limits = config();
    limits.result_rows = 2;
    limits.parameter_bytes = 32;
    limits.result_json_bytes = 1024;
    let core = Core::new(limits, fixture.dsn()).expect("Core with test limits");
    let handle = open_and_schema(&core, &fixture.database).await;
    let write = call(core.open_write(&handle, CancellationToken::new())).await;
    assert_eq!(write["result"]["opened"], "write");

    let inserted = call(core.query(
        &handle,
        "CREATE TABLE limit_probe(id integer PRIMARY KEY, text_value text); INSERT INTO limit_probe VALUES (1, 'a'), (2, 'b'), (3, 'c')".to_owned(),
        Vec::new(),
        CancellationToken::new(),
    )).await;
    // PQsendQueryParams lets PostgreSQL enforce the one-command extended
    // protocol. The server rejection is a DATABASE_ERROR; v1 intentionally has
    // no SQL parser merely to pre-classify this as INVALID_INPUT.
    assert_eq!(error_code(&inserted), "DATABASE_ERROR");
    assert_status(&inserted, "in_error", Some(true), Some(true));
    let rolled_back = call(core.rollback(&handle, CancellationToken::new())).await;
    assert_eq!(rolled_back["result"]["rolled_back"], true);
    assert_status(&rolled_back, "idle", Some(false), Some(true));
    let schema = call(core.get_schema(&handle, CancellationToken::new())).await;
    assert_status(&schema, "idle", Some(false), Some(true));
    let write = call(core.open_write(&handle, CancellationToken::new())).await;
    assert_eq!(write["result"]["opened"], "write");
    let created = call(core.query(
        &handle,
        "CREATE TABLE limit_probe(id integer PRIMARY KEY, text_value text)".to_owned(),
        Vec::new(),
        CancellationToken::new(),
    ))
    .await;
    assert_eq!(created["result"]["command_tag"], "CREATE TABLE");
    let insert = call(core.query(
        &handle,
        "INSERT INTO limit_probe VALUES ($1, $2)".to_owned(),
        vec![
            Parameter::new(Some(23), Some("1".to_owned())).unwrap(),
            Parameter::inferred("escaped\\ntext").unwrap(),
        ],
        CancellationToken::new(),
    ))
    .await;
    assert_eq!(insert["result"]["affected_rows"], "1");
    let null_insert = call(core.query(
        &handle,
        "INSERT INTO limit_probe VALUES ($1, $2)".to_owned(),
        vec![
            Parameter::new(Some(23), Some("2".to_owned())).unwrap(),
            Parameter::null(Some(25)),
        ],
        CancellationToken::new(),
    ))
    .await;
    assert_eq!(null_insert["result"]["affected_rows"], "1");
    let too_many_parameters = call(core.query(
        &handle,
        "SELECT $1::text".to_owned(),
        vec![Parameter::inferred("123456789012345678901234567890123").unwrap()],
        CancellationToken::new(),
    ))
    .await;
    assert_eq!(error_code(&too_many_parameters), "INVALID_INPUT");
    let third = call(core.query(
        &handle,
        "INSERT INTO limit_probe VALUES (3, 'third')".to_owned(),
        Vec::new(),
        CancellationToken::new(),
    ))
    .await;
    assert_eq!(third["result"]["affected_rows"], "1");
    let rows = call(core.query(
        &handle,
        "SELECT id, text_value FROM limit_probe ORDER BY id".to_owned(),
        Vec::new(),
        CancellationToken::new(),
    ))
    .await;
    assert_eq!(error_code(&rows), "RESULT_LIMIT");
    assert_status(&rows, "in_transaction", Some(true), Some(true));
    let rollback = call(core.rollback(&handle, CancellationToken::new())).await;
    assert_eq!(rollback["result"]["rolled_back"], true);
    close(&core, &handle).await;
    assert!(core.shutdown().await.verified());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 17"]
async fn live_copy_is_disposed_with_exact_error() {
    let fixture = fixture(17);
    let core = core(&fixture);
    let handle = open_and_schema(&core, &fixture.database).await;
    let opened = call(core.open_write(&handle, CancellationToken::new())).await;
    assert_eq!(opened["result"]["opened"], "write");
    let copy = call(core.query(
        &handle,
        "COPY (SELECT 1) TO STDOUT".to_owned(),
        Vec::new(),
        CancellationToken::new(),
    ))
    .await;
    assert_eq!(error_code(&copy), "COPY_UNSUPPORTED");
    assert_eq!(
        copy["error"]["message"],
        "COPY streaming is unsupported; connection disposed."
    );
    assert_eq!(copy["error"]["transaction_outcome"], "unknown");
    assert_eq!(copy["handle_state"]["connection_status"], "bad");
    assert_eq!(copy["next_moves"][0]["tool"], "close_database");
    let closed = call(core.close_database(&handle, CancellationToken::new())).await;
    assert_eq!(closed["result"]["disposed"], true);
    assert!(core.shutdown().await.verified());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 17"]
async fn same_target_handles_progress_concurrently() {
    let fixture = fixture(17);
    let core = core(&fixture);
    let (first_handle, second_handle) = tokio::join!(
        open_and_schema(&core, &fixture.database),
        open_and_schema(&core, &fixture.database)
    );
    assert_ne!(first_handle, second_handle);
    let (r1, r2) = tokio::join!(
        call(core.open_read(&first_handle, CancellationToken::new())),
        call(core.open_read(&second_handle, CancellationToken::new()))
    );
    assert_eq!(r1["result"]["opened"], "read");
    assert_eq!(r2["result"]["opened"], "read");
    let q1 = call(core.query(
        &first_handle,
        "SELECT pg_sleep(0.10), 11".to_owned(),
        Vec::new(),
        CancellationToken::new(),
    ));
    let q2 = call(core.query(
        &second_handle,
        "SELECT 22".to_owned(),
        Vec::new(),
        CancellationToken::new(),
    ));
    let (q1, q2) = tokio::join!(q1, q2);
    assert_eq!(q1["result"]["rows"][0][1], "11");
    assert_eq!(q2["result"]["rows"][0][0], "22");
    let (c1, c2) = tokio::join!(
        call(core.commit(&first_handle, CancellationToken::new())),
        call(core.commit(&second_handle, CancellationToken::new()))
    );
    assert_eq!(c1["result"]["committed"], true);
    assert_eq!(c2["result"]["committed"], true);
    close(&core, &first_handle).await;
    close(&core, &second_handle).await;
    assert!(core.shutdown().await.verified());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 17"]
async fn same_target_cancellation_finishes_without_replay() {
    let fixture = fixture(17);
    let core = core(&fixture);
    let handle = open_and_schema(&core, &fixture.database).await;
    let opened = call(core.open_read(&handle, CancellationToken::new())).await;
    assert_eq!(opened["result"]["opened"], "read");
    let cancellation = CancellationToken::new();
    let query = core.query(
        &handle,
        "SELECT pg_sleep(30)".to_owned(),
        Vec::new(),
        cancellation.clone(),
    );
    tokio::pin!(query);
    let outcome = tokio::select! {
        result = &mut query => result,
        _ = tokio::time::sleep(Duration::from_millis(100)) => {
            cancellation.cancel();
            query.await
        }
    }
    .unwrap_or_else(|error| panic!("query transport failure: {error:?}"));
    let outcome = serde_json::to_value(outcome).expect("serializable envelope");
    assert!(matches!(
        error_code(&outcome),
        "CANCELLED" | "DEADLINE_EXCEEDED" | "CONNECTION_LOST"
    ));
    assert!(
        outcome["error"]["transaction_outcome"] == "unknown"
            || outcome["handle_state"]["transaction_status"] == "in_transaction"
            || outcome["handle_state"]["transaction_status"] == "idle"
    );
    if outcome["handle_state"]["connection_status"] == "ok" {
        let rollback = call(core.rollback(&handle, CancellationToken::new())).await;
        assert_eq!(rollback["result"]["rolled_back"], true);
    }
    close(&core, &handle).await;
    assert!(core.shutdown().await.verified());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 17"]
async fn live_commit_disconnect_reports_unknown_without_retry() {
    let fixture = fixture(17);
    let proxy = TransparentProxy::bind(fixture.address()).expect("loopback proxy");
    let control = proxy.control();
    let core = Core::new(
        config(),
        fixture.dsn_at("127.0.0.1", proxy.local_addr().port()),
    )
    .expect("Core through proxy");
    let handle = open_and_schema(&core, &fixture.database).await;
    let opened = call(core.open_write(&handle, CancellationToken::new())).await;
    assert_eq!(opened["result"]["opened"], "write");
    let created = call(core.query(
        &handle,
        "CREATE TABLE commit_disconnect_probe(id integer)".to_owned(),
        Vec::new(),
        CancellationToken::new(),
    ))
    .await;
    assert_eq!(created["result"]["command_tag"], "CREATE TABLE");
    control.wait_for_connection().expect("proxy connection");
    control
        .pause(Direction::ServerToClient)
        .expect("pause commit response");
    let commit_task = tokio::spawn({
        let core = core.clone();
        let handle = handle.clone();
        async move { core.commit(&handle, CancellationToken::new()).await }
    });
    let _ = tokio::time::timeout(Duration::from_millis(750), async {
        while control.connection_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    control
        .drop_connection()
        .expect("drop proxied commit connection");
    let commit = tokio::time::timeout(Duration::from_secs(8), commit_task)
        .await
        .expect("commit returns after disconnect")
        .expect("commit task join")
        .expect("commit returns envelope");
    let commit = serde_json::to_value(commit).expect("commit envelope JSON");
    assert!(
        matches!(
            error_code(&commit),
            "COMMIT_OUTCOME_UNKNOWN" | "CONNECTION_LOST" | "CLEANUP_UNCERTAIN"
        ),
        "unexpected commit disconnect response: {commit}"
    );
    assert_eq!(commit["error"]["transaction_outcome"], "unknown");
    assert!(
        commit["next_moves"]
            .as_array()
            .unwrap()
            .iter()
            .any(|movement| movement["tool"] == "close_database")
    );
    assert!(core.shutdown().await.verified());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 17"]
async fn live_cancel_and_disconnect_reports_uncertainty() {
    let fixture = fixture(17);
    let proxy = TransparentProxy::bind(fixture.address()).expect("loopback proxy");
    let control = proxy.control();
    let core = Core::new(
        config(),
        fixture.dsn_at("127.0.0.1", proxy.local_addr().port()),
    )
    .expect("Core through proxy");
    let handle = open_and_schema(&core, &fixture.database).await;
    let opened = call(core.open_read(&handle, CancellationToken::new())).await;
    assert_eq!(opened["result"]["opened"], "read");
    control.wait_for_connection().expect("proxy connection");
    control
        .pause(Direction::ServerToClient)
        .expect("pause query response");
    let cancellation = CancellationToken::new();
    let query_task = tokio::spawn({
        let core = core.clone();
        let handle = handle.clone();
        let cancellation = cancellation.clone();
        async move {
            core.query(
                &handle,
                "SELECT pg_sleep(30)".to_owned(),
                Vec::new(),
                cancellation,
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_millis(750), async {
        while control.connection_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("query reaches proxy watchdog");
    cancellation.cancel();
    control
        .drop_connection()
        .expect("drop active query connection");
    let outcome = tokio::time::timeout(Duration::from_secs(8), query_task)
        .await
        .expect("cancel/disconnect returns")
        .expect("query task join")
        .expect("query returns envelope");
    let outcome = serde_json::to_value(outcome).expect("query envelope JSON");
    assert!(
        matches!(
            error_code(&outcome),
            "CANCELLED" | "DEADLINE_EXCEEDED" | "CONNECTION_LOST"
        ),
        "unexpected cancel/disconnect response: {outcome}"
    );
    assert!(
        outcome["error"]["transaction_outcome"] == "unknown"
            || outcome["handle_state"]["connection_status"] == "bad"
    );
    let report = core.shutdown().await;
    assert!(
        !report.verified()
            && report
                .workers
                .iter()
                .any(|worker| worker.outcome
                    == postgres_mcp_core::worker::ShutdownOutcome::Uncertain),
        "disconnect cleanup must remain truthfully uncertain: {report:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PG_MCP_LIVE=1 and disposable Podman PostgreSQL 17"]
async fn live_fixture_cleanup_is_disposable_and_local() {
    let fixture = fixture(17);
    let container = fixture.container_name().to_owned();
    assert!(container.starts_with("postgres-mcp-live-"));
    assert_eq!(fixture.host, "127.0.0.1");
    assert_ne!(fixture.port, 5432);
    assert!(fixture.image().starts_with("postgres:"));
    drop(fixture);
    let output = std::process::Command::new("podman")
        .args(["container", "exists", &container])
        .output()
        .expect("podman container exists");
    assert!(!output.status.success(), "fixture container survived Drop");
}
