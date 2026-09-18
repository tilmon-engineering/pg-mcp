//! Disposable PostgreSQL containers for ignored live integration tests.
//!
//! The fixture deliberately owns the complete lifecycle. It never reads an
//! operator DSN, uses a fixed host port, or writes credentials to disk. A live
//! test opts in with `PG_MCP_LIVE=1` and receives a failure (rather than a
//! skip) when Podman or an image is unavailable.

use std::net::SocketAddr;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(90);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub struct PostgresFixture {
    container: String,
    image: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

#[derive(Debug)]
pub struct FixtureError {
    operation: &'static str,
    detail: String,
}

impl FixtureError {
    fn new(operation: &'static str, detail: impl Into<String>) -> Self {
        Self {
            operation,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fixture {} failed: {}", self.operation, self.detail)
    }
}

impl std::error::Error for FixtureError {}

impl PostgresFixture {
    /// Start an isolated PostgreSQL image on a dynamically allocated loopback
    /// port and wait until its server accepts connections.
    pub fn start(version: u16) -> Result<Self, FixtureError> {
        let image = match version {
            12 => "postgres:12",
            17 => "postgres:17-bookworm",
            _ => return Err(FixtureError::new("start", "unsupported fixture version")),
        };
        podman_version()?;

        let nonce = unique_nonce();
        let container = format!("postgres-mcp-live-{version}-{nonce}");
        let user = format!("fixture_user_{nonce}");
        let database = format!("fixture_db_{nonce}");
        // These are process-local strings. They are passed only to Podman and
        // never persisted or included in an error message.
        let password = format!("fixture_pw_{nonce}_a9F4");

        let output = Command::new("podman")
            .args([
                "run",
                "--detach",
                "--rm",
                "--name",
                &container,
                "--env",
                &format!("POSTGRES_USER={user}"),
                "--env",
                &format!("POSTGRES_PASSWORD={password}"),
                "--env",
                &format!("POSTGRES_DB={database}"),
                "--publish",
                "127.0.0.1::5432",
                image,
            ])
            .output()
            .map_err(|error| FixtureError::new("podman run", error.to_string()))?;
        if !output.status.success() {
            return Err(FixtureError::new("podman run", command_detail(&output)));
        }

        let fixture = Self {
            container,
            image: image.to_owned(),
            host: "127.0.0.1".to_owned(),
            port: 0,
            user,
            password,
            database,
        };

        let port = fixture.published_port()?;
        // Keep the original Drop owner intact; replacing one field with struct
        // update syntax would move Strings out of a type that owns cleanup.
        let mut fixture = fixture;
        fixture.port = port;
        if let Err(error) = fixture.wait_ready() {
            drop(fixture);
            return Err(error);
        }
        Ok(fixture)
    }

    /// A libpq keyword/value DSN built exclusively from this fixture's
    /// generated, loopback-only values. Callers should not print it.
    pub fn dsn(&self) -> String {
        self.dsn_at(&self.host, self.port)
    }

    /// Build a fixture DSN against a test-only loopback endpoint, retaining
    /// this fixture's generated identity and database.
    pub fn dsn_at(&self, host: &str, port: u16) -> String {
        assert!(
            host == "127.0.0.1" || host == "localhost",
            "fixture endpoint must be loopback"
        );
        assert_ne!(port, 0, "fixture endpoint must have a port");
        format!(
            "host={} port={} user={} password={} dbname={} sslmode=disable",
            host, port, self.user, self.password, self.database
        )
    }

    /// The direct endpoint used as the upstream target for a transparent proxy.
    pub fn address(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.port))
    }

    /// Execute setup or independent verification SQL inside the container.
    /// This is intentionally not used for the operations whose behavior is
    /// under test; those go through the actual Core API.
    pub fn exec_sql(&self, sql: &str) -> Result<String, FixtureError> {
        let output = Command::new("podman")
            .args([
                "exec",
                "--env",
                &format!("PGPASSWORD={}", self.password),
                &self.container,
                "psql",
                "--no-psqlrc",
                "--tuples-only",
                "--quiet",
                "--set",
                "ON_ERROR_STOP=1",
                "--username",
                &self.user,
                "--dbname",
                &self.database,
                "--command",
                sql,
            ])
            .env("PGPASSWORD", &self.password)
            .output()
            .map_err(|error| FixtureError::new("podman exec", error.to_string()))?;
        if !output.status.success() {
            return Err(FixtureError::new("psql", command_detail(&output)));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    /// The selected image name is useful in diagnostics without exposing
    /// generated credentials or the complete DSN.
    pub fn image(&self) -> &str {
        &self.image
    }

    /// Container identifier used only by the cleanup assertion; it contains no
    /// credentials or connection data.
    pub fn container_name(&self) -> &str {
        &self.container
    }

    fn published_port(&self) -> Result<u16, FixtureError> {
        let output = Command::new("podman")
            .args(["port", &self.container, "5432/tcp"])
            .output()
            .map_err(|error| FixtureError::new("podman port", error.to_string()))?;
        if !output.status.success() {
            return Err(FixtureError::new("podman port", command_detail(&output)));
        }
        let text = String::from_utf8_lossy(&output.stdout);
        text.lines()
            .find_map(|line| {
                line.rsplit_once(':')
                    .and_then(|(_, port)| port.trim().parse().ok())
            })
            .filter(|port: &u16| *port != 0)
            .ok_or_else(|| {
                FixtureError::new("podman port", "no dynamic loopback port was published")
            })
    }

    fn wait_ready(&self) -> Result<(), FixtureError> {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            let output = Command::new("podman")
                .args([
                    "exec",
                    &self.container,
                    "pg_isready",
                    "--host=127.0.0.1",
                    &format!("--port={}", 5432),
                    &format!("--username={}", self.user),
                    &format!("--dbname={}", self.database),
                ])
                .output()
                .map_err(|error| FixtureError::new("pg_isready", error.to_string()))?;
            if output.status.success() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(FixtureError::new(
                    "pg_isready",
                    "server did not become ready before deadline",
                ));
            }
            thread::sleep(POLL_INTERVAL);
        }
    }
}

impl Drop for PostgresFixture {
    fn drop(&mut self) {
        // `--rm` handles normal exits; `rm --force` also handles a failed
        // startup and is intentionally best effort during unwinding.
        let _ = Command::new("podman")
            .args(["rm", "--force", &self.container])
            .output();
    }
}

fn podman_version() -> Result<(), FixtureError> {
    let output = Command::new("podman")
        .arg("--version")
        .output()
        .map_err(|error| FixtureError::new("podman", error.to_string()))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(FixtureError::new("podman", command_detail(&output)))
    }
}

fn unique_nonce() -> String {
    let counter = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{}-{}", std::process::id(), nanos, counter)
}

fn command_detail(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let detail = if stderr.trim().is_empty() {
        stdout
    } else {
        stderr
    };
    // Podman/psql output is only surfaced as setup diagnostics. Redact common
    // credential labels so a future image error cannot echo the password.
    let mut detail = detail.trim().replace("password=", "password=[redacted]");
    if detail.len() > 512 {
        detail.truncate(512);
    }
    if detail.is_empty() {
        format!("exit status {}", output.status)
    } else {
        detail
    }
}
