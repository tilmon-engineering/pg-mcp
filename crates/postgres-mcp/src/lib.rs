//! Production rmcp serving orchestration for `postgres-mcp`.
//!
//! There is one cleanup owner: [`serve_with_cleanup`].  The binary uses it for
//! EOF, SIGINT, initialization failures, and serving failures; tests can use a
//! caller-owned transport while exercising the same path.

mod tools;

pub use tools::{McpServer, TOOL_NAMES};

use postgres_mcp_core::{
    Core,
    config::{Config, ConfigFile},
    worker::ShutdownReport,
};
use rmcp::{RoleServer, transport::IntoTransport};
use std::fmt;
use tokio_util::sync::CancellationToken;

/// The primary serving failure and cleanup failures are kept separate so a
/// transport error cannot be hidden by a later cleanup problem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServeFailure {
    pub primary: String,
    pub cleanup_errors: Vec<String>,
}

impl fmt::Display for ServeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.primary)?;
        for error in &self.cleanup_errors {
            write!(f, "; cleanup failure: {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ServeFailure {}

fn cleanup_errors(report: &ShutdownReport) -> Vec<String> {
    report
        .workers
        .iter()
        .filter(|entry| entry.outcome != postgres_mcp_core::worker::ShutdownOutcome::Verified)
        .map(|entry| {
            format!(
                "handle {}: cleanup uncertain at {}",
                entry.handle,
                entry.stage.unwrap_or("unknown")
            )
        })
        .collect()
}

/// Serve one MCP session and always perform aggregate core cleanup.
///
/// The cancellation token represents SIGINT (or a test equivalent).  EOF and
/// rmcp serve failures also converge here.  The token is not used to bypass
/// cleanup: it only asks rmcp to stop accepting work before the core report is
/// collected.
pub async fn serve_with_cleanup<T, E, A>(
    config: Config,
    dsn: String,
    transport: T,
    shutdown_ct: CancellationToken,
) -> Result<(), ServeFailure>
where
    T: IntoTransport<RoleServer, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let core = Core::new(config, dsn).map_err(|error| ServeFailure {
        primary: static_core_error(error),
        cleanup_errors: Vec::new(),
    })?;
    let server = McpServer::new(core.clone());

    // Initialization itself can be waiting on a transport read. Race it against
    // shutdown so SIGINT never waits for rmcp initialization before core cleanup.
    let initialization =
        rmcp::service::serve_server_with_ct(server, transport, shutdown_ct.clone());
    tokio::pin!(initialization);
    let primary = tokio::select! {
        initialized = &mut initialization => match initialized {
            Ok(running) => {
                tokio::select! {
                    result = running.waiting() => result
                        .err()
                        .map(|error| format!("MCP stdio server failed: {error}")),
                    _ = shutdown_ct.cancelled() => None,
                }
            }
            Err(_error) if shutdown_ct.is_cancelled() => None,
            Err(error) => Some(format!("failed to initialize MCP stdio server: {error}")),
        },
        _ = shutdown_ct.cancelled() => None,
    };

    // Core owns the aggregate deadline.  It freezes a report and returns it
    // idempotently, including uncertainty for native calls that cannot join.
    let report = core.shutdown().await;
    let cleanup = cleanup_errors(&report);
    match (primary, cleanup.is_empty()) {
        (Some(primary), _) => Err(ServeFailure {
            primary,
            cleanup_errors: cleanup,
        }),
        (None, true) => Ok(()),
        (None, false) => Err(ServeFailure {
            primary: "server stopped with cleanup uncertainty".to_owned(),
            cleanup_errors: cleanup,
        }),
    }
}

/// Convenience production wrapper over rmcp's line-delimited stdio transport.
pub async fn serve_stdio(
    config: Config,
    dsn: String,
    shutdown_ct: CancellationToken,
) -> Result<(), ServeFailure> {
    serve_with_cleanup(config, dsn, rmcp::transport::io::stdio(), shutdown_ct).await
}

pub async fn serve_profiles(
    config: ConfigFile,
    shutdown_ct: CancellationToken,
) -> Result<(), ServeFailure> {
    let core = Core::new_with_profiles(config).map_err(|error| ServeFailure {
        primary: static_core_error(error),
        cleanup_errors: Vec::new(),
    })?;
    let server = McpServer::new(core.clone());
    let initialization = rmcp::service::serve_server_with_ct(
        server,
        rmcp::transport::io::stdio(),
        shutdown_ct.clone(),
    );
    tokio::pin!(initialization);
    let primary = tokio::select! {
        initialized = &mut initialization => match initialized {
            Ok(running) => tokio::select! { result = running.waiting() => result.err().map(|e| format!("MCP stdio server failed: {e}")), _ = shutdown_ct.cancelled() => None },
            Err(_) if shutdown_ct.is_cancelled() => None,
            Err(error) => Some(format!("failed to initialize MCP stdio server: {error}")),
        },
        _ = shutdown_ct.cancelled() => None,
    };
    let report = core.shutdown().await;
    let cleanup = cleanup_errors(&report);
    match (primary, cleanup.is_empty()) {
        (Some(primary), _) => Err(ServeFailure {
            primary,
            cleanup_errors: cleanup,
        }),
        (None, true) => Ok(()),
        (None, false) => Err(ServeFailure {
            primary: "server stopped with cleanup uncertainty".to_owned(),
            cleanup_errors: cleanup,
        }),
    }
}

/// Keep startup diagnostics static.  In particular, do not expose the DSN or
/// native backend text in process stderr.
fn static_core_error(error: postgres_mcp_core::protocol::CoreError) -> String {
    format!("startup failed: {}", error.code().message())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertises_exactly_nine_tools() {
        assert_eq!(TOOL_NAMES.len(), 9);
        assert_eq!(
            TOOL_NAMES,
            [
                "open_database",
                "list_handles",
                "get_schema",
                "open_read",
                "open_write",
                "query",
                "commit",
                "rollback",
                "close_database",
            ]
        );
    }
}
