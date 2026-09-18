//! Worker-owned PostgreSQL connections and explicit MCP lifecycle contracts.
pub mod config;
mod libpq;
pub mod protocol;
mod schema;
pub mod worker;

pub use worker::{Core, CoreFailure};
