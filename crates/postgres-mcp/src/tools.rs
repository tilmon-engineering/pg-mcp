//! The rmcp tool surface for the PostgreSQL MCP server.
//!
//! This module deliberately contains no PostgreSQL access.  All connection and
//! transaction work is delegated to `postgres-mcp-core::Core`, and the request
//! cancellation token is bridged into every call by [`McpServer::call_tool`].

use postgres_mcp_core::{
    protocol::{
        ConnectionStatus, Envelope, ErrorCode, ErrorPayload, HandleState, Parameter, QueryRequest,
        TransactionStatus, canonicalize_json, process_moves,
    },
    worker::{Core, CoreFailure},
};
use rmcp::{
    RoleServer,
    handler::server::tool::ToolCallContext,
    handler::server::wrapper::Parameters,
    model::{
        CallToolRequestParams, CallToolResult, ListToolsResult, PaginatedRequestParams,
        ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
    tool, tool_router,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

// Task-local bridge used by every generated tool route.
tokio::task_local! {
    pub(crate) static REQUEST_CT: CancellationToken;
}

pub const TOOL_NAMES: [&str; 9] = [
    "open_database",
    "list_handles",
    "get_schema",
    "open_read",
    "open_write",
    "query",
    "commit",
    "rollback",
    "close_database",
];

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OpenDatabaseArgs {
    pub profile: String,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HandleArgs {
    pub handle: String,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NoArgs {}

/// A strict wire parameter.  Both keys are required even though each value is
/// nullable: `{ "type_oid": null, "value": null }` is the SQL NULL form.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParameterArg {
    pub type_oid: Option<u32>,
    pub value: Option<String>,
}

impl<'de> Deserialize<'de> for ParameterArg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `Option<Option<T>>` collapses a missing field and explicit null in
        // serde. Decode through Value so both required nullable keys remain
        // distinguishable and unknown keys are rejected.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            type_oid: Value,
            value: Value,
        }
        let raw = Raw::deserialize(deserializer)?;
        let type_oid = match raw.type_oid {
            Value::Null => None,
            Value::Number(number) => number
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| {
                    serde::de::Error::custom("type_oid must be an unsigned 32-bit integer or null")
                })
                .map(Some)?,
            _ => {
                return Err(serde::de::Error::custom(
                    "type_oid must be an unsigned 32-bit integer or null",
                ));
            }
        };
        let value = match raw.value {
            Value::Null => None,
            Value::String(value) => Some(value),
            _ => return Err(serde::de::Error::custom("value must be a string or null")),
        };
        Ok(Self { type_oid, value })
    }
}

impl TryFrom<ParameterArg> for Parameter {
    type Error = String;

    fn try_from(value: ParameterArg) -> Result<Self, Self::Error> {
        Parameter::new(value.type_oid, value.value).map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryArgs {
    pub handle: String,
    pub sql: String,
    #[serde(default)]
    pub parameters: Vec<ParameterArg>,
}

impl QueryArgs {
    fn into_core(self) -> Result<(String, String, Vec<Parameter>), String> {
        let request = QueryRequest {
            sql: self.sql.clone(),
            parameters: self
                .parameters
                .into_iter()
                .map(Parameter::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        };
        // Core performs the application-level byte/count checks with the
        // operator's actual configuration before worker admission.
        Ok((self.handle, request.sql, request.parameters))
    }
}

#[derive(Clone)]
pub struct McpServer {
    pub core: Core,
}

impl McpServer {
    pub fn new(core: Core) -> Self {
        Self { core }
    }

    pub async fn shutdown(&self) -> postgres_mcp_core::worker::ShutdownReport {
        self.core.shutdown().await
    }
}

/// Convert a core envelope into one canonical JSON object for both rmcp output
/// channels.  `CallToolResult::structured` itself mirrors the compact text
/// value, but canonicalizing before both calls makes the byte contract explicit.
fn canonical_value<T: Serialize>(envelope: &Envelope<T>) -> Value {
    canonicalize_json(&serde_json::to_value(envelope).unwrap_or_else(|_| {
        json!({
            "envelope_version": 1,
            "handle_state": null,
            "next_moves": [],
            "error": {
                "code": "INTERNAL",
                "message": ErrorCode::Internal.message(),
                "sqlstate": null,
                "transaction_outcome": null
            }
        })
    }))
}

fn success<T: Serialize>(envelope: Envelope<T>) -> CallToolResult {
    let value = canonical_value(&envelope);
    if envelope.error.is_some() {
        CallToolResult::structured_error(value)
    } else {
        CallToolResult::structured(value)
    }
}

fn failure(code: ErrorCode) -> CallToolResult {
    let envelope = Envelope::<Value>::failure(None, process_moves(), ErrorPayload::new(code));
    CallToolResult::structured_error(canonical_value(&envelope))
}

/// Convert a typed core failure without inspecting diagnostic text. The core
/// already retains the only permitted dynamic details: an observed handle
/// snapshot, a validated SQLSTATE, and operation-outcome uncertainty.
fn failure_for(failure: CoreFailure) -> CallToolResult {
    let envelope = postgres_mcp_core::protocol::error_envelope_with_sqlstate(
        failure.handle_state,
        failure.error,
        failure.outcome_unknown,
        failure.sqlstate.as_deref(),
    );
    CallToolResult::structured_error(canonical_value(&envelope))
}

#[tool_router]
impl McpServer {
    #[tool(
        name = "open_database",
        description = "Open a configured PostgreSQL profile by name; its DSN is resolved at open time. Inspect schema before opening a transaction.",
        input_schema = rmcp::handler::server::common::schema_for_type::<Parameters<OpenDatabaseArgs>>()
    )]
    async fn open_database(
        &self,
        Parameters(args): Parameters<OpenDatabaseArgs>,
    ) -> CallToolResult {
        match self
            .core
            .open_database(
                &args.profile,
                REQUEST_CT.try_with(Clone::clone).unwrap_or_default(),
            )
            .await
        {
            Ok(envelope) => success(envelope),
            Err(error) => failure_for(error),
        }
    }

    #[tool(
        name = "list_handles",
        description = "List all live opaque PostgreSQL handles sorted by handle byte order.",
        input_schema = rmcp::handler::server::common::schema_for_type::<Parameters<NoArgs>>()
    )]
    async fn list_handles(&self, Parameters(_): Parameters<NoArgs>) -> CallToolResult {
        match self.core.list_handles().await {
            Ok(envelope) => success(envelope),
            Err(error) => failure_for(error),
        }
    }

    #[tool(
        name = "get_schema",
        description = "Retrieve bounded role-visible PostgreSQL schema metadata. Schema observation is required before opening a transaction.",
        input_schema = rmcp::handler::server::common::schema_for_type::<Parameters<HandleArgs>>()
    )]
    async fn get_schema(&self, Parameters(args): Parameters<HandleArgs>) -> CallToolResult {
        match self
            .core
            .get_schema(
                &args.handle,
                REQUEST_CT.try_with(Clone::clone).unwrap_or_default(),
            )
            .await
        {
            Ok(envelope) => success(envelope),
            Err(error) => failure_for(error),
        }
    }

    #[tool(
        name = "open_read",
        description = "Open a PostgreSQL native READ ONLY transaction after schema observation. Native read-only mode is not a general side-effect sandbox.",
        input_schema = rmcp::handler::server::common::schema_for_type::<Parameters<HandleArgs>>()
    )]
    async fn open_read(&self, Parameters(args): Parameters<HandleArgs>) -> CallToolResult {
        match self
            .core
            .open_read(
                &args.handle,
                REQUEST_CT.try_with(Clone::clone).unwrap_or_default(),
            )
            .await
        {
            Ok(envelope) => success(envelope),
            Err(error) => failure_for(error),
        }
    }

    #[tool(
        name = "open_write",
        description = "Open a PostgreSQL native READ WRITE transaction after schema observation; changes persist only after commit.",
        input_schema = rmcp::handler::server::common::schema_for_type::<Parameters<HandleArgs>>()
    )]
    async fn open_write(&self, Parameters(args): Parameters<HandleArgs>) -> CallToolResult {
        match self
            .core
            .open_write(
                &args.handle,
                REQUEST_CT.try_with(Clone::clone).unwrap_or_default(),
            )
            .await
        {
            Ok(envelope) => success(envelope),
            Err(error) => failure_for(error),
        }
    }

    #[tool(
        name = "query",
        description = "Execute one PostgreSQL SQL statement with text/null parameters inside the active transaction. Results preserve PostgreSQL text and metadata.",
        input_schema = rmcp::handler::server::common::schema_for_type::<Parameters<QueryArgs>>()
    )]
    async fn query(&self, Parameters(args): Parameters<QueryArgs>) -> CallToolResult {
        let (handle, sql, parameters) = match args.into_core() {
            Ok(value) => value,
            Err(_) => return failure(ErrorCode::InvalidInput),
        };
        match self
            .core
            .query(
                &handle,
                sql,
                parameters,
                REQUEST_CT.try_with(Clone::clone).unwrap_or_default(),
            )
            .await
        {
            Ok(envelope) => success(envelope),
            Err(error) => failure_for(error),
        }
    }

    #[tool(
        name = "commit",
        description = "Commit the active PostgreSQL write transaction. A lost commit response is reported as unknown and must not be replayed automatically.",
        input_schema = rmcp::handler::server::common::schema_for_type::<Parameters<HandleArgs>>()
    )]
    async fn commit(&self, Parameters(args): Parameters<HandleArgs>) -> CallToolResult {
        match self
            .core
            .commit(
                &args.handle,
                REQUEST_CT.try_with(Clone::clone).unwrap_or_default(),
            )
            .await
        {
            Ok(envelope) => success(envelope),
            Err(error) => failure_for(error),
        }
    }

    #[tool(
        name = "rollback",
        description = "Roll back the active transaction; rollback is an idempotent no-op for an idle valid handle.",
        input_schema = rmcp::handler::server::common::schema_for_type::<Parameters<HandleArgs>>()
    )]
    async fn rollback(&self, Parameters(args): Parameters<HandleArgs>) -> CallToolResult {
        match self
            .core
            .rollback(
                &args.handle,
                REQUEST_CT.try_with(Clone::clone).unwrap_or_default(),
            )
            .await
        {
            Ok(envelope) => success(envelope),
            Err(error) => failure_for(error),
        }
    }

    #[tool(
        name = "close_database",
        description = "Dispose an idle PostgreSQL handle locally. Active transactions must be resolved first.",
        input_schema = rmcp::handler::server::common::schema_for_type::<Parameters<HandleArgs>>()
    )]
    async fn close_database(&self, Parameters(args): Parameters<HandleArgs>) -> CallToolResult {
        match self
            .core
            .close_database(
                &args.handle,
                REQUEST_CT.try_with(Clone::clone).unwrap_or_default(),
            )
            .await
        {
            Ok(envelope) => success(envelope),
            Err(error) => failure_for(error),
        }
    }
}

impl rmcp::handler::server::ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    fn get_tool(&self, name: &str) -> Option<rmcp::model::Tool> {
        Self::tool_router().get(name).cloned()
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>>
    + rmcp::service::MaybeSendFuture
    + '_ {
        std::future::ready(Ok(ListToolsResult::with_all_items(
            Self::tool_router().list_all(),
        )))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>>
    + rmcp::service::MaybeSendFuture
    + '_ {
        let tool_context = ToolCallContext::new(self, request, context.clone());
        async move {
            let router = Self::tool_router();
            REQUEST_CT
                .scope(context.ct.clone(), router.call(tool_context))
                .await
        }
    }
}

// Keep these imports visible to rustdoc users inspecting generated schemas;
// they are also compile-time assertions that the protocol model exposes all
// required lifecycle fields rather than an alternate cached Boolean state.
#[allow(dead_code)]
fn _protocol_shape_assertions(
    state: HandleState,
    connection: ConnectionStatus,
    transaction: TransactionStatus,
) {
    let _ = (state, connection, transaction);
}
