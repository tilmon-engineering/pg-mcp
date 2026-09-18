//! Public version-1 PostgreSQL MCP protocol contracts.
//!
//! This module contains value types only.  It does not parse DSNs, execute SQL,
//! infer PostgreSQL types, or decide whether a statement is safe.  The adapter
//! remains responsible for libpq interaction and authoritative transaction
//! observations.

use crate::config::Config;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned, de::Error as DeError,
};
use serde_json::{Map, Value};
use thiserror::Error;

pub const ENVELOPE_VERSION: u8 = 1;
pub const QUERY_GUIDANCE: &str =
    "Provide SQL and parameters; do not replay a failed statement blindly.";
pub const SCHEMA_GUIDANCE: &str = "Inspect schema before opening a transaction.";
pub const OPEN_READ_GUIDANCE: &str = "Start a PostgreSQL read-only transaction.";
pub const OPEN_WRITE_GUIDANCE: &str = "Start a PostgreSQL read-write transaction.";
pub const COMMIT_GUIDANCE: &str = "Commit the current transaction.";
pub const ROLLBACK_GUIDANCE: &str = "Roll back the current transaction.";
pub const CLOSE_GUIDANCE: &str = "Dispose this handle locally.";
pub const INERROR_ROLLBACK_GUIDANCE: &str = "Transaction failed; roll back before continuing.";
pub const UNKNOWN_CLOSE_GUIDANCE: &str =
    "Connection unusable; dispose locally. Transaction outcome may be unknown.";

fn exact_object_keys(object: &Map<String, Value>, required: &[&str]) -> Result<(), String> {
    for key in required {
        if !object.contains_key(*key) {
            return Err(format!("missing field `{key}`"));
        }
    }
    allowed_object_keys(object, required)
}

fn allowed_object_keys(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), String> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!("unknown field `{key}`"));
    }
    Ok(())
}

fn required_value<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a Value, String> {
    object
        .get(key)
        .ok_or_else(|| format!("missing field `{key}`"))
}

fn decode_value<T: DeserializeOwned>(object: &Map<String, Value>, key: &str) -> Result<T, String> {
    serde_json::from_value(required_value(object, key)?.clone())
        .map_err(|error| format!("invalid field `{key}`: {error}"))
}

/// Connection health observed by the owning worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionStatus {
    Ok,
    Bad,
    Closed,
}

/// The five PostgreSQL transaction statuses exposed by this protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStatus {
    Idle,
    Active,
    InTransaction,
    InError,
    Unknown,
}

impl TransactionStatus {
    /// The protocol's nullable transaction-open projection.  `ACTIVE` is
    /// deliberately unknown while a command is in flight, not false.
    pub const fn transaction_open(self) -> Option<bool> {
        match self {
            Self::Idle => Some(false),
            Self::InTransaction | Self::InError => Some(true),
            Self::Active | Self::Unknown => None,
        }
    }

    pub const fn is_idle(self) -> bool {
        matches!(self, Self::Idle)
    }

    pub const fn is_transaction_open(self) -> bool {
        matches!(self, Self::InTransaction | Self::InError)
    }
}

/// Authoritative worker snapshot included in handle-bound envelopes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandleState {
    pub handle: String,
    pub database: String,
    pub connection_status: ConnectionStatus,
    pub transaction_status: TransactionStatus,
    pub transaction_open: Option<bool>,
    pub busy: bool,
    pub schema_observed: bool,
}

impl HandleState {
    pub fn new(
        handle: impl Into<String>,
        database: impl Into<String>,
        connection_status: ConnectionStatus,
        transaction_status: TransactionStatus,
        busy: bool,
        schema_observed: bool,
    ) -> Self {
        Self {
            handle: handle.into(),
            database: database.into(),
            connection_status,
            transaction_status,
            transaction_open: transaction_status.transaction_open(),
            busy,
            schema_observed,
        }
    }

    pub fn idle(
        handle: impl Into<String>,
        database: impl Into<String>,
        schema_observed: bool,
    ) -> Self {
        Self::new(
            handle,
            database,
            ConnectionStatus::Ok,
            TransactionStatus::Idle,
            false,
            schema_observed,
        )
    }

    pub fn closed(handle: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            handle: handle.into(),
            database: database.into(),
            connection_status: ConnectionStatus::Closed,
            transaction_status: TransactionStatus::Unknown,
            transaction_open: None,
            busy: false,
            schema_observed: false,
        }
    }

    /// Check the invariant between `transaction_status` and its nullable
    /// projection.  Worker code should call this before constructing guidance.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.transaction_open != self.transaction_status.transaction_open() {
            return Err(ValidationError::InconsistentState);
        }
        if self.connection_status == ConnectionStatus::Closed
            && (self.transaction_status != TransactionStatus::Unknown
                || self.transaction_open.is_some()
                || self.busy
                || self.schema_observed)
        {
            return Err(ValidationError::InconsistentState);
        }
        Ok(())
    }
}

/// The argument template attached to a lifecycle recommendation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Move {
    pub tool: String,
    pub arguments: Value,
    pub guidance: String,
}

impl Move {
    pub fn new(tool: impl Into<String>, arguments: Value, guidance: impl Into<String>) -> Self {
        Self {
            tool: tool.into(),
            arguments,
            guidance: guidance.into(),
        }
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.tool.is_empty() || !self.arguments.is_object() || self.guidance.is_empty() {
            return Err(ValidationError::InvalidMove);
        }
        if self.arguments.as_object().is_some_and(|object| {
            object.keys().any(|key| key != "handle")
                || (self.tool != "list_handles"
                    && self.tool != "open_database"
                    && object.get("handle").and_then(Value::as_str).is_none())
        }) {
            return Err(ValidationError::InvalidMove);
        }
        Ok(())
    }

    pub fn for_handle(
        tool: impl Into<String>,
        handle: impl Into<String>,
        guidance: impl Into<String>,
    ) -> Self {
        Self::new(
            tool,
            serde_json::json!({ "handle": handle.into() }),
            guidance,
        )
    }

    pub fn process(tool: impl Into<String>, guidance: impl Into<String>) -> Self {
        Self::new(tool, Value::Object(Map::new()), guidance)
    }

    pub fn handle_argument(&self) -> Option<&str> {
        self.arguments.get("handle").and_then(Value::as_str)
    }
}

/// Build canonical moves from the observed handle state.
pub fn next_moves(state: &HandleState) -> Vec<Move> {
    moves_for_state(state, false).unwrap_or_default()
}

/// Build the special move list returned when `close_database` is refused.
pub fn close_refusal_moves(state: &HandleState) -> Vec<Move> {
    moves_for_state(state, true).unwrap_or_default()
}

/// Build moves while retaining an explicit validation error for inconsistent
/// snapshots.  Inconsistency must never be turned into guessed transitions.
pub fn moves_for_state(
    state: &HandleState,
    close_refusal: bool,
) -> Result<Vec<Move>, ValidationError> {
    state.validate()?;
    if state.connection_status == ConnectionStatus::Closed {
        return Ok(Vec::new());
    }
    if state.connection_status == ConnectionStatus::Bad
        || state.transaction_status == TransactionStatus::Unknown
    {
        return Ok(vec![Move::for_handle(
            "close_database",
            state.handle.clone(),
            UNKNOWN_CLOSE_GUIDANCE,
        )]);
    }
    if state.busy || state.transaction_status == TransactionStatus::Active {
        return Ok(Vec::new());
    }
    match state.transaction_status {
        TransactionStatus::InError => Ok(vec![Move::for_handle(
            "rollback",
            state.handle.clone(),
            INERROR_ROLLBACK_GUIDANCE,
        )]),
        TransactionStatus::InTransaction if close_refusal => Ok(vec![
            Move::for_handle("commit", state.handle.clone(), COMMIT_GUIDANCE),
            Move::for_handle("rollback", state.handle.clone(), ROLLBACK_GUIDANCE),
        ]),
        TransactionStatus::InTransaction => Ok(vec![
            Move::for_handle("query", state.handle.clone(), QUERY_GUIDANCE),
            Move::for_handle("commit", state.handle.clone(), COMMIT_GUIDANCE),
            Move::for_handle("rollback", state.handle.clone(), ROLLBACK_GUIDANCE),
        ]),
        TransactionStatus::Idle if state.schema_observed => Ok(vec![
            Move::for_handle("open_read", state.handle.clone(), OPEN_READ_GUIDANCE),
            Move::for_handle("open_write", state.handle.clone(), OPEN_WRITE_GUIDANCE),
            Move::for_handle("get_schema", state.handle.clone(), SCHEMA_GUIDANCE),
            Move::for_handle("close_database", state.handle.clone(), CLOSE_GUIDANCE),
        ]),
        TransactionStatus::Idle => Ok(vec![
            Move::for_handle("get_schema", state.handle.clone(), SCHEMA_GUIDANCE),
            Move::for_handle("close_database", state.handle.clone(), CLOSE_GUIDANCE),
        ]),
        TransactionStatus::Unknown | TransactionStatus::Active => unreachable!(),
    }
}

/// Process-level responses (`list_handles`, startup failures, and stopping)
/// have no resolvable handle and therefore no executable moves.
pub fn process_moves() -> Vec<Move> {
    Vec::new()
}

/// Compatibility alias used by the worker publication layer.
pub fn move_for_state(state: &HandleState, close_refusal: bool) -> Vec<Move> {
    moves_for_state(state, close_refusal).unwrap_or_default()
}

/// Build a successful JSON envelope for a handle-bound or process-level tool.
pub fn handle_state_envelope(handle_state: Option<HandleState>, result: Value) -> Envelope {
    let moves = handle_state.as_ref().map_or_else(process_moves, next_moves);
    Envelope::success(handle_state, moves, result)
}

/// Build a static error envelope.  Dynamic SQLSTATE and outcome details are
/// intentionally separate from this convenience constructor.
pub fn error_envelope(
    handle_state: Option<HandleState>,
    error: CoreError,
    outcome_unknown: bool,
) -> Envelope {
    error_envelope_with_sqlstate(handle_state, error, outcome_unknown, None)
}

/// Build an error envelope while retaining a SQLSTATE copied from an actual
/// PostgreSQL result. Invalid values are deliberately omitted rather than
/// synthesized or parsed from backend text.
pub fn error_envelope_with_sqlstate(
    handle_state: Option<HandleState>,
    error: CoreError,
    outcome_unknown: bool,
    sqlstate: Option<&str>,
) -> Envelope {
    let mut payload = ErrorPayload::new(error.code)
        .try_with_sqlstate(sqlstate)
        .unwrap_or_else(|_| ErrorPayload::new(error.code));
    if outcome_unknown || error.outcome_unknown {
        payload = payload.with_unknown_outcome();
    }
    let moves = handle_state.as_ref().map_or_else(process_moves, next_moves);
    Envelope::failure(handle_state, moves, payload)
}

/// Closed error vocabulary.  No backend diagnostic is used as a code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCode {
    #[serde(rename = "UNSUPPORTED_SERVER")]
    UnsupportedServer,
    #[serde(rename = "HANDLE_UNKNOWN")]
    HandleUnknown,
    #[serde(rename = "HANDLE_LIMIT")]
    HandleLimit,
    #[serde(rename = "DATABASE_UNKNOWN")]
    DatabaseUnknown,
    #[serde(rename = "DATABASE_MISMATCH")]
    DatabaseMismatch,
    #[serde(rename = "CONFIG_ERROR")]
    ConfigError,
    #[serde(rename = "QUEUE_FULL")]
    QueueFull,
    #[serde(rename = "SERVER_STOPPING")]
    ServerStopping,
    #[serde(rename = "SCHEMA_REQUIRED")]
    SchemaRequired,
    #[serde(rename = "SCHEMA_LIMIT")]
    SchemaLimit,
    #[serde(rename = "TX_ALREADY_OPEN")]
    TxAlreadyOpen,
    #[serde(rename = "NO_TX_OPEN")]
    NoTxOpen,
    #[serde(rename = "TX_FAILED")]
    TxFailed,
    #[serde(rename = "ACTIVE_TRANSACTION")]
    ActiveTransaction,
    #[serde(rename = "INVALID_INPUT")]
    InvalidInput,
    #[serde(rename = "DATABASE_ERROR")]
    DatabaseError,
    #[serde(rename = "RESULT_LIMIT")]
    ResultLimit,
    #[serde(rename = "COPY_UNSUPPORTED")]
    CopyUnsupported,
    #[serde(rename = "CANCELLED")]
    Cancelled,
    #[serde(rename = "DEADLINE_EXCEEDED")]
    DeadlineExceeded,
    #[serde(rename = "CONNECTION_LOST")]
    ConnectionLost,
    #[serde(rename = "COMMIT_OUTCOME_UNKNOWN")]
    CommitOutcomeUnknown,
    #[serde(rename = "CLEANUP_UNCERTAIN")]
    CleanupUncertain,
    #[serde(rename = "INTERNAL")]
    Internal,
}

/// Backward-friendly name for callers that refer to error classes rather than
/// wire-level codes.
pub type ErrorClass = ErrorCode;

impl ErrorCode {
    pub const ALL: [Self; 24] = [
        Self::UnsupportedServer,
        Self::HandleUnknown,
        Self::HandleLimit,
        Self::DatabaseUnknown,
        Self::DatabaseMismatch,
        Self::ConfigError,
        Self::QueueFull,
        Self::ServerStopping,
        Self::SchemaRequired,
        Self::SchemaLimit,
        Self::TxAlreadyOpen,
        Self::NoTxOpen,
        Self::TxFailed,
        Self::ActiveTransaction,
        Self::InvalidInput,
        Self::DatabaseError,
        Self::ResultLimit,
        Self::CopyUnsupported,
        Self::Cancelled,
        Self::DeadlineExceeded,
        Self::ConnectionLost,
        Self::CommitOutcomeUnknown,
        Self::CleanupUncertain,
        Self::Internal,
    ];

    pub const fn code(self) -> &'static str {
        match self {
            Self::UnsupportedServer => "UNSUPPORTED_SERVER",
            Self::HandleUnknown => "HANDLE_UNKNOWN",
            Self::HandleLimit => "HANDLE_LIMIT",
            Self::DatabaseUnknown => "DATABASE_UNKNOWN",
            Self::DatabaseMismatch => "DATABASE_MISMATCH",
            Self::ConfigError => "CONFIG_ERROR",
            Self::QueueFull => "QUEUE_FULL",
            Self::ServerStopping => "SERVER_STOPPING",
            Self::SchemaRequired => "SCHEMA_REQUIRED",
            Self::SchemaLimit => "SCHEMA_LIMIT",
            Self::TxAlreadyOpen => "TX_ALREADY_OPEN",
            Self::NoTxOpen => "NO_TX_OPEN",
            Self::TxFailed => "TX_FAILED",
            Self::ActiveTransaction => "ACTIVE_TRANSACTION",
            Self::InvalidInput => "INVALID_INPUT",
            Self::DatabaseError => "DATABASE_ERROR",
            Self::ResultLimit => "RESULT_LIMIT",
            Self::CopyUnsupported => "COPY_UNSUPPORTED",
            Self::Cancelled => "CANCELLED",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::ConnectionLost => "CONNECTION_LOST",
            Self::CommitOutcomeUnknown => "COMMIT_OUTCOME_UNKNOWN",
            Self::CleanupUncertain => "CLEANUP_UNCERTAIN",
            Self::Internal => "INTERNAL",
        }
    }

    pub const fn message(self) -> &'static str {
        match self {
            Self::UnsupportedServer => "PostgreSQL server 12 or newer is required.",
            Self::HandleUnknown => "Unknown handle.",
            Self::HandleLimit => "Handle limit reached.",
            Self::DatabaseUnknown => "Database is not configured.",
            Self::DatabaseMismatch => "Connected database does not match configuration.",
            Self::ConfigError => "Invalid server configuration.",
            Self::QueueFull => "Request queue is full.",
            Self::ServerStopping => "Server is stopping.",
            Self::SchemaRequired => "Inspect schema before opening a transaction.",
            Self::SchemaLimit => "Schema response exceeds configured limits.",
            Self::TxAlreadyOpen => "A transaction is already open.",
            Self::NoTxOpen => "No transaction is open.",
            Self::TxFailed => "Transaction failed; roll back before continuing.",
            Self::ActiveTransaction => "Resolve the transaction before closing this handle.",
            Self::InvalidInput => "Invalid query or parameters.",
            Self::DatabaseError => "PostgreSQL rejected the operation.",
            Self::ResultLimit => {
                "Result exceeds configured limits; execution may have taken effect."
            }
            Self::CopyUnsupported => "COPY streaming is unsupported; connection disposed.",
            Self::Cancelled => "Request cancelled; execution may have taken effect.",
            Self::DeadlineExceeded => "Request deadline exceeded; execution may have taken effect.",
            Self::ConnectionLost => "Connection lost; operation outcome may be unknown.",
            Self::CommitOutcomeUnknown => "Commit outcome is unknown; do not retry automatically.",
            Self::CleanupUncertain => "Cleanup could not be verified.",
            Self::Internal => "Internal adapter failure.",
        }
    }

    pub const fn as_str(self) -> &'static str {
        self.code()
    }
}

/// Internal Rust error used by the worker and adapter.  Its `Display` value is
/// the public static message; dynamic backend diagnostics are retained only as
/// separately validated SQLSTATE data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreError {
    pub code: ErrorCode,
    pub outcome_unknown: bool,
}

impl CoreError {
    pub const fn new(code: ErrorCode) -> Self {
        Self {
            code,
            outcome_unknown: false,
        }
    }

    pub fn unknown(code: ErrorCode) -> Self {
        Self {
            outcome_unknown: true,
            ..Self::new(code)
        }
    }

    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    pub const fn as_str(&self) -> &'static str {
        self.code.code()
    }
}

impl std::fmt::Display for CoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for CoreError {}

impl From<ValidationError> for CoreError {
    fn from(error: ValidationError) -> Self {
        let code = match error {
            ValidationError::SchemaLimit => ErrorCode::SchemaLimit,
            ValidationError::ParameterCountLimit
            | ValidationError::ParameterByteLimit
            | ValidationError::ResultLimit => ErrorCode::ResultLimit,
            _ => ErrorCode::InvalidInput,
        };
        Self::new(code)
    }
}

macro_rules! core_error_constants {
    ($(($name:ident, $code:ident)),+ $(,)?) => {
        #[allow(non_upper_case_globals)]
        impl CoreError {
            $(pub const $name: Self = Self { code: ErrorCode::$code, outcome_unknown: false };)+
        }
    };
}

core_error_constants!(
    (UnsupportedServer, UnsupportedServer),
    (HandleUnknown, HandleUnknown),
    (HandleLimit, HandleLimit),
    (DatabaseUnknown, DatabaseUnknown),
    (DatabaseMismatch, DatabaseMismatch),
    (Config, ConfigError),
    (QueueFull, QueueFull),
    (ServerStopping, ServerStopping),
    (SchemaRequired, SchemaRequired),
    (SchemaLimit, SchemaLimit),
    (TxAlreadyOpen, TxAlreadyOpen),
    (NoTxOpen, NoTxOpen),
    (TxFailed, TxFailed),
    (ActiveTransaction, ActiveTransaction),
    (InvalidInput, InvalidInput),
    (DatabaseError, DatabaseError),
    (ResultLimit, ResultLimit),
    (CopyUnsupported, CopyUnsupported),
    (Cancelled, Cancelled),
    (DeadlineExceeded, DeadlineExceeded),
    (ConnectionLost, ConnectionLost),
    (CommitOutcomeUnknown, CommitOutcomeUnknown),
    (CleanupUncertain, CleanupUncertain),
    (Internal, Internal),
);

/// The only currently supported uncertain operation outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionOutcome {
    #[serde(rename = "unknown")]
    Unknown,
}

/// Error body in a version-1 envelope.  Its message is always the static
/// template associated with `code`; SQLSTATE is independent diagnostic data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorPayload {
    pub code: ErrorCode,
    pub message: String,
    pub sqlstate: Option<String>,
    pub transaction_outcome: Option<TransactionOutcome>,
}

pub type ProtocolErrorPayload = ErrorPayload;

impl ErrorPayload {
    pub fn new(code: ErrorCode) -> Self {
        Self {
            code,
            message: code.message().to_owned(),
            sqlstate: None,
            transaction_outcome: None,
        }
    }

    pub fn message(&self) -> &'static str {
        self.code.message()
    }

    pub fn with_sqlstate(self, sqlstate: &str) -> Result<Self, ValidationError> {
        self.try_with_sqlstate(Some(sqlstate))
    }

    pub fn try_with_sqlstate(mut self, sqlstate: Option<&str>) -> Result<Self, ValidationError> {
        self.sqlstate = sqlstate.map(str::to_owned);
        validate_sqlstate(self.sqlstate.as_deref())?;
        Ok(self)
    }

    pub fn with_unknown_outcome(mut self) -> Self {
        self.transaction_outcome = Some(TransactionOutcome::Unknown);
        self
    }

    pub fn is_outcome_unknown(&self) -> bool {
        self.transaction_outcome == Some(TransactionOutcome::Unknown)
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.message != self.code.message() {
            return Err(ValidationError::StaticErrorMessage);
        }
        validate_sqlstate(self.sqlstate.as_deref())
    }
}

impl Serialize for ErrorPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut out = serializer.serialize_struct("ErrorPayload", 4)?;
        out.serialize_field("code", &self.code)?;
        out.serialize_field("message", self.code.message())?;
        out.serialize_field("sqlstate", &self.sqlstate)?;
        out.serialize_field("transaction_outcome", &self.transaction_outcome)?;
        out.end()
    }
}

impl<'de> Deserialize<'de> for ErrorPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| D::Error::custom("error payload must be an object"))?;
        exact_object_keys(
            object,
            &["code", "message", "sqlstate", "transaction_outcome"],
        )
        .map_err(D::Error::custom)?;
        let code: ErrorCode = decode_value(object, "code").map_err(D::Error::custom)?;
        let message: String = decode_value(object, "message").map_err(D::Error::custom)?;
        let sqlstate: Option<String> =
            decode_value(object, "sqlstate").map_err(D::Error::custom)?;
        let transaction_outcome: Option<TransactionOutcome> =
            decode_value(object, "transaction_outcome").map_err(D::Error::custom)?;
        if message != code.message() {
            return Err(D::Error::custom(
                "error message does not match its static code template",
            ));
        }
        validate_sqlstate(sqlstate.as_deref()).map_err(D::Error::custom)?;
        Ok(Self {
            code,
            message,
            sqlstate,
            transaction_outcome,
        })
    }
}

/// An envelope contains exactly one of `result` and `error`.  The custom
/// serializer omits the absent outcome, while always emitting nullable
/// `handle_state`, `sqlstate`, and `transaction_outcome` fields where required.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope<T = Value> {
    pub envelope_version: u8,
    pub handle_state: Option<HandleState>,
    pub next_moves: Vec<Move>,
    pub result: Option<T>,
    pub error: Option<ErrorPayload>,
}

impl<T> Envelope<T> {
    pub fn success(handle_state: Option<HandleState>, next_moves: Vec<Move>, result: T) -> Self {
        Self {
            envelope_version: ENVELOPE_VERSION,
            handle_state,
            next_moves,
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(
        handle_state: Option<HandleState>,
        next_moves: Vec<Move>,
        error: ErrorPayload,
    ) -> Self {
        Self {
            envelope_version: ENVELOPE_VERSION,
            handle_state,
            next_moves,
            result: None,
            error: Some(error),
        }
    }

    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.envelope_version != ENVELOPE_VERSION
            || self.result.is_some() == self.error.is_some()
        {
            return Err(ValidationError::InvalidEnvelope);
        }
        if let Some(state) = &self.handle_state {
            state.validate()?;
        }
        for movement in &self.next_moves {
            movement.validate()?;
        }
        if let Some(error) = &self.error {
            error.validate()?;
        }
        Ok(())
    }
}

impl<T: Serialize> Serialize for Envelope<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(serde::ser::Error::custom)?;
        use serde::ser::SerializeStruct;
        let fields = 4;
        let mut out = serializer.serialize_struct("Envelope", fields)?;
        out.serialize_field("envelope_version", &self.envelope_version)?;
        out.serialize_field("handle_state", &self.handle_state)?;
        out.serialize_field("next_moves", &self.next_moves)?;
        if let Some(result) = &self.result {
            out.serialize_field("result", result)?;
        } else if let Some(error) = &self.error {
            out.serialize_field("error", error)?;
        }
        out.end()
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Envelope<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| D::Error::custom("envelope must be an object"))?;
        for key in ["envelope_version", "handle_state", "next_moves"] {
            if !object.contains_key(key) {
                return Err(D::Error::custom(format!("missing field `{key}`")));
            }
        }
        allowed_object_keys(
            object,
            &[
                "envelope_version",
                "handle_state",
                "next_moves",
                "result",
                "error",
            ],
        )
        .map_err(D::Error::custom)?;
        let envelope_version: u8 =
            decode_value(object, "envelope_version").map_err(D::Error::custom)?;
        let handle_state: Option<HandleState> =
            decode_value(object, "handle_state").map_err(D::Error::custom)?;
        let next_moves: Vec<Move> = decode_value(object, "next_moves").map_err(D::Error::custom)?;
        let result_value = object.get("result");
        let error_value = object.get("error");
        if result_value.is_some_and(|value| !value.is_null())
            && error_value.is_none_or(Value::is_null)
        {
            let result_value = result_value.expect("checked above");
            let result: T =
                serde_json::from_value(result_value.clone()).map_err(D::Error::custom)?;
            let envelope = Self {
                envelope_version,
                handle_state,
                next_moves,
                result: Some(result),
                error: None,
            };
            envelope.validate().map_err(D::Error::custom)?;
            return Ok(envelope);
        }
        if result_value.is_none_or(Value::is_null)
            && error_value.is_some_and(|value| !value.is_null())
        {
            let error_value = error_value.expect("checked above");
            let error: ErrorPayload =
                serde_json::from_value(error_value.clone()).map_err(D::Error::custom)?;
            let envelope = Self {
                envelope_version,
                handle_state,
                next_moves,
                result: None,
                error: Some(error),
            };
            envelope.validate().map_err(D::Error::custom)?;
            return Ok(envelope);
        }
        Err(D::Error::custom(
            "envelope must contain exactly one result or error",
        ))
    }
}

/// Text/null parameters sent through `PQsendQueryParams`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Parameter {
    pub type_oid: Option<u32>,
    pub value: Option<String>,
}

impl Parameter {
    pub fn new(type_oid: Option<u32>, value: Option<String>) -> Result<Self, ValidationError> {
        let parameter = Self { type_oid, value };
        parameter.validate()?;
        Ok(parameter)
    }

    pub fn inferred(value: impl Into<String>) -> Result<Self, ValidationError> {
        Self::new(None, Some(value.into()))
    }

    pub fn null(type_oid: Option<u32>) -> Self {
        Self {
            type_oid,
            value: None,
        }
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        if self
            .value
            .as_deref()
            .is_some_and(|value| value.contains('\0'))
        {
            return Err(ValidationError::EmbeddedNul);
        }
        Ok(())
    }

    pub fn value_bytes(&self) -> usize {
        self.value.as_ref().map_or(0, String::len)
    }
}

impl<'de> Deserialize<'de> for Parameter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| D::Error::custom("parameter must be an object"))?;
        exact_object_keys(object, &["type_oid", "value"]).map_err(D::Error::custom)?;
        let type_oid: Option<u32> = decode_value(object, "type_oid").map_err(D::Error::custom)?;
        let value: Option<String> = decode_value(object, "value").map_err(D::Error::custom)?;
        let parameter = Self { type_oid, value };
        parameter.validate().map_err(D::Error::custom)?;
        Ok(parameter)
    }
}

/// Query tool arguments.  SQL statement-boundary enforcement remains a libpq
/// adapter concern; this type checks only textual input and configured caps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default)]
    pub parameters: Vec<Parameter>,
}

impl QueryRequest {
    pub fn validate(&self, config: &Config) -> Result<(), ValidationError> {
        self.validate_with_limits(config.sql_bytes, config.parameters, config.parameter_bytes)
    }

    pub fn validate_with_limits(
        &self,
        sql_bytes: usize,
        parameter_limit: usize,
        parameter_bytes: usize,
    ) -> Result<(), ValidationError> {
        if self.sql.trim().is_empty() || self.sql.contains('\0') || self.sql.len() > sql_bytes {
            return Err(ValidationError::InvalidQuery);
        }
        if self.parameters.len() > parameter_limit {
            return Err(ValidationError::ParameterCountLimit);
        }
        let mut total = 0usize;
        for parameter in &self.parameters {
            parameter.validate()?;
            total = total
                .checked_add(parameter.value_bytes())
                .ok_or(ValidationError::ParameterByteLimit)?;
            if total > parameter_bytes {
                return Err(ValidationError::ParameterByteLimit);
            }
        }
        Ok(())
    }

    pub fn parameter_bytes(&self) -> Result<usize, ValidationError> {
        self.parameters.iter().try_fold(0usize, |total, parameter| {
            parameter.validate()?;
            total
                .checked_add(parameter.value_bytes())
                .ok_or(ValidationError::ParameterByteLimit)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnFormat {
    #[serde(rename = "text")]
    Text,
}

/// Result column metadata; values are never inferred into JSON numbers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultColumn {
    pub name: String,
    pub type_oid: u32,
    pub format: ColumnFormat,
}

pub type Column = ResultColumn;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenDatabaseResult {
    pub handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListHandlesResult {
    pub handles: Vec<HandleState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpenedTransaction {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenTransactionResult {
    pub opened: OpenedTransaction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitResult {
    pub committed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackResult {
    pub rolled_back: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseResult {
    pub disposed: bool,
    pub cleanup_verified: bool,
    pub transaction_outcome: Option<TransactionOutcome>,
}

impl ResultColumn {
    pub fn text(name: impl Into<String>, type_oid: u32) -> Self {
        Self {
            name: name.into(),
            type_oid,
            format: ColumnFormat::Text,
        }
    }
}

/// Query results in the exact version-1 public shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryResult {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tag: String,
    pub affected_rows: Option<String>,
}

impl QueryResult {
    pub fn new(
        columns: Vec<ResultColumn>,
        rows: Vec<Vec<Option<String>>>,
        command_tag: impl Into<String>,
        affected_rows: Option<String>,
    ) -> Self {
        Self {
            columns,
            rows,
            command_tag: command_tag.into(),
            affected_rows,
        }
    }

    pub fn validate(&self, config: &Config) -> Result<(), ValidationError> {
        if self.columns.len() > config.columns || self.rows.len() > config.result_rows {
            return Err(ValidationError::ResultLimit);
        }
        if !is_decimal_or_none(self.affected_rows.as_deref()) {
            return Err(ValidationError::InvalidAffectedRows);
        }
        for column in &self.columns {
            check_cell(&column.name, config.cell_bytes)?;
        }
        check_cell(&self.command_tag, config.cell_bytes)?;
        if let Some(affected_rows) = &self.affected_rows {
            check_cell(affected_rows, config.cell_bytes)?;
        }
        for row in &self.rows {
            if row.len() != self.columns.len() {
                return Err(ValidationError::InvalidResultShape);
            }
            for value in row.iter().flatten() {
                check_cell(value, config.cell_bytes)?;
            }
        }
        let value = serde_json::to_value(self)?;
        if canonical_json_bytes(&value)?.len() > config.result_json_bytes {
            return Err(ValidationError::ResultLimit);
        }
        Ok(())
    }

    pub fn canonical_json(&self) -> Result<String, ValidationError> {
        Ok(canonical_json(&serde_json::to_value(self)?)?)
    }
}

/// A bounded role-visible schema response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaResult {
    pub schemas: Vec<SchemaInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaInfo {
    pub name: String,
    pub relations: Vec<Relation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationKind {
    #[serde(rename = "table")]
    Table,
    #[serde(rename = "partitioned_table")]
    PartitionedTable,
    #[serde(rename = "view")]
    View,
    #[serde(rename = "materialized_view")]
    MaterializedView,
    #[serde(rename = "foreign_table")]
    ForeignTable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Relation {
    pub name: String,
    pub kind: RelationKind,
    pub columns: Vec<SchemaColumn>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaColumn {
    pub name: String,
    pub ordinal: u32,
    pub type_oid: u32,
    pub type_name: String,
    pub nullable: bool,
    pub default_expression: Option<String>,
    pub identity: IdentityKind,
    pub generated: bool,
}

pub type ColumnInfo = SchemaColumn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdentityKind {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "always")]
    Always,
    #[serde(rename = "by_default")]
    ByDefault,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Constraint {
    pub name: String,
    pub kind: ConstraintKind,
    pub definition: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintKind {
    #[serde(rename = "primary_key")]
    PrimaryKey,
    #[serde(rename = "unique")]
    Unique,
    #[serde(rename = "foreign_key")]
    ForeignKey,
    #[serde(rename = "check")]
    Check,
    #[serde(rename = "exclusion")]
    Exclusion,
}

impl SchemaResult {
    pub fn validate(&self, config: &Config) -> Result<(), ValidationError> {
        let mut units = 0usize;
        for schema in &self.schemas {
            units = checked_unit(units)?;
            check_schema_cell(&schema.name, config.cell_bytes)?;
            if units > config.result_rows {
                return Err(ValidationError::SchemaLimit);
            }
            for relation in &schema.relations {
                units = checked_unit(units)?;
                check_schema_cell(&relation.name, config.cell_bytes)?;
                for column in &relation.columns {
                    units = checked_unit(units)?;
                    check_schema_cell(&column.name, config.cell_bytes)?;
                    check_schema_cell(&column.type_name, config.cell_bytes)?;
                    if let Some(default) = &column.default_expression {
                        check_schema_cell(default, config.cell_bytes)?;
                    }
                }
                for constraint in &relation.constraints {
                    units = checked_unit(units)?;
                    check_schema_cell(&constraint.name, config.cell_bytes)?;
                    check_schema_cell(&constraint.definition, config.cell_bytes)?;
                }
                if units > config.result_rows {
                    return Err(ValidationError::SchemaLimit);
                }
            }
        }
        let value = serde_json::to_value(self)?;
        if canonical_json_bytes(&value)?.len() > config.result_json_bytes {
            return Err(ValidationError::SchemaLimit);
        }
        Ok(())
    }

    pub fn canonical_json(&self) -> Result<String, ValidationError> {
        Ok(canonical_json(&serde_json::to_value(self)?)?)
    }
}

fn checked_unit(units: usize) -> Result<usize, ValidationError> {
    units.checked_add(1).ok_or(ValidationError::SchemaLimit)
}

fn check_cell(value: &str, limit: usize) -> Result<(), ValidationError> {
    if value.len() > limit {
        Err(ValidationError::ResultLimit)
    } else {
        Ok(())
    }
}

fn check_schema_cell(value: &str, limit: usize) -> Result<(), ValidationError> {
    if value.len() > limit {
        Err(ValidationError::SchemaLimit)
    } else {
        Ok(())
    }
}

fn is_decimal_text(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_decimal_or_none(value: Option<&str>) -> bool {
    value.is_none() || value.is_some_and(is_decimal_text)
}

fn validate_sqlstate(value: Option<&str>) -> Result<(), ValidationError> {
    if value.is_some_and(|state| {
        state.len() != 5 || !state.bytes().all(|byte| byte.is_ascii_alphanumeric())
    }) {
        Err(ValidationError::InvalidSqlState)
    } else {
        Ok(())
    }
}

/// Return a recursively canonicalized compact JSON encoding.  Object keys are
/// ordered by their UTF-8 bytes; array order and scalar spellings are kept.
pub fn canonical_json(value: &Value) -> Result<String, serde_json::Error> {
    let canonical = canonicalize(value);
    serde_json::to_string(&canonical)
}

pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    let canonical = canonicalize(value);
    serde_json::to_vec(&canonical)
}

pub fn canonicalize_json(value: &Value) -> Value {
    canonicalize(value)
}

/// Canonical byte length of a query result subobject, excluding its outer
/// envelope.  The worker uses this before retaining rows.
pub fn canonical_result_bytes<T: Serialize>(result: &T) -> Result<usize, serde_json::Error> {
    Ok(canonical_json_bytes(&serde_json::to_value(result)?)?.len())
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut keys: Vec<&String> = object.keys().collect();
            keys.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
            let mut sorted = Map::new();
            for key in keys {
                sorted.insert(key.clone(), canonicalize(&object[key]));
            }
            Value::Object(sorted)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize).collect()),
        scalar => scalar.clone(),
    }
}

/// Errors from public protocol validation.  They are intentionally not
/// backend diagnostics and can be mapped to `ErrorCode::InvalidInput` or a
/// configured-limit code by the tool layer.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("invalid envelope")]
    InvalidEnvelope,
    #[error("inconsistent handle state")]
    InconsistentState,
    #[error("invalid lifecycle move")]
    InvalidMove,
    #[error("error message is not the checked-in static template")]
    StaticErrorMessage,
    #[error("SQLSTATE must be exactly five ASCII alphanumeric characters")]
    InvalidSqlState,
    #[error("parameter contains an embedded NUL")]
    EmbeddedNul,
    #[error("query must contain non-empty UTF-8 SQL within the configured byte limit")]
    InvalidQuery,
    #[error("parameter count exceeds the configured limit")]
    ParameterCountLimit,
    #[error("parameter byte total exceeds the configured limit")]
    ParameterByteLimit,
    #[error("result affected_rows is not decimal text")]
    InvalidAffectedRows,
    #[error("result rows do not match the column count")]
    InvalidResultShape,
    #[error("result exceeds configured limits")]
    ResultLimit,
    #[error("schema response exceeds configured limits")]
    SchemaLimit,
    #[error("JSON serialization failed: {0}")]
    Serialization(String),
}

impl From<serde_json::Error> for ValidationError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config::default()
    }

    #[test]
    fn status_serialization_and_nullable_open_projection_are_exact() {
        assert_eq!(
            serde_json::to_string(&ConnectionStatus::Ok).unwrap(),
            "\"ok\""
        );
        assert_eq!(
            serde_json::to_string(&ConnectionStatus::Bad).unwrap(),
            "\"bad\""
        );
        assert_eq!(
            serde_json::to_string(&ConnectionStatus::Closed).unwrap(),
            "\"closed\""
        );
        assert_eq!(
            serde_json::to_string(&TransactionStatus::InTransaction).unwrap(),
            "\"in_transaction\""
        );
        assert_eq!(TransactionStatus::Idle.transaction_open(), Some(false));
        assert_eq!(TransactionStatus::InError.transaction_open(), Some(true));
        assert_eq!(TransactionStatus::Active.transaction_open(), None);
        assert_eq!(TransactionStatus::Unknown.transaction_open(), None);
    }

    #[test]
    fn handle_state_closed_snapshot_is_exact() {
        let state = HandleState::closed("h", "db");
        assert_eq!(state.transaction_open, None);
        assert!(state.validate().is_ok());
        assert_eq!(
            serde_json::to_value(state).unwrap(),
            serde_json::json!({
                "handle":"h", "database":"db", "connection_status":"closed",
                "transaction_status":"unknown", "transaction_open":null,
                "busy":false, "schema_observed":false
            })
        );
    }

    #[test]
    fn canonical_json_sorts_nested_objects_by_utf8_bytes_and_is_compact() {
        let value = serde_json::json!({
            "z": {"é": 1, "a": "x\n", "中": null},
            "a": [ {"b": 2, "a": true} ]
        });
        assert_eq!(
            canonical_json(&value).unwrap(),
            "{\"a\":[{\"a\":true,\"b\":2}],\"z\":{\"a\":\"x\\n\",\"é\":1,\"中\":null}}"
        );
        assert_eq!(
            canonical_json_bytes(&value).unwrap().len(),
            canonical_json(&value).unwrap().len()
        );
    }

    #[test]
    fn parameter_contract_accepts_nulls_and_oid_zero_but_rejects_nul() {
        let null = serde_json::from_str::<Parameter>(r#"{"type_oid":null,"value":null}"#).unwrap();
        assert_eq!(null, Parameter::null(None));
        let explicit_zero = Parameter::new(Some(0), Some("text".into())).unwrap();
        assert_eq!(explicit_zero.type_oid, Some(0));
        assert!(
            serde_json::from_str::<Parameter>(r#"{"type_oid":null,"value":"a\u0000b"}"#).is_err()
        );
        assert!(serde_json::from_str::<Parameter>(r#"{"type_oid":null}"#).is_err());
        assert!(
            serde_json::from_str::<Parameter>(r#"{"type_oid":null,"value":null,"extra":1}"#)
                .is_err()
        );
    }

    #[test]
    fn parameter_budget_checks_exact_limit_over_limit_many_small_and_null() {
        let exact = QueryRequest {
            sql: "SELECT $1, $2, $3".into(),
            parameters: vec![
                Parameter::inferred("ab").unwrap(),
                Parameter::inferred("cd").unwrap(),
                Parameter::null(None),
            ],
        };
        assert!(exact.validate_with_limits(100, 3, 4).is_ok());
        assert!(exact.validate_with_limits(100, 3, 3).is_err());
        assert!(exact.validate_with_limits(100, 2, 4).is_err());
        assert_eq!(exact.parameter_bytes().unwrap(), 4);
    }

    #[test]
    fn query_validation_rejects_empty_nul_oversize_and_default_parameters_are_empty() {
        let default_request: QueryRequest = serde_json::from_str(r#"{"sql":"SELECT 1"}"#).unwrap();
        assert!(default_request.parameters.is_empty());
        assert!(
            QueryRequest {
                sql: "  ".into(),
                parameters: vec![]
            }
            .validate(&config())
            .is_err()
        );
        assert!(
            QueryRequest {
                sql: "SELECT \u{0}".into(),
                parameters: vec![]
            }
            .validate(&config())
            .is_err()
        );
        assert!(
            QueryRequest {
                sql: "12345".into(),
                parameters: vec![]
            }
            .validate_with_limits(4, 0, 0)
            .is_err()
        );
    }

    #[test]
    fn result_contract_preserves_strings_nulls_metadata_and_shape() {
        let result = QueryResult::new(
            vec![
                ResultColumn::text("answer", 23),
                ResultColumn::text("none", 25),
            ],
            vec![vec![Some("42".into()), None]],
            "SELECT 1",
            Some("1".into()),
        );
        assert!(result.validate(&config()).is_ok());
        let value = serde_json::to_value(&result).unwrap();
        assert_eq!(value["rows"][0][0], "42");
        assert!(value["rows"][0][1].is_null());
        assert_eq!(value["columns"][0]["format"], "text");
        assert!(
            QueryResult {
                rows: vec![vec![Some("x".into())]],
                ..result.clone()
            }
            .validate(&config())
            .is_err()
        );
        assert!(
            QueryResult {
                affected_rows: Some("not-count".into()),
                ..result
            }
            .validate(&config())
            .is_err()
        );
    }

    #[test]
    fn result_json_budget_counts_escaping_metadata_and_exact_boundary() {
        let result = QueryResult::new(
            vec![ResultColumn::text("é", 25)],
            vec![vec![Some("line\n☃".into())]],
            "SELECT",
            None,
        );
        let bytes = result.canonical_json().unwrap().len();
        let mut exact = config();
        exact.result_json_bytes = bytes;
        assert!(result.validate(&exact).is_ok());
        exact.result_json_bytes = bytes - 1;
        assert!(result.validate(&exact).is_err());
    }

    #[test]
    fn schema_budget_counts_all_units_and_metadata_json() {
        let schema = SchemaResult {
            schemas: vec![SchemaInfo {
                name: "public".into(),
                relations: vec![Relation {
                    name: "items".into(),
                    kind: RelationKind::Table,
                    columns: vec![SchemaColumn {
                        name: "id".into(),
                        ordinal: 1,
                        type_oid: 23,
                        type_name: "integer".into(),
                        nullable: false,
                        default_expression: None,
                        identity: IdentityKind::None,
                        generated: false,
                    }],
                    constraints: vec![],
                }],
            }],
        };
        let mut bounded = config();
        bounded.result_rows = 3; // schema + relation + column
        assert!(schema.validate(&bounded).is_ok());
        bounded.result_rows = 2;
        assert!(schema.validate(&bounded).is_err());
    }

    #[test]
    fn every_static_error_code_has_exact_message_and_wire_code() {
        assert_eq!(ErrorCode::ALL.len(), 24);
        for code in ErrorCode::ALL {
            let error = ErrorPayload::new(code);
            assert_eq!(error.message, code.message());
            let value = serde_json::to_value(&error).unwrap();
            assert_eq!(value["code"], code.code());
            assert_eq!(value["message"], code.message());
            assert!(value["sqlstate"].is_null());
            assert!(value["transaction_outcome"].is_null());
        }
    }

    #[test]
    fn golden_error_fixture_matches_every_wire_code_and_message() {
        let fixture: std::collections::BTreeMap<String, String> =
            serde_json::from_str(include_str!("../tests/error_messages.json")).unwrap();
        assert_eq!(fixture.len(), ErrorCode::ALL.len());
        for code in ErrorCode::ALL {
            assert_eq!(
                fixture.get(code.code()).map(String::as_str),
                Some(code.message())
            );
        }
    }

    #[test]
    fn sqlstate_is_optional_exactly_five_ascii_alphanumeric_and_uncertainty_is_explicit() {
        let error = ErrorPayload::new(ErrorCode::DatabaseError)
            .with_sqlstate("42601")
            .unwrap()
            .with_unknown_outcome();
        assert_eq!(error.sqlstate.as_deref(), Some("42601"));
        assert!(error.is_outcome_unknown());
        for invalid in ["", "1234", "123456", "12-45", "é2345"] {
            assert!(
                ErrorPayload::new(ErrorCode::DatabaseError)
                    .with_sqlstate(invalid)
                    .is_err()
            );
        }
    }

    #[test]
    fn envelope_has_exactly_one_outcome_and_rejects_missing_or_unknown_fields() {
        let state = HandleState::idle("h", "db", false);
        let envelope = Envelope::success(
            Some(state.clone()),
            next_moves(&state),
            serde_json::json!({"handle":"h"}),
        );
        assert!(!envelope.is_error());
        let encoded = serde_json::to_string(&envelope).unwrap();
        assert!(encoded.contains("\"result\""));
        assert!(!encoded.contains("\"error\""));
        let decoded: Envelope<Value> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, envelope);
        assert!(serde_json::from_str::<Envelope<Value>>(
            r#"{"envelope_version":1,"handle_state":null,"next_moves":[],"result":{},"error":{}}"#
        ).is_err());
        assert!(
            serde_json::from_str::<Envelope<Value>>(
                r#"{"envelope_version":1,"handle_state":null,"next_moves":[]}"#
            )
            .is_err()
        );
        assert!(serde_json::from_str::<Envelope<Value>>(
            r#"{"envelope_version":1,"handle_state":null,"next_moves":[],"result":{},"extra":1}"#
        ).is_err());
    }

    #[test]
    fn canonical_move_matrix_matches_plan_and_close_exception() {
        let idle_unobserved = HandleState::idle("h", "db", false);
        assert_eq!(
            next_moves(&idle_unobserved)
                .iter()
                .map(|m| m.tool.as_str())
                .collect::<Vec<_>>(),
            ["get_schema", "close_database"]
        );
        let idle_observed = HandleState::idle("h", "db", true);
        assert_eq!(
            next_moves(&idle_observed)
                .iter()
                .map(|m| m.tool.as_str())
                .collect::<Vec<_>>(),
            ["open_read", "open_write", "get_schema", "close_database"]
        );
        let in_tx = HandleState::new(
            "h",
            "db",
            ConnectionStatus::Ok,
            TransactionStatus::InTransaction,
            false,
            true,
        );
        assert_eq!(
            next_moves(&in_tx)
                .iter()
                .map(|m| m.tool.as_str())
                .collect::<Vec<_>>(),
            ["query", "commit", "rollback"]
        );
        assert_eq!(
            close_refusal_moves(&in_tx)
                .iter()
                .map(|m| m.tool.as_str())
                .collect::<Vec<_>>(),
            ["commit", "rollback"]
        );
        let in_error = HandleState::new(
            "h",
            "db",
            ConnectionStatus::Ok,
            TransactionStatus::InError,
            false,
            true,
        );
        assert_eq!(
            next_moves(&in_error)
                .iter()
                .map(|m| m.tool.as_str())
                .collect::<Vec<_>>(),
            ["rollback"]
        );
        let active = HandleState::new(
            "h",
            "db",
            ConnectionStatus::Ok,
            TransactionStatus::Active,
            true,
            true,
        );
        assert!(next_moves(&active).is_empty());
        let bad = HandleState::new(
            "h",
            "db",
            ConnectionStatus::Bad,
            TransactionStatus::InTransaction,
            false,
            true,
        );
        assert_eq!(
            next_moves(&bad)
                .iter()
                .map(|m| m.tool.as_str())
                .collect::<Vec<_>>(),
            ["close_database"]
        );
        let closed = HandleState::closed("h", "db");
        assert!(next_moves(&closed).is_empty());
    }

    #[test]
    fn inconsistent_states_have_no_guessed_moves() {
        let mut state = HandleState::idle("h", "db", false);
        state.transaction_open = Some(true);
        assert!(moves_for_state(&state, false).is_err());
        assert!(next_moves(&state).is_empty());
    }

    #[test]
    fn process_moves_are_empty_and_move_arguments_are_handle_only() {
        assert!(process_moves().is_empty());
        let state = HandleState::idle("h", "db", false);
        let movement = next_moves(&state).remove(0);
        assert_eq!(movement.arguments, serde_json::json!({"handle":"h"}));
        assert_eq!(movement.guidance, SCHEMA_GUIDANCE);
        assert_eq!(movement.tool, "get_schema");
    }

    #[test]
    fn unknown_input_fields_are_rejected_across_public_objects() {
        assert!(serde_json::from_str::<HandleState>(
            r#"{"handle":"h","database":"d","connection_status":"ok","transaction_status":"idle","transaction_open":false,"busy":false,"schema_observed":false,"x":1}"#
        ).is_err());
        assert!(
            serde_json::from_str::<Move>(r#"{"tool":"x","arguments":{},"guidance":"g","x":1}"#)
                .is_err()
        );
        assert!(
            serde_json::from_str::<QueryResult>(
                r#"{"columns":[],"rows":[],"command_tag":"","affected_rows":null,"x":1}"#
            )
            .is_err()
        );
    }
}
