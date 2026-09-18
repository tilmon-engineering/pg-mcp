//! Private, worker-owned libpq adapter.
//!
//! This module is deliberately the only place in the core crate that contains
//! FFI or operating-system polling.  A `Connection` is an exclusive owner of a
//! `PGconn`; it must not be shared or used concurrently.  All data returned by
//! libpq is copied before its owning `PGresult` is cleared.  No backend error
//! text is exposed: SQLSTATE is retained only when it came from a real result
//! and passed the five-character validation below.

use std::ffi::{CStr, CString};
use std::fmt;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::{self, NonNull};
use std::time::{Duration, Instant};

use crate::protocol::{QueryResult as PublicQueryResult, ResultColumn};
use pq_sys as ffi;

const SERVER_VERSION_MINIMUM: c_int = 120_000;
const POLL_SLICE: Duration = Duration::from_millis(25);
const PG_DIAG_SQLSTATE: c_int = b'C' as c_int;

/// A request's cancellation/deadline/shutdown state.  The worker supplies this
/// trait; production code cannot replace the libpq implementation with a fake.
pub(crate) trait PollControl {
    fn shutdown_requested(&self) -> bool;
    fn cancelled(&self) -> bool;
    fn cancel_drain_grace(&self) -> Duration;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CancelReason {
    Shutdown,
    Cancelled,
    Deadline,
}

impl CancelReason {
    fn error(self) -> AdapterError {
        match self {
            Self::Shutdown => AdapterError::new(ErrorClass::ServerStopping),
            Self::Cancelled => AdapterError::new(ErrorClass::Cancelled),
            Self::Deadline => AdapterError::new(ErrorClass::DeadlineExceeded),
        }
    }
}

/// The complete application-side retention budget for one query result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryLimits {
    pub sql_bytes: usize,
    pub parameters: usize,
    pub parameter_bytes: usize,
    pub result_rows: usize,
    pub columns: usize,
    pub cell_bytes: usize,
    pub result_json_bytes: usize,
}

/// A text-format PostgreSQL parameter. `None` value is SQL NULL; a `None` OID
/// asks PostgreSQL to infer the type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryParameter {
    pub type_oid: Option<u32>,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Column {
    pub name: String,
    pub type_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tag: String,
    pub affected_rows: Option<String>,
}

/// Classify completed PostgreSQL command tags for schema-cache invalidation.
/// This deliberately recognizes only direct catalog-changing commands. Tags
/// produced by trusted code, dynamic SQL, triggers/rules, and SELECT-tagged
/// CREATE TABLE AS / SELECT INTO remain documented blind spots.
pub(crate) fn direct_ddl_command_tag(tag: &str) -> bool {
    let command = tag.split_ascii_whitespace().next().unwrap_or_default();
    if matches!(
        command,
        "SELECT"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "MERGE"
            | "VALUES"
            | "FETCH"
            | "MOVE"
            | "SET"
            | "SHOW"
            | "BEGIN"
            | "COMMIT"
            | "ROLLBACK"
            | "ABORT"
            | "END"
            | "SAVEPOINT"
            | "RELEASE"
            | "DISCARD"
            | "NOTIFY"
            | "LISTEN"
            | "UNLISTEN"
    ) {
        return false;
    }
    matches!(
        command,
        "CREATE"
            | "ALTER"
            | "DROP"
            | "TRUNCATE"
            | "REINDEX"
            | "GRANT"
            | "REVOKE"
            | "COMMENT"
            | "REFRESH"
            | "SECURITY"
            | "DO"
            | "CALL"
            | "EXECUTE"
    ) || !tag.is_empty()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionStatus {
    Ok,
    Bad,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionStatus {
    Idle,
    Active,
    InTransaction,
    InError,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StatusSnapshot {
    pub connection_status: ConnectionStatus,
    pub transaction_status: TransactionStatus,
    pub busy: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorClass {
    Config,
    InvalidInput,
    Database,
    DatabaseMismatch,
    ConnectionLost,
    UnsupportedServer,
    ResultLimit,
    CopyUnsupported,
    Cancelled,
    DeadlineExceeded,
    ServerStopping,
    Internal,
}

impl ErrorClass {
    #[allow(dead_code)]
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::Config => "CONFIG_ERROR",
            Self::InvalidInput => "INVALID_INPUT",
            Self::Database => "DATABASE_ERROR",
            Self::DatabaseMismatch => "DATABASE_MISMATCH",
            Self::ConnectionLost => "CONNECTION_LOST",
            Self::UnsupportedServer => "UNSUPPORTED_SERVER",
            Self::ResultLimit => "RESULT_LIMIT",
            Self::CopyUnsupported => "COPY_UNSUPPORTED",
            Self::Cancelled => "CANCELLED",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::ServerStopping => "SERVER_STOPPING",
            Self::Internal => "INTERNAL",
        }
    }

    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::Config => "Invalid server configuration.",
            Self::InvalidInput => "Invalid query or parameters.",
            Self::Database => "PostgreSQL rejected the operation.",
            Self::DatabaseMismatch => "Connected database does not match configuration.",
            Self::ConnectionLost => "Connection lost; operation outcome may be unknown.",
            Self::UnsupportedServer => "PostgreSQL server 12 or newer is required.",
            Self::ResultLimit => {
                "Result exceeds configured limits; execution may have taken effect."
            }
            Self::CopyUnsupported => "COPY streaming is unsupported; connection disposed.",
            Self::Cancelled => "Request cancelled; execution may have taken effect.",
            Self::DeadlineExceeded => "Request deadline exceeded; execution may have taken effect.",
            Self::ServerStopping => "Server is stopping.",
            Self::Internal => "Internal adapter failure.",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdapterError {
    pub class: ErrorClass,
    pub sqlstate: Option<String>,
    /// True only when a request was sent and completion was not established.
    pub outcome_unknown: bool,
}

impl AdapterError {
    fn new(class: ErrorClass) -> Self {
        Self {
            class,
            sqlstate: None,
            outcome_unknown: false,
        }
    }

    fn with_sqlstate(mut self, sqlstate: Option<String>) -> Self {
        self.sqlstate = sqlstate;
        self
    }

    fn unknown(mut self) -> Self {
        self.outcome_unknown = true;
        self
    }
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.class.message())
    }
}

impl std::error::Error for AdapterError {}

/// The operator-owned connection string after libpq has parsed it.  The parsed
/// `dbname` is authoritative; callers must pass this exact database name.
#[derive(Debug, Clone)]
pub(crate) struct Dsn {
    raw: CString,
    database: String,
}

impl Dsn {
    pub(crate) fn parse(value: &str) -> Result<Self, AdapterError> {
        let raw = CString::new(value).map_err(|_| AdapterError::new(ErrorClass::Config))?;
        let mut parse_error: *mut c_char = ptr::null_mut();
        // SAFETY: `raw` is NUL-terminated and remains alive while libpq reads
        // it.  The returned array and optional error are released below using
        // their documented libpq deallocators.
        let options = unsafe { ffi::PQconninfoParse(raw.as_ptr(), &mut parse_error) };
        if !parse_error.is_null() {
            // SAFETY: PQconninfoParse allocated this pointer for PQfreemem.
            unsafe { ffi::PQfreemem(parse_error.cast::<c_void>()) };
        }
        let options = NonNull::new(options).ok_or_else(|| AdapterError::new(ErrorClass::Config))?;
        let options = ConninfoOptions(options);
        let mut database: Option<String> = None;
        let mut saw_dbname = false;
        let mut saw_service = false;
        // PQconninfoOption is terminated by a record whose keyword is NULL.
        // We never retain any pointers into this array after it is freed.
        let mut index = 0usize;
        loop {
            // SAFETY: libpq returns a contiguous, NULL-terminated array.  The
            // index is bounded by the first null keyword and is advanced only
            // after reading one in-bounds record.
            let option = unsafe { options.0.as_ptr().add(index).as_ref() };
            let Some(option) = option else {
                return Err(AdapterError::new(ErrorClass::Config));
            };
            if option.keyword.is_null() {
                break;
            }
            let keyword =
                copy_cstr(option.keyword).ok_or_else(|| AdapterError::new(ErrorClass::Config))?;
            if keyword.eq_ignore_ascii_case("service") && !option.val.is_null() {
                saw_service = true;
            }
            if keyword.eq_ignore_ascii_case("dbname") {
                saw_dbname = true;
                if let Some(value) = copy_cstr(option.val)
                    && !value.is_empty()
                {
                    database = Some(value);
                }
            }
            index = index
                .checked_add(1)
                .ok_or_else(|| AdapterError::new(ErrorClass::Config))?;
        }
        if saw_service || !saw_dbname {
            return Err(AdapterError::new(ErrorClass::Config));
        }
        let database = database.ok_or_else(|| AdapterError::new(ErrorClass::Config))?;
        if nested_connection_string(&database) {
            return Err(AdapterError::new(ErrorClass::Config));
        }
        Ok(Self { raw, database })
    }

    pub(crate) fn database(&self) -> &str {
        &self.database
    }

    pub(crate) fn as_cstr(&self) -> &CStr {
        &self.raw
    }
}

fn nested_connection_string(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.starts_with("postgres://")
        || lower.starts_with("postgresql://")
        || value.contains('=')
        || value.contains('\n')
        || value.contains('\r')
}

struct ConninfoOptions(NonNull<ffi::PQconninfoOption>);

impl Drop for ConninfoOptions {
    fn drop(&mut self) {
        // SAFETY: this pointer came from PQconninfoParse and is freed exactly
        // once by its matching libpq function.
        unsafe { ffi::PQconninfoFree(self.0.as_ptr()) }
    }
}

/// A connection owner.  It contains no synchronization and must stay on its
/// dedicated worker thread.  The `Send` impl transfers exclusive ownership
/// between setup and worker execution; no alias or shared pointer is exposed.
pub(crate) struct Connection {
    conn: Option<ConnOwner>,
    busy: bool,
}

// SAFETY: `Connection` has exclusive ownership of its pointer and all methods
// require `&mut self`; moving that ownership to a worker cannot create aliases.
unsafe impl Send for Connection {}

impl Connection {
    pub(crate) fn connect(
        dsn: &Dsn,
        deadline: Instant,
        control: &dyn PollControl,
    ) -> Result<Self, AdapterError> {
        check_control(control, deadline)?;
        // SAFETY: dsn's CString is alive for the complete PQconnectStart call.
        let conn = unsafe { ffi::PQconnectStart(dsn.as_cstr().as_ptr()) };
        let owner =
            NonNull::new(conn).ok_or_else(|| AdapterError::new(ErrorClass::ConnectionLost))?;
        let mut connection = Self {
            conn: Some(ConnOwner(owner)),
            busy: false,
        };
        // This must happen before the first PQconnectPoll so libpq cannot emit
        // notices to its default stderr processor.
        // SAFETY: owner is valid and callback is a static, non-panicking no-op.
        unsafe {
            ffi::PQsetNoticeProcessor(connection.raw_mut(), Some(discard_notice), ptr::null_mut());
        }
        loop {
            check_control(control, deadline)?;
            // SAFETY: the owner is valid until Connection/ConnOwner drops.
            let state = unsafe { ffi::PQconnectPoll(connection.raw_mut()) };
            check_control(control, deadline)?;
            match state {
                ffi::PostgresPollingStatusType::PGRES_POLLING_OK => break,
                ffi::PostgresPollingStatusType::PGRES_POLLING_FAILED => {
                    return Err(AdapterError::new(ErrorClass::ConnectionLost));
                }
                ffi::PostgresPollingStatusType::PGRES_POLLING_ACTIVE => continue,
                ffi::PostgresPollingStatusType::PGRES_POLLING_READING => {
                    wait_connection(connection.raw(), true, false, deadline)?;
                }
                ffi::PostgresPollingStatusType::PGRES_POLLING_WRITING => {
                    wait_connection(connection.raw(), false, true, deadline)?;
                }
            }
        }
        check_control(control, deadline)?;
        connection.set_nonblocking()?;
        connection.setup_utf8(deadline, control)?;
        check_control(control, deadline)?;
        let observation = connection.status();
        if observation.connection_status != ConnectionStatus::Ok {
            return Err(AdapterError::new(ErrorClass::ConnectionLost));
        }
        if observation.transaction_status != TransactionStatus::Idle {
            return Err(AdapterError::new(ErrorClass::Database));
        }
        let actual = connection
            .database_name()
            .ok_or_else(|| AdapterError::new(ErrorClass::Database))?;
        if actual.as_bytes() != dsn.database.as_bytes() {
            return Err(AdapterError::new(ErrorClass::DatabaseMismatch));
        }
        if connection.server_version() < SERVER_VERSION_MINIMUM {
            return Err(AdapterError::new(ErrorClass::UnsupportedServer));
        }
        Ok(connection)
    }

    fn set_nonblocking(&mut self) -> Result<(), AdapterError> {
        // SAFETY: `self.raw_mut()` is the live PGconn owned by self.
        let result = unsafe { ffi::PQsetnonblocking(self.raw_mut(), 1) };
        if result == 0 {
            Ok(())
        } else {
            Err(AdapterError::new(ErrorClass::ConnectionLost))
        }
    }

    fn setup_utf8(
        &mut self,
        deadline: Instant,
        control: &dyn PollControl,
    ) -> Result<(), AdapterError> {
        let result = self.send_and_collect(
            b"SET client_encoding TO 'UTF8'",
            &[],
            false,
            deadline,
            control,
        )?;
        if result.command_tag != "SET" {
            return Err(AdapterError::new(ErrorClass::Database));
        }
        let encoding = CString::new("client_encoding").expect("literal has no NUL");
        // SAFETY: encoding is NUL-terminated and self owns the PGconn.
        let value = unsafe { ffi::PQparameterStatus(self.raw(), encoding.as_ptr()) };
        let value = copy_cstr(value);
        if value.as_deref() != Some("UTF8") {
            return Err(AdapterError::new(ErrorClass::Database));
        }
        Ok(())
    }

    pub(crate) fn query(
        &mut self,
        sql: &str,
        parameters: &[QueryParameter],
        limits: QueryLimits,
        deadline: Instant,
        control: &dyn PollControl,
    ) -> Result<ResultSet, AdapterError> {
        self.send_query(sql, parameters, limits, deadline, control)
    }

    pub(crate) fn execute(
        &mut self,
        sql: &str,
        deadline: Instant,
        control: &dyn PollControl,
    ) -> Result<ResultSet, AdapterError> {
        self.send_and_collect(sql.as_bytes(), &[], false, deadline, control)
    }

    pub(crate) fn status(&self) -> StatusSnapshot {
        let Some(conn) = self.conn.as_ref() else {
            return StatusSnapshot {
                connection_status: ConnectionStatus::Closed,
                transaction_status: TransactionStatus::Unknown,
                busy: false,
            };
        };
        // SAFETY: the pointer is valid for the duration of these read-only
        // accessors because ConnOwner is held by self.
        let connection_status = unsafe {
            match ffi::PQstatus(conn.0.as_ptr()) {
                ffi::ConnStatusType::CONNECTION_OK => ConnectionStatus::Ok,
                _ => ConnectionStatus::Bad,
            }
        };
        // SAFETY: PQtransactionStatus is the sole transaction-state authority.
        let transaction_status =
            unsafe { map_transaction_status(ffi::PQtransactionStatus(conn.0.as_ptr())) };
        StatusSnapshot {
            connection_status,
            transaction_status,
            busy: self.busy,
        }
    }

    pub(crate) fn database_name(&self) -> Option<String> {
        let conn = self.conn.as_ref()?;
        // SAFETY: PQdb returns libpq-owned storage valid until connection finish.
        unsafe { cstr_utf8(ffi::PQdb(conn.0.as_ptr())) }
    }

    pub(crate) fn server_version(&self) -> c_int {
        let Some(conn) = self.conn.as_ref() else {
            return 0;
        };
        // SAFETY: conn is valid and the accessor does not retain pointers.
        unsafe { ffi::PQserverVersion(conn.0.as_ptr()) }
    }

    pub(crate) fn cancel_and_drain(
        &mut self,
        reason: CancelReason,
        deadline: Instant,
    ) -> Result<bool, AdapterError> {
        let dispatched = self.dispatch_cancel(reason, deadline)?;
        // Dispatch success is not completion. Always drain the original
        // connection before permitting another command on it.
        if self.drain_until_idle(deadline).is_err() {
            self.dispose();
            return Err(reason.error().unknown());
        }
        if dispatched {
            Ok(true)
        } else {
            Err(reason.error().unknown())
        }
    }

    /// Drive only libpq's independent cancellation connection. The caller must
    /// still drain the original PGconn to final NULL before reuse or publication.
    fn dispatch_cancel(
        &mut self,
        reason: CancelReason,
        deadline: Instant,
    ) -> Result<bool, AdapterError> {
        let Some(conn) = self.conn.as_ref() else {
            return Ok(false);
        };
        // SAFETY: cancel handle is created from this worker-owned PGconn and is
        // not used concurrently with the original connection.
        let cancel = unsafe { ffi::PQcancelCreate(conn.0.as_ptr()) };
        let Some(cancel) = NonNull::new(cancel) else {
            self.dispose();
            return Err(reason.error().unknown());
        };
        let cancel = CancelOwner(cancel);
        // SAFETY: cancel is valid and exclusively owned here.
        if unsafe { ffi::PQcancelStart(cancel.0.as_ptr()) } == 0 {
            drop(cancel);
            self.dispose();
            return Err(reason.error().unknown());
        }
        // PQcancelStart starts the write phase.  Treat the first wait as
        // writable, then reacquire the descriptor after every PQcancelPoll.
        let mut poll_state = ffi::PostgresPollingStatusType::PGRES_POLLING_WRITING;
        let dispatched = loop {
            if Instant::now() >= deadline {
                drop(cancel);
                self.dispose();
                return Err(reason.error().unknown());
            }
            let (read, write) = match poll_state {
                ffi::PostgresPollingStatusType::PGRES_POLLING_READING => (true, false),
                ffi::PostgresPollingStatusType::PGRES_POLLING_WRITING => (false, true),
                ffi::PostgresPollingStatusType::PGRES_POLLING_ACTIVE => (false, false),
                ffi::PostgresPollingStatusType::PGRES_POLLING_OK => break true,
                ffi::PostgresPollingStatusType::PGRES_POLLING_FAILED => break false,
            };
            if read || write {
                // Reacquire the descriptor for every wait.  libpq permits it to
                // change after PQcancelPoll, so a cached value is not safe.
                let socket = unsafe { ffi::PQcancelSocket(cancel.0.as_ptr()) };
                if socket < 0 || poll_fd(socket, read, write, deadline).is_err() {
                    drop(cancel);
                    self.dispose();
                    return Err(reason.error().unknown());
                }
            }
            // SAFETY: cancel remains valid and owned by this loop.
            poll_state = unsafe { ffi::PQcancelPoll(cancel.0.as_ptr()) };
        };
        drop(cancel);
        Ok(dispatched)
    }

    fn send_query(
        &mut self,
        sql: &str,
        parameters: &[QueryParameter],
        limits: QueryLimits,
        deadline: Instant,
        control: &dyn PollControl,
    ) -> Result<ResultSet, AdapterError> {
        if sql.is_empty() || sql.len() > limits.sql_bytes || sql.as_bytes().contains(&0) {
            return Err(AdapterError::new(ErrorClass::InvalidInput));
        }
        validate_parameters(parameters, limits)?;
        let command = CString::new(sql).map_err(|_| AdapterError::new(ErrorClass::InvalidInput))?;
        let mut values = Vec::with_capacity(parameters.len());
        let mut type_oids = Vec::with_capacity(parameters.len());
        for parameter in parameters {
            type_oids.push(parameter.type_oid.unwrap_or(0));
            values.push(parameter.value.as_deref().map(|value| {
                // Parameter validation checked NUL before this allocation.
                CString::new(value).expect("validated parameter")
            }));
        }
        let pointers: Vec<*const c_char> = values
            .iter()
            .map(|value| value.as_ref().map_or(ptr::null(), |value| value.as_ptr()))
            .collect();
        let nparams = c_int::try_from(parameters.len())
            .map_err(|_| AdapterError::new(ErrorClass::InvalidInput))?;
        check_control(control, deadline)?;
        let Some(conn) = self.conn.as_ref() else {
            return Err(AdapterError::new(ErrorClass::ConnectionLost));
        };
        // SAFETY: all arrays and CStrings stay alive until PQsendQueryParams
        // returns; libpq copies the request into its output buffer.
        let sent = unsafe {
            ffi::PQsendQueryParams(
                conn.0.as_ptr(),
                command.as_ptr(),
                nparams,
                if type_oids.is_empty() {
                    ptr::null()
                } else {
                    type_oids.as_ptr()
                },
                if pointers.is_empty() {
                    ptr::null()
                } else {
                    pointers.as_ptr()
                },
                ptr::null(),
                ptr::null(),
                0,
            )
        };
        if sent == 0 {
            return Err(AdapterError::new(ErrorClass::Database));
        }
        self.busy = true;
        // This must be called immediately after a successful send and before
        // retrieving results.  Falling back would silently lose streaming.
        // SAFETY: conn remains valid and no result has been retrieved yet.
        if unsafe { ffi::PQsetSingleRowMode(conn.0.as_ptr()) } != 1 {
            let _ = self.cancel_and_drain(CancelReason::Cancelled, deadline);
            return Err(AdapterError::new(ErrorClass::Internal).unknown());
        }
        let result = self.collect_results(limits, deadline, control);
        self.busy = false;
        result
    }

    fn send_and_collect(
        &mut self,
        sql: &[u8],
        parameters: &[QueryParameter],
        single_row: bool,
        deadline: Instant,
        control: &dyn PollControl,
    ) -> Result<ResultSet, AdapterError> {
        let sql =
            std::str::from_utf8(sql).map_err(|_| AdapterError::new(ErrorClass::InvalidInput))?;
        let command = CString::new(sql).map_err(|_| AdapterError::new(ErrorClass::InvalidInput))?;
        validate_parameters(
            parameters,
            QueryLimits {
                sql_bytes: sql.len(),
                parameters: usize::MAX,
                parameter_bytes: usize::MAX,
                result_rows: usize::MAX,
                columns: usize::MAX,
                cell_bytes: usize::MAX,
                result_json_bytes: usize::MAX,
            },
        )?;
        let values: Vec<Option<CString>> = parameters
            .iter()
            .map(|parameter| parameter.value.as_deref().map(CString::new).transpose())
            .collect::<Result<_, _>>()
            .map_err(|_| AdapterError::new(ErrorClass::InvalidInput))?;
        let pointers: Vec<*const c_char> = values
            .iter()
            .map(|value| value.as_ref().map_or(ptr::null(), |value| value.as_ptr()))
            .collect();
        let type_oids: Vec<ffi::Oid> = parameters.iter().map(|p| p.type_oid.unwrap_or(0)).collect();
        let nparams = c_int::try_from(parameters.len())
            .map_err(|_| AdapterError::new(ErrorClass::InvalidInput))?;
        check_control(control, deadline)?;
        let Some(conn) = self.conn.as_ref() else {
            return Err(AdapterError::new(ErrorClass::ConnectionLost));
        };
        // SAFETY: borrowed arrays remain alive through the libpq call.
        let sent = unsafe {
            ffi::PQsendQueryParams(
                conn.0.as_ptr(),
                command.as_ptr(),
                nparams,
                if type_oids.is_empty() {
                    ptr::null()
                } else {
                    type_oids.as_ptr()
                },
                if pointers.is_empty() {
                    ptr::null()
                } else {
                    pointers.as_ptr()
                },
                ptr::null(),
                ptr::null(),
                0,
            )
        };
        if sent == 0 {
            return Err(AdapterError::new(ErrorClass::Database));
        }
        self.busy = true;
        if single_row {
            // SAFETY: called before any result retrieval.
            if unsafe { ffi::PQsetSingleRowMode(conn.0.as_ptr()) } != 1 {
                let _ = self.cancel_and_drain(CancelReason::Cancelled, deadline);
                return Err(AdapterError::new(ErrorClass::Internal).unknown());
            }
        }
        let result = self.collect_results(
            QueryLimits {
                sql_bytes: sql.len(),
                parameters: usize::MAX,
                parameter_bytes: usize::MAX,
                result_rows: usize::MAX,
                columns: usize::MAX,
                cell_bytes: usize::MAX,
                result_json_bytes: usize::MAX,
            },
            deadline,
            control,
        );
        self.busy = false;
        result
    }

    fn collect_results(
        &mut self,
        limits: QueryLimits,
        deadline: Instant,
        control: &dyn PollControl,
    ) -> Result<ResultSet, AdapterError> {
        self.flush_output(deadline, control)?;
        let mut collector = ResultCollector::new(limits);
        let mut first_error: Option<AdapterError> = None;
        let mut drain_deadline = deadline;
        let mut cancellation_dispatched = false;
        loop {
            // A request cancellation/deadline after send does not permit us to
            // abandon PGconn result draining. Record it, then keep consuming to
            // final NULL so busy/status publication remains authoritative.
            if first_error.is_none() {
                let reason = if control.shutdown_requested() {
                    Some(CancelReason::Shutdown)
                } else if control.cancelled() {
                    Some(CancelReason::Cancelled)
                } else if Instant::now() >= deadline {
                    Some(CancelReason::Deadline)
                } else {
                    None
                };
                if let Some(reason) = reason {
                    first_error = Some(reason.error().unknown());
                    drain_deadline = Instant::now() + control.cancel_drain_grace();
                    if !cancellation_dispatched {
                        match self.dispatch_cancel(reason, drain_deadline) {
                            Ok(_) => cancellation_dispatched = true,
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            let conn = self.raw_mut();
            if conn.is_null() {
                return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
            }
            // SAFETY: consumeInput only mutates the owned PGconn.
            if unsafe { ffi::PQconsumeInput(conn) } == 0 {
                self.busy = false;
                return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
            }
            // SAFETY: PQisBusy is valid after PQconsumeInput.
            if unsafe { ffi::PQisBusy(conn) } != 0 {
                let socket = unsafe { ffi::PQsocket(conn) };
                if socket < 0 {
                    self.dispose();
                    return Err(first_error.unwrap_or_else(|| {
                        AdapterError::new(ErrorClass::ConnectionLost).unknown()
                    }));
                }
                if poll_fd(socket, true, false, drain_deadline).is_err() {
                    self.dispose();
                    return Err(first_error.unwrap_or_else(|| {
                        AdapterError::new(ErrorClass::ConnectionLost).unknown()
                    }));
                }
                continue;
            }
            // Once not busy, retrieve every result immediately until NULL.
            loop {
                // SAFETY: result belongs to this connection and is owned by the
                // ResultOwner until the loop iteration ends.
                let result = unsafe { ffi::PQgetResult(conn) };
                let Some(result) = NonNull::new(result) else {
                    self.busy = false;
                    if let Some(error) = first_error {
                        return Err(error);
                    }
                    return collector.finish();
                };
                let result = ResultOwner(result);
                match self.consume_result(&mut collector, &result) {
                    Ok(()) => {}
                    Err(error) => {
                        let is_copy = error.class == ErrorClass::CopyUnsupported;
                        first_error.get_or_insert(error);
                        if is_copy {
                            drop(result);
                            self.dispose();
                            return Err(AdapterError::new(ErrorClass::CopyUnsupported).unknown());
                        }
                    }
                }
            }
        }
    }

    fn consume_result(
        &self,
        collector: &mut ResultCollector,
        result: &ResultOwner,
    ) -> Result<(), AdapterError> {
        // SAFETY: result is live for this call and all accessors are read-only.
        let status = unsafe { ffi::PQresultStatus(result.0.as_ptr()) };
        match status {
            ffi::ExecStatusType::PGRES_SINGLE_TUPLE | ffi::ExecStatusType::PGRES_TUPLES_CHUNK => {
                collector.consume_rows(result)
            }
            // Single-row mode ends with TUPLES_OK. It carries the final command
            // tag and affected-row text even though the data rows arrived as
            // PGRES_SINGLE_TUPLE results.
            ffi::ExecStatusType::PGRES_TUPLES_OK => {
                collector.consume_rows(result)?;
                collector.consume_command(result)
            }
            ffi::ExecStatusType::PGRES_COMMAND_OK => collector.consume_command(result),
            ffi::ExecStatusType::PGRES_COPY_IN
            | ffi::ExecStatusType::PGRES_COPY_OUT
            | ffi::ExecStatusType::PGRES_COPY_BOTH => {
                Err(AdapterError::new(ErrorClass::CopyUnsupported))
            }
            ffi::ExecStatusType::PGRES_FATAL_ERROR
            | ffi::ExecStatusType::PGRES_BAD_RESPONSE
            | ffi::ExecStatusType::PGRES_NONFATAL_ERROR => {
                let sqlstate = result_sqlstate(result);
                Err(AdapterError::new(ErrorClass::Database).with_sqlstate(sqlstate))
            }
            ffi::ExecStatusType::PGRES_EMPTY_QUERY => {
                Err(AdapterError::new(ErrorClass::InvalidInput))
            }
            _ => Err(AdapterError::new(ErrorClass::Database)),
        }
    }

    fn flush_output(
        &mut self,
        deadline: Instant,
        control: &dyn PollControl,
    ) -> Result<(), AdapterError> {
        loop {
            check_control(control, deadline)?;
            let Some(conn) = self.conn.as_ref() else {
                return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
            };
            // SAFETY: conn is owned and valid.
            match unsafe { ffi::PQflush(conn.0.as_ptr()) } {
                0 => return Ok(()),
                1 => {
                    let socket = unsafe { ffi::PQsocket(conn.0.as_ptr()) };
                    if socket < 0 {
                        return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
                    }
                    // Readability is serviced before the next flush because a
                    // server can send traffic while client output is pending.
                    let ready = poll_fd(socket, true, true, deadline)?;
                    if ready.readable {
                        // SAFETY: conn remains owned by self and the descriptor
                        // readiness was observed for this connection.
                        if unsafe { ffi::PQconsumeInput(conn.0.as_ptr()) } == 0 {
                            return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
                        }
                    }
                }
                _ => return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown()),
            }
        }
    }

    fn drain_until_idle(&mut self, deadline: Instant) -> Result<(), AdapterError> {
        loop {
            let Some(conn) = self.conn.as_ref() else {
                return Ok(());
            };
            // SAFETY: owned connection and nonblocking mode.
            if unsafe { ffi::PQconsumeInput(conn.0.as_ptr()) } == 0 {
                return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
            }
            if unsafe { ffi::PQisBusy(conn.0.as_ptr()) } != 0 {
                let socket = unsafe { ffi::PQsocket(conn.0.as_ptr()) };
                if socket < 0 {
                    return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
                }
                poll_fd(socket, true, false, deadline)?;
                continue;
            }
            loop {
                // SAFETY: clear every PGresult before reusing the connection.
                let result = unsafe { ffi::PQgetResult(conn.0.as_ptr()) };
                let Some(result) = NonNull::new(result) else {
                    return Ok(());
                };
                drop(ResultOwner(result));
            }
        }
    }

    fn raw(&self) -> *const ffi::PGconn {
        self.conn
            .as_ref()
            .map_or(ptr::null(), |owner| owner.0.as_ptr())
    }

    fn raw_mut(&mut self) -> *mut ffi::PGconn {
        self.conn
            .as_mut()
            .map_or(ptr::null_mut(), |owner| owner.0.as_ptr())
    }

    fn dispose(&mut self) {
        let _ = self.conn.take();
        self.busy = false;
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // ConnOwner's Drop performs the one and only PQfinish.
        let _ = self.conn.take();
    }
}

struct ConnOwner(NonNull<ffi::PGconn>);

impl Drop for ConnOwner {
    fn drop(&mut self) {
        // SAFETY: ConnOwner is created only from a non-null PQconnectStart
        // result and is dropped exactly once.
        unsafe { ffi::PQfinish(self.0.as_ptr()) }
    }
}

struct ResultOwner(NonNull<ffi::PGresult>);

impl Drop for ResultOwner {
    fn drop(&mut self) {
        // SAFETY: ResultOwner is created only from a non-null PQgetResult result
        // and is cleared exactly once.
        unsafe { ffi::PQclear(self.0.as_ptr()) }
    }
}

struct CancelOwner(NonNull<ffi::PGcancelConn>);

impl Drop for CancelOwner {
    fn drop(&mut self) {
        // SAFETY: CancelOwner is created only from a non-null PQcancelCreate
        // result and is finished exactly once.
        unsafe { ffi::PQcancelFinish(self.0.as_ptr()) }
    }
}

struct ResultCollector {
    limits: QueryLimits,
    columns: Vec<Column>,
    rows: Vec<Vec<Option<String>>>,
    command_tag: Option<String>,
    affected_rows: Option<String>,
}

impl ResultCollector {
    fn new(limits: QueryLimits) -> Self {
        Self {
            limits,
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: None,
            affected_rows: None,
        }
    }

    fn consume_rows(&mut self, result: &ResultOwner) -> Result<(), AdapterError> {
        // SAFETY: result remains live while all values are copied.
        let fields = unsafe { ffi::PQnfields(result.0.as_ptr()) };
        let rows = unsafe { ffi::PQntuples(result.0.as_ptr()) };
        if fields < 0 || rows < 0 {
            return Err(AdapterError::new(ErrorClass::Database));
        }
        let fields =
            usize::try_from(fields).map_err(|_| AdapterError::new(ErrorClass::Internal))?;
        let rows = usize::try_from(rows).map_err(|_| AdapterError::new(ErrorClass::Internal))?;
        if fields > self.limits.columns {
            return Err(AdapterError::new(ErrorClass::ResultLimit));
        }
        if self.columns.is_empty() && fields > 0 {
            for field in 0..fields {
                let field =
                    c_int::try_from(field).map_err(|_| AdapterError::new(ErrorClass::Internal))?;
                // SAFETY: field is in the range reported by PQnfields.
                let name = unsafe { cstr_utf8(ffi::PQfname(result.0.as_ptr(), field)) }
                    .ok_or_else(|| AdapterError::new(ErrorClass::Database))?;
                let column = Column {
                    name,
                    type_oid: unsafe { ffi::PQftype(result.0.as_ptr(), field) },
                };
                self.columns.push(column);
                if let Err(error) = self.ensure_candidate_fits() {
                    self.columns.pop();
                    return Err(error);
                }
            }
        } else if self.columns.len() != fields {
            return Err(AdapterError::new(ErrorClass::Database));
        }
        for row_index in 0..rows {
            if self.rows.len() >= self.limits.result_rows {
                return Err(AdapterError::new(ErrorClass::ResultLimit));
            }
            let mut row = Vec::with_capacity(fields);
            for field_index in 0..fields {
                let row_index = c_int::try_from(row_index)
                    .map_err(|_| AdapterError::new(ErrorClass::Internal))?;
                let field_index = c_int::try_from(field_index)
                    .map_err(|_| AdapterError::new(ErrorClass::Internal))?;
                // SAFETY: indices are bounded by the result metadata.
                let is_null =
                    unsafe { ffi::PQgetisnull(result.0.as_ptr(), row_index, field_index) } != 0;
                let value = if is_null {
                    None
                } else {
                    let length =
                        unsafe { ffi::PQgetlength(result.0.as_ptr(), row_index, field_index) };
                    if length < 0 {
                        return Err(AdapterError::new(ErrorClass::Database));
                    }
                    let length = usize::try_from(length)
                        .map_err(|_| AdapterError::new(ErrorClass::Internal))?;
                    if length > self.limits.cell_bytes {
                        return Err(AdapterError::new(ErrorClass::ResultLimit));
                    }
                    // SAFETY: libpq guarantees length bytes at PQgetvalue for a
                    // non-null cell; copy before ResultOwner clears the result.
                    let pointer =
                        unsafe { ffi::PQgetvalue(result.0.as_ptr(), row_index, field_index) };
                    if pointer.is_null() {
                        return Err(AdapterError::new(ErrorClass::Database));
                    }
                    let bytes = unsafe { std::slice::from_raw_parts(pointer.cast::<u8>(), length) };
                    let text = std::str::from_utf8(bytes)
                        .map_err(|_| AdapterError::new(ErrorClass::Database))?
                        .to_owned();
                    Some(text)
                };
                row.push(value);
            }
            self.rows.push(row);
            if let Err(error) = self.ensure_candidate_fits() {
                self.rows.pop();
                return Err(error);
            }
        }
        Ok(())
    }

    fn consume_command(&mut self, result: &ResultOwner) -> Result<(), AdapterError> {
        // SAFETY: result is live and command accessors return libpq-owned text.
        let tag = unsafe { cstr_utf8(ffi::PQcmdStatus(result.0.as_ptr())) }
            .ok_or_else(|| AdapterError::new(ErrorClass::Database))?;
        let previous_tag = self.command_tag.replace(tag);
        if let Err(error) = self.ensure_candidate_fits() {
            self.command_tag = previous_tag;
            return Err(error);
        }
        let affected = unsafe { cstr_utf8(ffi::PQcmdTuples(result.0.as_ptr())) }
            .ok_or_else(|| AdapterError::new(ErrorClass::Database))?;
        if !affected.is_empty() {
            if !affected.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(AdapterError::new(ErrorClass::Database));
            }
            let previous_affected = self.affected_rows.replace(affected);
            if let Err(error) = self.ensure_candidate_fits() {
                self.affected_rows = previous_affected;
                return Err(error);
            }
        }
        Ok(())
    }

    /// Validate the exact canonical public result object before retaining the
    /// candidate currently present in this collector. This includes all keys,
    /// commas, brackets, nulls, metadata, escaping, and UTF-8 bytes.
    fn ensure_candidate_fits(&self) -> Result<(), AdapterError> {
        let public = PublicQueryResult::new(
            self.columns
                .iter()
                .map(|column| ResultColumn::text(column.name.clone(), column.type_oid))
                .collect(),
            self.rows.clone(),
            self.command_tag.clone().unwrap_or_default(),
            self.affected_rows.clone(),
        );
        public
            .validate(&crate::config::Config {
                result_json_bytes: self.limits.result_json_bytes,
                result_rows: self.limits.result_rows,
                columns: self.limits.columns,
                cell_bytes: self.limits.cell_bytes,
                ..crate::config::Config::default()
            })
            .map_err(|_| AdapterError::new(ErrorClass::ResultLimit))
    }

    fn finish(self) -> Result<ResultSet, AdapterError> {
        Ok(ResultSet {
            columns: self.columns,
            rows: self.rows,
            command_tag: self.command_tag.unwrap_or_default(),
            affected_rows: self.affected_rows,
        })
    }
}

#[cfg(test)]
mod ddl_tests {
    use super::direct_ddl_command_tag;

    #[test]
    fn direct_ddl_classifier_cases() {
        for tag in [
            "CREATE TABLE",
            "ALTER TABLE",
            "DROP INDEX",
            "TRUNCATE TABLE",
            "REINDEX",
            "GRANT",
            "REVOKE",
            "COMMENT",
            "DO",
            "CALL",
            "EXECUTE",
        ] {
            assert!(direct_ddl_command_tag(tag), "expected dirty tag: {tag}");
        }
        for tag in [
            "SELECT",
            "SELECT 1",
            "INSERT 0 1",
            "UPDATE 1",
            "DELETE 1",
            "MERGE",
            "VALUES",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SET",
            "SHOW",
        ] {
            assert!(!direct_ddl_command_tag(tag), "expected row-only tag: {tag}");
        }
        assert!(!direct_ddl_command_tag("SELECT"));
        assert!(direct_ddl_command_tag("VACUUM"));
    }
}

fn validate_parameters(
    parameters: &[QueryParameter],
    limits: QueryLimits,
) -> Result<(), AdapterError> {
    if parameters.len() > limits.parameters {
        return Err(AdapterError::new(ErrorClass::InvalidInput));
    }
    let mut total = 0usize;
    for parameter in parameters {
        if let Some(value) = parameter.value.as_deref() {
            if value.as_bytes().contains(&0) {
                return Err(AdapterError::new(ErrorClass::InvalidInput));
            }
            total = total
                .checked_add(value.len())
                .ok_or_else(|| AdapterError::new(ErrorClass::InvalidInput))?;
        }
    }
    if total > limits.parameter_bytes {
        return Err(AdapterError::new(ErrorClass::InvalidInput));
    }
    Ok(())
}

fn cstr_utf8(pointer: *const c_char) -> Option<String> {
    if pointer.is_null() {
        return None;
    }
    // SAFETY: callers only pass libpq-owned NUL-terminated strings or pointers
    // returned from the same FFI call while their owner is live.  The bytes are
    // copied before this function returns, so no libpq pointer escapes.
    unsafe { CStr::from_ptr(pointer).to_str().ok().map(str::to_owned) }
}

fn copy_cstr(pointer: *const c_char) -> Option<String> {
    cstr_utf8(pointer)
}

fn result_sqlstate(result: &ResultOwner) -> Option<String> {
    // SAFETY: PQresultErrorField returns a result-owned pointer, valid until
    // ResultOwner drops; only the SQLSTATE field is retained.
    let pointer = unsafe { ffi::PQresultErrorField(result.0.as_ptr(), PG_DIAG_SQLSTATE) };
    let value = cstr_utf8(pointer)?;
    if value.len() == 5 && value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        Some(value)
    } else {
        None
    }
}

fn map_transaction_status(status: ffi::PGTransactionStatusType) -> TransactionStatus {
    match status {
        ffi::PGTransactionStatusType::PQTRANS_IDLE => TransactionStatus::Idle,
        ffi::PGTransactionStatusType::PQTRANS_ACTIVE => TransactionStatus::Active,
        ffi::PGTransactionStatusType::PQTRANS_INTRANS => TransactionStatus::InTransaction,
        ffi::PGTransactionStatusType::PQTRANS_INERROR => TransactionStatus::InError,
        ffi::PGTransactionStatusType::PQTRANS_UNKNOWN => TransactionStatus::Unknown,
    }
}

fn check_control(control: &dyn PollControl, deadline: Instant) -> Result<(), AdapterError> {
    if control.shutdown_requested() {
        Err(AdapterError::new(ErrorClass::ServerStopping))
    } else if control.cancelled() {
        Err(AdapterError::new(ErrorClass::Cancelled))
    } else if Instant::now() >= deadline {
        Err(AdapterError::new(ErrorClass::DeadlineExceeded))
    } else {
        Ok(())
    }
}

struct Ready {
    readable: bool,
}

fn wait_connection(
    conn: *const ffi::PGconn,
    read: bool,
    write: bool,
    deadline: Instant,
) -> Result<(), AdapterError> {
    if conn.is_null() {
        return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
    }
    // Refresh PQsocket at each connection-poll boundary.  The descriptor may
    // change while TLS/authentication setup progresses.
    let socket = unsafe { ffi::PQsocket(conn) };
    if socket < 0 {
        return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
    }
    poll_fd(socket, read, write, deadline).map(|_| ())
}

fn poll_fd(fd: c_int, read: bool, write: bool, deadline: Instant) -> Result<Ready, AdapterError> {
    if fd < 0 || (!read && !write) {
        return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
    }
    let mut pollfd = libc::pollfd {
        fd,
        events: (if read { libc::POLLIN } else { 0 }) | (if write { libc::POLLOUT } else { 0 }),
        revents: 0,
    };
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(AdapterError::new(ErrorClass::DeadlineExceeded).unknown());
        }
        let slice = remaining.min(POLL_SLICE);
        let millis = i32::try_from(slice.as_millis().max(1)).unwrap_or(25);
        // SAFETY: pollfd points to a valid initialized one-element array and
        // the timeout is bounded.  libc::poll does not retain the pointer.
        let result = unsafe { libc::poll(&mut pollfd, 1, millis) };
        if result > 0 {
            let error = pollfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL);
            if error != 0 {
                return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
            }
            return Ok(Ready {
                readable: pollfd.revents & libc::POLLIN != 0,
            });
        }
        if result == 0 {
            if Instant::now() >= deadline {
                return Err(AdapterError::new(ErrorClass::DeadlineExceeded).unknown());
            }
            continue;
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            pollfd.revents = 0;
            continue;
        }
        return Err(AdapterError::new(ErrorClass::ConnectionLost).unknown());
    }
}

unsafe extern "C" fn discard_notice(_arg: *mut c_void, _message: *const c_char) {}

/// Native-library check exposed to the factory without leaking build details
/// into protocol code.  build.rs performs the authoritative pkg-config check;
/// this runtime guard protects loadable installations and is intentionally
/// independent from the server version check.
pub(crate) fn runtime_libpq_version() -> Result<i32, AdapterError> {
    // SAFETY: PQlibVersion has no arguments and returns a process-global integer.
    let version = unsafe { ffi::PQlibVersion() };
    if version < 17_00_00 {
        Err(AdapterError::new(ErrorClass::Config))
    } else {
        Ok(version)
    }
}

pub(crate) struct LibpqFactory;

impl LibpqFactory {
    pub(crate) fn new() -> Result<Self, AdapterError> {
        runtime_libpq_version().map(|_| Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestControl {
        shutdown: AtomicBool,
        cancelled: AtomicBool,
    }

    impl PollControl for TestControl {
        fn shutdown_requested(&self) -> bool {
            self.shutdown.load(Ordering::Relaxed)
        }

        fn cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Relaxed)
        }

        fn cancel_drain_grace(&self) -> Duration {
            Duration::from_millis(10)
        }
    }

    #[test]
    fn transaction_status_maps_every_native_variant() {
        assert_eq!(
            map_transaction_status(ffi::PGTransactionStatusType::PQTRANS_IDLE),
            TransactionStatus::Idle
        );
        assert_eq!(
            map_transaction_status(ffi::PGTransactionStatusType::PQTRANS_ACTIVE),
            TransactionStatus::Active
        );
        assert_eq!(
            map_transaction_status(ffi::PGTransactionStatusType::PQTRANS_INTRANS),
            TransactionStatus::InTransaction
        );
        assert_eq!(
            map_transaction_status(ffi::PGTransactionStatusType::PQTRANS_INERROR),
            TransactionStatus::InError
        );
        assert_eq!(
            map_transaction_status(ffi::PGTransactionStatusType::PQTRANS_UNKNOWN),
            TransactionStatus::Unknown
        );
    }

    #[test]
    fn parameter_bytes_are_checked_before_cstring_construction() {
        let parameters = [
            QueryParameter {
                type_oid: None,
                value: Some("abc".to_owned()),
            },
            QueryParameter {
                type_oid: None,
                value: None,
            },
        ];
        let limits = QueryLimits {
            parameters: 2,
            parameter_bytes: 3,
            ..QueryLimits::unbounded_for_tests()
        };
        assert!(validate_parameters(&parameters, limits).is_ok());
        let limits = QueryLimits {
            parameters: 2,
            parameter_bytes: 2,
            ..QueryLimits::unbounded_for_tests()
        };
        assert_eq!(
            validate_parameters(&parameters, limits).unwrap_err().class,
            ErrorClass::InvalidInput
        );
    }

    #[test]
    fn embedded_nul_is_rejected_without_ffi() {
        let parameters = [QueryParameter {
            type_oid: None,
            value: Some("a\0b".to_owned()),
        }];
        let limits = QueryLimits {
            parameters: 1,
            parameter_bytes: 3,
            ..QueryLimits::unbounded_for_tests()
        };
        assert_eq!(
            validate_parameters(&parameters, limits).unwrap_err().class,
            ErrorClass::InvalidInput
        );
    }

    #[test]
    fn nested_database_values_are_not_authoritative() {
        assert!(nested_connection_string("postgresql://host/db"));
        assert!(nested_connection_string("host=other dbname=x"));
        assert!(!nested_connection_string("literal-db"));
    }

    #[test]
    fn result_budget_uses_exact_canonical_object_before_retention() {
        let mut collector = ResultCollector::new(QueryLimits {
            result_json_bytes: 1,
            ..QueryLimits::unbounded_for_tests()
        });
        collector.columns.push(Column {
            name: "quote\"é".to_owned(),
            type_oid: 25,
        });
        assert_eq!(
            collector.ensure_candidate_fits().unwrap_err().class,
            ErrorClass::ResultLimit
        );
        collector.columns.clear();
        collector.rows.push(vec![None]);
        assert_eq!(
            collector.ensure_candidate_fits().unwrap_err().class,
            ErrorClass::ResultLimit
        );
    }

    #[test]
    fn poll_control_precedence_is_shutdown_then_cancel_then_deadline() {
        let control = TestControl {
            shutdown: AtomicBool::new(true),
            cancelled: AtomicBool::new(true),
        };
        let deadline = Instant::now() + Duration::from_secs(1);
        assert_eq!(
            check_control(&control, deadline).unwrap_err().class,
            ErrorClass::ServerStopping
        );
        control.shutdown.store(false, Ordering::Relaxed);
        assert_eq!(
            check_control(&control, deadline).unwrap_err().class,
            ErrorClass::Cancelled
        );
    }

    #[test]
    fn resource_guards_accept_null_free_paths_without_double_release() {
        // Null handles are never constructed by production code.  This test
        // documents the guard invariant without requiring a live PostgreSQL.
        assert!(NonNull::<ffi::PGconn>::dangling().as_ptr() as usize != 0);
        assert!(NonNull::<ffi::PGresult>::dangling().as_ptr() as usize != 0);
        assert!(NonNull::<ffi::PGcancelConn>::dangling().as_ptr() as usize != 0);
    }

    impl QueryLimits {
        fn unbounded_for_tests() -> Self {
            Self {
                sql_bytes: usize::MAX,
                parameters: usize::MAX,
                parameter_bytes: usize::MAX,
                result_rows: usize::MAX,
                columns: usize::MAX,
                cell_bytes: usize::MAX,
                result_json_bytes: usize::MAX,
            }
        }
    }
}
