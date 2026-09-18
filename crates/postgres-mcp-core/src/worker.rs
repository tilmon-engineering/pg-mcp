//! Worker-owned PostgreSQL connections and registry lifecycle.
//!
//! `Core` is the public orchestration API used by the stdio server.  Each opened
//! database owns one dedicated OS thread and one bounded FIFO queue.  The worker
//! is the sole owner of the backend connection; caller futures only own a
//! one-shot receiver and may be dropped without stopping worker cleanup or state
//! publication.

use crate::config::{Config, ConfigFile};
use crate::libpq::PollControl;
use crate::protocol::{
    ConnectionStatus, CoreError, Envelope, ErrorCode, HandleState, Parameter, QueryRequest,
    QueryResult, SchemaResult, TransactionStatus,
};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU8, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

fn core_error(code: ErrorCode) -> CoreError {
    match code {
        ErrorCode::UnsupportedServer => CoreError::UnsupportedServer,
        ErrorCode::HandleUnknown => CoreError::HandleUnknown,
        ErrorCode::HandleLimit => CoreError::HandleLimit,
        ErrorCode::DatabaseUnknown => CoreError::DatabaseUnknown,
        ErrorCode::DatabaseMismatch => CoreError::DatabaseMismatch,
        ErrorCode::ConfigError => CoreError::Config,
        ErrorCode::QueueFull => CoreError::QueueFull,
        ErrorCode::ServerStopping => CoreError::ServerStopping,
        ErrorCode::SchemaRequired => CoreError::SchemaRequired,
        ErrorCode::SchemaLimit => CoreError::SchemaLimit,
        ErrorCode::TxAlreadyOpen => CoreError::TxAlreadyOpen,
        ErrorCode::NoTxOpen => CoreError::NoTxOpen,
        ErrorCode::TxFailed => CoreError::TxFailed,
        ErrorCode::ActiveTransaction => CoreError::ActiveTransaction,
        ErrorCode::InvalidInput => CoreError::InvalidInput,
        ErrorCode::DatabaseError => CoreError::DatabaseError,
        ErrorCode::ResultLimit => CoreError::ResultLimit,
        ErrorCode::CopyUnsupported => CoreError::CopyUnsupported,
        ErrorCode::Cancelled => CoreError::Cancelled,
        ErrorCode::DeadlineExceeded => CoreError::DeadlineExceeded,
        ErrorCode::ConnectionLost => CoreError::ConnectionLost,
        ErrorCode::CommitOutcomeUnknown => CoreError::CommitOutcomeUnknown,
        ErrorCode::CleanupUncertain => CoreError::CleanupUncertain,
        ErrorCode::Internal => CoreError::Internal,
    }
}

/// Registry lifecycle.  The transition is atomic with open/request admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryLifecycle {
    Running,
    Stopping,
    Stopped,
}

impl RegistryLifecycle {
    fn byte(self) -> u8 {
        match self {
            Self::Running => 0,
            Self::Stopping => 1,
            Self::Stopped => 2,
        }
    }

    #[allow(dead_code)]
    fn from_byte(value: u8) -> Self {
        match value {
            1 => Self::Stopping,
            2 => Self::Stopped,
            _ => Self::Running,
        }
    }
}

/// Authoritative backend transaction state.  Production values are converted
/// directly from `PQtransactionStatus`; no SQLSTATE-derived state is retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendTransactionStatus {
    Idle,
    Active,
    InTransaction,
    InError,
    Unknown,
}

impl BackendTransactionStatus {
    fn protocol(self) -> TransactionStatus {
        match self {
            Self::Idle => TransactionStatus::Idle,
            Self::Active => TransactionStatus::Active,
            Self::InTransaction => TransactionStatus::InTransaction,
            Self::InError => TransactionStatus::InError,
            Self::Unknown => TransactionStatus::Unknown,
        }
    }
}

/// Authoritative backend connection health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendConnectionStatus {
    Ok,
    Bad,
    Unknown,
}

impl BackendConnectionStatus {
    fn protocol(self) -> ConnectionStatus {
        match self {
            Self::Ok => ConnectionStatus::Ok,
            Self::Bad | Self::Unknown => ConnectionStatus::Bad,
        }
    }
}

/// Last native status observation published by a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendObservation {
    pub connection_status: BackendConnectionStatus,
    pub transaction_status: BackendTransactionStatus,
    pub busy: bool,
}

impl Default for BackendObservation {
    fn default() -> Self {
        Self {
            connection_status: BackendConnectionStatus::Ok,
            transaction_status: BackendTransactionStatus::Idle,
            busy: false,
        }
    }
}

/// Cancellation and deadline bridge passed to backend poll loops.
#[derive(Clone)]
pub struct RequestContext {
    pub cancellation: CancellationToken,
    pub shutdown: CancellationToken,
    pub deadline: Instant,
    pub cancel_drain_grace: Duration,
}

impl RequestContext {
    fn cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    fn stopping(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }
}

/// Backend operation output.  Backend implementations must include the final
/// status observation after draining all result messages.
#[derive(Debug)]
pub enum BackendResult {
    Opened {
        mode: &'static str,
        observation: BackendObservation,
    },
    Schema(SchemaResult, BackendObservation),
    Query(QueryResult, BackendObservation, bool),
    Committed(BackendObservation),
    RolledBack {
        changed: bool,
        observation: BackendObservation,
    },
    Closed {
        observation: BackendObservation,
        cleanup_verified: bool,
    },
}

/// Backend failure with status captured by the owning worker.
#[derive(Debug, Clone)]
pub struct BackendFailure {
    pub error: CoreError,
    pub observation: BackendObservation,
    pub sqlstate: Option<String>,
    pub outcome_unknown: bool,
}

/// A typed outer-core failure. Unlike a plain code, it preserves the most
/// recent authoritative handle snapshot and backend diagnostics when available
/// so the MCP adapter never has to classify Debug text or fabricate state.
#[derive(Debug, Clone)]
pub struct CoreFailure {
    pub error: CoreError,
    pub handle_state: Option<HandleState>,
    pub sqlstate: Option<String>,
    pub outcome_unknown: bool,
}

impl CoreFailure {
    fn process(error: CoreError) -> Self {
        Self {
            error,
            handle_state: None,
            sqlstate: None,
            outcome_unknown: error.outcome_unknown,
        }
    }

    fn for_handle(core: &Core, handle: &str, error: CoreError) -> Self {
        Self {
            error,
            handle_state: current_state(core, handle),
            sqlstate: None,
            outcome_unknown: error.outcome_unknown,
        }
    }

    fn from_backend(handle_state: Option<HandleState>, failure: BackendFailure) -> Self {
        Self {
            error: failure.error,
            handle_state,
            sqlstate: failure.sqlstate,
            outcome_unknown: failure.outcome_unknown,
        }
    }
}

impl From<CoreError> for CoreFailure {
    fn from(error: CoreError) -> Self {
        Self::process(error)
    }
}

impl BackendFailure {
    fn core_error(&self) -> CoreError {
        self.error
    }
}

/// Internal backend seam.  It is intentionally not re-exported from the crate;
/// fake implementations are only available to unit tests in this module.
pub trait Backend: Send + 'static {
    fn open_read(&mut self, context: &RequestContext) -> Result<BackendResult, BackendFailure>;
    fn open_write(&mut self, context: &RequestContext) -> Result<BackendResult, BackendFailure>;
    fn get_schema(
        &mut self,
        config: &Config,
        context: &RequestContext,
    ) -> Result<BackendResult, BackendFailure>;
    fn query(
        &mut self,
        request: &QueryRequest,
        config: &Config,
        context: &RequestContext,
    ) -> Result<BackendResult, BackendFailure>;
    fn commit(&mut self, context: &RequestContext) -> Result<BackendResult, BackendFailure>;
    fn rollback(&mut self, context: &RequestContext) -> Result<BackendResult, BackendFailure>;
    fn close(&mut self, context: &RequestContext) -> Result<BackendResult, BackendFailure>;
    fn observe(&mut self) -> BackendObservation;
    fn shutdown(&mut self, context: &RequestContext) -> ShutdownWorkerResult;
}

/// A backend together with the database selected by its resolved DSN.
///
/// The profile used to resolve the DSN is intentionally not part of this
/// value: profile names and PostgreSQL database names are independent.
pub struct ConnectedBackend {
    pub backend: Box<dyn Backend>,
    pub database: String,
}

/// Internal connection factory implemented by the libpq adapter.
pub trait BackendFactory: Send + Sync + 'static {
    fn connect(
        &self,
        dsn: &str,
        config: &Config,
        context: &RequestContext,
    ) -> Result<ConnectedBackend, BackendFailure>;
}

#[derive(Debug, Clone)]
struct PublishedHandle {
    id: String,
    database: String,
    observation: BackendObservation,
    schema_observed: bool,
    /// Application bookkeeping for successfully observed direct catalog DDL in
    /// the current native transaction. Native transaction status remains the
    /// authority for transaction state.
    schema_dirty: bool,
    closed: bool,
}

impl PublishedHandle {
    fn state(&self) -> HandleState {
        let transaction_status = if self.closed {
            TransactionStatus::Unknown
        } else {
            self.observation.transaction_status.protocol()
        };
        let connection_status = if self.closed {
            ConnectionStatus::Closed
        } else {
            self.observation.connection_status.protocol()
        };
        HandleState::new(
            self.id.clone(),
            self.database.clone(),
            connection_status,
            transaction_status,
            self.observation.busy,
            if self.closed {
                false
            } else {
                self.schema_observed
            },
        )
    }
}

struct WorkerEntry {
    published: PublishedHandle,
    profile: String,
    config: Config,
    sender: mpsc::Sender<Request>,
    join: Option<thread::JoinHandle<()>>,
    admitted: usize,
}

struct PendingOpen {
    profile: String,
    shutdown: CancellationToken,
}

struct RegistryState {
    lifecycle: RegistryLifecycle,
    handles: BTreeMap<String, WorkerEntry>,
    pending_opens: BTreeMap<String, PendingOpen>,
    next_handle: u64,
    frozen_report: Option<ShutdownReport>,
}

impl RegistryState {
    fn new() -> Self {
        Self {
            lifecycle: RegistryLifecycle::Running,
            handles: BTreeMap::new(),
            pending_opens: BTreeMap::new(),
            next_handle: 1,
            frozen_report: None,
        }
    }

    fn allocate_handle(&mut self) -> String {
        let handle = format!("h-{:016x}", self.next_handle);
        self.next_handle = self.next_handle.saturating_add(1);
        handle
    }
}

/// The core public API consumed by the binary and live tests.
#[derive(Clone)]
pub struct Core {
    config: Config,
    profiles: Option<Arc<ConfigFile>>,
    dsn: Arc<str>,
    registry: Arc<Mutex<RegistryState>>,
    lifecycle: Arc<AtomicU8>,
    shutdown: CancellationToken,
    factory: Arc<dyn BackendFactory>,
}

impl Core {
    /// Construct a production core backed by the private libpq adapter.
    pub fn new(config: Config, dsn: String) -> Result<Self, CoreError> {
        config
            .validate()
            .map_err(|_| core_error(ErrorCode::ConfigError))?;
        let factory =
            crate::libpq::LibpqFactory::new().map_err(|_| core_error(ErrorCode::ConfigError))?;
        Ok(Self::from_factory(config, dsn, Arc::new(factory)))
    }

    fn from_factory(config: Config, dsn: String, factory: Arc<dyn BackendFactory>) -> Self {
        Self {
            config,
            profiles: None,
            dsn: Arc::from(dsn),
            registry: Arc::new(Mutex::new(RegistryState::new())),
            lifecycle: Arc::new(AtomicU8::new(RegistryLifecycle::Running.byte())),
            shutdown: CancellationToken::new(),
            factory,
        }
    }

    /// Deterministic backend constructor available only to unit tests.
    #[cfg(test)]
    pub fn with_backend_factory(
        config: Config,
        dsn: impl Into<String>,
        factory: Arc<dyn BackendFactory>,
    ) -> Result<Self, CoreError> {
        config
            .validate()
            .map_err(|_| core_error(ErrorCode::ConfigError))?;
        Ok(Self::from_factory(config, dsn.into(), factory))
    }

    /// Open one configured profile. The profile name is not a DSN and is never
    /// passed to libpq as connection input.
    pub async fn open_database(
        &self,
        profile: &str,
        cancellation: CancellationToken,
    ) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        let (id, context) = self
            .reserve_open(profile, cancellation)
            .map_err(CoreFailure::from)?;
        let profile = profile.to_owned();
        let core = self.clone();
        let open_id = id.clone();
        match tokio::task::spawn_blocking(move || {
            core.connect_and_publish(open_id, profile, context)
        })
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.finish_open(&id);
                Err(CoreFailure::process(core_error(ErrorCode::Internal)))
            }
        }
    }

    pub fn new_with_profiles(config: ConfigFile) -> Result<Self, CoreError> {
        config
            .validate()
            .map_err(|_| core_error(ErrorCode::ConfigError))?;
        let base = config.base_config();
        let factory =
            crate::libpq::LibpqFactory::new().map_err(|_| core_error(ErrorCode::ConfigError))?;
        Ok(Self {
            config: base,
            profiles: Some(Arc::new(config)),
            dsn: Arc::from(""),
            registry: Arc::new(Mutex::new(RegistryState::new())),
            lifecycle: Arc::new(AtomicU8::new(RegistryLifecycle::Running.byte())),
            shutdown: CancellationToken::new(),
            factory: Arc::new(factory),
        })
    }

    fn reserve_open(
        &self,
        profile: &str,
        cancellation: CancellationToken,
    ) -> Result<(String, RequestContext), CoreError> {
        let mut registry = self.registry.lock().expect("registry mutex poisoned");
        if registry.lifecycle != RegistryLifecycle::Running {
            return Err(core_error(ErrorCode::ServerStopping));
        }
        if profile.is_empty() {
            return Err(core_error(ErrorCode::DatabaseUnknown));
        }
        let profile_config = match self.profiles.as_ref() {
            Some(profiles) => Some(profiles.profile_config(profile).map_err(
                |error| match error {
                    crate::config::ConfigError::UnknownProfile => {
                        core_error(ErrorCode::DatabaseUnknown)
                    }
                    _ => core_error(ErrorCode::ConfigError),
                },
            )?),
            None => None,
        };
        let max_handles = profile_config
            .as_ref()
            .map_or(self.config.max_handles, |c| c.max_handles);
        let connection_timeout = profile_config
            .as_ref()
            .map_or(self.config.connection_timeout(), Config::connection_timeout);
        let cancel_drain_grace = profile_config
            .as_ref()
            .map_or(self.config.cancel_drain_grace(), Config::cancel_drain_grace);
        let profile_handles = registry
            .handles
            .values()
            .filter(|entry| self.profile_for_entry(entry) == profile)
            .count();
        let profile_pending = registry
            .pending_opens
            .values()
            .filter(|entry| entry.profile == profile)
            .count();
        if profile_handles.saturating_add(profile_pending) >= max_handles {
            return Err(core_error(ErrorCode::HandleLimit));
        }
        let id = registry.allocate_handle();
        let open_shutdown = CancellationToken::new();
        registry.pending_opens.insert(
            id.clone(),
            PendingOpen {
                profile: profile.to_owned(),
                shutdown: open_shutdown.clone(),
            },
        );
        let context = RequestContext {
            cancellation,
            shutdown: open_shutdown,
            deadline: Instant::now() + connection_timeout,
            cancel_drain_grace,
        };
        Ok((id, context))
    }

    fn connect_and_publish(
        &self,
        id: String,
        profile: String,
        context: RequestContext,
    ) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        let (config, dsn) = match self.profiles.as_ref() {
            Some(profiles) => match profiles.resolve_with_context(
                &profile,
                &context.cancellation,
                &context.shutdown,
                context.deadline,
            ) {
                Ok(value) => value,
                Err(crate::config::ConfigError::UnknownProfile) => {
                    self.finish_open(&id);
                    return Err(CoreFailure::process(core_error(ErrorCode::DatabaseUnknown)));
                }
                Err(crate::config::ConfigError::DsnTimeout) => {
                    self.finish_open(&id);
                    return Err(CoreFailure::process(if context.stopping() {
                        core_error(ErrorCode::ServerStopping)
                    } else if context.cancelled() {
                        core_error(ErrorCode::Cancelled)
                    } else {
                        CoreError::unknown(ErrorCode::DeadlineExceeded)
                    }));
                }
                Err(_) => {
                    self.finish_open(&id);
                    return Err(CoreFailure::process(core_error(ErrorCode::ConfigError)));
                }
            },
            None => (self.config.clone(), self.dsn.to_string()),
        };
        let connected = self.factory.connect(&dsn, &config, &context);
        let ConnectedBackend {
            mut backend,
            database,
        } = match connected {
            Ok(connected) => connected,
            Err(failure) => {
                self.finish_open(&id);
                return Err(CoreFailure::from_backend(None, failure));
            }
        };
        let observation = backend.observe();
        if context.stopping() || context.cancelled() || context.expired() {
            let _ = backend.close(&context);
            self.finish_open(&id);
            return Err(CoreFailure::process(if context.stopping() {
                core_error(ErrorCode::ServerStopping)
            } else if context.expired() {
                CoreError::unknown(ErrorCode::DeadlineExceeded)
            } else {
                CoreError::unknown(ErrorCode::Cancelled)
            }));
        }
        if observation.connection_status != BackendConnectionStatus::Ok
            || observation.transaction_status != BackendTransactionStatus::Idle
        {
            let _ = backend.close(&context);
            self.finish_open(&id);
            return Err(CoreFailure::process(CoreError::unknown(
                ErrorCode::ConnectionLost,
            )));
        }
        let (sender, receiver) = mpsc::channel(config.outstanding_requests.max(1));
        let worker_id = id.clone();
        let worker_core = self.clone();
        let worker_config = config.clone();
        let join = match thread::Builder::new()
            .name(format!("postgres-mcp-{id}"))
            .spawn(move || worker_loop(worker_core, worker_id, worker_config, backend, receiver))
        {
            Ok(join) => join,
            Err(_) => {
                self.finish_open(&id);
                return Err(CoreFailure::process(core_error(ErrorCode::Internal)));
            }
        };
        let published = PublishedHandle {
            id: id.clone(),
            database: database.clone(),
            observation,
            schema_observed: false,
            schema_dirty: false,
            closed: false,
        };
        let mut registry = self.registry.lock().expect("registry mutex poisoned");
        if registry.lifecycle != RegistryLifecycle::Running {
            drop(registry);
            let stop = RequestContext {
                cancellation: CancellationToken::new(),
                shutdown: self.shutdown.clone(),
                deadline: Instant::now() + config.cancel_drain_grace(),
                cancel_drain_grace: config.cancel_drain_grace(),
            };
            let _ = sender.try_send(Request::Shutdown {
                context: stop,
                reply: None,
            });
            self.finish_open(&id);
            return Err(CoreFailure::process(core_error(ErrorCode::ServerStopping)));
        }
        registry.pending_opens.remove(&id);
        registry.handles.insert(
            id.clone(),
            WorkerEntry {
                published: published.clone(),
                profile,
                config: config.clone(),
                sender,
                join: Some(join),
                admitted: 0,
            },
        );
        Ok(success_envelope(
            Some(published.state()),
            serde_json::json!({ "handle": id }),
        ))
    }

    fn profile_for_entry<'a>(&self, entry: &'a WorkerEntry) -> &'a str {
        &entry.profile
    }

    fn finish_open(&self, id: &str) {
        let mut registry = self.registry.lock().expect("registry mutex poisoned");
        registry.pending_opens.remove(id);
    }

    pub async fn list_handles(&self) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        let registry = self.registry.lock().expect("registry mutex poisoned");
        let states: Vec<_> = registry
            .handles
            .values()
            .map(|entry| entry.published.state())
            .collect();
        Ok(success_envelope(
            None,
            serde_json::json!({ "handles": states }),
        ))
    }

    pub async fn get_schema(
        &self,
        handle: &str,
        cancellation: CancellationToken,
    ) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        self.dispatch(handle, Operation::GetSchema, cancellation)
            .await
    }

    pub async fn open_read(
        &self,
        handle: &str,
        cancellation: CancellationToken,
    ) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        self.dispatch(handle, Operation::OpenRead, cancellation)
            .await
    }

    pub async fn open_write(
        &self,
        handle: &str,
        cancellation: CancellationToken,
    ) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        self.dispatch(handle, Operation::OpenWrite, cancellation)
            .await
    }

    pub async fn query(
        &self,
        handle: &str,
        sql: String,
        parameters: Vec<Parameter>,
        cancellation: CancellationToken,
    ) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        self.dispatch(
            handle,
            Operation::Query(QueryRequest { sql, parameters }),
            cancellation,
        )
        .await
    }

    pub async fn commit(
        &self,
        handle: &str,
        cancellation: CancellationToken,
    ) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        self.dispatch(handle, Operation::Commit, cancellation).await
    }

    pub async fn rollback(
        &self,
        handle: &str,
        cancellation: CancellationToken,
    ) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        self.dispatch(handle, Operation::Rollback, cancellation)
            .await
    }

    pub async fn close_database(
        &self,
        handle: &str,
        cancellation: CancellationToken,
    ) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        self.dispatch(handle, Operation::Close, cancellation).await
    }

    async fn dispatch(
        &self,
        handle: &str,
        operation: Operation,
        cancellation: CancellationToken,
    ) -> Result<Envelope<serde_json::Value>, CoreFailure> {
        let (reply, response) = oneshot::channel();
        let (request_timeout, cancel_drain_grace) = {
            let registry = self.registry.lock().expect("registry mutex poisoned");
            let entry = registry
                .handles
                .get(handle)
                .ok_or_else(|| CoreFailure::process(core_error(ErrorCode::HandleUnknown)))?;
            (
                entry.config.request_timeout(),
                entry.config.cancel_drain_grace(),
            )
        };
        let context = RequestContext {
            cancellation,
            shutdown: self.shutdown.clone(),
            deadline: Instant::now() + request_timeout,
            cancel_drain_grace,
        };
        let request = Request::Operation {
            operation,
            context,
            reply,
        };
        {
            let mut registry = self.registry.lock().expect("registry mutex poisoned");
            if registry.lifecycle != RegistryLifecycle::Running {
                return Err(CoreFailure::process(core_error(ErrorCode::ServerStopping)));
            }
            let entry = registry
                .handles
                .get_mut(handle)
                .ok_or_else(|| CoreFailure::process(core_error(ErrorCode::HandleUnknown)))?;
            let state = entry.published.state();
            if entry.published.closed {
                return Err(CoreFailure::for_handle(
                    self,
                    handle,
                    core_error(ErrorCode::HandleUnknown),
                ));
            }
            if entry.admitted >= entry.config.outstanding_requests {
                return Err(CoreFailure {
                    error: core_error(ErrorCode::QueueFull),
                    handle_state: Some(state),
                    sqlstate: None,
                    outcome_unknown: false,
                });
            }
            entry.admitted += 1;
            if let Err(error) = entry.sender.try_send(request) {
                entry.admitted = entry.admitted.saturating_sub(1);
                let code = if matches!(error, mpsc::error::TrySendError::Full(_)) {
                    ErrorCode::QueueFull
                } else {
                    ErrorCode::ConnectionLost
                };
                return Err(CoreFailure {
                    error: core_error(code),
                    handle_state: Some(state),
                    sqlstate: None,
                    outcome_unknown: code == ErrorCode::ConnectionLost,
                });
            }
        }
        // `response` is the caller's only completion ownership. Dropping this
        // future leaves the queued request and its publication path intact.
        response.await.unwrap_or_else(|_| {
            Err(CoreFailure::for_handle(
                self,
                handle,
                CoreError::unknown(ErrorCode::ConnectionLost),
            ))
        })
    }

    /// Transition to Stopping before snapshotting entries. Exactly one caller
    /// owns cleanup; concurrent callers wait for and reuse its frozen report.
    pub async fn shutdown(&self) -> ShutdownReport {
        let shutdown_owner = {
            let mut registry = self.registry.lock().expect("registry mutex poisoned");
            if let Some(report) = registry.frozen_report.clone() {
                return report;
            }
            if registry.lifecycle != RegistryLifecycle::Running {
                None
            } else {
                registry.lifecycle = RegistryLifecycle::Stopping;
                self.lifecycle
                    .store(RegistryLifecycle::Stopping.byte(), Ordering::SeqCst);
                let ids = registry.handles.keys().cloned().collect::<Vec<_>>();
                let pending = registry
                    .pending_opens
                    .iter()
                    .map(|(id, pending)| {
                        pending.shutdown.cancel();
                        id.clone()
                    })
                    .collect::<Vec<_>>();
                let shutdown_total = self
                    .profiles
                    .as_ref()
                    .map_or(self.config.shutdown_total(), |profiles| {
                        profiles.shutdown_total()
                    });
                Some((ids, pending, Instant::now() + shutdown_total))
            }
        };
        let (handles, pending_opens, deadline) = match shutdown_owner {
            Some(shutdown_owner) => shutdown_owner,
            None => loop {
                let frozen_report = {
                    self.registry
                        .lock()
                        .expect("registry mutex poisoned")
                        .frozen_report
                        .clone()
                };
                if let Some(report) = frozen_report {
                    return report;
                }
                tokio::task::yield_now().await;
            },
        };
        self.shutdown.cancel();
        let mut workers = Vec::new();
        for id in handles {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                workers.push(WorkerShutdownEntry::uncertain(id, "deadline"));
                continue;
            }
            let (tx, rx) = oneshot::channel();
            let entry = {
                let registry = self.registry.lock().expect("registry mutex poisoned");
                registry.handles.get(&id).map(|entry| {
                    (
                        entry.published.closed,
                        entry.sender.clone(),
                        entry.config.clone(),
                    )
                })
            };
            let Some((already_closed, sender, worker_config)) = entry else {
                workers.push(WorkerShutdownEntry::uncertain(id, "missing"));
                continue;
            };
            if already_closed {
                if join_closed_worker(&self.registry, &id, deadline).await {
                    workers.push(WorkerShutdownEntry::verified(id));
                } else {
                    workers.push(WorkerShutdownEntry::uncertain(id, "join"));
                }
                continue;
            }
            let request = Request::Shutdown {
                context: RequestContext {
                    cancellation: CancellationToken::new(),
                    // Cleanup remains permitted to issue its bounded rollback
                    // after normal shutdown admission has been closed.
                    shutdown: CancellationToken::new(),
                    deadline: Instant::now() + remaining,
                    cancel_drain_grace: worker_config.cancel_drain_grace(),
                },
                reply: Some(tx),
            };
            if tokio::time::timeout(remaining, sender.send(request))
                .await
                .is_err()
            {
                workers.push(WorkerShutdownEntry::uncertain(id, "enqueue"));
                continue;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                workers.push(WorkerShutdownEntry::uncertain(id, "deadline"));
                continue;
            }
            let result = tokio::time::timeout(remaining, rx).await;
            workers.push(match result {
                Ok(Ok(worker)) => {
                    if join_worker(&self.registry, &id) {
                        worker
                    } else {
                        WorkerShutdownEntry::uncertain(id, "join")
                    }
                }
                _ => WorkerShutdownEntry::uncertain(id, "deadline"),
            });
        }
        for id in pending_opens {
            loop {
                let still_pending = {
                    self.registry
                        .lock()
                        .expect("registry mutex poisoned")
                        .pending_opens
                        .contains_key(&id)
                };
                if !still_pending {
                    break;
                }
                if Instant::now() >= deadline {
                    workers.push(WorkerShutdownEntry::uncertain(id, "pending-open"));
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        workers.sort_by(|left, right| left.handle.cmp(&right.handle));
        let report = ShutdownReport { workers };
        let mut registry = self.registry.lock().expect("registry mutex poisoned");
        registry.lifecycle = RegistryLifecycle::Stopped;
        self.lifecycle
            .store(RegistryLifecycle::Stopped.byte(), Ordering::SeqCst);
        if let Some(existing) = registry.frozen_report.clone() {
            existing
        } else {
            registry.frozen_report = Some(report.clone());
            report
        }
    }
}

#[derive(Debug)]
enum Operation {
    GetSchema,
    OpenRead,
    OpenWrite,
    Query(QueryRequest),
    Commit,
    Rollback,
    Close,
}

fn operation_precondition(core: &Core, handle: &str, operation: &Operation) -> Option<CoreError> {
    let registry = core.registry.lock().expect("registry mutex poisoned");
    let entry = registry.handles.get(handle)?;
    let state = &entry.published;
    if state.closed {
        return Some(core_error(ErrorCode::HandleUnknown));
    }
    match operation {
        Operation::GetSchema => {
            if state.observation.transaction_status != BackendTransactionStatus::Idle {
                return Some(core_error(ErrorCode::TxAlreadyOpen));
            }
        }
        Operation::OpenRead | Operation::OpenWrite => {
            if !state.schema_observed {
                return Some(core_error(ErrorCode::SchemaRequired));
            }
            if state.observation.transaction_status != BackendTransactionStatus::Idle {
                return Some(
                    if state.observation.transaction_status == BackendTransactionStatus::InError {
                        core_error(ErrorCode::TxFailed)
                    } else {
                        core_error(ErrorCode::TxAlreadyOpen)
                    },
                );
            }
        }
        Operation::Query(_) => {
            if !state.schema_observed {
                return Some(core_error(ErrorCode::SchemaRequired));
            }
            return match state.observation.transaction_status {
                BackendTransactionStatus::InTransaction => None,
                BackendTransactionStatus::InError => Some(core_error(ErrorCode::TxFailed)),
                BackendTransactionStatus::Active => Some(core_error(ErrorCode::QueueFull)),
                BackendTransactionStatus::Idle => Some(core_error(ErrorCode::NoTxOpen)),
                BackendTransactionStatus::Unknown => Some(core_error(ErrorCode::ConnectionLost)),
            };
        }
        Operation::Commit => {
            return match state.observation.transaction_status {
                BackendTransactionStatus::InTransaction => None,
                BackendTransactionStatus::InError => Some(core_error(ErrorCode::TxFailed)),
                _ => Some(core_error(ErrorCode::NoTxOpen)),
            };
        }
        Operation::Rollback => {
            // IDLE rollback is an explicitly supported idempotent no-op.
            if matches!(
                state.observation.transaction_status,
                BackendTransactionStatus::Active | BackendTransactionStatus::Unknown
            ) {
                return Some(core_error(ErrorCode::ConnectionLost));
            }
        }
        Operation::Close => {
            if matches!(
                state.observation.transaction_status,
                BackendTransactionStatus::InTransaction | BackendTransactionStatus::InError
            ) {
                return Some(core_error(ErrorCode::ActiveTransaction));
            }
        }
    }
    None
}

struct AdmissionGuard {
    core: Core,
    handle: String,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        let mut registry = self.core.registry.lock().expect("registry mutex poisoned");
        if let Some(entry) = registry.handles.get_mut(&self.handle) {
            entry.admitted = entry.admitted.saturating_sub(1);
        }
    }
}

enum Request {
    Operation {
        operation: Operation,
        context: RequestContext,
        reply: oneshot::Sender<Result<Envelope<serde_json::Value>, CoreFailure>>,
    },
    Shutdown {
        context: RequestContext,
        reply: Option<oneshot::Sender<WorkerShutdownEntry>>,
    },
}

fn worker_loop(
    core: Core,
    handle: String,
    config: Config,
    mut backend: Box<dyn Backend>,
    mut receiver: mpsc::Receiver<Request>,
) {
    while let Some(request) = receiver.blocking_recv() {
        match request {
            Request::Operation {
                operation,
                context,
                reply,
            } => {
                let operation_handle = handle.clone();
                let admission = AdmissionGuard {
                    core: core.clone(),
                    handle: operation_handle.clone(),
                };
                let close_requested = matches!(&operation, Operation::Close);
                let commit_requested = matches!(&operation, Operation::Commit);
                let result = execute_operation(
                    &core,
                    &operation_handle,
                    &config,
                    &mut *backend,
                    operation,
                    &context,
                );
                // Always take a fresh native observation before publication,
                // including pre-send refusal/error paths.  This is the linear
                // publication point visible to the next FIFO request.
                let observation = backend.observe();
                update_observation(&core, &operation_handle, observation, None);
                let unknown_failure = result
                    .as_ref()
                    .is_err_and(|failure| failure.outcome_unknown)
                    || result.as_ref().is_ok_and(|envelope| {
                        envelope.error.as_ref().is_some_and(|error| {
                            error.transaction_outcome
                                == Some(crate::protocol::TransactionOutcome::Unknown)
                        })
                    });
                if unknown_failure {
                    if commit_requested {
                        publish_unknown(&core, &operation_handle);
                    } else {
                        update_observation(&core, &operation_handle, observation, Some(false));
                    }
                }
                let close_succeeded = close_requested
                    && result
                        .as_ref()
                        .is_ok_and(|envelope| envelope.result.is_some());
                let result = result.map(|envelope| {
                    refresh_envelope_state(&core, &operation_handle, envelope, close_succeeded)
                });
                let _ = reply.send(result);
                drop(admission);
                // Publication and admission release happen before this loop
                // dequeues the next FIFO request. A successful close owns the
                // final disposal and ends this dedicated worker.
                if close_succeeded {
                    break;
                }
            }
            Request::Shutdown { context, reply } => {
                let result = backend.shutdown(&context);
                let entry = match result {
                    ShutdownWorkerResult::Verified => WorkerShutdownEntry::verified(handle.clone()),
                    ShutdownWorkerResult::Uncertain(stage) => {
                        WorkerShutdownEntry::uncertain(handle.clone(), stage)
                    }
                };
                publish_closed(&core, &handle);
                if let Some(reply) = reply {
                    let _ = reply.send(entry);
                }
                break;
            }
        }
    }
}

fn error_code_for_validation(error: crate::protocol::ValidationError) -> ErrorCode {
    match error {
        crate::protocol::ValidationError::ParameterCountLimit
        | crate::protocol::ValidationError::ParameterByteLimit
        | crate::protocol::ValidationError::EmbeddedNul
        | crate::protocol::ValidationError::InvalidQuery => ErrorCode::InvalidInput,
        crate::protocol::ValidationError::SchemaLimit => ErrorCode::SchemaLimit,
        _ => ErrorCode::ResultLimit,
    }
}

fn error_envelope_for(core: &Core, handle: &str, error: CoreError) -> Envelope<serde_json::Value> {
    crate::protocol::error_envelope(current_state(core, handle), error, error.outcome_unknown)
}

fn execute_operation(
    core: &Core,
    handle: &str,
    config: &Config,
    backend: &mut dyn Backend,
    operation: Operation,
    context: &RequestContext,
) -> Result<Envelope<serde_json::Value>, CoreFailure> {
    if let Some(error) = operation_precondition(core, handle, &operation) {
        return Ok(error_envelope_for(core, handle, error));
    }
    if context.stopping() {
        return Ok(error_envelope_for(
            core,
            handle,
            core_error(ErrorCode::ServerStopping),
        ));
    }
    if context.cancelled() {
        let error = match operation {
            // A queued query may have crossed dispatch before its caller token
            // was observed. Without a worker send-boundary proof, preserve the
            // conservative unknown outcome rather than claiming no effects.
            Operation::Query(_) => CoreError::unknown(ErrorCode::Cancelled),
            _ => core_error(ErrorCode::Cancelled),
        };
        return Ok(error_envelope_for(core, handle, error));
    }
    if context.expired() {
        let error = match operation {
            Operation::Query(_) => CoreError::unknown(ErrorCode::DeadlineExceeded),
            _ => core_error(ErrorCode::DeadlineExceeded),
        };
        return Ok(error_envelope_for(core, handle, error));
    }
    let result = match operation {
        Operation::GetSchema => backend.get_schema(config, context),
        Operation::OpenRead => backend.open_read(context),
        Operation::OpenWrite => backend.open_write(context),
        Operation::Query(request) => {
            if let Err(error) = request.validate(config) {
                return Ok(error_envelope_for(
                    core,
                    handle,
                    core_error(error_code_for_validation(error)),
                ));
            }
            backend.query(&request, config, context)
        }
        Operation::Commit => backend.commit(context),
        Operation::Rollback => backend.rollback(context),
        Operation::Close => backend.close(context),
    };
    match result {
        Ok(BackendResult::Opened { mode, observation }) => {
            update_observation(core, handle, observation, None);
            let state = current_state(core, handle);
            Ok(success_envelope(
                state,
                serde_json::json!({ "opened": mode }),
            ))
        }
        Ok(BackendResult::Schema(schema, observation)) => {
            if schema.validate(config).is_err() {
                return Err(CoreFailure::for_handle(
                    core,
                    handle,
                    core_error(ErrorCode::SchemaLimit),
                ));
            }
            update_observation(core, handle, observation, Some(true));
            update_schema_dirty(core, handle, false);
            Ok(success_envelope(
                current_state(core, handle),
                serde_json::to_value(schema).unwrap_or_default(),
            ))
        }
        Ok(BackendResult::Query(query, observation, direct_ddl)) => {
            if query.validate(config).is_err() {
                return Err(CoreFailure::for_handle(
                    core,
                    handle,
                    core_error(ErrorCode::ResultLimit),
                ));
            }
            if direct_ddl {
                update_schema_dirty(core, handle, true);
            }
            let value = serde_json::to_value(&query).unwrap_or_default();
            update_observation(core, handle, observation, None);
            Ok(success_envelope(current_state(core, handle), value))
        }
        Ok(BackendResult::Committed(observation)) => {
            let schema_dirty = current_schema_dirty(core, handle);
            update_observation(core, handle, observation, schema_dirty.then_some(false));
            update_schema_dirty(core, handle, false);
            Ok(success_envelope(
                current_state(core, handle),
                serde_json::json!({ "committed": true }),
            ))
        }
        Ok(BackendResult::RolledBack {
            changed,
            observation,
        }) => {
            update_observation(core, handle, observation, None);
            update_schema_dirty(core, handle, false);
            Ok(success_envelope(
                current_state(core, handle),
                serde_json::json!({ "rolled_back": changed }),
            ))
        }
        Ok(BackendResult::Closed {
            observation,
            cleanup_verified,
        }) => {
            update_observation(core, handle, observation, Some(false));
            if cleanup_verified {
                publish_closed(core, handle);
                // A successful close is terminal for this worker. The loop
                // receives the FIFO close command before any later request.
                Ok(success_envelope(
                    Some(HandleState::closed(handle, current_database(core, handle))),
                    serde_json::json!({ "disposed": true, "cleanup_verified": true, "transaction_outcome": null }),
                ))
            } else {
                Err(CoreFailure::for_handle(
                    core,
                    handle,
                    CoreError::unknown(ErrorCode::CleanupUncertain),
                ))
            }
        }
        Err(failure) => {
            update_observation(core, handle, failure.observation, None);
            Ok(crate::protocol::error_envelope_with_sqlstate(
                current_state(core, handle),
                failure.core_error(),
                failure.outcome_unknown,
                failure.sqlstate.as_deref(),
            ))
        }
    }
}

fn current_schema_dirty(core: &Core, handle: &str) -> bool {
    let registry = core.registry.lock().expect("registry mutex poisoned");
    registry
        .handles
        .get(handle)
        .is_some_and(|entry| entry.published.schema_dirty)
}

fn update_schema_dirty(core: &Core, handle: &str, dirty: bool) {
    let mut registry = core.registry.lock().expect("registry mutex poisoned");
    if let Some(entry) = registry.handles.get_mut(handle) {
        entry.published.schema_dirty = dirty;
    }
}

fn publish_unknown(core: &Core, handle: &str) {
    let mut registry = core.registry.lock().expect("registry mutex poisoned");
    if let Some(entry) = registry.handles.get_mut(handle) {
        entry.published.observation = BackendObservation {
            connection_status: BackendConnectionStatus::Bad,
            transaction_status: BackendTransactionStatus::Unknown,
            busy: false,
        };
        entry.published.schema_observed = false;
        entry.published.schema_dirty = false;
    }
}

fn update_observation(
    core: &Core,
    handle: &str,
    observation: BackendObservation,
    schema: Option<bool>,
) {
    let mut registry = core.registry.lock().expect("registry mutex poisoned");
    if let Some(entry) = registry.handles.get_mut(handle) {
        entry.published.observation = observation;
        if let Some(schema) = schema {
            entry.published.schema_observed = schema;
        }
    }
}

async fn join_closed_worker(
    registry: &Arc<Mutex<RegistryState>>,
    handle: &str,
    deadline: Instant,
) -> bool {
    loop {
        let finished = {
            let registry = registry.lock().expect("registry mutex poisoned");
            registry.handles.get(handle).is_none_or(|entry| {
                entry.join.is_none()
                    || entry
                        .join
                        .as_ref()
                        .is_some_and(thread::JoinHandle::is_finished)
            })
        };
        if finished {
            return join_worker(registry, handle);
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::task::yield_now().await;
    }
}

fn join_worker(registry: &Arc<Mutex<RegistryState>>, handle: &str) -> bool {
    let join = {
        let mut registry = registry.lock().expect("registry mutex poisoned");
        let Some(entry) = registry.handles.get_mut(handle) else {
            return true;
        };
        if entry
            .join
            .as_ref()
            .is_some_and(thread::JoinHandle::is_finished)
        {
            entry.join.take()
        } else {
            return entry.join.is_none();
        }
    };
    if let Some(join) = join {
        let _ = join.join();
    }
    true
}

fn publish_closed(core: &Core, handle: &str) {
    let mut registry = core.registry.lock().expect("registry mutex poisoned");
    if let Some(entry) = registry.handles.get_mut(handle) {
        entry.published.closed = true;
        entry.published.schema_observed = false;
        entry.published.schema_dirty = false;
        entry.published.observation = BackendObservation {
            connection_status: BackendConnectionStatus::Unknown,
            transaction_status: BackendTransactionStatus::Unknown,
            busy: false,
        };
    }
}

fn current_database(core: &Core, handle: &str) -> String {
    core.registry
        .lock()
        .expect("registry mutex poisoned")
        .handles
        .get(handle)
        .map(|entry| entry.published.database.clone())
        .unwrap_or_default()
}

fn current_state(core: &Core, handle: &str) -> Option<HandleState> {
    core.registry
        .lock()
        .expect("registry mutex poisoned")
        .handles
        .get(handle)
        .map(|entry| entry.published.state())
}

fn success_envelope(
    handle_state: Option<HandleState>,
    result: serde_json::Value,
) -> Envelope<serde_json::Value> {
    let next_moves = handle_state
        .as_ref()
        .map(crate::protocol::next_moves)
        .unwrap_or_default();
    Envelope::success(handle_state, next_moves, result)
}

/// Rebuild the public state projection at the worker's final observation
/// boundary. A verified close intentionally keeps its terminal closed snapshot
/// rather than projecting the now-disposed backend connection.
fn refresh_envelope_state(
    core: &Core,
    handle: &str,
    mut envelope: Envelope<serde_json::Value>,
    verified_close: bool,
) -> Envelope<serde_json::Value> {
    if !verified_close {
        envelope.handle_state = current_state(core, handle);
        envelope.next_moves = envelope
            .handle_state
            .as_ref()
            .map(crate::protocol::next_moves)
            .unwrap_or_default();
    }
    envelope
}

struct LibpqControl<'a>(&'a RequestContext);

/// Control used for dispatched commit/rollback. Caller cancellation must not
/// interrupt these commands because their remote outcome cannot be retried or
/// inferred; shutdown remains authoritative.
struct NoClientCancelControl<'a>(&'a RequestContext);

impl PollControl for LibpqControl<'_> {
    fn shutdown_requested(&self) -> bool {
        self.0.stopping()
    }

    fn cancelled(&self) -> bool {
        self.0.cancelled() || self.0.expired()
    }

    fn cancel_drain_grace(&self) -> Duration {
        self.0.cancel_drain_grace
    }
}

struct LibpqBackend {
    connection: Option<crate::libpq::Connection>,
}

impl PollControl for NoClientCancelControl<'_> {
    fn shutdown_requested(&self) -> bool {
        self.0.stopping()
    }

    fn cancelled(&self) -> bool {
        false
    }

    fn cancel_drain_grace(&self) -> Duration {
        self.0.cancel_drain_grace
    }
}

impl LibpqBackend {
    fn failure(
        error: crate::libpq::AdapterError,
        observation: BackendObservation,
    ) -> BackendFailure {
        let code = match error.class {
            crate::libpq::ErrorClass::Config => ErrorCode::ConfigError,
            crate::libpq::ErrorClass::InvalidInput => ErrorCode::InvalidInput,
            crate::libpq::ErrorClass::Database => ErrorCode::DatabaseError,
            crate::libpq::ErrorClass::DatabaseMismatch => ErrorCode::DatabaseMismatch,
            crate::libpq::ErrorClass::ConnectionLost => ErrorCode::ConnectionLost,
            crate::libpq::ErrorClass::UnsupportedServer => ErrorCode::UnsupportedServer,
            crate::libpq::ErrorClass::ResultLimit => ErrorCode::ResultLimit,
            crate::libpq::ErrorClass::CopyUnsupported => ErrorCode::CopyUnsupported,
            crate::libpq::ErrorClass::Cancelled => ErrorCode::Cancelled,
            crate::libpq::ErrorClass::DeadlineExceeded => ErrorCode::DeadlineExceeded,
            crate::libpq::ErrorClass::ServerStopping => ErrorCode::ServerStopping,
            crate::libpq::ErrorClass::Internal => ErrorCode::Internal,
        };
        BackendFailure {
            error: core_error(code),
            observation,
            sqlstate: error.sqlstate,
            outcome_unknown: error.outcome_unknown,
        }
    }

    fn observation(&self) -> BackendObservation {
        self.connection.as_ref().map_or(
            BackendObservation {
                connection_status: BackendConnectionStatus::Unknown,
                transaction_status: BackendTransactionStatus::Unknown,
                busy: false,
            },
            |connection| {
                let status = connection.status();
                BackendObservation {
                    connection_status: match status.connection_status {
                        crate::libpq::ConnectionStatus::Ok => BackendConnectionStatus::Ok,
                        crate::libpq::ConnectionStatus::Bad => BackendConnectionStatus::Bad,
                        crate::libpq::ConnectionStatus::Closed => BackendConnectionStatus::Unknown,
                    },
                    transaction_status: match status.transaction_status {
                        crate::libpq::TransactionStatus::Idle => BackendTransactionStatus::Idle,
                        crate::libpq::TransactionStatus::Active => BackendTransactionStatus::Active,
                        crate::libpq::TransactionStatus::InTransaction => {
                            BackendTransactionStatus::InTransaction
                        }
                        crate::libpq::TransactionStatus::InError => {
                            BackendTransactionStatus::InError
                        }
                        crate::libpq::TransactionStatus::Unknown => {
                            BackendTransactionStatus::Unknown
                        }
                    },
                    busy: status.busy,
                }
            },
        )
    }

    fn execute_command(
        &mut self,
        sql: &str,
        context: &RequestContext,
        ignore_client_cancel: bool,
    ) -> Result<crate::libpq::ResultSet, BackendFailure> {
        let Some(connection) = self.connection.as_mut() else {
            return Err(BackendFailure {
                error: core_error(ErrorCode::ConnectionLost),
                observation: self.observation(),
                sqlstate: None,
                outcome_unknown: true,
            });
        };
        let control: &dyn PollControl = if ignore_client_cancel {
            &NoClientCancelControl(context)
        } else {
            &LibpqControl(context)
        };
        connection
            .execute(sql, context.deadline, control)
            .map_err(|error| Self::failure(error, self.observation()))
    }
}

impl Backend for LibpqBackend {
    fn open_read(&mut self, context: &RequestContext) -> Result<BackendResult, BackendFailure> {
        let result = self.execute_command("BEGIN READ ONLY", context, false)?;
        if result.command_tag != "BEGIN" {
            return Err(BackendFailure {
                error: core_error(ErrorCode::DatabaseError),
                observation: self.observation(),
                sqlstate: None,
                outcome_unknown: false,
            });
        }
        Ok(BackendResult::Opened {
            mode: "read",
            observation: self.observation(),
        })
    }

    fn open_write(&mut self, context: &RequestContext) -> Result<BackendResult, BackendFailure> {
        let result = self.execute_command("BEGIN READ WRITE", context, false)?;
        if result.command_tag != "BEGIN" {
            return Err(BackendFailure {
                error: core_error(ErrorCode::DatabaseError),
                observation: self.observation(),
                sqlstate: None,
                outcome_unknown: false,
            });
        }
        Ok(BackendResult::Opened {
            mode: "write",
            observation: self.observation(),
        })
    }

    fn get_schema(
        &mut self,
        config: &Config,
        context: &RequestContext,
    ) -> Result<BackendResult, BackendFailure> {
        let Some(connection) = self.connection.as_mut() else {
            return Err(BackendFailure {
                error: core_error(ErrorCode::ConnectionLost),
                observation: self.observation(),
                sqlstate: None,
                outcome_unknown: true,
            });
        };
        let control = LibpqControl(context);
        match crate::schema::retrieve(connection, config, context.deadline, &control) {
            Ok(schema) => Ok(BackendResult::Schema(schema, self.observation())),
            Err(crate::schema::SchemaError::Limit) => Err(BackendFailure {
                error: core_error(ErrorCode::SchemaLimit),
                observation: self.observation(),
                sqlstate: None,
                outcome_unknown: false,
            }),
            Err(crate::schema::SchemaError::Malformed) => Err(BackendFailure {
                error: core_error(ErrorCode::Internal),
                observation: self.observation(),
                sqlstate: None,
                outcome_unknown: false,
            }),
            Err(crate::schema::SchemaError::Backend(error)) => {
                Err(Self::failure(error, self.observation()))
            }
        }
    }

    fn query(
        &mut self,
        request: &QueryRequest,
        config: &Config,
        context: &RequestContext,
    ) -> Result<BackendResult, BackendFailure> {
        let Some(connection) = self.connection.as_mut() else {
            return Err(BackendFailure {
                error: core_error(ErrorCode::ConnectionLost),
                observation: self.observation(),
                sqlstate: None,
                outcome_unknown: true,
            });
        };
        let parameters: Vec<_> = request
            .parameters
            .iter()
            .map(|parameter| crate::libpq::QueryParameter {
                type_oid: parameter.type_oid,
                value: parameter.value.clone(),
            })
            .collect();
        let limits = crate::libpq::QueryLimits {
            sql_bytes: config.sql_bytes,
            parameters: config.parameters,
            parameter_bytes: config.parameter_bytes,
            result_rows: config.result_rows,
            columns: config.columns,
            cell_bytes: config.cell_bytes,
            result_json_bytes: config.result_json_bytes,
        };
        let control = LibpqControl(context);
        match connection.query(
            &request.sql,
            &parameters,
            limits,
            context.deadline,
            &control,
        ) {
            Ok(result) => {
                let direct_ddl = crate::libpq::direct_ddl_command_tag(&result.command_tag);
                Ok(BackendResult::Query(
                    QueryResult::new(
                        result
                            .columns
                            .into_iter()
                            .map(|column| {
                                crate::protocol::ResultColumn::text(column.name, column.type_oid)
                            })
                            .collect(),
                        result.rows,
                        result.command_tag,
                        result.affected_rows,
                    ),
                    self.observation(),
                    direct_ddl,
                ))
            }
            Err(error) => {
                let observation = self.observation();
                if context.stopping() || context.cancelled() || context.expired() {
                    let reason = if context.stopping() {
                        crate::libpq::CancelReason::Shutdown
                    } else if context.cancelled() {
                        crate::libpq::CancelReason::Cancelled
                    } else {
                        crate::libpq::CancelReason::Deadline
                    };
                    if let Some(connection) = self.connection.as_mut()
                        && let Err(cancel_error) = connection
                            .cancel_and_drain(reason, Instant::now() + context.cancel_drain_grace)
                    {
                        return Err(Self::failure(cancel_error, self.observation()));
                    }
                }
                let mut failure = Self::failure(error, observation);
                // Once query dispatch has entered the adapter, a cancellation or
                // deadline observed here may follow a successful PQsendQueryParams.
                // Drain/status can describe the connection, but cannot prove the
                // statement's effects; preserve the required unknown outcome.
                if context.cancelled() || context.expired() {
                    failure.outcome_unknown = true;
                }
                Err(failure)
            }
        }
    }

    fn commit(&mut self, context: &RequestContext) -> Result<BackendResult, BackendFailure> {
        if self.observation().transaction_status == BackendTransactionStatus::InError {
            return Err(BackendFailure {
                error: core_error(ErrorCode::TxFailed),
                observation: self.observation(),
                sqlstate: None,
                outcome_unknown: false,
            });
        }
        if self.observation().transaction_status != BackendTransactionStatus::InTransaction {
            return Err(BackendFailure {
                error: core_error(ErrorCode::NoTxOpen),
                observation: self.observation(),
                sqlstate: None,
                outcome_unknown: false,
            });
        }
        let result = match self.execute_command("COMMIT", context, true) {
            Ok(result) => result,
            Err(failure) => {
                return Err(BackendFailure {
                    error: core_error(ErrorCode::CommitOutcomeUnknown),
                    observation: self.observation(),
                    sqlstate: failure.sqlstate,
                    outcome_unknown: true,
                });
            }
        };
        let observation = self.observation();
        if result.command_tag == "COMMIT"
            && observation.transaction_status == BackendTransactionStatus::Idle
        {
            Ok(BackendResult::Committed(observation))
        } else {
            Err(BackendFailure {
                error: core_error(ErrorCode::CommitOutcomeUnknown),
                observation,
                sqlstate: None,
                outcome_unknown: true,
            })
        }
    }

    fn rollback(&mut self, context: &RequestContext) -> Result<BackendResult, BackendFailure> {
        let before = self.observation();
        if before.transaction_status == BackendTransactionStatus::Idle {
            return Ok(BackendResult::RolledBack {
                changed: false,
                observation: before,
            });
        }
        let result = self.execute_command("ROLLBACK", context, true)?;
        let observation = self.observation();
        if result.command_tag == "ROLLBACK"
            && observation.transaction_status == BackendTransactionStatus::Idle
        {
            Ok(BackendResult::RolledBack {
                changed: true,
                observation,
            })
        } else {
            Err(BackendFailure {
                error: core_error(ErrorCode::ConnectionLost),
                observation,
                sqlstate: None,
                outcome_unknown: true,
            })
        }
    }

    fn close(&mut self, _context: &RequestContext) -> Result<BackendResult, BackendFailure> {
        let observation = self.observation();
        if matches!(
            observation.transaction_status,
            BackendTransactionStatus::InTransaction | BackendTransactionStatus::InError
        ) {
            return Err(BackendFailure {
                error: core_error(ErrorCode::ActiveTransaction),
                observation,
                sqlstate: None,
                outcome_unknown: false,
            });
        }
        self.connection.take();
        Ok(BackendResult::Closed {
            observation: BackendObservation {
                connection_status: BackendConnectionStatus::Unknown,
                transaction_status: BackendTransactionStatus::Unknown,
                busy: false,
            },
            cleanup_verified: true,
        })
    }

    fn observe(&mut self) -> BackendObservation {
        self.observation()
    }

    fn shutdown(&mut self, context: &RequestContext) -> ShutdownWorkerResult {
        let observation = self.observation();
        if matches!(
            observation.transaction_status,
            BackendTransactionStatus::InTransaction | BackendTransactionStatus::InError
        ) && (self.execute_command("ROLLBACK", context, true).is_err()
            || self.observation().transaction_status != BackendTransactionStatus::Idle)
        {
            self.connection.take();
            return ShutdownWorkerResult::Uncertain("rollback");
        }
        self.connection.take();
        ShutdownWorkerResult::Verified
    }
}

struct FactoryControl<'a>(&'a RequestContext);

impl PollControl for FactoryControl<'_> {
    fn shutdown_requested(&self) -> bool {
        self.0.stopping()
    }
    fn cancelled(&self) -> bool {
        self.0.cancelled() || self.0.expired()
    }

    fn cancel_drain_grace(&self) -> Duration {
        self.0.cancel_drain_grace
    }
}

impl BackendFactory for crate::libpq::LibpqFactory {
    fn connect(
        &self,
        dsn: &str,
        _config: &Config,
        context: &RequestContext,
    ) -> Result<ConnectedBackend, BackendFailure> {
        let parsed = crate::libpq::Dsn::parse(dsn).map_err(|error| BackendFailure {
            error: core_error(match error.class {
                crate::libpq::ErrorClass::UnsupportedServer => ErrorCode::UnsupportedServer,
                _ => ErrorCode::ConfigError,
            }),
            observation: BackendObservation {
                connection_status: BackendConnectionStatus::Unknown,
                transaction_status: BackendTransactionStatus::Unknown,
                busy: false,
            },
            sqlstate: error.sqlstate,
            outcome_unknown: false,
        })?;
        let control = FactoryControl(context);
        let connection = crate::libpq::Connection::connect(&parsed, context.deadline, &control)
            .map_err(|error| {
                let code = match error.class {
                    crate::libpq::ErrorClass::UnsupportedServer => ErrorCode::UnsupportedServer,
                    crate::libpq::ErrorClass::Config => ErrorCode::ConfigError,
                    crate::libpq::ErrorClass::DatabaseMismatch => ErrorCode::DatabaseMismatch,
                    crate::libpq::ErrorClass::ConnectionLost => ErrorCode::ConnectionLost,
                    crate::libpq::ErrorClass::Cancelled => ErrorCode::Cancelled,
                    crate::libpq::ErrorClass::DeadlineExceeded => ErrorCode::DeadlineExceeded,
                    crate::libpq::ErrorClass::ServerStopping => ErrorCode::ServerStopping,
                    _ => ErrorCode::DatabaseError,
                };
                BackendFailure {
                    error: core_error(code),
                    observation: BackendObservation {
                        connection_status: BackendConnectionStatus::Unknown,
                        transaction_status: BackendTransactionStatus::Unknown,
                        busy: false,
                    },
                    sqlstate: error.sqlstate,
                    outcome_unknown: error.outcome_unknown,
                }
            })?;
        Ok(ConnectedBackend {
            backend: Box::new(LibpqBackend {
                connection: Some(connection),
            }),
            database: parsed.database().to_owned(),
        })
    }
}

/// Terminal shutdown outcome for a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownWorkerResult {
    Verified,
    Uncertain(&'static str),
}

/// Sorted, frozen aggregate shutdown report.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShutdownReport {
    pub workers: Vec<WorkerShutdownEntry>,
}

impl ShutdownReport {
    pub fn verified(&self) -> bool {
        self.workers
            .iter()
            .all(|entry| entry.outcome == ShutdownOutcome::Verified)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOutcome {
    Verified,
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerShutdownEntry {
    pub handle: String,
    pub outcome: ShutdownOutcome,
    pub stage: Option<&'static str>,
}

impl WorkerShutdownEntry {
    fn verified(handle: String) -> Self {
        Self {
            handle,
            outcome: ShutdownOutcome::Verified,
            stage: None,
        }
    }

    fn uncertain(handle: String, stage: &'static str) -> Self {
        Self {
            handle,
            outcome: ShutdownOutcome::Uncertain,
            stage: Some(stage),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_mapping_covers_all_native_variants() {
        assert_eq!(
            BackendTransactionStatus::Idle.protocol(),
            TransactionStatus::Idle
        );
        assert_eq!(
            BackendTransactionStatus::Active.protocol(),
            TransactionStatus::Active
        );
        assert_eq!(
            BackendTransactionStatus::InTransaction.protocol(),
            TransactionStatus::InTransaction
        );
        assert_eq!(
            BackendTransactionStatus::InError.protocol(),
            TransactionStatus::InError
        );
        assert_eq!(
            BackendTransactionStatus::Unknown.protocol(),
            TransactionStatus::Unknown
        );
    }

    #[test]
    fn lifecycle_encoding_round_trips() {
        for lifecycle in [
            RegistryLifecycle::Running,
            RegistryLifecycle::Stopping,
            RegistryLifecycle::Stopped,
        ] {
            assert_eq!(RegistryLifecycle::from_byte(lifecycle.byte()), lifecycle);
        }
    }
}
