//! Test-only, protocol-agnostic TCP fault fixture.
//!
//! `TransparentProxy` forwards bytes between a dynamically allocated loopback
//! endpoint and a caller-supplied socket address. It deliberately knows
//! nothing about PostgreSQL (or any other protocol). Each direction has its
//! own worker and pause barrier, so a stalled server-to-client stream does not
//! stop client-to-server traffic. All waits have a caller-configured watchdog.
//!
//! The module is included directly by integration tests with:
//!
//! ```ignore
//! #[path = "support/proxy.rs"]
//! mod proxy;
//! ```

#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// The maximum interval for a forwarding worker to remain in a socket call.
/// This is an I/O timeout, not a test sleep; it lets workers observe barriers
/// and shutdown promptly while preserving ordinary blocking TCP semantics.
const IO_SLICE: Duration = Duration::from_millis(25);
const BUFFER_SIZE: usize = 16 * 1024;

/// A byte-forwarding direction as viewed from the proxy's client endpoint.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum Direction {
    /// Bytes received from the proxy client and sent to the upstream peer.
    ClientToServer,
    /// Bytes received from the upstream peer and sent to the proxy client.
    ServerToClient,
}

impl Direction {
    fn index(self) -> usize {
        match self {
            Self::ClientToServer => 0,
            Self::ServerToClient => 1,
        }
    }
}

/// Runtime settings for a [`TransparentProxy`].
#[derive(Debug, Clone, Copy)]
pub struct ProxyConfig {
    /// Maximum time spent waiting for a barrier or teardown to complete.
    pub watchdog: Duration,
}

impl ProxyConfig {
    /// Construct a configuration with an explicit finite watchdog.
    pub const fn new(watchdog: Duration) -> Self {
        Self { watchdog }
    }
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            watchdog: Duration::from_secs(5),
        }
    }
}

struct Connection {
    id: u64,
    client: TcpStream,
    server: TcpStream,
    live_workers: AtomicUsize,
}

impl Connection {
    fn close(&self) {
        // The control clones are retained solely to wake both forwarding
        // workers. Shutdown is idempotent and errors are intentionally local
        // to the fixture; the worker observes the resulting socket error.
        let _ = self.client.shutdown(Shutdown::Both);
        let _ = self.server.shutdown(Shutdown::Both);
    }
}

struct State {
    paused: [bool; 2],
    active_workers: [usize; 2],
    paused_workers: [usize; 2],
    connections: HashMap<u64, Arc<Connection>>,
    next_connection_id: u64,
    dropping: bool,
    stopping: bool,
    accept_alive: bool,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

impl Shared {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                paused: [false; 2],
                active_workers: [0; 2],
                paused_workers: [0; 2],
                connections: HashMap::new(),
                next_connection_id: 0,
                dropping: false,
                stopping: false,
                accept_alive: true,
            }),
            changed: Condvar::new(),
        })
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("proxy state lock poisoned"))
    }

    fn stopped(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.stopping || state.dropping)
            .unwrap_or(true)
    }

    fn stopping(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.stopping)
            .unwrap_or(true)
    }

    fn wait_until<F>(&self, deadline: Instant, condition: F) -> io::Result<bool>
    where
        F: Fn(&State) -> bool,
    {
        let mut state = self.lock()?;
        loop {
            if condition(&state) {
                return Ok(true);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            let (next, timeout) = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| io::Error::other("proxy state lock poisoned"))?;
            state = next;
            if timeout.timed_out() {
                return Ok(condition(&state));
            }
        }
    }

    fn allocate_connection_id(&self) -> io::Result<u64> {
        let mut state = self.lock()?;
        let id = state.next_connection_id;
        state.next_connection_id = state.next_connection_id.wrapping_add(1);
        Ok(id)
    }

    fn add_connection(&self, connection: Arc<Connection>) -> io::Result<bool> {
        let mut state = self.lock()?;
        if state.stopping || state.dropping {
            return Ok(false);
        }
        state.active_workers[0] += 1;
        state.active_workers[1] += 1;
        state.connections.insert(connection.id, connection);
        self.changed.notify_all();
        Ok(true)
    }

    fn worker_finished(&self, connection: &Arc<Connection>, direction: Direction, paused: bool) {
        let index = direction.index();
        let last_worker = connection.live_workers.fetch_sub(1, Ordering::AcqRel) == 1;
        if let Ok(mut state) = self.state.lock() {
            state.active_workers[index] = state.active_workers[index].saturating_sub(1);
            if paused {
                state.paused_workers[index] = state.paused_workers[index].saturating_sub(1);
            }
            if last_worker {
                // Remove the connection only after its final direction has
                // published its inactive counter. Drop/shutdown barriers wait
                // for both observations before returning.
                state.connections.remove(&connection.id);
            }
        }
        self.changed.notify_all();
    }

    fn accept_finished(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.accept_alive = false;
        }
        self.changed.notify_all();
    }

    fn begin_shutdown(&self) -> Vec<Arc<Connection>> {
        let connections = match self.state.lock() {
            Ok(mut state) => {
                state.stopping = true;
                state.dropping = true;
                state.connections.values().cloned().collect()
            }
            Err(_) => Vec::new(),
        };
        self.changed.notify_all();
        connections
    }

    fn close_connections(&self) -> Vec<Arc<Connection>> {
        self.state
            .lock()
            .map(|state| state.connections.values().cloned().collect())
            .unwrap_or_default()
    }

    fn wait_for_direction(&self, direction: Direction, deadline: Instant) -> io::Result<bool> {
        let index = direction.index();
        self.wait_until(deadline, |state| {
            state.stopping || state.active_workers[index] == state.paused_workers[index]
        })
    }

    fn wait_for_resume(&self, direction: Direction, deadline: Instant) -> io::Result<bool> {
        let index = direction.index();
        self.wait_until(deadline, |state| {
            state.stopping || state.paused_workers[index] == 0
        })
    }
}

/// A cloneable controller for a running [`TransparentProxy`].
///
/// The controller is useful when a live-test parent needs to arm a barrier in
/// one thread and drive the PostgreSQL client in another. It contains no
/// protocol-specific callback or production hook.
#[derive(Clone)]
pub struct ProxyControl {
    shared: Arc<Shared>,
    watchdog: Duration,
}

impl ProxyControl {
    fn deadline(&self) -> Instant {
        Instant::now() + self.watchdog
    }

    /// Pause forwarding in `direction`, returning only after every currently
    /// active worker in that direction has reached the pause barrier. If no
    /// connection exists yet, the pause is armed for the next connection.
    pub fn pause(&self, direction: Direction) -> io::Result<()> {
        {
            let mut state = self.shared.lock()?;
            if state.stopping {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "proxy is stopping",
                ));
            }
            state.paused[direction.index()] = true;
        }
        self.shared.changed.notify_all();

        if self.shared.wait_for_direction(direction, self.deadline())? {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "proxy pause barrier watchdog expired",
            ))
        }
    }

    /// Resume forwarding in `direction`, returning after paused workers have
    /// observed the release. New connections are also released.
    pub fn resume(&self, direction: Direction) -> io::Result<()> {
        {
            let mut state = self.shared.lock()?;
            state.paused[direction.index()] = false;
        }
        self.shared.changed.notify_all();

        if self.shared.wait_for_resume(direction, self.deadline())? {
            if self.shared.stopping() {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "proxy is stopping",
                ))
            } else {
                Ok(())
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "proxy resume barrier watchdog expired",
            ))
        }
    }

    /// Drop every currently proxied connection and wait until both direction
    /// workers have retired. The listener remains available for later use.
    pub fn drop_connections(&self) -> io::Result<()> {
        {
            let mut state = self.shared.lock()?;
            if state.stopping {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "proxy is stopping",
                ));
            }
            state.dropping = true;
        }
        self.shared.changed.notify_all();
        for connection in self.shared.close_connections() {
            connection.close();
        }

        let complete = self.shared.wait_until(self.deadline(), |state| {
            state.connections.is_empty() && state.active_workers.iter().all(|workers| *workers == 0)
        })?;
        {
            let mut state = self.shared.lock()?;
            state.dropping = false;
        }
        self.shared.changed.notify_all();

        if complete {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "proxy drop barrier watchdog expired",
            ))
        }
    }

    /// Singular alias for [`Self::drop_connections`], convenient for tests
    /// that maintain one client connection.
    pub fn drop_connection(&self) -> io::Result<()> {
        self.drop_connections()
    }

    /// Alias for [`Self::drop_connections`].
    pub fn disconnect(&self) -> io::Result<()> {
        self.drop_connections()
    }

    /// Wait for at least one accepted proxied connection. This is useful for
    /// making a subsequent pause assertion deterministic without a sleep.
    pub fn wait_for_connection(&self) -> io::Result<()> {
        let complete = self.shared.wait_until(self.deadline(), |state| {
            state.stopping || !state.connections.is_empty()
        })?;
        if complete {
            if self.shared.stopping() {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "proxy is stopping",
                ))
            } else {
                Ok(())
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "proxy connection watchdog expired",
            ))
        }
    }

    /// Number of proxied connections currently owned by the fixture.
    pub fn connection_count(&self) -> usize {
        self.shared
            .state
            .lock()
            .map(|state| state.connections.len())
            .unwrap_or(0)
    }

    /// The finite wait bound used by this controller.
    pub const fn watchdog(&self) -> Duration {
        self.watchdog
    }
}

/// A transparent, byte-preserving TCP proxy for integration tests.
pub struct TransparentProxy {
    local_addr: SocketAddr,
    target_addr: SocketAddr,
    wake_addr: SocketAddr,
    control: ProxyControl,
    accept_thread: Option<JoinHandle<()>>,
}

/// Short name for callers that prefer `TcpProxy`.
pub type TcpProxy = TransparentProxy;

impl TransparentProxy {
    /// Bind a dynamic loopback endpoint and forward to `target_addr`.
    pub fn bind(target_addr: SocketAddr) -> io::Result<Self> {
        Self::bind_with_config(target_addr, ProxyConfig::default())
    }

    /// Bind a dynamic loopback endpoint with an explicit finite watchdog.
    pub fn bind_with_watchdog(target_addr: SocketAddr, watchdog: Duration) -> io::Result<Self> {
        Self::bind_with_config(target_addr, ProxyConfig::new(watchdog))
    }

    /// Bind a dynamic loopback endpoint using `config`.
    pub fn bind_with_config(target_addr: SocketAddr, config: ProxyConfig) -> io::Result<Self> {
        if config.watchdog.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "proxy watchdog must be nonzero",
            ));
        }

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
        let local_addr = listener.local_addr()?;
        let shared = Shared::new();
        let control = ProxyControl {
            shared: Arc::clone(&shared),
            watchdog: config.watchdog,
        };
        let wake_addr = local_addr;
        let accept_shared = Arc::clone(&shared);
        let accept_thread = thread::Builder::new()
            .name("postgres-mcp-test-proxy-accept".to_owned())
            .spawn(move || accept_loop(listener, target_addr, accept_shared, config.watchdog))
            .map_err(|error| io::Error::other(format!("spawn proxy accept worker: {error}")))?;

        Ok(Self {
            local_addr,
            target_addr,
            wake_addr,
            control,
            accept_thread: Some(accept_thread),
        })
    }

    /// Alias for [`Self::bind`].
    pub fn new(target_addr: SocketAddr) -> io::Result<Self> {
        Self::bind(target_addr)
    }

    /// The loopback endpoint clients should connect to.
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Alias for [`Self::local_addr`].
    pub const fn address(&self) -> SocketAddr {
        self.local_addr
    }

    /// The upstream endpoint receiving forwarded bytes.
    pub const fn target_addr(&self) -> SocketAddr {
        self.target_addr
    }

    /// Obtain a cloneable controller for barrier operations.
    pub fn control(&self) -> ProxyControl {
        self.control.clone()
    }

    pub fn pause(&self, direction: Direction) -> io::Result<()> {
        self.control.pause(direction)
    }

    pub fn resume(&self, direction: Direction) -> io::Result<()> {
        self.control.resume(direction)
    }

    pub fn drop_connections(&self) -> io::Result<()> {
        self.control.drop_connections()
    }

    pub fn drop_connection(&self) -> io::Result<()> {
        self.control.drop_connections()
    }

    pub fn disconnect(&self) -> io::Result<()> {
        self.control.drop_connections()
    }

    pub fn wait_for_connection(&self) -> io::Result<()> {
        self.control.wait_for_connection()
    }

    /// Stop accepting clients, disconnect active streams, and wait for every
    /// worker to retire. A timeout is returned rather than joining an
    /// unresponsive OS thread indefinitely.
    pub fn shutdown(&mut self) -> io::Result<()> {
        let Some(accept_thread) = self.accept_thread.as_ref() else {
            return Ok(());
        };
        if accept_thread.thread().id() == thread::current().id() {
            return Err(io::Error::other(
                "proxy cannot shut down from its accept worker",
            ));
        }

        for connection in self.control.shared.begin_shutdown() {
            connection.close();
        }
        // TcpListener::accept has no standard timeout. A loopback connection
        // wakes the blocking accept call; the accept worker sees `stopping`
        // and closes this wake stream without treating it as a client.
        let wake_timeout = self.control.watchdog.min(IO_SLICE);
        let _ = TcpStream::connect_timeout(&self.wake_addr, wake_timeout);

        let complete =
            self.control
                .shared
                .wait_until(Instant::now() + self.control.watchdog, |state| {
                    !state.accept_alive
                        && state.connections.is_empty()
                        && state.active_workers.iter().all(|workers| *workers == 0)
                })?;
        if !complete {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "proxy shutdown watchdog expired",
            ));
        }

        let accept_thread = self
            .accept_thread
            .take()
            .expect("accept thread was checked above");
        accept_thread
            .join()
            .map_err(|_| io::Error::other("proxy accept worker panicked"))
    }
}

impl Drop for TransparentProxy {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn accept_loop(
    listener: TcpListener,
    target_addr: SocketAddr,
    shared: Arc<Shared>,
    watchdog: Duration,
) {
    loop {
        let accepted = listener.accept();
        let Ok((client, _peer_addr)) = accepted else {
            let error = accepted.expect_err("accepted result was checked");
            if shared.stopping() || error.kind() == io::ErrorKind::Interrupted {
                if shared.stopping() {
                    break;
                }
                continue;
            }
            break;
        };

        if shared.stopping() {
            let _ = client.shutdown(Shutdown::Both);
            break;
        }
        if shared
            .state
            .lock()
            .map(|state| state.dropping)
            .unwrap_or(true)
        {
            let _ = client.shutdown(Shutdown::Both);
            continue;
        }

        // The target is expected to be a local fixture peer. Capping this
        // individual connect attempt at one I/O slice ensures shutdown does
        // not wait for the full watchdog behind a stalled connect call.
        let connect_timeout = watchdog.min(IO_SLICE);
        let server = match TcpStream::connect_timeout(&target_addr, connect_timeout) {
            Ok(server) => server,
            Err(_) => {
                let _ = client.shutdown(Shutdown::Both);
                continue;
            }
        };
        if shared.stopping() {
            let _ = client.shutdown(Shutdown::Both);
            let _ = server.shutdown(Shutdown::Both);
            break;
        }

        configure_stream(&client);
        configure_stream(&server);
        let id = match shared.allocate_connection_id() {
            Ok(id) => id,
            Err(_) => {
                let _ = client.shutdown(Shutdown::Both);
                let _ = server.shutdown(Shutdown::Both);
                break;
            }
        };
        let connection = Arc::new(Connection {
            id,
            client,
            server,
            live_workers: AtomicUsize::new(2),
        });
        if !shared
            .add_connection(Arc::clone(&connection))
            .unwrap_or(false)
        {
            connection.close();
            continue;
        }

        let client_to_server = match (connection.client.try_clone(), connection.server.try_clone())
        {
            (Ok(source), Ok(destination)) => Some((source, destination)),
            _ => None,
        };
        let server_to_client = match (connection.server.try_clone(), connection.client.try_clone())
        {
            (Ok(source), Ok(destination)) => Some((source, destination)),
            _ => None,
        };
        if client_to_server.is_none() || server_to_client.is_none() {
            connection.close();
            shared.worker_finished(&connection, Direction::ClientToServer, false);
            shared.worker_finished(&connection, Direction::ServerToClient, false);
            continue;
        }
        spawn_direction_worker(
            Arc::clone(&shared),
            Arc::clone(&connection),
            Direction::ClientToServer,
            client_to_server.expect("checked above"),
        );
        spawn_direction_worker(
            Arc::clone(&shared),
            Arc::clone(&connection),
            Direction::ServerToClient,
            server_to_client.expect("checked above"),
        );
    }
    shared.accept_finished();
}

fn configure_stream(stream: &TcpStream) {
    let _ = stream.set_read_timeout(Some(IO_SLICE));
    let _ = stream.set_write_timeout(Some(IO_SLICE));
    let _ = stream.set_nodelay(true);
}

fn spawn_direction_worker(
    shared: Arc<Shared>,
    connection: Arc<Connection>,
    direction: Direction,
    streams: (TcpStream, TcpStream),
) {
    let worker_shared = Arc::clone(&shared);
    let worker_connection = Arc::clone(&connection);
    let spawned = thread::Builder::new()
        .name(format!("postgres-mcp-test-proxy-{direction:?}"))
        .spawn(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                forward_direction(
                    &worker_shared,
                    &worker_connection,
                    direction,
                    streams.0,
                    streams.1,
                )
            }));
            if result.is_err() {
                worker_connection.close();
            }
            // `forward_direction` returns with the pause count already
            // balanced. A panic is caught so the aggregate teardown wait is
            // not left waiting forever for a worker publication.
            worker_shared.worker_finished(&worker_connection, direction, false);
        });
    if spawned.is_err() {
        connection.close();
        shared.worker_finished(&connection, direction, false);
    }
}

fn forward_direction(
    shared: &Shared,
    connection: &Connection,
    direction: Direction,
    mut source: TcpStream,
    mut destination: TcpStream,
) {
    let index = direction.index();
    let mut paused = false;
    let mut buffer = [0_u8; BUFFER_SIZE];

    loop {
        if !wait_until_resumed(shared, index, &mut paused) {
            break;
        }
        if shared.stopped() {
            break;
        }

        match source.read(&mut buffer) {
            Ok(0) => {
                // Preserve half-close semantics. The opposite direction may
                // still carry a response after this side reaches EOF.
                let _ = destination.shutdown(Shutdown::Write);
                break;
            }
            Ok(length) => {
                if !write_all_interruptible(shared, connection, &mut destination, &buffer[..length])
                {
                    break;
                }
            }
            Err(error) if is_retryable_io(&error) => continue,
            Err(_) => break,
        }
    }

    if paused {
        if let Ok(mut state) = shared.state.lock() {
            state.paused_workers[index] = state.paused_workers[index].saturating_sub(1);
        }
        shared.changed.notify_all();
    }
}

fn wait_until_resumed(shared: &Shared, index: usize, paused: &mut bool) -> bool {
    loop {
        let Ok(mut state) = shared.state.lock() else {
            return false;
        };
        if state.stopping || state.dropping {
            if *paused {
                state.paused_workers[index] = state.paused_workers[index].saturating_sub(1);
                *paused = false;
                shared.changed.notify_all();
            }
            return false;
        }
        if !state.paused[index] {
            if *paused {
                state.paused_workers[index] = state.paused_workers[index].saturating_sub(1);
                *paused = false;
                shared.changed.notify_all();
            }
            return true;
        }
        if !*paused {
            state.paused_workers[index] += 1;
            *paused = true;
            shared.changed.notify_all();
        }
        let waited = shared.changed.wait(state);
        if waited.is_err() {
            return false;
        }
    }
}

fn write_all_interruptible(
    shared: &Shared,
    connection: &Connection,
    destination: &mut TcpStream,
    mut bytes: &[u8],
) -> bool {
    while !bytes.is_empty() {
        if shared.stopped() {
            return false;
        }
        match destination.write(bytes) {
            Ok(0) => return false,
            Ok(written) => bytes = &bytes[written..],
            Err(error) if is_retryable_io(&error) => continue,
            Err(_) => {
                // A failed destination means the peer cannot receive this
                // direction; wake the other worker as well.
                connection.close();
                return false;
            }
        }
    }
    true
}

fn is_retryable_io(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_indices_are_stable() {
        assert_eq!(Direction::ClientToServer.index(), 0);
        assert_eq!(Direction::ServerToClient.index(), 1);
    }

    #[test]
    fn zero_watchdog_is_rejected() {
        let result = TransparentProxy::bind_with_watchdog(
            SocketAddr::from(([127, 0, 0, 1], 9)),
            Duration::ZERO,
        );
        assert!(matches!(
            result,
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
    }
}
