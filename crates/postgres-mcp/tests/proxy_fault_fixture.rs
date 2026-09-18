//! Acceptance tests for the test-only transparent TCP proxy.
//!
//! These tests use a tiny byte echo peer rather than PostgreSQL. That keeps
//! the fixture independent of libpq and verifies the properties the proxy
//! actually promises: transparent byte forwarding, independently controlled
//! directions, deterministic barriers, disconnect behavior, and bounded
//! teardown.

#[path = "support/proxy.rs"]
mod proxy;

use proxy::{Direction, TransparentProxy};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const WATCHDOG: Duration = Duration::from_secs(3);
const IO_TIMEOUT: Duration = Duration::from_millis(100);

struct EchoPeer {
    address: SocketAddr,
    stop: Option<Sender<()>>,
    received: Receiver<Vec<u8>>,
    done: Receiver<io::Result<Vec<u8>>>,
    thread: Option<JoinHandle<()>>,
}

impl EchoPeer {
    fn bind() -> io::Result<Self> {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
        let address = listener.local_addr()?;
        let (stop, stop_rx) = mpsc::channel();
        let (received_tx, received) = mpsc::channel();
        let (done_tx, done) = mpsc::channel();
        let thread = thread::Builder::new()
            .name("postgres-mcp-test-echo-peer".to_owned())
            .spawn(move || {
                let result = echo_peer_loop(listener, stop_rx, received_tx);
                let _ = done_tx.send(result);
            })?;
        Ok(Self {
            address,
            stop: Some(stop),
            received,
            done,
            thread: Some(thread),
        })
    }

    fn address(&self) -> SocketAddr {
        self.address
    }

    fn wait_for_received(&self, expected: &[u8]) -> io::Result<()> {
        let deadline = Instant::now() + WATCHDOG;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "echo peer receipt watchdog expired",
                ));
            }
            match self.received.recv_timeout(remaining) {
                Ok(received) if received.ends_with(expected) => return Ok(()),
                Ok(_) => continue,
                Err(RecvTimeoutError::Timeout) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "echo peer receipt watchdog expired",
                    ));
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "echo peer receipt channel closed",
                    ));
                }
            }
        }
    }

    fn finish(mut self) -> io::Result<Vec<u8>> {
        self.stop.take();
        let result = self
            .done
            .recv_timeout(WATCHDOG)
            .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error.to_string()))??;
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::other("echo peer panicked"))?;
        }
        Ok(result)
    }
}

impl Drop for EchoPeer {
    fn drop(&mut self) {
        self.stop.take();
        // Tests always close their proxy/client before dropping the peer. The
        // bounded receive avoids an accidental unbounded join on a regression.
        if self.done.recv_timeout(WATCHDOG).is_ok()
            && let Some(thread) = self.thread.take()
        {
            let _ = thread.join();
        }
    }
}

fn echo_peer_loop(
    listener: TcpListener,
    stop_rx: Receiver<()>,
    received_tx: Sender<Vec<u8>>,
) -> io::Result<Vec<u8>> {
    listener.set_nonblocking(true)?;
    let deadline = Instant::now() + WATCHDOG;
    let stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if matches!(
                    stop_rx.try_recv(),
                    Ok(()) | Err(mpsc::TryRecvError::Disconnected)
                ) {
                    return Ok(Vec::new());
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "echo peer accept watchdog expired",
                    ));
                }
                thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    };
    stream.set_read_timeout(Some(WATCHDOG))?;
    stream.set_write_timeout(Some(WATCHDOG))?;
    echo_connection(stream, stop_rx, received_tx)
}

fn echo_connection(
    mut stream: TcpStream,
    stop_rx: Receiver<()>,
    received_tx: Sender<Vec<u8>>,
) -> io::Result<Vec<u8>> {
    let mut received = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        if matches!(
            stop_rx.try_recv(),
            Ok(()) | Err(mpsc::TryRecvError::Disconnected)
        ) {
            return Ok(received);
        }
        match stream.read(&mut buffer) {
            Ok(0) => return Ok(received),
            Ok(length) => {
                received.extend_from_slice(&buffer[..length]);
                let _ = received_tx.send(received.clone());
                stream.write_all(&buffer[..length])?;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::Interrupted
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::WouldBlock
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        }
    }
}

fn connect_to_proxy(proxy: &TransparentProxy) -> io::Result<TcpStream> {
    let stream = TcpStream::connect_timeout(&proxy.local_addr(), IO_TIMEOUT)?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    proxy.wait_for_connection()?;
    Ok(stream)
}

fn read_exact_with_deadline(
    stream: &mut TcpStream,
    bytes: &mut [u8],
    deadline: Instant,
) -> io::Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "read watchdog expired",
            ));
        }
        stream.set_read_timeout(Some(remaining.min(IO_TIMEOUT)))?;
        match stream.read(&mut bytes[offset..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "peer disconnected before expected bytes",
                ));
            }
            Ok(length) => offset += length,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::Interrupted
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::WouldBlock
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn assert_no_byte_before_deadline(stream: &mut TcpStream, deadline: Instant) -> io::Result<()> {
    let mut byte = [0_u8; 1];
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        stream.set_read_timeout(Some(remaining.min(IO_TIMEOUT)))?;
        match stream.read(&mut byte) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed")),
            Ok(_) => return Err(io::Error::other("unexpected byte crossed pause barrier")),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::Interrupted
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::WouldBlock
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[test]
fn proxy_preserves_bytes_and_controls_each_direction() -> io::Result<()> {
    let echo = EchoPeer::bind()?;
    let mut proxy = TransparentProxy::bind_with_watchdog(echo.address(), WATCHDOG)?;
    let mut client = connect_to_proxy(&proxy)?;
    let control = proxy.control();

    let first = [0_u8, 1, 2, 3, 0xff, b'\n', 0, 0x80, 0xfe];
    client.write_all(&first)?;
    let mut echoed = vec![0_u8; first.len()];
    read_exact_with_deadline(&mut client, &mut echoed, Instant::now() + WATCHDOG)?;
    assert_eq!(echoed, first);

    // Pause only client -> server. The client can write into the proxy-side
    // kernel buffer, but no bytes reach the peer until this direction resumes.
    control.pause(Direction::ClientToServer)?;
    let held_client_to_server = b"held in client direction";
    client.write_all(held_client_to_server)?;
    assert_no_byte_before_deadline(&mut client, Instant::now() + IO_TIMEOUT)?;
    control.resume(Direction::ClientToServer)?;
    let mut resumed = vec![0_u8; held_client_to_server.len()];
    read_exact_with_deadline(&mut client, &mut resumed, Instant::now() + WATCHDOG)?;
    assert_eq!(resumed, held_client_to_server);

    // Pause only server -> client. Client -> server still progresses: the
    // echo peer receives this payload while its response waits behind S2C.
    control.pause(Direction::ServerToClient)?;
    let held_server_to_client = b"held in server direction";
    client.write_all(held_server_to_client)?;
    echo.wait_for_received(held_server_to_client)?;
    assert_no_byte_before_deadline(&mut client, Instant::now() + IO_TIMEOUT)?;
    control.resume(Direction::ServerToClient)?;
    let mut resumed = vec![0_u8; held_server_to_client.len()];
    read_exact_with_deadline(&mut client, &mut resumed, Instant::now() + WATCHDOG)?;
    assert_eq!(resumed, held_server_to_client);

    client.shutdown(Shutdown::Both)?;
    proxy.shutdown()?;
    let received = echo.finish()?;
    assert_eq!(
        received,
        [
            first.as_slice(),
            held_client_to_server,
            held_server_to_client
        ]
        .concat()
    );
    Ok(())
}

#[test]
fn proxy_drop_disconnects_and_tears_down_within_watchdog() -> io::Result<()> {
    let echo = EchoPeer::bind()?;
    let mut proxy = TransparentProxy::bind_with_watchdog(echo.address(), WATCHDOG)?;
    let mut client = connect_to_proxy(&proxy)?;
    let control = proxy.control();

    control.pause(Direction::ServerToClient)?;
    let sent_before_disconnect = b"discarded before disconnect";
    client.write_all(sent_before_disconnect)?;
    echo.wait_for_received(sent_before_disconnect)?;
    control.drop_connections()?;
    assert_eq!(control.connection_count(), 0);

    let deadline = Instant::now() + WATCHDOG;
    let mut one = [0_u8; 1];
    loop {
        client.set_read_timeout(Some(deadline.saturating_duration_since(Instant::now())))?;
        match client.read(&mut one) {
            Ok(0) => break,
            Ok(_) => panic!("dropped proxy delivered a byte"),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::Interrupted
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::WouldBlock
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "disconnect watchdog expired",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }
    drop(client);
    proxy.shutdown()?;
    let received = echo.finish()?;
    assert_eq!(received, sent_before_disconnect);
    Ok(())
}

#[test]
fn proxy_shutdown_is_bounded_without_a_fixed_sleep() -> io::Result<()> {
    let echo = EchoPeer::bind()?;
    let mut proxy = TransparentProxy::bind_with_watchdog(echo.address(), WATCHDOG)?;
    let client = connect_to_proxy(&proxy)?;
    let started = Instant::now();
    proxy.shutdown()?;
    assert!(started.elapsed() <= WATCHDOG + Duration::from_secs(1));
    drop(client);
    let _ = echo.finish()?;
    Ok(())
}

#[test]
fn proxy_rejects_zero_watchdog() {
    let target = SocketAddr::from(([127, 0, 0, 1], 9));
    let result = TransparentProxy::bind_with_watchdog(target, Duration::ZERO);
    assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
}
