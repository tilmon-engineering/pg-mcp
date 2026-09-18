//! Bounded subprocess shutdown/exit matrix.
//!
//! The SIGINT case uses the real POSIX signal after a successful MCP initialize
//! handshake.  No production fault flag is used: uncertainty is represented by
//! a deterministic startup/transport condition and the test only asserts the
//! process-level contract where the environment can establish it.

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

const WATCHDOG: Duration = Duration::from_secs(8);
const MAX_CAPTURE: usize = 2 * 1024 * 1024;
const HOMEBREW_LIBPQ_DIR: &str = "/home/linuxbrew/.linuxbrew/opt/libpq/lib";

fn configure_test_loader(command: &mut Command) {
    if std::path::Path::new(HOMEBREW_LIBPQ_DIR).is_dir() {
        let loader_var = if cfg!(target_os = "macos") {
            "DYLD_LIBRARY_PATH"
        } else {
            "LD_LIBRARY_PATH"
        };
        let existing = std::env::var_os(loader_var).unwrap_or_default();
        let mut paths = std::env::split_paths(&existing).collect::<Vec<_>>();
        paths.insert(0, HOMEBREW_LIBPQ_DIR.into());
        command.env(
            if cfg!(target_os = "macos") {
                "DYLD_LIBRARY_PATH"
            } else {
                "LD_LIBRARY_PATH"
            },
            std::env::join_paths(paths).expect("valid loader paths"),
        );
    }
}

struct Process {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Receiver<Result<Vec<u8>, String>>,
    stderr: Receiver<Vec<u8>>,
    signal_identity: Option<String>,
}

impl Process {
    fn spawn(envs: &[(&str, &str)]) -> Result<Self, String> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_postgres-mcp"));
        configure_test_loader(&mut command);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for &(key, value) in envs {
            command.env(key, value);
        }
        let mut child = command.spawn().map_err(|error| format!("spawn: {error}"))?;
        let child_stdout = child.stdout.take().ok_or("missing stdout")?;
        let stderr = child.stderr.take().ok_or("missing stderr")?;
        let (stdout_tx, stdout_rx) = mpsc::channel();
        thread::Builder::new()
            .name("postgres-mcp-shutdown-stdout".to_owned())
            .spawn(move || {
                let mut reader = BufReader::new(child_stdout);
                loop {
                    let mut line = Vec::new();
                    match reader.read_until(b'\n', &mut line) {
                        Ok(0) => break,
                        Ok(_) if line.len() <= MAX_CAPTURE => {
                            let _ = stdout_tx.send(Ok(line));
                        }
                        Ok(_) => {
                            let _ = stdout_tx.send(Err("stdout capture limit exceeded".to_owned()));
                            break;
                        }
                        Err(error) => {
                            let _ = stdout_tx.send(Err(format!("stdout read: {error}")));
                            break;
                        }
                    }
                }
            })
            .map_err(|error| format!("stdout drain: {error}"))?;
        let (stderr_tx, stderr_rx) = mpsc::channel();
        thread::Builder::new()
            .name("postgres-mcp-shutdown-stderr".to_owned())
            .spawn(move || {
                let reader = BufReader::new(stderr);
                let mut bytes = Vec::new();
                let _ = reader
                    .take((MAX_CAPTURE + 1) as u64)
                    .read_to_end(&mut bytes);
                let _ = stderr_tx.send(bytes);
            })
            .map_err(|error| format!("stderr drain: {error}"))?;
        Ok(Self {
            stdin: child.stdin.take(),
            child,
            stdout: stdout_rx,
            stderr: stderr_rx,
            signal_identity: None,
        })
    }

    fn send(&mut self, message: Value) -> Result<(), String> {
        let stdin = self.stdin.as_mut().ok_or("stdin closed")?;
        writeln!(stdin, "{message}").map_err(|error| format!("write: {error}"))?;
        stdin.flush().map_err(|error| format!("flush: {error}"))
    }

    fn initialize(&mut self) -> Result<Value, String> {
        self.send(json!({
            "jsonrpc":"2.0", "id":1, "method":"initialize",
            "params": {
                "protocolVersion":"2025-03-26", "capabilities":{},
                "clientInfo":{"name":"shutdown-matrix","version":"1"}
            }
        }))?;
        self.receive_json()
    }

    fn receive_json(&self) -> Result<Value, String> {
        match self.stdout.recv_timeout(WATCHDOG) {
            Ok(Ok(bytes)) => {
                serde_json::from_slice(&bytes).map_err(|error| format!("JSON: {error}"))
            }
            Ok(Err(error)) => Err(error),
            Err(error) => Err(format!("stdout watchdog: {error}")),
        }
    }

    fn close_stdin(&mut self) {
        self.stdin.take();
    }

    fn wait(&mut self) -> Result<std::process::ExitStatus, String> {
        let deadline = Instant::now() + WATCHDOG;
        loop {
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|error| format!("try_wait: {error}"))?
            {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                let stderr = self
                    .stderr
                    .recv_timeout(Duration::from_millis(250))
                    .unwrap_or_else(|_| b"<stderr unavailable>".to_vec());
                return Err(format!(
                    "child watchdog expired; identity: {}; stderr: {}",
                    self.signal_identity.as_deref().unwrap_or("<not signaled>"),
                    String::from_utf8_lossy(&stderr)
                ));
            }
            // This is watchdog polling, not race coordination. Yield-only
            // spinning can starve the child on a saturated test executor.
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn stderr(&self) -> Vec<u8> {
        self.stderr
            .recv_timeout(WATCHDOG)
            .unwrap_or_else(|_| b"<stderr watchdog expired>".to_vec())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.stdin.take();
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn assert_no_secret(stderr: &[u8]) {
    let text = String::from_utf8_lossy(stderr);
    for secret in ["secret-user", "secret-password", "secret-db"] {
        assert!(!text.contains(secret), "secret leaked to stderr: {secret}");
    }
}

#[cfg(unix)]
#[test]
fn sigint_probe_confirms_child_signal_delivery() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sigint-probe"))
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn signal probe");
    let stdout = child.stdout.take().expect("probe stdout");
    let mut line = String::new();
    BufReader::new(stdout)
        .read_line(&mut line)
        .expect("probe ready line");
    assert_eq!(line, "ready\n");
    let result = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    assert_eq!(result, 0, "libc::kill(SIGINT) probe");
    let deadline = Instant::now() + WATCHDOG;
    loop {
        if let Some(status) = child.try_wait().expect("probe wait") {
            assert_eq!(status.code(), Some(0));
            break;
        }
        assert!(Instant::now() < deadline, "probe did not exit after SIGINT");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn stdin_eof_is_verified_clean_exit() {
    let mut process =
        Process::spawn(&[("PG_MCP_DSN", "postgres://127.0.0.1:1/unused")]).expect("spawn");
    let initialize = process.initialize().expect("initialized handshake");
    assert!(
        initialize["result"].is_object(),
        "initialize response: {initialize}"
    );
    process.close_stdin();
    let status = process.wait().expect("bounded EOF shutdown");
    let stderr = process.stderr();
    assert_no_secret(&stderr);
    // A connection is not opened until open_database, so EOF has no worker
    // cleanup and should be a verified clean exit.
    assert_eq!(
        status.code(),
        Some(0),
        "EOF stderr: {}",
        String::from_utf8_lossy(&stderr)
    );
}

#[cfg(unix)]
#[test]
fn real_sigint_after_initialized_handshake_is_clean_exit() {
    let mut process =
        Process::spawn(&[("PG_MCP_DSN", "postgres://127.0.0.1:1/unused")]).expect("spawn");
    let initialize = process.initialize().expect("initialized handshake");
    assert_eq!(initialize["id"], 1);
    assert!(
        initialize["result"].is_object(),
        "initialize response: {initialize}"
    );
    // Complete MCP's initialization notification before signaling. This is the
    // protocol-level readiness boundary; it avoids racing the server's own
    // post-initialize setup with an OS signal.
    process
        .send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .expect("initialized notification");

    let pid = process.child.id() as libc::pid_t;
    // SAFETY: pid came from the live child process we just spawned. Record the
    // process identity in failure output because Cargo test binaries may use a
    // launch shim that differs from the server process on some platforms.
    let identity = std::fs::read_to_string(format!("/proc/{pid}/cmdline"))
        .unwrap_or_else(|_| "<cmdline unavailable>".to_owned())
        .replace('\0', " ");
    let sigint_state = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .map(|status| {
            status
                .lines()
                .filter(|line| line.starts_with("SigCgt:") || line.starts_with("SigIgn:"))
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_else(|| "<signal state unavailable>".to_owned());
    process.signal_identity = Some(format!("{identity}; {sigint_state}"));
    let result = unsafe { libc::kill(pid, libc::SIGINT) };
    assert_eq!(result, 0, "libc::kill(SIGINT) failed for {identity}");
    let status = process.wait().expect("bounded SIGINT shutdown");
    let stderr = process.stderr();
    assert_no_secret(&stderr);
    assert_eq!(
        status.code(),
        Some(0),
        "SIGINT stderr: {}",
        String::from_utf8_lossy(&stderr)
    );
}

#[test]
fn startup_validation_is_exit_two_and_never_protocol_stdout() {
    let directory = tempfile::tempdir().expect("tempdir");
    let config = directory.path().join("invalid.toml");
    std::fs::write(&config, "unknown = true\n").expect("write config");
    let mut command = Command::new(env!("CARGO_BIN_EXE_postgres-mcp"));
    configure_test_loader(&mut command);
    let output = command
        .args(["--config", config.to_str().expect("path")])
        .env(
            "PG_MCP_DSN",
            "postgres://secret-user:secret-password@127.0.0.1/secret-db",
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run startup validation");
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_no_secret(&output.stderr);
}

#[test]
fn serve_failure_is_nonzero_and_bounded() {
    let mut process =
        Process::spawn(&[("PG_MCP_DSN", "postgres://127.0.0.1:1/unused")]).expect("spawn");
    // MCP requires initialize first. Sending an invalid first request causes
    // rmcp initialization failure and exercises the same cleanup owner.
    process
        .send(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query","arguments":{}}}))
        .expect("invalid initialize request");
    let status = process.wait().expect("bounded serve failure");
    let stderr = process.stderr();
    assert_no_secret(&stderr);
    assert_ne!(status.code(), Some(0));
}

#[test]
fn output_watchdog_is_finite_even_when_initialize_fails() {
    let mut process =
        Process::spawn(&[("PG_MCP_DSN", "postgres://127.0.0.1:1/unused")]).expect("spawn");
    process
        .send(json!({"jsonrpc":"2.0","id":1,"method":"not_initialize","params":{}}))
        .expect("invalid request");
    let _ = process.wait().expect("watchdog bounded");
    let stderr = process.stderr();
    assert!(stderr.len() <= MAX_CAPTURE + 1);
}
