//! Black-box stdio contract tests for the production PostgreSQL MCP binary.
//!
//! These tests intentionally avoid a live PostgreSQL dependency.  Startup
//! validation and protocol discovery are tested with an invalid configuration,
//! while the wire-shape assertions are independently authored against the MCP
//! JSON-RPC contract.  Live database workflow coverage belongs to the parent
//! fixture suite.

use postgres_mcp::TOOL_NAMES;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

const WATCHDOG: Duration = Duration::from_secs(5);
const MAX_OUTPUT: usize = 2 * 1024 * 1024;
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

struct ChildHarness {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<Result<Vec<u8>, String>>,
    stderr: Receiver<Vec<u8>>,
}

impl ChildHarness {
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
        let stdout = child.stdout.take().ok_or("missing child stdout")?;
        let child_stderr = child.stderr.take().ok_or("missing child stderr")?;
        let (line_tx, lines) = mpsc::channel();
        thread::Builder::new()
            .name("postgres-mcp-stdio-contract-stdout".to_owned())
            .spawn(move || {
                let mut reader = BufReader::new(stdout);
                loop {
                    let mut line = Vec::new();
                    match reader.read_until(b'\n', &mut line) {
                        Ok(0) => break,
                        Ok(_) if line.len() <= MAX_OUTPUT => {
                            let _ = line_tx.send(Ok(line));
                        }
                        Ok(_) => {
                            let _ = line_tx.send(Err("child stdout exceeded test cap".to_owned()));
                            break;
                        }
                        Err(error) => {
                            let _ = line_tx.send(Err(format!("read child stdout: {error}")));
                            break;
                        }
                    }
                }
            })
            .map_err(|error| format!("stdout drain thread: {error}"))?;
        let (stderr_tx, stderr_rx) = mpsc::channel();
        thread::Builder::new()
            .name("postgres-mcp-stdio-contract-stderr".to_owned())
            .spawn(move || {
                let reader = BufReader::new(child_stderr);
                let mut bytes = Vec::new();
                let _ = reader.take((MAX_OUTPUT + 1) as u64).read_to_end(&mut bytes);
                let _ = stderr_tx.send(bytes);
            })
            .map_err(|error| format!("stderr drain thread: {error}"))?;
        Ok(Self {
            stdin: child.stdin.take(),
            child,
            lines,
            stderr: stderr_rx,
        })
    }

    fn send(&mut self, value: Value) -> Result<(), String> {
        let stdin = self.stdin.as_mut().ok_or("child stdin closed")?;
        writeln!(stdin, "{value}").map_err(|error| format!("write request: {error}"))?;
        stdin
            .flush()
            .map_err(|error| format!("flush request: {error}"))
    }

    fn receive(&self) -> Result<Value, String> {
        match self.lines.recv_timeout(WATCHDOG) {
            Ok(Ok(line)) => {
                serde_json::from_slice(&line).map_err(|error| format!("invalid JSON: {error}"))
            }
            Ok(Err(error)) => Err(error),
            Err(error) => Err(format!("child stdout watchdog: {error}")),
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
                .map_err(|error| format!("poll child: {error}"))?
            {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                return Err("child watchdog expired".to_owned());
            }
            thread::yield_now();
        }
    }

    fn stderr_bytes(&self) -> Vec<u8> {
        self.stderr
            .recv_timeout(WATCHDOG)
            .unwrap_or_else(|_| b"<stderr drain watchdog expired>".to_vec())
    }
}

impl Drop for ChildHarness {
    fn drop(&mut self) {
        self.stdin.take();
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn initialize_request(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "postgres-mcp-contract", "version": "1"}
        }
    })
}

#[test]
fn invalid_startup_is_non_protocol_and_redacted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = dir.path().join("invalid.toml");
    std::fs::write(&config, "unknown_field = true\n").expect("write config");
    let mut command = Command::new(env!("CARGO_BIN_EXE_postgres-mcp"));
    configure_test_loader(&mut command);
    let output = command
        .args(["--config", config.to_str().expect("config path")])
        .env(
            "PG_MCP_DSN",
            "postgres://secret-user:secret-password@127.0.0.1:5432/secret-db",
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run invalid config");
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty(), "startup emitted protocol bytes");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Invalid server configuration."));
    assert!(!stderr.contains("secret-password"));
    assert!(!stderr.contains("secret-user"));
    assert!(!stderr.contains("secret-db"));
}

#[test]
fn help_version_and_relative_config_are_cli_contracts() {
    let mut help_command = Command::new(env!("CARGO_BIN_EXE_postgres-mcp"));
    configure_test_loader(&mut help_command);
    let help = help_command.arg("--help").output().expect("help");
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("--config ABSOLUTE_PATH"));
    assert!(help.stderr.is_empty());

    let mut version_command = Command::new(env!("CARGO_BIN_EXE_postgres-mcp"));
    configure_test_loader(&mut version_command);
    let version = version_command.arg("--version").output().expect("version");
    assert!(version.status.success());
    assert!(String::from_utf8_lossy(&version.stdout).starts_with("postgres-mcp "));

    let mut relative_command = Command::new(env!("CARGO_BIN_EXE_postgres-mcp"));
    configure_test_loader(&mut relative_command);
    let relative = relative_command
        .args(["--config", "relative.toml"])
        .output()
        .expect("relative config");
    assert_eq!(relative.status.code(), Some(2));
    assert!(relative.stdout.is_empty());
}

#[test]
fn initialize_and_tools_list_are_jsonrpc_shapes() {
    // Opening a PostgreSQL handle is deliberately not needed for protocol
    // discovery: Core only parses/uses the DSN when open_database is called.
    let mut child = ChildHarness::spawn(&[("PG_MCP_DSN", "postgres://127.0.0.1:1/unused")])
        .expect("spawn binary");
    child.send(initialize_request(1)).expect("send initialize");
    let initialize = child.receive().expect("initialize response");
    assert_eq!(initialize["jsonrpc"], "2.0");
    assert_eq!(initialize["id"], 1);
    assert!(initialize["result"].is_object());

    child
        .send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .expect("send initialized notification");
    child
        .send(json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}))
        .expect("send tools list");
    let listed = child.receive().expect("tools/list response");
    assert_eq!(listed["jsonrpc"], "2.0");
    let tools = listed["result"]["tools"].as_array().expect("tools array");
    let mut names = tools
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    let mut expected = TOOL_NAMES.to_vec();
    expected.sort_unstable();
    assert_eq!(names, expected);

    child.close_stdin();
    let status = child.wait().expect("bounded EOF shutdown");
    let _stderr = child.stderr_bytes();
    assert_eq!(status.code(), Some(0));
}
