use postgres_mcp::{serve_profiles, serve_stdio};
use postgres_mcp_core::{
    config::{Config, ConfigFile, DSN_ENVIRONMENT_VARIABLE},
    protocol::ErrorCode,
};
use std::{
    env,
    path::PathBuf,
    process::ExitCode,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(unix)]
static SIGINT_RECEIVED: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn sigint_handler(_signal: libc::c_int) {
    // Atomic stores are async-signal-safe. Do not allocate, lock, log, or call
    // Tokio from a signal handler.
    SIGINT_RECEIVED.store(true, Ordering::Release);
}

#[cfg(unix)]
struct SigintBridge {
    previous: libc::sigaction,
}

#[cfg(unix)]
impl SigintBridge {
    fn install() -> Result<Self, std::io::Error> {
        SIGINT_RECEIVED.store(false, Ordering::Release);
        // SAFETY: zero-initializing sigaction is documented; fields are then
        // initialized before sigaction receives it. The handler only performs
        // an atomic store and is compatible with the C signal ABI.
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = sigint_handler as *const () as usize;
        action.sa_flags = libc::SA_RESTART;
        // SAFETY: action/old_action point to valid storage.
        unsafe {
            libc::sigemptyset(&mut action.sa_mask);
            let mut previous: libc::sigaction = std::mem::zeroed();
            if libc::sigaction(libc::SIGINT, &action, &mut previous) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Self { previous })
        }
    }
}

#[cfg(unix)]
impl Drop for SigintBridge {
    fn drop(&mut self) {
        // SAFETY: this restores the action captured during successful install.
        unsafe {
            libc::sigaction(libc::SIGINT, &self.previous, std::ptr::null_mut());
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Serve { config: Option<PathBuf> },
    Help,
    Version,
}

fn usage() -> &'static str {
    "Usage: postgres-mcp [--config ABSOLUTE_PATH]\n\nRun the PostgreSQL MCP server over stdio.\n\nOptions:\n    --config ABSOLUTE_PATH  Load and validate TOML configuration\n    -h, --help              Show this help\n    -V, --version           Show version\n"
}

fn parse_args<I>(args: I) -> Result<Command, String>
where
    I: IntoIterator<Item = String>,
{
    let mut config = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--config requires an absolute path".to_owned())?;
                let path = PathBuf::from(value);
                if !path.is_absolute() {
                    return Err("--config requires an absolute path".to_owned());
                }
                config = Some(path);
            }
            value if value.starts_with('-') => return Err(format!("unknown option: {value}")),
            value => return Err(format!("unexpected argument: {value}")),
        }
    }
    Ok(Command::Serve { config })
}

enum LoadedConfig {
    Legacy(Config),
    Profiles(Box<ConfigFile>),
}

fn load_config(path: Option<PathBuf>) -> Result<LoadedConfig, String> {
    match path {
        Some(path) => {
            let source = std::fs::read_to_string(path)
                .map_err(|_| ErrorCode::ConfigError.message().to_owned())?;
            if ConfigFile::is_profile_document(&source)
                .map_err(|_| ErrorCode::ConfigError.message())?
            {
                let config =
                    ConfigFile::from_toml(&source).map_err(|_| ErrorCode::ConfigError.message())?;
                if config.profiles.is_empty() {
                    return Err(ErrorCode::ConfigError.message().to_owned());
                }
                Ok(LoadedConfig::Profiles(Box::new(config)))
            } else {
                Ok(LoadedConfig::Legacy(
                    Config::from_toml(&source).map_err(|_| ErrorCode::ConfigError.message())?,
                ))
            }
        }
        None => Ok(LoadedConfig::Legacy(Config::default())),
    }
}

async fn run() -> ExitCode {
    let command = match parse_args(env::args().skip(1)) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("postgres-mcp: {error}\n{}", usage());
            return ExitCode::from(2);
        }
    };
    match command {
        Command::Help => {
            print!("{}", usage());
            ExitCode::SUCCESS
        }
        Command::Version => {
            println!("postgres-mcp {VERSION}");
            ExitCode::SUCCESS
        }
        Command::Serve { config } => {
            let config = match load_config(config) {
                Ok(config) => config,
                Err(error) => {
                    eprintln!("postgres-mcp: {error}");
                    return ExitCode::from(2);
                }
            };
            let (shutdown_timeout, serve_profiles_config, legacy_dsn) = match config {
                LoadedConfig::Profiles(config) => (config.shutdown_total(), Some(config), None),
                LoadedConfig::Legacy(config) => {
                    let dsn = match env::var(DSN_ENVIRONMENT_VARIABLE) {
                        Ok(dsn) if !dsn.is_empty() => dsn,
                        _ => {
                            eprintln!("postgres-mcp: {} is required", DSN_ENVIRONMENT_VARIABLE);
                            return ExitCode::from(2);
                        }
                    };
                    (config.shutdown_total(), None, Some((config, dsn)))
                }
            };
            let _sigint_bridge = match SigintBridge::install() {
                Ok(bridge) => bridge,
                Err(error) => {
                    eprintln!("postgres-mcp: signal setup failed: {error}");
                    return ExitCode::from(1);
                }
            };
            let shutdown_ct = CancellationToken::new();
            let monitor_ct = shutdown_ct.clone();
            let monitor_stop = std::sync::Arc::new(AtomicBool::new(false));
            let monitor_stop_thread = monitor_stop.clone();
            let signal_monitor = std::thread::spawn(move || {
                while !monitor_stop_thread.load(Ordering::Acquire) {
                    if SIGINT_RECEIVED.load(Ordering::Acquire) {
                        monitor_ct.cancel();
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            });
            let serve_ct = shutdown_ct.clone();
            let mut serve_task = tokio::spawn(async move {
                match (serve_profiles_config, legacy_dsn) {
                    (Some(config), None) => serve_profiles(*config, serve_ct).await,
                    (None, Some((config, dsn))) => serve_stdio(config, dsn, serve_ct).await,
                    _ => unreachable!(),
                }
            });
            let outcome = tokio::select! {
                result = &mut serve_task => result.unwrap_or_else(|error| Err(postgres_mcp::ServeFailure {
                    primary: format!("server task failed: {error}"),
                    cleanup_errors: Vec::new(),
                })),
                _ = shutdown_ct.cancelled() => {
                    // The monitor observed SIGINT and cancelled this token. The
                    // cleanup owner is inside serve_stdio; wait for its bounded
                    // report rather than bypassing it at the boundary.
                    match tokio::time::timeout(shutdown_timeout, &mut serve_task).await {
                        Ok(result) => result.unwrap_or_else(|error| Err(postgres_mcp::ServeFailure {
                            primary: format!("server task failed during shutdown: {error}"),
                            cleanup_errors: Vec::new(),
                        })),
                        Err(_) => {
                            eprintln!("postgres-mcp: shutdown deadline exceeded");
                            // A native C/platform call cannot be safely killed
                            // from Core. Enforce the process boundary instead
                            // of claiming that cleanup was verified.
                            std::process::exit(1);
                        }
                    }
                }
            };
            monitor_stop.store(true, Ordering::Release);
            let _ = signal_monitor.join();
            match outcome {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("postgres-mcp: {error}");
                    ExitCode::from(1)
                }
            }
        }
    }
}

fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("postgres-mcp: runtime setup failed: {error}");
            return ExitCode::from(1);
        }
    };
    let code = runtime.block_on(run());
    // Tokio's stdio helper performs an uncancellable blocking read. Core and
    // rmcp cleanup already completed above; do not let that helper defeat the
    // documented process-level shutdown deadline.
    runtime.shutdown_background();
    code
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_strict_absolute_config() {
        assert_eq!(
            parse_args(["--config".to_owned(), "/tmp/server.toml".to_owned()]),
            Ok(Command::Serve {
                config: Some(PathBuf::from("/tmp/server.toml"))
            })
        );
        assert!(parse_args(["--config".to_owned(), "relative.toml".to_owned()]).is_err());
    }

    #[test]
    fn help_and_version_do_not_enter_serve() {
        assert_eq!(parse_args(["--help".to_owned()]), Ok(Command::Help));
        assert_eq!(parse_args(["--version".to_owned()]), Ok(Command::Version));
    }
}
