//! Configuration for process policy and named PostgreSQL connection profiles.

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    os::unix::{fs::OpenOptionsExt, process::CommandExt},
    path::Path,
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub const DSN_ENVIRONMENT_VARIABLE: &str = "PG_MCP_DSN";
pub const DEFAULT_MAX_HANDLES: usize = 16;
pub const DEFAULT_OUTSTANDING_REQUESTS: usize = 32;
pub const DEFAULT_SQL_BYTES: usize = 65_536;
pub const DEFAULT_PARAMETERS: usize = 256;
pub const DEFAULT_PARAMETER_BYTES: usize = 1_048_576;
pub const DEFAULT_RESULT_ROWS: usize = 1_000;
pub const DEFAULT_COLUMNS: usize = 256;
pub const DEFAULT_CELL_BYTES: usize = 1_048_576;
pub const DEFAULT_RESULT_JSON_BYTES: usize = 8_388_608;
pub const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_CONNECTION_TIMEOUT_SECONDS: u64 = 10;
pub const DEFAULT_CANCEL_DRAIN_GRACE_SECONDS: u64 = 5;
pub const DEFAULT_SHUTDOWN_TOTAL_SECONDS: u64 = 10;
pub const MAX_HANDLES_RANGE: (usize, usize) = (1, 128);
pub const OUTSTANDING_REQUESTS_RANGE: (usize, usize) = (1, 1_024);
pub const SQL_BYTES_RANGE: (usize, usize) = (1, 1_048_576);
pub const PARAMETERS_RANGE: (usize, usize) = (0, 65_535);
pub const PARAMETER_BYTES_RANGE: (usize, usize) = (0, 16_777_216);
pub const RESULT_ROWS_RANGE: (usize, usize) = (1, 100_000);
pub const COLUMNS_RANGE: (usize, usize) = (1, 1_600);
pub const CELL_BYTES_RANGE: (usize, usize) = (1, 16_777_216);
pub const RESULT_JSON_BYTES_RANGE: (usize, usize) = (1_024, 67_108_864);
pub const TIMEOUT_SECONDS_RANGE: (u64, u64) = (1, 300);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_max_handles")]
    pub max_handles: usize,
    #[serde(default = "default_outstanding_requests")]
    pub outstanding_requests: usize,
    #[serde(default = "default_sql_bytes")]
    pub sql_bytes: usize,
    #[serde(default = "default_parameters")]
    pub parameters: usize,
    #[serde(default = "default_parameter_bytes")]
    pub parameter_bytes: usize,
    #[serde(default = "default_result_rows")]
    pub result_rows: usize,
    #[serde(default = "default_columns")]
    pub columns: usize,
    #[serde(default = "default_cell_bytes")]
    pub cell_bytes: usize,
    #[serde(default = "default_result_json_bytes")]
    pub result_json_bytes: usize,
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    #[serde(default = "default_connection_timeout_seconds")]
    pub connection_timeout_seconds: u64,
    #[serde(default = "default_cancel_drain_grace_seconds")]
    pub cancel_drain_grace_seconds: u64,
    #[serde(default = "default_shutdown_total_seconds")]
    pub shutdown_total_seconds: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigOverrides {
    pub max_handles: Option<usize>,
    pub outstanding_requests: Option<usize>,
    pub sql_bytes: Option<usize>,
    pub parameters: Option<usize>,
    pub parameter_bytes: Option<usize>,
    pub result_rows: Option<usize>,
    pub columns: Option<usize>,
    pub cell_bytes: Option<usize>,
    pub result_json_bytes: Option<usize>,
    pub request_timeout_seconds: Option<u64>,
    pub connection_timeout_seconds: Option<u64>,
    pub cancel_drain_grace_seconds: Option<u64>,
    pub shutdown_total_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub dsn: String,
    #[serde(flatten)]
    pub overrides: ConfigOverrides,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigFile {
    #[serde(default)]
    pub defaults: ConfigOverrides,
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
    #[serde(flatten)]
    pub legacy: ConfigOverrides,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read configuration: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid TOML configuration: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("{field} must be {minimum}..={maximum} (got {value})")]
    Invalid {
        field: &'static str,
        minimum: u64,
        maximum: u64,
        value: u64,
    },
    #[error("profile is not configured")]
    UnknownProfile,
    #[error("profile DSN command failed")]
    DsnCommand,
    #[error("profile DSN command returned empty output")]
    EmptyDsn,
    #[error("profile DSN command returned invalid UTF-8")]
    InvalidDsnOutput,
    #[error("profile DSN command timed out")]
    DsnTimeout,
}

macro_rules! defaults { ($($name:ident : $ty:ty = $value:expr),+ $(,)?) => { $(fn $name() -> $ty { $value })+ }; }
defaults!(default_max_handles: usize = DEFAULT_MAX_HANDLES, default_outstanding_requests: usize = DEFAULT_OUTSTANDING_REQUESTS, default_sql_bytes: usize = DEFAULT_SQL_BYTES, default_parameters: usize = DEFAULT_PARAMETERS, default_parameter_bytes: usize = DEFAULT_PARAMETER_BYTES, default_result_rows: usize = DEFAULT_RESULT_ROWS, default_columns: usize = DEFAULT_COLUMNS, default_cell_bytes: usize = DEFAULT_CELL_BYTES, default_result_json_bytes: usize = DEFAULT_RESULT_JSON_BYTES, default_request_timeout_seconds: u64 = DEFAULT_REQUEST_TIMEOUT_SECONDS, default_connection_timeout_seconds: u64 = DEFAULT_CONNECTION_TIMEOUT_SECONDS, default_cancel_drain_grace_seconds: u64 = DEFAULT_CANCEL_DRAIN_GRACE_SECONDS, default_shutdown_total_seconds: u64 = DEFAULT_SHUTDOWN_TOTAL_SECONDS);

impl Default for Config {
    fn default() -> Self {
        ConfigOverrides::default().into_config()
    }
}

impl ConfigOverrides {
    pub(crate) fn merge(&self, higher: &Self) -> Self {
        macro_rules! m { ($($f:ident),+) => { Self { $($f: higher.$f.or(self.$f),)+ } }; }
        m!(
            max_handles,
            outstanding_requests,
            sql_bytes,
            parameters,
            parameter_bytes,
            result_rows,
            columns,
            cell_bytes,
            result_json_bytes,
            request_timeout_seconds,
            connection_timeout_seconds,
            cancel_drain_grace_seconds,
            shutdown_total_seconds
        )
    }
    fn into_config(self) -> Config {
        Config::default_unchecked().apply(&self)
    }
    fn from_config(c: &Config) -> Self {
        Self {
            max_handles: Some(c.max_handles),
            outstanding_requests: Some(c.outstanding_requests),
            sql_bytes: Some(c.sql_bytes),
            parameters: Some(c.parameters),
            parameter_bytes: Some(c.parameter_bytes),
            result_rows: Some(c.result_rows),
            columns: Some(c.columns),
            cell_bytes: Some(c.cell_bytes),
            result_json_bytes: Some(c.result_json_bytes),
            request_timeout_seconds: Some(c.request_timeout_seconds),
            connection_timeout_seconds: Some(c.connection_timeout_seconds),
            cancel_drain_grace_seconds: Some(c.cancel_drain_grace_seconds),
            shutdown_total_seconds: Some(c.shutdown_total_seconds),
        }
    }
}

impl Config {
    fn default_unchecked() -> Self {
        Self {
            max_handles: DEFAULT_MAX_HANDLES,
            outstanding_requests: DEFAULT_OUTSTANDING_REQUESTS,
            sql_bytes: DEFAULT_SQL_BYTES,
            parameters: DEFAULT_PARAMETERS,
            parameter_bytes: DEFAULT_PARAMETER_BYTES,
            result_rows: DEFAULT_RESULT_ROWS,
            columns: DEFAULT_COLUMNS,
            cell_bytes: DEFAULT_CELL_BYTES,
            result_json_bytes: DEFAULT_RESULT_JSON_BYTES,
            request_timeout_seconds: DEFAULT_REQUEST_TIMEOUT_SECONDS,
            connection_timeout_seconds: DEFAULT_CONNECTION_TIMEOUT_SECONDS,
            cancel_drain_grace_seconds: DEFAULT_CANCEL_DRAIN_GRACE_SECONDS,
            shutdown_total_seconds: DEFAULT_SHUTDOWN_TOTAL_SECONDS,
        }
    }
    fn apply(mut self, o: &ConfigOverrides) -> Self {
        macro_rules! a { ($($f:ident),+) => { $(if let Some(v) = o.$f { self.$f = v; })+ }; }
        a!(
            max_handles,
            outstanding_requests,
            sql_bytes,
            parameters,
            parameter_bytes,
            result_rows,
            columns,
            cell_bytes,
            result_json_bytes,
            request_timeout_seconds,
            connection_timeout_seconds,
            cancel_drain_grace_seconds,
            shutdown_total_seconds
        );
        self
    }
    pub fn from_toml(source: &str) -> Result<Self, ConfigError> {
        let o: ConfigOverrides = toml::from_str(source)?;
        let c = Self::default().apply(&o);
        c.validate()?;
        Ok(c)
    }
    pub fn from_path(path: &Path) -> Result<Self, ConfigError> {
        Self::from_toml(&fs::read_to_string(path)?)
    }
    pub fn load_from_path(path: &Path) -> Result<Self, ConfigError> {
        Self::from_path(path)
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_usize("max_handles", self.max_handles, MAX_HANDLES_RANGE)?;
        validate_usize(
            "outstanding_requests",
            self.outstanding_requests,
            OUTSTANDING_REQUESTS_RANGE,
        )?;
        validate_usize("sql_bytes", self.sql_bytes, SQL_BYTES_RANGE)?;
        validate_usize("parameters", self.parameters, PARAMETERS_RANGE)?;
        validate_usize(
            "parameter_bytes",
            self.parameter_bytes,
            PARAMETER_BYTES_RANGE,
        )?;
        validate_usize("result_rows", self.result_rows, RESULT_ROWS_RANGE)?;
        validate_usize("columns", self.columns, COLUMNS_RANGE)?;
        validate_usize("cell_bytes", self.cell_bytes, CELL_BYTES_RANGE)?;
        validate_usize(
            "result_json_bytes",
            self.result_json_bytes,
            RESULT_JSON_BYTES_RANGE,
        )?;
        validate_u64(
            "request_timeout_seconds",
            self.request_timeout_seconds,
            TIMEOUT_SECONDS_RANGE,
        )?;
        validate_u64(
            "connection_timeout_seconds",
            self.connection_timeout_seconds,
            TIMEOUT_SECONDS_RANGE,
        )?;
        validate_u64(
            "cancel_drain_grace_seconds",
            self.cancel_drain_grace_seconds,
            TIMEOUT_SECONDS_RANGE,
        )?;
        validate_u64(
            "shutdown_total_seconds",
            self.shutdown_total_seconds,
            TIMEOUT_SECONDS_RANGE,
        )?;
        Ok(())
    }
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_seconds)
    }
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_secs(self.connection_timeout_seconds)
    }
    pub fn cancel_drain_grace(&self) -> Duration {
        Duration::from_secs(self.cancel_drain_grace_seconds)
    }
    pub fn shutdown_total(&self) -> Duration {
        Duration::from_secs(self.shutdown_total_seconds)
    }
}

impl ConfigFile {
    pub fn from_toml(source: &str) -> Result<Self, ConfigError> {
        let f: Self = toml::from_str(source)?;
        f.validate()?;
        Ok(f)
    }
    pub fn from_path(path: &Path) -> Result<Self, ConfigError> {
        Self::from_toml(&fs::read_to_string(path)?)
    }
    pub fn is_profile_document(source: &str) -> Result<bool, ConfigError> {
        let value: toml::Value = toml::from_str(source)?;
        let table = value.as_table().ok_or(ConfigError::DsnCommand)?;
        Ok(table.contains_key("profiles") || table.contains_key("defaults"))
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        let base = self.defaults.merge(&self.legacy).into_config();
        base.validate()?;
        let base = ConfigOverrides::from_config(&base);
        for p in self.profiles.values() {
            p.overrides.merge(&base).into_config().validate()?;
        }
        Ok(())
    }
    pub fn shutdown_total(&self) -> Duration {
        self.profiles
            .values()
            .filter_map(|profile| self.profile_config_for_profile(profile).ok())
            .map(|config| config.shutdown_total())
            .max()
            .unwrap_or_else(|| self.base_config().shutdown_total())
    }
    fn profile_config_for_profile(&self, profile: &Profile) -> Result<Config, ConfigError> {
        let config = self
            .defaults
            .merge(&self.legacy)
            .merge(&profile.overrides)
            .into_config();
        config.validate()?;
        Ok(config)
    }
    pub fn base_config(&self) -> Config {
        self.defaults.merge(&self.legacy).into_config()
    }
    pub fn profile_config(&self, profile: &str) -> Result<Config, ConfigError> {
        let p = self
            .profiles
            .get(profile)
            .ok_or(ConfigError::UnknownProfile)?;
        let base = self.defaults.merge(&self.legacy);
        let config = base.merge(&p.overrides).into_config();
        config.validate()?;
        Ok(config)
    }
    pub fn resolve(&self, profile: &str) -> Result<(Config, String), ConfigError> {
        self.resolve_with_context(
            profile,
            &CancellationToken::new(),
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(30),
        )
    }
    pub fn resolve_with_context(
        &self,
        profile: &str,
        cancellation: &CancellationToken,
        shutdown: &CancellationToken,
        deadline: Instant,
    ) -> Result<(Config, String), ConfigError> {
        let p = self
            .profiles
            .get(profile)
            .ok_or(ConfigError::UnknownProfile)?;
        let base = self.defaults.merge(&self.legacy);
        let config = base.merge(&p.overrides).into_config();
        config.validate()?;
        static NEXT_DSN_FILE: AtomicU64 = AtomicU64::new(0);
        let output_path = std::env::temp_dir().join(format!(
            "postgres-mcp-dsn-{}-{}",
            std::process::id(),
            NEXT_DSN_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        struct TempOutput(std::path::PathBuf);
        impl Drop for TempOutput {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&output_path)
            .map_err(|_| ConfigError::DsnCommand)?;
        let output_guard = TempOutput(output_path.clone());
        let mut command = Command::new("bash");
        // SAFETY: setsid is called in the child immediately before exec, and
        // does not access Rust-managed state. This creates a private process
        // group without depending on an external setsid utility.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        command
            .args(["-c", &p.dsn])
            .stdout(output)
            .stderr(Stdio::null());
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(_) => return Err(ConfigError::DsnCommand),
        };
        let result = loop {
            if cancellation.is_cancelled() || shutdown.is_cancelled() || Instant::now() >= deadline
            {
                terminate_process_group(child.id());
                let _ = child.wait();
                break Err(ConfigError::DsnTimeout);
            }
            match child.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        break Err(ConfigError::DsnCommand);
                    }
                    let mut file =
                        std::fs::File::open(&output_path).map_err(|_| ConfigError::DsnCommand)?;
                    let mut bytes = Vec::new();
                    file.by_ref()
                        .take(65_537)
                        .read_to_end(&mut bytes)
                        .map_err(|_| ConfigError::DsnCommand)?;
                    if bytes.len() > 65_536 {
                        break Err(ConfigError::DsnCommand);
                    }
                    let dsn = String::from_utf8(bytes)
                        .map_err(|_| ConfigError::InvalidDsnOutput)?
                        .trim()
                        .to_owned();
                    if dsn.is_empty() {
                        break Err(ConfigError::EmptyDsn);
                    }
                    break Ok((config, dsn));
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(5)),
                Err(_) => {
                    terminate_process_group(child.id());
                    let _ = child.wait();
                    break Err(ConfigError::DsnCommand);
                }
            }
        };
        drop(output_guard);
        result
    }
}

fn terminate_process_group(pid: u32) {
    let pid = pid.to_string();
    let _ = Command::new("/bin/kill")
        .args(["-TERM", &format!("-{pid}")])
        .status();
    std::thread::sleep(Duration::from_millis(50));
    let _ = Command::new("/bin/kill")
        .args(["-KILL", &format!("-{pid}")])
        .status();
}

fn validate_usize(
    field: &'static str,
    value: usize,
    (min, max): (usize, usize),
) -> Result<(), ConfigError> {
    if (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(ConfigError::Invalid {
            field,
            minimum: min as u64,
            maximum: max as u64,
            value: value as u64,
        })
    }
}
fn validate_u64(
    field: &'static str,
    value: u64,
    (min, max): (u64, u64),
) -> Result<(), ConfigError> {
    if (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(ConfigError::Invalid {
            field,
            minimum: min,
            maximum: max,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn profile_overrides_and_command_parse() {
        let f = ConfigFile::from_toml("[defaults]\nmax_handles=8\n[profiles.local]\ndsn=\"printf dbname=actual_db\"\nresult_rows=42\n").unwrap();
        let (c, d) = f.resolve("local").unwrap();
        assert_eq!(c.max_handles, 8);
        assert_eq!(c.result_rows, 42);
        assert_eq!(d, "dbname=actual_db");
    }
    #[test]
    fn defaults_validate() {
        assert!(Config::default().validate().is_ok());
    }
}
