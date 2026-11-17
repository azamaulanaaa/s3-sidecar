//! Load configuration from defaults, file, environment, and flags.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value, json};

use super::secret::SecretString;
use super::{Config, ConfigError, validate};

/// Where a configuration value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Source {
    /// Built-in default.
    #[default]
    Default,
    /// TOML configuration file.
    File,
    /// Environment variable.
    Env,
    /// Command-line flag.
    Flag,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("default"),
            Self::File => f.write_str("file"),
            Self::Env => f.write_str("env"),
            Self::Flag => f.write_str("flag"),
        }
    }
}

/// Winning source for each configuration path.
pub type SourceMap = BTreeMap<String, Source>;

/// Inputs for one configuration load.
#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// Value of the `--config` flag.
    pub config_flag: Option<PathBuf>,
    /// Environment key-value pairs.
    pub env: Vec<(String, String)>,
    /// Flag values as a JSON object.
    pub flags: Value,
    /// Working directory for default file discovery.
    pub cwd: PathBuf,
    /// Default file paths to try, in order.
    pub default_paths: Vec<PathBuf>,
}

/// A merged configuration and its metadata.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    /// Merged and validated configuration.
    pub config: Config,
    /// Winning source for each path.
    pub sources: SourceMap,
    /// File that was read, when a file was found.
    pub config_path: Option<PathBuf>,
    /// Accepted settings that have no effect.
    pub warnings: Vec<String>,
}

const ENV_PREFIX: &str = "S3_SIDECAR_";

/// Flags-layer key for bucket names from `--bucket`.
pub(crate) const BUCKETS_ADD_KEY: &str = "__buckets_add";

/// Load and validate the configuration.
///
/// # Errors
///
/// Returns an error when a source cannot be read, a key is unknown, or a
/// value violates a validation rule.
pub fn load(options: &LoadOptions) -> Result<LoadedConfig, ConfigError> {
    let env = index_env(&options.env);
    let config_path = discover_file(options, &env)?;
    let mut merged = Value::Object(Map::new());
    let mut sources = SourceMap::new();

    if let Some(path) = &config_path {
        let text = fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
            path: path.clone(),
            source,
        })?;
        let toml_value: toml::Value =
            toml::from_str(&text).map_err(|source| ConfigError::ParseFile {
                path: path.clone(),
                source,
            })?;
        let json_value = serde_json::to_value(toml_value)
            .map_err(|error| ConfigError::Deserialize(error.to_string()))?;
        merge(&mut merged, json_value, Source::File, "", &mut sources);
    }

    let env_layer = build_env_layer(&options.env)?;
    merge(&mut merged, env_layer, Source::Env, "", &mut sources);

    let mut flags = options.flags.clone();
    let bucket_names = take_bucket_names(&mut flags);
    merge(&mut merged, flags, Source::Flag, "", &mut sources);
    append_bucket_names(&mut merged, &bucket_names, &mut sources);

    let config: Config = serde_json::from_value(merged)
        .map_err(|error| ConfigError::Deserialize(error.to_string()))?;
    let mut config = config;
    load_otel_headers(&mut config, &sources)?;
    let warnings = validate(&config)?;

    Ok(LoadedConfig {
        config,
        sources,
        config_path,
        warnings,
    })
}

/// Merge headers from the headers file into the configuration.
///
/// A file header replaces an inline header only when the file has higher
/// precedence. Inline headers at the same or higher precedence stay unchanged.
fn load_otel_headers(config: &mut Config, sources: &SourceMap) -> Result<(), ConfigError> {
    let Some(path) = config.otel.headers_file.clone() else {
        return Ok(());
    };
    let file_source = sources
        .get("otel.headers_file")
        .copied()
        .unwrap_or_default();
    let text = fs::read_to_string(&path).map_err(|source| ConfigError::ReadHeaders {
        path: path.clone(),
        source,
    })?;
    let value: Value = serde_json::from_str(&text).map_err(|error| {
        ConfigError::invalid(format!(
            "OTLP headers file {} is not valid JSON: {error}",
            path.display()
        ))
    })?;
    let Value::Object(headers) = value else {
        return Err(ConfigError::invalid(format!(
            "OTLP headers file {} must contain a JSON object",
            path.display()
        )));
    };
    for (name, value) in headers {
        let Some(value) = value.as_str() else {
            return Err(ConfigError::invalid(format!(
                "OTLP header \"{name}\" in {} must be a string",
                path.display()
            )));
        };
        let inline_source = sources.get(&format!("otel.headers.{name}")).copied();
        let file_wins = match inline_source {
            Some(inline_source) => file_source > inline_source,
            None => true,
        };
        if file_wins {
            config.otel.headers.insert(name, SecretString::new(value));
        }
    }
    Ok(())
}

fn index_env(env: &[(String, String)]) -> HashMap<String, String> {
    let mut indexed = HashMap::with_capacity(env.len());
    for (key, value) in env {
        indexed.insert(key.to_ascii_uppercase(), value.clone());
    }
    indexed
}

fn discover_file(
    options: &LoadOptions,
    env: &HashMap<String, String>,
) -> Result<Option<PathBuf>, ConfigError> {
    if let Some(path) = &options.config_flag {
        return require_file(path, "config file");
    }
    if let Some(path) = env.get("S3_SIDECAR_CONFIG") {
        return require_file(Path::new(path), "config file");
    }
    for path in &options.default_paths {
        let candidate = if path.is_absolute() {
            path.clone()
        } else {
            options.cwd.join(path)
        };
        if candidate.is_file() {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn require_file(path: &Path, label: &str) -> Result<Option<PathBuf>, ConfigError> {
    if path.is_file() {
        Ok(Some(path.to_path_buf()))
    } else {
        Err(ConfigError::invalid(format!(
            "{label} does not exist: {}",
            path.display()
        )))
    }
}

#[derive(Debug, Clone, Copy)]
enum EnvKind {
    Text,
    Bool,
    Unsigned,
    Float,
    Json,
}

fn build_env_layer(env: &[(String, String)]) -> Result<Value, ConfigError> {
    let mut layer = Value::Object(Map::new());
    let mut seen = std::collections::HashSet::new();
    for (key, value) in env {
        let upper = key.to_ascii_uppercase();
        let Some(rest) = upper.strip_prefix(ENV_PREFIX) else {
            continue;
        };
        if !seen.insert(upper.clone()) {
            return Err(ConfigError::invalid(format!(
                "environment variable \"{key}\" is set more than one time with different case"
            )));
        }
        if matches!(rest, "CONFIG" | "LOG" | "LOG_FORMAT") {
            continue;
        }
        if let Some(secret) = secret_env_key(rest) {
            let secret_value = read_secret_file(Path::new(value))?;
            insert_path(
                &mut layer,
                &secret.path,
                Value::String(secret_value.expose_secret().to_owned()),
            );
            continue;
        }
        let (kind, path) = env_key(rest).ok_or_else(|| ConfigError::UnknownEnv(key.clone()))?;
        let parsed = match kind {
            EnvKind::Text => Value::String(value.clone()),
            EnvKind::Bool => {
                let parsed = if value.eq_ignore_ascii_case("true") {
                    true
                } else if value.eq_ignore_ascii_case("false") {
                    false
                } else {
                    return Err(ConfigError::InvalidValue {
                        name: key.clone(),
                        value: value.clone(),
                    });
                };
                Value::Bool(parsed)
            }
            EnvKind::Unsigned => {
                let parsed = value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::InvalidValue {
                        name: key.clone(),
                        value: value.clone(),
                    })?;
                json!(parsed)
            }
            EnvKind::Float => {
                let parsed = value
                    .parse::<f64>()
                    .map_err(|_| ConfigError::InvalidValue {
                        name: key.clone(),
                        value: value.clone(),
                    })?;
                json!(parsed)
            }
            EnvKind::Json => {
                let parsed: Value =
                    serde_json::from_str(value).map_err(|_| ConfigError::InvalidValue {
                        name: key.clone(),
                        value: value.clone(),
                    })?;
                parsed
            }
        };
        insert_path(&mut layer, &path, parsed);
    }
    Ok(layer)
}

struct SecretEnv {
    path: Vec<&'static str>,
}

#[allow(clippy::too_many_lines)]
fn env_key(key: &str) -> Option<(EnvKind, Vec<&'static str>)> {
    let entry = match key {
        "MODE" => (EnvKind::Text, vec!["mode"]),
        "SERVER__BIND" => (EnvKind::Text, vec!["server", "bind"]),
        "SERVER__TLS__CERT_FILE" => (EnvKind::Text, vec!["server", "tls", "cert_file"]),
        "SERVER__TLS__KEY_FILE" => (EnvKind::Text, vec!["server", "tls", "key_file"]),
        "SERVER__MAX_BODY_BYTES" => (EnvKind::Unsigned, vec!["server", "max_body_bytes"]),
        "SERVER__REQUEST_TIMEOUT" => (EnvKind::Text, vec!["server", "request_timeout"]),
        "SERVER__IDLE_TIMEOUT" => (EnvKind::Text, vec!["server", "idle_timeout"]),
        "SERVER__MAX_CONNECTIONS" => (EnvKind::Unsigned, vec!["server", "max_connections"]),
        "SERVER__SHUTDOWN_TIMEOUT" => (EnvKind::Text, vec!["server", "shutdown_timeout"]),
        "UPSTREAM__CONNECT_TIMEOUT" => (EnvKind::Text, vec!["upstream", "connect_timeout"]),
        "UPSTREAM__REQUEST_TIMEOUT" => (EnvKind::Text, vec!["upstream", "request_timeout"]),
        "UPSTREAM__RETRY_ATTEMPTS" => (EnvKind::Unsigned, vec!["upstream", "retry_attempts"]),
        "MULTIPART__ENABLED" => (EnvKind::Bool, vec!["multipart", "enabled"]),
        "MULTIPART__THRESHOLD" => (EnvKind::Text, vec!["multipart", "threshold"]),
        "MULTIPART__CHUNK_SIZE" => (EnvKind::Text, vec!["multipart", "chunk_size"]),
        "MULTIPART__MIN_CHUNK_SIZE" => (EnvKind::Text, vec!["multipart", "min_chunk_size"]),
        "MULTIPART__MAX_PARTS" => (EnvKind::Unsigned, vec!["multipart", "max_parts"]),
        "MULTIPART__CONCURRENCY" => (EnvKind::Unsigned, vec!["multipart", "concurrency"]),
        "ORIGIN__ENDPOINT" => (EnvKind::Text, vec!["origin", "endpoint"]),
        "ORIGIN__REGION" => (EnvKind::Text, vec!["origin", "region"]),
        "ORIGIN__ACCESS_KEY_ID" => (EnvKind::Text, vec!["origin", "access_key_id"]),
        "ORIGIN__SECRET_ACCESS_KEY" => (EnvKind::Text, vec!["origin", "secret_access_key"]),
        "ORIGIN__SESSION_TOKEN" => (EnvKind::Text, vec!["origin", "session_token"]),
        "ORIGIN__FORCE_PATH_STYLE" => (EnvKind::Bool, vec!["origin", "force_path_style"]),
        "CACHE__ENDPOINT" => (EnvKind::Text, vec!["cache", "endpoint"]),
        "CACHE__REGION" => (EnvKind::Text, vec!["cache", "region"]),
        "CACHE__ACCESS_KEY_ID" => (EnvKind::Text, vec!["cache", "access_key_id"]),
        "CACHE__SECRET_ACCESS_KEY" => (EnvKind::Text, vec!["cache", "secret_access_key"]),
        "CACHE__SESSION_TOKEN" => (EnvKind::Text, vec!["cache", "session_token"]),
        "CACHE__FORCE_PATH_STYLE" => (EnvKind::Bool, vec!["cache", "force_path_style"]),
        "CACHE__PREFIX" => (EnvKind::Text, vec!["cache", "prefix"]),
        "CACHE__BUCKET" => (EnvKind::Text, vec!["cache", "bucket"]),
        "CACHE__MODE" => (EnvKind::Text, vec!["cache", "mode"]),
        "CACHE__MAX_BYTES" => (EnvKind::Unsigned, vec!["cache", "max_bytes"]),
        "CACHE__MAX_OBJECT_BYTES" => (EnvKind::Unsigned, vec!["cache", "max_object_bytes"]),
        "CACHE__WRITE_POLICY" => (EnvKind::Text, vec!["cache", "write_policy"]),
        "CACHE__ON_CACHE_ERROR" => (EnvKind::Text, vec!["cache", "on_cache_error"]),
        "CACHE__HIGH_WATERMARK" => (EnvKind::Float, vec!["cache", "high_watermark"]),
        "CACHE__LOW_WATERMARK" => (EnvKind::Float, vec!["cache", "low_watermark"]),
        "CACHE__STAGING_TTL" => (EnvKind::Text, vec!["cache", "staging_ttl"]),
        "CACHE__ORPHAN_TTL" => (EnvKind::Text, vec!["cache", "orphan_ttl"]),
        "CACHE__SYNC_WORKERS" => (EnvKind::Unsigned, vec!["cache", "sync_workers"]),
        "CACHE__SYNC_MAX_ATTEMPTS" => (EnvKind::Unsigned, vec!["cache", "sync_max_attempts"]),
        "CACHE__SYNC_RETRY_BASE_MS" => (EnvKind::Unsigned, vec!["cache", "sync_retry_base_ms"]),
        "CACHE__SYNC_DEBOUNCE_MS" => (EnvKind::Unsigned, vec!["cache", "sync_debounce_ms"]),
        "CACHE__GC_INTERVAL" => (EnvKind::Text, vec!["cache", "gc_interval"]),
        "CACHE__INDEX_PATH" => (EnvKind::Text, vec!["cache", "index_path"]),
        "CACHE__SERVE_QUARANTINED" => (EnvKind::Bool, vec!["cache", "serve_quarantined"]),
        "BACKING__ENDPOINT" => (EnvKind::Text, vec!["backing", "endpoint"]),
        "BACKING__BUCKET" => (EnvKind::Text, vec!["backing", "bucket"]),
        "BACKING__REGION" => (EnvKind::Text, vec!["backing", "region"]),
        "BACKING__ACCESS_KEY_ID" => (EnvKind::Text, vec!["backing", "access_key_id"]),
        "BACKING__SECRET_ACCESS_KEY" => (EnvKind::Text, vec!["backing", "secret_access_key"]),
        "BACKING__SESSION_TOKEN" => (EnvKind::Text, vec!["backing", "session_token"]),
        "BACKING__FORCE_PATH_STYLE" => (EnvKind::Bool, vec!["backing", "force_path_style"]),
        "CONSENSUS__BACKEND" => (EnvKind::Text, vec!["consensus", "backend"]),
        "CONSENSUS__RAFT__NODE_ID" => (EnvKind::Text, vec!["consensus", "raft", "node_id"]),
        "CONSENSUS__RAFT__PEERS" => (EnvKind::Json, vec!["consensus", "raft", "peers"]),
        "CONSENSUS__RAFT__BOOTSTRAP" => (EnvKind::Bool, vec!["consensus", "raft", "bootstrap"]),
        "CONSENSUS__RAFT__SECRET" => (EnvKind::Text, vec!["consensus", "raft", "secret"]),
        "CONSENSUS__RAFT__RPC_TIMEOUT" => (EnvKind::Text, vec!["consensus", "raft", "rpc_timeout"]),
        "CONSENSUS__RAFT__NODES" => (EnvKind::Json, vec!["consensus", "raft", "nodes"]),
        "CONSENSUS__CONSUL__NODE_ID" => (EnvKind::Text, vec!["consensus", "consul", "node_id"]),
        "CONSENSUS__CONSUL__ADDRESS" => (EnvKind::Text, vec!["consensus", "consul", "address"]),
        "CONSENSUS__CONSUL__ADVERTISE_ADDR" => {
            (EnvKind::Text, vec!["consensus", "consul", "advertise_addr"])
        }
        "CONSENSUS__CONSUL__TOKEN" => (EnvKind::Text, vec!["consensus", "consul", "token"]),
        "CONSENSUS__CONSUL__SESSION_TTL" => {
            (EnvKind::Text, vec!["consensus", "consul", "session_ttl"])
        }
        "CONSENSUS__CONSUL__SECRET" => (EnvKind::Text, vec!["consensus", "consul", "secret"]),
        "CLUSTER__STAGING_TTL" => (EnvKind::Text, vec!["cluster", "staging_ttl"]),
        "CLUSTER__ORPHAN_TTL" => (EnvKind::Text, vec!["cluster", "orphan_ttl"]),
        "CLUSTER__GC_INTERVAL" => (EnvKind::Text, vec!["cluster", "gc_interval"]),
        "CLUSTER__METADATA_PATH" => (EnvKind::Text, vec!["cluster", "metadata_path"]),
        "CLUSTER__SECRET" => (EnvKind::Text, vec!["cluster", "secret"]),
        "CLUSTER__READ_REPAIR" => (EnvKind::Bool, vec!["cluster", "read_repair"]),
        "CLUSTER__REPLICATE_WORKERS" => (EnvKind::Unsigned, vec!["cluster", "replicate_workers"]),
        "CLUSTER__REPLICATE_TIMEOUT" => (EnvKind::Text, vec!["cluster", "replicate_timeout"]),
        "CLUSTER__RECONCILE_INTERVAL" => (EnvKind::Text, vec!["cluster", "reconcile_interval"]),
        "CLUSTER__NODES" => (EnvKind::Json, vec!["cluster", "nodes"]),
        "OTEL__ENABLED" => (EnvKind::Bool, vec!["otel", "enabled"]),
        "OTEL__METRICS" => (EnvKind::Bool, vec!["otel", "metrics"]),
        "OTEL__TRACES" => (EnvKind::Bool, vec!["otel", "traces"]),
        "OTEL__LOGS" => (EnvKind::Bool, vec!["otel", "logs"]),
        "OTEL__SERVICE_NAME" => (EnvKind::Text, vec!["otel", "service_name"]),
        "OTEL__SERVICE_VERSION" => (EnvKind::Text, vec!["otel", "service_version"]),
        "OTEL__ENDPOINT" => (EnvKind::Text, vec!["otel", "endpoint"]),
        "OTEL__TRACE_ENDPOINT" => (EnvKind::Text, vec!["otel", "trace_endpoint"]),
        "OTEL__LOGS_ENDPOINT" => (EnvKind::Text, vec!["otel", "logs_endpoint"]),
        "OTEL__TRACE_SAMPLE_RATIO" => (EnvKind::Float, vec!["otel", "trace_sample_ratio"]),
        "OTEL__PROTOCOL" => (EnvKind::Text, vec!["otel", "protocol"]),
        "OTEL__HEADERS" => (EnvKind::Json, vec!["otel", "headers"]),
        "OTEL__HEADERS_FILE" => (EnvKind::Text, vec!["otel", "headers_file"]),
        "OTEL__TIMEOUT" => (EnvKind::Text, vec!["otel", "timeout"]),
        "OTEL__EXPORT_INTERVAL" => (EnvKind::Text, vec!["otel", "export_interval"]),
        "OTEL__TRACE_SCHEDULE_DELAY" => (EnvKind::Text, vec!["otel", "trace_schedule_delay"]),
        "OTEL__LOG_SCHEDULE_DELAY" => (EnvKind::Text, vec!["otel", "log_schedule_delay"]),
        "OTEL__EXPORT_TIMEOUT" => (EnvKind::Text, vec!["otel", "export_timeout"]),
        "OTEL__RESOURCE_ATTRIBUTES" => (EnvKind::Json, vec!["otel", "resource_attributes"]),
        "OTEL__RECORD_OBJECT_KEYS" => (EnvKind::Bool, vec!["otel", "record_object_keys"]),
        "CLIENTS" => (EnvKind::Json, vec!["clients"]),
        "BUCKETS" => (EnvKind::Json, vec!["buckets"]),
        _ => return None,
    };
    Some(entry)
}

fn secret_env_key(key: &str) -> Option<SecretEnv> {
    let path = match key {
        "ORIGIN__SECRET_ACCESS_KEY_FILE" => vec!["origin", "secret_access_key"],
        "CACHE__SECRET_ACCESS_KEY_FILE" => vec!["cache", "secret_access_key"],
        "BACKING__SECRET_ACCESS_KEY_FILE" => vec!["backing", "secret_access_key"],
        "CONSENSUS__RAFT__SECRET_FILE" => vec!["consensus", "raft", "secret"],
        "CONSENSUS__CONSUL__TOKEN_FILE" => vec!["consensus", "consul", "token"],
        "CONSENSUS__CONSUL__SECRET_FILE" => vec!["consensus", "consul", "secret"],
        "CLUSTER__SECRET_FILE" => vec!["cluster", "secret"],
        _ => return None,
    };
    Some(SecretEnv { path })
}

fn insert_path(target: &mut Value, path: &[&str], value: Value) {
    let mut current = target;
    for part in path {
        let Some(object) = current.as_object_mut() else {
            return;
        };
        current = object
            .entry((*part).to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    *current = value;
}

fn merge(base: &mut Value, overlay: Value, source: Source, prefix: &str, sources: &mut SourceMap) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (key, value) in overlay_map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                let nested =
                    value.is_object() && matches!(base_map.get(&key), Some(Value::Object(_)));
                if nested {
                    if let Some(existing) = base_map.get_mut(&key) {
                        merge(existing, value, source, &path, sources);
                    }
                } else {
                    remove_descendants(sources, &path);
                    record_sources(&value, source, &path, sources);
                    base_map.insert(key, value);
                }
            }
        }
        (base_value, overlay_value) => {
            remove_descendants(sources, prefix);
            record_sources(&overlay_value, source, prefix, sources);
            *base_value = overlay_value;
        }
    }
}

/// Remove bucket names added by `--bucket` from the flags layer.
fn take_bucket_names(flags: &mut Value) -> Vec<String> {
    let Some(object) = flags.as_object_mut() else {
        return Vec::new();
    };
    let Some(value) = object.remove(BUCKETS_ADD_KEY) else {
        return Vec::new();
    };
    let Value::Array(items) = value else {
        return Vec::new();
    };
    items
        .into_iter()
        .filter_map(|item| item.as_str().map(str::to_owned))
        .collect()
}

/// Add bucket names to the merged configuration without replacing other buckets.
fn append_bucket_names(merged: &mut Value, names: &[String], sources: &mut SourceMap) {
    if names.is_empty() {
        return;
    }
    let Some(object) = merged.as_object_mut() else {
        return;
    };
    let buckets = object
        .entry("buckets")
        .or_insert_with(|| Value::Array(Vec::new()));
    let Value::Array(items) = buckets else {
        return;
    };
    for name in names {
        let exists = items
            .iter()
            .any(|item| item.get("name").and_then(Value::as_str) == Some(name.as_str()));
        if exists {
            continue;
        }
        let index = items.len();
        items.push(json!({ "name": name }));
        record_sources(
            &items[index],
            Source::Flag,
            &format!("buckets[{index}]"),
            sources,
        );
    }
}

fn record_sources(value: &Value, source: Source, path: &str, sources: &mut SourceMap) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                record_sources(child, source, &child_path, sources);
            }
        }
        Value::Array(items) => {
            sources.insert(path.to_owned(), source);
            for (index, child) in items.iter().enumerate() {
                let child_path = format!("{path}[{index}]");
                record_sources(child, source, &child_path, sources);
            }
        }
        _ => {
            sources.insert(path.to_owned(), source);
        }
    }
}

fn remove_descendants(sources: &mut SourceMap, path: &str) {
    let dot_prefix = format!("{path}.");
    let index_prefix = format!("{path}[");
    sources.retain(|key, _| {
        key != path && !key.starts_with(&dot_prefix) && !key.starts_with(&index_prefix)
    });
}

/// Read a secret from a file. A final line break is removed.
///
/// # Errors
///
/// Returns an error when the file cannot be read.
pub fn read_secret_file(path: &Path) -> Result<SecretString, ConfigError> {
    let value = fs::read_to_string(path).map_err(|source| ConfigError::ReadSecret {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(SecretString::new(trim_line_end(&value)))
}

/// Remove one final line break from a secret value.
#[must_use]
pub fn trim_line_end(value: &str) -> &str {
    value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(value)
}

#[cfg(test)]
fn test_cache_flags(mut flags: Value) -> Value {
    let Value::Object(map) = &mut flags else {
        return flags;
    };
    let cache = map
        .entry("cache".to_owned())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Value::Object(cache) = cache {
        cache
            .entry("endpoint".to_owned())
            .or_insert_with(|| Value::String("http://127.0.0.1:9002".to_owned()));
        cache
            .entry("bucket".to_owned())
            .or_insert_with(|| Value::String("cache".to_owned()));
        cache
            .entry("max_bytes".to_owned())
            .or_insert_with(|| json!(0));
    }
    flags
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::path::PathBuf;

    use serde_json::json;
    use tempfile::tempdir;

    use super::{LoadOptions, Source, load, test_cache_flags};
    use crate::cli::ConfigFlags;
    use crate::config::secret::SecretString;

    fn load_with(
        dir: &std::path::Path,
        file: Option<&str>,
        env: Vec<(String, String)>,
        flags: serde_json::Value,
    ) -> Result<super::LoadedConfig, super::ConfigError> {
        let config_flag = file.map(|contents| {
            let path = dir.join("sidecar.toml");
            std::fs::write(&path, contents).unwrap();
            path
        });
        load(&LoadOptions {
            config_flag,
            env,
            flags: test_cache_flags(flags),
            cwd: dir.to_path_buf(),
            default_paths: Vec::new(),
        })
    }

    #[test]
    fn defaults_apply_when_no_source_is_set() {
        let dir = tempdir().unwrap();
        let loaded = load_with(dir.path(), None, Vec::new(), json!({})).unwrap();
        assert_eq!(loaded.config.server.bind, "127.0.0.1:9000");
        assert_eq!(loaded.config.cache.mode, super::super::CacheMode::ReadWrite);
        assert_eq!(loaded.sources.get("server.bind"), None);
    }

    #[test]
    fn precedence_is_file_then_env_then_flag() {
        let dir = tempdir().unwrap();
        let file = r#"
[server]
bind = "127.0.0.1:1111"
[origin]
endpoint = "http://file:9000"
[[buckets]]
name = "filebucket"
"#;
        let env = vec![
            (
                "S3_SIDECAR_SERVER__BIND".to_owned(),
                "127.0.0.1:2222".to_owned(),
            ),
            (
                "S3_SIDECAR_ORIGIN__ENDPOINT".to_owned(),
                "http://env:9000".to_owned(),
            ),
            (
                "S3_SIDECAR_BUCKETS".to_owned(),
                r#"[{"name":"envbucket"}]"#.to_owned(),
            ),
        ];
        let flags = json!({"server": {"bind": "127.0.0.1:3333"}});
        let loaded = load_with(dir.path(), Some(file), env, flags).unwrap();
        assert_eq!(loaded.config.server.bind, "127.0.0.1:3333");
        assert_eq!(loaded.config.origin.endpoint, "http://env:9000");
        assert_eq!(loaded.config.buckets[0].name, "envbucket");
        assert_eq!(loaded.sources.get("server.bind"), Some(&Source::Flag));
        assert_eq!(loaded.sources.get("origin.endpoint"), Some(&Source::Env));
        assert_eq!(loaded.sources.get("buckets"), Some(&Source::Env));
    }

    #[test]
    fn bucket_flag_adds_name_to_configured_buckets() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sidecar.toml");
        let file = r#"
[cache]
endpoint = "http://127.0.0.1:9002"
bucket = "cache"
max_bytes = 0
[[buckets]]
name = "media"
ttl = "1h"
cache_mode = "read"
write_ack = "all"
replicas = 3
min_replicas = 2
"#;
        std::fs::write(&path, file).unwrap();
        let flags = ConfigFlags {
            bucket: vec!["logs".to_owned()],
            ..ConfigFlags::default()
        };
        let layer = test_cache_flags(flags.to_layer(None).unwrap());
        let loaded = load(&LoadOptions {
            config_flag: Some(path),
            env: Vec::new(),
            flags: layer,
            cwd: dir.path().to_path_buf(),
            default_paths: Vec::new(),
        })
        .unwrap();

        assert_eq!(loaded.config.buckets.len(), 2);
        assert_eq!(loaded.config.buckets[0].name, "media");
        assert_eq!(
            loaded.config.buckets[0].ttl.unwrap().as_duration(),
            std::time::Duration::from_secs(3600)
        );
        assert_eq!(
            loaded.config.buckets[0].cache_mode,
            Some(super::super::CacheMode::Read)
        );
        assert_eq!(loaded.config.buckets[0].replicas, Some(3));
        assert_eq!(loaded.config.buckets[0].min_replicas, Some(2));
        assert_eq!(loaded.config.buckets[1].name, "logs");
    }

    #[test]
    fn environment_names_are_case_insensitive() {
        let dir = tempdir().unwrap();
        let env = vec![(
            "s3_sidecar_server__bind".to_owned(),
            "127.0.0.1:4444".to_owned(),
        )];
        let loaded = load_with(dir.path(), None, env, json!({})).unwrap();
        assert_eq!(loaded.config.server.bind, "127.0.0.1:4444");
    }

    #[test]
    fn unknown_environment_variable_is_rejected() {
        let dir = tempdir().unwrap();
        let env = vec![("S3_SIDECAR_TYPO".to_owned(), "1".to_owned())];
        let error = load_with(dir.path(), None, env, json!({})).unwrap_err();
        assert!(error.to_string().contains("S3_SIDECAR_TYPO"));
    }

    #[test]
    fn secret_file_environment_variable_is_read() {
        let dir = tempdir().unwrap();
        let secret_path = dir.path().join("secret");
        std::fs::write(&secret_path, "file-secret\n").unwrap();
        let env = vec![(
            "S3_SIDECAR_ORIGIN__SECRET_ACCESS_KEY_FILE".to_owned(),
            secret_path.display().to_string(),
        )];
        let loaded = load_with(dir.path(), None, env, json!({})).unwrap();
        assert_eq!(
            loaded.config.origin.secret_access_key.expose_secret(),
            "file-secret"
        );
    }

    #[test]
    fn raft_secret_file_environment_variable_is_read() {
        let dir = tempdir().unwrap();
        let secret_path = dir.path().join("raft-secret");
        std::fs::write(&secret_path, "raft-file-secret\n").unwrap();
        let file = r#"
mode = "cluster"
[backing]
endpoint = "http://127.0.0.1:9002"
bucket = "data"
[consensus]
backend = "raft"
[consensus.raft]
node_id = "a"
[[consensus.raft.nodes]]
id = "a"
addr = "http://127.0.0.1:9000"
"#;
        let env = vec![(
            "S3_SIDECAR_CONSENSUS__RAFT__SECRET_FILE".to_owned(),
            secret_path.display().to_string(),
        )];
        let loaded = load_with(dir.path(), Some(file), env, json!({})).unwrap();
        assert_eq!(
            loaded.config.consensus.raft.secret.expose_secret(),
            "raft-file-secret"
        );
    }

    #[test]
    fn cluster_secret_file_environment_variable_is_read() {
        let dir = tempdir().unwrap();
        let secret_path = dir.path().join("cluster-secret");
        std::fs::write(&secret_path, "cluster-file-secret\n").unwrap();
        let file = r#"
mode = "cluster"
[backing]
endpoint = "http://127.0.0.1:9002"
bucket = "data"
[consensus]
backend = "single"
[consensus.raft]
node_id = "a"
peers = ["a"]
"#;
        let env = vec![(
            "S3_SIDECAR_CLUSTER__SECRET_FILE".to_owned(),
            secret_path.display().to_string(),
        )];
        let loaded = load_with(dir.path(), Some(file), env, json!({})).unwrap();
        assert_eq!(
            loaded.config.cluster.secret.expose_secret(),
            "cluster-file-secret"
        );
    }

    #[test]
    fn consul_secret_files_are_read() {
        let dir = tempdir().unwrap();
        let token_path = dir.path().join("consul-token");
        let secret_path = dir.path().join("consul-secret");
        std::fs::write(&token_path, "acl-token\n").unwrap();
        std::fs::write(&secret_path, "proposal-token\n").unwrap();
        let file = r#"
mode = "cluster"
[backing]
endpoint = "http://127.0.0.1:9002"
bucket = "data"
[consensus]
backend = "consul"
[consensus.consul]
node_id = "a"
address = "http://127.0.0.1:8500"
advertise_addr = "http://127.0.0.1:9000"
session_ttl = "15s"
"#;
        let env = vec![
            (
                "S3_SIDECAR_CONSENSUS__CONSUL__TOKEN_FILE".to_owned(),
                token_path.display().to_string(),
            ),
            (
                "S3_SIDECAR_CONSENSUS__CONSUL__SECRET_FILE".to_owned(),
                secret_path.display().to_string(),
            ),
        ];
        let loaded = load_with(dir.path(), Some(file), env, json!({})).unwrap();
        assert_eq!(
            loaded.config.consensus.consul.token.expose_secret(),
            "acl-token"
        );
        assert_eq!(
            loaded.config.consensus.consul.secret.expose_secret(),
            "proposal-token"
        );
    }

    #[test]
    fn explicit_missing_config_file_is_an_error() {
        let dir = tempdir().unwrap();
        let error = load(&LoadOptions {
            config_flag: Some(PathBuf::from("/nonexistent/sidecar.toml")),
            env: Vec::new(),
            flags: json!({}),
            cwd: dir.path().to_path_buf(),
            default_paths: Vec::new(),
        })
        .unwrap_err();
        assert!(error.to_string().contains("does not exist"));
    }

    #[test]
    fn default_config_file_is_optional() {
        let dir = tempdir().unwrap();
        let loaded = load(&LoadOptions {
            config_flag: None,
            env: Vec::new(),
            flags: test_cache_flags(json!({})),
            cwd: dir.path().to_path_buf(),
            default_paths: vec![PathBuf::from("sidecar.toml")],
        })
        .unwrap();
        assert_eq!(loaded.config.server.bind, "127.0.0.1:9000");
    }

    #[test]
    fn secret_is_redacted_in_the_table() {
        let dir = tempdir().unwrap();
        let file = r#"
[origin]
endpoint = "http://127.0.0.1:9001"
secret_access_key = "do-not-show-this"
[[clients]]
access_key_id = "AKIA"
secret_access_key = "also-do-not-show"
buckets = ["media"]
"#;
        let loaded = load_with(dir.path(), Some(file), Vec::new(), json!({})).unwrap();
        let table = super::super::show_table(&loaded.config, &loaded.sources);
        assert!(!table.contains("do-not-show-this"));
        assert!(!table.contains("also-do-not-show"));
        assert!(table.contains("****"));
    }

    #[test]
    fn invalid_json_in_environment_is_rejected() {
        let dir = tempdir().unwrap();
        let env = vec![("S3_SIDECAR_BUCKETS".to_owned(), "not-json".to_owned())];
        let error = load_with(dir.path(), None, env, json!({})).unwrap_err();
        assert!(error.to_string().contains("S3_SIDECAR_BUCKETS"));
    }

    #[test]
    fn otel_headers_file_environment_variable_is_read() {
        let dir = tempdir().unwrap();
        let headers_path = dir.path().join("otel-headers.json");
        std::fs::write(&headers_path, r#"{"x-api-key":"header-secret"}"#).unwrap();
        let env = vec![(
            "S3_SIDECAR_OTEL__HEADERS_FILE".to_owned(),
            headers_path.display().to_string(),
        )];
        let loaded = load_with(dir.path(), None, env, json!({})).unwrap();
        assert_eq!(
            loaded
                .config
                .otel
                .headers
                .get("x-api-key")
                .map(SecretString::expose_secret),
            Some("header-secret")
        );
    }

    #[test]
    fn otel_signal_environment_variables_are_parsed() {
        let dir = tempdir().unwrap();
        let env = vec![
            ("S3_SIDECAR_OTEL__ENABLED".to_owned(), "true".to_owned()),
            ("S3_SIDECAR_OTEL__TRACES".to_owned(), "true".to_owned()),
            ("S3_SIDECAR_OTEL__LOGS".to_owned(), "true".to_owned()),
            (
                "S3_SIDECAR_OTEL__TRACE_SAMPLE_RATIO".to_owned(),
                "0.25".to_owned(),
            ),
            (
                "S3_SIDECAR_OTEL__TRACE_ENDPOINT".to_owned(),
                "http://127.0.0.1:4317".to_owned(),
            ),
            (
                "S3_SIDECAR_OTEL__LOGS_ENDPOINT".to_owned(),
                "http://127.0.0.1:4317".to_owned(),
            ),
            (
                "S3_SIDECAR_OTEL__RECORD_OBJECT_KEYS".to_owned(),
                "true".to_owned(),
            ),
        ];
        let loaded = load_with(dir.path(), None, env, json!({})).unwrap();
        assert!(loaded.config.otel.enabled);
        assert!(loaded.config.otel.traces);
        assert!(loaded.config.otel.logs);
        assert!(loaded.config.otel.record_object_keys);
        assert!((loaded.config.otel.trace_sample_ratio - 0.25).abs() < f64::EPSILON);
        assert_eq!(
            loaded.config.otel.trace_endpoint.as_deref(),
            Some("http://127.0.0.1:4317")
        );
        assert_eq!(
            loaded.config.otel.logs_endpoint.as_deref(),
            Some("http://127.0.0.1:4317")
        );
    }

    #[test]
    fn inline_headers_at_equal_precedence_win_over_the_headers_file() {
        let dir = tempdir().unwrap();
        let headers_path = dir.path().join("otel-headers.json");
        std::fs::write(&headers_path, r#"{"x-api-key":"from-file"}"#).unwrap();
        let env = vec![
            (
                "S3_SIDECAR_OTEL__HEADERS_FILE".to_owned(),
                headers_path.display().to_string(),
            ),
            (
                "S3_SIDECAR_OTEL__HEADERS".to_owned(),
                r#"{"x-api-key":"from-env"}"#.to_owned(),
            ),
        ];
        let loaded = load_with(dir.path(), None, env, json!({})).unwrap();
        assert_eq!(
            loaded
                .config
                .otel
                .headers
                .get("x-api-key")
                .map(SecretString::expose_secret),
            Some("from-env")
        );
    }

    #[test]
    fn headers_file_at_higher_precedence_overrides_inline_headers() {
        let dir = tempdir().unwrap();
        let headers_path = dir.path().join("otel-headers.json");
        std::fs::write(&headers_path, r#"{"x-api-key":"from-file"}"#).unwrap();
        let file = r#"
[otel.headers]
"x-api-key" = "from-config"
"#;
        let env = vec![(
            "S3_SIDECAR_OTEL__HEADERS_FILE".to_owned(),
            headers_path.display().to_string(),
        )];
        let loaded = load_with(dir.path(), Some(file), env, json!({})).unwrap();
        assert_eq!(
            loaded
                .config
                .otel
                .headers
                .get("x-api-key")
                .map(SecretString::expose_secret),
            Some("from-file")
        );
    }

    #[test]
    fn inline_headers_at_higher_precedence_win_over_the_headers_file() {
        let dir = tempdir().unwrap();
        let headers_path = dir.path().join("otel-headers.json");
        std::fs::write(&headers_path, r#"{"x-api-key":"from-file"}"#).unwrap();
        let file = format!("[otel]\nheaders_file = \"{}\"\n", headers_path.display());
        let env = vec![(
            "S3_SIDECAR_OTEL__HEADERS".to_owned(),
            r#"{"x-api-key":"from-env"}"#.to_owned(),
        )];
        let loaded = load_with(dir.path(), Some(&file), env, json!({})).unwrap();
        assert_eq!(
            loaded
                .config
                .otel
                .headers
                .get("x-api-key")
                .map(SecretString::expose_secret),
            Some("from-env")
        );
    }

    #[test]
    fn flag_headers_file_overrides_env_inline_headers() {
        let dir = tempdir().unwrap();
        let headers_path = dir.path().join("otel-headers.json");
        std::fs::write(&headers_path, r#"{"x-api-key":"from-file"}"#).unwrap();
        let env = vec![(
            "S3_SIDECAR_OTEL__HEADERS".to_owned(),
            r#"{"x-api-key":"from-env"}"#.to_owned(),
        )];
        let flags = json!({"otel": {"headers_file": headers_path.display().to_string()}});
        let loaded = load_with(dir.path(), None, env, flags).unwrap();
        assert_eq!(
            loaded
                .config
                .otel
                .headers
                .get("x-api-key")
                .map(SecretString::expose_secret),
            Some("from-file")
        );
    }
}

#[cfg(test)]
mod provenance_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use serde_json::json;
    use tempfile::tempdir;

    use super::{LoadOptions, load, test_cache_flags};
    use crate::config::show_table;

    fn line<'a>(table: &'a str, field: &str) -> &'a str {
        table
            .lines()
            .find(|line| line.starts_with(field))
            .unwrap_or_else(|| panic!("missing field {field} in\n{table}"))
    }

    #[test]
    fn show_config_reports_each_field_source() {
        let dir = tempdir().unwrap();
        let file = r#"
[origin]
endpoint = "http://file:9000"
[[clients]]
access_key_id = "AKIA"
secret_access_key = "secret"
buckets = ["media"]
[[buckets]]
name = "media"
"#;
        let env = vec![(
            "S3_SIDECAR_ORIGIN__REGION".to_owned(),
            "eu-west-1".to_owned(),
        )];
        let flags = json!({"server": {"bind": "127.0.0.1:9999"}});
        let path = dir.path().join("sidecar.toml");
        std::fs::write(&path, file).unwrap();
        let loaded = load(&LoadOptions {
            config_flag: Some(path),
            env,
            flags: test_cache_flags(flags),
            cwd: dir.path().to_path_buf(),
            default_paths: Vec::new(),
        })
        .unwrap();
        let table = show_table(&loaded.config, &loaded.sources);

        assert!(line(&table, "server.bind").contains("flag"));
        assert!(line(&table, "server.bind").contains("127.0.0.1:9999"));
        assert!(line(&table, "origin.endpoint").contains("file"));
        assert!(line(&table, "origin.region").contains("env"));
        assert!(line(&table, "origin.region").contains("eu-west-1"));
        assert!(line(&table, "origin.force_path_style").contains("default"));
        assert!(line(&table, "cache.prefix").contains("default"));
        assert!(line(&table, "origin.session_token").contains("(unset)"));
        assert!(line(&table, "origin.session_token").contains("unset"));
        assert!(line(&table, "cache.max_bytes").contains("0 (unlimited)"));
        assert!(line(&table, "clients[0].session_token").contains("(unset)"));
        assert!(line(&table, "buckets[0].ttl").contains("(unset)"));
    }

    #[test]
    fn flag_wins_over_env_and_file() {
        let dir = tempdir().unwrap();
        let file = r#"
[server]
bind = "127.0.0.1:1111"
[origin]
region = "us-east-1"
"#;
        let env = vec![
            (
                "S3_SIDECAR_SERVER__BIND".to_owned(),
                "127.0.0.1:2222".to_owned(),
            ),
            (
                "S3_SIDECAR_ORIGIN__REGION".to_owned(),
                "eu-west-1".to_owned(),
            ),
        ];
        let flags = json!({"server": {"bind": "127.0.0.1:3333"}});
        let path = dir.path().join("sidecar.toml");
        std::fs::write(&path, file).unwrap();
        let loaded = load(&LoadOptions {
            config_flag: Some(path),
            env,
            flags: test_cache_flags(flags),
            cwd: dir.path().to_path_buf(),
            default_paths: Vec::new(),
        })
        .unwrap();
        assert_eq!(loaded.config.server.bind, "127.0.0.1:3333");
        assert_eq!(loaded.config.origin.region, "eu-west-1");
        let table = show_table(&loaded.config, &loaded.sources);
        assert!(line(&table, "server.bind").contains("flag"));
        assert!(line(&table, "origin.region").contains("env"));
    }
}
