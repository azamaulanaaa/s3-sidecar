//! Command-line interface and flag-to-configuration mapping.

use std::io::Read;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use serde_json::{Map, Value, json};

use crate::config::ConfigError;
use crate::config::loader::{read_secret_file, trim_line_end};
use crate::config::secret::SecretString;

/// S3 sidecar command line.
#[derive(Debug, Parser)]
#[command(name = "s3-sidecar", version, about = "Run an S3 sidecar proxy.")]
pub struct Cli {
    /// Subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level command.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start the S3 proxy server.
    Serve(ServeArgs),
    /// Check the configuration and exit.
    Check(CheckArgs),
}

/// Arguments for `serve`.
#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Configuration sources.
    #[command(flatten)]
    pub config: ConfigFlags,
}

/// Arguments for `check`.
#[derive(Debug, Args)]
pub struct CheckArgs {
    /// Configuration sources.
    #[command(flatten)]
    pub config: ConfigFlags,
    /// Print the merged configuration with secrets redacted.
    #[arg(long)]
    pub show_config: bool,
}

/// Configuration flags. The names follow the TOML paths in kebab-case.
// Each boolean flag is one independent command-line switch.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Args, Default)]
pub struct ConfigFlags {
    /// Path to the TOML configuration file.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,
    /// Run mode: cache or cluster.
    #[arg(long, value_name = "MODE")]
    pub mode: Option<String>,
    /// Listen address.
    #[arg(long = "server-bind", value_name = "ADDRESS")]
    pub server_bind: Option<String>,
    /// Origin S3 endpoint URL.
    #[arg(long = "origin-endpoint", value_name = "URL")]
    pub origin_endpoint: Option<String>,
    /// Origin `SigV4` region.
    #[arg(long = "origin-region", value_name = "REGION")]
    pub origin_region: Option<String>,
    /// Origin access key ID.
    #[arg(long = "origin-access-key-id", value_name = "KEY")]
    pub origin_access_key_id: Option<String>,
    /// File that contains the origin secret access key.
    #[arg(long = "origin-secret-access-key-file", value_name = "PATH")]
    pub origin_secret_access_key_file: Option<PathBuf>,
    /// Read the origin secret access key from stdin.
    #[arg(long = "origin-secret-access-key-stdin")]
    pub origin_secret_access_key_stdin: bool,
    /// Use path-style origin bucket addresses.
    #[arg(long = "origin-force-path-style")]
    pub origin_force_path_style: bool,
    /// Cache S3 endpoint URL.
    #[arg(long = "cache-endpoint", value_name = "URL")]
    pub cache_endpoint: Option<String>,
    /// Cache `SigV4` region.
    #[arg(long = "cache-region", value_name = "REGION")]
    pub cache_region: Option<String>,
    /// Cache access key ID.
    #[arg(long = "cache-access-key-id", value_name = "KEY")]
    pub cache_access_key_id: Option<String>,
    /// File that contains the cache secret access key.
    #[arg(long = "cache-secret-access-key-file", value_name = "PATH")]
    pub cache_secret_access_key_file: Option<PathBuf>,
    /// Read the cache secret access key from stdin.
    #[arg(long = "cache-secret-access-key-stdin")]
    pub cache_secret_access_key_stdin: bool,
    /// Use path-style cache bucket addresses.
    #[arg(long = "cache-force-path-style")]
    pub cache_force_path_style: bool,
    /// Cache S3 bucket. All cache objects and journal entries live here.
    #[arg(long = "cache-bucket", value_name = "BUCKET")]
    pub cache_bucket: Option<String>,
    /// Cache key prefix for this node.
    #[arg(long = "cache-prefix", value_name = "PREFIX")]
    pub cache_prefix: Option<String>,
    /// Cache node name. It is part of the cache key and journal path.
    #[arg(long = "cache-node-id", value_name = "ID")]
    pub cache_node_id: Option<String>,
    /// Cache mode: `off`, `read`, `write`, or `read_write`.
    #[arg(long = "cache-mode", value_name = "MODE")]
    pub cache_mode: Option<String>,
    /// Maximum cache size in bytes. 0 means unlimited.
    #[arg(long = "cache-max-bytes", value_name = "BYTES")]
    pub cache_max_bytes: Option<u64>,
    /// Maximum object size for the cache in bytes.
    #[arg(long = "cache-max-object-bytes", value_name = "BYTES")]
    pub cache_max_object_bytes: Option<u64>,
    /// Cache write policy: `write_back` or `write_through`.
    #[arg(long = "cache-write-policy", value_name = "POLICY")]
    pub cache_write_policy: Option<String>,
    /// Cache failure action: origin or fail.
    #[arg(long = "cache-on-cache-error", value_name = "ACTION")]
    pub cache_on_cache_error: Option<String>,
    /// Eviction high watermark.
    #[arg(long = "cache-high-watermark", value_name = "FRACTION")]
    pub cache_high_watermark: Option<f64>,
    /// Eviction low watermark.
    #[arg(long = "cache-low-watermark", value_name = "FRACTION")]
    pub cache_low_watermark: Option<f64>,
    /// Age at which a blob with no metadata is an orphan.
    #[arg(long = "cache-staging-ttl", value_name = "DURATION")]
    pub cache_staging_ttl: Option<String>,
    /// Age at which a stale journal entry is removed.
    #[arg(long = "cache-orphan-ttl", value_name = "DURATION")]
    pub cache_orphan_ttl: Option<String>,
    /// Number of concurrent key syncs.
    #[arg(long = "cache-sync-workers", value_name = "COUNT")]
    pub cache_sync_workers: Option<usize>,
    /// Maximum sync attempts before quarantine.
    #[arg(long = "cache-sync-max-attempts", value_name = "COUNT")]
    pub cache_sync_max_attempts: Option<u32>,
    /// Base delay for sync retries, in milliseconds.
    #[arg(long = "cache-sync-retry-base-ms", value_name = "MS")]
    pub cache_sync_retry_base_ms: Option<u64>,
    /// Delay before a key sync starts, in milliseconds.
    #[arg(long = "cache-sync-debounce-ms", value_name = "MS")]
    pub cache_sync_debounce_ms: Option<u64>,
    /// Interval between garbage collection passes.
    #[arg(long = "cache-gc-interval", value_name = "DURATION")]
    pub cache_gc_interval: Option<String>,
    /// Path of the `redb` LRU index.
    #[arg(long = "cache-index-path", value_name = "PATH")]
    pub cache_index_path: Option<String>,
    /// Serve quarantined entries instead of a 404.
    #[arg(long = "cache-serve-quarantined")]
    pub cache_serve_quarantined: bool,
    /// Backing S3 endpoint URL for cluster mode.
    #[arg(long = "backing-endpoint", value_name = "URL")]
    pub backing_endpoint: Option<String>,
    /// Backing `SigV4` region.
    #[arg(long = "backing-region", value_name = "REGION")]
    pub backing_region: Option<String>,
    /// Backing access key ID.
    #[arg(long = "backing-access-key-id", value_name = "KEY")]
    pub backing_access_key_id: Option<String>,
    /// File that contains the backing secret access key.
    #[arg(long = "backing-secret-access-key-file", value_name = "PATH")]
    pub backing_secret_access_key_file: Option<PathBuf>,
    /// Read the backing secret access key from stdin.
    #[arg(long = "backing-secret-access-key-stdin")]
    pub backing_secret_access_key_stdin: bool,
    /// Use path-style backing bucket addresses.
    #[arg(long = "backing-force-path-style")]
    pub backing_force_path_style: bool,
    /// Coordination backend: raft or consul.
    #[arg(long = "consensus-backend", value_name = "BACKEND")]
    pub consensus_backend: Option<String>,
    /// Raft node name.
    #[arg(long = "consensus-raft-node-id", value_name = "ID")]
    pub consensus_raft_node_id: Option<String>,
    /// Raft peers as a JSON array.
    #[arg(long = "consensus-raft-peers", value_name = "JSON")]
    pub consensus_raft_peers: Option<String>,
    /// Buckets as a JSON array.
    #[arg(long = "buckets-json", value_name = "JSON")]
    pub buckets_json: Option<String>,
    /// Bucket name. Repeat this flag to add more buckets.
    #[arg(long = "bucket", value_name = "NAME")]
    pub bucket: Vec<String>,
    /// Clients as a JSON array.
    #[arg(long = "clients-json", value_name = "JSON")]
    pub clients_json: Option<String>,
}

impl ConfigFlags {
    /// Return the endpoint that must read its secret from stdin.
    ///
    /// # Errors
    ///
    /// Returns an error when more than one stdin secret flag is set.
    pub fn stdin_target(&self) -> Result<Option<StdinTarget>, ConfigError> {
        let targets = [
            (self.origin_secret_access_key_stdin, StdinTarget::Origin),
            (self.cache_secret_access_key_stdin, StdinTarget::Cache),
            (self.backing_secret_access_key_stdin, StdinTarget::Backing),
        ];
        let mut selected = targets
            .into_iter()
            .filter_map(|(enabled, target)| enabled.then_some(target));
        let first = selected.next();
        if selected.next().is_some() {
            return Err(ConfigError::invalid(
                "only one secret may be read from stdin",
            ));
        }
        Ok(first)
    }

    /// Build a JSON layer from the flags.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid JSON, a missing secret file, or a stdin
    /// secret that was not supplied.
    // The flag table is flat on purpose: each flag maps to one TOML path.
    #[allow(clippy::too_many_lines)]
    pub fn to_layer(&self, stdin_secret: Option<&SecretString>) -> Result<Value, ConfigError> {
        let mut object = Map::new();

        if let Some(value) = &self.mode {
            set(&mut object, &["mode"], Value::String(value.clone()));
        }
        if let Some(value) = &self.server_bind {
            set(
                &mut object,
                &["server", "bind"],
                Value::String(value.clone()),
            );
        }

        if let Some(value) = &self.origin_endpoint {
            set(
                &mut object,
                &["origin", "endpoint"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.origin_region {
            set(
                &mut object,
                &["origin", "region"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.origin_access_key_id {
            set(
                &mut object,
                &["origin", "access_key_id"],
                Value::String(value.clone()),
            );
        }
        if let Some(path) = &self.origin_secret_access_key_file {
            let secret = read_secret_file(path)?;
            set(
                &mut object,
                &["origin", "secret_access_key"],
                Value::String(secret.expose_secret().to_owned()),
            );
        }
        if self.origin_force_path_style {
            set(
                &mut object,
                &["origin", "force_path_style"],
                Value::Bool(true),
            );
        }

        if let Some(value) = &self.cache_endpoint {
            set(
                &mut object,
                &["cache", "endpoint"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cache_region {
            set(
                &mut object,
                &["cache", "region"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cache_access_key_id {
            set(
                &mut object,
                &["cache", "access_key_id"],
                Value::String(value.clone()),
            );
        }
        if let Some(path) = &self.cache_secret_access_key_file {
            let secret = read_secret_file(path)?;
            set(
                &mut object,
                &["cache", "secret_access_key"],
                Value::String(secret.expose_secret().to_owned()),
            );
        }
        if self.cache_force_path_style {
            set(
                &mut object,
                &["cache", "force_path_style"],
                Value::Bool(true),
            );
        }
        if let Some(value) = &self.cache_prefix {
            set(
                &mut object,
                &["cache", "prefix"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cache_bucket {
            set(
                &mut object,
                &["cache", "bucket"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cache_node_id {
            set(
                &mut object,
                &["cache", "node_id"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cache_mode {
            set(
                &mut object,
                &["cache", "mode"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = self.cache_max_bytes {
            set(&mut object, &["cache", "max_bytes"], json!(value));
        }
        if let Some(value) = self.cache_max_object_bytes {
            set(&mut object, &["cache", "max_object_bytes"], json!(value));
        }
        if let Some(value) = &self.cache_write_policy {
            set(
                &mut object,
                &["cache", "write_policy"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cache_on_cache_error {
            set(
                &mut object,
                &["cache", "on_cache_error"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = self.cache_high_watermark {
            set(&mut object, &["cache", "high_watermark"], json!(value));
        }
        if let Some(value) = self.cache_low_watermark {
            set(&mut object, &["cache", "low_watermark"], json!(value));
        }
        if let Some(value) = &self.cache_staging_ttl {
            set(
                &mut object,
                &["cache", "staging_ttl"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cache_orphan_ttl {
            set(
                &mut object,
                &["cache", "orphan_ttl"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = self.cache_sync_workers {
            set(&mut object, &["cache", "sync_workers"], json!(value));
        }
        if let Some(value) = self.cache_sync_max_attempts {
            set(&mut object, &["cache", "sync_max_attempts"], json!(value));
        }
        if let Some(value) = self.cache_sync_retry_base_ms {
            set(&mut object, &["cache", "sync_retry_base_ms"], json!(value));
        }
        if let Some(value) = self.cache_sync_debounce_ms {
            set(&mut object, &["cache", "sync_debounce_ms"], json!(value));
        }
        if let Some(value) = &self.cache_gc_interval {
            set(
                &mut object,
                &["cache", "gc_interval"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cache_index_path {
            set(
                &mut object,
                &["cache", "index_path"],
                Value::String(value.clone()),
            );
        }
        if self.cache_serve_quarantined {
            set(
                &mut object,
                &["cache", "serve_quarantined"],
                Value::Bool(true),
            );
        }

        if let Some(value) = &self.backing_endpoint {
            set(
                &mut object,
                &["backing", "endpoint"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.backing_region {
            set(
                &mut object,
                &["backing", "region"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.backing_access_key_id {
            set(
                &mut object,
                &["backing", "access_key_id"],
                Value::String(value.clone()),
            );
        }
        if let Some(path) = &self.backing_secret_access_key_file {
            let secret = read_secret_file(path)?;
            set(
                &mut object,
                &["backing", "secret_access_key"],
                Value::String(secret.expose_secret().to_owned()),
            );
        }
        if self.backing_force_path_style {
            set(
                &mut object,
                &["backing", "force_path_style"],
                Value::Bool(true),
            );
        }

        if let Some(value) = &self.consensus_backend {
            set(
                &mut object,
                &["consensus", "backend"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.consensus_raft_node_id {
            set(
                &mut object,
                &["consensus", "raft", "node_id"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.consensus_raft_peers {
            let parsed = parse_json_array("--consensus-raft-peers", value)?;
            set(&mut object, &["consensus", "raft", "peers"], parsed);
        }

        if let Some(value) = &self.buckets_json {
            let parsed = parse_json_array("--buckets-json", value)?;
            set(&mut object, &["buckets"], parsed);
        }
        if !self.bucket.is_empty() {
            let mut buckets = match object.remove("buckets") {
                Some(Value::Array(buckets)) => buckets,
                _ => Vec::new(),
            };
            for name in &self.bucket {
                buckets.push(json!({ "name": name }));
            }
            set(&mut object, &["buckets"], Value::Array(buckets));
        }

        if let Some(value) = &self.clients_json {
            let parsed = parse_json_array("--clients-json", value)?;
            set(&mut object, &["clients"], parsed);
        }

        if let Some(target) = self.stdin_target()? {
            let secret = stdin_secret.ok_or_else(|| {
                ConfigError::invalid("a stdin secret flag is set but stdin was not read")
            })?;
            let path = match target {
                StdinTarget::Origin => ["origin", "secret_access_key"],
                StdinTarget::Cache => ["cache", "secret_access_key"],
                StdinTarget::Backing => ["backing", "secret_access_key"],
            };
            set(
                &mut object,
                &path,
                Value::String(secret.expose_secret().to_owned()),
            );
        }

        Ok(Value::Object(object))
    }
}

/// Endpoint that reads a secret from stdin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdinTarget {
    /// Origin endpoint.
    Origin,
    /// Cache endpoint.
    Cache,
    /// Backing endpoint.
    Backing,
}

fn parse_json_array(name: &str, value: &str) -> Result<Value, ConfigError> {
    let parsed: Value = serde_json::from_str(value).map_err(|source| ConfigError::FlagsJson {
        name: name.to_owned(),
        source,
    })?;
    if parsed.is_array() {
        Ok(parsed)
    } else {
        Err(ConfigError::FlagsType {
            name: name.to_owned(),
            expected: "array".to_owned(),
        })
    }
}

fn set(object: &mut Map<String, Value>, path: &[&str], value: Value) {
    let mut current = object;
    for part in &path[..path.len().saturating_sub(1)] {
        let entry = current
            .entry((*part).to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        if !entry.is_object() {
            *entry = Value::Object(Map::new());
        }
        let Some(next) = entry.as_object_mut() else {
            return;
        };
        current = next;
    }
    if let Some(last) = path.last() {
        current.insert((*last).to_owned(), value);
    }
}

/// Read a secret from stdin. A final line break is removed.
///
/// # Errors
///
/// Returns an error when stdin cannot be read.
pub fn read_stdin_secret() -> Result<SecretString, ConfigError> {
    let mut value = String::new();
    std::io::stdin()
        .read_to_string(&mut value)
        .map_err(ConfigError::ReadStdin)?;
    Ok(SecretString::new(trim_line_end(&value).to_owned()))
}

impl std::fmt::Display for StdinTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Origin => f.write_str("origin"),
            Self::Cache => f.write_str("cache"),
            Self::Backing => f.write_str("backing"),
        }
    }
}
