//! Command-line interface and flag-to-configuration mapping.

use std::io::Read;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use serde_json::{Map, Value, json};

use crate::config::ConfigError;
use crate::config::loader::{BUCKETS_ADD_KEY, read_secret_file, trim_line_end};
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
#[expect(
    clippy::struct_excessive_bools,
    reason = "each boolean flag is one command-line switch"
)]
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
    /// Use multipart for internal object writes. The value is optional and
    /// defaults to true when the flag has no value.
    #[arg(
        long = "multipart-enabled",
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        conflicts_with = "no_multipart"
    )]
    pub multipart_enabled: Option<bool>,
    /// Disable multipart for internal object writes.
    #[arg(long = "no-multipart", conflicts_with = "multipart_enabled")]
    pub no_multipart: bool,
    /// Smallest object size that uses multipart, for example `64MiB`.
    #[arg(long = "multipart-threshold", value_name = "SIZE")]
    pub multipart_threshold: Option<String>,
    /// Target part size, for example `16MiB`.
    #[arg(long = "multipart-chunk-size", value_name = "SIZE")]
    pub multipart_chunk_size: Option<String>,
    /// Smallest allowed part size, for example `5MiB`.
    #[arg(long = "multipart-min-chunk-size", value_name = "SIZE")]
    pub multipart_min_chunk_size: Option<String>,
    /// Largest number of parts in one upload.
    #[arg(long = "multipart-max-parts", value_name = "COUNT")]
    pub multipart_max_parts: Option<u32>,
    /// Largest number of part uploads in flight.
    #[arg(long = "multipart-concurrency", value_name = "COUNT")]
    pub multipart_concurrency: Option<usize>,
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
    /// Cache key prefix.
    #[arg(long = "cache-prefix", value_name = "PREFIX")]
    pub cache_prefix: Option<String>,
    /// Cache mode: `read`, `write`, or `read_write`.
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
    /// Backing S3 bucket for cluster blobs and uploads.
    #[arg(long = "backing-bucket", value_name = "BUCKET")]
    pub backing_bucket: Option<String>,
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
    /// File that contains the raft internal token.
    #[arg(long = "consensus-raft-secret-file", value_name = "PATH")]
    pub consensus_raft_secret_file: Option<PathBuf>,
    /// Read the raft internal token from stdin.
    #[arg(long = "consensus-raft-secret-stdin")]
    pub consensus_raft_secret_stdin: bool,
    /// Consul node name.
    #[arg(long = "consensus-consul-node-id", value_name = "ID")]
    pub consensus_consul_node_id: Option<String>,
    /// Consul HTTP API base URL.
    #[arg(long = "consensus-consul-address", value_name = "URL")]
    pub consensus_consul_address: Option<String>,
    /// Base URL that peers use to reach this sidecar.
    #[arg(long = "consensus-consul-advertise-addr", value_name = "URL")]
    pub consensus_consul_advertise_addr: Option<String>,
    /// File that contains the Consul ACL token.
    #[arg(long = "consensus-consul-token-file", value_name = "PATH")]
    pub consensus_consul_token_file: Option<PathBuf>,
    /// Read the Consul ACL token from stdin.
    #[arg(long = "consensus-consul-token-stdin")]
    pub consensus_consul_token_stdin: bool,
    /// Consul session time to live.
    #[arg(long = "consensus-consul-session-ttl", value_name = "DURATION")]
    pub consensus_consul_session_ttl: Option<String>,
    /// File that contains the Consul proposal token.
    #[arg(long = "consensus-consul-secret-file", value_name = "PATH")]
    pub consensus_consul_secret_file: Option<PathBuf>,
    /// Read the Consul proposal token from stdin.
    #[arg(long = "consensus-consul-secret-stdin")]
    pub consensus_consul_secret_stdin: bool,
    /// Age at which a staged blob with no commit is an orphan.
    #[arg(long = "cluster-staging-ttl", value_name = "DURATION")]
    pub cluster_staging_ttl: Option<String>,
    /// Age at which a tombstone and its blob are removed.
    #[arg(long = "cluster-orphan-ttl", value_name = "DURATION")]
    pub cluster_orphan_ttl: Option<String>,
    /// Interval between cluster garbage collection passes.
    #[arg(long = "cluster-gc-interval", value_name = "DURATION")]
    pub cluster_gc_interval: Option<String>,
    /// Path of the `redb` cluster metadata file.
    #[arg(long = "cluster-metadata-path", value_name = "PATH")]
    pub cluster_metadata_path: Option<String>,
    /// File that contains the cluster blob transport token.
    #[arg(long = "cluster-secret-file", value_name = "PATH")]
    pub cluster_secret_file: Option<PathBuf>,
    /// Read the cluster blob transport token from stdin.
    #[arg(long = "cluster-secret-stdin")]
    pub cluster_secret_stdin: bool,
    /// Store a peer blob locally while serving it. The value is optional and
    /// defaults to true when the flag has no value.
    #[arg(
        long = "cluster-read-repair",
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        conflicts_with = "no_cluster_read_repair"
    )]
    pub cluster_read_repair: Option<bool>,
    /// Do not store a peer blob locally while serving it.
    #[arg(
        long = "no-cluster-read-repair",
        conflicts_with = "cluster_read_repair"
    )]
    pub no_cluster_read_repair: bool,
    /// Number of concurrent replication workers.
    #[arg(long = "cluster-replicate-workers", value_name = "COUNT")]
    pub cluster_replicate_workers: Option<usize>,
    /// Maximum time to wait for one synchronous peer blob copy.
    #[arg(long = "cluster-replicate-timeout", value_name = "DURATION")]
    pub cluster_replicate_timeout: Option<String>,
    /// Interval between replica reconciliation passes.
    #[arg(long = "cluster-reconcile-interval", value_name = "DURATION")]
    pub cluster_reconcile_interval: Option<String>,
    /// Cluster nodes as a JSON array.
    #[arg(long = "cluster-nodes-json", value_name = "JSON")]
    pub cluster_nodes_json: Option<String>,
    /// Export metrics over OTLP.
    #[arg(long = "otel-enabled")]
    pub otel_enabled: bool,
    /// Export metrics over OTLP when OpenTelemetry is enabled. The value is
    /// optional and defaults to true when the flag has no value.
    #[arg(
        long = "otel-metrics",
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        conflicts_with = "no_otel_metrics"
    )]
    pub otel_metrics: Option<bool>,
    /// Do not export metrics over OTLP.
    #[arg(long = "no-otel-metrics", conflicts_with = "otel_metrics")]
    pub no_otel_metrics: bool,
    /// Export traces over OTLP. The value is optional and defaults to true
    /// when the flag has no value.
    #[arg(
        long = "otel-traces",
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        conflicts_with = "no_otel_traces"
    )]
    pub otel_traces: Option<bool>,
    /// Do not export traces over OTLP.
    #[arg(long = "no-otel-traces", conflicts_with = "otel_traces")]
    pub no_otel_traces: bool,
    /// Export logs over OTLP. The value is optional and defaults to true when
    /// the flag has no value.
    #[arg(
        long = "otel-logs",
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        conflicts_with = "no_otel_logs"
    )]
    pub otel_logs: Option<bool>,
    /// Do not export logs over OTLP.
    #[arg(long = "no-otel-logs", conflicts_with = "otel_logs")]
    pub no_otel_logs: bool,
    /// Fraction of root traces to sample, from 0.0 to 1.0.
    #[arg(long = "otel-trace-sample-ratio", value_name = "RATIO")]
    pub otel_trace_sample_ratio: Option<f64>,
    /// OTLP collector URL for traces.
    #[arg(long = "otel-trace-endpoint", value_name = "URL")]
    pub otel_trace_endpoint: Option<String>,
    /// OTLP collector URL for logs.
    #[arg(long = "otel-logs-endpoint", value_name = "URL")]
    pub otel_logs_endpoint: Option<String>,
    /// Delay between trace exports.
    #[arg(long = "otel-trace-schedule-delay", value_name = "DURATION")]
    pub otel_trace_schedule_delay: Option<String>,
    /// Delay between log exports.
    #[arg(long = "otel-log-schedule-delay", value_name = "DURATION")]
    pub otel_log_schedule_delay: Option<String>,
    /// Add the `s3.object.key` attribute to spans. The value is optional and
    /// defaults to true when the flag has no value.
    #[arg(
        long = "otel-record-object-keys",
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        conflicts_with = "no_otel_record_object_keys"
    )]
    pub otel_record_object_keys: Option<bool>,
    /// Do not add the `s3.object.key` attribute to spans.
    #[arg(
        long = "no-otel-record-object-keys",
        conflicts_with = "otel_record_object_keys"
    )]
    pub no_otel_record_object_keys: bool,
    /// Service name for exported telemetry.
    #[arg(long = "otel-service-name", value_name = "NAME")]
    pub otel_service_name: Option<String>,
    /// Service version for exported telemetry.
    #[arg(long = "otel-service-version", value_name = "VERSION")]
    pub otel_service_version: Option<String>,
    /// OTLP collector URL.
    #[arg(long = "otel-endpoint", value_name = "URL")]
    pub otel_endpoint: Option<String>,
    /// OTLP transport: `grpc` or `http/protobuf`.
    #[arg(long = "otel-protocol", value_name = "PROTOCOL")]
    pub otel_protocol: Option<String>,
    /// OTLP request headers as a JSON object.
    #[arg(long = "otel-headers-json", value_name = "JSON")]
    pub otel_headers_json: Option<String>,
    /// File that contains a JSON object of OTLP request headers.
    #[arg(long = "otel-headers-file", value_name = "PATH")]
    pub otel_headers_file: Option<PathBuf>,
    /// Timeout for one OTLP export request.
    #[arg(long = "otel-timeout", value_name = "DURATION")]
    pub otel_timeout: Option<String>,
    /// Interval between metrics exports.
    #[arg(long = "otel-export-interval", value_name = "DURATION")]
    pub otel_export_interval: Option<String>,
    /// Bound for the final flush during shutdown.
    #[arg(long = "otel-export-timeout", value_name = "DURATION")]
    pub otel_export_timeout: Option<String>,
    /// OTLP resource attributes as a JSON object.
    #[arg(long = "otel-resource-attributes-json", value_name = "JSON")]
    pub otel_resource_attributes_json: Option<String>,
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
            (self.consensus_raft_secret_stdin, StdinTarget::Raft),
            (self.consensus_consul_token_stdin, StdinTarget::ConsulToken),
            (
                self.consensus_consul_secret_stdin,
                StdinTarget::ConsulSecret,
            ),
            (self.cluster_secret_stdin, StdinTarget::Cluster),
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
    #[expect(
        clippy::too_many_lines,
        reason = "the flag table maps each flag to one TOML path"
    )]
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
        if let Some(value) = self.multipart_enabled {
            set(&mut object, &["multipart", "enabled"], Value::Bool(value));
        }
        if self.no_multipart {
            set(&mut object, &["multipart", "enabled"], Value::Bool(false));
        }
        if let Some(value) = &self.multipart_threshold {
            set(
                &mut object,
                &["multipart", "threshold"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.multipart_chunk_size {
            set(
                &mut object,
                &["multipart", "chunk_size"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.multipart_min_chunk_size {
            set(
                &mut object,
                &["multipart", "min_chunk_size"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = self.multipart_max_parts {
            set(&mut object, &["multipart", "max_parts"], json!(value));
        }
        if let Some(value) = self.multipart_concurrency {
            set(&mut object, &["multipart", "concurrency"], json!(value));
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
        if let Some(value) = &self.backing_bucket {
            set(
                &mut object,
                &["backing", "bucket"],
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
        if let Some(path) = &self.consensus_raft_secret_file {
            let secret = read_secret_file(path)?;
            set(
                &mut object,
                &["consensus", "raft", "secret"],
                Value::String(secret.expose_secret().to_owned()),
            );
        }
        if let Some(value) = &self.consensus_consul_node_id {
            set(
                &mut object,
                &["consensus", "consul", "node_id"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.consensus_consul_address {
            set(
                &mut object,
                &["consensus", "consul", "address"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.consensus_consul_advertise_addr {
            set(
                &mut object,
                &["consensus", "consul", "advertise_addr"],
                Value::String(value.clone()),
            );
        }
        if let Some(path) = &self.consensus_consul_token_file {
            let secret = read_secret_file(path)?;
            set(
                &mut object,
                &["consensus", "consul", "token"],
                Value::String(secret.expose_secret().to_owned()),
            );
        }
        if let Some(value) = &self.consensus_consul_session_ttl {
            set(
                &mut object,
                &["consensus", "consul", "session_ttl"],
                Value::String(value.clone()),
            );
        }
        if let Some(path) = &self.consensus_consul_secret_file {
            let secret = read_secret_file(path)?;
            set(
                &mut object,
                &["consensus", "consul", "secret"],
                Value::String(secret.expose_secret().to_owned()),
            );
        }
        if let Some(value) = &self.cluster_staging_ttl {
            set(
                &mut object,
                &["cluster", "staging_ttl"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cluster_orphan_ttl {
            set(
                &mut object,
                &["cluster", "orphan_ttl"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cluster_gc_interval {
            set(
                &mut object,
                &["cluster", "gc_interval"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cluster_metadata_path {
            set(
                &mut object,
                &["cluster", "metadata_path"],
                Value::String(value.clone()),
            );
        }
        if let Some(path) = &self.cluster_secret_file {
            let secret = read_secret_file(path)?;
            set(
                &mut object,
                &["cluster", "secret"],
                Value::String(secret.expose_secret().to_owned()),
            );
        }
        if let Some(value) = self.cluster_read_repair {
            set(&mut object, &["cluster", "read_repair"], Value::Bool(value));
        }
        if self.no_cluster_read_repair {
            set(&mut object, &["cluster", "read_repair"], Value::Bool(false));
        }
        if let Some(value) = self.cluster_replicate_workers {
            set(&mut object, &["cluster", "replicate_workers"], json!(value));
        }
        if let Some(value) = &self.cluster_replicate_timeout {
            set(
                &mut object,
                &["cluster", "replicate_timeout"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cluster_reconcile_interval {
            set(
                &mut object,
                &["cluster", "reconcile_interval"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cluster_nodes_json {
            let parsed = parse_json_array("--cluster-nodes-json", value)?;
            set(&mut object, &["cluster", "nodes"], parsed);
        }

        if self.otel_enabled {
            set(&mut object, &["otel", "enabled"], Value::Bool(true));
        }
        if let Some(value) = self.otel_metrics {
            set(&mut object, &["otel", "metrics"], Value::Bool(value));
        }
        if self.no_otel_metrics {
            set(&mut object, &["otel", "metrics"], Value::Bool(false));
        }
        if let Some(value) = self.otel_traces {
            set(&mut object, &["otel", "traces"], Value::Bool(value));
        }
        if self.no_otel_traces {
            set(&mut object, &["otel", "traces"], Value::Bool(false));
        }
        if let Some(value) = self.otel_logs {
            set(&mut object, &["otel", "logs"], Value::Bool(value));
        }
        if self.no_otel_logs {
            set(&mut object, &["otel", "logs"], Value::Bool(false));
        }
        if let Some(value) = self.otel_trace_sample_ratio {
            set(&mut object, &["otel", "trace_sample_ratio"], json!(value));
        }
        if let Some(value) = &self.otel_trace_endpoint {
            set(
                &mut object,
                &["otel", "trace_endpoint"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.otel_logs_endpoint {
            set(
                &mut object,
                &["otel", "logs_endpoint"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.otel_trace_schedule_delay {
            set(
                &mut object,
                &["otel", "trace_schedule_delay"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.otel_log_schedule_delay {
            set(
                &mut object,
                &["otel", "log_schedule_delay"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = self.otel_record_object_keys {
            set(
                &mut object,
                &["otel", "record_object_keys"],
                Value::Bool(value),
            );
        }
        if self.no_otel_record_object_keys {
            set(
                &mut object,
                &["otel", "record_object_keys"],
                Value::Bool(false),
            );
        }
        if let Some(value) = &self.otel_service_name {
            set(
                &mut object,
                &["otel", "service_name"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.otel_service_version {
            set(
                &mut object,
                &["otel", "service_version"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.otel_endpoint {
            set(
                &mut object,
                &["otel", "endpoint"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.otel_protocol {
            set(
                &mut object,
                &["otel", "protocol"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.otel_headers_json {
            let parsed = parse_json_object("--otel-headers-json", value)?;
            set(&mut object, &["otel", "headers"], parsed);
        }
        if let Some(path) = &self.otel_headers_file {
            set(
                &mut object,
                &["otel", "headers_file"],
                Value::String(path.display().to_string()),
            );
        }
        if let Some(value) = &self.otel_timeout {
            set(
                &mut object,
                &["otel", "timeout"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.otel_export_interval {
            set(
                &mut object,
                &["otel", "export_interval"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.otel_export_timeout {
            set(
                &mut object,
                &["otel", "export_timeout"],
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.otel_resource_attributes_json {
            let parsed = parse_json_object("--otel-resource-attributes-json", value)?;
            set(&mut object, &["otel", "resource_attributes"], parsed);
        }

        if let Some(value) = &self.buckets_json {
            let parsed = parse_json_array("--buckets-json", value)?;
            set(&mut object, &["buckets"], parsed);
        }
        if !self.bucket.is_empty() {
            let names = self.bucket.iter().map(|name| json!(name)).collect();
            object.insert(BUCKETS_ADD_KEY.to_owned(), Value::Array(names));
        }

        if let Some(value) = &self.clients_json {
            let parsed = parse_json_array("--clients-json", value)?;
            set(&mut object, &["clients"], parsed);
        }

        if let Some(target) = self.stdin_target()? {
            let secret = stdin_secret.ok_or_else(|| {
                ConfigError::invalid("a stdin secret flag is set but stdin was not read")
            })?;
            let path: &[&str] = match target {
                StdinTarget::Origin => &["origin", "secret_access_key"],
                StdinTarget::Cache => &["cache", "secret_access_key"],
                StdinTarget::Backing => &["backing", "secret_access_key"],
                StdinTarget::Raft => &["consensus", "raft", "secret"],
                StdinTarget::ConsulToken => &["consensus", "consul", "token"],
                StdinTarget::ConsulSecret => &["consensus", "consul", "secret"],
                StdinTarget::Cluster => &["cluster", "secret"],
            };
            set(
                &mut object,
                path,
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
    /// Raft internal token.
    Raft,
    /// Consul ACL token.
    ConsulToken,
    /// Consul proposal token.
    ConsulSecret,
    /// Cluster blob transport token.
    Cluster,
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

fn parse_json_object(name: &str, value: &str) -> Result<Value, ConfigError> {
    let parsed: Value = serde_json::from_str(value).map_err(|source| ConfigError::FlagsJson {
        name: name.to_owned(),
        source,
    })?;
    if parsed.is_object() {
        Ok(parsed)
    } else {
        Err(ConfigError::FlagsType {
            name: name.to_owned(),
            expected: "object".to_owned(),
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
            Self::Raft => f.write_str("raft"),
            Self::ConsulToken => f.write_str("consul token"),
            Self::ConsulSecret => f.write_str("consul secret"),
            Self::Cluster => f.write_str("cluster secret"),
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code uses unwrap for fixtures")]

    use std::fs;

    use clap::Parser;
    use tempfile::tempdir;

    use super::{Cli, Command, ConfigFlags, StdinTarget};
    use crate::config::loader::{LoadOptions, load};

    fn flag_layer(args: &[&str]) -> serde_json::Value {
        let cli = Cli::try_parse_from(args).unwrap();
        let Command::Check(check) = cli.command else {
            panic!("the test must use the check command");
        };
        check.config.to_layer(None).unwrap()
    }

    fn layer_bool(layer: &serde_json::Value, section: &str, key: &str) -> Option<bool> {
        layer
            .get(section)
            .and_then(|value| value.get(key))
            .and_then(serde_json::Value::as_bool)
    }

    fn multipart_enabled(args: &[&str]) -> Option<bool> {
        let cli = Cli::try_parse_from(args).unwrap();
        let Command::Check(check) = cli.command else {
            panic!("the test must use the check command");
        };
        let layer = check.config.to_layer(None).unwrap();
        layer
            .get("multipart")
            .and_then(|multipart| multipart.get("enabled"))
            .and_then(serde_json::Value::as_bool)
    }

    #[test]
    fn multipart_flag_accepts_true_and_false_forms() {
        assert_eq!(
            multipart_enabled(&["s3-sidecar", "check", "--multipart-enabled"]),
            Some(true)
        );
        assert_eq!(
            multipart_enabled(&["s3-sidecar", "check", "--multipart-enabled=true"]),
            Some(true)
        );
        assert_eq!(
            multipart_enabled(&["s3-sidecar", "check", "--multipart-enabled", "true"]),
            Some(true)
        );
        assert_eq!(
            multipart_enabled(&["s3-sidecar", "check", "--multipart-enabled=false"]),
            Some(false)
        );
        assert_eq!(
            multipart_enabled(&["s3-sidecar", "check", "--multipart-enabled", "false"]),
            Some(false)
        );
        assert_eq!(
            multipart_enabled(&["s3-sidecar", "check", "--no-multipart"]),
            Some(false)
        );
        assert_eq!(multipart_enabled(&["s3-sidecar", "check"]), None);
    }

    #[test]
    fn boolean_flags_accept_bare_true_and_inverse_false_forms() {
        for (flag, section, key) in [
            ("cluster-read-repair", "cluster", "read_repair"),
            ("otel-metrics", "otel", "metrics"),
            ("otel-traces", "otel", "traces"),
            ("otel-logs", "otel", "logs"),
            ("otel-record-object-keys", "otel", "record_object_keys"),
        ] {
            let bare_flag = format!("--{flag}");
            let bare = flag_layer(&["s3-sidecar", "check", &bare_flag]);
            assert_eq!(layer_bool(&bare, section, key), Some(true), "{bare_flag}");

            let inverse_flag = format!("--no-{flag}");
            let inverse = flag_layer(&["s3-sidecar", "check", &inverse_flag]);
            assert_eq!(
                layer_bool(&inverse, section, key),
                Some(false),
                "{inverse_flag}"
            );

            let explicit_flag = format!("--{flag}=false");
            let explicit = flag_layer(&["s3-sidecar", "check", &explicit_flag]);
            assert_eq!(
                layer_bool(&explicit, section, key),
                Some(false),
                "{explicit_flag}"
            );

            let absent = flag_layer(&["s3-sidecar", "check"]);
            assert_eq!(layer_bool(&absent, section, key), None, "no flag");
        }
    }

    #[test]
    fn inverse_flags_override_true_settings_from_the_config_file() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("sidecar.toml");
        fs::write(
            &config_path,
            r#"
[cache]
endpoint = "http://127.0.0.1:9002"
bucket = "cache"
max_bytes = 0

[cluster]
read_repair = true

[otel]
enabled = true
metrics = true
traces = true
logs = true
record_object_keys = true
"#,
        )
        .unwrap();

        let config_arg = config_path.to_str().unwrap();
        let cli = Cli::try_parse_from([
            "s3-sidecar",
            "check",
            "--config",
            config_arg,
            "--no-cluster-read-repair",
            "--no-otel-metrics",
            "--no-otel-traces",
            "--no-otel-logs",
            "--no-otel-record-object-keys",
        ])
        .unwrap();
        let Command::Check(check) = cli.command else {
            panic!("the test must use the check command");
        };
        let flags = check.config.to_layer(None).unwrap();
        let loaded = load(&LoadOptions {
            config_flag: Some(config_path),
            env: Vec::new(),
            flags,
            cwd: dir.path().to_path_buf(),
            default_paths: Vec::new(),
        })
        .unwrap();

        assert!(!loaded.config.cluster.read_repair);
        assert!(!loaded.config.otel.metrics);
        assert!(!loaded.config.otel.traces);
        assert!(!loaded.config.otel.logs);
        assert!(!loaded.config.otel.record_object_keys);
    }

    #[test]
    fn cluster_secret_file_flag_sets_the_token() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cluster-token");
        fs::write(&path, "cluster-file-token\n").unwrap();
        let flags = ConfigFlags {
            cluster_secret_file: Some(path),
            ..ConfigFlags::default()
        };
        let layer = flags.to_layer(None).unwrap();
        assert_eq!(layer["cluster"]["secret"], "cluster-file-token");
    }

    #[test]
    fn cluster_secret_stdin_flag_selects_the_cluster_target() {
        let flags = ConfigFlags {
            cluster_secret_stdin: true,
            ..ConfigFlags::default()
        };
        assert_eq!(flags.stdin_target().unwrap(), Some(StdinTarget::Cluster));
    }

    #[test]
    fn raft_secret_file_flag_sets_the_token() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raft-token");
        fs::write(&path, "file-token\n").unwrap();
        let flags = ConfigFlags {
            consensus_raft_secret_file: Some(path),
            ..ConfigFlags::default()
        };
        let layer = flags.to_layer(None).unwrap();
        assert_eq!(layer["consensus"]["raft"]["secret"], "file-token");
    }

    #[test]
    fn consul_secret_file_flags_set_both_tokens() {
        let dir = tempdir().unwrap();
        let token_path = dir.path().join("consul-token");
        let secret_path = dir.path().join("consul-secret");
        fs::write(&token_path, "acl-token\n").unwrap();
        fs::write(&secret_path, "proposal-token\n").unwrap();
        let flags = ConfigFlags {
            consensus_consul_token_file: Some(token_path),
            consensus_consul_secret_file: Some(secret_path),
            ..ConfigFlags::default()
        };
        let layer = flags.to_layer(None).unwrap();
        assert_eq!(layer["consensus"]["consul"]["token"], "acl-token");
        assert_eq!(layer["consensus"]["consul"]["secret"], "proposal-token");
    }
}
