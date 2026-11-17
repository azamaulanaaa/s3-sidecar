//! Layered configuration loading and validation.

pub mod duration;
pub mod loader;
pub mod secret;
pub mod show;
pub mod size;

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use self::duration::HumanDuration;
use self::loader::SourceMap;
use self::secret::SecretString;
use self::size::Size;

/// Smallest part size that S3 accepts, except for the last part.
pub const S3_MIN_PART_BYTES: u64 = 5 * 1_024 * 1_024;
/// Largest part size that S3 accepts.
pub const S3_MAX_PART_BYTES: u64 = 5 * 1_024 * 1_024 * 1_024;
/// Largest number of parts that one S3 multipart upload accepts.
pub const S3_MAX_PARTS: u32 = 10_000;

/// Run mode for the sidecar.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Read and write through a cache S3 endpoint.
    #[default]
    Cache,
    /// Join a sidecar cluster.
    Cluster,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cache => f.write_str("cache"),
            Self::Cluster => f.write_str("cluster"),
        }
    }
}

/// Cache behavior for reads and writes.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheMode {
    /// Cache reads only.
    Read,
    /// Buffer writes only.
    Write,
    /// Cache reads and buffer writes.
    #[default]
    ReadWrite,
}

impl fmt::Display for CacheMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read => f.write_str("read"),
            Self::Write => f.write_str("write"),
            Self::ReadWrite => f.write_str("read_write"),
        }
    }
}

/// Cache write policy.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WritePolicy {
    /// Acknowledge after the cache write. Sync to origin later.
    #[default]
    WriteBack,
    /// Write to origin before the acknowledgement.
    WriteThrough,
}

impl fmt::Display for WritePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteBack => f.write_str("write_back"),
            Self::WriteThrough => f.write_str("write_through"),
        }
    }
}

/// Action when the cache is not available.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnCacheError {
    /// Send the request to origin.
    #[default]
    Origin,
    /// Return an error.
    Fail,
}

impl fmt::Display for OnCacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Origin => f.write_str("origin"),
            Self::Fail => f.write_str("fail"),
        }
    }
}

/// Coordination backend for cluster mode.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusBackend {
    /// Use one local node with no network agreement.
    #[default]
    Single,
    /// Use the built-in raft implementation.
    Raft,
    /// Use Consul for coordination.
    Consul,
}

impl fmt::Display for ConsensusBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single => f.write_str("single"),
            Self::Raft => f.write_str("raft"),
            Self::Consul => f.write_str("consul"),
        }
    }
}

/// Write acknowledgement level for a cluster bucket.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteAck {
    /// Acknowledge after the leader commits.
    Leader,
    /// Acknowledge after a quorum of replicas.
    Quorum,
    /// Acknowledge after all replicas.
    All,
}

impl fmt::Display for WriteAck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leader => f.write_str("leader"),
            Self::Quorum => f.write_str("quorum"),
            Self::All => f.write_str("all"),
        }
    }
}

/// Read routing mode for a cluster bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    /// Read locally.
    Local,
    /// Read locally, then ask a peer.
    LocalThenPeer,
    /// Read through the leader.
    Leader,
}

impl fmt::Display for ReadMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local => f.write_str("local"),
            Self::LocalThenPeer => f.write_str("local_then_peer"),
            Self::Leader => f.write_str("leader"),
        }
    }
}

/// OTLP transport protocol for signal export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum OtelProtocol {
    /// gRPC over HTTP/2.
    #[serde(rename = "grpc")]
    Grpc,
    /// Protobuf over HTTP.
    #[serde(rename = "http/protobuf")]
    HttpProtobuf,
}

impl fmt::Display for OtelProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Grpc => f.write_str("grpc"),
            Self::HttpProtobuf => f.write_str("http/protobuf"),
        }
    }
}

impl OtelProtocol {
    /// Parse a protocol name. The name must be `grpc` or `http/protobuf`.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "grpc" => Some(Self::Grpc),
            "http/protobuf" => Some(Self::HttpProtobuf),
            _ => None,
        }
    }
}

/// OpenTelemetry export settings for metrics, traces, and logs.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct OtelConfig {
    /// Master switch for all OpenTelemetry export. The default is off.
    pub enabled: bool,
    /// Export metrics over OTLP. The default is on.
    pub metrics: bool,
    /// Export traces over OTLP. The default is off.
    pub traces: bool,
    /// Export logs over OTLP. The default is off.
    pub logs: bool,
    /// Name of this service. An empty value uses `OTEL_SERVICE_NAME`, then
    /// `s3-sidecar`.
    pub service_name: Option<String>,
    /// Version of this service.
    pub service_version: String,
    /// OTLP collector URL for all signals. An empty value uses
    /// `OTEL_EXPORTER_OTLP_ENDPOINT`.
    pub endpoint: Option<String>,
    /// OTLP collector URL for traces. An empty value uses `endpoint`, then
    /// `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`.
    pub trace_endpoint: Option<String>,
    /// OTLP collector URL for logs. An empty value uses `endpoint`, then
    /// `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`.
    pub logs_endpoint: Option<String>,
    /// Fraction of root traces to sample. The value is in `0.0..=1.0`.
    pub trace_sample_ratio: f64,
    /// OTLP transport. An empty value uses `OTEL_EXPORTER_OTLP_PROTOCOL`, then
    /// `grpc`.
    pub protocol: Option<OtelProtocol>,
    /// Extra OTLP request headers. An empty map uses
    /// `OTEL_EXPORTER_OTLP_HEADERS`.
    pub headers: BTreeMap<String, SecretString>,
    /// File that contains a JSON object of OTLP request headers.
    pub headers_file: Option<PathBuf>,
    /// Timeout for one OTLP export request.
    pub timeout: HumanDuration,
    /// Interval between metric exports. An empty value uses
    /// `OTEL_METRIC_EXPORT_INTERVAL`, then `60s`.
    pub export_interval: Option<HumanDuration>,
    /// Delay between trace exports.
    pub trace_schedule_delay: HumanDuration,
    /// Delay between log exports.
    pub log_schedule_delay: HumanDuration,
    /// Bound for the final flush during shutdown.
    pub export_timeout: HumanDuration,
    /// Extra resource attributes. Each value is sent as a string.
    pub resource_attributes: BTreeMap<String, String>,
    /// Add the `s3.object.key` span attribute. The default is off.
    pub record_object_keys: bool,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            metrics: true,
            traces: false,
            logs: false,
            service_name: None,
            service_version: env!("CARGO_PKG_VERSION").to_owned(),
            endpoint: None,
            trace_endpoint: None,
            logs_endpoint: None,
            trace_sample_ratio: 1.0,
            protocol: None,
            headers: BTreeMap::new(),
            headers_file: None,
            timeout: HumanDuration::new(Duration::from_secs(10)),
            export_interval: None,
            trace_schedule_delay: HumanDuration::new(Duration::from_secs(5)),
            log_schedule_delay: HumanDuration::new(Duration::from_secs(5)),
            export_timeout: HumanDuration::new(Duration::from_secs(10)),
            resource_attributes: BTreeMap::new(),
            record_object_keys: false,
        }
    }
}

/// TLS listener settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TlsConfig {
    /// PEM certificate chain file.
    pub cert_file: String,
    /// PEM private key file.
    pub key_file: String,
}

/// HTTP listener settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Listen address, for example `127.0.0.1:9000`.
    pub bind: String,
    /// Optional TLS settings. Plain HTTP is used when the section is absent.
    pub tls: Option<TlsConfig>,
    /// Maximum request body size in bytes.
    pub max_body_bytes: u64,
    /// Timeout for a non-streaming control request.
    pub request_timeout: HumanDuration,
    /// Idle timeout between stream reads.
    pub idle_timeout: HumanDuration,
    /// Maximum number of concurrent S3 requests.
    pub max_connections: usize,
    /// Bound for draining in-flight requests during shutdown.
    pub shutdown_timeout: HumanDuration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:9000".to_owned(),
            tls: None,
            max_body_bytes: 5_368_709_120,
            request_timeout: HumanDuration::new(Duration::from_mins(5)),
            idle_timeout: HumanDuration::new(Duration::from_secs(60)),
            max_connections: 1024,
            shutdown_timeout: HumanDuration::new(Duration::from_secs(30)),
        }
    }
}

/// Upstream S3 connection settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct UpstreamConfig {
    /// Timeout for the upstream connection phase.
    pub connect_timeout: HumanDuration,
    /// Timeout for upstream response headers on a control request.
    pub request_timeout: HumanDuration,
    /// Number of retries after the first attempt for an idempotent request.
    pub retry_attempts: u32,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            connect_timeout: HumanDuration::new(Duration::from_secs(10)),
            request_timeout: HumanDuration::new(Duration::from_mins(5)),
            retry_attempts: 2,
        }
    }
}

/// Multipart upload settings for sidecar-owned object writes.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MultipartConfig {
    /// Use multipart for internal object writes when the size permits.
    pub enabled: bool,
    /// Smallest object size that uses multipart. `0` always uses multipart.
    pub threshold: Size,
    /// Target size of one part.
    pub chunk_size: Size,
    /// Smallest allowed part size, except for the last part.
    pub min_chunk_size: Size,
    /// Largest number of parts in one upload.
    pub max_parts: u32,
    /// Largest number of part uploads in flight.
    pub concurrency: usize,
}

impl Default for MultipartConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: Size::new(64 * 1_048_576),
            chunk_size: Size::new(16 * 1_048_576),
            min_chunk_size: Size::new(S3_MIN_PART_BYTES),
            max_parts: S3_MAX_PARTS,
            concurrency: 4,
        }
    }
}

/// Connection settings for one S3 endpoint.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EndpointConfig {
    /// Base URL of the S3 endpoint.
    pub endpoint: String,
    /// Bucket for cluster backing data. Ignored for the origin endpoint.
    pub bucket: String,
    /// `SigV4` region.
    pub region: String,
    /// Access key ID.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: SecretString,
    /// Optional session token.
    pub session_token: Option<SecretString>,
    /// Use path-style bucket addresses.
    pub force_path_style: bool,
}

impl EndpointConfig {
    /// Return true when the endpoint has a URL.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        !self.endpoint.trim().is_empty()
    }

    /// Return the region, or the default region.
    #[must_use]
    pub fn region_or_default(&self) -> &str {
        if self.region.is_empty() {
            "us-east-1"
        } else {
            &self.region
        }
    }
}

/// Cache settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CacheConfig {
    /// Cache S3 endpoint URL.
    pub endpoint: String,
    /// Cache `SigV4` region.
    pub region: String,
    /// Cache access key ID.
    pub access_key_id: String,
    /// Cache secret access key.
    pub secret_access_key: SecretString,
    /// Optional cache session token.
    pub session_token: Option<SecretString>,
    /// Use path-style bucket addresses for the cache.
    pub force_path_style: bool,
    /// Cache S3 bucket. All blobs, metadata, and journal entries live here.
    pub bucket: String,
    /// Cache namespace. It is part of the cache key and journal path.
    pub prefix: String,
    /// Cache behavior.
    pub mode: CacheMode,
    /// Maximum cache size in bytes. `0` means unlimited.
    pub max_bytes: Option<u64>,
    /// Maximum object size that can enter the cache.
    pub max_object_bytes: u64,
    /// Cache write policy.
    pub write_policy: WritePolicy,
    /// Fallback when the cache is not available.
    pub on_cache_error: OnCacheError,
    /// Eviction high watermark, as a fraction.
    pub high_watermark: f64,
    /// Eviction low watermark, as a fraction.
    pub low_watermark: f64,
    /// Age at which a blob with no metadata is an orphan.
    pub staging_ttl: HumanDuration,
    /// Age at which a stale journal entry is removed.
    pub orphan_ttl: HumanDuration,
    /// Number of concurrent key syncs.
    pub sync_workers: usize,
    /// Maximum sync attempts before quarantine.
    pub sync_max_attempts: u32,
    /// Base delay for sync retries, in milliseconds.
    pub sync_retry_base_ms: u64,
    /// Delay before a key sync starts, in milliseconds. This lets writes
    /// coalesce and lets a delete cancel a queued put.
    pub sync_debounce_ms: u64,
    /// Interval between garbage collection passes.
    pub gc_interval: HumanDuration,
    /// Path of the `redb` LRU index. Used when `max_bytes` is not 0.
    pub index_path: String,
    /// Serve a quarantined entry instead of a 404.
    pub serve_quarantined: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            region: "us-east-1".to_owned(),
            access_key_id: String::new(),
            secret_access_key: SecretString::default(),
            session_token: None,
            force_path_style: false,
            bucket: String::new(),
            prefix: "cache".to_owned(),
            mode: CacheMode::ReadWrite,
            max_bytes: None,
            max_object_bytes: 1_073_741_824,
            write_policy: WritePolicy::default(),
            on_cache_error: OnCacheError::default(),
            high_watermark: 0.9,
            low_watermark: 0.8,
            staging_ttl: HumanDuration::new(Duration::from_secs(3600)),
            orphan_ttl: HumanDuration::new(Duration::from_hours(24)),
            sync_workers: 4,
            sync_max_attempts: 5,
            sync_retry_base_ms: 200,
            sync_debounce_ms: 100,
            gc_interval: HumanDuration::new(Duration::from_secs(60)),
            index_path: "s3-sidecar-cache-index.redb".to_owned(),
            serve_quarantined: false,
        }
    }
}

/// Address of one raft cluster member.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RaftNodeConfig {
    /// Node name.
    pub id: String,
    /// Base URL of the node, for example `http://10.0.0.2:9000`.
    pub addr: String,
}

/// Raft settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RaftConfig {
    /// Node name.
    pub node_id: String,
    /// Cluster member names for the single backend. The list includes this node.
    pub peers: Vec<String>,
    /// Start the cluster from this node with the configured nodes.
    pub bootstrap: bool,
    /// Shared token for internal raft requests.
    pub secret: SecretString,
    /// Timeout for one internal raft request.
    pub rpc_timeout: HumanDuration,
    /// Addresses of the raft cluster members. The list includes this node.
    pub nodes: Vec<RaftNodeConfig>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            peers: Vec::new(),
            bootstrap: false,
            secret: SecretString::default(),
            rpc_timeout: HumanDuration::new(Duration::from_secs(5)),
            nodes: Vec::new(),
        }
    }
}

impl RaftConfig {
    /// Return the address of one member.
    #[must_use]
    pub fn node_addr(&self, id: &str) -> Option<&str> {
        self.nodes
            .iter()
            .find(|node| node.id == id)
            .map(|node| node.addr.as_str())
    }
}

/// Consul settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConsulConfig {
    /// Node name. It identifies this sidecar in the leader record.
    pub node_id: String,
    /// Base URL of the Consul HTTP API, for example `http://127.0.0.1:8500`.
    pub address: String,
    /// Base URL that peers use to reach this sidecar.
    pub advertise_addr: String,
    /// Optional Consul ACL token.
    pub token: SecretString,
    /// Consul session time to live.
    pub session_ttl: HumanDuration,
    /// Shared token for internal proposal requests.
    pub secret: SecretString,
}

impl Default for ConsulConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            address: String::new(),
            advertise_addr: String::new(),
            token: SecretString::default(),
            session_ttl: HumanDuration::new(Duration::from_secs(15)),
            secret: SecretString::default(),
        }
    }
}

/// Coordination settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConsensusConfig {
    /// Coordination backend.
    pub backend: ConsensusBackend,
    /// Raft settings.
    pub raft: RaftConfig,
    /// Consul settings.
    pub consul: ConsulConfig,
}

/// One sidecar node in the cluster directory.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterNodeConfig {
    /// Node name.
    pub id: String,
    /// Base URL of the sidecar listener, for example `http://10.0.0.2:9000`.
    pub addr: String,
}

/// Garbage collection, replication, and metadata settings for cluster mode.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClusterConfig {
    /// Age at which a staged blob with no commit is an orphan.
    pub staging_ttl: HumanDuration,
    /// Age at which a tombstone and its blob are removed.
    pub orphan_ttl: HumanDuration,
    /// Interval between garbage collection passes.
    pub gc_interval: HumanDuration,
    /// Path of the `redb` metadata file.
    pub metadata_path: String,
    /// Shared token for internal blob transport requests.
    pub secret: SecretString,
    /// Store a peer blob locally while serving it.
    pub read_repair: bool,
    /// Number of concurrent replication workers.
    pub replicate_workers: usize,
    /// Maximum time to wait for one synchronous peer blob copy.
    pub replicate_timeout: HumanDuration,
    /// Interval between replica reconciliation passes.
    pub reconcile_interval: HumanDuration,
    /// Every sidecar node and its listener address.
    pub nodes: Vec<ClusterNodeConfig>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            staging_ttl: HumanDuration::new(Duration::from_hours(1)),
            orphan_ttl: HumanDuration::new(Duration::from_hours(24)),
            gc_interval: HumanDuration::new(Duration::from_secs(60)),
            metadata_path: "./s3-sidecar-meta.redb".to_owned(),
            secret: SecretString::default(),
            read_repair: true,
            replicate_workers: 4,
            replicate_timeout: HumanDuration::new(Duration::from_secs(10)),
            reconcile_interval: HumanDuration::new(Duration::from_mins(5)),
            nodes: Vec::new(),
        }
    }
}

impl ClusterConfig {
    /// Return the address of one node.
    #[must_use]
    pub fn node_addr(&self, id: &str) -> Option<&str> {
        self.nodes
            .iter()
            .find(|node| node.id == id)
            .map(|node| node.addr.as_str())
    }
}

/// One client credential and its bucket allow-list.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    /// Access key ID that the client sends.
    pub access_key_id: String,
    /// Secret access key that the client uses to sign.
    pub secret_access_key: SecretString,
    /// Optional session token.
    #[serde(default)]
    pub session_token: Option<SecretString>,
    /// Buckets that this client may use. An empty list allows no bucket.
    #[serde(default)]
    pub buckets: Vec<String>,
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &self.secret_access_key)
            .field("session_token", &self.session_token)
            .field("buckets", &self.buckets)
            .finish()
    }
}

/// Per-bucket settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BucketConfig {
    /// Bucket name.
    pub name: String,
    /// Cache time to live.
    pub ttl: Option<HumanDuration>,
    /// Cache behavior for this bucket.
    pub cache_mode: Option<CacheMode>,
    /// Cache write policy for this bucket.
    pub cache_write_policy: Option<WritePolicy>,
    /// Write acknowledgement level for cluster mode.
    pub write_ack: Option<WriteAck>,
    /// Read routing mode for cluster mode.
    pub read: Option<ReadMode>,
    /// Replica count for cluster mode.
    pub replicas: Option<u32>,
    /// Minimum replica count for cluster mode.
    pub min_replicas: Option<u32>,
}

/// Merged configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Run mode.
    pub mode: Mode,
    /// Listener settings.
    pub server: ServerConfig,
    /// Upstream connection and retry settings.
    pub upstream: UpstreamConfig,
    /// Multipart settings for internal object writes.
    pub multipart: MultipartConfig,
    /// Origin S3 endpoint.
    pub origin: EndpointConfig,
    /// Cache settings.
    pub cache: CacheConfig,
    /// Backing S3 endpoint for cluster mode.
    pub backing: EndpointConfig,
    /// Coordination settings.
    pub consensus: ConsensusConfig,
    /// Cluster garbage collection and metadata settings.
    pub cluster: ClusterConfig,
    /// OpenTelemetry export settings for metrics, traces, and logs.
    pub otel: OtelConfig,
    /// Client credentials.
    pub clients: Vec<ClientConfig>,
    /// Per-bucket settings.
    pub buckets: Vec<BucketConfig>,
}

/// A problem found while loading or validating the configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The configuration file could not be read.
    #[error("cannot read config file {path}: {source}")]
    ReadFile {
        /// File path.
        path: std::path::PathBuf,
        /// I/O error.
        source: std::io::Error,
    },
    /// The configuration file is not valid TOML.
    #[error("cannot parse config file {path}: {source}")]
    ParseFile {
        /// File path.
        path: std::path::PathBuf,
        /// Parse error.
        source: toml::de::Error,
    },
    /// A secret file could not be read.
    #[error("cannot read secret file {path}: {source}")]
    ReadSecret {
        /// File path.
        path: std::path::PathBuf,
        /// I/O error.
        source: std::io::Error,
    },
    /// The OTLP headers file could not be read.
    #[error("cannot read OTLP headers file {path}: {source}")]
    ReadHeaders {
        /// File path.
        path: std::path::PathBuf,
        /// I/O error.
        source: std::io::Error,
    },
    /// Standard input could not be read.
    #[error("cannot read the secret from stdin: {0}")]
    ReadStdin(std::io::Error),
    /// The configuration is not valid.
    #[error("invalid config: {0}")]
    Invalid(String),
    /// An environment variable is not known.
    #[error("unknown environment variable: {0}. Remove it or correct the name.")]
    UnknownEnv(String),
    /// A value has the wrong type or format.
    #[error("invalid value for {name}: {value}")]
    InvalidValue {
        /// Field name.
        name: String,
        /// Rejected value.
        value: String,
    },
    /// The merged value could not be deserialized.
    #[error("invalid config: {0}")]
    Deserialize(String),
    /// A flags value is not valid JSON.
    #[error("invalid JSON for {name}: {source}")]
    FlagsJson {
        /// Flag name.
        name: String,
        /// Parse error.
        source: serde_json::Error,
    },
    /// A JSON value has the wrong type.
    #[error("{name} must be a JSON {expected}")]
    FlagsType {
        /// Flag name.
        name: String,
        /// Expected JSON type.
        expected: String,
    },
}

impl ConfigError {
    /// Build an invalid-config error from a message.
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}

/// Validate the merged configuration.
///
/// Returns warnings for settings that are accepted but have no effect.
///
/// # Errors
///
/// Returns an error when a setting violates a rule.
pub fn validate(config: &Config) -> Result<Vec<String>, ConfigError> {
    let mut warnings = Vec::new();
    validate_server(config)?;
    validate_multipart(config)?;
    validate_cache(config, &mut warnings)?;
    validate_endpoints(config)?;
    validate_cluster(config)?;
    validate_otel(config)?;
    validate_buckets(config)?;
    validate_clients(config)?;
    Ok(warnings)
}

fn validate_otel(config: &Config) -> Result<(), ConfigError> {
    let otel = &config.otel;
    for (name, endpoint) in [
        ("otel.endpoint", otel.endpoint.as_deref()),
        ("otel.trace_endpoint", otel.trace_endpoint.as_deref()),
        ("otel.logs_endpoint", otel.logs_endpoint.as_deref()),
    ] {
        if let Some(endpoint) = endpoint
            && !endpoint.trim().is_empty()
            && !is_http_url(endpoint)
        {
            return Err(ConfigError::invalid(format!(
                "{name} must be an absolute http:// or https:// URL"
            )));
        }
    }
    for (name, duration) in [
        ("otel.timeout", otel.timeout),
        ("otel.trace_schedule_delay", otel.trace_schedule_delay),
        ("otel.log_schedule_delay", otel.log_schedule_delay),
        ("otel.export_timeout", otel.export_timeout),
    ] {
        if duration.as_duration() == Duration::ZERO {
            return Err(ConfigError::invalid(format!(
                "{name} must be greater than 0"
            )));
        }
    }
    if !(0.0..=1.0).contains(&otel.trace_sample_ratio) {
        return Err(ConfigError::invalid(
            "otel.trace_sample_ratio must be between 0.0 and 1.0",
        ));
    }
    if let Some(interval) = otel.export_interval
        && interval.as_duration() == Duration::ZERO
    {
        return Err(ConfigError::invalid(
            "otel.export_interval must be greater than 0",
        ));
    }
    for name in otel.headers.keys() {
        if name.trim().is_empty() {
            return Err(ConfigError::invalid("otel header name must not be empty"));
        }
        if name != &name.to_ascii_lowercase() {
            return Err(ConfigError::invalid(format!(
                "otel header name \"{name}\" must be lowercase"
            )));
        }
        if name.ends_with("-bin") {
            return Err(ConfigError::invalid(format!(
                "otel header name \"{name}\" must not end with -bin"
            )));
        }
        if axum::http::HeaderName::from_bytes(name.as_bytes()).is_err() {
            return Err(ConfigError::invalid(format!(
                "otel header name \"{name}\" is not valid"
            )));
        }
    }
    for key in otel.resource_attributes.keys() {
        if key.trim().is_empty() {
            return Err(ConfigError::invalid(
                "otel.resource_attributes key must not be empty",
            ));
        }
    }
    Ok(())
}

fn validate_server(config: &Config) -> Result<(), ConfigError> {
    let server = &config.server;
    if server.max_body_bytes == 0 {
        return Err(ConfigError::invalid(
            "server.max_body_bytes must be greater than 0",
        ));
    }
    if server.max_connections == 0 {
        return Err(ConfigError::invalid(
            "server.max_connections must be greater than 0",
        ));
    }
    for (name, duration) in [
        ("server.request_timeout", server.request_timeout),
        ("server.idle_timeout", server.idle_timeout),
        ("server.shutdown_timeout", server.shutdown_timeout),
        ("upstream.connect_timeout", config.upstream.connect_timeout),
        ("upstream.request_timeout", config.upstream.request_timeout),
    ] {
        if duration.as_duration() == Duration::ZERO {
            return Err(ConfigError::invalid(format!(
                "{name} must be greater than 0"
            )));
        }
    }
    if let Some(tls) = &server.tls {
        if tls.cert_file.trim().is_empty() {
            return Err(ConfigError::invalid(
                "server.tls.cert_file must not be empty",
            ));
        }
        if tls.key_file.trim().is_empty() {
            return Err(ConfigError::invalid(
                "server.tls.key_file must not be empty",
            ));
        }
    }
    Ok(())
}

fn validate_multipart(config: &Config) -> Result<(), ConfigError> {
    let multipart = &config.multipart;
    if multipart.min_chunk_size.as_bytes() < S3_MIN_PART_BYTES {
        return Err(ConfigError::invalid(
            "multipart.min_chunk_size must be at least 5MiB, the S3 minimum for a part",
        ));
    }
    if multipart.min_chunk_size.as_bytes() > S3_MAX_PART_BYTES {
        return Err(ConfigError::invalid(
            "multipart.min_chunk_size must not be greater than 5GiB, the S3 maximum for a part",
        ));
    }
    if multipart.chunk_size < multipart.min_chunk_size {
        return Err(ConfigError::invalid(
            "multipart.chunk_size must not be less than multipart.min_chunk_size",
        ));
    }
    if multipart.chunk_size.as_bytes() > S3_MAX_PART_BYTES {
        return Err(ConfigError::invalid(
            "multipart.chunk_size must not be greater than 5GiB, the S3 maximum for a part",
        ));
    }
    if !(1..=S3_MAX_PARTS).contains(&multipart.max_parts) {
        return Err(ConfigError::invalid(
            "multipart.max_parts must be between 1 and 10000",
        ));
    }
    if !(1..=64).contains(&multipart.concurrency) {
        return Err(ConfigError::invalid(
            "multipart.concurrency must be between 1 and 64",
        ));
    }
    Ok(())
}

fn validate_cache(config: &Config, warnings: &mut Vec<String>) -> Result<(), ConfigError> {
    if config.mode == Mode::Cluster {
        return Ok(());
    }
    if config.cache.max_object_bytes == 0 {
        return Err(ConfigError::invalid(
            "cache.max_object_bytes must be greater than 0",
        ));
    }
    let Some(max_bytes) = config.cache.max_bytes else {
        return Err(ConfigError::invalid(
            "cache.max_bytes is required. Set cache.max_bytes to a byte count, or 0 for unlimited.",
        ));
    };
    if max_bytes > 0 {
        if !(0.0..=1.0).contains(&config.cache.high_watermark) {
            return Err(ConfigError::invalid(
                "cache.high_watermark must be between 0.0 and 1.0",
            ));
        }
        if !(0.0..=1.0).contains(&config.cache.low_watermark) {
            return Err(ConfigError::invalid(
                "cache.low_watermark must be between 0.0 and 1.0",
            ));
        }
        if config.cache.low_watermark >= config.cache.high_watermark {
            return Err(ConfigError::invalid(
                "cache.low_watermark must be less than cache.high_watermark",
            ));
        }
    } else if (config.cache.high_watermark - 0.9).abs() > f64::EPSILON
        || (config.cache.low_watermark - 0.8).abs() > f64::EPSILON
    {
        warnings.push(
            "cache.high_watermark and cache.low_watermark are ignored when cache.max_bytes is 0"
                .to_owned(),
        );
    }
    if config.cache.endpoint.trim().is_empty() {
        return Err(ConfigError::invalid("cache.endpoint is required"));
    }
    if config.cache.bucket.trim().is_empty() {
        return Err(ConfigError::invalid("cache.bucket is required"));
    }
    if config.cache.prefix.trim().is_empty() {
        return Err(ConfigError::invalid("cache.prefix must not be empty"));
    }
    if config.cache.sync_workers == 0 {
        return Err(ConfigError::invalid(
            "cache.sync_workers must be greater than 0",
        ));
    }
    if config.cache.sync_max_attempts == 0 {
        return Err(ConfigError::invalid(
            "cache.sync_max_attempts must be greater than 0",
        ));
    }
    if config.cache.staging_ttl.as_duration() == Duration::ZERO {
        return Err(ConfigError::invalid(
            "cache.staging_ttl must be greater than 0",
        ));
    }
    if config.cache.orphan_ttl.as_duration() == Duration::ZERO {
        return Err(ConfigError::invalid(
            "cache.orphan_ttl must be greater than 0",
        ));
    }
    if config.cache.gc_interval.as_duration() == Duration::ZERO {
        return Err(ConfigError::invalid(
            "cache.gc_interval must be greater than 0",
        ));
    }
    if config.cache.index_path.trim().is_empty() {
        return Err(ConfigError::invalid("cache.index_path must not be empty"));
    }
    Ok(())
}

fn validate_endpoints(config: &Config) -> Result<(), ConfigError> {
    if !config.origin.endpoint.trim().is_empty() && !is_http_url(&config.origin.endpoint) {
        return Err(ConfigError::invalid(
            "origin.endpoint must start with http:// or https://",
        ));
    }
    if !config.cache.endpoint.trim().is_empty() && !is_http_url(&config.cache.endpoint) {
        return Err(ConfigError::invalid(
            "cache.endpoint must start with http:// or https://",
        ));
    }
    if !config.backing.endpoint.trim().is_empty() && !is_http_url(&config.backing.endpoint) {
        return Err(ConfigError::invalid(
            "backing.endpoint must start with http:// or https://",
        ));
    }
    Ok(())
}

fn validate_cluster(config: &Config) -> Result<(), ConfigError> {
    if config.mode != Mode::Cluster {
        return Ok(());
    }
    match config.consensus.backend {
        ConsensusBackend::Single => {
            require_raft_node_id(config)?;
            validate_single_backend(config)?;
        }
        ConsensusBackend::Raft => {
            require_raft_node_id(config)?;
            validate_raft_backend(config)?;
        }
        ConsensusBackend::Consul => validate_consul_backend(config)?,
    }
    if config.backing.endpoint.trim().is_empty() {
        return Err(ConfigError::invalid(
            "backing.endpoint is required in cluster mode",
        ));
    }
    if config.backing.bucket.trim().is_empty() {
        return Err(ConfigError::invalid(
            "backing.bucket is required in cluster mode",
        ));
    }
    if config.cluster.metadata_path.trim().is_empty() {
        return Err(ConfigError::invalid(
            "cluster.metadata_path must not be empty in cluster mode",
        ));
    }
    if config.cluster.staging_ttl.as_duration() == Duration::ZERO {
        return Err(ConfigError::invalid(
            "cluster.staging_ttl must be greater than 0",
        ));
    }
    if config.cluster.orphan_ttl.as_duration() == Duration::ZERO {
        return Err(ConfigError::invalid(
            "cluster.orphan_ttl must be greater than 0",
        ));
    }
    if config.cluster.gc_interval.as_duration() == Duration::ZERO {
        return Err(ConfigError::invalid(
            "cluster.gc_interval must be greater than 0",
        ));
    }
    if config.cluster.reconcile_interval.as_duration() == Duration::ZERO {
        return Err(ConfigError::invalid(
            "cluster.reconcile_interval must be greater than 0",
        ));
    }
    if config.cluster.replicate_workers == 0 {
        return Err(ConfigError::invalid(
            "cluster.replicate_workers must be greater than 0",
        ));
    }
    if config.cluster.replicate_timeout.as_duration() == Duration::ZERO {
        return Err(ConfigError::invalid(
            "cluster.replicate_timeout must be greater than 0",
        ));
    }
    validate_cluster_nodes(config)?;
    validate_bucket_replication(config)?;
    Ok(())
}

fn validate_cluster_nodes(config: &Config) -> Result<(), ConfigError> {
    let replication = config
        .buckets
        .iter()
        .any(|bucket| bucket.replicas.unwrap_or(1) > 1);
    if replication {
        if config.cluster.nodes.is_empty() {
            return Err(ConfigError::invalid(
                "cluster.nodes is required when a bucket replicas value is greater than 1",
            ));
        }
        if config.cluster.secret.is_empty() {
            return Err(ConfigError::invalid(
                "cluster.secret is required when replication is enabled",
            ));
        }
    }
    let mut ids = HashSet::new();
    for node in &config.cluster.nodes {
        if node.id.trim().is_empty() {
            return Err(ConfigError::invalid(
                "each cluster.nodes entry requires an id",
            ));
        }
        if !ids.insert(node.id.clone()) {
            return Err(ConfigError::invalid(format!(
                "cluster node \"{}\" is configured more than one time",
                node.id
            )));
        }
        if node.addr.trim().is_empty() {
            return Err(ConfigError::invalid(format!(
                "cluster node \"{}\" requires an addr",
                node.id
            )));
        }
        if !is_http_url(&node.addr) {
            return Err(ConfigError::invalid(format!(
                "cluster node \"{}\" addr must start with http:// or https://",
                node.id
            )));
        }
    }
    if !config.cluster.nodes.is_empty() {
        let node_id = match config.consensus.backend {
            ConsensusBackend::Consul => config.consensus.consul.node_id.as_str(),
            ConsensusBackend::Raft | ConsensusBackend::Single => {
                config.consensus.raft.node_id.as_str()
            }
        };
        if !ids.contains(node_id) {
            return Err(ConfigError::invalid(
                "cluster.nodes must contain the local node id",
            ));
        }
    }
    Ok(())
}

fn validate_bucket_replication(config: &Config) -> Result<(), ConfigError> {
    // A single-node cluster always has the local node. Use it when the
    // directory is empty.
    let node_count = config.cluster.nodes.len().max(1);
    for bucket in &config.buckets {
        let replicas = bucket.replicas.unwrap_or(1);
        let min_replicas = bucket.min_replicas.unwrap_or(1);
        if replicas == 0 {
            return Err(ConfigError::invalid(format!(
                "bucket \"{}\" replicas must be at least 1",
                bucket.name
            )));
        }
        if min_replicas == 0 {
            return Err(ConfigError::invalid(format!(
                "bucket \"{}\" min_replicas must be at least 1",
                bucket.name
            )));
        }
        if min_replicas > replicas {
            return Err(ConfigError::invalid(format!(
                "bucket \"{}\" min_replicas must not be greater than replicas",
                bucket.name
            )));
        }
        if u64::from(replicas) > u64::try_from(node_count).unwrap_or(u64::MAX) {
            return Err(ConfigError::invalid(format!(
                "bucket \"{}\" replicas must not be greater than cluster.nodes",
                bucket.name
            )));
        }
    }
    Ok(())
}

fn require_raft_node_id(config: &Config) -> Result<(), ConfigError> {
    if config.consensus.raft.node_id.trim().is_empty() {
        return Err(ConfigError::invalid(
            "consensus.raft.node_id is required in cluster mode",
        ));
    }
    Ok(())
}

fn validate_consul_backend(config: &Config) -> Result<(), ConfigError> {
    let consul = &config.consensus.consul;
    if consul.node_id.trim().is_empty() {
        return Err(ConfigError::invalid(
            "consensus.consul.node_id is required with the consul consensus backend",
        ));
    }
    if consul.address.trim().is_empty() {
        return Err(ConfigError::invalid(
            "consensus.consul.address is required with the consul consensus backend",
        ));
    }
    if !is_http_url(&consul.address) {
        return Err(ConfigError::invalid(
            "consensus.consul.address must start with http:// or https://",
        ));
    }
    if consul.advertise_addr.trim().is_empty() {
        return Err(ConfigError::invalid(
            "consensus.consul.advertise_addr is required with the consul consensus backend",
        ));
    }
    if !is_http_url(&consul.advertise_addr) {
        return Err(ConfigError::invalid(
            "consensus.consul.advertise_addr must start with http:// or https://",
        ));
    }
    if consul.secret.expose_secret().is_empty() {
        return Err(ConfigError::invalid(
            "consensus.consul.secret is required with the consul consensus backend",
        ));
    }
    if consul.session_ttl.as_duration() < Duration::from_secs(10) {
        return Err(ConfigError::invalid(
            "consensus.consul.session_ttl must be at least 10s, the Consul minimum",
        ));
    }
    Ok(())
}

fn validate_single_backend(config: &Config) -> Result<(), ConfigError> {
    let raft = &config.consensus.raft;
    if !raft.nodes.is_empty() {
        return Err(ConfigError::invalid(
            "consensus.raft.nodes is not valid with the single consensus backend; use peers",
        ));
    }
    if raft.peers.is_empty() {
        return Err(ConfigError::invalid(
            "consensus.raft.peers must not be empty in cluster mode",
        ));
    }
    if raft.peers.len() != 1 {
        return Err(ConfigError::invalid(
            "the single consensus backend requires exactly one peer",
        ));
    }
    if !raft.peers.iter().any(|peer| peer == &raft.node_id) {
        return Err(ConfigError::invalid(
            "consensus.raft.peers must contain consensus.raft.node_id",
        ));
    }
    Ok(())
}

fn validate_raft_backend(config: &Config) -> Result<(), ConfigError> {
    let raft = &config.consensus.raft;
    if !raft.peers.is_empty() {
        return Err(ConfigError::invalid(
            "consensus.raft.peers is not valid with the raft consensus backend; use nodes",
        ));
    }
    if raft.nodes.is_empty() {
        return Err(ConfigError::invalid(
            "consensus.raft.nodes must not be empty with the raft consensus backend",
        ));
    }
    if raft.secret.expose_secret().is_empty() {
        return Err(ConfigError::invalid(
            "consensus.raft.secret is required with the raft consensus backend",
        ));
    }
    let mut ids = HashSet::new();
    for node in &raft.nodes {
        if node.id.trim().is_empty() {
            return Err(ConfigError::invalid(
                "each consensus.raft.nodes entry requires an id",
            ));
        }
        if !ids.insert(node.id.clone()) {
            return Err(ConfigError::invalid(format!(
                "raft node \"{}\" is configured more than one time",
                node.id
            )));
        }
        if node.addr.trim().is_empty() {
            return Err(ConfigError::invalid(format!(
                "raft node \"{}\" requires an addr",
                node.id
            )));
        }
        if !is_http_url(&node.addr) {
            return Err(ConfigError::invalid(format!(
                "raft node \"{}\" addr must start with http:// or https://",
                node.id
            )));
        }
    }
    if !ids.contains(&raft.node_id) {
        return Err(ConfigError::invalid(
            "consensus.raft.nodes must contain consensus.raft.node_id",
        ));
    }
    Ok(())
}

fn validate_buckets(config: &Config) -> Result<(), ConfigError> {
    let mut bucket_names = HashSet::new();
    for bucket in &config.buckets {
        if bucket.name.trim().is_empty() {
            return Err(ConfigError::invalid("bucket name must not be empty"));
        }
        if !bucket_names.insert(bucket.name.clone()) {
            return Err(ConfigError::invalid(format!(
                "bucket \"{}\" is configured more than one time",
                bucket.name
            )));
        }
        if bucket
            .ttl
            .is_some_and(|ttl| ttl.as_duration() == Duration::ZERO)
        {
            return Err(ConfigError::invalid(format!(
                "bucket \"{}\" ttl must be greater than 0",
                bucket.name
            )));
        }
        if let Some(replicas) = bucket.replicas {
            if replicas == 0 {
                return Err(ConfigError::invalid(format!(
                    "bucket \"{}\" replicas must be greater than 0",
                    bucket.name
                )));
            }
            if let Some(min_replicas) = bucket.min_replicas
                && min_replicas > replicas
            {
                return Err(ConfigError::invalid(format!(
                    "bucket \"{}\" min_replicas must not be greater than replicas",
                    bucket.name
                )));
            }
        }
    }
    Ok(())
}

fn validate_clients(config: &Config) -> Result<(), ConfigError> {
    let mut access_keys = HashSet::new();
    for client in &config.clients {
        if client.access_key_id.trim().is_empty() {
            return Err(ConfigError::invalid(
                "client access_key_id must not be empty",
            ));
        }
        if client.secret_access_key.expose_secret().is_empty() {
            return Err(ConfigError::invalid(format!(
                "client \"{}\" secret_access_key must not be empty",
                client.access_key_id
            )));
        }
        if !access_keys.insert(client.access_key_id.clone()) {
            return Err(ConfigError::invalid(format!(
                "client access_key_id \"{}\" is configured more than one time",
                client.access_key_id
            )));
        }
        for bucket in &client.buckets {
            if bucket.trim().is_empty() {
                return Err(ConfigError::invalid(format!(
                    "client \"{}\" has an empty bucket name",
                    client.access_key_id
                )));
            }
        }
    }
    Ok(())
}

/// Format a configuration table with redacted secrets and field sources.
#[must_use]
pub fn show_table(config: &Config, sources: &SourceMap) -> String {
    show::render(config, sources)
}

fn is_http_url(value: &str) -> bool {
    reqwest::Url::parse(value)
        .is_ok_and(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::time::Duration;

    use super::duration::HumanDuration;
    use super::*;

    fn base() -> Config {
        let mut config = Config::default();
        config.cache.endpoint = "http://127.0.0.1:9002".to_owned();
        config.cache.bucket = "cache".to_owned();
        config.cache.max_bytes = Some(0);
        config
    }

    #[test]
    fn cache_mode_defaults_to_read_write() {
        assert_eq!(CacheMode::default(), CacheMode::ReadWrite);
    }

    #[test]
    fn config_enums_keep_their_display_text() {
        assert_eq!(Mode::Cache.to_string(), "cache");
        assert_eq!(Mode::Cluster.to_string(), "cluster");
        assert_eq!(CacheMode::Read.to_string(), "read");
        assert_eq!(CacheMode::Write.to_string(), "write");
        assert_eq!(CacheMode::ReadWrite.to_string(), "read_write");
        assert_eq!(WritePolicy::WriteBack.to_string(), "write_back");
        assert_eq!(WritePolicy::WriteThrough.to_string(), "write_through");
        assert_eq!(OnCacheError::Origin.to_string(), "origin");
        assert_eq!(OnCacheError::Fail.to_string(), "fail");
        assert_eq!(ConsensusBackend::Single.to_string(), "single");
        assert_eq!(ConsensusBackend::Raft.to_string(), "raft");
        assert_eq!(ConsensusBackend::Consul.to_string(), "consul");
        assert_eq!(WriteAck::Leader.to_string(), "leader");
        assert_eq!(WriteAck::Quorum.to_string(), "quorum");
        assert_eq!(WriteAck::All.to_string(), "all");
    }

    #[test]
    fn config_enums_keep_their_serialized_names() {
        assert_eq!(serde_json::to_string(&Mode::Cache).unwrap(), "\"cache\"");
        assert_eq!(
            serde_json::to_string(&Mode::Cluster).unwrap(),
            "\"cluster\""
        );
        assert_eq!(serde_json::to_string(&CacheMode::Read).unwrap(), "\"read\"");
        assert_eq!(
            serde_json::to_string(&CacheMode::Write).unwrap(),
            "\"write\""
        );
        assert_eq!(
            serde_json::to_string(&CacheMode::ReadWrite).unwrap(),
            "\"read_write\""
        );
        assert_eq!(
            serde_json::to_string(&WritePolicy::WriteBack).unwrap(),
            "\"write_back\""
        );
        assert_eq!(
            serde_json::to_string(&WritePolicy::WriteThrough).unwrap(),
            "\"write_through\""
        );
        assert_eq!(
            serde_json::to_string(&OnCacheError::Origin).unwrap(),
            "\"origin\""
        );
        assert_eq!(
            serde_json::to_string(&OnCacheError::Fail).unwrap(),
            "\"fail\""
        );
        assert_eq!(
            serde_json::to_string(&ConsensusBackend::Single).unwrap(),
            "\"single\""
        );
        assert_eq!(
            serde_json::to_string(&ConsensusBackend::Raft).unwrap(),
            "\"raft\""
        );
        assert_eq!(
            serde_json::to_string(&ConsensusBackend::Consul).unwrap(),
            "\"consul\""
        );
        assert_eq!(
            serde_json::to_string(&WriteAck::Leader).unwrap(),
            "\"leader\""
        );
        assert_eq!(
            serde_json::to_string(&WriteAck::Quorum).unwrap(),
            "\"quorum\""
        );
        assert_eq!(serde_json::to_string(&WriteAck::All).unwrap(), "\"all\"");
    }

    #[test]
    fn config_enums_keep_their_deserialized_values() {
        assert_eq!(
            serde_json::from_str::<Mode>("\"cache\"").unwrap(),
            Mode::Cache
        );
        assert_eq!(
            serde_json::from_str::<CacheMode>("\"read_write\"").unwrap(),
            CacheMode::ReadWrite
        );
        assert_eq!(
            serde_json::from_str::<WritePolicy>("\"write_through\"").unwrap(),
            WritePolicy::WriteThrough
        );
        assert_eq!(
            serde_json::from_str::<OnCacheError>("\"fail\"").unwrap(),
            OnCacheError::Fail
        );
        assert_eq!(
            serde_json::from_str::<ConsensusBackend>("\"consul\"").unwrap(),
            ConsensusBackend::Consul
        );
        assert_eq!(
            serde_json::from_str::<WriteAck>("\"quorum\"").unwrap(),
            WriteAck::Quorum
        );
    }

    #[test]
    fn cache_off_mode_fails_to_deserialize() {
        assert!(serde_json::from_str::<CacheConfig>(r#"{"mode":"off"}"#).is_err());
    }

    #[test]
    fn cache_node_id_fails_to_deserialize() {
        let error = serde_json::from_str::<CacheConfig>(r#"{"node_id":"node-a"}"#).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn cache_mode_requires_max_bytes() {
        let mut config = base();
        config.cache.mode = CacheMode::Read;
        config.cache.max_bytes = None;
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cache.max_bytes is required"));
    }

    #[test]
    fn unlimited_cache_accepts_zero() {
        let mut config = base();
        config.cache.mode = CacheMode::Read;
        config.cache.max_bytes = Some(0);
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn cache_mode_requires_a_cache_bucket() {
        let mut config = base();
        config.cache.mode = CacheMode::Read;
        config.cache.bucket.clear();
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cache.bucket is required"));
    }

    #[test]
    fn cache_mode_requires_an_endpoint() {
        let mut config = base();
        config.cache.endpoint.clear();
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cache.endpoint is required"));
    }

    #[test]
    fn cache_mode_rejects_an_empty_prefix() {
        let mut config = base();
        config.cache.prefix.clear();
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cache.prefix"));
    }

    #[test]
    fn watermarks_must_be_ordered() {
        let mut config = base();
        config.cache.mode = CacheMode::Read;
        config.cache.endpoint = "http://127.0.0.1:9002".to_owned();
        config.cache.bucket = "cache".to_owned();
        config.cache.max_bytes = Some(100);
        config.cache.low_watermark = 0.95;
        config.cache.high_watermark = 0.9;
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("low_watermark"));
    }

    #[test]
    fn server_limits_must_be_positive() {
        let mut config = base();
        config.server.max_connections = 0;
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("max_connections"));

        let mut config = base();
        config.server.request_timeout = HumanDuration::new(Duration::ZERO);
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("request_timeout"));

        let mut config = base();
        config.upstream.retry_attempts = 0;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn tls_requires_both_files() {
        let mut config = base();
        config.server.tls = Some(TlsConfig {
            cert_file: String::new(),
            key_file: "key.pem".to_owned(),
        });
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cert_file"));

        let mut config = base();
        config.server.tls = Some(TlsConfig {
            cert_file: "cert.pem".to_owned(),
            key_file: String::new(),
        });
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("key_file"));
    }

    #[test]
    fn max_object_bytes_must_be_positive() {
        let mut config = base();
        config.cache.max_object_bytes = 0;
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("max_object_bytes"));
    }

    #[test]
    fn bucket_ttl_must_be_positive() {
        let mut config = base();
        config.buckets.push(BucketConfig {
            name: "media".to_owned(),
            ttl: Some(HumanDuration::new(Duration::ZERO)),
            ..BucketConfig::default()
        });
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("ttl"));
    }

    #[test]
    fn cluster_mode_requires_peers() {
        let mut config = base();
        config.mode = Mode::Cluster;
        config.consensus.raft.node_id = "a".to_owned();
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("peers"));
    }

    fn cluster_base() -> Config {
        let mut config = base();
        config.mode = Mode::Cluster;
        config.consensus.backend = ConsensusBackend::Single;
        config.consensus.raft.node_id = "a".to_owned();
        config.consensus.raft.peers = vec!["a".to_owned()];
        config.backing.endpoint = "http://127.0.0.1:9002".to_owned();
        config.backing.bucket = "data".to_owned();
        config
    }

    #[test]
    fn cluster_requires_a_backing_bucket() {
        let mut config = cluster_base();
        config.backing.bucket.clear();
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("backing.bucket"));
    }

    fn raft_base() -> Config {
        let mut config = cluster_base();
        config.consensus.backend = ConsensusBackend::Raft;
        config.consensus.raft.peers.clear();
        config.consensus.raft.secret = secret::SecretString::new("token");
        config.consensus.raft.nodes = vec![
            RaftNodeConfig {
                id: "a".to_owned(),
                addr: "http://127.0.0.1:9000".to_owned(),
            },
            RaftNodeConfig {
                id: "b".to_owned(),
                addr: "http://127.0.0.1:9100".to_owned(),
            },
        ];
        config
    }

    #[test]
    fn raft_backend_requires_nodes() {
        let mut config = raft_base();
        config.consensus.raft.nodes.clear();
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("nodes"));
    }

    #[test]
    fn raft_backend_requires_the_local_node() {
        let mut config = raft_base();
        config.consensus.raft.nodes.retain(|node| node.id != "a");
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("node_id"));
    }

    #[test]
    fn raft_backend_requires_a_secret() {
        let mut config = raft_base();
        config.consensus.raft.secret = secret::SecretString::new("");
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("secret"));
    }

    #[test]
    fn raft_node_address_must_be_absolute() {
        let mut config = raft_base();
        config.consensus.raft.nodes[0].addr = "127.0.0.1:9000".to_owned();
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("addr"));
    }

    #[test]
    fn raft_backend_rejects_peers() {
        let mut config = raft_base();
        config.consensus.raft.peers = vec!["a".to_owned()];
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("peers"));
    }

    #[test]
    fn single_backend_rejects_nodes() {
        let mut config = cluster_base();
        config.consensus.raft.nodes = vec![RaftNodeConfig {
            id: "a".to_owned(),
            addr: "http://127.0.0.1:9000".to_owned(),
        }];
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("nodes"));
    }

    fn consul_base() -> Config {
        let mut config = cluster_base();
        config.consensus.backend = ConsensusBackend::Consul;
        config.consensus.consul.node_id = "a".to_owned();
        config.consensus.consul.address = "http://127.0.0.1:8500".to_owned();
        config.consensus.consul.advertise_addr = "http://127.0.0.1:9000".to_owned();
        config.consensus.consul.secret = secret::SecretString::new("proposal-token");
        config
    }

    #[test]
    fn consul_backend_loads() {
        let config = consul_base();
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn consul_backend_requires_address() {
        let mut config = consul_base();
        config.consensus.consul.address.clear();
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("address"));
    }

    #[test]
    fn consul_backend_requires_an_advertise_address() {
        let mut config = consul_base();
        config.consensus.consul.advertise_addr.clear();
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("advertise_addr"));
    }

    #[test]
    fn consul_backend_requires_a_secret() {
        let mut config = consul_base();
        config.consensus.consul.secret = secret::SecretString::new("");
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("secret"));
    }

    #[test]
    fn consul_secrets_are_redacted() {
        let mut config = consul_base();
        config.consensus.consul.token = secret::SecretString::new("consul-acl-token");
        config.consensus.consul.secret = secret::SecretString::new("proposal-secret-value");
        let table = show_table(&config, &SourceMap::new());
        assert!(!table.contains("consul-acl-token"));
        assert!(!table.contains("proposal-secret-value"));
        assert!(table.contains("consensus.consul.token"));
        assert!(table.contains("consensus.consul.secret"));
        assert!(table.contains("****"));
    }

    #[test]
    fn raft_secret_is_redacted() {
        let mut config = raft_base();
        config.consensus.raft.secret = secret::SecretString::new("raft-secret-value");
        let table = show_table(&config, &SourceMap::new());
        assert!(!table.contains("raft-secret-value"));
        assert!(table.contains("consensus.raft.secret"));
        assert!(table.contains("****"));
    }

    #[test]
    fn cluster_mode_accepts_a_cache_config() {
        let mut config = cluster_base();
        config.cache.mode = CacheMode::Write;
        config.cache.endpoint = "http://127.0.0.1:9003".to_owned();
        config.cache.bucket = "cache".to_owned();
        config.cache.max_bytes = Some(0);
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn cluster_mode_accepts_default_cache_settings() {
        let mut config = cluster_base();
        config.cache = CacheConfig::default();
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn cluster_peers_must_contain_the_node() {
        let mut config = cluster_base();
        config.consensus.raft.peers = vec!["b".to_owned()];
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("must contain"));
    }

    #[test]
    fn single_backend_requires_one_peer() {
        let mut config = cluster_base();
        config.consensus.raft.peers = vec!["a".to_owned(), "b".to_owned()];
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("exactly one peer"));
    }

    fn replicated_bucket() -> BucketConfig {
        BucketConfig {
            name: "media".to_owned(),
            replicas: Some(2),
            min_replicas: Some(1),
            ..BucketConfig::default()
        }
    }

    #[test]
    fn replication_requires_cluster_nodes() {
        let mut config = cluster_base();
        config.buckets.push(replicated_bucket());
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cluster.nodes"));
    }

    #[test]
    fn replication_requires_a_cluster_secret() {
        let mut config = cluster_base();
        config.buckets.push(replicated_bucket());
        config.cluster.nodes = vec![ClusterNodeConfig {
            id: "a".to_owned(),
            addr: "http://127.0.0.1:9000".to_owned(),
        }];
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cluster.secret"));
    }

    #[test]
    fn cluster_nodes_must_contain_the_local_node() {
        let mut config = cluster_base();
        config.cluster.nodes = vec![ClusterNodeConfig {
            id: "b".to_owned(),
            addr: "http://127.0.0.1:9100".to_owned(),
        }];
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("local node id"));
    }

    #[test]
    fn cluster_nodes_reject_duplicate_ids() {
        let mut config = cluster_base();
        config.cluster.nodes = vec![
            ClusterNodeConfig {
                id: "a".to_owned(),
                addr: "http://127.0.0.1:9000".to_owned(),
            },
            ClusterNodeConfig {
                id: "a".to_owned(),
                addr: "http://127.0.0.1:9100".to_owned(),
            },
        ];
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("more than one time"));
    }

    #[test]
    fn cluster_secret_is_redacted() {
        let mut config = cluster_base();
        config.cluster.secret = secret::SecretString::new("cluster-secret-value");
        let table = show_table(&config, &SourceMap::new());
        assert!(!table.contains("cluster-secret-value"));
        assert!(table.contains("cluster.secret"));
        assert!(table.contains("****"));
    }

    #[test]
    fn quorum_and_all_write_ack_are_accepted() {
        for policy in [WriteAck::Quorum, WriteAck::All] {
            let mut config = cluster_base();
            config.buckets.push(BucketConfig {
                name: "media".to_owned(),
                write_ack: Some(policy),
                replicas: Some(1),
                min_replicas: Some(1),
                ..BucketConfig::default()
            });
            assert!(validate(&config).is_ok());
        }
    }

    #[test]
    fn min_replicas_must_not_exceed_replicas() {
        let mut config = cluster_base();
        config.cluster.nodes = vec![ClusterNodeConfig {
            id: "a".to_owned(),
            addr: "http://127.0.0.1:9000".to_owned(),
        }];
        config.cluster.secret = secret::SecretString::new("cluster-secret");
        config.buckets.push(BucketConfig {
            name: "media".to_owned(),
            replicas: Some(1),
            min_replicas: Some(2),
            ..BucketConfig::default()
        });
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("min_replicas"));
    }

    #[test]
    fn replicas_must_not_exceed_cluster_nodes() {
        let mut config = cluster_base();
        config.cluster.nodes = vec![ClusterNodeConfig {
            id: "a".to_owned(),
            addr: "http://127.0.0.1:9000".to_owned(),
        }];
        config.cluster.secret = secret::SecretString::new("cluster-secret");
        config.buckets.push(BucketConfig {
            name: "media".to_owned(),
            replicas: Some(2),
            min_replicas: Some(1),
            ..BucketConfig::default()
        });
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cluster.nodes"));
    }

    #[test]
    fn replicate_timeout_must_be_positive() {
        let mut config = cluster_base();
        config.cluster.replicate_timeout = HumanDuration::new(Duration::ZERO);
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("replicate_timeout"));
    }

    #[test]
    fn duplicate_client_keys_are_rejected() {
        let client = ClientConfig {
            access_key_id: "AKIA".to_owned(),
            secret_access_key: secret::SecretString::new("secret"),
            session_token: None,
            buckets: vec!["media".to_owned()],
        };
        let mut config = base();
        config.clients = vec![client.clone(), client];
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("more than one time"));
    }

    #[test]
    fn duplicate_bucket_names_are_rejected() {
        let bucket = BucketConfig {
            name: "media".to_owned(),
            ..BucketConfig::default()
        };
        let mut config = base();
        config.buckets = vec![bucket.clone(), bucket];
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("more than one time"));
    }

    #[test]
    fn duration_parser_accepts_suffixes() {
        let value: HumanDuration = serde_json::from_str("\"1h\"").unwrap();
        assert_eq!(value.as_duration(), Duration::from_secs(3_600));
        let value: HumanDuration = serde_json::from_str("\"30m\"").unwrap();
        assert_eq!(value.as_duration(), Duration::from_mins(30));
        let value: HumanDuration = serde_json::from_str("15").unwrap();
        assert_eq!(value.as_duration(), Duration::from_secs(15));
    }

    #[test]
    fn otel_endpoint_must_be_absolute() {
        let mut config = base();
        config.otel.endpoint = Some("127.0.0.1:4317".to_owned());
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("otel.endpoint"));
    }

    #[test]
    fn otel_durations_must_be_positive() {
        let mut config = base();
        config.otel.timeout = HumanDuration::new(Duration::ZERO);
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("otel.timeout"));

        let mut config = base();
        config.otel.export_interval = Some(HumanDuration::new(Duration::ZERO));
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("export_interval"));
    }

    #[test]
    fn otel_signal_defaults_keep_metrics_on() {
        let otel = OtelConfig::default();
        assert!(otel.metrics);
        assert!(!otel.traces);
        assert!(!otel.logs);
        assert!(!otel.record_object_keys);
        assert!((otel.trace_sample_ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn otel_trace_and_log_endpoints_must_be_absolute() {
        let mut config = base();
        config.otel.trace_endpoint = Some("127.0.0.1:4317".to_owned());
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("otel.trace_endpoint"));

        let mut config = base();
        config.otel.logs_endpoint = Some("127.0.0.1:4317".to_owned());
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("otel.logs_endpoint"));
    }

    #[test]
    fn otel_schedule_delays_must_be_positive() {
        let mut config = base();
        config.otel.trace_schedule_delay = HumanDuration::new(Duration::ZERO);
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("trace_schedule_delay"));

        let mut config = base();
        config.otel.log_schedule_delay = HumanDuration::new(Duration::ZERO);
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("log_schedule_delay"));
    }

    #[test]
    fn otel_sample_ratio_must_be_in_range() {
        for ratio in [-0.1, 1.1, f64::NAN] {
            let mut config = base();
            config.otel.trace_sample_ratio = ratio;
            let error = validate(&config).unwrap_err();
            assert!(error.to_string().contains("trace_sample_ratio"));
        }
        for ratio in [0.0, 0.5, 1.0] {
            let mut config = base();
            config.otel.trace_sample_ratio = ratio;
            assert!(validate(&config).is_ok());
        }
    }

    #[test]
    fn otel_header_names_must_be_valid_and_lowercase() {
        let mut config = base();
        config
            .otel
            .headers
            .insert("X-Api-Key".to_owned(), secret::SecretString::new("secret"));
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("lowercase"));

        let mut config = base();
        config
            .otel
            .headers
            .insert("x api key".to_owned(), secret::SecretString::new("secret"));
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("not valid"));
    }

    #[test]
    fn otel_headers_are_redacted() {
        let mut config = base();
        config.otel.headers.insert(
            "x-api-key".to_owned(),
            secret::SecretString::new("header-secret"),
        );
        let table = show_table(&config, &SourceMap::new());
        assert!(!table.contains("header-secret"));
        assert!(table.contains("otel.headers"));
        assert!(table.contains("****"));
    }
}
