//! Layered configuration loading and validation.

pub mod duration;
pub mod loader;
pub mod secret;
pub mod show;

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use self::duration::HumanDuration;
use self::loader::SourceMap;
use self::secret::SecretString;

/// Run mode for the sidecar.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheMode {
    /// Do not cache. All requests go to origin.
    #[default]
    Off,
    /// Cache reads only.
    Read,
    /// Buffer writes only.
    Write,
    /// Cache reads and buffer writes.
    ReadWrite,
}

impl fmt::Display for CacheMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Off => f.write_str("off"),
            Self::Read => f.write_str("read"),
            Self::Write => f.write_str("write"),
            Self::ReadWrite => f.write_str("read_write"),
        }
    }
}

/// Cache write policy.
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

/// HTTP listener settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Listen address, for example `127.0.0.1:9000`.
    pub bind: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:9000".to_owned(),
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
    /// Key prefix for this node.
    pub prefix: String,
    /// Name of this sidecar node. It is part of the cache key and journal path.
    pub node_id: String,
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
            node_id: "node".to_owned(),
            mode: CacheMode::Off,
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
    validate_cache(config, &mut warnings)?;
    validate_endpoints(config)?;
    validate_cluster(config)?;
    validate_buckets(config)?;
    validate_clients(config)?;
    Ok(warnings)
}

fn validate_cache(config: &Config, warnings: &mut Vec<String>) -> Result<(), ConfigError> {
    if config.cache.max_object_bytes == 0 {
        return Err(ConfigError::invalid(
            "cache.max_object_bytes must be greater than 0",
        ));
    }
    if config.cache.mode != CacheMode::Off && config.cache.max_bytes.is_none() {
        return Err(ConfigError::invalid(
            "cache.max_bytes is required when cache.mode is not \"off\". \
             Set cache.max_bytes to a byte count, or 0 for unlimited.",
        ));
    }
    let Some(max_bytes) = config.cache.max_bytes else {
        return Ok(());
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
    if config.cache.mode != CacheMode::Off && config.cache.endpoint.trim().is_empty() {
        return Err(ConfigError::invalid(
            "cache.endpoint is required when cache.mode is not \"off\"",
        ));
    }
    if config.cache.mode != CacheMode::Off {
        if config.cache.bucket.trim().is_empty() {
            return Err(ConfigError::invalid(
                "cache.bucket is required when cache.mode is not \"off\"",
            ));
        }
        if config.cache.node_id.trim().is_empty() {
            return Err(ConfigError::invalid(
                "cache.node_id must not be empty when cache.mode is not \"off\"",
            ));
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
            return Err(ConfigError::invalid(
                "cache.index_path must not be empty when cache.mode is not \"off\"",
            ));
        }
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
    if config.cache.mode != CacheMode::Off {
        return Err(ConfigError::invalid(
            "cache.mode must be \"off\" when mode is \"cluster\"",
        ));
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
        Config::default()
    }

    #[test]
    fn cache_mode_requires_max_bytes() {
        let mut config = base();
        config.cache.mode = CacheMode::Read;
        config.cache.endpoint = "http://127.0.0.1:9002".to_owned();
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cache.max_bytes is required"));
    }

    #[test]
    fn unlimited_cache_accepts_zero() {
        let mut config = base();
        config.cache.mode = CacheMode::Read;
        config.cache.endpoint = "http://127.0.0.1:9002".to_owned();
        config.cache.bucket = "cache".to_owned();
        config.cache.max_bytes = Some(0);
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn cache_mode_requires_a_cache_bucket() {
        let mut config = base();
        config.cache.mode = CacheMode::Read;
        config.cache.endpoint = "http://127.0.0.1:9002".to_owned();
        config.cache.max_bytes = Some(0);
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cache.bucket is required"));
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
    fn cluster_mode_rejects_a_cache_mode() {
        let mut config = cluster_base();
        config.cache.mode = CacheMode::Write;
        config.cache.endpoint = "http://127.0.0.1:9003".to_owned();
        config.cache.bucket = "cache".to_owned();
        config.cache.max_bytes = Some(0);
        let error = validate(&config).unwrap_err();
        assert!(error.to_string().contains("cache.mode"));
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
}
