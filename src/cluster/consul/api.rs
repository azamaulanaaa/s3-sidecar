//! Consul KV and session API.
//!
//! The production client uses the Consul HTTP API. Tests use an in-memory
//! implementation of the same trait.

use std::time::Duration;

use base64::Engine as _;
use futures_util::future::BoxFuture;
use serde::Deserialize;

use crate::config::ConsulConfig;

/// Key that holds the current leader record.
pub const LEADER_KEY: &str = "s3-sidecar/leader";
/// Prefix that holds object metadata records.
pub const META_PREFIX: &str = "s3-sidecar/meta/";
/// Prefix that holds bucket config records.
pub const BUCKET_PREFIX: &str = "s3-sidecar/bucket/";
/// Default wait time for one blocking query.
pub const WATCH_WAIT: Duration = Duration::from_secs(30);

/// A Consul API problem.
#[derive(Debug, thiserror::Error)]
pub enum ConsulError {
    /// The HTTP request failed.
    #[error("the consul request failed: {0}")]
    Transport(String),
    /// Consul returned an unexpected status.
    #[error("consul returned status {0}")]
    Status(u16),
    /// The response body is not valid.
    #[error("the consul response is not valid: {0}")]
    Decode(String),
}

impl ConsulError {
    /// Return true when the error is a transport problem.
    #[must_use]
    pub fn is_transport(&self) -> bool {
        matches!(self, Self::Transport(_))
    }
}

/// One KV entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvEntry {
    /// Full key path.
    pub key: String,
    /// Raw value bytes.
    pub value: Vec<u8>,
    /// Consul modify index.
    pub modify_index: u64,
    /// Session that holds a lock on the key.
    pub session: Option<String>,
}

/// The result of a prefix read or a blocking query.
#[derive(Debug, Clone, Default)]
pub struct PrefixResult {
    /// Consul index for the result.
    pub index: u64,
    /// Entries under the prefix, ordered by key.
    pub entries: Vec<KvEntry>,
}

/// Consul operations used by the coordinator.
pub trait ConsulApi: Send + Sync {
    /// Create one session and return its identifier.
    fn create_session(&self, ttl: Duration) -> BoxFuture<'_, Result<String, ConsulError>>;

    /// Renew one session. Returns false when the session does not exist.
    fn renew_session<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, ConsulError>>;

    /// Destroy one session.
    fn destroy_session<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), ConsulError>>;

    /// Acquire one key with a session.
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        session: &'a str,
    ) -> BoxFuture<'a, Result<bool, ConsulError>>;

    /// Read one key. A consistent read is linearizable.
    fn get<'a>(
        &'a self,
        key: &'a str,
        consistent: bool,
    ) -> BoxFuture<'a, Result<Option<KvEntry>, ConsulError>>;

    /// Read every key under a prefix.
    fn list<'a>(
        &'a self,
        prefix: &'a str,
        consistent: bool,
    ) -> BoxFuture<'a, Result<PrefixResult, ConsulError>>;

    /// Write one key when its current index equals `cas`. `cas = 0` creates.
    fn put_cas<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        cas: u64,
    ) -> BoxFuture<'a, Result<bool, ConsulError>>;

    /// Delete one key when its current index equals `cas`.
    fn delete_cas<'a>(&'a self, key: &'a str, cas: u64)
    -> BoxFuture<'a, Result<bool, ConsulError>>;

    /// Wait until a prefix changes after `index`, or until `wait` elapses.
    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        index: u64,
        wait: Duration,
    ) -> BoxFuture<'a, Result<PrefixResult, ConsulError>>;
}

/// Consul HTTP API client.
pub struct HttpConsul {
    client: reqwest::Client,
    base: String,
    token: String,
    watch_wait: Duration,
}

impl HttpConsul {
    /// Build one client from the Consul configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be built.
    pub fn new(config: &ConsulConfig) -> Result<Self, ConsulError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(WATCH_WAIT + Duration::from_secs(10))
            .build()
            .map_err(|error| ConsulError::Transport(error.to_string()))?;
        Ok(Self {
            client,
            base: config.address.trim_end_matches('/').to_owned(),
            token: config.token.expose_secret().to_owned(),
            watch_wait: WATCH_WAIT,
        })
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let request = self.client.request(method, format!("{}{path}", self.base));
        if self.token.is_empty() {
            request
        } else {
            request.header("X-Consul-Token", &self.token)
        }
    }

    async fn json<T: for<'de> Deserialize<'de>>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, ConsulError> {
        let response = request
            .send()
            .await
            .map_err(|error| ConsulError::Transport(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ConsulError::Status(status.as_u16()));
        }
        response
            .json()
            .await
            .map_err(|error| ConsulError::Decode(error.to_string()))
    }

    async fn bool_result(&self, request: reqwest::RequestBuilder) -> Result<bool, ConsulError> {
        let response = request
            .send()
            .await
            .map_err(|error| ConsulError::Transport(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ConsulError::Status(status.as_u16()));
        }
        let body = response
            .text()
            .await
            .map_err(|error| ConsulError::Decode(error.to_string()))?;
        Ok(body.trim().eq_ignore_ascii_case("true"))
    }

    async fn prefix(
        &self,
        request: reqwest::RequestBuilder,
        fallback_index: u64,
    ) -> Result<PrefixResult, ConsulError> {
        let response = request
            .send()
            .await
            .map_err(|error| ConsulError::Transport(error.to_string()))?;
        let status = response.status();
        let index = response
            .headers()
            .get("x-consul-index")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(fallback_index);
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(PrefixResult {
                index,
                entries: Vec::new(),
            });
        }
        if !status.is_success() {
            return Err(ConsulError::Status(status.as_u16()));
        }
        let raw: Vec<RawEntry> = response
            .json()
            .await
            .map_err(|error| ConsulError::Decode(error.to_string()))?;
        let mut entries = decode_entries(raw)?;
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(PrefixResult { index, entries })
    }
}

impl ConsulApi for HttpConsul {
    fn create_session(&self, ttl: Duration) -> BoxFuture<'_, Result<String, ConsulError>> {
        Box::pin(async move {
            let body = serde_json::json!({
                "Name": "s3-sidecar",
                "TTL": format!("{}s", ttl.as_secs().max(1)),
                "Behavior": "delete",
                "LockDelay": "0s",
            });
            let reply: SessionReply = self
                .json(
                    self.request(reqwest::Method::PUT, "/v1/session/create")
                        .json(&body),
                )
                .await?;
            Ok(reply.id)
        })
    }

    fn renew_session<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move {
            let path = format!("/v1/session/renew/{}", encode_path(id));
            let response = self
                .request(reqwest::Method::PUT, &path)
                .send()
                .await
                .map_err(|error| ConsulError::Transport(error.to_string()))?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(false);
            }
            if !response.status().is_success() {
                return Err(ConsulError::Status(response.status().as_u16()));
            }
            Ok(true)
        })
    }

    fn destroy_session<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), ConsulError>> {
        Box::pin(async move {
            let path = format!("/v1/session/destroy/{}", encode_path(id));
            let response = self
                .request(reqwest::Method::PUT, &path)
                .send()
                .await
                .map_err(|error| ConsulError::Transport(error.to_string()))?;
            if response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND
            {
                return Ok(());
            }
            Err(ConsulError::Status(response.status().as_u16()))
        })
    }

    fn acquire<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        session: &'a str,
    ) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move {
            let path = format!(
                "/v1/kv/{}?acquire={}",
                encode_path(key),
                encode_path(session)
            );
            self.bool_result(self.request(reqwest::Method::PUT, &path).body(value))
                .await
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
        consistent: bool,
    ) -> BoxFuture<'a, Result<Option<KvEntry>, ConsulError>> {
        Box::pin(async move {
            let mut path = format!("/v1/kv/{}", encode_path(key));
            if consistent {
                path.push_str("?consistent");
            }
            let response = self
                .request(reqwest::Method::GET, &path)
                .send()
                .await
                .map_err(|error| ConsulError::Transport(error.to_string()))?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !response.status().is_success() {
                return Err(ConsulError::Status(response.status().as_u16()));
            }
            let raw: Vec<RawEntry> = response
                .json()
                .await
                .map_err(|error| ConsulError::Decode(error.to_string()))?;
            Ok(decode_entries(raw)?.into_iter().next())
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        consistent: bool,
    ) -> BoxFuture<'a, Result<PrefixResult, ConsulError>> {
        Box::pin(async move {
            let mut path = format!("/v1/kv/{}?recurse", encode_path(prefix));
            if consistent {
                path.push_str("&consistent");
            }
            self.prefix(self.request(reqwest::Method::GET, &path), 0)
                .await
        })
    }

    fn put_cas<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        cas: u64,
    ) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move {
            let path = format!("/v1/kv/{}?cas={cas}", encode_path(key));
            self.bool_result(self.request(reqwest::Method::PUT, &path).body(value))
                .await
        })
    }

    fn delete_cas<'a>(
        &'a self,
        key: &'a str,
        cas: u64,
    ) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move {
            let path = format!("/v1/kv/{}?cas={cas}", encode_path(key));
            self.bool_result(self.request(reqwest::Method::DELETE, &path))
                .await
        })
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        index: u64,
        wait: Duration,
    ) -> BoxFuture<'a, Result<PrefixResult, ConsulError>> {
        Box::pin(async move {
            let wait = wait.min(self.watch_wait);
            let path = format!(
                "/v1/kv/{}?recurse&index={index}&wait={}s",
                encode_path(prefix),
                wait.as_secs().max(1)
            );
            self.prefix(self.request(reqwest::Method::GET, &path), index)
                .await
        })
    }
}

#[derive(Deserialize)]
struct SessionReply {
    #[serde(rename = "ID")]
    id: String,
}

#[derive(Deserialize)]
struct RawEntry {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: Option<String>,
    #[serde(rename = "ModifyIndex")]
    modify_index: u64,
    #[serde(rename = "Session")]
    session: Option<String>,
}

fn decode_entries(raw: Vec<RawEntry>) -> Result<Vec<KvEntry>, ConsulError> {
    let mut output = Vec::with_capacity(raw.len());
    for entry in raw {
        let value = match entry.value {
            Some(value) => base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(|error| ConsulError::Decode(error.to_string()))?,
            None => Vec::new(),
        };
        output.push(KvEntry {
            key: entry.key,
            value,
            modify_index: entry.modify_index,
            session: entry.session.filter(|value| !value.is_empty()),
        });
    }
    Ok(output)
}

fn encode_path(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace(' ', "%20")
        .replace('?', "%3F")
        .replace('#', "%23")
}
