//! S3 HTTP client for the origin endpoint.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, Method, StatusCode, header};
use reqwest::{Client, Response, Url};
use tracing::Instrument;
use tracing::warn;

use crate::auth::body::BodyStream;
use crate::auth::sign::sign_request;
use crate::auth::{ClientCredential, EMPTY_SHA256, now};
use crate::config::{EndpointConfig, UpstreamConfig};
use crate::errors::S3Error;
use crate::telemetry;

/// A request to the upstream S3 endpoint.
pub struct ForwardRequest {
    /// HTTP method.
    pub method: Method,
    /// Raw path and query. The path must start with `/`.
    pub path_and_query: String,
    /// Request headers. Authentication headers must be removed.
    pub headers: HeaderMap,
    /// Request body. The body must already be decoded.
    pub body: Option<BodyStream>,
    /// Decoded content length, when known.
    pub content_length: Option<u64>,
}

/// An error from the upstream S3 client.
pub enum StorageError {
    /// The request could not be signed.
    Sign(S3Error),
    /// The upstream returned a non-success status.
    Status(StatusCode),
    /// The request body ended before or after the declared length.
    Body,
    /// The object needs a part size above the S3 limit.
    TooLarge,
    /// The request did not finish within the configured timeout.
    Timeout,
    /// The transport failed.
    Transport(reqwest::Error),
}

impl StorageError {
    /// Return a short transport description. This never includes the URL.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Sign(_) => "sign",
            Self::Status(_) => "status",
            Self::Body => "body",
            Self::TooLarge => "too_large",
            Self::Timeout => "timeout",
            Self::Transport(error) if error.is_timeout() => "timeout",
            Self::Transport(error) if error.is_connect() => "connect",
            Self::Transport(error) if error.is_body() => "body",
            Self::Transport(error) if error.is_decode() => "decode",
            Self::Transport(_) => "transport",
        }
    }
}

impl fmt::Debug for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageError")
            .field("kind", &self.kind())
            .finish()
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.kind())
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sign(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::Status(_) | Self::Body | Self::TooLarge | Self::Timeout => None,
        }
    }
}

/// An S3 client for one endpoint.
#[derive(Clone)]
pub struct S3Client {
    http: Client,
    endpoint: Url,
    region: String,
    request_timeout: Duration,
    retry_attempts: u32,
    record_object_keys: bool,
}

impl S3Client {
    /// Build a client from endpoint configuration with default upstream
    /// settings.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint URL is not valid.
    pub fn new(config: &EndpointConfig) -> Result<Self, S3Error> {
        Self::new_with_upstream(config, &UpstreamConfig::default())
    }

    /// Build a client from endpoint and upstream configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint URL is not valid.
    pub fn new_with_upstream(
        config: &EndpointConfig,
        upstream: &UpstreamConfig,
    ) -> Result<Self, S3Error> {
        let endpoint = Url::parse(config.endpoint.trim()).map_err(|_| {
            S3Error::new(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "The origin endpoint is not a valid URL.",
            )
        })?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(S3Error::new(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "The origin endpoint must be an http or https URL without a query.",
            ));
        }
        let http = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .connect_timeout(upstream.connect_timeout.as_duration())
            .read_timeout(upstream.request_timeout.as_duration())
            .pool_idle_timeout(Duration::from_secs(90))
            .user_agent(concat!("s3-sidecar/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| {
                S3Error::new(
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    "The HTTP client could not be built.",
                )
            })?;
        Ok(Self {
            http,
            endpoint,
            region: config.region_or_default().to_owned(),
            request_timeout: upstream.request_timeout.as_duration(),
            retry_attempts: upstream.retry_attempts,
            record_object_keys: false,
        })
    }

    /// Set whether span paths keep the object key.
    #[must_use]
    pub fn with_record_object_keys(mut self, record_object_keys: bool) -> Self {
        self.record_object_keys = record_object_keys;
        self
    }

    /// Send a signed request to the origin.
    ///
    /// A GET, HEAD, DELETE, or list request is retried on a connection error,
    /// a 429 response, or a 5xx response. A request with a body is never
    /// retried because the body cannot be replayed after it starts streaming.
    ///
    /// # Errors
    ///
    /// Returns a signing, timeout, or transport error.
    pub async fn execute(
        &self,
        credential: &ClientCredential,
        request: ForwardRequest,
    ) -> Result<Response, StorageError> {
        let base = self.endpoint.as_str().trim_end_matches('/');
        let path_and_query = encode_dot_segments(&request.path_and_query);
        let url = Url::parse(&format!("{base}{path_and_query}"))
            .map_err(|_| StorageError::Sign(S3Error::internal()))?;

        let span = tracing::info_span!(
            "s3.upstream",
            "http.request.method" = %request.method,
            "server.address" = url.host_str().unwrap_or_default(),
            "url.path" = telemetry::redact_object_path(url.path(), self.record_object_keys),
            "http.response.status_code" = tracing::field::Empty,
            "otel.kind" = "client",
        );
        let result = self
            .execute_url(credential, request, url)
            .instrument(span.clone())
            .await;
        if let Ok(response) = &result {
            span.record("http.response.status_code", response.status().as_u16());
        }
        result
    }

    async fn execute_url(
        &self,
        credential: &ClientCredential,
        request: ForwardRequest,
        url: Url,
    ) -> Result<Response, StorageError> {
        if request.body.is_some() {
            return self.send_once(credential, request, url, false).await;
        }

        let retryable = is_idempotent(&request.method);
        let attempts = if retryable {
            self.retry_attempts.saturating_add(1)
        } else {
            1
        };
        for attempt in 0..attempts {
            let headers = request.headers.clone();
            let content_length = request.content_length;
            match self
                .send_headers(credential, &request.method, &url, headers, content_length)
                .await
            {
                Ok(response)
                    if attempt.saturating_add(1) < attempts
                        && retryable_status(response.status()) =>
                {
                    let retry_after = retry_after(response.headers());
                    warn!(
                        status = response.status().as_u16(),
                        attempt = attempt.saturating_add(1),
                        "upstream request will retry"
                    );
                    drop(response);
                    tokio::time::sleep(backoff(attempt, retry_after)).await;
                }
                Ok(response) => return Ok(response),
                Err(StorageError::Timeout | StorageError::Transport(_))
                    if retryable && attempt.saturating_add(1) < attempts =>
                {
                    warn!(
                        attempt = attempt.saturating_add(1),
                        "upstream request will retry after a transport error"
                    );
                    tokio::time::sleep(backoff(attempt, None)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageError::Timeout)
    }

    async fn send_once(
        &self,
        credential: &ClientCredential,
        request: ForwardRequest,
        url: Url,
        apply_timeout: bool,
    ) -> Result<Response, StorageError> {
        let mut headers = request.headers;
        telemetry::inject_context(&mut headers);
        sign_request(
            &request.method,
            &url,
            &mut headers,
            "UNSIGNED-PAYLOAD",
            credential,
            &self.region,
            now(),
        )
        .map_err(StorageError::Sign)?;
        let mut builder = self.http.request(request.method, url).headers(headers);
        if let Some(length) = request.content_length {
            builder = builder.header(header::CONTENT_LENGTH, length.to_string());
        }
        if let Some(body) = request.body {
            builder = builder.body(reqwest::Body::wrap_stream(body));
        }
        let send = builder.send();
        if apply_timeout {
            match tokio::time::timeout(self.request_timeout, send).await {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(error)) => Err(StorageError::Transport(error)),
                Err(_) => Err(StorageError::Timeout),
            }
        } else {
            send.await.map_err(StorageError::Transport)
        }
    }

    async fn send_headers(
        &self,
        credential: &ClientCredential,
        method: &Method,
        url: &Url,
        mut headers: HeaderMap,
        content_length: Option<u64>,
    ) -> Result<Response, StorageError> {
        telemetry::inject_context(&mut headers);
        sign_request(
            method,
            url,
            &mut headers,
            EMPTY_SHA256,
            credential,
            &self.region,
            now(),
        )
        .map_err(StorageError::Sign)?;
        let mut builder = self
            .http
            .request(method.clone(), url.clone())
            .headers(headers);
        if let Some(length) = content_length {
            builder = builder.header(header::CONTENT_LENGTH, length.to_string());
        }
        match tokio::time::timeout(self.request_timeout, builder.send()).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(StorageError::Transport(error)),
            Err(_) => Err(StorageError::Timeout),
        }
    }

    /// Return the endpoint URL.
    #[must_use]
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }
}

/// Encode dot segments in a request path. The query string is not changed.
///
/// The `url` crate resolves `.` and `..` path segments, and it also resolves
/// their percent-encoded forms. S3 object keys are literal, so this function
/// keeps dot segments intact before URL parsing. It writes each dot segment as
/// `%2E` or `%2E%2E`. It also writes a separator that touches a dot segment as
/// `%2F`. The origin decodes that separator back to `/`.
fn encode_dot_segments(path_and_query: &str) -> String {
    let (path, query) = match path_and_query.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (path_and_query, None),
    };
    let segments: Vec<&str> = path.split('/').collect();
    let dot: Vec<Option<&str>> = segments
        .iter()
        .map(|segment| dot_segment(segment))
        .collect();
    let mut output = String::with_capacity(path.len() + 8);
    for (index, segment) in segments.iter().enumerate() {
        if index > 0 {
            if dot[index - 1].is_some() || dot[index].is_some() {
                output.push_str("%2F");
            } else {
                output.push('/');
            }
        }
        match dot[index] {
            Some(encoded) => output.push_str(encoded),
            None => output.push_str(segment),
        }
    }
    if let Some(query) = query {
        output.push('?');
        output.push_str(query);
    }
    output
}

fn dot_segment(segment: &str) -> Option<&'static str> {
    match percent_encoding::percent_decode_str(segment)
        .decode_utf8()
        .ok()
        .as_deref()
    {
        Some(".") => Some("%2E"),
        Some("..") => Some("%2E%2E"),
        _ => None,
    }
}

fn is_idempotent(method: &Method) -> bool {
    matches!(*method, Method::GET | Method::HEAD | Method::DELETE)
}

fn retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(header::RETRY_AFTER)?.to_str().ok()?;
    let seconds = value.trim().parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds.min(60)))
}

fn backoff(attempt: u32, retry_after: Option<Duration>) -> Duration {
    if let Some(delay) = retry_after {
        return delay;
    }
    let factor = 1_u64.checked_shl(attempt.min(5)).unwrap_or(32);
    let base = 50_u64.saturating_mul(factor);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.subsec_nanos());
    let jitter = u64::from(nanos) % (base / 2 + 1);
    Duration::from_millis(base.saturating_add(jitter))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use axum::http::StatusCode;

    use super::{StorageError, encode_dot_segments};
    use crate::errors::S3Error;

    #[test]
    fn sign_error_keeps_the_source() {
        let error = StorageError::Sign(S3Error::internal());
        let source = std::error::Error::source(&error).unwrap();
        assert!(source.downcast_ref::<S3Error>().is_some());
    }

    #[test]
    fn transport_error_keeps_the_source() {
        let request_error = reqwest::Client::new().get("not a url").build().unwrap_err();
        let error = StorageError::Transport(request_error);
        let source = std::error::Error::source(&error).unwrap();
        assert!(source.downcast_ref::<reqwest::Error>().is_some());
    }

    #[test]
    fn status_error_has_no_source() {
        let error = StorageError::Status(StatusCode::NOT_FOUND);
        assert!(std::error::Error::source(&error).is_none());
    }

    #[test]
    fn dot_segments_survive_url_parsing() {
        let cases = [
            ("/media/a/../b", "/media/a%2F%2E%2E%2Fb"),
            ("/media/a/./b", "/media/a%2F%2E%2Fb"),
            ("/media/a/b", "/media/a/b"),
            ("/media/a/%2E/b", "/media/a%2F%2E%2Fb"),
            ("/media/a/%2E%2E/b", "/media/a%2F%2E%2E%2Fb"),
            ("/media/..", "/media%2F%2E%2E"),
        ];
        for (path, expected) in cases {
            let encoded = encode_dot_segments(path);
            assert_eq!(encoded, expected);
            let url = reqwest::Url::parse(&format!("http://origin{encoded}")).unwrap();
            assert_eq!(url.path(), expected);
        }
    }

    #[test]
    fn query_string_is_not_encoded() {
        assert_eq!(
            encode_dot_segments("/media/a/../b?list-type=2&prefix=../x&delimiter=%2F"),
            "/media/a%2F%2E%2E%2Fb?list-type=2&prefix=../x&delimiter=%2F"
        );
    }

    #[test]
    fn existing_percent_escapes_are_not_double_encoded() {
        assert_eq!(
            encode_dot_segments("/media/a%2Eb/c%20d"),
            "/media/a%2Eb/c%20d"
        );
        assert!(!encode_dot_segments("/media/a/%2E/b").contains("%25"));
    }
}
