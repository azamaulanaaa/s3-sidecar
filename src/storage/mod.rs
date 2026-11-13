//! S3 HTTP client for the origin endpoint.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, Method, StatusCode, header};
use reqwest::{Client, Response, Url};
use tracing::warn;

use crate::auth::body::BodyStream;
use crate::auth::sign::sign_request;
use crate::auth::{ClientCredential, EMPTY_SHA256, now};
use crate::config::{EndpointConfig, UpstreamConfig};
use crate::errors::S3Error;

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

impl std::error::Error for StorageError {}

/// An S3 client for one endpoint.
#[derive(Clone)]
pub struct S3Client {
    http: Client,
    endpoint: Url,
    region: String,
    request_timeout: Duration,
    retry_attempts: u32,
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
        })
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
        let url = Url::parse(&format!("{base}{}", request.path_and_query))
            .map_err(|_| StorageError::Sign(S3Error::internal()))?;

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
