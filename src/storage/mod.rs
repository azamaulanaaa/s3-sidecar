//! S3 HTTP client for the origin endpoint.

use std::fmt;
use std::time::Duration;

use axum::http::{HeaderMap, Method, header};
use reqwest::{Client, Response, Url};

use crate::auth::body::BodyStream;
use crate::auth::sign::sign_request;
use crate::auth::{ClientCredential, EMPTY_SHA256, now};
use crate::config::EndpointConfig;
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
    /// The transport failed.
    Transport(reqwest::Error),
}

impl StorageError {
    /// Return a short transport description. This never includes the URL.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Sign(_) => "sign",
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
pub struct S3Client {
    http: Client,
    endpoint: Url,
    region: String,
}

impl S3Client {
    /// Build a client from endpoint configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint URL is not valid.
    pub fn new(config: &EndpointConfig) -> Result<Self, S3Error> {
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
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(300))
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
        })
    }

    /// Send a signed request to the origin.
    ///
    /// # Errors
    ///
    /// Returns a signing or transport error.
    pub async fn execute(
        &self,
        credential: &ClientCredential,
        request: ForwardRequest,
    ) -> Result<Response, StorageError> {
        let base = self.endpoint.as_str().trim_end_matches('/');
        let url = Url::parse(&format!("{base}{}", request.path_and_query))
            .map_err(|_| StorageError::Sign(S3Error::internal()))?;
        let mut headers = request.headers;
        let payload_hash = if request.body.is_some() {
            "UNSIGNED-PAYLOAD"
        } else {
            EMPTY_SHA256
        };
        sign_request(
            &request.method,
            &url,
            &mut headers,
            payload_hash,
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
        builder.send().await.map_err(StorageError::Transport)
    }

    /// Return the endpoint URL.
    #[must_use]
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }
}
