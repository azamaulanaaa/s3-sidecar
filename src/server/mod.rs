//! HTTP server and S3 request forwarding.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, TryStreamExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::auth::body::{BodyFailure, BodyStream, failure, failure_flag, verify_hash};
use crate::auth::chunked::{ChunkMode, decode};
use crate::auth::verify::authenticate;
use crate::auth::{AuthContext, CredentialStore, PayloadKind, now};
use crate::config::{CacheMode, Config, Mode};
use crate::errors::S3Error;
use crate::storage::{ForwardRequest, S3Client, StorageError};

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Shared server state.
pub struct AppState {
    config: Arc<Config>,
    credentials: CredentialStore,
    client: S3Client,
    ready: AtomicBool,
}

impl AppState {
    /// Build the shared state.
    ///
    /// # Errors
    ///
    /// Returns an error when the origin endpoint is not valid.
    pub fn new(config: Config) -> Result<Self, S3Error> {
        let client = S3Client::new(&config.origin)?;
        let credentials = CredentialStore::new(&config.clients);
        if credentials.is_empty() {
            warn!("no clients are configured; all S3 requests will be denied");
        }
        Ok(Self {
            config: Arc::new(config),
            credentials,
            client,
            ready: AtomicBool::new(false),
        })
    }

    /// Return the configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Set the readiness state.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

/// Build the HTTP router.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .fallback(handle_s3)
        .with_state(state)
}

/// Return the liveness response.
pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Return the readiness response.
pub async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.is_ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

/// Start the HTTP server and wait for shutdown.
///
/// # Errors
///
/// Returns an error when the configuration is not supported in this build,
/// the state cannot be built, or the listener cannot bind.
pub async fn serve(config: Config, warnings: &[String]) -> Result<(), ServeError> {
    if config.mode == Mode::Cluster {
        return Err(ServeError::Unsupported(
            "cluster mode is not implemented in this build".to_owned(),
        ));
    }
    if config.cache.mode != CacheMode::Off {
        return Err(ServeError::Unsupported(
            "cache modes are not implemented in this build. Set cache.mode to \"off\".".to_owned(),
        ));
    }
    if config.origin.endpoint.trim().is_empty() {
        return Err(ServeError::Config(
            "origin.endpoint is required for serve".to_owned(),
        ));
    }
    for warning in warnings {
        warn!("{warning}");
    }

    let state =
        Arc::new(AppState::new(config.clone()).map_err(|error| ServeError::Config(error.message))?);
    let address = config.server.bind.clone();
    let listener = TcpListener::bind(&config.server.bind)
        .await
        .map_err(|source| ServeError::Bind {
            address: address.clone(),
            source,
        })?;
    let local = listener
        .local_addr()
        .map_or_else(|_| address.clone(), |value| value.to_string());
    info!(address = %local, "server started");
    state.set_ready(true);
    let result = axum::serve(listener, router(Arc::clone(&state)))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(ServeError::Run);
    state.set_ready(false);
    result
}

/// Error from the server.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// The configuration uses a mode that this build does not implement.
    #[error("unsupported config: {0}")]
    Unsupported(String),
    /// The configuration is not complete.
    #[error("invalid config: {0}")]
    Config(String),
    /// The listener cannot bind.
    #[error("cannot bind {address}: {source}")]
    Bind {
        /// Listen address.
        address: String,
        /// I/O error.
        source: io::Error,
    },
    /// The server stopped with an error.
    #[error("server stopped: {0}")]
    Run(io::Error),
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

async fn handle_s3(
    State(state): State<Arc<AppState>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let request_id = next_request_id();
    let started = Instant::now();
    let response = match forward(&state, &method, &uri, &headers, body).await {
        Ok(response) => response,
        Err(error) => error.into_response(&request_id),
    };
    info!(
        request_id = %request_id,
        method = %method,
        path = %uri.path(),
        status = response.status().as_u16(),
        elapsed_ms = started.elapsed().as_millis(),
        "request"
    );
    response
}

async fn forward(
    state: &AppState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let auth = authenticate(
        method,
        uri,
        headers,
        &state.credentials,
        state.config.origin.region_or_default(),
        now(),
    )?;
    check_bucket(&auth, uri, headers)?;

    let (credential, payload) = auth.into_parts();
    let bucket = bucket_name(uri.path())?;
    let key = object_key(uri.path());
    warn_checksum_dropped(headers, &payload, &bucket, &key);
    let prepared = prepare_request(body, headers, payload)?;
    let path_and_query = upstream_path_and_query(uri, is_presigned(uri));
    let request = ForwardRequest {
        method: method.clone(),
        path_and_query,
        headers: prepared.headers,
        body: prepared.body,
        content_length: prepared.content_length,
    };

    let result = state.client.execute(credential.as_ref(), request).await;
    let failure_flag = prepared.failure;
    let response = result.map_err(|error| map_storage_error(error, failure_flag.as_ref()))?;
    forward_response(method, response).await
}

fn check_bucket(auth: &AuthContext, uri: &Uri, headers: &HeaderMap) -> Result<(), S3Error> {
    let bucket = bucket_name(uri.path())?;
    if !auth.allows_bucket(&bucket) {
        return Err(S3Error::access_denied().with_resource(uri.path()));
    }
    if let Some(source) = headers.get("x-amz-copy-source") {
        let source = source
            .to_str()
            .map_err(|_| S3Error::invalid_request("The copy source header is not valid."))?;
        if let Some(source_bucket) = copy_source_bucket(source)
            && !auth.allows_bucket(&source_bucket)
        {
            return Err(S3Error::access_denied().with_resource(uri.path()));
        }
    }
    Ok(())
}

struct PreparedRequest {
    headers: HeaderMap,
    body: Option<BodyStream>,
    content_length: Option<u64>,
    failure: Option<Arc<std::sync::atomic::AtomicU8>>,
}

fn prepare_request(
    body: Body,
    headers: &HeaderMap,
    payload: PayloadKind,
) -> Result<PreparedRequest, S3Error> {
    let original_length = content_length(headers)?;
    let mut output_headers = filter_request_headers(headers);

    match payload {
        PayloadKind::Unsigned => {
            let has_body = original_length.is_some_and(|length| length > 0)
                || headers.contains_key(header::TRANSFER_ENCODING);
            let body = has_body.then(|| plain_body(body));
            Ok(PreparedRequest {
                headers: output_headers,
                body,
                content_length: original_length,
                failure: None,
            })
        }
        PayloadKind::Hash(expected) => {
            if is_empty_hash(&expected) && original_length.unwrap_or(0) == 0 {
                return Ok(PreparedRequest {
                    headers: output_headers,
                    body: None,
                    content_length: original_length,
                    failure: None,
                });
            }
            let flag = failure_flag();
            let stream = verify_hash(body.into_data_stream(), expected, Arc::clone(&flag));
            Ok(PreparedRequest {
                headers: output_headers,
                body: Some(stream),
                content_length: original_length,
                failure: Some(flag),
            })
        }
        PayloadKind::Streaming(context) => {
            let decoded_length = context.decoded_length;
            let trailer = context.trailer;
            strip_aws_chunked(&mut output_headers);
            output_headers.remove("x-amz-decoded-content-length");
            output_headers.remove("x-amz-trailer");
            if trailer {
                strip_trailer_checksum_headers(&mut output_headers);
            }
            let (stream, flag) = decode(body.into_data_stream(), ChunkMode::Signed(context));
            Ok(PreparedRequest {
                headers: output_headers,
                body: Some(stream),
                content_length: decoded_length,
                failure: Some(flag),
            })
        }
        PayloadKind::StreamingUnsignedTrailer => {
            let decoded_length = content_length_header(headers, "x-amz-decoded-content-length")?;
            strip_aws_chunked(&mut output_headers);
            output_headers.remove("x-amz-decoded-content-length");
            output_headers.remove("x-amz-trailer");
            strip_trailer_checksum_headers(&mut output_headers);
            let (stream, flag) = decode(body.into_data_stream(), ChunkMode::UnsignedTrailer);
            Ok(PreparedRequest {
                headers: output_headers,
                body: Some(stream),
                content_length: decoded_length,
                failure: Some(flag),
            })
        }
    }
}

fn is_empty_hash(hash: &[u8; 32]) -> bool {
    hex::encode(hash) == crate::auth::EMPTY_SHA256
}

fn plain_body(body: Body) -> BodyStream {
    Box::pin(
        body.into_data_stream()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string())),
    )
}

fn content_length(headers: &HeaderMap) -> Result<Option<u64>, S3Error> {
    content_length_header(headers, "content-length")
}

fn content_length_header(headers: &HeaderMap, name: &str) -> Result<Option<u64>, S3Error> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| S3Error::invalid_request(&format!("The {name} header is not valid.")))?;
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| S3Error::invalid_request(&format!("The {name} header is not a number.")))
}

fn filter_request_headers(headers: &HeaderMap) -> HeaderMap {
    let mut output = HeaderMap::new();
    for (name, value) in headers {
        let name = name.as_str();
        if skip_header(name) {
            continue;
        }
        if let Ok(name) = axum::http::HeaderName::from_bytes(name.as_bytes()) {
            output.append(name, value.clone());
        }
    }
    output
}

fn skip_header(name: &str) -> bool {
    // The sidecar decodes aws-chunked bodies. The HTTP client cannot send
    // trailer checksums, so it also drops the multipart checksum algorithm.
    // The upstream then does not require a checksum on each part.
    matches!(
        name,
        "authorization"
            | "host"
            | "content-length"
            | "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "expect"
            | "cookie"
            | "user-agent"
            | "accept-encoding"
            | "x-amz-date"
            | "x-amz-content-sha256"
            | "x-amz-security-token"
            | "x-amz-checksum-algorithm"
    ) || name.starts_with("x-forwarded-")
}

// A trailer checksum is not available before the body stream starts. Remove
// the checksum request so the upstream does not wait for a trailer.
fn strip_trailer_checksum_headers(headers: &mut HeaderMap) {
    output_remove_prefix(headers, "x-amz-checksum-");
    headers.remove("x-amz-sdk-checksum-algorithm");
}

fn output_remove_prefix(headers: &mut HeaderMap, prefix: &str) {
    let names: Vec<axum::http::HeaderName> = headers
        .keys()
        .filter(|name| name.as_str().starts_with(prefix))
        .cloned()
        .collect();
    for name in names {
        headers.remove(name);
    }
}

fn strip_aws_chunked(headers: &mut HeaderMap) {
    let Some(value) = headers.get(header::CONTENT_ENCODING) else {
        return;
    };
    let Ok(value) = value.to_str() else {
        headers.remove(header::CONTENT_ENCODING);
        return;
    };
    let remaining: Vec<&str> = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty() && !part.eq_ignore_ascii_case("aws-chunked"))
        .collect();
    if remaining.is_empty() {
        headers.remove(header::CONTENT_ENCODING);
    } else if let Ok(value) = HeaderValue::from_str(&remaining.join(", ")) {
        headers.insert(header::CONTENT_ENCODING, value);
    } else {
        headers.remove(header::CONTENT_ENCODING);
    }
}

fn warn_checksum_dropped(headers: &HeaderMap, payload: &PayloadKind, bucket: &str, key: &str) {
    let algorithm = headers.contains_key("x-amz-checksum-algorithm");
    let trailer_checksum = match payload {
        PayloadKind::Streaming(context) => context.trailer && has_checksum_header(headers),
        PayloadKind::StreamingUnsignedTrailer => has_checksum_header(headers),
        PayloadKind::Unsigned | PayloadKind::Hash(_) => false,
    };
    if algorithm || trailer_checksum {
        warn!(
            bucket,
            key,
            "multipart checksum headers were removed; the origin will not validate part checksums"
        );
    }
}

fn has_checksum_header(headers: &HeaderMap) -> bool {
    headers.keys().any(|name| {
        let name = name.as_str();
        name.starts_with("x-amz-checksum-")
            || name == "x-amz-sdk-checksum-algorithm"
            || name == "x-amz-trailer"
    })
}

fn object_key(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    match trimmed.split_once('/') {
        Some((_, key)) => percent_encoding::percent_decode_str(key)
            .decode_utf8_lossy()
            .into_owned(),
        None => String::new(),
    }
}

fn bucket_name(path: &str) -> Result<String, S3Error> {
    let trimmed = path.trim_start_matches('/');
    let first = trimmed.split('/').next().unwrap_or_default();
    if first.is_empty() {
        return Err(S3Error::not_implemented(
            "The ListBuckets operation is not supported.",
        ));
    }
    Ok(percent_encoding::percent_decode_str(first)
        .decode_utf8_lossy()
        .into_owned())
}

fn copy_source_bucket(value: &str) -> Option<String> {
    let value = value.split('?').next().unwrap_or(value);
    let decoded = percent_encoding::percent_decode_str(value).decode_utf8_lossy();
    let trimmed = decoded.trim_start_matches('/');
    let first = trimmed.split('/').next().unwrap_or_default();
    if first.is_empty() {
        None
    } else {
        Some(first.to_owned())
    }
}

fn is_presigned(uri: &Uri) -> bool {
    uri.query().is_some_and(|query| {
        query.split('&').any(|pair| {
            let key = pair.split_once('=').map_or(pair, |(key, _)| key);
            key == "X-Amz-Algorithm"
        })
    })
}

fn upstream_path_and_query(uri: &Uri, presigned: bool) -> String {
    let mut path = uri.path().to_owned();
    if path.is_empty() {
        path.push('/');
    }
    if let Some(query) = uri.query() {
        let query = filter_query(query, presigned);
        if !query.is_empty() {
            path.push('?');
            path.push_str(&query);
        }
    }
    path
}

fn filter_query(query: &str, presigned: bool) -> String {
    query
        .split('&')
        .filter(|pair| {
            if !presigned {
                return true;
            }
            let key = pair.split_once('=').map_or(*pair, |(key, _)| key);
            !key.to_ascii_lowercase().starts_with("x-amz-")
        })
        .collect::<Vec<_>>()
        .join("&")
}

async fn forward_response(
    method: &Method,
    response: reqwest::Response,
) -> Result<Response, S3Error> {
    let status = response.status();
    if status.is_success() || status == StatusCode::NOT_MODIFIED {
        return Ok(success_response(method, response));
    }

    let headers = response.headers().clone();
    let bytes = collect_limited(response, 1024 * 1024).await;
    match bytes {
        Some(bytes) if contains_error_xml(&bytes) => {
            let mut output = Response::new(Body::from(bytes));
            *output.status_mut() = status;
            copy_response_headers(&headers, output.headers_mut());
            output
                .headers_mut()
                .entry(header::CONTENT_TYPE)
                .or_insert_with(|| HeaderValue::from_static("application/xml"));
            Ok(output)
        }
        _ => Err(S3Error::from_status(status)),
    }
}

fn success_response(method: &Method, response: reqwest::Response) -> Response {
    let status = response.status();
    let headers = response.headers().clone();
    let body = if method == Method::HEAD
        || matches!(status, StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED)
    {
        Body::empty()
    } else {
        Body::from_stream(
            response
                .bytes_stream()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string())),
        )
    };
    let mut output = Response::new(body);
    *output.status_mut() = status;
    copy_response_headers(&headers, output.headers_mut());
    output
}

fn copy_response_headers(source: &HeaderMap, target: &mut HeaderMap) {
    for (name, value) in source {
        if matches!(
            name.as_str(),
            "connection"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
        ) {
            continue;
        }
        target.append(name.clone(), value.clone());
    }
}

fn contains_error_xml(bytes: &Bytes) -> bool {
    bytes
        .windows(6)
        .any(|window| window.eq_ignore_ascii_case(b"<Error"))
}

async fn collect_limited(response: reqwest::Response, limit: usize) -> Option<Bytes> {
    let mut stream = response.bytes_stream();
    let mut buffer = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.ok()?;
        if buffer.len().saturating_add(chunk.len()) > limit {
            return None;
        }
        buffer.extend_from_slice(&chunk);
    }
    Some(buffer.freeze())
}

fn map_storage_error(
    error: StorageError,
    failure_flag: Option<&Arc<std::sync::atomic::AtomicU8>>,
) -> S3Error {
    let kind = error.kind();
    match error {
        StorageError::Sign(error) => error,
        StorageError::Transport(_) => {
            warn!(kind, "upstream request failed");
            match failure_flag.map(|flag| failure(flag)) {
                Some(BodyFailure::PayloadHash) => S3Error::payload_mismatch(),
                Some(BodyFailure::ChunkSignature) => S3Error::signature_mismatch(),
                Some(BodyFailure::ChunkFormat | BodyFailure::DecodedLength) => {
                    S3Error::invalid_request("The request body is not valid.")
                }
                _ => S3Error::service_unavailable(),
            }
        }
    }
}

fn next_request_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        });
    format!("{seed:016x}{counter:08x}")
}

/// Log an accepted configuration warning.
pub fn log_config(config: &Config, path: Option<&std::path::Path>) {
    info!(
        mode = %config.mode,
        cache_mode = %config.cache.mode,
        bind = %config.server.bind,
        config_file = %path.map_or_else(|| "none".to_owned(), |value| value.display().to_string()),
        "configuration loaded"
    );
}
