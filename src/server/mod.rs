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
use crate::auth::{AuthContext, ClientCredential, CredentialStore, PayloadKind, now};
use crate::cache::Cache;
use crate::cache::meta::{CacheMetadata, EntryState};
use crate::config::{CacheMode, Config, Mode, WritePolicy};
use crate::errors::S3Error;
use crate::metrics;
use crate::storage::{ForwardRequest, S3Client, StorageError};

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Shared server state.
pub struct AppState {
    config: Arc<Config>,
    credentials: CredentialStore,
    client: S3Client,
    cache: Option<Arc<Cache>>,
    metrics: Arc<metrics::Metrics>,
    ready: AtomicBool,
}

impl AppState {
    /// Build the shared state.
    ///
    /// # Errors
    ///
    /// Returns an error when an endpoint URL or the cache index is not valid.
    pub fn new(config: Config) -> Result<Self, S3Error> {
        let config = Arc::new(config);
        let client = S3Client::new(&config.origin)?;
        let credentials = CredentialStore::new(&config.clients);
        if credentials.is_empty() {
            warn!("no clients are configured; all S3 requests will be denied");
        }
        let metrics = Arc::new(metrics::Metrics::default());
        metrics
            .cache_max_bytes
            .store(config.cache.max_bytes.unwrap_or(0), Ordering::Relaxed);
        metrics.cache_unlimited.store(
            u64::from(config.cache.max_bytes == Some(0) || config.cache.mode == CacheMode::Off),
            Ordering::Relaxed,
        );
        let cache = Cache::new(Arc::clone(&config), Arc::clone(&metrics))?;
        Ok(Self {
            config,
            credentials,
            client,
            cache,
            metrics,
            ready: AtomicBool::new(false),
        })
    }

    /// Start cache recovery and background workers.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be read.
    pub async fn start(&self) -> Result<(), S3Error> {
        if let Some(cache) = &self.cache {
            cache.start().await.map_err(|error| {
                S3Error::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    &format!("cache recovery failed: {error}"),
                )
            })?;
        }
        Ok(())
    }

    /// Return the configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Return the cache, when one is configured.
    #[must_use]
    pub fn cache(&self) -> Option<&Arc<Cache>> {
        self.cache.as_ref()
    }

    /// Return the metrics.
    #[must_use]
    pub fn metrics(&self) -> &Arc<metrics::Metrics> {
        &self.metrics
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
        .route("/metrics", get(metrics_endpoint))
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

/// Return the Prometheus metrics response.
pub async fn metrics_endpoint(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (pending, lag) = state
        .cache
        .as_ref()
        .map_or((0, 0), |cache| cache.pending_stats());
    let tracked = state
        .cache
        .as_ref()
        .map_or(0, |cache| cache.tracked_bytes());
    state.metrics.set_tracked(tracked);
    let text = metrics::render(&state.metrics, pending, lag);
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text)
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
    state
        .start()
        .await
        .map_err(|error| ServeError::Config(error.message))?;
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

#[allow(clippy::too_many_lines)]
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
    let query = uri.query().unwrap_or_default();
    let multipart =
        query.contains("uploads") || query.contains("uploadId") || query.contains("partNumber");
    let copy = headers.contains_key("x-amz-copy-source");
    let object = !key.is_empty();

    let Some(cache) = state.cache.as_ref() else {
        return proxy_request(state, method, &credential, prepared, path_and_query).await;
    };

    if !object || multipart {
        let response = proxy_request(state, method, &credential, prepared, path_and_query).await?;
        if method == Method::POST && query.contains("uploadId") && response.status().is_success() {
            cache.invalidate(&bucket, &key).await;
        }
        return Ok(response);
    }

    if copy {
        let response = proxy_request(state, method, &credential, prepared, path_and_query).await?;
        if response.status().is_success() {
            cache.invalidate(&bucket, &key).await;
        }
        return Ok(response);
    }

    match method {
        &Method::GET | &Method::HEAD => {
            read_path(
                state,
                cache,
                method,
                headers,
                &credential,
                prepared,
                &bucket,
                &key,
                &path_and_query,
            )
            .await
        }
        &Method::PUT => {
            if cache.writes_cache() {
                write_put(
                    state,
                    cache,
                    &credential,
                    headers,
                    prepared,
                    &bucket,
                    &key,
                    &path_and_query,
                )
                .await
            } else {
                let response =
                    proxy_request(state, method, &credential, prepared, path_and_query).await?;
                if response.status().is_success() {
                    cache.invalidate(&bucket, &key).await;
                }
                Ok(response)
            }
        }
        &Method::DELETE => {
            if cache.writes_cache() {
                write_delete(
                    state,
                    cache,
                    &credential,
                    method,
                    prepared,
                    &bucket,
                    &key,
                    &path_and_query,
                )
                .await
            } else {
                let response =
                    proxy_request(state, method, &credential, prepared, path_and_query).await?;
                if response.status().is_success() {
                    cache.invalidate(&bucket, &key).await;
                }
                Ok(response)
            }
        }
        _ => proxy_request(state, method, &credential, prepared, path_and_query).await,
    }
}

async fn proxy_request(
    state: &AppState,
    method: &Method,
    credential: &Arc<ClientCredential>,
    prepared: PreparedRequest,
    path_and_query: String,
) -> Result<Response, S3Error> {
    let failure_flag = prepared.failure.clone();
    let request = ForwardRequest {
        method: method.clone(),
        path_and_query,
        headers: prepared.headers,
        body: prepared.body,
        content_length: prepared.content_length,
    };
    let result = state.client.execute(credential, request).await;
    let response = result.map_err(|error| map_storage_error(error, failure_flag.as_ref()))?;
    forward_response(method, response).await
}

// The read path needs the request context and the cache policy.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn read_path(
    state: &AppState,
    cache: &Arc<Cache>,
    method: &Method,
    headers: &HeaderMap,
    credential: &Arc<ClientCredential>,
    prepared: PreparedRequest,
    bucket: &str,
    key: &str,
    path_and_query: &str,
) -> Result<Response, S3Error> {
    if cache.is_tombstoned(bucket, key) {
        return Err(S3Error::new(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "The key does not exist.",
        ));
    }

    let origin_headers = prepared.headers.clone();
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    if let Some(meta) = cache.pending_entry(bucket, key) {
        match serve_cached(state, cache, method, range.as_deref(), &meta, bucket, key).await {
            Ok(Some(response)) => return Ok(response),
            Ok(None) => {}
            Err(error) => {
                if !cache_error_fallback(cache, &error) {
                    return Err(cache_error(&error));
                }
            }
        }
    }

    if cache.mode() == CacheMode::Write {
        return origin_read(
            state,
            cache,
            method,
            &origin_headers,
            credential,
            bucket,
            key,
            path_and_query,
            None,
        )
        .await;
    }

    let meta = match cache.read_meta(bucket, key).await {
        Ok(meta) => meta,
        Err(error) => {
            if !cache_error_fallback(cache, &error) {
                return Err(cache_error(&error));
            }
            state.metrics.fallback();
            None
        }
    };

    let Some(meta) = meta else {
        state.metrics.miss();
        return origin_read(
            state,
            cache,
            method,
            &origin_headers,
            credential,
            bucket,
            key,
            path_and_query,
            None,
        )
        .await;
    };

    if meta.state == EntryState::Failed && !cache.serve_quarantined() {
        return Err(S3Error::new(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "The key does not exist.",
        ));
    }

    let ttl = cache.ttl_for(bucket);
    if meta.is_fresh(unix_now(), ttl) || meta.state == EntryState::Failed {
        match serve_cached(state, cache, method, range.as_deref(), &meta, bucket, key).await {
            Ok(Some(response)) => return Ok(response),
            Ok(None) => {}
            Err(error) => {
                if !cache_error_fallback(cache, &error) {
                    return Err(cache_error(&error));
                }
                state.metrics.fallback();
            }
        }
    } else if range.is_some() {
        // A range is only served from a fresh complete entry. A stale entry
        // passes the range to origin and does not cache the partial body.
        state.metrics.miss();
        return origin_read(
            state,
            cache,
            method,
            &origin_headers,
            credential,
            bucket,
            key,
            path_and_query,
            None,
        )
        .await;
    } else {
        let response = origin_read(
            state,
            cache,
            method,
            &origin_headers,
            credential,
            bucket,
            key,
            path_and_query,
            Some(&meta),
        )
        .await?;
        return Ok(response);
    }

    origin_read(
        state,
        cache,
        method,
        &origin_headers,
        credential,
        bucket,
        key,
        path_and_query,
        None,
    )
    .await
}

// The read path passes request headers, credentials, and cache metadata.
#[allow(clippy::too_many_arguments)]
async fn origin_read(
    state: &AppState,
    cache: &Arc<Cache>,
    method: &Method,
    headers: &HeaderMap,
    credential: &Arc<ClientCredential>,
    bucket: &str,
    key: &str,
    path_and_query: &str,
    revalidate: Option<&CacheMetadata>,
) -> Result<Response, S3Error> {
    let mut request_headers = headers.clone();
    if let Some(meta) = revalidate
        && !meta.etag.is_empty()
        && let Ok(value) = HeaderValue::from_str(&meta.etag)
    {
        request_headers.insert(header::IF_NONE_MATCH, value);
    }
    let request = ForwardRequest {
        method: method.clone(),
        path_and_query: path_and_query.to_owned(),
        headers: request_headers,
        body: None,
        content_length: None,
    };
    let response = state
        .client
        .execute(credential, request)
        .await
        .map_err(|error| map_storage_error(error, None))?;

    if revalidate.is_some() && response.status() == StatusCode::NOT_MODIFIED {
        state.metrics.revalidated();
        if let Some(mut meta) = revalidate.cloned() {
            meta.refresh(unix_now());
            if let Err(error) = cache.store_meta(bucket, key, &meta).await {
                warn!(%error, "cache metadata refresh failed");
            }
            let range = headers
                .get(header::RANGE)
                .and_then(|value| value.to_str().ok());
            if let Ok(Some(response)) =
                serve_cached(state, cache, method, range, &meta, bucket, key).await
            {
                return Ok(response);
            }
            cache.invalidate(bucket, key).await;
            return Box::pin(origin_read(
                state,
                cache,
                method,
                headers,
                credential,
                bucket,
                key,
                path_and_query,
                None,
            ))
            .await;
        }
    }

    if response.status() == StatusCode::NOT_FOUND {
        cache.invalidate(bucket, key).await;
        return forward_response(method, response).await;
    }

    if !response.status().is_success() {
        return forward_response(method, response).await;
    }

    let size = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let eligible = cache.reads_cache()
        && method == Method::GET
        && headers.get(header::RANGE).is_none()
        && size.is_some_and(|size| size <= cache.max_object_bytes());

    if eligible {
        let meta = Cache::metadata_from_response(bucket, key, &response);
        if meta.size > 0 {
            state
                .metrics
                .origin_bytes
                .fetch_add(meta.size, Ordering::Relaxed);
            let size = meta.size;
            let body = cache.tee_response(response, meta);
            return Ok(response_from_stream(body, StatusCode::OK, Some(size)));
        }
    }

    if let Some(size) = size {
        metrics::Metrics::add_bytes(&state.metrics.origin_bytes, size);
    }
    if method == Method::HEAD && revalidate.is_some() {
        cache.invalidate(bucket, key).await;
    }
    forward_response(method, response).await
}

#[allow(clippy::too_many_arguments)]
async fn serve_cached(
    state: &AppState,
    cache: &Arc<Cache>,
    method: &Method,
    range: Option<&str>,
    meta: &CacheMetadata,
    bucket: &str,
    key: &str,
) -> Result<Option<Response>, crate::cache::CacheError> {
    if meta.size > cache.max_object_bytes() {
        return Ok(None);
    }
    if method == Method::HEAD {
        state.metrics.hit();
        cache.touch(bucket, key);
        return Ok(Some(metadata_response(meta)));
    }
    let Some(response) = cache.get_blob(bucket, key, range).await? else {
        cache.invalidate(bucket, key).await;
        return Ok(None);
    };
    let status = if range.is_some() && response.status() == StatusCode::PARTIAL_CONTENT {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let cache_headers = response.headers().clone();
    let mut output =
        Response::new(Body::from_stream(response.bytes_stream().map_err(
            |error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()),
        )));
    *output.status_mut() = status;
    copy_cache_headers(&cache_headers, output.headers_mut());
    set_meta_headers(meta, output.headers_mut());
    if status == StatusCode::OK
        && let Ok(value) = HeaderValue::from_str(&meta.size.to_string())
    {
        output.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    state.metrics.hit();
    metrics::Metrics::add_bytes(&state.metrics.bytes_served, meta.size);
    cache.touch(bucket, key);
    Ok(Some(output))
}

fn metadata_response(meta: &CacheMetadata) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    set_meta_headers(meta, response.headers_mut());
    if let Ok(value) = HeaderValue::from_str(&meta.size.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    response
}

fn set_meta_headers(meta: &CacheMetadata, headers: &mut HeaderMap) {
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if !meta.etag.is_empty()
        && let Ok(value) = HeaderValue::from_str(&meta.etag)
    {
        headers.insert(header::ETAG, value);
    }
    if let Some(value) = &meta.last_modified
        && let Ok(value) = HeaderValue::from_str(value)
    {
        headers.insert(header::LAST_MODIFIED, value);
    }
    if let Some(value) = &meta.content_type
        && let Ok(value) = HeaderValue::from_str(value)
    {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Some(value) = &meta.content_encoding
        && let Ok(value) = HeaderValue::from_str(value)
    {
        headers.insert(header::CONTENT_ENCODING, value);
    }
}

fn copy_cache_headers(source: &HeaderMap, target: &mut HeaderMap) {
    for name in [
        header::CONTENT_RANGE,
        header::CONTENT_LENGTH,
        header::ACCEPT_RANGES,
    ] {
        if let Some(value) = source.get(&name) {
            target.insert(name, value.clone());
        }
    }
}

fn response_from_stream(body: BodyStream, status: StatusCode, size: Option<u64>) -> Response {
    let mut response = Response::new(Body::from_stream(body));
    *response.status_mut() = status;
    if let Some(size) = size
        && let Ok(value) = HeaderValue::from_str(&size.to_string())
    {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    response
}

// The write path needs the request context and the cache policy.
#[allow(clippy::too_many_arguments)]
async fn write_put(
    state: &AppState,
    cache: &Arc<Cache>,
    credential: &Arc<ClientCredential>,
    headers: &HeaderMap,
    prepared: PreparedRequest,
    bucket: &str,
    key: &str,
    path_and_query: &str,
) -> Result<Response, S3Error> {
    let policy = cache.write_policy_for(bucket);
    if policy == WritePolicy::WriteThrough {
        let response = proxy_request(
            state,
            &Method::PUT,
            credential,
            prepared,
            path_and_query.to_owned(),
        )
        .await?;
        if response.status().is_success() {
            cache.spawn_fill_from_origin(bucket.to_owned(), key.to_owned());
        }
        return Ok(response);
    }

    let Some(length) = prepared.content_length else {
        // A body without a length cannot be replayed by the sync worker.
        let response = proxy_request(
            state,
            &Method::PUT,
            credential,
            prepared,
            path_and_query.to_owned(),
        )
        .await?;
        if response.status().is_success() {
            cache.invalidate(bucket, key).await;
        }
        return Ok(response);
    };

    if length > cache.max_object_bytes() {
        let response = proxy_request(
            state,
            &Method::PUT,
            credential,
            prepared,
            path_and_query.to_owned(),
        )
        .await?;
        if response.status().is_success() {
            cache.invalidate(bucket, key).await;
        }
        return Ok(response);
    }

    if let Err(error) = cache.probe(bucket).await {
        if cache.on_cache_error() == crate::config::OnCacheError::Origin {
            state.metrics.fallback();
            warn!(%error, "cache probe failed; writing to origin");
            let response = proxy_request(
                state,
                &Method::PUT,
                credential,
                prepared,
                path_and_query.to_owned(),
            )
            .await?;
            return Ok(response);
        }
        return Err(cache_error(&error));
    }

    let content_type = header_string(&prepared.headers, header::CONTENT_TYPE);
    let content_encoding = header_string(&prepared.headers, header::CONTENT_ENCODING);
    let failure_flag = prepared.failure.clone();
    let body = prepared.body.unwrap_or_else(empty_body);
    let body_started = Arc::new(AtomicBool::new(false));
    let meta = match cache
        .stage_put(
            bucket,
            key,
            body,
            length,
            content_type,
            content_encoding,
            body_started,
        )
        .await
    {
        Ok(meta) => meta,
        Err(error) => {
            if let Some(body_error) = body_failure_error(failure_flag.as_ref()) {
                return Err(body_error);
            }
            return Err(cache_error(&error));
        }
    };
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    if let Ok(value) = HeaderValue::from_str(&meta.etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    let _ = headers;
    Ok(response)
}

// The delete path needs the request context and the cache policy.
#[allow(clippy::too_many_arguments)]
async fn write_delete(
    state: &AppState,
    cache: &Arc<Cache>,
    credential: &Arc<ClientCredential>,
    method: &Method,
    prepared: PreparedRequest,
    bucket: &str,
    key: &str,
    path_and_query: &str,
) -> Result<Response, S3Error> {
    if cache.pending_entry(bucket, key).is_some() {
        cache
            .delete_pending(bucket, key)
            .await
            .map_err(|error| cache_error(&error))?;
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::OK;
        return Ok(response);
    }
    match cache.stage_delete(bucket, key).await {
        Ok(()) => {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::OK;
            Ok(response)
        }
        Err(error) => {
            if cache.on_cache_error() == crate::config::OnCacheError::Origin {
                state.metrics.fallback();
                warn!(%error, "cache delete failed; deleting at origin");
                proxy_request(
                    state,
                    method,
                    credential,
                    prepared,
                    path_and_query.to_owned(),
                )
                .await
            } else {
                Err(cache_error(&error))
            }
        }
    }
}

fn header_string(headers: &HeaderMap, name: header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn empty_body() -> BodyStream {
    let stream = async_stream::try_stream! {
        if false {
            yield Bytes::new();
        }
    };
    Box::pin(stream)
}

fn cache_error_fallback(cache: &Cache, error: &crate::cache::CacheError) -> bool {
    cache.on_cache_error() == crate::config::OnCacheError::Origin && error.is_unavailable()
}

fn cache_error(error: &crate::cache::CacheError) -> S3Error {
    warn!(%error, "cache request failed");
    S3Error::service_unavailable()
}

fn unix_now() -> i64 {
    crate::cache::meta::unix_now()
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
            body_failure_error(failure_flag).unwrap_or_else(S3Error::service_unavailable)
        }
    }
}

fn body_failure_error(failure_flag: Option<&Arc<std::sync::atomic::AtomicU8>>) -> Option<S3Error> {
    match failure_flag.map(|flag| failure(flag)) {
        Some(BodyFailure::PayloadHash) => Some(S3Error::payload_mismatch()),
        Some(BodyFailure::ChunkSignature) => Some(S3Error::signature_mismatch()),
        Some(BodyFailure::ChunkFormat | BodyFailure::DecodedLength) => {
            Some(S3Error::invalid_request("The request body is not valid."))
        }
        _ => None,
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
