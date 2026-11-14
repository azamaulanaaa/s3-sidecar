#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Cache-mode integration tests with an in-process fake origin and fake cache.

mod support;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use reqwest::Url;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, CredentialStore, EMPTY_SHA256};
use s3_sidecar::cache::keys::{blob_key, journal_key, object_path};
use s3_sidecar::cache::meta::{CacheMetadata, EntryState, JournalEntry, unix_now};
use s3_sidecar::config::duration::HumanDuration;
use s3_sidecar::config::secret::SecretString;
use s3_sidecar::config::{
    BucketConfig, CacheConfig, CacheMode, ClientConfig, Config, EndpointConfig, Mode, OnCacheError,
    WritePolicy,
};
use s3_sidecar::server::{AppState, router};
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
struct Stored {
    body: Bytes,
    etag: String,
    last_modified: String,
    content_type: Option<String>,
    content_encoding: Option<String>,
}

#[derive(Default)]
struct Counts {
    get: AtomicU64,
    put: AtomicU64,
    delete: AtomicU64,
    get_bytes: AtomicU64,
}

#[derive(Clone)]
struct OriginState {
    objects: Arc<Mutex<HashMap<String, Stored>>>,
    counts: Arc<Counts>,
    gate: Arc<Semaphore>,
    put_status: Arc<Mutex<Option<StatusCode>>>,
    put_headers: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
}

#[derive(Clone)]
struct CacheState {
    objects: Arc<Mutex<HashMap<String, Stored>>>,
    put_status: Arc<Mutex<Option<StatusCode>>>,
    uploads: CacheUploads,
    next_upload: Arc<AtomicU64>,
}

type CacheUploads = Arc<Mutex<HashMap<String, (String, std::collections::BTreeMap<u32, Bytes>)>>>;

#[derive(Clone)]
struct OriginHandle {
    objects: Arc<Mutex<HashMap<String, Stored>>>,
    counts: Arc<Counts>,
    gate: Arc<Semaphore>,
    put_status: Arc<Mutex<Option<StatusCode>>>,
    put_headers: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
}

#[derive(Clone)]
struct CacheHandle {
    objects: Arc<Mutex<HashMap<String, Stored>>>,
    put_status: Arc<Mutex<Option<StatusCode>>>,
}

impl OriginHandle {
    fn put(&self, key: &str, body: &[u8], etag: &str) {
        self.objects.lock().unwrap().insert(
            key.to_owned(),
            Stored {
                body: Bytes::copy_from_slice(body),
                etag: etag.to_owned(),
                last_modified: "Tue, 24 May 2013 00:00:00 GMT".to_owned(),
                content_type: Some("text/plain".to_owned()),
                content_encoding: None,
            },
        );
    }

    fn body(&self, key: &str) -> Option<Vec<u8>> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .map(|stored| stored.body.to_vec())
    }

    fn get_count(&self) -> u64 {
        self.counts.get.load(Ordering::Relaxed)
    }

    fn put_count(&self) -> u64 {
        self.counts.put.load(Ordering::Relaxed)
    }

    fn delete_count(&self) -> u64 {
        self.counts.delete.load(Ordering::Relaxed)
    }

    fn block_puts(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.gate.clone().try_acquire_owned().unwrap()
    }

    fn release_puts(&self) {
        self.gate.add_permits(1);
    }

    fn put_headers(&self, key: &str) -> Option<HashMap<String, String>> {
        self.put_headers.lock().unwrap().get(key).cloned()
    }
}

impl CacheHandle {
    fn keys(&self) -> Vec<String> {
        self.objects.lock().unwrap().keys().cloned().collect()
    }

    fn has(&self, key: &str) -> bool {
        self.objects.lock().unwrap().contains_key(key)
    }

    fn fail_puts(&self) {
        *self.put_status.lock().unwrap() = Some(StatusCode::INTERNAL_SERVER_ERROR);
    }
}

async fn spawn_origin() -> (String, OriginHandle) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let objects = Arc::new(Mutex::new(HashMap::new()));
    let counts = Arc::new(Counts::default());
    let gate = Arc::new(Semaphore::new(1));
    let put_status = Arc::new(Mutex::new(None));
    let put_headers = Arc::new(Mutex::new(HashMap::new()));
    let state = OriginState {
        objects: Arc::clone(&objects),
        counts: Arc::clone(&counts),
        gate: Arc::clone(&gate),
        put_status: Arc::clone(&put_status),
        put_headers: Arc::clone(&put_headers),
    };
    let app = Router::new().fallback(origin_handler).with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        format!("http://{address}"),
        OriginHandle {
            objects,
            counts,
            gate,
            put_status,
            put_headers,
        },
    )
}

#[allow(clippy::too_many_lines)]
async fn origin_handler(
    State(state): State<OriginState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (bucket, key) = split_path(uri.path());
    if key.is_empty() {
        return xml(
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchKey</Code></Error>",
        );
    }
    let object_key = format!("{bucket}/{key}");
    match method {
        Method::GET | Method::HEAD => {
            state.counts.get.fetch_add(1, Ordering::Relaxed);
            state
                .counts
                .get_bytes
                .fetch_add(u64::try_from(body.len()).unwrap_or(0), Ordering::Relaxed);
            let objects = state.objects.lock().unwrap();
            let Some(stored) = objects.get(&object_key) else {
                return xml(
                    StatusCode::NOT_FOUND,
                    "<Error><Code>NoSuchKey</Code></Error>",
                );
            };
            if let Some(value) = headers.get(header::IF_NONE_MATCH)
                && value.to_str().ok() == Some(stored.etag.as_str())
            {
                return Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .body(Body::empty())
                    .unwrap();
            }
            let body = if method == Method::HEAD {
                Body::empty()
            } else {
                Body::from(stored.body.clone())
            };
            let mut response = Response::new(body);
            response
                .headers_mut()
                .insert(header::ETAG, HeaderValue::from_str(&stored.etag).unwrap());
            response.headers_mut().insert(
                header::LAST_MODIFIED,
                HeaderValue::from_str(&stored.last_modified).unwrap(),
            );
            if let Some(value) = &stored.content_type {
                response
                    .headers_mut()
                    .insert(header::CONTENT_TYPE, HeaderValue::from_str(value).unwrap());
            }
            if let Some(value) = &stored.content_encoding {
                response.headers_mut().insert(
                    header::CONTENT_ENCODING,
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&stored.body.len().to_string()).unwrap(),
            );
            response
        }
        Method::PUT => {
            let _permit = state.gate.acquire().await.unwrap();
            state.counts.put.fetch_add(1, Ordering::Relaxed);
            if let Some(status) = *state.put_status.lock().unwrap() {
                return xml(status, "<Error><Code>AccessDenied</Code></Error>");
            }
            let etag = format!("\"etag-{}\"", state.counts.put.load(Ordering::Relaxed));
            let recorded: HashMap<String, String> = headers
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.as_str().to_owned(), value.to_owned()))
                })
                .collect();
            state
                .put_headers
                .lock()
                .unwrap()
                .insert(object_key.clone(), recorded);
            state.objects.lock().unwrap().insert(
                object_key,
                Stored {
                    body,
                    etag: etag.clone(),
                    last_modified: "Tue, 24 May 2013 00:00:00 GMT".to_owned(),
                    content_type: headers
                        .get(header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned),
                    content_encoding: headers
                        .get(header::CONTENT_ENCODING)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned),
                },
            );
            let mut response = Response::new(Body::empty());
            response
                .headers_mut()
                .insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
            response
        }
        Method::DELETE => {
            state.counts.delete.fetch_add(1, Ordering::Relaxed);
            state.objects.lock().unwrap().remove(&object_key);
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        _ => xml(
            StatusCode::NOT_IMPLEMENTED,
            "<Error><Code>NotImplemented</Code></Error>",
        ),
    }
}

async fn spawn_cache() -> (String, CacheHandle) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let objects = Arc::new(Mutex::new(HashMap::new()));
    let put_status = Arc::new(Mutex::new(None));
    let state = CacheState {
        objects: Arc::clone(&objects),
        put_status: Arc::clone(&put_status),
        uploads: Arc::new(Mutex::new(HashMap::new())),
        next_upload: Arc::new(AtomicU64::new(0)),
    };
    let app = Router::new().fallback(cache_handler).with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        format!("http://{address}"),
        CacheHandle {
            objects,
            put_status,
        },
    )
}

#[allow(clippy::too_many_lines)]
async fn cache_handler(
    State(state): State<CacheState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (_bucket, key) = split_path(uri.path());
    if key.is_empty() {
        let query = uri.query().unwrap_or_default();
        if query.contains("list-type=2") {
            let prefix = query_value(query, "prefix").unwrap_or_default();
            let prefix = percent_encoding::percent_decode_str(&prefix)
                .decode_utf8_lossy()
                .into_owned();
            let objects = state.objects.lock().unwrap();
            let mut xml_body = String::from("<ListBucketResult>");
            for key in objects.keys() {
                if key.starts_with(&prefix) {
                    xml_body.push_str("<Contents><Key>");
                    xml_body.push_str(&escape_xml(key));
                    xml_body.push_str("</Key></Contents>");
                }
            }
            xml_body.push_str("</ListBucketResult>");
            return xml(StatusCode::OK, &xml_body);
        }
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap();
    }
    let query = uri.query().unwrap_or_default();
    if method == Method::POST && query.split('&').any(|pair| pair == "uploads") {
        let id = state.next_upload.fetch_add(1, Ordering::Relaxed);
        let upload_id = format!("cache-upload-{id}");
        state
            .uploads
            .lock()
            .unwrap()
            .insert(upload_id.clone(), (key, std::collections::BTreeMap::new()));
        return xml(
            StatusCode::OK,
            &format!(
                "<InitiateMultipartUploadResult><UploadId>{upload_id}</UploadId>\
                 </InitiateMultipartUploadResult>"
            ),
        );
    }
    if method == Method::PUT && query.contains("partNumber=") {
        let upload_id = query_value(query, "uploadId").unwrap_or_default();
        let number = query_value(query, "partNumber")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let mut uploads = state.uploads.lock().unwrap();
        let Some((_, parts)) = uploads.get_mut(&upload_id) else {
            return xml(
                StatusCode::NOT_FOUND,
                "<Error><Code>NoSuchUpload</Code></Error>",
            );
        };
        parts.insert(number, body);
        drop(uploads);
        let mut response = Response::new(Body::empty());
        response
            .headers_mut()
            .insert(header::ETAG, HeaderValue::from_static("\"part-etag\""));
        return response;
    }
    if method == Method::POST && query.contains("uploadId=") {
        let upload_id = query_value(query, "uploadId").unwrap_or_default();
        let Some((key, parts)) = state.uploads.lock().unwrap().remove(&upload_id) else {
            return xml(
                StatusCode::NOT_FOUND,
                "<Error><Code>NoSuchUpload</Code></Error>",
            );
        };
        let mut bytes = Vec::new();
        for part in parts.values() {
            bytes.extend_from_slice(part);
        }
        let body = Bytes::from(bytes);
        let etag = format!("\"{}\"", body.len());
        state.objects.lock().unwrap().insert(
            key,
            Stored {
                body,
                etag: etag.clone(),
                last_modified: "Tue, 24 May 2013 00:00:00 GMT".to_owned(),
                content_type: None,
                content_encoding: None,
            },
        );
        return xml(
            StatusCode::OK,
            &format!(
                "<CompleteMultipartUploadResult><ETag>{etag}</ETag>\
                 </CompleteMultipartUploadResult>"
            ),
        );
    }
    if method == Method::DELETE && query.contains("uploadId=") {
        if let Some(upload_id) = query_value(query, "uploadId") {
            state.uploads.lock().unwrap().remove(&upload_id);
        }
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap();
    }
    match method {
        Method::GET | Method::HEAD => {
            let objects = state.objects.lock().unwrap();
            let Some(stored) = objects.get(&key) else {
                return xml(
                    StatusCode::NOT_FOUND,
                    "<Error><Code>NoSuchKey</Code></Error>",
                );
            };
            let body = if method == Method::HEAD {
                Body::empty()
            } else {
                Body::from(stored.body.clone())
            };
            let mut response = Response::new(body);
            response
                .headers_mut()
                .insert(header::ETAG, HeaderValue::from_str(&stored.etag).unwrap());
            response.headers_mut().insert(
                header::LAST_MODIFIED,
                HeaderValue::from_str(&stored.last_modified).unwrap(),
            );
            if let Some(value) = &stored.content_type {
                response
                    .headers_mut()
                    .insert(header::CONTENT_TYPE, HeaderValue::from_str(value).unwrap());
            }
            if let Some(value) = &stored.content_encoding {
                response.headers_mut().insert(
                    header::CONTENT_ENCODING,
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&stored.body.len().to_string()).unwrap(),
            );
            response
        }
        Method::PUT => {
            if let Some(status) = *state.put_status.lock().unwrap() {
                return xml(status, "<Error><Code>InternalError</Code></Error>");
            }
            let etag = format!("\"{}\"", body.len());
            let stored = Stored {
                body,
                etag: etag.clone(),
                last_modified: "Tue, 24 May 2013 00:00:00 GMT".to_owned(),
                content_type: headers
                    .get(header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
                content_encoding: headers
                    .get(header::CONTENT_ENCODING)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
            };
            state.objects.lock().unwrap().insert(key, stored);
            let mut response = Response::new(Body::empty());
            response
                .headers_mut()
                .insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
            response
        }
        Method::DELETE => {
            state.objects.lock().unwrap().remove(&key);
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        _ => xml(
            StatusCode::NOT_IMPLEMENTED,
            "<Error><Code>NotImplemented</Code></Error>",
        ),
    }
}

fn split_path(path: &str) -> (String, String) {
    let trimmed = path.trim_start_matches('/');
    match trimmed.split_once('/') {
        Some((bucket, key)) => (
            percent_encoding::percent_decode_str(bucket)
                .decode_utf8_lossy()
                .into_owned(),
            percent_encoding::percent_decode_str(key)
                .decode_utf8_lossy()
                .into_owned(),
        ),
        None => (
            percent_encoding::percent_decode_str(trimmed)
                .decode_utf8_lossy()
                .into_owned(),
            String::new(),
        ),
    }
}

fn query_value(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == name).then(|| value.to_owned())
    })
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn xml(status: StatusCode, body: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/xml")],
        body.to_owned(),
    )
        .into_response()
}

fn client_config() -> ClientConfig {
    ClientConfig {
        access_key_id: "AKIA".to_owned(),
        secret_access_key: SecretString::new("secret"),
        session_token: None,
        buckets: vec!["media".to_owned()],
    }
}

#[allow(clippy::too_many_arguments)]
fn config(
    origin: String,
    cache: String,
    cache_mode: CacheMode,
    max_bytes: u64,
    ttl_seconds: u64,
    write_policy: WritePolicy,
    on_cache_error: OnCacheError,
    index_path: &std::path::Path,
) -> Config {
    Config {
        mode: Mode::Cache,
        cache: CacheConfig {
            endpoint: cache,
            region: "us-east-1".to_owned(),
            access_key_id: "cache".to_owned(),
            secret_access_key: SecretString::new("cache-secret"),
            session_token: None,
            force_path_style: true,
            bucket: "cachebucket".to_owned(),
            prefix: "cache".to_owned(),
            mode: cache_mode,
            max_bytes: Some(max_bytes),
            max_object_bytes: 1_048_576,
            write_policy,
            on_cache_error,
            high_watermark: 0.9,
            low_watermark: 0.8,
            staging_ttl: HumanDuration::new(Duration::from_secs(3_600)),
            orphan_ttl: HumanDuration::new(Duration::from_hours(24)),
            sync_workers: 2,
            sync_max_attempts: 10,
            sync_retry_base_ms: 50,
            sync_debounce_ms: 10,
            gc_interval: HumanDuration::new(Duration::from_secs(3_600)),
            index_path: index_path.display().to_string(),
            serve_quarantined: false,
        },
        origin: EndpointConfig {
            endpoint: origin,
            bucket: String::new(),
            region: "us-east-1".to_owned(),
            access_key_id: "origin".to_owned(),
            secret_access_key: SecretString::new("origin-secret"),
            session_token: None,
            force_path_style: true,
        },
        clients: vec![client_config()],
        buckets: vec![BucketConfig {
            name: "media".to_owned(),
            ttl: Some(HumanDuration::new(Duration::from_secs(ttl_seconds))),
            ..BucketConfig::default()
        }],
        ..Config::default()
    }
}

struct Sidecar {
    url: String,
    _dir: tempfile::TempDir,
}

async fn spawn_sidecar(config: Config) -> Sidecar {
    s3_sidecar::observability::init();
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(AppState::new(config).unwrap());
    state.start().await.unwrap();
    state.set_ready(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = router(Arc::clone(&state));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Sidecar {
        url: format!("http://{address}"),
        _dir: dir,
    }
}

async fn spawn_sidecar_with_metrics(config: Config, metrics: &support::TestMetrics) -> Sidecar {
    s3_sidecar::observability::init();
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(
        AppState::new_with_metrics(config, Arc::clone(&metrics.metrics), None, None).unwrap(),
    );
    state.start().await.unwrap();
    state.set_ready(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = router(Arc::clone(&state));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Sidecar {
        url: format!("http://{address}"),
        _dir: dir,
    }
}

fn credential() -> Arc<ClientCredential> {
    CredentialStore::new(&[client_config()])
        .lookup("AKIA")
        .unwrap()
}

fn signed_request(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    body: Option<Vec<u8>>,
) -> reqwest::RequestBuilder {
    signed_request_with_headers(client, method, url, body, &[])
}

fn signed_request_with_headers(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    body: Option<Vec<u8>>,
    extra: &[(&str, &str)],
) -> reqwest::RequestBuilder {
    let parsed = Url::parse(url).unwrap();
    let credential = credential();
    let mut headers = HeaderMap::new();
    for (name, value) in extra {
        headers.insert(
            axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let payload_hash = if body.is_some() {
        "UNSIGNED-PAYLOAD"
    } else {
        EMPTY_SHA256
    };
    sign_request(
        &method,
        &parsed,
        &mut headers,
        payload_hash,
        &credential,
        "us-east-1",
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    let mut builder = client.request(method, url);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    if let Some(body) = body {
        let length = body.len();
        builder = builder.header(header::CONTENT_LENGTH, length).body(body);
    }
    builder
}

async fn body_text(response: reqwest::Response) -> String {
    response.text().await.unwrap()
}

async fn wait_for<F, Fut>(timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition was not met before the timeout"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn repeat_get_is_served_from_cache() {
    let (origin_url, origin) = spawn_origin().await;
    origin.put("media/item", b"hello", "\"etag-1\"");
    let (cache_url, cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::Read,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let first = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/item", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(body_text(first).await, "hello");
    let blob = blob_key("cache", "media", "item");
    wait_for(Duration::from_secs(2), || {
        let cache = cache.clone();
        let blob = blob.clone();
        async move { cache.has(&blob) && cache.has(&format!("{blob}.meta")) }
    })
    .await;
    let second = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/item", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(body_text(second).await, "hello");
    assert_eq!(origin.get_count(), 1);
}

#[tokio::test]
async fn origin_etag_change_is_visible_after_ttl() {
    let (origin_url, origin) = spawn_origin().await;
    origin.put("media/item", b"first", "\"etag-1\"");
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::Read,
        0,
        1,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let first = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/item", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(body_text(first).await, "first");
    origin.put("media/item", b"second", "\"etag-2\"");
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let second = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/item", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(body_text(second).await, "second");
    assert_eq!(origin.get_count(), 2);
}

#[tokio::test]
async fn pending_write_is_readable_before_sync() {
    let (origin_url, origin) = spawn_origin().await;
    let permit = origin.block_puts();
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/pending", sidecar.url),
        Some(b"pending body".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let get = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/pending", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_text(get).await, "pending body");
    assert_eq!(origin.get_count(), 0);
    drop(permit);
    wait_for(Duration::from_secs(3), || {
        let origin = origin.clone();
        async move { origin.body("media/pending").is_some() }
    })
    .await;
    assert_eq!(origin.body("media/pending").unwrap(), b"pending body");
}

#[tokio::test]
async fn crash_after_ack_resumes_on_restart() {
    let (origin_url, origin) = spawn_origin().await;
    let (cache_url, cache) = spawn_cache().await;
    let body = b"recovered body";
    let blob = blob_key("cache", "media", "recovered");
    let journal = journal_key("cache", "crash-id");
    let now = unix_now();
    let meta = CacheMetadata {
        version: 1,
        origin_bucket: "media".to_owned(),
        origin_key: "recovered".to_owned(),
        etag: "\"etag-crash\"".to_owned(),
        last_modified: None,
        validated_at: now,
        size: u64::try_from(body.len()).unwrap(),
        checksum: String::new(),
        content_type: Some("text/plain".to_owned()),
        content_encoding: None,
        headers: std::collections::BTreeMap::new(),
        state: EntryState::Pending,
        created_at: now,
    };
    let entry = JournalEntry::put("crash-id".to_owned(), blob.clone(), &meta);
    seed_cache(&cache_url, "cachebucket", &blob, body).await;
    seed_cache(
        &cache_url,
        "cachebucket",
        &format!("{blob}.meta"),
        serde_json::to_string(&meta).unwrap().as_bytes(),
    )
    .await;
    seed_cache(
        &cache_url,
        "cachebucket",
        &journal,
        serde_json::to_string(&entry).unwrap().as_bytes(),
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let _sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    wait_for(Duration::from_secs(5), || {
        let origin = origin.clone();
        async move { origin.body("media/recovered").as_deref() == Some(body.as_slice()) }
    })
    .await;
    assert_eq!(origin.put_count(), 1);
    wait_for(Duration::from_secs(2), || {
        let cache = cache.clone();
        let journal = journal.clone();
        async move { !cache.has(&journal) }
    })
    .await;
}

async fn seed_cache(base: &str, bucket: &str, key: &str, body: &[u8]) {
    let response = reqwest::Client::new()
        .put(format!("{base}{}", object_path(bucket, key)))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
}

#[tokio::test]
async fn retry_succeeds_after_origin_recovers() {
    let (origin_url, origin) = spawn_origin().await;
    *origin.put_status.lock().unwrap() = Some(StatusCode::SERVICE_UNAVAILABLE);
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/retry", sidecar.url),
        Some(b"retry body".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    tokio::time::sleep(Duration::from_millis(150)).await;
    *origin.put_status.lock().unwrap() = None;
    wait_for(Duration::from_secs(5), || {
        let origin = origin.clone();
        async move { origin.body("media/retry").is_some() }
    })
    .await;
    assert_eq!(origin.body("media/retry").unwrap(), b"retry body");
}

#[tokio::test]
async fn permanent_origin_error_quarantines() {
    let (origin_url, origin) = spawn_origin().await;
    *origin.put_status.lock().unwrap() = Some(StatusCode::FORBIDDEN);
    let (cache_url, cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let telemetry = support::TestMetrics::new();
    let sidecar = spawn_sidecar_with_metrics(
        config(
            origin_url,
            cache_url,
            CacheMode::ReadWrite,
            0,
            3_600,
            WritePolicy::WriteBack,
            OnCacheError::Origin,
            &dir.path().join("index.redb"),
        ),
        &telemetry,
    )
    .await;
    let client = reqwest::Client::new();
    let put = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/quarantine", sidecar.url),
        Some(b"quarantine body".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let blob = blob_key("cache", "media", "quarantine");
    wait_for(Duration::from_secs(3), || {
        let telemetry = telemetry.clone();
        async move { support::counter(&telemetry.collect(), "s3_sidecar.cache.quarantine") >= 1 }
    })
    .await;
    let get = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/quarantine", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
    assert!(cache.has(&blob));
    assert!(cache.keys().iter().any(|key| key.contains("journal")));
}

#[tokio::test]
async fn delete_of_pending_write_makes_no_origin_call() {
    let (origin_url, origin) = spawn_origin().await;
    let _permit = origin.block_puts();
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    );
    config.cache.sync_debounce_ms = 500;
    let sidecar = spawn_sidecar(config).await;
    let client = reqwest::Client::new();
    let put = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/doomed", sidecar.url),
        Some(b"doomed".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let delete = signed_request(
        &client,
        Method::DELETE,
        &format!("{}/media/doomed", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(origin.put_count(), 0);
    assert_eq!(origin.delete_count(), 0);
    let get = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/doomed", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
    origin.release_puts();
}

#[tokio::test]
async fn two_writes_to_one_key_coalesce() {
    let (origin_url, origin) = spawn_origin().await;
    let permit = origin.block_puts();
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    );
    config.cache.sync_debounce_ms = 200;
    let sidecar = spawn_sidecar(config).await;
    let client = reqwest::Client::new();
    for body in [b"first".to_vec(), b"second".to_vec()] {
        let put = signed_request(
            &client,
            Method::PUT,
            &format!("{}/media/coalesce", sidecar.url),
            Some(body),
        )
        .send()
        .await
        .unwrap();
        assert_eq!(put.status(), StatusCode::OK);
    }
    drop(permit);
    wait_for(Duration::from_secs(3), || {
        let origin = origin.clone();
        async move { origin.body("media/coalesce").as_deref() == Some(b"second") }
    })
    .await;
    assert_eq!(origin.body("media/coalesce").unwrap(), b"second");
    assert!(origin.put_count() <= 2);
}

#[tokio::test]
async fn cache_outage_falls_back_to_origin() {
    let (origin_url, origin) = spawn_origin().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let closed = listener.local_addr().unwrap();
    drop(listener);
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        format!("http://{closed}"),
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/fallback", sidecar.url),
        Some(b"fallback body".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(origin.body("media/fallback").unwrap(), b"fallback body");
}

#[tokio::test]
async fn unlimited_cache_never_evicts() {
    let (origin_url, origin) = spawn_origin().await;
    origin.put("media/one", &[b'a'; 60], "\"etag-1\"");
    origin.put("media/two", &[b'b'; 60], "\"etag-2\"");
    let (cache_url, cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let telemetry = support::TestMetrics::new();
    let sidecar = spawn_sidecar_with_metrics(
        config(
            origin_url,
            cache_url,
            CacheMode::Read,
            0,
            3_600,
            WritePolicy::WriteBack,
            OnCacheError::Origin,
            &dir.path().join("index.redb"),
        ),
        &telemetry,
    )
    .await;
    let client = reqwest::Client::new();
    for key in ["one", "two"] {
        let get = signed_request(
            &client,
            Method::GET,
            &format!("{}/media/{key}", sidecar.url),
            None,
        )
        .send()
        .await
        .unwrap();
        assert_eq!(get.status(), StatusCode::OK);
    }
    let blob_one = blob_key("cache", "media", "one");
    let blob_two = blob_key("cache", "media", "two");
    wait_for(Duration::from_secs(2), || {
        let cache = cache.clone();
        let one = blob_one.clone();
        let two = blob_two.clone();
        async move { cache.has(&one) && cache.has(&two) }
    })
    .await;
    let metrics = telemetry.collect();
    assert_eq!(support::counter(&metrics, "s3_sidecar.cache.evictions"), 0);
    assert_eq!(support::gauge(&metrics, "s3_sidecar.cache.unlimited"), 1);
    assert_eq!(support::gauge(&metrics, "s3_sidecar.cache.max_bytes"), 0);
}

#[tokio::test]
async fn small_cap_evicts_to_low_watermark() {
    let (origin_url, origin) = spawn_origin().await;
    origin.put("media/one", &[b'a'; 60], "\"etag-1\"");
    origin.put("media/two", &[b'b'; 60], "\"etag-2\"");
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let telemetry = support::TestMetrics::new();
    let sidecar = spawn_sidecar_with_metrics(
        config(
            origin_url,
            cache_url,
            CacheMode::Read,
            100,
            3_600,
            WritePolicy::WriteBack,
            OnCacheError::Origin,
            &dir.path().join("index.redb"),
        ),
        &telemetry,
    )
    .await;
    let client = reqwest::Client::new();
    for key in ["one", "two"] {
        let get = signed_request(
            &client,
            Method::GET,
            &format!("{}/media/{key}", sidecar.url),
            None,
        )
        .send()
        .await
        .unwrap();
        assert_eq!(get.status(), StatusCode::OK);
    }
    wait_for(Duration::from_secs(3), || {
        let telemetry = telemetry.clone();
        async move { support::counter(&telemetry.collect(), "s3_sidecar.cache.evictions") >= 1 }
    })
    .await;
    let metrics = telemetry.collect();
    let tracked = support::gauge(&metrics, "s3_sidecar.cache.tracked_bytes");
    assert!(tracked <= 80, "tracked bytes: {tracked}");
}

#[tokio::test]
async fn read_mode_put_invalidates_the_cache() {
    let (origin_url, origin) = spawn_origin().await;
    origin.put("media/item", b"old", "\"etag-1\"");
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::Read,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let first = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/item", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(body_text(first).await, "old");
    let put = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/item", sidecar.url),
        Some(b"new".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let second = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/item", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(body_text(second).await, "new");
    assert_eq!(origin.get_count(), 2);
}

#[tokio::test]
async fn write_through_tees_to_cache_without_origin_get() {
    let (origin_url, origin) = spawn_origin().await;
    let (cache_url, cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteThrough,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/through", sidecar.url),
        Some(b"through body".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(origin.body("media/through").unwrap(), b"through body");
    let blob = blob_key("cache", "media", "through");
    wait_for(Duration::from_secs(2), || {
        let cache = cache.clone();
        let blob = blob.clone();
        async move { cache.has(&blob) && cache.has(&format!("{blob}.meta")) }
    })
    .await;
    let get = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/through", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(body_text(get).await, "through body");
    assert_eq!(origin.get_count(), 0);
}

#[tokio::test]
async fn write_through_cache_failure_still_writes_origin() {
    let (origin_url, origin) = spawn_origin().await;
    let (cache_url, cache) = spawn_cache().await;
    cache.fail_puts();
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteThrough,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/cache-down", sidecar.url),
        Some(b"origin body".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(origin.body("media/cache-down").unwrap(), b"origin body");
    let blob = blob_key("cache", "media", "cache-down");
    assert!(!cache.has(&blob));
}

#[tokio::test]
async fn write_through_origin_failure_discards_cache() {
    let (origin_url, origin) = spawn_origin().await;
    *origin.put_status.lock().unwrap() = Some(StatusCode::SERVICE_UNAVAILABLE);
    let (cache_url, cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteThrough,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/origin-down", sidecar.url),
        Some(b"lost body".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert!(!put.status().is_success());
    let blob = blob_key("cache", "media", "origin-down");
    wait_for(Duration::from_secs(2), || {
        let cache = cache.clone();
        let blob = blob.clone();
        async move { !cache.has(&blob) && !cache.has(&format!("{blob}.meta")) }
    })
    .await;
}

#[tokio::test]
async fn otel_metrics_report_counters() {
    let (origin_url, origin) = spawn_origin().await;
    origin.put("media/item", b"hello", "\"etag-1\"");
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let telemetry = support::TestMetrics::new();
    let sidecar = spawn_sidecar_with_metrics(
        config(
            origin_url,
            cache_url,
            CacheMode::Read,
            0,
            3_600,
            WritePolicy::WriteBack,
            OnCacheError::Origin,
            &dir.path().join("index.redb"),
        ),
        &telemetry,
    )
    .await;
    let client = reqwest::Client::new();
    for _ in 0..2 {
        let get = signed_request(
            &client,
            Method::GET,
            &format!("{}/media/item", sidecar.url),
            None,
        )
        .send()
        .await
        .unwrap();
        assert_eq!(get.status(), StatusCode::OK);
    }
    let metrics = telemetry.collect();
    assert!(support::counter(&metrics, "s3_sidecar.cache.hits") >= 1);
    assert!(support::counter(&metrics, "s3_sidecar.cache.bytes_served") >= 5);
    assert!(support::counter(&metrics, "s3_sidecar.cache.origin_bytes") >= 5);
    assert_eq!(support::gauge(&metrics, "s3_sidecar.cache.max_bytes"), 0);
    assert_eq!(support::gauge(&metrics, "s3_sidecar.cache.unlimited"), 1);
}

const OBJECT_HEADERS: [(&str, &str); 5] = [
    ("x-amz-meta-color", "blue"),
    ("content-disposition", "attachment; filename=\"report.txt\""),
    ("cache-control", "max-age=60"),
    ("content-language", "en-US"),
    ("expires", "Wed, 21 Oct 2015 07:28:00 GMT"),
];

fn assert_object_headers(headers: &HeaderMap) {
    for (name, value) in OBJECT_HEADERS {
        assert_eq!(
            headers.get(name).and_then(|value| value.to_str().ok()),
            Some(value),
            "header {name}"
        );
    }
}

#[tokio::test]
async fn write_back_preserves_object_headers() {
    let (origin_url, origin) = spawn_origin().await;
    let _permit = origin.block_puts();
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request_with_headers(
        &client,
        Method::PUT,
        &format!("{}/media/hdr-back", sidecar.url),
        Some(b"body".to_vec()),
        &OBJECT_HEADERS,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let get = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/hdr-back", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_object_headers(get.headers());
    assert_eq!(body_text(get).await, "body");
    assert_eq!(origin.get_count(), 0);
    origin.release_puts();
}

#[tokio::test]
async fn write_back_sync_sends_object_headers() {
    let (origin_url, origin) = spawn_origin().await;
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request_with_headers(
        &client,
        Method::PUT,
        &format!("{}/media/hdr-sync", sidecar.url),
        Some(b"body".to_vec()),
        &OBJECT_HEADERS,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    wait_for(Duration::from_secs(3), || {
        let origin = origin.clone();
        async move { origin.body("media/hdr-sync").is_some() }
    })
    .await;
    let recorded = origin.put_headers("media/hdr-sync").unwrap();
    for (name, value) in OBJECT_HEADERS {
        assert_eq!(
            recorded.get(name).map(String::as_str),
            Some(value),
            "header {name}"
        );
    }
}

#[tokio::test]
async fn write_through_preserves_object_headers() {
    let (origin_url, origin) = spawn_origin().await;
    let (cache_url, cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteThrough,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request_with_headers(
        &client,
        Method::PUT,
        &format!("{}/media/hdr-through", sidecar.url),
        Some(b"body".to_vec()),
        &OBJECT_HEADERS,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let blob = blob_key("cache", "media", "hdr-through");
    wait_for(Duration::from_secs(3), || {
        let cache = cache.clone();
        let blob = blob.clone();
        async move { cache.has(&blob) && cache.has(&format!("{blob}.meta")) }
    })
    .await;
    let recorded = origin.put_headers("media/hdr-through").unwrap();
    for (name, value) in OBJECT_HEADERS {
        assert_eq!(
            recorded.get(name).map(String::as_str),
            Some(value),
            "header {name}"
        );
    }
    let get = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/hdr-through", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_object_headers(get.headers());
    assert_eq!(body_text(get).await, "body");
    assert_eq!(origin.get_count(), 0);
}

#[tokio::test]
async fn write_back_if_none_match_rejects_an_existing_key() {
    let (origin_url, origin) = spawn_origin().await;
    let permit = origin.block_puts();
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let first = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/create-only", sidecar.url),
        Some(b"first".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let second = signed_request_with_headers(
        &client,
        Method::PUT,
        &format!("{}/media/create-only", sidecar.url),
        Some(b"second".to_vec()),
        &[("if-none-match", "*")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(second.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(origin.put_count(), 0);
    drop(permit);
}

#[tokio::test]
async fn write_back_if_match_checks_the_cached_etag() {
    let (origin_url, origin) = spawn_origin().await;
    let permit = origin.block_puts();
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let first = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/conditional", sidecar.url),
        Some(b"first".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let etag = first
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_owned();
    let wrong = signed_request_with_headers(
        &client,
        Method::PUT,
        &format!("{}/media/conditional", sidecar.url),
        Some(b"wrong".to_vec()),
        &[("if-match", "\"not-the-etag\"")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(wrong.status(), StatusCode::PRECONDITION_FAILED);
    let correct = signed_request_with_headers(
        &client,
        Method::PUT,
        &format!("{}/media/conditional", sidecar.url),
        Some(b"correct".to_vec()),
        &[("if-match", etag.as_str())],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(correct.status(), StatusCode::OK);
    drop(permit);
}

#[tokio::test]
async fn cached_get_honors_if_none_match() {
    let (origin_url, origin) = spawn_origin().await;
    origin.put("media/cond-get", b"hello", "\"etag-get\"");
    let (cache_url, cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::Read,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let first = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/cond-get", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(body_text(first).await, "hello");
    let blob = blob_key("cache", "media", "cond-get");
    wait_for(Duration::from_secs(2), || {
        let cache = cache.clone();
        let blob = blob.clone();
        async move { cache.has(&blob) && cache.has(&format!("{blob}.meta")) }
    })
    .await;
    let cached = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/cond-get", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(cached.status(), StatusCode::OK);
    let etag = cached
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_owned();
    assert_eq!(body_text(cached).await, "hello");
    let matched = signed_request_with_headers(
        &client,
        Method::GET,
        &format!("{}/media/cond-get", sidecar.url),
        None,
        &[("if-none-match", etag.as_str())],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(matched.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(body_text(matched).await, "");
    let other = signed_request_with_headers(
        &client,
        Method::GET,
        &format!("{}/media/cond-get", sidecar.url),
        None,
        &[("if-none-match", "\"other\"")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(other.status(), StatusCode::OK);
    assert_eq!(body_text(other).await, "hello");
    assert_eq!(origin.get_count(), 1);
}

#[tokio::test]
async fn cached_head_honors_if_none_match() {
    let (origin_url, origin) = spawn_origin().await;
    origin.put("media/cond-head", b"hello", "\"etag-head\"");
    let (cache_url, cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::Read,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let first = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/cond-head", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(body_text(first).await, "hello");
    let blob = blob_key("cache", "media", "cond-head");
    wait_for(Duration::from_secs(2), || {
        let cache = cache.clone();
        let blob = blob.clone();
        async move { cache.has(&blob) && cache.has(&format!("{blob}.meta")) }
    })
    .await;
    let cached = signed_request(
        &client,
        Method::HEAD,
        &format!("{}/media/cond-head", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(cached.status(), StatusCode::OK);
    let etag = cached
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_owned();
    let matched = signed_request_with_headers(
        &client,
        Method::HEAD,
        &format!("{}/media/cond-head", sidecar.url),
        None,
        &[("if-none-match", etag.as_str())],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(matched.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(origin.get_count(), 1);
}

#[tokio::test]
async fn delete_with_a_wrong_if_match_is_rejected() {
    let (origin_url, origin) = spawn_origin().await;
    let permit = origin.block_puts();
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::ReadWrite,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/cond-delete", sidecar.url),
        Some(b"body".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let delete = signed_request_with_headers(
        &client,
        Method::DELETE,
        &format!("{}/media/cond-delete", sidecar.url),
        None,
        &[("if-match", "\"not-the-etag\"")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(delete.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(origin.delete_count(), 0);
    drop(permit);
}

#[tokio::test]
async fn write_mode_rejects_sse_c() {
    let (origin_url, origin) = spawn_origin().await;
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::Write,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    let put = signed_request_with_headers(
        &client,
        Method::PUT,
        &format!("{}/media/sse-c", sidecar.url),
        Some(b"body".to_vec()),
        &[
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            ("x-amz-server-side-encryption-customer-key", "customer-key"),
            ("x-amz-server-side-encryption-customer-key-md5", "key-md5"),
        ],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(put.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(origin.put_count(), 0);
}

#[tokio::test]
async fn read_mode_forwards_sse_c_to_origin() {
    let (origin_url, origin) = spawn_origin().await;
    origin.put("media/sse-c-read", b"hello", "\"etag-sse\"");
    let (cache_url, _cache) = spawn_cache().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(config(
        origin_url,
        cache_url,
        CacheMode::Read,
        0,
        3_600,
        WritePolicy::WriteBack,
        OnCacheError::Origin,
        &dir.path().join("index.redb"),
    ))
    .await;
    let client = reqwest::Client::new();
    for _ in 0..2 {
        let get = signed_request_with_headers(
            &client,
            Method::GET,
            &format!("{}/media/sse-c-read", sidecar.url),
            None,
            &[("x-amz-server-side-encryption-customer-algorithm", "AES256")],
        )
        .send()
        .await
        .unwrap();
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(body_text(get).await, "hello");
    }
    assert_eq!(origin.get_count(), 2);
}
