#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Cluster-mode integration tests with an in-process fake backing S3.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use md5::{Digest, Md5};
use reqwest::Url;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, CredentialStore, EMPTY_SHA256};
use s3_sidecar::config::duration::HumanDuration;
use s3_sidecar::config::secret::SecretString;
use s3_sidecar::config::{
    BucketConfig, ClientConfig, ClusterConfig, Config, ConsensusBackend, ConsensusConfig,
    EndpointConfig, Mode, RaftConfig, ReadMode, WriteAck,
};
use s3_sidecar::server::{AppState, router};
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
struct Stored {
    body: Bytes,
    etag: String,
    last_modified: String,
}

#[derive(Default)]
struct Counts {
    puts: AtomicU64,
    gets: AtomicU64,
    deletes: AtomicU64,
}

#[derive(Debug, Clone)]
struct Upload {
    key: String,
    parts: BTreeMap<u32, Bytes>,
}

#[derive(Clone)]
struct BackingState {
    objects: Arc<Mutex<HashMap<String, Stored>>>,
    uploads: Arc<Mutex<HashMap<String, Upload>>>,
    counts: Arc<Counts>,
    next_upload: Arc<AtomicU64>,
}

#[derive(Clone)]
struct BackingHandle {
    objects: Arc<Mutex<HashMap<String, Stored>>>,
    counts: Arc<Counts>,
}

impl BackingHandle {
    fn insert(&self, key: &str, body: &[u8], last_modified: &str) {
        self.objects.lock().unwrap().insert(
            format!("objects/{key}"),
            Stored {
                body: Bytes::copy_from_slice(body),
                etag: md5_hex(body),
                last_modified: last_modified.to_owned(),
            },
        );
    }

    fn has(&self, key: &str) -> bool {
        self.objects
            .lock()
            .unwrap()
            .contains_key(&format!("objects/{key}"))
    }

    fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.objects.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }

    fn put_count(&self) -> u64 {
        self.counts.puts.load(Ordering::Relaxed)
    }
}

async fn spawn_backing() -> (String, BackingHandle) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let objects = Arc::new(Mutex::new(HashMap::new()));
    let uploads = Arc::new(Mutex::new(HashMap::new()));
    let counts = Arc::new(Counts::default());
    let next_upload = Arc::new(AtomicU64::new(0));
    let state = BackingState {
        objects: Arc::clone(&objects),
        uploads: Arc::clone(&uploads),
        counts: Arc::clone(&counts),
        next_upload: Arc::clone(&next_upload),
    };
    let app = Router::new().fallback(backing_handler).with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        format!("http://{address}"),
        BackingHandle { objects, counts },
    )
}

#[allow(clippy::too_many_lines, clippy::format_push_string)]
async fn backing_handler(
    State(state): State<BackingState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (bucket, key) = split_path(uri.path());
    if bucket != "data" {
        return xml(
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchBucket</Code></Error>",
        );
    }
    let query = uri.query().unwrap_or_default();
    let query = query_pairs(query);

    if method == Method::GET && key.is_empty() && get(&query, "list-type") == Some("2") {
        let objects = state.objects.lock().unwrap();
        let mut contents = String::new();
        for (name, stored) in objects.iter() {
            contents.push_str("<Contents>");
            contents.push_str(&format!("<Key>{name}</Key>"));
            contents.push_str(&format!(
                "<LastModified>{}</LastModified>",
                stored.last_modified
            ));
            contents.push_str(&format!("<Size>{}</Size>", stored.body.len()));
            contents.push_str("</Contents>");
        }
        return xml(
            StatusCode::OK,
            &format!(
                "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>\
                 <KeyCount>0</KeyCount>{contents}</ListBucketResult>"
            ),
        );
    }

    if method == Method::POST && !key.is_empty() && has(&query, "uploads") {
        let id = state.next_upload.fetch_add(1, Ordering::Relaxed);
        let upload_id = format!("backing-{id}");
        state.uploads.lock().unwrap().insert(
            upload_id.clone(),
            Upload {
                key: key.to_owned(),
                parts: BTreeMap::new(),
            },
        );
        return xml(
            StatusCode::OK,
            &format!(
                "<InitiateMultipartUploadResult><UploadId>{upload_id}</UploadId>\
                 </InitiateMultipartUploadResult>"
            ),
        );
    }

    if method == Method::PUT && has(&query, "partNumber") {
        let Some(upload_id) = get(&query, "uploadId") else {
            return xml(
                StatusCode::BAD_REQUEST,
                "<Error><Code>InvalidRequest</Code></Error>",
            );
        };
        let part_number = get(&query, "partNumber")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let mut uploads = state.uploads.lock().unwrap();
        let Some(upload) = uploads.get_mut(upload_id) else {
            return xml(
                StatusCode::NOT_FOUND,
                "<Error><Code>NoSuchUpload</Code></Error>",
            );
        };
        upload.parts.insert(part_number, body);
        drop(uploads);
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(header::ETAG, HeaderValue::from_static("\"part-etag\""));
        return response;
    }

    if method == Method::POST && has(&query, "uploadId") {
        let Some(upload_id) = get(&query, "uploadId") else {
            return xml(
                StatusCode::BAD_REQUEST,
                "<Error><Code>InvalidRequest</Code></Error>",
            );
        };
        let Some(upload) = state.uploads.lock().unwrap().remove(upload_id) else {
            return xml(
                StatusCode::NOT_FOUND,
                "<Error><Code>NoSuchUpload</Code></Error>",
            );
        };
        let mut body = Vec::new();
        let mut digests = Vec::new();
        for part in upload.parts.values() {
            body.extend_from_slice(part);
            digests.extend_from_slice(Md5::digest(part).as_slice());
        }
        let count = upload.parts.len();
        let etag = format!("{}-{count}", md5_hex(&digests));
        state.objects.lock().unwrap().insert(
            upload.key.clone(),
            Stored {
                body: Bytes::from(body),
                etag: etag.clone(),
                last_modified: now_timestamp(),
            },
        );
        return xml(
            StatusCode::OK,
            &format!(
                "<CompleteMultipartUploadResult><ETag>\"{etag}\"</ETag>\
                 </CompleteMultipartUploadResult>"
            ),
        );
    }

    if method == Method::DELETE && has(&query, "uploadId") {
        if let Some(upload_id) = get(&query, "uploadId") {
            state.uploads.lock().unwrap().remove(upload_id);
        }
        return empty(StatusCode::NO_CONTENT);
    }

    if method == Method::GET && has(&query, "uploadId") {
        let upload_id = get(&query, "uploadId").unwrap_or_default();
        return xml(
            StatusCode::OK,
            &format!("<ListPartsResult><UploadId>{upload_id}</UploadId></ListPartsResult>"),
        );
    }

    if method == Method::PUT && headers.contains_key("x-amz-copy-source") {
        let source = headers
            .get("x-amz-copy-source")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let trimmed = source.trim_start_matches('/');
        let Some((source_bucket, source_key)) = trimmed.split_once('/') else {
            return xml(
                StatusCode::BAD_REQUEST,
                "<Error><Code>InvalidRequest</Code></Error>",
            );
        };
        if source_bucket != "data" {
            return xml(
                StatusCode::NOT_FOUND,
                "<Error><Code>NoSuchKey</Code></Error>",
            );
        }
        let stored = state.objects.lock().unwrap().get(source_key).cloned();
        let Some(stored) = stored else {
            return xml(
                StatusCode::NOT_FOUND,
                "<Error><Code>NoSuchKey</Code></Error>",
            );
        };
        let etag = stored.etag.clone();
        let last_modified = stored.last_modified.clone();
        state.objects.lock().unwrap().insert(key.to_owned(), stored);
        return xml(
            StatusCode::OK,
            &format!(
                "<CopyObjectResult><ETag>\"{etag}\"</ETag>\
                 <LastModified>{last_modified}</LastModified></CopyObjectResult>"
            ),
        );
    }

    if key.is_empty() {
        return xml(
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchKey</Code></Error>",
        );
    }

    match method {
        Method::PUT => {
            state.counts.puts.fetch_add(1, Ordering::Relaxed);
            let stored = Stored {
                body,
                etag: String::new(),
                last_modified: now_timestamp(),
            };
            let etag = md5_hex(&stored.body);
            state.objects.lock().unwrap().insert(
                key.to_owned(),
                Stored {
                    etag: etag.clone(),
                    ..stored
                },
            );
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::OK;
            if let Ok(value) = HeaderValue::from_str(&format!("\"{etag}\"")) {
                response.headers_mut().insert(header::ETAG, value);
            }
            response
        }
        Method::GET | Method::HEAD => {
            state.counts.gets.fetch_add(1, Ordering::Relaxed);
            let stored = state.objects.lock().unwrap().get(key).cloned();
            let Some(stored) = stored else {
                return xml(
                    StatusCode::NOT_FOUND,
                    "<Error><Code>NoSuchKey</Code></Error>",
                );
            };
            let range = headers
                .get(header::RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_range);
            let (status, body, length, range_header) = if let Some((start, end)) = range {
                let end = end.min(stored.body.len().saturating_sub(1));
                let slice = stored
                    .body
                    .get(start..=end)
                    .map_or_else(Bytes::new, Bytes::copy_from_slice);
                (
                    StatusCode::PARTIAL_CONTENT,
                    if method == Method::HEAD {
                        Bytes::new()
                    } else {
                        slice.clone()
                    },
                    slice.len(),
                    Some(format!("bytes {start}-{end}/{}", stored.body.len())),
                )
            } else {
                (
                    StatusCode::OK,
                    if method == Method::HEAD {
                        Bytes::new()
                    } else {
                        stored.body.clone()
                    },
                    stored.body.len(),
                    None,
                )
            };
            let mut response = Response::new(Body::from(body));
            *response.status_mut() = status;
            response.headers_mut().insert(
                header::ETAG,
                HeaderValue::from_str(&format!("\"{}\"", stored.etag)).unwrap(),
            );
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&length.to_string()).unwrap(),
            );
            response.headers_mut().insert(
                header::LAST_MODIFIED,
                HeaderValue::from_str(&stored.last_modified).unwrap(),
            );
            if let Some(value) = range_header
                && let Ok(value) = HeaderValue::from_str(&value)
            {
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
            response
        }
        Method::DELETE => {
            state.counts.deletes.fetch_add(1, Ordering::Relaxed);
            state.objects.lock().unwrap().remove(key);
            empty(StatusCode::NO_CONTENT)
        }
        _ => xml(
            StatusCode::NOT_IMPLEMENTED,
            "<Error><Code>NotImplemented</Code></Error>",
        ),
    }
}

fn parse_range(value: &str) -> Option<(usize, usize)> {
    let value = value.strip_prefix("bytes=")?;
    let (start, end) = value.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

fn split_path(path: &str) -> (&str, &str) {
    let trimmed = path.trim_start_matches('/');
    match trimmed.split_once('/') {
        Some((bucket, key)) => (bucket, key),
        None => (trimmed, ""),
    }
}

fn query_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_encoding::percent_decode_str(key)
                    .decode_utf8_lossy()
                    .into_owned(),
                percent_encoding::percent_decode_str(value)
                    .decode_utf8_lossy()
                    .into_owned(),
            )
        })
        .collect()
}

fn has(query: &[(String, String)], name: &str) -> bool {
    query.iter().any(|(key, _)| key == name)
}

fn get<'a>(query: &'a [(String, String)], name: &str) -> Option<&'a str> {
    query
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn md5_hex(body: &[u8]) -> String {
    hex::encode(Md5::digest(body))
}

fn now_timestamp() -> String {
    OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

fn empty(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
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

fn cluster_config(
    backing: String,
    node_id: &str,
    metadata_path: &std::path::Path,
    read: ReadMode,
    staging_ttl: u64,
    orphan_ttl: u64,
) -> Config {
    Config {
        mode: Mode::Cluster,
        backing: EndpointConfig {
            endpoint: backing,
            bucket: "data".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKIA".to_owned(),
            secret_access_key: SecretString::new("secret"),
            session_token: None,
            force_path_style: true,
        },
        consensus: ConsensusConfig {
            backend: ConsensusBackend::Single,
            raft: RaftConfig {
                node_id: node_id.to_owned(),
                peers: vec![node_id.to_owned()],
                ..RaftConfig::default()
            },
        },
        cluster: ClusterConfig {
            staging_ttl: HumanDuration::new(Duration::from_secs(staging_ttl)),
            orphan_ttl: HumanDuration::new(Duration::from_secs(orphan_ttl)),
            gc_interval: HumanDuration::new(Duration::from_hours(1)),
            metadata_path: metadata_path.display().to_string(),
        },
        buckets: vec![BucketConfig {
            name: "media".to_owned(),
            write_ack: Some(WriteAck::Leader),
            read: Some(read),
            replicas: Some(1),
            min_replicas: Some(1),
            ..BucketConfig::default()
        }],
        clients: vec![client_config()],
        ..Config::default()
    }
}

struct Sidecar {
    url: String,
    state: Arc<AppState>,
    task: JoinHandle<()>,
}

impl Sidecar {
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
        drop(self.state);
    }
}

async fn spawn_sidecar(config: Config) -> Sidecar {
    let state = Arc::new(AppState::new(config).unwrap());
    state.start().await.unwrap();
    state.set_ready(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = router(Arc::clone(&state));
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Sidecar {
        url: format!("http://{address}"),
        state,
        task,
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

fn extract(text: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = text.find(&open).unwrap();
    let after = &text[start + open.len()..];
    let end = after.find(&close).unwrap();
    after[..end].to_owned()
}

async fn put(
    client: &reqwest::Client,
    url: &str,
    body: &[u8],
    extra: &[(&str, &str)],
) -> reqwest::Response {
    signed_request(client, Method::PUT, url, Some(body.to_vec()), extra)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn put_get_head_delete_and_list() {
    let (backing, _) = spawn_backing().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cluster_config(
        backing,
        "a",
        &dir.path().join("meta.redb"),
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let client = reqwest::Client::new();
    let url = format!("{}/media/hello", sidecar.url);

    let response = put(
        &client,
        &url,
        b"hello",
        &[
            ("content-type", "text/plain"),
            ("x-amz-meta-color", "blue"),
            ("cache-control", "max-age=60"),
        ],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::ETAG).unwrap(),
        &format!("\"{}\"", md5_hex(b"hello"))
    );

    let response = signed_request(&client, Method::GET, &url, None, &[])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-amz-meta-color").unwrap(), "blue");
    assert_eq!(body_text(response).await, "hello");

    let response = signed_request(&client, Method::HEAD, &url, None, &[])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "5");
    assert_eq!(body_text(response).await, "");

    let list_url = format!("{}/media?list-type=2", sidecar.url);
    let response = signed_request(&client, Method::GET, &list_url, None, &[])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let list = body_text(response).await;
    assert!(list.contains("<Key>hello</Key>"));
    assert!(!list.contains("<Key>objects/"));

    let response = signed_request(&client, Method::DELETE, &url, None, &[])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = signed_request(&client, Method::GET, &url, None, &[])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = signed_request(&client, Method::GET, &list_url, None, &[])
        .send()
        .await
        .unwrap();
    assert!(!body_text(response).await.contains("<Key>hello</Key>"));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn copy_object_preserves_and_replaces_metadata() {
    let (backing, handle) = spawn_backing().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cluster_config(
        backing,
        "a",
        &dir.path().join("meta.redb"),
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let client = reqwest::Client::new();
    let source_url = format!("{}/media/source", sidecar.url);
    let source = put(
        &client,
        &source_url,
        b"copy-body",
        &[
            ("content-type", "text/plain"),
            ("cache-control", "max-age=30"),
            ("x-amz-meta-color", "blue"),
        ],
    )
    .await;
    assert_eq!(source.status(), StatusCode::OK);
    let source_etag = source.headers().get(header::ETAG).unwrap().clone();
    assert!(
        sidecar
            .state
            .cluster()
            .unwrap()
            .state()
            .get_metadata("media", "source")
            .unwrap()
            .is_some()
    );

    let copied = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/copied", sidecar.url),
        None,
        &[("x-amz-copy-source", "/media/source")],
    )
    .send()
    .await
    .unwrap();
    let copied_status = copied.status();
    let copied_body = body_text(copied).await;
    assert_eq!(copied_status, StatusCode::OK, "{copied_body}");
    assert!(copied_body.contains("<CopyObjectResult"));
    assert!(copied_body.contains(source_etag.to_str().unwrap()));

    let response = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/copied", sidecar.url),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/plain"
    );
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "max-age=30"
    );
    assert_eq!(response.headers().get("x-amz-meta-color").unwrap(), "blue");
    assert_eq!(body_text(response).await, "copy-body");

    let replaced = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/replaced", sidecar.url),
        None,
        &[
            ("x-amz-copy-source", "/media/source"),
            ("x-amz-metadata-directive", "REPLACE"),
            ("content-type", "application/json"),
            ("x-amz-meta-color", "red"),
        ],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(replaced.status(), StatusCode::OK);
    let response = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/replaced", sidecar.url),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert_eq!(response.headers().get("x-amz-meta-color").unwrap(), "red");
    assert_eq!(body_text(response).await, "copy-body");

    let missing = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/missing-copy", sidecar.url),
        None,
        &[("x-amz-copy-source", "/media/absent")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let before = handle.keys().len();
    let blocked = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/copied", sidecar.url),
        None,
        &[
            ("x-amz-copy-source", "/media/source"),
            ("if-none-match", "*"),
        ],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(blocked.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(handle.keys().len(), before);
}

#[tokio::test]
async fn list_hides_staging_and_upload_objects() {
    let (backing, handle) = spawn_backing().await;
    handle.insert("staged-orphan", b"x", "2000-01-01T00:00:00Z");
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cluster_config(
        backing,
        "a",
        &dir.path().join("meta.redb"),
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let client = reqwest::Client::new();

    let response = signed_request(
        &client,
        Method::POST,
        &format!("{}/media/hidden?uploads", sidecar.url),
        Some(Vec::new()),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _upload = extract(&body_text(response).await, "UploadId");

    let response = signed_request(
        &client,
        Method::GET,
        &format!("{}/media?list-type=2", sidecar.url),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    let list = body_text(response).await;
    assert!(!list.contains("staged-orphan"));
    assert!(!list.contains("hidden"));
}

#[tokio::test]
async fn conditional_writes_remove_the_staged_blob() {
    let (backing, handle) = spawn_backing().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cluster_config(
        backing,
        "a",
        &dir.path().join("meta.redb"),
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let client = reqwest::Client::new();
    let url = format!("{}/media/cond", sidecar.url);
    let first = put(&client, &url, b"one", &[]).await;
    assert_eq!(first.status(), StatusCode::OK);
    let etag = first
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let before = handle.keys().len();
    let before_puts = handle.put_count();

    let response = put(&client, &url, b"two", &[("if-none-match", "*")]).await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(handle.keys().len(), before);
    assert_eq!(handle.put_count(), before_puts);

    let response = put(&client, &url, b"two", &[("if-match", "\"wrong\"")]).await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(handle.keys().len(), before);

    let response = put(&client, &url, b"two", &[("if-match", etag.as_str())]).await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = signed_request(&client, Method::GET, &url, None, &[])
        .send()
        .await
        .unwrap();
    assert_eq!(body_text(response).await, "two");
}

#[tokio::test]
async fn tombstone_hides_the_key_and_delete_is_idempotent() {
    let (backing, _) = spawn_backing().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cluster_config(
        backing,
        "a",
        &dir.path().join("meta.redb"),
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let client = reqwest::Client::new();
    let url = format!("{}/media/gone", sidecar.url);
    assert_eq!(put(&client, &url, b"x", &[]).await.status(), StatusCode::OK);
    assert_eq!(
        signed_request(&client, Method::DELETE, &url, None, &[])
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        signed_request(&client, Method::GET, &url, None, &[])
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        signed_request(&client, Method::DELETE, &url, None, &[])
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn metadata_survives_restart() {
    let (backing, _) = spawn_backing().await;
    let dir = tempfile::tempdir().unwrap();
    let metadata_path = dir.path().join("meta.redb");
    let sidecar = spawn_sidecar(cluster_config(
        backing.clone(),
        "a",
        &metadata_path,
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let client = reqwest::Client::new();
    let url = format!("{}/media/persist", sidecar.url);
    assert_eq!(
        put(&client, &url, b"body", &[]).await.status(),
        StatusCode::OK
    );
    sidecar.stop().await;

    let sidecar = spawn_sidecar(cluster_config(
        backing,
        "a",
        &metadata_path,
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let response = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/persist", sidecar.url),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response).await, "body");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn multipart_create_upload_complete_and_abort() {
    let (backing, _) = spawn_backing().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cluster_config(
        backing,
        "a",
        &dir.path().join("meta.redb"),
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let client = reqwest::Client::new();
    let create_url = format!("{}/media/big?uploads", sidecar.url);
    let response = signed_request(
        &client,
        Method::POST,
        &create_url,
        Some(Vec::new()),
        &[
            ("content-type", "text/plain"),
            ("cache-control", "max-age=60"),
            ("x-amz-meta-kind", "demo"),
        ],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let upload_id = extract(&body_text(response).await, "UploadId");

    for (part, body) in [(1, "hello "), (2, "world")] {
        let url = format!(
            "{}/media/big?partNumber={part}&uploadId={upload_id}",
            sidecar.url
        );
        let response = put(&client, &url, body.as_bytes(), &[]).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    let complete_url = format!("{}/media/big?uploadId={upload_id}", sidecar.url);
    let complete_body = "<CompleteMultipartUpload>\
        <Part><PartNumber>1</PartNumber><ETag>\"part-etag\"</ETag></Part>\
        <Part><PartNumber>2</PartNumber><ETag>\"part-etag\"</ETag></Part>\
        </CompleteMultipartUpload>"
        .as_bytes()
        .to_vec();
    let response = signed_request(
        &client,
        Method::POST,
        &complete_url,
        Some(complete_body),
        &[("content-type", "application/xml")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(body_text(response).await.contains("-2"));

    let response = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/big", sidecar.url),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    let etag = response.headers().get(header::ETAG).unwrap().to_owned();
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/plain"
    );
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "max-age=60"
    );
    assert_eq!(response.headers().get("x-amz-meta-kind").unwrap(), "demo");
    assert_eq!(body_text(response).await, "hello world");
    let expected = format!(
        "\"{}-2\"",
        md5_hex(
            &[
                Md5::digest(b"hello ").as_slice(),
                Md5::digest(b"world").as_slice()
            ]
            .concat()
        )
    );
    assert_eq!(etag, expected);

    let response = signed_request(&client, Method::POST, &create_url, Some(Vec::new()), &[])
        .send()
        .await
        .unwrap();
    let abort_id = extract(&body_text(response).await, "UploadId");
    let response = signed_request(
        &client,
        Method::DELETE,
        &format!("{}/media/other?uploadId={abort_id}", sidecar.url),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn upload_id_on_the_wrong_node_returns_a_clear_error() {
    let (backing_a, _) = spawn_backing().await;
    let (backing_b, _) = spawn_backing().await;
    let dir = tempfile::tempdir().unwrap();
    let node_a = spawn_sidecar(cluster_config(
        backing_a,
        "a",
        &dir.path().join("a.redb"),
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let node_b = spawn_sidecar(cluster_config(
        backing_b,
        "b",
        &dir.path().join("b.redb"),
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::POST,
        &format!("{}/media/key?uploads", node_a.url),
        Some(Vec::new()),
        &[],
    )
    .send()
    .await
    .unwrap();
    let upload_id = extract(&body_text(response).await, "UploadId");

    let response = put(
        &client,
        &format!("{}/media/key?partNumber=1&uploadId={upload_id}", node_b.url),
        b"part",
        &[],
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("NoSuchUpload") || text.contains("InvalidRequest"));
    assert!(text.contains("belongs to node"));
    assert!(text.contains('a'));
}

#[tokio::test]
async fn leader_read_calls_the_read_index() {
    let (backing, _) = spawn_backing().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cluster_config(
        backing,
        "a",
        &dir.path().join("meta.redb"),
        ReadMode::Leader,
        3_600,
        86_400,
    ))
    .await;
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/missing", sidecar.url),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let metrics = body_text(
        client
            .get(format!("{}/metrics", sidecar.url))
            .send()
            .await
            .unwrap(),
    )
    .await;
    let line = metrics
        .lines()
        .find(|line| line.starts_with("s3_sidecar_cluster_read_index_calls_total"))
        .unwrap();
    let value: u64 = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(value >= 1);
}

#[tokio::test]
async fn quorum_write_ack_is_rejected_at_startup() {
    let (backing, _) = spawn_backing().await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = cluster_config(
        backing,
        "a",
        &dir.path().join("meta.redb"),
        ReadMode::Local,
        3_600,
        86_400,
    );
    config.buckets[0].write_ack = Some(WriteAck::Quorum);
    let valid = s3_sidecar::config::validate(&config).unwrap_err();
    assert!(valid.to_string().contains("not implemented"));
    let error = AppState::new(config).err().unwrap();
    assert!(error.message.contains("not implemented"));
}

#[tokio::test]
async fn bucket_config_is_seeded_once() {
    let (backing, _) = spawn_backing().await;
    let dir = tempfile::tempdir().unwrap();
    let metadata_path = dir.path().join("meta.redb");
    let sidecar = spawn_sidecar(cluster_config(
        backing.clone(),
        "a",
        &metadata_path,
        ReadMode::Local,
        3_600,
        86_400,
    ))
    .await;
    let stored = sidecar
        .state
        .cluster()
        .unwrap()
        .state()
        .bucket_config("media")
        .unwrap()
        .unwrap();
    assert_eq!(stored.read, ReadMode::Local);
    assert_eq!(stored.replicas, 1);
    sidecar.stop().await;

    let sidecar = spawn_sidecar(cluster_config(
        backing,
        "a",
        &metadata_path,
        ReadMode::Leader,
        3_600,
        86_400,
    ))
    .await;
    let stored = sidecar
        .state
        .cluster()
        .unwrap()
        .state()
        .bucket_config("media")
        .unwrap()
        .unwrap();
    assert_eq!(stored.read, ReadMode::Local);
    assert_eq!(stored.replicas, 1);
}

#[tokio::test]
async fn gc_removes_orphan_and_tombstoned_blobs() {
    let (backing, handle) = spawn_backing().await;
    handle.insert("orphan", b"orphan", "2000-01-01T00:00:00Z");
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cluster_config(
        backing,
        "a",
        &dir.path().join("meta.redb"),
        ReadMode::Local,
        1,
        1,
    ))
    .await;
    let client = reqwest::Client::new();
    let url = format!("{}/media/gone", sidecar.url);
    assert_eq!(put(&client, &url, b"x", &[]).await.status(), StatusCode::OK);
    assert_eq!(
        signed_request(&client, Method::DELETE, &url, None, &[])
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let cluster = sidecar.state.cluster().unwrap();
    let report = cluster.gc_once().await.unwrap();
    assert!(report.orphan_blobs_removed >= 1);
    assert!(report.tombstones_removed >= 1);
    assert!(!handle.has("orphan"));
    assert!(
        cluster
            .state()
            .get_metadata("media", "gone")
            .unwrap()
            .is_none()
    );
}
