#![allow(dead_code)]

//! In-process fake S3 for multipart and data-path tests.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use md5::{Digest, Md5};
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
struct Stored {
    body: Bytes,
    etag: String,
}

#[derive(Debug, Clone)]
struct Upload {
    key: String,
    parts: BTreeMap<u32, Bytes>,
}

#[derive(Default)]
struct Counts {
    gets: AtomicU64,
    puts: AtomicU64,
    deletes: AtomicU64,
    uploads: AtomicU64,
    parts: AtomicU64,
    completes: AtomicU64,
    aborts: AtomicU64,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

#[derive(Clone)]
struct FakeState {
    objects: Arc<Mutex<HashMap<String, Stored>>>,
    uploads: Arc<Mutex<HashMap<String, Upload>>>,
    counts: Arc<Counts>,
    part_sizes: Arc<Mutex<Vec<u64>>>,
    next_upload: Arc<AtomicU64>,
    fail_part: Arc<Mutex<Option<u32>>>,
    part_delay: Arc<Mutex<Option<Duration>>>,
}

/// A handle to one fake S3 server.
#[derive(Clone)]
pub struct FakeS3 {
    objects: Arc<Mutex<HashMap<String, Stored>>>,
    counts: Arc<Counts>,
    part_sizes: Arc<Mutex<Vec<u64>>>,
    fail_part: Arc<Mutex<Option<u32>>>,
    part_delay: Arc<Mutex<Option<Duration>>>,
}

impl FakeS3 {
    /// Return one stored object body.
    #[must_use]
    pub fn object(&self, key: &str) -> Option<Bytes> {
        self.objects
            .lock()
            .ok()?
            .get(key)
            .map(|stored| stored.body.clone())
    }

    /// Return true when the fake stores the key.
    #[must_use]
    pub fn has(&self, key: &str) -> bool {
        self.objects
            .lock()
            .is_ok_and(|objects| objects.contains_key(key))
    }

    /// Return all stored keys.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .objects
            .lock()
            .map_or_default(|objects| objects.keys().cloned().collect());
        keys.sort();
        keys
    }

    /// Return the number of single-object `PUT` requests.
    #[must_use]
    pub fn puts(&self) -> u64 {
        self.counts.puts.load(Ordering::Relaxed)
    }

    /// Return the number of `GET` and `HEAD` requests.
    #[must_use]
    pub fn gets(&self) -> u64 {
        self.counts.gets.load(Ordering::Relaxed)
    }

    /// Return the number of multipart uploads that started.
    #[must_use]
    pub fn uploads(&self) -> u64 {
        self.counts.uploads.load(Ordering::Relaxed)
    }

    /// Return the number of parts that were stored.
    #[must_use]
    pub fn parts(&self) -> u64 {
        self.counts.parts.load(Ordering::Relaxed)
    }

    /// Return the number of completed multipart uploads.
    #[must_use]
    pub fn completes(&self) -> u64 {
        self.counts.completes.load(Ordering::Relaxed)
    }

    /// Return the number of aborted multipart uploads.
    #[must_use]
    pub fn aborts(&self) -> u64 {
        self.counts.aborts.load(Ordering::Relaxed)
    }

    /// Return the stored part lengths in arrival order.
    #[must_use]
    pub fn part_sizes(&self) -> Vec<u64> {
        self.part_sizes.lock().map_or_default(|sizes| sizes.clone())
    }

    /// Return the largest number of part uploads in flight.
    #[must_use]
    pub fn max_in_flight(&self) -> usize {
        self.counts.max_in_flight.load(Ordering::Relaxed)
    }

    /// Fail one part number with status 500.
    pub fn fail_part(&self, number: u32) {
        if let Ok(mut fail) = self.fail_part.lock() {
            *fail = Some(number);
        }
    }

    /// Add a delay to every part upload.
    pub fn delay_parts(&self, delay: Duration) {
        if let Ok(mut current) = self.part_delay.lock() {
            *current = Some(delay);
        }
    }
}

/// Spawn one fake S3 server and return its URL and handle.
pub async fn spawn() -> (String, FakeS3) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let objects = Arc::new(Mutex::new(HashMap::new()));
    let uploads = Arc::new(Mutex::new(HashMap::new()));
    let counts = Arc::new(Counts::default());
    let part_sizes = Arc::new(Mutex::new(Vec::new()));
    let next_upload = Arc::new(AtomicU64::new(0));
    let fail_part = Arc::new(Mutex::new(None));
    let part_delay = Arc::new(Mutex::new(None));
    let state = FakeState {
        objects: Arc::clone(&objects),
        uploads: Arc::clone(&uploads),
        counts: Arc::clone(&counts),
        part_sizes: Arc::clone(&part_sizes),
        next_upload,
        fail_part: Arc::clone(&fail_part),
        part_delay: Arc::clone(&part_delay),
    };
    let app = Router::new().fallback(handler).with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        format!("http://{address}"),
        FakeS3 {
            objects,
            counts,
            part_sizes,
            fail_part,
            part_delay,
        },
    )
}

#[allow(clippy::too_many_lines)]
async fn handler(
    State(state): State<FakeState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (bucket, key) = split_path(uri.path());
    let query = query_pairs(uri.query().unwrap_or_default());
    let object_key = format!("{bucket}/{key}");

    if key.is_empty() && method == Method::GET && get(&query, "list-type") == Some("2") {
        let prefix = get(&query, "prefix").unwrap_or_default();
        let bucket_prefix = format!("{bucket}/");
        let Ok(objects) = state.objects.lock() else {
            return empty(StatusCode::INTERNAL_SERVER_ERROR);
        };
        let mut contents = String::new();
        for name in objects.keys() {
            let Some(name) = name.strip_prefix(&bucket_prefix) else {
                continue;
            };
            if name.starts_with(prefix) {
                let _ = write!(
                    contents,
                    "<Contents><Key>{}</Key><Size>0</Size></Contents>",
                    escape_xml(name)
                );
            }
        }
        return xml(
            StatusCode::OK,
            &format!(
                "<ListBucketResult><Name>{bucket}</Name><IsTruncated>false</IsTruncated>\
                 <KeyCount>0</KeyCount>{contents}</ListBucketResult>"
            ),
        );
    }

    if method == Method::POST && !key.is_empty() && has(&query, "uploads") {
        let id = state.next_upload.fetch_add(1, Ordering::Relaxed);
        let upload_id = format!("upload-{id}");
        if let Ok(mut uploads) = state.uploads.lock() {
            uploads.insert(
                upload_id.clone(),
                Upload {
                    key: object_key,
                    parts: BTreeMap::new(),
                },
            );
        }
        state.counts.uploads.fetch_add(1, Ordering::Relaxed);
        return xml(
            StatusCode::OK,
            &format!(
                "<InitiateMultipartUploadResult><UploadId>{upload_id}</UploadId>\
                 </InitiateMultipartUploadResult>"
            ),
        );
    }

    if method == Method::PUT && has(&query, "partNumber") {
        let upload_id = get(&query, "uploadId").unwrap_or_default().to_owned();
        let number = get(&query, "partNumber")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let current = state
            .counts
            .in_flight
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        state
            .counts
            .max_in_flight
            .fetch_max(current, Ordering::AcqRel);
        let delay = state.part_delay.lock().ok().and_then(|delay| *delay);
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        let fail = state
            .fail_part
            .lock()
            .ok()
            .and_then(|fail| *fail)
            .is_some_and(|failed| failed == number);
        state.counts.in_flight.fetch_sub(1, Ordering::AcqRel);
        if fail {
            return xml(
                StatusCode::INTERNAL_SERVER_ERROR,
                "<Error><Code>InternalError</Code></Error>",
            );
        }
        let length = u64::try_from(body.len()).unwrap_or(0);
        let Ok(mut uploads) = state.uploads.lock() else {
            return empty(StatusCode::INTERNAL_SERVER_ERROR);
        };
        let Some(upload) = uploads.get_mut(&upload_id) else {
            return xml(
                StatusCode::NOT_FOUND,
                "<Error><Code>NoSuchUpload</Code></Error>",
            );
        };
        upload.parts.insert(number, body);
        drop(uploads);
        if let Ok(mut sizes) = state.part_sizes.lock() {
            sizes.push(length);
        }
        state.counts.parts.fetch_add(1, Ordering::Relaxed);
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::OK;
        if let Ok(value) = HeaderValue::from_str(&format!("\"part-{number}\"")) {
            response.headers_mut().insert(header::ETAG, value);
        }
        return response;
    }

    if method == Method::POST && has(&query, "uploadId") {
        let upload_id = get(&query, "uploadId").unwrap_or_default().to_owned();
        let Some(upload) = state
            .uploads
            .lock()
            .ok()
            .and_then(|mut uploads| uploads.remove(&upload_id))
        else {
            return xml(
                StatusCode::NOT_FOUND,
                "<Error><Code>NoSuchUpload</Code></Error>",
            );
        };
        let mut bytes = Vec::new();
        for part in upload.parts.values() {
            bytes.extend_from_slice(part);
        }
        let body = Bytes::from(bytes);
        let etag = format!("\"{}-{}\"", md5_hex(&body), upload.parts.len());
        if let Ok(mut objects) = state.objects.lock() {
            objects.insert(
                upload.key,
                Stored {
                    body,
                    etag: etag.clone(),
                },
            );
        }
        state.counts.completes.fetch_add(1, Ordering::Relaxed);
        return xml(
            StatusCode::OK,
            &format!(
                "<CompleteMultipartUploadResult><ETag>{}</ETag>\
                 </CompleteMultipartUploadResult>",
                escape_xml(&etag)
            ),
        );
    }

    if method == Method::DELETE && has(&query, "uploadId") {
        let upload_id = get(&query, "uploadId").unwrap_or_default().to_owned();
        if let Ok(mut uploads) = state.uploads.lock() {
            uploads.remove(&upload_id);
        }
        state.counts.aborts.fetch_add(1, Ordering::Relaxed);
        return empty(StatusCode::NO_CONTENT);
    }

    if method == Method::POST && key.is_empty() && has(&query, "delete") {
        let mut result = String::from("<DeleteResult>");
        for key in parse_keys(&body) {
            let removed = state
                .objects
                .lock()
                .is_ok_and(|mut objects| objects.remove(&format!("{bucket}/{key}")).is_some());
            if removed {
                state.counts.deletes.fetch_add(1, Ordering::Relaxed);
            }
            let _ = write!(result, "<Deleted><Key>{}</Key></Deleted>", escape_xml(&key));
        }
        result.push_str("</DeleteResult>");
        return xml(StatusCode::OK, &result);
    }

    if key.is_empty() {
        return empty(StatusCode::OK);
    }

    match method {
        Method::PUT => {
            state.counts.puts.fetch_add(1, Ordering::Relaxed);
            let etag = md5_hex(&body);
            if let Ok(mut objects) = state.objects.lock() {
                objects.insert(
                    object_key,
                    Stored {
                        body,
                        etag: etag.clone(),
                    },
                );
            }
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::OK;
            if let Ok(value) = HeaderValue::from_str(&format!("\"{etag}\"")) {
                response.headers_mut().insert(header::ETAG, value);
            }
            response
        }
        Method::GET | Method::HEAD => {
            state.counts.gets.fetch_add(1, Ordering::Relaxed);
            let stored = state
                .objects
                .lock()
                .ok()
                .and_then(|objects| objects.get(&object_key).cloned());
            let Some(stored) = stored else {
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
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&stored.body.len().to_string())
                    .unwrap_or_else(|_| HeaderValue::from_static("0")),
            );
            if let Ok(value) = HeaderValue::from_str(&stored.etag) {
                response.headers_mut().insert(header::ETAG, value);
            }
            if let Some(value) = headers.get(header::CONTENT_TYPE) {
                response
                    .headers_mut()
                    .insert(header::CONTENT_TYPE, value.clone());
            }
            response
        }
        Method::DELETE => {
            state.counts.deletes.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut objects) = state.objects.lock() {
                objects.remove(&object_key);
            }
            empty(StatusCode::NO_CONTENT)
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
        Some((bucket, key)) => (decode(bucket), decode(key)),
        None => (decode(trimmed), String::new()),
    }
}

fn decode(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .decode_utf8_lossy()
        .into_owned()
}

fn query_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(key), decode(value))
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

fn parse_keys(body: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(body);
    let mut keys = Vec::new();
    let mut rest = text.as_ref();
    while let Some(start) = rest.find("<Key>") {
        rest = &rest[start + "<Key>".len()..];
        let Some(end) = rest.find("</Key>") else {
            break;
        };
        keys.push(unescape_xml(&rest[..end]));
        rest = &rest[end + "</Key>".len()..];
    }
    keys
}

fn unescape_xml(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
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

fn empty(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}
