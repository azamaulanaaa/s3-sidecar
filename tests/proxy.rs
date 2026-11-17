#![expect(clippy::unwrap_used, reason = "test code uses unwrap for fixtures")]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use reqwest::Url;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{
    ALGORITHM, ClientCredential, CredentialStore, EMPTY_SHA256, canonical_uri, credential_scope,
    format_amz_date, format_scope_date, hmac_sha256, now, sha256_hex, signing_key,
};
use s3_sidecar::config::secret::SecretString;
use s3_sidecar::config::{CacheMode, ClientConfig, Config, EndpointConfig, Mode, OnCacheError};
use s3_sidecar::server::{AppState, router};
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    query: Option<String>,
    headers: HeaderMap,
    body_length: usize,
}

#[derive(Clone)]
struct OriginState {
    records: Arc<Mutex<Vec<Recorded>>>,
}

async fn origin_handler(
    State(state): State<OriginState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.records.lock().unwrap().push(Recorded {
        method: method.to_string(),
        path: uri.path().to_owned(),
        query: uri.query().map(str::to_owned),
        headers: headers.clone(),
        body_length: body.len(),
    });

    let query = uri.query().unwrap_or_default();
    if uri.path() == "/media/missing" {
        return xml(
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchKey</Code><Message>The key does not exist.</Message></Error>",
        );
    }
    if uri.path() == "/media/range" {
        let mut response = Response::new(Body::from("hello"));
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_static("bytes 0-4/11"),
        );
        response
            .headers_mut()
            .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        return response;
    }
    if uri.path() == "/media" && query.contains("list-type=2") {
        return xml(
            StatusCode::OK,
            "<ListBucketResult><Name>media</Name><KeyCount>1</KeyCount>\
             <Contents><Key>item</Key><Size>5</Size></Contents></ListBucketResult>",
        );
    }
    if method == Method::POST && query.contains("uploads") {
        return xml(
            StatusCode::OK,
            "<InitiateMultipartUploadResult><Bucket>media</Bucket><Key>item</Key>\
             <UploadId>upload-1</UploadId></InitiateMultipartUploadResult>",
        );
    }
    if method == Method::PUT && query.contains("partNumber") {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(header::ETAG, HeaderValue::from_static("\"part-etag\""));
        return response;
    }
    if method == Method::POST && query.contains("uploadId") {
        return xml(
            StatusCode::OK,
            "<CompleteMultipartUploadResult><Bucket>media</Bucket><Key>item</Key>\
             <ETag>\"complete-etag\"</ETag></CompleteMultipartUploadResult>",
        );
    }
    if method == Method::GET && query.contains("uploadId") {
        return xml(
            StatusCode::OK,
            "<ListPartsResult><Bucket>media</Bucket><Key>item</Key>\
             <UploadId>upload-1</UploadId></ListPartsResult>",
        );
    }
    if method == Method::DELETE {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NO_CONTENT;
        return response;
    }
    if method == Method::PUT && headers.contains_key("x-amz-copy-source") {
        return xml(
            StatusCode::OK,
            "<CopyObjectResult><ETag>\"copy-etag\"</ETag></CopyObjectResult>",
        );
    }

    let mut response = Response::new(Body::from("hello"));
    response
        .headers_mut()
        .insert(header::ETAG, HeaderValue::from_static("\"etag\""));
    response.headers_mut().insert(
        header::LAST_MODIFIED,
        HeaderValue::from_static("Tue, 24 May 2013 00:00:00 GMT"),
    );
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

async fn spawn_origin() -> (String, Arc<Mutex<Vec<Recorded>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let records = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .fallback(origin_handler)
        .with_state(OriginState {
            records: Arc::clone(&records),
        });
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), records)
}

fn client_config() -> ClientConfig {
    ClientConfig {
        access_key_id: "AKIA".to_owned(),
        secret_access_key: SecretString::new("secret"),
        session_token: None,
        buckets: vec!["media".to_owned()],
    }
}

fn config(origin: String) -> Config {
    Config {
        mode: Mode::Cache,
        cache: s3_sidecar::config::CacheConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            bucket: "cachebucket".to_owned(),
            mode: CacheMode::Write,
            max_bytes: Some(0),
            on_cache_error: OnCacheError::Origin,
            ..s3_sidecar::config::CacheConfig::default()
        },
        origin: EndpointConfig {
            endpoint: origin,
            bucket: String::new(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKIA".to_owned(),
            secret_access_key: SecretString::new("secret"),
            session_token: None,
            force_path_style: true,
        },
        clients: vec![client_config()],
        ..Config::default()
    }
}

async fn spawn_sidecar(origin: String) -> (String, Arc<AppState>) {
    let state = Arc::new(AppState::new(config(origin)).unwrap());
    state.start().await.unwrap();
    state.set_ready(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    let app = router(Arc::clone(&state));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), state)
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

fn raw_authorization(method: &str, host: &str, path: &str, payload_hash: &str) -> (String, String) {
    let timestamp = now();
    let date = format_amz_date(timestamp);
    let scope_date = format_scope_date(timestamp);
    let scope = credential_scope(&scope_date, "us-east-1", "s3");
    let credential = credential();
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "{method}\n{}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        canonical_uri(path)
    );
    let to_sign = format!(
        "{ALGORITHM}\n{date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let key = signing_key(
        credential.secret_access_key(),
        &scope_date,
        "us-east-1",
        "s3",
    );
    let signature = hex::encode(hmac_sha256(&key, to_sign.as_bytes()));
    let authorization = format!(
        "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        credential.access_key_id(),
    );
    (date, authorization)
}

/// Send a signed request with a raw target. A URL parser would resolve the dot
/// segments in the target, so a socket writes the request line directly.
async fn raw_signed_request(
    sidecar: &str,
    method: &str,
    path: &str,
    payload_hash: &str,
    body: &[u8],
) -> String {
    use std::fmt::Write as _;

    let host = sidecar.trim_start_matches("http://").to_owned();
    let (date, authorization) = raw_authorization(method, &host, path, payload_hash);
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nx-amz-date: {date}\r\n\
         x-amz-content-sha256: {payload_hash}\r\nAuthorization: {authorization}\r\n\
         Connection: close\r\n"
    );
    if !body.is_empty() {
        write!(request, "Content-Length: {}\r\n", body.len()).unwrap();
    }
    request.push_str("\r\n");

    let mut stream = tokio::net::TcpStream::connect(&host).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    stream.flush().await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8_lossy(&response).into_owned()
}

#[tokio::test]
async fn dot_segments_in_object_keys_are_preserved() {
    let (origin, records) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;

    let response = raw_signed_request(&sidecar, "GET", "/media/a/../b", EMPTY_SHA256, &[]).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    let response = raw_signed_request(
        &sidecar,
        "PUT",
        "/media/a/../b",
        "UNSIGNED-PAYLOAD",
        b"dot-body",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    let records = records.lock().unwrap();
    assert_eq!(records.len(), 2);
    for record in records.iter() {
        let decoded = percent_encoding::percent_decode_str(&record.path)
            .decode_utf8()
            .unwrap();
        assert_eq!(decoded, "/media/a/../b");
        assert_ne!(record.path, "/media/b");
    }
}

#[tokio::test]
async fn health_and_readiness_respond() {
    let (origin, _) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let client = reqwest::Client::new();
    let health = client
        .get(format!("{sidecar}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    let ready = client
        .get(format!("{sidecar}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::OK);
}

#[tokio::test]
async fn metrics_endpoint_is_removed() {
    let (origin, _) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let response = reqwest::Client::new()
        .get(format!("{sidecar}/metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn signed_get_is_forwarded_and_response_headers_pass_through() {
    let (origin, records) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::GET,
        &format!("{sidecar}/media/item"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(header::ETAG).unwrap(), "\"etag\"");
    assert_eq!(body_text(response).await, "hello");

    let records = records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].method, "GET");
    assert_eq!(records[0].path, "/media/item");
    assert!(records[0].headers.contains_key("authorization"));
}

#[tokio::test]
async fn range_request_passes_through() {
    let (origin, records) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::GET,
        &format!("{sidecar}/media/range"),
        None,
        &[("range", "bytes=0-4")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers().get(header::CONTENT_RANGE).unwrap(),
        "bytes 0-4/11"
    );
    assert_eq!(
        response.headers().get(header::ACCEPT_RANGES).unwrap(),
        "bytes"
    );
    assert_eq!(body_text(response).await, "hello");
    assert_eq!(
        records.lock().unwrap()[0]
            .headers
            .get("range")
            .unwrap()
            .to_str()
            .unwrap(),
        "bytes=0-4"
    );
}

#[tokio::test]
async fn missing_key_returns_upstream_xml() {
    let (origin, _) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::GET,
        &format!("{sidecar}/media/missing"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/xml"
    );
    let body = body_text(response).await;
    assert!(body.contains("<Code>NoSuchKey</Code>"));
}

#[tokio::test]
async fn unsigned_request_is_rejected() {
    let (origin, records) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{sidecar}/media/item"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        body_text(response)
            .await
            .contains("<Code>AccessDenied</Code>")
    );
    assert!(records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn wrong_signature_is_rejected() {
    let (origin, records) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let client = reqwest::Client::new();
    let wrong_store = CredentialStore::new(&[ClientConfig {
        access_key_id: "AKIA".to_owned(),
        secret_access_key: SecretString::new("wrong-secret"),
        session_token: None,
        buckets: vec!["media".to_owned()],
    }]);
    let credential = wrong_store.lookup("AKIA").unwrap();
    let url = format!("{sidecar}/media/item");
    let parsed = Url::parse(&url).unwrap();
    let method = Method::GET;
    let mut headers = HeaderMap::new();
    sign_request(
        &method,
        &parsed,
        &mut headers,
        EMPTY_SHA256,
        &credential,
        "us-east-1",
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    let mut builder = client.request(method, &url);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    let response = builder.send().await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        body_text(response)
            .await
            .contains("<Code>SignatureDoesNotMatch</Code>")
    );
    assert!(records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn bucket_allow_list_is_enforced() {
    let (origin, records) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::GET,
        &format!("{sidecar}/other/item"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        body_text(response)
            .await
            .contains("<Code>AccessDenied</Code>")
    );
    assert!(records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn copy_source_bucket_is_checked() {
    let (origin, records) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let client = reqwest::Client::new();
    let denied = signed_request(
        &client,
        Method::PUT,
        &format!("{sidecar}/media/copied"),
        None,
        &[("x-amz-copy-source", "/other/item")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert!(records.lock().unwrap().is_empty());

    let allowed = signed_request(
        &client,
        Method::PUT,
        &format!("{sidecar}/media/copied"),
        None,
        &[("x-amz-copy-source", "/media/item")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    assert!(body_text(allowed).await.contains("CopyObjectResult"));
    assert_eq!(
        records.lock().unwrap()[0]
            .headers
            .get("x-amz-copy-source")
            .unwrap()
            .to_str()
            .unwrap(),
        "/media/item"
    );
}

#[tokio::test]
async fn list_objects_v2_query_passes_through() {
    let (origin, records) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::GET,
        &format!(
            "{sidecar}/media?list-type=2&prefix=a&delimiter=%2F&max-keys=2&continuation-token=t"
        ),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(body_text(response).await.contains("ListBucketResult"));
    assert_eq!(
        records.lock().unwrap()[0].query.as_deref(),
        Some("list-type=2&prefix=a&delimiter=%2F&max-keys=2&continuation-token=t")
    );
}

#[tokio::test]
async fn multipart_and_delete_pass_through() {
    let (origin, records) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let client = reqwest::Client::new();

    let created = signed_request(
        &client,
        Method::POST,
        &format!("{sidecar}/media/item?uploads"),
        Some(Vec::new()),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    assert!(body_text(created).await.contains("upload-1"));

    let part = signed_request(
        &client,
        Method::PUT,
        &format!("{sidecar}/media/item?partNumber=1&uploadId=upload-1"),
        Some(b"part-data".to_vec()),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(part.status(), StatusCode::OK);
    assert_eq!(part.headers().get(header::ETAG).unwrap(), "\"part-etag\"");

    let completed = signed_request(
        &client,
        Method::POST,
        &format!("{sidecar}/media/item?uploadId=upload-1"),
        Some(b"<CompleteMultipartUpload></CompleteMultipartUpload>".to_vec()),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(completed.status(), StatusCode::OK);
    assert!(body_text(completed).await.contains("complete-etag"));

    let parts = signed_request(
        &client,
        Method::GET,
        &format!("{sidecar}/media/item?uploadId=upload-1"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(parts.status(), StatusCode::OK);
    assert!(body_text(parts).await.contains("ListPartsResult"));

    let aborted = signed_request(
        &client,
        Method::DELETE,
        &format!("{sidecar}/media/item?uploadId=upload-1"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(aborted.status(), StatusCode::NO_CONTENT);

    let records = records.lock().unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(
        records[1].query.as_deref(),
        Some("partNumber=1&uploadId=upload-1")
    );
    assert_eq!(records[1].body_length, 9);
}

#[tokio::test]
async fn presigned_get_is_verified() {
    let (origin, _) = spawn_origin().await;
    let (sidecar, _) = spawn_sidecar(origin).await;
    let url = presigned_url(&format!("{sidecar}/media/item"));
    let response = reqwest::Client::new().get(url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response).await, "hello");
}

fn presigned_url(url: &str) -> String {
    let timestamp = now();
    let date = format_amz_date(timestamp);
    let scope_date = format_scope_date(timestamp);
    let scope = credential_scope(&scope_date, "us-east-1", "s3");
    let credential = credential();
    let mut parsed = Url::parse(url).unwrap();
    parsed
        .query_pairs_mut()
        .append_pair("X-Amz-Algorithm", ALGORITHM)
        .append_pair(
            "X-Amz-Credential",
            &format!("AKIA/{scope_date}/us-east-1/s3/aws4_request"),
        )
        .append_pair("X-Amz-Date", &date)
        .append_pair("X-Amz-Expires", "3600")
        .append_pair("X-Amz-SignedHeaders", "host");
    let host = format!("{}:{}", parsed.host_str().unwrap(), parsed.port().unwrap());
    let canonical_headers = format!("host:{host}\n");
    let query = s3_sidecar::auth::canonical_query(parsed.query(), None);
    let canonical_request = format!(
        "GET\n{}\n{}\n{}\nhost\nUNSIGNED-PAYLOAD",
        parsed.path(),
        query,
        canonical_headers
    );
    let to_sign = format!(
        "{ALGORITHM}\n{date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let key = signing_key(
        credential.secret_access_key(),
        &scope_date,
        "us-east-1",
        "s3",
    );
    let signature = hex::encode(hmac_sha256(&key, to_sign.as_bytes()));
    parsed
        .query_pairs_mut()
        .append_pair("X-Amz-Signature", &signature);
    parsed.to_string()
}
