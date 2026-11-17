#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::Response;
use reqwest::Url;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, CredentialStore, EMPTY_SHA256};
use s3_sidecar::config::secret::SecretString;
use s3_sidecar::config::{
    CacheConfig, CacheMode, ClientConfig, Config, EndpointConfig, OnCacheError, ServerConfig,
    TlsConfig, UpstreamConfig,
};
use s3_sidecar::server::{ServeError, Server, start};
use time::OffsetDateTime;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::sleep;

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    headers: HeaderMap,
}

#[derive(Clone)]
struct OriginState {
    records: Arc<std::sync::Mutex<Vec<Recorded>>>,
    started: Arc<Notify>,
    delay: Duration,
    get_503: Arc<AtomicUsize>,
    put_503: bool,
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
        headers: headers.clone(),
    });
    let _ = uri.path();
    state.started.notify_one();
    if !state.delay.is_zero() {
        sleep(state.delay).await;
    }
    let _ = body;
    if method == Method::GET {
        if state
            .get_503
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_sub(1)
            })
            .is_ok()
        {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("0"));
            return response;
        }
        let mut response = Response::new(Body::from("hello"));
        response
            .headers_mut()
            .insert(header::ETAG, HeaderValue::from_static("\"etag\""));
        return response;
    }
    if method == Method::PUT && state.put_503 {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
        return response;
    }
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    response
}

async fn spawn_origin(delay: Duration, get_503: usize, put_503: bool) -> (String, OriginState) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = OriginState {
        records: Arc::new(std::sync::Mutex::new(Vec::new())),
        started: Arc::new(Notify::new()),
        delay,
        get_503: Arc::new(AtomicUsize::new(get_503)),
        put_503,
    };
    let app = Router::new()
        .fallback(origin_handler)
        .with_state(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), state)
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
        mode: s3_sidecar::config::Mode::Cache,
        server: ServerConfig {
            bind: "127.0.0.1:0".to_owned(),
            ..ServerConfig::default()
        },
        upstream: UpstreamConfig::default(),
        cache: CacheConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            bucket: "cachebucket".to_owned(),
            mode: CacheMode::Read,
            max_bytes: Some(0),
            on_cache_error: OnCacheError::Origin,
            ..CacheConfig::default()
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

fn credential() -> Arc<ClientCredential> {
    CredentialStore::new(&[client_config()])
        .lookup("AKIA")
        .unwrap()
}

fn signed_builder(
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

async fn spawn(config: Config) -> (Server, String) {
    let server = start(config, &[]).await.unwrap();
    let url = format!("http://{}", server.local_addr());
    (server, url)
}

async fn stop(server: Server) {
    server.shutdown();
    let _ = server.wait().await;
}

#[tokio::test]
async fn tls_handshake_serves_the_s3_api() {
    let (origin, _) = spawn_origin(Duration::ZERO, 0, false).await;
    let mut config = config(origin);
    config.server.tls = Some(TlsConfig {
        cert_file: certificate(),
        key_file: key(),
    });
    let server = start(config, &[]).await.unwrap();
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let base = format!("https://{}", server.local_addr());

    let health = client.get(format!("{base}/healthz")).send().await.unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let response = signed_builder(
        &client,
        Method::GET,
        &format!("{base}/media/item"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-s3-sidecar-request-id"));
    assert_eq!(response.text().await.unwrap(), "hello");
    stop(server).await;
}

#[tokio::test]
async fn tls_startup_fails_for_a_missing_or_invalid_certificate() {
    let (origin, _) = spawn_origin(Duration::ZERO, 0, false).await;
    let mut missing = config(origin.clone());
    missing.server.tls = Some(TlsConfig {
        cert_file: "/nonexistent/cert.pem".to_owned(),
        key_file: key(),
    });
    assert!(matches!(start(missing, &[]).await, Err(ServeError::Tls(_))));

    let dir = tempfile::tempdir().unwrap();
    let invalid = dir.path().join("invalid.pem");
    std::fs::write(&invalid, "not a certificate").unwrap();
    let mut malformed = config(origin);
    malformed.server.tls = Some(TlsConfig {
        cert_file: invalid.display().to_string(),
        key_file: key(),
    });
    assert!(matches!(
        start(malformed, &[]).await,
        Err(ServeError::Tls(_))
    ));
}

#[tokio::test]
async fn graceful_shutdown_lets_an_in_flight_request_finish() {
    let (origin, state) = spawn_origin(Duration::from_millis(300), 0, false).await;
    let mut config = config(origin);
    config.server.shutdown_timeout =
        s3_sidecar::config::duration::HumanDuration::new(Duration::from_secs(5));
    let (server, base) = spawn(config).await;
    let address = server.local_addr();
    let client = reqwest::Client::new();
    let request = tokio::spawn(async move {
        signed_builder(
            &client,
            Method::GET,
            &format!("{base}/media/item"),
            None,
            &[],
        )
        .send()
        .await
        .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(1), state.started.notified())
        .await
        .unwrap();
    server.shutdown();
    let response = request.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    server.wait().await.unwrap();
    assert!(TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn shutdown_timeout_returns_after_the_bound() {
    let (origin, state) = spawn_origin(Duration::from_secs(2), 0, false).await;
    let mut config = config(origin);
    config.server.shutdown_timeout =
        s3_sidecar::config::duration::HumanDuration::new(Duration::from_millis(200));
    let (server, base) = spawn(config).await;
    let client = reqwest::Client::new();
    let request = tokio::spawn(async move {
        signed_builder(
            &client,
            Method::GET,
            &format!("{base}/media/item"),
            None,
            &[],
        )
        .send()
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), state.started.notified())
        .await
        .unwrap();
    let started = Instant::now();
    server.shutdown();
    let result = server.wait().await;
    assert!(matches!(result, Err(ServeError::ShutdownTimeout)));
    assert!(started.elapsed() < Duration::from_secs(2));
    let _ = request.await;
}

#[tokio::test]
async fn content_length_over_the_limit_returns_413() {
    let (origin, state) = spawn_origin(Duration::ZERO, 0, false).await;
    let mut config = config(origin);
    config.server.max_body_bytes = 16;
    let (server, base) = spawn(config).await;
    let client = reqwest::Client::new();
    let response = signed_builder(
        &client,
        Method::PUT,
        &format!("{base}/media/too-large"),
        Some(vec![b'x'; 32]),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = response.text().await.unwrap();
    assert!(body.contains("<Code>EntityTooLarge</Code>"));
    assert!(state.records.lock().unwrap().is_empty());
    stop(server).await;
}

#[tokio::test]
async fn aws_chunked_body_with_a_decoded_length_under_the_limit_succeeds() {
    let (origin, _) = spawn_origin(Duration::ZERO, 0, false).await;
    let mut config = config(origin);
    let max_body_bytes = 16;
    config.server.max_body_bytes = max_body_bytes;
    let (server, base) = spawn(config).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/media/aws-chunked-within-limit");
    let parsed = Url::parse(&url).unwrap();
    let credential = credential();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_ENCODING,
        HeaderValue::from_static("aws-chunked"),
    );
    headers.insert(
        "x-amz-decoded-content-length",
        HeaderValue::from_static("11"),
    );
    sign_request(
        &Method::PUT,
        &parsed,
        &mut headers,
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        &credential,
        "us-east-1",
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    // The encoded body is 26 bytes and exceeds the limit. The decoded object
    // is 11 bytes and stays under the limit.
    let body = "6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n";
    assert!(body.len() as u64 > max_body_bytes);
    let mut builder = client.request(Method::PUT, url).body(body.to_owned());
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    let response = builder.send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    stop(server).await;
}

#[tokio::test]
async fn a_chunked_body_over_the_limit_returns_413() {
    let (origin, _) = spawn_origin(Duration::ZERO, 0, false).await;
    let mut config = config(origin);
    config.server.max_body_bytes = 16;
    let (server, base) = spawn(config).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/media/chunked-large");
    let parsed = Url::parse(&url).unwrap();
    let credential = credential();
    let mut headers = HeaderMap::new();
    sign_request(
        &Method::PUT,
        &parsed,
        &mut headers,
        "UNSIGNED-PAYLOAD",
        &credential,
        "us-east-1",
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    let chunks = futures_util::stream::iter(vec![
        Ok::<Bytes, io::Error>(Bytes::from(vec![b'x'; 8])),
        Ok::<Bytes, io::Error>(Bytes::from(vec![b'x'; 8])),
        Ok::<Bytes, io::Error>(Bytes::from(vec![b'x'; 8])),
    ]);
    let mut builder = client
        .request(Method::PUT, url)
        .body(reqwest::Body::wrap_stream(chunks));
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    let response = builder.send().await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = response.text().await.unwrap();
    assert!(body.contains("<Code>EntityTooLarge</Code>"));
    stop(server).await;
}

#[tokio::test]
async fn an_upstream_timeout_returns_504() {
    let (origin, _) = spawn_origin(Duration::from_millis(500), 0, false).await;
    let mut config = config(origin);
    config.upstream.request_timeout =
        s3_sidecar::config::duration::HumanDuration::new(Duration::from_millis(100));
    config.upstream.retry_attempts = 0;
    let (server, base) = spawn(config).await;
    let client = reqwest::Client::new();
    let response = signed_builder(
        &client,
        Method::GET,
        &format!("{base}/media/item"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let body = response.text().await.unwrap();
    assert!(body.contains("<Code>RequestTimeout</Code>"));
    stop(server).await;
}

#[tokio::test]
async fn a_get_retries_once_after_503_and_keeps_the_request_id() {
    let (origin, state) = spawn_origin(Duration::ZERO, 1, false).await;
    let mut config = config(origin);
    config.upstream.retry_attempts = 1;
    let (server, base) = spawn(config).await;
    let client = reqwest::Client::new();
    let response = signed_builder(
        &client,
        Method::GET,
        &format!("{base}/media/item"),
        None,
        &[("x-s3-sidecar-request-id", "client-request-id")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-s3-sidecar-request-id").unwrap(),
        "client-request-id"
    );
    {
        let records = state.records.lock().unwrap();
        assert_eq!(records.len(), 2);
        for record in records.iter() {
            assert_eq!(
                record.headers.get("x-s3-sidecar-request-id").unwrap(),
                "client-request-id"
            );
        }
    }
    stop(server).await;
}

#[tokio::test]
async fn a_put_with_a_body_is_not_retried() {
    let (origin, state) = spawn_origin(Duration::ZERO, 0, true).await;
    let mut config = config(origin);
    config.upstream.retry_attempts = 2;
    let (server, base) = spawn(config).await;
    let client = reqwest::Client::new();
    let response = signed_builder(
        &client,
        Method::PUT,
        &format!("{base}/media/item"),
        Some(b"body".to_vec()),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    {
        let records = state.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].method, "PUT");
    }
    stop(server).await;
}

#[tokio::test]
async fn a_generated_request_id_is_returned_to_the_client() {
    let (origin, _) = spawn_origin(Duration::ZERO, 0, false).await;
    let (server, base) = spawn(config(origin)).await;
    let client = reqwest::Client::new();
    let response = signed_builder(
        &client,
        Method::GET,
        &format!("{base}/media/item"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    let id = response
        .headers()
        .get("x-s3-sidecar-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_ne!(id, "");
    stop(server).await;
}

fn certificate() -> String {
    format!("{}/tests/data/test-cert.pem", env!("CARGO_MANIFEST_DIR"))
}

fn key() -> String {
    format!("{}/tests/data/test-key.pem", env!("CARGO_MANIFEST_DIR"))
}
