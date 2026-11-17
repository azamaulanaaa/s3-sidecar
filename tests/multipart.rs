#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code uses unwrap and expect for fixtures"
)]

//! Tests for the shared multipart writer and its data-path callers.

mod support;

use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{HeaderMap, Method, header};
use bytes::Bytes;
use reqwest::Url;
use s3_sidecar::auth::body::BodyStream;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, CredentialStore, EMPTY_SHA256};
use s3_sidecar::config::duration::HumanDuration;
use s3_sidecar::config::loader::{LoadOptions, Source, load};
use s3_sidecar::config::secret::SecretString;
use s3_sidecar::config::size::Size;
use s3_sidecar::config::{
    BucketConfig, CacheConfig, CacheMode, ClientConfig, ClusterConfig, Config, ConsensusBackend,
    ConsensusConfig, EndpointConfig, Mode, MultipartConfig, OnCacheError, RaftConfig, WriteAck,
    WritePolicy, validate,
};
use s3_sidecar::metrics::Metrics;
use s3_sidecar::multipart::put_object;
use s3_sidecar::server::{AppState, router};
use s3_sidecar::storage::S3Client;
use serde_json::json;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

fn multipart_config(threshold: u64, chunk: u64) -> MultipartConfig {
    MultipartConfig {
        threshold: Size::new(threshold),
        chunk_size: Size::new(chunk),
        min_chunk_size: Size::new(1),
        ..MultipartConfig::default()
    }
}

fn endpoint(url: &str) -> EndpointConfig {
    EndpointConfig {
        endpoint: url.to_owned(),
        bucket: String::new(),
        region: "us-east-1".to_owned(),
        access_key_id: "AKIA".to_owned(),
        secret_access_key: SecretString::new("secret"),
        session_token: None,
        force_path_style: true,
    }
}

fn client_for(url: &str) -> S3Client {
    S3Client::new(&endpoint(url)).unwrap()
}

fn credential() -> ClientCredential {
    ClientCredential::new(
        "AKIA",
        SecretString::new("secret"),
        None,
        Vec::<String>::new(),
    )
}

fn body_stream(body: Vec<u8>) -> BodyStream {
    let bytes = Bytes::from(body);
    Box::pin(futures_util::stream::once(async move { Ok(bytes) }))
}

#[tokio::test]
async fn body_below_threshold_uses_one_put() {
    let (url, s3) = support::fake_s3::spawn().await;
    let body = vec![7_u8; 100];
    let response = put_object(
        &client_for(&url),
        &credential(),
        "media",
        "item",
        HeaderMap::new(),
        body_stream(body.clone()),
        Some(100),
        &multipart_config(1_024, 512),
        &Metrics::default(),
    )
    .await
    .unwrap();
    assert!(response.status().is_success());
    assert_eq!(s3.puts(), 1);
    assert_eq!(s3.uploads(), 0);
    assert_eq!(s3.object("media/item"), Some(Bytes::from(body)));
}

#[tokio::test]
async fn body_at_threshold_uses_parts_of_the_chunk_size() {
    let (url, s3) = support::fake_s3::spawn().await;
    let body = vec![3_u8; 250];
    let response = put_object(
        &client_for(&url),
        &credential(),
        "media",
        "item",
        HeaderMap::new(),
        body_stream(body.clone()),
        Some(250),
        &multipart_config(250, 100),
        &Metrics::default(),
    )
    .await
    .unwrap();
    assert!(response.status().is_success());
    assert_eq!(s3.puts(), 0);
    assert_eq!(s3.uploads(), 1);
    assert_eq!(s3.completes(), 1);
    assert_eq!(s3.parts(), 3);
    let mut sizes = s3.part_sizes();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![50, 100, 100]);
    assert_eq!(s3.object("media/item"), Some(Bytes::from(body)));
}

#[tokio::test]
async fn complete_etag_is_taken_from_the_body() {
    let (url, s3) = support::fake_s3::spawn().await;
    let response = put_object(
        &client_for(&url),
        &credential(),
        "media",
        "item",
        HeaderMap::new(),
        body_stream(vec![4_u8; 200]),
        Some(200),
        &multipart_config(100, 100),
        &Metrics::default(),
    )
    .await
    .unwrap();
    assert_eq!(s3.completes(), 1);
    let etag = response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .unwrap();
    assert!(etag.starts_with('"'));
    assert!(etag.ends_with("-2\""));
    assert!(response.bytes().await.unwrap().is_empty());
}

#[tokio::test]
async fn unknown_length_uses_one_put() {
    let (url, s3) = support::fake_s3::spawn().await;
    let body = vec![9_u8; 500];
    let response = put_object(
        &client_for(&url),
        &credential(),
        "media",
        "item",
        HeaderMap::new(),
        body_stream(body.clone()),
        None,
        &multipart_config(100, 100),
        &Metrics::default(),
    )
    .await
    .unwrap();
    assert!(response.status().is_success());
    assert_eq!(s3.puts(), 1);
    assert_eq!(s3.uploads(), 0);
    assert_eq!(s3.object("media/item"), Some(Bytes::from(body)));
}

#[tokio::test]
async fn a_failed_part_aborts_the_upload() {
    let (url, s3) = support::fake_s3::spawn().await;
    s3.fail_part(2);
    let metrics = support::TestMetrics::new();
    let error = put_object(
        &client_for(&url),
        &credential(),
        "media",
        "item",
        HeaderMap::new(),
        body_stream(vec![1_u8; 300]),
        Some(300),
        &multipart_config(100, 100),
        &metrics.metrics,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("status"));
    assert_eq!(s3.aborts(), 1);
    assert!(!s3.has("media/item"));
    let data = metrics.collect();
    assert_eq!(support::counter(&data, "s3_sidecar.multipart.aborts"), 1);
    assert_eq!(
        support::counter_with(&data, "s3_sidecar.multipart.failures", "reason", "part"),
        1
    );
}

#[tokio::test]
async fn max_parts_raises_the_part_size() {
    let (url, s3) = support::fake_s3::spawn().await;
    let mib = 1_048_576_u64;
    let body = vec![5_u8; usize::try_from(4 * mib + 1).unwrap()];
    let config = MultipartConfig {
        enabled: true,
        threshold: Size::new(1),
        chunk_size: Size::new(mib),
        min_chunk_size: Size::new(1),
        max_parts: 4,
        concurrency: 2,
    };
    let response = put_object(
        &client_for(&url),
        &credential(),
        "media",
        "item",
        HeaderMap::new(),
        body_stream(body.clone()),
        Some(4 * mib + 1),
        &config,
        &Metrics::default(),
    )
    .await
    .unwrap();
    assert!(response.status().is_success());
    assert_eq!(s3.uploads(), 1);
    assert_eq!(s3.parts(), 3);
    let mut sizes = s3.part_sizes();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![1, 2 * mib, 2 * mib]);
    assert_eq!(s3.object("media/item"), Some(Bytes::from(body)));
}

#[tokio::test]
async fn concurrency_bounds_in_flight_part_uploads() {
    let (url, s3) = support::fake_s3::spawn().await;
    s3.delay_parts(Duration::from_millis(25));
    let config = MultipartConfig {
        enabled: true,
        threshold: Size::new(1),
        chunk_size: Size::new(1_024),
        min_chunk_size: Size::new(1),
        max_parts: 10_000,
        concurrency: 2,
    };
    let body = vec![4_u8; 8 * 1_024];
    let response = put_object(
        &client_for(&url),
        &credential(),
        "media",
        "item",
        HeaderMap::new(),
        body_stream(body.clone()),
        Some(8 * 1_024),
        &config,
        &Metrics::default(),
    )
    .await
    .unwrap();
    assert!(response.status().is_success());
    assert_eq!(s3.parts(), 8);
    assert_eq!(s3.max_in_flight(), 2);
    assert_eq!(s3.object("media/item"), Some(Bytes::from(body)));
}

#[tokio::test]
async fn a_short_body_aborts_the_upload() {
    let (url, s3) = support::fake_s3::spawn().await;
    let error = put_object(
        &client_for(&url),
        &credential(),
        "media",
        "item",
        HeaderMap::new(),
        body_stream(vec![1_u8; 50]),
        Some(200),
        &multipart_config(100, 100),
        &Metrics::default(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("body"));
    assert_eq!(s3.aborts(), 1);
    assert!(!s3.has("media/item"));
}

#[tokio::test]
async fn a_long_body_aborts_the_upload() {
    let (url, s3) = support::fake_s3::spawn().await;
    let error = put_object(
        &client_for(&url),
        &credential(),
        "media",
        "item",
        HeaderMap::new(),
        body_stream(vec![1_u8; 250]),
        Some(200),
        &multipart_config(100, 100),
        &Metrics::default(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("body"));
    assert_eq!(s3.aborts(), 1);
    assert!(!s3.has("media/item"));
}

#[test]
fn invalid_multipart_config_is_rejected() {
    let mut config = Config::default();
    config.multipart.min_chunk_size = Size::new(4 * 1_024 * 1_024);
    assert!(
        validate(&config)
            .unwrap_err()
            .to_string()
            .contains("min_chunk_size")
    );

    let mut config = Config::default();
    config.multipart.chunk_size = Size::new(1_024 * 1_024);
    assert!(
        validate(&config)
            .unwrap_err()
            .to_string()
            .contains("chunk_size")
    );

    let mut config = Config::default();
    config.multipart.chunk_size = Size::new(6 * 1_024 * 1_024 * 1_024);
    config.multipart.min_chunk_size = Size::new(6 * 1_024 * 1_024 * 1_024);
    assert!(validate(&config).unwrap_err().to_string().contains("5GiB"));

    let mut config = Config::default();
    config.multipart.max_parts = 0;
    assert!(
        validate(&config)
            .unwrap_err()
            .to_string()
            .contains("max_parts")
    );

    let mut config = Config::default();
    config.multipart.concurrency = 65;
    assert!(
        validate(&config)
            .unwrap_err()
            .to_string()
            .contains("concurrency")
    );
}

#[test]
fn multipart_settings_load_from_file_env_and_flags() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sidecar.toml");
    std::fs::write(
        &path,
        "[multipart]\nthreshold = \"32MiB\"\n[cache]\nendpoint = \"http://127.0.0.1:9002\"\n\
         bucket = \"cache\"\nmode = \"read_write\"\nmax_bytes = 0\n",
    )
    .unwrap();
    let loaded = load(&LoadOptions {
        config_flag: Some(path),
        env: vec![(
            "S3_SIDECAR_MULTIPART__CHUNK_SIZE".to_owned(),
            "8MiB".to_owned(),
        )],
        flags: json!({"multipart": {"concurrency": 2}}),
        cwd: dir.path().to_path_buf(),
        default_paths: Vec::new(),
    })
    .unwrap();
    assert!(loaded.config.multipart.enabled);
    assert_eq!(loaded.config.multipart.threshold.as_bytes(), 32 * 1_048_576);
    assert_eq!(loaded.config.multipart.chunk_size.as_bytes(), 8 * 1_048_576);
    assert_eq!(loaded.config.multipart.concurrency, 2);
    assert_eq!(
        loaded.sources.get("multipart.threshold"),
        Some(&Source::File)
    );
    assert_eq!(
        loaded.sources.get("multipart.chunk_size"),
        Some(&Source::Env)
    );
    assert_eq!(
        loaded.sources.get("multipart.concurrency"),
        Some(&Source::Flag)
    );
}

fn client_config() -> ClientConfig {
    ClientConfig {
        access_key_id: "AKIA".to_owned(),
        secret_access_key: SecretString::new("secret"),
        session_token: None,
        buckets: vec!["media".to_owned()],
    }
}

fn signed_request(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    body: Option<Vec<u8>>,
) -> reqwest::RequestBuilder {
    let parsed = Url::parse(url).unwrap();
    let credential = CredentialStore::new(&[client_config()])
        .lookup("AKIA")
        .unwrap();
    let mut headers = HeaderMap::new();
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

struct Sidecar {
    url: String,
    task: JoinHandle<()>,
    state: Arc<AppState>,
}

impl Sidecar {
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
        drop(self.state);
    }
}

async fn spawn_sidecar(config: Config) -> Sidecar {
    s3_sidecar::observability::init();
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
        task,
        state,
    }
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

fn cache_config(origin: &str, cache: &str, index: &Path) -> Config {
    Config {
        mode: Mode::Cache,
        multipart: multipart_config(512, 512),
        cache: CacheConfig {
            endpoint: cache.to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: "cache".to_owned(),
            secret_access_key: SecretString::new("cache-secret"),
            force_path_style: true,
            bucket: "cachebucket".to_owned(),
            prefix: "cache".to_owned(),
            mode: CacheMode::ReadWrite,
            max_bytes: Some(0),
            max_object_bytes: 1_048_576,
            write_policy: WritePolicy::WriteBack,
            on_cache_error: OnCacheError::Origin,
            sync_workers: 2,
            sync_retry_base_ms: 10,
            sync_debounce_ms: 0,
            index_path: index.display().to_string(),
            ..CacheConfig::default()
        },
        origin: endpoint(origin),
        clients: vec![client_config()],
        buckets: vec![BucketConfig {
            name: "media".to_owned(),
            ttl: Some(HumanDuration::new(Duration::from_hours(1))),
            ..BucketConfig::default()
        }],
        ..Config::default()
    }
}

fn multipart_cache_config(
    origin: &str,
    cache: &str,
    index: &Path,
    mode: CacheMode,
    policy: WritePolicy,
) -> Config {
    let mut config = cache_config(origin, cache, index);
    config.cache.mode = mode;
    config.cache.write_policy = policy;
    config
}

fn xml_value(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)?.saturating_add(open.len());
    let rest = xml.get(start..)?;
    let end = rest.find(&close)?;
    rest.get(..end).map(str::to_owned)
}

fn xml_text(xml: &str, tag: &str) -> Option<String> {
    xml_value(xml, tag).map(|value| {
        value
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&")
    })
}

fn complete_xml(parts: &[(u32, String)]) -> Vec<u8> {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (number, etag) in parts {
        let _ = write!(
            xml,
            "<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
        );
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml.into_bytes()
}

async fn client_create_upload(client: &reqwest::Client, url: &str) -> String {
    let response = signed_request(
        client,
        Method::POST,
        &format!("{url}/media/item?uploads"),
        None,
    )
    .send()
    .await
    .unwrap();
    assert!(response.status().is_success());
    let body = response.text().await.unwrap();
    xml_value(&body, "UploadId").expect("the create response has an upload id")
}

async fn client_upload_part(
    client: &reqwest::Client,
    url: &str,
    upload_id: &str,
    number: u32,
    body: Vec<u8>,
) -> String {
    let response = signed_request(
        client,
        Method::PUT,
        &format!("{url}/media/item?partNumber={number}&uploadId={upload_id}"),
        Some(body),
    )
    .send()
    .await
    .unwrap();
    assert!(response.status().is_success());
    response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("the part response has an ETag")
        .to_owned()
}

fn cache_has_body(cache: &support::fake_s3::FakeS3, body: &[u8]) -> bool {
    cache
        .keys()
        .iter()
        .any(|key| cache.object(key).as_deref() == Some(body))
}

fn cache_metadata(cache: &support::fake_s3::FakeS3) -> Option<s3_sidecar::cache::Metadata> {
    cache
        .keys()
        .into_iter()
        .find(|key| is_meta_key(key))
        .and_then(|key| cache.object(&key))
        .and_then(|body| serde_json::from_slice(&body).ok())
}

fn is_meta_key(key: &str) -> bool {
    Path::new(key)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("meta"))
}

#[tokio::test]
async fn client_multipart_read_mode_fills_cache_after_origin() {
    let (origin_url, origin) = support::fake_s3::spawn().await;
    let (cache_url, cache) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(multipart_cache_config(
        &origin_url,
        &cache_url,
        &dir.path().join("index.redb"),
        CacheMode::Read,
        WritePolicy::WriteBack,
    ))
    .await;
    let client = reqwest::Client::new();
    let upload_id = client_create_upload(&client, &sidecar.url).await;
    let etag_one = client_upload_part(&client, &sidecar.url, &upload_id, 1, vec![1_u8; 40]).await;
    let etag_two = client_upload_part(&client, &sidecar.url, &upload_id, 2, vec![2_u8; 60]).await;
    let body = [vec![1_u8; 40], vec![2_u8; 60]].concat();
    let complete = signed_request(
        &client,
        Method::POST,
        &format!("{}/media/item?uploadId={upload_id}", sidecar.url),
        Some(complete_xml(&[(1, etag_one), (2, etag_two)])),
    )
    .send()
    .await
    .unwrap();
    assert!(complete.status().is_success());
    assert_eq!(
        origin.object("media/item").as_deref(),
        Some(body.as_slice())
    );
    assert_eq!(origin.parts(), 2);
    assert!(cache_has_body(&cache, &body));
    assert!(cache.keys().iter().any(|key| is_meta_key(key)));

    let origin_gets = origin.gets();
    let get = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/item", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert!(get.status().is_success());
    assert_eq!(get.bytes().await.unwrap(), Bytes::from(body));
    assert_eq!(origin.gets(), origin_gets);
    sidecar.stop().await;
}

#[tokio::test]
async fn client_multipart_write_back_completes_in_cache_then_syncs() {
    let (origin_url, origin) = support::fake_s3::spawn().await;
    let (cache_url, cache) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(multipart_cache_config(
        &origin_url,
        &cache_url,
        &dir.path().join("index.redb"),
        CacheMode::ReadWrite,
        WritePolicy::WriteBack,
    ))
    .await;
    let client = reqwest::Client::new();
    let upload_id = client_create_upload(&client, &sidecar.url).await;
    let etag_one = client_upload_part(&client, &sidecar.url, &upload_id, 1, vec![3_u8; 50]).await;
    let etag_two = client_upload_part(&client, &sidecar.url, &upload_id, 2, vec![4_u8; 70]).await;
    let body = [vec![3_u8; 50], vec![4_u8; 70]].concat();
    let complete = signed_request(
        &client,
        Method::POST,
        &format!("{}/media/item?uploadId={upload_id}", sidecar.url),
        Some(complete_xml(&[(1, etag_one), (2, etag_two)])),
    )
    .send()
    .await
    .unwrap();
    assert!(complete.status().is_success());
    assert_eq!(cache.uploads(), 1);
    assert_eq!(cache.parts(), 2);
    assert_eq!(cache.completes(), 1);
    assert_eq!(origin.uploads(), 0);
    wait_for(Duration::from_secs(10), || {
        let origin = origin.clone();
        let body = body.clone();
        async move { origin.object("media/item").as_deref() == Some(body.as_slice()) }
    })
    .await;
    let meta = cache_metadata(&cache).expect("the cache has metadata");
    assert!(matches!(
        meta.state,
        s3_sidecar::cache::meta::EntryState::Pending | s3_sidecar::cache::meta::EntryState::Synced
    ));
    let get = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/item", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert!(get.status().is_success());
    assert_eq!(get.bytes().await.unwrap(), Bytes::from(body));
    sidecar.stop().await;
}

#[tokio::test]
async fn client_multipart_write_back_complete_names_the_origin_object() {
    let (origin_url, _origin) = support::fake_s3::spawn().await;
    let (cache_url, _cache) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(multipart_cache_config(
        &origin_url,
        &cache_url,
        &dir.path().join("index.redb"),
        CacheMode::ReadWrite,
        WritePolicy::WriteBack,
    ))
    .await;
    let client = reqwest::Client::new();
    let upload_id = client_create_upload(&client, &sidecar.url).await;
    let etag_one = client_upload_part(&client, &sidecar.url, &upload_id, 1, vec![1_u8; 40]).await;
    let complete = signed_request(
        &client,
        Method::POST,
        &format!("{}/media/item?uploadId={upload_id}", sidecar.url),
        Some(complete_xml(&[(1, etag_one)])),
    )
    .send()
    .await
    .unwrap();
    assert!(complete.status().is_success());
    let body = complete.text().await.unwrap();
    assert_eq!(xml_value(&body, "Bucket").as_deref(), Some("media"));
    assert_eq!(xml_value(&body, "Key").as_deref(), Some("item"));
    let etag = xml_text(&body, "ETag").expect("the complete response has an ETag");
    assert!(etag.starts_with('"'));
    assert!(etag.ends_with("-1\""));
    assert!(!body.contains("cachebucket"));
    assert!(!body.contains("cache/"));
    sidecar.stop().await;
}

#[tokio::test]
async fn client_multipart_write_back_list_parts_names_the_origin_object() {
    let (origin_url, _origin) = support::fake_s3::spawn().await;
    let (cache_url, _cache) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(multipart_cache_config(
        &origin_url,
        &cache_url,
        &dir.path().join("index.redb"),
        CacheMode::ReadWrite,
        WritePolicy::WriteBack,
    ))
    .await;
    let client = reqwest::Client::new();
    let upload_id = client_create_upload(&client, &sidecar.url).await;
    let etag_one = client_upload_part(&client, &sidecar.url, &upload_id, 1, vec![2_u8; 40]).await;
    let etag_two = client_upload_part(&client, &sidecar.url, &upload_id, 2, vec![3_u8; 60]).await;
    let parts = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/item?uploadId={upload_id}", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert!(parts.status().is_success());
    let body = parts.text().await.unwrap();
    assert_eq!(xml_value(&body, "Bucket").as_deref(), Some("media"));
    assert_eq!(xml_value(&body, "Key").as_deref(), Some("item"));
    assert_eq!(
        xml_value(&body, "UploadId").as_deref(),
        Some(upload_id.as_str())
    );
    assert!(body.contains(&etag_one));
    assert!(body.contains(&etag_two));
    assert!(!body.contains("upload-0"));
    assert!(!body.contains("cachebucket"));
    assert!(!body.contains("cache/"));
    sidecar.stop().await;
}

#[tokio::test]
async fn client_multipart_completes_with_the_list_parts_upload_id() {
    let (origin_url, origin) = support::fake_s3::spawn().await;
    let (cache_url, cache) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(multipart_cache_config(
        &origin_url,
        &cache_url,
        &dir.path().join("index.redb"),
        CacheMode::ReadWrite,
        WritePolicy::WriteBack,
    ))
    .await;
    let client = reqwest::Client::new();
    let upload_id = client_create_upload(&client, &sidecar.url).await;
    let etag_one = client_upload_part(&client, &sidecar.url, &upload_id, 1, vec![4_u8; 40]).await;
    let parts = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/item?uploadId={upload_id}", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert!(parts.status().is_success());
    let listed = parts.text().await.unwrap();
    let listed_id = xml_value(&listed, "UploadId").expect("the list response has an upload id");
    let complete = signed_request(
        &client,
        Method::POST,
        &format!("{}/media/item?uploadId={listed_id}", sidecar.url),
        Some(complete_xml(&[(1, etag_one)])),
    )
    .send()
    .await
    .unwrap();
    assert!(complete.status().is_success());
    assert_eq!(cache.completes(), 1);
    assert_eq!(origin.uploads(), 0);
    sidecar.stop().await;
}

#[tokio::test]
async fn client_multipart_write_through_uses_origin_then_cache() {
    let (origin_url, origin) = support::fake_s3::spawn().await;
    let (cache_url, cache) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(multipart_cache_config(
        &origin_url,
        &cache_url,
        &dir.path().join("index.redb"),
        CacheMode::ReadWrite,
        WritePolicy::WriteThrough,
    ))
    .await;
    let client = reqwest::Client::new();
    let upload_id = client_create_upload(&client, &sidecar.url).await;
    let etag_one = client_upload_part(&client, &sidecar.url, &upload_id, 1, vec![5_u8; 30]).await;
    let etag_two = client_upload_part(&client, &sidecar.url, &upload_id, 2, vec![6_u8; 90]).await;
    let body = [vec![5_u8; 30], vec![6_u8; 90]].concat();
    let complete = signed_request(
        &client,
        Method::POST,
        &format!("{}/media/item?uploadId={upload_id}", sidecar.url),
        Some(complete_xml(&[(1, etag_one), (2, etag_two)])),
    )
    .send()
    .await
    .unwrap();
    assert!(complete.status().is_success());
    assert_eq!(origin.completes(), 1);
    assert_eq!(
        origin.object("media/item").as_deref(),
        Some(body.as_slice())
    );
    assert!(cache_has_body(&cache, &body));
    sidecar.stop().await;
}

#[tokio::test]
async fn client_multipart_abort_removes_cache_state() {
    let (origin_url, _origin) = support::fake_s3::spawn().await;
    let (cache_url, cache) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(multipart_cache_config(
        &origin_url,
        &cache_url,
        &dir.path().join("index.redb"),
        CacheMode::ReadWrite,
        WritePolicy::WriteBack,
    ))
    .await;
    let client = reqwest::Client::new();
    let upload_id = client_create_upload(&client, &sidecar.url).await;
    let etag_one = client_upload_part(&client, &sidecar.url, &upload_id, 1, vec![7_u8; 20]).await;
    let abort = signed_request(
        &client,
        Method::DELETE,
        &format!("{}/media/item?uploadId={upload_id}", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert!(abort.status().is_success());
    assert_eq!(cache.aborts(), 1);
    let complete = signed_request(
        &client,
        Method::POST,
        &format!("{}/media/item?uploadId={upload_id}", sidecar.url),
        Some(complete_xml(&[(1, etag_one)])),
    )
    .send()
    .await
    .unwrap();
    assert!(!complete.status().is_success());
    sidecar.stop().await;
}

#[tokio::test]
async fn client_multipart_write_back_rejects_object_above_cache_limit() {
    let (origin_url, _origin) = support::fake_s3::spawn().await;
    let (cache_url, cache) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = multipart_cache_config(
        &origin_url,
        &cache_url,
        &dir.path().join("index.redb"),
        CacheMode::ReadWrite,
        WritePolicy::WriteBack,
    );
    config.cache.max_object_bytes = 64;
    let sidecar = spawn_sidecar(config).await;
    let client = reqwest::Client::new();
    let upload_id = client_create_upload(&client, &sidecar.url).await;
    let etag_one = client_upload_part(&client, &sidecar.url, &upload_id, 1, vec![8_u8; 40]).await;
    let over_limit = signed_request(
        &client,
        Method::PUT,
        &format!(
            "{}/media/item?partNumber=2&uploadId={upload_id}",
            sidecar.url
        ),
        Some(vec![9_u8; 40]),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(over_limit.status(), reqwest::StatusCode::BAD_REQUEST);
    let error = over_limit.text().await.unwrap();
    assert!(error.contains("<Code>EntityTooLarge</Code>"));
    assert_eq!(cache.completes(), 0);
    assert_eq!(cache.keys(), Vec::<String>::new());
    let complete = signed_request(
        &client,
        Method::POST,
        &format!("{}/media/item?uploadId={upload_id}", sidecar.url),
        Some(complete_xml(&[(1, etag_one)])),
    )
    .send()
    .await
    .unwrap();
    assert!(!complete.status().is_success());
    sidecar.stop().await;
}

#[tokio::test]
async fn cache_write_back_sync_uses_multipart() {
    let (origin_url, origin) = support::fake_s3::spawn().await;
    let (cache_url, cache) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cache_config(
        &origin_url,
        &cache_url,
        &dir.path().join("index.redb"),
    ))
    .await;
    let body = vec![2_u8; 4_096];
    let response = signed_request(
        &reqwest::Client::new(),
        Method::PUT,
        &format!("{}/media/large", sidecar.url),
        Some(body.clone()),
    )
    .send()
    .await
    .unwrap();
    assert!(response.status().is_success());
    wait_for(Duration::from_secs(10), || {
        let origin = origin.clone();
        let body = body.clone();
        async move { origin.object("media/large").as_deref() == Some(body.as_slice()) }
    })
    .await;
    assert_eq!(cache.uploads(), 1);
    assert_eq!(cache.parts(), 8);
    assert_eq!(origin.uploads(), 1);
    assert_eq!(origin.parts(), 8);
    sidecar.stop().await;
}

fn cluster_config(backing: &str, metadata: &Path, secret: Option<&str>) -> Config {
    Config {
        mode: Mode::Cluster,
        multipart: multipart_config(512, 512),
        backing: EndpointConfig {
            bucket: "data".to_owned(),
            ..endpoint(backing)
        },
        consensus: ConsensusConfig {
            backend: ConsensusBackend::Single,
            raft: RaftConfig {
                node_id: "a".to_owned(),
                peers: vec!["a".to_owned()],
                ..RaftConfig::default()
            },
            ..ConsensusConfig::default()
        },
        cluster: ClusterConfig {
            metadata_path: metadata.display().to_string(),
            secret: secret.map_or_else(SecretString::default, SecretString::new),
            ..ClusterConfig::default()
        },
        buckets: vec![BucketConfig {
            name: "media".to_owned(),
            write_ack: Some(WriteAck::Leader),
            replicas: Some(1),
            min_replicas: Some(1),
            ..BucketConfig::default()
        }],
        clients: vec![client_config()],
        ..Config::default()
    }
}

#[tokio::test]
async fn cluster_staging_uses_multipart() {
    let (backing_url, backing) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cluster_config(
        &backing_url,
        &dir.path().join("meta.redb"),
        None,
    ))
    .await;
    let body = vec![6_u8; 4_096];
    let response = signed_request(
        &reqwest::Client::new(),
        Method::PUT,
        &format!("{}/media/large", sidecar.url),
        Some(body.clone()),
    )
    .send()
    .await
    .unwrap();
    assert!(response.status().is_success());
    assert_eq!(backing.uploads(), 1);
    assert_eq!(backing.parts(), 8);
    let stored = backing
        .keys()
        .into_iter()
        .find(|key| key.starts_with("data/objects/"))
        .and_then(|key| backing.object(&key));
    assert_eq!(stored, Some(Bytes::from(body)));
    sidecar.stop().await;
}

#[tokio::test]
async fn replication_receive_uses_multipart() {
    let (backing_url, backing) = support::fake_s3::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let sidecar = spawn_sidecar(cluster_config(
        &backing_url,
        &dir.path().join("meta.redb"),
        Some("cluster-secret"),
    ))
    .await;
    let body = vec![8_u8; 4_096];
    let blob = "0123456789abcdef0123456789abcdef0123456789abcdef";
    let response = reqwest::Client::new()
        .put(format!("{}/_blob/{blob}", sidecar.url))
        .header("x-s3-sidecar-cluster-token", "cluster-secret")
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(backing.uploads(), 1);
    assert_eq!(backing.parts(), 8);
    assert_eq!(
        backing.object(&format!("data/objects/{blob}")),
        Some(Bytes::from(body))
    );
    sidecar.stop().await;
}
