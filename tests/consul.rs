#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

//! Consul backend integration tests with an in-memory Consul API.

mod support;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use futures_util::future::BoxFuture;
use reqwest::Url;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, CredentialStore, EMPTY_SHA256};
use s3_sidecar::cluster::consul::api::{ConsulApi, ConsulError, KvEntry, PrefixResult};
use s3_sidecar::cluster::consul::coordinator::{ConsulCoordinator, ProposeReply, TOKEN_HEADER};
use s3_sidecar::cluster::consul::memory::MemoryConsul;
use s3_sidecar::cluster::coordinator::Coordinator;
use s3_sidecar::cluster::state::{Metadata, MetadataStore, ObjectState};
use s3_sidecar::config::duration::HumanDuration;
use s3_sidecar::config::secret::SecretString;
use s3_sidecar::config::{
    BucketConfig, ClientConfig, ClusterConfig, Config, ConsensusBackend, ConsensusConfig,
    ConsulConfig, EndpointConfig, Mode, ReadMode,
};
use s3_sidecar::server::{AppState, router};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const PROPOSAL_TOKEN: &str = "proposal-token";

// The wrapper models a Consul that can race one compare-and-swap or stop
// delivering blocking query updates. Both behaviors are needed to test rare
// failure paths.
#[derive(Clone)]
struct WrappedConsul {
    inner: Arc<MemoryConsul>,
    race_key: Option<String>,
    race_value: Option<Vec<u8>>,
    raced: Arc<AtomicBool>,
    watch: bool,
}

impl WrappedConsul {
    fn plain(inner: Arc<MemoryConsul>) -> Self {
        Self {
            inner,
            race_key: None,
            race_value: None,
            raced: Arc::new(AtomicBool::new(false)),
            watch: true,
        }
    }

    fn with_race(inner: Arc<MemoryConsul>, key: String, value: Vec<u8>) -> Self {
        Self {
            inner,
            race_key: Some(key),
            race_value: Some(value),
            raced: Arc::new(AtomicBool::new(false)),
            watch: true,
        }
    }

    fn without_watch(mut self) -> Self {
        self.watch = false;
        self
    }
}

impl ConsulApi for WrappedConsul {
    fn create_session(&self, ttl: Duration) -> BoxFuture<'_, Result<String, ConsulError>> {
        Box::pin(async move { self.inner.create_session(ttl).await })
    }

    fn renew_session<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move { self.inner.renew_session(id).await })
    }

    fn destroy_session<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), ConsulError>> {
        Box::pin(async move { self.inner.destroy_session(id).await })
    }

    fn acquire<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        session: &'a str,
    ) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move { self.inner.acquire(key, value, session).await })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
        consistent: bool,
    ) -> BoxFuture<'a, Result<Option<KvEntry>, ConsulError>> {
        Box::pin(async move { self.inner.get(key, consistent).await })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        consistent: bool,
    ) -> BoxFuture<'a, Result<PrefixResult, ConsulError>> {
        Box::pin(async move { self.inner.list(prefix, consistent).await })
    }

    fn put_cas<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        cas: u64,
    ) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move {
            if let (Some(race_key), Some(race_value)) = (&self.race_key, &self.race_value)
                && key == race_key
                && cas == 0
                && !self.raced.swap(true, Ordering::SeqCst)
            {
                let _ = self.inner.put_cas(key, race_value.clone(), 0).await?;
            }
            self.inner.put_cas(key, value, cas).await
        })
    }

    fn delete_cas<'a>(
        &'a self,
        key: &'a str,
        cas: u64,
    ) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move { self.inner.delete_cas(key, cas).await })
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        index: u64,
        wait: Duration,
    ) -> BoxFuture<'a, Result<PrefixResult, ConsulError>> {
        if self.watch {
            Box::pin(async move { self.inner.watch_prefix(prefix, index, wait).await })
        } else {
            Box::pin(async move {
                tokio::time::sleep(wait).await;
                Ok(PrefixResult::default())
            })
        }
    }
}

#[derive(Clone, Default)]
struct Backing {
    objects: Arc<Mutex<HashMap<String, Bytes>>>,
    puts: Arc<AtomicU64>,
}

impl Backing {
    fn count(&self) -> usize {
        self.objects.lock().unwrap().len()
    }
}

async fn spawn_backing() -> (String, Backing) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let backing = Backing::default();
    let app = Router::new()
        .fallback(backing_handler)
        .with_state(backing.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), backing)
}

#[allow(clippy::format_push_string)]
async fn backing_handler(
    State(state): State<Backing>,
    method: Method,
    uri: Uri,
    _headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path();
    if method == Method::GET && path == "/data" {
        let objects = state.objects.lock().unwrap();
        let mut contents = String::new();
        for key in objects.keys() {
            contents.push_str("<Contents>");
            contents.push_str(&format!("<Key>{key}</Key>"));
            contents.push_str("<LastModified>2000-01-01T00:00:00Z</LastModified>");
            contents.push_str("</Contents>");
        }
        return xml(
            StatusCode::OK,
            &format!(
                "<ListBucketResult><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
            ),
        );
    }
    let Some(key) = path.strip_prefix("/data/") else {
        return xml(
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchKey</Code></Error>",
        );
    };
    match method {
        Method::PUT => {
            state.puts.fetch_add(1, Ordering::Relaxed);
            state.objects.lock().unwrap().insert(key.to_owned(), body);
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::OK;
            response
                .headers_mut()
                .insert(header::ETAG, HeaderValue::from_static("\"etag\""));
            response
        }
        Method::DELETE => {
            state.objects.lock().unwrap().remove(key);
            StatusCode::NO_CONTENT.into_response()
        }
        Method::GET | Method::HEAD => {
            let Some(value) = state.objects.lock().unwrap().get(key).cloned() else {
                return xml(
                    StatusCode::NOT_FOUND,
                    "<Error><Code>NoSuchKey</Code></Error>",
                );
            };
            let mut response = if method == Method::HEAD {
                Response::new(Body::empty())
            } else {
                Response::new(Body::from(value.clone()))
            };
            *response.status_mut() = StatusCode::OK;
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&value.len().to_string()).unwrap(),
            );
            response
                .headers_mut()
                .insert(header::ETAG, HeaderValue::from_static("\"etag\""));
            response
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn xml(status: StatusCode, body: &str) -> Response {
    let mut response = Response::new(Body::from(body.to_owned()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response
}

fn client_config() -> ClientConfig {
    ClientConfig {
        access_key_id: "AKIA".to_owned(),
        secret_access_key: SecretString::new("secret"),
        session_token: None,
        buckets: vec!["media".to_owned()],
    }
}

fn credential() -> Arc<ClientCredential> {
    CredentialStore::new(&[client_config()])
        .lookup("AKIA")
        .unwrap()
}

fn consul_config(
    backing: String,
    node_id: &str,
    advertise_addr: &str,
    metadata_path: &std::path::Path,
    read: ReadMode,
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
            backend: ConsensusBackend::Consul,
            consul: ConsulConfig {
                node_id: node_id.to_owned(),
                address: "http://127.0.0.1:8500".to_owned(),
                advertise_addr: advertise_addr.to_owned(),
                token: SecretString::new("acl-token"),
                session_ttl: HumanDuration::new(Duration::from_secs(10)),
                secret: SecretString::new(PROPOSAL_TOKEN),
            },
            ..ConsensusConfig::default()
        },
        cluster: ClusterConfig {
            metadata_path: metadata_path.display().to_string(),
            ..ClusterConfig::default()
        },
        clients: vec![client_config()],
        buckets: vec![BucketConfig {
            name: "media".to_owned(),
            read: Some(read),
            replicas: Some(1),
            min_replicas: Some(1),
            ..BucketConfig::default()
        }],
        ..Config::default()
    }
}

struct Sidecar {
    url: String,
    state: Arc<AppState>,
    task: JoinHandle<()>,
}

async fn spawn_sidecar(listener: TcpListener, config: Config, api: Arc<dyn ConsulApi>) -> Sidecar {
    let state = Arc::new(AppState::new_with_consul_api(config, Some(api)).unwrap());
    state.start().await.unwrap();
    state.set_ready(true);
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

async fn spawn_sidecar_with_metrics(
    listener: TcpListener,
    config: Config,
    api: Arc<dyn ConsulApi>,
    metrics: &support::TestMetrics,
) -> Sidecar {
    let state = Arc::new(
        AppState::new_with_metrics(config, Arc::clone(&metrics.metrics), Some(api), None).unwrap(),
    );
    state.start().await.unwrap();
    state.set_ready(true);
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

fn signed_request(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    body: Option<Vec<u8>>,
) -> reqwest::RequestBuilder {
    let parsed = Url::parse(url).unwrap();
    let credential = credential();
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

async fn wait_until<F>(timeout: Duration, mut condition: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "the condition did not become true"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn leader_id(sidecars: &[&Sidecar]) -> Option<usize> {
    sidecars.iter().position(|sidecar| {
        sidecar
            .state
            .cluster()
            .and_then(|cluster| cluster.consul())
            .is_some_and(|coordinator| coordinator.is_leader())
    })
}

fn metadata(
    _bucket: &str,
    _key: &str,
    owner: &str,
    blob: &str,
    state: ObjectState,
    etag: &str,
) -> Metadata {
    Metadata {
        state,
        version: 0,
        blob: blob.to_owned(),
        size: 5,
        checksum: "checksum".to_owned(),
        content_type: Some("text/plain".to_owned()),
        etag: etag.to_owned(),
        user_metadata: BTreeMap::new(),
        owner: owner.to_owned(),
        replicas: vec![owner.to_owned()],
        created_at: 100,
        committed_at: Some(100),
        deleted_at: None,
        cache_control: None,
        content_encoding: None,
        content_disposition: None,
    }
}

fn stored_metadata(sidecar: &Sidecar, bucket: &str, key: &str) -> Option<Metadata> {
    sidecar
        .state
        .cluster()
        .unwrap()
        .state()
        .get_metadata(bucket, key)
        .unwrap()
}

fn consul_key(bucket: &str, key: &str) -> String {
    format!(
        "s3-sidecar/meta/{}/{}",
        hex::encode(bucket.as_bytes()),
        hex::encode(key.as_bytes())
    )
}

async fn two_nodes(memory: Arc<MemoryConsul>, read: ReadMode) -> (TempDir, Sidecar, Sidecar) {
    let dir = TempDir::new().unwrap();
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url_a = format!("http://{}", listener_a.local_addr().unwrap());
    let url_b = format!("http://{}", listener_b.local_addr().unwrap());
    let (backing_a, _) = spawn_backing().await;
    let (backing_b, _) = spawn_backing().await;
    let node_a = spawn_sidecar(
        listener_a,
        consul_config(backing_a, "a", &url_a, &dir.path().join("a.redb"), read),
        Arc::new(WrappedConsul::plain(Arc::clone(&memory))),
    )
    .await;
    let node_b = spawn_sidecar(
        listener_b,
        consul_config(backing_b, "b", &url_b, &dir.path().join("b.redb"), read),
        Arc::new(WrappedConsul::plain(Arc::clone(&memory))),
    )
    .await;
    (dir, node_a, node_b)
}

#[tokio::test]
async fn two_nodes_elect_one_leader_and_the_other_follows() {
    let memory = Arc::new(MemoryConsul::new());
    let (_dir, node_a, node_b) = two_nodes(Arc::clone(&memory), ReadMode::Local).await;
    let sidecars = [&node_a, &node_b];
    wait_until(Duration::from_secs(5), || leader_id(&sidecars).is_some()).await;
    let leader = leader_id(&sidecars).unwrap();
    let follower = 1 - leader;
    let leader_id = if leader == 0 { "a" } else { "b" };
    let follower_coordinator = sidecars[follower]
        .state
        .cluster()
        .unwrap()
        .consul()
        .unwrap();
    wait_until(Duration::from_secs(5), || {
        follower_coordinator
            .leader()
            .is_some_and(|record| record.id == leader_id)
    })
    .await;
    assert!(!follower_coordinator.is_leader());
    assert!(
        sidecars[leader]
            .state
            .cluster()
            .unwrap()
            .consul()
            .unwrap()
            .is_leader()
    );
    for sidecar in [node_a, node_b] {
        sidecar.task.abort();
        let _ = sidecar.task.await;
    }
}

#[tokio::test]
async fn follower_forwards_a_proposal_and_returns_after_the_cas_write() {
    let memory = Arc::new(MemoryConsul::new());
    let (_dir, node_a, node_b) = two_nodes(Arc::clone(&memory), ReadMode::Local).await;
    let sidecars = [&node_a, &node_b];
    wait_until(Duration::from_secs(5), || leader_id(&sidecars).is_some()).await;
    let leader = leader_id(&sidecars).unwrap();
    let follower = 1 - leader;
    let client = reqwest::Client::new();
    let url = format!("{}/media/forwarded.txt", sidecars[follower].url);
    let response = signed_request(&client, Method::PUT, &url, Some(b"hello".to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        memory
            .value(&consul_key("media", "forwarded.txt"))
            .is_some()
    );
    let stored = stored_metadata(sidecars[follower], "media", "forwarded.txt").unwrap();
    assert!(stored.is_committed());
    let response = signed_request(&client, Method::GET, &url, None)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.unwrap(),
        Bytes::from_static(b"hello")
    );
    for sidecar in [node_a, node_b] {
        sidecar.task.abort();
        let _ = sidecar.task.await;
    }
}

#[tokio::test]
async fn a_cas_race_returns_412_and_deletes_the_staged_blob() {
    let memory = Arc::new(MemoryConsul::new());
    let dir = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (backing, handle) = spawn_backing().await;
    let key = consul_key("media", "raced");
    let raced = metadata(
        "media",
        "raced",
        "other",
        "other-blob",
        ObjectState::Committed,
        "old",
    );
    let api = WrappedConsul::with_race(
        Arc::clone(&memory),
        key.clone(),
        serde_json::to_vec(&raced).unwrap(),
    );
    let sidecar = spawn_sidecar(
        listener,
        consul_config(
            backing,
            "a",
            &url,
            &dir.path().join("a.redb"),
            ReadMode::Local,
        ),
        Arc::new(api),
    )
    .await;
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/raced", sidecar.url),
        Some(b"new".to_vec()),
    )
    .header("if-none-match", "*")
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(handle.count(), 0);
    let stored = memory.value(&key).unwrap();
    let stored: Metadata = serde_json::from_slice(&stored).unwrap();
    assert_eq!(stored.owner, "other");
    sidecar.task.abort();
    let _ = sidecar.task.await;
}

#[tokio::test]
async fn session_loss_steps_down_and_a_new_leader_forms() {
    let memory = Arc::new(MemoryConsul::new());
    let (_dir, node_a, node_b) = two_nodes(Arc::clone(&memory), ReadMode::Local).await;
    let sidecars = [&node_a, &node_b];
    wait_until(Duration::from_secs(5), || leader_id(&sidecars).is_some()).await;
    let leader = leader_id(&sidecars).unwrap();
    let old_session = sidecars[leader]
        .state
        .cluster()
        .unwrap()
        .consul()
        .unwrap()
        .session()
        .unwrap();
    memory.destroy_session(&old_session).await.unwrap();
    wait_until(Duration::from_secs(8), || {
        !sidecars[leader]
            .state
            .cluster()
            .unwrap()
            .consul()
            .unwrap()
            .is_leader()
    })
    .await;
    wait_until(Duration::from_secs(8), || {
        sidecars.iter().any(|sidecar| {
            sidecar
                .state
                .cluster()
                .unwrap()
                .consul()
                .unwrap()
                .is_leader()
        })
    })
    .await;
    let new_leader = leader_id(&sidecars).unwrap();
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/after-failover", sidecars[new_leader].url),
        Some(b"ok".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        memory
            .value(&consul_key("media", "after-failover"))
            .is_some()
    );
    for sidecar in [node_a, node_b] {
        sidecar.task.abort();
        let _ = sidecar.task.await;
    }
}

#[tokio::test]
async fn the_watcher_materializes_a_leader_write_on_a_follower() {
    let memory = Arc::new(MemoryConsul::new());
    let (_dir, node_a, node_b) = two_nodes(Arc::clone(&memory), ReadMode::Local).await;
    let sidecars = [&node_a, &node_b];
    wait_until(Duration::from_secs(5), || leader_id(&sidecars).is_some()).await;
    let leader = leader_id(&sidecars).unwrap();
    let follower = 1 - leader;
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::PUT,
        &format!("{}/media/watched", sidecars[leader].url),
        Some(b"watched".to_vec()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    wait_until(Duration::from_secs(5), || {
        stored_metadata(sidecars[follower], "media", "watched")
            .is_some_and(|value| !value.blob.is_empty())
    })
    .await;
    for sidecar in [node_a, node_b] {
        sidecar.task.abort();
        let _ = sidecar.task.await;
    }
}

#[tokio::test]
async fn leader_read_uses_a_consistent_get() {
    let memory = Arc::new(MemoryConsul::new());
    let dir = TempDir::new().unwrap();
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url_a = format!("http://{}", listener_a.local_addr().unwrap());
    let url_b = format!("http://{}", listener_b.local_addr().unwrap());
    let (backing_a, _) = spawn_backing().await;
    let (backing_b, _) = spawn_backing().await;
    let node_a = spawn_sidecar(
        listener_a,
        consul_config(
            backing_a,
            "a",
            &url_a,
            &dir.path().join("a.redb"),
            ReadMode::Leader,
        ),
        Arc::new(WrappedConsul::plain(Arc::clone(&memory))),
    )
    .await;
    let telemetry = support::TestMetrics::new();
    let node_b = spawn_sidecar_with_metrics(
        listener_b,
        consul_config(
            backing_b,
            "b",
            &url_b,
            &dir.path().join("b.redb"),
            ReadMode::Leader,
        ),
        Arc::new(WrappedConsul::plain(Arc::clone(&memory)).without_watch()),
        &telemetry,
    )
    .await;
    let sidecars = [&node_a, &node_b];
    wait_until(Duration::from_secs(5), || leader_id(&sidecars).is_some()).await;
    wait_until(Duration::from_secs(5), || {
        node_b.state.cluster().unwrap().consul().unwrap().is_ready()
    })
    .await;
    assert!(stored_metadata(&node_b, "media", "direct").is_none());
    let direct = metadata(
        "media",
        "direct",
        "remote",
        "missing-blob",
        ObjectState::Committed,
        "direct-etag",
    );
    memory
        .put_cas(
            &consul_key("media", "direct"),
            serde_json::to_vec(&direct).unwrap(),
            0,
        )
        .await
        .unwrap();
    let client = reqwest::Client::new();
    let response = signed_request(
        &client,
        Method::GET,
        &format!("{}/media/direct", node_b.url),
        None,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let stored = stored_metadata(&node_b, "media", "direct").unwrap();
    assert_eq!(stored.owner, "remote");
    let metrics = telemetry.collect();
    assert!(support::counter(&metrics, "s3_sidecar.cluster.read_index_calls") >= 1);
    for sidecar in [node_a, node_b] {
        sidecar.task.abort();
        let _ = sidecar.task.await;
    }
}

#[tokio::test]
async fn delete_writes_a_tombstone_and_hides_the_key() {
    let memory = Arc::new(MemoryConsul::new());
    let dir = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (backing, _) = spawn_backing().await;
    let sidecar = spawn_sidecar(
        listener,
        consul_config(
            backing,
            "a",
            &url,
            &dir.path().join("a.redb"),
            ReadMode::Local,
        ),
        Arc::new(WrappedConsul::plain(Arc::clone(&memory))),
    )
    .await;
    let client = reqwest::Client::new();
    let object_url = format!("{}/media/gone", sidecar.url);
    assert_eq!(
        signed_request(&client, Method::PUT, &object_url, Some(b"gone".to_vec()))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_request(&client, Method::DELETE, &object_url, None)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        signed_request(&client, Method::GET, &object_url, None)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let value = memory.value(&consul_key("media", "gone")).unwrap();
    let stored: Metadata = serde_json::from_slice(&value).unwrap();
    assert_eq!(stored.state, ObjectState::Tombstone);
    let list = signed_request(
        &client,
        Method::GET,
        &format!("{}/media?list-type=2", sidecar.url),
        None,
    )
    .send()
    .await
    .unwrap()
    .text()
    .await
    .unwrap();
    assert!(!list.contains("<Key>gone</Key>"));
    sidecar.task.abort();
    let _ = sidecar.task.await;
}

#[tokio::test]
async fn list_objects_reads_materialized_metadata() {
    let memory = Arc::new(MemoryConsul::new());
    let (_dir, node_a, node_b) = two_nodes(Arc::clone(&memory), ReadMode::Local).await;
    let sidecars = [&node_a, &node_b];
    wait_until(Duration::from_secs(5), || leader_id(&sidecars).is_some()).await;
    let leader = leader_id(&sidecars).unwrap();
    let follower = 1 - leader;
    let client = reqwest::Client::new();
    for key in ["one", "two"] {
        let response = signed_request(
            &client,
            Method::PUT,
            &format!("{}/media/{key}", sidecars[leader].url),
            Some(b"body".to_vec()),
        )
        .send()
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    wait_until(Duration::from_secs(5), || {
        stored_metadata(sidecars[follower], "media", "one").is_some()
            && stored_metadata(sidecars[follower], "media", "two").is_some()
    })
    .await;
    let list = signed_request(
        &client,
        Method::GET,
        &format!("{}/media?list-type=2", sidecars[follower].url),
        None,
    )
    .send()
    .await
    .unwrap()
    .text()
    .await
    .unwrap();
    assert!(list.contains("<Key>one</Key>"));
    assert!(list.contains("<Key>two</Key>"));
    for sidecar in [node_a, node_b] {
        sidecar.task.abort();
        let _ = sidecar.task.await;
    }
}

#[tokio::test]
async fn proposal_endpoint_rejects_a_missing_or_wrong_token() {
    let memory = Arc::new(MemoryConsul::new());
    let dir = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (backing, _) = spawn_backing().await;
    let sidecar = spawn_sidecar(
        listener,
        consul_config(
            backing,
            "a",
            &url,
            &dir.path().join("a.redb"),
            ReadMode::Local,
        ),
        Arc::new(WrappedConsul::plain(Arc::clone(&memory))),
    )
    .await;
    let client = reqwest::Client::new();
    let endpoint = format!("{}/_coord/propose", sidecar.url);
    let response = client.post(&endpoint).body("{}").send().await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = client
        .post(&endpoint)
        .header(TOKEN_HEADER, "wrong")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = client
        .post(&endpoint)
        .header(TOKEN_HEADER, PROPOSAL_TOKEN)
        .body("{not-json}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    sidecar.task.abort();
    let _ = sidecar.task.await;
}

#[test]
fn consul_backend_requires_its_configuration() {
    let mut config = Config {
        mode: Mode::Cluster,
        backing: EndpointConfig {
            endpoint: "http://127.0.0.1:9002".to_owned(),
            bucket: "data".to_owned(),
            ..EndpointConfig::default()
        },
        consensus: ConsensusConfig {
            backend: ConsensusBackend::Consul,
            ..ConsensusConfig::default()
        },
        ..Config::default()
    };
    let error = s3_sidecar::config::validate(&config).unwrap_err();
    assert!(error.to_string().contains("consensus.consul"));
    config.consensus.consul.node_id = "a".to_owned();
    config.consensus.consul.address = "http://127.0.0.1:8500".to_owned();
    config.consensus.consul.advertise_addr = "http://127.0.0.1:9000".to_owned();
    config.consensus.consul.secret = SecretString::new("token");
    assert!(s3_sidecar::config::validate(&config).is_ok());
}

#[tokio::test]
async fn coordinator_proposal_reply_round_trips() {
    let reply = ProposeReply::Rejected(s3_sidecar::cluster::coordinator::RejectReason::Conflict);
    let encoded = serde_json::to_vec(&reply).unwrap();
    let decoded: ProposeReply = serde_json::from_slice(&encoded).unwrap();
    assert!(matches!(
        decoded,
        ProposeReply::Rejected(s3_sidecar::cluster::coordinator::RejectReason::Conflict)
    ));
    let store =
        Arc::new(MetadataStore::open(&TempDir::new().unwrap().path().join("x.redb")).unwrap());
    let coordinator = ConsulCoordinator::new(
        store,
        Arc::new(s3_sidecar::metrics::Metrics::default()),
        ConsulConfig::default(),
        Arc::new(MemoryConsul::new()),
    );
    assert!(!coordinator.is_leader());
    assert!(coordinator.leader().is_none());
}

#[derive(Clone, Default)]
struct FakeConsul {
    values: Arc<Mutex<BTreeMap<String, FakeValue>>>,
    index: Arc<AtomicU64>,
    last_token: Arc<Mutex<Option<String>>>,
}

#[derive(Clone)]
struct FakeValue {
    value: Vec<u8>,
    modify_index: u64,
    session: Option<String>,
}

async fn spawn_fake_consul() -> (String, FakeConsul) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = FakeConsul::default();
    let app = Router::new()
        .fallback(fake_consul_handler)
        .with_state(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), state)
}

async fn fake_consul_handler(
    State(state): State<FakeConsul>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    *state.last_token.lock().unwrap() = headers
        .get("x-consul-token")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let path = uri.path().to_owned();
    let query = uri.query().unwrap_or_default();
    if path == "/v1/session/create" && method == Method::PUT {
        return axum::Json(serde_json::json!({"ID": "session-1"})).into_response();
    }
    if path.starts_with("/v1/session/renew/") && method == Method::PUT {
        return axum::Json(serde_json::json!([])).into_response();
    }
    if path.starts_with("/v1/session/destroy/") && method == Method::PUT {
        return "true".into_response();
    }
    let Some(key) = path.strip_prefix("/v1/kv/") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if method == Method::GET && query.contains("recurse") {
        let values = state.values.lock().unwrap();
        let entries: Vec<serde_json::Value> = values
            .iter()
            .filter(|(name, _)| name.starts_with(key))
            .map(|(name, value)| {
                serde_json::json!({
                    "Key": name,
                    "Value": base64::engine::general_purpose::STANDARD.encode(&value.value),
                    "ModifyIndex": value.modify_index,
                    "Session": value.session,
                })
            })
            .collect();
        let mut response = axum::Json(entries).into_response();
        response.headers_mut().insert(
            "x-consul-index",
            HeaderValue::from_str(&state.index.load(Ordering::Relaxed).to_string()).unwrap(),
        );
        return response;
    }
    if method == Method::GET {
        let values = state.values.lock().unwrap();
        let Some(value) = values.get(key) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let entries = serde_json::json!([{
            "Key": key,
            "Value": base64::engine::general_purpose::STANDARD.encode(&value.value),
            "ModifyIndex": value.modify_index,
            "Session": value.session,
        }]);
        let mut response = axum::Json(entries).into_response();
        response.headers_mut().insert(
            "x-consul-index",
            HeaderValue::from_str(&state.index.load(Ordering::Relaxed).to_string()).unwrap(),
        );
        return response;
    }
    if method == Method::PUT {
        let cas = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("cas="))
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let acquire = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("acquire="))
            .map(str::to_owned);
        let mut values = state.values.lock().unwrap();
        let current = values.get(key).map(|value| value.modify_index);
        if let Some(session) = acquire {
            if values
                .get(key)
                .and_then(|value| value.session.as_ref())
                .is_some_and(|holder| holder != &session)
            {
                return "false".into_response();
            }
            let index = state.index.fetch_add(1, Ordering::Relaxed) + 1;
            values.insert(
                key.to_owned(),
                FakeValue {
                    value: body.to_vec(),
                    modify_index: index,
                    session: Some(session),
                },
            );
            return "true".into_response();
        }
        let matches = if cas == 0 {
            current.is_none()
        } else {
            current == Some(cas)
        };
        if !matches {
            return "false".into_response();
        }
        let index = state.index.fetch_add(1, Ordering::Relaxed) + 1;
        values.insert(
            key.to_owned(),
            FakeValue {
                value: body.to_vec(),
                modify_index: index,
                session: None,
            },
        );
        return "true".into_response();
    }
    if method == Method::DELETE {
        let cas = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("cas="))
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let mut values = state.values.lock().unwrap();
        if values
            .get(key)
            .is_some_and(|value| value.modify_index == cas)
        {
            values.remove(key);
            return "true".into_response();
        }
        return "false".into_response();
    }
    StatusCode::METHOD_NOT_ALLOWED.into_response()
}

#[tokio::test]
async fn http_consul_maps_sessions_and_cas() {
    use s3_sidecar::cluster::consul::api::HttpConsul;

    let (address, fake) = spawn_fake_consul().await;
    let config = ConsulConfig {
        node_id: "a".to_owned(),
        address,
        advertise_addr: "http://127.0.0.1:9000".to_owned(),
        token: SecretString::new("acl-token"),
        session_ttl: HumanDuration::new(Duration::from_secs(15)),
        secret: SecretString::new(PROPOSAL_TOKEN),
    };
    let api = HttpConsul::new(&config).unwrap();
    let session = api.create_session(Duration::from_secs(15)).await.unwrap();
    assert_eq!(session, "session-1");
    assert_eq!(
        fake.last_token.lock().unwrap().as_deref(),
        Some("acl-token")
    );
    assert!(api.renew_session(&session).await.unwrap());
    assert!(
        api.acquire("lock", b"value".to_vec(), &session)
            .await
            .unwrap()
    );
    let entry = api.get("lock", true).await.unwrap().unwrap();
    assert_eq!(entry.value, b"value");
    assert_eq!(entry.session.as_deref(), Some("session-1"));
    assert!(!api.put_cas("lock", b"new".to_vec(), 0).await.unwrap());
    assert!(
        api.put_cas("lock", b"new".to_vec(), entry.modify_index)
            .await
            .unwrap()
    );
    let entry = api.get("lock", true).await.unwrap().unwrap();
    assert_eq!(entry.value, b"new");
    assert!(api.delete_cas("lock", entry.modify_index).await.unwrap());
    assert!(api.get("lock", true).await.unwrap().is_none());
    let listed = api.list("l", true).await.unwrap();
    assert_eq!(listed.entries.len(), 0);
    api.destroy_session(&session).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a real Consul agent at CONSUL_HTTP_ADDR"]
async fn real_consul_session_and_cas() {
    use s3_sidecar::cluster::consul::api::HttpConsul;

    let Ok(address) = std::env::var("CONSUL_HTTP_ADDR") else {
        eprintln!("CONSUL_HTTP_ADDR is not set; skip the real Consul test");
        return;
    };
    let config = ConsulConfig {
        node_id: "real".to_owned(),
        address,
        advertise_addr: "http://127.0.0.1:9000".to_owned(),
        token: SecretString::default(),
        session_ttl: HumanDuration::new(Duration::from_secs(15)),
        secret: SecretString::new("token"),
    };
    let api = HttpConsul::new(&config).unwrap();
    let session = api.create_session(Duration::from_secs(15)).await.unwrap();
    let key = format!(
        "s3-sidecar/test/{}",
        OffsetDateTime::now_utc().unix_timestamp()
    );
    assert!(api.put_cas(&key, b"one".to_vec(), 0).await.unwrap());
    let entry = api.get(&key, true).await.unwrap().unwrap();
    assert_eq!(entry.value, b"one");
    assert!(api.delete_cas(&key, entry.modify_index).await.unwrap());
    api.destroy_session(&session).await.unwrap();
}

#[test]
fn metadata_key_encoding_is_url_safe() {
    let key = s3_sidecar::cluster::consul::metadata_key("media", "a/b c");
    assert!(key.starts_with("s3-sidecar/meta/"));
    assert!(!key.contains(' '));
    assert!(!key.contains('?'));
}
