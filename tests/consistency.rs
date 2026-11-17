#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code uses unwrap and expect for fixtures"
)]

//! Write acknowledgement policies and the failure matrix with an in-memory
//! Consul metadata authority.

mod support;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use futures_util::future::BoxFuture;
use reqwest::Url;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, CredentialStore, EMPTY_SHA256};
use s3_sidecar::cluster::consul::api::{ConsulApi, ConsulError, KvEntry, PrefixResult};
use s3_sidecar::cluster::consul::memory::MemoryConsul;
use s3_sidecar::cluster::coordinator::Coordinator;
use s3_sidecar::config::duration::HumanDuration;
use s3_sidecar::config::secret::SecretString;
use s3_sidecar::config::{
    BucketConfig, ClientConfig, ClusterConfig, ClusterNodeConfig, Config, ConsensusBackend,
    ConsensusConfig, ConsulConfig, EndpointConfig, Mode, ReadMode, ServerConfig, WriteAck,
};
use s3_sidecar::server::{AppState, router};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const CLUSTER_TOKEN: &str = "cluster-token";
const PROPOSAL_TOKEN: &str = "proposal-token";

#[derive(Clone, Default)]
struct Backing {
    objects: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl Backing {
    fn has(&self, uuid: &str) -> bool {
        self.objects
            .lock()
            .unwrap()
            .contains_key(&format!("objects/{uuid}"))
    }

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

#[expect(
    clippy::format_push_string,
    reason = "the test builds the expected body with push_str"
)]
async fn backing_handler(
    State(state): State<Backing>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method == Method::GET && uri.path() == "/data" {
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
    let Some(key) = uri.path().strip_prefix("/data/") else {
        return xml(
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchKey</Code></Error>",
        );
    };
    match method {
        Method::PUT => {
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
            let total = value.len();
            let range = headers
                .get(header::RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| parse_range(value, total));
            let (status, body, length, content_range) = if let Some((start, end)) = range {
                let slice = value
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
                    Some(format!("bytes {start}-{end}/{total}")),
                )
            } else {
                (
                    StatusCode::OK,
                    if method == Method::HEAD {
                        Bytes::new()
                    } else {
                        value
                    },
                    total,
                    None,
                )
            };
            let mut response = Response::new(Body::from(body));
            *response.status_mut() = status;
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&length.to_string()).unwrap(),
            );
            if let Some(value) = content_range
                && let Ok(value) = HeaderValue::from_str(&value)
            {
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
            response
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn parse_range(value: &str, total: usize) -> Option<(usize, usize)> {
    let value = value.strip_prefix("bytes=")?;
    let (start, end) = value.split_once('-')?;
    let start = start.parse::<usize>().ok()?;
    let end = if end.is_empty() {
        total.saturating_sub(1)
    } else {
        end.parse::<usize>().ok()?.min(total.saturating_sub(1))
    };
    Some((start, end))
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

async fn get(client: &reqwest::Client, url: &str) -> reqwest::Response {
    signed_request(client, Method::GET, url, None, &[])
        .send()
        .await
        .unwrap()
}

async fn delete(client: &reqwest::Client, url: &str) -> reqwest::Response {
    signed_request(client, Method::DELETE, url, None, &[])
        .send()
        .await
        .unwrap()
}

#[expect(
    clippy::too_many_arguments,
    reason = "the test helper receives all fixture inputs"
)]
fn node_config(
    id: &str,
    url: &str,
    backing: &str,
    metadata_path: &std::path::Path,
    nodes: &[(&str, String)],
    write_ack: WriteAck,
    replicas: u32,
    min_replicas: u32,
    read: ReadMode,
) -> Config {
    let directory = nodes
        .iter()
        .map(|(node, addr)| ClusterNodeConfig {
            id: (*node).to_owned(),
            addr: addr.clone(),
        })
        .collect();
    Config {
        mode: Mode::Cluster,
        server: ServerConfig {
            bind: url.trim_start_matches("http://").to_owned(),
            ..ServerConfig::default()
        },
        backing: EndpointConfig {
            endpoint: backing.to_owned(),
            bucket: "data".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKIA".to_owned(),
            secret_access_key: SecretString::new("secret"),
            ..EndpointConfig::default()
        },
        consensus: ConsensusConfig {
            backend: ConsensusBackend::Consul,
            consul: ConsulConfig {
                node_id: id.to_owned(),
                address: "http://127.0.0.1:8500".to_owned(),
                advertise_addr: url.to_owned(),
                token: SecretString::new("acl-token"),
                session_ttl: HumanDuration::new(Duration::from_secs(10)),
                secret: SecretString::new(PROPOSAL_TOKEN),
            },
            ..ConsensusConfig::default()
        },
        cluster: ClusterConfig {
            metadata_path: metadata_path.display().to_string(),
            secret: SecretString::new(CLUSTER_TOKEN),
            nodes: directory,
            replicate_workers: 2,
            replicate_timeout: HumanDuration::new(Duration::from_secs(2)),
            reconcile_interval: HumanDuration::new(Duration::from_hours(1)),
            ..ClusterConfig::default()
        },
        clients: vec![client_config()],
        buckets: vec![BucketConfig {
            name: "media".to_owned(),
            write_ack: Some(write_ack),
            read: Some(read),
            replicas: Some(replicas),
            min_replicas: Some(min_replicas),
            ..BucketConfig::default()
        }],
        ..Config::default()
    }
}

// The wrapper races one compare-and-swap write to force a commit rejection
// after the peer copy completed.
#[derive(Clone)]
struct WrappedConsul {
    inner: Arc<MemoryConsul>,
    race_key: Option<String>,
    race_value: Option<Vec<u8>>,
    raced: Arc<AtomicBool>,
    watch_wait: Duration,
}

impl WrappedConsul {
    fn plain(inner: Arc<MemoryConsul>) -> Self {
        Self {
            inner,
            race_key: None,
            race_value: None,
            raced: Arc::new(AtomicBool::new(false)),
            watch_wait: Duration::from_secs(30),
        }
    }

    fn with_race(
        inner: Arc<MemoryConsul>,
        race_key: String,
        race_value: Vec<u8>,
        raced: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner,
            race_key: Some(race_key),
            race_value: Some(race_value),
            raced,
            watch_wait: Duration::from_secs(30),
        }
    }

    fn fast_watch(mut self) -> Self {
        self.watch_wait = Duration::from_millis(50);
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
        let wait = wait.min(self.watch_wait);
        Box::pin(async move { self.inner.watch_prefix(prefix, index, wait).await })
    }
}

struct Node {
    id: String,
    url: String,
    state: Arc<AppState>,
    backing: Backing,
    task: JoinHandle<()>,
}

impl Node {
    fn stop_transport(&self) {
        self.task.abort();
    }
}

struct TestCluster {
    nodes: Vec<Node>,
    _dir: TempDir,
}

impl TestCluster {
    fn node(&self, id: &str) -> &Node {
        self.nodes
            .iter()
            .find(|node| node.id == id)
            .expect("node id is present")
    }

    fn leader(&self) -> Option<&Node> {
        self.nodes.iter().find(|node| {
            node.state
                .cluster()
                .and_then(|cluster| cluster.consul())
                .is_some_and(|coordinator| coordinator.is_leader())
        })
    }
}

async fn spawn_consul_cluster(
    ids: &[&str],
    write_ack: WriteAck,
    replicas: u32,
    min_replicas: u32,
    read: ReadMode,
    race: Option<(String, Vec<u8>)>,
) -> TestCluster {
    spawn_consul_cluster_with_metrics(ids, write_ack, replicas, min_replicas, read, race, None)
        .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "the test helper receives all fixture inputs"
)]
async fn spawn_consul_cluster_with_metrics(
    ids: &[&str],
    write_ack: WriteAck,
    replicas: u32,
    min_replicas: u32,
    read: ReadMode,
    race: Option<(String, Vec<u8>)>,
    metrics: Option<&support::TestMetrics>,
) -> TestCluster {
    let dir = TempDir::new().unwrap();
    let memory = Arc::new(MemoryConsul::new());
    let mut listeners = Vec::new();
    let mut backings = Vec::new();
    let mut urls = Vec::new();
    for _ in ids {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        urls.push(format!("http://{}", listener.local_addr().unwrap()));
        listeners.push(listener);
        backings.push(spawn_backing().await);
    }
    let nodes: Vec<(&str, String)> = ids
        .iter()
        .enumerate()
        .map(|(index, id)| (*id, urls[index].clone()))
        .collect();
    let raced = Arc::new(AtomicBool::new(false));
    let mut spawned = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        let config = node_config(
            id,
            &urls[index],
            &backings[index].0,
            &dir.path().join(format!("{id}.redb")),
            &nodes,
            write_ack,
            replicas,
            min_replicas,
            read,
        );
        let api: Arc<dyn ConsulApi> = match &race {
            Some((key, value)) => Arc::new(WrappedConsul::with_race(
                Arc::clone(&memory),
                key.clone(),
                value.clone(),
                Arc::clone(&raced),
            )),
            None => Arc::new(WrappedConsul::plain(Arc::clone(&memory))),
        };
        let state = match metrics {
            Some(metrics) => Arc::new(
                AppState::new_with_metrics(config, Arc::clone(&metrics.metrics), Some(api), None)
                    .expect("cluster state is valid"),
            ),
            None => Arc::new(
                AppState::new_with_consul_api(config, Some(api)).expect("cluster state is valid"),
            ),
        };
        spawned.push(((*id).to_owned(), state, backings[index].1.clone()));
    }
    let mut nodes_out = Vec::new();
    for (index, (id, state, backing)) in spawned.into_iter().enumerate() {
        let listener = listeners.remove(0);
        let app = router(Arc::clone(&state));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        nodes_out.push(Node {
            id,
            url: urls[index].clone(),
            state,
            backing,
            task,
        });
    }
    let starts: Vec<_> = nodes_out
        .iter()
        .map(|node| {
            let state = Arc::clone(&node.state);
            tokio::spawn(async move { state.start().await })
        })
        .collect();
    for start in starts {
        start.await.unwrap().unwrap();
    }
    let cluster = TestCluster {
        nodes: nodes_out,
        _dir: dir,
    };
    wait_until(Duration::from_secs(10), || cluster.leader().is_some()).await;
    cluster
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

fn committed(
    cluster: &TestCluster,
    id: &str,
    key: &str,
) -> Option<s3_sidecar::cluster::state::Metadata> {
    cluster
        .node(id)
        .state
        .cluster()
        .unwrap()
        .state()
        .get_metadata("media", key)
        .unwrap()
        .filter(s3_sidecar::cluster::state::Metadata::is_committed)
}

async fn wait_committed(cluster: &TestCluster, id: &str, key: &str, timeout: Duration) {
    wait_until(timeout, || committed(cluster, id, key).is_some()).await;
}

async fn wait_absent_on_all_backings(cluster: &TestCluster, timeout: Duration) {
    wait_until(timeout, || {
        cluster.nodes.iter().all(|node| node.backing.count() == 0)
    })
    .await;
}

#[tokio::test]
async fn quorum_write_succeeds_with_one_peer_down() {
    let telemetry = support::TestMetrics::new();
    let cluster = spawn_consul_cluster_with_metrics(
        &["a", "b", "c"],
        WriteAck::Quorum,
        3,
        2,
        ReadMode::LocalThenPeer,
        None,
        Some(&telemetry),
    )
    .await;
    let leader = cluster.leader().unwrap().id.clone();
    let down = cluster
        .nodes
        .iter()
        .find(|node| node.id != leader)
        .unwrap()
        .id
        .clone();
    cluster.node(&down).stop_transport();
    let client = reqwest::Client::new();
    let response = put(
        &client,
        &format!("{}/media/quorum-ok", cluster.node(&leader).url),
        b"quorum-body",
        &[],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    wait_committed(&cluster, &leader, "quorum-ok", Duration::from_secs(10)).await;
    let metadata = committed(&cluster, &leader, "quorum-ok").unwrap();
    assert_eq!(metadata.replicas.len(), 2);
    assert!(metadata.replicas.iter().any(|node| node == &leader));
    assert!(!metadata.replicas.iter().any(|node| node == &down));
    let blob = metadata.blob;
    let acked = metadata
        .replicas
        .iter()
        .find(|node| *node != &leader)
        .unwrap()
        .clone();
    assert!(cluster.node(&acked).backing.has(&blob));
    assert!(cluster.node(&leader).backing.has(&blob));

    let metrics = telemetry.collect();
    assert!(
        support::counter_with(&metrics, "s3_sidecar.cluster.write_ack", "policy", "quorum",) >= 1
    );
    assert!(support::counter(&metrics, "s3_sidecar.cluster.quorum.success") >= 1);
    assert!(
        support::histogram_count(&metrics, "s3_sidecar.cluster.sync_replication.duration") >= 1
    );
    assert!(support::counter(&metrics, "s3_sidecar.cluster.replica_acks") >= 1);
}

#[tokio::test]
async fn quorum_write_with_too_few_peers_returns_503_and_cleans_up() {
    let cluster = spawn_consul_cluster(
        &["a", "b", "c"],
        WriteAck::Quorum,
        3,
        3,
        ReadMode::Local,
        None,
    )
    .await;
    let leader = cluster.leader().unwrap().id.clone();
    let down: Vec<String> = cluster
        .nodes
        .iter()
        .filter(|node| node.id != leader)
        .map(|node| node.id.clone())
        .collect();
    for id in &down {
        cluster.node(id).stop_transport();
    }
    let client = reqwest::Client::new();
    let response = put(
        &client,
        &format!("{}/media/quorum-fail", cluster.node(&leader).url),
        b"quorum-fail",
        &[],
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(committed(&cluster, &leader, "quorum-fail").is_none());
    wait_absent_on_all_backings(&cluster, Duration::from_secs(5)).await;
}

#[tokio::test]
async fn all_write_with_one_peer_down_returns_503_and_cleans_up() {
    let cluster =
        spawn_consul_cluster(&["a", "b", "c"], WriteAck::All, 3, 2, ReadMode::Local, None).await;
    let leader = cluster.leader().unwrap().id.clone();
    let down = cluster
        .nodes
        .iter()
        .find(|node| node.id != leader)
        .unwrap()
        .id
        .clone();
    cluster.node(&down).stop_transport();
    let client = reqwest::Client::new();
    let response = put(
        &client,
        &format!("{}/media/all-fail", cluster.node(&leader).url),
        b"all-fail",
        &[],
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(committed(&cluster, &leader, "all-fail").is_none());
    wait_absent_on_all_backings(&cluster, Duration::from_secs(10)).await;
}

#[tokio::test]
async fn commit_rejection_after_peer_replication_removes_every_blob() {
    let key = s3_sidecar::cluster::consul::metadata_key("media", "raced");
    let raced = s3_sidecar::cluster::state::Metadata {
        state: s3_sidecar::cluster::state::ObjectState::Committed,
        version: 0,
        blob: "other-blob".to_owned(),
        size: 5,
        checksum: "checksum".to_owned(),
        content_type: Some("text/plain".to_owned()),
        etag: "old".to_owned(),
        user_metadata: std::collections::BTreeMap::new(),
        owner: "other".to_owned(),
        replicas: vec!["other".to_owned()],
        created_at: 100,
        committed_at: Some(100),
        deleted_at: None,
        cache_control: None,
        content_encoding: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: None,
    };
    let cluster = spawn_consul_cluster(
        &["a", "b"],
        WriteAck::Quorum,
        2,
        2,
        ReadMode::Local,
        Some((key, serde_json::to_vec(&raced).unwrap())),
    )
    .await;
    let leader = cluster.leader().unwrap().id.clone();
    let client = reqwest::Client::new();
    let response = put(
        &client,
        &format!("{}/media/raced", cluster.node(&leader).url),
        b"new",
        &[("if-none-match", "*")],
    )
    .await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    wait_absent_on_all_backings(&cluster, Duration::from_secs(10)).await;
}

#[tokio::test]
async fn leader_write_reads_from_a_replica_on_another_node() {
    let cluster = spawn_consul_cluster(
        &["a", "b"],
        WriteAck::Leader,
        2,
        1,
        ReadMode::LocalThenPeer,
        None,
    )
    .await;
    let leader = cluster.leader().unwrap().id.clone();
    let follower = cluster
        .nodes
        .iter()
        .find(|node| node.id != leader)
        .unwrap()
        .id
        .clone();
    let client = reqwest::Client::new();
    let url = format!("{}/media/half-replicated", cluster.node(&leader).url);
    assert_eq!(
        put(&client, &url, b"replica-body", &[]).await.status(),
        StatusCode::OK
    );
    wait_committed(
        &cluster,
        &follower,
        "half-replicated",
        Duration::from_secs(10),
    )
    .await;
    let metadata = committed(&cluster, &follower, "half-replicated").unwrap();
    let blob = metadata.blob.clone();
    wait_until(Duration::from_secs(10), || {
        cluster.node(&follower).backing.has(&blob)
    })
    .await;
    cluster
        .node(&follower)
        .backing
        .objects
        .lock()
        .unwrap()
        .clear();
    assert!(!cluster.node(&follower).backing.has(&blob));
    let response = get(
        &client,
        &format!("{}/media/half-replicated", cluster.node(&follower).url),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "replica-body");
}

#[tokio::test]
async fn stale_replica_after_delete_is_hidden_by_the_tombstone() {
    let cluster =
        spawn_consul_cluster(&["a", "b"], WriteAck::Leader, 2, 1, ReadMode::Local, None).await;
    let leader = cluster.leader().unwrap().id.clone();
    let follower = cluster
        .nodes
        .iter()
        .find(|node| node.id != leader)
        .unwrap()
        .id
        .clone();
    let client = reqwest::Client::new();
    let url = format!("{}/media/stale", cluster.node(&leader).url);
    assert_eq!(
        put(&client, &url, b"stale-body", &[]).await.status(),
        StatusCode::OK
    );
    wait_committed(&cluster, &follower, "stale", Duration::from_secs(10)).await;
    let metadata = committed(&cluster, &leader, "stale").unwrap();
    let blob = metadata.blob.clone();
    assert_eq!(delete(&client, &url).await.status(), StatusCode::NO_CONTENT);
    wait_until(Duration::from_secs(10), || {
        cluster
            .node(&follower)
            .state
            .cluster()
            .unwrap()
            .state()
            .get_metadata("media", "stale")
            .unwrap()
            .is_some_and(|metadata| !metadata.is_committed())
    })
    .await;
    // Reinsert the old blob to model a replica that did not process the delete.
    for node in &cluster.nodes {
        node.backing
            .objects
            .lock()
            .unwrap()
            .insert(format!("objects/{blob}"), Bytes::from_static(b"stale-body"));
    }
    for node in &cluster.nodes {
        let response = get(&client, &format!("{}/media/stale", node.url)).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
#[expect(
    clippy::similar_names,
    reason = "the test compares similarly named fields"
)]
async fn restart_after_a_failed_quorum_write_leaves_no_partial_object() {
    let dir = TempDir::new().unwrap();
    let memory = Arc::new(MemoryConsul::new());
    let (peer_backing_url, peer_backing) = spawn_backing().await;
    let (owner_backing_url, owner_backing) = spawn_backing().await;
    let listener_owner = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let owner_url = format!("http://{}", listener_owner.local_addr().unwrap());
    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_url = format!("http://{}", peer_listener.local_addr().unwrap());
    let nodes = [("a", owner_url.clone()), ("b", peer_url.clone())];
    let nodes: Vec<(&str, String)> = nodes.to_vec();
    let owner_config = node_config(
        "a",
        &owner_url,
        &owner_backing_url,
        &dir.path().join("a.redb"),
        &nodes,
        WriteAck::Quorum,
        2,
        2,
        ReadMode::Local,
    );
    let peer_config = node_config(
        "b",
        &peer_url,
        &peer_backing_url,
        &dir.path().join("b.redb"),
        &nodes,
        WriteAck::Quorum,
        2,
        2,
        ReadMode::Local,
    );
    let owner_api: Arc<dyn ConsulApi> =
        Arc::new(WrappedConsul::plain(Arc::clone(&memory)).fast_watch());
    let peer_api: Arc<dyn ConsulApi> =
        Arc::new(WrappedConsul::plain(Arc::clone(&memory)).fast_watch());
    let owner =
        Arc::new(AppState::new_with_consul_api(owner_config.clone(), Some(owner_api)).unwrap());
    let peer = Arc::new(AppState::new_with_consul_api(peer_config, Some(peer_api)).unwrap());
    let peer_app = router(Arc::clone(&peer));
    let peer_task = tokio::spawn(async move {
        axum::serve(peer_listener, peer_app).await.unwrap();
    });
    let owner_app = router(Arc::clone(&owner));
    let owner_task = tokio::spawn(async move {
        axum::serve(listener_owner, owner_app).await.unwrap();
    });
    owner.start().await.unwrap();
    peer.start().await.unwrap();
    wait_until(Duration::from_secs(10), || {
        owner.cluster().unwrap().consul().unwrap().is_leader()
            || peer.cluster().unwrap().consul().unwrap().is_leader()
    })
    .await;
    peer_task.abort();
    let client = reqwest::Client::new();
    let response = put(
        &client,
        &format!("{owner_url}/media/restart-fail"),
        b"restart-fail",
        &[],
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    wait_until(Duration::from_secs(5), || {
        owner_backing.count() == 0 && peer_backing.count() == 0
    })
    .await;
    owner_task.abort();
    let _ = owner_task.await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    // The Consul coordinator has no shutdown hook in this milestone. Serve the
    // same durable state from a new listener to model the restart.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let restarted_url = format!("http://{}", listener.local_addr().unwrap());
    let app = router(Arc::clone(&owner));
    let restarted_task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    owner.start().await.unwrap();
    assert!(
        owner
            .cluster()
            .unwrap()
            .state()
            .get_metadata("media", "restart-fail")
            .unwrap()
            .is_none()
    );
    assert_eq!(owner_backing.count(), 0);
    let client = reqwest::Client::new();
    let response = get(&client, &format!("{restarted_url}/media/restart-fail")).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    restarted_task.abort();
    let _ = restarted_task.await;
}
