#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code uses unwrap and expect for fixtures"
)]

//! Blob replication, peer fetch, read repair, delete propagation, and the
//! internal blob transport.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use reqwest::Url;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, CredentialStore, EMPTY_SHA256};
use s3_sidecar::cluster::state::{MetadataStore, ReplicationJob, ReplicationOp};
use s3_sidecar::config::duration::HumanDuration;
use s3_sidecar::config::secret::SecretString;
use s3_sidecar::config::{
    BucketConfig, ClientConfig, ClusterConfig, ClusterNodeConfig, Config, ConsensusBackend,
    ConsensusConfig, EndpointConfig, Mode, RaftConfig, RaftNodeConfig, ReadMode, ServerConfig,
    WriteAck,
};
use s3_sidecar::server::{AppState, router};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const CLUSTER_TOKEN: &str = "cluster-token";

#[derive(Clone, Default)]
struct Backing {
    objects: Arc<Mutex<HashMap<String, Bytes>>>,
    put_stall_ms: Arc<AtomicU64>,
}

impl Backing {
    fn stall_puts(&self, millis: u64) {
        self.put_stall_ms.store(millis, Ordering::Relaxed);
    }

    fn has(&self, uuid: &str) -> bool {
        self.objects
            .lock()
            .unwrap()
            .contains_key(&format!("objects/{uuid}"))
    }

    fn remove(&self, uuid: &str) {
        self.objects
            .lock()
            .unwrap()
            .remove(&format!("objects/{uuid}"));
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

async fn backing_handler(
    State(state): State<Backing>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if method == Method::PUT {
        let stall = state.put_stall_ms.load(Ordering::Relaxed);
        if stall > 0 {
            tokio::time::sleep(Duration::from_millis(stall)).await;
        }
    }
    let body = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();
    if method == Method::GET && uri.path() == "/data" {
        return xml(
            StatusCode::OK,
            "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
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

async fn put(client: &reqwest::Client, url: &str, body: &[u8]) -> reqwest::Response {
    signed_request(client, Method::PUT, url, Some(body.to_vec()))
        .send()
        .await
        .unwrap()
}

async fn get(client: &reqwest::Client, url: &str) -> reqwest::Response {
    signed_request(client, Method::GET, url, None)
        .send()
        .await
        .unwrap()
}

async fn delete(client: &reqwest::Client, url: &str) -> reqwest::Response {
    signed_request(client, Method::DELETE, url, None)
        .send()
        .await
        .unwrap()
}

fn client_request() -> reqwest::Client {
    reqwest::Client::new()
}

/// Send a cluster request with a raw target. A URL parser would resolve `..`,
/// so a socket writes the request line directly.
async fn raw_cluster_status(url: &str, method: &str, path: &str) -> StatusCode {
    let host = url.trim_start_matches("http://").to_owned();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\n\
         x-s3-sidecar-cluster-token: {CLUSTER_TOKEN}\r\nConnection: close\r\n\r\n"
    );
    let mut stream = tokio::net::TcpStream::connect(&host).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8_lossy(&response);
    let code = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap();
    StatusCode::from_u16(code.parse().unwrap()).unwrap()
}

#[expect(
    clippy::too_many_arguments,
    reason = "the test helper receives all fixture inputs"
)]
fn cluster_base(
    id: &str,
    url: &str,
    backing: &str,
    metadata_path: &std::path::Path,
    nodes: &[&str],
    urls: &[String],
    read: ReadMode,
    replicas: u32,
    read_repair: bool,
) -> Config {
    let cluster_nodes = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| ClusterNodeConfig {
            id: (*node).to_owned(),
            addr: urls[index].clone(),
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
            backend: ConsensusBackend::Single,
            raft: RaftConfig {
                node_id: id.to_owned(),
                peers: vec![id.to_owned()],
                ..RaftConfig::default()
            },
            ..ConsensusConfig::default()
        },
        cluster: ClusterConfig {
            metadata_path: metadata_path.display().to_string(),
            secret: SecretString::new(CLUSTER_TOKEN),
            read_repair,
            replicate_workers: 4,
            reconcile_interval: HumanDuration::new(Duration::from_hours(1)),
            nodes: cluster_nodes,
            ..ClusterConfig::default()
        },
        clients: vec![client_config()],
        buckets: vec![BucketConfig {
            name: "media".to_owned(),
            write_ack: Some(WriteAck::Leader),
            read: Some(read),
            replicas: Some(replicas),
            min_replicas: Some(1),
            ..BucketConfig::default()
        }],
        ..Config::default()
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the test helper receives all fixture inputs"
)]
fn raft_config(
    id: &str,
    index: usize,
    nodes: &[&str],
    urls: &[String],
    backing: &str,
    metadata_path: &std::path::Path,
    read: ReadMode,
    replicas: u32,
    read_repair: bool,
) -> Config {
    let raft_nodes = nodes
        .iter()
        .enumerate()
        .map(|(position, node)| RaftNodeConfig {
            id: (*node).to_owned(),
            addr: urls[position].clone(),
        })
        .collect();
    let mut config = cluster_base(
        id,
        &urls[index],
        backing,
        metadata_path,
        nodes,
        urls,
        read,
        replicas,
        read_repair,
    );
    config.consensus = ConsensusConfig {
        backend: ConsensusBackend::Raft,
        raft: RaftConfig {
            node_id: id.to_owned(),
            bootstrap: index == 0,
            secret: SecretString::new("raft-token"),
            rpc_timeout: HumanDuration::new(Duration::from_secs(2)),
            nodes: raft_nodes,
            ..RaftConfig::default()
        },
        ..ConsensusConfig::default()
    };
    config
}

struct Node {
    id: String,
    url: String,
    state: Arc<AppState>,
    backing: Backing,
    _task: JoinHandle<()>,
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
}

async fn spawn_cluster(read: ReadMode, replicas: u32, read_repair: bool) -> TestCluster {
    spawn_cluster_ack(read, replicas, 1, WriteAck::Leader, read_repair).await
}

async fn spawn_cluster_ack(
    read: ReadMode,
    replicas: u32,
    min_replicas: u32,
    write_ack: WriteAck,
    read_repair: bool,
) -> TestCluster {
    let dir = TempDir::new().unwrap();
    let ids = ["a", "b", "c"];
    let mut listeners = Vec::new();
    let mut backings = Vec::new();
    for _ in &ids {
        listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        backings.push(spawn_backing().await);
    }
    let urls: Vec<String> = listeners
        .iter()
        .map(|listener| format!("http://{}", listener.local_addr().unwrap()))
        .collect();
    let mut nodes = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        let mut config = raft_config(
            id,
            index,
            &ids,
            &urls,
            &backings[index].0,
            &dir.path().join(format!("{id}.redb")),
            read,
            replicas,
            read_repair,
        );
        config.buckets[0].min_replicas = Some(min_replicas);
        config.buckets[0].write_ack = Some(write_ack);
        let listener = listeners.remove(0);
        let state = Arc::new(AppState::new(config).unwrap());
        let app = router(Arc::clone(&state));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        nodes.push(Node {
            id: (*id).to_owned(),
            url: urls[index].clone(),
            state,
            backing: backings[index].1.clone(),
            _task: task,
        });
    }
    let starts: Vec<_> = nodes
        .iter()
        .map(|node| {
            let state = Arc::clone(&node.state);
            tokio::spawn(async move { state.start().await })
        })
        .collect();
    for start in starts {
        start.await.unwrap().unwrap();
    }
    for node in &nodes {
        node.state
            .cluster()
            .unwrap()
            .wait_ready(Duration::from_secs(20))
            .await
            .unwrap();
    }
    TestCluster { nodes, _dir: dir }
}

async fn wait_metadata(cluster: &TestCluster, key: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let ready = cluster.nodes.iter().all(|node| {
            node.state
                .cluster()
                .unwrap()
                .state()
                .get_metadata("media", key)
                .unwrap()
                .is_some_and(|metadata| metadata.is_committed())
        });
        if ready {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "metadata for \"{key}\" did not replicate"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_present(backing: &Backing, uuid: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !backing.has(uuid) {
        assert!(
            Instant::now() < deadline,
            "blob \"{uuid}\" did not reach the replica"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_absent(backing: &Backing, uuid: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while backing.has(uuid) {
        assert!(
            Instant::now() < deadline,
            "blob \"{uuid}\" was not deleted from the replica"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_queue_empty(cluster: &TestCluster, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let empty = cluster.nodes.iter().all(|node| {
            node.state
                .cluster()
                .unwrap()
                .state()
                .replication_jobs()
                .unwrap()
                .is_empty()
        });
        if empty {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the replication queue did not drain"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn metadata(cluster: &TestCluster, id: &str, key: &str) -> s3_sidecar::cluster::state::Metadata {
    cluster
        .node(id)
        .state
        .cluster()
        .unwrap()
        .state()
        .get_metadata("media", key)
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn write_copies_blob_and_reads_use_local_or_peer() {
    let cluster = spawn_cluster(ReadMode::LocalThenPeer, 2, true).await;
    let client = client_request();
    let url = format!("{}/media/replicated", cluster.node("a").url);
    assert_eq!(
        put(&client, &url, b"replicated-body").await.status(),
        StatusCode::OK
    );
    wait_metadata(&cluster, "replicated", Duration::from_secs(10)).await;

    let metadata = metadata(&cluster, "a", "replicated");
    assert_eq!(metadata.replicas.len(), 2);
    assert!(metadata.replicas.iter().any(|node| node == "a"));
    let peer = metadata
        .replicas
        .iter()
        .find(|node| *node != "a")
        .unwrap()
        .clone();
    let missing = ["a", "b", "c"]
        .into_iter()
        .find(|node| !metadata.replicas.iter().any(|value| value == node))
        .unwrap();
    let blob = metadata.blob.clone();
    wait_present(&cluster.node(&peer).backing, &blob, Duration::from_secs(10)).await;

    let peer_url = format!("{}/media/replicated", cluster.node(&peer).url);
    let response = get(&client, &peer_url).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "replicated-body");

    let missing_url = format!("{}/media/replicated", cluster.node(missing).url);
    let response = get(&client, &missing_url).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "replicated-body");
}

#[tokio::test]
async fn reconcile_restores_a_missing_owner_blob() {
    let cluster = spawn_cluster(ReadMode::Local, 2, false).await;
    let client = client_request();
    let url = format!("{}/media/reconcile-owner", cluster.node("a").url);
    assert_eq!(
        put(&client, &url, b"owner-repair").await.status(),
        StatusCode::OK
    );
    wait_metadata(&cluster, "reconcile-owner", Duration::from_secs(10)).await;
    let before = metadata(&cluster, "a", "reconcile-owner");
    let blob = before.blob.clone();
    for replica in &before.replicas {
        wait_present(
            &cluster.node(replica).backing,
            &blob,
            Duration::from_secs(10),
        )
        .await;
    }
    wait_queue_empty(&cluster, Duration::from_secs(10)).await;

    cluster.node("a").backing.remove(&blob);
    assert!(!cluster.node("a").backing.has(&blob));
    cluster
        .node("a")
        .state
        .cluster()
        .unwrap()
        .reconcile_once()
        .await
        .unwrap();
    wait_present(&cluster.node("a").backing, &blob, Duration::from_secs(10)).await;

    let after = metadata(&cluster, "a", "reconcile-owner");
    assert_eq!(after.version, before.version);
    assert_eq!(after.replicas, before.replicas);
}

#[tokio::test]
async fn reconcile_repairs_a_missing_replica_from_a_non_replica_node() {
    // Two replicas on three nodes. The node outside the replica set holds no
    // local blob. Reconcile on that node must still repair a missing replica.
    let cluster = spawn_cluster(ReadMode::Local, 2, false).await;
    let client = client_request();
    let url = format!("{}/media/reconcile-outsider", cluster.node("a").url);
    assert_eq!(
        put(&client, &url, b"outsider-repair").await.status(),
        StatusCode::OK
    );
    wait_metadata(&cluster, "reconcile-outsider", Duration::from_secs(10)).await;
    let before = metadata(&cluster, "a", "reconcile-outsider");
    assert_eq!(before.replicas.len(), 2);
    let blob = before.blob.clone();
    for replica in &before.replicas {
        wait_present(
            &cluster.node(replica).backing,
            &blob,
            Duration::from_secs(10),
        )
        .await;
    }
    wait_queue_empty(&cluster, Duration::from_secs(10)).await;

    let outsider = ["a", "b", "c"]
        .into_iter()
        .find(|node| !before.replicas.iter().any(|value| value == node))
        .unwrap();
    let broken = before
        .replicas
        .iter()
        .find(|node| **node != "a")
        .unwrap()
        .clone();
    assert!(!cluster.node(outsider).backing.has(&blob));
    cluster.node(&broken).backing.remove(&blob);
    assert!(!cluster.node(&broken).backing.has(&blob));

    cluster
        .node(outsider)
        .state
        .cluster()
        .unwrap()
        .reconcile_once()
        .await
        .unwrap();
    wait_present(
        &cluster.node(&broken).backing,
        &blob,
        Duration::from_secs(10),
    )
    .await;
    wait_queue_empty(&cluster, Duration::from_secs(10)).await;
}

#[tokio::test]
async fn read_repair_stores_the_blob_when_enabled() {
    let cluster = spawn_cluster(ReadMode::LocalThenPeer, 2, true).await;
    let client = client_request();
    let url = format!("{}/media/repair-on", cluster.node("a").url);
    assert_eq!(
        put(&client, &url, b"repair-body").await.status(),
        StatusCode::OK
    );
    wait_metadata(&cluster, "repair-on", Duration::from_secs(10)).await;
    let metadata = metadata(&cluster, "a", "repair-on");
    let missing = ["a", "b", "c"]
        .into_iter()
        .find(|node| !metadata.replicas.iter().any(|value| value == node))
        .unwrap();
    let blob = metadata.blob.clone();
    assert!(!cluster.node(missing).backing.has(&blob));

    let response = get(
        &client,
        &format!("{}/media/repair-on", cluster.node(missing).url),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "repair-body");
    wait_present(
        &cluster.node(missing).backing,
        &blob,
        Duration::from_secs(10),
    )
    .await;
}

#[tokio::test]
async fn read_repair_does_not_store_when_disabled() {
    let cluster = spawn_cluster(ReadMode::LocalThenPeer, 2, false).await;
    let client = client_request();
    let url = format!("{}/media/repair-off", cluster.node("a").url);
    assert_eq!(
        put(&client, &url, b"no-repair").await.status(),
        StatusCode::OK
    );
    wait_metadata(&cluster, "repair-off", Duration::from_secs(10)).await;
    let metadata = metadata(&cluster, "a", "repair-off");
    let missing = ["a", "b", "c"]
        .into_iter()
        .find(|node| !metadata.replicas.iter().any(|value| value == node))
        .unwrap();
    let blob = metadata.blob.clone();

    let response = get(
        &client,
        &format!("{}/media/repair-off", cluster.node(missing).url),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "no-repair");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!cluster.node(missing).backing.has(&blob));
}

#[tokio::test]
async fn read_repair_does_not_delay_the_client_response() {
    let cluster = spawn_cluster(ReadMode::LocalThenPeer, 2, true).await;
    let client = client_request();
    let url = format!("{}/media/repair-slow", cluster.node("a").url);
    let body = vec![7_u8; 8 * 1024 * 1024];
    assert_eq!(put(&client, &url, &body).await.status(), StatusCode::OK);
    wait_metadata(&cluster, "repair-slow", Duration::from_secs(10)).await;
    let metadata = metadata(&cluster, "a", "repair-slow");
    let missing = ["a", "b", "c"]
        .into_iter()
        .find(|node| !metadata.replicas.iter().any(|value| value == node))
        .unwrap();
    let blob = metadata.blob.clone();
    assert!(!cluster.node(missing).backing.has(&blob));

    let backing = cluster.node(missing).backing.clone();
    backing.stall_puts(3_000);
    let started = Instant::now();
    let response = get(
        &client,
        &format!("{}/media/repair-slow", cluster.node(missing).url),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let received = response.bytes().await.unwrap();
    assert_eq!(received.as_ref(), body.as_slice());
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "client waited {elapsed:?} for the repair write"
    );

    backing.stall_puts(0);
    wait_present(&backing, &blob, Duration::from_secs(10)).await;
}

#[tokio::test]
async fn read_without_a_replica_returns_the_owner() {
    let cluster = spawn_cluster(ReadMode::LocalThenPeer, 2, false).await;
    let client = client_request();
    let url = format!("{}/media/lost", cluster.node("a").url);
    assert_eq!(
        put(&client, &url, b"lost-body").await.status(),
        StatusCode::OK
    );
    wait_metadata(&cluster, "lost", Duration::from_secs(10)).await;
    let metadata = metadata(&cluster, "a", "lost");
    let blob = metadata.blob.clone();
    for replica in &metadata.replicas {
        wait_present(
            &cluster.node(replica).backing,
            &blob,
            Duration::from_secs(10),
        )
        .await;
    }
    wait_queue_empty(&cluster, Duration::from_secs(10)).await;
    for replica in &metadata.replicas {
        cluster.node(replica).backing.remove(&blob);
    }
    let missing = ["a", "b", "c"]
        .into_iter()
        .find(|node| !metadata.replicas.iter().any(|value| value == node))
        .unwrap();
    let response = get(
        &client,
        &format!("{}/media/lost", cluster.node(missing).url),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .headers()
            .get("x-s3-sidecar-owner")
            .and_then(|value| value.to_str().ok()),
        Some("a")
    );
}

#[tokio::test]
async fn delete_propagates_to_every_replica() {
    let cluster = spawn_cluster(ReadMode::Local, 3, false).await;
    let client = client_request();
    let url = format!("{}/media/gone", cluster.node("a").url);
    assert_eq!(
        put(&client, &url, b"gone-body").await.status(),
        StatusCode::OK
    );
    wait_metadata(&cluster, "gone", Duration::from_secs(10)).await;
    let metadata = metadata(&cluster, "a", "gone");
    let blob = metadata.blob.clone();
    for node in &cluster.nodes {
        wait_present(&node.backing, &blob, Duration::from_secs(10)).await;
    }

    let response = delete(&client, &url).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    for node in &cluster.nodes {
        wait_absent(&node.backing, &blob, Duration::from_secs(20)).await;
        let response = get(&client, &format!("{}/media/gone", node.url)).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    clippy::similar_names,
    reason = "the test covers the full flow and uses similar field names"
)]
async fn restart_resumes_a_queued_replication_job() {
    let dir = TempDir::new().unwrap();
    let (backing_a_url, backing_a) = spawn_backing().await;
    let (backing_b_url, backing_b) = spawn_backing().await;
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url_b = format!("http://{}", listener_b.local_addr().unwrap());
    drop(listener_b);
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url_a = format!("http://{}", listener_a.local_addr().unwrap());
    let ids = ["a", "b"];
    let urls = vec![url_a.clone(), url_b.clone()];
    let metadata_path = dir.path().join("a.redb");
    let config = cluster_base(
        "a",
        &url_a,
        &backing_a_url,
        &metadata_path,
        &ids,
        &urls,
        ReadMode::Local,
        2,
        false,
    );
    let state = Arc::new(AppState::new(config.clone()).unwrap());
    state.start().await.unwrap();
    state.set_ready(true);
    let app = router(Arc::clone(&state));
    let task = tokio::spawn(async move {
        axum::serve(listener_a, app).await.unwrap();
    });
    let client = client_request();
    assert_eq!(
        put(&client, &format!("{url_a}/media/queued"), b"queued-body")
            .await
            .status(),
        StatusCode::OK
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let jobs = state.cluster().unwrap().state().replication_jobs().unwrap();
        if jobs
            .iter()
            .any(|job| job.targets.iter().any(|node| node == "b"))
        {
            break;
        }
        assert!(Instant::now() < deadline, "replication job was not queued");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    task.abort();
    let _ = task.await;
    drop(state);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let store = MetadataStore::open(&metadata_path).unwrap();
    let mut jobs = store.replication_jobs().unwrap();
    assert_eq!(jobs.len(), 1);
    let mut job = jobs.remove(0);
    let blob = job.uuid.clone();
    job.next_at = s3_sidecar::cluster::state::unix_now();
    store.enqueue_replication_job(&job).unwrap();
    drop(store);

    let config_b = cluster_base(
        "b",
        &url_b,
        &backing_b_url,
        &dir.path().join("b.redb"),
        &ids,
        &urls,
        ReadMode::Local,
        1,
        false,
    );
    let state_b = Arc::new(AppState::new(config_b).unwrap());
    state_b.start().await.unwrap();
    state_b.set_ready(true);
    let listener_b = TcpListener::bind(
        url_b
            .trim_start_matches("http://")
            .parse::<std::net::SocketAddr>()
            .unwrap(),
    )
    .await
    .unwrap();
    let app_b = router(Arc::clone(&state_b));
    tokio::spawn(async move {
        axum::serve(listener_b, app_b).await.unwrap();
    });

    let restarted = Arc::new(AppState::new(config).unwrap());
    restarted.start().await.unwrap();
    restarted.set_ready(true);
    let listener_a = TcpListener::bind(
        url_a
            .trim_start_matches("http://")
            .parse::<std::net::SocketAddr>()
            .unwrap(),
    )
    .await
    .unwrap();
    let app = router(Arc::clone(&restarted));
    tokio::spawn(async move {
        axum::serve(listener_a, app).await.unwrap();
    });

    wait_present(&backing_b, &blob, Duration::from_secs(20)).await;
    assert!(backing_a.has(&blob));
}

#[tokio::test]
async fn blob_transport_requires_the_token_and_checks_references() {
    let dir = TempDir::new().unwrap();
    let (backing_url, _backing) = spawn_backing().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let ids = ["a"];
    let urls = vec![url.clone()];
    let config = cluster_base(
        "a",
        &url,
        &backing_url,
        &dir.path().join("a.redb"),
        &ids,
        &urls,
        ReadMode::Local,
        1,
        false,
    );
    let state = Arc::new(AppState::new(config).unwrap());
    state.start().await.unwrap();
    state.set_ready(true);
    let app = router(Arc::clone(&state));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = client_request();
    assert_eq!(
        put(&client, &format!("{url}/media/range"), b"0123456789")
            .await
            .status(),
        StatusCode::OK
    );
    let blob = state
        .cluster()
        .unwrap()
        .state()
        .get_metadata("media", "range")
        .unwrap()
        .unwrap()
        .blob;
    let blob_url = format!("{url}/_blob/{blob}");

    let response = client.get(&blob_url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = client
        .get(&blob_url)
        .header("x-s3-sidecar-cluster-token", "wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = client
        .get(&blob_url)
        .header("x-s3-sidecar-cluster-token", CLUSTER_TOKEN)
        .header(header::RANGE, "bytes=2-5")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.text().await.unwrap(), "2345");

    let response = client
        .head(&blob_url)
        .header("x-s3-sidecar-cluster-token", CLUSTER_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap(),
        "10"
    );

    let response = client
        .delete(&blob_url)
        .header("x-s3-sidecar-cluster-token", CLUSTER_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn blob_transport_rejects_invalid_identifiers() {
    let dir = TempDir::new().unwrap();
    let (backing_url, _backing) = spawn_backing().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let ids = ["a"];
    let urls = vec![url.clone()];
    let config = cluster_base(
        "a",
        &url,
        &backing_url,
        &dir.path().join("a.redb"),
        &ids,
        &urls,
        ReadMode::Local,
        1,
        false,
    );
    let state = Arc::new(AppState::new(config).unwrap());
    state.start().await.unwrap();
    state.set_ready(true);
    let app = router(Arc::clone(&state));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    for (method, path) in [
        ("GET", "/_blob/.."),
        ("DELETE", "/_blob/.."),
        ("HEAD", "/_blob/not-a-blob"),
    ] {
        assert_eq!(
            raw_cluster_status(&url, method, path).await,
            StatusCode::BAD_REQUEST,
            "{method} {path}"
        );
    }
}

#[tokio::test]
async fn quorum_write_survives_leader_loss_when_a_quorum_remains() {
    let cluster = spawn_cluster_ack(ReadMode::LocalThenPeer, 3, 2, WriteAck::Quorum, false).await;
    let leader = cluster
        .nodes
        .iter()
        .find(|node| {
            node.state
                .cluster()
                .and_then(|cluster| cluster.raft_node())
                .is_some_and(|raft| raft.is_leader())
        })
        .unwrap()
        .id
        .clone();
    stop_raft(cluster.node(&leader)).await;
    let new_leader = wait_any_leader(&cluster, &[leader.as_str()], Duration::from_secs(20)).await;
    let client = client_request();
    let url = format!("{}/media/after-leader-loss", cluster.node(&new_leader).url);
    assert_eq!(
        put(&client, &url, b"leader-loss").await.status(),
        StatusCode::OK
    );
    let metadata = metadata(&cluster, &new_leader, "after-leader-loss");
    assert!(metadata.is_committed());
    assert!(metadata.replicas.iter().any(|node| node == &new_leader));
}

#[tokio::test]
async fn quorum_write_without_a_metadata_quorum_returns_503() {
    let cluster = spawn_cluster_ack(ReadMode::Local, 1, 1, WriteAck::Quorum, false).await;
    let writer = cluster.nodes[0].id.clone();
    for node in cluster.nodes.iter().filter(|node| node.id != writer) {
        stop_raft(node).await;
    }
    shutdown_raft(cluster.node(&writer)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let client = client_request();
    let url = format!("{}/media/no-metadata-quorum", cluster.node(&writer).url);
    let response = put(&client, &url, b"no-quorum").await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        cluster
            .node(&writer)
            .state
            .cluster()
            .unwrap()
            .state()
            .get_metadata("media", "no-metadata-quorum")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        cluster.node(&writer).backing.objects.lock().unwrap().len(),
        0
    );
}

async fn shutdown_raft(node: &Node) {
    if let Some(cluster) = node.state.cluster()
        && let Some(raft) = cluster.raft_node()
    {
        let _ = raft.raft().shutdown().await;
    }
}

#[expect(
    clippy::used_underscore_binding,
    reason = "the test reads an underscore-prefixed field"
)]
async fn stop_raft(node: &Node) {
    shutdown_raft(node).await;
    node._task.abort();
}

async fn wait_any_leader(cluster: &TestCluster, skip: &[&str], timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        for node in &cluster.nodes {
            if skip.contains(&node.id.as_str()) {
                continue;
            }
            if node
                .state
                .cluster()
                .and_then(|cluster| cluster.raft_node())
                .is_some_and(|raft| raft.is_leader() && raft.is_ready())
            {
                return node.id.clone();
            }
        }
        assert!(
            Instant::now() < deadline,
            "no raft leader was elected after the loss"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[test]
fn placement_is_deterministic_and_uses_all_nodes() {
    let nodes = vec!["c".to_owned(), "a".to_owned(), "b".to_owned()];
    let first = s3_sidecar::cluster::replication::replica_set(&nodes, "a", 2, "media", "key");
    let second = s3_sidecar::cluster::replication::replica_set(&nodes, "a", 2, "media", "key");
    assert_eq!(first, second);
    assert_eq!(first.len(), 2);
    assert_eq!(first[0], "a");
    let all = s3_sidecar::cluster::replication::replica_set(&nodes, "a", 5, "media", "key");
    assert_eq!(all.len(), 3);
}

#[test]
fn replication_job_round_trips_through_the_store() {
    let dir = TempDir::new().unwrap();
    let store = MetadataStore::open(&dir.path().join("jobs.redb")).unwrap();
    let job = ReplicationJob {
        uuid: "blob".to_owned(),
        op: ReplicationOp::Put,
        targets: vec!["b".to_owned()],
        done: Vec::new(),
        attempts: 0,
        next_at: 0,
        created_at: 0,
        bucket: "media".to_owned(),
        key: "key".to_owned(),
        lease_owner: None,
        lease_expires_at: None,
    };
    store.enqueue_replication_job(&job).unwrap();
    let jobs = store.replication_jobs().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].uuid, "blob");
    store.complete_replication_target("blob", "b").unwrap();
    assert!(store.replication_jobs().unwrap().is_empty());
}

#[test]
fn lease_prevents_reclaim_until_expiry() {
    let dir = TempDir::new().unwrap();
    let store = MetadataStore::open(&dir.path().join("jobs.redb")).unwrap();
    let job = ReplicationJob {
        uuid: "blob".to_owned(),
        op: ReplicationOp::Put,
        targets: vec!["b".to_owned()],
        done: Vec::new(),
        attempts: 0,
        next_at: 0,
        created_at: 0,
        bucket: "media".to_owned(),
        key: "key".to_owned(),
        lease_owner: None,
        lease_expires_at: None,
    };
    store.enqueue_replication_job(&job).unwrap();

    let claimed = store
        .claim_due_replication_job(0, 30, "worker-a", &[])
        .unwrap()
        .expect("one job is due");
    assert_eq!(claimed.uuid, "blob");

    let stored = store.replication_job("blob").unwrap().unwrap();
    assert_eq!(stored.lease_owner.as_deref(), Some("worker-a"));
    assert_eq!(stored.lease_expires_at, Some(30));

    let during_lease = store
        .claim_due_replication_job(10, 30, "worker-b", &[])
        .unwrap();
    assert!(
        during_lease.is_none(),
        "worker-b must not re-claim a leased job"
    );

    let active = store
        .claim_due_replication_job(40, 30, "worker-b", &["blob".to_owned()])
        .unwrap();
    assert!(
        active.is_none(),
        "an active job must be skipped after its lease expires"
    );

    let after_lease = store
        .claim_due_replication_job(40, 30, "worker-b", &[])
        .unwrap()
        .expect("the job is due after the lease expires");
    assert_eq!(after_lease.uuid, "blob");
    assert_eq!(after_lease.lease_owner.as_deref(), Some("worker-a"));
}
