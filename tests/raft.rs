#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Raft backend integration tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use reqwest::Url;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, CredentialStore};
use s3_sidecar::cluster::coordinator::{CommitRequest, Coordinator, Entry};
use s3_sidecar::cluster::raft::RaftNode;
use s3_sidecar::cluster::raft::coordinator::RaftCoordinator;
use s3_sidecar::cluster::raft::memory::{self, MemoryNetworkFactory};
use s3_sidecar::cluster::state::MetadataStore;
use s3_sidecar::config::duration::HumanDuration;
use s3_sidecar::config::secret::SecretString;
use s3_sidecar::config::{
    BucketConfig, ClientConfig, ClusterConfig, Config, ConsensusBackend, ConsensusConfig,
    EndpointConfig, Mode, RaftConfig, RaftNodeConfig, ReadMode,
};
use s3_sidecar::server::{AppState, router};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

fn raft_config(ids: &[&str], node_id: &str, bootstrap: bool, path: &std::path::Path) -> Config {
    let nodes = ids
        .iter()
        .enumerate()
        .map(|(index, id)| RaftNodeConfig {
            id: (*id).to_owned(),
            addr: format!("http://127.0.0.1:{}", 9000 + index),
        })
        .collect();
    Config {
        mode: Mode::Cluster,
        backing: EndpointConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            bucket: "data".to_owned(),
            ..EndpointConfig::default()
        },
        consensus: ConsensusConfig {
            backend: ConsensusBackend::Raft,
            raft: RaftConfig {
                node_id: node_id.to_owned(),
                bootstrap,
                secret: SecretString::new("token"),
                addr: format!("http://127.0.0.1:{}", 9000),
                rpc_timeout: HumanDuration::new(Duration::from_secs(2)),
                nodes,
                ..RaftConfig::default()
            },
        },
        cluster: ClusterConfig {
            metadata_path: path.display().to_string(),
            ..ClusterConfig::default()
        },
        ..Config::default()
    }
}

fn commit(key: &str, blob: &str) -> CommitRequest {
    CommitRequest {
        bucket: "media".to_owned(),
        key: key.to_owned(),
        base_version: 0,
        blob: blob.to_owned(),
        size: 5,
        checksum: "checksum".to_owned(),
        content_type: Some("text/plain".to_owned()),
        etag: "etag".to_owned(),
        user_metadata: std::collections::BTreeMap::new(),
        owner: "a".to_owned(),
        replicas: vec!["a".to_owned()],
        if_none_match: false,
        if_match: None,
        cache_control: None,
        content_encoding: None,
        content_disposition: None,
        committed_at: 100,
    }
}

async fn wait_leader(nodes: &[Arc<RaftNode>], timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if node.is_leader() && node.is_ready() {
                return index;
            }
        }
        assert!(Instant::now() < deadline, "no raft leader was elected");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_metadata(store: &MetadataStore, key: &str, blob: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if store
            .get_metadata("media", key)
            .unwrap()
            .is_some_and(|metadata| metadata.is_committed() && metadata.blob == blob)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "metadata for \"{key}\" did not replicate"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn single_node_bootstraps_and_persists_after_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("meta.redb");
    let registry = memory::registry();
    let metrics = Arc::new(s3_sidecar::metrics::Metrics::default());
    let config = raft_config(&["a"], "a", false, &path);
    let store = Arc::new(MetadataStore::open(&path).unwrap());
    let node = RaftNode::start_with_network(
        Arc::clone(&store),
        &config,
        Arc::clone(&metrics),
        MemoryNetworkFactory::new(Arc::clone(&registry)),
    )
    .await
    .unwrap();
    memory::register(&registry, "a", node.raft().clone());
    let coordinator = Arc::new(RaftCoordinator::new(
        Arc::clone(&store),
        Arc::clone(&metrics),
        "a".to_owned(),
    ));
    coordinator.attach(Arc::clone(&node)).unwrap();
    node.raft()
        .wait(Some(Duration::from_secs(10)))
        .state(openraft::ServerState::Leader, "single node leader")
        .await
        .unwrap();

    let applied = coordinator
        .propose(Entry::Commit {
            term: coordinator.term(),
            request: Box::new(commit("k", "blob-one")),
        })
        .await
        .unwrap();
    assert!(applied.index > 0);
    wait_metadata(&store, "k", "blob-one", Duration::from_secs(2)).await;

    node.raft().shutdown().await.unwrap();
    drop(node);
    drop(coordinator);
    drop(store);
    memory::unregister(&registry, "a");

    let store = Arc::new(MetadataStore::open(&path).unwrap());
    assert!(store.applied_index() > 0);
    let metadata = store.get_metadata("media", "k").unwrap().unwrap();
    assert!(metadata.is_committed());
    assert_eq!(metadata.blob, "blob-one");

    let node = RaftNode::start_with_network(
        Arc::clone(&store),
        &config,
        Arc::clone(&metrics),
        MemoryNetworkFactory::new(Arc::clone(&registry)),
    )
    .await
    .unwrap();
    memory::register(&registry, "a", node.raft().clone());
    let coordinator = RaftCoordinator::new(Arc::clone(&store), metrics, "a".to_owned());
    coordinator.attach(Arc::clone(&node)).unwrap();
    node.raft()
        .wait(Some(Duration::from_secs(10)))
        .state(openraft::ServerState::Leader, "restarted leader")
        .await
        .unwrap();
    coordinator
        .propose(Entry::Commit {
            term: coordinator.term(),
            request: Box::new(commit("second", "blob-two")),
        })
        .await
        .unwrap();
    wait_metadata(&store, "second", "blob-two", Duration::from_secs(2)).await;
    node.raft().shutdown().await.unwrap();
}

#[tokio::test]
async fn non_bootstrap_node_does_not_initialize_a_competing_cluster() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("b.redb");
    let registry = memory::registry();
    let metrics = Arc::new(s3_sidecar::metrics::Metrics::default());
    let config = raft_config(&["a", "b"], "b", false, &path);
    let store = Arc::new(MetadataStore::open(&path).unwrap());
    let node = RaftNode::start_with_network(
        Arc::clone(&store),
        &config,
        metrics,
        MemoryNetworkFactory::new(Arc::clone(&registry)),
    )
    .await
    .unwrap();
    memory::register(&registry, "b", node.raft().clone());
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(!node.raft().is_initialized().await.unwrap());
    assert!(node.leader().is_none());
    node.raft().shutdown().await.unwrap();
}

#[tokio::test]
async fn three_nodes_replicate_and_fail_over() {
    let dir = TempDir::new().unwrap();
    let registry = memory::registry();
    let metrics = Arc::new(s3_sidecar::metrics::Metrics::default());
    let ids = ["a", "b", "c"];
    let mut nodes = Vec::new();
    for id in ids {
        let path = dir.path().join(format!("{id}.redb"));
        let config = raft_config(&ids, id, id == "a", &path);
        let store = Arc::new(MetadataStore::open(&path).unwrap());
        let node = RaftNode::start_with_network(
            Arc::clone(&store),
            &config,
            Arc::clone(&metrics),
            MemoryNetworkFactory::new(Arc::clone(&registry)),
        )
        .await
        .unwrap();
        memory::register(&registry, id, node.raft().clone());
        let coordinator = Arc::new(RaftCoordinator::new(
            Arc::clone(&store),
            Arc::clone(&metrics),
            id.to_owned(),
        ));
        coordinator.attach(Arc::clone(&node)).unwrap();
        nodes.push((id.to_owned(), node, store, coordinator));
    }
    let handles: Vec<Arc<RaftNode>> = nodes
        .iter()
        .map(|(_, node, _, _)| Arc::clone(node))
        .collect();
    let leader = wait_leader(&handles, Duration::from_secs(15)).await;
    let leader_id = nodes[leader].0.clone();
    nodes[leader]
        .3
        .propose(Entry::Commit {
            term: nodes[leader].3.term(),
            request: Box::new(commit("shared", "blob-shared")),
        })
        .await
        .unwrap();
    for (_, _, store, _) in &nodes {
        wait_metadata(store, "shared", "blob-shared", Duration::from_secs(10)).await;
    }

    let (id, node, _, coordinator) = nodes.remove(leader);
    assert_eq!(id, leader_id);
    memory::unregister(&registry, &id);
    node.raft().shutdown().await.unwrap();
    drop(node);
    drop(coordinator);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let handles: Vec<Arc<RaftNode>> = nodes
        .iter()
        .map(|(_, node, _, _)| Arc::clone(node))
        .collect();
    let new_leader = wait_leader(&handles, Duration::from_secs(15)).await;
    nodes[new_leader]
        .3
        .propose(Entry::Commit {
            term: nodes[new_leader].3.term(),
            request: Box::new(commit("after-failover", "blob-after")),
        })
        .await
        .unwrap();
    for (_, _, store, _) in &nodes {
        wait_metadata(
            store,
            "after-failover",
            "blob-after",
            Duration::from_secs(10),
        )
        .await;
    }
    for (_, node, _, _) in &nodes {
        node.raft().shutdown().await.unwrap();
    }
}

#[derive(Clone, Default)]
struct Backing {
    objects: Arc<Mutex<HashMap<String, Bytes>>>,
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
    _headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path();
    if method == Method::GET && path == "/data" {
        return xml(
            StatusCode::OK,
            "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
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
            state.objects.lock().unwrap().insert(key.to_owned(), body);
            let mut response = Response::new(axum::body::Body::empty());
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
                Response::new(axum::body::Body::empty())
            } else {
                Response::new(axum::body::Body::from(value.clone()))
            };
            *response.status_mut() = StatusCode::OK;
            if let Ok(value) = HeaderValue::from_str(&value.len().to_string()) {
                response.headers_mut().insert(header::CONTENT_LENGTH, value);
            }
            response
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn xml(status: StatusCode, body: &str) -> Response {
    let mut response = Response::new(axum::body::Body::from(body.to_owned()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response
}

struct Sidecar {
    url: String,
    state: Arc<AppState>,
    task: JoinHandle<()>,
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
        s3_sidecar::auth::EMPTY_SHA256
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

fn spawn_sidecar(listener: TcpListener, config: Config) -> (Sidecar, Arc<AppState>) {
    let state = Arc::new(AppState::new(config).unwrap());
    let address = listener.local_addr().unwrap();
    let app = router(Arc::clone(&state));
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        Sidecar {
            url: format!("http://{address}"),
            state: Arc::clone(&state),
            task,
        },
        state,
    )
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn http_transport_replicates_and_serves_owner_headers() {
    let dir = TempDir::new().unwrap();
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let urls: Vec<String> = listeners
        .iter()
        .map(|listener| format!("http://{}", listener.local_addr().unwrap()))
        .collect();
    let mut backings = Vec::new();
    for _ in 0..3 {
        backings.push(spawn_backing().await);
    }

    let ids = ["a", "b", "c"];
    let mut states = Vec::new();
    let mut sidecars = Vec::new();
    for index in 0..3 {
        let listener = listeners.remove(0);
        let path = dir.path().join(format!("{}.redb", ids[index]));
        let nodes = ids
            .iter()
            .enumerate()
            .map(|(node_index, id)| RaftNodeConfig {
                id: (*id).to_owned(),
                addr: urls[node_index].clone(),
            })
            .collect();
        let config = Config {
            mode: Mode::Cluster,
            server: s3_sidecar::config::ServerConfig {
                bind: urls[index].trim_start_matches("http://").to_owned(),
            },
            backing: EndpointConfig {
                endpoint: backings[index].0.clone(),
                bucket: "data".to_owned(),
                region: "us-east-1".to_owned(),
                access_key_id: "AKIA".to_owned(),
                secret_access_key: SecretString::new("secret"),
                ..EndpointConfig::default()
            },
            consensus: ConsensusConfig {
                backend: ConsensusBackend::Raft,
                raft: RaftConfig {
                    node_id: ids[index].to_owned(),
                    bootstrap: index == 0,
                    secret: SecretString::new("token"),
                    addr: urls[index].clone(),
                    rpc_timeout: HumanDuration::new(Duration::from_secs(2)),
                    nodes,
                    ..RaftConfig::default()
                },
            },
            cluster: ClusterConfig {
                metadata_path: path.display().to_string(),
                ..ClusterConfig::default()
            },
            clients: vec![client_config()],
            buckets: vec![BucketConfig {
                name: "media".to_owned(),
                read: Some(ReadMode::Leader),
                ..BucketConfig::default()
            }],
            ..Config::default()
        };
        let (sidecar, state) = spawn_sidecar(listener, config);
        sidecars.push(sidecar);
        states.push(state);
    }
    let starts: Vec<_> = states
        .iter()
        .cloned()
        .map(|state| tokio::spawn(async move { state.start().await }))
        .collect();
    for start in starts {
        start.await.unwrap().unwrap();
    }
    for state in &states {
        state
            .cluster()
            .unwrap()
            .wait_ready(Duration::from_secs(20))
            .await
            .unwrap();
    }

    let leader = sidecars
        .iter()
        .position(|sidecar| {
            sidecar.state.cluster().unwrap().is_ready()
                && sidecar
                    .state
                    .cluster()
                    .unwrap()
                    .raft_node()
                    .unwrap()
                    .is_leader()
        })
        .unwrap();
    let follower = (0..3).find(|index| *index != leader).unwrap();
    assert_ne!(leader, follower);

    let client = reqwest::Client::new();
    let put_url = format!("{}/media/object.txt", sidecars[follower].url);
    let response = signed_request(&client, Method::PUT, &put_url, Some(b"hello".to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = signed_request(&client, Method::GET, &put_url, None)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.unwrap(),
        Bytes::from_static(b"hello")
    );

    let reader = (0..3).find(|index| *index != follower).unwrap();
    let reader_url = format!("{}/media/object.txt", sidecars[reader].url);
    let response = signed_request(&client, Method::GET, &reader_url, None)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .headers()
            .get("x-s3-sidecar-owner")
            .and_then(|value| value.to_str().ok()),
        Some(ids[follower])
    );

    let list_url = format!("{}/media?list-type=2", sidecars[leader].url);
    let response = signed_request(&client, Method::GET, &list_url, None)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = response.text().await.unwrap();
    assert!(text.contains("<Key>object.txt</Key>"));

    let response = signed_request(&client, Method::DELETE, &put_url, None)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = signed_request(&client, Method::GET, &put_url, None)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let vote_url = format!("{}/_raft/vote", sidecars[follower].url);
    let response = client.post(&vote_url).body("{}").send().await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = client
        .post(&vote_url)
        .header("x-s3-sidecar-raft-token", "wrong")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = client
        .post(&vote_url)
        .header("x-s3-sidecar-raft-token", "token")
        .body("{not-json}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    for sidecar in sidecars {
        sidecar.task.abort();
        let _ = sidecar.task.await;
    }
}
