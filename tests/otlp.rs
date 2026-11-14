#![allow(clippy::unwrap_used, clippy::expect_used)]

//! OTLP metrics export integration tests with an in-process HTTP collector.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::post;
use s3_sidecar::config::duration::HumanDuration;
use s3_sidecar::config::{OtelConfig, OtelProtocol};
use s3_sidecar::metrics::Metrics;
use s3_sidecar::telemetry::Telemetry;
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
struct Request {
    content_type: Option<String>,
    body_len: usize,
}

#[derive(Clone, Default)]
struct Collector {
    requests: Arc<Mutex<Vec<Request>>>,
}

impl Collector {
    fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }
}

async fn collect(
    State(collector): State<Collector>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    collector.requests.lock().unwrap().push(Request {
        content_type: headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        body_len: body.len(),
    });
    StatusCode::OK
}

async fn spawn_collector() -> (String, Collector) {
    let collector = Collector::default();
    let app = Router::new()
        .route("/v1/metrics", post(collect))
        .with_state(collector.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), collector)
}

fn otel_config(endpoint: String) -> OtelConfig {
    OtelConfig {
        enabled: true,
        service_name: Some("s3-sidecar-test".to_owned()),
        service_version: "0.1.0".to_owned(),
        endpoint: Some(endpoint),
        protocol: Some(OtelProtocol::HttpProtobuf),
        headers: BTreeMap::new(),
        headers_file: None,
        timeout: HumanDuration::new(Duration::from_secs(2)),
        export_interval: Some(HumanDuration::new(Duration::from_millis(50))),
        export_timeout: HumanDuration::new(Duration::from_secs(2)),
        resource_attributes: BTreeMap::from([(
            "deployment.environment.name".to_owned(),
            "test".to_owned(),
        )]),
        ..OtelConfig::default()
    }
}

async fn wait_for_export(collector: &Collector) -> Vec<Request> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let requests = collector.requests();
        if !requests.is_empty() || tokio::time::Instant::now() >= deadline {
            return requests;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn otlp_http_export_sends_protobuf() {
    let (endpoint, collector) = spawn_collector().await;
    let telemetry = Telemetry::new(&otel_config(endpoint), Some("node-a"))
        .unwrap()
        .unwrap();
    let metrics = Arc::new(Metrics::new(&telemetry.meter()));
    metrics.hit();
    let requests = wait_for_export(&collector).await;
    telemetry.shutdown();
    assert!(!requests.is_empty(), "the collector received no export");
    let request = &requests[0];
    assert!(
        request
            .content_type
            .as_deref()
            .is_some_and(|value| value.starts_with("application/x-protobuf")),
        "unexpected content type: {:?}",
        request.content_type
    );
    assert!(request.body_len > 0, "the export body was empty");
}

#[tokio::test]
async fn disabled_export_sends_nothing() {
    let (endpoint, collector) = spawn_collector().await;
    let mut config = otel_config(endpoint);
    config.enabled = false;
    let telemetry = Telemetry::new(&config, Some("node-a")).unwrap();
    assert!(telemetry.is_none());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(collector.requests().is_empty());
}
