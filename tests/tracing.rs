#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Trace, log, propagation, sampling, and OTLP export tests.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use opentelemetry::trace::{SpanId, TraceContextExt, TraceId, TracerProvider as _};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{
    BatchLogProcessor, InMemoryLogExporter, LogBatch, LogExporter, SdkLogRecord, SdkLoggerProvider,
};
use opentelemetry_sdk::trace::{
    BatchSpanProcessor, InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData, SpanExporter,
};
use reqwest::Url;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, CredentialStore, EMPTY_SHA256};
use s3_sidecar::config::duration::HumanDuration;
use s3_sidecar::config::secret::SecretString;
use s3_sidecar::config::{
    CacheConfig, CacheMode, ClientConfig, Config, EndpointConfig, Mode, OtelConfig, OtelProtocol,
};
use s3_sidecar::metrics::Metrics;
use s3_sidecar::server::{AppState, router};
use s3_sidecar::telemetry::Telemetry;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tracing::{info, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;

const TRACE_ID_HEX: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const PARENT_ID_HEX: &str = "00f067aa0ba902b7";

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    headers: HeaderMap,
}

#[derive(Clone, Default)]
struct Origin {
    records: Arc<Mutex<Vec<Recorded>>>,
}

async fn origin_handler(
    State(origin): State<Origin>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    origin.records.lock().unwrap().push(Recorded {
        method: method.to_string(),
        path: uri.path().to_owned(),
        headers: headers.clone(),
    });
    let mut response = Response::new(Body::from("hello"));
    response
        .headers_mut()
        .insert(header::ETAG, HeaderValue::from_static("\"etag\""));
    response
}

async fn spawn_origin() -> (String, Arc<Mutex<Vec<Recorded>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = Origin::default();
    let records = Arc::clone(&origin.records);
    let app = Router::new().fallback(origin_handler).with_state(origin);
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

fn config(origin: String, record_object_keys: bool) -> Config {
    Config {
        mode: Mode::Cache,
        cache: CacheConfig {
            mode: CacheMode::Off,
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
        otel: OtelConfig {
            enabled: true,
            metrics: false,
            traces: true,
            logs: true,
            record_object_keys,
            ..OtelConfig::default()
        },
        ..Config::default()
    }
}

async fn spawn_sidecar(origin: String, record_object_keys: bool) -> (String, Arc<AppState>) {
    let state = Arc::new(AppState::new(config(origin, record_object_keys)).unwrap());
    state.start().await.unwrap();
    state.set_ready(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
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
    let mut builder = client.request(method, url);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    builder
}

fn trace_provider(ratio: f64) -> (SdkTracerProvider, InMemorySpanExporter) {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            ratio,
        ))))
        .build();
    (provider, exporter)
}

fn log_provider() -> (SdkLoggerProvider, InMemoryLogExporter) {
    let exporter = InMemoryLogExporter::default();
    let provider = SdkLoggerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    (provider, exporter)
}

fn trace_subscriber(
    provider: &SdkTracerProvider,
) -> impl tracing::Subscriber + Send + Sync + 'static {
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")))
}

fn trace_and_log_subscriber(
    tracer: &SdkTracerProvider,
    logger: &SdkLoggerProvider,
) -> impl tracing::Subscriber + Send + Sync + 'static {
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer.tracer("test")))
        .with(OpenTelemetryTracingBridge::new(logger))
}

fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes.iter().find_map(|attribute| {
        (attribute.key.as_str() == key).then(|| attribute.value.as_str().into_owned())
    })
}

fn set_propagator() {
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn signed_get_creates_server_and_upstream_spans() {
    set_propagator();
    let (provider, exporter) = trace_provider(1.0);
    let guard = tracing::subscriber::set_default(trace_subscriber(&provider));
    let (origin, _records) = spawn_origin().await;
    let (sidecar, _state) = spawn_sidecar(origin, false).await;

    let response = signed_request(
        &reqwest::Client::new(),
        Method::GET,
        &format!("{sidecar}/media/item"),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = response.text().await.unwrap();

    provider.force_flush().unwrap();
    drop(guard);
    let spans = exporter.get_finished_spans().unwrap();
    let server = spans
        .iter()
        .find(|span| span.name == "s3.request")
        .expect("the server span is missing");
    let upstream = spans
        .iter()
        .find(|span| span.name == "s3.upstream")
        .expect("the upstream span is missing");
    assert_eq!(
        attribute(server, "http.response.status_code").as_deref(),
        Some("200")
    );
    assert_eq!(
        attribute(server, "s3.operation").as_deref(),
        Some("GetObject")
    );
    assert_eq!(
        attribute(server, "http.request.method").as_deref(),
        Some("GET")
    );
    assert_eq!(
        attribute(upstream, "http.request.method").as_deref(),
        Some("GET")
    );
    assert_eq!(
        upstream.parent_span_id,
        server.span_context.span_id(),
        "the upstream span must be a child of the server span"
    );
    assert_eq!(
        upstream.span_context.trace_id(),
        server.span_context.trace_id()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn traceparent_sets_the_parent_and_is_injected_upstream() {
    set_propagator();
    let (provider, exporter) = trace_provider(1.0);
    let guard = tracing::subscriber::set_default(trace_subscriber(&provider));
    let (origin, records) = spawn_origin().await;
    let (sidecar, _state) = spawn_sidecar(origin, false).await;
    let traceparent = format!("00-{TRACE_ID_HEX}-{PARENT_ID_HEX}-01");

    let response = signed_request(
        &reqwest::Client::new(),
        Method::GET,
        &format!("{sidecar}/media/item"),
        &[("traceparent", &traceparent)],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = response.text().await.unwrap();

    let records = records.lock().unwrap();
    let record = records.first().expect("the origin received no request");
    assert_eq!(record.method, "GET");
    assert_eq!(record.path, "/media/item");
    let upstream = record
        .headers
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .expect("the origin did not receive traceparent")
        .to_owned();
    assert_eq!(
        upstream.split('-').nth(1),
        Some(TRACE_ID_HEX),
        "the origin received a different trace id"
    );

    provider.force_flush().unwrap();
    drop(guard);
    let spans = exporter.get_finished_spans().unwrap();
    let server = spans
        .iter()
        .find(|span| span.name == "s3.request")
        .expect("the server span is missing");
    assert_eq!(
        server.span_context.trace_id(),
        TraceId::from_hex(TRACE_ID_HEX).unwrap()
    );
    assert_eq!(
        server.parent_span_id,
        SpanId::from_hex(PARENT_ID_HEX).unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn sample_ratio_controls_root_spans() {
    let (off_provider, off_exporter) = trace_provider(0.0);
    {
        let _guard = tracing::subscriber::set_default(trace_subscriber(&off_provider));
        info_span!("s3.request").in_scope(|| {});
    }
    off_provider.force_flush().unwrap();
    assert!(
        off_exporter.get_finished_spans().unwrap().is_empty(),
        "a zero ratio produced a span"
    );

    let (on_provider, on_exporter) = trace_provider(1.0);
    {
        let _guard = tracing::subscriber::set_default(trace_subscriber(&on_provider));
        info_span!("s3.request").in_scope(|| {});
    }
    on_provider.force_flush().unwrap();
    assert_eq!(
        on_exporter.get_finished_spans().unwrap().len(),
        1,
        "a ratio of one did not produce a span"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn log_record_carries_the_span_trace_id() {
    set_propagator();
    let (tracer, _span_exporter) = trace_provider(1.0);
    let (logger, log_exporter) = log_provider();
    let guard = tracing::subscriber::set_default(trace_and_log_subscriber(&tracer, &logger));

    let span = info_span!("s3.request");
    let trace_id = span.context().span().span_context().trace_id();
    span.in_scope(|| {
        info!("inside the request span");
    });
    span.context().span().end();
    drop(span);

    logger.force_flush().unwrap();
    drop(guard);
    let logs = log_exporter.get_emitted_logs().unwrap();
    let record = logs
        .iter()
        .find(|log| log.record.body().is_some())
        .expect("the log record is missing");
    let trace_context = record
        .record
        .trace_context()
        .expect("the log record has no trace context");
    assert_eq!(trace_context.trace_id, trace_id);
}

#[tokio::test(flavor = "current_thread")]
async fn object_keys_are_omitted_by_default() {
    set_propagator();
    let (provider, exporter) = trace_provider(1.0);
    let guard = tracing::subscriber::set_default(trace_subscriber(&provider));
    let (origin, _records) = spawn_origin().await;
    let (sidecar, _state) = spawn_sidecar(origin, false).await;
    let response = signed_request(
        &reqwest::Client::new(),
        Method::GET,
        &format!("{sidecar}/media/private-key"),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = response.text().await.unwrap();

    provider.force_flush().unwrap();
    drop(guard);
    let spans = exporter.get_finished_spans().unwrap();
    let server = spans
        .iter()
        .find(|span| span.name == "s3.request")
        .expect("the server span is missing");
    assert!(attribute(server, "s3.object.key").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn object_keys_are_recorded_when_enabled() {
    set_propagator();
    let (provider, exporter) = trace_provider(1.0);
    let guard = tracing::subscriber::set_default(trace_subscriber(&provider));
    let (origin, _records) = spawn_origin().await;
    let (sidecar, _state) = spawn_sidecar(origin, true).await;
    let response = signed_request(
        &reqwest::Client::new(),
        Method::GET,
        &format!("{sidecar}/media/private-key"),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = response.text().await.unwrap();

    provider.force_flush().unwrap();
    drop(guard);
    let spans = exporter.get_finished_spans().unwrap();
    let server = spans
        .iter()
        .find(|span| span.name == "s3.request")
        .expect("the server span is missing");
    assert_eq!(
        attribute(server, "s3.object.key").as_deref(),
        Some("private-key")
    );
}

#[derive(Debug, Clone, Default)]
struct SpanSink(Arc<Mutex<Vec<SpanData>>>);

#[allow(clippy::unused_async_trait_impl)]
impl SpanExporter for SpanSink {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.lock().unwrap().extend(batch);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct LogSink(Arc<Mutex<Vec<SdkLogRecord>>>);

#[allow(clippy::unused_async_trait_impl)]
impl LogExporter for LogSink {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let mut records = self.0.lock().unwrap();
        records.extend(batch.iter().map(|(record, _scope)| record.clone()));
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_flushes_pending_spans_and_logs() {
    set_propagator();
    let spans = SpanSink::default();
    let logs = LogSink::default();
    let tracer_provider = SdkTracerProvider::builder()
        .with_span_processor(BatchSpanProcessor::builder(spans.clone()).build())
        .build();
    let logger_provider = SdkLoggerProvider::builder()
        .with_log_processor(BatchLogProcessor::builder(logs.clone()).build())
        .build();
    let tracer = tracer_provider.tracer("test");
    let telemetry = Telemetry::from_parts(
        None,
        Some(tracer_provider),
        Some(logger_provider.clone()),
        Duration::from_secs(2),
    );
    let state = Arc::new(
        AppState::new_with_metrics(
            config("http://127.0.0.1:1".to_owned(), false),
            Arc::new(Metrics::default()),
            None,
            Some(telemetry),
        )
        .unwrap(),
    );

    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(OpenTelemetryTracingBridge::new(&logger_provider));
    let guard = tracing::subscriber::set_default(subscriber);
    let span = info_span!("s3.request");
    span.in_scope(|| {
        info!("pending before shutdown");
    });
    span.context().span().end();
    drop(span);
    drop(guard);

    state.shutdown().await;

    assert!(
        !spans.0.lock().unwrap().is_empty(),
        "shutdown did not flush the pending span"
    );
    assert!(
        !logs.0.lock().unwrap().is_empty(),
        "shutdown did not flush the pending log"
    );
}

#[derive(Debug, Clone)]
struct Collected {
    path: String,
    body_len: usize,
}

#[derive(Clone, Default)]
struct Collector {
    requests: Arc<Mutex<Vec<Collected>>>,
}

impl Collector {
    fn requests(&self) -> Vec<Collected> {
        self.requests.lock().unwrap().clone()
    }
}

async fn collect(State(collector): State<Collector>, uri: Uri, body: Bytes) -> impl IntoResponse {
    collector.requests.lock().unwrap().push(Collected {
        path: uri.path().to_owned(),
        body_len: body.len(),
    });
    StatusCode::OK
}

async fn spawn_collector() -> (String, Collector) {
    let collector = Collector::default();
    let app = Router::new()
        .route("/v1/metrics", axum::routing::post(collect))
        .route("/v1/traces", axum::routing::post(collect))
        .route("/v1/logs", axum::routing::post(collect))
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
        metrics: false,
        traces: true,
        logs: true,
        endpoint: Some(endpoint),
        trace_endpoint: None,
        logs_endpoint: None,
        trace_sample_ratio: 1.0,
        protocol: Some(OtelProtocol::HttpProtobuf),
        headers: BTreeMap::new(),
        headers_file: None,
        timeout: HumanDuration::new(Duration::from_secs(2)),
        export_interval: None,
        trace_schedule_delay: HumanDuration::new(Duration::from_millis(10)),
        log_schedule_delay: HumanDuration::new(Duration::from_millis(10)),
        export_timeout: HumanDuration::new(Duration::from_secs(2)),
        resource_attributes: BTreeMap::from([(
            "deployment.environment.name".to_owned(),
            "test".to_owned(),
        )]),
        record_object_keys: false,
        ..OtelConfig::default()
    }
}

async fn wait_for_paths(collector: &Collector, paths: &[&str]) -> Vec<Collected> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let requests = collector.requests();
        if paths
            .iter()
            .all(|path| requests.iter().any(|request| request.path == *path))
            || tokio::time::Instant::now() >= deadline
        {
            return requests;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn otlp_http_receives_traces_and_logs() {
    set_propagator();
    let (endpoint, collector) = spawn_collector().await;
    let telemetry = Telemetry::new(&otel_config(endpoint), Some("node-a"))
        .unwrap()
        .unwrap();
    let tracer = telemetry.tracer().unwrap();
    let logger = telemetry.logger_provider().unwrap();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(OpenTelemetryTracingBridge::new(logger));
    let guard = tracing::subscriber::set_default(subscriber);

    let span = info_span!("s3.request");
    span.in_scope(|| {
        info!("exported event");
    });
    span.context().span().end();
    drop(span);
    drop(guard);
    telemetry.shutdown();

    let requests = wait_for_paths(&collector, &["/v1/traces", "/v1/logs"]).await;
    for path in ["/v1/traces", "/v1/logs"] {
        let request = requests
            .iter()
            .find(|request| request.path == path)
            .unwrap_or_else(|| panic!("the collector did not receive {path}"));
        assert!(request.body_len > 0, "the {path} body was empty");
    }
}

#[tokio::test]
async fn disabled_traces_and_logs_export_nothing() {
    set_propagator();
    let (endpoint, collector) = spawn_collector().await;
    let mut config = otel_config(endpoint);
    config.traces = false;
    config.logs = false;
    let telemetry = Telemetry::new(&config, Some("node-a")).unwrap().unwrap();
    assert!(telemetry.tracer().is_none());
    assert!(telemetry.logger_provider().is_none());
    info_span!("s3.request").in_scope(|| {
        info!("not exported");
    });
    telemetry.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(collector.requests().is_empty());
}
