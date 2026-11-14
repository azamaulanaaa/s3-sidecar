//! OpenTelemetry providers and OTLP export for metrics, traces, and logs.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use opentelemetry::Context;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::logs::LoggerProvider as _;
use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{
    LogExporter, MetricExporter, Protocol, SpanExporter, WithExportConfig, WithHttpConfig,
    WithTonicConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{BatchLogProcessor, SdkLoggerProvider};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{BatchSpanProcessor, Sampler, SdkTracerProvider};
use opentelemetry_semantic_conventions::attribute::{SERVICE_INSTANCE_ID, SERVICE_VERSION};
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};
use tracing::warn;

use crate::config::secret::SecretString;
use crate::config::{OtelConfig, OtelProtocol};
use crate::errors::S3Error;

/// The process-wide OpenTelemetry providers.
pub struct Telemetry {
    meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    export_timeout: Duration,
}

impl Telemetry {
    /// Build the enabled providers.
    ///
    /// # Errors
    ///
    /// Returns an error when an OTLP exporter cannot use the configuration.
    pub fn new(config: &OtelConfig, node_id: Option<&str>) -> Result<Option<Self>, S3Error> {
        if !config.enabled {
            return Ok(None);
        }
        let resource = build_resource(config, node_id);
        let meter_provider = if config.metrics {
            Some(build_meter_provider(config, &resource).map_err(|error| exporter_error(&error))?)
        } else {
            None
        };
        let tracer_provider = if config.traces {
            Some(build_tracer_provider(config, &resource).map_err(|error| exporter_error(&error))?)
        } else {
            None
        };
        let logger_provider = if config.logs {
            Some(build_logger_provider(config, &resource).map_err(|error| exporter_error(&error))?)
        } else {
            None
        };
        Ok(Some(Self {
            meter_provider,
            tracer_provider,
            logger_provider,
            export_timeout: config.export_timeout.as_duration(),
        }))
    }

    /// Build a telemetry value from existing providers. For tests only.
    #[doc(hidden)]
    #[must_use]
    pub fn from_parts(
        meter_provider: Option<SdkMeterProvider>,
        tracer_provider: Option<SdkTracerProvider>,
        logger_provider: Option<SdkLoggerProvider>,
        export_timeout: Duration,
    ) -> Self {
        Self {
            meter_provider,
            tracer_provider,
            logger_provider,
            export_timeout,
        }
    }

    /// Return the meter for the sidecar instruments.
    #[must_use]
    pub fn meter(&self) -> Meter {
        self.meter_provider.as_ref().map_or_else(
            || opentelemetry::global::meter("s3-sidecar-noop"),
            |provider| provider.meter("s3-sidecar"),
        )
    }

    /// Return the tracer for the tracing subscriber, when traces are enabled.
    #[must_use]
    pub fn tracer(&self) -> Option<opentelemetry_sdk::trace::SdkTracer> {
        self.tracer_provider
            .as_ref()
            .map(|provider| provider.tracer("s3-sidecar"))
    }

    /// Return the logger provider, when logs are enabled.
    #[must_use]
    pub fn logger_provider(&self) -> Option<&SdkLoggerProvider> {
        self.logger_provider.as_ref()
    }

    /// Return the logger for explicit log records, when logs are enabled.
    #[must_use]
    pub fn logger(&self) -> Option<opentelemetry_sdk::logs::SdkLogger> {
        self.logger_provider
            .as_ref()
            .map(|provider| provider.logger("s3-sidecar"))
    }

    /// Flush pending signals and stop every exporter.
    pub fn shutdown(&self) {
        if let Some(provider) = &self.tracer_provider {
            if let Err(error) = provider.force_flush() {
                warn!(%error, "trace flush failed");
            }
            if let Err(error) = provider.shutdown_with_timeout(self.export_timeout) {
                warn!(%error, "trace shutdown failed");
            }
        }
        if let Some(provider) = &self.logger_provider {
            if let Err(error) = provider.force_flush() {
                warn!(%error, "log flush failed");
            }
            if let Err(error) = provider.shutdown_with_timeout(self.export_timeout) {
                warn!(%error, "log shutdown failed");
            }
        }
        if let Some(provider) = &self.meter_provider {
            if let Err(error) = provider.force_flush() {
                warn!(%error, "metrics flush failed");
            }
            if let Err(error) = provider.shutdown_with_timeout(self.export_timeout) {
                warn!(%error, "metrics shutdown failed");
            }
        }
    }
}

fn exporter_error(error: &str) -> S3Error {
    S3Error::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalError",
        &format!("cannot build the OTLP exporter: {error}"),
    )
}

fn build_meter_provider(
    config: &OtelConfig,
    resource: &Resource,
) -> Result<SdkMeterProvider, String> {
    let exporter = build_metric_exporter(config)?;
    let mut reader = PeriodicReader::builder(exporter);
    if let Some(interval) = config.export_interval {
        reader = reader.with_interval(interval.as_duration());
    }
    let reader = reader.build();
    Ok(SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource.clone())
        .build())
}

fn build_tracer_provider(
    config: &OtelConfig,
    resource: &Resource,
) -> Result<SdkTracerProvider, String> {
    let exporter = build_span_exporter(config)?;
    let batch = opentelemetry_sdk::trace::BatchConfigBuilder::default()
        .with_scheduled_delay(config.trace_schedule_delay.as_duration())
        .build();
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(batch)
        .build();
    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
        config.trace_sample_ratio,
    )));
    Ok(SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_sampler(sampler)
        .with_resource(resource.clone())
        .build())
}

fn build_logger_provider(
    config: &OtelConfig,
    resource: &Resource,
) -> Result<SdkLoggerProvider, String> {
    let exporter = build_log_exporter(config)?;
    let batch = opentelemetry_sdk::logs::BatchConfigBuilder::default()
        .with_scheduled_delay(config.log_schedule_delay.as_duration())
        .build();
    let processor: BatchLogProcessor = BatchLogProcessor::builder(exporter)
        .with_batch_config(batch)
        .build();
    Ok(SdkLoggerProvider::builder()
        .with_log_processor(processor)
        .with_resource(resource.clone())
        .build())
}

fn build_metric_exporter(config: &OtelConfig) -> Result<MetricExporter, String> {
    let endpoint = config
        .endpoint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match resolve_protocol(config) {
        OtelProtocol::Grpc => {
            let mut builder = MetricExporter::builder().with_tonic();
            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if !config.headers.is_empty() {
                builder = builder.with_metadata(metadata_map(&config.headers)?);
            }
            builder
                .with_timeout(config.timeout.as_duration())
                .build()
                .map_err(|error| error.to_string())
        }
        OtelProtocol::HttpProtobuf => {
            let mut builder = MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary);
            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(http_endpoint(endpoint, "/v1/metrics"));
            }
            if !config.headers.is_empty() {
                builder = builder.with_headers(header_map(&config.headers));
            }
            builder
                .with_timeout(config.timeout.as_duration())
                .build()
                .map_err(|error| error.to_string())
        }
    }
}

fn build_span_exporter(config: &OtelConfig) -> Result<SpanExporter, String> {
    let endpoint = resolve_signal_endpoint(
        config.trace_endpoint.as_deref(),
        config.endpoint.as_deref(),
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    );
    match resolve_protocol(config) {
        OtelProtocol::Grpc => {
            let mut builder = SpanExporter::builder().with_tonic();
            if let Some(endpoint) = &endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if !config.headers.is_empty() {
                builder = builder.with_metadata(metadata_map(&config.headers)?);
            }
            builder
                .with_timeout(config.timeout.as_duration())
                .build()
                .map_err(|error| error.to_string())
        }
        OtelProtocol::HttpProtobuf => {
            let mut builder = SpanExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary);
            if let Some(endpoint) = &endpoint {
                builder = builder.with_endpoint(http_endpoint(endpoint, "/v1/traces"));
            }
            if !config.headers.is_empty() {
                builder = builder.with_headers(header_map(&config.headers));
            }
            builder
                .with_timeout(config.timeout.as_duration())
                .build()
                .map_err(|error| error.to_string())
        }
    }
}

fn build_log_exporter(config: &OtelConfig) -> Result<LogExporter, String> {
    let endpoint = resolve_signal_endpoint(
        config.logs_endpoint.as_deref(),
        config.endpoint.as_deref(),
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
    );
    match resolve_protocol(config) {
        OtelProtocol::Grpc => {
            let mut builder = LogExporter::builder().with_tonic();
            if let Some(endpoint) = &endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if !config.headers.is_empty() {
                builder = builder.with_metadata(metadata_map(&config.headers)?);
            }
            builder
                .with_timeout(config.timeout.as_duration())
                .build()
                .map_err(|error| error.to_string())
        }
        OtelProtocol::HttpProtobuf => {
            let mut builder = LogExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary);
            if let Some(endpoint) = &endpoint {
                builder = builder.with_endpoint(http_endpoint(endpoint, "/v1/logs"));
            }
            if !config.headers.is_empty() {
                builder = builder.with_headers(header_map(&config.headers));
            }
            builder
                .with_timeout(config.timeout.as_duration())
                .build()
                .map_err(|error| error.to_string())
        }
    }
}

fn resolve_signal_endpoint(
    specific: Option<&str>,
    common: Option<&str>,
    env_name: &str,
) -> Option<String> {
    [specific, common]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            std::env::var(env_name)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
}

fn http_endpoint(endpoint: &str, path: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.ends_with(path) {
        endpoint.to_owned()
    } else {
        format!("{endpoint}{path}")
    }
}

fn resolve_protocol(config: &OtelConfig) -> OtelProtocol {
    if let Some(protocol) = config.protocol {
        return protocol;
    }
    if let Ok(value) = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        && let Some(protocol) = OtelProtocol::parse(&value)
    {
        return protocol;
    }
    OtelProtocol::Grpc
}

fn metadata_map(headers: &BTreeMap<String, SecretString>) -> Result<MetadataMap, String> {
    let mut metadata = MetadataMap::new();
    for (name, value) in headers {
        let key: MetadataKey<Ascii> = name
            .parse()
            .map_err(|_| format!("OTLP header name \"{name}\" is not valid for gRPC"))?;
        let value: MetadataValue<Ascii> = value
            .expose_secret()
            .parse()
            .map_err(|_| format!("OTLP header value for \"{name}\" is not valid for gRPC"))?;
        metadata.insert(key, value);
    }
    Ok(metadata)
}

fn header_map(headers: &BTreeMap<String, SecretString>) -> HashMap<String, String> {
    headers
        .iter()
        .map(|(name, value)| (name.clone(), value.expose_secret().to_owned()))
        .collect()
}

fn build_resource(config: &OtelConfig, node_id: Option<&str>) -> Resource {
    let mut builder = Resource::builder()
        .with_service_name(resolve_service_name(config))
        .with_attribute(KeyValue::new(
            SERVICE_VERSION,
            config.service_version.clone(),
        ))
        .with_attribute(KeyValue::new(SERVICE_INSTANCE_ID, instance_id(node_id)));
    for (key, value) in &config.resource_attributes {
        builder = builder.with_attribute(KeyValue::new(key.clone(), value.clone()));
    }
    builder.build()
}

fn resolve_service_name(config: &OtelConfig) -> String {
    if let Some(name) = config
        .service_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return name.to_owned();
    }
    if let Ok(name) = std::env::var("OTEL_SERVICE_NAME")
        && !name.trim().is_empty()
    {
        return name;
    }
    SERVICE_NAME_DEFAULT.to_owned()
}

const SERVICE_NAME_DEFAULT: &str = "s3-sidecar";

fn instance_id(node_id: Option<&str>) -> String {
    node_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(HeaderName::as_str).collect()
    }
}

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(key.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, value);
        }
    }
}

/// Extract the W3C trace context from request headers.
#[must_use]
pub fn extract_context(headers: &HeaderMap) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(headers)))
}

/// Inject the current trace context into outbound request headers.
pub fn inject_context(headers: &mut HeaderMap) {
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&Context::current(), &mut HeaderInjector(headers));
    });
}

/// Return a copy of the headers with the current trace context added.
#[must_use]
pub fn injected_headers(headers: HeaderMap) -> HeaderMap {
    let mut headers = headers;
    inject_context(&mut headers);
    headers
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn disabled_config_builds_no_provider() {
        let config = OtelConfig::default();
        assert!(Telemetry::new(&config, None).unwrap().is_none());
    }

    #[test]
    fn protocol_defaults_to_grpc() {
        let config = OtelConfig::default();
        assert_eq!(resolve_protocol(&config), OtelProtocol::Grpc);
    }

    #[tokio::test]
    async fn grpc_exporters_build() {
        let config = OtelConfig {
            enabled: true,
            metrics: true,
            traces: true,
            logs: true,
            protocol: Some(OtelProtocol::Grpc),
            endpoint: Some("http://127.0.0.1:4317".to_owned()),
            ..OtelConfig::default()
        };
        assert!(build_metric_exporter(&config).is_ok());
        assert!(build_span_exporter(&config).is_ok());
        assert!(build_log_exporter(&config).is_ok());
    }

    #[test]
    fn http_exporters_build() {
        let config = OtelConfig {
            enabled: true,
            metrics: true,
            traces: true,
            logs: true,
            protocol: Some(OtelProtocol::HttpProtobuf),
            endpoint: Some("http://127.0.0.1:4318".to_owned()),
            ..OtelConfig::default()
        };
        assert!(build_metric_exporter(&config).is_ok());
        assert!(build_span_exporter(&config).is_ok());
        assert!(build_log_exporter(&config).is_ok());
    }

    #[test]
    fn http_endpoint_replaces_the_signal_path() {
        assert_eq!(
            http_endpoint("http://127.0.0.1:4318", "/v1/traces"),
            "http://127.0.0.1:4318/v1/traces"
        );
        assert_eq!(
            http_endpoint("http://127.0.0.1:4318/v1/traces", "/v1/traces"),
            "http://127.0.0.1:4318/v1/traces"
        );
    }

    #[test]
    fn instance_id_uses_the_node_id() {
        assert_eq!(instance_id(Some("node-a")), "node-a");
    }
}
