//! OpenTelemetry meter provider and OTLP metrics export.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use axum::http::StatusCode;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry_otlp::{
    MetricExporter, Protocol, WithExportConfig, WithHttpConfig, WithTonicConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_semantic_conventions::attribute::{SERVICE_INSTANCE_ID, SERVICE_VERSION};
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};
use tracing::warn;

use crate::config::secret::SecretString;
use crate::config::{OtelConfig, OtelProtocol};
use crate::errors::S3Error;

/// The process-wide OpenTelemetry meter provider.
pub struct Telemetry {
    provider: SdkMeterProvider,
    export_timeout: Duration,
}

impl Telemetry {
    /// Build the meter provider when metrics export is enabled.
    ///
    /// # Errors
    ///
    /// Returns an error when the OTLP exporter cannot use the configuration.
    pub fn new(config: &OtelConfig, node_id: Option<&str>) -> Result<Option<Self>, S3Error> {
        if !config.enabled {
            return Ok(None);
        }
        let exporter = build_exporter(config).map_err(|error| {
            S3Error::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("cannot build the OTLP metrics exporter: {error}"),
            )
        })?;
        let mut reader = PeriodicReader::builder(exporter);
        if let Some(interval) = config.export_interval {
            reader = reader.with_interval(interval.as_duration());
        }
        let reader = reader.build();
        let resource = build_resource(config, node_id);
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();
        Ok(Some(Self {
            provider,
            export_timeout: config.export_timeout.as_duration(),
        }))
    }

    /// Return the meter for the sidecar instruments.
    #[must_use]
    pub fn meter(&self) -> Meter {
        self.provider.meter("s3-sidecar")
    }

    /// Flush pending metrics and stop the exporter.
    pub fn shutdown(&self) {
        if let Err(error) = self.provider.force_flush() {
            warn!(%error, "metrics flush failed");
        }
        if let Err(error) = self.provider.shutdown_with_timeout(self.export_timeout) {
            warn!(%error, "metrics shutdown failed");
        }
    }
}

fn build_exporter(config: &OtelConfig) -> Result<MetricExporter, String> {
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
                builder = builder.with_endpoint(http_metrics_endpoint(endpoint));
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

fn http_metrics_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.ends_with("/v1/metrics") {
        endpoint.to_owned()
    } else {
        format!("{endpoint}/v1/metrics")
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
    async fn grpc_exporter_builds() {
        let config = OtelConfig {
            enabled: true,
            protocol: Some(OtelProtocol::Grpc),
            endpoint: Some("http://127.0.0.1:4317".to_owned()),
            ..OtelConfig::default()
        };
        assert!(build_exporter(&config).is_ok());
    }

    #[test]
    fn http_exporter_builds() {
        let config = OtelConfig {
            enabled: true,
            protocol: Some(OtelProtocol::HttpProtobuf),
            endpoint: Some("http://127.0.0.1:4318".to_owned()),
            ..OtelConfig::default()
        };
        assert!(build_exporter(&config).is_ok());
    }

    #[test]
    fn instance_id_uses_the_node_id() {
        assert_eq!(instance_id(Some("node-a")), "node-a");
    }
}
