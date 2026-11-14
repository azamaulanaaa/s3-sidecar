//! Tracing subscriber setup.

use opentelemetry::global;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::OtelConfig;
use crate::telemetry::Telemetry;

/// Install the stderr formatter. This does not start OpenTelemetry.
pub fn init() {
    let _ = install(&OtelConfig::default(), None);
}

/// Install one subscriber with the stderr formatter and the enabled OTLP layers.
///
/// # Errors
///
/// Returns an error when a subscriber is already installed.
pub fn init_with(config: &OtelConfig, telemetry: Option<&Telemetry>) -> Result<(), String> {
    install(config, telemetry).map_err(|error| format!("cannot install tracing: {error}"))
}

fn install(
    config: &OtelConfig,
    telemetry: Option<&Telemetry>,
) -> Result<(), tracing_subscriber::util::TryInitError> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let filter = std::env::var("S3_SIDECAR_LOG")
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "info".to_owned());
    let filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));

    let trace_layer = if config.traces {
        telemetry
            .and_then(Telemetry::tracer)
            .map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer))
    } else {
        None
    };
    let log_layer = if config.logs {
        telemetry
            .and_then(Telemetry::logger_provider)
            .map(OpenTelemetryTracingBridge::new)
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_writer(std::io::stderr),
        )
        .with(trace_layer)
        .with(log_layer)
        .try_init()
}
