//! Render the merged configuration with redacted secrets and field sources.

use std::fmt::Write as _;

use super::Config;
use super::loader::{Source, SourceMap};

// The table builder lists each field one time, in field order.
#[allow(clippy::too_many_lines)]
pub(crate) fn render(config: &Config, sources: &SourceMap) -> String {
    let mut rows: Vec<(String, String, String)> = Vec::new();

    push_row(&mut rows, sources, "mode", config.mode.to_string());
    push_row(
        &mut rows,
        sources,
        "server.bind",
        config.server.bind.clone(),
    );
    push_optional(
        &mut rows,
        sources,
        "server.tls.cert_file",
        config
            .server
            .tls
            .as_ref()
            .map_or_else(String::new, |tls| tls.cert_file.clone()),
    );
    push_optional(
        &mut rows,
        sources,
        "server.tls.key_file",
        config
            .server
            .tls
            .as_ref()
            .map_or_else(String::new, |tls| tls.key_file.clone()),
    );
    push_row(
        &mut rows,
        sources,
        "server.max_body_bytes",
        config.server.max_body_bytes.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "server.request_timeout",
        config.server.request_timeout.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "server.idle_timeout",
        config.server.idle_timeout.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "server.max_connections",
        config.server.max_connections.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "server.shutdown_timeout",
        config.server.shutdown_timeout.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "upstream.connect_timeout",
        config.upstream.connect_timeout.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "upstream.request_timeout",
        config.upstream.request_timeout.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "upstream.retry_attempts",
        config.upstream.retry_attempts.to_string(),
    );

    push_endpoint(&mut rows, sources, "origin", &config.origin);

    push_row(
        &mut rows,
        sources,
        "cache.endpoint",
        config.cache.endpoint.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.region",
        config.cache.region.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.access_key_id",
        config.cache.access_key_id.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.secret_access_key",
        config.cache.secret_access_key.redacted().to_owned(),
    );
    push_optional(
        &mut rows,
        sources,
        "cache.session_token",
        redact_option(config.cache.session_token.as_ref()),
    );
    push_row(
        &mut rows,
        sources,
        "cache.force_path_style",
        config.cache.force_path_style.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.bucket",
        config.cache.bucket.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.prefix",
        config.cache.prefix.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.node_id",
        config.cache.node_id.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.mode",
        config.cache.mode.to_string(),
    );
    push_optional(
        &mut rows,
        sources,
        "cache.max_bytes",
        match config.cache.max_bytes {
            Some(0) => "0 (unlimited)".to_owned(),
            Some(value) => value.to_string(),
            None => String::new(),
        },
    );
    push_row(
        &mut rows,
        sources,
        "cache.max_object_bytes",
        config.cache.max_object_bytes.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.write_policy",
        config.cache.write_policy.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.on_cache_error",
        config.cache.on_cache_error.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.high_watermark",
        config.cache.high_watermark.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.low_watermark",
        config.cache.low_watermark.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.staging_ttl",
        config.cache.staging_ttl.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.orphan_ttl",
        config.cache.orphan_ttl.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.sync_workers",
        config.cache.sync_workers.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.sync_max_attempts",
        config.cache.sync_max_attempts.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.sync_retry_base_ms",
        config.cache.sync_retry_base_ms.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.sync_debounce_ms",
        config.cache.sync_debounce_ms.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.gc_interval",
        config.cache.gc_interval.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.index_path",
        config.cache.index_path.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "cache.serve_quarantined",
        config.cache.serve_quarantined.to_string(),
    );

    push_endpoint(&mut rows, sources, "backing", &config.backing);
    push_row(
        &mut rows,
        sources,
        "backing.bucket",
        config.backing.bucket.clone(),
    );

    push_row(
        &mut rows,
        sources,
        "consensus.backend",
        config.consensus.backend.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.raft.node_id",
        config.consensus.raft.node_id.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.raft.peers",
        serde_json::to_string(&config.consensus.raft.peers).unwrap_or_else(|_| "[]".to_owned()),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.raft.bootstrap",
        config.consensus.raft.bootstrap.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.raft.secret",
        config.consensus.raft.secret.redacted().to_owned(),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.raft.rpc_timeout",
        config.consensus.raft.rpc_timeout.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.raft.nodes",
        serde_json::to_string(&config.consensus.raft.nodes).unwrap_or_else(|_| "[]".to_owned()),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.consul.node_id",
        config.consensus.consul.node_id.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.consul.address",
        config.consensus.consul.address.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.consul.advertise_addr",
        config.consensus.consul.advertise_addr.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.consul.token",
        config.consensus.consul.token.redacted().to_owned(),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.consul.session_ttl",
        config.consensus.consul.session_ttl.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "consensus.consul.secret",
        config.consensus.consul.secret.redacted().to_owned(),
    );
    push_row(
        &mut rows,
        sources,
        "cluster.staging_ttl",
        config.cluster.staging_ttl.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cluster.orphan_ttl",
        config.cluster.orphan_ttl.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cluster.gc_interval",
        config.cluster.gc_interval.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cluster.metadata_path",
        config.cluster.metadata_path.clone(),
    );
    push_row(
        &mut rows,
        sources,
        "cluster.secret",
        config.cluster.secret.redacted().to_owned(),
    );
    push_row(
        &mut rows,
        sources,
        "cluster.read_repair",
        config.cluster.read_repair.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cluster.replicate_workers",
        config.cluster.replicate_workers.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cluster.replicate_timeout",
        config.cluster.replicate_timeout.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cluster.reconcile_interval",
        config.cluster.reconcile_interval.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "cluster.nodes",
        serde_json::to_string(&config.cluster.nodes).unwrap_or_else(|_| "[]".to_owned()),
    );

    push_row(
        &mut rows,
        sources,
        "otel.enabled",
        config.otel.enabled.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "otel.metrics",
        config.otel.metrics.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "otel.traces",
        config.otel.traces.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "otel.logs",
        config.otel.logs.to_string(),
    );
    push_optional(
        &mut rows,
        sources,
        "otel.service_name",
        config.otel.service_name.clone().unwrap_or_default(),
    );
    push_row(
        &mut rows,
        sources,
        "otel.service_version",
        config.otel.service_version.clone(),
    );
    push_optional(
        &mut rows,
        sources,
        "otel.endpoint",
        config.otel.endpoint.clone().unwrap_or_default(),
    );
    push_optional(
        &mut rows,
        sources,
        "otel.trace_endpoint",
        config.otel.trace_endpoint.clone().unwrap_or_default(),
    );
    push_optional(
        &mut rows,
        sources,
        "otel.logs_endpoint",
        config.otel.logs_endpoint.clone().unwrap_or_default(),
    );
    push_row(
        &mut rows,
        sources,
        "otel.trace_sample_ratio",
        config.otel.trace_sample_ratio.to_string(),
    );
    push_optional(
        &mut rows,
        sources,
        "otel.protocol",
        config
            .otel
            .protocol
            .map_or_else(String::new, |value| value.to_string()),
    );
    push_row(
        &mut rows,
        sources,
        "otel.headers",
        redacted_headers(&config.otel.headers),
    );
    push_optional(
        &mut rows,
        sources,
        "otel.headers_file",
        config
            .otel
            .headers_file
            .as_ref()
            .map_or_else(String::new, |path| path.display().to_string()),
    );
    push_row(
        &mut rows,
        sources,
        "otel.timeout",
        config.otel.timeout.to_string(),
    );
    push_optional(
        &mut rows,
        sources,
        "otel.export_interval",
        config
            .otel
            .export_interval
            .map_or_else(String::new, |value| value.to_string()),
    );
    push_row(
        &mut rows,
        sources,
        "otel.trace_schedule_delay",
        config.otel.trace_schedule_delay.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "otel.log_schedule_delay",
        config.otel.log_schedule_delay.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "otel.export_timeout",
        config.otel.export_timeout.to_string(),
    );
    push_row(
        &mut rows,
        sources,
        "otel.resource_attributes",
        serde_json::to_string(&config.otel.resource_attributes).unwrap_or_else(|_| "{}".to_owned()),
    );
    push_row(
        &mut rows,
        sources,
        "otel.record_object_keys",
        config.otel.record_object_keys.to_string(),
    );

    if config.clients.is_empty() {
        push_row(&mut rows, sources, "clients", "[]".to_owned());
    } else {
        for (index, client) in config.clients.iter().enumerate() {
            let base = format!("clients[{index}]");
            push_row(
                &mut rows,
                sources,
                &format!("{base}.access_key_id"),
                client.access_key_id.clone(),
            );
            push_row(
                &mut rows,
                sources,
                &format!("{base}.secret_access_key"),
                client.secret_access_key.redacted().to_owned(),
            );
            push_optional(
                &mut rows,
                sources,
                &format!("{base}.session_token"),
                redact_option(client.session_token.as_ref()),
            );
            push_row(
                &mut rows,
                sources,
                &format!("{base}.buckets"),
                serde_json::to_string(&client.buckets).unwrap_or_else(|_| "[]".to_owned()),
            );
        }
    }

    if config.buckets.is_empty() {
        push_row(&mut rows, sources, "buckets", "[]".to_owned());
    } else {
        for (index, bucket) in config.buckets.iter().enumerate() {
            let base = format!("buckets[{index}]");
            push_row(
                &mut rows,
                sources,
                &format!("{base}.name"),
                bucket.name.clone(),
            );
            push_optional(
                &mut rows,
                sources,
                &format!("{base}.ttl"),
                bucket.ttl.map_or_else(String::new, |ttl| ttl.to_string()),
            );
            push_optional(
                &mut rows,
                sources,
                &format!("{base}.cache_mode"),
                bucket
                    .cache_mode
                    .map_or_else(String::new, |value| value.to_string()),
            );
            push_optional(
                &mut rows,
                sources,
                &format!("{base}.cache_write_policy"),
                bucket
                    .cache_write_policy
                    .map_or_else(String::new, |value| value.to_string()),
            );
            push_optional(
                &mut rows,
                sources,
                &format!("{base}.write_ack"),
                bucket
                    .write_ack
                    .map_or_else(String::new, |value| value.to_string()),
            );
            push_optional(
                &mut rows,
                sources,
                &format!("{base}.read"),
                bucket
                    .read
                    .map_or_else(String::new, |value| value.to_string()),
            );
            push_optional(
                &mut rows,
                sources,
                &format!("{base}.replicas"),
                bucket
                    .replicas
                    .map_or_else(String::new, |value| value.to_string()),
            );
            push_optional(
                &mut rows,
                sources,
                &format!("{base}.min_replicas"),
                bucket
                    .min_replicas
                    .map_or_else(String::new, |value| value.to_string()),
            );
        }
    }

    let field_width = rows
        .iter()
        .map(|(field, _, _)| field.len())
        .max()
        .unwrap_or(4)
        .max("field".len());
    let value_width = rows
        .iter()
        .map(|(_, value, _)| value.len())
        .max()
        .unwrap_or(5)
        .max("value".len());

    let mut output = String::new();
    let _ = writeln!(
        output,
        "{:<field_width$}  {:<value_width$}  source",
        "field", "value"
    );
    for (field, value, source) in rows {
        let _ = writeln!(
            output,
            "{field:<field_width$}  {value:<value_width$}  {source}"
        );
    }
    output
}

fn push_endpoint(
    rows: &mut Vec<(String, String, String)>,
    sources: &SourceMap,
    prefix: &str,
    endpoint: &super::EndpointConfig,
) {
    push_row(
        rows,
        sources,
        &format!("{prefix}.endpoint"),
        endpoint.endpoint.clone(),
    );
    push_row(
        rows,
        sources,
        &format!("{prefix}.region"),
        endpoint.region.clone(),
    );
    push_row(
        rows,
        sources,
        &format!("{prefix}.access_key_id"),
        endpoint.access_key_id.clone(),
    );
    push_row(
        rows,
        sources,
        &format!("{prefix}.secret_access_key"),
        endpoint.secret_access_key.redacted().to_owned(),
    );
    push_optional(
        rows,
        sources,
        &format!("{prefix}.session_token"),
        redact_option(endpoint.session_token.as_ref()),
    );
    push_row(
        rows,
        sources,
        &format!("{prefix}.force_path_style"),
        endpoint.force_path_style.to_string(),
    );
}

fn push_row(
    rows: &mut Vec<(String, String, String)>,
    sources: &SourceMap,
    field: &str,
    value: String,
) {
    push_value(rows, sources, field, value, false);
}

fn push_optional(
    rows: &mut Vec<(String, String, String)>,
    sources: &SourceMap,
    field: &str,
    value: String,
) {
    push_value(rows, sources, field, value, true);
}

fn push_value(
    rows: &mut Vec<(String, String, String)>,
    sources: &SourceMap,
    field: &str,
    value: String,
    optional: bool,
) {
    let source = source_at(sources, field);
    let (value, source) = match source {
        Some(source) => (value, source.to_string()),
        None if optional => ("(unset)".to_owned(), "unset".to_owned()),
        None => (value, Source::Default.to_string()),
    };
    rows.push((field.to_owned(), value, source));
}

fn source_at(sources: &SourceMap, field: &str) -> Option<Source> {
    sources.get(field).copied()
}

fn redact_option(value: Option<&super::secret::SecretString>) -> String {
    value.map_or_else(String::new, |secret| secret.redacted().to_owned())
}

fn redacted_headers(
    headers: &std::collections::BTreeMap<String, super::secret::SecretString>,
) -> String {
    let redacted: std::collections::BTreeMap<&str, &str> = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.redacted()))
        .collect();
    serde_json::to_string(&redacted).unwrap_or_else(|_| "{}".to_owned())
}
