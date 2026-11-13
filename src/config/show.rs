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
