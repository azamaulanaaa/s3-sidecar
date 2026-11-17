//! A small load generator for the sidecar.
//!
//! The generator sends signed S3 requests to one endpoint and reports latency
//! and throughput. It reuses the `SigV4` signer from the crate. Run it with:
//!
//! ```text
//! cargo run --release --example loadgen -- --help
//! ```

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, HeaderValue, header};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use reqwest::{Client, Method, Url};
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, EMPTY_SHA256, now, sha256_hex, uri_encode};
use s3_sidecar::config::secret::SecretString;

/// Request pattern for one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// Send PUT requests.
    Put,
    /// Send GET requests.
    Get,
    /// Send PUT and GET requests in equal shares.
    Mixed,
}

/// Command-line arguments.
#[derive(Debug, Parser)]
#[command(name = "loadgen", about = "Send signed S3 requests to one endpoint.")]
struct Args {
    /// Base URL of the sidecar or S3 endpoint.
    #[arg(long, default_value = "http://127.0.0.1:9000")]
    endpoint: String,
    /// Bucket name.
    #[arg(long, default_value = "bench")]
    bucket: String,
    /// Key prefix. The request index is appended.
    #[arg(long, default_value = "bench/object")]
    key: String,
    /// Total number of requests.
    #[arg(long, default_value_t = 1000)]
    requests: usize,
    /// Number of concurrent workers.
    #[arg(long, default_value_t = 16)]
    concurrency: usize,
    /// Body size in bytes for a PUT.
    #[arg(long, default_value_t = 1024)]
    size: usize,
    /// Request pattern.
    #[arg(long, value_enum, default_value = "mixed")]
    mode: Mode,
    /// Access key ID.
    #[arg(long, default_value = "minioadmin")]
    access_key: String,
    /// Secret access key.
    #[arg(long, default_value = "minioadmin")]
    secret_key: String,
    /// `SigV4` region.
    #[arg(long, default_value = "us-east-1")]
    region: String,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("loadgen: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), String> {
    if args.requests == 0 {
        return Err("--requests must be greater than 0".to_owned());
    }
    if args.concurrency == 0 {
        return Err("--concurrency must be greater than 0".to_owned());
    }
    if args.mode != Mode::Get && args.size == 0 {
        return Err("--size must be greater than 0 for a PUT".to_owned());
    }

    let client = Client::builder()
        .pool_max_idle_per_host(args.concurrency)
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|error| format!("cannot build the HTTP client: {error}"))?;
    let args = Arc::new(args);
    let credential = Arc::new(ClientCredential::new(
        args.access_key.clone(),
        SecretString::new(args.secret_key.clone()),
        None,
        vec![args.bucket.clone()],
    ));
    let body = Bytes::from(vec![b'x'; args.size]);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();

    let mut handles = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        let client = client.clone();
        let args = Arc::clone(&args);
        let credential = Arc::clone(&credential);
        let body = body.clone();
        let next = Arc::clone(&next);
        handles.push(tokio::spawn(async move {
            let mut results = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= args.requests {
                    break;
                }
                let method = request_method(args.mode, index);
                let key = format!("{}-{index}", args.key);
                results.push(send(&client, &args, &credential, &body, method, &key).await);
            }
            results
        }));
    }

    let mut latencies = Vec::with_capacity(args.requests);
    let mut errors = 0_usize;
    for handle in handles {
        match handle.await {
            Ok(results) => {
                for (latency, failed) in results {
                    latencies.push(latency);
                    if failed {
                        errors += 1;
                    }
                }
            }
            Err(error) => {
                eprintln!("loadgen: a worker stopped: {error}");
                errors += 1;
            }
        }
    }

    let elapsed = started.elapsed();
    report(&args, &mut latencies, errors, elapsed);
    if latencies.is_empty() {
        return Err("no request completed".to_owned());
    }
    Ok(())
}

fn request_method(mode: Mode, index: usize) -> Method {
    match mode {
        Mode::Put => Method::PUT,
        Mode::Get => Method::GET,
        Mode::Mixed => {
            if index.is_multiple_of(2) {
                Method::PUT
            } else {
                Method::GET
            }
        }
    }
}

async fn send(
    client: &Client,
    args: &Args,
    credential: &ClientCredential,
    body: &Bytes,
    method: Method,
    key: &str,
) -> (u64, bool) {
    let started = Instant::now();
    let Ok(url) = object_url(&args.endpoint, &args.bucket, key) else {
        return (0, true);
    };

    let payload_hash = if method == Method::PUT {
        sha256_hex(body)
    } else {
        EMPTY_SHA256.to_owned()
    };
    let mut headers = HeaderMap::new();
    if method == Method::PUT {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
    }
    if sign_request(
        &method,
        &url,
        &mut headers,
        &payload_hash,
        credential,
        &args.region,
        now(),
    )
    .is_err()
    {
        return (elapsed_micros(started), true);
    }

    let mut request = client.request(method, url).headers(headers);
    if payload_hash != EMPTY_SHA256 {
        request = request.body(body.clone());
    }

    let failed = match request.send().await {
        Ok(response) => {
            let status = response.status();
            let _ = response.bytes().await;
            !status.is_success()
        }
        Err(_) => true,
    };
    (elapsed_micros(started), failed)
}

fn object_url(endpoint: &str, bucket: &str, key: &str) -> Result<Url, String> {
    let base = endpoint.trim_end_matches('/');
    let key: Vec<String> = key.split('/').map(uri_encode).collect();
    Url::parse(&format!("{base}/{}/{}", uri_encode(bucket), key.join("/")))
        .map_err(|error| format!("invalid endpoint URL: {error}"))
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "the report converts the request count to f64 for throughput"
)]
fn report(args: &Args, latencies: &mut [u64], errors: usize, elapsed: Duration) {
    latencies.sort_unstable();
    let completed = latencies.len();
    let succeeded = completed.saturating_sub(errors);
    let seconds = elapsed.as_secs_f64();
    let throughput = if seconds == 0.0 {
        0.0
    } else {
        succeeded as f64 / seconds
    };

    println!("endpoint: {}", args.endpoint);
    println!("bucket: {}", args.bucket);
    println!("mode: {:?}", args.mode);
    println!("concurrency: {}", args.concurrency);
    println!("size_bytes: {}", args.size);
    println!("requests: {}", args.requests);
    println!("completed: {completed}");
    println!("errors: {errors}");
    println!("duration_seconds: {seconds:.3}");
    println!("throughput_ops_per_second: {throughput:.2}");
    println!("p50_ms: {:.3}", micros_to_ms(percentile(latencies, 50)));
    println!("p95_ms: {:.3}", micros_to_ms(percentile(latencies, 95)));
    println!("p99_ms: {:.3}", micros_to_ms(percentile(latencies, 99)));
}

fn percentile(sorted: &[u64], percent: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (sorted.len() * percent).div_ceil(100).max(1);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[expect(
    clippy::cast_precision_loss,
    reason = "the converter divides microseconds by 1000.0"
)]
fn micros_to_ms(micros: u64) -> f64 {
    micros as f64 / 1000.0
}
