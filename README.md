# s3-sidecar

An S3 sidecar proxy in Rust.

This build implements milestone M0, the proxy core:

- Layered configuration: defaults, TOML file, environment, and flags.
- SigV4 authentication: header auth and presigned query auth.
- Byte-preserving pass-through to one origin S3 endpoint.
- Object, list, multipart, and copy operations.
- Health and readiness endpoints.
- `s3-sidecar check --show-config` with redacted secrets and field sources.

Cache mode and cluster mode are not in this build. The sidecar rejects them at
startup and names the missing feature.

## Build and test

This project uses [mise](https://mise.jdx.dev/) to manage tools and tasks.

```bash
mise install
mise run build
mise run test
mise run fmt
mise run lint
```

The full local gates are:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo doc --no-deps
cargo deny check
```

## Run

Write a configuration file. See [`sidecar.example.toml`](sidecar.example.toml)
for every field.

```toml
mode = "cache"

[server]
bind = "127.0.0.1:9002"

[origin]
endpoint = "http://127.0.0.1:9001"
region = "us-east-1"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"

[cache]
mode = "off"

[[clients]]
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
buckets = ["media"]
```

Check the merged configuration:

```bash
s3-sidecar check --config sidecar.toml --show-config
```

Start the proxy:

```bash
s3-sidecar serve --config sidecar.toml
```

Point an S3 client at the sidecar. The client access key must be in the
`[[clients]]` list, and the bucket must be in that client's `buckets` list.

```bash
aws --endpoint-url http://127.0.0.1:9002 s3 ls s3://media/
mc alias set sidecar http://127.0.0.1:9002 minioadmin minioadmin
mc ls sidecar/media
```

## Configuration

Precedence from highest to lowest:

1. CLI flag
2. Environment variable
3. TOML configuration file
4. Built-in default

Environment names use the `S3_SIDECAR_` prefix and `__` between levels.
Names are case-insensitive.

```bash
S3_SIDECAR_SERVER__BIND=0.0.0.0:9001
S3_SIDECAR_BUCKETS='[{"name":"media","ttl":"1h"}]'
```

Flags use the TOML path in kebab-case. Arrays and tables take JSON.

```bash
s3-sidecar serve --server-bind 0.0.0.0:9001 \
  --origin-endpoint http://minio:9000 \
  --buckets-json '[{"name":"media","ttl":"1h"}]'
```

The convenience flag `--bucket media` adds a bucket name. Repeat it to add
more buckets.

### Secrets

Do not put a secret in `argv`. Use one of these forms:

- Environment: `S3_SIDECAR_ORIGIN__SECRET_ACCESS_KEY`
- Config file: `secret_access_key = "..."`
- Secret file from the environment: `S3_SIDECAR_ORIGIN__SECRET_ACCESS_KEY_FILE=/run/secrets/origin`
- Secret file from a flag: `--origin-secret-access-key-file /run/secrets/origin`
- Standard input: `--origin-secret-access-key-stdin`

`check --show-config` prints `****` for each set secret. The sidecar never logs
`Authorization` headers or query strings.

### File discovery

The sidecar reads the first file that exists:

1. `--config <path>`
2. `S3_SIDECAR_CONFIG`
3. `./sidecar.toml`
4. `/etc/s3-sidecar/config.toml`

A missing explicit file is an error. A missing default file is allowed.

## Auth

The sidecar verifies SigV4 header auth and presigned query auth. It checks the
canonical request, the signed headers, the payload hash, the region, the clock
skew (15 minutes), and the presigned lifetime (1 to 604800 seconds).

The sidecar re-signs each upstream request with the client's secret. The
bucket allow-list is enforced before the upstream call. A copy request must
name an allowed source bucket.

`aws-chunked` bodies are decoded and verified:
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`,
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`, and
`STREAMING-UNSIGNED-PAYLOAD-TRAILER` are accepted. The signed modes verify
each chunk signature. The unsigned trailer mode does not verify the payload.
Do not use it without TLS.

## Limits in this milestone

- Path-style bucket addressing only.
- One origin endpoint. No cache or cluster.
- No TLS listener. Put the sidecar behind a TLS terminator.
- The sidecar decodes `aws-chunked` bodies. The HTTP client cannot send trailer
  checksums, so the sidecar drops `x-amz-checksum-algorithm` on
  `CreateMultipartUpload` and removes trailer checksum headers. The object
  bytes are unchanged. A client that sends part checksums in
  `CompleteMultipartUpload` can fail, because the origin did not receive the
  part checksums.
- A plain payload hash is checked while the body streams. A mismatch aborts
  the upstream request, but the upstream can receive the full body first.
- The readiness endpoint reports the sidecar process, not the origin.
- `ListBuckets` is not supported.

## Design choices

- The upstream client is `reqwest` plus the local `SigV4` implementation.
- The sidecar streams request and response bodies, passes XML through byte
  for byte, and must verify `SigV4` locally in any case.
- `aws-sdk-s3` and `s3s` were considered. `s3s` would replace the pass-through
  request and response path, so it was not adopted.

## End-to-end tests

`tests/proxy.rs` runs the full proxy path against an in-process fake origin.
It covers streaming, ranges, list, multipart, copy, presigned URLs, invalid
credentials, and the bucket allow-list.

`scripts/e2e-rustfs.sh` runs a real origin and the AWS CLI. It needs `rustfs`
and the AWS CLI v2. The MinIO client `mc` is optional.

```bash
RUSTFS_BIN=/path/to/rustfs \
AWS_BIN=/path/to/aws \
MC_BIN=/path/to/mc \
  scripts/e2e-rustfs.sh
```

The script runs `aws s3 cp`, `aws s3 sync`, `aws s3 ls`, multipart upload,
presigned GET, invalid credentials, and `mc` when `mc` is available. It also
runs the ignored test in `tests/e2e_rustfs.rs`.

## Dependency policy

`deny.toml` allows the permissive licenses in the HTTP and TLS tree:
ISC, BSD-3-Clause, Unicode-3.0, and CDLA-Permissive-2.0. It also skips three
transitive duplicate versions (`base64`, `syn`, and `windows-sys`) with a
reason for each. The project does not use the AWS SDK, so no AWS crate
exceptions are needed.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
