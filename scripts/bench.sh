#!/usr/bin/env bash
# Run the reproducible sidecar benchmarks.
#
# Requirements:
#   - rustfs or minio on PATH, or RUSTFS_BIN or MINIO_BIN points to the binary
#   - aws on PATH or AWS_BIN, or mc on PATH or MC_BIN, to create buckets
#   - curl
#
# Set BENCH_REQUESTS to change the request count. Set BENCH_RESULTS to change
# the output directory. The default output directory is bench-results/.
#
# The script does not print invented numbers. It writes the measured output of
# each scenario to a file under the results directory.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
REQUESTS=${BENCH_REQUESTS:-200}
RESULTS=${BENCH_RESULTS:-$ROOT/bench-results}
ACCESS_KEY=s3sidecartest
SECRET_KEY=s3sidecartest-secret
REGION=us-east-1

ORIGIN_PORT=19101
CACHE_PORT=19102
CACHE_PROXY_PORT=19104
CLUSTER_PORT=19105
RAFT_PORTS=(19201 19202 19203)

ORIGIN="http://127.0.0.1:${ORIGIN_PORT}"
CACHE="http://127.0.0.1:${CACHE_PORT}"
CACHE_PROXY="http://127.0.0.1:${CACHE_PROXY_PORT}"
CLUSTER="http://127.0.0.1:${CLUSTER_PORT}"

BUCKET_ORIGIN=bench-origin
BUCKET_CACHE=bench-cache
BUCKET_A=bench-a
BUCKET_B=bench-b
BUCKET_C=bench-c

S3_BIN=
S3_KIND=
CLI=
AWS=
MC=
SIDECAR=
LOADGEN=
STARTED_PID=
ORIGIN_PID=
CACHE_PID=
PIDS=()
TMP=

detect_tools() {
	if [ -n "${RUSTFS_BIN:-}" ]; then
		S3_BIN=$RUSTFS_BIN
		S3_KIND=rustfs
	elif [ -n "${MINIO_BIN:-}" ]; then
		S3_BIN=$MINIO_BIN
		S3_KIND=minio
	elif command -v rustfs >/dev/null 2>&1; then
		S3_BIN=$(command -v rustfs)
		S3_KIND=rustfs
	elif command -v minio >/dev/null 2>&1; then
		S3_BIN=$(command -v minio)
		S3_KIND=minio
	else
		printf 'ERROR: set RUSTFS_BIN or MINIO_BIN, or put one binary on PATH.\n' >&2
		exit 2
	fi

	if ! command -v "$S3_BIN" >/dev/null 2>&1 && [ ! -x "$S3_BIN" ]; then
		printf 'ERROR: cannot run the S3 binary %s.\n' "$S3_BIN" >&2
		exit 2
	fi

	if [ -n "${AWS_BIN:-}" ] || command -v aws >/dev/null 2>&1; then
		AWS=${AWS_BIN:-aws}
		CLI=aws
	elif [ -n "${MC_BIN:-}" ] || command -v mc >/dev/null 2>&1; then
		MC=${MC_BIN:-mc}
		CLI=mc
	else
		printf 'ERROR: no S3 client found. Set AWS_BIN or MC_BIN, or put aws or mc on PATH.\n' >&2
		exit 2
	fi

	if [ "$CLI" = aws ]; then
		if ! command -v "$AWS" >/dev/null 2>&1 && [ ! -x "$AWS" ]; then
			printf 'ERROR: cannot run the S3 client %s.\n' "$AWS" >&2
			exit 2
		fi
	elif ! command -v "$MC" >/dev/null 2>&1 && [ ! -x "$MC" ]; then
		printf 'ERROR: cannot run the S3 client %s.\n' "$MC" >&2
		exit 2
	fi

	if ! command -v curl >/dev/null 2>&1; then
		printf 'ERROR: curl is required to wait for the services.\n' >&2
		exit 2
	fi
}

cleanup() {
	for pid in "${PIDS[@]:-}"; do
		if [ -n "$pid" ]; then
			kill "$pid" 2>/dev/null || true
		fi
	done
	wait 2>/dev/null || true
	if [ -n "$TMP" ]; then
		rm -rf "$TMP"
	fi
}
trap cleanup EXIT

wait_http() {
	local url=$1
	local log=$2
	for _ in $(seq 1 120); do
		if curl -s -o /dev/null --max-time 1 "$url"; then
			return 0
		fi
		sleep 0.5
	done
	printf 'ERROR: %s did not become ready. See %s.\n' "$url" "$log" >&2
	return 1
}

wait_ready() {
	local base=$1
	local log=$2
	for _ in $(seq 1 120); do
		if curl -sf --max-time 1 "$base/readyz" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.5
	done
	printf 'ERROR: %s did not become ready. See %s.\n' "$base" "$log" >&2
	return 1
}

start_s3() {
	local port=$1
	local data=$2
	local log=$3
	mkdir -p "$data"
	if [ "$S3_KIND" = rustfs ]; then
		RUSTFS_ACCESS_KEY=$ACCESS_KEY \
			RUSTFS_SECRET_KEY=$SECRET_KEY \
			RUSTFS_CONSOLE_ENABLE=false \
			"$S3_BIN" server --address "127.0.0.1:$port" "$data" >"$log" 2>&1 &
	else
		MINIO_ROOT_USER=$ACCESS_KEY \
			MINIO_ROOT_PASSWORD=$SECRET_KEY \
			"$S3_BIN" server --address "127.0.0.1:$port" "$data" >"$log" 2>&1 &
	fi
	STARTED_PID=$!
	PIDS+=("$STARTED_PID")
}

start_sidecar() {
	local config=$1
	local log=$2
	"$SIDECAR" serve --config "$config" >"$log" 2>&1 &
	STARTED_PID=$!
	PIDS+=("$STARTED_PID")
}

stop_pid() {
	local pid=${1:-}
	if [ -z "$pid" ]; then
		return 0
	fi
	kill "$pid" 2>/dev/null || true
	for _ in $(seq 1 120); do
		if ! kill -0 "$pid" 2>/dev/null; then
			return 0
		fi
		sleep 0.5
	done
	kill -9 "$pid" 2>/dev/null || true
}

mc_alias() {
	local url=$1
	if [ "$url" = "$ORIGIN" ]; then
		printf 'bench-origin'
	else
		printf 'bench-cache'
	fi
}

bucket_exists() {
	local url=$1
	local bucket=$2
	if [ "$CLI" = aws ]; then
		AWS_ACCESS_KEY_ID=$ACCESS_KEY \
			AWS_SECRET_ACCESS_KEY=$SECRET_KEY \
			AWS_DEFAULT_REGION=$REGION \
			AWS_EC2_METADATA_DISABLED=true \
			"$AWS" --endpoint-url "$url" s3api head-bucket --bucket "$bucket" >/dev/null 2>&1
	else
		local alias_name
		alias_name=$(mc_alias "$url")
		MC_CONFIG_DIR=$TMP/mc "$MC" ls "$alias_name/$bucket" >/dev/null 2>&1
	fi
}

s3_mb() {
	local url=$1
	local bucket=$2
	if bucket_exists "$url" "$bucket"; then
		printf '==> bucket %s already exists; skip creation\n' "$bucket"
		return 0
	fi
	if [ "$CLI" = aws ]; then
		AWS_ACCESS_KEY_ID=$ACCESS_KEY \
			AWS_SECRET_ACCESS_KEY=$SECRET_KEY \
			AWS_DEFAULT_REGION=$REGION \
			AWS_EC2_METADATA_DISABLED=true \
			"$AWS" --endpoint-url "$url" s3 mb "s3://$bucket" --only-show-errors
	else
		local alias_name
		alias_name=$(mc_alias "$url")
		MC_CONFIG_DIR=$TMP/mc "$MC" mb --ignore-existing "$alias_name/$bucket" >/dev/null
	fi
}

run_load() {
	local name=$1
	shift
	printf '==> %s\n' "$name"
	"$LOADGEN" "$@" | tee "$RESULTS/$name.txt"
}

run_seed() {
	printf '==> seed data\n'
	"$LOADGEN" "$@" >/dev/null
}

write_cache_config() {
	cat >"$TMP/cache.toml" <<EOF
mode = "cache"

[server]
bind = "127.0.0.1:${CACHE_PROXY_PORT}"

[origin]
endpoint = "${ORIGIN}"
region = "${REGION}"
access_key_id = "${ACCESS_KEY}"
secret_access_key = "${SECRET_KEY}"

[cache]
endpoint = "${CACHE}"
region = "${REGION}"
access_key_id = "${ACCESS_KEY}"
secret_access_key = "${SECRET_KEY}"
bucket = "${BUCKET_CACHE}"
prefix = "cache"
mode = "read_write"
max_bytes = 0
max_object_bytes = 1073741824
write_policy = "write_back"
on_cache_error = "origin"

[[clients]]
access_key_id = "${ACCESS_KEY}"
secret_access_key = "${SECRET_KEY}"
buckets = ["${BUCKET_ORIGIN}"]

[[buckets]]
name = "${BUCKET_ORIGIN}"
ttl = "1h"
EOF
}

write_single_config() {
	cat >"$TMP/single.toml" <<EOF
mode = "cluster"

[server]
bind = "127.0.0.1:${CLUSTER_PORT}"

[consensus]
backend = "single"

[consensus.raft]
node_id = "a"
peers = ["a"]

[backing]
bucket = "${BUCKET_A}"
endpoint = "${ORIGIN}"
region = "${REGION}"
access_key_id = "${ACCESS_KEY}"
secret_access_key = "${SECRET_KEY}"

[cluster]
metadata_path = "${TMP}/meta-single.redb"
secret = "bench-cluster-secret"

[[cluster.nodes]]
id = "a"
addr = "http://127.0.0.1:${CLUSTER_PORT}"

[[clients]]
access_key_id = "${ACCESS_KEY}"
secret_access_key = "${SECRET_KEY}"
buckets = ["${BUCKET_ORIGIN}"]

[[buckets]]
name = "${BUCKET_ORIGIN}"
write_ack = "leader"
read = "local"
replicas = 1
min_replicas = 1
EOF
}

write_raft_config() {
	local node=$1
	local port=$2
	local bucket=$3
	cat >"$TMP/raft-${node}.toml" <<EOF
mode = "cluster"

[server]
bind = "127.0.0.1:${port}"

[consensus]
backend = "raft"

[consensus.raft]
node_id = "${node}"
bootstrap = $([ "$node" = a ] && printf 'true' || printf 'false')
secret = "bench-raft-secret"
rpc_timeout = "5s"

[[consensus.raft.nodes]]
id = "a"
addr = "http://127.0.0.1:${RAFT_PORTS[0]}"

[[consensus.raft.nodes]]
id = "b"
addr = "http://127.0.0.1:${RAFT_PORTS[1]}"

[[consensus.raft.nodes]]
id = "c"
addr = "http://127.0.0.1:${RAFT_PORTS[2]}"

[backing]
bucket = "${bucket}"
endpoint = "${ORIGIN}"
region = "${REGION}"
access_key_id = "${ACCESS_KEY}"
secret_access_key = "${SECRET_KEY}"

[cluster]
metadata_path = "${TMP}/meta-${node}.redb"
secret = "bench-cluster-secret"

[[cluster.nodes]]
id = "a"
addr = "http://127.0.0.1:${RAFT_PORTS[0]}"

[[cluster.nodes]]
id = "b"
addr = "http://127.0.0.1:${RAFT_PORTS[1]}"

[[cluster.nodes]]
id = "c"
addr = "http://127.0.0.1:${RAFT_PORTS[2]}"

[[clients]]
access_key_id = "${ACCESS_KEY}"
secret_access_key = "${SECRET_KEY}"
buckets = ["${BUCKET_ORIGIN}"]

[[buckets]]
name = "${BUCKET_ORIGIN}"
write_ack = "quorum"
read = "local_then_peer"
replicas = 3
min_replicas = 2
EOF
}

detect_tools
mkdir -p "$RESULTS"
TMP=$(mktemp -d)

printf '==> build\n'
(
	cd "$ROOT"
	cargo build --release --quiet --example loadgen
	cargo build --release --quiet
)
SIDECAR=$ROOT/target/release/s3-sidecar
LOADGEN=$ROOT/target/release/examples/loadgen
if [ ! -x "$SIDECAR" ] || [ ! -x "$LOADGEN" ]; then
	printf 'ERROR: the release build did not produce the binaries.\n' >&2
	exit 1
fi

{
	printf 'started_at: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	printf 's3_binary: %s (%s)\n' "$S3_BIN" "$S3_KIND"
	printf 'requests: %s\n' "$REQUESTS"
	"$SIDECAR" --version 2>/dev/null || true
	if git -C "$ROOT" rev-parse HEAD >/dev/null 2>&1; then
		printf 'commit: %s\n' "$(git -C "$ROOT" rev-parse HEAD)"
	fi
} >"$RESULTS/environment.txt"

printf '==> start S3 servers\n'
start_s3 "$ORIGIN_PORT" "$TMP/origin" "$TMP/origin.log"
ORIGIN_PID=$STARTED_PID
start_s3 "$CACHE_PORT" "$TMP/cache" "$TMP/cache.log"
CACHE_PID=$STARTED_PID
wait_http "$ORIGIN" "$TMP/origin.log"
wait_http "$CACHE" "$TMP/cache.log"

if [ "$CLI" = mc ]; then
	export MC_CONFIG_DIR=$TMP/mc
	mkdir -p "$MC_CONFIG_DIR"
	"$MC" alias set bench-origin "$ORIGIN" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null
	"$MC" alias set bench-cache "$CACHE" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null
fi

printf '==> create buckets\n'
s3_mb "$ORIGIN" "$BUCKET_ORIGIN"
s3_mb "$CACHE" "$BUCKET_CACHE"
s3_mb "$ORIGIN" "$BUCKET_A"
s3_mb "$ORIGIN" "$BUCKET_B"
s3_mb "$ORIGIN" "$BUCKET_C"

write_cache_config
printf '==> cache scenario\n'
start_sidecar "$TMP/cache.toml" "$TMP/cache-sidecar.log"
CACHE_PROXY_PID=$STARTED_PID
wait_ready "$CACHE_PROXY" "$TMP/cache-sidecar.log"
run_seed --endpoint "$ORIGIN" --bucket "$BUCKET_ORIGIN" --key bench/cache-hit \
	--requests "$REQUESTS" --concurrency 8 --size 4096 --mode put \
	--access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY" --region "$REGION"
run_load cache-miss --endpoint "$CACHE_PROXY" --bucket "$BUCKET_ORIGIN" --key bench/cache-hit \
	--requests "$REQUESTS" --concurrency 8 --size 4096 --mode get \
	--access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY" --region "$REGION"
sleep 2
run_load cache-hit --endpoint "$CACHE_PROXY" --bucket "$BUCKET_ORIGIN" --key bench/cache-hit \
	--requests "$REQUESTS" --concurrency 8 --size 4096 --mode get \
	--access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY" --region "$REGION"
stop_pid "$CACHE_PROXY_PID"

write_single_config
printf '==> cluster leader scenario\n'
start_sidecar "$TMP/single.toml" "$TMP/single.log"
SINGLE_PID=$STARTED_PID
wait_ready "$CLUSTER" "$TMP/single.log"
run_load cluster-leader --endpoint "$CLUSTER" --bucket "$BUCKET_ORIGIN" --key bench/leader \
	--requests "$REQUESTS" --concurrency 8 --size 4096 --mode put \
	--access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY" --region "$REGION"
stop_pid "$SINGLE_PID"

printf '==> cluster quorum scenario\n'
write_raft_config a "${RAFT_PORTS[0]}" "$BUCKET_A"
write_raft_config b "${RAFT_PORTS[1]}" "$BUCKET_B"
write_raft_config c "${RAFT_PORTS[2]}" "$BUCKET_C"
start_sidecar "$TMP/raft-a.toml" "$TMP/raft-a.log"
RAFT_A_PID=$STARTED_PID
start_sidecar "$TMP/raft-b.toml" "$TMP/raft-b.log"
RAFT_B_PID=$STARTED_PID
start_sidecar "$TMP/raft-c.toml" "$TMP/raft-c.log"
RAFT_C_PID=$STARTED_PID
wait_ready "http://127.0.0.1:${RAFT_PORTS[0]}" "$TMP/raft-a.log"
run_seed --endpoint "http://127.0.0.1:${RAFT_PORTS[0]}" --bucket "$BUCKET_ORIGIN" \
	--key bench/quorum-warmup --requests 1 --concurrency 1 --size 4096 --mode put \
	--access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY" --region "$REGION"
wait_ready "http://127.0.0.1:${RAFT_PORTS[1]}" "$TMP/raft-b.log"
wait_ready "http://127.0.0.1:${RAFT_PORTS[2]}" "$TMP/raft-c.log"
run_load cluster-quorum --endpoint "http://127.0.0.1:${RAFT_PORTS[0]}" --bucket "$BUCKET_ORIGIN" \
	--key bench/quorum --requests "$REQUESTS" --concurrency 4 --size 4096 --mode put \
	--access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY" --region "$REGION"
stop_pid "$RAFT_A_PID"
stop_pid "$RAFT_B_PID"
stop_pid "$RAFT_C_PID"

printf '==> results written to %s\n' "$RESULTS"
