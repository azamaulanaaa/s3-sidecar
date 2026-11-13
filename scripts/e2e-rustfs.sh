#!/usr/bin/env bash
# Run the end-to-end checks against rustfs through the sidecar.
#
# Requirements:
#   - rustfs on PATH, or RUSTFS_BIN points to the binary
#   - aws on PATH, or AWS_BIN points to the AWS CLI v2 binary
#   - mc on PATH, or MC_BIN points to the MinIO client (optional)
#
# The script uses the fixed test credentials below. Use it only for local tests.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUSTFS_BIN=${RUSTFS_BIN:-rustfs}
AWS_BIN=${AWS_BIN:-aws}
MC_BIN=${MC_BIN:-mc}
ACCESS_KEY=s3sidecartest
SECRET_KEY=s3sidecartest-secret
ORIGIN_PORT=19001
PROXY_PORT=19002
ORIGIN="http://127.0.0.1:${ORIGIN_PORT}"
PROXY="http://127.0.0.1:${PROXY_PORT}"
BUCKET=media

for tool in "$RUSTFS_BIN" "$AWS_BIN"; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		printf 'ERROR: cannot find %s. Set RUSTFS_BIN or AWS_BIN.\n' "$tool" >&2
		exit 2
	fi
done

TMP=$(mktemp -d)
RUSTFS_PID=
SIDECAR_PID=
cleanup() {
	if [ -n "$SIDECAR_PID" ]; then kill "$SIDECAR_PID" 2>/dev/null || true; fi
	if [ -n "$RUSTFS_PID" ]; then kill "$RUSTFS_PID" 2>/dev/null || true; fi
	rm -rf "$TMP"
}
trap cleanup EXIT

mkdir -p "$TMP/data" "$TMP/work"
printf 'seed\n' > "$TMP/work/seed.txt"
mkdir -p "$TMP/work/sync"
printf 'one\n' > "$TMP/work/sync/one.txt"
printf 'two\n' > "$TMP/work/sync/two.txt"

export AWS_ACCESS_KEY_ID=$ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$SECRET_KEY
export AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true

RUSTFS_ACCESS_KEY=$ACCESS_KEY \
	RUSTFS_SECRET_KEY=$SECRET_KEY \
	RUSTFS_CONSOLE_ENABLE=false \
	"$RUSTFS_BIN" server --address "127.0.0.1:${ORIGIN_PORT}" "$TMP/data" \
	>"$TMP/rustfs.log" 2>&1 &
RUSTFS_PID=$!

for _ in $(seq 1 120); do
	if "$AWS_BIN" s3api list-buckets --endpoint-url "$ORIGIN" >/dev/null 2>&1; then
		break
	fi
	sleep 0.5
done
if ! "$AWS_BIN" s3api list-buckets --endpoint-url "$ORIGIN" >/dev/null 2>&1; then
	printf 'ERROR: rustfs did not become ready. See %s.\n' "$TMP/rustfs.log" >&2
	exit 1
fi

cat >"$TMP/sidecar.toml" <<EOF
mode = "cache"

[server]
bind = "127.0.0.1:${PROXY_PORT}"

[origin]
endpoint = "${ORIGIN}"
region = "us-east-1"
access_key_id = "${ACCESS_KEY}"
secret_access_key = "${SECRET_KEY}"

[cache]
mode = "off"

[[clients]]
access_key_id = "${ACCESS_KEY}"
secret_access_key = "${SECRET_KEY}"
buckets = ["${BUCKET}"]

[[buckets]]
name = "${BUCKET}"
ttl = "1h"
EOF

if [ ! -x "$ROOT/target/debug/s3-sidecar" ]; then
	(cd "$ROOT" && cargo build --quiet)
fi
"$ROOT/target/debug/s3-sidecar" serve --config "$TMP/sidecar.toml" \
	>"$TMP/sidecar.log" 2>&1 &
SIDECAR_PID=$!

for _ in $(seq 1 120); do
	if curl -sf --max-time 1 "$PROXY/readyz" >/dev/null 2>&1; then
		break
	fi
	sleep 0.5
done
if ! curl -sf --max-time 1 "$PROXY/readyz" >/dev/null 2>&1; then
	printf 'ERROR: the sidecar did not become ready. See %s.\n' "$TMP/sidecar.log" >&2
	exit 1
fi

printf '==> direct bucket setup\n'
"$AWS_BIN" s3 mb "s3://${BUCKET}" --endpoint-url "$ORIGIN"
"$AWS_BIN" s3 cp "$TMP/work/seed.txt" "s3://${BUCKET}/seed.txt" --endpoint-url "$ORIGIN" --only-show-errors

printf '==> aws s3 cp and ls\n'
"$AWS_BIN" s3 cp "$TMP/work/seed.txt" "s3://${BUCKET}/through-sidecar.txt" --endpoint-url "$PROXY" --only-show-errors
"$AWS_BIN" s3 ls "s3://${BUCKET}/" --endpoint-url "$PROXY"
"$AWS_BIN" s3 cp "s3://${BUCKET}/through-sidecar.txt" "$TMP/work/download.txt" --endpoint-url "$PROXY" --only-show-errors
cmp "$TMP/work/seed.txt" "$TMP/work/download.txt"

printf '==> aws s3 sync\n'
"$AWS_BIN" s3 sync "$TMP/work/sync" "s3://${BUCKET}/sync" --endpoint-url "$PROXY" --only-show-errors
"$AWS_BIN" s3 ls "s3://${BUCKET}/sync/" --endpoint-url "$PROXY"

printf '==> range, copy, and list\n'
"$AWS_BIN" s3api head-object --bucket "$BUCKET" --key through-sidecar.txt --endpoint-url "$PROXY" >/dev/null
"$AWS_BIN" s3api get-object --bucket "$BUCKET" --key through-sidecar.txt --range bytes=0-3 \
	"$TMP/work/range.txt" --endpoint-url "$PROXY" >/dev/null
cmp "$TMP/work/range.txt" <(printf 'seed')
"$AWS_BIN" s3api copy-object --bucket "$BUCKET" --key copied.txt \
	--copy-source "${BUCKET}/through-sidecar.txt" --endpoint-url "$PROXY" >/dev/null
"$AWS_BIN" s3api list-objects-v2 --bucket "$BUCKET" --prefix through --endpoint-url "$PROXY" >/dev/null

printf '==> multipart upload\n'
dd if=/dev/zero of="$TMP/work/big.bin" bs=1M count=20 status=none
"$AWS_BIN" s3 cp "$TMP/work/big.bin" "s3://${BUCKET}/big.bin" --endpoint-url "$PROXY" --only-show-errors
"$AWS_BIN" s3 cp "s3://${BUCKET}/big.bin" "$TMP/work/big-down.bin" --endpoint-url "$PROXY" --only-show-errors
cmp "$TMP/work/big.bin" "$TMP/work/big-down.bin"

printf '==> presigned GET\n'
PRESIGNED=$("$AWS_BIN" s3 presign "s3://${BUCKET}/seed.txt" --endpoint-url "$PROXY" --expires-in 600)
curl -sf "$PRESIGNED" >"$TMP/work/presigned.txt"
cmp "$TMP/work/seed.txt" "$TMP/work/presigned.txt"

printf '==> invalid credentials\n'
if AWS_SECRET_ACCESS_KEY=wrong-secret "$AWS_BIN" s3 ls "s3://${BUCKET}/" \
	--endpoint-url "$PROXY" 2>"$TMP/invalid.log"; then
	printf 'ERROR: invalid credentials were accepted.\n' >&2
	exit 1
fi
if ! grep -q SignatureDoesNotMatch "$TMP/invalid.log"; then
	printf 'ERROR: the invalid request did not return SignatureDoesNotMatch.\n' >&2
	cat "$TMP/invalid.log" >&2
	exit 1
fi

if command -v "$MC_BIN" >/dev/null 2>&1; then
	printf '==> mc\n'
	export MC_CONFIG_DIR="$TMP/mc"
	mkdir -p "$MC_CONFIG_DIR"
	"$MC_BIN" alias set proxy "$PROXY" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null
	"$MC_BIN" ls "proxy/${BUCKET}"
	printf 'mc body\n' >"$TMP/work/mc.txt"
	"$MC_BIN" cp "$TMP/work/mc.txt" "proxy/${BUCKET}/mc.txt" >/dev/null
	"$MC_BIN" cat "proxy/${BUCKET}/mc.txt" | cmp - "$TMP/work/mc.txt"
	"$MC_BIN" rm "proxy/${BUCKET}/mc.txt" >/dev/null
else
	printf '==> mc is not installed. Skip the mc checks.\n'
fi

printf '==> ignored integration test\n'
export S3_SIDECAR_E2E_PROXY=$PROXY
export S3_SIDECAR_E2E_BUCKET=$BUCKET
export S3_SIDECAR_E2E_REGION=us-east-1
export S3_SIDECAR_E2E_ACCESS_KEY=$ACCESS_KEY
export S3_SIDECAR_E2E_SECRET_KEY=$SECRET_KEY
(cd "$ROOT" && cargo test --test e2e_rustfs -- --ignored --nocapture)
