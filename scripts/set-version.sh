#!/usr/bin/env bash
# Set the crate version in Cargo.toml and Cargo.lock.
#
# Usage: scripts/set-version.sh <semver>
#
# The script changes the [package] version in Cargo.toml and the version of the
# s3-sidecar entry in Cargo.lock. It does not run cargo.

set -euo pipefail

usage() {
  echo "Usage: scripts/set-version.sh <semver>" >&2
}

# The SemVer 2.0.0 pattern, with an optional pre-release and build suffix.
semver_pattern='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*)(\.(0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*))*))?([+]([0-9a-zA-Z-]+(\.[0-9a-zA-Z-]+)*))?$'

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

new_version=$1

if [[ -z $new_version ]]; then
  echo "ERROR: The version is empty." >&2
  exit 2
fi

if [[ ! $new_version =~ $semver_pattern ]]; then
  echo "ERROR: The version is not valid SemVer: ${new_version}" >&2
  exit 2
fi

root=$(cd "$(dirname "$0")/.." && pwd)
manifest=${root}/Cargo.toml
lockfile=${root}/Cargo.lock

if [[ ! -f $manifest ]]; then
  echo "ERROR: Cargo.toml is missing." >&2
  exit 2
fi

if [[ ! -f $lockfile ]]; then
  echo "ERROR: Cargo.lock is missing." >&2
  exit 2
fi

old_version=$(
  awk '
    /^\[/ { in_package = ($0 == "[package]") }
    in_package && /^version[[:space:]]*=/ {
      line = $0
      sub(/^[^"]*"/, "", line)
      sub(/".*$/, "", line)
      print line
      exit
    }
  ' "$manifest"
)

if [[ -z $old_version ]]; then
  echo "ERROR: Cargo.toml has no version in [package]." >&2
  exit 2
fi

old_lock_version=$(
  awk '
    /^\[\[package\]\]/ { in_package = 1; name = ""; next }
    in_package && /^name = "/ {
      line = $0
      sub(/^name = "/, "", line)
      sub(/"$/, "", line)
      name = line
    }
    in_package && name == "s3-sidecar" && /^version = "/ {
      line = $0
      sub(/^version = "/, "", line)
      sub(/"$/, "", line)
      print line
      exit
    }
  ' "$lockfile"
)

if [[ -z $old_lock_version ]]; then
  echo "ERROR: Cargo.lock has no s3-sidecar package." >&2
  exit 2
fi

if [[ $new_version == "$old_version" ]]; then
  echo "ERROR: The version is already ${new_version}." >&2
  exit 1
fi

manifest_tmp=$(mktemp "${manifest}.XXXXXX")
lock_tmp=$(mktemp "${lockfile}.XXXXXX")

cleanup() {
  rm -f -- "$manifest_tmp" "$lock_tmp"
}
trap cleanup EXIT

cp -p -- "$manifest" "$manifest_tmp"
cp -p -- "$lockfile" "$lock_tmp"

if ! awk -v new="$new_version" '
  /^\[/ { in_package = ($0 == "[package]") }
  in_package && !done && /^version[[:space:]]*=[[:space:]]*"/ {
    match($0, /^version[[:space:]]*=[[:space:]]*/)
    printf "%s\"%s\"\n", substr($0, 1, RLENGTH), new
    done = 1
    next
  }
  { print }
  END { if (!done) exit 1 }
' "$manifest" > "$manifest_tmp"; then
  echo "ERROR: Cargo.toml has no version in [package]." >&2
  exit 2
fi

if ! awk -v new="$new_version" '
  /^\[\[package\]\]/ { in_package = 1; name = ""; print; next }
  in_package && /^name = "/ {
    line = $0
    sub(/^name = "/, "", line)
    sub(/"$/, "", line)
    name = line
  }
  in_package && name == "s3-sidecar" && !done && /^version = "/ {
    printf "version = \"%s\"\n", new
    done = 1
    next
  }
  { print }
  END { if (!done) exit 1 }
' "$lockfile" > "$lock_tmp"; then
  echo "ERROR: Cargo.lock has no s3-sidecar version." >&2
  exit 2
fi

mv -f -- "$manifest_tmp" "$manifest"
mv -f -- "$lock_tmp" "$lockfile"

printf 'Cargo.toml: %s -> %s\n' "$old_version" "$new_version"
printf 'Cargo.lock: %s -> %s\n' "$old_lock_version" "$new_version"
