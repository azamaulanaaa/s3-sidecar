#!/bin/sh
# Run the local CI checks. Usage: ci-check.sh commit|push
set -eu

if [ "$#" -ne 1 ]; then
	printf 'ERROR: invalid arguments.\nUsage: %s commit|push\n' "${0##*/}" >&2
	exit 2
fi

case "$1" in
commit | push) mode=$1 ;;
*)
	printf 'ERROR: invalid mode: %s\nUsage: %s commit|push\n' "$1" "${0##*/}" >&2
	exit 2
	;;
esac

if ! command -v mise >/dev/null 2>&1; then
	printf 'ERROR: mise is not installed. Install mise first.\n' >&2
	exit 1
fi

root=$(git rev-parse --show-toplevel)
cd "$root"

run_step() {
	label=$1
	shift
	printf '==> %s\n' "$label"
	if ! "$@"; then
		printf 'ERROR: the step failed: %s\n' "$label" >&2
		exit 1
	fi
}

run_quiet_step() {
	label=$1
	shift
	printf '==> %s\n' "$label"
	if ! "$@" >/dev/null; then
		printf 'ERROR: the step failed: %s\n' "$label" >&2
		exit 1
	fi
}

run_step "Check formatting" mise exec -- cargo fmt --all --check
run_step "Run Clippy" mise exec -- cargo clippy --all-targets --all-features --locked -- -D warnings

if [ "$mode" = push ]; then
	run_step "Run the tests" mise exec -- cargo test --all-targets --all-features --locked
	run_step "Build the documentation" env RUSTDOCFLAGS="-D warnings" mise exec -- cargo doc --no-deps --all-features
	run_quiet_step "Check the release plan" mise exec -- dist plan --output-format=human
	run_step "Check advisories" mise exec -- cargo deny check advisories
	run_step "Check bans" mise exec -- cargo deny check bans
	run_step "Check licenses" mise exec -- cargo deny check licenses
	run_step "Check sources" mise exec -- cargo deny check sources
fi

printf 'OK: all %s checks passed.\n' "$mode"
