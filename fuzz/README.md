# Fuzz targets

This crate holds the `cargo-fuzz` targets for the parsers that accept data
from clients.

- `sigv4` sends arbitrary bytes to the `SigV4` authorization parser and the
  canonical request code.
- `s3_xml` sends arbitrary bytes to the XML tag and list parsers. The targets
  cover `ListObjectsV2`, multipart, and copy responses.

The crate has an empty `[workspace]` table. It is not part of the root
workspace. The root `cargo test` and `cargo clippy` do not build it.

## Requirements

Install the nightly toolchain and `cargo-fuzz` one time:

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
```

## Run a target

Run the commands from the `fuzz/` directory:

```bash
cd fuzz
cargo +nightly fuzz run sigv4 -- -max_total_time=60
cargo +nightly fuzz run s3_xml -- -max_total_time=60
```

`-max_total_time=60` stops the target after 60 seconds. Increase the value for
a longer run.

The `fuzz` mise task runs both targets for 60 seconds each:

```bash
mise run fuzz
```

## Check the crate on stable

The stable toolchain can compile the targets when the fuzz runtime is present:

```bash
cargo check --manifest-path fuzz/Cargo.toml
```

A target that panics is a finding. Do not ignore it. Fix the parser or add a
guard in the production code.

## Reproduce a finding

`cargo-fuzz` writes the input that caused the panic under
`fuzz/artifacts/<target>/`. Run the target with that file:

```bash
cargo +nightly fuzz run sigv4 artifacts/sigv4/crash-<hash>
```

## Corpus

The corpus lives under `fuzz/corpus/<target>/`. It is not in Git. A new run
starts from an empty corpus. Add useful inputs to the corpus when you find a
case that exercises a hard path.
