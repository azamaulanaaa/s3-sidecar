# s3-sidecar

A small sidecar binary for S3 operations.

## Build and test

This project uses [mise](https://mise.jdx.dev/) to manage tools and tasks.

```bash
mise install
mise run build
mise run test
mise run fmt
mise run lint
```

To print the cargo-dist release plan, run:

```bash
mise run dist-plan
```

## Local CI

Run this command one time for each clone to install the Git hooks:

```bash
mise run hooks
```

The `pre-commit` hook runs the formatting check and Clippy.
The `pre-push` hook runs the formatting check, Clippy, the tests, the docs, the release plan, and all cargo-deny checks.
To run the full set by hand, run:

```bash
mise run ci
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
