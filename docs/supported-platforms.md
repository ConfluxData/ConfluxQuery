# Supported Platforms and Engines

## Release platforms

| Operating system | Architecture | Rust target | Archive |
|---|---|---|---|
| Linux | x86-64 | `x86_64-unknown-linux-gnu` | `.tar.gz` |
| Linux | ARM64 | `aarch64-unknown-linux-gnu` | `.tar.gz` |
| macOS | Intel | `x86_64-apple-darwin` | `.tar.gz` |
| macOS | Apple Silicon | `aarch64-apple-darwin` | `.tar.gz` |
| Windows | x86-64 | `x86_64-pc-windows-msvc` | `.zip` |
| OCI container | Linux x86-64 | `linux/amd64` | `.oci.tar.gz` |

Rust 1.86 is the minimum supported toolchain for source builds.

## Initial engine/client matrix

| Engine | qcli transport foundation | Initial authentication |
|---|---|---|
| Trino | `trino-rust-client` 0.11 plus qcli adapter | Basic and bearer |
| Databricks SQL | qcli Statement Execution API adapter using `reqwest` | PAT |
| Snowflake | `snowflakedb-rs` 1.1 | Username/password and programmatic access token through the password field |

Live engine behavior is validated against configured test environments before a
stable release. Exact server versions used for each release should be recorded
in that release's notes.
