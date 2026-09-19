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

Rust 1.89 is the minimum supported toolchain for source builds. This baseline
matches the standard-library file-lock API required by the interactive client
dependency set.

## Initial engine/client matrix

| Engine | qcli transport foundation | Initial authentication |
|---|---|---|
| Trino | `trino-rust-client` 0.11 plus qcli adapter | Basic and bearer |
| Databricks SQL | qcli Statement Execution API adapter using `reqwest` | PAT |
| Snowflake | `snowflakedb-rs` 1.1 | Username/password and programmatic access token through the password field |

Every supported `v...` release tag runs publication-blocking live certification
against the pinned `trinodb/trino:483` image. The profile covers the packaged
CLI and Gateway through direct SQL, metadata, streaming, cancellation, HTTP,
Flight SQL, Python/Go/Java/Rust ADBC, and the ConfluxQuery JDBC Driver.
Databricks SQL and Snowflake remain manually validated for `v0.1.0`; their
credentialed three-engine workflow can be run independently when protected
test environments are available.
