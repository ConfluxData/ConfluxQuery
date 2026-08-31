# Unified Connectivity Release Contract

M24 publishes one ConfluxQuery Gateway artifact through the `qcli` binary. Its
HTTP and Flight SQL frontends use
the same target configuration, authenticated principal, session/query service,
authorization, quotas, audit events, retention, and optional cluster state.

## Released surfaces

| Surface | Release status | Required evidence |
|---|---|---|
| CLI batch and terminal | Supported | Cross-platform workspace and archive smoke tests |
| HTTP v1 and OpenAPI | Supported | Packaged stdlib HTTP query profile and Rust API suite |
| Native Flight SQL | Supported | Rust protocol suite and packaged server smoke test |
| Python/Go/Java/Rust ADBC | Supported at pinned versions | Clean-client M19 profiles |
| Apache Arrow Flight SQL JDBC | Demo adapter supported | Clean Java JDBC profile |
| Apache Arrow Flight SQL ODBC | Experimental | Source-profile integrity only; no end-user package |
| ConfluxQuery JDBC Driver | Supported named surface | M25 complete |

No ODBC/BI client is advertised as approved in M24. This preserves the M20
decision to keep ODBC experimental until an installable upstream package,
cancellation, parameter binding, and representative BI exits are available.
Consequently the supported release gate covers the complete set of approved
ODBC workflows, which is currently empty, and records that limitation rather
than implying certification.

## Evidence layers

- Every pull request: format, strict Clippy, rustdoc, workspace tests on Linux,
  macOS, and Windows, Rust 1.89, RustSec, clean ADBC/JDBC clients, PostgreSQL HA,
  million-row streaming, release archive, and OCI image smoke tests.
- Protected live workflow: pinned ADBC clients against Trino, Databricks SQL,
  and Snowflake using repository secrets.
- Tagged release: repeats validation, builds five native archives and one
  Linux AMD64 OCI archive, runs packaged HTTP/ADBC/JDBC profiles, runs HA/load
  gates, generates SPDX SBOM and SHA-256 checksums, creates provenance, and
  signs every published file with keyless Sigstore.

Compatibility is intentionally explicit in `connectivity-compatibility.md` and
`supported-platforms.md`. A client, engine, platform, or authentication method
not listed there is unverified rather than implicitly supported.

## Release boundaries

- Transactions are not normalized.
- JDBC against the three production engines awaits safe preparation metadata.
- ODBC remains experimental.
- Active warehouse queries cannot yet reattach after qcli node loss.
- The OCI artifact is Linux AMD64; native archives cover the five declared
  platform targets.
- Performance gates prove boundedness and backpressure, not a universal SLA.
