# Milestone 24 Notes: Unified Connectivity Release

## Outcome

Milestone 24 turns the shared qcli service runtime into a distributable gateway
release. The native archive and OCI image run the same HTTP and Flight SQL
listeners, authorization, session/query core, Arrow results, audit events,
limits, and high-availability adapters.

## Delivered

- A multi-stage, non-root OCI image with HTTP and Flight SQL ports.
- Native release archives containing the license, changelog, shell
  completions, manual page, operations guides, and deployment manifests.
- Kubernetes and systemd deployment examples with protected configuration,
  health probes, graceful termination, resource limits, and hardened runtime
  settings.
- Unauthenticated `/health/live` and `/health/ready` endpoints; readiness is
  withdrawn as soon as graceful shutdown starts.
- Environment-backed cluster, result-store, node-ID, and Flight signing-key
  settings so orchestrator secrets do not need to appear in process arguments.
- A standard-library HTTP conformance profile that runs against the packaged
  executable rather than a Cargo test process.
- Release automation for a packaged HTTP query, ADBC, Arrow Flight SQL JDBC,
  native Flight SQL, load/backpressure, PostgreSQL coordination, OCI smoke,
  checksums, SBOMs, provenance, and Sigstore signatures.
- A corrected crates.io publish order that includes `qcli-cluster` before its
  dependents.
- Operations documentation covering installation verification, TLS and
  identity, standalone and clustered topology, scaling, migration, rollback,
  and incident response.
- A direct completion unit test and a stable pseudo-terminal navigation gate,
  removing terminal-rendering timing from the release suite.

## Demonstration

The packaged macOS archive was extracted into a clean temporary directory. Its
binary started the HTTP gateway with the demo target and auth file, accepted an
authenticated query through `conformance/m24/http_profile.py`, returned the two
expected rows, and returned HTTP 204 from both health endpoints.

The OCI image built from the repository and its non-root packaged binary
reported `qcli 0.1.0`. The release workflow repeats this on Linux and runs the
existing Python ADBC, Java Arrow Flight SQL JDBC, Go Flight SQL, and Rust Flight
SQL profiles against the packaged server.

## Connectivity support boundary

| Surface | M24 status |
|---|---|
| HTTP API and OpenAPI/Swagger UI | Supported |
| Native Arrow Flight SQL | Supported |
| Python ADBC Flight SQL | Supported |
| Arrow Flight SQL JDBC client | Supported integration profile |
| qcli-branded JDBC driver | Planned for M25 |
| ODBC and BI clients | Experimental; no approved M24 workflow |

M24 does not claim ODBC certification. M20 stays in progress until named ODBC
and BI clients pass repeatable clean-machine suites and are deliberately added
to the support matrix.

## Evidence

The following gates passed during milestone completion:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo test --release -p qcli-core million_rows_stream_in_bounded_batches_to_csv_and_jsonl --locked -- --ignored
QCLI_TEST_POSTGRES_URL=... cargo test -p qcli-cluster postgres_coordination_profile --locked -- --ignored
bash -n scripts/package-release.sh
ruby YAML parse of CI, release, and Kubernetes manifests
bash scripts/package-release.sh x86_64-apple-darwin 0.1.0-m24
python3 conformance/m24/http_profile.py
docker build -t qcli:m24 .
docker run --rm qcli:m24 --version
git diff --check
```

The workspace suite passed 103 executed Rust tests with the intended live
engine and explicit release profiles ignored. The million-row release profile
and PostgreSQL 17 coordination profile were then executed separately and
passed. GitHub release jobs provide the repeatable clean Linux environment for
the packaged language-client matrix, OCI archive, SBOM, provenance, checksums,
and signatures.

## Operational decisions

- The same executable is the CLI and gateway; `serve` selects server mode.
- TLS may terminate directly for Flight SQL or at a trusted proxy according to
  the documented fail-closed modes.
- PostgreSQL remains the certified coordination store; object storage is only
  for immutable retained Arrow results.
- Kubernetes secrets are projected as read-only files or narrow environment
  values and are never included in the image.
- Readiness, draining, and the pod disruption budget protect rolling updates.
- Release support is defined by tested client/version/platform combinations,
  not merely by protocol similarity.

## Accepted limitations

- ODBC/BI remains experimental and is not an M24 supported surface.
- The Java profile certifies the upstream Arrow Flight SQL JDBC client, not the
  branded qcli JDBC driver planned for M25.
- Live Trino, Databricks, and Snowflake release evidence still requires the
  protected connectivity workflow and organization-provided credentials.
- PostgreSQL is the only certified cluster coordinator and cross-region
  active/active operation is not certified.
- The supplied Kubernetes manifest is a secure baseline; production operators
  must replace image coordinates, secrets, certificates, storage, sizing, and
  network policy for their environment.

## Next work

Before M25, the requested product documentation site can turn these engineering
documents into a navigable, versioned, statically hosted user and operator
manual.
