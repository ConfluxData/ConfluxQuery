# Milestone 19 — ADBC and JDBC compatibility

Status: Complete

## Outcome

qcli now has version-pinned, clean-client connectivity profiles for Python, Go,
Java, and Rust/C-driver-manager ADBC plus Apache Arrow Flight SQL JDBC. These
profiles run against a real qcli Flight listener rather than invoking Rust
service internals.

The same suite is a pull-request gate and a packaged-release gate. The release
job extracts the built Linux archive and starts that exact binary before any
release is published. A separate protected workflow certifies the ADBC profiles
against configured Trino, Databricks SQL, and Snowflake targets.

## Versions

- Python `adbc-driver-flightsql` 1.12.0 with PyArrow 25.0.1.
- Go `github.com/apache/arrow-adbc/go/adbc` 1.12.0.
- Java `org.apache.arrow.adbc:adbc-driver-flight-sql` 0.24.0.
- Rust `adbc_driver_manager` and `adbc_core` 0.24.0.
- Native Flight SQL C ABI driver 1.12.0, located by the community
  `adbc-driver-flightsql` 0.1.2 distribution crate and checksum-verified against
  Apache's PyPI wheel.
- Apache Arrow `flight-sql-jdbc-driver` 19.0.0.

Every dependency is locked by its ecosystem manifest. The compatibility table
is maintained in `docs/connectivity-compatibility.md`.

## JDBC compatibility fix

The first Arrow JDBC execution exposed that the driver prepares even an
ordinary JDBC `Statement`. It uses the dataset schema returned by
`CreatePreparedStatement` to determine whether the statement returns rows.

qcli therefore gained an asynchronous adapter `prepare` contract returning
best-known dataset and parameter schemas. The gateway records those schemas in
the existing owner/session-bound prepared registry and Flight returns them as
standard IPC schemas. The deterministic adapter provides exact schemas without
executing SQL at prepare time.

Stateless clients may create a prepared statement with `qcli-target`. qcli
creates a private logical session owned by that handle and removes the session
when the statement closes. Existing cookie sessions continue unchanged.

qcli deliberately does not execute SQL while preparing, manufacture placeholder
columns, interpolate parameters, or rewrite arbitrary SQL into schema probes.
Consequently JDBC is supported for the demo adapter and explicitly not yet for
the three warehouse adapters, whose current client libraries lack a proven
non-executing schema-preparation operation. This boundary is visible in the
published matrix rather than hidden behind partial behavior.

## Automated profiles

The profiles demonstrate:

- bearer authentication and invalid-token rejection;
- explicit target routing through lowercase gRPC metadata;
- statement execution and exact row counts;
- Arrow schemas and typed prepared values;
- Flight SQL catalog/schema/table metadata;
- JDBC prepared statements and cancellation;
- deterministic close and resource cleanup;
- Rust dynamic loading of the native Flight SQL library through the ADBC C ABI.

The deeper M15–M18 protocol tests continue to cover replay, tickets, session
ownership, expiry, nested and batched values, update counts, TLS, proxy policy,
and shutdown.

## CI and release gates

`CI / ADBC and JDBC conformance` installs each client into a clean Ubuntu job,
starts the release-mode qcli binary, and runs all five profiles.

`Release / Packaged ADBC and JDBC conformance` downloads and extracts the
`x86_64-unknown-linux-gnu` release artifact. GitHub publication depends on that
job succeeding.

`Live connectivity certification` is manually dispatched with protected
configuration and runs the four ADBC bindings against Trino, Databricks SQL,
and Snowflake independently. It never serializes secrets into artifacts.

## Local verification completed

The following passed against qcli on localhost:

```text
python-adbc: PASS
go-adbc: PASS
java-adbc: PASS
rust-c-driver-manager-adbc: PASS
arrow-jdbc: PASS
```

Full repository validation remains:

```text
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

## Next boundary

M20 selects and certifies an ODBC driver and representative BI workflow. It must
reuse this compatibility policy: explicit versions, clean packaged clients,
honest target labels, and no claim beyond the tested matrix.
