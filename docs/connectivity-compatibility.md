# ADBC and JDBC connectivity

This document is the supported qcli client contract. A client/version pair is
supported only when it appears in the matrix and passes the clean-client CI
profile. Newer versions are unverified until that pin is deliberately updated.

## Compatibility matrix

| Client path | Pinned version | Demo CI | Trino | Databricks SQL | Snowflake |
|---|---:|---:|---:|---:|---:|
| Python ADBC Flight SQL | 1.12.0 | Supported | Live profile | Live profile | Live profile |
| Go ADBC Flight SQL | 1.12.0 | Supported | Live profile | Live profile | Live profile |
| Java ADBC Flight SQL | 0.24.0 | Supported | Live profile | Live profile | Live profile |
| Rust + ADBC C driver manager | 0.24.0 | Supported | Live profile | Live profile | Live profile |
| Apache Arrow Flight SQL JDBC | 19.0.0 | Supported | Not yet supported | Not yet supported | Not yet supported |

`Supported` means the profile is a required pull-request and packaged-release
gate. `Live profile` means the same client is exercised by the protected
`Live connectivity certification` workflow against the named target and is
certified for the engine versions recorded by that workflow run.

JDBC is currently supported for the deterministic demo adapter. Arrow JDBC
prepares ordinary `Statement` queries and requires a result schema at prepare
time. qcli now has an explicit native preparation-schema contract, but the
current Trino, Databricks, and Snowflake client libraries do not expose a safe
non-executing query-description operation. qcli does not execute a query during
prepare, invent a schema, or rewrite it into a vendor-specific probe. Those
three JDBC combinations remain unsupported until their adapters implement and
pass that contract.

The Python package uses the ADBC C driver manager and the Apache Go Flight SQL
driver. The separate Rust profile dynamically loads the packaged native Flight
SQL driver 1.12 through Apache `adbc_driver_manager`, directly exercising the C
ABI. The small `adbc-driver-flightsql` 0.1.2 community distribution crate only
locates and checksum-verifies Apache's official PyPI wheel; it is not treated as
an Apache Rust driver.

## Connection settings

### Common values

- Endpoint: the address passed to `qcli serve --flight-bind`.
- Plaintext URI: `grpc://host:port`.
- TLS URI: `grpc+tls://host:port`.
- Authentication: `authorization: Bearer <qcli API key>` on every RPC.
- Target: lowercase gRPC metadata header `qcli-target: <section-name>`.
- Session: optional standard Flight SQL session cookie. Stateless statements
  use `qcli-target`; prepared statements without a cookie receive a private,
  handle-owned logical session that is closed with the handle.
- Timeouts: set query, fetch, and update RPC deadlines in the client. The
  conformance profiles use ten seconds for deterministic operations.

### Python ADBC

Use `adbc_driver_flightsql.DatabaseOptions.AUTHORIZATION_HEADER` and prefix the
target header with `DatabaseOptions.RPC_CALL_HEADER_PREFIX`. The complete pinned
recipe is in `conformance/m19/python/profile.py`.

### Go ADBC

Use `flightsql.NewDriver`, `adbc.OptionKeyURI`,
`flightsql.OptionAuthorizationHeader`, and
`flightsql.OptionRPCCallHeaderPrefix + "qcli-target"`. Timeout values are
floating-point seconds, not Go duration strings.

### Java ADBC

Use `FlightSqlDriver` and set the URI with `AdbcDriver.PARAM_URI`. Authorization
and target use `FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX`.

### Rust/C driver manager

Load the Flight SQL shared library with
`ManagedDriver::load_dynamic_from_filename`, then set the standard URI plus
`adbc.flight.sql.authorization_header` and
`adbc.flight.sql.rpc.call_header.qcli-target` database options.

### Apache Arrow JDBC

The plaintext form is:

```text
jdbc:arrow-flight-sql://host:port/?useEncryption=false
```

Set `token` to the raw qcli API key and `qcli-target` to the target section.
For TLS use `useEncryption=true`, retain certificate verification, and configure
the documented trust store. Java 9 and newer require:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

## Conformance coverage

The credential-free profiles cover bearer rejection, target routing, query and
Arrow result types, SQL/object metadata, typed preparation, cancellation,
closure, and lifecycle cleanup. M15–M18 Rust protocol tests remain the deeper
wire-level suite for tickets, replay, sessions, metadata schemas, parameter
batches, update counts, errors, TLS, and shutdown.

The protected live workflow uses `SELECT 1 AS qcli_m19_value` and metadata
browsing through every pinned ADBC language binding. It consumes:

- `QCLI_LIVE_CONFIG`: complete protected qcli target configuration;
- `QCLI_LIVE_AUTH`: caller authentication file allowing all live targets;
- `QCLI_LIVE_TOKEN`: corresponding raw caller API key.

No warehouse or caller credential is stored in the repository or test output.

## Reproducing a packaged profile

Extract a qcli release archive, start its binary with the demo configuration and
authentication file, then run the client fixtures with:

```text
QCLI_FLIGHT_TOKEN=<key> python conformance/m19/python/profile.py
QCLI_FLIGHT_TOKEN=<key> go run ./conformance/m19/go
QCLI_FLIGHT_TOKEN=<key> mvn -q -f conformance/m19/java/pom.xml \
  exec:java -Dexec.mainClass=org.apache.qcli.JdbcProfile
QCLI_FLIGHT_TOKEN=<key> cargo run --locked \
  --manifest-path conformance/m19/rust/Cargo.toml
```

The release workflow performs this against the actual Linux archive before the
GitHub release may be published.
