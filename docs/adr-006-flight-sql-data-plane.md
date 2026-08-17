# ADR-006: Arrow Flight SQL as the Remote SQL Data Plane

## Status

Accepted as the post-M12 service roadmap.

## Context

qcli exposes a production HTTP API, but HTTP-specific query/result endpoints do
not by themselves provide a standard database connectivity surface. Analytical
applications increasingly consume Arrow data through ADBC, while Java and many
BI tools expect JDBC or ODBC.

ADBC and Flight SQL occupy different layers:

- ADBC is a client API and driver model.
- Flight SQL is an Arrow-native network protocol over Flight/gRPC.
- Standard ADBC Flight SQL and Apache Arrow JDBC drivers can wrap a compliant
  Flight SQL endpoint.
- ODBC connectivity depends on a compatible third-party Flight SQL ODBC driver
  and must be verified rather than assumed.

Creating a qcli-specific ADBC driver would require distributing native driver
libraries and would embed qcli connectivity and credentials into every client
process. Replacing the existing backend adapters with ADBC would also sacrifice
current Trino, Databricks, and Snowflake protocol/authentication control because
backend-driver coverage and maturity are uneven.

## Decision

`qcli serve` becomes a global service runtime:

```text
HTTP       = control and operational plane
Flight SQL = standard SQL and Arrow data plane
qcli-service = shared protocol-neutral service layer
qcli-core    = session and engine execution core
```

qcli will implement a standards-compliant Flight SQL server over its existing
engine adapters. It will initially rely on standard ADBC Flight SQL and Apache
Arrow JDBC clients rather than publishing a custom qcli ADBC driver.

The implementation is full-scope but milestone-driven. It includes:

- shared HTTP/Flight authentication, authorization, ownership, quotas, audit,
  expiry, shutdown, and observability;
- signed session tokens and Flight tickets;
- streaming Arrow query results with backpressure and replay policy;
- complete relevant SQL metadata and accurate per-target `GetSqlInfo`;
- prepared statements, typed parameter binding, updates, and ingestion where
  adapters support them;
- target-native transactions only after correct adapter contracts exist;
- ADBC, JDBC, and approved ODBC conformance suites;
- direct/proxy TLS, JWT/OIDC, mTLS, and multi-node shared state in later
  milestones.

## Protocol mapping

```text
Flight SQL                         qcli
----------------------------------------------------------------
authorization metadata            Authenticator
session token/cookie               principal-owned SessionService
Set/Get/CloseSessionOptions        versioned session operations
GetFlightInfo statement query      QueryService submission
Flight ticket                      signed query/result capability
DoGet                              Arrow result stream
cancellation                       shared CancellationSignal
GetCatalogs/GetSchemas/GetTables   qcli-metadata
GetSqlInfo                         adapter capability registry
prepared statement handle          PreparedStatementService
```

HTTP and Flight SQL must never create separate canonical registries. Either
protocol can manage a query submitted through the shared service when ownership
and authorization permit it.

## Compatibility policy

- Native Flight SQL and selected ADBC Flight SQL clients are primary.
- Apache Arrow Flight SQL JDBC is supported after the JDBC conformance
  milestone.
- Flight SQL ODBC is experimental until a selected driver passes Windows,
  Linux, macOS, metadata, type, cancellation, and representative BI-tool tests.
- Unsupported transactions, typed parameters, updates, ingestion, or metadata
  are reported through standard capabilities and errors; they are not silently
  emulated. Prepared lifecycle support is independent from native binding and
  update-count support.

## Consequences

Positive:

- One server protocol opens qcli to Arrow-native, ADBC, JDBC, and compatible
  ODBC ecosystems.
- Arrow batches stream without HTTP JSON rendering.
- qcli retains engine-specific authentication, session, error, query ID, and
  cancellation behavior.
- New frontends continue to reuse one service/core boundary.

Costs:

- qcli becomes a gRPC/HTTP2 server with a substantially larger conformance and
  security surface.
- JDBC/ODBC compatibility requires exact metadata and type behavior, not merely
  statement execution.
- Native parameter binding, update counts, and ingestion require explicit
  adapter contracts and per-target conformance.
- High availability requires shared sessions, query leases, distributed quotas,
  and object-backed results.

## Rejected alternatives

### Replace qcli adapters with ADBC drivers

Rejected as the primary architecture because current multi-engine coverage,
authentication control, and capability parity do not meet qcli requirements.
ADBC-backed adapters may be added individually later.

### Publish only a custom qcli ADBC driver

Rejected initially because it does not provide a language-neutral network
service and creates native client packaging and credential-distribution burden.

### Replace HTTP with Flight SQL

Rejected because OpenAPI, administration, browser access, health, metrics, and
operational workflows remain better suited to HTTP. The protocols are
complementary.

### Emulate PostgreSQL wire protocol

Rejected because it would imply PostgreSQL semantics, lose Arrow-native
transport, and conflict with qcli's Trino/Databricks/Snowflake-centered model.

## References

- [Apache Arrow Flight SQL protocol](https://arrow.apache.org/docs/format/FlightSql.html)
- [Rust Arrow Flight SQL module](https://arrow.apache.org/rust/arrow_flight/sql/index.html)
- [ADBC Flight SQL driver](https://arrow.apache.org/adbc/current/driver/flight_sql.html)
- [Apache Arrow Flight SQL JDBC driver](https://arrow.apache.org/docs/java/flight_sql_jdbc_driver.html)
