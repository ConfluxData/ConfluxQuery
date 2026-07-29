# Milestone 14 Notes: Flight SQL Foundation

Status: Complete

## Outcome

Milestone 14 adds an authenticated Arrow Flight SQL listener to global
`qcli serve`. It establishes the protocol, transport, identity, health,
capability, and lifecycle boundaries required for query streaming in Milestone
15.

This milestone intentionally does not execute SQL over Flight. It implements
discovery and honest server information, then returns standard `UNIMPLEMENTED`
responses for statement execution and later Flight SQL capabilities.

## Client and protocol choice

qcli uses the official Apache Arrow Rust implementation:

```text
arrow-flight 59.1.0, feature flight-sql
tonic 0.14.x
tonic-health 0.14.x
```

This was selected because:

- The workspace already uses Apache Arrow 59.1.0 record batches.
- `FlightSqlService` performs the standard Flight command dispatch.
- Apache supplies the corresponding Rust Flight SQL client used by the
  conformance tests.
- Tonic provides HTTP/2, gRPC status handling, TLS, message limits, deadlines,
  keepalive, health integration, and graceful shutdown.
- Future query results can use `FlightDataEncoderBuilder` without a JSON or row
  conversion boundary.

qcli implements the service hooks rather than maintaining its own Flight SQL
protobuf fork.

## Crate boundary

The new `qcli-flight-sql` crate owns:

- Flight and Flight SQL gRPC protocol handling.
- Authentication interception and gRPC metadata policy.
- SQL information schemas and Arrow encoding.
- Tonic listener configuration.
- Direct TLS configuration.
- gRPC health.
- Stable mapping from qcli service errors to gRPC status codes.

It receives a clone of `GatewayService`. It does not own engine adapters,
sessions, query registries, quotas, results, or audit state.

```text
Flight SQL client
       |
       v
qcli-flight-sql
├── gRPC authentication interceptor
├── FlightSqlService dispatch
├── GetSqlInfo / DoGet
├── health
└── transport policy
       |
       v
shared GatewayService
```

## Global serve mode

HTTP remains enabled at its existing default address. Flight SQL is opt-in:

```text
qcli serve \
  --auth-file ~/.qcli/http-auth.toml \
  --flight-bind 127.0.0.1:32010
```

Both listeners:

- Use the same loaded configuration and engine adapter registry.
- Hold clones of the same `GatewayService`.
- Use the same `Authenticator`.
- Observe one Ctrl-C shutdown signal.
- Stop as one serve invocation.
- Share service shutdown, cancellation, retention, quota, and audit state.

Flight cannot be enabled without `--auth-file`.

## Authentication

The Flight gRPC interceptor accepts:

```text
authorization: Bearer qcli_k1_...
```

It validates the credential through the same `Authenticator` contract used by
HTTP and stores the resulting `AuthenticatedPrincipal` in the Tonic request
extensions. Future session and query handlers consume that principal rather
than reparsing credentials.

The authentication contract now exposes an immediate validation path for
metadata interceptors. API-key verification uses it. Future providers that
require external asynchronous exchange can override the asynchronous path and
must define an appropriate Flight-compatible validation strategy.

Missing and invalid credentials return `UNAUTHENTICATED`. Authentication
configuration failures return `INTERNAL` without revealing credential
material.

The standard Flight handshake accepts an already authenticated bearer token and
returns it in the response authorization metadata. Direct bearer metadata is
also supported without a handshake.

## Minimal SQL information

`GetFlightInfo(CommandGetSqlInfo)` returns one endpoint with the
specification-defined SQL-info Arrow schema. `DoGet` returns only requested
information codes.

M14 reports:

| SQL info | Value |
|---|---|
| Server name | `qcli` |
| Server version | qcli package version |
| Arrow format version | `1.3` |
| Read only | `false` |
| SQL execution | `false` until M15 |
| Substrait | `false` |
| Flight transaction API | `NONE` |
| Cancellation action | `false` until implemented |
| Bulk ingestion | `false` |

These values are deliberately conservative. qcli does not advertise a feature
before its protocol and conformance tests exist.

## Unsupported operations

The Apache `FlightSqlService` defaults produce standard `UNIMPLEMENTED` status
for:

- Statement queries.
- Prepared statements.
- Updates.
- Transactions and savepoints.
- Metadata not introduced until M17.
- Bulk ingestion.
- Substrait plans.

Authentication is applied before Flight SQL dispatch, so unsupported endpoints
cannot be used to bypass caller validation.

## Transport security

Loopback plaintext is allowed for local development. A non-loopback Flight bind
fails unless one of the following is configured.

### Direct TLS

```text
qcli serve \
  --auth-file ~/.qcli/http-auth.toml \
  --flight-bind 0.0.0.0:32010 \
  --flight-tls-cert /path/to/server-chain.pem \
  --flight-tls-key /path/to/server-key.pem
```

Tonic/rustls provides TLS with HTTP/2 ALPN. The certificate and private key must
both be supplied and readable.

### Trusted gRPC proxy

```text
qcli serve \
  --auth-file ~/.qcli/http-auth.toml \
  --flight-bind 0.0.0.0:32010 \
  --flight-trusted-proxy
```

Every Flight request must then contain:

```text
x-forwarded-proto: https
```

Forwarded metadata is rejected when trusted-proxy mode is disabled. Direct TLS
and trusted-proxy mode are mutually exclusive to keep the deployment contract
unambiguous.

Health is intentionally available without warehouse/API credentials so
orchestrators can determine whether the gRPC process is serving.

## Operational limits

The Flight server config sets:

- Maximum decoded message size: 16 MiB.
- Maximum encoded message size: 16 MiB.
- Request timeout: 60 seconds.
- HTTP/2 keepalive interval: 30 seconds.
- HTTP/2 keepalive timeout: 10 seconds.
- TCP keepalive.

Oversized messages fail before application dispatch. These are code-level M14
defaults; deployment configuration can be exposed later without changing the
transport boundary.

## Stable service-error mapping

The Flight frontend defines this mapping for future handlers:

| Service error | gRPC status |
|---|---|
| Invalid argument | `INVALID_ARGUMENT` |
| Not found | `NOT_FOUND` |
| Forbidden | `PERMISSION_DENIED` |
| Version conflict | `ABORTED` |
| Quota exceeded | `RESOURCE_EXHAUSTED` |
| Failed precondition | `FAILED_PRECONDITION` |
| Upstream unavailable | `UNAVAILABLE` |
| Internal failure | `INTERNAL` |

Engine-specific details remain in the structured service/query model and must
not leak credentials through gRPC messages.

## Demo and verification

Start both listeners:

```text
qcli serve \
  --auth-file ~/.qcli/http-auth.toml \
  --flight-bind 127.0.0.1:32010
```

Run the deterministic official-client profile:

```text
cargo test -p qcli-flight-sql
```

The tests prove:

- A valid bearer can call `GetSqlInfo` and fetch the Arrow result through
  `DoGet`.
- Missing credentials return `UNAUTHENTICATED`.
- Statement execution returns `UNIMPLEMENTED`.
- Health is available and reports `SERVING`.
- Forwarded metadata fails closed outside trusted-proxy mode.
- Oversized commands are rejected at the gRPC boundary.
- Non-loopback plaintext is rejected.
- Contradictory or unreadable TLS configuration is rejected.

The qcli binary test also starts HTTP and Flight listeners over one
`GatewayService`, connects to both sockets, triggers one coordinated shutdown,
and verifies both servers exit cleanly.

Full release-gate verification:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Known M14 boundaries

- No statement query execution over Flight SQL.
- No Flight session options or cookies.
- No catalog/schema/table metadata.
- No prepared statements.
- No Flight cancellation action.
- No ingestion.
- No mTLS or OIDC.
- No distributed service state.

These are sequenced explicitly into later milestones rather than partially
advertised.

## Next milestone

Milestone 15 submits `CommandStatementQuery` through `GatewayService`, creates
opaque owner-bound Flight tickets, and streams exact Arrow batches through
`DoGet` with bounded memory and backpressure across Trino, Databricks SQL, and
Snowflake.
