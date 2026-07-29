# Milestone 13 Notes: Shared Service Runtime

Status: Complete

## Outcome

Milestone 13 extracted qcli's stateful gateway behavior from `qcli-http` into
the protocol-neutral `qcli-service` crate. HTTP now delegates to one canonical
service runtime that can also be used directly by the Flight SQL frontend
planned for Milestone 14.

The extraction preserves the existing HTTP API and does not change engine
adapters, SQL execution semantics, OpenAPI routes, authentication format, or
result representations.

## Architecture delivered

```text
HTTP routes and middleware
        |
        v
qcli-service
├── caller authorization context
├── owned, versioned sessions
├── target authorization
├── query admission and quotas
├── canonical query registry
├── Arrow result retention and disk spill
├── replayable lifecycle events
├── cancellation
├── session and result expiry
├── audit events
└── graceful shutdown coordination
        |
        v
qcli-core -> engine adapter API
```

`qcli-http` continues to own transport concerns:

- Bearer extraction and HTTP authentication middleware.
- Trusted-proxy, HTTPS-forwarding, and CORS enforcement.
- HTTP request and response DTOs.
- HTTP status mapping from structured service errors.
- Content negotiation and output serialization.
- Opaque HTTP pagination tokens.
- SSE serialization.
- OpenAPI and Swagger UI.
- Listener binding and Axum server orchestration.

It no longer owns session maps, query records, result storage, quotas, expiry,
audit storage, or shutdown state.

## New crate

`crates/qcli-service` exposes `GatewayService`, the cloneable entry point for
all stateful gateway operations.

Important public contracts include:

- `ServiceLimits`
- `ServiceError` and `ServiceErrorKind`
- `QueryStatus`
- `QueryError`
- `ServiceEvent`
- `ResultPage`
- `AuditEvent` and `AuditSink`

The service accepts an `AuthenticatedPrincipal` supplied by a transport. It
performs target authorization, ownership checks, and quota decisions itself.
This allows Flight SQL to authenticate through gRPC middleware later while
reusing exactly the same policy decisions.

## Session contract

The shared service provides:

- Create an owned session for an authorized target.
- Retrieve and touch a session.
- Apply version-checked option changes.
- Atomically switch target.
- Close a session.
- Expire inactive sessions.

Cross-principal session access returns the same not-found response as a missing
session, preventing resource enumeration. Existing immutable
`SessionSnapshot` behavior remains in `qcli-core`.

## Query contract

The shared service provides:

- Session-bound and stateless submission.
- SQL-size and retained-query admission limits.
- Per-principal concurrent-query quotas.
- Query status and normalized errors.
- Owner-bound cancellation.
- Replayable lifecycle event history and live subscriptions.
- Bounded Arrow result retention.
- Transparent Arrow-file spill after the memory threshold.
- Offset/limit result pages independent of transport encoding.
- Result expiry and spill-file cleanup.

Stateless submissions use a temporary owned session that is closed after the
query reaches a terminal state. Session-bound queries apply engine-reported
session property changes using the original session version.

## Error boundary

`qcli-service` returns transport-independent error kinds:

```text
InvalidArgument
NotFound
Forbidden
Conflict
ResourceExhausted
FailedPrecondition
Upstream
Internal
```

The HTTP frontend maps these to its existing status contract. A future Flight
SQL frontend will map the same errors to gRPC status codes without importing
Axum or HTTP response types into the service.

## Audit and shutdown

Audit events moved into the shared service so session creation, target denial,
query submission, cancellation, deletion, and expiry use one event model across
protocols. HTTP authentication attempts also write through the service audit
sink.

Shutdown is coordinated by the service:

1. Mark the runtime unavailable for new mutable work.
2. Cancel active queries.
3. Let the HTTP listener drain.
4. Wait for query collectors up to the configured grace period.

Flight SQL can join the same sequence in Milestone 14.

## Demo

Run the deterministic service and HTTP tests:

```text
cargo test -p qcli-service
cargo test -p qcli-http
```

The `http_and_direct_service_machine_results_match` HTTP test executes the same
demo query through:

1. The HTTP stateless query and result endpoints.
2. The direct `GatewayService` API.

Both paths must produce identical JSON machine output. The direct service tests
also demonstrate Arrow pagination, owned/versioned sessions, cancellation,
event replay, retention expiry, and shutdown admission behavior.

The existing server remains runnable as before:

```text
qcli serve
```

Swagger remains available at:

```text
http://127.0.0.1:8088/docs/
```

## Verification

Completed verification:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

The full workspace suite passed. Live tests requiring separately configured
Trino, Databricks, or Snowflake services remain ignored by their existing
explicit gates.

## Design consequences

- Flight SQL can reuse canonical session/query state without depending on
  `qcli-http`.
- HTTP and Flight SQL cannot accidentally maintain conflicting query
  registries.
- Result batches remain Arrow-native until a transport chooses an encoding.
- Transport-specific pagination and streaming framing remain outside the
  service.
- Service errors can be mapped independently to HTTP or gRPC.
- The service is process-local in M13. Distributed state remains a later
  milestone.

## Next milestone

Milestone 14 adds the Flight SQL protocol foundation and secure listener to the
global `qcli serve` runtime. It should consume `GatewayService` rather than
creating new session, query, quota, result, or audit state.
