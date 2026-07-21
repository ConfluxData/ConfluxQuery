# ADR-002: Adopt trino-rust-client Behind the Trino Adapter

Status: Accepted for Milestone 4

Date: 2026-07-21

## Context

qcli needs native SQL pass-through, remote query IDs, paginated streaming,
progress, session properties, structured errors, and cancellation. Those
features are protocol concerns and must remain visible to the engine adapter.

The Trino client protocol submits SQL with `POST /v1/statement`, follows
`nextUri` pages with `GET`, and cancels by query ID.
Query results contain the remote ID, columns, data, statistics, errors, and the
next page URI. Trino session context is carried in `X-Trino-*` headers.

## Decision

Use `trino-rust-client` 0.11 as the protocol and transport implementation,
behind `qcli-driver-trino`. Specifically:

- Use the public low-level `Client::get`, `Client::get_next`, and
  `Client::cancel` APIs so qcli retains query IDs, page statistics, and explicit
  lifecycle control.
- Keep qcli's Arrow conversion, common events, bounded result delivery,
  structured error mapping, and retry policy in the adapter.
- Apache Arrow as the adapter's output boundary.
- Basic authentication and bearer/JWT token authentication in the first cut.
- Direct protocol pages rather than the optional spooling protocol initially.

The client owns request construction, authentication, Trino session headers,
response-driven session updates, pagination requests, TLS, and cancellation.
The adapter owns qcli policy and conversion. Core and frontends interact only
through `EngineAdapter` and common query events.

The higher-level `RowStream` API is deliberately not used yet because it
exposes columns and rows but not the remote query ID or final statistics needed
by qcli's terminal and future HTTP API.

## Security policy

- Credentials are never sent over plain HTTP.
- TLS certificate verification defaults to enabled.
- Disabling verification requires the explicit target property
  `tls_verify=false` and is intended only for controlled development systems.
- Secrets remain redacted by the configuration layer and are not included in
  adapter errors.
- Redirect behavior currently follows the client's HTTP implementation. A
  configurable or injectable HTTP client is an upstream enhancement requested
  before qcli can enforce a no-redirect policy itself.

## Consequences

Benefits:

- Full visibility into pagination, query IDs, statistics, and cancellation.
- Reuses tested Trino request headers, authentication, session mutation, TLS,
  cancellation, and protocol models.
- Low-level page APIs preserve the metadata qcli needs.
- Engine behavior remains isolated in one adapter crate.
- Deterministic protocol tests can exercise exact HTTP requests and responses.

Costs:

- qcli depends on the client's release compatibility and Rust 1.86 MSRV.
- Authentication methods beyond Basic and bearer/JWT need later additions.
- The spooling protocol needs a separate future implementation.
- Response-session mutations are retained inside the client; exposing a session
  snapshot to qcli's shared session state is a later integration.
- qcli retains a small retry wrapper because its transient-status policy is
  broader than the client currently provides on the low-level page calls.

## Fallback

If the client cannot support a required protocol feature, retain the adapter
contract and replace only its internal client integration. No core, frontend,
or output API should need to change.
