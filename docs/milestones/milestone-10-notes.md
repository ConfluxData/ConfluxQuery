# Milestone 10 Notes: Local HTTP Query Service

Status: Complete

Completed: 2026-07-23

## Demonstrable outcome

qcli now exposes its shared session and query core through a versioned,
loopback-only HTTP preview:

```text
qcli serve
qcli serve --bind 127.0.0.1:18088
```

API discovery:

```text
http://127.0.0.1:8088/docs/
http://127.0.0.1:8088/openapi.json
```

Swagger UI is embedded in the executable and does not depend on a CDN. The
OpenAPI 3 contract is generated from the Rust request/response DTOs and route
annotations using `utoipa`, so the browser documentation and running handlers
share one source tree.

The server calls `SessionManager`, `QueryService`, and the registered engine
adapters directly. It never shells out to the qcli executable.

Create a persistent session:

```text
curl -X POST http://127.0.0.1:8088/v1/sessions \
  -H 'content-type: application/json' \
  -d '{"target":"trino-local","context":{"catalog":"tpch","schema":"tiny"}}'
```

Submit and observe a query:

```text
curl -X POST http://127.0.0.1:8088/v1/sessions/SESSION_ID/queries \
  -H 'content-type: application/json' \
  -d '{"sql":"select * from nation limit 10"}'

curl http://127.0.0.1:8088/v1/queries/QUERY_ID
curl http://127.0.0.1:8088/v1/queries/QUERY_ID/events \
  -H 'accept: text/event-stream'
curl 'http://127.0.0.1:8088/v1/queries/QUERY_ID/results?limit=100'
```

## Implemented API

```text
POST   /v1/sessions
GET    /v1/sessions/{session_id}
PATCH  /v1/sessions/{session_id}
POST   /v1/sessions/{session_id}/target
PATCH  /v1/sessions/{session_id}/properties
PATCH  /v1/sessions/{session_id}/options
DELETE /v1/sessions/{session_id}

POST   /v1/sessions/{session_id}/queries
POST   /v1/queries
GET    /v1/queries/{query_id}
GET    /v1/queries/{query_id}/results
GET    /v1/queries/{query_id}/events
POST   /v1/queries/{query_id}/cancel
```

Session mutations use `expected_version` and return HTTP 409 when the caller
supplies a stale version. Query submissions capture immutable session snapshots,
so later mutations cannot affect running or completed queries. Stateless query
submissions create an ephemeral core session and close it after execution.
Engine-returned session properties are accumulated during execution and applied
with one optimistic version update before the query becomes terminal. A
concurrent caller mutation is never overwritten.

## Results and events

`GET /results` supports:

- `application/json`
- `application/x-ndjson`
- `text/csv`
- `application/vnd.apache.arrow.stream`

The `limit` parameter bounds page size. `x-qcli-next-page-token` carries an opaque,
integrity-checked continuation token. Machine results preserve exact values and
do not apply terminal decimal shortening or string truncation.

`GET /events` returns server-sent events with stable event IDs. Supplying
`Last-Event-ID` replays later retained events and continues with live progress
until a terminal state. Events cover state, engine query ID, progress, produced
rows, and session property changes. Sensitive property names are redacted.

## Bounded local retention

The preview defaults are:

- 128 retained queries.
- 1 MiB in-memory result threshold per query.
- Arrow IPC spill after the memory threshold.
- 64 MiB total retained result cap per query.
- 15-minute completed-result TTL.
- 1 MiB SQL request limit.
- 1,000 default and 10,000 maximum page rows.

Spill files use unique names in the operating-system temporary directory and are
removed when their retained query record expires or is dropped. Hitting a result
or query limit fails explicitly rather than retaining unbounded data.

## Local-preview security boundary

- Binding defaults to `127.0.0.1:8088`.
- M10 refuses every non-loopback bind.
- CORS is not enabled.
- Query IDs are random UUID-based identifiers.
- Target credentials and resolved connection properties are never returned.
- The server has one fixed local owner.

Authentication, per-caller ownership, target authorization, TLS/trusted-proxy
deployment, quotas by caller, and production audit integration belong to M11.

## Verification

Completed:

```text
cargo test -p qcli-http
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The HTTP suite covers session creation, optimistic mutation conflicts, closure,
session query execution, stateless execution, query status, cancellation,
pagination, invalid/opaque tokens, SSE replay and terminal events, JSON and CSV
results, Arrow-stream results, terminal/HTTP output parity, engine-returned
session state, memory-to-disk spill, spill cleanup, result caps, TTL expiry, and
refusal of non-loopback binding.

The generated OpenAPI contract test also verifies all version-one paths, the
primary session/query/error schemas, the `/openapi.json` response, and the
embedded Swagger UI page.

An end-to-end process demo using the deterministic demo adapter confirmed:

- HTTP 201 session creation.
- HTTP 202 asynchronous query submission.
- completed status with two retained rows.
- one-row pagination with an opaque continuation token.
- SSE delivery from submitted through completed.
- engine query ID propagation.

## Accepted limitations

- M10 is process-local and single-owner.
- Retention cleanup is opportunistic on API activity rather than a background
  sweeper.
- Spill storage is local Arrow IPC, not shared or distributed storage.
- Active queries continue when a persistent session is closed.
- The preview has no production authentication and therefore cannot bind beyond
  loopback.
