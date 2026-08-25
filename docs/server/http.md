# ConfluxQuery Gateway HTTP API

Swagger UI at `/docs/` is the interactive reference; `/openapi.json` is the
machine contract. This page explains resource workflows.

All `/v1` requests use `Authorization: Bearer TOKEN` in authenticated mode.
Errors have a stable code and message. Session/query IDs are opaque.

## Stateless query

```bash
response=$(curl -sS -X POST "$QCLI_URL/v1/queries" \
  -H "Authorization: Bearer $QCLI_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"target":"trino-prod","sql":"select * from nation","context":{"catalog":"tpch","schema":"tiny"}}')
```

The response contains `id`, `session_id`, target, engine, state, and row count.
Poll `GET /v1/queries/{query_id}` until `completed`, `failed`, or `cancelled`.

## Persistent session

```bash
curl -sS -X POST "$QCLI_URL/v1/sessions" \
  -H "Authorization: Bearer $QCLI_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"target":"trino-prod","context":{"catalog":"hive","schema":"sales"},"properties":{},"options":{}}'
```

Session responses contain a monotonically increasing `version`. Mutations send
`expected_version`; stale writers receive conflict rather than overwriting a
newer change.

| Method/path | Purpose |
|---|---|
| `POST /v1/sessions` | Create a principal-owned session. |
| `GET /v1/sessions/{id}` | Read current version and target. |
| `PATCH /v1/sessions/{id}` | Update context/properties/options atomically. |
| `PATCH /v1/sessions/{id}/properties` | Update properties. |
| `PATCH /v1/sessions/{id}/options` | Update session options. |
| `POST /v1/sessions/{id}/target` | Atomically switch target. |
| `POST /v1/sessions/{id}/queries` | Submit SQL in the session. |
| `DELETE /v1/sessions/{id}` | Close session and owned ephemeral state. |

## Results

```bash
curl -sS "$QCLI_URL/v1/queries/$QUERY_ID/results?offset=0&limit=100" \
  -H "Authorization: Bearer $QCLI_TOKEN"
```

Content negotiation/query parameters support JSON-oriented paging plus CSV,
NDJSON, and Arrow stream paths described by OpenAPI. Always paginate; do not
assume an unbounded response. Results can expire after query completion.

## Events

```bash
curl -N "$QCLI_URL/v1/queries/$QUERY_ID/events" \
  -H "Authorization: Bearer $QCLI_TOKEN" \
  -H 'Last-Event-ID: 0'
```

SSE reports state, native query IDs, batches, rows, cancellation, failure, and
completion. `Last-Event-ID` supports bounded reconnection within retention.

## Cancellation

```bash
curl -sS -X POST "$QCLI_URL/v1/queries/$QUERY_ID/cancel" \
  -H "Authorization: Bearer $QCLI_TOKEN"
```

The request is idempotent at the service boundary. Engine confirmation depends
on adapter capability.

## Browser access

CORS is disabled unless exact origins are supplied with repeated
`--cors-origin`. Wildcard credentialed access is not enabled. Preflight allows
the documented methods and `Authorization`, `Content-Type`, and
`Last-Event-ID` headers.
