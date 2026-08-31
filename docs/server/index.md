# ConfluxQuery Gateway

ConfluxQuery Gateway is a governed, Arrow-native query access layer for
applications and data tools. Start it with `qcli serve`. HTTP and Flight SQL
share one runtime, so identity, target authorization, sessions, queries,
results, cancellation, quotas, audit, and cluster state behave consistently.

## Capability map

| Capability | HTTP | Flight SQL |
|---|---:|---:|
| API key and OIDC bearer auth | Yes | Yes |
| mTLS caller identity | Proxy policy | Direct Flight TLS |
| Stateless query | Yes | Header-selected statement |
| Persistent versioned session | Yes | Standard session actions/cookie |
| Target/context mutation | REST resources | Session options |
| Asynchronous status | Yes | Flight RPC lifecycle |
| JSON/CSV/NDJSON results | Yes | No; Arrow-native |
| Arrow streaming | Arrow response | Native |
| SSE query events | Yes | No |
| Cancellation | REST action | CancelFlightInfo/client API |
| Metadata | Via target/session workflows | Standard Flight SQL metadata |
| Prepared statements | Service internal/API evolution | Standard Flight SQL |
| Arrow ingestion | No | DoPut |
| OpenAPI/Swagger | Yes | Protocol-defined |

## Startup modes

### Local preview

```bash
qcli --config targets.env serve
```

Loopback HTTP, local principal, no Flight listener. Use only for development.

### Authenticated standalone

```bash
qcli --config targets.env serve \
  --bind 127.0.0.1:8088 \
  --auth-file auth.toml \
  --flight-bind 127.0.0.1:32010
```

All state and retained results are process-local. Restarting the node removes
them. This is a valid production topology when one node and bounded downtime
meet the service objective.

### Clustered

```bash
QCLI_CLUSTER_URL='postgresql://qcli:...@postgres/qcli' \
QCLI_RESULT_STORE_URL='s3://qcli-results/prod' \
QCLI_NODE_ID='gateway-a' \
QCLI_FLIGHT_SIGNING_KEY='/run/secrets/flight-signing-key' \
qcli --config targets.env serve \
  --bind 0.0.0.0:8088 --trusted-proxy \
  --auth-file auth.toml \
  --flight-bind 0.0.0.0:32010 --flight-trusted-proxy
```

PostgreSQL stores mutable coordination; the object store retains immutable
Arrow data. Nodes share signing material so Flight cookies and tickets remain
valid across load balancing.

## Health and shutdown

- `GET /health/live` returns 204 while the process is alive.
- `GET /health/ready` returns 204 until draining starts, then 503.

Both are intentionally unauthenticated and disclose no configuration. On
SIGTERM causes ConfluxQuery Gateway to withdraw readiness, drain listeners,
stop admitting work, and
allows in-flight cleanup within the deployment grace period.

## Resource controls

Principals have maximum sessions and concurrent queries. Service limits bound
retained result bytes/rows, pagination, request size, stream concurrency,
prepared handles, ingestion, and expiry. Cluster mode makes quota permits
distributed rather than per-node.

## Observability

The default audit sink emits structured `qcli_audit` JSON to stderr. Correlate
principal, target, session ID, qcli query ID, and native query ID. Capture
stdout/stderr through the service manager, but preserve redaction and do not
enable raw SQL logging in a shared environment.
