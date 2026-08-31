# Milestone 15 — Flight SQL query streaming

Status: Complete

## Outcome

qcli now accepts standard Flight SQL `CommandStatementQuery` requests and
returns exact Arrow record batches through `DoGet`. Flight, HTTP, and the CLI
all execute through `GatewayService`; the Flight frontend contains no
engine-specific protocol code. Consequently, every registered adapter—Trino,
Databricks SQL, Snowflake, and the deterministic demo adapter—uses the same
query lifecycle, ownership, quotas, audit events, cancellation signal, spill,
and retention policy.

## Demo

Start the global service:

```text
qcli serve \
  --auth-file ~/.qcli/http-auth.toml \
  --flight-bind 127.0.0.1:32010
```

A Flight SQL client must send:

- `authorization: Bearer <api-key>`
- `qcli-target: <target-section-name>`

The second header is the stateless M15 target selector. M16 replaces this
requirement for session-aware clients with standard Flight SQL session options.
The client executes SQL normally, receives one `FlightEndpoint`, and consumes
its ticket with `DoGet`.

## Protocol contract

### Submission

`GetFlightInfo(CommandStatementQuery)` creates a stateless shared-service query.
It waits only until the exact Arrow schema is known (or the query reaches a
terminal state), then returns:

- the original descriptor;
- one statement-query endpoint;
- an opaque signed ticket;
- exact Arrow schema;
- qcli query ID, engine query ID, and target in `app_metadata`;
- totals when already known, otherwise Flight's `-1` unknown value.

SQL is native pass-through. Transaction IDs are not interpreted in this phase.

### Tickets

Tickets contain a versioned JSON capability protected by HMAC-SHA-256 and
base64url encoding. They are:

- opaque to clients;
- bound to the authenticated principal;
- bound to one qcli query;
- tamper-evident;
- valid for 15 minutes by default;
- invalid after process restart because signing keys are process-local.

A bad format is `INVALID_ARGUMENT`, a bad signature or wrong owner is
`PERMISSION_DENIED`, and expiry is `NOT_FOUND`. The gateway independently
enforces result retention, so an unexpired ticket cannot resurrect an expired
result.

### Result streaming

`DoGet` waits for the shared query to become terminal and opens a sequential
result reader. In-memory batches share immutable Arrow buffers. Spilled Arrow
IPC files are opened once and read forward once; they are not rescanned for
every page. The encoder pulls one record batch at a time and splits FlightData
to the configured gRPC message limit. Tonic and the Arrow encoder therefore
propagate downstream backpressure without collecting the complete result in
the Flight frontend.

The schema placed in `FlightInfo` is also supplied to the stream encoder.
Arrow carries decimals, timestamps, null validity, nested values, and binary
data without converting them to display strings.

### Retry, replay, disconnect, and partial results

- A ticket may be replayed from row zero while both ticket and retained result
  remain valid.
- A client disconnect stops that `DoGet` reader. It does not re-execute or
  automatically cancel the shared query.
- Retry is replay, not query resubmission.
- A cancelled query may expose batches retained before cancellation.
- Failed queries return a stable mapped gRPC error rather than a partial
  successful stream.
- Result expiry follows the shared gateway retention TTL and removes spill
  files with the query record.

This is an at-least-once read contract: clients retrying a broken stream must
discard prior rows or deduplicate at the application boundary.

### Cancellation

The Flight SQL cancel-query action validates the embedded statement ticket and
calls the same `GatewayService::cancel` operation as HTTP. Cancellation remains
cooperative because the concrete adapter and warehouse decide when work
actually stops. The server advertises cancellation support in `GetSqlInfo`.

## Resource model

Large results use the existing shared-service threshold to move from memory to
an Arrow IPC spill file. Flight adds only a sequential reader, an Arrow encoder,
and bounded transport buffers. Operators must raise
`max_result_bytes_per_query` above its conservative default before a
multi-gigabyte workload is admitted; the limit remains deliberate protection,
not a streaming limitation.

## Verification

Automated tests cover:

- official Arrow Flight SQL client discovery;
- authenticated statement submission and `DoGet`;
- exact `FlightInfo`/record-batch schema equality;
- full replay of a ticket;
- opaque, tamper-evident, owner-bound tickets;
- missing authentication and missing target failures;
- message, proxy, listener, TLS, health, and shutdown policies;
- the protocol-neutral service result reader and existing spill lifecycle.

Run:

```text
cargo test -p qcli-service
cargo test -p qcli-flight-sql
cargo test --workspace
```

Live Trino, Databricks SQL, and Snowflake validation uses the same configured
targets and SQL profile as M9. Those tests require operator credentials and
running services; they are not exercised by credential-free CI.

## Deliberate boundaries

- M16 adds standard Flight session options and removes the need to attach
  `qcli-target` to every stateless request.
- M17 adds catalog and table metadata.
- Prepared statements and updates remain M18.
- Distributed ticket signing and cross-node result replay remain M23.
