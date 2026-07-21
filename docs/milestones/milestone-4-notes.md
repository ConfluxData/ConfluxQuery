# Milestone 4 Notes: Real Trino Execution

Status: Complete

Completed: 2026-07-21

## Demonstrated outcome

qcli now executes native SQL against Trino through the reusable core and
output pipeline:

```text
$ qcli --config examples/milestone-4.env target test trino-local
Target 'trino-local' is reachable (trino, 1 test row(s))
Engine query ID: 20260721_110618_00001_srbua

$ qcli --config examples/milestone-4.env \
    --target trino-local \
    --command "SELECT current_catalog, current_schema"
┌───────┬───────┐
│ _col0 │ _col1 │
├───────┼───────┤
│ tpch  │ tiny  │
└───────┴───────┘
1 rows
Splits: 1/1
Processed: 0 rows, 0 bytes
Time: 0.062s
```

The implementation uses `trino-rust-client` 0.11 to submit native SQL, retrieve
every result page, maintain Trino session headers, and cancel by query ID.

## Architecture decision

[ADR-002](../adr-002-trino-direct-http-protocol.md) selects
`trino-rust-client` behind qcli's engine-adapter boundary.

This keeps Trino-specific concerns in `qcli-driver-trino`:

- Client construction and qcli-specific connection policy.
- Low-level query-page traversal through the client.
- Trino type signatures.
- Remote errors, statistics, retry, and cancellation.

There are no Trino-specific branches in `qcli-core` or `qcli-output`. The CLI
registers available adapters and submits through the common trait.

## Delivered

- New reusable `qcli-driver-trino` crate.
- Native SQL body pass-through without parsing or rewriting.
- `trino-rust-client` 0.11 integration using its low-level page API.
- Native statement submission and `nextUri` pagination until completion.
- Query-ID cancellation through the client's public API.
- Remote Trino query ID events.
- Common progress snapshots for state, scheduling, splits, processed rows,
  processed bytes, and elapsed milliseconds when available.
- Final timing, split, and processed-data summaries on stderr.
- Generic `qcli target test TARGET` implemented with `SELECT 1`.
- Generic adapter registry containing demo and Trino adapters.
- Stable connection/authentication exit code `4`.
- Structured connection, timeout, authentication, protocol, query, type,
  cancellation, and consumer errors.
- Conservative retry for HTTP 502, 503, and 504.
- `Retry-After` support for HTTP 429.
- Basic authentication using `user` and `password`.
- Bearer/JWT authentication using `token`.
- HTTPS with certificate verification enabled by default through the client.
- Explicit `tls_verify=false` development override.
- Refusal to send password or token credentials over plain HTTP.
- Catalog, schema, source, user, client tags, and `session.*` propagation.
- Configured connection and request deadlines.
- Arrow conversion driven by Trino type signatures.
- Scalar boolean, integer, floating-point, character, binary, date/time,
  interval, JSON, IP address, and UUID mappings.
- Exact Decimal128 conversion.
- Typed arrays, maps, and row/struct conversion.
- Wider human rendering for Arrow scalar and nested types.

## Session property behavior

Every target property prefixed with `session.` is sent without the prefix in
the `X-Trino-Session` header. For example:

```ini
session.query_max_run_time = 30m
session.join_distribution_type = AUTOMATIC
```

becomes:

```text
X-Trino-Session: query_max_run_time=30m,join_distribution_type=AUTOMATIC
```

The immutable session snapshot guarantees that later session changes cannot
alter an already submitted query.

## Authentication and TLS policy

The initial supported modes are:

- Unauthenticated Trino with an explicit user header.
- HTTP Basic authentication with `user` and `password`.
- Bearer/JWT authentication with `token`.

Password and token modes require an `https://` URL. The client TLS stack uses
platform verification by default. Unit tests
verify both authorization header forms, reject mixed authentication, reject a
password without a user, reject credentials over HTTP, and confirm that errors
do not contain the secret.

## Type conversion evidence

The live Trino 483 query:

```sql
SELECT
  DECIMAL '12345678901234.123456' AS price,
  ARRAY[1, 2] AS items,
  MAP(ARRAY['a'], ARRAY[7]) AS labels,
  CAST(ROW(9, 'north') AS ROW(x bigint, y varchar)) AS point
```

produced exact JSONL:

```json
{"price":"12345678901234.123456","items":[1,2],"labels":{"a":7},"point":{"x":9,"y":"north"}}
```

The decimal remains a string in JSON according to qcli's exact-value policy.
Arrays, maps, and rows remain typed nested Arrow values through the output
boundary.

## Live container evidence

The live exit gate used the official `trinodb/trino:483` image with the TPCH
catalog. The container reported healthy before tests started.

Verified live paths:

- `target test` with a remote query ID.
- Catalog and schema context.
- Native SQL and exact complex values.
- Table and JSONL output.
- 100,000-row export from `tpch.sf1.lineitem`.
- Multiple result pages and bounded batch delivery.
- Final Trino split and processed-data metrics.
- Confirmed cancellation of a running `tpch.sf100000` aggregation.

The 100,000-row export completed with 21/21 splits and reported 19,723,105
processed bytes. The manual JSONL-to-sink run completed in approximately five
seconds on this development machine.

## Reproducible live tests

Start Trino:

```text
docker run --rm --name qcli-m4-trino -p 8080:8080 trinodb/trino:483
```

Run the standard CLI demonstrations with `examples/milestone-4.env`.

Run the opt-in live gates:

```text
QCLI_TRINO_URL=http://127.0.0.1:8080 \
  cargo test -p qcli-driver-trino \
  tests::live_trino_streams_multiple_result_pages -- --ignored --exact

QCLI_TRINO_URL=http://127.0.0.1:8080 \
  cargo test -p qcli-driver-trino \
  tests::live_trino_cancellation_is_confirmed -- --ignored --exact
```

Both live tests passed. The pagination gate received exactly 100,000 rows over
more than one page. The cancellation gate completed in about 0.12 seconds and
  received Trino's successful cancellation response.

## Client selection spike

Before adopting the dependency, qcli ran an isolated spike with
`trino-rust-client` 0.11 against Trino on `localhost:8080`. It verified:

- Exact column metadata and complex type signatures.
- Lazy retrieval of exactly 100,000 rows over multiple direct-protocol pages.
- Cancellation by remote query ID.
- Response-driven `USE tpch.sf1` and `SET SESSION` state persisted into the
  next query.
- Direct page access to query IDs, columns, statistics, errors, and `nextUri`.

The server was not configured to return spooled pages, so the spike verified
the client's spooling request/fallback behavior but not an end-to-end spooled
segment download.

We chose this client because it covers the error-prone Trino protocol and
session-header machinery while still exposing low-level pages. qcli deliberately
does not use `get_all` (unbounded materialization) or the high-level `RowStream`
(query ID and final statistics are not exposed). This leaves qcli in control of
bounded Arrow batches, progress, cancellation semantics, and frontend-neutral
events without duplicating the wire client.

After integration, both ignored live gates passed again on `localhost:8080`:
100,000-row multi-page streaming and confirmed cancellation.

## Upstream client improvements

No blocking defect was found for Milestone 4's direct protocol. The following
enhancements would let qcli remove workarounds or adopt more client features:

- Expose query ID and current/final statistics from `RowStream`, ideally as
  metadata accessors or page events.
- Provide separate connect and whole-request timeouts. qcli currently maps its
  configured deadline to the single client request timeout.
- Allow injecting/configuring the underlying HTTP client, especially redirect
  policy. qcli cannot currently enforce redirects-off through the public API.
- Extend low-level page retries to 429 (honoring `Retry-After`), 502, and 504.
  qcli currently supplies this thin policy around the client calls.
- Make spooling opt-in/opt-out explicit and return decoding errors instead of a
  panic/empty result from low-level `QueryResultData::into_vec`; remote spooled
  segments also need a low-level streaming path that preserves page metadata.
- Expose a read-only session snapshot so response-driven catalog, schema, role,
  and session changes can be synchronized with qcli's shared session state.

These are proposed upstream improvements, not reasons to fork the client. The
only qcli compatibility change required now is raising the workspace MSRV from
Rust 1.85 to 1.86, matching `trino-rust-client` 0.11.

## Automated evidence

The ordinary workspace suite covers:

- Exact native SQL request body.
- User, catalog, schema, source, and session headers.
- Two-page result retrieval.
- Remote query IDs and progress events.
- Decimal, array, map, and row conversion.
- Basic and bearer authentication construction.
- Credential transport safety and secret leakage.
- Transient gateway retry.
- Confirmed cancellation and DELETE semantics.
- Generic target testing and connection exit code `4`.

The two live Trino tests are ignored in the ordinary suite because they require
a coordinator URL. They were executed separately against the pinned container
for this milestone.

`cargo test --workspace` passes 31 regular tests. Three explicit release/live
gates are intentionally ignored by that command: the Milestone 3 million-row
gate and the two Milestone 4 Trino container gates.

`cargo clippy --workspace --all-targets -- -D warnings` passes.

`cargo fmt --all -- --check` passes.

## Backpressure issue found by the live gate

The first live `target test` found that publishing an awaited progress event on
every Trino page could fill the bounded event channel while a batch frontend
was draining result batches. That prevented terminal events from closing the
query stream.

Batch mode now emits the final progress snapshot only, while query identity and
lifecycle transitions remain guaranteed. Result pages remain backpressured by
the bounded batch channel. A future combined event/result stream or watch-style
progress channel can support high-frequency interactive refresh without this
contention.

## Known limitations

- OAuth browser flows, Kerberos, and client-certificate authentication are not
  included yet.
- The optional Trino spooling protocol is not requested; the direct protocol is
  used.
- Response-driven session mutations are persisted inside each client but are
  not yet surfaced into qcli's shared session snapshot.
- A cancellation requested before Trino returns `nextUri` is explicitly
  reported as `cancel_unconfirmed`.
- Batch mode reports the final progress snapshot rather than every intermediate
  page to protect bounded lifecycle delivery.
- Temporal values currently preserve Trino's exact textual representation in
  UTF-8 Arrow columns. Native Arrow temporal normalization can be added after
  timezone semantics are finalized.
- Non-string Trino map keys need broader live fixtures across supported Trino
  versions.
- TLS and authorization headers are covered deterministically; the milestone
  container itself used Trino's local unauthenticated HTTP development mode.

## Prerequisites established for Milestone 5

- Interactive queries can reuse the same Trino adapter and Arrow stream.
- Remote IDs, cancellation, progress, timing, and engine errors are exposed
  through frontend-neutral contracts.
- Session property headers already accept immutable session snapshots.
- The terminal can add target selection and context mutation without embedding
  Trino protocol logic.
