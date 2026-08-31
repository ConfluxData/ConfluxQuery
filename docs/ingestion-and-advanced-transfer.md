# Ingestion and advanced transfer

qcli implements the standard Flight SQL `CommandStatementIngest` `DoPut` path
through the shared gateway and a protocol-neutral adapter contract. Ingestion
is capability-driven: a target that does not advertise `bulk_ingest` is rejected
before its adapter can mutate warehouse state.

## Ingestion contract

An ingestion request carries:

- the authorized target and immutable session snapshot;
- optional catalog and schema plus a required table;
- create-if-missing behavior;
- fail, append, or replace behavior for an existing table;
- the standard temporary and transaction fields;
- string-valued backend options;
- a bounded stream of schema-consistent Arrow record batches;
- a cancellation signal.

The Flight frontend decodes at most two batches ahead of the adapter. A single
request is limited to 1,000,000 rows and 64 MiB of Arrow array memory. It rejects
mixed schemas before commit, cancels the adapter on decode or quota failure,
and returns the adapter's affected-row count in `DoPutUpdateResult`.

Transactions are not implemented and are rejected before data is consumed.
Temporary ingestion requires a persistent Flight session and additionally
requires native adapter support. qcli never implements create, append, replace,
or temporary tables through generic SQL text generation.

## Current target support

| Target | Create | Append | Replace | Temporary | Status |
|---|---:|---:|---:|---:|---|
| Deterministic demo | Yes | Yes | Yes | No | Supported for conformance |
| Trino | No | No | No | No | Capability-rejected |
| Databricks SQL | No | No | No | No | Capability-rejected |
| Snowflake | No | No | No | No | Capability-rejected |

The three warehouse adapters will advertise ingestion only after their native
write APIs pass type, atomicity, cancellation, retry, and partial-failure tests.
Connector-dependent Trino writes, generated SQL inserts, or driver-side value
interpolation are not treated as safe bulk ingestion.

## Multi-endpoint reads

Single-endpoint early streaming remains the default. A Flight client may ask
for retained-result partitioning with the request metadata header:

```text
qcli-result-partitions: 4
```

The accepted range is 1 through 16. Requests above one wait for query
completion, divide retained Arrow batches into contiguous partitions, and
return up to the requested number of `FlightEndpoint` values. Empty results
still receive one endpoint. Every endpoint carries a signed ticket containing
the principal, query, partition index, partition count, and expiry.

The endpoints are independent and may be consumed concurrently. FlightInfo
sets `ordered=false` for partitioned delivery because Flight SQL does not define
a global row order across endpoints. Clients that require SQL ordering should
use the default endpoint or apply an explicit downstream merge policy.

## Compression policy

qcli currently preserves Arrow IPC buffers and relies on HTTP/2 flow control;
it does not force gRPC or IPC compression. Compression can reduce network bytes
but consumes gateway and client CPU and can duplicate compression already
performed by Arrow buffers or the underlying warehouse protocol. A future
release may expose a negotiated codec only after per-type throughput, latency,
CPU, and memory benchmarks. Clients must not infer compression support today.

## Failure semantics

- Schema, command, transaction, and quota failures occur before adapter commit.
- Dropped or malformed upload streams signal cancellation and close the bounded
  channel.
- The demo adapter buffers an operation and swaps table state only after the
  complete stream succeeds, demonstrating atomic retry behavior.
- Native adapters must document whether their platform offers the same atomic
  boundary; weaker guarantees must be surfaced before the mode is enabled.
- Result partition tickets retain the existing principal ownership and expiry
  checks and cannot be exchanged between users.
