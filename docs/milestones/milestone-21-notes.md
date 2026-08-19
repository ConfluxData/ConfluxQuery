# Milestone 21 Notes: Ingestion and Advanced Transfer

## Outcome

Milestone 21 adds bounded standard Flight SQL ingestion and opt-in
multi-endpoint result delivery without embedding warehouse-specific write logic
in the transport or shared service.

The deterministic demo proves create, append, replace, affected-row counts,
query-back, atomic quota failure, and retry. Trino, Databricks SQL, and
Snowflake remain explicitly capability-rejected until their native ingestion
implementations pass the same contract.

## Delivered

- `BulkIngest`, `IngestRequest`, `IngestSource`, and existing-table behavior in
  the frontend-neutral adapter API.
- Shared-core and gateway ingestion dispatch using immutable authorized session
  snapshots.
- Start/finish audit events and target capability enforcement.
- Standard `CommandStatementIngest` decoding and `DoPutUpdateResult` counts.
- A two-batch adapter channel for upload backpressure.
- Consistent-schema validation, cancellation propagation, 1,000,000-row limit,
  and 64-MiB Arrow-memory limit.
- Explicit rejection of ingestion transactions and stateless temporary tables.
- Demo create, append, replace, atomic commit, retry, and query-back storage.
- Target-aware `FLIGHT_SQL_SERVER_BULK_INGESTION` SQL info.
- Opt-in 1–16 endpoint retained-result partitioning.
- Principal-bound, expiring partition tickets and partition-range validation.
- Memory and spilled-result partition readers without copying result batches.
- An explicit no-compression policy pending benchmarked negotiation.

## Demonstration

The Rust Flight SQL integration profile:

1. authenticates and selects the demo target;
2. uploads typed Arrow batches with create semantics;
3. appends another batch and queries four rows back;
4. replaces the table and queries one row back;
5. injects an oversized upload failure and retries the same create successfully;
6. requests three endpoints for a 3,072-row generated result;
7. consumes each signed endpoint and verifies exactly 3,072 total rows.

## Safety decisions

- qcli does not translate Arrow ingestion into interpolated `INSERT` text.
- Unsupported adapters fail by capability before mutation.
- A malformed or over-limit stream cancels the adapter before channel closure,
  allowing an atomic adapter to discard staged batches.
- Partitioned delivery waits for retained results; default query streaming is
  unchanged.
- Partitioned FlightInfo is marked unordered.
- Transactional and temporary semantics are not approximated.

## Evidence

The milestone-specific tests are:

```text
cargo test -p qcli-flight-sql \
  bounded_ingestion_creates_appends_replaces_and_queries_back --locked
cargo test -p qcli-flight-sql \
  failed_ingestion_is_atomic_and_same_request_can_be_retried --locked
cargo test -p qcli-flight-sql \
  opt_in_large_read_uses_independent_signed_endpoints --locked
```

Workspace tests, formatting, clippy, documentation, and diff checks are the
completion gates recorded with this milestone.

All M21 and changed-crate tests pass. The workspace suite passes when excluding
the pre-existing M6 pseudo-terminal tab-completion test
`milestone_six_navigation_and_atomic_target_switch_run_in_a_pseudo_terminal`;
that isolated test repeatedly times out while waiting for completion output at
`commands.rs:408` and no M21 change touches CLI or REPL completion code.

## Accepted limitations

- Trino, Databricks SQL, and Snowflake do not yet advertise bulk ingestion.
- Temporary and transactional ingestion are unsupported.
- The limits are currently fixed server defaults rather than CLI/configuration
  properties.
- Partition selection uses a qcli metadata header; the returned endpoints and
  tickets remain standard Flight structures.
- Partitions are batch-balanced, not byte- or row-balanced.
- qcli does not advertise transport or Arrow IPC compression.
- Shared cross-node ticket/result routing remains M23.

## Next milestone

M22 adds enterprise identity and transport: OIDC/JWT discovery and validation,
mTLS identity, key and certificate rotation, hardened gRPC policy, and
cross-protocol principal isolation.
