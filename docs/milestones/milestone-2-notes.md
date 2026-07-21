# Milestone 2 Notes: End-to-End Query with a Demo Adapter

Status: Complete

Completed: 2026-07-21

## Demonstrated outcome

qcli now executes a deterministic query through the same architectural layers intended for real engines:

```text
CLI -> SessionManager -> immutable SessionSnapshot -> QueryService
    -> EngineAdapter -> bounded Arrow RecordBatch -> qcli-output
```

Demo command:

```text
$ qcli --config examples/milestone-2.env \
    --target demo --command "select * from sample"
┌────┬──────────────┬─────────┐
│ id │ name         │ amount  │
├────┼──────────────┼─────────┤
│ 1  │ alpha        │ 123.457 │
│ 2  │ beta-name-t… │ NULL    │
└────┴──────────────┴─────────┘
2 rows
Query ID: qcli_0000000000000001
Engine query ID: demo-qcli_0000000000000001
```

Failure demo:

```text
$ qcli --config examples/milestone-2.env --target demo --command fail
error: examples/milestone-2.env: driver error: demo_failure: requested deterministic failure
```

## Delivered

- `qcli-driver-api` object-safe asynchronous adapter contract.
- `qcli-core` reusable session and query orchestration.
- `qcli-driver-demo` deterministic adapter.
- `qcli-output` engine-independent human renderer.
- Apache Arrow 59.1 as the common typed batch boundary.
- Tokio-based asynchronous adapter execution.
- Bounded event and result channels with backpressure.
- Versioned logical sessions and immutable execution snapshots.
- Optimistic session mutation conflicts.
- Adapter registration by engine identity.
- qcli and engine query IDs.
- Lifecycle and row-production events.
- Cooperative cancellation with deterministic coverage.
- Structured driver error code and safe message.
- Arrow integer, UTF-8, nullable, and Decimal128 demonstration values.
- Unicode table rendering, visible string truncation, and half-even decimal rounding.
- CLI query execution with `--target` and `--command`.
- Deterministic success and failure integration tests.

## Automated evidence

`cargo test --workspace` passes 15 tests across configuration, executable integration, core lifecycle, demo adapter, and output behavior.

`cargo clippy --workspace --all-targets -- -D warnings` passes with no warnings.

`cargo fmt --all -- --check` passes.

Core tests prove:

- Sessions start at version one and mutations increment the version.
- Stale writes fail with a version conflict.
- Earlier snapshots remain unchanged after mutation.
- A query produces a bounded Arrow batch.
- Lifecycle states arrive in the expected order.
- Cancellation produces a structured cancelled outcome.
- Frontends interact with a trait object rather than a concrete adapter.

CLI tests prove inherited display settings, decimal rounding, visible truncation, NULL display, both query identifiers, and structured failure propagation.

## Reusability boundaries exercised

- `qcli-core` has no dependency on the CLI, REPL, HTTP, or a production engine.
- `qcli-driver-api` has no frontend or output dependency.
- The demo adapter produces typed batches but never renders them.
- The output crate renders batches without knowing their engine.
- The CLI composes adapters and core services without demo protocol logic.
- Adding another adapter does not require changing query lifecycle code.

## Known limitations

- Only the deterministic demo engine executes queries.
- The CLI currently renders table output only.
- Batch/file/stdin execution and machine formats belong to Milestone 3.
- Query IDs are process-local counters rather than globally unique IDs.
- Session storage is in memory and uses a standard mutex.
- Cancellation is cooperative; real adapters must map it to remote cancellation.
- `Cancelling` exists in the state vocabulary, but the demo emits the confirmed terminal `Cancelled` state rather than a separate transition event.
- The demo returns one batch; high-volume multi-batch behavior belongs to Milestone 3.
- The renderer supports only types used by this demo.
- Driver errors currently share the configuration-oriented CLI exit path; stable automation exit codes belong to Milestone 3.
- Query timing and metrics are not rendered yet.

## Prerequisites established for Milestone 3

- Arrow batches provide the exact machine-value source.
- Result delivery is bounded and asynchronous.
- Display transformations are isolated in `qcli-output`.
- Query execution is independent of output format.
- The demo adapter generates deterministic success, failure, and cancellation.

Milestone 3 can add table, vertical, CSV, TSV, JSON, and JSONL output; query files and stdin; stable stream separation and exit codes; and million-row bounded-memory evidence without redesigning sessions or adapters.
