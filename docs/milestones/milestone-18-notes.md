# Milestone 18 — Prepared statements, parameters, and updates

Status: Complete

## Outcome

qcli now implements the standard Flight SQL prepared-statement lifecycle on a
protocol-neutral gateway registry. An authenticated client can create an opaque
session-bound statement, upload typed Arrow parameter batches with `DoPut`,
execute it as a query or update, and close it explicitly.

Parameter values are never converted into SQL text. The adapter receives the
original SQL and Arrow record batches as separate values. If an adapter cannot
bind those values natively, qcli returns an unsupported-capability error.

## Shared prepared-statement registry

Each registry entry contains:

- an opaque random handle;
- authenticated owner and logical session ID;
- original SQL text;
- parameter and best-known result schemas;
- immutable typed Arrow parameter batches;
- last-access time for expiry.

Handles are hidden from other principals as not found. Closing a session
removes its statements, explicit close prevents reuse, and idle entries expire
under `prepared_statement_ttl`. `max_prepared_statements` bounds the registry.
Execution resolves the current immutable snapshot of the owning session, so a
prepared statement cannot switch targets or escape session ownership.

## Adapter contract and capability boundary

The driver API separates:

- `prepared_statements` — lifecycle and execution of the original statement;
- `typed_parameters` — native Arrow parameter binding;
- `statement_updates` — native DDL/DML update counts.

It also provides distinct query and update methods for prepared execution. The
default implementation permits only zero-parameter prepared execution and
rejects typed values. There is deliberately no interpolation fallback.

The demo adapter implements all three capabilities and echoes bound batches for
deterministic type conformance. The Trino, Databricks SQL, and Snowflake
community clients used by qcli do not currently expose one uniform, proven
native typed-binding/update-count contract. Their adapters therefore advertise
prepared lifecycle only; typed binding and updates fail honestly until a native
implementation is added and tested.

## Flight SQL operations

Implemented standard operations:

- `CreatePreparedStatement` action;
- `CommandPreparedStatementQuery` through `GetFlightInfo` and `DoGet`;
- parameter upload through `DoPut`;
- `CommandPreparedStatementUpdate` with a standard record count;
- `CommandStatementUpdate` with a standard record count;
- `ClosePreparedStatement` action.

Transactions remain explicitly unsupported. Creation requires an authenticated
Flight session, and every later RPC revalidates principal ownership.

## Verification

The official Arrow Rust Flight SQL client performs the complete create, bind,
query, update, and close flow against a real localhost qcli Flight server.
Protocol-neutral tests cover:

- null and non-null values;
- decimal precision and scale;
- microsecond timestamps;
- binary values;
- nested list values;
- multiple parameter batches with an identical schema;
- exact Arrow batch preservation;
- prepared update counts;
- cross-principal access denial;
- reuse after explicit closure;
- idle expiry.

Run:

```text
cargo test -p qcli-service
cargo test -p qcli-flight-sql
cargo test --workspace
```

## Demo

Using the official Flight SQL client, open a qcli session for the demo target,
prepare `select ?`, bind a typed Arrow batch, execute it, and verify the returned
batch is type- and value-identical. Prepare an update, bind two parameter rows,
and verify the returned update count is `2`; then close both handles.

## Next boundary

M19 validates qcli with supported ADBC and JDBC client versions. It focuses on
client compatibility rather than adding another execution model: those clients
must traverse the same M18 registry, capability checks, and Flight SQL methods.
