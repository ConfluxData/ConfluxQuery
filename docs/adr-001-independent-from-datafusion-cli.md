# ADR 001: Build qcli independently of DataFusion CLI

Status: Accepted

Date: 2026-07-20

## Context

qcli is a query client for remote analytical platforms, initially Trino, Databricks SQL, and Snowflake. Its primary responsibilities are target management, authentication, native SQL execution, remote query progress and cancellation, metadata discovery, and consistent result presentation.

DataFusion CLI is primarily a shell for the embedded DataFusion query engine. It parses, plans, and executes queries locally against files, object stores, and tables registered in a DataFusion session.

Although both products expose an interactive SQL prompt, their execution models are different:

```text
DataFusion CLI: SQL -> local DataFusion planning and execution
qcli:           SQL -> selected remote platform -> remote execution
```

## Decision

We will build qcli as an independent CLI rather than fork or extend DataFusion CLI.

qcli will send native SQL to the selected engine without requiring DataFusion to parse or plan it. Each engine will be implemented behind a qcli-owned adapter responsible for authentication, query submission, status, cancellation, metadata, results, and engine-specific errors.

## Why

- DataFusion CLI does not provide Trino, Databricks SQL, or Snowflake connection and authentication support.
- DataFusion's session and catalog model represents local execution, not switchable remote targets and warehouses.
- Passing SQL through DataFusion could reject or alter valid engine-specific SQL, including native DDL, session commands, functions, hints, and platform extensions.
- Remote query IDs, queue state, progress, profile links, metrics, and cancellation must be handled directly by each platform adapter.
- Most qcli-specific functionality—sectioned configuration, target selection, target switching, metadata caching, authentication, and secret redaction—would still need to be built.
- Forking the existing CLI would couple qcli to an upstream application whose goals and internal structure differ from ours.
- Embedding the complete DataFusion engine would increase dependency size and complexity without solving qcli's main remote-client problems.

## What we may reuse

This decision does not exclude the DataFusion and Apache Arrow ecosystem.

qcli may reuse:

- Apache Arrow schemas, arrays, and record batches as its common result representation.
- Arrow CSV, JSON, and related serialization components where their behavior matches qcli's output contract.
- Ideas or suitably licensed implementation patterns from DataFusion CLI for terminal rendering, signal handling, and testing.
- DataFusion itself as a future optional `local` target for querying Parquet, CSV, JSON, Arrow, and object-store data.

DataFusion will not be the abstraction through which Trino, Databricks SQL, and Snowflake queries are executed.

## Consequences

Positive consequences:

- Native engine SQL remains fully available.
- qcli controls its target, session, query lifecycle, and output abstractions.
- Remote query cancellation and metrics remain direct and understandable.
- Engine adapters can expose capabilities without pretending all platforms behave identically.
- DataFusion can still be added later where local execution provides real value.

Costs:

- qcli must build and maintain its own REPL, meta-command routing, configuration, and output policy.
- Some terminal and formatting work may overlap with DataFusion CLI.
- Local file querying will not be available until a dedicated adapter is added.

## When to revisit

Reconsider this decision if cross-source local execution becomes a primary product requirement—for example, joining data from Trino, Snowflake, and local Parquet in one query. Even then, DataFusion should be evaluated as an optional federation layer rather than automatically replacing qcli's direct remote adapters.
