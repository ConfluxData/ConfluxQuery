# Milestone 9 Notes: Unified Release Candidate

Status: Complete

Completed: 2026-07-23

## Demonstrable outcome

qcli now exposes one CLI execution workflow for Trino, Databricks SQL, and
Snowflake. Each target is resolved from its configuration section and dispatched
through the common engine-adapter contract.

Examples:

```text
qcli --target trino --command "select 1"
qcli --target databricks-dev --command "select 1"
qcli --target snowflake-dev --command "select 1"

qcli target capabilities trino
qcli target capabilities databricks-dev
qcli target capabilities snowflake-dev
```

The adapters and CLI paths are implemented for all three engines. Trino and
Databricks were exercised during their implementation milestones. On 2026-07-23,
Snowflake PAT authentication through the current password field, `SELECT 1`, and
a ten-row sample-data query were demonstrated. The corrected portable profile
then passed against configured live Trino, Databricks, and Snowflake targets.

## Implemented

- A reusable `qcli-driver-conformance` crate that executes adapter requests while
  concurrently draining bounded result and event channels.
- Common assertions for result streaming, normalized metadata discovery, query
  success, non-empty results, and consistent row counts.
- A portable SQL profile covering integer, decimal, Unicode text, and null values.
- An ignored, credential-safe live integration test that runs the same portable
  query against configured Trino, Databricks, and Snowflake targets.
- A stable `qcli target capabilities TARGET` command that reports normalized
  adapter capabilities without connecting to the engine.
- Explicit capability reporting for Snowflake's current lack of server-side query
  cancellation; Trino and Databricks advertise cancellation.

## Verification

Completed locally:

```text
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The deterministic Milestone 9 capability profile passes for all three adapters.
The credential-gated live profile passed on 2026-07-23. It remains ignored by
default because ordinary workspace and CI runs do not have three live engines or
their credentials.

Run the live profile with the default target names:

```text
cargo test -p qcli --test milestone9 \
  live_three_engine_portable_query_profile -- --ignored --exact
```

Target section names and the configuration path can be selected through
`QCLI_M9_CONFIG`, `QCLI_M9_TRINO_TARGET`, `QCLI_M9_DATABRICKS_TARGET`, and
`QCLI_M9_SNOWFLAKE_TARGET`. Credentials remain in configuration/environment
resolution and are neither accepted as test arguments nor printed.

## Live gate evidence

The live profile executed one portable query containing integer, fixed decimal,
Unicode text, and null values through each engine adapter:

```text
QCLI_M9_TRINO_TARGET=trino-local \
QCLI_M9_DATABRICKS_TARGET=databricks-dev \
QCLI_M9_SNOWFLAKE_TARGET=snowflake-dev \
cargo test -p qcli --test milestone9 \
  live_three_engine_portable_query_profile -- --ignored --exact
```

The original profile used an unbounded `VARCHAR`, which Databricks correctly
rejected. The shared SQL now uses `VARCHAR(100)`, which passed all three engines.

## Accepted release-candidate limitations

The following remain explicit follow-up work:

- Snowflake server-side query cancellation is unavailable through the selected
  client because it does not expose the required query ID/cancellation API.
- Snowflake PAT smoke execution is confirmed; password/MFA, large multi-chunk
  results, exact type coverage, renewal, and failure paths still require live
  hardening.
- Databricks qualified `USE SCHEMA catalog.schema` behavior is engine-specific;
  catalog and schema changes should continue to be issued separately where
  required by Unity Catalog.

These gaps are isolated behind adapter and conformance boundaries and do not
change the shared CLI execution contract.
