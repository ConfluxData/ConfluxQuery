# Milestone 8 Notes: Snowflake

Status: Implementation complete; live Snowflake gate pending

Implemented: 2026-07-21

## Demonstrable outcome

qcli now recognizes Snowflake targets and uses `snowflakedb-rs` for native
username/password login, session renewal, query execution, JSON result streaming,
and metadata navigation.

Example configuration:

```ini
[snowflake-dev]
engine = snowflake
auth_type = password
account = organization-account
user = deepak
password = ${SNOWFLAKE_PASSWORD}
warehouse = COMPUTE_WH
database = REPORTING
schema = PUBLIC
role = ANALYST
```

Demo commands:

```text
qcli target test snowflake-dev
qcli --target snowflake-dev --command "select current_database(), current_schema(), current_warehouse(), current_role()"
qcli --target snowflake-dev
snowflake-dev[REPORTING.PUBLIC]> \tables
snowflake-dev[REPORTING.PUBLIC]> USE WAREHOUSE REPORTING_WH;
```

The environment used for implementation did not contain Snowflake account
credentials, so these live commands remain the milestone's final external gate.

## Implemented

- New `qcli-driver-snowflake` adapter using `snowflakedb-rs` 1.1.7.
- Explicit `auth_type = password` validation and redacted
  `UsernamePasswordCredential` handling at qcli's boundary.
- Native Snowflake session login and automatic session-token renewal supplied by
  the selected client.
- Warehouse, database, schema, and role initialization.
- JSON result streaming in bounded 1,000-row Arrow 59 batches.
- Four-way parallel Snowflake chunk download while preserving chunk order.
- Exact decimal and timestamp parsing through the client's `decimal` and
  `chrono` features, serialized without deliberate display truncation into the
  common result batch.
- Native Snowflake type, precision, and scale retained as Arrow field metadata.
- Metadata support for databases/catalogs, schemas, objects, and columns.
- Successful `USE DATABASE`, `USE SCHEMA`, `USE WAREHOUSE`, and `USE ROLE`
  statements update terminal session context.
- CLI adapter registration for batch, target testing, and interactive use.

## Why JSON streaming in this cut

qcli uses Arrow 59 while `snowflakedb-rs` 1.1.7 uses Arrow 57. Enabling its Arrow
feature would place incompatible Arrow types in one process and could require
copying every result batch. The JSON streaming path still downloads chunks in
parallel and preserves exact values. We should enable native Arrow after the
client publishes a compatible Arrow version or after measurement supports a
specific conversion strategy.

## Verification

Completed locally:

```text
cargo check --workspace
cargo test -p qcli-driver-snowflake
```

Focused tests cover credential redaction, identifier quoting, metadata pattern
matching, and session-context parsing. The selected crate and all required
features compile on qcli's Rust 1.86 baseline.

## Live exit gate

Before changing the status to `Complete`, run against a test Snowflake account and
record:

- password-authenticated `SELECT 1`.
- a multi-chunk result and bounded process memory.
- exact 38-digit decimals and all Snowflake timestamp variants.
- warehouse, database, schema, and role changes.
- database, schema, object, and column navigation.
- concurrent logical sessions with different contexts.
- invalid credentials, authorization failures, and expired session renewal.

## Known client gaps

- `snowflakedb-rs` does not publicly expose query IDs or a cancellation method.
  The adapter therefore does not advertise `CancelQuery`; a cancellation request
  stops local result consumption and reports that server cancellation is
  unavailable. This is the largest remaining Milestone 8 exit-gate gap.
- The client's authentication enum currently contains password and optional
  certificate/key-pair methods. OAuth, browser SSO, programmatic access tokens,
  profiles, and workload identity require an upstream extensibility hook or a
  later transport pivot.
- The crate uses Snowflake's native driver protocol rather than the documented
  SQL REST API, so live compatibility tests are mandatory.
- Client option structures derive `Debug` over authentication strategy data.
  qcli never logs those structures, but an upstream secret-safe representation is
  recommended before broader embedding.

These limitations are isolated inside `qcli-driver-snowflake`; replacing or
extending the client does not change qcli core, terminal, batch, or HTTP contracts.
