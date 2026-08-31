# Milestone 8 Notes: Snowflake

Status: Complete for the Phase 1 release candidate

Implemented: 2026-07-21

## Demonstrable outcome

qcli now recognizes Snowflake targets and uses `snowflakedb-rs` for native
login, session renewal, Arrow-backed query execution, and metadata navigation.

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
credentials. We accepted the adapter foundation without claiming that these
commands were demonstrated against Snowflake. Every live gate below is inherited
by Milestone 9.

## Acceptance decision

Milestone 8 was intentionally compressed to keep Phase 1 moving. Acceptance
means the adapter boundary, password configuration, selected client integration,
bounded conversion path, metadata implementation, tests, and documentation are
present. It does not mean production compatibility has been established.

The release candidate cannot graduate while the required Snowflake live matrix
remains untested. If `snowflakedb-rs` fails that matrix, M9 may extend it, replace
it, or move the adapter to a qcli-owned protocol implementation without changing
the frontend or core contracts.

## Implemented

- New `qcli-driver-snowflake` adapter using `snowflakedb-rs` 1.1.7.
- Explicit `auth_type = password` validation and redacted
  `UsernamePasswordCredential` handling at qcli's boundary.
- Native Snowflake session login and automatic session-token renewal supplied by
  the selected client.
- Warehouse, database, schema, and role initialization.
- Arrow result decoding through the client's Arrow 57 implementation, converted
  through its neutral row representation into bounded 1,000-row qcli Arrow 59
  batches.
- Four-way parallel Snowflake chunk download while preserving chunk order.
- Exact decimal and timestamp parsing through the client's `decimal` and
  `chrono` features, serialized without deliberate display truncation into the
  common result batch.
- Native Snowflake type, precision, and scale retained as Arrow field metadata.
- Metadata support for databases/catalogs, schemas, objects, and columns.
- Successful `USE DATABASE`, `USE SCHEMA`, `USE WAREHOUSE`, and `USE ROLE`
  statements update terminal session context.
- CLI adapter registration for batch, target testing, and interactive use.

## Arrow version boundary

qcli uses Arrow 59 while `snowflakedb-rs` 1.1.7 uses Arrow 57. Enabling its Arrow
feature places both versions in one process, but their concrete batch types do
not cross the adapter boundary. The client decodes Snowflake's binary chunks and
exposes neutral `Row` values; qcli then builds its own Arrow 59 batches.

The original JSON path silently returned zero rows when Snowflake supplied
`rowsetBase64` results. The adapter now requests Arrow explicitly and rejects any
reported-versus-decoded row-count mismatch instead of accepting silent data loss.

## Verification

Completed locally:

```text
cargo check --workspace
cargo test -p qcli-driver-snowflake
```

Focused tests cover credential redaction, identifier quoting, metadata pattern
matching, and session-context parsing. The selected crate and all required
features compile on qcli's Rust 1.86 baseline.

Live smoke validation on 2026-07-23 confirmed that a Snowflake PAT supplied
through the current password field authenticates successfully, `SELECT 1`
returns one row, and
`SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION LIMIT 10` returns ten rows through the
Arrow-backed path.

## Deferred live gate inherited by Milestone 9

Before completing Milestone 9, run against a test Snowflake account and record:

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
