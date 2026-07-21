# Milestone 7 Notes: Databricks SQL

Status: Implementation complete; live Databricks gate pending

Implemented: 2026-07-21

## Demonstrable outcome

qcli now recognizes Databricks targets and runs the common batch and interactive
query workflows through the official SQL Statement Execution API.

Example configuration:

```ini
[databricks-dev]
engine = databricks
auth_type = pat
host = https://dbc-example.cloud.databricks.com
http_path = /sql/1.0/warehouses/abc123
token = ${DATABRICKS_TOKEN}
catalog = main
schema = default
```

Demo commands:

```text
qcli target test databricks-dev
qcli --target databricks-dev --command "select current_catalog(), current_schema()"
qcli --target databricks-dev
databricks-dev[main.default]> \tables
```

The environment used for implementation did not contain a Databricks host and
PAT, so these live commands remain the milestone's final external gate.

## Implemented

- New `qcli-auth` crate with redacted secret values and replaceable Bearer-token
  providers.
- PAT provider selected by `auth_type = pat`.
- HTTPS enforcement for credentials, with loopback HTTP permitted only for
  deterministic tests.
- Warehouse ID derivation from the configured Databricks SQL HTTP path.
- Statement submission with catalog and schema context.
- Hybrid execution followed by asynchronous status polling.
- Statement ID propagation as qcli's engine query ID.
- Inline JSON result handling and internal chunk-link traversal.
- Cancellation through the Statement Execution cancel endpoint.
- Structured HTTP, authentication, protocol, and execution errors.
- Metadata support for catalogs, schemas, tables, and object descriptions.
- Successful `USE CATALOG` and `USE SCHEMA` statements update terminal session
  context without changing an already-running query snapshot.
- Native Databricks type text retained as Arrow field metadata. Values currently
  remain exact UTF-8 strings, avoiding decimal or timestamp precision loss while
  typed Arrow conversion is developed.
- CLI adapter registration for batch, target testing, and interactive use.

## Authentication extensibility

The adapter requests a credential for every HTTP operation rather than retaining
a copied authorization header. A future OAuth or workload provider can therefore
renew a token without changing statement submission, polling, chunk retrieval,
metadata, cancellation, core sessions, or frontends.

Unsupported `auth_type` values fail before network access. Token values are
redacted from `Debug`, display, configuration output, and adapter errors.

## Verification

Completed locally:

```text
cargo check --workspace
cargo test -p qcli-auth -p qcli-driver-databricks
```

Focused tests cover:

- PAT configuration and warehouse-ID derivation.
- Unsupported authentication rejection.
- secret redaction.
- exact high-precision value preservation.
- native type metadata.
- session context parsing.

## Live exit gate

Before changing the status to `Complete`, run against a test SQL warehouse and
record:

- PAT-authenticated `SELECT 1` and engine statement ID.
- a query that requires polling.
- more than one result chunk.
- cancellation of a running query.
- catalog, schema, table, and column navigation.
- decimal, date, timestamp, binary, null, and complex values.
- a failed query and an expired or invalid PAT.

## Known limitations

- PAT is the only implemented provider; the provider boundary is ready for
  OAuth M2M, OAuth U2M, profiles, and workload identity.
- Results preserve exact text and native type metadata but are not yet converted
  into all corresponding Arrow physical types.
- External-link Arrow/CSV result disposition is not implemented in this cut;
  inline internal chunks are bounded by Databricks API behavior.
- A live warehouse accepted separate `USE CATALOG` and `USE SCHEMA` statements,
  but the attempted qualified catalog/schema switch remained unsuccessful. Use
  the two-statement form for now and retain qualified switching as a live
  compatibility investigation.
