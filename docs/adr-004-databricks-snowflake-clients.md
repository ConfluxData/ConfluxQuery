# ADR-004: Databricks and Snowflake Rust Client Selection

Status: Accepted for implementation, subject to each milestone's live exit gate

## Decision summary

| Engine | Selected foundation | Initial authentication |
|---|---|---|
| Databricks SQL | `reqwest` plus qcli-owned typed models for the official Statement Execution API | PAT |
| Snowflake | `snowflakedb-rs` 1.x behind `qcli-driver-snowflake` | Username/password |

Both choices remain behind qcli-owned adapter and authentication-provider
contracts. A failed live exit gate can replace a library without changing core or
frontend APIs.

## Selection criteria

The candidates were compared in this order:

1. Authentication fit now and ability to add providers later.
2. Query lifecycle: submit, identify, poll/stream, cancel, and report errors.
3. Result performance: bounded streaming, parallel chunk retrieval, and Arrow.
4. Type fidelity, especially decimals and timestamps.
5. Metadata and context support.
6. TLS/HTTP configurability, maintenance, license, and dependency footprint.

Vendor ownership is useful but not required. Community crates are acceptable
when live interoperability and maintenance quality are adequate.

## Databricks decision

Use `reqwest` and `serde` with small qcli-owned models for the documented
Databricks SQL Statement Execution API.

### Why not `rustbricks` 0.1.1

`rustbricks` implements statement submission, status, and result-chunk requests,
but its session configuration owns a static token string and constructs the
Bearer header internally. This conflicts with renewable credential providers. It
also reads every HTTP response fully into text before deserialization and does
not expose the complete cancellation and external-result lifecycle qcli needs.
Its useful protocol surface is small enough that wrapping it provides less value
than owning focused API models.

### Why direct REST is the closer fit

- PAT and future OAuth/workload tokens all become the same injected Bearer
  credential at the query transport boundary.
- The official API exposes statement IDs, hybrid/asynchronous execution,
  cancellation, inline chunks, and external links.
- qcli controls bounded downloads, retry classification, credential renewal, and
  redaction.
- `reqwest` is already a well-supported transport dependency and does not couple
  authentication to Databricks response types.

This is a deliberate choice of a general Rust HTTP crate rather than a dedicated
Databricks crate.

## Snowflake decision

Use `snowflakedb-rs` 1.x with the `reqwest`, `arrow`, `chrono`, and `decimal`
features, initially through `AuthStrategy::Password`.

### Why it is the closest current fit

- Native username/password login, which the Snowflake SQL REST API does not
  provide as its primary authentication path.
- Optional key-pair/certificate authentication.
- Automatic Snowflake session-token renewal.
- Streaming JSON or Arrow results and configurable parallel chunk downloading.
- Exact decimal and timestamp-oriented feature support.
- Connection pooling, bindings, query descriptions, custom HTTP clients, and no
  Go/JDBC runtime dependency.

### Alternatives not selected

- `rsql_driver_snowflake` is coupled to rsql's driver abstractions and currently
  treats URL password material as an OAuth token; it does not satisfy qcli's
  initial username/password target.
- `snowflake-api`/`snowflake-jwt` and direct SQL REST are useful for OAuth and
  key-pair flows but do not provide the initial native password login plus Arrow
  result path as one reusable client.
- ODBC through a Rust wrapper has broad vendor-supported authentication but adds
  native driver installation and packaging complexity, so it remains a fallback
  rather than the default qcli architecture.

## Required Snowflake validation and likely upstream work

`snowflakedb-rs` uses the same native protocol family as Snowflake drivers rather
than the documented SQL REST API. Milestone 8 must therefore run live compatibility
tests and retain an API fallback plan.

Before calling the integration production-capable, qcli needs either upstream
support or a small maintained extension for:

- a public, extensible authentication hook beyond the current password and
  certificate enum variants;
- supplied OAuth, programmatic-access-token, and workload-identity credentials;
- external browser/SSO orchestration or injection of its resulting token;
- public query ID access and reliable cancellation;
- explicit proxy, timeout, TLS, and retry configuration tests;
- confirmation that secrets cannot appear through `Debug` or error paths;
- Arrow version compatibility with qcli's common result model. At selection time,
  `snowflakedb-rs` 1.1.7 depends on Arrow 57 while qcli uses Arrow 59. Shipping
  both versions or copying every batch is not acceptable without measurement;
  prefer an upstream Arrow upgrade or a compatible release before integration.

These gaps do not block the initial password spike, but they block claims of broad
enterprise authentication support. Prefer upstream contributions over a long-lived
fork. The qcli adapter boundary remains the fallback if upstream changes are not
accepted.

## Milestone exit gates

### Databricks

- PAT-authenticated live query, polling, chunk retrieval, cancellation, metadata,
  and type conversion.
- Mocked renewable-token test proving the transport requests a fresh credential
  without reconstructing core session state.
- Large-result test covering bounded inline or external chunk retrieval.

### Snowflake

- Password-authenticated live query with query ID, streamed multi-chunk results,
  exact decimals/timestamps, metadata, and cancellation.
- Connection/session isolation across role, warehouse, database, and schema.
- A compile-time or mock proof that the adapter can accept another credential
  provider without changing qcli core, terminal, or HTTP contracts.
- Documented outcome for each upstream gap listed above.
