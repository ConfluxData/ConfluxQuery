# Support policy

Protocol compatibility is not the same as a supported product integration.
ConfluxQuery labels every surface as **supported**, **experimental**, or
**planned**.

## Supported

A supported surface has a documented contract, deterministic tests, a packaged
release profile, security ownership tests, and an operational path. M24
supports:

- CLI execution for Trino, Databricks SQL, Snowflake, and the demo adapter.
- Authenticated HTTP sessions, queries, results, events, and cancellation.
- Native Arrow Flight SQL discovery, sessions, queries, metadata, prepared
  statements, ingestion, and result transfer.
- The versioned Python, Go, Java, and Rust ADBC profiles listed in the
  [compatibility contract](../connectivity-compatibility.md).
- The upstream Arrow Flight SQL JDBC integration profile.
- Standalone mode and PostgreSQL/object-store cluster mode within the stated
  deployment limits.

## Experimental

Experimental features may be useful but are not release-blocking, may have
incomplete interoperability, and can change:

- ODBC and BI connectivity.
- Any client or version not named in the compatibility matrix.
- Cross-region active/active cluster operation.

Experimental pages and examples carry an explicit warning. Protocol similarity
alone never promotes a client to supported status.

## Planned

Planned features are design intent, not usable behavior. The branded
ConfluxQuery JDBC Driver and automatic SQL dialect transpilation are examples. They are kept out
of quickstarts and production runbooks until their milestone gates pass.

## Backend differences

ConfluxQuery standardizes workflow and data transport; it does not erase backend
semantics. SQL grammar, identifiers, data types, catalog hierarchy,
authentication availability, cancellation guarantees, and command responses
can differ. Consult [engine setup](../guides/engines.md) and inspect a target:

```bash
qcli target capabilities warehouse-name
```
