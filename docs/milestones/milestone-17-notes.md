# Milestone 17 — Target-aware Flight SQL metadata

Status: Complete

## Outcome

An authenticated Flight SQL client can now browse the warehouse selected by
its qcli session or stateless `qcli-target` metadata. Catalogs, database
schemas, tables, views, embedded Arrow table schemas, table types, and XDBC
type information use standard Apache Flight SQL commands and schemas.

The Flight frontend does not execute engine-specific discovery SQL. It builds a
protocol-neutral `MetadataRequest` from the authenticated immutable session
snapshot, then calls the shared `MetadataService` and the selected adapter.
Trino, Databricks SQL, Snowflake, and future adapters therefore share the same
authorization, caching, filtering, and Flight encoding path.

## Implemented standard commands

- `CommandGetCatalogs`
- `CommandGetDbSchemas`
- `CommandGetTables`, including `include_schema`
- `CommandGetTableTypes`
- `CommandGetXdbcTypeInfo`
- `CommandGetPrimaryKeys`
- `CommandGetExportedKeys`
- `CommandGetImportedKeys`
- `CommandGetCrossReference`
- target-sensitive `CommandGetSqlInfo`

`GetFlightInfo` returns the exact Apache schema and a standard command ticket.
`DoGet` returns bounded Arrow batches produced by Arrow Rust's official Flight
SQL metadata builders wherever Arrow Rust supplies one.

## Target and identity scoping

Metadata context is resolved in this order:

1. A valid `arrow_flight_session_id` cookie selects the owned logical session.
2. Otherwise, the authenticated request must provide `qcli-target` metadata.
3. Catalog/schema values in the metadata command narrow the session context.

Session ownership and target ACLs are checked before adapter discovery. A
principal cannot browse a target that it cannot query.

Cache keys include:

- authenticated principal identity;
- target and engine;
- catalog and schema;
- search pattern;
- a hash of the resolved execution properties;
- metadata operation and described object.

This prevents results cached for one user, target, role, compute, catalog, or
schema from leaking into another context. Target invalidation removes every
entry for that target, and entries expire after the service TTL.

## Catalogs, schemas, tables, and filters

The adapters enumerate the broad authorized context. Arrow's standard builders
then apply Flight SQL catalog equality, `%`/`_` schema and table patterns, table
type filters, stable ordering, and nullability rules. This ensures Flight SQL
pattern semantics remain consistent even when an engine's native metadata
syntax differs.

Object kinds map to `TABLE`, `VIEW`, and `OTHER`. `GetTableTypes` reports only
kinds observed for the selected target, with `TABLE` and `VIEW` as the empty
catalog baseline.

## Embedded table schemas and type mapping

When `include_schema=true`, qcli describes each returned object and serializes
its Arrow schema in the standard `table_schema` field. Fields preserve:

- column name and nullability;
- native type name;
- catalog, schema, and table metadata;
- column remarks/comments;
- booleans and signed integer widths;
- floating-point values;
- decimal precision and scale;
- dates and microsecond timestamps;
- UTF-8 and binary values;
- arrays and maps, including nested type arguments.

Unknown or vendor-specific types fail soft to Arrow UTF-8 while retaining the
native type name in `ARROW:FLIGHT:SQL:TYPE_NAME`. This is preferable to claiming
an incorrect physical type and gives clients an observable fallback.

## XDBC types and SQL capabilities

The XDBC response uses Arrow Rust's exact 19-column schema and publishes the
portable types currently preserved across the three adapters: boolean,
integer, bigint, double, decimal, varchar, varbinary, date, and timestamp.
Filtering by XDBC data-type code is handled by the official builder.

`GetSqlInfo` is now evaluated for the selected adapter. Query cancellation is
advertised only when that adapter reports `CancelQuery`. Identifier quote and
case behavior comes from the adapter contract rather than frontend engine-name
conditionals. Snowflake reports uppercase unquoted identifiers; the common
default reports case-insensitive unquoted identifiers and standard double-quote
delimiters.

## Key relationships

The current Trino, Databricks, and Snowflake adapters do not yet expose primary
or foreign-key discovery. qcli still returns the exact Apache primary/foreign
key schemas with zero rows so JDBC/ADBC browsers can distinguish "no reported
relationships" from malformed or unsupported protocol data. It never invents
constraints. Adapter relationship capabilities and populated rows can be added
without changing the Flight contract.

## Security

- Every metadata `DoGet` is bearer-authenticated.
- Session cookies remain principal-bound and versioned.
- Unauthorized targets fail before discovery.
- Resolved target properties and credentials never enter Flight metadata.
- `GetSessionOptions` continues to expose only client-controlled overrides.
- Cache keys hash resolved properties; cache responses never serialize them.

## Verification

Automated tests cover:

- official Arrow Flight SQL client calls for every populated metadata family;
- byte-equivalent `FlightInfo` and streamed Arrow schemas;
- catalog and SQL-LIKE pattern filtering;
- table/view classification and ordering;
- embedded table schemas and column metadata;
- XDBC type rows and filtering foundation;
- exact empty primary-key schema;
- identity-, target-, context-, and property-scoped caching;
- existing session ownership and unauthorized target protections;
- all prior Flight query, replay, cancellation, TLS, proxy, and lifecycle tests.

Run:

```text
cargo test -p qcli-metadata
cargo test -p qcli-flight-sql
cargo test --workspace
```

Live three-engine browsing uses the same configured targets and credentials as
the M9 profile. Credential-free CI validates the shared contract with the demo
adapter; operator environments validate native catalog contents.

## Next boundary

M18 adds prepared statements, typed parameter batches, updates, and explicit
handle lifecycle. M17 does not interpolate parameters or claim transaction
support.
