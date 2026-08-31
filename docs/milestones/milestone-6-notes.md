# Milestone 6 Notes: Target Switching and Warehouse Navigation

Status: Complete

Completed: 2026-07-21

## Demonstrated outcome

The interactive shell can now inspect warehouse metadata, change catalog and
schema context, persist successful native Trino `USE` statements, and switch
configured targets without restarting qcli.

The live gate against Trino on `localhost:8080` demonstrated:

```text
trino-local[tpch.tiny]> \catalogs
jmx
memory
system
tpcds
tpch

trino-local[tpch.tiny]> \schemas sf*
sf1
sf100
sf1000
...

trino-local[tpch.tiny]> \tables nat*
nation                                   table

trino-local[tpch.tiny]> \describe nation
nationkey                        bigint
name                             varchar(25)
regionkey                        bigint
comment                          varchar(152)

trino-local[tpch.tiny]> use tpch.sf100;
0 rows
trino-local[tpch.sf100]> select current_schema;
sf100
```

## Architecture

Milestone 6 extends the internal adapter contract with normalized catalog,
schema, object, and column metadata types. Metadata operations remain engine
specific, while consumers use one frontend-neutral API.

The new `qcli-metadata` crate owns:

- Adapter routing without frontend engine conditionals.
- Target, engine, catalog, schema, pattern, properties, and operation-scoped
  cache keys.
- Configurable cache TTL.
- Target-scoped invalidation after context or target changes.
- No retained plaintext property values in cache keys; resolved properties are
  represented by a hash.

The REPL receives the same adapter registry for `QueryService` and
`MetadataService`. This keeps terminal and future HTTP metadata behavior on the
same contracts.

## Delivered commands

```text
\targets
\use TARGET
\catalogs [PATTERN]
\schemas [PATTERN]
\tables [PATTERN]
\describe OBJECT
\use-catalog CATALOG
\use-schema SCHEMA
\status
```

Patterns support the common `*` and `?` glob forms. Trino translates them to a
properly escaped `LIKE` predicate for `information_schema.tables`.

## Context realization

`\use-catalog` and `\use-schema` validate the requested value through metadata
before applying a versioned session override. The prompt and `\status` show the
effective catalog and schema:

```text
trino-local[tpch.sf100]>
```

Each subsequent query receives an immutable snapshot containing that context,
so the Trino client is constructed with the selected catalog and schema.

Successful Trino `USE schema` and `USE catalog.schema` statements produce a
normalized `SessionProperties` query event. The REPL applies those properties
through `SessionManager`, increments the session version, invalidates metadata,
and updates the prompt. Failed `USE` queries produce no mutation.

## Atomic target switching

`\use TARGET` validates the prospective target before changing the logical
session. A missing or unreachable target reports an error and preserves the
current target, context, session version, and prompt. A successful switch uses
`SessionManager::switch_target` in one locked version-checked mutation and
clears target-specific overrides.

Queries submitted before a switch retain their original immutable snapshots.
The new target affects only later submissions.

## Completion

Rustyline completion starts with SQL keywords, meta-commands, and configured
target names. Successful metadata commands add the returned catalogs, schemas,
objects, and columns to the candidate set. Completion is therefore scoped to
metadata actually discovered in the active interactive workflow rather than a
global cross-target cache.

## Automated evidence

The Milestone 6 pseudo-terminal test covers:

- Listing targets and identifying the active one.
- Catalog and schema discovery.
- Versioned catalog and schema changes.
- Context-bearing prompt changes.
- Pattern-filtered tables and views.
- Object description.
- Metadata-driven tab completion.
- Failed target switch preservation.
- Successful target switch and cleared old context.
- Updated `\status` target and session version.

The metadata unit gate proves that repeated requests hit the cache, different
targets do not share entries, and target invalidation forces a refresh. Trino
unit coverage verifies native `USE` normalization.

Live Trino gates verified catalog/schema/table discovery, object description,
meta-command schema switching, native SQL `USE tpch.sf100`, prompt updates, and
subsequent query context.

## Known limitations

- Native Trino session synchronization currently normalizes successful `USE`
  statements. Other response-driven updates such as roles, prepared statements,
  and arbitrary `SET SESSION` values still require a client session-snapshot
  API or additional normalized adapter events.
- Completion candidates are refreshed after explicit metadata commands; there
  is no background prefetch yet.
- Object discovery currently uses Trino `information_schema.tables`; materialized
  views and engine-specific object classes map to `Other` until normalized.
- Metadata cache storage is in-process with a fixed 30-second REPL TTL. HTTP
  service quotas and eviction policies will be added with the HTTP milestones.
- Target switches currently use catalog discovery as their connection and
  metadata-capability validation operation.

## Prerequisites established for Milestone 7

- Databricks can implement the same normalized metadata contract.
- Target switching is adapter-neutral and versioned.
- Catalog/schema context is carried by immutable query snapshots.
- Metadata caching is reusable by terminal and HTTP frontends.
- Capability fields describe each adapter's discovery support.
