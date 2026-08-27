# ConfluxQuery Feature Roadmap

Status: Product direction

Related documents:

- [Product and technical design](product-design.md)
- [Sequenced execution plan](execution-plan.md)
- [Flight SQL data-plane decision](adr-006-flight-sql-data-plane.md)

## 1. Product direction

ConfluxQuery Gateway is a governed, Arrow-native query gateway for analytical platforms, with a
first-class terminal client. It provides one access layer for Trino, Databricks
SQL, and Snowflake through:

- Interactive and batch CLI.
- HTTP control and operations APIs.
- Flight SQL for Arrow-native remote SQL access.
- Standard ADBC and JDBC clients, plus explicitly certified ODBC clients.

ConfluxQuery owns connectivity, identity, policy, routing, query lifecycle, metadata,
observability, and result delivery. The selected warehouse continues to own SQL
execution, optimization, storage, and engine-native transaction semantics.

This document records longer-term product capabilities so later implementation
decisions retain their original intent. It is a feature roadmap, not a promise
that every item belongs in the first stable release. The authoritative delivery
order and completion status remain in the
[execution plan](execution-plan.md).

## 2. Product forms

ConfluxQuery has two offerings built from the same core:

### 2.1 Terminal client

Running `qcli` starts the interactive query shell. Batch commands provide
scriptable execution. This remains the safe default because starting a network
listener must be deliberate.

### 2.2 Query gateway

Running `qcli serve` starts the shared service. It hosts:

- HTTP for administration, automation, health, metrics, and query operations.
- Flight SQL for standard SQL metadata and Arrow result transport.
- One protocol-neutral service layer for sessions, ownership, quotas, results,
  audit, and query lifecycle.

A server-oriented container or future `qcli-server` package may default to
serve mode, but the local `qcli` executable should continue to default to the
terminal.

## 3. Roadmap principles

Features should preserve these boundaries:

1. One query executes on one selected engine. Cross-engine joins are not an
   implicit qcli responsibility.
2. Engine-native SQL pass-through remains the default.
3. SQL translation is explicit, observable, versioned, and fail-closed.
4. Frontends do not contain engine protocol logic.
5. Drivers do not contain presentation, routing, or authorization policy.
6. HTTP and Flight SQL share canonical sessions and query state.
7. Exact values and Arrow types are preserved before display formatting.
8. Every retained resource has ownership, limits, expiry, and audit behavior.
9. Unsupported functionality is reported honestly rather than silently
   emulated.
10. New engines and policies should plug into stable internal contracts.

## 4. Foundational gateway capabilities

### 4.1 Universal warehouse endpoint

A client connects once and selects an authorized target:

```text
trino-production
databricks-finance
snowflake-analytics
```

qcli normalizes connection establishment, query lifecycle, cancellation,
metadata, Arrow results, error categories, and query identifiers. It does not
hide engine-specific capabilities when those details matter.

Success means an application can change its target without installing a
different database client or rebuilding its result-processing path.

### 4.2 Cross-protocol sessions

Terminal, HTTP, and Flight SQL use the same logical session model:

- Principal and authorization context.
- Active target.
- Catalog/database and schema.
- Compute/warehouse and role.
- Engine session properties.
- qcli execution and display overrides.
- Version and expiry.

Where policy permits, a query submitted through one protocol can be inspected,
cancelled, or retrieved through another. Every query still captures an
immutable session snapshot, so later session changes cannot alter running work.

### 4.3 Query passport

Every submitted query receives a qcli query ID and an operational record
containing:

- Caller identity and client type.
- Logical and physical target.
- Original SQL fingerprint.
- Executed SQL and translation metadata, when applicable.
- Session snapshot version.
- Engine query ID and profile URL.
- Admission, queue, execution, and result timings.
- Rows, bytes, spill, and engine-reported cost information.
- Cancellation and retry history.
- Result retention and expiry.
- Trace, policy, and audit references.

The passport is accessible through CLI and service APIs and becomes the common
debugging language across all supported warehouses. SQL text and sensitive
values remain protected by configurable audit policy.

### 4.4 Durable and shareable results

Large results should survive client disconnects and be retrievable in Arrow,
Parquet, CSV, JSONL, or paginated JSON where supported. The service may provide:

- Bounded in-memory buffering and disk spill.
- Object-storage-backed results.
- Resumable downloads.
- Signed, expiring, authorization-bound result references.
- Explicit result sharing.
- Configurable retention and deletion.
- Multi-endpoint Flight delivery for scalable reads.

Durability must not accidentally make sensitive results broadly accessible.

### 4.5 Unified observability

Operators need one view across all engines:

- Active, queued, completed, failed, and cancelled queries.
- qcli latency versus warehouse latency.
- Per-target and per-principal concurrency.
- Result bytes, rows, spill, and retention.
- Engine query links and normalized errors.
- Authentication and authorization failures.
- OpenTelemetry traces, metrics, structured logs, and audit events.

## 5. Dialect-aware SQL transpilation

### 5.1 Product promise

Users may declare the dialect in which a query was written and execute it on a
different engine:

```text
input dialect: snowflake
target engine: databricks
mode: strict
```

The goal is:

> Write once for a documented portable subset, then execute safely against
> Trino, Databricks SQL, or Snowflake.

qcli must not claim that all SQL is universally portable. A syntactically valid
translation can still change behavior because engines differ in NULL ordering,
implicit casts, timestamps, case sensitivity, numeric overflow, JSON semantics,
collations, and error behavior.

### 5.2 Relationship to native pass-through

Native pass-through remains the default:

```text
transpile=off
```

Transpilation is an optional stage before policy validation and adapter
submission:

```text
client SQL
    |
    v
declared or detected input dialect
    |
    v
parse -> normalized SQL representation -> target-dialect generation
    |
    v
semantic warnings and policy/capability checks
    |
    v
immutable query snapshot and selected engine adapter
```

The transpiler is not implemented inside individual engine drivers. A shared
normalized representation avoids building and maintaining every pairwise
translation.

### 5.3 Modes

The proposed modes are:

| Mode | Behavior |
|---|---|
| `off` | Send the original SQL unchanged. This is the default. |
| `auto` | Translate when declared input and target dialects differ; reject uncertain constructs according to policy. |
| `strict` | Execute only when every relevant construct has a supported, high-confidence translation. |
| `best_effort` | Produce a translation with explicit warnings; execution may require a separate opt-in. |

`best_effort` should never silently execute potentially different write
semantics. Organizations may disable it entirely in serve mode.

### 5.4 Configuration

Defaults, target overrides, session options, and per-query options follow normal
qcli precedence:

```ini
[default]
input_dialect=ansi
transpile=off

[databricks-production]
engine=databricks
input_dialect=snowflake
transpile=strict
```

Likely options include:

- `input_dialect`
- `transpile`
- `transpile_ruleset`
- `transpile_min_confidence`
- `transpile_allow_writes`
- `transpile_warning_policy`

The target adapter declares its output dialect and supported dialect version.

### 5.5 User interfaces

Offline translation must be available without connecting to a warehouse:

```text
qcli transpile --from snowflake --to databricks --file report.sql
qcli transpile --from trino --to snowflake --command "select ..."
```

Execution can opt in explicitly:

```text
qcli query \
  --target databricks-production \
  --input-dialect snowflake \
  --transpile strict \
  --file report.sql
```

Serve mode should expose:

```text
POST /v1/sql/transpile
POST /v1/queries
```

Flight SQL sessions should accept documented qcli session options for input
dialect and translation mode. Client-visible warnings must use protocol-native
metadata or a documented retrieval API.

### 5.6 Translation report

Every translation produces a report containing:

- Original and generated SQL.
- Source and target dialect plus versions.
- Transpiler and ruleset versions.
- Rewritten constructs.
- Unsupported or ambiguous constructs.
- Semantic-risk warnings.
- Confidence classification.
- Source-to-generated position mapping.
- Stable fingerprint of both statements.

The query passport records this report or an authorization-protected reference
to it. Engine errors should be mapped back to the original source position when
possible.

### 5.7 Initial portable subset

Good early candidates include:

- Identifier quoting and case rules.
- Literals, aliases, predicates, joins, grouping, and ordering.
- Common scalar and aggregate functions.
- Cast syntax and compatible primitive types.
- Date, time, interval, and string functions with equivalent semantics.
- Standard window functions.
- Pagination syntax.
- Basic array, map, and JSON access where semantics align.
- Read-only common table expressions and subqueries.
- Selected patterns such as `QUALIFY` when a provably equivalent rewrite
  exists.

The supported subset is versioned and tested as a product contract.

### 5.8 High-risk and initially unsupported areas

The following require explicit capability work and should initially fail closed:

- Stored procedures and scripting.
- UDF definitions and engine-specific functions.
- Administrative and security statements.
- Engine-specific DDL.
- `MERGE`, mutation statements, and write retries.
- Dynamic SQL.
- Table functions with no equivalent.
- Trino lambdas and complex higher-order expressions.
- Snowflake `VARIANT` behavior without equivalent target semantics.
- Delta-, Iceberg-, or engine-specific table operations.
- Transactional assumptions.

Automatic routing of writes and DDL should remain disabled until qcli has
statement classification, equivalence guarantees, idempotency policy, and
extensive conformance tests.

### 5.9 Transpilation safety model

Before execution qcli should:

1. Parse and classify the statement.
2. Resolve source and destination dialect versions.
3. Translate using a versioned ruleset.
4. Produce warnings and confidence.
5. Validate target capabilities.
6. Apply governance policy to both original and generated forms.
7. Select the physical target.
8. Record the generated SQL in the immutable query snapshot.
9. Execute exactly that recorded SQL.

Authorization and query restrictions must inspect a normalized representation;
otherwise rewriting could bypass policies applied only to the original text.

### 5.10 Differential validation

Transpilation becomes substantially more valuable when paired with migration
validation:

```text
qcli validate \
  --source snowflake-production \
  --destination databricks-validation \
  --input-dialect snowflake \
  report.sql
```

Validation profiles may compare:

- Output schemas and normalized types.
- Row counts.
- Ordered results or unordered result hashes.
- NULL placement.
- Decimal and floating-point tolerances.
- Timestamp and timezone tolerances.
- Translation warnings.
- Execution time and engine-reported cost.

Both executions require independent authorization. Validation must avoid
materializing unlimited results and must clearly distinguish exact, normalized,
sampled, and probabilistic comparisons.

### 5.11 Suggested transpiler delivery sequence

Each increment should be independently demonstrable:

1. **Dialect inventory and corpus** — capture representative queries and known
   semantic differences across all three engines.
2. **Offline parser and formatter** — parse one dialect, generate another, and
   emit a structured translation report without executing.
3. **Read-only portable subset** — certify basic `SELECT` translation through a
   deterministic cross-engine conformance suite.
4. **Execution integration** — add explicit per-query and per-session modes,
   immutable generated SQL, query passport data, and policy validation.
5. **Function and type expansion** — add versioned mappings for temporal,
   string, numeric, semi-structured, and collection operations.
6. **Gateway routing integration** — allow only certified read-only queries to
   choose among authorized compatible targets.
7. **Differential validation** — compare source and destination results using
   bounded exact and probabilistic profiles.
8. **Controlled write support** — consider specific statements only after
   idempotency, authorization, and equivalence gates exist.

Before implementation, qcli must evaluate whether an existing Rust SQL parser
and transpiler can preserve source locations, dialect extensions, comments,
types, and required rewrites. Library choice is an ADR, not an assumption in
this roadmap.

### 5.12 M26 delivery boundary

M26 is the first implementation milestone for this roadmap area. It combines
the inventory, offline translator, certified read-only subset, and explicit
execution integration so the result is useful from both ConfluxQuery CLI and
ConfluxQuery Gateway. Function/type breadth may grow in later milestones, but
M26 is not complete with only a parser spike or syntax formatter.

M26 requires `off`, `strict`, and offline-only `best_effort`. It does not enable
automatic execution of best-effort output, dialect guessing as a correctness
mechanism, writes, DDL, dynamic SQL, scripting, or cross-engine execution. The
authoritative demo, support boundary, and completion gates are defined in the
[M26 execution-plan contract](execution-plan.md#m26-dialect-aware-sql-transpilation).

## 6. Intelligent routing

Logical targets may map to multiple physical targets:

```ini
[analytics]
type=route
targets=trino-production,databricks-production,snowflake-production
routing_policy=interactive_aware
```

Routing inputs may include:

- Authorization and data residency.
- Declared workload class.
- Target health and queue depth.
- Required SQL and metadata capabilities.
- Input dialect and translation confidence.
- Interactive or batch latency objective.
- Estimated scan, compute, and egress cost.
- Region and dataset availability.

The selected physical target and routing explanation are recorded in the query
passport. Early routing should use explicit rules and metadata, not pretend to
be a cross-engine cost-based SQL optimizer.

## 7. Governance and cost controls

Serve mode can apply consistent controls before reaching a warehouse:

- Per-principal and per-target authorization.
- Catalog, schema, and operation restrictions.
- Read-only profiles.
- Concurrency, queue, runtime, row, byte, and retained-result limits.
- Required query tags and cost centers.
- Estimated-cost approval thresholds.
- Daily or monthly team budgets.
- Runaway-query cancellation.
- Sensitive-result retention and sharing policy.

These controls supplement rather than replace warehouse authorization. qcli
should preserve the downstream identity where the authentication mode supports
delegation.

## 8. Credential brokerage

Clients authenticate to qcli; qcli selects or acquires the appropriate
downstream credential using an extensible provider:

- Service identities.
- User-delegated OAuth.
- Workload identity.
- Short-lived tokens.
- Key-pair authentication.
- External secret managers.
- Credential renewal and rotation.

The client should not need raw warehouse credentials. The query passport must
record the credential identity and provider type without exposing secret
material.

## 9. Availability and continuity

For production gateway deployments qcli should support:

- Shared session and query state.
- Distributed admission control and quotas.
- Query ownership leases.
- Object-backed retained results.
- Graceful draining and rolling upgrades.
- Result retrieval after node failure.
- Health-based routing away from unavailable targets.

Automatic retry is permitted only when qcli can establish that submission did
not occur or the operation is safely idempotent. Ambiguous writes must never be
replayed automatically.

## 10. Extension surface

Stable internal contracts should eventually allow:

- Engine adapters.
- Authentication and credential providers.
- Authorization and governance policies.
- Logical-target routing policies.
- Transpiler dialects and rulesets.
- Audit and telemetry sinks.
- Result and state stores.
- Metadata enrichment.

The first extension mechanism remains Rust workspace traits and crates. A stable
third-party ABI or out-of-process plugin protocol should be designed only after
the contracts have been exercised by multiple implementations.

## 11. Product phases

The roadmap can be understood as four product phases:

| Phase | Product outcome |
|---|---|
| Foundation | Reliable terminal and batch execution across three engines |
| Connectivity gateway | Shared HTTP and Flight SQL service with ADBC/JDBC and governed sessions |
| Intelligent gateway | Query passports, durable results, routing, cost controls, and credential brokerage |
| Dialect-aware gateway | Explicit SQL transpilation, certified portable subsets, and differential migration validation |

The phases describe product maturity, not strict serialization. Foundational
transpiler research and query-corpus collection may start before all enterprise
gateway work is complete, but production execution must pass the required
security, correctness, and conformance gates.

## 12. Marquee product statement

The intended long-term positioning is:

> ConfluxQuery is a governed, dialect-aware, Arrow-native query platform that lets
> applications and people use one interface across analytical warehouses.

Its strongest combined promises are:

1. Connect once to Trino, Databricks SQL, and Snowflake.
2. Use terminal, HTTP, Flight SQL, ADBC, JDBC, and certified ODBC clients.
3. Carry one governed session and query lifecycle across protocols.
4. Observe, cancel, audit, retain, and share warehouse queries consistently.
5. Write against a certified SQL subset and translate explicitly to another
   supported engine.
6. Route eligible workloads using authorization, capability, health, and cost
   policy.

qcli remains a gateway, not a warehouse: it governs and translates access while
the selected engine performs the computation.
