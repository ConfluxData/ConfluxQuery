# qcli Rust Execution Plan

Status: Proposed

Related design: [Product and Technical Design](product-design.md)

Initial engines: Trino, Databricks SQL, Snowflake

## 1. Objective

Deliver qcli as a reliable Rust query client for analytical platforms, with:

- One reusable execution core shared by interactive CLI, batch CLI, and HTTP.
- Named targets loaded from the sectioned `~/.qcli/.env` file.
- Trino, Databricks SQL, and Snowflake adapters.
- Versioned sessions and immutable query snapshots.
- Asynchronous query status, cancellation, metrics, and streaming results.
- Consistent table and machine-readable output.
- Clear extension points for new engines, frontends, authentication methods, result formats, and state stores.

The plan prioritizes a thin but complete vertical path before breadth. Every milestone must leave the repository in a usable and tested state.

## 2. Engineering principles

### 2.1 Stable core, replaceable edges

Business concepts belong in reusable core crates. Protocols and presentation belong at the edges.

```text
                         ┌──────────────┐
                         │ Interactive  │
                         │ CLI          │
                         └──────┬───────┘
                                │
┌───────────┐             ┌─────▼──────┐             ┌──────────────┐
│ Batch CLI │────────────►│ qcli-core  │◄────────────│ HTTP service │
└───────────┘             │ sessions   │             └──────────────┘
                          │ queries    │
                          │ results    │
                          └─────┬──────┘
                                │
                    ┌───────────▼───────────┐
                    │ engine adapter API    │
                    └─────┬──────┬──────┬───┘
                          │      │      │
                     Trino   Databricks Snowflake
```

Frontends must never contain engine protocol logic. Adapters must never render terminal output or HTTP responses.

### 2.2 Native SQL pass-through

qcli sends SQL to the selected engine without requiring a local semantic parse. Local SQL processing is limited to statement boundary detection, highlighting, completion context, and optional warnings.

### 2.3 Capability-driven behavior

Engines do not behave identically. Adapters report capabilities such as cancellation, compute switching, role switching, query metrics, or profile URLs. Core and frontends use capability checks rather than engine-name conditionals.

### 2.4 Immutable execution context

A query runs from an immutable snapshot of target, catalog, schema, compute, role, engine properties, and applicable qcli options. Later session changes cannot affect it.

### 2.5 Exact data before pretty data

The internal value and result model preserves precision and types. Display transformations such as decimal shortening and string truncation happen only in human-oriented rendering.

### 2.6 Bounded resources

Rows are streamed in bounded batches. Memory, disk spill, history, metadata caches, sessions, concurrent queries, and retained HTTP results all have explicit limits.

### 2.7 Secure by construction

Secrets use redacted types, not ordinary strings passed throughout the program. The design prevents credentials from reaching logs, errors, history, debug output, or HTTP responses.

Authentication is a replaceable provider layer beneath each engine adapter. Query
execution depends on acquired credentials, not on PAT-, password-, OAuth-, or
workload-specific fields. The first Databricks and Snowflake providers are PAT and
username/password respectively; later providers must not change core query or
frontend contracts. See [ADR-003](adr-003-extensible-authentication.md).

### 2.8 Prefer internal APIs before external plugin APIs

The first releases use Rust traits and workspace crates as extension points. A stable third-party plugin ABI is deferred until the internal adapter contract has survived multiple real engines.

### 2.9 Vertical delivery

Build one end-to-end Trino path before implementing all engines or all commands. This validates the abstractions with running software and avoids designing unused generality.

## 3. Proposed Rust workspace

Start with these crates:

```text
qcli/
├── Cargo.toml
├── crates/
│   ├── qcli-core/
│   ├── qcli-config/
│   ├── qcli-driver-api/
│   ├── qcli-driver-trino/
│   ├── qcli-output/
│   ├── qcli-cli/
│   └── qcli-test-support/
└── docs/
```

Add these only when their milestones begin:

```text
qcli-driver-databricks/
qcli-driver-snowflake/
qcli-repl/
qcli-metadata/
qcli-http/
```

Avoid creating a crate for every concept immediately. A new crate is justified when it establishes a dependency boundary, has multiple consumers, or isolates a large optional dependency.

### 3.1 `qcli-core`

Owns platform-neutral application behavior:

- Session IDs, session state, versions, mutations, snapshots, and expiration.
- Query IDs, lifecycle state machine, handles, status, events, and cancellation.
- Query submission orchestration.
- Common errors and error phases.
- Common target/context types.
- Result stream contracts.
- Time and metrics model.
- Concurrency and resource policies.

Must not depend on Clap, HTTP frameworks, line editors, or concrete engine adapters.

### 3.2 `qcli-config`

Owns:

- `~/.qcli/.env` grammar.
- `[default]` and target discovery.
- Environment substitution.
- Typed property validation.
- Default/target/session/query precedence support.
- File permission validation.
- Secret and redacted value types.
- Resolved configuration diagnostics.

It may depend on driver property schemas through a registration interface, but must not depend on concrete network clients.

### 3.3 `qcli-driver-api`

Owns the internal engine adapter contract:

- Adapter factory and target validation.
- Connection/session realization.
- Query submission and result streaming.
- Query status, events, metrics, and cancellation.
- Context and engine session properties.
- Metadata discovery.
- Capability reporting.
- Native error conversion.

This crate should be deliberately small. Types shared with core should live in one clearly chosen crate to avoid cyclic dependencies.

### 3.4 Engine adapter crates

Each adapter owns only its platform:

- Protocol/client library selection.
- Authentication.
- Request/session configuration.
- Native type conversion.
- Native state and metrics mapping.
- Metadata implementation.
- Driver-specific configuration schema.
- Integration tests and fixtures.

Adapters must pass a common conformance suite from `qcli-test-support`.

### 3.5 `qcli-output`

Owns:

- Arrow or common batch-to-output conversion.
- Table and vertical rendering.
- CSV, TSV, JSON, and JSONL serialization.
- Display-only decimal and string transformations.
- TTY-aware styling.
- Streaming and broken-pipe behavior.

It must not know how queries are executed.

### 3.6 `qcli-cli`

Owns:

- Executable entry point.
- CLI argument parsing.
- Batch command/file/stdin behavior.
- Target picker startup.
- Composition of core services and registered adapters.
- Process exit codes and stdout/stderr separation.

It should contain minimal business logic.

### 3.7 `qcli-repl`

Owns:

- Line editing and history.
- Multiline statement collection.
- Meta-command parsing and routing.
- Prompt rendering.
- Completion frontend.
- Ctrl-C/Ctrl-D behavior.

It calls core services for sessions and queries.

### 3.8 `qcli-metadata`

Owns normalized metadata models, capability-aware discovery, cache keys, TTLs, invalidation, and background refresh. Add it when at least two consumers or two engines need shared metadata behavior.

### 3.9 `qcli-http`

Owns HTTP transport only:

- Routes and versioned API DTOs.
- Authentication/authorization middleware.
- Session and query ownership.
- Content negotiation.
- SSE serialization.
- HTTP pagination tokens.
- Result retention and service quotas.

It calls the same `SessionManager` and `QueryService` as the terminal.

### 3.10 `qcli-test-support`

Owns reusable test components:

- Fake adapter.
- Deterministic clocks and IDs.
- Adapter conformance suite.
- Result fixtures and golden helpers.
- Cancellation and lifecycle test utilities.
- Secret-leak assertions.

This crate prevents every adapter and frontend from reinventing integration scaffolding.

## 4. Core contracts to stabilize early

Exact Rust syntax is intentionally excluded from this plan. The following semantic contracts must be agreed before broad implementation.

### 4.1 Adapter capabilities

At minimum:

```text
stream_results
cancel_query
list_catalogs
list_schemas
list_objects
describe_object
change_compute
change_role
session_properties
query_metrics
query_profile_url
```

Capabilities may include associated details, not only booleans. For example, cancellation may be synchronous, asynchronous, or best-effort.

### 4.2 Query handle

Submission returns a query handle that supports:

```text
qcli query ID
optional engine query ID
current status
event stream
result batch stream
metrics snapshot
cancel operation
final outcome
```

No frontend should need the concrete adapter type after submission.

### 4.3 Session snapshot

The snapshot contains only resolved execution state. It does not contain mutable session references. It includes session ID/version for traceability and redacted target identity for diagnostics.

### 4.4 Result stream

The stream carries:

- Schema once.
- Bounded batches.
- Optional progress/metrics separately from rows.
- Final completion or structured error.

Rows should use Apache Arrow where adapter feasibility confirms it. If one initial driver cannot produce Arrow directly, conversion belongs inside that adapter.

### 4.5 Error model

Errors include:

- Phase.
- qcli code.
- Safe human message.
- Optional native engine code/message.
- qcli and engine query IDs.
- Retryability classification where known.
- Redacted diagnostic context.
- Source error retained internally without leaking secrets.

### 4.6 Configuration property registry

Portable properties and engine-specific properties need typed schemas containing:

- Name and aliases.
- Value type.
- Scope: global, target, session, or query.
- Default.
- Whether target override is allowed.
- Whether secret.
- Validation and help text.

This registry should drive configuration validation, `config show`, `\set` help, and HTTP validation.

## 5. Authoritative demo milestone sequence

This is the sequence used to implement and track qcli. Work begins on the first incomplete milestone and does not advance until its demo and exit gate pass.

| Milestone | Status | Demonstrable outcome |
|---|---|---|
| M1 — Configuration and target discovery | Complete | Inspect and validate section-defined targets |
| M2 — End-to-end demo query | Complete | Execute a query through core using the demo adapter |
| M3 — Batch output and automation | Complete | Stream exact results in all initial machine formats |
| M4 — Real Trino execution | Complete | Execute and cancel native queries on Trino |
| M5 — Interactive terminal | Complete | Use qcli as an interactive Trino shell |
| M6 — Target switching and navigation | Complete | Switch targets and browse warehouse metadata |
| M7 — Databricks SQL | Complete | Use the same qcli workflow with Databricks SQL |
| M8 — Snowflake | Complete | Use the same qcli workflow with Snowflake |
| M9 — Unified release candidate | Complete | Run consistent workflows across all three engines |
| M10 — Local HTTP query service | Complete | Manage sessions and queries over localhost HTTP |
| M11 — Production HTTP service | Pending | Secure multi-user HTTP operation with bounded resources |
| M12 — Packaged release | Pending | Install and run signed cross-platform artifacts |

Allowed status values are `Pending`, `In progress`, `Blocked`, and `Complete`. A milestone becomes `Complete` only after its automated tests, live or deterministic demo, documentation, and milestone report are present.

### M1 — Configuration and target discovery

Demo:

```text
qcli config check
qcli target list
qcli target show trino-dev
```

Must demonstrate:

- A working Rust executable and workspace CI.
- Parsing the sectioned `~/.qcli/.env` format.
- `[default]` inheritance.
- Every non-default section discovered as a target.
- Environment substitution and typed properties.
- Secret redaction and file-permission validation.
- Target-specific overrides such as `decimal_places` and `string_truncate`.

Exit gate:

- Product-document configuration examples parse in automated tests.
- Invalid configuration reports section, property, and source location.
- No inspection command can expose a resolved secret.

### M2 — End-to-end query with a demo adapter

Demo:

```text
qcli --target demo --command "select * from sample"
```

Must demonstrate:

- A versioned logical session.
- An immutable query snapshot.
- Adapter registration without frontend engine conditionals.
- Query lifecycle and qcli query ID.
- Bounded Arrow result batches.
- Human table rendering.
- Display-only decimal shortening and string truncation.
- Structured success and failure outcomes.

Exit gate:

- A deterministic demo adapter exercises submit, events, results, completion, failure, and cancellation.
- Core has no dependency on CLI, REPL, HTTP, or concrete engine clients.

### M3 — Batch output and automation

Demo:

```text
qcli --target demo --command "select * from sample" --format csv
qcli --target demo --file report.sql --format jsonl
generate_sql | qcli --target demo --file -
```

Must demonstrate:

- Table, vertical, CSV, TSV, JSON, and JSONL formats.
- Exact machine values regardless of display shortening.
- Query files, direct commands, and SQL from stdin.
- Stable stdout/stderr separation and exit codes.
- Bounded-memory streaming and clean broken-pipe behavior.

Exit gate:

- A generated million-row CSV/JSONL result streams with bounded memory.
- Golden tests cover exact values, NULL, Unicode, nested types, and output errors.

### M4 — Real Trino execution

Demo:

```text
qcli target test trino-dev
qcli --target trino-dev --command "select current_catalog, current_schema"
```

Must demonstrate:

- Required Trino authentication and TLS.
- Native SQL pass-through.
- Remote query ID and lifecycle states.
- Paginated result conversion to common batches.
- Trino values, errors, progress metrics, and session properties.
- Confirmed or explicitly unconfirmed cancellation.

Exit gate:

- Execute, stream, export, time, and cancel against containerized or designated Trino.
- No Trino-specific conditional exists in core, output, or frontend behavior.

### M5 — Interactive terminal

Demo:

```text
qcli
# Pick trino-dev
select count(*) from events;
\status
\set decimal_places 8
\properties
```

Must demonstrate:

- Interactive target picker.
- Multiline native SQL.
- Prompt and continuation prompt.
- Query history with sensitive-query filtering.
- Syntax highlighting and query buffer commands.
- Ctrl-C cancellation and Ctrl-D behavior.
- Session option, property, timing, and format changes.

Exit gate:

- Pseudo-terminal tests reproduce the core interactive flow.
- The REPL uses `SessionManager` and `QueryService` rather than concrete adapters.

### M6 — Target switching and warehouse navigation

Demo:

```text
\targets
\catalogs
\schemas
\tables event*
\describe events
\use trino-staging
```

Must demonstrate:

- Atomic target switching and failed-switch preservation.
- Catalog/schema context changes.
- Catalog, schema, table/view, and column discovery.
- Context-aware completion.
- Scoped metadata cache and invalidation.
- Session version changes while existing queries retain their snapshots.

Exit gate:

- Metadata APIs are reusable by terminal and HTTP.
- Target, identity, role, catalog, and schema cannot leak cache entries across contexts.

### M7 — Databricks SQL

Demo:

```text
qcli target test databricks-dev
qcli --target databricks-dev --command "select current_catalog(), current_schema()"
qcli
# Pick databricks-dev, then run \tables
```

Must demonstrate:

- PAT authentication through the Databricks credential-provider boundary.
- Rejection of ambiguous or unsupported authentication configuration before a query is submitted.
- Statement submission, polling, results, and cancellation.
- SQL warehouse and catalog/schema context.
- Databricks query IDs, errors, metrics, and metadata.
- The same CLI and result behavior already demonstrated with Trino.

Exit gate:

- Databricks passes the shared adapter conformance profile.
- Contract refinements are capability-based and not Databricks branches in generic code.
- The selected client permits credential injection/renewal for future OAuth and workload providers without replacing the query lifecycle.

### M8 — Snowflake

Demo:

```text
qcli target test snowflake-prod
qcli
# Pick snowflake-prod
\status
\use-role ANALYST
\use-compute REPORTING_WH
```

Must demonstrate:

- Username/password authentication through the Snowflake credential-provider boundary, with TLS required for credentials.
- Rejection of ambiguous or unsupported authentication configuration before a connection is opened.
- Query execution, results, query ID, cancellation, and errors.
- Warehouse, role, database/catalog, and schema changes.
- Exact decimal and timestamp handling.
- Metadata and available metrics/profile links.
- Safe context realization when connections are reused.

Exit gate:

- Snowflake passes the shared adapter conformance profile.
- Concurrent logical sessions cannot leak context through pooled connections.
- The selected client can add key-pair, OAuth, browser/SSO, programmatic-token, and workload providers without changing core or frontend contracts.

Implementation was accepted for Phase 1 without a Snowflake test account. The
unverified live portions of this exit gate are explicitly inherited by M9; they
are not treated as proven behavior.

### M9 — Unified three-engine release candidate

Demo:

```text
qcli --target trino-dev --file validation.sql --format jsonl
qcli --target databricks-dev --file validation.sql --format jsonl
qcli --target snowflake-prod --file validation.sql --format jsonl
```

Must demonstrate:

- Consistent commands, output, errors, cancellation, and metrics.
- Explicit unsupported-capability responses.
- Cross-engine type and metadata behavior.
- Stable version-one configuration and CLI compatibility policy.
- Large-result reliability and bounded memory.
- The deferred Snowflake M8 live matrix: password login, session renewal,
  multi-chunk results, exact decimals/timestamps, metadata, context isolation,
  and failure behavior.
- An explicit decision for Snowflake query IDs and server-side cancellation:
  upstream client enhancement, adapter extension, transport pivot, or a declared
  unsupported capability for the release candidate.
- The deferred Databricks live edge cases, including qualified catalog/schema
  switching and large-result chunk behavior.

Exit gate:

- All three engines pass their required conformance profiles.
- Cross-engine scenarios and fault-injection tests pass.

Reusable automated gate:

```text
# Defaults to ~/.qcli/.env and targets trino, databricks-dev, snowflake-dev.
cargo test -p qcli --test milestone9 \
  live_three_engine_portable_query_profile -- --ignored --exact

# Select another configuration or section names without duplicating credentials.
QCLI_M9_CONFIG=/path/to/qcli.env \
QCLI_M9_TRINO_TARGET=trino-local \
QCLI_M9_DATABRICKS_TARGET=databricks-dev \
QCLI_M9_SNOWFLAKE_TARGET=snowflake-dev \
cargo test -p qcli --test milestone9 \
  live_three_engine_portable_query_profile -- --ignored --exact
```

The gate reads resolved target sections through `qcli-config`; secrets are not
accepted as test command-line arguments and are not printed by the harness.

### M10 — Local HTTP query service

Demo:

```text
qcli serve
POST /v1/sessions
POST /v1/sessions/{session_id}/queries
GET  /v1/queries/{query_id}
GET  /v1/queries/{query_id}/events
GET  /v1/queries/{query_id}/results
POST /v1/queries/{query_id}/cancel
```

Must demonstrate:

- Versioned persistent HTTP sessions.
- Stateless query submission.
- Immutable query snapshots.
- Status, cancellation, pagination, and SSE progress.
- JSON, JSONL, CSV, and Arrow-stream responses.
- Reuse of the same session/query services and engine adapters as the terminal.

Exit gate:

- Terminal and HTTP execution parity tests pass.
- The preview binds to loopback and uses development authentication.
- Memory and retained results are bounded.

### M11 — Production HTTP service

Demo:

- Authenticate two independent callers.
- Prove per-target authorization.
- Prove one caller cannot access another caller's sessions, queries, or results.
- Demonstrate concurrency quotas, disk spill, session expiry, result expiry, and graceful shutdown.
- Inspect audit events that contain no credentials or SQL text by default.

Must demonstrate:

- Production authentication and authorization integration.
- Caller ownership enforcement.
- Query, session, request, memory, disk, and retention limits.
- TLS or trusted-proxy deployment policy.
- CORS restrictions and safe network binding.
- Defined active-query behavior during shutdown and session expiration.

Exit gate:

- Multi-user security and resource-exhaustion suites pass.
- Operational deployment and recovery documentation is complete.

### M12 — Packaged release

Demo:

```text
brew install qcli
qcli --version
qcli config check
qcli
```

Equivalent clean-machine installation demos must pass for supported Linux, macOS, and Windows channels.

Must demonstrate:

- Signed binaries, checksums, and software bill of materials.
- Shell completion and manual pages.
- Upgrade and downgrade compatibility.
- Supported engine/client version matrix.
- Installation, authentication, and troubleshooting documentation.

Exit gate:

- Clean-machine smoke tests pass on every supported platform.
- Published artifacts reproduce the documented capabilities.

### Milestone completion artifacts

Every milestone produces:

1. A runnable user-facing demo.
2. Automated tests that reproduce the demo without relying solely on manual verification.
3. Updated user and architecture documentation.
4. A milestone report listing delivered behavior, known limitations, evidence, and prerequisites for the next milestone.

## 6. Detailed implementation work packages

### Work package 0: Repository and decision baseline

Purpose: create a maintainable foundation before feature work.

Tasks:

- Initialize the Cargo workspace and pinned Rust toolchain policy.
- Select license, minimum supported Rust version, formatting, lint, and dependency policies.
- Add CI for format, Clippy, unit tests, documentation, and vulnerability/license checks.
- Establish conventional error and logging policies.
- Record architecture decisions for DataFusion independence, Arrow result model, async runtime, and initial adapter strategy.
- Establish dependency update automation and release profile defaults.
- Create contribution and testing documentation.

Gate:

- A clean workspace builds on Linux, macOS, and Windows CI.
- Warnings fail CI for qcli-owned crates.
- No production feature exists only in the binary crate.

Deliverable: buildable workspace skeleton and engineering policy.

### Work package 1: Configuration and target resolution

Purpose: make qcli's target model independently usable and thoroughly specified.

Tasks:

- Implement the sectioned `.env` lexer/parser without using conventional dotenv semantics.
- Discover every non-`[default]` section as a target.
- Implement comments, quoting, typed scalars, durations, and environment substitution.
- Implement portable property schemas and engine schema registration.
- Resolve built-in, default, target, session, and query layers.
- Add secret/redacted types and safe debug formatting.
- Validate Unix permissions and define Windows ACL behavior.
- Implement `qcli config path`, `config check`, and redacted `config show`.
- Implement `qcli target list` and `target show` without making network calls.

Tests:

- Parser golden tests with line/column diagnostics.
- Property and fuzz tests for arbitrary text.
- Precedence matrix.
- Duplicate/unknown property suggestions.
- Secret leakage tests across success and every error class.
- Unix permission tests.

Gate:

- The product document's configuration examples parse.
- Target-specific `decimal_places` and `string_truncate` override `[default]`.
- Resolved configuration output cannot expose secrets.

Deliverable: reusable configuration crate and inspection commands.

### Work package 2: Core sessions and fake engine

Purpose: validate reusable state and lifecycle design before a real protocol constrains it.

Tasks:

- Implement session IDs, state, versions, mutations, snapshots, and close/expiry.
- Implement portable qcli options and engine property separation.
- Implement atomic target switching semantics.
- Implement query IDs and lifecycle state machine.
- Implement query event and result stream contracts.
- Implement cancellation states and final outcomes.
- Implement a deterministic fake adapter.
- Implement session/query orchestration without any frontend dependency.
- Add configurable clocks and ID sources for tests.

Tests:

- Stale session mutations conflict.
- Concurrent queries receive independent immutable snapshots.
- Later context mutations never affect active queries.
- Failed target switches preserve the original state.
- Cancellation covers pre-submit, queued, running, result-streaming, confirmed, and unconfirmed cases.
- Session expiry releases resources but follows active-query policy.

Gate:

- The fake adapter can exercise a complete submit/status/results/cancel flow.
- Core has no dependency on terminal, HTTP, or concrete drivers.

Deliverable: frontend-neutral `SessionManager` and `QueryService`.

### Work package 3: Output pipeline

Purpose: establish exact, streaming results before real engines multiply type behavior.

Tasks:

- Finalize the common Arrow result boundary.
- Implement bounded batch streaming.
- Implement table and vertical renderers.
- Implement CSV, TSV, JSON, and JSONL.
- Define JSON envelope and exact decimal encoding.
- Apply decimal shortening and string truncation only to human renderers.
- Define NULL, binary, temporal, nested, and invalid text behavior.
- Add TTY-aware format/color selection and broken-pipe handling.
- Establish stdout/stderr separation.

Tests:

- Golden output for all formats.
- Exact decimal preservation.
- Human-only truncation.
- Nested Arrow types.
- Unicode width and terminal sizing.
- Large bounded-memory streams.
- Broken pipe and partial writer failure.

Gate:

- One million generated rows can stream through CSV/JSONL with bounded memory.
- Machine output is independent of locale and display settings.

Deliverable: reusable output crate usable by CLI and HTTP.

### Work package 4: Trino vertical slice

Purpose: prove the architecture with one real warehouse engine.

Tasks:

- Select and document the Trino Rust protocol/client approach.
- Implement target schema and minimum authentication.
- Implement connection validation.
- Submit native SQL without semantic rewriting.
- Map Trino query IDs and lifecycle states.
- Stream result pages into Arrow batches.
- Map Trino types and structured errors.
- Implement cancellation and session property propagation.
- Capture available progress metrics.
- Implement `target test` for Trino.
- Connect batch CLI to config, session, query, and output services.

CLI paths:

```text
qcli --target trino --command "select 1"
qcli --target trino --file query.sql
qcli --target trino --file -
```

Tests:

- Containerized Trino integration suite.
- Authentication failure and redaction.
- Native Trino SQL pass-through.
- Complex and decimal type conversion.
- Large paginated result.
- Cancellation and remote query ID.
- Timeout and server error mapping.

Gate:

- A user can reliably execute, stream, export, time, and cancel a Trino query.
- No Trino conditionals appear in frontend crates.

Deliverable: first useful qcli batch release.

### Work package 5: Interactive shell

Purpose: make the Trino vertical slice a productive terminal client.

Tasks:

- Select a line editor based on multiline, completion, history, signal, and cross-platform tests.
- Implement statement boundary detection with Trino syntax fixtures.
- Implement prompt and continuation prompt.
- Implement history with sensitive-query filtering.
- Implement meta-command parser independently from SQL parsing.
- Add `\targets`, `\use`, `\status`, `\set`, `\reset`, `\properties`, `\set-property`, `\unset-property`, `\format`, `\timing`, `\cancel`, and `\quit`.
- Implement atomic target switching through core.
- Implement Ctrl-C/Ctrl-D and bracketed paste.
- Add query buffer print, clear, edit, write, and include.

Tests:

- Pseudo-terminal integration tests.
- Multiline strings, identifiers, comments, and pasted SQL.
- Unicode editing.
- Query cancellation versus input-buffer clearing.
- Failed target switching.
- History secret detection.

Gate:

- Terminal commands use the same core services as batch mode.
- No REPL command directly invokes a concrete adapter.

Deliverable: interactive Trino client.

### Work package 6: Metadata and completion

Purpose: add warehouse-native navigation without coupling it to the REPL.

Tasks:

- Define normalized catalog, schema, object, column, and type metadata.
- Add metadata methods to the capability contract where necessary.
- Implement Trino discovery.
- Implement scoped caching, TTL, invalidation, and background refresh.
- Add `\catalogs`, `\schemas`, `\tables`, `\describe`, `\use-catalog`, and `\use-schema`.
- Implement completion provider independent of the selected line editor.
- Include target and property completion.

Tests:

- Cache isolation by target, identity, role, catalog, and schema.
- Context mutation invalidation.
- Completion remains functional during partial metadata failures.
- Permission-denied metadata responses degrade gracefully.

Gate:

- Metadata logic is reusable by terminal and future HTTP endpoints.

Deliverable: warehouse discovery and context-aware completion.

### Work package 7: Databricks SQL adapter

Purpose: prove the adapter contract against a statement-oriented warehouse API.

Tasks:

- Score driver/API candidates on authentication extensibility, query functionality, performance path, maintenance, and dependency cost.
- Implement the shared credential-provider contract and Databricks PAT provider.
- Keep token acquisition separate from Statement Execution request construction so OAuth and workload providers can be added independently.
- Implement statement submission, status polling, result retrieval, and cancellation.
- Convert results to common Arrow batches.
- Map catalog/schema and SQL warehouse context.
- Implement query IDs, metrics, errors, and metadata.
- Run the shared adapter conformance suite.
- Add cloud integration tests behind protected CI configuration.

Gate:

- No core contract change is made solely by testing `engine == databricks`.
- Any necessary extension is capability-based and applicable to future adapters.

Deliverable: production-capable Databricks target.

### Work package 8: Snowflake adapter

Purpose: validate connection-oriented context, roles, and warehouse switching.

Tasks:

- Score Rust client/API candidates on authentication extensibility, query functionality, performance path, maintenance, and dependency cost.
- Implement the Snowflake username/password provider through the shared credential-provider contract.
- Prove that connection authentication can later accept key-pair, OAuth, browser/SSO, programmatic-token, profile, and workload providers.
- Implement target schema and TLS behavior.
- Implement query submission, query ID, results, cancellation, and errors.
- Map database to qcli catalog.
- Implement warehouse, role, database, schema, and session parameter realization.
- Decide sticky connection versus explicit initialization based on correctness and measurement.
- Convert native results to Arrow.
- Implement metadata and available metrics/profile links.
- Run adapter conformance and protected cloud tests.

Gate:

- Concurrent queries from one logical session cannot leak context across connections.
- Target switching and connection reuse preserve isolation.

Deliverable: production-capable Snowflake target.

### Work package 9: Cross-engine hardening

Purpose: stabilize the common product rather than leaving three separate clients behind one command.

Tasks:

- Run the same behavioral scenarios across all adapters.
- Remove accidental engine conditionals from core and frontends.
- Normalize errors and metrics without discarding native information.
- Validate context and property behavior across engines.
- Benchmark startup, connection, first-row latency, rendering, and memory.
- Add fault injection for dropped connections, partial pages, timeouts, and cancellation races.
- Freeze version-one configuration and CLI compatibility policy.
- Complete user documentation and troubleshooting guides.

Gate:

- All three adapters pass the required conformance profile.
- Unsupported capabilities fail explicitly.
- CLI and batch behavior is consistent across engines.

Deliverable: first stable CLI release candidate.

### Work package 10: HTTP service

Purpose: expose the same reusable execution core safely over HTTP.

Tasks:

- Select the HTTP framework and define versioned DTOs separately from core types.
- Implement authentication and target authorization hooks.
- Implement session create/read/mutate/switch/close with optimistic versions.
- Implement session query and stateless query submission.
- Implement status, cancellation, paginated results, and SSE events.
- Implement JSON, JSONL, CSV, and Arrow-stream content negotiation.
- Implement result memory limits, disk spill, TTL, quotas, and cleanup.
- Enforce query/session ownership and unguessable IDs.
- Implement graceful shutdown and active-query policy.
- Add loopback-by-default binding, CORS restrictions, request limits, and TLS/proxy configuration.
- Add audit events without query text or credentials by default.

Tests:

- HTTP contract and schema snapshots.
- Terminal/HTTP execution parity.
- Stale session mutation conflicts.
- Cross-caller access denial.
- Target authorization.
- SSE reconnect and terminal events.
- Pagination token integrity.
- Result expiration during access.
- Resource exhaustion and quota behavior.
- Shutdown during active queries.

Gate:

- HTTP calls the same core services as terminal execution.
- The service cannot access another caller's sessions, queries, or results.
- Memory and disk usage remain bounded under load.

Deliverable: `qcli serve` preview.

### Work package 11: Release and operational maturity

Purpose: make qcli installable and supportable.

Tasks:

- Produce signed Linux, macOS, and Windows binaries.
- Generate checksums and software bill of materials.
- Add shell completions and manual pages.
- Establish semantic versioning and configuration migration rules.
- Add Homebrew and selected package distribution.
- Document proxy, TLS, authentication, logging, and HTTP deployment.
- Add upgrade, downgrade, and compatibility smoke tests.
- Establish supported engine/client version matrix.

Gate:

- Clean-machine installation tests pass.
- Release artifacts reproduce documented capabilities.
- Vulnerability and license checks are clean or explicitly reviewed.

Deliverable: supported qcli release.

## 7. Workstream ownership

These workstreams can proceed concurrently only after their dependencies stabilize:

| Workstream | Starts after | Main outputs |
|---|---|---|
| Configuration | Work package 0 | Parsed and resolved targets |
| Core lifecycle | Work package 0 | Sessions, snapshots, queries |
| Output | Core result contract draft | Renderers and serializers |
| Trino | Config + core contract | First real adapter |
| REPL | Core + Trino thin path | Interactive client |
| Metadata | Adapter capability baseline | Discovery and completion |
| Databricks | Trino contract review | Second adapter |
| Snowflake | Trino contract review | Third adapter |
| HTTP | Stable session/query core | Service frontend |
| Release | Stable cross-engine behavior | Packages and support policy |

The second adapter is an architecture test. Expect small contract refinements, but reject changes that expose Databricks-specific concepts directly through generic frontend APIs. The third adapter should require fewer core changes; otherwise the abstraction remains too narrow.

## 8. Quality gates applied to every milestone

### 7.1 Correctness

- Unit and integration tests accompany behavior.
- Async operations are cancellation-safe.
- State transitions are explicit and validated.
- Machine formats preserve exact values.
- Errors contain actionable context and stable classifications.

### 7.2 Reusability

- New behavior is placed in the lowest appropriate reusable layer.
- Frontends do not import concrete adapter crates except in composition/bootstrap code.
- Adapter crates do not import CLI, REPL, or HTTP crates.
- Shared tests use public internal contracts rather than private implementation details.

### 7.3 Extensibility

- Unsupported behavior is represented through capabilities.
- New engines can be registered without editing query orchestration.
- New output formats consume the common result stream.
- New frontends consume session/query services.
- New session stores do not change query adapter contracts.

### 7.4 Security

- Secret-leak tests pass.
- Dependencies and licenses are reviewed.
- Unsafe Rust is forbidden unless separately justified and audited.
- Network defaults verify TLS.
- Logs and errors are safe at every supported verbosity.

### 7.5 Performance

- Streaming paths have bounded memory tests.
- No unbounded channel exists in query/result processing.
- Blocking operations are isolated from async executors.
- Benchmarks detect meaningful regressions in hot paths.

### 7.6 Portability

- Linux, macOS, and Windows CI pass.
- Terminal-specific behavior has fallbacks.
- Filesystem permissions and paths use platform abstractions.

## 9. Dependency selection criteria

Before adding a major dependency, record:

- Maintenance activity and release cadence.
- License compatibility.
- Security history.
- Minimum supported Rust version impact.
- Native library or OpenSSL requirements.
- Cross-compilation and Windows support.
- Async runtime compatibility.
- Feature flag and binary-size impact.
- Ability to expose native query IDs, cancellation, streaming, and authentication.
- Whether qcli can wrap it without leaking dependency-specific types into core APIs.

Critical dependency decisions requiring a short ADR:

- Trino client/protocol implementation.
- Databricks Statement Execution or driver strategy.
- Snowflake driver/API strategy.
- Arrow version and result representation.
- Line editor.
- HTTP framework.
- TLS stack.
- Secret storage integration.

## 10. Testing pyramid

### Fast tests on every change

- Unit tests.
- Parser/property tests.
- Core lifecycle state tests.
- Fake adapter conformance.
- Output golden tests.
- Secret-leak tests.

### Repository integration tests

- CLI subprocess tests.
- Pseudo-terminal REPL tests.
- Containerized Trino.
- HTTP service contract tests.
- Disk spill and cleanup tests.

### Protected external tests

- Databricks SQL test warehouse.
- Snowflake test account.
- Authentication variants.
- Proxy/TLS deployments.
- Cancellation of genuinely long-running queries.

### Scheduled tests

- Large-result and memory tests.
- Fuzzing.
- Dependency vulnerability scans.
- Cross-platform release smoke tests.
- Engine version compatibility matrix.

## 11. Documentation deliverables

Documentation is part of each feature, not a final milestone.

Required documents:

- Installation and first query.
- Configuration grammar and complete property reference.
- Trino, Databricks, and Snowflake authentication guides.
- Interactive command reference.
- Output and exact-value semantics.
- Exit codes and automation contract.
- HTTP API specification and examples.
- Security and deployment guide.
- Adapter author guide after the contract stabilizes.
- Troubleshooting by error phase.
- Architecture decision records.

## 12. Versioning strategy

### Before `1.0`

- Internal Rust APIs may evolve.
- User-facing configuration and CLI changes require release notes and migration guidance.
- HTTP endpoints remain under `/v1`, but preview releases may declare selected fields unstable.

### At `1.0`

Stabilize:

- `.env` grammar and precedence.
- Target identity rules.
- Core CLI options and exit codes.
- Machine-output schemas.
- HTTP endpoint semantics.
- Session/query lifecycle states.

The internal adapter Rust trait need not become a public ABI at `1.0`.

## 13. Risk-driven spikes

Complete these short, time-boxed investigations before committing to adapter implementations:

### Trino spike

- Authenticate against a representative deployment.
- Submit native SQL.
- Obtain query ID and progress.
- Stream pages.
- Cancel reliably.
- Convert decimals, arrays, maps, rows, and timestamps.

### Databricks spike

- Validate PAT authentication first.
- Inventory OAuth M2M, OAuth U2M/browser, Databricks profile, supplied OAuth token, and OIDC/workload identity support.
- Confirm credentials can be injected and renewed independently of query execution.
- Submit and poll a statement.
- Retrieve inline and external/chunked results.
- Cancel.
- Confirm Arrow availability and metadata APIs.

### Snowflake spike

- Validate the chosen Rust client in target environments.
- Exercise username/password authentication first.
- Inventory key-pair JWT, OAuth, external browser/SSO, programmatic access token, Snowflake profile, and workload identity support.
- Confirm authentication can be extended without replacing query/result handling.
- Obtain query ID, cancel, and retrieve large results.
- Switch warehouse, role, database, and schema safely.
- Confirm decimal and timestamp precision.

Each spike ends with an ADR: selected approach, authentication matrix,
limitations, dependency impact, required upstream changes, and fallback plan.
The initial selection is recorded in
[ADR-004](adr-004-databricks-snowflake-clients.md); live spikes validate or
supersede it.

## 14. Initial backlog order

The recommended first implementation sequence is:

1. Workspace and CI.
2. Secret/redacted types.
3. Sectioned configuration parser.
4. Property registry and target resolution.
5. Session state, version, and immutable snapshot.
6. Query lifecycle and fake adapter.
7. Common Arrow result stream.
8. CSV/JSONL streaming and table renderer.
9. Trino feasibility spike.
10. Trino adapter.
11. Batch CLI vertical slice.
12. Cancellation and metrics hardening.
13. Interactive shell.
14. Metadata and completion.
15. Databricks feasibility spike and adapter.
16. Snowflake feasibility spike and adapter.
17. Cross-engine conformance and stable CLI candidate.
18. HTTP sessions and stateless execution.
19. HTTP result retention and production security.
20. Packaging and supported release.

## 15. Definition of done

A feature is done only when:

- Its layer and extension boundary are clear.
- Public/internal contracts are documented.
- Unit and appropriate integration tests pass.
- Cancellation and error behavior are defined.
- Secret handling is reviewed.
- Memory behavior is bounded or explicitly limited.
- CLI and HTTP reuse is considered, even if HTTP delivery is later.
- User documentation is updated.
- Unsupported-engine behavior is explicit.
- No engine-specific workaround leaks into generic frontend code.

## 16. Success criteria

The execution plan succeeds when qcli can add a fourth engine without redesigning terminal, HTTP, session, query lifecycle, or output code. The expected work for a new engine should primarily be:

1. Define its target property schema.
2. Implement the adapter capabilities it supports.
3. Convert native results into the common batch representation.
4. Map native errors and metrics.
5. Implement metadata operations.
6. Pass the shared conformance suite.

Likewise, a new frontend should be able to submit and manage queries through core services without importing engine clients, and a new output format should consume result batches without knowing which engine produced them.
