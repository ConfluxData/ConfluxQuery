# qcli Product and Technical Design

Status: Proposed

Long-term capabilities: [Feature Roadmap](features-roadmap.md)

Initial engines: Trino, Databricks SQL, Snowflake

Implementation language: Rust

Primary configuration: `~/.qcli/.env`

## 1. Executive summary

qcli is an interactive and automation-friendly command-line query client for cloud data platforms. It provides one consistent workflow for selecting a configured target, discovering data, executing SQL, inspecting query progress, and exporting results across Trino, Databricks SQL, and Snowflake.

The same execution core is exposed through `qcli serve`. HTTP is the operational
control plane and Arrow Flight SQL is the standard remote SQL and Arrow data
plane. Terminal, HTTP, and Flight SQL clients share the same authentication,
authorization, session, adapter, query lifecycle, cancellation, metadata,
quota, audit, and result abstractions; neither service frontend may shell out to
the qcli executable.

qcli is not designed around PostgreSQL compatibility. Its common model is the model shared by analytical query platforms:

```text
target -> compute -> catalog -> schema -> object
```

Not every engine exposes every level. qcli presents the concepts that are meaningful for the active target and hides or marks unsupported concepts rather than inventing behavior.

The first release deliberately excludes transaction management. It concentrates on the workflow that matters most for analytical querying:

1. Start qcli.
2. Pick a target defined in `~/.qcli/.env`.
3. Inspect or change catalog, schema, compute, and session properties.
4. Enter and execute SQL.
5. Observe query status and engine metrics.
6. View, page, or export results.
7. Switch to another target without restarting the client.

## 2. Product identity

The product and executable are named `qcli`.

Suggested description:

> qcli — one query shell for cloud data platforms.

The name means “query command-line interface.” It avoids implying that qcli is a database, SQL dialect, or PostgreSQL clone. It also leaves room for additional query engines in the future.

## 3. Goals

### 3.1 Primary goals

- Offer a consistent interactive query experience across Trino, Databricks SQL, and Snowflake.
- Make named targets the primary connection model.
- Allow safe, fast target switching inside an interactive session.
- Standardize navigation around compute, catalog, schema, and object discovery.
- Stream large analytical result sets with bounded memory.
- Expose query IDs, lifecycle state, timing, and engine-provided metrics.
- Produce deterministic output suitable for shell pipelines and automation.
- Let defaults be overridden globally, per target, per invocation, and per session.
- Keep credentials out of command history and redact them from diagnostics.
- Provide useful errors without hiding native engine error information.

### 3.2 Secondary goals

- Feel familiar to users of warehouse CLIs without copying any one client.
- Make engine differences visible and understandable.
- Support metadata-aware completion.
- Allow future engines to be added through a stable internal adapter contract.
- Provide strong cross-platform binaries for Linux, macOS, and Windows.
- Expose query execution through a versioned HTTP API after the direct CLI path is stable.
- Expose a standards-compliant Arrow Flight SQL endpoint for ADBC, JDBC, ODBC,
  and native Flight SQL clients.
- Use one session and query model for terminal, HTTP, and Flight SQL state.
- Keep HTTP as the control/operations API and Flight SQL as the Arrow-native SQL
  data plane.
- Add an explicit, versioned SQL transpilation mode for a certified portable
  subset after native multi-engine execution is stable.

## 4. Non-goals for the first release

- Transaction lifecycle management.
- PostgreSQL or `usql` meta-command compatibility as a product requirement.
- A universal SQL dialect or automatic query translation.
- Cross-engine data copying.
- Database migrations.
- Data editing UI.
- Query charting or terminal graphics.
- Arbitrary third-party plugins or a stable external plugin ABI.
- A custom qcli ADBC driver while standard ADBC Flight SQL drivers can connect
  to qcli.
- Claiming universal JDBC or ODBC compatibility without client conformance
  testing.
- Complete implementation of every planned authentication mechanism on day one;
  the architecture must nevertheless support adding them without changing query,
  session, frontend, HTTP, or Flight SQL contracts.
- Normalizing every engine-specific query plan into one model.
- Predicting query cost before execution.
- Acting as a security boundary. Database permissions remain authoritative.

qcli may execute DDL and DML accepted by an engine. Not being transaction-aware does not mean qcli is read-only.

## 5. Users and use cases

### 5.1 Data engineer

- Switches between development Trino and production Snowflake.
- Inspects catalogs and schemas.
- Runs validation queries after a pipeline deployment.
- Exports a query result as CSV or JSON Lines.

### 5.2 Analyst

- Connects to a Databricks SQL warehouse.
- Uses completion to find tables and columns.
- Pages through results interactively.
- Changes output precision and string truncation for an exploratory session.

### 5.3 Platform engineer

- Tests whether configured targets are reachable.
- Diagnoses authentication, TLS, and warehouse configuration.
- Uses query IDs to correlate CLI failures with engine logs.

### 5.4 Automation user

- Runs one query against an explicit target.
- Receives only result data on stdout.
- Receives diagnostics on stderr.
- Relies on documented exit codes and deterministic serialization.

## 6. Common platform model

qcli uses the following neutral concepts:

| qcli concept | Trino | Databricks SQL | Snowflake |
|---|---|---|---|
| Target | Trino deployment | Workspace and SQL warehouse endpoint | Account and connection profile |
| Compute | Usually implied by target | SQL warehouse | Virtual warehouse |
| Catalog | Catalog | Unity Catalog catalog | Database |
| Schema | Schema | Schema | Schema |
| Object | Table, view, materialized view | Table, view, materialized view | Table, view, materialized view |
| Role | Engine/access configuration | Identity and entitlements | Active role |
| Query ID | Trino query ID | Statement/query identifier | Snowflake query ID |

An adapter reports which capabilities it supports. Commands must respond explicitly when a capability is unavailable.

Examples:

```text
trino-dev[hive.analytics]>
databricks-dev[shared-warehouse/main.default]>
snowflake-prod[ANALYST_WH/REPORTING.PUBLIC]>
```

Prompts must make the active target unambiguous. They must not contain passwords, tokens, or full connection URLs.

## 7. Configuration

### 7.1 Location and format

The primary configuration file is:

```text
~/.qcli/.env
```

Despite the `.env` filename, qcli uses a sectioned, INI-style format. Conventional dotenv files do not support section headers. qcli therefore owns the parsing rules for this file and must not silently treat it as a standard dotenv file.

Every section except `[default]` is a target. No `QCLI_TARGETS` list is required. Target discovery is performed by enumerating section headers.

```ini
[default]
decimal_places = 3
string_truncate = 80
output_format = table
timing = true
page_size = 1000

[trino]
engine = trino
url = https://trino.example.com
user = deepak
catalog = hive
schema = analytics
decimal_places = 10

[databricks-dev]
engine = databricks
auth_type = pat
host = https://dbc-example.cloud.databricks.com
http_path = /sql/1.0/warehouses/abc123
token = ${DATABRICKS_TOKEN}
catalog = main
schema = default

[snowflake-analytics]
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

The section name is the exact target identity shown by `qcli target list`, the interactive picker, the prompt, and `\use`.

### 7.2 Section rules

- `[default]` is reserved and cannot be selected as a target.
- Every other section defines exactly one target.
- Section names must be unique.
- Section names are case-sensitive in storage and display.
- A normalized case-insensitive match may be offered interactively only when it is unambiguous.
- Recommended section characters are letters, digits, `_`, `-`, and `.`.
- An empty target section is invalid.
- A target must define `engine` unless engine inference is unambiguous and documented. Explicit `engine` is recommended.
- Unknown properties are errors by default, with a file location and suggested correction.
- Engine-specific extension properties may use a documented namespace.
- Duplicate keys within a section are errors.

### 7.3 Defaults and overrides

`[default]` contains properties shared by targets. A target section can override any property that is valid as a target-level setting.

For example:

```ini
[default]
decimal_places = 3
string_truncate = 80

[trino]
engine = trino
decimal_places = 10
```

The resolved Trino target uses ten decimal places and an 80-character string limit.

Configuration precedence, from highest to lowest, is:

```text
CLI option
  > interactive session override
  > target section
  > [default] section
  > qcli built-in default
```

Process environment variables referenced with `${NAME}` resolve values; they do not automatically override arbitrary properties unless a property explicitly supports such an override.

This distinction keeps configuration predictable. Running qcli should not change behavior because an unrelated environment variable happens to share a name.

### 7.4 Recommended default properties

The exact names should be finalized before implementation. The first specification should cover at least:

#### Display

| Property | Purpose | Suggested built-in |
|---|---|---|
| `output_format` | Interactive result format | `table` |
| `decimal_places` | Maximum displayed fractional digits | `3` |
| `decimal_rounding` | Display rounding policy | `half_even` |
| `strip_trailing_decimal_zeros` | Remove insignificant displayed zeros | `true` |
| `string_truncate` | Maximum displayed string width | `80` |
| `binary_format` | Display for binary data | `hex` |
| `null_value` | Text used for SQL NULL | `NULL` |
| `table_style` | Table border style | `unicode` on TTY |
| `color` | Color policy | `auto` |
| `expanded` | One field per line | `auto` |
| `headers` | Show result headers | `true` |
| `row_numbers` | Show local row numbers | `false` |
| `max_column_width` | Maximum table column width | terminal-derived |
| `timestamp_format` | Timestamp rendering | `iso8601` |
| `timezone` | Display timezone | `local` or explicit |

`decimal_places` and `string_truncate` affect display only. They must never alter values exported in machine-oriented formats unless a separate explicit export transformation is requested.

#### Query execution

| Property | Purpose | Suggested built-in |
|---|---|---|
| `timing` | Show timing summary | `true` |
| `query_timeout` | Client-side query deadline | unset |
| `connect_timeout` | Connection establishment deadline | `15s` |
| `fetch_size` | Requested fetch/page size | adapter default |
| `page_size` | Rows between interactive pauses | `1000` |
| `max_display_rows` | Interactive rendering safeguard | unset, warn for large data |
| `progress` | Query progress display | `auto` |
| `retry` | Safe connection-level retry policy | conservative |

#### Interactive shell

| Property | Purpose | Suggested built-in |
|---|---|---|
| `history` | Persist query history | `true` |
| `history_limit` | Maximum history entries | `10000` |
| `syntax_highlight` | Highlight SQL | `true` |
| `completion` | Metadata completion | `true` |
| `pager` | Pager command/policy | `auto` |
| `editor` | External editor | environment-derived |
| `prompt` | Prompt template | qcli default |
| `confirm_target_switch` | Ask before selected switches | `false` |

#### Safety and privacy

| Property | Purpose | Suggested built-in |
|---|---|---|
| `redact_secrets` | Redact known secret material | always `true` |
| `history_sensitive_detection` | Omit likely secrets from history | `true` |
| `tls_verify` | Verify TLS certificates | `true` |
| `show_query_id` | Display engine query ID | `true` |
| `log_level` | Diagnostic verbosity | `warn` |

Security invariants such as secret redaction should not be disableable merely through a target override.

### 7.5 Duration, boolean, and scalar syntax

Recommended duration syntax:

```text
250ms
15s
5m
2h
```

Booleans should accept `true` and `false`. Permissive aliases such as `yes`, `no`, `1`, and `0` should either be rejected or normalized with a warning; one canonical representation improves clarity.

Quoted values are required when leading/trailing spaces or comment markers are intentional:

```ini
null_value = "<null>"
```

### 7.6 Comments

Support whole-line comments:

```ini
# Shared defaults
[default]
decimal_places = 3
```

Inline comments should only be recognized outside quoted values.

### 7.7 Environment substitution

Values may reference process environment variables:

```ini
token = ${DATABRICKS_TOKEN}
password = ${SNOWFLAKE_PASSWORD}
```

Requirements:

- Missing required substitutions are configuration errors.
- Error messages name the missing variable but never print adjacent secret text.
- Recursive expansion is not performed.
- Command substitution is not supported.
- Defaults such as `${NAME:-value}` should be omitted initially unless fully specified.
- Resolved secrets are held only as long as needed and represented internally as redacted values.

### 7.8 File permissions

On Unix, qcli should create `~/.qcli` with mode `0700` and `.env` with mode `0600`. If an existing file is accessible to group or other users, qcli should refuse to load embedded credentials by default and explain how to correct the permissions.

On Windows, qcli should use the strongest practical user-only ACL and provide a diagnostic when the file is broadly accessible.

### 7.9 Configuration diagnostics

Provide:

```text
qcli config check
qcli config show --resolved --target trino
qcli config path
```

Resolved output must redact secrets:

```text
token = <redacted>
password = <redacted>
```

Every parsing or validation error should include section, property, and line number where possible.

## 8. Engine configuration

### 8.1 Trino

Representative properties:

```ini
[trino]
engine = trino
url = https://trino.example.com
user = deepak
password = ${TRINO_PASSWORD}
catalog = hive
schema = analytics
source = qcli
client_tags = interactive,engineering
tls_verify = true
```

Future authentication properties may include basic authentication, JWT, OAuth/OIDC, Kerberos, and client certificates. Authentication capability depends on the selected Rust driver or qcli adapter.

Trino-specific session properties must be supported without polluting the portable property namespace. One possible convention is:

```ini
session.query_max_run_time = 30m
session.join_distribution_type = AUTOMATIC
```

### 8.2 Databricks SQL

Representative properties:

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

The first Databricks provider is PAT. Planned providers include OAuth
machine-to-machine, OAuth user-to-machine/browser, supplied OAuth tokens,
existing Databricks CLI/configuration profiles, and OIDC/workload identity.
Credential acquisition and renewal remain separate from Statement Execution API
request construction.

The SQL warehouse is configured by `http_path`; the adapter derives the
Statement Execution API `warehouse_id` from it. qcli should derive a friendly
compute label when the API makes one available, without requiring an additional
configuration lookup to execute a query.

### 8.3 Snowflake

Representative properties:

```ini
[snowflake-analytics]
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

The first Snowflake provider is username/password. Planned providers include
key-pair JWT, OAuth token and refresh flows, external browser/SSO, programmatic
access tokens, existing Snowflake CLI profiles, and workload identity federation.
The connection/query layer must accept credentials from any provider without
changing the common adapter contract.

Snowflake `database` maps to qcli's catalog concept. qcli commands should use the neutral term `catalog`, while connection diagnostics may show both terms:

```text
Catalog (Snowflake database): REPORTING
```

## 9. CLI surface

### 9.1 Starting qcli

With no target argument, qcli opens a target picker:

```text
$ qcli
Select target:
> trino
  databricks-dev
  snowflake-analytics
```

If `[default]` later supports a `target` property, qcli may select it automatically only when that behavior is explicit. The first release should prefer visible selection over silently choosing a potentially expensive or production target.

Direct selection:

```text
qcli trino
qcli --target trino
```

One-shot query:

```text
qcli --target trino --command "select current_catalog, current_schema"
```

Query file:

```text
qcli --target snowflake-analytics --file report.sql
```

Standard input:

```text
generate_sql | qcli --target databricks-dev --file -
```

### 9.2 Proposed top-level commands

```text
qcli [TARGET]
qcli --target TARGET [OPTIONS]
qcli target list
qcli target show TARGET
qcli target test TARGET
qcli config check
qcli config show
qcli config path
qcli completion <shell>
qcli version
```

Editing target configuration can be added later. A first release should avoid rewriting a credential-bearing file unless formatting, comments, permissions, and atomic writes are fully handled.

### 9.3 Core options

```text
-t, --target <TARGET>
-c, --command <SQL>
-f, --file <PATH|->
-o, --output <PATH>
    --format <table|csv|tsv|json|jsonl|vertical>
    --catalog <CATALOG>
    --schema <SCHEMA>
    --compute <COMPUTE>
    --role <ROLE>
    --timeout <DURATION>
    --no-color
    --quiet
    --verbose
```

CLI options override the selected target and `[default]` for that process only.

## 10. Interactive shell

### 10.1 Input behavior

- Read multiline SQL until a complete statement terminator is found.
- Understand quoted strings, quoted identifiers, comments, and engine-specific delimiters sufficiently to avoid premature execution.
- Use a distinct continuation prompt.
- Support bracketed paste.
- Keep cursor movement Unicode-safe.
- Allow Ctrl-C to clear an idle input buffer or cancel a running query.
- Allow Ctrl-D to exit when the query buffer is empty.
- Preserve an incomplete buffer when an external editor is opened.

### 10.2 Native meta-commands

```text
\help [COMMAND]              show help
\quit                        exit qcli
\targets                     list targets
\use TARGET                  connect to another target
\status                      show active context and connection details
\catalogs [PATTERN]          list catalogs/databases
\schemas [PATTERN]           list schemas
\tables [PATTERN]            list tables and views
\describe OBJECT             describe an object
\use-catalog CATALOG         change catalog/database
\use-schema SCHEMA           change schema
\use-compute COMPUTE         change compute when supported
\use-role ROLE               change active role when supported
\properties                  show session properties
\set-property NAME VALUE     set an engine session property
\unset-property NAME         unset an engine session property
\format FORMAT               change result format
\timing [on|off]             change timing display
\set NAME VALUE              set a qcli session display/runtime option
\reset NAME                  remove a session override
\history                     display query history
\edit                        edit current query buffer
\print                       print current query buffer
\clear                       clear current query buffer
\write PATH                  write query buffer to a file
\include PATH                execute a query file
\cancel                      cancel active query
```

Short compatibility aliases may be added later, but documentation should lead with readable qcli-native commands.

### 10.3 Session option override

Example:

```text
trino[hive.analytics]> \set decimal_places 8
trino[hive.analytics]> \set string_truncate 120
```

These changes last only for the current process and do not edit `~/.qcli/.env`.

`\reset decimal_places` restores the resolved target/default value.

### 10.4 Target switching

Target switching must be atomic from the user's perspective:

1. Validate that the named target exists.
2. Resolve and validate its configuration.
3. Attempt the new connection.
4. Keep the current connection alive until the new connection succeeds.
5. Replace active connection and context.
6. Clear target-specific metadata and statement state.
7. Preserve session overrides such as output format and display precision.
8. Update the prompt.

If the new target fails, qcli remains connected to the original target.

Because transaction awareness is out of scope, qcli does not inspect transaction state before switching in the first release. This limitation should be documented.

## 11. Query lifecycle

Analytical engines frequently expose asynchronous execution. qcli must model query progress independently of any single driver API:

```text
Submitted -> Queued -> Running -> ProducingRows -> Completed
                |          |             |
                +----------+-------------+-> Failed
                +----------+-------------+-> Cancelling -> Cancelled
```

The common query record contains:

- qcli-local execution ID.
- Engine query ID, once available.
- Target name.
- Catalog, schema, compute, and role.
- Submission time.
- Queue duration, when available.
- Execution duration.
- Time to first row.
- Total elapsed time.
- Rows and bytes received by qcli.
- Engine-reported rows and bytes scanned, when available.
- Final status and structured error.

Adapters may attach engine-specific metrics without forcing them into the common schema.

## 12. Query cancellation

Ctrl-C during execution requests cancellation from the active adapter.

Required behavior:

- Show that cancellation was requested.
- Use the engine query ID when cancellation requires it.
- Distinguish “cancelled,” “cancellation requested but unconfirmed,” and “cancellation failed.”
- Stop rendering additional rows promptly.
- Recover or replace the underlying connection if cancellation invalidates it.
- Exit with a distinct status in non-interactive mode.
- Never report success merely because the local result stream was dropped.

## 13. Results and values

### 13.1 Internal value model

The driver boundary must preserve values rather than converting every cell to text. The common representation should cover:

- SQL NULL.
- Boolean.
- Signed and unsigned integers where supported.
- Arbitrary-precision decimal.
- Floating-point values.
- String.
- Binary.
- Date.
- Time.
- Timestamp with and without timezone.
- Interval/duration.
- JSON/variant.
- Array.
- Map/object.
- Row/struct.
- Engine-specific value fallback with type metadata.

Preserving decimal precision is mandatory for analytical and financial data.

### 13.2 Display transformations

Display settings such as decimal shortening and string truncation apply only to human-oriented rendering.

Example:

```text
Source decimal: 123.456789
decimal_places = 3
Table display:  123.457
CSV/JSON value: 123.456789
```

Truncation must be visible:

```text
This string was truncated becaus…
```

qcli should never silently change an exported value because the interactive table is narrow.

### 13.3 Output formats

First release:

- `table`: aligned human-readable table.
- `vertical`: one column per line for each row.
- `csv`: RFC-compatible CSV with explicit encoding policy.
- `tsv`: tab-delimited records.
- `json`: JSON array or structured result object, finalized by specification.
- `jsonl`: one JSON object per line.

Potential later formats:

- Markdown.
- Apache Arrow IPC.
- Parquet.
- SQL insert statements.

### 13.4 Paging and streaming

- Rows are streamed where the engine API permits.
- Memory usage must not grow linearly with result size.
- Interactive table rendering may buffer a bounded sample to calculate column widths.
- Non-interactive CSV/TSV/JSONL should stream immediately.
- JSON array output may require explicit framing but must remain bounded.
- Broken pipe is handled without a stack trace.
- Pager invocation is limited to interactive TTY output.

## 14. Query metrics

Common metrics should include, when available:

- Query ID.
- Status.
- Queue time.
- Planning time.
- Execution time.
- Time to first row.
- Total elapsed time.
- Rows returned.
- Bytes returned.
- Rows scanned.
- Bytes scanned.
- Peak memory.
- Compute/warehouse.
- Cache usage.
- Engine-provided query/profile URL.

Missing metrics are omitted or shown as unavailable. qcli must not derive misleading estimates merely to fill every field.

An optional verbose completion summary could be:

```text
42 rows in 1.82s (queued 120ms, first row 640ms)
Query ID: 20260720_142201_00042_abcd
Scanned: 18.4M rows / 2.7 GiB
```

## 15. Metadata discovery and completion

### 15.1 Metadata operations

Each adapter should provide:

- List catalogs/databases.
- List schemas.
- List tables and views.
- Describe columns and types.
- Identify current catalog and schema.
- List compute resources if supported and authorized.
- List roles if supported and authorized.

### 15.2 Completion

Completion should cover:

- qcli meta-commands.
- Target names after `\use`.
- Catalogs, schemas, tables, views, and columns.
- Qualified object paths.
- Common SQL keywords and functions for the active engine.
- Values for known configuration properties.

Metadata should load asynchronously and use a bounded cache. Cache entries are scoped to target, catalog, schema, identity, and role. Switching target or role invalidates the relevant view.

Completion failures must not prevent query execution.

## 16. Authentication and secrets

### 16.1 First-release baseline

The initial mechanisms are fixed and intentionally narrow:

- Trino: basic credentials and/or token authentication.
- Databricks: personal access token.
- Snowflake: username and password.

Authentication breadth remains a release-blocking architecture requirement. See
[ADR-003](adr-003-extensible-authentication.md).

### 16.2 Authentication provider model

Authentication has separate configuration, acquisition, application, renewal,
and invalidation stages. An engine adapter receives usable credentials or an
authenticated connection; core sessions do not know whether those credentials
came from a password, PAT, browser, profile, private key, or workload identity.

Every provider declares:

- engine and authentication method;
- required non-secret and secret properties;
- whether user interaction is required;
- whether credentials expire and can be renewed;
- whether batch, interactive terminal, and HTTP-server modes are supported.

Interactive authentication must be explicitly enabled by an interactive command.
Batch and server operation must never unexpectedly open a browser. Refresh is
synchronized per provider to avoid concurrent refresh storms.

If `auth_type` is absent, qcli may infer a method only from one unambiguous set of
properties. Mixed methods, such as a password and token together, fail target
validation before network activity.

### 16.3 Authentication roadmap

| Engine | Initial | Planned next methods |
|---|---|---|
| Databricks | PAT | OAuth M2M, OAuth U2M/browser, existing profile/CLI credentials, supplied OAuth token, OIDC/workload identity |
| Snowflake | Username/password | Key-pair JWT, OAuth, browser/SSO, programmatic access token, existing profile, workload identity federation |
| Trino | Basic, bearer | OAuth/OIDC, Kerberos, client certificates |

### 16.4 Secret handling invariants

- Secret values are redacted by type, not by best-effort string replacement alone.
- Connection URLs are parsed before logging and credentials removed.
- SQL history excludes recognized credential-management statements.
- Debug logs do not contain authorization headers.
- Panic reports do not dump configuration objects containing secrets.
- `config show` always redacts.
- Authentication failures never echo the submitted credential.
- Environment variable names may be shown; resolved values may not.
- Refresh tokens and browser-login caches use OS-native secure storage where available.
- Private keys are referenced by path by default and private-key material is never serialized in target diagnostics.
- HTTP session state stores a provider reference and effective identity, never a copy of credentials.

### 16.5 Future secret providers

- macOS Keychain.
- Windows Credential Manager.
- Linux Secret Service.
- `pass` or another external command with an explicit opt-in.
- Cloud secret managers.
- Short-lived OAuth login and token refresh.

## 17. Error model

Errors should be structured by phase:

- Configuration.
- Name resolution/network.
- TLS.
- Authentication.
- Connection/session setup.
- Query submission.
- Query execution.
- Result decoding.
- Output serialization.
- Cancellation.

Interactive presentation should include:

```text
Query failed on target snowflake-analytics
Engine code: 002003
Query ID: 01b...
Object REPORTING.PUBLIC.MISSING_TABLE does not exist or is not authorized.
```

Native messages should be preserved, but passwords, tokens, and authorization headers must be removed.

## 18. Automation contract

### 18.1 Streams

- stdout contains query result data.
- stderr contains progress, diagnostics, metrics, and errors.
- `--quiet` suppresses nonessential status output, not errors.
- Color is disabled when the destination is not a TTY unless forced.

### 18.2 Proposed exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | General qcli error |
| `2` | CLI usage error |
| `3` | Configuration error |
| `4` | Connection or authentication error |
| `5` | Query failed |
| `6` | Query cancelled or timed out |
| `7` | Output/write error |

The exact codes must be frozen before a stable release.

### 18.3 Determinism

- Machine formats use stable escaping and documented NULL behavior.
- Locale does not change numeric serialization.
- Display timezone never silently changes machine-oriented timestamps.
- Progress indicators are never written to stdout.
- A query failure never emits a syntactically successful-looking partial JSON document without a failing exit code.

### 18.4 Shared session model

Terminal, HTTP, and Flight SQL frontends use the same logical session
abstraction. A session is not necessarily one permanent physical database
connection.

```text
Session
├── owner or caller identity
├── active target
├── catalog/database
├── schema
├── compute/warehouse
├── role
├── engine session properties
├── qcli display and execution overrides
├── version
├── created and last-used timestamps
└── active query references
```

Configuration resolves in the following order:

```text
qcli built-in default
  -> [default]
    -> target section
      -> session override
        -> individual query override
```

An individual query override applies only to that query and never mutates the session.

The terminal creates one process-local session. Commands such as `\use`,
`\use-schema`, `\set-property`, and `\set` mutate that session. In serve mode,
HTTP and Flight SQL share multiple owned, versioned, expiring sessions through
the protocol-neutral service layer.

### 18.5 Immutable query snapshots

Every submitted query captures an immutable snapshot of its session state, including:

- Session ID and version.
- Target.
- Catalog and schema.
- Compute and role.
- Engine session properties.
- Relevant qcli execution and output options.

A later session mutation must not affect a running or completed query. Query status and diagnostics record the session version used for execution.

For example, a query submitted from session version 4 against `hive.analytics` continues with that context even if the caller changes the session to `hive.reporting` in version 5.

### 18.6 Session concurrency

Initial concurrency rules:

- Session mutations are serialized.
- Mutations use optimistic version checks.
- Multiple queries may execute concurrently from one session.
- Each query receives an immutable snapshot.
- Target or context changes affect only later submissions.
- Engine connections are leased from a pool or established according to adapter requirements.
- Metadata is cached by target, identity, role, catalog, schema, and applicable session version.

HTTP mutations should accept an `If-Match` version or equivalent expected-version field. A stale mutation returns a conflict instead of silently overwriting newer state.

### 18.7 HTTP service

The HTTP interface is provided through:

```text
qcli serve
```

It calls qcli core services directly and never launches a qcli subprocess. The canonical query API is asynchronous because warehouse queries can queue or run for long periods.

Minimum session endpoints:

```text
POST   /v1/sessions
GET    /v1/sessions/{session_id}
PATCH  /v1/sessions/{session_id}
POST   /v1/sessions/{session_id}/target
PATCH  /v1/sessions/{session_id}/properties
PATCH  /v1/sessions/{session_id}/options
DELETE /v1/sessions/{session_id}
```

Minimum query endpoints:

```text
POST   /v1/sessions/{session_id}/queries
POST   /v1/queries
GET    /v1/queries/{query_id}
GET    /v1/queries/{query_id}/results
GET    /v1/queries/{query_id}/events
POST   /v1/queries/{query_id}/cancel
```

`POST /v1/queries` is the stateless alternative. The request supplies a target and optional context, properties, and query overrides. qcli creates an ephemeral execution context without requiring explicit session cleanup.

Use persistent sessions for interactive applications and repeated contextual queries. Use stateless queries for automation and independent requests.

### 18.8 HTTP session operations

Creating a session:

```http
POST /v1/sessions
Content-Type: application/json
```

```json
{
  "target": "trino",
  "context": {
    "catalog": "hive",
    "schema": "analytics"
  },
  "properties": {
    "query_max_run_time": "10m"
  },
  "options": {
    "decimal_places": 8,
    "string_truncate": 120
  }
}
```

Submitting through the session:

```http
POST /v1/sessions/deepak_20260721_1605_01/queries
Content-Type: application/json
```

```json
{
  "sql": "select * from events limit 100"
}
```

The response includes both identifiers:

```json
{
  "id": "qcli_01J...",
  "session_id": "deepak_20260721_1605_01",
  "session_version": 3,
  "engine_query_id": null,
  "state": "submitted"
}
```

The engine query ID is added when it becomes available.

### 18.9 HTTP results and events

Results support pagination and streaming. At minimum:

```text
application/json
application/x-ndjson
text/csv
application/vnd.apache.arrow.stream
```

JSON decimals should be encoded without precision loss, normally as strings with type metadata. Display-only settings such as `decimal_places` and `string_truncate` must not alter machine-oriented HTTP results unless an explicit transformation is requested.

Server-Sent Events are recommended for one-way progress updates:

```http
GET /v1/queries/{query_id}/events
Accept: text/event-stream
```

Events may include state changes, metrics, engine query ID availability, cancellation state, result availability, and expiration. WebSockets are unnecessary for the initial one-way progress model.

### 18.10 HTTP target switching

Switching a session target is atomic:

1. Resolve and validate the new target.
2. Establish or verify the new engine session.
3. Preserve the old target until the new target succeeds.
4. Apply the new target defaults.
5. Preserve portable qcli options.
6. Clear old engine-specific session properties.
7. Reset or validate catalog, schema, compute, and role.
8. Invalidate affected metadata.
9. Increment the session version.

A failed switch leaves the HTTP session unchanged, matching terminal behavior.

### 18.11 Engine-specific session realization

Adapters decide how logical state maps to an engine:

- Trino can carry much session context in protocol headers and returned session updates.
- Databricks SQL can apply supported context and statement options during submission.
- Snowflake may require connection-specific context initialization for warehouse, database, schema, role, and session parameters.

An adapter may use a sticky physical connection, a pool keyed by resolved state, or explicit initialization whenever a connection is leased. This is an implementation detail behind the logical session contract.

The first implementation should prefer correctness and isolation over minimizing context initialization calls.

### 18.12 Session lifetime and cleanup

The initial single-node service may keep sessions in process memory with:

- Readable session IDs in `username_YYYYMMDD_HHMM_XX` form. Session IDs are
  identifiers, not authentication credentials; caller ownership and
  authorization must be enforced independently on every operation.
- Caller ownership on every operation.
- Configurable idle TTL.
- Configurable absolute lifetime.
- Per-caller session limits.
- Automatic release of connections and metadata.
- Explicit policy for active queries when a session closes.

Closing or expiring a session rejects new submissions. It does not automatically cancel running queries unless configured or explicitly requested. Query result retention is governed independently.

A distributed deployment requires an external logical session store plus session affinity or distributed ownership for physical engine connections. Physical connections themselves are not serializable session state.

### 18.13 HTTP result retention

The service must bound completed result storage:

- Keep small results in bounded memory.
- Spill larger results to temporary Arrow IPC or Parquet files.
- Apply result TTLs.
- Enforce per-query, per-caller, and total storage limits.
- Use opaque pagination tokens.
- Remove expired results automatically.

Distributed result storage may be added later using an object store. Unlimited in-memory result retention is never permitted.

### 18.14 HTTP security

HTTP exposure materially increases risk because the service can access every configured target.

Required controls:

- Bind to loopback by default.
- Refuse non-loopback binding without authentication.
- Require TLS directly or through a trusted proxy for network deployment.
- Authenticate callers and authorize them per target.
- Enforce session and query ownership.
- Use unguessable session and query IDs.
- Limit request, SQL, result, concurrency, and retention sizes.
- Disable shell execution and local file inclusion through HTTP.
- Restrict CORS by default.
- Never return target credentials or complete connection properties.
- Do not log SQL by default.
- Record caller, target, query ID, status, and timing for audit purposes.

A shared bearer token is acceptable only for local development. Multi-user deployment should use OIDC/JWT or an authenticated gateway.

The product must explicitly choose whether a query runs as a shared service identity, propagated caller identity, or an authorized named credential profile. Shared service identities are simplest but carry the greatest privilege-sharing risk.

### 18.15 Global `qcli serve` mode

`qcli serve` is one service runtime with two protocol frontends:

```text
qcli serve
├── HTTP control plane
│   ├── OpenAPI and Swagger
│   ├── operational session/query APIs
│   ├── health, readiness, metrics, and administration
│   └── browser-oriented integration
└── Flight SQL data plane
    ├── SQL execution
    ├── Arrow result streaming
    ├── SQL metadata
    ├── session options
    ├── prepared statements and parameter batches
    └── ADBC/JDBC/approved ODBC connectivity
```

The listeners normally use different ports because Flight SQL requires gRPC
over HTTP/2. A reverse proxy may present one public hostname, but qcli does not
depend on protocol multiplexing at one socket.

Representative production startup:

```text
qcli serve \
  --http-bind 127.0.0.1:8088 \
  --flight-bind 0.0.0.0:32010 \
  --auth-file ~/.qcli/http-auth.toml \
  --tls-cert /etc/qcli/tls.crt \
  --tls-key /etc/qcli/tls.key
```

HTTP and Flight SQL must share one protocol-neutral `qcli-service` layer.
Neither frontend owns canonical session, query, result, quota, expiry, audit,
or shutdown state.

### 18.16 Flight SQL connectivity contract

Flight SQL is qcli's standard remote SQL protocol. Standard clients connect as:

```text
ADBC Flight SQL ──┐
Arrow JDBC ───────┼── Flight SQL/gRPC ── qcli-service ── engine adapters
approved ODBC ────┤
native clients ───┘
```

qcli will not initially publish a custom ADBC driver. ADBC is the client API;
the existing ADBC Flight SQL driver supplies the protocol adapter. An arbitrary
backend-specific ADBC driver does not connect to qcli.

Compatibility levels are explicit:

- Native Flight SQL and selected ADBC Flight SQL clients are primary.
- Apache Arrow Flight SQL JDBC becomes supported after conformance testing.
- ODBC remains experimental until a selected third-party Flight SQL ODBC driver
  passes the qcli compatibility matrix on supported platforms.

### 18.17 Flight sessions and target selection

Flight SQL `SetSessionOptions`, `GetSessionOptions`, and `CloseSession` map to
the shared qcli session model. Required portable options include:

```text
qcli.target
catalog
schema
qcli.query_timeout
qcli.session.<engine-property>
```

Authentication identifies the principal. An opaque signed session token or
cookie identifies the principal-owned qcli session. The token never contains
credentials, SQL, connection properties, or physical engine connection state.

`qcli.target` must be authorized and selected before query execution. Target
switching remains atomic and versioned. Flight and HTTP operations against the
same session observe the same state and ownership rules.

### 18.18 Flight query and result lifecycle

Statement execution follows the standard Flight pattern:

```text
GetFlightInfo(CommandStatementQuery)
  -> authenticate and authorize
  -> submit shared qcli query
  -> return schema and signed endpoint ticket

DoGet(ticket)
  -> validate version, owner, query, partition, and expiry
  -> stream Arrow record batches with backpressure
```

Tickets are opaque, signed, expiring, replay-policy aware, and contain no SQL or
credentials. HTTP cancellation and Flight cancellation act on the same qcli
query. Disconnect behavior, result replay, ticket expiry, and partial-stream
failure are documented protocol contracts rather than incidental behavior.

The data path preserves Arrow types and metadata without passing through JSON
or human rendering. Memory, buffered batches, concurrent streams, result spill,
and replay reads remain bounded.

### 18.19 Flight SQL metadata and capabilities

The complete relevant metadata surface includes:

- SQL information and supported features.
- Catalogs, schemas, tables, table types, and XDBC type information.
- Primary, imported, exported, and cross-reference keys where supported.
- Exact Flight SQL-defined Arrow metadata schemas.

`GetSqlInfo` is generated from the active adapter capability profile. qcli must
not claim the union of all engines or silently emulate unsupported behavior.
Transactions, updates, prepared statements, ingestion, and Substrait are each
advertised per target.

JDBC and ODBC compatibility depends on metadata correctness as much as query
execution, so metadata conformance is a release gate.

### 18.20 Prepared statements, transactions, and ingestion

Full connectivity requires a protocol-neutral prepared-statement service with
owner/session binding, opaque handles, parameter and result schemas, expiry,
and explicit closure. Engine-native parameter binding is preferred. qcli must
never implement parameters through unsafe SQL string interpolation.

The shared service now owns that registry. Flight SQL maps standard create,
bind, query/update execution, and close operations onto it. Arrow parameter
batches remain typed end to end, including nulls and nested values. Adapters
advertise three independent capabilities: prepared statement lifecycle, native
typed parameters, and statement update counts. A request is rejected when its
selected adapter lacks the required native capability; qcli does not substitute
SQL text or invent an update count. The deterministic demo adapter implements
all three for conformance. Trino, Databricks SQL, and Snowflake currently expose
prepared lifecycle and zero-parameter execution, but not typed binding or
updates until their client libraries provide correct native contracts.

Transactions remain unsupported until the shared session and adapter contracts
can implement target-native begin, commit, rollback, failure, expiry, and
shutdown behavior correctly. qcli never emulates a cross-engine transaction or
silently ignores transaction commands.

Arrow `DoPut` ingestion is capability-driven. Create, append, replace, batch
parameter, partial-failure, retry, and update-count semantics must be explicit
for each adapter.

### 18.21 Flight SQL security and operations

Flight SQL uses the same `Authenticator`, `AuthenticatedPrincipal`, target ACLs,
ownership, quotas, and audit policy as HTTP. Bearer credentials travel in gRPC
metadata and are validated on every RPC unless exchanged for a short-lived,
principal-bound session credential.

Production requirements include:

- Direct TLS with ALPN `h2` or an explicitly trusted gRPC-aware proxy.
- Optional mTLS and future JWT/OIDC identity.
- Connection, stream, request, query, memory, and result quotas.
- Maximum message sizes, deadlines, keepalive, and connection age.
- Certificate rotation and graceful listener shutdown.
- Stable gRPC status and structured SQL/vendor error mapping.
- Metrics and traces linking Flight request ID, qcli query ID, and engine query
  ID.

Audit records exclude credentials, authorization metadata, SQL, and result
values by default.

### 18.22 Multi-node service

Single-node Flight SQL uses the existing process-local session/query service.
Multi-node operation requires shared logical sessions, query ownership leases,
distributed quotas, object-backed retained results, and node-independent or
routable tickets.

Submitting through node A and consuming through node B must either work through
shared state or return a Flight endpoint location for the owning node. Sticky
routing alone is an initial deployment constraint, not the final consistency
model.

## 19. Rust architecture

Proposed workspace boundaries:

```text
qcli-cli              argument parsing and process contract
qcli-repl             line editing, prompt, history, meta-commands
qcli-core             sessions, query lifecycle, values, errors
qcli-config           sectioned .env parsing and resolution
qcli-driver-api       internal adapter traits and capabilities
qcli-auth             authentication providers, credential lifecycle, secret resolution
qcli-driver-trino     Trino adapter
qcli-driver-databricks Databricks SQL adapter
qcli-driver-snowflake Snowflake adapter
qcli-metadata         normalized metadata and caching
qcli-output           table and machine serializers
qcli-http             HTTP transport, resources, events, and result representations
qcli-service          protocol-neutral ownership, quotas, retention, and lifecycle
qcli-flight-sql       Flight and Flight SQL gRPC frontend
```

This is a conceptual decomposition, not a requirement to create many crates immediately. Early development may keep components in modules until interfaces stabilize.

### 19.1 Driver adapter responsibilities

Each adapter owns:

- Applying acquired credentials to engine requests or connections.
- Session initialization.
- Query submission.
- Status polling or streaming.
- Cancellation.
- Row and type conversion.
- Catalog/schema context changes.
- Engine session properties.
- Metadata queries.
- Query metrics.
- Native error conversion.

The authentication layer owns method selection, secret resolution, interactive
login, credential renewal, and invalidation. It does not submit SQL or interpret
query results. An adapter may expose engine-specific hooks required to apply a
credential, but those hooks do not leak into core or frontend APIs.

### 19.2 Capability reporting

Capabilities should be explicit, for example:

```text
list_catalogs
list_schemas
list_computes
change_compute
change_role
cancel_query
stream_results
query_metrics
query_profile_url
session_properties
```

The CLI uses capability information to render help and reject unsupported operations cleanly.

### 19.3 Async boundary

Network drivers and asynchronous warehouse APIs favor async I/O. The internal query execution interface should be asynchronous and cancellation-aware. Output encoding can remain synchronous where appropriate but must not block query progress indefinitely; bounded channels can provide backpressure.

### 19.4 Core services

Terminal, HTTP, and Flight SQL frontends depend on the same core services:

```text
SessionManager
├── create
├── get
├── mutate
├── switch_target
├── snapshot
├── close
└── expire

QueryService
├── submit(session_snapshot, sql, query_options)
├── status
├── results
├── events
└── cancel
```

The terminal owns one process-local session. In serve mode, `qcli-service`
manages many sessions shared by the HTTP and Flight SQL frontends. No frontend
contains engine-specific execution logic.

## 20. Performance requirements

Initial targets:

- Startup without connection should feel immediate.
- Target picker should not contact every configured target.
- Connecting only contacts the selected target.
- Result memory usage remains bounded for CSV, TSV, and JSONL.
- Ctrl-C is observed promptly during network waits and result rendering.
- Metadata loading does not block initial query entry.
- Slow metadata endpoints use timeouts and partial completion results.
- Rendering should handle wide and nested analytical values without quadratic behavior.

Specific latency and memory budgets should be established through benchmarks after driver prototypes exist.

## 21. Observability

qcli should provide opt-in diagnostic logging with levels such as error, warn, info, debug, and trace.

Logs may include:

- Target name.
- Adapter and qcli versions.
- Lifecycle state transitions.
- Query ID.
- Durations.
- Retry decisions.
- Metadata cache events.

Logs must not include:

- Passwords or tokens.
- Authorization headers.
- Private keys.
- Full credential-bearing URLs.
- Query text by default.

Query logging may be enabled explicitly, with a warning that SQL can contain sensitive data.

## 22. Testing strategy

### 22.1 Configuration

- Section discovery and `[default]` exclusion.
- Default and target override precedence.
- Duplicate section and key errors.
- Unknown property suggestions.
- Quoting, comments, Unicode, and line endings.
- Environment substitution and missing variables.
- File permission checks.
- Secret redaction in every error path.
- Property-based and fuzz testing of the parser.

### 22.2 Query shell

- Multiline SQL and statement termination.
- Quotes, comments, and engine-specific syntax.
- Unicode cursor behavior.
- Bracketed paste.
- Ctrl-C at idle, submission, polling, and rendering stages.
- Target switch success and failure.
- Query buffer preservation.
- History filtering.

### 22.3 Adapters

Every adapter should pass a conformance suite covering:

- Connect and authenticate.
- Obtain query ID.
- Execute scalar and tabular results.
- Preserve NULL and supported value types.
- Large streaming result.
- Server error mapping.
- Cancellation.
- Metadata discovery.
- Context switching.
- Metrics reporting.
- TLS validation.
- Redaction.

### 22.4 Output

- Golden tests for each format.
- Decimal precision and display rounding.
- String truncation only in human output.
- NULL, binary, temporal, nested, and non-ASCII values.
- Broken pipes.
- Partial write failures.
- Bounded-memory tests.

### 22.5 Integration environments

- Containerized Trino where practical.
- Dedicated test Databricks SQL and Snowflake accounts managed through CI secrets.
- Feature-gated cloud integration tests to control cost.
- Recorded protocol fixtures only for cases where terms and credential safety permit them.

### 22.6 Sessions and HTTP

- Default, target, session, and query precedence.
- Immutable query snapshots across later session mutations.
- Optimistic version conflicts.
- Concurrent queries from one session.
- Atomic target switching and failed-switch preservation.
- Idle expiry, absolute expiry, explicit close, and cleanup.
- Session and query ownership enforcement.
- Stateless and session-based submission parity.
- Status, result pagination, SSE events, and cancellation.
- Content negotiation for JSON, JSONL, CSV, and Arrow streams.
- Result TTL, spill, quota, and cleanup behavior.
- Loopback defaults, authentication requirements, CORS, and request limits.
- Secret and SQL redaction across HTTP errors and logs.

## 23. Release plan

### Phase 0: specification and driver feasibility

- Finalize `.env` grammar and property names.
- Prototype initial authentication and cancellation for all three engines.
- Verify that Databricks PAT and Snowflake password implementations use the same
  extensible provider lifecycle.
- Inventory and design-test the planned Databricks and Snowflake authentication
  methods even when their providers are scheduled later.
- Validate type coverage and result streaming in candidate Rust libraries.
- Define the adapter capability contract.
- Freeze stdout/stderr and exit-code behavior.
- Confirm the post-baseline authentication priority and compatibility-test matrix.

Exit criterion: no critical driver capability remains an assumption.

### Phase 1: executable query MVP

- Parse `[default]` and target sections.
- Interactive target picker.
- Direct target selection.
- Connect to all three initial engines.
- Execute one-shot SQL and query files.
- Interactive multiline shell.
- Table, CSV, JSON, and JSONL output.
- Query IDs, timing, errors, and cancellation.
- Basic target switching.
- Secret redaction.

Exit criterion: users can reliably select any configured target and execute queries interactively or in automation.

### Phase 2: warehouse-native daily driver

- Catalog/schema/table discovery.
- Metadata-aware completion.
- Compute and role switching where supported.
- Session properties.
- Query progress and normalized metrics.
- Pager, history, external editor, and display overrides.
- Target test and configuration diagnostics.
- Shared session manager with immutable query snapshots.

### Phase 3: robustness and distribution

- Authentication expansion.
- Keychain integrations.
- Performance and memory hardening.
- Cross-platform release binaries.
- Shell completions and package-manager distribution.
- Compatibility and upgrade guarantees for configuration.
- `qcli serve` with versioned sessions, stateless queries, asynchronous query endpoints, SSE progress, cancellation, and bounded result retention.
- HTTP authentication, target authorization, quotas, TTLs, and audit events.

### Later phases

- Additional analytical engines.
- Arrow and Parquet export.
- Saved queries and reusable parameters.
- Cross-target comparison or data movement, if validated by users.
- A plugin strategy only after the internal adapter API stabilizes.

## 24. Key risks

### 24.1 Rust driver maturity

The three target platforms may not have equally mature native Rust drivers, especially across authentication, cancellation, Arrow results, proxies, and TLS. Phase 0 must validate real production requirements before the architecture commits to one library per engine.

### 24.2 False uniformity

Trino, Databricks SQL, and Snowflake differ in catalog terminology, compute management, authentication, query lifecycle, metrics, and complex types. qcli should normalize workflows but preserve engine-specific details.

### 24.3 Configuration scalability

The sectioned `.env` design is approachable, but authentication configuration can grow complex. The grammar must allow namespaced engine properties without becoming ambiguous. A future migration path to an additional structured format should remain possible, while `~/.qcli/.env` continues to work.

### 24.4 Secret exposure

A single config file may hold credentials for several valuable systems. Permission checks, redacted types, safe diagnostics, and careful history behavior are release-blocking requirements.

### 24.5 Large results

Analytical users can accidentally request enormous datasets. Streaming, paging, backpressure, clear limits, and machine-format behavior must be designed before output implementation.

## 25. Product decisions already made

- The name is `qcli`.
- qcli is warehouse-oriented, not PostgreSQL-oriented.
- Initial engines are Trino, Databricks SQL, and Snowflake.
- Configuration lives at `~/.qcli/.env`.
- `[default]` contains shared properties.
- Every other section header is a target.
- No `QCLI_TARGETS` property is needed.
- Target properties override defaults.
- A target can override display settings such as decimal places and string truncation.
- Users can select a target at startup and switch targets during an interactive session.
- Transaction awareness is not required in the first release.
- Terminal, HTTP, and Flight SQL frontends share one logical session and query
  execution model.
- Every query executes from an immutable session snapshot.
- HTTP and Flight SQL support are delivered through one global `qcli serve`
  runtime, not by spawning CLI subprocesses.
- HTTP is the control and operations plane; Flight SQL is the standard remote
  SQL and Arrow data plane.
- Standard ADBC Flight SQL drivers are the first client path; JDBC and ODBC are
  enabled through independently tested Flight SQL-compatible drivers.
- Native SQL pass-through remains the default. Future transpilation is opt-in,
  observable, versioned, and fail-closed as defined by the feature roadmap.

## 26. Open decisions

The following should be resolved before implementation:

1. Whether the property is named `decimal_places`, `decimal_scale`, or `decimal_shorten`; `decimal_places` is recommended because it describes observable behavior.
2. Whether `[default]` may specify a default target, and whether that bypasses the picker.
3. Whether section and property names are case-sensitive.
4. Exact JSON output envelope and schema.
5. Minimum authentication modes for each engine.
6. Whether direct, unconfigured connection URLs are supported.
7. Whether query text is stored in history for production targets by default.
8. Whether an interactive row limit is a warning, a pause, or a hard limit.
9. How nested types are rendered in tables and serialized in CSV.
10. Whether `\set` can alter only qcli options or also engine properties; separate commands are recommended.
11. Whether target-specific visual labels such as environment and prompt color are included in the first config schema.
12. Which settings are safe to override per target and which are global invariants.
13. Whether HTTP is required for the first stable release or follows the stable CLI release.
14. Which HTTP authentication and target-authorization model is required initially.
15. Whether HTTP queries use service identities, propagated caller identities, or authorized credential profiles.
16. Default session idle TTL, absolute lifetime, result TTL, and storage quotas.
17. Whether closing a session leaves active queries running or cancels them by default.
18. Whether Snowflake sessions use sticky connections or explicit context initialization per lease.

## 27. Acceptance criteria for the first release

The first release is complete when:

- qcli discovers targets solely from section headers in `~/.qcli/.env`.
- `[default]` settings apply to every target unless overridden.
- Target-specific decimal and string display overrides work without modifying exported values.
- Users can select Trino, Databricks SQL, or Snowflake at startup.
- Users can switch to another configured target without restarting.
- A failed target switch preserves the existing working connection.
- Interactive and one-shot queries work for all three engines.
- Queries can be cancelled with a clear confirmed or unconfirmed outcome.
- Large CSV and JSONL results stream with bounded memory.
- Table, CSV, JSON, JSONL, TSV, and vertical output behavior is documented and tested.
- Query IDs and available timing metrics are displayed consistently.
- Catalog, schema, and table discovery works for all three engines.
- stdout, stderr, and exit codes are stable enough for automation.
- Credentials never appear in normal output, debug logs, configuration diagnostics, or history.
- Linux, macOS, and Windows release artifacts pass smoke tests.

HTTP acceptance criteria, when `qcli serve` is released:

- Persistent and stateless query APIs use the same adapter path as terminal execution.
- Session mutations are versioned and stale writes are rejected.
- Submitted queries retain immutable session context.
- Concurrent queries cannot corrupt shared session state.
- Query status, events, results, and cancellation are independently accessible.
- Result retention is bounded by memory, disk, TTL, and caller quotas.
- Sessions expire and release resources predictably.
- Network binding requires appropriate authentication and TLS policy.
- Every session, query, and result operation enforces caller ownership and target authorization.
