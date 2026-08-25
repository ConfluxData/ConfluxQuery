# Interactive shell reference

Start with `qcli --target NAME`; omit the target to select interactively.
Statements execute when a semicolon appears outside quotes. Multiline input,
history, highlighting, metadata completion, Ctrl-C cancellation, and EOF are
handled by the terminal layer.

| Command | Purpose | Example |
|---|---|---|
| `\help` | Print interactive command summary. | `\help` |
| `\targets` | List targets; mark the active one with `*`. | `\targets` |
| `\use TARGET` | Validate and atomically switch target. | `\use snowflake-dev` |
| `\catalogs [PATTERN]` | List catalogs using `*`/`?` glob filtering. | `\catalogs hive*` |
| `\schemas [PATTERN]` | List schemas in current context. | `\schemas sales*` |
| `\tables [PATTERN]` | List tables/views and their kind. | `\tables fact_*` |
| `\describe OBJECT` | List column name, native type, and comment. | `\describe orders` |
| `\use-catalog CATALOG` | Validate and change current catalog. | `\use-catalog hive` |
| `\use-schema SCHEMA` | Validate and change current schema. | `\use-schema analytics` |
| `\status` | Show target, engine, context, session, version, and last status. | `\status` |
| `\set NAME VALUE` | Set a versioned session option. | `\set query_timeout 30s` |
| `\format FORMAT` | Change output renderer. | `\format vertical` |
| `\timing [on\|off]` | Toggle or explicitly set timing. | `\timing on` |
| `\properties` | Show target properties and overrides with redaction. | `\properties` |
| `\p` | Print the current query buffer. | `\p` |
| `\r` | Clear the current query buffer. | `\r` |
| `\q`, `\quit` | Exit. | `\q` |

## Target and context safety

`\use` probes metadata before committing a target switch. A failed probe leaves
the old target active. Catalog/schema commands validate the requested value and
only then increment the session version. Engine-issued `USE` commands can also
update tracked context when the adapter reports successful session changes.

Backend namespace rules still apply. For example, Databricks distinguishes a
catalog from a schema; `USE SCHEMA catalog.schema` may be rejected as a nested
Unity Catalog namespace even when separate `USE CATALOG` and `USE SCHEMA`
commands work.

## Completion and history

Completion begins with commands and SQL keywords, then learns target names,
catalogs, schemas, tables, and columns from successful metadata calls. Metadata
cache entries are target-scoped and invalidated on context change.

History defaults to a file beside the qcli configuration. Sensitive-looking
statements and interactive properties are not persisted. Use `history=false`
for environments where no local query history is permitted.

## Cancellation

Ctrl-C while a query runs requests cancellation and keeps the shell alive. A
second command can be issued after terminal state. Cancellation strength
depends on the active adapter's advertised capability.
