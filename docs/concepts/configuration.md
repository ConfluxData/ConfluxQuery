# Configuration and targets

The default path is `~/.qcli/.env`. The file belongs to qcli and uses INI-like
sections; it is not a flat dotenv file.

```ini
[default]
decimal_places = 3
string_truncate = 80
timing = true

[trino-production]
engine = trino
url = https://trino.example.com
user = ${TRINO_USER}
token = ${TRINO_TOKEN}
catalog = hive
schema = analytics
decimal_places = 10

[snowflake-production]
engine = snowflake
account = xy12345
user = ${SNOWFLAKE_USER}
password = ${SNOWFLAKE_PASSWORD}
warehouse = ANALYTICS_WH
database = ANALYTICS
schema = PUBLIC
```

## Resolution rules

1. `[default]` defines portable defaults but is never a target.
2. Every other section header is the target name.
3. A target must set `engine` to `trino`, `databricks`, `snowflake`, or `demo`.
4. Target values override defaults property by property.
5. `${NAME}` reads an environment variable when the target is fully loaded.
6. Unknown and mistyped properties fail validation with source location and a
   suggestion when possible.
7. Secrets are marked and redacted from `config show`, `target show`, debug
   output, and interactive property output.

This lazy expansion is why `qcli target list` can list section headers without
requiring every target's credentials.

## Permissions

If a configuration contains secrets, qcli rejects overly broad Unix file
permissions. Prefer environment references and mode `0600`:

```bash
install -m 600 qcli.env ~/.qcli/.env
```

## Scope

- **Default scope:** shared display and behavior defaults.
- **Target scope:** engine connection plus target-specific overrides.
- **Session scope:** catalog, schema, and other options changed through the
  REPL, HTTP, or Flight session actions.
- **Query scope:** an immutable snapshot submitted to an adapter.

Session changes are versioned. Concurrent stale mutations fail rather than
silently overwriting newer context.

See the [property reference](../reference/configuration.md) and
[engine examples](../guides/engines.md).
