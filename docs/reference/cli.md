# Complete CLI command reference

```text
qcli [--config PATH] [--target TARGET]
qcli [--config PATH] <command>
qcli [--config PATH] --target TARGET (--command SQL | --file PATH) [--format FORMAT]
```

`--config` must be the first option when supplied. The default is
`~/.qcli/.env`.

## Global options

| Option | Meaning | Example |
|---|---|---|
| `--config PATH` | Use another sectioned configuration file. | `qcli --config ./dev.env target list` |
| `--target TARGET` | Select a target for interactive or batch execution. | `qcli --target trino-prod` |
| `--command SQL` | Execute one SQL string. Requires `--target`. | `qcli --target demo --command 'select 1'` |
| `--file PATH` | Execute UTF-8 SQL from a file; `-` reads stdin. | `qcli --target demo --file query.sql` |
| `--format FORMAT` | Override target output format for batch execution. | `--format jsonl` |
| `--help`, `-h` | Print the built-in command summary. | `qcli --help` |
| `--version`, `-V` | Print the binary version. | `qcli --version` |

Exactly one of `--command` or `--file` is required for batch execution. Query
options cannot be repeated. Supported formats are `table`, `vertical`, `csv`,
`tsv`, `json`, and `jsonl`.

## Configuration commands

### `config path`

Print the effective configuration path without loading targets.

```bash
qcli config path
qcli --config ./qa.env config path
```

### `config check`

Load, expand, type-check, and validate the whole configuration. This command
requires referenced environment variables.

```bash
qcli config check
# Configuration is valid: 3 target(s)
```

### `config show`

Print resolved defaults and targets. Secret values are rendered as
`<redacted>`.

```bash
qcli config show
```

## Target commands

### `target list`

Discover target section names and engines without expanding credentials.

```bash
qcli target list
# trino-production       trino
# warehouse-dev          databricks
```

### `target show NAME`

The `target show` command prints one resolved target with inherited defaults
and redacted secrets.

```bash
qcli target show trino-production
```

### `target test NAME`

The `target test` command connects through the selected adapter and executes
`SELECT 1`. Success includes
the engine, returned test-row count, and native engine query ID when available.

```bash
qcli target test snowflake-production
```

### `target capabilities NAME`

The `target capabilities` command inspects normalized adapter capabilities
without opening a network connection.
Use this before relying on cancellation, metadata, updates, or streaming.

```bash
qcli target capabilities databricks-production
```

## Authentication command

### `auth key create ID`

The `auth key create` command generates an opaque API key and its Argon2id
hash. The raw key is shown once.

```bash
qcli auth key create reporting-service
```

Put the hash in the auth file and deliver the raw key through a secret manager.

## Batch execution

```bash
qcli --target trino-production --command 'select current_date'
qcli --target trino-production --file ./daily-report.sql
printf 'select 1' | qcli --target trino-production --file - --format json
```

Display-only transformations apply to `table` and `vertical`; machine output
preserves full values. A downstream closed pipe is treated as normal success,
allowing `qcli ... | head`.

## `serve`

Start HTTP and optionally Flight SQL using the shared gateway runtime.

```text
serve [--bind ADDRESS]
      [--auth-file PATH] [--oidc-file PATH]
      [--trusted-proxy] [--cors-origin ORIGIN]...
      [--flight-bind ADDRESS]
      [--flight-tls-cert PATH --flight-tls-key PATH]
      [--flight-tls-client-ca PATH]
      [--flight-trusted-proxy]
      [--cluster-url POSTGRES_URL]
      [--node-id ID]
      [--result-store-url URL]
      [--flight-signing-key 32_BYTE_FILE]
```

| Option | Default/requirement |
|---|---|
| `--bind ADDRESS` | HTTP bind; default `127.0.0.1:8088`. |
| `--auth-file PATH` | Hashed API-key principals and quotas. |
| `--oidc-file PATH` | JWT/OIDC issuer, audience, JWKS, and mapping policy. |
| `--trusted-proxy` | Required for non-loopback HTTP; demands forwarded HTTPS. |
| `--cors-origin ORIGIN` | Allow one exact browser origin; repeat for more. |
| `--flight-bind ADDRESS` | Enable Flight SQL; requires an authenticator. |
| `--flight-tls-cert PATH` | PEM server certificate chain; pair with key. |
| `--flight-tls-key PATH` | PEM private key; pair with certificate. |
| `--flight-tls-client-ca PATH` | Require Flight client certificates from CA. |
| `--flight-trusted-proxy` | Trust a gRPC proxy that asserts forwarded HTTPS. |
| `--cluster-url POSTGRES_URL` | Enable PostgreSQL coordination. |
| `--node-id ID` | Stable node identity; defaults to process-derived ID. |
| `--result-store-url URL` | Shared object store required by cluster mode. |
| `--flight-signing-key 32_BYTE_FILE` | Shared cluster Flight token/ticket key. |

Cluster settings also accept `QCLI_CLUSTER_URL`, `QCLI_NODE_ID`,
`QCLI_RESULT_STORE_URL`, and `QCLI_FLIGHT_SIGNING_KEY`. CLI values take
precedence. See [server mode](../server/index.md).

## Exit codes

| Code | Class |
|---:|---|
| 0 | Success, including a downstream broken pipe. |
| 2 | Usage or SQL input error. |
| 3 | Configuration error. |
| 4 | Authentication, connection, insecure transport, or timeout driver error. |
| 5 | Other query/driver failure. |
| 6 | Interactive shell failure. |
| 7 | Output failure. |
| 8 | HTTP or Flight server failure. |
| 9 | Gateway authentication configuration failure. |

Errors go to stderr. Machine result rows remain on stdout.
