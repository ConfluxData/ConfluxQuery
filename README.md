# qcli

qcli is one query shell for cloud data platforms. Initial engine targets are Trino, Databricks SQL, and Snowflake.

Architecture and delivery documentation:

- [Product design](docs/product-design.md)
- [Execution plan and demonstrable milestones](docs/execution-plan.md)
- [Extensible authentication providers](docs/adr-003-extensible-authentication.md)
- [Databricks and Snowflake Rust client selection](docs/adr-004-databricks-snowflake-clients.md)
- [HTTP caller authentication](docs/adr-005-http-caller-authentication.md)

Inspect an engine's normalized capabilities without connecting:

```text
qcli target capabilities TARGET
```

Start the loopback-only HTTP preview:

```text
qcli serve
```

Then open `http://127.0.0.1:8088/docs/` for interactive Swagger UI or fetch
`http://127.0.0.1:8088/openapi.json` for the generated OpenAPI contract.

To require a caller identity, first generate key material:

```text
qcli auth key create analytics-key
```

The raw `qcli_k1_...` key is shown once. Put only its printed Argon2id
`secret_hash` in a mode-`0600` authentication file:

```toml
[principals.analytics]
targets = ["trino-local", "snowflake-dev"]
max_sessions = 5
max_concurrent_queries = 3

[keys.analytics-key]
principal = "analytics"
secret_hash = "$argon2id$v=19$..."
enabled = true
```

Start authenticated mode and pass the raw key as a bearer credential:

```text
qcli serve --auth-file ~/.qcli/http-auth.toml
curl -H 'Authorization: Bearer qcli_k1_...' \
  http://127.0.0.1:8088/v1/sessions/session-id
```

Keys may optionally set an RFC 3339 `expires_at`. Authenticated mode enforces
principal ownership, target allowlists, session quotas, and concurrent-query
quotas. It remains loopback-only until the remaining M11 network controls are
implemented.

The versioned API creates persistent sessions, submits asynchronous session or
stateless queries, reports status, streams SSE events, cancels work, and returns
paginated JSON, NDJSON, CSV, or Arrow-stream results. See the
[Milestone 10 notes](docs/milestones/milestone-10-notes.md) for the endpoint
contract and runnable examples.

The project is under sequenced implementation. See the [product design](docs/product-design.md) and [execution plan](docs/execution-plan.md).

## Current milestone

Milestone 1 provides configuration validation and target discovery:

```text
qcli config path
qcli config check
qcli config show
qcli target list
qcli target show TARGET
```

## Build

```text
cargo build
cargo test --workspace
```

## Milestone 1 demo

The example contains environment substitutions and must be copied with private permissions:

```text
install -m 600 examples/milestone-1.env /tmp/qcli-milestone-1.env
export QCLI_DEMO_TOKEN=demo-secret
cargo run -- --config /tmp/qcli-milestone-1.env config check
cargo run -- --config /tmp/qcli-milestone-1.env target list
cargo run -- --config /tmp/qcli-milestone-1.env target show trino-dev
```

The normal configuration location is `~/.qcli/.env`. Despite its filename, it is a qcli-owned sectioned format. `[default]` contains shared properties and every other section defines a target.

## Milestone 2 demo

Execute a deterministic query through the shared session, query, adapter, Arrow, and rendering layers:

```text
cargo run -- --config examples/milestone-2.env \
  --target demo --command "select * from sample"
```

The internal demo engine is deterministic test infrastructure. It does not replace any real warehouse adapter.
