# qcli

qcli is one query shell for cloud data platforms. Initial engine targets are Trino, Databricks SQL, and Snowflake.

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
