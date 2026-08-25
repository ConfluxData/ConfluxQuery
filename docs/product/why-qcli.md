# Why qcli exists

## The problem

Modern data estates are intentionally heterogeneous. Interactive analytics may
live in Trino, governed lakehouse workloads in Databricks SQL, and commercial
or shared datasets in Snowflake. Each system is capable, but each brings a
different CLI, configuration model, output behavior, authentication surface,
session vocabulary, driver ecosystem, and operational story.

That fragmentation has a real cost:

- People relearn basic query workflows for each platform.
- Automation depends on engine-specific formatting and error behavior.
- Connection details and secrets spread through shell history and scripts.
- Application teams must choose and operate several driver stacks.
- Platform teams repeat identity, authorization, quotas, audit, and result
  retention controls at every integration boundary.
- A query that is easy from a laptop can be difficult to expose safely to a
  notebook, service, or BI client.

qcli treats those as one product problem. It provides one terminal experience
and one governed gateway without hiding the fact that the engines have
different capabilities.

## Inspiration

The original inspiration was `usql`: a broad, pleasant SQL shell showing that
users should not need a completely different terminal workflow for every
database. qcli keeps that spirit but makes different product choices.

qcli is centered on cloud analytical warehouses—Trino, Databricks SQL, and
Snowflake—and on Arrow-native application connectivity. It does not attempt to
reproduce PostgreSQL transaction semantics on engines where those semantics do
not apply. The first release prioritizes query execution, metadata, context,
streaming results, identity, and interoperability.

DataFusion CLI was evaluated as a base and rejected. It is an excellent shell
for a local DataFusion execution engine, but qcli needs remote engine adapters,
shared multi-user state, HTTP and Flight SQL protocol front ends, enterprise
identity, and high availability. Retrofitting those concerns around a local
execution CLI would create more coupling than reuse. See the
[decision record](../adr-001-independent-from-datafusion-cli.md).

## Product principles

### One workflow, honest capabilities

Common operations look consistent, while `qcli target capabilities` reports
what an adapter can actually stream, cancel, mutate, or discover. qcli does not
fake unsupported behavior.

### Exact data before pretty display

Arrow record batches are the internal result contract. Human formatting is a
view; CSV, TSV, JSON, and JSONL preserve machine values without inheriting
terminal truncation or decimal shortening.

### Shared core, multiple front ends

CLI, HTTP, and Flight SQL use the same sessions, queries, adapters, results,
cancellation, ownership rules, quotas, and audit model. Adding a protocol must
not create a second query engine.

### Secure defaults and explicit trust

The preview server binds to loopback. Non-loopback HTTP requires an explicitly
trusted TLS proxy. Non-loopback Flight requires direct TLS or trusted-proxy
mode. Secrets are redacted, config permissions are checked, and resources are
principal-bound.

### Extension points over engine conditionals

Drivers implement a common adapter boundary. Authentication, metadata,
coordination, object storage, and protocol behavior are similarly separated so
new implementations can be introduced without rewriting the product.

## What qcli is—and is not

qcli is both a query CLI and a query gateway. The CLI is the direct user
interface; `qcli serve` is the shared-service deployment mode. The same binary
supports both.

qcli is not a warehouse, optimizer, transaction coordinator, or semantic
layer. SQL is executed by the selected backend. Cross-dialect transpilation is
on the roadmap, but current releases send native SQL unless a feature page says
otherwise.
