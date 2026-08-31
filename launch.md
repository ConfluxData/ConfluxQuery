# ConfluxQuery launch messaging

Status: launch copy and campaign reference

Brand authority: [ConfluxQuery brand directives](docs/brand-directives.md)

This document contains reusable launch messaging for the ConfluxData website,
GitHub, social posts, technical communities, demos, and design-partner
outreach. Current product claims and future vision are deliberately separated.

## Brand lockup

> **ConfluxQuery** by **ConfluxData**

ConfluxQuery is distributed as the `qcli` command.

## Current-release positioning

### Headline

> Query anywhere. Govern access once.

### Subheadline

> ConfluxQuery is an open-source CLI and query Gateway for Trino, Databricks
> SQL, and Snowflake. Give engineers, applications, and data tools one
> consistent way to connect, execute, stream, secure, and observe queries
> across modern analytical platforms.

### One-line description

> One open-source query access layer for Trino, Databricks SQL, and Snowflake.

### GitHub description

> Open-source query CLI and Gateway for Trino, Databricks SQL, and
> Snowflake—with HTTP, Arrow Flight SQL, ADBC, and JDBC connectivity.

### Thirty-second pitch

Most data teams use more than one analytical engine, but every engine brings
different clients, authentication, session behavior, APIs, and result formats.

ConfluxQuery provides one shared access layer across Trino, Databricks SQL, and
Snowflake. Use ConfluxQuery CLI for terminal workflows, or deploy ConfluxQuery
Gateway to connect applications through HTTP, Arrow Flight SQL, ADBC, and JDBC.

The existing platforms continue executing the queries. ConfluxQuery
standardizes connectivity, identity, sessions, query lifecycle, results, and
auditing around them.

## Website hero

```text
ConfluxQuery
by ConfluxData

Query anywhere. Govern access once.

One open-source CLI and query Gateway for Trino,
Databricks SQL, and Snowflake.

Connect engineers, applications, and data tools through
the terminal, HTTP, Arrow Flight SQL, ADBC, or JDBC—
without replacing your existing data platforms.

[Get started] [View on GitHub] [Read the documentation]
```

Supporting line:

> One workflow across multiple native SQL engines.

## The problem

Modern data teams commonly use different engines for different workloads:

- Trino for lakehouse, federated, and cost-conscious analytical queries.
- Databricks for data engineering and lakehouse workloads.
- Snowflake for managed analytics and business intelligence.

Each platform introduces different clients, drivers, authentication models,
connection configuration, sessions, metadata behavior, APIs, and operational
visibility. Applications become tied to one warehouse, engineers maintain
several tools, and platform teams repeat security and lifecycle work.

## The product answer

```text
Engineers       Applications       Data and BI tools
    |                |                    |
    +-------- CLI / HTTP / Flight / JDBC -+
                         |
                ConfluxQuery Gateway
                         |
            +------------+------------+
            |            |            |
          Trino      Databricks     Snowflake
```

ConfluxQuery creates one access boundary while every query still executes
natively on one explicitly selected configured target.

## Three product promises

### One terminal experience

> Query Trino, Databricks SQL, and Snowflake without changing your workflow.

```text
qcli --target trino-production
qcli --target databricks-production
qcli --target snowflake-production
```

Switch targets interactively, browse metadata, execute native SQL, cancel
queries, and export results in consistent formats.

### One application endpoint

> Connect applications once and select an authorized target at runtime.

Applications and tools can use:

- HTTP and OpenAPI.
- Arrow Flight SQL.
- Supported Python, Go, Java, and Rust ADBC clients.
- The ConfluxQuery JDBC Driver.
- Streaming Arrow results.

### One governed query lifecycle

> Apply consistent identity, sessions, ownership, cancellation, retention, and
> audit behavior around every warehouse query.

ConfluxQuery provides API-key and JWT/OIDC caller authentication, mTLS, target
authorization, shared query/session lifecycle, query IDs, audit events,
cancellation, bounded result retention, and optional clustered deployment.

## Product-page structure

The ConfluxData product page should use this order:

1. Hero and primary promise.
2. Multi-engine connectivity problem.
3. ConfluxQuery CLI.
4. ConfluxQuery Gateway.
5. Architecture diagram.
6. Supported engines, protocols, clients, and exact compatibility matrix.
7. Security and operations.
8. Five-minute quickstart.
9. Current limitations and roadmap.
10. Design-partner invitation.
11. GitHub and documentation links.

## Launch announcement

### Title

> Introducing ConfluxQuery: one open-source query access layer for Trino,
> Databricks, and Snowflake

### Announcement body

Data teams rarely operate a single query engine.

Trino, Databricks, and Snowflake often coexist, but applications and engineers
must deal with different drivers, authentication models, session behavior,
APIs, and operational tooling.

Today, ConfluxData is introducing ConfluxQuery: an open-source CLI and query
Gateway that provides one consistent connectivity and query-lifecycle layer
across all three platforms.

ConfluxQuery is distributed as the `qcli` command. Use ConfluxQuery CLI for
interactive and automated terminal workflows. Deploy ConfluxQuery Gateway when
applications, services, and data tools need shared access through HTTP, Arrow
Flight SQL, ADBC, or JDBC.

ConfluxQuery does not replace or federate the underlying engines. Each query
runs natively on one selected target. The Gateway standardizes how clients
connect, authenticate, manage sessions, execute, stream results, cancel, and
audit queries.

ConfluxQuery is not only a universal database client. The CLI and Gateway share
the same Rust query runtime, adapter contracts, session model, and execution
lifecycle. A query submitted through the terminal, HTTP, Flight SQL, or JDBC
reaches the same target adapters and produces the same query identity and
result contract.

ConfluxQuery is open source and ready for developers and design partners. Try
it locally, connect a target, explore the Gateway APIs, and tell us where
multi-engine access creates operational pain for your team.

## Hacker News and technical-community launch

### Title

> Show HN: ConfluxQuery – an open-source Rust CLI and query gateway for Trino,
> Databricks, and Snowflake

### Post

I built ConfluxQuery because teams using multiple analytical engines repeatedly
solve the same connectivity problems: separate CLIs, drivers, authentication,
session management, result handling, and application integration.

ConfluxQuery has two forms:

- A terminal client distributed as `qcli`.
- A query Gateway exposing HTTP, Arrow Flight SQL, ADBC, and a branded JDBC
  driver.

The underlying warehouse still executes native SQL. ConfluxQuery provides one
connection, identity, session, query lifecycle, and result boundary around
Trino, Databricks SQL, and Snowflake.

It is written in Rust and is open source. The repository includes product and
operations documentation, OpenAPI/Swagger, Flight SQL conformance profiles,
Java/Python/Go/Rust client examples, deployment assets, and a deterministic
demo adapter.

I would especially value feedback from teams operating two or more of these
engines.

Technical-community posts should remain factual, disclose limitations, and
invite architectural feedback instead of using unsupported performance or
compatibility claims.

## LinkedIn launch

> Data platforms are becoming multi-engine.
>
> A team may use Trino for low-cost lakehouse queries, Databricks for
> engineering workloads, and Snowflake for business analytics. But applications
> and engineers still manage separate clients, drivers, authentication models,
> and query lifecycles.
>
> I am introducing **ConfluxQuery by ConfluxData**—an open-source query CLI and
> Gateway for Trino, Databricks SQL, and Snowflake.
>
> ConfluxQuery provides:
>
> - One terminal workflow
> - HTTP and OpenAPI
> - Arrow Flight SQL
> - ADBC and JDBC connectivity
> - Shared sessions and query lifecycle
> - Enterprise identity and target authorization
> - Streaming Arrow results
>
> ConfluxQuery does not replace your data platforms. It provides a consistent,
> governed access layer around them.
>
> GitHub: `<repository-link>`  
> Documentation: `<documentation-link>`
>
> I am looking to speak with platform teams operating multiple query engines,
> especially those working on warehouse migration, access standardization, or
> query-cost governance.

## Suggested GitHub topics

```text
rust
sql
trino
databricks
snowflake
arrow-flight
flight-sql
adbc
jdbc
query-gateway
data-platform
```

## Launch demo

A short terminal recording should show the connection between the two product
forms:

```text
$ qcli target list

trino-low-cost
databricks-engineering
snowflake-analytics

$ qcli --target trino-low-cost
trino-low-cost[hive.default]> select count(*) from sales;

trino-low-cost[hive.default]> \use databricks-engineering
databricks-engineering[main.default]> select current_user();

$ qcli serve --flight-bind 0.0.0.0:32010
ConfluxQuery Gateway listening...
```

Then show the same configured environment from Java:

```java
Properties properties = new Properties();
properties.setProperty("token", System.getenv("QCLI_TOKEN"));

Connection connection = DriverManager.getConnection(
    "jdbc:qcli://gateway.example.com:32010/snowflake-analytics",
    properties
);
```

Demo message:

> The same targets used from the terminal are available to applications through
> one Gateway.

## Design-partner call to action

> Operating Trino alongside Databricks or Snowflake?
>
> ConfluxData is looking for design partners working on standardized
> application connectivity, warehouse migration, cross-engine query
> portability, query-cost visibility, or governed AI access.
>
> Bring a real workload. We will help deploy ConfluxQuery, qualify the
> integrations, and use the evidence to shape the product roadmap.

Initial design-partner targets are platform teams operating at least two
supported engines, with a concrete migration, governance, connectivity, or cost
problem and a willingness to run a self-hosted pilot.

## Current claims versus vision

### Say today

> ConfluxQuery is an open-source CLI and governed query Gateway for Trino,
> Databricks SQL, and Snowflake. It gives engineers, applications, and data
> tools one consistent way to connect, execute, and manage queries—without
> replacing the platforms that already run them.

### Label clearly as future vision

> ConfluxQuery is becoming the governed query control plane for modern data
> platforms—connect, translate, inspect, optimize, and route analytical
> workloads across the best authorized engine.

The vision depends on completing and validating dialect transpilation, query
passports, engine eligibility analysis, and intelligent routing. It must never
be presented as current released behavior before the corresponding milestone
gates pass.

## Claims not permitted at initial launch

Do not claim:

- Universal SQL or “write once, run anywhere.”
- Automatic cross-engine routing or warehouse cost optimization.
- Cross-engine joins or federated query execution.
- Full JDBC compliance or production-ready ODBC.
- Guaranteed performance, savings, semantic equivalence, or availability.
- A semantic layer, database, query engine, transaction coordinator, or model
  hosting platform.
- Governed MCP/agent access before its release milestone passes.

Use precise alternatives:

- “One workflow across multiple native SQL engines.”
- “One selected configured target per query.”
- “Supported named clients and engine capabilities are listed in the
  compatibility matrix.”
- “Planned” or “future vision” for M26 and later capabilities.

## Recommended launch category

Use:

> Open-source multi-engine query access layer

Do not lead with only “SQL CLI.” Do not use “intelligent query control plane”
as a current product category until the intelligence and routing milestones are
implemented and validated.

## Reusable final message

> **ConfluxQuery is an open-source CLI and governed query Gateway for Trino,
> Databricks SQL, and Snowflake. It gives engineers, applications, and data
> tools one consistent way to connect, execute, and manage queries—without
> replacing the platforms that already run them.**
